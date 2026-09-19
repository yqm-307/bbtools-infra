#!/usr/bin/env bash
# Issue #6 真实 Redis 容器验收编排：
#   1) 动态项目名 compose up（cpus<=0.5 / mem_limit<=256m）
#   2) PHASE=A：功能/二进制/错误/并发/断连恢复/close
#   3) PHASE=B：容器停止态命令必须失败
#   4) PHASE=C：容器重启后新 client 恢复
#   5) trap 兜底 down -v：容器、网络、测试数据全清理
# 用法：run.sh <Test_redis_live 可执行路径>
set -euo pipefail

BIN="${1:?usage: run.sh <path-to-Test_redis_live>}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="bbt-redis-live-$$-$(date +%s)"
COMPOSE=(docker compose -p "${PROJECT}" -f "${DIR}/compose.yml")

# 宿主机端口固定：实测 compose stop/start 会重分随机映射端口，
# “同一 client 断连重连”与“重启后新 client”都需要稳定目标地址。
BBT_REDIS_PORT="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
export BBT_REDIS_PORT

cleanup() {
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${COMPOSE[@]}" up -d --wait
ADDR="$("${COMPOSE[@]}" port redis 6379)"

export BBT_TEST_REDIS_ADDR="${ADDR}"
export BBT_REDIS_CTL="docker compose -p ${PROJECT} -f ${DIR}/compose.yml"
export BBT_REDIS_RCLI="docker compose -p ${PROJECT} -f ${DIR}/compose.yml exec -T redis redis-cli"

echo "[redis-live] project=${PROJECT} addr=${ADDR}"

BBT_REDIS_PHASE=A "${BIN}"

"${COMPOSE[@]}" stop redis
BBT_REDIS_PHASE=B "${BIN}"

"${COMPOSE[@]}" start redis
# 等待健康检查恢复，避免 start 与 connect 竞争
for i in $(seq 1 30); do
    if "${COMPOSE[@]}" exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; then
        break
    fi
    sleep 1
done
BBT_REDIS_PHASE=C "${BIN}"

echo "[redis-live] done; cleaning up project=${PROJECT}"
