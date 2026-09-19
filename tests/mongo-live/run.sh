#!/usr/bin/env bash
# Issue #7 真实 MongoDB 容器验收编排：
#   1) 动态项目名 compose up（cpus<=0.75 / mem_limit<=512m /
#      wiredTigerCacheSizeGB<=0.25）
#   2) PHASE=A：CRUD/not-found/duplicate-key/driver 错误/16 并发 smoke/close
#   3) PHASE=B：容器停止态命令必须失败（Unavailable，不悬挂不假成功）
#   4) PHASE=C：容器重启后新 client 恢复
#   5) trap 兜底 down -v：容器、网络、测试数据全清理
# 用法：run.sh <Test_mongo_live 可执行路径>
set -euo pipefail

BIN="${1:?usage: run.sh <path-to-Test_mongo_live>}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="bbt-mongo-live-$$-$(date +%s)"
COMPOSE=(docker compose -p "${PROJECT}" -f "${DIR}/compose.yml")

# 宿主机端口固定：实测 compose stop/start 会重分随机映射端口，
# “容器重启后新 client”与 PHASE=B 停止态都需要稳定目标地址。
BBT_MONGO_PORT="$(python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)"
export BBT_MONGO_PORT

cleanup() {
    "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

"${COMPOSE[@]}" up -d --wait
ADDR="$("${COMPOSE[@]}" port mongo 27017)"
HOST="${ADDR%:*}"
PORT="${ADDR##*:}"

export BBT_TEST_MONGO_URI="mongodb://${HOST}:${PORT}/"
export BBT_MONGO_CTL="docker compose -p ${PROJECT} -f ${DIR}/compose.yml"

echo "[mongo-live] project=${PROJECT} uri=${BBT_TEST_MONGO_URI}"

BBT_MONGO_PHASE=A "${BIN}"

"${COMPOSE[@]}" stop mongo
BBT_MONGO_PHASE=B "${BIN}"

"${COMPOSE[@]}" start mongo
# 等待健康检查恢复，避免 start 与连接竞争
for i in $(seq 1 45); do
    if "${COMPOSE[@]}" exec -T mongo mongosh --quiet --eval \
        "db.adminCommand('ping').ok" 2>/dev/null | grep -q 1; then
        break
    fi
    sleep 1
done
BBT_MONGO_PHASE=C "${BIN}"

echo "[mongo-live] done; cleaning up project=${PROJECT}"
