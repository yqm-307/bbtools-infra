#!/usr/bin/env bash
# Issue #6 真实 Redis 容器验收编排（有界、fail-closed、独占清理）：
#   1) 动态项目名 compose up（cpus<=0.5 / mem_limit<=256m）
#   2) PHASE=A：功能/二进制/错误/并发/断连恢复/close（可选追加 cotcp binding live 组）
#   3) PHASE=B：容器停止态命令必须失败
#   4) PHASE=C：容器重启后新 client 恢复
#   5) trap 兜底 down -v：仅本 project 的容器、网络、测试数据全清理
# 用法：run.sh <Test_redis_live 可执行> [<Test_redis_cotcp_binding 可执行>]
#
# fail-closed：缺 docker / docker compose / live 二进制，或 compose 启动/健康在界内
# 未恢复，一律非零退出——绝不整组 skip 变绿（CTest 的 SKIP_RETURN_CODE=77 只在
# 二进制自身判定「环境不可用」时适用，不由本编排制造）。所有等待均有界：compose
# up/health/test、stop/start/cleanup 与健康探测 exec 全部经 timeout 包裹。
set -euo pipefail

BIN="${1:?usage: run.sh <path-to-Test_redis_live> [path-to-Test_redis_cotcp_binding]}"
COTCP_BIN="${2:-}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="bbt-redis-live-$$-$(date +%s)"
COMPOSE=(docker compose -p "${PROJECT}" -f "${DIR}/compose.yml")

# 有界上限（秒）：compose up+wait 内层超时；重启后健康恢复轮询截止；单阶段测试超时；
# stop/start/health-exec/cleanup 的 timeout 与 SIGKILL 宽限。
# PHASE_TIMEOUT 对齐 CTest redis.live TIMEOUT 120；COTCP_TIMEOUT 对齐 cotcp.live TIMEOUT 240。
UP_TIMEOUT=180
HEALTH_TIMEOUT=60
PHASE_TIMEOUT=120
COTCP_TIMEOUT=240
CTL_TIMEOUT=30
EXEC_TIMEOUT=10
CLEANUP_TIMEOUT=60
KILL_AFTER=10

fail() { echo "[redis-live] FAIL: $*" >&2; exit 1; }

# 前置检查：缺 docker/compose/二进制一律非零失败，不得整组 skip 变绿。
command -v docker >/dev/null 2>&1 || fail "docker 不可用"
docker compose version >/dev/null 2>&1 || fail "docker compose 不可用"
[ -x "${BIN}" ] || fail "live 二进制不存在或不可执行: ${BIN}"
if [ -n "${COTCP_BIN}" ]; then
    [ -x "${COTCP_BIN}" ] || fail "cotcp live 二进制不存在或不可执行: ${COTCP_BIN}"
fi

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

# 只清理本编排独占的 project：down -v 带走本 project 的容器/网络/匿名卷；
# 不 prune、不按宽前缀清理其他任务资源。清理失败可见但不覆盖主失败（EXIT trap 中非致命）。
cleanup() {
    local rc=0
    echo "[redis-live] cleanup project=${PROJECT} (down -v)"
    timeout --kill-after="${KILL_AFTER}" "${CLEANUP_TIMEOUT}" \
        "${COMPOSE[@]}" down -v --remove-orphans || rc=$?
    if [ "${rc}" -eq 0 ]; then
        echo "[redis-live] cleanup ok project=${PROJECT}"
    else
        echo "[redis-live] WARN: cleanup failed project=${PROJECT} rc=${rc}" >&2
    fi
    return 0
}
trap cleanup EXIT
# 追加信号路径：INT/TERM 显式退出（130/143）以触发上面的 EXIT 清理；SIGKILL 无法捕获，
# 由 hosted 一次性 VM 回收兜底（仅 hosted 边界声明）。
trap 'exit 130' INT
trap 'exit 143' TERM

# 健康恢复轮询：按 SECONDS 截止（不按 90×固定 sleep 累计），成功 0，超上界非零（调用方 fail）。
# 每次探测 exec 自身也有界（timeout --kill-after），避免 backend 挂死时单次探测无界。
wait_healthy() {
    local deadline=$((SECONDS + HEALTH_TIMEOUT))
    while [ "${SECONDS}" -lt "${deadline}" ]; do
        if timeout --kill-after="${KILL_AFTER}" "${EXEC_TIMEOUT}" \
            "${COMPOSE[@]}" exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# compose up --wait：内层 timeout 有界；非零/超时即 fail-closed。
timeout --kill-after="${KILL_AFTER}" "${UP_TIMEOUT}" \
    "${COMPOSE[@]}" up -d --wait --wait-timeout "${HEALTH_TIMEOUT}" \
    || fail "compose up --wait 失败或超过 ${UP_TIMEOUT}s（含健康检查）"
ADDR="$(timeout --kill-after="${KILL_AFTER}" "${CTL_TIMEOUT}" "${COMPOSE[@]}" port redis 6379)"
[ -n "${ADDR}" ] || fail "无法解析 redis 端口映射"

export BBT_TEST_REDIS_ADDR="${ADDR}"
export BBT_REDIS_CTL="docker compose -p ${PROJECT} -f ${DIR}/compose.yml"
export BBT_REDIS_RCLI="docker compose -p ${PROJECT} -f ${DIR}/compose.yml exec -T redis redis-cli"

echo "[redis-live] project=${PROJECT} addr=${ADDR}"

# PHASE=A：容器在线。timeout 有界，失败即非零。
echo "[redis-live] PHASE=A (online)"
timeout --kill-after="${KILL_AFTER}" "${PHASE_TIMEOUT}" env BBT_REDIS_PHASE=A "${BIN}"

# 可选：redis.cotcp_binding.live 复用同一容器（真实 round-trip / 2MiB 大回复 /
# RemoteError 大值回归不属于 Test_redis_live，必须在此实跑而非漏计）。
if [ -n "${COTCP_BIN}" ]; then
    echo "[redis-live] PHASE=A cotcp_binding.live (online)"
    timeout --kill-after="${KILL_AFTER}" "${COTCP_TIMEOUT}" "${COTCP_BIN}" \
        --run_test=redis_cotcp_binding/t_setup_scheduler:redis_cotcp_binding/t_resp2_roundtrip_live
fi

# PHASE=B：停止态。命令必须失败而非悬挂/假成功。stop 有界。
echo "[redis-live] PHASE=B (stopped)"
timeout --kill-after="${KILL_AFTER}" "${CTL_TIMEOUT}" "${COMPOSE[@]}" stop redis
timeout --kill-after="${KILL_AFTER}" "${PHASE_TIMEOUT}" env BBT_REDIS_PHASE=B "${BIN}"

# PHASE=C：重启态。待健康恢复（有界），否则 fail-closed。start 有界。
echo "[redis-live] PHASE=C (restarted)"
timeout --kill-after="${KILL_AFTER}" "${CTL_TIMEOUT}" "${COMPOSE[@]}" start redis
wait_healthy || fail "redis 重启后健康检查未在 ${HEALTH_TIMEOUT}s 内恢复"
timeout --kill-after="${KILL_AFTER}" "${PHASE_TIMEOUT}" env BBT_REDIS_PHASE=C "${BIN}"

# 不在此处声明 cleanup 结果：清理发生在 EXIT trap，其成败由 cleanup() 自行打印。
echo "[redis-live] done (A/B/C ok); cleaning up project=${PROJECT}"
