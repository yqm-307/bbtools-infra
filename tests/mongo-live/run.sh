#!/usr/bin/env bash
# Issue #7 真实 MongoDB 容器验收编排（有界、fail-closed、独占清理）：
#   1) 动态项目名 compose up（cpus<=0.75 / mem_limit<=512m /
#      wiredTigerCacheSizeGB<=0.25）
#   2) PHASE=A：CRUD/not-found/duplicate-key/driver 错误/16 并发 smoke/close
#   3) PHASE=B：容器停止态命令必须失败（Unavailable，不悬挂不假成功）
#   4) PHASE=C：容器重启后新 client 恢复
#   5) trap 兜底 down -v：仅本 project 的容器、网络、测试数据全清理
#   6) 失败诊断：仅非零退出时、在 down -v 前把 compose 状态/日志/inspect
#      落到 ${BBT_MONGO_DIAG_DIR:-$PWD/mongo-live-diagnostics}，供 workflow 失败上传。
# 用法：run.sh <Test_mongo_live 可执行>
#
# fail-closed：缺 docker / docker compose / live 二进制，或 compose 启动/健康在界内
# 未恢复，一律非零退出——绝不整组 skip 变绿。所有等待均有界：compose up/health/test、
# stop/start/cleanup 与健康探测 exec 全部经 timeout 包裹。
set -euo pipefail

BIN="${1:?usage: run.sh <path-to-Test_mongo_live>}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT="bbt-mongo-live-$$-$(date +%s)"
COMPOSE=(docker compose -p "${PROJECT}" -f "${DIR}/compose.yml")

# 有界上限（秒）：compose up+wait 内层超时；重启后健康恢复轮询截止；单阶段测试超时；
# stop/start/health-exec/cleanup 的 timeout 与 SIGKILL 宽限。
# PHASE_TIMEOUT 对齐 CTest mongo.live TIMEOUT 120。
UP_TIMEOUT=240
HEALTH_TIMEOUT=90
PHASE_TIMEOUT=120
CTL_TIMEOUT=30
EXEC_TIMEOUT=10
CLEANUP_TIMEOUT=60
KILL_AFTER=10

# 失败诊断产物：仅失败路径在 down -v 前写入；默认工作区根，workflow 上传该目录。
# 诊断命令全部有界；采集失败只降级为无产物，绝不覆盖原始失败退出码或放宽门禁。
DIAG_DIR="${BBT_MONGO_DIAG_DIR:-${GITHUB_WORKSPACE:-$PWD}/mongo-live-diagnostics}"
DIAG_TIMEOUT=10
DIAG_KILL_AFTER=5
COMPOSE_STARTED=0

fail() { echo "[mongo-live] FAIL: $*" >&2; exit 1; }

# 一次有界诊断采集：命令失败/超时只记录到文件，不抛出、不改变主流程退出码。
diag_capture() {
    local file="$1"
    shift
    [ -n "${DIAG_DIR}" ] || return 0
    local rc=0
    {
        printf '$ %s\n' "$*"
        timeout --kill-after="${DIAG_KILL_AFTER}" "${DIAG_TIMEOUT}" "$@" 2>&1 || rc=$?
        printf '\n[exit=%d]\n' "${rc}"
    } > "${DIAG_DIR}/${file}" 2>&1
    return 0
}

# 失败诊断：必须在 cleanup 的 down -v 之前采集（容器/网络/日志尚在）。
# 所有输出落到独立文件；任何采集错误都吞掉，调用方保持原始退出码。
collect_failure_diagnostics() {
    local exit_code="$1"
    [ -n "${DIAG_DIR}" ] || return 0
    # 目录不可创建时只放弃诊断，不改变主失败退出码。
    mkdir -p "${DIAG_DIR}" 2>/dev/null || return 0
    (
        set +e
        {
            echo "timestamp_utc=$(date -u '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null)"
            echo "exit_code=${exit_code}"
            echo "project=${PROJECT}"
            echo "binary=${BIN}"
            echo "mongo_uri=${BBT_TEST_MONGO_URI:-<unset>}"
            echo "working_dir=${PWD}"
            echo "diag_dir=${DIAG_DIR}"
            echo "=== uname -a ==="
            timeout --kill-after="${DIAG_KILL_AFTER}" "${DIAG_TIMEOUT}" uname -a 2>&1
            echo "[exit=$?]"
            echo "=== docker version ==="
            timeout --kill-after="${DIAG_KILL_AFTER}" "${DIAG_TIMEOUT}" docker version 2>&1
            echo "[exit=$?]"
            echo "=== docker compose version ==="
            timeout --kill-after="${DIAG_KILL_AFTER}" "${DIAG_TIMEOUT}" docker compose version 2>&1
            echo "[exit=$?]"
        } > "${DIAG_DIR}/00-context.txt" 2>&1

        diag_capture "05-compose-config.yml" "${COMPOSE[@]}" config
        diag_capture "10-compose-ps.txt" "${COMPOSE[@]}" ps -a
        diag_capture "20-compose-logs.txt" "${COMPOSE[@]}" logs --no-color --tail=500

        local ids=""
        ids="$(timeout --kill-after="${DIAG_KILL_AFTER}" "${DIAG_TIMEOUT}" \
            "${COMPOSE[@]}" ps -a -q 2>/dev/null)"
        if [ -n "${ids}" ]; then
            local -a containers=()
            mapfile -t containers <<< "${ids}"
            diag_capture "30-container-inspect.json" docker inspect "${containers[@]}"
        else
            printf 'no containers for project %s\n' "${PROJECT}" \
                > "${DIAG_DIR}/30-container-inspect.json"
        fi

        {
            echo "=== df -h ==="
            timeout --kill-after="${DIAG_KILL_AFTER}" "${DIAG_TIMEOUT}" df -h 2>&1
            echo "[exit=$?]"
            echo "=== df -i ==="
            timeout --kill-after="${DIAG_KILL_AFTER}" "${DIAG_TIMEOUT}" df -i 2>&1
            echo "[exit=$?]"
            echo "=== free -m ==="
            timeout --kill-after="${DIAG_KILL_AFTER}" "${DIAG_TIMEOUT}" free -m 2>&1
            echo "[exit=$?]"
            echo "=== ulimit -a ==="
            timeout --kill-after="${DIAG_KILL_AFTER}" "${DIAG_TIMEOUT}" bash -c 'ulimit -a' 2>&1
            echo "[exit=$?]"
        } > "${DIAG_DIR}/40-host.txt" 2>&1

        diag_capture "50-mongo-ping.txt" "${COMPOSE[@]}" exec -T mongo mongosh \
            --quiet --eval "db.adminCommand('ping').ok"

        echo "[mongo-live] 诊断产物已写入 ${DIAG_DIR}"
    ) || true
    return 0
}

# 只清理本编排独占的 project：down -v 带走本 project 的容器/网络/匿名卷；
# 不 prune、不按宽前缀清理其他任务资源。清理失败可见但不覆盖主失败（EXIT trap 中非致命）。
cleanup() {
    local exit_code=$?
    local rc=0
    if [ "${exit_code}" -ne 0 ]; then
        collect_failure_diagnostics "${exit_code}"
    fi
    if [ "${COMPOSE_STARTED}" -ne 1 ]; then
        echo "[mongo-live] cleanup skipped project=${PROJECT} (compose 未启动)"
        return 0
    fi
    echo "[mongo-live] cleanup project=${PROJECT} (down -v)"
    timeout --kill-after="${KILL_AFTER}" "${CLEANUP_TIMEOUT}" \
        "${COMPOSE[@]}" down -v --remove-orphans || rc=$?
    if [ "${rc}" -eq 0 ]; then
        echo "[mongo-live] cleanup ok project=${PROJECT}"
    else
        echo "[mongo-live] WARN: cleanup failed project=${PROJECT} rc=${rc}" >&2
    fi
    return 0
}
trap cleanup EXIT
# 追加信号路径：INT/TERM 显式退出（130/143）以触发上面的 EXIT 清理；SIGKILL 无法捕获，
# 由 hosted 一次性 VM 回收兜底（仅 hosted 边界声明）。
trap 'exit 130' INT
trap 'exit 143' TERM

# 前置检查：缺 docker/compose/二进制一律非零失败，不得整组 skip 变绿。
command -v docker >/dev/null 2>&1 || fail "docker 不可用"
docker compose version >/dev/null 2>&1 || fail "docker compose 不可用"
[ -x "${BIN}" ] || fail "live 二进制不存在或不可执行: ${BIN}"

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

# 健康恢复轮询：按 SECONDS 截止（不按 90×固定 sleep 累计），成功 0，超上界非零（调用方 fail）。
# 每次探测 exec 自身也有界（timeout --kill-after），避免 backend 挂死时单次探测无界。
wait_healthy() {
    local deadline=$((SECONDS + HEALTH_TIMEOUT))
    while [ "${SECONDS}" -lt "${deadline}" ]; do
        if timeout --kill-after="${KILL_AFTER}" "${EXEC_TIMEOUT}" \
            "${COMPOSE[@]}" exec -T mongo mongosh --quiet --eval \
            "db.adminCommand('ping').ok" 2>/dev/null | grep -q 1; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# compose up --wait：内层 timeout 有界；非零/超时即 fail-closed。
COMPOSE_STARTED=1
timeout --kill-after="${KILL_AFTER}" "${UP_TIMEOUT}" \
    "${COMPOSE[@]}" up -d --wait --wait-timeout "${HEALTH_TIMEOUT}" \
    || fail "compose up --wait 失败或超过 ${UP_TIMEOUT}s（含健康检查）"
ADDR="$(timeout --kill-after="${KILL_AFTER}" "${CTL_TIMEOUT}" "${COMPOSE[@]}" port mongo 27017)"
[ -n "${ADDR}" ] || fail "无法解析 mongo 端口映射"
HOST="${ADDR%:*}"
PORT="${ADDR##*:}"

export BBT_TEST_MONGO_URI="mongodb://${HOST}:${PORT}/"
export BBT_MONGO_CTL="docker compose -p ${PROJECT} -f ${DIR}/compose.yml"
# Issue #34：容器内 mongosh 控制通道前缀——用例经它在真实后端注入
# failCommand(blockConnection) 故障并做 waitForFailPoint 进入握手（后端确证
# 目标命令在途），与 Redis live 的容器内 redis-cli 控制同源；只覆盖本 project。
export BBT_MONGO_CTL_EVAL="docker compose -p ${PROJECT} -f ${DIR}/compose.yml exec -T mongo mongosh --quiet --eval"

echo "[mongo-live] project=${PROJECT} uri=${BBT_TEST_MONGO_URI}"

# PHASE=A：容器在线。timeout 有界，失败即非零。
echo "[mongo-live] PHASE=A (online)"
timeout --kill-after="${KILL_AFTER}" "${PHASE_TIMEOUT}" env BBT_MONGO_PHASE=A "${BIN}"

# PHASE=B：停止态。命令必须失败而非悬挂/假成功。stop 有界。
echo "[mongo-live] PHASE=B (stopped)"
timeout --kill-after="${KILL_AFTER}" "${CTL_TIMEOUT}" "${COMPOSE[@]}" stop mongo
timeout --kill-after="${KILL_AFTER}" "${PHASE_TIMEOUT}" env BBT_MONGO_PHASE=B "${BIN}"

# PHASE=C：重启态。待健康恢复（有界），否则 fail-closed。start 有界。
echo "[mongo-live] PHASE=C (restarted)"
timeout --kill-after="${KILL_AFTER}" "${CTL_TIMEOUT}" "${COMPOSE[@]}" start mongo
wait_healthy || fail "mongo 重启后健康检查未在 ${HEALTH_TIMEOUT}s 内恢复"
timeout --kill-after="${KILL_AFTER}" "${PHASE_TIMEOUT}" env BBT_MONGO_PHASE=C "${BIN}"

# 不在此处声明 cleanup 结果：清理发生在 EXIT trap，其成败由 cleanup() 自行打印。
echo "[mongo-live] done (A/B/C ok); cleaning up project=${PROJECT}"
