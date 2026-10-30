#!/usr/bin/env bash
# 真实 Redis live 门禁（fail-closed，有界）：复用 Release 已构建产物。
# 缺 docker/compose/任一 live 二进制一律非零失败，不得 skip 变绿。
# run.sh 内部已做 compose 编排与 trap 清理；本脚本只做前置校验与镜像身份观测。
#
# 输入（env）：
#   BUILD_DIR   Release 构建目录（含 tests/Test_redis_live、Test_redis_cotcp_binding）
#   REPO_DIR    infra 检出根（含 tests/redis-live/run.sh）
set -euo pipefail

: "${BUILD_DIR:?need BUILD_DIR}" "${REPO_DIR:?need REPO_DIR}"

if ! command -v docker >/dev/null 2>&1; then
    echo "FAIL: docker 不可用（live 门禁要求真实容器，不得 skip 变绿）" >&2
    exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
    echo "FAIL: docker compose 不可用（live 门禁要求真实容器，不得 skip 变绿）" >&2
    exit 1
fi
REDIS_BIN="$BUILD_DIR/tests/Test_redis_live"
COTCP_BIN="$BUILD_DIR/tests/Test_redis_cotcp_binding"
for b in "$REDIS_BIN" "$COTCP_BIN"; do
    if [ ! -x "$b" ]; then
        echo "FAIL: 缺 Release live 二进制 $b（不得 skip 变绿）" >&2
        exit 1
    fi
done
# run.sh 第二参数：redis.cotcp_binding.live 复用同一容器实跑，
# 覆盖大值 / 2MiB 大回复回归，不与 Test_redis_live 混计。
bash "$REPO_DIR/tests/redis-live/run.sh" "$REDIS_BIN" "$COTCP_BIN"
echo "=== live 容器镜像身份（复用现 compose redis7-alpine，未引入新镜像治理）==="
docker image inspect redis:7-alpine --format 'redis:7-alpine id={{.Id}}' 2>/dev/null || true
