#!/usr/bin/env bash
# redis-net job：REDIS_NET=ON 独立构建 + 临时容器 + redis.net.* 全注册集实跑门禁。
# 等价现役 ci.yml redis-net job 的各 step（编排内嵌，trap 保证清理）。
#
# 输入（env）：REPO_DIR BUILD_DIR BOOST_PREFIX DEPS_PREFIX COROUTINE_DIR JOBS LAUNCHER
set -euo pipefail

: "${REPO_DIR:?need REPO_DIR}" "${BUILD_DIR:?need BUILD_DIR}" \
  "${BOOST_PREFIX:?need BOOST_PREFIX}" "${DEPS_PREFIX:?need DEPS_PREFIX}" \
  "${COROUTINE_DIR:?need COROUTINE_DIR}" "${JOBS:?need JOBS}"
LAUNCHER="${LAUNCHER:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$REPO_DIR/tests/redis-live/compose.yml"

command -v docker >/dev/null 2>&1 || { echo "FAIL: docker 不可用" >&2; exit 1; }
docker compose version >/dev/null 2>&1 || { echo "FAIL: docker compose 不可用" >&2; exit 1; }
test -d "$DEPS_PREFIX/hiredis" || { echo "FAIL: hiredis 私有前缀缺失: $DEPS_PREFIX/hiredis" >&2; exit 1; }
echo "docker/compose/hiredis 前缀就绪"

cmake -S "$REPO_DIR" -B "$BUILD_DIR" -G Ninja \
    -DBBT_INFRA_REDIS_NET=ON -DBUILD_TESTING=ON -DCMAKE_BUILD_TYPE=Debug \
    $LAUNCHER \
    -DBOOST_ROOT="$BOOST_PREFIX" -DCMAKE_PREFIX_PATH="$BOOST_PREFIX" \
    -DBBT_COROUTINE_SOURCE_DIR="$COROUTINE_DIR" \
    -DBBT_HIREDIS_PREFIX="$DEPS_PREFIX/hiredis"
cmake --build "$BUILD_DIR" --parallel "$JOBS" --target \
    bbt_infra_redis_net_header_selfcheck Test_redis_net_contract redis_net_example

bash "$REPO_DIR/tests/redis-live/run.sh" "$BUILD_DIR"
