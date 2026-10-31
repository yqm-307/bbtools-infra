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

PROJECT="bbt-redis-net-$$-$(date +%s)"
export PROJECT
cleanup() {
    set +e
    timeout 60 docker compose -p "$PROJECT" -f "$COMPOSE_FILE" down -v
    remaining="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT")"
    test -z "$remaining"
}
trap cleanup EXIT

BBT_REDIS_PORT="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
export BBT_REDIS_PORT
timeout 180 docker compose -p "$PROJECT" -f "$COMPOSE_FILE" up -d
for _ in $(seq 1 60); do
    if timeout 10 docker compose -p "$PROJECT" -f "$COMPOSE_FILE" exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; then
        echo "redis 容器就绪 port=$BBT_REDIS_PORT"
        break
    fi
    sleep 1
done
docker compose -p "$PROJECT" -f "$COMPOSE_FILE" exec -T redis redis-cli ping >/dev/null 2>&1 \
    || { echo "FAIL: redis 容器健康检查超时" >&2; exit 1; }

ctest --test-dir "$BUILD_DIR" --show-only=json-v1 > "$BUILD_DIR/registered.json"
ctest --test-dir "$BUILD_DIR" -j1 -R '^redis\.net\.' --no-tests=error \
    --output-on-failure --timeout 120 --output-junit "$BUILD_DIR/redis-net.xml"
python3 - "$BUILD_DIR/registered.json" "$BUILD_DIR/redis-net.xml" <<'PY'
import json, sys, xml.etree.ElementTree as ET
registered = json.load(open(sys.argv[1]))
names = sorted(t["name"] for t in registered["tests"] if t["name"].startswith("redis.net."))
root = ET.parse(sys.argv[2]).getroot()
ran = sorted(tc.attrib["name"] for ts in root.iter("testsuite") for tc in ts.iter("testcase"))
skipped = [tc.attrib["name"] for ts in root.iter("testsuite") for tc in ts.iter("skipped")]
assert names == ran, f"注册集 {len(names)} 与实跑 {len(ran)} 不一致: {set(names)^set(ran)}"
assert not skipped, f"意外 skipped: {skipped}"
print(f"redis.net 注册集与实跑一致: {len(ran)} 项零跳过")
PY

ctest --test-dir "$BUILD_DIR" -j1 -R '^example_redis_net_consumer$' --no-tests=error \
    --output-on-failure --timeout 60
