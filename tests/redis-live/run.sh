#!/usr/bin/env bash
# 新 Redis 真实后端门禁；独占容器、动态端口、注册集=实跑集且零跳过。
set -euo pipefail
BUILD_DIR="${1:?usage: run.sh <build-dir>}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$DIR/compose.yml"
command -v docker >/dev/null 2>&1 || { echo 'FAIL: docker 不可用' >&2; exit 1; }
docker compose version >/dev/null 2>&1 || { echo 'FAIL: compose 不可用' >&2; exit 1; }
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

export BBT_TEST_REDIS_ADDR="127.0.0.1:$BBT_REDIS_PORT"
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
