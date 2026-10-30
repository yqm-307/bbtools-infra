#!/usr/bin/env bash
# 真实 Mongo live 门禁（fail-closed，有界）：复用 Release 已构建产物。
# 缺 docker/compose/live 二进制一律非零失败，不得 skip 变绿。
#
# 输入（env）：BUILD_DIR REPO_DIR
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
MONGO_BIN="$BUILD_DIR/tests/Test_mongo_live"
if [ ! -x "$MONGO_BIN" ]; then
    echo "FAIL: 缺 Release live 二进制 $MONGO_BIN（不得 skip 变绿）" >&2
    exit 1
fi
bash "$REPO_DIR/tests/mongo-live/run.sh" "$MONGO_BIN"
echo "=== live 容器镜像身份（复用现 compose mongo:7.0，未引入新镜像治理）==="
docker image inspect mongo:7.0 --format 'mongo:7.0 id={{.Id}}' 2>/dev/null || true
