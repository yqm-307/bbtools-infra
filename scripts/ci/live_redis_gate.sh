#!/usr/bin/env bash
# 复用新 Redis 注册/实跑与容器清理门禁；缺产物或环境明确失败。
set -euo pipefail
: "${BUILD_DIR:?need BUILD_DIR}" "${REPO_DIR:?need REPO_DIR}"
for name in Test_redis_net_contract redis_net_example; do
    case "$name" in
        Test_redis_net_contract) bin="$BUILD_DIR/bbt_infra_redis_net/redis_net_tests/$name" ;;
        *) bin="$BUILD_DIR/bbt_infra_redis_net/redis_net_examples/$name" ;;
    esac
    test -x "$bin" || { echo "FAIL: 缺 live 产物 $bin" >&2; exit 1; }
done
bash "$REPO_DIR/tests/redis-live/run.sh" "$BUILD_DIR"
