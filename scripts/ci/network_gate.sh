#!/usr/bin/env bash
# network job：network-only 独立构建 + 全注册集实跑 + pingpong 复跑门禁。
# 等价现役 ci.yml network job 的各 step。
#
# 输入（env）：REPO_DIR BUILD_DIR BOOST_PREFIX COROUTINE_DIR JOBS LAUNCHER
set -euo pipefail

: "${REPO_DIR:?need REPO_DIR}" "${BUILD_DIR:?need BUILD_DIR}" \
  "${BOOST_PREFIX:?need BOOST_PREFIX}" "${COROUTINE_DIR:?need COROUTINE_DIR}" \
  "${JOBS:?need JOBS}"
LAUNCHER="${LAUNCHER:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

cmake -S "$REPO_DIR" -B "$BUILD_DIR" -G Ninja \
    -DBBT_INFRA_NETWORK_ONLY=ON -DBUILD_TESTING=ON -DCMAKE_BUILD_TYPE=Debug \
    $LAUNCHER \
    -DBOOST_ROOT="$BOOST_PREFIX" -DCMAKE_PREFIX_PATH="$BOOST_PREFIX" \
    -DBBT_COROUTINE_SOURCE_DIR="$COROUTINE_DIR"
# 子 consumer 由 CTest configure，不继承父 cache；显式传递同一锁定 Boost 前缀。
# 本脚本单 step 内 cmake+ctest 串行：export 才对本 step 后续子进程生效
# （写 GITHUB_ENV 只作用于下一 step，无法送达本 step 内 ctest 的子 cmake）。
export CMAKE_PREFIX_PATH="$BOOST_PREFIX"
export BOOST_ROOT="$BOOST_PREFIX"
cmake --build "$BUILD_DIR" --parallel "$JOBS" --target \
    bbt_infra_net_header_selfcheck Test_net_primitives Test_net_allocation \
    Test_net_foundation Test_net_reactor_driver Test_net_tcp Test_net_contract_regressions \
    Test_net_freeze_contract Test_net_append_busy_hook \
    net_foundation_consumer Example_co_net_service \
    Test_net_reclaim_receipt Test_net_accept_stop Test_net_accept_stop_fd \
    Test_net_ico_write_close Test_net_pop_all Test_net_buffer_oom \
    Example_ico_connection

ctest --test-dir "$BUILD_DIR" --show-only=json-v1 > "$BUILD_DIR/registered.json"
ctest --test-dir "$BUILD_DIR" -j1 --no-tests=error \
    --output-on-failure --timeout 120 --output-junit "$BUILD_DIR/network.xml"
python3 "$SCRIPT_DIR/check_network_tests.py" \
    "$BUILD_DIR/registered.json" "$BUILD_DIR/network.xml"
ctest --test-dir "$BUILD_DIR" -j1 -R '^net\.co_pingpong_strict$' \
    --no-tests=error --repeat until-fail:5 --output-on-failure --timeout 60
