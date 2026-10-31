#!/usr/bin/env bash
# Debug job 的构建+旧模块 9 项契约门禁+新 RPC/HTTP Debug 契约编排。
# 等价现役 ci.yml debug job 的各 step；假定 prepared deps 已解包、
# ccache 开关已由调用方判定（本脚本按同一规则重算，不引入新语义）。
#
# 输入（env）：
#   REPO_DIR / WORKSPACE / BUILD_DIR / BOOST_PREFIX / DEPS_PREFIX /
#   PROTOBUF_PREFIX / COROUTINE_DIR / CORE_DIR / JOBS / LAUNCHER
set -euo pipefail

: "${REPO_DIR:?need REPO_DIR}" "${BUILD_DIR:?need BUILD_DIR}" \
  "${BOOST_PREFIX:?need BOOST_PREFIX}" "${DEPS_PREFIX:?need DEPS_PREFIX}" \
  "${PROTOBUF_PREFIX:?need PROTOBUF_PREFIX}" "${COROUTINE_DIR:?need COROUTINE_DIR}" \
  "${CORE_DIR:?need CORE_DIR}" "${JOBS:?need JOBS}"
LAUNCHER="${LAUNCHER:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---- Debug 旧模块配置与构建（9 项既有集） ----
DEBUG_BUILD_DIR="${BUILD_DIR}-debug"
cmake -S "$REPO_DIR" -B "$DEBUG_BUILD_DIR" -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTING=ON -DBBT_INFRA_STRINGENT_DEBUG=ON \
    -DBOOST_ROOT="$BOOST_PREFIX" -DCMAKE_PREFIX_PATH="$BOOST_PREFIX" \
    $LAUNCHER \
    -DBBT_COROUTINE_SOURCE_DIR="$COROUTINE_DIR" -DBBT_CORE_SOURCE_DIR="$CORE_DIR" \
    -DBBT_HIREDIS_PREFIX="$DEPS_PREFIX/hiredis" \
    -DBBT_MONGOCXX_PREFIX="$DEPS_PREFIX/mongocxx" \
    -DBBT_MONGOC_PREFIX="$DEPS_PREFIX/mongoc"
cmake --build "$DEBUG_BUILD_DIR" --parallel "$JOBS" --target \
    Test_contract_basics Test_http_session_lifetime \
    Test_http_timer_rearm Test_http_inflight_close \
    Test_http_child_unregister Test_redis_unit Test_redis_cotcp_binding Test_mongo_unit \
    Test_rpc_payload

# ---- Debug 旧模块门禁：符号、9 项测试与零跳过 ----
python3 - "$DEBUG_BUILD_DIR" <<'PY'
import pathlib, subprocess, sys
build = sys.argv[1]
for module in ('http', 'redis', 'mongo'):
    archive = pathlib.Path(build) / 'src' / f'libbbt_infra_{module}.a'
    if not archive.exists():
        raise SystemExit(f'FAIL: 缺 Debug 归档 {archive}')
    symbols = subprocess.check_output(['nm', '-C', str(archive)], text=True)
    if 'bbt::infra::debug::' not in symbols:
        raise SystemExit(f'FAIL: Debug {archive} 缺 debug 观测符号（STRINGENT_DEBUG 未生效）')
print('Debug 归档: 三模块均含 debug 观测符号 (ok)')
PY
RX='^(contract\.basics|http\.session_lifetime|http\.timer_rearm|http\.inflight_close|http\.child_unregister|redis\.unit|redis\.cotcp_binding|mongo\.unit|rpc\.payload)$'
bash "$SCRIPT_DIR/run_ctest_label.sh" \
    --build-dir "$DEBUG_BUILD_DIR" \
    --regex "$RX" \
    --timeout 180 \
    --junit "$DEBUG_BUILD_DIR/diagnostics.xml" \
    --require-count 9 \
    --require-file "$SCRIPT_DIR/required_debug_legacy.txt"

# ---- 新 RPC/HTTP Debug 编译与完整契约门禁 ----
RPC_BUILD_DIR="${BUILD_DIR}-rpc-debug"
cmake -S "$REPO_DIR" -B "$RPC_BUILD_DIR" -G Ninja \
    -DCMAKE_BUILD_TYPE=Debug $LAUNCHER \
    -DBUILD_TESTING=ON -DBBT_INFRA_HTTP_NET=ON -DBBT_INFRA_RPC_NET=ON \
    -DBOOST_ROOT="$BOOST_PREFIX" -DCMAKE_PREFIX_PATH="$BOOST_PREFIX" \
    -DBBT_COROUTINE_SOURCE_DIR="$COROUTINE_DIR" -DBBT_PROTOBUF_PREFIX="$PROTOBUF_PREFIX"
cmake --build "$RPC_BUILD_DIR" --parallel "$JOBS" --target \
    bbt_infra_rpc_net_header_selfcheck bbt_infra_http_net_header_selfcheck \
    Test_rpc_net_wire Test_rpc_net_roundtrip Test_rpc_net_faults Test_rpc_net_lifetime rpc_net_example \
    Test_http_net_contract Test_http_net_regressions Test_http_net_streaming Test_http_net_server Test_http_net_stop_regressions http_net_example
bash "$SCRIPT_DIR/run_ctest_label.sh" \
    --build-dir "$RPC_BUILD_DIR" \
    --regex '^(rpc|http)\.net\.' \
    --timeout 180 \
    --junit "$RPC_BUILD_DIR/rpc-http.xml" \
    --require-file "$SCRIPT_DIR/required_rpc_http_net.txt"
