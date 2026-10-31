#!/usr/bin/env bash
# Release job 的构建+全量 ctest+新 RPC/HTTP 契约+HTTP-only+符号断言+ldd 门禁编排。
# 等价现役 ci.yml release job 的各 step；假定 prepared deps 已解包、
# 资源前缀 A/B 已由调用方跑过、ccache 开关已由 resolve_ccache.sh 解析。
#
# 输入（env）：
#   REPO_DIR        infra 检出根（含 scripts/、tests/）
#   WORKSPACE       工作区根（含 deps-bundle 解包产物 coroutine/ deps-prefix/ 等）
#   BUILD_DIR       Release 构建目录（脚本内 rm -rf 重建）
#   BOOST_PREFIX    锁定 Boost 私有前缀
#   DEPS_PREFIX     hiredis/mongoc/mongocxx 私有前缀根
#   PROTOBUF_PREFIX 锁定 protobuf 3.21.12 私有前缀
#   COROUTINE_DIR   coroutine 源码树
#   CORE_DIR        bbtools-core 源码树
#   JOBS            构建并行度
#   LAUNCHER        ccache launcher（由 resolve_ccache.sh 解析后传入；空=不启用）
set -euo pipefail

: "${REPO_DIR:?need REPO_DIR}" "${WORKSPACE:?need WORKSPACE}" \
  "${BUILD_DIR:?need BUILD_DIR}" "${BOOST_PREFIX:?need BOOST_PREFIX}" \
  "${DEPS_PREFIX:?need DEPS_PREFIX}" "${PROTOBUF_PREFIX:?need PROTOBUF_PREFIX}" \
  "${COROUTINE_DIR:?need COROUTINE_DIR}" "${CORE_DIR:?need CORE_DIR}" \
  "${JOBS:?need JOBS}"
LAUNCHER="${LAUNCHER:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---- 干净性预检 ----
echo "=== /usr/local 下的 bbt 痕迹（应为空）==="
find /usr/local/include/bbt /usr/local/lib -name '*bbt*' 2>/dev/null || true
echo "=== cmake/编译器版本 ==="
cmake --version | head -1
g++ --version | head -1
ninja --version
echo "=== 锁定 Boost ==="
awk '$1=="#define" && $2=="BOOST_VERSION"{print "BOOST_VERSION="$3; exit}' \
    "$BOOST_PREFIX/include/boost/version.hpp"

# ---- Release 主构建 + 全量 ctest ----
rm -rf "$BUILD_DIR" && mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"
cmake "$REPO_DIR" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTING=ON \
    -DBOOST_ROOT="$BOOST_PREFIX" \
    -DCMAKE_PREFIX_PATH="$BOOST_PREFIX" \
    $LAUNCHER \
    -DBBT_COROUTINE_SOURCE_DIR="$COROUTINE_DIR" \
    -DBBT_CORE_SOURCE_DIR="$CORE_DIR" \
    -DBBT_HIREDIS_PREFIX="$DEPS_PREFIX/hiredis" \
    -DBBT_MONGOCXX_PREFIX="$DEPS_PREFIX/mongocxx" \
    -DBBT_MONGOC_PREFIX="$DEPS_PREFIX/mongoc"
cmake --build . --parallel "$JOBS"
if [ -n "$LAUNCHER" ] && command -v ccache >/dev/null 2>&1; then
    echo "=== ccache 统计（编译后） ==="
    ccache --show-stats || true
fi

# 单测注册断言：若模块 target 缺失（前缀未传递或 find_package 失败）导致
# redis.unit/mongo.unit/rpc.payload 没注册，这里直接非零失败，不进入 ctest 漏判。
for t in redis.unit mongo.unit rpc.payload; do
    if ! ctest -N -R "^${t}$" | grep -q "Test.*: ${t}$"; then
        echo "FAIL: '${t}' 未注册（依赖前缀/模块 target 缺失）"
        exit 1
    fi
done
# --timeout 只作用于无 TIMEOUT 属性的测试；http.loopback/http.lifecycle 的
# TIMEOUT 60/120 测试属性优先级更高。保持 180：contract.basics 无实测耗时证据
# 就收紧会造成门禁 flake。
ctest -j1 --output-on-failure --timeout 180

# ---- 新 RPC/HTTP Release 编译与完整契约门禁 ----
RPC_BUILD_DIR="${BUILD_DIR}-rpc-release"
RPC_LAUNCHER=""
[ -n "$LAUNCHER" ] && RPC_LAUNCHER="$LAUNCHER"
cmake -S "$REPO_DIR" -B "$RPC_BUILD_DIR" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release $RPC_LAUNCHER \
    -DBUILD_TESTING=ON -DBBT_INFRA_HTTP_NET=ON -DBBT_INFRA_RPC_NET=ON \
    -DBOOST_ROOT="$BOOST_PREFIX" -DCMAKE_PREFIX_PATH="$BOOST_PREFIX" \
    -DBBT_COROUTINE_SOURCE_DIR="$COROUTINE_DIR" -DBBT_PROTOBUF_PREFIX="$PROTOBUF_PREFIX"
cmake --build "$RPC_BUILD_DIR" --parallel "$JOBS" --target \
    bbt_infra_rpc_net_header_selfcheck bbt_infra_http_net_header_selfcheck \
    Test_rpc_net_wire Test_rpc_net_roundtrip Test_rpc_net_faults Test_rpc_net_lifetime rpc_net_example \
    Test_http_net_contract Test_http_net_regressions Test_http_net_server Test_http_net_stop_regressions Test_http_net_streaming http_net_example
bash "$SCRIPT_DIR/run_ctest_label.sh" \
    --build-dir "$RPC_BUILD_DIR" \
    --regex '^(rpc|http)\.net\.' \
    --timeout 180 \
    --junit "$RPC_BUILD_DIR/rpc-http.xml" \
    --require-file "$SCRIPT_DIR/required_rpc_http_net.txt"

# ---- HTTP-only 无 protobuf 依赖及 RPC 缺前缀拒绝门禁 ----
NO_WIRE_BUILD_DIR="${BUILD_DIR}-http-no-wire"
NO_WIRE_LAUNCHER=""
[ -n "$LAUNCHER" ] && NO_WIRE_LAUNCHER="$LAUNCHER"
cmake -S "$REPO_DIR" -B "$NO_WIRE_BUILD_DIR" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release $NO_WIRE_LAUNCHER \
    -DBUILD_TESTING=ON -DBBT_INFRA_HTTP_NET=ON -DBBT_INFRA_RPC_NET=OFF \
    -DBBT_PROTOBUF_PREFIX= -DBOOST_ROOT="$BOOST_PREFIX" -DCMAKE_PREFIX_PATH="$BOOST_PREFIX" \
    -DBBT_COROUTINE_SOURCE_DIR="$COROUTINE_DIR"
cmake --build "$NO_WIRE_BUILD_DIR" --parallel "$JOBS" --target Test_http_net_contract
ctest --test-dir "$NO_WIRE_BUILD_DIR" -N --show-only=json-v1 > "$NO_WIRE_BUILD_DIR/registered.json"
python3 - "$NO_WIRE_BUILD_DIR" <<'PY'
import json, pathlib, subprocess, sys
build = pathlib.Path(sys.argv[1])
names = [t['name'] for t in json.loads((build / 'registered.json').read_text())['tests']]
if any(n.startswith('rpc.') for n in names):
    raise SystemExit('FAIL: HTTP-only unexpectedly registers RPC')
archives = list(build.rglob('libbbt_infra_http_net.a'))
if len(archives) != 1:
    raise SystemExit('FAIL: HTTP archive missing or duplicated')
syms = subprocess.check_output(['nm', '-C', str(archives[0])], text=True)
if any(s in syms for s in ('protobuf', 'rpc_envelope')):
    raise SystemExit('FAIL: HTTP archive depends on protobuf')
PY
ctest --test-dir "$NO_WIRE_BUILD_DIR" -j1 -R '^http\.net\.get_post_reuse$' \
    --no-tests=error --output-on-failure
REJECT_DIR="${BUILD_DIR}-rpc-missing-prefix"
mkdir -p "$REJECT_DIR"
if cmake -S "$REPO_DIR" -B "$REJECT_DIR" -G Ninja \
    -DBBT_INFRA_HTTP_NET=ON -DBBT_INFRA_RPC_NET=ON -DBBT_PROTOBUF_PREFIX= \
    -DBOOST_ROOT="$BOOST_PREFIX" -DCMAKE_PREFIX_PATH="$BOOST_PREFIX" \
    -DBBT_COROUTINE_SOURCE_DIR="$COROUTINE_DIR" > "$REJECT_DIR/rejection.log" 2>&1; then
    echo 'FAIL: RPC without locked protobuf unexpectedly configured' >&2
    exit 1
fi
grep -q '需要显式 BBT_PROTOBUF_PREFIX' "$REJECT_DIR/rejection.log"

# ---- Release 侧观测符号不变式：三份归档必须存在且无 debug 观测符号 ----
python3 - "$BUILD_DIR" <<'PY'
import pathlib, subprocess, sys
build = sys.argv[1]
for module in ('http', 'redis', 'mongo'):
    archive = pathlib.Path(build) / 'src' / f'libbbt_infra_{module}.a'
    if not archive.exists():
        raise SystemExit(f'FAIL: 缺 Release 归档 {archive}')
    symbols = subprocess.check_output(['nm', '-C', str(archive)], text=True)
    if 'bbt::infra::debug::' in symbols:
        raise SystemExit(f'FAIL: Release {archive} 含 debug 观测符号（Release 必须无符号）')
print('Release 归档: 三模块均 0 debug 符号 (ok)')
PY

# ---- 依赖来源核查 ----
cd "$BUILD_DIR"
echo "=== bbt 源码依赖产物 ==="
find . \( -name 'libbbt_core.so*' -o -name 'libbbt_coroutine.so*' \) -exec ls -la {} \;
# coroutine 已自闭环、不再产出 libbbt_core.so：libbbt_coroutine.so 必检，
# libbbt_core.so 仅告警，不再卡门禁。
test -n "$(find . -name 'libbbt_coroutine.so*' -print -quit)"
if [ -z "$(find . -name 'libbbt_core.so*' -print -quit)" ]; then
    echo "WARN: 未见 libbbt_core.so（coroutine 自闭环预期内）"
fi
echo "=== test 二进制 ldd（关键依赖）==="
for bin in tests/Test_contract_basics tests/Test_http_loopback tests/Test_http_lifecycle \
           tests/Test_redis_unit tests/Test_mongo_unit; do
    [ -x "$bin" ] || continue
    echo "--- $bin ---"
    ldd "$bin" | grep -E 'bbt|boost|libevent|hiredis|mongo|bson' || echo "(静态/无外部依赖)"
done
