#!/usr/bin/env bash
#
# 影子构建 + ctest 门禁配方（与现役 .github/workflows/ci.yml 的 build-and-test job
# **等价**，只是运行在 hosted ubuntu-24.04 且无缓存）。本脚本不改现役 ci.yml；两者
# 的关键判据由 scripts/ci/tests/run_tests.py 的耦合测试逐项对照，防漂移。
#
# 覆盖（与现役判据一致）：
#   1) resource-prefix fail-closed 回归 A/B（tests/resource-prefix/run.sh，不带 --prefix；
#      C/D 需真实前缀，本 gate 与现役一致**不运行**，如实声明）；
#   2) hiredis / mongoc / mongocxx 私有前缀：经本仓既有唯一 recipe
#      scripts/prepare_resource_deps.sh 以固定 tag+SHA 源码构建（不写 /usr/local）；
#   3) 记录上游 coroutine/core 实际 HEAD；
#   4) 干净性预检（无 /usr/local 兜底 bbt 产物）；
#   5) cmake 配置（Release, BUILD_TESTING=ON, coroutine/core 源码接入, hire/mongo 前缀,
#      锁定 Boost 前缀）+ Ninja 构建（固定 JOBS，不用 bare nproc）；
#   6) ctest -N 断言 redis.unit / mongo.unit 已注册（缺前缀/target 即早失败）；
#   7) ctest -j1 --output-on-failure --timeout 180（与现役同一命令）；
#   8) live skipped 如实报告：仅允许已知无容器 live 组被标 Skipped；出现**意外 skipped**
#      （非 live 测试被跳过）即失败，不把跳过当通过；
#   9) 依赖来源断言：libbbt_coroutine.so 必检、libbbt_core.so 仅告警、关键 test 二进制 ldd。
#
# 用法：
#   scripts/ci/run_infra_gate.sh --infra <dir> --coroutine <dir> --core <dir> \
#     --boost-prefix <dir> --resource-deps-root <empty-dir> --build-dir <dir> [--jobs N]
set -euo pipefail

INFRA=""; COROUTINE=""; CORE=""; BOOST_PREFIX=""; RES_ROOT=""; BUILD_DIR=""; JOBS=""
while [ $# -gt 0 ]; do
    case "$1" in
        --infra) INFRA="${2:?--infra 需要目录}"; shift 2 ;;
        --coroutine) COROUTINE="${2:?--coroutine 需要目录}"; shift 2 ;;
        --core) CORE="${2:?--core 需要目录}"; shift 2 ;;
        --boost-prefix) BOOST_PREFIX="${2:?--boost-prefix 需要目录}"; shift 2 ;;
        --resource-deps-root) RES_ROOT="${2:?--resource-deps-root 需要目录}"; shift 2 ;;
        --build-dir) BUILD_DIR="${2:?--build-dir 需要目录}"; shift 2 ;;
        --jobs) JOBS="${2:?--jobs 需要数值}"; shift 2 ;;
        -h|--help) sed -n '3,30p' "$0"; exit 0 ;;
        *) echo "[infra-gate] FATAL: 未知参数: $1" >&2; exit 2 ;;
    esac
done
for kv in "infra=$INFRA" "coroutine=$COROUTINE" "core=$CORE" "boost-prefix=$BOOST_PREFIX" \
          "resource-deps-root=$RES_ROOT" "build-dir=$BUILD_DIR"; do
    [ -n "${kv#*=}" ] || { echo "[infra-gate] FATAL: 缺参数 --${kv%%=*}" >&2; exit 2; }
done
[ -n "$JOBS" ] || JOBS=3
case "$JOBS" in ''|*[!0-9]*) echo "[infra-gate] FATAL: --jobs 必须为正整数" >&2; exit 2 ;; esac
[ -f "$INFRA/CMakeLists.txt" ] || { echo "[infra-gate] FATAL: --infra 无效: $INFRA" >&2; exit 2; }
[ -f "$COROUTINE/CMakeLists.txt" ] || { echo "[infra-gate] FATAL: --coroutine 无效: $COROUTINE" >&2; exit 2; }
[ -d "$BOOST_PREFIX" ] || { echo "[infra-gate] FATAL: --boost-prefix 不存在: $BOOST_PREFIX" >&2; exit 2; }

log() { printf '[infra-gate] %s\n' "$*"; }

# 已知无容器即跳过的 live 组（README「CI 覆盖边界」：live 未纳入 CI，按 env 驱动）。
EXPECTED_LIVE_SKIPS="redis.live mongo.live redis.cotcp_binding.live"

# --- 1) resource-prefix fail-closed 回归 A/B（不含 C/D） ----------------------
# 与现役同范围：A) 已存在目录/悬浮符号链接/锁占用拒绝；B) 克隆失败清理；
# 不联网、不跑 docker、不依赖已构建前缀。C/D 需真实前缀，本 gate 与现役一致不运行。
log "资源前缀拒绝路径回归（A/B，不含 C/D）"
echo "范围：A(fail-closed 拒绝) + B(克隆失败清理)；C/D 需真实前缀，本次不运行。"
bash "$INFRA/tests/resource-prefix/run.sh"

# --- 2) 固定源资源前缀（hiredis/mongoc/mongocxx）-----------------------------
# 复用本仓唯一 recipe；固定 tag+SHA 源码构建，不写 /usr/local、不装系统包。
log "构建 hiredis/mongoc/mongocxx 私有前缀（scripts/prepare_resource_deps.sh）"
bash "$INFRA/scripts/prepare_resource_deps.sh" "$RES_ROOT" --jobs "$JOBS"
HIREDIS_PREFIX="$RES_ROOT/hiredis"
MONGOC_PREFIX="$RES_ROOT/mongoc"
MONGOCXX_PREFIX="$RES_ROOT/mongocxx"

# --- 3) 记录上游依赖版本 ------------------------------------------------------
log "上游依赖版本"
git -C "$COROUTINE" log -1 --format='coroutine main HEAD: %H %s (%ci)'
git -C "$CORE" log -1 --format='core master HEAD: %H %s (%ci)'

# --- 4) 干净性预检：无 /usr/local 兜底产物 ------------------------------------
echo "=== /usr/local 下的 bbt 痕迹（应为空）==="
find /usr/local/include/bbt /usr/local/lib -name '*bbt*' 2>/dev/null || true
echo "=== cmake/编译器版本 ==="
cmake --version | head -1
g++ --version | head -1
ninja --version
echo "=== 锁定 Boost ==="
awk '$1=="#define" && $2=="BOOST_VERSION"{print "BOOST_VERSION="$3; exit}' \
    "$BOOST_PREFIX/include/boost/version.hpp"

# --- 5) 配置 + 构建（clean，无缓存）------------------------------------------
log "配置（Release, BUILD_TESTING=ON, JOBS=$JOBS, 无缓存）"
rm -rf "$BUILD_DIR" && mkdir -p "$BUILD_DIR"
cmake -S "$INFRA" -B "$BUILD_DIR" -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTING=ON \
    -DBOOST_ROOT="$BOOST_PREFIX" \
    -DCMAKE_PREFIX_PATH="$BOOST_PREFIX" \
    -DBBT_COROUTINE_SOURCE_DIR="$COROUTINE" \
    -DBBT_CORE_SOURCE_DIR="$CORE" \
    -DBBT_HIREDIS_PREFIX="$HIREDIS_PREFIX" \
    -DBBT_MONGOCXX_PREFIX="$MONGOCXX_PREFIX" \
    -DBBT_MONGOC_PREFIX="$MONGOC_PREFIX"
log "构建（--parallel $JOBS）"
cmake --build "$BUILD_DIR" --parallel "$JOBS"

# --- 6) unit 注册断言（缺前缀/target 即早失败，不进入 ctest 漏判）------------
log "断言 redis.unit / mongo.unit 已注册"
for t in redis.unit mongo.unit; do
    if ! ctest --test-dir "$BUILD_DIR" -N -R "^${t}$" | grep -q "Test.*: ${t}$"; then
        echo "[infra-gate] FATAL: '${t}' 未注册（依赖前缀/模块 target 缺失）" >&2
        exit 1
    fi
done

# --- 7) ctest -j1（与现役同一命令）-------------------------------------------
log "ctest -j1 --output-on-failure --timeout 180"
CTEST_LOG="$BUILD_DIR/ctest-shadow.log"
set +e
ctest --test-dir "$BUILD_DIR" -j1 --output-on-failure --timeout 180 | tee "$CTEST_LOG"
ctest_rc=${PIPESTATUS[0]}
set -e
[ "$ctest_rc" -eq 0 ] || { echo "[infra-gate] FATAL: ctest 退出码 $ctest_rc" >&2; exit "$ctest_rc"; }

# --- 8) live skipped 如实报告（意外 skipped 不得绿）---------------------------
log "核对跳过集（只允许已知无容器 live 组）"
python3 - "$CTEST_LOG" "$EXPECTED_LIVE_SKIPS" <<'PY'
import re
import sys

log_path, expected = sys.argv[1], sys.argv[2].split()
text = open(log_path, encoding="utf-8", errors="replace").read()
skipped = re.findall(r"Test\s+#\d+:\s+(\S+)\s+\.*\*\*\*Skipped", text)
skipped = sorted(set(skipped))
print("[infra-gate] skipped 测试: " + (", ".join(skipped) if skipped else "(无)"))
unexpected = [name for name in skipped if name not in expected]
if unexpected:
    print("[infra-gate] FATAL: 出现意外 skipped（非已知 live 组）: " + ", ".join(unexpected),
          file=sys.stderr)
    sys.exit(1)
if skipped:
    print("[infra-gate] 注：live 组因无容器后端被标 Skipped，**不冒充已通过**，"
          "不计入 build 门禁的绿色覆盖。")
PY

# --- 9) 依赖来源断言（证明未吃 /usr/local 旧产物）---------------------------
log "依赖来源核查"
cd "$BUILD_DIR"
echo "=== bbt 源码依赖产物 ==="
find . \( -name 'libbbt_core.so*' -o -name 'libbbt_coroutine.so*' \) -exec ls -la {} \;
test -n "$(find . -name 'libbbt_coroutine.so*' -print -quit)" \
    || { echo "[infra-gate] FATAL: 未见 libbbt_coroutine.so" >&2; exit 1; }
if [ -z "$(find . -name 'libbbt_core.so*' -print -quit)" ]; then
    echo "WARN: 未见 libbbt_core.so（P2 后预期内）"
fi
echo "=== test 二进制 ldd（关键依赖）==="
for bin in tests/Test_contract_basics tests/Test_http_loopback tests/Test_http_lifecycle \
           tests/Test_redis_unit tests/Test_mongo_unit; do
    [ -x "$bin" ] || continue
    echo "--- $bin ---"
    ldd "$bin" | grep -E 'bbt|boost|libevent|hiredis|mongo|bson' || echo "(静态/无外部依赖)"
done

log "门禁通过（构建 + ctest -j1；live 组如实标 Skipped）"
