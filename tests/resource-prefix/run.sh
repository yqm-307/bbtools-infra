#!/usr/bin/env bash
#
# 资源前缀 recipe / 消费者自检的最小回归入口（不联网、不重复三方依赖构建）。
#
#   A) prepare_resource_deps.sh 的 fail-closed 路径必须拒绝：已存在目录、悬浮符号
#      链接（realpath -m 会遮掉）、被其他进程占用的锁目录
#   B) 克隆失败后的清理：不留下半个前缀，锁目录必须被释放
#   C) 消费者自检正向：--prefix 指定的真实前缀必须构建 + 运行 + ldd 来源校验通过
#   D) 消费者自检负向：复制真实前缀并只把 hiredis 版本头改成 1.3.0 后，必须失败，
#      且失败原因是版本不符（不是别的构建错误）
#   E) 提供 BBT_COROUTINE_SOURCE_DIR 时，验证 Mongo 各版本目录 0/多/非目录
#      命中的拒绝诊断；同时给 --prefix 时验证真实单命中 configure 成功
#
# 用法：
#   tests/resource-prefix/run.sh [--prefix <prepare_resource_deps.sh 产出的前缀根>]
# 不给 --prefix 时运行 A/B；有 coroutine 源码前置时另跑 E 的拒绝路径。
# E 不构建目标；未提供前置时明确跳过，不把整体 exit 0 当成 E 已通过。
#
# 说明：D 用 cp -al 硬链接复制真实前缀（不重编依赖），再换掉复制件的 hiredis 版本头，
# 属于「异版」负向 fixture，不是又一次真实 1.3.0 三方构建。
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$DIR/../.." && pwd)"
RECIPE="$REPO/scripts/prepare_resource_deps.sh"
VERIFY_SRC="$REPO/scripts/resource_prefix_verify"

usage() {
    cat <<'EOF'
用法：tests/resource-prefix/run.sh [--prefix <prefix-root>]
  --prefix <prefix-root>  真实前缀根（prepare_resource_deps.sh 的 install_root），
                          给了才跑正向(C)与异版负向(D)。
EOF
}

PREFIX_ROOT=""
while [ $# -gt 0 ]; do
    case "$1" in
        --prefix) PREFIX_ROOT="${2:?--prefix 需要参数}"; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "[resource-prefix-regress] FATAL: 未知参数: $1" >&2; usage >&2; exit 2 ;;
    esac
done
[ -x "$RECIPE" ] || { echo "[resource-prefix-regress] FATAL: 找不到 $RECIPE" >&2; exit 2; }

WORK="$(mktemp -d "${TMPDIR:-/tmp}/bbt-resource-prefix-regress.XXXXXX")"
cleanup() { rm -rf "$WORK"; }
trap cleanup EXIT

fail() { echo "[resource-prefix-regress] FAIL: $*" >&2; exit 1; }
pass() { echo "[resource-prefix-regress] ok: $*"; }

# recipe 的 fail-closed 拒绝路径一律退出码 2，且不得改动目标路径。
expect_refuse() {
    local label="$1"; shift
    local rc=0 out
    out="$("$RECIPE" "$@" 2>&1)" || rc=$?
    [ "$rc" -eq 2 ] || fail "${label}: 期望退出码 2，实际 ${rc}；输出: ${out}"
    pass "${label}: 拒绝（exit 2）"
}

# A1 已存在（空）目录
mkdir -p "$WORK/existing"
expect_refuse "已存在目录" "$WORK/existing"
[ -d "$WORK/existing" ] || fail "已存在目录被删改"
[ ! -e "$WORK/existing.lock" ] || fail "拒绝路径上留下了锁目录"

# A2 悬浮符号链接：realpath -m 会把它解析成「不存在的目标」
ln -s "$WORK/never-created" "$WORK/dangling"
expect_refuse "悬浮符号链接" "$WORK/dangling"
[ -L "$WORK/dangling" ] || fail "悬浮符号链接被删除"
[ ! -e "$WORK/never-created" ] || fail "悬浮符号链接的目标被创建"

# A3 锁目录被占用：必须拒绝，且不得抢走/删掉别人的锁
# （目标前缀本身不存在，否则先被「已存在」那条拦下，测不到锁分支）
mkdir -p "$WORK/held.lock"
expect_refuse "锁已占用" "$WORK/held"
[ -d "$WORK/held.lock" ] || fail "别人的锁目录被删除"
[ ! -e "$WORK/held" ] || fail "锁占用时仍创建了前缀"

# B 克隆失败：非零退出，不留前缀，锁必须释放（否则该路径永久不可用）
SHIM="$WORK/shim"; mkdir -p "$SHIM"
printf '#!/bin/sh\nexit 3\n' > "$SHIM/git"
chmod +x "$SHIM/git"
rc=0
PATH="$SHIM:$PATH" "$RECIPE" "$WORK/clonefail" >"$WORK/clonefail.log" 2>&1 || rc=$?
[ "$rc" -ne 0 ] || fail "克隆失败未返回非零退出码"
[ ! -e "$WORK/clonefail" ] || fail "失败后残留了前缀"
[ ! -e "$WORK/clonefail.lock" ] || fail "失败后锁目录未释放"
pass "克隆失败清理: exit ${rc}，无残留前缀、锁已释放"

# E) Mongo 依赖目录多版本（Issue #12 M3）：真实 configure 的 0/多命中必须
#    fail-closed 且诊断准确（区分「未找到」与「命中多个版本目录」），1 命中
#    必须成功。configure 前置需要 coroutine 源码树与 cmake；未提供 coroutine
#    源码树时跳过，不伪造通过。
M3_CO="${BBT_COROUTINE_SOURCE_DIR:-}"
if [ -z "$M3_CO" ] || [ ! -d "$M3_CO" ]; then
    echo "[resource-prefix-regress] 未提供 BBT_COROUTINE_SOURCE_DIR，跳过 M3 多版本 configure 回归"
elif ! command -v cmake >/dev/null 2>&1; then
    echo "[resource-prefix-regress] 无 cmake，跳过 M3 多版本 configure 回归"
else
    M3="$WORK/m3"
    # 0 命中：前缀内无任何 cmake 版本目录。
    mkdir -p "$M3/zero/mongoc/lib/cmake" "$M3/zero/mongocxx/lib/cmake"
    mkdir -p "$M3/zero-bson/mongoc/lib/cmake/mongoc-2.5.4" \
             "$M3/zero-bson/mongocxx/lib/cmake/bsoncxx-4.6.0" \
             "$M3/zero-bsoncxx/mongoc/lib/cmake/mongoc-2.5.4" \
             "$M3/zero-bsoncxx/mongoc/lib/cmake/bson-2.5.4" \
             "$M3/zero-bsoncxx/mongocxx/lib/cmake"
    # 同名普通文件不能被当作合法包目录，也不能触发系统包搜索兜底。
    for pkg in mongoc bson bsoncxx; do
        fixture="$M3/file-$pkg"
        mkdir -p "$fixture/mongoc/lib/cmake/mongoc-2.5.4" \
                 "$fixture/mongoc/lib/cmake/bson-2.5.4" \
                 "$fixture/mongocxx/lib/cmake/bsoncxx-4.6.0"
        if [ "$pkg" = bsoncxx ]; then
            path="$fixture/mongocxx/lib/cmake/bsoncxx-4.6.0"
        else
            path="$fixture/mongoc/lib/cmake/$pkg-2.5.4"
        fi
        rmdir "$path"
        touch "$path"
    done
    # 多命中（mongoc）：单前缀内两个 mongoc 版本目录。
    mkdir -p "$M3/multi-mongoc/mongoc/lib/cmake/mongoc-2.5.3" \
             "$M3/multi-mongoc/mongoc/lib/cmake/mongoc-2.5.4" \
             "$M3/multi-mongoc/mongoc/lib/cmake/bson-2.5.4" \
             "$M3/multi-mongoc/mongocxx/lib/cmake/bsoncxx-4.6.0"
    # 多命中（bson）：单 mongoc，两个 bson 版本目录。
    mkdir -p "$M3/multi-bson/mongoc/lib/cmake/mongoc-2.5.4" \
             "$M3/multi-bson/mongoc/lib/cmake/bson-2.5.3" \
             "$M3/multi-bson/mongoc/lib/cmake/bson-2.5.4" \
             "$M3/multi-bson/mongocxx/lib/cmake/bsoncxx-4.6.0"
    # 多命中（bsoncxx）：单 mongoc/bson，两个 bsoncxx 版本目录。
    mkdir -p "$M3/multi-bsoncxx/mongoc/lib/cmake/mongoc-2.5.4" \
             "$M3/multi-bsoncxx/mongoc/lib/cmake/bson-2.5.4" \
             "$M3/multi-bsoncxx/mongocxx/lib/cmake/bsoncxx-4.5.0" \
             "$M3/multi-bsoncxx/mongocxx/lib/cmake/bsoncxx-4.6.0"

    # 期望 configure 失败（fail-closed），且输出含诊断 needle。
    expect_configure_refuse() {
        local label="$1" mpfx="$2" mxpfx="$3" needle="$4"
        local rc=0
        cmake -S "$REPO" -B "$M3/build-$label" -G Ninja \
            -DBBT_COROUTINE_SOURCE_DIR="$M3_CO" \
            -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
            -DBBT_MONGOC_PREFIX="$mpfx" -DBBT_MONGOCXX_PREFIX="$mxpfx" \
            >"$M3/$label.log" 2>&1 || rc=$?
        [ "$rc" -ne 0 ] || fail "${label}: configure 未失败（rc=$rc）"
        grep -q -F "$needle" "$M3/$label.log" \
            || fail "${label}: 诊断不含 '${needle}'（见 $M3/$label.log）"
        pass "${label}: configure 拒绝（rc=$rc），诊断含 '${needle}'"
    }

    expect_configure_refuse "m3-zero-hit"      "$M3/zero/mongoc" \
        "$M3/zero/mongocxx" "未找到 lib/cmake/mongoc-*"
    expect_configure_refuse "m3-zero-bson"     "$M3/zero-bson/mongoc" \
        "$M3/zero-bson/mongocxx" "未找到 lib/cmake/bson-*"
    expect_configure_refuse "m3-zero-bsoncxx"  "$M3/zero-bsoncxx/mongoc" \
        "$M3/zero-bsoncxx/mongocxx" "未找到 lib/cmake/bsoncxx-*"
    for pkg in mongoc bson bsoncxx; do
        expect_configure_refuse "m3-file-$pkg" "$M3/file-$pkg/mongoc" \
            "$M3/file-$pkg/mongocxx" "lib/cmake/$pkg-* 命中路径不是目录"
    done
    expect_configure_refuse "m3-multi-mongoc"  "$M3/multi-mongoc/mongoc" \
        "$M3/multi-mongoc/mongocxx" "lib/cmake/mongoc-* 命中 2 个"
    expect_configure_refuse "m3-multi-bson"    "$M3/multi-bson/mongoc" \
        "$M3/multi-bson/mongocxx" "lib/cmake/bson-* 命中 2 个"
    expect_configure_refuse "m3-multi-bsoncxx" "$M3/multi-bsoncxx/mongoc" \
        "$M3/multi-bsoncxx/mongocxx" "lib/cmake/bsoncxx-* 命中 2 个"

    if [ -n "$PREFIX_ROOT" ]; then
        # 1 命中：真实前缀（mongoc/bson/bsoncxx 各恰一个版本目录）必须成功。
        cmake -S "$REPO" -B "$M3/build-one-hit" -G Ninja \
            -DBBT_COROUTINE_SOURCE_DIR="$M3_CO" \
            -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
            -DBBT_MONGOC_PREFIX="$PREFIX_ROOT/mongoc" \
            -DBBT_MONGOCXX_PREFIX="$PREFIX_ROOT/mongocxx" \
            >"$M3/one-hit.log" 2>&1 || fail "m3-one-hit: configure 失败（见 $M3/one-hit.log）"
        pass "m3-one-hit: configure 成功（rc=0）"
    else
        echo "[resource-prefix-regress] 未给 --prefix，跳过 M3 1 命中正向"
    fi
fi

[ -n "$PREFIX_ROOT" ] || { echo "[resource-prefix-regress] 未给 --prefix，跳过 C/D"; exit 0; }
command -v cmake >/dev/null 2>&1 || { echo "[resource-prefix-regress] 无 cmake，跳过 C/D"; exit 0; }
[ -d "$PREFIX_ROOT/hiredis" ] || fail "--prefix 不是资源前缀根: $PREFIX_ROOT"

# C 正向：真实前缀必须通过 imported target + 版本锁定 + ldd 来源校验
cmake -S "$VERIFY_SRC" -B "$WORK/verify-pos" -G Ninja \
    -DRESOURCE_PREFIX_ROOT="$PREFIX_ROOT" >"$WORK/pos-config.log" 2>&1 \
    || fail "正向: configure 失败（见 $WORK/pos-config.log）"
cmake --build "$WORK/verify-pos" >"$WORK/pos-build.log" 2>&1 \
    || fail "正向: 构建/运行验证据失败（见 $WORK/pos-build.log）"
pass "正向: 真实前缀通过（版本锁定 + 链接来源）"

# D 负向：硬链接复制真实前缀，只把 hiredis 版本头改成 1.3.0
NEG="$WORK/prefix-hiredis-1.3.0"
cp -al "$PREFIX_ROOT" "$NEG"
HDR="$NEG/hiredis/include/hiredis/hiredis.h"
[ -f "$HDR" ] || fail "负向: 前缀里没有 hiredis/include/hiredis/hiredis.h"
# 必须先 unlink 再写：硬链接副本上原地改写会连带改掉原前缀的头文件。
python3 - "$HDR" <<'PY'
import os, sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
old = "#define HIREDIS_MINOR 4"
if old not in src:
    raise SystemExit("负向 fixture 失败: 找不到 " + old)
src = src.replace(old, "#define HIREDIS_MINOR 3", 1)
os.unlink(path)
with open(path, "w", encoding="utf-8") as fh:
    fh.write(src)
PY
rc=0
if cmake -S "$VERIFY_SRC" -B "$WORK/verify-neg" -G Ninja \
        -DRESOURCE_PREFIX_ROOT="$NEG" >"$WORK/neg.log" 2>&1 \
   && cmake --build "$WORK/verify-neg" >>"$WORK/neg.log" 2>&1; then
    rc=0
else
    rc=1
fi
[ "$rc" -ne 0 ] || fail "负向: 异版前缀（hiredis 1.3.0）竟然通过了验证据"
grep -q "MISMATCH" "$WORK/neg.log" || fail "负向: 失败原因不是版本不符（见 $WORK/neg.log）"
pass "负向: hiredis 1.3.0 异版前缀被版本锁定检查拦下"

echo "[resource-prefix-regress] 全部通过"
