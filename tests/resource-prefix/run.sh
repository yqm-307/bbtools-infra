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
#
# 用法：
#   tests/resource-prefix/run.sh [--prefix <prepare_resource_deps.sh 产出的前缀根>]
# 不给 --prefix 时只跑 A/B（不需要已构建的前缀）。
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
