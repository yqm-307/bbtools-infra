#!/usr/bin/env bash
# required 门禁结果汇聚（fail-closed）。
#
# 旧 required check 名 "编译 & ctest" 改由本 always() 汇聚 job 承载，避免改 GitHub
# branch rule；只有全部门禁 config 真实 success 才 green——**不用 needs 跳过当通过**。
#   - changes 必须 success（否则 conservative 拒绝）；
#   - code=true ：prepare-deps / release / debug 均必须 success（skipped/cancelled=失败）；
#   - code=false：docs-only，三者应显式 skipped，required check 如实报成功；
#   - code 取值非法：保守失败，不擅自判绿。
#
# 用法：
#   scripts/ci/required_result_gate.sh --code <true|false> \
#     --changes <result> --prepare <result> --release <result> --debug <result>
set -euo pipefail

code=""; changes=""; prepare=""; release=""; debug=""
while [ $# -gt 0 ]; do case "$1" in
  --code) code="$2"; shift 2 ;;
  --changes) changes="$2"; shift 2 ;;
  --prepare) prepare="$2"; shift 2 ;;
  --release) release="$2"; shift 2 ;;
  --debug) debug="$2"; shift 2 ;;
  *) echo "[required-gate] FATAL: 未知参数 $1" >&2; exit 2 ;;
esac; done

fail() { echo "[required-gate] FAIL: $*" >&2; exit 1; }

echo "[required-gate] code=$code changes=$changes prepare=$prepare release=$release debug=$debug"

[ "$changes" = "success" ] || fail "changes job 必须 success（实际 '$changes'）"

if [ "$code" = "true" ]; then
  [ "$prepare" = "success" ] || fail "prepare-deps 必须 success（实际 '$prepare'）—— 跳过/取消不算通过"
  [ "$release" = "success" ] || fail "Release 必须 success（实际 '$release'）"
  [ "$debug" = "success" ] || fail "Debug 必须 success（实际 '$debug'）"
elif [ "$code" = "false" ]; then
  for kv in "prepare=$prepare" "release=$release" "debug=$debug"; do
    [ "${kv#*=}" = "skipped" ] || fail "docs-only 期望 ${kv%%=*} skipped（实际 '${kv#*=}'）"
  done
  echo "[required-gate] docs-only：构建门禁显式 skipped，required check 如实报成功。"
else
  fail "changes.code 取值非法 '$code'（保守拒绝）"
fi

echo "[required-gate] PASS"
