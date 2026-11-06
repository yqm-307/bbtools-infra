#!/usr/bin/env bash
# 本地文档导出入口：Doxygen 只解析一次，同时产出两份文档。
#   $DOCS_OUT/site/       人看：HTML（站点根）；其下 ai/ 是同一份 AI 版
#   $DOCS_OUT/ai/         AI 看：分层 Markdown（L0 index.md → L1 modules → L2 symbols）
#   $DOCS_OUT/warnings.log  Doxygen 告警（缺注释等）
# 环境变量（均可选）：
#   DOCS_OUT              输出目录，默认 <repo>/build-docs（已被 build*/ 忽略）；
#                         只允许位于 <repo>/build/ 或 <repo>/build-*/ 下，否则拒绝（因为会 rm -rf）
#   DOXYGEN_AWESOME_CSS   doxygen-awesome.css 的路径；缺省用 Doxygen 默认样式
#   DOCS_WERROR=1         有任何 Doxygen 告警即失败（缺注释门禁；默认只告警）
#   DOCS_VERSION          显式版本标签；缺省按 HEAD + 工作区内容指纹生成
set -euo pipefail

root=$(cd "$(dirname "$0")/../.." && pwd)
cd "$root"
cfg=scripts/docs/modules.json
pinned_doxygen=1.15.0

command -v doxygen >/dev/null || { echo "缺少 doxygen" >&2; exit 2; }
command -v dot >/dev/null || echo "警告: 缺少 graphviz(dot)，类图将缺失" >&2

# 输出目录规范化：解符号链接后取绝对路径，guard 判断、rm -rf、mkdir 与 Doxygen 全部只用这一个路径。
# 只看 abspath 不足以证明安全（指向仓库外的符号链接仍会通过），因此必须 realpath。
root_real=$(python3 -I -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$root")
out_real=$(python3 -I -c 'import os,sys; print(os.path.realpath(os.path.abspath(sys.argv[1])))' \
    "${DOCS_OUT:-$root/build-docs}")
case "$out_real" in
    "$root_real"/build/* | "$root_real"/build-*) ;;
    *)
        echo "DOCS_OUT 必须位于 $root_real/build/ 或 $root_real/build-*/ 之下，当前: $out_real" >&2
        exit 2
        ;;
esac
export DOCS_OUT=$out_real

export DOCS_PROJECT=$(python3 -I -c 'import json,sys; print(json.load(open(sys.argv[1]))["project"])' "$cfg")

# 版本标签：干净树上用 HEAD；有未提交改动时追加内容指纹，使版本号能对应到实际候选内容
# （提交后可用该指纹核对产物是同一内容，而不是只标一个 base HEAD）。
content_fingerprint() {
    {
        git diff HEAD 2>/dev/null || true
        git ls-files -o --exclude-standard 2>/dev/null | sort | while IFS= read -r file; do
            printf '=== %s\n' "$file"
            cat -- "$file"
        done
    } | sha256sum | cut -c1-12
}
base_version=$(git rev-parse --short=12 HEAD 2>/dev/null || echo unknown)
if [ -n "$(git status --porcelain 2>/dev/null)" ]; then
    base_version="${base_version}+wip.$(content_fingerprint)"
fi
export DOCS_VERSION=${DOCS_VERSION:-$base_version}
export DOXYGEN_AWESOME_CSS=${DOXYGEN_AWESOME_CSS:-}

doxy_ver=$(doxygen --version | awk '{print $1}')
echo "doxygen $doxy_ver · project=$DOCS_PROJECT · version=$DOCS_VERSION"
if [ "$doxy_ver" != "$pinned_doxygen" ]; then
    echo "警告: 实际 doxygen $doxy_ver 与文档约定的 $pinned_doxygen 不同，XML/HTML 可能不同" >&2
fi
rm -rf "$DOCS_OUT"
mkdir -p "$DOCS_OUT"

doxygen scripts/docs/Doxyfile
python3 -I scripts/docs/export_ai_docs.py "$DOCS_OUT/xml" "$DOCS_OUT/ai" "$cfg"

# 站点：HTML 在根，AI 版放 /ai/；.nojekyll 防止 Pages 把 .md 当 Jekyll 源处理
mkdir -p "$DOCS_OUT/site"
cp -r "$DOCS_OUT/html/." "$DOCS_OUT/site/"
cp -r "$DOCS_OUT/ai" "$DOCS_OUT/site/ai"
touch "$DOCS_OUT/site/.nojekyll"

warn=$(grep -c . "$DOCS_OUT/warnings.log" 2>/dev/null || true)
echo "doxygen 告警: ${warn:-0} 条（$DOCS_OUT/warnings.log）"
if [ "${DOCS_WERROR:-0}" = 1 ] && [ "${warn:-0}" -gt 0 ]; then
    echo "DOCS_WERROR=1：存在 Doxygen 告警，失败" >&2
    exit 1
fi
