#!/usr/bin/env bash
# 只读检查全仓或暂存区的 C/C++ 格式，不修改工作区或索引。
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

case "$#:${1:-}" in
    0:) scope=--all ;;
    1:--staged) scope=--staged ;;
    *) printf '用法：%s [--staged]\n' "$0" >&2; exit 2 ;;
esac

formatter=third-party/bbtools-pub/tools/format/format-cpp.py
if [ ! -f "$formatter" ]; then
    printf 'check_format: 缺少 %s，请核对 bbtools-pub 子模块版本与检出状态\n' "$formatter" >&2
    exit 2
fi
exec python3 -I "$formatter" --check "$scope"
