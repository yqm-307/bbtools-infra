#!/usr/bin/env bash
# 读取并校验 coroutine pin 真源（cmake/coroutine.sha，40hex 单行）。
# CMake/CI 都从这里读，不维护第二份字面量。格式不符（非恰好 40 个
# 小写 hex）即 fail-closed——pin 文件本身是门禁的一部分。
#
# 用法：verify_coroutine_pin.sh <pin-file>
# 输出：stdout 与 $GITHUB_OUTPUT 写 coroutine_pin_sha=<40hex>
set -euo pipefail

PIN_FILE="${1:?usage: verify_coroutine_pin.sh <path-to-coroutine.sha>}"

pin="$(python3 - "$PIN_FILE" <<'PY'
import pathlib, re, sys
raw = pathlib.Path(sys.argv[1]).read_bytes()
assert re.fullmatch(rb"[0-9a-f]{40}\n?", raw), "invalid coroutine.sha"
sys.stdout.write(raw[:40].decode("ascii"))
PY
)"
if ! printf '%s' "$pin" | grep -Eq '^[0-9a-f]{40}$'; then
    echo "FAIL: ${PIN_FILE} 非法（'$pin'），要求恰好 40 个小写 hex" >&2
    exit 1
fi
echo "coroutine_pin_sha=${pin}" | tee "${GITHUB_OUTPUT:-/dev/stdout}"
