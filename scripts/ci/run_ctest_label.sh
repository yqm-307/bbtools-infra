#!/usr/bin/env bash
# 按 ctest label 实跑并核对：注册集非空、实跑集合与 -N 选中集一致、零跳过零失败。
# 与现役 ci.yml 的 ctest 判据同口径：-j1 --no-tests=error --output-on-failure，
# --timeout 只对无 TIMEOUT 属性的用例生效（测试属性优先）。
#
# 用法：
#   run_ctest_label.sh --build-dir <dir> [--label-expr 'smoke|unit'] [--regex '^net\.'] \
#       [--timeout 120] [--junit <file>] [--require-count N] [--require-file <file>]
#   run_ctest_label.sh --build-dir <dir> --repeat 'until-fail:5' --regex '^x$' ...
#
# --require-file：一个测试名/行的清单文件；registered 中按 --regex 选中后
#   必须包含其中每一项（空行与 # 注释忽略）。
# --require-count N：registered 中按 --regex 选中数必须 >= N。
set -euo pipefail

BUILD_DIR=""
LABEL_EXPR=""
REGEX=""
TIMEOUT="120"
JUNIT=""
REQUIRE_COUNT=""
REQUIRE_FILE=""
REPEAT=""

while [ $# -gt 0 ]; do
    case "$1" in
        --build-dir) BUILD_DIR="$2"; shift 2 ;;
        --label-expr) LABEL_EXPR="$2"; shift 2 ;;
        --regex) REGEX="$2"; shift 2 ;;
        --timeout) TIMEOUT="$2"; shift 2 ;;
        --junit) JUNIT="$2"; shift 2 ;;
        --require-count) REQUIRE_COUNT="$2"; shift 2 ;;
        --require-file) REQUIRE_FILE="$2"; shift 2 ;;
        --repeat) REPEAT="$2"; shift 2 ;;
        *) echo "[ctest-label] FATAL: 未知参数 $1" >&2; exit 2 ;;
    esac
done

[ -n "$BUILD_DIR" ] || { echo "[ctest-label] FATAL: 缺 --build-dir" >&2; exit 2; }

REGISTERED="$BUILD_DIR/registered.json"
SELECT_ARGS=()
[ -n "$LABEL_EXPR" ] && SELECT_ARGS+=(-L "$LABEL_EXPR")
[ -n "$REGEX" ] && SELECT_ARGS+=(-R "$REGEX")
ctest --test-dir "$BUILD_DIR" -N --show-only=json-v1 \
    "${SELECT_ARGS[@]+"${SELECT_ARGS[@]}"}" > "$REGISTERED"

SELECTED_JSON="$BUILD_DIR/.ctest-selected.json"
python3 - "$REGISTERED" "$SELECTED_JSON" <<'PY'
import json, sys
data = json.load(open(sys.argv[1]))
names = [t["name"] for t in data["tests"]]
json.dump(names, open(sys.argv[2], "w"))
print(len(names))
PY
COUNT="$(python3 -c "import json; print(len(json.load(open('$SELECTED_JSON'))))")"
if [ "$COUNT" -eq 0 ]; then
    echo "[ctest-label] FATAL: -N 选中集为空（label/regex 未注册任何用例）" >&2
    exit 1
fi
if [ -n "$REQUIRE_COUNT" ] && [ "$COUNT" -lt "$REQUIRE_COUNT" ]; then
    echo "[ctest-label] FATAL: 注册集 $COUNT < 要求 $REQUIRE_COUNT" >&2
    exit 1
fi
if [ -n "$REQUIRE_FILE" ]; then
    python3 - "$SELECTED_JSON" "$REQUIRE_FILE" <<'PY'
import json, sys
selected = set(json.load(open(sys.argv[1])))
required = {line.strip() for line in open(sys.argv[2])
            if line.strip() and not line.lstrip().startswith("#")}
missing = required - selected
if missing:
    raise SystemExit(f"[ctest-label] FATAL: 必选注册集缺项: {sorted(missing)}")
print(f"[ctest-label] 必选 {len(required)} 项齐全")
PY
fi

JUNIT_ARG=()
[ -n "$JUNIT" ] && JUNIT_ARG=(--output-junit "$JUNIT")
REPEAT_ARG=()
[ -n "$REPEAT" ] && REPEAT_ARG=(--repeat "$REPEAT")
RUN_ARGS=(-j1 --no-tests=error --output-on-failure --timeout "$TIMEOUT")
[ -n "$LABEL_EXPR" ] && RUN_ARGS+=(-L "$LABEL_EXPR")
[ -n "$REGEX" ] && RUN_ARGS+=(-R "$REGEX")

ctest --test-dir "$BUILD_DIR" "${RUN_ARGS[@]}" \
    "${JUNIT_ARG[@]+"${JUNIT_ARG[@]}"}" "${REPEAT_ARG[@]+"${REPEAT_ARG[@]}"}"

if [ -n "$JUNIT" ]; then
    python3 - "$JUNIT" "$SELECTED_JSON" <<'PY'
import json, sys, xml.etree.ElementTree as ET
selected = set(json.load(open(sys.argv[2])))
cases = list(ET.parse(sys.argv[1]).getroot().iter("testcase"))
ran = {c.get("name") for c in cases}
if len(cases) != len(ran) or ran != selected:
    raise SystemExit(
        f"[ctest-label] FATAL: 实跑集与注册集不一致 "
        f"(selected={len(selected)} ran={len(ran)} diff={sorted(selected ^ ran)})")
for tag in ("skipped", "failure", "error"):
    if any(c.find(tag) is not None for c in cases):
        raise SystemExit(f"[ctest-label] FATAL: 存在 {tag} 用例")
print(f"[ctest-label] {len(cases)} passed, 0 failed, 0 skipped")
PY
fi
