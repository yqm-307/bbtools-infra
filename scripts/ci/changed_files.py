#!/usr/bin/env python3
# 通用 recipe：从真实 git diff 归一受限变更集并在本仓内完成路径分类。
# 与仓库/语言无关的 diff 部分源自已独立审查的 bbtools-infra #50 影子候选
# scripts/ci/changed_files.py（sha256 bbe31710e256b9b9464fdbb363a677974b110a4046a3dc1bef3ccd2d9cf9e70c）；
# 分类规则为本仓内联实现（CI 不再引用共享 classify/result callee 链）。
"""从真实 git diff 计算受限 changed_files 列表，并完成路径分类。

只做两件事：
1. 把「真实仓库 diff」归一为仓库相对路径列表 + 有界事件对象；
2. 按本仓分类规则把变更集判为 docs-only / code / unknown。

分类规则（路径 → doc | code；全集 doc 才判 docs-only，其余一律 code）：
- `CMakeLists.txt`、`.github/workflows/**`、`*.cmake`、`*.py` 一律判 code
  （`CMakeLists.txt` 虽是 `.txt` 后缀，属构建入口；workflows 与 CI 脚本改动可能
  改变构建/测试行为，绝不允许被 markdown 后缀或目录形状带成 docs-only）；
- `scripts/ci/**` 整目录判 code：required_*.txt 等清单是门禁输入（改动会
  改变 smoke/focused 选集），绝不能按 `.txt` 后缀落入 docs-only；
- `docs/` 前缀路径、`.md`/图片/`.txt` 等文档后缀、`LICENSE` 等根目录文书判 doc；
- 混合变更集（任一 code 路径）判 code；
- 变更集为空或 `classifier_status != ok` 判 unknown，保守执行全部。

保守原则：
- PR：三点 diff `origin/<base>...HEAD`；
- push：`before..sha`；首推（before 为空/全零）回退 `HEAD^..HEAD`；
- 任何无法确定的情形（未知 revision、diff 失败、三点 base 缺失）→
  changed_files=[]、classifier_status=unknown、classification=unknown，
  下游按「执行全部」处理，绝不擅自判绿；
- 变更集超出受限预算（文件数/字节数上限）时整集回退为空列表
  （classification=unknown→执行全部），**不裁剪成假子集**。

输出（写入 `$GITHUB_OUTPUT`，未设置时写 stdout，便于离线冒烟）：
  changed_files_json, classifier_status, classification, event_json, focused

focused（必要 focused 测试名正则）：把变更路径经 `FOCUS_MAP` 前缀→测试名正则
映射表归并为 `<a>|<b>|...`（`-R` 语义下前缀匹配即等价按名选择）；无匹配或
分类器非 ok 输出空串。映射表覆盖本仓源码模块（src/include/bbt/proto/tests/
cmake/构建入口），只作代码路径的 focused 补充选择，不改变 docs-only 分类语义。

环境输入（由 workflow 注入；离线冒烟可用同名 env 覆盖）：
  EVENT_NAME, BASE_REF, SHA, BEFORE, REF_NAME, GITHUB_OUTPUT
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

MAX_FILES = 3000
MAX_JSON_BYTES = 60000
ZERO_SHA = "0" * 40

# ---- 路径分类（本仓内联规则；唯一真源即本文件，配套单测 test_changed_files.py）----
DOC_SUFFIXES = (".md", ".png", ".jpg", ".jpeg", ".svg", ".gif", ".txt")
DOC_PREFIXES = ("docs/",)
DOC_BASENAMES = {"LICENSE", "NOTICE", "COPYING", "CODEOWNERS", "AUTHORS"}
# 后缀或位置像文档、但实际是构建/CI 入口：改它会改变构建或流水线行为，必须按
# code 处理。CMakeLists.txt 判 code 是本规则存在的第一动机（改动构建入口绝不允许
# 被打成 docs-only 放行）。
CODE_BASENAMES = {"CMakeLists.txt"}
CODE_SUFFIXES = (".cmake", ".py")
# scripts/ci/ 整目录判 code：required_*.txt 是 smoke/focused 门禁输入清单，
# 改动必须触发构建与测试，不得被 .txt 后缀判成 doc（F-02）。
CODE_PREFIXES = (".github/workflows/", "scripts/ci/")


def classify_path(path: str) -> str:
    """单路径分类：doc 或 code。"""
    base = path.rsplit("/", 1)[-1]
    if base in CODE_BASENAMES:
        return "code"
    if path.startswith(CODE_PREFIXES):
        return "code"
    lowered = path.lower()
    if lowered.endswith(CODE_SUFFIXES):
        return "code"
    if path.startswith(DOC_PREFIXES):
        return "doc"
    if lowered.endswith(DOC_SUFFIXES):
        return "doc"
    if base in DOC_BASENAMES:
        return "doc"
    return "code"


# ---- 必要 focused 映射（仓内维护：路径前缀 → ctest 测试名正则）----
# 每条 (prefix, regex)：变更路径以 prefix 开头即并入 regex。regex 为 `|` 分隔的
# 测试名前缀（ctest -R 子串语义下等价按名选择），与 tests/CMakeLists.txt 及
# tests/{net,http_net,rpc_net,redis_net,mcp} 注册的 `<module>.<行为>` 命名轴对齐。
# 无匹配 → focused 输出空串，workflow 不追加 focused 实跑。
FOCUS_MAP = (
    ('bbt/infra/config/', '^config\\.'),
    ('bbt/infra/mongo/', '^mongo\\.'),
    ('bbt/infra/CoMongoCli.hpp', '^mongo\\.'),
    ('bbt/infra/debug/', '^mongo\\.'),
    ('bbt/infra/net/', '^net\\.|^http\\.net\\.|^redis\\.net\\.|^rpc\\.net\\.'),
    ('bbt/infra/http/', '^http\\.net\\.|^rpc\\.net\\.|^mcp\\.http'),
    ('bbt/infra/redis/', '^redis\\.net\\.'),
    ('bbt/infra/rpc/', '^rpc\\.'),
    ('bbt/infra/mcp/', '^mcp\\.'),
    ('proto/', '^rpc\\.'),
    ('bbt/infra/', '^contract\\.|^config\\.|^mongo\\.|^http\\.net\\.|^redis\\.net\\.|^rpc\\.net\\.'),
    ('tests/net/', '^net\\.'),
    ('tests/http_net/', '^http\\.net\\.'),
    ('tests/redis_net/', '^redis\\.net\\.'),
    ('tests/rpc_net/', '^rpc\\.net\\.'),
    ('tests/mcp/', '^mcp\\.'),
    ('tests/Test_config', '^config\\.'),
    ('tests/Test_mongo', '^mongo\\.'),
    ('tests/mongo_async/', '^mongo\\.async_compile'),
    ('tests/Test_rpc_payload', '^rpc\\.payload'),
    ('tests/Test_iowait', '^iowait\\.'),
    ('tests/', '^contract\\.'),
    ('cmake/', '^contract\\.|^net\\.|^http\\.net\\.|^redis\\.net\\.|^rpc\\.'),
    ('CMakeLists.txt', '^contract\\.|^net\\.|^http\\.net\\.|^redis\\.net\\.|^rpc\\.'),
    ('examples/rpc_net/', '^rpc\\.net\\.v2_session_consumer'),
    ('examples/', '^example_|^net\\.consumer|^mongo\\.client\\.consumer'),
)


def focused_regex(changed_files, status: str) -> str:
    """变更集 → focused 测试名正则（`|` 归并、去重、保序）；无匹配返回空串。"""
    if status != "ok" or not changed_files:
        return ""
    parts: list[str] = []
    for path in changed_files:
        for prefix, regex in FOCUS_MAP:
            if path.startswith(prefix):
                parts.extend(regex.split("|"))
                break  # 首个命中前缀即停（FOCUS_MAP 已按最具体到最泛排序）
    return "|".join(dict.fromkeys(parts))


def classify_changes(changed_files) -> str:
    """变更集分类：docs-only | code | unknown。"""
    if not changed_files:
        # 无变更数据视为 unknown，保守执行，不擅自判绿。
        return "unknown"
    if all(classify_path(p) == "doc" for p in changed_files):
        return "docs-only"
    return "code"


def classify_status(status: str, changed_files) -> str:
    """结合 classifier 状态：失败/未知一律 conservative unknown。"""
    if status != "ok":
        return "unknown"
    return classify_changes(changed_files)


# ---- 真实 diff 归一 ----
def git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args], capture_output=True, text=True, check=False
    )


def valid_commit(rev: str) -> bool:
    return git("rev-parse", "--verify", "--quiet", f"{rev}^{{commit}}").returncode == 0


def compute_range():
    """返回真实 diff 范围表达式；无法确定时返回 None（保守 unknown）。"""
    name = os.environ.get("EVENT_NAME", "")
    base = os.environ.get("BASE_REF", "")
    sha = os.environ.get("SHA", "")
    before = os.environ.get("BEFORE", "")
    if name == "pull_request":
        if not base:
            return None
        return f"origin/{base}...HEAD"
    if not before or before == ZERO_SHA:
        # 新分支首推：改用 HEAD^ 对比；根提交无法对比 → 保守 unknown
        return "HEAD^..HEAD" if valid_commit("HEAD^") else None
    # force-push 或 before 提交被 GC/不可达 → 保守 unknown
    if valid_commit(before) and sha:
        return f"{before}..{sha}"
    return None


def bounded_event() -> dict:
    name = os.environ.get("EVENT_NAME", "")
    if name == "pull_request":
        return {"name": "pull_request", "base_ref": os.environ.get("BASE_REF") or "main"}
    return {"name": "push", "head_ref": os.environ.get("REF_NAME", "")}


def emit(key: str, value: str) -> None:
    line = f"{key}={value}\n"
    path = os.environ.get("GITHUB_OUTPUT")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(line)
    else:
        sys.stdout.write(line)


def main() -> int:
    diff_range = compute_range()
    files: list[str] = []
    status = "unknown"
    if diff_range is not None:
        proc = git("diff", "--name-only", diff_range)
        if proc.returncode == 0:
            files = [line for line in proc.stdout.splitlines() if line]
            status = "ok"

    payload = json.dumps(files, ensure_ascii=False)
    if len(files) > MAX_FILES or len(payload.encode("utf-8")) > MAX_JSON_BYTES:
        # 超预算：整集回退为空 → classification=unknown → 执行全部，保守不放绿
        payload = "[]"
        status = "over-budget"

    classification = classify_status(status, files if status == "ok" else [])

    emit("changed_files_json", payload)
    emit("classifier_status", status if status == "ok" else "unknown")
    emit("classification", classification)
    emit("event_json", json.dumps(bounded_event(), ensure_ascii=False))
    emit("focused", focused_regex(files, status))
    return 0


if __name__ == "__main__":
    sys.exit(main())
