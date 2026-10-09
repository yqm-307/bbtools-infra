#!/usr/bin/env python3
"""bbtools-infra 正式 hosted CI 的离线冒烟与契约测试（stdlib + 可选 PyYAML）。

本地只验证「CI 配方 / 分类路由 / workflow 静态契约 / prepared deps 闭包」，
**不跑 C++ 全量构建**（真实构建走父级授权的 PR CI）。覆盖：

- 配方守卫：脚本 bash -n、参数校验、fail-closed 拒绝已存在前缀；影子/被取代脚本已移除；
- changed_files 路由：push/PR 真实 diff 归一、未知保守回退、超预算整集回退；
- 正式 workflow 静态契约（需 PyYAML）：触发面/权限/SHA pin/hosted runs-on/concurrency/
  复用已发布 callee/required/optional/docs-only 门控/`always()` 唯一结果汇聚/artifact pin/
  无跨 run cache/旧 check 名保持；
- prepared deps 闭包新行为（负向）：pack→unpack 往返（含 boost-prefix 成员与 loader）、
  篡改归档或错 expected sha 必须 fail-closed；
- result job format 模板严格 JSON 回归：event_json/results_json 以真实正常结果集合与
  fallback event 渲染后必须通过 strict json.loads，并经真实 callee 输入契约接受；
- 分类路由冒烟（需 BBT_CI_SHARED_DIR 指向已发布 framework 的 scripts/ci/shared）：
  用**同一个真实 cli.py** 校验正式 workflow 传入的受限输入与 required/optional 判定，
  含未知 job key 负向拒绝；本仓不复制契约。

运行：
  PYTHONDONTWRITEBYTECODE=1 python3 scripts/ci/tests/run_tests.py
  # 需要 PyYAML 才跑静态 workflow 契约；需要真实 callee 逻辑才跑分类路由冒烟，否则显式 SKIP。
  PYTHONDONTWRITEBYTECODE=1 BBT_CI_SHARED_DIR=<framework>/scripts/ci/shared \\
      python3 scripts/ci/tests/run_tests.py
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
CI_DIR = os.path.normpath(os.path.join(HERE, ".."))
WORKTREE = os.path.normpath(os.path.join(CI_DIR, "..", ".."))
WORKFLOWS = os.path.join(WORKTREE, ".github", "workflows")
CI_YML = os.path.join(WORKFLOWS, "ci.yml")
SHADOW = os.path.join(WORKFLOWS, "ci-shadow-v1.yml")
PREPARE_BOOST = os.path.join(CI_DIR, "prepare_boost.sh")
DEPS_BUNDLE = os.path.join(CI_DIR, "deps_bundle.sh")
CHANGED = os.path.join(CI_DIR, "changed_files.py")
RES_DEPS = os.path.join(WORKTREE, "scripts", "prepare_resource_deps.sh")

CALLEE_SHA = "1c0b0fb0ebc8e7ca3888aa4f6a74d1baecf08cc7"
CALLEE = f"yqm-307/bbt-framework/.github/workflows/bbtools-classify-v1.yml@{CALLEE_SHA}"
CHECKOUT_SHA = "11d5960a326750d5838078e36cf38b85af677262"
SHA_PIN_RE = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")

REQUIRED = ["changes", "plan"]
OPTIONAL = ["prepare-deps", "release", "debug"]
HEAVY = ("prepare-deps", "release", "debug")

ENV = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")


def raw(path: str) -> str:
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def body(path: str) -> str:
    """去掉说明性注释行后的实际 YAML 内容（注释中可提及被禁写法）。"""
    return "\n".join(
        line for line in raw(path).splitlines() if not line.lstrip().startswith("#")
    )


def gha_format(fmt, *args):
    """模拟 GitHub Actions format()：`{{`/`}}` 为字面花括号，`{N}` 取第 N 个参数。"""
    out, i = [], 0
    while i < len(fmt):
        ch = fmt[i]
        if ch == "{":
            if i + 1 < len(fmt) and fmt[i + 1] == "{":
                out.append("{")
                i += 2
                continue
            end = fmt.index("}", i)
            out.append(str(args[int(fmt[i + 1:end])]))
            i = end + 1
            continue
        if ch == "}":
            if i + 1 < len(fmt) and fmt[i + 1] == "}":
                out.append("}")
                i += 2
                continue
            raise ValueError("unbalanced '}' in format template")
        out.append(ch)
        i += 1
    return "".join(out)


def format_template(key):
    """从实际 ci.yml 取 `key:` 行里 format('...') 的单引号模板（模板内不含单引号）。"""
    for line in raw(CI_YML).splitlines():
        if re.match(rf"\s*{re.escape(key)}:", line) and "format(" in line:
            match = re.search(r"format\('([^']*)'", line)
            if match:
                return match.group(1)
    raise AssertionError(f"未在 ci.yml 找到 {key} 的 format 模板")


def run(cmd, **kwargs):
    return subprocess.run(cmd, capture_output=True, text=True, check=False, **kwargs)


# --------------------------------------------------------------------------- #
# 配方守卫
# --------------------------------------------------------------------------- #
class RecipeGuardTests(unittest.TestCase):
    def test_shell_scripts_parse(self):
        for path in (PREPARE_BOOST, DEPS_BUNDLE, RES_DEPS):
            with self.subTest(path=os.path.basename(path)):
                proc = run(["bash", "-n", path], env=ENV)
                self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_python_recipes_compile(self):
        # 用 compile() 做纯语法检查，不落 .pyc（PYTHONDONTWRITEBYTECODE 对 py_compile 无效）。
        for path in (CHANGED, os.path.abspath(__file__)):
            with self.subTest(path=os.path.basename(path)):
                compile(raw(path), path, "exec")

    def test_prepare_boost_print_plan_is_side_effect_free_and_pinned(self):
        proc = run(["bash", PREPARE_BOOST, "--print-plan"], env=ENV)
        self.assertEqual(proc.returncode, 0, proc.stderr)
        plan = dict(
            line.split("=", 1) for line in proc.stdout.splitlines() if "=" in line
        )
        self.assertEqual(plan["boost_version"], "1.90.0")
        self.assertEqual(
            plan["boost_sha256"],
            "5e93d582aff26868d581a52ae78c7d8edf3f3064742c6e77901a1f18a437eea9",
        )
        self.assertEqual(plan["boost_libs"], "context")
        self.assertEqual(plan["boost_version_num"], "109000")
        self.assertEqual(
            plan["archive_url"],
            "https://archives.boost.io/release/1.90.0/source/boost_1_90_0.tar.gz",
        )

    def test_prepare_boost_refuses_existing_prefix(self):
        with tempfile.TemporaryDirectory() as td:
            existing = os.path.join(td, "prefix")
            os.makedirs(existing)
            proc = run(["bash", PREPARE_BOOST, existing], env=ENV)
            self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
            self.assertTrue(os.path.isdir(existing), "拒绝时不得改动已存在前缀")

    def test_prepare_boost_refuses_dangling_symlink(self):
        with tempfile.TemporaryDirectory() as td:
            link = os.path.join(td, "dangling")
            os.symlink(os.path.join(td, "never-created"), link)
            proc = run(["bash", PREPARE_BOOST, link], env=ENV)
            self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)
            self.assertTrue(os.path.islink(link))

    def test_deps_bundle_rejects_unknown_subcommand(self):
        proc = run(["bash", DEPS_BUNDLE, "bogus"], env=ENV)
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)

    def test_shadow_entry_and_superseded_scripts_removed(self):
        # 正式切换：影子入口与被取代的本地结果/影子配方脚本不得残留（历史保留在 Git）。
        self.assertFalse(os.path.exists(SHADOW), "影子 workflow 应已删除")
        for name in ("run_infra_gate.sh", "required_result_gate.sh"):
            self.assertFalse(
                os.path.exists(os.path.join(CI_DIR, name)), f"{name} 应已删除"
            )


# --------------------------------------------------------------------------- #
# changed_files 路由
# --------------------------------------------------------------------------- #
def _git(repo, *args):
    return run(["git", "-C", repo, *args], env=ENV)


def _init_repo(repo):
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "ci@test")
    _git(repo, "config", "user.name", "ci")
    _git(repo, "config", "commit.gpgsign", "false")


def _commit(repo, relpath, content, message):
    full = os.path.join(repo, relpath)
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w", encoding="utf-8") as handle:
        handle.write(content)
    _git(repo, "add", relpath)
    _git(repo, "commit", "-q", "-m", message)
    return _git(repo, "rev-parse", "HEAD").stdout.strip()


def _changed_files(repo, env_extra):
    out_path = os.path.join(repo, ".ci-output")
    env = dict(ENV, GITHUB_OUTPUT=out_path, **env_extra)
    proc = run([sys.executable, CHANGED], env=env, cwd=repo)
    with open(out_path, encoding="utf-8") as handle:
        values = dict(
            line.split("=", 1) for line in handle.read().splitlines() if "=" in line
        )
    return proc, values


class ChangedFilesRoutingTests(unittest.TestCase):
    def test_push_range_yields_real_paths(self):
        with tempfile.TemporaryDirectory() as repo:
            _init_repo(repo)
            first = _commit(repo, "docs/a.md", "a", "docs")
            second = _commit(repo, "src/x.cc", "x", "code")
            proc, values = _changed_files(
                repo,
                {"EVENT_NAME": "push", "BEFORE": first, "SHA": second,
                 "REF_NAME": "ci/issue-50-formal-cutover-local"},
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(values["classifier_status"], "ok")
            self.assertEqual(json.loads(values["changed_files_json"]), ["src/x.cc"])
            self.assertEqual(
                json.loads(values["event_json"]),
                {"name": "push", "head_ref": "ci/issue-50-formal-cutover-local"},
            )

    def test_pull_request_three_dot(self):
        with tempfile.TemporaryDirectory() as repo:
            _init_repo(repo)
            base = _commit(repo, "docs/a.md", "a", "base")
            _git(repo, "update-ref", "refs/remotes/origin/main", base)
            _git(repo, "checkout", "-q", "-b", "feat")
            head = _commit(repo, "docs/ci/note.md", "n", "note")
            proc, values = _changed_files(
                repo,
                {"EVENT_NAME": "pull_request", "BASE_REF": "main", "SHA": head},
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(json.loads(values["changed_files_json"]), ["docs/ci/note.md"])
            self.assertEqual(
                json.loads(values["event_json"]),
                {"name": "pull_request", "base_ref": "main"},
            )

    def test_unknown_range_falls_back_conservative(self):
        with tempfile.TemporaryDirectory() as repo:
            _init_repo(repo)
            first = _commit(repo, "src/x.cc", "x", "one")
            proc, values = _changed_files(
                repo,
                {"EVENT_NAME": "push", "BEFORE": "f" * 40, "SHA": first},
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(values["changed_files_json"], "[]")
            self.assertEqual(values["classifier_status"], "unknown")

    def test_oversized_change_set_falls_back_to_empty(self):
        with tempfile.TemporaryDirectory() as repo:
            _init_repo(repo)
            first = _commit(repo, "src/gen0.cc", "0", "base")
            for i in range(1, 6):
                _commit(repo, f"src/gen{i}.cc", "x", f"c{i}")
            head = _git(repo, "rev-parse", "HEAD").stdout.strip()
            # 人为把文件数上限调低到 2（避免造上千文件），验证超预算整集回退为空。
            out_path = os.path.join(repo, ".o2")
            env = dict(ENV, GITHUB_OUTPUT=out_path, EVENT_NAME="push",
                       BEFORE=first, SHA=head)
            script = (
                "import sys; sys.path.insert(0, %r); "
                "import changed_files as m; m.MAX_FILES = 2; sys.exit(m.main())" % CI_DIR
            )
            proc = run([sys.executable, "-c", script], env=env, cwd=repo)
            with open(out_path, encoding="utf-8") as handle:
                values = dict(
                    line.split("=", 1)
                    for line in handle.read().splitlines() if "=" in line
                )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(values["changed_files_json"], "[]")


# --------------------------------------------------------------------------- #
# prepared deps 闭包新行为（含 boost-prefix 成员；负向 fail-closed）
# --------------------------------------------------------------------------- #
class PreparedDepsClosureTests(unittest.TestCase):
    def _fixture(self, root):
        # 造出 deps_bundle pack 要求的全部成员（含 boost-prefix）。
        for member in ("coroutine", "bbtools-core", "deps-prefix", "boost-prefix"):
            os.makedirs(os.path.join(root, member), exist_ok=True)
        os.makedirs(os.path.join(root, "coroutine", "src"), exist_ok=True)
        with open(os.path.join(root, "coroutine", "src", "a.cc"), "w", encoding="utf-8") as fh:
            fh.write("int a;\n")
        # 真实 symlink：tar 必须按 symlink 存（不 dereference）。
        os.symlink("a.cc", os.path.join(root, "coroutine", "src", "link.cc"))
        for module in ("hiredis", "mongoc", "mongocxx"):
            libdir = os.path.join(root, "deps-prefix", module, "lib")
            os.makedirs(libdir, exist_ok=True)
            with open(os.path.join(libdir, f"lib{module}.so"), "w", encoding="utf-8") as fh:
                fh.write("x")
        os.makedirs(os.path.join(root, "boost-prefix", "lib"), exist_ok=True)
        with open(os.path.join(root, "boost-prefix", "lib", "libboost_context.so"),
                  "w", encoding="utf-8") as fh:
            fh.write("b")

    def test_pack_unpack_roundtrip_includes_boost_prefix(self):
        with tempfile.TemporaryDirectory() as td:
            root = os.path.join(td, "ws")
            os.mkdir(root)
            self._fixture(root)
            tar = os.path.join(root, "deps-bundle.tar")
            meta = os.path.join(root, "deps-bundle.meta")
            pack = run(["bash", DEPS_BUNDLE, "pack", "--root", root,
                        "--out", tar, "--meta", meta], env=ENV)
            self.assertEqual(pack.returncode, 0, pack.stdout + pack.stderr)
            meta_text = raw(meta)
            self.assertIn("boost-prefix", meta_text)
            m = re.search(r"^sha256=([0-9a-f]{64})$", meta_text, re.M)
            self.assertIsNotNone(m, meta_text)
            self.assertIn("symlinks=1", meta_text)

            # 同绝对路径解包（hosted 行为）：sha 命中，loader 必须含 boost-prefix/lib。
            env_file = os.path.join(td, "gh_env")
            env = dict(ENV, GITHUB_ENV=env_file)
            unpack = run(["bash", DEPS_BUNDLE, "unpack", "--tar", tar, "--meta", meta,
                          "--expect-sha", m.group(1), "--dest", root], env=env)
            self.assertEqual(unpack.returncode, 0, unpack.stdout + unpack.stderr)
            loader = raw(env_file)
            self.assertIn("boost-prefix/lib", loader)
            self.assertIn("deps-prefix/hiredis/lib", loader)

    def test_unpack_rejects_wrong_expected_sha(self):
        with tempfile.TemporaryDirectory() as td:
            root = os.path.join(td, "ws")
            os.mkdir(root)
            self._fixture(root)
            tar = os.path.join(root, "deps-bundle.tar")
            meta = os.path.join(root, "deps-bundle.meta")
            run(["bash", DEPS_BUNDLE, "pack", "--root", root, "--out", tar,
                 "--meta", meta], env=ENV)
            proc = run(["bash", DEPS_BUNDLE, "unpack", "--tar", tar, "--meta", meta,
                        "--expect-sha", "0" * 64, "--dest", root], env=ENV)
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("expected sha", proc.stderr)

    def test_unpack_rejects_tampered_archive(self):
        with tempfile.TemporaryDirectory() as td:
            root = os.path.join(td, "ws")
            os.mkdir(root)
            self._fixture(root)
            tar = os.path.join(root, "deps-bundle.tar")
            meta = os.path.join(root, "deps-bundle.meta")
            pack = run(["bash", DEPS_BUNDLE, "pack", "--root", root, "--out", tar,
                        "--meta", meta], env=ENV)
            self.assertEqual(pack.returncode, 0, pack.stderr)
            m = re.search(r"^sha256=([0-9a-f]{64})$", raw(meta), re.M)
            with open(tar, "ab") as fh:
                fh.write(b"tamper")
            proc = run(["bash", DEPS_BUNDLE, "unpack", "--tar", tar, "--meta", meta,
                        "--expect-sha", m.group(1), "--dest", root], env=ENV)
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("sha256 不匹配", proc.stderr)


# --------------------------------------------------------------------------- #
# 正式 workflow 静态契约（需 PyYAML）
# --------------------------------------------------------------------------- #
try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None


def _loader():
    assert yaml is not None

    class Base(yaml.SafeLoader):
        pass

    Base.yaml_implicit_resolvers = {
        ch: [e for e in entries if e[0] != "tag:yaml.org,2002:bool"]
        for ch, entries in yaml.SafeLoader.yaml_implicit_resolvers.items()
    }
    Base.add_implicit_resolver(
        "tag:yaml.org,2002:bool",
        re.compile(r"^(?:true|false|True|False|TRUE|FALSE)$"),
        list("tTfF"),
    )

    class Strict(Base):
        def construct_mapping(self, node, deep=False):
            seen = set()
            for key_node, _ in node.value:
                key = self.construct_object(key_node, deep=deep)
                if key in seen:
                    raise ValueError(f"duplicate key: {key}")
                seen.add(key)
            return super().construct_mapping(node, deep)

    return Strict


@unittest.skipIf(yaml is None, "PyYAML 不可用：静态 workflow 契约显式 SKIP（不等于通过）")
class WorkflowStaticTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        assert yaml is not None
        cls.wf = yaml.load(raw(CI_YML), Loader=_loader())
        cls.text = raw(CI_YML)
        cls.jobs = cls.wf["jobs"]

    def _caller_jobs(self):
        return {n: j for n, j in self.jobs.items() if "uses" in j}

    def test_name_and_toplevel_shape(self):
        self.assertEqual(self.wf["name"], "CI")
        self.assertEqual(
            set(self.wf), {"name", "on", "permissions", "concurrency", "env", "jobs"}
        )

    def test_triggers_preserved(self):
        on = self.wf["on"]
        self.assertEqual(set(on), {"pull_request", "push", "workflow_dispatch"})
        self.assertEqual(on["pull_request"]["branches"], ["main"])
        self.assertEqual(on["push"]["branches"], ["main"])
        inputs = on["workflow_dispatch"]["inputs"]
        self.assertEqual(set(inputs), {"build-mode", "cache-enabled"})
        self.assertEqual(inputs["build-mode"]["default"], "clean")
        self.assertEqual(inputs["build-mode"]["options"], ["clean", "cache-bypass"])
        self.assertEqual(inputs["cache-enabled"]["type"], "boolean")
        self.assertNotIn("schedule", self.text)
        self.assertNotIn("merge_group", self.text)

    def test_permissions_minimal_no_write_no_secrets(self):
        self.assertEqual(self.wf["permissions"], {})
        self.assertNotIn("secrets: inherit", self.text)
        self.assertNotIn("${{ secrets.", self.text)
        self.assertNotRegex(self.text, r"(?m)^\s*secrets:\s*$")
        for name, job in self.jobs.items():
            with self.subTest(job=name):
                perms = job.get("permissions", {})
                self.assertNotIn("write", str(perms))
                if "uses" not in job:
                    self.assertEqual(perms, {"contents": "read"})

    def test_concurrency_pr_only_cancel(self):
        conc = self.wf["concurrency"]
        self.assertTrue(conc["group"].startswith("CI-"))
        self.assertIn("pull_request", str(conc["group"]))
        self.assertIn("run_id", str(conc["group"]))
        self.assertEqual(
            conc["cancel-in-progress"], "${{ github.event_name == 'pull_request' }}"
        )

    def test_local_jobs_are_hosted_ubuntu_24_04(self):
        for name, job in self.jobs.items():
            if "uses" in job:
                self.assertNotIn("runs-on", job, f"{name} 为 reusable 调用，应由 callee 决定 runs-on")
                continue
            with self.subTest(job=name):
                self.assertEqual(job["runs-on"], "ubuntu-24.04")

    def test_all_uses_are_full_sha_pinned(self):
        def iter_uses():
            for job in self.jobs.values():
                if "uses" in job:
                    yield job["uses"]
                for step in job.get("steps", []) or []:
                    if "uses" in step:
                        yield step["uses"]

        for uses in iter_uses():
            with self.subTest(uses=uses):
                self.assertRegex(uses, SHA_PIN_RE)

    def test_reuses_published_callee_and_separates_source(self):
        calls = self._caller_jobs()
        self.assertEqual(set(calls), {"plan", "result"})
        self.assertEqual({job["uses"] for job in calls.values()}, {CALLEE})
        self.assertNotIn("./.github/workflows", self.text, "必须引用已发布 callee，不能用本地相对引用")
        for job in calls.values():
            with self.subTest(job=job.get("name")):
                self.assertNotIn("secrets", job)
                self.assertEqual(job["with"]["profile"], "hosted")
                # source（github.sha）由调用方给；automation 身份由 callee 经 job.workflow_* 解析。
                self.assertIn("source_sha", job["with"])
                self.assertIn("github.sha", job["with"]["source_sha"])

    def test_required_and_optional_are_disjoint_and_heavy_is_optional(self):
        for job in self._caller_jobs().values():
            with self.subTest(job=job.get("name")):
                required = json.loads(job["with"]["required_checks_json"])
                optional = json.loads(job["with"]["optional_checks_json"])
                self.assertEqual(required, REQUIRED)
                self.assertEqual(optional, OPTIONAL)
                self.assertTrue(required, "required 不得为空（防全跳过变绿）")
                self.assertFalse(set(required) & set(optional))
                # optional 必须恰为重型 job；required 必须恰为轻量常跑 job。
                self.assertEqual(set(optional), set(HEAVY))
                self.assertEqual(set(required), {"changes", "plan"})

    def test_heavy_jobs_gated_on_shared_classification(self):
        for name in HEAVY:
            with self.subTest(job=name):
                cond = str(self.jobs[name]["if"])
                self.assertIn("plan.result", cond)
                self.assertIn("classification", cond)
                self.assertIn("docs-only", cond)
                # release/debug 还需等 prepare-deps 成功，避免缺归档空跑。
                if name != "prepare-deps":
                    self.assertIn("prepare-deps.result", cond)

    def test_result_job_is_unique_producer_with_old_check_name(self):
        names = [j.get("name") for j in self.jobs.values()]
        self.assertEqual(names.count("编译 & ctest"), 1, "旧 required check 名必须恰有一个 producer")
        result = self.jobs["result"]
        self.assertEqual(result["name"], "编译 & ctest")
        self.assertEqual(result["if"], "${{ always() }}")
        # 不得再有第二份跨 job 结果脚本（避免不匹配脚本导致空绿）。
        self.assertNotIn("required_result_gate.sh", self.text)

    def test_result_job_aggregates_all_five_jobs(self):
        result = self.jobs["result"]
        self.assertEqual(set(result["needs"]), {"changes", "plan", *HEAVY})
        results_expr = result["with"]["results_json"]
        for key in ("changes", "plan", *HEAVY):
            self.assertIn(f'"{key}"', results_expr)
        for ref in ("needs.changes.result", "needs.plan.result",
                    "needs.prepare-deps.result", "needs.release.result", "needs.debug.result"):
            self.assertIn(ref, results_expr)

    def test_result_format_templates_render_to_strict_json(self):
        # F1 回归：result job 的 event_json/results_json 是 plain YAML scalar，format 模板内
        # 双引号前不得有 literal 反斜杠；以真实正常结果集合与 fallback event 渲染后必须通过
        # strict json.loads（仅断言子串无法捕获该缺陷）。
        with_block = self.jobs["result"]["with"]
        order = ("changes", "plan", *HEAVY)

        ev_expr = with_block["event_json"]
        self.assertNotIn('\\"', ev_expr, "event_json 模板不得含 literal 反斜杠转义")
        self.assertEqual(
            json.loads(gha_format(format_template("event_json"), "push")),
            {"name": "push"},
        )

        rs_expr = with_block["results_json"]
        self.assertNotIn('\\"', rs_expr, "results_json 模板不得含 literal 反斜杠转义")
        normal = {name: "success" for name in order}
        rendered = gha_format(format_template("results_json"), *(normal[n] for n in order))
        self.assertEqual(json.loads(rendered), normal)

    def test_header_fork_admission_is_truthful(self):
        # F2：文件头不得再声称 approval_policy=all_external_contributors（平台实测为
        # first_time_contributors）；须如实说明本注释不构成 fork/self-hosted 准入闸门。
        self.assertNotIn("all_external_contributors", self.text)
        self.assertIn("first_time_contributors", self.text)

    def test_result_job_has_conservative_fallbacks(self):
        with_block = self.jobs["result"]["with"]
        self.assertIn("|| '[]'", with_block["changed_files_json"])
        self.assertIn("|| 'unknown'", with_block["classifier_status"])
        self.assertIn("github.event_name", with_block["event_json"])

    def test_artifacts_pinned_and_no_cross_run_cache(self):
        text = body(CI_YML)
        self.assertIn("actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02", text)
        self.assertIn("actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093", text)
        self.assertNotIn("actions/cache", text, "跨 run 缓存按 hosted 明确阻塞")

    def test_prepared_deps_digest_closure(self):
        text = body(CI_YML)
        self.assertIn("deps_bundle.sh pack", text)
        self.assertIn("deps_bundle.sh unpack", text)
        self.assertIn("--expect-sha", text)
        self.assertIn("needs.prepare-deps.outputs.bundle_sha256", text)
        self.assertIn("prepare_resource_deps.sh", text)
        self.assertIn("prepare_boost.sh", text)

    def test_gate_criteria_present(self):
        text = body(CI_YML)
        for needle in (
            "tests/resource-prefix/run.sh",
            "redis.unit", "mongo.unit",
            "ctest -j1 --output-on-failure --timeout 180",
            "BOOST_ROOT",
            "-DBBT_INFRA_STRINGENT_DEBUG=ON",
            "Test_http_session_lifetime_probe", "Test_mongo_unit",
            "libbbt_coroutine.so", "libbbt_core.so", "ldd",
        ):
            with self.subTest(needle=needle):
                self.assertIn(needle, text)

    def test_inline_scripts_pass_bash_n(self):
        for name, job in self.jobs.items():
            for step in job.get("steps", []) or []:
                script = step.get("run")
                if not script:
                    continue
                with self.subTest(job=name, step=step.get("name")):
                    fd, tmp = tempfile.mkstemp(suffix=".sh")
                    try:
                        with os.fdopen(fd, "w", encoding="utf-8") as handle:
                            handle.write(script)
                        proc = run(["bash", "-n", tmp], env=ENV)
                        self.assertEqual(proc.returncode, 0, proc.stderr)
                    finally:
                        os.unlink(tmp)


# --------------------------------------------------------------------------- #
# 分类路由冒烟：真实复用 framework callee 逻辑（不复制契约）
# --------------------------------------------------------------------------- #
SHARED = os.environ.get("BBT_CI_SHARED_DIR", "")


def _payload(changed_files_json, classifier_status="ok"):
    return {
        "repo": "yqm-307/bbtools-infra",
        "source_sha": "a" * 40,
        "profile": "hosted",
        "concurrency": "2",
        "timeout_minutes": "60",
        "event_json": '{"name":"pull_request","base_ref":"main"}',
        "changed_files_json": changed_files_json,
        "required_checks_json": json.dumps(REQUIRED),
        "optional_checks_json": json.dumps(OPTIONAL),
        "classifier_status": classifier_status,
    }


@unittest.skipUnless(
    SHARED and os.path.isfile(os.path.join(SHARED, "cli.py")),
    "BBT_CI_SHARED_DIR 未指向已发布 callee scripts/ci/shared：分类路由冒烟显式 SKIP",
)
class ClassificationRoutingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cli = os.path.join(SHARED, "cli.py")

    def _classify(self, changed, status="ok"):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(_payload(changed, status), handle)
            path = handle.name
        proc = run([sys.executable, self.cli, "classify", "--file", path], env=ENV)
        os.unlink(path)
        return proc

    def _evaluate(self, plan, results):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump({"plan": plan, "results": results}, handle)
            path = handle.name
        proc = run([sys.executable, self.cli, "evaluate", "--file", path], env=ENV)
        os.unlink(path)
        return proc

    def _plan_for(self, changed, status="ok"):
        proc = self._classify(changed, status)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        return json.loads(proc.stdout)["plan"]

    def test_docs_only_routing(self):
        proc = self._classify('["docs/ci/infra-ci-v1.md"]')
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["classification"], "docs-only")

    def test_code_routing(self):
        self.assertEqual(
            json.loads(self._classify('["src/http/HttpClientImpl.cc"]').stdout)["classification"],
            "code",
        )

    def test_classifier_failure_is_unknown_not_docs(self):
        proc = self._classify('["docs/ci/x.md"]', status="failed")
        self.assertEqual(json.loads(proc.stdout)["classification"], "unknown")
        self.assertEqual(json.loads(self._classify("[]").stdout)["classification"], "unknown")

    def test_docs_only_allows_heavy_skip_and_is_success(self):
        plan = self._plan_for('["docs/ci/x.md"]')
        proc = self._evaluate(plan, {
            "changes": "success", "plan": "success",
            "prepare-deps": "skipped", "release": "skipped", "debug": "skipped",
        })
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "success")

    def test_code_heavy_failure_is_failure(self):
        plan = self._plan_for('["src/x.cc"]')
        proc = self._evaluate(plan, {
            "changes": "success", "plan": "success",
            "prepare-deps": "success", "release": "failure", "debug": "success",
        })
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "failure")

    def test_required_cancelled_is_failure(self):
        plan = self._plan_for('["src/x.cc"]')
        proc = self._evaluate(plan, {
            "changes": "cancelled", "plan": "success",
            "prepare-deps": "success", "release": "success", "debug": "success",
        })
        self.assertEqual(proc.returncode, 1)

    def test_unknown_classification_forbids_heavy_skip(self):
        plan = self._plan_for("[]", status="failed")
        proc = self._evaluate(plan, {
            "changes": "success", "plan": "success",
            "prepare-deps": "skipped", "release": "skipped", "debug": "skipped",
        })
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "failure")

    def test_code_all_success_is_success(self):
        plan = self._plan_for('["src/x.cc"]')
        proc = self._evaluate(plan, {
            "changes": "success", "plan": "success",
            "prepare-deps": "success", "release": "success", "debug": "success",
        })
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

    def test_unknown_job_key_is_rejected(self):
        # 负向新行为：结果键不在 required∪optional 内 → 契约拒绝（非零），不放绿。
        plan = self._plan_for('["src/x.cc"]')
        proc = self._evaluate(plan, {
            "changes": "success", "plan": "success",
            "prepare-deps": "success", "release": "success", "debug": "success",
            "build-and-test": "success",
        })
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(json.loads(proc.stdout)["code"], "E_RESULT_UNKNOWN_JOB")

    def test_rendered_workflow_inputs_pass_real_callee(self):
        # F1 回归（真实 callee）：用实际 workflow 的 format 模板渲染出的 event_json /
        # results_json（真实正常结果集合 + fallback event）按 workflow_call 输入喂给真实
        # cli.py；契约必须接受并给出 verdict=success（不复制契约、不虚构平台执行）。
        order = ("changes", "plan", *HEAVY)
        results = {name: "success" for name in order}
        rendered_rs = gha_format(format_template("results_json"), *(results[n] for n in order))
        rendered_ev = gha_format(format_template("event_json"), "push")
        # 先做本地 strict parse：从 workflow 实际模板渲染必须是合法 JSON。
        self.assertEqual(json.loads(rendered_rs), results)
        self.assertEqual(json.loads(rendered_ev), {"name": "push"})

        payload = _payload('["src/x.cc"]')
        payload["event_json"] = rendered_ev
        payload["results_json"] = rendered_rs
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(payload, handle)
            path = handle.name
        proc = run([sys.executable, self.cli, "classify", "--file", path], env=ENV)
        os.unlink(path)
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        out = json.loads(proc.stdout)
        self.assertEqual(out["classification"], "code")
        self.assertEqual(out["evaluation"]["verdict"], "success")


if __name__ == "__main__":
    unittest.main(verbosity=2)
