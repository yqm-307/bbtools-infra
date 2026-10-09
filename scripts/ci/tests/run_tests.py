#!/usr/bin/env python3
"""ci-shadow-v1 离线冒烟与直接耦合测试（stdlib + 可选 PyYAML）。

本地只验证「CI 配方 / 分类路由 / 静态契约」，**不跑 C++ 全量构建**（真实构建走父级
授权的 PR CI）。覆盖：

- 配方守卫：脚本 bash -n、参数校验、fail-closed 拒绝已存在前缀；
- changed_files 路由：push/PR 真实 diff 归一、未知保守回退、超预算整集回退；
- 影子 workflow 静态契约（需 PyYAML）：触发面/权限/SHA pin/hosted runs-on/
  concurrency 独立/复用 callee/required/optional/docs-only/always 汇聚/无 artifact/无 cache；
- 与现役 .github/workflows/ci.yml 的直接耦合：固定源 tag+SHA、ctest 判据、unit 注册集；
- 分类路由冒烟（需 BBT_CI_SHARED_DIR 指向已发布 framework 的 scripts/ci/shared）：
  用**同一个真实 cli.py** 校验影子传入的受限输入与 required/optional 判定，本仓不复制契约。

运行：
  PYTHONDONTWRITEBYTECODE=1 python3 scripts/ci/tests/run_tests.py
  # 需要 PyYAML 才跑静态 workflow 契约；需要真实 callee 逻辑才跑分类路由冒烟，否则显式 SKIP。
  PYTHONDONTWRITEBYTECODE=1 BBT_CI_SHARED_DIR=<framework>/scripts/ci/shared \
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
SHADOW = os.path.join(WORKTREE, ".github", "workflows", "ci-shadow-v1.yml")
CI_YML = os.path.join(WORKTREE, ".github", "workflows", "ci.yml")
PREPARE_BOOST = os.path.join(CI_DIR, "prepare_boost.sh")
GATE = os.path.join(CI_DIR, "run_infra_gate.sh")
CHANGED = os.path.join(CI_DIR, "changed_files.py")
RES_DEPS = os.path.join(WORKTREE, "scripts", "prepare_resource_deps.sh")

CALLEE_SHA = "5b04115e5c871b75c6bbf357e2a9bcd2d26ef3f8"
CALLEE = f"yqm-307/bbt-framework/.github/workflows/bbtools-classify-v1.yml@{CALLEE_SHA}"
SHA_PIN_RE = re.compile(r"^[^@\s]+@[0-9a-f]{40}$")

ENV = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")


def raw(path: str) -> str:
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def body(path: str) -> str:
    """去掉说明性注释行后的实际 YAML 内容（注释中可提及被禁写法）。"""
    return "\n".join(
        line for line in raw(path).splitlines() if not line.lstrip().startswith("#")
    )


def run(cmd, **kwargs):
    return subprocess.run(cmd, capture_output=True, text=True, check=False, **kwargs)


# --------------------------------------------------------------------------- #
# 配方守卫
# --------------------------------------------------------------------------- #
class RecipeGuardTests(unittest.TestCase):
    def test_shell_scripts_parse(self):
        for path in (PREPARE_BOOST, GATE):
            with self.subTest(path=os.path.basename(path)):
                proc = run(["bash", "-n", path], env=ENV)
                self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_python_recipes_compile(self):
        # 用 compile() 做纯语法检查，不落 .pyc（PYTHONDONTWRITEBYTECODE 对 py_compile 无效）。
        for path in (CHANGED, os.path.abspath(__file__)):
            with self.subTest(path=os.path.basename(path)):
                with open(path, encoding="utf-8") as handle:
                    source = handle.read()
                compile(source, path, "exec")

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

    def test_gate_rejects_missing_required_args(self):
        proc = run(["bash", GATE, "--infra", "/nonexistent"], env=ENV)
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)

    def test_gate_rejects_invalid_infra_dir(self):
        proc = run(
            ["bash", GATE, "--infra", "/nonexistent", "--coroutine", "/x",
             "--core", "/y", "--boost-prefix", "/z", "--resource-deps-root", "/r",
             "--build-dir", "/b"],
            env=ENV,
        )
        self.assertEqual(proc.returncode, 2, proc.stdout + proc.stderr)


# --------------------------------------------------------------------------- #
# changed_files 路由
# --------------------------------------------------------------------------- #
def _git(repo, *args):
    return run(["git", "-C", repo, *args], env=ENV)


def _init_repo(repo):
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "shadow@test")
    _git(repo, "config", "user.name", "shadow")
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
    out_path = os.path.join(repo, ".shadow-output")
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
                 "REF_NAME": "ci/issue-50-hosted-shadow"},
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(values["classifier_status"], "ok")
            self.assertEqual(json.loads(values["changed_files_json"]), ["src/x.cc"])
            self.assertEqual(
                json.loads(values["event_json"]),
                {"name": "push", "head_ref": "ci/issue-50-hosted-shadow"},
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
# 影子 workflow 静态契约（需 PyYAML）
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
        cls.wf = yaml.load(raw(SHADOW), Loader=_loader())
        cls.text = raw(SHADOW)

    def test_name_and_toplevel_shape(self):
        self.assertEqual(self.wf["name"], "ci-shadow-v1")
        self.assertEqual(set(self.wf), {"name", "on", "permissions", "concurrency", "env", "jobs"})

    def test_trigger_is_branch_push_and_main_pr_only(self):
        on = self.wf["on"]
        self.assertEqual(set(on), {"push", "pull_request"})
        self.assertEqual(on["push"]["branches"], ["ci/issue-50-hosted-shadow"])
        self.assertEqual(on["pull_request"]["branches"], ["main"])
        text = body(SHADOW)
        self.assertNotIn("workflow_dispatch", text)
        self.assertNotIn("schedule", text)
        self.assertNotIn("merge_group", text)

    def test_permissions_minimal_no_write_no_secrets(self):
        self.assertEqual(self.wf["permissions"], {})
        self.assertNotIn("secrets: inherit", self.text)
        self.assertNotIn("${{ secrets.", self.text)
        self.assertNotRegex(self.text, r"(?m)^\s*secrets:\s*$")
        for name, job in self.wf["jobs"].items():
            with self.subTest(job=name):
                perms = job.get("permissions", {})
                self.assertNotIn("write", str(perms))
                if "uses" not in job:
                    self.assertEqual(perms, {"contents": "read"})

    def test_concurrency_is_independent_and_pr_only_cancel(self):
        conc = self.wf["concurrency"]
        self.assertTrue(conc["group"].startswith("ci-shadow-v1-"))
        self.assertNotIn("${{ github.workflow }}", conc["group"])
        self.assertIn("pull_request", str(conc["cancel-in-progress"]))

    def test_local_jobs_are_hosted_ubuntu_24_04(self):
        for name, job in self.wf["jobs"].items():
            if "uses" in job:
                self.assertNotIn("runs-on", job, f"{name} 为 reusable 调用，应由 callee 决定 runs-on")
                continue
            with self.subTest(job=name):
                self.assertEqual(job["runs-on"], "ubuntu-24.04")

    def test_boost_setup_does_not_create_resource_prefix(self):
        # 执行真实 resource recipe 的前缀检查；git shim 阻断所有联网/构建。
        with tempfile.TemporaryDirectory() as td:
            workspace = os.path.join(td, "workspace")
            os.mkdir(workspace)
            envs = self.wf["env"]
            boost = envs["BOOST_PREFIX"].replace("${{ github.workspace }}", workspace)
            resource = envs["RESOURCE_DEPS_ROOT"].replace("${{ github.workspace }}", workspace)
            os.makedirs(os.path.dirname(boost), exist_ok=True)
            os.mkdir(boost)  # 模拟 Boost 已完成安装。
            self.assertFalse(os.path.lexists(resource), "Boost 安装不得占用资源 recipe 根目录")
            shim = os.path.join(td, "bin")
            os.mkdir(shim)
            git_shim = os.path.join(shim, "git")
            with open(git_shim, "w", encoding="utf-8") as handle:
                handle.write("#!/bin/sh\nprintf 'offline-clone-boundary\\n' >&2\nexit 97\n")
            os.chmod(git_shim, 0o755)
            env = dict(ENV, PATH=shim + os.pathsep + ENV["PATH"], TMPDIR=td)
            proc = run(["bash", RES_DEPS, resource, "--jobs", "2"], env=env)
            self.assertEqual(proc.returncode, 97, proc.stdout + proc.stderr)
            self.assertIn("offline-clone-boundary", proc.stderr)
            self.assertNotIn("目标前缀已存在", proc.stderr)
            self.assertFalse(os.path.lexists(resource))
            self.assertFalse(os.path.lexists(resource + ".lock"))

    def test_all_uses_are_full_sha_pinned(self):
        def iter_uses():
            for job in self.wf["jobs"].values():
                if "uses" in job:
                    yield job["uses"]
                for step in job.get("steps", []) or []:
                    if "uses" in step:
                        yield step["uses"]

        for uses in iter_uses():
            with self.subTest(uses=uses):
                self.assertRegex(uses, SHA_PIN_RE)

    def test_reuses_published_callee_and_separates_source(self):
        calls = [job for job in self.wf["jobs"].values() if "uses" in job]
        self.assertEqual({job["uses"] for job in calls}, {CALLEE})
        self.assertNotIn("./.github/workflows", self.text, "影子必须引用已发布 callee，不能用本地相对引用")
        for job in calls:
            with self.subTest(job=job.get("name")):
                self.assertNotIn("secrets", job)
                self.assertEqual(job["with"]["profile"], "hosted")
                # source（github.sha）由调用方给；automation 身份由 callee 经 job.workflow_* 解析。
                self.assertIn("source_sha", job["with"])
                self.assertIn("github.sha", job["with"]["source_sha"])

    def test_changed_files_come_from_real_diff_not_caller_input(self):
        text = body(SHADOW)
        self.assertIn("scripts/ci/changed_files.py", text)
        # 不接受调用方任意 shell/jobs 输入：不得声明 workflow_call/workflow_dispatch 输入。
        self.assertNotIn("workflow_call", text)
        self.assertNotIn("workflow_dispatch", text)

    def test_required_and_optional_are_disjoint_and_build_is_optional(self):
        plan = next(j for j in self.wf["jobs"].values()
                    if "uses" in j and len(j["needs"]) == 1)
        required = json.loads(plan["with"]["required_checks_json"])
        optional = json.loads(plan["with"]["optional_checks_json"])
        self.assertTrue(required, "required 不得为空（防全跳过变绿）")
        self.assertFalse(set(required) & set(optional))
        self.assertEqual(optional, ["build"])
        # 重型 build 为 optional：仅 docs-only 允许跳过；若 required 则 docs-only 会误判失败。
        build = self.wf["jobs"]["build"]
        self.assertIn("docs-only", str(build["if"]))
        self.assertIn("plan.result", str(build["if"]))

    def test_result_job_aggregates_always_and_passes_all_job_results(self):
        result = self.wf["jobs"]["result"]
        self.assertEqual(result["if"], "${{ always() }}")
        self.assertEqual(set(result["needs"]), {"changes", "plan", "build"})
        results_expr = result["with"]["results_json"]
        for key in ("changes", "plan", "build"):
            self.assertIn(f'"{key}":"', results_expr)
        for ref in ("needs.changes.result", "needs.plan.result", "needs.build.result"):
            self.assertIn(ref, results_expr)

    def test_result_job_has_conservative_fallbacks(self):
        # changes 失败时其 outputs 为空；result 必须回退为保守值，才能仍汇聚真实 job 结果
        # （空变更 + unknown → callee 判 unknown→执行全部 → 必跑 skipped 即 failure），
        # 而不是让 callee 因缺输入直接拒绝、绕过结果汇聚。
        with_block = self.wf["jobs"]["result"]["with"]
        self.assertIn("|| '[]'", with_block["changed_files_json"])
        self.assertIn("|| 'unknown'", with_block["classifier_status"])
        self.assertIn("github.event_name", with_block["event_json"])

    def test_no_artifact_and_no_cache(self):
        text = body(SHADOW)
        self.assertNotIn("upload-artifact", text)
        self.assertNotIn("download-artifact", text)
        self.assertNotIn("actions/cache", text)
        self.assertNotIn("ccache", text)

    def test_inline_scripts_pass_bash_n(self):
        for name, job in self.wf["jobs"].items():
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
# 与现役 ci.yml 的直接耦合（防漂移）
# --------------------------------------------------------------------------- #
class CiYmlCouplingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ci = raw(CI_YML)
        cls.gate = raw(GATE)
        cls.res = raw(RES_DEPS)

    def test_resource_deps_pins_match_ci_yml(self):
        # ci.yml 内联资源依赖构建使用的固定 tag/commit 必须与唯一 recipe 一致。
        for needle in ("v1.4.1", "616f2286ba5503f74ae96e720623fa11dbc690af",
                       "2.5.4", "r4.6.0"):
            with self.subTest(needle=needle):
                self.assertIn(needle, self.ci)
                self.assertIn(needle, self.res)

    def test_ctest_criteria_match_ci_yml(self):
        self.assertIn("ctest -j1 --output-on-failure --timeout 180", self.ci)
        self.assertIn("ctest --test-dir \"$BUILD_DIR\" -j1 --output-on-failure --timeout 180",
                      self.gate)
        for unit in ("redis.unit", "mongo.unit"):
            with self.subTest(unit=unit):
                self.assertIn(unit, self.ci)
                self.assertIn(unit, self.gate)

    def test_resource_prefix_ab_regression_included(self):
        self.assertIn("tests/resource-prefix/run.sh", self.ci)
        self.assertIn("tests/resource-prefix/run.sh", self.gate)

    def test_build_options_match_ci_yml(self):
        for needle in ("-DCMAKE_BUILD_TYPE=Release", "-DBUILD_TESTING=ON",
                       "-DBBT_COROUTINE_SOURCE_DIR", "-DBBT_CORE_SOURCE_DIR",
                       "-DBBT_HIREDIS_PREFIX", "-DBBT_MONGOCXX_PREFIX", "-DBBT_MONGOC_PREFIX"):
            with self.subTest(needle=needle):
                self.assertIn(needle, self.ci)
                self.assertIn(needle, self.gate)

    def test_dependency_source_assertions_preserved(self):
        for needle in ("libbbt_coroutine.so", "libbbt_core.so", "ldd"):
            with self.subTest(needle=needle):
                self.assertIn(needle, self.ci)
                self.assertIn(needle, self.gate)


# --------------------------------------------------------------------------- #
# 分类路由冒烟：真实复用 framework callee 逻辑（不复制契约）
# --------------------------------------------------------------------------- #
SHARED = os.environ.get("BBT_CI_SHARED_DIR", "")


def _shadow_payload(changed_files_json, classifier_status="ok"):
    return {
        "repo": "yqm-307/bbtools-infra",
        "source_sha": "a" * 40,
        "profile": "hosted",
        "concurrency": "2",
        "timeout_minutes": "60",
        "event_json": '{"name":"pull_request","base_ref":"main"}',
        "changed_files_json": changed_files_json,
        "required_checks_json": '["changes","plan"]',
        "optional_checks_json": '["build"]',
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
            json.dump(_shadow_payload(changed, status), handle)
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
        proc = self._classify('["docs/ci/infra-shadow-v1.md"]')
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

    def test_docs_only_allows_build_skip_and_is_success(self):
        plan = self._plan_for('["docs/ci/x.md"]')
        proc = self._evaluate(plan, {"changes": "success", "plan": "success", "build": "skipped"})
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "success")

    def test_code_build_failure_is_failure(self):
        plan = self._plan_for('["src/x.cc"]')
        proc = self._evaluate(plan, {"changes": "success", "plan": "success", "build": "failure"})
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "failure")

    def test_required_skipped_is_failure(self):
        plan = self._plan_for('["src/x.cc"]')
        proc = self._evaluate(plan, {"changes": "success", "plan": "skipped", "build": "skipped"})
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "failure")

    def test_unknown_classification_forbids_build_skip(self):
        plan = self._plan_for("[]", status="failed")
        proc = self._evaluate(plan, {"changes": "success", "plan": "success", "build": "skipped"})
        self.assertEqual(proc.returncode, 1)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "failure")

    def test_required_cancelled_is_failure(self):
        plan = self._plan_for('["src/x.cc"]')
        proc = self._evaluate(plan, {"changes": "cancelled", "plan": "success", "build": "success"})
        self.assertEqual(proc.returncode, 1)

    def test_code_build_success_is_success(self):
        plan = self._plan_for('["src/x.cc"]')
        proc = self._evaluate(plan, {"changes": "success", "plan": "success", "build": "success"})
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
