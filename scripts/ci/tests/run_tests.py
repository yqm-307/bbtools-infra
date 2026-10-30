#!/usr/bin/env python3
"""bbtools-infra CI 脚本的离线冒烟与契约测试（仅标准库）。

覆盖：
- 配方守卫：scripts/ci/*.sh 的 bash -n、prepare_boost --print-plan 与
  fail-closed 拒绝已存在前缀、deps_bundle 未知子命令/篡改归档/错 expected
  sha 拒绝、影子/被取代脚本已移除；
- changed_files 路由：push/PR 真实 diff 归一、未知保守回退、超预算整集回退、
  classification 输出（本仓内联，替代已删除的 shared callee）；
- result.py 契约：required 非空/失败闭合、docs-only 放行 optional 跳过、
  非法输入拒绝、CLI 退出码；
- prepared deps 闭包：pack→unpack 往返（含 boost-prefix/protobuf-prefix
  成员与 loader）、篡改归档 fail-closed；
- network-only 报告门禁：check_network_tests.py 拒绝缺项/重复/失败/跳过；
- 新增 gate 脚本 bash -n 与 pin 文件校验。

运行：
  PYTHONDONTWRITEBYTECODE=1 python3 scripts/ci/tests/run_tests.py
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
PREPARE_BOOST = os.path.join(CI_DIR, "prepare_boost.sh")
DEPS_BUNDLE = os.path.join(CI_DIR, "deps_bundle.sh")
CHANGED = os.path.join(CI_DIR, "changed_files.py")
RESULT = os.path.join(CI_DIR, "result.py")
RES_DEPS = os.path.join(WORKTREE, "scripts", "prepare_resource_deps.sh")
REDIS_LIVE = os.path.join(WORKTREE, "tests", "redis-live", "run.sh")
MONGO_LIVE = os.path.join(WORKTREE, "tests", "mongo-live", "run.sh")

GATE_SCRIPTS = [
    "verify_coroutine_pin.sh", "prepare_deps.sh", "resolve_ccache.sh",
    "run_ctest_label.sh", "release_gate.sh", "debug_gate.sh",
    "live_redis_gate.sh", "live_mongo_gate.sh", "network_gate.sh",
    "redis_net_gate.sh",
]

ENV = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")


def raw(path: str) -> str:
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def run(cmd, **kwargs):
    return subprocess.run(cmd, capture_output=True, text=True, check=False, **kwargs)


# --------------------------------------------------------------------------- #
# 配方守卫
# --------------------------------------------------------------------------- #
class RecipeGuardTests(unittest.TestCase):
    def test_shell_scripts_parse(self):
        for path in [PREPARE_BOOST, DEPS_BUNDLE, RES_DEPS, REDIS_LIVE, MONGO_LIVE] + \
                    [os.path.join(CI_DIR, s) for s in GATE_SCRIPTS]:
            with self.subTest(path=os.path.basename(path)):
                proc = run(["bash", "-n", path], env=ENV)
                self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_python_scripts_compile(self):
        for path in (CHANGED, RESULT,
                     os.path.join(CI_DIR, "check_network_tests.py"),
                     os.path.abspath(__file__)):
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
        workflows = os.path.join(WORKTREE, ".github", "workflows")
        self.assertFalse(
            os.path.exists(os.path.join(workflows, "ci-shadow-v1.yml")),
            "影子 workflow 应已删除",
        )
        for name in ("run_infra_gate.sh", "required_result_gate.sh"):
            self.assertFalse(
                os.path.exists(os.path.join(CI_DIR, name)), f"{name} 应已删除"
            )
        # shared callee 已删除（D3 本仓内联）。
        self.assertFalse(
            os.path.isdir(os.path.join(CI_DIR, "shared")),
            "scripts/ci/shared 应已删除",
        )

    def test_verify_coroutine_pin_rejects_bad_format(self):
        script = os.path.join(CI_DIR, "verify_coroutine_pin.sh")
        with tempfile.TemporaryDirectory() as td:
            for content, ok in (
                (b"a" * 40 + b"\n", True),
                (b"A" * 40 + b"\n", False),
                (b"a" * 39 + b"\n", False),
                (b"a" * 41 + b"\n", False),
                (b"", False),
            ):
                pin = os.path.join(td, "coroutine.sha")
                with open(pin, "wb") as handle:
                    handle.write(content)
                proc = run(["bash", script, pin], env=ENV)
                self.assertEqual(proc.returncode == 0, ok,
                                 f"content={content!r}: {proc.stdout + proc.stderr}")


# --------------------------------------------------------------------------- #
# changed_files 路由 + 本仓内联分类
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
                 "REF_NAME": "main"},
            )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(values["classifier_status"], "ok")
            self.assertEqual(json.loads(values["changed_files_json"]), ["src/x.cc"])
            self.assertEqual(values["classification"], "code")
            self.assertEqual(
                json.loads(values["event_json"]),
                {"name": "push", "head_ref": "main"},
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
            self.assertEqual(json.loads(values["changed_files_json"]),
                             ["docs/ci/note.md"])
            self.assertEqual(values["classification"], "docs-only")
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
            self.assertEqual(values["classification"], "unknown")

    def test_oversized_change_set_falls_back_to_empty(self):
        with tempfile.TemporaryDirectory() as repo:
            _init_repo(repo)
            first = _commit(repo, "src/gen0.cc", "0", "base")
            for i in range(1, 6):
                _commit(repo, f"src/gen{i}.cc", "x", f"c{i}")
            head = _git(repo, "rev-parse", "HEAD").stdout.strip()
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
            self.assertEqual(values["classification"], "unknown")


# --------------------------------------------------------------------------- #
# result.py 契约（本仓内联，替代 shared result_contract）
# --------------------------------------------------------------------------- #
class ResultContractTests(unittest.TestCase):
    def _cli(self, payload):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
            json.dump(payload, handle)
            path = handle.name
        proc = run([sys.executable, RESULT, "--file", path], env=ENV)
        os.unlink(path)
        return proc

    def _ok(self, results, required, optional=(), classification="code"):
        return self._cli({
            "results": results, "required": required,
            "optional": list(optional), "classification": classification,
        })

    def test_all_success_is_success(self):
        proc = self._ok({"changes": "success", "build": "success"},
                        ["changes"], ["build"])
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)
        self.assertEqual(json.loads(proc.stdout)["verdict"], "success")

    def test_docs_only_allows_optional_skip(self):
        proc = self._ok({"changes": "success", "build": "skipped", "test": "skipped"},
                        ["changes"], ["build", "test"], "docs-only")
        self.assertEqual(proc.returncode, 0, proc.stdout + proc.stderr)

    def test_code_forbids_optional_skip(self):
        proc = self._ok({"changes": "success", "build": "skipped"},
                        ["changes"], ["build"], "code")
        self.assertEqual(proc.returncode, 1)

    def test_required_failure_is_failure(self):
        proc = self._ok({"changes": "failure", "build": "success"},
                        ["changes"], ["build"])
        self.assertEqual(proc.returncode, 1)

    def test_rejects_contract_violations(self):
        for payload, code in (
            ({"results": {}, "required": ["x"]}, "E_RESULT_EMPTY"),
            ({"results": {"x": "success"}, "required": []}, "E_REQUIRED_EMPTY"),
            ({"results": {"x": "success", "y": "success"}, "required": ["x"]},
             "E_RESULT_UNKNOWN_JOB"),
            ({"results": {"x": "bogus"}, "required": ["x"]}, "E_RESULT_STATUS"),
        ):
            proc = self._cli(payload)
            self.assertEqual(proc.returncode, 3, proc.stdout + proc.stderr)
            self.assertEqual(json.loads(proc.stdout)["code"], code)


# --------------------------------------------------------------------------- #
# prepared deps 闭包新行为（含 boost-prefix / protobuf-prefix 成员；负向 fail-closed）
# --------------------------------------------------------------------------- #
class PreparedDepsClosureTests(unittest.TestCase):
    def _fixture(self, root):
        for member in ("coroutine", "bbtools-core", "deps-prefix", "boost-prefix",
                       "protobuf-prefix"):
            os.makedirs(os.path.join(root, member), exist_ok=True)
        os.makedirs(os.path.join(root, "coroutine", "src"), exist_ok=True)
        with open(os.path.join(root, "coroutine", "src", "a.cc"), "w", encoding="utf-8") as fh:
            fh.write("int a;\n")
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
        pb_libdir = os.path.join(root, "protobuf-prefix", "usr", "lib", "x86_64-linux-gnu")
        os.makedirs(pb_libdir, exist_ok=True)
        with open(os.path.join(pb_libdir, "libprotobuf.so.32.0.12"),
                  "w", encoding="utf-8") as fh:
            fh.write("p")
        os.symlink("libprotobuf.so.32.0.12", os.path.join(pb_libdir, "libprotobuf.so"))

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
            self.assertIn("protobuf-prefix", meta_text)
            m = re.search(r"^sha256=([0-9a-f]{64})$", meta_text, re.M)
            self.assertIsNotNone(m, meta_text)
            self.assertIn("symlinks=2", meta_text)

            env_file = os.path.join(td, "gh_env")
            env = dict(ENV, GITHUB_ENV=env_file)
            unpack = run(["bash", DEPS_BUNDLE, "unpack", "--tar", tar, "--meta", meta,
                          "--expect-sha", m.group(1), "--dest", root], env=env)
            self.assertEqual(unpack.returncode, 0, unpack.stdout + unpack.stderr)
            loader = raw(env_file)
            self.assertIn("boost-prefix/lib", loader)
            self.assertIn("deps-prefix/hiredis/lib", loader)
            self.assertIn("protobuf-prefix/usr/lib/x86_64-linux-gnu", loader)

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
# network-only 报告门禁：测试夹具不是构建/运行证据。
# --------------------------------------------------------------------------- #
class NetworkReportTests(unittest.TestCase):
    def setUp(self):
        import runpy
        import xml.etree.ElementTree as ET
        module = runpy.run_path(os.path.join(CI_DIR, 'check_network_tests.py'))
        self.check = module['check']
        names = sorted(module['REQUIRED'])
        names += [f'net.fixture_{i}' for i in range(111 - len(names))]
        self.registration = {'tests': [{'name': name} for name in names]}
        self.report = ET.Element('testsuite')
        for name in names:
            ET.SubElement(self.report, 'testcase', name=name)

    def test_complete_report(self):
        self.check(self.registration, self.report)

    def test_empty_or_incomplete_registration_rejected(self):
        for registration in ({'tests': []}, {'tests': self.registration['tests'][:-1]}):
            with self.assertRaises(ValueError):
                self.check(registration, self.report)

    def test_missing_or_duplicate_result_rejected(self):
        import copy
        for replacement in (None, self.report[0]):
            report = copy.deepcopy(self.report)
            report.remove(report[-1])
            if replacement is not None:
                report.append(copy.deepcopy(replacement))
            with self.assertRaises(ValueError):
                self.check(self.registration, report)

    def test_failed_error_or_skipped_rejected(self):
        import copy
        import xml.etree.ElementTree as ET
        for tag in ('failure', 'error', 'skipped'):
            report = copy.deepcopy(self.report)
            ET.SubElement(report[0], tag)
            with self.assertRaises(ValueError):
                self.check(self.registration, report)


if __name__ == "__main__":
    unittest.main(verbosity=2)
