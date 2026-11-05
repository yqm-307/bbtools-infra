#!/usr/bin/env python3
"""changed_files.py 的分类规则与 result.py 的结果契约单元测试（仅标准库）。

分类用例集按 CI 骨架验收要求覆盖：CMakeLists.txt、*.cmake、*.py、
.github/workflows/** 判 code；纯 md/docs 判 docs-only；混合判 code；
空集/非 ok 状态判 unknown。

运行：
  PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v scripts/ci/test_changed_files.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import changed_files  # noqa: E402
import result as result_mod  # noqa: E402

ENV = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")


class ClassifyPathTests(unittest.TestCase):
    """单路径分类：doc | code。"""

    def test_cmake_lists_txt_is_code(self):
        # 构建入口绝不允许被 .txt 后缀带成 docs-only。
        for path in ("CMakeLists.txt", "tests/CMakeLists.txt",
                     "bbt/infra/CMakeLists.txt", "examples/co_net_service/CMakeLists.txt"):
            with self.subTest(path=path):
                self.assertEqual(changed_files.classify_path(path), "code")

    def test_cmake_suffix_is_code(self):
        for path in ("cmake/NetworkOnly.cmake", "cmake/x.CMAKE"):
            with self.subTest(path=path):
                self.assertEqual(changed_files.classify_path(path), "code")

    def test_python_is_code(self):
        for path in ("scripts/ci/changed_files.py", "scripts/ci/tests/run_tests.py",
                     "examples/config_consumer/consumer.py"):
            with self.subTest(path=path):
                self.assertEqual(changed_files.classify_path(path), "code")

    def test_workflows_are_code(self):
        for path in (".github/workflows/ci.yml", ".github/workflows/x.yaml"):
            with self.subTest(path=path):
                self.assertEqual(changed_files.classify_path(path), "code")

    def test_markdown_and_docs_are_doc(self):
        for path in ("README.md", "docs/guides/ci.md", "docs/architecture/x.md",
                     "AGENTS.md", "CHANGELOG.txt", "docs/pic.png", "LICENSE"):
            with self.subTest(path=path):
                self.assertEqual(changed_files.classify_path(path), "doc")

    def test_source_and_unknown_suffix_are_code(self):
        for path in ("bbt/infra/net/Engine.cc", "bbt/infra/x.hpp",
                     "scripts/ci/release_gate.sh", "deps.lock", "docker/Dockerfile",
                     "tests/Test_net_tcp.cc"):
            with self.subTest(path=path):
                self.assertEqual(changed_files.classify_path(path), "code")

    def test_ci_gate_inputs_are_code_not_doc(self):
        # scripts/ci/ 下的 .txt 门禁清单是测试选集输入，绝不允许落入 docs-only。
        for path in ("scripts/ci/required_smoke.txt",
                     "scripts/ci/required_rpc_http_net.txt",
                     "scripts/ci/required_debug.txt",
                     "scripts/ci/required_network_tests.txt",
                     "scripts/ci/notes.txt"):
            with self.subTest(path=path):
                self.assertEqual(changed_files.classify_path(path), "code")
        # 仅改 required_smoke.txt 的变更集必须是 code，不是 docs-only。
        self.assertEqual(
            changed_files.classify_changes(["scripts/ci/required_smoke.txt"]),
            "code")


class ClassifyChangesTests(unittest.TestCase):
    """变更集分类：docs-only | code | unknown。"""

    def test_pure_docs_set_is_docs_only(self):
        self.assertEqual(
            changed_files.classify_changes(["docs/guides/ci.md", "README.md"]),
            "docs-only",
        )

    def test_empty_set_is_unknown(self):
        self.assertEqual(changed_files.classify_changes([]), "unknown")

    def test_mixed_set_is_code(self):
        # md + CMakeLists.txt 混合必须判 code（防止构建入口改动被文档带偏）。
        self.assertEqual(
            changed_files.classify_changes(["docs/a.md", "CMakeLists.txt"]),
            "code",
        )
        self.assertEqual(
            changed_files.classify_changes(["README.md", "scripts/ci/result.py"]),
            "code",
        )

    def test_status_not_ok_is_unknown(self):
        for status in ("failed", "unknown", "over-budget", ""):
            with self.subTest(status=status):
                self.assertEqual(
                    changed_files.classify_status(status, ["README.md"]), "unknown"
                )
        self.assertEqual(
            changed_files.classify_status("ok", ["README.md"]), "docs-only"
        )


class ChangedFilesRoutingTests(unittest.TestCase):
    """真实 git diff → GITHUB_OUTPUT 的路由（沿用既有口径，新增 classification 输出）。"""

    def _git(self, repo, *args):
        return subprocess.run(["git", "-C", repo, *args], env=ENV,
                              capture_output=True, text=True, check=False)

    def _init_repo(self, repo):
        self._git(repo, "init", "-q", "-b", "main")
        self._git(repo, "config", "user.email", "ci@test")
        self._git(repo, "config", "user.name", "ci")
        self._git(repo, "config", "commit.gpgsign", "false")

    def _commit(self, repo, relpath, content, message):
        full = os.path.join(repo, relpath)
        os.makedirs(os.path.dirname(full), exist_ok=True)
        with open(full, "w", encoding="utf-8") as handle:
            handle.write(content)
        self._git(repo, "add", relpath)
        self._git(repo, "commit", "-q", "-m", message)
        return self._git(repo, "rev-parse", "HEAD").stdout.strip()

    def _run(self, repo, env_extra):
        out_path = os.path.join(repo, ".ci-output")
        env = dict(ENV, GITHUB_OUTPUT=out_path, **env_extra)
        proc = subprocess.run([sys.executable,
                               os.path.join(HERE, "changed_files.py")],
                              env=env, cwd=repo, capture_output=True, text=True)
        with open(out_path, encoding="utf-8") as handle:
            values = dict(line.split("=", 1)
                          for line in handle.read().splitlines() if "=" in line)
        return proc, values

    def test_push_code_change_classified_code(self):
        with tempfile.TemporaryDirectory() as repo:
            self._init_repo(repo)
            first = self._commit(repo, "docs/a.md", "a", "docs")
            second = self._commit(repo, "bbt/infra/x.cc", "x", "code")
            proc, values = self._run(
                repo, {"EVENT_NAME": "push", "BEFORE": first, "SHA": second,
                       "REF_NAME": "main"})
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(values["classifier_status"], "ok")
            self.assertEqual(values["classification"], "code")
            self.assertEqual(json.loads(values["changed_files_json"]),
                             ["bbt/infra/x.cc"])

    def test_push_docs_change_classified_docs_only(self):
        with tempfile.TemporaryDirectory() as repo:
            self._init_repo(repo)
            first = self._commit(repo, "x.cc", "x", "code")
            second = self._commit(repo, "docs/guides/ci.md", "d", "docs")
            proc, values = self._run(
                repo, {"EVENT_NAME": "push", "BEFORE": first, "SHA": second,
                       "REF_NAME": "main"})
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(values["classification"], "docs-only")

    def test_pr_cmake_change_is_code(self):
        with tempfile.TemporaryDirectory() as repo:
            self._init_repo(repo)
            base = self._commit(repo, "docs/a.md", "a", "base")
            self._git(repo, "update-ref", "refs/remotes/origin/main", base)
            self._git(repo, "checkout", "-q", "-b", "feat")
            head = self._commit(repo, "CMakeLists.txt", "c", "cmake")
            proc, values = self._run(
                repo, {"EVENT_NAME": "pull_request", "BASE_REF": "main", "SHA": head})
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(json.loads(values["changed_files_json"]),
                             ["CMakeLists.txt"])
            self.assertEqual(values["classification"], "code")

    def test_unknown_range_is_conservative_unknown(self):
        with tempfile.TemporaryDirectory() as repo:
            self._init_repo(repo)
            first = self._commit(repo, "bbt/infra/x.cc", "x", "one")
            proc, values = self._run(
                repo, {"EVENT_NAME": "push", "BEFORE": "f" * 40, "SHA": first})
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(values["changed_files_json"], "[]")
            self.assertEqual(values["classifier_status"], "unknown")
            self.assertEqual(values["classification"], "unknown")


class FocusedRegexTests(unittest.TestCase):
    """FOCUS_MAP：变更路径前缀 → ctest 测试名正则。"""

    def test_module_paths_map_to_test_prefixes(self):
        cases = {'bbt/infra/config/detail/CloseSupport.hpp': '^config\\.', 'bbt/infra/mongo/detail/MongoSupport.hpp': '^mongo\\.', 'bbt/infra/CoMongoCli.hpp': '^mongo\\.', 'bbt/infra/net/Engine.cc': '^net\\.|^http\\.net\\.|^redis\\.net\\.|^rpc\\.net\\.', 'bbt/infra/http/Client.cc': '^http\\.net\\.|^rpc\\.net\\.|^mcp\\.http', 'bbt/infra/redis/CoRedisCli.cc': '^redis\\.net\\.', 'bbt/infra/debug/InfraDebug.hpp': '^mongo\\.', 'tests/Test_config_watch.cc': '^config\\.', 'tests/Test_mongo_unit.cc': '^mongo\\.', 'tests/Test_rpc_payload.cc': '^rpc\\.payload', 'tests/http_net/Test_x.cc': '^http\\.net\\.', 'tests/redis_net/Test_x.cc': '^redis\\.net\\.', 'tests/rpc_net/Test_x.cc': '^rpc\\.net\\.', 'cmake/RpcNet.cmake': '^contract\\.|^net\\.|^http\\.net\\.|^redis\\.net\\.|^rpc\\.', 'CMakeLists.txt': '^contract\\.|^net\\.|^http\\.net\\.|^redis\\.net\\.|^rpc\\.'}
        for path, expected in cases.items():
            with self.subTest(path=path):
                self.assertEqual(
                    changed_files.focused_regex([path], "ok"), expected)

    def test_unmatched_and_non_ok_status_yield_empty(self):
        self.assertEqual(changed_files.focused_regex(["docs/a.md"], "ok"), "")
        self.assertEqual(changed_files.focused_regex(["README.md"], "ok"), "")
        self.assertEqual(changed_files.focused_regex([], "ok"), "")
        self.assertEqual(
            changed_files.focused_regex(["bbt/infra/http/Client.cc"], "over-budget"), "")
        self.assertEqual(
            changed_files.focused_regex(["bbt/infra/http/Client.cc"], "unknown"), "")

    def test_union_dedup_preserves_order(self):
        out = changed_files.focused_regex(
            ["bbt/infra/net/Engine.cc", "bbt/infra/http/Client.cc", "bbt/infra/net/Server.cc"], "ok")
        parts = out.split("|")
        self.assertEqual(len(parts), len(set(parts)))
        self.assertIn(r"^net\.", out)
        self.assertIn(r"^http\.net\.", out)
        self.assertIn(r"^redis\.net\.", out)
        self.assertLess(out.index(r"^net\."), out.index(r"^http\.net\."))



class ResultContractTests(unittest.TestCase):
    """result.py：必跑集失败闭合、docs-only 放行 optional 跳过、防全跳过变绿。"""

    def _eval(self, results, required, optional=(), classification="code"):
        return result_mod.evaluate(results, required, optional, classification)

    def test_all_success_is_success(self):
        out = self._eval({"changes": "success", "build": "success", "test": "success"},
                         ["changes"], ["build", "test"])
        self.assertEqual(out["verdict"], "success")
        self.assertEqual(out["failed"], [])

    def test_docs_only_optional_skipped_is_success(self):
        out = self._eval({"changes": "success", "build": "skipped", "test": "skipped"},
                         ["changes"], ["build", "test"], "docs-only")
        self.assertEqual(out["verdict"], "success")
        self.assertEqual(sorted(out["skipped"]), ["build", "test"])

    def test_expected_skip_missing_result_key_is_failure(self):
        # expected_skips 只放行显式 skipped；结果键缺失必须判失败。
        out = result_mod.evaluate(
            {"changes": "success", "docs-check": "success",
             "build-and-gates": "success"},
            ["changes", "docs-check"], ["build-and-gates", "live"],
            "code", ["live"])
        self.assertEqual(out["verdict"], "failure")
        self.assertEqual(out["failed"], ["live"])
        self.assertEqual(out["skipped"], [])

    def test_docs_only_missing_result_key_is_failure(self):
        # docs-only 同样不放行缺失键：optional skipped 显式才放行。
        out = result_mod.evaluate(
            {"changes": "success"}, ["changes"], ["build"], "docs-only")
        self.assertEqual(out["verdict"], "failure")
        self.assertEqual(out["failed"], ["build"])

    def test_code_optional_skipped_is_failure(self):
        out = self._eval({"changes": "success", "build": "skipped", "test": "success"},
                         ["changes"], ["build", "test"], "code")
        self.assertEqual(out["verdict"], "failure")
        self.assertEqual(out["failed"], ["build"])

    def test_unknown_classification_forbids_skip(self):
        out = self._eval({"changes": "success", "build": "skipped"},
                         ["changes"], ["build"], "unknown")
        self.assertEqual(out["verdict"], "failure")

    def test_required_non_success_is_failure(self):
        for status in ("failure", "cancelled", "skipped"):
            with self.subTest(status=status):
                out = self._eval({"changes": status, "build": "success"},
                                 ["changes"], ["build"])
                self.assertEqual(out["verdict"], "failure")

    def test_required_missing_is_failure(self):
        out = self._eval({"build": "success"}, ["changes"], ["build"])
        self.assertEqual(out["verdict"], "failure")
        self.assertEqual(out["failed"], ["changes"])

    def test_rejects(self):
        with self.assertRaises(result_mod.ContractError):
            self._eval({}, ["changes"], [])
        with self.assertRaises(result_mod.ContractError):
            self._eval({"changes": "success"}, [], [])  # 空 required 拒绝
        with self.assertRaises(result_mod.ContractError):
            self._eval({"changes": "success", "other": "success"},
                       ["changes"], [])  # 集合外 job 拒绝
        with self.assertRaises(result_mod.ContractError):
            self._eval({"changes": "bogus"}, ["changes"], [])  # 非法状态拒绝
        with self.assertRaises(result_mod.ContractError):
            self._eval({"a": "success"}, ["a"], ["a"])  # required/optional 重叠

    def test_cli_exit_codes(self):
        script = os.path.join(HERE, "result.py")

        def run(payload):
            return subprocess.run([sys.executable, script], input=payload,
                                  capture_output=True, text=True, env=ENV)

        ok = run(json.dumps({"results": {"changes": "success"},
                             "required": ["changes"], "classification": "code"}))
        self.assertEqual(ok.returncode, 0, ok.stdout + ok.stderr)
        self.assertEqual(json.loads(ok.stdout)["verdict"], "success")

        bad = run(json.dumps({"results": {"changes": "failure"},
                              "required": ["changes"], "classification": "code"}))
        self.assertEqual(bad.returncode, 1)
        self.assertEqual(json.loads(bad.stdout)["verdict"], "failure")

        rejected = run("not json")
        self.assertEqual(rejected.returncode, 3)
        self.assertFalse(json.loads(rejected.stdout)["ok"])

        missing = run(json.dumps({"results": {}}))
        self.assertEqual(missing.returncode, 3)


if __name__ == "__main__":
    unittest.main()
