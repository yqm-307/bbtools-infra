#!/usr/bin/env python3
"""临时 Release 取证；不重试判绿，不上传 core/环境/参数/locals。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import re
import shlex
import shutil
import signal
import subprocess
import tempfile
import time


GDB_COMMANDS = r"""set pagination off
set confirm off
set auto-load off
set debuginfod enabled off
set print frame-arguments none
set print entry-values no
set startup-with-shell off
set disable-randomization off
handle SIGPIPE nostop noprint pass
python
import gdb
result = {"exit": None, "stop": None}
def exited(event):
    result["exit"] = getattr(event, "exit_code", None)
def stopped(event):
    result["stop"] = getattr(event, "stop_signal", "unexpected-stop")
gdb.events.exited.connect(exited)
gdb.events.stop.connect(stopped)
end
run
python
if result["stop"] is not None:
    gdb.execute("thread apply all backtrace 40")
gdb.execute("info sharedlibrary")
code = 0 if result["exit"] == 0 and result["stop"] is None else 1
gdb.write("MCP_DIAG_RESULT exit=%s stop=%s\n" % (result["exit"], result["stop"]))
gdb.execute("quit %d" % code)
end
"""


def child_setup():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))


def run(command, cwd, deadline, output):
    """每个命令独占进程组，正常/失败/超时都清理 debugger 及 inferior。"""
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise TimeoutError("overall diagnostic deadline exceeded")
    with output.open("w") as stream:
        process = subprocess.Popen(command, cwd=cwd, stdout=stream,
                                   stderr=subprocess.STDOUT, start_new_session=True,
                                   preexec_fn=child_setup)
        try:
            return process.wait(timeout=remaining)
        except subprocess.TimeoutExpired as error:
            raise TimeoutError("overall diagnostic deadline exceeded") from error
        finally:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()


def diagnose(args, result, deadline, private):
    build, output = args.build.resolve(), args.output.resolve()
    binary = build / "tests/Test_mcp_teardown"
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise RuntimeError("missing executable: Test_mcp_teardown")
    # 原始 CTest fixture 摘录与后续 debugger 复现明确分开；不上传其它测试日志。
    ctest_log = build / "Testing/Temporary/LastTest.log"
    excerpt = "original ctest outcome: " + args.original_outcome + "\n"
    if ctest_log.exists():
        text = ctest_log.read_text(errors="replace")
        for block in re.split(r"\n(?=\d+/\d+ Testing: )", text):
            if re.search(r"^Test: mcp\.teardown$", block, re.MULTILINE):
                excerpt += block[:24000]
                break
    (output / "original-ctest.txt").write_text(excerpt)
    gdb = shutil.which("gdb")
    if not gdb:
        raise RuntimeError("gdb missing on runner; diagnostics fail closed (no install)")
    for tool in ("readelf",):
        if not shutil.which(tool):
            raise RuntimeError(tool + " missing on runner")
    if run([gdb, "--version"], build, deadline, output / "gdb-version.txt"):
        raise RuntimeError("gdb --version failed")
    artifacts = [binary] + sorted({p.resolve() for p in build.rglob("libbbt_coroutine.so*")})
    if len(artifacts) == 1:
        raise RuntimeError("missing built libbbt_coroutine.so")
    provenance = {"source_sha": args.source_sha, "coroutine_sha": args.coroutine_sha,
                  "core_sha": args.core_sha, "deps_bundle_sha256": args.bundle_sha,
                  "artifacts": [], "compile_flags": [],
                  "limits": "Boost.Context stack switches/optimized frames may not unwind; prebuilt Boost/system libraries may lack symbols"}
    for artifact in artifacts:
        sections = private / "sections.txt"
        if run(["readelf", "-S", "--wide", str(artifact)], build, deadline, sections):
            raise RuntimeError("readelf failed")
        provenance["artifacts"].append({"path": str(artifact.relative_to(build)),
            "sha256": hashlib.file_digest(artifact.open("rb"), "sha256").hexdigest(),
            "debug_info": ".debug_info" in sections.read_text()})
    commands = json.loads((build / "compile_commands.json").read_text())
    for item in commands:
        if "Test_mcp_teardown" in item["file"] or "coroutine" in item["file"]:
            flags = [flag for flag in shlex.split(item["command"])
                     if flag.startswith(("-O", "-g", "-DNDEBUG", "-f"))]
            if flags not in provenance["compile_flags"]:
                provenance["compile_flags"].append(flags)
    (output / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    if not provenance["compile_flags"] or any("-O3" not in flags or "-DNDEBUG" not in flags
                                             or "-g" not in flags for flags in provenance["compile_flags"]):
        raise RuntimeError("expected Release -O3/-DNDEBUG plus -g not found")
    if not all(item["debug_info"] for item in provenance["artifacts"]):
        raise RuntimeError("missing debug_info in teardown/coroutine artifacts")
    commands_file = private / "gdb.commands"
    commands_file.write_text(GDB_COMMANDS)
    for attempt in range(1, args.attempts + 1):
        result["attempts"] = attempt
        log = output / "debugger.txt"
        code = run([gdb, "--nx", "--nh", "--batch", "-iex", "set auto-load off",
                    "-iex", "set debuginfod enabled off", "-x", str(commands_file),
                    "--args", str(binary)], build / "tests", deadline, log)
        if code or "MCP_DIAG_RESULT exit=0 stop=None" not in log.read_text(errors="replace"):
            result["status"] = "rerun_fault_or_debugger_error"
            result["debugger_returncode"] = code
            return 1
    result["status"] = "no_reproduction"
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--original-outcome", choices=("success", "failure", "skipped"), required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--coroutine-sha", required=True)
    parser.add_argument("--core-sha", required=True)
    parser.add_argument("--bundle-sha", required=True)
    parser.add_argument("--attempts", type=int, default=30)
    parser.add_argument("--seconds", type=float, default=270)
    args = parser.parse_args()
    if not 1 <= args.attempts <= 30 or not 0 < args.seconds <= 270:
        parser.error("attempts must be 1..30; seconds must be >0 and <=270")
    started = time.monotonic()
    deadline = started + args.seconds
    args.output.mkdir(parents=True, exist_ok=False)
    result = {"original_ctest_outcome": args.original_outcome, "attempts": 0, "status": "diagnostic_error"}
    code = 1
    def terminate(signum, frame):
        raise RuntimeError("diagnostic interrupted by signal %d" % signum)
    signal.signal(signal.SIGTERM, terminate)
    try:
        # 私有临时文件随异常一起删除；绝不生成 core，不改全局 sysctl。
        with tempfile.TemporaryDirectory(prefix="mcp-diagnostic-") as private:
            code = diagnose(args, result, deadline, Path(private))
    except (OSError, ValueError, RuntimeError, TimeoutError) as error:
        result["error"] = str(error)
    finally:
        result["elapsed_seconds"] = round(time.monotonic() - started, 3)
        (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))
    return code


if __name__ == "__main__":
    raise SystemExit(main())
