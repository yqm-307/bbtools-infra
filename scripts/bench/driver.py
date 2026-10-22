#!/usr/bin/env python3
"""infra-perf 闭环：纯 TCP echo 基准编排 driver（Python 标准库，无第三方）。

职责：
  - 以独立子进程拉起 tcp_echo_server（raw/co）与 tcp_loadgen（POSIX 参照）；
    server 用 port=0 内核分配动态端口（本进程独有，不共享端口）。
  - 统一测量窗：同一 server 进程上先跑一个独立 warmup 调用（loadgen
    --warmup-ms=0，时长 --warmup-ms），等它退出且 server 报告 active_conns==0，
    再取 server stat 快照作为测量窗起点，随后跑 measured loadgen
    （--warmup-ms=0，--duration-ms）。吞吐量/时延/操作数全部来自 measured
    调用；server 侧错误计数跨两阶段累计（warmup 发生的错误如实计入 server
    最终 JSON——warmup 清零会掩盖真实失败）。
  - 采集两端单行 JSON 指标并合成一份结构化结果（含运行身份/参数/flag）。
  - 进程级 watchdog + 自身 PID 专属 teardown：只终止本 driver 拉起的子进程
    （进程组 SIGTERM→SIGKILL），从不按端口/PID 范围清理不相关进程。

输出：stdout 打印一行聚合 JSON（也可 --out 写文件）；server/loadgen 的 stderr
分别落 --log-dir 下 server.stderr / loadgen.stderr / loadgen-warmup.stderr
供取证。

校验（fail-closed，任何一项不符都 ok=false，不拿部分数据冒充成功）：
  - server/loadgen 两进程都必须产出合法 JSON（缺 JSON/schema 不符即拒绝）；
  - server.proc_ok 与 loadgen.proc_ok 必须 true——/proc 不可用时测量无效，
    不允许 0 值冒充采样成功；
  - loadgen.completed 必须 >0（零工作/部分完成即无效测量）；正式计时还须
    completed>=10000 才解释 p99（min-samples 由 verifier 判，driver 如实上报）；
  - 字节对账：server.echoed_fin 覆盖 conns 客户端发出的每连接 fin；pingpong/
    transfer 的 server.completed 必须与 loadgen.completed 一致（逐帧 echo 语义）。
    conns 负载下 server.completed==loadgen.completed 且 fin_overhead 对账
    echoed_fin==server.accepted（每个已接受连接恰收到一个 fin）。

诚实边界：本 driver 只编排+归并两进程已上报指标，不做跨进程统计推断、不做
正式性能结论（baseline/candidate 配对矩阵由 verifier 复跑本 driver 产生原始
JSON 后离线分析）。server 崩溃/hang 记为非零与原因，不伪造通过。
"""

import argparse
import json
import os
import queue
import signal
import subprocess
import sys
import threading
import time

READY_PREFIX = "PORT="
READY_TIMEOUT_S = 15.0


class _StdoutLines:
    """单进程 stdout 的持久按行读取器（一次启动，直到 EOF；None 哨兵）。"""

    def __init__(self, proc):
        self.proc = proc
        self.q = queue.Queue()
        self.eof = False
        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        try:
            for raw in iter(self.proc.stdout.readline, b""):
                self.q.put(raw.decode("utf-8", "replace").rstrip("\r\n"))
        finally:
            self.eof = True
            self.q.put(None)

    def next_line(self, timeout_s):
        try:
            return self.q.get(timeout=max(0.0, timeout_s))
        except queue.Empty:
            return ""

    def await_ready(self, timeout_s):
        """等 PORT= 行；返回 (port, early_lines)。超时/EOF → (0, early)。"""
        deadline = time.monotonic() + timeout_s
        early = []
        while time.monotonic() < deadline:
            line = self.next_line(deadline - time.monotonic())
            if line is None or line == "":
                if line is None:
                    break
                continue
            if line.startswith(READY_PREFIX):
                try:
                    return int(line[len(READY_PREFIX):]), early
                except ValueError:
                    early.append(line)
                    continue
            early.append(line)
        return 0, early

    def await_role_json(self, role, timeout_s):
        """等一行 JSON 且 role 匹配；超时/EOF 返回 None。"""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            line = self.next_line(deadline - time.monotonic())
            if line is None or line == "":
                if line is None:
                    return None
                continue
            if line.startswith("{"):
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if obj.get("role") == role:
                    return obj
        return None

    def await_last_json(self, timeout_s):
        """收 EOF/超时前最后一个 JSON 行；进程退出且队列空即止。"""
        deadline = time.monotonic() + timeout_s
        last = None
        while time.monotonic() < deadline:
            line = self.next_line(0.05)
            if line is None:
                break  # EOF sentinel follows every stdout line in FIFO order.
            if line == "":
                continue
            if line.startswith("{"):
                try:
                    last = json.loads(line)
                except json.JSONDecodeError:
                    pass
        return last


def _start_process(cmd, stderr_path):
    fh = open(stderr_path, "wb")
    return subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=fh, bufsize=0, preexec_fn=os.setsid)


def _kill_pgroup(proc, sig):
    try:
        os.killpg(os.getpgid(proc.pid), sig)
        return True
    except (ProcessLookupError, PermissionError):
        try:
            proc.send_signal(sig)
        except ProcessLookupError:
            pass
        return False


def _teardown(proc, grace_s):
    """SIGTERM → grace → SIGKILL（进程组级），回收退出码；幂等。"""
    if proc.poll() is not None:
        return proc.returncode
    _kill_pgroup(proc, signal.SIGTERM)
    try:
        return proc.wait(timeout=grace_s)
    except subprocess.TimeoutExpired:
        pass
    _kill_pgroup(proc, signal.SIGKILL)
    try:
        return proc.wait(timeout=grace_s)
    except subprocess.TimeoutExpired:
        return -9


def _stdin_cmd(proc, text):
    try:
        proc.stdin.write(text.encode("utf-8"))
        proc.stdin.flush()
        return True
    except (BrokenPipeError, ValueError, OSError):
        return False


def _server_stat(server_lines, server, timeout_s=5.0):
    """向 server 发 stat，等一行 server_stat JSON；None=失败。"""
    if not _stdin_cmd(server, "stat\n"):
        return None
    return server_lines.await_role_json("server_stat", timeout_s)


def _await_conns_idle(server_lines, server, timeout_s):
    """轮询 stat 直到 active_conns==0（warmup 连接全部释放）。"""
    deadline = time.monotonic() + timeout_s
    last = None
    while time.monotonic() < deadline:
        last = _server_stat(server_lines, server, 3.0)
        if last is None:
            return None
        if int(last.get("active_conns", -1)) == 0:
            return last
        time.sleep(0.05)
    return last


# loadgen/server JSON 必填键（schema 轻校验：缺键即拒绝，不拿半成品冒充）。
_SERVER_KEYS = ("role", "mode", "accepted", "completed", "echoed_fin",
                "bytes_in", "bytes_out", "errors", "frames_bad", "seq_bad",
                "proc_ok", "active_conns_end", "rss_max_bytes")
_LOADGEN_KEYS = ("role", "mode", "clients", "conns_ok", "completed",
                 "bytes_out", "bytes_in", "errors", "seq_bad", "timeouts",
                 "measured_wall_seconds", "proc_ok", "latency_samples")


def _schema_ok(obj, keys):
    return isinstance(obj, dict) and all(k in obj for k in keys)


def run_one(server_bin, loadgen_bin, mode, workload, clients, payload_bytes,
            duration_ms, warmup_ms, runtime_workers, subs, timeout_ms,
            log_dir, meta):
    """单次配对运行；返回聚合 dict。任何阶段异常都先 teardown 再如实上报。"""
    os.makedirs(log_dir, exist_ok=True)
    result = {
        "mode": mode, "workload": workload, "clients": clients,
        "payload_bytes": payload_bytes, "duration_ms": duration_ms,
        "warmup_ms": warmup_ms, "runtime_workers": runtime_workers,
        "subs": subs, "meta": meta,
        "port": 0, "server": None, "loadgen": None,
        "warmup_loadgen": None,
        "server_rc": None, "loadgen_rc": None, "warmup_rc": None,
        "ok": False, "error": None,
    }
    server = None
    loadgen = None
    warm = None
    try:
        server = _start_process(
            [server_bin, f"--mode={mode}",
             f"--runtime-workers={runtime_workers}", f"--subs={subs}"],
            os.path.join(log_dir, "server.stderr"))
        server_lines = _StdoutLines(server)
        port, early = server_lines.await_ready(READY_TIMEOUT_S)
        if port == 0:
            result["error"] = "server 未在 %ss 内输出 PORT=（early=%s）" % (
                READY_TIMEOUT_S, early[:5])
            result["server_rc"] = _teardown(server, 3.0)
            return result
        result["port"] = port

        # ---- 统一测量窗：同一 server 上先跑独立 warmup 调用（warmup=0，时长
        # warmup_ms），等其退出且 server 报告连接全释放，再取 stat 起点快照。
        warmup_json = None
        if warmup_ms > 0:
            warm = _start_process(
                [loadgen_bin, f"--mode={workload}", f"--port={port}",
                 f"--clients={clients}", f"--payload-bytes={payload_bytes}",
                 f"--duration-ms={warmup_ms}", "--warmup-ms=0"],
                os.path.join(log_dir, "loadgen-warmup.stderr"))
            warm_lines = _StdoutLines(warm)
            # warmup 窗口 + 收尾余量（fin/drain 最长 ~4s）作为硬上限。
            warm_budget = max(timeout_ms / 1000.0, warmup_ms / 1000.0 + 12.0)
            warmup_json = warm_lines.await_last_json(warm_budget)
            result["warmup_rc"] = (_teardown(warm, 3.0)
                                   if warm.poll() is None else warm.returncode)
            warm = None
            if warmup_json is None:
                result["error"] = ("warmup loadgen 未产生 JSON（rc=%s）"
                                   % result["warmup_rc"])
                return result
            # warmup 失败同样计入证据（server 错误已在其计数器累计，不归零）。
            result["warmup_loadgen"] = warmup_json
            if (result["warmup_rc"] != 0 or not _schema_ok(warmup_json, _LOADGEN_KEYS)
                    or not warmup_json.get("proc_ok")
                    or int(warmup_json.get("completed", 0)) <= 0
                    or any(warmup_json.get(k, 0) for k in ("errors", "seq_bad", "timeouts"))):
                result["error"] = "warmup failed; measured phase refused"
                return result
            # 等 server 把 warmup 连接全部释放（测量窗边界=无残留连接）。
            idle = _await_conns_idle(server_lines, server, 15.0)
            if idle is None:
                result["error"] = "server stat 不可用或连接未在 15s 内归零"
                return result
            if int(idle.get("active_conns", -1)) != 0:
                result["error"] = ("warmup 后 server 仍有 %s 个未释放连接"
                                   % idle.get("active_conns"))
                return result
            result["measurement_start"] = idle  # server 视角测量窗起点快照
            if workload == "conns":
                # conns 稳态对账：此时已接受连接应全部收到 fin（每连接恰一）。
                if not _stdin_cmd(server, "fin_overhead\n"):
                    result["error"] = "server fin_overhead 命令失败"
                    return result
                fin = server_lines.await_role_json("fin_overhead", 5.0)
                if fin is None:
                    result["error"] = "server 未回 fin_overhead"
                    return result
                result["fin_overhead_start"] = fin

        if "measurement_start" not in result:
            result["measurement_start"] = _server_stat(server_lines, server)
        if not result["measurement_start"] or not result["measurement_start"].get("proc_ok"):
            result["error"] = "measurement start /proc snapshot unavailable"
            return result

        # ---- measured 调用（warmup=0）：全部业务指标只覆盖测量窗。
        loadgen = _start_process(
            [loadgen_bin, f"--mode={workload}", f"--port={port}",
             f"--clients={clients}", f"--payload-bytes={payload_bytes}",
             f"--duration-ms={duration_ms}", "--warmup-ms=0"],
            os.path.join(log_dir, "loadgen.stderr"))
        lg_lines = _StdoutLines(loadgen)
        lg_json = lg_lines.await_last_json(
            max(timeout_ms / 1000.0, duration_ms / 1000.0 + 12.0))
        result["loadgen_rc"] = (_teardown(loadgen, 3.0)
                                if loadgen.poll() is None else loadgen.returncode)
        loadgen = None

        if workload == "conns":
            # measured 段 fin 对账：收齐后 echoed_fin 应追上 accepted。
            deadline = time.monotonic() + 8.0
            fin = None
            while time.monotonic() < deadline:
                if not _stdin_cmd(server, "fin_overhead\n"):
                    break
                fin = server_lines.await_role_json("fin_overhead", 5.0)
                if fin is None:
                    break
                if int(fin.get("echoed_fin", 0)) >= int(fin.get("accepted", 0)):
                    break
                time.sleep(0.05)
            result["fin_overhead_end"] = fin

        result["measurement_end"] = _server_stat(server_lines, server)
        start = result["measurement_start"]
        end = result["measurement_end"]
        if not end or not end.get("proc_ok"):
            result["error"] = "measurement end /proc snapshot unavailable"
            return result
        count = int(lg_json.get("completed", 0)) if lg_json else 0
        cpu_ticks = int(end["cpu_ticks"]) - int(start["cpu_ticks"])
        cpu_seconds = cpu_ticks / int(end["ticks_per_second"])
        result["measurement"] = {
            "server_cpu_seconds": cpu_seconds,
            "server_cpu_per_op": cpu_seconds / count if count > 0 else None,
            "server_rss_end_bytes": int(end["rss_bytes"]),
            "server_rss_start_bytes": int(start["rss_bytes"]),
            "window": "server stat before loadgen launch to stat after loadgen exit; excludes warmup and engine teardown",
        }

        if server.stdin is not None:
            try:
                server.stdin.write(b"quit\n")
                server.stdin.flush()
                server.stdin.close()
            except (BrokenPipeError, ValueError):
                pass
        srv_json = server_lines.await_last_json(10.0)
        result["server_rc"] = (_teardown(server, 3.0)
                               if server.poll() is None else server.returncode)

        result["server"] = srv_json
        result["loadgen"] = lg_json

        # ---- fail-closed 校验：schema、proc_ok、工作量、字节对账。
        if lg_json is None:
            result["error"] = "loadgen 未产生 JSON（rc=%s）" % result["loadgen_rc"]
        elif srv_json is None:
            result["error"] = "server 未产生 JSON（rc=%s）" % result["server_rc"]
        elif not _schema_ok(srv_json, _SERVER_KEYS):
            result["error"] = "server JSON schema 不符（缺键）"
        elif not _schema_ok(lg_json, _LOADGEN_KEYS):
            result["error"] = "loadgen JSON schema 不符（缺键）"
        elif not srv_json.get("proc_ok"):
            result["error"] = "server /proc 采样无效（proc_ok=false），测量无效"
        elif not lg_json.get("proc_ok"):
            result["error"] = "loadgen /proc 采样无效（proc_ok=false），测量无效"
        elif int(lg_json.get("completed", 0)) <= 0:
            result["error"] = "loadgen.completed=0：零工作/未完成测量"
        else:
            srv_bad = (srv_json.get("errors", 0) or
                       srv_json.get("frames_bad", 0) or
                       srv_json.get("seq_bad", 0))
            lg_bad = (lg_json.get("errors", 0) or lg_json.get("seq_bad", 0)
                      or lg_json.get("timeouts", 0))
            ok = (srv_bad == 0 and lg_bad == 0 and
                  result["server_rc"] == 0 and result["loadgen_rc"] == 0)
            # 逐帧 echo 语义对账：server.completed 在同一进程上跨 warmup 累计，
            # 故用测量窗 delta（final - measurement_start 快照）与
            # loadgen.completed 一致（丢帧/截断在两侧都不能藏）。无 warmup 时
            # 起点快照缺失，delta 即全量。
            start_snap = result.get("measurement_start") or {}
            srv_completed = (int(srv_json.get("completed", -1))
                             - int(start_snap.get("completed", 0)))
            lg_completed = int(lg_json.get("completed", -1))
            if srv_completed != lg_completed:
                ok = False
                result["error"] = (
                    "字节对账失败：server 测量窗 completed=%s != "
                    "loadgen.completed=%s（逐帧 echo 语义要求一致；"
                    "server 全量=%s 起点快照=%s）"
                    % (srv_completed, lg_completed,
                       srv_json.get("completed"),
                       start_snap.get("completed")))
            if ok and workload == "conns":
                fin_end = result.get("fin_overhead_end")
                fin_start = result.get("fin_overhead_start") or {}
                if fin_end is None:
                    ok = False
                    result["error"] = "missing fin accounting evidence"
                elif \
                        (int(fin_end.get("echoed_fin", 0))
                         - int(fin_start.get("echoed_fin", 0))) < \
                        (int(fin_end.get("accepted", 0))
                         - int(fin_start.get("accepted", 0))):
                    ok = False
                    result["error"] = (
                        "fin 对账失败：测量窗 echoed_fin_delta=%s < "
                        "accepted_delta=%s（conns 每连接恰一 fin，缺失即"
                        "连接未正常收尾）"
                        % (int(fin_end.get("echoed_fin", 0))
                           - int(fin_start.get("echoed_fin", 0)),
                           int(fin_end.get("accepted", 0))
                           - int(fin_start.get("accepted", 0))))
                else:
                    srv_end = srv_json.get("active_conns_end")
                    if srv_end is not None and int(srv_end) != 0:
                        ok = False
                        result["error"] = (
                            "teardown 不全：server 收口后仍有 %s 个未释放连接"
                            % srv_end)
            if ok:
                result["ok"] = True
            elif result["error"] is None:
                result["error"] = ("errors: server_bad=%s loadgen_bad=%s rc=%s/%s"
                                   % (srv_bad, lg_bad,
                                      result["server_rc"], result["loadgen_rc"]))
        return result
    finally:
        for p in (warm, loadgen, server):
            if p is not None and p.poll() is None:
                _teardown(p, 2.0)


def main(argv):
    ap = argparse.ArgumentParser(description="infra-perf TCP echo 基准 driver")
    ap.add_argument("--mode", required=True, choices=["raw", "co"])
    ap.add_argument("--workload", required=True,
                    choices=["pingpong", "transfer", "conns"])
    ap.add_argument("--server-bin", required=True)
    ap.add_argument("--loadgen-bin", required=True)
    ap.add_argument("--clients", type=int, default=2)
    ap.add_argument("--payload-bytes", type=int, default=64)
    ap.add_argument("--duration-ms", type=int, default=800)
    ap.add_argument("--warmup-ms", type=int, default=100,
                    help="预热时长：同一 server 上先跑一次独立 warmup 调用"
                         "（其内部 warmup=0），全部业务指标只落在其后的"
                         " measured 调用上；0 = 不预热直接测量")
    ap.add_argument("--runtime-workers", type=int, default=0,
                    help="0 = 自动取 subs+1（server 需 1 main + subs 个 owner 槽）")
    ap.add_argument("--subs", type=int, default=2)
    ap.add_argument("--timeout-ms", type=int, default=15000,
                    help="loadgen 硬超时（独立于 duration，给收尾余量）")
    ap.add_argument("--log-dir", default=".")
    ap.add_argument("--out", default=None, help="聚合 JSON 写文件（默认仅 stdout）")
    ap.add_argument("--label", default="run", help="结果标签（baseline/candidate/…）")
    ap.add_argument("--toolchain", default="", help="工具链描述（元数据）")
    ap.add_argument("--flags", default="", help="编译 flag 描述（元数据）")
    args = ap.parse_args(argv)

    workers = args.runtime_workers if args.runtime_workers > 0 else args.subs + 1
    meta = {
        "label": args.label,
        "toolchain": args.toolchain,
        "flags": args.flags,
        "driver": os.path.abspath(__file__),
        "server_bin": os.path.abspath(args.server_bin),
        "loadgen_bin": os.path.abspath(args.loadgen_bin),
        "pid": os.getpid(),
    }
    result = run_one(args.server_bin, args.loadgen_bin, args.mode,
                     args.workload, args.clients, args.payload_bytes,
                     args.duration_ms, args.warmup_ms, workers,
                     args.subs, args.timeout_ms, args.log_dir, meta)
    line = json.dumps(result, ensure_ascii=False)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(line + "\n")
    print(line)
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
