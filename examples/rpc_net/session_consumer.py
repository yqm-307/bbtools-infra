#!/usr/bin/env python3
"""RPC v2 AppSession 双进程 consumer 驱动（CTest 前台受管父进程）。

对齐 scripts/bench/driver.py 的既有项目惯例（标准库、前台编排、只清自己
拉起的子进程）；bench driver 的 JSON 负载协议不适配本 consumer，故为最小
专用实现。

职责：
  - 前台拉起 server 子进程，从其 stdout 管道读 "PORT <n>" 行取得动态
    loopback 端口（无固定 /tmp 文件、无 shell 后台、无外部端口）；
  - 拉起 client 子进程拨号 + Call/Send，校验退出码与客户端关键断言
    （单 session/link）；
  - 正常收口：关闭 server stdin（EOF）触发 server 协作退出（自身
    StopAccept/Drain/Close），检查 server 退出码与 "SERVER CLOSED ok
    admitted>=2" 收口输出（一次 Call + 一次 Send）；
  - try/finally 兜底 terminate→kill 仅用于异常/超时路径，不能替代正常
    关闭成为通过依据。

诚实边界：任一校验不符即非零退出，不伪造通过。
"""

import queue
import re
import subprocess
import sys
import threading
import time

PORT_TIMEOUT_S = 10.0
CLIENT_TIMEOUT_S = 30.0
SERVER_CLOSE_TIMEOUT_S = 20.0
KILL_GRACE_S = 5.0


class _Lines:
    """单条子进程管道的持久按行读取器（一次启动，读到 EOF）。"""

    def __init__(self, proc, stream):
        self._proc = proc
        self._lines = []
        self._lock = threading.Lock()
        self._eof = threading.Event()
        threading.Thread(target=self._run, args=(stream,), daemon=True).start()

    def _run(self, stream):
        for line in stream:
            with self._lock:
                self._lines.append(line.rstrip("\n"))
        self._eof.set()

    def snapshot(self):
        with self._lock:
            return list(self._lines)

    def wait_for_line(self, prefix, timeout_s):
        """等待首个以 prefix 开头的行；进程退出且输出稳定后快速放弃。"""
        deadline = time.monotonic() + timeout_s
        stable_since = None
        last_len = -1
        while time.monotonic() < deadline:
            lines = self.snapshot()
            for line in lines:
                if line.startswith(prefix):
                    return line
            if len(lines) != last_len:
                last_len = len(lines)
                stable_since = time.monotonic()
            elif (self._proc.poll() is not None and stable_since is not None
                  and time.monotonic() - stable_since > 0.5):
                return None
            time.sleep(0.02)
        return None


def _reap(proc):
    """兜底回收：terminate→kill 本 driver 拉起的子进程，绝不扩大范围。"""
    if proc is None or proc.poll() is not None:
        return False
    proc.terminate()
    try:
        proc.wait(timeout=KILL_GRACE_S)
    except subprocess.TimeoutExpired:
        proc.kill()
        try:
            proc.wait(timeout=KILL_GRACE_S)
        except subprocess.TimeoutExpired:
            pass
    return True


def main(argv):
    if len(argv) != 2:
        print("usage: session_consumer.py <exe>", file=sys.stderr)
        return 2
    exe = argv[1]
    server = None
    client = None
    ok = False
    try:
        server = subprocess.Popen(
            [exe, "server"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, bufsize=1)
        server_out = _Lines(server, server.stdout)
        server_err = _Lines(server, server.stderr)

        port_line = server_out.wait_for_line("PORT ", PORT_TIMEOUT_S)
        if port_line is None:
            print("FAIL server PORT not observed within %.0fs: out=%r err=%r"
                  % (PORT_TIMEOUT_S, server_out.snapshot(),
                     server_err.snapshot()), file=sys.stderr)
            return 1
        port = int(port_line.split()[1])
        print("session consumer server pid=%d port=%d" % (server.pid, port))

        client = subprocess.Popen(
            [exe, "client", str(port)], stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True)
        try:
            client_out, client_err = client.communicate(
                timeout=CLIENT_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            print("FAIL client timed out after %.0fs" % CLIENT_TIMEOUT_S,
                  file=sys.stderr)
            return 1
        if client.returncode != 0:
            print("FAIL client exit=%d out=%r err=%r"
                  % (client.returncode, client_out, client_err),
                  file=sys.stderr)
            return 1
        if "CONSUMER PASS sessions=1 links=1" not in client_out:
            print("FAIL client missing single session/link assertion: "
                  "out=%r err=%r" % (client_out, client_err), file=sys.stderr)
            return 1
        print("client: %s" % client_out.strip().replace("\n", " | "))

        # 正常协作收口：关闭 stdin → server EOF → 自身 StopAccept/Drain/Close。
        if server.stdin is None:
            print("FAIL server stdin pipe unavailable", file=sys.stderr)
            return 1
        server.stdin.close()
        try:
            server.wait(timeout=SERVER_CLOSE_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            print("FAIL server did not exit after stdin EOF within %.0fs: "
                  "out=%r err=%r" % (SERVER_CLOSE_TIMEOUT_S,
                                     server_out.snapshot(),
                                     server_err.snapshot()), file=sys.stderr)
            return 1
        if server.returncode != 0:
            print("FAIL server exit=%d out=%r err=%r"
                  % (server.returncode, server_out.snapshot(),
                     server_err.snapshot()), file=sys.stderr)
            return 1
        closed_line = next(
            (l for l in server_out.snapshot() if l.startswith("SERVER CLOSED ok")),
            None)
        if closed_line is None:
            print("FAIL server missing normal closeout marker: out=%r"
                  % (server_out.snapshot(),), file=sys.stderr)
            return 1
        admitted = re.search(r"admitted=(\d+)", closed_line)
        if admitted is None or int(admitted.group(1)) < 2:
            print("FAIL server admitted<2 (call+send): %r" % (closed_line,),
                  file=sys.stderr)
            return 1
        print("server: %s" % closed_line)

        print("rpc.net.v2_session_consumer PASS port=%d" % port)
        ok = True
        return 0
    finally:
        # 兜底只发生在异常/超时路径；正常路径两进程均已自行退出。
        _reap(client)
        _reap(server)
        if not ok:
            print("session consumer driver did not pass (see FAIL above)",
                  file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
