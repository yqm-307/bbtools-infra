# 新 Client → 新 Server HTTP 消费示例：只链接 `bbt::infra_http_net`。

本示例演示 Co-only 应用入口。`bbtco_main` 在 Engine Idle 阶段装配 Server 与 Client，再把共享 Engine 与配置发布给宿主。宿主在原生 main 线程调用一次 `Engine::Run`，主任务用 `Engine::CoWaitRunning` 等 Running。`setup_runtime` 在 Start 前冻结线程和栈配置，普通 handler/Get/Post 在协程内直接消费。

主任务结束时，`OnMainTaskDone` 回调对共享 Engine 调用 `Close()`。`Run` 返回后由宿主检查 `Stopped`。出错且仍为 `Closing` 时，宿主调用 `FinalizeNative()` 重试一次，并保留 `Run` 失败的退出结果。参见[宿主关闭契约](../../docs/architecture/native-lifecycle-seam-contract.md#4-关闭与回收)。

示例经 loopback TCP 发出 GET/404/POST 并检查连接复用。服务端通过 `StopAccepting`、`Close(Graceful)` 和 `Close(wait_release=true)` 收口。handler 闭包按值持有请求计数的共享状态，可以安全跨过主任务退栈。

依赖 `BBT_INFRA_HTTP_NET=ON`。在本机 loopback 上运行，无需外部服务。

## 怎么跑

```bash
cmake -S <repo> -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DBBT_INFRA_HTTP_NET=ON -DBUILD_TESTING=ON -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
flock /tmp/bbt-build.lock cmake --build build --target http_net_example --parallel 2
ctest --test-dir build -j1 -R '^example_http_net_consumer$' --output-on-failure --no-tests=error
```

## 期望输出

```
new Client -> new Server accepted=1 requests=3 peer_requests=3 GET=200,404 POST=echo reuse=1
HTTP stopped tasks=0 sessions=0 inflight=0 listener_stopped=1
network stopped engine_run_returned=1 phase=Stopped connections=0
```

退出码 0 表示全部 Check 通过；Engine 的物理收口凭据是 host hook 内 `Engine::Run` 返回且 `Phase() == Stopped`，不是进程退出。
