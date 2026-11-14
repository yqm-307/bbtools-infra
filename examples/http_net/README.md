# 新 Client → 新 Server HTTP 消费示例：只链接 `bbt::infra_http_net`。

本示例演示 Co-only 应用入口：`bbtco_main` 在 Engine Idle 阶段装配 Server 与 Client，随后把共享 Engine 与配置发布给入口 native host hook；`bbtco_main_host_hook` 在原生 main 线程调用一次 `Engine::Run`，主任务用 `Engine::CoWaitRunning` 等 Running。`setup_runtime` 在 Start 前冻结线程/栈配置；普通 handler/Get/Post 在协程内直接消费，不接触 Run、不建 runner 线程、不等待 future/join。主任务结束后 runtime 恰好一次调用 `OnMainTaskDone` 回调，回调对共享 Engine 调用 `RequestClose`，`Run` 返回即本代网络资源收口的唯一凭据。经真实 loopback TCP 完成 GET/404/POST 位置参数往返与连接复用，服务端按 `StopAccepting → Close(Graceful) → WaitStopped` 收口。已准入的 handler 不保证在主任务退栈前退出，示例让 handler 闭包按值持有请求计数的共享状态，不引用主协程栈上的对象。

依赖新 HTTP 栈开关 `BBT_INFRA_HTTP_NET=ON`（无需 PROTOBUF），目标缺失时跳过；不依赖外部服务，loopback 自给自足。

## 怎么跑

```bash
cmake -S <repo> -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DBBT_INFRA_HTTP_NET=ON
cmake --build build --target http_net_example --parallel 2
timeout 20s ./build/examples/http_net/http_net_example
ctest --test-dir build -j1 -R '^example_http_net_consumer$' --output-on-failure
```

## 期望输出

```
new Client -> new Server accepted=1 requests=3 peer_requests=3 GET=200,404 POST=echo reuse=1
HTTP stopped tasks=0 sessions=0 inflight=0 listener_stopped=1
network stopped engine_run_returned=1 phase=Stopped connections=0
```

退出码 0 表示全部 Check 通过；Engine 的物理收口凭据是 host hook 内 `Engine::Run` 返回且 `Phase() == Stopped`，不是进程退出。
