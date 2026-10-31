# 新 Client → 新 Server HTTP 消费示例：只链接 `bbt::infra_http_net`。

本示例演示应用装配两个 Engine（server 监听 + client 池），普通 handler/Get/Post 不接触 Run，经真实 loopback TCP 完成 GET/404/POST 位置参数往返与连接复用，收口走 `StopAccepting → Drain → Close → WaitStopped`。只消费公开头与公开 target，不触碰 net detail。

依赖新 HTTP 栈开关 `BBT_INFRA_HTTP_NET=ON`（无需 PROTOBUF），目标缺失时跳过；不依赖外部服务，loopback 自给自足。

## 怎么跑

```bash
cmake -S <repo> -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DBBT_INFRA_HTTP_NET=ON
cmake --build build --target http_net_example --parallel 2
./build/examples/http_net/http_net_example
ctest --test-dir build -j1 -R '^example_http_net_consumer$' --output-on-failure
```

## 期望输出

```
new Client -> new Server accepted=1 requests=3 peer_requests=3 GET=200,404 POST=echo reuse=1
HTTP stopped tasks=0 sessions=0 inflight=0 listener_stopped=1 engines_still_running=1
network stopped server_run=returned client_run=returned joined=2 connections=0 consumer_tasks=1/1
```
退出码 0 表示全部 Check 通过。
