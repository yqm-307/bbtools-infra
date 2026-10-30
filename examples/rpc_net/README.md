# RPC core 最小真实消费示例：只链接 `bbt::infra_rpc_net`。

本示例演示应用装配双 Engine（server 监听 + client 池），`rpc::Client ↔ rpc::Server` 经真实 loopback TCP 完成 (string,int64) 位置参数回声与一次业务错误往返；收口走 `StopAccepting → Drain → Close → WaitStopped`。只消费公开调用方式/错误/关闭责任，不是 mock 冒烟。

依赖 `BBT_INFRA_RPC_NET=ON`（需 HTTP_NET + PROTOBUF），目标缺失时跳过；不依赖外部服务，loopback 自给自足。

## 怎么跑

```bash
cmake -S <repo> -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DBBT_INFRA_RPC_NET=ON
cmake --build build --target rpc_net_example --parallel 2
./build/examples/rpc_net/rpc_net_example
ctest --test-dir build -j1 -R '^example_rpc_net_consumer$' --output-on-failure
```

## 期望输出

```
rpc example echo=(string,int64) errors=NotFound stopped=2 engines connections=0
```
退出码 0 表示回声与业务错误往返均通过，收口干净。
