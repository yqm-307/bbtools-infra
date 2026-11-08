# RPC V2 双进程 consumer 示例：只链接 `bbt::infra_rpc_net`。

本示例用两个真实 OS 进程验证 V2 会话面：server 进程绑定动态 loopback 端口
并把端口经 stdout 交给 client；client 进程拨号完成 Hello/HelloAck 握手后
发起一次 `Call`（Required）与一次 `Send`（RequireRemoteAcceptance），两侧
各一 Engine + Client owner + 同 owner 的 Server。收口只走统一入口：
`Close(Graceful)` 等已接纳责任归还后封口，`Close(Timeout)` 有界确认物理
收口（析构内 `Shutdown` 幂等兜底），并打印 `SERVER CLOSED ok` 与
`CONSUMER PASS` 供父进程断言。不是 mock 冒烟。

依赖 `BBT_INFRA_RPC_NET=ON`（protobuf 经既有前缀），目标缺失时跳过；不依赖
外部服务，loopback 自给自足。

## 怎么跑

```bash
cmake -S <repo> -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DBBT_PROTOBUF_PREFIX=<protobuf 前缀> \
  -DBBT_INFRA_RPC_NET=ON
cmake --build build --target rpc_v2_session_example --parallel 2
ctest --test-dir build -j1 -R '^rpc\.net\.v2_session_consumer$' --output-on-failure
```

`session_consumer.py` 是 CTest 前台受管父进程：管道交换端口、stdin EOF 触发
server 协作收口，并核对两端退出码与关键断言行。

## 期望输出

```
CONSUMER PASS sessions=1 links=1
SERVER CLOSED ok admitted=2 accepted=1 active=0
```

退出码 0 表示握手、Call、Send 与两侧资源收口均通过。
