# co_net_service — R2 协程网络用法示例

用 R2 公开 API 做真实 TCP 回环，只链接 `bbt::infra_net` 与 bbtools-coroutine：

- `Engine`：`Run(config, handler)` 占调用它的宿主 native 控制线程；每个 Reactor
  （server 为 1 main + N sub，client-only 无 main）自动绑定一个空闲已登记
  Processer driver 槽，由该 P 的 `OnTick/Wait` 推进自有 Asio context，不新建网络
  I/O 线程。
- `RawHandler`：原始短回调 `OnAccept/OnConnect/OnRead/OnWrite/OnClose` 在 owner
  非业务协程边界执行，必须短、非阻塞、不可挂起。
- `CoConnection.hpp`：`CoAccept/CoRead/CoWrite/CoDial` 只复用同一 raw 能力与既有
  `CoWaiter`，不建第二收包内核、不等业务回包、不自动重连/重发。

本示例流程：`Engine` server（`EnableAcceptHandoff`）→ 协程 `CoAccept` 取连接 →
`CoRead` 收到 `hello` → `CoWrite` 原样回显 → `RequestClose`；client 用 `NativeSocket`
直连服务端实际绑定地址（请求 port 0，由内核分配真实端口）。

构建与运行（network-only，独立构建目录）：

```sh
cmake -S <repo> -B build-r2 -G Ninja -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DBBT_INFRA_NETWORK_ONLY=ON \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 固定源码树> -DBUILD_TESTING=ON
cmake --build build-r2 --target Example_co_net_service --parallel 3
./build-r2/examples/co_net_service/Example_co_net_service
ctest --test-dir build-r2 -R '^example_co_net_service_roundtrip$' --output-on-failure
```

runtime 无 Stop/优雅停机，示例业务收口后以 `std::_Exit` 结束进程，不展开栈；这不是
runtime 资源回收证据。
