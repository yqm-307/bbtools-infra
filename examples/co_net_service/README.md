# co_net_service — R2 协程网络用法示例

用 R2 公开 API 做真实 TCP 回环，只链接 `bbt::infra_net` 与 bbtools-coroutine：

- `Engine`：构造时传入 `std::shared_ptr<RawHandler>`，`Run(const EngineConfig&)` 占调用它的
  宿主 native 控制线程；每个 Reactor（server 为 1 main + N sub，client-only 无 main）自动绑定
  一个空闲已登记 Processer driver 槽，由该 P 的 `OnTick/Wait` 推进自有 Asio context，不新建
  网络 I/O 线程。
- `RawHandler`：原始短回调 `OnAccept/OnConnect/OnRead/OnWrite/OnClose` 在 owner
  非业务协程边界执行，必须短、非阻塞、不可挂起。
- `ICoConnection.hpp`/`CoTcpConnection.hpp`：协程消费面是 `ICoConnection` 成员接口——
  `Engine::CoAccept`/`Engine::CoDial` 返回 `ConnectionResult`（`connection` 为 `CoTcpConnection`），
  成员 `Read`/`Write`/`AsyncWrite`/`RequestClose`/`WaitClosed`/`Status` 只复用同一 raw 能力与既有
  `CoWaiter`，不建第二收包内核、不等业务回包、不自动重连/重发。

本示例流程：`Engine` server（`EnableAcceptHandoff`）→ 协程 `engine.CoAccept` 取连接 →
`conn.Read` 收到 `hello` → `conn.Write` 原样回显 → 读空后再取一次公开读取守卫必须
`WouldBlock`（守卫寿命/状态断言）→ `conn.RequestClose`；client 用 `NativeSocket` 直连服务端
实际绑定地址（请求 port 0，由内核分配真实端口）。

本示例消费的语义中，两条容易写错的：
- `WritePhase::Done` 只表示该次发送已到终态，**成功还须 `outcome.error` 为空**；
  `conn.Write` 的 `wait_error` 只描述本次等待，不代替发送终态。
- `ReadAccess` 守卫私有持有连接状态强引用：即使先释放 `Connection`，守卫内仍可安全
  `Size`/`Peek`/`Consume`/析构；`Peek` 视图只在守卫有效且内容未被 `Consume`/`ConsumeInto` 改变时有效。

构建与运行（network-only，独立构建目录）：

```sh
cmake -S <repo> -B build-r2 -G Ninja -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DBBT_INFRA_NETWORK_ONLY=ON \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 固定源码树> -DBUILD_TESTING=ON
cmake --build build-r2 --target Example_co_net_service --parallel 3
./build-r2/examples/co_net_service/Example_co_net_service
ctest --test-dir build-r2 -R '^example_co_net_service_roundtrip$' --output-on-failure
```

预期可观察输出（按当前源码）：`co_net_service echo=ok guard_wouldblock_after_drain=ok failures=0`；失败时进程非零退出。

runtime 无 Stop/优雅停机，示例业务收口后以 `std::_Exit` 结束进程，不展开栈；这不是
runtime 资源回收证据。

> 示例源码只 include 公共头、只链接 `bbt::infra_net`。公共 API 收口已由
> [PR #104](https://github.com/yqm-307/bbtools-infra/pull/104) 合入（新增
> `Types.hpp`/`ReadAccess.hpp`/`ICoConnection.hpp`/`CoTcpConnection.hpp`，删除
> `Connection::State()`、公开 `WaitConnect`/`WaitAccepted`/`AsyncConnectAwaitable`
> 与 `CoRead`/`CoWrite`/`CoAccept`/`CoDial` 自由函数）；符号与迁移清单见
> [决策 0010](../../docs/decisions/0010-gnet-network-foundation.md) 与
> [2026-10-29 接口终评](../../docs/decisions/2026-10-29-icoconn-interface-final-review.md)。
