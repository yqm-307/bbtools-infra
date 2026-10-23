# co_net_service — R2 协程网络用法示例

用 R2 公开 API 做真实 TCP 回环，只链接 `bbt::infra_net` 与 bbtools-coroutine：

- `Engine`：构造时传入 `std::shared_ptr<RawHandler>`，`Run(const EngineConfig&)` 占调用它的
  宿主 native 控制线程；每个 Reactor（server 为 1 main + N sub，client-only 无 main）自动绑定
  一个空闲已登记 Processer driver 槽，由该 P 的 `OnTick/Wait` 推进自有 Asio context，不新建
  网络 I/O 线程。
- `RawHandler`：原始短回调 `OnAccept/OnConnect/OnRead/OnWrite/OnClose` 在 owner
  非业务协程边界执行，必须短、非阻塞、不可挂起。
- `CoConnection.hpp`：`CoAccept/CoRead/CoWrite/CoDial` 只复用同一 raw 能力与既有
  `CoWaiter`，不建第二收包内核、不等业务回包、不自动重连/重发。

本示例流程：`Engine` server（`EnableAcceptHandoff`）→ 协程 `CoAccept` 取连接 →
`CoRead` 收到 `hello` → `CoWrite` 原样回显 → 读空后再取一次公开读取守卫必须
`WouldBlock`（守卫寿命/状态断言）→ `RequestClose`；client 用 `NativeSocket` 直连服务端
实际绑定地址（请求 port 0，由内核分配真实端口）。

本示例消费的语义中，两条容易写错的：
- `WritePhase::Done` 只表示该次发送已到终态，**成功还须 `outcome.error` 为空**。
- **仅冻结候选**（本分支未提交；main 不保证这条）：`ReadAccess` 守卫私有持有连接状态强引用，
  所以即使先释放 `Connection`，守卫内仍可安全 `Size`/`Peek`/`Consume`/析构。main 上守卫内部
  是裸 ring 指针，先释放 `Connection` 再访问守卫是悬垂，必须让 `Connection` 活得比守卫久。
  两种形态共同成立的一条：`Peek` 视图只在守卫有效且内容未被 `Consume`/`ConsumeInto` 改变时有效。

构建与运行（network-only，独立构建目录）：

```sh
cmake -S <repo> -B build-r2 -G Ninja -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DBBT_INFRA_NETWORK_ONLY=ON \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 固定源码树> -DBUILD_TESTING=ON
cmake --build build-r2 --target Example_co_net_service --parallel 3
./build-r2/examples/co_net_service/Example_co_net_service
ctest --test-dir build-r2 -R '^example_co_net_service_roundtrip$' --output-on-failure
```

预期可观察输出（按当前工作区源码）：`co_net_service echo=ok guard_wouldblock_after_drain=ok failures=0`；失败时进程非零退出。

`guard_wouldblock_after_drain` 这一项来自本轮未提交的示例改动（读空后再取一次公开守卫必须得到
`WouldBlock`）。main 上已合入的示例源码没有这一段，其输出只有 `echo=` 与 `failures=` 两个字段。
示例本身在两种形态下都只 include 公共头、只链接 `bbt::infra_net`。

runtime 无 Stop/优雅停机，示例业务收口后以 `std::_Exit` 结束进程，不展开栈；这不是
runtime 资源回收证据。

> 源码兼容性：示例源码在 main 与本分支两种形态下都能编译（只 include 公共头、只链接
> `bbt::infra_net`），但这不等于语义相同——上文标"仅冻结候选"的条目在 main 上不成立。
> 分支 `refactor/net-minimal-api-freeze` 上的公共 API 冻结候选（新增
> `Types.hpp`/`ReadAccess.hpp`，删除 `Connection::State()`、公开
> `WaitConnect`/`WaitAccepted`/`AsyncConnectAwaitable`）尚未提交、未经 PR CI；
> 冻结符号与迁移清单见 [决策 0010](../../docs/decisions/0010-gnet-network-foundation.md)。
