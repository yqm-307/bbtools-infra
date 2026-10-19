# bbtools-infra

统一的现代 C++ 工具层与基础接入层：HTTP、CoTCP/CoUDP transport、Redis、Mongo 及分布式运行时基础能力。

本仓不承载 `bbt-framework` 的 App、Service 或业务配置编排。infra 提供可独立消费的第三方适配、执行域、错误映射、资源生命周期和基础动态配置机制；上层分布式服务框架负责配置治理、服务发现、灰度、业务 schema 与动态变更编排。

稳定架构边界见 [0006：infra 基础层与动态配置边界](docs/decisions/0006-infra-foundation-and-dynamic-config.md)。新增 Redis/Mongo 公共 API 使用 `bbt::infra::redis` 与 `bbt::infra::mongo`，不使用新的扁平模块命名空间；公共头不泄漏 hiredis、mongocxx、bsoncxx 或底层线程类型。

## 模块交付状态

| 模块 | CMake target | 状态 | 说明 |
|------|-------------|------|------|
| HTTP（client + server + NetworkRuntime） | `bbt::infra_http` | 已实现，单测覆盖中 | Beast/Asio 异步；配额、关闭排空、子对象反登记等可靠性边界仍在 Issue 中推进 |
| CoTCP / CoUDP transport | `bbt::infra_tcp`、`bbt::infra_udp` | 已实现 | 基础收发与受管工厂可用；Runtime 级 `max_inflight` 在途门禁已实现并合入（Issue #31/#32 已关闭）；跨平台/非 Linux、公网 DNS 与部分 lifecycle 直接证据仍未覆盖 |
| Redis client | `bbt::infra_redis` | 已实现最小切片 | CoTCP owner binding（`CoRedisCliImpl` + 窄 `RedisCotcpConn`）：CoTCP 承担 Dial/读写/等待/关闭，hiredis 只做同步 RESP2 编解码；单连接语义；`bbt::infra::redis` 公共 API 目录尚未拆分 |
| Mongo client | `bbt::infra_mongo` | 已实现 owner + 句柄形态 | 同步 driver + worker bridge；`mongo::CoMongoDb` 为资源 owner，`mongo::CoMongoColl` 为集合句柄；`include/bbt/infra/mongo/` 公共目录已建，剩余 API 按决策 0006 逐步迁移 |
| 基础动态配置 | `bbt::infra_config` | 最小切片已合入主线，独立消费已验证 | `bbt::infra::config`；版本化不可变快照 + 内存源 + 真实本地文件源 + watch（初始/更新/重复去重、失败恢复、关闭与晚到通知、正常 drain 与 owner 同步 `Close()` 的资源边界）。**未交付**：远程 provider 与真实重连、业务 schema 校验、客户端资源热切换。契约/命令/未覆盖矩阵见 [docs/config-watch-v1.md](docs/config-watch-v1.md) |
| RPC | `bbt::infra_rpc` | wire profile + runtime 候选已实现 | 版本化 Protobuf RpcEnvelope wire profile 与 HTTP-backed RpcClient/RpcServer runtime（`bbt::infra::rpc`，Issue #8/N2）：值类型校验、编解码、HTTP body 绑定、请求关联、deadline、错误和同步 Close；候选仍待 framework 接线、独立审查与合入 main |
| R2 TCP 底座（Processer driver，当前未提交候选） | `bbt::infra_net` | network-only 下聚焦回归本地通过；未独立审查/未过 CI/未发布 | 自有 main/sub Asio context 由已登记 Processer driver 的 `OnTick/Wait` 推进，不建网络 I/O 线程；`Engine::Run` 占宿主 native 线程、自动绑定空闲槽并在失败时回滚、`RequestClose` 封口、同代同步解绑后收口；原始短回调 `OnAccept/OnConnect/OnRead/OnWrite/OnClose`（owner 非业务协程边界，不可挂起）；并发接收 ring 为单一真源（`TryRead`、RAII `Peek+Consume`、Busy/WouldBlock、持久 EOF/错误、关闭后尾部可读）；`AsyncWrite` 全量/零接纳、有界字节与操作数、逐操作真实前缀与唯一终态、`WriteOperation` 句柄终态可查；`CoRead/CoWrite/CoAccept/CoDial` 仅复用同一 raw 能力。固定消费 coroutine `ae2cc1a0ad02de0027c3cab5d36402139f7aabd3`。旧 `Client/EventHandler/CoTcpConnection/ICoConnection/Action` 已删除，不保留兼容壳。 |
| MCP | — | 未开始 | 归对应 Issue 推进，当前无实现 |

> 状态口径：表中 R2 TCP 行指**未合入「原已合并基线」**的**本地未提交候选**——已在 `BBT_INFRA_NETWORK_ONLY=ON` 下局部验证，但**未独立审查、未过 CI、未跑完整 P6（仅有界短负载）**，不得据此升格为已交付。本 README 中「原已合并基线」特指本地固定提交 `3136d1de5031a2eec55475e770d5ebbadc3c32da`（本机 `main`，2026-10-15 fast-forward 合入），**不代表远端实时 `main`**；其余模块各行维持原有说明，本轮未改动，不为其背书或新增含义。

已实现能力的单测与示例均可独立构建运行，见下文「独立消费」与 [examples/README.md](examples/README.md)。

## I/O 执行边界

新 `bbt::infra_net` 的 **R2 TCP 核心契约已获用户确认；当前未提交候选已实现本地聚焦验证，尚未独立审查/CI/发布**：infra 拥有自有 main/sub Asio context 与网络资源，每个 Reactor 自动绑定一个空闲已登记 Processer driver 槽；不新增网络 I/O 线程。原始 `OnAccept/OnConnect/OnRead/OnWrite/OnClose` 回调在 owner 非业务协程边界短、非阻塞、不可挂起地运行；协程同步 API 只用既有 CoWaiter/Chan 做薄封装。接收 ring 是并发安全的数据真源，OnRead 仅通知；提交接纳、本地发送完成和业务回包严格分开，普通 EOF/错误/等待超时不自动关连接或重发，协议和业务策略归上层。固定依据是 coroutine PR #396 合并提交 `1e9f0e8c25de85dde003def65bb58b90bdac0e0b`，不表示构建 pin 已升级或两仓已联调。

当前目标见 [网络执行域设计](docs/architecture/gnet-network-execution.md) 与 [0010](docs/decisions/0010-gnet-network-foundation.md)，后续接续见 [迁移计划](docs/plans/gnet-network-migration.md)。上表 R2 行及下方新增的 R2 使用入口描述当前未提交候选；更下方的旧消费命令/旧测试名保留为历史记录，不再代表当前公共 API（旧自建线程/事件快照/受管 OnTraffic 已删除）。不重复驱动 `io::GetExecutor()` 的共享 context，不改变 runtime Timer/Hook/CoPoller，也不迁移其它模块。

[CoTCP/CoUDP 与第三方 Binding 规格](docs/decisions/0005-co-io-adapter-contract-v1.md) 的“proc 执行收发与协议，sche 只检测就绪并唤醒”继续说明旧路径，不覆盖上述新 net 目标。此次不改造或声称已迁移以下历史模块。

当前 HTTP 为 Asio/Beast 异步推进，Redis 为 CoTCP owner binding（CoTCP 承担字节 I/O/等待/关闭，hiredis 只做同步 RESP2 编解码），Mongo 为同步 driver + worker bridge。CoTCP/CoUDP 已交付基础实现，Runtime 级 `max_inflight` 在途门禁已实现并合入（Issue #32 已关闭，见 [0005 §4](docs/decisions/0005-co-io-adapter-contract-v1.md)）；跨平台/非 Linux、公网 DNS 与部分 lifecycle 的直接验收证据仍未覆盖。

## 资源关闭与请求完成契约（进程寿命运行时）

coroutine 运行时按**进程寿命**存在、不参与业务资源收口（不再有 `Scheduler::Stop()`/restart 与运行时代际）。新 net 的 R2 目标见 [关闭契约](docs/architecture/gnet-network-execution.md#6-网络关闭不是-runtime-停机)：`RequestClose` 仅封口并请求控制域收口，`Run` 返回才确认网络资源、callback 与同代解绑完成；不等待业务协程或外部句柄归零，关闭后已有接收数据仍可读，不承诺硬 deadline。上述接口是目标、未实测，取代旧 Close(opt) 作为新 net 约束，不改下面的**旧模块** [0002 co-network/v1](docs/decisions/0002-co-network-contract-v1.md) 契约及状态：

- `ICoCloseable` 只有 `void Close() noexcept` + `bool IsClosed() const noexcept`。`Close()` 由资源 owner/framework manager 主动调用、同步完成物理释放并返回：**返回即该对象拥有的物理资源已释放、后端不会再访问它们**；未发送数据直接丢弃、不做 flush、不重开；**没有「等待关闭完成」的入口**。
- 已删除且不保留兼容壳：`RequestClose`、`WaitClosed`、`ReleaseClosed`、`CloseStatus` 等待结果枚举、`CompletionSignal`、`CancellationToken`/`CancellationSource`（含 `CallOptions.cancel` 字段）、`RuntimeGeneration`/`CurrentRuntimeGeneration`、`Scheduler::Stop()`/restart。
- **请求完成**：业务一次调用内部以 `CoWaiter::WaitWithCallback`（登记等待 → 一次投递回调 → 挂起）+ adapter 路径 `Notify`（可带载荷 `CoEventValue`）唤醒；结果放各模块 operation state。早到响应走 `CoPollEvent::PENDING`；每个请求只在完成/超时/取消/关闭中交付**一次**终态；pending（未发送）请求遇关闭直接丢弃、不发送；已发送请求关闭后业务立即返回终态，迟到响应继续被消费以保持连接对齐但不再交付；不在 infra 内重试；事件回调不得阻塞。
- **分层**：Scheduler 只驱动 FD 就绪与通用唤醒；Connection handler 处理本连接的非阻塞读写、buffer 与关闭；协议（Redis/HTTP/Mongo）各自实现协议与错误映射，不共享万能请求接口。
- 「运行时是否在跑」用初始化状态 `Scheduler::IsInitialized()`，不再有代际语义。

> 状态：新的关闭/进程寿命契约**已合入 main**（close/stop 迁移 `02567ed`）：`ICoCloseable.hpp`、`NetworkTypes.hpp`、`src/detail/IoSupport.hpp` 与模块头/实现、tests、examples 的公开调用链均已迁到该契约（`RequestClose`/`WaitClosed`/`ReleaseClosed`/`CloseStatus` 等旧公开入口已删除）。跨线程同步 `Close()` 的实现口径见 [0005 §6.4](docs/decisions/0005-co-io-adapter-contract-v1.md)；本 README 只陈述契约与迁移状态，不据此宣称同步 `Close()` 实现已被完整验收，未覆盖项以各决策文档与 Issue 为准。

## 依赖方向与公共边界

- `bbt-framework → bbtools-infra → bbtools-coroutine` 为单向依赖；本仓不依赖 framework。
- `bbtools-core` 已冻结不再维护：coroutine 已脱离 `libbbt_core` 自闭环；本仓在 coroutine 拆分后同样不再引入 `bbt_core` 依赖（CMake 中 `BBT_CORE_SOURCE_DIR` 仅兼容 coroutine 未完全拆分前的过渡形态）。
- 公共头位于 `include/bbt/infra/`；各模块为独立 CMake target，不强制同时构建或链接。
- Redis/Mongo 旧公共头 `CoRedisCli.hpp`/`CoMongoCli.hpp` 仍保留兼容入口；新公共 API 目录（`include/bbt/infra/redis/`、`include/bbt/infra/mongo/`）按决策 0006 拆分后逐步迁移。

## 独立消费

### R2 TCP 使用入口（当前未提交候选）

公共头：`<bbt/infra/net/Engine.hpp>`、`RawHandler.hpp`、`Connection.hpp`、`CoConnection.hpp`、`Error.hpp`；只链接 `bbt::infra_net`。

前提与关闭：
- `Engine::Run(EngineConfig, ...)` 阻塞占用**调用它的宿主 native 控制线程**（不能是受管业务协程或本 Engine 的 owner 线程）。
- `EngineConfig.runtime_progress` 必填 `RuntimeProgress::Independent`，声明 runtime 内部 EventLoop 由独立执行流持续推进；未声明在绑定/建 socket 前以 `operation_not_permitted` 拒绝，runtime 未初始化以 `operation_not_supported` 拒绝。
- `Connection::RequestClose()` 只是请求封口：立即拒绝新写并唤 owner；已 recv 但尚未
  进入 ring 的尾部由 owner 先转入 state 真源，之后才发布读侧最终 EOF。请求后、owner
  完成前读取可得到 `WouldBlock`，不得提前 EOF。首次请求的 reason（包括默认 `{}`）
  固定，之后写失败/重复请求不会重开接纳或覆盖该原因。`Run` 返回才表示 Engine 收口完成。不要在 `Run` 未返回时并发析构 Engine。
  - 正常收口由原 owner 完成已有连接的唯一 `OnClose`、待结算写的 `OnWrite` 与 pending connect 的 `OnConnect`，随后同代解绑；**不设隐藏关闭 deadline**，单次可恢复的命令提交失败会补交而非提前以 `Closing` 返回。只有 driver 真实 Failed 才走错误出口冻结并解绑。
  - 普通用户回调（`OnRead`/`OnAccept`/`OnConnect`）的开始授权点，是**最后一次紧邻 `SafeHandler` 派发前**的 `shared service_sealed` 原子检查（`NotifyReadOwner`、`AdoptFdOnOwner` 的交付前）：检查通过即为已授权在途，短回调必须完成；封口先于该检查则不启动成功回调（`OnAccept` 不调用，`OnConnect` 交付 canceled）。构造/登记阶段的入口早查只是提前短路，不构成授权。授权点与封口共同线性化，检查之后不再有可撤销的无限推迟。
- 原始回调必须短、非阻塞、不可挂起；接收 ring 是唯一真源，`OnRead` 只是可合并通知。
- `CoDial`/`WaitConnect` 只用于未配置 `RawHandler` 的 Engine；配置了 raw handler 时
  `AsyncConnectAwaitable`/`WaitConnect` 明确返回 `operation_not_supported`，raw 的
  pending/完成由 `OnConnect` 唯一交付，Co 等待不得取消或二次领取。每个 connect op
  只允许一个 `WaitConnect` 接收者，第二个返回 `device_or_resource_busy`。
  等待超时/取消终结该接收者并由 owner 回收未交付连接，不能用同 id 再次等待；
  waiter 建立失败返回 `not_enough_memory`、回滚 claim，同 id 可重试。
- `CoDial` 的 client-only Engine 若无 raw 接收者，请用空 `shared_ptr<RawHandler>` 构造：
  等待超时/取消后 owner 才可回收未交付 socket；有 raw handler 时视为已交付，不会因另一个
  Co 等待取消而误关。
- `AsyncWrite` 成功只表示库已接纳；`WriteOperation::Query()` 区分仍在途/完成与真实前缀。协程等待超时/取消只结束等待，已接纳操作仍由 owner 推进，可用同一 `WriteOperation` 查最终结果；不自动关闭或重发。
- `RawHandler::OnClose` 在 owner 上按连接最多一次；peer EOF 只结束读方向，不自动关闭写侧。

最小 raw 入口（伪代码，实际见 `examples/co_net_service` 与 `tests/net/r2_tcp.cc`）：

```cpp
net::Engine engine(handler);                       // handler : RawHandler
net::EngineConfig config;
config.listen_addresses = {address};              // port 0 由内核分配
config.runtime_progress = net::RuntimeProgress::Independent;
std::thread runner([&] { error = engine.Run(config); });
// handler->OnAccept / OnRead / OnWrite / OnClose 在 sub owner 线程执行
engine.RequestClose();
runner.join();
```

Co 薄封装（同一 raw 内核）：

```cpp
net::Connection conn = /* from CoAccept/CoDial */;
std::string data;
if (net::CoRead(conn, data, options) == net::NetErrc::Eof) { /* 尾部已读完 */ }
net::WriteOutcome outcome;
net::WriteOperation operation;
if (net::CoWrite(conn, data, options, outcome, operation)) {
    // timed_out: outcome.phase==InFlight 时已接纳仍在途，可 operation.Query()
    //            outcome.phase==Rejected 时未提交
}
```

已验证范围（本地、未提交）：真实 port0 raw/Co 回环、sub owner 身份、多 listener、关闭后尾部/残留 Conn、有界发送/部分写/错误结算、容量超时未提交、完成超时仍在途、no-deadline 等待关闭结算、handoff 容量恢复、公平性、运行前提与 driver Failed 收口。未覆盖：长时/多连接性能、部分 Options 组合接线、connect 超时回收的确定性动态用例等，详见实现报告。

### 新 net 首包候选（与下述旧模块消费分开）

公共头使用 `<bbt/infra/net/...>`，include 根是仓库根；只链接 `bbt::infra_net`。先在**独立目录**检出 coroutine `8ae6a88997c5acaa4fe03430c35e1820ed5b29b0`（[PR #392](https://github.com/yqm-307/bbtools-coroutine/pull/392) 的 `mergeCommit`；其父 `6dfda8158d76f5030cde50a9fbc3c97a8335e932` 为 [PR #388](https://github.com/yqm-307/bbtools-coroutine/pull/388) 的 `mergeCommit`，消费标识是落地 commit + 其 tree，不用 PR head），并显式核对源码树干净；`BBT_COROUTINE_SOURCE_DIR` 仍是源码路径输入，CMake 的来源路径检查**不代替** SHA 与实际链接核验。

```bash
set -e
test "$(git -C <固定 coroutine 源码树> rev-parse HEAD)" = 8ae6a88997c5acaa4fe03430c35e1820ed5b29b0
co_status=$(git -C <固定 coroutine 源码树> status --porcelain --untracked-files=all)
test -z "$co_status"
cmake -S . -B <独立 build> -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DBBT_INFRA_NETWORK_ONLY=ON \
  -DBBT_COROUTINE_SOURCE_DIR=<固定 coroutine 源码树> -DBUILD_TESTING=ON
JOBS=$(( $(nproc) / 4 )); [ "$JOBS" -ge 2 ] || JOBS=2
cmake --build <独立 build> --target bbt_infra_net bbt_infra_net_header_selfcheck \
  Test_net_foundation Test_net_primitives Test_net_allocation net_foundation_consumer --parallel "$JOBS"
ctest --test-dir <独立 build> -R '^net\.' -j1 --output-on-failure
```

`network-only` 在旧目标、数据库/protobuf 包发现之前分流，残留 prefix 不启用被排除模块；不修改旧功能源码。首包消费工程见 [examples/net_foundation_consumer](examples/net_foundation_consumer/README.md)。`net.context_no_create` 保留为正常的**必过门禁**：切换到上述精确提交后，公开查询及 infra gate 均不再隐式创建 Processer（该修复经 [PR #388](https://github.com/yqm-307/bbtools-coroutine/pull/388) 合入，`mergeCommit` `6dfda8158d76f5030cde50a9fbc3c97a8335e932`，即上述固定 commit 的父提交）；未改断言或设置 WILL_FAIL/skip。已核编译、ELF/DSO 来源并运行独立 UDP 消费工程，不以临时换库代替源码消费。**本轮（160）重绑定结果**：上述配方的前置条件在固定 commit `8ae6a889…` 上成立（该 commit 的干净检出、无未提交差异、无未跟踪闭包缺口），同一组配置项（生成器用 Unix Makefiles，未逐字复跑配方）已成功构建并重验，从它干净重建的 `libbbt_coroutine.so` 已核 `ldd`/`RUNPATH` 实解析；本仓 Engine/Conn 等待与 Close 接线在**「原已合并基线」上**仍待实现（本轮 gnet Engine/Client 本地候选见上表，尚未合入该基线、未独立审查、未过 CI、未跑完整 P6）。当前 infra CI 尚未切换到 network-only，本包局部测试不证明旧模块或 infra CI 已回归。

> **验证绑定（本轮 160 已重绑定到固定 commit）**：上述配方要求上游源码树干净（`test -z "$co_status"`），它复现的是「`8ae6a889…` 干净树」产物——**与本轮集中验证所绑定的运行库同源**（本轮生成器为 Unix Makefiles，非配方示例的 Ninja；未逐字复跑配方，故不称「同一二进制」）。本轮把消费出处从「`6f650eee` + 一份未提交本地差异」替换为已合并固定 commit `8ae6a88997c5acaa4fe03430c35e1820ed5b29b0`（[PR #392](https://github.com/yqm-307/bbtools-coroutine/pull/392) 的 `mergeCommit`，tree `89c5012ca22d06a226727943a89dd7f08548b8cc`；其父 `6dfda815…` 为 [PR #388](https://github.com/yqm-307/bbtools-coroutine/pull/388) 的 `mergeCommit`）。**消费标识是落地 merge commit + 其 tree，不是 PR head `a20a6d7b…`，也不是可变 `main`。** 从该固定 commit 干净重建的 `libbbt_coroutine.so` sha256 `0f8e03796304319ab110f3d1d07403c71fe8901490639b7c09fc28ff2cdee5b0`（build-id `f28d22a77311202fe7d796844c612c90fe1ebcdc`），与旧 dirty 候选 DSO `2a3b084b…e63ebe` **不同**（两条基线无祖先关系）。旧说明的「补丁闭包不完整 / 按现有配方不可复原 / 发布可复现门禁 HOLD」前提**已随该 commit 落地消失**：`unit_test/Test_runtime_oom.cc` 现已被跟踪（blob sha256 `ab92bebde62d8bed96cbf4fdd57a44f2f522e001ad32101af51b47ab6e59d6e2`）并被 `unit_test/CMakeLists.txt` 引用，不再有未跟踪闭包缺口。本轮固定版本影响面实测 **107/107 通过、0 失败、0 skip**（= 152 的 98 项影响面 + 9 项 socket primitives/`net.context_no_create` 门禁；含 Shutdown/Tick/Client 直接集合、150 家族守卫、151 多 loop/LB/预算、负编译 `net.udp_high_level_compile_reject`、公开示例 4 模式真实重链）。**证据边界**：旧候选的 98 项与有界 P6 结果绑定旧 dirty DSO，**不能**作为固定 commit 的 PASS 依据，本轮不引用其绿灯；本轮**未跑** Release/ASAN/完整 P6/infra CI，本仓该候选自身仍未独立审查、未提交、未合入 main，故**本仓发布层面**的可复现与验收仍以合入后的 PR/CI 为准。标识与限制见 [bbtools-coroutine 候选绑定出处](docs/dependencies/bbtools-coroutine-candidate.provenance.md)。

### 旧模块源码消费

本仓当前**不提供安装包或导出 CMake 包**。消费方式为**源码树内 add_subdirectory** 或**直接在本仓构建后链接静态库**。

### 源码接入（推荐）

```cmake
# 消费者 CMakeLists.txt 中
set(BBT_COROUTINE_SOURCE_DIR "<bbtools-coroutine 源码树>")
add_subdirectory(<bbtools-infra 源码树> bbtools-infra)

target_link_libraries(your_target PRIVATE bbt::infra_http)   # 按需选择
```

`bbt_add_source_dependency` 会遮蔽上游单测/示例开关、隔离 `CMAKE_POLICY_VERSION_MINIMUM`，并把上游头路径补成目标级接口。

### 本仓构建

```bash
cmake -S . -B build \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  [-DBBT_CORE_SOURCE_DIR=<bbtools-core 源码树>]          # coroutine 未完全拆分时需要
cmake --build build --parallel $JOBS
ctest --test-dir build --output-on-failure
```

### 可选模块依赖

| 模块 | 额外 CMake 变量 | 说明 |
|------|----------------|------|
| Redis | `BBT_HIREDIS_PREFIX` | hiredis 私有安装前缀；未设置时跳过 `bbt_infra_redis` |
| Mongo | `BBT_MONGOCXX_PREFIX` + `BBT_MONGOC_PREFIX` | mongo-cxx-driver 与 mongo-c-driver 私有前缀；须成对设置 |

### 示例

最小真实消费示例见 [examples/README.md](examples/README.md)：
- `Example_http_loopback`：无外部依赖的 HTTP loopback 完整生命周期
- `Example_config_consume`：配置读取 → 上层校验 → 接受/拒绝（含格式错误不发布伪成功）
- `Example_redis_mongo_shape`：Redis/Mongo 调用形态与错误路径演示（不连真实服务）

另有独立消费工程 `examples/config_consumer`（只链接 `bbt::infra_config`，不被本仓构建引用），
用于从仓外视角复核模块 target 可独立消费；命令见该目录 README 与 [docs/config-watch-v1.md](docs/config-watch-v1.md)。

## CI 覆盖边界

- PR/push CI 中**本仓定义的 job** 跑在 hosted `ubuntu-24.04`（`arc-s4-infra` 仅存历史含义）；跨仓 reusable callee（分类/计划、编译 & ctest 汇聚）按其固定 `ubuntu-latest` 运行。对本构建中已注册的全部测试执行 `ctest -j1`。
- CI 在 `prepare-deps` 阶段用 `scripts/prepare_resource_deps.sh` 以固定 tag+SHA 源码构建 hiredis/mongoc/mongocxx 私有前缀，并在 `release` job 显式断言 `redis.unit`/`mongo.unit` 已注册后纳入 `ctest -j1`；故 CI 覆盖 HTTP/transport/contract 面**以及 Redis/Mongo unit**。真实 run 证据见 [docs/ci/infra-ci-v1.md](docs/ci/infra-ci-v1.md)。
- Redis/Mongo **live 容器验收**纳入 Release job 后段：复用已构建产物，在独占临时容器中运行正常、停止态失败和重启恢复（A/B/C），另执行 `redis.cotcp_binding.live` 大值回归。缺 Docker/Compose/二进制、启动或测试失败均非零退出；不以 Skipped 代替通过。前段无服务的全量 CTest 仍可报告 live Skipped，不能将其当作后段 live 的实跑证据。`docs-only` 变更允许跳过整个 Release job。命令、资源边界和遗留条款对账见 [Redis/Mongo 验收说明](docs/redis-mongo-acceptance.md)。
- CI 固定依赖到上游 `main`/`master` HEAD，不代表对任意历史 SHA 的回溯兼容。

## 构建约束

本地验证边界与并发限制按 AGENTS.md 执行：必走冒烟 + 本次开发功能单测 + 直接耦合功能单测；新 configure 加 `-DCMAKE_CXX_COMPILER_LAUNCHER=ccache`；本地并发取 `JOBS=$(( $(nproc) / 4 ))`；禁止 bare `--parallel` 或 `ninja -j$(nproc)`。
