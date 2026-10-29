# bbtools-infra

统一的现代 C++ 工具层与基础接入层：HTTP、R2 TCP 网络底座、RPC、CoTCP/CoUDP transport、Redis、Mongo 及分布式运行时基础能力。

本仓不承载 `bbt-framework` 的 App、Service 或业务配置编排。infra 提供可独立消费的第三方适配、执行域、错误映射、资源生命周期和基础动态配置机制；上层分布式服务框架负责配置治理、服务发现、灰度、业务 schema 与动态变更编排。

稳定架构边界见 [0006：infra 基础层与动态配置边界](docs/decisions/0006-infra-foundation-and-dynamic-config.md)。新增 Redis/Mongo 公共 API 使用 `bbt::infra::redis` 与 `bbt::infra::mongo`，不使用新的扁平模块命名空间；公共头不泄漏 hiredis、mongocxx、bsoncxx 或底层线程类型。

## 模块交付状态

| 模块 | CMake target | 状态 | 说明 |
|------|-------------|------|------|
| HTTP（client + server + NetworkRuntime） | `bbt::infra_http` | 已实现，单测覆盖中 | Beast/Asio 异步；配额、关闭排空、子对象反登记等可靠性边界仍在 Issue 中推进 |
| CoTCP / CoUDP transport | `bbt::infra_tcp`、`bbt::infra_udp` | 已实现 | 基础收发与受管工厂可用；Runtime 级 `max_inflight` 在途门禁已实现并合入（Issue #31/#32 已关闭）；跨平台/非 Linux、公网 DNS 与部分 lifecycle 直接证据仍未覆盖 |
| Redis client | `bbt::infra_redis` | 已实现最小切片 | 旧入口仍使用 CoTCP owner binding（`CoRedisCliImpl` + 窄 `RedisCotcpConn`）；原同步关闭与排队配置契约保留 |
| Redis net 窄入口 | `bbt::infra_redis_net` | 隔离候选，待独立审查 | `bbt::infra::redis::CoRedisCli`（共享 Engine、单连接/单活动操作、非阻塞 Close）与 `CoRedisPool`（固定大小、命令内借还、有界 FIFO 排队）；等待入口以 `bbt_co_async`/`bbt_co_await` 标记与消费，公共头与实现同目录在 `bbt/infra/redis/`；真实 Redis、池、分帧与宿主寿命验收入口见 [docs/redis-net.md](docs/redis-net.md)。framework 消费者未切换 |
| HTTP net Client / Server | `bbt::infra_http_net` | 候选已实现，独立审查通过，待远端 CI 验证 | HTTPX 形态原生有栈协程 Client + 有界连接池；Server 提供 owning request/handler、顺序 keepalive session、StopAccepting/Drain/Close/WaitStopped 与物理回收额度。Beast 只编解码，网络只经 net 公开 Co 接口，无库内新线程。真实新 Client→新 Server 双端示例完成两 GET/一 POST 同连接复用；RPC/HTTP 定向 workflow 已接线，远端 CI 尚未观察。https/DNS/json/form/流式/重定向/HTTP2 未实现，framework 消费者未切换。契约与构建见 [docs/http-net.md](docs/http-net.md) |
| Mongo client | `bbt::infra_mongo` | 已实现 owner + 句柄形态；BSON/async 入口为待审查增量 | 同步 driver + worker bridge、owner/句柄归属与关闭语义不变；`mongo::CoMongoDb` 为资源 owner，`mongo::CoMongoColl` 为集合句柄。公共值类型迁到 `include/bbt/infra/mongo/Bson.hpp`：`MongoDocument::FromJson/ToJson` + 4 个顶层标量 getter（字段缺失 `NotFound`、类型不符 `TypeMismatch`、畸形 BSON/JSON `InvalidArgument`，数值保真语义见 0007 增量）；旧头 `CoMongoCli.hpp` 保留 `using` 别名与 `MongoDocument{{...}}` 聚合直写。`CoMongoColl` 的 4 个 CRUD 以 `bbt_co_async` 标记、调用方经 `bbt_co_await` 消费（旧 `CoMongoCli` 的 4 个方法保持裸 `result<T>`，framework 与仓内既有 100+ 调用点不动）；async/`ToJson` 契约见 [0007 增量](docs/decisions/0007-mongo-owner-handles.md)。真实消费示例 `mongo_client_example`（只链接 `bbt::infra_mongo`、只含公开头）见 [examples/README.md](examples/README.md) |
| 基础动态配置 | `bbt::infra_config` | 最小切片已合入主线，独立消费已验证 | `bbt::infra::config`；版本化不可变快照 + 内存源 + 真实本地文件源 + watch（初始/更新/重复去重、失败恢复、关闭与晚到通知、正常 drain 与 owner 同步 `Close()` 的资源边界）。**未交付**：远程 provider 与真实重连、业务 schema 校验、客户端资源热切换。契约/命令/未覆盖矩阵见 [docs/config-watch-v1.md](docs/config-watch-v1.md) |
| RPC core（新 HTTP 栈） | `bbt::infra_rpc_net` | 候选已实现，独立审查通过，待远端 CI 验证 | 消费新 `bbt::infra::http::Client/Server` 的 RPC core（`bbt/infra/rpc/`）：版本化 Protobuf RpcEnvelope wire profile 与 RpcPayload 逐位复用，HTTP body 绑定改绑新 `http::IncomingRequest/Response`；请求关联、deadline local-min、错误透传（含 `OutcomeUnknown`+request_phase）、畸形报文错误信封、`StopAccepting/Drain/Close/WaitStopped`+`IsClosed`。真实双端（双 Engine loopback）21 项 `rpc.net.*` 契约测试 + 消费示例。旧 `RpcClient/RpcServer` 兼容壳、`NetworkRuntime::CreateRpcClient/ListenRpc` 与 `rpc.runtime*` 测试已删除；全量闭包不再接入 protobuf（锁定接入收敛到 `cmake/RpcNet.cmake`，`BBT_INFRA_RPC_NET` 显式 fail-closed）。RPC/HTTP 定向 workflow 已接线，远端 CI 尚未观察；https/DNS/双进程/framework 消费切换未覆盖。契约、构建与验证命令见 [docs/rpc-net.md](docs/rpc-net.md)；旧 runtime v1 契约的历史记录见 [0009](docs/decisions/0009-rpc-runtime-v1.md) |
| R2 TCP 网络底座（Processer driver） | `bbt::infra_net` | 已合入主线 | 自有 main/sub Asio context 由已登记 Processer driver 的 `OnTick/Wait` 推进，不建网络 I/O 线程；`Engine::Run` 占宿主 native 线程、自动绑定空闲槽并在失败时回滚、`RequestClose` 封口、同代同步解绑后收口；原始短回调 `OnAccept/OnConnect/OnRead/OnWrite/OnClose`（owner 非业务协程边界，不可挂起）；并发接收 ring 为单一真源（`TryRead`、RAII `Peek+Consume`、Busy/WouldBlock、持久 EOF/错误、关闭后尾部可读）；`AsyncWrite` 全量/零接纳、有界字节与操作数、逐操作真实前缀与唯一终态、`WriteOperation` 句柄终态可查。协程消费面已收口为 `ICoConnection` 成员接口（[PR #104](https://github.com/yqm-307/bbtools-infra/pull/104)，接口终评见 [2026-10-29 终评](docs/decisions/2026-10-29-icoconn-interface-final-review.md)）：`Engine::CoAccept`/`Engine::CoDial` 返回 `ConnectionResult{error, connection, reclaim}`，`connection` 为 `CoTcpConnection`，成员 `Read`（owning `Buffer`）/`Write`（`WriteReport`）/`AsyncWrite`/`RequestClose`/`WaitClosed`/`Status` 复用同一 raw 能力；旧 `CoRead/CoWrite/CoAccept/CoDial` 自由函数已删除。更早的 `Client/EventHandler/Action` 与旧 `CoTcpConnection/ICoConnection` 也不保留兼容壳；消费者迁移（HTTP/Redis/Mongo/config 仍走旧路径）不在本次 PR 范围。底座初版见 [PR #84](https://github.com/yqm-307/bbtools-infra/pull/84)（合入提交 `99d0821a`）；公共 API 面与删除迁移表见 [0010](docs/decisions/0010-gnet-network-foundation.md) |
| MCP server | — | 未交付（草稿候选） | C++17 server-only 的协议核心、tools/resources/prompts 与 stdio/Streamable HTTP 候选保存在 [草稿 PR #80](https://github.com/yqm-307/bbtools-infra/pull/80)，未合入 main，当前主线无可消费 target。目标、阻断与 R1–R11 / S1–S24 验收由 [Issue #85](https://github.com/yqm-307/bbtools-infra/issues/85) 维护；完整 client SDK 与 OAuth 不在首版范围。实时冲突和检查状态以该 PR 为准 |

已实现能力的单测与示例均可独立构建运行，见下文「独立消费」与 [examples/README.md](examples/README.md)。

## I/O 执行边界

新 `bbt::infra_net` 的 **R2 TCP 契约已合入主线**（PR #84，合入提交 `99d0821a`）：infra 拥有自有 main/sub Asio context 与网络资源，每个 Reactor 自动绑定一个空闲已登记 Processer driver 槽；不新增网络 I/O 线程。原始 `OnAccept/OnConnect/OnRead/OnWrite/OnClose` 回调在 owner 非业务协程边界短、非阻塞、不可挂起地运行；协程同步 API 只用既有 CoWaiter/Chan 做薄封装。接收 ring 是并发安全的数据真源，OnRead 仅通知；提交接纳、本地发送完成和业务回包严格分开，普通 EOF/错误/等待超时不自动关连接或重发，协议和业务策略归上层。

设计与契约真源见 [网络执行域设计](docs/architecture/gnet-network-execution.md) 与 [0010](docs/decisions/0010-gnet-network-foundation.md)。执行模型的设计依据是 coroutine PR #396 合并提交 `1e9f0e8c25de85dde003def65bb58b90bdac0e0b`；实际构建消费的 coroutine 固定 commit 由 `cmake/coroutine.sha` 唯一提供（CI checkout 与本地 `bbt_coroutine_pin_verify()` 都读它，见 docs/dependencies/bbtools-coroutine-pin.provenance.md），设计依据 SHA 与消费 pin 不是同一个概念。旧自建线程/事件快照/受管 OnTraffic 路径已随接口收敛删除。不重复驱动 `io::GetExecutor()` 的共享 context，不改变 runtime Timer/Hook/CoPoller，也不迁移其它模块。

[CoTCP/CoUDP 与第三方 Binding 规格](docs/decisions/0005-co-io-adapter-contract-v1.md) 的“proc 执行收发与协议，sche 只检测就绪并唤醒”描述旧路径，不覆盖新 net。HTTP/Redis/Mongo/config/RPC 仍走各自既有路径，新 net 仅替换旧 TCP/协程桥接 API，不表示这些消费者已迁移。

当前 HTTP 为 Asio/Beast 异步推进，Redis 为 CoTCP owner binding（CoTCP 承担字节 I/O/等待/关闭，hiredis 只做同步 RESP2 编解码），Mongo 为同步 driver + worker bridge。CoTCP/CoUDP 已交付基础实现，Runtime 级 `max_inflight` 在途门禁已实现并合入（Issue #32 已关闭，见 [0005 §4](docs/decisions/0005-co-io-adapter-contract-v1.md)）；跨平台/非 Linux、公网 DNS 与部分 lifecycle 的直接验收证据仍未覆盖。

## 资源关闭与请求完成契约（进程寿命运行时）

coroutine 运行时按**进程寿命**存在、不参与业务资源收口（不再有 `Scheduler::Stop()`/restart 与运行时代际）。新 net 的关闭语义见 [关闭契约](docs/architecture/gnet-network-execution.md#6-网络关闭不是-runtime-停机)：`RequestClose` 仅封口并请求控制域收口，`Run` 返回才确认网络资源、callback 与同代解绑完成；不等待业务协程或外部句柄归零，关闭后已有接收数据仍可读，不承诺硬 deadline。该约束取代旧 Close(opt)，但不改下面的**旧模块** [0002 co-network/v1](docs/decisions/0002-co-network-contract-v1.md) 契约及状态：

- `ICoCloseable` 只有 `void Close() noexcept` + `bool IsClosed() const noexcept`。`Close()` 由资源 owner/framework manager 主动调用、同步完成物理释放并返回：**返回即该对象拥有的物理资源已释放、后端不会再访问它们**；未发送数据直接丢弃、不做 flush、不重开；**没有「等待关闭完成」的入口**。
- 旧模块 `ICoCloseable` 层已删除且不保留兼容壳：`RequestClose`、`WaitClosed`、`ReleaseClosed`、`CloseStatus` 等待结果枚举、`CompletionSignal`、`CancellationToken`/`CancellationSource`（含 `CallOptions.cancel` 字段）、`RuntimeGeneration`/`CurrentRuntimeGeneration`、`Scheduler::Stop()`/restart。新 net 的 `Connection::RequestClose()` 是连接级的封口请求，不是旧关闭契约的恢复。
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

### R2 TCP 使用入口（已合入主线）

公共头：`<bbt/infra/net/Engine.hpp>`、`RawHandler.hpp`、`Connection.hpp`、`ICoConnection.hpp`、`CoTcpConnection.hpp`、`ConnectionResult.hpp`、`Error.hpp`；只链接 `bbt::infra_net`。

> 下面是 **main 上已合入的形态**。0010 的「API 冻结候选」已由 [PR #104](https://github.com/yqm-307/bbtools-infra/pull/104) 合入：`Connection::State()`、公共 `Connection(state)`/`WriteOperation(record)` 构造、`Engine::AsyncConnectAwaitable`/`WaitConnect`/`WaitAccepted`、公共 `NormalizeOptions` 与 12 个历史 Options 字段已被删除或收回内部；新增公共 `Types.hpp`/`ReadAccess.hpp`。PR #104 进一步把协程消费面收口为 `ICoConnection`/`CoTcpConnection` 成员接口并删除 `CoRead/CoWrite/CoAccept/CoDial` 自由函数；符号表与删除迁移表见 [0010](docs/decisions/0010-gnet-network-foundation.md)。

前提与关闭：
- `Engine::Run(const EngineConfig&)` 阻塞占用**调用它的宿主 native 控制线程**：在受管协程上下文或任一已登记 Processer 线程上调用会以 `operation_not_permitted` 拒绝（判定取自 `GetLocalCoroutineId`/`TryGetLocalProcesser`，两版一致）。
- `EngineConfig.runtime_progress` 必填 `RuntimeProgress::Independent`，声明 runtime 内部 EventLoop 由独立执行流持续推进；未声明在绑定/建 socket 前以 `operation_not_permitted` 拒绝，runtime 未初始化以 `operation_not_supported` 拒绝。
- server 布局（`listen_addresses` 非空）要求 handler 与 handoff 至少一个存在：两者都无时 `Run` 在 driver 绑定/socket 副作用之前以 `operation_not_permitted` 拒绝。handler 与 handoff 可共存：同一连接既通知 `OnAccept` 也进 handoff，二者 `Id()` 相同，两个读取者只保证线性化字节前缀。
- `Connection::RequestClose()` 只是请求封口：立即拒绝新写并唤 owner；已 recv 但尚未
  进入 ring 的尾部由 owner 先转入 state 真源，之后才发布读侧最终 EOF。请求后、owner
  完成前读取可得到 `WouldBlock`，不得提前 EOF。首次请求的 reason（包括默认 `{}`）
  固定，之后写失败/重复请求不会重开接纳或覆盖该原因。`Run` 返回才表示 Engine 收口完成。不要在 `Run` 未返回时并发析构 Engine。
  - 正常收口由原 owner 完成已有连接的唯一 `OnClose`、待结算写的 `OnWrite` 与 pending connect 的 `OnConnect`，随后同代解绑；**不设隐藏关闭 deadline**，单次可恢复的命令提交失败会补交而非提前以 `Closing` 返回。只有 driver 真实 Failed 才走错误出口冻结并解绑。
  - 普通用户回调（`OnRead`/`OnAccept`/`OnConnect`）的开始授权点，是**最后一次紧邻 `SafeHandler` 派发前**的 `shared service_sealed` 原子检查（`NotifyReadOwner`、`AdoptFdOnOwner` 的交付前）：检查通过即为已授权在途，短回调必须完成；封口先于该检查则不启动成功回调（`OnAccept` 不调用，`OnConnect` 交付 canceled）。构造/登记阶段的入口早查只是提前短路，不构成授权。授权点与封口共同线性化，检查之后不再有可撤销的无限推迟。
- 原始回调必须短、非阻塞、不可挂起；接收 ring 是唯一真源，`OnRead` 只是可合并通知。
- `Engine::CoDial` 只用于未配置 `RawHandler` 的 Engine；配置了 raw handler 时
  在任何提交前明确返回 `operation_not_supported`，raw 的 pending/完成由
  `OnConnect` 唯一交付。两段式（提交 → 领取）是 `CoDial` 内部细节，不公开中间 ID；
  等待超时/取消终结本次领取并由 owner 回收未交付连接。
- `CoDial` 的 client-only Engine 若无 raw 接收者，请用空 `shared_ptr<RawHandler>` 构造：
  等待超时/取消后 owner 才可回收未交付 socket；有 raw handler 时视为已交付，不会因另一个
  Co 等待取消而误关。
- `Engine::CoAccept` 前置快速拒绝（先于任何状态判定 = 执行域 gate）：执行域非法 → `operation_not_permitted`；已封口/`Closing`/`Stopped` → `operation_canceled`；`Idle`/`Starting` → `operation_not_permitted`；`Running` 未开 handoff → `operation_not_permitted`；`Running` + handoff 但无 main accept owner → `operation_not_supported`。`error` 非空时 `connection` 无效（不覆盖调用者原句柄）。
- 所有协程成员入口（`Engine::CoAccept`/`Engine::CoDial` 与 `ICoConnection::Read`/`Write`/`WaitClosed`）在消费/接纳/提交之前校验执行域，非协程上下文一律 `operation_not_permitted`（含带现成数据、零长写、已就绪 accept 的快路径），不构造 `CoObject`、不创建 Processer、不产生网络副作用。
- `ICoConnection::AsyncWrite`/`Connection::AsyncWrite` 成功只表示库已接纳；`WriteOperation::Query()` 区分仍在途/完成与真实前缀。`WritePhase::Done` 只表示该次发送已到终态，**成功还须 `error` 为空**。`conn.Write(...)` 的等待超时/取消只结束等待（`wait_error` 写本次等待错误），已接纳操作仍由 owner 推进，可用 `report.operation` 查最终结果；不自动关闭或重发。
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

Co 薄封装（同一 raw 内核，成员接口形态）：

```cpp
// WaitOptions 只有一个字段 deadline（steady_clock::time_point）；examples 里的
// Until(3000ms) 就是这个类型。deadline 只约束本次等待，不撤销已接纳的 I/O。
const bbt::coroutine::WaitOptions options{std::chrono::steady_clock::now() + 3000ms};
net::ConnectionResult accepted = engine.CoAccept(options);   // 或 engine.CoDial(address, options)
if (accepted.error) { return; }
net::CoTcpConnection& conn = accepted.connection;            // ICoConnection 视图
net::ReadOutcome read = conn.Read(options);                  // owning Buffer 移交
if (read.state == net::ReadState::Eof) { /* 尾部已读完 */ }
net::WriteReport report = conn.Write(read.data.CopyTo(), options);
if (report.wait_error) {
    // timed_out: outcome.phase==InFlight 时已接纳仍在途，可 report.operation.Query()
    //            outcome.phase==Rejected 时未提交
}
conn.RequestClose();
conn.WaitClosed(options);   // 只等逻辑 closed；物理回收看 IsReleased/reclaim
```

覆盖边界：真实 port0 raw/Co 回环、sub owner 身份、多 listener、关闭后尾部/残留 Conn、有界发送/部分写/错误结算、容量超时未提交、完成超时仍在途、no-deadline 等待关闭结算、handoff 容量恢复、公平性、运行前提与 driver Failed 收口均有对应注册用例。未覆盖：长时/多连接性能、部分 Options 组合接线、Release/ASAN、全部 connect 超时回收的确定性动态用例。`ticker`、`edge_triggered_io`、`bind_to_device`、`multicast_interface_index`、`lock_os_thread` 这五个字段已从 `Options` 删除，是编译期拒绝（不存在入口），不再是 `Run` 里返回 `operation_not_supported`。高层 UDP 仅有 `<bbt/infra/net/Udp.hpp>` 编译拒绝入口。

### 新 net 构建与消费

公共头使用 `<bbt/infra/net/...>`，include 根是仓库根；只链接 `bbt::infra_net`。`BBT_COROUTINE_SOURCE_DIR` 指向实际消费的 coroutine 源码树；CMake 的来源路径检查**不代替** SHA 与实际链接核验。CI 与本地 configure 都把 coroutine 消费固定到 `cmake/coroutine.sha` 里的不可变 commit（见 docs/dependencies/bbtools-coroutine-pin.provenance.md）：CI checkout 读同一文件，`bbt_coroutine_pin_verify()` 要求该检出 HEAD==pin 且 tracked/staged 干净、消费面无 untracked 源码；非 git 树一律拒绝（来历不明的源码不能证明固定版本）。本地复现只需把 `BBT_COROUTINE_SOURCE_DIR` 指向 pin 的干净 git 检出，configure 自动核验。

```bash
set -e
test "$(git -C <coroutine 源码树> rev-parse HEAD)" = <本次核对的 commit>
test -z "$(git -C <coroutine 源码树> status --porcelain --untracked-files=all)"
cmake -S . -B <独立 build> -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON -DBBT_INFRA_NETWORK_ONLY=ON \
  -DBBT_COROUTINE_SOURCE_DIR=<coroutine 源码树> -DBUILD_TESTING=ON
cmake --build <独立 build> --parallel 2 --target \
  bbt_infra_net_header_selfcheck Test_net_primitives Test_net_allocation \
  Test_net_foundation Test_net_reactor_driver Test_net_r2 Test_net_b4_contract \
  net_foundation_consumer Example_co_net_service
ctest --test-dir <独立 build> -j1 --no-tests=error --output-on-failure
```

`network-only` 在旧目标、数据库/protobuf 包发现之前分流，残留 prefix 不启用被排除模块；不修改旧功能源码，也不代表旧模块已回归。首包消费工程见 [examples/net_foundation_consumer](examples/net_foundation_consumer/README.md)，协程用法示例见 [examples/co_net_service](examples/co_net_service/README.md)。`net.context_no_create` 是必过门禁（查询路径不再隐式创建 Processer，修复经 coroutine [PR #388](https://github.com/yqm-307/bbtools-coroutine/pull/388) 合入），未改断言或设置 skip。

上面的 target 列表是 main 的基线形态；PR #104 另加目标 `Test_net_freeze`（`net.freeze_*` 用例 + `net.freeze_public_surface` 正/负编译门禁）与 `Test_net_ico_write_close`（`net.ico_*`），已随该 PR 合入。

已启动 runtime 的测试与示例按 [执行域设计 §9](docs/architecture/gnet-network-execution.md#9-测试进程退出约定与已知限制) 经 `RunTestProcessExit` / `std::_Exit` 收口；该约定只规避测试进程静态析构竞态，不是 runtime 资源回收证据。

验收口径：PR #84 合入前 head `9f8e6019` 的交接记录报告云端 network-only job 的 registered.json 与 JUnit 一致（111 项注册、111 实跑、0 失败、0 跳过）；合入后主线 run [39305113509](https://github.com/yqm-307/bbtools-infra/actions/runs/39305113509) 的 `TCP network-only 构建与契约测试` job 元数据为 success。本次整理核对了交接记录与 run 元数据，没有重新下载并逐例复核原始测试日志或重跑本地测试，也不扩张为 Release/sanitizer/长压验收。

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
- `mongo_client_example`：Mongo 公开 target 消费（`FromJson` → `InsertOne` → `FindOne` → 类型化 getter/`ToJson` → `DeleteOne` 读回），只链接 `bbt::infra_mongo`、只含公开头；真实后端经 `BBT_TEST_MONGO_URI` 注入，未注入时 `skip:` + 77
- `Example_co_net_service`：R2 `Engine` + `Engine::CoAccept`/`conn.Read`/`conn.Write` 的真实 TCP 回环（network-only 构建，见 [该目录 README](examples/co_net_service/README.md)）

另有独立消费工程 `examples/config_consumer`（只链接 `bbt::infra_config`，不被本仓构建引用），
用于从仓外视角复核模块 target 可独立消费；命令见该目录 README 与 [docs/config-watch-v1.md](docs/config-watch-v1.md)。

## CI 覆盖边界

- PR/push CI 中**本仓定义的 job** 跑在 hosted `ubuntu-24.04`（`arc-s4-infra` 仅存历史含义）；跨仓 reusable callee（分类/计划、编译 & ctest 汇聚）按其固定 `ubuntu-latest` 运行。对本构建中已注册的全部测试执行 `ctest -j1`。
- CI 在 `prepare-deps` 阶段用 `scripts/prepare_resource_deps.sh` 以固定 tag+SHA 源码构建 hiredis/mongoc/mongocxx 私有前缀，并在 `release` job 显式断言 `redis.unit`/`mongo.unit` 已注册后纳入 `ctest -j1`；故 CI 覆盖 HTTP/transport/contract 面**以及 Redis/Mongo unit**。真实 run 证据见 [docs/ci/infra-ci-v1.md](docs/ci/infra-ci-v1.md)。
- Redis/Mongo **live 容器验收**纳入 Release job 后段：复用已构建产物，在独占临时容器中运行正常、停止态失败和重启恢复（A/B/C），另执行 `redis.cotcp_binding.live` 大值回归。缺 Docker/Compose/二进制、启动或测试失败均非零退出；不以 Skipped 代替通过。前段无服务的全量 CTest 仍可报告 live Skipped，不能将其当作后段 live 的实跑证据。`docs-only` 变更允许跳过整个 Release job。命令、资源边界和遗留条款对账见 [Redis/Mongo 验收说明](docs/redis-mongo-acceptance.md)。
- **TCP network-only 独立 job**：`network` job 复用同一 run 的 prepared deps，核实际 coroutine SHA 与 `cmake/coroutine.sha` 的 pin 双命中，以 `-DBBT_INFRA_NETWORK_ONLY=ON` 独立 Debug 构建显式目标集，跑全注册集并用 `scripts/ci/check_network_tests.py` 拒绝缺项/重复/失败/跳过，另对 `net.b4_co_pingpong_strict` 复跑 5 次。它不计入 Release/Debug 的旧模块结果，也不代替旧模块回归；`docs-only` 可跳过该 job 但结果汇聚仍判负于意外跳过。契约见 [docs/ci/infra-ci-v1.md](docs/ci/infra-ci-v1.md)。
- CI 的 coroutine 消费固定到 `cmake/coroutine.sha` 的不可变 commit；其它上游（core 等）仍按 `master` HEAD 消费，不代表对任意历史 SHA 的回溯兼容。

## 构建约束

本地验证边界与并发限制按 AGENTS.md 执行：必走冒烟 + 本次开发功能单测 + 直接耦合功能单测；新 configure 加 `-DCMAKE_CXX_COMPILER_LAUNCHER=ccache`；本地编译固定 `--parallel 2`；禁止 bare `--parallel` 或 `ninja -j$(nproc)`。
