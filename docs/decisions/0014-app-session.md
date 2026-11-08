# 0014：infra v2 AppSession 会话与 RPC 公共面

> 后续取代关系（2026-11-08）：本文的 RPC 入口命名、codec 布局和生命周期接口已由 [RPC 分层现状](../architecture/rpc-layered-protocol.md) 与 [现行指南](../guides/rpc-net.md) 取代；下文保留原会话设计与历史证据，旧签名和文件路径不再是当前消费入口。

- 状态：**目标契约草案（经父会话验收后冻结）**。本文冻结的是实现目标，不代表当前基线已经实现或通过验证。
- 适用：`bbtools-infra` 的通用 RPC 会话适配，以及 `bbt-framework` 的 AppNetwork 适配。
- 依赖方向：`bbt-framework → bbtools-infra → bbtools-coroutine`。
- 依据：`workspace/dag/bbt-v2-r1-brief.md`、`workspace/infra-interface-v2-plan.md`、三仓 `AGENTS.md`、`bbtools-infra/docs/guides/rpc-net.md`、`bbtools-infra/docs/architecture/gnet-network-execution.md`、当前公开头和 `proto/bbt/infra/rpc/v1/rpc_envelope.proto`。

本文不以旧 ADR 的自述冻结为正确性证据。当前基线 revision 及代码事实只用于区分已观察行为、目标契约和未实现证据；契约正确性由父会话逐项验收，真实实现和测试另行提供证据。

## 决策摘要

infra 提供通用的字节传输、会话、请求/回复关联、有限队列、错误阶段和资源生命周期。framework 负责 App 身份到业务对象的映射、Service/Actor 分类、Resolver、授权、mailbox 和业务处理。infra 公共头不引用 `App`、`Service`、`Actor`、`Component`、`MonitorService`、`AppObjectAddress` 或任何 framework 类型，也不解析业务 target。

一个本地 App 到一个远端 App 的默认资源键是由会话 owner 内部维护的 `(remote opaque identity, protocol profile, security context)`。同一键默认只有一个双向 TCP `AppSession` 和一个 `AppLink`。`AppSession`、`AppLink`、socket、fd、buffer、reader、writer 和 `net::Connection` 只属于 infra detail；framework 只能通过注入到 `AppNetwork` 的 RPC owner 使用共享会话并读取汇总观测。

一个会话承载 `System`、`Service`、`Actor` 三类 traffic class。三类流共享 envelope，lane 只决定有限排队、计量和调度；它不改变业务寻址，也不使 infra 解释 Service 名、Actor 类型、Actor id 或 mailbox 归属。默认消息流为 `Call`/`Response` 与 `Send`/可选 `SendAccepted`，均复用同一会话。

## 1. 现状、目标和分层

当前基线 `bbtools-infra` revision 为 `8573b7a831eeab75f7d787d1709ee330f20f8b63`。已观察到的代码事实如下：

| 当前事实 | 证据 | 对本契约的含义 |
|---|---|---|
| `rpc::Client` 当前使用 `IRpcTransport`，公开面是 `Create(transport)` 和 `Call(RpcAddress, RpcEnvelope, CallOptions)`，没有 `Send` | `bbt/infra/rpc/Client.hpp`、`bbt/infra/rpc/Transport.hpp` | v2 需要替换稳定入口；旧 seam 不作为 v2 目标 API |
| 当前 TCP adapter 使用 `u32be` 长度前缀，一连接一请求一回复，没有 multiplex 或跨请求复用 | `bbt/infra/rpc/TcpTransport.hpp` | 只能作为 v1 证据；不能证明 v2 AppSession |
| 当前 HTTP RPC 是固定 `POST /rpc` 的 request/reply 包装 | `bbt/infra/rpc/HttpTransport.hpp`、`docs/guides/rpc-net.md` | HTTP profile 保留为 request/reply；不冒充 AppSession 的 `Send` |
| 当前 RPC Server 的生命周期有 `Start → StopAccepting → Drain → Close → WaitStopped` | `bbt/infra/rpc/Server.hpp`、`bbt/infra/rpc/Transport.hpp` | v2 目标把封口与有界等待收敛到 `Close(deadline)`；超时仍保留 owner 责任 |
| 当前 v1 protobuf 使用字段 1–11，100–199 和 `extensions` 保留 | `proto/bbt/infra/rpc/v1/rpc_envelope.proto`、`bbt/infra/rpc/RpcWire.hpp` | v2 使用独立 profile 和正式字段号；不在 v1 上隐式复用字段 |
| 当前 Engine 的物理 owner、接收 ring、写入记账和 `Run` 收口边界不向上泄漏 | `bbt/infra/net/Engine.hpp`、`docs/architecture/gnet-network-execution.md` | v2 会话只能借用 Engine；不把物理连接责任移给 framework |

目标分层固定为：

```text
CoApp
  └── framework AppNetwork
        ├── shared infra::net::Engine
        ├── shared infra::rpc::Client  ← AppSession owner
        └── infra::rpc::Server         ← 注入同一个 Client owner
              └── infra::rpc::detail::AppSessionState
                    └── detail::AppLink × 1..N
                          └── net::Connection
```

`Client` 是一个 App 的出站会话表和会话状态 owner。`Server` 不创建第二份 session registry；创建时显式注入同一 `std::shared_ptr<Client>`，把入站连接交给该 owner。这个新增参数是双向复用所必需的最小扩展，不新增公共 `SessionRegistry` 或 App 专用 manager。

## 2. C1：稳定 RPC 接口和完整值类型

### 2.1 通用值类型

以下类型属于 infra 公共面，只使用标准库、`bbt::infra` 和 `bbt::coroutine` 类型。

```cpp
namespace bbt::infra::rpc {

enum class TrafficClass : std::uint8_t {
    System  = 0,
    Service = 1,
    Actor   = 2,
};

enum class ReplyMode : std::uint8_t {
    None     = 0,
    Required = 1,
};

using Metadata = std::vector<std::pair<std::string, std::string>>;

struct RequestContext {
    // 入站时由 infra 根据本地期限和 wire remaining budget 设置。
    bbt::coroutine::Deadline deadline{bbt::coroutine::Deadline::max()};
    // 来自已完成会话握手的远端 opaque identity；不是认证凭据。
    std::string peer_identity;
    // 来自可信 transport/adapter 的认证结果；trusted-network 模式恒为空。
    std::string peer_principal;
};

class Request {
public:
    Request() = default;
    Request(std::string service, std::string method);

    const std::string& Service() const noexcept;
    const std::string& Method() const noexcept;
    const std::string& RequestId() const noexcept;
    const std::string& MessageId() const noexcept;
    const std::string& RequestSchema() const noexcept;
    const std::string& ResponseSchema() const noexcept;
    const std::vector<std::uint8_t>& Payload() const noexcept;
    const bbt::infra::rpc::Metadata& Metadata() const noexcept;
    TrafficClass Traffic() const noexcept;
    ReplyMode Reply() const noexcept;
    const std::string& OrderingKey() const noexcept;
    std::uint64_t Sequence() const noexcept;
    const RequestContext& Context() const noexcept;

    Request& SetService(std::string value);
    Request& SetMethod(std::string value);
    Request& SetRequestId(std::string value);
    Request& SetMessageId(std::string value);
    Request& SetRequestSchema(std::string value);
    Request& SetResponseSchema(std::string value);
    Request& SetPayload(std::vector<std::uint8_t> value);
    Request& SetMetadata(Metadata value);
    Request& SetTraffic(TrafficClass value);
    Request& SetReply(ReplyMode value);
    // ordering_key 是 opaque bytes；infra 只做长度、相等和同会话比较。
    Request& SetOrderingKey(std::string value);
    Request& SetSequence(std::uint64_t value);
};

class Response {
public:
    Response() = default;
    explicit Response(std::vector<std::uint8_t> payload);

    const std::string& RequestId() const noexcept;
    const std::string& Service() const noexcept;
    const std::string& Method() const noexcept;
    const std::string& RequestSchema() const noexcept;
    const std::string& ResponseSchema() const noexcept;
    const std::vector<std::uint8_t>& Payload() const noexcept;
    const bbt::infra::rpc::Metadata& Metadata() const noexcept;

    Response& SetRequestId(std::string value);
    Response& SetService(std::string value);
    Response& SetMethod(std::string value);
    Response& SetRequestSchema(std::string value);
    Response& SetResponseSchema(std::string value);
    Response& SetPayload(std::vector<std::uint8_t> value);
    Response& SetMetadata(Metadata value);
};

struct TrafficStats {
    std::uint64_t queued = 0;       // 当前等待出队的条目数
    std::uint64_t dequeued = 0;     // 累计从队列交给 dispatch/mailbox 的条目数
    std::uint64_t inflight = 0;     // 已交给 admission、尚未到责任终态的条目数
    std::uint64_t finished = 0;     // 累计完成处理和所需回复写终态的条目数
    std::uint64_t dropped = 0;      // 过载、旧 epoch 或显式丢弃
    std::uint64_t bytes_queued = 0; // 当前排队字节数
};

struct ClientStats {
    std::uint64_t sessions = 0;
    std::uint64_t links = 0;
    std::uint64_t remote_acceptance_pending = 0;
    TrafficStats system;
    TrafficStats service;
    TrafficStats actor;
};

struct ServerStats {
    std::uint64_t accepted_connections = 0;
    std::uint64_t active_connections = 0;
    std::uint64_t retiring_connections = 0;
    std::uint64_t callbacks = 0;
    TrafficStats system;
    TrafficStats service;
    TrafficStats actor;
};

} // namespace bbt::infra::rpc
```

`Request::Context()` 是 handler 读取入站期限、远端 opaque identity 和已认证主体的唯一入口。它没有公开 setter，业务不能伪造 `peer_identity` 或 `peer_principal`。出站 Request 的 Context 为空值；跨进程期限只由 `CallOptions`/`SendOptions` 换算成 `remaining_budget_ms`，不传播本地 `steady_clock::time_point`。

`Request::Traffic()` 是 traffic class 的唯一真源。`SendOptions` 不再设置 traffic class；`Client::Send` 使用 `Request::Traffic()`，避免 `Request.Traffic` 与 `SendOptions.Traffic` 同时存在而产生冲突。`System` Request 只能由 infra 内部生成，业务和 framework 不能通过公共 Request 伪造控制帧。

`Request::Reply()` 是 wire 的唯一 reply mode。`Call` 要求 `Required`，`Send` 要求 `None`；带远端接纳确认的 `Send` 仍然是 `None`，确认使用独立 `SendAccepted` 控制帧，不把业务处理结果变成第二套 RPC。

`Response` 的 correlation 字段由 handler 返回后由 Server 校验。Server 允许 handler 填写业务 payload 和 metadata；关联字段必须与 Request 一致，缺失或不一致返回 `ProtocolError`，不能让 handler 改写另一个请求的回复。

### 2.2 Options、Admission 和 Client

`CallOptions` 保留在 `bbt::infra` 公共值层，目标接口为：

```cpp
namespace bbt::infra {

class CallOptions {
public:
    CallOptions() = default;
    CallOptions& SetDeadline(bbt::coroutine::Deadline value) noexcept;
    bbt::coroutine::Deadline Deadline() const noexcept;
    result<void> Validate() const;
};

} // namespace bbt::infra
```

顶层 Call 必须有有限、未过期的 deadline；子调用只能收紧父期限。出站 adapter 在提交前验证期限，超时不会创建连接、写入帧或进入无界等待。

```cpp
namespace bbt::infra::rpc {

class ClientOptions {
public:
    ClientOptions& SetLocalIdentity(std::string value);
    ClientOptions& SetTrustedNetworkPlaintext(bool value) noexcept;
    bool TrustedNetworkPlaintext() const noexcept;
    ClientOptions& SetMaxSessions(std::size_t value);
    ClientOptions& SetMaxFrameBytes(std::size_t value);
    ClientOptions& SetSystemQueueBytes(std::size_t value);
    ClientOptions& SetServiceQueueBytes(std::size_t value);
    ClientOptions& SetActorQueueBytes(std::size_t value);
    ClientOptions& SetServiceWeight(std::uint32_t value);
    ClientOptions& SetActorWeight(std::uint32_t value);
    ClientOptions& SetControlDataSplit(bool value);
    result<void> Validate() const;
};

class SendOptions {
public:
    SendOptions& SetDeadline(bbt::coroutine::Deadline value) noexcept;
    SendOptions& RequireRemoteAcceptance(bool value = true) noexcept;
    bbt::coroutine::Deadline Deadline() const noexcept;
    bool RequiresRemoteAcceptance() const noexcept;
    result<void> Validate() const;
};

class Client {
public:
    static result<std::shared_ptr<Client>> Create(
        std::shared_ptr<bbt::infra::net::Engine> engine,
        ClientOptions options);

    bbt_co_async(result<Response>) Call(
        std::string_view target,
        Request request,
        const bbt::infra::CallOptions& options);

    bbt_co_async(result<void>) Send(
        std::string_view target,
        Request message,
        const SendOptions& options);

    void Close() noexcept;
    bool IsClosed() const noexcept;
    ClientStats Stats() const noexcept;
};

} // namespace bbt::infra::rpc
```

`Client::Create` 只校验 options 并持有共享 Engine，不启动 Engine、不创建线程、不监听、不拨号。`target` 的 v2 TCP 形态是 `tcp://host:port`；首次使用时按 endpoint 建立 provisional session，握手完成后才绑定远端 opaque identity。HTTP RPC 可继续支持 `Call`，但在 HTTP profile 上 `Send` 返回 `UnsupportedRoute`，不把 HTTP request/reply 假装成 AppSession 单向消息。

`Client::Close()` 是幂等、不可重开、立即逻辑封口；它不关闭共享 Engine。未提交调用返回 `Closed`，已经交给当前会话但没有可信终态的调用保留 `OutcomeUnknown`/请求阶段事实。物理连接由 session/net owner 继续收口，不能因为 Client 的逻辑状态已关闭就释放仍有 callback 或 I/O 责任的对象。

### 2.3 Server、LocalAddress、Stats 和生命周期

Server 的入站 adapter callback 接收 owning `AdmissionItem`；其中的 `Request::Context()` 是唯一上下文入口，不再通过独立的 `IncomingCallContext` 参数传递。

```cpp
namespace bbt::infra::rpc {

class ReplyCompletion {
public:
    bbt_co_async(result<void>) Complete(Response response);
    bbt_co_async(result<void>) Fail(Error error);
    bool IsTerminal() const noexcept;
};

class AdmissionLease {
public:
    bool Release() noexcept;             // 仅第一次调用成功
    bool IsReleased() const noexcept;
};

struct AdmissionItem {
    Request request;                     // owning，转移给 framework 队列
    std::shared_ptr<ReplyCompletion> reply;
    std::shared_ptr<AdmissionLease> lease;
};

// 只做同步校验和有界转移；不得挂起或调用普通业务 handler。
using AdmissionHandler = std::function<result<void>(AdmissionItem&&)>;

class AdmissionQueue {
public:
    static result<std::shared_ptr<AdmissionQueue>> Create(
        std::size_t max_items, std::size_t max_bytes,
        AdmissionHandler handler);
    result<void> Admit(AdmissionItem item); // 单 FIFO、不可挂起
    void Close() noexcept;
};

class ServerOptions {
public:
    ServerOptions& SetListenAddress(bbt::infra::net::Address value);
    ServerOptions& SetMaxFrameBytes(std::size_t value);
    ServerOptions& SetMaxConnections(std::size_t value);
    ServerOptions& SetMaxInflight(std::size_t value);
    ServerOptions& SetMaxHandlerBytes(std::size_t value);
    ServerOptions& SetIncomingTimeout(std::chrono::milliseconds value);
    result<void> Validate() const;
};

class Server {
public:
    static result<std::shared_ptr<Server>> Create(
        std::shared_ptr<bbt::infra::net::Engine> engine,
        std::shared_ptr<Client> session_owner,
        AdmissionHandler admission_handler,
        ServerOptions options);

    result<void> Start();
    void StopAccepting() noexcept;
    result<void> Drain(bbt::coroutine::Deadline deadline);
    result<void> Close(bbt::coroutine::Deadline deadline);
    bool IsClosed() const noexcept;
    result<bbt::infra::net::Address> LocalAddress() const;
    ServerStats Stats() const noexcept;
};

} // namespace bbt::infra::rpc
```

`session_owner` 是双向会话复用的必要装配参数。Server 不另建 registry，也不把入站 socket 交给 handler；它把完成握手的 link 和入站 Request 交给同一 Client owner 的 session state。`session_owner` 的生命周期必须覆盖 Server；framework 的 `AppNetwork` 按 `Server → Client → Engine` 的顺序收口。

Server 状态和调用上下文固定如下：

- `Start()`、`StopAccepting()`、`Drain()`、`Close()` 和 `LocalAddress()` 由非协程宿主上下文调用；误在受管协程或 Processer 域调用返回 `InvalidContext`，不等待自身业务任务。
- `Start()` 一次性完成监听和 admission handler 登记；Engine 必须已处于可接纳状态。`LocalAddress()` 仅在 `Start()` 成功后有效，动态端口返回实际绑定的 `net::Address`；未启动、启动失败或地址已不可观察时返回明确错误。
- `StopAccepting()` 只封新连接和新 Request，不取消已经被 session 接纳的 handler、mailbox admission 或回复写入。
- `Drain(deadline)` 必须先 `StopAccepting()`；它等待 Call 的 reply/lease 终态以及 Send admission 和确认写责任收口，不等待 mailbox 已接纳后的 Tell 业务执行，不强杀业务任务。超时返回 `TimedOut`，保留 Server、session owner、Engine 和所有在途责任。
- `Close(deadline)` 立即封 Server 的逻辑入口，继续等待 listener、callback、发送责任和物理连接在 deadline 内收口；它不强杀 handler，不关闭共享 Engine。超时返回 `TimedOut`，`IsClosed()` 仍为 true，但 owner 必须保留并继续驱动 Engine，之后可以再次以更晚 deadline 调用 `Close()` 观察最终收口。没有物理收口证据时不得析构 owner。
- `Close(deadline)` 收敛原先 `Close() + WaitStopped()` 的两段等待接口；`Stats()` 中的 `retiring`、`inflight`、`callbacks` 和连接计数是后续继续收口的观察面。

Stats 是按值返回的汇总取样，不是同一时刻的一致快照：字段由多把锁/多个 atomic 分步读取（owner gauge、逐 session 的 out_mu、队列自己的 impl->mu），因此跨字段等式可能短暂不成立。lane 字段是当前 session 台账的汇总，session 退役后可能下降；当前字段是各自取样点的 gauge；判定泄漏或收口用 `Drain`/`Close`/`WaitStopped` 的返回值或 `detail::ServerV2Ledger`，不要用单次取样的跨字段不等式。这些类型已在 §2.1 的稳定值类型代码中定义。

`queued/dequeued/inflight/finished` 的职责不能混用：`queued` 是有限队列尚未交付；`dequeued` 表示已经交给 framework dispatch 或 mailbox；Call 的 `inflight` 延续到业务处理和回复写入终态，Send 的投递责任在 framework mailbox 接纳和所需 `SendAccepted` 写终态后结束。`finished` 只在对应责任有终态后累计。`Drain` 必须同时看到业务 admission 队列为空、infra 仍持有的 `inflight == 0`、回复写责任已结算和 Server 自有 callback 已收口；只看 TCP 写完成或只看 `queued == 0` 都不足以完成 Drain。

### 2.3.1 受控检查点（固定动作，不是用户回调）

写路径与握手路径上有四个库内固定检查点，类型是公开的 `rpc::SessionHooks`（`bbt/infra/rpc/Hooks.hpp`），只经 `Client::Hooks()` 取得 owning handle。Server 不提供 `Hooks()`：Server 创建时已经注入同一个 Client owner，入站检查点与出站检查点共用这一份控制状态。

| 检查点 | 位置 | 停在这里意味着 |
|---|---|---|
| `HookPoint::BeforeWrite` | writer 已出队、尚未标记提交、尚未 `conn.Write` | 本条已出队、写终态未发生：回复凭据不会提前成功，Drain/Close 也不会提前报收口 |
| `HookPoint::OutboundBeforeHandshake` | 出站 conn 已拨号成功、握手 I/O 尚未开始 | 额度已占、link 未发布：封口与发布在这里竞争 |
| `HookPoint::InboundBeforeHandshake` | 入站 conn 已 accept、握手 I/O 尚未开始 | 入站连接已在握手台账、尚未进入会话 |
| `HookPoint::InboundBeforePublish` | 入站握手完成、link 尚未安装/发布 | 握手结果已在本端、封口与发布在这里竞争 |

每个点只有两种由库实现的动作，`HookAction::Pause` 与 `HookAction::ThrowOnce`。公开面没有 `HookSlot`、`std::function`、回调对象、虚接口或异常对象输入；严格路径不执行任何用户代码。四个点都在现有受管协程内、相关锁已释放之后进入。

使用方只做三件事：

- `Arm(point, action, expires_at)` 返回该点的单调 generation。窗口必须落在 `(now, now + 10s]`；过去时间、`Deadline::max()`、超过上限、未知 point/action 返回 `InvalidArgument`，owner 封口后返回 `Closed`，generation 耗尽返回 `RuntimeUnavailable` 且不回绕。新注册解除旧 generation 的 Pause；旧在途命中只退出旧 generation，不追随新配置。
- `Clear(point, generation)` 只解除与 token 匹配的那一次注册。重复、已过期或不属于当前配置的 token 是成功的 no-op，不会清掉后来的注册。Clear 只更新状态，不等待在途退出，也不做排空；需要排空证据的调用方在自己的 deadline 内轮询 `Snapshot` 的 `in_flight`。
- `Snapshot(point)` 只返回固定值：`generation`、`action`、`armed`、`closed`、`entered`、`in_flight`。它是控制状态的小值快照，不是 RPC 全局一致快照，也不代表业务成功或物理释放。`entered` 统计当前 generation 的命中次数，`in_flight` 统计跨 generation 尚未退出的命中次数，所以 Clear 返回不等于 `in_flight` 为零。

`Pause` 的挂起只发生在库自己的 `CoWaiter` 上：每个在途 Pause 独占一个等待者，按 `10ms` 分片检查注册状态、owner/server/link/conn 关闭、协程取消与原操作期限，每片重新计算 `min(操作期限, 注册到期, now + 10ms)`。这里只承诺库控制的等待，不声称 timeout 能中止任意 C++ 代码。解除后仍走原封口检查与安装/提交事务；Clear、替换和注册到期解除诊断暂停，但不重置业务期限。关闭、取消和操作期限按既有错误失败。Server Close 只影响对应入站连接和已关闭的 link，不封掉同一 owner 的 Client 能力。控制 handle 不反向拥有业务 owner 或 Engine：owner 收口后 handle 仍可查询，只是永久 `closed`。

`ThrowOnce` 由库抛出固定内部标签，生产与测试走同一机制，不接受用户异常对象或消息。调用点把它转成可见错误并完成资源结算：`BeforeWrite` → 本条已出队写责任结算为 `InternalError`（回复凭据拿到失败终态、`writes_outstanding` 归零）并退役 link；`OutboundBeforeHandshake` → 本次建立返回 `InternalError`，establishing 记录、占位额度、`done` 与 conn 由建立路径统一归还；`InboundBeforeHandshake` 与 `InboundBeforePublish` → 该入站连接退役，不发布 link。异常不得越过各自的责任边界。提交之后的写失败仍按既有 `OutcomeUnknown` 结算，不把 `RequestClose` 写成物理 Released。

配置挂在 owner 上，同一 owner 下的所有 link 共用一份，不同 owner 完全隔离；没有进程级全局检查点。真实 TCP 集成测试保留，检查点不替代真实验收。

### 2.4 Response 构造和业务异常

业务可以用默认构造的 Response 并调用 `SetPayload`、`SetMetadata` 和关联字段 setter 生成回复；不能通过 Response 构造函数获得 socket、session 或业务上下文。`ReplyCompletion` 与 `AdmissionLease` 由 session 为每项创建，不能由业务构造。framework 将 `AdmissionItem` 转移进自己的有界 mailbox/dispatch queue 后，才在独立业务协程中执行普通业务 handler；Call handler 完成后 `bbt_co_await item.reply->Complete(response)`（异常则 `Fail(error)`），回复写入终态后再 `item.lease->Release()`。infra 不创建 mailbox、不解析 target、不替 framework 执行 Actor/Service 策略。

`AdmissionQueue::Admit` 是 Call 与 Send 共用的唯一按序接纳入口：队列满立即返回 `Overloaded`，不等待、不调用可挂起代码。`AdmissionHandler` 返回成功前必须完成 owning item 的框架队列转移；返回错误表示未接纳，由 infra 依 `ReplyMode` 生成 Call 错误或一次性 SendAccepted 错误。Send 成功入 mailbox 后，infra 的投递 lease 即可收口，后续 Tell 业务队列与执行寿命归 framework。

## 3. C2：双端共享一个会话的装配、身份和 epoch

### 3.1 AppNetwork 的最小装配

framework 的 `AppNetwork` 在每个进程内持有一个 Engine、一个 Client 和一个绑定同一 Client 的 Server。两端都使用同样的装配形态：

```cpp
bbt::infra::rpc::ClientOptions client_options;
client_options.SetLocalIdentity("app-a")
              .SetTrustedNetworkPlaintext(true)
              .SetMaxSessions(64)
              .SetMaxFrameBytes(8 * 1024 * 1024);

auto client_a = bbt::infra::rpc::Client::Create(engine_a, client_options);
// result 检查后：
std::shared_ptr<bbt::infra::rpc::Client> shared_client_a = client_a.value();

auto server_a = bbt::infra::rpc::Server::Create(
    engine_a,
    shared_client_a,
    app_a_dispatch_handler,
    server_options_a);
```

进程 B 使用 `SetLocalIdentity("app-b")`、自己的 Engine、自己的 Client owner 和同样的 Server 注入关系。两个 Engine 由各自宿主推进；Client/Server 不偷偷创建第二个 Engine、线程或公共 manager。`CoApp` 只在 framework 内把 `CoCall`/`CoTell` 映射到这个共享 Client，业务 Service/Actor 不持有 Client、Engine 或 link。

### 3.2 endpoint 到 opaque peer 的首次映射

1. `Client::Call/Send(target, ...)` 先解析 `tcp://host:port`，形成 provisional endpoint key；地址只说明拨号位置，不等于远端 App identity。
2. 新 link 只能先发送受限的 `Hello`。Hello 携带本端 `ClientOptions::SetLocalIdentity()` 产生的 opaque identity、协议 profile、connection nonce 和本端 epoch proposal；不携带 framework App 类型或业务 metadata。
3. 当前公开基线没有 verifier API。双方显式设置 `SetTrustedNetworkPlaintext(true)` 时，完成 profile 校验后可把 Hello 中的自报 opaque identity 仅用于进程内寻址和去重；此模式的 `RequestContext.peer_principal` 恒为空，identity 不用于授权，也不能替换已有认证 session。未显式启用该模式则因没有可用 verifier 在 Create/Start 前返回 `UnsupportedRoute`。
4. handshake 成功后，session owner 原子地安装 `endpoint → peer identity` 和 `peer identity + profile + security context → session` 两个映射。已有认证 session 不被新的自报 identity 替换；冲突 provisional link 被 fenced 后关闭。
5. 后续相同 PeerKey 的 Service/Actor Call 和 Send 都使用同一 session。不同 remote identity、profile 或 security context 不共享 link。

trusted-network 明文模式是部署边界，不是认证保障；默认不得把它暴露到公网。当前公开装配只依赖 `ClientOptions::SetTrustedNetworkPlaintext(true)`，不引用未定义的测试 adapter/verifier；若以后接入认证 transport，必须另定 verifier API 和已认证 peer-principal 契约。

### 3.3 同时拨号和入站复用

两端同时拨号时，双方比较已协商的 identity 字节序：identity 较小的一侧保留其**本地发起**的 link；identity 较大的一侧保留其**入站接受**的 link；另一条 duplicate link 在完成必要的 close/fence 后退出。identity 相同直接拒绝自连。这个规则只处理同一 PeerKey 的 duplicate link，不为不同 security context 或不同 profile 合并会话；trusted-network 的 self-reported identity 不因此变成认证主体。

B 的 Server 收到 A 的入站 link 后，把 link 交给 B 的 `session_owner`。A 的 Client 仍是 A 的会话 owner；B 的 Client 仍是 B 的会话 owner。入站和出站方向共享同一 session state、epoch、lane 统计和 Request 关联表，因此 B 不会形成一份只供 Server 使用的孤立连接池。

### 3.4 epoch 协商和旧 link fencing

session epoch 不是两端各自递增后碰巧相等的值。每个 PeerKey 的 epoch owner 由已协商 identity 的字节序确定：identity 较小的一侧是 epoch owner；trusted-network 模式下这仍只是去重/围栏键，不是认证依据。握手过程为：

1. 双方交换 `Hello`，校验 identity、profile、connection nonce 和最大帧预算。
2. epoch owner 在自身 session owner 内为本次新 session 选择非零 epoch；同一进程寿命内单调递增，不使用另一端独立计数推导。
3. epoch owner 发送带 selected epoch 的 `HelloAck`；另一端只能原样 echo 该 epoch 的 `HelloAck`。双方都收到并校验后才开放 Service/Actor 数据帧。
4. 每条数据帧的 `session_epoch` 必须等于当前 negotiated epoch。旧 epoch 的帧、旧 link 的晚到 callback 和重复 confirmation 都丢弃，不得改写已经发布的终态。

epoch 不是认证凭据，也不承诺跨进程重启、路由迁移或持久化连续性。若上层需要跨重启身份连续性，必须另有持久代际或权威 fencing contract；本 ADR 不虚构这一保证。

当新 epoch 被发布时，旧 transport queue 中尚未进入 framework admission 的帧全部丢弃；已经成功进入 framework mailbox/admission 的消息不回滚、不重复 admission，由 framework 按自己的业务状态继续处理或记录缺口。旧 link 物理 owner 先完成退役和 callback 收口，再允许新 epoch 的额度完全替代旧额度。

## 4. C3：Send 的本地接纳、远端接纳和业务处理

### 4.1 两个独立阶段

`Send` 有两个可观察阶段：

- **本地接纳**：Request 已通过校验并进入当前 session 的有限 lane queue；`RequireRemoteAcceptance(false)` 在此阶段成功返回。它不表示已经写出、远端收到、远端 mailbox 接纳或业务处理完成。
- **远端接纳**：远端单 reader 将完整 Send frame 交给 framework adapter，adapter 已成功把它放入目标 mailbox/dispatch queue 后生成 `SendAccepted`。`RequireRemoteAcceptance(true)` 只有收到可信 `SendAccepted` 才返回成功；此确认仍不表示业务 handler 已结束。

`SendOptions::RequireRemoteAcceptance()` 只编码为 Send 的 `acceptance_requested`；false 不生成成功确认，true 在 mailbox/dispatch 入队成功或被拒后各生成一次确认。发送方 waiter 以 `(PeerKey, session_epoch, message_id)` 关联，重复或旧 epoch 的确认只丢弃/计数，不二次完成 waiter。

本地容量满、参数错误、deadline 已过都在本地接纳前返回，零接纳且不创建补偿队列。`Send` 可以挂起当前调用协程等待一个**有界**本地名额或远端确认，但不得阻塞共享连接 reader、Engine driver 或其他 traffic lane；deadline/取消只结束调用方等待，不撤销已经接纳的发送责任。

### 4.2 单 reader、共用 admission 队列和异步完成

每条 AppSession 只有一个按帧顺序推进的 reader。reader 完成长度和 profile 校验后，只调用该 session 的 `AdmissionQueue::Admit`；reader 不调用 `AdmissionHandler`、普通业务 handler 或回复等待，也不把同一帧复制给多个并发 reader。队列只有一个 FIFO admission worker，`Admit` 满则立即 `Overloaded`，不挂起。

Call 与 Send 共用这个 FIFO。framework 为同一目标使用同一 opaque `ordering_key` 和连续 `sequence`：`Call1(seq=1) → Send2(seq=2) → Call3(seq=3)` 必须按该顺序进入同一目标的 admission/mailbox；不同 key/目标的 item 在各自入队后可由不同业务协程并行，infra 不承诺跨目标处理顺序。`AdmissionHandler` 只校验并把 owning `AdmissionItem` 转移给 framework，不解析 `ActorId`。

接纳成功后，Call 的 framework 业务协程可挂起执行；它必须在 `ReplyCompletion::Complete/Fail` 的异步回复写入终态后释放 `AdmissionLease`。Send 在 mailbox 入队成功后即结束 infra 投递责任，业务 Tell 的执行、异常和排空由 framework 自己的队列/lease 管理，infra Drain 不等待业务完成。接纳失败由 infra 只发布一次 Call 错误或（`acceptance_requested=true` 时）一次 SendAccepted 错误；Send 已获成功接纳后，后续业务异常不改写已发出的确认。

`AdmissionHandler` 或异步业务任务抛出的异常必须在各自边界转为 `InternalError`/框架终态，不穿透 reader、Engine 或 App 主循环；一条 item 的 lease 和 reply completion 只能各自发布一次终态。

### 4.3 lane 和 mailbox 过载

`System` lane 有固定保留容量，包含 Hello、HelloAck、SessionError、Close 和 SendAccepted。`Service` lane 有独立发送/在途额度和最小服务份额。`Actor` lane 有独立 aggregate queue；每 mailbox、Actor 总额度和 Actor 数量由 framework 管理。任何 lane 都不使用无界排队或等待另一个 lane 释放。

mailbox 满时，framework adapter 立即拒绝该目标消息并记录 dead-letter/drop；reader 继续处理同一 session 的其他 frame。`RequireRemoteAcceptance(true)` 收到 `Overloaded`，`false` 不产生远端业务错误。一个 mailbox 的满载不阻塞 System、Service 或其他 Actor。

## 5. C4：v2 wire、分帧、错误和可靠性边界

### 5.1 profile 和字段号

AppSession 使用独立 profile version `2`。逻辑 frame 的 protobuf 定义必须使用下表字段号；生成 protobuf 类型停留在 `rpc/detail`，不进入 C++ 稳定头。字段号一经实现发布不得复用、改号或改变含义；废弃字段改为 `reserved`。

```proto
syntax = "proto3";
package bbt.infra.rpc.v2;

message SessionFrame {
  uint32 profile_version = 1;   // 固定 2
  FrameKind frame_kind = 2;
  TrafficClass traffic_class = 3;
  string peer_identity = 4;     // 仅 Hello/HelloAck
  uint64 session_epoch = 5;     // Hello/HelloAck 和所有数据帧
  string message_id = 6;       // Send/SendAccepted 必填
  string request_id = 7;        // Call/Response 必填
  string service = 8;
  string method = 9;
  ReplyMode reply_mode = 10;
  uint64 sequence = 11;
  bytes ordering_key = 12;
  uint32 remaining_budget_ms = 13;
  string request_schema = 14;
  string response_schema = 15;
  bytes payload = 16;
  repeated MetadataEntry metadata = 17;
  oneof outcome {
    bool success = 18;
    RpcError error = 19;
  }
  uint64 connection_nonce = 20; // 仅 Hello/HelloAck，非认证随机数
  optional bool acceptance_requested = 21; // 仅 Send；false 不请求确认
}

message MetadataEntry {
  string key = 1;
  string value = 2;
}

enum FrameKind {
  FRAME_KIND_UNSPECIFIED = 0;
  HELLO = 1;
  HELLO_ACK = 2;
  CALL = 3;
  RESPONSE = 4;
  SEND = 5;
  SEND_ACCEPTED = 6;
  SESSION_ERROR = 7;
  CLOSE = 8;
}

enum TrafficClass {
  TRAFFIC_SYSTEM = 0;
  TRAFFIC_SERVICE = 1;
  TRAFFIC_ACTOR = 2;
}

enum ReplyMode {
  REPLY_NONE = 0;
  REPLY_REQUIRED = 1;
}
```

字段约束固定为：

| 字段 | 存在条件与约束 |
|---|---|
| `profile_version` | 所有 frame 必须为 `2`；其他版本返回 `ProtocolError`，不得回退 v1 |
| `frame_kind` | 只接受表中 1–8；未知枚举拒绝；System lane 只允许控制 frame |
| `traffic_class` | 数据 frame 使用 `Service` 或 `Actor`；Hello/HelloAck/错误/关闭/确认使用 `System` |
| `peer_identity` | 仅 Hello/HelloAck，UTF-8、非空、≤256 字节；不是认证凭据 |
| `session_epoch` | 非零；必须等于当前协商 epoch；旧值丢弃并计 session drop |
| `message_id` | Send 和 SendAccepted 非空，≤128 字节；不得代替 `request_id` |
| `acceptance_requested` | 使用 presence 的 bool；仅 Send 可出现，`false` 不生成成功确认，`true` 在入队成功或拒绝后只生成一次 SendAccepted；Call/Response/Hello/HelloAck/SessionError/Close 必须 absent |
| `request_id` | Call 和 Response 非空且 Response 原样回显；无回复 Send 可为空 |
| `service`/`method` | Call/Response 必须非空；Send 由 framework 映射逻辑地址，infra 只做值和长度检查 |
| `reply_mode` | Call 固定 Required；Send 固定 None；SendAccepted 不承载业务 reply |
| `sequence`/`ordering_key` | `sequence == 0` 表示未启用顺序检查；启用时 key 非空、≤256 字节，infra 只比较 opaque bytes，不解析 Actor |
| `remaining_budget_ms` | 跨进程只传剩余预算，合法范围 `1..24h`；0、溢出或缺失按 frame 类型拒绝 |
| `request_schema`/`response_schema` | Call/Response 必须非空；Response 必须与请求相同 |
| `payload` | 只受 frame 和本地 body 预算限制；infra 不解业务 payload |
| `metadata` | 只允许 `fw.*`、`route.*`、`trace.*`；≤32 项，key ≤64、value ≤256 UTF-8 字节，不得重复 |
| `outcome` | 请求 frame 不带 outcome；Response/SendAccepted/SessionError 为 success/error 二选一 |
| `connection_nonce` | 仅握手 frame，非零；只用于 duplicate link 诊断和选择，不作为认证或 epoch |

`RpcError` 的字段号固定为 `code=1`、`domain=2`、`domain_code=3`、`message=4`、`backend_category=5`、`backend_code=6`、`transferred_bytes=7`、`details=8`、`request_phase=9`。`details` 沿用 infra 的最多 16 项、key ≤64、value ≤256、无重复键和 UTF-8 约束；`request_phase` 缺失保持 `nullopt`，不得自动补成未提交。

`Request::Traffic()` 是 `traffic_class` 的唯一来源；`Request::Reply()` 是 `reply_mode` 的唯一来源。`SendOptions` 只影响 deadline 和是否等待 `SendAccepted`，并映射为 `acceptance_requested` 的 presence/value。`AppObjectAddress`、ActorId、mailbox、业务 operation/idempotency key 不进入 infra wire 的解释规则。

### 5.2 TCP 分帧和资源上限

TCP 每一条 frame 使用固定 4 字节无符号大端长度前缀：

```text
u32be payload_length | payload[payload_length]
```

- `payload_length == 0` 立即拒绝并关闭当前 link。
- 建立 session 前同样先读取 4 字节前缀；Hello/HelloAck 的 payload 长度不得超过 `min(max_frame_bytes, 64 KiB)`。检查长度后才允许分配 body，防止握手阶段用大长度耗尽资源。
- 数据 frame 的默认 `max_frame_bytes` 为 `8 MiB`，必须为有限正值且不超过 `kNetworkLimitsMaxBodyBytes` 的 `1 GiB` 硬上限。前缀、body、解码临时内存和每 lane 字节额度都计入对应 budget。
- 前缀和 body 可以任意分片或合并到达；reader 必须读满一帧后再解码，不能把一次 `recv` 当作一帧。
- 一次 frame 的完整字节必须在同一 link 的 writer 中连续写出。若前缀或 body 部分写出后发生错误、取消、deadline 或 Close，该 link 立即退休，未完整 frame 不得拼接到后续复用 link，后续 frame 不得继续写入残帧连接。
- 完整交给底层 socket 只把本地请求阶段推进到 `RequestCommitted`；不证明对端收到、接纳或执行。

当前 `proto/bbt/infra/rpc/v1/rpc_envelope.proto` 没有这些 v2 字段，现役 `TcpRpcTransport` 也没有 multiplex、双向 reader 或跨请求复用；v2 wire 仍是实现后的 evidence gap。

### 5.3 关联、顺序和断线

- `Call` Response 按 `request_id + service + method + request_schema + response_schema` 关联；TCP 到达顺序不参与匹配。
- 同一发送方、同一 opaque `ordering_key` 的消息必须按 `sequence` 递增进入该 session 的发送队列，并按接收端单 reader 的 frame 顺序交给 framework admission。公平调度可以延迟整个序列，不能先交付后序列。
- 不同 ordering key、不同发送方或不同目标之间不承诺全局处理顺序或因果顺序；同一发送方到同一目标的保证不跨新 epoch、进程重启、路由迁移或持久化边界延伸。
- 旧 epoch frame 和旧 link callback 只能丢弃或结算既有未知终态，不能重新进入新 session 的 admission，也不能改写已发布的 Call/Send 结果。
- 已进入 framework mailbox 的消息不因 transport epoch fencing 自动回滚；framework 负责其已接纳消息的业务收口和缺口记录。

### 5.4 deadline、RequestPhase 和失败阶段

跨进程只传剩余预算；接收端使用 `min(received budget, local handler budget)`，并在 handler 返回和回复写出前检查过期。deadline 只约束本次等待和接纳责任，不撤销已进入 session 的 frame。

| 场景 | `Call` | `Send(false)` | `Send(true)` |
|---|---|---|---|
| 参数/profile/metadata/frame 校验失败 | `InvalidArgument`/`ProtocolError`，未发起 | 同左，未本地接纳 | 同左，未本地接纳 |
| 本地 lane/session 容量满 | `Overloaded`，未提交 | `Overloaded`，未提交 | `Overloaded`，未提交 |
| 未完整写出前断线、取消、Close | 确定 `TransportError`/`TimedOut`/`Cancelled`/`Closed` | 已本地接纳但未完整提交的 frame 丢弃；不补发 | 已本地接纳但未完整提交的 frame 失败；不补发 |
| frame 完整写出但没有可信回复/确认 | `OutcomeUnknown`，`RequestCommitted` | 本地调用仍是成功的本地接纳事实 | `OutcomeUnknown`，`RequestCommitted` |
| 可信的远端错误 | 原样保留 code/domain/details/request phase | 只在等待远端确认时可观察为远端错误 | 原样返回远端错误 |
| mailbox 满 | framework 将 `Overloaded` 作为 Call 的远端结果 | 只计 dead-letter/drop | `Overloaded` 的 SendAccepted 错误 |
| 旧 epoch 或晚到 callback | 丢弃，不改终态 | 丢弃并计 session drop | 丢弃；等待保持既有未知/关闭终态 |

`TimedOut`、`TransportError` 和 `Closed` 本身不能证明远端业务没有执行；只有未提交阶段是确定未发起。`OutcomeUnknown` 不触发 infra 自动重试。是否使用同一业务 idempotency key 查询或恢复归 framework/应用。

## 6. AppSession / AppLink 内部责任和资源收口

`AppSessionState` 内部负责：

- 保存已验证 peer identity、PeerKey、当前 epoch、握手状态和逻辑关闭状态；
- 维护单 reader、Call correlation、有限 lane queue、Send acceptance waiter 和统计；
- 处理同时拨号选举、旧 link retirement、旧 epoch fencing 和新 epoch 发布；
- 不解释 Service/Actor/ActorId，不拥有 framework mailbox，不执行 Resolver 或授权。

`AppLink` 内部只负责可靠字节流、长度前缀、读写推进、连接关闭和物理回收。它不得暴露业务回调槽、socket/fd、buffer、`net::Connection` 或连接指针，也不得让 framework 持有。

默认一个 PeerKey 一个双向 TCP AppLink。只有 `ClientOptions::SetControlDataSplit(true)` 才允许同一 AppSession 使用 `ControlLink`（System + Service）和 `DataLink`（Actor）；两条 link 共享 identity、epoch、生命周期和会话统计。不得按 Service、Actor、user key 或每个请求创建物理连接。

infra owner 的关闭责任是：

1. Client/Server 先封各自逻辑入口并拒绝新 admission；
2. Server 按 StopAccepting/Drain/Close deadline 继续持有 handler、queue、callback 和物理回收责任；
3. session owner 退休旧 link，结算所有已接纳 frame，不自动重放；
4. 物理 `Released`/callback 全部完成后才释放额度和内部 state；
5. 共享 Engine 的 Start/Run/Close 由 App/宿主 owner 负责，RPC Client/Server 不关闭共享 Engine。

任何晚到 callback 只能捕获堆上的 session/operation state，不捕获 framework 裸指针或调用者栈；一条逻辑调用只发布一次终态。try/catch 只能隔离异常，不能替代借用/inflight 生命周期、数据竞争或未定义行为的修复。

## 7. C1–C5 逐项回应和验收边界

### C1：恢复稳定接口形态

已在目标契约中明确：

- Server adapter 使用 `AdmissionHandler(result<void>(AdmissionItem&&))`，业务回复经 `ReplyCompletion` 异步完成，入站上下文由 `Request::Context()` 读取；删除独立 `IncomingCallContext` handler 参数。
- Server 使用 `Start()`、`StopAccepting()`、`Drain(deadline)`、`Close(deadline)`、`IsClosed()`；`Close(deadline)` 超时不强杀业务任务、不释放 owner，后续继续驱动和观察 `Stats()`。
- `Server::LocalAddress()` 返回 `result<net::Address>`，只在 Start 后有意义。
- `Client::Stats()` 与 `Server::Stats()` 的值类型、lane 字段和 queued/dequeued/inflight/finished 计数已写明；Monitor 只能消费这些 infra 事实。
- `Request`/`Response` 都有构造、查询和 setter，业务可以生成 payload/metadata/关联字段；framework 类型不出现在签名中。

当前代码仍有旧 `RpcEnvelope`/`IncomingCallContext`/`Close()+WaitStopped()` 形态，属于基线事实，不是 v2 正确性证据。实现迁移必须按本节目标一次替换，并为新 API 建 include/API gate。

### C2：双端共用一条真实会话

已明确最小装配：每个 App 一个 `Client` owner，Server 创建时显式注入同一个 `std::shared_ptr<Client>`；两端的入站和出站 session state 因此进入同一 owner。已明确 local identity 的来源、Hello/HelloAck 的实际握手字段、endpoint 到 peer identity 的首次绑定、认证前禁止替换、同时拨号的保留规则、epoch owner、Call/Send 双向复用和 `Control/Data` 显式拆分边界。

当前没有 AppSession、shared session injection、handshake、multiplex 或真实双端 v2 实现；必须由实现和双进程测试证明，不能由 ADR 文本替代。

### C3：Send 接纳和处理分离

已明确本地 bounded admission、远端 `SendAccepted`、framework mailbox admission 和业务 handler processing 的时间点。单 reader 只按 frame 顺序进入 `AdmissionQueue`；`AdmissionHandler` 成功表示 owning item 已转入上层 mailbox，不表示业务 handler 完成；Call 通过 `ReplyCompletion` 和 `AdmissionLease` 收口，Send 只收口投递责任；异常有 `InternalError`/远端拒绝兜底；infra 不创建 Actor mailbox、不解析业务 target。Stats 中的 queued/dequeued/inflight/finished 和 Drain 归属已固定。

当前没有 `Send`、SendAccepted、mailbox admission 或 lane 统计实现，不能报告该行为已通过。

### C4：正式 wire 和失败边界

已给出 v2 profile、protobuf 字段号、frame kind、traffic/reply 单一真源、Hello/HelloAck、epoch、opaque ordering key、remaining budget、metadata/error 限制、u32be 分帧、握手前长度上限、部分写退役和旧 epoch fencing。未承诺 exactly-once、跨重启保序、身份自报即认证或透明自动重试；Call/Send 的本地接纳、完整写出、远端接纳和业务处理阶段已区分。

当前 v1 proto 和一次请求/回复 TCP adapter 不能作为 v2 wire 或可靠性证据；需要独立 round-trip、恶意 frame、部分写、旧 epoch 和断线测试。

### C5：完整 infra 范围

本 ADR 不把 infra 重构缩成 RPC。第 8 节逐项列出 net、HTTP、RPC、Redis、Mongo、Payload 边界、引用图/API compat 和 Doxygen 职责卡的 requirement、target/header/API、owner 与 evidence。未落实现的模块保持明确的 owner task 和 acceptance evidence，不把缺口交给实现者自由发挥。

## 8. 完整 infra requirement / owner / evidence 映射

下表是本次 infra 范围的交接台账。`当前证据` 只说明当前树里能观察到什么；`验收证据` 是实现后必须产生的可运行或静态门禁，不因已有 ADR、旧测试或文档文字自动通过。

| requirement | target header / API | owner task | 当前证据 | 验收 evidence |
|---|---|---|---|---|
| 公共面、依赖方向和引用图 | `include/bbt/infra/{Result,CallOptions}.hpp`；`bbt/infra/net`、`http`、`rpc`、`redis`、`mongo` 稳定头；公开头不得引用 framework/third-party 类型 | infra owner：按模块拆稳定头；framework owner：只消费公开头 | 三仓 `AGENTS.md`、`docs/conventions.md`；现有公开头仍有旧聚合/兼容入口 | 每个公开头独立 include/selfcheck；include-layer 负向门禁证明 `framework → infra → coroutine`，禁止 framework 类型向 infra 泄漏；失败必须阻断构建 |
| net Engine 生命周期和错误 | `bbt/infra/net/Engine.hpp`、`Options.hpp`、`Address.hpp`；目标包含 `Create(EngineOptions)`、`Start()`、`Run()`、`Close()`、`LastError()`、`Stats()`，并保留共享 Engine 不被 adapter 关闭 | infra net owner | 当前 `Engine.hpp` 观察到 `Run(config)`、`RequestClose()`、`Phase()`、`LastError()`、`LocalAddresses()`；当前 Stats 尚未收敛为目标值型入口 | Engine Start 自带线程、Run 占宿主线程、Close/Run 收口和错误阶段测试；`Stats()` 字段、LocalAddress 动态端口、重复 Close、绑定失败与无 runtime 进展测试；与 coroutine pin 的独立构建证据 |
| HTTP Request/Response 值面 | 目标 `bbt/infra/http/{Request,Response}.hpp`；Request 的 `Method/Path/Header/Body/PathParam/Query/Json/Context/SetHeader/SetBody`；Response 的 `Text/Json/Stream/StatusCode/Body/Ok` | infra HTTP owner | 当前新 HTTP `Client.hpp`/`Server.hpp` 有 `IncomingRequest`、`Response`、stream `ResponseBody` 和基础 Options；旧 `HttpRequest/HttpResponse` 仍在 `include/bbt/infra` | 参数路由类型转换、缺失/类型错误、header 大小/重复规则、Request context、buffered/stream response、客户端取消后不再拉取 chunk 的测试 |
| Router、middleware、流式取消 | 目标 `bbt/infra/http/Router.hpp`；`Get/Post/Put/Delete/Route/Use/Include/Mount`；有序 middleware chain、`Freeze()`、`RouteRegistrationToken` 的 `Revoke()`/状态查询 | infra HTTP owner；framework HttpComponent 只持有 token | 当前仓中未观察到稳定 Router 公共头；计划明确 `Router Start` 后不可修改，F15 又要求 Monitor 动态 token，存在冲突 | 采用单一规则：Server Start/Router Freeze 前完成结构注册；运行后 token 只撤销既有 dispatch，不修改 route topology；in-flight handler 按 token lease 排空后才释放；动态 Install/Uninstall 在已 Freeze 时返回 `Busy` 或使用预注册 dispatch route；冲突测试覆盖 token 失效、并发请求和路由快照 |
| HTTP Client/Server 生命周期、地址和统计 | 目标 `http::Client::Create(engine, ClientOptions)`、`Send/Get/Post`、`Stats()`、`Close()`；`http::Server::Create(engine, Router, ServerOptions)`、`Start/StopAccepting/Drain/Close(deadline)/LocalAddress/Stats` | infra HTTP owner | 当前 `http::Client.hpp` 有连接池 `PoolStats` 和 `Close()`；当前 `http::Server.hpp` 有 `ServerStats`、`LocalAddress` 尚未提供，仍有 `WaitStopped()` | `http.net.*` 构建/CTest、dynamic port、参数路由、中间件异常转错误、stream cancel、StopAccepting/Drain/Close deadline、Stats/LocalAddress 和物理回收证据；不得以 HTTP 现有通过替代 v2 RPC 双端证据 |
| RPC Request/Response/Client/Server/Codec | `bbt/infra/rpc/{Request,Response,Client,Server,Codec}.hpp`；本 ADR §2 的完整签名；`AdmissionHandler(result<void>(AdmissionItem&&))`、`ReplyCompletion`、`AdmissionLease`；Server 注入 `shared_ptr<Client>` | infra RPC owner；framework AppNetwork adapter | 当前 `Client.hpp`/`Server.hpp`/`Transport.hpp` 仍使用 `RpcEnvelope`、`RpcAddress`、独立 context 和旧 transport seam | v2 public-surface compile；Codec 字段/unknown enum/metadata/error round-trip；真实双进程 TCP、Call/Response 并发关联、Send/SendAccepted、epoch fencing、overload isolation、lifecycle；HTTP profile 的 Send 明确拒绝 |
| Redis pool、Acquire 和握手 | 目标 `bbt/infra/redis/Client.hpp`；`Client::Create(engine, Address, ClientOptions)`、`Connect`、公开 RAII `Acquire(CallOptions)`、命令方法、`Close/Stats`；`RedisHandshake{username,password,database}`，AUTH 后 SELECT，禁止自动重连/重发 | infra Redis owner | 当前 `bbt/infra/redis/CoRedisCli.hpp`、`CoRedisPool.hpp` 已有连接级 `RedisHandshake`、固定池、私有 `Acquire`、显式 Connect、Borrow RAII 和有界 waiters | pool size/max_waiters、Acquire 超时/取消/RAII 归还、坏连接 Retire、AUTH/SELECT 顺序和密码不入错误、显式 Connect 修复、真实 Redis live 测试；公共面不泄漏 hiredis |
| Mongo Database/Collection/Document 和 Options | 目标 `bbt/infra/mongo/{Database,Collection,Document,Options}.hpp`；`Database::Create/Start/Close`、`Collection(name)`、CRUD/Find options、`Document::FromJson/ToJson/Get*`、`Options::Validate` | infra Mongo owner | 当前 `include/bbt/infra/mongo/Client.hpp` 已有 `CoMongoDb/CoMongoColl/MongoRuntimeConfig`、有界 worker/queue、Collection；`mongo/Bson.hpp` 已有 owning Document；旧 `CoMongoCli.hpp` 仍保留兼容面 | driver worker/queue/timeout、Collection 句柄依赖 owner、BSON 限制、关闭与 in-flight lease、Redis/Mongo live 分离 job；第三方类型和线程不进入公共头 |
| Payload 归属和 codec 边界 | target 为 `bbt::framework::Payload` 及 framework 方法表/codec；infra 不再把业务 Payload 放进 RPC 稳定面；`Request::Payload()` 只承载 bytes | framework owner 实现 Payload；infra owner 只保留 bytes 和 transport schema | 当前 `include/bbt/infra/rpc/RpcPayload.hpp` 仍存在，且注释说明它是迁移期通用 codec；framework 当前有 `framework/include/bbt/framework/CoRpc.hpp` | framework 迁移后 infra include/API 负向门禁拒绝业务 Payload；位置参数/proto 编解码由 framework 测试覆盖；RPC wire 只验证 bytes/schema，不 include framework proto |
| API compatibility 和引用图 | `tests/api_compat/`、公开头 selfcheck、include-layer checker；不保留 `RpcAddress/RpcEnvelope/IRpcTransport` 稳定入口 | infra + framework owners 联合 | plan 要求 `tests/api_compat`；当前基线仍有旧头/旧 seam，v2 尚未切换 | 真实 framework consumer 原样编译；旧符号负向编译失败；target/link/compile_commands 与注册集一致；不把 stale cache、未注册测试或编译成功单独当成行为证据 |
| Doxygen 职责卡 | 每个目标公开类声明处有 `@brief`、`@param`、`@return`、`@pre`、`@par 职责/不负责/约束`；索引放 `docs/guides/framework-surface.md` | 各仓模块 owner；不由 ADR 代替头文件注释 | 三仓 `docs/conventions.md` 和计划第 2.1/8 节已要求职责卡；当前 v2 目标头尚未存在 | Doxygen 生成无新增 warning，公开符号/参数/生命周期/所有权均可导航；职责卡与本 ADR、实际实现和 API compat 一致；未生成的页面不宣称已通过 |

HTTP Router 的动态冲突采用上表唯一规则：**路由拓扑在 Server Start 前冻结；运行后 token 只撤销已有注册的 dispatch lease，不新增或改变 Router match tree。** 因此 MonitorService 如果在 Server 已启动后才绑定 `HttpComponent`，只能使用预注册的静态 monitor dispatch route，绑定/解绑改变该 dispatch 的 handler snapshot；不能调用 Router 的结构注册 API。若业务需要新的 path，必须在 Server Start 前完成 Install/registration，或按控制面策略停服、重建 Router、再启动。token 的 owner 是注册方，token revoke 先阻止新 dispatch，再等待已有 dispatch lease 结束，最后释放 handler；不把 token 析构等同于立即杀掉在途请求。

## 9. framework 交接边界

framework 负责：

- `AppObjectAddress`、Service/Actor 方法和 message 到 Request fields/payload 的映射；
- `Service`/`Actor` traffic class 的选择，opaque `ordering_key`/sequence 的生成和业务缺口处理；
- Resolver、授权、可信 transport principal 到业务身份的绑定；
- mailbox 每实例/总额度、dead-letter 计数、业务 handler 和动态对象生命周期；
- `CoCall`/`CoTell` 的重试策略（默认关闭）、operation/idempotency key 和 OutcomeUnknown 恢复；
- `AppNetwork` 对 Client/Server/Engine 的共享持有、Stop 顺序和 MonitorService 的显式安装；
- `AppInspector`/MonitorService 对 Client/Server Stats 的汇总，以及 `bbt.app.monitor/v1` 的 typed snapshot 和 JSON schema。Monitor HTTP/RPC route 必须 opt-in、受访问控制、无 payload/secret、有限资源，并遵守 Router freeze/token 规则。

infra 不负责：

- 创建 Actor mailbox、执行 Service/Actor handler、解释 ActorId 或提供集群唯一性；
- Resolver、路由治理、业务授权、业务 payload codec、自动重试、exactly-once、跨重启顺序或持久化 mailbox；
- 自动安装 MonitorService、自动添加公共 monitor route、关闭 framework App 对象；
- 把 Engine、socket、fd、buffer 或第三方 driver 线程暴露给 framework。

## 10. 删除项和迁移规则

在 v2 迁移窗口完成下列一次性替换；新旧稳定入口不得长期并存：

| 旧入口/概念 | v2 处理 |
|---|---|
| `RpcAddress` / `Endpoint` | `Client::Call/Send(std::string_view target, ...)`；target 只负责 endpoint/profile，peer identity 在握手后映射 |
| `RpcEnvelope` | `rpc::Request` / `rpc::Response`；不保留别名 |
| `IncomingCallContext` handler 参数 | 入站 `Request::Context()`；不再增加第二个 context 入口 |
| `IRpcTransport` / `IRpcInboundTransport`、旧 HTTP/TCP transport seam | 移入 `rpc/detail`；Client/Server 是稳定入口 |
| `WireRequest` / `WireReply` / `RpcWire*` 自由函数 | 收进 `Codec`/detail；不进入 framework include |
| `AppSession` / `AppLink` / mux public symbols | 仅保留 detail state 和 framework 的受控 Stats 视图 |
| `LegacyHeaders`、`x-bbt-*` | 删除；不以 header 或 metadata 同名键回退 v2 字段 |
| 一连接一请求一回复 TCP stable adapter | 替换为 session multiplex；旧 adapter 不能作为 v2 复用证据 |
| `WaitStopped()` | 由 `Server::Close(deadline)` 收敛；超时保留 owner 和 Stats 观察责任 |
| `RpcPayload` infra 公共业务 codec | 迁移到 framework `Payload`；infra 只保留 bytes/schema 传输边界 |

迁移顺序固定为：先落稳定头、v2 Codec/wire 和 include/API 负向门禁；再切换 framework 的 AppNetwork；先通过 infra 真实双端 session，再通过 framework 双进程 Service/Actor。任何未实现、依赖不足、跳过或 stale cache 都标记为 evidence gap，不转写为通过。

## 11. 目标验证形态和当前未验证项

实现后至少需要以下可判定场景，并确保 CTest 注册集与实际运行集一致、无隐式跳过：

- `rpc.app_session.one_link`：两个 App、一个 PeerKey、多个 Service/Actor，只建立一条默认双向 link；
- `rpc.app_session.mixed_call_send`：混合 Call/Send，RequestId/MessageId 不串线，远端接纳不等业务完成；
- `rpc.app_session.ordering`：同 opaque ordering key 的 sequence 保序，不同 key 可并发；
- `rpc.app_session.overload_isolation`：一个 mailbox 满时 System/Service/其他 Actor 仍进展，Overloaded/dead-letter 可观察；
- `rpc.app_session.disconnect_epoch`：部分写、断线、旧 link 退休、新 epoch 握手、旧 frame 丢弃、不自动重放；
- `rpc.app_session.lifecycle`：StopAccepting、Drain、Close(deadline)、Engine 收口、Stats 和物理释放责任可观察；
- `rpc.app_session.public_surface`：稳定头消费成功，旧 RpcAddress/RpcEnvelope/transport seam/AppLink/socket 泄漏的负向用例失败；
- `rpc.app_session.wire`：字段号、profile、handshake 资源上限、metadata、budget、unknown enum、关联、错误阶段和未知字段矩阵；
- HTTP/Redis/Mongo 直接耦合测试：分别覆盖 Router token/freeze、stream cancel、pool Acquire/handshake、Mongo owner/Collection/Document/Options；
- framework 独立 gate：四种消息方向、显式 MonitorService、Component binding、observer invalidation 和 StopIncomplete。

本修复节点只做文档契约，不运行 v2 build/test，也不把 baseline smoke/聚焦结果当作 v2 证据。baseline 已记录：framework `d55cdb5ef392facd74f6929360affa141f9623aa`、infra `8573b7a831eeab75f7d787d1709ee330f20f8b63`、coroutine `45c575b37ee6b38cfada86cc3d0339cf85c54fe3`；当前 v2 session handshake、Send、mailbox admission、wire round-trip、真实双端和 framework consumer 仍未验证。

## 12. 本节点结论

本 ADR 已将 C1–C5 合并为一套目标契约：

- C1 的 `Request&`、`Context`、Response 构造/setter、`Close(deadline)`、`LocalAddress` 和 Client/Server Stats 已有完整签名与生命周期语义；
- C2 的实际身份来源、Client-owned shared session injection、endpoint 首次映射、同时拨号选举、epoch 协商和双向复用已明确；
- C3 的 Send 本地/远端接纳与业务处理阶段、单 reader、异常兜底、bounded admission 和 Drain 计数已明确；
- C4 的字段号、分帧、握手前资源上限、traffic 单一真源、opaque ordering、epoch fencing、部分写退役和错误阶段已明确；
- C5 的完整 infra 范围、owner、target header/API 和 evidence map 已补回，Router dynamic registration 冲突已用 freeze + token revocation 规则收敛。

这些是经父会话验收前的目标契约草案；实现、构建、测试和独立验证仍须按第 8、11 节产生真实证据。
