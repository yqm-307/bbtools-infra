# 通信层契约 v1：协程原生 HTTP、RPC 与资源寿命

## 状态与权威

契约 ID：`co-network/v1`。日期：2026-09-17。本文冻结跨仓公共形态与行为，是目标而非已实现功能；本轮仅设计与 Issue，不实现代码、不 commit/push、不安装依赖或部署。源码基线：`38c3990c6a7e2b68275629522a9dbf3039cbc8d3`，只有初始 README；主工作区的初始化 AGENTS/决策尚未发布。

本仓拥有网络协议、transport、client/server、第三方 adapter、依赖与构建消费契约。框架侧 Service/Actor/路由策略不下沉。上游 coroutine 的 `service-runtime/v1` 由其 `agent-docs/2026-09-17-service-runtime-contract-v1.md`/关联 Issue 拥有，本仓只按固定 SHA 消费，不另造等待状态机。

本文发布前，实施 Issue 的完整 v1 快照为可访问依据；合入后由 Issue 指向本文已发布版本。公共签名/语义变化需记录影响、协调下游并获用户确认；后端实现选择不得改变接口承诺。关联主 PRD：[#1](https://github.com/yqm-307/bbtools-infra/issues/1)。首个真实切片为 HTTP；RPC 是其后的独立切片，MCP 不在本任务实现范围。

### 后续目标规格

[0005：CoTCP/CoUDP 与第三方 Binding](0005-co-io-adapter-contract-v1.md) 补充目标 I/O 执行边界：proc 执行实际收发和协议，sche 只检测等待条件并唤醒。本文的公共错误、逻辑终态、关闭和资源寿命契约继续有效；末尾 Asio/Beast 实现附记描述旧实现，不代表新模型已完成迁移。0005 已通过独立只读规格终审，但仍是未实现、未完成兼容验收的目标规格。

## 概念与分层

- Server 是入站通信组件，不是物理服务器，也不等于一个 Service。允许一个 listener 分发多个逻辑服务。
- Client 是出站通信组件，可以面向多个 endpoint 并复用连接；不要求每个业务服务拥有专属客户端。
- ICoNetwork 是网络相关受管对象的公共身份边界，不是包含所有 read/write/accept 的万能接口。
- 协程原生指调用外观为普通函数，等待时挂起当前有栈协程；不使用 C++20 co_await，不向调用者暴露 Asio/thread/socket。
- 接入外部库必须使用其合法执行域；同一 I/O 对象的发起、取消、关闭、最终释放遵守同一同步规则。仅把完成 handler 绑到 executor 不等于所有操作已串行。

## N0：公共值类型、对象与关闭契约

公开命名空间 `bbt::infra`，公开头目标 `include/bbt/infra/`。结果表面冻结为：

```cpp
template<class T> class result;  // 支持 move-only T；另有 void 特化
// 静态工厂：result<T>::ok(T)、result<T>::err(Error)
// void 工厂：result<void>::ok()、result<void>::err(Error)
// 查询：explicit operator bool() const noexcept;
// 访问：value() 的 &、const&、&& 重载；error() 的 &、const& 重载。
// 访问错误分支抛 std::logic_error；预期网络失败不靠异常报告。

enum class ErrorCode {
    InvalidArgument, InvalidContext, RuntimeUnavailable, Closed,
    Cancelled, TimedOut, Overloaded, Unavailable, TransportError,
    ProtocolError, NotFound, TypeMismatch, UnsupportedRoute,
    OutcomeUnknown, RemoteError, InternalError
};
struct Error {
    ErrorCode code;
    std::string domain;          // 稳定错误域，网络默认 "infra"
    std::string domain_code;     // 稳定扩展错误码，不承担路由
    std::string message;
    std::string backend_category;
    int backend_code;
    std::uint64_t transferred_bytes;
    std::vector<std::pair<std::string, std::string>> details;
};

class ICoNetwork : public bbt::coroutine::ICoObject {
public:
    ~ICoNetwork() override = default;
};

enum class CloseStatus {
    Closed, TimedOut, Cancelled, InvalidContext,
    AlreadyWaiting, RuntimeUnavailable
};
class ICoCloseable {
public:
    virtual ~ICoCloseable() = default;
    virtual void RequestClose() noexcept = 0;
    virtual bool IsClosed() const noexcept = 0;
    virtual CloseStatus WaitClosed(
        bbt::coroutine::Deadline deadline,
        bbt::coroutine::CancellationToken cancel) = 0;
};
```

result 是本仓网络结果表面，优先复用 core 的 `Result<T,E>`，必要时最小适配 void/访问语义，不为此改变 core 全仓错误协议。本命名例外是用户指定的业务体验，旧库 PascalCase 接口不统一重命名。不得把 std::type_info、编译器函数名或进程内哈希作为远端类型 ID。

ICoObject 只提供身份，关闭能力独立；ICoNetwork 不承诺任意派生类都可监听或按字节读取。第一版不新增全局 CoHandle/CoScope/动态插件系统；继承本身不提供安全沙箱或任意对象生命周期接管。

Error.details 是错误分支的结构化详情，RPC 错误信封必须无损携带 code/domain/domain_code/details，不以成功 envelope 冒充错误。详情最多 16 项，键最多 64 个 UTF-8 字节，值最多 256 个 UTF-8 字节；重复键、非法编码拒绝为 ProtocolError，不能截断后继续。域专属保留键的归属与格式校验不由 infra 通用层承担，改由该域拥有者在错误构造/消费边界落实：`framework.actor` 域保留 `expected_sequence`（无符号十进制字符串）供顺序错误使用，其域归属与格式由 bbt-framework 侧校验（infra #39）；其他上层域声明自身保留键时同样自行校验，infra 不登记跨域键表。客户端不得写入响应错误详情。公开错误文本脱敏，不携带堆栈、凭据或原始请求。

### 关闭规则

Open → Closing → Closed，不重开。RequestClose 可从普通线程调用，幂等，不等待 I/O/业务处理完成；允许内部短临界区。线性化之后的新提交必须拒绝，正在竞态的提交要么被接纳并纳入清理，要么明确拒绝，不能遗漏。

WaitClosed 只在协程里等待；每个对象最多一个并发等待者，第二个明确返回 AlreadyWaiting，不影响第一个。映射固定：WaitStatus::Completed → Closed；TimedOut/Cancelled/InvalidContext/AlreadyWaiting/RuntimeUnavailable 一一映射为同名 CloseStatus。先校验上下文和代际，已关闭对象在合法上下文立即返回 Closed；普通线程可用 IsClosed 查询。超时/取消不撤销关闭、不表示资源已释放。Closed 表示后端不会再访问该组件拥有的操作资源；独立业务调用者仍须结束并释放自身引用，不能据此推断所有调用方栈已消失。

析构不能在任意线程隐式阻塞等待。组件由 Runtime/宿主持有到真正清理完成；所有晚到回调指向栈外 operation state，不能借用调用者栈或裸 Service。资源保活不靠协程栈上的 shared_ptr 析构。

## N1：HTTP 第一真实切片

目标公共表面（声明形态，非已有实现）：

```cpp
struct CallOptions {
    bbt::coroutine::Deadline deadline;
    bbt::coroutine::CancellationToken cancel;
};
struct HttpRequest {
    std::string method;
    std::string url;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;
};
struct HttpResponse {
    unsigned status;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;
};

// HttpClient : ICoNetwork, ICoCloseable
result<HttpResponse> Request(HttpRequest request, const CallOptions& options);
// HttpServer : ICoNetwork, ICoCloseable
struct IncomingCallContext {
    bbt::coroutine::Deadline deadline;
    bbt::coroutine::CancellationToken cancel;
    std::string peer_principal;
};
using HttpHandler = std::function<result<HttpResponse>(
    IncomingCallContext, HttpRequest)>;
// Runtime 负责安装 handler、启动和拥有 server/client；工厂形态见 N0。
```

本切片至少支持 HTTP/1.1 请求/响应与真实 client/server；消息体采用有上限的完整缓冲，不默认为无限制。首版不提供流式 body、WebSocket、HTTP/2、多后端切换，后续不能把完整缓冲 API 当作流式协议已经完成。headers 保留重复值，不用 map 静默丢重复头。

正常的 4xx/5xx 是有效 HttpResponse，不是网络异常。非法 framing/超限/断连为 Error；不足一条消息不能被当作成功响应。URL、头名/值、长度和体积边界由协议库与 adapter 明确校验，禁止 CRLF 注入。HTTPS 若不在首切片交付，必须显式拒绝且限定本地测试用途，不能降级明文；一旦支持，默认校验证书与主机名，TLS 依赖另行确认。

IncomingCallContext 是本次入站请求的拥有型上下文，infra 负责构造；取消 token 由在途请求状态持有，直到 handler 真正退出，不能响应超时后就释放。deadline 为 min(本地接纳时刻 + listener 有限预算, 经协议验证的远端剩余预算换算结果)，排队也消耗该期限。HTTP 首切片不信任任意自定义 deadline 头，只使用 listener 本地预算；RPC 协议 profile 必须定义有上限的 remaining-budget 字段及单位，未定义不得开始 N2。取消由本地期限、连接断开、owner close 或后端支持的协议取消触发；取消帧若协议不支持就明确不支持，不承诺远端取消必达。peer_principal 来自验证过的连接身份，未认证为空；不得从业务 metadata 直接复制认证身份。首闭环仅限隔离 loopback 测试，不能当作公网鉴权已完成。

首版 handler 在 infra 提供的协程执行环境调用；网络库 I/O 回调不直接运行可能挂起的业务 handler。接纳失败有可观察错误/响应，禁止无界创建协程。实际回调、线程、io_context 隐藏于实现，不要求依赖 Linux Hook 才正确。

### 逻辑结果与物理清理分离

底层 operation 由 runtime 的在途集合和后端回调持有自己的请求/响应/缓冲。逻辑结果与后端清理进度分别记录。

完成、取消、deadline、owner close 竞争时，首次成功发布的逻辑终态不可覆盖。超时或取消可以使调用方及时返回，但底层清理可继续，必须保留所有仍被后端访问的资源，直到完成确认；WaitClosed 负责等待物理清理。保留后端错误和已知部分传输信息，不把未完整的响应变成成功。

这明确替代早期讨论中“所有 Request 超时也必须等到物理清理才返回”的示意承诺，避免不可取消 DNS 等操作无限拖住业务截止时间。取消不等于业务回滚；请求可能已到达远端时不能声称“未执行”。

协程强制 Stop 后不保证函数返回，但 operation 清理不得依赖被销毁的栈。Runtime 必须由宿主强持有；正常顺序为 Scheduler::Start → NetworkRuntime::Create/Start → 使用 → server.StopAccepting → 等待 handler 结束 → Runtime.RequestClose/WaitClosed → 释放已关闭网络对象 → Scheduler::Stop。不默认让全局静态析构承担关闭。若任何等待超时，宿主必须继续强持有 Runtime/操作并保持驱动可用；不得按正常栈退出析构它们，超时处理由 framework 的 run 契约收口。

## N2：RPC 编解码与定址传输，不含服务治理

RPC 目标接口如下；具体协议库与 wire 格式在 N2 开始前通过候选评估/协议记录锁定，不用自行拼装不完整 RPC 或安全协议凑验收。

```cpp
struct RpcAddress { std::string transport; std::string endpoint; };
struct RpcEnvelope {
    std::string service;
    std::string method;
    std::string request_id;
    std::string request_schema;
    std::string response_schema;
    std::vector<std::uint8_t> payload;
    std::vector<std::pair<std::string, std::string>> metadata;
};
// 客户端：具体地址已由框架解析，infra 不读取服务发现。
result<RpcEnvelope> Call(
    const RpcAddress& address, RpcEnvelope request,
    const CallOptions& options);
```

Client/Server 分开实现，由网络 Runtime 组合管理；一个 client 可面向多个 endpoint，一个 server 可分发多个 service/method。请求关联使用 request_id，不能把连接上的 FIFO 顺序误作 RPC 响应匹配。未知 service/method/schema 返回明确错误，不调用错误类型的 handler。

Call 是请求—响应；不接收返回值不会变成单向发送。第一版不提供隐式 fire-and-forget、不自动重试可能有副作用的 RPC、不自动广播。发出后失联时能够表达 OutcomeUnknown；调用方超时不证明远端停止，后端能证明未发送时可返回确定失败。

RPC 元数据分命名空间：`fw.*` 为框架保留字段（`fw.producer_id`、`fw.producer_epoch`、`fw.sequence`、`fw.receiver_epoch`）；`route.*` 是经过 RouteFields 校验后编码的用户路由字段；`trace.*` 为已注册追踪字段。request_id 只用 envelope 顶层字段，不允许 metadata 同名替代。每个 envelope 最多 32 个 metadata 项，键最多 64 个 UTF-8 字节，值最多 256 个 UTF-8 字节，累计同时受 max_header_bytes 限制。重复键、未知前缀/保留键、非法数字或编码均拒绝；没有覆盖优先级或静默忽略。RouteFields 不接受任何带 `fw.`/`trace.`/`route.` 前缀的键，转换器统一添加 `route.`。框架通过内部可信适配器写 fw.*，接收端仍验证 producer 与 peer_principal 的授权绑定，前缀本身不授予信任。schema/profile 必须显式包含这些系统字段。deadline 属协议控制字段而非用户 custom，跨进程传剩余预算，不序列化 steady_clock 时间点；IncomingCallContext 是 handler 获取其有效期限和取消的唯一入口。业务路由字段不能代替鉴权或授予任意 endpoint 访问权。

模板 codec 定制点固定为 `bbt::infra::Codec<T>` 主模板的显式特化，主模板不提供默认成功实现。业务在公开业务协议头提供特化，且必须在 binder/call 实例化前可见；不使用 ADL 或动态注册表。三个静态成员为 `SchemaId() -> std::string_view`、`Encode(const T&) -> result<std::vector<std::uint8_t>>`、`Decode(const std::vector<std::uint8_t>&) -> result<T>`；SchemaId 返回静态寿命、非空稳定 ID，必须显式版本化。`Codec<void>` 由 infra 提供，ID 为 `bbt.void/v1`，Encode 无参数生成空 payload，Decode 仅接受空 payload 并返回 result<void>。内建 std::string codec ID 为 `bbt.string.utf8/v1`，编码为原始 UTF-8 字节，非法 UTF-8 返回 ProtocolError。

框架 call 根据 Request/Reply 的 Codec 填 request_schema/response_schema，服务端 binder 在解码/调用之前与注册签名逐项比较，不匹配返回 TypeMismatch；匹配但字节非法返回 ProtocolError。响应沿用关联 service/method/request_id 与 schema，客户端在解码前复验；错误响应保留关联标识但不解码为 Reply。模板编译期检查 codec 成员签名，启动时检查非空 SchemaId 和同服务方法的唯一注册，不声称能静态证明不同进程的 codec 实现相同。

## N0 的依赖与后端选型门

优先标准库/Boost/已有能力。首个 HTTP 后端优先评估 Boost.Beast/Asio，但本文不代表已选定版本或已授权安装；正式实现先记录维护、安全、许可、平台、取消和执行域、体积、退出路径及锁定 SHA/版本。需要新依赖按仓库既有规则确认。

N0 的宿主装配接口同步冻结如下（类型均在 bbt::infra，定义顺序应使头自给）：

```cpp
struct NetworkLimits {
    std::size_t max_connections;
    std::size_t max_inflight;
    std::size_t max_header_bytes;
    std::size_t max_body_bytes;
    std::chrono::milliseconds incoming_timeout;
};
struct ListenAddress { std::string host; std::uint16_t port; };
using RpcHandler = std::function<result<RpcEnvelope>(
    IncomingCallContext, RpcEnvelope)>;
// NetworkRuntime / HttpClient / HttpServer / RpcClient / RpcServer
// 均实现 ICoNetwork 与 ICoCloseable；Runtime 是资源拥有者。
// NetworkRuntime 成员：
static result<std::shared_ptr<NetworkRuntime>> Create(NetworkLimits limits);
result<void> Start();
result<std::shared_ptr<HttpClient>> CreateHttpClient();
result<std::shared_ptr<HttpServer>> ListenHttp(ListenAddress address,
                                              HttpHandler handler);
result<std::shared_ptr<RpcClient>> CreateRpcClient();
result<std::shared_ptr<RpcServer>> ListenRpc(ListenAddress address,
                                            RpcHandler handler);
// HttpServer / RpcServer 成员：
ListenAddress LocalAddress() const;
void StopAccepting() noexcept;
```

NetworkLimits 各项显式、大于零且校验上限，incoming_timeout 必须有限，header/body 限制也用于 RPC 元数据/payload。Server.StopAccepting 幂等且可从控制线程调用：停止新连接及既有连接上的新请求接纳，不取消已接纳 handler，不关闭其回复路径；不等价于 RequestClose。Create/Start/工厂在启动控制线程使用，不挂起协程；Start 要求 Scheduler 已启动且同一 runtime 只成功启动一次，关闭后不重开。工厂返回的 shared_ptr 是受托管对象引用，Runtime 的强所有权保证清理，不能互相强引用成环。监听绑定只接受调用者明确给出的地址，测试用动态端口；LocalAddress 返回实际绑定地址。Listen 成功后可以接纳，框架只有在方法/路由配置验证完成后才调用它。业务 handler 在已接纳的受管协程调用，异常由边界捕获并转换为 InternalError。

**HTTP 出站配额（Issue #37）**：`max_connections` 与 `max_inflight` 由 `NetworkRuntime`（资源 owner）统一持有，作用域是整个 Runtime——同一 runtime 下所有 `HttpClient` 共享同一预算，多 client 不各自独立计量。`Request` 在登记底层 operation、发起任何 `async_*` 之前原子预留名额；名额不足立即返回 `Overloaded`，此时不创建 socket/resolver、不向 io 域投递，也不引入排队。名额由堆上 operation 持有，在物理收口（`finished && in-flight==0`，后端不再访问该 op）时准确归还一次，覆盖正常完成、发起失败、deadline、cancel、RequestClose 与强制 Stop；不依赖挂起协程的栈析构，不把「调用者超时返回」当作 socket 已回收。本项只覆盖 HTTP 出站路径，与 #32 的 CoTCP/CoUDP 在途门禁（`m_transport_count` 计量真实 socket）是不同入口，互不替代。

RPC 服务分发不强迫 infra 依赖 Service：RpcHandler 由 framework 适配器提供，它收到 IncomingCallContext 与 owning envelope 后决定服务/Actor 入队和响应；网络执行域不会等待阻塞业务 handler。生产绑定、TLS、鉴权仍需对应配置/授权，不由这些工厂默认开放。

HTTP 模块与 RPC 模块按需链接；未构建 RPC 时不导出能假成功的 Rpc 工厂。后续新增可选工厂参数不得改变本版无线程/后端类型泄漏的形态。

RPC 后端和 wire profile 未锁定时，N2 状态为 BLOCKED，不妨碍 N1 HTTP 切片，也不允许以 mock/stub 宣称 RPC 完成。MCP transport/client/server 仅保留模块边界，不在本任务展开。

## 构建、目录与验证

目标目录：`include/bbt/infra/` 公共契约；`src/http/`、`src/rpc/` 模块实现；`src/adapters/` 后端隔离；`cmake/` 构建消费；`tests/` 的 unit/contract/integration；`examples/` 真实消费者；`docs/decisions/` 契约与选型。不建空模块树。

拟交付 CMake 目标：`bbt::infra_common`、`bbt::infra_http`，N2 就绪后增加 `bbt::infra_rpc`。RPC backend 不被强制依赖本仓 HTTP adapter；按真实消费声明无环依赖。framework 只使用公开头/目标，第三方类型默认不进入公共头；不继承 App/Service。

| 验收 ID | 必须证明 |
|---|---|
| N-01 | C++17 独立消费公共头与隔离构建/安装前缀，依赖来源固定，不污染全局库 |
| N-02 | HTTP 真实 loopback client/server，成功、4xx/5xx、坏消息、限长、断连、并发；非协程调用明确拒绝 |
| N-03 | 提前/重复/晚到完成、超时/取消/关闭竞争；逻辑结果唯一；物理清理完成后在途资源归零 |
| N-04 | Scheduler 正常关闭与强制 Stop 分开验证；无用户栈析构承诺，晚到 handler 不触及旧栈/协程 |
| N-05 | RPC 两个独立进程、多个 service/method 与请求关联、schema 不匹配、错误结果、断连 OutcomeUnknown |
| N-06 | 关键失败/取消/关闭路径使用真实后端，mock 仅补充可控时序；支持矩阵不越过实际平台证据 |

N1 依赖 coroutine C1 就绪包与所需 C0 身份。N2 依赖本仓公共类型/生命周期契约、协议选型；若后端不依赖 HTTP，不人为规定 N1 的代码依赖，但按本轮先 HTTP 的交付顺序推进。交给 framework 的包：完整源码 SHA、coroutine/core SHA、公开目标和头、协议 profile/schema、真实构建/测试命令与 N-01 至 N-06 的覆盖矩阵。N2 未就绪前框架可实现绑定/邮箱，不得关闭真实 RPC 集成验收。

本仓起始没有构建测试入口；本文的目标/测试 ID 是待实现验收项，不伪造现有可运行命令。实施交付必须提供干净构建与独立消费可复跑命令。

## 附记：N1 落地后的真实线程模型

HTTP 切片已实现为「无专用 I/O 线程」模型，与早期草案中"infra 自建 io_context + io_thread"的描述相比，实际形态如下：

- infra 不再拥有 `io_context` 或任何线程。所有 Asio/Beast I/O 对象（socket、resolver、acceptor、steady_timer）构造在 `bbt::coroutine::io::GetExecutor()` 返回的共享 executor 派生的 strand 上；`GetExecutor` 指向 coroutine 全局 CoPoller/EventLoop 所持的同一 `io_context`。
- 实际驱动线程是 Scheduler 既有的事件循环线程（`PollOnce`），不随 runtime 数量增加；业务 `HttpHandler` 仍只在受管协程内执行，经 `Scheduler::RegistCoroutineTask` 派生、经 strand 回投结果，两域纪律不变。
- 串行化由 strand 承担：runtime/client/server 共享一份 `HttpIoEngine`，其 `TryPost` 经互斥封口后投递到 strand，等价于旧单 io 线程的执行域语义；`strand` 仅串行化调度，不新增线程。
- 关闭链路不 `stop`/`restart` 共享 context：teardown 在 io 域内中止在途操作；各受管对象按 in-flight async 计数归零后才 `MarkClosed`。引擎 `SealOnIoDomain` 只封死 `TryPost`，不再用 strand marker 冒充 Asio 排空。runtime 在全部子对象 Closed 之后封口引擎并 `MarkClosed`。`Scheduler::Stop` 不代替网络对象的 `RequestClose`/`WaitClosed`，正常顺序仍是先关网络对象再停调度器。
- executor 获取失败或运行时代际不匹配统一返回 `RuntimeUnavailable`；`Request`/`WaitClosed` 的 InvalidContext、TimedOut、Cancelled、Closed、ProtocolError/InternalError 等既有映射不变。
