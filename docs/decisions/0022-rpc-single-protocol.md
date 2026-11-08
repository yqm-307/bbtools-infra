# 0022：RPC V2 唯一消费面（删除 V1 wire、adapter 与双版本分支）

> 后续取代关系（2026-11-08）：V2-only 方向保留；RPC 入口命名、codec 布局和关闭接口以 [RPC 分层现状](../architecture/rpc-layered-protocol.md) 与 [现行指南](../guides/rpc-net.md) 为准。原文保留 codec 能力不等于保留旧路径兼容壳，旧头和原生命周期签名只记录迁移历史。

- 状态：**已授权实施**。用户批准「两仓仅保留 RPC V2、删除 V1 与 LegacyHeaders」的不兼容收敛；本文冻结唯一旧→新消费映射，实施节点按本文实现，不另选架构、不再询问。
- 适用：`bbtools-infra` 的 `bbt/infra/rpc` 公共面，以及 `bbt-framework` 的 RPC 消费链（`CoApp`/`InfraHttpHost`/`InboundDispatcher`/`ICoService`）。
- 取代：`0002` 的 `RpcAddress`/`RpcEnvelope`/`RpcHandler` 段、`0009` 全篇、`0020` 的迁移方向与 `Channel` 目标抽象。
- 不被取代：`0014`（AppSession/SessionFrame wire 与 admission 契约）、`0017`（`Response::FromRequest`）、`0015`/`0018`/`0019`（关闭、写终态、读视图取舍）。
- 依据：`bbt/infra/rpc/{Client,Server,Request,Response,Options,Admission,SessionCodec}.hpp`、`proto/bbt/infra/rpc/v2/session_frame.proto`、`tests/rpc_net/Test_rpc_v2_{wire,session}.cc`、`bbt-framework` 的 `framework/src/host/*`。

本文只描述已有代码可核验的 V2 面；不引入新传输、不新增 HTTP/ACP 通道、不做 `Channel` 插件体系、不保留同名兼容壳。

## 1. 授权范围与不兼容点

已获授权的不兼容收敛：

1. infra 删除 wire profile 1（`RpcEnvelope` + `proto/bbt/infra/rpc/v1`）、`IRpcTransport`/`IRpcInboundTransport` seam、HTTP/TCP 旧 adapter、`bbt_infra_rpc_tcp` 与 `BBT_INFRA_RPC_TCP`。
2. framework 删除 `LegacyHeaders`/`ProtoWireV1`/`TcpV1` 模式选择、`x-bbt-*` 编解码与两座旧桥。
3. 两仓以 `tcp://` 长连接会话为唯一 RPC 通道；`bbt::infra::Codec<T>`、`rpc::RpcPayload`（`bbt.rpc.positional/v1` 业务参数编码）与 HTTP 的 `IncomingCallContext` 保留。

不在授权内：新增 HTTP Session 通道、改变业务 payload 字节格式、触碰 MCP/Mongo/Redis、修改 CI 发布流程或依赖锁。

## 2. infra 唯一公共面

命名空间 `bbt::infra::rpc`；公共头只出现在 `bbt/infra/rpc/`（`detail/` 为非公开实现头）。

| 类型/入口 | 职责 | 明确不负责 |
|---|---|---|
| `Request` | 值层：service/method/request_id/message_id/两个 schema/payload/metadata/traffic/reply/ordering_key/sequence/`Context()` | 不做校验、不持会话；`Context` 无公共 setter |
| `Response`（含 `FromRequest`） | 值层回复与五字段关联初值 | 不校验关联相等，不持 socket |
| `TrafficClass`/`ReplyMode`/`Metadata`/`RequestContext` | wire 枚举与入站上下文（deadline/peer_identity/peer_principal） | System traffic 由 infra 内部生成 |
| `Client::Create(std::shared_ptr<net::Engine>, ClientOptions)` | 会话 owner：一条双向 link 的 Call/Send、握手/epoch、lane 记账、`Stats()`/`Hooks()` | 不启动 Engine、不建线程、不监听 |
| `Client::Call(std::string_view target, Request, const bbt::infra::CallOptions&)` | 出站 Call（`Reply()==Required`，Service/Actor），`result<Response>` | 不做业务重试/降级 |
| `Client::Send(std::string_view target, Request, const SendOptions&)` | 出站单向 Send（`Reply()==None`），可选远端接纳确认 | 不把 SendAccepted 当业务结果 |
| `Client::Close/IsClosed/CloseReason/Shutdown` | 逻辑封口与 owner 排空；已提交无回复 = `OutcomeUnknown` | 不承诺连接物理 Released |
| `Server::Create(engine, std::shared_ptr<Client> session_owner, AdmissionHandler, ServerOptions)` | 双向会话入站：握手、单 FIFO admission、五字段校验、错误终态 | 不建第二份 session registry；不持有业务调度 |
| `Server::RegisterRpc/UnregisterRpc`（可选） | `service/method` 路由 + 每路由令牌桶；只接 `ReplyMode::Required` | 未注册路由回落 admission；Send 永不进注册路由 |
| `Server::Start/StopAccepting/Drain/WaitStopped/Shutdown/Close/IsClosed/CloseReason/LocalAddress/Stats` | 监听与关闭排序工具 | 不强制多阶段关闭协议；不替业务停止任务 |
| `AdmissionItem`/`AdmissionHandler`/`ReplyCompletion`/`AdmissionLease` | 一次入站帧的责任链：同步接纳 → 业务终态一次 → lease 一次 | 接纳回调不得挂起、不得跑业务 |
| `ClientOptions`/`SendOptions`/`ServerOptions` | 装配值 + `Validate()`；明文 trusted-network 唯一开关 | 不联网、不建线程 |
| `SessionCodec`（`SessionFrame`、`ToSessionFrame`/`RequestFromSessionFrame`/`ResponseToSessionFrame`/`ResponseFromSessionFrame`、u32be 分帧） | profile 2 wire 单一真源 | 不服务业务类型编解码 |

`target` 唯一形态：`"tcp://<numeric-ip>:<port>"`（IPv6 用方括号）。无 scheme/路径/查询/用户信息/主机名（无 DNS）；不合法即 `InvalidArgument`，不做隐式解析。

## 3. 唯一旧→新映射

| 旧（删除） | 新（唯一） | 迁移要点 |
|---|---|---|
| `bbt::infra::RpcEnvelope` | `rpc::Request` / `rpc::Response` | `service/method/request_id/request_schema/response_schema/payload/metadata` 同名同义；请求侧另需 `SetReply(Required)`+`SetTraffic(Service/Actor)` |
| `bbt::infra::RpcAddress{transport,endpoint}` | `std::string target`（`"tcp://host:port"`） | transport 固定 tcp；endpoint 由 infra 解析 |
| `bbt::infra::RpcHandler(ctx, env)` | `rpc::AdmissionHandler(AdmissionItem&&)` | 入站改为「同步接纳 + 业务协程结算」；`Request::Context()` 取代 `IncomingCallContext` |
| `bbt::infra::CallOptions`（RPC 用） | 同类型（仅 `deadline`）用于 `Call` | Send 用 `rpc::SendOptions`（deadline + 远端确认） |
| `Client::Create(std::shared_ptr<IRpcTransport>)` | `Client::Create(std::shared_ptr<net::Engine>, ClientOptions)` | 无 transport seam |
| `Client::Create(std::shared_ptr<http::Client>)` | 同上 | HTTP RPC 删除 |
| `Client::Call(const RpcAddress&, RpcEnvelope, CallOptions)` | `Client::Call(target, Request, CallOptions)` | 五字段关联由 infra 校验，失败 `ProtocolError` |
| — | `Client::Send(target, Request, SendOptions)` | 新增能力；framework `CoTell` 消费 |
| `Server::Create(IRpcInboundTransport, RpcHandler)` | `Server::Create(engine, session_owner, AdmissionHandler, ServerOptions)` | 无 inbound transport seam |
| `Server::Create(engine, RpcHandler[, http::ServerOptions])` | 同上 | HTTP 入站 RPC 删除 |
| `RpcWire`/`ParseRpcWire*`/`MakeRpcWire*` | `SessionCodec`（profile 2） | 字节规则不合并、不复用字段号 |
| `HttpRpcTransport`/`HttpRpcInboundTransport` | — | 删除；HTTP 仅保留通用 `bbt::infra::http` |
| `TcpRpcTransport`/`TcpRpcInboundTransport` | 会话内建 TCP link | 旧「一连接一请求」adapter 删除 |
| `bbt_infra_rpc_core` + `bbt_infra_rpc_http` + INTERFACE `bbt_infra_rpc_net` | 单一 STATIC `bbt_infra_rpc_net` | 只留一个公共 target；`bbt_infra_rpc_proto` 保持 PRIVATE |
| `bbt_infra_rpc_tcp` / `BBT_INFRA_RPC_TCP` | — | 删除 flag、`cmake/RpcTcp*.cmake`、`bbt/infra/rpc_tcp/` |
| `RpcInboundBridge{LegacyHeaders,ProtoWireV1,TcpV1}` | — | framework 无模式选择 |
| `RpcEgressProfile{LegacyHeaders,ProtoWireV1,TcpV1}` + `HttpEgress` | `RpcEgress`（单一路径） | 出站只有会话 Call/Send |
| `x-bbt-*` header 编解码（`http_bridge::*`） | — | 随 `RpcHttpBridge` 删除 |
| `IRpcTransport`/`IRpcInboundTransport` | — | 无 seam、无替代抽象 |

## 4. framework 唯一消费映射

framework 不注册 `Server::RegisterRpc`：其 service/method 表带 actor key 提取、schema 校验与有序入口，且 ADR 0014 §4.2 要求「唯一按序接纳入口」。注册路由会为每帧派生独立协程、绕过该 FIFO 与有序接纳，故 framework 一律走 admission handler（未注册路由即回落 admission，`Session.cc:1433` `DispatchAdmission` 已验证）。

| 职责 | 唯一入口 | 必须保持的不变量 |
|---|---|---|
| 装配 | 一个 `net::Engine` + `Client::Create(engine, ClientOptions)` + `Server::Create(engine, 同一 client, admission, ServerOptions)` | Engine 必须 Idle；`Server::Start` 需 Engine Running 且 scheduler 已启动 |
| 本端身份 | `ClientOptions::SetLocalIdentity`（framework `CoAppOptions::local_identity`） | 非空 ≤256 UTF-8；同 identity 自连被拒；identity 字典序小者定 epoch |
| 明文模式 | `ClientOptions::SetTrustedNetworkPlaintext(true)` | 未启用且无 verifier → `UnsupportedRoute`；`peer_principal` 恒空 |
| 监听 | `ServerOptions::SetListenAddress(net::Address)`、`Server::LocalAddress()` | 动态端口经 `LocalAddress()` 回填 `bound_endpoint()` |
| 入站接纳 | `AdmissionHandler` → framework 自有有界队列 + worker 协程 | 回调只做同步校验/转移；满 → `Overloaded`；封口 → `Closed`；不得挂起 |
| 业务分发 | `InboundDispatcher::Dispatch(const rpc::Request&) -> result<rpc::Response>` | 元数据白名单（`route.*`/`fw.*`）、schema 校验、`max_inflight`、`RequestContext` 只取 `Request::Context()` 已验证字段 |
| 回复 | `Response::FromRequest(request)` + `ReplyCompletion::Complete/Fail` | 五字段关联由 infra 复检；一次终态；`lease->Release()` 恰好一次 |
| 单向消息 | `Request::Reply()==None` 的 admission item | infra 写 SendAccepted 并收口 lease；framework 只执行投递，不写终态 |
| 出站 Call | `Client::Call(target, Request, CallOptions)` | `Traffic=Service`、`Reply=Required`；`target` 由路由门解析 |
| 出站 Tell | `Client::Send(target, Request, SendOptions)` | `Reply=None`；`RequireRemoteAcceptance` 映射 `acceptance_requested` |
| 路由 | framework 自己的 `StaticRoute`/`DynamicRouteTable`（endpoint 字符串） | endpoint 只做非空校验，语法归 infra；动态优先、不回退静态、陈旧 `RuntimeUnavailable` |
| 治理 | `RpcPolicy::Preflight`（只收紧期限/拒绝） | 位置不变：路由授权后、发送前一次 |
| deadline | `Request::Context().deadline`（入站 local-min）与 `CallOptions.deadline`（出站） | 只收紧不放大；已过期在 I/O 前 `TimedOut` |
| 取消 | 协程 `RequestCancel` → `Cancelled`；提交后保留 `request_phase` | 不新增业务取消令牌 |
| 排序 | framework `OrderedIngress` + `fw.*` 元数据 | **不填** V2 `OrderingKey`/`Sequence`（`sequence=0` 即 infra 不启用顺序检查），避免与业务有序票据双重语义 |
| 限流 | 入站 `ServiceOptions::max_inflight`；出站 `RpcPolicy` | 每路由令牌桶归 infra 且 framework 不使用（无注册路由） |
| 关闭 | `StopAccepting` → 业务在途归零 → `Shutdown`/`Drain` → `Close(reason)` → `WaitStopped` → Engine `RequestClose` | `Close` 只封口；物理回收另证；不提前关共享 Engine |
| 观测 | `ClientStats`/`ServerStats`（owner 汇总） | 不发明逐 peer 枚举 |

metadata 约束（`SessionCodec::ValidateMetadata`）：只允许 `fw.`/`route.`/`trace.` 前缀，≤32 项，键 ≤64 字节、值 ≤256 字节、无重复键。framework 的 `fw.trace_id`、`fw.producer_id`、`fw.producer_epoch`、`fw.sequence`、`fw.receiver_epoch` 与 `route.*` 全部落在白名单内，字节格式不变。

## 5. 删除清单与负向约束

infra 删除（整文件）：`bbt/infra/rpc/{RpcWire,Transport,HttpTransport,TcpTransport}.{hpp,cc}`、`proto/bbt/infra/rpc/v1/`、`cmake/RpcTcp.cmake`、`cmake/RpcTcpTarget.cmake`、`bbt/infra/rpc_tcp/`、`tests/rpc_net/Test_rpc_net_{wire,roundtrip,faults,lifetime,tcp}.cc`、`examples/rpc_net/{main,tcp_main}.cc`；`NetworkTypes.hpp` 移除 `RpcEnvelope`/`RpcAddress`/`RpcHandler`。

framework 删除（整文件）：`internal/RpcWireBridge.{hpp,cc}`、`internal/RpcHttpBridge.{hpp,cc}`、`tests/Test_framework_rpc_profile.cc`；`CoAppOptions::inbound_bridge`、`RpcInboundBridge`、`RpcEgressProfile`、`RpcSendAppFn` 的旧签名、`RpcCallView` 的 `RpcAddress` 字段。

保留（不得整块误删）：`bbt::infra::Codec<T>`、`rpc::RpcPayload` 与 `bbt.rpc.positional/v1`、`SessionCodec`、`Request`/`Response`、`Admission`、`Session.cc` 内 `detail/Tcp` 的 `ParseTcpEndpoint`/`TcpCallError`/`TcpSettleWrite`、HTTP 的 `IncomingCallContext` 与 `bbt::infra::http`、MCP/Mongo/Redis。

负向验证（必须真实运行，不以「文件不存在」自述代替）：新增 CTest `rpc.net.v1_absent`，判据分三层，编译拒绝为主。

(a) 编译拒绝：每个探针单独编译，期望非零退出；同一探针删掉违例行后必须编译通过，以证明拒绝来自旧面而不是环境。探针逐项引用 `bbt::infra::RpcEnvelope`、`bbt::infra::RpcAddress`、`bbt::infra::RpcHandler`（旧签名 `std::function<result<RpcEnvelope>(IncomingCallContext, RpcEnvelope)>`）、`bbt::infra::IRpcTransport`、`bbt::infra::IRpcInboundTransport`、`bbt::infra::RpcWire`/`ParseRpcWire*`/`MakeRpcWire*`、`bbt::infra::HttpRpc*`/`TcpRpc*`，以及 `#include <bbt/infra/rpc/{RpcWire,Transport,HttpTransport,TcpTransport}.hpp>`、`bbt/infra/rpc_tcp/…`、`bbt/infra/rpc/v1/…`。

(b) 正向对照（同一探针必须编译通过）：`bbt::infra::rpc::Server::RpcHandler`（`Server.hpp` 保留的 V2 路由别名，`std::function<bbt_co_async(result<Response>)(Request)>`）、`rpc::AdmissionHandler`、`rpc::Request`/`rpc::Response`。裸 `RpcHandler` 不是残留证据——只有 `bbt::infra::RpcHandler` 限定名或旧签名才判失败。

(c) 文本扫描只定位活跃产品调用，不作主判据：范围限 `bbt/infra/**`、`cmake/`、顶层 `CMakeLists.txt`，不含 `tests/`、`examples/` 与负向测试自身；旧类型按限定名 `bbt::infra::(RpcHandler|RpcEnvelope|RpcAddress|RpcWire|IRpcTransport|IRpcInboundTransport|HttpRpc\w*|TcpRpc\w*)` 匹配，旧头按 `bbt/infra/rpc/(RpcWire|Transport|HttpTransport|TcpTransport)\.hpp`、`bbt/infra/rpc_tcp/`、`bbt/infra/rpc/v1` 匹配；`cmake/` 与顶层 `CMakeLists.txt` 不出现 `BBT_INFRA_RPC_TCP|infra_rpc_tcp|infra_rpc_core|infra_rpc_http`。路径存在性另断言：`proto/bbt/infra/rpc/v1`、`bbt/infra/rpc_tcp/`、`cmake/RpcTcp*.cmake` 均不存在。

framework 侧同口径由 `framework.v1_absent` 承担：编译拒绝 `RpcInboundBridge`、`RpcEgressProfile`、`LegacyHeaders`/`ProtoWireV1`/`TcpV1` 与 `#include <bbt/framework/internal/{RpcHttpBridge,RpcWireBridge}.hpp>`；正向对照 `rpc::AdmissionHandler`、`rpc::Server::RpcHandler`。文本扫描同样只覆盖活跃产品面（`framework/src`、`framework/include`、CMake，不含 `tests`/`examples`/负向测试自身），且 `x-bbt-` 只按旧 RPC 信封头族 `x-bbt-(service|method|request-id|request-schema|response-schema|meta-)` 匹配，不用裸 `x-bbt-`；`x-bbt-err-*` 是 HTTP 错误映射（保留用例 `framework.ordered_egress` 断言的对象），不在负向范围，只要不再由 RPC 编解码产生或消费。

## 6. 构建与验收

| 项 | 值 |
|---|---|
| 开关 | `BBT_INFRA_RPC_NET=ON`（仍需 `BBT_INFRA_HTTP_NET=ON` 与 `BBT_PROTOBUF_PREFIX`，fail-closed） |
| 公共 target | `bbt::infra_rpc_net`（STATIC）；`bbt_infra_rpc_net_header_selfcheck`；`bbt_infra_rpc_public_header_leak_check` |
| 私有 target | `bbt_infra_rpc_proto`（只生成 v2 `session_frame.pb.{h,cc}`） |
| C++ | C++17，Boost 1.90，ccache，`--parallel 2`，`ctest -j1` |

infra 定向验证集（`tests/rpc_net/CMakeLists.txt` 注册名，`rpc.net.*`）：

`v2_roundtrip v2_reject v2_malformed v2_framing v2_values response_from_request`（`Test_rpc_v2_wire`，均已存在）、
`v2s_roundtrip v2s_registered_route v2s_bidirectional v2s_send_acceptance v2s_ordering v2s_overload v2s_lane_fairness v2s_disconnect v2s_deadline v2s_handshake_raw v2s_handshake_prefix_reject v2s_close_drain v2s_close_reason v2s_admission_inflight v2s_admission_reject v2s_admission_bytes v2s_reply_corr_mismatch v2s_close_released v2s_close_inflight_reason v2s_shutdown_timeout_draining v2s_never_started_drain_close v2s_hook_pause_cancel_close`（`Test_rpc_v2_session`，均已存在）、
`v2_session_consumer`（`examples/rpc_net/CMakeLists.txt` 注册的双进程 consumer，已存在）、`co_lifecycle`（`tests/rpc_net`，已存在）、`write_terminal_once`（`Test_rpc_v2_session`，已存在）。

**新增用例（实现时必须创建，名字固定）**：`rpc.net.v1_absent`（§5 负向检查）、`rpc.net.v2s_client_input`（原 `client_input`：target 形态、`Call`/`Send` 的 `Reply` 错配、业务伪造 `System` traffic、metadata 越界 → `InvalidArgument` 且不发 I/O）、`rpc.net.v2s_cancel_post_commit`（原 `cancel_post_commit`：请求字节提交后取消 → `Cancelled` 且保留 `request_phase=RequestCommitted`）、`rpc.net.v2s_malformed_reply`（原 `malformed_reply`：裸对端回错误 profile/非法 Response → `ProtocolError`，不 crash）。

V1 断言去处：`wire_roundtrip|wire_request_phase→v2_roundtrip`、`wire_reject→v2_reject`、`wire_malformed→v2_malformed`、`client_input→v2s_client_input`、`cancelled→v2s_hook_pause_cancel_close`、`cancel_post_commit→v2s_cancel_post_commit`、`malformed_reply→v2s_malformed_reply`、`budget_local_min→v2s_deadline`、`association→v2s_roundtrip+v2s_reply_corr_mismatch`、`inflight_authorization→v2s_admission_inflight`、`roundtrip|reuse→v2s_roundtrip+v2s_bidirectional`、`transport_errors|unknown_outcome→v2s_disconnect+v2s_close_inflight_reason`、`client_close→v2s_close_reason`、`close_drops_inflight|server_lifecycle→v2s_close_drain+v2s_shutdown_timeout_draining+v2s_never_started_drain_close`、`released_client_handle→v2s_close_released`、`server_malformed→v2s_handshake_prefix_reject`、`transport_defaults`：**删除**（HTTP 默认头合并随 profile 1 消失；等价的限额生效由 `v2_values`+`v2s_admission_bytes` 承担）。

`scripts/ci/required_rpc_http_net.txt` 的 21 个 `rpc.net.*` 旧名按上表替换为对应新名；`scripts/ci/debug_gate.sh` 的构建 target 列表去掉 `Test_rpc_net_{wire,roundtrip,faults,lifetime}`/`rpc_net_example`，加入 `Test_rpc_v2_wire`/`Test_rpc_v2_session`/`rpc_v2_session_example`/`v1_absent` 承载的 target；`scripts/ci/changed_files.py` 删除 `bbt/infra/rpc_tcp/` 映射行。其余门禁不变。

framework 定向验证集（`tests/CMakeLists.txt` 注册名，标签不变）：`framework.rpc_surface framework.request_context framework.coapp_lifecycle framework.actor_mailbox framework.ordered_egress framework.ordered_ingress framework.rpc_codec framework.dynamic_routes framework.resource_lifecycle framework.close_gate framework.inbound_http framework.shutdown framework.rpc_matrix framework.egress_request_phase framework.getvalue_codec framework.rpc_xlang`（均已存在，内部改为 V2 会话）与 `examples.*`（`examples.getvalue.xlang`、`examples.getvalue.s2s`、`examples.two_service`、`examples.rpc_policy`、`examples.lifecycle_matrix`、`examples.component_services`、`examples.ecommerce_cluster`，均已存在）。**改名（实现时执行）**：`framework.ordered_ingress_http` → `framework.ordered_ingress_rpc`（原用例名不再描述 V2 行为，标签仍 `smoke;integration`）。**新增**：`framework.v1_absent`（§5 的编译拒绝 + 限定名文本扫描口径；`x-bbt-` 按旧 RPC 头族匹配，不用裸串，标签 `smoke;unit`）。**删除**：`framework.rpc_profile`（LegacyHeaders/HTTP/TCP profile 专有；其 roundtrip/OutcomeUnknown/Close 断言分别由 `rpc_matrix`/`egress_request_phase`/`shutdown` 承担）。

## 7. 未覆盖与边界

- 本轮只承诺本地交付：不声明 CI、提交、发布完成；`deps.lock` 不得写入未来或伪造 SHA，正式提交后再钉真实 infra SHA。
- 无 DNS/TLS/代理/重定向/自动重试；`target` 只接受数值 IP。
- 逐 peer/epoch 观测仍是缺口（`0014`）；不因本迁移新增。
- framework 的 `peer_principal` 在 trusted-network 明文模式下恒空，授权仍由 `RpcPolicy` 与业务层承担，本迁移不改变该事实。
