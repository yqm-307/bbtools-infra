# RPC 分层现状（bbt::infra_rpc_net）

> 状态：**现行（已落地）**。本文只描述当前代码与测试的事实。尚未落地的设计项集中在 §8，不是现状。公共用法见 [RPC core 指南](../guides/rpc-net.md)。

## 1. 分层

```text
CoRpcClient / CoRpcServer                   公共面：Call/Send/Close/Shutdown；client 是共享 session owner
  └─ detail/RpcSession.cc, AdmissionImpl.cc 会话实现：握手、pending、deadline、lane、admission、写终态
       ├─ detail/RpcFrameProtocol.hpp       无状态帧协议（静态纯函数）：校验、编码、解码、增量拆包入口
       │    └─ SessionCodec.hpp             profile 2 字节规则真源；wire 字段见 session_frame.proto
       └─ detail/RpcChannel.hpp             ICoChannel 无状态字节通道；当前实现为 TcpChannel
            └─ detail/Tcp.hpp               数值 endpoint 解析、写终态结算助手

codec/PositionalCodec.hpp                   payload 位置参数 codec，不被会话层依赖，由调用方在 payload 外使用
```

## 2. 状态归属

会话状态集中在 `detail/RpcSessionState.hpp`，三类台账互不交叉：

| 状态 | 归属 | 说明 |
|---|---|---|
| 单 link 的收发缓存、拆包 decoder、pending、握手进度、lane 顺序、关闭状态 | `SessionCore` | 每条 link 一份 |
| 连接复用台账（epoch、identity → session） | `SessionOwnerState`，由 owner（`CoRpcClient`）持有 | |
| listener、admission 队列、路由台账 | `ServerV2State`，由 `CoRpcServer` 持有 | |
| 字节读写与物理关闭标志 | `ICoChannel` 实现（`TcpChannel`） | 不解析帧、不关联请求 |
| 帧编解码 | `RpcFrameProtocol` | 全部为静态函数，不持有状态；拆包缓存由 session 持有的 `SessionFrameDecoder` 承担 |

## 3. 线上格式（profile 2）

- 分帧：`u32be payload_length | payload[payload_length]`，payload 为 protobuf `SessionFrame`（`proto/bbt/infra/rpc/v2/session_frame.proto`）。
- 版本：`profile_version` 固定为 2，其他值为 `ProtocolError`。这是当前唯一的版本字段。
- 长度：`payload_length == 0` 立即拒绝；Hello/HelloAck 帧上限 64 KiB；数据帧默认上限 8 MiB，硬上限 1 GiB。
- payload：Request/Response 的 payload 是不透明字节，RPC core 不解析。线上只携带 `request_schema` / `response_schema`，**没有 `codec_id` 字段**，编码方式由两端在本地选择。
- 关联：`request_id/service/method/request_schema/response_schema` 与请求不一致为 `ProtocolError`。字段长度上限见 `SessionCodec.hpp`（`kRpcV2Max*`）。
- 顺序：`sequence == 0` 不做顺序检查；非 0 时 `ordering_key` 必须非空。启用后 session 维护 per-key 入站台账：同 key 首帧即基值，之后必须逐帧连续递增；重复、逆序与缺口都按 `ProtocolError` 退役该 link。台账只描述 wire 到达序，不缓冲重排——可靠字节流上的缺口不会自愈。

## 4. 握手与 epoch

- 双向 `Hello`，身份字典序小者为 epoch owner（`LocalIdentity() < peer_identity` 时本端持有 epoch），之后交换 `HelloAck` 并回显对端确认。
- 自连接、echo 不匹配、`peer_identity` 缺失或超过上限，均为 `ProtocolError`。
- 未启用 `SetTrustedNetworkPlaintext(true)` 且没有 verifier 时返回 `UnsupportedRoute`。

## 5. 关闭语义

`Close` 是唯一的协程关闭入口，三态共用它；`Shutdown` 是宿主侧同步强制关闭。不存在 Drain、WaitClosed、StopAccepting 阶段。

| 入口 | 行为 | 等待与结果 |
|---|---|---|
| `Close(Immediate, 0)` | 立即逻辑封口并发布首个关闭原因（幂等、不可重开）；拒绝新 Call/Send；强制关闭在途 link，结算全部未终结调用 | 不等待任何在途完成 |
| `Close(Timeout, timeout_ms)` | 先执行 Immediate 的强制收口，再有界等待 RPC 自身持有的 session、握手与写责任物理收口 | 超时返回 `TimedOut` |
| `Close(Graceful, timeout_ms)` | 先拒新（不发布原因）；等已接纳调用真实归还责任后封口并发布原因。`timeout_ms == -1` 表示无限等待 | 有限期限超时返回 `TimedOut` 并强制收口 |
| `Shutdown(reason)` | 任意域、不挂起、幂等；立即拒绝新操作，强制关闭所有 session/link，未终结调用立即失败 | 不等待 handler、reply、队列或物理释放 |

- `Shutdown` 与 `Close(Immediate)` 共用同一关闭状态，没有第二套生命周期。
- 负 `timeout_ms` 只允许 `Close(Graceful, -1)`；Immediate/Timeout 及其他负值直接返回 `InvalidArgument`，不启动等待。
- `CoRpcServer::Create` 要求传入的 `net::Engine` 与 `session_owner`（CoRpcClient）装配的 Engine 是同一实例，否则返回 `InvalidArgument`。
- Server 的 Graceful 等待的是 admission 队列、已授权的 reply/lease 与写责任归还。
- 等待只覆盖 RPC 自身责任，不承诺业务 handler 的优雅结束。
- `Close` 与 `Shutdown` 都不关闭共享 Engine；物理回收以 Engine 收口为证。
- 框架宿主的 RPC 路径使用同步 `CoRpcServer::Shutdown()` 与 `CoRpcClient::Shutdown()`；协程 `Close` 不在该入口的执行域内使用。
- 公共 API 不暴露 draining 状态。`detail/` 内的 draining 标记不是调用方需要理解的阶段。

## 6. 测试与证据

用例注册见 `tests/rpc_net/CMakeLists.txt`、`tests/CMakeLists.txt` 与 `examples/rpc_net/CMakeLists.txt`；label 与执行口径见 [测试策略](../guides/testing.md)。本文不记录通过数，以 CI 实跑为准。

- `rpc.net.*`（`ctest -j1 -R '^rpc\.net\.'`）覆盖并点名：
  - `rpc.net.v2_protocol_seam`：直接驱动 `RpcFrameProtocol` 的 wire 与帧协议接缝；
  - `rpc.net.v2s_ordering_ledger`：顺序台账；
  - `rpc.net.v2s_close_negative_timeout`：负 timeout 拒绝；
  - `rpc.net.v2s_server_cross_engine`：跨 Engine 装配拒绝；
  - 会话层 `rpc.net.v2s_*`、`rpc.net.co_lifecycle`、`rpc.net.v1_absent` 公共面负向门禁。
- `rpc.net.v2_session_consumer`（`examples/rpc_net/session_consumer.py` 驱动）：双进程 Call/Send 与收口测试。
- `rpc.payload`：PositionalCodec 对旧源 literal 的字节级 golden 比对（smoke/debug 必选）。它不是线上帧的跨语言 fixture。

## 7. 已删除的旧面

以下已不在公共头中，也不是任何现行入口，迁移时不要再引用：

- `rpc::Client` / `rpc::Server`：由 `CoRpcClient` / `CoRpcServer` 取代；直接包含对应的 `CoRpcClient.hpp` / `CoRpcServer.hpp`，旧 `Client.hpp` / `Server.hpp` 转发头已删除。
- `RpcPayload.hpp/.cc` 兼容空壳已删除；位置参数编码能力保留在 `codec/PositionalCodec.hpp/.cc`，调用方直接包含 `codec/PositionalCodec.hpp`。
- `Drain`、`WaitStopped`、`StopAccepting`、`WaitClosed`、`Shutdown(Deadline)`：由 `Close(CloseFlag, ...)` 与 `Shutdown(reason)` 取代。
- V1：`RpcEnvelope`、`IRpcTransport` seam、旧 HTTP/TCP adapter、`BBT_INFRA_RPC_TCP`、`bbt/infra/rpc_tcp/`（依据 [ADR 0022](../decisions/0022-rpc-single-protocol.md)）。

## 8. 未落地（不是现状）

以下曾在设计中出现，当前代码与测试中**不存在**：

- **HTTP Channel**：RPC 只经 `TcpChannel` 承载；core 不链接 `bbt::infra_http_net`。
- **`codec_id`**：线上不声明 codec，编码方式由两端本地决定。
- **`magic`**：帧首没有 magic 字节，线上只有 u32be 长度前缀与 `profile_version`。
- **`checksum`**：wire 中没有 checksum 字段，也没有帧尾完整性校验。
- **C ABI / codec 插件平台**：不引入 registry 或 ABI。
- **跨实现 golden fixture（帧字节）**：没有由非 C++ 实现与 C++ 逐字节比对的冻结帧样本。`rpc.net.v2_session_consumer` 是双进程协作收口测试，不是字节 fixture。
