# RPC 分层现状（bbt::infra_rpc_net）

> 状态：**现行（已落地）**。本文只描述当前代码与测试的事实。尚未落地的设计项集中在 §8，不是现状。公共用法见 [RPC core 指南](../guides/rpc-net.md)。

## 1. 分层

```text
CoRpcClient / CoRpcServer                   客户端负责 Call/Send 与共享会话，服务端负责接纳，两者均用 Close(options) 收口
  └─ Session.cc                   启动、accept 与关闭
       ├─ detail/RpcSessionOwner.cc   拨号、复用、链路安装和 Call/Send
       ├─ detail/RpcHandshake.cc      Hello / HelloAck 与 decoder 交接
       ├─ detail/RpcSession.cc        单 link 收发、lane、写终态与退役
       ├─ detail/RpcInbound.cc        帧分发、入站接纳与路由
       │    └─ detail/AdmissionImpl.cc    接纳 FIFO、lease 与回复凭据
       ├─ SessionCodec.hpp           profile 2 帧值、字节规则与增量 decoder
       └─ detail/RpcChannel.hpp      ICoChannel 字节通道；当前实现为 TcpChannel
            └─ detail/Tcp.hpp        数值 endpoint 解析与写终态结算

codec/PositionalCodec.hpp                   payload 位置参数 codec，不被会话层依赖，由调用方在 payload 外使用
```

## 2. 状态归属

会话状态集中在 `detail/RpcSessionState.hpp`，三类台账各有职责。安装事务按 owner → server 的顺序持锁并交接连接；真正的 I/O 和关闭等待不在该临界区内执行：

| 状态 | 归属 | 说明 |
|---|---|---|
| 单 link 的收发缓存、拆包 decoder、pending、lane 顺序、关闭状态 | `SessionCore` | 每条 link 一份 |
| 连接复用台账（epoch、identity → session）、自动尝试 ID 计数 | `SessionOwnerState`，由 owner（`CoRpcClient`）持有 | 自动 ID 使用固定大小计数，不记录已用 ID |
| listener、入站握手、admission 队列、路由台账 | `ServerState`，由 `CoRpcServer` 持有 | |
| 字节读写与物理关闭标志 | `ICoChannel` 实现（`TcpChannel`） | 不解析帧、不关联请求 |
| 帧编解码 | `SessionCodec.hpp` | 帧值与字节规则不持有连接状态；握手通过后将本连接的 `SessionFrameDecoder` 交给 reader |

`AdmissionQueue` 只接纳 Call 和 Send；Response 与 SendAccepted 不进入该 FIFO，直接结算当前 link 的关联表。

## 3. 线上格式（profile 2）

- 分帧：`u32be payload_length | payload[payload_length]`，payload 为 protobuf `SessionFrame`（`proto/bbt/infra/rpc/v2/session_frame.proto`）。
- 版本：`profile_version` 固定为 2，其他值为 `ProtocolError`。这是当前唯一的版本字段。
- 长度：`payload_length == 0` 立即拒绝；Hello/HelloAck 帧上限 64 KiB；数据帧默认上限 8 MiB，硬上限 1 GiB。
- payload：Request/Response 的 payload 是不透明字节，RPC core 不解析。线上只携带 `request_schema` / `response_schema`，**没有 `codec_id` 字段**，编码方式由两端在本地选择。
- 关联：`request_id/service/method/request_schema/response_schema` 与请求不一致为 `ProtocolError`。字段长度上限见 `SessionCodec.hpp`（`kRpcV2Max*`）。
- 顺序：`sequence == 0` 不做顺序检查；非 0 时 `ordering_key` 必须非空。启用后 session 维护 per-key 入站台账：同 key 首帧即基值，之后必须逐帧连续递增；重复、逆序与缺口都按 `ProtocolError` 退役该 link。台账只描述 wire 到达序，不缓冲重排——可靠字节流上的缺口不会自愈。

## 4. 握手与 epoch

- 双向 `Hello`，身份字典序小者为 epoch owner（`LocalIdentity() < peer_identity` 时本端持有 epoch），之后交换 `HelloAck` 并回显对端确认。
- 关联按本地 link 的 `(PeerKey, session_epoch)` 隔离，分别用 `request_id` 匹配 Response、用 `message_id` 匹配 SendAccepted。旧 epoch、已摘除的等待或尚未提交的条目不会结算当前尝试。自动 ID 只承诺同一 owner 实例不重复，不是跨进程或重启的全局 ID；显式值的适用条件见 [RPC 指南](../guides/rpc-net.md#调用约定)。
- 自连接、echo 不匹配、`peer_identity` 缺失或超过上限，均为 `ProtocolError`。
- 未启用 `SetTrustedNetworkPlaintext(true)` 且没有 verifier 时返回 `UnsupportedRoute`。

## 5. 关闭语义

客户端与服务端均通过 `Close(const CloseOptions&)` 关闭。模式、首因、绝对期限与物理回收遵循[统一关闭契约](close-options-contract.md)。

RPC 服务端排空已接纳的请求、回复、lease 与写责任。Graceful 到期时返回 `TimedOut` 并保留在途责任。`wait_release` 在同一期限内等待本对象责任退出及真实 `Released`，不等待共享 Engine 退出。执行域和调用示例见 [RPC 指南](../guides/rpc-net.md)。

- 会话 owner 在拨号前占 `MaxSessions`，成功安装后由 link 租约接管；被替换的 link 只退出路由索引，仍占额度直到收口。未安装的出站连接及已提交但未交付的拨号由原占位保留回收凭据；未回收的同地址拨号不新建。
- 服务端从 `CoAccept` 成功交付起负责入站连接。握手安装时同一事务将连接交给会话 owner 并登记服务端 link 租约，握手任务仍须退出；未安装的连接由握手台账保留到释放。服务端 `MaxConnections` 覆盖握手和入站 link 退役期；超额或封口竞态下，至多额外保留一条已交付、待拒绝连接，未释放前不再领取新的连接。未交付的 accept 仍由 net 负责。

## 6. 测试与证据

用例注册见 `tests/rpc_net/CMakeLists.txt`、`tests/CMakeLists.txt` 与 `examples/rpc_net/CMakeLists.txt`；label 与执行口径见 [测试策略](../guides/testing.md)。本文不记录通过数，以 CI 实跑为准。

- `rpc.net.*`（`ctest -j1 -R '^rpc\.net\.'`）覆盖并点名：
  - `rpc.net.v2_protocol_seam`：通过私有兼容壳 `RpcFrameProtocol` 驱动 `SessionCodec`，覆盖分片、粘连和独立 decoder；生产不使用该壳；
  - `rpc.net.v2s_ordering_ledger`：顺序台账；
  - `rpc.net.v2s_close_negative_timeout` 验证等待类关闭在缺少有限期限时拒绝调用；
  - `rpc.net.v2s_server_cross_engine`：跨 Engine 装配拒绝；
  - 会话层 `rpc.net.v2s_*`、`rpc.net.co_lifecycle`、`rpc.net.v1_absent` 公共面负向门禁。
- `rpc.net.v2_session_consumer`（`examples/rpc_net/session_consumer.py` 驱动）：双进程 Call/Send 与收口测试。
- `rpc.payload`：PositionalCodec 对旧源 literal 的字节级 golden 比对（smoke/debug 必选）。它不是线上帧的跨语言 fixture。

## 7. 已删除的旧面

以下已不在公共头中，也不是任何现行入口，迁移时不要再引用：

- `rpc::Client` / `rpc::Server`：由 `CoRpcClient` / `CoRpcServer` 取代；直接包含对应的 `CoRpcClient.hpp` / `CoRpcServer.hpp`，旧 `Client.hpp` / `Server.hpp` 转发头已删除。
- `RpcPayload.hpp/.cc` 兼容空壳已删除；位置参数编码能力保留在 `codec/PositionalCodec.hpp/.cc`，调用方直接包含 `codec/PositionalCodec.hpp`。
- 旧的分阶段关闭入口已收敛为 `Close(const CloseOptions&)`。
- V1：`RpcEnvelope`、`IRpcTransport` seam、旧 HTTP/TCP adapter、`BBT_INFRA_RPC_TCP`、`bbt/infra/rpc_tcp/`（依据 [ADR 0022](../decisions/0022-rpc-single-protocol.md)）。

## 8. 未落地（不是现状）

以下曾在设计中出现，当前代码与测试中**不存在**：

- **HTTP Channel**：RPC 只经 `TcpChannel` 承载；core 不链接 `bbt::infra_http_net`。
- **`codec_id`**：线上不声明 codec，编码方式由两端本地决定。
- **`magic`**：帧首没有 magic 字节，线上只有 u32be 长度前缀与 `profile_version`。
- **`checksum`**：wire 中没有 checksum 字段，也没有帧尾完整性校验。
- **C ABI / codec 插件平台**：不引入 registry 或 ABI。
- **跨实现 golden fixture（帧字节）**：没有由非 C++ 实现与 C++ 逐字节比对的冻结帧样本。`rpc.net.v2_session_consumer` 是双进程协作收口测试，不是字节 fixture。
