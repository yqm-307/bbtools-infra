# 0008：出站请求阶段与 OutcomeUnknown 分界

> 需求来源：`bbtools-infra#64`（下游阻塞 `bbt-framework#4` R5/V05）。
> 状态：已实现；本文件记录取舍与可复验口径，不复制实现正文。

## 背景

`bbtools-infra` 的 HTTP 出站路径（`HttpClientImpl`）在 connect、send、读回复各
阶段失去连接时，过去统一以 `TransportError` / `TimedOut` / `Cancelled` /
`Closed` 对外呈现。`ErrorCode::OutcomeUnknown` 早已存在（`Result.hpp`，且
`docs/decisions/0002` 的 N2 已写明「发出后失联时能够表达 OutcomeUnknown」），
RPC wire profile（`rpc/RpcWire.hpp`）也已双向映射它，但缺少**请求阶段事实**——
调用方无法机械判定「请求是否已完整写出」。

后果是二选一都不安全：

- 把所有断连都改成 `OutcomeUnknown`，会把一次都没发出去的请求误报成「可能已生效」；
- 维持现状，会把可能已经在远端生效、只是回复丢失的请求伪装成确定失败，
  上层据此重试就会产生重复副作用。

## 决定

### 1. 阶段事实随终态错误发布（公开、稳定、可序列化）

`include/bbt/infra/Result.hpp` 新增公开枚举 `RequestPhase`
（`NotStarted` / `Connecting` / `Writing` / `RequestCommitted` / `ReplyTerminal`）、
判定函数 `IsRequestCommitted(RequestPhase)`，并在 `Error` 末尾追加字段：

```cpp
std::optional<RequestPhase> request_phase;
```

性质与兼容性：

- 只使用标准库类型，不泄漏 Beast/Asio/socket 或任何第三方对象；
- 追加在结构末尾、默认 `nullopt`，不改变任何既有字段的语义与位置，
  也不改 `ErrorCode` 的取值（`OutcomeUnknown` 复用既有枚举值，wire 编码不变）；
- `nullopt` 表示「该错误不来自出站 operation」（上下文/参数校验/装配/已关闭
  拒绝等），与「已完整写出后未知」在机器上可区分；
- 描述符稳定可序列化：`OutcomeUnknown` 已可经 `rpc/RpcWire` 编解码，
  本决定不改 envelope schema。

### 2. 「完整写出」的唯一线性化点

`ClientOp::PumpWrite` 是唯一的写出泵（序列化器 + 非阻塞 `send`）。
`serializer->is_done()` 为真表示请求消息（头 + body）的全部字节已由
`socket.send` 交给内核——这是本模块能提供的唯一可靠线性化点，在该处发布
`RequestCommitted`。**不在** connect 成功、也不在首次 `send` 之前发布：那时
请求可能一字节都没有发出去。

### 3. 终态映射（唯一分界点 `ClientOp::SealError`）

| 落定时的阶段 | 失去可信回复终态的错误 | 结果 |
|---|---|---|
| `NotStarted` / `Connecting` | 任意 | 保持既有确定错误（`TransportError` / `Unavailable` / `TimedOut` / `Cancelled` / `Closed` / `InternalError`） |
| `Writing` | 任意 | 同上：请求未完整写出，写中断也是确定失败 |
| `RequestCommitted` 及以后 | `TransportError` / `TimedOut` / `Cancelled` / `Closed` / `InternalError`（`IsReplyLossAfterCommit`） | 升级为 `OutcomeUnknown` |
| `RequestCommitted` | `ProtocolError` 等「对回复本身的确定判决」 | 保持原分类 |

要点：

- **确定性分类只由阶段决定**，不解析 message / errno / 日志顺序，也不用固定等待；
- 升级只改 `code`（并绑定 `request_phase`），不做字符串分配（该路径在
  `noexcept` 边界内）；物理原因保留在 `domain_code` / `backend_category` /
  `backend_code` / `message`；
- 可信回复不被升级：完整响应（含明确 4xx/5xx）以 `result<HttpResponse>` 成功
  返回，不经过错误分支；`ReplyTerminal` 只在解析器完整收下一条消息时发布；
- 端到端失联（对端字节到达但 framing 不可用）仍是 `ProtocolError`——它是对
  已到达字节的确定判决，不是「回复丢失」，符合 `0002` 中 OutcomeUnknown
  「发出后失联」的口径。

### 4. 一次逻辑终态与放弃线性化

调用方提前放弃（deadline / 协程级取消）时，`Request` 在调用线程取 engine 的
`IoGate`，**在同一门内**依次执行 `Abort`（记账被中止的等待项、关 socket /
resolver、释放本 op 载荷）→ 读一次阶段 → `SealError` → `Finish`，再返回。
这与 owner `Close` / `TeardownOnIoDomain` 的既有范式同源，使「放弃决定」与
「后端不再推进写侧」成为同一线性化点：

- 未提交（`NotStarted` / `Connecting` / `Writing`）：`Abort` 已关 socket 并
  释放载荷，等门排在其后的 io 域入口都因 `finished` / `!op_armed` 早退 ⇒ 返回
  的确定失败为真，此后不可能再完整写出；
- 已提交（`RequestCommitted` / `ReplyTerminal`）：读到已提交阶段，`SealError`
  按 §3 判据升级为 `OutcomeUnknown`，不谎报确定失败。

反例（本决定消除的路径）：只读一次阶段快照、把 `Abort` 异步投递出去就返回，
快照与「未来完整写出」之间没有同步关系，可能把「即将完整写出」的请求返回为
确定失败，上层据此重试即产生重复副作用（正是本需求要堵的方向）。后端晚到的
完成项仍由 `finished` 的 CAS 拦住，不覆盖已发布终态。`Abort` / 物理清理继续由
infra owner 完成，适用既有 #37 / #62 语义：「调用方返回」不冒充「物理释放」。

## 后果与不变项

- 保持不变：`ErrorCode` 数值、RpcWire 编解码、HTTP 成功/4xx/5xx 语义、
  未提交失败的既有确定错误、出站配额（#37）与 session teardown（#62）行为、
  不新增默认重试、TLS/认证不变。
- 有意改变：**请求已完整写出**后发生的 deadline / 协程取消 / owner Close /
  对端断开，终态从 `TimedOut` / `Cancelled` / `Closed` / `TransportError`
  变为 `OutcomeUnknown`。这是本需求的目的，不是回归。
- 不提供：自动重试、幂等键、业务补偿、远端取消必达的承诺。

## 可复验口径（S1–S8）

`tests/Test_http_request_phase.cc`（`http.request_phase`）用自有动态 loopback
对端做事件控制，不靠固定 sleep、不放宽断言：

- S1 未建立连接：连接被拒 → `TransportError`、阶段未提交；
- S2 写中断：对端 accept 后关闭且不读、请求体 8MiB 远超内核缓冲 → 写侧不可能
  完成 → `TransportError`、阶段 `Writing`；
- S3 完整写出后丢回复：服务端**自己读到请求结束符**后才置位收全标记，随后
  不回复直接断开 → 服务端标记与客户端 `request_phase == RequestCommitted`
  交叉断言，终态 `OutcomeUnknown`，对端只被连接一次（无重试）；
- S4 可信回复：200/404/500 保持成功/远端错误语义；畸形 framing 仍为
  `ProtocolError`（不升级）；
- S5 deadline/Close 两阶段：未提交 → 确定失败（`TimedOut` / `Closed`，阶段未
  提交）；已提交 → `OutcomeUnknown` 且配额恰好归还；
- S6 多 operation：同一 client 上「未提交失败 / 已提交未知 / 正常回复」交替，
  阶段状态互不串用；
- S8 放弃边界（独立审查 F-1）：用 `on_write_started` 事件确认客户端已进入写出，
  持有型对端 + 8MiB 体使写侧必然停在 `Writing`（未提交），deadline 到点后放弃
  返回确定失败（`TimedOut`）、阶段未提交，且放行对端排空后仍收不到完整请求（无
  事后完整写出、无重试）。**限制（不伪称覆盖）**：本运行时 io handler 与 deadline
  定时器同由单一 Scheduler 事件循环线程（`CoPoller::PollOnce`）驱动，放弃与
  「io 正在同步写出」不可能并发，故本用例只在动态上验证放弃边界的可观察不变量，
  不能区分未修复实现；F-1 的「放弃决定与 io 域线性化」由源码线性化点证明
  （`HttpClientImpl::Request` 放弃路径在同一 `IoGate` 内 Abort→读阶段→落定，见 §4）。
- S7 兼容消费：既有 HTTP loopback / lifecycle / quota / child_unregister /
  failure_close 回归随本变更更新「已提交后终态」的期望值并保留全部
  配额、排空、反登记断言。

## 后续升级路径

若后续需要把阶段事实带过 `rpc/RpcWire`（跨进程可见），在 wire profile 中新增
字段并单独记录；本决定的前提是「同进程消费 `Error::request_phase` 已足够」。
