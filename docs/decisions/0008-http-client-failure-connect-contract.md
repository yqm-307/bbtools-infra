# HTTP client 失败收口、resolver 寿命与 connect 串行化契约

契约 ID：`http-client-failure-connect/v1`。

日期：2026-10-03。状态：实现与定向回归已通过；独立审查结论为可提交，PR CI 未完成，
不表示兼容验收通过、也不授权合并。

本文冻结 `HttpClientImpl` / `ClientOp` 在以下三点的内部契约，与
[0005 §6.3/§6.4](0005-co-io-adapter-contract-v1.md)（跨线程同步 Close、返回即物理收口）
和 [0002](0002-co-network-contract-v1.md)（网络关闭语义）一致，不改变公开 HTTP API、
错误分类或消息。对应 Issue #55（F2）、#56（H1）、#57（H2）。

## 1. 失败出口先物理释放再离开 owner 账本（#55 F2）

`ClientOp` 的 owner 账本是 `HttpClientImpl::m_ops`。op 只有在 `finished && inflight == 0`
时才经 `MaybeUnregister → UnregisterOp` 离开该账本；`UnregisterOp` 同时是出站配额的
物理归还点。因此**失败路径必须在该离开当刻之前，先物理释放本 op 自有资源**，否则
owner `Close()` 的快照可能已看不到该 op，socket/body/parser/flat_buffer 只能等 op 析构
释放——违反「不能依赖析构」。

统一收口入口 `ClientOp::FailFinish(result<HttpResponse>)`：在同一 `IoGate` 内先
`Abort()`（关 socket、cancel resolver、`ReleaseBuffers`：serializer → request body →
parser → flat_buffer → endpoints），再 `Finish()`。纳入的失败出口：

1. resolve 失败（`OnResolve` 的 `ec` 分支）；
2. connect（含回退）发起异常（`TryStartConnect` 的 catch）；
3. `OnConnect` 内部写发起异常（serializer/PumpWrite 处的 catch）与设置用户非阻塞态失败；
4. `PumpWrite` 非 EAGAIN 失败（`send` 错误）；
5. `ArmWait` 发起失败（`async_wait` 处的 catch）；
6. `OnIoReady` 错误与泵内异常；
7. `PumpRead` 失败（解析错误 / 读错误 / 对端 FIN 且消息不完整）。

另含 `Begin` 的「已封口/已落定」早退与 resolve 发起异常。成功路径（`PumpRead` 解析完整）
不在本契约范围，保持原有 `socket.close` + `Finish(ok)` 行为。

可核验不变量（测试在 `on_unregister` 当刻直读）：`socket.is_open()==false`、
`request.body().size()==0`、`serializer` 已 reset、反登记与配额归还各恰好一次。

## 2. resolver 后台寿命边界（#56 H1）

`boost::asio::ip::tcp::resolver` 的后台 `getaddrinfo` 由 Asio 解析服务线程执行，
`resolver.cancel()` 返回**不等于**该线程已停止，也无法同步 join。本适配器**不新增
阻塞 join 层**，也不把 cancel 描述成「后台已停止」。

寿命保护：解析完成 handler 捕获 `shared_from_this()`，只要完成项尚未投递，本 op
（含其 `resolver` 成员）就不会析构；`Abort()` 只 cancel / 关 socket，**绝不销毁
resolver**。晚到完成项经 `OnResolve` 的 `!op_armed` 早退，只消费空壳，不触碰已释放的
socket/buffer/owner。故须分别记录两笔事实：

- 本 op 自有可控物理资源（socket、载荷）在 `Close()` 返回时已释放；
- 第三方后台查询是否仍在运行不由本适配器断言。

`Close()` 返回时 `m_ops` 已空（op 逻辑/回调账本落定），但 op 对象可能仍被在途完成
handler 保活——这是有意的安全内部回调状态，不是资源泄漏。

## 3. composed connect 串行化（#57 H2）

不再使用 `Asio::async_connect(socket, results, handler)` 的 range 组合操作：其对每个
endpoint 的 `close/open` 在 socket executor 上异步恢复，不经过本模块入口（不持
`IoGate`），会与跨线程 `Abort/Close` 的 socket 触碰并发。

改为 `ClientOp::TryStartConnect()`：在 `IoGate` 内逐条 `socket.close()` +
单次 `socket.async_connect(endpoint, ...)`，`next_endpoint` 只在
`OnConnect` 收到非 `operation_aborted` 失败且仍有下一条时推进。于是「换 endpoint 的
socket 状态变化」与 owner `Abort/Close` 共享同一把串行域；封口后（`finished`）
不再重开。

错误语义保持既有 range 组合操作：如果单 endpoint 完成时 socket 仍未打开（包括
按 endpoint 协议打开 socket 失败），先将该错误归一为 `operation_aborted`，不继续
回退，并按既有映射报告 `Cancelled`；socket 仍打开的普通连接失败保留原始错误并
按原规则回退下一条。该判断对应 Boost.Asio `impl/connect.hpp` 的
`!socket_.is_open() → operation_aborted` 分支。多 endpoint 语义、其他错误分类与
公开 HTTP API 保持不变。

`resolver` 第三方后台查找的寿命边界归 §2（#56），本票不冒称其已停止。

## 4. 变更面与边界

- 变更仅及 `src/http/HttpClientImpl.*`、直接耦合 HTTP 测试与本文。
- 不迁移 HTTP 到 CoTCP，不改公开 API/错误码/消息，不改 framework/coroutine，
  不恢复 Stop/token/signal/旧等待兼容壳。
- 测试 seam（`io_fault` / `on_resolve` / `on_connect_attempt` /
  `SetEndpointsForTest` / `on_read_armed`）仅供确定性与故障注入，生产路径恒为空。
