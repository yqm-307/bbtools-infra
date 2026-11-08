# 0012：协程连接成员接口（ICoConnection / CoTcpConnection）

**状态：已合入主线**（PR #104）。本文记录协程消费面的接口取舍；签名与行为以 `bbt/infra/net/` 公共头为准，冻结符号清单与删除迁移表见 [0010「冻结公共面与迁移」](0010-gnet-network-foundation.md)，执行域与生命周期见 [网络执行域设计](../architecture/gnet-network-execution.md)。

## 决定

`ICoConnection` 提供协程成员读写，`CoTcpConnection` 只包装既有 `Connection`（共享值句柄，无第二数据队列/关闭槽）；`Engine` 增加 `CoAccept`/`CoDial` 成员并返回 `ConnectionResult{error, connection, reclaim}`，继续由宿主装配、`Run`、`RequestClose`。`CoRead`/`CoWrite`/`CoAccept`/`CoDial` 自由函数删除，不保留兼容壳。

- **读**：`Read` 等待后把数据复制到调用方 `Buffer` 并消费接收真源；`View` 只提供短时零拷贝只读借用，不进游标、不移交 backing。用户持有的数据不计入 `max_inbound_bytes`（该上限只管库内未交付字节及预留责任）。
- **写**：非挂起提交固定叫 `AsyncWrite`（全量接纳或零接纳，返回 `std::error_code`）；挂起组合等待叫 `Write`，返回单一 `WriteReport{wait_error, outcome, operation}`——等待事实与发送事实两个字段互相不覆盖；`WriteReport` 中的 op 票可保存、查询和等待（`op.Wait`），等待超时/取消不撤销已接纳 I/O。
- **状态观察**：保留聚合既有事实的 `ConnectionStatus`（不新增连接状态机，各字段分方向采样，非原子快照）；`IsClosed`/`IsReleased` 是从 Status 投影的非虚便捷方法，物理名统一 `IsReleased`。**删除 `IsConnected`**——已交付不证明对端可达，半关闭无法压成诚实布尔值。
- **关闭**：连接只有 `RequestClose`（异步意图，第一次 reason 固定），不提供同步 `Close`/`Seal`，不继承 `ICoCloseable`；`WaitClosed` 只等逻辑 `Closed`，不承诺 fd/owner 责任已清、不把 EOF 当 `Closed`。**不设 `SetOnClose` 连接级回调槽**（raw 事件出口是既有 `RawHandler::OnClose`，Engine 构造注入；换绑竞争与 Failed 路径无 owner 回调不支持第二订阅面）。物理回收经 `ReclaimReceipt`/`IsReleased` 观察：`Engine::CoDial` 单一签名连同错误返回本次 receipt，失败拨号仍有观察票。
- **归属**：`CoAccept`/`CoDial` 归 `Engine`（listener/handoff、拨号配额与未交付回收都由 Engine 拥有，不另造连接工厂）。`ConnectionResult::connection` 是 `CoTcpConnection` 值包装。
- **错误体系**：net 保留 `std::error_code`，协议层保留 `result<T>`/`Error`，不新增异常出口或第二套错误体系；只在 adapter 接缝做一次权威映射——共享 `NetToError`（`include/bbt/infra/NetError.hpp`）负责底层基础分类（`NetErrc::Closed→Closed`、资源竞争/结算中→`Overloaded` 等），阶段与可信回复结算归协议 owner；EOF 与 `Busy/WouldBlock/WriteSettling` 不在桥中升级为 `OutcomeUnknown` 或最终传输失败。`backend_category`/`backend_code` 保留原始事实，`domain_code` 用 `net.*`。
- **RPC phase 保真**：`RpcError` 新增 `optional RpcRequestPhase request_phase = 9`（UNSPECIFIED=0 缺省，合法阶段明确值）；存在且合法才写入 `Error.request_phase`，非法值按 `ProtocolError` 拒绝；远端 phase 是远端 operation 的事实，不覆盖本地提交程度判断；缺字段保持 `nullopt`，不补成 `NotStarted`。
- **协议头 Engine 引用**：协议公开头只前置声明 `Engine`，实现文件才 include；拒绝没有新职责的 runtime 抽象。

## 实现约束

- 每个可等待成员先复制 raw/state 强引用到局部；等待回调只捕获 state/op，不捕获 facade `this`，不缓存「上次 op」或常驻 waiter。
- `Read` 保留「Busy 前采释放代次、登记后复查、锁外 Notify」；等待名额沿既有 hub 有界，满额 `no_buffer_space`。
- `Read`/`TryRead(Buffer&)` 在单一线性化点复制全部已发布前缀到调用方 Buffer，再消费 ring；尚在 owner recv_hold 的字节稍后发布，不属于本次读取；尾部未交付前禁止 EOF。
- `RequestClose` 是尽快终止连接责任，不新增 graceful-drain 模式；已完成写保留成功，未完整发送写以 `Done` + 非空 `error` 结算，默认空 reason 不能让未发完写变成功。
- `Status` 先采 `Released` 再采 `Closed`，保证同次返回不出现 `released=true && closed=false`（依赖既有「先发布 `Closed` 后 `Released`」顺序）；仍非完整原子快照，`readable_bytes` 不涵盖未发布的 owner hold。

## 迁移表（旧入口 → 现行入口）

| 旧/草案入口 | 最终入口 | 行为与迁移要求 |
|---|---|---|
| `CoRead(conn, out, options)` | `CoTcpConnection(conn).Read(options)` | 返回 owning `Buffer`；原 out 累加需调用方逐段显式处理，失败零消费 |
| `CoWrite(conn, data, options, outcome, operation)` | `conn.Write(data, options)` | `report.wait_error/outcome/operation` 替代返回值及双出参；保留等容量语义 |
| `AsyncWrite(data, out_op)` | `conn.AsyncWrite(data) -> std::error_code` | 只表示接纳结果；需要等待容量和本地发送完成时使用 `Write` |
| `WaitWrite(conn, op, options)` | `op.Wait(options)` | 等待归操作票；不用连接仍存活才能查终态 |
| 自由 `CoAccept`/`CoDial` | `engine.CoAccept/CoDial -> ConnectionResult` | 每个成员一个签名；普通消费者可忽略 `reclaim` |
| `Closed()/Released()` | `IsClosed()/IsReleased()` | 逻辑终态与物理出账事实，均为 Status 投影 |
| 连接级分散 getter | `Status()` 对应字段 | `IsConnected` 删除；raw `Connection` 维持既有查询 |
| `SetOnClose`/连接级构造回调 | `WaitClosed`；raw 用 `RawHandler::OnClose` | WaitClosed 等逻辑终态；OnClose 是 owner fd-close 通知，均不冒充 Released |
| `Id/LastWriteError/View` | `View` 只提供只读借用，不进入等待语义 | raw 事件关联与短视图约束保留，数据消费统一由 `Read`/`TryRead` 完成 |

## 未交付与开放项

- `infra_net_full` umbrella、Buffer 连续化 helper、backing 池化、Engine graceful drain、shutdown-write/半关闭控制、per-op cancel 在有真实需求时另立提案。
- TLS/DNS/UDP、跨平台、长压、sanitizer 与性能门槛沿各模块未覆盖边界继续，不从 TCP 冒烟推导支持。

读取结果的 ReadOutcome/ReadState 承诺由 [0016：owning 读取重载](0016-owning-read-overloads.md) 取代；旧 raw 复制/借用兼容面仍保留。

AsyncWrite 的票据返回/出参约定已由 [0018：AsyncWrite 只提交](0018-async-write-fire.md) 取代；Write 的等待语义保留。

读取 backing 移交与 `TryAccess`/`ReadAccess` 约定已由 [0019：读取面收敛为 Read 与只读 View](0019-read-view.md) 取代。
