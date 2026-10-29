# net 物理回收观察（ReclaimReceipt / Connection::Released）

面向 HTTP 池等消费者的最小非破坏支撑：让「一次 dial 的物理 socket 责任」可观察，
替代以 `AcceptanceState::Closed()`/进程计数/固定延迟近似记账的做法。本文只描述
net 公共契约与内部责任流；不改变既有 Engine 生命周期模型。

## 公共接口（非破坏增量）

### `net::ReclaimReceipt`（Connection.hpp）

`Engine::CoDial` 返回包（`ConnectionResult::reclaim`）里的物理回收观察句柄，值语义，默认无效：

- 无效句柄（默认构造）：`Valid()=false`、`Released()=true` —— 没有已提交资源责任。
- 有效句柄 `Released()=false`：本次建连/物理 socket 责任仍存活（排队、建连中、
  已连接未交付、或已交付连接尚未物理收口）。
- 有效句柄 `Released()=true`：本次 fd 已真实关闭且 owner 资源责任清理完毕。

约束：句柄只观察——不等待、不取消、不收回；寿命不延长 socket/Engine，不与
Engine 成环，持句柄不阻碍回收。内部共享记录（`ReclaimRecord`）仅在 detail，
布局不出公共面。不是 Future/等待平台。

### `Connection::Released() const noexcept`

- 无效句柄：`true`。
- 有效句柄：仅底层 fd 已物理关闭且 owner 对本连接的资源责任清理后为 `true`。

`Closed()` 保持既有逻辑接纳语义（可能先于物理收口）；`Released()` 不取代
`Closed()`。对 `Engine::CoDial` 交付的连接，`Released()` 与其
`ConnectionResult::reclaim` 的 `Released()` 同一记录、同终态。

### `Engine::CoDial` 返回的回收观察（ConnectionResult.hpp）

```cpp
struct ConnectionResult {
    std::error_code error;
    CoTcpConnection connection;   // 失败时无效，不覆盖调用者原句柄
    ReclaimReceipt reclaim;       // 本次 socket 责任的物理回收观察
};
ConnectionResult Engine::CoDial(const Address&,
                                const bbt::coroutine::WaitOptions& = {});
```

- 每次调用 `reclaim` 描述本次提交：执行域非法/提交未接纳（上下文、Idle、
  容量、投递失败等）句柄保持无效（`Released()=true`，无已提交责任）。
- 提交接纳后，无论后续超时/取消/未交付/Close 竞态，句柄有效并描述本次真实
  物理 socket 责任；`error` 非空时 `connection` 无效，不遗留不可观察的责任
  （最终释放必可达）。
- 不关心回收的消费者可忽略 `reclaim` 字段，门禁与等待首胜语义不变。

## 内部责任流（实现要点）

- 共享释放记录（`detail/ReclaimRecord`，一个原子 bool）在提交前预分配，沿
  owner 捕获链（StartConnectImpl → BeginConnectOnOwner → StartConnectSocketOnOwner
  → AdoptFdOnOwner）传递；交付成功后原样随连接转入 `ConnectionState`。
- 标记释放只发生在真实 close 点之后：`NativeSocket::Close()`/`FdHandle::Close()`/
  asio descriptor `close()`/`::close()`（assign 失败兜底）、`ConnCore::ShutdownOwner`
  /`CancelControl` 的 backend close，以及 `~ConnCore` 的显式兜底 close。
  fd 创建前失败即时收口；fd 转交成功仅转移记录，不提前标记；排队/取消的 owner
  稍后创建 fd 时记录不会被提前置位。
- 控制域兜底：connect op 保留记录副本；op 擦表条件排除「未转移且未释放」的
  记录，`Run` 收口（含 driver Failed 路径）与 `~Engine` 在 unbind/FinalizeControl
  之后才由 `SettleAbandonedConnectReclaim()` 对未转移记录补标记。
  `FinalizeControl` 必须先显式关闭 pending descriptor 和交付定时器捕获的 holder；
  解绑和清空容器本身不证明异步捕获已析构，不能提前发布 Released。
- 交付延迟/提交屏障等既有私有测试接缝语义不变；`SetConnectDeliveryDelay`
  窗口内 fd 责任由 holder 与记录同行，取消/失败关闭 fd 后仍可观察释放。

## 验证口径

`tests/net/reclaim_receipt.cc`（`net.reclaim_*`）覆盖：默认无效句柄、成功交付
后关闭的 fd 真实收回与续拨号、未交付窗口（服务端真实 accept 证明已建立）与
回收可观察、真实拒连、提交前拒绝（上下文/投递失败/容量）、物理 close 先于
Released（ShutdownOwner 屏障）、Engine seal 与 driver Failed 的收口；其中
`driver_failed_live_fd` 在真实 TCP 建立后触发 Failed，逐轮以 fd/inode 验证
首次观察到 Released 时原 socket 已关闭（32 轮），不以计数或进程退出代替。
回归基线：net.b4_/net.freeze_/net.r2_/append/consumer 选集（见交付报告）。
