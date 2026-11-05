# 0015：立即关闭（Close）与优雅退出（Shutdown）的两种语义

- 状态：**已定契约**，随本次实现进入公共头与测试。
- 适用：`bbtools-infra` 的 `http::Client`、`http::Server`、`rpc::Client`、`rpc::Server`
  与 transport adapter，以及 `bbt-framework` 的 `InfraHttpHost` / `HostLifecycle`。
- 取代关系：本文取代 `0014-app-session.md` 中「`Close(deadline)` 单次调用即完成
  Seal + 物理收口等待」的生命周期表述，以及旧 RPC 文档「关闭必须按
  `StopAccepting → Drain → Close → WaitStopped` 四段」的唯一路径表述；不改变
  0014 的会话、寻址、配额与准入决策。
- 依据：`docs/guides/http-net.md`、`docs/guides/rpc-net.md`、
  `docs/architecture/gnet-network-execution.md`、`bbt-framework/docs/guides/http-host.md`。

## 问题

原公共面把两类不同承诺混在一个入口：`Close()` 语义上「立即」，实现上却可能
等 5 秒或等排空；RPC 侧 `Close(deadline)` 又把封口与物理收口绑在一起。调用方
无法区分「现在停、在途操作立刻失败」与「不再接新、让已接的正常做完」，也无法
在关闭原因、超时和关闭打断之间做可观察的判定。

## 决策

1. **两个入口，两个承诺；状态单向前 `Open → Draining → Closed`**
   - `Shutdown(deadline)`：置 Draining（与接纳同一把锁拒新）；已接纳任务可
     继续其必要 I/O 与回复；等待本对象承诺范围内的真实排空。有限绝对期限
     超时如实返回 `TimedOut` 且仍拒新，不恢复服务、不隐式转 Close。
   - `Close(reason)`：立即置 Closed，令本对象管理的尚未终结操作失败并
     发布关闭原因，唤醒已登记的排空等待；不等待任意业务 handler 完成。
   原因类型复用 `bbt::infra::Error`，不新增取消框架；首个原因固定，`Close`
   幂等（「失败在途」恰好执行一次，Draining 下同样执行，与 `Shutdown` 不
   共用同一关闭位）。重复 `Shutdown`（仍 Draining 且有在途）返回
   `Overloaded` 表示 Busy；已 Closed 时 `Shutdown` 返回首个关闭原因。

2. **完成与关闭的仲裁唯一；排空只看 inflight**
   先完成者胜：`Shutdown` 观察到排空已成立即返回 `ok`，不被随后 `Close`
   追改；等待中被 `Close` 且仍有在途则返回其关闭原因。排空谓词只读
   逻辑在途计数（连接池在途借用+排队、server inflight、RPC admission/
   写责任、owner 已接纳操作计数），不读条目 phase/claimed 等物理状态，
   也不保存「到达时是否已排空」快照。已提交且无可信回复的 RPC 结果保持
   `OutcomeUnknown` 与 `request_phase`，关闭原因只作为可观察补充，不把
   code 强覆成 `Closed` 而丢失「结果未知」事实。

3. **逻辑失败与物理 Released 分离**
   `Retiring` 只是逻辑退役，仍按真实回收凭据出账；物理收口仍由
   `Close()` + `WaitStopped(deadline)` 证明，与 `Shutdown` 返回值无关。

4. **关闭原因只在调用方可见的返回点统一附加**
   `Call`/`Send`/`Get` 等返回失败且对象已 Closed 时，把首个关闭原因附到
   `Error.details`（`close_reason`、`close_reason_code` 等）；已提交者保持
   `OutcomeUnknown`+`request_phase` 只加详情，成功结果不改。内部各失败
   出口不分别补原因。

5. **关闭不撤销物理副作用，也不强杀业务**
   立即只承诺及时发布状态/失败与唤醒，不承诺所有协程已运行。任意 C++ handler
   或第三方阻塞调用不可强杀；关闭不撤销远端副作用。已提交未知结果按既有
   `request_phase` / `OutcomeUnknown` 表达，不谎报未执行。

6. **宿主线程收口**
   `InfraHttpHost::Close(reason)` 只做「封业务门 → 发布原因 → 封 server/client →
   `RequestClose()` → 允许的短同步 join」。当 Engine 的 owner 收口仍排在业务
   handler 之后时，不在 `Close` 里等业务完成；join 责任留给析构（必须 join，不
   detach，也不能因仍 joinable 的 `std::thread` terminate）。handler 闭包只弱捕获
   分发端口，不捕获可析构宿主的裸引用。

7. **编排接线**
   `HostLifecycle` 以 `INetworkHost::CloseReason()` 是否存在这一明确状态识别
   「排空被 Close 打断」（不按错误码猜）；打断时不再 park：跳过
   `on_handlers_drained`（业务对象可能仍被借用），走网络 Close 与 `on_release`，
   记录失败并返回 `kExitShutdownFailed`；若曾耗尽关闭预算，则按
   `kExitShutdownLate` 报告迟到收尾。

## 未覆盖与能力边界

- `rpc::Client` v2 会话 owner 的 `Shutdown` 使用 owner 已接纳操作计数；逐 peer
  的独立排空观测仍不在本轮范围。
- 仅实现 envelope `Exchange` 的外来 transport 没有排空谓词时，
  `Client::Shutdown` 返回 `UnsupportedRoute`，是明确的能力边界而非成功。
- `net::Engine` 与 coroutine runtime 不在本决策内：Engine 仍是「`RequestClose()`
  登记、`Run` 返回才确认资源/回调/driver 收口」。
