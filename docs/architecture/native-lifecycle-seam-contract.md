# Engine 主线程宿主与共享所有权契约

状态：**目标契约**。本文冻结「应用入口 native 主线程直接执行一次 `Engine::Run`、Redis/Mongo/发现/网络组件共用同一 Engine」的所有权与退出语义；原候选中的 `net::EngineLifecycle`（内部 pthread runner + timer 试收割）已被本契约取代并删除，`Engine::CoWaitRunning` 与单一控制门已由 infra-engine 节点落地。framework 侧消费与真实 loopback 由 framework-components 节点验证；本文不把 infra 定向测试当作 framework 已交付。

## 1. 范围与所有权

- 一个应用运行实例恰好一个 `net::Engine`。Engine 由应用根（framework `v2::CoApp`）在主协程内创建并在整个运行期持有；创建只占 Idle 状态，不监听、不建线程。
- 应用在主协程完成全部 **Idle 阶段装配**（所有要求 `Engine Idle` 的 `Create`，包括 HTTP/RPC Server 的 accept handoff 登记、HTTP/RPC Client、Redis 池、发现用 HTTP Client）后，把 `{shared_ptr<Engine>, EngineConfig}` 发布给入口宿主闸门（framework 拥有，进程内单槽位，不是通用管理器）；发布即表示「装配完成，可以 Run」。
- 发布之后，runtime 提交后 native host hook 在**原生 main 线程**上调用一次 `Engine::Run(config)`。`Run` 返回即本代 Engine 的 listener、连接台账、driver 绑定、原始回调与 context 已收口；不存在第二个执行 Run 的线程，也没有 runner/join/`pthread_tryjoin_np`/reaper timer/control 线程。
- 网络宿主、Redis、发现等组件**借用**同一 Engine（`shared_ptr` 只在装配与调用期有效）：只关闭自己拥有的客户端、连接、会话、watcher；不调用 `Run`，不调用共享 Engine 的 `Close(Immediate)`，不在自身 `Close`/`OnStop`/析构中关闭 Engine。
- `Engine::Run` 只允许应用入口的 native host hook 调用；业务协程、Service、Component、模块 `Close` 都不调用它。runtime 不认识 Engine：只提供「提交后 native hook 的执行位置」与「主任务完成通知」，不 include infra，不持有 Engine/runner/future/join。
- 同一应用运行实例不再出现第二个 Engine，也不以 Engine 注册表、按地址自动合并或多 Engine 管理器替代本形态；本条只约束 framework 宿主形态，不禁止不经 framework 的 infra 独立消费者各自持有自己的 Engine。

## 2. 主线程 hook 与两向退出

入口顺序固定为：`setup_runtime`（runtime Start 前）→ `RuntimeMain` Start → 可选提交前同步 hook（短、不挂起，保留）→ 提交主任务 → **提交后 native host hook**（本项目新增）→ 只等「主任务完成 + host hook 返回」。

- **方向 A（主任务结束 → Run 退出）**：主任务正常返回或未捕获异常时，runtime 恰好一次调用 hook 登记的 `OnMainTaskDone` 回调；hook 在回调里对共享 Engine 调用一次 `Close()`（任意线程、非阻塞、幂等），`Run` 的控制循环因此封口并返回。hook 必须在返回前接住 `Run` 的返回值。
- **方向 B（Run 提前失败 → 主任务被唤醒）**：`Run` 在发布 Running 前失败时，Engine 记录 `LastError` 并进入 `Stopped`；主任务在 `CoWaitRunning` 上有界等待，观察 Stopped 即返回该错误（无错误时为 `Closed`），按启动失败收口并返回。hook 不得在未让主任务可观察失败的情况下先行退出。
- hook 等待发布时同时等待「主任务完成」：主任务在发布前失败或提前返回时，hook 不再 Run，直接返回 `0`，让 `MainReturned/MainThrew` 的原退出码生效；此时不存在需要关闭的 Engine。
- **完成恰好一次**：一次 RuntimeMain 运行只发布一个结果。优先级：host hook 返回非零 → `HostHookFailed`（取该返回值）；主任务未捕获异常 → `MainThrew`（1）；否则 `MainReturned`（取主任务 int）。提交前 hook 非零仍为 `HookRejected`，且不再提交主任务。
- 「只等主任务」改为「等主任务与 host hook 两者」：hook 先返回时 runtime 继续等主任务结束（主任务通过方向 B 的失败观察自行收口）；主任务先结束时 runtime 继续等 hook 返回。两部分都结束后才返回 native `main`。禁止用 `result.get()` 前后换位掩盖死等，也不新增线程/condition 轮询替代该等待。`OnMainTaskDone` 登记只活到 hook 返回或抛异常（见 coroutine 契约 §12.2）：hook 提前返回后它不会再被调用，所以「主任务之后完成」只能靠 hook 已设置的共享状态或已发出的 `Close(Immediate)`，不能靠已随 hook 栈失效的回调。
- host hook 是普通 C 函数指针，禁止在 hook 内调用 `bbtco_*`/Co 等待原语、等待 Engine Ready、提交并行任务或阻塞等待尚未完成的 Co 主任务；它只做「等发布 → Run → 接 `Run` 返回 / 等主任务结束」。
- runtime 侧具体类型与宏名见 coroutine `docs/architecture/core-runtime-contract.md` §12.2（`HostHookContext`/`HostHook`/`BBTCO_MAIN_*_HOST_HOOK`）；framework 侧只提供 hook 函数体与宿主闸门，不修改 runtime 契约。

## 3. Co 面：状态与等待

Engine 向受管协程只暴露「运行状态等待」，不暴露 native 控制：

```cpp
namespace bbt::infra::net {
class Engine {
public:
    // 受管协程内等待本代进入 Running；不是 EngineLifecycle，也不是自由函数。
    result<void> CoWaitRunning(const bbt::coroutine::WaitOptions& wait);
};
} // namespace bbt::infra::net
```

- 持久谓词是 `Phase()`：`Running` 返回 ok；`Stopped` 先于 `Running` 出现时返回 `LastError()` 映射的错误（无错误为 `Closed`），不得伪造 Ready；期限必须有限，到期返回 `TimedOut` 且不清任何状态；非受管协程调用返回 `InvalidContext` 且无副作用。
- 等待用既有 `CoWaiter` + 持久谓词，按 `min(deadline, now + 50ms)` 分片，Engine 在 Running/Stopped 发布点通知；不退回 native CV/future/join，不阻塞 worker。
- `Phase()`、`LastError()`、`LastFailure()`、`LocalAddresses()` 保持只读快照语义；`LocalAddresses` 只在 `Running` 后完整，主任务在 `CoWaitRunning` 成功后再读监听端点。
- **删除**：`EngineLifecycle.hpp/.cc`、`EngineLifecycleStatus`、`ControlOwner::Lifecycle`、`RunOwnedByLifecycle`、`SetRunningObserver`、`FinalizeNative` 的 lifecycle 用途，以及 `Engine::Create`/`Run` 注释中的 handle 形态。保留单一控制门：同一 Engine 第二次 `Run`（含并发）在绑定/socket 副作用前返回 `operation_not_permitted`/`device_or_resource_busy`；`Run` 成功后不可重开。
- 旧审查 F1（`Start` 取消时同 mutex 重入死锁）与 F5（pthread 创建后责任发布竞争导致伪 physical_released）都位于被删除的 EngineLifecycle；随删除消失，替代路径必须由保留的 `net.lifecycle_*` 等价用例覆盖（首次 Start/取消/启动失败/Close 竞争/单 worker 活性/真实 Ready→Run 返回），不得删除测试规避。

## 4. 关闭与回收

- 组件关闭只处理自己拥有的对象：`Close(CloseOptions)` 的 `mode`/`wait_release`/`deadline` 含义见 [关闭控制面契约](close-options-contract.md)。共享 Engine 不由任何组件关闭。
- 应用 `CoStop` 只收口组件与业务对象；它在 Engine 仍 Running 时完成，不等待 Engine 物理回收。主任务返回后由 host hook 调用 `Close(Immediate)`，`Run` 返回才表示 Engine 本代网络资源收口。
- 共享 Engine 的物理凭据只有一项：`Run` 已在 host hook 内返回且 `Phase()==Stopped`。`Run` 返回后 Engine 析构只做兜底，不再承担未完成的物理责任；`Run` 返回时仍 `Closing`（解绑失败）按失败保留错误，由宿主报告，不伪报 Stopped。
- 组件的物理回收各自独立：HTTP/RPC/Redis/consul 用各自 `Close(wait_release=true)`（`wait_release=true` 时在同一 `Close` 内消费同一绝对期限），Mongo 用 infra 自有 native finalizer；不得把 Engine 的 `Run` 返回当作它们已排空或已回收。Mongo 的 `wait_release=true` 在 Co 域同样成立：Co 调用挂起等该 native finalizer 的完成信号，join 只在 finalizer 线程执行，不在 Co worker 或组件析构中。
- `Run` 提前失败、主任务异常、hook 失败都保留首因与 owner；同一次停止不换模式、不续预算、不自动重试；下一次显式关闭才可给新的绝对期限。

## 5. 单 Engine 可行性核对结论

- **监听**：`EngineConfig.listen_addresses` 是地址向量，全部由 server 布局的 main reactor 安装；每个 Engine 只有一条 accept handoff 队列（`Engine::EnableAcceptHandoff`，`Run` 前开启一次），因此同一 Engine 只由一个入站协议 owner 消费（`http::Server` 或 `rpc::CoRpcServer`）。**「单 CoAccept 消费面」与「单协议」不是逻辑等价**：它只说明当前宿主恰好只选一个入站协议，证据是选择点本身——`InfraHttpHost::Create` 在 HTTP 与 RPC handler 同时存在时直接返回 `InvalidArgument`（"HTTP and RPC cannot share a listener"），否则按 `if (m_state->http) … else if (m_state->rpc)` 只创建一个 server；`CoApp` 默认装配只装 RPC admission（`InstallRpcAdmission` → `InfraHttpHost::InstallRpcHandler`）。当前不存在「同一 Engine 必须同时承载 HTTP 与 RPC 入站」的必需路径，故共享 Engine 在本宿主形态下可行、不 blocked；若将来出现该必需路径，按 blocked 上报用户，本轮不加 handoff 路由、线程或多 Engine 注册表。本结论也不推广为「禁止所有独立 infra 进程内多 Engine」：不经 framework 的消费者各自持有自己的 Engine 属既有能力，与本契约的「一个应用运行实例一个 Engine」无关。
- **driver 槽**：`slot_count = num_event_loop + (server ? 1 : 0)`，与地址数无关；`num_event_loop=1` 时 server 布局占 2 个 P 槽、client-only 占 1 个。现有示例固定 4 个静态 worker，单 Engine 不会因组件数量增加占槽；只有多 Engine 才会叠加占槽。
- **现有消费者**：`http::Server`/`http::Client`、`rpc::CoRpcServer`/`CoRpcClient`、`redis::CoRedisCli`/`CoRedisPool` 的 `Create` 都接收 `std::shared_ptr<net::Engine>`；`consul::Client` 经专属 `http::Client` 走同一 Engine；`mongo` 不使用 Engine（自有 worker）。因此「同一 Engine 被网络宿主/Redis/发现共用」不需要新接缝。
- **不做的形态**：不为每个组件新建 Engine，不建多 Engine 注册表，不按地址自动合并，不引入 Tick/control 线程；不为「两个入站协议共用一个 Engine」扩 handoff 路由。

## 6. 下游落点与归属（目标，不表示已实现）

- **infra-engine**：删除 `EngineLifecycle.hpp/.cc` 与 CMake/导出；Engine 增加 `CoWaitRunning` 与 Running/Stopped 通知，收敛控制门；迁移 `examples/http_net/main.cc` 等直接 infra 消费；保留并改写 `tests/net/Test_net_lifecycle_seam.cc` 的等价覆盖。命令见派发说明的 VERIFY（target `Test_net_lifecycle_seam Test_net_reactor_driver Test_net_reclaim_receipt Test_rpc_co_lifecycle`，regex `^net\.lifecycle_|^rpc\.net\.co_lifecycle$`）。
- **runtime-hook**：`RuntimeMain` 增加提交后 host hook、`OnMainTaskDone` 恰好一次、两向退出与完成优先级；新增/调整 `Test_runtime_main` 与宏覆盖。
- **framework-components**：宿主闸门、`bbtco_main` 的 host hook 函数体、应用根创建并发布 Engine、组件借用规则；修复 F2/F3/F6 的关闭路径。
- **consumers-docs**：`examples/ecommerce_cluster/discovery.*`、`docs/guides/http-net.md`、`docs/guides/http-host.md` 中旧 handle 叙述与示例迁移。

三仓契约的静态一致性不是产品实现、独立审查或测试通过证据；上述实现与真实定向验证留给对应节点。
