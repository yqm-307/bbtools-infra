# gnet 风格网络：执行域、物理线程与所有权

**状态：R2 TCP 契约与实现已合入主线**（PR #84，合入提交 `99d0821a0175c43d9ec2fff78de1f90af068f92b`）。本文描述执行模型与生命周期约束；当前实现落点与已知缺口见第 7 节。旧线程、事件快照及局部验证记录在文末历史区保留，不作为当前实施约束或新版本验收证据。

## 1. 决定与适用范围

infra net 提供稳定可靠、并发安全、有界的 TCP 字节收发，尽可能报告可确认的接纳、进度、本地完成、EOF 和错误。这里的可靠不等于网络永不失败、数据必达或对端业务必执行。

- coroutine 继续负责 Scheduler、Processer、有栈协程，以及内部等待、Timer、Hook、CoPoller 和对象安全；进程寿命、单次初始化、无 Stop/restart 不变。
- infra 自有 main/sub Asio context、listener、socket、连接、缓冲与物理收口；每个 Reactor 绑定一个已登记 Processer driver 槽，不另建网络 I/O 线程。
- 原始入口为异步操作与短回调；协程同步接口是复用 CoWaiter/Chan 等原语的可选薄封装，不另建网络内核。
- 上层负责分帧、请求匹配、等待业务回包、重试、避让、重连及业务关闭策略。net 不因普通等待超时、读 EOF 或写错误自动关闭连接或重发。
- 本次设计覆盖完整 TCP server/client；不包含 HTTP/RPC 接入、消费者迁移、UDP/TLS、动态 owner 迁移或自愈平台。旧模块与原语保留，不因设计确认变成已迁移。
- 依赖保持 `bbt-framework → bbtools-infra → bbtools-coroutine`；不向 coroutine 下沉网络类型、缓冲或业务策略。

## 2. 目标线程拓扑

默认 THREAD 模式仍由 S 线程推进 runtime 内部 EventLoop。P 线程交替执行业务协程与非业务协程边界的 driver；业务协程可迁移 P，连接 I/O owner 固定。

```mermaid
flowchart TB
    Host["宿主 native 控制线程<br/>Engine::Run / 封口 / 同代解绑 / 收口"]
    S["Scheduler S 线程<br/>runtime 内部 CoPoller · Timer · Hook"]
    P0["已登记 P：main driver 槽<br/>main 自有 context · accept · LB<br/>也可执行业务协程"]
    P1["已登记 P：sub driver 槽<br/>sub 自有 context · connect/read/write/close<br/>也可执行业务协程"]
    State["稳定 Conn / operation 状态<br/>并发安全接收 ring · 有界发送序列"]
    Co["业务协程可迁移 P<br/>可选 ICoConnection 成员薄封装<br/>（Read/Write/AsyncWrite/WaitClosed）+ Engine::CoAccept/CoDial"]
    Host -->|"自动绑定；不新增 I/O 线程"| P0
    Host --> P1
    P0 -->|"唯一 fd 交接"| P1
    P1 -->|"提交数据/状态后短回调"| State
    Co -->|"有界命令 / try 消费"| State
    State -->|"既有 Notify；不直接 Resume"| Co
    S -->|"runtime 等待/定时维护"| Co
```

| 角色 | 驱动与责任 | 边界 |
|---|---|---|
| main Reactor | server 普通布局的 listener、accept、LB、fd 唯一交接 | 一个已登记 P 的 driver 槽；不执行业务等待 |
| sub Reactor | 归属连接的 native I/O、注册/撤销、物理 close、自有 context | 各占一个 P 槽；同连接 owner 固定；非阻塞推进有预算 |
| Processer | 在 worker 线程调度业务协程并推进绑定 driver | 不给业务协程提供永久 P/CPU 亲和或实时性保证 |
| Engine::Run | 调用它的非受管、非 owner native 线程上的启动、控制与收口 | 不暗中创建控制线程或第二 I/O driver |
| Conn/operation | 共享数据与持久状态、数据所有权、有界接纳和完成责任 | 不暴露 Asio 对象，不将 shared_ptr 当作对象同步 |

server 普通布局保留一个 main 加 N 个 sub，配置 N 表示 sub 数，启动必须报告包含 main 的实际槽需求。client-only 不创建 accept owner（`EngineConfig::listen_addresses` 为空即为 client-only），ReusePort、UDP 等旧目标不自动加入本次 TCP 实施范围。

Engine 自动枚举并绑定空闲已登记 P；查询空槽不是预留，以注册成功的同代 token 为准。槽不足或竞争失败回滚本次绑定，不抢占、不扩 worker、不静默缩数量、不退回独立网络线程。

### runtime 推进前提

宿主必须在启动配置中明确声明：runtime 内部 EventLoop 由独立于 Run 的执行流持续推进。THREAD 使用已有 S 线程；SCHE_LOOP 的 Start 在另一 native 线程运行；NO_LOOP 由另一执行流持续 LoopOnce。未初始化或未声明/声明不独立，Run 在绑定或建 socket 前拒绝；当前实现口径分别是 `operation_not_supported`（runtime 未初始化）与 `operation_not_permitted`（`runtime_progress != Independent`）。

现有 Scheduler 提供 IsInitialized 和 driver 查询，但没有公开的启动模式/驱动线程存活探测。声明是宿主须履行的前提，不伪称库能证明另一线程持续进展；宿主停止推进后不能承诺协程 deadline 正常兑现。不得后台驱动与手动 tick 同时推进同一 runtime context。

## 3. Asio 与 gnet 的复用边界

`io::GetExecutor()` 仍属于 runtime 内部共享 context，不额外 run/poll/stop/restart，不注册到 infra driver。net 只由绑定 owner 推进自己的 context；解绑后的清理接管另见第 6 节。现有 HTTP/Redis/Mongo/config 等路径不因此迁移。

gnet v2.10.0 是接纳交接、owner、读写泵、缓冲及公平性的参考，不是必须逐方法兼容的模板。其 OnTraffic 在 eventloop 栈直接执行；本方案区分 owner 短原始回调与上层可挂起业务协程，不将 goroutine 当固定 OS 线程。

复用已有 Address、NativeSocket、LoadBalancer、EventRingBuffer、OutboundQueue 和部分读写机制，只补实际缺口。EventRingBuffer 当前是单 owner、非线程安全；需要最小同步访问面，不能直接宣称已有类可跨线程使用。OutboundQueue 合并字节不保留公共操作身份，须补有界记账，不能因此重造通用 Future 平台。

读泵是「有预算的非阻塞读」：单轮最多若干次 `recv(MSG_DONTWAIT)`，遇 EAGAIN 或预算耗尽即返回、由下一次 driver Tick 继续；ring 满则暂停读（`m_read_paused`），消费后由持久通知恢复。Asio 公共 `async_wait` 不等价 native per-fd EPOLLET，因此本实现不提供 edge-triggered 配置项（`Options::edge_triggered_io` 已随 PR #104 删除，见决策 0010）。Wake 必须 noexcept、线程安全、有界、持久记忆早到通知（`detail/WakeHub`），不能单靠可能分配的 post 假定满足。

## 4. 跨域调用与完成时序

下表为当前公共入口的语义（签名以头文件为准）：

| 入口/出口 | 目标语义 |
|---|---|
| OnAccept / OnConnect | 交付稳定连接句柄或明确建连失败；net 不自动创建每连接业务协程 |
| AsyncWrite | 取得不可变数据所有权，全量接纳或零接纳；成功只表示承担发送责任，返回可查询的操作身份 |
| OnWrite | 报真实交内核进度和唯一终态；非终态进度须可区分，不承诺对端收到/执行 |
| OnRead | 只是通知，提示复查同一接收 ring 与持久读状态；可合并，不是包数或数据快照 |
| `Connection` 状态查询 | `ReadEof`/`ReadError`/`ReadableBytes`/`Closed`/`CloseReason`/`LastWriteError`/`WriteAcceptance` 持久可查，通知合并或已发生后仍可观察 |
| RequestClose / OnClose | 显式封该连接并提交物理收口；关闭后通知，不把普通 fd 错误自动解释成该策略 |

同连接接纳次序定义发送次序，一次 AsyncWrite 的字节连续；部分写不插入别的操作字节，多次调用不自动组成业务帧。字节、操作数、命令、在途数据和通知责任统一有界计账，接纳后不能因第二层排队满而丢失完成。

容量满立即明确拒绝且零接纳；`ICoConnection::Write` 可无限或带绝对 deadline 等容量/本地完成，等待名额同样有界。容量等待超时=未提交；已接纳后的等待超时/取消仅结束等待，`WriteReport` 返回操作身份、采样进度及仍在途标记，owner 继续持有和推进，最终状态可查。采样不是冻结前缀，不可据此自动重发后缀。等待业务回包始终归上层。

`WritePhase::Done` 只表示该次发送已到终态，**成功还须 `outcome.error` 为空**（`Done` + 非空 `error` 是失败结算）；`InFlight` 是已接纳未终态，`Rejected` 是未提交，`Unknown` 只来自无效写句柄的 `Query`。`WriteOutcome` 是「回 X 时采样」，不表示调用返回时仍在途。同理，`WaitOptions` 的 deadline 只约束本次等待，不撤销已接纳的 I/O。

真实写错误结算当前操作已发前缀与本轮后续未发操作；结算期间明确拒绝新写，不能跳过失败前缀继续旧队列。结算完且未关闭时允许新的 AsyncWrite，该调用本身表示上层新尝试；不续发旧操作，最近错误持久保留。关闭与结算同序仲裁，错误结算不能重开关闭状态。

CoWaiter 使用现有登记、复查、投递与 Notify 仲裁，登记失败不留下孤儿责任；完成/超时/取消竞争只产生一次等待结果，不等于底层操作已取消。Notify 在所有外层锁之外执行，不直接 Resume、不缓存协程创建线程/TLS。

### Co 入口的执行域 gate 与快速拒绝

全部协程成员入口（`Engine::CoAccept`/`Engine::CoDial` 与 `ICoConnection::Read`/`Write`/`WaitClosed`）在任何消费、接纳、提交或创建等待责任**之前**校验当前执行域，非协程上下文（普通线程、runtime idle、raw owner 回调）统一 `operation_not_permitted`，包括带现成数据、零长度写、已就绪 accept 的快路径。gate 复用上游无创建查询 `GetLocalCoroutineId()`，不构造 `CoObject`、不创建 Processer、不产生网络副作用。

`Engine::CoAccept` 的拒绝优先级本身是契约：执行域非法 → `operation_not_permitted`；已封口/`Closing`/`Stopped` → `operation_canceled`；`Idle`/`Starting` → `operation_not_permitted`；`Running` 未开 handoff → `operation_not_permitted`；`Running` + handoff 但无 main accept owner（client-only）→ `operation_not_supported`。终态判定先于布局判定：`Stopped` 时 main owner 已解绑，但那是收口结果，不得据此误报成「这类 Engine 不支持 accept」。完整错误码矩阵与迁移清单见 [决策 0010「冻结公共面与迁移」](../decisions/0010-gnet-network-foundation.md)。

`Engine::CoDial` 是唯一 Co 建连交付，支持无 `RawHandler` 的 server 与 client-only Engine；配置了 handler 时在任何提交前拒绝。两段式（提交 → 领取）是内部步骤，不公开中间 ID。凡是**已提交**之后未交付就离开的路径（等待对象建立失败、等待名额满、等待返回 `InvalidContext`/`RuntimeUnavailable`、超时/取消）都由内部归还 claim 并让 owner 回收未交付 fd——这是资源回滚，不是对已交付连接的关闭策略；执行域 gate 失败在提交之前发生，没有 claim 需要归还。

## 5. Handler、数据与执行上下文

### 原始回调与业务协程

原始 OnAccept/OnConnect/OnRead/OnWrite/OnClose 在 Reactor owner P 的非业务协程边界执行，短、非阻塞、不可挂起。同连接不重入，不递归触发用户回调；不同 owner 可并发调用共享 handler。回调和 Notify 前释放内部保护；上层自行将业务转发到协程，业务协程可迁移，不自动为每次 readiness 建任务。

### 接收 ring 是唯一真源

- 复用已有 ring 的存储与游标，补最小并发访问；Co 不另建权威收包队列。
- 普通 TryRead 原子地复制并消费；多读者只保证各取线性化前缀，不匹配业务请求。竞争返回 Busy、零消费；空且无终态返回 WouldBlock。所有公开 ring 访问入口 try 获取保护，不让回调/owner 等 OS mutex。
- Peek 与 Consume 必须处在同一次短保护内；不能持保护挂起、调用未知代码或将裸 view 带出。长期持有数据用自有拷贝。
- owner 竞争失败延后；保护释放使用持久重试谓词、先登记再复查，覆盖先释放后登记交错。raw OnRead 遇 Busy 后合并补通知，不在当前栈递归、不依赖未来新字节。`ICoConnection::Read` 可挂起等释放，不 spin。
- 先预留容量再非阻塞读取，临时未提交字节也算入上限；复制失败不前移消费游标。满则暂停读取，不覆盖未消费数据；消费后持久唤醒并主动复查，不依赖下一次边沿。
- 先发布数据/持久读状态，再 OnRead；仅有旧未消费数据不空转通知。EOF/错误即使无新增字节也须可观察。

EOF 只终结读取方向，不自动关闭发送侧。尾部仍可消费，耗尽后读返回 EOF；读错误同时保留已收数据与错误观察面。显式连接关闭或 Engine 收口后，已收数据仍可读，剩余 Conn/ring 的寿命不依赖 socket/context；不把缓冲内字节当成已分帧业务包。

`Engine::CoAccept` 只是 OnAccept 的有界交接封装：先预留容量再 native accept，满则暂停接纳，不无限生成协程或漏 fd；单个等待超时不误关供其它等待者取得的连接。`Engine::CoDial` 等待结束且无接收者的未交付 socket 由 owner 回收，这是资源回滚，不是对已交付连接的自动关闭策略。

## 6. 网络关闭，不是 runtime 停机

Engine::Run 占合法宿主 native 线程：校验前提→自动绑定→安装网络资源→发布 Running→控制循环。RequestClose 并发安全、幂等地封 Listen/Connect/Write 等新业务入口并唤醒控制域；返回只表示登记请求，不同步解绑或等待自己。停止新普通业务回调，已有短回调完成，操作结算/关闭通知仍须兑现。

正常收口顺序：原 owner 撤 listener、取消 connect、结算 handoff、关闭连接并完成既有责任→原 owner 推进取消完成项→控制线程同代同步解绑、等待全部 Tick/Wait/Wake lease→必要时控制域独占清理剩余 context 项→context 存活时拆 socket/backend→销毁 context→发布 Stopped、Run 返回。

RequestClose 是服务封口，Run 返回才确认该 Engine 的网络资源、callback 与 driver 已收口；不等待上层业务协程返回、外部 Conn/数据引用归零或业务回包。外部句柄继续查冻结状态、读已有缓冲，后续新网络操作明确拒绝，不访问已销毁 context。

不提供 Engine Close(deadline) 硬返回时限：同步解绑无 deadline，不捏造可取消解绑。调用者先 RequestClose、等 Run 返回再销毁 Engine，不并发销毁仍被 Run 使用的对象。不建后台 reaper、不 terminate，不以 context.stop 丢 callback 或 runtime 停机伪造资源完成。

### 既有 driver 异常边界

不假设正常 Reactor 必然失败，不新增自愈/迁移或容错平台。普通 fd errno、EOF、协程超时不升级为 driver Failed。

Run 用有限间隔谓词等待，控制唤醒或到期后通过 QueryProcesserDriver 检查持有 token 的各槽，不无限只等 RequestClose，也不依赖不存在的失败 callback。匹配 Failed 时每代记录一次来源/异常并封入口，再同代同步解绑；全部 callback lease 退出后才独占接管旧 context。代次不符或查询失败明确报控制错误，不能改用新 token 解绑他人。

正常 OnWrite/OnClose 由 owner 发出；driver 已失败时控制域不伪调用 owner 回调。替代出口为 Engine 控制域错误报告与可查询 Conn/operation 冻结结果；内部完成订阅在发布状态后锁外唤醒。物理与 callback 未收口不发布 Stopped、不释放 context，保持责任可达；持续 OOM、永久阻塞回调、线程不获调度不承诺有界完成。这里维持既有故障底线，不把假设扩为新需求。

## 7. 当前实现与验证边界

设计依据为 design 期 infra `2cb3ebf60a101bb9cbf93f065db4d41e2130d3cd`、coroutine PR #396 合并提交 `1e9f0e8c25de85dde003def65bb58b90bdac0e0b`；R2 原分析使用的 PR head 为 `8d7b4383c8631f4dccad74df088abe54ca000b87`，不冒充 merge SHA。构建实际消费的 coroutine 版本由 CI 拉取 `coroutine@main` 并在 run 中记录 SHA。

已落地（PR #84 + PR #104，`bbt/infra/net/`）：`detail/ReactorDriver`（真实 Asio context 的 `OnTick/Wait/Wake`、预算、槽绑定/回滚/同代解绑、driver Failed 出口）、`detail/DriverBinder`、`detail/ReceiveRing`（共享并发 ring + 释放代次通知）、`detail/OutboundQueue`（逐操作记账）、`detail/WakeHub`、`Engine`（`Run`/`RequestClose`/`Phase`/`LastError`/`LastFailure`/`LocalAddresses`/`CountConnections`/`HandlerErrorCount`/`ReactorOwnerIds`/`AsyncConnect`/`EnableAcceptHandoff`/`RequestStopAccepting`/`AcceptingStopped`/`CoAccept`/`CoDial`）、`Connection`/`ReclaimReceipt`、`WriteOperation`/`WriteReport`、`RawHandler`、`ICoConnection`/`CoTcpConnection`/`ConnectionResult`/`Buffer`、独立公共 `ReadAccess` 与 `Types.hpp`、13 字段 `Options`。

PR #104 把公共面收口落地：`Connection::State()`、两处内部构造、`AsyncConnectAwaitable`、公开 `WaitConnect`/`WaitAccepted`、公共 `NormalizeOptions` 与 12 个历史 Options 字段被删除或收回内部，测试接缝全部落在 `detail/`；协程消费面改为 `ICoConnection` 成员接口，`CoRead`/`CoWrite`/`CoAccept`/`CoDial` 自由函数删除（`CoConnection.hpp` 只剩聚合注释）。冻结符号清单、删除迁移表与逐条行为契约见 [决策 0010「冻结公共面与迁移」](../decisions/0010-gnet-network-foundation.md) 与 [决策 0012](../decisions/0012-icoconn-member-interface.md)。

验证面：`tests/net/` 的 `Test_net_foundation`、`Test_net_primitives`、`Test_net_allocation`、`net.reactor_driver_*`、`net.tcp_*`、`net.freeze_*`、`net.ico_*`、`net.stop_*`、`net.reclaim_*`、`net_foundation_consumer`、`example_co_net_service_roundtrip`、契约回归 `net.<case>`（`Test_net_contract_regressions` 分发），以及 `net.consumer_coroutine_first_{configure,build,run}` 与 `net.consumer_source_dir_mismatch` 负向配置；CI 的 `build-and-gates` job（`network_gate.sh` step）用 `scripts/ci/check_network_tests.py` 强制注册集与 JUnit 实跑集一致、零跳过。合入后主线 run `39305113509` 的该 job 为 success —— 该 success 属于 PR #84 的形态；PR #104 的 CI 结果以该 PR run 为准。

未覆盖：Long-running/多连接压力与性能门槛、Release/ASAN/TSAN、非 Linux 平台、ReusePort 跨 loop 验收、TLS、高层 UDP（仅编译拒绝入口）、HTTP/RPC/Redis/Mongo/config 消费者迁移、Engine 级 graceful drain。文末旧用例与次数只绑定其当时版本，不能转记为当前通过证据。

本地复现照 README「新 net 构建与消费」执行：只冒烟 + 本次功能与直接耦合测试，全量走 PR CI。性能先建立 raw/Co 吞吐、尾延迟、内存和队列峰值基线，不设未经确认的硬门槛。

## 8. 两仓分工

coroutine 契约真源为其 [核心运行时契约 §11](https://github.com/yqm-307/bbtools-coroutine/blob/1e9f0e8c25de85dde003def65bb58b90bdac0e0b/agent-docs/2026-09-07-core-runtime-contract.md)，该链接固定的是文档修订前基线；配套修改涉及 §11.5 的 owner 原始回调与受保护共享 ring 边界，§11.3–§11.4 锁外调用、callback lease、Failed、同步解绑保持不变。消费以发布后的实际契约及 CI 记录的上游 SHA 为准。

C++17、`bbt::infra::net`、Boost 优先及现有构建目标保持，不新增依赖或通用异步平台。其余模块的迁移是独立任务，不在新 net 范围内顺带进行。

## 8.1 停止接纳与物理回收观察

`Engine::RequestStopAccepting()` 把「停止接纳新业务」与「关闭整个 Engine」分开：任意线程、非阻塞、幂等，本代不重新开放接纳；已有业务连接仍可读写，`CoDial` 不因此被封口。`AcceptingStopped()` 初始为 false，只在本代 listener 真实关闭、handoff 封口、接纳等待者被通知后为真；停止请求本身不是物理停止凭据，真实停止由 owner 关闭 listener（或 driver Failed 后同代同步解绑、控制域关闭 listener）确认。它不证明已 accepted fd、外部连接引用或业务任务已释放；连接回收看 `Connection::Released()`，Engine 最终收口看 `Run` 返回。

- Idle 停止后不允许本代再创建监听，server `Run` 返回 `operation_canceled`；client-only 布局不需监听，不因此禁止启动或拨号。Starting 期间在不能确认 owner 与安装责任收口前，观察面保持未完成；已确认停止后不得再建本代监听。
- Running 停止不封整个 Engine：不关闭已交付连接、不解绑 driver，Engine 仍推进在途读写。停止封口与 `CoAccept`/handoff 授权共门：已授权的 raw `OnAccept` 允许完成，门后不授予新交付；停止后的 `CoAccept` 返回 `operation_canceled` 且 `connection` 无效；未交付队列与晚到交接请求回收。
- 投递失败不等于 fd 已关：停止请求保持可达，由控制域有界节拍重交原 owner；driver Failed 的最终停止依据同步解绑后的显式关闭，不依赖清容器或异步捕获析构。`LocalAddresses` 是本代启动后的绑定快照，不是仍在监听的凭据。
- 对应回归为 `net.stop_*`（真实地址拒连、停止后双向收发、启动前/重复/并发停止、启动竞态、driver Failed、未交付交接回收、停止唤醒 `CoAccept` 等待者）与 `net.reclaim_*` 用例。

`CoDial`/`CoAccept` 的 `ConnectionResult::reclaim`（`net::ReclaimReceipt`，值语义、默认无效）与 `Connection::Released()` 让「一次提交的物理 socket 责任」可观察，替代以逻辑 `Closed()` 或进程计数近似记账的做法：

- 无效句柄（默认构造）：`Valid()=false`、`Released()=true`，没有已提交责任。提交未接纳（执行域、Idle、容量、投递失败等）句柄保持无效。
- 有效句柄 `Released()=false`：本次建连/物理 socket 责任仍存活（排队、建连中、已连接未交付、已交付未物理收口）；`Released()=true` 表示 fd 已真实关闭且 owner 资源责任清理完毕。
- 提交接纳后，无论超时/取消/未交付/Close 竞态，句柄有效并描述本次真实责任；`error` 非空时 `connection` 无效但不遗留不可观察责任。句柄只观察：不等待、不取消、不收回；寿命不延长 socket/Engine，不与 Engine 成环。已交付连接的 `Released()` 与其 `reclaim` 同一记录、同终态。不关心回收的消费者可忽略该字段。
- 内部责任流：共享释放记录（`detail/ReclaimRecord`）在提交前预分配、沿 owner 捕获链传递并随交付转入 `ConnectionState`；标记释放只发生在真实 close 点之后（fd 关闭、`ConnCore` backend close 及析构兜底）；fd 创建前失败即时收口，fd 转交成功仅转移记录不提前标记。控制域兜底：`Run` 收口（含 driver Failed）与 `~Engine` 在同代解绑/控制收口后才对未转移记录补标记——解绑和清空容器本身不证明异步捕获已析构，不能提前发布 Released。

## 8.2 测试进程退出约定与已知限制

当前 runtime 模型没有 Stop/重启/优雅停机：调度线程随进程存活，公共契约即「runtime 进程寿命」。由此有一个**未修复的已知退出限制**：启动 runtime 的测试进程从 main 正常 `return`（或 `exit`）进入静态析构后，runtime 线程仍可能 `PollOnce` 延迟销毁 Event → `CancelListen` → `DelEventCallback` → 对已析构的 `Event::m_callback_map` 执行 `erase`（是 erase 非 find）→ SIGSEGV（GDB27/exit-forensics-24 受控交错复现）。

约定（`tests/net/ProcessRuntimeTestMain.hpp`，测试私有，不是产品 API）：已启动 runtime 的测试 main 与示例一律经 `bbt_net_test::RunTestProcessExit` 收口（`tests/net/Test_net_tcp.cc`、`Test_net_contract_regressions.cc`、`Test_net_reactor_driver.cc`、`Test_net_foundation.cc` 与 `examples/co_net_service/main.cc`）。推荐写法是一 case 一独立进程，顺序固定为：

```
case 作用域 → 业务资源显式 close/join（socket/listener/owner loop、callback/ticket/snapshot 配对、FD/线程回 warm 基线）→ 断言 → 原退出码 → flush C++/C stdio → std::_Exit(code)
```

边界与限制，逐条准确陈述：

- `std::_Exit` 不跑静态析构与 atexit，也不展开当前栈；所以局部对象必须先离开作用域，wrapper 只在原测试函数返回（所有测试局部 RAII 已真实析构）后调用它。业务资源清理与断言保持在 `_Exit` 之前完成，不以 OS 代清作为通过证据。
- 失败仍失败：原非 0 返回不变 0；未捕获异常/terminate/abort/信号/外层 ctest 超时不被转换成功；本头不注册 atexit、不安装/吞掉信号，崩溃/超时保持非 0 并留日志。
- `_Exit` 只是规避测试进程退出竞态：不是 runtime 资源回收证据，不验证全局/静态 RAII，不向生产库推广；普通 `return`/`exit` 对启动 runtime 的进程仍不安全，本节不声称已修复。
- 需要验证普通 return/静态析构/全局资源寿命的用例，须另设明确的生命周期测试，不得套用本 helper 宣称验证通过。
- 跨进程资源（临时文件、外部服务/共享记录/子进程）不依赖 OS 终止自动恢复，须在 case 内清理或由外层监督回收。
- 未启动 runtime 的工具测试（如 `Test_net_primitives`、`Test_net_allocation`）保持普通 return，不用此边界。优雅 runtime 停机仍是独立任务，不在本仓建设。

本地命令示例（独占构建目录，新 configure 必加 ccache）：

```sh
cmake --build build-net --target Test_net_tcp --parallel 2
ctest --test-dir build-net -R '^net\.tcp_raw_roundtrip$' --output-on-failure
```

## 9. 验收场景索引（C1–C14）

以下承接原迁移计划 §5 的编号、场景与证据要求，供逐项追溯，不是第二份施工进度或整体验收结论。原阶段编号仅作历史映射；当前已交付范围和未覆盖项以 §7、[决策 0010](../decisions/0010-gnet-network-foundation.md) 与对应 PR 验收为准。未执行、范围待裁决或已改用其他 API 的场景不能因计划删除而自动通过或取消。

| Criterion | 原阶段 | 保留的可核验场景 |
|---|---|---|
| C1 gnet 抽象/API/Options | P0/P3 | 符号/Option/差异覆盖；不预设连接类树或固定 RunInProcesser；gnet 回调栈与新业务协程的刻意差异明确 |
| C2 ICoObject 环境限制 | P1/P4 | 普通线程、TLS-enabled idle 非法等待副作用前拒绝；受管协程可用，恢复后现查；owner 与协程判定不混用 |
| C3 非挂起标签 | P0/P4 | 新 Co 方法完整路径分类，override 一致，C++17；不以 noexcept/const/inline 推定不挂起 |
| C4 reactor/owner/投递 | P3 | 独立 main/多 sub/context/线程身份、LB/ReusePort、单移交与关闭竞争；无重复驱动 runtime context |
| C5 socket 必需支撑 | P2 | 真实 TCP/UDP/IPv4/IPv6/Unix、选项/flags/dup/失败清理；平台缺口逐项明示 |
| C6 事件/协程语义 | P3/P4 | 原 Handler/Action 顺序及其现行等价行为，业务 handler 挂起不堵 reactor 或 worker，同连接不重入；完成只置 runnable 不直接 Resume |
| C7 缓冲/写完成寿命 | P4 | 半包/粘包/绕环、快照/view、异步保活、partial/慢对端、接受/发送/对端结果区别 |
| C8 Close 资源安全 | P4 | 混合 Block/Unblock、物理完成与 OnClose 分开、在途 callback、晚事件/fd 复用、引用释放、Engine 自有线程回收 |
| C9 有界资源/公平 | P4/P6 | 队列/待收待发边界、高水位恢复、两种读泵预算无饿死、不每 readiness 无限建协程、资源回落 |
| C10 上层完整排除 | P1/P7 | target/CTest/compile_commands/link/selfcheck/CI 依赖无排除模块，残留 prefix 不能悄悄启用；不称旧模块回归 |
| C11 写入围栏 | 全部 | 产品重构只在 infra；原 U1 与 coroutine docs-only 例外分别审查/分支/PR，不改 framework/上层源码/共享服务/规则 |
| C12 example/移植接缝 | P5 | 公共 target 真实 client/server 与轻量帧处理，错误/关闭可见，不凭编译就称运行通过 |
| C13 新鲜压测 | P6 | 冒烟后按固定负载预算，机器汇总与原始证据绑定当前 SHA；未执行/超限/意外 skip 不算通过 |
| C14 PR 可 merge | P7 | 未参与实现者审查、最新 head 必需 CI 绿、无冲突、可合并；不自动 merge。D2 的 coroutine 文档 PR 单独验，不替代 infra 产品 PR |

C5 中高层 UDP 的暂缓裁定不因保留原矩阵改变；底层 datagram 原语与高层 UDP 分开验收。C6 的旧 `EventHandler`/`Action`、C8 的旧关闭细节不能作为恢复已删除 API 的要求：适配现行 `RawHandler`/Co 路径时须说明场景与证据对应，不能以类型改名替代行为验证。ReusePort、平台矩阵、压力和所有关闭交错也不因对应功能存在而视为已验证。

原计划 §7 的文档交付纪律为「infra 文档不单独发布，随本次重构 PR 的 commit 一并交付」，针对的是已合入 #84 的那次产品重构；coroutine 文档 PR 独立收口，不能代替 infra 产品验收。本次是合入后的文档事实纠偏，不把这条一次性安排扩成禁止后续独立文档修正的永久规则，也不声称本次修正补做或替代当时的产品验收。文档图的结构/链接检查与实际渲染证据仍分开，不把未渲染说成已验收视觉效果。

---

<details>
<summary>已删除实现的原理记录（不约束当前 API，不作为验收依据）</summary>

以下两节描述 PR #84 之前的私有 `ConnectionBridge`/`ServerEngine`/`EventHandlerAdapter` 实现。这些类型已在主线删除，代码与测试名不再存在；保留的是仍可复用的**原理与故障边界判断**，不是任务状态或交付证据。

### owner 安全脱离的调用顺序（原理记录）

强制停止后的收口顺序是显式契约，不是「释放桥即随 RAII 自然销毁」的隐含假设：

```
owner 域 StopServiceOwner（封服务 + 物理关闭全部连接，不等待 handler）
  → 控制线程 RequestStop + Join（全部 owner/asio 回调完成；owner 线程回收）
  → DetachOwner（owner 的 io_context 仍存活时逐会话断开 owner 域 backend，
     并释放 registry 强持有；前置核查每会话已物理关闭）
  → owner/context 与 bridge facade 可自然销毁
  → 外部 shell / 活动 handler 继续合法收尾
```

可复用的原理：

- **前置即拒绝**：存在未物理关闭的会话时不发布 detached、不清 registry、不拆 backend，避免半脱离（强拆活 socket、丢 waiter、丢物理收口责任人）。
- **context 先毁不是 UAF**：断开 socket 必须在 owner context 仍存活时完成，此后 registry 清空、context 销毁，残留 shell 的析构不再触碰已销毁的 io_context。
- **析构不在锁下**：registry 与 backend 先交换到局部、再于短锁外销毁；不新增 reaper、不 detach 线程、不要求 owner 活过所有 shell。
- **脱离发布后不再分配**：锁外直接遍历已转移的 registry 释放 backend，避免发布后在受限线程里分配临时列表。

上述原理已由 R2 的 owner 收口实现继承（`Engine::Run` 返回即同代解绑完成）；旧 bridge 的 `Drained()`/`WaitDrained`/`operation_in_progress` 等具体入口不存在。

### 旧公共 Engine 切片（F1–F8 系列，已删除）

2026-10 的 `ServerEngine`/`EventHandler`/`Action` 实现（含 F1/F2 共享计数、F3 Boot 取消、F6 OnBoot 观测、F7 facade 竞态、F8 停止开始授权、一次性控制/关闭故障收尾等切片）随 PR #84 整体替换：`Engine` 现在只有 `Run`/`RequestClose`，没有 Boot 事件、`OnTick`、`Action`、`Engine::Stop` 或 `ServerEngine` 相位机。当时记录的定向用例名（`net.engine_*`、`net.co_bridge_*`）与计数不再存在于仓库，不能引用为当前通过证据。

仍成立的两条故障边界判断：

- **持续资源耗尽不在支持域**：不保证强停/析构完成，不保证等待者被唤醒；不因此新增泄漏兜底、强杀、无限重试或 reaper，也不扩建 runtime 停机模型。
- **不要在控制域伪调 owner 用户回调**：driver Failed 时以 Engine 错误与冻结状态为出口，不通过伪造回调制造「看似完成」。

## 10. F17 R0：native bridge、Co 域与对象迁移边界

本节承接 coroutine 的 process-lifetime/单次初始化和 framework 的 Co-only 业务生命周期。infra 继续拥有 socket、Asio context、Reactor、连接状态、RPC admission、callback lease 与物理回收；coroutine 只提供调度、等待/唤醒、`Deadline` 与 `CoObject` 统计；framework 不直接碰 native owner。本文新增的是消费边界与迁移清单，不代表 R3 已实施。

### 10.1 两个执行域的固定边界

```text
native owner（infra）
  Engine::Run（宿主 native 控制线程）→ Reactor driver（既有 P）→ native OnAccept/OnConnect/
  OnRead/OnWrite/OnClose → socket/context/fd close → Released
              │  有界命令、值结果、持久状态、CoWaiter/Notify
              ▼
受管 Co 域（framework / infra Co facade）
  CoApp::CoInstall/CoBind/CoStart/CoStop
  Engine::CoAccept/CoDial
  ICoConnection::Read/Write/WaitClosed
  rpc::Client::Call/Send 与带 Deadline 的 Co 关闭等待
```

- `Engine::Run(const EngineConfig&) -> std::error_code` 是唯一控制入口：
  宿主 native 控制线程调用，自动绑定、进入控制循环，RequestClose 后返回。
  应用是普通 `main` + 既有 `bbtco` 注册——没有第二 runtime、隐藏 main、
  native 控制桥或库内常驻线程；详见 §10.1.1（已否决方案仅作历史保留）。
- `Engine::RequestClose() noexcept`、`RequestStopAccepting() noexcept`、状态快照和 `Connection::RequestClose()` 是跨域的非挂起控制/观察入口；它们只发布请求或值快照，不执行业务 hook、不等待自身、不证明物理释放。
- `Engine::CoAccept(const WaitOptions&) -> ConnectionResult`、`Engine::CoDial(const Address&, const WaitOptions&) -> ConnectionResult` 以及 `ICoConnection::Read/Write/WaitClosed` 是 Co 域入口。执行域 gate 先于状态读取、容量消费、连接提交和 waiter 登记；非法域返回既有 `operation_not_permitted`/`InvalidContext`，快路径也不能绕过。
- `ICoConnection::AsyncWrite`、`Status()`、`Connection` 的状态查询与 `ReclaimReceipt::{Valid,Released}` 是非挂起值/命令面，仍不取得 native owner 锁的长时间占用；`Write/Read/WaitClosed` 的等待只由 `CoWaiter` 等现有原语承接。
- `OnAccept/OnConnect/OnRead/OnWrite/OnClose` 必须继续在 owner 的 native 非业务协程边界短、非阻塞、不可挂起地执行。回调更新稳定状态并通知 Co waiter，不能直接 `Resume`，不能要求协程回原 P，不能把 raw callback 改成业务 hook。

### 10.1.1 主入口与 native 控制接入（F17 已定型）

已实现面：应用是普通 `main` + 既有 `bbtco` 注册（`bbtco_main()` 宏生成的
主协程入口），coroutine 公共 `TryGetTLSInst`/`GetLocalCoroutineId` 做执行域
判定；`Engine::Run` 是 Engine 的唯一 native 控制入口——宿主线程调用即自动
绑定、进入控制循环、`RequestClose` 后返回。infra 不为 F17 提供
RunMainTask/native 控制桥/`Advance` 步进接口，也不新增常驻线程或第二
runtime；coroutine 公共契约 §12.2 已按同一口径重写，infra 侧不再保留另一套
启动机制作为实现契约或放行条件。

（已否决方案，仅作历史保留，不再作为实现契约：先前 §10.1.1 曾设计
`Engine::BeginNativeControl/AdvanceNativeControl` + `MainEngineControl`
（`IMainNativeControl`/`RegisterMainNativeControl`/`MainNativeWake`/
`MainNativePoll`/`RunMainTask`）把 Engine 控制拆成可非阻塞推进、由主入口
原生等待循环驱动的桥接——该方案依赖 coroutine 的
`TryUnregisterProcesserDriver(Pending)` 拆分与主入口注册槽，已随 F17 定型
整体否决，相关类型与注册/唤醒 API 不再要求实现。Engine 物理回收/有限
deadline/CoWaiter 谓词等约束仍由 §6/§10.2 与
`Engine::Run`/`RequestClose`/`Connection::Released` 承担。）

**单 worker 真实 loopback 的可实施布局**：当前 server 布局
`Engine.cc:2494–2495` 要 main+sub 两槽，`DriverBinder.cc` 在 worker=1 时
会拒绝。F17 代表测试因此固定为 **client-only Engine（单 driver 槽）+
隔离的 POSIX TCP echo peer 子进程**（动态端口经 pipe 报 readiness，无
coroutine runtime）；不是 StubNetHost、mock 响应或新增产品常驻线程。
server 布局原多 worker 测试继续保留；不为本任务合并 Reactor 槽或声称单
worker server 布局已支持。

### 10.2 deadline、Close 与 `Released` 不可合并

`WaitOptions`、RPC `CallOptions`、`SendOptions` 和 framework `LifecycleOptions` 均携带绝对 `bbt::coroutine::Deadline`；下层只取父子预算的最小值，不能在 adapter 层重置为新的完整预算。超时/取消结束调用方等待，不能撤销已接纳的 I/O、强杀 handler、清除 admission/lease、关闭共享 Engine 或把 `Released` 改成 true。

`WaitClosed`/RPC `Client::Close`/`Server::Close(deadline)` 是逻辑状态或有界观察；`Connection::Closed()`、`ICoConnection::Status().closed` 与 `Released()` 分开。只有 owner 已真实关闭 fd/backend、清掉本次责任并发布共享释放记录后，`Connection::Released()` 和 `ReclaimReceipt::Released()` 才为真。有效 receipt 在超时、取消、未交付或握手失败后继续可观察；无效 receipt 的 `Released()==true` 只表示提交前没有责任。物理责任、quota、retiring session、callback lease 未收口时必须保留 owner，不能以句柄清空、map 变空、逻辑 Close 或连接数为零充当证据。

### 10.3 对象/行为 → reuse、adapt、omit

| 对象/行为 | 处理 | 目标名/创建入口 | 所有权与执行域 | 错误/测试映射 |
|---|---|---|---|---|
| `include/bbt/infra/ICoObject.hpp` 与 `bbt::coroutine::ICoObject` | reuse | 不复制类型；继续聚合 coroutine 的 `ICoObject/CoObjectInfo/Deadline/WaitOptions` | 不拥有对象；公共类型由 coroutine 真源提供 | 公开头 selfcheck、`Test_contract_basics`；不新增 infra 同名接口 |
| `rpc::Client::Create`、`rpc::Server::Create` | adapt | 既有 Create 委托 `MakeCoObject<T>(kind,name,creator)` → coroutine ManagerBase 唯一路径，沿用合法 shared_ptr creator/deleter | Client/Server owner 仍由 infra/framework AppNetwork 持有；观测索引只存 weak_ptr，Member 析构通知不保活；业务/回收 owner 不改弱 | 工厂构造/观测登记失败不提交计数；已创建后业务发布失败正常 destroyed；测试见 §10.4 实名映射 |
| `net::Engine` | adapt observation, reuse native execution | 保留 Engine 名与 EnginePhase；`Engine::Run` 是唯一 native 控制入口；R3 创建点统一经 `Engine::Create`（委托 `MakeCoObject<Engine>("infra.net.Engine",name,creator)`），Engine 接 CoObject 身份/Member 实际析构通知 | 保留 native owner 和既有合法 owner/deleter；不继承 CoRuntimeOnly，Run 仍仅 native；物理回收与 G01 不因观测解决 | 创建/实际析构计数一次，RequestClose/Run 返回均不减 alive；观察不保活；现有 net.reactor_driver_*/net.stop_*/net.reclaim_* 另证明原执行/物理语义 |
| `Connection`/`CoTcpConnection` | reuse behavior, omit factory | `CoTcpConnection` 继续是值语义视图；不新增对象 registry | 视图销毁不关连接；owner 兑现 close；Co 方法在受管协程，状态/RequestClose 可观察 | `WaitClosed` 不等 `Released`；`net.ico_*`、`net.reclaim_*` |
| `ReclaimReceipt`/`ConnectionResult` | reuse | 保留 `Valid()`、`Released()` 与 `{error, connection, reclaim}` | receipt 只观察，不等待、不取消、不延长 Engine/连接寿命 | 提交前无效、提交后有效且最终 Released；未交付/握手失败/Close 交错窗口 |
| raw callback 与 native `AsyncConnect` | reuse | 保留 `RawHandler`、`AsyncConnect`；不加 `Co` 前缀 | native owner 短回调；通过状态/通知桥接 Co，不能直接 Resume 或调业务 hook | callback 抛错隔离、投递失败、fd 物理回收；`net.tcp_*`、driver failure |
| `CoAccept`/`CoDial` 与 `ICoConnection` Co 方法 | adapt | 保留现有成员名；统一调用 coroutine context gate 和 `WaitOptions.deadline` | Co 域等待，native owner 持有连接/receipt/物理责任 | `InvalidContext`/`TimedOut`/`Closed`/`Overloaded`；快路径负例与真实 loopback |
| RPC `Call`/`Send` | reuse | `bbt_co_async(result<Response>) Call(...)`、`bbt_co_async(result<void>) Send(...)` | 业务 Co handler 与 admission lease 分层；infra session owner 继续推进 callback/写终态 | `Deadline` 取 min；`OutcomeUnknown` 保留；按 `tests/rpc_net/CMakeLists.txt` 的真实 `rpc.net.v2s_*`、`rpc.net.*` 名称映射，不使用未注册的 `rpc.app_session.*` |
| RPC `Start`/`StopAccepting`/`Drain`/`Close(deadline)` | adapt | native owner 端保留既有语义；framework 只经明确 bridge 调用，不直接从 Co worker 调 native `Run`/Close | `Close(deadline)` 只等本次；超时保留 Server/session/Engine/lease；必要 Co facade 使用现有 CoWaiter 通知，不新建常驻线程 | 逻辑 Close、Drain、callback、写终态、物理 Released 分别断言；旧 `WaitStopped` 不恢复为第二 public 等待入口 |
| `Session.cc` 物理回收 ledger | reuse/adapt | 保留 `ReclaimReceipt`、retiring/handshake/lease 责任链；只把 framework Co 生命周期接到现有收口入口 | session owner 强持到真实 callback/conn `Released`；不由 coroutine 计数替代 | F6/F9/F10 原有上限不重置；`Test_rpc_v2_session` 定向物理窗口 |

`CoObject` 统计覆盖 Client/Server 和有独立创建/销毁寿命的 Engine；native 域不是免观测理由。Client/Server 保留现名用于 native owner/facade，不把 raw callback 或内核一刀切 Co-only；其 Co 业务方法按 §10.1 gate。Connection/CoTcpConnection、receipt、消息/Options、ring 元素和 callback lease 等值/责任记录不强制工厂化，不能把观测统计误报成 fd/任务/物理回收。通用 ManagerBase/MemberBase 放 coroutine，infra 只消费、去旧反射依赖，不为观测恢复 core 依赖；Manager 先销毁/最后引用 native 释放也须安全记账，不用 Co 锁做底层析构通知。
### 10.4 R3 文件、签名与测试映射

| R3 责任 | 最小文件范围 | 固定签名/改动 | 验证映射 |
|---|---|---|---|
| native Engine bridge / 观测 | `bbt/infra/net/Engine.hpp/.cc`、`Options.hpp`、`detail/ReactorDriver.*`、`detail/DriverBinder.*`、`detail/WakeHub.*` 及直接创建点 | 同一 creator/CoObject 接身份和实际析构（`Engine::Create` 委托 `MakeCoObject<Engine>`）；`Engine::Run` 保留为唯一 native 控制入口，RequestClose/StopAccepting/CoAccept/CoDial 保留；不新增 Begin/Advance/MainEngineControl/CoRun/runtime Stop | `net.reactor_driver_*`、`net.stop_*`、`net.tcp_*` 保原断言；R3 另补 Engine 创建/销毁计数及 native 非 Co-only 正例（`net.co_lifecycle`） |
| connection Co view | `bbt/infra/net/Connection.hpp/.cc`、`ICoConnection.hpp`、`CoTcpConnection.hpp/.cc`、`ConnectionResult.hpp`、`detail/ConnectionState.hpp` | 保留 `Read/AsyncWrite/Write/RequestClose/WaitClosed/Status/IsReleased`；`Released` 与 receipt 共用物理记录 | `net.ico_*`、`net.reclaim_*`；Read/Write/WaitClosed 非 Co 负例、deadline 后责任仍在途 |
| RPC consumer | `bbt/infra/rpc/Client.hpp/.cc`、`Server.hpp/.cc`、`Session.cc`、`detail/Session.hpp`、必要 `Admission.hpp`/`Transport.hpp` | 保留 `Call/Send` Co 签名与 `Deadline`；保留 `Server::Close(Deadline)` 物理责任语义；如需 Co-facing owner wrapper，放在明确 bridge，不复制 session ledger | `tests/rpc_net/Test_rpc_v2_session.cc` 的 `rpc.net.v2s_roundtrip`、`rpc.net.v2s_bidirectional`、`rpc.net.v2s_admission_*`、`rpc.net.v2s_close_*`；`Test_rpc_v2_wire.cc` 的 `rpc.net.v2_*`；`Test_rpc_net_lifetime.cc` 的 `rpc.net.client_close`、`rpc.net.server_lifecycle`、`rpc.net.released_client_handle` 等真实注册项 |
| framework v2 单 worker loopback | 拟新增 framework `tests/Test_framework_v2_coapp_loopback.cc`、`tests/CMakeLists.txt` 和直接消费点 | 拟新增 target `Test_framework_v2_coapp_loopback` / CTest `framework.v2_coapp_loopback`；真实 Engine/动态端口，单 worker 装配→启动→请求→CoStop/子对象 Join→实际 Released/计数回基线 | 尚未注册/构建/运行；按 §10.1.1 单槽 client-only Engine + 动态端口 peer 子进程；旧 framework.coapp_lifecycle/StubNetHost 不能充数。断言 worker raw I/O 由既有 P driver 推进、waiter 早到不丢、request≠stop、超时保 owner、解绑 Pending 无强制回收，真实 conn Released 后以析构通知而非 sleep 核统计回基线 |

R3 只能在 coroutine R1/R2 的实际公开头、构建 pin 和 CTest 注册已可消费后开始。**仅 R3** 可更新 `infra-v2-r2`、`framework-v2-r2` 的隔离消费 pin/直接依赖引用，绑定本次 coroutine 候选完整源码指纹（包括未提交改动，不拿 HEAD 冒充），使用各自隔离 build/prefix 真实重建、核公开头及链接路径；不改 `infra-framework-base-v3`、`bbtools-coroutine-pin-206772b` 旧只读依赖、任何主仓和共享安装 prefix，本契约节点不改 pin。F17 主入口已定型为普通 `main` + `bbtco` 注册（§10.1.1）；不在 worker 同步 Run、不新增永久线程或第二 runtime。旧 RPC repair/review 上限、deadline、admission/写终态、Released 与 Close 责任均保持原边界。

CTest 的直接断言映射以当前 `tests/rpc_net/CMakeLists.txt` 为准，不存在 `rpc.app_session.*`：

| 保留的断言责任 | 真实 CTest 名（均非本节点通过证据） |
|---|---|
| admission 在途/拒绝/字节/封口 | `rpc.net.v2s_admission_inflight`、`rpc.net.v2s_admission_reject`、`rpc.net.v2s_admission_bytes`、`rpc.net.v2s_admission_sealed` |
| 写终态延迟/失败关闭/预算 | `rpc.net.v2s_reply_write_deferred`、`rpc.net.v2s_reply_write_fail_close`、`rpc.net.v2s_reply_write_budget` |
| 物理 close / 握手 / 未发布责任 | `rpc.net.v2s_close_released`、`rpc.net.v2s_close_handshake`、`rpc.net.v2s_close_no_publish` |
| quota / 建连预留 / 释放重试 | `rpc.net.v2s_quota_peers`、`rpc.net.v2s_quota_establish_dial`、`rpc.net.v2s_quota_release_retry` |
| wire 前缀/值；lifetime | `rpc.net.v2_framing`、`rpc.net.v2_values`；`rpc.net.client_close`、`rpc.net.server_lifecycle`、`rpc.net.released_client_handle` |

每项仍须核对应源码断言、真实输出和非零筛选，不能把注册存在当行为通过；F2 原 20/20 只证明原前缀目标，不重开旧 finding 或扩大为全 RPC 通过。筛选用 `[.]` 或单转义点；R3 先用 `ctest -N`/`--show-only=json-v1` 核非零且新目标已注册，再在真实新依赖构建内 `ctest -j1 --output-on-failure` 聚焦运行，不用旧 build/旧 pin 结果充数。
