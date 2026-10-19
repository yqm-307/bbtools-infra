# 0010：gnet 网络基础与 R2 TCP 契约

**状态：R2 核心契约已获用户确认；产品实现与两仓集成尚未验收。** 当前执行模型及生命周期真源见 [网络执行域设计](../architecture/gnet-network-execution.md)，后续接续见 [迁移计划](../plans/gnet-network-migration.md)。旧逐符号表保留在历史区，不再以 gnet 签名强制约束新 API。

## 当前范围与版本

- 本次固定源码依据：infra `2cb3ebf60a101bb9cbf93f065db4d41e2130d3cd`；coroutine driver PR #396 的 merge commit `1e9f0e8c25de85dde003def65bb58b90bdac0e0b`。设计最初读取的 PR head `8d7b4383c8631f4dccad74df088abe54ca000b87` 与合并提交分开记录。
- 该版本具备通用 Processer driver 支撑，不等于 infra 已消费或已验证。现有构建 pin 不在本次文档修改中升级；后续须核实际源码、契约和链接 SHA。
- 目标是完整 TCP 底座：Reactor adapter、Engine、连接、接纳/建连、收发、背压及协程薄封装。上层 HTTP/RPC 接入、消费者切换、UDP/TLS、自愈/动态迁移不在本次范围；旧能力保留，不按文档更新删除。
- 继续使用 C++17、`bbt::infra::net`、既有 `bbt::infra_net` 与 network-only 构建，不新增依赖、兼容别名壳或第二 runtime。公共网络接口允许重设计，不承诺旧 API/ABI 兼容；迁移消费者须另行授权。
- 新 net 的 R2 目标替代本决策旧的独立网络线程、受管事件快照、FIN 自动关闭和旧 Close(opt) 要求；0002/0005 的既有模块契约不因此被整体改写。

## 当前核心契约

| 主题 | R2 决定 |
|---|---|
| 边界 | net 负责可靠、并发安全的 TCP 字节收发与准确状态；分帧、请求/响应匹配、业务回包、重试/重连/关闭策略归上层 |
| driver / owner | 自有 Asio context，各 Reactor 自动绑定一个空闲已登记 P 槽；槽不足回滚，不抢占、扩 worker、缩数量或回退网络线程 |
| 布局 | 普通 server 一个 main + N 个 sub，N 是 sub 数，启动报告全部槽需求；client-only 无 accept main；P 仍可执行业务协程，连接 owner 固定 |
| 原始回调 | OnAccept/OnConnect/OnRead/OnWrite/OnClose 在 owner 的非业务协程边界短、非阻塞、不可挂起地运行；同连接不重入，不锁内/递归调用用户代码 |
| 同步接口 | CoRead/CoWrite/Accept/Dial 复用同一异步能力和既有 CoWaiter/Chan；不自动为每条连接建业务协程，不建第二收包内核 |
| 接收真源 | 并发安全接收 ring 保持 TCP 顺序及未消费数据；OnRead 只是可合并通知，EOF/错误持久可查询，不作为业务包边界 |
| 读取与 view | TryRead 原子复制/消费；竞争 Busy、空且无终态 WouldBlock，均零消费；组合 Peek/Consume 在短保护内，不跨挂起/外借 view；保护释放持久通知、登记后复查，不阻塞 P |
| 缓冲寿命 | 显式连接关闭/Engine 收口后已收数据仍可读；Conn/ring/操作结果独立于 socket/context 存活，无强环、UAF 或虚假 drain |
| AsyncWrite 接纳 | 所有权转移后全量接纳或零接纳；操作身份、容量与通知责任一起预留，有界字节/操作数/命令；同次写字节连续，多次写不自动组成一帧 |
| 发送完成 | 提交成功、本地交内核完成、对端收到/执行严格分开；OnWrite 报可确认的已发前缀、实际错误和唯一终态，不宣称不可观测的业务成功 |
| 写错误后继续 | 先冻结/结算本轮已接纳操作，期间新写明确零接纳；结算后未关闭则允许上层新 AsyncWrite 作为新尝试，不自动续发旧操作，保留最近错误；关闭状态不可重开 |
| 协程超时/取消 | 容量等待超时=未提交；已接纳后只结束等待，操作仍在途，返回身份/采样进度并可查最终结果；不自动关连接、撤字节或重发，不用采样进度推导安全重发 |
| EOF / fd 错误 | EOF 仅结束读方向，保留尾部与错误，不自动关闭发送侧；普通 errno 不升级为 driver Failed；上层决定连接策略 |
| 有界背压 | ring 满暂停读取、消费后主动恢复；发送满立即拒绝或由 Co 有界登记等待；accept 先预留 handoff 容量，满则暂停，不丢 fd/完成 |
| Engine 生命周期 | Run 占合法宿主控制线程；宿主声明 runtime loop 独立持续推进，非法前提在绑定/socket 副作用前拒绝；RequestClose 仅封口，Run 返回才确认网络资源/回调/解绑收口 |
| 关闭约束 | owner 正常收口→控制域同代同步解绑→必要时独占清理→context 存活时拆 backend→销毁 context；不等上层业务任务/外部句柄归零，不承诺硬 deadline、不建 reaper/Stop |
| driver Failed | 控制域有限间隔查询所持 token，封口并同代解绑后清理；不在控制域伪调 owner 回调，以 Engine 错误与冻结状态/内部完成订阅为替代出口；不做自动迁移/重绑 |

上表的接口名和状态名表达目标职责，不是已编译的公共头文件声明；精确签名由实现阶段按既定契约落地，不用 stub 冒充交付。

## 复用、兼容与验证

复用现有 EventRingBuffer 存储/游标并补最小同步访问面；其当前类本身仍是非线程安全的。复用 OutboundQueue 的 FIFO/部分发送并补逐操作边界记账，不能把字节合并后失去操作归属。Address/NativeSocket/LB/StreamConnection 的有效能力优先保留，不借机重写基础算法。

现有 EventLoop 自建线程、ConnectionBridge 事件快照、受管 OnOpen/OnTraffic、Engine::Stop/旧 CloseMode 等是旧实现事实，不再是新目标，也没有因本文自动改成 R2。旧局部测试次数和旧依赖消费证明只绑定当时版本。新 driver/原始回调/共享 ring/发送完成/Run 必须补真实验证后才能升格为已交付；精确验证形状见执行域文档第 7 节。

两仓配套对齐 coroutine 核心契约 §11.5 的消费者责任，不改 §11.3–§11.4 的锁外 callback、全租约、Failed 与同代同步解绑。当前本地文档一致性不代表已发布，未来消费记录真实发布契约和链接 SHA。本轮不改代码/构建 pin/测试/CI，不授权提交、推送、消费者迁移或发布。

## 参考与许可

固定参考 gnet v2.10.0 `58b59a11350707153f8c48369693ab9beba7b2fa`；仅作机制/行为比较，不承诺逐方法迁移、性能等价或所有平台可用。现有出处及许可约束继续见 [gnet 移植出处](../dependencies/gnet-2.10.0.provenance.md)，不因重设计删除版权义务。本次不移植新第三方代码。

---

<details>
<summary>旧迁移候选与验证记录（历史，不作为 R2 约束）</summary>

下文原样保留此前逐符号/Options/Close(opt)/FIN 收口方案与局部验证。旧“冻结”“必须实现”“本轮”“未合并”仅属于当时版本；与上文冲突时不再适用于 R2。高层 UDP 暂缓、现存原语能力与历史消费证据仍须分别理解，不从旧清单自动扩大本轮范围。

## 范围与版本

- infra 基线：`3136d1de5031a2eec55475e770d5ebbadc3c32da`。
- coroutine 消费：`8ae6a88997c5acaa4fe03430c35e1820ed5b29b0`（[PR #392](https://github.com/yqm-307/bbtools-coroutine/pull/392) 的 mergeCommit，tree `89c5012ca22d06a226727943a89dd7f08548b8cc`；其父 `6dfda8158d76f5030cde50a9fbc3c97a8335e932` 即[最小查询修复 PR #388](https://github.com/yqm-307/bbtools-coroutine/pull/388) 的 mergeCommit，两项均已合并）；消费标识是落地 merge commit + 其 tree，不用 PR head `a20a6d7b…` 或可变 main；不新增 `bbt_core` 外链。原 `39fdfff71a8a66d98a874b2e71408f9936b1166b` 的 TLS-enabled idle 隐式创建反例保留为修复前证据。
- gnet 参考：v2.10.0，`58b59a11350707153f8c48369693ab9beba7b2fa`。
- 新代码根 `bbt/infra/net/`，命名空间 `bbt::infra::net`，C++17，头/实现共置；不建立 CoTCP/CoUDP 兼容壳，不预设连接子类树。
- 新公开目标 `bbt::infra_net`；`BBT_INFRA_NETWORK_ONLY=ON` 在注册任何旧目标/依赖之前分流。旧模式没有变成新交付；数据库、HTTP、RPC、config、MCP 与旧 transport 的源码保留但不参与此配置。
- 本决策只对新 net 契约替代 0002 的身份-only 使用方式及旧同步 Close、0005 的旧网络执行模型；旧模块本包不改写，继续由旧目标表达，不把它们的状态当成新 net 状态。
- **`Engine::Run` 接受集收窄为 IPv4（2026-10-17 150）**：当前接纳路径只服务 IPv4 stream（`StreamConnection::Create` 家族门），因此 Unix/IPv6 等**合法**但未支持的家族在 owner 线程/listener socket/OnBoot 登记等**任何副作用之前**返回 `operation_not_supported`。旧实现是**假成功**（`Run(unix)` 返回 0、bind 成功并发布 bound 快照，却零业务事件：`OnOpen/OnTraffic/OnClose` 全 0，146 探针 2.1 实证），本修订消除该假成功；空/无效 `Address` 仍 `invalid_argument`，域门禁与 busy/canceled 优先级不变，拒绝后可换 IPv4 地址重试。这是**收窄公共接受集**的行为变更，已获用户确认；不改变上述 UDP 暂缓条目与本决策的 TCP/UDP/IPv4/IPv6/Unix 目标覆盖矩阵（目标能力未变，只是未支持家族不再以假成功进入）。
- **高层 UDP 本轮暂缓（2026-10-17 用户裁定）**：不实现高层 UDP（`Engine`/`Client`/`Options` 都没有 datagram 参数或入口），只留一个显式的**使用时编译拒绝** `<bbt/infra/net/Udp.hpp>`（全部内容就是 `#error "UDP is not supported yet / UDP 暂不支持…"`；不声明任何类/函数/别名/模板，故不是 API），该头不进聚合头与头自检。因此 UDP **不是本轮 P 阶段的必过阻塞项**，下文 TCP/UDP/IPv4/IPv6/Unix 的目标覆盖矩阵**保留不改**；将来实现高层 UDP 时删除该拒绝并按本文目标语义（报文边界/截断/零长、完成项与错误传递、Wake 与事件位、预算与背压、家族门）新增真实入口。`NativeSocket` 的 datagram 原语（`SocketType::Datagram`/`SendTo`/`ReceiveFrom`、零长、truncation）属原语层承诺、不在高层承诺内、**不是**「UDP 整体未实现」；旧模块 `bbt::infra_udp` 的 `CoUDP`（`include/bbt/infra/CoUDP.hpp`）是独立已交付实现，不属于本暂缓。

## 覆盖标记

**F**：本包代码；**D3/D4**：后续包必须实现（P3/P4，不是可用 API）；**R**：固定参考本不支持；**U**：平台/运行机制未验证。下表的目标名称是契约约束，只有 F 行可由本包消费者链接。未提供实现的名称不创建 stub。

### 核心接口与运行机制

| gnet 符号/机制 | C++ 迁移目标与差异 | 状态/场景 |
|---|---|---|
| Run、Rotate | Engine 生命周期入口，单/多地址监听；启动失败清理已创建资源 | D3：部分启动失败、重复启动 |
| 全局 Stop、Engine.Stop | Engine 实例强制停止服务，关闭后拒绝新业务，不等待用户回调/drain；资源随所有权自然销毁，不停止 coroutine runtime，不建全局隐式 registry | D3/D4：拒绝与在途结算、残留句柄安全、自然销毁无悬空/环泄漏 |
| Engine.Validate、CountConnections | 显式错误及 owner 连接索引计数 | D3 |
| Engine.Register、RegisteredResult | 显式 Address 或唯一 socket 所有权转移；既有 CoWaiter 交结果，不移植 Go channel | D3：投递失败不丢 fd |
| NewNetConnContext、FromNetConnContext、NewNetAddrContext、FromNetAddrContext | Go context 注入替换为类型化 Register/Enroll 参数，不复制 string-key context 协议 | D3 |
| Engine.Dup、DupListener | listener 的独立 RAII fd 副本；单 listener 限制保留于 Dup，多地址由明确地址选择 | D3；底层 Dup 为 F |
| EventLoop.Register、Enroll | 选择 owner 后一次移交；Enroll 消费唯一所有权而非暗中复制外部连接 | D3：关闭竞争、重复移交 |
| EventLoop.Execute、Runnable、RunnableFunc | 非挂起 owner 命令；用户任意函数不因包装就获得非挂起标签 | D3：有界命令队列、拒绝可见 |
| EventLoop.Schedule | 声明旁明确未支持；不把声明当实现，不创造假成功入口 | R：gnet.go:411–415 |
| EventLoop.Close | 必须处于该 owner 域，内部物理收口；不同于业务 Close(opt) | D3/D4 |
| Conn 组合 Reader/Writer/Socket | 一种 Conn 组合能力，协议差异由明确 UnsupportedOperation 表达，不强制 TCP/UDP 子类树 | D3/D4 |
| Context、SetContext、SafeContext、SetSafeContext | 单事件位值与线程安全快照分开；不把 shared_ptr 等同于对象同步 | D3 |
| EventLoop、LocalAddr、RemoteAddr | owner 句柄及地址值快照，不传播第三方对象或借用可变地址 | D3；Address 值基础 F |
| Wake、CloseWithCallback、Close | Wake 只排 OnTraffic；CloseWithCallback 为非挂起完成项；Close 必须显式 opt | D3/D4 |
| SetDeadline、SetReadDeadline、SetWriteDeadline | 固定 Unix 参考均返回 ErrUnsupportedOp（connection_unix.go:554–563）；不把 net.Conn 签名当真实支持。Co 等待用独立 WaitOptions，不默默改 Reader/Writer 为网络等待 | R；如需新增 deadline，D4 单列差异 |
| NewClient、Start、Stop | client 复用 Engine/owner，不复制第二套 runtime；Engine 强制停止服务不以 Shutdown/事件 drain 通知完成为前置 | D3/D4 |
| Client.Dial、DialContext、Enroll、EnrollContext | 非阻塞 connect + owner 接纳；上下文值与 deadline 参数分开；挂起等待仅受管协程 | D3/D4；connect 基础 F |
| EventHandler、BuiltinEventEngine | OnBoot/OnOpen/OnTraffic/OnClose/OnTick/OnShutdownRequest 的默认空实现；业务事件可挂起但必须序列化。`OnShutdown`（已关闭生命周期通知）本仓**未交付**、不造 stub；`Action::Shutdown` 的向上请求上报口是 `OnShutdownRequest`（见下 Action 行） | D3 |
| Action.None/Close/Shutdown | None 不变。**Close** 保持「收口只执行一次；关闭首胜后晚回调不能重新 Open」；**Shutdown 是向上请求、不含任何自行动作**：发生它的连接不封口、不关闭，Engine 不封门、不停 ticker、不改 stopping_/phase_、不 Stop、不拒绝后续业务；一次真实返回上报一次 `EventHandler::OnShutdownRequest`，**可重复、无首胜**，是否停止与何时停止由使用者在自己的控制域决定（可完全忽略）。关闭引擎仍只能用既有显式 `Engine::Stop()`/析构 | D3/D4 |
| main/sub reactor、连接 owner | 非 ReusePort stream 的 main 与各 sub 分别使用自有 OS 线程和自有 Asio context；接纳后按 LB 唯一交接；业务在 runtime Processer 协程，不是 fd owner，也不重复驱动 runtime 共享 context | D3：独立 main、多 sub；ReusePort/UDP/client 分别布局，见执行域设计 |
| LB.RoundRobin/LeastConnections/SourceAddrHash | 三算法保留；跨域 Register 串行化 LB 状态，修正参考 RoundRobin 的并发警告，不复现 data race | D3 |
| ReusePort 与普通接纳 | 两种接纳模式分别验证；Linux IP 才承诺候选，Unix 明确拒绝复用端口 | D3；底层 IP 选项 F，其他 OS U |
| LT/ET、EdgeTriggeredIOChunk | 作为 stream 的读泵策略分开；ET 风格读到 EAGAIN，预算耗尽向同 owner 续投；高水位暂停后显式恢复；UDP 不套用 stream ET。Asio 公共 async_wait 不提供逐 fd EPOLLET 开关，不承诺 native LT/ET 事件计数等价 | D3/P6：首后端用现有 Asio 能力，上层策略/公平/无饿死仍待接线实测 |
| ticker/OnTick | engine 全局单 ticker，序列化每次事件；返回 delay 明确下一次投递，关闭后取消 timer、排空通知 | D3 |
| inbound elastic ring、outbound ring+list | 原参考 WriteBufferCap 不是总量上限；新实现明确总待发/待收/排队事件资源界，满时可见背压，不静默丢数据 | D4 |
| buffer/pool/连接索引 | 优先 Boost/既有容器与 RAII；必要才移植最小 ring；单 owner 管 mutable buffer，跨协程持有 snapshot | D4：绕环、慢消费者、归还 |
| 跨域注册/执行/唤醒 | owner 命令与业务事件分层；等待登记后投递复用 CoWaiter.WaitWithCallback，早到 Notify 不丢失 | D3/D4；本包不旁造 Yield/调度器 |
| 特殊 netpoll 构建分支/Windows/BSD/macOS | 本包 Linux/WSL 原生 fd 基础；不从 Boost/gnet 支持名单推定本仓支持 | U |

### Reader/Writer/Socket 逐方法

| 固定参考方法 | 迁移语义 | 状态 |
|---|---|---|
| Reader.Read、Next、Peek、Discard、InboundBuffered | 只消费当前事件快照/已就绪数据，不等网络；不足返回明确 short-buffer；Next/Peek 不可越过本事件/使视图失效的操作 | D4 |
| Reader.WriteTo | 目标 sink 可执行任意代码，不能自动贴非挂起标签；范围/异常与部分进展显式 | D4 |
| Writer.Write、Writev、Flush、OutboundBuffered | 进展与缓冲接纳分开；Flush 尝试非阻塞进展，不把一次 Flush 写成全部已发送 | D4；raw Send/Sendv F |
| Writer.ReadFrom | source 可能阻塞/挂起，需独立执行契约，不因 Writer 名字标非挂起 | D4 |
| SendTo | 数据报独立边界，stream 拒绝；无自动拆包/等待网络 | D4；raw SendTo/ReceiveFrom F |
| AsyncWrite、AsyncWritev、AsyncCallback | 复制/取得不可变所有权后入有界命令队列；完成只表示 owner 处理/缓冲接纳，不证明对端收到；UDP callback 来源见下表 | D4 |
| Socket.Fd、Dup | fd 借用仅观察或 owner 控制；dup 返回唯一所有权、cloexec，共享 open-file-description 状态 | D3 Socket 组合；NativeSocket F |
| SetReadBuffer、SetWriteBuffer、SetNoDelay | 失败保留 errno；TCP no-delay 对非 TCP 拒绝 | F 基础，D3 转接 |
| SetKeepAlivePeriod、SetKeepAlive | 以秒明确边界；本包 SetKeepAlive 提供 idle/interval/count，Period 由后续组合（没有假入口） | F/D3 |
| SetLinger | 禁用或 abortive 0 可用；正数可能阻塞 close，本包副作用前拒绝；后续如需正数必须专门的非 Processer 收口方案 | F 差异 |

### Options（覆盖 options.go 的全部字段；WithX 是同字段配置语法）

| 字段 / With 方法 | C++ 去向 | 状态 |
|---|---|---|
| Options / WithOptions | 类型化 Options 一次校验，不移植任意 option 回调 | D3 |
| LB / WithLoadBalancing | RoundRobin、LeastConnections、SourceAddrHash | D3 |
| ReuseAddr / WithReuseAddr | NativeSocket.SetReuseAddr | F；engine 配置 D3 |
| ReusePort / WithReusePort | NativeSocket.SetReusePort，仅 IP；跨 loop 接纳另验 | F；engine D3、非 Linux U |
| MulticastInterfaceIndex / WithMulticastInterfaceIndex | IP membership、接口索引；不因普通 UDP 测试就宣称 multicast 已验 | D3/U：需自有 multicast 场景 |
| BindToDevice / WithBindToDevice | Linux 专属能力；权限不足显式错误，非 Linux UnsupportedOperation | D3/U：本包不需要特权 |
| Multicore / WithMulticore | 默认单 owner；显式启用才按 CPU，不能偷偷扩大资源 | D3 |
| NumEventLoop / WithNumEventLoop | 正值优先，非法负值拒绝；不复现参考注释与实际取值的歧义 | D3 |
| ReadBufferCap / WithReadBufferCap | 每轮读预算，默认 64KiB，向上 2 次幂时做溢出检查；另有总量上限 | D3/D4 |
| WriteBufferCap / WithWriteBufferCap | static 段容量，默认 64KiB，不是总量上限；另设总待发资源界 | D3/D4 |
| LockOSThread / WithLockOSThread | gnet 仅启用此项才绑定 OS 线程；本迁移目标每 reactor driver 固定自有 OS 线程，与此开关取值无关，不等于 CPU affinity；不把可迁 Processer 的业务协程锁到创建线程 | D3 刻意差异，不伪装原开关的条件绑定效果 |
| Ticker / WithTicker | 显式 timer 启用 | D3 |
| TCPKeepAlive / WithTCPKeepAlive | idle 秒；后续 Options 层处理默认 interval=idle/5、count=5 与合法取整 | F primitive，D3 normalization |
| TCPKeepInterval / WithTCPKeepInterval | interval 秒 | F primitive，D3 |
| TCPKeepCount / WithTCPKeepCount | 正整数 count | F primitive，D3 |
| TCPNoDelay / WithTCPNoDelay（TCPNoDelay/TCPDelay） | bool SetNoDelay；engine 默认 true | F primitive，D3 |
| SocketRecvBuffer / WithSocketRecvBuffer | 正字节数，内核可能调整实际值 | F primitive，D3 |
| SocketSendBuffer / WithSocketSendBuffer | 正字节数，内核可能调整实际值 | F primitive，D3 |
| LogPath / WithLogPath | 本地日志配置，不新增 zap 依赖，不隐式覆盖其他 engine 的 logger | D3 |
| LogLevel / WithLogLevel | 项目日志等级值 | D3 |
| Logger / WithLogger | 自有窄日志接口或已有能力，不导出第三方对象 | D3 |
| EdgeTriggeredIO / WithEdgeTriggeredIO | stream ET 开关 | D3 |
| EdgeTriggeredIOChunk / WithEdgeTriggeredIOChunk | 正值启用 ET，默认 1MiB，向上取整先检查溢出 | D3 |

## 纯逻辑包覆盖（2026-10 增补；整体仍非完整迁移）

以下为本轮纯逻辑包**已实现并局部验证**的 F 项，均只到值/算法层：

- `bbt/infra/net/Options.hpp/.cc`：gnet options.go 全字段映射的类型化 Options 与 `NormalizeOptions` 纯值归一化（loop 解析、cap 默认/最小 1024/向上 2 次幂、ET chunk、keepalive 默认取整、socket buffer/枚举/NUL 校验、失败不改输入）；logger 为自有窄回调，仅可存储。有界资源扩展三字段：每连接待收/待发总量（默认各 1MiB）与待处理事件/命令限额（默认 1024），必须正值且基础 cap 不得超对应总量。**网络行为 setter/接线仍 D3，无任何 reactor 已启动的声明。**
- `bbt/infra/net/LoadBalancing.hpp/.cc`：RoundRobin/LeastConnections/SourceAddrHash 纯选择（单 owner、非线程安全；CRC32-IEEE 对 Address::ToString 取模，父级 zlib 固定向量验证）。跨域 Register 串行化与计数维护仍 D3。
- `bbt/infra/net/detail/{EventRingBuffer,OutboundQueue,SourceAddrHash}.hpp`：Boost.circular_buffer 内部薄包装的有界弹性 ring（仅增长、预检拒绝不覆盖；char 的 erase_begin 为 O(1)，自身 Peek view/子串追加先保存副本，拒绝/分配失败原内容不变）、独立拥有连续事件快照（owner 继续更改/销毁 buffer 不影响快照；view 借用范围显式）、ring+分段溢出待发队列（总量与段数有界、writev 有限段浏览、已发送前缀跨 ring/list 部分消费）。detail 头不进公共自检、不是公开 API。

上表既有 F/D3/D4/R/U 标记不变：Conn/Reader/Writer/Engine/等待/Close 未接，`ReadBufferCap/WriteBufferCap/LB` 等行的 D3 部分仍指 Engine 接线，不因本包视为完成。

## 同一张执行权 / 回调 / 视图表

本表是 [执行域设计](../architecture/gnet-network-execution.md) 的业务/数据接缝约束。coroutine 保留通用等待/Timer/Hook 及自己的 CoPoller；网络 main/sub 的 context 与线程归 infra Engine，不使用 `io::GetExecutor()` 开第二 driver。网络 owner 数与 Processer 数不一一绑定。gnet 原 OnTraffic 在 sub eventloop 直接执行，本迁移放入受管协程是有意差异；并非声称 gnet 每事件创建业务 goroutine。

采用 **业务独占事件快照**：owner 读取的共享缓冲不直接借给 worker。每连接任意时刻至多一个业务事件；入队事件有独立不可变 backing storage，owner 后续读取不得修改它。Reader 游标由当前事件持有，Peek/Next 的 view 只借该 snapshot；挂起时 backing storage 延寿，但 Discard/Next 等使其失效的边界仍明确。Writer 将业务数据复制/转移到 owner 命令，不允许两个域同时改 mutable ring。无 OS mutex 持有跨挂起。

| 类别 / 入口 | 执行权及域 | 可挂起 | 同连接 / Action | view 与完成点 |
|---|---|---|---|---|
| main accept、fd read/write、注册/撤销、物理 close | native owner/main reactor，普通线程合法 | 否 | 只管 fd；不等待业务事件归还 | 不能借出可复用 reactor 读缓冲 |
| OnBoot、OnShutdown | 受管协程的 engine 生命周期事件位 | 是 | Boot 后再开放业务；Shutdown 不是 Engine 强制关闭的等待门槛，不承诺强制关闭时运行完毕。**本轮（2026-10-17）用户裁定**：`OnShutdownRequest`（`Action::Shutdown` 的向上请求上报口，与产生它的事件同一受管协程任务）取代旧的「Shutdown 自动收口」表述；`OnShutdown`（已关闭通知）本仓未交付 | 不借 connection mutable buffer |
| OnOpen、OnTraffic（含 Wake）、OnClose | 受管协程，单连接串行事件位；与 fd owner 不同 | 是 | 回调返回后回投 Action；首个 Closing 封口不可被晚 Action 撤销；OnClose 最多一次 | 只用事件 snapshot；OnClose 不获得已关 fd |
| OnTick | 受管协程，engine 单 ticker 事件位 | 是 | 不重入 tick；Shutdown 回投 engine = **上报 engine 层级/向上层**（经 `OnShutdownRequest`），**不是自动执行收口**，也不停 ticker | 不借 owner buffer |
| stream AsyncCallback、CloseWithCallback 完成项 | owner 线程；只做短且非挂起处理 | 否 | 不占业务事件位，不等待 OnClose | 写处理/缓冲接纳不等于内核全部发送/对端收到 |
| **gnet UDP AsyncWrite callback 例外** | 参考 `connection_unix.go:501–511` 在调用方同步 callback(nil,nil)，即使 send 失败也给 callback nil 错误 | 原版不控制调用方挂起 | 不是“一律 loop” | 本迁移 D4 改为 owner 完成项，传实际错误，不传已释放 Conn；明确破坏该错误行为，不暗中保留 bug |
| Execute / Runnable、ReadFrom / WriteTo、自定义 callback | 只按各入口显式约束；不能推断用户函数不阻塞 | 不可默认承诺 | owner Runnable 禁止等待；用户违约可诊断，不冒充静态证明 | 任意第三方/用户路径不自动贴标签 |

## Engine 强制关闭修订（2026-10-16，待实现）

Engine 通常贯穿进程寿命，关闭只承诺停止服务及后续操作安全拒绝，不负责业务优雅排空，不等待 Handler/OnClose/全桥 drain，不把全部资源已释放作为返回含义。关联对象按所有权和 RAII 自然销毁；持有关闭句柄不应悬空或维持引用环。关闭必须处理在途网络操作和等待者，不能以停 driver 遗失完成；不强杀用户协程、不要求 runtime Stop、不因未先显式 Close 而 terminate。完整约束见[执行域设计第 6 节](../architecture/gnet-network-execution.md#6-网络关闭不是-runtime-停机)。这是用户修订的目标，不是现有实现已通过。

本轮（2026-10-17）用户裁定：**Shutdown 向上层传递，自由度给使用者**——`Action::Shutdown`（值 2）只表示向上请求，`EventHandler` 新增 `virtual void OnShutdownRequest()`（默认空实现）作为上报口，由 `EventHandlerAdapter::ApplyAction` 在原事件的受管协程任务内同步转发一次；不新增引擎关闭位/路由/队列/线程，`ServerEngine`/`Engine` 零 diff。请求可重复上报、不设首胜；回调内不得调用 `Engine::Stop()`（受管协程被域门禁在任何副作用之前拒绝 `operation_not_permitted`）。本节与上表的相关措辞是**本地候选，未独立审查、未提交**，不构成冻结契约；物理强停仍只来自控制域的既有 `Engine::Stop()`/析构。

以下为连接级 Close(opt)，其物理完成约定不因 Engine 放弃业务优雅排空而删除；已完成的内部 drain 测试保留为局部资源/事件观测，不再等价 Engine 关闭门槛。

## Close(opt) 冻结

- `CloseMode::{Block, Unblock}` **无默认值**；网络等待与 FlushPolicy 是正交配置。
- `Open → Closing → Closed` 首胜；Unblock 成功表示封口并可靠投递，非物理完成。Closed 只能在撤销 fd 事件、最后内核访问结束、唯一 fd 关闭后发布。
- Block 仅允许当前受管协程，先校验再副作用；普通控制线程、main/owner 驱动线程拒绝，不扩大调用域。等待用 CoWaiter，不忙等/阻塞 Processer。
- **物理完成与 OnClose 通知拆开**。OnTraffic 内 `Close(Block)`：封口 → owner 撤销/关 fd → Notify 物理完成 → OnTraffic 恢复返回 → 已排队 OnClose 才执行。因此 Block 不等待 OnClose，自等环不存在。参考 gnet 在 fd close 前 OnClose，本迁移明确改变顺序。
- OnClose 内重复 Close(Block) 对已经 Closed 直接返回物理完成；不能重复通知/close。异域 Unblock 与此竞争只共享同一次终态。
- FlushPolicy 独立表达 discard 或有界尝试/期限排空；本包 primitive Close 不执行用户 buffer flush。正 SO_LINGER 本包拒绝，不在 Processer 使用阻塞 linger。
- P4 必测上述时序、混合 Block/Unblock、迟到事件、fd 复用、最后引用释放；文档不是这些竞态已经通过的证据。

## 对端 FIN/RST 收口（2026-10-17 增补）

- **peer FIN（干净读终态）= 连接终态并自动收口**：桥在 owner 域只读记录读终态，在业务事件（`OnTraffic`，含快照归还）**返回之后**发起物理收口，随后 `OnClose` 按既有串行契约送达。提交是**有界的**：**单个事件最多两次有界尝试**（一次 `Submit` 未被接纳且状态为队列满时，同一事件内再补偿一次；非 `Full` 的失败不再重试）；两次都未接纳时该事件不产生命令，收口责任退回 registry/session，**下一个可达事件会重新进入该入口再试**（dispatcher 每个事件返回后都会重新调用收口入口）。同一连接**最多只有一条被接纳的命令**，物理收口只由它负责执行，接纳后不再重复投递。因此契约只承诺「**存在后续可达事件即可收敛**」，**不承诺持续队列满或再无任何事件时自动恢复**——此时责任留在 registry/session，`Engine::Stop` 仍直接收口、`Detach` 仍拒绝活 fd。不改 `Open→Closing→Closed` 推进，也不新增公共 EOF 观察面。
- **尾帧先于收口**：同一事件位内的已读字节先由业务在 `OnTraffic` 内通过既有 Reader 消费；不自动丢弃已派发事件，也不在业务返回前 close。未消费尾部随物理关闭释放。
- **通知与原因口径**：终态通知面仍为既有 `ICoConnectionHandler::OnClose(connection, reason)`；干净 FIN → `reason` 为空；RST/致命 IO 错误 → 真实 `errno`（`std::system_category`，如 `ECONNRESET`）；`OnClose` 仍最多一次。业务无需显式 `Close`、无需 `Engine::Stop` 即可观察到对端关闭，活跃连接计数在 `OnClose` 执行时自然归零。
- **不支持写侧半关闭**：不提供 `shutdown(SHUT_WR)`/FlushPolicy/优雅 drain，与 gnet v2.10.0 一致（无半关闭 API，残余 outbound 只 best-effort）。**不承诺已接纳的响应送达对端**；FIN 到达后未发出的响应不可恢复（与前述「关闭不承诺待发数据送达」同向）。
- 强停路径不变：`StopServiceOwner`/`DetachOwner` 仍可压掉未开始的 `OnClose` 且不算 drain；EOF 收口只在不与强停竞争同一终态时进行。

## ICoObject 与标签

新 `net::ICoObject` 派生上游身份接口，环境查询/入口校验集中在新基类；不修改 canonical namespace。`CoObject` 提供身份值，创建前必须上游 runtime 已初始化。合法性每次现查当前受管协程，不能缓存 TLS、对象创建线程或创建协程；网络 owner 域另行验证。

`BBT_INFRA_NET_NONSUSPENDING` 是统一 C++17 标签（Clang annotate，其他编译器空语义标记）。它只承诺完整路径不协程挂起且不阻塞执行线程，不等于 noexcept、lock-free、线程安全或协程外合法。纯环境快照 `CanSuspend` 可标；复制字符串的 GetObjectInfo 不标。等待入口不标，且必须在副作用前调用集中 gate。

**查询支持缺口 U1 已在固定版本解决**：coroutine `8ae6a88997c5acaa4fe03430c35e1820ed5b29b0` 的两项公开查询保留 TLS 许可门禁，经唯一原 TLS 存储的无创建路径读取当前 Processer，不再经过创建型入口。原 `39fdfff…` 的两个查询冷线程反例仍可触发额外创建，未被 skip/WILL_FAIL 掩盖；修复已独立审查并经 [PR #388](https://github.com/yqm-307/bbtools-coroutine/pull/388) 合入（mergeCommit `6dfda8158d76f5030cde50a9fbc3c97a8335e932`），该 head 的上游 CI 实跑 59/59，六模块同 runner 严格性能比较 PASS。infra 从干净精确源码新建 Debug/Release 构建，各 16/16 新 net 测试通过（含 `net.context_no_create`），核实 compile/ELF/实际 DSO 来源；独立 UDP 消费 example 也通过，未用环境变量临时换库。**本轮（160）已把该消费出处从 `6f650eee`+未提交差异重绑定到固定 commit `8ae6a889…`（[PR #392](https://github.com/yqm-307/bbtools-coroutine/pull/392) 的 mergeCommit，其父即 #388 的 mergeCommit），并从该固定 commit 干净重建新 DSO（sha256 `0f8e0379…e5b0`）重跑固定版本影响面 107/107 通过**；上述 16/16 与旧 98/P6 证据绑定旧 dirty DSO，不作固定 commit 依据。源码输入继续复用 `BBT_COROUTINE_SOURCE_DIR`，使用者必须按 README 核 SHA/clean 及实际链接，不将 CMake 的路径一致检查冒称为 SHA 锁定机制。上游两 PR 均已合并，完整 P3/P4 仍未交付。

`GetEnvironment`/`RequireCoroutine` 的签名及实现不因本次切换改变。LocalThread helper 首次初始化仍可能分配；本次无创建回归不等于 allocation-free 或完整路径非阻塞证明，暂不因此给这两个入口无条件加 NONSUSPENDING 标记。后续等待能力仍须在副作用前经过集中 gate；不能由“当前线程允许协程”推定“当前正在受管协程”。

## socket 基础边界

`Address` 只解析 numeric IPv4/IPv6（含 scope id）与 Unix 路径/抽象地址，无同步 DNS；校验端口、空/过长路径、嵌入 NUL。IPv6 地址解析复用 Boost，不把 Boost 对象公开；scope 在进入 Boost 前自行严格校验——只支持完整十进制数字（C++17 from_chars，u32 范围，0 合法），拒绝负号、空、尾随垃圾、溢出、重复 `%`、IPv4/主机名带 scope 与接口名（无 if_nametoindex/atoi 回退）。

`NativeSocket` 是单 owner 非阻塞 Linux fd RAII，不是业务 CoObject；Open/Accept 原子 nonblock+cloexec，Dup cloexec 且共享文件描述状态。main/reactor 可用。Close 先放弃所有权再一次 syscall，Linux EINTR 不重试已可能复用的 fd；显式 Close 返回错误，析构仅兜底。公开 Fd 借用不得私自 close/改成阻塞/开启正 linger，Release 才转移所有权。Accept/Connect 在 Linux 上走 `SYS_accept4`/`SYS_connect` 原生 syscall，不经 coroutine Hook——Hook 介入的 accept4/connect 会把非挂起 primitive 隐式推进协程等待（独立审查已证实）；close 仍走 Hook 路径保留 fd waiter/epoch 记账。

Send/Receive/Sendv/SendTo/ReceiveFrom 显式 DONTWAIT，send 加 NOSIGNAL；保留 EINTR/EAGAIN/EINPROGRESS、partial、stream EOF、datagram 零长与 truncation。flags 只承诺保守 allowlist：Send/Sendv/SendTo 仅接受 MSG_DONTWAIT|MSG_NOSIGNAL（内部始终附加）；Receive 仅接受 MSG_DONTWAIT|MSG_PEEK；数据报 Receive/ReceiveFrom 另接受 MSG_TRUNC，返回 bytes 只计拷贝到 buffer 的字节、truncated 明确反映截断；stream TRUNC 拒绝（防 bytes>0 而 buffer 未填）。未列出 flags（OOB/ZEROCOPY/ERRQUEUE/WAITALL/MORE 与未知位）在任何 syscall 前以 operation_not_supported 拒绝，返回 bytes=0 且不发出/不消费报文；不泛称支持任意 POSIX 组合。Connect 的 EINPROGRESS 不是失败收口也不是成功；可写就绪后查 PendingError，SO_ERROR 清零行为由调用者掌握。选项预校验失败无副作用；组合 keepalive 在 syscall 失败时可部分应用并返回第一错误，不虚构事务回滚。只实现后续 owner 必需窄表面，不建万能 transport/plugin。

## 文件、验证与后续依赖

首包源码表：根 CMake 分流、`cmake/NetworkOnly.cmake`、`bbt/infra/net/{CMakeLists.txt,Annotations.hpp,ICoObject.hpp,CoObject.hpp,CoObject.cc,Address.hpp,Address.cc,NativeSocket.hpp,NativeSocket.cc}`、`tests/net/`、`examples/net_foundation_consumer/`、本决策与移植许可说明。根分流跳过旧 src/tests/examples，故不必改上层 CMake/功能文件。CI 的 `deps_bundle.sh`、`run_tests.py` 和 workflow 由 P7 整体切换，本包不写也不宣称已隔离 CI。

NetworkOnly 对新建/复用两类 coroutine target 统一补 target 级公共头 include；复用分支解 alias、realpath 核真实 SOURCE_DIR，与固定源码路径不一致时 configure 明确失败。tests/net 提供真实 coroutine-first parent consumer（configure/build/run/DSO 来源核验）与 mismatch 负向 fixture。

首包必验：单头 C++17、身份派生、普通/TLS-enabled idle/受管协程、无隐式 Processer 反例；TCP/UDP IPv4/IPv6/Unix real roundtrip；flags、dup、partial/EAGAIN/EINPROGRESS、EOF/truncation、失败回收；target/CTest JSON/compile/link/selfcheck 闭包、残留 prefix 负向配置、仓外 add_subdirectory consumer、diff hygiene。测试使用显式检查，不受 NDEBUG 影响。非 Linux、multicast、BindToDevice、EINTR 注入及完整 engine 竞态不由普通 roundtrip 推定。

P6 进入前采用候选首轮上界：2 loop、2 Processer，最多 32 连接、单包 1KiB，每场景 10秒、总 60秒，RSS 256MiB、fd 256、每连接待发/待收各 1MiB、队列总 1024。先冒烟，超限停止并只清理自有资源；CPU 按两 owner+两 worker 的显式并发约束记录，禁止压满机器。此处只是后续预算候选，未实施负载监控则不得启动 P6，更不是压测通过证据。

## 参考与许可

- [gnet.go](https://github.com/panjf2000/gnet/blob/58b59a11350707153f8c48369693ab9beba7b2fa/gnet.go)、[options.go](https://github.com/panjf2000/gnet/blob/58b59a11350707153f8c48369693ab9beba7b2fa/options.go)、[client_unix.go](https://github.com/panjf2000/gnet/blob/58b59a11350707153f8c48369693ab9beba7b2fa/client_unix.go)。
- 同 SHA 的 `connection_unix.go`、`eventloop_unix.go`、`acceptor_unix.go`、`load_balancer.go`、`pkg/socket/`、`pkg/buffer/` 为实现语义依据；不是 Go 二进制兼容承诺。
- Apache-2.0 主体与 ring 文件 MIT 适用声明、版权和后续逐文件移植规则见 [gnet 移植出处](../dependencies/gnet-2.10.0.provenance.md)。本包不移植 ring 算法。


</details>
