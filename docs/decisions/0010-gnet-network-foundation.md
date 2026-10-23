# 0010：gnet 网络基础与 R2 TCP 契约

**状态：已合入主线**（PR #84，合入提交 `99d0821a0175c43d9ec2fff78de1f90af068f92b`）。下表为核心契约，当前实现覆盖 TCP server/client 的 raw 与 Co 两条入口；HTTP/RPC/Redis/Mongo/config 消费者迁移、高层 UDP、Reactor 级 Release/sanitizer/长时压力仍未做。当前执行模型及生命周期真源见 [网络执行域设计](../architecture/gnet-network-execution.md)。文末逐符号表保留为迁移分析记录，不再以 gnet 签名强制约束新 API，也不作为交付状态来源（交付状态以 README 与本文「当前实现落点」为准）。

## 当前范围与版本

- 本次固定源码依据：设计期 infra `2cb3ebf60a101bb9cbf93f065db4d41e2130d3cd`；coroutine driver PR #396 的 merge commit `1e9f0e8c25de85dde003def65bb58b90bdac0e0b`。设计最初读取的 PR head `8d7b4383c8631f4dccad74df088abe54ca000b87` 与合并提交分开记录。
- 该 upstream 版本提供通用 Processer driver 支撑，已由本仓 `detail/ReactorDriver`、`detail/DriverBinder` 消费；构建使用的 coroutine 版本由 CI 从 `coroutine@main` 拉取并在 run 中记录 SHA，不以本段 SHA 作为构建 pin。
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

上表的接口名和状态名表达职责与约束；当前公共头见下节「当前实现落点」。未落地的选项（`ticker`、`edge_triggered_io`、`bind_to_device`、`multicast_interface_index`、`lock_os_thread`）在 `Run` 中以 `operation_not_supported` 明确拒绝，不静默忽略。

## 当前实现落点

- 公共头：`<bbt/infra/net/Engine.hpp>`（`Engine`/`EngineConfig`/`RuntimeProgress`/`EnginePhase`）、`RawHandler.hpp`、`Connection.hpp`、`CoConnection.hpp`、`Options.hpp`、`LoadBalancing.hpp`、`Address.hpp`、`NativeSocket.hpp`、`Error.hpp`、`ICoObject.hpp`/`CoObject.hpp`/`Annotations.hpp`；内部部件在 `detail/`（`ReactorDriver`、`DriverBinder`、`ReceiveRing`、`OutboundQueue`、`EventRingBuffer`、`WakeHub`、`ConnectionState`），不进公共自检。高层 UDP 只有 `<bbt/infra/net/Udp.hpp>` 的编译拒绝入口。
- 目标：`bbt::infra_net`（STATIC），依赖 `bbt_coroutine` 与 Boost 头，不引入新第三方依赖。
- 测试面：`tests/net/` 的 `net_foundation`/`net_primitives`/`net_allocation`/`reactor_driver`/`r2_tcp`/`b4_contract`/`net_foundation_consumer` 与 `example_co_net_service_roundtrip`，由 `scripts/ci/check_network_tests.py` 强制「注册集 = 实跑集、零跳过」。
- 旧 `Client`/`EventHandler`/`CoTcpConnection`/`ICoConnection`/`Action`/`Engine::Stop`/`CloseMode` 已删除，不保留兼容壳；`Engine` 只对外提供 `Run`/`RequestClose`。
- 未交付：HTTP/RPC/Redis/Mongo/config 消费者迁移、TLS、ReusePort 跨 loop 验收、非 Linux 平台、Release/sanitizer/长时压力、Engine 级 graceful drain。

## 复用、兼容与验证

复用现有 `EventRingBuffer` 存储/游标并补最小同步访问面（`ReceiveRing`）；其原类本身仍是非线程安全的。复用 `OutboundQueue` 的 FIFO/部分发送并补逐操作边界记账，不把字节合并后失去操作归属。`Address`/`NativeSocket`/`LoadBalancing` 的有效能力保留，不借机重写基础算法。

旧实现（EventLoop 自建线程、`ConnectionBridge` 事件快照、受管 `OnOpen/OnTraffic`、`Engine::Stop`、旧 `CloseMode`）已在 PR #84 中被替换并删除，不再提供兼容壳；其历史局部测试次数只绑定当时版本，不作为新 net 的验收依据。新能力的当前验证形状见执行域文档第 7 节与 `tests/net/` 注册集。

consumer 侧对齐 coroutine 核心契约 §11.5 的责任，不改 §11.3–§11.4 的锁外 callback、全租约、Failed 与同代同步解绑。本仓消费的 coroutine 版本以 CI run 记录的实际 SHA 为准，不以某个历史 merge commit 代替。

## 值层与并发接缝的现行口径

- `Options.hpp`/`detail/OptionsNormalize.{hpp,cc}`：`Options` 只保留已接线字段（lb/reuse_addr/reuse_port/num_event_loop/tcp_keep_*/tcp_no_delay/socket_*_buffer/max_inbound_bytes/max_outbound_bytes/max_pending_events）；归一化（loop 解析、keepalive 默认取整、socket buffer/枚举/NUL 校验、失败不改输入）下沉 `detail/OptionsNormalize.*`，`NormalizeOptions`/`NormalizedOptions` 不再是公共入口。有界资源三字段必须正值（默认各 1MiB、pending 1024），`0` 拒绝。D3 已删除 `multicore`、读写 buffer cap、ET/ticker/锁线程/设备/组播与全部日志相关字段及类型。
- `LoadBalancing.hpp`：RoundRobin/LeastConnections/SourceAddrHash 纯选择；跨域调用由 Engine 的短锁串行化，算法对象自身非线程安全。
- `detail/{ReceiveRing,OutboundQueue,EventRingBuffer,SourceAddrHash}.hpp`：`EventRingBuffer` 为仅增长、预检拒绝不覆盖的有界弹性 ring；`ReceiveRing` 在其上补最小并发访问面（单次 `TryRead` 原子复制+消费、`TryAccess` 的 Peek+Consume 组合守卫、释放代次通知）；`OutboundQueue` 为 ring+分段溢出队列，保留逐操作身份与已发前缀。这些是内部部件，不进公共自检、不是公开 API。
- 执行权与数据接缝的原理约束（详见[执行域设计](../architecture/gnet-network-execution.md)第 2、5 节）：fd 与物理 close 只在 native owner/reactor 线程；原始回调在 owner 的非业务协程边界短、非阻塞、不可挂起；业务协程可迁移 Processer，不持有 fd 所有权；接收 ring 是并发安全的唯一真源，`OnRead` 只是可合并通知；不允许任何 OS mutex 持有跨挂起，也不把 `shared_ptr` 当作对象同步。
- 与 gnet 的刻意差异：参考实现在 sub eventloop 栈上直接执行 `OnTraffic`，本迁移改为原始短回调 + 可选协程薄封装；参考 UDP `AsyncWrite` 在调用方同步回调且吞掉 send 错误，本迁移不复制该行为（且高层 UDP 本轮不实现）。

## 对端 FIN/RST 的当前口径

R2 重建后（PR #84），对端关闭只影响读方向，**不自动收口连接**：

- peer FIN → owner 把读终态写入 ring（`ReceiveRing::MarkEof()`）并 `OnRead` 通知；已收尾部继续可读，读尽后返回 `NetErrc::Eof`。写侧仍可用，是否关闭由上层决定。
- RST/致命读错误 → 真实 `errno`（`std::system_category`，如 `ECONNRESET`）记录为持久读错误，同样不自动关闭写侧、不在库内重试。
- `OnClose` 由显式 `Connection::RequestClose()` 或 `Engine::RequestClose()` 的 owner 收口兑现，每连接最多一次，`reason` 为首次请求值（`OnClose` 不是 peer EOF 的通知面）。
- 不提供写侧半关闭（`shutdown(SHUT_WR)`）与 FlushPolicy；**不承诺已接纳的响应在关闭前送达对端**。

分析期旧 bridge 曾有「peer FIN = 连接终态并自动收口」的规则（含最多两次有界提交、责任退回 registry/session 等细节）；该规则随旧实现删除，不再适用。保留其原理结论：EOF 与错误必须持久可查、尾帧先被消费再谈收口、收口不承诺业务回包已送达。

## ICoObject 与标签

新 `net::ICoObject` 派生上游身份接口，环境查询/入口校验集中在新基类；不修改 canonical namespace。`CoObject` 提供身份值，创建前必须上游 runtime 已初始化。合法性每次现查当前受管协程，不能缓存 TLS、对象创建线程或创建协程；网络 owner 域另行验证。

`BBT_INFRA_NET_NONSUSPENDING` 是统一 C++17 标签（Clang annotate，其他编译器空语义标记）。它只承诺完整路径不协程挂起且不阻塞执行线程，不等于 noexcept、lock-free、线程安全或协程外合法。纯环境快照 `CanSuspend` 可标；复制字符串的 GetObjectInfo 不标。等待入口不标，且必须在副作用前调用集中 gate。

**查询支持缺口 U1 已解决**：coroutine 的公开查询保留 TLS 许可门禁，经唯一原 TLS 存储的无创建路径读取当前 Processer，不再经过创建型入口。修复经 [PR #388](https://github.com/yqm-307/bbtools-coroutine/pull/388) 合入，并由 `net.context_no_create` 作为必过门禁长期守住；若该门禁失败，说明当前消费的 coroutine 版本回退到创建型入口，应先核实际 SHA。源码输入继续复用 `BBT_COROUTINE_SOURCE_DIR`，使用者必须核对 SHA/clean 及实际链接，不将 CMake 的路径一致检查当作 SHA 锁定机制。

`GetEnvironment`/`RequireCoroutine` 的签名及实现不因本次切换改变。LocalThread helper 首次初始化仍可能分配；本次无创建回归不等于 allocation-free 或完整路径非阻塞证明，暂不因此给这两个入口无条件加 NONSUSPENDING 标记。后续等待能力仍须在副作用前经过集中 gate；不能由“当前线程允许协程”推定“当前正在受管协程”。

## socket 基础边界

`Address` 只解析 numeric IPv4/IPv6（含 scope id）与 Unix 路径/抽象地址，无同步 DNS；校验端口、空/过长路径、嵌入 NUL。IPv6 地址解析复用 Boost，不把 Boost 对象公开；scope 在进入 Boost 前自行严格校验——只支持完整十进制数字（C++17 from_chars，u32 范围，0 合法），拒绝负号、空、尾随垃圾、溢出、重复 `%`、IPv4/主机名带 scope 与接口名（无 if_nametoindex/atoi 回退）。

`NativeSocket` 是单 owner 非阻塞 Linux fd RAII，不是业务 CoObject；Open/Accept 原子 nonblock+cloexec，Dup cloexec 且共享文件描述状态。main/reactor 可用。Close 先放弃所有权再一次 syscall，Linux EINTR 不重试已可能复用的 fd；显式 Close 返回错误，析构仅兜底。公开 Fd 借用不得私自 close/改成阻塞/开启正 linger，Release 才转移所有权。Accept/Connect 在 Linux 上走 `SYS_accept4`/`SYS_connect` 原生 syscall，不经 coroutine Hook——Hook 介入的 accept4/connect 会把非挂起 primitive 隐式推进协程等待（独立审查已证实）；close 仍走 Hook 路径保留 fd waiter/epoch 记账。

Send/Receive/Sendv/SendTo/ReceiveFrom 显式 DONTWAIT，send 加 NOSIGNAL；保留 EINTR/EAGAIN/EINPROGRESS、partial、stream EOF、datagram 零长与 truncation。flags 只承诺保守 allowlist：Send/Sendv/SendTo 仅接受 MSG_DONTWAIT|MSG_NOSIGNAL（内部始终附加）；Receive 仅接受 MSG_DONTWAIT|MSG_PEEK；数据报 Receive/ReceiveFrom 另接受 MSG_TRUNC，返回 bytes 只计拷贝到 buffer 的字节、truncated 明确反映截断；stream TRUNC 拒绝（防 bytes>0 而 buffer 未填）。未列出 flags（OOB/ZEROCOPY/ERRQUEUE/WAITALL/MORE 与未知位）在任何 syscall 前以 operation_not_supported 拒绝，返回 bytes=0 且不发出/不消费报文；不泛称支持任意 POSIX 组合。Connect 的 EINPROGRESS 不是失败收口也不是成功；可写就绪后查 PendingError，SO_ERROR 清零行为由调用者掌握。选项预校验失败无副作用；组合 keepalive 在 syscall 失败时可部分应用并返回第一错误，不虚构事务回滚。只实现后续 owner 必需窄表面，不建万能 transport/plugin。

## 文件、验证与 CI

源码面：根 CMake 分流、`cmake/NetworkOnly.cmake`、`bbt/infra/net/`（公共头 + `detail/` 内部部件 + `Engine.cc` 等实现）、`tests/net/`、`examples/net_foundation_consumer/`、`examples/co_net_service/`。根分流跳过旧 `src/tests/examples`，故 network-only 不触碰旧模块 CMake。

CI：`.github/workflows/ci.yml` 的 `network` job 已接入（不属于旧 Release/Debug job），复用同 run 的 prepared deps、核实际 coroutine SHA、独立目录 `-DBBT_INFRA_NETWORK_ONLY=ON` Debug 构建，然后 `ctest -j1 --no-tests=error` 跑全注册集并由 `scripts/ci/check_network_tests.py` 拒绝缺项/重复/失败/跳过，另对 `net.b4_co_pingpong_strict` 复跑 5 次。该 job 计入唯一结果汇聚。

NetworkOnly 对新建/复用两类 coroutine target 统一补 target 级公共头 include；复用分支解 alias、realpath 核真实 SOURCE_DIR，与固定源码路径不一致时 configure 明确失败。tests/net 提供真实 coroutine-first parent consumer（configure/build/run/DSO 来源核验）与 mismatch 负向 fixture。

原语层必验项已由 `net.*` 注册集覆盖：单头 C++17、身份派生、普通/TLS-enabled idle/受管协程、无隐式 Processer 反例；TCP/UDP IPv4/IPv6/Unix real roundtrip（`NativeSocket` 原语层，不等于 `Engine` 端到端）；flags、dup、partial/EAGAIN/EINPROGRESS、EOF/truncation、失败回收；CTest 闭包、残留 prefix 负向配置、仓外 add_subdirectory consumer、diff hygiene。非 Linux、multicast、BindToDevice、EINTR 注入与 `Engine` 上的 IPv6/Unix 端到端不由上述用例推定。

面向用户的示例验收要求（`examples/co_net_service` 已满足的部分见该目录 README）：

- 只消费公开头与 `bbt::infra_net`，不让使用者接触私有 owner/factory/waiter/native fd；公共入口不足时补契约，不用示例内私有胶水掩盖。
- 至少一条真实请求/响应或轻量分帧路径以顺序协程代码表达等待，不迫使业务手写 callback 状态机；并展示一个 handler 挂起时另一连接仍能进展、同连接事件保持串行。
- 说明按需消费缓冲与分帧、view 有效期、有界待发与慢对端、deadline/错误检查、结果未知时不盲目重试，以及显式收口顺序。
- 配套可复制的构建/运行命令与预期可观察输出；使用真实对端、动态端口与自有资源，异常退出也收口；不以 skip、mock 或私有实现作为通过条件。

压测预算（未执行，仅作启动前上界）：2 loop、2 Processer，最多 32 连接、单包 1KiB，每场景 10 秒、总 60 秒，RSS 256MiB、fd 256、每连接待发/待收各 1MiB、队列总 1024；先冒烟，超限停止并只清理自有资源，禁止压满机器。未实施负载监控前不得把它当作压测通过证据。

## 参考与许可

固定参考 gnet v2.10.0 `58b59a11350707153f8c48369693ab9beba7b2fa`；仅作机制/行为比较，不承诺逐方法迁移、性能等价或所有平台可用。现有出处及许可约束继续见 [gnet 移植出处](../dependencies/gnet-2.10.0.provenance.md)，不因重设计删除版权义务。本次不移植新第三方代码。

---

<details>
<summary>gnet v2.10.0 迁移对照分析（历史，不是当前 API 清单）</summary>

## 分析期版本

- 分析期 infra 基线：`3136d1de5031a2eec55475e770d5ebbadc3c32da`（PR #79 合入点）。
- 分析期 coroutine 消费：`8ae6a88997c5acaa4fe03430c35e1820ed5b29b0`（[PR #392](https://github.com/yqm-307/bbtools-coroutine/pull/392) 的 mergeCommit，其父 `6dfda815…` 为 [PR #388](https://github.com/yqm-307/bbtools-coroutine/pull/388) 的 mergeCommit）。该 SHA 用于记录当时的消费口径；当前构建由 CI 拉取 `coroutine@main` 并在 run 中记录实际 SHA。
- gnet 参考：v2.10.0，`58b59a11350707153f8c48369693ab9beba7b2fa`。
- 新代码根 `bbt/infra/net/`，命名空间 `bbt::infra::net`，C++17，头/实现共置；不建立 CoTCP/CoUDP 兼容壳，不预设连接子类树。
- 新公开目标 `bbt::infra_net`；`BBT_INFRA_NETWORK_ONLY=ON` 在注册任何旧目标/依赖之前分流。旧模式没有变成新交付；数据库、HTTP、RPC、config、MCP 与旧 transport 的源码保留但不参与此配置。
- 本决策只对新 net 契约替代 0002 的身份-only 使用方式及旧同步 Close、0005 的旧网络执行模型；旧模块本包不改写，继续由旧目标表达，不把它们的状态当成新 net 状态。
- **`Engine::Run` 家族门（原实现约束，R2 重建后不再适用）**：旧实现只服务 IPv4 stream，Unix/IPv6 等**合法**但未支持的家族在任何副作用之前返回 `operation_not_supported`，以消除旧版的**假成功**（bind 成功、发布 bound 快照、`error=0`，却零业务事件）。PR #84 重建后 `Engine` 直接以 `Address::Family()` 打开 `NativeSocket`，不再带该家族门；`Address` 仍接受 IPv4/IPv6/Unix 值。当前注册测试只覆盖 IPv4 与 loopback，Engine 上的 IPv6/Unix 端到端**未验证**，不能按原语层能力推定已支持。
- **高层 UDP 暂缓（2026-10-17 用户裁定，仍有效）**：不实现高层 UDP（`Engine`/`Options` 都没有 datagram 参数或入口），只留显式的**使用时编译拒绝** `<bbt/infra/net/Udp.hpp>`（全部内容就是 `#error`；不声明任何类/函数/别名/模板，故不是 API），该头不进聚合头与头自检，并由 `net.udp_high_level_compile_reject` 做正/负编译对照。将来实现时删除该拒绝并按本文目标语义（报文边界/截断/零长、完成项与错误传递、Wake 与事件位、预算与背压）新增真实入口。`NativeSocket` 的 datagram 原语（`SocketType::Datagram`/`SendTo`/`ReceiveFrom`、零长、truncation）属原语层能力、不在高层承诺内、**不是**「UDP 整体未实现」；旧模块 `bbt::infra_udp` 的 `CoUDP`（`include/bbt/infra/CoUDP.hpp`）是独立已交付实现，不属于本暂缓。

## 覆盖标记

本草节是 gnet v2.10.0 的迁移对照分析，**不是当前公共 API 清单**（当前清单见「当前实现落点」）。
状态列含义：**已由 R2 实现** / **未采用**（分析期计划过，R2 明确不做或改名）/ **原语层已实现**（`NativeSocket` 有、Engine 未接）/ **分析期计划**（当时记为 F 或 D3/D4）/ **R**（固定参考自身不支持）/ **U**（平台或运行机制未验证）。
D3/D4 是分析期的阶段标签（P3/P4），阶段编号已随交付取消，不再表示当前缺口；当前缺口以「当前实现落点」与「文件、验证与 CI」为准。

### 核心接口与运行机制

| gnet 符号/机制 | C++ 迁移目标与差异 | 状态/场景 |
|---|---|---|
| Run、Rotate | 现行 `Engine::Run(EngineConfig)`：单/多地址监听、自动绑槽、失败回滚并清理已创建资源；运行中重复 Run → `device_or_resource_busy`，收口后 Run → `operation_canceled`。无 `Rotate` | 已由 R2 实现 |
| 全局 Stop、Engine.Stop | **未采用**：R2 不提供 `Engine::Stop`，收口只有 `Engine::RequestClose` + `Run` 返回；不等待用户回调、资源随所有权销毁、不停止 runtime、不建全局 registry 的原则保留 | 分析期计划；现行口径见「当前核心契约」Engine 生命周期行 |
| Engine.Validate、CountConnections | `Engine::LastError()`/`LastFailure()`/`Phase()` 表达启动与收口错误，`CountConnections()` 报当前连接数 | 已由 R2 实现 |
| Engine.Register、RegisteredResult | 未提供；等价能力为 `EnableAcceptHandoff(capacity)` 的有界 handoff（满则暂停 accept，不丢 fd，`CoAccept` 领取）与 `AsyncConnect`（raw `OnConnect`）/`CoDial`（Co）的建连交付；`WaitAccepted`/`WaitConnect` 是内部步骤 | 已实现（D5 收口） |
| NewNetConnContext、FromNetConnContext、NewNetAddrContext、FromNetAddrContext | 未提供 Go 式 context 注入；不复制 string-key context 协议 | 未采用 |
| Engine.Dup、DupListener | 未提供；`EngineConfig::listen_addresses` 直接支持多 listener，`NativeSocket::Dup` 仍是原语 | 未采用（多地址已由配置表达） |
| EventLoop.Register、Enroll | 内部化为 Engine 的 sub owner 选择 + 唯一 fd 移交（`detail/ReactorDriver`）；不公开 Enroll 入口 | 分析期计划（内部形态已实现） |
| EventLoop.Execute、Runnable、RunnableFunc | 未作为公共入口提供；owner 命令是有界内部队列（配额由 `max_pending_events` 表达），拒绝可见 | 分析期计划 |
| EventLoop.Schedule | 声明旁明确未支持；不把声明当实现，不创造假成功入口 | R：gnet.go:411–415 |
| EventLoop.Close | 未公开；物理收口只由 owner/控制域执行，业务侧入口是 `Connection::RequestClose()` 与 `Engine::RequestClose()` | 分析期计划 |
| Conn 组合 Reader/Writer/Socket | 现行 `net::Connection` 是稳定值句柄（`TryRead`/`TryAccess`/`AsyncWrite`/`RequestClose` + 状态查询），无 Reader/Writer/Socket 子对象，也无 TCP/UDP 子类树 | 分析期计划（以单一句柄形态落地） |
| Context、SetContext、SafeContext、SetSafeContext | 未提供 per-connection context 注入；用户数据由使用者自己在 `Connection::Id()` 之外维护映射，`shared_ptr` 不等于对象同步 | 分析期计划 |
| EventLoop、LocalAddr、RemoteAddr | `Engine::LocalAddresses()` 返回实际绑定地址快照（值语义，不传播 Asio 对象）；无公开 owner 句柄 | 分析期计划（地址快照已实现） |
| Wake、CloseWithCallback、Close | 未提供 `CloseWithCallback`/Close(opt)。`Connection::RequestClose(reason)` 只封口并唤 owner；`AsyncWrite` 的终态经 `OnWrite` 与 `WriteOperation::Query()` 观察 | 分析期计划 |
| SetDeadline、SetReadDeadline、SetWriteDeadline | 不提供连接级 deadline；协程等待用 coroutine 的 `WaitOptions`（`CoRead`/`CoWrite`/`CoAccept`/`CoDial` 均为参数），不改 Reader/Writer 语义 | R（固定参考同不支持） |
| NewClient、Start、Stop | **未采用**：R2 不提供独立 `Client` 类型，client-only 即 `EngineConfig::listen_addresses` 为空的 Engine，不复制第二套 runtime | 分析期计划 |
| Client.Dial、DialContext、Enroll、EnrollContext | 现行形态为 `Engine::AsyncConnect`（raw：`OnConnect` 唯一交付）+ `CoDial`（协程内建连）；`AsyncConnectAwaitable`/`WaitConnect` 已内部化，不移植 Go context | 已由 R2 实现（D5 收口） |
| EventHandler、BuiltinEventEngine | **未采用**：R2 只有 `RawHandler`（`OnAccept/OnConnect/OnRead/OnWrite/OnClose` 默认空实现，在 owner 非业务协程边界短、非阻塞、不可挂起），没有 `OnBoot`/`OnTick`/`OnShutdown*`/`Action`。业务事件若要挂起须由使用者自行转发到自己的协程 | 分析期计划（曾是 `EventHandler`） |
| Action.None/Close/Shutdown | **未采用**：R2 没有 `Action` 枚举与 `OnShutdown`/`OnShutdownRequest`。可复用的原则只有两条：连接收口只执行一次且不可重开；库不替使用者决定「是否关闭整个 Engine」，该决定归宿主控制域（现为 `Engine::RequestClose`） | 分析期计划 |
| main/sub reactor、连接 owner | main 与各 sub 由已登记 Processer driver 槽推进各自 Asio context（不自建线程）；接纳后按 LB 唯一交接；业务协程可迁移 Processer，不是 fd owner，也不重复驱动 runtime 共享 context | 已由 R2 实现（`detail/ReactorDriver`）；ReusePort/UDP/client 布局见执行域设计 |
| LB.RoundRobin/LeastConnections/SourceAddrHash | 三算法由 `LoadBalancing.hpp` 实现并在 Engine 内以短锁串行化，不复现参考 RoundRobin 的并发警告 | 已由 R2 实现 |
| ReusePort 与普通接纳 | `NativeSocket` 层支持 `SetReusePort`（仅 IP），Engine 侧跨 loop 接纳未验收；其他 OS U | 原语层已实现，Engine 侧未覆盖 |
| LT/ET、EdgeTriggeredIOChunk | **未采用**：`edge_triggered_io` 在 `Run` 中明确拒绝（Asio 公共 async_wait 不提供逐 fd EPOLLET 控制）。现行读泵是「有预算非阻塞读到 EAGAIN/预算耗尽即返回，ring 满则暂停、消费后恢复」，不承诺 native LT/ET 事件计数等价 | 分析期计划 |
| ticker/OnTick | **未采用**：`Options::ticker = true` 在 `Run` 中直接 `operation_not_supported` 拒绝，R2 不提供 engine ticker / `OnTick` 业务事件 | 分析期计划 |
| inbound elastic ring、outbound ring+list | 已实现为 `detail/ReceiveRing`（有界弹性 ring，满则暂停读）与 `detail/OutboundQueue`（ring+分段队列，总量与段数有界，保留逐操作身份）；`WriteBufferCap` 不是总量上限，总量由 `max_inbound_bytes`/`max_outbound_bytes`/`max_pending_events` 表达 | 已由 R2 实现 |
| buffer/pool/连接索引 | 优先 Boost/既有容器与 RAII；单 owner 管 mutable buffer，跨协程只持自有拷贝（R2 不再引入不可变事件快照） | 已由 R2 实现（`EventRingBuffer` + `ReceiveRing`） |
| 跨域注册/执行/唤醒 | owner 命令与业务事件分层；等待登记复用 CoWaiter（`WaitWithCallback` + `detail/WakeHub` 有界登记），早到 Notify 不丢失 | 已由 R2 实现 |
| 特殊 netpoll 构建分支/Windows/BSD/macOS | 本包 Linux/WSL 原生 fd 基础；不从 Boost/gnet 支持名单推定本仓支持 | U |

### Reader/Writer/Socket 逐方法

| 固定参考方法 | 迁移语义 | 状态 |
|---|---|---|
| Reader.Read、Next、Peek、Discard、InboundBuffered | 等价能力为 `Connection::TryRead`（原子复制+消费）、`TryAccess`（短保护内 Peek/Consume）、`ReadableBytes`/`ReadEof`/`ReadError`；不等网络，竞争时返回 Busy | 已由 R2 实现（以 ring 访问面形态） |
| Reader.WriteTo | 未提供；使用者自行从 `TryRead` 取数据再写往自有 sink，不在库内对未知代码贴非挂起标签 | 分析期计划 |
| Writer.Write、Writev、Flush、OutboundBuffered | 等价能力为 `Connection::AsyncWrite`（全量/零接纳、逐操作记账）+ `OnWrite`/`WriteOperation::Query()` 的真实前缀与终态；无显式 Flush，也不会把一次调用写成「对端已收到」 | 已由 R2 实现 |
| Writer.ReadFrom | 未提供；调用者自行读取 source 后 `AsyncWrite` | 分析期计划 |
| SendTo | 高层无 datagram 入口（`Udp.hpp` 编译拒绝）；`NativeSocket::SendTo`/`ReceiveFrom` 仍提供原语 | 原语层已实现，高层未采用 |
| AsyncWrite、AsyncWritev、AsyncCallback | `Connection::AsyncWrite(std::string, WriteOperation&)`：取得所有权、全量或零接纳、有界字节/操作数，终态经 `OnWrite` 唯一交付；无 `AsyncWritev`/`AsyncCallback`，完成不证明对端收到 | 已由 R2 实现（单条形态） |
| Socket.Fd、Dup | `NativeSocket::Fd()` 只借观察（不得 close/改阻塞/开正 linger），`Dup()` 返回唯一所有权、cloexec，共享 open-file-description 状态 | 原语层已实现 |
| SetReadBuffer、SetWriteBuffer、SetNoDelay | `NativeSocket` 提供，失败保留 errno；TCP no-delay 对非 TCP 拒绝 | 原语层已实现 |
| SetKeepAlivePeriod、SetKeepAlive | `NativeSocket::SetKeepAlive` 提供 idle/interval/count（秒）；不提供 `Period` 便捷入口 | 原语层已实现 |
| SetLinger | 禁用或 abortive 0 可用；正数可能阻塞 close，在副作用前拒绝 | 原语层已实现（差异保留） |

### Options（覆盖 options.go 的全部字段；WithX 是同字段配置语法）

| 字段 / With 方法 | C++ 去向 | 状态 |
|---|---|---|
| Options / WithOptions | `net::Options` 是公共配置类型（只保留已接线字段）；归一化实现下沉 `detail/OptionsNormalize.*`，`NormalizeOptions`/`NormalizedOptions` 不再是公共入口 | 已由 R2 实现（D3 收敛后） |
| LB / WithLoadBalancing | `Options::lb` 取 `LoadBalancing::{RoundRobin,LeastConnections,SourceAddrHash}` | 已由 R2 实现 |
| ReuseAddr / WithReuseAddr | `NativeSocket::SetReuseAddr`；Engine 侧经 `Options::reuse_addr` 应用到 listener | 已由 R2 实现 |
| ReusePort / WithReusePort | `NativeSocket::SetReusePort`（仅 IP）；Engine 接受该字段但未提供跨 loop 接纳验收，非 Linux U | 原语层已实现，Engine 侧未验收 |
| MulticastInterfaceIndex / WithMulticastInterfaceIndex | 字段已随 D3 删除；组播不在冻结面，`Udp.hpp` 是显式编译拒绝入口 | 未提供（字段已删） |
| BindToDevice / WithBindToDevice | 字段已随 D3 删除；不引入特权路径 | 未提供（字段已删） |
| Multicore / WithMulticore | 字段已随 D3 删除；sub 数量由 `num_event_loop` 显式给出，不按 CPU 自动扩张 | 未提供（字段已删） |
| NumEventLoop / WithNumEventLoop | `Options::num_event_loop`：0 表示未指定（默认 1 个 sub），>0 覆盖，<0 归一化时拒绝 | 已由 R2 实现 |
| ReadBufferCap / WithReadBufferCap | 字段已随 D3 删除；接收容量由 `Options::max_inbound_bytes` 约束，不再有独立 cap | 未提供（字段已删） |
| WriteBufferCap / WithWriteBufferCap | 字段已随 D3 删除；发送容量由 `max_outbound_bytes` 与 `max_pending_events` 约束 | 未提供（字段已删） |
| LockOSThread / WithLockOSThread | 字段已随 D3 删除；R2 不提供 CPU affinity，也不把可迁 Processer 的业务协程锁到创建线程 | 未提供（字段已删） |
| Ticker / WithTicker | 字段已随 D3 删除；无 engine ticker | 未提供（字段已删） |
| TCPKeepAlive / WithTCPKeepAlive | idle 秒，`> 0` 时 Engine 应用 `SO_KEEPALIVE` + `TCP_KEEPIDLE`；归一化处理默认 interval=idle/5、count=5 | 已由 R2 实现 |
| TCPKeepInterval / WithTCPKeepInterval | interval 秒，应用 `TCP_KEEPINTVL`（最小 1） | 已由 R2 实现 |
| TCPKeepCount / WithTCPKeepCount | 正整数 count，应用 `TCP_KEEPCNT` | 已由 R2 实现 |
| TCPNoDelay / WithTCPNoDelay（TCPNoDelay/TCPDelay） | `bool tcp_no_delay`，默认 true，由 `SetNoDelay` 应用 | 已由 R2 实现 |
| SocketRecvBuffer / WithSocketRecvBuffer | 正字节数，应用到 `SO_RCVBUF`；内核可能调整实际值 | 已由 R2 实现 |
| SocketSendBuffer / WithSocketSendBuffer | 正字节数，应用到 `SO_SNDBUF`；内核可能调整实际值 | 已由 R2 实现 |
| LogPath / WithLogPath | 字段已随 D3 删除；不新增 zap 依赖 | 未提供（字段已删） |
| LogLevel / WithLogLevel | 字段与 `net::LogLevel` 类型已随 D3 删除 | 未提供（字段已删） |
| Logger / WithLogger | 字段与 `LoggerCallback` 已随 D3 删除；不导出第三方日志对象 | 未提供（字段已删） |
| EdgeTriggeredIO / WithEdgeTriggeredIO | 字段已随 D3 删除；IO 触发模式不属公共配置 | 未提供（字段已删） |
| EdgeTriggeredIOChunk / WithEdgeTriggeredIOChunk | 字段已随 D3 删除 | 未提供（字段已删） |

## 冻结公共面（D1–D5）

本决策的公共面以 14 个头为准（include 根为仓库根，消费目标 `bbt::infra_net`）：

`bbt/infra/net/{Annotations,ICoObject,CoObject,Address,NativeSocket,Options,LoadBalancing,Types,ReadAccess,Connection,RawHandler,Engine,CoConnection,Error}.hpp`

`detail/**`、私有成员与实现 `.cc` 不在冻结面内（可优化，但须守住下列行为）；`Udp.hpp` 是显式编译拒绝入口，不属冻结面；测试接缝只存在于 `detail/`。

D1–D5 迁移结论：

- D1 隔离内部表示：读写值类型集中在 `Types.hpp`；`Connection` 等只前置声明内部状态，实现在 `.cc`。`Connection::State()`、内部状态构造、公共头对 detail 枚举/类的依赖已移除，调用者迁移到公开值类型。
- D2 读取守卫：独立 move-only `ReadAccess`，短保护内 `Peek`/`Consume`，持连接状态强引用；复制被删除（双持保护），移动/释放顺序即寿命顺序。保活不授予跨挂起持锁权限。
- D3 配置面收敛：只保留已接线配置，归一化下沉 `detail/OptionsNormalize.*`；`multicore`、读写 buffer cap、ET/ticker/锁线程/设备/组播与日志字段及类型删除，`NormalizeOptions`/`NormalizedOptions` 不再是公共入口。这是源码兼容变更，不以「仅内部整理」描述。
- D4 统一 Co 执行域：`CoRead`/`CoWrite`/`CoAccept`/`CoDial` 在消费、接纳或登记之前校验当前受管协程；含已有数据、零长写、现成 accept 的快路径；非法域零副作用，输出按各入口契约处理（失败不覆盖调用者原有 `out`）。
- D5 唯一建连交付：保留 raw `AsyncConnect` + `OnConnect`；Co 用 `CoDial`。`AsyncConnectAwaitable`、`WaitConnect`、`WaitAccepted` 内部化，无 raw handler 的建连走 `CoDial`，Co 接纳走 `CoAccept`，不恢复兼容壳。

冻结的六个方面：公开签名/默认参数/`noexcept`/类型与字段；逐入口错误与输出；线程与执行域；关闭及残留句柄寿命；容量与全量/零接纳；C++17 构建消费与固定依赖。

兼容承诺边界：本次只承诺**源码与可观测行为兼容**；不承诺跨编译器或已编译二进制的 ABI 稳定性（不因无安装包而忽略 ABI 影响）。`Options` 的默认值是冻结契约的一部分，新增字段视为契约变更。公共头正文指纹只能作变更报警，不能单独证明语义兼容。

门禁：`net.freeze_public_surface` 用正/负 TU 对照（正对照只 include 上述 14 头且不引用 `detail/`；负对照逐项证明被删除/收回的符号真实编译失败），`net.freeze_options_contract` 断言 `Options{}`/`ReadResult`/`WriteOutcome`/`EngineConfig` 默认值与空句柄输出。

## Engine 强制关闭修订（分析期记录）

Engine 通常贯穿进程寿命，关闭只承诺停止服务及后续操作安全拒绝，不负责业务优雅排空，不等待 handler 返回，不把全部资源已释放作为返回含义。关联对象按所有权和 RAII 自然销毁；持有关闭句柄不应悬空或维持引用环。关闭必须处理在途网络操作和等待者，不能以停 driver 遗失完成；不强杀用户协程、不要求 runtime Stop、不因未先显式 Close 而 terminate。完整约束见[执行域设计第 6 节](../architecture/gnet-network-execution.md#6-网络关闭不是-runtime-停机)。

分析期针对 `ServerEngine`/`EventHandlerAdapter` 的相位（`Idle/Starting/Running/Stopped`）、Boot 取消、`Action::Shutdown` 上报等讨论只属于被删除的旧实现；R2 重建后 `Engine` 只有 `Run`/`RequestClose`，一次 Run 一个生命周期，重跑返回 `operation_canceled`，没有 Boot 相位、`OnTick`、`Action` 或 `Engine::Stop`。

分析期的连接级 `Close(opt)`（`CloseMode::{Block,Unblock}`、`Open→Closing→Closed` 首胜、Block 仅限受管协程、物理完成与通知分开、FlushPolicy 正交、正 SO_LINGER 拒绝）已由 `Connection::RequestClose` + owner 收口取代，不再有 `CloseMode` 入口。仍成立并可继续引用的原理有四条：

- **物理完成与通知分开**：`RequestClose` 返回只表示封口被接纳；fd 关闭、尾部入 ring、最终 EOF 与 `OnClose` 都由 owner 兑现，`Run` 返回才代表 Engine 侧全部收口。
- **关闭首胜、不可重开**：首次请求的原因固定，后续写失败或重复请求不会重开接纳或覆盖原因。
- **物理完成先于通知**：`OnClose` 在同连接最多一次，且在物理关闭之后送达。
- **不承诺待发数据送达**：关闭丢弃未发送数据、不 flush、不在库内重试；正 SO_LINGER 被拒绝，避免在 Processer 上阻塞 close。

## 参考与许可

- [gnet.go](https://github.com/panjf2000/gnet/blob/58b59a11350707153f8c48369693ab9beba7b2fa/gnet.go)、[options.go](https://github.com/panjf2000/gnet/blob/58b59a11350707153f8c48369693ab9beba7b2fa/options.go)、[client_unix.go](https://github.com/panjf2000/gnet/blob/58b59a11350707153f8c48369693ab9beba7b2fa/client_unix.go)。
- 同 SHA 的 `connection_unix.go`、`eventloop_unix.go`、`acceptor_unix.go`、`load_balancer.go`、`pkg/socket/`、`pkg/buffer/` 为实现语义依据；不是 Go 二进制兼容承诺。
- Apache-2.0 主体与 ring 文件 MIT 适用声明、版权和后续逐文件移植规则见 [gnet 移植出处](../dependencies/gnet-2.10.0.provenance.md)。本包不移植 ring 算法。

</details>
