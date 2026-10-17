# co_net_service — P5 推荐协程网络用法示例

用公开 API（`Engine` + `Client` + `CoTcpConnection`）做真实 TCP server-client，
展示受管协程内的顺序收发等待、跨连接并发进展、同连接串行、分帧/view 寿命、
有界配置与可观察返回、`Close` 双模式与安全进程退出。只消费 `bbt::infra_net`
与 bbtools-coroutine 公共头，不用 detail 头、不用私有 owner/工厂/fd 胶水。

**只适用 IPv4 TCP stream**（本例只演示 TCP 回环）。本仓公共 `Engine`/`Client` 的接纳
路径只接受 IPv4 stream 对端：`Engine::Run` 对 Unix/IPv6 等未支持家族在**任何 socket/线程/
OnBoot 副作用之前**返回 `operation_not_supported`（不 bind、不发布绑定快照、不进入终态，可换
IPv4 地址重试），`Client::Dial` 对 Unix 对端同样返回 `operation_not_supported`。**不适用 UDP**：本仓没有公共 datagram 面
（数据报只在 `NativeSocket` 原语层），且数据报保留报文边界，不套 stream 分帧/半包语义；
高层 UDP 已由用户裁定（2026-10-17）本轮暂缓，使用时的编译拒绝入口是
`<bbt/infra/net/Udp.hpp>`（只含 `#error`，不是 API）。

## 构建

network-only 构建（与测试同一份依赖路径，不新增依赖）：

```sh
cmake -S <本仓> -B <build> -DCMAKE_BUILD_TYPE=Debug \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine>
cmake --build <build> --target Example_co_net_service --parallel 2
```

## 运行

可执行文件位于 `examples/co_net_service/` 子目录下（**不是** `examples/` 直接下）：

```sh
<build>/examples/co_net_service/Example_co_net_service lifecycle
<build>/examples/co_net_service/Example_co_net_service concurrent
<build>/examples/co_net_service/Example_co_net_service limits
<build>/examples/co_net_service/Example_co_net_service errors
```

测试开关（`BUILD_TESTING`）打开时，本目录的 `CMakeLists.txt` 会为四个模式各注册一个
CTest（`example_co_net_service_{lifecycle,concurrent,limits,errors}`），每个都带 TIMEOUT。
`errors` 模式按设计以退出码 3 结束，用同目录的最小脚本 `check_exit_code.cmake`
**精确校验退出码 = 3**；刻意不使用 `WILL_FAIL`，因为它会把断言失败、崩溃、被信号杀死
或 CTest 超时等任何非零结果反转成通过。

| 模式 | 覆盖 | 期望退出码 |
|---|---|---|
| `lifecycle` | Options 归一化自检 → Engine 绑定 port 0 并读实际端口 → 两连接 Dial → 8 轮顺序请求/响应 → 长度前缀分帧（粘包/半包/尾帧保留）→ Wake-only Reader 为空 → `Close(Unblock)` + `Close(Block)` 物理完成 + 物理完成后再 Close 的幂等实测 → `Client::Stop`/`Engine::Stop` → fd 回落 | 0 |
| `concurrent` | A 的事件在受管协程内挂起等对端 gate（对端在 B 回显字节完成前不回显 A）→ 直行协程先发 A、`Wake` A、完成 B 整轮、再取 A；每连接事件串行、每连接活跃峰值 1 | 0 |
| `limits` | 小 `max_outbound_bytes`/`write_buffer_cap` + 不读对端：逐次记录成功接纳 / 资源错误 / deadline 超时（结果未知、不重发），放行后已接纳字节全部回显。**只报记账与不丢数据**，不以本模式宣传背压已覆盖 | 0 |
| `errors` | 受控失败模式：闭环连接 `connection_refused`、真实未决窗口 Dial deadline 由**库内**返回 `timed_out`、非受管线程/受管协程内的域拒绝、Reader 拒绝；计数为实测累计 | 3（按设计非零退出） |

## 阅读要点（规范放在使用点）

- **等待登记先于投递（响应侧）**：`AwaitBytes` 用公开 `CoWaiter::WaitWithCallback`——
  只有登记成功（协程已挂上等待事件）之后才执行 `on_registered` 回调。回调在同一把
  observer 锁内做两件**不挂起**的事：(1) 把本连接的等待者登记进 `ClientObserver`；
  (2) 复查已到字节，若响应在本协程登记之前就已到达，立即 `Notify()` 走 PENDING 早到
  路径唤醒自己。`OnTraffic` 的「追加字节 + 取本连接等待者」用同一把锁，二者必有先后
  可见性，因此**不存在「检查-订阅」丢唤醒窗口**，也就不需要短期限切片轮询兜底。
  回调里不做任何可挂起调用（不投递 `Write`/`Wake`）。
- **请求投递在等待登记之前**：`Exchange` 先 `Write(req)`（只等 owner 接纳）再进入上面的
  等待；响应早到由回调里的复查兜住。等待责任按 connection id 归属，只唤醒「正在等这条
  连接新字节」的协程，其它连接的数据事件不会误唤醒。
- **handler 内的条件等待**：`GateHandler`/`SlowPeerHandler` 用同一模式的 `ConditionLatch`
  在受管协程内挂起等条件成立（有界 deadline），通知方在同一把锁内 `Signal`；不再用
  `bbtco_yield` 自旋等待数据条件，也不把执行线程占住。
- **view 寿命**：`Peek()` 借出的视图只在本次事件内有效。`ClientObserver::OnTraffic` 与
  `EchoHandler::OnTraffic` 都在进入任何挂起点（`Write`）**之前**把字节复制为自有数据，
  之后不再触碰视图；未消费尾部由库保序保留，因此只在确实消费时 `Discard`。
- **接纳 ≠ 发送 ≠ 对端收到**：`Write` 成功只表示命令进了 owner 待发队列。
- **结果未知不重试**：`Write`/`Dial` 的 deadline 到期只结束本次等待，已投递命令可能仍然生效；
  示例只记账、不重发。请求配对是应用层约定（库只保证事件送达）。
- **关闭分界**：`Close(CloseMode::Unblock)` 返回时已封口并可靠投递，不等物理完成；
  `Close(CloseMode::Block)` 挂起等物理关闭完成，但**不等自身 OnClose**；已关闭后重复 `Close`
  是幂等观察，示例在物理完成后再次 `Close(Block)` 做实测计数。
- **收口顺序与推荐用法**：连接 `Close(Block)` → 对端 FIN 驱动的服务端自动收口（服务端 `OnClose`
  送达、链路级计数归零，`lifecycle` 在有界期限内**强停前**实测）→ `Client::Stop()` → `Engine::Stop()`
  → 断言 fd 回落到基线。强停不等 handler/OnClose，不是业务优雅 drain。
  **推荐顺序是先读完对端响应再主动 `Close`**：`docs/decisions/0010` 明确不承诺已接纳的响应送达对端，
  对端 FIN 到达后未发出的响应不可恢复，因此不把「先 Close 再指望残帧/残余响应到达」当用法。
- **进程退出**：既有模型没有 runtime 停机入口；示例在所有业务对象离开作用域后显式 flush 并
  `std::_Exit`。这不是优雅停机、不展开栈，也不作为库资源回收的证据。

## 已知未覆盖（不夸大）

- Reader 完整表 `Read/Next/WriteTo` 未交付：本例只用 `InboundBuffered/Peek/Discard` 最小子集。
- 未覆盖：UDP/Unix 路径、示例内的 `OnBoot` 接线、有界压测与 CI。模块侧的 `OnBoot` 已实现并有
  `net.engine_boot_*` 注册（`bbt/infra/net/EventHandler.hpp:40`、`tests/net/CMakeLists.txt:234-237`），
  **示例未接线不等于模块未交付**；`Action::Shutdown` 请求动作与上报口 `OnShutdownRequest()` 已在
  模块侧实现（本地候选、未独立审查；见 `bbt/infra/net/EventHandler.hpp:64-84`、`Action.hpp`，
  用例注册见 `tests/net/CMakeLists.txt` 的 `net.engine_shutdown_request_*`），
  **已关闭生命周期通知 `OnShutdown` 仍未交付**，是模块侧剩余项；ticker/`OnTick` 已在模块侧实现并
  有 `net.engine_tick_*` 注册，但**本示例仍未接线**（示例未接线与模块未交付是两件事），
  不用示例状态代替模块状态。
- **背压未覆盖**：`limits` 只输出记账与不丢数据；`backpressure_dynamic_hit=0/NOT_COVERED`
  表示本轮未观察到队列满/deadline 返回。配置了小上限**不等于**观察到了命中。原设计 E4 的
  有界背压断言已被 examples-review-08 撤销，本示例不以 `limits` 通过宣传背压已覆盖。
- **封口后 `Write` 的错误码**：实测返回 `operation_not_supported`（errno 95）。这与公开契约
  `ICoConnection.hpp:47-50`（「非 Open 状态由物理连接拒绝（operation_not_supported 等）」）一致：
  示例据此只断言它落在契约给出的「非 Open 拒绝族」内，不把该码冒充 closed/canceled 语义
  （封口/关闭路径的 `Wake` 才是 `operation_canceled`）。
- **服务端对端 EOF 收口（已集成实测）**：`lifecycle` 现在在 `Engine::Stop()` 之前用有界期限等到
  `server_closes=2` 与 `conns_before_stop=0` 并断言（不是观测记录）：客户端读完 8 轮响应后 `Close(Block)`，
  服务端由对端 FIN 自动收口并在强停前送达 `OnClose`、链路级计数自然归零。此前候选上 `server_closes=0`、
  `CountConnections()` 只在 `Stop()` 后归零的观测线索在本候选上不再复现，收敛到
  `docs/decisions/0010`「对端 FIN/RST 收口」契约。仍不覆盖：`OnClose` 与 `Engine::Stop` 并发竞争、
  「两次提交都未接纳后长期无任何可达事件」的边界。
- **等待槽位单实例上限**：`ClientObserver` 每条连接只有一个等待槽、`ConditionLatch` 单槽（见用点注释）；
  本例每条连接同一时刻只有一条等待链路故未触发，同一连接上多协程同时等待会退化为等 deadline。

## 交付状态（候选，未合并）

- 本示例与网络模块同属**未合并候选**：基线 `3136d1de5031a2eec55475e770d5ebbadc3c32da`（分支
  `refactor/infra-gnet-cd13ff42`），net 模块源码在本工作区仍为未跟踪改动，未 commit/push。
- **不是 P5 完成**：本示例只覆盖 TCP 回环的公开 API 用法。示例侧未覆盖：UDP/Unix 路径、`OnBoot`
  接线、有界压测与 CI。模块侧 `OnBoot` 已实现（`bbt/infra/net/EventHandler.hpp:40`，用例注册见
  `tests/net/CMakeLists.txt` 的 `net.engine_boot_*`），`Action::Shutdown` 请求动作与
  `OnShutdownRequest()` 上报口已实现（模块侧，本地候选未审查；用例注册见同文件的
  `net.engine_shutdown_request_*`），**`OnShutdown` 生命周期通知仍未交付**（`EventHandler.hpp`
  的未交付注释，与请求上报口 `OnShutdownRequest` 是两件事）；ticker/`OnTick` 已实现（模块侧，
  本地候选未审查）但本示例未接线；
  **示例未接线与模块未交付是两件事，不混写**。
- 业务优雅 drain（runner/Engine 级排空）与 graceful runtime 停机是**明确的非目标或另行任务**，
  不作为本示例或 P3/P5 的缺口门槛：`docs/plans/gnet-network-migration.md:14`（不建设业务优雅
  drain）、`docs/plans/gnet-network-migration.md:137`（优雅 runtime 停机另行任务）、
  `docs/decisions/0010-gnet-network-foundation.md:122`（关闭不负责业务优雅排空）。本示例只用强停 +
  显式进程退出。
- 旧 transport 消费者切换**待核**：`docs/plans/gnet-network-migration.md:72` 只把「需旧
  transport/stub 才能跑」列为 P5 的按需未完成条件；本包未验证是否仍需要，故不写成已判定缺口，
  也不自加门槛。
- 对端 FIN 自动收口（含每事件有界重试口径）是**已独立复审的窄增量**，不构成整体 Engine/P5 验收；
  契约措辞以 `docs/decisions/0010-gnet-network-foundation.md` 为准。
