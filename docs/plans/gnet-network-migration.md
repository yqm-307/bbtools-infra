# TCP 网络实施接续：R2 契约对齐

**状态：核心契约已由用户审阅确认；本轮仅获授权对齐两仓文档，尚未启动 R2 产品实现、构建或集成验证。** 当前目标以 [执行域设计](../architecture/gnet-network-execution.md) 与 [0010](../decisions/0010-gnet-network-foundation.md) 为准。下面的旧 P0–P7/C1–C14 计划保留为历史，不是当前任务状态，也不自动授权任何实现、提交或发布。

## 当前输入与边界

- infra 源码依据 `2cb3ebf60a101bb9cbf93f065db4d41e2130d3cd`；coroutine driver 已合并于 `1e9f0e8c25de85dde003def65bb58b90bdac0e0b`（PR #396；原分析 head `8d7b4383c8631f4dccad74df088abe54ca000b87`）。文档修订不升级构建 pin，不伪称两仓已联调。
- 不再延续旧独立网络线程、事件快照为接收真源、FIN 自动关闭、gnet 全签名对照或旧 Close(opt) 作为 R2 必做条件。旧能力不删除、不改消费者。
- R2 范围为 TCP 底座；raw 回调 + 既有同步原语薄封装，同一接收 ring 与网络内核，稳定可靠机制与事实报告归 net，上层保留业务策略。
- 原有有效基础实现/测试可复用，但不能将旧候选测试和压测自动记为 R2 已通过；高层 UDP、HTTP/RPC 接入与消费者迁移不在本次范围。

## 后续出口（非已授权施工清单）

1. 两仓文档按 R2 一致修订并独立审查；在需要发布/合入时另取授权。coroutine §11.5 对齐 owner 原始回调与受保护共享 ring，§11.3–§11.4 的回调租约/锁外调用/同代解绑不变。发布前只交本地候选，不把旧“文档不阻塞 P3”当作跳过当前契约协调的许可。
2. 取得实现授权后，固定实际消费源码/契约版本和链接来源，先核验最小真实 Asio driver（Tick/Wait/Wake/预算/解绑）再落地 Engine/Conn；共享公共契约由一个负责人协调，不重复建设已有 foundation。
3. 连贯完成 raw TCP server/client、并发 ring、发送操作/背压/终态、Run 收口与 Co 薄封装；开发中只做聚焦检查，收敛后合并跑本地冒烟、本功能与直接耦合测试。性能先建实测基线，不预设硬门槛。
4. 固定最终候选→独立审查及必要修复复审→按授权提交/PR→当前 head 必需 CI/可合并状态验收；不自行合并。消费者迁移另议，不借此改 HTTP/Redis/Mongo/config 或 framework。

---

<details>
<summary>旧阶段计划与局部验收历史（不作为当前施工指令）</summary>

原计划中的负责人、授权、阶段勾选、线程/快照/Close 规则和“下一步”均仅属于当时版本，不覆盖上文；其中真实证据按原版本保留，不能用于宣称 R2 已实现。

## 2026-10-16 Engine 生命周期修订（待实施）

本轮修正了 P3 rollback 故障注入：测试在 `NativeSocket::Close` 的 Release 前边界注入两次失败，
不再在旧 `close` wrapper 已 Release 后伪造仍可重试的 fd。rollback 仍是“两次持续失败”专项，
保留原错、busy、显式 Stop 与 FD/线程基线断言；这不等同于一次性故障或 F4 整体收口。
Detach 现在在清空 registry 前同时要求 `PhysicalCloseSettled`，避免清表后以空 registry 自证 readiness。
68 报告中关于 rollback fixture 的“真实关闭重试”表述已被上述边界纠正；最后引用析构策略、Accepted 晚命令和
runtime 内部限制仍是未解项。

用户确认通常全局、从启动使用到结束：Engine 采用强制停止服务，关闭后拒绝提供服务；关联资源由对象所有权和 RAII 自然销毁。不建设业务优雅 drain，不等待 Handler/OnClose 返回，不要求全部句柄释放才算关闭，不采用“未先成功 Close 就析构则 terminate”的候选策略。具体边界以[执行域设计第 6 节](../architecture/gnet-network-execution.md#6-网络关闭不是-runtime-停机)与 0010 为准；先前将 drain/join/全部 FD 回落绑定到 Engine Close 返回的候选不再适用。

P3/P4 下一实现切片应验证关闭后的新操作拒绝、关闭与接纳/在途操作竞争、等待者关闭结果、残留句柄安全；另在用户释放自有引用、网络操作和线程实际结束后验证最终资源回落。自然销毁不能成为悬空/引用环/遗失等待或未回收线程的借口。内部 bridge drain 的既有测试不删除，但不是新 Engine 的业务成功条件。既有连接 Block/Unblock 物理完成契约保留；关闭停止服务与资源最终销毁两个时间点明确分开。

本段是新目标与验收口径，非已实现/测试通过声明；独立审查、真实测试与最终 PR/CI 仍须完成。

## 1. 目标与非目标

在 **bbtools-infra** 迁移 gnet 的 Engine/EventLoop/Conn、Handler/Action、Client/Options、socket/地址/缓冲与必要支撑，通过现有 bbtools-coroutine 实现可挂起的 Co 业务接缝，交付真实 example、有界压测及 **infra PR 当前精确 head 必需 CI 绿、无冲突可 merge**；不自动 merge。

- 线程/owner/bridge 的最新契约见 [执行域设计](../architecture/gnet-network-execution.md)；逐方法/Option、CoObject、标签、视图与 Close 见 [0010](../decisions/0010-gnet-network-foundation.md)。图表达目标，不代表已实现。
- 模仿 gnet 的职责与高效 owner/pump/事件思路，不逐字节翻译 Go，不移植其调度器。ET/LT 是可选读泵策略，不能把 Asio 是否公开 native flags 当作整个网络设计的前置阻塞。
- `ICoObject` 统一感知受管协程与限制调用域；不固定 `RunInProcesser` 方法名，不用“线程允许协程”代替“正在受管协程”。非挂起标签与上下文限制正交。
- 新代码 C++17，`bbt/infra/net/` 头/实现共置，目标 `bbt::infra_net`；不增加旧 API 别名/转发壳/第二 runtime。旧活动网络路径仅在新路径及消费者映射已核后清理。
- Redis/Mongo/HTTP/RPC/config/MCP/旧 transport 功能源码不重构，network-only 下完整排除构建、链接、头自检、示例与本轮 CI 依赖准备；不将排除视为通过。framework 不改。

### 文档里程碑与后续实施的围栏

文档冻结阶段只更新 infra 文档与 plan；coroutine 子代理只更新 runtime 相关 Markdown、走独立文档 PR，用户 merge，不改产品代码、头文件注释、测试/CMake、CI 或规则。该围栏不扩张到已授权的 infra 后续实施：infra 设计审查无阻塞后即可推进 P3/P4，不等 coroutine 文档修复、PR 或合并。**infra 文档随本次重构 PR 的 commit 提交，不另开文档 PR；不为该线程设计新增 coroutine 产品兼容层。** 以下待实施项不能因计划已写就标为已完成。

此前最小环境查询 U1 修复已获跨仓授权，和本次 docs-only 是两个独立范围。现有公开 `CoWaiter` 自定义等待/载荷通知及任务注册已具备所需接缝；infra 负责登记后投递、每操作状态/寿命与网络语义。仅当后续实测违反已有公开契约时才附固定 SHA/最小复现向上游报缺陷，不预建兼容层；本次文档权限不能用于新的 coroutine 产品修改。

## 2. 已有产物与固定版本

| 对象 / 阶段 | 实际版本或产物 | 状态 / 不可推导的结论 |
|---|---|---|
| infra 基线 | `3136d1de5031a2eec55475e770d5ebbadc3c32da` | 原隔离分支上的基础候选尚未提交；不是 main 全模块回归 |
| gnet v2.10.0 | `58b59a11350707153f8c48369693ab9beba7b2fa` | 固定行为与许可参考，不承诺任意 Go 库零修改兼容或性能相等 |
| coroutine main 基线 | `39fdfff71a8a66d98a874b2e71408f9936b1166b` | 本轮 runtime 文档 PR 独立基于它，不夹带查询修复 |
| coroutine U1 消费 | `8ae6a88997c5acaa4fe03430c35e1820ed5b29b0`（[PR #392](https://github.com/yqm-307/bbtools-coroutine/pull/392) 的 mergeCommit，tree `89c5012ca22d06a226727943a89dd7f08548b8cc`；父 `6dfda8158d76f5030cde50a9fbc3c97a8335e932` 为[查询无创建修复 PR #388](https://github.com/yqm-307/bbtools-coroutine/pull/388) 的 mergeCommit） | 两项均已合并；消费标识为落地 merge commit + 其 tree，不用 PR head、不用可变 main。本轮从该固定 commit 干净重建，固定版本影响面 107/107；旧 dirty 候选的 16/16、98 项与 P6 证据不作固定 commit 依据 |
| P1/P2 与纯逻辑包 | network-only、环境门禁、NativeSocket/Address、Options/LB、有界 ring/outbound queue | 已局部验证并独立审查；精确 U1 消费新建 Debug/Release 各 16/16；Engine/Conn/Bridge/Close/压测/infra CI 未交付 |
| P3 局部探针 | 独立事件 owner、部分读再次就绪、撤销回调时序 | 只说明被测包装/fixture；撤销栈重入不是已证 UAF，不要求先重写 coroutine poller |

已有环境/构建/测试命令与 SHA/实际链接核验见 [README](../../README.md) 和 0010。每个后续候选仍须核自己的源码、实际 DSO、真实测试与审查；不能沿用旧候选测试作为新 head 的最终验收。

## 3. 新增 D2 文档冻结里程碑

| 步骤 | 负责人 / 写入 | 产物与就绪条件 |
|---|---|---|
| D2-a 两仓共同契约 | Alice 决策；先冻结输入再独立写作 | main/sub/Scheduler/Processer 对物理线程关系；共享 executor 不被重复驱动；保留内部 CoPoller、Hook/Timer 与三种启动模式 |
| D2-b coroutine 文档候选 | 子代理，独立 docs 分支/worktree | 权威 runtime 契约 + 最小入口修订；Mermaid 图、当前实现/目标差异；纯 Markdown diff，不改 .hpp/.cc |
| D2-c infra 文档与 plan | Alice，既有隔离候选 | 本计划、执行域设计、0010/README 适用范围；文档阶段不改变已有代码字节，之后随同重构 PR 提交 |
| D2-d 独立审查 | 未参与写作的人，只读最终两仓候选 | 检查跨仓一致性、目标/实现界线、线程/driver/owner/等待/寿命、图与链接；修复后复核增量 |
| D2-e coroutine 文档 PR | 审查通过后由子代理发布，Alice 回读 | 精确 commit/分支/PR、docs-only diff 与 checks/CI 分类；无条件 skip 不能冒充网络测试通过；用户手动 merge |

D2 不通过删掉 P3–P7 来“完成”总任务。infra 文档审查已无必须修项，可继续实现；coroutine 文档旧停机语义定向修后复审并独立走 PR，不把它的发布/合并设为 P3 前置。runtime 文档 PR 的合并不代表 infra 网络能力已上线；infra 文档最终随产品候选一并交付。

## 4. 实施阶段、依赖与回退

每阶段只有一个产品实现负责人；共享接口先冻结，同候选不并行改写。按完整交付批次推进，采用「集中开发 → 集中验证 → 最终审查 → 授权提交」，不把每个小迭代当成独立发布或反复启动整套验收。

- **集中开发**：连贯完成批次内相关功能与修复；开发中只做定位问题必需的增量编译、聚焦用例与必要红绿验证，不逐小项重跑整组回归或安排完整独立审查。
- **集中验证**：候选收敛后，按最终影响面合并跑一次冒烟、新功能及直接耦合单测，去重测试集合；本地不跑全量，全量走 PR CI。失败须定位修复并重验，不以合并验证为由跳过失败。
- **最终审查**：对批次最终完整 diff 做一次独立审查，复用同版本且覆盖有效的构建和测试证据；审查者只补与风险或证据缺口直接相关的检查，不无差别重复实现者的整组测试。审查修复仅重验、复核修复增量及其影响；扩大范围或旧证据失效时，相应扩大验证与复审，不机械重启全流程。
- **授权提交**：批次验证与适用审查通过后，按用户明确授权统一提交，不逐小内容单独提交；提交、推送、合并分别遵守授权边界，流程讨论本身不授予这些权限。安全检查、公共契约与生命周期门禁不因合批而削减。

下表各阶段的验证项是批次验收出口，不要求每个小改动都重复执行。

| 阶段 | 工作 / 范围 | 产物与验证 | 前置 / 停止条件 / 回退 |
|---|---|---|---|
| P0 契约/API 清单 | gnet 核心符号/Option、环境、标签、Close、许可及构建矩阵；D2 修订线程模型 | 0010、执行域设计、逐方法差异，C1–C14 有场景 | 当前 D2 审查通过后进入受影响实现；重大业务取舍/权限缺失暂停，回上个已审候选 |
| P1 构建与上下文 | network-only target/CTest/compile/link/selfcheck 闭包，合法公开查询 | 基础候选已核；后续每次改动验无排除模块，普通/TLS-idle/受管协程与无隐式创建 | P0；真实上游缺口则只阻塞依赖项，不造同名类型；回上个已审 P1 候选 |
| P2 socket/地址/选项 | TCP/UDP/Unix/IPv4/IPv6、nonblock/cloexec、accept/connect/dup、必要选项 | 基础候选已核；保留 EINTR/EAGAIN/EINPROGRESS、partial、EOF/truncation、flags 与单一关闭等回归 | P1；需特权/依赖/安装未授权则停；回 P1 候选 |
| P3 Engine/EventLoop/Conn | 自有 main/sub/context/thread、ReusePort/LB/client、交接、Handler/Action、任务/通知接缝、ticker | 实际线程/context 身份、单 owner、单次 fd 交接、业务事件序列化、部分启动失败、队列满失败路径 | infra 架构审查、P1/P2；不依赖 coroutine 文档 PR 合并；真实公开契约缺陷或必须越权时停；回 P2 候选 |
| P4 buffer/Writer/Close | 消费快照/视图、同步/异步写与回调、有界背压、读策略、Block/Unblock、标签 | 真 I/O、partial/EAGAIN、挂起不堵 worker、早完成/超时/取消、混合 Close/晚回调/fd 复用/引用寿命 | P3；假 Closed、悬空 view、丢唤醒或无界增长必须停并修根因；回 P3 候选 |
| P5 example/consumer | 推荐协程内用法与技巧/规范、TCP/UDP/Unix 适用路径、client/server、业务事件与等待、轻量帧处理例 | 按下文示例设计与验收要求改造；只用公共头/target，真实对端核内容/顺序/错误/关闭，不引入大协议库 | P4；需旧 transport/stub 才能跑则未完成；回 P4 候选 |
| P6 有界压测 | 短冒烟后稳定连接/churn/慢对端/跨 loop/等待关闭竞争 | 当前候选 SHA、环境、预算、命令/退出码、吞吐/延迟/错误/RSS/fd/队列/缓冲及回落；无可比基线不造提升百分比 | P4/P5、资源监控/清理就绪；冒烟失败、超限、异常增长或预算耗尽立即停；回 P5，产品修复回原阶段复审 |
| P7 CI/清理/infra PR | network-only clean Debug/Release、精确测试/example/负载注册与汇聚、旧活动路径清理、最终审查 | C1–C14 与当前 head 独立审查/真实 CI/mergeability；无兼容壳/最终双轨，用户 merge | P5/P6；新依赖/共享 runner/callee/仓保护权限不扩张；保留可恢复 SHA，无 CI/审查证据不称可合并 |

**UDP 暂缓（2026-10-17 用户裁定）**：高层 UDP（`Engine`/`Client` 的 datagram 面）本轮不实现，只留显式 `#include <bbt/infra/net/Udp.hpp>` 的**编译拒绝**入口（该头只含 `#error`，不进聚合头与自检）。上表 P2/P5/P6 与 §5 C1–C14 里 UDP 相关的覆盖项**保留**（用户裁定的是"本轮不做高层 UDP"，不是删除矩阵），但 UDP 不构成本轮任何阶段的必过阻塞；`NativeSocket` 的 datagram 原语（P2 socket 基础的一部分）继续可用，不写成"UDP 整体未实现"或永久废弃。恢复实现时删除该拒绝并按 0010 目标语义新增入口。

### P5 示例设计与验收要求（待实施）

示例是推荐使用方式的可执行说明，不以“socket 能连通”或底层 consumer 冒烟代替。现有基础 consumer 仍保留其构建/链接验收职责；面向用户的示例需另外设计并改造，体现库在协程编程中的便利、使用技巧与规范。此处是计划要求，不是已有能力或示例已完成的声明。

- **先设计使用路径，再实现示例**：从启动、注册业务 Handler、受管协程内收发/等待到连接与引擎收口，给出最小推荐流程与简短说明。应用只消费公开 API 和 `bbt::infra_net`，不得要求用户接触私有 owner、factory、waiter 接线或 native fd 管理；公共入口不足时回到 P3/P4 补契约，不用示例内私有胶水掩盖。
- **体现好用之处**：至少一条真实请求/响应或轻量分帧路径以顺序协程代码表达等待，不迫使业务手工搭 callback 状态机；通过可观察场景展示一个 Handler 挂起时另一连接仍能进展、同连接事件保持串行。说明哪些便利由库负责，哪些状态/业务协议仍由应用负责，不虚构性能优势。
- **配套实用技巧**：展示按需消费缓冲与分帧、view 的有效期和跨事件保存数据的方法、有界待发与慢对端处理、deadline/错误检查、结果未知时不盲目重试、显式 `CloseMode` 与关闭完成点。以最终公开接口为准，不预写不存在的方法；TCP/UDP/Unix 的差异明确标注，不把 TCP 帧语义套给 UDP。
- **把规范放在使用点**：标清仅受管协程可调用的业务入口、非挂起标注不等于允许协程外使用、不能在 Handler 中用阻塞系统等待占住 Processer；示例展示生命周期拥有者与正确收口顺序，不在析构中阻塞、不用 runtime Stop 代替网络排空。必要反例短小说明原因，不增加第二套运行时或大协议依赖。
- **可复制、可运行、可验证**：配套公共消费构建、运行命令与预期可观察结果；真实对端检查消息内容/顺序/失败/关闭，动态端口和自有资源，异常退出也收口。分别验证正常路径、挂起时并发进展、deadline/背压失败路径和关闭资源回落；有限规模示例测试与压测分开，示例不能靠 skip、mock 或私有实现才能通过。
- **独立验收与入口同步**：非实现者复核“推荐用法”是否真由公开契约支持、技巧是否安全、命令是否可执行；当前候选实跑后再更新 README/示例入口与限制。P5 未完成时 P7 不得宣称面向用户的示例交付完成。

### P3/P4 先后顺序与共享根因

先做最小两域真实 bridge：业务受管协程登记等待 → owner 命令 → 非阻塞结果 → Notify → Processer 恢复，覆盖早完成/登记失败/队列满；随后接单连接与生命周期，再扩多 loop、读取策略与压力。不要先并行建立互相不兼容的 Handler、等待、Close 三套状态机。

网络 reactor 线程不是 runtime worker；等待代码不能复用共享 context 作为网络 driver。若某底层包装不满足撤销寿命，优先在 infra adapter 定义并验证最小安全方案；只有通用等待/唤醒公共接缝经实测确有缺口才提出新的 coroutine 产品 PR。

## 5. C1–C14：原出口保持，场景按新架构补齐

| Criterion | Task | 必须可核验的场景 |
|---|---|---|
| C1 gnet 抽象/API/Options | P0/P3 | 符号/Option/差异覆盖；不预设连接类树或固定 RunInProcesser；gnet 回调栈与新业务协程的刻意差异明确 |
| C2 ICoObject 环境限制 | P1/P4 | 普通线程、TLS-enabled idle 非法等待副作用前拒绝；受管协程可用，恢复后现查；owner 与协程判定不混用 |
| C3 非挂起标签 | P0/P4 | 新 Co 方法完整路径分类，override 一致，C++17；不以 noexcept/const/inline 推定不挂起 |
| C4 reactor/owner/投递 | P3 | 独立 main/多 sub/context/线程身份、LB/ReusePort、单移交与关闭竞争；无重复驱动 runtime context |
| C5 socket 必需支撑 | P2 | 真实 TCP/UDP/IPv4/IPv6/Unix、选项/flags/dup/失败清理；平台缺口逐项明示 |
| C6 事件/协程语义 | P3/P4 | Handler/Action 顺序，业务 handler 挂起不堵 reactor 或 worker，同连接不重入；完成只置 runnable 不直接 Resume |
| C7 缓冲/写完成寿命 | P4 | 半包/粘包/绕环、快照/view、异步保活、partial/慢对端、接受/发送/对端结果区别 |
| C8 Close 资源安全 | P4 | 混合 Block/Unblock、物理完成与 OnClose 分开、在途 callback、晚事件/fd 复用、引用释放、Engine 自有线程回收 |
| C9 有界资源/公平 | P4/P6 | 队列/待收待发边界、高水位恢复、两种读泵预算无饿死、不每 readiness 无限建协程、资源回落 |
| C10 上层完整排除 | P1/P7 | target/CTest/compile_commands/link/selfcheck/CI 依赖无排除模块，残留 prefix 不能悄悄启用；不称旧模块回归 |
| C11 写入围栏 | 全部 | 产品重构只在 infra；原 U1 与本次 coroutine docs-only 例外分别审查/分支/PR，不改 framework/上层源码/共享服务/规则 |
| C12 example/移植接缝 | P5 | 公共 target 真实 client/server 与轻量帧处理，错误/关闭可见，不凭编译就称运行通过 |
| C13 新鲜压测 | P6 | 冒烟后按固定负载预算，机器汇总与原始证据绑定当前 SHA；未执行/超限/意外 skip 不算通过 |
| C14 PR 可 merge | P7 | 未参与实现者审查、最新 head 必需 CI 绿、无冲突、可合并；不自动 merge。D2 的文档 PR 单独验，不替代产品 PR |

## 6. 文件、执行与资源围栏

- D2 写入：`README.md`、`docs/decisions/0010-gnet-network-foundation.md`、本文、`docs/architecture/gnet-network-execution.md`。coroutine 仅自己的权威 runtime 文档/必要 Markdown 入口；不把报告/会话/私有路径写入产品仓。
- 后续产品范围：`bbt/infra/net/`、`cmake/NetworkOnly.cmake`、必要根/测试/示例注册、`tests/net/`、公共 examples/负载 harness，以及本次 CI workflow/配套脚本。新耦合先补逐文件清单；root/build 由一个负责人汇合，不并行改共享索引或 build。
- 不动共享 CI callee/runner/仓保护、系统安装/新依赖、部署/发布或共享服务；不借可回滚扩大权限。旧网络删除/调用者切换必须先核行为与兼容影响，不静默删消费者正在使用的能力。
- 新 configure 必加 `-DCMAKE_CXX_COMPILER_LAUNCHER=ccache`；本地 `JOBS=$(( $(nproc) / 4 ))`，最小 2；增量明确 `--target ... --parallel "$JOBS"`。全量不在本地重复，CI 按既有授权范围推进。文档阶段跑 diff/链接/围栏检查，不假造 build/test。
- P6 沿 0010 的**待验证首轮预算**：2 loop、2 Processer、最多 32 连接、1KiB 包、每场景 10 秒/总 60 秒、RSS 256MiB/fd 256、每连接待收待发各 1MiB、排队总 1024。监控与超限清理未就绪前不启动；后台 Scheduler/main 的开销也需记入总资源，不只数 sub/worker；不擅自增预算。
- 每包固定候选后独立审查，修复回原负责人再定向复审。连续两次无新证据不重复同法；资源/权限门禁到达就交已有结果和恢复条件，不放松断言/skip/超限来换绿。

## 7. 本轮交付与剩余事项

文档阶段出口：两仓文档/架构图、仓内最新 plan、独立审查结论；coroutine 文档 PR 与精确 head 的真实检查状态独立收口，merge 留给用户。infra 文档不单独发布，随本次重构 PR 的 commit 一并交付；网络实现按下述剩余阶段继续，不等待 coroutine 文档合并。文档图用 Markdown Mermaid，结构/链接检查与实际渲染的证据分开，不把未渲染说成已看过效果。

总体仍有 P3/P4 完整 Engine/Conn/Bridge/Close、P5 真实 example、P6 有界压测及 P7 infra PR/CI；它们不是本轮文档变更已完成的能力。API 错误/拒绝映射、物理关闭完成点、buffer 接线及平台/资源场景仍须对应实现与测试，不能从架构成立推导通过。

### 2026-10-16 owner 安全脱离切片（私有 bridge，已局部验证）

在既有私有 bridge 上补齐强制停止后的资源收口顺序：`StopServiceOwner`（物理全闭）→ `RequestStop`/`Join` → `DetachOwner`（在 owner context 仍存活时断开每会话 owner 域 backend 并释放 registry 强持有）→ context/facade 自然销毁 → 残留 shell/handler 收尾；同时修掉三处缺陷：强停后内部快照归还被服务门误拒（假错误计数 + 永久 pending）、registry 清空后 `Drained()` 虚报、桥锁下析构用户对象。调用顺序与边界见[执行域文档 §6 末段](../architecture/gnet-network-execution.md#owner-安全脱离的调用顺序私有-bridge-切片已局部验证)。

补充两个边界回归：owner 已 `Join`、尚未 `DetachOwner` 时 handler 归还快照，不留 pending 或虚假错误；脱离发布后不再复制会话列表，在控制线程 C++ 分配被拒绝时仍真正释放 backend，随后才销毁 context。两例均先在修复前实际失败，再在修复后通过；不以异常罕见为由接受半脱离状态。

边界：这是私有 bridge 的局部切片与本地验证（真实 TCP 动态端口 case + 既有耦合回归），**不是**公共 Engine 关闭契约的实现；Engine/accept/connect/UDP/example、Engine 级 drain 门槛、业务优雅排空与 PR/CI 独立验收仍未完成，本段不代表可合并。

## 8. 测试进程退出边界（2026-10-16）

按用户决策保持 runtime 无停机模型：不恢复 `SchedulerStop`、不修 coroutine 上游。已启动 runtime 的测试入口统一经 `tests/net/ProcessRuntimeTestMain.hpp` 的 `RunTestProcessExit` 收口（原退出码/异常/abort 语义不变，业务 RAII 先于 `_Exit` 完成）；测试进程静态析构竞态（GDB27 erase 死表 SIGSEGV）记录为已知限制，未修复。验证边界与写法见[执行域文档 §9](../architecture/gnet-network-execution.md#9-测试进程退出约定与已知限制)；需要验证静态析构/全局资源寿命的用例另设生命周期测试。优雅 runtime 停机与普通 return 安全性仍是剩余事项，另行任务。

## 9. 首包公共 Engine 实施切片（2026-10-16，本地验证，未审查/未合并）

在 P3-c1 私有 bridge 之上落地公共首包闭环：新增
`bbt/infra/net/{Action.hpp,EventHandler.hpp,Engine.hpp,Engine.cc}`、
`detail/{ServerEngine.hpp,ServerEngine.cc,EventHandlerAdapter.hpp}`、
`tests/net/engine_server_accept.cc`（目标 `Test_net_engine_server`，5 个 case）；
`bbt/infra/net/CMakeLists.txt` 接入编译与单头自检，`tests/net/CMakeLists.txt` 注册
`net.engine_*`。实际能力、强停顺序与未交付项见[执行域文档 §10](../architecture/gnet-network-execution.md#10-首包公共-engine2026-10-16本地验证未审查未合并)。

本切片不改变 0010 的后续目标，也不把下列能力记为已完成：`OnShutdown`（已关闭生命周期通知）、
协程发起的强停、Engine 级优雅排空/FlushPolicy、
多 listener、UDP/connect/client、example 与有界压测（P5/P6）、PR/CI 独立验收（P7）。
`Action::Shutdown` 已按**本轮（2026-10-17）用户裁定**实现为**向上请求上报**
（`EventHandler::OnShutdownRequest`，由 `EventHandlerAdapter::ApplyAction` 在同一受管协程任务内
转发；本地候选、未独立审查、未提交，旧的「Shutdown 自动收口」表述作废）；ticker/`OnTick` 亦为本地候选实现（见执行域文档 §10 的 ticker 事实行）。
内存约定精度：本切片新增代码使用普通 `new`/`make_shared`，无 aligned 分配声明。

## 10. Engine 计数/寿命修订 F1/F2（2026-10-16，本地验证，未审查/未合并）

按 engine-review-42 的 must-fix 处置 F1（`OnClose` 跨 Engine 销毁的裸计数指针 UAF）与
F2（`OnClose` 抛出跳过减计数）：活跃计数改为共享 `std::shared_ptr<std::atomic<std::size_t>>`
（每 `Run` 一代，适配器与 Engine 共享，不持 Engine/handler），`OnClose` 用 RAII guard
完成恰一次减计数并跨挂起保活计数；停止不再清零陈旧计数。真实 TCP 新 case
`net.engine_onclose_throw_count`（F2 先红后绿）与 `net.engine_onclose_suspended_stop_destroy`
（F1，含白盒共享计数 weak/value 证据）。F3–F8 及 §9 未交付项仍未完成；本段不代表可合并
或 Engine 验收，独立复审仍须。


## 11. Engine 拥有型 Handler 入口（2026-10-16，本地验证，未审查/未合并）

按用户确认的所有权变更，公共 Engine 改为唯一拥有型入口
`explicit Engine(std::shared_ptr<EventHandler>)`：Engine/ServerEngine/适配器按值共同持有，
无借用重载、无 no-op deleter 假拥有；`Run` 对空拥有关系在任何副作用前拒绝
`invalid_argument`。`BootHandler` 闭包改为只持 handler+promise，不持 ServerEngine，因此
挂起 `OnBoot` 不延长 Engine 寿命、10s 超时语义不变且不需要取消任务；析构线程约束见
[执行域文档第 12 节](../architecture/gnet-network-execution.md)。未销项（F3–F8、
LC/namespace、无 ASan/Release/全量 CI）同上；本切片不代表 Engine 验收或可合并。

## 12. Engine 启动/观测/并发停止收敛（2026-10-16，本地验证，未审查/未合并）

按 engine-review-50 的 must-fix 处置 F3（未开始 Boot 取消）、F6（OnBoot 观测堵 worker）、
F7（facade 竞态与 Stop 终态）：`ServerEngine` 相位扩展为 `Idle/Starting/Running/Stopped`，
Run 在锁内起 owner/绑定并发布 `Starting`、在锁外等 Boot；Stop 可对 `Starting` 真实收口且不等
OnBoot；每 Run 的窄 `BootResult` 以 `Pending→Running` / `Pending→Abandoned` 窄 CAS 取消未开始
的 Boot 任务、放行已经开始的 Boot；`Engine` 门面用独立短锁在 nativeRun 之前发布稳定
`shared_ptr<ServerEngine>` 并在锁外调用。设计事实与相位/锁边界见
[执行域文档第 13 节](../architecture/gnet-network-execution.md#13-engine-启动相位--boot-观测--并发停止收敛2026-10-16本地验证未审查未合并)。
新增 `net.engine_boot_observation` / `net.engine_stop_during_boot` /
`net.engine_stop_before_run_and_contention` / `net.engine_boot_pending_abandon` 四 case
（原实现上先红后绿）与原 57 case 精准回归。F4/F8/LC/namespace、ASan/Release/全量 CI 未销项，
本切片不代表 Engine 验收或可合并。

## 13. 跨代 OnBoot 重入栅栏 + Pending 窗口确定化（2026-10-16，本地验证，未审查/未合并）

按 engine-start-control-review-52 的处置：(1) R52-1 裁定落地——超时回落时旧代 Boot 已开始
未完成，`ServerEngine` 保留其窄 `BootResult` 句柄作跨代栅栏，下一次 Run 拒绝
`device_or_resource_busy`，同一 handler 不跨代重入 `OnBoot`；完成后清柄可重试；`Stop` 终态
不等待 Handler、不保活网络 context。(2) `net.engine_boot_pending_abandon` 的 51 版占用手段
无效根因已证实：协程内 `std::this_thread::sleep_for` 经 libc nanosleep 被 runtime Hook 拦截
（`Hook_Nanosleep` → `YieldUntilTimeout`），挂起让出而非占住 Processer；本包改用绕过 Hook 的
原生 nanosleep（测试内 `RawNanosleep`，先例 runtime #284）在既有单 Processer 配置
（`m_cfg_static_thread_num=1`，51 时已设）下真实占住唯一执行线程，started 握手后 Boot 必然
排队，Pending→Abandoned 窄 CAS 首次获得行为验证。新增
`net.engine_boot_crossgen_reentry_busy`（无栅栏实现上先红：重试返回 `timed_out` 且
`boot_calls` 增至 2；栅栏落地后绿）。F4/F8/LC/namespace、ASan/Release/全量 CI 未销项，
本切片不代表 Engine 验收或可合并。

## 14. F8 停止开始授权切片（本地验证，待独立审查）

在既有 P1 异常安全增量之上，仅增加私有 `SealService`、Engine 全桥前置封门及排队 Write
的 owner 执行复查。开始按同短锁不可撤销授权定义；所有桥封门后发布 `stopping_=true`
为 Engine 停止线性化点，已授权任务可自然结束，不跨 handler 持锁或等待。
Open/Close/Traffic 已有开始门、Internal 快照归还与已接纳物理清理保持；不增加全局 gate
或 runtime 接口，不改 F4 持续失败/析构处理。详见执行域文档 §14。

新增 `net.co_bridge_service_gate_{queued_write,pending_open,pending_traffic,engine_handoff}`：
真实 TCP、确定性 owner/Processer 握手与测试独立 linkwrap。仅加 SealService 的旧行为候选上
Write 与 Engine handoff 两项红，现有事件门两项原已绿；同 fixture 修复后四项绿，旧 68
精确名单加四项共 72 项定向回归 unique=72、bad=0、skip=0；受影响消费者重链与网络头
自给构建通过。handoff 门闸位于 owner 取出后、原闭包执行前，并非 deque 内部暂停。
该结果只支持本切片，不自验收整体 F8/F4 或 Engine；多 sub 全交错、Traffic Take 后挂起
的新增确定性验证、LeastConnections/namespace 与 ASan/Release/全量 CI 继续开放。
持续 Notify 失败/析构能力按 §15 用户决策不在支持域，不作为本轮新增兜底实施项。未 commit/push。


## 15. Engine 一次性控制/关闭边界故障收尾切片（2026-10-16，本地验证，未审查/未合并）

在 F8 停止开始授权切片之上交付强停收尾的普通一次性故障增量（engine-stop-fix-68）：
SubmitCommand 分配异常分类、CloseListenerOwner 首错、Stop 每环有界推进一次、内部
readiness 收口判据（physical_closed+physical_waiters 结算，非 Drained）、未收口不发布
Stopped/Idle、析构去 shared_from_this。新增 `tests/net/engine_stop_failure.cc`
（目标 `Test_net_engine_stop_failure`，4 case：stop_close_before_fault /
stop_close_after_fault / stop_submit_alloc_fault / rollback_close_faults_not_idle，
真实 TCP 两连接 + 测试私有 link-wrap/operator new，无生产注入接口）；旧候选红证据与
绿证据、72+4 定向回归见 engine-stop-fix-68/{report.md,report.json,logs/}。
故障域按本节下述用户决策收窄；持续故障能力不在支持域，不再作为本轮新增兜底的实施项。
F4 整体、LeastConnections/namespace、ASan/Release/全量 CI 仍未验收。


### 单次入队内存错误分类（71 F3 修正）

EventLoop 私有 Submit 保留原状态集合；新增带 `std::error_code&` 的私有重载，
仅把本次 push/post 失败分类传给 Engine 控制命令：`bad_alloc` 为
`not_enough_memory`，其它异常为 `io_error`。旧单参数消费者仍按原
`Accepted/Full/Sealed/Failed` 语义工作，不读取共享 LastFailure 判本次错误。
分类使用值类型，不新增分配、不重试已接纳命令、不改变强停模型。
初次 post 分配失败时仍持队列锁回滚本次命令；owner 无法在回滚前取走它。
续投 post 失败仍在 owner 连续 drain，已经 Accepted 的责任不变。

独立测试 `net.engine_stop_post_alloc_fault` 在真实 Asio post 的 aligned_alloc
分配点单次返回空，先验证未接纳命令执行零次、恢复命令执行恰一次，再经真实
TCP Engine Stop 验证首错 `not_enough_memory`、对端关闭、线程/FD 回落及幂等。
该证据不覆盖持续失败策略、Accepted 迟到命令或最后引用析构交叉故障，
不表示整体 F4 已接受。

### 强停与最后引用析构的故障域（用户已接受，非整体验收）

本轮仅承诺：runtime 健康，且故障在既有有界补偿内及时恢复时，强停与最后引用析构可完成。单次目标 OOM 的修复及该恢复域已测，T1/T3 精准 79 项已独立通过；这不是当前整体 Engine、全部 F4 或 PR/CI 的验收。上游 OOM 修复仍以 HEAD 加未提交差异 hash 标识候选，不是已锁定的新 commit。

持续资源耗尽不在支持域：不保证强停/析构完成，不保证等待者被唤醒，也不承诺持续故障下的安全析构能力。这不是允许新增 UAF 或泄漏策略；不新增泄漏兜底、强杀、无限重试或 reaper，也不扩建 runtime 停机模型。`Stop`/`Join` 没有整体 deadline；控制命令的 15s 等待超时不取消已 `Accepted` 的命令，不能据此推定物理关闭或线程回收已经完成。强停仍不等 Handler/`OnClose`，不是业务优雅 drain。

原 C1“异常导致 physical waiter 记录残留进而悬空”的具体推测撤销，并非实测漏洞：实际 `CoIoOperation::Complete` → `CoWaiter::Notify` → `CoPollEvent::Trigger` 路径中，timer 撤销的 `DeferDestroyEvent` 在同锁内对首次 `bad_alloc` 有界重做一次；再次失败沿 `_CannelAllFdEvent` → `_Complete` 上抛，被 `Trigger` 的 catch-all 吞掉并返回 0。因而 `Complete` 不会沿这条链向 `OnClosedOwner` 抛出异常，原先据此推断的 swap-back 记录残留前提不成立；这项判断来自实际调用链与异常处理，不是由 `noexcept` 标签推导。持续 OOM 仍可能使带有限 deadline/timer 的等待丢失唤醒，即记录已清空但 wake 未送达；该真实限制保留，不能以撤销原推测冒充持续故障安全。


</details>
