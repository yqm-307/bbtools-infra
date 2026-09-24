# 协程 I/O 与第三方适配契约 v1：CoTCP、CoUDP 与协议 Binding

## 1. 状态与范围

契约 ID：`co-io-adapter/v1`。

日期：2026-09-22。状态：已确认方向的规格草案；独立只读终审结论为 `PASS`，非阻断问题已按审查建议修订。不是已实现 API，也不表示兼容验收通过。

本文定义 `bbtools-infra` 如何把成熟的第三方协议库绑定到 bbtco 的运行时，重点冻结：

- `CoTCP` 与 `CoUDP` 的传输对象职责；
- proc、sche 与第三方协议库的执行边界；
- 非阻塞立即操作与协程等待操作的语义；
- 连接/套接字所有权、并发使用和关闭规则；
- hiredis、KCP 等不同库形态的 Binding 接入方式；
- 不能直接绑定时的降级和 worker bridge 边界。

本文不实现 TCP/UDP 库，不定义 Redis、KCP、HTTP 或 Mongo 的业务 API，不创建通用 `IMiddlewareCli`、万能协议插件平台或动态注册表。

现有通信层总契约 `docs/decisions/0002-co-network-contract-v1.md` 继续拥有网络 Runtime、公共错误和关闭语义；本文补充其中缺失的 transport/adapter 执行契约。两者冲突时，以本文对 I/O 执行边界的较新明确规定为准，并在实施 Issue 中记录迁移。

## 2. 设计结论

### 2.1 统一的是 I/O 能力，不是中间件客户端

第三方库已经拥有协议状态机、编码、解码、重传或请求关联逻辑。infra 不重新实现这些能力，只提供与 bbtco 绑定的传输和等待能力。

统一层固定为：

```text
第三方协议库
    ↓ 私有 Binding
CoTCP / CoUDP
    ↓
bbtco 等待、唤醒、取消与 Scheduler
```

不统一为：

```text
IMiddlewareCli / IProtocol / IConnection
```

Redis、KCP、HTTP、Mongo 的客户端对象拥有不同的连接复用、消息关联、线程模型和关闭语义。可接管传输的 adapter 应通过组合使用共同的 `CoTCP`/`CoUDP` 能力；worker bridge 保留 driver 自有传输，而不是继承一个无法准确表达这些差异的万能接口。

### 2.2 命名

- `CoTCP`：面向 TCP 的协程传输能力，提供字节流读写。首版不将 Unix domain socket 冒充 TCP；确有需求时另行扩展。
- `CoUDP`：面向 UDP datagram 的协程传输能力，保留报文边界和 peer 地址。
- `CoSocket` 不作为首版公共传输类型：它同时掩盖 stream/datagram 语义，容易让调用者误以为读写、部分发送、EOF 和地址边界相同。
- `Cli` 表示协议或业务操作对象，不表示一个 FD。
- `Conn` 或 `Session` 表示协议连接/会话状态；它可以持有一个 `CoTCP`，也可以由一个 `CoUDP` 驱动多个逻辑会话。

典型关系：

```text
RedisCli
  └─ RedisConn
       └─ HiredisBinding
            └─ CoTCP
                 └─ socket

KcpEndpoint
  ├─ KcpOwnerCoroutine
  ├─ KcpSession(s)
  └─ CoUDP
       └─ one shared UDP socket
```

“一条协议连接对应一个 socket”是 TCP/Redis 等连接型协议的常见实现形态，不是所有中间件或协议的总契约。KCP 的多个逻辑会话可以共享一个 UDP socket。

## 3. 执行模型

### 3.1 三方职责

| 部件 | 允许执行 | 禁止执行 |
|---|---|---|
| proc / 当前 coroutine | 协议调用、编码、解码、状态机、`CoTCP`/`CoUDP` 的实际 syscall、错误翻译 | 将同一不安全协议对象交给多个并发 coroutine；无界忙循环 |
| sche / poller | FD/timer/wakeup 就绪检测、等待事件发布、协程唤醒 | 调用第三方协议函数、解析协议、执行用户 handler、代替 proc 做收发 |
| 第三方 Binding | 翻译第三方库的回调/返回值/生命周期，连接协议库与 `CoTCP`/`CoUDP` | 创建隐含线程或 event loop；绕过 transport 直接操作同一 FD |

协程挂起只释放执行 worker，不释放协议对象或传输资源的使用权。恢复后可以落在其他 worker，但仍由原操作主体继续执行；adapter 不得假定固定 worker。

### 3.2 立即操作与协程操作分离

`CoTCP`/`CoUDP` 同时提供两类能力：

1. `Try*`：只尝试一次，绝不挂起、不等待、不阻塞 OS 线程；不能立即完成时返回 `WouldBlock`。
2. 协程方法：在 `WouldBlock` 时注册 FD/timer/cancel/close 等等待条件，挂起当前 coroutine；sche 只负责就绪检测和唤醒，恢复后由 proc 重试 syscall。

就绪事件只表示“可以重试”，不表示本次操作已经成功。任何协程方法都必须使用同一绝对 deadline，重试不能重置超时。

`Try*` 路径不得再次调用会挂起当前 coroutine 的透明 Hook，避免显式等待层和 Hook 形成双重等待。实现必须选择一个等待责任方。

### 3.3 bbtco 运行时的适配边界

transport 只依赖 bbtco 提供的稳定等待/唤醒能力，不向 coroutine 仓库引入 infra 或第三方依赖。首版不要求 coroutine 暴露 Redis/KCP 专用 API。

在 Linux 上可以使用 Hook 把同步 syscall 的 `EAGAIN` 转成 coroutine wait，但“存在 read/write Hook”不等于任意第三方库都可安全挂起。每个 Binding 仍必须验证库自身的锁、线程局部状态、回调重入、超时和销毁语义。

## 4. CoTCP 公共能力

以下为目标声明，不是现有可编译 API。复用现有 `CallOptions`、`result<T>`、`Error`、`ICoNetwork` 与 `ICoCloseable`，不另建一套 deadline/cancel/error，也不新增 `IoOptions`——等待条件统一走 `CallOptions`（deadline/cancel）与内部 `IoWait`（§5.2），`IoProgress` 只携带 `Ok`/`WouldBlock`/`Eof`，其余结果一律走 `Error` 分支。新增缓冲和地址值类型在下方定义；公共头不暴露 Asio 或第三方类型。`CoTCP`、`CoUDP`、`CoTCPListener` 均不可复制，由 Runtime 工厂返回受托管引用。raw FD 接管（§6.1）保留为内部能力且遵循单一所有权，不作为公共 FD API 导出。

```cpp
namespace bbt::infra {

struct MutableBytes { void* data; std::size_t size; };
struct ConstBytes { const void* data; std::size_t size; };
struct SocketAddress {
    std::string ip;              // 数值 IPv4 / IPv6，不接受主机名
    std::uint16_t port;
};
struct TcpEndpoint { std::string host; std::uint16_t port; };

enum class IoState { Ok, WouldBlock, Eof };
struct IoProgress { IoState state; std::size_t bytes; };
// TimedOut/Cancelled/Closed 等通过 result 的 Error 分支表示。
// 部分失败的累计字节写入 Error.transferred_bytes。
using IoResult = result<IoProgress>;

} // namespace bbt::infra
```

`CoTCP`/`CoUDP` 复用 `0002` 既有 `Error`（`ErrorCode` + `domain` + `details`），不新建错误体系。除 `0002` 既有映射外，transport 层明确以下映射：

| 条件 | 结果 |
|---|---|
| 参数非法（`data=nullptr && size>0`）/ 非受管协程执行数据操作 | 分别返回 `InvalidArgument` / `InvalidContext`，立即返回，不挂起 |
| Runtime 未 `Start`、运行时代际不匹配、executor 获取失败 | `RuntimeUnavailable`，立即返回，不挂起 |
| 非阻塞 syscall 立即失败（`EAGAIN`/`EWOULDBLOCK`/`EINTR` 之外） | 对应 `Error`（如 `TransportError`），不挂起 |
| DNS 解析失败 | `TransportError`，`backend_category="dns"`；解析等待使用本次调用的同一绝对 deadline |
| 对端有序关闭（TCP read 返回 0） | `IoState::Eof` |
| deadline 到期 | `TimedOut`（Error 分支） |
| cancel 触发 | `Cancelled`（Error 分支） |
| owner 已 `RequestClose` 后的新调用 / 等待中关闭 | `Closed`（Error 分支） |

调用入口先检查参数、受管协程上下文和 Runtime 代际；然后按 Closed → Cancelled → TimedOut 检查已经成立的终止条件，再尝试 I/O。`Try*` 没有调用选项，只检查参数、上下文、代际和关闭状态。调用开始后的竞争沿用 `0002`：首次成功发布的逻辑终态不可覆盖；就绪本身不是终态。`Try*` 不挂起，因此不存在被透明 Hook 再挂起的路径：任何 `Try*` 实现内不得调用可能挂起当前 coroutine 的 Hook 或等待原语（见 §3.2）。

EOF 与部分传输：

- TCP 的零长度读写在入口检查后立即返回 `Ok/0`，不调用 syscall、不把空读误判 EOF。非空 TCP 读的成功进展必须大于零。
- `Try*` 遇 `EINTR` 返回 `TransportError` 并保留 `backend_code=EINTR`；可等待方法识别后检查关闭/取消/期限再重试，持续中断时有界让出 proc，不能无界旋转。
- TCP read 侧 `Eof` 携带 `bytes=0`；`WriteSome`/`WriteAll` 遇 `EPIPE` 等进入 Error 分支，`transferred_bytes` 保留累计写入，不把部分写入伪装成未发送。
- UDP 无 EOF：read 返回 0 字节是合法零长度 datagram（`Ok`），`WouldBlock` 才表示无数据。
- UDP 截断：报文长度超过 `dst.size` 即 `truncated=true`（超出部分已被内核丢弃）；`dst.size=0` 时同样成立。空 buffer 读到零长度 datagram 返回 `Ok`/`bytes=0`/`truncated=false`。截断报文不可喂给 KCP，由调用方丢弃。
- TCP 是字节流：`ReadSome`/`WriteSome` 返回小于请求量的字节数不是错误。

```cpp
namespace bbt::infra {
class CoUDP;

class CoTCP : public ICoNetwork, public ICoCloseable {
public:
    IoResult TryReadSome(MutableBytes dst);
    IoResult TryWriteSome(ConstBytes src);
    IoResult ReadSome(MutableBytes dst, const CallOptions& options);
    IoResult WriteSome(ConstBytes src, const CallOptions& options);
    IoResult WriteAll(ConstBytes src, const CallOptions& options);
    // RequestClose / IsClosed / WaitClosed 按 ICoCloseable 实现。
};

class CoTCPListener : public ICoNetwork, public ICoCloseable {
public:
    result<std::shared_ptr<CoTCP>> Accept(const CallOptions& options);
    SocketAddress LocalAddress() const;
    // RequestClose 停止接纳；不关闭已经交付的 CoTCP。
};

// 以下为 NetworkRuntime 的新增成员：
// DialTCP 只能在受管 coroutine 内调用，可以挂起。
result<std::shared_ptr<CoTCP>> DialTCP(
    TcpEndpoint endpoint, const CallOptions& options);
// ListenTCP/BindUDP 只接受数值地址，可在控制线程配置，不等网络事件。
result<std::shared_ptr<CoTCPListener>> ListenTCP(
    SocketAddress local, unsigned backlog);
result<std::shared_ptr<CoUDP>> BindUDP(SocketAddress local);

} // namespace bbt::infra
```

### 4.0.1 工厂与监听生命周期

以下工厂签名及语义是目标表面；实现通过公开头检查后才形成可消费 API。保持现有工厂兼容，新网络传输能力按独立构建目标开放。

工厂与生命周期规则：

1. **工厂执行位置**：`DialTCP` 只能在受管 coroutine 内调用，内部可因 DNS、非阻塞 connect 等待而挂起；首版不包含 TLS；`ListenTCP`/`BindUDP` 是控制线程配置操作，不挂起、不等网络事件，可在任何线程调用，但要求 Runtime 已 `Start`。
2. **所有权**：工厂返回的 `shared_ptr` 是受托管对象引用；Runtime 强持有全部交付对象直至其 Closed，调用方持有引用不能阻止 Runtime 清理。对象关闭后引用仍可用于查询 `IsClosed`/`WaitClosed`。
3. **DialTCP 内部步骤**：数值地址（`host` 是合法数值 IP）直接进入非阻塞 connect；主机名先经 transport 内 DNS（或显式复用既有 resolver 能力）解析——DNS 属于 transport 接管的阻塞路径，不允许落到未接管的同步 `getaddrinfo`。非阻塞 connect 以 `EINPROGRESS` 等待 writability，用 `SO_ERROR` 取回真实结果；`SO_ERROR` 非零映射为 `TransportError`（原生错误写入 `backend_category`/`backend_code`，不拼出含凭据的消息）。首版不承诺 TLS；`TcpEndpoint` 若需要 TLS 语义由后续切片扩展，不在本文冒充已支持。
4. **ListenTCP**：`bind` + `listen(backlog)`；绑定只接受 `SocketAddress` 数值地址（`ip` 不得为空，通配必须显式写 `0.0.0.0` 或 `::`；端口 0 表示动态分配）；`LocalAddress()` 返回实际绑定地址。`Accept` 挂起等待可读，Linux 使用 `accept4` 或等价非阻塞设置交付新 `CoTCP`，其他平台不得把此 syscall 名当公共契约；listener `RequestClose` 唤醒等待中的 `Accept` 并返回 `Closed`，不关闭已交付的连接。
5. **BindUDP**：`socket` + `bind`，同样只接受数值地址；`LocalAddress()` 返回实际绑定地址；未 bind 显式地址的组合发送路径不在首版公共 API。
6. **失败即无对象**：工厂失败返回 Error 且不产出半成品对象；`ListenTCP`/`BindUDP` 失败时内部负责 `close` 已创建的 FD。
7. **资源限额**：首版复用 `NetworkLimits.max_connections` 作为每个 Runtime 同时拥有的 transport socket 容量（含 listener、UDP socket、accepted TCP、连接中的候选 socket；协议 Conn 引用同一 socket 不重复计数）。创建/接纳前原子预留容量，失败回退、物理关闭后释放；超限返回 `Overloaded`，不静默排队。`backlog` 必须为 1..INT_MAX，非法返回 `InvalidArgument`，内核实际队列上限可能更小。目标契约另要求 `max_inflight` 限制等待中的拨号、Accept 和数据操作，`Try*` 不建排队任务；**CoTCP/CoUDP 首版暂缓此项传输在途门禁**，配置校验及 HTTP handler 已使用 `max_inflight` 不代表传输层已限流。由 [#32](https://github.com/yqm-307/bbtools-infra/issues/32) 跟进：在 Runtime 范围内实现跨挂起占用、所有终态/强制 Stop 安全释放与超限回归后，才移除此例外并关闭 Issue。

### 4.1 CoTCP 语义

- `TryReadSome`/`TryWriteSome` 只完成一次立即尝试；可以返回部分字节。
- `ReadSome`/`WriteSome` 在可恢复等待时挂起调用 coroutine；唤醒后重新尝试。
- `WriteAll` 直到全部写出或返回明确错误；发生错误时在 `Error.transferred_bytes` 保留累计写入数，不得把部分写入伪装成未发送。
- EOF 是对端有序关闭的结果，不等同于 `WouldBlock`。
- FD 必须是非阻塞的；禁止因为外部传入 blocking FD 而卡住 proc。
- 读写、关闭和最终释放必须遵守同一个对象的所有权规则。
- `CoTCP` 首版不承诺可被多个 coroutine 并发调用。默认契约是：一个协议操作主体独占一个连接；需要并发时使用连接池，而不是让多个 coroutine 直接共享一个协议 context。

缓冲是非拥有视图：非零长度要求有效地址，调用期间禁止并发修改。异步就绪回调不得持有调用栈/缓冲指针，只通知栈外操作状态；真正复制只能在 syscall 的同步执行期间。Binding 跨等待所需的拥有型缓冲与租约放在 Runtime 可枚举的操作状态中，不能只放在会被直接销毁的协程栈。

“独占”至少覆盖一轮会改变协议状态的完整交互，而不是只覆盖一次 `read` 或 `write`。例如 Redis 请求必须避免 A 写入请求、B 读取 A 的响应。事务、pipeline、认证和多步握手可扩大独占范围，由具体协议 Binding 定义。

## 5. CoUDP 公共能力

```cpp
namespace bbt::infra {

struct DatagramRead {
    IoState state;              // 仅 Ok / WouldBlock，UDP 不返回 Eof
    std::size_t bytes;          // 实际复制长度
    SocketAddress peer;        // 仅 Ok 时有效
    bool truncated;            // Ok 时检查；截断报文不可喂给 KCP
};

class CoUDP : public ICoNetwork, public ICoCloseable {
public:
    result<DatagramRead> TryReceive(MutableBytes dst);
    IoResult TrySend(ConstBytes packet, const SocketAddress& peer);
    result<DatagramRead> Receive(MutableBytes dst, const CallOptions& options);
    IoResult Send(ConstBytes packet, const SocketAddress& peer,
                  const CallOptions& options);
    SocketAddress LocalAddress() const;
    // RequestClose / IsClosed / WaitClosed 按 ICoCloseable 实现。
};

} // namespace bbt::infra
```

### 5.1 CoUDP 语义

- 一个 `Send` 对应一个完整 datagram；不能把部分发送当作下一次 packet 的前缀。
- 零长度 datagram 不是 EOF。
- `Receive` 返回实际 peer 和截断状态；不能静默丢失报文边界。
- `TryReceive`/`TrySend` 不等待；`Receive`/`Send` 可以挂起当前 coroutine。
- 一个 UDP socket 可以承载多个逻辑会话；此时必须由一个明确的 owner coroutine 或等价串行执行域推进该 socket 和协议状态。
- 组合等待通过内部 `IoWait` 完成，见 §5.2；不要求调用者用两个顺序等待拼出读/写/timer/command 的任一唤醒。

`CoUDP` 的 `Send` 成功只表示报文已交给本地内核发送路径，不表示对端收到或业务处理完成。

### 5.2 IoWait 组合等待契约

`IoWait` 是 transport 内部的组合等待原语，不是公共 API。一个协程方法（`ReadSome`/`WriteSome`/`Receive`/`Send`/`Accept` 等）在 syscall 返回 `WouldBlock` 后，向 `IoWait` 注册本轮所需的全部等待条件并一次性挂起，而不是用多个顺序等待拼装：

| 等待条件 | 触发者 | 语义 |
|---|---|---|
| FD readable / writable | sche poller | 只表示“可以重试”，不表示操作成功 |
| timer / deadline | runtime 时钟 | 到期即参与唤醒竞争，不重置 |
| cancel | 调用方 token | 立即参与唤醒竞争 |
| close（owner `RequestClose`） | 任意线程 | 只封口并唤醒，见 §6.4 |
| command / 自定义事件 | Binding / owner | 供 KCP output queue 等内部状态投递唤醒 |

契约要求：

1. 每轮 `IoWait` 先确定一个 transport 及固定的 readable/writable 掩码、最早 deadline、命令通知、关闭与取消条件，完成登记后 park；任一条件触发即唤醒并报告原因。不要求收集所有同时发生的原因，也不允许挂起后动态修改注册集合。只在待发队列非空时监听 writable，避免空转。
2. **Wakeup 不丢失**：从“调用者检查队列/状态”到“完成 park”之间到达的唤醒必须被 `IoWait` 捕获。实现形态是先登记等待者再复查状态、或等价的内存序保证；不依赖“先 park 后事件必然晚到”的假设。具体内存序方案是实施验证项，不是已验证的上游能力。
3. deadline 与 cancel 同时到期的胜负由既有 0002 规则裁决（首次成功发布的逻辑终态不可覆盖）；入口已成立的条件按 §4 检查；挂起后的竞争不规定固定先后。
4. `IoWait` 不调用第三方协议函数、不解析协议、不代替 proc 重试 syscall；唤醒后由恢复该 coroutine 的 proc 决定重试还是退出，不保证原 worker。
5. `Try*` 路径不经过 `IoWait`。命令队列非空、协议定时已到或还有本轮预算截断的可执行工作时，owner 先有界处理/主动让出后继续，不进入可能无限睡眠的等待。
6. 内部等待输入必须区分两种绝对时间点：业务 `CallOptions.deadline` 与可选的协议推进 `hint_deadline`。内部目标形态为 `IoWait::Wait(transport_state, read_interest, write_interest, wake_sequence, CallOptions, optional<hint_deadline>)`；transport_state 为受托管资源身份而非裸 FD。只有带命令/自定义通知的 owner 才提供 wake_sequence（owner 检查队列前读取的单调通知序号）；普通 transport 等待不提供该字段。登记后序号变化或队列非空则不 park。`hint_deadline` 到期返回 timer 提示，由 owner 推进协议；`CallOptions.deadline` 到期返回 `TimedOut`，二者同时成立时先按首次成功发布的内部唤醒原因落定，但业务方法在返回前仍须检查其逻辑终态。其他返回为 readable/writable/command 提示或 Closed/Cancelled/RuntimeUnavailable。具体类型留私有头，但上述输入/返回语义必须有测试。
7. **上游等待能力**：截至本文基线，coroutine 公共层只稳定承诺 FD、Timer、Wakeup 与单次等待生命周期，尚未导出本文完整的多源 `IoWait`。首个 transport 实现必须先完成 §11 步骤1的组合等待探针；如需上游新增能力，仍由 coroutine 拥有等待状态机，infra 只消费，不在 adapter 内复制另一套。

## 6. FD 与资源所有权

### 6.1 首选：transport 拥有 FD

首选形态是 `CoTCP`/`CoUDP` 拥有底层 socket，第三方 Binding 只获得受控的借用句柄或回调能力。关闭由 transport 的唯一 owner 线性化，第三方 cleanup 不得再次直接 `close`。

接管已有 FD 时必须明确以下信息：

- 是否转移所有权；
- 是否强制设置 `O_NONBLOCK`；
- 谁负责 close；
- 谁负责错误后的释放；
- FD 被关闭后晚到事件如何失效；
- 是否允许通过 `dup` 或 raw handle 绕过 owner。

### 6.2 第三方强制拥有 FD 的情况

若第三方 API 明确要求由 driver 关闭 FD，Binding 可以采用 driver-owned 模式，但必须满足。此模式下该 FD 自创建或接管起就归 driver，不归 `CoTCP`/`CoUDP`；§3.1 的“不得绕过 transport”针对 transport-owned FD，driver 在已验证的自有执行域操作自有 FD 不构成绕过：

1. `CoTCP` 不再声明拥有该 FD；
2. driver close 是唯一实际 close；
3. 关闭由 owner 协调：先使等待失效、确认无 syscall/协议调用正在使用资源、解除监视，再由 driver 唯一关闭并释放 context；
4. FD 数字复用期间旧事件不能触达新资源；
5. `RequestClose`/`WaitClosed` 等待 driver callback 和 cleanup 完成。

不能同时让 `CoTCP` 和 driver 各自认为自己拥有 FD。`dup` 创建不同的描述符数字，但与原 FD 共享 open file description（包括 O_NONBLOCK 等状态），不能被原 FD 的串行契约自动覆盖；adapter 必须禁止或显式纳入所有权模型。

### 6.2.1 单一 owner：infra 使用契约，不是 sche 互斥保证

本文所有“单一 owner / 串行使用”约束是**对 infra adapter 与第三方 Binding 的使用契约**：谁持有对象，谁负责保证同一时刻只有一个执行主体使用它。当前 bbtco 上游（Scheduler/Poller）不提供也不承诺“FD 全局互斥”或“sche 自动串行化协议调用”；不得把单 owner 约束描述成 sche 已实现的能力。需要串行化时由 infra 自行提供（如 Binding 内串行队列、per-connection 状态机）。关闭协议不是正常读写的独占锁；首版不提供跨调用的自动锁，协议 owner/池租约负责完整交互独占。

### 6.3 停止与栈外保活

bbtco `Scheduler::Stop()` 对仍挂起协程采用直接销毁、不做栈展开。因而不能依赖协程栈上的 RAII 释放第三方 context、FD、缓冲或 lease。

Runtime 必须在栈外持有 operation、Binding、连接和 transport，直至 `WaitClosed` 确认后端不会再访问它们。逻辑 `Cancelled`/`TimedOut` 不等于物理清理完成。

栈外保活的边界：`shared_ptr` 只保活对象本体；硬 Stop 时第三方栈上（库调用帧内）的临时分配无法被任何栈外 `shared_ptr` 自动回收。adapter 不得宣称“对象保活 = 第三方栈安全”；可行的处理是把等待/阻塞深度控制在栈外可枚举的 operation 上（B/C 级），或证明 A 级库调用在任意挂起点栈内无第三方临时分配依赖（见 §10.1）。

### 6.4 关闭协议与 FD 复用防护

`RequestClose` 的分层语义：

1. **封口与通知（任意线程）**：`RequestClose` 幂等、可在任意线程调用；它置关闭标志、使在途等待失效（唤醒等待者返回 `Closed`）、拒绝新调用，不等待物理完成。
2. **解除监视**：owner 或关闭协调者把 FD 从 poller 解除注册，晚到就绪事件不再发布给该对象。
3. **同步退出**：物理 `close` 必须与正在进行的 syscall 和协议库调用同步——确认没有线程/协程正持有该 FD 做 read/write/recvfrom/send 或处于第三方协议调用内部。实现方式由 Binding 决定（等待 in-flight 计数归零、owner 串行化关闭步骤、或 worker bridge 的物理归零），但“封口后立即 close”不满足本契约。
4. **物理 close 与释放**：唯一 owner 执行 `close` 和 context 释放。`WaitClosed` 返回后保证后端不会再访问资源。
5. **FD 代际防护**：close 之后 FD 数字可能被其他组件复用。解除监视必须先于物理 close 完成；poller 侧对每个注册事件持有代际/身份标识，旧代际事件（含已入队未派发的就绪通知）不得触达新资源。该防护的具体机制是 transport 实现责任，不得依赖“解除与 close 之间不会有事件”的时序假设。

反例（明确禁止）：Binding 在收到 close 通知后，一边让第三方协议调用（如 `redisCommandArgv`）仍悬停在库栈内部，一边立刻 `close(fd)`——协议库内部可能正持有该 fd 的缓冲、TLS 状态或重试逻辑，且晚到事件可能触达复用后的 FD。

## 7. 第三方 Binding 契约

Binding 是 infra 的私有适配实现，不进入公共协议客户端头。一个 Binding 至少要定义：

```text
第三方对象创建与销毁
第三方回调 → CoTCP/CoUDP 的返回值翻译
WouldBlock / EOF / timeout / close 的映射
协议对象的单一执行主体
取消、关闭和晚到回调的生命周期
第三方线程、TLS、内部锁和重入限制
```

Binding 必须回答以下问题后才可声明支持：

- 协议函数是否可能在 callback 中持有内部锁？
- callback 是否总在 owner coroutine / proc 中执行？
- 是否有 DNS、TLS、压缩、文件或其他未被 transport 接管的同步阻塞路径？
- 第三方对象是否允许在 syscall 等待期间挂起？
- 取消或 Stop 后，第三方栈上的临时对象和锁如何释放？
- close、cleanup、FD 复用和晚到事件是否有唯一时序？
- 返回值为 `0`、负值、EOF 或特殊错误时，每个值的精确定义是什么？

Hook 数量、编译通过或单个 loopback 成功不能替代这份兼容矩阵。

### 7.1 直接 coroutine binding

只有当第三方协议函数允许在 transport callback 内挂起，且已验证锁、TLS、线程局部状态和强制停止边界时，才允许采用：

```text
当前 coroutine
  → 第三方同步协议函数
      → Binding read/write callback
          → CoTCP/CoUDP 协程方法
              → WouldBlock 时挂起
  → 恢复后返回第三方协议函数
  → 协议函数继续解析/推进
```

这种方案的“同步”只表示第三方控制流同步，不表示 OS socket blocking。

### 7.2 非阻塞分步 binding

若第三方库不能安全采用 §7.1，或它天然要求主动驱动，则采用：

```text
owner coroutine
  → 调用一次第三方推进函数
  → 得到需要读/写/定时器/自定义事件
  → 等待 CoTCP/CoUDP 或 timer
  → 恢复后再次调用推进函数
```

sche 只报告就绪，不调用推进函数。

### 7.3 worker bridge

如果第三方函数不能安全挂起，或内部拥有不可迁移线程状态，则不能用 Hook 伪装成协程原生。使用固定上限 worker bridge：

- worker 内完成 acquire/use/release；
- 协程只等待 operation state；
- 队列和 worker 数有界；
- deadline/cancel 可以先发布逻辑结果，但不能谎称已强杀 driver 调用；
- `WaitClosed` 等待 driver 和队列物理归零。

当前 Mongo adapter 使用此路径；这是保守兼容例外，不是已经证明 mongocxx 永远不能协程化。是否迁移由单独兼容调查裁决，不因存在 socket Hook 而移除 worker bridge。

## 8. hiredis Binding 目标形态

hiredis 提供 `redisContextFuncs` 的 `read`、`write`、`close` 等函数表，且同步 `redisContext` 本身不是线程安全对象。目标接入形态为：

```text
RedisCli
  → RedisConn（一个 hiredis context 的单一 owner）
      → HiredisBinding
          → CoTCP
```

`RedisConn::Execute` 的完整请求—响应由一个 coroutine 独占：

```text
redisAppendCommandArgv / redisCommandArgv
  → hiredis 格式化请求
  → Binding::Write
      → CoTCP::WriteSome
  → Binding::Read
      → CoTCP::ReadSome
  → hiredis reader 解析响应
  → infra 自有 Reply / Error
```
目标方案不是 sche 调用 hiredis 的 `async_read/async_write` 或推进 RESP，而是由 proc 中的 hiredis 调用自然进入 Binding，并在底层等待点挂起。

### 8.1 hiredis 函数表契约（固定 commit `616f2286ba5503f74ae96e720623fa11dbc690af`）

Binding 对 hiredis `redisContextFuncs` 各回调的目标行为（均为设计目标，需经最小实验验证，见下）：

| 回调 | 目标行为 |
|---|---|
| `read` | A 路径调用 `CoTCP::ReadSome` 并在 transport 内消化 `WouldBlock`，不返回 0 令同步 hiredis 忙循环。B 路径仅在库以非阻塞方式受控推进时调用 `TryReadSome`，此时 0 表示可恢复无进展；EOF 或终止态返回 `< 0` 并置 context error（`err`/`errstr`），让 hiredis 进入错误路径。不伪造“读到 0 字节等于 EOF”。 |
| `write` | 从 `c->obuf` 取数据，A 路径经 `CoTCP::WriteSome` 发出；单次 `WriteSome` 保持 hiredis 按回调返回值消费 `c->obuf` 的推进语义，不由 Binding 代替 hiredis 删除或重排缓冲。只报告本次实际写入的字节数；B 路径使用 `TryWriteSome`，`WouldBlock` 返回 0；A 路径在 transport 内等待，终止返回 `< 0` 并设 context error。累计已发送数量另存栈外 operation，错误时使连接不可复用，不自动重发。 |
| `close` | 由 Binding 唯一拥有：转发为 transport `RequestClose`，保证物理 close 走 §6.4 协议；hiredis 不再自己 `close(fd)`。 |
| `free_privctx` | 只释放该 hiredis context 独占的私有包装；仍被等待/关闭访问的状态由 Runtime 独立保活，不能在此提前释放。先让非清理协议调用退出再执行 redisFree，cleanup 完整退出后才发布 Closed。 |
| connect / TLS | connect 经 `DialTCP`（§4.0.1）；TLS 函数表项不可盲覆盖——未实现 TLS 接管时在连接前明确拒绝 TLS 请求，不降级为明文，也不盲目保留会绕过 CoTCP 的 TLS 收发。 |

附加约束：

- **禁止自动 reconnect 绕过 binding**：Binding 不得启用或伪造会自行重建 socket 的路径；断线必须映射为明确错误交回 `RedisConn`，由它决定新建连接（走 `DialTCP`）。
- **0 与负值的精确语义**：`read`/`write` 的每个返回值含义以上表为准，并在最小实验中用 hiredis 实际调用点（`redisBufferRead`、`redisBufferWrite`、`redisGetReply`）核对，不得凭直觉映射。
- 该目标形态是 A 级**候选**，未经 §10 验收前不得据此把 `0003` 的 async + strand 决策改写为“已迁移”或“已淘汰”；也不能反向把本目标描述回 sche async 形态。

实施时必须另行验证：

- 当前固定 hiredis 版本的函数表是否允许完整替换 transport；
- callback 内挂起时 hiredis 是否持有不可安全跨挂起的锁或临时状态；
- `close`/`free_privctx` 与 transport close 的唯一所有权；
- connect、DNS、TLS、command timeout 是否仍有未接管路径；
- 直接 binding 与当前 async adapter 的兼容/性能/关闭差异。

在这些证据完成前，`0003` 中的 hiredis async + strand 仍是当前实现决策，不得把本文目标形态写成已交付。

### 8.2 与 0003 的关系

`0003-redis-client-hiredis-dependency.md` 是**当前实现**的决策（hiredis
async + strand，v1.4.1 / commit `616f2286ba5503f74ae96e720623fa11dbc690af`），
本文 §8/§8.1 是**候选目标形态**，二者并存；A 级实验通过并经 §11 排序中的迁移
决定前，旧实现的单执行域、连接单生命周期与 cleanup 时序仍须遵守；目标设计不再受“只能 async + strand”的选型条款限制。`scheduleTimer` 未接是旧实现缺口，不是新实现要保留的不变量。

## 9. KCP Binding 目标形态

KCP 提供 output callback，并要求调用方主动调用 `ikcp_input`、`ikcp_update`、`ikcp_check`、`ikcp_recv` 等函数。它不是 TCP 字节流 API，不能直接套 hiredis 的 read/write binding。

目标对象：

```text
KcpEndpoint
  ├─ 一个 KcpOwnerCoroutine
  ├─ 一个 CoUDP
  ├─ 一个有界 output queue
  └─ 多个 KcpSession / ikcpcb
```

KCP output callback 必须是立即返回路径：

```text
output callback
  → 已有待发报文：保序复制入有界队列
  → 队列为空：CoUDP::TrySend
  → 成功：返回
  → WouldBlock：复制完整 datagram 到有界 output queue，返回可识别状态
  → 错误/队列满：发布明确失败状态
```

output callback 不应直接调用可能挂起的 `CoUDP::Send`。KCP owner coroutine 负责：

1. 处理业务发送命令并调用 `ikcp_send`；
2. 读取 UDP datagram 并调用 `ikcp_input`；
3. 调用 `ikcp_update` 推进重传/ACK；
4. 调用 `ikcp_recv` 提取完整业务消息；
5. 排空有界 output queue；
6. 按 `ikcp_check` 等待下一次协议推进时间；
7. 等待 UDP readable、UDP writable、timer、业务命令或 close（经 §5.2 `IoWait` 组合等待）。

### 9.1 output queue 与失败锁存

- **即时 TrySend 优先**：仅当队列为空时，output callback 尝试 `CoUDP::TrySend`；`WouldBlock` 时入队，队列非空直接按保序规则入队。
- **保序**：一旦某次 output 入队，后续 output 必须全部经过同一队列直到排空——不允许“先入队的在等、后入队的绕过队列直接发”，否则适配器会引入额外本地重排（KCP 本身能够处理网络乱序）。队列空且 socket 可写后恢复即时路径。
- **有界容量**：队列同时设 packet 数上限与字节总量上限（具体数值由实现 Issue 冻结）；入队前复制完整 datagram（callback 返回后 KCP 缓冲不保证有效）。
- **满/错误锁存（side-channel）**：已核对 KCP 的 flush 调用点可能忽略 output callback 返回值，**不能假称“callback 返回 -1 会让 KCP 重试”**。队列满、复制失败或 `TrySend` 非 `WouldBlock` 错误时，callback 把失败锁存到 endpoint 级 side-channel 状态并立即返回；owner coroutine 在下一轮循环检查该状态，首版由 owner 关闭该 endpoint 的全部会话并向业务报告错误，避免未定义的局部恢复。callback 自身不终止会话、不抛异常。
- **公平性**：每轮 owner 循环对“读 UDP → input”、“排空 output queue”、“处理业务命令”、“检查 side-channel”各自设预算（packet 数/字节数/条数），保证持续入包时 timer（`ikcp_check`）和控制命令不被饿死；单轮超预算的部分留到下一轮。

### 9.2 共享 socket 与多会话

多个 `KcpSession`/`ikcpcb` 共享一个 `CoUDP`：所有 `ikcp_input`/`ikcp_update`/`ikcp_recv` 只在 owner coroutine 中执行；owner 按已绑定的 peer 地址与 conv 联合标识分发 datagram；conv 本身不是鉴权，未知 peer/conv 不自动创建会话。业务 API 层（`KcpSession` 的 send/recv）与 owner 循环之间用有界命令队列衔接，不把 ikcpcb 暴露给多协程。本节是 KCP 的目标说明，不代表已锁定依赖版本或存在可编译接口。

## 10. 兼容分级与验收

每个第三方 Binding 必须标注支持级别：

| 级别 | 含义 |
|---|---|
| A：直接 coroutine binding | 已证明协议调用可在 transport callback 中挂起，锁、TLS、线程局部状态、取消和销毁均有证据 |
| B：非阻塞 owner binding | 第三方提供可重复调用的推进接口，owner coroutine 驱动，等待由 CoTCP/CoUDP 提供 |
| C：worker bridge | 不能安全挂起，固定 worker 执行，协程只等待 operation state |
| D：不支持 | 关键生命周期或阻塞路径无法隔离，不能以“有 Hook”或“有 callback”宣称兼容 |

最小验收必须覆盖：

- 当前第三方版本、编译和实际链接来源；
- 单连接/单 owner 串行性；
- 多连接并发；
- `WouldBlock`、部分读写、EOF、错误和关闭；
- deadline、cancel、owner close、晚到 completion 的竞争；
- FD close 与数字复用保护；
- Stop 不展开栈时的栈外资源清理；
- DNS/TLS/第三方线程/内部定时器等未接管路径；
- 真实后端端到端交互。

通过依赖库自带测试、编译成功、单个 loopback 或观察到 read/write callback，均不足以单独提高兼容级别。

### 10.1 A 级强制 Stop 资源安全门禁

A 级（直接 coroutine binding）意味着第三方协议调用会在库栈内部挂起。由于 `Scheduler::Stop()` 直接销毁挂起协程、不展开栈，任何 A 级 Binding 必须证明：**在任意挂起点被硬 Stop 时，第三方库栈内的资源不会泄漏或悬空**。库栈内的临时分配（缓冲、锁、内部 lease）没有栈外 `shared_ptr` 能替它释放（§6.3）。

- 优先路径是 graceful drain：Stop 前由宿主/owner 排空在途调用、不再发起新调用，让库栈自然退出后再关闭。A 级文档必须给出 drain 顺序与超时行为。
- 若无法证明 forced stop 下资源安全，该 Binding 降级为 B（非阻塞步进：库调用退出后才等待/关闭）或 C（worker bridge），不得以“有 Hook”维持 A 级。
- 硬 Stop 后，Runtime 与相关对象仍由宿主持有；若资源未能安全回收，不得宣称该连接/对象已达 `Closed` 语义（物理清理完成）。遗留缺口转为对上游 runtime/库的验证任务，不虚构“Stop 回调”或“资源回收回调”等当前上游不存在的能力。

### 10.2 HTTP 目标形态

HTTP 遵循同一执行模型，单独说明因为它已按 `0002` N1 落地为 Asio/Beast 实现：

- **协议复用，执行位置推进**：HTTP 消息解析/framing 继续复用 Beast（不自写 HTTP 解析器，也不把解析搬进 sche）；Beast 的异步 completion 不得在 sche 线程直接运行业务 handler，目标协议推进严格在受管 coroutine/proc，而不是沿用 sche strand。
- **迁移目标**：现有 `HttpIoEngine`/strand 驱动逐步改为薄 `SyncReadStream` 适配层——在受管协程内以 `CoTCP::ReadSome`/`WriteSome` 喂给 Beast 的同步流接口，让 Beast 在 proc 内推进、等待由 CoTCP 完成；若同步栈不能满足 Stop 门禁，改用 Beast parser/serializer 的非阻塞分步接口，库函数退出后再等 CoTCP（B 级）。不能仅从 proc 发起 async 操作、却仍让 composed completion 在 sche 解析协议。两条路径的选择依据与迁移差异记录在实现 Issue，未验证前不改写 `0002` 附记中已实现的真实线程模型。
- **不变量**：`CallOptions`、`Error`、逻辑终态/物理清理分离、`StopAccepting` 语义沿用 `0002`；本节不新增 HTTP 语义。

### 10.3 Mongo 现状边界

Mongo 维持 `0004` 的 C 级 worker bridge 例外：这是当前实现的兼容路径，但**不宣称 mongocxx 天然不兼容、也不宣称永远不兼容**——是否可迁移到 A/B 级由独立兼容调查裁决，调查前既有 worker bridge 行为不因本规格改变。`amongoc` 等替代路径与既有 URI 超时注入的兼容性属于该调查的待核项。

## 11. 实施顺序

1. **公共规格与上游探针**：冻结 `CoTCP`/`CoUDP` 的值类型、错误和所有权契约（本文 §3–§7），不实现协议客户端；同时验证 `IoWait` 组合等待（§5.2）、单 owner 关闭协议（§6.4）等依赖的上游 runtime 能力——这些探针是后续 Binding 的前置门禁，不是已验证的既有 API。
2. **CoTCP 最小切片**：完成 CoTCP 的真实 FD、超时、取消、关闭、`DialTCP`/`ListenTCP` 生命周期和最小 contract test。
3. **Redis 兼容证明**：hiredis 按固定 commit 做最小函数表实验，验证 §8.1 各回调行为与 A 级 Stop 门禁（§10.1）；实验不直接修改现有 Redis adapter。通过后，比较目标 A 级路径与当前 async + strand 路径的兼容、性能和关闭差异，再决定迁移。
4. **HTTP 迁移与执行位置门禁**：在 CoTCP 就绪后启动 §10.2 的 Beast 执行位置迁移（SyncReadStream 适配或非阻塞步进），迁移 Issue 必须先声明执行位置与关闭门禁，保留 `0002` HTTP 对应的 N-01～N-04、N-06；RPC 的 N-05 不是 HTTP 迁移前置。
5. **CoUDP/KCP（并行可选）**：独立验证 KCP 的 B 级 owner coroutine 路径（§9.1/§9.2），覆盖 output queue、timer、side-channel 与 UDP 共享 socket；CoUDP/KCP 与 TCP 线并行推进，不阻塞 CoTCP/Redis/HTTP。
6. **Mongo 调查（并行）**：Mongo 保持 C 级 worker bridge；是否可迁移到 A/B 级由独立兼容调查（§10.3）并行推进，不阻塞其他模块。
7. **RPC profile 设计（并行）**：RPC wire profile/协议选型设计可并行开展；真实发布受其所消费的 HTTP 切片约束（`0002` N2 门禁不变）。
8. **收口**：最后更新 Cli/Conn/Runtime 的公共文档与 framework 消费契约；未通过真实验收的类型只标为目标，不导出为稳定能力。各模块按自身验收就绪独立交付，不强制同一批次。

本规格不授权实现、依赖安装、公共 API 发布或旧 adapter 迁移；后续必须取得实施授权并通过对应 Issue/PR 记录验收；Issue/PR 的创建本身不构成实施或发布授权。

## 12. 依据与当前基线

平台边界：首个实现验证面为 Linux。Windows/iOS 保留后续验证目标；不得把 POSIX FD、accept4 或 Linux Hook 测试冒充跨平台已通过。文件 I/O、TLS、Unix socket、自动连接池和透明任意第三方兼容不在首切片。

本规格编写时核对的本地仓库基线（不是远端 main 最新版本声明）：

- `bbtools-infra`: `6cb2aec9133cf78e32f9805a508b9002e93b4ba8`
- `bbtools-coroutine`: `44b77d5e5e03ca33081a4a798d06c67344a825db`

已核对的现有事实：

- `bbtools-coroutine/agent-docs/2026-09-07-core-runtime-contract.md`：EventLoop/Poller 负责等待与事件交付，Hook 是适配层，不以 Hook 数量代替兼容验证；Stop 不展开挂起协程栈。
- `bbtools-infra/docs/decisions/0002-co-network-contract-v1.md`：现有 Runtime、错误、关闭和在途资源契约。
- `bbtools-infra/docs/decisions/0003-redis-client-hiredis-dependency.md`：当前 hiredis async + strand 决策及连接生命周期约束。
- `bbtools-infra/docs/decisions/0004-mongo-client-mongocxx-dependency.md`：当前 Mongo synchronous driver + worker bridge 决策。
- hiredis 固定 commit `616f2286ba5503f74ae96e720623fa11dbc690af` 的 `redisContextFuncs`、`redisBufferRead`、`redisBufferWrite`、`redisGetReply` 和 `redisFree` 路径。
- KCP `ikcp.h` 的 `ikcp_setoutput`、`ikcp_input`、`ikcp_update`、`ikcp_check`、`ikcp_recv` 接口。

上述第三方接口依据用于设计边界，不代表本仓已经完成对应 Binding 的兼容性验收。

官方接口来源（用于设计核对，不是集成验收）：

- [hiredis 固定版本函数表](https://github.com/redis/hiredis/blob/616f2286ba5503f74ae96e720623fa11dbc690af/hiredis.h#L242-L254)
- [hiredis 同步缓冲与读取推进](https://github.com/redis/hiredis/blob/616f2286ba5503f74ae96e720623fa11dbc690af/hiredis.c#L983-L1096)
- [KCP 接口](https://github.com/skywind3000/kcp/blob/master/ikcp.h)与 [output 调用点](https://github.com/skywind3000/kcp/blob/master/ikcp.c)：本次读取 master，仅作接口调查；实施前必须固定 commit，不将浮动来源用作可复现验收基线。
