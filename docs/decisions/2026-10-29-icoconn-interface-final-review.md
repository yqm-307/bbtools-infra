# infra net 接口终评与收口方案

本终评建议采用：**`ICoConnection` 提供协程成员读写，`CoTcpConnection` 只包装既有 `Connection`；读交付 owning `Buffer`，非挂起提交叫 `AsyncWrite`，组合等待叫 `Write`；保留事实聚合 `Status` 与物理回收观察，删除 `IsConnected` 和连接级 `SetOnClose`。** `Engine` 增加 `CoAccept/CoDial` 成员，继续由宿主装配、`Run`、`RequestClose`。这是一份待接口拥有者接受的实施方案，不是已实现、已冻结或测试通过的声明。

范围是把数据交付、提交/等待、逻辑终态/物理释放和模块组合讲清并落到可验收入口；不改 Processer driver、owner 固定、ring 唯一真源、CoWaiter 首胜、等待不取消 I/O、Engine `RequestClose → Run` 收口及既有 OutcomeUnknown 分界。不迁移 framework，不新增通用 Future、runtime 接口或关闭基类。

证据基线为五路报告共同使用的 `9dd431349b601d1b65fe969d57b040f6bd6965a9`。五份报告已全部读完，另定点读了该 SHA 的只读源码包。活动仓库为 `537d35508cf050ee5e79888e0b761414dbfb9336`，不能拿它替代被审快照。以下 `L1`–`L5`、`R` 均为**对应报告的行号**，源码位置另列：

- L1：[并发与寿命](lane1-concurrency.md)，136 行。
- L2：[外部库对比](lane2-library-comparison.md)，101 行；外库结论作为比较材料，本终评没有重新查验三库源码。
- L3：[公共 API 表面积](lane3-api-surface.md)，228 行。
- L4：[CMake 与目标图](lane4-cmake.md)，144 行。
- L5：[错误与关闭语义](lane5-error-close.md)，136 行。
- R：[先前独立审查](reviewer-report.md)，121 行；F1/F2/F3 已确认，不能因本轮改名而核销。

## 1) 裁决表

| 争议建议 | 裁决 | 最终取舍与一句话理由 | 依据 |
|---|---|---|---|
| `Read` 裸 `Buffer`、`result<ReadChunk>`、`result<std::string>` | **修正后采纳 owning Buffer** | 数据仍叫 `Buffer`，外层用 `ReadOutcome{state, data, error}` 表达 Data/Eof/Error；名称歧义不足以推翻用户的所有权选择，裸值却不能表达失败。 | L2:32–50；L3:11–22 |
| “移动返回值”即零拷贝 | **否决** | `Read` 必须从现有真源移交 backing storage；先 `TakeSnapshot` 复制、再 move 字符串不满足该契约。 | L1:38–47；L3:17–18 |
| 因现有 ring 做不到而否决零拷贝 | **修正后采纳** | 现状确实是复制读，但 `EventRingBuffer` 已可移动；本批补最小 pop-all 存储移交原语，不把底层缺口伪装成薄外壳已经支持。 | L3:18；快照 `detail/EventRingBuffer.hpp:100–112,204–215`；`detail/ReceiveRing.hpp:419–431` |
| `Buffer` 必须连续 | **否决作为本批保证** | 绕环和待合入尾部可分段拥有；强制连续会把线性化复制重新带回来，调用方需要连续字符串时自行显式复制。 | L1:40–47；L2:36–38 |
| `ReadAccess/TryAccess` 也进 `ICoConnection` | **否决进接口，保留 raw 能力** | 原有 `Connection::TryAccess` 不删除；协程接口只承诺 owned 数据，避免所有消费者学习守卫与持锁限制。 | L1:47；L3:174；L2:79–81 |
| owning 数据继续计入 `max_inbound_bytes` | **否决** | 该上限管库内未交付字节及预留/hold 责任，pop 后立即释放额度；用户持有的数据由用户管理，不在 Buffer 析构中回写连接。 | L1:42–47 |
| 非挂起 `Write` 与挂起 `CoWrite` 共用模糊名字 | **修正后采纳成员化** | 非挂起提交固定叫 `AsyncWrite`；既有 CoWrite 的等容量、等完成语义收为成员 `Write`，两者明确区分。 | L1:30–36,63–65；L2:11–18；L3:198–206 |
| `Write` 两个出参 | **采纳合并** | 挂起组合操作返回一个 `WriteReport`，包含等待结果、操作采样和观察票；不用交叉维护两个输出变量。 | L3:26–43 |
| `result<WriteReport>` 外层再加协议 Error | **否决** | net 保留 `std::error_code`；在一个返回对象里保存等待事实和发送事实，不再套一层另一体系的错误。 | L5:22–34 |
| `AsyncWrite → WriteOperation`，拒绝时只给 `Unknown` | **修正后采纳** | 返回票必须自带 `Rejected/0/具体原因`；只给无效票会丢失 Closed、过载、结算中的差别。 | L1:34–36；L3:133–135 |
| `WriteOperation` 收 detail 或只许作为 report 内部成员 | **否决** | 显式异步提交的调用者就是直接消费者，票必须可保存、查询和等待；记录布局及有效构造继续私有。 | L1:13,65；L3:91–100 |
| `Status` 用 muduo 单枚举三态 | **否决** | EOF、写接纳、逻辑关闭和物理释放相互正交，三态枚举无法无损表达半关闭及收口窗口。 | L2:57–61；L1:49–55 |
| `Status` 完全删除，留下六七个分方向 getter | **否决** | 保留只聚合既有事实的 `ConnectionStatus`；它不新增连接状态机，减少 getter 数量并满足用户的观察需求。 | L1:51–55；L3:55–67 |
| `IsConnected` | **否决** | 已交付仅证明曾建连，查询不能证明对端仍可达；半关闭也无法压成诚实的健康布尔值。 | L1:51–55；L3:60 |
| `IsClosed/Released` 与 Status 重复 | **修正后采纳** | 保留非虚便捷方法 `IsClosed/IsReleased`，只从 Status 投影，不维护第二套状态；物理名统一 `IsReleased`。 | L3:62–67；L5:122 |
| `Released` 只属于 Engine/pool，应从连接删除 | **否决** | 责任在 Engine 不等于不能从连接观察；已交付连接的池出账需要这个事实，不能以逻辑 Closed 代替。 | L1:67–73；L3:63–65；R:79 |
| `SetOnClose` 保留任意时刻换绑 | **否决** | 用户已明确 owner 短回调，技术上可以实现；否决理由是别名句柄换绑竞争、重复订阅面及 Failed 路径没有 owner 回调，不能说它“根本无法执行”。 | L1:20–28；L2:63–66；L3:73–79 |
| `SetOnClose` 改成 CoTcpConnection 构造注入 | **否决新增连接级槽** | 一次注入消除了换绑竞争，却仍复制 RawHandler 的事件出口；raw 回调已有 Engine 构造注入。 | L2:63–66；L3:75–79 |
| `WaitClosed(WaitOptions)` | **修正后采纳** | 等既有逻辑 Closed 谓词，在调用者业务协程恢复；不承诺 fd/owner 责任已清，也不把 EOF 当 Closed。 | L1:28,129；L5:90–92 |
| `Read EOF` 已覆盖全部关闭观察，不需等待入口 | **否决** | FIN 只终结读方向，写侧可继续；写侧业务也不应为了观察关闭被迫消费 ring。 | L1:51,129；L5:83–92 |
| `RawHandler::OnClose` 删除，或把 reason 改 CloseSource | **否决** | 保留现行签名和 owner 唯一通知，不新造来源枚举；注释补准为 fd 已关、回调责任未必已清。 | L3:219；L5:82–92 |
| `ICoConnection : ICoCloseable` | **否决** | 连接请求关闭不能兑现同步物理释放；只提供 `RequestClose`，不加 `Close/Seal/ICoSealable`。 | L3:47–53,81–89 |
| `CoAccept/CoDial` 归连接、独立连接工厂或 Engine | **采纳归 Engine** | listener/handoff、拨号配额和未交付回收都由 Engine 拥有，另造工厂只多一层。 | L2:68–71；L1:136 |
| `CoDial` 两个重载，或把 receipt 收 detail | **修正后采纳单签名，否决隐藏 receipt** | 单个 `ConnectionResult` 连同错误返回本次 `ReclaimReceipt`，失败拨号仍有物理观察票；HTTP 不得引用 net 内部头。 | L3:96–100；L1:69–73；R:79 |
| net 与协议都改为 `result<T>/Error` | **否决** | 两层继续各用现有错误体系，只在 adapter 接缝做一次权威映射。 | L5:24–34 |
| HTTP/Redis 各自维护 NetErrc 映射 | **采纳去重** | 一个 `NetToError` 负责底层分类，协议 owner 保留阶段与可信回复判断；同码不等于同一业务结果。 | L5:20–34,114,132 |
| 全仓 `RequestClose/Close` 统一改 `Seal` | **否决** | 本仓要求按模块责任判断；保留已正确的 RequestClose，修真实基类冲突，不能靠同名不同 namespace 推断资源泄漏。 | L5:68–74,100,133；R:70–72,87–88 |
| 顶层提前 return 改模块图 | **采纳，本批独立子任务** | 修目标可达性才能使新 HTTP/Redis/RPC 与 config/Mongo 同闭包，接口改名不能替代它。 | L4:57–96；R:37–54 |
| protocol 公开头完整 include Engine，或新增 ICoRuntime | **修正后采纳前置声明** | 只在实现文件 include Engine；保留现有 Engine 注入契约，拒绝没有新职责的 runtime 抽象。 | L4:9–14,121 |
| 自动拼“当前存在模块”的 `infra_net_full` umbrella | **后续迭代** | 不解决 F2 根因，且可能把缺失能力静默藏进同名目标；本批消费明确目标。 | L4:115–117 |
| `detail` 物理可达，增加 include 必须失败测试 | **否决当前布局下的负向断言** | 仓根仍由 BUILD_INTERFACE 导出时该 include 本就能成功；本批核公共头不引用 detail，物理隔离另行安排。 | L4:39–45,126；开发指南:27 |
| muduo 高水位回调、无界输出；Asio token/整体 cancel；gnet 借用 view | **否决引入本接口** | 分别削弱硬有界接纳、扩大异步派发模型、改变已有等待/所有权边界，没有本批必要性。 | L2:75–97 |

## 2) 最终接口骨架（C++ 注释级）

下面的声明是建议冻结的形态。拆分为 `Buffer.hpp`、`WriteOperation.hpp`、`ICoConnection.hpp`、`CoTcpConnection.hpp` 和连接交付结果头；只前向声明内部类型，定义留在 `.cc`。`Engine` 不出现在这些连接头中。`ReadStatus/ReadResult` 的原有 raw try 形态可继续保留，不能为了名字少而与 datagram 的截断结果强行合并。

```cpp
// C++17；以下按依赖顺序展示新/调整声明，非一份替换全部旧头的实现。
#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/infra/net/Connection.hpp> // raw 值句柄、既有公共值类型

namespace bbt::infra::net {

class ReceiveRing;       // 仅 friend 使用，不公开布局
class ConnectionState;
struct WriteOpRecord;

// 独占拥有已取走的字节；可跨协程/线程移交，寿命独立于连接和 Engine。
// 不持 ring 锁、不在析构中返还连接额度；不含 Boost/Asio 公共类型。
class Buffer final {
public:
    Buffer() noexcept;
    ~Buffer();                           // 本对象字节释放；不做网络动作
    Buffer(Buffer&&) noexcept;            // 移交所有权，源变为空
    Buffer& operator=(Buffer&&) noexcept;
    Buffer(const Buffer&) = delete;
    Buffer& operator=(const Buffer&) = delete;

    std::size_t Size() const noexcept;    // 总字节数，非容量
    std::size_t SegmentCount() const noexcept;
    // 任意域、非挂起；i < SegmentCount()。按 FIFO 遍历全部片段。
    // view/指针借用本 Buffer；销毁或移动 Buffer 后不得继续使用。
    std::string_view Segment(std::size_t i) const noexcept;
    char* MutableData(std::size_t i) noexcept; // 用户独占数据可修改；长度看 Segment
private:
    friend class ReceiveRing;
    struct Storage;
    std::unique_ptr<Storage> storage_;
};

enum class ReadState { Data, Eof, Error };
struct ReadOutcome {
    ReadState state{ReadState::Error};
    Buffer data;
    std::error_code error;
    // Data: data.Size() > 0 && !error；Eof: 空 data && !error；
    // Error: 空 data && error。失败零消费；没有“空 Data”或裸空 Buffer 表 EOF。
};

struct WriteReport;

// 调整既有 WriteOperation，不在 Connection.hpp 再定义一个同名类型。
// Query/Wait 都查同一记录；释放观察票不取消发送，也不延长 Engine/fd 寿命。
class WriteOperation {
public:
    WriteOperation() noexcept;           // 默认无身份，Query() == Unknown/0/空
    bool Valid() const noexcept;          // 是否确实被接纳；仅持有 op record 时 true
    // 任意合法域，非挂起。返回采样；终态在票寿命内永久可查。
    // AsyncWrite 拒绝返回 recordless 票，但 Query == Rejected/0/具体错误。
    // 因此 !Valid() 不再足以推断 Query == Unknown；无身份且无拒绝原因才是 Unknown。
    WriteOutcome Query() const;
    // 仅业务协程，可挂起；gate 在查快路径之前；只等该操作，不等容量。
    // Rejected 已落定：wait_error 为空、outcome 保留拒绝；Unknown 返回 invalid_argument。
    // 沿现有 write hub 醒后复查记录，无 per-op 新 hub。
    // 等待超时/取消不撤销 I/O；report.operation 保留本票供日后 Query。
    WriteReport Wait(const bbt::coroutine::WaitOptions& options = {}) const;
private:
    friend class Connection;
    friend class CoTcpConnection;
    std::shared_ptr<WriteOpRecord> record_;
    std::weak_ptr<ConnectionState> state_; // Wait 取局部强引用；避免与 state 成环
    std::error_code rejection_;           // 拒绝信息内联存储，不为拒绝分配 op
};

struct WriteReport {
    std::error_code wait_error;           // 本次等待是否正常完成；不冒充发送错误
    WriteOutcome outcome;                // 同一操作在返回处的采样
    WriteOperation operation;            // 已接纳时有效；超时也不丢失
    // 唯一本地发送成功判据：Done && !outcome.error（且内部保证 sent==总长）。
    // wait_error 可以非空而 outcome 已采到 Done；两者不能互相覆盖。
    // Rejected + 无有效 operation = 未接纳；InFlight + 有效 operation = 仍在途。
};

// 诊断/资源观察值；每字段来自既有真源，跨方向分别采样，不承诺原子总快照。
// 不可用一次 Status 来证明下一次操作必成功；是否接纳只看操作本身。
struct ConnectionStatus {
    bool valid{false};
    std::size_t readable_bytes{0};        // 库内已发布、尚未交付字节的采样
    bool read_eof{true};                  // 读方向已见 EOF，可与尾部字节共存
    std::error_code read_error;           // 持久读错误，可与尾部字节共存
    AcceptanceState write{AcceptanceState::Closed};
    bool closed{true};                   // 既有逻辑 Closed；不是请求刚登记的标志
    std::error_code close_reason;         // 第一次请求原因固定，包括默认空原因
    bool released{true};                 // fd 已关且 owner 本连接责任清理
};

class ICoConnection {                     // 不继承 ICoCloseable/新通用关闭基类
public:
    virtual ~ICoConnection() = default;

    // 仅业务协程，可挂起；先 gate，再访问任何数据/快路径。
    // 等到有数据时，原子取走线性化点上所有已发布可读字节并移交 backing。
    // Busy/WouldBlock 内部等待；先交尾部，耗尽后才 Eof/Error。
    // options 只约束等待；超时/取消/OOM 不消费，不改为自动 RequestClose。
    virtual ReadOutcome Read(
        const bbt::coroutine::WaitOptions& options = {}) = 0;

    // 任意合法调用域，非挂起；全量接纳或零接纳，不等容量/发送完成。
    // 按值取得字符串；接纳后库拥有发送数据。拒绝原因只经返回票 Query 表达。
    // 调用方 move 传入后，即使拒绝也不得依赖原字符串内容被保留。
    virtual WriteOperation AsyncWrite(std::string data) = 0;

    // 仅业务协程，可挂起；原 CoWrite 的成员化组合操作。
    // 同一绝对 deadline 等容量 -> 接纳 -> 等本地完成；不等业务回包。
    // 等容量失败为 Rejected；接纳后等待结束仍返回 op 与真实采样。
    virtual WriteReport Write(
        std::string data, const bbt::coroutine::WaitOptions& options = {}) = 0;

    // 仅业务协程，可挂起；等 Status().closed 对应的既有逻辑谓词。
    // 返回 error_code 只描述此次等待；成功后原因从 Status().close_reason 读。
    // 不是 WaitReleased，不消费 ring，不等业务协程/RawHandler 回调全部退出。
    virtual std::error_code WaitClosed(
        const bbt::coroutine::WaitOptions& options = {}) = 0;

    // 任意合法域，非挂起、幂等；拒绝新写并唤 owner，第一次 reason 固定。
    // 不保证冲刷未发完写；owner 结算它们，保留已发前缀，不冒充发送成功。
    // fd/已 recv 尾部归 owner 收口；析构及普通 I/O 错误不隐式触发本策略。
    virtual void RequestClose(std::error_code reason = {}) = 0;

    // 任意合法域，非挂起；返回值自有，无需持锁使用，无网络探活副作用。
    virtual ConnectionStatus Status() const = 0;

    // 非虚便捷投影，不维护第二套标志；这些投影也不提供跨查询原子性。
    bool Valid() const { return Status().valid; }
    bool IsClosed() const { return Status().closed; }
    bool IsReleased() const { return Status().released; }
};

class CoTcpConnection final : public ICoConnection {
public:
    CoTcpConnection() = default;          // 无效值；无 socket 责任
    explicit CoTcpConnection(Connection connection); // 仅包装现成 raw 句柄，不拨号
    ~CoTcpConnection() override = default; // 不隐式 RequestClose，不等物理回收
    CoTcpConnection(const CoTcpConnection&) = default;
    CoTcpConnection& operator=(const CoTcpConnection&) = default;
    CoTcpConnection(CoTcpConnection&&) = default;
    CoTcpConnection& operator=(CoTcpConnection&&) = default;

    // 以下每个 override 完整继承 ICoConnection 的域/挂起/所有权/错误契约。
    ReadOutcome Read(const bbt::coroutine::WaitOptions& options = {}) override;
    WriteOperation AsyncWrite(std::string data) override;
    WriteReport Write(std::string data,
        const bbt::coroutine::WaitOptions& options = {}) override;
    std::error_code WaitClosed(
        const bbt::coroutine::WaitOptions& options = {}) override;
    void RequestClose(std::error_code reason = {}) override;
    ConnectionStatus Status() const override;
private:
    Connection connection_;               // 只有共享值句柄，无第二数据队列/关闭槽
};

// ReclaimReceipt 保留既有公共类型：Valid()/Released()；布局与有效构造私有。
struct ConnectionResult {
    std::error_code error;                // 本次交付结果；!error iff connection.Valid()
    CoTcpConnection connection;           // 成功才有效；值包装，不另分配 facade
    ReclaimReceipt reclaim;              // 本次提交的资源观察，不取消/不延长 fd
    // Dial 提交前拒绝：无效 receipt，Released()==true（没有责任）。
    // Dial 已接纳后未交付：连接无效，但 receipt 有效，回收完成前保持 false。
    // 成功交付（Accept/Dial）：receipt 与 connection.IsReleased 同一真源。
};

} // namespace bbt::infra::net
```

`WriteOperation` 原有的 `Id()` 若仍被 raw 事件关联使用，继续保留在其实际头中；本骨架聚焦新消费契约，不把现有身份删减夹带进来。将它从原头抽到独立公共头时，由原头 include 新头保持唯一类定义。

`Engine.hpp` 拼入以下**新增成员**，不创建另一个 Engine 或复刻旧公开面；它可 include 连接结果头，但连接头不能反向 include Engine：

```cpp
// 拼入 namespace bbt::infra::net 下现有 class Engine 的 public 区。
// 仅业务协程，可挂起，先执行协程 gate；从既有有界 handoff 领取。
// 保留现有配置/相位拒绝优先级，超时不关闭供别的等待者领取的连接。
ConnectionResult CoAccept(const bbt::coroutine::WaitOptions& options = {});

// 仅业务协程，可挂起，先 gate；仅无 RawHandler 的 Engine 可提交。
// 唯一成员签名；内部仍“提交 -> 领取”，不公开中间 ID。
// 未交付离开必须归还 claim 并由 owner 回收；错误返回不等于 fd 已释放。
ConnectionResult CoDial(const Address& address,
    const bbt::coroutine::WaitOptions& options = {});

// 既有 Engine::Run / RequestClose / RequestStopAccepting / AcceptingStopped /
// AsyncConnect / 状态观测维持各自职责；Run 返回才证明 Engine 网络责任已清。
```

最小消费方式如下；这是拟议接口的调用示意，没有把它标成现成可运行示例：

```cpp
// 已在业务协程；宿主在另一 native 控制线程驱动 engine.Run。
auto dial = engine.CoDial(address, options);
if (dial.error) {
    // 外部池保存 dial.reclaim，真实 Released 前不能物理出账。
    return;
}
auto& conn = dial.connection;
auto sent = conn.Write(std::move(request_bytes), options);
if (sent.wait_error || sent.outcome.phase != WritePhase::Done || sent.outcome.error) {
    // 已接纳时保存 sent.operation；不能把等待结束当作零提交或自动重发理由。
    conn.RequestClose();
    return;
}
auto read = conn.Read(options);
if (read.state == ReadState::Data) {
    Buffer owned = std::move(read.data); // 可移交别的任务，连接继续使用同一真源
}
conn.RequestClose();
auto close_wait = conn.WaitClosed(options); // 仅逻辑；最终宿主仍须等 Run 返回
```

实现约束与异常约定：

1. 每个可能等待的成员进入后先复制 raw/state 强引用到局部；等待回调只捕获 state/op，不捕获 facade 的 `this`。不缓存“上次 op”或单个常驻 waiter。调用者仍须保证进入/调用期间对象生命周期合法，不把 state 保活夸大为“任意并发析构同一 C++ 对象安全”。
2. Read 保留“Busy 前采释放代次、登记后复查、锁外 Notify”，等待名额沿既有 hub 有界，满额 `no_buffer_space`。WaitClosed 必须补 Create/Add-close-waiter 的内部入口，不能直接拿 RegisterCloseWaiter 在 runtime 登记前注册；driver Failed 的内部冻结路径同样唤醒，绝不控制域伪调 RawHandler。依据 L1:75–81,121–135。
3. pop-all 持短保护在线性化点取得 ring/pending/detached 全部已发布前缀；尚在 owner recv_hold 的字节稍后发布，不属于这次快照。尾部未交付前禁止 EOF。传出存储后连接保留同一逻辑真源继续接收，不建第二权威队列。零拷贝指**交付 backing 不复制载荷**，不承诺 recv、扩容或协议解析全链路零复制。
4. Buffer 承接绕环 backing 及待发布尾部的所有权，必要的新空存储与结果元数据先准备，成功后一次提交。分配失败不丢字节、不先减额度。pending/detached 的表示若仍需在线性化时复制，必须一并改到可移交表示，不能把隐藏复制放进 Read 来兑现“零拷贝”。这是本批内核工作，非已成立事实。
5. `max_inbound_bytes` 包括库内 ring、pending/detached、owner hold/预留责任；pop 成功后该批字节不再计入。它不是“库+用户所有 Buffer 的总内存上限”，也不承诺 backing capacity 等于有效字节数。对象和 Buffer 都不负责限制业务囤积。
6. RequestClose 是尽快终止连接责任，不新增 graceful-drain 模式。已完成写保留成功；未完整发送写必须以 Done + 非空错误结算，默认空 CloseReason 不能让未发完写变成功。该处是本方案要求冻结的契约与验收点，不冒称已动态证实了一个新缺陷。依据 L2:40–42；快照 `Engine.cc:512–518,555–585` 与 `ConnectionState.hpp:307–348`。
7. 已有 net/标准 `error_code` 是运行错误出口，不新增异常或 callback 错误出口。等待错误与发送错误是两种事实，装进同一 report 不代表可以丢其中一项；例如等待超时首胜后采到 Done。参数按值复制发生在函数进入前，仍遵守普通 C++ 分配异常规则，不给这些入口虚假的全路径 `noexcept`。
8. Status 的方向字段分别采样；实现应先采 Released 再采 Closed，保证同一次返回不出现 `released=true && closed=false`，前提是维持既有“先发布 Closed 后 Released”的顺序。即便如此，它仍不是完整原子快照，readable_bytes 不保证涵盖尚未发布的 owner hold。

命名与迁移表：

| 旧/草案入口 | 最终入口 | 行为与迁移要求 |
|---|---|---|
| `CoRead(conn, out, options)` | `CoTcpConnection(conn).Read(options)` | 返回 owned Buffer；原 out 累加需调用方逐段显式处理，失败零消费。 |
| `CoWrite(conn, data, options, outcome, operation)` | `conn.Write(data, options)` | `report.wait_error/outcome/operation` 替代返回值及双出参；保留等容量语义。 |
| 草案非挂起 `Write` | `conn.AsyncWrite(data)` | 明确只提交；`Query` 中带拒绝原因，不把提交成功叫本地完成。 |
| `AsyncWrite(data, out_op)` | `conn.AsyncWrite(data) -> WriteOperation` | 新 facade 单返回；raw Connection 原成员可暂保留实现接缝。 |
| 另建 `WaitWrite(conn, op, options)` | `op.Wait(options)` | 等待归操作票；不用连接仍存活才能查最终结果。 |
| free `CoAccept/CoDial`、带/不带 receipt 重载 | `engine.CoAccept/CoDial -> ConnectionResult` | 每个成员一个签名；忽略 receipt 的普通消费者无须另选重载。 |
| `Closed()/Released()` | `IsClosed()/IsReleased()` | 分别是逻辑终态与物理出账事实，均为 Status 投影。 |
| `ReadEof/ReadError/ReadableBytes/WriteAcceptance/CloseReason` | `Status()` 对应字段 | 从接口删重复 getter；raw Connection 维持既有查询供 raw 消费。 |
| `Status` 单枚举、`IsConnected` | `ConnectionStatus` 聚合；删除 IsConnected | 不维护“健康连接”或 Connecting 状态机。 |
| `SetOnClose`/连接级构造回调 | `WaitClosed`；raw 用户用既有 `RawHandler::OnClose` | WaitClosed 等逻辑终态；OnClose 是 owner fd-close 通知，均不冒充 Released。 |
| `RequestClose` | `RequestClose` | 名称和异步意图保留；没有同步 `Close` 或 `Seal` 别名。 |
| `ReclaimReceipt` | 保留公共类型及现有 `Released()` | 票非连接，不为对仗再改名；默认无效票 Released=true。 |
| `Id/LastWriteError/TryAccess` | 不进入 ICoConnection | raw 事件关联、旧观测/守卫保留；不顺带破坏已存在 raw API。 |

新形态冻结后，迁移仓内 HTTP、Redis 及受影响服务示例与测试，移除公开 CoRead/CoWrite/CoAccept/CoDial 自由入口及其 friend 门面中不再需要的装配；raw Connection、RawHandler 留作独立 raw 消费面。不以无限期兼容重载维持两套正式 Co API。

## 3) 本批必做清单（含与 F1/F2/F3 的关系）

本批是一个验收集合，可以分四个独立变更：A 连接接口与直接消费者、B F1 HTTP 默认期限、C F2 构建组合、D F3 Watcher。共享公开头由一位负责人协调；B/C/D 可独立准备，不要求为它们先重写网络内核。不得以 A 完成宣称 B/C/D 已修。

| 任务 | 必须产出 | 验收判据与外部判定者 | 关联 |
|---|---|---|---|
| A1 接口、文档与消费例 | 上述头、迁移表、一个 TCP client/server 的成员调用例；同步更新公开符号 allowlist | C++17 公共头自给编译；仓外消费者只链接公开 target，走真实 TCP Read/Write/关闭并检查结果。编译器和真实 peer 判定，模型静态阅读不算运行结果。 | L1:121–130；L3:196–219 |
| A2 真正 owning pop-all | 扩展同一 ReceiveRing/EventRingBuffer 的存储移交事务；不经 TakeSnapshot 实现新 Read | 受控交错覆盖绕环、多 reader、pending/detached 尾部、owner append Busy、Close 前 hold；用私有探针核 backing 身份/复制路径，证明移交零载荷复制。注入准备分配失败断言 ring/额度不变；保留 Buffer 到连接及 Engine 结束后验证内容。 | L1:38–47,123–130；用户零拷贝要求 |
| A3 写结果与原等待语义 | AsyncWrite 拒绝票、成员 Write、op.Wait，沿用当前 FIFO、容量和终态仲裁 | 真实慢 peer + 阶段屏障：未接纳超时、已接纳超时/取消、等待首胜后采到 Done、部分发送错误、零长写、过大永不可能接纳、RequestClose 默认 reason 下未发完写非成功；释放 facade 后 op 终态仍可查。禁止 sleep 代替进入阶段证据。 | L1:30–36,57–65；L3:26–43 |
| A4 关闭观察与 Status | 无 SetOnClose 槽；WaitClosed 复用 close hub；Status 投影；保持 OnClose 原签名 | FIN 后仍可写；Read 排空尾部才 EOF；晚到 WaitClosed 不丢通知；driver Failed 内部唤醒且不伪调 OnClose；在 OnClose 内断言不把 Released 当已真。池只在 Released/receipt 真后出账。 | L1:49–55,123–136；L5:82–92 |
| A5 adapter 错误桥 | 一个公共接缝 `NetToError` 与权威映射表；HTTP/Redis 删重复 raw code 分类 | 表驱动覆盖 Closed、超时、取消、Busy/WriteSettling、资源名额错误及未知 backend code；HTTP/Redis 相同 cause 得相同基础分类，随后保留各自已接纳/完整写出/可信回复的原有结算。真实回复丢失场景仍为既有 OutcomeUnknown/阶段事实，不自动重放。 | L5:20–34,114,126；不重划既有分界 |
| A6 RPC Error 字段往返 | `RpcError` 新增具备 presence 的 request_phase，编码/解码不丢 Error 的该字段；补来源说明 | 固定 protobuf 版本 codec roundtrip 覆盖 nullopt、各合法阶段、旧 peer 缺字段、未知值；缺字段保持 nullopt，不能补成 NotStarted/未提交。客户端本地 transport error 的阶段继续来自本地 HTTP operation，不被远端 Error 的来源阶段覆盖。 | L5:112,135；快照 `RpcWire.cc:189–206,231–244` |
| B F1 默认调用 | 只将新 HTTP 省略参数的默认值改为 `CallOptions{Deadline::max()}`；阶段默认期限仍生效 | 一个省略最终 CallOptions 的真实成功 GET/Send；显式过去 deadline 在池/I/O 前拒绝且 peer 零请求。保留显式 `CallOptions{}` 的当前过期含义，RPC 必须提供期限的拒绝不改。 | **F1 独立修，接口迁移不会自动覆盖**；R:17–35 |
| C F2 可组合目标图 | 取消“任一新模块开关即顶层 return”的互斥结构；公共依赖接入与 pin 一次，按模块接目标 | 同一次仓外 add_subdirectory/link/run 新 HTTP + common + config；有现成授权前缀/后端时再验证新 Redis + Mongo。所有选中目标只链接一份 coroutine；纯 net 与 HTTP-only 不拉旧栈/可选包。 | **F2 同批独立子任务，阻断组合收口**；R:37–54；L4:57–103 |
| C2 公开头接线 | 协议头 Engine 改 forward-decl；新 target 通过 common 传递值头；处理同名 RpcWire 正式入口 | 编译/消费测试覆盖 common 与 RPC 两种 target 链接顺序，命中同一 RPC 公共声明；公共头不 include net/http 等兄弟 detail。旧孤儿 RpcWire 不再列入正式公共自检，以消费盘点支撑其退出，不制造一个没有实现的兼容 target。 | L4:9–28,39–53,121–132；R:89,96 |
| D F3 Watcher 契约 | 从 Watcher 移除 ICoCloseable；改为独立 RequestClose 与显式宿主 WaitStopped，修提前 MarkClosed | 在回调确实进入后调用 RequestClose，外部等待直到 loop 真退出；预先登记尚未运行路径不提前报物理完成；native 超时返回 TimedOut，回调/受管协程等自身立即 InvalidContext；正常等待成功时无后续回调/等待资源责任。 | **F3 单独修，ICoConnection 不继承该基类并不能修掉 Watcher**；R:56–74；L5:102–106 |

A5 的桥可置于公共值层与 net 的接缝头，例如 `include/bbt/infra/NetError.hpp`，由 HTTP/Redis include，**net 原语头不 include Result/Error**，common 也不反向链接 net。基础分类至少统一 `NetErrc::Closed → ErrorCode::Closed`，资源竞争/结算中/名额不足按已声明规则映射 Overloaded，deadline/cancel 保留对应分类；`backend_category` 存真实 `ec.category().name()`，`backend_code` 存 `ec.value()`，domain_code 才使用 `net.*`。EOF 先交给协议 parser 判断是否是合法定界，不能在桥中一律升级 OutcomeUnknown；Busy/WouldBlock 是 try/等待条件，也不能直接冒充最终传输失败。阶段与可信回复策略仍由协议 owner 结算，这不是第二套 errno 映射。

A6 的落点明确为 `RpcError` 的新字段号 9，使用 `optional RpcRequestPhase request_phase = 9`；独立 wire enum 保留 UNSPECIFIED=0，合法阶段另占明确值，不能把 C++ `NotStarted=0` 当缺字段默认。已有字段号与 profile_version 不重排。只有存在且合法才写入 Error.request_phase；出现已存在字段的非法枚举值按 ProtocolError 拒绝。远端 Error 的 phase 表示该 Error 来源 operation，不是对本地 RPC 提交程度的推断依据。这是字段保真修补，不声称当前客户端本地 HTTP 阶段出口全部失效。

C 的开关边界如下，不能原样照搬 L4“config 总是构建”的建议：

- `BBT_INFRA_NET` 单独选择 net；任一 *_NET 协议开关建立 net 的依赖闭包。新旧目标名保持独立，不为组合强制迁移 config/Mongo。
- 普通模式提供独立 config/Mongo/legacy-stack 选择；默认保留旧普通构建习惯。有明确模块 ON 却缺必需前缀时明确拒绝，不能导出假成功目标。
- `BBT_INFRA_NETWORK_ONLY=ON` 继续严格只含新 net 及显式选择的新协议闭包；config、Mongo、legacy 默认关闭，显式冲突选择报配置错误，不能被残留 prefix 偷偷启用。`NETWORK_ONLY=OFF + HTTP_NET=ON + CONFIG=ON` 才是新 HTTP 与 config 组合。
- common 与 internal_debug 在公共段定义一次；coroutine 接入、源路径校验、pin、CMake policy 和上游开关遮蔽规则合并。公共段不无条件发现 core/hiredis/mongocxx/protobuf。选到新 net 时使用已有 Boost 1.90 要求，不借此扩大旧栈最低依赖变更。
- 自检只针对本次选中公共契约及它实际引用的值头；公共/内部 allowlist、CTest 注册集与实际执行集相符。头文件 source-root 可达不等于正式公共支持，也不伪造“include detail 必须失败”的测试。

D 的建议出口为 `RequestClose() noexcept`（任意合法域、非挂起）和 `result<void> WaitStopped(Deadline)`（仅非受管宿主 native 控制域等待，失败显式报告）。Watcher 的 `IsClosed` 只表示其独立逻辑封口，物理事实由 WaitStopped 成功证明；不继承同步关闭基类。析构只请求封口，需排空的宿主先 WaitStopped 再释放对象；loop/退出记号具有独立寿命，不允许靠提前 MarkClosed 或 weak_ptr 消失伪造已退出。该变更需同步更新 Watcher 消费者，不修改 ICoCloseable 的同步物理承诺。

执行次序：先接受第 2 节签名/语义，再完成 A2/A3/A4 的机制与针对性契约测试，随后迁移 A1 的消费者并完成 A5/A6；B/C/D 各自单独验证，最后做一次成员形态的公开消费与组合验收。各次验证绑定各自产物 SHA，不把旧 CI 绿灯转记为新接口通过。

验证命令形状沿本仓规范：新 configure 使用 `-DCMAKE_CXX_COMPILER_LAUNCHER=ccache`，私有 build；构建只选本任务目标并固定 `cmake --build <private-build> --target <affected-targets> --parallel 2`，`ctest --test-dir <private-build> -j1 -R '<本批实际注册测试>' --output-on-failure --no-tests=error`，重任务经仓内既有构建队列。这里是执行要求，**本终评未运行这些产品构建或测试**；不得把拟定测试名或模板当结果。

Review Focus：上述验收已覆盖主要公开输入。仍须实现负责人交独立评审者核三项：A2 的预分配与 pop 提交之间故障是否保留全部责任；A3 的 wait 首胜与 Done 采样是否被错误折叠；C2 的 common-first/RPC-first include 顺序是否命中同一声明。判定材料是私有阶段探针输出、故障注入断言和真实 compile commands；没有对应证据就只标该项未验，不扩大成全仓验证。

规范核对：C++17、namespace 不加第四层、infra→coroutine 单向、不新增 core/第三方依赖、不引用兄弟内部实现、保留 raw 与协程同一 ring、区分逻辑与物理关闭均满足 AGENTS.md 和开发指南的约束。公共接口破坏与消费者迁移在此仅形成可审阅提案；本次只写终评，不执行该破坏性迁移。

## 4) 后续迭代清单

| 建议 | 后续触发条件与边界 |
|---|---|
| `PendingWriteBytes` 等积压观测 | 有真实节流消费者时再补事实字段；本批的硬拒绝及显式 Write 等容量已能正确使用，不先加高水位回调。L2:28–30,52–55。 |
| Buffer 连续化 helper、用户侧拼帧便捷函数 | 多个消费者出现相同代码后再加；明确哪个调用会复制，不改变 owned/分段的核心契约。 |
| pop-all 后 backing 复用、池化、减少重新分配、按 op 唤醒 | 本批先证明移交/故障正确；有相同真实负载的吞吐、尾延迟及内存数据再优化，不引进 per-op hub 或无界池。L1:134–135。 |
| Engine graceful drain、shutdown-write/半关闭控制、per-op cancel | 需要新增业务能力时独立定义；不把 RequestClose 改成隐式冲刷，也不改变普通等待不取消的规则。L2:40–42,95–97。 |
| 完整安装/导出头隔离 | 出现 install/export 交付任务时整理 include 布局、detail 不安装，并测试物理不可达；本批 source-tree include 方式不虚称已隔离。L4:39–45,126。 |
| `infra_net_full` umbrella | 先定义固定能力/缺依赖行为后再考虑；不采用“有什么就偷偷链什么”的 full 契约。L4:115–117。 |
| 旧 HTTP PUBLIC transport 边、proto target include 面、legacy Redis codec 共享目录 | 用实际公开类型与目标检查决定 PUBLIC/PRIVATE/共享值层；它们不自动构成 F2 根因，不借本批做大规模 target 整形。L4:16–35,75–80,122–125。 |
| 新旧协议 `Close` 的统一命名、MongoColl 的基类关系、Redis Failed/Disconnected 文档 | 有可替换契约冲突或真实误用证据时独立处理；保留 Mongo owner/handle 的已正确资源归属，不把 handle 未 drain 说成 owner 泄漏。L5:100,118–120；R:87–88。 |
| 删除 raw LastWriteError、Id 或合并所有 IoResult 类型 | 先盘点各自用途与行为差异；操作级结果、连接级观测和 datagram 截断不是同一事实，不为减少名字牺牲语义。L3:223–228。 |
| TLS/DNS/UDP、跨平台、长压、sanitizer 与性能门槛 | 沿原范围另立验证；既有报告没有覆盖，不能从本批 TCP 冒烟推导支持。 |

直接否决而非“以后照抄”的项：muduo 无界队列+高水位通知替代硬接纳、Asio completion-token 多形态派发/整体 socket.cancel、gnet callback-only 裸 view、健康 `IsConnected`、单枚举总状态机、ICoCloseable 继承、可变连接级关闭槽、隐藏跨模块必要的回收票，以及全仓强制改名 Seal。后续真实需求改变时可重新提案；它们不是待办占位。

## 5) 与用户的分歧清单

以下只列对给定意图的实际偏离，不把用户未要求的假想 API 当成分歧：

| 用户给定意图 | 本终评取舍 | 分歧与代价 |
|---|---|---|
| `Read → owning Buffer` | `Read → ReadOutcome`，其中 data 是 owning Buffer | **外层返回形态不同**：必须可区分 EOF/错误，数据所有权、pop-all 与零拷贝移交意图保留。 |
| “pop 全部可读字节、零拷贝 move” | 保留，但交付可分段的 backing；以发布/线性化点界定“全部” | **澄清与实现成本**：不保证一个连续 data 指针，不包含还在 owner hold 尚未发布的字节；需要本批改 ring 导出原语，不能只改外壳。 |
| `Write → 非挂起 AsyncWrite 返回 WriteOperation` | 非挂起入口确为 AsyncWrite；另将旧 CoWrite 的等待能力放入成员 Write | **多保留一个明确挂起成员**：保留既有等容量使用方式；只提交的用户只学 AsyncWrite/Query 即可。 |
| 单一返回，尽量少学类型 | 挂起 Write 用一个 WriteReport，保留 wait_error 与 outcome 两个不同事实 | **不能进一步压成一个错误码**：否则等待超时与随后完成的竞态会丢失信息；不增加异常/第二套错误体系。 |
| `Status/IsConnected/IsClosed/Released` | 保留聚合 Status，IsClosed/IsReleased 为便捷投影，删除 IsConnected | **删除健康布尔，Released 改名**：半关闭和网络可达性不能诚实实现该布尔；保留其余观察能力。 |
| `SetOnClose(owner 域短回调事件)` | 不设连接级槽；raw 事件仍是 OnClose，协程侧增加 WaitClosed | **实质分歧**：用户的 owner 短回调并非不可实现；本终评仍选择避免第二订阅面。WaitClosed 等逻辑 Closed，不能当成同域、同阶段事件的直接替换。 |
| `CoTcpConnection` 薄实现 | 保留，只持 raw Connection | **无接口方向分歧，有底层工作量**：Buffer 真移交与新增等待需在现有 state/ring 上补能力，薄 facade 本身不会提供这些能力。 |
| 自由函数收成员、Engine 装配/Run/RequestClose | CoRead/CoWrite 归连接，CoAccept/CoDial 归 Engine | **明确归属，非额外工厂**：Engine 增加两项交付成员；物理回收票随单一返回保留。 |

与用户一致的点：面向对象、Co 表示协程域、数据归用户、AsyncWrite 按值接管、RequestClose 不假装同步关闭、Engine 宿主责任不下放。接口最小化通过聚合事实和移除重复 getter/注册槽完成，不通过删去物理回收或在途观察能力达成。

## 6) 未裁决风险

以下是仍缺执行证据或需要实施时回核的风险，不是未选定的 API 选项，也不把方案标成已接受：

1. **真零拷贝 pop-all 的故障/公平性尚无运行证据。** 快照已有可移动 ring，但 pending/detached 的存储移交、替换空 backing 的准备、锁序与 owner 预留尚未实现。A2 未通过前，不得交付“zero-copy Read”；性能复用可后置，正确所有权不能后置。
2. **Rejected 观察票扩展了 WriteOperation 旧语义。** 旧无效票 Query=Unknown，新 AsyncWrite 的拒绝票 Query=Rejected。必须逐个检查依赖 `!Valid ⇒ Unknown` 的调用者；raw 内部构造与 public Query 注释需同时更新，不静默改变旧假设。A3 是冻结前门禁。
3. **Status 是采样，不能承诺完整原子状态。** 提案指定的采样顺序只维护 Released/Closed 单调含义；并发读者仍可能拿走 readable_bytes。若后续消费者要求原子复合决策，应把决策放到操作事务，不能添一个“更准确的 IsConnected”。
4. **Failed 路径不是回调保证。** WaitClosed 来源于内部冻结状态，OnClose 正常 owner 路径最多一次；Failed 不伪调用户回调。资源出账仍看 Released/receipt，Engine 最终仍看 Run 返回，不能用一个新等待方法覆盖全部责任。L1:129；L5:92。
5. **RPC phase 的跨进程保真与旧 peer 互通未测。** 新字段保留来源 Error 的事实；旧 peer 会丢弃新字段，重试判断必须把 nullopt 当未知而非未提交。A6 不允许据此改写 HTTP/Redis 原有 OutcomeUnknown 边界。
6. **F2 的完整目标组合与 CI 注册未运行。** 需使用实际 coroutine/protobuf/hiredis/Mongo pin/前缀执行消费者。缺授权依赖或真实服务时，分别写明哪些组合 BLOCKED；已有纯新栈 CI 不证明新旧组合。CMake 新测试是否已被现行 job 选中要查实，CI 改动不由本终评自动授权。
7. **F3 的消费者兼容性与 loop 退出寿命要实施时核对。** 本终评选择独立逻辑关闭并显式等物理终态；仍需迁移把 Watcher 存为 ICoCloseable 的 owner，并对 callback 内释放最后一个句柄、未调度 loop、native 超时做阶段验证。不能只移除继承声明就认为完成。
8. **部分 lane 推断不升级为 confirmed。** L5:124 的“RequestClose 吞异常导致请求丢失、Run 永不返回”尚未证明：快照 `Engine.cc:2297–2303` 先持久置 close_requested 再 SealService；仍可定点核异常后的唤醒与最终收口，但不能据 catch-all 单独下结论。L5:118 对 MongoColl 的 owner 泄漏推断亦不采纳，先前 R:87–88 已反证资源归属。L4:94 对双 build-tree 必然 ODR 冲突是风险场景，F2 的已证根因仅需目标图不可达，无须依赖该推断。
9. **外部公共消费者盘点未完成。** Co 自由函数退出、Watcher 基类关系、旧 RpcWire 正式入口清理均是公共变更；应以真实消费清单给出迁移说明，不宣称 ABI/源码完全兼容。此终评只形成可执行的取舍，不代表获得实施、消费者切换、合并或发布批准。

本次实际完成的是报告通读、被审快照的关键头/实现定点核对、裁决与方案落盘。没有修改产品代码，没有运行产品 build/test，没有重新验证外库或线上/跨仓消费者。后续验收以第 3 节每项的实际产物与结果为准。
