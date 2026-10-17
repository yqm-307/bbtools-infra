#pragma once

// 私有连接桥（P3-c1）：公共 ICoConnection/CoTcpConnection/Handler 与
// 已验收 StreamConnection 之间的真实接缝，也是未来 Engine 可
// 复用的私有 factory/业务 drain 完成点。本包不建公共 Engine。
//
// 执行域（依据 docs/architecture/gnet-network-execution.md §4–6、决策
// 0010）：
// - I/O owner（EventLoop 线程）：只做短通知/状态发布/物理收口命令，
//   不运行业务协程、不调用 detail Resume、不等待业务。
// - 业务：OnOpen/OnTraffic(含Wake)/OnClose 全部运行在受管协程；同连接
//   单事件位（active_peak==1），有界合并事件位（open/traffic/wake/close），
//   不复制无界 deque，不为每次 readiness 无界创建协程。
// - 短锁纪律：session 短锁绝不跨等待、用户回调或 owner 投递持有；
//   StreamConnection::Close 同步发布 on_closed，封口与物理关闭在锁外
//   推进，防重入死锁。
// - G3：业务命令入口先取得强状态并随 owner 命令保活（shared_from_this），
//   Submit=Accepted 后不因目标消失而悄悄 no-op；只有 owner 通知侧使用
//   weak，weak 过期可忽略。封口后的 Wake 明确拒绝（operation_canceled）；
//   Closed 后的 Close 是明确幂等物理观察。
// - G6：Block 等待的 operation 由 session owned state 持有到最后回调；
//   超时/取消只结束等待，晚到结果不写调用栈、无二次业务完成；快照归还
//   超时后由迟到的 owner 命令完成清理，不永久 lent。
// - 内部 native 管理域（控制线程 WaitDrained/owner 域 CloseNative）不是
//   公共 Co 入口，不放宽公共 Connection Block 域。

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/infra/net/CoObject.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/CoConnectionHandler.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/Options.hpp>
#include <bbt/infra/net/detail/CoIoOperation.hpp>
#include <bbt/infra/net/detail/EventLoop.hpp>
#include <bbt/infra/net/detail/EventRingBuffer.hpp>
#include <bbt/infra/net/detail/StreamConnection.hpp>

namespace bbt::infra::net {

// 桥接内部等待/投递的统一期限：事件内 owner 往返与快照归还使用。
// 固定常量而非新配置项（0010：不新增配置系统；Engine 级预算接线是
// 后续包范围）。到期只结束等待，已投递命令结果由 owned state 收口。
constexpr std::chrono::seconds kInternalOpDeadline{30};

// 投递门类别：Business=业务事件/业务命令入口（强停后拒绝新业务）；
// Internal=桥内部收口与快照归还，只受「owner 是否可达」约束。强停不等
// handler 返回，handler 稍后返回时必须仍能把借出的快照还给 owner 域。
enum class SubmitGate { Business, Internal };

// 桥接命令/观测统计（可原子任意线程读；副本随 facade 与共享状态分离）。
struct BridgeStats {
    std::atomic<std::size_t> ops_submitted{0};   // 桥接命令投递计数
    std::atomic<std::size_t> ops_executed{0};    // 命令真实结束计数
    std::atomic<std::size_t> sessions_registered{0};
};

// 桥的轻量共享控制状态：与 ConnectionBridge facade 分离，由 session/
// OperationTicket 共享持有。只含 owner 门/统计/关闭标志，不含 registry
// 与连接对象，因此脱离后 facade 可析构而本状态仍安全服务已接纳命令/
// 晚到账票与已关命令；session→state 无环，不造 session→sharedbridge→
// registry→shell→session 环。owner 仍由调用方拥有，不共享 EventLoop、
// 不在 owner 最终 release 自 join。
class ConnectionBridgeState final {
public:
    ConnectionBridgeState(EventLoop& owner, const NormalizedOptions& options)
        : owner_(&owner), options_(options) {}

    // 同一同步门内取有效 owner 并完成入队；只覆盖投递，不跨挂起等待/
    // 用户回调，绝不解锁取裸指针再 Submit。owner 不可达 → Sealed
    // （调用方映射为 operation_canceled）；Business 门另拒强停后的新业务。
    // 被拒绝时投递闭包与调用方局部票在同一栈帧内保引用，闭包不在本门内
    // 析构；若未来出现不保留局部保引用的投递，Full/Sealed 会在 m_ 内析构
    // 闭包并重入 NotifyChange，形成同线程自死锁。
    SubmitStatus Submit(std::function<void()> command,
        SubmitGate gate = SubmitGate::Business);

    EventLoop& Owner() const noexcept { return *owner_; }   // 仅 attached 期间有效
    const NormalizedOptions& Options() const noexcept { return options_; }
    bool Attached() const noexcept { std::lock_guard<std::mutex> guard(m_); return owner_attached_; }

    void OpSubmitted() noexcept { ++stats_.ops_submitted; }
    void OpExecuted() noexcept { ++stats_.ops_executed; NotifyChange(); }
    void NotifyChange() const;
    const BridgeStats& GetStats() const noexcept { return stats_; }

    // ---- 短锁 m_ 之下：绝不跨等待/用户回调/owner 投递持有 ----
    mutable std::mutex m_;
    mutable std::condition_variable cv_;
    EventLoop* owner_{nullptr};        // m_：仅 owner_attached_ 时有效
    bool owner_attached_{true};        // m_
    bool service_stopped_{false};      // m_：与业务事件开始共用线性化门
    std::atomic<bool> detached_{false}; // 脱离后无 owner 可达（无锁读）
    bool drained_at_detach_{false};     // m_：脱离前冻结的真实 DrainedLocked()

    // 命令/会话登记簿记（原子读写；session 与 facade 共用）。
    BridgeStats stats_;

private:
    const NormalizedOptions options_;
};

// 单连接私有会话：合并事件位调度 + 独立快照保管 + 桥接命令簿记。
// 生命周期：由 CoTcpConnection::Impl 持有（工厂接纳时创建）；owner
// 通知侧回调持 weak。facade（ConnectionBridge）经 registry 强持有连接
// 对象；session 只共享 ConnectionBridgeState，不持 facade 引用，故脱离
// 后 facade 析构不产生悬垂引用。
class ConnectionSession final : public std::enable_shared_from_this<ConnectionSession> {
public:
    // 观测计数快照（契约测试/审查证据；原子维护，任意线程读）。
    struct Counters {
        std::size_t spawns{0};                  // dispatcher 协程登记次数
        std::size_t open_entered{0};
        std::size_t traffic_dispatched{0};      // 含 wake 事件
        std::size_t peer_terminal_detected{0};  // 观察到对端读终态（FIN/致命IO）次数
        std::size_t wake_calls{0};              // Wake() 合法调用次数
        std::size_t wake_rejected_sealed{0};    // 封口后 Wake 明确拒绝
        std::size_t late_dropped{0};            // 封口后到达的 readiness（只计数）
        std::size_t handler_exceptions{0};      // handler 抛出次数
        std::size_t dispatch_crashes{0};        // dispatcher 本身异常（物理收口兜底）
        std::size_t onclose_entered{0};
        std::size_t onclose_returned{0};
        std::size_t op_timeouts{0};             // 桥接命令等待超时（结果未知）
        std::size_t snapshot_return_errors{0};  // 快照归还拒绝（内部记账，理论不可达）
        std::size_t close_init_submit_failures{0}; // 异常/崩溃路径物理收口投递失败
        std::size_t active_peak{0};             // 单事件位活跃业务事件峰值（必须为 1）
    };
    // 状态快照（G3/G4 测试证据；短锁下拷贝位标记）。
    struct StateSnapshot {
        std::size_t id{0};
        bool sealed{false};
        bool physical_closed{false};
        bool busy{false};
        bool snap_lent{false};
        bool close_finished{false};
        bool open_dispatched{false};
        bool snap_return_pending{false};
    };

    // 内部只读（Engine 收口判据，非 Drained）：物理已关闭且 owned 等待记录已结算。
    bool PhysicalCloseSettled() const
    {
        std::lock_guard<std::mutex> guard(mx_);
        return physical_closed_ && physical_waiters_.empty();
    }

    ConnectionSession(std::shared_ptr<ConnectionBridgeState> state,
        std::shared_ptr<ICoConnectionHandler> handler, Address peer, std::size_t id);

    std::size_t Id() const noexcept { return id_; }
    const Address& PeerAddress() const noexcept { return peer_; }
    ConnectionBridgeState& State() const noexcept { return *state_; }
    bbt::coroutine::CoObjectInfo ObjectInfo() const;

    // 工厂绑定公共连接对象（弱引用；事件派发时取强引用）。
    void BindShell(const std::shared_ptr<CoTcpConnection>& shell);

    // ---- 工厂接纳（owner 域，Adopt 内调用）----
    // 绑定物理连接并登记首个 OnOpen 事件位。回调在绑定前不会触发
    // （async_wait 只登记，readiness 在后续 owner 循环发布）。
    void AdoptOwner(std::shared_ptr<StreamConnection> connection);
    // 生成 StreamConnection 短通知回调（owner 通知侧 weak，过期可忽略）。
    StreamConnection::Callbacks MakeCallbacks();

    // ---- owner 域（StreamConnection 回调与桥接命令）----
    // on_traffic：owner 域只「记录」读终态（对端 FIN / 致命 IO），绝不在业务事件
    // 返回前物理收口（会丢尾帧/未发响应）。收口由 DispatchLoop 在 RunEvent 返回后
    // 经 CloseOnPeerTerminal 提交一次。
    void OnTrafficOwner();
    void OnDrainedOwner();
    void OnClosedOwner(const std::error_code& reason);
    // 封口 + 物理关闭一次推进（Block 的 start 与 Unblock/CloseNative/
    // CloseNative 全部共用；on_closed 同步发布时重入安全）。
    std::error_code SealAndPhysicalCloseOwner(std::error_code reason);
    // 强停只撤网络服务与未开始事件，不销毁运行中 handler 的快照。
    std::error_code StopServiceOwner();
    // 脱离专用（owner 循环已 join、非 owner 域）：owner 域 backend（socket/
    // EventLoop 依赖）的强引用移出后在短锁外析构，使 owner context 可以先于
    // 残留 shell 销毁；未物理关闭时拒绝，不拆活 socket。
    std::error_code ReleaseOwnerBackend();
    // 测试/审查证据通道：owner 域 backend 弱观察（不延长寿命、不触 owner）。
    std::weak_ptr<StreamConnection> BackendProbe() const;

    // ---- 业务命令（受管协程；CoTcpConnection 转发，副作用前已 RequireCoroutine）----
    std::error_code Wake();
    std::error_code Write(std::string data, CoDeadline deadline);
    std::error_code Close(CloseMode mode, std::error_code reason, CoDeadline deadline);

private:
    std::error_code CloseBlock(std::error_code reason, CoDeadline deadline);
    std::error_code CloseUnblock(std::error_code reason);
    void WakeArriveOwner();

public:

    // ---- 事件内 Reader（仅当前事件协程；CoTcpConnection 转发）----
    std::size_t InboundBuffered() const;
    std::string_view Peek() const;
    std::error_code Discard(std::size_t bytes);

    // ---- 观测 ----
    Counters GetCounters() const;
    StateSnapshot SnapshotState() const;
    // owner-only：透传物理连接 Stats（契约测试在 owner 域取样）。
    StreamConnection::Stats ConnStatsOwner() const;
    // drain 谓词（控制线程 native 管理域评估；短锁拷贝判断）：
    // 物理已关闭 && OnClose 已返回 && 无待处理事件位 && 无借出快照。
    bool DrainedSnapshot() const;

private:
    // dispatcher 事件种类（优先级：Open → Traffic/Wake → Close 终态）。
    enum class EventKind { Open, Traffic, Close };

    // 受管协程内：单事件位循环；同连接活跃业务事件峰值 1。
    void DispatchLoop(const std::shared_ptr<CoTcpConnection>& shell);
    void RunEvent(const std::shared_ptr<CoTcpConnection>& shell, EventKind kind,
        bool wake, const std::error_code& reason);
    // 事件开始前取得当前事件独立快照（owner 往返；wake 事件不借出）。
    void PrepareEventSnapshot(bool wake);
    // 事件结束自动归还：未消费尾部保序；超时后由迟到 owner 命令收口。
    void AutoReturnSnapshot();
    // Take 的交付与放弃在 mx_ 下首胜；未归还的资源始终留在会话内。
    struct SnapshotTransfer {
        EventSnapshot snapshot;
        bool ready{false};
        bool abandoned{false};
        bool lent{false};
        bool cleanup_claimed{false};
    };
    void ReturnAbandonedSnapshotOwner(const std::shared_ptr<SnapshotTransfer>& transfer);
    struct OperationTicket {
        std::shared_ptr<ConnectionSession> session;
        explicit OperationTicket(std::shared_ptr<ConnectionSession> value);
        ~OperationTicket();
    };
    CoIoResult RunOperation(CoDeadline deadline, CoIoOperation::Start start,
        SubmitGate gate = SubmitGate::Business);
    SubmitStatus SubmitOperation(std::function<void()> command,
        SubmitGate gate = SubmitGate::Business);
    // SpawnDispatcher 复register兜底：观测 + busy 复位 + 物理收口提交。
    void OnDispatchCrash();
    // 读终态收口：每次事件后至多两次尝试；Accepted 才把收口责任移交 owner。
    void CloseOnPeerTerminal();
    void SpawnDispatcher();
    // 事件门禁：当前受管协程必须是本连接活跃事件协程（每次现查）。
    std::error_code RequireEventContext() const;
    static bbt::coroutine::WaitOptions WaitOpts(CoDeadline deadline);
    static std::error_code MapSubmitError(SubmitStatus status);

    std::shared_ptr<ConnectionBridgeState> state_;
    std::shared_ptr<ICoConnectionHandler> handler_;
    const Address peer_;
    const std::size_t id_;
    const CoObject identity_;                       // 组合既有 CoObject 身份
    std::weak_ptr<CoTcpConnection> shell_;          // 事件派发时取强引用
    std::shared_ptr<StreamConnection> conn_;        // owner 域绑定

    // ---- 短锁 mx_ 之下：绝不跨等待/用户回调/owner 投递持有 ----
    mutable std::mutex mx_;
    // 有界合并事件位（固定位，无 unbounded deque）：
    bool pending_open_{false};
    bool open_dispatched_{false};
    bool pending_traffic_{false};
    bool pending_wake_{false};
    bool close_event_pending_{false};               // on_closed 已发布，OnClose 待执行（sticky）
    // 读终态（对端 FIN/RST）：owner 域 on_traffic 只记录，收口在业务事件返回后提交一次。
    bool eof_terminal_{false};                      // mx_：owner 已观察到读终态
    std::error_code eof_reason_{};                   // mx_：空=干净 FIN；非空=RST/errno
    enum class PeerCloseSubmission { Idle, Submitting, Accepted };
    PeerCloseSubmission eof_close_submission_{PeerCloseSubmission::Idle}; // mx_：尝试≠责任移交
    bool sealed_{false};                            // 首胜封口
    bool service_stopped_{false};                   // mx_：强停后不再派发业务
    bool close_started_{false};                     // 物理关闭已发起（Unblock 失败回滚判据）
    bool physical_closed_{false};
    std::error_code sealed_reason_;
    bool busy_{false};                              // 至多一个 dispatcher
    bool close_finished_{false};                    // OnClose handler 已返回/抛出
    // 事件内快照：session 自有 storage，不存 handler 栈指针。
    EventSnapshot snap_;
    std::shared_ptr<SnapshotTransfer> pending_take_; // 迟到 Take/归还完成前阻止 drain
    bool snap_lent_{false};
    bool snap_return_pending_{false};               // 归还命令已投递未确认
    std::shared_ptr<EventSnapshot> pending_return_; // 拒绝/异常时也不销毁借出资源
    std::size_t snap_taken_{0};
    // 当前事件协程（Reader 门禁；每次现查，不缓存线程 TLS）。
    bool event_running_{false};
    std::uint64_t event_coro_{0};

    // ---- owner 域专用（Block 物理等待；由 owned state 持有到最后回调，G6）----
    std::vector<std::shared_ptr<CoIoOperation>> physical_waiters_;

    // ---- 观测（原子）----
    std::atomic<std::size_t> spawns_{0};
    std::atomic<std::size_t> open_entered_{0};
    std::atomic<std::size_t> traffic_dispatched_{0};
    std::atomic<std::size_t> peer_terminal_detected_{0};
    std::atomic<std::size_t> wake_calls_{0};
    std::atomic<std::size_t> wake_rejected_sealed_{0};
    std::atomic<std::size_t> late_dropped_{0};
    std::atomic<std::size_t> handler_exceptions_{0};
    std::atomic<std::size_t> dispatch_crashes_{0};
    std::atomic<std::size_t> onclose_entered_{0};
    std::atomic<std::size_t> onclose_returned_{0};
    std::atomic<std::size_t> op_timeouts_{0};
    std::atomic<std::size_t> snapshot_return_errors_{0};
    std::atomic<std::size_t> close_init_submit_failures_{0};
    std::atomic<int> active_{0};
    std::atomic<int> active_peak_{0};
};

// 私有连接桥：未来 Engine 的创建/drain 接缝（本包交付的最小内部形态）。
// registry 强持有连接对象（Engine 拥有连接的语义）。两条收尾路径都成立：
// (a) 真实 drain → RequestStop → Join → 释放桥，连带释放连接与物理 backend；
// (b) 强停 StopServiceOwner（物理全闭）→ RequestStop → Join → DetachOwner
//     （在 owner context 仍活时释放每会话 backend 与 registry 强持有），
//     此后 context/facade 可自然销毁，外部 shell/活动 handler 继续合法收尾。
class ConnectionBridge final : public std::enable_shared_from_this<ConnectionBridge> {
public:
    // 控制线程创建（owner 需已 Start 由使用者保证；不在此探测）。
    static std::shared_ptr<ConnectionBridge> Create(EventLoop& owner,
        const NormalizedOptions& options, std::error_code& error);

    // owner 域接纳：由 owner 命令调用（真实 TCP 测试在业务协程内经既有
    // CoIoOperation 等待接纳完成）。输入 fd 唯一所有权；接纳前校验失败
    // 时所有权原样退回 NativeSocket，不静默丢 fd。返回的连接对象已登记
    // 并将派发 OnOpen。
    std::shared_ptr<CoTcpConnection> Adopt(NativeSocket&& socket, Address peer,
        std::shared_ptr<ICoConnectionHandler> handler, std::error_code& error);

    // owner 域 native 物理关闭入口（内部管理面，非公共 Co 入口）：
    // 封口 + 物理关闭一次推进。未知 id → invalid_argument；已物理关闭
    // → 幂等成功。
    std::error_code CloseNative(std::size_t connection_id, std::error_code reason);

    // 控制线程短锁封门：只拒绝尚未获得开始授权的业务，不碰 registry/socket，
    // 不等待 handler；已授权的工作由 owner 完成或回滚。物理关闭仍须另行推进。
    void SealService();

    // owner 线性封口接纳/业务，再逐连接撤 I/O 并物理关闭；不等待 handler。
    // owner/bridge 仍由调用方保活。未开始 OnClose 可以压掉，不构成 drain。
    // 单连接失败不阻断其余关闭；本轮返回错误，重复调用继续推进未完成项。
    std::error_code StopServiceOwner();

    // 内部只读（Engine 收口判据）：每会话物理已关闭且 owned 等待记录已结算。
    // 不是 Drained：不等 Handler/OnClose/快照/桥接命令全部结束。
    bool NetworkCloseReady() const;

    // owner 安全脱离（网络完成点，调用顺序是契约的一部分）：
    //   1) owner 域 StopServiceOwner() 物理关闭全部连接（不等 handler）；
    //   2) 控制线程 RequestStop() + Join()：全部 owner/asio 回调完成；
    //   3) 本调用在 owner 的 io_context 仍存活时逐会话断开 owner 域 backend
    //      （socket/EventLoop 依赖），再释放 registry 强持有。
    // 前置未满足（存在未物理关闭的会话）→ 不发布 detached、不清 registry、
    // 不释放 backend，返回拒绝错误，由调用方推进收口后重试（不做半脱离）。
    // 成功后 facade 与自有 owner/context 均可自然销毁，外部 shell/活动
    // handler 继续合法收尾（残留命令面只触会话自有状态）。
    // 幂等；不等 handler/drain，不销毁运行中 handler 的快照。
    std::error_code DetachOwner();

    // 控制线程完成点（native 管理域，不挂起协程）：等待全部会话
    // 「handler 事件/快照/桥接命令均已结束」——每会话物理已关闭、
    // OnClose 已返回、无待处理事件位、无借出快照，且全桥无未执行完的
    // 桥接命令。无成功 OnClose 执行时不报成功 drain；依赖仍打开的会话
    // 继续保活（超时返回 false，不标 drained）。 drained 后保持 drained。
    // DetachOwner 之后不再重估：沿用脱离前冻结的真实结果（已证 drained 或
    // 保守 false），不把强停压掉 OnClose 的会话报成 drained。
    bool WaitDrained(CoDeadline deadline) const;
    bool Drained() const noexcept;

    // ---- 观测 ----
    using Stats = BridgeStats;
    const Stats& GetStats() const noexcept { return state_->GetStats(); }
    std::size_t SessionCount() const;
    // LC 内部采样：登记但尚未物理关闭；不改变 registry 保活/业务计数。
    std::size_t LiveSessionCount() const;
    // 测试/审查证据通道：按 id 取会话（registry 强持有 ⇒ 返回非空）。
    std::shared_ptr<ConnectionSession> SessionOf(std::size_t connection_id) const;
    EventLoop& Owner() noexcept { return state_->Owner(); }
    const NormalizedOptions& Options() const noexcept { return state_->Options(); }

private:
    friend class ConnectionSession;
    ConnectionBridge(EventLoop& owner, const NormalizedOptions& options);

    bool DrainedLocked() const;

    void NotifyChange() const { state_->NotifyChange(); }

    std::shared_ptr<ConnectionBridgeState> state_; // 轻量共享控制状态（含 m_/cv_/stats_/owner 门）
    std::map<std::size_t, std::shared_ptr<CoTcpConnection>> registry_; // 登记写入/查询受 state_->m_ 保护；Stop 在 owner 内锁外只读遍历，Detach 仅 Join 后
    std::size_t next_id_{0};
};

} // namespace bbt::infra::net
