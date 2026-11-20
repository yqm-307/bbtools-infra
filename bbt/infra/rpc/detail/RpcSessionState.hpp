#pragma once

// 会话私有状态：单 link、共享连接的 owner 和服务端接纳各持一份台账。
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/ReclaimReceipt.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/rpc/Admission.hpp>
#include <bbt/infra/rpc/CoRpcServer.hpp>
#include <bbt/infra/rpc/Hooks.hpp>
#include <bbt/infra/rpc/Options.hpp>
#include <bbt/infra/rpc/SessionCodec.hpp>
#include <bbt/infra/rpc/Stats.hpp>
#include <bbt/infra/rpc/detail/RpcChannel.hpp>
#include <bbt/infra/rpc/detail/Tcp.hpp>

namespace bbt::infra::rpc {

struct SessionCore;
struct SessionOwnerState;
struct ServerState;

/**
 * 内部期限统一使用单调时钟
 */
using RpcClock = std::chrono::steady_clock;

/**
 * 单 link 的 lane 数与 TrafficClass → lane 下标（出站调度真源）。
 */
inline constexpr std::size_t kLaneCount = 3;

inline std::size_t LaneIndex(TrafficClass traffic) noexcept
{
    switch (traffic) {
    case TrafficClass::System:
        return 0;
    case TrafficClass::Service:
        return 1;
    case TrafficClass::Actor:
        return 2;
    }
    return 1;
}

inline Error Err(ErrorCode code, std::string message)
{
    return MakeError(code, "rpc session: " + std::move(message));
}

inline result<void> ErrV(ErrorCode code, std::string message)
{
    return result<void>::err(Err(code, std::move(message)));
}

inline Error ErrPhase(ErrorCode code, std::string message, RequestPhase phase)
{
    auto error = Err(code, std::move(message));
    error.request_phase = phase;
    return error;
}

template <class Table, class Pending>
bool ErasePending(Table& table, const std::string& key, const std::shared_ptr<Pending>& operation)
{
    const auto it = table.find(key);
    if (it != table.end() && it->second == operation) {
        table.erase(it);
        return true;
    }
    return false;
}

Error WaitFailure(bbt::coroutine::WaitStatus status, const char* message);

/**
 * Close 使用默认 Closed 原因补齐空消息。
 */
Error SettleCloseReason(Error reason, const char* default_message);

/**
 * 受管协程内检查点：域非法/取消/期限到。
 */
std::error_code CheckCall(bbt::coroutine::Deadline deadline);

/**
 * deadline → wire remaining_budget_ms（[1, kRpcV2MaxRemainingBudgetMs]）。
 */
result<std::uint32_t> Budget(bbt::coroutine::Deadline deadline);

/**
 * 关闭原因作为可观察详情附加：保留既有失败分类（未提交 Closed /
 * 已提交 OutcomeUnknown+request_phase），只增加原因 code/message。
 */
void AttachCloseReason(Error& error, const Error& reason);

/**
 * SessionCore：单 link 的会话状态——收发缓存、拆包、pending、顺序与关闭。
 */
struct SessionCore final : std::enable_shared_from_this<SessionCore> {
    std::shared_ptr<SessionOwnerState> owner;

    /**
     * 入站接纳侧
     */
    std::weak_ptr<ServerState> server;

    /**
     * 无状态字节转发：Read/Write/物理关闭
     */
    std::shared_ptr<detail::TcpChannel> channel;
    std::string peer_identity;

    bool outbound{false};
    std::uint64_t epoch{0};
    std::atomic<bool> dead{false};

    /**
     * 发布 link 前登记连接和任务安装责任；reader/writer 各持一份退出责任。
     * Kill 结算连接责任，任务安装和退出分别结算。责任归零且 conn Released
     * 后才归还额度；session.dead 只是逻辑死亡。
     */
    std::atomic<int> responsibilities{0};

    // 入队和等待共用同一操作对象。认领与撤回均在 pending_mu 下裁定。
    struct PendingWrite {
        enum class Phase { Queued, Committed, Withdrawn };
        Phase phase{Phase::Queued};
        bool done{false};
        std::shared_ptr<bbt::coroutine::sync::CoWaiter> waiter = bbt::coroutine::sync::CoWaiter::Create();
    };

    struct OutItem {
        std::string bytes;
        TrafficClass lane{TrafficClass::Service};

        std::string key;
        std::shared_ptr<PendingWrite> pending;
        bbt::coroutine::Deadline deadline{RpcClock::time_point::max()};

        /**
         * 回复写终态凭据：Complete/Fail 挂 waiter，WriterLoop/Kill 在到达
         * 真实写终态（写出/写失败/关闭丢弃）后写 outcome 并 Notify——共享
         * 对象不随 item move 失效，观察晚于写完成时仍能取回首个结果。
         */
        class Settle {
        public:
            void Publish(result<void> outcome);
            result<void> Wait(bbt::coroutine::Deadline deadline);

        private:
            std::mutex m_mutex;
            std::optional<result<void>> m_outcome;
            std::shared_ptr<bbt::coroutine::sync::CoWaiter> m_waiter = bbt::coroutine::sync::CoWaiter::Create();
        };

        std::shared_ptr<Settle> settle;
    };

private:
    mutable std::mutex out_mu;
    std::deque<OutItem> out_q[kLaneCount];

    std::shared_ptr<bbt::coroutine::sync::CoWaiter> out_signal = bbt::coroutine::sync::CoWaiter::Create();
    bool out_sealed{false};

    /**
     * 出站 lane 统计（queued/dequeued/dropped/bytes_queued 与 out_q
     * 同享 out_mu——读写同锁，stats 面不再有第二把锁的竞态）。
     */
    TrafficStats out_lanes[kLaneCount];

    std::uint32_t rr_credit[kLaneCount] = {0, 0, 0};

public:
    /**
     * 排队中 + writer 正在写出的总条数（Drain 持有队列/写责任直到真实
     * 终态）：Enqueue+1、WriterLoop 结算-1、Kill 对丢弃项逐条结算。
     */
    std::atomic<std::uint64_t> writes_outstanding{0};

    struct PendingCall : PendingWrite {
        result<Response> outcome = result<Response>::err(Err(ErrorCode::InternalError, "unset"));
        std::string service;
        std::string method;
        std::string request_schema;
        std::string response_schema;
    };

    struct PendingSend : PendingWrite {
        result<void> outcome = result<void>::err(Err(ErrorCode::InternalError, "unset"));
    };

    mutable std::mutex pending_mu;
    std::unordered_map<std::string, std::shared_ptr<PendingCall>> calls;
    std::unordered_map<std::string, std::shared_ptr<PendingSend>> sends;

    /**
     * ---- reader：接收缓存与拆包 decoder 都是 session 状态，不在协议层 --------
     */

private:
    SessionFrameDecoder decoder{kRpcV2DefaultMaxFrameBytes};

    /**
     * ---- 入站顺序台账（仅唯一 reader 协程访问，不加锁）------------------------
     * per-key 已接纳的最大 sequence（首帧即台账基值）；跨 lane 同一 key 同样
     * 约束——ledger 描述 wire 到达序，不描述 lane 调度序。重复/逆序/缺口在
     * DispatchInbound 拒绝（RejectProtocol），不与 ADR 的「同 key 递增」
     * 契约混放。
     */
    std::unordered_map<std::string, std::uint64_t> inbound_sequence;

public:
    // 只在启动 reader 前移交握手 decoder。
    void TakeHandshakeDecoder(SessionFrameDecoder&& handshake_decoder);
    void NoteOperationStarted(TrafficClass traffic);
    void NoteEnqueueFailed(TrafficClass traffic);
    void NoteOperationFinished(TrafficClass traffic, bool published);
    void AccumulateOutboundStats(ClientStats& stats) const;
    bbt::coroutine::WaitStatus WaitPending(const PendingWrite& pending, bbt::coroutine::Deadline deadline);

    bool Submit(std::size_t stack, std::function<void()> fn);

    /**
     * 唯一「失败在途」入口：幂等（dead.exchange），丢弃出站队列、关闭物理
     * 连接、结算全部未终结 pending。关闭原因不在此附加——统一由 Call/Send
     * 调用方可见的返回点读取 owner 原因后附加。
     */
    void Kill(std::error_code reason);

    /**
     * 一份退出责任结算（reader/writer/物理连接）。返回到达 0：唯一归零
     * 点负责唤醒 server/owner 收口观察。
     */
    bool ExitDone();

    result<void> Enqueue(OutItem item);

    /**
     * 持 out_mu 且 lane 非空；移出队列的唯一记账点，写责任随返回值交接。
     */
    OutItem TakeQueuedLocked(std::size_t lane);

    /**
     * 单条 OutItem 唯一结算点：发布 settle 终态（若有凭据）+ 写责任计数
     * 归零观察；outcome 是该条目的真实终态。
     */
    void NoteWriteSettled(OutItem& item, result<void> outcome);

    /**
     * 按操作对象结算本条写失败。ID 可在 Call 与 Send 之间重名。
     */
    void FailItemPending(const OutItem& item, const Error& error);
    void WriterLoop();
    void ReaderLoop();
    void FailPendingOnLinkLoss();
    void DispatchInbound(const SessionFrame& frame);
    void AdmitInbound(const SessionFrame& frame, Request request);
    void PostSessionError(const Error& error);

    /**
     * 协议性终止：先尽力交付一帧 SessionError（系统 lane，有界等待写终态），
     * 再退役 link；避免对端只看到 TCP close 而不知道违规原因。
     */
    void RejectProtocol(const std::string& message);
    bool Dequeue(OutItem& out);
};

// 路由索引与物理租约分离：任务持有 SessionCore，台账不反向强持有它。
struct LinkLease {
    std::weak_ptr<SessionCore> session;
    net::Connection conn;

    bool Released() const;
};

/**
 * SessionOwnerState：会话 owner 台账（多 session 复用、拨号中连接与关闭）。
 */
struct SessionOwnerState {
    std::shared_ptr<net::Engine> engine;
    ClientOptions options;

    /**
     * owner 级受控检查点（bbt/infra/rpc/Hooks.hpp）：同 owner 的 link 共用
     * 一份，不建全局态。由库分配，对外只给 owning handle——handle 可以比
     * owner 活得久，owner 收口后只读到 closed。
     */
    std::shared_ptr<SessionHooks> hooks{new SessionHooks()};
    mutable std::mutex mu;

    /**
     * Graceful 设置 draining，发布首因后拒新并等 active_ops 归零。
     * Immediate 仍可在排空期间强制 Seal，让在途调用取得终态。
     */
    bool closed{false};
    bool draining{false};

    /**
     * 首次关闭时发布，升级为 Immediate 时保留。
     */
    std::optional<Error> close_reason;

    /**
     * Close 等待唤醒：EndOperation / Seal / ReleasedLocked / 建立收尾统一
     * 经它 Notify；调用方在 owner->mu 外循环复查谓词（单等待位 + 周期复查，
     * 不承诺实时推送到每个观察者）。
     */
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> close_signal = bbt::coroutine::sync::CoWaiter::Create();

    /**
     * 已接纳 Call/Send 责任计数：与 closed/draining 同门（mu）接纳/归还，
     * Graceful Close 等它归零，不依赖连接物理 Released。
     */
    std::uint64_t active_ops{0};

    /**
     * 当前持有本 owner 已接纳操作责任的协程 id（含同协程嵌套调用的计数）：
     * Close 自等待拒绝用，不依赖线程 TLS，协程跨 worker 恢复后仍能识别。
     */
    std::unordered_map<bbt::coroutine::detail::CoroutineId, std::uint32_t> operation_tasks;
    std::unordered_map<std::string, std::weak_ptr<SessionCore>> by_endpoint;
    std::unordered_map<std::string, std::weak_ptr<SessionCore>> by_identity;
    std::vector<LinkLease> link_leases;

    struct Establishing {
        struct Completion {
            std::optional<result<std::shared_ptr<SessionCore>>> outcome;
        };

        /* 只有拨号方和已加入的等待方持有结果；回收台账不能经赢家反向持有 owner。 */
        std::weak_ptr<Completion> completion;
        bool done{false};

        // 安装接管额度；未安装则保留到建立任务结束且 receipt 已 Released。
        bool reserved{true};
        net::Connection conn;
        net::ReclaimReceipt reclaim;
    };

    std::unordered_map<std::string, std::shared_ptr<Establishing>> establishing;
    std::weak_ptr<ServerState> inbound_server;
    std::uint64_t epoch_counter{0};
    std::atomic<std::uint64_t> next_attempt_id{1};
    std::mt19937_64 random{std::random_device{}()};

    std::atomic<std::uint64_t> acceptance_pending{0};

    /**
     * 逻辑排空谓词（Graceful）：无在途调用且无未完结的拨号/握手。
     * 持 owner->mu 调用；实现见 detail/RpcSessionState.cc。
     */
    bool DrainedLocked() const;

    /**
     * wait_release 的完成条件是全部 link 映射与建立中记录都已归还。
     * 持 owner->mu 调用；实现见 detail/RpcSessionState.cc。
     */
    bool TeardownLocked() const;
    void ReapReleasedLocked();
    std::size_t OccupiedLocked() const;

    ~SessionOwnerState();
};

/**
 * Admission 不透明实现（公共类型见 Admission.hpp；impl 只归 Session 侧）。
 */
namespace detail {

struct ReplyCompletionImpl {
    std::weak_ptr<SessionCore> session;
    std::weak_ptr<ServerState> server;

    /**
     * Response | SendAccepted
     */
    FrameKind kind{FrameKind::Response};
    TrafficClass lane{TrafficClass::Service};
    std::string request_id;
    std::string service;
    std::string method;
    std::string request_schema;
    std::string response_schema;

    /**
     * SendAccepted only
     */
    std::string message_id;

    /**
     * 回复写终态的有限等待预算：取入站 RequestContext.deadline
     * （min(wire remaining budget, 本地 handler 预算)）；max() 时调用方
     * 仍按 30s 上限封顶。入队与等待共用同一预算，不另起计时。
     */
    bbt::coroutine::Deadline reply_deadline{bbt::coroutine::Deadline::max()};
    std::atomic<bool> terminal{false};
};

struct AdmissionLeaseImpl {
    std::weak_ptr<ServerState> server;
    TrafficClass lane{TrafficClass::Service};
    std::atomic<bool> released{false};
};

struct AdmissionQueueImpl final : std::enable_shared_from_this<AdmissionQueueImpl> {
    std::size_t max_items;
    std::size_t max_bytes;
    AdmissionHandler handler;
    mutable std::mutex mu;
    std::deque<AdmissionItem> queue;
    std::uint64_t bytes{0};
    bool closed{false};
    bool working{false};
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> signal = bbt::coroutine::sync::CoWaiter::Create();

    /**
     * per-lane 快照
     */
    std::uint64_t queued_items[kLaneCount] = {0, 0, 0};
    std::uint64_t queued_bytes[kLaneCount] = {0, 0, 0};
    std::weak_ptr<ServerState> server;

    /**
     * owning request 的实际计费字节——payload 之外含全部字符串字段、
     * metadata 键值对与 ordering_key；request/reply/lease 本身的固定开销
     * 用 64 近似（不再仅 payload+64 低估）。实现见 detail/RpcSessionState.cc。
     */
    static std::size_t ItemBytes(const AdmissionItem& item);
};

// clang-format off
}
// clang-format on

/**
 * ServerState：listener/admission/路由/令牌桶与收口台账。
 */
struct ServerState final : std::enable_shared_from_this<ServerState> {
    struct Route {
        CoRpcServer::RpcHandler handler;
        CoRpcServer::RouteOptions options;
        double tokens{0.0};
        RpcClock::time_point last_refill{RpcClock::now()};
    };

    std::shared_ptr<net::Engine> engine;
    std::shared_ptr<SessionOwnerState> owner;
    AdmissionHandler handler;
    AdmissionHandler framework_handler;
    ServerOptions options;
    mutable std::mutex mu;

    /**
     * Close 等待唤醒：收口观察点统一经它 Notify，调用方在 mu 外复查谓词。
     */
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> close_signal = bbt::coroutine::sync::CoWaiter::Create();

    /**
     * starting：Start 已认领启动权、accept 任务尚未登记（队列创建与协程
     * 任务注册都可能因栈池未命中在 Co 域挂起）。该窗口内 Close/WaitStopped
     * 照常可进；收口谓词必须把它当未落账责任，不能按「无 started 任务」
     * 提前判定已物理收口。
     */
    bool starting{false};
    bool started{false};
    bool stopping{false};
    bool closed{false};

    /**
     * Open → Draining → Closed：draining 只由 Graceful Close 置位（拒新），
     * Close 经 closed 表达；两位不共用。
     */
    bool draining{false};

    /**
     * 首次关闭时发布，升级为 Immediate 时保留。
     */
    std::optional<Error> close_reason;
    bool queue_started{false};
    std::shared_ptr<AdmissionQueue> queue;
    std::unordered_map<std::string, Route> routes;
    std::uint64_t accepted{0};
    std::uint64_t callbacks{0};

    /**
     * 当前执行本 Server 已授权 handler 的协程 id：Close/WaitStopped 自等待
     * 拒绝用；由路由任务在调用 handler 前后登记/移除。
     */
    std::unordered_set<bbt::coroutine::detail::CoroutineId> responsibility_tasks;

    /**
     * 已交 admission、尚未到回复写终态的 reply 数：Graceful 排空同时等它归零，
     * 防止「admission lease 已归还、回复仍在写」被误判为排空。
     */
    std::uint64_t open_replies{0};
    std::uint64_t inflight[kLaneCount] = {0, 0, 0};
    std::uint64_t finished[kLaneCount] = {0, 0, 0};
    std::uint64_t dropped[kLaneCount] = {0, 0, 0};
    std::vector<LinkLease> links;

    struct Handshake {
        net::Connection conn;
        bool finished{false};
    };

    // conn 为空表示已由 links 接管；未安装时握手退出后仍保留到 Released。
    std::unordered_map<std::uint64_t, Handshake> handshakes;
    // 超额或封口竞态下，至多多持有一条已交付但未接纳的连接。
    net::Connection rejected_accept;
    void ReapReleasedLocked();

    /**
     * AcceptLoop 协程退出标记：accept 循环本身也是一份退出责任——它可能
     * 刚 accept 出 conn、尚未登记 handshakes，谓词不能以「台账空」放行。
     */
    bool accept_loop_exited{false};

    /**
     * 物理收口谓词在 server->mu 下评估；queue 计数经 AdmissionQueue 观察面
     * （自身 impl->mu）同步——锁序统一为 server->mu → impl->mu 单向。
     * 实现见 detail/RpcSessionState.cc。
     */
    bool DrainedLocked() const;

    /**
     * 物理收口谓词：逻辑 dead 不是完成凭据——reader/writer 协程责任全部
     * 退出（ExitDone）且 conn 真实 Released（fd 关 + owner 责任清）才计；
     * 建立中握手 conn 同样要 Released。accept 循环未退出前不算收口——
     * 从未 Start 时 accept_loop_exited 语义为「无责任」按已退出计。
     * 实现见 detail/RpcSessionState.cc。
     */
    bool StoppedLocked() const;

    /**
     * lane 责任计数增减（持 server->mu）；close_signal 有值时通知收口观察。
     * 实现见 detail/RpcSessionState.cc。
     */
    void NoteInflight(std::size_t lane, long delta);
};

/**
 * 库内部固定异常标签：ThrowOnce 只抛这个类型，不接受用户异常对象或消息。
 * 调用点都在库内，由 RunHookCheckpoint 就地转成可见错误。
 */
struct HookInjectedFailure final {};

/**
 * 检查点退出谓词使用的借用视图：指针指向调用方当前持有的共享对象，
 * 只在本函数调用期间有效。
 */
struct HookGate {
    const SessionOwnerState* owner{nullptr};
    const ServerState* server{nullptr};
    const SessionCore* session{nullptr};
    const detail::ICoChannel* channel{nullptr};
    bbt::coroutine::Deadline deadline{bbt::coroutine::Deadline::max()};
};

/**
 * 检查点与跨 TU 接缝声明（实现见对应 .cc；外部接缝仍走 detail/Session.hpp）。
 */
namespace detail {

/**
 * @brief 固定动作检查点的实现接缝（detail/SessionHooks.cc 定义）。
 * 只做三件事：未注册直接通过；Pause 在库自己的 CoWaiter 上分片等待；
 * ThrowOnce 在锁内唯一认领并抛出内部标签。不执行用户代码。
 */
result<void> RunHookCheckpoint(SessionHooks& hooks, HookPoint point, const HookGate& gate);

class SessionHookAccess {
public:
    static result<void> Checkpoint(SessionHooks& hooks, HookPoint point, const HookGate& gate);

    /**
     * @brief owner 封口/销毁：永久 closed 并解除全部注册，不等在途退出。
     */
    static void Close(SessionHooks& hooks) noexcept;

private:
    static void Leave(SessionHooks& hooks, std::size_t index) noexcept;

    struct Flight {
        SessionHooks& hooks;
        std::size_t index;

        ~Flight() { Leave(hooks, index); }
    };

    static result<void> PauseUntil(
        SessionHooks& hooks,
        std::size_t index,
        std::uint64_t generation,
        bbt::coroutine::Deadline expires,
        const HookGate& gate);
};

/**
 * @brief 路由分发：注册路由 + 令牌桶，未注册回落
 * framework admission handler。CoRpcServer::Create 的 handler lambda 转调。
 */
result<void> DispatchAdmission(const std::shared_ptr<ServerState>& server, AdmissionItem item);

/**
 * @brief 入站 link 的握手与安装。
 * AcceptLoop 每接纳一条连接起一个本任务；channel 寿命随任务持有。
 */
void ServeInbound(std::shared_ptr<ServerState> server, detail::TcpChannel channel);

// clang-format off
}
// clang-format on

// clang-format off
}
// clang-format on
