#pragma once
// RPC v2 会话层状态（bbt/infra/rpc 实现 TU 专用，非公开头）。
//
// 这里是「Session 状态收敛」的真源：单 link 的收发缓存、拆包 decoder、
// pending、握手进度凭据、lane 顺序与关闭状态全部属于 SessionCore；连接
// 复用台账属于 SessionOwnerState；listener/admission/路由台账属于
// ServerV2State。Channel（detail/RpcChannel.hpp）与帧协议
// （detail/RpcFrameProtocol.hpp）无状态，不持有这里任何一项。
//
// 本头同时是 RPC 实现 TU 的共享基座：跨 TU 的小常量、错误助手与协程
// 上下文检查放在文件头部；文件拆分见 detail/RpcSession.cc（link 实现）、
// detail/AdmissionImpl.cc（admission 公共类型）、
// detail/SessionHooks.cc（检查点）与 Session.cc（owner/server 生命周期）。

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
struct ServerV2State;

// ============================================================================
// 实现 TU 共享的词法助手（原 Session.cc 匿名段上移，跨 TU 复用）
// ============================================================================

using RpcClock = std::chrono::steady_clock;

/// 单 link 的 lane 数与 TrafficClass → lane 下标（出站调度真源）。
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

/// Close/Shutdown 的默认原因补齐：message 为空的 reason 换成默认 Closed。
inline Error SettleCloseReason(Error reason, const char* default_message)
{
    if (reason.message.empty()) {
        return MakeError(ErrorCode::Closed, default_message);
    }
    return reason;
}

inline bool NativeHost()
{
    return bbt::coroutine::GetLocalCoroutineId() == 0
           && bbt::coroutine::detail::Processer::TryGetLocalProcesser() == nullptr;
}

/// 受管协程内检查点：域非法/取消/期限到。
inline std::error_code CheckCall(bbt::coroutine::Deadline deadline)
{
    const auto current = g_bbt_tls_coroutine_co;
    if (!current) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    if (current->IsCancelRequested()) {
        return std::make_error_code(std::errc::operation_canceled);
    }
    if (RpcClock::now() >= deadline) {
        return std::make_error_code(std::errc::timed_out);
    }
    return {};
}

inline Error CallError(std::error_code cause, const char* message, bool committed)
{
    auto error = detail::TcpCallError(cause, message, committed);
    if (!error.request_phase.has_value() && committed) {
        error.request_phase = RequestPhase::RequestCommitted;
    }
    return error;
}

/// deadline → wire remaining_budget_ms（[1, kRpcV2MaxRemainingBudgetMs]）。
inline result<std::uint32_t> Budget(bbt::coroutine::Deadline deadline)
{
    const auto now = RpcClock::now();
    if (deadline <= now) {
        return result<std::uint32_t>::err(Err(ErrorCode::TimedOut, "deadline already expired"));
    }
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count();
    if (remaining < 1) {
        remaining = 1;
    }
    if (remaining > kRpcV2MaxRemainingBudgetMs) {
        remaining = kRpcV2MaxRemainingBudgetMs;
    }
    return result<std::uint32_t>::ok(static_cast<std::uint32_t>(remaining));
}

/// 关闭原因作为可观察详情附加：保留既有失败分类（未提交 Closed /
/// 已提交 OutcomeUnknown+request_phase），只增加原因 code/message。
inline void AttachCloseReason(Error& error, const Error& reason)
{
    error.details.emplace_back("close_reason", reason.message);
    error.details.emplace_back("close_reason_code", std::to_string(static_cast<int>(reason.code)));
    if (!reason.domain_code.empty()) {
        error.details.emplace_back("close_reason_domain_code", reason.domain_code);
    }
}

// ============================================================================
// SessionCore：单 link 的会话状态——收发缓存、拆包、pending、顺序与关闭。
// ============================================================================

struct SessionCore final : std::enable_shared_from_this<SessionCore> {
    std::shared_ptr<SessionOwnerState> owner;
    std::weak_ptr<ServerV2State> server;         // 入站接纳侧
    std::shared_ptr<detail::TcpChannel> channel; // 无状态字节转发：Read/Write/物理关闭
    std::string peer_identity;
    std::string endpoint; // outbound only
    bool outbound{false};
    std::uint64_t epoch{0};
    std::atomic<bool> dead{false};
    // 关闭责任：reader/writer 协程各持一份「未退出」责任（1=在跑）。启动时
    // 一并 +1 持物理连接责任；Reader/Writer 任一路退出时连同物理责任一起
    // 结算（写路径 Kill 与读路径 Kill 同义）。ExitDone()==0 才允许收口
    // 谓词把本 link 当已退役——session.dead 只是逻辑死亡，不是物理完成。
    std::atomic<int> responsibilities{0};
    // 本 link 占用的 owner 会话额度是否已归还（InstallSession 占位 →
    // Released 唯一归还点；输家 link 从未占位）。
    std::atomic<bool> quota_held{false};

    // ---- 出站 lane 队列（per-session FIFO worker 串行写；发送缓存归本类）----
    struct OutItem {
        std::string bytes;
        TrafficClass lane{TrafficClass::Service};
        std::string key; // request_id 或 message_id（数据帧）
        bbt::coroutine::Deadline deadline{RpcClock::time_point::max()};
        bool control_response{false}; // Response/SendAccepted
        // 回复写终态凭据：Complete/Fail 挂 waiter，WriterLoop/Kill 在到达
        // 真实写终态（写出/写失败/关闭丢弃）后写 outcome 并 Notify——共享
        // 对象不随 item move 失效，settled 保证唯一终态发布。
        struct Settle {
            std::shared_ptr<bbt::coroutine::sync::CoWaiter> waiter = bbt::coroutine::sync::CoWaiter::Create();
            result<void> outcome = result<void>::ok();
            std::atomic<bool> settled{false};
        };
        std::shared_ptr<Settle> settle;
    };
    mutable std::mutex out_mu;
    std::deque<OutItem> out_q[kLaneCount];

    std::shared_ptr<bbt::coroutine::sync::CoWaiter> out_signal = bbt::coroutine::sync::CoWaiter::Create();
    bool out_sealed{false};
    // 出站 lane 统计（queued/dequeued/dropped/bytes_queued 与 out_q
    // 同享 out_mu——读写同锁，stats 面不再有第二把锁的竞态）。
    TrafficStats out_lanes[kLaneCount];
    // 写侧 inflight/finished/dropped 计数（与出站真源同锁；RPC inflight
    // 覆盖「已入队未终态」）。
    std::uint32_t rr_credit[kLaneCount] = {0, 0, 0};
    // 排队中 + writer 正在写出的总条数（Drain 持有队列/写责任直到真实
    // 终态）：Enqueue+1、WriterLoop 结算-1、Kill 对丢弃项逐条结算。
    std::atomic<std::uint64_t> writes_outstanding{0};

    // ---- 关联表 -------------------------------------------------------------
    struct PendingCall {
        std::shared_ptr<bbt::coroutine::sync::CoWaiter> waiter = bbt::coroutine::sync::CoWaiter::Create();
        result<Response> outcome = result<Response>::err(Err(ErrorCode::InternalError, "unset"));
        std::string service;
        std::string method;
        std::string request_schema;
        std::string response_schema;
        // committed：writer 已承担发送责任（conn.Write 提交，结果可能未知），
        // 不只「写成功」。先通知后等待的丢唤醒由 done 持久谓词关闭——
        // Dispatch/写终态先写 done 再 Notify，等待方登记后复查 done。
        bool committed{false};
        bool done{false};
    };
    struct PendingSend {
        std::shared_ptr<bbt::coroutine::sync::CoWaiter> waiter = bbt::coroutine::sync::CoWaiter::Create();
        result<void> outcome = result<void>::err(Err(ErrorCode::InternalError, "unset"));
        bool committed{false};
        bool done{false};
    };
    mutable std::mutex pending_mu;
    std::unordered_map<std::string, std::shared_ptr<PendingCall>> calls;
    std::unordered_map<std::string, std::shared_ptr<PendingSend>> sends;

    // ---- reader：接收缓存与拆包 decoder 都是 session 状态，不在协议层 --------
    mutable std::mutex read_mu;
    SessionFrameDecoder decoder{kRpcV2DefaultMaxFrameBytes};

    // ---- 入站顺序台账（仅唯一 reader 协程访问，不加锁）------------------------
    // per-key 已接纳的最大 sequence（首帧即台账基值）；跨 lane 同一 key 同样
    // 约束——ledger 描述 wire 到达序，不描述 lane 调度序。重复/逆序/缺口在
    // DispatchInbound 拒绝（RejectProtocol），不与 ADR 的「同 key 递增」
    // 契约混放。
    std::unordered_map<std::string, std::uint64_t> inbound_sequence;

    // ---- stats ---------------------------------------------------------------
    // 出站 lane 统计真源已并入 out_mu/out_lanes；此处仅保留会话级序号。
    std::atomic<std::uint64_t> seq_counter{1};

    bool Submit(std::size_t stack, std::function<void()> fn);
    // 唯一「失败在途」入口：幂等（dead.exchange），丢弃出站队列、关闭物理
    // 连接、结算全部未终结 pending。关闭原因不在此附加——统一由 Call/Send
    // 调用方可见的返回点读取 owner 原因后附加。
    void Kill(std::error_code reason);
    // 一份退出责任结算（reader/writer/物理连接）。返回到达 0：唯一归零
    // 点负责唤醒 server/owner 收口观察。
    bool ExitDone();
    // Released 唯一归还点：物理 conn 已 Released 且退出责任归零后，归还
    // 本 link 占用的 owner 会话额度与映射（幂等）。持 owner->mu 调用；
    // 返回是否有额外交付（notify 已由调用方统一做时不重复）。
    bool ReleasedLocked();
    // conn 物理 Released 可能晚于退出责任归零，所以收口观察点会重驱一次
    // 归还（幂等）。持 owner->mu 调用。
    bool DriveReleaseLocked()
    {
        return ReleasedLocked();
    }
    result<void> Enqueue(OutItem item);
    // 持 out_mu 且 lane 非空；移出队列的唯一记账点，写责任随返回值交接。
    OutItem TakeQueuedLocked(std::size_t lane);
    // 单条 OutItem 唯一结算点：发布 settle 终态（若有凭据）+ 写责任计数
    // 归零观察；outcome 是该条目的真实终态。
    void NoteWriteSettled(OutItem& item, result<void> outcome);
    // 把本条关联的 pending（calls/sends，按 key 唯一）结算成 error 并唤醒
    // 等待方；key 为空或已结算时无副作用。写失败与写前钩子抛出共用。
    void FailItemPending(const OutItem& item, const Error& error);
    void WriterLoop();
    void ReaderLoop();
    void FailPending(Error error, bool committed_only);
    void DispatchInbound(const SessionFrame& frame);
    void AdmitInbound(const SessionFrame& frame, Request request);
    void PostSessionError(const Error& error);
    // 协议性终止：先尽力交付一帧 SessionError（系统 lane，有界等待写终态），
    // 再退役 link；避免对端只看到 TCP close 而不知道违规原因。
    void RejectProtocol(const std::string& message);
    bool Dequeue(OutItem& out);
};

// ============================================================================
// SessionOwnerState：会话 owner 台账（多 session 复用、拨号中连接与关闭）。
// ============================================================================

struct SessionOwnerState {
    std::shared_ptr<net::Engine> engine;
    ClientOptions options;
    // owner 级受控检查点（bbt/infra/rpc/Hooks.hpp）：同 owner 的 link 共用
    // 一份，不建全局态。由库分配，对外只给 owning handle——handle 可以比
    // owner 活得久，owner 收口后只读到 closed。
    std::shared_ptr<SessionHooks> hooks{new SessionHooks()};
    mutable std::mutex mu;
    // Open → Draining → Closed：draining 由 Graceful Close 置位（拒新、等
    // active_ops 归零），closed 由 Seal 置位（失败在途、发布首个原因）——
    // 两位不共用，Immediate/Timeout Close 在 Draining 下同样执行「失败在途」。
    bool closed{false};
    bool draining{false};
    std::optional<Error> close_reason; // Close/Shutdown 发布；首个原因固定
    // Close 等待唤醒：EndOperation / Seal / ReleasedLocked / 建立收尾统一
    // 经它 Notify；调用方在 owner->mu 外循环复查谓词（单等待位 + 周期复查，
    // 不承诺实时推送到每个观察者）。
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> close_signal = bbt::coroutine::sync::CoWaiter::Create();
    // 已接纳 Call/Send 责任计数：与 closed/draining 同门（mu）接纳/归还，
    // Graceful Close 等它归零，不依赖连接物理 Released。
    std::uint64_t active_ops{0};
    std::unordered_map<std::string, std::weak_ptr<SessionCore>> by_endpoint;
    std::unordered_map<std::string, std::shared_ptr<SessionCore>> by_identity;
    struct Establishing {
        std::shared_ptr<SessionCore> session;
        Error error = Err(ErrorCode::InternalError, "unset");
        bool done{false};
        // 建立中额度占位：GetSession 创建记录即置位（同事务），安装成功
        // 转化为 by_identity 成员额度；记录擦除时随记录消亡。InstallSession
        // 计数 = by_identity.size() + 其他 reserved 记录。
        bool reserved{false};
        // 建立中握手 conn（拨号成功→安装/失败前）：SessionOwnerSeal 据
        // 此 RequestClose——owner Close 不等握手自然到期（关闭责任）。
        net::Connection conn;
    };
    std::unordered_map<std::string, std::shared_ptr<Establishing>> establishing;
    std::weak_ptr<ServerV2State> inbound_server;
    std::uint64_t epoch_counter{0};
    std::mt19937_64 random{std::random_device{}()};

    // stats（sessions/links gauge，lane 汇总由 Session 上报）
    std::atomic<std::uint64_t> sessions{0};
    std::atomic<std::uint64_t> links{0};
    std::atomic<std::uint64_t> acceptance_pending{0};

    // 逻辑排空谓词（Graceful）：无在途调用且无未完结的拨号/握手。
    bool DrainedLocked() const
    {
        return active_ops == 0 && establishing.empty();
    }
    // 物理收口谓词（Timeout）：全部 link 映射与建立中记录都已归还。
    bool TeardownLocked() const
    {
        return by_identity.empty() && establishing.empty();
    }

    ~SessionOwnerState();
};

// ============================================================================
// Admission 不透明实现（公共类型见 Admission.hpp；impl 只归 Session 侧）。
// ============================================================================

namespace detail {

struct ReplyCompletionImpl {
    std::weak_ptr<SessionCore> session;
    std::weak_ptr<ServerV2State> server;
    FrameKind kind{FrameKind::Response}; // Response | SendAccepted
    TrafficClass lane{TrafficClass::Service};
    std::string request_id;
    std::string service;
    std::string method;
    std::string request_schema;
    std::string response_schema;
    std::string message_id; // SendAccepted only
    // 回复写终态的有限等待预算：取入站 RequestContext.deadline
    // （min(wire remaining budget, 本地 handler 预算)）；max() 时调用方
    // 仍按 30s 上限封顶。入队与等待共用同一预算，不另起计时。
    bbt::coroutine::Deadline reply_deadline{bbt::coroutine::Deadline::max()};
    std::atomic<bool> terminal{false};
};

struct AdmissionLeaseImpl {
    std::weak_ptr<ServerV2State> server;
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
    // per-lane 快照
    std::uint64_t queued_items[kLaneCount] = {0, 0, 0};
    std::uint64_t queued_bytes[kLaneCount] = {0, 0, 0};
    std::weak_ptr<ServerV2State> server;

    static std::size_t ItemBytes(const AdmissionItem& item)
    {
        // owning request 的实际计费字节——payload 之外含全部字符串
        // 字段、metadata 键值对与 ordering_key；request/reply/lease 本身
        // 的固定开销用 64 近似（不再仅 payload+64 低估）。
        const auto& r = item.request;
        std::size_t bytes = 64 + r.Payload().size() + r.Service().size() + r.Method().size() + r.RequestId().size()
                            + r.MessageId().size() + r.RequestSchema().size() + r.ResponseSchema().size()
                            + r.OrderingKey().size();
        for (const auto& kv : r.Metadata()) {
            bytes += kv.first.size() + kv.second.size();
        }
        return bytes;
    }
};

} // namespace detail

// ============================================================================
// ServerV2State：listener/admission/路由/令牌桶与收口台账。
// ============================================================================

struct ServerV2State final : std::enable_shared_from_this<ServerV2State> {
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
    // Close 等待唤醒：收口观察点统一经它 Notify，调用方在 mu 外复查谓词。
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> close_signal = bbt::coroutine::sync::CoWaiter::Create();
    bool started{false};
    bool stopping{false};
    bool closed{false};
    // Open → Draining → Closed：draining 只由 Graceful Close 置位（拒新），
    // Close 经 closed 表达；两位不共用。
    bool draining{false};
    std::optional<Error> close_reason; // Close/Shutdown 发布；首个原因固定
    bool queue_started{false};
    std::shared_ptr<AdmissionQueue> queue;
    std::unordered_map<std::string, Route> routes;
    std::uint64_t accepted{0};
    std::uint64_t callbacks{0};
    std::uint64_t inflight[kLaneCount] = {0, 0, 0};
    std::uint64_t finished[kLaneCount] = {0, 0, 0};
    std::uint64_t dropped[kLaneCount] = {0, 0, 0};
    std::vector<std::weak_ptr<SessionCore>> links;
    // 建立中入站握手 conn 台账（conn.Id() → 连接句柄）：accept 即登记、
    // ServeInbound 结束（安装成功/失败/封口拒绝）移除。Seal/Close 对台账
    // RequestClose 并保留责任——握手未入 links 期间同样受 Close 约束。
    std::unordered_map<std::uint64_t, net::Connection> handshakes;
    // AcceptLoop 协程退出标记：accept 循环本身也是一份退出责任——它可能
    // 刚 accept 出 conn、尚未登记 handshakes，谓词不能以「台账空」放行。
    bool accept_loop_exited{false};

    // 物理收口谓词在 server->mu 下评估；queue 计数经 AdmissionQueue 观察面
    // （自身 impl->mu）同步——锁序统一为 server->mu → impl->mu 单向。
    bool DrainedLocked() const
    {
        for (std::size_t i = 0; i != kLaneCount; ++i) {
            if (inflight[i] != 0) {
                return false;
            }
        }
        if (queue && queue->Queued() != 0) {
            return false;
        }
        // Drain 含队列/写责任：任何已登记 link 的 writes_outstanding 未归零
        // 都不算收口。dead 链接同样可能残留未结算写责任（writer 手中的
        // 在写项不属于排队丢弃口径）——不能用 !dead 跳过。
        for (const auto& weak : links) {
            if (auto session = weak.lock(); session && session->writes_outstanding.load() != 0) {
                return false;
            }
        }
        return true;
    }
    // 物理收口谓词：逻辑 dead 不是完成凭据——reader/writer 协程责任全部
    // 退出（ExitDone）且 conn 真实 Released（fd 关 + owner 责任清）才计；
    // 建立中握手 conn 同样要 Released。accept 循环未退出前不算收口——
    // 从未 Start 时 accept_loop_exited 语义为「无责任」按已退出计。
    bool StoppedLocked() const
    {
        if (started && !engine->AcceptingStopped()) {
            return false;
        }
        if (started && !accept_loop_exited) {
            return false;
        }
        if (!handshakes.empty()) {
            return false;
        }
        for (const auto& weak : links) {
            auto session = weak.lock();
            if (!session) {
                continue;
            }
            if (session->responsibilities.load() != 0 || !session->channel->IsReleased()) {
                return false;
            }
        }
        return DrainedLocked();
    }
    void NoteInflight(std::size_t lane, long delta)
    {
        auto& value = inflight[lane];
        value = static_cast<std::uint64_t>(static_cast<long>(value) + delta);
        if (close_signal) {
            close_signal->Notify();
        }
    }
};

// ============================================================================
// 检查点与跨 TU 接缝声明（实现见对应 .cc；外部接缝仍走 detail/Session.hpp）
// ============================================================================

/// 库内部固定异常标签：ThrowOnce 只抛这个类型，不接受用户异常对象或消息。
/// 调用点都在库内，由 RunHookCheckpoint 就地转成可见错误。
struct HookInjectedFailure final {};

/// 检查点退出谓词使用的借用视图：指针指向调用方当前持有的共享对象，
/// 只在本函数调用期间有效。
struct HookGate {
    const SessionOwnerState* owner{nullptr};
    const ServerV2State* server{nullptr};
    const SessionCore* session{nullptr};
    const detail::ICoChannel* channel{nullptr};
    bbt::coroutine::Deadline deadline{bbt::coroutine::Deadline::max()};
};

namespace detail {

/// @brief 固定动作检查点的实现接缝（detail/SessionHooks.cc 定义）。
/// 只做三件事：未注册直接通过；Pause 在库自己的 CoWaiter 上分片等待；
/// ThrowOnce 在锁内唯一认领并抛出内部标签。不执行用户代码。
class SessionHookAccess {
public:
    static result<void> Checkpoint(SessionHooks& hooks, HookPoint point, const HookGate& gate);
    /// @brief owner 封口/销毁：永久 closed 并解除全部注册，不等在途退出。
    static void Close(SessionHooks& hooks) noexcept;

private:
    static void Leave(SessionHooks& hooks, std::size_t index) noexcept;
    struct Flight {
        SessionHooks& hooks;
        std::size_t index;
        ~Flight()
        {
            Leave(hooks, index);
        }
    };
    static result<void> PauseUntil(SessionHooks& hooks, std::size_t index, std::uint64_t generation,
                                   bbt::coroutine::Deadline expires, const HookGate& gate);
};

/// @brief 路由分发（Session.cc 定义）：注册路由 + 令牌桶，未注册回落
/// framework admission handler。CoRpcServer::Create 的 handler lambda 转调。
result<void> DispatchAdmission(const std::shared_ptr<ServerV2State>& server, AdmissionItem item);

/// @brief 入站 link 的握手 → session 发布服务循环（detail/RpcSession.cc 定义）。
/// AcceptLoop 每接纳一条连接起一个本任务；channel 寿命随任务持有。
void ServeInbound(std::shared_ptr<ServerV2State> server, detail::TcpChannel channel);

} // namespace detail

} // namespace bbt::infra::rpc
