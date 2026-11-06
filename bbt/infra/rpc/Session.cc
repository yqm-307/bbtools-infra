// RPC v2 AppSession 实现（ADR 0014 §2.2/§2.3/§3/§4/§5）：
// Client-owned 双向会话表 + Server 共享同一 owner、trusted-network
// Hello/HelloAck 握手与 epoch 协商/去重、Call/Send/Response/SendAccepted
// 单 link 复用、单 FIFO AdmissionQueue worker、per-lane 有界出站调度、
// Stats/Close(deadline)/Drain。真实 I/O 复用 net::Engine Co* 消费面
// （CoDial/CoAccept/Read/Write/RequestClose），不重造网络引擎。
//
// 与 v1 共存：Client/Server 的旧 IRpc* seam 不动；本 TU 只实现 v2 方法
// 与 detail::Session.hpp 挂钩。SessionFrame wire 编解码复用 Codec.hpp
// （profile 2 + u32be 分帧 + decoder 分配前硬上限）。

#include <bbt/infra/rpc/Client.hpp>
#include <bbt/infra/rpc/Server.hpp>
#include <bbt/infra/rpc/detail/Session.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <functional>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/rpc/Codec.hpp>
#include <bbt/infra/rpc/Options.hpp>
#include <bbt/infra/rpc/detail/Tcp.hpp>

namespace bbt::infra::rpc {

namespace {

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;
using bbt::coroutine::sync::CoWaiter;
using bbt::coroutine::WaitStatus;

constexpr std::size_t kLaneCount = 3;
std::size_t LaneIndex(TrafficClass traffic) noexcept
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

Error Err(ErrorCode code, std::string message)
{
    return MakeError(code, "rpc session: " + std::move(message));
}
result<void> ErrV(ErrorCode code, std::string message)
{
    return result<void>::err(Err(code, std::move(message)));
}
Error ErrPhase(ErrorCode code, std::string message, RequestPhase phase)
{
    auto error = Err(code, std::move(message));
    error.request_phase = phase;
    return error;
}

bool NativeHost()
{
    return bbt::coroutine::GetLocalCoroutineId() == 0
           && bbt::coroutine::detail::Processer::TryGetLocalProcesser() == nullptr;
}

// 受管协程内检查点（与 TcpTransport.cc 同语义）：域非法/取消/期限到。
std::error_code CheckCall(bbt::coroutine::Deadline deadline)
{
    const auto current = g_bbt_tls_coroutine_co;
    if (!current) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    if (current->IsCancelRequested()) {
        return std::make_error_code(std::errc::operation_canceled);
    }
    if (Clock::now() >= deadline) {
        return std::make_error_code(std::errc::timed_out);
    }
    return {};
}

Error CallError(std::error_code cause, const char* message, bool committed)
{
    auto error = detail::TcpCallError(cause, message, committed);
    if (!error.request_phase.has_value() && committed) {
        error.request_phase = RequestPhase::RequestCommitted;
    }
    return error;
}

// deadline → wire remaining_budget_ms（[1, kRpcV2MaxRemainingBudgetMs]）。
result<std::uint32_t> Budget(bbt::coroutine::Deadline deadline)
{
    const auto now = Clock::now();
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

} // namespace

// ============================================================================

struct SessionCore final : std::enable_shared_from_this<SessionCore> {
    std::shared_ptr<SessionOwnerState> owner;
    std::weak_ptr<ServerV2State> server; // 入站接纳侧
    net::CoTcpConnection conn;
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

    // ---- 出站 lane 队列（per-session FIFO worker 串行写）-------------------
    struct OutItem {
        std::string bytes;
        TrafficClass lane{TrafficClass::Service};
        std::string key; // request_id 或 message_id（数据帧）
        bbt::coroutine::Deadline deadline{Clock::time_point::max()};
        bool control_response{false}; // Response/SendAccepted
        // 回复写终态凭据：Complete/Fail 挂 waiter，WriterLoop/Kill 在到达
        // 真实写终态（写出/写失败/关闭丢弃）后写 outcome 并 Notify——共享
        // 对象不随 item move 失效，settled 保证唯一终态发布。
        struct Settle {
            std::shared_ptr<CoWaiter> waiter = CoWaiter::Create();
            result<void> outcome = result<void>::ok();
            std::atomic<bool> settled{false};
        };
        std::shared_ptr<Settle> settle;
    };
    mutable std::mutex out_mu;
    std::deque<OutItem> out_q[kLaneCount];

    std::shared_ptr<CoWaiter> out_signal = CoWaiter::Create();
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
        std::shared_ptr<CoWaiter> waiter = CoWaiter::Create();
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
        std::shared_ptr<CoWaiter> waiter = CoWaiter::Create();
        result<void> outcome = result<void>::err(Err(ErrorCode::InternalError, "unset"));
        bool committed{false};
        bool done{false};
    };
    mutable std::mutex pending_mu;
    std::unordered_map<std::string, std::shared_ptr<PendingCall>> calls;
    std::unordered_map<std::string, std::shared_ptr<PendingSend>> sends;

    // ---- reader --------------------------------------------------------------
    mutable std::mutex read_mu;
    SessionFrameDecoder decoder{kRpcV2DefaultMaxFrameBytes};

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
    bool Dequeue(OutItem& out);
};

struct SessionOwnerState {
    std::shared_ptr<net::Engine> engine;
    ClientOptions options;
    // owner 级受控检查点（bbt/infra/rpc/Hooks.hpp）：同 owner 的 link 共用
    // 一份，不建全局态。由库分配，对外只给 owning handle——handle 可以比
    // owner 活得久，owner 收口后只读到 closed。
    std::shared_ptr<SessionHooks> hooks{new SessionHooks()};
    mutable std::mutex mu;
    std::condition_variable cv; // Shutdown 排空等待
    // Open → Draining → Closed：draining 由 Shutdown 置位（拒新、等
    // active_ops 归零），closed 由 Seal 置位（失败在途、发布首个原因）——
    // 两位不共用，Close 在 Draining 下同样执行「失败在途」。
    bool closed{false};
    bool draining{false};
    std::optional<Error> close_reason; // Close 发布；首个原因固定
    // 已接纳 Call/Send 责任计数：与 closed/draining 同门（mu）接纳/归还，
    // Shutdown 等它归零，不依赖连接物理 Released。
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

    ~SessionOwnerState();
};

namespace {

std::uint64_t NextEpoch(SessionOwnerState& owner)
{
    std::lock_guard<std::mutex> lock(owner.mu);
    if (++owner.epoch_counter == 0) {
        ++owner.epoch_counter;
    }
    return owner.epoch_counter;
}
std::uint64_t NextNonce(SessionOwnerState& owner)
{
    std::lock_guard<std::mutex> lock(owner.mu);
    std::uint64_t nonce = 0;
    do {
        nonce = owner.random();
    } while (nonce == 0);
    return nonce;
}

} // namespace

// ============================================================================
// detail::ReplyCompletionImpl / AdmissionLeaseImpl / AdmissionQueueImpl
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
    std::shared_ptr<CoWaiter> signal = CoWaiter::Create();
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
// ServerV2State
// ============================================================================

struct ServerV2State final : std::enable_shared_from_this<ServerV2State> {
    std::shared_ptr<net::Engine> engine;
    std::shared_ptr<SessionOwnerState> owner;
    AdmissionHandler handler;
    ServerOptions options;
    mutable std::mutex mu;
    std::condition_variable cv;
    bool started{false};
    bool stopping{false};
    bool closed{false};
    // Open → Draining → Closed：draining 只由 Shutdown 置位（StopAccepting
    // 不算重复 Shutdown），Close 经 closed 表达；两位不共用。
    bool draining{false};
    std::optional<Error> close_reason; // Close 发布；首个原因固定
    bool queue_started{false};
    std::shared_ptr<AdmissionQueue> queue;
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
            if (session->responsibilities.load() != 0 || !session->conn.IsReleased()) {
                return false;
            }
        }
        return DrainedLocked();
    }
    void NoteInflight(std::size_t lane, long delta)
    {
        auto& value = inflight[lane];
        value = static_cast<std::uint64_t>(static_cast<long>(value) + delta);
        cv.notify_all();
    }
};

namespace {

// 库内部固定异常标签：ThrowOnce 只抛这个类型，不接受用户异常对象或消息。
// 四个调用点都在本 TU，由 RunHookCheckpoint 就地转成可见错误。
struct HookInjectedFailure final {};

// 检查点退出谓词使用的借用视图：指针指向调用方当前持有的 shared 对象，
// 只在本函数调用期间有效。
struct HookGate {
    const SessionOwnerState* owner{nullptr};
    const ServerV2State* server{nullptr};
    const SessionCore* session{nullptr};
    const net::CoTcpConnection* conn{nullptr};
    bbt::coroutine::Deadline deadline{bbt::coroutine::Deadline::max()};
};

} // namespace

namespace detail {

/**
 * @brief 固定动作检查点的实现接缝（只在本 TU 定义，不提供给测试）。
 *
 * 只做三件事：未注册直接通过；Pause 在库自己的 CoWaiter 上分片等待；
 * ThrowOnce 在锁内唯一认领并抛出内部标签。不执行用户代码，不持业务锁等待，
 * 也不让异常穿过 writer/握手路径。
 */
class SessionHookAccess {
public:
    static result<void> Checkpoint(SessionHooks& hooks, HookPoint point, const HookGate& gate)
    {
        const auto index = SessionHooks::IndexOf(point);
        if (index == SessionHooks::kHookPointCount) {
            return ErrV(ErrorCode::InvalidArgument, "unknown hook point");
        }
        std::uint64_t generation = 0;
        HookAction action = HookAction::Pause;
        bbt::coroutine::Deadline expires = bbt::coroutine::Deadline::max();
        {
            std::lock_guard<std::mutex> lock(hooks.m_mu);
            if (hooks.m_closed) {
                return ErrV(ErrorCode::Closed, "hook owner closed");
            }
            auto& state = hooks.m_points[index];
            if (state.armed && Clock::now() >= state.expires_at) {
                state.armed = false; // 过期未命中的注册（含 ThrowOnce）失效
            }
            if (!state.armed) {
                return result<void>::ok();
            }
            generation = state.generation;
            action = state.action;
            expires = state.expires_at;
            ++state.entered;
            ++state.in_flight;
        }
        // 所有出口（正常、超时、取消、封口、抛出）都归还 in_flight。
        Flight flight{hooks, index};
        if (action == HookAction::ThrowOnce) {
            {
                std::lock_guard<std::mutex> lock(hooks.m_mu);
                auto& state = hooks.m_points[index];
                if (!state.armed || state.generation != generation) {
                    return result<void>::ok(); // 已被替换/注销：本次通过
                }
                state.armed = false; // 锁内唯一认领并消费
            }
            throw HookInjectedFailure{};
        }
        return PauseUntil(hooks, index, generation, expires, gate);
    }

    /// @brief owner 封口/销毁：永久 closed 并解除全部注册，不等在途退出。
    static void Close(SessionHooks& hooks) noexcept
    {
        std::lock_guard<std::mutex> lock(hooks.m_mu);
        hooks.m_closed = true;
        for (auto& state : hooks.m_points) {
            state.armed = false;
        }
    }

private:
    static void Leave(SessionHooks& hooks, std::size_t index) noexcept
    {
        std::lock_guard<std::mutex> lock(hooks.m_mu);
        auto& state = hooks.m_points[index];
        if (state.in_flight != 0) {
            --state.in_flight;
        }
    }

    struct Flight {
        SessionHooks& hooks;
        std::size_t index;
        ~Flight()
        {
            Leave(hooks, index);
        }
    };

    // Pause 的等待循环：每片重新取 min(操作期限, 注册到期, now + 10ms)，
    // 并在每片前后检查库对象终态。每片都不持 hook 锁。
    static result<void> PauseUntil(SessionHooks& hooks, std::size_t index, std::uint64_t generation,
                                   bbt::coroutine::Deadline expires, const HookGate& gate)
    {
        // 库对象终态：任一所属边界关闭都直接退出，不等注册被 Clear。
        const auto closed_reason = [&gate]() -> const char* {
            if (gate.owner && SessionOwnerClosed(*gate.owner)) {
                return "hook pause: owner closed";
            }
            if (gate.server && ServerV2Closed(*gate.server)) {
                return "hook pause: server closed";
            }
            if (gate.session && gate.session->dead.load()) {
                return "hook pause: session closed";
            }
            if (gate.conn && gate.conn->IsClosed()) {
                return "hook pause: connection closed";
            }
            return nullptr;
        };
        auto waiter = CoWaiter::Create();
        for (;;) {
            if (const char* reason = closed_reason()) {
                return ErrV(ErrorCode::Closed, reason);
            }
            bool paused = false;
            {
                std::lock_guard<std::mutex> lock(hooks.m_mu);
                if (hooks.m_closed) {
                    return ErrV(ErrorCode::Closed, "hook owner closed");
                }
                auto& state = hooks.m_points[index];
                if (state.armed && Clock::now() >= state.expires_at) {
                    state.armed = false;
                }
                paused = state.armed && state.generation == generation;
            }
            if (!paused) {
                break;
            } // 注销/替换/到期：解除暂停
            const auto now = Clock::now();
            if (now >= gate.deadline) {
                return ErrV(ErrorCode::TimedOut, "hook pause: operation deadline");
            }
            const auto slice = std::min({gate.deadline, expires, now + SessionHooks::kPauseSlice});
            const auto status = waiter->Wait({slice});
            switch (status) {
            case WaitStatus::Completed:
            case WaitStatus::TimedOut:
                break; // 本片结束，下一片重新检查注册与终态
            case WaitStatus::Cancelled:
                return ErrV(ErrorCode::Cancelled, "hook pause: cancelled");
            case WaitStatus::InvalidContext:
                return ErrV(ErrorCode::InvalidContext, "hook pause requires coroutine context");
            case WaitStatus::AlreadyWaiting:
            case WaitStatus::RuntimeUnavailable:
                return ErrV(ErrorCode::RuntimeUnavailable, "hook pause: wait unavailable");
            }
        }
        // 解除后再次检查关闭与操作期限，随后才允许提交 I/O 或发布 link。
        if (const char* reason = closed_reason()) {
            return ErrV(ErrorCode::Closed, reason);
        }
        if (Clock::now() >= gate.deadline) {
            return ErrV(ErrorCode::TimedOut, "hook pause: operation deadline");
        }
        return result<void>::ok();
    }
};

} // namespace detail

namespace {

const char* HookPointLabel(HookPoint point) noexcept
{
    switch (point) {
    case HookPoint::BeforeWrite:
        return "before_write";
    case HookPoint::OutboundBeforeHandshake:
        return "outbound_handshake";
    case HookPoint::InboundBeforeHandshake:
        return "inbound_handshake";
    case HookPoint::InboundBeforePublish:
        return "inbound_publish";
    }
    return "unknown";
}

// 把库内部固定标签转成可见错误：四个调用点都只处理 result，各自完成
// pending/记录/额度/连接的收尾，异常不越过各自的责任边界。
result<void> RunHookCheckpoint(SessionHooks& hooks, HookPoint point, const HookGate& gate)
{
    try {
        return detail::SessionHookAccess::Checkpoint(hooks, point, gate);
    } catch (const HookInjectedFailure&) {
        return ErrV(ErrorCode::InternalError,
                    std::string("hook checkpoint injected failure: ") + HookPointLabel(point));
    }
}

} // namespace

namespace {

// ============================================================================
// Outbound lane helpers
// ============================================================================

std::size_t LaneCap(const ClientOptions& options, TrafficClass traffic)
{
    switch (traffic) {
    case TrafficClass::System:
        return options.SystemQueueBytes();
    case TrafficClass::Service:
        return options.ServiceQueueBytes();
    case TrafficClass::Actor:
        return options.ActorQueueBytes();
    }
    return options.ServiceQueueBytes();
}

} // namespace

// ============================================================================
// SessionCore 方法
// ============================================================================

// 会话任务提交统一入口：每份协程任务计一份退出责任，fn 真实返回才结算。
// RegistCoroutineTask 只保证任务被接管，被接管任务必运行到 fn 返回（含
// 异常路径），责任随 fn 边界结算——不会把「已提交未运行」误报成完成。
bool SessionCore::Submit(std::size_t stack, std::function<void()> fn)
{
    auto session = shared_from_this();
    responsibilities.fetch_add(1);
    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        stack,
        [session, fn = std::move(fn)]() mutable {
            try {
                fn();
            } catch (...) {
                session->Kill({});
            }
            session->ExitDone();
        },
        registered);
    if (!registered) {
        responsibilities.fetch_sub(1);
    }
    return registered;
}

result<void> SessionCore::Enqueue(OutItem item)
{
    const auto lane = LaneIndex(item.lane);
    {
        std::lock_guard<std::mutex> lock(out_mu);
        if (out_sealed || dead.load()) {
            return ErrV(ErrorCode::Closed, "session sealed");
        }
        auto& stats = out_lanes[lane];
        const auto cap = LaneCap(owner->options, item.lane);
        const auto bytes = item.bytes.size();
        if (stats.bytes_queued + bytes > cap) {
            ++stats.dropped;
            // 满队列隔离：只拒本 lane 本条，不阻其他 lane。
            return ErrV(ErrorCode::Overloaded, "lane queue full");
        }
        out_q[lane].push_back(std::move(item));
        ++stats.queued;
        stats.bytes_queued += bytes;
        writes_outstanding.fetch_add(1);
    }
    if (out_signal) {
        out_signal->Notify();
    }
    return result<void>::ok();
}

SessionCore::OutItem SessionCore::TakeQueuedLocked(std::size_t lane)
{
    auto item = std::move(out_q[lane].front());
    out_q[lane].pop_front();
    auto& stats = out_lanes[lane];
    --stats.queued;
    stats.bytes_queued -= item.bytes.size();
    return item;
}

// 一条 OutItem 到达真实写终态（写出/写失败/被 Kill 丢弃）后的唯一
// 结算点：向回复凭据发布 outcome（settled 保证唯一），writes_outstanding
// 恰好 -1，归零时唤醒 server 收口观察。
void SessionCore::NoteWriteSettled(OutItem& item, result<void> outcome)
{
    if (item.settle && !item.settle->settled.exchange(true)) {
        item.settle->outcome = std::move(outcome);
        item.settle->waiter->Notify();
    }
    if (writes_outstanding.fetch_sub(1) == 1) {
        if (auto server = this->server.lock()) {
            std::lock_guard<std::mutex> lock(server->mu);
            server->cv.notify_all();
        } else if (auto server = owner->inbound_server.lock()) {
            std::lock_guard<std::mutex> lock(server->mu);
            server->cv.notify_all();
        }
    }
}

// 把本条关联的 pending 结算成 error 并唤醒等待方。写失败与「写前钩子抛出」
// 共用同一条责任路径：两者都发生在真实写终态之前，等待方必须拿到确定失败
// 而不是等到自己的 deadline。
void SessionCore::FailItemPending(const OutItem& item, const Error& error)
{
    if (item.key.empty()) {
        return;
    }
    std::shared_ptr<CoWaiter> waiter;
    {
        std::lock_guard<std::mutex> lock(pending_mu);
        if (auto it = calls.find(item.key); it != calls.end()) {
            it->second->outcome = result<Response>::err(error);
            it->second->done = true;
            waiter = it->second->waiter;
            calls.erase(it);
        } else if (auto si = sends.find(item.key); si != sends.end()) {
            si->second->outcome = result<void>::err(error);
            si->second->done = true;
            waiter = si->second->waiter;
            sends.erase(si);
        }
    }
    if (waiter) {
        waiter->Notify();
    }
}

bool SessionCore::Dequeue(OutItem& out)
{
    std::lock_guard<std::mutex> lock(out_mu);
    // 调度顺序：System 保留容量优先；Service/Actor 按权重轮询。
    auto pop = [&](std::size_t lane) -> bool {
        if (out_q[lane].empty()) {
            return false;
        }
        out = TakeQueuedLocked(lane);
        ++out_lanes[lane].dequeued;
        return true;
    };
    if (pop(0)) {
        return true;
    }
    const std::uint32_t weights[kLaneCount - 1] = {owner->options.ServiceWeight(), owner->options.ActorWeight()};
    // 权重轮询：rr_credit[lane]=本轮剩余可出队次数，两侧都耗尽时按
    // 权重整体重载。Service 连续取满自身份额后 credit=0，下一条落到还有
    // credit 的 Actor——Service 恒有流量也不会饿死 Actor。某 lane 队列空
    // 时 credit 保留但不消费；本条两侧都无有效 pop 走兜底任取。
    if (rr_credit[1] == 0 && rr_credit[2] == 0) {
        rr_credit[1] = weights[0];
        rr_credit[2] = weights[1];
    }
    for (unsigned attempt = 0; attempt != 2; ++attempt) {
        const std::size_t lane = 1 + attempt;
        if (rr_credit[lane] != 0 && pop(lane)) {
            --rr_credit[lane];
            return true;
        }
    }
    // 两 lane 本轮都无有效出队（空队列或 credit 耗尽且空）：兜底任取一条，
    // 不重置 credit——credit 描述的是份额，不是队列是否有货。
    return pop(1) || pop(2);
}

void SessionCore::WriterLoop()
{
    for (;;) {
        OutItem item;
        if (!Dequeue(item)) {
            if (dead.load()) {
                return;
            }
            const auto status = out_signal->Wait({Clock::now() + 25ms});
            if (dead.load()) {
                return;
            }
            if (status == WaitStatus::AlreadyWaiting || status == WaitStatus::InvalidContext) {
                bbtco_sleep(5);
            }
            continue;
        }
        auto deadline = item.deadline == Clock::time_point::max() ? Clock::now() + 30s : item.deadline;
        // 已出队项的写责任区间：检查点、committed 标记、conn.Write 与结果
        // 映射同属一份责任。任何出口都先结算这一条（关联 pending + reply
        // settle + writes_outstanding 恰好一次）；Kill 只清扫仍在队列里的
        // 条目，替已出队项结不了账，漏掉这一笔会让 writes_outstanding 永不
        // 归零，Drain/Close 只能等到超时。
        std::optional<Error> failure;
        bool write_attempted = false;
        try {
            // 固定动作检查点：未注册直接通过；Pause 在库内分片等待，不执行
            // 任何用户代码。失败（期限/取消/封口/注入）都是提交前确定失败。
            const auto gate = RunHookCheckpoint(*owner->hooks, HookPoint::BeforeWrite,
                                                HookGate{owner.get(), nullptr, this, &conn, deadline});
            if (!gate) {
                failure = gate.error();
            } else {
                // writer 已把本条从队列取出并即将交给 conn.Write——责任自
                // 此刻由 writer 承担，不再属「排队中」。先把关联 pending 标
                // committed（承担发送责任，结果可能未知），随后挂起期间若
                // link 被 Kill，FailPending 的 committed_only 结算按「已承担」
                // 返回 OutcomeUnknown 而非「未提交」Closed。
                if (!item.key.empty()) {
                    std::lock_guard<std::mutex> lock(pending_mu);
                    if (auto it = calls.find(item.key); it != calls.end()) {
                        it->second->committed = true;
                    } else if (auto si = sends.find(item.key); si != sends.end()) {
                        si->second->committed = true;
                    }
                }
                write_attempted = true;
                const auto report = conn.Write(std::move(item.bytes), {deadline});
                const auto settled = detail::TcpSettleWrite(report, "frame write failed");
                if (!settled) {
                    failure = settled.error();
                }
            }
        } catch (...) {
            // 提交前：确定失败。已承担 conn.Write：字节可能已发出，按既有
            // RequestCommitted/OutcomeUnknown 语义结算，不假装零提交、不重试。
            failure = write_attempted ? ErrPhase(ErrorCode::OutcomeUnknown, "write path threw after commit",
                                                 RequestPhase::RequestCommitted)
                                      : Err(ErrorCode::InternalError, "write path threw before commit");
        }
        // 请求关联只在失败时结束；写成功仍须等可信回复。无论成功或失败，
        // writer 持有项都在同一处交还写责任，Kill 只负责队列中其余条目。
        if (failure) {
            FailItemPending(item, *failure);
        }
        NoteWriteSettled(item, failure ? result<void>::err(*failure) : result<void>::ok());
        if (failure) {
            Kill(std::make_error_code(std::errc::io_error));
            return;
        }
    }
}

namespace {

// 关闭原因作为可观察详情附加：保留既有失败分类（未提交 Closed /
// 已提交 OutcomeUnknown+request_phase），只增加原因 code/message。
// 唯一附加点是 Call/Send 调用方可见的返回点，不在内部各失败出口分别补。
void AttachCloseReason(Error& error, const Error& reason)
{
    error.details.emplace_back("close_reason", reason.message);
    error.details.emplace_back("close_reason_code", std::to_string(static_cast<int>(reason.code)));
    if (!reason.domain_code.empty()) {
        error.details.emplace_back("close_reason_domain_code", reason.domain_code);
    }
}

// pending 唯一终态仲裁：committed_only 下未提交按 Closed、已提交按
// OutcomeUnknown + RequestCommitted；关闭原因不在此附加（见 AttachCloseReason）。
Error SettledPendingError(bool committed_only, bool committed, const Error& error)
{
    if (committed_only && !committed) {
        return Err(ErrorCode::Closed, "session closed before commit");
    }
    if (!committed_only) {
        return error;
    }
    return ErrPhase(ErrorCode::OutcomeUnknown, "session lost after commit", RequestPhase::RequestCommitted);
}

} // namespace

void SessionCore::FailPending(Error error, bool committed_only)
{
    std::vector<std::shared_ptr<CoWaiter>> wake;
    {
        std::lock_guard<std::mutex> lock(pending_mu);
        for (auto it = calls.begin(); it != calls.end();) {
            auto& pending = it->second;
            pending->outcome = result<Response>::err(SettledPendingError(committed_only, pending->committed, error));
            pending->done = true;
            wake.push_back(pending->waiter);
            it = calls.erase(it);
        }
        for (auto it = sends.begin(); it != sends.end();) {
            auto& pending = it->second;
            pending->outcome = result<void>::err(SettledPendingError(committed_only, pending->committed, error));
            pending->done = true;
            wake.push_back(pending->waiter);
            it = sends.erase(it);
        }
    }
    for (auto& waiter : wake) {
        waiter->Notify();
    }
}

void SessionCore::Kill(std::error_code reason)
{
    if (dead.exchange(true)) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(out_mu);
        out_sealed = true;
        for (std::size_t lane = 0; lane != kLaneCount; ++lane) {
            while (!out_q[lane].empty()) {
                auto item = TakeQueuedLocked(lane);
                ++out_lanes[lane].dropped;
                // 队列项被 Kill 丢弃：发布 Closed 终态（未写出不能保留
                // 「成功终态已发布」的假象）并结算本条写责任。writer 手中
                // 在写的那条不在队列内，由 WriterLoop 终态路径正常结算。
                NoteWriteSettled(item, result<void>::err(Err(ErrorCode::Closed, "session killed before reply write")));
            }
        }
    }
    if (out_signal) {
        out_signal->Notify();
    }
    if (conn.Raw().Valid()) {
        conn.RequestClose(reason == std::error_code{} ? std::make_error_code(std::errc::io_error) : reason);
    }
    // committed 但无可信终态 → OutcomeUnknown；未提交 → Closed。
    FailPending({}, /*committed_only=*/true);
    // owner 链接数/会话额度与映射不归这里结算：session.dead 只是逻辑死亡，
    // 物理完成凭据是「退出责任归零 + conn Released」——唯一归还点在
    // Reader/WriterLoop 尾部的 ReleasedLocked()。
    if (auto server = this->server.lock()) {
        std::lock_guard<std::mutex> lock(server->mu);
        server->cv.notify_all();
    } else if (auto server = owner->inbound_server.lock()) {
        std::lock_guard<std::mutex> lock(server->mu);
        server->cv.notify_all();
    }
    // 物理连接责任在逻辑死亡点结算：Kill 只进一次（dead.exchange 保证），
    // 此后 reader/writer 各自退出再结算自己的那份——责任归零点即本 link
    // 物理收口观察点。
    ExitDone();
}

// 一份退出责任到达终态。到达 0 时若物理连接也已 Released，在本调用内
// 完成额度归还（ReleasedLocked 幂等，重复收口无效果）。
bool SessionCore::ExitDone()
{
    if (responsibilities.fetch_sub(1) != 1) {
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        ReleasedLocked();
    }
    if (auto server = this->server.lock()) {
        std::lock_guard<std::mutex> lock(server->mu);
        server->cv.notify_all();
    } else if (auto server = owner->inbound_server.lock()) {
        std::lock_guard<std::mutex> lock(server->mu);
        server->cv.notify_all();
    }
    return true;
}

// Released 唯一归还点（持 owner->mu）：物理收口真实完成后才归还额度——
// 退役未完成的 link 持续占用 MaxSessions，防止「逻辑死亡即释放额度」
// 让新建立绕过上限。物理 conn 未 Released 前不得归还——conn 仍
// 承担在途读写/物理关闭责任时，session/link 计数与 by_identity 台账必须
// 保留，否则失去物理 Released 凭据。返回是否有额外交付。
bool SessionCore::ReleasedLocked()
{
    // 唯一归还点：物理 conn 必须已 Released（fd 关 + owner 责任清）。
    if (!conn.IsReleased()) {
        return false;
    }
    if (!quota_held.exchange(false)) {
        return false;
    }
    owner->links.fetch_sub(1);
    owner->sessions.fetch_sub(1);
    for (auto it = owner->by_identity.begin(); it != owner->by_identity.end();) {
        if (it->second.get() == this) {
            it = owner->by_identity.erase(it);
        } else {
            ++it;
        }
    }
    for (auto it = owner->by_endpoint.begin(); it != owner->by_endpoint.end();) {
        if (auto session = it->second.lock(); !session || session.get() == this) {
            it = owner->by_endpoint.erase(it);
        } else {
            ++it;
        }
    }
    return true;
}

void SessionCore::PostSessionError(const Error& error)
{
    SessionFrame frame;
    frame.kind = FrameKind::SessionError;
    frame.traffic = TrafficClass::System;
    frame.session_epoch = epoch;
    frame.has_outcome = true;
    frame.success = false;
    frame.error = error;
    auto bytes = EncodeFramedSessionFrame(frame, owner->options.MaxFrameBytes());
    if (bytes) {
        OutItem item;
        item.bytes.assign(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
        item.lane = TrafficClass::System;
        item.control_response = true;
        item.deadline = Clock::now() + 5s;
        (void)Enqueue(std::move(item));
    }
}

namespace {
// reader 上的 admission 拒绝路径不等写终态（定义在 TU 后部）。
void PostReplyErrorNoWait(const std::shared_ptr<SessionCore>& session, const detail::ReplyCompletionImpl& impl,
                          Error error);
} // namespace

void SessionCore::AdmitInbound(const SessionFrame& frame, Request request)
{
    auto server = this->server.lock();
    if (!server) {
        server = owner->inbound_server.lock();
    }
    auto& stats = out_lanes[LaneIndex(request.Traffic())];
    std::shared_ptr<AdmissionQueue> queue;
    if (server) {
        // queue 指针写入受 server->mu 保护（ServerV2Start）；观察面取同一
        // 临界区快照，不与 Start/Close 的台账写并发裸读。
        std::lock_guard<std::mutex> lock(server->mu);
        queue = server->queue;
    }
    if (!server || !queue) {
        PostSessionError(Err(ErrorCode::Unavailable, "no inbound admission target"));
        {
            std::lock_guard<std::mutex> lock(out_mu);
            ++stats.dropped;
        }
        return;
    }
    AdmissionItem item;
    item.request = std::move(request);
    auto lease_impl = std::make_shared<detail::AdmissionLeaseImpl>();
    lease_impl->server = server;
    lease_impl->lane = item.request.Traffic();
    item.lease = detail::AdmissionAccess::MakeLease(lease_impl);
    if (frame.kind == FrameKind::Call
        || (frame.kind == FrameKind::Send && frame.acceptance_requested.has_value()
            && frame.acceptance_requested.value())) {
        auto impl = std::make_shared<detail::ReplyCompletionImpl>();
        impl->session = weak_from_this();
        impl->server = server;
        impl->lane = item.request.Traffic();
        impl->reply_deadline = item.request.Context().deadline;
        if (frame.kind == FrameKind::Call) {
            impl->kind = FrameKind::Response;
            impl->request_id = frame.request_id;
            impl->service = frame.service;
            impl->method = frame.method;
            impl->request_schema = frame.request_schema;
            impl->response_schema = frame.response_schema;
        } else {
            impl->kind = FrameKind::SendAccepted;
            impl->message_id = frame.message_id;
        }
        item.reply = detail::AdmissionAccess::MakeReply(impl);
    }
    {
        std::lock_guard<std::mutex> lock(out_mu);
        ++stats.inflight;
    }
    const auto item_lane = LaneIndex(item.request.Traffic());
    // MaxInflight 覆盖「已交 admission 仍未到责任终态」的持有 lease——
    // 在唯一 +1 之前按容量拒绝（拒绝立即 Overloaded，不等 deadline）。
    // 计数与额度检查在同一 server->mu 临界区，杜绝越过上限的竞态。
    bool over_cap = false;
    {
        std::lock_guard<std::mutex> lock(server->mu);
        if (server->inflight[item_lane] >= server->options.MaxInflight()) {
            ++server->dropped[item_lane];
            over_cap = true;
        } else {
            server->NoteInflight(item_lane, +1);
        }
    }
    if (over_cap) {
        {
            std::lock_guard<std::mutex> slock(out_mu);
            ++stats.dropped;
            --stats.inflight;
        }
        // 保留 reply 凭据发布唯一拒绝终态，连接仍可进展。本路径在
        // 唯一 reader 上——不能调用会挂起等写终态的 Fail（写背压会阻塞
        // reader，无法继续解后续帧）。标 terminal 后直接入队错误帧不等待；
        // 写责任仍由 WriterLoop/Kill 真实结算。
        if (item.reply) {
            if (auto impl = detail::AdmissionAccess::ClaimTerminal(*item.reply)) {
                auto session = impl->session.lock();
                if (session) {
                    PostReplyErrorNoWait(session, *impl, Err(ErrorCode::Overloaded, "inflight limit reached"));
                }
            }
        }
        // lease 从未 +1（额度检查先于唯一记账）——不 Release，
        // 随 item 析构，不欠账。
        return;
    }
    // 保留完成凭据/lease 的本地副本——Admit 按值转移 item，拒绝路径
    // 仍需可用 reply/lease 结算唯一终态。
    auto reply = item.reply;
    auto lease = item.lease;
    const auto admitted = queue->Admit(std::move(item));
    if (!admitted) {
        // Admit 失败（满/封口）：item 未入队，infra 直接发布一次终态——
        // Call → Response error；带确认 Send → SendAccepted error。本路径
        // 同在唯一 reader 上，不等写终态。
        if (reply) {
            if (auto impl = detail::AdmissionAccess::ClaimTerminal(*reply)) {
                auto session = impl->session.lock();
                if (session) {
                    PostReplyErrorNoWait(session, *impl, admitted.error());
                }
            }
        }
        // lease 单次结算：Release 已完成 inflight-1，这里不再重复扣账。
        lease->Release();
        {
            std::lock_guard<std::mutex> lock(server->mu);
            ++server->dropped[item_lane];
        }
        {
            std::lock_guard<std::mutex> lock(out_mu);
            ++stats.dropped;
            --stats.inflight;
        }
    }
}

void SessionCore::DispatchInbound(const SessionFrame& frame)
{
    if (frame.session_epoch != 0 && frame.session_epoch != epoch) {
        // 旧 epoch 帧丢弃并计数（不改写已发布终态）。
        auto& stats = out_lanes[LaneIndex(frame.traffic)];
        std::lock_guard<std::mutex> lock(out_mu);
        ++stats.dropped;
        return;
    }
    switch (frame.kind) {
    case FrameKind::Response: {
        std::shared_ptr<PendingCall> pending;
        {
            std::lock_guard<std::mutex> lock(pending_mu);
            auto it = calls.find(frame.request_id);
            if (it != calls.end()) {
                pending = it->second;
                calls.erase(it);
            }
        }
        if (!pending) {
            return;
        } // 晚到/超时后回复丢弃
        if (frame.has_outcome && frame.success) {
            auto response = ResponseFromSessionFrame(frame);
            if (response && frame.service == pending->service && frame.method == pending->method
                && frame.request_schema == pending->request_schema
                && frame.response_schema == pending->response_schema) {
                pending->outcome = std::move(response);
            } else {
                pending->outcome = result<Response>::err(ErrPhase(
                    ErrorCode::ProtocolError, response ? "response correlation mismatch" : "response decode failed",
                    RequestPhase::ReplyTerminal));
            }
        } else if (frame.has_outcome) {
            auto error = frame.error;
            if (!error.request_phase.has_value()) {
                error.request_phase = RequestPhase::ReplyTerminal;
            }
            pending->outcome = result<Response>::err(std::move(error));
        } else {
            pending->outcome = result<Response>::err(
                ErrPhase(ErrorCode::ProtocolError, "response missing outcome", RequestPhase::ReplyTerminal));
        }
        pending->done = true;
        pending->waiter->Notify();
        return;
    }
    case FrameKind::SendAccepted: {
        std::shared_ptr<PendingSend> pending;
        {
            std::lock_guard<std::mutex> lock(pending_mu);
            auto it = sends.find(frame.message_id);
            if (it != sends.end()) {
                pending = it->second;
                sends.erase(it);
            }
        }
        if (!pending) {
            return;
        }
        if (frame.has_outcome && frame.success) {
            pending->outcome = result<void>::ok();
        } else if (frame.has_outcome) {
            pending->outcome = result<void>::err(frame.error);
        } else {
            pending->outcome = result<void>::err(
                ErrPhase(ErrorCode::ProtocolError, "send-accepted missing outcome", RequestPhase::ReplyTerminal));
        }
        pending->done = true;
        pending->waiter->Notify();
        return;
    }
    case FrameKind::Call:
    case FrameKind::Send: {
        auto request = RequestFromSessionFrame(frame);
        if (!request) {
            PostSessionError(request.error());
            return;
        }
        RequestContext context;
        const auto now = Clock::now();
        const auto remote = now + std::chrono::milliseconds{frame.remaining_budget_ms};
        const auto local = [this, now] {
            if (auto server = this->server.lock()) {
                return now + server->options.IncomingTimeout();
            }
            return Clock::time_point::max();
        }();
        context.deadline = std::min(remote, local);
        context.peer_identity = peer_identity;
        detail::RequestContextAccess::Set(request.value(), std::move(context));
        AdmitInbound(frame, std::move(request.value()));
        return;
    }
    case FrameKind::SessionError:
        Kill(std::make_error_code(std::errc::connection_reset));
        return;
    case FrameKind::Close:
        Kill({});
        return;
    case FrameKind::Hello:
    case FrameKind::HelloAck:
    case FrameKind::Unspecified:
        PostSessionError(Err(ErrorCode::ProtocolError, "unexpected frame kind on live session"));
        Kill(std::make_error_code(std::errc::protocol_error));
        return;
    }
}

void SessionCore::ReaderLoop()
{
    for (;;) {
        if (dead.load()) {
            return;
        }
        // 先耗 decoder 缓存（含握手移交的合包余帧），再阻塞 Read。
        for (;;) {
            auto next = decoder.Next();
            if (!next) {
                Kill(std::make_error_code(std::errc::protocol_error));
                return;
            }
            if (!next.value().has_value()) {
                break;
            }
            DispatchInbound(*next.value());
            if (dead.load()) {
                return;
            }
        }
        auto [read_data, read_error] = conn.Read({Clock::time_point::max()});
        if (!read_error) {
            for (const auto& seg : read_data.Segments()) {
                const auto fed = decoder.Feed(reinterpret_cast<const std::uint8_t*>(seg.data), seg.size);
                if (!fed) {
                    Kill(std::make_error_code(std::errc::protocol_error));
                    return;
                }
            }
            continue;
        }
        if (read_error == bbt::infra::net::make_error_code(bbt::infra::net::NetErrc::Eof)) {
            Kill(std::make_error_code(std::errc::connection_reset));
        } else {
            Kill(read_error);
        }
        return;
    }
}
// AdmissionQueue 实现
// ============================================================================

namespace {

void AdmissionWorker(std::shared_ptr<detail::AdmissionQueueImpl> impl)
{
    for (;;) {
        AdmissionItem item;
        bool have = false;
        {
            std::lock_guard<std::mutex> lock(impl->mu);
            if (!impl->queue.empty()) {
                item = std::move(impl->queue.front());
                impl->queue.pop_front();
                const auto lane = LaneIndex(item.request.Traffic());
                impl->bytes -= detail::AdmissionQueueImpl::ItemBytes(item);
                --impl->queued_items[lane];
                impl->queued_bytes[lane] -= detail::AdmissionQueueImpl::ItemBytes(item);
                have = true;
            }
        }
        if (!have) {
            {
                std::lock_guard<std::mutex> lock(impl->mu);
                if (impl->closed && impl->queue.empty()) {
                    if (auto server = impl->server.lock()) {
                        server->cv.notify_all();
                    }
                    return;
                }
            }
            const auto status = impl->signal->Wait({Clock::now() + 25ms});
            if (status == WaitStatus::AlreadyWaiting || status == WaitStatus::InvalidContext) {
                bbtco_sleep(5);
            }
            continue;
        }
        const auto lane = LaneIndex(item.request.Traffic());
        const auto is_call = item.request.Reply() == ReplyMode::Required;
        auto reply = item.reply;
        auto lease = item.lease;
        result<void> handled = result<void>::ok();
        try {
            handled = impl->handler(std::move(item));
        } catch (...) {
            handled = ErrV(ErrorCode::InternalError, "admission handler threw");
        }
        auto server = impl->server.lock();
        if (server) {
            std::lock_guard<std::mutex> lock(server->mu);
            ++server->callbacks;
        }
        if (handled) {
            if (reply && reply->ForSendAcceptance()) {
                // Send + RequireRemoteAcceptance：mailbox 接纳成功由 infra
                // 写一次 SendAccepted 终态；Complete({}) 产成功确认。
                (void)reply->Complete(Response{});
            }
            if (!is_call && lease) {
                // Send：mailbox 接纳即结束 infra 投递责任（ADR §4.2）。
                lease->Release();
            }
            // Call：reply/lease 归 framework，worker 不发布、不减 inflight。
            continue;
        }
        // 未接纳：infra 发布一次终态（Call → Response error；带确认 Send →
        // SendAccepted error），item 未转移（handler 契约：失败即未转移）。
        if (reply && !reply->IsTerminal()) {
            auto error = handled.error();
            if (reply->ForSendAcceptance() || is_call) {
                (void)reply->Fail(std::move(error));
            }
        }
        // lease 单次结算——Release 内部已完成 inflight-1 与 finished+1，
        // 这里不再重复 NoteInflight（下溢 UINT64_MAX 会让 Drain 假超时）。
        if (lease) {
            lease->Release();
        }
        if (server) {
            std::lock_guard<std::mutex> lock(server->mu);
            ++server->dropped[lane];
        }
    }
}

} // namespace

// --- AdmissionQueue 公共方法 -------------------------------------------------

result<std::shared_ptr<AdmissionQueue>> AdmissionQueue::Create(std::size_t max_items, std::size_t max_bytes,
                                                               AdmissionHandler handler)
{
    using R = result<std::shared_ptr<AdmissionQueue>>;
    if (max_items == 0 || max_bytes == 0 || !handler) {
        return R::err(Err(ErrorCode::InvalidArgument, "admission queue requires bounds and handler"));
    }
    if (!g_scheduler || !g_scheduler->IsInitialized()) {
        return R::err(Err(ErrorCode::RuntimeUnavailable, "coroutine scheduler not running"));
    }
    auto impl = std::make_shared<detail::AdmissionQueueImpl>();
    impl->max_items = max_items;
    impl->max_bytes = max_bytes;
    impl->handler = std::move(handler);
    bool registered = false;
    g_scheduler->RegistCoroutineTask(128 * 1024, [impl] { AdmissionWorker(impl); }, registered);
    if (!registered) {
        return R::err(Err(ErrorCode::RuntimeUnavailable, "admission worker registration failed"));
    }
    return R::ok(detail::AdmissionAccess::MakeQueue(std::move(impl)));
}

// --- detail::AdmissionAccess：不透明实现的唯一构造/收口入口 -------------------

std::shared_ptr<AdmissionQueue> detail::AdmissionAccess::MakeQueue(std::shared_ptr<detail::AdmissionQueueImpl> impl)
{
    return std::shared_ptr<AdmissionQueue>(new AdmissionQueue(std::move(impl)));
}

std::shared_ptr<ReplyCompletion> detail::AdmissionAccess::MakeReply(std::shared_ptr<detail::ReplyCompletionImpl> impl)
{
    return std::shared_ptr<ReplyCompletion>(new ReplyCompletion(std::move(impl)));
}

std::shared_ptr<AdmissionLease> detail::AdmissionAccess::MakeLease(std::shared_ptr<detail::AdmissionLeaseImpl> impl)
{
    return std::shared_ptr<AdmissionLease>(new AdmissionLease(std::move(impl)));
}

std::shared_ptr<detail::ReplyCompletionImpl> detail::AdmissionAccess::ClaimTerminal(ReplyCompletion& reply) noexcept
{
    return reply.MarkTerminalForReject();
}

void detail::AdmissionAccess::AttachQueue(const std::shared_ptr<AdmissionQueue>& queue,
                                          const std::shared_ptr<ServerV2State>& server)
{
    if (!queue || !queue->m_impl) {
        return;
    }
    queue->m_impl->server = server;
}

result<void> AdmissionQueue::Admit(AdmissionItem item)
{
    if (!m_impl) {
        return ErrV(ErrorCode::Closed, "admission queue invalid");
    }
    const auto lane = LaneIndex(item.request.Traffic());
    const auto bytes = detail::AdmissionQueueImpl::ItemBytes(item);
    bool over_cap = false;
    {
        std::lock_guard<std::mutex> lock(m_impl->mu);
        if (m_impl->closed) {
            return ErrV(ErrorCode::Closed, "admission queue closed");
        }
        if (m_impl->queue.size() >= m_impl->max_items || m_impl->bytes + bytes > m_impl->max_bytes) {
            over_cap = true;
        } else {
            ++m_impl->queued_items[lane];
            m_impl->queued_bytes[lane] += bytes;
            m_impl->bytes += bytes;
            m_impl->queue.push_back(std::move(item));
        }
    }
    // dropped 记账移出 impl->mu 临界区：锁序统一为 server->mu → impl->mu
    //（ServerV2Drain/Stats 持 server->mu 时可经 queue 观察面取 impl->mu；
    // 反向在 impl->mu 内再取 server->mu 构成闭环死锁）。
    if (over_cap) {
        if (auto server = m_impl->server.lock()) {
            std::lock_guard<std::mutex> slock(server->mu);
            ++server->dropped[lane];
        }
        return ErrV(ErrorCode::Overloaded, "admission queue full");
    }
    m_impl->signal->Notify();
    return result<void>::ok();
}

void AdmissionQueue::Close() noexcept
{
    if (!m_impl) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(m_impl->mu);
        m_impl->closed = true;
    }
    m_impl->signal->Notify();
}

bool AdmissionQueue::IsClosed() const noexcept
{
    if (!m_impl) {
        return true;
    }
    std::lock_guard<std::mutex> lock(m_impl->mu);
    return m_impl->closed;
}

std::size_t AdmissionQueue::Queued() const noexcept
{
    if (!m_impl) {
        return 0;
    }
    std::lock_guard<std::mutex> lock(m_impl->mu);
    return m_impl->queue.size();
}

std::size_t AdmissionQueue::BytesQueued() const noexcept
{
    if (!m_impl) {
        return 0;
    }
    std::lock_guard<std::mutex> lock(m_impl->mu);
    return m_impl->bytes;
}

std::uint64_t AdmissionQueue::Queued(std::size_t lane) const noexcept
{
    if (!m_impl || lane >= kLaneCount) {
        return 0;
    }
    std::lock_guard<std::mutex> lock(m_impl->mu);
    return m_impl->queued_items[lane];
}

std::uint64_t AdmissionQueue::BytesQueued(std::size_t lane) const noexcept
{
    if (!m_impl || lane >= kLaneCount) {
        return 0;
    }
    std::lock_guard<std::mutex> lock(m_impl->mu);
    return m_impl->queued_bytes[lane];
}

// --- ReplyCompletion / AdmissionLease 公共方法 --------------------------------

bbt_co_async(result<void>) ReplyCompletion::Complete(Response response)
{
    auto impl = m_impl;
    if (!impl) {
        return ErrV(ErrorCode::InvalidArgument, "reply completion invalid");
    }
    // 契约要求受管协程挂起等写终态；非协程调用在 terminal/入队等任何
    // 副作用前拒绝——「无法等待」不得伪报成写成功（写责任唯一结算由
    // infra 完成，本条不产生任何写责任）。
    if (!g_bbt_tls_coroutine_co) {
        return ErrV(ErrorCode::InvalidContext, "reply completion requires coroutine context");
    }
    if (impl->terminal.exchange(true)) {
        return ErrV(ErrorCode::InvalidArgument, "reply completion already terminal");
    }
    auto session = impl->session.lock();
    if (!session || session->dead.load()) {
        return ErrV(ErrorCode::Closed, "session closed before reply");
    }
    const auto write_deadline = impl->reply_deadline;
    SessionFrame frame;
    frame.session_epoch = session->epoch;
    frame.has_outcome = true;
    frame.success = true;
    if (impl->kind == FrameKind::SendAccepted) {
        frame.kind = FrameKind::SendAccepted;
        frame.traffic = TrafficClass::System;
        frame.message_id = impl->message_id;
    } else {
        // Response 关联字段必须与 Request 一致（ADR §2.4）；不一致
        // ProtocolError 并改写为错误终态，不让 handler 改写他人回复。
        if (response.RequestId() != impl->request_id || response.Service() != impl->service
            || response.Method() != impl->method || response.RequestSchema() != impl->request_schema
            || response.ResponseSchema() != impl->response_schema) {
            frame.kind = FrameKind::Response;
            frame.traffic = impl->lane;
            frame.reply = ReplyMode::Required;
            frame.request_id = impl->request_id;
            frame.service = impl->service;
            frame.method = impl->method;
            frame.request_schema = impl->request_schema;
            frame.response_schema = impl->response_schema;
            frame.success = false;
            frame.error = Err(ErrorCode::ProtocolError, "response correlation mismatch");
            auto bytes = EncodeFramedSessionFrame(frame, session->owner->options.MaxFrameBytes());
            if (bytes) {
                SessionCore::OutItem item;
                item.bytes.assign(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
                item.lane = impl->lane;
                item.control_response = true;
                item.deadline = write_deadline;
                item.settle = std::make_shared<SessionCore::OutItem::Settle>();
                auto settle = item.settle;
                // 关联错误终态同样是一条真实写责任：入队失败写责任从未
                // 产生（Enqueue 只在成功时 +1）；入队成功则等真实写终态
                // 再返回，写责任如实计入排空。
                auto enqueued = session->Enqueue(std::move(item));
                if (enqueued) {
                    const auto wait_deadline = write_deadline == Clock::time_point::max()
                                                   ? Clock::now() + 30s
                                                   : std::min(write_deadline, Clock::now() + 30s);
                    (void)settle->waiter->Wait({wait_deadline});
                }
            }
            // 调用方面前始终保留 ProtocolError 语义（关联违规的判定，
            // 不描述写终态本身）。
            return ErrV(ErrorCode::ProtocolError, "response correlation mismatch");
        }
        auto built = ResponseToSessionFrame(response, session->epoch);
        if (!built) {
            return result<void>::err(built.error());
        }
        frame = std::move(built.value());
        frame.traffic = impl->lane;
    }
    auto bytes = EncodeFramedSessionFrame(frame, session->owner->options.MaxFrameBytes());
    if (!bytes) {
        return result<void>::err(bytes.error());
    }
    SessionCore::OutItem item;
    item.bytes.assign(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
    item.lane = frame.traffic;
    item.control_response = true;
    item.deadline = write_deadline;
    item.settle = std::make_shared<SessionCore::OutItem::Settle>();
    auto settle = item.settle;
    auto enqueued = session->Enqueue(std::move(item));
    if (!enqueued) {
        return enqueued;
    }
    // Complete 的成功终态对应 socket 真实写终态：等到 WriterLoop 结算
    // 而非入队即返回。等待与入队共用请求的有限预算（无界时封顶 30s），
    // 不另起计时。
    const auto wait_deadline =
        write_deadline == Clock::time_point::max() ? Clock::now() + 30s : std::min(write_deadline, Clock::now() + 30s);
    const auto status = settle->waiter->Wait({wait_deadline});
    if (status == WaitStatus::Completed) {
        return settle->outcome;
    }
    if (status == WaitStatus::Cancelled) {
        return ErrV(ErrorCode::Cancelled, "reply write wait cancelled");
    }
    if (status == WaitStatus::TimedOut) {
        // 预算内未观察到写终态：写责任可能仍在途，由 infra 继续结算，
        // 但本调用不得伪报成功。
        return ErrV(ErrorCode::TimedOut, "reply write not settled in budget");
    }
    // InvalidContext/AlreadyWaiting/RuntimeUnavailable：无法观察终态的
    // 等待基础设施失败——不是写成功，写责任仍由 WriterLoop/Kill 结算。
    return ErrV(ErrorCode::Unavailable, "reply write wait unavailable");
}

bbt_co_async(result<void>) ReplyCompletion::Fail(Error error)
{
    auto impl = m_impl;
    if (!impl) {
        return ErrV(ErrorCode::InvalidArgument, "reply completion invalid");
    }
    if (!g_bbt_tls_coroutine_co) {
        return ErrV(ErrorCode::InvalidContext, "reply completion requires coroutine context");
    }
    if (impl->terminal.exchange(true)) {
        return ErrV(ErrorCode::InvalidArgument, "reply completion already terminal");
    }
    auto session = impl->session.lock();
    if (!session || session->dead.load()) {
        return ErrV(ErrorCode::Closed, "session closed before reply");
    }
    const auto write_deadline = impl->reply_deadline;
    SessionFrame frame;
    frame.session_epoch = session->epoch;
    frame.has_outcome = true;
    frame.success = false;
    frame.error = std::move(error);
    frame.reply = ReplyMode::Required; // wire 约束：Call/Response 必 Required
    if (impl->kind == FrameKind::SendAccepted) {
        frame.kind = FrameKind::SendAccepted;
        frame.traffic = TrafficClass::System;
        frame.message_id = impl->message_id;
        frame.reply = ReplyMode::None; // 非 Call/Response kind 必 None
    } else {
        frame.kind = FrameKind::Response;
        frame.traffic = impl->lane;
        frame.request_id = impl->request_id;
        frame.service = impl->service;
        frame.method = impl->method;
        frame.request_schema = impl->request_schema;
        frame.response_schema = impl->response_schema;
    }
    auto bytes = EncodeFramedSessionFrame(frame, session->owner->options.MaxFrameBytes());
    if (!bytes) {
        return result<void>::err(bytes.error());
    }
    SessionCore::OutItem item;
    item.bytes.assign(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
    item.lane = frame.traffic;
    item.control_response = true;
    item.deadline = write_deadline;
    item.settle = std::make_shared<SessionCore::OutItem::Settle>();
    auto settle = item.settle;
    auto enqueued = session->Enqueue(std::move(item));
    if (!enqueued) {
        return enqueued;
    }
    // Fail 同 Complete——成功终态对应 socket 真实写终态。
    const auto wait_deadline =
        write_deadline == Clock::time_point::max() ? Clock::now() + 30s : std::min(write_deadline, Clock::now() + 30s);
    const auto status = settle->waiter->Wait({wait_deadline});
    if (status == WaitStatus::Completed) {
        return settle->outcome;
    }
    if (status == WaitStatus::Cancelled) {
        return ErrV(ErrorCode::Cancelled, "reply write wait cancelled");
    }
    if (status == WaitStatus::TimedOut) {
        return ErrV(ErrorCode::TimedOut, "reply write not settled in budget");
    }
    return ErrV(ErrorCode::Unavailable, "reply write wait unavailable");
}

namespace {

// reader 上的 admission 拒绝路径——唯一 reader 不能调用会挂起等写终态
// 的 Complete/Fail（背压会阻塞 reader，无法继续解后续 Response/System 帧）。
// 本函数直接构造错误终态帧入队，不等待写终态：写责任仍由 WriterLoop/Kill
// 在真实终态唯一结算，reader 立即返回继续 Read。terminal 已在本路径
// 置位（调用方先取 IsTerminal），这里不再二次判定。
void PostReplyErrorNoWait(const std::shared_ptr<SessionCore>& session, const detail::ReplyCompletionImpl& impl,
                          Error error)
{
    if (!session || session->dead.load()) {
        return;
    }
    SessionFrame frame;
    frame.session_epoch = session->epoch;
    frame.has_outcome = true;
    frame.success = false;
    frame.error = std::move(error);
    frame.reply = ReplyMode::Required;
    if (impl.kind == FrameKind::SendAccepted) {
        frame.kind = FrameKind::SendAccepted;
        frame.traffic = TrafficClass::System;
        frame.message_id = impl.message_id;
        frame.reply = ReplyMode::None;
    } else {
        frame.kind = FrameKind::Response;
        frame.traffic = impl.lane;
        frame.request_id = impl.request_id;
        frame.service = impl.service;
        frame.method = impl.method;
        frame.request_schema = impl.request_schema;
        frame.response_schema = impl.response_schema;
    }
    auto bytes = EncodeFramedSessionFrame(frame, session->owner->options.MaxFrameBytes());
    if (!bytes) {
        return;
    }
    SessionCore::OutItem item;
    item.bytes.assign(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
    item.lane = frame.traffic;
    item.control_response = true;
    item.deadline = Clock::now() + 5s;
    item.settle = std::make_shared<SessionCore::OutItem::Settle>();
    // 不挂 reader：写终态由 settle/WriterLoop 结算，不入队失败也只是
    // 该条回复终态丢失——link 与后续帧继续进展。
    (void)session->Enqueue(std::move(item));
}

} // namespace

bool ReplyCompletion::IsTerminal() const noexcept
{
    return !m_impl || m_impl->terminal.load();
}

std::shared_ptr<detail::ReplyCompletionImpl> ReplyCompletion::MarkTerminalForReject() noexcept
{
    if (!m_impl || m_impl->terminal.exchange(true)) {
        return nullptr;
    }
    return m_impl;
}

bool ReplyCompletion::ForSendAcceptance() const noexcept
{
    return m_impl && m_impl->kind == FrameKind::SendAccepted;
}

bool AdmissionLease::Release() noexcept
{
    auto impl = m_impl;
    if (!impl || impl->released.exchange(true)) {
        return false;
    }
    if (auto server = impl->server.lock()) {
        std::lock_guard<std::mutex> lock(server->mu);
        server->NoteInflight(LaneIndex(impl->lane), -1);
        ++server->finished[LaneIndex(impl->lane)];
    }
    return true;
}

bool AdmissionLease::IsReleased() const noexcept
{
    return !m_impl || m_impl->released.load();
}

namespace {

// ============================================================================
// Handshake helpers（trusted-network plaintext，ADR §3.2/§3.4）
// ============================================================================

// 受限握手阶段 I/O：直接在 conn 上 Write/Read（未建 lane worker）。
// Hello/HelloAck 帧上限 min(max_frame,64KiB) 由 codec 在 hello_phase
// 前缀阶段强制。先耗 decoder 缓存再 Read；同批 Read 只消费到「下一个
// 完整握手帧」为止，排在其后的字节经 HandshakeStash 移交调用方——调用
// 方在验证握手边界的时刻切数据态后按数据限额补喂（合法最终
// HelloAck+大首数据帧合包不被握手 pending 界误拒；未验证前这些字节
// 尚未进 decoder，大声明仍按握手限额拒绝）。

// 同批 Read 解出握手帧后、decoder 里尚未耗尽的余字节（排在握手帧之后
// 的合包数据）。由调用方持有到握手边界验证完成，再按数据态补喂——
// 验证前这些字节不属任何阶段限额，未验证不进入 decoder。
struct HandshakeStash {
    std::vector<std::uint8_t> bytes; // owning 余量（可为空）
    bool Empty() const noexcept
    {
        return bytes.empty();
    }
};

// ReadHandshakeFrame 的增量补喂块大小：两次 Next() 之间 decoder 缓冲只
// 容纳不完整单帧（声明≤64KiB+4），32KiB 块使 buffer+take 恒小于握手
// pending 界（4+2*64KiB），合法帧头不触硬界、非法大声明仍在解码点被拒。
constexpr std::size_t kHandshakeFeedChunk = 32 * 1024;

// 读一帧握手帧。同批 Read 只消费到「下一个完整握手帧」为止——解码成功
// 的瞬间把 decoder 里尚未耗尽的同批余字节 TakePending 进 stash 返回，
// 由调用方在握手边界验证完成后按数据态补喂（合法最终 HelloAck+
// 大首数据帧合包不被握手 pending 界误拒；未验证前这些字节未进
// decoder，大声明仍按握手限额拒绝）。stash 无跨批语义，每帧调用清零。
result<SessionFrame> ReadHandshakeFrame(net::ICoConnection& conn, SessionFrameDecoder& decoder,
                                        bbt::coroutine::Deadline deadline, HandshakeStash& stash)
{
    // 帧接受门禁：kind 校验 + 已喂未解余字节移交 stash。stash 内字节序 =
    // 字节流序（decoder 余量在前，其后是调用方补喂的未喂输入）。
    const auto accept = [&](SessionFrame&& frame) -> result<SessionFrame> {
        if (frame.kind != FrameKind::Hello && frame.kind != FrameKind::HelloAck
            && frame.kind != FrameKind::SessionError) {
            return result<SessionFrame>::err(Err(ErrorCode::ProtocolError, "non-handshake frame during handshake"));
        }
        if (decoder.PendingBytes() != 0) {
            // decoder 余量在字节流序中先于 stash 中尚未喂入的尾部——前置
            // 而非覆盖，否则 stash 分支（本调用已从 stash 喂过一块）丢字节。
            auto pending = decoder.TakePending();
            if (!stash.bytes.empty()) {
                pending.insert(pending.end(), std::make_move_iterator(stash.bytes.begin()),
                               std::make_move_iterator(stash.bytes.end()));
            }
            stash.bytes = std::move(pending);
        }
        return result<SessionFrame>::ok(std::move(frame));
    };
    for (;;) {
        if (const auto gate = CheckCall(deadline)) {
            return result<SessionFrame>::err(CallError(gate, "handshake timed out", false));
        }
        // 先耗缓存：上次已移交的字节先解。一帧到手即返回。
        {
            auto cached = decoder.Next();
            if (!cached) {
                return result<SessionFrame>::err(cached.error());
            }
            if (cached.value().has_value()) {
                return accept(std::move(cached.value().value()));
            }
        }
        // 再耗 stash：同批未验证余字节仍按握手限额增量补喂（与下方 Read
        // 批同规则）；解出首个帧即停，未喂余量留在 stash 交调用方裁决。
        while (!stash.Empty()) {
            const std::size_t take = std::min(stash.bytes.size(), kHandshakeFeedChunk);
            const auto fed = decoder.Feed(stash.bytes.data(), take);
            if (!fed) {
                return result<SessionFrame>::err(Err(ErrorCode::ProtocolError, "handshake stash decode rejected"));
            }
            stash.bytes.erase(stash.bytes.begin(), stash.bytes.begin() + take);
            auto nxt = decoder.Next();
            if (!nxt) {
                return result<SessionFrame>::err(nxt.error());
            }
            if (nxt.value().has_value()) {
                return accept(std::move(nxt.value().value()));
            }
        }
        auto [read_data, read_error] = conn.Read({deadline});
        if (!read_error) {
            // 同一 Read 批不整批先按 hello_phase Feed——分段内按
            // kHandshakeFeedChunk 块补喂，每块后尝试解帧；解出首个握手帧即
            // 停喂，本批未喂字节排在 decoder 余量之后移交 stash（由调用方
            // 在边界验证后按数据态补喂）。decoder 缓冲在两次 Next 之间只
            // 容纳不完整的单帧（≤64KiB+4），单块补喂不会触达握手 pending
            // 界；合法最终 HelloAck+大首数据帧合包因此不被误拒。
            const auto& segments = read_data.Segments();
            for (std::size_t si = 0; si < segments.size(); ++si) {
                const auto* base = reinterpret_cast<const std::uint8_t*>(segments[si].data);
                std::size_t offset = 0;
                while (offset < segments[si].size) {
                    const std::size_t take = std::min(segments[si].size - offset, kHandshakeFeedChunk);
                    const auto fed = decoder.Feed(base + offset, take);
                    if (!fed) {
                        return result<SessionFrame>::err(
                            Err(ErrorCode::ProtocolError, "handshake frame decode rejected"));
                    }
                    offset += take;
                    auto nxt = decoder.Next();
                    if (!nxt) {
                        return result<SessionFrame>::err(nxt.error());
                    }
                    if (nxt.value().has_value()) {
                        auto accepted = accept(std::move(nxt.value().value()));
                        if (!accepted) {
                            return accepted;
                        }
                        // 未喂输入排在 decoder 余量之后（字节流序）。
                        stash.bytes.insert(stash.bytes.end(), base + offset, base + segments[si].size);
                        for (std::size_t sj = si + 1; sj < segments.size(); ++sj) {
                            const auto* tail = reinterpret_cast<const std::uint8_t*>(segments[sj].data);
                            stash.bytes.insert(stash.bytes.end(), tail, tail + segments[sj].size);
                        }
                        return accepted;
                    }
                }
            }
            continue;
        }
        if (read_error == bbt::infra::net::make_error_code(bbt::infra::net::NetErrc::Eof)) {
            return result<SessionFrame>::err(Err(ErrorCode::Unavailable, "peer closed during handshake"));
        }
        return result<SessionFrame>::err(CallError(read_error, "handshake read failed", false));
    }
}

// 验证最终握手帧完成后的边界：切数据态并把 stash 余字节按数据限额
// 一次性补喂回 decoder。调用前提：ack/echo 已逐项验证——此后这些字节
// 按数据帧上限判定（数据 pending 界 4+2*max_frame 可容纳整批）。握手
// 中途的 stash 补喂不经过本函数：ReadHandshakeFrame 内按
// kHandshakeFeedChunk 增量消耗，超界 stash 不会撞握手 pending 硬界。
result<void> CommitStashAfterFinalAck(SessionFrameDecoder& decoder, HandshakeStash& stash)
{
    decoder.SetHelloPhase(false);
    if (stash.Empty()) {
        return result<void>::ok();
    }
    const auto fed = decoder.Feed(stash.bytes.data(), stash.bytes.size());
    stash.bytes.clear();
    if (!fed) {
        return result<void>::err(Err(ErrorCode::ProtocolError, "handshake stash decode rejected"));
    }
    return result<void>::ok();
}

result<void> WriteHandshakeFrame(net::ICoConnection& conn, const SessionFrame& frame, bbt::coroutine::Deadline deadline,
                                 std::size_t max_frame)
{
    auto bytes = EncodeFramedSessionFrame(frame, max_frame);
    if (!bytes) {
        return result<void>::err(bytes.error());
    }
    std::string out(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
    const auto report = conn.Write(std::move(out), {deadline});
    return detail::TcpSettleWrite(report, "handshake write failed");
}

SessionFrame MakeHello(const ClientOptions& options, std::uint64_t epoch, std::uint64_t nonce)
{
    SessionFrame frame;
    frame.kind = FrameKind::Hello;
    frame.traffic = TrafficClass::System;
    frame.peer_identity = options.LocalIdentity();
    frame.session_epoch = epoch;
    frame.connection_nonce = nonce;
    return frame;
}

SessionFrame MakeHelloAck(const ClientOptions& options, std::uint64_t epoch, std::uint64_t peer_nonce)
{
    SessionFrame frame;
    frame.kind = FrameKind::HelloAck;
    frame.traffic = TrafficClass::System;
    frame.peer_identity = options.LocalIdentity();
    frame.session_epoch = epoch;
    frame.connection_nonce = peer_nonce;
    return frame;
}

// 出站方向（拨号侧）：写 Hello → 读 Hello → epoch owner 决定 epoch →
// 写 HelloAck → 读 HelloAck echo。identity 较小一侧是 epoch owner。
// 成功时 out_decoder 收编握手 decoder（含未耗尽的合包余帧），供 session
// live reader 继续消费（余帧移交）。
result<std::uint64_t> OutboundHandshake(net::ICoConnection& conn, const ClientOptions& options, std::uint64_t proposal,
                                        std::uint64_t nonce, std::string& out_peer_identity,
                                        SessionFrameDecoder& out_decoder, bbt::coroutine::Deadline deadline)
{
    using R = result<std::uint64_t>;
    HandshakeStash stash;
    auto hello = MakeHello(options, proposal, nonce);
    if (auto wrote = WriteHandshakeFrame(conn, hello, deadline, options.MaxFrameBytes()); !wrote) {
        return R::err(wrote.error());
    }
    SessionFrameDecoder decoder(options.MaxFrameBytes());
    auto peer_hello = ReadHandshakeFrame(conn, decoder, deadline, stash);
    if (!peer_hello) {
        return R::err(peer_hello.error());
    }
    if (peer_hello.value().kind != FrameKind::Hello) {
        return R::err(Err(ErrorCode::ProtocolError, "expected Hello from peer"));
    }
    const auto& remote = peer_hello.value();
    if (remote.peer_identity == options.LocalIdentity()) {
        return R::err(Err(ErrorCode::ProtocolError, "self-connection rejected"));
    }
    out_peer_identity = remote.peer_identity;
    const bool i_own_epoch = options.LocalIdentity() < remote.peer_identity;
    std::uint64_t epoch = 0;
    if (i_own_epoch) {
        epoch = proposal; // epoch owner：本端 proposal 即 selected。
        auto ack = MakeHelloAck(options, epoch, remote.connection_nonce);
        if (auto wrote = WriteHandshakeFrame(conn, ack, deadline, options.MaxFrameBytes()); !wrote) {
            return R::err(wrote.error());
        }
    }
    // 第一帧取出时若有同批余字节（合包 HelloAck 前缀/数据帧等），留在
    // stash——ReadHandshakeFrame 内按握手限额增量补喂，不整批硬喂。
    auto peer_ack = ReadHandshakeFrame(conn, decoder, deadline, stash);
    if (!peer_ack) {
        return R::err(peer_ack.error());
    }
    if (peer_ack.value().kind != FrameKind::HelloAck) {
        return R::err(Err(ErrorCode::ProtocolError, "expected HelloAck from peer"));
    }
    const auto& ack = peer_ack.value();
    if (ack.peer_identity != remote.peer_identity) {
        return R::err(Err(ErrorCode::ProtocolError, "HelloAck identity mismatch"));
    }
    if (i_own_epoch) {
        if (ack.session_epoch != epoch || ack.connection_nonce != nonce) {
            return R::err(Err(ErrorCode::ProtocolError, "HelloAck echo mismatch"));
        }
        // 验证完成的边界：切数据态后 stash 余字节按数据限额补喂。
        if (auto committed = CommitStashAfterFinalAck(decoder, stash); !committed) {
            return R::err(committed.error());
        }
        out_decoder = std::move(decoder);
        return R::ok(epoch);
    }
    // 对端是 epoch owner：原样 echo selected epoch。
    epoch = ack.session_epoch;
    auto echo = MakeHelloAck(options, epoch, remote.connection_nonce);
    if (auto wrote = WriteHandshakeFrame(conn, echo, deadline, options.MaxFrameBytes()); !wrote) {
        return R::err(wrote.error());
    }
    if (auto committed = CommitStashAfterFinalAck(decoder, stash); !committed) {
        return R::err(committed.error());
    }
    out_decoder = std::move(decoder);
    return R::ok(epoch);
}

// 入站方向（接纳侧）：读 Hello → 写 Hello → 同样规则。
// out_decoder 同 OutboundHandshake：成功时收编含余帧的握手 decoder。
result<std::uint64_t> InboundHandshake(net::ICoConnection& conn, const ClientOptions& options, std::uint64_t proposal,
                                       std::uint64_t nonce, std::string& out_peer_identity,
                                       SessionFrameDecoder& out_decoder, bbt::coroutine::Deadline deadline)
{
    using R = result<std::uint64_t>;
    HandshakeStash stash;
    SessionFrameDecoder decoder(options.MaxFrameBytes());
    auto peer_hello = ReadHandshakeFrame(conn, decoder, deadline, stash);
    if (!peer_hello) {
        return R::err(peer_hello.error());
    }
    if (peer_hello.value().kind != FrameKind::Hello) {
        return R::err(Err(ErrorCode::ProtocolError, "expected Hello from inbound peer"));
    }
    const auto& remote = peer_hello.value();
    if (remote.peer_identity == options.LocalIdentity()) {
        return R::err(Err(ErrorCode::ProtocolError, "self-connection rejected"));
    }
    out_peer_identity = remote.peer_identity;
    auto hello = MakeHello(options, proposal, nonce);
    if (auto wrote = WriteHandshakeFrame(conn, hello, deadline, options.MaxFrameBytes()); !wrote) {
        return R::err(wrote.error());
    }
    const bool i_own_epoch = options.LocalIdentity() < remote.peer_identity;
    std::uint64_t epoch = 0;
    if (i_own_epoch) {
        epoch = proposal;
        auto ack = MakeHelloAck(options, epoch, remote.connection_nonce);
        if (auto wrote = WriteHandshakeFrame(conn, ack, deadline, options.MaxFrameBytes()); !wrote) {
            return R::err(wrote.error());
        }
        auto peer_ack = ReadHandshakeFrame(conn, decoder, deadline, stash);
        if (!peer_ack) {
            return R::err(peer_ack.error());
        }
        if (peer_ack.value().kind != FrameKind::HelloAck || peer_ack.value().session_epoch != epoch
            || peer_ack.value().connection_nonce != nonce || peer_ack.value().peer_identity != remote.peer_identity) {
            return R::err(Err(ErrorCode::ProtocolError, "HelloAck echo mismatch"));
        }
        // 验证完成边界：切数据态，stash 余字节按数据限额补喂。
        if (auto committed = CommitStashAfterFinalAck(decoder, stash); !committed) {
            return R::err(committed.error());
        }
        out_decoder = std::move(decoder);
        return R::ok(epoch);
    }
    // 对端是 epoch owner：先读 selected epoch，再 echo。
    auto peer_ack = ReadHandshakeFrame(conn, decoder, deadline, stash);
    if (!peer_ack) {
        return R::err(peer_ack.error());
    }
    if (peer_ack.value().kind != FrameKind::HelloAck || peer_ack.value().connection_nonce != nonce
        || peer_ack.value().peer_identity != remote.peer_identity) {
        return R::err(Err(ErrorCode::ProtocolError, "HelloAck echo mismatch"));
    }
    epoch = peer_ack.value().session_epoch;
    auto echo = MakeHelloAck(options, epoch, remote.connection_nonce);
    if (auto wrote = WriteHandshakeFrame(conn, echo, deadline, options.MaxFrameBytes()); !wrote) {
        return R::err(wrote.error());
    }
    if (auto committed = CommitStashAfterFinalAck(decoder, stash); !committed) {
        return R::err(committed.error());
    }
    out_decoder = std::move(decoder);
    return R::ok(epoch);
}

// 会话启动：握手完成后装注册表 + 去重，再启动 reader/writer 协程。
// 返回可交付调用的 session；输家 link 已关闭时返回 err。
// 会话额度（MaxSessions）在这里同一 owner->mu 检查+占位——by_identity
// 成员与建立中的占位 session 都计入；未过检查不产生任何长期 session。
// 同时拨号选举（ADR §3.3）：identity 较小侧保留其本地发起 link。
result<std::shared_ptr<SessionCore>>
InstallSession(std::shared_ptr<SessionOwnerState> owner, std::shared_ptr<SessionCore> session,
               const std::string& endpoint_key, SessionFrameDecoder&& decoder,
               const std::shared_ptr<SessionOwnerState::Establishing>& establishing = nullptr)
{
    using R = result<std::shared_ptr<SessionCore>>;
    std::shared_ptr<SessionCore> evicted;
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        if (owner->closed) {
            return R::err(Err(ErrorCode::Closed, "owner closed"));
        }
        if (auto existing = owner->by_identity.find(session->peer_identity); existing != owner->by_identity.end()) {
            auto& winner = existing->second;
            const bool keep_outbound = owner->options.LocalIdentity() < session->peer_identity;
            if (keep_outbound == session->outbound) {
                // 本 link 胜出：旧 session 退役。退役额度先不移交（仍由
                // evicted 持有到其 Released）；替换本就不新增净占用。
                evicted = winner;
                winner = session;
            } else {
                // 本 link 输家：关闭（fencing），映射到胜者。握手余帧随
                // 输家 decoder 一并销毁——不再属于任何 live reader。
                session->dead.store(true);
                if (session->conn.Raw().Valid()) {
                    session->conn.RequestClose(std::make_error_code(std::errc::connection_reset));
                }
                if (!endpoint_key.empty()) {
                    owner->by_endpoint[endpoint_key] = winner;
                }
                return R::ok(winner);
            }
        } else {
            // MaxSessions 真实占用——已安装成员 + 其他建立中占位都
            // 计入；本 establishing 的 reserved 槽位在安装成功时转化为
            // by_identity 成员（-1 排除自身），不占双份。退役实际完成
            // （ReleasedLocked）才归还额度。
            std::size_t occupied = owner->by_identity.size();
            for (const auto& [key, est] : owner->establishing) {
                (void)key;
                if (est->reserved) {
                    ++occupied;
                }
            }
            if (establishing && establishing->reserved) {
                --occupied;
            }
            if (occupied >= owner->options.MaxSessions()) {
                return R::err(Err(ErrorCode::Overloaded, "max sessions reached"));
            }
            if (establishing) {
                establishing->reserved = false;
            }
            owner->by_identity[session->peer_identity] = session;
        }
        session->quota_held.store(true);
        if (!endpoint_key.empty()) {
            owner->by_endpoint[endpoint_key] = session;
        }
        // 胜者接管被退役者的 endpoint 映射：被 fencing 的 session 曾占用
        // 的拨号键改指赢家——调用方重取会话时落到存活 link，不会因旧键
        // 失效触发对同一 PeerKey 的第三次拨号。
        if (evicted) {
            for (auto& [key, weak] : owner->by_endpoint) {
                if (auto prev = weak.lock(); prev && prev.get() == evicted.get()) {
                    weak = session;
                }
            }
        }
        // 胜者收编握手 decoder（含合包余帧）——输家路径上面已提前返回。
        session->decoder = std::move(decoder);
        session->decoder.SetHelloPhase(false);
        owner->links.fetch_add(1);
        owner->sessions.fetch_add(1);
    }
    if (evicted) {
        evicted->Kill({});
    }
    // reader+writer 两份协程责任 + 一份物理连接责任（任一路退出时一并
    // 结算——写失败 Kill 与读终态 Kill 同义）。物理责任 ExitDone 结算在
    // 最后一份协程责任之后：conn.IsReleased() 由该点观察。
    session->responsibilities.fetch_add(1);
    if (!session->Submit(128 * 1024, [s = session] { s->ReaderLoop(); })
        || !session->Submit(128 * 1024, [s = session] { s->WriterLoop(); })) {
        session->Kill({});
        return R::err(Err(ErrorCode::RuntimeUnavailable, "session task registration failed"));
    }
    return R::ok(session);
}

// 拨号 + 握手（在调用方受管协程内执行）。establishing 记录由 GetSession
// 同事务创建并置 reserved（建立中额度占位），这里透传给 InstallSession——
// 安装成功时 reserved 转化为 by_identity 成员额度。
result<std::shared_ptr<SessionCore>>
EstablishOutbound(std::shared_ptr<SessionOwnerState> owner, const net::Address& target, const std::string& endpoint_key,
                  const std::shared_ptr<SessionOwnerState::Establishing>& establishing,
                  bbt::coroutine::Deadline deadline)
{
    using R = result<std::shared_ptr<SessionCore>>;
    const auto dialed = owner->engine->CoDial(target, {deadline});
    if (dialed.error) {
        return R::err(CallError(dialed.error, "connect failed", false));
    }
    net::CoTcpConnection conn = dialed.connection;
    {
        // 建立中 conn 挂进占位记录：owner 封口可即时 RequestClose（物理关闭责任）。
        std::lock_guard<std::mutex> lock(owner->mu);
        establishing->conn = conn.Raw();
        if (owner->closed) {
            conn.RequestClose();
            return R::err(Err(ErrorCode::Closed, "owner closed"));
        }
    }
    // 固定动作检查点：拨号已成功、额度已占，link 还没发布。Pause 在库内
    // 分片等待；失败/注入转成可见错误后，establishing 记录、额度与 conn 由
    // GetSession 的共同收尾归还——异常不越过该收尾块，也不在这里另造一条
    // 半截清理路径。
    if (const auto gate = RunHookCheckpoint(*owner->hooks, HookPoint::OutboundBeforeHandshake,
                                            HookGate{owner.get(), nullptr, nullptr, &conn, deadline});
        !gate) {
        conn.RequestClose();
        return R::err(gate.error());
    }
    std::string peer_identity;
    const auto proposal = NextEpoch(*owner);
    const auto nonce = NextNonce(*owner);
    SessionFrameDecoder decoder(owner->options.MaxFrameBytes());
    auto epoch = OutboundHandshake(conn, owner->options, proposal, nonce, peer_identity, decoder, deadline);
    if (!epoch) {
        conn.RequestClose();
        return R::err(epoch.error());
    }
    auto session = std::make_shared<SessionCore>();
    session->owner = owner;
    session->conn = std::move(conn);
    session->peer_identity = std::move(peer_identity);
    session->endpoint = endpoint_key;
    session->outbound = true;
    session->epoch = epoch.value();
    auto installed = InstallSession(owner, session, endpoint_key, std::move(decoder), establishing);
    if (!installed) {
        // 安装拒绝（owner 封口/额度满）：拨号 conn 未发布，本端唯一责任
        // 人直接 RequestClose——不留给 engine 兜底。
        session->conn.RequestClose();
    }
    return installed;
}

// 获取或建立 session（在调用方协程内；并发同 target 只建一次）。
// 并发等待：CoWaiter 单等待位，不能放共享 Establishing 上——每个等待者
// 用自己的 CoWaiter 轮询 done 位（建立通常毫秒级完成）。
result<std::shared_ptr<SessionCore>> GetSession(std::shared_ptr<SessionOwnerState> owner,
                                                const std::string& endpoint_key, const net::Address& target,
                                                bbt::coroutine::Deadline deadline)
{
    using R = result<std::shared_ptr<SessionCore>>;
    std::shared_ptr<SessionOwnerState::Establishing> mine;
    bool establisher = false;
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        if (owner->closed) {
            return R::err(Err(ErrorCode::Closed, "owner closed"));
        }
        // 每次取会话都在 owner->mu 下顺带重驱各 link 的物理 Released
        // 归还（幂等）——conn 物理 Released 晚于 ExitDone 归零时，新调用
        // 是天然的收口观察点，不必另起清扫线程。先收集再驱动：ReleasedLocked
        // 会从 by_identity 擦除条目，持锁遍历中不能边迭代边擦除。
        std::vector<std::shared_ptr<SessionCore>> live;
        live.reserve(owner->by_identity.size());
        for (auto& [id, s] : owner->by_identity) {
            (void)id;
            live.push_back(s);
        }
        for (auto& s : live) {
            s->DriveReleaseLocked();
        }
        if (auto it = owner->by_endpoint.find(endpoint_key); it != owner->by_endpoint.end()) {
            if (auto session = it->second.lock(); session && !session->dead.load()) {
                return R::ok(session);
            }
            owner->by_endpoint.erase(it);
        }
        if (auto it = owner->establishing.find(endpoint_key); it == owner->establishing.end()) {
            // 建立责任在拨号前的同一事务内先占额度：已安装成员 +
            // 其他 reserved 建立中记录达到 MaxSessions 时直接
            // Overloaded——不拨号、不产生物理连接、不消耗握手责任。
            // 同 endpoint 的等待者共用本条记录不另占位（不双占）。
            std::size_t occupied = owner->by_identity.size();
            for (const auto& [key, est] : owner->establishing) {
                (void)key;
                if (est->reserved) {
                    ++occupied;
                }
            }
            if (occupied >= owner->options.MaxSessions()) {
                return R::err(Err(ErrorCode::Overloaded, "max sessions reached (pre-dial)"));
            }
            mine = std::make_shared<SessionOwnerState::Establishing>();
            mine->reserved = true; // 建立中额度占位（同事务）
            owner->establishing[endpoint_key] = mine;
            establisher = true;
        } else {
            mine = it->second;
        }
    }
    if (!establisher) {
        auto self = CoWaiter::Create();
        for (;;) {
            {
                std::lock_guard<std::mutex> lock(owner->mu);
                if (mine->done) {
                    if (mine->session && !mine->session->dead.load()) {
                        return R::ok(mine->session);
                    }
                    return R::err(mine->error);
                }
            }
            if (Clock::now() >= deadline) {
                return R::err(
                    CallError(std::make_error_code(std::errc::timed_out), "session establish wait timed out", false));
            }
            const auto status = self->Wait({std::min(deadline, Clock::now() + 10ms)});
            if (status == WaitStatus::Cancelled) {
                return R::err(Err(ErrorCode::Cancelled, "session establish wait cancelled"));
            }
        }
    }
    // 本协程执行拨号。
    // 异常全路径归还：EstablishOutbound 内部已把检查点注入与失败转成可见
    // 错误，这里再兜一层——任何逃出的异常都不能跳过下面的收尾块，否则
    // establishing 记录、reserved 额度与 done 位会永久留下。异常转成
    // InternalError 返回，收尾块照常执行。
    result<std::shared_ptr<SessionCore>> made = R::err(Err(ErrorCode::InternalError, "establish did not run"));
    try {
        made = EstablishOutbound(owner, target, endpoint_key, mine, deadline);
    } catch (const std::exception& error) {
        made = R::err(Err(ErrorCode::InternalError, std::string("session establish threw: ") + error.what()));
    } catch (...) {
        made = R::err(Err(ErrorCode::InternalError, "session establish threw"));
    }
    net::Connection abandoned;
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        if (made) {
            mine->session = made.value();
        } else {
            mine->error = made.error();
            // 失败/异常路径的物理关闭责任兜底：EstablishOutbound 的正常失败
            // 分支已自行 RequestClose（幂等），这里覆盖「异常逃出、还没关」
            // 的情况——建立中 conn 不能因为记录被擦除而失去关闭责任人。
            abandoned = mine->conn;
        }
        // 归还顺序（建立中责任）：conn 句柄先从记录上移除，使本条
        // shared_ptr 在临界区外随擦除析构——物理释放在额度归还前
        // 完成；reserved 占位随记录消亡，不是独立计数。擦除只匹配本次
        // 建立记录，不误删同名键上后来的建立者。
        mine->conn = net::Connection{};
        if (auto it = owner->establishing.find(endpoint_key); it != owner->establishing.end() && it->second == mine) {
            owner->establishing.erase(it);
        }
        mine->done = true;
    }
    if (abandoned.Valid()) {
        abandoned.RequestClose();
    }
    return made;
}

// ============================================================================
// ServerV2 accept（入站 link → 握手 → owner session）
// ============================================================================

void ServeInbound(std::shared_ptr<ServerV2State> server, net::CoTcpConnection conn)
{
    const auto conn_id = conn.Raw().Id();
    const auto deadline = Clock::now() + server->options.IncomingTimeout();
    auto owner = server->owner;
    // 固定动作检查点：conn 已 accept、握手还没开始。失败时本条握手责任仍
    // 在本协程——关闭 conn 并移除台账记录，不把退役留给更外层的兜底 catch。
    if (const auto gate = RunHookCheckpoint(*owner->hooks, HookPoint::InboundBeforeHandshake,
                                            HookGate{owner.get(), server.get(), nullptr, &conn, deadline});
        !gate) {
        conn.RequestClose();
        std::lock_guard<std::mutex> lock(server->mu);
        server->handshakes.erase(conn_id);
        server->cv.notify_all();
        return;
    }
    std::string peer_identity;
    const auto proposal = NextEpoch(*owner);
    const auto nonce = NextNonce(*owner);
    SessionFrameDecoder decoder(owner->options.MaxFrameBytes());
    auto epoch =
        detail::InboundHandshakeOnConnection(conn, owner->options, proposal, nonce, peer_identity, decoder, deadline);
    // 握手 conn 责任随本协程结束（任一路径）：台账唯一移除点。
    const auto finish = [&] {
        std::lock_guard<std::mutex> lock(server->mu);
        server->handshakes.erase(conn_id);
        server->cv.notify_all();
    };
    if (!epoch) {
        conn.RequestClose();
        finish();
        return;
    }
    auto session = std::make_shared<SessionCore>();
    session->owner = owner;
    session->server = server;
    session->conn = std::move(conn);
    session->peer_identity = std::move(peer_identity);
    session->outbound = false;
    session->epoch = epoch.value();
    // 第二个挂点：握手完成、link 还没进台账——封口与发布在这里竞争。
    // 失败时 conn 已在 session 上、台账记录仍在，两者都要退；这里就地收尾，
    // 因为外层 catch 只持有原始的 conn 局部量，不能替已移交的 session 关闭。
    if (const auto gate = RunHookCheckpoint(*owner->hooks, HookPoint::InboundBeforePublish,
                                            HookGate{owner.get(), server.get(), nullptr, &session->conn, deadline});
        !gate) {
        session->conn.RequestClose();
        finish();
        return;
    }
    // 封口后不发布 link（物理关闭责任）：Close/Seal 与安装竞争时，本握手
    // 必须退役而不是溜进台账成为漏管连接。检查与 InstallSession 同一
    // owner->mu 事务之外不可行（InstallSession 内部已查 owner->closed）；
    // 这里先查 server.closed——Seal 置 closed 后到达的握手直接退役。
    bool publish = false;
    {
        std::lock_guard<std::mutex> lock(server->mu);
        if (server->closed) {
            // finish() 自身取 server->mu——不能在持锁内联调用（自死锁）。
            server->handshakes.erase(conn_id);
            server->cv.notify_all();
        } else {
            publish = true;
        }
    }
    if (!publish) {
        session->conn.RequestClose();
        return;
    }
    auto installed = InstallSession(owner, session, {}, std::move(decoder));
    if (!installed) {
        session->conn.RequestClose();
        finish();
        return;
    }
    auto live = installed.value();
    {
        std::lock_guard<std::mutex> lock(server->mu);
        ++server->accepted;
        server->links.push_back(live);
        // 顺带清扫已死弱引用。
        server->links.erase(
            std::remove_if(server->links.begin(), server->links.end(), [](const auto& w) { return w.expired(); }),
            server->links.end());
        server->handshakes.erase(conn_id);
        server->cv.notify_all();
    }
}

void AcceptLoop(std::shared_ptr<ServerV2State> server)
{
    const auto exit_loop = [&] {
        std::lock_guard<std::mutex> lock(server->mu);
        server->accept_loop_exited = true;
        server->cv.notify_all();
    };
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(server->mu);
            if (server->stopping) {
                // exit_loop 自身取 server->mu——持锁内联调用会自死锁；
                // 标记与唤醒直接在本临界区内联完成。
                server->accept_loop_exited = true;
                server->cv.notify_all();
                return;
            }
        }
        auto accepted = server->engine->CoAccept({Clock::now() + 50ms});
        if (accepted.error == std::errc::timed_out) {
            continue;
        }
        if (accepted.error) {
            // RequestStopAccepting/Seal 会取消在途 CoAccept：这是正常封口路径，
            // 不是 accept 故障，不得发布失败原因覆盖调用方的 Close/Shutdown。
            bool sealed = false;
            {
                std::lock_guard<std::mutex> lock(server->mu);
                sealed = server->stopping;
            }
            if (sealed) {
                exit_loop();
                return;
            }
            detail::ServerV2Seal(server, Err(ErrorCode::RuntimeUnavailable, "rpc server: accept loop failed"));
            exit_loop();
            return;
        }
        {
            std::lock_guard<std::mutex> lock(server->mu);
            // 握手 conn 与已发布 link 同占 MaxConnections——
            // 建立中不算免费额度。
            if (server->stopping
                || server->links.size() + server->handshakes.size() >= server->options.MaxConnections()) {
                accepted.connection.RequestClose();
                continue;
            }
            // 握手责任早登记：conn.Id() 台账先于任务接管——Close/Seal
            // 能立即看到并 RequestClose 这条建立中连接（物理关闭责任）。
            server->handshakes.emplace(accepted.connection.Raw().Id(), accepted.connection.Raw());
        }
        bool registered = false;
        std::weak_ptr<ServerV2State> weak = server;
        const auto conn_id = accepted.connection.Raw().Id();
        g_scheduler->RegistCoroutineTask(
            128 * 1024,
            [weak, conn = accepted.connection]() mutable {
                if (auto server = weak.lock()) {
                    // 裸协程任务：异常逃逸即 terminate——隔离到本 conn
                    // 退役，不穿透 scheduler。
                    try {
                        ServeInbound(server, conn);
                    } catch (...) {
                        conn.RequestClose();
                        std::lock_guard<std::mutex> lock(server->mu);
                        server->handshakes.erase(conn.Raw().Id());
                        server->cv.notify_all();
                    }
                } else {
                    conn.RequestClose();
                }
            },
            registered);
        if (!registered) {
            accepted.connection.RequestClose();
            std::lock_guard<std::mutex> lock(server->mu);
            server->handshakes.erase(conn_id);
        }
        bbtco_yield;
    }
}

} // namespace

// ============================================================================
// detail/Session.hpp 测试接缝（非成员，转调匿名 InboundHandshake）
// ============================================================================

result<std::uint64_t> detail::InboundHandshakeOnConnection(net::ICoConnection& conn, const ClientOptions& options,
                                                           std::uint64_t proposal, std::uint64_t nonce,
                                                           std::string& out_peer_identity,
                                                           SessionFrameDecoder& out_decoder,
                                                           bbt::coroutine::Deadline deadline)
{
    return InboundHandshake(conn, options, proposal, nonce, out_peer_identity, out_decoder, deadline);
}

std::shared_ptr<SessionHooks> Client::Hooks() const noexcept
{
    return m_owner ? m_owner->hooks : nullptr;
}

// v2 台账快照：links/handshakes/writes 三项在同一把 server 锁内取样。
detail::ServerV2Ledger detail::ServerV2LedgerOf(const std::shared_ptr<ServerV2State>& v2) noexcept
{
    detail::ServerV2Ledger out;
    if (!v2) {
        return out;
    }
    std::lock_guard<std::mutex> lock(v2->mu);
    for (const auto& weak : v2->links) {
        auto session = weak.lock();
        if (!session) {
            continue;
        }
        ++out.links;
        out.writes_outstanding += session->writes_outstanding.load();
    }
    out.handshakes = v2->handshakes.size();
    return out;
}

// ============================================================================
// Client v2 公共方法
// ============================================================================

result<std::shared_ptr<Client>> Client::Create(std::shared_ptr<net::Engine> engine, ClientOptions options)
{
    using R = result<std::shared_ptr<Client>>;
    if (!engine) {
        return R::err(Err(ErrorCode::InvalidArgument, "engine required"));
    }
    if (auto valid = options.Validate(); !valid) {
        return R::err(valid.error());
    }
    if (!options.TrustedNetworkPlaintext()) {
        return R::err(Err(ErrorCode::UnsupportedRoute, "no verifier: trusted-network plaintext required"));
    }
    auto owner = std::make_shared<SessionOwnerState>();
    owner->engine = std::move(engine);
    owner->options = std::move(options);
    return R::ok(bbt::coroutine::MakeCoObject<Client>("infra.rpc.Client", owner->options.LocalIdentity(), [&] {
        return std::shared_ptr<Client>(new Client(std::move(owner)));
    }));
}

namespace {

// "tcp://host:port" → numeric host/port（file-local，非成员）。
result<void> ParseTarget(std::string_view target, std::string& host, std::uint16_t& port)
{
    const std::string value(target);
    if (value.size() < 8 || value.compare(0, 6, "tcp://") != 0) {
        return ErrV(ErrorCode::InvalidArgument, "target must be tcp://host:port");
    }
    return detail::ParseTcpEndpoint(value.substr(6), host, port);
}

// 已接纳调用的一次性责任：与 owner 封口同门登记，作用域结束（含所有
// 早退/异常路径）归还并唤醒 Shutdown 等待者。
struct OwnerOperationGuard {
    std::shared_ptr<SessionOwnerState> owner;
    ~OwnerOperationGuard()
    {
        detail::SessionOwnerEndOperation(owner);
    }
};

} // namespace

namespace {

// v2 出站关闭原因的统一附加点：Call/Send 返回失败且 owner 已发布原因时，
// 在调用方可见的返回点把首个关闭原因附到 Error.details；内部各失败出口
// 只保留失败分类（未提交 Closed / 已提交 OutcomeUnknown+request_phase /
// 超时 / 取消），不分别补原因。错误本身即关闭原因或已含 close_reason 键
// 时不重复附加；成功结果不改。
void AttachOwnerCloseReason(const std::shared_ptr<SessionOwnerState>& owner, Error& error)
{
    const auto reason = detail::SessionOwnerCloseReason(*owner);
    if (!reason) {
        return;
    }
    if (error.code == reason->code && error.message == reason->message) {
        return;
    }
    for (const auto& detail : error.details) {
        if (detail.first == "close_reason") {
            return;
        }
    }
    AttachCloseReason(error, *reason);
}

result<Response> CallImpl(const std::shared_ptr<SessionOwnerState>& owner, std::string_view target, Request request,
                          const CallOptions& options)
{
    using R = result<Response>;
    if (!bbt::coroutine::IsInCoroutineContext())
        return R::err(Err(ErrorCode::InvalidContext, "coroutine context required"));
    Error closed_error;
    if (!detail::SessionOwnerBeginOperation(owner, closed_error)) {
        return R::err(std::move(closed_error));
    }
    OwnerOperationGuard operation_guard{owner};
    if (const auto gate = CheckCall(options.deadline)) {
        if (gate == std::errc::operation_not_permitted) {
            return R::err(Err(ErrorCode::InvalidContext, "coroutine context required"));
        }
        return R::err(CallError(gate, "call gate", false));
    }
    if (request.Traffic() == TrafficClass::System || request.Reply() != ReplyMode::Required) {
        return R::err(Err(ErrorCode::InvalidArgument, "Call requires Service/Actor + ReplyRequired"));
    }
    std::string host;
    std::uint16_t port = 0;
    if (auto parsed = ParseTarget(target, host, port); !parsed) {
        return R::err(parsed.error());
    }
    std::error_code addr_error;
    const auto address = net::Address::Ip(host, static_cast<int>(port), addr_error);
    if (addr_error || !address.Valid()) {
        return R::err(Err(ErrorCode::InvalidArgument, "invalid target address"));
    }
    // 同时拨号 fencing 竞速：GetSession 返回的 session 可能在交付/登记前
    // 被选举退役（dead）。by_endpoint 已被胜者接管，重取落到存活 link；
    // pending 登记的 dead 拒绝同样回环一次，不循环拨号。
    std::shared_ptr<SessionCore> session;
    std::shared_ptr<SessionCore::PendingCall> pending;
    bool inserted = false;
    for (unsigned attempt = 0; attempt != 2 && !inserted; ++attempt) {
        auto session_result = GetSession(owner, std::string(target), address, options.deadline);
        if (!session_result) {
            return R::err(session_result.error());
        }
        session = std::move(session_result.value());
        if (session->dead.load()) {
            continue;
        }

        // request_id：空时 session 生成唯一值。
        if (request.RequestId().empty()) {
            request.SetRequestId("r" + std::to_string(session->seq_counter.fetch_add(1)));
        }
        const auto request_id = request.RequestId();
        auto budget = Budget(options.deadline);
        if (!budget) {
            return R::err(budget.error());
        }
        auto frame = ToSessionFrame(request, FrameKind::Call, session->epoch, budget.value());
        if (!frame) {
            return R::err(frame.error());
        }
        auto bytes = EncodeFramedSessionFrame(frame.value(), owner->options.MaxFrameBytes());
        if (!bytes) {
            return R::err(bytes.error());
        }

        pending = std::make_shared<SessionCore::PendingCall>();
        pending->service = request.Service();
        pending->method = request.Method();
        pending->request_schema = request.RequestSchema();
        pending->response_schema = request.ResponseSchema();
        {
            std::lock_guard<std::mutex> lock(session->pending_mu);
            if (session->dead.load()) {
                continue;
            }
            if (session->calls.count(request_id)) {
                return R::err(Err(ErrorCode::InvalidArgument, "duplicate request_id on session"));
            }
            session->calls[request_id] = pending;
        }
        inserted = true;
        {
            std::lock_guard<std::mutex> lock(session->out_mu);
            ++session->out_lanes[LaneIndex(request.Traffic())].inflight;
        }
        SessionCore::OutItem item;
        item.bytes.assign(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
        item.lane = request.Traffic();
        item.key = request_id;
        item.deadline = options.deadline;
        auto enqueued = session->Enqueue(std::move(item));
        if (!enqueued) {
            std::lock_guard<std::mutex> lock(session->pending_mu);
            session->calls.erase(request_id);
            {
                std::lock_guard<std::mutex> slock(session->out_mu);
                --session->out_lanes[LaneIndex(request.Traffic())].inflight;
            }
            // 退役会话的 Enqueue Closed：同样重取一次落到存活 link。
            if (enqueued.error().code == ErrorCode::Closed && session->dead.load()) {
                inserted = false;
                continue;
            }
            return R::err(enqueued.error());
        }
    }
    if (!inserted) {
        return R::err(Err(ErrorCode::Closed, "session fenced during dial"));
    }
    const auto request_id = request.RequestId();
    owner->acceptance_pending.fetch_add(1);
    // 先通知后等待的丢唤醒窗口由持久 done 谓词 + 登记后复查关闭。
    // Dispatch/写终态在 pending_mu 下先写 done 再 Notify；on_registered 在
    // waiter 已挂起登记后于同一锁复查 done——done 已真即同步 Notify 走
    // PENDING 早到路径，不依赖那次 Notify 的时序，永不丢唤醒。
    const auto status = pending->waiter->WaitWithCallback({options.deadline}, [&] {
        std::lock_guard<std::mutex> lock(session->pending_mu);
        if (pending->done) {
            pending->waiter->Notify();
        }
        return true;
    });
    owner->acceptance_pending.fetch_sub(1);
    if (status == WaitStatus::Completed) {
        std::lock_guard<std::mutex> lock(session->out_mu);
        auto& lane = session->out_lanes[LaneIndex(request.Traffic())];
        --lane.inflight;
        ++lane.finished;
        return std::move(pending->outcome);
    }
    // 超时/取消：移除 pending，阶段事实结算。
    bool committed;
    {
        std::lock_guard<std::mutex> lock(session->pending_mu);
        session->calls.erase(request_id);
        committed = pending->committed;
    }
    {
        std::lock_guard<std::mutex> lock(session->out_mu);
        auto& lane = session->out_lanes[LaneIndex(request.Traffic())];
        --lane.inflight;
        ++lane.dropped;
    }
    if (status == WaitStatus::Cancelled) {
        return R::err(ErrPhase(ErrorCode::Cancelled, "call cancelled",
                               committed ? RequestPhase::RequestCommitted : RequestPhase::NotStarted));
    }
    if (committed) {
        return R::err(
            ErrPhase(ErrorCode::OutcomeUnknown, "call timed out after commit", RequestPhase::RequestCommitted));
    }
    return R::err(Err(ErrorCode::TimedOut, "call timed out"));
}

result<void> SendImpl(const std::shared_ptr<SessionOwnerState>& owner, std::string_view target, Request message,
                      const SendOptions& options)
{
    using R = result<void>;
    if (!bbt::coroutine::IsInCoroutineContext())
        return R::err(Err(ErrorCode::InvalidContext, "coroutine context required"));
    Error closed_error;
    if (!detail::SessionOwnerBeginOperation(owner, closed_error)) {
        return R::err(std::move(closed_error));
    }
    OwnerOperationGuard operation_guard{owner};
    if (auto valid = options.Validate(); !valid) {
        return R::err(valid.error());
    }
    if (const auto gate = CheckCall(options.Deadline())) {
        if (gate == std::errc::operation_not_permitted) {
            return R::err(Err(ErrorCode::InvalidContext, "coroutine context required"));
        }
        return R::err(CallError(gate, "send gate", false));
    }
    if (message.Traffic() == TrafficClass::System || message.Reply() != ReplyMode::None) {
        return R::err(Err(ErrorCode::InvalidArgument, "Send requires Service/Actor + ReplyNone"));
    }
    std::string host;
    std::uint16_t port = 0;
    if (auto parsed = ParseTarget(target, host, port); !parsed) {
        return R::err(parsed.error());
    }
    std::error_code addr_error;
    const auto address = net::Address::Ip(host, static_cast<int>(port), addr_error);
    if (addr_error || !address.Valid()) {
        return R::err(Err(ErrorCode::InvalidArgument, "invalid target address"));
    }
    // 同 Call：fencing 竞速下重取一次落到存活 link；pending 登记的
    // dead 拒绝同样回环一次，不循环拨号。
    std::shared_ptr<SessionCore> session;
    std::shared_ptr<SessionCore::PendingSend> pending;
    bool inserted = false;
    for (unsigned attempt = 0; attempt != 2 && !inserted; ++attempt) {
        auto session_result = GetSession(owner, std::string(target), address, options.Deadline());
        if (!session_result) {
            return R::err(session_result.error());
        }
        session = std::move(session_result.value());
        if (session->dead.load()) {
            continue;
        }

        if (message.MessageId().empty()) {
            message.SetMessageId("m" + std::to_string(session->seq_counter.fetch_add(1)));
        }
        const auto message_id = message.MessageId();
        auto budget = Budget(options.Deadline());
        if (!budget) {
            return R::err(budget.error());
        }
        auto frame = ToSessionFrame(message, FrameKind::Send, session->epoch, budget.value(),
                                    options.RequiresRemoteAcceptance() ? std::optional<bool>{true} : std::nullopt);
        if (!frame) {
            return R::err(frame.error());
        }
        auto bytes = EncodeFramedSessionFrame(frame.value(), owner->options.MaxFrameBytes());
        if (!bytes) {
            return R::err(bytes.error());
        }

        const auto lane_index = LaneIndex(message.Traffic());
        if (options.RequiresRemoteAcceptance()) {
            pending = std::make_shared<SessionCore::PendingSend>();
            std::lock_guard<std::mutex> lock(session->pending_mu);
            if (session->dead.load()) {
                pending.reset();
                continue;
            }
            if (session->sends.count(message_id)) {
                return R::err(Err(ErrorCode::InvalidArgument, "duplicate message_id on session"));
            }
            session->sends[message_id] = pending;
        }
        inserted = true;
        {
            std::lock_guard<std::mutex> lock(session->out_mu);
            ++session->out_lanes[lane_index].inflight;
        }
        SessionCore::OutItem item;
        item.bytes.assign(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
        item.lane = message.Traffic();
        item.key = message_id;
        item.deadline = options.Deadline();
        auto enqueued = session->Enqueue(std::move(item));
        if (!enqueued) {
            if (pending) {
                std::lock_guard<std::mutex> lock(session->pending_mu);
                session->sends.erase(message_id);
            }
            {
                std::lock_guard<std::mutex> lock(session->out_mu);
                --session->out_lanes[lane_index].inflight;
            }
            if (enqueued.error().code == ErrorCode::Closed && session->dead.load()) {
                inserted = false;
                pending.reset();
                continue;
            }
            return R::err(enqueued.error());
        }
    }
    if (!inserted) {
        return R::err(Err(ErrorCode::Closed, "session fenced during dial"));
    }
    const auto message_id = message.MessageId();
    const auto lane_index = LaneIndex(message.Traffic());
    if (!pending) {
        // 本地接纳即成功（ADR §4.1：不要求远端确认）。
        std::lock_guard<std::mutex> lock(session->out_mu);
        --session->out_lanes[lane_index].inflight;
        ++session->out_lanes[lane_index].finished;
        return R::ok();
    }
    // 同 Call——on_registered 在 waiter 登记后于 pending_mu 复查
    // 持久 done，关闭先通知后等待的丢唤醒窗口。
    const auto status = pending->waiter->WaitWithCallback({options.Deadline()}, [&] {
        std::lock_guard<std::mutex> lock(session->pending_mu);
        if (pending->done) {
            pending->waiter->Notify();
        }
        return true;
    });
    if (status == WaitStatus::Completed) {
        std::lock_guard<std::mutex> lock(session->out_mu);
        auto& lane = session->out_lanes[lane_index];
        --lane.inflight;
        ++lane.finished;
        return std::move(pending->outcome);
    }
    {
        std::lock_guard<std::mutex> lock(session->pending_mu);
        session->sends.erase(message_id);
    }
    {
        std::lock_guard<std::mutex> lock(session->out_mu);
        auto& lane = session->out_lanes[lane_index];
        --lane.inflight;
        ++lane.dropped;
    }
    if (status == WaitStatus::Cancelled) {
        return R::err(ErrPhase(ErrorCode::Cancelled, "send cancelled",
                               pending->committed ? RequestPhase::RequestCommitted : RequestPhase::NotStarted));
    }
    if (pending->committed) {
        return R::err(ErrPhase(ErrorCode::OutcomeUnknown, "send acceptance timed out after commit",
                               RequestPhase::RequestCommitted));
    }
    return R::err(Err(ErrorCode::TimedOut, "send acceptance timed out"));
}

} // namespace

bbt_co_async(result<Response>) Client::Call(std::string_view target, Request request, const CallOptions& options)
{
    auto owner = m_owner;
    if (!owner) {
        return result<Response>::err(Err(ErrorCode::Closed, "client not a v2 owner"));
    }
    auto outcome = CallImpl(owner, target, std::move(request), options);
    if (!outcome) {
        AttachOwnerCloseReason(owner, outcome.error());
    }
    return outcome;
}

bbt_co_async(result<void>) Client::Send(std::string_view target, Request message, const SendOptions& options)
{
    auto owner = m_owner;
    if (!owner) {
        return result<void>::err(Err(ErrorCode::Closed, "client not a v2 owner"));
    }
    auto outcome = SendImpl(owner, target, std::move(message), options);
    if (!outcome) {
        AttachOwnerCloseReason(owner, outcome.error());
    }
    return outcome;
}

ClientStats Client::Stats() const noexcept
{
    ClientStats out;
    auto owner = m_owner;
    if (!owner) {
        return out;
    }
    out.sessions = owner->sessions.load();
    out.links = owner->links.load();
    out.remote_acceptance_pending = owner->acceptance_pending.load();
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        for (const auto& [id, session] : owner->by_identity) {
            (void)id;
            std::lock_guard<std::mutex> slock(session->out_mu);
            for (std::size_t i = 0; i != kLaneCount; ++i) {
                auto& dst = i == 0 ? out.system : i == 1 ? out.service : out.actor;
                const auto& src = session->out_lanes[i];
                dst.queued += src.queued;
                dst.dequeued += src.dequeued;
                dst.inflight += src.inflight;
                dst.finished += src.finished;
                dst.dropped += src.dropped;
                dst.bytes_queued += src.bytes_queued;
            }
        }
    }
    return out;
}

// ============================================================================
// Server v2 公共方法与 detail 挂钩
// ============================================================================

result<std::shared_ptr<Server>> Server::Create(std::shared_ptr<net::Engine> engine,
                                               std::shared_ptr<Client> session_owner,
                                               AdmissionHandler admission_handler, ServerOptions options)
{
    using R = result<std::shared_ptr<Server>>;
    if (!engine || !session_owner || !admission_handler) {
        return R::err(Err(ErrorCode::InvalidArgument, "engine/session_owner/handler required"));
    }
    auto owner = session_owner->m_owner;
    if (!owner) {
        return R::err(Err(ErrorCode::InvalidArgument, "session_owner is not a v2 client"));
    }
    if (auto valid = options.Validate(); !valid) {
        return R::err(valid.error());
    }
    if (engine->Phase() != net::EnginePhase::Idle) {
        return R::err(Err(ErrorCode::InvalidContext, "engine must be Idle at Create"));
    }
    if (auto handoff = engine->EnableAcceptHandoff(options.MaxConnections() < 64 ? options.MaxConnections() : 64);
        handoff) {
        return R::err(Err(ErrorCode::RuntimeUnavailable, "accept handoff setup failed"));
    }
    auto v2 = std::make_shared<ServerV2State>();
    v2->engine = std::move(engine);
    v2->owner = owner;
    v2->handler = std::move(admission_handler);
    v2->options = options;
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        owner->inbound_server = v2;
    }
    return R::ok(bbt::coroutine::MakeCoObject<Server>(
        "infra.rpc.Server", "", [&] { return std::shared_ptr<Server>(new Server(std::move(v2))); }));
}

result<void> detail::ServerV2Start(const std::shared_ptr<ServerV2State>& v2)
{
    if (!NativeHost()) {
        return ErrV(ErrorCode::InvalidContext, "Start requires native host context");
    }
    std::lock_guard<std::mutex> lock(v2->mu);
    if (v2->closed) {
        return ErrV(ErrorCode::Closed, "server closed");
    }
    if (v2->started) {
        return result<void>::ok();
    }
    if (v2->engine->Phase() != net::EnginePhase::Running) {
        return ErrV(ErrorCode::InvalidContext, "engine must be Running with listener");
    }
    // 单 FIFO admission worker。
    auto queue = AdmissionQueue::Create(v2->options.MaxInflight(), v2->options.MaxHandlerBytes(), v2->handler);
    if (!queue) {
        return result<void>::err(queue.error());
    }
    detail::AdmissionAccess::AttachQueue(queue.value(), v2);
    v2->queue = std::move(queue.value());
    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        128 * 1024,
        [v2] {
            // 裸协程任务：异常逃逸即 terminate——兜底封 server。
            try {
                AcceptLoop(v2);
            } catch (...) {
                detail::ServerV2Seal(v2, Err(ErrorCode::InternalError, "rpc server: accept loop threw"));
                std::lock_guard<std::mutex> lock(v2->mu);
                v2->accept_loop_exited = true;
                v2->cv.notify_all();
            }
        },
        registered);
    if (!registered) {
        v2->queue->Close();
        return ErrV(ErrorCode::RuntimeUnavailable, "accept task registration failed");
    }
    v2->started = true;
    return result<void>::ok();
}

void detail::ServerV2StopAccepting(const std::shared_ptr<ServerV2State>& v2) noexcept
{
    std::shared_ptr<AdmissionQueue> queue;
    {
        std::lock_guard<std::mutex> lock(v2->mu);
        v2->stopping = true;
        queue = v2->queue;
        v2->cv.notify_all();
    }
    v2->engine->RequestStopAccepting();
    if (queue) {
        queue->Close();
    }
}

result<void> detail::ServerV2Drain(const std::shared_ptr<ServerV2State>& v2, bbt::coroutine::Deadline deadline)
{
    if (!NativeHost()) {
        return ErrV(ErrorCode::InvalidContext, "Drain requires native host context");
    }
    detail::ServerV2StopAccepting(v2);
    std::unique_lock<std::mutex> lock(v2->mu);
    const auto done =
        v2->cv.wait_until(lock, deadline, [&] { return v2->DrainedLocked() || v2->close_reason.has_value(); });
    if (v2->DrainedLocked()) {
        return result<void>::ok();
    }
    if (v2->close_reason) {
        return result<void>::err(*v2->close_reason);
    }
    (void)done;
    return ErrV(ErrorCode::TimedOut, "drain deadline exceeded");
}

result<void> detail::ServerV2Shutdown(const std::shared_ptr<ServerV2State>& v2, bbt::coroutine::Deadline deadline)
{
    if (!NativeHost()) {
        return ErrV(ErrorCode::InvalidContext, "Shutdown requires native host context");
    }
    {
        std::lock_guard<std::mutex> lock(v2->mu);
        if (v2->close_reason) {
            return result<void>::err(*v2->close_reason); // 已 Closed
        }
        if (v2->draining) {
            // 重复调用（已 Draining）：已排空按 ok，否则 Busy。
            if (v2->DrainedLocked()) {
                return result<void>::ok();
            }
            return ErrV(ErrorCode::Overloaded, "shutdown already in progress");
        }
        v2->draining = true;
    }
    // 封口拒新（不发布关闭原因：Close 的打断原因才能如实传出）。
    detail::ServerV2StopAccepting(v2);
    std::unique_lock<std::mutex> lock(v2->mu);
    const auto done =
        v2->cv.wait_until(lock, deadline, [&] { return v2->DrainedLocked() || v2->close_reason.has_value(); });
    // 先完成者胜：真实排空即 ok，不被随后 Close 追改。
    if (v2->DrainedLocked()) {
        return result<void>::ok();
    }
    if (v2->close_reason) {
        return result<void>::err(*v2->close_reason);
    }
    (void)done;
    return ErrV(ErrorCode::TimedOut, "shutdown deadline exceeded; server remains sealed");
}

void detail::ServerV2Seal(const std::shared_ptr<ServerV2State>& v2, Error reason) noexcept
{
    std::vector<std::shared_ptr<SessionCore>> links;
    std::vector<net::Connection> handshakes;
    std::shared_ptr<AdmissionQueue> queue;
    {
        std::lock_guard<std::mutex> lock(v2->mu);
        if (!v2->close_reason) {
            v2->close_reason = std::move(reason);
        }
        if (v2->closed) {
            return;
        }
        v2->closed = true;
        v2->stopping = true;
        queue = v2->queue;
        for (auto& weak : v2->links) {
            if (auto session = weak.lock()) {
                links.push_back(session);
            }
        }
        // 建立中握手 conn 同样是关闭责任：RequestClose 让握手 Read 尽快
        // 失败退出；台账条目由 ServeInbound 结束时移除（物理关闭责任）。
        for (auto& [id, conn] : v2->handshakes) {
            (void)id;
            handshakes.push_back(conn);
        }
        v2->cv.notify_all();
    }
    v2->engine->RequestStopAccepting();
    if (queue) {
        queue->Close();
    }
    for (auto& session : links) {
        session->Kill({});
    }
    for (auto& conn : handshakes) {
        conn.RequestClose(std::make_error_code(std::errc::connection_aborted));
    }
}

result<void> detail::ServerV2WaitStopped(const std::shared_ptr<ServerV2State>& v2, bbt::coroutine::Deadline deadline)
{
    if (!NativeHost()) {
        return ErrV(ErrorCode::InvalidContext, "WaitStopped requires native host context");
    }
    // conn 物理 Released 可能晚于 link 退出责任归零——cv 每醒一次就
    // 在 owner->mu 下重驱各 link 的 ReleasedLocked（幂等），让额度/台账
    // 在真实 Released 后归还，不依赖 ExitDone 的单一驱动点。
    const auto sweep_release = [&] {
        std::vector<std::shared_ptr<SessionCore>> live;
        {
            std::lock_guard<std::mutex> lock(v2->mu);
            for (auto& weak : v2->links) {
                if (auto s = weak.lock()) {
                    live.push_back(std::move(s));
                }
            }
        }
        if (live.empty()) {
            return;
        }
        std::lock_guard<std::mutex> lock(v2->owner->mu);
        for (auto& s : live) {
            s->DriveReleaseLocked();
        }
    };
    for (;;) {
        sweep_release();
        std::unique_lock<std::mutex> lock(v2->mu);
        if (v2->cv.wait_until(lock, deadline, [&] { return v2->StoppedLocked(); })) {
            return result<void>::ok();
        }
        if (Clock::now() >= deadline) {
            return ErrV(ErrorCode::TimedOut, "wait-stopped deadline exceeded");
        }
        // 超时未收口但 conn 可能刚 Released——再驱一次后让 cv 继续。
    }
}

bool detail::ServerV2Closed(const ServerV2State& v2) noexcept
{
    std::lock_guard<std::mutex> lock(v2.mu);
    return v2.closed;
}

std::optional<Error> detail::ServerV2CloseReason(const ServerV2State& v2)
{
    std::lock_guard<std::mutex> lock(v2.mu);
    return v2.close_reason;
}

result<bbt::infra::net::Address> Server::LocalAddress() const
{
    using R = result<bbt::infra::net::Address>;
    if (!m_v2) {
        return R::err(Err(ErrorCode::InvalidContext, "v2 server not started"));
    }
    {
        std::lock_guard<std::mutex> lock(m_v2->mu);
        if (!m_v2->started) {
            return R::err(Err(ErrorCode::InvalidContext, "v2 server not started"));
        }
    }
    std::vector<net::Address> addresses;
    if (auto error = m_v2->engine->LocalAddresses(addresses); error || addresses.empty()) {
        return R::err(Err(ErrorCode::Unavailable, "listen address not observable"));
    }
    return R::ok(addresses.front());
}

ServerStats Server::Stats() const noexcept
{
    ServerStats out;
    if (!m_v2) {
        return out;
    }
    auto& v2 = *m_v2;
    std::lock_guard<std::mutex> lock(v2.mu);
    out.accepted_connections = v2.accepted;
    std::uint64_t active = 0;
    for (const auto& weak : v2.links) {
        if (auto session = weak.lock(); session && !session->dead.load()) {
            ++active;
        }
    }
    out.active_connections = active;
    out.retiring_connections = v2.links.size() > active ? v2.links.size() - active : 0;
    out.callbacks = v2.callbacks;
    // queue 计数经 AdmissionQueue 同步观察面读（impl->mu 内取），不得在
    // 只持 v2.mu 时直读 impl 字段——避免跨锁直读未同步字段。注意 Queued(i)
    // 与 BytesQueued(i) 是两次独立取锁：本快照只保证每个字段自身读数合法，
    // 不构成同一时刻的一致视图（见 Stats.hpp 文件头「取样语义」）。
    auto queue = v2.queue;
    std::array<std::uint64_t, kLaneCount> queued_items{};
    std::array<std::uint64_t, kLaneCount> queued_bytes{};
    if (queue) {
        for (std::size_t i = 0; i != kLaneCount; ++i) {
            queued_items[i] = queue->Queued(i);
            queued_bytes[i] = queue->BytesQueued(i);
        }
    }
    for (std::size_t i = 0; i != kLaneCount; ++i) {
        auto& dst = i == 0 ? out.system : i == 1 ? out.service : out.actor;
        dst.inflight = v2.inflight[i];
        dst.finished = v2.finished[i];
        dst.dropped = v2.dropped[i];
        dst.queued = queued_items[i];
        dst.bytes_queued = queued_bytes[i];
    }
    return out;
}

// ============================================================================
// detail 挂钩实现
// ============================================================================

void detail::SessionOwnerSeal(const std::shared_ptr<SessionOwnerState>& owner, Error reason) noexcept
{
    std::vector<std::shared_ptr<SessionCore>> sessions;
    std::vector<net::Connection> handshakes;
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        if (!owner->close_reason) {
            owner->close_reason = std::move(reason);
        }
        if (!owner->closed) {
            // 「失败在途」恰好一次：closed 位唯一表达，Draining 下同样执行。
            owner->closed = true;
            for (auto& [id, session] : owner->by_identity) {
                (void)id;
                sessions.push_back(session);
            }
            // 建立中出站握手 conn 同受关闭责任：RequestClose 让握手尽快
            // 失败退出，不等调用 deadline（物理关闭责任）。
            for (auto& [key, est] : owner->establishing) {
                (void)key;
                if (est->conn.Valid()) {
                    handshakes.push_back(est->conn);
                }
            }
        }
        owner->cv.notify_all();
    }
    for (auto& session : sessions) {
        session->Kill({});
    }
    for (auto& conn : handshakes) {
        conn.RequestClose(std::make_error_code(std::errc::connection_aborted));
    }
    // 控制器永久 closed：无论本次是否首次封口都置位，保证 Close 返回后 Arm
    // 一定看到 Closed；在途 Pause 由 runtime 的 owner.closed 检查退出。锁外
    // 单独取 hook 锁，不与 owner.mu 形成嵌套锁序。
    if (owner->hooks) {
        detail::SessionHookAccess::Close(*owner->hooks);
    }
}

SessionOwnerState::~SessionOwnerState()
{
    // 兜底：owner 未经 Seal 直接销毁时同样封口控制器。handle 可能比 owner
    // 活得久，但只保留有界控制状态，不再接受注册。
    if (hooks) {
        detail::SessionHookAccess::Close(*hooks);
    }
}

bool detail::SessionOwnerClosed(const SessionOwnerState& owner) noexcept
{
    std::lock_guard<std::mutex> lock(owner.mu);
    return owner.closed || owner.draining;
}

std::optional<Error> detail::SessionOwnerCloseReason(const SessionOwnerState& owner)
{
    std::lock_guard<std::mutex> lock(owner.mu);
    return owner.close_reason;
}

bool detail::SessionOwnerBeginOperation(const std::shared_ptr<SessionOwnerState>& owner, Error& closed_error)
{
    std::lock_guard<std::mutex> lock(owner->mu);
    // 非 Open 即拒绝：Close 原因优先，否则通用 Closed。
    if (owner->closed || owner->draining) {
        closed_error = owner->close_reason ? *owner->close_reason : Err(ErrorCode::Closed, "rpc client: closed");
        return false;
    }
    ++owner->active_ops;
    return true;
}

void detail::SessionOwnerEndOperation(const std::shared_ptr<SessionOwnerState>& owner) noexcept
{
    std::lock_guard<std::mutex> lock(owner->mu);
    if (owner->active_ops != 0) {
        --owner->active_ops;
    }
    owner->cv.notify_all();
}

result<void> detail::SessionOwnerShutdown(const std::shared_ptr<SessionOwnerState>& owner,
                                          bbt::coroutine::Deadline deadline)
{
    if (!NativeHost()) {
        return ErrV(ErrorCode::InvalidContext, "Shutdown requires native host context");
    }
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        if (owner->closed) {
            // 已 Closed：如实返回首个关闭原因（缺失时不伪造）。
            if (owner->close_reason) {
                return result<void>::err(*owner->close_reason);
            }
            return ErrV(ErrorCode::Closed, "rpc client: closed");
        }
        if (owner->draining) {
            // 重复调用（已 Draining）：已排空按 ok，否则 Busy。
            if (owner->active_ops == 0) {
                return result<void>::ok();
            }
            return ErrV(ErrorCode::Overloaded, "shutdown already in progress");
        }
        // 拒新：与 Call/Send 的接纳同门（BeginOperation 读 draining），
        // 不发布关闭原因——Close 的打断原因才能如实传出。不占用 closed 位，
        // Seal 在其后仍执行且仅执行一次「失败在途」。
        owner->draining = true;
        owner->cv.notify_all();
    }
    std::unique_lock<std::mutex> lock(owner->mu);
    for (;;) {
        // 先排空先胜：当前已归还即 ok；等待中被 Close 且仍有在途 → 原因。
        if (owner->active_ops == 0) {
            return result<void>::ok();
        }
        if (owner->close_reason) {
            return result<void>::err(*owner->close_reason);
        }
        if (deadline != bbt::coroutine::Deadline::max() && Clock::now() >= deadline) {
            return ErrV(ErrorCode::TimedOut, "shutdown deadline exceeded; owner remains sealed");
        }
        owner->cv.wait_until(lock, deadline);
    }
}

} // namespace bbt::infra::rpc
