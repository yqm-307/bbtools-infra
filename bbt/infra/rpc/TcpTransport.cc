// RPC 原生 TCP adapter 实现（architecture §6）：u32be 长度前缀帧，
// 一连接一请求一回复，无 multiplex/半关闭/连接池。组合公开 net::Engine/
// ICoConnection（CoDial/CoAccept/Read/Write/RequestClose），不触碰
// net/http detail 或第三方类型。
//
// 错误面：与 HTTP adapter 同一契约——NetToError 基础分类 + adapter 阶段
// 结算；committed（帧字节本地完成）后失联为 OutcomeUnknown 并携带
// request_phase=RequestCommitted，协议违规为 ProtocolError。共享助手见
// bbt/infra/rpc/detail/Tcp.hpp。

#include <bbt/infra/rpc/TcpTransport.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/rpc/detail/Tcp.hpp>

namespace bbt::infra::rpc {
namespace {

using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

result<void> Failure(ErrorCode code, const char* message) {
    return result<void>::err(MakeError(code, message));
}

bool NativeHost() {
    return bbt::coroutine::GetLocalCoroutineId() == 0 &&
           bbt::coroutine::detail::Processer::TryGetLocalProcesser() == nullptr;
}

// 受管协程内检查点：域非法/已取消/期限已到 返回对应 error_code。
// （与 http::detail::CheckCall 同一语义，本 TU 内局部实现不引 http detail。）
std::error_code CheckCall(bbt::coroutine::Deadline deadline) {
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

// request.body → 4+N 单帧字节（长度边界先于分配）。
result<std::string> EncodeRequestFrame(const WireRequest& request,
                                     std::size_t max_frame_bytes) {
    if (!detail::TcpFrameSizeValid(request.body.size(), max_frame_bytes)) {
        return result<std::string>::err(MakeError(
            ErrorCode::InvalidArgument,
            "rpc tcp: request frame length out of bounds"));
    }
    std::string frame;
    frame.resize(detail::kTcpFramePrefixBytes + request.body.size());
    detail::TcpFrameWritePrefix(
        frame.data(), static_cast<std::uint32_t>(request.body.size()));
    std::memcpy(frame.data() + detail::kTcpFramePrefixBytes,
                request.body.data(), request.body.size());
    return result<std::string>::ok(std::move(frame));
}

result<std::string> EncodeReplyFrame(const WireReply& reply,
                                     std::size_t max_frame_bytes) {
    if (!detail::TcpFrameSizeValid(reply.body.size(), max_frame_bytes)) {
        return result<std::string>::err(MakeError(
            ErrorCode::InternalError,
            "rpc tcp: reply frame length out of bounds"));
    }
    std::string frame;
    frame.resize(detail::kTcpFramePrefixBytes + reply.body.size());
    detail::TcpFrameWritePrefix(
        frame.data(), static_cast<std::uint32_t>(reply.body.size()));
    std::memcpy(frame.data() + detail::kTcpFramePrefixBytes,
                reply.body.data(), reply.body.size());
    return result<std::string>::ok(std::move(frame));
}

// 从连接上按帧边界读满 need 字节到 out（pending 是跨 Read 的本地缓冲）。
// 返回：{ok, 流终态 ec}；流终态由调用方结算（Eof=定界/截断语义归协议层）。
// 不锁内嵌字节：读到的字节全部进 pending，调用方按帧界消费。
result<std::error_code> FillUpTo(net::ICoConnection& connection,
                                 std::string& pending, std::size_t need,
                                 bbt::coroutine::Deadline deadline,
                                 std::vector<std::uint8_t>& out) {
    while (!detail::TcpTake(pending, need, out)) {
        if (const auto gate = CheckCall(deadline)) {
            return result<std::error_code>::ok(gate);
        }
        const auto read = connection.Read({deadline});
        if (read.state == net::ReadState::Data) {
            read.data.AppendTo(pending);
            continue;
        }
        if (read.state == net::ReadState::Eof) {
            return result<std::error_code>::ok(
                net::make_error_code(net::NetErrc::Eof));
        }
        return result<std::error_code>::ok(read.error);
    }
    return result<std::error_code>::ok(std::error_code{});
}

} // namespace

// --- TcpRpcTransport ---------------------------------------------------------------

// gate + inflight 名额 + 活动连接台账。连接句柄只在 Exchange 协程与
// Close 之间共享；台账只为「Close 对在途连接 RequestClose」存在，
// 不记账重发、不做池。
struct TcpRpcTransport::State {
    std::shared_ptr<net::Engine> engine;   // shared lifetime only
    TcpRpcOptions options;
    mutable std::mutex mutex;
    std::condition_variable cv;
    bool closed{false};
    std::size_t inflight{0};
    std::vector<net::Connection> connections;  // 已发布给 Close 的在途连接

    // 在途名额（拨号前原子预占）：返回值 0=接纳，1=已封口，2=名额满。
    // 先于 CoDial 占位，超额调用立即 Overloaded、不发起真实 connect。
    unsigned Admit() {
        std::lock_guard<std::mutex> lock(mutex);
        if (closed) { return 1; }
        if (inflight >= options.max_inflight) { return 2; }
        ++inflight;
        return 0;
    }
    // 连接交付后发布给 Close（RequestClose 名单）；失败路径 nullptr 跳过。
    // 锁内复查 closed：拨号途中 Seal 不在台账上的连接，发布即发现封口——
    // 返回 false，调用方须 RequestClose 本连接并把调用结算为 Closed，
    // 不得写请求帧（§5.1：Close 时未提交调用落 Closed）。
    bool Publish(const net::Connection& connection) {
        if (!connection.Valid()) { return true; }
        std::lock_guard<std::mutex> lock(mutex);
        if (closed) { return false; }
        connections.push_back(connection);
        return true;
    }
    void Release(const net::Connection& connection) {
        std::lock_guard<std::mutex> lock(mutex);
        if (connection.Valid()) {
            connections.erase(std::remove_if(
                connections.begin(), connections.end(),
                [&](const net::Connection& item) {
                    return item.Id() == connection.Id();
                }),
                connections.end());
        }
        --inflight;
        cv.notify_all();
    }
    void Seal() {
        std::vector<net::Connection> doomed;
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (closed) { return; }
            closed = true;
            doomed = connections;
            cv.notify_all();
        }
        // 锁外请求关闭：连接 owner 结算在途写/读，已发前缀保留。
        for (auto& connection : doomed) { connection.RequestClose(); }
    }
};

TcpRpcTransport::TcpRpcTransport(std::shared_ptr<State> state) noexcept
    : m_state(std::move(state)) {}

TcpRpcTransport::~TcpRpcTransport() noexcept { Close(); }

result<std::shared_ptr<TcpRpcTransport>> TcpRpcTransport::Create(
    std::shared_ptr<net::Engine> engine, TcpRpcOptions options) {
    using R = result<std::shared_ptr<TcpRpcTransport>>;
    if (!engine) {
        return R::err(MakeError(ErrorCode::InvalidArgument,
            "rpc tcp transport: engine required (adapter 不自建执行底座)"));
    }
    if (options.max_inflight == 0 ||
        options.max_inflight > kNetworkLimitsMaxInflight ||
        options.max_frame_bytes == 0 ||
        options.max_frame_bytes > kNetworkLimitsMaxBodyBytes) {
        return R::err(MakeError(ErrorCode::InvalidArgument,
            "rpc tcp transport: invalid options"));
    }
    auto state = std::make_shared<State>();
    state->engine = std::move(engine);
    state->options = options;
    return R::ok(std::shared_ptr<TcpRpcTransport>(
        new TcpRpcTransport(std::move(state))));
}

bbt_co_async(result<WireReply>) TcpRpcTransport::Exchange(
    const RpcAddress& address, WireRequest request,
    const CallOptions& options) {
    const auto state = m_state;  // 局部安全引用
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->closed) {
            return result<WireReply>::err(
                MakeError(ErrorCode::Closed, "rpc tcp transport: closed"));
        }
    }
    if (address.transport != "tcp") {
        return result<WireReply>::err(MakeError(
            ErrorCode::InvalidArgument,
            "rpc tcp transport: address requires tcp transport"));
    }
    std::string host;
    std::uint16_t port = 0;
    if (auto parsed = detail::ParseTcpEndpoint(address.endpoint, host, port);
        !parsed) {
        return result<WireReply>::err(parsed.error());
    }
    std::error_code addr_error;
    const auto target = net::Address::Ip(host, static_cast<int>(port), addr_error);
    if (addr_error || !target.Valid()) {
        return result<WireReply>::err(MakeError(
            ErrorCode::InvalidArgument,
            "rpc tcp transport: invalid endpoint address"));
    }
    auto frame = EncodeRequestFrame(request, state->options.max_frame_bytes);
    if (!frame) {
        return result<WireReply>::err(std::move(frame.error()));
    }
    if (const auto gate = CheckCall(options.deadline)) {
        return result<WireReply>::err(
            detail::TcpCallError(gate, "exchange rejected before dial", false));
    }

    // 一连接一请求一回复：先原子预占名额（不发起 connect），再 CoDial。
    // 超额调用立即 Overloaded（architecture：无隐藏无界队列、不阻塞到
    // connect）；拨号失败/关闭经 lease 归还名额。
    const auto admission = state->Admit();
    if (admission != 0) {
        if (admission == 1) {
            return result<WireReply>::err(MakeError(
                ErrorCode::Closed, "rpc tcp transport: closed"));
        }
        return result<WireReply>::err(MakeError(
            ErrorCode::Overloaded,
            "rpc tcp transport: max_inflight reached"));
    }
    // 拨号段 lease：连接未交付时 Release 无效句柄（仅归还名额）。
    struct DialLease {
        std::shared_ptr<State> state;
        net::CoTcpConnection connection;
        ~DialLease() {
            if (connection.Raw().Valid()) { connection.RequestClose(); }
            state->Release(connection.Raw());
        }
    } lease{state, {}};
    const auto dialed = state->engine->CoDial(target, {options.deadline});
    if (dialed.error) {
        return result<WireReply>::err(
            detail::TcpCallError(dialed.error, "connect failed", false));
    }
    lease.connection = dialed.connection;
    if (!state->Publish(lease.connection.Raw())) {
        // 拨号完成时 transport 已封口：连接不在 Close 名单上，主动封口并
        // 结算 Closed——不写请求帧（lease 析构再幂等 RequestClose+归还名额）。
        lease.connection.RequestClose();
        return result<WireReply>::err(MakeError(
            ErrorCode::Closed, "rpc tcp transport: closed during dial"));
    }
    auto& connection = lease.connection;

    // 写整帧：全部帧字节本地完成才推进 committed。
    auto settled = detail::TcpSettleWrite(
        connection.Write(std::move(frame).value(), {options.deadline}),
        "request write did not complete");
    if (!settled) {
        return result<WireReply>::err(std::move(settled.error()));
    }

    // 读一条响应帧：先 4 前缀，再按长度读 body；多余字节为协议违规。
    std::string pending;
    std::vector<std::uint8_t> prefix;
    auto status = FillUpTo(connection, pending, detail::kTcpFramePrefixBytes,
                           options.deadline, prefix);
    if (!status) {
        return result<WireReply>::err(
            MakeError(ErrorCode::InternalError, "rpc tcp: read failed"));
    }
    if (const auto& read_error = status.value()) {
        return result<WireReply>::err(detail::TcpCallError(
            read_error, "response prefix read ended", true));
    }
    const auto payload_length =
        detail::TcpFrameReadPrefix(reinterpret_cast<const char*>(prefix.data()));
    if (payload_length == 0 ||
        payload_length > state->options.max_frame_bytes) {
        return result<WireReply>::err(detail::TcpProtocolError(
            "response frame length out of bounds", true));
    }
    std::vector<std::uint8_t> body;
    status = FillUpTo(connection, pending, payload_length,
                      options.deadline, body);
    const auto& body_error = status.value();
    if (body_error) {
        return result<WireReply>::err(detail::TcpCallError(
            body_error, "response body read ended", true));
    }
    // 单条响应帧之外的字节是协议违规（architecture §6.3）：prefetch 已进
    // pending 立即判违规；pending 空再在剩余期限内做一次真实 Read——收到
    // 任何字节即违规，EOF/关闭/期限到则单帧已完整、判定干净终态。
    if (!pending.empty()) {
        return result<WireReply>::err(detail::TcpProtocolError(
            "response frame has trailing bytes", true));
    }
    {
        if (const auto gate = CheckCall(options.deadline)) {
            // 期限已到而 pending 空：单帧已完整到达，按干净终态收口，
            // 不再等待更多字节。
            (void)gate;
        } else {
            const auto trailing = connection.Read({options.deadline});
            if (trailing.state == net::ReadState::Data) {
                return result<WireReply>::err(detail::TcpProtocolError(
                    "response frame has trailing bytes", true));
            }
        }
    }
    WireReply reply;
    reply.body = std::move(body);
    return result<WireReply>::ok(std::move(reply));
}

void TcpRpcTransport::Close() noexcept {
    if (m_state) { m_state->Seal(); }
}

bool TcpRpcTransport::IsClosed() const noexcept {
    if (!m_state) { return true; }
    std::lock_guard<std::mutex> lock(m_state->mutex);
    return m_state->closed;
}

// --- TcpRpcInboundTransport ----------------------------------------------------------

// 有界 accept/session/handler 台账（与 http::Server::State 同构裁剪版：
// v1 一连接一请求一回复，无 keep-alive 循环、无 streaming 源）。
struct TcpRpcInboundTransport::State
    : std::enable_shared_from_this<TcpRpcInboundTransport::State> {
    struct Session {
        net::Connection connection;
        bool authorized{false};   // handler+回复责任起止（Drain 计据）
        bool task_done{false};
    };
    // 最后一个 callback 捕获销毁时才减任务数；不是 handler 返回即算任务结束。
    struct TaskLease {
        std::shared_ptr<State> state;
        std::shared_ptr<Session> session;
        ~TaskLease() {
            if (!state) { return; }  // 未计数（停止拒绝/失败回滚）无计数责任
            std::lock_guard<std::mutex> lock(state->mutex);
            if (session) { session->task_done = true; }
            --state->tasks;
            state->cv.notify_all();
        }
    };

    std::shared_ptr<net::Engine> engine;
    // wire handler 由本 State 与 rpc::Server 共同强持有：在途调用 lock
    // 期间引用稳定，Server 释放后本 State 仍持有一份，回调不会悬空。
    std::shared_ptr<WireInboundHandler> handler;
    TcpRpcServerOptions options;
    mutable std::mutex mutex;
    std::condition_variable cv;
    bool started{false};
    bool stopping{false};
    bool closing{false};
    std::size_t accepted{0}, inflight{0}, tasks{0};
    std::vector<std::shared_ptr<Session>> sessions;

    void Sweep() {
        sessions.erase(std::remove_if(sessions.begin(), sessions.end(),
            [](const auto& session) {
                return session->task_done && session->connection.Released();
            }),
            sessions.end());
    }

    bool Submit(const std::shared_ptr<Session>& session) {
        auto lease = std::make_shared<TaskLease>();
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (stopping) {
                if (session) { session->task_done = true; }
                return false;  // lease 无 state（未计数），析构无下溢
            }
            ++tasks;
            lease->state = shared_from_this();
            lease->session = session;
        }
        bool registered = false;
        g_scheduler->RegistCoroutineTask(128 * 1024, [lease] {
            auto state = lease->state;
            auto session = lease->session;
            try {
                if (session) { state->Serve(session); }
                else { state->Accept(); }
            } catch (...) {
                // 包括分配异常；accept 任务异常不遗失封口责任。
                if (!session) { state->Stop(false); }
            }
            if (session) {
                session->connection.RequestClose();
                std::lock_guard<std::mutex> lock(state->mutex);
                if (session->authorized) {
                    session->authorized = false;
                    --state->inflight;
                    state->cv.notify_all();
                }
            }
        }, registered);
        if (!registered) {
            std::lock_guard<std::mutex> lock(mutex);
            --tasks;
            lease->state.reset();  // 回滚计数责任，析构不再 --tasks
            if (session) { session->task_done = true; }
            cv.notify_all();
        }
        return registered;
    }

    void Stop(bool force) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            stopping = true;
            closing = closing || force;
            for (const auto& session : sessions) {
                if (closing || !session->authorized) {
                    session->connection.RequestClose();
                }
            }
            cv.notify_all();
        }
        engine->RequestStopAccepting();
    }

    void Accept() {
        for (;;) {
            bool full;
            {
                std::lock_guard<std::mutex> lock(mutex);
                Sweep();
                if (stopping) { return; }
                full = sessions.size() >= options.limits.max_connections;
            }
            if (full) { bbtco_sleep(5); continue; }
            auto session = std::make_shared<Session>();
            const auto accept_result = engine->CoAccept({Clock::now() + 50ms});
            if (accept_result.error == std::errc::timed_out) { continue; }
            if (accept_result.error) { Stop(false); return; }
            session->connection = accept_result.connection.Raw();
            {
                std::lock_guard<std::mutex> lock(mutex);
                ++accepted;
                sessions.push_back(session);
                if (stopping) {
                    session->connection.RequestClose();
                    session->task_done = true;
                    return;
                }
            }
            try {
                if (!Submit(session)) { session->connection.RequestClose(); }
            } catch (...) {
                session->connection.RequestClose();
                std::lock_guard<std::mutex> lock(mutex);
                session->task_done = true;
            }
            bbtco_yield;
        }
    }

    // 一连接一请求一回复：读 4 前缀 → 校验长度 → 读 body → handler →
    // 写一回复帧 → 返回（连接由 Submit 尾段 RequestClose）。
    void Serve(const std::shared_ptr<Session>& session) {
        const auto deadline = Clock::now() + options.limits.incoming_timeout;
        net::CoTcpConnection connection{session->connection};
        std::string pending;
        std::vector<std::uint8_t> prefix;
        auto status = FillUpTo(connection, pending, detail::kTcpFramePrefixBytes,
                               deadline, prefix);
        if (!status || status.value()) { return; }  // 未收满前缀的断连静默放弃
        const auto payload_length = detail::TcpFrameReadPrefix(
            reinterpret_cast<const char*>(prefix.data()));
        if (payload_length == 0 ||
            payload_length > options.max_frame_bytes) {
            return;  // 无法定界完整帧，不回复（契约 §6）
        }
        std::vector<std::uint8_t> body;
        status = FillUpTo(connection, pending, payload_length, deadline, body);
        if (!status || status.value()) { return; }  // 截断 body：静默放弃

        {
            std::lock_guard<std::mutex> lock(mutex);
            if (stopping || Clock::now() >= deadline ||
                inflight >= options.limits.max_inflight) {
                return;
            }
            session->authorized = true;
            ++inflight;
        }
        // wire handler 与 rpc::Server 共同强持有（本 State 一份）：在途
        // 调用期间引用稳定，Server 释放后本 State 仍持有一份，回调不悬空；
        // 回调异常隔离为不写回复（本连接明确放弃），授权责任仍含回复写。
        auto reply = result<WireReply>::err(
            MakeError(ErrorCode::InternalError, "rpc tcp: handler failed"));
        try {
            const auto wire_handler = handler;
            if (wire_handler) {
                reply = (*wire_handler)({deadline, {}},
                                        WireRequest{std::move(body)});
            }
        } catch (...) { /* handler 异常按不写回复收口 */ }
        // closing 下不再写回复；写路径持锁外执行（Write 可挂起，持锁会
        // 阻塞 Stop/Close 的收敛推进）。
        bool sealed = false;
        {
            std::lock_guard<std::mutex> lock(mutex);
            sealed = closing;
        }
        bool sent = false;
        if (!sealed && reply && !CheckCall(deadline)) {
            auto frame = EncodeReplyFrame(reply.value(),
                                          options.max_frame_bytes);
            if (frame) {
                const auto report =
                    connection.Write(std::move(frame).value(), {deadline});
                sent = !report.wait_error &&
                       report.outcome.phase == net::WritePhase::Done &&
                       !report.outcome.error;
            }
        }
        {
            std::lock_guard<std::mutex> lock(mutex);
            session->authorized = false;
            --inflight;
            cv.notify_all();
        }
        if (!sent) { session->connection.RequestClose(); }
    }
};

TcpRpcInboundTransport::TcpRpcInboundTransport(
    std::shared_ptr<State> state) noexcept
    : m_state(std::move(state)) {}

TcpRpcInboundTransport::~TcpRpcInboundTransport() noexcept { Close(); }

result<std::shared_ptr<TcpRpcInboundTransport>> TcpRpcInboundTransport::Create(
    std::shared_ptr<net::Engine> engine, TcpRpcServerOptions options) {
    using R = result<std::shared_ptr<TcpRpcInboundTransport>>;
    const auto valid = ValidateNetworkLimits(options.limits);
    if (!engine || !valid || options.handoff_capacity == 0 ||
        options.handoff_capacity > kNetworkLimitsMaxConnections ||
        options.max_frame_bytes == 0 ||
        options.max_frame_bytes > kNetworkLimitsMaxBodyBytes) {
        return R::err(MakeError(ErrorCode::InvalidArgument,
            "rpc tcp inbound: invalid options"));
    }
    if (engine->Phase() != net::EnginePhase::Idle) {
        return R::err(MakeError(ErrorCode::InvalidContext,
            "rpc tcp inbound: Create requires Idle Engine"));
    }
    auto state = std::make_shared<State>();
    state->engine = std::move(engine);
    state->options = options;
    state->sessions.reserve(options.limits.max_connections);
    auto transport = std::shared_ptr<TcpRpcInboundTransport>(
        new TcpRpcInboundTransport(state));
    if (state->engine->EnableAcceptHandoff(options.handoff_capacity)) {
        return R::err(MakeError(ErrorCode::InvalidContext,
            "rpc tcp inbound: accept handoff configuration failed"));
    }
    return R::ok(std::move(transport));
}

result<void> TcpRpcInboundTransport::Start(WireInboundHandler handler) {
    if (!handler) {
        return Failure(ErrorCode::InvalidArgument,
                       "rpc tcp inbound: handler must not be null");
    }
    auto state = m_state;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->stopping || state->started) {
            return Failure(ErrorCode::Closed,
                           "rpc tcp inbound: already started or sealed");
        }
        std::vector<net::Address> addresses;
        if (!g_scheduler->IsInitialized() ||
            state->engine->Phase() != net::EnginePhase::Running ||
            state->engine->LocalAddresses(addresses) || addresses.empty()) {
            return Failure(ErrorCode::InvalidContext,
                "rpc tcp inbound: Start requires Running listening Engine");
        }
        state->handler = std::make_shared<WireInboundHandler>(
            std::move(handler));
        state->started = true;
    }
    try {
        if (state->Submit({})) { return result<void>::ok(); }
    } catch (...) { }
    state->Stop(false);
    return Failure(ErrorCode::RuntimeUnavailable,
                   "rpc tcp inbound: accept task registration failed");
}

void TcpRpcInboundTransport::StopAccepting() noexcept {
    m_state->Stop(false);
}

result<void> TcpRpcInboundTransport::Drain(bbt::coroutine::Deadline deadline) {
    if (!NativeHost()) {
        return Failure(ErrorCode::InvalidContext,
                       "rpc tcp inbound: Drain requires native host");
    }
    auto state = m_state;
    std::unique_lock<std::mutex> lock(state->mutex);
    if (!state->stopping) {
        return Failure(ErrorCode::InvalidArgument,
                       "rpc tcp inbound: StopAccepting before Drain");
    }
    if (!state->cv.wait_until(lock, deadline,
                              [&] { return state->inflight == 0; })) {
        return Failure(ErrorCode::TimedOut,
            "rpc tcp inbound: Drain deadline expired; handler/reply "
            "responsibility retained");
    }
    return result<void>::ok();
}

void TcpRpcInboundTransport::Close() noexcept {
    if (m_state) { m_state->Stop(true); }
}

result<void> TcpRpcInboundTransport::WaitStopped(
    bbt::coroutine::Deadline deadline) {
    if (!NativeHost()) {
        return Failure(ErrorCode::InvalidContext,
                       "rpc tcp inbound: WaitStopped requires native host");
    }
    auto state = m_state;
    std::unique_lock<std::mutex> lock(state->mutex);
    if (!state->stopping) {
        return Failure(ErrorCode::InvalidArgument,
                       "rpc tcp inbound: StopAccepting before WaitStopped");
    }
    for (;;) {
        state->Sweep();
        if (state->tasks == 0 && state->sessions.empty() &&
            state->engine->AcceptingStopped()) {
            return result<void>::ok();
        }
        if (Clock::now() >= deadline) {
            return Failure(ErrorCode::TimedOut,
                "rpc tcp inbound: physical stop deadline expired");
        }
        state->cv.wait_until(lock, std::min(deadline, Clock::now() + 5ms));
    }
}

bool TcpRpcInboundTransport::IsClosed() const noexcept {
    if (!m_state) { return true; }
    std::lock_guard<std::mutex> lock(m_state->mutex);
    return m_state->closing;
}

} // namespace bbt::infra::rpc
