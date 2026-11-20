// owner 的连接复用、握手交接、路由选举和出站操作。
#include <bbt/infra/rpc/detail/RpcSessionState.hpp>
#include <bbt/infra/rpc/detail/Session.hpp>
#include <bbt/infra/rpc/detail/SessionCodecInternal.hpp>
#ifdef BBT_INFRA_RPC_TEST_ATTEMPT_BARRIER
#include <bbt/infra/rpc/detail/SessionTestAccess.hpp>
#endif

#include <algorithm>
#include <chrono>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

namespace bbt::infra::rpc {

using namespace std::chrono_literals;
using bbt::coroutine::WaitStatus;
using bbt::coroutine::sync::CoWaiter;

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

/**
 * 在发布事务内交接连接；替换只改路由赢家，不移除旧 link 的租约。
 * 同时拨号时 identity 较小侧保留其本地发起 link。
 */
result<std::shared_ptr<SessionCore>> InstallSession(
    std::shared_ptr<SessionOwnerState> owner,
    std::shared_ptr<SessionCore> session,
    const std::string& endpoint_key,
    SessionFrameDecoder&& decoder,
    const std::shared_ptr<SessionOwnerState::Establishing>& establishing,
    const std::shared_ptr<ServerState>& inbound = {})
{
    using R = result<std::shared_ptr<SessionCore>>;
    std::shared_ptr<SessionCore> evicted;
    {
        std::unique_lock<std::mutex> lock(owner->mu);
        if (owner->closed) {
            return R::err(Err(ErrorCode::Closed, "owner closed"));
        }
        std::unique_lock<std::mutex> inbound_lock;
        if (inbound) {
            inbound_lock = std::unique_lock<std::mutex>(inbound->mu);
            if (inbound->stopping) {
                return R::err(Err(ErrorCode::Closed, "server closed"));
            }
        }
        owner->ReapReleasedLocked();
        if (auto existing = owner->by_identity.find(session->peer_identity); existing != owner->by_identity.end()) {
            evicted = existing->second.lock();
        }
        const bool keep_outbound = owner->options.LocalIdentity() < session->peer_identity;
        if (evicted && !evicted->dead.load() && keep_outbound != session->outbound) {
            if (!endpoint_key.empty()) {
                owner->by_endpoint[endpoint_key] = evicted;
            }
            if (inbound) {
                ++inbound->accepted;
                inbound_lock.unlock();
            }
            lock.unlock();
            session->channel->RequestClose(std::make_error_code(std::errc::connection_reset));
            return R::ok(evicted);
        }
        if (!establishing && owner->OccupiedLocked() >= owner->options.MaxSessions()) {
            return R::err(Err(ErrorCode::Overloaded, "max sessions reached"));
        }

        const LinkLease lease{session, session->channel->Raw()};
        // 所有可分配步骤先于发布；失败时原握手台账仍持有连接。
        owner->link_leases.reserve(owner->link_leases.size() + 1);
        if (inbound) {
            inbound->links.reserve(inbound->links.size() + 1);
        }
        auto& identity_slot = owner->by_identity[session->peer_identity];
        if (!endpoint_key.empty()) {
            owner->by_endpoint[endpoint_key] = session;
        }
        session->TakeHandshakeDecoder(std::move(decoder));
        if (session->outbound) {
            session->server = owner->inbound_server;
        }
        // 发布后 Close 可立即 Kill；任务登记结束前也必须保留租约。
        session->responsibilities.store(2);
        owner->link_leases.push_back(lease);
        if (inbound) {
            inbound->links.push_back(lease);
            inbound->handshakes.at(lease.conn.Id()).conn = {};
        } else {
            establishing->reserved = false;
            establishing->conn = {};
            establishing->reclaim = {};
        }
        identity_slot = session;
        if (evicted) {
            for (auto& [key, weak] : owner->by_endpoint) {
                if (auto prev = weak.lock(); prev == evicted) {
                    weak = session;
                }
            }
        }
    }
    bool registered;
    try {
        if (evicted) {
            evicted->Kill({});
        }
        registered = session->Submit(128 * 1024, [s = session] { s->ReaderLoop(); })
                     && session->Submit(128 * 1024, [s = session] { s->WriterLoop(); });
    } catch (...) {
        session->Kill({});
        session->ExitDone();
        throw;
    }
    if (!registered) {
        session->Kill({});
    }
    session->ExitDone();
    if (!registered) {
        return R::err(Err(ErrorCode::RuntimeUnavailable, "session task registration failed"));
    }
    if (inbound) {
        std::lock_guard<std::mutex> lock(inbound->mu);
        ++inbound->accepted;
    }
    return R::ok(session);
}

/**
 * 拨号 + 握手（在调用方受管协程内执行）。establishing 记录由 GetSession
 * 同事务创建并置 reserved（建立中额度占位），这里透传给 InstallSession——
 * 安装成功时 该额度交给 link 租约。
 */
result<std::shared_ptr<SessionCore>> EstablishOutbound(
    std::shared_ptr<SessionOwnerState> owner,
    const net::Address& target,
    const std::string& endpoint_key,
    const std::shared_ptr<SessionOwnerState::Establishing>& establishing,
    bbt::coroutine::Deadline deadline)
{
    using R = result<std::shared_ptr<SessionCore>>;
    const auto dialed = owner->engine->CoDial(target, {deadline});
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        // 失败也可能已有排队或 fd 责任；未交付时由 net owner 关闭。
        establishing->reclaim = dialed.reclaim;
        if (dialed.error) {
            return R::err(detail::TcpCallError(dialed.error, "connect failed", false));
        }
        establishing->conn = dialed.connection.Raw();
        if (owner->closed) {
            return R::err(Err(ErrorCode::Closed, "owner closed"));
        }
    }
    detail::TcpChannel channel(dialed.connection);

    /**
     * 固定动作检查点：拨号已成功、额度已占，link 还没发布。Pause 在库内
     * 分片等待；失败/注入转成可见错误后，establishing 记录、额度与 conn 由
     * GetSession 的共同收尾归还——异常不越过该收尾块，也不在这里另造一条
     * 半截清理路径。
     */
    if (const auto gate = detail::RunHookCheckpoint(
            *owner->hooks,
            HookPoint::OutboundBeforeHandshake,
            HookGate{owner.get(), nullptr, nullptr, &channel, deadline});
        !gate) {
        channel.RequestClose();
        return R::err(gate.error());
    }
    std::string peer_identity;
    const auto proposal = NextEpoch(*owner);
    const auto nonce = NextNonce(*owner);
    SessionFrameDecoder decoder(owner->options.MaxFrameBytes());
    auto epoch =
        detail::OutboundHandshakeOnChannel(channel, owner->options, proposal, nonce, peer_identity, decoder, deadline);
    if (!epoch) {
        channel.RequestClose();
        return R::err(epoch.error());
    }
    auto session = std::make_shared<SessionCore>();
    session->owner = owner;
    session->channel = std::make_shared<detail::TcpChannel>(std::move(channel));
    session->peer_identity = std::move(peer_identity);
    session->outbound = true;
    session->epoch = epoch.value();
    return InstallSession(owner, session, endpoint_key, std::move(decoder), establishing);
}

/**
 * 获取或建立 session（在调用方协程内；并发同 target 只建一次）。
 * CoWaiter 只有一个等待位；各等待方分片检查同一结果，独立响应期限和取消。
 */
result<std::shared_ptr<SessionCore>> GetSession(
    std::shared_ptr<SessionOwnerState> owner,
    const std::string& endpoint_key,
    const net::Address& target,
    bbt::coroutine::Deadline deadline)
{
    using R = result<std::shared_ptr<SessionCore>>;
    using Establishing = SessionOwnerState::Establishing;
    std::shared_ptr<Establishing> mine;
    std::shared_ptr<Establishing::Completion> completion;
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        if (owner->closed) {
            return R::err(Err(ErrorCode::Closed, "owner closed"));
        }

        if (auto it = owner->by_endpoint.find(endpoint_key); it != owner->by_endpoint.end()) {
            if (auto session = it->second.lock(); session && !session->dead.load()) {
                return R::ok(session);
            }
            owner->by_endpoint.erase(it);
        }
        owner->ReapReleasedLocked();
        if (auto it = owner->establishing.find(endpoint_key); it != owner->establishing.end()) {
            if (it->second->done) {
                return R::err(Err(ErrorCode::Overloaded, "previous connection release pending"));
            }

            /**
             * done 发布前，拨号方必定仍持有结果槽。登记和发布共用 owner->mu。
             */
            completion = it->second->completion.lock();
        } else {
            if (owner->OccupiedLocked() >= owner->options.MaxSessions()) {
                return R::err(Err(ErrorCode::Overloaded, "max sessions reached (pre-dial)"));
            }
            mine = std::make_shared<Establishing>();
            completion = std::make_shared<Establishing::Completion>();
            mine->completion = completion;
            owner->establishing[endpoint_key] = mine;
        }
    }
    if (!mine) {
        auto self = bbt::coroutine::sync::CoWaiter::Create();
        for (;;) {
            {
                std::lock_guard<std::mutex> lock(owner->mu);
                if (owner->closed) {
                    return R::err(Err(ErrorCode::Closed, "owner closed"));
                }
                if (completion->outcome) {
                    return *completion->outcome;
                }
            }
            if (RpcClock::now() >= deadline) {
                return R::err(
                    detail::TcpCallError(
                        std::make_error_code(std::errc::timed_out), "session establish wait timed out", false));
            }
            const auto status = self->Wait({std::min(deadline, RpcClock::now() + 10ms)});
            if (status != WaitStatus::TimedOut) {
                return R::err(WaitFailure(status, "session establish wait ended"));
            }
        }
    }

    /**
     * 本协程执行拨号。
     * 异常全路径归还：EstablishOutbound 内部已把检查点注入与失败转成可见
     * 错误，这里再兜一层——任何逃出的异常都不能跳过下面的收尾块，否则
     * establishing 记录、reserved 额度与 done 位会永久留下。异常转成
     * InternalError 返回，收尾块照常执行。
     */
    result<std::shared_ptr<SessionCore>> made = R::err(Err(ErrorCode::InternalError, "establish did not run"));
    try {
        made = EstablishOutbound(owner, target, endpoint_key, mine, deadline);
    } catch (const std::exception& error) {
        made = R::err(Err(ErrorCode::InternalError, std::string("session establish threw: ") + error.what()));
    } catch (...) {
        made = R::err(Err(ErrorCode::InternalError, "session establish threw"));
    }
    if (mine->conn.Valid()) {
        (void)mine->conn.Close();
    }
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        completion->outcome.emplace(std::move(made));
        mine->completion.reset();
        mine->conn = {};
        mine->done = true;
        if (mine->reclaim.Released()) {
            owner->establishing.erase(endpoint_key);
        }
        owner->close_signal->Notify();
    }
    return *completion->outcome;
}

} // namespace

void detail::ServeInbound(std::shared_ptr<ServerState> server, detail::TcpChannel channel)
{
    const auto deadline = RpcClock::now() + server->options.IncomingTimeout();
    auto owner = server->owner;

    if (const auto gate = detail::RunHookCheckpoint(
            *owner->hooks,
            HookPoint::InboundBeforeHandshake,
            HookGate{owner.get(), server.get(), nullptr, &channel, deadline});
        !gate) {
        return;
    }
    std::string peer_identity;
    const auto proposal = NextEpoch(*owner);
    const auto nonce = NextNonce(*owner);
    SessionFrameDecoder decoder(owner->options.MaxFrameBytes());
    auto epoch =
        detail::InboundHandshakeOnChannel(channel, owner->options, proposal, nonce, peer_identity, decoder, deadline);

    if (!epoch) {
        return;
    }
    auto session = std::make_shared<SessionCore>();
    session->owner = owner;
    session->server = server;
    session->channel = std::make_shared<detail::TcpChannel>(std::move(channel));
    session->peer_identity = std::move(peer_identity);
    session->outbound = false;
    session->epoch = epoch.value();

    if (const auto gate = detail::RunHookCheckpoint(
            *owner->hooks,
            HookPoint::InboundBeforePublish,
            HookGate{owner.get(), server.get(), nullptr, session->channel.get(), deadline});
        !gate) {
        return;
    }
    (void)InstallSession(owner, session, {}, std::move(decoder), nullptr, server);
}

namespace {

/**
 * "tcp://host:port" → numeric host/port（file-local，非成员）。
 */
result<void> ParseTarget(std::string_view target, std::string& host, std::uint16_t& port)
{
    const std::string value(target);
    if (value.size() < 8 || value.compare(0, 6, "tcp://") != 0) {
        return ErrV(ErrorCode::InvalidArgument, "target must be tcp://host:port");
    }
    return detail::ParseTcpEndpoint(value.substr(6), host, port);
}

/**
 * 已接纳调用的一次性责任：与 owner 封口同门登记，作用域结束（含所有
 * 早退/异常路径）归还并通知等待排空的 Close 调用方。
 */
struct OwnerOperationGuard {
    std::shared_ptr<SessionOwnerState> owner;

    ~OwnerOperationGuard() { detail::SessionOwnerEndOperation(owner); }
};

/**
 * 出站关闭原因的统一附加点：Call/Send 返回失败且 owner 已发布原因时，
 * 在调用方可见的返回点把首个关闭原因附到 Error.details；内部各失败出口
 * 只保留失败分类（未提交 Closed / 已提交 OutcomeUnknown+request_phase /
 * 超时 / 取消），不分别补原因。错误本身即关闭原因或已含 close_reason 键
 * 时不重复附加；成功结果不改。
 */
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

constexpr std::string_view kAutoAttemptIdPrefix = "~bbt-rpc:";

result<std::string> NextAttemptId(SessionOwnerState& owner)
{
    // Fencing 后仍用 fresh ID；耗尽时拒绝而不是回卷到旧 ID。
    auto& counter = owner.next_attempt_id;
    auto next = counter.load(std::memory_order_relaxed);
    do {
        if (next == std::numeric_limits<std::uint64_t>::max()) {
            return result<std::string>::err(Err(ErrorCode::Overloaded, "automatic attempt IDs exhausted"));
        }
    } while (!counter.compare_exchange_weak(next, next + 1, std::memory_order_relaxed));
    return result<std::string>::ok(std::string(kAutoAttemptIdPrefix) + std::to_string(next));
}

result<Response> CallImpl(
    const std::shared_ptr<SessionOwnerState>& owner,
    std::string_view target,
    Request request,
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
        return R::err(detail::TcpCallError(gate, "call gate", false));
    }
    if (request.Traffic() == TrafficClass::System || request.Reply() != ReplyMode::Required) {
        return R::err(Err(ErrorCode::InvalidArgument, "Call requires Service/Actor + ReplyRequired"));
    }
    const bool automatic_id = request.RequestId().empty();
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

    /**
     * 同时拨号 fencing 竞速：GetSession 返回的 session 可能在交付/登记前
     * 被选举退役（dead）。by_endpoint 已被胜者接管，重取落到存活 link；
     * pending 登记的 dead 拒绝同样回环一次，不循环拨号。
     */
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

        if (automatic_id) {
            auto id = NextAttemptId(*owner);
            if (!id) {
                return R::err(id.error());
            }
            request.SetRequestId(std::move(id.value()));
        }
        const auto request_id = request.RequestId();
#ifdef BBT_INFRA_RPC_TEST_ATTEMPT_BARRIER
        detail::RunAttemptIdBarrierForTest(request_id);
#endif
        auto budget = Budget(options.deadline);
        if (!budget) {
            return R::err(budget.error());
        }
        auto frame = ToSessionFrame(request, FrameKind::Call, session->epoch, budget.value());
        if (!frame) {
            return R::err(frame.error());
        }
        auto bytes = detail::EncodeFramedSessionBytes(frame.value(), owner->options.MaxFrameBytes());
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
        session->NoteOperationStarted(request.Traffic());
        SessionCore::OutItem item;
        item.bytes = std::move(bytes.value());
        item.lane = request.Traffic();
        item.key = request_id;
        item.pending = pending;
        item.deadline = options.deadline;
        auto enqueued = session->Enqueue(std::move(item));
        if (!enqueued) {
            {
                std::lock_guard<std::mutex> lock(session->pending_mu);
                ErasePending(session->calls, request_id, pending);
            }
            session->NoteEnqueueFailed(request.Traffic());

            /**
             * 退役会话的 Enqueue Closed：同样重取一次落到存活 link。
             */
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

    const auto status = session->WaitPending(*pending, options.deadline);
    std::optional<R> published;
    bool committed;
    const bool withdraw = request.Sequence() == 0 && status != WaitStatus::TimedOut;
    {
        std::lock_guard<std::mutex> lock(session->pending_mu);
        committed = pending->phase == SessionCore::PendingWrite::Phase::Committed;
        if (pending->done) {
            published.emplace(std::move(pending->outcome));
        } else {
            ErasePending(session->calls, request_id, pending);
            // 有序帧须由 writer 结算，否则后继帧会在对端形成序号缺口。
            if (!committed && withdraw) {
                pending->phase = SessionCore::PendingWrite::Phase::Withdrawn;
            }
        }
    }
    session->NoteOperationFinished(request.Traffic(), published.has_value());
    if (published) {
        return std::move(*published);
    }
    if (status == bbt::coroutine::WaitStatus::Cancelled) {
        if (!committed && !withdraw) {
            return R::err(Err(ErrorCode::Cancelled, "ordered call cancelled while queued"));
        }
        return R::err(ErrPhase(
            ErrorCode::Cancelled,
            "call cancelled",
            committed ? RequestPhase::RequestCommitted : RequestPhase::NotStarted));
    }
    if (status != WaitStatus::TimedOut) {
        auto error = WaitFailure(status, "call wait failed");
        if (committed) {
            error.request_phase = RequestPhase::RequestCommitted;
        } else if (withdraw) {
            error.request_phase = RequestPhase::NotStarted;
        }
        return R::err(std::move(error));
    }
    if (committed) {
        return R::err(
            ErrPhase(ErrorCode::OutcomeUnknown, "call timed out after commit", RequestPhase::RequestCommitted));
    }
    return R::err(Err(ErrorCode::TimedOut, "call timed out"));
}

result<void> SendImpl(
    const std::shared_ptr<SessionOwnerState>& owner,
    std::string_view target,
    Request message,
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
        return R::err(detail::TcpCallError(gate, "send gate", false));
    }
    if (message.Traffic() == TrafficClass::System || message.Reply() != ReplyMode::None) {
        return R::err(Err(ErrorCode::InvalidArgument, "Send requires Service/Actor + ReplyNone"));
    }
    const bool automatic_id = message.MessageId().empty();
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

    // 只在入队前的 fencing 竞态重选一次；已入队帧不重发。
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

        if (automatic_id) {
            auto id = NextAttemptId(*owner);
            if (!id) {
                return R::err(id.error());
            }
            message.SetMessageId(std::move(id.value()));
        }
        const auto message_id = message.MessageId();
#ifdef BBT_INFRA_RPC_TEST_ATTEMPT_BARRIER
        detail::RunAttemptIdBarrierForTest(message_id);
#endif
        auto budget = Budget(options.Deadline());
        if (!budget) {
            return R::err(budget.error());
        }
        auto frame = ToSessionFrame(
            message,
            FrameKind::Send,
            session->epoch,
            budget.value(),
            options.RequiresRemoteAcceptance() ? std::optional<bool>{true} : std::nullopt);
        if (!frame) {
            return R::err(frame.error());
        }
        auto bytes = EncodeFramedSessionFrame(frame.value(), owner->options.MaxFrameBytes());
        if (!bytes) {
            return R::err(bytes.error());
        }

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
        session->NoteOperationStarted(message.Traffic());
        SessionCore::OutItem item;
        item.bytes.assign(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
        item.lane = message.Traffic();
        if (pending) {
            item.key = message_id;
            item.pending = pending;
        }
        item.deadline = options.Deadline();
        auto enqueued = session->Enqueue(std::move(item));
        if (!enqueued) {
            if (pending) {
                std::lock_guard<std::mutex> lock(session->pending_mu);
                ErasePending(session->sends, message_id, pending);
            }
            session->NoteEnqueueFailed(message.Traffic());
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
    if (!pending) {
        session->NoteOperationFinished(message.Traffic(), true);
        return R::ok();
    }

    owner->acceptance_pending.fetch_add(1);
    const auto status = session->WaitPending(*pending, options.Deadline());
    owner->acceptance_pending.fetch_sub(1);
    std::optional<R> published;
    bool committed = false;
    {
        std::lock_guard<std::mutex> lock(session->pending_mu);
        committed = pending->phase == SessionCore::PendingWrite::Phase::Committed;
        if (pending->done) {
            published.emplace(std::move(pending->outcome));
        } else {
            // 入队已经接纳发送，取消只解除确认等待。
            ErasePending(session->sends, message_id, pending);
        }
    }
    session->NoteOperationFinished(message.Traffic(), published.has_value());
    if (published) {
        return std::move(*published);
    }
    if (status == bbt::coroutine::WaitStatus::Cancelled) {
        if (committed) {
            return R::err(ErrPhase(ErrorCode::Cancelled, "send cancelled", RequestPhase::RequestCommitted));
        }
        return R::err(Err(ErrorCode::Cancelled, "send acceptance wait cancelled while queued"));
    }
    if (status != WaitStatus::TimedOut) {
        auto error = WaitFailure(status, "send acceptance wait failed");
        if (committed) {
            error.request_phase = RequestPhase::RequestCommitted;
        }
        return R::err(std::move(error));
    }
    if (committed) {
        return R::err(ErrPhase(
            ErrorCode::OutcomeUnknown, "send acceptance timed out after commit", RequestPhase::RequestCommitted));
    }
    return R::err(Err(ErrorCode::TimedOut, "send acceptance timed out"));
}

} // namespace

result<Response> detail::SessionOwnerCall(
    const std::shared_ptr<SessionOwnerState>& owner,
    std::string_view target,
    Request request,
    const CallOptions& options)
{
    auto outcome = CallImpl(owner, target, std::move(request), options);
    if (!outcome) {
        AttachOwnerCloseReason(owner, outcome.error());
    }
    return outcome;
}

result<void> detail::SessionOwnerSend(
    const std::shared_ptr<SessionOwnerState>& owner,
    std::string_view target,
    Request message,
    const SendOptions& options)
{
    auto outcome = SendImpl(owner, target, std::move(message), options);
    if (!outcome) {
        AttachOwnerCloseReason(owner, outcome.error());
    }
    return outcome;
}

/**
 * CoRpcClient::Stats 的真源在 CoRpcClient.cc；本函数由 CoRpcClient.cc 转调。
 */
ClientStats detail::SessionOwnerStats(const std::shared_ptr<SessionOwnerState>& owner) noexcept
{
    ClientStats out;
    out.remote_acceptance_pending = owner->acceptance_pending.load();
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        owner->ReapReleasedLocked();
        out.sessions = out.links = owner->link_leases.size();
        for (const auto& [id, weak] : owner->by_identity) {
            (void)id;
            auto session = weak.lock();
            if (!session) {
                continue;
            }
            session->AccumulateOutboundStats(out);
        }
    }
    return out;
}

} // namespace bbt::infra::rpc
