// 入站帧按关联表或接纳路径分发；接纳项再按服务路由调度。
#include <bbt/infra/rpc/CoRpcServer.hpp>
#include <bbt/infra/rpc/detail/RpcSessionState.hpp>
#include <bbt/infra/rpc/detail/Session.hpp>

#include <algorithm>
#include <chrono>
#include <string>
#include <utility>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

namespace bbt::infra::rpc {

using namespace std::chrono_literals;

namespace {

/**
 * reader 上的 admission 拒绝路径——唯一 reader 不能调用会挂起等写终态
 * 的 Complete/Fail（背压会阻塞 reader，无法继续解后续 Response/System 帧）。
 * 本函数直接构造错误终态帧入队，不等待写终态：写责任仍由 WriterLoop/Kill
 * 在真实终态唯一结算，reader 立即返回继续 Read。terminal 已在本路径
 * 置位（调用方先取 IsTerminal），这里不再二次判定。
 */
void PostReplyErrorNoWait(
    const std::shared_ptr<SessionCore>& session, const detail::ReplyCompletionImpl& impl, Error error)
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
    item.deadline = RpcClock::now() + 5s;
    item.settle = std::make_shared<SessionCore::OutItem::Settle>();

    /**
     * 不挂 reader：写终态由 settle/WriterLoop 结算，不入队失败也只是
     * 该条回复终态丢失——link 与后续帧继续进展。
     */
    (void)session->Enqueue(std::move(item));
}

} // namespace

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
        item.deadline = RpcClock::now() + 5s;
        (void)Enqueue(std::move(item));
    }
}

void SessionCore::RejectProtocol(const std::string& message)
{
    if (dead.load()) {
        return;
    }
    SessionFrame frame;
    frame.kind = FrameKind::SessionError;
    frame.traffic = TrafficClass::System;
    frame.session_epoch = epoch;
    frame.has_outcome = true;
    frame.success = false;
    frame.error = Err(ErrorCode::ProtocolError, message);
    auto bytes = EncodeFramedSessionFrame(frame, owner->options.MaxFrameBytes());
    if (bytes) {
        OutItem item;
        item.bytes.assign(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
        item.lane = TrafficClass::System;
        item.deadline = RpcClock::now() + 5s;
        item.settle = std::make_shared<OutItem::Settle>();
        auto settle = item.settle;
        if (Enqueue(std::move(item))) {

            /**
             * 终态控制帧有界等待真实写结算；写不出也不阻塞退役。
             */
            (void)settle->Wait(RpcClock::now() + 500ms);
        }
    }
    Kill(std::make_error_code(std::errc::protocol_error));
}

void SessionCore::AdmitInbound(const SessionFrame& frame, Request request)
{
    auto server = this->server.lock();
    auto& stats = out_lanes[LaneIndex(request.Traffic())];
    std::shared_ptr<AdmissionQueue> queue;
    if (server) {

        /**
         * queue 指针写入受 server->mu 保护（ServerStart）；观察面取同一
         * 临界区快照，不与 Start/Close 的台账写并发裸读。
         */
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

    /**
     * MaxInflight 覆盖「已交 admission 仍未到责任终态」的持有 lease——
     * 在唯一 +1 之前按容量拒绝（拒绝立即 Overloaded，不等 deadline）。
     * 计数与额度检查在同一 server->mu 临界区，杜绝越过上限的竞态。
     */
    bool over_cap = false;
    {
        std::lock_guard<std::mutex> lock(server->mu);
        if (server->inflight[item_lane] >= server->options.MaxInflight()) {
            ++server->dropped[item_lane];
            over_cap = true;
        } else {
            server->NoteInflight(item_lane, +1);

            /**
             * 只有会写回复终态的 Call / 要求远端确认的 Send 才占排空计数；
             * 不要求确认的 Send 没有可结算的回复写责任，不能计入。
             */
            if (item.reply) {
                ++server->open_replies;
            }
        }
    }
    if (over_cap) {
        {
            std::lock_guard<std::mutex> slock(out_mu);
            ++stats.dropped;
            --stats.inflight;
        }

        /**
         * 保留 reply 凭据发布唯一拒绝终态，连接仍可进展。本路径在
         * 唯一 reader 上——不能调用会挂起等写终态的 Fail（写背压会阻塞
         * reader，无法继续解后续帧）。标 terminal 后直接入队错误帧不等待；
         * 写责任仍由 WriterLoop/Kill 真实结算。
         */
        if (item.reply) {
            if (auto impl = detail::AdmissionAccess::ClaimTerminal(*item.reply)) {
                auto session = impl->session.lock();
                if (session) {
                    PostReplyErrorNoWait(session, *impl, Err(ErrorCode::Overloaded, "inflight limit reached"));
                }
            }
        }

        /**
         * lease 从未 +1（额度检查先于唯一记账）——不 Release，
         * 随 item 析构，不欠账。
         */
        return;
    }

    /**
     * 保留完成凭据/lease 的本地副本——Admit 按值转移 item，拒绝路径
     * 仍需可用 reply/lease 结算唯一终态。
     */
    auto reply = item.reply;
    auto lease = item.lease;
    const auto admitted = queue->Admit(std::move(item));
    if (!admitted) {

        /**
         * Admit 失败（满/封口）：item 未入队，infra 直接发布一次终态——
         * Call → Response error；带确认 Send → SendAccepted error。本路径
         * 同在唯一 reader 上，不等写终态。
         */
        if (reply) {
            if (auto impl = detail::AdmissionAccess::ClaimTerminal(*reply)) {
                auto session = impl->session.lock();
                if (session) {
                    PostReplyErrorNoWait(session, *impl, admitted.error());
                }
            }
        }

        /**
         * lease 单次结算：Release 已完成 inflight-1，这里不再重复扣账。
         */
        lease->Release();
        {
            std::lock_guard<std::mutex> lock(server->mu);
            ++server->dropped[item_lane];

            /**
             * 与入账同一判据：只有持有回复写责任的项才归还 open_replies。
             */
            if (reply && server->open_replies != 0) {
                --server->open_replies;
            }
            if (server->close_signal) {
                server->close_signal->Notify();
            }
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

        /**
         * 旧 epoch 帧丢弃并计数（不改写已发布终态）。
         */
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
            // 同名的新调用可能仍在队列中，此时收到的回复不属于它。
            if (it != calls.end() && it->second->phase == PendingWrite::Phase::Committed) {
                pending = it->second;
            }
        }
        if (!pending) {
            return;
        }
        result<Response> outcome = [&]() -> result<Response> {
            if (frame.has_outcome && frame.success) {
                auto response = ResponseFromSessionFrame(frame);
                if (response && frame.service == pending->service && frame.method == pending->method
                    && frame.request_schema == pending->request_schema
                    && frame.response_schema == pending->response_schema) {
                    return response;
                }
                return result<Response>::err(ErrPhase(
                    ErrorCode::ProtocolError,
                    response ? "response correlation mismatch" : "response decode failed",
                    RequestPhase::ReplyTerminal));
            }
            if (frame.has_outcome) {
                auto error = frame.error;
                if (!error.request_phase.has_value()) {
                    error.request_phase = RequestPhase::ReplyTerminal;
                }
                return result<Response>::err(std::move(error));
            }
            return result<Response>::err(
                ErrPhase(ErrorCode::ProtocolError, "response missing outcome", RequestPhase::ReplyTerminal));
        }();
        {
            std::lock_guard<std::mutex> lock(pending_mu);
            // 解码期间等待方或关闭方可能已结算，发布时再认领原登记。
            if (!ErasePending(calls, frame.request_id, pending)) {
                return;
            }
            pending->outcome = std::move(outcome);
            pending->done = true;
        }
        pending->waiter->Notify();
        return;
    }
    case FrameKind::SendAccepted: {
        std::shared_ptr<PendingSend> pending;
        {
            std::lock_guard<std::mutex> lock(pending_mu);
            auto it = sends.find(frame.message_id);
            if (it != sends.end() && it->second->phase == PendingWrite::Phase::Committed) {
                pending = it->second;
            }
        }
        if (!pending) {
            return;
        }
        result<void> outcome = [&]() -> result<void> {
            if (frame.has_outcome && frame.success) {
                return result<void>::ok();
            }
            if (frame.has_outcome) {
                return result<void>::err(frame.error);
            }
            return result<void>::err(
                ErrPhase(ErrorCode::ProtocolError, "send-accepted missing outcome", RequestPhase::ReplyTerminal));
        }();
        {
            std::lock_guard<std::mutex> lock(pending_mu);
            if (!ErasePending(sends, frame.message_id, pending)) {
                return;
            }
            pending->outcome = std::move(outcome);
            pending->done = true;
        }
        pending->waiter->Notify();
        return;
    }
    case FrameKind::Call:
    case FrameKind::Send: {

        /**
         * 同 session 同 ordering_key 的入站顺序台账：首个有序帧即基值，此后
         * 必须逐帧连续递增。重复/逆序（seq <= last）与缺口（seq != last+1）
         * 都按协议违规退役本 link——单 reader 的 wire 到达序是唯一事实源，
         * 缺口在可靠字节流上不会自愈，等待没有对象可等。
         */
        if (frame.sequence != 0) {
            const auto it = inbound_sequence.find(frame.ordering_key);
            if (it != inbound_sequence.end() && (frame.sequence <= it->second || frame.sequence != it->second + 1)) {
                RejectProtocol("ordering sequence violation");
                return;
            }
            inbound_sequence[frame.ordering_key] = frame.sequence;
        }
        auto request = RequestFromSessionFrame(frame);
        if (!request) {
            PostSessionError(request.error());
            return;
        }
        RequestContext context;
        const auto now = RpcClock::now();
        const auto remote = now + std::chrono::milliseconds{frame.remaining_budget_ms};
        const auto local = [this, now] {
            if (!outbound) {
                if (auto server = this->server.lock()) {
                    return now + server->options.IncomingTimeout();
                }
            }
            return RpcClock::time_point::max();
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
        RejectProtocol("unexpected frame kind on live session");
        return;
    }
}

namespace {

std::string RouteKey(const Request& request)
{
    std::string key = request.Service();
    key.push_back('/');
    key.append(request.Method());
    return key;
}

bool ConsumeRouteToken(ServerState::Route& route)
{
    if (route.options.burst_tokens == 0) {
        return true;
    }
    const auto now = RpcClock::now();
    const auto elapsed = std::chrono::duration<double>(now - route.last_refill).count();
    route.tokens = std::min(
        static_cast<double>(route.options.burst_tokens), route.tokens + elapsed * route.options.refill_per_second);
    route.last_refill = now;
    if (route.tokens < 1.0) {
        return false;
    }
    route.tokens -= 1.0;
    return true;
}

/**
 * 已授权路由 handler 的责任标记：以协程 id 登记，Close/WaitStopped 据此
 * 在副作用前拒绝自等待；作用域结束（含异常）移除。
 */
class RouteResponsibility {
public:
    explicit RouteResponsibility(std::shared_ptr<ServerState> server)
        : m_server(std::move(server))
    {
        m_id = bbt::coroutine::GetLocalCoroutineId();
        if (m_id != 0) {
            std::lock_guard<std::mutex> lock(m_server->mu);
            m_server->responsibility_tasks.insert(m_id);
        }
    }

    ~RouteResponsibility()
    {
        if (m_id != 0) {
            std::lock_guard<std::mutex> lock(m_server->mu);
            m_server->responsibility_tasks.erase(m_id);
        }
    }

    RouteResponsibility(const RouteResponsibility&) = delete;
    RouteResponsibility& operator=(const RouteResponsibility&) = delete;

private:
    std::shared_ptr<ServerState> m_server;
    bbt::coroutine::detail::CoroutineId m_id{0};
};

void RunRoute(std::shared_ptr<ServerState> server, CoRpcServer::RpcHandler handler, AdmissionItem item)
{
    RouteResponsibility responsibility(std::move(server));
    try {
        auto response = bbt_co_await handler(std::move(item.request));
        if (item.reply) {
            if (response) {
                const auto completion = bbt_co_await item.reply->Complete(std::move(response).value());
                (void)completion;
            } else {
                const auto completion = bbt_co_await item.reply->Fail(response.error());
                (void)completion;
            }
        }
    } catch (...) {
        if (item.reply) {
            const auto completion =
                bbt_co_await item.reply->Fail(Err(ErrorCode::InternalError, "rpc route handler threw"));
            (void)completion;
        }
    }
    if (item.lease) {
        item.lease->Release();
    }
}

} // namespace

result<void> detail::DispatchAdmission(const std::shared_ptr<ServerState>& server, AdmissionItem item)
{
    CoRpcServer::RpcHandler route_handler;
    AdmissionHandler fallback;
    {
        std::lock_guard<std::mutex> lock(server->mu);
        const auto it = server->routes.find(RouteKey(item.request));
        if (it == server->routes.end()) {
            fallback = server->framework_handler;
        } else {
            if (item.request.Reply() != ReplyMode::Required) {
                return ErrV(ErrorCode::InvalidArgument, "registered rpc route requires a Call request");
            }
            if (!ConsumeRouteToken(it->second)) {
                return ErrV(ErrorCode::Overloaded, "rpc route rate limit exceeded");
            }
            route_handler = it->second.handler;
        }
    }
    if (!route_handler) {
        if (!fallback) {
            return ErrV(ErrorCode::Unavailable, "rpc server has no admission handler");
        }
        RouteResponsibility responsibility(server);
        return fallback(std::move(item));
    }

    bool registered = false;
    // bbt-lint: ignore-next-line coroutine-sugar -- route task needs the explicit 128 KiB stack
    g_scheduler->RegistCoroutineTask(
        128 * 1024,
        [server, handler = std::move(route_handler), item = std::move(item)]() mutable {
            RunRoute(server, std::move(handler), std::move(item));
        },
        registered);
    if (!registered) {
        return ErrV(ErrorCode::RuntimeUnavailable, "rpc route task registration failed");
    }

    return result<void>::ok();
}

} // namespace bbt::infra::rpc
