// RPC admission 公共类型的不透明实现：AdmissionQueue 单 FIFO worker、
// ReplyCompletion 写终态、AdmissionLease 单次释放与 AdmissionAccess 构造
// 接缝。impl 类型见 detail/RpcSessionState.hpp；会话内使用点（reader 的
// AdmitInbound/拒绝路径）在 detail/RpcSession.cc。
#include <bbt/infra/rpc/Admission.hpp>
#include <bbt/infra/rpc/detail/RpcSessionState.hpp>
#include <bbt/infra/rpc/detail/Session.hpp>
#include <bbt/infra/rpc/detail/RpcFrameProtocol.hpp>

#include <algorithm>
#include <chrono>
#include <memory>
#include <mutex>
#include <utility>

namespace bbt::infra::rpc {

using namespace std::chrono_literals;
using bbt::coroutine::WaitStatus;
using bbt::coroutine::sync::CoWaiter;

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
                        server->close_signal->Notify();
                    }
                    return;
                }
            }
            const auto status = impl->signal->Wait({RpcClock::now() + 25ms});
            if (status == bbt::coroutine::WaitStatus::AlreadyWaiting
                || status == bbt::coroutine::WaitStatus::InvalidContext) {
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
    //（ServerV2Close/Stats 持 server->mu 时可经 queue 观察面取 impl->mu；
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
    // 回复写窗口已过：迟到回复对调用方已无交付意义，且把过期期限交给共享
    // stream 会在接纳后超时并把整条 link 退役。只发布本条终态，不产生写责任。
    if (write_deadline != RpcClock::time_point::max() && write_deadline <= RpcClock::now()) {
        return ErrV(ErrorCode::TimedOut, "reply deadline expired before write");
    }
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
            auto bytes = detail::RpcFrameProtocol::Encode(frame, session->owner->options.MaxFrameBytes());
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
                    const auto wait_deadline = write_deadline == RpcClock::time_point::max()
                                                   ? RpcClock::now() + 30s
                                                   : std::min(write_deadline, RpcClock::now() + 30s);
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
    auto bytes = detail::RpcFrameProtocol::Encode(frame, session->owner->options.MaxFrameBytes());
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
    const auto wait_deadline = write_deadline == RpcClock::time_point::max()
                                   ? RpcClock::now() + 30s
                                   : std::min(write_deadline, RpcClock::now() + 30s);
    const auto status = settle->waiter->Wait({wait_deadline});
    if (status == bbt::coroutine::WaitStatus::Completed) {
        return settle->outcome;
    }
    if (status == bbt::coroutine::WaitStatus::Cancelled) {
        return ErrV(ErrorCode::Cancelled, "reply write wait cancelled");
    }
    if (status == bbt::coroutine::WaitStatus::TimedOut) {
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
    // 回复写窗口已过：迟到回复对调用方已无交付意义，且把过期期限交给共享
    // stream 会在接纳后超时并把整条 link 退役。只发布本条终态，不产生写责任。
    if (write_deadline != RpcClock::time_point::max() && write_deadline <= RpcClock::now()) {
        return ErrV(ErrorCode::TimedOut, "reply deadline expired before write");
    }
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
    auto bytes = detail::RpcFrameProtocol::Encode(frame, session->owner->options.MaxFrameBytes());
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
    const auto wait_deadline = write_deadline == RpcClock::time_point::max()
                                   ? RpcClock::now() + 30s
                                   : std::min(write_deadline, RpcClock::now() + 30s);
    const auto status = settle->waiter->Wait({wait_deadline});
    if (status == bbt::coroutine::WaitStatus::Completed) {
        return settle->outcome;
    }
    if (status == bbt::coroutine::WaitStatus::Cancelled) {
        return ErrV(ErrorCode::Cancelled, "reply write wait cancelled");
    }
    if (status == bbt::coroutine::WaitStatus::TimedOut) {
        return ErrV(ErrorCode::TimedOut, "reply write not settled in budget");
    }
    return ErrV(ErrorCode::Unavailable, "reply write wait unavailable");
}

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

} // namespace bbt::infra::rpc
