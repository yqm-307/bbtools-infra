/**
 * 会话层状态的锁内谓词与计费实现：谓词仍按声明要求持 owner->mu / server->mu
 * 评估，锁序与 waiters 语义不变——本文件只收纳 RpcSessionState.hpp 的
 * 非平凡成员体。
 */
#include <bbt/infra/rpc/detail/RpcSessionState.hpp>

#include <algorithm>

namespace bbt::infra::rpc {

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

Error WaitFailure(bbt::coroutine::WaitStatus status, const char* message)
{
    using bbt::coroutine::WaitStatus;
    switch (status) {
    case WaitStatus::Cancelled:
        return Err(ErrorCode::Cancelled, message);
    case WaitStatus::TimedOut:
        return Err(ErrorCode::TimedOut, message);
    case WaitStatus::InvalidContext:
        return Err(ErrorCode::InvalidContext, message);
    case WaitStatus::RuntimeUnavailable:
        return Err(ErrorCode::RuntimeUnavailable, message);
    case WaitStatus::AlreadyWaiting:
    case WaitStatus::Completed:
        return Err(ErrorCode::InternalError, message);
    }
    return Err(ErrorCode::InternalError, message);
}

Error SettleCloseReason(Error reason, const char* default_message)
{
    if (reason.message.empty()) {
        return MakeError(ErrorCode::Closed, default_message);
    }
    return reason;
}

std::error_code CheckCall(bbt::coroutine::Deadline deadline)
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

result<std::uint32_t> Budget(bbt::coroutine::Deadline deadline)
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

void AttachCloseReason(Error& error, const Error& reason)
{
    error.details.emplace_back("close_reason", reason.message);
    error.details.emplace_back("close_reason_code", std::to_string(static_cast<int>(reason.code)));
    if (!reason.domain_code.empty()) {
        error.details.emplace_back("close_reason_domain_code", reason.domain_code);
    }
}

bool LinkLease::Released() const
{
    const auto live = session.lock();
    return (!live || live->responsibilities.load() == 0) && conn.Released();
}

bool SessionOwnerState::DrainedLocked() const
{
    return active_ops == 0;
}

bool SessionOwnerState::TeardownLocked() const
{
    return link_leases.empty() && establishing.empty();
}

void SessionOwnerState::ReapReleasedLocked()
{
    link_leases.erase(
        std::remove_if(link_leases.begin(), link_leases.end(), [](const auto& lease) { return lease.Released(); }),
        link_leases.end());
    for (auto it = by_identity.begin(); it != by_identity.end();) {
        auto session = it->second.lock();
        if (!session || (session->responsibilities.load() == 0 && session->channel->IsReleased())) {
            it = by_identity.erase(it);
        } else {
            ++it;
        }
    }
    for (auto it = by_endpoint.begin(); it != by_endpoint.end();) {
        auto session = it->second.lock();
        if (!session || (session->responsibilities.load() == 0 && session->channel->IsReleased())) {
            it = by_endpoint.erase(it);
        } else {
            ++it;
        }
    }
    for (auto it = establishing.begin(); it != establishing.end();) {
        if (it->second->done && it->second->reclaim.Released()) {
            it = establishing.erase(it);
        } else {
            ++it;
        }
    }
}

std::size_t SessionOwnerState::OccupiedLocked() const
{
    return link_leases.size() + std::count_if(establishing.begin(), establishing.end(), [](const auto& entry) {
               return entry.second->reserved;
           });
}

void ServerState::ReapReleasedLocked()
{
    links.erase(
        std::remove_if(links.begin(), links.end(), [](const auto& lease) { return lease.Released(); }), links.end());
    for (auto it = handshakes.begin(); it != handshakes.end();) {
        if (it->second.finished && it->second.conn.Released()) {
            it = handshakes.erase(it);
        } else {
            ++it;
        }
    }
    if (rejected_accept.Released()) {
        rejected_accept = {};
    }
}

bool ServerState::DrainedLocked() const
{
    for (std::size_t i = 0; i != kLaneCount; ++i) {
        if (inflight[i] != 0) {
            return false;
        }
    }
    if (open_replies != 0) {
        return false;
    }
    if (queue && queue->Queued() != 0) {
        return false;
    }

    /**
     * Drain 含队列/写责任：任何已登记 link 的 writes_outstanding 未归零
     * 都不算收口。dead 链接同样可能残留未结算写责任（writer 手中的
     * 在写项不属于排队丢弃口径）——不能用 !dead 跳过。
     */
    for (const auto& lease : links) {
        if (auto session = lease.session.lock(); session && session->writes_outstanding.load() != 0) {
            return false;
        }
    }
    return true;
}

bool ServerState::StoppedLocked() const
{
    if (starting) {
        return false;
    }
    if (started && !engine->AcceptingStopped()) {
        return false;
    }
    if (started && !accept_loop_exited) {
        return false;
    }
    if (!handshakes.empty() || !links.empty() || rejected_accept.Valid()) {
        return false;
    }
    return DrainedLocked();
}

void ServerState::NoteInflight(std::size_t lane, long delta)
{
    auto& value = inflight[lane];
    value = static_cast<std::uint64_t>(static_cast<long>(value) + delta);
    if (close_signal) {
        close_signal->Notify();
    }
}

namespace detail {

std::size_t AdmissionQueueImpl::ItemBytes(const AdmissionItem& item)
{
    const auto& r = item.request;
    std::size_t bytes = 64 + r.Payload().size() + r.Service().size() + r.Method().size() + r.RequestId().size()
                        + r.MessageId().size() + r.RequestSchema().size() + r.ResponseSchema().size()
                        + r.OrderingKey().size();
    for (const auto& kv : r.Metadata()) {
        bytes += kv.first.size() + kv.second.size();
    }
    return bytes;
}

// clang-format off
}
// clang-format on
// clang-format off
}
// clang-format on
