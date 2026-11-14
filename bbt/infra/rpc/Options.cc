#include <bbt/infra/rpc/Options.hpp>

namespace bbt::infra::rpc {

result<void> ClientOptions::Validate() const
{
    if (m_local_identity.empty() || m_local_identity.size() > 256 || !infra::detail::IsValidUtf8(m_local_identity))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "local_identity empty/too long/not UTF-8"));
    if (m_max_sessions == 0 || m_max_sessions > kMaxMaxSessions)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "max_sessions out of range"));
    if (m_max_frame_bytes < kMinFrameBytes || m_max_frame_bytes > kMaxFrameBytes)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "max_frame_bytes out of range"));
    if (m_system_queue_bytes == 0 || m_system_queue_bytes > kMaxQueueBytes || m_service_queue_bytes == 0
        || m_service_queue_bytes > kMaxQueueBytes || m_actor_queue_bytes == 0 || m_actor_queue_bytes > kMaxQueueBytes)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "queue bytes out of range"));
    if (m_service_weight == 0 || m_actor_weight == 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "lane weight must be non-zero"));
    return result<void>::ok();
}

result<void> SendOptions::Validate() const
{
    if (m_deadline == bbt::coroutine::Deadline::max() || m_deadline <= std::chrono::steady_clock::now())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "deadline must be finite and in the future"));
    return result<void>::ok();
}

result<void> ServerOptions::Validate() const
{
    if (!m_listen_address.Valid())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "listen_address invalid"));
    if (m_max_frame_bytes < kMinFrameBytes || m_max_frame_bytes > kMaxFrameBytes)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "max_frame_bytes out of range"));
    if (m_max_connections == 0 || m_max_connections > kNetworkLimitsMaxConnections)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "max_connections out of range"));
    if (m_max_inflight == 0 || m_max_inflight > kNetworkLimitsMaxInflight)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "max_inflight out of range"));
    if (m_max_handler_bytes == 0 || m_max_handler_bytes > kNetworkLimitsMaxBodyBytes)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "max_handler_bytes out of range"));
    if (m_incoming_timeout <= std::chrono::milliseconds{0} || m_incoming_timeout > kNetworkLimitsMaxIncomingTimeout)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "incoming_timeout out of range"));
    return result<void>::ok();
}

// clang-format off
}
// clang-format on
