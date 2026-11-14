#include <bbt/infra/NetworkTypes.hpp>

namespace bbt::infra {

result<void> ValidateNetworkLimits(const NetworkLimits& limits)
{
    if (limits.max_connections == 0 || limits.max_connections > kNetworkLimitsMaxConnections)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "max_connections out of range"));
    if (limits.max_inflight == 0 || limits.max_inflight > kNetworkLimitsMaxInflight)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "max_inflight out of range"));
    if (limits.max_header_bytes == 0 || limits.max_header_bytes > kNetworkLimitsMaxHeaderBytes)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "max_header_bytes out of range"));
    if (limits.max_body_bytes == 0 || limits.max_body_bytes > kNetworkLimitsMaxBodyBytes)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "max_body_bytes out of range"));
    if (limits.incoming_timeout <= std::chrono::milliseconds{0}
        || limits.incoming_timeout > kNetworkLimitsMaxIncomingTimeout)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "incoming_timeout out of range"));
    return result<void>::ok();
}

// clang-format off
}
// clang-format on
