#pragma once
// co-network/v1 N0：调用选项、装配值类型与协议消息形态。
// 签名冻结见 docs/decisions/0002-co-network-contract-v1.md。

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

#include <bbt/infra/ICoObject.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra {

struct CallOptions {
    bbt::coroutine::Deadline          deadline;
    bbt::coroutine::CancellationToken cancel;
};

// 各项必须显式、大于零且不超过下列上限；incoming_timeout 必须有限。
inline constexpr std::size_t kNetworkLimitsMaxConnections = 1000000;
inline constexpr std::size_t kNetworkLimitsMaxInflight    = 1000000;
inline constexpr std::size_t kNetworkLimitsMaxHeaderBytes = 16 * 1024 * 1024;
inline constexpr std::size_t kNetworkLimitsMaxBodyBytes   = 1024 * 1024 * 1024;
inline constexpr std::chrono::milliseconds kNetworkLimitsMaxIncomingTimeout{
    std::chrono::hours{24}};

struct NetworkLimits {
    std::size_t               max_connections;
    std::size_t               max_inflight;
    std::size_t               max_header_bytes;
    std::size_t               max_body_bytes;
    std::chrono::milliseconds incoming_timeout;
};

struct ListenAddress {
    std::string   host;
    std::uint16_t port;
};

struct IncomingCallContext {
    bbt::coroutine::Deadline          deadline;
    bbt::coroutine::CancellationToken cancel;
    std::string                       peer_principal;
};

struct HttpRequest {
    std::string method;
    std::string url;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;
};

struct HttpResponse {
    unsigned status;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;
};

struct RpcAddress {
    std::string transport;
    std::string endpoint;
};

struct RpcEnvelope {
    std::string service;
    std::string method;
    std::string request_id;
    std::string request_schema;
    std::string response_schema;
    std::vector<std::uint8_t> payload;
    std::vector<std::pair<std::string, std::string>> metadata;
};

using HttpHandler = std::function<result<HttpResponse>(
    IncomingCallContext, HttpRequest)>;
using RpcHandler = std::function<result<RpcEnvelope>(
    IncomingCallContext, RpcEnvelope)>;

// NetworkLimits 装配校验：各项 >0 且不超上限，违规返回 InvalidArgument。
inline result<void> ValidateNetworkLimits(const NetworkLimits& limits) {
    if (limits.max_connections == 0 ||
        limits.max_connections > kNetworkLimitsMaxConnections)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "max_connections out of range"));
    if (limits.max_inflight == 0 ||
        limits.max_inflight > kNetworkLimitsMaxInflight)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "max_inflight out of range"));
    if (limits.max_header_bytes == 0 ||
        limits.max_header_bytes > kNetworkLimitsMaxHeaderBytes)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "max_header_bytes out of range"));
    if (limits.max_body_bytes == 0 ||
        limits.max_body_bytes > kNetworkLimitsMaxBodyBytes)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "max_body_bytes out of range"));
    if (limits.incoming_timeout <= std::chrono::milliseconds{0} ||
        limits.incoming_timeout > kNetworkLimitsMaxIncomingTimeout)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "incoming_timeout out of range"));
    return result<void>::ok();
}

} // namespace bbt::infra
