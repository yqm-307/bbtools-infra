#include <bbt/infra/rpc/detail/Tcp.hpp>

namespace bbt::infra::rpc {
namespace detail {

result<void> ParseTcpEndpoint(const std::string& endpoint, std::string& out_host, std::uint16_t& out_port)
{
    const auto invalid =
        result<void>::err(MakeError(ErrorCode::InvalidArgument, "rpc tcp: endpoint must be numeric-ip:port"));
    if (endpoint.empty() || endpoint.size() > 128) {
        return invalid;
    }
    std::string port;
    if (endpoint.front() == '[') {
        const auto close = endpoint.find(']');
        if (close == std::string::npos || close <= 1) {
            return invalid;
        }
        out_host = endpoint.substr(1, close - 1);
        if (close + 1 >= endpoint.size() || endpoint[close + 1] != ':') {
            return invalid;
        }
        port = endpoint.substr(close + 2);
    } else {
        const auto colon = endpoint.rfind(':');
        if (colon == std::string::npos) {
            return invalid;
        }

        /**
         * 未 bracket 的多冒号串不是合法 authority（裸 IPv6 必须 bracket）。
         */
        if (endpoint.find(':') != colon) {
            return invalid;
        }
        out_host = endpoint.substr(0, colon);
        port = endpoint.substr(colon + 1);
    }
    if (out_host.empty() || port.empty() || port.size() > 5) {
        return invalid;
    }
    unsigned number = 0;
    for (const unsigned char c : port) {
        if (c < '0' || c > '9') {
            return invalid;
        }
        number = number * 10 + (c - '0');
        if (number > 65535) {
            return invalid;
        }
    }
    if (number == 0) {
        return invalid;
    }

    /**
     * host 必须是数值 IP：Address::Ip 是唯一权威（不 DNS、不截断端口）。
     */
    std::error_code addr_error;
    const auto address = net::Address::Ip(out_host, 0, addr_error);
    if (addr_error || !address.Valid()) {
        return invalid;
    }
    out_port = static_cast<std::uint16_t>(number);
    return result<void>::ok();
}

Error TcpCallError(std::error_code cause, const char* message, bool committed)
{
    auto error = NetToError(cause, std::string("rpc tcp: ") + message);
    if (committed) {
        error.request_phase = RequestPhase::RequestCommitted;
        if (error.code != ErrorCode::Cancelled) {
            error.code = ErrorCode::OutcomeUnknown;
        }
    }
    return error;
}

result<void> TcpSettleWrite(
    const std::tuple<std::size_t, std::error_code>& report,
    std::size_t total,
    const char* message)
{
    const auto [sent, cause] = report;
    if (!cause && sent == total)
        return result<void>::ok();
    auto error = TcpCallError(cause, message, sent == total);
    if (!error.request_phase.has_value())
        error.request_phase = RequestPhase::Writing;
    if (sent > 0 && sent < total && error.code != ErrorCode::Cancelled) {
        error.code = ErrorCode::OutcomeUnknown;
    }
    return result<void>::err(std::move(error));
}

// clang-format off
}
// clang-format on
// clang-format off
}
// clang-format on
