#include <bbt/infra/rpc/detail/Tcp.hpp>

namespace bbt::infra::rpc {
namespace detail {

void TcpFrameWritePrefix(char out[kTcpFramePrefixBytes], std::uint32_t payload_bytes) noexcept
{
    out[0] = static_cast<char>((payload_bytes >> 24) & 0xff);
    out[1] = static_cast<char>((payload_bytes >> 16) & 0xff);
    out[2] = static_cast<char>((payload_bytes >> 8) & 0xff);
    out[3] = static_cast<char>(payload_bytes & 0xff);
}

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
        // 未 bracket 的多冒号串不是合法 authority（裸 IPv6 必须 bracket）。
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
    // host 必须是数值 IP：Address::Ip 是唯一权威（不 DNS、不截断端口）。
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

Error TcpProtocolError(const std::string& message, bool committed)
{
    auto error = MakeError(ErrorCode::ProtocolError, "rpc tcp: " + message);
    if (committed) {
        error.request_phase = RequestPhase::RequestCommitted;
    }
    return error;
}

result<void> TcpSettleWrite(const net::WriteReport& report, const char* message)
{
    const auto& outcome = report.outcome;
    if (!report.wait_error && outcome.phase == net::WritePhase::Done && !outcome.error) {
        return result<void>::ok();
    }
    const auto cause = report.wait_error ? report.wait_error : outcome.error;
    auto error = TcpCallError(cause, message, /*committed=*/false);
    if (!error.request_phase.has_value()) {
        error.request_phase = RequestPhase::Writing;
    }
    // 已接纳仍在发送：等待结束不等于撤销；除调用方取消外不得当作零提交。
    if (report.operation.Valid() && outcome.phase == net::WritePhase::InFlight && error.code != ErrorCode::Cancelled) {
        error.code = ErrorCode::OutcomeUnknown;
    }
    return result<void>::err(std::move(error));
}

bool TcpTake(std::string& pending, std::size_t need, std::vector<std::uint8_t>& out)
{
    if (pending.size() < need) {
        return false;
    }
    out.assign(reinterpret_cast<const std::uint8_t*>(pending.data()),
               reinterpret_cast<const std::uint8_t*>(pending.data()) + need);
    pending.erase(0, need);
    return true;
}

} // namespace detail
} // namespace bbt::infra::rpc
