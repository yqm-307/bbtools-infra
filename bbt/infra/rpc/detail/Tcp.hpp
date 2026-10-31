#pragma once
// RPC TCP adapter 模块内部共享助手（bbt/infra/rpc 实现 TU 专用，非公开头）：
// u32be 帧编解码、numeric-ip:port endpoint 解析、底层 error_code →
// Error 的共享结算（A5 NetToError 基础分类 + adapter 阶段事实）。
// TcpTransport.cc 是唯一消费者；不进入公开头面。

#include <cstddef>
#include <cstdint>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <bbt/infra/NetError.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Error.hpp>
#include <bbt/infra/net/WriteOperation.hpp>

namespace bbt::infra::rpc {
namespace detail {

inline constexpr std::size_t kTcpFramePrefixBytes = 4;

// --- u32be 帧前缀 ---------------------------------------------------------------

// 写路径长度边界（分配/拼接前）：payload==0 或超过 frame 上限拒绝。
// 上限本身在 Create 已校验 (0, kNetworkLimitsMaxBodyBytes]。
inline bool TcpFrameSizeValid(std::size_t payload_bytes,
                              std::size_t max_frame_bytes) noexcept {
    return payload_bytes > 0 && payload_bytes <= max_frame_bytes;
}

inline void TcpFrameWritePrefix(char out[kTcpFramePrefixBytes],
                                std::uint32_t payload_bytes) noexcept {
    out[0] = static_cast<char>((payload_bytes >> 24) & 0xff);
    out[1] = static_cast<char>((payload_bytes >> 16) & 0xff);
    out[2] = static_cast<char>((payload_bytes >> 8) & 0xff);
    out[3] = static_cast<char>(payload_bytes & 0xff);
}

inline std::uint32_t TcpFrameReadPrefix(const char in[kTcpFramePrefixBytes]) noexcept {
    return (static_cast<std::uint32_t>(static_cast<unsigned char>(in[0])) << 24) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(in[1])) << 16) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(in[2])) << 8) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(in[3])));
}

// --- endpoint：numeric-ip:port（IPv6 bracketed），无 DNS ---------------------------

// 只做 authority 局部边界：host 必须是数值 IP（Address::Ip 校验），端口
// 1..65535；拒绝 scheme/path/query/空白/主机名。返回 host 原文（IPv6 不
// 含括号），由调用方交给 Address::Ip。
inline result<void> ParseTcpEndpoint(const std::string& endpoint,
                                     std::string& out_host,
                                     std::uint16_t& out_port) {
    const auto invalid = result<void>::err(MakeError(
        ErrorCode::InvalidArgument,
        "rpc tcp: endpoint must be numeric-ip:port"));
    if (endpoint.empty() || endpoint.size() > 128) { return invalid; }
    std::string port;
    if (endpoint.front() == '[') {
        const auto close = endpoint.find(']');
        if (close == std::string::npos || close <= 1) { return invalid; }
        out_host = endpoint.substr(1, close - 1);
        if (close + 1 >= endpoint.size() || endpoint[close + 1] != ':') {
            return invalid;
        }
        port = endpoint.substr(close + 2);
    } else {
        const auto colon = endpoint.rfind(':');
        if (colon == std::string::npos) { return invalid; }
        // 未 bracket 的多冒号串不是合法 authority（裸 IPv6 必须 bracket）。
        if (endpoint.find(':') != colon) { return invalid; }
        out_host = endpoint.substr(0, colon);
        port = endpoint.substr(colon + 1);
    }
    if (out_host.empty() || port.empty() || port.size() > 5) { return invalid; }
    unsigned number = 0;
    for (const unsigned char c : port) {
        if (c < '0' || c > '9') { return invalid; }
        number = number * 10 + (c - '0');
        if (number > 65535) { return invalid; }
    }
    if (number == 0) { return invalid; }
    // host 必须是数值 IP：Address::Ip 是唯一权威（不 DNS、不截断端口）。
    std::error_code addr_error;
    const auto address = net::Address::Ip(out_host, 0, addr_error);
    if (addr_error || !address.Valid()) { return invalid; }
    out_port = static_cast<std::uint16_t>(number);
    return result<void>::ok();
}

// --- 错误结算 -----------------------------------------------------------------------

// committed = 请求帧字节已完整写出（RequestPhase::RequestCommitted）。
// 基础分类经 A5 公共桥 NetToError；committed 是 adapter 阶段事实，
// committed 后失联/超时/封口升级为 OutcomeUnknown（请求可能已生效），
// 协议违规（ProtocolError）与本地校验类保持原分类。
// 与 HTTP CallError 相同的不对称：协程级 Cancelled 在 committed 后仍保留
// Cancelled（调用方主动放弃是确定判决），但 request_phase 事实始终结算。
inline Error TcpCallError(std::error_code cause, const char* message,
                          bool committed) {
    auto error = NetToError(cause, std::string("rpc tcp: ") + message);
    if (committed) {
        error.request_phase = RequestPhase::RequestCommitted;
        if (error.code != ErrorCode::Cancelled) {
            error.code = ErrorCode::OutcomeUnknown;
        }
    }
    return error;
}

// 完整帧字节已到达但对帧边界/长度是确定判决：ProtocolError 携带阶段事实。
inline Error TcpProtocolError(const std::string& message, bool committed) {
    auto error = MakeError(ErrorCode::ProtocolError,
                           "rpc tcp: " + message);
    if (committed) {
        error.request_phase = RequestPhase::RequestCommitted;
    }
    return error;
}

// 写报告结算（ICoConnection::Write 三元组 → Error）：
//   - wait_error 非空 + Rejected/无 operation = 零提交，确定失败；
//   - InFlight（已接纳未终态）= 不能认定零发送 → OutcomeUnknown
//     （Cancelled 保留取消码，等待结束不撤销已接纳发送）；
//   - Done+error / wait_error = 写路径失败（未全量 committed）；
//   - Done 且无错 = 成功（committed 由调用方推进阶段）。
inline result<void> TcpSettleWrite(const net::WriteReport& report,
                                   const char* message) {
    const auto& outcome = report.outcome;
    if (!report.wait_error && outcome.phase == net::WritePhase::Done &&
        !outcome.error) {
        return result<void>::ok();
    }
    const auto cause =
        report.wait_error ? report.wait_error : outcome.error;
    auto error = TcpCallError(cause, message, /*committed=*/false);
    if (!error.request_phase.has_value()) {
        error.request_phase = RequestPhase::Writing;
    }
    // 已接纳仍在发送：等待结束不等于撤销；除调用方取消外不得当作零提交。
    if (report.operation.Valid() &&
        outcome.phase == net::WritePhase::InFlight &&
        error.code != ErrorCode::Cancelled) {
        error.code = ErrorCode::OutcomeUnknown;
    }
    return result<void>::err(std::move(error));
}

// 从任意分片的字节流缓冲中消费恰好 need 字节到 out；不够返回 false
// （pending 不动——调用方继续读）。帧体大小已在上层校验。
inline bool TcpTake(std::string& pending, std::size_t need,
                    std::vector<std::uint8_t>& out) {
    if (pending.size() < need) { return false; }
    out.assign(reinterpret_cast<const std::uint8_t*>(pending.data()),
               reinterpret_cast<const std::uint8_t*>(pending.data()) + need);
    pending.erase(0, need);
    return true;
}

} // namespace detail
} // namespace bbt::infra::rpc
