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
inline bool TcpFrameSizeValid(std::size_t payload_bytes, std::size_t max_frame_bytes) noexcept
{
    return payload_bytes > 0 && payload_bytes <= max_frame_bytes;
}

void TcpFrameWritePrefix(char out[kTcpFramePrefixBytes], std::uint32_t payload_bytes) noexcept;

inline std::uint32_t TcpFrameReadPrefix(const char in[kTcpFramePrefixBytes]) noexcept
{
    return (static_cast<std::uint32_t>(static_cast<unsigned char>(in[0])) << 24)
           | (static_cast<std::uint32_t>(static_cast<unsigned char>(in[1])) << 16)
           | (static_cast<std::uint32_t>(static_cast<unsigned char>(in[2])) << 8)
           | (static_cast<std::uint32_t>(static_cast<unsigned char>(in[3])));
}

// --- endpoint：numeric-ip:port（IPv6 bracketed），无 DNS ---------------------------

// 只做 authority 局部边界：host 必须是数值 IP（Address::Ip 校验），端口
// 1..65535；拒绝 scheme/path/query/空白/主机名。返回 host 原文（IPv6 不
// 含括号），由调用方交给 Address::Ip。
result<void> ParseTcpEndpoint(const std::string& endpoint, std::string& out_host, std::uint16_t& out_port);

// --- 错误结算 -----------------------------------------------------------------------

// committed = 请求帧字节已完整写出（RequestPhase::RequestCommitted）。
// 基础分类经 A5 公共桥 NetToError；committed 是 adapter 阶段事实，
// committed 后失联/超时/封口升级为 OutcomeUnknown（请求可能已生效），
// 协议违规（ProtocolError）与本地校验类保持原分类。
// 与 HTTP CallError 相同的不对称：协程级 Cancelled 在 committed 后仍保留
// Cancelled（调用方主动放弃是确定判决），但 request_phase 事实始终结算。
Error TcpCallError(std::error_code cause, const char* message, bool committed);

// 完整帧字节已到达但对帧边界/长度是确定判决：ProtocolError 携带阶段事实。
Error TcpProtocolError(const std::string& message, bool committed);

// 写报告结算（ICoConnection::Write 三元组 → Error）：
//   - wait_error 非空 + Rejected/无 operation = 零提交，确定失败；
//   - InFlight（已接纳未终态）= 不能认定零发送 → OutcomeUnknown
//     （Cancelled 保留取消码，等待结束不撤销已接纳发送）；
//   - Done+error / wait_error = 写路径失败（未全量 committed）；
//   - Done 且无错 = 成功（committed 由调用方推进阶段）。
result<void> TcpSettleWrite(const net::WriteReport& report, const char* message);

// 从任意分片的字节流缓冲中消费恰好 need 字节到 out；不够返回 false
// （pending 不动——调用方继续读）。帧体大小已在上层校验。
bool TcpTake(std::string& pending, std::size_t need, std::vector<std::uint8_t>& out);

} // namespace detail
} // namespace bbt::infra::rpc
