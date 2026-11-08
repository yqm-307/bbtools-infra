#pragma once
// 双向 RPC 会话内部 TCP 助手，不进入公开头面。
// 数值地址解析与写终态错误结算由会话握手、writer 和调用路径共用。

#include <cstdint>
#include <string>
#include <system_error>
#include <utility>

#include <bbt/infra/NetError.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Error.hpp>
#include <bbt/infra/net/WriteOperation.hpp>

namespace bbt::infra::rpc {
namespace detail {

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

// 写报告结算（ICoConnection::Write 三元组 → Error）：
//   - wait_error 非空 + Rejected/无 operation = 零提交，确定失败；
//   - InFlight（已接纳未终态）= 不能认定零发送 → OutcomeUnknown
//     （Cancelled 保留取消码，等待结束不撤销已接纳发送）；
//   - Done+error / wait_error = 写路径失败（未全量 committed）；
//   - Done 且无错 = 成功（committed 由调用方推进阶段）。
result<void> TcpSettleWrite(const net::WriteReport& report, const char* message);

} // namespace detail
} // namespace bbt::infra::rpc
