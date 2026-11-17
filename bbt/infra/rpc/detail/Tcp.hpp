#pragma once

/**
 * 双向 RPC 会话内部 TCP 助手，不进入公开头面。
 * 数值地址解析与写终态错误结算由会话握手、writer 和调用路径共用。
 */
#include <cstdint>
#include <string>
#include <system_error>
#include <utility>

#include <bbt/infra/NetError.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Error.hpp>
#include <tuple>

namespace bbt::infra::rpc {
namespace detail {

/**
 * @name endpoint：numeric-ip:port（IPv6 bracketed），无 DNS @{
 */

/**
 * 只做 authority 局部边界：host 必须是数值 IP（Address::Ip 校验），端口
 * 1..65535；拒绝 scheme/path/query/空白/主机名。返回 host 原文（IPv6 不
 * 含括号），由调用方交给 Address::Ip。
 */
result<void> ParseTcpEndpoint(const std::string& endpoint, std::string& out_host, std::uint16_t& out_port);

/**
 * @}
 * @name 错误结算 @{
 */

/**
 * @brief committed = 请求帧字节已完整写出（RequestPhase::RequestCommitted）。
 * 基础分类经 NetToError 映射；committed 是 adapter 阶段事实，
 * committed 后失联/超时/封口升级为 OutcomeUnknown（请求可能已生效），
 * 协议违规（ProtocolError）与本地校验类保持原分类。
 * 与 HTTP CallError 相同的不对称：协程级 Cancelled 在 committed 后仍保留
 * Cancelled（调用方主动放弃是确定判决），但 request_phase 事实始终结算。
 */
Error TcpCallError(std::error_code cause, const char* message, bool committed);

/**
 * @brief 按已发前缀结算写结果。零字节未提交，部分发送为未知结果，全部发送成功。
 * 调用方取消保留原取消码；连接作废仍由会话负责。
 */
result<void>
TcpSettleWrite(const std::tuple<std::size_t, std::error_code>& report, std::size_t total, const char* message);

// clang-format off
}
// clang-format on

/**
 * @}
 */
// clang-format off
}
// clang-format on
