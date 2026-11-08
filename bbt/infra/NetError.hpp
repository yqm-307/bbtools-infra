/** @file NetError.hpp
 * @brief net 层错误到协议层 Error 的桥接：把 std::error_code 折成携带后端事实的基础分类。
 *
 * 本头是 result<T>/Error 与 net 原语之间的接缝：只有协议或适配层（HTTP、Redis 及后续适配器）包含它；
 * net 原语头不包含 Result/Error。
 *
 * 边界：
 *   - NetToError 只输出**基础分类**：Closed、TimedOut、Cancelled、Overloaded、InvalidContext、
 *     InvalidArgument、RuntimeUnavailable、TransportError 由 ec 决定。已接纳、完整写出、可信回复
 *     这些阶段结算仍归协议 owner，按各自的阶段规则结算（如 RequestPhase）。
 *   - Eof 映射为 TransportError，不升级为 OutcomeUnknown；是合法定界还是截断由协议 owner 判定。
 *   - Busy、WouldBlock、WriteSettling、Rejected 映射为 Overloaded，表示名额或背压竞争
 *     不等同于最终传输失败。
 *
 * 后端事实：backend_category 恒存 ec.category().name() 的真实值（generic、system、bbt.net），
 * backend_code 存 ec.value()；只有 ec 来自 bbt.net 类别时才把 net.* 语义名填入 domain_code，
 * 其他类别不动 domain_code。
 */
#pragma once

#include <string>
#include <system_error>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Error.hpp>

namespace bbt::infra {

namespace detail {

/// @brief 表驱动基础分类：同一底层 cause 在所有 adapter 得到同一基础 code。
/// 只覆盖「底层分类」，不掺入协议阶段判断。
ErrorCode NetBaseErrorCode(const std::error_code& ec) noexcept;

/// @brief bbt.net 类别的稳定 domain_code 语义名；与 NetErrc 枚举一一对应，
/// 只表达「net 域内发生了什么」，不携带协议阶段含义。
const char* NetDomainCode(const std::error_code& ec) noexcept;

} // namespace detail

/**
 * @brief 把 net 返回的 error_code 折成携带后端事实的基础 Error，是本桥唯一的公共入口。
 * @param ec net 层返回的错误码（generic、system 或 bbt.net 类别）。
 * @param message 描述调用方这一层正在做什么。阶段结算、重试、退役都由调用方决定，本函数不做。
 * @return 基础分类的 Error，backend_category 与 backend_code 保留后端事实。
 */
Error NetToError(const std::error_code& ec, std::string message);

} // namespace bbt::infra
