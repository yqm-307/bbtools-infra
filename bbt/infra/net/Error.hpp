#pragma once

/**
 * @file Error.hpp
 * @brief net 自有错误码。
 *
 * 不借用第三方或系统的错误类别。Eof、WouldBlock、Busy、WriteSettling、Rejected 与 Closed 各自独立成码，
 * 不伪装成 errno：若把 Busy 或 Eof 映射成某个系统错误，调用方就分不清「对端已结束」「暂时无数据」
 * 「与其它路径竞争」与「操作未提交」。
 */

#include <string>
#include <system_error>

namespace bbt::infra::net {

/**
 * @brief net 自有错误码。
 *
 * 通过 make_error_code 转为 std::error_code，类别名为 "bbt.net"。
 */
enum class NetErrc {
    Eof = 1,       ///< 读方向已到 EOF：对端已关闭且没有剩余数据
    WouldBlock,    ///< 暂无数据，且尚未到达终态，可稍后重试
    Busy,          ///< 环被其它路径占用（try-lock 失败），本次零消费，可重试
    WriteSettling, ///< 写方向正在关闭或失败结算，新写被拒绝
    Rejected,      ///< 操作未提交，没有任何副作用
    Closed,        ///< 连接或 Engine 已进入关闭终态
};

/**
 * @brief 返回 net 错误类别的单例。
 *
 * 未知枚举值的 message 为 "unknown net error"，不抛异常。
 */
const std::error_category& NetErrorCategory();

/** @brief 把 NetErrc 转为 std::error_code，供 ADL 与 std::error_code 构造使用。 */
std::error_code make_error_code(NetErrc value) noexcept;

} // namespace bbt::infra::net

namespace std {
// 让 NetErrc 可以直接参与 std::error_code 的构造与比较。
template <> struct is_error_code_enum<bbt::infra::net::NetErrc> : true_type {};
} // namespace std
