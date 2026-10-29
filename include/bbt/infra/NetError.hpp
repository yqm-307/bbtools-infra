#pragma once
// co-network/v1 A5 错误桥：net 的 std::error_code/NetErrc 到协议层
// result<T>/Error 的**唯一权威基础分类**接缝。
// 契约来源：docs/decisions/2026-10-29-icoconn-interface-final-review.md A5。
//
// 使用边界：
//   - 只由协议/适配层（HTTP/Redis/未来的 adapter）include；net 原语头不
//     include Result/Error，common 也不反向链接 net——本头正是两者的接缝。
//   - NetToError 输出的是**基础分类**：Closed/TimedOut/Cancelled/Overloaded/
//     InvalidContext/InvalidArgument/RuntimeUnavailable/TransportError 由 ec
//     决定，已接纳/完整写出/可信回复的阶段结算仍归协议 owner
//     （HTTP 的 RequestPhase/OutcomeUnknown、Redis 的 Fail(accepted) 封口）。
//   - EOF 不在桥中升级：NetErrc::Eof 只是「流定界」信号，交协议 parser
//     判断是合法定界还是截断（HTTP beast put_eof、Redis 无回复即失败）。
//   - Busy/WouldBlock/WriteSettling 是 try/等待条件，出现在返回错误里只能
//     当名额竞争处理（Overloaded），不能冒充最终传输失败。
//
// backend 事实：backend_category 恒存 ec.category().name() 的真实值
// （generic/system/bbt.net），backend_code 存 ec.value()；domain_code 只在
// ec 来自 bbt.net 类别时填 net.* 语义名，其他类别不动 domain_code。

#include <string>
#include <system_error>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Error.hpp>

namespace bbt::infra {

namespace detail {

// 表驱动基础分类：同一底层 cause 在所有 adapter 得到同一基础 code。
// 只覆盖「底层分类」，不掺入协议阶段判断。
inline ErrorCode NetBaseErrorCode(const std::error_code& ec) noexcept {
    if (!ec) { return ErrorCode::InternalError; }  // 空 ec 调用属调用方 bug
    // bbt.net 类别：NetErrc 逐项映射；EOF 落到 TransportError 但语义由
    // 协议 owner 另行判定（桥不擅自 OutcomeUnknown）。
    if (ec.category() == net::NetErrorCategory()) {
        switch (static_cast<net::NetErrc>(ec.value())) {
            case net::NetErrc::Closed:        return ErrorCode::Closed;
            case net::NetErrc::Eof:           return ErrorCode::TransportError;
            case net::NetErrc::WouldBlock:    return ErrorCode::Overloaded;
            case net::NetErrc::Busy:          return ErrorCode::Overloaded;
            case net::NetErrc::WriteSettling: return ErrorCode::Overloaded;
            case net::NetErrc::Rejected:      return ErrorCode::Overloaded;
        }
        return ErrorCode::TransportError;
    }
    // 只有 generic/system 类别才按 errc 值统一映射（其 value 域是 errno）；
    // 其余未知类别的 value 域不承诺 errno 语义，不能按 errc 强转分类——
    // 外来 category（含等价于某 errc 的私有域）统一归 TransportError，
    // backend_category/backend_code 仍保留原事实供排障。
    if (ec.category() != std::generic_category() &&
        ec.category() != std::system_category()) {
        return ErrorCode::TransportError;
    }
    // 连接层常见系统错（EPIPE/ECONNRESET/ENETUNREACH…）一律归
    // TransportError，不逐一枚举。
    switch (static_cast<std::errc>(ec.value())) {
        case std::errc::timed_out:                return ErrorCode::TimedOut;
        case std::errc::operation_canceled:       return ErrorCode::Cancelled;
        case std::errc::operation_not_permitted:  return ErrorCode::InvalidContext;
        case std::errc::invalid_argument:         return ErrorCode::InvalidArgument;
        case std::errc::operation_not_supported:  return ErrorCode::RuntimeUnavailable;
        case std::errc::no_buffer_space:
        case std::errc::device_or_resource_busy:  return ErrorCode::Overloaded;
        default:                                  break;
    }
    return ErrorCode::TransportError;
}

// bbt.net 类别的稳定 domain_code 语义名；与 NetErrc 枚举一一对应，
// 只表达「net 域内发生了什么」，不携带协议阶段含义。
inline const char* NetDomainCode(const std::error_code& ec) noexcept {
    if (ec.category() != net::NetErrorCategory()) { return nullptr; }
    switch (static_cast<net::NetErrc>(ec.value())) {
        case net::NetErrc::Closed:        return "net.closed";
        case net::NetErrc::Eof:           return "net.eof";
        case net::NetErrc::WouldBlock:    return "net.would_block";
        case net::NetErrc::Busy:          return "net.busy";
        case net::NetErrc::WriteSettling: return "net.write_settling";
        case net::NetErrc::Rejected:      return "net.rejected";
    }
    return "net.unknown";
}

} // namespace detail

// 唯一公共入口：把 net 返回的 error_code 折成携带后端事实的基础 Error。
// message 由调用方给（描述自己这一层在做什么）；阶段结算、重试、退役
// 全在调用方，桥本身不再造第二套 errno 语义。
inline Error NetToError(const std::error_code& ec, std::string message) {
    Error error = MakeError(detail::NetBaseErrorCode(ec), std::move(message));
    error.backend_category = ec.category().name();
    error.backend_code     = ec.value();
    if (const char* code = detail::NetDomainCode(ec)) {
        error.domain_code = code;
    }
    return error;
}

} // namespace bbt::infra
