#include <bbt/infra/NetError.hpp>

namespace bbt::infra::detail {

ErrorCode NetBaseErrorCode(const std::error_code& ec) noexcept
{

    /**
     * 空 ec 调用属调用方 bug
     */
    if (!ec) {
        return ErrorCode::InternalError;
    }

    /**
     * bbt.net 类别：NetErrc 逐项映射；EOF 落到 TransportError 但语义由
     * 协议 owner 另行判定（桥不擅自 OutcomeUnknown）。
     */
    if (ec.category() == net::NetErrorCategory()) {
        switch (static_cast<net::NetErrc>(ec.value())) {
        case net::NetErrc::Closed:
            return ErrorCode::Closed;
        case net::NetErrc::Eof:
            return ErrorCode::TransportError;
        case net::NetErrc::WouldBlock:
            return ErrorCode::Overloaded;
        case net::NetErrc::Busy:
            return ErrorCode::Overloaded;
        case net::NetErrc::WriteSettling:
            return ErrorCode::Overloaded;
        case net::NetErrc::Rejected:
            return ErrorCode::Overloaded;
        }
        return ErrorCode::TransportError;
    }

    /**
     * 只有 generic/system 类别才按 errc 值统一映射（其 value 域是 errno）；
     * 其余未知类别的 value 域不承诺 errno 语义，不能按 errc 强转分类——
     * 外来 category（含等价于某 errc 的私有域）统一归 TransportError，
     * backend_category/backend_code 仍保留原事实供排障。
     */
    if (ec.category() != std::generic_category() && ec.category() != std::system_category()) {
        return ErrorCode::TransportError;
    }

    /**
     * 连接层常见系统错（EPIPE/ECONNRESET/ENETUNREACH…）一律归
     * TransportError，不逐一枚举。
     */
    switch (static_cast<std::errc>(ec.value())) {
    case std::errc::timed_out:
        return ErrorCode::TimedOut;
    case std::errc::operation_canceled:
        return ErrorCode::Cancelled;
    case std::errc::operation_not_permitted:
        return ErrorCode::InvalidContext;
    case std::errc::invalid_argument:
        return ErrorCode::InvalidArgument;
    case std::errc::operation_not_supported:
        return ErrorCode::RuntimeUnavailable;
    case std::errc::no_buffer_space:
    case std::errc::device_or_resource_busy:
        return ErrorCode::Overloaded;
    default:
        break;
    }
    return ErrorCode::TransportError;
}

const char* NetDomainCode(const std::error_code& ec) noexcept
{
    if (ec.category() != net::NetErrorCategory()) {
        return nullptr;
    }
    switch (static_cast<net::NetErrc>(ec.value())) {
    case net::NetErrc::Closed:
        return "net.closed";
    case net::NetErrc::Eof:
        return "net.eof";
    case net::NetErrc::WouldBlock:
        return "net.would_block";
    case net::NetErrc::Busy:
        return "net.busy";
    case net::NetErrc::WriteSettling:
        return "net.write_settling";
    case net::NetErrc::Rejected:
        return "net.rejected";
    }
    return "net.unknown";
}

// clang-format off
}
// clang-format on

namespace bbt::infra {

Error NetToError(const std::error_code& ec, std::string message)
{
    Error error = MakeError(detail::NetBaseErrorCode(ec), std::move(message));
    error.backend_category = ec.category().name();
    error.backend_code = ec.value();
    if (const char* code = detail::NetDomainCode(ec)) {
        error.domain_code = code;
    }
    return error;
}

// clang-format off
}
// clang-format on
