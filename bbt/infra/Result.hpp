/** @file Result.hpp
 * @brief 统一结果类型 result<T> / result<void>、错误分类 ErrorCode 与错误体 Error。
 *
 * 公共面用返回值表达失败，不以异常传递业务错误：调用方用 explicit operator bool 判定成功，
 * 失败详情通过 error() 取出。契约来源：docs/decisions/0002-co-network-contract.md。
 *
 * 不复用 core 的 bbt::core::util::Result<T,E>：core 在误用时抛出 std::bad_variant_access，
 * 本契约要求 value()/error() 误用时抛出 std::logic_error，访问语义不同，因此本头独立实现。
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace bbt::infra {

/**
 * @brief 错误分类码，调用方据此分支；domain_code 与 message 只用于排障和展示。
 *
 * Overloaded 表示名额或背压竞争，不等于传输失败；OutcomeUnknown 表示请求可能已经生效、
 * 但回复未知，判定方式见 RequestPhase。
 */
enum class ErrorCode {
    InvalidArgument,
    InvalidContext,
    RuntimeUnavailable,
    Closed,
    Cancelled,
    TimedOut,
    Overloaded,
    Unavailable,
    TransportError,
    ProtocolError,
    NotFound,
    TypeMismatch,
    UnsupportedRoute,
    OutcomeUnknown,
    RemoteError,
    InternalError
};

/// @brief Error.details 的条目上限；超出由 ValidateErrorDetails 整体拒绝，不截断。
inline constexpr std::size_t kMaxErrorDetails = 16;
/// @brief 单个详情键的字节上限（UTF-8）。
inline constexpr std::size_t kMaxKeyBytes = 64;
/// @brief 单个详情值的字节上限（UTF-8）。
inline constexpr std::size_t kMaxValueBytes = 256;

/// @brief 结构化详情：有序键值对；同一详情内键不得重复，由 ValidateErrorDetails 校验。
using ErrorDetails = std::vector<std::pair<std::string, std::string>>;

/// @brief infra 自身错误的默认 domain。上层 domain 及其保留键的语义由上层边界拥有，infra 不在本头硬编码任何上层字段。
inline constexpr std::string_view kErrorDomainInfra = "infra";

/**
 * @brief 出站请求阶段：由 operation owner（HTTP/RPC adapter 的 op 状态）维护，并随终态错误对外发布。
 *
 * 消费者据此判定「请求字节是否已完整写出」，不解析 message、errno、日志顺序，也不靠固定等待猜测。
 * 逻辑终态只线性化一次，见 docs/decisions/0008-request-phase-outcome-unknown.md。
 *
 * 语义分界：
 *   - NotStarted / Connecting / Writing：请求字节尚未确认完整写出，此时失去连接、超时、取消或
 *     Close 都是**确定失败**（请求可能一次都没有发出）；
 *   - RequestCommitted：请求字节已全部交给底层 socket，之后未取得可信回复终态时为 OutcomeUnknown
 *     （请求可能已生效，回复丢失）；
 *   - ReplyTerminal：已获得可信回复终态（完整响应，含明确的 4xx/5xx）。
 */
enum class RequestPhase {
    NotStarted = 0,   ///< 尚未发起任何 I/O（未解析、未连接）。
    Connecting,       ///< 解析或连接中。
    Writing,          ///< 请求字节写出中，尚未确认完整写出。
    RequestCommitted, ///< 请求字节已完整写出，等待回复。
    ReplyTerminal,    ///< 已获得可信回复终态。
};

/**
 * @brief 判定请求字节是否已完整写出（RequestCommitted 或 ReplyTerminal）。
 *
 * 消费者用它代替对枚举序号的比较，不依赖枚举顺序，也不接触 Beast/Asio/socket 类型。
 */
inline constexpr bool IsRequestCommitted(RequestPhase phase) noexcept
{
    return phase == RequestPhase::RequestCommitted || phase == RequestPhase::ReplyTerminal;
}

/**
 * @brief 错误体：code 是契约分类，其余字段保留排障事实。
 *
 * domain 与 domain_code 标识错误来源，backend_* 保留后端原始事实；message 只给人读，
 * 调用方不应据此分支。
 */
struct Error {
    ErrorCode code = ErrorCode::InternalError;           ///< 错误分类码。
    std::string domain = std::string(kErrorDomainInfra); ///< 错误域，默认 "infra"。
    std::string domain_code;                             ///< 域内语义名（如 net.closed），不适用时为空。
    std::string message;                                 ///< 面向人的说明，不参与分支判定。
    std::string backend_category;                        ///< 后端错误类别名（如 generic、system、bbt.net）。
    int backend_code = 0;                                ///< 后端原始错误值。
    std::uint64_t transferred_bytes = 0;                 ///< 已传输字节数，由产生错误的协议路径按需填写，默认 0。
    ErrorDetails details;                                ///< 结构化详情，结构规则见 ValidateErrorDetails。
    /// 出站请求落定或放弃时的阶段（见 RequestPhase）。
    /// 不来自出站请求 operation 的错误（上下文、参数校验、装配、已关闭拒绝等），或该路径不携带阶段时为 nullopt。
    /// OutcomeUnknown 的机器可读判定：阶段与 code 一起看；domain_code、backend_* 与 message 保留物理原因。
    std::optional<RequestPhase> request_phase;
};

/**
 * @brief 构造只含 code 与 message 的 Error，其余字段保持默认值。
 * @param code 错误分类码。
 * @param message 面向人的说明，按值移入。
 * @return 新构造的 Error。
 */
inline Error MakeError(ErrorCode code, std::string message)
{
    Error e;
    e.code = code;
    e.message = std::move(message);
    return e;
}

namespace detail {

/// @brief 严格 UTF-8 良构检查：拒绝截断序列、过长编码、代理区与 >U+10FFFF 码点。
inline bool IsValidUtf8(std::string_view s) noexcept
{
    const auto* bytes = reinterpret_cast<const unsigned char*>(s.data());
    const std::size_t n = s.size();
    std::size_t i = 0;
    while (i < n) {
        const unsigned char c = bytes[i];
        if (c < 0x80) {
            ++i;
            continue;
        }
        std::size_t cont;
        std::uint32_t code;
        std::uint32_t lowest;
        if ((c & 0xE0) == 0xC0) {
            cont = 1;
            code = c & 0x1F;
            lowest = 0x80;
        } else if ((c & 0xF0) == 0xE0) {
            cont = 2;
            code = c & 0x0F;
            lowest = 0x800;
        } else if ((c & 0xF8) == 0xF0) {
            cont = 3;
            code = c & 0x07;
            lowest = 0x10000;
        } else
            return false;
        if (i + cont >= n)
            return false;
        for (std::size_t j = 1; j <= cont; ++j) {
            const unsigned char t = bytes[i + j];
            if ((t & 0xC0) != 0x80)
                return false;
            code = (code << 6) | (t & 0x3F);
        }
        if (code < lowest || code > 0x10FFFF)
            return false;
        if (code >= 0xD800 && code <= 0xDFFF)
            return false;
        i += cont + 1;
    }
    return true;
}

} // namespace detail

template <class T> class result;

template <> class result<void>;

/**
 * @brief 校验 Error.details 的结构：条目数、键与值的字节上限、UTF-8 良构与键的唯一性。
 *
 * 任何违规整体拒绝为 ProtocolError，不截断后继续。上层域专属语义（保留键归属与值格式）
 * 不在此校验，由上层边界负责。
 * @param error 待校验的错误体。
 * @return 通过返回 ok；违规返回 ProtocolError。
 */
inline result<void> ValidateErrorDetails(const Error& error);

/**
 * @brief 成功值 T 或错误体 Error 二选一的结果类型。
 *
 * 成功时 operator bool 为 true。对错误分支调用 value()、对成功分支调用 error() 会抛出
 * std::logic_error：这是调用方的逻辑错误，不是可恢复的运行时错误分类。
 */
template <class T> class result {
public:
    /// @brief 构造成功结果。
    static result ok(T value)
    {
        return result(OkTag{}, std::move(value));
    }
    /// @brief 构造错误结果；error 按值移入。
    static result err(Error error)
    {
        return result(ErrTag{}, std::move(error));
    }

    /// @brief 成功时为 true。
    explicit operator bool() const noexcept
    {
        return m_data.index() == 0;
    }

    /// @brief 取成功值的左值引用；在错误分支调用会抛出 std::logic_error。
    T& value() &
    {
        RequireOk();
        return std::get<0>(m_data);
    }
    /// @brief 取成功值的 const 左值引用；在错误分支调用会抛出 std::logic_error。
    const T& value() const&
    {
        RequireOk();
        return std::get<0>(m_data);
    }
    /// @brief 取出成功值（移动语义）；在错误分支调用会抛出 std::logic_error。
    T&& value() &&
    {
        RequireOk();
        return std::move(std::get<0>(m_data));
    }

    /// @brief 取错误体的引用；在成功分支调用会抛出 std::logic_error。
    Error& error() &
    {
        RequireErr();
        return std::get<1>(m_data);
    }
    /// @brief 取错误体的 const 引用；在成功分支调用会抛出 std::logic_error。
    const Error& error() const&
    {
        RequireErr();
        return std::get<1>(m_data);
    }

private:
    struct OkTag {};
    struct ErrTag {};
    result(OkTag, T value) : m_data(std::in_place_index<0>, std::move(value))
    {
    }
    result(ErrTag, Error error) : m_data(std::in_place_index<1>, std::move(error))
    {
    }

    void RequireOk() const
    {
        if (m_data.index() != 0)
            throw std::logic_error("bbt::infra::result: value() on error result");
    }
    void RequireErr() const
    {
        if (m_data.index() != 1)
            throw std::logic_error("bbt::infra::result: error() on ok result");
    }

    std::variant<T, Error> m_data;
};

/**
 * @brief result<void> 特化：只表达成功或失败，不携带值。
 *
 * 成功时 operator bool 为 true；在成功分支调用 error() 会抛出 std::logic_error。
 */
template <> class result<void> {
public:
    /// @brief 构造无值的成功结果。
    static result ok()
    {
        return result();
    }
    /// @brief 构造错误结果；error 按值移入。
    static result err(Error error)
    {
        return result(std::move(error));
    }

    /// @brief 成功时为 true。
    explicit operator bool() const noexcept
    {
        return !m_error.has_value();
    }

    /// @brief 取错误体的引用；在成功分支调用会抛出 std::logic_error。
    Error& error() &
    {
        RequireErr();
        return *m_error;
    }
    /// @brief 取错误体的 const 引用；在成功分支调用会抛出 std::logic_error。
    const Error& error() const&
    {
        RequireErr();
        return *m_error;
    }

private:
    result() = default;
    explicit result(Error error) : m_error(std::move(error))
    {
    }

    void RequireErr() const
    {
        if (!m_error.has_value())
            throw std::logic_error("bbt::infra::result: error() on ok result");
    }

    std::optional<Error> m_error;
};

inline result<void> ValidateErrorDetails(const Error& error)
{
    const ErrorDetails& details = error.details;
    if (details.size() > kMaxErrorDetails)
        return result<void>::err(MakeError(ErrorCode::ProtocolError, "error.details exceeds max item count"));
    for (std::size_t i = 0; i < details.size(); ++i) {
        const std::string& key = details[i].first;
        const std::string& val = details[i].second;
        if (key.size() > kMaxKeyBytes)
            return result<void>::err(MakeError(ErrorCode::ProtocolError, "error.details key exceeds max bytes"));
        if (val.size() > kMaxValueBytes)
            return result<void>::err(MakeError(ErrorCode::ProtocolError, "error.details value exceeds max bytes"));
        if (!detail::IsValidUtf8(key) || !detail::IsValidUtf8(val))
            return result<void>::err(MakeError(ErrorCode::ProtocolError, "error.details not valid UTF-8"));
        for (std::size_t j = 0; j < i; ++j)
            if (details[j].first == key)
                return result<void>::err(MakeError(ErrorCode::ProtocolError, "error.details duplicate key"));
    }
    // 上层域的保留键归属与格式由上层边界校验；infra 通用层只保证本函数的结构规则，
    // 契约见 docs/decisions/0002-co-network-contract.md。
    return result<void>::ok();
}

} // namespace bbt::infra
