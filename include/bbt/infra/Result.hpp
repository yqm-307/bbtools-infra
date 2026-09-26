#pragma once
// co-network/v1 N0：统一结果表面 result<T>/result<void>、ErrorCode 与 Error。
// 契约来源：docs/decisions/0002-co-network-contract-v1.md（bbtools-infra#2）。
//
// core 的 bbt::core::util::Result<T,E> 已评估：其 IsOk/Ok/Err 访问语义与本契约
// 「value()/error() 访问错误分支抛 std::logic_error、&& 重载、void 特化」不一致，
// 直接复用需要改动 core 全仓错误协议（契约禁止），故本仓按冻结签名独立实现。

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

enum class ErrorCode {
    InvalidArgument, InvalidContext, RuntimeUnavailable, Closed,
    Cancelled, TimedOut, Overloaded, Unavailable, TransportError,
    ProtocolError, NotFound, TypeMismatch, UnsupportedRoute,
    OutcomeUnknown, RemoteError, InternalError
};

// Error.details 结构化详情上限：最多 16 项，键 ≤64 UTF-8 字节，值 ≤256 UTF-8 字节。
inline constexpr std::size_t kMaxErrorDetails = 16;
inline constexpr std::size_t kMaxKeyBytes     = 64;
inline constexpr std::size_t kMaxValueBytes   = 256;

using ErrorDetails = std::vector<std::pair<std::string, std::string>>;

// 稳定错误域：infra 自身错误的默认域。上层域（如 framework.actor）及其保留键
// 语义归各自上层边界拥有（见 decisions/0002 第 76 行的分工说明），本头不再
// 硬编码任何上层专属字段语义。
inline constexpr std::string_view kErrorDomainInfra = "infra";

struct Error {
    ErrorCode   code = ErrorCode::InternalError;
    std::string domain = std::string(kErrorDomainInfra);
    std::string domain_code;
    std::string message;
    std::string backend_category;
    int         backend_code = 0;
    std::uint64_t transferred_bytes = 0;
    ErrorDetails details;
};

inline Error MakeError(ErrorCode code, std::string message) {
    Error e;
    e.code = code;
    e.message = std::move(message);
    return e;
}

namespace detail {

// 严格 UTF-8 良构检查：拒绝截断序列、过长编码、代理区与 >U+10FFFF 码点。
inline bool IsValidUtf8(std::string_view s) noexcept {
    const auto* bytes = reinterpret_cast<const unsigned char*>(s.data());
    const std::size_t n = s.size();
    std::size_t i = 0;
    while (i < n) {
        const unsigned char c = bytes[i];
        if (c < 0x80) { ++i; continue; }
        std::size_t   cont;
        std::uint32_t code;
        std::uint32_t lowest;
        if      ((c & 0xE0) == 0xC0) { cont = 1; code = c & 0x1F; lowest = 0x80; }
        else if ((c & 0xF0) == 0xE0) { cont = 2; code = c & 0x0F; lowest = 0x800; }
        else if ((c & 0xF8) == 0xF0) { cont = 3; code = c & 0x07; lowest = 0x10000; }
        else return false;
        if (i + cont >= n) return false;
        for (std::size_t j = 1; j <= cont; ++j) {
            const unsigned char t = bytes[i + j];
            if ((t & 0xC0) != 0x80) return false;
            code = (code << 6) | (t & 0x3F);
        }
        if (code < lowest || code > 0x10FFFF) return false;
        if (code >= 0xD800 && code <= 0xDFFF) return false;
        i += cont + 1;
    }
    return true;
}

} // namespace detail

template <class T>
class result;

template <>
class result<void>;

// details 结构校验：条目/键/值上限、UTF-8 良构与重复键。
// 任何违规整体拒绝为 ProtocolError，不截断后继续。
// 上层域专属语义（保留键归属/值格式）不在此校验，由上层 ErrorDomainRule 负责。
inline result<void> ValidateErrorDetails(const Error& error);

template <class T>
class result {
public:
    static result ok(T value)      { return result(OkTag{}, std::move(value)); }
    static result err(Error error) { return result(ErrTag{}, std::move(error)); }

    explicit operator bool() const noexcept { return m_data.index() == 0; }

    T& value() &             { RequireOk();  return std::get<0>(m_data); }
    const T& value() const&  { RequireOk();  return std::get<0>(m_data); }
    T&& value() &&           { RequireOk();  return std::move(std::get<0>(m_data)); }

    Error& error() &             { RequireErr(); return std::get<1>(m_data); }
    const Error& error() const&  { RequireErr(); return std::get<1>(m_data); }

private:
    struct OkTag {};
    struct ErrTag {};
    result(OkTag, T value)      : m_data(std::in_place_index<0>, std::move(value)) {}
    result(ErrTag, Error error) : m_data(std::in_place_index<1>, std::move(error)) {}

    void RequireOk() const {
        if (m_data.index() != 0)
            throw std::logic_error("bbt::infra::result: value() on error result");
    }
    void RequireErr() const {
        if (m_data.index() != 1)
            throw std::logic_error("bbt::infra::result: error() on ok result");
    }

    std::variant<T, Error> m_data;
};

template <>
class result<void> {
public:
    static result ok()             { return result(); }
    static result err(Error error) { return result(std::move(error)); }

    explicit operator bool() const noexcept { return !m_error.has_value(); }

    Error& error() &             { RequireErr(); return *m_error; }
    const Error& error() const&  { RequireErr(); return *m_error; }

private:
    result() = default;
    explicit result(Error error) : m_error(std::move(error)) {}

    void RequireErr() const {
        if (!m_error.has_value())
            throw std::logic_error("bbt::infra::result: error() on ok result");
    }

    std::optional<Error> m_error;
};

inline result<void> ValidateErrorDetails(const Error& error) {
    const ErrorDetails& details = error.details;
    if (details.size() > kMaxErrorDetails)
        return result<void>::err(MakeError(ErrorCode::ProtocolError,
            "error.details exceeds max item count"));
    for (std::size_t i = 0; i < details.size(); ++i) {
        const std::string& key = details[i].first;
        const std::string& val = details[i].second;
        if (key.size() > kMaxKeyBytes)
            return result<void>::err(MakeError(ErrorCode::ProtocolError,
                "error.details key exceeds max bytes"));
        if (val.size() > kMaxValueBytes)
            return result<void>::err(MakeError(ErrorCode::ProtocolError,
                "error.details value exceeds max bytes"));
        if (!detail::IsValidUtf8(key) || !detail::IsValidUtf8(val))
            return result<void>::err(MakeError(ErrorCode::ProtocolError,
                "error.details not valid UTF-8"));
        for (std::size_t j = 0; j < i; ++j)
            if (details[j].first == key)
                return result<void>::err(MakeError(ErrorCode::ProtocolError,
                    "error.details duplicate key"));
    }
    // 注意：上层域的保留键归属与格式（如 framework.actor 的
    // expected_sequence）由上层边界的 ErrorDomainRule 校验负责；
    // infra 通用层只保证本函数之上的结构规则，见 decisions/0002。
    return result<void>::ok();
}

} // namespace bbt::infra
