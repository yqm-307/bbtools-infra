#pragma once
// 父头末尾包含的实现；依赖父头已完成类型与声明。

namespace bbt::infra {

inline Error MakeError(ErrorCode code, std::string message)
{
    Error e;
    e.code = code;
    e.message = std::move(message);
    return e;
}

template <class T> inline result<T> result<T>::ok(T value)
{
    return result(OkTag{}, std::move(value));
}

template <class T> inline result<T> result<T>::err(Error error)
{
    return result(ErrTag{}, std::move(error));
}

template <class T> inline T& result<T>::value() &
{
    RequireOk();
    return std::get<0>(m_data);
}

template <class T> inline const T& result<T>::value() const&
{
    RequireOk();
    return std::get<0>(m_data);
}

template <class T> inline T&& result<T>::value() &&
{
    RequireOk();
    return std::move(std::get<0>(m_data));
}

template <class T> inline Error& result<T>::error() &
{
    RequireErr();
    return std::get<1>(m_data);
}

template <class T> inline const Error& result<T>::error() const&
{
    RequireErr();
    return std::get<1>(m_data);
}

template <class T> inline result<T>::result(OkTag, T value) : m_data(std::in_place_index<0>, std::move(value))
{
}

template <class T> inline result<T>::result(ErrTag, Error error) : m_data(std::in_place_index<1>, std::move(error))
{
}

template <class T> inline void result<T>::RequireOk() const
{
    if (m_data.index() != 0)
        throw std::logic_error("bbt::infra::result: value() on error result");
}

template <class T> inline void result<T>::RequireErr() const
{
    if (m_data.index() != 1)
        throw std::logic_error("bbt::infra::result: error() on ok result");
}

inline result<void> result<void>::ok()
{
    return result();
}

inline result<void> result<void>::err(Error error)
{
    return result(std::move(error));
}

inline Error& result<void>::error() &
{
    RequireErr();
    return *m_error;
}

inline const Error& result<void>::error() const&
{
    RequireErr();
    return *m_error;
}

inline result<void>::result(Error error) : m_error(std::move(error))
{
}

inline void result<void>::RequireErr() const
{
    if (!m_error.has_value())
        throw std::logic_error("bbt::infra::result: error() on ok result");
}

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

namespace bbt::infra::detail {

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

} // namespace bbt::infra::detail
