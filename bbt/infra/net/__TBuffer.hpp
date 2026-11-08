#pragma once

#include <limits>

#include <boost/endian/conversion.hpp>

namespace bbt::infra::net {

template <class T, std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T, bool>, int>>
inline Buffer& Buffer::operator<<(T value)
{
    static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8,
                  "Buffer supports 8/16/32/64-bit integers");
    using U = std::make_unsigned_t<T>;
    const U encoded = boost::endian::native_to_big(static_cast<U>(value));
    WriteBytes({reinterpret_cast<const char*>(&encoded), sizeof(encoded)});
    return *this;
}

template <class T, std::enable_if_t<std::is_integral_v<T> && !std::is_same_v<T, bool>, int>>
inline Buffer& Buffer::operator>>(T& value)
{
    static_assert(sizeof(T) == 1 || sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8,
                  "Buffer supports 8/16/32/64-bit integers");
    using U = std::make_unsigned_t<T>;
    U encoded{};
    if (!ReadBytes(&encoded, sizeof(encoded))) {
        return *this;
    }
    const U bits = boost::endian::big_to_native(encoded);
    if constexpr (std::is_signed_v<T>) {
        // 用可表示的运算还原负值，不依赖超范围 unsigned→signed 转换。
        if (bits > static_cast<U>((std::numeric_limits<T>::max)())) {
            const U inverted = static_cast<U>(~bits);
            value = static_cast<T>(-1 - static_cast<T>(inverted));
        } else {
            value = static_cast<T>(bits);
        }
    } else {
        value = bits;
    }
    return *this;
}

} // namespace bbt::infra::net
