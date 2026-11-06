#pragma once

namespace bbt::infra::rpc {

inline void PutU32(std::vector<std::uint8_t>& out, std::uint32_t v)
{
    for (int i = 0; i < 4; ++i)
        out.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}

inline bool TakeU32(const std::vector<std::uint8_t>& in, std::size_t& pos, std::uint32_t& v)
{
    if (pos > in.size() || in.size() - pos < 4)
        return false;
    v = static_cast<std::uint32_t>(in[pos]) | (static_cast<std::uint32_t>(in[pos + 1]) << 8)
        | (static_cast<std::uint32_t>(in[pos + 2]) << 16) | (static_cast<std::uint32_t>(in[pos + 3]) << 24);
    pos += 4;
    return true;
}

inline Error Bad(const char* what)
{
    return MakeError(ErrorCode::ProtocolError, std::string("rpc payload: ") + what);
}

inline Error LengthOverflow()
{
    return MakeError(ErrorCode::InvalidArgument, "rpc payload: field length exceeds 32-bit field");
}

template <class T> inline result<std::vector<std::uint8_t>> EncodeOne(const T& value)
{
    using D = Decay<T>;
    static_assert(scalar_supported_v<D>, "rpc payload: positional argument has no scalar codec");
    static_assert(scalar_size_supported_v<D>, "rpc payload: integer positional argument wider than 8 bytes "
                                              "is unsupported");
    static_assert(float_size_supported_v<D>, "rpc payload: float positional argument must be 4 or 8 bytes");
    std::vector<std::uint8_t> out;
    constexpr auto wt = wire_type_v<D>;
    if constexpr (wt == WireType::Bool) {
        out.push_back(value ? 1 : 0);
    } else if constexpr (wt == WireType::Int || wt == WireType::Uint) {
        const std::uint64_t u = static_cast<std::uint64_t>(value);
        for (std::size_t i = 0; i < sizeof(D); ++i)
            out.push_back(static_cast<std::uint8_t>(u >> (8 * i)));
    } else if constexpr (wt == WireType::Float) {
        if constexpr (sizeof(D) == 4) {
            std::uint32_t u = 0;
            std::memcpy(&u, &value, sizeof(u));
            for (int i = 0; i < 4; ++i)
                out.push_back(static_cast<std::uint8_t>(u >> (8 * i)));
        } else {
            std::uint64_t u = 0;
            std::memcpy(&u, &value, sizeof(u));
            for (int i = 0; i < 8; ++i)
                out.push_back(static_cast<std::uint8_t>(u >> (8 * i)));
        }
    } else if constexpr (wt == WireType::String) {
        out.insert(out.end(), value.begin(), value.end());
    } else {
        out.insert(out.end(), value.begin(), value.end());
    }
    return result<std::vector<std::uint8_t>>::ok(std::move(out));
}

template <class T> inline result<T> DecodeOne(std::uint8_t type, const std::uint8_t* data, std::size_t len)
{
    using D = Decay<T>;
    static_assert(scalar_supported_v<D>, "rpc payload: positional argument has no scalar codec");
    static_assert(scalar_size_supported_v<D>, "rpc payload: integer positional argument wider than 8 bytes "
                                              "is unsupported");
    static_assert(float_size_supported_v<D>, "rpc payload: float positional argument must be 4 or 8 bytes");
    if (type != static_cast<std::uint8_t>(wire_type_v<D>))
        return result<T>::err(MakeError(ErrorCode::TypeMismatch, "rpc payload argument type mismatch"));
    if constexpr (std::is_same_v<D, bool>) {
        if (len != 1 || data[0] > 1)
            return result<T>::err(Bad("invalid bool"));
        return result<T>::ok(data[0] != 0);
    } else if constexpr (std::is_integral_v<D> && !std::is_same_v<D, bool>) {
        if (len != sizeof(D))
            return result<T>::err(Bad("invalid integer length"));
        std::uint64_t u = 0;
        for (std::size_t i = 0; i < len; ++i)
            u |= static_cast<std::uint64_t>(data[i]) << (8 * i);
        if constexpr (std::is_signed_v<D>) {
            if (len < 8 && (u & (std::uint64_t{1} << (len * 8 - 1))))
                u |= ~std::uint64_t{0} << (len * 8);
            return result<T>::ok(static_cast<D>(static_cast<std::int64_t>(u)));
        } else {
            return result<T>::ok(static_cast<D>(u));
        }
    } else if constexpr (std::is_floating_point_v<D>) {
        if (len != sizeof(D))
            return result<T>::err(Bad("invalid float length"));
        D out{};
        if constexpr (sizeof(D) == 4) {
            std::uint32_t u = 0;
            for (int i = 0; i < 4; ++i)
                u |= static_cast<std::uint32_t>(data[i]) << (8 * i);
            std::memcpy(&out, &u, sizeof(out));
        } else {
            std::uint64_t u = 0;
            for (int i = 0; i < 8; ++i)
                u |= static_cast<std::uint64_t>(data[i]) << (8 * i);
            std::memcpy(&out, &u, sizeof(out));
        }
        return result<T>::ok(out);
    } else if constexpr (std::is_same_v<D, std::string>) {
        return result<T>::ok(std::string(reinterpret_cast<const char*>(data), len));
    } else {
        return result<T>::ok(std::vector<std::uint8_t>(data, data + len));
    }
}

template <class T> inline result<T> DecodeAt(const std::vector<std::uint8_t>& in, std::size_t wanted)
{
    std::size_t pos = 0;
    std::uint32_t count = 0;
    if (!TakeU32(in, pos, count))
        return result<T>::err(Bad("header truncated"));
    if (wanted >= count)
        return result<T>::err(MakeError(ErrorCode::InvalidArgument, "rpc payload argument index out of range"));

    std::optional<T> value;
    for (std::size_t index = 0; index < count; ++index) {
        if (pos >= in.size())
            return result<T>::err(Bad("missing argument"));
        const std::uint8_t type = in[pos++];
        std::uint32_t len = 0;
        if (!TakeU32(in, pos, len) || len > in.size() - pos)
            return result<T>::err(Bad("truncated argument"));
        if (index == wanted) {
            auto decoded = DecodeOne<T>(type, in.data() + pos, len);
            if (!decoded)
                return result<T>::err(std::move(decoded.error()));
            value = std::move(decoded.value());
        }
        pos += len;
    }
    if (pos != in.size())
        return result<T>::err(Bad("trailing garbage"));
    if (!value.has_value())
        return result<T>::err(Bad("missing argument"));
    return result<T>::ok(std::move(*value));
}

template <class... Ts, std::size_t... I>
inline result<std::tuple<Ts...>> DecodeTuple(const std::vector<std::uint8_t>& in, std::index_sequence<I...>)
{
    std::size_t pos = 0;
    std::uint32_t count = 0;
    if (!TakeU32(in, pos, count))
        return result<std::tuple<Ts...>>::err(Bad("header truncated"));
    if (count != sizeof...(Ts))
        return result<std::tuple<Ts...>>::err(
            MakeError(ErrorCode::InvalidArgument, "rpc payload argument count mismatch"));
    std::tuple<Ts...> out;
    result<void> status = result<void>::ok();
    auto one = [&](auto index) {
        constexpr std::size_t N = decltype(index)::value;
        using T = std::tuple_element_t<N, std::tuple<Ts...>>;
        if (!status)
            return;
        if (pos >= in.size()) {
            status = result<void>::err(Bad("missing argument"));
            return;
        }
        const std::uint8_t type = in[pos++];
        std::uint32_t len = 0;
        if (!TakeU32(in, pos, len) || len > in.size() - pos) {
            status = result<void>::err(Bad("truncated argument"));
            return;
        }
        auto v = DecodeOne<T>(type, in.data() + pos, len);
        pos += len;
        if (!v) {
            status = result<void>::err(std::move(v.error()));
            return;
        }
        std::get<N>(out) = std::move(v.value());
    };
    (one(std::integral_constant<std::size_t, I>{}), ...);
    if (!status)
        return result<std::tuple<Ts...>>::err(std::move(status.error()));
    if (pos != in.size())
        return result<std::tuple<Ts...>>::err(Bad("trailing garbage"));
    return result<std::tuple<Ts...>>::ok(std::move(out));
}

template <class Tuple, std::size_t... I>
inline result<std::vector<std::uint8_t>> EncodeTuple(const Tuple& values, std::index_sequence<I...>)
{
    static_assert(sizeof...(I) <= std::size_t{kMaxFieldBytes}, "rpc payload: argument count exceeds 32-bit field");
    std::vector<std::uint8_t> out;
    PutU32(out, sizeof...(I));
    result<void> status = result<void>::ok();
    auto one = [&](auto index) {
        constexpr std::size_t N = decltype(index)::value;
        if (!status)
            return;
        auto encoded = EncodeOne(std::get<N>(values));
        if (!encoded) {
            status = result<void>::err(std::move(encoded.error()));
            return;
        }
        if (!FitsU32(encoded.value().size())) {
            status = result<void>::err(LengthOverflow());
            return;
        }
        out.push_back(static_cast<std::uint8_t>(wire_type_v<std::tuple_element_t<N, Tuple>>));
        PutU32(out, static_cast<std::uint32_t>(encoded.value().size()));
        out.insert(out.end(), encoded.value().begin(), encoded.value().end());
    };
    (one(std::integral_constant<std::size_t, I>{}), ...);
    if (!status)
        return result<std::vector<std::uint8_t>>::err(std::move(status.error()));
    return result<std::vector<std::uint8_t>>::ok(std::move(out));
}

} // namespace bbt::infra::rpc
