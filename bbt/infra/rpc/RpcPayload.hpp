#pragma once
// bbtools-infra：RPC 位置参数 payload codec 公共面（co-network/v1 通用下层算法）。
//
// 本头把 framework `CoRpc.hpp`（基线 bd9c4905 的 rpc_detail 通用区间）的
// 位置参数 codec 收敛为 infra 唯一实现，供消费侧迁移前置：
//   * 字节规则、schema 常量、错误码与 EncodeOne/DecodeOne/DecodeAt/
//     DecodeTuple/EncodeTuple 语义与旧实现逐位一致（count/tag/length 小端，
//     schema bbt.rpc.positional/v1）；
//   * 只承载通用位置参数 codec 与可继承的 ProtoCodec 默认 trait seam，
//     不承载 CoRpcReq/CoRpcResp 业务包装，也不 include 任何 protobuf 头；
//     业务 ProtoCodec 特化仍留在上层（framework），infra 不导入业务 proto。
//
// 依赖与命名：仅依赖 bbt::infra::result/Error（bbt/infra/Result.hpp），
// namespace 收敛到 bbt::infra::rpc（AGENTS.md「namespace 不超过三层」），
// 无 bbt-framework 依赖。
//
// 兼容基线与迁移映射见 docs/decisions/0009-rpc-runtime.md「codec 兼容映射」。
// 旧源 pin：framework rev bd9c490541ccad10cf32e15b91bb40104aa86118，
//   blob oid 08d1c954553f625ef396e534d898d6a2697ac232，
//   sha256 ae37d61fdcfe604f0c08ba518fffd3721a2d56f51202b471177ae19b071cf0d4。

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/infra/Result.hpp>

namespace bbt::infra::rpc {

// 位置参数 schema 常量（与 framework 旧 `kCoRpcPositionalSchema` 值相同）。
// 上层迁移时应引用本常量而不是复制字面量，避免出现第二套 schema 标识。
inline constexpr std::string_view kRpcPositionalSchema = "bbt.rpc.positional/v1";

// 逻辑 field 的 tag（与旧 WireType 数值逐位一致，不得重排）。
enum class WireType : std::uint8_t {
    Bool = 1, Int = 2, Uint = 3, Float = 4, String = 5, Bytes = 6,
};

// 无 protobuf 依赖的 codec 默认 trait seam：默认 kDefined=false。
// 上层（framework）可令其 ProtoCodec 继承本模板再对业务类型显式特化，
// 既保留既有用户特化，又不让 infra 认识业务 proto 类型。
// 只提供 trait，不新增 registry/codecbase/polymorphism。
template <class T>
struct ProtoCodec {
    static constexpr bool kDefined = false;
};

template <class T>
inline constexpr bool is_proto_codec_v = ProtoCodec<T>::kDefined;

// 去引用/去 cv 后的存储类型。
template <class T>
using Decay = std::decay_t<T>;

template <class T>
inline constexpr bool is_bytes_v =
    std::is_same_v<Decay<T>, std::vector<std::uint8_t>>;

// 标量 → wire tag。未识别类型落 String（与旧实现一致；真正的拒绝在
// EncodeOne/DecodeOne 的 scalar_supported_v 静态断言）。
template <class T>
inline constexpr WireType wire_type_v = [] {
    using D = Decay<T>;
    if constexpr (std::is_same_v<D, bool>) return WireType::Bool;
    else if constexpr (std::is_integral_v<D> && std::is_signed_v<D>)
        return WireType::Int;
    else if constexpr (std::is_integral_v<D> && std::is_unsigned_v<D>)
        return WireType::Uint;
    else if constexpr (std::is_floating_point_v<D>) return WireType::Float;
    else if constexpr (std::is_same_v<D, std::string>) return WireType::String;
    else if constexpr (is_bytes_v<D>) return WireType::Bytes;
    else return WireType::String;
}();

// 可编码标量：bool / 整数 / 浮点 / string / bytes。
template <class T>
inline constexpr bool scalar_supported_v =
    std::is_same_v<Decay<T>, bool> ||
    (std::is_integral_v<Decay<T>> && !std::is_same_v<Decay<T>, bool>) ||
    std::is_floating_point_v<Decay<T>> ||
    std::is_same_v<Decay<T>, std::string> || is_bytes_v<T>;

// 32 位长度字段上界：encode 端写长度前显式检查，超界报错而不是静默截断。
inline constexpr std::uint32_t kMaxFieldBytes = 0xFFFFFFFFu;

// 长度是否放得进 wire 的 32 位长度字段。用 64 位比较，32 位平台上恒真。
inline bool FitsU32(std::size_t n) noexcept {
    return static_cast<std::uint64_t>(n) <=
           static_cast<std::uint64_t>(kMaxFieldBytes);
}

inline void PutU32(std::vector<std::uint8_t>& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i)
        out.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}

inline bool TakeU32(const std::vector<std::uint8_t>& in, std::size_t& pos,
                    std::uint32_t& v) {
    if (pos > in.size() || in.size() - pos < 4) return false;
    v = static_cast<std::uint32_t>(in[pos]) |
        (static_cast<std::uint32_t>(in[pos + 1]) << 8) |
        (static_cast<std::uint32_t>(in[pos + 2]) << 16) |
        (static_cast<std::uint32_t>(in[pos + 3]) << 24);
    pos += 4;
    return true;
}

// 结构/字节错误：与旧实现同一前缀与错误码（ProtocolError）。
inline Error Bad(const char* what) {
    return MakeError(ErrorCode::ProtocolError,
                     std::string("rpc payload: ") + what);
}

// 编码端长度超出 32 位字段：调用方值无法在 wire 上表达。
inline Error LengthOverflow() {
    return MakeError(ErrorCode::InvalidArgument,
                     "rpc payload: field length exceeds 32-bit field");
}

// 迁移兼容性的编译期约束（旧实现隐含 UB 面显式化）：
//   * 整数按 sizeof(D) 写字节并在 len<8 时做符号扩展，>8 字节会左移越界；
//   * 浮点只按 4/8 字节 memcpy，long double(16) 等会错读/错写且截断。
// 不扩张类型支持，超出即编译期拒绝。
template <class D>
inline constexpr bool scalar_size_supported_v =
    !(std::is_integral_v<D> && !std::is_same_v<D, bool>) || sizeof(D) <= 8;

template <class D>
inline constexpr bool float_size_supported_v =
    !std::is_floating_point_v<D> || sizeof(D) == 4 || sizeof(D) == 8;

// 单标量编码为 payload 字节（不含 tag/length，由 EncodeTuple 追加）。
template <class T>
result<std::vector<std::uint8_t>> EncodeOne(const T& value) {
    using D = Decay<T>;
    static_assert(scalar_supported_v<D>,
                  "rpc payload: positional argument has no scalar codec");
    static_assert(scalar_size_supported_v<D>,
                  "rpc payload: integer positional argument wider than 8 bytes "
                  "is unsupported");
    static_assert(float_size_supported_v<D>,
                  "rpc payload: float positional argument must be 4 or 8 bytes");
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

// 单标量解码：type 必须等于 T 的 wire tag，长度必须匹配标量宽度。
template <class T>
result<T> DecodeOne(std::uint8_t type, const std::uint8_t* data,
                    std::size_t len) {
    using D = Decay<T>;
    static_assert(scalar_supported_v<D>,
                  "rpc payload: positional argument has no scalar codec");
    static_assert(scalar_size_supported_v<D>,
                  "rpc payload: integer positional argument wider than 8 bytes "
                  "is unsupported");
    static_assert(float_size_supported_v<D>,
                  "rpc payload: float positional argument must be 4 or 8 bytes");
    if (type != static_cast<std::uint8_t>(wire_type_v<D>))
        return result<T>::err(MakeError(ErrorCode::TypeMismatch,
                                        "rpc payload argument type mismatch"));
    if constexpr (std::is_same_v<D, bool>) {
        if (len != 1 || data[0] > 1)
            return result<T>::err(Bad("invalid bool"));
        return result<T>::ok(data[0] != 0);
    } else if constexpr (std::is_integral_v<D> && !std::is_same_v<D, bool>) {
        if (len != sizeof(D)) return result<T>::err(Bad("invalid integer length"));
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
        if (len != sizeof(D)) return result<T>::err(Bad("invalid float length"));
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

// 按序号取单个位置参数（旧语义保留：越界 InvalidArgument，结构错误 ProtocolError）。
template <class T>
result<T> DecodeAt(const std::vector<std::uint8_t>& in, std::size_t wanted) {
    std::size_t pos = 0;
    std::uint32_t count = 0;
    if (!TakeU32(in, pos, count))
        return result<T>::err(Bad("header truncated"));
    if (wanted >= count)
        return result<T>::err(MakeError(
            ErrorCode::InvalidArgument, "rpc payload argument index out of range"));

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

// 解码整条位置参数 tuple：count 必须严格等于 tuple 元素数。
template <class... Ts, std::size_t... I>
result<std::tuple<Ts...>> DecodeTuple(const std::vector<std::uint8_t>& in,
                                      std::index_sequence<I...>) {
    std::size_t pos = 0;
    std::uint32_t count = 0;
    if (!TakeU32(in, pos, count))
        return result<std::tuple<Ts...>>::err(Bad("header truncated"));
    if (count != sizeof...(Ts))
        return result<std::tuple<Ts...>>::err(MakeError(
            ErrorCode::InvalidArgument, "rpc payload argument count mismatch"));
    std::tuple<Ts...> out;
    result<void> status = result<void>::ok();
    auto one = [&](auto index) {
        constexpr std::size_t N = decltype(index)::value;
        using T = std::tuple_element_t<N, std::tuple<Ts...>>;
        if (!status) return;
        if (pos >= in.size()) { status = result<void>::err(Bad("missing argument")); return; }
        const std::uint8_t type = in[pos++];
        std::uint32_t len = 0;
        if (!TakeU32(in, pos, len) || len > in.size() - pos) {
            status = result<void>::err(Bad("truncated argument"));
            return;
        }
        auto v = DecodeOne<T>(type, in.data() + pos, len);
        pos += len;
        if (!v) { status = result<void>::err(std::move(v.error())); return; }
        std::get<N>(out) = std::move(v.value());
    };
    (one(std::integral_constant<std::size_t, I>{}), ...);
    if (!status) return result<std::tuple<Ts...>>::err(std::move(status.error()));
    if (pos != in.size()) return result<std::tuple<Ts...>>::err(Bad("trailing garbage"));
    return result<std::tuple<Ts...>>::ok(std::move(out));
}

// 编码整条位置参数 tuple：count(u32) + 逐字段 [tag(u8)][len(u32)][payload]。
// 写长度前用 FitsU32 显式拒绝 32 位截断，不静默丢高位。
template <class Tuple, std::size_t... I>
result<std::vector<std::uint8_t>> EncodeTuple(const Tuple& values,
                                              std::index_sequence<I...>) {
    static_assert(sizeof...(I) <= std::size_t{kMaxFieldBytes},
                  "rpc payload: argument count exceeds 32-bit field");
    std::vector<std::uint8_t> out;
    PutU32(out, sizeof...(I));
    result<void> status = result<void>::ok();
    auto one = [&](auto index) {
        constexpr std::size_t N = decltype(index)::value;
        if (!status) return;
        auto encoded = EncodeOne(std::get<N>(values));
        if (!encoded) { status = result<void>::err(std::move(encoded.error())); return; }
        if (!FitsU32(encoded.value().size())) {
            status = result<void>::err(LengthOverflow());
            return;
        }
        out.push_back(static_cast<std::uint8_t>(wire_type_v<std::tuple_element_t<N, Tuple>>));
        PutU32(out, static_cast<std::uint32_t>(encoded.value().size()));
        out.insert(out.end(), encoded.value().begin(), encoded.value().end());
    };
    (one(std::integral_constant<std::size_t, I>{}), ...);
    if (!status) return result<std::vector<std::uint8_t>>::err(std::move(status.error()));
    return result<std::vector<std::uint8_t>>::ok(std::move(out));
}

} // namespace bbt::infra::rpc
