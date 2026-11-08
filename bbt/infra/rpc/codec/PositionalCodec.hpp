#pragma once

/**
 * @file PositionalCodec.hpp
 * @brief RPC 位置参数 payload codec 的公共面（infra 唯一实现）。
 *
 * 提供字节规则、schema 常量、错误码与 EncodeOne/DecodeOne/DecodeAt/
 * DecodeTuple/EncodeTuple：count/tag/length 均为小端，schema 标识为
 * bbt.rpc.positional/v1。本头只承载通用位置参数 codec 与可继承的
 * ProtoCodec 默认 trait seam，不承载 CoRpcReq/CoRpcResp 业务包装，也不
 * include 任何 protobuf 头；业务 ProtoCodec 特化留在上层（framework），
 * infra 不导入业务 proto。
 *
 * 依赖与命名：仅依赖 bbt::infra::result/Error（bbt/infra/Result.hpp），
 * namespace 为 bbt::infra::rpc，不依赖 bbt-framework。
 *
 * 与 framework `CoRpc.hpp` 的位置参数 codec 映射关系见
 * docs/decisions/0009-rpc-runtime.md §4.1。
 */

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

/// 位置参数 schema 标识。上层应引用本常量而不是复制字面量，避免出现
/// 第二套 schema 标识。
inline constexpr std::string_view kRpcPositionalSchema = "bbt.rpc.positional/v1";

/**
 * @brief 逻辑 field 的 wire tag。
 *
 * 数值即线上字节，不得重排；新增类型只能追加。
 */
enum class WireType : std::uint8_t {
    Bool = 1,
    Int = 2,
    Uint = 3,
    Float = 4,
    String = 5,
    Bytes = 6,
};

/**
 * @brief 无 protobuf 依赖的 codec 默认 trait seam，默认 kDefined=false。
 *
 * 上层（framework）可令其 ProtoCodec 继承本模板再对业务类型显式特化：既保留
 * 既有用户特化，又不让 infra 认识业务 proto 类型。只提供 trait，不新增
 * registry/codecbase/polymorphism。
 * @tparam T 待编码/解码类型
 */
template <class T> struct ProtoCodec {
    static constexpr bool kDefined = false; ///< 是否已为 T 提供 codec 特化
};

/// T 是否有 ProtoCodec 特化。
template <class T> inline constexpr bool is_proto_codec_v = ProtoCodec<T>::kDefined;

/// 去引用/去 cv 后的存储类型。
template <class T> using Decay = std::decay_t<T>;

/// T 是否为 bytes（std::vector<std::uint8_t>）。
template <class T> inline constexpr bool is_bytes_v = std::is_same_v<Decay<T>, std::vector<std::uint8_t>>;

/// 标量 → wire tag。未识别类型落 String（真正拒绝在 EncodeOne/DecodeOne
/// 的 scalar_supported_v 静态断言）。
template <class T>
inline constexpr WireType wire_type_v = [] {
    using D = Decay<T>;
    if constexpr (std::is_same_v<D, bool>)
        return WireType::Bool;
    else if constexpr (std::is_integral_v<D> && std::is_signed_v<D>)
        return WireType::Int;
    else if constexpr (std::is_integral_v<D> && std::is_unsigned_v<D>)
        return WireType::Uint;
    else if constexpr (std::is_floating_point_v<D>)
        return WireType::Float;
    else if constexpr (std::is_same_v<D, std::string>)
        return WireType::String;
    else if constexpr (is_bytes_v<D>)
        return WireType::Bytes;
    else
        return WireType::String;
}();

/// 可编码标量：bool / 整数 / 浮点 / string / bytes。
template <class T>
inline constexpr bool scalar_supported_v =
    std::is_same_v<Decay<T>, bool>
    || (std::is_integral_v<Decay<T>> && !std::is_same_v<Decay<T>, bool>) || std::is_floating_point_v<Decay<T>>
    || std::is_same_v<Decay<T>, std::string> || is_bytes_v<T>;

/// 32 位长度字段上界：encode 端写长度前显式检查，超界报错而不是静默截断。
inline constexpr std::uint32_t kMaxFieldBytes = 0xFFFFFFFFu;

/// 长度是否放得进 wire 的 32 位长度字段。用 64 位比较，32 位平台上恒真。
bool FitsU32(std::size_t n) noexcept;

/// 追加 4 字节小端长度。
void PutU32(std::vector<std::uint8_t>& out, std::uint32_t v);

/// 从 pos 处读取 4 字节小端长度并前移 pos；越界返回 false 且不改 pos。
bool TakeU32(const std::vector<std::uint8_t>& in, std::size_t& pos, std::uint32_t& v);

/// 构造结构/字节错误（ProtocolError，前缀 "rpc payload: "）。
Error Bad(const char* what);

/// 编码端长度超出 32 位字段：调用方的值无法在 wire 上表达。
Error LengthOverflow();

/**
 * @brief 整数按 sizeof(D) 写字节的限制：超过 8 字节会在 len<8 的符号扩展处
 *        左移越界。
 *
 * 不扩张类型支持，超出即编译期拒绝。
 * @tparam D 去引用后的存储类型
 */
template <class D>
inline constexpr bool scalar_size_supported_v = !(std::is_integral_v<D> && !std::is_same_v<D, bool>) || sizeof(D) <= 8;

/// 浮点只按 4/8 字节 memcpy 编解码，long double(16) 等会错读/错写并截断。
template <class D>
inline constexpr bool float_size_supported_v = !std::is_floating_point_v<D> || sizeof(D) == 4 || sizeof(D) == 8;

/**
 * @brief 单标量编码为 payload 字节（不含 tag/length，由 EncodeTuple 追加）。
 * @tparam T 标量类型，须满足 scalar_supported_v
 * @param value 待编码标量
 * @return 按 WireType 规则排列的字节；不支持的宽度在编译期被 static_assert 拒绝
 */
template <class T> result<std::vector<std::uint8_t>> EncodeOne(const T& value);

/**
 * @brief 单标量解码。
 *
 * type 必须等于 T 的 wire tag（否则 TypeMismatch），长度必须匹配标量宽度
 * （否则 ProtocolError）。
 * @tparam T 目标标量类型，须满足 scalar_supported_v
 * @param type 线上的 WireType 字节
 * @param data payload 起始地址（不含 tag/length）
 * @param len payload 字节数
 * @return 解码值或类型/长度错误
 */
template <class T> result<T> DecodeOne(std::uint8_t type, const std::uint8_t* data, std::size_t len);

/**
 * @brief 按序号取单个位置参数，返回第 wanted 个元素。
 *
 * 越界返回 InvalidArgument；截断、缺参数、尾部多余字节返回 ProtocolError。
 * @tparam T 目标标量类型
 * @param in 整条位置参数 payload
 * @param wanted 目标下标（从 0 起）
 * @return 解码值或错误
 */
template <class T> result<T> DecodeAt(const std::vector<std::uint8_t>& in, std::size_t wanted);

/**
 * @brief 解码整条位置参数 tuple：count 必须严格等于 tuple 元素数。
 * @tparam Ts tuple 元素类型
 * @tparam I 下标序列
 * 下标序列参数只用于展开，不参与语义。
 * @param in 整条位置参数 payload
 * @return 解码后的 tuple 或错误
 */
template <class... Ts, std::size_t... I>
result<std::tuple<Ts...>> DecodeTuple(const std::vector<std::uint8_t>& in, std::index_sequence<I...>);

/**
 * @brief 编码整条位置参数 tuple。
 *
 * 布局：count(u32) + 逐字段 [tag(u8)][len(u32)][payload]。写长度前用
 * FitsU32 显式拒绝 32 位截断，不静默丢高位。
 * @tparam Tuple tuple 类型
 * @tparam I 下标序列
 * 下标序列参数只用于展开，不参与语义。
 * @param values 待编码的 tuple
 * @return payload 字节或编码错误
 */
template <class Tuple, std::size_t... I>
result<std::vector<std::uint8_t>> EncodeTuple(const Tuple& values, std::index_sequence<I...>);

} // namespace bbt::infra::rpc

#include <bbt/infra/rpc/codec/__TPositionalCodec.hpp>
