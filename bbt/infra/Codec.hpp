/** @file Codec.hpp
 * @brief RPC 编解码定制点：按消息类型 T 提供 SchemaId、Encode 与 Decode。
 *
 * Codec<T> 主模板只声明、不提供实现：业务在公开的业务协议头中给出显式特化，并且必须在
 * binder/call 实例化之前可见；不使用 ADL，也不使用动态注册表。
 * SchemaId 返回静态寿命、非空、显式版本化的稳定 ID。
 */
#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <bbt/infra/Result.hpp>

namespace bbt::infra {

/// @brief 编解码主模板，仅声明；每种消息类型须给出显式特化。
template <class T> class Codec;

/**
 * @brief void 消息的编解码：编码结果为空载荷，解码要求载荷为空。
 */
template <> class Codec<void> {
public:
    /// @brief schema ID，静态寿命。
    static std::string_view SchemaId() noexcept
    {
        return "bbt.void/v1";
    }
    /// @brief 编码：返回空载荷。
    static result<std::vector<std::uint8_t>> Encode();
    /// @brief 解码：载荷必须为空，否则返回 ProtocolError。
    static result<void> Decode(const std::vector<std::uint8_t>& payload);
};

/**
 * @brief std::string 的编解码：载荷为原始 UTF-8 字节，编码与解码都校验 UTF-8 良构。
 */
template <> class Codec<std::string> {
public:
    /// @brief schema ID，静态寿命。
    static std::string_view SchemaId() noexcept
    {
        return "bbt.string.utf8/v1";
    }
    /// @brief 编码：先校验 UTF-8，非法返回 ProtocolError。
    static result<std::vector<std::uint8_t>> Encode(const std::string& value);
    /// @brief 解码：先按 UTF-8 校验载荷，非法返回 ProtocolError。
    static result<std::string> Decode(const std::vector<std::uint8_t>& payload);
};

} // namespace bbt::infra

#include <bbt/infra/impl/CodecImpl.hpp>
