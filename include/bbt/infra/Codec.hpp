#pragma once
// co-network/v1 N0：RPC 编解码定制点。
// Codec<T> 主模板只声明不提供实现：业务在公开业务协议头给出显式特化，
// 且必须在 binder/call 实例化前可见；不使用 ADL 或动态注册表。
// SchemaId 返回静态寿命、非空、显式版本化的稳定 ID。

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include <bbt/infra/Result.hpp>

namespace bbt::infra {

template <class T>
class Codec;

template <>
class Codec<void> {
public:
    static std::string_view SchemaId() noexcept { return "bbt.void/v1"; }
    static result<std::vector<std::uint8_t>> Encode() {
        return result<std::vector<std::uint8_t>>::ok({});
    }
    static result<void> Decode(const std::vector<std::uint8_t>& payload) {
        if (!payload.empty())
            return result<void>::err(MakeError(ErrorCode::ProtocolError,
                "Codec<void>: payload must be empty"));
        return result<void>::ok();
    }
};

template <>
class Codec<std::string> {
public:
    static std::string_view SchemaId() noexcept { return "bbt.string.utf8/v1"; }
    static result<std::vector<std::uint8_t>> Encode(const std::string& value) {
        if (!detail::IsValidUtf8(value))
            return result<std::vector<std::uint8_t>>::err(MakeError(
                ErrorCode::ProtocolError, "Codec<string>: invalid UTF-8"));
        return result<std::vector<std::uint8_t>>::ok(
            std::vector<std::uint8_t>(value.begin(), value.end()));
    }
    static result<std::string> Decode(const std::vector<std::uint8_t>& payload) {
        std::string value(payload.begin(), payload.end());
        if (!detail::IsValidUtf8(value))
            return result<std::string>::err(MakeError(
                ErrorCode::ProtocolError, "Codec<string>: invalid UTF-8"));
        return result<std::string>::ok(std::move(value));
    }
};

} // namespace bbt::infra
