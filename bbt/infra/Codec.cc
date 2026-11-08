#include <bbt/infra/Codec.hpp>

namespace bbt::infra {

result<std::vector<std::uint8_t>> Codec<void>::Encode()
{
    return result<std::vector<std::uint8_t>>::ok({});
}

result<void> Codec<void>::Decode(const std::vector<std::uint8_t>& payload)
{
    if (!payload.empty())
        return result<void>::err(MakeError(ErrorCode::ProtocolError, "Codec<void>: payload must be empty"));
    return result<void>::ok();
}

result<std::vector<std::uint8_t>> Codec<std::string>::Encode(const std::string& value)
{
    if (!detail::IsValidUtf8(value))
        return result<std::vector<std::uint8_t>>::err(
            MakeError(ErrorCode::ProtocolError, "Codec<string>: invalid UTF-8"));
    return result<std::vector<std::uint8_t>>::ok(std::vector<std::uint8_t>(value.begin(), value.end()));
}

result<std::string> Codec<std::string>::Decode(const std::vector<std::uint8_t>& payload)
{
    std::string value(payload.begin(), payload.end());
    if (!detail::IsValidUtf8(value))
        return result<std::string>::err(MakeError(ErrorCode::ProtocolError, "Codec<string>: invalid UTF-8"));
    return result<std::string>::ok(std::move(value));
}

} // namespace bbt::infra
