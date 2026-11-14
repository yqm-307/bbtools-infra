/**
 * 位置参数 payload codec 的非模板实现；模板实现随 __TPositionalCodec.hpp 提供。
 */
#include <bbt/infra/rpc/codec/PositionalCodec.hpp>

namespace bbt::infra::rpc {

void PutU32(std::vector<std::uint8_t>& out, std::uint32_t v)
{
    for (int i = 0; i < 4; ++i)
        out.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}

bool TakeU32(const std::vector<std::uint8_t>& in, std::size_t& pos, std::uint32_t& v)
{
    if (pos > in.size() || in.size() - pos < 4)
        return false;
    v = static_cast<std::uint32_t>(in[pos]) | (static_cast<std::uint32_t>(in[pos + 1]) << 8)
        | (static_cast<std::uint32_t>(in[pos + 2]) << 16) | (static_cast<std::uint32_t>(in[pos + 3]) << 24);
    pos += 4;
    return true;
}

Error Bad(const char* what)
{
    return MakeError(ErrorCode::ProtocolError, std::string("rpc payload: ") + what);
}

Error LengthOverflow()
{
    return MakeError(ErrorCode::InvalidArgument, "rpc payload: field length exceeds 32-bit field");
}

bool FitsU32(std::size_t n) noexcept
{
    return static_cast<std::uint64_t>(n) <= static_cast<std::uint64_t>(kMaxFieldBytes);
}

// clang-format off
}
// clang-format on
