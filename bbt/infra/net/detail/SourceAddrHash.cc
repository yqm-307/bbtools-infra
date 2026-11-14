#include <bbt/infra/net/detail/SourceAddrHash.hpp>

namespace bbt::infra::net {

std::uint32_t Crc32Checksum(std::string_view bytes) noexcept
{
    boost::crc_32_type crc;
    crc.process_bytes(bytes.data(), bytes.size());
    return crc.checksum();
}

// clang-format off
}
// clang-format on
