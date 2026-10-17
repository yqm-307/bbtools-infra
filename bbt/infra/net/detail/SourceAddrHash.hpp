#pragma once

// 内部件：IEEE CRC32（与 Go hash/crc32 ChecksumIEEE 同算法），供
// SourceAddrHash 选择逻辑使用。detail 路径，不进公共头自检、不作为
// 公开 API；测试经同仓 detail 头直接引用固定校验向量。
//
// 用 Boost.crc_32_type 的理由：成熟 IEEE CRC32 实现，复用成熟能力
// （统一约束 2）；不用 std::hash（跨实现不稳定）、不自造算法、不新增
// 依赖（Boost 已是仓内既有依赖）。

#include <boost/crc.hpp>

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace bbt::infra::net {

inline std::uint32_t Crc32Checksum(std::string_view bytes) noexcept
{
    boost::crc_32_type crc;
    crc.process_bytes(bytes.data(), bytes.size());
    return crc.checksum();
}

} // namespace bbt::infra::net
