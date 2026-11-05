#pragma once
/** @file Snapshot.hpp
 * @brief 版本化的不可变配置快照。
 *
 * 契约来源：docs/decisions/0006-infra-foundation-and-dynamic-config.md。
 */

#include <chrono>
#include <cstdint>
#include <string>

#include <bbt/infra/config/Value.hpp>

namespace bbt::infra::config {

/**
 * @brief 一次配置读取或变更携带的元数据与值。
 *
 * version 由源给出（单调版本或内容推导），revision 为内容指纹（etag 等价）；
 * observed_at 只作观测元数据，不参与去重。
 */
struct Snapshot {
    std::string ns;                                      ///< 配置命名空间。
    std::string key;                                     ///< 命名空间内的键。
    std::uint64_t version{0};                            ///< 源给出的版本号。
    std::string revision;                                ///< 内容指纹，与 Value::Fingerprint 一致。
    std::chrono::system_clock::time_point observed_at{}; ///< 观测时刻，仅作元数据。
    std::string source;                                  ///< 源标识，如 "memory:<ns>/<key>" 或 "file:<path>"。
    Value value;                                         ///< 配置值。

    /// @brief 组合键，仅用于标识，不作语义分隔保证。
    std::string FullKey() const
    {
        return ns + "/" + key;
    }

    /**
     * @brief 版本身份：(version, revision) 都相同时视为重复版本。
     */
    bool SameVersionAs(const Snapshot& other) const noexcept
    {
        return version == other.version && revision == other.revision;
    }
};

} // namespace bbt::infra::config
