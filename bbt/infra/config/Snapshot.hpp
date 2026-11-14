/**
 * @file Snapshot.hpp
 * @brief 版本化的不可变配置快照。
 *
 * 契约来源：docs/decisions/0006-infra-foundation-and-dynamic-config.md。
 */
#pragma once

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

    /**
     * 配置命名空间。
     */
    std::string ns;

    /**
     * 命名空间内的键。
     */
    std::string key;

    /**
     * 源给出的版本号。
     */
    std::uint64_t version{0};

    /**
     * 内容指纹，与 Value::Fingerprint 一致。
     */
    std::string revision;

    /**
     * 观测时刻，仅作元数据。
     */
    std::chrono::system_clock::time_point observed_at{};

    /**
     * 源标识，如 "memory:<ns>/<key>" 或 "file:<path>"。
     */
    std::string source;

    /**
     * 配置值。
     */
    Value value;

    /**
     * @brief 组合键，仅用于标识，不作语义分隔保证。
     */
    std::string FullKey() const;

    /**
     * @brief 版本身份：(version, revision) 都相同时视为重复版本。
     */
    bool SameVersionAs(const Snapshot& other) const noexcept
    {
        return version == other.version && revision == other.revision;
    }
};

// clang-format off
}
// clang-format on
