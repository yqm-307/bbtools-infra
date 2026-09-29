#pragma once
// config/v1：版本化不可变快照。契约来源：docs/decisions/0006（bbtools-infra#35）。

#include <chrono>
#include <cstdint>
#include <string>

#include <bbt/infra/config/Value.hpp>

namespace bbt::infra::config {

// 一次配置读取/变更携带的最小元数据。version 由源给出（单调版本或内容推导），
// revision 为内容指纹（etag 等价）；observed_at 仅作观测元数据，不参与去重。
struct Snapshot {
    std::string ns;     // 配置命名空间
    std::string key;    // 命名空间内键
    std::uint64_t version{0};
    std::string revision;
    std::chrono::system_clock::time_point observed_at{};
    std::string source; // 源标识，如 "memory:<ns>/<key>" / "file:<path>"
    Value value;

    // 组合键，仅用于标识，不作语义分隔保证。
    std::string FullKey() const { return ns + "/" + key; }

    // 版本身份：同一配置单元在 (version, revision) 都不变时视为重复版本。
    bool SameVersionAs(const Snapshot& other) const noexcept {
        return version == other.version && revision == other.revision;
    }
};

} // namespace bbt::infra::config
