/**
 * @file MemorySource.hpp
 * @brief 内存配置源：Set 立即发布新快照（版本单调加一，revision 为内容指纹）。
 *
 * 线程安全：Set、Read、Current 可在任意线程调用。契约来源：docs/decisions/0006-infra-foundation-and-dynamic-config.md。
 */
#pragma once

#include <mutex>
#include <string>
#include <utility>

#include <bbt/infra/config/Source.hpp>

namespace bbt::infra::config {

/**
 * @brief 内存配置源，可在运行时用 Set 更新内容。
 */
class MemorySource final : public ISource {
public:
    /**
     * @brief 以命名空间与键构造，初始版本为 0。
     */
    MemorySource(std::string ns, std::string key);

    /**
     * @brief 发布新内容，版本为上一版本加一。
     * @param value 新配置值。
     * @return 已发布的快照。
     */
    Snapshot Set(Value value);

    /**
     * @brief 以显式版本发布，供测试与回填使用。
     * @param value 新配置值。
     * @param version 必须严格大于当前版本，否则返回 InvalidArgument。
     * @return 已发布的快照。
     */
    result<Snapshot> Set(Value value, std::uint64_t version);

    /**
     * @brief 读取当前快照。
     */
    result<Snapshot> Read() override;

    /**
     * @brief 源标识，形如 "memory:<ns>/<key>"。
     */
    std::string SourceId() const noexcept override;

    /**
     * @brief 返回当前快照的拷贝。
     */
    Snapshot Current() const;

private:
    /**
     * 保护 m_current 与发布顺序。
     */
    mutable std::mutex m_mtx;
    std::string m_ns;
    std::string m_key;
    std::string m_source;
    Snapshot m_current;
};

// clang-format off
}
// clang-format on
