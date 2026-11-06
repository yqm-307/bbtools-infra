#pragma once
/** @file Source.hpp
 * @brief 配置源抽象：只负责读取带版本的快照，变更监测由 Watcher 在协程域轮询驱动。
 *
 * 契约来源：docs/decisions/0006-infra-foundation-and-dynamic-config.md。
 */

#include <memory>
#include <string>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/config/Snapshot.hpp>

namespace bbt::infra::config {

/**
 * @brief 配置源接口。读取失败必须返回错误结果，不得发布伪成功快照。
 *
 * 接口不泄漏第三方或 provider 类型。
 */
class ISource {
public:
    /// @brief 虚析构，供上层按接口持有具体实现。
    virtual ~ISource() = default;

    /// @brief 读取当前快照；失败返回错误结果。
    virtual result<Snapshot> Read() = 0;
    /// @brief 源标识，用于日志与快照的 source 字段。
    virtual std::string SourceId() const noexcept = 0;
};

/// @brief 配置源的共享句柄。
using SourcePtr = std::shared_ptr<ISource>;

} // namespace bbt::infra::config
