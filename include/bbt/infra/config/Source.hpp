#pragma once
// config/v1：配置源抽象。只负责读带版本快照；变更监测由 Watcher 在协程域轮询驱动。
// 契约来源：docs/decisions/0006-infra-foundation-and-dynamic-config.md（bbtools-infra#35）。

#include <memory>
#include <string>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/config/Snapshot.hpp>

namespace bbt::infra::config {

// 读取失败必须返回错误结果，不得发布伪成功快照（Issue #35）。
// 本接口不泄漏任何第三方/provider 类型；远程适配后续单独切片。
class ISource {
public:
    virtual ~ISource() = default;

    virtual result<Snapshot> Read() = 0;
    virtual std::string SourceId() const noexcept = 0;
};

using SourcePtr = std::shared_ptr<ISource>;

} // namespace bbt::infra::config
