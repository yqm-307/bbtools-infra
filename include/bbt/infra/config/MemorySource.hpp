#pragma once
// config/v1：内存源。Set() 立即发布新快照（版本单调 +1，revision 为内容指纹）。
// 线程安全：Set/Read/Current 可在任意线程调用。契约来源：docs/decisions/0006。

#include <mutex>
#include <string>
#include <utility>

#include <bbt/infra/config/Source.hpp>

namespace bbt::infra::config {

class MemorySource final : public ISource {
public:
    MemorySource(std::string ns, std::string key);

    // 发布新内容：version = 上一版本 + 1；返回已发布快照。
    Snapshot Set(Value value);
    // 显式版本发布（测试/回填用）；version 必须严格大于当前版本。
    result<Snapshot> Set(Value value, std::uint64_t version);

    result<Snapshot> Read() override;
    std::string SourceId() const noexcept override;
    Snapshot Current() const;

private:
    mutable std::mutex m_mtx;
    std::string m_ns;
    std::string m_key;
    std::string m_source;
    Snapshot m_current;
};

} // namespace bbt::infra::config
