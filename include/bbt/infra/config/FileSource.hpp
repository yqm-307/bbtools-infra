#pragma once
// config/v1：本地文件源。
//
// 文件格式（行式，无第三方解析依赖）：
//   - '#' 起始行为注释，空行忽略；
//   - "key = value" 每行一对，首个 '=' 分割，键值两端空白裁剪；
//   - 保留元数据键 "version" 为可选 uint64（缺省 0），不进入 Value；
//   - 其余键重复、键为空、"version" 非数字均判为格式错误。
//
// version 取自文件内容（显式字段），revision 为解析后 Value 的内容指纹；
// 二者都不依赖墙钟/mtime。读取失败（缺失/格式错误/读取失败）返回错误结果，
// 不发布伪成功快照。文件监测由 Watcher 在协程域内轮询，不新增 I/O 线程。

#include <memory>
#include <string>

#include <bbt/infra/config/Source.hpp>

namespace bbt::infra::config {

class FileSource final : public ISource {
public:
    using SPtr = std::shared_ptr<FileSource>;

    static result<SPtr> Create(std::string path, std::string ns, std::string key);

    result<Snapshot> Read() override;
    std::string SourceId() const noexcept override;
    const std::string& Path() const noexcept;

private:
    FileSource(std::string path, std::string ns, std::string key);

    std::string m_path;
    std::string m_ns;
    std::string m_key;
    std::string m_source;
};

} // namespace bbt::infra::config
