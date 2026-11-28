/**
 * @file FileSource.hpp
 * @brief 本地文件配置源。
 *
 * 文件格式为逐行文本，由模块自带的行式解析器实现，不引入第三方解析依赖：
 *   - 以 '#' 开头的行为注释，空行忽略；行尾不产生注释，'#' 在值中按普通字符保留；
 *   - 每行 "key = value"，按首个 '=' 切分，两端空白（空格/制表符/回车）裁剪，
 *     值中的 '=' 原样保留；
 *   - 无 section 概念：'['、']' 与 ';' 都不是特殊字符，"[s]" 这样的行因缺少 '=' 判格式错误；
 *   - 保留键 "version" 为可选 uint64（缺省 0），不进入 Value；多次出现时后者覆盖前者；
 *   - 其余键重复、键为空或 "version" 不是数字，均判为格式错误。
 *
 * version 取自文件内容，revision 为解析后 Value 的内容指纹，二者都不依赖墙钟或 mtime。
 * 读取失败（文件缺失、格式错误、读取失败）返回错误结果，不发布伪成功快照；空文件是合法
 * 空配置。与 Boost.PropertyTree 行式 INI 的语义对照及自研取舍见 docs/guides/config-watch.md。
 */
#pragma once

#include <memory>
#include <string>

#include <bbt/infra/config/Source.hpp>

namespace bbt::infra::config {

/**
 * @brief 本地文件配置源。
 */
class FileSource final : public ISource {
public:
    /**
     * @brief 共享句柄类型。
     */
    using SPtr = std::shared_ptr<FileSource>;

    /**
     * @brief 创建文件源。
     * @param path 文件路径。
     * @param ns 配置命名空间。
     * @param key 命名空间内的键。
     * @return 成功返回文件源句柄；参数为空时返回 InvalidArgument。
     */
    static result<SPtr> Create(std::string path, std::string ns, std::string key);

    /**
     * @brief 解析文件并返回快照；失败返回错误结果。
     */
    result<Snapshot> Read() override;

    /**
     * @brief 源标识，形如 "file:<path>"。
     */
    std::string SourceId() const noexcept override;

    /**
     * @brief 文件路径。
     */
    const std::string& Path() const noexcept;

private:
    FileSource(std::string path, std::string ns, std::string key);

    std::string m_path;
    std::string m_ns;
    std::string m_key;
    std::string m_source;
};

// clang-format off
}
// clang-format on
