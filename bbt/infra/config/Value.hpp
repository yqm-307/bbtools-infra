/**
 * @file Value.hpp
 * @brief 最小结构化值：有序 string→string 映射，附带类型化读取与内容指纹。
 *
 * infra 只提供读取、类型转换与基础格式；业务 schema 校验归 framework。
 * 契约来源：docs/decisions/0006-infra-foundation-and-dynamic-config.md。
 */
#pragma once

#include <cctype>
#include <cerrno>
#include <charconv>
#include <cstdint>
#include <cstdlib>
#include <map>
#include <string>
#include <string_view>
#include <system_error>

#include <bbt/infra/Result.hpp>

namespace bbt::infra::config {

/**
 * @brief 有序配置值：键与值都是字符串，按键排序；类型化读取由 Get* 完成。
 */
class Value {
public:
    /**
     * @brief 底层有序映射。
     */
    using Map = std::map<std::string, std::string>;

    /**
     * @brief 由映射构造，按值移入。
     */
    static Value FromMap(Map entries) noexcept;

    /**
     * @brief 键是否存在。
     */
    bool Has(std::string_view key) const noexcept;

    /**
     * @brief 键的数量。
     */
    std::size_t Size() const noexcept { return m_entries.size(); }

    /**
     * @brief 是否不含任何键。
     */
    bool Empty() const noexcept { return m_entries.empty(); }

    /**
     * @brief 只读访问底层映射（按键有序）。
     */
    const Map& Entries() const noexcept { return m_entries; }

    /**
     * @brief 取原始字符串；键缺失返回 nullptr，与「值为空串」区分。
     */
    const std::string* Find(std::string_view key) const noexcept;

    /**
     * @brief 取字符串值；键缺失返回 NotFound。
     */
    result<std::string> GetString(std::string_view key) const;

    /**
     * @brief 解析为 int64；格式非法返回 TypeMismatch。
     */
    result<std::int64_t> GetInt64(std::string_view key) const;

    /**
     * @brief 解析为布尔：接受 true/1/yes/on 与 false/0/no/off，忽略大小写。
     */
    result<bool> GetBool(std::string_view key) const;

    /**
     * @brief 解析为 double；格式非法或溢出返回 TypeMismatch。
     */
    result<double> GetDouble(std::string_view key) const;

    /**
     * @brief 内容指纹：FNV-1a 64 作用于按键排序后逐行的 key=value 换行序列化，十六进制小写。
     *
     * 用作快照的 revision/etag；同一 Value 恒得同一指纹，不依赖墙钟。
     */
    std::string Fingerprint() const noexcept;

private:
    /**
     * 实现辅助留在类内：本仓要求 namespace 不超过三层，bbt::infra::config 已占用第三层。
     */
    static std::uint64_t Fnv1a64(std::string_view data) noexcept;
    static std::string Fnv1a64Hex(std::string_view data);
    static bool EqualsIgnoreCase(std::string_view a, std::string_view b) noexcept;

    Map m_entries;
};

// clang-format off
}
// clang-format on
