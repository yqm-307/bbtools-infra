#pragma once
// config/v1：最小结构化值——有序 string→string 映射 + 类型化读取 + 内容指纹。
// 契约来源：docs/decisions/0006-infra-foundation-and-dynamic-config.md（bbtools-infra#35）。
// infra 只提供读取、类型转换与基础格式；业务 schema 校验归 framework。

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

class Value {
public:
    using Map = std::map<std::string, std::string>;

    static Value FromMap(Map entries) noexcept {
        Value value;
        value.m_entries = std::move(entries);
        return value;
    }

    bool Has(std::string_view key) const noexcept { return Find(key) != nullptr; }
    std::size_t Size() const noexcept { return m_entries.size(); }
    bool Empty() const noexcept { return m_entries.empty(); }
    const Map& Entries() const noexcept { return m_entries; }

    // 原始字符串；nullptr 表示键缺失（与「值为空串」区分）。
    const std::string* Find(std::string_view key) const noexcept;

    result<std::string> GetString(std::string_view key) const;
    result<std::int64_t> GetInt64(std::string_view key) const;
    result<bool> GetBool(std::string_view key) const;
    result<double> GetDouble(std::string_view key) const;

    // 内容指纹：FNV-1a 64 作用于规范序列化（排序后 "k=v\n"），十六进制小写。
    // 用作快照 revision/etag；同一 Value 恒得同一指纹，不依赖墙钟。
    std::string Fingerprint() const noexcept;

private:
    // 实现辅助（原 bbt::infra::config::detail 自由函数）：留在类内以满足
    // 本仓「namespace 不超过三层」约束（bbt::infra::config 已是第三层）。
    static std::uint64_t Fnv1a64(std::string_view data) noexcept;
    static std::string Fnv1a64Hex(std::string_view data);
    static bool EqualsIgnoreCase(std::string_view a, std::string_view b) noexcept;

    Map m_entries;
};

inline std::uint64_t Value::Fnv1a64(std::string_view data) noexcept {
    std::uint64_t hash = 14695981039346656037ULL;
    for (const unsigned char c : data)
        hash = (hash ^ static_cast<std::uint64_t>(c)) * 1099511628211ULL;
    return hash;
}

inline std::string Value::Fnv1a64Hex(std::string_view data) {
    const std::uint64_t hash = Fnv1a64(data);
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out(16, '0');
    for (int i = 0; i < 16; ++i)
        out[static_cast<std::size_t>(i)] = kHex[(hash >> (4 * (15 - i))) & 0xF];
    return out;
}

inline bool Value::EqualsIgnoreCase(std::string_view a, std::string_view b) noexcept {
    if (a.size() != b.size()) return false;
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(a[i])) !=
            std::tolower(static_cast<unsigned char>(b[i])))
            return false;
    }
    return true;
}

inline const std::string* Value::Find(std::string_view key) const noexcept {
    const auto it = m_entries.find(std::string(key));
    return it == m_entries.end() ? nullptr : &it->second;
}

inline result<std::string> Value::GetString(std::string_view key) const {
    const std::string* found = Find(key);
    if (found == nullptr)
        return result<std::string>::err(MakeError(ErrorCode::NotFound,
            std::string("config value key not found: ") + std::string(key)));
    return result<std::string>::ok(*found);
}

inline result<std::int64_t> Value::GetInt64(std::string_view key) const {
    auto raw = GetString(key);
    if (!raw) return result<std::int64_t>::err(std::move(raw).error());
    const std::string& text = raw.value();
    std::int64_t out = 0;
    const char* begin = text.data();
    const char* end = begin + text.size();
    const auto [ptr, ec] = std::from_chars(begin, end, out);
    if (ec != std::errc() || ptr != end)
        return result<std::int64_t>::err(MakeError(ErrorCode::TypeMismatch,
            "config value not an int64: " + text));
    return result<std::int64_t>::ok(out);
}

inline result<bool> Value::GetBool(std::string_view key) const {
    auto raw = GetString(key);
    if (!raw) return result<bool>::err(std::move(raw).error());
    const std::string& text = raw.value();
    static constexpr std::string_view kTrue[]  = {"true", "1", "yes", "on"};
    static constexpr std::string_view kFalse[] = {"false", "0", "no", "off"};
    for (const auto t : kTrue)
        if (EqualsIgnoreCase(text, t)) return result<bool>::ok(true);
    for (const auto f : kFalse)
        if (EqualsIgnoreCase(text, f)) return result<bool>::ok(false);
    return result<bool>::err(MakeError(ErrorCode::TypeMismatch,
        "config value not a bool: " + text));
}

inline result<double> Value::GetDouble(std::string_view key) const {
    auto raw = GetString(key);
    if (!raw) return result<double>::err(std::move(raw).error());
    const std::string& text = raw.value();
    errno = 0;
    char* end = nullptr;
    const double out = std::strtod(text.c_str(), &end);
    if (end != text.c_str() + text.size() || errno == ERANGE)
        return result<double>::err(MakeError(ErrorCode::TypeMismatch,
            "config value not a double: " + text));
    return result<double>::ok(out);
}

inline std::string Value::Fingerprint() const noexcept {
    std::string canonical;
    for (const auto& entry : m_entries) {
        canonical += entry.first;
        canonical += '=';
        canonical += entry.second;
        canonical += '\n';
    }
    return Fnv1a64Hex(canonical);
}

} // namespace bbt::infra::config
