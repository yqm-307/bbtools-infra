// MCP JsonValue 实现：全仓唯一包含 Boost.JSON 源码实现的 TU。
//
// 设计要点（对应 specs R2 与 design「JsonValue」段）：
//   - 解析走 Boost 的 basic_parser + 自定义 SAX handler：词法/转义/UTF-8/数字
//     全部由 Boost 完成，本文件只做「重复对象键」「不可表示数值」「值栈装配」
//     三件 Boost 公开接口不直接提供的事，不手写任何 JSON 词法扫描。
//   - handler 与 boost/json/detail/handler.ipp（Boost 自带 DOM handler）逐回调
//     同构，避免自造第二套语义；差异仅在 on_key 里检查重复键、on_double 里拒绝
//     不可表示数值（非有限，或落到 double 的超 int64/uint64 整数词法）。
//   - 浮点用 number_precision::precise，整数天然走 int64/uint64，不经 double。
//   - 有限浮点词法按 IEEE 754 最近舍入；下溢到 0（如 1e-999 → 0）保留 IEEE 语义，
//     属正常解析而非拒绝面（见 JsonValue.hpp 与 docs/decisions/0010）。
//   - ponytail 天花板：on_number_part 与 Boost 自带 handler 同口径（忽略分片通知），
//     Boost 的 parser 会把完整数值经 on_int64/on_uint64/on_double 交付；只有当单个
//     数字字面量超过解析器内部暂存区时该回调才出现，此时 Boost 自身行为一致。

#include <bbt/infra/mcp/JsonValue.hpp>

#include <boost/json/src.hpp>

#include <cmath>
#include <limits>
#include <set>
#include <utility>
#include <vector>

namespace bbt::infra::mcp {

namespace {

// 固定错误构造：domain_code 机器可读，message 不回显任何输入字节。
Error MakeJsonError(ErrorCode code, const char* domain_code, const char* message) {
    Error e;
    e.code = code;
    e.domain_code = domain_code;
    e.message = message;
    return e;
}

template <class T>
result<T> JsonErr(ErrorCode code, const char* domain_code, const char* message) {
    return result<T>::err(MakeJsonError(code, domain_code, message));
}

// 纯整数词法（可选前导 '-'，其余全为数字）判定：无 '.'/'e'/'E'。
// Boost 的解析器把能装进 int64/uint64 的整数交给 on_int64/on_uint64；一个纯整数
// 词法落到 on_double，只可能是它已超出 int64/uint64 可表示范围（见 Boost
// basic_parser_impl 的整数溢出分支 goto finish_dub）。据此拒绝，不静默降级为 double。
bool IsIntegerLexeme(boost::json::string_view raw) noexcept {
    if (raw.empty()) return false;
    for (std::size_t i = 0; i < raw.size(); ++i) {
        const char c = raw[i];
        if (i == 0 && c == '-') continue;
        if (c < '0' || c > '9') return false;
    }
    return true;
}

// Boost 的 DOM handler 同构实现 + 对象重复键拒绝。
// 键集合按对象层级分帧；on_key/on_key_part 可能与 Boost 自带 handler 一样分片到达，
// 因此本处按「分片累积 + 完成时判定」处理，绝不假设一次到齐。
class StrictDomHandler {
public:
    // Boost 的 SAX 契约要求 handler 暴露这些容量上界（与 detail::handler 同值）。
    static constexpr std::size_t max_object_size = boost::json::object::max_size();
    static constexpr std::size_t max_array_size = boost::json::array::max_size();
    static constexpr std::size_t max_key_size = boost::json::string::max_size();
    static constexpr std::size_t max_string_size = boost::json::string::max_size();

    bool on_document_begin(boost::system::error_code&) { return true; }
    bool on_document_end(boost::system::error_code&) { return true; }

    bool on_object_begin(boost::system::error_code&) {
        frames_.emplace_back(true);
        return true;
    }
    bool on_object_end(std::size_t n, boost::system::error_code&) {
        if (!frames_.empty()) frames_.pop_back();
        stack_.push_object(n);
        return true;
    }
    bool on_array_begin(boost::system::error_code&) {
        frames_.emplace_back(false);
        return true;
    }
    bool on_array_end(std::size_t n, boost::system::error_code&) {
        if (!frames_.empty()) frames_.pop_back();
        stack_.push_array(n);
        return true;
    }

    bool on_key_part(boost::json::string_view s, std::size_t, boost::system::error_code&) {
        key_.append(s.data(), s.size());
        stack_.push_chars(s);
        return true;
    }
    bool on_key(boost::json::string_view s, std::size_t, boost::system::error_code& ec) {
        key_.append(s.data(), s.size());
        if (!frames_.empty() && frames_.back().is_object) {
            if (!frames_.back().keys.insert(key_).second) {
                duplicate_key_ = true;
                ec = boost::json::error::syntax;
                return false;
            }
        }
        stack_.push_key(s);
        key_.clear();
        return true;
    }

    bool on_string_part(boost::json::string_view s, std::size_t, boost::system::error_code&) {
        stack_.push_chars(s);
        return true;
    }
    bool on_string(boost::json::string_view s, std::size_t, boost::system::error_code&) {
        stack_.push_string(s);
        return true;
    }

    bool on_number_part(boost::json::string_view, boost::system::error_code&) { return true; }

    // 注释扩展默认关闭（allow_comments=false），此回调不会被触发；为满足
    // Boost 的 handler 契约（编译期必须存在）而与 detail::handler 同构保留。
    bool on_comment_part(boost::json::string_view, boost::system::error_code&) { return true; }
    bool on_comment(boost::json::string_view, boost::system::error_code&) { return true; }

    bool on_int64(std::int64_t i, boost::json::string_view, boost::system::error_code&) {
        stack_.push_int64(i);
        return true;
    }
    bool on_uint64(std::uint64_t u, boost::json::string_view, boost::system::error_code&) {
        stack_.push_uint64(u);
        return true;
    }
    // 非有限（NaN/±Inf 字面量、1e309/1e400）与超范围整数都不可表示，拒绝。
    // 纯整数词法走到 on_double，说明它已超出 int64/uint64（能装下的整数走
    // on_int64/on_uint64）——不得静默降级为 double。raw 是 Boost 解析器交付的
    // 原始数字词法（单次 write_some 下即完整字面量），本处只做形态判断，
    // 不手写 JSON 词法扫描。
    bool on_double(double d, boost::json::string_view raw, boost::system::error_code& ec) {
        if (!std::isfinite(d) || IsIntegerLexeme(raw)) {
            number_error_ = true;
            ec = boost::json::error::syntax;
            return false;
        }
        stack_.push_double(d);
        return true;
    }
    bool on_bool(bool b, boost::system::error_code&) {
        stack_.push_bool(b);
        return true;
    }
    bool on_null(boost::system::error_code&) {
        stack_.push_null();
        return true;
    }

    bool duplicate_key() const noexcept { return duplicate_key_; }
    bool number_error() const noexcept { return number_error_; }
    boost::json::value Release() noexcept { return stack_.release(); }

private:
    struct Frame {
        explicit Frame(bool object) : is_object(object) {}
        bool is_object;
        std::set<std::string> keys;
    };

    boost::json::value_stack stack_;
    std::vector<Frame> frames_;
    std::string key_;
    bool duplicate_key_ = false;
    bool number_error_ = false;
};

// 与 std::uint64_t 精确表示上界：2^53（double 尾数位宽）。
constexpr std::uint64_t kExactDoubleLimit = 1ULL << 53;

} // namespace

// 实现细节：类内嵌套（namespace 深度 ≤ 3），定义在本 TU。
struct JsonValue::Impl {
    explicit Impl(boost::json::value v) : value(std::move(v)) {}
    boost::json::value value;
};

JsonValue::JsonValue(std::unique_ptr<Impl> impl) noexcept
    : m_impl(std::move(impl)) {}

JsonValue::JsonValue() noexcept = default;
JsonValue::JsonValue(std::nullptr_t) noexcept {}
JsonValue::JsonValue(JsonValue&& other) noexcept = default;
JsonValue& JsonValue::operator=(JsonValue&& other) noexcept = default;
JsonValue::~JsonValue() = default;

JsonValue::JsonValue(bool value) : m_impl(std::make_unique<Impl>(boost::json::value(value))) {}
JsonValue::JsonValue(int value) : m_impl(std::make_unique<Impl>(boost::json::value(std::int64_t(value)))) {}
JsonValue::JsonValue(std::int64_t value) : m_impl(std::make_unique<Impl>(boost::json::value(value))) {}
JsonValue::JsonValue(std::uint64_t value) : m_impl(std::make_unique<Impl>(boost::json::value(value))) {}

JsonValue::JsonValue(double value) {
    if (!std::isfinite(value))
        throw std::invalid_argument("bbt::infra::mcp::JsonValue: double must be finite");
    m_impl = std::make_unique<Impl>(boost::json::value(value));
}

JsonValue::JsonValue(std::string_view value) {
    if (!bbt::infra::detail::IsValidUtf8(value))
        throw std::invalid_argument("bbt::infra::mcp::JsonValue: string must be valid UTF-8");
    m_impl = std::make_unique<Impl>(boost::json::value(boost::json::string(value)));
}

JsonValue::JsonValue(const char* value)
    : JsonValue(value == nullptr ? std::string_view() : std::string_view(value)) {}

JsonValue::JsonValue(const JsonValue& other) {
    if (other.m_impl) m_impl = std::make_unique<Impl>(other.m_impl->value);
}

JsonValue& JsonValue::operator=(const JsonValue& other) {
    if (this == &other) return *this;
    // 深复制：先构造新值再替换，失败时本对象保持原值（基本保证）。
    std::unique_ptr<Impl> copy;
    if (other.m_impl) copy = std::make_unique<Impl>(other.m_impl->value);
    m_impl = std::move(copy);
    return *this;
}

JsonValue JsonValue::Object() { return JsonValue(std::make_unique<Impl>(boost::json::value(boost::json::object()))); }
JsonValue JsonValue::Array()  { return JsonValue(std::make_unique<Impl>(boost::json::value(boost::json::array()))); }

result<JsonValue> JsonValue::Parse(std::string_view text, std::size_t max_bytes,
                                   std::size_t max_depth) {
    if (max_bytes == 0 || max_depth == 0)
        return JsonErr<JsonValue>(ErrorCode::InvalidArgument, "mcp.json.bound",
                                  "json parse bound must be positive");
    if (text.size() > max_bytes)
        return JsonErr<JsonValue>(ErrorCode::ProtocolError, "mcp.json.max_bytes",
                                  "json input exceeds max bytes");

    boost::json::parse_options options;
    options.max_depth = max_depth;
    options.numbers = boost::json::number_precision::precise;
    // allow_invalid_utf8 / allow_infinity_and_nan / comments 等非标准扩展保持默认关闭。

    boost::json::basic_parser<StrictDomHandler> parser(options);
    boost::system::error_code ec;
    // more=false：本次调用即整帧末尾，因此解析器不会在等更多输入的状态下返回。
    const std::size_t parsed_bytes = parser.write_some(false, text.data(), text.size(), ec);

    if (parser.handler().duplicate_key())
        return JsonErr<JsonValue>(ErrorCode::ProtocolError, "mcp.json.duplicate_key",
                                  "json object has duplicate key");
    // 不可表示数值：非有限或超范围整数（on_double 已拒绝，不留 DOM）。
    if (parser.handler().number_error())
        return JsonErr<JsonValue>(ErrorCode::ProtocolError, "mcp.json.number",
                                  "json number is not representable");
    if (ec) {
        if (ec == boost::json::error::too_deep)
            return JsonErr<JsonValue>(ErrorCode::ProtocolError, "mcp.json.max_depth",
                                      "json input exceeds max depth");
        return JsonErr<JsonValue>(ErrorCode::ProtocolError, "mcp.json.syntax",
                                  "json input is not a single valid document");
    }
    // 未吃完全部字节 = 尾随数据；done() 为假 = 不完整。两者都不是「单一完整文档」。
    if (parsed_bytes != text.size() || !parser.done())
        return JsonErr<JsonValue>(ErrorCode::ProtocolError, "mcp.json.syntax",
                                  "json input is not a single valid document");

    return result<JsonValue>::ok(JsonValue(std::make_unique<Impl>(parser.handler().Release())));
}

JsonKind JsonValue::Kind() const noexcept {
    if (!m_impl) return JsonKind::Null;
    switch (m_impl->value.kind()) {
        case boost::json::kind::bool_:   return JsonKind::Bool;
        case boost::json::kind::int64:   return JsonKind::Int64;
        case boost::json::kind::uint64:  return JsonKind::UInt64;
        case boost::json::kind::double_: return JsonKind::Double;
        case boost::json::kind::string:  return JsonKind::String;
        case boost::json::kind::array:   return JsonKind::Array;
        case boost::json::kind::object:  return JsonKind::Object;
        case boost::json::kind::null:    return JsonKind::Null;
    }
    return JsonKind::Null;
}

bool JsonValue::IsNull() const noexcept   { return Kind() == JsonKind::Null; }
bool JsonValue::IsBool() const noexcept   { return Kind() == JsonKind::Bool; }
bool JsonValue::IsInt64() const noexcept  { return Kind() == JsonKind::Int64; }
bool JsonValue::IsUInt64() const noexcept { return Kind() == JsonKind::UInt64; }
bool JsonValue::IsDouble() const noexcept { return Kind() == JsonKind::Double; }
bool JsonValue::IsString() const noexcept { return Kind() == JsonKind::String; }
bool JsonValue::IsArray() const noexcept  { return Kind() == JsonKind::Array; }
bool JsonValue::IsObject() const noexcept { return Kind() == JsonKind::Object; }

std::size_t JsonValue::Size() const noexcept {
    if (!m_impl) return 0;
    if (m_impl->value.is_object()) return m_impl->value.as_object().size();
    if (m_impl->value.is_array())  return m_impl->value.as_array().size();
    return 0;
}

result<JsonValue> JsonValue::Find(std::string_view key) const {
    if (!m_impl || !m_impl->value.is_object())
        return JsonErr<JsonValue>(ErrorCode::InvalidArgument, "mcp.json.not_object",
                                  "json value is not an object");
    const boost::json::object& object = m_impl->value.as_object();
    const auto it = object.find(key);
    if (it == object.end())
        return JsonErr<JsonValue>(ErrorCode::NotFound, "mcp.json.key_missing",
                                  "json object key not found");
    return result<JsonValue>::ok(JsonValue(std::make_unique<Impl>(it->value())));
}

result<JsonValue> JsonValue::At(std::size_t index) const {
    if (!m_impl || !m_impl->value.is_array())
        return JsonErr<JsonValue>(ErrorCode::InvalidArgument, "mcp.json.not_array",
                                  "json value is not an array");
    const boost::json::array& array = m_impl->value.as_array();
    if (index >= array.size())
        return JsonErr<JsonValue>(ErrorCode::NotFound, "mcp.json.index_out_of_range",
                                  "json array index out of range");
    return result<JsonValue>::ok(JsonValue(std::make_unique<Impl>(array[index])));
}

result<std::vector<std::string>> JsonValue::Keys() const {
    if (!m_impl || !m_impl->value.is_object())
        return JsonErr<std::vector<std::string>>(ErrorCode::InvalidArgument, "mcp.json.not_object",
                                                 "json value is not an object");
    // 直接复用 Boost object 的原生迭代（保持插入顺序），只做 owned string 拷贝。
    std::vector<std::string> keys;
    const boost::json::object& object = m_impl->value.as_object();
    keys.reserve(object.size());
    for (const auto& member : object)
        keys.emplace_back(member.key().data(), member.key().size());
    return result<std::vector<std::string>>::ok(std::move(keys));
}

result<void> JsonValue::Set(std::string_view key, JsonValue value) {
    if (!m_impl || !m_impl->value.is_object())
        return JsonErr<void>(ErrorCode::InvalidArgument, "mcp.json.not_object",
                             "json value is not an object");
    if (!bbt::infra::detail::IsValidUtf8(key))
        return JsonErr<void>(ErrorCode::InvalidArgument, "mcp.json.utf8",
                             "json object key must be valid UTF-8");
    // 校验全部通过后才写入：不存在半份修改。
    m_impl->value.as_object()[key] =
        value.m_impl ? std::move(value.m_impl->value) : boost::json::value();
    return result<void>::ok();
}

result<void> JsonValue::Push(JsonValue value) {
    if (!m_impl || !m_impl->value.is_array())
        return JsonErr<void>(ErrorCode::InvalidArgument, "mcp.json.not_array",
                             "json value is not an array");
    m_impl->value.as_array().push_back(value.m_impl ? std::move(value.m_impl->value)
                                                    : boost::json::value());
    return result<void>::ok();
}

result<bool> JsonValue::AsBool() const {
    if (!m_impl || !m_impl->value.is_bool())
        return JsonErr<bool>(ErrorCode::TypeMismatch, "mcp.json.type",
                             "json value type mismatch");
    return result<bool>::ok(m_impl->value.as_bool());
}

result<std::int64_t> JsonValue::AsInt64() const {
    if (!m_impl) return JsonErr<std::int64_t>(ErrorCode::TypeMismatch, "mcp.json.type",
                                              "json value type mismatch");
    if (m_impl->value.is_int64()) return result<std::int64_t>::ok(m_impl->value.as_int64());
    if (m_impl->value.is_uint64()) {
        const std::uint64_t u = m_impl->value.as_uint64();
        if (u <= static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()))
            return result<std::int64_t>::ok(static_cast<std::int64_t>(u));
    }
    return JsonErr<std::int64_t>(ErrorCode::TypeMismatch, "mcp.json.type",
                                 "json value type mismatch");
}

result<std::uint64_t> JsonValue::AsUInt64() const {
    if (!m_impl) return JsonErr<std::uint64_t>(ErrorCode::TypeMismatch, "mcp.json.type",
                                               "json value type mismatch");
    if (m_impl->value.is_uint64()) return result<std::uint64_t>::ok(m_impl->value.as_uint64());
    if (m_impl->value.is_int64()) {
        const std::int64_t i = m_impl->value.as_int64();
        if (i >= 0) return result<std::uint64_t>::ok(static_cast<std::uint64_t>(i));
    }
    return JsonErr<std::uint64_t>(ErrorCode::TypeMismatch, "mcp.json.type",
                                  "json value type mismatch");
}

result<double> JsonValue::AsDouble() const {
    if (!m_impl) return JsonErr<double>(ErrorCode::TypeMismatch, "mcp.json.type",
                                        "json value type mismatch");
    if (m_impl->value.is_double()) return result<double>::ok(m_impl->value.as_double());
    if (m_impl->value.is_int64()) {
        const std::int64_t i = m_impl->value.as_int64();
        if (i >= -static_cast<std::int64_t>(kExactDoubleLimit) &&
            i <= static_cast<std::int64_t>(kExactDoubleLimit))
            return result<double>::ok(static_cast<double>(i));
    } else if (m_impl->value.is_uint64()) {
        const std::uint64_t u = m_impl->value.as_uint64();
        if (u <= kExactDoubleLimit) return result<double>::ok(static_cast<double>(u));
    }
    return JsonErr<double>(ErrorCode::TypeMismatch, "mcp.json.precision",
                           "json integer not exactly representable as double");
}

result<std::string> JsonValue::AsString() const {
    if (!m_impl || !m_impl->value.is_string())
        return JsonErr<std::string>(ErrorCode::TypeMismatch, "mcp.json.type",
                                    "json value type mismatch");
    const boost::json::string& s = m_impl->value.as_string();
    return result<std::string>::ok(std::string(s.data(), s.size()));
}

std::string JsonValue::Serialize() const {
    if (!m_impl) return "null";
    return boost::json::serialize(m_impl->value);
}

} // namespace bbt::infra::mcp
