#include <bbt/infra/mongo/Bson.hpp>

#include <cctype>
#include <utility>

#include <bsoncxx/document/element.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/exception/exception.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <bsoncxx/validate.hpp>

namespace bbt::infra::mongo {

namespace {

Error InvalidArg(std::string message) {
    return MakeError(ErrorCode::InvalidArgument, std::move(message));
}

// 首个非空白字符是否 '{'：libbson 的 JSON 前端会把顶层数组解析成以数字为键
// 的文档（实测 `[1,2]` → {"0":1,"1":2}）。BSON 文档语义上没有这种形态，
// 这里显式拒绝，不把「数组字面量」静默变成文档。返回首个非空白位置，
// 全空白时返回 npos。
std::size_t FirstNonSpace(std::string_view json) {
    std::size_t i = 0;
    while (i < json.size() &&
           std::isspace(static_cast<unsigned char>(json[i])) != 0)
        ++i;
    return i == json.size() ? std::string_view::npos : i;
}

// 首个顶层对象的结束位置（对象右花括号的后一位）：只做结构范围定位——跳过
// 双引号字符串及其反斜杠转义、按层数配平花括号，不判断词法与类型。未闭合
// 返回 npos。
std::size_t ObjectEnd(std::string_view json, std::size_t begin) {
    int  depth     = 0;
    bool in_string = false;
    bool escaped   = false;
    for (std::size_t i = begin; i < json.size(); ++i) {
        const char c = json[i];
        if (in_string) {
            if (escaped)
                escaped = false;
            else if (c == '\\')
                escaped = true;
            else if (c == '"')
                in_string = false;
            continue;
        }
        if (c == '"') {
            in_string = true;
        } else if (c == '{') {
            ++depth;
        } else if (c == '}') {
            if (--depth == 0)
                return i + 1;
        }
    }
    return std::string_view::npos;
}

// bytes → 只读视图：空/不良构 → InvalidArgument（libbson 全结构校验，
// 与实现内部 MongoDetail::ViewOf 同一道门，driver 之前返回）。
result<bsoncxx::v_noabi::document::view> ViewOf(const MongoDocument& doc) {
    if (doc.bytes.empty())
        return result<bsoncxx::v_noabi::document::view>::err(
            InvalidArg("mongo: empty BSON document"));
    if (!bsoncxx::validate(doc.bytes.data(), doc.bytes.size()))
        return result<bsoncxx::v_noabi::document::view>::err(
            InvalidArg("mongo: malformed BSON document"));
    return result<bsoncxx::v_noabi::document::view>::ok(
        bsoncxx::v_noabi::document::view{doc.bytes.data(), doc.bytes.size()});
}

Error FieldNotFound(std::string_view field) {
    return MakeError(ErrorCode::NotFound,
                     "mongo: field not found: " + std::string(field));
}

Error FieldTypeMismatch(std::string_view field, const char* expected) {
    return MakeError(ErrorCode::TypeMismatch,
                     "mongo: field '" + std::string(field) +
                         "' is not " + expected);
}

// 顶层字段定位：bytes 非法 → InvalidArgument；缺失 → NotFound。
result<bsoncxx::v_noabi::document::element> FindField(
    const MongoDocument& doc, std::string_view field) {
    auto view = ViewOf(doc);
    if (!view)
        return result<bsoncxx::v_noabi::document::element>::err(
            std::move(view).error());
    const auto element = view.value()[field];
    if (!element)
        return result<bsoncxx::v_noabi::document::element>::err(
            FieldNotFound(field));
    return result<bsoncxx::v_noabi::document::element>::ok(element);
}

} // namespace

result<MongoDocument> MongoDocument::FromJson(std::string_view json) {
    const std::size_t first = FirstNonSpace(json);
    if (first == std::string_view::npos)
        return result<MongoDocument>::err(InvalidArg("mongo FromJson: empty JSON string"));
    if (json[first] != '{')
        return result<MongoDocument>::err(
            InvalidArg("mongo FromJson: top-level JSON value must be an object"));

    try {
        const auto parsed = bsoncxx::from_json(json);
        // 单文档校验：libbson 的 JSON 前端只消费第一份文档、静默忽略其后的
        // 内容（实测 `{"v":1}{"w":2}` 只剩前者），故解析成功后再核对整段输入
        // 确实只有一份顶层对象、其后仅允许空白。字符串内的括号与转义由
        // ObjectEnd 跳过，不参与配平。
        const std::size_t end = ObjectEnd(json, first);
        if (end == std::string_view::npos ||
            FirstNonSpace(json.substr(end)) != std::string_view::npos) {
            // 本组件的输入完整性检查（不是库报错），故不带 backend_* 元数据。
            return result<MongoDocument>::err(InvalidArg(
                "mongo FromJson: input must be exactly one JSON object"));
        }
        const auto view = parsed.view();
        MongoDocument out;
        out.bytes.assign(view.data(), view.data() + view.length());
        return result<MongoDocument>::ok(std::move(out));
    } catch (const bsoncxx::v_noabi::exception& e) {
        // 解析期失败全部是调用方输入问题（空/畸形/数字越界），
        // 沿用既有 BSON 域口径：InvalidArgument + backend_category="bsoncxx"。
        Error err = InvalidArg(std::string("mongo FromJson: ") + e.what());
        err.backend_category = "bsoncxx";
        err.backend_code     = e.code().value();
        return result<MongoDocument>::err(std::move(err));
    }
}

result<std::string> MongoDocument::ToJson(MongoJsonMode mode) const {
    auto view = ViewOf(*this);
    if (!view)
        return result<std::string>::err(std::move(view).error());
    try {
        return result<std::string>::ok(bsoncxx::to_json(
            view.value(),
            mode == MongoJsonMode::k_relaxed
                ? bsoncxx::ExtendedJsonMode::k_relaxed
                : bsoncxx::ExtendedJsonMode::k_canonical));
    } catch (const bsoncxx::v_noabi::exception& e) {
        Error err = MakeError(ErrorCode::InternalError,
                              std::string("mongo ToJson: ") + e.what());
        err.backend_category = "bsoncxx";
        err.backend_code     = e.code().value();
        return result<std::string>::err(std::move(err));
    }
}

bool MongoDocument::Has(std::string_view field) const {
    auto view = ViewOf(*this);
    if (!view)
        return false;
    return static_cast<bool>(view.value()[field]);
}

result<std::string> MongoDocument::GetString(std::string_view field) const {
    auto element = FindField(*this, field);
    if (!element)
        return result<std::string>::err(std::move(element).error());
    if (element.value().type() != bsoncxx::type::k_string)
        return result<std::string>::err(FieldTypeMismatch(field, "a string"));
    const auto value = element.value().get_string().value;
    return result<std::string>::ok(std::string(value.data(), value.size()));
}

result<std::int64_t> MongoDocument::GetInt64(std::string_view field) const {
    auto element = FindField(*this, field);
    if (!element)
        return result<std::int64_t>::err(std::move(element).error());
    // int32/int64 统一读出；其它类型（double/bool/string…）不隐式转换。
    switch (element.value().type()) {
    case bsoncxx::type::k_int32:
        return result<std::int64_t>::ok(element.value().get_int32().value);
    case bsoncxx::type::k_int64:
        return result<std::int64_t>::ok(element.value().get_int64().value);
    default:
        return result<std::int64_t>::err(
            FieldTypeMismatch(field, "an int32/int64"));
    }
}

result<double> MongoDocument::GetDouble(std::string_view field) const {
    auto element = FindField(*this, field);
    if (!element)
        return result<double>::err(std::move(element).error());
    if (element.value().type() != bsoncxx::type::k_double)
        return result<double>::err(FieldTypeMismatch(field, "a double"));
    return result<double>::ok(element.value().get_double().value);
}

result<bool> MongoDocument::GetBool(std::string_view field) const {
    auto element = FindField(*this, field);
    if (!element)
        return result<bool>::err(std::move(element).error());
    if (element.value().type() != bsoncxx::type::k_bool)
        return result<bool>::err(FieldTypeMismatch(field, "a bool"));
    return result<bool>::ok(element.value().get_bool().value);
}

} // namespace bbt::infra::mongo
