// MCP JsonValue（task 2-1）契约测试：值语义/精确整数/拒绝面/边界。
//
// 无第三方测试框架依赖；只链接 bbt::infra_mcp_core（公共头 + 实现 TU）。
// 失败输出到 stderr 并以非零退出；末尾打印 "mcp.json: N checks, M failures"。

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>

namespace {

int g_checks = 0;
int g_failures = 0;

void Check(bool ok, const char* expr, int line) {
    ++g_checks;
    if (!ok) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d  %s\n", __FILE__, line, expr);
    }
}

// static_cast<bool>：result<T>::operator bool 是 explicit，不能直接作为函数实参；
// 包一层显式转换后 CHECK(r)、CHECK(!r) 都可直接用。
#define CHECK(expr) Check(static_cast<bool>(expr), #expr, __LINE__)

using bbt::infra::ErrorCode;
using bbt::infra::mcp::JsonKind;
using bbt::infra::mcp::JsonValue;

// 解析成功断言（返回解析结果供继续检查）。
bool ParsedOk(bbt::infra::result<JsonValue>& parsed, int line) {
    ++g_checks;
    if (!parsed) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d  parse should succeed: %s\n", __FILE__, line,
                     parsed.error().message.c_str());
        return false;
    }
    return true;
}

// 解析被拒断言 + domain_code 精确匹配（固定文本，不回显输入）。
void RejectedWith(bbt::infra::result<JsonValue>& parsed, const char* domain_code, int line) {
    ++g_checks;
    if (parsed) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d  parse should be rejected (%s)\n", __FILE__, line,
                     domain_code);
        return;
    }
    ++g_checks;
    if (parsed.error().domain_code != domain_code) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d  domain_code=%s expected=%s\n", __FILE__, line,
                     parsed.error().domain_code.c_str(), domain_code);
    }
}

constexpr std::size_t kBig = 1U << 20;
constexpr std::size_t kDeep = 32;

void TestDefaultsAndFactories() {
    JsonValue null_value;
    CHECK(null_value.IsNull());
    CHECK(null_value.Kind() == JsonKind::Null);
    CHECK(null_value.Size() == 0);
    CHECK(null_value.Serialize() == "null");
    CHECK(!null_value.IsObject());

    JsonValue object = JsonValue::Object();
    CHECK(object.IsObject());
    CHECK(object.Size() == 0);
    CHECK(object.Serialize() == "{}");

    JsonValue array = JsonValue::Array();
    CHECK(array.IsArray());
    CHECK(array.Size() == 0);
    CHECK(array.Serialize() == "[]");

    // int 字面量不得被静默解释成 null。
    CHECK(JsonValue(0).IsInt64());
    CHECK(JsonValue(0).AsInt64().value() == 0);
}

void TestScalarsAndTypeErrors() {
    CHECK(JsonValue(true).IsBool());
    CHECK(JsonValue(true).AsBool().value());
    CHECK(JsonValue(std::int64_t(-7)).IsInt64());
    CHECK(JsonValue(std::int64_t(-7)).AsInt64().value() == -7);
    CHECK(JsonValue(std::uint64_t(7)).IsUInt64());
    CHECK(JsonValue(1.5).IsDouble());
    CHECK(JsonValue(1.5).AsDouble().value() == 1.5);
    CHECK(JsonValue("hi").IsString());
    CHECK(JsonValue("hi").AsString().value() == "hi");
    CHECK(JsonValue("hi").Serialize() == "\"hi\"");

    // 类型不匹配一律 TypeMismatch，不做隐式转换。
    CHECK(!JsonValue(true).AsInt64());
    CHECK(!JsonValue(true).AsInt64() &&
          JsonValue(true).AsInt64().error().code == ErrorCode::TypeMismatch);
    CHECK(!JsonValue(std::int64_t(1)).AsString());
    CHECK(!JsonValue(std::int64_t(1)).AsBool());
    CHECK(!JsonValue("1").AsUInt64());
    CHECK(!JsonValue().AsString());
    // 负整数不能变成无符号（不静默回绕）。
    CHECK(!JsonValue(std::int64_t(-1)).AsUInt64());

    // 非有限 double 拒绝（JSON 无法表达），不静默写入。
    bool threw_nan = false;
    try { JsonValue v(std::numeric_limits<double>::quiet_NaN()); (void)v; }
    catch (const std::invalid_argument&) { threw_nan = true; }
    CHECK(threw_nan);
    bool threw_inf = false;
    try { JsonValue v(std::numeric_limits<double>::infinity()); (void)v; }
    catch (const std::invalid_argument&) { threw_inf = true; }
    CHECK(threw_inf);

    // 非法 UTF-8 字符串构造拒绝。
    const std::string bad_utf8 = std::string("\"\xC3\x28\"");
    bool threw_utf8 = false;
    try { JsonValue v(std::string_view("\xFF\xFE")); (void)v; }
    catch (const std::invalid_argument&) { threw_utf8 = true; }
    CHECK(threw_utf8);
    (void)bad_utf8;
}

void TestExactIntegers() {
    // 2^53+1：double 无法精确表示，必须保持整数身份与原值。
    auto beyond = JsonValue::Parse("9007199254740993", kBig, kDeep);
    if (ParsedOk(beyond, __LINE__)) {
        JsonValue v = std::move(beyond).value();
        CHECK(v.IsInt64());
        CHECK(v.AsInt64().value() == 9007199254740993LL);
        CHECK(v.Serialize() == "9007199254740993");
        // 超 2^53 不转 double（不静默丢精度）。
        CHECK(!v.AsDouble());
        CHECK(!v.AsDouble() && v.AsDouble().error().domain_code == "mcp.json.precision");
    }

    auto u64max = JsonValue::Parse("18446744073709551615", kBig, kDeep);
    if (ParsedOk(u64max, __LINE__)) {
        JsonValue v = std::move(u64max).value();
        CHECK(v.IsUInt64());
        CHECK(v.AsUInt64().value() == std::numeric_limits<std::uint64_t>::max());
        CHECK(v.Serialize() == "18446744073709551615");
        CHECK(!v.AsInt64());  // 超出 int64 可表示范围
        CHECK(!v.AsDouble()); // 超出 2^53
    }

    auto i64min = JsonValue::Parse("-9223372036854775808", kBig, kDeep);
    if (ParsedOk(i64min, __LINE__)) {
        JsonValue v = std::move(i64min).value();
        CHECK(v.AsInt64().value() == std::numeric_limits<std::int64_t>::min());
        CHECK(v.Serialize() == "-9223372036854775808");
        CHECK(v.AsUInt64().error().code == ErrorCode::TypeMismatch);
    }

    // 2^53 以内可精确转 double；2^53 本身也可。
    auto exact = JsonValue::Parse("9007199254740992", kBig, kDeep);
    if (ParsedOk(exact, __LINE__)) {
        CHECK(std::move(exact).value().AsDouble().value() == 9007199254740992.0);
    }

    // 浮点用 precise 模式：值保持 double 且不丢尾数。
    auto dbl = JsonValue::Parse("0.1234567890123456789", kBig, kDeep);
    if (ParsedOk(dbl, __LINE__)) {
        JsonValue v = std::move(dbl).value();
        CHECK(v.IsDouble());
        CHECK(v.AsDouble().value() == 0.1234567890123456789);
    }

    // 整数 + 小数混合结构往返不失真。
    const std::string text =
        "{\"a\":9007199254740993,\"b\":18446744073709551615,"
        "\"c\":-9223372036854775808,\"d\":1.5,\"e\":true,\"f\":null,"
        "\"g\":\"\\u4e2d\",\"h\":[1,2,{\"i\":[]}]}";
    // 期望的重新序列化结果：\u4e2d 解码为 UTF-8「中」，非 ASCII 不再转义；
    // Boost.JSON 1.90 的 double 输出用科学计数法（1.5 → "1.5E0"，合法 JSON，
    // 回读仍得同一 double），整数不受影响（精确原值）。
    const std::string expected_serialized =
        "{\"a\":9007199254740993,\"b\":18446744073709551615,"
        "\"c\":-9223372036854775808,\"d\":1.5E0,\"e\":true,\"f\":null,"
        "\"g\":\"\u4e2d\",\"h\":[1,2,{\"i\":[]}]}";
    auto nested = JsonValue::Parse(text, kBig, kDeep);
    if (ParsedOk(nested, __LINE__)) {
        JsonValue v = std::move(nested).value();
        CHECK(v.IsObject());
        CHECK(v.Serialize() == expected_serialized);
        // 幂等：重新解析序列化结果得到同一文档；double 回读值不变。
        auto again = JsonValue::Parse(v.Serialize(), kBig, kDeep);
        if (ParsedOk(again, __LINE__)) {
            JsonValue round_trip = std::move(again).value();
            CHECK(round_trip.Serialize() == expected_serialized);
            CHECK(round_trip.Find("d").value().AsDouble().value() == 1.5);
        }
        CHECK(v.Find("a").value().AsInt64().value() == 9007199254740993LL);
        CHECK(v.Find("b").value().AsUInt64().value() ==
              std::numeric_limits<std::uint64_t>::max());
        CHECK(v.Find("g").value().AsString().value() == "中");
        CHECK(v.Find("h").value().At(2).value().Find("i").value().IsArray());
    }
}

void TestRejections() {
    auto dup = JsonValue::Parse("{\"a\":1,\"a\":2}", kBig, kDeep);
    RejectedWith(dup, "mcp.json.duplicate_key", __LINE__);

    auto dup_nested = JsonValue::Parse("{\"a\":{\"b\":1,\"b\":2}}", kBig, kDeep);
    RejectedWith(dup_nested, "mcp.json.duplicate_key", __LINE__);

    auto dup_mixed = JsonValue::Parse("{\"a\":{\"b\":1},\"a\":2}", kBig, kDeep);
    RejectedWith(dup_mixed, "mcp.json.duplicate_key", __LINE__);

    // 数组内不同对象各有同名键不是重复键：不得误拒（假阳性回归）。
    auto array_same_key = JsonValue::Parse("[{\"k\":1},{\"k\":2}]", kBig, kDeep);
    if (ParsedOk(array_same_key, __LINE__)) {
        CHECK(std::move(array_same_key).value().Serialize() == "[{\"k\":1},{\"k\":2}]");
    }
    // 同层与跨层的真重复必须拒。
    auto dup_in_array = JsonValue::Parse("[{\"k\":1,\"k\":2}]", kBig, kDeep);
    RejectedWith(dup_in_array, "mcp.json.duplicate_key", __LINE__);

    // 不同对象里的同名键不是重复（不得误拒）。
    auto distinct = JsonValue::Parse("{\"x\":{\"k\":1},\"y\":{\"k\":2}}", kBig, kDeep);
    if (ParsedOk(distinct, __LINE__)) {
        CHECK(std::move(distinct).value().Serialize() == "{\"x\":{\"k\":1},\"y\":{\"k\":2}}");
    }

    // 非法 UTF-8（字符串字面量与裸字节）都被拒。
    auto bad_string = JsonValue::Parse("{\"a\":\"\xFF\xFE\"}", kBig, kDeep);
    RejectedWith(bad_string, "mcp.json.syntax", __LINE__);
    auto lone_surrogate = JsonValue::Parse("{\"a\":\"\\ud800\"}", kBig, kDeep);
    RejectedWith(lone_surrogate, "mcp.json.syntax", __LINE__);

    // 非标准数值字面量默认关闭。
    auto nan = JsonValue::Parse("{\"a\":NaN}", kBig, kDeep);
    RejectedWith(nan, "mcp.json.syntax", __LINE__);
    auto inf = JsonValue::Parse("{\"a\":Infinity}", kBig, kDeep);
    RejectedWith(inf, "mcp.json.syntax", __LINE__);

    // 尾随数据 / 截断 / 空输入都不是「单一完整文档」。
    auto trailing = JsonValue::Parse("{}{}", kBig, kDeep);
    RejectedWith(trailing, "mcp.json.syntax", __LINE__);
    auto trailing_text = JsonValue::Parse("{} x", kBig, kDeep);
    RejectedWith(trailing_text, "mcp.json.syntax", __LINE__);
    auto truncated = JsonValue::Parse("{\"a\":", kBig, kDeep);
    RejectedWith(truncated, "mcp.json.syntax", __LINE__);
    auto empty = JsonValue::Parse("", kBig, kDeep);
    RejectedWith(empty, "mcp.json.syntax", __LINE__);
    auto bare_word = JsonValue::Parse("oops", kBig, kDeep);
    RejectedWith(bare_word, "mcp.json.syntax", __LINE__);

    // 字节边界。
    auto too_big = JsonValue::Parse("{\"a\":1}", 4, kDeep);
    RejectedWith(too_big, "mcp.json.max_bytes", __LINE__);
    auto zero_bytes = JsonValue::Parse("{}", 0, kDeep);
    RejectedWith(zero_bytes, "mcp.json.bound", __LINE__);
    auto zero_depth = JsonValue::Parse("{}", kBig, 0);
    RejectedWith(zero_depth, "mcp.json.bound", __LINE__);
    auto at_limit = JsonValue::Parse("{\"a\":1}", 7, kDeep);
    CHECK(at_limit);

    // 深度边界：[[[[[1]]]]] 深度 5。
    auto deep_ok = JsonValue::Parse("[[[[[1]]]]]", kBig, 5);
    CHECK(deep_ok);
    auto deep_bad = JsonValue::Parse("[[[[[1]]]]]", kBig, 4);
    RejectedWith(deep_bad, "mcp.json.max_depth", __LINE__);
    auto deep_object = JsonValue::Parse("{\"a\":{\"b\":{\"c\":1}}}", kBig, 2);
    RejectedWith(deep_object, "mcp.json.max_depth", __LINE__);
}

void TestNumberRepresentability() {
    // 非有限字面量：不可表示，固定 domain mcp.json.number（不是 syntax/duplicate），
    // 且不留 DOM（Parse 直接失败，调用方拿不到值）。
    auto inf_309 = JsonValue::Parse("1e309", kBig, kDeep);
    RejectedWith(inf_309, "mcp.json.number", __LINE__);
    auto inf_400 = JsonValue::Parse("1e400", kBig, kDeep);
    RejectedWith(inf_400, "mcp.json.number", __LINE__);
    auto neg_inf = JsonValue::Parse("-1e309", kBig, kDeep);
    RejectedWith(neg_inf, "mcp.json.number", __LINE__);
    auto huge_exp = JsonValue::Parse("1e99999", kBig, kDeep);
    RejectedWith(huge_exp, "mcp.json.number", __LINE__);

    // 嵌套 params / 数组里的非有限同样拒绝（不是只看顶层标量）。
    auto nested = JsonValue::Parse("{\"params\":{\"x\":1e400}}", kBig, kDeep);
    RejectedWith(nested, "mcp.json.number", __LINE__);
    auto in_array = JsonValue::Parse("[1,2,1e400]", kBig, kDeep);
    RejectedWith(in_array, "mcp.json.number", __LINE__);
    auto deep = JsonValue::Parse("{\"a\":{\"b\":{\"c\":-1e400}}}", kBig, kDeep);
    RejectedWith(deep, "mcp.json.number", __LINE__);

    // 超 int64/uint64 的整数词法不得悄悄降级为 double（保精确整数承诺）。
    auto over_u64 = JsonValue::Parse("18446744073709551616", kBig, kDeep);   // 2^64
    RejectedWith(over_u64, "mcp.json.number", __LINE__);
    auto under_i64 = JsonValue::Parse("-9223372036854775809", kBig, kDeep);  // int64 min - 1
    RejectedWith(under_i64, "mcp.json.number", __LINE__);
    auto big_int = JsonValue::Parse("123456789012345678901234567890", kBig, kDeep);
    RejectedWith(big_int, "mcp.json.number", __LINE__);
    auto nested_big = JsonValue::Parse("{\"a\":[{\"b\":99999999999999999999}]}", kBig, kDeep);
    RejectedWith(nested_big, "mcp.json.number", __LINE__);

    // 正常有限边界仍接受：1e308（最大附近有限 double）。
    auto max_finite = JsonValue::Parse("1e308", kBig, kDeep);
    if (ParsedOk(max_finite, __LINE__)) {
        JsonValue v = std::move(max_finite).value();
        CHECK(v.IsDouble());
        CHECK(std::isfinite(v.AsDouble().value()));
        CHECK(v.AsDouble().value() == 1e308);
    }
    // 整数边界未被误拒：uint64 上界与 int64 上界仍精确接受。
    auto u64max = JsonValue::Parse("18446744073709551615", kBig, kDeep);
    if (ParsedOk(u64max, __LINE__)) {
        CHECK(std::move(u64max).value().AsUInt64().value() ==
              std::numeric_limits<std::uint64_t>::max());
    }
    auto i64max = JsonValue::Parse("9223372036854775807", kBig, kDeep);
    if (ParsedOk(i64max, __LINE__)) {
        CHECK(std::move(i64max).value().AsInt64().value() ==
              std::numeric_limits<std::int64_t>::max());
    }
    auto i64min = JsonValue::Parse("-9223372036854775808", kBig, kDeep);
    if (ParsedOk(i64min, __LINE__)) {
        CHECK(std::move(i64min).value().AsInt64().value() ==
              std::numeric_limits<std::int64_t>::min());
    }

    // 下溢到 0 属 IEEE 语义、非拒绝面：1e-999 → 0（显式记录，非静默丢精度）。
    auto underflow = JsonValue::Parse("1e-999", kBig, kDeep);
    if (ParsedOk(underflow, __LINE__)) {
        JsonValue v = std::move(underflow).value();
        CHECK(v.IsDouble());
        CHECK(v.AsDouble().value() == 0.0);
    }
    // 有限小数带指数按 IEEE 最近舍入，正常范围无损。
    auto small = JsonValue::Parse("1.5e-3", kBig, kDeep);
    if (ParsedOk(small, __LINE__)) {
        CHECK(std::move(small).value().AsDouble().value() == 1.5e-3);
    }

    // 只被接受有限值：Serialize 结果必可再 Parse 得同一有限值（往返）。
    auto finite = JsonValue::Parse("{\"a\":1.5,\"b\":-2.25,\"c\":1e308}", kBig, kDeep);
    if (ParsedOk(finite, __LINE__)) {
        JsonValue v = std::move(finite).value();
        const std::string wire = v.Serialize();
        auto again = JsonValue::Parse(wire, kBig, kDeep);
        if (ParsedOk(again, __LINE__)) {
            JsonValue rt = std::move(again).value();
            CHECK(rt.Find("a").value().AsDouble().value() == 1.5);
            CHECK(rt.Find("b").value().AsDouble().value() == -2.25);
            CHECK(rt.Find("c").value().AsDouble().value() == 1e308);
            CHECK(std::isfinite(rt.Find("c").value().AsDouble().value()));
        }
    }
}

void TestObjectArrayAccessAndMutation() {
    auto parsed = JsonValue::Parse("{\"a\":{\"n\":1},\"b\":[10,20]}", kBig, kDeep);
    if (!ParsedOk(parsed, __LINE__)) return;
    JsonValue v = std::move(parsed).value();
    CHECK(v.Size() == 2);

    // Find/At 返回拥有型 copy：修改返回值不影响本源。
    auto found = v.Find("a");
    CHECK(found);
    JsonValue copy = std::move(found).value();
    CHECK(copy.Set("n", JsonValue(std::int64_t(99))));
    CHECK(v.Find("a").value().Find("n").value().AsInt64().value() == 1);
    CHECK(copy.Find("n").value().AsInt64().value() == 99);
    // 嵌套子值是拥有型 copy：Push 进 Find 的返回值不会改到本源，必须显式写回。
    auto nested_copy = v.Find("b").value();
    CHECK(nested_copy.Push(JsonValue(std::int64_t(30))));
    CHECK(nested_copy.Size() == 3);
    CHECK(v.Find("b").value().Size() == 2);
    CHECK(v.Set("b", std::move(nested_copy)));
    CHECK(v.Find("b").value().Size() == 3);

    auto element = v.Find("b").value().At(1);
    CHECK(element && element.value().AsInt64().value() == 20);
    auto oob = v.Find("b").value().At(3);  // 上一步写回后 b=[10,20,30]
    CHECK(!oob && oob.error().domain_code == "mcp.json.index_out_of_range");
    auto at_object = v.At(0);
    CHECK(!at_object && at_object.error().domain_code == "mcp.json.not_array");
    auto missing = v.Find("zzz");
    CHECK(!missing && missing.error().code == ErrorCode::NotFound);
    CHECK(!missing && missing.error().domain_code == "mcp.json.key_missing");
    auto find_array = v.Find("b").value().Find("a");
    CHECK(!find_array && find_array.error().domain_code == "mcp.json.not_object");

    // Set/Push 目标类型不对：失败且不修改本对象（不存在半份修改）。
    JsonValue array = JsonValue::Array();
    auto set_on_array = array.Set("k", JsonValue(std::int64_t(1)));
    CHECK(!set_on_array && set_on_array.error().domain_code == "mcp.json.not_object");
    CHECK(array.Size() == 0 && array.IsArray());

    JsonValue object = JsonValue::Object();
    auto push_on_object = object.Push(JsonValue(std::int64_t(1)));
    CHECK(!push_on_object && push_on_object.error().domain_code == "mcp.json.not_array");
    CHECK(object.Size() == 0 && object.IsObject());

    // 非法 UTF-8 键被拒且不写入。
    auto bad_key = object.Set(std::string_view("\xFF"), JsonValue(std::int64_t(1)));
    CHECK(!bad_key && bad_key.error().domain_code == "mcp.json.utf8");
    CHECK(object.Size() == 0);

    // 合法写入按序生效。
    CHECK(object.Set("k", JsonValue("v")));
    CHECK(object.Set("n", JsonValue(std::int64_t(3))));
    CHECK(object.Size() == 2);
    CHECK(object.Find("k").value().AsString().value() == "v");
    CHECK(object.Serialize() == "{\"k\":\"v\",\"n\":3}");
    CHECK(object.Set("k", JsonValue("w")));  // 覆盖同键
    CHECK(object.Size() == 2);
    CHECK(object.Serialize() == "{\"k\":\"w\",\"n\":3}");

    CHECK(array.Push(JsonValue(std::int64_t(1))));
    CHECK(array.Push(JsonValue("two")));
    CHECK(array.Size() == 2);
    CHECK(array.Serialize() == "[1,\"two\"]");
}

void TestCopyAndMoveSemantics() {
    auto parsed = JsonValue::Parse("{\"a\":[1,2],\"b\":\"x\"}", kBig, kDeep);
    if (!ParsedOk(parsed, __LINE__)) return;
    JsonValue original = std::move(parsed).value();

    // copy 深复制：改副本不动原值，改原值不动副本。
    JsonValue copy = original;
    CHECK(copy.Serialize() == original.Serialize());
    auto copy_inner = copy.Find("a").value();
    CHECK(copy_inner.Push(JsonValue(std::int64_t(3))));
    CHECK(copy_inner.Size() == 3);
    CHECK(copy.Set("a", std::move(copy_inner)));
    CHECK(copy.Find("a").value().Size() == 3);
    CHECK(original.Find("a").value().Size() == 2);
    CHECK(original.Set("c", JsonValue(std::int64_t(9))));
    CHECK(copy.Find("c").error().code == ErrorCode::NotFound);

    // 拷贝赋值同样深复制。
    JsonValue assigned;
    assigned = original;
    CHECK(assigned.Serialize() == original.Serialize());
    CHECK(assigned.Set("d", JsonValue(true)));
    CHECK(original.Find("d").error().code == ErrorCode::NotFound);

    // move 后源为 null 且仍可用（可赋值、可析构、不崩）。
    JsonValue moved_from = original;
    JsonValue moved_to = std::move(moved_from);
    CHECK(moved_from.IsNull());
    CHECK(moved_from.Serialize() == "null");
    CHECK(moved_from.Kind() == JsonKind::Null);
    CHECK(moved_from.Size() == 0);
    CHECK(!moved_from.Find("a"));
    CHECK(!moved_from.Set("k", JsonValue(std::int64_t(1))));
    CHECK(!moved_from.Push(JsonValue(std::int64_t(1))));
    moved_from = JsonValue("reusable");
    CHECK(moved_from.AsString().value() == "reusable");
    CHECK(moved_to.Serialize() == "{\"a\":[1,2],\"b\":\"x\",\"c\":9}");

    // move 赋值后源同样为 null。
    JsonValue move_assign_target = JsonValue("old");
    move_assign_target = std::move(moved_to);
    CHECK(moved_to.IsNull());
    CHECK(move_assign_target.IsObject());
    CHECK(move_assign_target.Serialize() == "{\"a\":[1,2],\"b\":\"x\",\"c\":9}");
}

void TestUtf8Strings() {
    // 多字节 UTF-8 原样保留（不做转义/规范化）。
    JsonValue s = JsonValue("中\u00e9\U0001F600");
    CHECK(s.AsString().value() == "中\u00e9\U0001F600");
    auto parsed = JsonValue::Parse("\"\\u4e2d\\ud83d\\ude00\"", kBig, kDeep);
    if (ParsedOk(parsed, __LINE__)) {
        CHECK(std::move(parsed).value().AsString().value() == "中\U0001F600");
    }
    // 合法 4 字节序列不被误拒。
    auto four = JsonValue::Parse("\"\xf0\x9f\x98\x80\"", kBig, kDeep);
    CHECK(four);
}

// Keys()：object 键枚举（M2 依赖的最小 owned API）。空 object、Unicode/内嵌 NUL 键、
// 插入顺序、owned copy 语义、非 object（含 move 后 null）拒绝；不新增第二套 DOM。
void TestObjectKeys() {
    // 空 object → 空键列表。
    JsonValue empty = JsonValue::Object();
    auto empty_keys = empty.Keys();
    CHECK(empty_keys);
    if (empty_keys) CHECK(empty_keys.value().empty());

    // 插入顺序保持；Unicode（3/4 字节）与内嵌 NUL 键按字节原样返回。
    JsonValue object = JsonValue::Object();
    CHECK(object.Set("b", JsonValue(1)));
    CHECK(object.Set("键", JsonValue(2)));
    CHECK(object.Set(std::string_view("a\0b", 3), JsonValue(3)));
    CHECK(object.Set("\xF0\x9F\x98\x80", JsonValue(4)));
    auto keys = object.Keys();
    CHECK(keys);
    if (keys) {
        CHECK(keys.value().size() == 4);
        CHECK(keys.value()[0] == "b");
        CHECK(keys.value()[1] == "键");
        CHECK(keys.value()[2] == std::string("a\0b", 3));
        CHECK(keys.value()[3] == std::string("\xF0\x9F\x98\x80"));
    }

    // owned copy：返回值可独立修改；源对象后续变化不影响已取出的键列表。
    if (keys) {
        std::vector<std::string> mine = keys.value();
        mine.push_back("extra");
        CHECK(mine.size() == 5);
        CHECK(object.Set("c", JsonValue(5)));
        CHECK(mine.size() == 5);
        CHECK(mine[0] == "b");
        auto again = object.Keys();
        CHECK(again && again.value().size() == 5);
        CHECK(again && again.value()[4] == "c");
    }

    // 非 object：null / 标量 / array / move 之后的源对象 → mcp.json.not_object。
    JsonValue null_value;
    auto null_keys = null_value.Keys();
    CHECK(!null_keys);
    CHECK(null_keys.error().domain_code == "mcp.json.not_object");
    JsonValue scalar(true);
    CHECK(!scalar.Keys());
    CHECK(scalar.Keys().error().domain_code == "mcp.json.not_object");
    JsonValue array = JsonValue::Array();
    CHECK(!array.Keys());
    CHECK(array.Keys().error().domain_code == "mcp.json.not_object");
    JsonValue source = JsonValue::Object();
    CHECK(source.Set("k", JsonValue(1)));
    JsonValue moved = std::move(source);
    CHECK(source.IsNull());
    CHECK(source.Keys().error().domain_code == "mcp.json.not_object");
    CHECK(moved.Keys() && moved.Keys().value().size() == 1);

    // Parse 得到的 object 走同一实现（无第二套 parser/DOM）。
    auto parsed = JsonValue::Parse(R"({"x":1,"y":{"z":2}})", kBig, kDeep);
    if (ParsedOk(parsed, __LINE__)) {
        JsonValue value = std::move(parsed).value();
        auto parsed_keys = value.Keys();
        CHECK(parsed_keys);
        if (parsed_keys) {
            CHECK(parsed_keys.value().size() == 2);
            CHECK(parsed_keys.value()[0] == "x");
            CHECK(parsed_keys.value()[1] == "y");
        }
    }
}

} // namespace

int main() {
    TestDefaultsAndFactories();
    TestScalarsAndTypeErrors();
    TestExactIntegers();
    TestRejections();
    TestNumberRepresentability();
    TestObjectArrayAccessAndMutation();
    TestCopyAndMoveSemantics();
    TestUtf8Strings();
    TestObjectKeys();

    std::printf("mcp.json: %d checks, %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
