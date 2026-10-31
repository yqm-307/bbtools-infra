// MCP 协议基础（task 2-2 不依赖 Schema/transport 的部分）契约测试：
// JSON-RPC 输入完整性、owned 请求 id、modern `_meta`/版本校验、discover/error/result
// 构造。
//
// 无第三方测试框架依赖；经 ${PROJECT_SOURCE_DIR}/bbt 消费内部实现头（与仓内其他
// impl 层测试同口径，仅本目标可见）。

#include <cstdint>
#include <cstdio>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>
#include <bbt/infra/mcp/McpTypes.hpp>

#include "mcp/detail/Protocol.hpp"

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
using bbt::infra::mcp::JsonRpcErrorCode;
using bbt::infra::mcp::JsonRpcRequest;
using bbt::infra::mcp::JsonValue;
using bbt::infra::mcp::ProtocolFailure;
using bbt::infra::mcp::ProtocolLimits;
using bbt::infra::mcp::RequestId;

const ProtocolLimits kLimits{};
// 对外真实支持的 revision（discover supportedVersions / -32022 data.supported）。
// 2025-06-18 已按 owner 决策移除（spec crosswalk 未核对），不在列表中。
const std::vector<std::string> kVersions{
    std::string(bbt::infra::mcp::kProtocolVersion2026_07_28),
    std::string(bbt::infra::mcp::kProtocolVersion2025_11_25),
};
// modern 只接受唯一现代 revision。
const std::vector<std::string> kModernVersions{
    std::string(bbt::infra::mcp::kProtocolVersion2026_07_28),
};

// 规范命名空间键（golden 原始字串，独立于实现常量，避免同常量自证）。
constexpr const char* kMetaVersionKey = "io.modelcontextprotocol/protocolVersion";
constexpr const char* kMetaCapsKey = "io.modelcontextprotocol/clientCapabilities";

// 被拒帧：断言 wire 错误码（经 ToProtocolFailure 映射）。
void RejectedWithCode(bbt::infra::result<JsonRpcRequest>& parsed, JsonRpcErrorCode code,
                      int line) {
    ++g_checks;
    if (parsed) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d  frame should be rejected\n", __FILE__, line);
        return;
    }
    const ProtocolFailure failure = bbt::infra::mcp::ToProtocolFailure(parsed.error());
    ++g_checks;
    if (failure.code != code) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d  code=%d expected=%d (domain_code=%s)\n", __FILE__, line,
                     static_cast<int>(failure.code), static_cast<int>(code),
                     parsed.error().domain_code.c_str());
    }
}

void TestValidFrames() {
    auto string_id = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":\"req-1\",\"method\":\"tools/list\"}", kLimits);
    CHECK(string_id);
    if (string_id) {
        JsonRpcRequest request = std::move(string_id).value();
        CHECK(!request.notification);
        CHECK(request.id.IsString());
        CHECK(request.id.AsString() == "req-1");
        CHECK(request.method == "tools/list");
        CHECK(!request.params);
    }

    // 整数 id 保留精确值（>2^53 与 uint64 上界都不经 double）。
    auto big_id = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":9007199254740993,\"method\":\"ping\"}", kLimits);
    CHECK(big_id);
    if (big_id) {
        JsonRpcRequest request = std::move(big_id).value();
        CHECK(!request.id.IsString());
        CHECK(request.id.AsInt64() == 9007199254740993LL);
    }

    auto uint_id = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":18446744073709551615,\"method\":\"ping\"}", kLimits);
    CHECK(uint_id);
    if (uint_id) {
        JsonRpcRequest request = std::move(uint_id).value();
        CHECK(request.id.AsUInt64() == std::numeric_limits<std::uint64_t>::max());
    }

    // 负数整数 id 保持符号。
    auto negative_id = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":-3,\"method\":\"ping\"}", kLimits);
    CHECK(negative_id);
    if (negative_id) {
        CHECK(std::move(negative_id).value().id.AsInt64() == -3);
    }

    // 通知：无 id 成员，不回 JSON-RPC 响应。
    auto notification = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}", kLimits);
    CHECK(notification);
    if (notification) {
        JsonRpcRequest request = std::move(notification).value();
        CHECK(request.notification);
        CHECK(request.method == "notifications/initialized");
    }

    // params 允许 object 或 array；_meta 单独取出（规范命名空间键）。
    auto with_params = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\","
        "\"params\":{\"name\":\"echo\",\"_meta\":{"
        "\"io.modelcontextprotocol/protocolVersion\":\"2026-07-28\","
        "\"io.modelcontextprotocol/clientCapabilities\":{}}}}",
        kLimits);
    CHECK(with_params);
    if (with_params) {
        JsonRpcRequest request = std::move(with_params).value();
        CHECK(request.params && request.params->IsObject());
        CHECK(request.params->Find("name").value().AsString().value() == "echo");
        CHECK(request.meta && request.meta->IsObject());
        CHECK(request.meta->Find(kMetaVersionKey).value().AsString().value() == "2026-07-28");
    }

    auto array_params = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"m\",\"params\":[1,2]}", kLimits);
    CHECK(array_params);
    if (array_params) {
        CHECK(std::move(array_params).value().params->IsArray());
    }
}

void TestOwnedRequestId() {
    std::string raw = "{\"jsonrpc\":\"2.0\",\"id\":\"owned-42\",\"method\":\"m\"}";
    auto parsed = bbt::infra::mcp::ParseRequestFrame(raw, kLimits);
    CHECK(parsed);
    if (!parsed) return;
    JsonRpcRequest request = std::move(parsed).value();
    // 覆盖原始帧内存（同长度原地改写，不触发重分配）：id 必须是自有副本。
    raw.assign(raw.size(), 'X');
    CHECK(request.id.IsString());
    CHECK(request.id.AsString() == "owned-42");
    CHECK(request.method == "m");

    // id 键区分类型：字符串 "1" 与整数 1 不得混为同一身份。
    CHECK(RequestId(std::string("1")).Key() != RequestId(std::int64_t(1)).Key());
    CHECK(RequestId(std::int64_t(1)) != RequestId(std::uint64_t(1)));
    CHECK(RequestId(std::string("1")) == RequestId(std::string("1")));
    CHECK(RequestId(std::int64_t(-1)).Key() != RequestId(std::uint64_t(1)).Key());

    // 原值回包：id 不经 double 也不改类型。
    JsonValue result = JsonValue::Object();
    JsonValue response = bbt::infra::mcp::MakeSuccessResponse(RequestId(std::int64_t(9007199254740993LL)),
                                                             std::move(result));
    CHECK(response.Serialize() ==
          "{\"jsonrpc\":\"2.0\",\"id\":9007199254740993,\"result\":{}}");
    JsonValue string_response =
        bbt::infra::mcp::MakeSuccessResponse(RequestId(std::string("req-1")), JsonValue::Object());
    CHECK(string_response.Serialize() == "{\"jsonrpc\":\"2.0\",\"id\":\"req-1\",\"result\":{}}");
    JsonValue uint_response = bbt::infra::mcp::MakeSuccessResponse(
        RequestId(std::numeric_limits<std::uint64_t>::max()), JsonValue::Object());
    CHECK(uint_response.Serialize() ==
          "{\"jsonrpc\":\"2.0\",\"id\":18446744073709551615,\"result\":{}}");
}

void TestFrameRejections() {
    // 非 JSON / 空 / 超深 / 超大 / 重复协议字段 → -32700。
    auto bad_json = bbt::infra::mcp::ParseRequestFrame("{oops", kLimits);
    RejectedWithCode(bad_json, JsonRpcErrorCode::ParseError, __LINE__);
    auto empty = bbt::infra::mcp::ParseRequestFrame("", kLimits);
    RejectedWithCode(empty, JsonRpcErrorCode::ParseError, __LINE__);
    auto duplicate_field = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"m\"}", kLimits);
    RejectedWithCode(duplicate_field, JsonRpcErrorCode::ParseError, __LINE__);
    auto duplicate_method = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"a\",\"method\":\"b\"}", kLimits);
    RejectedWithCode(duplicate_method, JsonRpcErrorCode::ParseError, __LINE__);
    ProtocolLimits small;
    small.max_message_bytes = 8;
    auto too_big = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"m\"}", small);
    RejectedWithCode(too_big, JsonRpcErrorCode::ParseError, __LINE__);
    ProtocolLimits shallow;
    shallow.max_depth = 2;
    auto too_deep = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"m\",\"params\":{\"a\":{\"b\":{\"c\":1}}}}",
        shallow);
    RejectedWithCode(too_deep, JsonRpcErrorCode::ParseError, __LINE__);

    // batch / 顶层非 object / 信封不合法 → -32600。
    auto batch = bbt::infra::mcp::ParseRequestFrame(
        "[{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"m\"}]", kLimits);
    RejectedWithCode(batch, JsonRpcErrorCode::InvalidRequest, __LINE__);
    auto scalar = bbt::infra::mcp::ParseRequestFrame("\"m\"", kLimits);
    RejectedWithCode(scalar, JsonRpcErrorCode::InvalidRequest, __LINE__);
    auto no_version = bbt::infra::mcp::ParseRequestFrame("{\"id\":1,\"method\":\"m\"}", kLimits);
    RejectedWithCode(no_version, JsonRpcErrorCode::InvalidRequest, __LINE__);
    auto wrong_version = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"1.0\",\"id\":1,\"method\":\"m\"}", kLimits);
    RejectedWithCode(wrong_version, JsonRpcErrorCode::InvalidRequest, __LINE__);
    auto version_type = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":2.0,\"id\":1,\"method\":\"m\"}", kLimits);
    RejectedWithCode(version_type, JsonRpcErrorCode::InvalidRequest, __LINE__);
    auto no_method = bbt::infra::mcp::ParseRequestFrame("{\"jsonrpc\":\"2.0\",\"id\":1}", kLimits);
    RejectedWithCode(no_method, JsonRpcErrorCode::InvalidRequest, __LINE__);
    auto method_type = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":3}", kLimits);
    RejectedWithCode(method_type, JsonRpcErrorCode::InvalidRequest, __LINE__);
    auto method_empty = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"\"}", kLimits);
    RejectedWithCode(method_empty, JsonRpcErrorCode::InvalidRequest, __LINE__);

    // id 只接受 string/integer：null / float / bool / array / object 全拒。
    auto id_null = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":null,\"method\":\"m\"}", kLimits);
    RejectedWithCode(id_null, JsonRpcErrorCode::InvalidRequest, __LINE__);
    auto id_float = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":1.5,\"method\":\"m\"}", kLimits);
    RejectedWithCode(id_float, JsonRpcErrorCode::InvalidRequest, __LINE__);
    auto id_bool = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":true,\"method\":\"m\"}", kLimits);
    RejectedWithCode(id_bool, JsonRpcErrorCode::InvalidRequest, __LINE__);
    auto id_array = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":[1],\"method\":\"m\"}", kLimits);
    RejectedWithCode(id_array, JsonRpcErrorCode::InvalidRequest, __LINE__);
    auto id_object = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":{},\"method\":\"m\"}", kLimits);
    RejectedWithCode(id_object, JsonRpcErrorCode::InvalidRequest, __LINE__);

    // 请求里混入 result/error（response 伪装成 request）→ -32600。
    auto with_result = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"m\",\"result\":{}}", kLimits);
    RejectedWithCode(with_result, JsonRpcErrorCode::InvalidRequest, __LINE__);
    auto with_error = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"m\","
        "\"error\":{\"code\":-32601,\"message\":\"x\"}}",
        kLimits);
    RejectedWithCode(with_error, JsonRpcErrorCode::InvalidRequest, __LINE__);

    // params 非 object|array → -32602。
    auto params_string = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"m\",\"params\":\"x\"}", kLimits);
    RejectedWithCode(params_string, JsonRpcErrorCode::InvalidParams, __LINE__);
    auto params_number = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"m\",\"params\":3}", kLimits);
    RejectedWithCode(params_number, JsonRpcErrorCode::InvalidParams, __LINE__);
    auto params_null = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"m\",\"params\":null}", kLimits);
    RejectedWithCode(params_null, JsonRpcErrorCode::InvalidParams, __LINE__);
}

// F6：modern clientCapabilities 已知 shape 校验（三 revision profile + 扩展键命名）。
void TestCapabilitiesShapeProfiles() {
    namespace mcp = bbt::infra::mcp;
    auto caps = [](const char* text) {
        auto parsed = JsonValue::Parse(text, 1U << 20, 32);
        if (!parsed) {
            ++g_checks;
            ++g_failures;
            std::fprintf(stderr, "FAIL caps fixture is not valid JSON\n");
            return JsonValue();
        }
        return std::move(parsed).value();
    };
    auto check = [&](const char* text, mcp::CapabilitiesRevision revision, bool expect_ok) {
        auto shape = mcp::ValidateClientCapabilities(caps(text), revision);
        ++g_checks;
        if (std::holds_alternative<ProtocolFailure>(shape) == expect_ok) {
            ++g_failures;
            std::fprintf(stderr, "FAIL caps revision=%d expect_ok=%d text=%s\n",
                         static_cast<int>(revision), expect_ok ? 1 : 0, text);
        }
        if (!expect_ok) {
            ++g_checks;
            const ProtocolFailure& failure = std::get<ProtocolFailure>(shape);
            if (failure.code != JsonRpcErrorCode::InvalidParams ||
                failure.message.find("injected") != std::string::npos)
                ++g_failures;  // -32602 固定诊断，不回显注入值
        }
    };
    using Rev = mcp::CapabilitiesRevision;
    const Rev M = Rev::Modern2026_07_28;
    const Rev N = Rev::Legacy2025_11_25;

    // 两 profile 通用合法：空 / 已知字段 / 未知顶层能力（开放集合，不凭类型名拒）。
    for (Rev revision : {M, N}) {
        check("{}", revision, true);
        check(R"({"roots":{},"sampling":{},"elicitation":{},"experimental":{"x":{}}})", revision,
              true);
        check(R"({"vendor/custom-cap":{"any":true}})", revision, true);
        check(R"({"roots":"bad"})", revision, false);
        check(R"({"sampling":1})", revision, false);
        check(R"({"elicitation":[]})", revision, false);
        check(R"({"experimental":{"x":1}})", revision, false);
    }
    // modern 专有：roots 无 listChanged 已知字段限制；extensions 须带合法 prefix。
    check(R"({"roots":{"listChanged":true}})", M, true);
    check(R"({"extensions":{"io.modelcontextprotocol/oauth-client-credentials":{}}})", M, true);
    check(R"({"extensions":{"io.modelcontextprotocol/":{}}})", M, true);  // name 允许为空
    check(R"({"extensions":{"no-slash":{}}})", M, false);
    check(R"({"extensions":{"1bad/x":{}}})", M, false);   // label 须以字母开头
    check(R"({"extensions":{"a..b/x":{}}})", M, false);   // 空 label
    check(R"({"extensions":{"a/-b":{}}})", M, false);     // name 首字符须字母数字
    check(R"({"extensions":{"a/b/c":{}}})", M, false);    // name 不含 '/'
    check(R"({"extensions":{"a/x":1}})", M, false);       // 值须 object
    // revision 差异：roots.listChanged 与 sampling 子字段只在 legacy 已知。
    check(R"({"roots":{"listChanged":true}})", N, true);
    check(R"({"roots":{"listChanged":"yes"}})", N, false);
    check(R"({"sampling":{"context":"bad"}})", N, false);
    check(R"({"sampling":{"context":{},"tools":{},"future":1}})", N, true);
    // tasks：仅 11 月已知（本 server 不实现，只校验声明 shape）。
    check(R"({"tasks":{"list":{},"cancel":{},"requests":{"sampling":{"createMessage":{}},)"
          R"("elicitation":{"create":{}}}}})", N, true);
    check(R"({"tasks":{"list":"bad"}})", N, false);
    check(R"({"tasks":{"requests":{"sampling":{"createMessage":1}}}})", N, false);
    // owned raw：未知字段原样保留（不规范化、不丢弃）。
    auto kept = mcp::ValidateClientCapabilities(
        caps(R"({"roots":{},"vendor/custom":true})"), Rev::Legacy2025_11_25);
    ++g_checks;
    if (!std::holds_alternative<JsonValue>(kept) ||
        !std::get<JsonValue>(kept).Find("vendor/custom"))
        ++g_failures;
}

void TestModernMetaValidation() {
    // M1：规范命名空间键被接受（此前被误拒为 -32602）；扩展键按规则允许共存。
    auto spec_params = JsonValue::Parse(
        "{\"_meta\":{\"io.modelcontextprotocol/protocolVersion\":\"2026-07-28\","
        "\"io.modelcontextprotocol/clientCapabilities\":{\"sampling\":{}},"
        "\"io.modelcontextprotocol/clientInfo\":{\"name\":\"c\",\"version\":\"1\"},"
        "\"progressToken\":\"t\"}}",
        1U << 20, 32);
    CHECK(spec_params);
    if (spec_params) {
        JsonValue parsed_params = std::move(spec_params).value();
        auto meta = bbt::infra::mcp::ValidateModernMeta(parsed_params, kVersions);
        CHECK(std::holds_alternative<bbt::infra::mcp::ModernMeta>(meta));
        if (std::holds_alternative<bbt::infra::mcp::ModernMeta>(meta)) {
            auto& good = std::get<bbt::infra::mcp::ModernMeta>(meta);
            CHECK(good.protocol_version == "2026-07-28");
            CHECK(good.client_capabilities.IsObject());
            CHECK(good.client_capabilities.Find("sampling").value().IsObject());
        }
    }

    // M1：仅裸键（非规范命名空间）必须被拒为 -32602（此前被误收为 modern）。
    auto bare_params = JsonValue::Parse(
        "{\"_meta\":{\"protocolVersion\":\"2026-07-28\",\"clientCapabilities\":{}}}",
        1U << 20, 32);
    CHECK(bare_params);
    if (bare_params) {
        auto meta = bbt::infra::mcp::ValidateModernMeta(std::move(bare_params).value(), kVersions);
        CHECK(std::holds_alternative<ProtocolFailure>(meta));
        if (std::holds_alternative<ProtocolFailure>(meta))
            CHECK(std::get<ProtocolFailure>(meta).code == JsonRpcErrorCode::InvalidParams);
    }
    // 裸 version + 规范 caps：缺规范 version，仍拒（不因任一裸键存在而放行）。
    auto mixed_params = JsonValue::Parse(
        "{\"_meta\":{\"protocolVersion\":\"2026-07-28\","
        "\"io.modelcontextprotocol/clientCapabilities\":{}}}",
        1U << 20, 32);
    CHECK(mixed_params);
    if (mixed_params) {
        auto meta = bbt::infra::mcp::ValidateModernMeta(std::move(mixed_params).value(), kVersions);
        CHECK(std::holds_alternative<ProtocolFailure>(meta));
        if (std::holds_alternative<ProtocolFailure>(meta))
            CHECK(std::get<ProtocolFailure>(meta).code == JsonRpcErrorCode::InvalidParams);
    }

    // 缺字段/类型错 → -32602，且 message 固定不回显输入。
    const std::string secret = "injected-secret-value";
    auto wrong_meta_type = JsonValue::Parse("{\"_meta\":\"x\"}", 1U << 20, 32);
    auto no_meta = JsonValue::Parse("{\"name\":\"echo\"}", 1U << 20, 32);
    auto no_version = JsonValue::Parse(
        "{\"_meta\":{\"io.modelcontextprotocol/clientCapabilities\":{}}}", 1U << 20, 32);
    auto version_type = JsonValue::Parse(
        "{\"_meta\":{\"io.modelcontextprotocol/protocolVersion\":7,"
        "\"io.modelcontextprotocol/clientCapabilities\":{}}}",
        1U << 20, 32);
    auto no_caps = JsonValue::Parse(
        "{\"_meta\":{\"io.modelcontextprotocol/protocolVersion\":\"2026-07-28\"}}", 1U << 20, 32);
    auto caps_type = JsonValue::Parse(
        "{\"_meta\":{\"io.modelcontextprotocol/protocolVersion\":\"2026-07-28\","
        "\"io.modelcontextprotocol/clientCapabilities\":[]}}",
        1U << 20, 32);
    auto caps_type_secret = JsonValue::Parse(
        "{\"_meta\":{\"io.modelcontextprotocol/protocolVersion\":\"" + secret + "\","
        "\"io.modelcontextprotocol/clientCapabilities\":[]}}",
        1U << 20, 32);
    const std::optional<JsonValue> no_params;

    // 逐例显式断言（避免对 result/optional 取巧的类型转换）。
    auto check_invalid_params = [](std::variant<bbt::infra::mcp::ModernMeta, ProtocolFailure> v,
                                   const char* what) {
        ++g_checks;
        if (std::holds_alternative<ProtocolFailure>(v)) {
            const ProtocolFailure& failure = std::get<ProtocolFailure>(v);
            ++g_checks;
            if (failure.code != JsonRpcErrorCode::InvalidParams) {
                ++g_failures;
                std::fprintf(stderr, "FAIL %s: code=%d expected -32602\n", what,
                             static_cast<int>(failure.code));
            }
        } else {
            ++g_failures;
            std::fprintf(stderr, "FAIL %s: should be rejected\n", what);
        }
    };

    check_invalid_params(bbt::infra::mcp::ValidateModernMeta(no_params, kVersions), "no params");
    if (no_meta) {
        check_invalid_params(
            bbt::infra::mcp::ValidateModernMeta(std::move(no_meta).value(), kVersions),
            "no _meta");
    }
    if (wrong_meta_type) {
        check_invalid_params(
            bbt::infra::mcp::ValidateModernMeta(std::move(wrong_meta_type).value(), kVersions),
            "_meta wrong type");
    }
    if (no_version) {
        check_invalid_params(
            bbt::infra::mcp::ValidateModernMeta(std::move(no_version).value(), kVersions),
            "no namespaced protocolVersion");
    }
    if (version_type) {
        check_invalid_params(
            bbt::infra::mcp::ValidateModernMeta(std::move(version_type).value(), kVersions),
            "protocolVersion wrong type");
    }
    if (no_caps) {
        check_invalid_params(
            bbt::infra::mcp::ValidateModernMeta(std::move(no_caps).value(), kVersions),
            "no clientCapabilities");
    }
    if (caps_type) {
        check_invalid_params(
            bbt::infra::mcp::ValidateModernMeta(std::move(caps_type).value(), kVersions),
            "clientCapabilities wrong type");
    }

    // 不支持版本 → -32022，data.requested 为原请求 string，data.supported 为真实服务列表。
    auto unsupported_params = JsonValue::Parse(
        "{\"_meta\":{\"io.modelcontextprotocol/protocolVersion\":\"2025-03-26\","
        "\"io.modelcontextprotocol/clientCapabilities\":{}}}",
        1U << 20, 32);
    CHECK(unsupported_params);
    if (unsupported_params) {
        auto meta = bbt::infra::mcp::ValidateModernMeta(std::move(unsupported_params).value(),
                                                       kVersions);
        CHECK(std::holds_alternative<ProtocolFailure>(meta));
        if (std::holds_alternative<ProtocolFailure>(meta)) {
            const ProtocolFailure& failure = std::get<ProtocolFailure>(meta);
            CHECK(failure.code == JsonRpcErrorCode::UnsupportedProtocolVersion);
            CHECK(static_cast<int>(failure.code) == -32022);
            CHECK(failure.data.has_value());
            if (failure.data) {
                CHECK(failure.data->Find("requested").value().AsString().value() == "2025-03-26");
                auto supported = failure.data->Find("supported");
                CHECK(supported && supported.value().IsArray());
                CHECK(supported.value().Size() == 2);
                CHECK(supported.value().At(0).value().AsString().value() == "2026-07-28");
                CHECK(supported.value().At(1).value().AsString().value() == "2025-11-25");
            }
            // 规范错误文本不含输入回显。
            CHECK(failure.message.find("2025-03-26") == std::string::npos);
            JsonValue error_response = bbt::infra::mcp::MakeErrorResponse(std::nullopt, failure);
            CHECK(error_response.Find("error").value().Find("code").value().AsInt64().value() ==
                  -32022);
            CHECK(error_response.Find("error")
                      .value()
                      .Find("data")
                      .value()
                      .Find("requested")
                      .value()
                      .AsString()
                      .value() == "2025-03-26");
        }
    }

    // M3：legacy/未服务 revision 不得经 modern 校验被接受（两套方法表隔离）。
    // data.supported 仍返回真实服务列表（不伪 advertise、不缩成现代单版）。
    // 2025-06-18 已移除出服务矩阵：它走同一条 -32022 拒绝路径，但 supported
    // 列表不再回显它。
    const char* const kRejectedVersions[] = {"2025-11-25", "2025-06-18"};
    for (const char* legacy : kRejectedVersions) {
        const std::string json =
            std::string("{\"_meta\":{\"io.modelcontextprotocol/protocolVersion\":\"") + legacy +
            "\",\"io.modelcontextprotocol/clientCapabilities\":{}}}";
        auto parsed = JsonValue::Parse(json, 1U << 20, 32);
        CHECK(parsed);
        if (!parsed) continue;
        auto meta = bbt::infra::mcp::ValidateModernMeta(std::move(parsed).value(), kVersions);
        CHECK(std::holds_alternative<ProtocolFailure>(meta));
        if (std::holds_alternative<ProtocolFailure>(meta)) {
            const ProtocolFailure& failure = std::get<ProtocolFailure>(meta);
            CHECK(failure.code == JsonRpcErrorCode::UnsupportedProtocolVersion);
            CHECK(static_cast<int>(failure.code) == -32022);
            CHECK(failure.data.has_value());
            if (failure.data) {
                CHECK(failure.data->Find("requested").value().AsString().value() == legacy);
                auto sup = failure.data->Find("supported");
                CHECK(sup && sup.value().IsArray() && sup.value().Size() == 2);
                if (sup && sup.value().IsArray() && sup.value().Size() == 2) {
                    CHECK(sup.value().At(0).value().AsString().value() == "2026-07-28");
                    CHECK(sup.value().At(1).value().AsString().value() == "2025-11-25");
                }
            }
        }
    }

    // modern 成功与 advertised 列表无关（acceptance 固定为现代单版）：
    // 传「真实 3 版列表」或「仅现代列表」都接受 2026-07-28。
    auto modern_ok_a = JsonValue::Parse(
        "{\"_meta\":{\"io.modelcontextprotocol/protocolVersion\":\"2026-07-28\","
        "\"io.modelcontextprotocol/clientCapabilities\":{}}}",
        1U << 20, 32);
    if (modern_ok_a) {
        auto m = bbt::infra::mcp::ValidateModernMeta(std::move(modern_ok_a).value(), kVersions);
        CHECK(std::holds_alternative<bbt::infra::mcp::ModernMeta>(m));
    }
    auto modern_ok_b = JsonValue::Parse(
        "{\"_meta\":{\"io.modelcontextprotocol/protocolVersion\":\"2026-07-28\","
        "\"io.modelcontextprotocol/clientCapabilities\":{}}}",
        1U << 20, 32);
    if (modern_ok_b) {
        auto m = bbt::infra::mcp::ValidateModernMeta(std::move(modern_ok_b).value(),
                                                    kModernVersions);
        CHECK(std::holds_alternative<bbt::infra::mcp::ModernMeta>(m));
    }

    // 类型错时的 message 不回显输入（含唯一注入值）。
    if (caps_type_secret) {
        auto meta = bbt::infra::mcp::ValidateModernMeta(std::move(caps_type_secret).value(),
                                                       kVersions);
        if (std::holds_alternative<ProtocolFailure>(meta)) {
            CHECK(std::get<ProtocolFailure>(meta).message.find(secret) == std::string::npos);
        } else {
            CHECK(false);
        }
    }

    // 每请求独立：前一个请求的 caps 不影响后一个（无跨请求状态），
    // 且一次被拒的 legacy 请求不污染其后的 modern 成功。
    auto first = JsonValue::Parse(
        "{\"_meta\":{\"io.modelcontextprotocol/protocolVersion\":\"2026-07-28\","
        "\"io.modelcontextprotocol/clientCapabilities\":{\"sampling\":{}}}}",
        1U << 20, 32);
    auto second = JsonValue::Parse(
        "{\"_meta\":{\"io.modelcontextprotocol/protocolVersion\":\"2026-07-28\","
        "\"io.modelcontextprotocol/clientCapabilities\":{}}}",
        1U << 20, 32);
    CHECK(first && second);
    if (first && second) {
        JsonValue first_params = std::move(first).value();
        JsonValue second_params = std::move(second).value();
        auto first_meta = bbt::infra::mcp::ValidateModernMeta(first_params, kVersions);
        auto second_meta = bbt::infra::mcp::ValidateModernMeta(second_params, kVersions);
        CHECK(std::holds_alternative<bbt::infra::mcp::ModernMeta>(first_meta));
        CHECK(std::holds_alternative<bbt::infra::mcp::ModernMeta>(second_meta));
        if (std::holds_alternative<bbt::infra::mcp::ModernMeta>(first_meta) &&
            std::holds_alternative<bbt::infra::mcp::ModernMeta>(second_meta)) {
            auto& a = std::get<bbt::infra::mcp::ModernMeta>(first_meta);
            auto& b = std::get<bbt::infra::mcp::ModernMeta>(second_meta);
            CHECK(a.protocol_version == "2026-07-28");
            CHECK(b.protocol_version == "2026-07-28");
            CHECK(a.client_capabilities.Find("sampling").value().IsObject());
            CHECK(b.client_capabilities.Size() == 0);  // 不继承前一请求的能力
            // 后一请求的校验不改写前一请求的 owned 数据。
            CHECK(a.client_capabilities.Serialize() == "{\"sampling\":{}}");
        }
    }
}

void TestErrorResponseIdPreservation() {
    // M4：id 已知的失败必须原值回包（JSON-RPC 2.0 只在 id 不可确定时回 null）。
    // id77 + params 形状非法 → 错误响应 id == 77。
    std::optional<RequestId> detected;
    auto bad_params = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":77,\"method\":\"tools/call\",\"params\":3}", kLimits,
        &detected);
    CHECK(!bad_params);
    CHECK(detected.has_value());
    if (detected) {
        CHECK(!detected->IsString());
        CHECK(detected->AsInt64() == 77);
        ProtocolFailure failure = bbt::infra::mcp::ToProtocolFailure(bad_params.error());
        CHECK(failure.code == JsonRpcErrorCode::InvalidParams);
        JsonValue resp = bbt::infra::mcp::MakeErrorResponse(detected, failure);
        CHECK(resp.Find("id").value().AsInt64().value() == 77);
        CHECK(resp.Serialize() ==
              "{\"jsonrpc\":\"2.0\",\"id\":77,\"error\":{\"code\":-32602,\"message\":\"Invalid "
              "params\"}}");
    }

    // owned string id：错误响应回原字符串（不借用帧内存）。
    std::optional<RequestId> str_detected;
    auto str_bad = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":\"req-77\",\"method\":\"m\",\"params\":\"x\"}", kLimits,
        &str_detected);
    CHECK(!str_bad);
    CHECK(str_detected.has_value() && str_detected->IsString() &&
          str_detected->AsString() == "req-77");
    if (str_detected) {
        JsonValue resp = bbt::infra::mcp::MakeErrorResponse(
            str_detected, bbt::infra::mcp::ToProtocolFailure(str_bad.error()));
        CHECK(resp.Find("id").value().AsString().value() == "req-77");
    }

    // >2^53 与 uint64 上界 id：错误响应原值回包（不经 double）。
    std::optional<RequestId> big_detected;
    auto big_bad = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":9007199254740993,\"method\":\"m\",\"params\":3}", kLimits,
        &big_detected);
    CHECK(!big_bad);
    CHECK(big_detected.has_value() && big_detected->AsInt64() == 9007199254740993LL);
    if (big_detected) {
        JsonValue resp = bbt::infra::mcp::MakeErrorResponse(
            big_detected, bbt::infra::mcp::ToProtocolFailure(big_bad.error()));
        CHECK(resp.Serialize() ==
              "{\"jsonrpc\":\"2.0\",\"id\":9007199254740993,\"error\":{\"code\":-32602,"
              "\"message\":\"Invalid params\"}}");
    }
    std::optional<RequestId> u64_detected;
    auto u64_bad = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":18446744073709551615,\"method\":\"m\",\"params\":3}", kLimits,
        &u64_detected);
    CHECK(!u64_bad);
    CHECK(u64_detected.has_value() &&
          u64_detected->AsUInt64() == std::numeric_limits<std::uint64_t>::max());
    if (u64_detected) {
        JsonValue resp = bbt::infra::mcp::MakeErrorResponse(
            u64_detected, bbt::infra::mcp::ToProtocolFailure(u64_bad.error()));
        CHECK(resp.Find("id").value().AsUInt64().value() ==
              std::numeric_limits<std::uint64_t>::max());
    }

    // 连续调用清理：第二次若 id 不可确定，不得沿用第一次的 id。
    std::optional<RequestId> reused = RequestId(std::int64_t(999));
    auto parse_err = bbt::infra::mcp::ParseRequestFrame("{oops", kLimits, &reused);
    CHECK(!parse_err);
    CHECK(!reused.has_value());  // parse error：id 不可确定，旧值被清

    // 非法 id 类型（float）→ id 不可确定，不误沿用。
    std::optional<RequestId> bad_id = RequestId(std::int64_t(5));
    auto id_float = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":1.5,\"method\":\"m\"}", kLimits, &bad_id);
    CHECK(!id_float);
    CHECK(!bad_id.has_value());

    // 无 id（notification）：不产生可回包 id，也不误回包。
    std::optional<RequestId> notif_detected = RequestId(std::int64_t(7));
    auto notif = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}", kLimits, &notif_detected);
    CHECK(notif);
    CHECK(!notif_detected.has_value());
    if (notif) CHECK(std::move(notif).value().notification);

    // _meta 校验失败：帧本身解析成功，调用方持有 request.id，可原值回包。
    std::optional<RequestId> meta_detected;
    auto meta_frame = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":77,\"method\":\"tools/call\",\"params\":{\"_meta\":{"
        "\"protocolVersion\":\"2026-07-28\",\"clientCapabilities\":{}}}}",
        kLimits, &meta_detected);
    CHECK(meta_frame);
    if (meta_frame) {
        JsonRpcRequest request = std::move(meta_frame).value();
        CHECK(request.id.AsInt64() == 77);
        auto meta = bbt::infra::mcp::ValidateModernMeta(request.params, kVersions);
        CHECK(std::holds_alternative<ProtocolFailure>(meta));
        if (std::holds_alternative<ProtocolFailure>(meta)) {
            JsonValue resp = bbt::infra::mcp::MakeErrorResponse(
                std::optional<RequestId>(request.id), std::get<ProtocolFailure>(meta));
            CHECK(resp.Find("id").value().AsInt64().value() == 77);
            CHECK(resp.Find("error").value().Find("code").value().AsInt64().value() == -32602);
        }
    }
}

void TestResultConstruction() {
    // 成功信封 + resultType 由库写。
    JsonValue result = JsonValue::Object();
    CHECK(bbt::infra::mcp::ApplyModernResultType(result));
    CHECK(result.Find("resultType").value().AsString().value() == "complete");
    // 业务预置的 resultType 被库覆盖为 complete。
    CHECK(result.Set("resultType", JsonValue("partial")));
    CHECK(bbt::infra::mcp::ApplyModernResultType(result));
    CHECK(result.Find("resultType").value().AsString().value() == "complete");

    // cacheable 默认字段固定。
    CHECK(bbt::infra::mcp::ApplyPrivateCacheHints(result));
    CHECK(result.Find("ttlMs").value().IsInt64());
    CHECK(result.Find("ttlMs").value().AsInt64().value() == 0);
    CHECK(result.Find("cacheScope").value().AsString().value() == "private");

    // 非 object 目标拒绝。
    JsonValue array = JsonValue::Array();
    CHECK(!bbt::infra::mcp::ApplyModernResultType(array));
    CHECK(!bbt::infra::mcp::ApplyPrivateCacheHints(array));

    // 错误信封：id 无法确定时回 null（规范要求），message 为固定文本。
    ProtocolFailure parse_failure;
    parse_failure.code = JsonRpcErrorCode::ParseError;
    parse_failure.message = "Parse error";
    JsonValue error_response = bbt::infra::mcp::MakeErrorResponse(std::nullopt, parse_failure);
    CHECK(error_response.Serialize() ==
          "{\"jsonrpc\":\"2.0\",\"id\":null,\"error\":{\"code\":-32700,\"message\":\"Parse "
          "error\"}}");
    CHECK(error_response.Find("id").value().IsNull());
    CHECK(!error_response.Find("result"));
    // 有 id 时原值回包。
    JsonValue with_id = bbt::infra::mcp::MakeErrorResponse(
        std::optional<RequestId>(RequestId(std::string("abc"))), parse_failure);
    CHECK(with_id.Serialize() ==
          "{\"jsonrpc\":\"2.0\",\"id\":\"abc\",\"error\":{\"code\":-32700,\"message\":\"Parse "
          "error\"}}");
    // data 只在提供时出现。
    ProtocolFailure with_data;
    with_data.code = JsonRpcErrorCode::InvalidParams;
    with_data.message = "Invalid params";
    JsonValue data = JsonValue::Object();
    data.Set("detail", JsonValue("fixed"));
    with_data.data = std::move(data);
    JsonValue data_response = bbt::infra::mcp::MakeErrorResponse(std::nullopt, with_data);
    CHECK(data_response.Find("error").value().Find("data").value().Find("detail").value()
              .AsString().value() == "fixed");
}

void TestDiscoverResult() {
    JsonValue capabilities = JsonValue::Object();  // 本切片无已实现能力可宣告
    auto discover = bbt::infra::mcp::MakeDiscoverResult(kVersions, capabilities,
                                                       std::string("bbt infra mcp"));
    CHECK(discover);
    if (discover) {
        JsonValue result = std::move(discover).value();
        CHECK(result.IsObject());
        auto versions = result.Find("supportedVersions");
        CHECK(versions && versions.value().IsArray() && versions.value().Size() == 2);
        CHECK(versions.value().At(0).value().AsString().value() == "2026-07-28");
        CHECK(versions.value().At(1).value().AsString().value() == "2025-11-25");
        CHECK(result.Find("capabilities").value().IsObject());
        CHECK(result.Find("capabilities").value().Size() == 0);
        CHECK(result.Find("instructions").value().AsString().value() == "bbt infra mcp");
        CHECK(result.Find("resultType").value().AsString().value() == "complete");
        CHECK(result.Find("ttlMs").value().AsInt64().value() == 0);
        CHECK(result.Find("cacheScope").value().AsString().value() == "private");

        // 完整成功信封：id 原值 + result 带 resultType。
        JsonValue response =
            bbt::infra::mcp::MakeSuccessResponse(RequestId(std::int64_t(7)), std::move(result));
        CHECK(response.Find("jsonrpc").value().AsString().value() == "2.0");
        CHECK(response.Find("id").value().AsInt64().value() == 7);
        CHECK(response.Find("result").value().Find("resultType").value().AsString().value() ==
              "complete");
    }

    // 无 instructions 时不出现该成员。
    auto no_instructions =
        bbt::infra::mcp::MakeDiscoverResult(kVersions, JsonValue::Object(), std::nullopt);
    CHECK(no_instructions);
    if (no_instructions) {
        CHECK(!std::move(no_instructions).value().Find("instructions"));
    }

    // capabilities 必须是 object；版本列表不能为空。
    auto bad_caps = bbt::infra::mcp::MakeDiscoverResult(kVersions, JsonValue::Array(), std::nullopt);
    CHECK(!bad_caps && bad_caps.error().domain_code == "mcp.discover.capabilities");
    auto no_versions = bbt::infra::mcp::MakeDiscoverResult({}, JsonValue::Object(), std::nullopt);
    CHECK(!no_versions && no_versions.error().domain_code == "mcp.discover.versions");
}

void TestNotificationClassificationOutParam() {
    // F2：notification_recognized 仅在「完整基本信封合法 + 无 id」后置 true；
    // 非法 method / 非法 jsonrpc / result 混入 / parse 失败 不置位（仍 -32700/-32600）。
    std::optional<RequestId> id;
    bool recognized = false;

    // 无 id + 坏 params：基本信封合法 → 分类为通知（调用方据此静默丢弃，不回 -32602）。
    auto bad_params = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"method\":\"m\",\"params\":3}", kLimits, &id, &recognized);
    CHECK(!bad_params);
    CHECK(recognized);

    // 有 id + 坏 params：是请求，不是通知。
    recognized = true;
    auto with_id = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"m\",\"params\":3}", kLimits, &id, &recognized);
    CHECK(!with_id);
    CHECK(!recognized);

    // 非法 method 且无 id：不构成请求，不分类为通知。
    recognized = true;
    auto bad_method = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"method\":1,\"params\":3}", kLimits, &id, &recognized);
    CHECK(!bad_method);
    CHECK(!recognized);

    // 非法 jsonrpc 且无 id：不分类。
    recognized = true;
    auto bad_version = bbt::infra::mcp::ParseRequestFrame(
        "{\"id\":1,\"method\":\"m\",\"params\":3}", kLimits, &id, &recognized);
    CHECK(!bad_version);
    CHECK(!recognized);

    // parse 失败：不分类（且旧分类被清）。
    recognized = true;
    auto parse_err = bbt::infra::mcp::ParseRequestFrame("{oops", kLimits, &id, &recognized);
    CHECK(!parse_err);
    CHECK(!recognized);

    // result 混入且无 id：不构成请求，不分类。
    recognized = true;
    auto with_result = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"method\":\"m\",\"result\":{}}", kLimits, &id, &recognized);
    CHECK(!with_result);
    CHECK(!recognized);

    // 合法通知（params 合法）：成功解析且分类为 true。
    recognized = false;
    auto ok_notif = bbt::infra::mcp::ParseRequestFrame(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/initialized\"}", kLimits, &id, &recognized);
    CHECK(ok_notif);
    CHECK(recognized);
}

} // namespace

int main() {
    TestValidFrames();
    TestOwnedRequestId();
    TestFrameRejections();
    TestModernMetaValidation();
    TestCapabilitiesShapeProfiles();
    TestErrorResponseIdPreservation();
    TestResultConstruction();
    TestDiscoverResult();
    TestNotificationClassificationOutParam();

    std::printf("mcp.protocol: %d checks, %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
