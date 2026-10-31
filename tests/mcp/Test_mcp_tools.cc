// MCP tools 契约测试（2-6 切片，替换原 speculative gateway 假设的 914 行未实现测试；
// 真源：specs/design proposal-rev12 与公共头契约，非旧测试反推）：
//   - AddTool：Schema 未接通 fail-closed、接通后注册、坏 schema/外部 ref 注册整体拒绝、
//     重复名/空名/Start 后拒绝（fail-closed 无半条记录）；
//   - tools/list：稳定分页 + nextCursor + resultType/ttlMs/cacheScope（modern）；
//   - tools/call：真实 input Schema 校验（缺 required/错类型/额外字段 → -32602、
//     handler 0 次）、未知 tool → -32602、isError 业务错误、handler 异常 → 固定 -32603、
//     output Schema 校验（未声明却给 structuredContent / 不匹配 → 固定 -32603）；
//   - 业务 MRTR：Deferred(InputRequired) → 缺 sampling 能力 -32021+data.requiredCapabilities、
//     有能力 → resultType=input_required + inputRequests/requestState、
//     重试 inputResponses/requestState 进 handler 上下文、legacy Deferred → 固定 -32603；
//   - capabilities：真实注册事实（无 tool 不宣告 tools，有 tool 宣告）。
//
// 全部经真实 core 入口 McpSession::HandleFrame；Schema 校验经公共
// EnableToolsSchemaValidation + 私有 adapter（bbt_infra_mcp_schema，真实编译 validator：
// 官方 metaschema 全文校验 / ref 策略 / 复杂度上限），非 stub 冒充。

#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>
#include <bbt/infra/mcp/McpServer.hpp>
#include <bbt/infra/mcp/McpTypes.hpp>

#include "mcp/detail/Server.hpp"

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

#define CHECK(expr) Check(static_cast<bool>(expr), #expr, __LINE__)

using bbt::infra::result;
using bbt::infra::mcp::ClientInputRequest;
using bbt::infra::mcp::Content;
using bbt::infra::mcp::CreateSession;
using bbt::infra::mcp::DispatchOutcome;
using bbt::infra::mcp::EnableToolsSchemaValidation;
using bbt::infra::mcp::HandlerResult;
using bbt::infra::mcp::InputRequired;
using bbt::infra::mcp::JsonValue;
using bbt::infra::mcp::McpServer;
using bbt::infra::mcp::McpSession;
using bbt::infra::mcp::RequestContext;
using bbt::infra::mcp::ServerOptions;
using bbt::infra::mcp::ToolDefinition;
using bbt::infra::mcp::ToolHandler;
using bbt::infra::mcp::ToolResult;

std::optional<JsonValue> Find(const JsonValue& value, const std::string& key) {
    auto found = value.Find(key);
    if (!found) return std::nullopt;
    return std::move(found).value();
}

std::optional<JsonValue> Pick(bbt::infra::result<JsonValue>&& r) {
    if (!r) return std::nullopt;
    return std::move(r).value();
}

std::optional<JsonValue> Respond(McpSession& session, const std::string& frame) {
    auto outcome = session.HandleFrame(frame);
    if (!outcome || !outcome.value().has_response) return std::nullopt;
    auto parsed = JsonValue::Parse(outcome.value().response, 1U << 20, 32);
    if (!parsed) return std::nullopt;
    return std::move(parsed).value();
}

std::optional<std::int64_t> ErrorCodeOf(const JsonValue& response) {
    auto error = Find(response, "error");
    if (!error) return std::nullopt;
    auto code = Find(*error, "code");
    if (!code) return std::nullopt;
    auto number = code->AsInt64();
    if (!number) return std::nullopt;
    return number.value();
}

JsonValue ModernMeta(const std::string& capabilities_json = "{}") {
    JsonValue meta = JsonValue::Object();
    meta.Set("io.modelcontextprotocol/protocolVersion", JsonValue(std::string("2026-07-28")));
    meta.Set("io.modelcontextprotocol/clientCapabilities",
             JsonValue::Parse(capabilities_json, 1U << 20, 32).value());
    JsonValue params = JsonValue::Object();
    params.Set("_meta", std::move(meta));
    return params;
}

// 带 sampling 能力的 modern tools/call 帧（params._meta 两键 + name[/arguments/extra]）。
std::string ModernCall(int id, const std::string& name, const std::string& arguments_json,
                       const std::string& caps = "{\"sampling\":{}}",
                       const std::string& extra_params = "") {
    std::string params = ModernMeta(caps).Serialize();
    params.insert(params.size() - 1,
                  ",\"name\":\"" + name + "\"" +
                      (arguments_json.empty() ? "" : ",\"arguments\":" + arguments_json) +
                      extra_params);
    return "{\"jsonrpc\":\"2.0\",\"id\":" + std::to_string(id) +
           ",\"method\":\"tools/call\",\"params\":" + params + "}";
}

std::string ModernList(int id, const std::string& cursor_json = "") {
    std::string params = ModernMeta().Serialize();
    params.insert(params.size() - 1, cursor_json);
    return "{\"jsonrpc\":\"2.0\",\"id\":" + std::to_string(id) +
           ",\"method\":\"tools/list\",\"params\":" + params + "}";
}

std::string LegacyCall(int id, const std::string& name, const std::string& arguments_json) {
    std::string args = arguments_json.empty() ? "" : ",\"arguments\":" + arguments_json;
    return "{\"jsonrpc\":\"2.0\",\"id\":" + std::to_string(id) +
           ",\"method\":\"tools/call\",\"params\":{\"name\":\"" + name + "\"" + args + "}}";
}

// echo 工具：原样回显 message（真实 handler，全流程实调）。
ToolHandler EchoHandler(int* counter) {
    return [counter](const RequestContext&, JsonValue arguments)
               -> result<HandlerResult<ToolResult>> {
        ++*counter;
        std::string text;
        if (auto message = arguments.Find("message"))
            if (auto parsed = message.value().AsString()) text = std::string(parsed.value());
        ToolResult produced;
        produced.content.push_back(Content::Text(std::move(text)));
        return result<HandlerResult<ToolResult>>::ok(
            HandlerResult<ToolResult>::Completed(std::move(produced)));
    };
}

const char* kEchoInputSchema =
    R"({"type":"object","properties":{"message":{"type":"string"}},"required":["message"],"additionalProperties":false})";
const char* kBadInputSchema = R"({"type":"strng"})";
// R5：根 type 必须 string "object" 的拒绝样本（{} / 缺 type / type:string / type:array）。
const char* kRootEmptySchema = R"({})";
const char* kRootStringSchema = R"({"type":"string"})";
const char* kRootMissingTypeSchema = R"({"properties":{"message":{"type":"string"}}})";
const char* kRootArrayTypeSchema = R"({"type":["object","null"]})";
const char* kExternalRefSchema =
    R"({"type":"object","properties":{"x":{"$ref":"https://example.invalid/schema"}}})";

}  // namespace

int main() {
    // ---- 1. 未接通 Schema：AddTool fail-closed --------------------------------
    {
        auto server = McpServer::Create(ServerOptions{}).value();
        ToolDefinition def;
        def.name = "echo";
        def.input_schema = JsonValue::Parse(kEchoInputSchema, 1U << 20, 32).value();
        int count = 0;
        auto refused = server->AddTool(std::move(def), EchoHandler(&count));
        CHECK(!refused);  // fail-closed：不注册未校验工具
    }

    // ---- 2. 接通 + 注册 + tools/list + echo 调用 + 输入校验 --------------------
    {
        auto server = McpServer::Create(ServerOptions{}).value();
        CHECK(EnableToolsSchemaValidation(*server));
        CHECK(!EnableToolsSchemaValidation(*server));  // 重复接通拒绝

        ToolDefinition echo;
        echo.name = "echo";
        echo.title = "Echo";
        echo.description = "echoes message";
        echo.input_schema = JsonValue::Parse(kEchoInputSchema, 1U << 20, 32).value();
        int count = 0;
        CHECK(server->AddTool(std::move(echo), EchoHandler(&count)));

        // 坏 schema / 外部 ref：注册整体拒绝（S10 fail-closed，无网络请求）。
        ToolDefinition bad;
        bad.name = "bad";
        bad.input_schema = JsonValue::Parse(kBadInputSchema, 1U << 20, 32).value();
        CHECK(!server->AddTool(std::move(bad), EchoHandler(&count)));
        ToolDefinition external;
        external.name = "external";
        external.input_schema = JsonValue::Parse(kExternalRefSchema, 1U << 20, 32).value();
        CHECK(!server->AddTool(std::move(external), EchoHandler(&count)));
        // R5：input_schema 根 type 必须是 string "object"——{}、缺 type、type:string、
        // type:array 一律 InvalidArgument 拒绝；拒绝先于任何 mutation，不消耗
        // 名字/容量，随后同名合法注册仍成功（事务性）。
        const char* rejected_root_schemas[] = {kRootEmptySchema, kRootStringSchema,
                                               kRootMissingTypeSchema, kRootArrayTypeSchema};
        for (const char* schema_json : rejected_root_schemas) {
            ToolDefinition rejected_root;
            rejected_root.name = "rootcheck";
            rejected_root.input_schema = JsonValue::Parse(schema_json, 1U << 20, 32).value();
            CHECK(!server->AddTool(std::move(rejected_root), EchoHandler(&count)));
        }
        ToolDefinition root_ok;
        root_ok.name = "rootcheck";
        root_ok.input_schema = JsonValue::Parse(kEchoInputSchema, 1U << 20, 32).value();
        CHECK(server->AddTool(std::move(root_ok), EchoHandler(&count)));
        // 重复名 / 空名拒绝。
        ToolDefinition dup;
        dup.name = "echo";
        dup.input_schema = JsonValue::Parse(kEchoInputSchema, 1U << 20, 32).value();
        CHECK(!server->AddTool(std::move(dup), EchoHandler(&count)));
        ToolDefinition empty;
        empty.name = "";
        empty.input_schema = JsonValue::Parse(kEchoInputSchema, 1U << 20, 32).value();
        CHECK(!server->AddTool(std::move(empty), EchoHandler(&count)));

        // architecture §8.4 [G]：x-mcp-header 注册拒绝（v1 不实现 Mcp-Param-*
        // 头↔参数映射）。顶层 x-mcp-header 与 properties.<name>.x-mcp-header
        // 两个标准注解位置都拒绝。
        const char* rejected_x_mcp_header_schemas[] = {
            R"({"type":"object","x-mcp-header":"X-Token","properties":{"a":{"type":"string"}}})",
            R"({"type":"object","properties":{"token":{"type":"string","x-mcp-header":"X-Token"}}})",
        };
        for (const char* schema_json : rejected_x_mcp_header_schemas) {
            ToolDefinition x_header;
            x_header.name = "xheader";
            x_header.input_schema = JsonValue::Parse(schema_json, 1U << 20, 32).value();
            CHECK(!server->AddTool(std::move(x_header), EchoHandler(&count)));
        }
        // 无注解的同名注册仍成功（拒绝不消耗名字/容量，事务性）。
        ToolDefinition x_ok;
        x_ok.name = "xheader";
        x_ok.input_schema = JsonValue::Parse(kEchoInputSchema, 1U << 20, 32).value();
        CHECK(server->AddTool(std::move(x_ok), EchoHandler(&count)));

        CHECK(server->Start());
        // Start 后注册拒绝（快照冻结）。
        ToolDefinition late;
        late.name = "late";
        late.input_schema = JsonValue::Parse(kEchoInputSchema, 1U << 20, 32).value();
        CHECK(!server->AddTool(std::move(late), EchoHandler(&count)));

        auto session = CreateSession(server);

        // capabilities：有 tool 注册 → 宣告 tools（真实注册事实）。
        auto discover = Respond(*session,
            R"({"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}})");
        CHECK(discover);
        auto caps = Find(Find(*discover, "result").value(), "capabilities");
        CHECK(Find(*caps, "tools"));

        // tools/list：单个工具、title/description/inputSchema、cacheable 字段。
        auto list = Respond(*session, ModernList(2));
        CHECK(list);
        CHECK(!ErrorCodeOf(*list));
        auto result_value = Find(*list, "result");
        auto tools = Find(*result_value, "tools");
        CHECK(tools && tools->IsArray());
        auto first = Pick(tools->At(0));
        CHECK(first);
        auto name0 = Find(*first, "name");
        CHECK(name0 && name0->AsString() && name0->AsString().value() == "echo");
        CHECK(Find(*first, "title"));
        auto input_schema = Find(*first, "inputSchema");
        CHECK(input_schema && input_schema->IsObject());
        auto result_type = Find(*result_value, "resultType");
        CHECK(result_type && result_type->AsString() && result_type->AsString().value() == "complete");
        auto ttl = Find(*result_value, "ttlMs");
        CHECK(ttl);

        // 正常调用：handler 恰 1 次，content 原样回显。
        auto ok_call = Respond(*session, ModernCall(3, "echo", R"({"message":"hello"})"));
        CHECK(ok_call);
        CHECK(!ErrorCodeOf(*ok_call));
        auto ok_result = Find(*ok_call, "result");
        auto content = Find(*ok_result, "content");
        CHECK(content && content->IsArray());
        auto block0 = Pick(content->At(0));
        CHECK(block0);
        auto text0 = Find(*block0, "text");
        CHECK(text0 && text0->AsString() && text0->AsString().value() == "hello");
        CHECK(!Find(*ok_result, "isError"));  // false 不写
        CHECK(count == 1);

        // 缺 required → -32602、handler 0 次（S9）。
        auto missing = Respond(*session, ModernCall(4, "echo", "{}"));
        CHECK(missing);
        CHECK(ErrorCodeOf(*missing) == -32602);
        CHECK(count == 1);
        // 错类型 → -32602、handler 0 次（无隐式转换）。
        auto wrong_type = Respond(*session, ModernCall(5, "echo", R"({"message":42})"));
        CHECK(wrong_type);
        CHECK(ErrorCodeOf(*wrong_type) == -32602);
        CHECK(count == 1);
        // 额外字段（additionalProperties:false）→ -32602。
        auto extra = Respond(*session, ModernCall(6, "echo", R"({"message":"x","zzz":1})"));
        CHECK(extra);
        CHECK(ErrorCodeOf(*extra) == -32602);
        CHECK(count == 1);
        // 未知 tool → -32602，handler 0 次。
        auto unknown = Respond(*session, ModernCall(7, "nope", "{}"));
        CHECK(unknown);
        CHECK(ErrorCodeOf(*unknown) == -32602);
        CHECK(count == 1);
    }

    // ---- 3. output Schema 校验 / isError / 异常脱敏 ----------------------------
    {
        auto server = McpServer::Create(ServerOptions{}).value();
        CHECK(EnableToolsSchemaValidation(*server));
        ToolDefinition def;
        def.name = "calc";
        def.input_schema = JsonValue::Parse(R"({"type":"object"})", 1U << 20, 32).value();
        def.output_schema = JsonValue::Parse(
            R"({"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]})", 1U << 20, 32).value();
        int count = 0;
        ToolHandler calc = [&count](const RequestContext&, JsonValue arguments)
                                  -> result<HandlerResult<ToolResult>> {
            ++count;
            std::string operation = "ok";
            if (auto op = arguments.Find("op"))
                if (auto parsed = op.value().AsString()) operation = std::string(parsed.value());
            ToolResult produced;
            if (operation == "boom") throw std::runtime_error("secret boom");
            if (operation == "badoutput") {
                produced.structured_content = JsonValue::Parse(R"({"n":"not-an-integer"})", 1U << 20, 32).value();
                return result<HandlerResult<ToolResult>>::ok(
                    HandlerResult<ToolResult>::Completed(std::move(produced)));
            }
            produced.structured_content = JsonValue::Parse(R"({"n":7})", 1U << 20, 32).value();
            if (operation == "iserror") {
                produced.is_error = true;
                produced.content.push_back(Content::Text("business failure"));
            }
            return result<HandlerResult<ToolResult>>::ok(
                HandlerResult<ToolResult>::Completed(std::move(produced)));
        };
        CHECK(server->AddTool(std::move(def), std::move(calc)));
        CHECK(server->Start());
        auto session = CreateSession(server);

        auto good = Respond(*session, ModernCall(10, "calc", "{}"));
        CHECK(good);
        CHECK(!ErrorCodeOf(*good));
        auto good_result = Find(*good, "result");
        auto structured = Find(*good_result, "structuredContent");
        CHECK(structured && Find(*structured, "n"));

        // 输出不满足 output schema → 固定 -32603（S11：不回显原始输出）。
        auto bad_out = Respond(*session, ModernCall(11, "calc", R"({"op":"badoutput"})"));
        CHECK(bad_out);
        CHECK(ErrorCodeOf(*bad_out) == -32603);
        CHECK(bad_out->Serialize().find("not-an-integer") == std::string::npos);

        // 业务 isError → 结果 isError=true，仍是非 error 帧（业务错误分层）。
        auto is_error = Respond(*session, ModernCall(12, "calc", R"({"op":"iserror"})"));
        CHECK(is_error);
        CHECK(!ErrorCodeOf(*is_error));
        auto error_flag = Find(Find(*is_error, "result").value(), "isError");
        CHECK(error_flag && error_flag->AsBool() && error_flag->AsBool().value());

        // handler 异常 → 固定 -32603，异常文本不回显（脱敏）。
        auto boom = Respond(*session, ModernCall(13, "calc", R"({"op":"boom"})"));
        CHECK(boom);
        CHECK(ErrorCodeOf(*boom) == -32603);
        CHECK(boom->Serialize().find("secret boom") == std::string::npos);
        CHECK(count == 4);  // good/badoutput/iserror/boom 全部进入 handler（输出校验在出口）
    }

    // ---- 4. 业务 MRTR：Deferred(InputRequired) ---------------------------------
    {
        auto server = McpServer::Create(ServerOptions{}).value();
        CHECK(EnableToolsSchemaValidation(*server));
        ToolDefinition def;
        def.name = "confirm";
        def.input_schema = JsonValue::Parse(R"({"type":"object"})", 1U << 20, 32).value();
        int count = 0;
        ToolHandler confirm = [&count](const RequestContext& context,
                                       JsonValue) -> result<HandlerResult<ToolResult>> {
            ++count;
            // 重试请求（第二轮）：库把 owned inputResponses/requestState 放进上下文。
            if (!context.InputResponses().empty()) {
                CHECK(context.RequestState().has_value());
                ToolResult produced;
                produced.content.push_back(Content::Text("resumed"));
                return result<HandlerResult<ToolResult>>::ok(
                    HandlerResult<ToolResult>::Completed(std::move(produced)));
            }
            std::map<std::string, ClientInputRequest> requests;
            requests.emplace("srv1",
                             ClientInputRequest{"sampling/createMessage", JsonValue::Object()});
            auto required = InputRequired::Create(std::move(requests), std::string("state-1"));
            return result<HandlerResult<ToolResult>>::ok(
                HandlerResult<ToolResult>::Deferred(std::move(required).value()));
        };
        CHECK(server->AddTool(std::move(def), std::move(confirm)));
        CHECK(server->Start());
        auto session = CreateSession(server);

        // 客户端缺 sampling 能力 → -32021 + data.requiredCapabilities.sampling（S15）。
        auto missing_caps = Respond(*session, ModernCall(20, "confirm", "{}", "{}"));
        CHECK(missing_caps);
        CHECK(ErrorCodeOf(*missing_caps) == -32021);
        auto error = Find(*missing_caps, "error");
        auto data = Find(*error, "data");
        auto required_caps = Find(*data, "requiredCapabilities");
        auto sampling = Find(*required_caps, "sampling");
        CHECK(sampling);
        CHECK(count == 1);

        // 有 sampling 能力 → resultType=input_required + inputRequests + requestState。
        auto deferred = Respond(*session, ModernCall(21, "confirm", "{}"));
        CHECK(deferred);
        CHECK(!ErrorCodeOf(*deferred));
        auto result_value = Find(*deferred, "result");
        auto result_type = Find(*result_value, "resultType");
        CHECK(result_type && result_type->AsString() &&
              result_type->AsString().value() == "input_required");
        auto input_requests = Find(*result_value, "inputRequests");
        CHECK(input_requests);
        auto srv1 = Find(*input_requests, "srv1");
        CHECK(srv1);
        auto method = Find(*srv1, "method");
        CHECK(method && method->AsString() && method->AsString().value() == "sampling/createMessage");
        auto state = Find(*result_value, "requestState");
        CHECK(state && state->AsString() && state->AsString().value() == "state-1");
        CHECK(count == 2);

        // MRTR 重试：inputResponses + requestState → handler 观察到 owned 输入（新 id）。
        std::string retry_extra =
            ",\"inputResponses\":{\"srv1\":{\"text\":\"yes\"}},\"requestState\":\"state-1\"";
        auto retry = Respond(*session,
                             ModernCall(22, "confirm", "{}", "{\"sampling\":{}}", retry_extra));
        CHECK(retry);
        CHECK(!ErrorCodeOf(*retry));
        auto retry_content = Find(Find(*retry, "result").value(), "content");
        auto retry_block = retry_content ? Pick(retry_content->At(0)) : std::nullopt;
        auto retry_text = retry_block ? Find(*retry_block, "text") : std::nullopt;
        CHECK(retry_text && retry_text->AsString() && retry_text->AsString().value() == "resumed");
        CHECK(count == 3);

        // legacy 路径 Deferred → 固定 -32603（不产生 modern input_required 结构）。
        auto legacy_server = McpServer::Create(ServerOptions{}).value();
        CHECK(EnableToolsSchemaValidation(*legacy_server));
        ToolDefinition legacy_def;
        legacy_def.name = "confirm";
        legacy_def.input_schema = JsonValue::Parse(R"({"type":"object"})", 1U << 20, 32).value();
        int legacy_count = 0;
        ToolHandler legacy_handler = [&legacy_count](const RequestContext&, JsonValue)
                                            -> result<HandlerResult<ToolResult>> {
            ++legacy_count;
            std::map<std::string, ClientInputRequest> requests;
            requests.emplace("srv1",
                             ClientInputRequest{"sampling/createMessage", JsonValue::Object()});
            auto required = InputRequired::Create(std::move(requests), std::string("s"));
            return result<HandlerResult<ToolResult>>::ok(
                HandlerResult<ToolResult>::Deferred(std::move(required).value()));
        };
        CHECK(legacy_server->AddTool(std::move(legacy_def), std::move(legacy_handler)));
        CHECK(legacy_server->Start());
        auto legacy_session = CreateSession(legacy_server);
        CHECK(Respond(*legacy_session,
            R"({"jsonrpc":"2.0","id":30,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"cli","version":"1"},"protocolVersion":"2025-11-25"}})"));
        auto legacy_deferred = Respond(*legacy_session, LegacyCall(31, "confirm", "{}"));
        CHECK(legacy_deferred);
        CHECK(ErrorCodeOf(*legacy_deferred) == -32603);
        CHECK(legacy_deferred->Serialize().find("input_required") == std::string::npos);
        CHECK(legacy_count == 1);
    }

    // ---- 5. tools/list 稳定分页 + 无 tool 不宣告 tools --------------------------
    {
        ServerOptions options;
        options.page_size = 1;
        auto server = McpServer::Create(options).value();
        CHECK(EnableToolsSchemaValidation(*server));
        for (const char* name : {"a", "b"}) {
            ToolDefinition def;
            def.name = name;
            def.input_schema = JsonValue::Parse(R"({"type":"object"})", 1U << 20, 32).value();
            int count = 0;
            CHECK(server->AddTool(std::move(def), EchoHandler(&count)));
        }
        CHECK(server->Start());
        auto session = CreateSession(server);
        auto page1 = Respond(*session, ModernList(40));
        CHECK(page1);
        auto r1 = Find(*page1, "result");
        auto t1 = Find(*r1, "tools");
        CHECK(t1 && t1->IsArray());
        auto item0 = Pick(t1->At(0));
        CHECK(item0 && item0->IsObject());
        auto next = Find(*r1, "nextCursor");
        CHECK(next);
        auto page2 = Respond(*session, ModernList(
                                          41, ",\"cursor\":\"" + std::string(next->AsString().value()) + "\""));
        CHECK(page2);
        auto r2 = Find(*page2, "result");
        auto t2 = Find(*r2, "tools");
        CHECK(t2);
        CHECK(!Find(*r2, "nextCursor"));  // 末页无 nextCursor
        // 非法 cursor → -32602。
        auto bad_cursor = Respond(*session, ModernList(42, ",\"cursor\":\"zz\""));
        CHECK(bad_cursor);
        CHECK(ErrorCodeOf(*bad_cursor) == -32602);
    }
    {
        // 无 tool 注册 → capabilities 不宣告 tools；tools/list → -32601（P4）。
        auto server = McpServer::Create(ServerOptions{}).value();
        CHECK(server->Start());
        auto session = CreateSession(server);
        auto discover = Respond(*session,
            R"({"jsonrpc":"2.0","id":50,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}})");
        CHECK(discover);
        auto caps = Find(Find(*discover, "result").value(), "capabilities");
        CHECK(!Find(*caps, "tools"));
        auto list = Respond(*session, ModernList(51));
        CHECK(list);
        CHECK(ErrorCodeOf(*list) == -32601);
    }

    std::printf("mcp.tools: %d checks / %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
