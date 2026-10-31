// MCP 注册与业务分派契约测试（task 2-4 不依赖 Schema 的切片：resources / prompts）。
//
// 全部经真实 core 入口：公共注册面（McpServer::AddResource/AddPrompt/Start/Close）+
// 内部帧入口（McpSession::HandleFrame，仅本目标可见）。覆盖：
//   - 注册校验/原子性/Start 冻结快照/Close 后拒绝（失败不留半条记录）；
//   - resources/list（稳定分页 + cursor 校验）、resources/templates/list（真实空列表）、
//     resources/read（text/blob、未知 uri、缺 uri）；
//   - prompts/list（稳定分页 + cursor 校验）、prompts/get（required/string 参数、
//     未知 name、role/content 结构）；
//   - 结果由库构造：modern 写 resultType=complete + ttlMs/cacheScope；legacy 不写；
//   - 能力只反映真实注册；异常/失败/空 contents/messages → 固定内部错误（脱敏）；
//   - legacy 生命周期门禁（未握手业务方法 → -32602，ping 豁免）；
//   - 回调不在注册表锁内执行（handler 内 Close 不自等、不死锁）。
//
// 无第三方测试框架依赖；经 ${PROJECT_SOURCE_DIR}/bbt 消费内部实现头。

#include <cstdint>
#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
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
using bbt::infra::mcp::Content;
using bbt::infra::mcp::CreateSession;
using bbt::infra::mcp::CancellationView;
using bbt::infra::mcp::JsonValue;
using bbt::infra::mcp::McpServer;
using bbt::infra::mcp::McpSession;
using bbt::infra::mcp::PromptArgument;
using bbt::infra::mcp::PromptDefinition;
using bbt::infra::mcp::PromptMessage;
using bbt::infra::mcp::PromptResult;
using bbt::infra::mcp::PromptRole;
using bbt::infra::mcp::RequestContext;
using bbt::infra::mcp::ResourceContents;
using bbt::infra::mcp::ResourceDefinition;
using bbt::infra::mcp::ResourceResult;
using bbt::infra::mcp::ServerOptions;

// --- 帧构造 --------------------------------------------------------------------

std::string ModernFrame(const std::string& id, const std::string& method,
                        const std::string& params_body = "") {
    std::string params = "{";
    if (!params_body.empty()) params += params_body + ",";
    params +=
        "\"_meta\":{\"io.modelcontextprotocol/protocolVersion\":\"2026-07-28\","
        "\"io.modelcontextprotocol/clientCapabilities\":{\"roots\":{}}}}";
    return "{\"jsonrpc\":\"2.0\",\"id\":" + id + ",\"method\":\"" + method +
           "\",\"params\":" + params + "}";
}

std::string LegacyFrame(const std::string& id, const std::string& method,
                        const std::string& params_body = "{}") {
    return "{\"jsonrpc\":\"2.0\",\"id\":" + id + ",\"method\":\"" + method +
           "\",\"params\":" + params_body + "}";
}

const char* kInitialize =
    R"({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"capabilities":{"roots":{}},)"
    R"("clientInfo":{"name":"client","version":"1.0"},"protocolVersion":"2025-11-25"}})";

// --- 响应工具 ------------------------------------------------------------------

JsonValue Respond(McpSession& session, const std::string& frame, int line) {
    auto outcome = session.HandleFrame(frame);
    ++g_checks;
    if (!outcome || !outcome.value().has_response) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d  no response for frame\n", __FILE__, line);
        return JsonValue();
    }
    auto parsed = JsonValue::Parse(outcome.value().response, 1U << 20, 32);
    if (!parsed) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d  response is not JSON\n", __FILE__, line);
        return JsonValue();
    }
    return std::move(parsed).value();
}

// array 元素（JsonValue::At(index)）；越界返回 null 值。
JsonValue Elem(const JsonValue& array, std::size_t index) {
    auto item = array.At(index);
    if (!item) return JsonValue();
    return std::move(item).value();
}

// handler 便捷构造：text / blob 单个 contents。
result<ResourceResult> TextResource(const std::string& uri, const std::string& mime,
                                    const std::string& body) {
    auto content = ResourceContents::Text(uri, mime, body);
    if (!content) return result<ResourceResult>::err(content.error());
    ResourceResult out;
    out.contents.push_back(std::move(content).value());
    return result<ResourceResult>::ok(std::move(out));
}

result<ResourceResult> BlobResource(const std::string& uri, const std::string& mime,
                                    const std::string& base64) {
    auto content = ResourceContents::Blob(uri, mime, base64);
    if (!content) return result<ResourceResult>::err(content.error());
    ResourceResult out;
    out.contents.push_back(std::move(content).value());
    return result<ResourceResult>::ok(std::move(out));
}

JsonValue At(const JsonValue& root, const std::string& key) {
    auto found = root.Find(key);
    if (!found) return JsonValue();
    return std::move(found).value();
}

std::int64_t ErrorCode(const JsonValue& root) {
    auto error = root.Find("error");
    if (!error) return 0;
    auto code = error.value().Find("code");
    if (!code) return 0;
    auto value = code.value().AsInt64();
    return value ? value.value() : 0;
}

std::string Str(const JsonValue& value) {
    auto text = value.AsString();
    return text ? text.value() : std::string("<not-string>");
}

// 缺键 → "<missing>"（避免测试自身在断言失败时终止）。
std::string Found(const JsonValue& root, const std::string& key) {
    auto found = root.Find(key);
    if (!found) return "<missing>";
    return Str(found.value());
}

// 结果里不含 modern 专有字段（legacy 隔离）。
bool HasModernFields(const JsonValue& result) {
    return static_cast<bool>(result.Find("resultType")) ||
           static_cast<bool>(result.Find("ttlMs")) ||
           static_cast<bool>(result.Find("cacheScope"));
}

result<std::shared_ptr<McpServer>> MakeServer(std::size_t page_size = 50,
                                              std::size_t max_registrations = 256) {
    ServerOptions options;
    options.page_size = page_size;
    options.max_registrations = max_registrations;
    return McpServer::Create(std::move(options));
}

// --- 测试 ----------------------------------------------------------------------

// P4：能力门禁矩阵（empty / prompts-only / resources-only / both，两 era）。
// 业务方法可用性与 BuildCapabilities 是同一冻结注册事实：未 advertise → -32601
//（无 handler 调用、不借空列表假成功）；已注册但未知 uri/name → -32602；legacy
// 可用性判定在握手状态门**之前**（未握手且已注册 → -32602，未注册 → -32601）。
void TestCapabilityGateMatrix() {
    for (int config = 0; config < 4; ++config) {
        const bool with_resources = (config & 1) != 0;
        const bool with_prompts = (config & 2) != 0;
        auto created = MakeServer();
        CHECK(created);
        auto server = created.value();
        int resource_calls = 0;
        if (with_resources) {
            ResourceDefinition definition;
            definition.uri = "mem://r";
            definition.name = "r";
            CHECK(server->AddResource(
                definition, [&resource_calls](const RequestContext&, const std::string& uri) {
                    ++resource_calls;
                    return TextResource(uri, "", "body");
                }));
        }
        if (with_prompts) {
            PromptDefinition definition;
            definition.name = "p";
            CHECK(server->AddPrompt(definition, [](const RequestContext&, const JsonValue&) {
                PromptResult produced;
                PromptMessage message;
                message.content = Content::Text("p");
                produced.messages.push_back(std::move(message));
                return result<PromptResult>::ok(std::move(produced));
            }));
        }
        CHECK(server->Start());

        // 能力宣告 = 同一注册事实。
        auto session = CreateSession(server);
        JsonValue discover = Respond(*session, ModernFrame("1", "server/discover"), __LINE__);
        JsonValue capabilities = At(At(discover, "result"), "capabilities");
        CHECK(capabilities.IsObject());
        CHECK(static_cast<bool>(capabilities.Find("resources")) == with_resources);
        CHECK(static_cast<bool>(capabilities.Find("prompts")) == with_prompts);

        // modern：按注册事实可用/不可用。
        CHECK(ErrorCode(Respond(*session, ModernFrame("2", "resources/list"), __LINE__)) ==
              (with_resources ? 0 : -32601));
        CHECK(ErrorCode(Respond(*session, ModernFrame("3", "resources/templates/list"),
                                __LINE__)) == (with_resources ? 0 : -32601));
        CHECK(ErrorCode(Respond(*session, ModernFrame("4", "prompts/list"), __LINE__)) ==
              (with_prompts ? 0 : -32601));
        // 已注册但未知 uri/name → -32602；未注册 → -32601 且 handler 0 次。
        CHECK(ErrorCode(Respond(*session,
                                ModernFrame("5", "resources/read", R"("uri":"mem://unknown")"),
                                __LINE__)) == (with_resources ? -32602 : -32601));
        CHECK(resource_calls == 0);
        CHECK(ErrorCode(
                  Respond(*session, ModernFrame("6", "prompts/get", R"("name":"unknown")"),
                          __LINE__)) == (with_prompts ? -32602 : -32601));
        if (with_resources) {
            JsonValue list = Respond(*session, ModernFrame("7", "resources/list"), __LINE__);
            CHECK(At(At(list, "result"), "resources").Size() == 1);
            // 有 resource 注册、无 template → templates/list 仍真实空数组成功。
            JsonValue templates =
                Respond(*session, ModernFrame("8", "resources/templates/list"), __LINE__);
            CHECK(At(At(templates, "result"), "resourceTemplates").Size() == 0);
        }

        // legacy：同一可用性事实，且在握手门禁之前判定。
        auto legacy = CreateSession(server);
        CHECK(ErrorCode(Respond(*legacy, LegacyFrame("1", "resources/list"), __LINE__)) ==
              (with_resources ? -32602 : -32601));
        CHECK(ErrorCode(Respond(*legacy, LegacyFrame("2", "prompts/list"), __LINE__)) ==
              (with_prompts ? -32602 : -32601));
        CHECK(ErrorCode(Respond(*legacy, kInitialize, __LINE__)) == 0);
        CHECK(ErrorCode(Respond(*legacy, LegacyFrame("3", "resources/list"), __LINE__)) ==
              (with_resources ? 0 : -32601));
        CHECK(ErrorCode(Respond(*legacy, LegacyFrame("4", "prompts/list"), __LINE__)) ==
              (with_prompts ? 0 : -32601));
        // ping/initialize 豁免不随注册事实变化。
        CHECK(ErrorCode(Respond(*legacy, LegacyFrame("5", "ping"), __LINE__)) == 0);
    }
}

void TestResourceRegistration() {
    auto created = MakeServer();
    CHECK(created);
    auto server = created.value();

    // 非法注册：空 uri / 空 name / 空 handler，全部整体拒绝。
    ResourceDefinition bad;
    CHECK(!server->AddResource(bad, [](const RequestContext&, const std::string&) {
        return result<ResourceResult>::ok(ResourceResult{});
    }));
    CHECK(server->AddResource(bad, [](const RequestContext&, const std::string&) {
              return result<ResourceResult>::ok(ResourceResult{});
          })
              .error()
              .domain_code == "mcp.registry.definition");
    bad.uri = "mem://a";
    CHECK(!server->AddResource(bad, nullptr));
    CHECK(server->AddResource(bad, nullptr).error().domain_code == "mcp.registry.handler");

    ResourceDefinition definition;
    definition.uri = "mem://greeting";
    definition.name = "greeting";
    definition.description = "hello";
    definition.mime_type = "text/plain";
    auto handler = [](const RequestContext&, const std::string& uri) {
        return TextResource(uri, "text/plain", "hello");
    };
    CHECK(server->AddResource(definition, handler));
    // 重复 uri：拒绝且不覆盖已有记录。
    definition.name = "other";
    CHECK(!server->AddResource(definition, handler));
    CHECK(server->AddResource(definition, handler).error().domain_code == "mcp.registry.duplicate");

    // 注册预算：总数上界被真实拒绝（不留半条记录）。
    auto tight = MakeServer(50, 1);
    CHECK(tight);
    CHECK(tight.value()->AddResource(definition, handler));
    ResourceDefinition second;
    second.uri = "mem://second";
    second.name = "second";
    CHECK(!tight.value()->AddResource(second, handler));
    CHECK(tight.value()->AddResource(second, handler).error().code ==
          bbt::infra::ErrorCode::Overloaded);

    // Start 冻结快照：之后注册被拒；Close 之后同样被拒（且不重开）。
    CHECK(server->Start());
    CHECK(!server->AddResource(second, handler));
    CHECK(server->AddResource(second, handler).error().domain_code == "mcp.registry.frozen");
    server->Close();
    CHECK(server->IsClosed());
    CHECK(!server->Start());
    CHECK(!server->AddResource(second, handler));
    CHECK(server->AddResource(second, handler).error().code == bbt::infra::ErrorCode::Closed);
}

void TestResourceListAndReadModern() {
    auto created = MakeServer(/*page_size=*/2);
    CHECK(created);
    auto server = created.value();

    // 3 个资源（按 uri 有序），page_size=2 → 稳定分页 + nextCursor。
    for (int i = 0; i < 3; ++i) {
        ResourceDefinition definition;
        definition.uri = "mem://r" + std::to_string(i);
        definition.name = "r" + std::to_string(i);
        const std::string uri = definition.uri;
        CHECK(server->AddResource(definition,
                                  [uri](const RequestContext&, const std::string& asked) {
                                      return TextResource(asked, "text/plain", "body:" + uri);
                                  }));
    }
    // 二进制资源（规范 base64 blob 二择一）。
    ResourceDefinition binary;
    binary.uri = "mem://blob";
    binary.name = "blob";
    binary.mime_type = "application/octet-stream";
    CHECK(server->AddResource(binary, [](const RequestContext&, const std::string& uri) {
        return BlobResource(uri, "application/octet-stream", "AAECAw==");
    }));
    CHECK(server->Start());
    auto session = CreateSession(server);

    // modern resources/list 第一页：有序、2 条、nextCursor=2、modern 字段齐备。
    JsonValue page1 = Respond(*session, ModernFrame("1", "resources/list"), __LINE__);
    JsonValue result1 = At(page1, "result");
    CHECK(!result1.IsNull());
    CHECK(Str(At(result1, "resultType")) == "complete");
    auto ttl = At(result1, "ttlMs").AsInt64();
    CHECK(ttl && ttl.value() == 0);
    CHECK(Str(At(result1, "cacheScope")) == "private");
    auto resources1 = At(result1, "resources");
    CHECK(resources1.IsArray() && resources1.Size() == 2);
    CHECK(Str(At(Elem(resources1, 0), "uri")) == "mem://blob");
    CHECK(Str(At(Elem(resources1, 0), "mimeType")) == "application/octet-stream");
    CHECK(Str(At(Elem(resources1, 1), "uri")) == "mem://r0");
    CHECK(Str(At(result1, "nextCursor")) == "2");

    // 第二页：cursor=2，无 nextCursor（末页）。
    JsonValue page2 = Respond(*session, ModernFrame("2", "resources/list", R"("cursor":"2")"),
                              __LINE__);
    JsonValue result2 = At(page2, "result");
    auto resources2 = At(result2, "resources");
    CHECK(resources2.IsArray() && resources2.Size() == 2);
    CHECK(Str(At(Elem(resources2, 0), "uri")) == "mem://r1");
    CHECK(Str(At(Elem(resources2, 1), "uri")) == "mem://r2");
    CHECK(At(result2, "nextCursor").IsNull());

    // cursor 校验：非 string / 越界 / 前导零 / 负数 一律 -32602，且不返回列表。
    for (const char* bad_cursor : {R"("cursor":5)", R"("cursor":1)", R"("cursor":"02")",
                                   R"("cursor":"-1")", R"("cursor":true)"}) {
        JsonValue bad = Respond(*session, ModernFrame("9", "resources/list", bad_cursor), __LINE__);
        CHECK(ErrorCode(bad) == -32602);
    }

    // templates/list：本切片不注册模板 → 真实空列表（不假 advertise 模板匹配）。
    JsonValue templates = Respond(*session, ModernFrame("3", "resources/templates/list"), __LINE__);
    JsonValue templates_result = At(templates, "result");
    auto items = At(templates_result, "resourceTemplates");
    CHECK(items.IsArray() && items.Size() == 0);
    CHECK(Str(At(templates_result, "resultType")) == "complete");

    // read：text 资源内容由 handler 产出，uri/mimeType/text 由库构造。
    JsonValue read = Respond(*session,
                             ModernFrame("4", "resources/read", R"("uri":"mem://r1")"), __LINE__);
    JsonValue read_result = At(read, "result");
    auto contents = At(read_result, "contents");
    CHECK(contents.IsArray() && contents.Size() == 1);
    CHECK(Str(At(Elem(contents, 0), "uri")) == "mem://r1");
    CHECK(Str(At(Elem(contents, 0), "text")) == "body:mem://r1");
    CHECK(Str(At(Elem(contents, 0), "mimeType")) == "text/plain");

    // read：blob 资源走规范 base64 字段（不出现 text）。
    JsonValue read_blob = Respond(*session,
                                  ModernFrame("5", "resources/read", R"("uri":"mem://blob")"),
                                  __LINE__);
    auto blob_contents = At(At(read_blob, "result"), "contents");
    CHECK(Str(At(Elem(blob_contents, 0), "blob")) == "AAECAw==");
    CHECK(At(Elem(blob_contents, 0), "text").IsNull());

    // read：未知 uri / 缺 uri / uri 非 string → -32602。
    CHECK(ErrorCode(Respond(*session, ModernFrame("6", "resources/read", R"("uri":"mem://nope")"),
                            __LINE__)) == -32602);
    CHECK(ErrorCode(Respond(*session, ModernFrame("7", "resources/read"), __LINE__)) == -32602);
    CHECK(ErrorCode(Respond(*session, ModernFrame("8", "resources/read", R"("uri":1)"), __LINE__)) ==
          -32602);

    // discover 的能力只反映真实注册：有 resources，无 prompts。
    JsonValue discover = Respond(*session, ModernFrame("10", "server/discover"), __LINE__);
    JsonValue capabilities = At(At(discover, "result"), "capabilities");
    CHECK(capabilities.IsObject());
    CHECK(!At(capabilities, "resources").IsNull());
    CHECK(At(capabilities, "prompts").IsNull());
}

void TestResourceFailuresAreRedacted() {
    auto created = MakeServer();
    CHECK(created);
    auto server = created.value();
    ResourceDefinition definition;
    definition.uri = "mem://boom";
    definition.name = "boom";
    CHECK(server->AddResource(definition, [](const RequestContext&, const std::string&)
                                  -> result<ResourceResult> {
        throw std::runtime_error("secret-token-42");
    }));
    ResourceDefinition empty;
    empty.uri = "mem://empty";
    empty.name = "empty";
    CHECK(server->AddResource(empty, [](const RequestContext&, const std::string&) {
        return result<ResourceResult>::ok(ResourceResult{});
    }));
    ResourceDefinition failed;
    failed.uri = "mem://failed";
    failed.name = "failed";
    CHECK(server->AddResource(failed, [](const RequestContext&, const std::string&) {
        bbt::infra::Error error;
        error.code = bbt::infra::ErrorCode::NotFound;
        error.domain_code = "mcp.test.secret";
        error.message = "leaked detail";
        return result<ResourceResult>::err(error);
    }));
    CHECK(server->Start());
    auto session = CreateSession(server);

    for (const char* uri : {"mem://boom", "mem://empty", "mem://failed"}) {
        const std::string frame =
            ModernFrame("1", "resources/read", std::string(R"("uri":")") + uri + "\"");
        auto outcome = session->HandleFrame(frame);
        CHECK(outcome && outcome.value().has_response);
        const std::string& wire = outcome.value().response;
        CHECK(wire.find("secret-token-42") == std::string::npos);
        CHECK(wire.find("leaked detail") == std::string::npos);
        CHECK(wire.find("mcp.test.secret") == std::string::npos);
        JsonValue root = JsonValue::Parse(wire, 1U << 20, 32).value();
        CHECK(ErrorCode(root) == -32603);
        CHECK(Str(At(At(root, "error"), "message")) == "Internal error");
    }
}

void TestLegacyGateAndIsolation() {
    auto created = MakeServer();
    CHECK(created);
    auto server = created.value();
    ResourceDefinition definition;
    definition.uri = "mem://legacy";
    definition.name = "legacy";
    CHECK(server->AddResource(definition, [](const RequestContext&, const std::string& uri) {
        return TextResource(uri, "", "legacy-body");
    }));
    CHECK(server->Start());
    auto session = CreateSession(server);

    // 未握手：业务方法 → -32602（SDK runner.py 同 shape）；ping 豁免。
    JsonValue early = Respond(*session, LegacyFrame("1", "resources/list"), __LINE__);
    CHECK(ErrorCode(early) == -32602);
    JsonValue ping = Respond(*session, LegacyFrame("2", "ping"), __LINE__);
    CHECK(!At(ping, "result").IsNull());
    CHECK(ErrorCode(ping) == 0);

    // initialize → 业务方法可用；结果不含 modern 专有字段。
    JsonValue initialized = Respond(*session, kInitialize, __LINE__);
    auto negotiated = At(At(initialized, "result"), "protocolVersion");
    CHECK(Str(negotiated) == "2025-11-25");
    JsonValue list = Respond(*session, LegacyFrame("3", "resources/list"), __LINE__);
    JsonValue list_result = At(list, "result");
    CHECK(!HasModernFields(list_result));
    CHECK(At(list_result, "resources").Size() == 1);
    JsonValue read = Respond(*session, LegacyFrame("4", "resources/read", R"({"uri":"mem://legacy"})"),
                             __LINE__);
    CHECK(Found(Elem(At(At(read, "result"), "contents"), 0), "text") == "legacy-body");

    // 会话隔离：另一个 session 未握手，业务方法仍被门禁拒绝。
    auto other = CreateSession(server);
    CHECK(ErrorCode(Respond(*other, LegacyFrame("5", "resources/list"), __LINE__)) == -32602);

    // 未注册方法（modern 专有）在 legacy 表 → -32601，与握手状态无关。
    CHECK(ErrorCode(Respond(*session, LegacyFrame("6", "server/discover"), __LINE__)) == -32601);
}

void TestPrompts() {
    auto created = MakeServer(/*page_size=*/1);
    CHECK(created);
    auto server = created.value();

    PromptDefinition bad;
    bad.name = "p";
    bad.arguments = {PromptArgument{"a", "", true}, PromptArgument{"a", "", false}};
    auto handler = [](const RequestContext&, const JsonValue& arguments) {
        PromptResult produced;
        produced.description = "greeting";
        PromptMessage message;
        message.role = PromptRole::User;
        message.content = Content::Text("hi " + Str(At(arguments, "who")));
        produced.messages.push_back(message);
        PromptMessage assistant;
        assistant.role = PromptRole::Assistant;
        assistant.content = Content::Text("ok");
        produced.messages.push_back(assistant);
        return result<PromptResult>::ok(std::move(produced));
    };
    // 参数名重复 / 空 name / 空 handler → 整体拒绝，不留半条记录。
    CHECK(!server->AddPrompt(bad, handler));
    PromptDefinition unnamed;
    CHECK(!server->AddPrompt(unnamed, handler));
    PromptDefinition ok;
    ok.name = "greet";
    CHECK(!server->AddPrompt(ok, nullptr));
    ok.description = "greeting";
    ok.arguments = {PromptArgument{"who", "who to greet", true}};
    CHECK(server->AddPrompt(ok, handler));
    CHECK(!server->AddPrompt(ok, handler));  // 重复 name
    CHECK(server->AddPrompt(ok, handler).error().domain_code == "mcp.registry.duplicate");

    PromptDefinition second;
    second.name = "second";
    CHECK(server->AddPrompt(second, handler));
    CHECK(server->Start());
    auto session = CreateSession(server);

    // prompts/list：page_size=1 → 有序分页 + nextCursor；非法 cursor → -32602。
    JsonValue page1 = Respond(*session, ModernFrame("1", "prompts/list"), __LINE__);
    JsonValue result1 = At(page1, "result");
    auto prompts1 = At(result1, "prompts");
    CHECK(prompts1.IsArray() && prompts1.Size() == 1);
    CHECK(Str(At(Elem(prompts1, 0), "name")) == "greet");
    CHECK(Str(At(Elem(prompts1, 0), "description")) == "greeting");
    CHECK(Found(Elem(At(Elem(prompts1, 0), "arguments"), 0), "name") == "who");
    CHECK(Str(At(result1, "nextCursor")) == "1");
    JsonValue page2 = Respond(*session, ModernFrame("2", "prompts/list", R"("cursor":"1")"), __LINE__);
    CHECK(Found(Elem(At(At(page2, "result"), "prompts"), 0), "name") == "second");
    CHECK(At(At(page2, "result"), "nextCursor").IsNull());
    CHECK(ErrorCode(Respond(*session, ModernFrame("3", "prompts/list", R"("cursor":"9")"),
                            __LINE__)) == -32602);

    // prompts/get：正常 → role/content 结构由库构造；description 保留。
    JsonValue got = Respond(
        *session,
        ModernFrame("4", "prompts/get", R"("name":"greet","arguments":{"who":"alice"})"), __LINE__);
    JsonValue got_result = At(got, "result");
    CHECK(Str(At(got_result, "description")) == "greeting");
    CHECK(Str(At(got_result, "resultType")) == "complete");
    CHECK(At(got_result, "ttlMs").IsNull());  // prompts/get 非 CacheableResult
    auto messages = At(got_result, "messages");
    CHECK(messages.IsArray() && messages.Size() == 2);
    CHECK(Str(At(Elem(messages, 0), "role")) == "user");
    CHECK(Str(At(At(Elem(messages, 0), "content"), "type")) == "text");
    CHECK(Str(At(At(Elem(messages, 0), "content"), "text")) == "hi alice");
    CHECK(Str(At(Elem(messages, 1), "role")) == "assistant");

    // prompts/get：缺 required / 错类型 / 未知 name → -32602。
    CHECK(ErrorCode(Respond(*session, ModernFrame("5", "prompts/get", R"("name":"greet")"),
                            __LINE__)) == -32602);
    CHECK(ErrorCode(Respond(*session,
                            ModernFrame("6", "prompts/get",
                                        R"("name":"greet","arguments":{"who":1})"),
                            __LINE__)) == -32602);
    CHECK(ErrorCode(Respond(*session,
                            ModernFrame("7", "prompts/get",
                                        R"("name":"greet","arguments":{"who":"x","who":"y"})"),
                            __LINE__)) != 0);  // 重复键由 JSON 层拒（-32700），不进入业务
    CHECK(ErrorCode(Respond(*session, ModernFrame("8", "prompts/get", R"("name":"nope")"),
                            __LINE__)) == -32602);
    CHECK(ErrorCode(Respond(*session, ModernFrame("9", "prompts/get"), __LINE__)) == -32602);

    // 空 messages / handler 失败 → 固定内部错误（脱敏）。
    PromptDefinition empty_prompt;
    empty_prompt.name = "empty";
    // 注册发生在 Start 之后 → 被拒（快照冻结），故用另一个 server 验证空 messages。
    auto created2 = MakeServer();
    CHECK(created2);
    auto server2 = created2.value();
    CHECK(server2->AddPrompt(empty_prompt, [](const RequestContext&, const JsonValue&) {
        return result<PromptResult>::ok(PromptResult{});
    }));
    CHECK(server2->Start());
    auto session2 = CreateSession(server2);
    JsonValue empty_get =
        Respond(*session2, ModernFrame("1", "prompts/get", R"("name":"empty")"), __LINE__);
    CHECK(ErrorCode(empty_get) == -32603);
    CHECK(Str(At(At(empty_get, "error"), "message")) == "Internal error");
    // 注册失败不推进 server 的注册表（Start 后 AddPrompt 被拒）。
    CHECK(server->AddPrompt(empty_prompt, handler).error().domain_code == "mcp.registry.frozen");
}

void TestCallbackOutsideLockAndClose() {
    auto created = MakeServer();
    CHECK(created);
    auto server = created.value();
    std::shared_ptr<McpServer> captured = server;
    bool reentered = false;
    ResourceDefinition definition;
    definition.uri = "mem://close";
    definition.name = "close";
    // handler 在回调内调用 Close()：注册表/admission 锁不在回调期间持有，故不自等/不死锁。
    // 行为（M3 关闭域）：Close 一旦封口，**该回调自身的晚到结果也不再输出**（协作式退出
    // 契约；库不区分「谁关闭的」，也不强杀 stack）；封口后新请求被拒且不进入 handler。
    CHECK(server->AddResource(definition,
                              [captured, &reentered](const RequestContext&, const std::string& uri) {
                                  reentered = true;
                                  captured->Close();
                                  return TextResource(uri, "", "after-close");
                              }));
    CHECK(server->Start());
    auto session = CreateSession(server);
    auto read = session->HandleFrame(ModernFrame("1", "resources/read", R"("uri":"mem://close")"));
    CHECK(reentered);                 // 回调确实进入了（Close 不自等）
    CHECK(!read);                     // 封口后晚到结果不产生响应（核心拒绝，非响应）
    CHECK(read.error().code == bbt::infra::ErrorCode::Closed);
    CHECK(server->IsClosed());
    // Close 封口后：新请求不再进入核心（result::err，非响应），handler 不再被调用。
    reentered = false;
    auto after = session->HandleFrame(ModernFrame("2", "resources/read", R"("uri":"mem://close")"));
    CHECK(!after);
    CHECK(!reentered);
}

// M1：handler 返回的 PromptResult.description 是自由 string，非法 UTF-8 不得让 JsonValue
// 构造异常逃出帧入口；应为固定 -32603 + 原 id，且不泄漏异常文本/注入值。合法/空继续成功。
void TestPromptDescriptionUtf8Boundary() {
    auto created = MakeServer();
    CHECK(created);
    auto server = created.value();
    auto prompt_handler = [](const std::string& description) {
        PromptResult produced;
        produced.description = description;
        PromptMessage message;
        message.role = PromptRole::User;
        message.content = Content::Text("body");
        produced.messages.push_back(std::move(message));
        return result<PromptResult>::ok(std::move(produced));
    };
    PromptDefinition bad;
    bad.name = "bad-desc";
    CHECK(server->AddPrompt(bad, [&prompt_handler](const RequestContext&, const JsonValue&) {
        return prompt_handler(std::string("\xFF\xFE"));
    }));
    PromptDefinition good;
    good.name = "good-desc";
    CHECK(server->AddPrompt(good, [&prompt_handler](const RequestContext&, const JsonValue&) {
        return prompt_handler("问候 \xF0\x9F\x98\x80");
    }));
    PromptDefinition empty;
    empty.name = "empty-desc";
    CHECK(server->AddPrompt(empty, [&prompt_handler](const RequestContext&, const JsonValue&) {
        return prompt_handler("");
    }));
    CHECK(server->Start());
    auto session = CreateSession(server);

    // 非法 UTF-8：不抛异常、不返回成功响应，固定 -32603 + 原 id、不回显异常文本/坏字节。
    bool threw = false;
    std::string body;
    try {
        auto outcome = session->HandleFrame(
            ModernFrame("77", "prompts/get", R"("name":"bad-desc")"));
        CHECK(outcome);
        if (outcome) body = outcome.value().response;
    } catch (...) {
        threw = true;
    }
    CHECK(!threw);                                        // 异常不得逃出帧入口
    CHECK(body.find("UTF-8") == std::string::npos);       // 不回显异常文本
    CHECK(body.find('\xff') == std::string::npos);        // 不注入 handler 的坏字节
    CHECK(body.find("mcp.json") == std::string::npos);    // 不泄漏 domain_code
    auto parsed = JsonValue::Parse(body, 1U << 20, 32);
    CHECK(parsed);
    if (parsed) {
        JsonValue response = std::move(parsed).value();
        CHECK(ErrorCode(response) == -32603);
        // 请求 id 为 JSON 数字 77（ModernFrame 原样嵌入），响应必须按原类型回显。
        auto echoed_id = At(response, "id").AsInt64();
        CHECK(echoed_id && echoed_id.value() == 77);
        CHECK(Str(At(At(response, "error"), "message")) == "Internal error");
    }

    // 合法 UTF-8 description 与空 description 继续成功。
    JsonValue good_response =
        Respond(*session, ModernFrame("78", "prompts/get", R"("name":"good-desc")"), __LINE__);
    CHECK(Str(At(At(good_response, "result"), "description")) == "问候 \xF0\x9F\x98\x80");
    JsonValue empty_response =
        Respond(*session, ModernFrame("79", "prompts/get", R"("name":"empty-desc")"), __LINE__);
    CHECK(At(At(empty_response, "result"), "description").IsNull());  // 空 description 不写字段
    CHECK(Str(At(Elem(At(At(empty_response, "result"), "messages"), 0), "role")) == "user");
}

// M2：固定 schema `GetPromptRequestParams.arguments` = { [key: string]: string } ——
// **所有**键的值都必须是 string（与是否已声明无关）；非 string → -32602 且 handler 0 次；
// 未声明键的合法 string（含 Unicode/内嵌 NUL 键）可透传给 handler；required 语义保持。
void TestPromptArgumentsAllValuesString() {
    auto created = MakeServer();
    CHECK(created);
    auto server = created.value();
    int calls = 0;
    PromptDefinition declared;
    declared.name = "declared";
    declared.arguments = {PromptArgument{"who", "who", true}};
    CHECK(server->AddPrompt(declared, [&calls](const RequestContext&, const JsonValue& arguments) {
        ++calls;
        PromptResult produced;
        PromptMessage message;
        message.content = Content::Text(
            Str(At(arguments, "who")) + "|" + Str(At(arguments, "extra")) + "|" +
            Str(At(arguments, "中文键")) + "|" + Str(At(arguments, std::string("a\0b", 3))));
        produced.messages.push_back(std::move(message));
        return result<PromptResult>::ok(std::move(produced));
    }));
    PromptDefinition bare;
    bare.name = "bare";  // 未声明任何参数
    CHECK(server->AddPrompt(bare, [&calls](const RequestContext&, const JsonValue&) {
        ++calls;
        PromptResult produced;
        PromptMessage message;
        message.content = Content::Text("bare");
        produced.messages.push_back(std::move(message));
        return result<PromptResult>::ok(std::move(produced));
    }));
    CHECK(server->Start());
    auto session = CreateSession(server);

    // 未声明键的非 string 值（number/bool/null/array/object/float）→ -32602，handler 0 次。
    const char* bad_values[] = {"123", "true", "null", "[1]", "{\"k\":1}", "1.5", "-0.5"};
    int frame_id = 1;
    for (const char* value : bad_values) {
        std::string params = std::string(R"("name":"bare","arguments":{"undeclared":)") + value + "}";
        CHECK(ErrorCode(Respond(*session, ModernFrame(std::to_string(frame_id++), "prompts/get", params),
                                __LINE__)) == -32602);
        CHECK(calls == 0);
    }
    // 已声明键的非 string 值同样 -32602。
    CHECK(ErrorCode(Respond(*session,
                            ModernFrame("20", "prompts/get",
                                        R"("name":"declared","arguments":{"who":7})"),
                            __LINE__)) == -32602);
    CHECK(calls == 0);
    // required 语义保持：缺 required → -32602。
    CHECK(ErrorCode(Respond(*session, ModernFrame("21", "prompts/get", R"("name":"declared")"),
                            __LINE__)) == -32602);
    CHECK(calls == 0);
    // arguments 非 object → -32602。
    CHECK(ErrorCode(Respond(*session,
                            ModernFrame("22", "prompts/get", R"("name":"bare","arguments":"x")"),
                            __LINE__)) == -32602);
    CHECK(calls == 0);

    // 未声明键的合法 string（含 Unicode 键、内嵌 NUL 键）→ 成功且值原样到达 handler。
    JsonValue ok = Respond(
        *session,
        ModernFrame("23", "prompts/get",
                    R"("name":"declared","arguments":{"who":"alice","extra":"E","中文键":"U","a\u0000b":"N"})"),
        __LINE__);
    CHECK(calls == 1);
    CHECK(Str(At(At(Elem(At(At(ok, "result"), "messages"), 0), "content"), "text")) ==
          "alice|E|U|N");
}

// L1：分页上界不得溢出回绕（page_size=SIZE_MAX 时 cursor 不得回退/丢项/循环）；
// 正常 page_size 的 begin=0 / begin≠0 / 末尾页行为保持；无 template 时 templates/list 真实空。
void TestPaginationBoundaries() {
    auto created = MakeServer(/*page_size=*/static_cast<std::size_t>(-1), /*max_registrations=*/8);
    CHECK(created);
    auto server = created.value();
    for (const char* name : {"a", "b", "c"}) {
        ResourceDefinition definition;
        definition.uri = std::string("mem://") + name;
        definition.name = name;
        CHECK(server->AddResource(definition, [](const RequestContext&, const std::string& uri) {
            return TextResource(uri, "", "x");
        }));
    }
    PromptDefinition prompt;
    prompt.name = "p";
    CHECK(server->AddPrompt(prompt, [](const RequestContext&, const JsonValue&) {
        PromptResult produced;
        PromptMessage message;
        message.content = Content::Text("p");
        produced.messages.push_back(std::move(message));
        return result<PromptResult>::ok(std::move(produced));
    }));
    CHECK(server->Start());
    auto session = CreateSession(server);

    // page_size=SIZE_MAX：begin=0 → 全部 3 条且无 nextCursor（end=begin+min(MAX,total-begin)）。
    JsonValue all = Respond(*session, ModernFrame("1", "resources/list"), __LINE__);
    CHECK(At(At(all, "result"), "resources").Size() == 3);
    CHECK(At(At(all, "result"), "nextCursor").IsNull());
    // begin≠0（cursor="1"）→ 剩 2 条，cursor 不回退、不出现 nextCursor="0"。
    JsonValue tail = Respond(*session, ModernFrame("2", "resources/list", R"("cursor":"1")"), __LINE__);
    CHECK(At(At(tail, "result"), "resources").Size() == 2);
    CHECK(At(At(tail, "result"), "nextCursor").IsNull());
    CHECK(Found(Elem(At(At(tail, "result"), "resources"), 0), "name") == "b");
    // begin=total → 真实空数组、无 nextCursor。
    JsonValue empty_page =
        Respond(*session, ModernFrame("3", "resources/list", R"("cursor":"3")"), __LINE__);
    CHECK(At(At(empty_page, "result"), "resources").Size() == 0);
    CHECK(At(At(empty_page, "result"), "nextCursor").IsNull());
    // prompts/list 同一上界路径。
    JsonValue prompts = Respond(*session, ModernFrame("4", "prompts/list"), __LINE__);
    CHECK(At(At(prompts, "result"), "prompts").Size() == 1);
    CHECK(At(At(prompts, "result"), "nextCursor").IsNull());
    // 已有资源 capability、无 template 注册 → templates/list 仍真实空数组成功。
    JsonValue templates = Respond(*session, ModernFrame("5", "resources/templates/list"), __LINE__);
    CHECK(At(At(templates, "result"), "resourceTemplates").Size() == 0);

    // 正常 page_size=2：3 条 → 第 1 页 2 条 + nextCursor="2"；第 2 页 1 条、无 nextCursor。
    auto normal_created = MakeServer(/*page_size=*/2);
    CHECK(normal_created);
    auto normal = normal_created.value();
    for (const char* name : {"a", "b", "c"}) {
        ResourceDefinition definition;
        definition.uri = std::string("mem://") + name;
        definition.name = name;
        CHECK(normal->AddResource(definition, [](const RequestContext&, const std::string& uri) {
            return TextResource(uri, "", "x");
        }));
    }
    CHECK(normal->Start());
    auto normal_session = CreateSession(normal);
    JsonValue page1 = Respond(*normal_session, ModernFrame("1", "resources/list"), __LINE__);
    CHECK(At(At(page1, "result"), "resources").Size() == 2);
    CHECK(Str(At(At(page1, "result"), "nextCursor")) == "2");
    JsonValue page2 =
        Respond(*normal_session, ModernFrame("2", "resources/list", R"("cursor":"2")"), __LINE__);
    CHECK(At(At(page2, "result"), "resources").Size() == 1);
    CHECK(At(At(page2, "result"), "nextCursor").IsNull());
    CHECK(Found(Elem(At(At(page2, "result"), "resources"), 0), "name") == "c");
}

// M3：Close 是完成语义。在途 Close（barrier 内阻塞）后放行 handler：成功、业务失败、
// 异常三类晚到结果一律不产生响应；已进入的回调可观察到 Close（协作式退出，不自杀/不强杀）；
// 封口后尚未取得 callback-start 门的请求不进入 handler（0 次）。
void TestCloseSealsInFlightAndLateOutput() {
    struct Case {
        bool is_prompt;
        int result_mode;  // 0=成功 1=业务错误 2=抛异常
    };
    const Case cases[] = {{false, 0}, {true, 0}, {true, 1}, {true, 2}};
    for (const Case& item : cases) {
        auto created = MakeServer();
        CHECK(created);
        auto server = created.value();
        std::mutex mutex;
        std::condition_variable cv;
        bool entered = false;
        bool release = false;
        bool saw_cancel = false;
        int calls = 0;
        CancellationView saved;  // 栈外存活：handler 内 copy 出来的视图
        if (item.is_prompt) {
            PromptDefinition definition;
            definition.name = "slow";
            CHECK(server->AddPrompt(definition, [&](const RequestContext& context, const JsonValue&) {
                ++calls;
                saved = context.Cancellation();
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    entered = true;
                    cv.notify_all();
                    cv.wait(lock, [&] { return release; });
                }
                saw_cancel = context.Cancellation().Cancelled();
                if (item.result_mode == 2) throw std::runtime_error("late boom");
                if (item.result_mode == 1)
                    return result<PromptResult>::err(ResourceContents::Text("", "", "x").error());
                PromptResult produced;
                PromptMessage message;
                message.content = Content::Text("late");
                produced.messages.push_back(std::move(message));
                return result<PromptResult>::ok(std::move(produced));
            }));
        } else {
            ResourceDefinition definition;
            definition.uri = "mem://slow";
            definition.name = "slow";
            CHECK(server->AddResource(definition, [&](const RequestContext& context,
                                                      const std::string& uri) {
                ++calls;
                saved = context.Cancellation();
                {
                    std::unique_lock<std::mutex> lock(mutex);
                    entered = true;
                    cv.notify_all();
                    cv.wait(lock, [&] { return release; });
                }
                saw_cancel = context.Cancellation().Cancelled();
                return TextResource(uri, "", "late");
            }));
        }
        CHECK(server->Start());
        auto session = CreateSession(server);
        const std::string frame =
            item.is_prompt ? ModernFrame("1", "prompts/get", R"("name":"slow")")
                           : ModernFrame("1", "resources/read", R"("uri":"mem://slow")");
        bool worker_ok = true;
        // 观测必须从**实际 result** 无条件推导（不再用初值哨兵伪装观测）：
        // err → 核心未产生任何响应；只有 ok 时才读取 value()。
        bool worker_has_response = true;
        std::string worker_body = "sentinel";
        bool worker_closed_error = false;
        std::thread worker([&] {
            auto outcome = session->HandleFrame(frame);
            worker_ok = static_cast<bool>(outcome);
            worker_has_response = outcome && outcome.value().has_response;
            worker_body = outcome ? outcome.value().response : std::string();
            worker_closed_error =
                !outcome && outcome.error().code == bbt::infra::ErrorCode::Closed;
        });
        {
            std::unique_lock<std::mutex> lock(mutex);
            cv.wait(lock, [&] { return entered; });
        }
        server->Close();  // 回调在途时封口：Close 立即返回（不自等、不强杀 stack）
        CHECK(server->IsClosed());
        {
            std::lock_guard<std::mutex> lock(mutex);
            release = true;
        }
        cv.notify_all();
        worker.join();
        CHECK(calls == 1);
        CHECK(!worker_ok);             // 晚到结果（成功/业务错误/异常）→ 核心拒绝，无响应
        CHECK(!worker_has_response);
        CHECK(worker_body.empty());
        CHECK(worker_closed_error);    // 拒绝语义确为 Closed（有 result 才读 value）
        CHECK(saw_cancel);             // 已进入的回调观察到 Close（可协作式退出）
        CHECK(saved.Cancelled());      // copy 到栈外的只读视图真实由 Close 置位
        // 封口后新准入：不进入 handler、不产生响应。
        CHECK(!session->HandleFrame(frame));
        CHECK(calls == 1);
    }

    // 封口发生在「已准入、尚未取得 callback-start 门」之间（内测同步点确定性构造）：
    // handler 0 次、无响应。
    {
        auto created = MakeServer();
        CHECK(created);
        auto server = created.value();
        int calls = 0;
        PromptDefinition definition;
        definition.name = "gated";
        CHECK(server->AddPrompt(definition, [&calls](const RequestContext&, const JsonValue&) {
            ++calls;
            PromptResult produced;
            PromptMessage message;
            message.content = Content::Text("x");
            produced.messages.push_back(std::move(message));
            return result<PromptResult>::ok(std::move(produced));
        }));
        CHECK(server->Start());
        auto session = CreateSession(server);
        std::mutex mutex;
        std::condition_variable cv;
        bool parked = false;
        bool resume = false;
        session->SetTestPreCallbackHook([&] {
            std::unique_lock<std::mutex> lock(mutex);
            parked = true;
            cv.notify_all();
            cv.wait(lock, [&] { return resume; });
        });
        bool worker_ok = true;
        bool worker_has_response = true;
        bool worker_closed_error = false;
        std::thread worker([&] {
            auto outcome =
                session->HandleFrame(ModernFrame("1", "prompts/get", R"("name":"gated")"));
            worker_ok = static_cast<bool>(outcome);
            worker_has_response = outcome && outcome.value().has_response;
            worker_closed_error =
                !outcome && outcome.error().code == bbt::infra::ErrorCode::Closed;
        });
        {
            std::unique_lock<std::mutex> lock(mutex);
            cv.wait(lock, [&] { return parked; });
        }
        server->Close();  // 请求已准入、尚未取得 callback-start 门
        {
            std::lock_guard<std::mutex> lock(mutex);
            resume = true;
        }
        cv.notify_all();
        worker.join();
        CHECK(calls == 0);
        CHECK(!worker_ok);
        CHECK(!worker_has_response);
        CHECK(worker_closed_error);
        session->SetTestPreCallbackHook(nullptr);
    }
}

} // namespace

int main() {
    TestCapabilityGateMatrix();
    TestResourceRegistration();
    TestResourceListAndReadModern();
    TestResourceFailuresAreRedacted();
    TestLegacyGateAndIsolation();
    TestPrompts();
    TestCallbackOutsideLockAndClose();
    TestPromptDescriptionUtf8Boundary();
    TestPromptArgumentsAllValuesString();
    TestPaginationBoundaries();
    TestCloseSealsInFlightAndLateOutput();
    std::printf("mcp.registry: %d checks, %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
