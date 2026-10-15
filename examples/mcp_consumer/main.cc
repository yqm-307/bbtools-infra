// 4-2A：MCP 独立源码 consumer（S24 source-build 形态，仓外视角公共契约验证）。
//
// 这是什么：一个**不启动 Scheduler** 的离线验证程序，从公共头（include/bbt/infra/mcp/）
// 消费 bbt::infra_mcp_core 可独立消费的契约面：
//   - ServerOptions 严格校验（空名/零配额在 Create 即拒，不产生半启动对象）；
//   - 注册面：AddResource/AddPrompt 的合法路径与坏路径（重复 uri/name、空 uri、
//     空 handler、参数名重复），坏路径必须真实拒绝，不产生伪成功；
//   - 生命周期：Start 冻结快照（重复 Start / Start 后注册拒绝）、
//     PublishResourceChanged 的未注册拒绝与已注册 ok、Close 幂等封口
//     （Close 后注册/Start 拒绝，Closed/InvalidContext 区分）；
//   - 自有类型原语：JsonValue 严格 Parse（语法/重复键/深度/字节上限）与
//     Serialize 往返、ResourceContents 构造即校验（空 uri / 非 base64 拒）、
//     Content、InputRequired 双空拒、RequestId owned 原值往返、
//     CancellationView 取消置位可观察、RequestContext 只读回读。
//
// 未覆盖（如实声明）：wire 请求分派与 transport 装配。McpSession::HandleFrame 是
// transport seam（头文件注释明确「不属公共契约」），不在公共头暴露方法，仓外
// consumer 无法也不应经它验证；该面由仓内 mcp.* 测试与 4-2B SDK 互通覆盖。
// tool 注册面同样未覆盖：公共面当前未提供 AddTool（tools 切片未实现）。
//
// 全部检查通过输出 `consumer: ALL OK` 并以 0 退出；任一检查失败输出 FAIL 行、
// 以 1 退出（坏路径检查失败 = 契约破坏，绝不静默放行）。

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>
#include <bbt/infra/mcp/McpServer.hpp>
#include <bbt/infra/mcp/McpTypes.hpp>

namespace {

int g_checks = 0;
int g_failures = 0;

void Check(bool ok, int line, const std::string& what) {
    ++g_checks;
    if (ok) {
        std::printf("ok %02d %s\n", g_checks, what.c_str());
    } else {
        ++g_failures;
        std::printf("FAIL %02d (line %d) %s\n", g_checks, line, what.c_str());
    }
}

#define CHECK(cond, what) Check(static_cast<bool>(cond), __LINE__, what)

using namespace bbt::infra;
using namespace bbt::infra::mcp;

// 确定性 echo resource/prompt handler（返回值由库校验，consumer 只交自有类型）。
result<ResourceResult> EchoResource(const RequestContext&, const std::string& uri) {
    auto content = ResourceContents::Text(uri, "text/plain", "consumer:" + uri);
    if (!content) return result<ResourceResult>::err(content.error());
    ResourceResult out;
    out.contents.push_back(content.value());
    return result<ResourceResult>::ok(std::move(out));
}

result<PromptResult> EchoPrompt(const RequestContext&, const JsonValue& arguments) {
    std::string text = "默认";
    if (arguments.IsObject()) {
        auto found = arguments.Find("text");
        if (found && found.value().IsString()) {
            auto as_string = found.value().AsString();
            if (as_string) text = as_string.value();
        }
    }
    PromptResult out;
    bbt::infra::mcp::PromptMessage message;
    message.role    = PromptRole::User;
    message.content = Content::Text(text);
    out.messages.push_back(std::move(message));
    return result<PromptResult>::ok(std::move(out));
}

// ---- 分组 1：Create 严格校验 -------------------------------------------------------

void CheckCreateValidation() {
    ServerOptions empty_name;
    empty_name.server_name = "";
    auto r1 = McpServer::Create(empty_name);
    CHECK(!r1, "Create 拒绝空 server_name");

    ServerOptions zero_quota;
    zero_quota.max_inflight_requests = 0;
    auto r2 = McpServer::Create(zero_quota);
    CHECK(!r2, "Create 拒绝 max_inflight_requests=0");

    ServerOptions zero_page;
    zero_page.page_size = 0;
    auto r3 = McpServer::Create(zero_page);
    CHECK(!r3, "Create 拒绝 page_size=0");

    auto ok = McpServer::Create(ServerOptions{});
    CHECK(ok, "Create 接受合法默认配置");
    const auto& versions = ok.value()->AdvertisedVersions();
    CHECK(versions.size() == 3, "AdvertisedVersions 为 3 个 revision");
    bool has_modern = false;
    for (const auto& v : versions)
        if (v == kProtocolVersion2026_07_28) has_modern = true;
    CHECK(has_modern, "supported 含 modern 2026-07-28");
}

// ---- 分组 2：注册面合法/坏路径 -----------------------------------------------------

void CheckRegistrationFace() {
    auto server = McpServer::Create(ServerOptions{}).value();

    ResourceDefinition resource;
    resource.uri   = "memo://consumer";
    resource.name  = "consumer";
    resource.mime_type = "text/plain";
    CHECK(server->AddResource(resource, EchoResource), "AddResource 合法注册");

    ResourceDefinition duplicate = resource;
    CHECK(!server->AddResource(duplicate, EchoResource), "AddResource 拒绝重复 uri");

    ResourceDefinition empty_uri;
    empty_uri.uri  = "";
    empty_uri.name = "empty";
    CHECK(!server->AddResource(empty_uri, EchoResource), "AddResource 拒绝空 uri");

    ResourceDefinition no_handler;
    no_handler.uri  = "memo://nohandler";
    no_handler.name = "nohandler";
    CHECK(!server->AddResource(no_handler, nullptr), "AddResource 拒绝空 handler");

    PromptDefinition prompt;
    prompt.name = "echo";
    PromptArgument argument;
    argument.name = "text";
    prompt.arguments.push_back(argument);
    CHECK(server->AddPrompt(prompt, EchoPrompt), "AddPrompt 合法注册");

    PromptDefinition duplicate_name = prompt;
    CHECK(!server->AddPrompt(duplicate_name, EchoPrompt), "AddPrompt 拒绝重复 name");

    PromptDefinition dup_args;
    dup_args.name = "dupargs";
    PromptArgument a1;
    a1.name = "text";
    PromptArgument a2;
    a2.name = "text";
    dup_args.arguments.push_back(a1);
    dup_args.arguments.push_back(a2);
    CHECK(!server->AddPrompt(dup_args, EchoPrompt), "AddPrompt 拒绝重复参数名");
}

// ---- 分组 3：生命周期（Start 快照 / Publish / Close 封口）--------------------------

void CheckLifecycle() {
    auto server = McpServer::Create(ServerOptions{}).value();
    ResourceDefinition resource;
    resource.uri  = "memo://life";
    resource.name = "life";
    CHECK(server->AddResource(resource, EchoResource), "生命周期：Start 前注册成功");

    CHECK(server->Start(), "Start 首次成功");
    CHECK(!server->Start(), "Start 重复拒绝（快照已冻结）");

    ResourceDefinition late;
    late.uri  = "memo://late";
    late.name = "late";
    auto after_start = server->AddResource(late, EchoResource);
    CHECK(!after_start, "Start 后注册拒绝（快照不可变）");
    CHECK(after_start.error().code == ErrorCode::InvalidContext,
          "Start 后注册错误码为 InvalidContext");

    auto unknown = server->PublishResourceChanged("memo://unregistered");
    CHECK(!unknown, "PublishResourceChanged 拒绝未注册 uri");
    auto known = server->PublishResourceChanged("memo://life");
    CHECK(known && known.value() == 0, "已注册 uri 发布 ok（无订阅投递数为 0）");

    server->Close();
    auto after_close = server->AddResource(late, EchoResource);
    CHECK(!after_close, "Close 后注册拒绝");
    CHECK(after_close.error().code == ErrorCode::Closed, "Close 后注册错误码为 Closed");
    CHECK(!server->Start(), "Close 后 Start 拒绝（不重开）");
    server->Close();  // 幂等：第二次 Close 不产生可观察副作用
    CHECK(server->IsClosed(), "Close 幂等且 IsClosed 置位");
}

// ---- 分组 4：JsonValue 严格解析与往返 ----------------------------------------------

void CheckJsonValueContract() {
    const std::string frame =
        R"({"jsonrpc":"2.0","id":1,"method":"initialize",)"
        R"("params":{"protocolVersion":"2026-07-28","capabilities":{},)"
        R"("clientInfo":{"name":"mcp_consumer","version":"0.0.1"}}})";
    auto parsed = JsonValue::Parse(frame, 1U << 20, 32);
    CHECK(parsed, "Parse 接受合法 initialize 帧");
    auto jsonrpc = parsed.value().Find("jsonrpc");
    CHECK(jsonrpc && jsonrpc.value().AsString() && jsonrpc.value().AsString().value() == "2.0",
          "initialize 帧 jsonrpc 字段形状正确");
    auto id = parsed.value().Find("id");
    CHECK(id && id.value().IsInt64() && id.value().AsInt64().value() == 1,
          "initialize 帧 id 原值保持");

    auto bad_syntax = JsonValue::Parse("{\"a\":", 1U << 20, 32);
    CHECK(!bad_syntax, "Parse 拒绝语法错误");
    CHECK(bad_syntax.error().code == ErrorCode::ProtocolError,
          "语法错误码为 ProtocolError");

    auto dup_key = JsonValue::Parse(R"({"a":1,"a":2})", 1U << 20, 32);
    CHECK(!dup_key, "Parse 拒绝重复键");

    std::string deep;
    for (int i = 0; i < 64; ++i) deep += "[";
    for (int i = 0; i < 64; ++i) deep += "]";
    auto too_deep = JsonValue::Parse(deep, 1U << 20, 32);
    CHECK(!too_deep, "Parse 拒绝超深嵌套（max_depth=32）");

    auto too_big = JsonValue::Parse(R"({"a":1})", 4, 32);
    CHECK(!too_big, "Parse 拒绝超 max_bytes");

    auto roundtrip = JsonValue::Parse(frame, 1U << 20, 32);
    auto reparse = JsonValue::Parse(roundtrip.value().Serialize(), 1U << 20, 32);
    CHECK(reparse && reparse.value().Serialize() == roundtrip.value().Serialize(),
          "Serialize → Parse 往返一致");
}

// ---- 分组 5：自有类型原语 ----------------------------------------------------------

void CheckTypePrimitives() {
    auto empty_uri = ResourceContents::Text("", "text/plain", "x");
    CHECK(!empty_uri, "ResourceContents::Text 拒绝空 uri");
    auto bad_blob = ResourceContents::Blob("memo://blob", "application/octet-stream",
                                           "not*base64!!");
    CHECK(!bad_blob, "ResourceContents::Blob 拒绝非规范 base64");
    auto good_text = ResourceContents::Text("memo://t", "text/plain", "hello");
    CHECK(good_text && good_text.value().IsText() && good_text.value().Text() == "hello",
          "ResourceContents::Text 合法构造");

    Content empty_block;
    CHECK(empty_block.TextValue().empty(), "Content 默认为空 text 块");
    auto text_block = Content::Text("你好");
    CHECK(text_block.TextValue() == "你好", "Content::Text 合法构造");

    auto empty_input = InputRequired::Create({}, std::nullopt);
    CHECK(!empty_input, "InputRequired 拒绝双空（MUST 至少其一非空）");
    std::map<std::string, ClientInputRequest> one{
        {"k", ClientInputRequest{"roots/list", JsonValue::Object()}}};
    auto ok_input = InputRequired::Create(std::move(one), std::nullopt);
    CHECK(ok_input, "InputRequired 接受单项 inputRequests");

    RequestId as_int(std::int64_t{-7});
    auto id_json = as_int.ToJson();
    CHECK(id_json.IsInt64() && id_json.AsInt64().value() == -7, "RequestId 负整数原值回包");
    RequestId as_string(std::string{"req-1"});
    CHECK(as_string.ToJson().AsString() && as_string.ToJson().AsString().value() == "req-1",
          "RequestId 字符串原值回包");
    CHECK(as_int != as_string, "RequestId 不同类型不相等");
    CHECK(!as_int.Key().empty(), "RequestId Key 非空");

    auto flag = std::make_shared<std::atomic<bool>>(false);
    CancellationView view{flag};
    CHECK(!view.Cancelled(), "CancellationView 未置位时不取消");
    flag->store(true);
    CHECK(view.Cancelled(), "CancellationView 置位后可观察取消");

    RequestContext context{as_int, std::string{kProtocolVersion2026_07_28},
                           JsonValue::Object(), view};
    CHECK(context.Id() == as_int, "RequestContext 回读请求 id");
    CHECK(context.ProtocolVersion() == kProtocolVersion2026_07_28,
          "RequestContext 回读协议版本");
    CHECK(context.ClientCapabilities().IsObject(), "RequestContext 回读 capabilities");
    CHECK(!context.HasDeadline(), "未关联期限时 HasDeadline 为 false");
    CHECK(context.InputResponses().empty() && context.RequestState() == std::nullopt,
          "legacy 请求 inputResponses/requestState 恒空");
}

} // namespace

int main() {
    CheckCreateValidation();
    CheckRegistrationFace();
    CheckLifecycle();
    CheckJsonValueContract();
    CheckTypePrimitives();

    if (g_failures == 0) {
        std::printf("consumer: ALL OK (%d checks)\n", g_checks);
        return 0;
    }
    std::printf("consumer: FAILED (%d/%d checks failed)\n", g_failures, g_checks);
    return 1;
}
