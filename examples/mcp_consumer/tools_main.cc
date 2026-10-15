// 2-6：tools×Schema 公共消费者验证（仓外视角，只 include 公共头）。
//
// 两种模式（同一公共面、无 src/ 头、无内部 CreateSession/HandleFrame 捷径）：
//
// 1) 默认（注册面）：显式链接 bbt::infra_mcp_core + bbt_infra_mcp_schema，证明——
//    - EnableToolsSchemaValidation 公共声明 → 私有 adapter 实现，不经 src/ 即可启用
//      真实 Schema 校验；
//    - AddTool 的 Schema 编译校验真实发生（坏 schema / 外部 ref / 未知类型注册被
//      fail-closed 拒绝；core 闭包自身不含 Schema/ICU —— 由 mcp_consumer（仅 core）
//      与 ldd 证据佐证）；
//    - 生命周期：Start 冻结（Start 后 AddTool InvalidContext）、Close 后 Closed。
//
// 2) --stdio（真实公共 wire）：经公共 McpStdioTransport::Start（fd 0/1，独占
//    非 socket pipe），由外部 stdlib Python 客户端以真实 JSON-RPC 行帧驱动：
//    initialize → tools/list（inputSchema/outputSchema 下发）→ tools/call echo
//    （输入 schema 校验、handler 计数进 structuredContent）、坏输入 -32602 且
//    handler 不进入、handler 异常 → 固定 -32603（脱敏）、业务 MRTR
//    （input_required → 新 id + inputResponses/requestState 重试续走）。
//    stdout 只有协议帧；诊断只写 stderr（R8 分离）。stdin EOF 自收口并回传
//    StdioCloseEvidence（协程汇合/队列封口/O_NONBLOCK 还原）。
//
// 全部通过输出 `consumer-tools: ALL OK` 退出 0；任一失败 FAIL 行退出 1。
// stdio 模式退出码即证据校验结果（0 = EOF 干净收口）。

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <string>
#include <thread>
#include <utility>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>
#include <bbt/infra/mcp/McpServer.hpp>
#include <bbt/infra/mcp/McpStdioTransport.hpp>
#include <bbt/infra/mcp/McpTypes.hpp>

#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

namespace {

using namespace bbt::infra::mcp;

int g_checks = 0;
int g_failures = 0;

// tools/call handler 真实进入计数（stdio wire 模式的协议断言面：坏输入必须 0 进入）。
std::atomic<int> g_handler_calls{0};

void Check(bool ok, int line, const std::string& what) {
    ++g_checks;
    if (ok) {
        std::printf("ok %02d %s\n", g_checks, what.c_str());
    } else {
        ++g_failures;
        std::printf("FAIL %02d (line %d) %s\n", g_checks, line, what.c_str());
    }
}

JsonValue ParseOr(const char* text) {
    auto parsed = JsonValue::Parse(text, 1U << 20, 32);
    if (!parsed) {
        std::fprintf(stderr, "CONSUMER_FAIL schema literal invalid\n");
        std::exit(2);
    }
    return std::move(parsed).value();
}

// echo 工具真实 handler：回显 message，structuredContent 携带 handler 计数
//（对应已声明 output schema {"echo":string,"count":integer}，wire 侧断言两者）。
bbt::infra::result<HandlerResult<ToolResult>> EchoHandler(const RequestContext&,
                                                          JsonValue arguments) {
    ++g_handler_calls;
    std::string message;
    if (auto found = arguments.Find("message"))
        if (auto text = found.value().AsString()) message = text.value();
    ToolResult produced;
    produced.content.push_back(Content::Text(message));
    JsonValue structured = JsonValue::Object();
    structured.Set("echo", JsonValue(message));
    structured.Set("count", JsonValue(g_handler_calls.load()));
    produced.structured_content = std::move(structured);
    return bbt::infra::result<HandlerResult<ToolResult>>::ok(
        HandlerResult<ToolResult>::Completed(std::move(produced)));
}

// boom 工具：handler 抛异常 → 库固定 -32603（异常文本脱敏，wire 侧断言不回显）。
bbt::infra::result<HandlerResult<ToolResult>> BoomHandler(const RequestContext&,
                                                          JsonValue) {
    ++g_handler_calls;
    throw std::runtime_error("secret boom detail");
}

// confirm 工具：业务 MRTR —— 首调 Deferred(InputRequired)（sampling 请求 + requestState）；
// 携带 inputResponses/requestState 的新 id 重试 → Completed("resumed")。
bbt::infra::result<HandlerResult<ToolResult>> ConfirmHandler(const RequestContext& context,
                                                             JsonValue) {
    ++g_handler_calls;
    if (!context.InputResponses().empty()) {
        ToolResult produced;
        produced.content.push_back(Content::Text("resumed"));
        return bbt::infra::result<HandlerResult<ToolResult>>::ok(
            HandlerResult<ToolResult>::Completed(std::move(produced)));
    }
    std::map<std::string, ClientInputRequest> requests;
    requests.emplace("srv1",
                     ClientInputRequest{"sampling/createMessage", JsonValue::Object()});
    auto required = InputRequired::Create(std::move(requests), std::string("state-1"));
    if (!required)
        return bbt::infra::result<HandlerResult<ToolResult>>::err(required.error());
    return bbt::infra::result<HandlerResult<ToolResult>>::ok(
        HandlerResult<ToolResult>::Deferred(std::move(required).value()));
}

bool RegisterTool(McpServer& server, const char* name, const char* input_schema,
                  const char* output_schema_json, ToolHandler handler) {
    ToolDefinition def;
    def.name = name;
    def.description = "public consumer tool";
    def.input_schema = ParseOr(input_schema);
    if (output_schema_json != nullptr) def.output_schema = ParseOr(output_schema_json);
    auto added = server.AddTool(std::move(def), std::move(handler));
    if (!added) {
        std::fprintf(stderr, "CONSUMER_FAIL AddTool %s\n", name);
        return false;
    }
    return true;
}

// ---- 模式 1：注册面检查（原公共组合契约，Result 显式转换口径） ------------------

int RunRegistrationChecks() {
    using namespace bbt::infra::mcp;

    auto server = McpServer::Create(ServerOptions{});
    Check(static_cast<bool>(server), 51, "McpServer::Create");

    // 未接通 Schema → AddTool fail-closed（不注册未校验工具）。
    {
        auto plain = McpServer::Create(ServerOptions{});
        ToolDefinition def;
        def.name = "echo";
        def.input_schema = ParseOr(
            R"({"type":"object","properties":{"message":{"type":"string"}},"required":["message"]})");
        auto refused = plain.value()->AddTool(std::move(def), [](const RequestContext&, JsonValue) {
            return bbt::infra::result<HandlerResult<ToolResult>>::ok(
                HandlerResult<ToolResult>::Completed(ToolResult{}));
        });
        Check(!refused, 68, "AddTool without schema enablement is rejected (fail-closed)");
    }

    // 接通（真实编译 validator）：坏 schema / 外部 ref 拒绝；合法注册成功。
    auto enabled = EnableToolsSchemaValidation(*server.value());
    Check(static_cast<bool>(enabled), 74, "EnableToolsSchemaValidation");
    Check(!EnableToolsSchemaValidation(*server.value()), 75, "double enable rejected");

    ToolDefinition bad;
    bad.name = "bad";
    bad.input_schema = ParseOr(R"({"type":"strng"})");  // 未知 type → 编译拒
    auto bad_added = server.value()->AddTool(
        std::move(bad), [](const RequestContext&, JsonValue) {
            return bbt::infra::result<HandlerResult<ToolResult>>::ok(
                HandlerResult<ToolResult>::Completed(ToolResult{}));
        });
    Check(!bad_added, 85, "invalid schema rejected at registration");

    ToolDefinition external;
    external.name = "external";
    external.input_schema = ParseOr(
        R"({"type":"object","properties":{"x":{"$ref":"https://example.invalid/s"}}})");
    auto external_added = server.value()->AddTool(
        std::move(external), [](const RequestContext&, JsonValue) {
            return bbt::infra::result<HandlerResult<ToolResult>>::ok(
                HandlerResult<ToolResult>::Completed(ToolResult{}));
        });
    Check(!external_added, 95, "external $ref rejected at registration (no network)");

    ToolDefinition echo;
    echo.name = "echo";
    echo.title = "Echo";
    echo.description = "public consumer echo tool";
    echo.input_schema = ParseOr(
        R"({"type":"object","properties":{"message":{"type":"string"}},"required":["message"]})");
    auto added = server.value()->AddTool(std::move(echo), [](const RequestContext&, JsonValue) {
        return bbt::infra::result<HandlerResult<ToolResult>>::ok(
            HandlerResult<ToolResult>::Completed(ToolResult{}));
    });
    Check(static_cast<bool>(added), 106, "echo tool registered with schema-backed compile");

    Check(static_cast<bool>(server.value()->Start()), 108, "Start freezes registration snapshot");
    ToolDefinition late;
    late.name = "late";
    late.input_schema = ParseOr(R"({"type":"object"})");
    Check(!server.value()->AddTool(std::move(late), [](const RequestContext&, JsonValue) {
        return bbt::infra::result<HandlerResult<ToolResult>>::ok(
            HandlerResult<ToolResult>::Completed(ToolResult{}));
    }), 115, "AddTool after Start rejected (frozen)");

    server.value()->Close();
    ToolDefinition after_close;
    after_close.name = "after_close";
    after_close.input_schema = ParseOr(R"({"type":"object"})");
    Check(!server.value()->AddTool(std::move(after_close), [](const RequestContext&, JsonValue) {
        return bbt::infra::result<HandlerResult<ToolResult>>::ok(
            HandlerResult<ToolResult>::Completed(ToolResult{}));
    }), 124, "AddTool after Close rejected (Closed)");

    std::printf("consumer-tools: %s (%d checks / %d failures)\n",
                g_failures == 0 ? "ALL OK" : "FAILED", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}

// ---- 模式 2：--stdio 真实公共 wire（fd 0/1；EOF 自收口；证据回传 stderr）-------

int RunStdioServer() {
    auto server = McpServer::Create(ServerOptions{});
    if (!server) {
        std::fprintf(stderr, "CONSUMER_FAIL create\n");
        return 2;
    }
    auto& raw = *server.value();
    if (!EnableToolsSchemaValidation(raw)) {
        std::fprintf(stderr, "CONSUMER_FAIL enable schema\n");
        return 2;
    }
    const char* kEchoInput =
        R"({"type":"object","properties":{"message":{"type":"string"}},"required":["message"],"additionalProperties":false})";
    const char* kEchoOutput =
        R"({"type":"object","properties":{"echo":{"type":"string"},"count":{"type":"integer"}},"required":["echo","count"],"additionalProperties":false})";
    if (!RegisterTool(raw, "echo", kEchoInput, kEchoOutput, EchoHandler) ||
        !RegisterTool(raw, "boom", R"({"type":"object"})", nullptr, BoomHandler) ||
        !RegisterTool(raw, "confirm", R"({"type":"object"})", nullptr, ConfirmHandler)) {
        return 2;
    }
    if (!raw.Start()) {
        std::fprintf(stderr, "CONSUMER_FAIL start\n");
        return 2;
    }

    auto started = McpStdioTransport::Start(server.value(), StdioOptions{});
    if (!started) {
        std::fprintf(stderr, "CONSUMER_FAIL stdio start\n");
        return 2;
    }
    auto transport = std::move(started).value();

    // stdin EOF → transport 自行走同一收口序列；兜底 deadline 保证有界退出。
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    while (!transport->IsClosed()) {
        if (std::chrono::steady_clock::now() > deadline) {
            std::fprintf(stderr, "CONSUMER_FAIL eof timeout\n");
            transport->Close();
            return 3;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    transport->Close();  // 幂等：取首次收口当刻快照
    const auto e = transport->LastCloseEvidence();
    std::fprintf(stderr,
                 "CONSUMER_EVIDENCE reader_joined=%d writer_joined=%d eof=%d "
                 "sink_mounted=%d waiters_empty=%d nonblock_restored=%d "
                 "pending_bytes=%zu dropped=%zu peak=%zu handler_calls=%d\n",
                 e.reader_joined ? 1 : 0, e.writer_joined ? 1 : 0, e.eof ? 1 : 0,
                 e.sink_mounted ? 1 : 0, e.waiters_empty ? 1 : 0,
                 e.nonblock_restored ? 1 : 0, e.pending_bytes, e.dropped_frames,
                 e.peak_pending_bytes, g_handler_calls.load());
    const bool ok = e.reader_joined && e.writer_joined && e.eof && !e.sink_mounted &&
                    e.waiters_empty && e.nonblock_restored && e.pending_bytes == 0 &&
                    e.dropped_frames == 0;
    return ok ? 0 : 4;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc > 1 && std::string(argv[1]) == "--stdio") {
        // 共享协程运行时（与仓内 stdio transport 测试同一生命周期口径）。
        auto* config = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
        config->m_cfg_static_thread_num = 2;
        config->m_cfg_stack_size = 1024 * 256;  // 分派协程要跑大帧解析
        bbt::coroutine::detail::Scheduler::GetInstance()->Start(
            bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);
        return RunStdioServer();
    }
    return RunRegistrationChecks();
}
