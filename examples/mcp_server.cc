// 4-2A：MCP 可运行示例 server（bbtools-infra mcp-server-foundation 交付线）。
//
// 这是什么：一个最小但真实可跑的 MCP server 示例，演示 bbtools-infra 当前公共面
// （include/bbt/infra/mcp/）的完整生命周期与两种 transport：
//
//   GlobalConfig/Scheduler 启动共享协程运行时
//   → McpServer::Create → 注册三原语（tools 需 Schema 选件）→ Start 冻结快照
//   → McpStdioTransport::Start（--stdio，逐行 JSON 帧，协议帧走 stdout、诊断走 stderr）
//     或 McpHttpTransport::Listen（--http <port>，单 POST endpoint，0=动态端口）
//   → 主线程等 stdin EOF（stdio）或 SIGINT/SIGTERM
//   → 按契约同步 Close：transport->Close()（幂等、任意线程、返回即物理收口）
//     → server->Close()（core 生命周期由业务决定，transport 不代管）
//   → 进程退出；不依赖协程栈展开与 Scheduler::Stop（运行时按进程寿命存在）。
//
// 怎么跑（构建目录 B 按仓库说明配置后）：
//   $ ./Example_mcp_server --stdio          < frames.ndjson      # stdin 用管道接帧
//   $ ./Example_mcp_server --http 0                              # stderr 打印实际端口
// 注意：stdio 模式要求 stdin/stdout 是管道（transport 按 S_ISFIFO 校验，独占使用），
// 终端直连会启动失败；协议帧只写 stdout，本文件的日志只写 stderr。
//
// Schema 选件 ON：真实 echo tool + resource + prompt；OFF：只注册 resource/prompt，
// 不链接 Schema/ICU，不宣告 tools。ON 的依赖或注册失败直接非零退出，不使用 stub。

#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <utility>
#include <unistd.h>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>
#include <bbt/infra/mcp/McpHttpTransport.hpp>
#include <bbt/infra/mcp/McpServer.hpp>
#include <bbt/infra/mcp/McpStdioTransport.hpp>
#include <bbt/infra/mcp/McpTypes.hpp>

#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

namespace {

// 信号只置位，Close 留给主线程在正常上下文按契约同步执行（handler 内不做
// 非异步安全调用，也不依赖协程栈展开）。
volatile std::sig_atomic_t g_stop = 0;

void OnStopSignal(int) { g_stop = 1; }

// --- 确定性业务 handler：echo，无外部依赖、无随机性 --------------------------------

#ifdef BBT_MCP_SCHEMA_ADAPTER
std::atomic<int> g_tool_calls{0};

bbt::infra::result<bbt::infra::mcp::HandlerResult<bbt::infra::mcp::ToolResult>> EchoTool(
    const bbt::infra::mcp::RequestContext&, bbt::infra::mcp::JsonValue arguments) {
    using namespace bbt::infra::mcp;
    using Result = bbt::infra::result<HandlerResult<ToolResult>>;
    auto found = arguments.Find("message");
    if (!found) return Result::err(found.error());
    auto message = found.value().AsString();
    if (!message) return Result::err(message.error());
    ToolResult out;
    out.content.push_back(Content::Text(message.value()));
    auto structured = JsonValue::Object();
    // 新 object 与常量 ASCII 键；值已由输入 Schema 校验。
    structured.Set("echo", JsonValue(message.value()));
    structured.Set("count", JsonValue(++g_tool_calls));
    out.structured_content = std::move(structured);
    return Result::ok(HandlerResult<ToolResult>::Completed(std::move(out)));
}
#endif

// resources/read：回显注册 uri（与注册 uri 相同，库已校验）。
bbt::infra::result<bbt::infra::mcp::ResourceResult> EchoResource(
    const bbt::infra::mcp::RequestContext&, const std::string& uri) {
    auto content =
        bbt::infra::mcp::ResourceContents::Text(uri, "text/plain", "echo:" + uri);
    if (!content)
        return bbt::infra::result<bbt::infra::mcp::ResourceResult>::err(content.error());
    bbt::infra::mcp::ResourceResult out;
    out.contents.push_back(content.value());
    return bbt::infra::result<bbt::infra::mcp::ResourceResult>::ok(std::move(out));
}

// prompts/get：取 params.arguments.text（缺省「你好」），回显到单条 user 消息。
bbt::infra::result<bbt::infra::mcp::PromptResult> EchoPrompt(
    const bbt::infra::mcp::RequestContext&, const bbt::infra::mcp::JsonValue& arguments) {
    std::string text = "你好";
    if (arguments.IsObject()) {
        auto found = arguments.Find("text");
        if (found && found.value().IsString()) {
            auto as_string = found.value().AsString();
            if (as_string) text = as_string.value();
        }
    }
    bbt::infra::mcp::PromptResult out;
    out.description = "echo 提示词：把 text 回显为一条 user 消息";
    bbt::infra::mcp::PromptMessage message;
    message.role    = bbt::infra::mcp::PromptRole::User;
    message.content = bbt::infra::mcp::Content::Text("echo:" + text);
    out.messages.push_back(std::move(message));
    return bbt::infra::result<bbt::infra::mcp::PromptResult>::ok(std::move(out));
}

void PrintUsage(std::FILE* out) {
    std::fprintf(out,
                 "用法：Example_mcp_server [--stdio | --http <port>]\n"
                 "  --stdio       逐行 JSON 帧（stdin/stdout 必须是管道）\n"
                 "  --http <port> Streamable HTTP，单 POST endpoint /mcp；0=动态端口\n");
}

} // namespace

int main(int argc, char** argv) {
    enum class Mode { kStdio, kHttp };
    Mode mode          = Mode::kStdio;
    std::uint16_t port = 0;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--stdio") {
            mode = Mode::kStdio;
        } else if (arg == "--http") {
            if (i + 1 >= argc) {
                PrintUsage(stderr);
                return 2;
            }
            char* end = nullptr;
            errno = 0;
            const char* value = argv[++i];
            const long parsed = std::strtol(value, &end, 10);
            if (errno != 0 || end == value || *end != '\0' || parsed < 0 || parsed > 65535) {
                std::fprintf(stderr, "mcp-example: 非法端口 %s\n", argv[i]);
                return 2;
            }
            port = static_cast<std::uint16_t>(parsed);
            mode = Mode::kHttp;
        } else {
            PrintUsage(stderr);
            return 2;
        }
    }

    struct sigaction action {};  // 先装信号，再启动运行时
    action.sa_handler = OnStopSignal;
    ::sigemptyset(&action.sa_mask);
    ::sigaction(SIGINT, &action, nullptr);
    ::sigaction(SIGTERM, &action, nullptr);

    // 共享协程运行时（transport 的就绪等待与协程派发都落在它上面；与仓内测试同参）：
    // 纤程栈给到 256KB，避免深调用链撞默认栈保护页。
    auto* config = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    config->m_cfg_static_thread_num = 2;
    config->m_cfg_stack_size        = 1024 * 256;
    bbt::coroutine::detail::Scheduler::GetInstance()->Start(
        bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);

    // server core：注册（Start 前）→ Start 一次冻结快照并开协议接纳。
    bbt::infra::mcp::ServerOptions options;
#ifdef BBT_MCP_SCHEMA_ADAPTER
    options.instructions = "bbtools-infra 示例 server：echo 工具、资源与提示词";
#else
    options.instructions = "bbtools-infra 示例 server：echo 资源与提示词（Schema 选件 OFF）";
#endif
    auto server_handle   = bbt::infra::mcp::McpServer::Create(options);
    if (!server_handle) {
        std::fprintf(stderr, "mcp-example: McpServer::Create 失败\n");
        return 1;
    }
    auto server = std::move(server_handle).value();

#ifdef BBT_MCP_SCHEMA_ADAPTER
    if (!bbt::infra::mcp::EnableToolsSchemaValidation(*server)) {
        std::fprintf(stderr, "mcp-example: EnableToolsSchemaValidation 失败\n");
        return 1;
    }
    auto input_schema = bbt::infra::mcp::JsonValue::Parse(
        R"({"type":"object","properties":{"message":{"type":"string"}},"required":["message"],"additionalProperties":false})",
        4096, 16);
    auto output_schema = bbt::infra::mcp::JsonValue::Parse(
        R"({"type":"object","properties":{"echo":{"type":"string"},"count":{"type":"integer"}},"required":["echo","count"],"additionalProperties":false})",
        4096, 16);
    if (!input_schema || !output_schema) {
        std::fprintf(stderr, "mcp-example: echo Schema 解析失败\n");
        return 1;
    }
    bbt::infra::mcp::ToolDefinition tool;
    tool.name = "echo";
    tool.description = "回显 message；structuredContent 含 echo 和有效调用计数 count";
    tool.input_schema = std::move(input_schema).value();
    tool.output_schema = std::move(output_schema).value();
    if (!server->AddTool(std::move(tool), EchoTool)) {
        std::fprintf(stderr, "mcp-example: AddTool 失败\n");
        return 1;
    }
#endif

    bbt::infra::mcp::ResourceDefinition resource;
    resource.uri         = "memo://echo";
    resource.name        = "echo";
    resource.description = "回显注册 uri 的确定性资源";
    resource.mime_type   = "text/plain";
    if (!server->AddResource(resource, EchoResource)) {
        std::fprintf(stderr, "mcp-example: AddResource 失败\n");
        return 1;
    }

    bbt::infra::mcp::PromptDefinition prompt;
    prompt.name        = "echo";
    prompt.description = "回显 text 参数（缺省「你好」）的提示词";
    bbt::infra::mcp::PromptArgument argument;
    argument.name        = "text";
    argument.description = "要回显的文本";
    argument.required    = false;
    prompt.arguments.push_back(argument);
    if (!server->AddPrompt(prompt, EchoPrompt)) {
        std::fprintf(stderr, "mcp-example: AddPrompt 失败\n");
        return 1;
    }

    if (!server->Start()) {
        std::fprintf(stderr, "mcp-example: server Start 失败\n");
        return 1;
    }

    int exit_code = 0;
    if (mode == Mode::kStdio) {
        auto started = bbt::infra::mcp::McpStdioTransport::Start(server);
        if (!started) {
            std::fprintf(stderr,
                         "mcp-example: stdio transport 启动失败（stdin/stdout 必须是独占管道）\n");
            server->Close();
            return 1;
        }
        auto transport = std::move(started).value();
        std::fprintf(stderr, "mcp-example: ready mode=stdio\n");

        // stdin EOF 由 transport 自行走同一收口序列；信号则由本线程补一次同步 Close。
        while (!transport->IsClosed() && g_stop == 0) ::usleep(20000);
        transport->Close();  // 幂等：EOF 已收口时等首次真实落定
        const auto evidence = transport->LastCloseEvidence();
        std::fprintf(stderr,
                     "mcp-example: stdio closed eof=%d nonblock_restored=%d dropped=%zu\n",
                     evidence.eof ? 1 : 0, evidence.nonblock_restored ? 1 : 0,
                     evidence.dropped_frames);
    } else {
        bbt::infra::mcp::HttpOptions http_options;
        http_options.listen_port = port;  // 0 = 动态端口
        auto listened = bbt::infra::mcp::McpHttpTransport::Listen(server, http_options);
        if (!listened) {
            std::fprintf(stderr, "mcp-example: http transport 启动失败\n");
            server->Close();
            return 1;
        }
        auto transport = std::move(listened).value();
        std::fprintf(stderr, "mcp-example: ready mode=http port=%u\n",
                     static_cast<unsigned>(transport->LocalAddress().port));

        while (g_stop == 0) ::usleep(50000);
        transport->Close();
        const auto evidence = transport->LastCloseEvidence();
        std::fprintf(stderr, "mcp-example: http closed listener_closed=%d live=%zu\n",
                     evidence.listener_closed ? 1 : 0, evidence.live_connections);
    }

    // core 生命周期由业务决定：transport 不代管，这里显式封口（幂等）。
    server->Close();
    return exit_code;
}
