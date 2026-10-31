// 3-2（本切片 HTTP 形态）：MCP Streamable HTTP 可运行示例 server。
//
// 这是什么：一个最小但真实可跑的 MCP over HTTP 示例，演示 bbtools-infra
// 公共面的完整生命周期与 Streamable HTTP transport：
//
//   GlobalConfig/Scheduler 启动共享协程运行时
//   → McpServer::Create → 注册 resource/prompt → Start 冻结快照
//   → net::Engine 装配 + McpHttpTransport::Create → engine->Run（监听 127.0.0.1）
//   → transport->Start → 打印 http://127.0.0.1:<port>/mcp
//   → 主线程等 SIGINT/SIGTERM
//   → 按契约同步收口：StopAccepting → Drain → Close → WaitStopped
//   → server->Close()（core 生命周期由业务决定，transport 不代管）
//
// 门禁与 wire：POST /mcp only；Accept 双媒体；Origin/Host 白名单；modern 三件套
// 路由头（含 base64 sentinel）；JSON 响应 / subscriptions/listen SSE /
// 通知 202 / 非 /mcp 404 / 非 POST 405。
//
// 怎么跑：
//   $ ./Example_mcp_http_server --http
//   → 打印 mcp-example: listening http://127.0.0.1:<port>/mcp
//   再用官方 Python SDK 2.3.0 的 streamable_http_client 连接（见 VERIFY interop），
//   或 curl 手工 POST。
//
// Origin/Host 白名单演示：无 Origin/匹配 Host 的 POST 放行；`-D` Origin 白名单
// 为空时任何带 Origin 的请求 → 403（浏览器默认拒绝），curl 不带 Origin 不受影响。

#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <chrono>
#include <utility>
#include <vector>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>
#include <bbt/infra/mcp/McpHttpTransport.hpp>
#include <bbt/infra/mcp/McpServer.hpp>
#include <bbt/infra/mcp/McpTypes.hpp>
#include <bbt/infra/net/Engine.hpp>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>

namespace {

volatile std::sig_atomic_t g_stop = 0;
void OnStopSignal(int) { g_stop = 1; }

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

bbt::infra::result<bbt::infra::mcp::PromptResult> EchoPrompt(
    const bbt::infra::mcp::RequestContext&, const bbt::infra::mcp::JsonValue& arguments) {
    std::string text = "你好";
    if (arguments.IsObject()) {
        auto found = arguments.Find("text");
        if (found && found.value().IsString()) {
            auto s = found.value().AsString();
            if (s) text = s.value();
        }
    }
    bbt::infra::mcp::PromptResult out;
    out.description = "echo 提示词：把 text 回显为一条 user 消息";
    bbt::infra::mcp::PromptMessage message;
    message.role = bbt::infra::mcp::PromptRole::User;
    message.content = bbt::infra::mcp::Content::Text("echo:" + text);
    out.messages.push_back(message);
    return bbt::infra::result<bbt::infra::mcp::PromptResult>::ok(std::move(out));
}

void PrintUsage(std::FILE* out) {
    std::fprintf(out, "用法：Example_mcp_http_server --http\n"
                      "  --http  POST /mcp Streamable HTTP（127.0.0.1 随机端口）\n");
}

} // namespace

int main(int argc, char** argv) {
    if (argc != 2 || std::strcmp(argv[1], "--http") != 0) {
        PrintUsage(stderr);
        return 2;
    }

    struct sigaction action {};
    action.sa_handler = OnStopSignal;
    ::sigemptyset(&action.sa_mask);
    ::sigaction(SIGINT, &action, nullptr);
    ::sigaction(SIGTERM, &action, nullptr);

    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 128 * 1024;
    g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
    g_scheduler->Start();

    bbt::infra::mcp::ServerOptions server_options;
    server_options.instructions =
        "bbtools-infra MCP HTTP 示例：echo 资源与提示词";
    auto made_server = bbt::infra::mcp::McpServer::Create(std::move(server_options));
    if (!made_server) {
        std::fprintf(stderr, "mcp-example: McpServer::Create 失败\n");
        return 1;
    }
    auto server = std::move(made_server).value();

    bbt::infra::mcp::ResourceDefinition resource;
    resource.uri = "memo://echo";
    resource.name = "echo";
    resource.description = "回显注册 uri 的确定性资源";
    resource.mime_type = "text/plain";
    if (!server->AddResource(resource, EchoResource)) {
        std::fprintf(stderr, "mcp-example: AddResource 失败\n");
        return 1;
    }
    bbt::infra::mcp::PromptDefinition prompt;
    prompt.name = "echo";
    prompt.description = "回显 text 参数（缺省「你好」）的提示词";
    bbt::infra::mcp::PromptArgument argument;
    argument.name = "text";
    argument.required = false;
    prompt.arguments.push_back(argument);
    if (!server->AddPrompt(prompt, EchoPrompt)) {
        std::fprintf(stderr, "mcp-example: AddPrompt 失败\n");
        return 1;
    }
    if (!server->Start()) {
        std::fprintf(stderr, "mcp-example: server Start 失败\n");
        return 1;
    }

    auto engine = std::make_shared<bbt::infra::net::Engine>(
        std::shared_ptr<bbt::infra::net::RawHandler>{});
    bbt::infra::mcp::McpHttpOptions http_options;
    // loopback-only：Host/Origin 白名单覆盖本机；非 loopback 部署经外层反代
    // 改 allowed_hosts（鉴权在 infra 之上）。
    http_options.allowed_hosts = {"127.0.0.1", "localhost"};
    auto made_transport = bbt::infra::mcp::McpHttpTransport::Create(
        server, engine, std::move(http_options));
    if (!made_transport) {
        std::fprintf(stderr, "mcp-example: McpHttpTransport::Create 失败\n");
        return 1;
    }
    auto transport = std::move(made_transport).value();

    bbt::infra::net::EngineConfig config;
    config.runtime_progress = bbt::infra::net::RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    std::error_code error;
    config.listen_addresses = {bbt::infra::net::Address::Ip("127.0.0.1", 0, error)};
    if (error) {
        std::fprintf(stderr, "mcp-example: listen address 失败\n");
        return 1;
    }
    std::thread runner([&] { error = engine->Run(config); });
    {
        const auto end = std::chrono::steady_clock::now() + std::chrono::seconds{3};
        while (engine->Phase() != bbt::infra::net::EnginePhase::Running &&
               std::chrono::steady_clock::now() < end)
            std::this_thread::sleep_for(std::chrono::milliseconds{1});
    }
    if (engine->Phase() != bbt::infra::net::EnginePhase::Running) {
        std::fprintf(stderr, "mcp-example: engine 未 Running\n");
        return 1;
    }
    std::vector<bbt::infra::net::Address> addresses;
    if (engine->LocalAddresses(addresses) || addresses.empty()) {
        std::fprintf(stderr, "mcp-example: 无监听地址\n");
        return 1;
    }
    if (!transport->Start()) {
        std::fprintf(stderr, "mcp-example: transport Start 失败\n");
        return 1;
    }
    std::fprintf(stderr, "mcp-example: listening http://127.0.0.1:%u/mcp\n",
                 addresses.front().Port());

    // 演示发布入口：每 2s 发一次 mem://echo 的资源变更，给 SSE 订阅流一个真实
    // 事件源（PublishResourceChanged 是公共面显式发布入口）。
    std::atomic<bool> publish_run{true};
    std::thread publisher([&] {
        while (publish_run.load()) {
            std::this_thread::sleep_for(std::chrono::seconds{2});
            if (publish_run.load()) server->PublishResourceChanged("memo://echo");
        }
    });

    while (g_stop == 0) std::this_thread::sleep_for(std::chrono::milliseconds{20});

    publish_run.store(false);
    publisher.join();

    transport->StopAccepting();
    const auto drained = transport->Drain(std::chrono::steady_clock::now() + std::chrono::seconds{3});
    transport->Close();
    const auto stopped = transport->WaitStopped(std::chrono::steady_clock::now() + std::chrono::seconds{3});
    std::fprintf(stderr, "mcp-example: http stopped drain=%d stopped=%d\n",
                 static_cast<bool>(drained), static_cast<bool>(stopped));
    server->Close();
    engine->RequestClose();
    runner.join();
    return 0;
}
