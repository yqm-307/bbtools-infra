/**
 * contract 检查：McpHttpTransport（Streamable HTTP，architecture §8）。
 *
 * 真实 TCP loopback（自有进程动态端口 + POSIX socket 原始字节断言）：
 *   - 头/安全矩阵：POST-only 405（GET/DELETE/PUT/HEAD）、非 /mcp 404、Accept
 *     双媒体 406、Origin 白名单 403、Host 白名单 421、体上限 413；
 *   - modern 路由头矩阵：MCP-Protocol-Version 必现且等于信封版本、Mcp-Method
 *     等于 method、Mcp-Name 解码（含 =?base64?…?= sentinel、非 ASCII、畸形
 *     sentinel、字面值 sentinel）等于 params.name/uri、重复路由头拒绝、
 *     非 named 方法给 Mcp-Name 拒绝 → 400 + -32020 JSON-RPC body（id 原值）；
 *   - JSON 响应：server/discover 200 application/json + resultType=complete；
 *     modern 未知方法 404 + -32601 body；不支持的 modern 协议版本 400 + -32022
 *     body（结构化 error.code 判定）；合法通知 POST → 202 空体；parse
 *     error → 200 + -32700；超在途 → 503；
 *   - subscriptions/listen SSE：text/event-stream + X-Accel-Buffering: no +
 *     chunked + ack 为首个 data 帧、PublishResourceChanged 事件携带
 *     _meta.subscriptionId、keepalive「: 」注释、断连驱动订阅结束（peer
 *     close 后发布不再投递、连接端 EOF）；
 *   - legacy sessionless：MCP-Protocol-Version == 2025-11-25 → 业务方法
 *     直通（无 initialize）、非服务 revision（含已移除的 2025-06-18）→
 *     400 -32020、Mcp-Method/Name 头不属于 legacy → 400；
 *   - loopback 默认：0.0.0.0 监听未 opt-in → Start 拒（UnsupportedRoute），
 *     显式 allow_non_loopback → Start 成功；
 *   - 生命周期：StopAccepting→Drain→Close→WaitStopped；SSE 进行中 Close
 *     取消源、peer EOF。
 *
 * 无第三方测试框架；经 ${PROJECT_SOURCE_DIR}/bbt 消费内部面
 * （detail/Server.hpp 的 CreateSession/ProtocolLimits）。
 */
#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/http/Server.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>
#include <bbt/infra/mcp/McpHttpTransport.hpp>
#include <bbt/infra/mcp/McpServer.hpp>
#include <bbt/infra/mcp/McpTypes.hpp>
#include <bbt/infra/net/Engine.hpp>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

namespace {

int g_checks = 0;
int g_failures = 0;

void Check(bool ok, const char* expr, int line)
{
    ++g_checks;
    if (!ok) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d  %s\n", __FILE__, line, expr);
    }
}

#define CHECK(expr) Check(static_cast<bool>(expr), #expr, __LINE__)

using namespace std::chrono_literals;
namespace mcp = bbt::infra::mcp;
namespace net = bbt::infra::net;

bool WaitUntil(const std::function<bool()>& pred, std::chrono::milliseconds timeout = 5s)
{
    const auto end = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() >= end)
            return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

/**
 * 单次探测 peer 侧是否已有可读字节（不消费，不改变窗口）。
 */
bool Readable(int fd, std::chrono::milliseconds patience)
{
    pollfd pfd{fd, POLLIN, 0};
    return ::poll(&pfd, 1, static_cast<int>(patience.count())) == 1;
}

/**
 * server 装配：
 *
 * options_factory 在 engine Running、地址已知后被调用：允许测试按真实端口写
 * 白名单（allowed_hosts 须非空，常见形态 host[:port]）。
 * sse_queue 的单帧放大面：SSE 事件帧的自由字段只有 uri，故注册一个长 uri 资源，
 * 让单帧远大于 socket 缓冲预算（见 SseBoundedQueueAndClose）。
 */
std::string LongResourceUri()
{
    return "mem://" + std::string(48 * 1024, 'p');
}

struct McpHost {
    std::shared_ptr<net::Engine> engine{net::Engine::Create(std::shared_ptr<net::RawHandler>{})};
    std::shared_ptr<mcp::McpServer> server;
    std::shared_ptr<mcp::McpHttpTransport> transport;

    /**
     * 回调内自关闭探针：mem://self-close 的真实 resources/read handler 在
     * handler 协程里调用 transport->Close，记录返回值与调用后的关闭状态。
     */
    struct SelfCloseProbe {
        std::atomic<bool> called{false};
        std::atomic<int> code{0};
        std::atomic<bool> closed_after{false};
        std::atomic<bool> sealed_after{false};
    };

    std::shared_ptr<SelfCloseProbe> self_close{std::make_shared<SelfCloseProbe>()};
    std::thread runner;
    std::mutex run_mutex;
    std::condition_variable run_cv;
    std::error_code run_error;
    bool run_finished = false;
    net::Address address;
    std::uint16_t port = 0;

    /**
     * socket_send_buffer > 0 时映射为引擎的 SO_SNDBUF（0 = 不设置，用系统默认）。
     */
    McpHost(
        std::function<mcp::McpHttpOptions(std::uint16_t)> options_factory,
        bool start = true,
        const char* listen_ip = "127.0.0.1",
        std::int64_t socket_send_buffer = 0)
    {
        mcp::ServerOptions server_options;
        auto made = mcp::McpServer::Create(std::move(server_options));
        CHECK(made);
        server = std::move(made).value();
        mcp::ResourceDefinition resource;
        resource.uri = "mem://alpha";
        resource.name = "alpha";
        resource.mime_type = "text/plain";
        CHECK(server->AddResource(std::move(resource), [](const auto&, const std::string& uri) {
            mcp::ResourceResult result;
            auto content = mcp::ResourceContents::Text(uri, "text/plain", "alpha-body");
            CHECK(content);
            result.contents.push_back(std::move(content).value());
            return bbt::infra::result<mcp::ResourceResult>::ok(std::move(result));
        }));
        mcp::ResourceDefinition resource_zh;
        resource_zh.uri = u8"mem://资源";
        resource_zh.name = "zh";
        resource_zh.mime_type = "text/plain";
        CHECK(server->AddResource(std::move(resource_zh), [](const auto&, const std::string& uri) {
            mcp::ResourceResult result;
            auto content = mcp::ResourceContents::Text(uri, "text/plain", "zh-body");
            CHECK(content);
            result.contents.push_back(std::move(content).value());
            return bbt::infra::result<mcp::ResourceResult>::ok(std::move(result));
        }));
        mcp::ResourceDefinition resource_long;
        resource_long.uri = LongResourceUri();
        resource_long.name = "long";
        resource_long.mime_type = "text/plain";
        CHECK(server->AddResource(std::move(resource_long), [](const auto&, const std::string& uri) {
            mcp::ResourceResult result;
            auto content = mcp::ResourceContents::Text(uri, "text/plain", "long-body");
            CHECK(content);
            result.contents.push_back(std::move(content).value());
            return bbt::infra::result<mcp::ResourceResult>::ok(std::move(result));
        }));
        mcp::PromptDefinition prompt;
        prompt.name = "echo";
        CHECK(server->AddPrompt(std::move(prompt), [](const auto&, const mcp::JsonValue&) {
            mcp::PromptResult out;
            mcp::PromptMessage message;
            message.role = mcp::PromptRole::User;
            message.content = mcp::Content::Text("echo-prompt");
            out.messages.push_back(message);
            return bbt::infra::result<mcp::PromptResult>::ok(std::move(out));
        }));

        /**
         * 自等待探针资源：handler 在真实请求分发内、handler 协程上同步调用
         * transport->Close；transport 在下面 Create 后才赋值，但 handler 只在
         * Engine Run 后执行。
         */
        mcp::ResourceDefinition self_close_resource;
        self_close_resource.uri = "mem://self-close";
        self_close_resource.name = "self-close";
        self_close_resource.mime_type = "text/plain";
        CHECK(server->AddResource(std::move(self_close_resource), [this](const auto&, const std::string& uri) {
            const auto outcome = transport->Close(
                bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::seconds{1}));
            self_close->called = true;
            self_close->code = outcome ? 0 : static_cast<int>(outcome.error().code);
            self_close->closed_after = transport->IsClosed();
            self_close->sealed_after = transport->CloseReason().has_value();
            mcp::ResourceResult result;
            auto content = mcp::ResourceContents::Text(uri, "text/plain", "self-close-body");
            CHECK(content);
            result.contents.push_back(std::move(content).value());
            return bbt::infra::result<mcp::ResourceResult>::ok(std::move(result));
        }));
        CHECK(server->Start());

        /**
         * transport Create（EnableAcceptHandoff）必须在 engine Run 前完成——
         * server 布局（有监听）要求 handoff 接收者已登记。端口到 Run 后才知，
         * allowed_hosts 用不带端口的条目（Host 门禁按主机部分匹配）。
         */
        net::EngineConfig config;
        config.runtime_progress = net::RuntimeProgress::Independent;
        config.options.num_event_loop = 1;
        config.options.socket_send_buffer = socket_send_buffer;
        std::error_code bind_error;
        config.listen_addresses = {net::Address::Ip(listen_ip, 0, bind_error)};
        CHECK(!bind_error);
        auto made_transport = mcp::McpHttpTransport::Create(server, engine, options_factory(0));
        CHECK(made_transport);
        transport = std::move(made_transport).value();

        runner = std::thread([this, config] {
            std::error_code error = engine->Run(config);
            std::lock_guard<std::mutex> lock(run_mutex);
            run_error = error;
            run_finished = true;
            run_cv.notify_all();
        });
        CHECK(WaitUntil([&] { return engine->Phase() == net::EnginePhase::Running || run_finished; }));
        if (run_finished) {
            std::lock_guard<std::mutex> lock(run_mutex);
            std::fprintf(stderr, "engine Run failed early: %s\n", run_error.message().c_str());
        }
        CHECK(engine->Phase() == net::EnginePhase::Running);
        std::vector<net::Address> addresses;
        CHECK(!engine->LocalAddresses(addresses) && addresses.size() == 1);
        address = addresses[0];
        port = address.Port();

        if (start)
            CHECK(transport->Start());
    }

    ~McpHost()
    {

        /**
         * 只请求封口：析构不等待在途请求；Engine 关闭前不需要物理排空。
         */
        (void)transport->Close(
            bbt::infra::CloseOptions::Immediate(
                bbt::infra::MakeError(bbt::infra::ErrorCode::Closed, "test host teardown")));
        (void)engine->Close();
        if (runner.joinable())
            runner.join();
    }

    std::string HostHeader() const { return "127.0.0.1:" + std::to_string(port); }
};

mcp::McpHttpOptions DefaultOptions(std::uint16_t)
{
    mcp::McpHttpOptions options;

    /**
     * Host 门禁按主机部分匹配（条目可带 :port；不带则端口自由）。真实端口在
     * engine Run 前未知，故白名单只给主机名。
     */
    options.allowed_hosts = {"127.0.0.1", "localhost"};
    options.keepalive_interval = 80ms;
    options.max_stream_events = 8;
    return options;
}

/**
 * raw TCP peer
 */
struct Peer {
    int fd = -1;
    std::string pending;

    /**
     * recv_buffer > 0：connect 前设 SO_RCVBUF（同时关掉接收方向自动调优），用于把
     * 接收窗口压到远小于单帧，制造「写协程卡在写」的受控阶段。
     */
    explicit Peer(std::uint16_t port, int recv_buffer = 0)
    {
        fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        CHECK(fd >= 0);
        timeval tv{5, 0};
        CHECK(::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) == 0);
        if (recv_buffer > 0)
            CHECK(::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &recv_buffer, sizeof(recv_buffer)) == 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        CHECK(::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    }

    ~Peer()
    {
        if (fd >= 0)
            ::close(fd);
    }

    void Send(const std::string& bytes)
    {
        std::size_t off = 0;
        while (off < bytes.size()) {
            const auto n = ::send(fd, bytes.data() + off, bytes.size() - off, MSG_NOSIGNAL);
            CHECK(n > 0);
            off += static_cast<std::size_t>(n);
        }
    }

    std::string Drain(std::chrono::milliseconds patience = 3s)
    {
        const auto end = std::chrono::steady_clock::now() + patience;
        for (;;) {
            const auto left = end - std::chrono::steady_clock::now();
            if (left <= 0ms)
                break;
            pollfd pfd{fd, POLLIN, 0};
            const auto ms = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(left).count());
            if (::poll(&pfd, 1, std::max(1, ms)) != 1)
                break;
            char buf[65536];
            const auto n = ::recv(fd, buf, sizeof(buf), 0);
            if (n <= 0)
                break;
            pending.append(buf, static_cast<std::size_t>(n));
        }
        auto out = std::move(pending);
        pending.clear();
        return out;
    }

    /**
     * 读一个完整 buffered 响应（\r\n\r\n + Content-Length 体）；keep-alive 连接
     * 不等 EOF，头+体齐即返回。
     */
    std::string ReadResponse(std::chrono::milliseconds patience = 3s)
    {
        const auto end = std::chrono::steady_clock::now() + patience;
        for (;;) {
            const auto head_end = pending.find("\r\n\r\n");
            if (head_end != std::string::npos) {
                std::size_t length = 0;
                const std::string head = pending.substr(0, head_end);
                const auto pos = head.find("Content-Length:");
                if (pos != std::string::npos)
                    length = static_cast<std::size_t>(std::atoll(head.c_str() + pos + 15));
                if (pending.size() >= head_end + 4 + length) {
                    auto out = pending.substr(0, head_end + 4 + length);
                    pending.erase(0, head_end + 4 + length);
                    return out;
                }
            }
            const auto left = end - std::chrono::steady_clock::now();
            if (left <= 0ms)
                break;
            pollfd pfd{fd, POLLIN, 0};
            const auto ms = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(left).count());
            if (::poll(&pfd, 1, std::max(1, ms)) != 1)
                break;
            char buf[65536];
            const auto n = ::recv(fd, buf, sizeof(buf), 0);
            if (n <= 0)
                break;
            pending.append(buf, static_cast<std::size_t>(n));
        }
        auto out = std::move(pending);
        pending.clear();
        return out;
    }

    std::string ReadUntil(std::string_view marker, std::chrono::milliseconds patience = 3s)
    {
        const auto end = std::chrono::steady_clock::now() + patience;
        while (pending.find(marker) == std::string::npos) {
            const auto left = end - std::chrono::steady_clock::now();
            if (left <= 0ms)
                break;
            pollfd pfd{fd, POLLIN, 0};
            const auto ms = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(left).count());
            if (::poll(&pfd, 1, std::max(1, ms)) != 1)
                break;
            char buf[65536];
            const auto n = ::recv(fd, buf, sizeof(buf), 0);
            if (n <= 0)
                break;
            pending.append(buf, static_cast<std::size_t>(n));
        }
        const auto found = pending.find(marker);
        auto out = found == std::string::npos ? std::move(pending) : pending.substr(0, found + marker.size());
        if (found != std::string::npos)
            pending.erase(0, found + marker.size());
        return out;
    }

    bool Eof()
    {
        char buf[64];
        const auto n = ::recv(fd, buf, sizeof(buf), 0);
        return n == 0 || (n < 0 && errno == ECONNRESET);
    }
};

/**
 * 帧/请求构造
 */
std::string ModernMeta()
{
    return R"("_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}})";
}

std::string Request(const std::string& id, const std::string& method, const std::string& params_inner)
{
    const std::string params = params_inner.empty() ? "" : params_inner + ",";
    return R"({"jsonrpc":"2.0","id":")" + id + R"(","method":")" + method + R"(","params":{)" + params + ModernMeta()
           + "}}";
}

std::string HttpRequest(
    const McpHost& host,
    const std::string& method,
    const std::string& target,
    const std::string& body,
    std::vector<std::pair<std::string, std::string>> headers = {})
{
    std::string out = method + " " + target + " HTTP/1.1\r\n";
    out += "Host: " + host.HostHeader() + "\r\n";
    for (const auto& h : headers)
        out += h.first + ": " + h.second + "\r\n";
    out += "Content-Length: " + std::to_string(body.size()) + "\r\n\r\n";
    out += body;
    return out;
}

std::vector<std::pair<std::string, std::string>> AcceptBoth()
{
    return {{"Accept", "application/json, text/event-stream"}};
}

std::string
PostModern(const McpHost& host, const std::string& body, std::vector<std::pair<std::string, std::string>> headers)
{
    headers.insert(headers.begin(), {"Accept", "application/json, text/event-stream"});
    headers.insert(headers.begin(), {"Content-Type", "application/json"});
    return HttpRequest(host, "POST", "/mcp", body, std::move(headers));
}

/**
 * base64（标准字母表、尾部 '=' 填充）。
 */
std::string B64Encode(std::string_view in)
{
    static constexpr char kTable[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    for (std::size_t i = 0; i < in.size(); i += 3) {
        unsigned n = static_cast<unsigned char>(in[i]) << 16;
        if (i + 1 < in.size())
            n |= static_cast<unsigned char>(in[i + 1]) << 8;
        if (i + 2 < in.size())
            n |= static_cast<unsigned char>(in[i + 2]);
        out += kTable[(n >> 18) & 63];
        out += kTable[(n >> 12) & 63];
        out += (i + 1 < in.size()) ? kTable[(n >> 6) & 63] : '=';
        out += (i + 2 < in.size()) ? kTable[n & 63] : '=';
    }
    return out;
}

std::string Sentinel(std::string_view value)
{
    return "=?base64?" + B64Encode(value) + "?=";
}

int StatusOf(const std::string& response)
{
    CHECK(response.rfind("HTTP/1.1 ", 0) == 0);
    return std::atoi(response.c_str() + 9);
}

std::string HeadOf(const std::string& response)
{
    const auto e = response.find("\r\n\r\n");
    return e == std::string::npos ? response : response.substr(0, e);
}

std::string BodyOf(const std::string& response)
{
    const auto e = response.find("\r\n\r\n");
    CHECK(e != std::string::npos);
    return response.substr(e + 4);
}

bool HasHeader(const std::string& response, std::string name, std::string_view needle)
{
    for (auto& c : name)
        c = static_cast<char>(std::tolower(c));
    const auto head = HeadOf(response);
    std::size_t pos = 0;
    while (pos < head.size()) {
        const auto eol = head.find("\r\n", pos);
        const std::string_view line =
            std::string_view(head).substr(pos, eol == std::string::npos ? std::string::npos : eol - pos);
        const auto colon = line.find(':');
        if (colon != std::string_view::npos) {
            std::string field(line.substr(0, colon));
            for (auto& c : field)
                c = static_cast<char>(std::tolower(c));
            if (field == name && line.substr(colon + 1).find(needle) != std::string_view::npos)
                return true;
        }
        if (eol == std::string::npos)
            break;
        pos = eol + 2;
    }
    return false;
}

/**
 * 用例：
 *
 * 1) 头/安全矩阵：方法/路径/Accept/Origin/Host/体上限。
 */
void HeaderSecurityMatrix()
{
    McpHost host{[](std::uint16_t port) {
        mcp::McpHttpOptions options = DefaultOptions(port);
        options.allowed_origins = {"https://allowed.example"};
        options.max_body_bytes = 64;
        return options;
    }};
    const std::string body = "{}";

    /**
     * GET/DELETE/PUT/HEAD → 405。
     */
    for (const char* m : {"GET", "DELETE", "PUT", "HEAD"}) {
        Peer peer(host.port);
        peer.Send(HttpRequest(host, m, "/mcp", "", AcceptBoth()));
        CHECK(StatusOf(peer.ReadResponse()) == 405);
    }

    /**
     * 非 /mcp → 404。
     */
    for (const char* t : {"/", "/mcp/", "/MCP", "/mcp2", "/mcp?x=1"}) {
        Peer peer(host.port);
        peer.Send(HttpRequest(host, "POST", t, body, AcceptBoth()));
        CHECK(StatusOf(peer.ReadResponse()) == 404);
    }

    /**
     * Accept 缺任一媒体 → 406；双媒体（含参数与 wildcard）过门禁。
     */
    for (const char* accept : {"application/json", "text/event-stream", "text/plain", ""}) {
        Peer peer(host.port);
        peer.Send(HttpRequest(host, "POST", "/mcp", body, {{"Accept", accept}}));
        CHECK(StatusOf(peer.ReadResponse()) == 406);
    }
    for (const char* accept : {"application/json;q=0.9, text/event-stream;v=1", "*/*", "application/*, text/*"}) {
        Peer peer(host.port);
        peer.Send(HttpRequest(host, "POST", "/mcp", body, {{"Accept", accept}}));
        CHECK(StatusOf(peer.ReadResponse()) != 406);
    }

    /**
     * Origin：命中放行 / 未命中 403 / 无 Origin 放行。
     */
    {
        Peer peer(host.port);
        peer.Send(HttpRequest(
            host,
            "POST",
            "/mcp",
            body,
            {{"Accept", "application/json, text/event-stream"}, {"Origin", "https://allowed.example"}}));
        CHECK(StatusOf(peer.ReadResponse()) != 403);
        Peer denied(host.port);
        denied.Send(HttpRequest(
            host,
            "POST",
            "/mcp",
            body,
            {{"Accept", "application/json, text/event-stream"}, {"Origin", "https://evil.example"}}));
        CHECK(StatusOf(denied.ReadResponse()) == 403);
        Peer none(host.port);
        none.Send(HttpRequest(host, "POST", "/mcp", body, AcceptBoth()));
        CHECK(StatusOf(none.ReadResponse()) != 403);
    }

    /**
     * Host 不在白名单 → 421。
     */
    {
        Peer peer(host.port);
        peer.Send(
            "POST /mcp HTTP/1.1\r\nHost: other.invalid:1\r\n"
            "Accept: application/json, text/event-stream\r\n"
            "Content-Length: 2\r\n\r\n{}");
        CHECK(StatusOf(peer.ReadResponse()) == 421);
    }

    /**
     * 体上限（64）：超限 → 413。
     */
    {
        Peer peer(host.port);
        peer.Send(HttpRequest(host, "POST", "/mcp", std::string(128, 'x'), AcceptBoth()));
        CHECK(StatusOf(peer.ReadResponse()) == 413);
    }
}

/**
 * 2) modern 路由头矩阵：三件套校验 + -32020 body + id 原值。
 */
void ModernHeaderMatrix()
{
    McpHost host{DefaultOptions};
    auto discover = Request("d1", "server/discover", "");

    auto post = [&](std::vector<std::pair<std::string, std::string>> headers, const std::string& b) {
        Peer peer(host.port);
        peer.Send(PostModern(host, b, std::move(headers)));
        return peer.ReadResponse();
    };

    /**
     * 完整头 → 200 application/json。
     */
    {
        const auto r = post({{"MCP-Protocol-Version", "2026-07-28"}, {"Mcp-Method", "server/discover"}}, discover);
        CHECK(StatusOf(r) == 200);
        CHECK(HasHeader(r, "content-type", "application/json"));
        CHECK(BodyOf(r).find("\"resultType\":\"complete\"") != std::string::npos);
        CHECK(BodyOf(r).find("\"supportedVersions\"") != std::string::npos);
    }

    /**
     * 缺版本头 / 版本头失配 → 400 -32020（id 原值回包）。
     */
    for (const auto& headers : std::vector<std::vector<std::pair<std::string, std::string>>>{
             {{"Mcp-Method", "server/discover"}},
             {{"MCP-Protocol-Version", "2025-11-25"}, {"Mcp-Method", "server/discover"}},
             {{"MCP-Protocol-Version", "bogus"}, {"Mcp-Method", "server/discover"}}}) {
        const auto r = post(headers, discover);
        CHECK(StatusOf(r) == 400);
        CHECK(BodyOf(r).find("-32020") != std::string::npos);

        /**
         * id 原值
         */
        CHECK(BodyOf(r).find("\"d1\"") != std::string::npos);
    }

    /**
     * 缺 Mcp-Method / Mcp-Method 失配 → 400 -32020。
     */
    for (const auto& headers : std::vector<std::vector<std::pair<std::string, std::string>>>{
             {{"MCP-Protocol-Version", "2026-07-28"}},
             {{"MCP-Protocol-Version", "2026-07-28"}, {"Mcp-Method", "resources/read"}}}) {
        const auto r = post(headers, discover);
        CHECK(StatusOf(r) == 400 && BodyOf(r).find("-32020") != std::string::npos);
    }

    /**
     * 重复路由头 → 400（第一/最后副本读者分歧）。
     */
    {
        Peer peer(host.port);
        peer.Send(HttpRequest(
            host,
            "POST",
            "/mcp",
            discover,
            {{"Accept", "application/json, text/event-stream"},
             {"MCP-Protocol-Version", "2026-07-28"},
             {"MCP-Protocol-Version", "2026-07-28"},
             {"Mcp-Method", "server/discover"}}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 400 && BodyOf(r).find("-32020") != std::string::npos);
    }
}

/**
 * 3) Mcp-Name 矩阵：named 方法必需 + 解码匹配（含 base64 sentinel 与非 ASCII）。
 */
void McpNameMatrix()
{
    McpHost host{DefaultOptions};
    const std::string read_alpha = Request("r1", "resources/read", R"("uri":"mem://alpha")");
    const std::string read_zh = Request("r2", "resources/read", R"("uri":"mem://资源")");

    auto post = [&](const std::string& name_header,
                    const std::string& body,
                    const std::string& method = "resources/read") {
        Peer peer(host.port);
        peer.Send(PostModern(
            host, body, {{"MCP-Protocol-Version", "2026-07-28"}, {"Mcp-Method", method}, {"Mcp-Name", name_header}}));
        return peer.ReadResponse();
    };

    /**
     * ASCII 直值匹配 → 200。
     */
    CHECK(StatusOf(post("mem://alpha", read_alpha)) == 200);

    /**
     * 失配 → 400 -32020。
     */
    CHECK(StatusOf(post("mem://other", read_alpha)) == 400);

    /**
     * 非 ASCII 必须 sentinel（直值非 ASCII 头值不匹配体值）→ 400。
     */
    CHECK(StatusOf(post("mem://资源", read_zh)) == 400);

    /**
     * sentinel 编码 → 200。
     */
    CHECK(StatusOf(post(Sentinel("mem://资源"), read_zh)) == 200);

    /**
     * sentinel 包裹的 ASCII 值 → 200。
     */
    CHECK(StatusOf(post(Sentinel("mem://alpha"), read_alpha)) == 200);

    /**
     * 体值形似 sentinel 的字面值：sentinel 包裹原值解码 = 原值 → 200；
     * 直值（形似 sentinel）解码失败 ≠ 体值 → 400。
     */
    {
        const std::string tricky = "=?base64?AAAA?=";
        const std::string body = Request("r3", "resources/read", R"("uri":"=?base64?AAAA?=")");
        CHECK(StatusOf(post(Sentinel(tricky), body)) == 200);
        CHECK(StatusOf(post(tricky, body)) == 400);
    }

    /**
     * 畸形 sentinel（坏 base64）→ 解码失败 → 400。
     */
    CHECK(StatusOf(post("=?base64?!!!?=", read_alpha)) == 400);

    /**
     * "ABCD" → 奇数 base64
     */
    CHECK(StatusOf(post("=?base64?QUJDRA?=", read_alpha)) == 400);

    /**
     * named 方法缺 Mcp-Name → 400。
     */
    {
        Peer peer(host.port);
        peer.Send(
            PostModern(host, read_alpha, {{"MCP-Protocol-Version", "2026-07-28"}, {"Mcp-Method", "resources/read"}}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 400 && BodyOf(r).find("-32020") != std::string::npos);
    }

    /**
     * 非 named 方法给 Mcp-Name → 400。
     */
    {
        Peer peer(host.port);
        peer.Send(PostModern(
            host,
            Request("d1", "server/discover", ""),
            {{"MCP-Protocol-Version", "2026-07-28"}, {"Mcp-Method", "server/discover"}, {"Mcp-Name", "anything"}}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 400 && BodyOf(r).find("-32020") != std::string::npos);
    }

    /**
     * prompts/get 同为 named 方法。
     */
    {
        Peer peer(host.port);
        peer.Send(PostModern(
            host,
            Request("p1", "prompts/get", R"("name":"echo")"),
            {{"MCP-Protocol-Version", "2026-07-28"}, {"Mcp-Method", "prompts/get"}, {"Mcp-Name", "echo"}}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 200);
        CHECK(BodyOf(r).find("echo-prompt") != std::string::npos);
    }
}

/**
 * 4) 通知 → 202；parse error → 200 -32700；超在途 → 503；未知方法 → 404 -32601。
 */
void ResponseMatrix()
{
    McpHost host{[](std::uint16_t port) {
        auto options = DefaultOptions(port);
        return options;
    }};

    /**
     * notifications/cancelled（无 id、legacy 无路由头）→ 202 空体。
     */
    {
        Peer peer(host.port);
        peer.Send(PostModern(
            host,
            R"({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":"x"}})",
            {{"MCP-Protocol-Version", "2025-11-25"}}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 202 && BodyOf(r).empty());
    }

    /**
     * 现代信封的合法通知（带 _meta + 三件套头）→ 202。
     */
    {
        Peer peer(host.port);
        peer.Send(PostModern(
            host,
            R"({"jsonrpc":"2.0","method":"notifications/anything","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}})",
            {{"MCP-Protocol-Version", "2026-07-28"}, {"Mcp-Method", "notifications/anything"}}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 202);
    }

    /**
     * parse error → 200 + -32700（JSON-RPC body 保留，不冒 4xx）。
     */
    {
        Peer peer(host.port);
        peer.Send(PostModern(host, "{not json", {{"MCP-Protocol-Version", "2026-07-28"}, {"Mcp-Method", "x"}}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 200);
        CHECK(BodyOf(r).find("-32700") != std::string::npos);
        CHECK(BodyOf(r).find("\"id\":null") != std::string::npos);
    }

    /**
     * modern 未知方法 → 404 + -32601 body（SDK 原样上抛的 wire 形状）。
     */
    {
        Peer peer(host.port);
        peer.Send(PostModern(
            host,
            Request("u1", "bogus/method", ""),
            {{"MCP-Protocol-Version", "2026-07-28"}, {"Mcp-Method", "bogus/method"}}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 404);
        CHECK(BodyOf(r).find("-32601") != std::string::npos);
        CHECK(HasHeader(r, "content-type", "application/json"));
    }

    /**
     * 不支持的 modern 协议版本 → 400 + -32022 body（UnsupportedProtocolVersionError
     * 规范 MUST 400；结构化 error.code 判定，非 body 子串）。版本头与信封一致
     * （同为未服务版本），过路由头门禁后由 core 产 -32022。
     */
    {
        Peer peer(host.port);
        const std::string unsupported =
            R"({"jsonrpc":"2.0","id":"v1","method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2099-01-01","io.modelcontextprotocol/clientCapabilities":{}}}})";
        peer.Send(
            PostModern(host, unsupported, {{"MCP-Protocol-Version", "2099-01-01"}, {"Mcp-Method", "server/discover"}}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 400);
        CHECK(BodyOf(r).find("-32022") != std::string::npos);
        CHECK(BodyOf(r).find("\"requested\":\"2099-01-01\"") != std::string::npos);
        CHECK(BodyOf(r).find("\"supported\":") != std::string::npos);
        CHECK(HasHeader(r, "content-type", "application/json"));
    }

    /**
     * resources/read 正常业务 → 200。
     */
    {
        Peer peer(host.port);
        peer.Send(PostModern(
            host,
            Request("rd", "resources/read", R"("uri":"mem://alpha")"),
            {{"MCP-Protocol-Version", "2026-07-28"}, {"Mcp-Method", "resources/read"}, {"Mcp-Name", "mem://alpha"}}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 200);
        CHECK(BodyOf(r).find("alpha-body") != std::string::npos);
    }

    /**
     * 超在途：max_inflight_requests=1 由 ServerOptions 控制——本机 HTTP 测试用
     * 独立 host 缩小在途上限验证 503。
     */
}

/**
 * 5) subscriptions/listen SSE：ack/事件/keepalive/断连收口（真实 chunked wire）。
 */
void SseSubscription()
{
    McpHost host{DefaultOptions};
    Peer peer(host.port);
    const std::string listen = Request(
        "L1", "subscriptions/listen", R"("notifications":{"resourceSubscriptions":["mem://alpha","mem://nope"]})");
    peer.Send(
        PostModern(host, listen, {{"MCP-Protocol-Version", "2026-07-28"}, {"Mcp-Method", "subscriptions/listen"}}));

    /**
     * 头：200 + text/event-stream + chunked + X-Accel-Buffering: no。
     */
    const auto head = peer.ReadUntil("\r\n\r\n");
    CHECK(StatusOf(head) == 200);
    CHECK(HasHeader(head, "content-type", "text/event-stream"));
    CHECK(HasHeader(head, "transfer-encoding", "chunked"));
    CHECK(HasHeader(head, "x-accel-buffering", "no"));
    CHECK(!HasHeader(head, "mcp-session-id", ""));

    /**
     * 首个 data 帧 = ack（notifications/subscriptions/acknowledged + accepted 过滤
     * 子集：mem://nope 未注册 → omitted）。
     */
    const auto ack = peer.ReadUntil("\n\n", 3s);
    CHECK(ack.find("notifications/subscriptions/acknowledged") != std::string::npos);
    CHECK(ack.find("mem://alpha") != std::string::npos);

    /**
     * 未注册 uri 不接纳
     */
    CHECK(ack.find("mem://nope") == std::string::npos);
    CHECK(ack.find("io.modelcontextprotocol/subscriptionId") != std::string::npos);

    /**
     * subscriptionId = listen id
     */
    CHECK(ack.find("\"L1\"") != std::string::npos);

    /**
     * 发布资源变更 → 事件经 SSE 到达，带 subscriptionId meta。
     */
    const auto published = host.server->PublishResourceChanged("mem://alpha");
    CHECK(published && published.value() == 1);
    const auto event = peer.ReadUntil("\n\n", 3s);
    CHECK(event.find("notifications/resources/updated") != std::string::npos);
    CHECK(event.find("mem://alpha") != std::string::npos);
    CHECK(event.find("io.modelcontextprotocol/subscriptionId") != std::string::npos);

    /**
     * keepalive：80ms tick → 「: 」注释块到达（不重置流期限）。
     */
    const auto tick = peer.ReadUntil(":", 3s);
    CHECK(!tick.empty());

    /**
     * 断连驱动订阅结束：关闭 peer → 后续 PublishResourceChanged 计 0（会话已结束）。
     */
    ::shutdown(peer.fd, SHUT_RDWR);
    ::close(peer.fd);
    peer.fd = -1;
    CHECK(WaitUntil(
        [&] {
            std::size_t delivered = 0;
            const auto r = host.server->PublishResourceChanged("mem://alpha");
            if (r)
                delivered = r.value();
            return delivered == 0;
        },
        5s));
}

/**
 * 7b) bounded queue 与 transport Close 取消（真实 loopback）：队列满即丢帧、关闭
 *     连接后订阅结束。
 *
 *     队列有界（max_stream_events=4）不等于「wire 上最多 4 帧」：写协程会排空，
 *     发布与出队并发时 6 条发布能全部上线。要观察容量本身，必须先卡住出队：
 *       - 写协程一次只取一帧、等这一帧写完再取下一帧（http::Server 的
 *         ServeStream → Next / Write 串行）；
 *       - 引擎 SO_SNDBUF 与 peer SO_RCVBUF 都压到最小（都关掉自动调优），而单帧
 *         远大于这份缓冲预算（长 uri 资源把帧放大到 48 KiB），因此一旦写协程取走
 *         一帧，它就卡在这一帧的写里，队列在这个窗口内既不出队也不被排空掩盖。
 *     受控阶段：先发布 1 帧并等到 peer 侧出现可读字节（= 该帧已出队、正卡在写，
 *     队列为空），再连发 6 帧（> 容量 4）：4 帧入队、2 帧在入队处丢弃。最终
 *     wire 上恰好 1 + 4 = 5 帧——这是容量的判据，无界实现会给出 1 + 6 = 7 帧。
 */
void SseBoundedQueueAndClose()
{
    McpHost host{
        [](std::uint16_t port) {
            auto options = DefaultOptions(port);

            /**
             * 事件窗口内不发注释块，隔离 keepalive 干扰
             */
            options.keepalive_interval = 5s;
            options.max_stream_events = 4;

            /**
             * 放行 48 KiB 单帧（默认 16 KiB 会拒写并取消流）
             */
            options.max_chunk_bytes = 128U << 10;
            return options;

            /**
         * SO_SNDBUF：请求值被内核抬到最小值，且关掉发送方向自动调优
         */
        },
        true,
        "127.0.0.1",
        4096};

    /**
     * SO_RCVBUF：peer 不读，窗口同样压到最小
     */
    Peer peer(host.port, 4096);
    const std::string uri = LongResourceUri();
    peer.Send(PostModern(
        host,
        Request("L7", "subscriptions/listen", R"("notifications":{"resourceSubscriptions":[")" + uri + R"("]})"),
        {{"MCP-Protocol-Version", "2026-07-28"}, {"Mcp-Method", "subscriptions/listen"}}));

    /**
     * 头 + ack。ack 已上线 → 写协程已把它出队并停在 Next 的挂起上，队列此刻为空。
     */
    const auto head = peer.ReadUntil("\r\n\r\n");
    CHECK(HasHeader(head, "content-type", "text/event-stream"));
    const auto ack = peer.ReadUntil("acknowledged");
    CHECK(ack.find("acknowledged") != std::string::npos);

    /**
     * ack 帧尾部（"data: " 前缀已随 ack 消费掉）：读干净，下面的可读性探测才只可能
     * 由首帧的写引起。
     */
    std::string wire = peer.Drain(300ms);

    /**
     * 受控阶段：首帧被取走并卡在写里（帧 > 缓冲预算，写不可能完成），队列清空。
     * peer 侧出现可读字节是该阶段的证据：字节只能来自这次写，而这次写停在中途。
     */
    const auto gate = host.server->PublishResourceChanged(uri);
    CHECK(gate && gate.value() == 1);
    CHECK(WaitUntil([&] { return Readable(peer.fd, 50ms); }, 5s));

    /**
     * 6 帧 > 容量 4：全部经订阅路由接纳（每次 1 个投递），满队丢弃只发生在 body
     * 队列，admitted 计数本身不因满队而变。
     */
    std::size_t admitted = 0;
    for (int i = 0; i < 6; ++i) {
        const auto r = host.server->PublishResourceChanged(uri);
        CHECK(r && r.value() == 1);
        if (r)
            admitted += r.value();
    }
    CHECK(admitted == 6);

    const auto count_frames = [](const std::string& wire) {
        std::size_t n = 0;
        for (auto pos = wire.find("data: "); pos != std::string::npos; pos = wire.find("data: ", pos + 6))
            ++n;
        return n;
    };
    {
        const auto end = std::chrono::steady_clock::now() + 10s;
        while (count_frames(wire) < 5 && std::chrono::steady_clock::now() < end)
            wire += peer.Drain(200ms);
    }

    /**
     * 队列容量 4 + 卡在写里的那 1 帧
     */
    CHECK(count_frames(wire) == 5);

    /**
     * 再收一拍：无界实现多出的 2 帧会在这里到账
     */
    wire += peer.Drain(500ms);

    /**
     * 第 6、7 帧在入队处被丢弃，从未进入 wire
     */
    CHECK(count_frames(wire) == 5);

    /**
     * transport Close：commit 后流被 Cancel、连接关闭 → peer EOF；Publish 归零。
     */
    (void)host.transport->Close(bbt::infra::CloseOptions::Immediate());
    CHECK(WaitUntil([&] { return peer.Eof(); }, 5s));
    const auto stopped = host.transport->Close(
        bbt::infra::CloseOptions{bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + 5s});
    CHECK(stopped);
    const auto late = host.server->PublishResourceChanged(uri);
    CHECK(late && late.value() == 0);
}

/**
 * 6) legacy sessionless：版本头门禁 + 业务方法直通（无 initialize）。
 */
void LegacySessionless()
{
    McpHost host{DefaultOptions};
    const std::string legacy_read =
        R"({"jsonrpc":"2.0","id":1,"method":"resources/read","params":{"uri":"mem://alpha"}})";

    /**
     * 已服务 legacy revision 头 → 业务方法直通（sessionless：版本头替代握手）。
     */
    {
        Peer peer(host.port);
        peer.Send(PostModern(host, legacy_read, {{"MCP-Protocol-Version", "2025-11-25"}}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 200);
        CHECK(BodyOf(r).find("alpha-body") != std::string::npos);
    }

    /**
     * 无版本头的 legacy 业务方法：无会话态 → -32602（SDK runner.py 同 shape）。
     */
    {
        Peer peer(host.port);
        peer.Send(PostModern(host, legacy_read, {}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 200);
        CHECK(BodyOf(r).find("-32602") != std::string::npos);
    }

    /**
     * initialize（无头）→ 正常握手结果。
     */
    {
        Peer peer(host.port);
        peer.Send(PostModern(
            host,
            R"({"jsonrpc":"2.0","id":0,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"c","version":"1"},"protocolVersion":"2025-11-25"}})",
            {}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 200);
        CHECK(BodyOf(r).find("\"protocolVersion\":\"2025-11-25\"") != std::string::npos);
    }

    /**
     * 未服务的 revision 头 → 400 -32020。2025-06-18 已被移除出服务矩阵
     * （spec crosswalk 未核对），与已淘汰的 2024-11-05 同按未服务版本拒。
     */
    for (const char* version : {"2024-11-05", "2025-06-18"}) {
        Peer peer(host.port);
        peer.Send(PostModern(host, legacy_read, {{"MCP-Protocol-Version", version}}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 400 && BodyOf(r).find("-32020") != std::string::npos);
    }

    /**
     * 2025-06-18 的 initialize：不再被采纳协商——反提议 2025-11-25。
     */
    {
        Peer peer(host.port);
        peer.Send(PostModern(
            host,
            R"({"jsonrpc":"2.0","id":6,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"c","version":"1"},"protocolVersion":"2025-06-18"}})",
            {}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 200);
        CHECK(BodyOf(r).find("\"protocolVersion\":\"2025-11-25\"") != std::string::npos);
    }

    /**
     * legacy 请求带 Mcp-Method → 400（头不属于 legacy profile）。
     */
    {
        Peer peer(host.port);
        peer.Send(
            PostModern(host, legacy_read, {{"MCP-Protocol-Version", "2025-11-25"}, {"Mcp-Method", "resources/read"}}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 400 && BodyOf(r).find("-32020") != std::string::npos);
    }

    /**
     * legacy ping → 200 EmptyResult。
     */
    {
        Peer peer(host.port);
        peer.Send(
            PostModern(host, R"({"jsonrpc":"2.0","id":9,"method":"ping"})", {{"MCP-Protocol-Version", "2025-11-25"}}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 200);
    }
}

/**
 * 8) loopback 默认：非 loopback 监听未 opt-in → Start 拒绝；显式 opt-in → 成功。
 *    allowed_hosts/allowed_origins 是头白名单（防 Host 头伪造），不能当 bind
 *    安全——本用例证明默认下 0.0.0.0 绑定被 Start 拦住，而非靠配置自觉。
 */
void BindPolicy()
{

    /**
     * 0.0.0.0 监听 + 默认（false）opt-in：Start 必须拒。
     */
    {
        McpHost host{DefaultOptions, false, "0.0.0.0"};
        const auto r = host.transport->Start();
        CHECK(!r);
        if (!r) {
            CHECK(r.error().code == bbt::infra::ErrorCode::UnsupportedRoute);
            CHECK(std::string(r.error().domain_code) == "mcp.http.non_loopback");
        }
    }

    /**
     * 同一 0.0.0.0 监听 + 显式 allow_non_loopback：Start 放行，请求照常服务。
     */
    {
        McpHost host{
            [](std::uint16_t port) {
                auto options = DefaultOptions(port);
                options.allow_non_loopback = true;
                options.allowed_hosts = {"127.0.0.1", "localhost"};
                return options;
            },
            true,
            "0.0.0.0"};
        Peer peer(host.port);
        peer.Send(PostModern(
            host,
            Request("d1", "server/discover", ""),
            {{"MCP-Protocol-Version", "2026-07-28"}, {"Mcp-Method", "server/discover"}}));
        const auto r = peer.ReadResponse();
        CHECK(StatusOf(r) == 200);
        CHECK(BodyOf(r).find("\"supportedVersions\"") != std::string::npos);

        /**
         * 已决定的服务矩阵：不含 2025-06-18。
         */
        CHECK(BodyOf(r).find("2025-06-18") == std::string::npos);
    }
}

/**
 * 7) 生命周期：SSE 进行中 Close 取消源 + peer EOF；Drain/WaitStopped 有界。
 */
void Lifecycle()
{
    McpHost host{DefaultOptions};
    Peer peer(host.port);
    peer.Send(PostModern(
        host,
        Request("L9", "subscriptions/listen", R"("notifications":{"resourceSubscriptions":["mem://alpha"]})"),
        {{"MCP-Protocol-Version", "2026-07-28"}, {"Mcp-Method", "subscriptions/listen"}}));
    const auto head = peer.ReadUntil("acknowledged");
    CHECK(head.find("acknowledged") != std::string::npos);

    /**
     * Graceful 短期限（流仍挂起）→ TimedOut 且保留流责任与封口。
     */
    const auto drained =
        host.transport->Close(bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + 30ms));
    CHECK(!drained && drained.error().code == bbt::infra::ErrorCode::TimedOut);
    CHECK(host.transport->IsClosed());
    CHECK(host.transport->CloseReason().has_value());

    /**
     * 升级为 Immediate：commit 后流 Cancel 源 + 关连接。
     */
    CHECK(static_cast<bool>(host.transport->Close(bbt::infra::CloseOptions::Immediate())));
    CHECK(WaitUntil([&] { return peer.Eof(); }, 5s));
    const auto stopped = host.transport->Close(
        bbt::infra::CloseOptions{bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + 5s});
    CHECK(stopped);
}

/**
 * 8) CloseOptions：无在途责任时 Graceful 成功；封口后新请求不再被服务；
 *    物理收口由 WaitStopped 证明（listener 停止 + 连接 Released）。
 */
void CloseOptions()
{
    McpHost host{DefaultOptions};

    /**
     * 先证明在服务（真实 TCP + 真实 handler）。
     */
    {
        Peer peer(host.port);
        peer.Send(PostModern(
            host,
            Request("c1", "resources/read", R"("uri":"mem://alpha")"),
            {{"MCP-Protocol-Version", "2026-07-28"}, {"Mcp-Method", "resources/read"}, {"Mcp-Name", "mem://alpha"}}));
        const auto response = peer.ReadResponse();
        CHECK(StatusOf(response) == 200);
        CHECK(BodyOf(response).find("alpha-body") != std::string::npos);
    }

    /**
     * 需要等待却没有有限绝对期限的两种组合都在任何副作用之前拒绝：不封口、
     * 不发布首因。随后真实请求仍被服务，证明失败调用没有污染关闭位。
     */
    const auto unbounded = host.transport->Close(bbt::infra::CloseOptions::Graceful(bbt::coroutine::Deadline::max()));
    CHECK(!unbounded && unbounded.error().code == bbt::infra::ErrorCode::InvalidArgument);
    bbt::infra::CloseOptions unbounded_wait = bbt::infra::CloseOptions::Immediate();
    unbounded_wait.wait_release = true;
    const auto unbounded_release = host.transport->Close(unbounded_wait);
    CHECK(!unbounded_release && unbounded_release.error().code == bbt::infra::ErrorCode::InvalidArgument);
    CHECK(!host.transport->IsClosed());
    CHECK(!host.transport->CloseReason().has_value());

    /**
     * 自等待：真实 MCP resources/read handler 内调用 Close。委托的内部 server
     * 会以 admitted handler 拒绝；本层必须在置位前做同一拒绝，返回
     * InvalidContext 且不留下关闭位/首因（拒绝零副作用）。
     */
    {
        Peer peer(host.port);
        peer.Send(PostModern(
            host,
            Request("c3", "resources/read", R"("uri":"mem://self-close")"),
            {{"MCP-Protocol-Version", "2026-07-28"},
             {"Mcp-Method", "resources/read"},
             {"Mcp-Name", "mem://self-close"}}));
        const auto response = peer.ReadResponse();
        CHECK(StatusOf(response) == 200);
        CHECK(BodyOf(response).find("self-close-body") != std::string::npos);
    }
    CHECK(host.self_close->called.load());
    CHECK(host.self_close->code.load() == static_cast<int>(bbt::infra::ErrorCode::InvalidContext));
    CHECK(!host.self_close->closed_after.load());
    CHECK(!host.self_close->sealed_after.load());
    CHECK(!host.transport->IsClosed());
    CHECK(!host.transport->CloseReason().has_value());
    {
        Peer peer(host.port);
        peer.Send(PostModern(
            host,
            Request("c2", "resources/read", R"("uri":"mem://alpha")"),
            {{"MCP-Protocol-Version", "2026-07-28"}, {"Mcp-Method", "resources/read"}, {"Mcp-Name", "mem://alpha"}}));
        const auto response = peer.ReadResponse();
        CHECK(StatusOf(response) == 200);
    }

    /**
     * 无在途请求：Graceful 在期限内排空成功（不假报 TimedOut）。
     */
    const auto graceful = host.transport->Close(
        bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::seconds{2}));
    CHECK(graceful);
    CHECK(host.transport->IsClosed());
    const auto reason = host.transport->CloseReason();
    CHECK(reason.has_value());

    /**
     * 首因固定 + 幂等：再次 Immediate 不覆盖、不失败；wait_release=true 让这次
     * Close 在同一绝对期限内完成物理回收，不再依赖调用方另外 WaitStopped。
     */
    bbt::infra::CloseOptions immediate_release = bbt::infra::CloseOptions::Immediate();
    immediate_release.wait_release = true;
    immediate_release.deadline = std::chrono::steady_clock::now() + std::chrono::seconds{5};
    CHECK(static_cast<bool>(host.transport->Close(immediate_release)));
    CHECK(host.transport->CloseReason().value().message == reason.value().message);

    /**
     * 物理收口：listener 停止、本 server 连接 Released（WaitStopped 成功即该
     * 凭据；连接是否可再建立不再由本用例断言——listener 已停）。
     */
    const auto stopped = host.transport->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + std::chrono::seconds{5}});
    CHECK(stopped);
    const auto stopped_again = host.transport->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + std::chrono::seconds{1}});

    /**
     * 幂等：物理已落定即返回成功
     */
    CHECK(stopped_again);
}

// clang-format off
}
// clang-format on

int main(int argc, char** argv)
{
    if (argc != 2) {
        std::fprintf(stderr, "usage: %s <case>\n", argv[0]);
        return 2;
    }

    /**
     * 共享协程运行时（与 http_net 契约测试同参）：transport 的 CoWaiter/挂起
     * 与 http 内部协程都落在它上面；processer 不自动扩张。
     */
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 128 * 1024;
    g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
    g_scheduler->Start();

    const std::string name = argv[1];

    if (name == "headers")
        HeaderSecurityMatrix();
    else if (name == "modern_headers")
        ModernHeaderMatrix();
    else if (name == "mcp_name")
        McpNameMatrix();
    else if (name == "responses")
        ResponseMatrix();
    else if (name == "sse")
        SseSubscription();
    else if (name == "sse_queue")
        SseBoundedQueueAndClose();
    else if (name == "legacy")
        LegacySessionless();
    else if (name == "bind")
        BindPolicy();
    else if (name == "lifecycle")
        Lifecycle();
    else if (name == "close_options")
        CloseOptions();
    else {
        std::fprintf(stderr, "unknown case %s\n", name.c_str());
        return 2;
    }
    std::printf("case=%s checks=%d failures=%d\n", name.c_str(), g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
