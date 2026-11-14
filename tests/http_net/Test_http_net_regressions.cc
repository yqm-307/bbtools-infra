/**
 * 审查发现的协议、输入、期限及物理预算回归；只操作真实动态端口对端。
 */
#include "TestSupport.hpp"
#include "../net/ProcessRuntimeTestMain.hpp"
#include <bbt/coroutine/detail/Coroutine.hpp>
#include <bbt/infra/http/Server.hpp>
#include <bbt/infra/http/detail/Http11.hpp>
#include <limits>

using namespace http_net_test;
namespace http = bbt::infra::http;

namespace {

struct Fixture {
    Host host;
    HttpPeer peer;
    std::shared_ptr<http::Client> client;
    std::size_t baseline{SocketCount()};

    explicit Fixture(http::ClientOptions options = {}, unsigned maximum = 1)
    {
        options.base_url = peer.base_url();
        options.limits.max_connections = maximum;
        options.limits.max_keepalive_connections = 1;
        options.limits.keepalive_expiry = std::nullopt;
        auto made = http::Client::Create(host.engine, options);
        CHECK(made);
        client = made.value();
    }

    auto GetPath(const std::string& path = "/test")
    {
        return host.Call([&] { return bbt_co_await client->Get(path, {}, Until()); });
    }

    void Finish()
    {
        (void)client->Close(bbt::infra::CloseOptions::Immediate());
        peer.Stop();
        host.Finish();
        CHECK(WaitUntil([&] { return SocketCount() <= baseline; }));
    }
};

void Protocol(const std::string& mode)
{
    Fixture f;
    if (mode == "fragment_header" || mode == "fragment_body") {
        std::promise<void> release;
        auto ready = release.get_future().share();
        std::atomic<bool> first_sent{false};
        f.peer.PushConnectionHook([&, ready](int fd) {
            CHECK(HttpPeer::ReadRequestForHook(fd));
            const bool header = mode == "fragment_header";
            CHECK(
                HttpPeer::SendForHook(
                    fd, header ? "HTTP/1.1 200 O" : "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nAB"));
            first_sent = true;
            ready.wait();
            CHECK(HttpPeer::SendForHook(fd, header ? "K\r\nContent-Length: 4\r\n\r\nABCD" : "CD"));
        });
        auto call = f.host.Submit([&] { return bbt_co_await f.client->Get("/fragment", {}, Until()); });
        CHECK(WaitUntil([&] { return first_sent.load(); }));

        /**
         * 已收完整请求且首片已发；扣住后片时请求应仍待完成，而不是 need_more 错误。
         */
        CHECK(call.wait_for(30ms) == std::future_status::timeout);
        release.set_value();
        auto response = Get(call);
        CHECK(response);
        CHECK(response.value().status_code == 200 && response.value().body == "ABCD");
    } else if (mode == "chunked") {
        f.peer.set_canned([](const HttpPeer::Recorded&) {
            std::string bytes = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n";
            for (unsigned i = 0; i != 64; ++i) {
                bytes += "1\r\nx\r\n";
            }
            return bytes + "0\r\nX-Trailer: yes\r\n\r\n";
        });
        for (unsigned i = 0; i != 2; ++i) {
            auto response = f.GetPath();
            CHECK(response && response.value().body == std::string(64, 'x'));
        }
        CHECK(f.peer.accept_total() == 1);
    } else if (mode == "head_interim") {
        f.peer.set_canned([](const HttpPeer::Recorded& request) {
            if (request.method == "HEAD") {
                return std::string("HTTP/1.1 200 OK\r\nContent-Length: 123\r\n\r\n");
            }
            return "HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 103 Early Hints\r\n\r\n"
                   + HttpPeer::HttpResponse(200, "OK", request.target);
        });
        auto head = f.host.Call([&] { return bbt_co_await f.client->Request("HEAD", "/head", {}, Until()); });
        CHECK(head && head.value().status_code == 200 && head.value().body.empty());
        for (const auto& target : {"/one", "/two"}) {
            auto response = f.GetPath(target);
            CHECK(response && response.value().status_code == 200 && response.value().body == target);
        }
        CHECK(f.peer.accept_total() == 1 && f.peer.requests_total() == 3);
    } else if (mode == "eof_truncated") {
        f.peer.PushConnectionHook([](int fd) {
            CHECK(HttpPeer::ReadRequestForHook(fd));
            CHECK(HttpPeer::SendForHook(fd, "HTTP/1.0 200 OK\r\nConnection: close\r\n\r\nEOF-BODY"));
        });
        auto eof = f.GetPath();
        CHECK(eof && eof.value().body == "EOF-BODY");
        f.peer.PushConnectionHook([](int fd) {
            CHECK(HttpPeer::ReadRequestForHook(fd));
            CHECK(HttpPeer::SendForHook(fd, "HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\nshort"));
        });
        auto partial = f.GetPath();
        ExpectCode(partial, ErrorCode::OutcomeUnknown);
        CHECK(partial.error().request_phase == RequestPhase::RequestCommitted);
        auto again = f.GetPath();
        CHECK(again && again.value().body == "peer-default");
        CHECK(f.peer.accept_total() == 3);
    } else if (mode == "body_limit") {
        http::ClientOptions options;
        options.base_url = f.peer.base_url();
        options.max_body_bytes = 3;
        auto small = http::Client::Create(f.host.engine, options);
        CHECK(small);
        auto rejected = f.host.Call([&] { return bbt_co_await small.value()->Get("/large", {}, Until()); });
        ExpectCode(rejected, ErrorCode::ProtocolError);
        f.peer.set_canned([](const HttpPeer::Recorded&) { return HttpPeer::HttpResponse(200, "OK", "ok"); });
        auto good = f.host.Call([&] { return bbt_co_await small.value()->Get("/small", {}, Until()); });
        CHECK(good && good.value().body == "ok");
        (void)small.value()->Close(bbt::infra::CloseOptions::Immediate());
    } else {
        CHECK(false);
    }
    f.Finish();
}

void InputBoundary()
{
    http::ClientOptions defaults;
    CHECK(
        defaults.connect_timeout == 5s && defaults.read_timeout == 5s && defaults.write_timeout == 5s
        && defaults.pool_timeout == 5s);
    defaults.SetConnectTimeout(2s).SetReadTimeout(3s).SetWriteTimeout(4s).SetPoolTimeout(6s);
    Fixture f(defaults);

    /**
     * 只覆盖读档不得重置其他 Client 档；禁用与继承必须可分别表达。
     */
    http::RequestOptions override;
    override.SetReadTimeout(30s);
    auto inherited = f.client->BuildRequest("GET", "/timeout", override);
    CHECK(inherited);
    CHECK(
        inherited.value().connect_timeout == 2s && inherited.value().read_timeout == 30s
        && inherited.value().write_timeout == 4s && inherited.value().pool_timeout == 6s);
    override.DisableReadTimeout();
    auto disabled = f.client->BuildRequest("GET", "/timeout", override);
    CHECK(disabled && disabled.value().read_timeout == std::chrono::milliseconds::max());
    CHECK(
        disabled.value().connect_timeout == 2s && disabled.value().write_timeout == 4s
        && disabled.value().pool_timeout == 6s);
    override.read_timeout.reset();
    auto restored = f.client->BuildRequest("GET", "/timeout", override);
    CHECK(restored && restored.value().read_timeout == 3s);

    override.SetTimeout(7s).SetConnectTimeout(8s).SetWriteTimeout(9s).SetPoolTimeout(10s);
    auto configured = f.client->BuildRequest("GET", "/timeout", override);
    CHECK(
        configured && configured.value().connect_timeout == 8s && configured.value().read_timeout == 7s
        && configured.value().write_timeout == 9s && configured.value().pool_timeout == 10s);
    override.DisableConnectTimeout().DisableWriteTimeout().DisablePoolTimeout();
    configured = f.client->BuildRequest("GET", "/timeout", override);
    CHECK(
        configured && configured.value().connect_timeout == std::chrono::milliseconds::max()
        && configured.value().read_timeout == 7s && configured.value().write_timeout == std::chrono::milliseconds::max()
        && configured.value().pool_timeout == std::chrono::milliseconds::max());
    override.DisableTimeout();
    configured = f.client->BuildRequest("GET", "/timeout", override);
    CHECK(configured && configured.value().read_timeout == std::chrono::milliseconds::max());

    auto all_disabled = defaults;
    all_disabled.DisableConnectTimeout().DisableReadTimeout().DisableWriteTimeout().DisablePoolTimeout();
    CHECK(
        all_disabled.connect_timeout == std::chrono::milliseconds::max()
        && all_disabled.read_timeout == std::chrono::milliseconds::max()
        && all_disabled.write_timeout == std::chrono::milliseconds::max()
        && all_disabled.pool_timeout == std::chrono::milliseconds::max());
    all_disabled.SetTimeout(1s).DisableTimeout();
    auto unlimited = http::Client::Create(f.host.engine, all_disabled);
    CHECK(unlimited);
    auto unlimited_request = unlimited.value()->BuildRequest("GET", f.peer.base_url(), {});
    CHECK(unlimited_request && unlimited_request.value().read_timeout == std::chrono::milliseconds::max());
    CHECK(unlimited.value()->Close());

    for (const auto invalid : {0ms, -1ms}) {
        auto bad = defaults;
        bad.SetReadTimeout(invalid);
        auto created = http::Client::Create(f.host.engine, bad);
        ExpectCode(created, ErrorCode::InvalidArgument);
        override.SetReadTimeout(invalid);
        ExpectCode(f.client->BuildRequest("GET", "/invalid", override), ErrorCode::InvalidArgument);
        auto tampered = inherited.value();
        tampered.read_timeout = invalid;
        ExpectCode(
            f.host.Call([&] { return bbt_co_await f.client->Send(tampered, Until()); }), ErrorCode::InvalidArgument);
    }

    /**
     * 禁用和超大时长不得溢出，也不得突破总期限；固定 now 避免墙钟误差。
     */
    const auto now = std::chrono::steady_clock::now();
    const auto total = now + 10s;
    http::PreparedRequest timing;
    timing.pool_timeout = 2s;
    CHECK(http::detail::ResolveDeadlines(timing, total, now).pool == now + 2s);
    timing.pool_timeout = std::chrono::milliseconds::max();
    CHECK(http::detail::ResolveDeadlines(timing, total, now).pool == total);
    CHECK(http::detail::StageDeadline(std::chrono::milliseconds::max(), total) == total);
    CHECK(
        http::detail::StageDeadline(std::chrono::milliseconds::max(), bbt::coroutine::Deadline::max())
        == bbt::coroutine::Deadline::max());
    http::ClientOptions bad_defaults;
    bad_defaults.headers = {{"X-Test", "safe\r\nInjected: yes"}};
    CHECK(!http::Client::Create(f.host.engine, bad_defaults));
    CHECK(!f.client->BuildRequest("BAD METHOD", "/", {}));
    CHECK(!f.client->BuildRequest("GET", "http://127.0.0.1:80garbage/", {}));
    CHECK(!f.client->BuildRequest("GET", "/with space", {}));
    CHECK(!f.client->BuildRequest("GET", "https://127.0.0.1/", {}));
    const std::vector<http::Headers> invalid{
        {{"X-Test", "safe\r\nInjected: yes"}},
        {{"Bad Name", "value"}},
        {{"X-Nul", std::string("a\0b", 3)}},
        {{"X-Huge", std::string(70000, 'x')}},
        {{"Content-Length", "100"}},
        {{"Transfer-Encoding", "chunked"}}};
    for (const auto& headers : invalid) {
        http::RequestOptions options;
        options.headers = headers;
        CHECK(!f.client->BuildRequest("GET", "/invalid", options));
        auto built = f.client->BuildRequest("GET", "/valid", {});
        CHECK(built);

        /**
         * Send 也验证，不信任可变的 BuildRequest 产物。
         */
        built.value().headers = headers;
        auto rejected = f.host.Call([&] { return bbt_co_await f.client->Send(built.value(), Until()); });
        ExpectCode(rejected, ErrorCode::InvalidArgument);
    }
    auto changed = f.client->BuildRequest("GET", "/valid", {});
    CHECK(changed);
    changed.value().target_for_wire = "/bad\r\nInjected: yes";
    ExpectCode(
        f.host.Call([&] { return bbt_co_await f.client->Send(changed.value(), Until()); }), ErrorCode::InvalidArgument);
    auto bad_type =
        f.host.Call([&] { return bbt_co_await f.client->Post("/bad", {"body", "x\r\nInjected: yes"}, {}, Until()); });
    ExpectCode(bad_type, ErrorCode::InvalidArgument);
    CHECK(f.client->Stats().budget == 0 && f.peer.accept_total() == 0);
    CHECK(f.GetPath());
    CHECK(f.GetPath());
    CHECK(f.client->Stats().active == 0 && f.client->Stats().idle == 1);
    CHECK(f.peer.accept_total() == 1 && f.peer.requests_total() == 2);
    f.Finish();
}

void Quotas()
{
    Fixture f;
    http::ClientOptions options;
    options.base_url = f.peer.base_url();
    options.max_request_body_bytes = 3;
    auto bounded = http::Client::Create(f.host.engine, options);
    CHECK(bounded);
    auto prepared = bounded.value()->BuildRequest("POST", "/quota", {});
    CHECK(prepared);
    prepared.value().body = "1234";
    auto rejected = f.host.Call([&] { return bbt_co_await bounded.value()->Send(prepared.value(), Until()); });
    ExpectCode(rejected, ErrorCode::InvalidArgument);
    CHECK(f.peer.accept_total() == 0);
    CHECK(bounded.value()->Stats().budget == 0);

    options.max_request_body_bytes.reset();
    options.max_header_bytes = 64;
    auto header_bounded = http::Client::Create(f.host.engine, options);
    CHECK(header_bounded);
    f.peer.set_canned([](const HttpPeer::Recorded&) {
        return std::string("HTTP/1.1 200 OK\r\nX-Large: ") + std::string(80, 'x') + "\r\nContent-Length: 2\r\n\r\nok";
    });
    auto header_rejected =
        f.host.Call([&] { return bbt_co_await header_bounded.value()->Get("/header-quota", {}, Until()); });
    ExpectCode(header_rejected, ErrorCode::ProtocolError);
    f.peer.set_canned([](const HttpPeer::Recorded&) { return HttpPeer::HttpResponse(200, "OK", "ok"); });
    auto healthy =
        f.host.Call([&] { return bbt_co_await header_bounded.value()->Get("/after-header-quota", {}, Until()); });
    CHECK(healthy && healthy.value().body == "ok");
    CHECK(header_bounded.value()->Stats().active == 0);

    http::ClientOptions invalid;
    invalid.max_request_body_bytes = 0;
    CHECK(!http::Client::Create(f.host.engine, invalid));
    invalid.max_request_body_bytes.reset();
    invalid.max_header_bytes = 0;
    CHECK(!http::Client::Create(f.host.engine, invalid));
    invalid.max_header_bytes = std::numeric_limits<std::uint64_t>::max();
    CHECK(!http::Client::Create(f.host.engine, invalid));
    (void)bounded.value()->Close(bbt::infra::CloseOptions::Immediate());
    (void)header_bounded.value()->Close(bbt::infra::CloseOptions::Immediate());
    f.Finish();
}

void PreexpiredCancelled()
{
    Fixture f;
    auto reject = [&](bool cancel) {
        return f.host.Call([&] {
            if (cancel) {
                g_bbt_tls_coroutine_co->RequestCancel();
            }
            const auto call = cancel ? Until() : CallOptions{std::chrono::steady_clock::now() - 1s};
            return bbt_co_await f.client->Post("/must-not-send", {"side-effect", ""}, {}, call);
        });
    };
    ExpectCode(reject(false), ErrorCode::TimedOut);
    ExpectCode(reject(true), ErrorCode::Cancelled);
    CHECK(f.peer.accept_total() == 0 && f.client->Stats().budget == 0);
    CHECK(Access::ConnectTableSize(*f.host.engine) == 0);
    CHECK(f.GetPath("/warm"));
    ExpectCode(reject(false), ErrorCode::TimedOut);
    ExpectCode(reject(true), ErrorCode::Cancelled);
    CHECK(f.GetPath("/after-rejections"));
    CHECK(f.peer.accept_total() == 1 && f.peer.requests_total() == 2);
    for (const auto& request : f.peer.requests()) {
        CHECK(request.method == "GET");
    }
    f.Finish();
}

void PhaseTimeouts()
{
    Fixture f;
    std::promise<void> release;
    auto ready = release.get_future().share();
    std::atomic<bool> first_seen{false};
    f.peer.PushConnectionHook([&, ready](int fd) {
        CHECK(HttpPeer::ReadRequestForHook(fd));
        first_seen = true;
        ready.wait();
        CHECK(HttpPeer::SendForHook(fd, HttpPeer::HttpResponse(200, "OK", "first")));
        CHECK(HttpPeer::ReadRequestForHook(fd));
        CHECK(HttpPeer::SendForHook(fd, HttpPeer::HttpResponse(200, "OK", "queued")));
    });
    auto first = f.host.Submit([&] { return bbt_co_await f.client->Get("/first", {}, Until()); });
    CHECK(WaitUntil([&] { return first_seen.load(); }));
    http::RequestOptions options;
    options.SetTimeout(60ms).SetPoolTimeout(1s);
    auto queued = f.host.Submit([&] { return bbt_co_await f.client->Get("/queued", options, Until()); });
    CHECK(WaitUntil([&] { return f.client->Stats().waiting == 1; }));

    /**
     * 排队阶段已成立；刻意持池超过后续阶段档，后续 write/read 仍各有 60ms。
     */
    CHECK(queued.wait_for(160ms) == std::future_status::timeout);
    release.set_value();
    CHECK(Get(first));
    auto response = Get(queued);
    CHECK(response && response.value().body == "queued");
    CHECK(f.peer.accept_total() == 1);
    f.Finish();
}

void PerReadAndTotal()
{
    Fixture f;
    std::atomic<bool> request_seen{false};
    f.peer.PushConnectionHook([&](int fd) {
        CHECK(HttpPeer::ReadRequestForHook(fd));
        request_seen = true;
        CHECK(HttpPeer::SendForHook(fd, "HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\n"));

        /**
         * 这里的间隔是被测输入（慢分片），不是用 sleep 冒充进入请求阶段。
         */
        for (unsigned i = 0; i != 6; ++i) {
            std::this_thread::sleep_for(60ms);
            CHECK(HttpPeer::SendForHook(fd, "x"));
        }
    });
    http::RequestOptions options;
    options.SetReadTimeout(200ms);
    auto reading = f.host.Submit([&] { return bbt_co_await f.client->Get("/slow-chunks", options, Until()); });
    CHECK(WaitUntil([&] { return request_seen.load(); }));
    auto response = Get(reading);
    CHECK(response && response.value().body == "xxxxxx");

    /**
     * 总期限在真正提交后到期，必须保留 RequestCommitted/OutcomeUnknown，不能重试。
     */
    std::promise<void> release;
    auto ready = release.get_future().share();
    std::atomic<bool> committed{false};
    f.peer.PushConnectionHook([&, ready](int fd) {
        CHECK(HttpPeer::ReadRequestForHook(fd));
        committed = true;
        ready.wait();
    });
    options.DisableTimeout();
    auto total =
        f.host.Submit([&] { return bbt_co_await f.client->Post("/total", {"effect", ""}, options, Until(150ms)); });
    CHECK(WaitUntil([&] { return committed.load(); }));
    auto expired = Get(total);
    ExpectCode(expired, ErrorCode::OutcomeUnknown);
    CHECK(expired.error().request_phase == RequestPhase::RequestCommitted);
    release.set_value();
    f.Finish();
}

void CrossOrigin()
{
    Fixture f;
    {
        HttpPeer other;
        CHECK(f.GetPath("/a"));
        auto b = f.GetPath(other.base_url() + "/b");
        CHECK(b && b.value().body == "peer-default");
        CHECK(f.GetPath("/a-again"));
        CHECK(f.peer.accept_total() == 2 && other.accept_total() == 1);
        CHECK(f.client->Stats().budget <= 1);
        other.Stop();
    }
    f.Finish();
}

void HandoffConcurrency()
{
    Fixture f;
    std::promise<void> release;
    auto ready = release.get_future().share();
    std::atomic<bool> first_seen{false};
    f.peer.PushConnectionHook([&, ready](int fd) {
        CHECK(HttpPeer::ReadRequestForHook(fd));
        first_seen = true;
        ready.wait();
        CHECK(HttpPeer::SendForHook(fd, HttpPeer::HttpResponse(200, "OK", "ok")));
        for (unsigned i = 0; i != 8; ++i) {
            CHECK(HttpPeer::ReadRequestForHook(fd));
            CHECK(HttpPeer::SendForHook(fd, HttpPeer::HttpResponse(200, "OK", "ok")));
        }
    });
    auto first = f.host.Submit([&] { return bbt_co_await f.client->Get("/first", {}, Until()); });
    CHECK(WaitUntil([&] { return first_seen.load(); }));
    std::vector<std::future<result<http::Response>>> waiting;
    for (unsigned i = 0; i != 8; ++i) {
        waiting.push_back(f.host.Submit([&] { return bbt_co_await f.client->Get("/handoff", {}, Until()); }));
    }
    CHECK(WaitUntil([&] { return f.client->Stats().waiting == waiting.size(); }));
    release.set_value();
    CHECK(Get(first));
    for (auto& future : waiting) {
        auto value = Get(future);
        CHECK(value && value.value().body == "ok");
    }
    CHECK(f.peer.accept_total() == 1);
    CHECK(f.client->Stats().active == 0 && f.client->Stats().waiting == 0);
    f.Finish();
}

/**
 * 放弃连接（对端 RST）不得提前归还 inflight 额度：仍在执行的 handler 协程
 * 继续持有名额，新连接的新请求停在接纳门前，直到该 handler 真正退出。
 */
void AbandonHoldsInflight()
{
    Gate gate;
    Gate second_gate;
    http::ServerOptions options;

    /**
     * 2 个名额：A 的 handler 占住 1 个，reader 仍能继续 Read 并对 RST 作出反应。
     */
    options.limits.max_inflight = 2;

    struct ServerHost {
        std::shared_ptr<net::Engine> engine{net::Engine::Create(std::shared_ptr<net::RawHandler>{})};
        std::shared_ptr<http::Server> server;
        std::thread runner;
        std::error_code run_error;
        net::Address address;

        ServerHost(http::ServerHandler handler, http::ServerOptions options)
        {
            auto made = http::Server::Create(engine, std::move(handler), options);
            CHECK(made);
            server = std::move(made).value();
            net::EngineConfig config;
            config.runtime_progress = net::RuntimeProgress::Independent;
            config.options.num_event_loop = 1;
            std::error_code error;
            config.listen_addresses = {net::Address::Ip("127.0.0.1", 0, error)};
            CHECK(!error);
            runner = std::thread([this, config] { run_error = engine->Run(config); });
            CHECK(WaitUntil([&] { return engine->Phase() == net::EnginePhase::Running; }));
            std::vector<net::Address> addresses;
            CHECK(!engine->LocalAddresses(addresses) && addresses.size() == 1);
            address = addresses.front();
            CHECK(server->Start());
        }

        ~ServerHost()
        {
            if (server) {
                (void)server->Close(bbt::infra::CloseOptions::Immediate());
            }
            (void)engine->Close();
            if (runner.joinable()) {
                runner.join();
            }
        }

        void Finish()
        {
            if (!server->CloseReason().has_value()) {
                CHECK(server->Close(bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + 3s)));
                (void)server->Close(bbt::infra::CloseOptions::Immediate());
            }
            CHECK(server->Close(
                bbt::infra::CloseOptions{
                    bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + 3s}));
            (void)engine->Close();
            runner.join();
            CHECK(!run_error && engine->Phase() == net::EnginePhase::Stopped);
            const auto stats = server->Stats();
            CHECK(stats.tasks == 0 && stats.sessions == 0 && stats.inflight == 0);
        }
    };

    struct RawPeer {
        int fd{-1};

        explicit RawPeer(const net::Address& address)
        {
            fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
            CHECK(fd >= 0);
            sockaddr_in target{};
            target.sin_family = AF_INET;
            target.sin_port = htons(address.Port());
            target.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            CHECK(::connect(fd, reinterpret_cast<sockaddr*>(&target), sizeof(target)) == 0);
        }

        ~RawPeer()
        {
            if (fd >= 0) {
                ::close(fd);
            }
        }

        void Send(const std::string& bytes) { CHECK(HttpPeer::SendForHook(fd, bytes)); }

        bool Quiet(std::chrono::milliseconds patience)
        {
            pollfd pfd{fd, POLLIN, 0};
            return ::poll(&pfd, 1, static_cast<int>(patience.count())) == 0;
        }

        std::string ReadUntil(const std::string& needle, std::chrono::milliseconds timeout)
        {
            std::string out;
            const auto end = std::chrono::steady_clock::now() + timeout;
            while (out.find(needle) == std::string::npos && std::chrono::steady_clock::now() < end) {
                pollfd pfd{fd, POLLIN, 0};
                if (::poll(&pfd, 1, 50) <= 0) {
                    continue;
                }
                char bytes[4096];
                const auto size = ::recv(fd, bytes, sizeof(bytes), 0);
                if (size <= 0) {
                    break;
                }
                out.append(bytes, static_cast<std::size_t>(size));
            }
            return out;
        }

        void Reset()
        {
            linger reset{1, 0};
            CHECK(::setsockopt(fd, SOL_SOCKET, SO_LINGER, &reset, sizeof(reset)) == 0);
            ::close(fd);
            fd = -1;
        }
    };

    ServerHost host(
        [&](IncomingCallContext, http::IncomingRequest request) {
            if (request.target == "/first") {
                gate.Pause();
            } else if (request.target == "/b1") {
                second_gate.Pause();
            }
            return result<http::Response>::ok({200, {}, request.target});
        },
        options);
    RawPeer first(host.address);

    /**
     * 完整请求 + 半包请求：RST 时 reader 仍持有已开始的半包，走明确放弃路径，
     * 而不是把连接间 EOF 当干净断连交给 writer 排空。
     */
    first.Send("GET /first HTTP/1.1\r\nHost: a\r\n\r\nGET /half HTTP/1.1\r\nHost: a\r\n");
    CHECK(WaitUntil([&] { return gate.entered.load(); }));
    CHECK(host.server->Stats().inflight == 1);

    /**
     * RST 放弃连接；reader/writer 退出（tasks 只剩 accept + 阻塞的 handler），
     * 但 handler 协程仍存活，inflight 不得提前回零。
     */
    first.Reset();
    CHECK(WaitUntil([&] { return host.server->Stats().tasks == 2; }, 2s));
    CHECK(host.server->Stats().inflight == 1);

    /**
     * 新连接的 pipeline 只能消耗剩余 1 个名额：B1 占住它并阻塞，B2 停在
     * 接纳门前，直到放弃连接上的 handler 真正退出才可能被授权。
     */
    RawPeer second(host.address);
    second.Send("GET /b1 HTTP/1.1\r\nHost: a\r\n\r\nGET /b2 HTTP/1.1\r\nHost: a\r\n\r\n");
    CHECK(WaitUntil([&] { return host.server->Stats().accepted == 2; }, 2s));
    CHECK(WaitUntil([&] { return host.server->Stats().requests == 2; }, 2s));
    CHECK(WaitUntil([&] { return host.server->Stats().inflight == 2; }, 2s));
    std::this_thread::sleep_for(200ms);

    /**
     * B2 未被授权（旧行为会立刻变 3）
     */
    CHECK(host.server->Stats().requests == 2);

    /**
     * B1 仍阻塞
     */
    CHECK(second.ReadUntil("/b1", 100ms).find("/b1") == std::string::npos);

    gate.Release();
    second_gate.Release();

    /**
     * B1 先按序上线，B2 在 A 的 handler 退出后获授权；累积读到 /b2 即两者都已出现。
     */
    const auto replies = second.ReadUntil("/b2", 3s);
    CHECK(replies.find("/b1") != std::string::npos);
    CHECK(replies.find("/b2") != std::string::npos);
    CHECK(WaitUntil([&] { return host.server->Stats().inflight == 0; }, 2s));
    host.Finish();
    std::cout << "abandoned connection keeps inflight credit until handler exits\n";
}

void PhysicalBudget(unsigned maximum)
{
    Fixture f({}, maximum);
    Access::SetConnectDeliveryDelay(*f.host.engine, 400ms);
    http::RequestOptions options;
    options.SetConnectTimeout(20ms).SetPoolTimeout(40ms);
    for (unsigned i = 0; i != maximum; ++i) {
        auto dialing = f.host.Submit([&] { return bbt_co_await f.client->Get("/late", options, Until()); });
        CHECK(WaitUntil([&] { return f.peer.accept_total() == i + 1; }));
        ExpectCode(Get(dialing), ErrorCode::TimedOut);
    }
    CHECK(f.client->Stats().budget == maximum && f.client->Stats().retiring == maximum);
    for (unsigned i = 0; i != 3; ++i) {
        auto blocked = f.host.Call([&] { return bbt_co_await f.client->Get("/blocked", options, Until()); });
        ExpectCode(blocked, ErrorCode::TimedOut);
        CHECK(f.peer.accept_total() == maximum && f.client->Stats().budget == maximum);
        CHECK(SocketCount() <= f.baseline + 2 * maximum);
    }
    Access::SetConnectDeliveryDelay(*f.host.engine, 0ms);
    CHECK(WaitUntil([&] { return f.client->Stats().budget == 0; }));
    CHECK(f.GetPath("/recovered"));
    CHECK(f.GetPath("/reused"));
    CHECK(f.peer.accept_total() == maximum + 1 && f.peer.requests_total() == 2);
    f.Finish();
}

void PhysicalCancel()
{
    Fixture f;
    Access::SetConnectDeliveryDelay(*f.host.engine, 400ms);

    /**
     * 已提交且真实对端已 accept，才在同一协程置取消；不是入口预取消。
     */
    net::CoTestAccess::SetDialBarrier([&] {
        CHECK(WaitUntil([&] { return f.peer.accept_total() == 1; }));
        g_bbt_tls_coroutine_co->RequestCancel();
    });
    auto cancelled = f.host.Call([&] { return bbt_co_await f.client->Get("/cancel-dial", {}, Until()); });
    net::CoTestAccess::SetDialBarrier({});
    ExpectCode(cancelled, ErrorCode::Cancelled);
    CHECK(f.client->Stats().budget <= 1);
    Access::SetConnectDeliveryDelay(*f.host.engine, 0ms);
    CHECK(WaitUntil([&] { return f.client->Stats().budget == 0; }));
    CHECK(f.GetPath("/after-cancel"));
    CHECK(f.peer.accept_total() == 2 && f.peer.requests_total() == 1);
    f.Finish();
}

int Run(int argc, char** argv)
{
    Watchdog watchdog;
    try {
        CHECK(argc == 2);
        g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
        g_bbt_coroutine_config->m_cfg_stack_size = 128 * 1024;
        g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
        g_scheduler->Start();

        /**
         * 复用既有运行时暖机，使 fd 基线不含一次性初始化抖动。
         */
        {
            Host warm;
            CHECK(warm.Call([] { return true; }));
            warm.Finish();
        }
        const std::string mode = argv[1];
        if (mode == "fragment_header" || mode == "fragment_body" || mode == "chunked" || mode == "head_interim"
            || mode == "eof_truncated" || mode == "body_limit") {
            Protocol(mode);
        } else if (mode == "quotas") {
            Quotas();
        } else if (mode == "input_boundary") {
            InputBoundary();
        } else if (mode == "preexpired_cancelled") {
            PreexpiredCancelled();
        } else if (mode == "phase_timeouts") {
            PhaseTimeouts();
        } else if (mode == "per_read_total") {
            PerReadAndTotal();
        } else if (mode == "cross_origin") {
            CrossOrigin();
        } else if (mode == "handoff_concurrency") {
            HandoffConcurrency();
        } else if (mode == "physical_budget") {
            PhysicalBudget(1);
        } else if (mode == "physical_budget_two") {
            PhysicalBudget(2);
        } else if (mode == "physical_cancel") {
            PhysicalCancel();
        } else if (mode == "abandon_holds_inflight") {
            AbandonHoldsInflight();
        } else {
            CHECK(false);
        }
        std::cout << "http regression " << mode << " PASS\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "http regression FAIL: " << error.what() << '\n';
        return 1;
    }
}

// clang-format off
}
// clang-format on

int main(int argc, char** argv)
{
    return bbt_net_test::RunTestProcessExit([&] { return Run(argc, argv); });
}
