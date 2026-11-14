#include "TestSupport.hpp"
#include <bbt/infra/http/Server.hpp>
#include <bbt/infra/http/detail/ServerTestAccess.hpp>
#include <boost/beast/http.hpp>
#include "../net/ProcessRuntimeTestMain.hpp"

using namespace http_net_test;
namespace http = bbt::infra::http;
namespace wire = boost::beast::http;

namespace {
struct ServerHost {
    std::shared_ptr<net::Engine> engine{net::Engine::Create(std::shared_ptr<net::RawHandler>{})};
    std::shared_ptr<http::Server> server;
    std::thread runner;
    std::error_code run_error;
    net::Address address;

    ServerHost(
        http::ServerHandler handler,
        http::ServerOptions options = {},
        bool start = true,
        std::int64_t outbound = 1024 * 1024,
        unsigned loops = 1)
    {
        auto made = http::Server::Create(engine, std::move(handler), options);
        CHECK(made);
        server = std::move(made).value();
        CHECK(engine->Phase() == net::EnginePhase::Idle);
        net::EngineConfig config;
        config.runtime_progress = net::RuntimeProgress::Independent;
        config.options.num_event_loop = loops;
        config.options.max_outbound_bytes = outbound;
        config.options.socket_send_buffer = 4096;
        std::error_code error;
        config.listen_addresses = {net::Address::Ip("127.0.0.1", 0, error)};
        CHECK(!error);
        runner = std::thread([this, config] { run_error = engine->Run(config); });
        CHECK(WaitUntil([&] { return engine->Phase() == net::EnginePhase::Running; }));
        std::vector<net::Address> addresses;
        CHECK(!engine->LocalAddresses(addresses) && addresses.size() == 1);
        address = addresses[0];
        if (start) {
            CHECK(server->Start());
        }
    }

    ~ServerHost()
    {
        (void)server->Close(bbt::infra::CloseOptions::Immediate());
        (void)engine->Close();
        if (runner.joinable()) {
            runner.join();
        }
    }

    std::string Url() const { return "http://127.0.0.1:" + std::to_string(address.Port()); }

    void Finish()
    {

        /**
         * 已 Close 时 Drain 会如实返回关闭原因，不能再当收口前奏；直接用
         * Shutdown（未 Close 时）→ Close → WaitStopped 的收口序列。
         */
        if (!server->CloseReason().has_value()) {
            CHECK(server->Close(bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + 3s)));
            (void)server->Close(bbt::infra::CloseOptions::Immediate());
        }
        CHECK(server->Close(
            bbt::infra::CloseOptions{
                bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + 3s}));
        auto stats = server->Stats();
        CHECK(stats.tasks == 0 && stats.sessions == 0 && stats.inflight == 0);
        CHECK(engine->AcceptingStopped());
        CHECK(engine->Phase() == net::EnginePhase::Running);
        (void)engine->Close();
        runner.join();
        CHECK(!run_error && engine->Phase() == net::EnginePhase::Stopped);
        CHECK(engine->CountConnections() == 0 && Access::FirstConnectionFd(*engine) == -1);
        std::cout
            << "server receipt http_tasks=0 sessions=0 inflight=0 listener_stopped=1 run_joined=1 connections=0\n";
    }
};

struct Peer {
    int fd{-1};
    std::string pending;

    explicit Peer(const net::Address& address, bool small_receive = false)
    {
        fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        CHECK(fd >= 0);
        timeval timeout{5, 0};
        CHECK(::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
        CHECK(::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0);
        if (small_receive) {
            int size = 4096;
            CHECK(::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof(size)) == 0);
        }
        sockaddr_in target{};
        target.sin_family = AF_INET;
        target.sin_port = htons(address.Port());
        target.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        CHECK(::connect(fd, reinterpret_cast<sockaddr*>(&target), sizeof(target)) == 0);
    }

    ~Peer()
    {
        if (fd >= 0) {
            ::close(fd);
        }
    }

    void Send(const std::string& bytes) { CHECK(HttpPeer::SendForHook(fd, bytes)); }

    http::Response Read(bool head = false)
    {
        wire::response_parser<wire::string_body> parser;
        parser.body_limit(16 * 1024 * 1024);
        parser.eager(true);
        parser.skip(head);
        while (!parser.is_done()) {
            if (!pending.empty()) {
                boost::system::error_code error;
                const auto consumed = parser.put(boost::asio::buffer(pending), error);
                pending.erase(0, consumed);
                CHECK(!error || error == wire::error::need_more);
                if (parser.is_done()) {
                    break;
                }
                if (consumed) {
                    continue;
                }
            }
            char bytes[32768];
            auto size = ::recv(fd, bytes, sizeof(bytes), 0);
            CHECK(size > 0);
            pending.append(bytes, static_cast<std::size_t>(size));
        }
        const auto& message = parser.get();
        http::Response out{static_cast<int>(message.result_int()), {}, message.body()};
        for (auto& field : message) {
            out.headers.emplace_back(std::string(field.name_string()), std::string(field.value()));
        }
        return out;
    }

    bool Eof()
    {
        char bytes[64];
        const auto n = ::recv(fd, bytes, sizeof(bytes), 0);
        return n == 0 || (n < 0 && errno == ECONNRESET);
    }

    /**
     * 在 patience 内既无数据也未 EOF（用于证明背压期间连接保持打开）。
     */
    bool Quiet(std::chrono::milliseconds patience)
    {
        pollfd pfd{fd, POLLIN, 0};
        return ::poll(&pfd, 1, static_cast<int>(patience.count())) == 0;
    }
};

struct NativeGate {
    std::promise<void> ready;
    std::future<void> arrival{ready.get_future()};
    std::promise<void> release;
    std::shared_future<void> released{release.get_future().share()};
    std::atomic<bool> claimed{false};
    std::atomic<bool> opened{false};

    void PauseOnce()
    {
        if (claimed.exchange(true)) {
            return;
        }
        ready.set_value();
        released.wait();
    }

    void Open()
    {
        if (!opened.exchange(true)) {
            release.set_value();
        }
    }
};

struct OpenOnExit {
    NativeGate& gate;

    ~OpenOnExit() { gate.Open(); }
};

http::ServerHandler Echo()
{
    return [](IncomingCallContext ctx, http::IncomingRequest request) {
        CHECK(ctx.peer_principal.empty());
        CHECK(ctx.deadline > std::chrono::steady_clock::now());
        return result<http::Response>::ok(
            {request.target == "/missing" ? 404 : 200, {}, request.method == "POST" ? request.body : request.target});
    };
}

void WireCases()
{
    std::atomic<unsigned> calls{0};
    ServerHost host([&](IncomingCallContext, http::IncomingRequest request) {
        ++calls;
        int status = request.target == "/204" ? 204 : request.target == "/304" ? 304 : 200;
        return result<http::Response>::ok({status, {}, request.method == "POST" ? request.body : request.target});
    });
    Peer peer(host.address);
    peer.Send("POST /echo HTTP/1.1\r\nHo");
    CHECK(WaitUntil([&] { return host.server->Stats().sessions == 1; }));
    CHECK(calls == 0);
    peer.Send("st: localhost\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nab");
    CHECK(calls == 0);
    peer.Send(
        "c\r\n2\r\nde\r\n0\r\n\r\nGET /next HTTP/1.1\r\nHost: localhost\r\n\r\n"
        "HEAD /head HTTP/1.1\r\nHost: localhost\r\n\r\n"
        "GET /204 HTTP/1.1\r\nHost: localhost\r\n\r\n"
        "GET /304 HTTP/1.1\r\nHost: localhost\r\n\r\n");

    /**
     * FIN 后完整合法请求仍须回复
     */
    CHECK(::shutdown(peer.fd, SHUT_WR) == 0);
    CHECK(peer.Read().body == "abcde");
    CHECK(peer.Read().body == "/next");
    auto head = peer.Read(true);
    CHECK(head.status_code == 200 && head.body.empty());
    CHECK(peer.Read().status_code == 204);
    CHECK(peer.Read().status_code == 304);

    /**
     * 无 HEAD/204/304 幽灵 body 污染下一响应
     */
    CHECK(peer.Eof() && peer.pending.empty());
    CHECK(calls == 5 && host.server->Stats().accepted == 1);
    host.Finish();
}

void StopAuthorization()
{
    Gate gate;
    std::atomic<unsigned> calls{0};
    std::shared_ptr<http::Server> server;
    ServerHost host([&](IncomingCallContext, http::IncomingRequest) {
        ++calls;
        ExpectCode(
            server->Close(bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + 1s)),
            ErrorCode::InvalidContext);
        ExpectCode(
            server->Close(
                bbt::infra::CloseOptions{
                    bbt::infra::CloseMode::Graceful, true, {}, std::chrono::steady_clock::now() + 1s}),
            ErrorCode::InvalidContext);
        gate.Pause();
        return result<http::Response>::ok({200, {}, "authorized-reply"});
    });
    server = host.server;
    Peer peer(host.address);
    peer.Send("GET /one HTTP/1.1\r\nHost: localhost\r\n\r\n");
    CHECK(WaitUntil([&] { return gate.entered.load(); }));
    CHECK(server->Stats().inflight == 1);
    Peer idle(host.address);
    Peer partial(host.address);
    partial.Send("POST /partial HTTP/1.1\r\nHost: a\r\nContent-Length: 9\r\n\r\npart");
    CHECK(WaitUntil([&] { return server->Stats().sessions == 3; }));
    server->StopAccepting();

    /**
     * 封口后到达的 pipeline 请求不得再授权；读取侧停下，已授权回复仍须写完。
     */
    peer.Send("GET /two HTTP/1.1\r\nHost: localhost\r\n\r\n");
    CHECK(idle.Eof() && partial.Eof());
    ExpectCode(
        server->Close(bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + 20ms)),
        ErrorCode::TimedOut);
    ExpectCode(
        server->Close(
            bbt::infra::CloseOptions{
                bbt::infra::CloseMode::Graceful, true, {}, std::chrono::steady_clock::now() + 20ms}),
        ErrorCode::TimedOut);
    CHECK(calls == 1);
    gate.Release();
    CHECK(peer.Read().body == "authorized-reply");
    CHECK(peer.Eof());

    /**
     * 同连接第二请求在停止门后不得授权
     */
    CHECK(calls == 1);
    host.Finish();
}

void ReplyDrain()
{
    const std::string large(2 * 1024 * 1024, 'R');
    std::atomic<bool> returned{false};
    NativeGate gate;

    /**
     * 大响应显式超过 net 单次接纳上限，必须分块
     */
    ServerHost host(
        [&](IncomingCallContext, http::IncomingRequest) {
            returned = true;
            return result<http::Response>::ok({200, {}, large});
        },
        {},
        true,
        8192);
    OpenOnExit cleanup{gate};
    Access::SetWriteReadyBarrierOnce(*host.engine, [&] { gate.PauseOnce(); });
    Peer peer(host.address);
    peer.Send("GET /large HTTP/1.1\r\nHost: localhost\r\n\r\n");
    CHECK(gate.arrival.wait_for(3s) == std::future_status::ready);
    CHECK(returned && host.server->Stats().inflight == 1);
    host.server->StopAccepting();
    auto drain = host.server->Close(bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + 20ms));
    const bool held = !drain && drain.error().code == ErrorCode::TimedOut;

    /**
     * 即使断言失败也先释放 owner，避免测试自身死锁
     */
    gate.Open();
    CHECK(held);
    CHECK(peer.Read().body == large);
    CHECK(peer.Eof());
    host.Finish();
}

void CloseTimeout()
{
    Gate gate;
    std::atomic<bool> handler_done{false};
    ServerHost host([&](IncomingCallContext, http::IncomingRequest) {
        gate.Pause();
        handler_done = true;
        return result<http::Response>::ok({200, {}, "must-not-write"});
    });
    Peer peer(host.address);
    peer.Send("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    CHECK(WaitUntil([&] { return gate.entered.load(); }));
    (void)host.server->Close(bbt::infra::CloseOptions::Immediate());
    (void)host.server->Close(bbt::infra::CloseOptions::Immediate());

    /**
     * Close 唤醒已登记的 Drain 等待：立即返回关闭原因，不再按 inflight 无限等。
     */
    auto drained = host.server->Close(bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + 20ms));
    CHECK(!drained && drained.error().code == ErrorCode::Closed);
    auto reason = host.server->CloseReason();
    CHECK(reason.has_value() && reason->code == ErrorCode::Closed);
    ExpectCode(
        host.server->Close(
            bbt::infra::CloseOptions{
                bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + 20ms}),
        ErrorCode::TimedOut);

    /**
     * Immediate 已让在途序号失败，但 handler 协程仍阻塞：inflight 名额保留到
     * 协程退出，不能提前把容量还给新请求；任务责任同样保留。
     */
    CHECK(host.server->Stats().inflight == 1 && host.server->Stats().tasks >= 1);
    CHECK(!handler_done.load());
    CHECK(peer.Eof());
    gate.Release();
    host.Finish();
}

/**
 * Shutdown：封口 + 等已授权 handler/reply 真实排空。有限预算内 inflight>0
 * 诚实 TimedOut 且保持封口（不隐式转 Close、不发布关闭原因）；handler 放行后
 * 再等待即成功。
 */
void ShutdownDrain()
{
    Gate gate;
    ServerHost host([&](IncomingCallContext, http::IncomingRequest) {
        gate.Pause();
        return result<http::Response>::ok({200, {}, "drained"});
    });
    Peer peer(host.address);
    peer.Send("GET /drain HTTP/1.1\r\nHost: localhost\r\n\r\n");
    CHECK(WaitUntil([&] { return gate.entered.load(); }));
    CHECK(host.server->Stats().inflight == 1);

    ExpectCode(
        host.server->Close(bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + 50ms)),
        ErrorCode::TimedOut);
    auto first = host.server->CloseReason();

    /**
     * 封口即发布首因，到期不回滚
     */
    CHECK(first.has_value() && first->code == ErrorCode::Closed);

    gate.Release();
    CHECK(peer.Read().body == "drained");
    CHECK(peer.Eof());
    CHECK(host.server->Close(bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + 2s)));
    (void)host.server->Close(
        bbt::infra::CloseOptions::Immediate(MakeError(ErrorCode::Unavailable, "owner shutdown complete")));
    auto reason = host.server->CloseReason();

    /**
     * 首因固定，升级不覆盖
     */
    CHECK(reason.has_value() && reason->code == ErrorCode::Closed);
    host.Finish();
}

/**
 * Shutdown 超时后仍拒新：同连接后续请求不再授权，且服务保持封口直到显式 Close。
 */
void ShutdownTimeout()
{
    Gate gate;
    std::atomic<unsigned> calls{0};
    ServerHost host([&](IncomingCallContext, http::IncomingRequest) {
        ++calls;
        gate.Pause();
        return result<http::Response>::ok({200, {}, "late"});
    });
    Peer peer(host.address);
    peer.Send("GET /first HTTP/1.1\r\nHost: localhost\r\n\r\n");
    CHECK(WaitUntil([&] { return gate.entered.load(); }));
    ExpectCode(
        host.server->Close(bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + 50ms)),
        ErrorCode::TimedOut);
    CHECK(host.server->CloseReason().has_value());
    CHECK(calls == 1);
    gate.Release();
    CHECK(peer.Read().body == "late");

    /**
     * 封口后回复 keep_alive=false
     */
    CHECK(peer.Eof());
    host.Finish();
}

/**
 * Close 打断正在等待的 Shutdown：以「Shutdown 返回时 handler 仍在 inflight」
 * 证明等待被打断，并返回真实（自定义）关闭原因。
 */
void ShutdownInterruptedByClose()
{
    Gate gate;
    std::atomic<bool> handler_done{false};
    ServerHost host([&](IncomingCallContext, http::IncomingRequest) {
        gate.Pause();
        handler_done = true;
        return result<http::Response>::ok({200, {}, "must-not-write"});
    });
    Peer peer(host.address);
    peer.Send("GET /hold HTTP/1.1\r\nHost: localhost\r\n\r\n");
    CHECK(WaitUntil([&] { return gate.entered.load(); }));

    std::optional<result<void>> shutdown_result;
    std::thread waiter([&] {
        shutdown_result.emplace(
            host.server->Close(bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + 10s)));
    });

    /**
     * 屏障：优雅关闭已封口并发布首因（只读观察，不重复触发封口）。
     */
    CHECK(WaitUntil([&] { return host.server->CloseReason().has_value(); }));

    /**
     * 仍在排空
     */
    CHECK(!shutdown_result.has_value());
    (void)host.server->Close(bbt::infra::CloseOptions::Immediate(MakeError(ErrorCode::Unavailable, "owner abort")));
    waiter.join();

    CHECK(shutdown_result.has_value());
    CHECK(!*shutdown_result);
    if (shutdown_result && !*shutdown_result) {
        CHECK(shutdown_result->error().code == ErrorCode::Closed);
        CHECK(shutdown_result->error().message == "http server: closed by owner");
    }

    /**
     * handler 协程仍运行，放弃后额度未归还
     */
    CHECK(host.server->Stats().inflight == 1);

    /**
     * handler 仍阻塞：确为 Close 打断
     */
    CHECK(!handler_done.load());
    gate.Release();
    host.Finish();
}

void Security()
{
    std::atomic<unsigned> calls{0};
    http::ServerOptions options;
    options.limits.max_header_bytes = 512;
    options.max_target_bytes = 64;
    options.limits.max_body_bytes = 32;
    options.limits.incoming_timeout = 300ms;
    ServerHost host(
        [&](IncomingCallContext, http::IncomingRequest request) -> result<http::Response> {
            ++calls;
            unsigned hosts = 0;
            for (const auto& field : request.headers) {
                if (boost::beast::iequals(field.first, "Host")) {
                    ++hosts;
                    CHECK(field.second == "a");
                }
            }
            CHECK(hosts == 1);
            if (request.target == "/throw") {
                throw std::runtime_error("isolated");
            }
            if (request.target == "/bad-status") {
                return result<http::Response>::ok({101, {}, {}});
            }
            if (request.target == "/bad-header") {
                return result<http::Response>::ok({200, {{"X", "a\r\nb"}}, {}});
            }
            if (request.target == "/bad-frame") {
                return result<http::Response>::ok({200, {{"Content-Length", "1"}}, "x"});
            }
            return result<http::Response>::ok({200, {}, "ok"});
        },
        options);
    const std::vector<std::string> invalid{
        "GET / HTTP/1.1\r\n\r\n",
        "GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n",
        "GET / HTTP/1.1\r\nHost: a:invalid\r\n\r\n",
        "GET / HTTP/1.1\r\nHost: .\r\n\r\n",
        "GET /bad%Q0 HTTP/1.1\r\nHost: a\r\n\r\n",
        "GET /bad\\path HTTP/1.1\r\nHost: a\r\n\r\n",
        "POST / HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: 4\r\n\r\n",
        "CONNECT a:80 HTTP/1.1\r\nHost: a\r\n\r\n",
        "GET / HTTP/1.1\r\nHost: a\r\nConnection: upgrade\r\nUpgrade: websocket\r\n\r\n",
        "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 33\r\n\r\n",
        "GET /" + std::string(65, 'a') + " HTTP/1.1\r\nHost: a\r\n\r\n",
        "GET / HTTP/1.1\r\nHost: a\r\nX: " + std::string(513, 'h') + "\r\n\r\n",
        "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n"};
    for (const auto& request : invalid) {
        Peer peer(host.address);
        peer.Send(request);
        const bool rejected = peer.Eof();
        if (!rejected) {
            std::cerr << "unexpected acceptance: " << request << '\n';
        }
        CHECK(rejected);
    }
    CHECK(calls == 0);
    for (const std::string target : {"/bad-status", "/bad-header", "/bad-frame"}) {
        Peer peer(host.address);
        peer.Send("GET " + target + " HTTP/1.1\r\nHost: a\r\n\r\n");
        CHECK(peer.Eof());
    }
    {
        Peer peer(host.address);
        peer.Send("GET /throw HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n");
        CHECK(peer.Read().status_code == 500);
        CHECK(peer.Eof());
    }
    {

        /**
         * Boost 1.90 parser::on_trailer_field_impl 丢弃 forbidden trailer；断言其
         * 不进入 owning headers/不覆盖 Host，而不是误要求合法消息必须断开。
         */
        Peer peer(host.address);
        peer.Send(
            "POST / HTTP/1.1\r\nHost: a\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n"
            "1\r\nx\r\n0\r\nHost: b\r\n\r\n");
        CHECK(peer.Read().body == "ok");
        CHECK(peer.Eof());
    }
    CHECK(calls == 5);
    host.Finish();
}

void IncomingDeadline()
{
    Gate gate;
    std::promise<bbt::coroutine::Deadline> deadline;
    auto future = deadline.get_future();
    http::ServerOptions options;
    options.limits.incoming_timeout = 80ms;
    ServerHost host(
        [&](IncomingCallContext ctx, http::IncomingRequest) {
            deadline.set_value(ctx.deadline);
            gate.Pause();
            return result<http::Response>::ok({200, {}, "late"});
        },
        options);
    {
        Peer partial(host.address);
        partial.Send("POST /slow HTTP/1.1\r\nHost: a\r\nContent-Length: 8\r\n\r\npart");
        CHECK(partial.Eof());
        CHECK(host.server->Stats().requests == 0);
    }
    Peer peer(host.address);
    peer.Send("GET / HTTP/1.1\r\nHost: a\r\n\r\n");
    const auto end = Get(future);
    CHECK(WaitUntil([&] { return gate.entered.load(); }));
    std::this_thread::sleep_until(end + 5ms);
    gate.Release();
    CHECK(peer.Eof());
    CHECK(host.server->Stats().requests == 1);
    host.Finish();
}

void StartAndCapacity()
{
    {
        auto engine = net::Engine::Create(std::shared_ptr<net::RawHandler>{});
        auto made = http::Server::Create(engine, Echo());
        CHECK(made && engine->Phase() == net::EnginePhase::Idle);
        ExpectCode(made.value()->Start(), ErrorCode::InvalidContext);
        CHECK(made.value()->Stats().tasks == 0);
        (void)made.value()->Close(bbt::infra::CloseOptions::Immediate());
        (void)CHECK(made.value()->Close(
            bbt::infra::CloseOptions{
                bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + 1s}));
    }
    http::ServerOptions options;
    options.limits.max_connections = 1;
    options.handoff_capacity = 1;
    options.limits.incoming_timeout = 200ms;
    ServerHost host(Echo(), options, false);
    http::ServerTestAccess::FailNextTask(*host.server);
    ExpectCode(host.server->Start(), ErrorCode::RuntimeUnavailable);
    CHECK(host.server->Stats().tasks == 0);

    /**
     * 未 Start 的 Graceful 没有在途责任：封口即完成（不再要求先 StopAccepting）。
     */
    CHECK(host.server->Close(bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + 1s)));
    CHECK(host.server->CloseReason().has_value());
    ExpectCode(host.server->Start(), ErrorCode::Closed);
    host.Finish();
    {
        ServerHost next(Echo(), options);
        ExpectCode(next.server->Start(), ErrorCode::Closed);
        http::ServerTestAccess::FailNextTask(*next.server);
        Peer first(next.address);
        first.Send("GET / HTTP/1.1\r\nHost: a\r\n\r\n");
        CHECK(first.Eof());
        CHECK(WaitUntil([&] { return next.server->Stats().sessions == 0; }));
        Peer second(next.address);
        second.Send("GET /good HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n");
        CHECK(second.Read().body == "/good");
        CHECK(second.Eof());
        CHECK(next.server->Stats().accepted == 2 && next.server->Stats().requests == 1);
        next.Finish();
    }
}

void PhysicalCapacity()
{
    NativeGate gate;
    http::ServerOptions options;
    options.limits.max_connections = 1;
    options.handoff_capacity = 1;
    options.limits.incoming_timeout = 3s;
    ServerHost host(Echo(), options, true, 1024 * 1024, 2);
    OpenOnExit cleanup{gate};
    Access::SetShutdownBarrierOnce(*host.engine, [&] { gate.PauseOnce(); });
    Peer first(host.address);
    first.Send("GET /first HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n");
    CHECK(first.Read().body == "/first");
    CHECK(gate.arrival.wait_for(3s) == std::future_status::ready);
    CHECK(WaitUntil([&] { return host.server->Stats().tasks == 1; }));
    const auto held = host.server->Stats();
    CHECK(held.sessions == 1 && held.retiring == 1 && held.inflight == 0);
    Peer second(host.address);
    second.Send("GET /second HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n");

    /**
     * 第二连接进入另一真实 sub owner/handoff，第一 fd 仍被屏障扣留。
     */
    CHECK(WaitUntil([&] { return Access::ConnectionCount(*host.engine) == 2; }));
    CHECK(host.server->Stats().accepted == 1 && host.server->Stats().requests == 1);
    CHECK(host.server->Stats().sessions == 1);
    gate.Open();
    CHECK(second.Read().body == "/second");
    CHECK(first.Eof() && second.Eof());
    CHECK(host.server->Stats().accepted == 2 && host.server->Stats().requests == 2);
    host.Finish();
}

/**
 * 全局 inflight 是硬背压：额度满时 reader 暂停读/解析，连接保持打开、已读字节
 * 不丢；名额释放后继续按请求顺序服务。不是「超限即断开」。
 */
void InflightLimit()
{
    Gate gate;
    std::atomic<unsigned> calls{0};
    http::ServerOptions options;
    options.limits.max_inflight = 1;
    ServerHost host(
        [&](IncomingCallContext, http::IncomingRequest request) {
            if (calls.fetch_add(1) == 0) {
                gate.Pause();
            }
            return result<http::Response>::ok({200, {}, request.target});
        },
        options);
    Peer first(host.address);
    first.Send("GET /active HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n");
    CHECK(WaitUntil([&] { return gate.entered.load(); }));
    Peer second(host.address);
    second.Send("GET /overload HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n");

    /**
     * 名额满：第二连接既不回包也不断开，reader 停在接纳门前。
     */
    CHECK(second.Quiet(150ms));
    CHECK(calls == 1 && host.server->Stats().inflight == 1 && host.server->Stats().accepted == 2);
    gate.Release();
    CHECK(first.Read().body == "/active" && first.Eof());
    CHECK(second.Read().body == "/overload" && second.Eof());
    CHECK(calls == 2 && host.server->Stats().requests == 2);
    CHECK(WaitUntil([&] { return host.server->Stats().inflight == 0 && host.server->Stats().sessions == 0; }));
    host.Finish();
}

/**
 * 请求级派发：同连接 pipeline 中 B 的 handler 先完成，线上仍按 A→B 回复。
 * 执行顺序（副作用）与响应顺序分开断言；A 未完成时 writer 不得抢写 B。
 */
void OversizedLaterResponse();

void PipelineOrder()
{
    Gate gate_a;
    std::atomic<unsigned> side{0};
    std::atomic<unsigned> a_side{0};
    std::atomic<unsigned> b_side{0};
    std::atomic<bool> b_done{false};
    ServerHost host([&](IncomingCallContext, http::IncomingRequest request) {
        const auto current = ++side;
        if (request.target == "/a") {
            a_side = current;
            gate_a.Pause();
            return result<http::Response>::ok({200, {}, "A"});
        }
        if (request.target == "/b") {
            b_side = current;
            b_done = true;
            return result<http::Response>::ok({200, {}, "B"});
        }
        return result<http::Response>::ok({200, {}, request.target});
    });
    Peer peer(host.address);
    peer.Send(
        "GET /a HTTP/1.1\r\nHost: a\r\n\r\nGET /b HTTP/1.1\r\nHost: a\r\n\r\n"
        "GET /c HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n");
    CHECK(WaitUntil([&] { return gate_a.entered.load(); }));
    CHECK(WaitUntil([&] { return b_done.load(); }, 2s));

    /**
     * A 先执行，B 在 A 仍挂起时完成
     */
    CHECK(a_side == 1 && b_side > a_side);
    CHECK(host.server->Stats().inflight >= 2);

    /**
     * 前序未完成：B 的响应不得抢写
     */
    CHECK(peer.Quiet(150ms));
    auto draining = host.server->Close(bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + 30ms));
    CHECK(!draining && draining.error().code == ErrorCode::TimedOut);

    /**
     * 超时保留责任，不谎报排空
     */
    CHECK(host.server->Stats().inflight >= 2);
    gate_a.Release();
    CHECK(peer.Read().body == "A");
    CHECK(peer.Read().body == "B");
    CHECK(peer.Read().body == "/c");
    CHECK(host.server->Close(bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + 2s)));
    host.Finish();
    OversizedLaterResponse();
}

/**
 * 后序超大响应在发布入队前被拒绝——结果不进入队列，整条连接按协议放弃。
 * 前序已接纳但在飞的请求随连接明确失败（inflight 结算归零），线上不出现超大
 * 响应；A 的迟到结果只释放自身，不复活连接。
 *
 * 后半段用真实收口握手而不是固定 sleep：A 的 handler 返回后置 a_produced，
 * 受管任务数落到只剩 accept（1）才说明 A 的 handler 任务已退出——发布点
 * Publish 在该任务体内同步执行，任务退出即证明迟到结果确实到达并被丢弃。
 */
void OversizedLaterResponse()
{
    Gate gate_a;
    std::atomic<bool> a_produced{false};
    std::atomic<bool> b_produced{false};
    http::ServerOptions options;
    options.limits.max_body_bytes = 64;
    ServerHost host(
        [&](IncomingCallContext, http::IncomingRequest request) {
            if (request.target == "/a") {
                gate_a.Pause();
                a_produced = true;
                return result<http::Response>::ok({200, {}, "A"});
            }
            b_produced = true;
            return result<http::Response>::ok({200, {}, std::string(4096, 'x')});
        },
        options);
    Peer peer(host.address);
    peer.Send("GET /a HTTP/1.1\r\nHost: a\r\n\r\nGET /big HTTP/1.1\r\nHost: a\r\n\r\n");
    CHECK(WaitUntil([&] { return gate_a.entered.load(); }));
    CHECK(WaitUntil([&] { return b_produced.load(); }, 2s));

    /**
     * 前半段：B 的结果已产生、A 仍挂起时超限结果被丢弃，连接放弃后 B 的额度归还，
     * 但 A 的 handler 协程仍在执行，其 inflight 名额保留；线上无响应字节，只有该
     * 连接被放弃，Server 本身不封口。受管任务只剩 accept 与挂起的 A handler。
     */
    CHECK(WaitUntil([&] { return host.server->Stats().inflight == 1; }, 2s));
    CHECK(peer.Eof());
    CHECK(!host.server->CloseReason().has_value());
    CHECK(WaitUntil([&] { return host.server->Stats().tasks == 2; }, 2s));

    /**
     * 后半段：释放 A → 等 handler 返回 + 其任务退出（tasks==1）。Publish 在
     * handler 任务体内、任务退出之前执行，因此该握手证明迟到结果实际到达发布点
     * 并被丢弃，而不是只凭固定等待后的 inflight==0。
     */
    gate_a.Release();
    CHECK(WaitUntil([&] { return a_produced.load() && host.server->Stats().tasks == 1; }, 2s));
    CHECK(host.server->Stats().inflight == 0);
    CHECK(peer.Eof());
    CHECK(!host.server->CloseReason().has_value());
    host.Finish();
    std::cout
        << "server oversized later response rejected before enqueue; late result reached publish and was dropped\n";
}

/**
 * Immediate：封口时 A 仍挂起、B 已完成；两条都明确放弃且 inflight 只结算一次。
 * A 的迟到结果不得复活连接或补写响应。
 */
void PipelineCloseImmediate()
{
    Gate gate_a;
    std::atomic<bool> b_done{false};
    ServerHost host([&](IncomingCallContext, http::IncomingRequest request) {
        if (request.target == "/a") {
            gate_a.Pause();
            return result<http::Response>::ok({200, {}, "A"});
        }
        b_done = true;
        return result<http::Response>::ok({200, {}, "B"});
    });
    Peer peer(host.address);
    peer.Send("GET /a HTTP/1.1\r\nHost: a\r\n\r\nGET /b HTTP/1.1\r\nHost: a\r\n\r\n");
    CHECK(WaitUntil([&] { return gate_a.entered.load(); }));
    CHECK(WaitUntil([&] { return b_done.load(); }, 2s));
    CHECK(host.server->Stats().inflight == 2);
    (void)host.server->Close(bbt::infra::CloseOptions::Immediate());

    /**
     * 未写任何回复即明确放弃
     */
    CHECK(peer.Eof());

    /**
     * A 的 handler 协程仍阻塞：放弃只归还已完成的 B 的名额，A 的名额保留到
     * 协程退出。
     */
    CHECK(WaitUntil([&] { return host.server->Stats().inflight == 1; }));

    /**
     * 迟到结果：只释放自身
     */
    gate_a.Release();
    CHECK(WaitUntil([&] { return host.server->Stats().inflight == 0; }));
    CHECK(peer.Eof());
    host.Finish();
}

/**
 * 连级解析失败：A 已接纳并挂起，后序请求帧非法；整条连接明确失败，A 不补写，
 * 后续序号不永久等待。
 */
void PipelineParseFailure()
{
    Gate gate_a;
    ServerHost host([&](IncomingCallContext, http::IncomingRequest) {
        gate_a.Pause();
        return result<http::Response>::ok({200, {}, "A"});
    });
    Peer peer(host.address);
    peer.Send("GET /a HTTP/1.1\r\nHost: a\r\n\r\nPOST /bad HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n");
    CHECK(WaitUntil([&] { return gate_a.entered.load(); }));

    /**
     * A 的 handler 协程仍阻塞：解析失败放弃整条连接，但 A 的 inflight 名额
     * 保留到协程退出。
     */
    CHECK(WaitUntil([&] { return host.server->Stats().inflight == 1; }));
    CHECK(peer.Eof());
    gate_a.Release();
    CHECK(WaitUntil([&] { return host.server->Stats().inflight == 0; }));
    CHECK(peer.Eof());
    host.Finish();
}

/**
 * 对端断开（RST）：已接纳请求明确放弃并结算；writer 不再写残连接。
 */
void PipelineDisconnect()
{
    Gate gate_a;
    ServerHost host([&](IncomingCallContext, http::IncomingRequest) {
        gate_a.Pause();
        return result<http::Response>::ok({200, {}, "A"});
    });
    Peer peer(host.address);
    peer.Send("GET /a HTTP/1.1\r\nHost: a\r\n\r\n");
    CHECK(WaitUntil([&] { return gate_a.entered.load(); }));
    CHECK(host.server->Stats().inflight == 1);
    linger reset{1, 0};
    CHECK(::setsockopt(peer.fd, SOL_SOCKET, SO_LINGER, &reset, sizeof(reset)) == 0);
    ::close(peer.fd);
    peer.fd = -1;

    /**
     * handler 放行后 writer 的写必然失败：请求仍以写失败终态结算，不挂起。
     */
    gate_a.Release();
    CHECK(WaitUntil([&] { return host.server->Stats().inflight == 0 && host.server->Stats().sessions == 0; }));
    std::this_thread::sleep_for(50ms);
    CHECK(host.server->Stats().inflight == 0);
    host.Finish();
}

int Run(int argc, char** argv)
{
    Watchdog watchdog;
    try {
        CHECK(argc == 2);
        g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
        g_bbt_coroutine_config->m_cfg_stack_size = 128 * 1024;
        g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
        g_scheduler->Start();
        const auto baseline = SocketCount();
        const std::string name = argv[1];
        if (name == "server_wire") {
            WireCases();
        } else if (name == "server_stop_authorization") {
            StopAuthorization();
        } else if (name == "server_reply_drain") {
            ReplyDrain();
        } else if (name == "server_close_timeout") {
            CloseTimeout();
        } else if (name == "server_shutdown_drain") {
            ShutdownDrain();
        } else if (name == "server_shutdown_timeout") {
            ShutdownTimeout();
        } else if (name == "server_shutdown_interrupted_by_close") {
            ShutdownInterruptedByClose();
        } else if (name == "server_security") {
            Security();
        } else if (name == "server_incoming_deadline") {
            IncomingDeadline();
        } else if (name == "server_start_capacity") {
            StartAndCapacity();
        } else if (name == "server_physical_capacity") {
            PhysicalCapacity();
        } else if (name == "server_inflight_limit") {
            InflightLimit();
        } else if (name == "server_pipeline_order") {
            PipelineOrder();
        } else if (name == "server_pipeline_close_immediate") {
            PipelineCloseImmediate();
        } else if (name == "server_pipeline_parse_failure") {
            PipelineParseFailure();
        } else if (name == "server_pipeline_disconnect") {
            PipelineDisconnect();
        } else {
            CHECK(false);
        }
        CHECK(WaitUntil([&] { return SocketCount() == baseline; }));
        std::cout << "PASS " << name << " socket_baseline=" << baseline << " restored=1\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL " << error.what() << '\n';
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
