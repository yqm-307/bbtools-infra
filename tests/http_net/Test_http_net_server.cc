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
    std::shared_ptr<net::Engine> engine{std::make_shared<net::Engine>(std::shared_ptr<net::RawHandler>{})};
    std::shared_ptr<http::Server> server;
    std::thread runner;
    std::error_code run_error;
    net::Address address;
    ServerHost(http::ServerHandler handler, http::ServerOptions options = {}, bool start = true,
               std::int64_t outbound = 1024 * 1024, unsigned loops = 1) {
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
        if (start) { CHECK(server->Start()); }
    }
    ~ServerHost() {
        server->Close();
        engine->RequestClose();
        if (runner.joinable()) { runner.join(); }
    }
    std::string Url() const { return "http://127.0.0.1:" + std::to_string(address.Port()); }
    void Finish() {
        server->StopAccepting();
        CHECK(server->Drain(std::chrono::steady_clock::now() + 3s));
        server->Close();
        CHECK(server->WaitStopped(std::chrono::steady_clock::now() + 3s));
        auto stats = server->Stats();
        CHECK(stats.tasks == 0 && stats.sessions == 0 && stats.inflight == 0);
        CHECK(engine->AcceptingStopped());
        CHECK(engine->Phase() == net::EnginePhase::Running);
        engine->RequestClose();
        runner.join();
        CHECK(!run_error && engine->Phase() == net::EnginePhase::Stopped);
        CHECK(engine->CountConnections() == 0 && Access::FirstConnectionFd(*engine) == -1);
        std::cout << "server receipt http_tasks=0 sessions=0 inflight=0 listener_stopped=1 run_joined=1 connections=0\n";
    }
};

struct Peer {
    int fd{-1};
    std::string pending;
    explicit Peer(const net::Address& address, bool small_receive = false) {
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
    ~Peer() { if (fd >= 0) { ::close(fd); } }
    void Send(const std::string& bytes) { CHECK(HttpPeer::SendForHook(fd, bytes)); }
    http::Response Read(bool head = false) {
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
                if (parser.is_done()) { break; }
                if (consumed) { continue; }
            }
            char bytes[32768];
            auto size = ::recv(fd, bytes, sizeof(bytes), 0);
            CHECK(size > 0);
            pending.append(bytes, static_cast<std::size_t>(size));
        }
        const auto& message = parser.get();
        http::Response out{static_cast<int>(message.result_int()), {}, message.body()};
        for (auto& field : message) { out.headers.emplace_back(std::string(field.name_string()), std::string(field.value())); }
        return out;
    }
    bool Eof() {
        char bytes[64];
        const auto n = ::recv(fd, bytes, sizeof(bytes), 0);
        return n == 0 || (n < 0 && errno == ECONNRESET);
    }
};

struct NativeGate {
    std::promise<void> ready;
    std::future<void> arrival{ready.get_future()};
    std::promise<void> release;
    std::shared_future<void> released{release.get_future().share()};
    std::atomic<bool> claimed{false};
    std::atomic<bool> opened{false};
    void PauseOnce() {
        if (claimed.exchange(true)) { return; }
        ready.set_value();
        released.wait();
    }
    void Open() { if (!opened.exchange(true)) { release.set_value(); } }
};
struct OpenOnExit {
    NativeGate& gate;
    ~OpenOnExit() { gate.Open(); }
};

http::ServerHandler Echo() {
    return [](IncomingCallContext ctx, http::IncomingRequest request) {
        CHECK(ctx.peer_principal.empty());
        CHECK(ctx.deadline > std::chrono::steady_clock::now());
        return result<http::Response>::ok({request.target == "/missing" ? 404 : 200, {},
                                           request.method == "POST" ? request.body : request.target});
    };
}

void WireCases() {
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
    peer.Send("c\r\n2\r\nde\r\n0\r\n\r\nGET /next HTTP/1.1\r\nHost: localhost\r\n\r\n"
              "HEAD /head HTTP/1.1\r\nHost: localhost\r\n\r\n"
              "GET /204 HTTP/1.1\r\nHost: localhost\r\n\r\n"
              "GET /304 HTTP/1.1\r\nHost: localhost\r\n\r\n");
    CHECK(::shutdown(peer.fd, SHUT_WR) == 0); // FIN 后完整合法请求仍须回复
    CHECK(peer.Read().body == "abcde");
    CHECK(peer.Read().body == "/next");
    auto head = peer.Read(true);
    CHECK(head.status_code == 200 && head.body.empty());
    CHECK(peer.Read().status_code == 204);
    CHECK(peer.Read().status_code == 304);
    CHECK(peer.Eof() && peer.pending.empty()); // 无 HEAD/204/304 幽灵 body 污染下一响应
    CHECK(calls == 5 && host.server->Stats().accepted == 1);
    host.Finish();
}

void StopAuthorization() {
    Gate gate;
    std::atomic<unsigned> calls{0};
    std::shared_ptr<http::Server> server;
    ServerHost host([&](IncomingCallContext, http::IncomingRequest) {
        ++calls;
        ExpectCode(server->Drain(std::chrono::steady_clock::now() + 1s), ErrorCode::InvalidContext);
        ExpectCode(server->WaitStopped(std::chrono::steady_clock::now() + 1s), ErrorCode::InvalidContext);
        gate.Pause();
        return result<http::Response>::ok({200, {}, "authorized-reply"});
    });
    server = host.server;
    Peer peer(host.address);
    peer.Send("GET /one HTTP/1.1\r\nHost: localhost\r\n\r\nGET /two HTTP/1.1\r\nHost: localhost\r\n\r\n");
    CHECK(WaitUntil([&] { return gate.entered.load(); }));
    CHECK(server->Stats().inflight == 1);
    Peer idle(host.address);
    Peer partial(host.address);
    partial.Send("POST /partial HTTP/1.1\r\nHost: a\r\nContent-Length: 9\r\n\r\npart");
    CHECK(WaitUntil([&] { return server->Stats().sessions == 3; }));
    server->StopAccepting();
    CHECK(idle.Eof() && partial.Eof());
    ExpectCode(server->Drain(std::chrono::steady_clock::now() + 20ms), ErrorCode::TimedOut);
    ExpectCode(server->WaitStopped(std::chrono::steady_clock::now() + 20ms), ErrorCode::TimedOut);
    CHECK(calls == 1);
    gate.Release();
    CHECK(peer.Read().body == "authorized-reply");
    CHECK(peer.Eof());
    CHECK(calls == 1); // 同连接第二请求在停止门后不得授权
    host.Finish();
}

void ReplyDrain() {
    const std::string large(2 * 1024 * 1024, 'R');
    std::atomic<bool> returned{false};
    NativeGate gate;
    ServerHost host([&](IncomingCallContext, http::IncomingRequest) {
        returned = true;
        return result<http::Response>::ok({200, {}, large});
    }, {}, true, 8192); // 大响应显式超过 net 单次接纳上限，必须分块
    OpenOnExit cleanup{gate};
    Access::SetWriteReadyBarrierOnce(*host.engine, [&] { gate.PauseOnce(); });
    Peer peer(host.address);
    peer.Send("GET /large HTTP/1.1\r\nHost: localhost\r\n\r\n");
    CHECK(gate.arrival.wait_for(3s) == std::future_status::ready);
    CHECK(returned && host.server->Stats().inflight == 1);
    host.server->StopAccepting();
    auto drain = host.server->Drain(std::chrono::steady_clock::now() + 20ms);
    const bool held = !drain && drain.error().code == ErrorCode::TimedOut;
    gate.Open(); // 即使断言失败也先释放 owner，避免测试自身死锁
    CHECK(held);
    CHECK(peer.Read().body == large);
    CHECK(peer.Eof());
    host.Finish();
}

void CloseTimeout() {
    Gate gate;
    ServerHost host([&](IncomingCallContext, http::IncomingRequest) {
        gate.Pause();
        return result<http::Response>::ok({200, {}, "must-not-write"});
    });
    Peer peer(host.address);
    peer.Send("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    CHECK(WaitUntil([&] { return gate.entered.load(); }));
    host.server->Close();
    host.server->Close();
    ExpectCode(host.server->Drain(std::chrono::steady_clock::now() + 20ms), ErrorCode::TimedOut);
    ExpectCode(host.server->WaitStopped(std::chrono::steady_clock::now() + 20ms), ErrorCode::TimedOut);
    CHECK(host.server->Stats().inflight == 1 && host.server->Stats().tasks >= 1);
    CHECK(peer.Eof());
    gate.Release();
    host.Finish();
}

void Security() {
    std::atomic<unsigned> calls{0};
    http::ServerOptions options;
    options.limits.max_header_bytes = 512;
    options.max_target_bytes = 64;
    options.limits.max_body_bytes = 32;
    options.limits.incoming_timeout = 300ms;
    ServerHost host([&](IncomingCallContext, http::IncomingRequest request) -> result<http::Response> {
        ++calls;
        unsigned hosts = 0;
        for (const auto& field : request.headers) {
            if (boost::beast::iequals(field.first, "Host")) { ++hosts; CHECK(field.second == "a"); }
        }
        CHECK(hosts == 1);
        if (request.target == "/throw") { throw std::runtime_error("isolated"); }
        if (request.target == "/bad-status") { return result<http::Response>::ok({101, {}, {}}); }
        if (request.target == "/bad-header") { return result<http::Response>::ok({200, {{"X", "a\r\nb"}}, {}}); }
        if (request.target == "/bad-frame") { return result<http::Response>::ok({200, {{"Content-Length", "1"}}, "x"}); }
        return result<http::Response>::ok({200, {}, "ok"});
    }, options);
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
        "POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n"
    };
    for (const auto& request : invalid) {
        Peer peer(host.address);
        peer.Send(request);
        const bool rejected = peer.Eof();
        if (!rejected) { std::cerr << "unexpected acceptance: " << request << '\n'; }
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
        // Boost 1.90 parser::on_trailer_field_impl 丢弃 forbidden trailer；断言其
        // 不进入 owning headers/不覆盖 Host，而不是误要求合法消息必须断开。
        Peer peer(host.address);
        peer.Send("POST / HTTP/1.1\r\nHost: a\r\nConnection: close\r\nTransfer-Encoding: chunked\r\n\r\n"
                  "1\r\nx\r\n0\r\nHost: b\r\n\r\n");
        CHECK(peer.Read().body == "ok");
        CHECK(peer.Eof());
    }
    CHECK(calls == 5);
    host.Finish();
}

void IncomingDeadline() {
    Gate gate;
    std::promise<bbt::coroutine::Deadline> deadline;
    auto future = deadline.get_future();
    http::ServerOptions options;
    options.limits.incoming_timeout = 80ms;
    ServerHost host([&](IncomingCallContext ctx, http::IncomingRequest) {
        deadline.set_value(ctx.deadline);
        gate.Pause();
        return result<http::Response>::ok({200, {}, "late"});
    }, options);
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

void StartAndCapacity() {
    {
        auto engine = std::make_shared<net::Engine>(std::shared_ptr<net::RawHandler>{});
        auto made = http::Server::Create(engine, Echo());
        CHECK(made && engine->Phase() == net::EnginePhase::Idle);
        ExpectCode(made.value()->Start(), ErrorCode::InvalidContext);
        CHECK(made.value()->Stats().tasks == 0);
        made.value()->Close();
        CHECK(made.value()->WaitStopped(std::chrono::steady_clock::now() + 1s));
    }
    http::ServerOptions options;
    options.limits.max_connections = 1;
    options.handoff_capacity = 1;
    options.limits.incoming_timeout = 200ms;
    ServerHost host(Echo(), options, false);
    ExpectCode(host.server->Drain(std::chrono::steady_clock::now()), ErrorCode::InvalidArgument);
    http::ServerTestAccess::FailNextTask(*host.server);
    ExpectCode(host.server->Start(), ErrorCode::RuntimeUnavailable);
    ExpectCode(host.server->Start(), ErrorCode::Closed);
    CHECK(host.server->Stats().tasks == 0);
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

void PhysicalCapacity() {
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
    // 第二连接进入另一真实 sub owner/handoff，第一 fd 仍被屏障扣留。
    CHECK(WaitUntil([&] { return Access::ConnectionCount(*host.engine) == 2; }));
    CHECK(host.server->Stats().accepted == 1 && host.server->Stats().requests == 1);
    CHECK(host.server->Stats().sessions == 1);
    gate.Open();
    CHECK(second.Read().body == "/second");
    CHECK(first.Eof() && second.Eof());
    CHECK(host.server->Stats().accepted == 2 && host.server->Stats().requests == 2);
    host.Finish();
}

void InflightLimit() {
    Gate gate;
    std::atomic<unsigned> calls{0};
    http::ServerOptions options;
    options.limits.max_inflight = 1;
    ServerHost host([&](IncomingCallContext, http::IncomingRequest request) {
        ++calls;
        gate.Pause();
        return result<http::Response>::ok({200, {}, request.target});
    }, options);
    Peer first(host.address);
    first.Send("GET /active HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n");
    CHECK(WaitUntil([&] { return gate.entered.load(); }));
    Peer second(host.address);
    second.Send("GET /overload HTTP/1.1\r\nHost: a\r\n\r\n");
    CHECK(second.Eof());
    CHECK(calls == 1 && host.server->Stats().inflight == 1 && host.server->Stats().accepted == 2);
    gate.Release();
    CHECK(first.Read().body == "/active" && first.Eof());
    host.Finish();
}

int Run(int argc, char** argv) {
    Watchdog watchdog;
    try {
        CHECK(argc == 2);
        g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
        g_bbt_coroutine_config->m_cfg_stack_size = 128 * 1024;
        g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
        g_scheduler->Start();
        const auto baseline = SocketCount();
        const std::string name = argv[1];
        if (name == "server_wire") { WireCases(); }
        else if (name == "server_stop_authorization") { StopAuthorization(); }
        else if (name == "server_reply_drain") { ReplyDrain(); }
        else if (name == "server_close_timeout") { CloseTimeout(); }
        else if (name == "server_security") { Security(); }
        else if (name == "server_incoming_deadline") { IncomingDeadline(); }
        else if (name == "server_start_capacity") { StartAndCapacity(); }
        else if (name == "server_physical_capacity") { PhysicalCapacity(); }
        else if (name == "server_inflight_limit") { InflightLimit(); }
        else { CHECK(false); }
        CHECK(WaitUntil([&] { return SocketCount() == baseline; }));
        std::cout << "PASS " << name << " socket_baseline=" << baseline << " restored=1\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL " << error.what() << '\n';
        return 1;
    }
}
} // namespace
int main(int argc, char** argv) {
    return bbt_net_test::RunTestProcessExit([&] { return Run(argc, argv); });
}
