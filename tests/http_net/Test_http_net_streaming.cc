#include "TestSupport.hpp"
#include <bbt/infra/http/Server.hpp>
#include <bbt/infra/http/detail/ServerTestAccess.hpp>
#include <boost/beast/http.hpp>
#include "../net/ProcessRuntimeTestMain.hpp"

// server 端 chunked streaming 契约（architecture §7）：
//   - wire 字节：Transfer-Encoding: chunked + 十六进制块 + 0 终止块；
//   - ServerReply variant：Response 直回、StreamStart::Buffered、Chunked 三路；
//   - HEAD/204/304 拒绝 chunked；commit 前 Start 错误走 500，commit 后错误断连；
//   - peer FIN/RST、slow reader 背压、stream lifetime、逐写 deadline；
//   - StopAccepting/Drain/Close/WaitStopped 覆盖流终态与连接 Released；
//   - Cancel 幂等一次性；released 凭据走既有 server receipt。

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
    ServerHost(http::StreamingServerHandler handler, http::ServerOptions options = {},
               http::StreamingOptions streaming = {}, bool start = true,
               std::int64_t outbound = 1024 * 1024, unsigned loops = 1,
               int send_buffer = 4096) {
        auto made = http::Server::CreateStreaming(engine, std::move(handler), options, streaming);
        CHECK(made);
        server = std::move(made).value();
        CHECK(engine->Phase() == net::EnginePhase::Idle);
        net::EngineConfig config;
        config.runtime_progress = net::RuntimeProgress::Independent;
        config.options.num_event_loop = loops;
        config.options.max_outbound_bytes = outbound;
        config.options.socket_send_buffer = send_buffer;
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

// 拉取源骨架：脚本化 chunks（含暂停/错误注入），记录 Start/Next/Cancel 调用证据。
// Pause 步在传入 WaitOptions 期限上等待内部 release waiter：测试经 Release()
// 放行；到期返回 TimedOut（等价真实源遵守 deadline）。Cancel() 释放等待者，
// 保证 Close/Cancel 后源能立即收口而不是挂到期限。
class ListBody : public http::ResponseBody {
public:
    struct Step {
        enum class Kind { Chunk, Eof, Error, Pause } kind;
        std::string data;
        ErrorCode code{ErrorCode::InternalError};
    };
    http::StreamStart start;
    std::vector<Step> steps;
    std::atomic<unsigned> starts{0}, nexts{0}, cancels{0}, pauses{0};
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> release{
        bbt::coroutine::sync::CoWaiter::Create()};

    static std::shared_ptr<ListBody> Make(http::StreamStart start, std::vector<Step> steps) {
        auto body = std::make_shared<ListBody>();
        body->start = std::move(start);
        body->steps = std::move(steps);
        return body;
    }
    void Release() { release->Notify(); }

    bbt_co_async(result<http::StreamStart>) Start(const bbt::coroutine::WaitOptions&) override {
        ++starts;
        return result<http::StreamStart>::ok(start);
    }
    bbt_co_async(result<std::optional<std::string>>) Next(
        const bbt::coroutine::WaitOptions& options) override {
        const auto index = nexts.fetch_add(1);
        if (index >= steps.size()) {
            return result<std::optional<std::string>>::ok(std::optional<std::string>{});
        }
        const auto& step = steps[index];
        switch (step.kind) {
        case Step::Kind::Chunk:
            return result<std::optional<std::string>>::ok(
                std::optional<std::string>{step.data});
        case Step::Kind::Eof:
            return result<std::optional<std::string>>::ok(std::optional<std::string>{});
        case Step::Kind::Error:
            return result<std::optional<std::string>>::err(MakeError(step.code, "source error"));
        case Step::Kind::Pause: {
            ++pauses;
            const auto status = release->WaitWithCallback(options, [] { return true; });
            if (status == bbt::coroutine::WaitStatus::TimedOut ||
                status == bbt::coroutine::WaitStatus::Cancelled) {
                return result<std::optional<std::string>>::err(
                    MakeError(ErrorCode::TimedOut, "source wait expired"));
            }
            return result<std::optional<std::string>>::ok(
                std::optional<std::string>{step.data});
        }
        }
        return result<std::optional<std::string>>::err(
            MakeError(ErrorCode::InternalError, "unreachable"));
    }
    void Cancel() noexcept override {
        ++cancels;
        release->Notify();
    }
};

using Step = ListBody::Step;
Step Chunk(std::string data) { return {Step::Kind::Chunk, std::move(data), {}}; }
Step Eof() { return {Step::Kind::Eof, {}, {}}; }
Step Fail(ErrorCode code) { return {Step::Kind::Error, {}, code}; }
Step Pause(std::string data = {}) { return {Step::Kind::Pause, std::move(data), {}}; }

struct Peer {
    int fd{-1};
    std::string pending;
    explicit Peer(const net::Address& address) {
        fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        CHECK(fd >= 0);
        timeval timeout{5, 0};
        CHECK(::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
        CHECK(::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0);
        sockaddr_in target{};
        target.sin_family = AF_INET;
        target.sin_port = htons(address.Port());
        target.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        CHECK(::connect(fd, reinterpret_cast<sockaddr*>(&target), sizeof(target)) == 0);
    }
    ~Peer() { if (fd >= 0) { ::close(fd); } }
    void Send(const std::string& bytes) { CHECK(HttpPeer::SendForHook(fd, bytes)); }
    // 读原始字节（含小睡等的有限尝试；不做协议解析，chunk wire 逐字节比对）。
    std::string ReadRaw(std::size_t want, std::chrono::milliseconds patience = 3s) {
        const auto end = std::chrono::steady_clock::now() + patience;
        while (pending.size() < want) {
            const auto left = end - std::chrono::steady_clock::now();
            if (left <= 0ms) { break; }
            pollfd pfd{fd, POLLIN, 0};
            const auto ms = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(left).count());
            if (::poll(&pfd, 1, std::max(1, ms)) != 1) { break; }
            char bytes[65536];
            const auto n = ::recv(fd, bytes, sizeof(bytes), 0);
            if (n <= 0) { break; }
            pending.append(bytes, static_cast<std::size_t>(n));
        }
        auto out = pending.substr(0, want);
        pending.erase(0, out.size());
        return out;
    }
    // 读至 EOF 或超时，返回全部字节（诊断/断言用）。
    std::string Drain(std::chrono::milliseconds patience = 3s) {
        const auto end = std::chrono::steady_clock::now() + patience;
        for (;;) {
            const auto left = end - std::chrono::steady_clock::now();
            if (left <= 0ms) { break; }
            pollfd pfd{fd, POLLIN, 0};
            const auto ms = static_cast<int>(std::chrono::duration_cast<std::chrono::milliseconds>(left).count());
            if (::poll(&pfd, 1, std::max(1, ms)) != 1) { break; }
            char bytes[65536];
            const auto n = ::recv(fd, bytes, sizeof(bytes), 0);
            if (n <= 0) { break; }
            pending.append(bytes, static_cast<std::size_t>(n));
        }
        auto out = std::move(pending);
        pending.clear();
        return out;
    }
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

http::StreamingServerHandler Route(
    std::function<result<http::ServerReply>(IncomingCallContext, http::IncomingRequest)> fn) {
    return fn;
}

result<http::ServerReply> Buffered(int status, std::string body, http::Headers headers = {}) {
    return result<http::ServerReply>::ok(http::ServerReply{http::Response{status, std::move(headers), std::move(body)}});
}
result<http::ServerReply> Stream(std::shared_ptr<http::ResponseBody> body) {
    return result<http::ServerReply>::ok(http::ServerReply{std::move(body)});
}

// 1) chunked wire 字节证据：TE 头 + HEX 块边界 + 0 终止块；keep-alive 后连接复用。
void ChunkedWire() {
    std::atomic<unsigned> calls{0};
    auto body = ListBody::Make(
        {http::StreamStart::Mode::Chunked, 200, {{"X-Stream", "yes"}}, "", {}},
        {Chunk("hello-"), Chunk(std::string(300, 'x')), Eof()});
    ServerHost host(Route([&](IncomingCallContext, http::IncomingRequest request) {
        ++calls;
        if (request.target == "/stream") { return Stream(body); }
        return Buffered(200, "plain");
    }));
    Peer peer(host.address);
    peer.Send("GET /stream HTTP/1.1\r\nHost: a\r\n\r\n");
    const auto wire_bytes = peer.Drain(1500ms);
    CHECK(wire_bytes.find("HTTP/1.1 200") == 0);
    CHECK(wire_bytes.find("Transfer-Encoding: chunked") != std::string::npos);
    CHECK(wire_bytes.find("X-Stream: yes") != std::string::npos);
    CHECK(wire_bytes.find("Content-Length") == std::string::npos);
    const auto body_start = wire_bytes.find("\r\n\r\n");
    CHECK(body_start != std::string::npos);
    const std::string chunks = wire_bytes.substr(body_start + 4);
    CHECK(chunks.find("6\r\nhello-\r\n") == 0);
    CHECK(chunks.find("12C\r\n" + std::string(300, 'x') + "\r\n") != std::string::npos);
    CHECK(chunks.size() >= 5 && chunks.compare(chunks.size() - 5, 5, "0\r\n\r\n") == 0);
    // 流 EOF 后 keep-alive：同连接普通 buffered 请求仍可服务（Drain 已读至暂停，
    // 连接未被服务器关闭时 poll 超时返回，pending 清空）。
    peer.Send("GET /plain HTTP/1.1\r\nHost: a\r\n\r\n");
    const auto plain = peer.Drain(1500ms);
    CHECK(plain.find("HTTP/1.1 200") == 0 && plain.find("plain") != std::string::npos);
    CHECK(body->starts == 1 && body->nexts == 3 && body->cancels == 0);
    CHECK(calls == 2);
    ::shutdown(peer.fd, SHUT_RDWR);
    CHECK(WaitUntil([&] { return host.server->Stats().sessions == 0; }));
    host.Finish();
}

// 2) ServerReply variant：Response 与 StreamStart::Buffered 均走普通 buffered 路径。
void VariantBuffered() {
    auto body = ListBody::Make({http::StreamStart::Mode::Buffered, 201,
                                {{"X-Mode", "buffered"}}, "body-from-start", ""},
                               {Fail(ErrorCode::InternalError)}); // Next 不应被调用
    ServerHost host(Route([&](IncomingCallContext, http::IncomingRequest request) {
        if (request.target == "/buffered") { return Stream(body); }
        if (request.target == "/direct") { return Buffered(202, "direct-reply"); }
        return Buffered(404, "missing");
    }));
    {
        Peer peer(host.address);
        peer.Send("GET /buffered HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n");
        const auto bytes = peer.ReadRaw(512);
        CHECK(bytes.find("HTTP/1.1 201") == 0);
        CHECK(bytes.find("Content-Length: 15") != std::string::npos);
        CHECK(bytes.find("body-from-start") != std::string::npos);
        CHECK(bytes.find("Transfer-Encoding") == std::string::npos);
        CHECK(peer.Eof());
    }
    {
        Peer peer(host.address);
        peer.Send("GET /direct HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n");
        const auto bytes = peer.ReadRaw(512);
        CHECK(bytes.find("HTTP/1.1 202") == 0 && bytes.find("direct-reply") != std::string::npos);
        CHECK(peer.Eof());
    }
    CHECK(body->starts == 1 && body->nexts == 0);
    host.Finish();
}

// 3) HEAD/204/304 拒绝 chunked：Start 已调用但 commit 前协议拒绝、明确断开。
void ChunkedDisallowed() {
    std::atomic<unsigned> calls{0};
    auto make_body = [&](int status) {
        return ListBody::Make({http::StreamStart::Mode::Chunked, status, {}, {}, "x"}, {Eof()});
    };
    ServerHost host(Route([&](IncomingCallContext, http::IncomingRequest request) {
        ++calls;
        if (request.target == "/204") { return Stream(make_body(204)); }
        if (request.target == "/304") { return Stream(make_body(304)); }
        return Stream(make_body(200));
    }));
    {
        Peer peer(host.address);
        peer.Send("HEAD /head HTTP/1.1\r\nHost: a\r\n\r\n");
        CHECK(peer.Eof()); // chunked + HEAD：拒绝并关连接
    }
    {
        Peer peer(host.address);
        peer.Send("GET /204 HTTP/1.1\r\nHost: a\r\n\r\n");
        CHECK(peer.Eof());
    }
    {
        Peer peer(host.address);
        peer.Send("GET /304 HTTP/1.1\r\nHost: a\r\n\r\n");
        CHECK(peer.Eof());
    }
    CHECK(calls == 3);
    host.Finish();
}

// Start 失败的源包装：Start 返回 err，Next 不可达。
class FailStartBody : public http::ResponseBody {
public:
    explicit FailStartBody(std::shared_ptr<ListBody> inner) : m_inner(std::move(inner)) {}
    bbt_co_async(result<http::StreamStart>) Start(const bbt::coroutine::WaitOptions&) override {
        ++m_inner->starts;
        return result<http::StreamStart>::err(MakeError(ErrorCode::InternalError, "start failed"));
    }
    bbt_co_async(result<std::optional<std::string>>) Next(
        const bbt::coroutine::WaitOptions&) override {
        ++m_inner->nexts;
        return result<std::optional<std::string>>::err(
            MakeError(ErrorCode::InternalError, "must not reach"));
    }
    void Cancel() noexcept override { ++m_inner->cancels; }
private:
    std::shared_ptr<ListBody> m_inner;
};
std::shared_ptr<http::ResponseBody> BodyStartFails(std::shared_ptr<ListBody> inner) {
    return std::make_shared<FailStartBody>(std::move(inner));
}

// 4) commit 前后错误分界：Start err → 500；Next err（commit 后）→ 断连无终止块。
void CommitBoundary() {
    ServerHost host(Route([&](IncomingCallContext, http::IncomingRequest request) {
        if (request.target == "/start-fail") {
            auto body = ListBody::Make({http::StreamStart::Mode::Chunked, 200, {}, "", {}}, {Eof()});
            body->start.status_code = 200;
            return Stream(BodyStartFails(std::move(body)));
        }
        if (request.target == "/mid-fail") {
            return Stream(ListBody::Make({http::StreamStart::Mode::Chunked, 200, {}, {}, "first"},
                                         {Chunk("second"), Fail(ErrorCode::InternalError)}));
        }
        return Buffered(200, "ok");
    }));
    {
        Peer peer(host.address);
        peer.Send("GET /start-fail HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n");
        const auto bytes = peer.ReadRaw(256);
        CHECK(bytes.find("HTTP/1.1 500") == 0); // commit 前错误=正常错误响应
        CHECK(peer.Eof());
    }
    {
        Peer peer(host.address);
        peer.Send("GET /mid-fail HTTP/1.1\r\nHost: a\r\n\r\n");
        const auto bytes = peer.Drain(1500ms);
        CHECK(bytes.find("HTTP/1.1 200") == 0);
        CHECK(bytes.find("5\r\nfirst\r\n") != std::string::npos);
        CHECK(bytes.find("6\r\nsecond\r\n") != std::string::npos);
        CHECK(bytes.find("0\r\n\r\n") == std::string::npos); // 错误后不得伪造终止块
        CHECK(peer.Eof());
    }
    host.Finish();
}

// 5) peer FIN/RST：写路径失败即收口（不重发），连接关闭、源取消一次。
void PeerAbort() {
    auto body = ListBody::Make({http::StreamStart::Mode::Chunked, 200, {}, {}, "first"},
                               {Chunk("second"), Pause(), Chunk("never")});
    ServerHost host(Route([&](IncomingCallContext, http::IncomingRequest) {
        return Stream(body);
    }));
    {
        Peer peer(host.address);
        peer.Send("GET / HTTP/1.1\r\nHost: a\r\n\r\n");
        const auto bytes = peer.Drain(800ms);
        CHECK(bytes.find("HTTP/1.1 200") == 0); // 头已提交
        CHECK(WaitUntil([&] { return body->pauses.load() >= 1; }));
        // RST：源挂于 Pause 时断链；放行后写出必失败 → 取消源、关连接。
        linger rst{1, 0};
        CHECK(::setsockopt(peer.fd, SOL_SOCKET, SO_LINGER, &rst, sizeof(rst)) == 0);
        ::close(peer.fd);
        peer.fd = -1;
        body->Release();
        CHECK(WaitUntil([&] { return body->cancels.load() >= 1; }, 5s));
    }
    CHECK(WaitUntil([&] { return host.server->Stats().inflight == 0 && host.server->Stats().sessions == 0; }, 5s));
    CHECK(body->cancels == 1);
    host.Finish();
}

// 6) slow reader 背压：小发送缓冲 + 大 chunk + 短 stream_write_timeout，写超时后
//    断连且源取消；Drain 计数覆盖到最终写终态而非 handler 返回。
//    阶段证据：写超时计时从请求时刻起算，取消断言 ≥ 配置 timeout 才可通过；
//    与 B3 的 max_chunk_bytes 守卫取消区分（守卫路径 elapsed < 1ms）。
void SlowReaderBackpressure() {
    const std::string big(512 * 1024, 'B');
    auto body = ListBody::Make({http::StreamStart::Mode::Chunked, 200, {}, "", {}},
                               {Chunk(big), Chunk(big), Eof()});
    http::ServerOptions options;
    http::StreamingOptions streaming;
    streaming.max_chunk_bytes = 1024 * 1024; // ≥ 512KiB 块通过 max_chunk 守卫，进入写超时阶段
    streaming.stream_write_timeout = 120ms;
    ServerHost host(Route([&](IncomingCallContext, http::IncomingRequest) {
        return Stream(body);
    }), options, streaming, true, 8 * 1024, 1, 2048); // 小 outbound/send buffer 放大背压
    Peer peer(host.address);
    int rcvbuf = 2048;
    CHECK(::setsockopt(peer.fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf)) == 0);
    peer.Send("GET / HTTP/1.1\r\nHost: a\r\n\r\n");
    // 不读制造 slow reader；写侧在 stream_write_timeout 内失败 → 取消源、断连。
    const auto started = std::chrono::steady_clock::now();
    CHECK(WaitUntil([&] { return body->cancels.load() >= 1; }, 6s));
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - started);
    // 阶段证据：必须实际等待 write timeout（≥120ms）；max_chunk 守卫路径 <1ms。
    CHECK(elapsed >= streaming.stream_write_timeout);
    std::cout << "stage_evidence write_timeout_elapsed_ms=" << elapsed.count()
              << " configured_timeout_ms=" << streaming.stream_write_timeout.count() << "\n";
    CHECK(WaitUntil([&] { return peer.Eof(); }, 5s));
    host.Finish();
}

// 7) 生命周期收口：StopAccepting→Drain(超时)→Close(取消源)→WaitStopped→Released。
void LifecycleDrain() {
    auto body = ListBody::Make({http::StreamStart::Mode::Chunked, 200, {}, {}, "head"},
                               {Pause("tail"), Eof()});
    ServerHost host(Route([&](IncomingCallContext, http::IncomingRequest) {
        return Stream(body);
    }));
    Peer peer(host.address);
    peer.Send("GET / HTTP/1.1\r\nHost: a\r\n\r\n");
    CHECK(peer.ReadRaw(128).find("200") != std::string::npos);
    CHECK(WaitUntil([&] { return body->pauses.load() >= 1; }));
    CHECK(host.server->Stats().inflight == 1);
    host.server->StopAccepting();
    ExpectCode(host.server->Drain(std::chrono::steady_clock::now() + 30ms), ErrorCode::TimedOut);
    host.server->Close(); // commit 后：取消源 + 关连接
    CHECK(WaitUntil([&] { return body->cancels.load() >= 1; }));
    CHECK(peer.Eof());
    body->Release();
    CHECK(host.server->WaitStopped(std::chrono::steady_clock::now() + 3s));
    CHECK(host.server->Stats().tasks == 0 && host.server->Stats().sessions == 0);
    CHECK(body->cancels == 1);
    host.Finish();
}

// 8) 流总期限：max_stream_lifetime 到期 → Next 内等待按传入 deadline 到期失败，
//    提交后错误路径取消源并关连接。
void StreamLifetime() {
    auto body = ListBody::Make({http::StreamStart::Mode::Chunked, 200, {}, {}, "x"},
                               {Chunk("y"), Pause(), Eof()});
    http::StreamingOptions streaming;
    streaming.max_stream_lifetime = 150ms;
    ServerHost host(Route([&](IncomingCallContext, http::IncomingRequest) {
        return Stream(body);
    }), {}, streaming);
    Peer peer(host.address);
    peer.Send("GET / HTTP/1.1\r\nHost: a\r\n\r\n");
    const auto bytes = peer.ReadRaw(256);
    CHECK(bytes.find("200") != std::string::npos);
    // Pause 挂起的 Next 持 stream_deadline；到期 → 源 err → 取消 + 断连。
    CHECK(WaitUntil([&] { return peer.Eof(); }, 5s));
    CHECK(WaitUntil([&] { return body->cancels.load() >= 1; }, 2s));
    CHECK(WaitUntil([&] { return host.server->Stats().sessions == 0; }, 5s));
    host.Finish();
}

// 9) Start 遵守传入期限：超 incoming 总期限返回 err；此时总期限已尽、回复写
//    不再可行，按「超限明确断开」收口（500 路径已由 commit_boundary/start-fail 覆盖）。
void StartDeadline() {
    http::ServerOptions options;
    options.limits.incoming_timeout = 100ms;
    class SlowStartBody : public http::ResponseBody {
    public:
        std::shared_ptr<bbt::coroutine::sync::CoWaiter> release{
            bbt::coroutine::sync::CoWaiter::Create()};
        std::atomic<unsigned> cancels{0};
        bbt_co_async(result<http::StreamStart>) Start(
            const bbt::coroutine::WaitOptions& options) override {
            const auto status = release->WaitWithCallback(options, [] { return true; });
            if (status == bbt::coroutine::WaitStatus::TimedOut) {
                return result<http::StreamStart>::err(MakeError(ErrorCode::TimedOut, "start expired"));
            }
            return result<http::StreamStart>::ok(
                http::StreamStart{http::StreamStart::Mode::Chunked, 200, {}, {}, "late"});
        }
        bbt_co_async(result<std::optional<std::string>>) Next(
            const bbt::coroutine::WaitOptions&) override {
            return result<std::optional<std::string>>::ok(std::optional<std::string>{});
        }
        void Cancel() noexcept override { ++cancels; release->Notify(); }
    };
    auto body = std::make_shared<SlowStartBody>();
    ServerHost host(Route([&](IncomingCallContext, http::IncomingRequest) {
        return Stream(body);
    }), options);
    Peer peer(host.address);
    peer.Send("GET / HTTP/1.1\r\nHost: a\r\n\r\n");
    CHECK(WaitUntil([&] { return peer.Eof(); }, 5s)); // 期限尽：放弃连接不写 500
    CHECK(WaitUntil([&] { return host.server->Stats().sessions == 0; }, 5s));
    host.Finish();
}

// 10) Close-during-handler：handler 内 Close 后返回 ResponseBody；source 注册时
//     closing 已置位，ServeStream 检测后取消一次、不 Start、连接关闭。
void CloseDuringHandler() {
    Gate gate; // handler 暂停屏障：测试线程在 handler 执行期间调用 Close
    auto body = ListBody::Make({http::StreamStart::Mode::Chunked, 200, {}, "", "x"}, {Eof()});
    ServerHost host(Route([&](IncomingCallContext, http::IncomingRequest) {
        gate.Pause(); // 阻塞 handler 执行直到 Close 发生
        return Stream(body);
    }));
    Peer peer(host.address);
    peer.Send("GET / HTTP/1.1\r\nHost: a\r\n\r\n");
    CHECK(WaitUntil([&] { return gate.entered.load(); }));
    host.server->Close(); // handler 执行期间 Close；此时 session->source 尚未注册
    gate.Release();       // 放行 handler 返回 StreamStart
    CHECK(WaitUntil([&] { return body->cancels.load() >= 1; }, 5s));
    CHECK(WaitUntil([&] { return peer.Eof(); }, 5s));
    CHECK(WaitUntil([&] { return host.server->Stats().sessions == 0; }, 5s));
    CHECK(body->cancels == 1);  // 恰好一次
    CHECK(body->starts == 0);   // closing 窗口：source 不应被 Start
    CHECK(body->nexts == 0);
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
        if (name == "stream_wire") { ChunkedWire(); }
        else if (name == "stream_variant_buffered") { VariantBuffered(); }
        else if (name == "stream_disallowed") { ChunkedDisallowed(); }
        else if (name == "stream_commit_boundary") { CommitBoundary(); }
        else if (name == "stream_peer_abort") { PeerAbort(); }
        else if (name == "stream_slow_reader") { SlowReaderBackpressure(); }
        else if (name == "stream_lifecycle") { LifecycleDrain(); }
        else if (name == "stream_lifetime") { StreamLifetime(); }
        else if (name == "stream_start_deadline") { StartDeadline(); }
        else if (name == "stream_close_in_handler") { CloseDuringHandler(); }
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
