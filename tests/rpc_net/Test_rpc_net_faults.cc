// rpc_net 核心错误矩阵：调用输入边界、传输/HTTP 错误映射、畸形回复、
// 未知结果（OutcomeUnknown + request_phase 事实）、协程取消透传，以及
// 服务端对畸形请求/坏 handler 的错误信封（真实双端 + raw POSIX 对端）。

#include "TestSupport.hpp"

#include <cstdlib>

namespace rpc_net_test {
namespace {

using rpc::RpcWireEnvelope;

void ClientInput() {
    ServerHost server(EchoHandler());
    ClientFixture client(server.Base());
    // transport 必须 http。
    {
        auto bad = client.host.Call([&] {
            return bbt_co_await client.client->Call({"tcp", server.Base()},
                MakeEnvelope("s", "m", "i-1"), Until());
        });
        ExpectCode(bad, ErrorCode::InvalidArgument);
    }
    // endpoint 非空。
    {
        auto bad = client.host.Call([&] {
            return bbt_co_await client.client->Call({"http", ""},
                MakeEnvelope("s", "m", "i-2"), Until());
        });
        ExpectCode(bad, ErrorCode::InvalidArgument);
    }
    // endpoint 是裸 authority：path/scheme/userinfo/空白都拒绝。
    for (const char* endpoint : {"127.0.0.1:9/rpc", "http://127.0.0.1:9",
                                 "user@127.0.0.1:9", "127.0.0.1:9 x"}) {
        auto bad = client.host.Call([&] {
            return bbt_co_await client.client->Call({"http", endpoint},
                MakeEnvelope("s", "m", "i-3"), Until());
        });
        ExpectCode(bad, ErrorCode::InvalidArgument);
    }
    // deadline 已过期/未设置：确定 TimedOut，不发请求。
    {
        auto bad = client.host.Call([&] {
            return bbt_co_await client.client->Call(server.Endpoint(),
                MakeEnvelope("s", "m", "i-4"), CallOptions{});
        });
        ExpectCode(bad, ErrorCode::TimedOut);
    }
    // envelope 校验失败（空 service）在编码前拒绝。
    {
        auto bad = client.host.Call([&] {
            return bbt_co_await client.client->Call(server.Endpoint(),
                MakeEnvelope("", "m", "i-5"), Until());
        });
        ExpectCode(bad, ErrorCode::InvalidArgument);
    }
    server.Finish();
    std::cout << "rpc client input boundary=5 rejected=5\n";
}

void TransportErrors() {
    // 连接拒绝：TransportError（端口先取后关，保证拒绝而非挂起）。
    int probe = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    CHECK(probe >= 0);
    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    CHECK(::bind(probe, reinterpret_cast<sockaddr*>(&local), sizeof(local)) == 0);
    socklen_t size = sizeof(local);
    CHECK(::getsockname(probe, reinterpret_cast<sockaddr*>(&local), &size) == 0);
    const auto port = ntohs(local.sin_port);
    ::close(probe);
    {
        Host host;
        http::ClientOptions options;
        options.base_url = "http://127.0.0.1:" + std::to_string(port);
        auto made = http::Client::Create(host.engine, options);
        CHECK(made);
        auto client = rpc::Client::Create(std::move(made).value());
        CHECK(client);
        auto refused = host.Call([&] {
            return bbt_co_await client.value()->Call(
                {"http", "127.0.0.1:" + std::to_string(port)},
                MakeEnvelope("s", "m", "t-1"), Until(2s));
        });
        ExpectCode(refused, ErrorCode::TransportError);
    }

    // HTTP 500：Unavailable + backend_category=http + backend_code=503。
    {
        HttpServerHost server([](IncomingCallContext, http::IncomingRequest)
                                  -> result<http::Response> {
            return result<http::Response>::ok(http::Response{503, {}, "down"});
        });
        ClientFixture client(server.Base());
        auto bad = client.host.Call([&] {
            return bbt_co_await client.client->Call(server.Endpoint(),
                MakeEnvelope("s", "m", "t-2"), Until());
        });
        CHECK(!bad && bad.error().code == ErrorCode::Unavailable);
        CHECK(bad.error().backend_category == "http" && bad.error().backend_code == 503);
        server.Finish();
    }
    std::cout << "rpc transport refused=1 http503=unavailable(503)\n";
}

void UnknownOutcome() {
    // raw 对端完整吃掉请求字节后直接 FIN：客户端请求已提交但无回复。
    class SilentPeer {
    public:
        SilentPeer() {
            listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
            CHECK(listener >= 0);
            int one = 1;
            CHECK(::setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) == 0);
            sockaddr_in local{};
            local.sin_family = AF_INET;
            local.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            CHECK(::bind(listener, reinterpret_cast<sockaddr*>(&local), sizeof(local)) == 0);
            CHECK(::listen(listener, 8) == 0);
            socklen_t size = sizeof(local);
            CHECK(::getsockname(listener, reinterpret_cast<sockaddr*>(&local), &size) == 0);
            port = ntohs(local.sin_port);
        }
        ~SilentPeer() { ::close(listener); }
        // accept 一次，读完请求头+body 后立即 close（不回复）。
        void ServeSilently() {
            thread = std::thread([this] {
                const int fd = ::accept(listener, nullptr, nullptr);
                if (fd < 0) { return; }
                std::string buffer;
                char chunk[4096];
                while (buffer.find("\r\n\r\n") == std::string::npos) {
                    const auto n = ::recv(fd, chunk, sizeof(chunk), 0);
                    if (n <= 0) { ::close(fd); return; }
                    buffer.append(chunk, static_cast<std::size_t>(n));
                }
                std::size_t content_length = 0;
                const auto head_end = buffer.find("\r\n\r\n");
                std::size_t pos = buffer.find("Content-Length:");
                if (pos != std::string::npos && pos < head_end) {
                    content_length = static_cast<std::size_t>(
                        std::atoll(buffer.c_str() + pos + 15));
                }
                std::size_t body = buffer.size() - head_end - 4;
                while (body < content_length) {
                    const auto n = ::recv(fd, chunk, sizeof(chunk), 0);
                    if (n <= 0) { break; }
                    body += static_cast<std::size_t>(n);
                }
                ::close(fd);  // 完整请求已吃掉，回复丢失
            });
        }
        void Join() { if (thread.joinable()) { thread.join(); } }
        int port{0};
    private:
        int listener{-1};
        std::thread thread;
    };

    SilentPeer peer;
    peer.ServeSilently();
    Host host;
    http::ClientOptions options;
    options.base_url = "http://127.0.0.1:" + std::to_string(peer.port);
    auto made = http::Client::Create(host.engine, options);
    CHECK(made);
    auto client = rpc::Client::Create(std::move(made).value());
    CHECK(client);
    auto unknown = host.Call([&] {
        return bbt_co_await client.value()->Call(
            {"http", "127.0.0.1:" + std::to_string(peer.port)},
            MakeEnvelope("s", "m", "u-1"), Until(3s));
    });
    CHECK(!unknown && unknown.error().code == ErrorCode::OutcomeUnknown);
    CHECK(unknown.error().request_phase.has_value());
    CHECK(IsRequestCommitted(*unknown.error().request_phase));
    peer.Join();
    std::cout << "rpc unknown outcome code=OutcomeUnknown phase=RequestCommitted passthrough=1\n";
}

void Cancelled() {
    // 协程取消透传：入口前请求取消 → Call 确定返回 Cancelled，不改写。
    ServerHost server([](IncomingCallContext, RpcEnvelope request)
                          -> result<RpcEnvelope> {
        return result<RpcEnvelope>::ok(std::move(request));
    });
    ClientFixture client(server.Base());
    auto cancelled = client.host.Call([&] {
        g_bbt_tls_coroutine_co->RequestCancel();
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("s", "m", "c-1"), Until());
    });
    ExpectCode(cancelled, ErrorCode::Cancelled);
    server.Finish();
    std::cout << "rpc cancel passthrough code=Cancelled\n";
}

void MalformedReply() {
    // canned 对端四连：错 Content-Type / 垃圾 body / 关联不匹配 / 错误信封。
    HttpServerHost server([](IncomingCallContext, http::IncomingRequest)
                              -> result<http::Response> {
        // 每个 body 都由同一 raw 客户端序列化，这里按请求顺序切换剧本。
        static std::atomic<unsigned> seq{0};
        const auto n = seq.fetch_add(1);
        http::Response reply;
        reply.status_code = 200;
        if (n == 0) {
            // Content-Type 不对（body 合法与否都不该被读）。
            reply.headers = {{rpc::kRpcWireContentTypeKey, "text/plain"}};
            reply.body = ResponseBody(rpc::ToWireEnvelope(
                MakeEnvelope("s", "m", "m-1"), 1).value());
        } else if (n == 1) {
            reply.headers = {{rpc::kRpcWireContentTypeKey, rpc::kRpcWireContentType}};
            reply.body = "this-is-not-protobuf";
        } else if (n == 2) {
            reply.headers = {{rpc::kRpcWireContentTypeKey, rpc::kRpcWireContentType}};
            // 合法信封但 request_id 被换掉。
            reply.body = ResponseBody(rpc::ToWireEnvelope(
                MakeEnvelope("s", "m", "spoofed"), 1).value());
        } else {
            reply.headers = {{rpc::kRpcWireContentTypeKey, rpc::kRpcWireContentType}};
            // 合法错误信封：NotFound 透传（不吞成传输错误）。
            auto wire_env = rpc::ToWireEnvelope(
                MakeEnvelope("s", "m", "handled-err"), 1);
            CHECK(wire_env);
            wire_env.value().success = false;
            wire_env.value().error = MakeError(ErrorCode::NotFound, "no such service");
            reply.body = ResponseBody(wire_env.value());
        }
        return result<http::Response>::ok(std::move(reply));
    });
    ClientFixture client(server.Base());
    const RpcAddress endpoint = server.Endpoint();

    {   // n==0：Content-Type 错（服务端剧本 body 是合法信封但类型头不对）。
        auto bad = client.host.Call([&] {
            return bbt_co_await client.client->Call(endpoint,
                MakeEnvelope("s", "m", "m-1"), Until());
        });
        ExpectCode(bad, ErrorCode::InvalidArgument);
    }
    {   // n==1：垃圾 body。
        auto bad = client.host.Call([&] {
            return bbt_co_await client.client->Call(endpoint,
                MakeEnvelope("s", "m", "m-2"), Until());
        });
        ExpectCode(bad, ErrorCode::ProtocolError);
    }
    {   // n==2：关联不匹配。
        auto bad = client.host.Call([&] {
            return bbt_co_await client.client->Call(endpoint,
                MakeEnvelope("s", "m", "m-3"), Until());
        });
        ExpectCode(bad, ErrorCode::ProtocolError);
        CHECK(bad.error().message == "rpc reply envelope association mismatch");
    }
    {   // n==3：handler 错误信封按原错误码交付。
        auto bad = client.host.Call([&] {
            return bbt_co_await client.client->Call(endpoint,
                MakeEnvelope("s", "m", "handled-err"), Until());
        });
        CHECK(!bad && bad.error().code == ErrorCode::NotFound);
        CHECK(bad.error().message == "no such service");
    }
    server.Finish();
    std::cout << "rpc malformed reply ctype=1 garbage=1 spoof=1 error-passthrough=1\n";
}

void ServerMalformed() {
    // raw 对端直接打服务端：错误形状都得到 200 + 错误信封（不 crash、不断言成功）。
    ServerHost server(EchoHandler());
    const RpcWireEnvelope valid = [] {
        auto env = rpc::ToWireEnvelope(MakeEnvelope("echo", "Say", "srv-1", Pack("v", 9)), 1000);
        CHECK(env);
        return env.value();
    }();
    const auto valid_body = RequestBody(valid);

    {   // GET /rpc → InvalidArgument 信封。
        RawPeer peer(server.address);
        peer.Send("GET /rpc HTTP/1.1\r\nHost: h\r\nContent-Length: " +
                  std::to_string(valid_body.size()) + "\r\n\r\n" + valid_body);
        const auto reply = peer.ReadResponse();
        const auto env = ParseReply(reply);
        CHECK(!env.success && env.error.code == ErrorCode::InvalidArgument);
        CHECK(env.request_id == "unparsed");
    }
    {   // POST 错 path → InvalidArgument 信封。
        RawPeer peer(server.address);
        peer.Send("POST /wrong HTTP/1.1\r\nHost: h\r\nContent-Type: " +
                  std::string(rpc::kRpcWireContentType) +
                  "\r\nContent-Length: " + std::to_string(valid_body.size()) +
                  "\r\n\r\n" + valid_body);
        const auto env = ParseReply(peer.ReadResponse());
        CHECK(!env.success && env.error.code == ErrorCode::InvalidArgument);
    }
    {   // 合法形状 + 空 body → ProtocolError 信封。
        RawPeer peer(server.address);
        peer.Send("POST /rpc HTTP/1.1\r\nHost: h\r\nContent-Type: " +
                  std::string(rpc::kRpcWireContentType) + "\r\nContent-Length: 0\r\n\r\n");
        const auto env = ParseReply(peer.ReadResponse());
        CHECK(!env.success && env.error.code == ErrorCode::ProtocolError);
    }
    {   // 合法形状 + 垃圾 protobuf body → ProtocolError 信封（id 占位 unparsed）。
        RawPeer peer(server.address);
        const std::string garbage = "not-an-envelope";
        peer.Send("POST /rpc HTTP/1.1\r\nHost: h\r\nContent-Type: " +
                  std::string(rpc::kRpcWireContentType) +
                  "\r\nContent-Length: " + std::to_string(garbage.size()) +
                  "\r\n\r\n" + garbage);
        const auto env = ParseReply(peer.ReadResponse());
        CHECK(!env.success && env.error.code == ErrorCode::ProtocolError);
        CHECK(env.request_id == "unparsed" && env.service == "unknown");
    }
    {   // handler 改关联字段 → ProtocolError 信封（关联字段保留请求值）。
        ServerHost spoof([](IncomingCallContext, RpcEnvelope request)
                             -> result<RpcEnvelope> {
            request.request_id = "hijacked";
            return result<RpcEnvelope>::ok(std::move(request));
        });
        RawPeer peer(spoof.address);
        const auto body = RequestBody(valid);
        peer.Send("POST /rpc HTTP/1.1\r\nHost: h\r\nContent-Type: " +
                  std::string(rpc::kRpcWireContentType) +
                  "\r\nContent-Length: " + std::to_string(body.size()) +
                  "\r\n\r\n" + body);
        const auto env = ParseReply(peer.ReadResponse());
        CHECK(!env.success && env.error.code == ErrorCode::ProtocolError);
        CHECK(env.error.message == "rpc handler changed association fields");
        CHECK(env.request_id == "srv-1");
        spoof.Finish();
    }
    server.Finish();
    std::cout << "rpc server malformed method/path/body/garbage/spoof -> error envelopes\n";
}

void CancelPostCommit() {
    // 已确认 request committed（服务端 handler 已进入）后协程取消：Call 立即
    // 以唯一终态 Cancelled + RequestCommitted 收场；之后放行 handler 的晚到
    // 回复不得再改变终态；连接随后真实收口。
    auto gate = std::make_shared<Gate>();
    ServerHost server([&](IncomingCallContext, RpcEnvelope request)
                          -> result<RpcEnvelope> {
        gate->Pause();
        return result<RpcEnvelope>::ok(std::move(request));
    });
    ClientFixture client(server.Base());
    std::promise<bbt::coroutine::detail::Coroutine*> co_promise;
    auto co_future = co_promise.get_future();
    auto inflight = client.host.Submit([&] {
        co_promise.set_value(g_bbt_tls_coroutine_co);
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("s", "m", "pc-1"), Until(5s));
    });
    CHECK(WaitUntil([&] { return gate->entered.load(); }));  // committed 已证明
    auto* current = Get(co_future);
    CHECK(current != nullptr);
    current->RequestCancel();
    const auto outcome = Get(inflight);
    CHECK(!outcome);
    CHECK(outcome.error().code == ErrorCode::Cancelled);
    CHECK(outcome.error().request_phase.has_value() &&
          IsRequestCommitted(*outcome.error().request_phase));
    // 晚到回复：终态已发布且唯一，放行后不得翻转（future 值保持不变）。
    gate->Release();
    CHECK(outcome.error().code == ErrorCode::Cancelled);
    server.Finish();
    std::cout << "rpc cancel post-commit code=Cancelled "
                 "phase=RequestCommitted late-reply=dropped\n";
}

} // namespace

int RunCase(const std::string& mode) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 256 * 1024;
    g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
    g_scheduler->Start();
    if (mode == "client_input") { ClientInput(); }
    else if (mode == "transport_errors") { TransportErrors(); }
    else if (mode == "unknown_outcome") { UnknownOutcome(); }
    else if (mode == "cancelled") { Cancelled(); }
    else if (mode == "cancel_post_commit") { CancelPostCommit(); }
    else if (mode == "malformed_reply") { MalformedReply(); }
    else if (mode == "server_malformed") { ServerMalformed(); }
    else { CHECK(false); }
    std::cout << "rpc faults " << mode << " PASS\n";
    return 0;
}

} // namespace rpc_net_test

#include "../net/ProcessRuntimeTestMain.hpp"

int main(int argc, char** argv) {
    return bbt_net_test::RunTestProcessExit([&] {
        rpc_net_test::Watchdog watchdog;
        try {
            CHECK(argc == 2);
            return rpc_net_test::RunCase(argv[1]);
        } catch (const std::exception& error) {
            std::cerr << "FAIL " << error.what() << '\n';
            return 1;
        }
    });
}
