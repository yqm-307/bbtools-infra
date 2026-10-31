// rpc_net TCP framing（architecture §6）：u32be 长度前缀帧、一连接一请求一
// 回复的真实 loopback 契约。覆盖：任意分片、length=0/超大、截断、commit
// 前后断连、协程取消、Close、OutcomeUnknown、超额 Overloaded 与 server
// 生命周期。raw POSIX 对端注入字节，不 mock adapter/协议。

#include "TestSupport.hpp"

namespace rpc_net_test {
namespace {

using rpc::RpcWireEnvelope;

void TcpRoundtrip() {
    // 真实双端：tcp profile endpoint 走 u32be 帧；成功回程 + handler 错误
    // 信封透传（不吞成传输错误）+ 关联字段五元校验。
    std::atomic<unsigned> handled{0};
    TcpServerHost server(EchoHandler(&handled));
    TcpClientFixture client;

    auto reply = client.host.Call([&] {
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("echo", "Say", "tcp-1", Pack("hello", 42)), Until());
    });
    CHECK(reply && reply.value().request_id == "tcp-1" &&
          reply.value().service == "echo" && reply.value().method == "Say");
    const auto [text, number] = Unpack(reply.value().payload);
    CHECK(text == "hello" && number == 42);
    CHECK(handled.load() == 1);
    server.Finish();  // 收口第一台（任一时刻并发 Engine 数与既有用例一致）

    // handler 错误信封按原错误码交付（不吞成传输错误）。
    TcpServerHost failing([](IncomingCallContext, RpcEnvelope)
                              -> result<RpcEnvelope> {
        return result<RpcEnvelope>::err(
            MakeError(ErrorCode::NotFound, "no such method"));
    });
    TcpClientFixture failing_client;
    auto failure = failing_client.host.Call([&] {
        return bbt_co_await failing_client.client->Call(failing.Endpoint(),
            MakeEnvelope("s", "m", "tcp-3"), Until());
    });
    CHECK(!failure && failure.error().code == ErrorCode::NotFound);
    CHECK(failure.error().message == "no such method");

    failing.Finish();
    std::cout << "rpc tcp roundtrip success + error-envelope passthrough=1\n";
}

void TcpFragmentation() {
    // raw 对端向 server 注入任意分片/合并的帧字节：1 字节片、奇数片、
    // prefix 与 body 跨界合并、body 内部分片——server 必须按帧界重组。
    std::atomic<unsigned> handled{0};
    TcpServerHost server(EchoHandler(&handled));

    const RpcWireEnvelope valid = [] {
        auto env = rpc::ToWireEnvelope(
            MakeEnvelope("echo", "Say", "frag-1", Pack("v", 9)), 3000);
        CHECK(env);
        return env.value();
    }();
    const auto frame = TcpRequestFrame(valid);

    // 1 字节分片：最极端的字节流形态。
    {
        RawTcpPeer peer(server.address);
        peer.SendFragmented(frame, 1);
        const auto reply = peer.ReadReply();
        CHECK(reply.success && reply.request_id == "frag-1");
        CHECK(peer.Eof());  // 一连接一请求一回复后 server 关闭
    }
    // 奇数片 + prefix/body 跨界（片段不切在协议边界上）。
    {
        RawTcpPeer peer(server.address);
        peer.SendFragmented(frame, 7);
        const auto reply = peer.ReadReply();
        CHECK(reply.success && reply.request_id == "frag-1");
    }
    // 前缀一次发完、body 分两片。
    {
        RawTcpPeer peer(server.address);
        peer.Send(frame.substr(0, 4));
        const auto body = frame.substr(4);
        peer.Send(body.substr(0, body.size() / 2));
        peer.Send(body.substr(body.size() / 2));
        const auto reply = peer.ReadReply();
        CHECK(reply.success && reply.request_id == "frag-1");
    }
    CHECK(handled.load() == 3);
    server.Finish();
    std::cout << "rpc tcp fragmentation 1byte/odd-split/cross-boundary handled=3\n";
}

void TcpBadPrefix() {
    // 非法前缀：length=0 与 length>max_frame_bytes 都不得分配/调 handler、
    // 不回复帧，连接被关闭。
    std::atomic<unsigned> handled{0};
    rpc::TcpRpcServerOptions options;
    options.max_frame_bytes = 64 * 1024;  // 小上限便于触发
    TcpServerHost server(EchoHandler(&handled), options);

    {   // length=0：非法帧。
        RawTcpPeer peer(server.address);
        peer.Send(TcpPrefix(0));
        CHECK(WaitUntil([&] { return peer.Eof(); }));
    }
    {   // 超大长度（>max_frame_bytes）：上限在分配/复制前强制。
        RawTcpPeer peer(server.address);
        peer.Send(TcpPrefix(128 * 1024));  // > 64 KiB 上限
        CHECK(WaitUntil([&] { return peer.Eof(); }));
        CHECK(handled.load() == 0);
    }
    {   // 对照：合法帧仍成功（坏前缀未污染 listener）。
        RawTcpPeer peer(server.address);
        auto env = rpc::ToWireEnvelope(
            MakeEnvelope("echo", "Say", "ok-1", Pack("v", 1)), 3000);
        CHECK(env);
        peer.Send(TcpRequestFrame(env.value()));
        const auto reply = peer.ReadReply();
        CHECK(reply.success && reply.request_id == "ok-1");
        CHECK(handled.load() == 1);
    }
    server.Finish();
    std::cout << "rpc tcp bad prefix zero/oversize rejected, valid-frame control=1\n";
}

void TcpTruncated() {
    // 截断：合法前缀 + 部分 body 后 FIN → server 静默放弃（不回复、
    // handler 未进入）。
    std::atomic<unsigned> handled{0};
    TcpServerHost server(EchoHandler(&handled));
    auto env = rpc::ToWireEnvelope(
        MakeEnvelope("echo", "Say", "trunc-1", Pack("v", 5)), 3000);
    CHECK(env);
    const auto frame = TcpRequestFrame(env.value());

    RawTcpPeer peer(server.address);
    peer.Send(frame.substr(0, frame.size() - 3));  // body 截断 3 字节
    peer.HalfClose();
    CHECK(WaitUntil([&] { return peer.Eof(); }));
    CHECK(handled.load() == 0);
    server.Finish();
    std::cout << "rpc tcp truncated body dropped handler=0\n";
}

void TcpDropPreCommit() {
    // commit 前断连：raw 对端吃掉部分请求前缀后 FIN —— 但这测试的是
    // *outbound* 在写前/写中失去对端。用端口先取后关的确定拒绝 +
    // 写路径失败（对端 accept 后立即 FIN，不让请求帧完整写出）。
    {   // 连接拒绝：TransportError，未 committed（确定失败）。
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
        TcpClientFixture client;
        auto refused = client.host.Call([&] {
            return bbt_co_await client.client->Call(
                {"tcp", "127.0.0.1:" + std::to_string(port)},
                MakeEnvelope("s", "m", "dp-1"), Until(2s));
        });
        ExpectCode(refused, ErrorCode::TransportError);
        // 连接拒绝发生在任何字节写出之前：request_phase 要么不携带
        // （提交前拒绝），要么为未提交阶段；绝不能标成已提交。
        CHECK(!refused.error().request_phase.has_value() ||
              !IsRequestCommitted(*refused.error().request_phase));
    }
    {   // 对端 accept 后立即 RST/FIN（写前断连）：未 committed → 确定失败
        // （TransportError/OutcomeUnknown 均可能——loopback 时序竞争；
        // 只断言非成功终态且若 committed 事实在则为 OutcomeUnknown）。
        int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
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
        const auto port = ntohs(local.sin_port);
        std::thread dropper([&] {
            const int conn = ::accept(listener, nullptr, nullptr);
            if (conn >= 0) { ::close(conn); }  // 立即断连
            ::close(listener);
        });
        TcpClientFixture client;
        auto outcome = client.host.Call([&] {
            return bbt_co_await client.client->Call(
                {"tcp", "127.0.0.1:" + std::to_string(port)},
                MakeEnvelope("s", "m", "dp-2"), Until(2s));
        });
        dropper.join();
        CHECK(!outcome);
        if (outcome.error().request_phase.has_value() &&
            IsRequestCommitted(*outcome.error().request_phase)) {
            CHECK(outcome.error().code == ErrorCode::OutcomeUnknown);
        }
    }
    std::cout << "rpc tcp drop pre-commit refused=TransportError accepted-then-drop=non-ok\n";
}

void TcpDropPostCommit() {
    // commit 后断连：raw 对端完整吃掉请求帧后 FIN（无回复）→
    // OutcomeUnknown + request_phase=RequestCommitted（请求可能已生效）。
    std::atomic<unsigned> got{0};
    int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
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
    const auto port = ntohs(local.sin_port);
    std::thread silent([&] {
        const int conn = ::accept(listener, nullptr, nullptr);
        if (conn < 0) { ::close(listener); return; }
        // 读满一条完整请求帧（4 前缀 + body）后立即 close。
        std::string prefix;
        char chunk[4096];
        while (prefix.size() < 4) {
            const auto n = ::recv(conn, chunk, 4 - prefix.size(), 0);
            if (n <= 0) { ::close(conn); ::close(listener); return; }
            prefix.append(chunk, static_cast<std::size_t>(n));
        }
        const auto length = TcpReadPrefix(prefix.data());
        std::size_t body = 0;
        while (body < length) {
            const auto n = ::recv(conn, chunk, sizeof(chunk), 0);
            if (n <= 0) { break; }
            body += static_cast<std::size_t>(n);
        }
        got.store(body == length ? 1 : 0);
        ::close(conn);  // 请求已提交，回复丢失
        ::close(listener);
    });
    TcpClientFixture client;
    auto unknown = client.host.Call([&] {
        return bbt_co_await client.client->Call(
            {"tcp", "127.0.0.1:" + std::to_string(port)},
            MakeEnvelope("s", "m", "pc-1"), Until(3s));
    });
    silent.join();
    CHECK(got.load() == 1);  // 服务端证明请求帧已完整到达
    CHECK(!unknown && unknown.error().code == ErrorCode::OutcomeUnknown);
    CHECK(unknown.error().request_phase.has_value());
    CHECK(IsRequestCommitted(*unknown.error().request_phase));
    std::cout << "rpc tcp drop post-commit OutcomeUnknown phase=RequestCommitted\n";
}

void TcpCancel() {
    // 协程取消透传：进入 Call 前 RequestCancel → 确定 Cancelled（不改写）。
    TcpServerHost server(EchoHandler());
    TcpClientFixture client;
    auto cancelled = client.host.Call([&] {
        g_bbt_tls_coroutine_co->RequestCancel();
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("s", "m", "tc-1"), Until());
    });
    ExpectCode(cancelled, ErrorCode::Cancelled);
    server.Finish();
    std::cout << "rpc tcp cancel passthrough code=Cancelled\n";
}

void TcpClose() {
    // Close：幂等、IsClosed、后续 Call 拒绝（Closed）；对在途连接
    // RequestClose（committed 的在途调用按 OutcomeUnknown/非成功终态收场）。
    std::atomic<unsigned> handled{0};
    auto gate = std::make_shared<Gate>();
    TcpServerHost server([&](IncomingCallContext, RpcEnvelope request)
                             -> result<RpcEnvelope> {
        handled.fetch_add(1);
        gate->Pause();
        return result<RpcEnvelope>::ok(std::move(request));
    });
    TcpClientFixture client;

    auto inflight = client.host.Submit([&] {
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("s", "m", "cl-0"), Until(5s));
    });
    CHECK(WaitUntil([&] { return gate->entered.load(); }));  // 请求已提交
    client.host.Call([&] { client.client->Close(); return true; });
    CHECK(client.host.Call([&] { return client.client->IsClosed(); }));
    CHECK(client.host.Call([&] { return client.transport->IsClosed(); }));
    // 重复 Close 幂等。
    client.host.Call([&] { client.client->Close(); return true; });
    CHECK(client.host.Call([&] { return client.client->IsClosed(); }));
    // 已提交在途：Close 后连接被封 → 非成功终态（OutcomeUnknown/Cancelled/
    // TransportError 均可，唯一要求是不得假成功或悬挂）。
    gate->Release();
    const auto outcome = Get(inflight);
    CHECK(!outcome);
    // 新调用被拒。
    auto rejected = client.host.Call([&] {
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("s", "m", "cl-1"), Until());
    });
    ExpectCode(rejected, ErrorCode::Closed);
    server.Finish();
    std::cout << "rpc tcp close idempotent inflight=non-ok rejected=Closed\n";
}

void TcpOverloaded() {
    // max_inflight=1：第一次调用占住名额（handler 挂 gate），第二次立即
    // Overloaded，不排队、不重试。
    auto gate = std::make_shared<Gate>();
    TcpServerHost server([&](IncomingCallContext, RpcEnvelope request)
                             -> result<RpcEnvelope> {
        gate->Pause();
        return result<RpcEnvelope>::ok(std::move(request));
    });
    rpc::TcpRpcOptions options;
    options.max_inflight = 1;
    TcpClientFixture client(options);

    auto inflight = client.host.Submit([&] {
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("s", "m", "ol-0"), Until(5s));
    });
    CHECK(WaitUntil([&] { return gate->entered.load(); }));
    // 名额已占：第二次调用必须立即 Overloaded（不等到第一次释放）。
    auto overloaded = client.host.Call([&] {
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("s", "m", "ol-1"), Until(2s));
    });
    ExpectCode(overloaded, ErrorCode::Overloaded);
    gate->Release();
    const auto first = Get(inflight);
    CHECK(first && first.value().request_id == "ol-0");
    server.Finish();
    std::cout << "rpc tcp overloaded max_inflight=1 immediate-Overloaded\n";
}

void TcpCloseDuringDial() {
    // R2-B1：拨号途中 Close 必须对未提交调用封口——调用落 Closed，且对端
    // 不得收到任何请求帧（request_seen==0）。固定时序：Engine connect 启动
    // 延迟 400ms + PendingConnectCount==1（第一次调用真实停在 CoDial），
    // 此刻 Client::Close；旧调用在 CoDial 返回后 Publish 阶段发现封口、
    // RequestClose 连接并结算 Closed。对端 accept 后只静听，若修复回退
    // （Publish 后不查 closed），请求帧会真正写出、request_seen=1。
    std::atomic<unsigned> request_seen{0};
    int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
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
    const auto port = ntohs(local.sin_port);
    const std::string endpoint = "127.0.0.1:" + std::to_string(port);
    std::thread peer([&] {
        const int fd = ::accept(listener, nullptr, nullptr);
        ::close(listener);
        if (fd < 0) { return; }
        // 静听请求字节：任何到达的字节都证明未提交调用越过了 Close 封口。
        // 2s 观察窗覆盖拨号完成（400ms）+ 写路径，随后主动断开。
        timeval window{2, 0};
        (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &window, sizeof(window));
        char bytes[512];
        while (::recv(fd, bytes, sizeof(bytes), 0) > 0) {
            request_seen.fetch_add(1);
        }
        ::close(fd);
    });

    TcpClientFixture client;
    net::EngineTestAccess::SetConnectStartDelay(
        *client.host.engine, std::chrono::milliseconds{400});
    auto inflight = client.host.Submit([&] {
        return bbt_co_await client.client->Call({"tcp", endpoint},
            MakeEnvelope("s", "m", "dd-0"), Until(3s));
    });
    // 阶段证据：第一次调用已提交拨号、尚在 CoDial（未交付连接）。
    CHECK(WaitUntil([&] {
        return net::EngineTestAccess::PendingConnectCount(
            *client.host.engine) == 1;
    }));
    client.host.Call([&] { client.client->Close(); return true; });
    CHECK(client.host.Call([&] { return client.transport->IsClosed(); }));
    // 新调用立即 Closed（不进入 CoDial）。
    const auto start = std::chrono::steady_clock::now();
    auto rejected = client.host.Call([&] {
        return bbt_co_await client.client->Call({"tcp", endpoint},
            MakeEnvelope("s", "m", "dd-1"), Until(1s));
    });
    const auto elapsed = std::chrono::steady_clock::now() - start;
    ExpectCode(rejected, ErrorCode::Closed);
    CHECK(elapsed < 1s);
    // 拨号中的旧调用：Close 先于 Publish → Closed（未提交，非 OutcomeUnknown）。
    const auto first = Get(inflight);
    CHECK(!first && first.error().code == ErrorCode::Closed);
    net::EngineTestAccess::SetConnectStartDelay(
        *client.host.engine, std::chrono::milliseconds{0});
    peer.join();
    CHECK(request_seen.load() == 0);  // 对端未收到任何请求帧字节
    std::cout << "rpc tcp close-during-dial old=Closed new=Closed"
                 " request_seen=0\n";
}

void TcpOverloadedNoDial() {
    // B-3：max_inflight 在 CoDial 前原子预占——超额调用立即 Overloaded 且
    // 不发起真实 connect。Engine 侧 connect 启动延迟 10s（对端是真实
    // stalled listener：端口可拨但永不交付），第一次调用停在 CoDial 等待
    // 占住名额；第二次调用若越过准入会再进一次 CoDial——PendingConnectCount
    // 是「已提交、未交付」的拨号计数，=1 即证明超额调用未进入 CoDial。
    int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
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
    const auto port = ntohs(local.sin_port);

    rpc::TcpRpcOptions options;
    options.max_inflight = 1;
    TcpClientFixture client(options);
    net::EngineTestAccess::SetConnectStartDelay(
        *client.host.engine, std::chrono::milliseconds{10000});
    auto inflight = client.host.Submit([&] {
        return bbt_co_await client.client->Call(
            {"tcp", "127.0.0.1:" + std::to_string(port)},
            MakeEnvelope("s", "m", "od-0"), Until(5s));
    });
    // 第一次调用真实停在 CoDial 等待（名额已占，拨号已提交未交付）。
    CHECK(WaitUntil([&] {
        return net::EngineTestAccess::PendingConnectCount(
            *client.host.engine) == 1;
    }));
    const auto start = std::chrono::steady_clock::now();
    auto overloaded = client.host.Call([&] {
        return bbt_co_await client.client->Call(
            {"tcp", "127.0.0.1:" + std::to_string(port)},
            MakeEnvelope("s", "m", "od-1"), Until(2s));
    });
    const auto elapsed = std::chrono::steady_clock::now() - start;
    ExpectCode(overloaded, ErrorCode::Overloaded);
    // 阶段证据：超额调用未进入 CoDial（提交未交付的拨号仍是 1 次）且立即
    // 返回（远小于 connect 启动延迟 10s）。
    CHECK(net::EngineTestAccess::PendingConnectCount(
        *client.host.engine) == 1);
    CHECK(elapsed < 1s);
    net::EngineTestAccess::SetConnectStartDelay(
        *client.host.engine, std::chrono::milliseconds{0});
    const auto first = Get(inflight);   // stalled listener：非成功终态
    CHECK(!first);
    client.client->Close();
    ::close(listener);
    std::cout << "rpc tcp overloaded-no-dial pending=1 immediate=1"
                 " code=Overloaded\n";
}

void TcpServerLifecycle() {
    // StopAccepting：listener 物理停（新拨号被拒），已授权 handler 放行；
    // Drain 在 handler+回复收口后完成；Close→IsClosed→WaitStopped。
    std::atomic<unsigned> handled{0};
    TcpServerHost server(EchoHandler(&handled));
    TcpClientFixture client;
    auto ok = client.host.Call([&] {
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("s", "m", "sl-0"), Until());
    });
    CHECK(ok && handled.load() == 1);

    server.server->StopAccepting();
    CHECK(WaitUntil([&] { return server.engine->AcceptingStopped(); }));
    CHECK(!server.server->IsClosed());
    // 新拨号被拒（listener 已关）→ TransportError。
    auto refused = client.host.Call([&] {
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("s", "m", "sl-1"), Until(2s));
    });
    ExpectCode(refused, ErrorCode::TransportError);

    server.server->Close();
    CHECK(server.server->IsClosed());
    CHECK(server.server->Drain(std::chrono::steady_clock::now() + 3s));
    CHECK(server.server->WaitStopped(std::chrono::steady_clock::now() + 3s));
    CHECK(server.engine->CountConnections() == 0);
    server.Finish();
    std::cout << "rpc tcp server lifecycle stop/close/drain/waitstopped=1\n";
}

void TcpLateTrailing() {
    // §6.3：单响应帧之后到达的尾部字节是协议违规。raw 对端吃掉完整请求帧，
    // 写回合法响应帧后再追加 trailing 字节——读满 body 的 prefetch 抓不到
    // 晚到字节，必须在剩余期限内的尾随 Read 上判 ProtocolError。
    std::atomic<unsigned> accepted{0};
    int listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
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
    const auto port = ntohs(local.sin_port);
    const RpcWireEnvelope reply_env = [] {
        auto env = rpc::ToWireEnvelope(
            MakeEnvelope("s", "m", "lt-1"), 1000);
        CHECK(env);
        return env.value();
    }();
    const auto reply_frame = TcpReplyFrame(reply_env);
    std::thread peer([&] {
        const int fd = ::accept(listener, nullptr, nullptr);
        if (fd < 0) { ::close(listener); return; }
        // 吃满一条完整请求帧（4 前缀 + body）。
        std::string prefix;
        char chunk[4096];
        while (prefix.size() < 4) {
            const auto n = ::recv(fd, chunk, 4 - prefix.size(), 0);
            if (n <= 0) { ::close(fd); ::close(listener); return; }
            prefix.append(chunk, static_cast<std::size_t>(n));
        }
        const auto length = TcpReadPrefix(prefix.data());
        std::size_t body = 0;
        while (body < length) {
            const auto n = ::recv(fd, chunk, sizeof(chunk), 0);
            if (n <= 0) { ::close(fd); ::close(listener); return; }
            body += static_cast<std::size_t>(n);
        }
        accepted.store(1);
        // 合法响应帧 + 紧随其后的尾部字节（协议违规注入）。
        std::size_t sent = 0;
        while (sent < reply_frame.size()) {
            const auto n = ::send(fd, reply_frame.data() + sent,
                                  reply_frame.size() - sent, MSG_NOSIGNAL);
            if (n <= 0) { ::close(fd); ::close(listener); return; }
            sent += static_cast<std::size_t>(n);
        }
        const char trailing[] = "\xde\xad\xbe\xef";
        (void)::send(fd, trailing, sizeof(trailing) - 1, MSG_NOSIGNAL);
        // 保持连接开着让 client 读到 trailing（不关 fd，等 client 判违规后
        // 自己 RequestClose；peer 线程随后被 join 收尾）。
        ::close(listener);
    });
    TcpClientFixture client;
    auto outcome = client.host.Call([&] {
        return bbt_co_await client.client->Call(
            {"tcp", "127.0.0.1:" + std::to_string(port)},
            MakeEnvelope("s", "m", "lt-1"), Until(3s));
    });
    peer.join();
    CHECK(accepted.load() == 1);
    CHECK(!outcome && outcome.error().code == ErrorCode::ProtocolError);
    CHECK(outcome.error().request_phase.has_value() &&
          IsRequestCommitted(*outcome.error().request_phase));
    std::cout << "rpc tcp late trailing code=ProtocolError phase=RequestCommitted\n";
}

} // namespace

int RunCase(const std::string& mode) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 256 * 1024;
    g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
    g_scheduler->Start();
    if (mode == "tcp_roundtrip") { TcpRoundtrip(); }
    else if (mode == "tcp_fragmentation") { TcpFragmentation(); }
    else if (mode == "tcp_bad_prefix") { TcpBadPrefix(); }
    else if (mode == "tcp_truncated") { TcpTruncated(); }
    else if (mode == "tcp_drop_pre_commit") { TcpDropPreCommit(); }
    else if (mode == "tcp_drop_post_commit") { TcpDropPostCommit(); }
    else if (mode == "tcp_cancel") { TcpCancel(); }
    else if (mode == "tcp_close") { TcpClose(); }
    else if (mode == "tcp_overloaded") { TcpOverloaded(); }
    else if (mode == "tcp_close_during_dial") { TcpCloseDuringDial(); }
    else if (mode == "tcp_overloaded_no_dial") { TcpOverloadedNoDial(); }
    else if (mode == "tcp_server_lifecycle") { TcpServerLifecycle(); }
    else if (mode == "tcp_late_trailing") { TcpLateTrailing(); }
    else { CHECK(false); }
    std::cout << "rpc net tcp " << mode << " PASS\n";
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
