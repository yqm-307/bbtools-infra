// rpc_net 生命周期：Close/IsClosed 拒绝语义、StopAccepting→Drain→Close→
// WaitStopped 收口序列、在途 handler 与停止授权语义（真实双端 + 受控屏障）。

#include "TestSupport.hpp"

namespace rpc_net_test {
namespace {

void ClientClose() {
    ServerHost server(EchoHandler());
    ClientFixture client(server.Base());
    // 正常一次调用后 Close：幂等、IsClosed、后续 Call 拒绝（Closed）。
    auto ok = client.host.Call([&] {
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("s", "m", "lc-0"), Until());
    });
    CHECK(ok);
    client.host.Call([&] { client.client->Close(); return true; });
    CHECK(client.host.Call([&] { return client.client->IsClosed(); }));
    // 底层 http owner 同步封口（共享同一份关闭状态，不出现第二份）。
    CHECK(client.host.Call([&] { return client.http_client->IsClosed(); }));
    auto rejected = client.host.Call([&] {
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("s", "m", "lc-1"), Until());
    });
    ExpectCode(rejected, ErrorCode::Closed);
    // 重复 Close 幂等（不崩溃、不重开）。
    client.host.Call([&] { client.client->Close(); return true; });
    CHECK(client.host.Call([&] { return client.client->IsClosed(); }));
    server.Finish();
    std::cout << "rpc client close idempotent=1 rejected=Closed shared-owner=1\n";
}

void ServerLifecycle() {
    std::atomic<unsigned> handled{0};
    ServerHost server(EchoHandler(&handled));
    ClientFixture client(server.Base());
    auto ok = client.host.Call([&] {
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("s", "m", "sl-0"), Until());
    });
    CHECK(ok && handled.load() == 1);

    // StopAccepting：listener 物理停（新拨号被拒），既有连接收尾。
    server.server->StopAccepting();
    CHECK(WaitUntil([&] { return server.engine->AcceptingStopped(); }));
    // 未 Close 前 IsClosed 仍为 false（IsClosed 只跟 RPC Server 的 Close 走）。
    CHECK(!client.host.Call([&] { return server.server->IsClosed(); }));
    auto refused = client.host.Call([&] {
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("s", "m", "sl-1"), Until(2s));
    });
    ExpectCode(refused, ErrorCode::TransportError);

    // Close → IsClosed；Drain/WaitStopped 依序收口。
    server.server->Close();
    CHECK(client.host.Call([&] { return server.server->IsClosed(); }));
    CHECK(server.server->Drain(std::chrono::steady_clock::now() + 3s));
    CHECK(server.server->WaitStopped(std::chrono::steady_clock::now() + 3s));
    CHECK(server.engine->CountConnections() == 0);
    server.Finish();
    std::cout << "rpc server lifecycle stop=listener-stopped close=is_closed receipts=1\n";
}

void InflightAuthorization() {
    // StopAccepting 不取消已授权 handler：gate 上挂起 → 停接纳 → 放行后
    // 回复仍真实送达（客户端拿到成功回包），随后 Drain 才能完成。
    auto gate = std::make_shared<Gate>();
    ServerHost server([&](IncomingCallContext, RpcEnvelope request)
                          -> result<RpcEnvelope> {
        gate->Pause();
        return result<RpcEnvelope>::ok(std::move(request));
    });
    ClientFixture client(server.Base());
    auto inflight = client.host.Submit([&] {
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("s", "m", "ia-0"), Until(5s));
    });
    CHECK(WaitUntil([&] { return gate->entered.load(); }));
    server.server->StopAccepting();
    CHECK(WaitUntil([&] { return server.engine->AcceptingStopped(); }));
    gate->Release();
    const auto reply = Get(inflight);
    CHECK(reply && reply.value().request_id == "ia-0");
    // handler 与回复都收口后 Drain 才通过。
    CHECK(server.server->Drain(std::chrono::steady_clock::now() + 3s));
    server.server->Close();
    CHECK(server.server->WaitStopped(std::chrono::steady_clock::now() + 3s));
    server.Finish();
    std::cout << "rpc inflight stop-accept kept handler=1 drain-after-reply=1\n";
}

void CloseDropsInflightReply() {
    // Close 强制封口：在途请求不再交付成功回复（回复明确放弃，客户端按
    // 非成功终态收场），不悬挂、不假装送达。
    auto gate = std::make_shared<Gate>();
    ServerHost server([&](IncomingCallContext, RpcEnvelope request)
                          -> result<RpcEnvelope> {
        gate->Pause();
        return result<RpcEnvelope>::ok(std::move(request));
    });
    ClientFixture client(server.Base());
    auto inflight = client.host.Submit([&] {
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("s", "m", "cd-0"), Until(5s));
    });
    CHECK(WaitUntil([&] { return gate->entered.load(); }));
    server.server->Close();
    CHECK(client.host.Call([&] { return server.server->IsClosed(); }));
    // 放行 handler：回复写已被封口，客户端必须收到非成功终态（确定错误或
    // 未知结果），不允许悬挂或假成功。
    gate->Release();
    const auto outcome = Get(inflight);
    CHECK(!outcome);
    CHECK(server.server->WaitStopped(std::chrono::steady_clock::now() + 3s));
    server.Finish();
    std::cout << "rpc close drops inflight reply success=false code="
              << static_cast<int>(outcome.error().code) << "\n";
}

void ReleasedClientHandle() {
    // 在途调用期间释放最后外部 RPC 句柄：协程持有局部安全引用，调用照常真实
    // 完成（Close 只逻辑封口，在飞借用不受影响）；共享 transport 随析构封口，
    // 连接退役出账、Engine 收口。
    auto gate = std::make_shared<Gate>();
    ServerHost server([&](IncomingCallContext, RpcEnvelope request)
                          -> result<RpcEnvelope> {
        gate->Pause();
        return result<RpcEnvelope>::ok(std::move(request));
    });
    ClientFixture client(server.Base());
    auto only = client.client;   // 外部唯一 RPC 句柄
    client.client.reset();
    auto inflight = client.host.Submit([&] {
        return bbt_co_await only->Call(server.Endpoint(),
            MakeEnvelope("s", "m", "rh-1"), Until(5s));
    });
    CHECK(WaitUntil([&] { return gate->entered.load(); }));  // 请求已提交
    only.reset();                                            // 释放最后外部句柄
    gate->Release();
    const auto reply = Get(inflight);
    CHECK(reply && reply.value().request_id == "rh-1");
    // 句柄析构已封口共享 transport；连接退役出账（物理回收凭据）。
    CHECK(client.host.Call([&] { return client.http_client->IsClosed(); }));
    CHECK(WaitUntil([&] { return client.http_client->Stats().budget == 0; }));
    server.Finish();
    std::cout << "rpc released client handle inflight=completed "
                 "transport=closed budget=0\n";
}

void CompatFactory() {
    // 源兼容工厂仍装配同一 HTTP adapter 栈：rpc::Server::Create(engine, handler)
    // 与 rpc::Client::Create(http_client) 可链接、行为与 seam 路径等价
    // （旧 include consumer 兼容窗口证据）。
    std::atomic<unsigned> handled{0};
    auto server_engine =
        std::make_shared<net::Engine>(std::shared_ptr<net::RawHandler>{});
    auto made_server = rpc::Server::Create(server_engine, EchoHandler(&handled));
    CHECK(made_server);
    auto server = std::move(made_server).value();
    net::EngineConfig config;
    config.runtime_progress = net::RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    std::error_code address_error;
    config.listen_addresses = {net::Address::Ip("127.0.0.1", 0, address_error)};
    CHECK(!address_error);
    std::error_code run_error;
    std::thread runner([&] { run_error = server_engine->Run(config); });
    CHECK(WaitUntil([&] { return server_engine->Phase() == net::EnginePhase::Running; }));
    CHECK(server->Start());
    std::vector<net::Address> addresses;
    CHECK(!server_engine->LocalAddresses(addresses) && addresses.size() == 1);
    const std::string endpoint =
        "127.0.0.1:" + std::to_string(addresses.front().Port());

    Host host;
    http::ClientOptions options;
    options.base_url = "http://" + endpoint;
    auto made_http = http::Client::Create(host.engine, options);
    CHECK(made_http);
    auto made_client = rpc::Client::Create(std::move(made_http).value());
    CHECK(made_client);
    auto client = std::move(made_client).value();
    auto reply = host.Call([&] {
        return bbt_co_await client->Call({"http", endpoint},
            MakeEnvelope("echo", "Say", "cf-1", Pack("c", 7)), Until());
    });
    CHECK(reply && reply.value().request_id == "cf-1" && handled.load() == 1);
    client->Close();
    server->StopAccepting();
    CHECK(server->Drain(std::chrono::steady_clock::now() + 3s));
    server->Close();
    CHECK(server->WaitStopped(std::chrono::steady_clock::now() + 3s));
    server_engine->RequestClose();
    runner.join();
    CHECK(!run_error && server_engine->Phase() == net::EnginePhase::Stopped);
    std::cout << "rpc compat factory engine/http_client overloads link+roundtrip=1\n";
}

void InboundCycleRelease() {
    // B-1：HTTP inbound adapter 不得成环——rpc::Server Close + 释放全部外部
    // 强引用后，weak<Engine> 必须 expired（对照组 adapter/Server 同理释放）。
    // 引用环会让 http::Server 持有的 bridge lambda 反向持有 adapter State，
    // Close 后 Engine 仍被 State 抓住无法过期。
    std::weak_ptr<net::Engine> weak_engine;
    std::weak_ptr<rpc::Server> weak_server;
    {
        auto engine =
            std::make_shared<net::Engine>(std::shared_ptr<net::RawHandler>{});
        weak_engine = engine;
        auto inbound = rpc::HttpRpcInboundTransport::Create(engine, {});
        CHECK(inbound);
        auto made = rpc::Server::Create(
            std::shared_ptr<rpc::IRpcInboundTransport>(
                std::move(inbound).value()),
            EchoHandler());
        CHECK(made);
        auto server = std::move(made).value();
        weak_server = server;
        // 未 Start：http::Server 已创建并挂 bridge lambda，Close 后释放。
        server->Close();
        server.reset();
    }
    CHECK(weak_server.expired());
    CHECK(weak_engine.expired());   // 引用环已断开：Engine 随 Close 释放
    std::cout << "rpc inbound cycle release engine expired="
              << weak_engine.expired() << "\n";
}

void DestroyDuringInflight() {
    // B-2：rpc::Server 析构不等待在途 handler——handler 必须靠共享计数
    // 的 wire handler 保持引用稳定，不能借裸 State*。在途 handler 挂在
    // gate 上、Server 析构后放行，回调必须安全完成（不悬空、不 UAF）。
    auto gate = std::make_shared<Gate>();
    std::atomic<unsigned> entered{0};
    std::weak_ptr<net::Engine> weak_engine;
    net::Address address;
    {
        auto engine =
            std::make_shared<net::Engine>(std::shared_ptr<net::RawHandler>{});
        weak_engine = engine;
        auto inbound = rpc::HttpRpcInboundTransport::Create(engine, {});
        CHECK(inbound);
        auto made = rpc::Server::Create(
            std::shared_ptr<rpc::IRpcInboundTransport>(
                std::move(inbound).value()),
            [&](IncomingCallContext, RpcEnvelope request)
                -> result<RpcEnvelope> {
                entered.fetch_add(1);
                gate->Pause();   // 阻塞在 Server 析构之后
                return result<RpcEnvelope>::ok(std::move(request));
            });
        CHECK(made);
        auto server = std::move(made).value();
        net::EngineConfig config;
        config.runtime_progress = net::RuntimeProgress::Independent;
        config.options.num_event_loop = 1;
        std::error_code error;
        config.listen_addresses = {net::Address::Ip("127.0.0.1", 0, error)};
        CHECK(!error);
        std::thread runner([&] { (void)engine->Run(config); });
        CHECK(WaitUntil([&] {
            return engine->Phase() == net::EnginePhase::Running; }));
        CHECK(server->Start());
        std::vector<net::Address> addresses;
        CHECK(!engine->LocalAddresses(addresses) && !addresses.empty());
        address = addresses[0];

        Host host;
        http::ClientOptions options;
        options.base_url = "http://127.0.0.1:" + std::to_string(address.Port());
        auto made_http = http::Client::Create(host.engine, options);
        CHECK(made_http);
        auto transport = rpc::HttpRpcTransport::Create(
            std::move(made_http).value());
        CHECK(transport);
        auto made_client = rpc::Client::Create(
            std::shared_ptr<rpc::IRpcTransport>(
                std::move(transport).value()));
        CHECK(made_client);
        auto client = std::move(made_client).value();
        auto inflight = host.Submit([&] {
            return bbt_co_await client->Call(
                {"http", "127.0.0.1:" + std::to_string(address.Port())},
                MakeEnvelope("s", "m", "dd-0"), Until(5s));
        });
        CHECK(WaitUntil([&] { return entered.load() == 1; }));
        // 在途 handler 已进 gate 时析构 rpc::Server：Close 封 listener/
        // 连接，handler 本体经 wire 共享计数保活，不悬空。
        server.reset();
        gate->Release();
        // 回复可能因 Close 被封口而不送达：只断言调用真实终态（非悬挂），
        // handler 已安全返回。
        const auto outcome = Get(inflight);
        (void)outcome;
        client->Close();
        CHECK(server.use_count() == 0);
        engine->RequestClose();
        runner.join();
        engine.reset();
    }
    CHECK(weak_engine.expired());
    std::cout << "rpc destroy-during-inflight handler=safe engine expired="
              << weak_engine.expired() << "\n";
}

} // namespace

int RunCase(const std::string& mode) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 256 * 1024;
    g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
    g_scheduler->Start();
    if (mode == "client_close") { ClientClose(); }
    else if (mode == "server_lifecycle") { ServerLifecycle(); }
    else if (mode == "inflight_authorization") { InflightAuthorization(); }
    else if (mode == "close_drops_inflight") { CloseDropsInflightReply(); }
    else if (mode == "released_client_handle") { ReleasedClientHandle(); }
    else if (mode == "compat_factory") { CompatFactory(); }
    else if (mode == "inbound_cycle_release") { InboundCycleRelease(); }
    else if (mode == "destroy_during_inflight") { DestroyDuringInflight(); }
    else { CHECK(false); }
    std::cout << "rpc lifetime " << mode << " PASS\n";
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
