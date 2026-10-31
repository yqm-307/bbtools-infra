// rpc_net 真实双端消费：新 rpc::Client ↔ 新 rpc::Server 经真实 loopback TCP
// （双 Engine、真实 http 栈）。覆盖成功回程、关联字段、payload 编解码、
// metadata 透传、deadline local-min 与同连接复用证据。

#include "TestSupport.hpp"

namespace rpc_net_test {
namespace {

using rpc::RpcWireEnvelope;

void Roundtrip() {
    std::atomic<unsigned> handled{0};
    ServerHost server(EchoHandler(&handled));
    ClientFixture client(server.Base());

    auto reply = client.host.Call([&] {
        return bbt_co_await client.client->Call(server.Endpoint(),
            MakeEnvelope("echo", "Say", "rt-1", Pack("hello", 42)), Until());
    });
    CHECK(reply && reply.value().request_id == "rt-1" &&
          reply.value().service == "echo" && reply.value().method == "Say");
    CHECK(reply.value().request_schema == std::string(rpc::kRpcPositionalSchema));
    const auto [text, number] = Unpack(reply.value().payload);
    CHECK(text == "hello" && number == 42);
    CHECK(handled.load() == 1);
    server.Finish();
    std::cout << "rpc roundtrip success payload=(string,int64) handled=1\n";
}

void MetadataAndConcurrentCalls() {
    // 两条并发 Call（不同 request_id）的回复不串线；metadata 透传到 handler
    // 并随回复返回。slow 分支延迟返回，先发的 fast 先回，检验关联不混。
    ServerHost server([](IncomingCallContext, RpcEnvelope request)
                          -> result<RpcEnvelope> {
        request.metadata.push_back({"fw.seen", "1"});
        if (request.request_id == "slow") { bbtco_sleep(120); }
        return result<RpcEnvelope>::ok(std::move(request));
    });
    ClientFixture client(server.Base());
    auto do_call = [&](const char* id) {
        auto env = MakeEnvelope("echo", "Say", id, Pack(id, 1));
        env.metadata = {{"trace.id", id}};
        return bbt_co_await client.client->Call(server.Endpoint(), std::move(env), Until());
    };
    auto fast = client.host.Submit([&] { return do_call("fast"); });
    auto slow = client.host.Submit([&] { return do_call("slow"); });
    const auto fast_reply = Get(fast);
    const auto slow_reply = Get(slow);
    CHECK(fast_reply && fast_reply.value().request_id == "fast" &&
          fast_reply.value().payload == Pack("fast", 1));
    CHECK(slow_reply && slow_reply.value().request_id == "slow" &&
          slow_reply.value().payload == Pack("slow", 1));
    bool fast_seen = false;
    for (const auto& [k, v] : fast_reply.value().metadata) {
        fast_seen |= (k == "fw.seen" && v == "1") || (k == "trace.id" && v == "fast");
    }
    CHECK(fast_seen);
    server.Finish();
    std::cout << "rpc concurrent association fast/slow no-crosstalk metadata=passed\n";
}

void BudgetLocalMin() {
    // handler 侧期限 = min(本请求期限, now + remaining_budget_ms)：
    // 远端预算 80ms，handler 挂屏障 300ms+ → 回复时预算已尽 → TimedOut 错误
    // 信封；客户端 Until(3s) 足够等到该信封（不是客户端自身超时）。
    auto gate = std::make_shared<Gate>();
    std::atomic<bool> observed_local_min{false};
    ServerHost server([&](IncomingCallContext context, RpcEnvelope request)
                          -> result<RpcEnvelope> {
        // 进入即校验 local-min：不晚于进入时刻 + 80ms 的硬上界。
        const auto now = std::chrono::steady_clock::now();
        observed_local_min.store(context.deadline <= now + std::chrono::milliseconds{100});
        gate->Pause();
        return result<RpcEnvelope>::ok(std::move(request));
    });
    ClientFixture client(server.Base());

    // 直接注入 80ms 远端预算（rpc::Call 的预算源自调用方 deadline，这里经
    // 原始 http client 手工构造 wire body 验证 server 侧 local-min）。
    auto wire = rpc::ToWireEnvelope(
        MakeEnvelope("echo", "Say", "budget-1", Pack("b", 2)), 80);
    CHECK(wire);
    auto body_bytes = rpc::EncodeRpcWireEnvelope(wire.value(), false);
    CHECK(body_bytes);
    auto raw = client.host.Submit([&] {
        http::RequestBody body;
        body.content.assign(body_bytes.value().begin(), body_bytes.value().end());
        body.content_type = rpc::kRpcWireContentType;
        return bbt_co_await client.http_client->Post(
            server.Base() + rpc::kRpcWireHttpPath, std::move(body), {}, Until(3s));
    });
    // handler 已进入且观察到 local-min 后，再等预算（80ms）确定过期才放行，
    // 保证回信封路径（而非成功回包）。
    CHECK(WaitUntil([&] { return observed_local_min.load(); }));
    std::this_thread::sleep_for(300ms);
    gate->Release();
    const auto response = Get(raw);
    CHECK(response && response.value().status_code == 200);
    auto reply_env = rpc::ParseRpcWireHttpResponse(response.value());
    CHECK(reply_env && !reply_env.value().success);
    CHECK(reply_env.value().error.code == ErrorCode::TimedOut);
    CHECK(reply_env.value().request_id == "budget-1");
    server.Finish();

    // 正常路径：预算充足的调用，handler 观察到的期限不晚于 now+budget。
    std::atomic<bool> bounded{false};
    ServerHost normal([&](IncomingCallContext context, RpcEnvelope request)
                          -> result<RpcEnvelope> {
        bounded.store(context.deadline <= std::chrono::steady_clock::now() + 2s);
        return result<RpcEnvelope>::ok(std::move(request));
    });
    ClientFixture normal_client(normal.Base());
    auto ok = normal_client.host.Call([&] {
        return bbt_co_await normal_client.client->Call(normal.Endpoint(),
            MakeEnvelope("echo", "Say", "budget-2", Pack("b", 3)), Until(2s));
    });
    CHECK(ok);
    CHECK(bounded.load());
    normal.Finish();
    std::cout << "rpc budget local-min observed=1 handler-overtime=TimedOut ok-path=bounded\n";
}

void Reuse() {
    // 同一 client 连续三次 Call：handler 计数 3，底层 http 池只有一条空闲
    // 连接（真实同连接复用证据）。
    std::atomic<unsigned> handled{0};
    ServerHost server(EchoHandler(&handled));
    ClientFixture client(server.Base());
    for (unsigned i = 0; i != 3; ++i) {
        auto reply = client.host.Call([&] {
            return bbt_co_await client.client->Call(server.Endpoint(),
                MakeEnvelope("echo", "Say", "reuse-" + std::to_string(i),
                             Pack("r", static_cast<std::int64_t>(i))), Until());
        });
        CHECK(reply && reply.value().request_id == "reuse-" + std::to_string(i));
    }
    CHECK(handled.load() == 3);
    const auto stats = client.http_client->Stats();
    CHECK(stats.idle == 1 && stats.budget == 1);
    server.Finish();
    std::cout << "rpc reuse calls=3 handler=3 pool idle=1 budget=1\n";
}

void TransportDefaults() {
    // 回归：HTTP 共享 transport 的默认配置不得破坏固定 RPC profile——
    // 默认 Content-Type（大小写混排）、默认 params 都存在时，真实 rpc::Server
    // handler 仍必须成功；其他默认头（Authorization）不被清掉；无默认配置的
    // 正常控制例同样成功（绑定检查未放宽）。fixture 按阶段作用域收口，任意
    // 时刻并发 Engine 数与既有用例一致。
    std::atomic<unsigned> handled{0};
    ServerHost server(EchoHandler(&handled));
    {
        ClientFixture tuned(server.Base(), [](http::ClientOptions& options) {
            options.headers = {{"content-TYPE", "application/json"},
                               {"Authorization", "Bearer tenant"}};
            options.params = {{"tenant", "a"}};
        });
        auto reply = tuned.host.Call([&] {
            return bbt_co_await tuned.client->Call(server.Endpoint(),
                MakeEnvelope("echo", "Say", "td-1", Pack("t", 1)), Until());
        });
        CHECK(reply && reply.value().request_id == "td-1");
        const auto [text, number] = Unpack(reply.value().payload);
        CHECK(text == "t" && number == 1);
    }
    CHECK(handled.load() == 1);
    {
        // 正常控制例：无默认配置的 client 同样成功。
        ClientFixture plain(server.Base());
        auto plain_reply = plain.host.Call([&] {
            return bbt_co_await plain.client->Call(server.Endpoint(),
                MakeEnvelope("echo", "Say", "td-2", Pack("t", 2)), Until());
        });
        CHECK(plain_reply && plain_reply.value().request_id == "td-2");
    }
    CHECK(handled.load() == 2);
    server.Finish();

    // wire 事实钉死：线上恰好一个 application/x-protobuf、精确 /rpc（无
    // query），默认授权头保留。handler 只记录观察并回合法成功信封。
    std::atomic<unsigned> type_headers{0};
    std::atomic<bool> type_exact{false};
    std::atomic<bool> target_exact{false};
    std::atomic<bool> auth_kept{false};
    HttpServerHost wire_server(
        [&](IncomingCallContext, http::IncomingRequest request)
            -> result<http::Response> {
            unsigned types = 0;
            bool exact = false;
            bool auth = false;
            for (const auto& [name, value] : request.headers) {
                if (name == rpc::kRpcWireContentTypeKey) {
                    ++types;
                    exact = exact || value == rpc::kRpcWireContentType;
                }
                if (name == "Authorization" && value == "Bearer tenant") {
                    auth = true;
                }
            }
            type_headers.store(types);
            type_exact.store(exact);
            target_exact.store(request.target == rpc::kRpcWireHttpPath);
            auth_kept.store(auth);
            auto wire_env = rpc::ToWireEnvelope(
                MakeEnvelope("s", "m", "td-3"), 1);
            CHECK(wire_env);
            return rpc::MakeRpcWireHttpResponse(wire_env.value());
        });
    ClientFixture wire_client(wire_server.Base(), [](http::ClientOptions& options) {
        options.headers = {{"content-TYPE", "application/json"},
                           {"Authorization", "Bearer tenant"}};
        options.params = {{"tenant", "a"}};
    });
    auto outcome = wire_client.host.Call([&] {
        return bbt_co_await wire_client.client->Call(wire_server.Endpoint(),
            MakeEnvelope("s", "m", "td-3"), Until());
    });
    CHECK(outcome && outcome.value().request_id == "td-3");
    CHECK(type_headers.load() == 1 && type_exact.load());
    CHECK(target_exact.load());
    CHECK(auth_kept.load());
    wire_server.Finish();
    std::cout << "rpc transport defaults ctype=converged target=/rpc "
                 "params=dropped auth=kept control=ok\n";
}

} // namespace

int RunCase(const std::string& mode) {
    // runtime：server 1 driver 槽 + client/业务共用（用例按需 4 worker）。
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 256 * 1024;
    g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
    g_scheduler->Start();
    if (mode == "roundtrip") { Roundtrip(); }
    else if (mode == "association") { MetadataAndConcurrentCalls(); }
    else if (mode == "budget_local_min") { BudgetLocalMin(); }
    else if (mode == "reuse") { Reuse(); }
    else if (mode == "transport_defaults") { TransportDefaults(); }
    else { CHECK(false); }
    std::cout << "rpc roundtrip " << mode << " PASS\n";
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
