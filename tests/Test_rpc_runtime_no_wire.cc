// co-network/v1 · waypoint rpc-infra-foundation 2-5（无 wire 配置面）：
// 未启用锁定的 Protobuf wire profile 时，RPC 工厂必须显式拒绝（RuntimeUnavailable），
// 不产生假成功；同时 HTTP 组件仍独立可用（本目标只链接 bbt::infra_http）。
//
// 本目标只在「未创建 bbt_infra_rpc（无 BBT_PROTOBUF_PREFIX）」的配置下注册，
// 见 tests/CMakeLists.txt 的 else 分支；有 wire 构建走 Test_rpc_runtime_faults。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/RpcClient.hpp>
#include <bbt/infra/RpcServer.hpp>

using namespace bbt::infra;

namespace {

NetworkLimits Limits() {
    NetworkLimits limits{};
    limits.max_connections  = 8;
    limits.max_inflight     = 8;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = 64 * 1024;
    limits.incoming_timeout = std::chrono::milliseconds{3000};
    return limits;
}

} // namespace

BOOST_AUTO_TEST_SUITE(rpc_runtime_no_wire)

BOOST_AUTO_TEST_CASE(t_factories_reject_without_wire) {
    auto& scheduler = *bbt::coroutine::detail::Scheduler::GetInstance();
    if (!scheduler.IsInitialized()) {
        auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
        cfg->m_cfg_static_thread_num = 2;
        scheduler.Start(bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);
    }
    BOOST_REQUIRE(scheduler.IsInitialized());

    auto created = NetworkRuntime::Create(Limits());
    BOOST_REQUIRE(created);
    auto runtime = std::move(created).value();
    BOOST_REQUIRE(runtime->Start());

    // HTTP 面不因缺 wire 而失效：G7 —— 不用「Create 成功」当运行证据，
    // 搭一个真实 ListenHttp 并对它发一条真实 HttpClient 请求，断言响应可观测。
    auto http_server = runtime->ListenHttp(
        {"127.0.0.1", 0},
        [](IncomingCallContext, HttpRequest request) -> result<HttpResponse> {
            HttpResponse response;
            response.status = 200;
            response.body   = "http-alive:" + request.method;
            return result<HttpResponse>::ok(std::move(response));
        });
    BOOST_REQUIRE(http_server);

    auto http_client = runtime->CreateHttpClient();
    BOOST_REQUIRE(http_client);

    HttpRequest http_request;
    http_request.method = "GET";
    http_request.url =
        "http://127.0.0.1:" +
        std::to_string(http_server.value()->LocalAddress().port) + "/health";
    CallOptions options;
    options.deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds{10};

    // 协程内发起真实请求：完成态与等待量都放堆上，外预算 = 内 deadline + 裕量。
    auto reply = std::make_shared<std::optional<result<HttpResponse>>>();
    auto done  = std::make_shared<bbt::core::thread::CountDownLatch>(1);
    bool started = false;
    scheduler.RegistCoroutineTask(
        [http_client = http_client.value(), http_request = std::move(http_request),
         options, reply, done]() mutable {
            reply->emplace(http_client->Request(std::move(http_request), options));
            done->Down();
        },
        started);
    BOOST_REQUIRE(started);
    BOOST_REQUIRE_EQUAL(done->WaitTimeout(15000), 0);
    BOOST_REQUIRE(reply->has_value());
    BOOST_REQUIRE(reply->value());
    BOOST_CHECK_EQUAL(reply->value().value().status, 200);
    BOOST_CHECK_EQUAL(reply->value().value().body, "http-alive:GET");

    http_server.value()->Close();
    http_client.value()->Close();

    // RPC 工厂在无 wire 时必须拒绝，且不返回可用对象。
    auto rpc_client = runtime->CreateRpcClient();
    BOOST_REQUIRE(!rpc_client);
    BOOST_CHECK(rpc_client.error().code == ErrorCode::RuntimeUnavailable);

    auto rpc_server = runtime->ListenRpc(
        {"127.0.0.1", 0},
        [](IncomingCallContext, RpcEnvelope request) -> result<RpcEnvelope> {
            return result<RpcEnvelope>::ok(std::move(request));
        });
    BOOST_REQUIRE(!rpc_server);
    BOOST_CHECK(rpc_server.error().code == ErrorCode::RuntimeUnavailable);

    runtime->Close();
    BOOST_CHECK(runtime->IsClosed());
}

BOOST_AUTO_TEST_SUITE_END()
