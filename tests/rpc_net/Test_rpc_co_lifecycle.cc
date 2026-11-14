/**
 * RPC Co 门禁、关闭回调及会话 wire 失效边界；不启动网络线程。
 * V2 单一会话面：Client 由 Engine + ClientOptions 装配，Server 由
 * Engine + 同一 Client owner + admission handler 装配。
 */
#define BOOST_TEST_MODULE rpc.co_lifecycle
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <functional>
#include <exception>
#include <memory>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/rpc/Admission.hpp>
#include <bbt/infra/rpc/CoRpcClient.hpp>
#include <bbt/infra/rpc/Options.hpp>
#include <bbt/infra/rpc/CoRpcServer.hpp>
#include <bbt/infra/rpc/SessionCodec.hpp>

namespace rpc = bbt::infra::rpc;
namespace co = bbt::coroutine;
using bbt::infra::ErrorCode;
using bbt::infra::result;

namespace {
struct Runtime {
    Runtime()
    {
        if (!g_scheduler->IsInitialized())
            g_scheduler->Start();
    }
};

void OnCo(std::function<void()> action)
{
    auto done = std::make_shared<std::promise<void>>();
    auto future = done->get_future();
    bool submitted = false;
    g_scheduler->RegistCoroutineTask(
        [done, action = std::move(action)] {
            try {
                action();
                done->set_value();
            } catch (...) {
                done->set_exception(std::current_exception());
            }
        },
        submitted);
    BOOST_REQUIRE(submitted);
    if (future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
        std::terminate();
    future.get();
}

template <class T> void ExpectCode(const result<T>& value, ErrorCode code)
{
    BOOST_CHECK(!value && value.error().code == code);
}

// clang-format off
}
// clang-format on

BOOST_FIXTURE_TEST_SUITE(co_lifecycle, Runtime)

/**
 * 非协程线程调用 Call/Send：bbt_co_await 前置协程上下文检查先于被测 API
 * 执行（既定协议），被测产品方法根本不执行、不产生 pending 工作；封口后
 * 同一入口仍只给出 await 前置异常。合并自旧 non_co_legacy_call_* 与
 * non_co_session_* 两条断言（旧 transport 已被删除，无 seam 可断言）。
 */
BOOST_AUTO_TEST_CASE(non_co_session_call_send_do_not_create_pending_work)
{
    auto engine = bbt::infra::net::Engine::Create(nullptr);
    rpc::ClientOptions options;
    options.SetLocalIdentity("gate-test").SetTrustedNetworkPlaintext(true);
    auto created = rpc::CoRpcClient::Create(engine, options);
    BOOST_REQUIRE(created);
    auto client = std::move(created).value();
    for (int closed = 0; closed != 2; ++closed) {

        /**
         * 非协程线程：bbt_co_await 前置检查先于产品方法抛
         * std::logic_error；被测 API 不执行，不能断言 InvalidContext result。
         * 这里只验证 await 前置异常协议与「不产生副作用」。
         */
        BOOST_CHECK_THROW((void)(bbt_co_await client->Call("invalid", rpc::Request{}, {})), std::logic_error);
        BOOST_CHECK_THROW(
            (void)(bbt_co_await client->Send("invalid", rpc::Request{}, rpc::SendOptions{})), std::logic_error);
        const auto stats = client->Stats();
        BOOST_CHECK_EQUAL(stats.sessions, 0U);
        BOOST_CHECK_EQUAL(stats.links, 0U);
        BOOST_CHECK_EQUAL(stats.remote_acceptance_pending, 0U);
        BOOST_CHECK_EQUAL(stats.service.queued, 0U);
        BOOST_CHECK_EQUAL(stats.actor.inflight, 0U);
        BOOST_CHECK_EQUAL(engine->CountConnections(), 0U);
        BOOST_CHECK(engine->Phase() == bbt::infra::net::EnginePhase::Idle);
        (void)client->Close(bbt::infra::CloseOptions::Immediate());
    }
    OnCo([&] {
        rpc::Request request("echo", "call");
        request.SetReply(rpc::ReplyMode::Required);
        bbt::infra::CallOptions call;
        call.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
        ExpectCode(bbt_co_await client->Call("tcp://127.0.0.1:1", request, call), ErrorCode::Closed);
    });
}

/**
 * 统一关闭入口（Shutdown）幂等、IsClosed 只读、Co 对象观测不延长 Server
 * 寿命（弱引用与 created/destroyed 台账收口）。旧 inbound transport 的 close
 * 计数随该接口删除，收口凭据改为 Co 对象台账与弱引用。
 */
BOOST_AUTO_TEST_CASE(close_callback_once_and_observation_does_not_retain_server)
{
    const auto before = co::GetCoObjectStats("infra.rpc.CoRpcServer");
    auto engine = bbt::infra::net::Engine::Create(nullptr);
    rpc::ClientOptions client_options;
    client_options.SetLocalIdentity("close-observer").SetTrustedNetworkPlaintext(true);
    auto made_client = rpc::CoRpcClient::Create(engine, client_options);
    BOOST_REQUIRE(made_client);
    auto client = std::move(made_client).value();

    rpc::ServerOptions server_options;
    std::error_code address_error;
    server_options.SetListenAddress(bbt::infra::net::Address::Ip("127.0.0.1", 0, address_error));
    BOOST_REQUIRE(!address_error);
    rpc::AdmissionHandler handler = [](rpc::AdmissionItem&& item) {
        (void)item;
        return result<void>::err(bbt::infra::MakeError(ErrorCode::Unavailable, "not started"));
    };
    auto created = rpc::CoRpcServer::Create(engine, client, std::move(handler), server_options);
    BOOST_REQUIRE(created);
    auto server = std::move(created).value();
    std::weak_ptr<rpc::CoRpcServer> weak = server;
    const auto identity = server->GetObjectInfo();
    BOOST_CHECK(identity.id != 0);
    BOOST_REQUIRE(co::GetCoObjectManager()->GetObjectInfo(identity.id));
    (void)server->Close(bbt::infra::CloseOptions::Immediate());
    (void)server->Close(bbt::infra::CloseOptions::Immediate());
    BOOST_CHECK(server->IsClosed());
    BOOST_CHECK_EQUAL(co::GetCoObjectStats("infra.rpc.CoRpcServer").alive, before.alive + 1);
    server.reset();
    BOOST_CHECK(weak.expired());
    BOOST_CHECK(!co::GetCoObjectManager()->GetObjectInfo(identity.id));
    const auto after = co::GetCoObjectStats("infra.rpc.CoRpcServer");
    BOOST_CHECK_EQUAL(after.alive, before.alive);
    BOOST_CHECK_EQUAL(after.created_total, before.created_total + 1);
    BOOST_CHECK_EQUAL(after.destroyed_total, before.destroyed_total + 1);
}

/**
 * Create 失败不得提交 Co 对象观测：V2 以 engine/handler 缺失与空 identity
 * 为拒绝面（旧 IRpcTransport 空 seam 已随接口删除）。
 */
BOOST_AUTO_TEST_CASE(failed_create_has_no_committed_observation)
{
    const auto before = co::GetCoObjectStats("infra.rpc.CoRpcClient");
    rpc::ClientOptions options;
    options.SetLocalIdentity("create-gate").SetTrustedNetworkPlaintext(true);
    ExpectCode(rpc::CoRpcClient::Create(nullptr, options), ErrorCode::InvalidArgument);
    rpc::ClientOptions empty_identity;
    empty_identity.SetTrustedNetworkPlaintext(true);
    ExpectCode(
        rpc::CoRpcClient::Create(bbt::infra::net::Engine::Create(nullptr), empty_identity), ErrorCode::InvalidArgument);
    const auto after = co::GetCoObjectStats("infra.rpc.CoRpcClient");
    BOOST_CHECK_EQUAL(after.created_total, before.created_total);
    BOOST_CHECK_EQUAL(after.destroyed_total, before.destroyed_total);
    BOOST_CHECK_EQUAL(after.alive, before.alive);
}

BOOST_AUTO_TEST_CASE(wire_terminal_error_survives_move_and_valid_followup)
{
    rpc::SessionFrameDecoder decoder;
    const std::uint8_t zero[4] = {};
    BOOST_REQUIRE(decoder.Feed(zero, sizeof(zero)));
    ExpectCode(decoder.Next(), ErrorCode::ProtocolError);
    BOOST_CHECK(decoder.IsTerminal());
    rpc::SessionFrameDecoder moved(std::move(decoder));
    rpc::SessionFrame hello;
    hello.kind = rpc::FrameKind::Hello;
    hello.peer_identity = "peer";
    hello.session_epoch = 1;
    hello.connection_nonce = 1;
    auto valid = rpc::EncodeFramedSessionFrame(hello);
    BOOST_REQUIRE(valid);
    ExpectCode(moved.Feed(valid.value()), ErrorCode::ProtocolError);
    ExpectCode(moved.Next(), ErrorCode::ProtocolError);
    BOOST_CHECK(moved.IsTerminal());
}

BOOST_AUTO_TEST_CASE(wire_budget_and_acceptance_presence_boundaries)
{
    rpc::SessionFrame frame;
    frame.kind = rpc::FrameKind::Call;
    frame.traffic = rpc::TrafficClass::Service;
    frame.session_epoch = 1;
    frame.request_id = "request";
    frame.service = "echo";
    frame.method = "call";
    frame.reply = rpc::ReplyMode::Required;

    /**
     * Call 必填 request_schema/response_schema（ValidateKindRules 的必填路由
     * 字段）；缺它们会把预算边界测试误报成 schema 缺失。
     */
    frame.request_schema = "req";
    frame.response_schema = "resp";
    frame.remaining_budget_ms = 1;
    BOOST_REQUIRE(rpc::ValidateSessionFrame(frame));
    frame.remaining_budget_ms = 0;
    ExpectCode(rpc::ValidateSessionFrame(frame), ErrorCode::InvalidArgument);
    frame.remaining_budget_ms = rpc::kRpcV2MaxRemainingBudgetMs;
    BOOST_REQUIRE(rpc::ValidateSessionFrame(frame));
    frame.remaining_budget_ms += 1;
    ExpectCode(rpc::ValidateSessionFrame(frame), ErrorCode::InvalidArgument);
    frame.remaining_budget_ms = 1;

    /**
     * presence is forbidden on Call, even false.
     */
    frame.acceptance_requested = false;
    ExpectCode(rpc::ValidateSessionFrame(frame), ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_SUITE_END()
