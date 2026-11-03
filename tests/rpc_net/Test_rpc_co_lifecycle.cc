// RPC Co 门禁、关闭回调及会话 wire 失效边界；不启动网络线程。
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
#include <bbt/infra/rpc/Client.hpp>
#include <bbt/infra/rpc/Codec.hpp>
#include <bbt/infra/rpc/Options.hpp>
#include <bbt/infra/rpc/Server.hpp>

namespace rpc = bbt::infra::rpc;
namespace co = bbt::coroutine;
using bbt::infra::result;
using bbt::infra::ErrorCode;

namespace {
struct Runtime {
    Runtime() { if (!g_scheduler->IsInitialized()) g_scheduler->Start(); }
};

void OnCo(std::function<void()> action) {
    auto done = std::make_shared<std::promise<void>>();
    auto future = done->get_future();
    bool submitted = false;
    g_scheduler->RegistCoroutineTask([done, action = std::move(action)] {
        try { action(); done->set_value(); }
        catch (...) { done->set_exception(std::current_exception()); }
    }, submitted);
    BOOST_REQUIRE(submitted);
    if (future.wait_for(std::chrono::seconds(5)) != std::future_status::ready)
        std::terminate();
    future.get();
}

template<class T>
void ExpectCode(const result<T>& value, ErrorCode code) {
    BOOST_CHECK(!value && value.error().code == code);
}

class Transport final : public rpc::IRpcTransport {
public:
    std::atomic<int> exchanges{0};
    std::atomic<int> closed_callbacks{0};
    std::atomic_bool closed{false};
    bbt_co_async(result<rpc::WireReply>) Exchange(
        const bbt::infra::RpcAddress&, rpc::WireRequest,
        const bbt::infra::CallOptions&) override {
        ++exchanges;
        return result<rpc::WireReply>::err(bbt::infra::MakeError(
            ErrorCode::Unavailable, "test transport must not be called"));
    }
    void Close() noexcept override {
        if (!closed.exchange(true)) ++closed_callbacks;
    }
    bool IsClosed() const noexcept override { return closed.load(); }
};

class Inbound final : public rpc::IRpcInboundTransport {
public:
    int closes = 0;
    result<void> Start(rpc::WireInboundHandler) override { return result<void>::ok(); }
    void StopAccepting() noexcept override {}
    result<void> Drain(co::Deadline) override { return result<void>::ok(); }
    void Close() noexcept override { ++closes; }
    result<void> WaitStopped(co::Deadline) override { return result<void>::ok(); }
    bool IsClosed() const noexcept override { return closes != 0; }
};
}

BOOST_FIXTURE_TEST_SUITE(co_lifecycle, Runtime)

BOOST_AUTO_TEST_CASE(non_co_legacy_call_rejected_even_when_closed) {
    auto transport = std::make_shared<Transport>();
    auto created = rpc::Client::Create(transport);
    BOOST_REQUIRE(created);
    auto client = std::move(created).value();
    client->Close();
    const bbt::infra::RpcAddress invalid{};
    const bbt::infra::CallOptions expired{};
    // P1-11：非协程线程——bbt_co_await 的前置协程上下文检查在被测 API
    // 执行前抛 std::logic_error（既定协议），被测产品方法根本不会执行，
    // 因此这里不能断言 product result gate，只验证 await 前置异常协议。
    BOOST_CHECK_THROW(
        (void)(bbt_co_await client->Call(invalid, {}, expired)),
        std::logic_error);
    BOOST_CHECK_EQUAL(transport->exchanges.load(), 0);
    BOOST_CHECK_EQUAL(transport->closed_callbacks.load(), 1);
}

BOOST_AUTO_TEST_CASE(non_co_session_call_send_do_not_create_pending_work) {
    auto engine = bbt::infra::net::Engine::Create(nullptr);
    rpc::ClientOptions options;
    options.SetLocalIdentity("gate-test").SetTrustedNetworkPlaintext(true);
    auto created = rpc::Client::Create(engine, options);
    BOOST_REQUIRE(created);
    auto client = std::move(created).value();
    for (int closed = 0; closed != 2; ++closed) {
        // P1-11：非协程线程——bbt_co_await 前置检查先于产品方法抛
        // std::logic_error；被测 API 不执行，不能断言 InvalidContext result。
        // 这里只验证 await 前置异常协议与「不产生副作用」。
        BOOST_CHECK_THROW(
            (void)(bbt_co_await client->Call("invalid", rpc::Request{}, {})),
            std::logic_error);
        BOOST_CHECK_THROW(
            (void)(bbt_co_await client->Send("invalid", rpc::Request{},
                                            rpc::SendOptions{})),
            std::logic_error);
        const auto stats = client->Stats();
        BOOST_CHECK_EQUAL(stats.sessions, 0U);
        BOOST_CHECK_EQUAL(stats.links, 0U);
        BOOST_CHECK_EQUAL(stats.remote_acceptance_pending, 0U);
        BOOST_CHECK_EQUAL(stats.service.queued, 0U);
        BOOST_CHECK_EQUAL(stats.actor.inflight, 0U);
        BOOST_CHECK_EQUAL(engine->CountConnections(), 0U);
        BOOST_CHECK(engine->Phase() == bbt::infra::net::EnginePhase::Idle);
        client->Close();
    }
    OnCo([&] {
        rpc::Request request("echo", "call");
        request.SetReply(rpc::ReplyMode::Required);
        bbt::infra::CallOptions call;
        call.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
        ExpectCode(bbt_co_await client->Call("tcp://127.0.0.1:1", request, call),
                   ErrorCode::Closed);
    });
}

BOOST_AUTO_TEST_CASE(close_callback_once_and_observation_does_not_retain_server) {
    const auto before = co::GetCoObjectStats("infra.rpc.Server");
    auto inbound = std::make_shared<Inbound>();
    auto created = rpc::Server::Create(inbound,
        [](bbt::infra::IncomingCallContext, bbt::infra::RpcEnvelope request) {
            return result<bbt::infra::RpcEnvelope>::ok(std::move(request));
        });
    BOOST_REQUIRE(created);
    auto server = std::move(created).value();
    std::weak_ptr<rpc::Server> weak = server;
    const auto identity = server->GetObjectInfo();
    BOOST_CHECK(identity.id != 0);
    BOOST_REQUIRE(co::GetCoObjectManager()->GetObjectInfo(identity.id));
    server->Close();
    server->Close();
    BOOST_CHECK(server->IsClosed());
    BOOST_CHECK_EQUAL(inbound->closes, 1);
    BOOST_CHECK_EQUAL(co::GetCoObjectStats("infra.rpc.Server").alive, before.alive + 1);
    server.reset();
    BOOST_CHECK(weak.expired());
    BOOST_CHECK(!co::GetCoObjectManager()->GetObjectInfo(identity.id));
    BOOST_CHECK_EQUAL(inbound->closes, 1);
    const auto after = co::GetCoObjectStats("infra.rpc.Server");
    BOOST_CHECK_EQUAL(after.alive, before.alive);
    BOOST_CHECK_EQUAL(after.created_total, before.created_total + 1);
    BOOST_CHECK_EQUAL(after.destroyed_total, before.destroyed_total + 1);
}

BOOST_AUTO_TEST_CASE(failed_create_has_no_committed_observation) {
    const auto before = co::GetCoObjectStats("infra.rpc.Client");
    std::shared_ptr<rpc::IRpcTransport> missing;
    ExpectCode(rpc::Client::Create(missing), ErrorCode::InvalidArgument);
    const auto after = co::GetCoObjectStats("infra.rpc.Client");
    BOOST_CHECK_EQUAL(after.created_total, before.created_total);
    BOOST_CHECK_EQUAL(after.destroyed_total, before.destroyed_total);
    BOOST_CHECK_EQUAL(after.alive, before.alive);
}

BOOST_AUTO_TEST_CASE(wire_terminal_error_survives_move_and_valid_followup) {
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

BOOST_AUTO_TEST_CASE(wire_budget_and_acceptance_presence_boundaries) {
    rpc::SessionFrame frame;
    frame.kind = rpc::FrameKind::Call;
    frame.traffic = rpc::TrafficClass::Service;
    frame.session_epoch = 1;
    frame.request_id = "request";
    frame.service = "echo";
    frame.method = "call";
    frame.reply = rpc::ReplyMode::Required;
    // Call 必填 request_schema/response_schema（ValidateKindRules 的必填路由
    // 字段）；缺它们会把预算边界测试误报成 schema 缺失。
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
    frame.acceptance_requested = false; // presence is forbidden on Call, even false.
    ExpectCode(rpc::ValidateSessionFrame(frame), ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_SUITE_END()
