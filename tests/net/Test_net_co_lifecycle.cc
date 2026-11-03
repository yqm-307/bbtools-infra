// Co 入口快路径门禁与 native Engine 的非拥有观测回归。
#define BOOST_TEST_MODULE net.co_lifecycle
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <future>
#include <functional>
#include <exception>
#include <memory>
#include <string>
#include <type_traits>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/object/CoRuntimeOnly.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>

namespace net = bbt::infra::net;
namespace co = bbt::coroutine;

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
        std::terminate(); // 超时不允许在途 action 借用已退出的用例栈。
    future.get();
}
}

BOOST_FIXTURE_TEST_SUITE(co_lifecycle, Runtime)

BOOST_AUTO_TEST_CASE(non_co_dial_accept_have_no_submission) {
    net::Engine engine(nullptr);
    std::error_code error;
    const auto address = net::Address::Ip("127.0.0.1", 1, error);
    BOOST_REQUIRE(!error);
    const auto dialed = engine.CoDial(address, {});
    const auto accepted = engine.CoAccept({});
    BOOST_CHECK(dialed.error == std::errc::operation_not_permitted);
    BOOST_CHECK(accepted.error == std::errc::operation_not_permitted);
    BOOST_CHECK(!dialed.connection.Valid());
    BOOST_CHECK(!accepted.connection.Valid());
    BOOST_CHECK(!dialed.reclaim.Valid());
    BOOST_CHECK(!accepted.reclaim.Valid());
    BOOST_CHECK(engine.Phase() == net::EnginePhase::Idle);
    BOOST_CHECK_EQUAL(engine.CountConnections(), 0U);
    engine.RequestClose();
    // 同一封口对象的 Co 调用返回真实终态，不再是上下文错误。
    BOOST_CHECK(engine.CoAccept({}).error == std::errc::operation_not_permitted);
    OnCo([&] {
        BOOST_CHECK(engine.CoAccept({}).error == std::errc::operation_canceled);
    });
}

BOOST_AUTO_TEST_CASE(non_co_connection_ready_paths_do_not_consume) {
    auto state = std::make_shared<net::ConnectionState>(1, 4096, 4096, 8, nullptr);
    net::CoTcpConnection connection(net::ConnectionTestAccess::Make(state));
    const auto before = connection.Status();
    const auto read = connection.Read({});
    const auto written = connection.Write("payload", {});
    BOOST_CHECK(read.error == std::errc::operation_not_permitted);
    BOOST_CHECK(written.wait_error == std::errc::operation_not_permitted);
    BOOST_CHECK(!written.operation.Valid());
    BOOST_CHECK_EQUAL(written.outcome.sent, 0U);
    BOOST_CHECK(connection.WaitClosed({}) == std::errc::operation_not_permitted);
    const auto after = connection.Status();
    BOOST_CHECK_EQUAL(after.readable_bytes, before.readable_bytes);
    BOOST_CHECK(after.write == before.write);
    BOOST_CHECK(!after.closed);

    // native 非挂起提交保持合法；已完成票仍不能绕过 Co 等待门禁。
    auto operation = connection.AsyncWrite("");
    BOOST_REQUIRE(operation.Valid());
    BOOST_CHECK(operation.Wait({}, nullptr) == std::errc::operation_not_permitted);
    connection.RequestClose();
    connection.RequestClose();
    BOOST_CHECK(connection.Status().closed);
    BOOST_CHECK(connection.WaitClosed({}) == std::errc::operation_not_permitted);
    OnCo([&] { BOOST_CHECK(!connection.WaitClosed({})); });
}

BOOST_AUTO_TEST_CASE(engine_observation_does_not_require_co_or_own_engine) {
    static_assert(!std::is_base_of_v<co::CoRuntimeOnly, net::Engine>);
    const auto before = co::GetCoObjectStats("infra.net.Engine");
    auto engine = net::Engine::Create(nullptr);
    std::weak_ptr<net::Engine> weak = engine;
    const auto identity = engine->GetObjectInfo();
    BOOST_CHECK(identity.id != 0);
    BOOST_REQUIRE(co::GetCoObjectManager()->GetObjectInfo(identity.id));
    engine->RequestClose();
    engine->RequestClose();
    BOOST_CHECK_EQUAL(co::GetCoObjectStats("infra.net.Engine").alive, before.alive + 1);
    engine.reset();
    BOOST_CHECK(weak.expired());
    BOOST_CHECK(!co::GetCoObjectManager()->GetObjectInfo(identity.id));
    const auto after = co::GetCoObjectStats("infra.net.Engine");
    BOOST_CHECK_EQUAL(after.created_total, before.created_total + 1);
    BOOST_CHECK_EQUAL(after.destroyed_total, before.destroyed_total + 1);
    BOOST_CHECK_EQUAL(after.alive, before.alive);
}

BOOST_AUTO_TEST_SUITE_END()
