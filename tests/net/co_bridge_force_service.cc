// 真实 TCP 强停切片；复用固定 fixture，不改变旧回归。
#define main CoBridgeFixtureMain
#include "co_bridge_contract.cc"
#undef main
#include <fcntl.h>

// 只在本测试可执行文件链接 --wrap；模拟依赖边界异常，不声称真实 OOM。
static thread_local int close_fault = 0;
extern "C" std::error_code RealStreamClose(StreamConnection*, std::error_code)
    asm("__real__ZN3bbt5infra3net16StreamConnection5CloseESt10error_code");
extern "C" std::error_code WrapStreamClose(StreamConnection*, std::error_code)
    asm("__wrap__ZN3bbt5infra3net16StreamConnection5CloseESt10error_code");
extern "C" std::error_code WrapStreamClose(StreamConnection* connection, std::error_code reason) {
    const auto fault = std::exchange(close_fault, 0);
    if (fault == 1) { throw std::bad_alloc(); }
    const auto error = RealStreamClose(connection, reason);
    if (fault == 2) { throw std::bad_alloc(); }
    return error;
}

static std::error_code StopService(const std::shared_ptr<ConnectionBridge>& bridge) {
    return bridge->StopServiceOwner();
}

static void FinishForce(ContractFixture& fixture) {
    for (const auto& release : fixture.releases) { release(); }
    const auto deadline = Until(5s);
    for (;;) {
        Get(Task([] {}));
        OnOwner(fixture.loop, [] {});
        bool idle = true;
        for (const auto& connection : fixture.connections) {
            idle = idle && !fixture.bridge->SessionOf(connection->Id())->SnapshotState().busy;
        }
        if (idle && fixture.bridge->GetStats().ops_submitted.load()
            == fixture.bridge->GetStats().ops_executed.load()) { break; }
        CHECK(Clock::now() < deadline);
        std::this_thread::yield();
    }
    Log("force_cleanup", 0, "submitted="
        + std::to_string(fixture.bridge->GetStats().ops_submitted.load())
        + " executed=" + std::to_string(fixture.bridge->GetStats().ops_executed.load()));
    fixture.loop.RequestStop();
    CHECK(fixture.loop.Join());
    CHECK(fixture.loop.FailureCount() == 0);
    for (auto& client : fixture.clients) { client.Close(); }
    fixture.listener.Close();
    fixture.connections.clear();
    fixture.bridge.reset();
}

static void ForceTraffic() {
    WarmRuntime();
    const auto fd_before = OpenFdCount();
    std::weak_ptr<CoTcpConnection> weak_shell;
    std::weak_ptr<ConnectionSession> weak_session;
    long owner_tid = 0;
    {
        auto fixture_owned = std::make_shared<ContractFixture>();
        auto& fixture = *fixture_owned;
        fixture.Start();
        auto gate = fixture.Gate();
        auto registered = gate->registered.get_future();
        auto returned = std::make_shared<std::promise<void>>();
        auto returning = returned->get_future();
        auto handler = std::make_shared<ProbeHandler>();
        handler->on_traffic = [gate, returned](ICoConnection& connection, bool wake) {
            CHECK(!wake);
            const auto view = connection.Peek();
            CHECK(!view.empty());
            const std::string held(view);
            gate->Wait();
            CHECK(connection.Peek() == held);
            CHECK(connection.Write("handler-late", Until(2s)) == std::errc::operation_canceled);
            CHECK(connection.Wake() == std::errc::operation_canceled);
            CHECK(!connection.Discard(held.size()));
            returned->set_value();
        };
        auto connection = fixture.Connect(handler);
        auto session = fixture.bridge->SessionOf(connection->Id());
        weak_shell = connection;
        weak_session = session;
        CHECK(ClientSend(fixture.clients.back(), "SNAPSHOT"));
        Get(registered);
        CHECK(session->SnapshotState().snap_lent);
        const auto held = OwnerStats(fixture.loop, session);
        CHECK(held.snapshots_taken > held.snapshots_returned);
        auto owner_result = std::make_shared<long>(0);
        OnOwner(fixture.loop, [fixture_owned, owner_result] {
            *owner_result = ::syscall(SYS_gettid);
            CHECK(!StopService(fixture_owned->bridge));
            CHECK(!StopService(fixture_owned->bridge));
        });
        owner_tid = *owner_result;
        CHECK(!gate->released.load());
        CHECK(returning.wait_for(0ms) != std::future_status::ready);
        CHECK(session->SnapshotState().physical_closed && session->SnapshotState().snap_lent);
        CHECK(!session->SnapshotState().close_finished && !fixture.bridge->Drained());
        Get(Task([connection] {
            CHECK(connection->Wake() == std::errc::operation_canceled);
            CHECK(connection->Write("external-late", Until(2s)) == std::errc::operation_canceled);
            CHECK(!connection->Close(CloseMode::Block, {}, Until(1s)));
            CHECK(!connection->Close(CloseMode::Unblock, {}, Until(1s)));
        }));
        pollfd eof{fixture.clients.back().Fd(), POLLIN, 0};
        CHECK(::poll(&eof, 1, 2000) == 1);
        char byte{};
        const auto end = fixture.clients.back().Receive(&byte, 1);
        CHECK(!end.error && end.bytes == 0); // 无新 peer 字节，而非仅检查标签。
        gate->Release();
        Get(returning);
        const auto deadline = Until(5s);
        while (session->SnapshotState().busy) { CHECK(Clock::now() < deadline); std::this_thread::yield(); }
        const auto physical = OwnerStats(fixture.loop, session);
        const auto counters = session->GetCounters();
        CHECK(physical.physical_closes == 1);
        CHECK(physical.snapshots_taken == physical.snapshots_returned);
        CHECK(!session->SnapshotState().snap_lent);
        CHECK(counters.snapshot_return_errors == 0 && counters.handler_exceptions == 0);
        CHECK(counters.op_timeouts == 0 && counters.onclose_entered == 0);
        CHECK(!fixture.bridge->Drained() && !session->SnapshotState().close_finished);
        Log("force_physical", connection->Id(), "closes=" + std::to_string(physical.physical_closes)
            + " takes=" + std::to_string(physical.snapshots_taken)
            + " returns=" + std::to_string(physical.snapshots_returned));
        connection.reset(); session.reset(); handler.reset();
        FinishForce(fixture);
    }
    CHECK(weak_shell.expired() && weak_session.expired());
    CHECK(::access(("/proc/self/task/" + std::to_string(owner_tid)).c_str(), F_OK) == -1);
    const auto fd_after = OpenFdCount();
    Log("force_resources", 0, "fd_before=" + std::to_string(fd_before)
        + " fd_after=" + std::to_string(fd_after) + " owner_tid=" + std::to_string(owner_tid));
    CHECK(fd_after == fd_before);
}

static void ForcePending() {
    WarmRuntime();
    const auto fd_before = OpenFdCount();
    {
        auto fixture_owned = std::make_shared<ContractFixture>();
        auto& fixture = *fixture_owned;
        fixture.Start();
        auto release = std::make_shared<std::promise<void>>();
        auto released = release->get_future().share();
        auto entered = std::make_shared<std::promise<void>>();
        auto entering = entered->get_future();
        auto blocked = Task([entered, released] {
            entered->set_value();
            CHECK(released.wait_for(8s) == std::future_status::ready);
        });
        Get(entering); // 单 Processer 真阻塞：dispatcher 尚不能开始。
        auto handler = std::make_shared<ProbeHandler>();
        auto client = ConnectTo(fixture.bound);
        fixture.clients.push_back(std::move(client));
        auto client2 = ConnectTo(fixture.bound);
        fixture.clients.push_back(std::move(client2));
        struct AdoptResult {
            bool rejected{false};
            std::vector<std::shared_ptr<CoTcpConnection>> connections;
        };
        auto result = std::make_shared<AdoptResult>();
        OnOwner(fixture.loop, [fixture_owned, result, handler] {
            std::error_code error;
            Address peer;
            auto socket = fixture_owned->listener.Accept(peer, error);
            CHECK(!error);
            auto connection = fixture_owned->bridge->Adopt(std::move(socket), peer, handler, error);
            CHECK(connection && !error);
            result->connections.push_back(connection);
            CHECK(!StopService(fixture_owned->bridge));
            auto socket2 = fixture_owned->listener.Accept(peer, error);
            CHECK(!error);
            const auto fd = socket2.Fd();
            auto denied = fixture_owned->bridge->Adopt(std::move(socket2), peer, handler, error);
            result->rejected = !denied && error == std::errc::operation_canceled
                && socket2.Fd() == fd && ::fcntl(fd, F_GETFD) >= 0;
            if (denied) {
                result->connections.push_back(denied);
                (void)fixture_owned->bridge->CloseNative(denied->Id(), {});
            }
        });
        fixture.connections.insert(fixture.connections.end(), result->connections.begin(), result->connections.end());
        result->connections.clear();
        const bool rejected = result->rejected;
        release->set_value();
        Get(blocked);
        Get(Task([] {}));
        auto session = fixture.bridge->SessionOf(0);
        const auto counters = session->GetCounters();
        const bool no_events = counters.open_entered == 0 && counters.traffic_dispatched == 0
            && counters.onclose_entered == 0;
        CHECK(session->SnapshotState().physical_closed);
        // 未借 snapshot：read readiness 登记须被真实 close 撤销并结算。
        auto physical = OwnerStats(fixture.loop, session);
        const auto deadline = Until(5s);
        while (physical.wait_callbacks < physical.read_arms + physical.write_arms) {
            CHECK(Clock::now() < deadline);
            physical = OwnerStats(fixture.loop, session);
        }
        CHECK(physical.read_arms > 0 && physical.guard_exits > 0);
        CHECK(physical.wait_callbacks == physical.read_arms + physical.write_arms);
        CHECK(physical.physical_closes == 1);
        Log("force_registration", 0, "arms="
            + std::to_string(physical.read_arms + physical.write_arms)
            + " callbacks=" + std::to_string(physical.wait_callbacks)
            + " guard_exits=" + std::to_string(physical.guard_exits));
        session.reset(); // 保持旧内部前提：session 不跨桥释放单独延寿。
        FinishForce(fixture);
        Log("force_pending", 0, "adopt_rejected=" + std::to_string(rejected)
            + " no_events=" + std::to_string(no_events));
        CHECK(rejected);
        CHECK(no_events);
    }
    CHECK(OpenFdCount() == fd_before);
}

static void ForceDependencyFailure(bool before) {
    WarmRuntime();
    const auto fd_before = OpenFdCount();
    auto fixture = std::make_shared<ContractFixture>();
    fixture->Start();
    for (int i = 0; i != 2; ++i) {
        auto gate = fixture->Gate();
        auto opened = gate->registered.get_future();
        auto handler = std::make_shared<ProbeHandler>();
        handler->on_open = [gate](ICoConnection&) { gate->Wait(); };
        fixture->Connect(handler);
        Get(opened);
    }
    struct Result {
        std::error_code first, retry;
        bool threw{false};
        bool first_eof[2]{};
        long tid{0};
        std::size_t first_closes[2]{}, retry_closes[2]{};
    };
    auto result = std::make_shared<Result>();
    OnOwner(fixture->loop, [fixture, result, before] {
        result->tid = ::syscall(SYS_gettid);
        close_fault = before ? 1 : 2;
        try { result->first = fixture->bridge->StopServiceOwner(); }
        catch (const std::bad_alloc&) { result->threw = true; }
        close_fault = 0;
        for (int i = 0; i != 2; ++i) {
            result->first_closes[i] = fixture->bridge->SessionOf(i)->ConnStatsOwner().physical_closes;
            pollfd eof{fixture->clients[i].Fd(), POLLIN, 0};
            if (::poll(&eof, 1, result->first_closes[i] ? 2000 : 0) == 1) {
                char byte{};
                result->first_eof[i] = ::recv(eof.fd, &byte, 1, MSG_DONTWAIT) == 0;
            }
        }
        result->retry = fixture->bridge->StopServiceOwner();
        for (int i = 0; i != 2; ++i) {
            result->retry_closes[i] = fixture->bridge->SessionOf(i)->ConnStatsOwner().physical_closes;
        }
        // 旧红也主动自救：不靠超时退出，不遗留第二条 fd。
        for (int i = 0; i != 2; ++i) { CHECK(!fixture->bridge->CloseNative(i, {})); }
        CHECK(!fixture->bridge->StopServiceOwner());
    });
    Log("dependency_fault", 0, "before=" + std::to_string(before)
        + " threw=" + std::to_string(result->threw) + " error=" + std::to_string(result->first.value())
        + " first=" + std::to_string(result->first_closes[0]) + "," + std::to_string(result->first_closes[1])
        + " first_eof=" + std::to_string(result->first_eof[0]) + "," + std::to_string(result->first_eof[1])
        + " retry_error=" + std::to_string(result->retry.value())
        + " retry=" + std::to_string(result->retry_closes[0]) + "," + std::to_string(result->retry_closes[1]));
    for (int i = 0; i != 2; ++i) {
        pollfd eof{fixture->clients[i].Fd(), POLLIN, 0};
        CHECK(::poll(&eof, 1, 2000) == 1);
        char byte{};
        auto end = fixture->clients[i].Receive(&byte, 1);
        CHECK(!end.error && end.bytes == 0);
        auto session = fixture->bridge->SessionOf(i);
        auto physical = OwnerStats(fixture->loop, session);
        const auto deadline = Until(5s);
        while (physical.wait_callbacks < physical.read_arms + physical.write_arms) {
            CHECK(Clock::now() < deadline);
            physical = OwnerStats(fixture->loop, session);
        }
        CHECK(physical.physical_closes == 1);
        CHECK(physical.wait_callbacks == physical.read_arms + physical.write_arms);
        Log("dependency_physical", i, "closes=" + std::to_string(physical.physical_closes)
            + " arms=" + std::to_string(physical.read_arms + physical.write_arms)
            + " callbacks=" + std::to_string(physical.wait_callbacks) + " eof=1");
    }
    FinishForce(*fixture);
    fixture.reset();
    const auto fd_after = OpenFdCount();
    CHECK(fd_after == fd_before);
    CHECK(::access(("/proc/self/task/" + std::to_string(result->tid)).c_str(), F_OK) == -1);
    Log("dependency_resources", 0, "fd_before=" + std::to_string(fd_before)
        + " fd_after=" + std::to_string(fd_after) + " owner_tid=" + std::to_string(result->tid));
    CHECK(!result->threw && result->first == std::errc::not_enough_memory);
    CHECK(result->first_closes[0] == (before ? 0u : 1u) && result->first_closes[1] == 1);
    CHECK(result->first_eof[0] == !before && result->first_eof[1]);
    CHECK(!result->retry && result->retry_closes[0] == 1 && result->retry_closes[1] == 1);
}

#include "ProcessRuntimeTestMain.hpp"

static int RunForceCases(int argc, char** argv) {
    try {
        CHECK(argc == 2);
        if (std::string(argv[1]) == "traffic") { ForceTraffic(); }
        else if (std::string(argv[1]) == "pending") { ForcePending(); }
        else if (std::string(argv[1]) == "failure_before") { ForceDependencyFailure(true); }
        else if (std::string(argv[1]) == "failure_after") { ForceDependencyFailure(false); }
        else { return 2; }
        std::cout << "CASE OK force_service " << argv[1] << " asserts=" << AssertCounter().load() << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "RUN FAILED: " << error.what() << '\n';
        return 1;
    }
}

int main(int argc, char** argv) {
    // 测试进程退出边界：局部 RAII 收口 → 刷新输出 → _Exit，见
    // ProcessRuntimeTestMain.hpp 头注（静态析构竞态为已知限制，未修复）。
    return bbt_net_test::RunTestProcessExit([&] { return RunForceCases(argc, argv); });
}
