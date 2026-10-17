// 精确注入 dispatcher 登记入口；复用真实 TCP fixture，不影响业务测试任务。
#define main CoBridgeFixtureMain
#include "co_bridge_contract.cc"
#undef main
#include <cstring>

static std::atomic<EventLoop*> fault_owner{nullptr};
static std::atomic<bool> fault_enabled{false};
static bool persistent_failure = false;
static std::atomic<int> attempts{0}, failures{0};
static std::shared_ptr<std::promise<void>> first_failure;

extern "C" void RealRegister(bbt::coroutine::detail::Scheduler*, std::size_t,
    const std::function<void()>&, const char*)
    asm("__real__ZN3bbt9coroutine6detail9Scheduler19RegistCoroutineTaskEmRKSt8functionIFvvEEPKc");
extern "C" void WrapRegister(bbt::coroutine::detail::Scheduler*, std::size_t,
    const std::function<void()>&, const char*)
    asm("__wrap__ZN3bbt9coroutine6detail9Scheduler19RegistCoroutineTaskEmRKSt8functionIFvvEEPKc");
extern "C" void WrapRegister(bbt::coroutine::detail::Scheduler* scheduler, std::size_t stack,
    const std::function<void()>& callback, const char* description) {
    auto* owner = fault_owner.load();
    if (fault_enabled.load() && owner && owner->IsOwner() && description
        && std::strcmp(description, "co.bridge.dispatch") == 0) {
        const int attempt = ++attempts;
        if (persistent_failure || attempt == 1) {
            if (++failures == 1) { first_failure->set_value(); }
            throw std::bad_alloc(); // 固定上游保证：抛出前没有入队任务。
        }
    }
    RealRegister(scheduler, stack, callback, description);
}

static void DispatchFailure(const std::string& stage, bool persistent) {
    const auto runtime = WarmRuntime();
    EventLoop loop(64);
    fault_owner.store(&loop);
    persistent_failure = persistent;
    first_failure = std::make_shared<std::promise<void>>();
    auto rejected = first_failure->get_future();
    CHECK(loop.Start());
    std::error_code error;
    auto bridge = ConnectionBridge::Create(loop, BridgeOptions(), error);
    CHECK(bridge && !error);
    auto listener = OpenStream();
    auto client = ConnectTo(ListenOn(listener));
    auto opened = std::make_shared<std::promise<void>>();
    auto opened_future = opened->get_future();
    auto close_entered = std::make_shared<std::promise<void>>();
    auto close_future = close_entered->get_future();
    auto close_gate = std::make_shared<bbt::coroutine::sync::CoWaiter>();
    auto handler = std::make_shared<ProbeHandler>();
    handler->on_open = [opened, runtime, &loop](ICoConnection& connection) {
        CHECK(!loop.IsOwner());
        CHECK(std::this_thread::get_id() == runtime.processer);
        CHECK(bbt::coroutine::GetLocalCoroutineId() != 0);
        Log("open", 0, "managed=true");
        opened->set_value();
    };
    handler->on_traffic = [](ICoConnection&, bool) { CHECK(false); };
    handler->on_close = [close_entered, close_gate, runtime, &loop](ICoConnection& connection,
        const std::error_code&) {
        CHECK(!loop.IsOwner());
        CHECK(std::this_thread::get_id() == runtime.processer);
        CHECK(bbt::coroutine::GetLocalCoroutineId() != 0);
        Log("close_gate", 0, "managed=true");
        bbt::coroutine::sync::CombinedWaitOptions options;
        options.deadline = Until(8s);
        CHECK(close_gate->Wait(options, [close_entered] {
            close_entered->set_value(); return true;
        }) == bbt::coroutine::sync::CombinedWaitStatus::Completed);
    };
    std::shared_ptr<CoTcpConnection> connection;
    std::exception_ptr owner_exception;
    if (stage == "initial") { fault_enabled.store(true); }
    OnOwner(loop, [&] {
        Address peer;
        auto accepted = listener.Accept(peer, error);
        CHECK(!error && accepted.Fd() >= 0);
        try { connection = bridge->Adopt(std::move(accepted), peer, handler, error); }
        catch (...) { owner_exception = std::current_exception(); }
    });
    auto session = bridge->SessionOf(0);
    CHECK(session);
    if (stage != "initial") {
        CHECK(connection && !error && !owner_exception);
        Get(opened_future);
        // 单 Processer 上的后续任务是 Open dispatcher 退出屏障，而非 sleep 轮询。
        auto idle = Task([] {});
        Get(idle);
        CHECK(!session->SnapshotState().busy);
        fault_enabled.store(true);
        if (stage == "traffic") { CHECK(ClientSend(client, "X")); }
        else if (stage == "wake") {
            auto wake = Task([connection] { CHECK(!connection->Wake()); });
            Get(wake);
        } else {
            OnOwner(loop, [&] { CHECK(!bridge->CloseNative(0, {})); });
        }
    }
    Get(rejected);
    StreamConnection::Stats physical{};
    // 故障调用已开始；后续 owner 命令证明调用已退出并读取真实物理收口。
    OnOwner(loop, [&] { physical = session->ConnStatsOwner(); });
    auto state = session->SnapshotState();
    std::cout << "FAULT stage=" << stage << " persistent=" << persistent
        << " attempts=" << attempts.load() << " failures=" << failures.load()
        << " physical_closes=" << physical.physical_closes
        << " busy=" << state.busy << " close_finished=" << state.close_finished
        << " drain=" << bridge->Drained() << " escaped=" << bool(owner_exception) << std::endl;
    CHECK(physical.physical_closes == 1); // 旧红：直接断言资源，不靠测试被超时杀死。
    CHECK(state.physical_closed && state.sealed);
    CHECK(!owner_exception && connection);
    CHECK(attempts.load() == 2);
    CHECK(failures.load() == (persistent ? 2 : 1));
    CHECK(!bridge->Drained());
    CHECK(!state.close_finished);
    CHECK(session->GetCounters().dispatch_crashes == static_cast<std::size_t>(failures.load()));
    if (stage == "traffic") { CHECK(physical.read_bytes == 1 && physical.traffic_notifications >= 1); }
    if (stage == "wake") { CHECK(session->GetCounters().wake_calls == 1); }
    if (persistent) {
        CHECK(!state.busy);
        CHECK(session->GetCounters().onclose_entered == 0);
        CHECK(!bridge->WaitDrained(Until(5ms)));
        // 连续 owner/Processer 屏障后仍无重试，也不制造业务完成。
        OnOwner(loop, [] {});
        auto progress = Task([] {});
        Get(progress);
        CHECK(attempts.load() == 2);
        CHECK(!bridge->Drained());
    } else {
        Get(close_future);
        CHECK(!bridge->WaitDrained(Until(5ms)));
        CHECK(!session->SnapshotState().close_finished);
        CHECK(session->GetCounters().onclose_returned == 0);
        CHECK(close_gate->Notify(bbt::coroutine::CoEventValue::From(true)) == 0);
        CHECK(bridge->WaitDrained(Until(5s)));
        state = session->SnapshotState();
        CHECK(!state.busy && state.close_finished);
        const auto counters = session->GetCounters();
        CHECK(counters.open_entered == 1);
        CHECK(counters.onclose_entered == 1 && counters.onclose_returned == 1);
        CHECK(counters.active_peak == 1 && counters.traffic_dispatched == 0);
        CHECK(counters.handler_exceptions == 0);
    }
    CHECK(bridge->GetStats().ops_submitted.load() == bridge->GetStats().ops_executed.load());
    OnOwner(loop, [&] {
        CHECK(!bridge->CloseNative(0, {}));
        physical = session->ConnStatsOwner();
    });
    CHECK(physical.physical_closes == 1);
    CHECK(physical.snapshots_taken == physical.snapshots_returned);
    CHECK(attempts.load() == 2);
    loop.RequestStop();
    CHECK(loop.Join());
    CHECK(loop.FailureCount() == 0);
    fault_owner.store(nullptr);
    client.Close();
    listener.Close();
    std::cout << "FINAL attempts=" << attempts.load() << " failures=" << failures.load()
        << " physical_closes=" << physical.physical_closes
        << " OnClose=" << session->GetCounters().onclose_returned
        << " active_peak=" << session->GetCounters().active_peak
        << " busy=" << session->SnapshotState().busy << " drain=" << bridge->Drained()
        << " ops=" << bridge->GetStats().ops_submitted.load() << "/"
        << bridge->GetStats().ops_executed.load() << " Join=true" << std::endl;
    // 持续拒绝只在本测试的进程边界释放 HOLD，不将其称作正常 drain。
}

#include "ProcessRuntimeTestMain.hpp"

static int RunDispatchCases(int argc, char** argv) {
    if (argc != 3) { return 2; }
    const std::string stage = argv[1], mode = argv[2];
    if ((stage != "initial" && stage != "traffic" && stage != "wake" && stage != "close")
        || (mode != "once" && mode != "persistent")) { return 2; }
    try {
        DispatchFailure(stage, mode == "persistent");
        std::cout << "CASE OK " << stage << " " << mode << " asserts=" << AssertCounter().load() << std::endl;
        return 0;
    } catch (const std::exception& error) {
        std::cout << "RUN FAILED: " << error.what() << std::endl;
        return 1;
    }
}

int main(int argc, char** argv) {
    // 测试进程退出边界：局部 RAII 收口 → 刷新输出 → _Exit，见
    // ProcessRuntimeTestMain.hpp 头注（静态析构竞态为已知限制，未修复）。
    return bbt_net_test::RunTestProcessExit([&] { return RunDispatchCases(argc, argv); });
}
