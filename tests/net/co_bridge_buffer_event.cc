// P2 回退契约回归（真实 TCP / 动态端口）：预交付会提前暂停读泵，不能以抑制
// terminal traffic 通知来修复。产品实现已完整退回基线；本文件验证原路径的
// take + Write + return 三命令、Wake 空快照、封门不启动业务以及 EOF 事件交付。
// fast_path_echo 是保留的历史用例标识，不再表示存在 fast path 或性能验收通过。
#define main CoBridgeBufferEventFixtureMain
#include "co_bridge_contract.cc"
#undef main

#include <sys/socket.h>
#include <string>

namespace {
constexpr std::size_t kFrameBytes = 1024;

void HoldExecutionThread(const std::atomic<bool>& until, bool& ok) {
    const auto deadline = Until(8s);
    while (!until.load()) {
        if (Clock::now() >= deadline) { ok = false; return; }
        timespec tick{0, 1000000};
        ::syscall(SYS_nanosleep, &tick, nullptr);
    }
    ok = true;
}

std::string Frame(std::size_t index) {
    std::string data(kFrameBytes, '\0');
    for (std::size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<char>((index * 31 + i) % 251);
    }
    return data;
}

bool ReadExact(NativeSocket& socket, std::size_t bytes, std::string& out) {
    out.clear();
    const auto deadline = Until(8s);
    while (out.size() < bytes) {
        pollfd pfd{socket.Fd(), POLLIN, 0};
        const int ready = ::poll(&pfd, 1, 50);
        CHECK(ready >= 0);
        CHECK(Clock::now() < deadline);
        if (!ready) { continue; }
        char buffer[2048];
        const auto count = ::recv(socket.Fd(), buffer,
            std::min<std::size_t>(sizeof(buffer), bytes - out.size()), 0);
        if (count <= 0) { return false; }
        out.append(buffer, static_cast<std::size_t>(count));
    }
    return true;
}

void WaitIdle(const std::shared_ptr<ConnectionSession>& session) {
    const auto deadline = Until(8s);
    while (session->SnapshotState().busy) {
        CHECK(Clock::now() < deadline);
        std::this_thread::yield();
    }
}

std::size_t OpsOf(const std::shared_ptr<ConnectionBridge>& bridge) {
    return bridge->GetStats().ops_submitted.load();
}

std::shared_ptr<ProbeHandler> EchoHandler() {
    auto handler = std::make_shared<ProbeHandler>();
    handler->on_traffic = [](ICoConnection& connection, bool wake) {
        CHECK(!wake);
        std::string data(connection.Peek());
        CHECK(!connection.Discard(data.size()));
        CHECK(!connection.Write(std::move(data), Until(3s)));
    };
    return handler;
}

// 原路径：空闲和连续到达均为 take + Write + return；不再接受 3→2 的性能声明。
void FallbackEcho() {
    ContractFixture fixture; fixture.Start();
    auto connection = fixture.Connect(EchoHandler());
    auto session = fixture.bridge->SessionOf(connection->Id());
    WaitCounters(*fixture.bridge, connection->Id(), [](const auto& c) { return c.open_entered == 1; });
    WaitIdle(session);
    for (const bool idle : {true, false}) {
        const auto ops_before = OpsOf(fixture.bridge);
        const auto events_before = session->GetCounters().traffic_dispatched;
        for (std::size_t i = 0; i < 16; ++i) {
            if (idle) { WaitIdle(session); }
            const auto frame = Frame(i + (idle ? 0 : 200));
            CHECK(ClientSend(fixture.clients.back(), frame));
            std::string echoed;
            CHECK(ReadExact(fixture.clients.back(), frame.size(), echoed));
            CHECK(echoed == frame);
        }
        WaitIdle(session);
        OnOwner(fixture.loop, [] {});
        const auto counters = session->GetCounters();
        const auto stats = OwnerStats(fixture.loop, session);
        const auto events = counters.traffic_dispatched - events_before;
        const auto ops = OpsOf(fixture.bridge) - ops_before;
        CHECK(events >= 16);
        CHECK(ops == 3 * events);
        CHECK(stats.snapshots_taken == stats.snapshots_returned);
        CHECK(counters.snapshot_return_errors == 0 && counters.active_peak == 1);
        Log(idle ? "fallback_echo_idle" : "fallback_echo_burst", connection->Id(),
            "events=" + std::to_string(events) + " ops=" + std::to_string(ops)
            + " taken=" + std::to_string(stats.snapshots_taken)
            + " returned=" + std::to_string(stats.snapshots_returned));
    }
    const auto stats = OwnerStats(fixture.loop, session);
    CHECK(stats.read_bytes == 32 * kFrameBytes);
    CHECK(stats.write_bytes_sent == stats.read_bytes);
    fixture.Finish();
}

// 反例能区分被拒绝的 terminal_only：Open 仍在途且积压为空时，FIN 的 Traffic
// 不能被 event_running_ 条件吞掉。用真实 EOF sticky 计数确认窗口，非 sleep 猜测。
void TerminalDuringOpen() {
    ContractFixture fixture; fixture.Start();
    auto gate = fixture.Gate();
    auto entered = gate->registered.get_future();
    auto handler = std::make_shared<ProbeHandler>();
    auto traffic = std::make_shared<std::atomic<std::size_t>>(0);
    handler->on_open = [gate](ICoConnection&) { gate->Wait(); };
    handler->on_traffic = [traffic](ICoConnection& connection, bool wake) {
        CHECK(!wake && connection.Peek().empty());
        ++*traffic;
    };
    auto connection = fixture.Connect(handler);
    auto session = fixture.bridge->SessionOf(connection->Id());
    Get(entered);
    CHECK(!fixture.clients.back().Shutdown(SHUT_WR));
    WaitCounters(*fixture.bridge, connection->Id(), [](const auto& c) { return c.peer_terminal_detected == 1; });
    CHECK(*traffic == 0);
    gate->Release();
    WaitCounters(*fixture.bridge, connection->Id(), [](const auto& c) { return c.onclose_returned == 1; });
    CHECK(*traffic == 1);
    CHECK(session->GetCounters().traffic_dispatched == 1);
    Log("terminal_during_open", connection->Id(), "empty_traffic=1 terminal=1");
    fixture.Finish();
}

void BusyFallback() {
    ContractFixture fixture; fixture.Start();
    auto gate = fixture.Gate();
    auto entered = gate->registered.get_future();
    auto handler = EchoHandler();
    handler->on_open = [gate](ICoConnection&) { gate->Wait(); };
    auto connection = fixture.Connect(handler);
    auto session = fixture.bridge->SessionOf(connection->Id());
    Get(entered);
    const auto before = OpsOf(fixture.bridge);
    const auto frame = Frame(101);
    CHECK(ClientSend(fixture.clients.back(), frame));
    const auto deadline = Until(8s);
    while (OwnerStats(fixture.loop, session).traffic_notifications < 1) { CHECK(Clock::now() < deadline); }
    CHECK(OwnerStats(fixture.loop, session).snapshots_taken == 0);
    gate->Release();
    std::string echoed;
    CHECK(ReadExact(fixture.clients.back(), frame.size(), echoed));
    CHECK(echoed == frame);
    WaitIdle(session);
    OnOwner(fixture.loop, [] {});
    CHECK(OpsOf(fixture.bridge) - before == 3);
    CHECK(session->GetCounters().traffic_dispatched == 1);
    const auto stats = OwnerStats(fixture.loop, session);
    CHECK(stats.snapshots_taken == 1 && stats.snapshots_returned == 1);
    fixture.Finish();
    TerminalDuringOpen();
}

// dispatcher 被原生门阻挡时，owner 只记 Traffic 位，不提前借快照。
void PendingEvent(bool seal) {
    ContractFixture fixture; fixture.Start();
    auto traffic = std::make_shared<std::atomic<std::size_t>>(0);
    auto wake_seen = std::make_shared<std::atomic<std::size_t>>(0);
    auto handler = std::make_shared<ProbeHandler>();
    handler->on_traffic = [traffic, wake_seen](ICoConnection& connection, bool wake) {
        ++*traffic;
        if (wake) {
            ++*wake_seen;
            CHECK(connection.Peek().empty());
            CHECK(!connection.Discard(0));
            return;
        }
        std::string data(connection.Peek());
        CHECK(!connection.Discard(data.size()));
        CHECK(!connection.Write(std::move(data), Until(3s)));
    };
    auto connection = fixture.Connect(handler);
    auto session = fixture.bridge->SessionOf(connection->Id());
    WaitCounters(*fixture.bridge, connection->Id(), [](const auto& c) { return c.open_entered == 1; });
    WaitIdle(session);
    struct Hold {
        std::atomic<bool> armed{false};
        std::atomic<bool> woke{false};
        std::atomic<bool> released{false};
    };
    auto hold_state = std::make_shared<Hold>();
    auto* loop = &fixture.loop;
    auto hold = Task([connection, hold_state, loop, seal] {
        bool ok = false;
        HoldExecutionThread(hold_state->armed, ok);
        CHECK(ok);
        if (!seal) {
            CHECK(!connection->Wake());
            OnOwner(*loop, [] {});
        }
        hold_state->woke.store(true);
        HoldExecutionThread(hold_state->released, ok);
        CHECK(ok);
    });
    CHECK(ClientSend(fixture.clients.back(), std::string("A")));
    const auto deadline = Until(8s);
    while (OwnerStats(fixture.loop, session).traffic_notifications < 1) { CHECK(Clock::now() < deadline); }
    CHECK(session->SnapshotState().busy);
    CHECK(OwnerStats(fixture.loop, session).snapshots_taken == 0);
    hold_state->armed.store(true);
    while (!hold_state->woke.load()) { CHECK(Clock::now() < deadline); }
    if (seal) { fixture.bridge->SealService(); }
    hold_state->released.store(true);
    Get(hold);
    WaitIdle(session);
    OnOwner(fixture.loop, [] {});
    const auto stats = OwnerStats(fixture.loop, session);
    CHECK(stats.snapshots_taken == 0 && stats.snapshots_returned == 0);
    CHECK(*traffic == (seal ? 0 : 1));
    CHECK(*wake_seen == (seal ? 0 : 1));
    CHECK(!session->SnapshotState().snap_lent);
    CHECK(session->GetCounters().snapshot_return_errors == 0);
    Log(seal ? "seal_no_predelivery" : "wake_no_predelivery", connection->Id(),
        "taken=0 returned=0 traffic=" + std::to_string(traffic->load()));
    if (!seal) {
        const auto frame = Frame(7);
        CHECK(ClientSend(fixture.clients.back(), frame));
        std::string echoed;
        CHECK(ReadExact(fixture.clients.back(), frame.size() + 1, echoed));
        CHECK(echoed == std::string("A") + frame);
        fixture.Finish();
        return;
    }
    // 强停不承诺业务 OnClose/drained；只按真实物理收口判据结束。
    OnOwner(fixture.loop, [&fixture] { CHECK(!fixture.bridge->StopServiceOwner()); });
    CHECK(session->SnapshotState().physical_closed);
    CHECK(fixture.bridge->GetStats().ops_submitted.load() == fixture.bridge->GetStats().ops_executed.load());
    for (auto& client : fixture.clients) { client.Close(); }
    fixture.loop.RequestStop();
    CHECK(fixture.loop.Join() && fixture.loop.FailureCount() == 0);
    CHECK(!fixture.bridge->DetachOwner());
}
} // namespace

int main(int argc, char** argv) {
    return bbt_net_test::RunTestProcessExit([&] {
        try {
            CHECK(argc == 2);
            const std::string name = argv[1];
            if (name == "fast_path_echo") { FallbackEcho(); }
            else if (name == "busy_fallback") { BusyFallback(); }
            else if (name == "wake_merge_discard") { PendingEvent(false); }
            else if (name == "seal_discard") { PendingEvent(true); }
            else { return 2; }
            std::cout << "CASE OK co_bridge_buffer_event " << name
                      << " mode=rollback_contract asserts=" << AssertCounter().load() << std::endl;
            return 0;
        } catch (const std::exception& error) {
            std::cerr << "RUN FAILED: " << error.what() << std::endl;
            return 1;
        }
    });
}
