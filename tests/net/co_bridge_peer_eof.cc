// peer FIN/RST 自动收口（真实 TCP 回环）：无显式 Close、无 Engine::Stop 下
// OnClose exactly-once、活跃连接计数归零、尾帧不丢、FIN vs RST reason 可区分。
//
// 复用 co_bridge_contract.cc 既有 fixture（WarmRuntime/ConnectTo/AcceptViaBridge/
// ProbeHandler/Until/CHECK/Log/OpenFdCount/OwnerStats/WaitCounters...），
// 同 co_bridge_lifetime.cc 的 include 范式，不复制测试框架。
//
// 执行域：产品修复（ConnectionBridge）在 RunEvent 返回后（含 AutoReturnSnapshot）
// 提交一次性 SealAndPhysicalCloseOwner；本测试只观测真实事件/计数，不用 sleep
// 推断时序，不显式 Close、不调用 Engine::Stop。
//
// case：
//   peer_eof_fin_auto_close                客户端 Shutdown(SHUT_WR) → 自动收口，
//                                          reason 空、计数归零、物理收口恰一次、
//                                          OnClose 在最后 OnTraffic 返回之后。
//   peer_eof_tail_frame_then_fin           客户端 Send(tail)+FIN → 业务先取到尾帧，
//                                          再自动收口（不丢尾帧）。
//   peer_eof_rst_reason                    SetLinger(0)+Close → RST → reason 为
//                                          connection_reset（system_category）。
//   peer_eof_fin_during_suspended_handler  handler 挂起期间 FIN：挂起期间物理收口
//                                          为 0，返回后恰一次 OnClose。
//   peer_eof_idempotent_after_auto_close   自动收口后重复 Close 幂等、无第二个
//                                          OnClose、DrainedSnapshot 为真。

#define main CoBridgePeerEofFixtureMain
#include "co_bridge_contract.cc"
#undef main

#include <sys/socket.h>

#include <bbt/infra/net/EventHandler.hpp>
#include <bbt/infra/net/detail/EventHandlerAdapter.hpp>

namespace {

using LiveCounter = std::shared_ptr<std::atomic<std::size_t>>;

// 公共 EventHandler 桩：各 case 用 std::function 注入；只回 Action::None，
// 不主动 Close（本测试的关键就是「业务不显式关闭」）。
struct ProbeEventHandler final : EventHandler {
    std::function<void(ICoConnection&)> open_hook;
    std::function<Action(ICoConnection&, bool)> traffic_hook;
    std::function<void(ICoConnection&, const std::error_code&)> close_hook;

    Action OnOpen(ICoConnection& connection) override {
        if (open_hook) { open_hook(connection); }
        return Action::None;
    }
    Action OnTraffic(ICoConnection& connection, bool wake) override {
        return traffic_hook ? traffic_hook(connection, wake) : Action::None;
    }
    void OnClose(ICoConnection& connection, const std::error_code& reason) override {
        if (close_hook) { close_hook(connection, reason); }
    }
};

// 事件顺序/原因观测：seq 只在真实执行点推进；reason 在 set_value 前写入，
// 经 promise/future 建立 happens-before，跨线程读不需要额外锁。
struct EofObservation {
    std::atomic<int> seq{0};
    std::atomic<int> last_traffic_seq{0};
    std::atomic<int> close_seq{0};
    std::error_code reason;
};

// 最小 fixture：EventLoop 值成员 + 真实 listener/client。析构守卫保证异常安全
// （先 RequestStop/Join 再释放资源，不留悬挂 owner）。
struct PeerEofCase {
    EventLoop loop{64};
    std::shared_ptr<ConnectionBridge> bridge;
    NativeSocket listener;
    Address bound;
    NativeSocket client;
    std::shared_ptr<ProbeEventHandler> handler;
    std::shared_ptr<CoTcpConnection> connection;
    std::shared_ptr<ConnectionSession> session;
    bool started{false};
    bool finished{false};

    void Start() {
        CHECK(loop.Start());
        started = true;
        std::error_code error;
        bridge = ConnectionBridge::Create(loop, BridgeOptions(), error);
        CHECK(bridge && !error);
        listener = OpenStream();
        bound = ListenOn(listener);
    }

    void Accept(std::shared_ptr<ICoConnectionHandler> handler_impl) {
        client = ConnectTo(bound);
        auto accept = Task([this, handler_impl] {
            connection = AcceptViaBridge(loop, *bridge, listener, handler_impl);
        });
        Get(accept);
        CHECK(connection);
        session = bridge->SessionOf(connection->Id());
        CHECK(session);
    }

    void Finish() {
        if (finished) { return; }
        finished = true;
        if (!started) { return; }
        loop.RequestStop();
        CHECK(loop.Join());
        CHECK(loop.FailureCount() == 0);
        client.Close();
        listener.Close();
        connection.reset();
        session.reset();
        handler.reset();
        bridge.reset();
        Log("peer_eof_finish", 0, "loop joined resources released");
    }

    ~PeerEofCase() {
        try { Finish(); } catch (...) {}
    }
};

// 等待活跃计数回落（真实观测 + 有限 deadline，不是 sleep 猜时序）。
void WaitLiveZero(const LiveCounter& live) {
    const auto deadline = Until(5s);
    while (live->load() != 0) {
        CHECK(Clock::now() < deadline);
        std::this_thread::sleep_for(1ms);
    }
}

} // namespace

// ===========================================================================
// T1：客户端半关闭(FIN) → 服务端自动收口，无显式 Close / 无 Engine::Stop
// ===========================================================================
// 主体函数：返回时 PeerEofCase（含 EventLoop/listener/client）已析构，
// 使外层可以做真实的 fd 回归基线断言。
static void RunFinAutoCloseBody() {
    PeerEofCase fixture;
    fixture.Start();
    const auto live = std::make_shared<std::atomic<std::size_t>>(0);
    const auto observation = std::make_shared<EofObservation>();
    auto opened = std::make_shared<std::promise<void>>();
    auto closed = std::make_shared<std::promise<void>>();

    fixture.handler = std::make_shared<ProbeEventHandler>();
    fixture.handler->open_hook = [live, opened](ICoConnection&) {
        live->fetch_add(1); // 模拟 ServerEngine 接纳时 live_connections +1
        opened->set_value();
    };
    fixture.handler->traffic_hook = [observation](ICoConnection& connection, bool wake) {
        CHECK(!wake);
        CHECK(bbt::coroutine::GetLocalCoroutineId() != 0);
        const auto view = connection.Peek();
        if (!view.empty()) { CHECK(!connection.Discard(view.size())); }
        observation->last_traffic_seq = ++observation->seq;
        return Action::None; // 无显式 Close：完全依赖对端 FIN 自动收口
    };
    fixture.handler->close_hook = [observation, closed](ICoConnection&, const std::error_code& reason) {
        observation->close_seq = ++observation->seq;
        observation->reason = reason;
        closed->set_value();
    };
    auto adapter = std::make_shared<EventHandlerAdapter>(fixture.handler, live);

    fixture.Accept(adapter);
    Get(opened->get_future());
    CHECK(live->load() == 1);

    // 干净 FIN（仅客户端半关闭；服务端不做任何 Close/Stop）。
    CHECK(!fixture.client.Shutdown(SHUT_WR));
    Get(closed->get_future()); // 有界等待：无收口时抛 assert@ 而非挂死

    CHECK(!observation->reason); // 干净 FIN → reason 空
    (void)WaitCounters(*fixture.bridge, fixture.connection->Id(),
        [](const ConnectionSession::Counters& c) { return c.onclose_returned == 1; });
    WaitLiveZero(live); // CountConnections 自然归零，无需 Stop
    const auto counters = fixture.session->GetCounters();
    CHECK(counters.onclose_entered == 1);
    CHECK(counters.onclose_returned == 1);
    CHECK(counters.active_peak == 1);
    CHECK(counters.handler_exceptions == 0);
    CHECK(fixture.session->SnapshotState().physical_closed);
    CHECK(fixture.session->SnapshotState().close_finished);
    const auto stats = OwnerStats(fixture.loop, fixture.session);
    CHECK(stats.physical_closes == 1);
    // OnClose 必须发生在最后一个 OnTraffic 返回之后（不丢尾帧/不抢跑）。
    CHECK(observation->close_seq > observation->last_traffic_seq);
    CHECK(fixture.bridge->WaitDrained(Until(8s)));
    CHECK(fixture.session->DrainedSnapshot());
    Log("peer_eof_fin_auto_close", fixture.connection->Id(),
        "physical_closes=1 onclose=1 live=0 reason=clean");

    fixture.Finish();
}

static void CaseFinAutoClose() {
    WarmRuntime();
    const auto fd_before = OpenFdCount();
    RunFinAutoCloseBody();
    const auto fd_after = OpenFdCount();
    Log("peer_eof_fd_baseline", 0, "before=" + std::to_string(fd_before)
        + " after=" + std::to_string(fd_after));
    // 被接纳 fd 与全部收口资源回落基线：无显式 Close、无 Engine::Stop 亦成立。
    CHECK(fd_after == fd_before);
}

// ===========================================================================
// T2：尾帧 + FIN → 业务先取到尾帧，再自动收口（不丢尾帧）
// ===========================================================================
static void CaseTailFrameThenFin() {
    WarmRuntime();
    PeerEofCase fixture;
    fixture.Start();
    const auto live = std::make_shared<std::atomic<std::size_t>>(1);
    const auto observation = std::make_shared<EofObservation>();
    const auto received = std::make_shared<std::string>();
    auto opened = std::make_shared<std::promise<void>>();
    auto tail_seen = std::make_shared<std::promise<void>>();
    auto closed = std::make_shared<std::promise<void>>();

    fixture.handler = std::make_shared<ProbeEventHandler>();
    fixture.handler->open_hook = [opened](ICoConnection&) { opened->set_value(); };
    fixture.handler->traffic_hook = [observation, received, tail_seen](ICoConnection& connection, bool wake) {
        CHECK(!wake);
        const auto view = connection.Peek();
        if (!view.empty()) {
            received->append(view.data(), view.size());
            CHECK(!connection.Discard(view.size()));
            if (*received == "tail") { tail_seen->set_value(); }
        }
        observation->last_traffic_seq = ++observation->seq;
        return Action::None;
    };
    fixture.handler->close_hook = [observation, closed](ICoConnection&, const std::error_code& reason) {
        observation->close_seq = ++observation->seq;
        observation->reason = reason;
        closed->set_value();
    };
    auto adapter = std::make_shared<EventHandlerAdapter>(fixture.handler, live);

    fixture.Accept(adapter);
    Get(opened->get_future());
    CHECK(ClientSend(fixture.client, "tail"));
    CHECK(!fixture.client.Shutdown(SHUT_WR)); // 尾帧与 FIN 紧邻到达
    Get(tail_seen->get_future());             // 尾帧在收口前被业务取到
    Get(closed->get_future());

    CHECK(*received == "tail"); // 尾帧不丢
    CHECK(!observation->reason);
    (void)WaitCounters(*fixture.bridge, fixture.connection->Id(),
        [](const ConnectionSession::Counters& c) { return c.onclose_returned == 1; });
    WaitLiveZero(live);
    CHECK(fixture.session->GetCounters().onclose_entered == 1);
    CHECK(observation->close_seq > observation->last_traffic_seq);
    CHECK(OwnerStats(fixture.loop, fixture.session).physical_closes == 1);
    Log("peer_eof_tail_frame_then_fin", fixture.connection->Id(),
        "tail_received=1 onclose=1 reason=clean");

    fixture.Finish();
}

// ===========================================================================
// T3：RST（SO_LINGER 0 + close）→ reason 为真实 errno，与干净 FIN 可区分
// ===========================================================================
static void CaseRstReason() {
    WarmRuntime();
    PeerEofCase fixture;
    fixture.Start();
    const auto live = std::make_shared<std::atomic<std::size_t>>(1);
    const auto observation = std::make_shared<EofObservation>();
    auto opened = std::make_shared<std::promise<void>>();
    auto closed = std::make_shared<std::promise<void>>();

    fixture.handler = std::make_shared<ProbeEventHandler>();
    fixture.handler->open_hook = [opened](ICoConnection&) { opened->set_value(); };
    fixture.handler->traffic_hook = [observation](ICoConnection& connection, bool wake) {
        CHECK(!wake);
        const auto view = connection.Peek();
        if (!view.empty()) { CHECK(!connection.Discard(view.size())); }
        observation->last_traffic_seq = ++observation->seq;
        return Action::None;
    };
    fixture.handler->close_hook = [observation, closed](ICoConnection&, const std::error_code& reason) {
        observation->close_seq = ++observation->seq;
        observation->reason = reason;
        closed->set_value();
    };
    auto adapter = std::make_shared<EventHandlerAdapter>(fixture.handler, live);

    fixture.Accept(adapter);
    Get(opened->get_future());

    // 中止式关闭：SO_LINGER {on=1, linger=0} → close 立即发 RST。
    CHECK(!fixture.client.SetLinger(0));
    CHECK(!fixture.client.Close());
    Get(closed->get_future());

    // 按实际 errno 判定，不硬编码平台数值：条件比较走 generic_category。
    CHECK(observation->reason == std::errc::connection_reset);
    CHECK(observation->reason.category() == std::system_category());
    (void)WaitCounters(*fixture.bridge, fixture.connection->Id(),
        [](const ConnectionSession::Counters& c) { return c.onclose_returned == 1; });
    WaitLiveZero(live);
    CHECK(fixture.session->GetCounters().onclose_entered == 1); // 无第二个 OnClose
    CHECK(OwnerStats(fixture.loop, fixture.session).physical_closes == 1);
    Log("peer_eof_rst_reason", fixture.connection->Id(),
        "reason=" + observation->reason.message() + " onclose=1");

    fixture.Finish();
}

// ===========================================================================
// T4：handler 挂起期间 FIN → 挂起期间物理收口为 0，返回后恰一次 OnClose
// ===========================================================================
static void CaseFinDuringSuspendedHandler() {
    WarmRuntime();
    PeerEofCase fixture;
    fixture.Start();
    const auto live = std::make_shared<std::atomic<std::size_t>>(1);
    const auto observation = std::make_shared<EofObservation>();
    const auto gate = std::make_shared<bbt::coroutine::sync::CoWaiter>();
    const auto gated = std::make_shared<std::atomic<bool>>(false);
    auto opened = std::make_shared<std::promise<void>>();
    auto traffic_seen = std::make_shared<std::promise<void>>();
    auto closed = std::make_shared<std::promise<void>>();

    fixture.handler = std::make_shared<ProbeEventHandler>();
    fixture.handler->open_hook = [opened](ICoConnection&) { opened->set_value(); };
    fixture.handler->traffic_hook = [observation, gate, gated, traffic_seen](
                                        ICoConnection& connection, bool wake) {
        if (wake) { return Action::None; }
        const auto view = connection.Peek();
        if (!view.empty()) {
            CHECK(!connection.Discard(view.size()));
            traffic_seen->set_value();
        }
        bool expected = false;
        if (gated->compare_exchange_strong(expected, true)) {
            bbt::coroutine::WaitOptions options;
            options.deadline = Until(30s);
            CHECK(gate->Wait(options) == bbt::coroutine::WaitStatus::Completed); // 挂起
        }
        observation->last_traffic_seq = ++observation->seq;
        return Action::None;
    };
    fixture.handler->close_hook = [observation, closed](ICoConnection&, const std::error_code& reason) {
        observation->close_seq = ++observation->seq;
        observation->reason = reason;
        closed->set_value();
    };
    auto adapter = std::make_shared<EventHandlerAdapter>(fixture.handler, live);

    fixture.Accept(adapter);
    Get(opened->get_future());
    CHECK(ClientSend(fixture.client, "FOO"));
    Get(traffic_seen->get_future()); // handler 已进入事件并挂起

    // 挂起期间对端 FIN 到达：物理收口必须仍为 0（不得压掉挂起中的业务事件）。
    CHECK(!fixture.client.Shutdown(SHUT_WR));
    for (int i = 0; i < 10; ++i) {
        CHECK(OwnerStats(fixture.loop, fixture.session).physical_closes == 0);
        CHECK(!fixture.session->SnapshotState().physical_closed);
        std::this_thread::sleep_for(10ms); // 给 owner 真实调度窗口，断言仍须成立
    }
    CHECK(fixture.session->GetCounters().onclose_entered == 0);

    gate->Notify(bbt::coroutine::CoEventValue::From(true)); // 放行挂起的 handler
    Get(closed->get_future());

    (void)WaitCounters(*fixture.bridge, fixture.connection->Id(),
        [](const ConnectionSession::Counters& c) { return c.onclose_returned == 1; });
    WaitLiveZero(live);
    CHECK(fixture.session->GetCounters().onclose_entered == 1);
    CHECK(fixture.session->GetCounters().active_peak == 1);
    CHECK(OwnerStats(fixture.loop, fixture.session).physical_closes == 1);
    CHECK(observation->close_seq > observation->last_traffic_seq); // handler 返回后才关
    CHECK(fixture.bridge->WaitDrained(Until(8s)));
    Log("peer_eof_fin_during_suspended_handler", fixture.connection->Id(),
        "suspended_physical_closes=0 after_return_onclose=1");

    fixture.Finish();
}

// ===========================================================================
// T5：自动收口后重复 Close 幂等、无第二个 OnClose、DrainedSnapshot 为真
// ===========================================================================
static void CaseIdempotentAfterAutoClose() {
    WarmRuntime();
    PeerEofCase fixture;
    fixture.Start();
    const auto live = std::make_shared<std::atomic<std::size_t>>(1);
    const auto observation = std::make_shared<EofObservation>();
    auto opened = std::make_shared<std::promise<void>>();
    auto closed = std::make_shared<std::promise<void>>();

    fixture.handler = std::make_shared<ProbeEventHandler>();
    fixture.handler->open_hook = [opened](ICoConnection&) { opened->set_value(); };
    fixture.handler->traffic_hook = [observation](ICoConnection& connection, bool wake) {
        CHECK(!wake);
        const auto view = connection.Peek();
        if (!view.empty()) { CHECK(!connection.Discard(view.size())); }
        observation->last_traffic_seq = ++observation->seq;
        return Action::None;
    };
    fixture.handler->close_hook = [observation, closed](ICoConnection&, const std::error_code& reason) {
        observation->close_seq = ++observation->seq;
        observation->reason = reason;
        closed->set_value();
    };
    auto adapter = std::make_shared<EventHandlerAdapter>(fixture.handler, live);

    fixture.Accept(adapter);
    Get(opened->get_future());
    CHECK(!fixture.client.Shutdown(SHUT_WR));
    Get(closed->get_future());
    CHECK(!observation->reason);

    // 已物理关闭后重复 Close（受管协程内合法）：幂等成功，不产生第二个 OnClose。
    const auto connection = fixture.connection;
    auto repeat = Task([connection] {
        CHECK(!connection->Close(CloseMode::Unblock, {}, Until(1s)));
        CHECK(!connection->Close(CloseMode::Block, {}, Until(1s)));
    });
    Get(repeat);

    (void)WaitCounters(*fixture.bridge, fixture.connection->Id(),
        [](const ConnectionSession::Counters& c) { return c.onclose_returned == 1; });
    WaitLiveZero(live);
    CHECK(fixture.session->GetCounters().onclose_entered == 1);
    CHECK(OwnerStats(fixture.loop, fixture.session).physical_closes == 1);
    CHECK(fixture.bridge->WaitDrained(Until(8s)));
    CHECK(fixture.session->DrainedSnapshot());
    Log("peer_eof_idempotent_after_auto_close", fixture.connection->Id(),
        "repeat_close=ok onclose=1 drained=1");

    fixture.Finish();
}

static int RunPeerEofCases(int argc, char** argv) {
    CHECK(argc == 2);
    const std::string case_name = argv[1];
    if (case_name == "peer_eof_fin_auto_close") { CaseFinAutoClose(); }
    else if (case_name == "peer_eof_tail_frame_then_fin") { CaseTailFrameThenFin(); }
    else if (case_name == "peer_eof_rst_reason") { CaseRstReason(); }
    else if (case_name == "peer_eof_fin_during_suspended_handler") { CaseFinDuringSuspendedHandler(); }
    else if (case_name == "peer_eof_idempotent_after_auto_close") { CaseIdempotentAfterAutoClose(); }
    else { std::cerr << "unknown case: " << case_name << '\n'; return 2; }
    std::cout << "CASE OK co_bridge_peer_eof " << case_name
              << " asserts=" << AssertCounter().load() << '\n';
    return 0;
}

int main(int argc, char** argv) {
    // 测试进程退出边界：局部 RAII 收口 → 刷新输出 → _Exit，见
    // ProcessRuntimeTestMain.hpp 头注（静态析构竞态为已知限制，未修复）。
    return bbt_net_test::RunTestProcessExit([&] { return RunPeerEofCases(argc, argv); });
}
