// F8 停止交错：真实 TCP，owner/单 Processer 握手，不靠 sleep 推断。
// 语义边界（与 review-95 F1 对齐）：本文件的 accepted/queued_inflight 只表示「命令已入队 sub owner」，
// 不是「业务开始授权」；业务是否开始只由产品侧第二道门（ConnectionBridge.cc:529-537）与写路径回执判定。
#define main CoBridgeFixtureMain
#include "co_bridge_contract.cc"
#undef main
#include <bbt/infra/net/Engine.hpp>
#include <sys/syscall.h>

struct NativeGate {
    std::promise<void> entered;
    std::atomic<bool> released{false};
    void Wait() {
        entered.set_value();
        const auto deadline = Until(8s);
        while (!released.load()) {
            CHECK(Clock::now() < deadline);
            timespec tick{0, 1000000};
            ::syscall(SYS_nanosleep, &tick, nullptr); // 绕过 coroutine Hook，占住真实执行线程。
        }
    }
};
static thread_local std::promise<void>* queued_notice = nullptr;
static thread_local bool stop_thread = false;
static std::promise<void>* stop_notice = nullptr;
static std::shared_ptr<NativeGate> handoff_gate;
static std::atomic<bool> intercept_handoff{false};
// 绑定标志：调用线程在发起某一条具体 Write 前置位，使被门闸的是「这条 Write 的 sub 提交」，
// 而不是「登记后到达该 sub 的第一条命令」（后者是自指计数，不能证明业务）。
static thread_local bool bind_write_submit = false;
static std::thread::id control_thread;

// F8 用例二：门闸停在「每 sub owner 命令」上。实测线程模型（ServerEngine.cc:289-309、
// ConnectionBridge.cc:428-436）:每个 sub 有独立 EventLoop 线程（网络 owner），而业务
// handler 由 runtime g_scheduler 的受管协程承载；WarmRuntime 固定 static_thread_num=1
// ⇒ 全进程只有一个 Processer。所以在 handler 里原生阻塞占住的是唯一 Processer，第二个
// sub 的事件协程根本无法开始（这正是用例二旧版 8.01s 门闸超时的原因），不能用来制造双
// sub 窗口。可定位的窗口是 owner 命令：两个 sub owner 是两条真实线程，可同时占住，
// 期间该 sub 的新命令只能排队，Processer 仍可推进两个 sub 的事件。
struct OwnerGate {
    std::atomic<bool> entered{false};
    std::atomic<bool> released{false};
    // LP 前在途业务 Write 被该 sub owner 接纳（Submit=Accepted，仅入队成功）的次数。
    // 入队 ≠ 业务开始授权：最终是否开始业务由产品第二道门/写路径回执判定。
    std::atomic<int> queued_inflight{0};
    std::atomic<int> held{0}; // 被绑定门闸实际占住的 owner 命令数（应为 1）
};
static std::mutex sub_loops_mu;
static std::map<EventLoop*, std::shared_ptr<OwnerGate>> sub_loops;
static std::atomic<bool> register_subs{false};
static std::atomic<bool> lp_published{false};
static std::atomic<int> post_lp_owner_access{0};
static std::thread::id stop_owner_thread;
extern "C" SubmitStatus RealSubmitWithError(EventLoop*, std::function<void()>, std::error_code&)
    asm("__real__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEERSt10error_code");
extern "C" SubmitStatus WrapSubmitWithError(EventLoop*, std::function<void()>, std::error_code&)
    asm("__wrap__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEERSt10error_code");
extern "C" SubmitStatus WrapSubmit(EventLoop*, std::function<void()>)
    asm("__wrap__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEE");
extern "C" SubmitStatus WrapSubmit(EventLoop* loop, std::function<void()> command) {
    // 旧入口也只走一次测试观测，不经单参数真实入口再次委托新重载。
    std::error_code ignored;
    return WrapSubmitWithError(loop, std::move(command), ignored);
}
extern "C" SubmitStatus WrapSubmitWithError(EventLoop* loop, std::function<void()> command,
    std::error_code& error) {
    if (intercept_handoff.load() && std::this_thread::get_id() != control_thread
        && bbt::coroutine::GetLocalCoroutineId() == 0 && !loop->IsOwner()
        && intercept_handoff.exchange(false)) {
        auto gate = handoff_gate;
        command = [gate, command = std::move(command)] { gate->Wait(); command(); };
    }
    // F8 用例二：登记 sub owner（main owner 的 handoff 目标），并按绑定标志门闸一条具体 Write 命令。
    std::shared_ptr<OwnerGate> owner_gate;
    bool known_sub = false;
    {
        std::lock_guard<std::mutex> guard(sub_loops_mu);
        if (register_subs.load() && !loop->IsOwner() && bbt::coroutine::GetLocalCoroutineId() == 0) {
            sub_loops.emplace(loop, std::make_shared<OwnerGate>());
        }
        const auto it = sub_loops.find(loop);
        known_sub = (it != sub_loops.end());
        // 只有「紧邻某条 Write 前置位」的提交才被门闸，从而把门闸绑定到可识别的 Write 操作。
        if (known_sub && bind_write_submit && !loop->IsOwner()) { owner_gate = it->second; }
    }
    // 封门（LP）后仍触达 sub owner 的业务命令 —— 预期 0；Stop 自身的控制命令不计。
    if (known_sub && lp_published.load() && std::this_thread::get_id() != stop_owner_thread) {
        ++post_lp_owner_access;
    }
    bool gated = false;
    if (owner_gate && owner_gate->held.fetch_add(1) == 0) {
        gated = true;
        command = [owner_gate, command = std::move(command)] {
            owner_gate->entered.store(true);
            const auto deadline = Clock::now() + 20s;
            while (!owner_gate->released.load()) {
                CHECK(Clock::now() < deadline);
                timespec tick{0, 1000000};
                ::syscall(SYS_nanosleep, &tick, nullptr); // 占住该 sub 的 owner 线程（非 Processer）
            }
            command();
        };
    }
    const auto status = RealSubmitWithError(loop, std::move(command), error);
    // 回执绑定：仅当这条被门闸的命令正是那条 Write 时，才记为该 Write 的「入队」计数。
    if (status == SubmitStatus::Accepted && gated) { owner_gate->queued_inflight.fetch_add(1); }
    if (status == SubmitStatus::Accepted && queued_notice) {
        std::exchange(queued_notice, nullptr)->set_value();
    }
    // Stop 首条资源命令投递：发生于 stopping_ 发布之后；不改生产路径。
    if (status == SubmitStatus::Accepted && stop_thread && stop_notice) {
        std::exchange(stop_notice, nullptr)->set_value();
    }
    return status;
}
static std::atomic<int> create_calls{0};
extern "C" std::shared_ptr<StreamConnection> RealCreate(EventLoop&, NativeSocket&&,
    const NormalizedOptions&, StreamConnection::Callbacks, std::error_code&)
    asm("__real__ZN3bbt5infra3net16StreamConnection6CreateERNS1_9EventLoopEONS1_12NativeSocketERKNS1_17NormalizedOptionsENS2_9CallbacksERSt10error_code");
extern "C" std::shared_ptr<StreamConnection> WrapCreate(EventLoop&, NativeSocket&&,
    const NormalizedOptions&, StreamConnection::Callbacks, std::error_code&)
    asm("__wrap__ZN3bbt5infra3net16StreamConnection6CreateERNS1_9EventLoopEONS1_12NativeSocketERKNS1_17NormalizedOptionsENS2_9CallbacksERSt10error_code");
extern "C" std::shared_ptr<StreamConnection> WrapCreate(EventLoop& loop, NativeSocket&& socket,
    const NormalizedOptions& options, StreamConnection::Callbacks callbacks, std::error_code& error) {
    ++create_calls;
    return RealCreate(loop, std::move(socket), options, std::move(callbacks), error);
}

// F8 用例一：借出快照后、业务开始授权前的确定性窗口。仅本测试二进制 wrap 真实
// TakeSnapshot（测试私有 linkwrap，不新增生产注入接口）。
static std::atomic<bool> take_hold_armed{false};
static std::shared_ptr<NativeGate> take_gate;
// 返回值类型是 StreamConnection 的私有成员类型，不能在此命名；以 decltype 取得同一类型。
using TakeSnapshotResult = decltype(std::declval<StreamConnection&>().TakeSnapshot(
    std::declval<std::error_code&>()));
extern "C" TakeSnapshotResult RealTakeSnapshot(StreamConnection&, std::error_code&)
    asm("__real__ZN3bbt5infra3net16StreamConnection12TakeSnapshotERSt10error_code");
extern "C" TakeSnapshotResult WrapTakeSnapshot(StreamConnection&, std::error_code&)
    asm("__wrap__ZN3bbt5infra3net16StreamConnection12TakeSnapshotERSt10error_code");
extern "C" TakeSnapshotResult WrapTakeSnapshot(StreamConnection& self, std::error_code& error) {
    auto snapshot = RealTakeSnapshot(self, error);
    if (take_hold_armed.exchange(false)) {
        auto gate = take_gate;
        // 快照由 dispatcher 协程在 runtime Processer 上的受管等待里借出（ConnectionBridge.cc:527-537
        // 的第二道门之前）；这里原生阻塞占住的是 Processer 线程，不是 sub owner，也不是
        // coroutine 协程让出 —— 只制造「受管快照挂起」窗口，等控制线程在业务调用前封门。
        if (gate) { gate->Wait(); }
    }
    return snapshot;
}

static void Eof(NativeSocket& client) {
    pollfd fd{client.Fd(), POLLIN, 0};
    CHECK(::poll(&fd, 1, 2000) == 1);
    char byte{};
    CHECK(::recv(fd.fd, &byte, 1, MSG_DONTWAIT) == 0);
}
static void FinishStopped(ContractFixture& fixture) {
    OnOwner(fixture.loop, [&fixture] { CHECK(!fixture.bridge->StopServiceOwner()); });
    for (auto& client : fixture.clients) { Eof(client); }
    const auto deadline = Until(5s);
    for (const auto& connection : fixture.connections) {
        auto session = fixture.bridge->SessionOf(connection->Id());
        while (session->SnapshotState().busy) { CHECK(Clock::now() < deadline); std::this_thread::yield(); }
    }
    OnOwner(fixture.loop, [] {});
    CHECK(fixture.bridge->GetStats().ops_submitted.load() == fixture.bridge->GetStats().ops_executed.load());
    fixture.loop.RequestStop(); CHECK(fixture.loop.Join());
    CHECK(fixture.loop.FailureCount() == 0);
    CHECK(!fixture.bridge->DetachOwner());
}
static void QueuedWrite() {
    ContractFixture fixture; fixture.Start();
    auto handler = std::make_shared<ProbeHandler>();
    auto connection = fixture.Connect(handler);
    auto session = fixture.bridge->SessionOf(connection->Id());
    WaitCounters(*fixture.bridge, connection->Id(), [](const auto& c) { return c.open_entered == 1; });
    auto gate = std::make_shared<NativeGate>();
    auto entering = gate->entered.get_future();
    CHECK(fixture.loop.Submit([gate] { gate->Wait(); }) == SubmitStatus::Accepted);
    Get(entering);
    auto notice = std::make_shared<std::promise<void>>(); auto queued = notice->get_future();
    auto error = std::make_shared<std::error_code>();
    auto write = Task([connection, notice, error] {
        queued_notice = notice.get();
        *error = connection->Write("MUST-NOT-WRITE", Until(5s));
        queued_notice = nullptr;
    });
    Get(queued); // 实际 Submit=Accepted（仅入队），owner 尚被握手阻塞；Write 未取得开始授权。
    fixture.bridge->SealService(); fixture.bridge->SealService();
    gate->released = true;
    Get(write); // 必须收到取消完成，不能丢 waiter。
    const auto stats = OwnerStats(fixture.loop, session);
    FinishStopped(fixture);
    Log("queued_write", 0, "error=" + std::to_string(error->value())
        + " accepted=" + std::to_string(stats.writes_accepted)
        + " sent=" + std::to_string(stats.write_bytes_sent));
    CHECK(*error == std::errc::operation_canceled);
    CHECK(stats.writes_accepted == 0 && stats.write_bytes_sent == 0);
}
static void PendingEvent(bool traffic) {
    ContractFixture fixture; fixture.Start();
    auto handler = std::make_shared<ProbeHandler>();
    std::shared_ptr<CoTcpConnection> connection;
    if (traffic) {
        connection = fixture.Connect(handler);
        WaitCounters(*fixture.bridge, connection->Id(), [](const auto& c) { return c.open_entered == 1; });
        Get(Task([] {}));
    }
    auto gate = std::make_shared<NativeGate>(); auto entering = gate->entered.get_future();
    auto blocked = Task([gate] { gate->Wait(); }); Get(entering);
    if (!traffic) {
        fixture.clients.push_back(ConnectTo(fixture.bound));
        OnOwner(fixture.loop, [&fixture, handler, &connection] {
            std::error_code error; Address peer;
            auto socket = fixture.listener.Accept(peer, error); CHECK(!error);
            connection = fixture.bridge->Adopt(std::move(socket), peer, handler, error);
            CHECK(connection && !error);
        });
        fixture.connections.push_back(connection);
    } else {
        // Wake 的 owner 到达通知是确定性业务 Traffic 事件；真实数据 readiness 另以计数握手。
        CHECK(ClientSend(fixture.clients.back(), "TRAFFIC"));
        auto session = fixture.bridge->SessionOf(connection->Id());
        const auto deadline = Until(5s);
        while (OwnerStats(fixture.loop, session).traffic_notifications == 0) { CHECK(Clock::now() < deadline); }
    }
    fixture.bridge->SealService(); // 不物理 Close，不借 session Stop 来压事件。
    gate->released = true; Get(blocked); Get(Task([] {}));
    auto session = fixture.bridge->SessionOf(connection->Id());
    const auto deadline = Until(5s);
    while (session->SnapshotState().busy) { CHECK(Clock::now() < deadline); std::this_thread::yield(); }
    const auto c = session->GetCounters();
    FinishStopped(fixture);
    CHECK(c.open_entered == (traffic ? 1u : 0u));
    CHECK(c.traffic_dispatched == 0 && c.onclose_entered == 0);
    Log("pending_event", 0, "traffic=" + std::to_string(traffic) + " open=" + std::to_string(c.open_entered)
        + " dispatched=" + std::to_string(c.traffic_dispatched));
}
static void EngineHandoff() {
    WarmRuntime(); control_thread = std::this_thread::get_id();
    struct Handler : EventHandler { std::atomic<int> opens{0};
        Action OnOpen(ICoConnection&) override { ++opens; return Action::None; } };
    auto handler = std::make_shared<Handler>(); Engine engine(handler);
    std::error_code error; auto address = Address::Ip("127.0.0.1", 0, error); CHECK(!error);
    Options options; options.num_event_loop = 1;
    CHECK(!engine.Run(address, options));
    std::vector<Address> bound; CHECK(!engine.LocalAddresses(bound)); CHECK(bound.size() == 1);
    handoff_gate = std::make_shared<NativeGate>(); auto entering = handoff_gate->entered.get_future();
    intercept_handoff = true;
    auto client = ConnectTo(bound.front()); Get(entering);
    // owner 已取出 handoff，但原闭包尚未执行，Adopt 尚未获得授权。
    std::promise<void> notice; auto stopping = notice.get_future(); stop_notice = &notice;
    auto stopped = std::async(std::launch::async, [&engine] { stop_thread = true; return engine.Stop(); });
    Get(stopping); handoff_gate->released = true;
    CHECK(stopped.wait_for(8s) == std::future_status::ready); CHECK(!stopped.get());
    Eof(client); Get(Task([] {}));
    Log("engine_create", 0, "create_calls=" + std::to_string(create_calls.load()));
    CHECK(create_calls == 0); CHECK(handler->opens == 0); CHECK(engine.CountConnections() == 0);
    Log("engine_handoff", 0, "open=0 eof=1");
    handoff_gate.reset();
}
// F8 用例一：Traffic 已借快照 → 挂起 → 最终开始授权前封门 → 恢复后业务不启动且快照真实归还。
static void TrafficTakeThenSeal() {
    ContractFixture fixture; fixture.Start();
    auto traffic_calls = std::make_shared<std::atomic<int>>(0);
    auto handler = std::make_shared<ProbeHandler>();
    handler->on_traffic = [traffic_calls](ICoConnection&, bool) { traffic_calls->fetch_add(1); };
    auto connection = fixture.Connect(handler);
    auto session = fixture.bridge->SessionOf(connection->Id());
    WaitCounters(*fixture.bridge, connection->Id(), [](const auto& c) { return c.open_entered == 1; });
    // 真实数据 → 非 Wake 事件 → PrepareEventSnapshot 在 owner 内借出快照。
    take_gate = std::make_shared<NativeGate>();
    auto entered = take_gate->entered.get_future();
    take_hold_armed = true;
    CHECK(ClientSend(fixture.clients.back(), "TAKE-THEN-SEAL"));
    Get(entered); // 快照已借出，事件协程停在业务调用之前
    fixture.bridge->SealService(); // 业务开始授权门在业务调用前关闭
    const auto during = session->SnapshotState();
    take_gate->released = true;
    const auto deadline = Until(8s);
    while (session->SnapshotState().busy) { CHECK(Clock::now() < deadline); std::this_thread::yield(); }
    const auto counters = session->GetCounters();
    const auto stats = OwnerStats(fixture.loop, session);
    const auto after = session->SnapshotState();
    FinishStopped(fixture);
    Log("traffic_take_then_seal", 0, "dispatched=" + std::to_string(counters.traffic_dispatched)
        + " handler_calls=" + std::to_string(traffic_calls->load())
        + " taken=" + std::to_string(stats.snapshots_taken)
        + " returned=" + std::to_string(stats.snapshots_returned)
        + " return_errors=" + std::to_string(counters.snapshot_return_errors));
    CHECK(counters.open_entered == 1);
    CHECK(during.busy);                        // 挂起窗口真实存在：事件在途未完成
    CHECK(counters.traffic_dispatched == 0);   // 封门后不开始业务
    CHECK(traffic_calls->load() == 0);
    CHECK(stats.snapshots_taken == 1 && stats.snapshots_returned == 1); // 快照真实归还
    CHECK(counters.snapshot_return_errors == 0);
    CHECK(!after.snap_lent && !after.snap_return_pending && !after.busy);
}

// F8 用例二：多 sub 逐桥封门期间的确定性交错。门闸停在每 sub owner 命令上（两 sub owner
// 是两条真实 owner 线程，可同时占住），不在业务 handler 内阻塞唯一 Processer。
class InterleaveHandler final : public EventHandler {
public:
    Action OnOpen(ICoConnection& connection) override {
        std::lock_guard<std::mutex> guard(mu_);
        ++opens_[Id(connection)];
        // 归属按对象指针记录：Id 相同（同号）也必须是两条不同的连接。
        if (std::find(order_.begin(), order_.end(), &connection) == order_.end()) {
            order_.push_back(&connection);
        }
        return Action::None;
    }

    std::size_t open_count() const {
        std::lock_guard<std::mutex> guard(mu_);
        std::size_t total = 0;
        for (const auto& entry : opens_) { total += entry.second; }
        return total;
    }
    ICoConnection* connection_at(std::size_t index) const {
        std::lock_guard<std::mutex> guard(mu_);
        if (index >= order_.size()) { return nullptr; }
        return order_[index];
    }

private:
    static std::size_t Id(ICoConnection& connection) {
        const auto* tcp = dynamic_cast<const CoTcpConnection*>(&connection);
        CHECK(tcp != nullptr);
        return tcp->Id();
    }
    mutable std::mutex mu_;
    std::vector<ICoConnection*> order_;
    std::map<std::size_t, std::size_t> opens_;
};

static void WaitForGate(const std::function<bool()>& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = Clock::now() + timeout;
    while (!predicate()) {
        CHECK(Clock::now() < deadline);
        std::this_thread::sleep_for(1ms);
    }
}
static std::shared_ptr<OwnerGate> GateOf(EventLoop* loop) {
    std::lock_guard<std::mutex> guard(sub_loops_mu);
    const auto it = sub_loops.find(loop);
    CHECK(it != sub_loops.end());
    return it->second;
}
static std::vector<EventLoop*> SubLoops() {
    std::lock_guard<std::mutex> guard(sub_loops_mu);
    std::vector<EventLoop*> loops;
    for (const auto& entry : sub_loops) { loops.push_back(entry.first); }
    return loops;
}
// 收尾只要求物理收口：在途授权写可能已真实写出字节，不能假定 EOF 前无数据。
static void DrainToEof(NativeSocket& client) {
    pollfd fd{client.Fd(), POLLIN, 0};
    const auto deadline = Until(8s);
    for (;;) {
        const auto ready = ::poll(&fd, 1, 200);
        CHECK(Clock::now() < deadline);
        if (ready == 0) { continue; }
        char buffer[256];
        const auto count = ::recv(fd.fd, buffer, sizeof(buffer), 0);
        if (count == 0) { return; }          // 物理 EOF
        if (count < 0 && errno == ECONNRESET) { return; } // 被强制收口的一端发 RST：同属物理已关闭
        CHECK(count > 0);
    }
}
// 负对照专用收尾：数据可能已真实写出，故用 DrainToEof 而非 Eof。
static void NegctlFinish(ContractFixture& fixture) {
    OnOwner(fixture.loop, [&fixture] { CHECK(!fixture.bridge->StopServiceOwner()); });
    for (auto& client : fixture.clients) { DrainToEof(client); }
    const auto deadline = Until(5s);
    for (const auto& connection : fixture.connections) {
        auto session = fixture.bridge->SessionOf(connection->Id());
        while (session->SnapshotState().busy) { CHECK(Clock::now() < deadline); std::this_thread::yield(); }
    }
    OnOwner(fixture.loop, [] {});
    CHECK(fixture.bridge->GetStats().ops_submitted.load() == fixture.bridge->GetStats().ops_executed.load());
    fixture.loop.RequestStop(); CHECK(fixture.loop.Join());
    CHECK(fixture.loop.FailureCount() == 0);
    CHECK(!fixture.bridge->DetachOwner());
}

// 负对照一：二次门判别力。窗口与 TrafficTakeThenSeal 相同，但**跳过测试自身的 SealService()**，
// 显式模拟「第二道门未关闭 / 缺开始授权」边界。边界说明：本对照不改产品，也不代表产品第二道门被删；
// 它只证明正向断言 traffic_dispatched==0 对「封门与否」具判别力（门未关 ⇒ dispatched==1）。
static void NegctlUnsealedTakeThenSeal() {
    ContractFixture fixture; fixture.Start();
    auto calls = std::make_shared<std::atomic<int>>(0);
    auto handler = std::make_shared<ProbeHandler>();
    handler->on_traffic = [calls](ICoConnection&, bool) { calls->fetch_add(1); };
    auto connection = fixture.Connect(handler);
    auto session = fixture.bridge->SessionOf(connection->Id());
    WaitCounters(*fixture.bridge, connection->Id(), [](const auto& c) { return c.open_entered == 1; });
    take_gate = std::make_shared<NativeGate>();
    auto entered = take_gate->entered.get_future();
    take_hold_armed = true;
    CHECK(ClientSend(fixture.clients.back(), "NEGCTL-TAKE"));
    Get(entered);
    // 负对照边界：不调用 SealService()。
    take_gate->released = true;
    const auto deadline = Until(8s);
    while (session->SnapshotState().busy) { CHECK(Clock::now() < deadline); std::this_thread::yield(); }
    const auto counters = session->GetCounters();
    Log("negctl_unsealed_take_then_seal", 0, "dispatched=" + std::to_string(counters.traffic_dispatched)
        + " handler_calls=" + std::to_string(calls->load()));
    CHECK(counters.traffic_dispatched == 1);   // 门未关 ⇒ 业务启动（正向断言因此具判别力）
    CHECK(calls->load() >= 1);
    fixture.bridge->SealService();
    NegctlFinish(fixture);
}
// 负对照二：排队 Write 入口门判别力。窗口与 QueuedWrite 相同，但**跳过测试自身的 SealService()**，
// 显式模拟「排队写入口门未关闭 / 缺授权」边界。边界说明：本对照不改产品，不代表产品 Submit 门被删；
// 它只证明正向断言 error==operation_canceled 对「封门与否」具判别力（门未关 ⇒ 写正常完成）。
static void NegctlUnsealedQueuedWrite() {
    ContractFixture fixture; fixture.Start();
    auto handler = std::make_shared<ProbeHandler>();
    auto connection = fixture.Connect(handler);
    auto session = fixture.bridge->SessionOf(connection->Id());
    WaitCounters(*fixture.bridge, connection->Id(), [](const auto& c) { return c.open_entered == 1; });
    auto gate = std::make_shared<NativeGate>();
    auto entering = gate->entered.get_future();
    CHECK(fixture.loop.Submit([gate] { gate->Wait(); }) == SubmitStatus::Accepted);
    Get(entering);
    auto notice = std::make_shared<std::promise<void>>(); auto queued = notice->get_future();
    auto error = std::make_shared<std::error_code>();
    auto write = Task([connection, notice, error] {
        queued_notice = notice.get();
        *error = connection->Write("NEGCTL-WRITE", Until(5s));
        queued_notice = nullptr;
    });
    Get(queued);   // Submit=Accepted（仅入队），owner 尚被握手阻塞
    // 负对照边界：不调用 SealService()。
    gate->released = true;
    Get(write);
    const auto stats = OwnerStats(fixture.loop, session);
    Log("negctl_unsealed_queued_write", 0, "error=" + std::to_string(error->value())
        + " accepted=" + std::to_string(stats.writes_accepted)
        + " sent=" + std::to_string(stats.write_bytes_sent));
    CHECK(*error != std::errc::operation_canceled); // 门未关 ⇒ 未被拒绝
    CHECK(stats.write_bytes_sent > 0);
    fixture.bridge->SealService();
    NegctlFinish(fixture);
}

static void EngineMultiSubSeal() {
    WarmRuntime(); control_thread = std::this_thread::get_id();
    auto handler = std::make_shared<InterleaveHandler>();
    Engine engine(handler);
    std::error_code error;
    auto address = Address::Ip("127.0.0.1", 0, error); CHECK(!error);
    Options options; options.num_event_loop = 2;
    CHECK(!engine.Run(address, options));
    std::vector<Address> bound; CHECK(!engine.LocalAddresses(bound)); CHECK(bound.size() == 1);
    register_subs = true;
    auto client_a = ConnectTo(bound.front());
    WaitForGate([&handler] { return handler->open_count() == 1; }, 8s);
    auto client_b = ConnectTo(bound.front());
    WaitForGate([&handler] { return handler->open_count() == 2; }, 8s);
    register_subs = false;
    const auto loops = SubLoops();
    CHECK(loops.size() == 2); // 白盒：两条连接分别落在两个 sub owner（最小连接数 LB）
    auto* target_a = handler->connection_at(0);
    auto* target_b = handler->connection_at(1);
    CHECK(target_a != nullptr && target_b != nullptr);
    CHECK(target_a != target_b);
    // ① LP 前的在途业务 Write：两条 Write 在各自 sub owner 被判据（Submit=Accepted，仅入队），
    //    并被绑定门闸停在 owner 命令上。入队 ≠ 业务开始授权；授权结论只由 ②/③ 的拒绝回执支撑。
    auto error_a = std::make_shared<std::error_code>();
    auto error_b = std::make_shared<std::error_code>();
    auto notice_a = std::make_shared<std::promise<void>>(); auto queued_a = notice_a->get_future();
    auto write_a = Task([target_a, error_a, notice_a] {
        queued_notice = notice_a.get();
        bind_write_submit = true;                  // 绑定这条 Write 的 sub 提交
        *error_a = target_a->Write("PRE-LP-A", Clock::now() + 30s);
        bind_write_submit = false;
        queued_notice = nullptr;
    });
    Get(queued_a);
    auto notice_b = std::make_shared<std::promise<void>>(); auto queued_b = notice_b->get_future();
    auto write_b = Task([target_b, error_b, notice_b] {
        queued_notice = notice_b.get();
        bind_write_submit = true;                  // 绑定这条 Write 的 sub 提交
        *error_b = target_b->Write("PRE-LP-B", Clock::now() + 30s);
        bind_write_submit = false;
        queued_notice = nullptr;
    });
    Get(queued_b);
    auto both_entered = [loops] {
        for (auto* loop : loops) { if (!GateOf(loop)->entered.load()) { return false; } }
        return true;
    };
    WaitForGate(both_entered, 8s);   // 两个 sub owner 同时被占住（两条真实线程）
    for (auto* loop : loops) {
        CHECK(GateOf(loop)->held.load() == 1);          // 恰被绑定门闸占住 1 条命令
        CHECK(GateOf(loop)->queued_inflight.load() == 1); // 该 sub 恰 1 条在途 Write 入队
    }
    // ② 控制线程 Stop：逐桥封门 → 发布 LP（stop_notice 为 LP 之后首条资源命令投递）。
    std::promise<void> notice_lp; auto stopping = notice_lp.get_future(); stop_notice = &notice_lp;
    auto stopped = std::async(std::launch::async, [&engine] {
        stop_owner_thread = std::this_thread::get_id();
        stop_thread = true;
        return engine.Stop();
    });
    Get(stopping);
    lp_published = true;
    // ③ LP 后未授权任务拒绝：submit 门在触达 owner 之前收口；这是对入口门的真实拒绝回执。
    auto post_error = std::make_shared<std::error_code>();
    auto post_write = Task([target_b, post_error] {
        *post_error = target_b->Write("POST-LP", Clock::now() + 5s);
    });
    Get(post_write);
    CHECK(*post_error == std::errc::operation_canceled);
    CHECK(post_lp_owner_access.load() == 0);         // 无业务命令触达 sub owner
    CHECK(handler->open_count() == 2);
    // ④ 放行：LP 前已入队的在途命令仍被允许推进并结算（不丢 waiter），Stop 正常收口。
    //    write_a/write_b 的最终回执（见 Log 与下方 CHECK）才是「是否开始业务」的判据。
    for (auto* loop : loops) { GateOf(loop)->released.store(true); }
    CHECK(stopped.wait_for(20s) == std::future_status::ready);
    CHECK(!stopped.get());
    Get(write_a); Get(write_b);
    Log("engine_multisub_seal", 0, "queued_inflight_a=" + std::to_string(GateOf(loops[0])->queued_inflight.load())
        + " queued_inflight_b=" + std::to_string(GateOf(loops[1])->queued_inflight.load())
        + " owner_access_after_lp=" + std::to_string(post_lp_owner_access.load())
        + " opens=" + std::to_string(handler->open_count())
        + " write_a=" + std::to_string(error_a->value())
        + " write_b=" + std::to_string(error_b->value()));
    CHECK(error_a->value() == static_cast<int>(std::errc::operation_canceled)); // 在途写结算为取消
    CHECK(error_b->value() == static_cast<int>(std::errc::operation_canceled));
    CHECK(handler->open_count() == 2);
    CHECK(engine.CountConnections() == 0);
    // 收口判据取网络层（Stop rc=0 = NetworkCloseReady）+ TCP 侧物理 EOF/RST，不用业务 OnClose 充数。
    DrainToEof(client_a); DrainToEof(client_b);
    stop_notice = nullptr; stop_thread = false;
}

#include "ProcessRuntimeTestMain.hpp"
int main(int argc, char** argv) {
    return bbt_net_test::RunTestProcessExit([&] {
        try {
            CHECK(argc == 2); const std::string name = argv[1];
            if (name == "queued_write") { QueuedWrite(); }
            else if (name == "pending_open") { PendingEvent(false); }
            else if (name == "pending_traffic") { PendingEvent(true); }
            else if (name == "engine_handoff") { EngineHandoff(); }
            else if (name == "traffic_take_then_seal") { TrafficTakeThenSeal(); }
            else if (name == "engine_multisub_seal_interleave") { EngineMultiSubSeal(); }
            else if (name == "negctl_unsealed_take_then_seal") { NegctlUnsealedTakeThenSeal(); }
            else if (name == "negctl_unsealed_queued_write") { NegctlUnsealedQueuedWrite(); }
            else { return 2; }
            std::cout << "CASE OK service_gate " << name << '\n'; return 0;
        } catch (const std::exception& error) { std::cerr << "RUN FAILED: " << error.what() << '\n'; return 1; }
    });
}
