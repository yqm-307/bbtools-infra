// P3-c1 owner 安全脱离切片：真实 TCP + 自有动态端口/线程。
//
// 显式顺序（本包 F1 契约）：owner 域 StopServiceOwner 物理关闭全部连接 →
// 控制线程 RequestStop/Join 完成所有 owner/asio 回调 → DetachOwner 在 context
// 仍存活时断开每会话 owner 域 backend（socket/EventLoop 依赖）→ owner/context
// 可自然销毁 → 外部 shell/活动 handler 继续合法收尾。
//
// case：
//   detach_lifetime        context 先毁、shell 后收尾；原 view/Id/Peer 仍有效，
//                          晚 Wake/Write 取消、两种 Close 幂等、FD 回基线；
//                          脱离后不得虚报 drain（F1/F3）。
//   snapshot_return_on_stop 强停封服务 → handler 先返回 → 归还真正完成（不虚报
//                          错误计数、不留 pending）→ 再 join/detach（F2/F3）。
//   detach_handler_destroy 脱离时最后 registry 强持有引发的用户析构重入合法
//                          native 观测不得死锁（F4）。
//   snapshot_return_on_join 封口且 join 后、detach 前归还，不留 pending（G1）。
//   detach_no_allocation    控制线程拒绝 new 仍实际释放 backend（G2）。
// handshake 用 ContractGate（注册即发布，非 sleep 猜时序）。复用既有 helpers，
// 不复制整套测试框架。
#define main CoBridgeLifetimeFixtureMain
#include "co_bridge_contract.cc"
#undef main

#include <cstdint>
#include <cstdlib>
#include <new>

namespace {
thread_local bool reject_detach_allocation = false;
thread_local std::size_t detach_allocation_attempts = 0;
} // namespace

// 与 net_stream_allocation 相同的可执行文件内 TLS 注入；只在控制线程
// DetachOwner 调用期间拒绝 C++ 分配，不影响 runtime/owner 线程。
void* operator new(std::size_t size) {
    if (reject_detach_allocation) {
        ++detach_allocation_attempts;
        throw std::bad_alloc();
    }
    if (void* value = std::malloc(size == 0 ? 1 : size)) { return value; }
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* value) noexcept { std::free(value); }
void operator delete(void* value, std::size_t) noexcept { std::free(value); }
void operator delete[](void* value) noexcept { std::free(value); }
void operator delete[](void* value, std::size_t) noexcept { std::free(value); }

// A/C 专用最小 fixture：EventLoop/io_context 由 unique_ptr 拥有，使「context 先
// 毁、shell 后收尾」与「脱离前先丢外部强引用」两种顺序可被真实执行
// （ContractFixture 的 loop 是值成员，只能活到整块末尾）。
struct ScopeFixture {
    std::unique_ptr<EventLoop> loop{std::make_unique<EventLoop>(64)};
    std::shared_ptr<ConnectionBridge> bridge;
    NativeSocket listener;
    Address bound;
    std::vector<NativeSocket> clients;
    std::vector<std::shared_ptr<CoTcpConnection>> connections;

    void Start() {
        WarmRuntime();
        CHECK(loop && loop->Start());
        std::error_code error;
        bridge = ConnectionBridge::Create(*loop, BridgeOptions(), error);
        CHECK(bridge && !error);
        listener = OpenStream();
        bound = ListenOn(listener);
    }
    std::shared_ptr<CoTcpConnection> Connect(const std::shared_ptr<ICoConnectionHandler>& handler) {
        clients.push_back(ConnectTo(bound));
        auto holder = std::make_shared<std::shared_ptr<CoTcpConnection>>();
        auto accepted = Task([this, holder, handler] {
            *holder = AcceptViaBridge(*loop, *bridge, listener, handler);
        });
        Get(accepted);
        CHECK(*holder);
        connections.push_back(*holder);
        return *holder;
    }
    // 等事件与桥接命令真正结算（busy 复位发生在 RunEvent 全部收尾之后）。
    void Settle(const std::shared_ptr<ConnectionSession>& session) {
        const auto deadline = Until(5s);
        for (;;) {
            CHECK(Clock::now() < deadline);
            if (!session->SnapshotState().busy && !session->SnapshotState().snap_lent) { break; }
            std::this_thread::yield();
        }
        if (!bridge) { return; }   // 已脱离/桥已析构：无桥命令账票可读
        for (;;) {
            CHECK(Clock::now() < deadline);
            const auto& stats = bridge->GetStats();
            if (stats.ops_submitted.load() == stats.ops_executed.load()) { break; }
            std::this_thread::yield();
        }
    }
    void Release(std::size_t connection_id) {
        if (bridge) {
            for (auto& client : clients) { client.Close(); }
            listener.Close();
            connections.clear();
            bridge.reset();
        }
        loop.reset(); // 真正销毁 io_context（owner 线程已 join）
        (void)connection_id;
    }
};

// ===========================================================================
// A：context 先毁、shell 后收尾（F1）+ 脱离后不得虚报 drain（F3）
// ===========================================================================
static void DetachLifetime() {
    WarmRuntime();
    const auto fd_before = OpenFdCount();
    std::weak_ptr<CoTcpConnection> weak_connection;
    std::weak_ptr<ConnectionSession> weak_session;
    long owner_tid = 0;
    std::size_t connection_id = 0;
    {
        ScopeFixture fixture;
        fixture.Start();
        auto gate = std::make_shared<ContractGate>();
        auto registered = gate->registered.get_future();
        auto returned = std::make_shared<std::promise<void>>();
        auto returning = returned->get_future();

        // handler 取得非空 view 并 handshake 挂起；context 真销毁后再收尾。
        // Id/PeerAddress/ObjectInfo 属公共 CoTcpConnection（不在 ICoConnection
        // 接口面），脱离后由控制线程经外部 shell 读同一会话（等价证据）。
        struct Held { std::string bytes; };
        auto held = std::make_shared<Held>();
        auto handler = std::make_shared<ProbeHandler>();
        handler->on_traffic = [gate, returned, held](ICoConnection& connection, bool wake) {
            CHECK(!wake);
            const auto view = connection.Peek();
            CHECK(!view.empty());                 // 绿色前提：handler 已取得非空 view
            held->bytes.assign(view);             // 独立持有，验证 view 跨脱离/析构仍有效
            gate->Wait();                         // 受管协程 handshake 挂起（非 sleep）
            // —— 此处 owner 线程已 join、context/bridge facade 已析构 ——
            CHECK(connection.Peek() == held->bytes);              // 原 view 仍有效
            CHECK(connection.InboundBuffered() == held->bytes.size());
            CHECK(connection.Wake() == std::errc::operation_canceled);
            CHECK(connection.Write("late", Until(2s)) == std::errc::operation_canceled);
            CHECK(!connection.Close(CloseMode::Block, {}, Until(1s)));    // 幂等成功
            CHECK(!connection.Close(CloseMode::Unblock, {}, Until(1s)));
            returned->set_value();
        };

        auto connection = fixture.Connect(handler);
        connection_id = connection->Id();
        auto session = fixture.bridge->SessionOf(connection_id);
        CHECK(session);
        weak_connection = connection;
        weak_session = session;
        CHECK(ClientSend(fixture.clients.back(), "SNAPSHOT"));
        Get(registered);                          // handler 已取非空 view 并挂起
        CHECK(session->SnapshotState().snap_lent);

        // owner 域强停：封口 + 物理关闭，不等 handler。
        OnOwner(*fixture.loop, [&fixture, &owner_tid] {
            owner_tid = ::syscall(SYS_gettid);
            CHECK(!fixture.bridge->StopServiceOwner());
        });
        CHECK(session->SnapshotState().physical_closed);

        // owner join：网络上下文本轮完成点（全部 owner/asio 回调已执行完）。
        fixture.loop->RequestStop();
        CHECK(fixture.loop->Join());
        CHECK(fixture.loop->FailureCount() == 0);

        // 显式安全脱离：前置物理关闭已满足，门内发布 detached，
        // context 仍活时释放 owner 域 backend 与 registry 强持有。
        const auto backend_probe = session->BackendProbe();
        CHECK(!backend_probe.expired());          // context 仍活时 backend 由 session 强持有
        CHECK(!fixture.bridge->DetachOwner());
        CHECK(backend_probe.expired());           // F1：context 仍活时 backend 已真正释放
        CHECK(fixture.bridge->SessionCount() == 0);          // registry 已释放强持有
        CHECK(session->SnapshotState().physical_closed);
        // F3：registry 清空后不得把「OnClose 从未执行」的会话虚报成 drained。
        CHECK(!fixture.bridge->Drained());
        CHECK(!fixture.bridge->WaitDrained(Until(1ms)));

        fixture.bridge.reset();
        fixture.loop.reset();                        // 真销毁 EventLoop/io_context
        CHECK(fixture.loop == nullptr);
        CHECK(::access(("/proc/self/task/" + std::to_string(owner_tid)).c_str(), F_OK) == -1);
        // context 已消亡，残留 shell/handler 仍自持会话。
        CHECK(!weak_connection.expired() && !weak_session.expired());
        CHECK(connection->Id() == connection_id);
        CHECK(connection->PeerAddress().Port() != 0);
        (void)connection->GetObjectInfo(); // 不可读则在此抛出

        // 收尾 handler：验证脱离后命令/Id/Peer/view 语义。
        gate->Release();
        Get(returning);
        fixture.Settle(session);

        const auto counters = session->GetCounters();
        Log("detach_counters", connection_id,
            "traffic=" + std::to_string(counters.traffic_dispatched)
            + " wake_rejected=" + std::to_string(counters.wake_rejected_sealed)
            + " snapshot_return_errors=" + std::to_string(counters.snapshot_return_errors)
            + " dispatch_crashes=" + std::to_string(counters.dispatch_crashes)
            + " handler_exceptions=" + std::to_string(counters.handler_exceptions));
        CHECK(counters.snapshot_return_errors == 0); // 脱离后本地释放，不虚报错误
        CHECK(counters.dispatch_crashes == 0);
        CHECK(counters.handler_exceptions == 0);
        CHECK(!session->SnapshotState().snap_lent);  // 脱离后本地释放借出快照

        connection.reset();
        session.reset();
        handler.reset();
        fixture.connections.clear();
        for (auto& client : fixture.clients) { client.Close(); }
        fixture.listener.Close();
    }

    CHECK(weak_connection.expired() && weak_session.expired());
    CHECK(::access(("/proc/self/task/" + std::to_string(owner_tid)).c_str(), F_OK) == -1);
    const auto fd_after = OpenFdCount();
    Log("detach_resources", 0, "fd_before=" + std::to_string(fd_before)
        + " fd_after=" + std::to_string(fd_after) + " owner_tid=" + std::to_string(owner_tid));
    CHECK(fd_after == fd_before);
}

// ===========================================================================
// B：强停 → handler 先返回 → 内部归还真正完成 → 再 join/detach（F2/F3）
// ===========================================================================
static void SnapshotReturnOnStop(bool join_before_return = false) {
    WarmRuntime();
    ScopeFixture fixture;
    fixture.Start();
    auto gate = std::make_shared<ContractGate>();
    auto registered = gate->registered.get_future();
    auto returned = std::make_shared<std::promise<void>>();
    auto returning = returned->get_future();

    auto handler = std::make_shared<ProbeHandler>();
    handler->on_traffic = [gate, returned](ICoConnection& connection, bool wake) {
        CHECK(!wake);
        const auto view = connection.Peek();
        CHECK(view == "SNAPSHOT");
        gate->Wait();                       // 强停发生在 handler 已进入之后
        CHECK(connection.Peek() == "SNAPSHOT");
        returned->set_value();              // 返回后由 RunEvent 归还快照
    };
    auto connection = fixture.Connect(handler);
    const auto connection_id = connection->Id();
    auto session = fixture.bridge->SessionOf(connection_id);
    CHECK(session);
    CHECK(ClientSend(fixture.clients.back(), "SNAPSHOT"));
    Get(registered);
    CHECK(session->SnapshotState().snap_lent);

    // 强停：封服务 + 物理关闭，不等 handler（handler 仍挂在 gate 上）。
    OnOwner(*fixture.loop, [&fixture] { CHECK(!fixture.bridge->StopServiceOwner()); });
    CHECK(session->SnapshotState().physical_closed);
    CHECK(session->SnapshotState().snap_lent);          // 借出快照未被强停销毁

    // 第三个合法窗口：owner 已封口并 join，但 DetachOwner 尚未发布。
    // handshake 让 handler 确定在此窗口返回，不靠 sleep 猜测时序。
    if (join_before_return) {
        fixture.loop->RequestStop();
        CHECK(fixture.loop->Join());
        CHECK(session->State().Attached());
    }
    // owner 可受理时真正归还；已 Sealed 且物理关闭时本地释放。
    gate->Release();
    Get(returning);
    fixture.Settle(session);

    const auto counters = session->GetCounters();
    Log("stop_return_counters", connection_id,
        "snapshot_return_errors=" + std::to_string(counters.snapshot_return_errors)
        + " op_timeouts=" + std::to_string(counters.op_timeouts));
    // F2：强停不等于已脱离，handler 返回时的内部归还不得被服务门一并拒绝。
    CHECK(counters.snapshot_return_errors == 0);
    CHECK(counters.op_timeouts == 0);
    CHECK(!session->SnapshotState().snap_lent);
    CHECK(!session->SnapshotState().snap_return_pending);   // 归还已确认，不永久 pending
    if (!join_before_return) {
        const auto physical = OwnerStats(*fixture.loop, session);
        CHECK(physical.snapshots_taken == physical.snapshots_returned); // 真的还给了 backend
    }

    // 再 join/detach：OnClose 从未执行（强停压掉），drain 不得虚报（F3）。
    fixture.loop->RequestStop();
    CHECK(fixture.loop->Join());
    CHECK(fixture.loop->FailureCount() == 0);
    CHECK(!fixture.bridge->DetachOwner());
    CHECK(!session->DrainedSnapshot());
    CHECK(!fixture.bridge->Drained());
    CHECK(!fixture.bridge->WaitDrained(Until(1ms)));

    connection.reset();
    session.reset();
    handler.reset();
    fixture.Release(connection_id);
}

// Detach 在发布后不得再分配：即使控制线程 new 全拒绝，仍须真正释放
// backend 才能允许 context 先于残留 shell 销毁（G2）。
static void DetachWithoutAllocation() {
    WarmRuntime();
    const auto fd_before = OpenFdCount();
    {
        ScopeFixture fixture;
        fixture.Start();
        auto handler = std::make_shared<ProbeHandler>();
        auto connection = fixture.Connect(handler);
        auto session = fixture.bridge->SessionOf(connection->Id());
        CHECK(session);
        const auto backend = session->BackendProbe();
        CHECK(!backend.expired());
        OnOwner(*fixture.loop, [&fixture] { CHECK(!fixture.bridge->StopServiceOwner()); });
        fixture.loop->RequestStop();
        CHECK(fixture.loop->Join());
        fixture.Settle(session);

        bool allocation_threw = false;
        std::error_code error;
        struct AllocationGuard {
            AllocationGuard() { detach_allocation_attempts = 0; reject_detach_allocation = true; }
            ~AllocationGuard() { reject_detach_allocation = false; }
        };
        {
            AllocationGuard guard;
            try { error = fixture.bridge->DetachOwner(); }
            catch (const std::bad_alloc&) { allocation_threw = true; }
        }
        Log("detach_allocation", connection->Id(),
            "attempts=" + std::to_string(detach_allocation_attempts)
            + " threw=" + std::to_string(allocation_threw));
        CHECK(!allocation_threw);
        CHECK(!error);
        CHECK(detach_allocation_attempts == 0);
        CHECK(backend.expired());
        CHECK(!fixture.bridge->DetachOwner()); // 成功重试也不能掩盖未释放 backend
        fixture.loop.reset();
        fixture.bridge.reset();
        connection.reset();
        session.reset();
        handler.reset();
        fixture.connections.clear();
    }
    CHECK(OpenFdCount() == fd_before);
}

// ===========================================================================
// C：脱离时最后一次强持有销毁引发的用户析构重入合法 native 观测（F4）
// ===========================================================================
static void DetachHandlerDestroy() {
    WarmRuntime();
    std::atomic<int> destroyed{0};
    std::atomic<int> reentered{0};
    {
        ScopeFixture fixture;
        fixture.Start();
        // 无强引用环：handler 只持共享 state（无 registry/无 session/shell 引用），
        // 析构里调用桥的 m_ 门入口（Attached/NotifyChange）做合法 native 观测。
        struct ReenterOnDestroy final : ICoConnectionHandler {
            // 非持有指针：session 的 state_ 成员在 handler_ 之后析构。
            ConnectionBridgeState* state{nullptr};
            std::atomic<int>* destroyed;
            std::atomic<int>* reentered;
            void OnOpen(ICoConnection&) override {}
            void OnTraffic(ICoConnection&, bool) override {}
            void OnClose(ICoConnection&, const std::error_code&) override {}
            ~ReenterOnDestroy() {
                ++*destroyed;
                if (state) {                  // 不在析构内抛异常/断言
                    (void)state->Attached();
                    state->NotifyChange();
                    ++*reentered;
                }
            }
        };
        auto handler = std::make_shared<ReenterOnDestroy>();
        handler->destroyed = &destroyed;
        handler->reentered = &reentered;
        auto connection = fixture.Connect(handler);
        auto session = fixture.bridge->SessionOf(connection->Id());
        CHECK(session);
        handler->state = &session->State();
        handler.reset();                      // 桥/registry 成为最后强持有者
        connection.reset();
        fixture.connections.clear();

        // F1 前置拒绝：连接尚未物理关闭时不做半脱离（不发布 detached、
        // 不清 registry、不拆活 socket），owner 命令面仍可用。
        CHECK(fixture.bridge->DetachOwner() == std::errc::operation_in_progress);
        CHECK(fixture.bridge->SessionCount() == 1);
        OnOwner(*fixture.loop, [] {});        // owner 仍 attached：命令仍可入队

        OnOwner(*fixture.loop, [&fixture] { CHECK(!fixture.bridge->StopServiceOwner()); });
        fixture.loop->RequestStop();
        CHECK(fixture.loop->Join());
        CHECK(fixture.loop->FailureCount() == 0);
        session.reset();                      // 只剩 registry（桥）强持有

        // 短超时保护：最后一次强持有在桥锁下析构会重入非递归 m_ 而死锁。
        auto done = std::make_shared<std::promise<void>>();
        auto finished = done->get_future();
        std::thread detacher([&fixture, done] {
            try { CHECK(!fixture.bridge->DetachOwner()); done->set_value(); }
            catch (...) { done->set_exception(std::current_exception()); }
        });
        const bool settled = finished.wait_for(10s) == std::future_status::ready;
        if (!settled) {
            std::cerr << "DETACH DEADLOCK: last registry holder destroyed under bridge lock\n";
            detacher.detach();               // 不挂宿主；进程经 _Exit 收尾
            throw std::runtime_error("DetachOwner deadlock: user destructor re-entered bridge lock");
        }
        detacher.join();
        finished.get();
        CHECK(destroyed.load() == 1);
        CHECK(reentered.load() == 1);
        Log("detach_handler_reentry", 0, "destroyed=1 reentered=1 no_deadlock");
        fixture.Release(0);
    }
}

#include "ProcessRuntimeTestMain.hpp"

static int RunLifetimeCases(int argc, char** argv) {
    try {
        CHECK(argc == 2);
        const std::string case_name = argv[1];
        if (case_name == "detach_lifetime") { DetachLifetime(); }
        else if (case_name == "snapshot_return_on_stop") { SnapshotReturnOnStop(); }
        else if (case_name == "snapshot_return_on_join") { SnapshotReturnOnStop(true); }
        else if (case_name == "detach_no_allocation") { DetachWithoutAllocation(); }
        else if (case_name == "detach_handler_destroy") { DetachHandlerDestroy(); }
        else { return 2; }
        std::cout << "CASE OK co_bridge_lifetime " << case_name
                  << " asserts=" << AssertCounter().load() << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "RUN FAILED: " << error.what() << '\n';
        return 1;
    }
}

int main(int argc, char** argv) {
    // 测试进程退出边界：局部 RAII 收口 → 刷新输出 → _Exit，见
    // ProcessRuntimeTestMain.hpp 头注（静态析构竞态为已知限制，未修复）。
    return bbt_net_test::RunTestProcessExit([&] { return RunLifetimeCases(argc, argv); });
}
