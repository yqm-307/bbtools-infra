// P3-c1 契约测试：公共 CoConnection/Handler 桥接（真实 TCP 动态端口/对端）。
//
// 场景（每个 case 独立进程；每进程 runtime 初始化一次、无 Stop）：
//   co_bridge_lifecycle        事件顺序/峰值1、held-view 跨真实协程等待、
//                              挂起期间 owner/另一连接/另一协程进展、Wake 合并、
//                              Wake 空快照、混合关闭、OnClose 内重复 Block。
//   co_bridge_close            owner gate 下 Unblock 封口、Block 只等物理、
//                              OnClose gate 下幂等、晚写/晚 Wake 拒绝。
//   co_bridge_handler_exception OnOpen/OnTraffic/OnClose 抛出均排空；快照自动归还。
//   co_bridge_context_reject   普通/TLS-enabled/owner 命令拒绝；非事件 co
//                              Wake 合法、Reader 拒绝；post-co idle 仍未覆盖。
//   co_bridge_queue_full       人为卡住 owner：Full 明确拒绝、Unblock 回滚无假
//                              Closing、恢复、Sealed 生命周期。
//   co_bridge_g1_drain         确定性延迟 OnClose 证明 drain 未完成；释放后
//                              drained→Join；每连接计数守恒；weak/FD/thread 回落。
//   co_bridge_g6_late_resource 有限 deadline 卡住 owner：Write/Block 超时、
//                              迟到命令不写调用栈、快照归还迟到收口、无假 Closing。
//
// 断言失败以异常抛出 → 进程非零退出（不受 NDEBUG 影响）；事件序号只取自
// 真实执行点，不做 sleep 推断时序。

#include <algorithm>
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <dirent.h>
#include <poll.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/CoConnectionHandler.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/Options.hpp>
#include <bbt/infra/net/detail/ConnectionBridge.hpp>
#include <bbt/infra/net/detail/EventLoop.hpp>

using namespace bbt::infra::net;
using namespace std::chrono_literals;
using bbt::coroutine::WaitStatus;
using Clock = std::chrono::steady_clock;

static std::atomic<int>& AssertCounter() {
    static std::atomic<int> counter{0};
    return counter;
}
static void Check(bool value, const char* expression, int line) {
    ++AssertCounter();
    if (!value) { throw std::runtime_error(std::string("assert@") + std::to_string(line) + ": " + expression); }
}
#define CHECK(expression) Check(static_cast<bool>(expression), #expression, __LINE__)

static CoDeadline Until(std::chrono::milliseconds duration) { return Clock::now() + duration; }
static CoDeadline DeadlineOf(std::chrono::milliseconds duration) { return Clock::now() + duration; }

// ---- 事件日志：只从真实执行点记录 ----
static std::mutex g_log_mutex;
static std::vector<std::string> g_log;
static const Clock::time_point g_start = Clock::now();
static std::int64_t Log(const std::string& event, std::size_t conn, const std::string& detail) {
    std::lock_guard<std::mutex> guard(g_log_mutex);
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - g_start).count();
    std::ostringstream line;
    line << "EV seq=" << g_log.size() << " t=" << ms << "ms ev=" << event
         << " conn=" << conn << " coro=" << bbt::coroutine::GetLocalCoroutineId() << " " << detail;
    g_log.push_back(line.str());
    std::cout << line.str() << '\n';
    return static_cast<std::int64_t>(g_log.size() - 1);
}

template<class T> static T Get(std::future<T>& future) {
    CHECK(future.wait_for(10s) == std::future_status::ready);
    return future.get();
}
static void Get(std::future<void>& future) {
    CHECK(future.wait_for(10s) == std::future_status::ready);
    future.get();
}
static void Get(std::future<void>&& future) {
    CHECK(future.wait_for(10s) == std::future_status::ready);
    future.get();
}
static std::future<void> Task(std::function<void()> callback) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    const std::size_t stack = g_bbt_coroutine_config->m_cfg_stack_size;
    g_scheduler->RegistCoroutineTask(stack, [promise, callback] {
        try { callback(); promise->set_value(); }
        catch (...) { promise->set_exception(std::current_exception()); }
    }, "co.bridge.test.task");
    return future;
}
// owner 域同步命令（等待完成）。
static void OnOwner(EventLoop& loop, std::function<void()> callback) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    CHECK(loop.Submit([promise, callback] {
        try { callback(); promise->set_value(); }
        catch (...) { promise->set_exception(std::current_exception()); }
    }) == SubmitStatus::Accepted);
    Get(future);
}

// 失败收尾仍由控制线程驱动：Full 只做有界退避重试，不阻塞 owner。
// 闭包按值持有桥和连接 id，不能借用 fixture 的 this 或调用栈。
static void SubmitOwnedCleanup(EventLoop& loop,
    std::shared_ptr<ConnectionBridge> bridge, std::vector<std::size_t> ids) {
    auto done = std::make_shared<std::promise<void>>();
    auto finished = done->get_future();
    const auto deadline = Until(8s);
    for (;;) {
        const auto status = loop.Submit([bridge, ids, done] {
            try {
                for (const auto id : ids) {
                    const auto error = bridge->CloseNative(id, {});
                    if (error) { throw std::system_error(error, "cleanup CloseNative"); }
                }
                done->set_value();
            } catch (...) {
                done->set_exception(std::current_exception());
            }
        });
        if (status == SubmitStatus::Accepted) {
            Get(finished);
            return;
        }
        if (status != SubmitStatus::Full) {
            throw std::runtime_error("cleanup Submit rejected status="
                + std::to_string(static_cast<int>(status)));
        }
        if (Clock::now() >= deadline) {
            throw std::runtime_error("cleanup Submit remained Full before deadline");
        }
        std::this_thread::sleep_for(1ms);
    }
}
// owner 域卡住命令：控制线程 promise 放行（有限等待，防止泄漏挂死）。
static std::future<void> StallOwner(EventLoop& loop, std::promise<void>& release) {
    auto done = std::make_shared<std::promise<void>>();
    auto future = done->get_future();
    CHECK(loop.Submit([&release, done] {
        release.get_future().wait_for(5s);
        done->set_value();
    }) == SubmitStatus::Accepted);
    return future;
}

// ---- 每进程一次 runtime；单 Processer 足以证明挂起让出 ----
struct RuntimeIdentity { std::thread::id processer; };
static RuntimeIdentity WarmRuntime() {
    static const bool started = [] {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 1;
        g_bbt_coroutine_config->m_cfg_stack_size = 65536;
        g_scheduler->Start();
        return true;
    }();
    CHECK(started);
    RuntimeIdentity identity{};
    auto warm = Task([&identity] {
        identity.processer = std::this_thread::get_id();
        bbt::coroutine::sync::CoWaiter waiter;
        CHECK(waiter.Wait(bbt::coroutine::WaitOptions{Clock::now() + 5ms}) == WaitStatus::TimedOut);
    });
    Get(warm);
    return identity;
}

// ---- TCP fixture（真实动态端口）----
static NativeSocket OpenStream() {
    std::error_code error;
    auto socket = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    CHECK(!error && socket.Fd() >= 0);
    return socket;
}
static Address ListenOn(NativeSocket& listener) {
    std::error_code error;
    CHECK(!listener.Bind(Address::Ip("127.0.0.1", 0, error)) && !error);
    CHECK(!listener.Listen(8));
    const auto bound = listener.LocalAddress(error);
    CHECK(!error);
    return bound;
}
static NativeSocket ConnectTo(const Address& bound) {
    auto client = OpenStream();
    const auto connected = client.Connect(bound);
    CHECK(!connected || connected.value() == EINPROGRESS);
    for (int i = 0; i < 2000; ++i) {
        if (!client.PendingError()) { return client; }
        std::this_thread::sleep_for(1ms);
    }
    CHECK(!client.PendingError());
    return client;
}
static bool ClientSend(NativeSocket& socket, const std::string& data) {
    return socket.Send(data.data(), data.size()).bytes == data.size();
}
static NormalizedOptions BridgeOptions() {
    Options raw;
    raw.read_buffer_cap = 1024;
    raw.write_buffer_cap = 4096;
    raw.max_inbound_bytes = 64 * 1024;
    raw.max_outbound_bytes = 1024 * 1024;
    raw.max_pending_events = 64;
    NormalizedOptions options;
    CHECK(!NormalizeOptions(raw, 1, options));
    return options;
}

// 受管协程内接纳：经既有 CoIoOperation 在 owner 上等待 Adopt 完成。
static std::shared_ptr<CoTcpConnection> AcceptViaBridge(EventLoop& loop,
    ConnectionBridge& bridge, NativeSocket& listener,
    std::shared_ptr<ICoConnectionHandler> handler) {
    pollfd pfd{listener.Fd(), POLLIN, 0};
    CHECK(::poll(&pfd, 1, 5000) == 1 && (pfd.revents & POLLIN) != 0);
    auto holder = std::make_shared<std::shared_ptr<CoTcpConnection>>();
    auto error_holder = std::make_shared<std::error_code>();
    auto socket = std::make_shared<NativeSocket>();
    auto peer = std::make_shared<Address>();
    auto* loop_pointer = &loop;
    auto* bridge_pointer = &bridge;
    const auto result = CoIoOperation::Run(loop,
        bbt::coroutine::WaitOptions{Clock::now() + 3s},
        [&, socket, peer, loop_pointer, bridge_pointer, handler](std::shared_ptr<CoIoOperation> operation) {
            CHECK(loop_pointer->IsOwner());
            std::error_code accept_error;
            *socket = listener.Accept(*peer, accept_error);
            if (accept_error || socket->Fd() < 0) {
                *error_holder = accept_error ? accept_error : std::make_error_code(std::errc::io_error);
                (void)operation->Complete({0, *error_holder});
                return;
            }
            *holder = bridge_pointer->Adopt(std::move(*socket), *peer, handler, *error_holder);
            (void)operation->Complete({1, *error_holder});
        });
    CHECK(result.wait_status == WaitStatus::Completed && !result.io.error && !*error_holder);
    CHECK(*holder);
    return std::move(*holder);
}

// 等待会话计数（轮询 session 原子快照；不 sleep 推断）。
static ConnectionSession::Counters WaitCounters(ConnectionBridge& bridge, std::size_t id,
    const std::function<bool(const ConnectionSession::Counters&)>& predicate) {
    const auto deadline = Clock::now() + 8s;
    for (;;) {
        auto session = bridge.SessionOf(id);
        CHECK(session);
        const auto counters = session->GetCounters();
        if (predicate(counters)) { return counters; }
        CHECK(Clock::now() < deadline);
        std::this_thread::sleep_for(2ms);
    }
}

// 通用 handler 桩：各场景用 std::function 注入。
struct ProbeHandler final : ICoConnectionHandler {
    std::function<void(ICoConnection&)> on_open;
    std::function<void(ICoConnection&, bool)> on_traffic;
    std::function<void(ICoConnection&, const std::error_code&)> on_close;
    void OnOpen(ICoConnection& connection) override { if (on_open) { on_open(connection); } }
    void OnTraffic(ICoConnection& connection, bool wake) override { if (on_traffic) { on_traffic(connection, wake); } }
    void OnClose(ICoConnection& connection, const std::error_code& reason) override { if (on_close) { on_close(connection, reason); } }
};

// 场景收尾：drain → RequestStop → Join → 释放依赖（G1 顺序）。
static void Teardown(EventLoop& loop, std::shared_ptr<ConnectionBridge>& bridge,
    std::vector<NativeSocket>& clients) {
    CHECK(bridge->WaitDrained(Until(8s)));
    const auto& stats = bridge->GetStats();
    CHECK(stats.ops_submitted.load() == stats.ops_executed.load());
    loop.RequestStop();
    CHECK(loop.Join());
    CHECK(loop.FailureCount() == 0);
    for (auto& client : clients) { client.Close(); CHECK(client.Fd() < 0); }
    bridge.reset();
    Log("teardown_done", 0, "failure_count=0");
}

// 结果和 session 随 owner 命令保活；等待成功后才读取结果。
static StreamConnection::Stats OwnerStats(EventLoop& loop,
    std::shared_ptr<ConnectionSession> session) {
    auto result = std::make_shared<StreamConnection::Stats>();
    OnOwner(loop, [result, session] { *result = session->ConnStatsOwner(); });
    return *result;
}

// ===========================================================================
// 场景一：生命周期（顺序 / 峰值1 / held-view / Wake 合并 / 混合关闭）
// ===========================================================================
static void ScenarioLifecycle() {
    WarmRuntime();
    EventLoop loop(64);
    CHECK(loop.Start());
    std::error_code bridge_error;
    auto bridge = ConnectionBridge::Create(loop, BridgeOptions(), bridge_error);
    CHECK(!bridge_error && bridge);

    auto listener = std::make_shared<NativeSocket>(OpenStream());
    const auto bound = ListenOn(*listener);
    auto client = std::make_shared<NativeSocket>();
    auto opened = std::make_shared<std::promise<void>>();
    auto traffic = std::make_shared<std::promise<void>>();
    auto block_returned = std::make_shared<std::promise<void>>();
    auto closed = std::make_shared<std::promise<void>>();
    std::atomic<int> sequence{0};
    std::atomic<int> block_sequence{0};
    std::atomic<int> close_sequence{0};

    auto handler = std::make_shared<ProbeHandler>();
    handler->on_open = [opened](ICoConnection& connection) {
        CHECK(bbt::coroutine::GetLocalCoroutineId() != 0);
        CHECK(connection.InboundBuffered() == 0);
        opened->set_value();
    };
    handler->on_traffic = [&, traffic, block_returned](ICoConnection& connection, bool wake) {
        CHECK(!wake);
        CHECK(bbt::coroutine::GetLocalCoroutineId() != 0);
        CHECK(connection.InboundBuffered() == 5);
        CHECK(connection.Peek() == "HELLO");
        CHECK(!connection.Discard(5));
        traffic->set_value();

        const auto close_error = connection.Close(CloseMode::Block,
            std::make_error_code(std::errc::operation_canceled), Until(5s));
        CHECK(!close_error);
        block_sequence.store(++sequence);
        block_returned->set_value();
    };
    handler->on_close = [&, closed](ICoConnection& connection, const std::error_code&) {
        CHECK(bbt::coroutine::GetLocalCoroutineId() != 0);
        CHECK(!connection.Close(CloseMode::Block, {}, Until(1s)));
        close_sequence.store(++sequence);
        closed->set_value();
    };

    *client = ConnectTo(bound);
    auto accepted = std::make_shared<std::promise<std::shared_ptr<CoTcpConnection>>>();
    auto accept_task = Task([&loop, &bridge, &listener, handler, accepted] {
        accepted->set_value(AcceptViaBridge(loop, *bridge, *listener, handler));
    });
    Get(accept_task);
    auto connection = accepted->get_future().get();
    Get(opened->get_future());
    CHECK(connection->PeerAddress().Port() != 0);
    CHECK(ClientSend(*client, "HELLO"));
    Get(traffic->get_future());
    Get(block_returned->get_future());
    Get(closed->get_future());

    CHECK(block_sequence.load() > 0);
    CHECK(close_sequence.load() > block_sequence.load());
    CHECK(bridge->WaitDrained(Until(8s)));
    const auto session = bridge->SessionOf(connection->Id());
    CHECK(session);
    const auto counters = session->GetCounters();
    CHECK(counters.open_entered == 1);
    CHECK(counters.traffic_dispatched == 1);
    CHECK(counters.active_peak == 1);
    CHECK(counters.onclose_entered == 1 && counters.onclose_returned == 1);
    CHECK(counters.handler_exceptions == 0);
    CHECK(bridge->GetStats().ops_submitted.load() == bridge->GetStats().ops_executed.load());

    const auto stats = OwnerStats(loop, session);
    CHECK(stats.physical_closes == 1);
    CHECK(stats.snapshots_taken == stats.snapshots_returned);
    CHECK(session->SnapshotState().physical_closed);
    CHECK(session->SnapshotState().close_finished);

    loop.RequestStop();
    CHECK(loop.Join());
    CHECK(loop.FailureCount() == 0);
    client->Close();
    listener->Close();
    bridge.reset();
}

// G6：只用真实通知/等待结果编排迟到 take；产品期限仍为 30s。
static void ScenarioLateResource() {
    WarmRuntime();
    EventLoop loop(64);
    CHECK(loop.Start());
    std::error_code error;
    auto bridge = ConnectionBridge::Create(loop, BridgeOptions(), error);
    CHECK(bridge && !error);
    auto listener = OpenStream();
    auto client = ConnectTo(ListenOn(listener));
    auto open_gate = std::make_shared<bbt::coroutine::sync::CoWaiter>();
    auto close_gate = std::make_shared<bbt::coroutine::sync::CoWaiter>();
    struct State {
        std::promise<void> opened, close_entered, received;
        std::string bytes;
    };
    auto state = std::make_shared<State>();
    auto opened_future = state->opened.get_future();
    auto close_future = state->close_entered.get_future();
    auto received_future = state->received.get_future();
    auto handler = std::make_shared<ProbeHandler>();
    handler->on_open = [state, open_gate](ICoConnection&) {
        bbt::coroutine::sync::CombinedWaitOptions options;
        options.deadline = Until(38s);
        CHECK(open_gate->Wait(options, [state] { state->opened.set_value(); return true; })
            == bbt::coroutine::sync::CombinedWaitStatus::Completed);
    };
    handler->on_traffic = [state](ICoConnection& connection, bool wake) {
        CHECK(!wake);
        const auto view = connection.Peek();
        if (view.empty()) { return; } // readiness 合并/超时后的空事件不重复完成数据 promise。
        state->bytes.append(view.data(), view.size());
        CHECK(!connection.Discard(view.size()));
        CHECK(std::string("HELLOWORLD").compare(0, state->bytes.size(), state->bytes) == 0);
        if (state->bytes == "HELLOWORLD") { state->received.set_value(); }
    };
    handler->on_close = [state, close_gate](ICoConnection&, const std::error_code&) {
        bbt::coroutine::sync::CombinedWaitOptions options;
        options.deadline = Until(5s);
        CHECK(close_gate->Wait(options, [state] { state->close_entered.set_value(); return true; })
            == bbt::coroutine::sync::CombinedWaitStatus::Completed);
    };
    std::shared_ptr<CoTcpConnection> connection;
    auto accept = Task([&] { connection = AcceptViaBridge(loop, *bridge, listener, handler); });
    Get(accept);
    Get(opened_future);
    auto session = bridge->SessionOf(connection->Id());
    CHECK(session);
    CHECK(ClientSend(client, "HELLO"));
    StreamConnection::Stats physical{};
    const auto ready_deadline = Until(5s);
    do {
        physical = OwnerStats(loop, session);
        CHECK(Clock::now() < ready_deadline);
    } while (physical.read_bytes != 5 || physical.traffic_notifications == 0);
    Log("hello_ready", connection->Id(), "read_bytes=5 traffic_notified");

    auto release = std::make_shared<std::promise<void>>();
    auto release_future = release->get_future().share();
    auto entered = std::make_shared<std::promise<void>>();
    auto owner_done = std::make_shared<std::promise<void>>();
    auto entered_future = entered->get_future();
    auto done_future = owner_done->get_future();
    // 即便断言抛出也释放自有 owner gate；不借用控制线程栈到迟到命令。
    struct ReleaseGuard {
        std::shared_ptr<std::promise<void>> promise;
        bool released{false};
        void Release() { if (!released) { released = true; promise->set_value(); } }
        ~ReleaseGuard() { Release(); }
    } guard{release};
    CHECK(loop.Submit([release_future, entered, owner_done] {
        entered->set_value();
        const bool released = release_future.wait_for(37s) == std::future_status::ready;
        owner_done->set_value();
        CHECK(released);
    }) == SubmitStatus::Accepted);
    Get(entered_future);
    CHECK(open_gate->Notify(bbt::coroutine::CoEventValue::From(true)) == 0);
    const auto before = session->GetCounters().op_timeouts;
    const auto timeout_deadline = Until(34s);
    while (session->GetCounters().op_timeouts == before) {
        CHECK(Clock::now() < timeout_deadline);
        CHECK(done_future.wait_for(1ms) != std::future_status::ready);
    }
    CHECK(bridge->GetStats().ops_submitted.load() > bridge->GetStats().ops_executed.load());
    CHECK(!bridge->WaitDrained(Until(1ms)));
    Log("take_timed_out", connection->Id(), "owner_still_blocked");
    guard.Release();
    Get(done_future);
    physical = OwnerStats(loop, session);
    CHECK(ClientSend(client, "WORLD"));
    CHECK(received_future.wait_for(5s) == std::future_status::ready);
    received_future.get();
    CHECK(state->bytes == "HELLOWORLD");
    OnOwner(loop, [&] { CHECK(!bridge->CloseNative(connection->Id(), {})); });
    Get(close_future);
    CHECK(!bridge->WaitDrained(Until(5ms)));
    CHECK(close_gate->Notify(bbt::coroutine::CoEventValue::From(true)) == 0);
    CHECK(bridge->WaitDrained(Until(5s)));
    CHECK(session->GetCounters().onclose_returned == 1);
    CHECK(session->GetCounters().handler_exceptions == 0);
    CHECK(session->GetCounters().snapshot_return_errors == 0);
    CHECK(!session->SnapshotState().snap_lent);
    CHECK(bridge->GetStats().ops_submitted.load() == bridge->GetStats().ops_executed.load());
    physical = OwnerStats(loop, session);
    CHECK(physical.physical_closes == 1);
    CHECK(physical.snapshots_taken == physical.snapshots_returned);
    CHECK(physical.snapshot_consumed_bytes == 10);
    CHECK(physical.read_bytes == 10 && physical.traffic_notifications >= 2);
    Log("late_resource_conserved", connection->Id(), "HELLOWORLD consumed=10 take=return drain=true");
    loop.RequestStop();
    CHECK(loop.Join());
    CHECK(loop.FailureCount() == 0);
    client.Close();
    listener.Close();
}

// Owned gates publish registration, not a timing guess. Release also handles
// failure cleanup racing a not-yet-entered handler.
struct ContractGate {
    bbt::coroutine::sync::CoWaiter waiter;
    std::promise<void> registered;
    std::atomic<bool> released{false};
    void Wait() {
        bbt::coroutine::sync::CombinedWaitOptions options;
        options.deadline = Until(8s);
        CHECK(waiter.Wait(options, [this] {
            registered.set_value();
            if (released.load()) { (void)waiter.Notify(bbt::coroutine::CoEventValue::From(true)); }
            return true;
        }) == bbt::coroutine::sync::CombinedWaitStatus::Completed);
    }
    void Release() {
        released.store(true);
        (void)waiter.Notify(bbt::coroutine::CoEventValue::From(true));
    }
};

struct ContractFixture {
    explicit ContractFixture(std::size_t capacity = 64) : loop(capacity) {}
    EventLoop loop;
    std::shared_ptr<ConnectionBridge> bridge;
    NativeSocket listener;
    Address bound;
    std::vector<NativeSocket> clients;
    std::vector<std::shared_ptr<CoTcpConnection>> connections;
    std::vector<std::function<void()>> releases;

    void Start() {
        WarmRuntime();
        CHECK(loop.Start());
        std::error_code error;
        bridge = ConnectionBridge::Create(loop, BridgeOptions(), error);
        CHECK(bridge && !error);
        listener = OpenStream();
        bound = ListenOn(listener);
    }
    std::shared_ptr<CoTcpConnection> Connect(const std::shared_ptr<ProbeHandler>& handler) {
        clients.push_back(ConnectTo(bound));
        auto holder = std::make_shared<std::shared_ptr<CoTcpConnection>>();
        auto accepted = Task([this, holder, handler] {
            *holder = AcceptViaBridge(loop, *bridge, listener, handler);
        });
        Get(accepted);
        CHECK(*holder);
        connections.push_back(*holder);
        return *holder;
    }
    std::shared_ptr<ContractGate> Gate() {
        auto gate = std::make_shared<ContractGate>();
        releases.push_back([gate] { gate->Release(); });
        return gate;
    }
    void Finish() {
        for (const auto& release : releases) { release(); }
        if (bridge) {
            auto bridge_owned = bridge;
            std::vector<std::size_t> ids;
            ids.reserve(connections.size());
            for (const auto& connection : connections) { ids.push_back(connection->Id()); }
            SubmitOwnedCleanup(loop, std::move(bridge_owned), std::move(ids));
            CHECK(bridge->WaitDrained(Until(8s)));
            CHECK(bridge->GetStats().ops_submitted.load() == bridge->GetStats().ops_executed.load());
        }
        loop.RequestStop();
        CHECK(loop.Join());
        CHECK(loop.FailureCount() == 0);
        for (auto& client : clients) { client.Close(); }
        listener.Close();
        connections.clear();
        bridge.reset();
    }
};

template<class F> static void RunContract(F body) {
    ContractFixture fixture;
    fixture.Start();
    try { body(fixture); }
    catch (...) {
        const auto failure = std::current_exception();
        try { fixture.Finish(); }
        catch (const std::exception& cleanup) { std::cerr << "CLEANUP FAILED: " << cleanup.what() << '\n'; }
        std::rethrow_exception(failure);
    }
    fixture.Finish();
}

static void CheckConserved(ContractFixture& fixture,
    const std::shared_ptr<ConnectionSession>& session, std::size_t exceptions) {
    const auto counters = session->GetCounters();
    CHECK(counters.open_entered == 1);
    CHECK(counters.onclose_entered == 1 && counters.onclose_returned == 1);
    CHECK(counters.active_peak == 1);
    CHECK(counters.handler_exceptions == exceptions);
    CHECK(counters.dispatch_crashes == 0 && counters.snapshot_return_errors == 0);
    CHECK(counters.close_init_submit_failures == 0 && counters.op_timeouts == 0);
    const auto state = session->SnapshotState();
    CHECK(state.physical_closed && state.close_finished && !state.snap_lent && !state.busy);
    auto physical_result = std::make_shared<StreamConnection::Stats>();
    OnOwner(fixture.loop, [physical_result, session] { *physical_result = session->ConnStatsOwner(); });
    const auto physical = *physical_result;
    CHECK(physical.physical_closes == 1);
    CHECK(physical.snapshots_taken == physical.snapshots_returned);
    Log("contract_conserved", session->Id(), "exceptions=" + std::to_string(exceptions)
        + " physical=1 take=" + std::to_string(physical.snapshots_taken)
        + " return=" + std::to_string(physical.snapshots_returned));
}

static void ScenarioClose() {
    RunContract([](ContractFixture& fixture) {
        auto open_gate = fixture.Gate();
        auto close_gate = fixture.Gate();
        auto opened = open_gate->registered.get_future();
        auto closing = close_gate->registered.get_future();
        auto handler = std::make_shared<ProbeHandler>();
        handler->on_open = [open_gate](ICoConnection&) { open_gate->Wait(); };
        handler->on_traffic = [](ICoConnection&, bool) { CHECK(false); };
        handler->on_close = [close_gate](ICoConnection& connection, const std::error_code&) {
            CHECK(!connection.Close(CloseMode::Block, {}, Until(1s)));
            CHECK(!connection.Close(CloseMode::Unblock, {}, Until(1s)));
            close_gate->Wait();
        };
        auto connection = fixture.Connect(handler);
        Get(opened);
        // An owner barrier consumes all earlier adoption commands. OnOpen is
        // parked, so no snapshot command can sit ahead of our close commands.
        OnOwner(fixture.loop, [] {});
        auto session = fixture.bridge->SessionOf(connection->Id());
        CHECK(session);
        auto release = std::make_shared<std::promise<void>>();
        auto released = release->get_future().share();
        auto entered = std::make_shared<std::promise<void>>();
        auto done = std::make_shared<std::promise<void>>();
        auto entered_future = entered->get_future();
        auto done_future = done->get_future();
        auto release_once = std::make_shared<std::atomic<bool>>(false);
        auto release_owner = [release, release_once] {
            if (!release_once->exchange(true)) { release->set_value(); }
        };
        fixture.releases.push_back(release_owner);
        CHECK(fixture.loop.Submit([released, entered, done] {
            entered->set_value();
            const bool ready = released.wait_for(8s) == std::future_status::ready;
            done->set_value();
            CHECK(ready);
        }) == SubmitStatus::Accepted);
        Get(entered_future);
        auto unblock = Task([connection] {
            CHECK(!connection->Close(CloseMode::Unblock, {}, Until(5s)));
            CHECK(!connection->Close(CloseMode::Unblock, {}, Until(5s)));
        });
        Get(unblock);
        CHECK(done_future.wait_for(0ms) != std::future_status::ready);
        CHECK(session->SnapshotState().sealed && !session->SnapshotState().physical_closed);
        CHECK(!fixture.bridge->WaitDrained(Until(1ms)));
        const auto rejected_before = session->GetCounters().wake_rejected_sealed;
        auto late_wake = Task([connection] { CHECK(connection->Wake() == std::errc::operation_canceled); });
        Get(late_wake);
        CHECK(session->GetCounters().wake_rejected_sealed == rejected_before + 1);
        auto block_entered = std::make_shared<std::promise<void>>();
        auto block_entered_future = block_entered->get_future();
        auto block = Task([connection, block_entered] {
            block_entered->set_value();
            CHECK(!connection->Close(CloseMode::Block, {}, Until(5s)));
        });
        Get(block_entered_future);
        auto processer_barrier = Task([] {});
        Get(processer_barrier); // single Processer cannot pass a blocking Close
        CHECK(block.wait_for(0ms) != std::future_status::ready);
        CHECK(!session->SnapshotState().physical_closed);
        CHECK(done_future.wait_for(0ms) != std::future_status::ready);
        Log("unblock_sealed_block_parked", connection->Id(), "owner_gate=held physical=false");
        release_owner();
        Get(done_future);
        Get(block);
        CHECK(session->SnapshotState().physical_closed);
        // The original OnOpen is still parked: Block did not wait for OnClose.
        CHECK(!fixture.bridge->WaitDrained(Until(1ms)));
        open_gate->Release();
        Get(closing);
        CHECK(session->GetCounters().onclose_returned == 0);
        CHECK(!fixture.bridge->WaitDrained(Until(1ms)));
        auto repeated = Task([connection] {
            CHECK(!connection->Close(CloseMode::Block, {}, Until(1s)));
            CHECK(!connection->Close(CloseMode::Unblock, {}, Until(1s)));
            CHECK(connection->Wake() == std::errc::operation_canceled);
            CHECK(static_cast<bool>(connection->Write("late", Until(1s))));
        });
        Get(repeated);
        OnOwner(fixture.loop, [&fixture, connection] {
            CHECK(!fixture.bridge->CloseNative(connection->Id(), {}));
        });
        close_gate->Release();
        CHECK(fixture.bridge->WaitDrained(Until(5s)));
        CheckConserved(fixture, session, 0);
        CHECK(session->GetCounters().traffic_dispatched == 0);
    });
}

static std::string ReceiveExact(NativeSocket& socket, std::size_t size) {
    std::string result;
    const auto deadline = Until(5s);
    while (result.size() < size) {
        CHECK(Clock::now() < deadline);
        pollfd pfd{socket.Fd(), POLLIN, 0};
        CHECK(::poll(&pfd, 1, 5000) == 1);
        char data[64];
        const auto received = socket.Receive(data, std::min(sizeof(data), size - result.size()));
        CHECK(!received.error && received.bytes != 0);
        result.append(data, received.bytes);
    }
    return result;
}

static void ScenarioHandlerException() {
    RunContract([](ContractFixture& fixture) {
        struct Sentinel {};
        for (int stage = 0; stage < 3; ++stage) {
            auto gate = fixture.Gate();
            auto opened = gate->registered.get_future();
            auto thrown = std::make_shared<std::atomic<int>>(0);
            auto handler = std::make_shared<ProbeHandler>();
            handler->on_open = [gate, thrown, stage](ICoConnection&) {
                gate->Wait();
                if (stage == 0) { ++*thrown; throw Sentinel{}; }
            };
            handler->on_traffic = [thrown, stage](ICoConnection& connection, bool wake) {
                CHECK(!wake);
                const auto view = connection.Peek();
                if (view.empty()) { return; }
                CHECK(stage == 1 && std::string_view("THROW").substr(0, view.size()) == view);
                CHECK(connection.InboundBuffered() == view.size());
                // Leave the nonempty snapshot unconsumed: exception cleanup owns it.
                ++*thrown;
                throw Sentinel{};
            };
            handler->on_close = [thrown, stage](ICoConnection& connection, const std::error_code&) {
                CHECK(!connection.Close(CloseMode::Block, {}, Until(1s)));
                if (stage == 2) { ++*thrown; throw Sentinel{}; }
            };
            auto connection = fixture.Connect(handler);
            Get(opened);
            auto session = fixture.bridge->SessionOf(connection->Id());
            CHECK(session);
            if (stage == 1) { CHECK(ClientSend(fixture.clients.back(), "THROW")); }
            gate->Release();
            if (stage == 2) {
                auto close = Task([connection] { CHECK(!connection->Close(CloseMode::Block, {}, Until(5s))); });
                Get(close);
            }
            CHECK(fixture.bridge->WaitDrained(Until(5s)));
            CHECK(thrown->load() == 1);
            CheckConserved(fixture, session, 1);
            if (stage == 1) { CHECK(session->GetCounters().traffic_dispatched >= 1); }
            const auto terminal = session->GetCounters();
            auto wake = Task([connection] { CHECK(connection->Wake() == std::errc::operation_canceled); });
            Get(wake);
            OnOwner(fixture.loop, [] {});
            Get(Task([] {}));
            CHECK(session->GetCounters().traffic_dispatched == terminal.traffic_dispatched);
            CHECK(session->GetCounters().onclose_entered == terminal.onclose_entered);

            // A separate normal connection still performs real TCP read/write.
            auto echo = std::make_shared<ProbeHandler>();
            auto echoed = std::make_shared<std::promise<void>>();
            auto echo_future = echoed->get_future();
            auto echo_bytes = std::make_shared<std::string>();
            echo->on_traffic = [echoed, echo_bytes](ICoConnection& normal, bool wake_event) {
                CHECK(!wake_event);
                const auto view = normal.Peek();
                if (view.empty()) { return; }
                echo_bytes->append(view.data(), view.size());
                CHECK(std::string("OK").compare(0, echo_bytes->size(), *echo_bytes) == 0);
                CHECK(!normal.Discard(view.size()));
                if (*echo_bytes != "OK") { return; }
                CHECK(!normal.Write("OK", Until(3s)));
                echoed->set_value();
            };
            auto normal = fixture.Connect(echo);
            CHECK(ClientSend(fixture.clients.back(), "OK"));
            Get(echo_future);
            CHECK(ReceiveExact(fixture.clients.back(), 2) == "OK");
            auto normal_close = Task([normal] { CHECK(!normal->Close(CloseMode::Block, {}, Until(3s))); });
            Get(normal_close);
            CHECK(fixture.bridge->WaitDrained(Until(5s)));
            CheckConserved(fixture, fixture.bridge->SessionOf(normal->Id()), 0);
        }
    });
}

static void RejectReader(ICoConnection& connection) {
    bool buffered = false;
    bool peek = false;
    try { (void)connection.InboundBuffered(); }
    catch (const std::system_error& error) { CHECK(error.code() == std::errc::operation_not_permitted); buffered = true; }
    try { (void)connection.Peek(); }
    catch (const std::system_error& error) { CHECK(error.code() == std::errc::operation_not_permitted); peek = true; }
    CHECK(buffered && peek);
    CHECK(connection.Discard(0) == std::errc::operation_not_permitted);
}

static void ScenarioContextReject() {
    RunContract([](ContractFixture& fixture) {
        auto gate = fixture.Gate();
        auto opened = gate->registered.get_future();
        auto handler = std::make_shared<ProbeHandler>();
        auto woke = std::make_shared<std::promise<void>>();
        auto wake_future = woke->get_future();
        auto data = std::make_shared<std::promise<void>>();
        auto data_future = data->get_future();
        handler->on_open = [gate](ICoConnection& connection) {
            CHECK(connection.GetEnvironment().CanSuspend());
            CHECK(connection.InboundBuffered() == 0 && connection.Peek().empty());
            CHECK(!connection.Discard(0));
            gate->Wait();
        };
        auto valid_bytes = std::make_shared<std::string>();
        handler->on_traffic = [woke, data, valid_bytes](ICoConnection& connection, bool wake) {
            CHECK(connection.GetEnvironment().CanSuspend());
            if (wake) {
                CHECK(connection.InboundBuffered() == 0 && connection.Peek().empty());
                CHECK(!connection.Discard(0));
                woke->set_value();
                return;
            }
            const auto view = connection.Peek();
            if (view.empty()) { return; }
            valid_bytes->append(view.data(), view.size());
            CHECK(std::string("VALID").compare(0, valid_bytes->size(), *valid_bytes) == 0);
            CHECK(!connection.Discard(view.size()));
            if (*valid_bytes != "VALID") { return; }
            CHECK(!connection.Write("VALID", Until(3s)));
            data->set_value();
        };
        auto connection = fixture.Connect(handler);
        Get(opened);
        OnOwner(fixture.loop, [] {});
        auto session = fixture.bridge->SessionOf(connection->Id());
        CHECK(session);
        const auto identity = connection->GetObjectInfo().id;
        const auto submitted = fixture.bridge->GetStats().ops_submitted.load();
        const auto executed = fixture.bridge->GetStats().ops_executed.load();
        const auto registered = fixture.bridge->GetStats().sessions_registered.load();
        const auto before = session->GetCounters();
        auto physical_before_result = std::make_shared<StreamConnection::Stats>();
        OnOwner(fixture.loop, [physical_before_result, session] {
            *physical_before_result = session->ConnStatsOwner();
        });
        const auto physical_before = *physical_before_result;
        auto reject = [connection] {
            CHECK(bbt::coroutine::GetLocalCoroutineId() == 0);
            CHECK(!connection->GetEnvironment().CanSuspend());
            CHECK(connection->RequireCoroutine() == std::errc::operation_not_permitted);
            CHECK(connection->Write("ILLEGAL", Until(1s)) == std::errc::operation_not_permitted);
            CHECK(connection->Wake() == std::errc::operation_not_permitted);
            CHECK(connection->Close(CloseMode::Block, {}, Until(1s)) == std::errc::operation_not_permitted);
            CHECK(connection->Close(CloseMode::Unblock, {}, Until(1s)) == std::errc::operation_not_permitted);
            RejectReader(*connection);
        };
        reject(); // ordinary thread
        OnOwner(fixture.loop, reject);
        // Foundation context_no_create baseline only: TLS enabled without a
        // Processer. Post-coroutine Processer-idle coverage remains open; the
        // fixed U1 exposes no stable post-return callback for that state.
        std::exception_ptr idle_error;
        std::thread idle([&idle_error, reject] {
            try {
                auto* tls = bbt::coroutine::detail::LocalThread::GetTLSInst().get();
                tls->SetEnableUseCo(true);
                CHECK(tls->EnableUseCo());
                CHECK(!bbt::coroutine::detail::Processer::TryGetLocalProcesser());
                reject();
                CHECK(!bbt::coroutine::detail::Processer::TryGetLocalProcesser());
            } catch (...) { idle_error = std::current_exception(); }
        });
        idle.join();
        if (idle_error) { std::rethrow_exception(idle_error); }
        CHECK(fixture.bridge->GetStats().ops_submitted.load() == submitted);
        CHECK(fixture.bridge->GetStats().ops_executed.load() == executed);
        CHECK(fixture.bridge->GetStats().sessions_registered.load() == registered);
        CHECK(fixture.bridge->SessionCount() == 1 && connection->GetObjectInfo().id == identity);
        const auto after = session->GetCounters();
        CHECK(after.wake_calls == before.wake_calls && after.wake_rejected_sealed == before.wake_rejected_sealed);
        CHECK(after.spawns == before.spawns && after.handler_exceptions == 0);
        CHECK(!session->SnapshotState().sealed && !session->SnapshotState().physical_closed);
        auto physical_after_result = std::make_shared<StreamConnection::Stats>();
        OnOwner(fixture.loop, [physical_after_result, session] {
            *physical_after_result = session->ConnStatsOwner();
        });
        const auto physical_after = *physical_after_result;
        CHECK(physical_after.physical_closes == physical_before.physical_closes);
        CHECK(physical_after.snapshots_taken == physical_before.snapshots_taken);
        CHECK(physical_after.snapshots_returned == physical_before.snapshots_returned);
        auto managed = Task([connection] {
            CHECK(connection->GetEnvironment().CanSuspend());
            RejectReader(*connection);
            CHECK(!connection->Wake()); // non-event coroutine is a legal command domain
        });
        Get(managed);
        gate->Release();
        Get(wake_future);
        CHECK(ClientSend(fixture.clients.back(), "VALID"));
        Get(data_future);
        CHECK(ReceiveExact(fixture.clients.back(), 5) == "VALID");
        auto close = Task([connection] { CHECK(!connection->Close(CloseMode::Block, {}, Until(3s))); });
        Get(close);
        CHECK(fixture.bridge->WaitDrained(Until(5s)));
        CheckConserved(fixture, session, 0);
        Log("context_domains_checked", connection->Id(),
            "ordinary owner TLS-enabled-no-Processer; managed Wake legal Reader rejected; post-co-idle=UNCOVERED");
    });
}

// 自有 gate：登记信号确认 owner 已进入；失败清理先释放，析构不等待。
static std::future<void> HoldContractOwner(ContractFixture& fixture) {
    auto release = std::make_shared<std::promise<void>>();
    auto released = release->get_future().share();
    auto once = std::make_shared<std::atomic<bool>>(false);
    fixture.releases.push_back([release, once] {
        if (!once->exchange(true)) { release->set_value(); }
    });
    auto entered = std::make_shared<std::promise<void>>();
    auto entering = entered->get_future();
    auto done = std::make_shared<std::promise<void>>();
    auto finished = done->get_future();
    CHECK(fixture.loop.Submit([released, entered, done] {
        entered->set_value();
        const bool ready = released.wait_for(8s) == std::future_status::ready;
        done->set_value();
        CHECK(ready);
    }) == SubmitStatus::Accepted);
    Get(entering);
    return finished;
}

static void ScenarioQueueFull() {
    constexpr std::size_t capacity = 4;
    ContractFixture fixture(capacity);
    fixture.Start();
    try {
        auto opened = std::make_shared<std::promise<void>>();
        auto opening = opened->get_future();
        auto woke = std::make_shared<std::promise<void>>();
        auto waking = woke->get_future();
        auto echoed = std::make_shared<std::promise<void>>();
        auto echoing = echoed->get_future();
        auto bytes = std::make_shared<std::string>();
        auto handler = std::make_shared<ProbeHandler>();
        handler->on_open = [opened](ICoConnection&) { opened->set_value(); };
        handler->on_traffic = [woke, echoed, bytes](ICoConnection& connection, bool wake) {
            if (wake) { woke->set_value(); return; }
            const auto view = connection.Peek();
            if (view.empty()) { return; }
            bytes->append(view.data(), view.size());
            CHECK(std::string("OK").compare(0, bytes->size(), *bytes) == 0);
            CHECK(!connection.Discard(view.size()));
            if (*bytes == "OK") {
                CHECK(!connection.Write("OK", Until(3s)));
                echoed->set_value();
            }
        };
        auto connection = fixture.Connect(handler);
        Get(opening);
        Get(Task([] {})); // 单 Processer barrier：OnOpen 已真正返回。
        OnOwner(fixture.loop, [] {});
        auto session = fixture.bridge->SessionOf(connection->Id());
        CHECK(session && !session->SnapshotState().busy);
        const auto physical_before = OwnerStats(fixture.loop, session);
        const auto before = session->GetCounters();
        const auto submitted = fixture.bridge->GetStats().ops_submitted.load();
        CHECK(submitted == fixture.bridge->GetStats().ops_executed.load());
        auto owner_done = HoldContractOwner(fixture);
        auto filled_executed = std::make_shared<std::atomic<std::size_t>>(0);
        std::size_t accepted = 0;
        SubmitStatus status = SubmitStatus::Accepted;
        for (std::size_t i = 0; i <= capacity; ++i) {
            status = fixture.loop.Submit([filled_executed] { ++*filled_executed; });
            if (status != SubmitStatus::Accepted) { break; }
            ++accepted;
        }
        CHECK(accepted == capacity && status == SubmitStatus::Full);
        CHECK(filled_executed->load() == 0);
        Get(Task([connection] {
            CHECK(connection->Wake() == std::errc::resource_unavailable_try_again);
            CHECK(connection->Close(CloseMode::Unblock, {}, Until(1s))
                == std::errc::resource_unavailable_try_again);
        }));
        const auto rollback = session->SnapshotState();
        CHECK(!rollback.sealed && !rollback.physical_closed && !rollback.close_finished);
        CHECK(session->GetCounters().wake_calls == before.wake_calls + 1);
        CHECK(session->GetCounters().traffic_dispatched == before.traffic_dispatched);
        Log("queue_full_rollback", connection->Id(), "Wake/Unblock=try_again sealed=false physical=false");
        Get(Task([connection] {
            const auto error = connection->Write("REJECT", Until(1s));
            Log("queue_full_write", connection->Id(), error.category().name()
                + std::string(":") + std::to_string(error.value()) + " " + error.message());
            // CoIoOperation Full 明确映射 no_buffer_space；不能把拒绝冒充 timeout。
            CHECK(error == std::errc::no_buffer_space);
        }));
        CHECK(owner_done.wait_for(0ms) != std::future_status::ready);
        CHECK(filled_executed->load() == 0);
        CHECK(fixture.bridge->GetStats().ops_submitted.load() == submitted + 3);
        CHECK(fixture.bridge->GetStats().ops_executed.load() == submitted + 3);
        CHECK(session->GetCounters().op_timeouts == before.op_timeouts);
        pollfd empty{fixture.clients.back().Fd(), POLLIN, 0};
        CHECK(::poll(&empty, 1, 0) == 0); // 被拒绝的 Write 未向真实 peer 发字节。
        fixture.releases.back()();
        Get(owner_done);
        OnOwner(fixture.loop, [] {});
        CHECK(filled_executed->load() == accepted);
        const auto physical_after = OwnerStats(fixture.loop, session);
        CHECK(physical_after.physical_closes == physical_before.physical_closes);
        CHECK(physical_after.read_bytes == physical_before.read_bytes);
        CHECK(physical_after.snapshots_taken == physical_before.snapshots_taken);
        Get(Task([connection] { CHECK(!connection->Wake()); }));
        Get(waking);
        CHECK(ClientSend(fixture.clients.back(), "OK"));
        Get(echoing);
        CHECK(ReceiveExact(fixture.clients.back(), 2) == "OK");
        Get(Task([connection] { CHECK(!connection->Close(CloseMode::Block, {}, Until(3s))); }));
        CHECK(fixture.bridge->WaitDrained(Until(5s)));
        CheckConserved(fixture, session, 0);
        Log("queue_full_recovered", connection->Id(), "TCP=OK Wake=OK close/drain before stop");
    } catch (...) {
        const auto failure = std::current_exception();
        try { fixture.Finish(); }
        catch (const std::exception& cleanup) { std::cerr << "CLEANUP FAILED: " << cleanup.what() << '\n'; }
        std::rethrow_exception(failure);
    }
    fixture.Finish();
    // 仅 driver 封口证据，不冒充未关闭 session 的 owner-Sealed Wake 路径。
    CHECK(fixture.loop.Submit([] {}) == SubmitStatus::Sealed);
}

static std::size_t OpenFdCount() {
    auto* directory = ::opendir("/proc/self/fd");
    CHECK(directory);
    std::size_t count = 0;
    while (const auto* entry = ::readdir(directory)) {
        if (entry->d_name[0] != '.') { ++count; }
    }
    CHECK(::closedir(directory) == 0);
    CHECK(count > 0);
    return count - 1; // 排除已关闭的本次目录 fd。
}

static void ScenarioG1Drain() {
    WarmRuntime();
    const auto fd_before = OpenFdCount();
    std::weak_ptr<CoTcpConnection> weak_connection;
    std::weak_ptr<ConnectionSession> weak_session;
    {
        ContractFixture fixture;
        fixture.Start();
        auto close_gate = fixture.Gate();
        auto closing = close_gate->registered.get_future();
        auto received = std::make_shared<std::promise<void>>();
        auto receiving = received->get_future();
        auto bytes = std::make_shared<std::string>();
        auto handler = std::make_shared<ProbeHandler>();
        handler->on_traffic = [received, bytes](ICoConnection& connection, bool wake) {
            CHECK(!wake);
            const auto view = connection.Peek();
            if (view.empty()) { return; }
            bytes->append(view.data(), view.size());
            CHECK(std::string("G1").compare(0, bytes->size(), *bytes) == 0);
            CHECK(!connection.Discard(view.size()));
            if (*bytes == "G1") { received->set_value(); }
        };
        handler->on_close = [close_gate](ICoConnection&, const std::error_code&) { close_gate->Wait(); };
        try {
            auto connection = fixture.Connect(handler);
            auto session = fixture.bridge->SessionOf(connection->Id());
            CHECK(session);
            weak_connection = connection;
            weak_session = session;
            CHECK(ClientSend(fixture.clients.back(), "G1"));
            Get(receiving);
            Get(Task([connection] { CHECK(!connection->Close(CloseMode::Block, {}, Until(3s))); }));
            Get(closing); // on_registered：OnClose 确实已挂在 gate 上。
            CHECK(session->SnapshotState().physical_closed);
            CHECK(!session->SnapshotState().close_finished);
            CHECK(session->GetCounters().onclose_entered == 1);
            CHECK(session->GetCounters().onclose_returned == 0);
            CHECK(!session->DrainedSnapshot() && !fixture.bridge->Drained());
            CHECK(!fixture.bridge->WaitDrained(Until(5ms)));
            const auto held = OwnerStats(fixture.loop, session);
            CHECK(held.physical_closes == 1);
            CHECK(held.snapshots_taken > 0 && held.snapshots_taken == held.snapshots_returned);
            pollfd eof{fixture.clients.back().Fd(), POLLIN, 0};
            CHECK(::poll(&eof, 1, 5000) == 1);
            char byte{};
            const auto end = fixture.clients.back().Receive(&byte, 1);
            CHECK(!end.error && end.bytes == 0);
            Log("private_g1_gate_held", connection->Id(), "physical=1 EOF=true drain=false OnClose-return=0");
            close_gate->Release();
            CHECK(fixture.bridge->WaitDrained(Until(5s)));
            CHECK(session->DrainedSnapshot() && fixture.bridge->Drained());
            CheckConserved(fixture, session, 0); // owner stats 必须在 Join 前。
            const auto physical = OwnerStats(fixture.loop, session);
            CHECK(physical.read_bytes == 2 && physical.snapshot_consumed_bytes == 2);
            auto owner_tid = std::make_shared<long>(0);
            OnOwner(fixture.loop, [owner_tid] { *owner_tid = ::syscall(SYS_gettid); });
            CHECK(*owner_tid > 0);
            // 先释放外部强引用；registry 仍拥有 shell，不在此要求 weak expired。
            connection.reset();
            session.reset();
            handler.reset();
            CHECK(!weak_connection.expired() && !weak_session.expired());
            fixture.Finish(); // drain → Stop/Join → 外部 connections.clear → bridge.reset
            CHECK(::access(("/proc/self/task/" + std::to_string(*owner_tid)).c_str(), F_OK) == -1);
            CHECK(weak_connection.expired() && weak_session.expired());
            Log("private_g1_joined", 0, "owner thread reclaimed registry released weak expired");
        } catch (...) {
            const auto failure = std::current_exception();
            if (fixture.bridge) {
                try { fixture.Finish(); }
                catch (const std::exception& cleanup) { std::cerr << "CLEANUP FAILED: " << cleanup.what() << '\n'; }
            }
            std::rethrow_exception(failure);
        }
    } // EventLoop/io_context 与自有 TCP 资源已全部析构；runtime 是进程寿命。
    const auto fd_after = OpenFdCount();
    Log("private_g1_resources", 0, "fd_before=" + std::to_string(fd_before)
        + " fd_after=" + std::to_string(fd_after));
    CHECK(fd_after == fd_before);
}

#include "ProcessRuntimeTestMain.hpp"

static int RunContractCases(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "usage: Test_net_co_bridge <case>\n";
        return 2;
    }
    const std::string case_name = argv[1];
    try {
        if (case_name == "co_bridge_lifecycle") { ScenarioLifecycle(); }
        else if (case_name == "co_bridge_close") { ScenarioClose(); }
        else if (case_name == "co_bridge_handler_exception") { ScenarioHandlerException(); }
        else if (case_name == "co_bridge_context_reject") { ScenarioContextReject(); }
        else if (case_name == "co_bridge_queue_full") { ScenarioQueueFull(); }
        else if (case_name == "co_bridge_g1_drain") { ScenarioG1Drain(); }
        else if (case_name == "co_bridge_g6_late_resource") { ScenarioLateResource(); }
        else {
            std::cerr << "unknown case: " << case_name << "\n";
            return 2;
        }
        std::cout << "CASE OK " << case_name << " asserts=" << AssertCounter().load() << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cout << "RUN FAILED: " << error.what() << '\n';
        return 1;
    }
}

int main(int argc, char** argv) {
    // 测试进程退出边界：局部 RAII 收口 → 刷新输出 → _Exit，见
    // ProcessRuntimeTestMain.hpp 头注（静态析构竞态为已知限制，未修复）。
    return bbt_net_test::RunTestProcessExit([&] { return RunContractCases(argc, argv); });
}
