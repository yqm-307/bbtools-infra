// B4 契约聚焦测试：针对父级处置中的真实根因做可判别回归。
//
// 覆盖：
//   ring_alloc_no_consume     复制/分配失败不前移消费游标
//   ring_release_notify       RAII/空读释放唤醒已登记 Busy 等待者
//   hub_early_notify_retained 挂起前 Notify 未交付时 weak 项必须保留
//   write_total_ops_bound     普通+零长+待派发合计受 max_ops
//   write_query_monotonic     Query 与 writer 共锁、sent 单调、终态一次
//   onwrite_throw_continues   用户 OnWrite 抛出后其余终态与 OnClose 仍兑现
//   wait_accepted_existing_out 既有 out 有效不算本轮成功
//   run_preconditions         未初始化/未声明独立推进零副作用拒绝
//   engine_driver_failed      Engine 级 Failed 同步解绑/冻结/不伪回调
//   invalid_options_rejected  保留字段非法值在绑定前拒绝（历史 ET/ticker 字段已删除）
//   handoff_full_resume       容量 1 下先 reserve 再 accept、满后恢复
//   lb_subs                   LB 分配真实落到两个 sub owner
//   fairness                  热连接 + 第二连接 + 业务协程同时进展

#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/net/ConnectionResult.hpp>
#include <bbt/infra/net/WriteOperation.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/RawHandler.hpp>
#include <bbt/infra/net/detail/CoInternal.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>
#include <bbt/infra/net/detail/WakeHub.hpp>
#include <bbt/infra/net/detail/EngineTestAccess.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <execinfo.h>
#include <filesystem>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <new>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <poll.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>

using namespace bbt::infra::net;
using namespace std::chrono_literals;

// ---- 分配失败注入（仅本可执行文件；thread_local 不影响 runtime 线程）----
namespace {
thread_local bool g_fail_alloc = false;
std::atomic<int> g_eagain_sendmsg_fd{-1};
}

// 仅本测试目标：一次 EAGAIN 强制经过真实 Asio write readiness，其后透传真实
// sendmsg；公共 RequestClose 与 readiness 的交错不依赖内核缓冲区填满速度。
extern "C" ssize_t __real_sendmsg(int fd, const msghdr* message, int flags);
extern "C" ssize_t __wrap_sendmsg(int fd, const msghdr* message, int flags) {
    int expected = fd;
    if (g_eagain_sendmsg_fd.compare_exchange_strong(expected, -1)) {
        errno = EAGAIN;
        return -1;
    }
    return __real_sendmsg(fd, message, flags);
}

void* operator new(std::size_t size) {
    if (g_fail_alloc) {
        throw std::bad_alloc();
    }
    void* memory = std::malloc(size == 0 ? 1 : size);
    if (memory == nullptr) {
        throw std::bad_alloc();
    }
    return memory;
}
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete[](void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void* memory, std::size_t) noexcept { std::free(memory); }

static void Check(bool value, const char* expression, int line) {
    if (!value) {
        throw std::runtime_error("line " + std::to_string(line) + ": " + expression);
    }
}
#define CHECK(expression) Check(static_cast<bool>(expression), #expression, __LINE__)

static bbt::coroutine::WaitOptions Until(std::chrono::milliseconds duration) {
    return {std::chrono::steady_clock::now() + duration};
}

static bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

static void StartRuntime(std::size_t workers) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = workers;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start();
}

static std::future<void> Task(std::function<void()> callback) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    g_scheduler->RegistCoroutineTask(65536, [promise, callback] {
        try {
            callback();
            promise->set_value();
        } catch (...) {
            promise->set_exception(std::current_exception());
        }
    }, "net.b4");
    return future;
}

static void Get(std::future<void>& future) {
    CHECK(future.wait_for(5s) == std::future_status::ready);
    future.get();
}

static Address Loopback(int port) {
    std::error_code error;
    Address address = Address::Ip("127.0.0.1", port, error);
    CHECK(!error);
    return address;
}

struct EngineRunner {
    Engine& engine;
    std::error_code error;
    std::thread thread;

    EngineRunner(Engine& target, EngineConfig config) : engine(target) {
        thread = std::thread([this, config]() mutable { error = engine.Run(config); });
    }
    bool WaitForReturn(std::chrono::milliseconds timeout) {
        return WaitUntil([&] { return engine.Phase() == EnginePhase::Stopped; }, timeout);
    }
    void CloseAndJoin() {
        engine.RequestClose();
        if (thread.joinable()) {
            thread.join();
        }
    }
    ~EngineRunner() { CloseAndJoin(); }
};

static NativeSocket ConnectClient(const Address& address) {
    std::error_code error;
    NativeSocket client = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    CHECK(!error);
    const auto connect_error = client.Connect(address);
    CHECK(!connect_error || connect_error.value() == EINPROGRESS
          || connect_error.value() == EWOULDBLOCK);
    pollfd descriptor{client.Fd(), POLLOUT, 0};
    CHECK(::poll(&descriptor, 1, 3000) > 0);
    CHECK(!client.PendingError());
    return client;
}

static bool SendAll(NativeSocket& client, const std::string& payload,
                    std::chrono::milliseconds timeout) {
    std::size_t sent = 0;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (sent < payload.size()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        const auto result = client.Send(payload.data() + sent, payload.size() - sent);
        if (result.bytes > 0) {
            sent += result.bytes;
            continue;
        }
        if (result.error.value() == EAGAIN || result.error.value() == EWOULDBLOCK) {
            pollfd descriptor{client.Fd(), POLLOUT, 0};
            (void)::poll(&descriptor, 1, 10);
            continue;
        }
        return false;
    }
    return true;
}

static bool RecvExact(NativeSocket& client, std::size_t size, std::string& out,
                      std::chrono::milliseconds timeout) {
    out.clear();
    char buffer[512];
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (out.size() < size) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        const auto result = client.Receive(buffer, sizeof(buffer));
        if (result.bytes > 0) {
            out.append(buffer, result.bytes);
            continue;
        }
        if (result.error.value() == EAGAIN || result.error.value() == EWOULDBLOCK) {
            pollfd descriptor{client.Fd(), POLLIN, 0};
            (void)::poll(&descriptor, 1, 10);
            continue;
        }
        return false;
    }
    return true;
}

struct EchoHandler : RawHandler {
    std::atomic<int> accepted{0};
    std::atomic<int> closed{0};
    std::mutex mutex;
    std::set<std::uint64_t> owners;
    std::shared_ptr<Connection> stored;

    void OnAccept(Connection connection) override {
        accepted.fetch_add(1);
        if (auto processer = bbt::coroutine::detail::Processer::TryGetLocalProcesser()) {
            std::lock_guard<std::mutex> lock(mutex);
            owners.insert(processer->GetId());
        }
        std::lock_guard<std::mutex> lock(mutex);
        stored = std::make_shared<Connection>(connection);
    }
    void OnRead(Connection connection) override {
        std::string data;
        if (connection.TryRead(data).status == ReadStatus::Ok && !data.empty()) {
            WriteOperation operation;
            connection.AsyncWrite(data, operation);
        }
    }
    void OnWrite(Connection, WriteResult) override {}
    void OnClose(Connection, std::error_code) override { closed.fetch_add(1); }
};

struct NullHandler : RawHandler {};

// 只观察不消费：用于 staged/尾部可读验证。
struct StoreHandler : RawHandler {
    std::atomic<int> accepted{0};
    std::atomic<int> reads{0};
    std::atomic<int> closed{0};
    std::mutex mutex;
    std::shared_ptr<Connection> stored;

    void OnAccept(Connection connection) override {
        accepted.fetch_add(1);
        std::lock_guard<std::mutex> lock(mutex);
        stored = std::make_shared<Connection>(connection);
    }
    void OnRead(Connection) override { reads.fetch_add(1); }
    void OnClose(Connection, std::error_code) override { closed.fetch_add(1); }
};


static std::error_code TryAccept(ConnectionState& state, std::string data,
                                 WriteOperation& out) {
    std::shared_ptr<WriteOpRecord> record;
    const auto error = state.AcceptWrite(std::move(data), record);
    if (!error) {
        out = ConnectionTestAccess::MakeWrite(std::move(record));
    }
    return error;
}
static EngineConfig ServerConfig(const Address& listen, std::size_t subs = 1) {
    EngineConfig config;
    config.listen_addresses = {listen};
    config.runtime_progress = RuntimeProgress::Independent;
    config.options.num_event_loop = static_cast<std::int64_t>(subs);
    return config;
}

// ---------------- cases ----------------

static void RingAllocNoConsume() {
    ReceiveRing ring(256);
    const std::string payload(64, 'z');
    CHECK(ring.TryAppend(payload).status == ReadStatus::Ok);
    CHECK(ring.Size() == payload.size());
    std::string out;
    g_fail_alloc = true;
    auto result = ring.TryRead(out);
    g_fail_alloc = false;
    CHECK(result.status == ReadStatus::Error);
    CHECK(ring.Size() == payload.size());  // 未消费、未丢字节
    std::string retry;
    CHECK(ring.TryRead(retry).status == ReadStatus::Ok);
    CHECK(retry == payload);
    CHECK(ring.Size() == 0);
    std::cout << "ring_alloc_no_consume fail_status=Error retry=ok\n";
}

static void RingReleaseNotify() {
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(1, 4096, 1024, 4, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);
    CHECK(state->ring.TryAppend("release!").status == ReadStatus::Ok);
    auto held = ConnectionTestAccess::HoldForTest(state);
    CHECK(bool(held));
    std::atomic<int> result{-1};
    auto task = Task([&] {
        CoTcpConnection conn{connection};
        const auto read = conn.Read(Until(500ms));
        if (read.state == ReadState::Data && read.data.CopyTo() == "release!") {
            result.store(0);
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    ConnectionTestAccess::Reset(held);  // 锁外 NotifyRelease
    Get(task);
    CHECK(result.load() == 0);
    std::cout << "ring_release_notify busy_wait_woken=ok\n";
}

static void HubEarlyNotifyRetained() {
    StartRuntime(1);
    WakeHub hub;
    auto waiter = hub.Register();
    CHECK(waiter != nullptr);
    hub.NotifyAll();  // 等待前通知：无法交付，但 weak 项必须保留
    std::atomic<int> result{-1};
    auto task = Task([&] {
        const auto status = waiter->WaitWithCallback(Until(300ms), [] { return true; });
        if (status == bbt::coroutine::WaitStatus::Completed) {
            result.store(0);
        } else if (status == bbt::coroutine::WaitStatus::TimedOut) {
            result.store(1);
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    hub.NotifyAll();  // 真正的完成事件
    Get(task);
    CHECK(result.load() == 0);
    std::cout << "hub_early_notify_retained completed=ok\n";
}

static void WriteTotalOpsBound() {
    ConnectionState state(1, 4096, 4096, 4, nullptr);
    WriteOperation a, b, c, d, e;
    CHECK(!TryAccept(state, "aa", a));
    CHECK(!TryAccept(state, "", b));   // 零长终态
    CHECK(!TryAccept(state, "cc", c));
    CHECK(!TryAccept(state, "", d));
    CHECK(TryAccept(state, "ee", e) == std::make_error_code(std::errc::no_buffer_space));
    CHECK(state.DrainTerminal().size() == 2);
    CHECK(!TryAccept(state, "", e));   // 派发后释放责任
    std::cout << "write_total_ops_bound normal+zero+pending<=max_ops\n";
}

static void WriteQueryMonotonic() {
    ConnectionState state(1, 4096, 8192, 16, nullptr);
    WriteOperation operation;
    CHECK(!TryAccept(state, std::string(4096, 'x'), operation));
    std::atomic<bool> stop{false};
    auto sink = [](const std::string_view* views, std::size_t, std::size_t budget) -> ssize_t {
        return static_cast<ssize_t>(std::min(views[0].size(), budget));
    };
    std::thread pump([&] {
        while (!stop.load()) {
            state.PumpWrites(1, 512, sink);
        }
        while (operation.Query().phase == WritePhase::InFlight) {
            state.PumpWrites(1, 512, sink);
        }
    });
    std::size_t previous = 0;
    for (int i = 0; i < 2000; ++i) {
        const auto outcome = operation.Query();
        CHECK(outcome.sent >= previous);
        previous = outcome.sent;
        if (outcome.phase == WritePhase::Done) {
            break;
        }
    }
    stop.store(true);
    pump.join();
    const auto final = operation.Query();
    CHECK(final.phase == WritePhase::Done && final.sent == 4096);
    CHECK(state.DrainTerminal().size() == 1);
    std::cout << "write_query_monotonic sent=" << final.sent << " terminal_once=ok\n";
}

static void OnWriteThrowContinues() {
    StartRuntime(2);
    struct Handler : RawHandler {
        std::atomic<int> accepted{0};
        std::atomic<int> writes{0};
        std::atomic<int> closes{0};
        std::mutex mutex;
        std::shared_ptr<Connection> stored;
        void OnAccept(Connection connection) override {
            accepted.fetch_add(1);
            {
                std::lock_guard<std::mutex> lock(mutex);
                stored = std::make_shared<Connection>(connection);
            }
            WriteOperation first;
            WriteOperation second;
            (void)connection.AsyncWrite("first", first);
            (void)connection.AsyncWrite("second", second);
        }
        void OnWrite(Connection, WriteResult) override {
            const int index = writes.fetch_add(1);
            if (index == 0) {
                throw std::runtime_error("user OnWrite throw");
            }
        }
        void OnClose(Connection, std::error_code) override { closes.fetch_add(1); }
    };
    auto handler = std::make_shared<Handler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    auto client = ConnectClient(addresses[0]);
    CHECK(WaitUntil([&] { return handler->accepted.load() == 1; }, 3000ms));
    CHECK(WaitUntil([&] { return handler->writes.load() >= 2; }, 3000ms));
    std::shared_ptr<Connection> stored;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        stored = handler->stored;
    }
    CHECK(stored && stored->Valid());
    stored->RequestClose();
    CHECK(WaitUntil([&] { return handler->closes.load() == 1; }, 3000ms));
    CHECK(!client.Close());
    runner.CloseAndJoin();
    CHECK(!runner.error);
    CHECK(engine.HandlerErrorCount() >= 1);
    std::cout << "onwrite_throw_continues writes=" << handler->writes.load()
              << " close=" << handler->closes.load() << "\n";
}

static void WaitAcceptedExistingOut() {
    StartRuntime(2);
    auto handler = std::make_shared<NullHandler>();
    Engine engine(handler);
    CHECK(!engine.EnableAcceptHandoff(4));
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    auto client = ConnectClient(addresses[0]);

    std::atomic<int> result{-1};
    auto task = Task([&] {
        auto res = engine.CoAccept(Until(3000ms));
        if (res.error || !res.connection.Valid()) { result.store(1); return; }
        Connection out = res.connection.Raw();
        // 无更多 handoff：既有 out 有效不能算本轮成功。
        // 内部等待步骤（公共面只留 CoAccept）：既有 out 有效不算本轮成功。
        const auto second = CoAccess::WaitAccepted(engine, out, Until(40ms));
        if (second == std::make_error_code(std::errc::timed_out)) {
            result.store(0);
        } else {
            result.store(2);
        }
    });
    Get(task);
    CHECK(result.load() == 0);
    CHECK(!client.Close());
    runner.CloseAndJoin();
    std::cout << "wait_accepted_existing_out timed_out=ok\n";
}

static void RunPreconditions() {
    // 未初始化：零副作用拒绝。
    {
        Engine engine(std::make_shared<NullHandler>());
        EngineConfig config;
        config.runtime_progress = RuntimeProgress::Independent;
        const auto error = engine.Run(config);
        CHECK(error == std::make_error_code(std::errc::operation_not_supported));
        CHECK(engine.Phase() == EnginePhase::Stopped);
    }
    StartRuntime(2);
    // 未声明独立推进：绑定/建 socket 前拒绝。
    {
        Engine engine(std::make_shared<NullHandler>());
        EngineConfig config;
        config.listen_addresses = {Loopback(0)};
        config.runtime_progress = RuntimeProgress::Unspecified;
        const auto error = engine.Run(config);
        CHECK(error == std::make_error_code(std::errc::operation_not_permitted));
        CHECK(engine.Phase() == EnginePhase::Stopped);
    }
    // 合法声明后可运行。
    {
        Engine engine(std::make_shared<NullHandler>());
        EngineRunner runner(engine, ServerConfig(Loopback(0)));
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
        runner.CloseAndJoin();
        CHECK(!runner.error);
    }
    std::cout << "run_preconditions not_initialized/progress_required/ok\n";
}

static void EngineDriverFailed() {
    StartRuntime(2);
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    auto client = ConnectClient(addresses[0]);
    CHECK(WaitUntil([&] { return handler->accepted.load() == 1; }, 3000ms));
    CHECK(!EngineTestAccess::InjectDriverFailure(engine));
    // 无 RequestClose，控制域必须主动检测 Failed 并收口。
    CHECK(runner.WaitForReturn(5000ms));
    const auto error = engine.LastError();
    CHECK(error);
    CHECK(engine.LastFailure() != nullptr);
    // Failed 路径不在控制域伪调 owner 回调。
    CHECK(handler->closed.load() == 0);
    CHECK(!client.Close());
    runner.CloseAndJoin();
    std::cout << "engine_driver_failed stopped=1 error=" << error.value()
              << " no_pseudo_onclose=ok\n";
}

// ET/ticker 等历史字段已从 Options 删除，"识别后拒绝"路径不复存在。本条改为：
// 保留字段的非法值仍在 Run 的归一化阶段、driver 绑定/socket 副作用之前拒绝，
// 且 Engine 回到 Stopped、LocalAddresses 拒绝（无零副作用泄漏）。
// 「字段本身不存在」由 Test_net_freeze 的 net.freeze_options_contract 负向编译
// 对照证明（编译期，不在运行期）。
static void InvalidOptionsRejected() {
    StartRuntime(2);
    struct Case {
        const char* label;
        std::int64_t num_event_loop;
        std::int64_t max_inbound_bytes;
        std::error_code expected;
    };
    const Case cases[] = {
        {"negative_loop", -1, 1024, std::make_error_code(std::errc::invalid_argument)},
        {"zero_inbound", 1, 0, std::make_error_code(std::errc::invalid_argument)},
    };
    for (const auto& item : cases) {
        Engine engine(std::make_shared<NullHandler>());
        EngineConfig config = ServerConfig(Loopback(0));
        config.options.num_event_loop = item.num_event_loop;
        config.options.max_inbound_bytes = item.max_inbound_bytes;
        const auto error = engine.Run(config);
        CHECK(error == item.expected);
        CHECK(engine.Phase() == EnginePhase::Stopped);
        std::vector<Address> addresses;
        CHECK(engine.LocalAddresses(addresses)
              == std::make_error_code(std::errc::operation_not_permitted));
        std::cout << "invalid_options " << item.label << " err=" << error.value() << '\n';
    }
}

static void HandoffFullResume() {
    StartRuntime(3);
    auto handler = std::make_shared<NullHandler>();
    Engine engine(handler);
    CHECK(!engine.EnableAcceptHandoff(1));
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    std::vector<NativeSocket> clients;
    for (int i = 0; i < 3; ++i) {
        clients.push_back(ConnectClient(addresses[0]));
    }
    std::atomic<int> accepted{0};
    auto task = Task([&] {
        for (int i = 0; i < 3; ++i) {
            auto res = engine.CoAccept(Until(3000ms));
            if (res.error || !res.connection.Valid()) {
                return;
            }
            accepted.fetch_add(1);
            res.connection.RequestClose();
        }
    });
    Get(task);
    CHECK(accepted.load() == 3);
    runner.CloseAndJoin();
    CHECK(!runner.error);
    for (auto& client : clients) {
        (void)client.Close();
    }
    std::cout << "handoff_full_resume accepted=" << accepted.load() << "\n";
}

static void LbSubs() {
    StartRuntime(3);
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineConfig config = ServerConfig(Loopback(0), 2);
    config.options.lb = LoadBalancing::RoundRobin;
    EngineRunner runner(engine, config);
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    std::vector<NativeSocket> clients;
    for (int i = 0; i < 4; ++i) {
        clients.push_back(ConnectClient(addresses[0]));
    }
    CHECK(WaitUntil([&] { return handler->accepted.load() == 4; }, 3000ms));
    std::set<std::uint64_t> owners;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        owners = handler->owners;
    }
    CHECK(owners.size() == 2);
    for (auto& client : clients) {
        (void)client.Close();
    }
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "lb_subs owners=" << owners.size() << " accepted=4\n";
}

static void Fairness() {
    StartRuntime(3);
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0), 2));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));

    auto hot = ConnectClient(addresses[0]);
    std::atomic<bool> stop{false};
    std::atomic<long long> hot_bytes{0};
    std::thread hot_thread([&] {
        const std::string payload(256, 'h');
        while (!stop.load()) {
            if (!SendAll(hot, payload, 100ms)) {
                break;
            }
            hot_bytes.fetch_add(static_cast<long long>(payload.size()));
            char buffer[512];
            const auto result = hot.Receive(buffer, sizeof(buffer));
            (void)result;
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    });

    auto second = ConnectClient(addresses[0]);
    bool second_ok = true;
    for (int i = 0; i < 100; ++i) {
        std::string echoed;
        if (!SendAll(second, "p", 500ms) || !RecvExact(second, 1, echoed, 500ms)) {
            second_ok = false;
            break;
        }
    }

    std::atomic<int> business{0};
    auto task = Task([&] {
        for (int i = 0; i < 100; ++i) {
            bbt::coroutine::sync::CoWaiter waiter;
            waiter.WaitWithTimeout(1);
            business.fetch_add(1);
        }
    });
    Get(task);

    stop.store(true);
    hot_thread.join();
    CHECK(second_ok);
    CHECK(business.load() == 100);
    CHECK(hot_bytes.load() > 0);
    (void)hot.Close();
    (void)second.Close();
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "fairness hot_bytes=" << hot_bytes.load() << " second=100 business=100\n";
}


// ---------------- B5 增量 ----------------

static std::size_t CountFds() {
    return static_cast<std::size_t>(std::distance(
        std::filesystem::directory_iterator("/proc/self/fd"),
        std::filesystem::directory_iterator{}));
}

static void RingAppendFaultSharedOut() {
    ReceiveRing ring(64);
    CHECK(ring.TryAppend("abcdef").status == ReadStatus::Ok);
    const std::string original = "prefix";
    std::string out = original;
    ReceiveRing::SetAppendFaultForTest([] { throw std::bad_alloc(); });
    const auto failed = ring.TryRead(out);
    ReceiveRing::SetAppendFaultForTest({});
    CHECK(failed.status == ReadStatus::Error);
    CHECK(ring.Size() == 6);       // 未消费
    CHECK(out == original);        // out 不变（强保证）
    std::string retry;
    CHECK(ring.TryRead(retry).status == ReadStatus::Ok);
    CHECK(retry == "abcdef");
    std::cout << "ring_append_fault_shared_out unchanged=ok\n";
}

static void CoReadAppendFaultNonEmptyOut() {
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(1, 4096, 1024, 4, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);
    CHECK(state->ring.TryAppend("co-payload").status == ReadStatus::Ok);
    auto out = std::make_shared<std::string>("existing");
    std::atomic<int> result{-1};
    // 成员 Read 的分配点在 TryPopAll 的 takeout/segments 元数据：注入
    // operator new 失败覆盖「分配失败零消费、不改 out」的同一契约。
    auto task = Task([&] {
        CoTcpConnection conn{connection};
        g_fail_alloc = true;
        try {
            const auto read = conn.Read(Until(500ms));
            g_fail_alloc = false;
            if (read.state == ReadState::Error) {
                result.store(0);
            }
        } catch (const std::bad_alloc&) {
            g_fail_alloc = false;
            result.store(0);  // 分配失败未交付任何数据，等价 Error 路径
        }
    });
    Get(task);
    CHECK(result.load() == 0);
    CHECK(state->ring.Size() == 10);   // 未消费
    CHECK(*out == "existing");
    // 重试必须在协程域内（Read 在非法域一律 operation_not_permitted）。
    auto retry_out = std::make_shared<std::string>();
    std::atomic<int> retry_result{-1};
    auto retry_task = Task([&] {
        CoTcpConnection conn{connection};
        const auto read = conn.Read(Until(500ms));
        if (read.state == ReadState::Data) {
            read.data.AppendTo(*retry_out);
            retry_result.store(0);
        }
    });
    Get(retry_task);
    CHECK(retry_result.load() == 0);
    CHECK(*retry_out == "co-payload");
    std::cout << "co_read_append_fault_nonempty_out unchanged=ok\n";
}

static void CoReadIdleNoSpin() {
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(1, 4096, 1024, 4, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);
    std::atomic<int> result{-1};
    const auto before = state->ring.AccessGeneration();
    auto task = Task([&] {
        CoTcpConnection conn{connection};
        const auto read = conn.Read(bbt::coroutine::WaitOptions{});
        if (read.state == ReadState::Data && read.data.CopyTo() == "x") {
            result.store(0);
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    const auto during = state->ring.AccessGeneration();
    CHECK(during - before <= 2);  // 空数据等待不自激取保护
    CHECK(state->ring.TryAppend("x").status == ReadStatus::Ok);
    Get(task);
    CHECK(result.load() == 0);
    CHECK(state->ring.AccessGeneration() - before <= 4);
    std::cout << "co_read_idle_no_spin generation_delta=" << (during - before) << "\n";
}

static void StagedCloseTail() {
    StartRuntime(2);
    auto handler = std::make_shared<StoreHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    auto client = ConnectClient(addresses[0]);
    CHECK(SendAll(client, "first", 1000ms));
    CHECK(WaitUntil([&] {
        std::lock_guard<std::mutex> lock(handler->mutex);
        return handler->reads.load() >= 1 && handler->stored != nullptr
            && handler->stored->ReadableBytes() >= 5;
    }, 3000ms));
    std::shared_ptr<Connection> retained;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        retained = handler->stored;
    }
    CHECK(retained && retained->Valid());
    // owner 已取走、追加遇 Busy 的字节走同 ring pending（统一锁由 owner 持，
    // 不在消费者保护内调用）。Busy 竞争路径由 staged_recv_busy_close 覆盖。
    CHECK(ConnectionTestAccess::StateOf(*retained)->ring.AppendDeferred("second"));
    retained->RequestClose();          // 请求封口；最终 EOF 由 owner stage 后发布
    std::string tail;
    for (int i = 0; i < 500; ++i) {
        std::string chunk;
        const auto result = retained->TryRead(chunk);
        if (result.status == ReadStatus::Ok) {
            tail += chunk;
            continue;
        }
        if (result.status == ReadStatus::WouldBlock
            || result.status == ReadStatus::Busy) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;  // owner 尚未发布最终 EOF
        }
        CHECK(result.status == ReadStatus::Eof
              || result.status == ReadStatus::Error);
        break;
    }
    CHECK(tail == "firstsecond");
    CHECK(!client.Close());
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "staged_close_tail tail=" << tail << "\n";
}

static NativeSocket MakeListener(Address& address) {
    std::error_code error;
    NativeSocket listener = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    CHECK(!error);
    CHECK(!listener.Bind(Loopback(0)));
    CHECK(!listener.Listen(8));
    address = listener.LocalAddress(error);
    CHECK(!error);
    return listener;
}

static EngineConfig ClientConfig() {
    EngineConfig config;
    config.runtime_progress = RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    return config;
}

static void ConnectCompleteBeforeWait() {
    StartRuntime(1);
    Address address;
    auto listener = MakeListener(address);
    Engine client{std::shared_ptr<RawHandler>()};  // Co-only：无 raw 接收者
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    std::atomic<int> result{-1};
    auto task = Task([&] {
        std::uint64_t id = 0;
        if (CoAccess::SubmitConnect(client, address, id)) { result.store(1); return; }
        std::this_thread::sleep_for(std::chrono::milliseconds(30));  // 完成早于等待
        Connection out;
        if (!CoAccess::WaitConnect(client, id, out, Until(1000ms)) && out.Valid()) {
            result.store(0);
        }
    });
    Get(task);
    CHECK(result.load() == 0);
    Address peer;
    std::error_code ignored;
    (void)listener.Accept(peer, ignored);
    CHECK(EngineTestAccess::ConnectTableSize(client) == 0);
    runner.CloseAndJoin();
    std::cout << "connect_complete_before_wait retained=ok\n";
}

static void ConnectTimeoutRecyclesAdopt() {
    StartRuntime(1);
    Address address;
    auto listener = MakeListener(address);
    Engine client{std::shared_ptr<RawHandler>()};  // Co-only：无 raw 接收者
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    auto cycle = [&] {
        std::atomic<int> wait_error{-1};
        auto task = Task([&] {
            std::uint64_t id = 0;
            if (CoAccess::SubmitConnect(client, address, id)) {
                return;
            }
            Connection out;
            wait_error.store(CoAccess::WaitConnect(client, id, out, Until(30ms)).value());
        });
        Get(task);
        CHECK(wait_error.load() == std::make_error_code(std::errc::timed_out).value());
        CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 2000ms));
    };
    EngineTestAccess::SetConnectDeliveryDelay(client, std::chrono::milliseconds(200));
    cycle();                              // 首次触发 Asio 上下文惰性 fd
    const auto fds_after_first = CountFds();
    cycle();                              // 第二次：迟到的 adopt 回收后 fd 不增长
    CHECK(WaitUntil([&] { return CountFds() <= fds_after_first; }, 2000ms));
    runner.CloseAndJoin();
    std::cout << "connect_timeout_recycles_adopt fd_baseline=ok\n";
}

static void ConnectHardCap() {
    StartRuntime(1);
    Address address;
    auto listener = MakeListener(address);
    Engine client{std::shared_ptr<RawHandler>()};  // Co-only：无 raw 接收者
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    EngineTestAccess::SetConnectLimit(client, 1);
    EngineTestAccess::SetConnectDeliveryDelay(client, std::chrono::milliseconds(300));
    std::uint64_t first = 0;
    CHECK(!CoAccess::SubmitConnect(client, address, first));
    std::uint64_t second = 0;
    CHECK(CoAccess::SubmitConnect(client, address, second)
          == std::make_error_code(std::errc::no_buffer_space));
    runner.CloseAndJoin();
    std::cout << "connect_hard_cap all_pending_zero_accept=ok\n";
}

static void ConnectRawCompletionCleanup() {
    StartRuntime(1);
    Address address;
    auto listener = MakeListener(address);
    struct RawConnect : RawHandler {
        std::atomic<int> connects{0};
        std::atomic<int> close{0};
        std::atomic<int> completed{0};
        void OnConnect(Connection connection, std::error_code error,
                       std::uint64_t operation) override {
            (void)operation;
            if (!error && connection.Valid()) {
                connects.fetch_add(1);
                connection.RequestClose();
            }
            completed.fetch_add(1);
        }
        void OnClose(Connection, std::error_code) override { close.fetch_add(1); }
    };
    auto handler = std::make_shared<RawConnect>();
    Engine client(handler);
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    std::uint64_t id = 0;
    CHECK(!client.AsyncConnect(address, id));  // raw-only
    CHECK(WaitUntil([&] { return handler->completed.load() == 1; }, 3000ms));
    CHECK(handler->connects.load() == 1);
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 2000ms));
    runner.CloseAndJoin();
    std::cout << "connect_raw_completion_cleanup table=0\n";
}

static void LbLeastConnections() {
    StartRuntime(3);
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineConfig config = ServerConfig(Loopback(0), 2);
    config.options.lb = LoadBalancing::LeastConnections;
    EngineRunner runner(engine, config);
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    std::vector<NativeSocket> clients;
    for (int i = 0; i < 4; ++i) {
        clients.push_back(ConnectClient(addresses[0]));
    }
    CHECK(WaitUntil([&] { return handler->accepted.load() == 4; }, 3000ms));
    std::set<std::uint64_t> owners;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        owners = handler->owners;
    }
    CHECK(owners.size() == 2);
    for (auto& client : clients) {
        (void)client.Close();
    }
    runner.CloseAndJoin();
    std::cout << "lb_least_connections owners=" << owners.size() << "\n";
}

static void SocketOptionsGetsockopt() {
    StartRuntime(2);
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineConfig config = ServerConfig(Loopback(0));
    config.options.tcp_no_delay = true;
    config.options.socket_recv_buffer = 8192;
    config.options.socket_send_buffer = 8192;
    config.options.tcp_keep_alive = std::chrono::seconds(5);
    EngineRunner runner(engine, config);
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    auto client = ConnectClient(addresses[0]);
    CHECK(WaitUntil([&] { return handler->accepted.load() == 1; }, 3000ms));
    const int fd = EngineTestAccess::FirstConnectionFd(engine);
    CHECK(fd >= 0);
    int nodelay = 0;
    socklen_t length = sizeof(nodelay);
    CHECK(::getsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, &length) == 0 && nodelay != 0);
    int recvbuf = 0;
    length = sizeof(recvbuf);
    CHECK(::getsockopt(fd, SOL_SOCKET, SO_RCVBUF, &recvbuf, &length) == 0 && recvbuf >= 8192);
    int keepalive = 0;
    length = sizeof(keepalive);
    CHECK(::getsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &keepalive, &length) == 0 && keepalive != 0);
    (void)client.Close();
    runner.CloseAndJoin();
    std::cout << "socket_options_getsockopt nodelay/recvbuf/keepalive=ok\n";
}

static void InstallFailureFdBaseline() {
    StartRuntime(1);
    Address address;
    auto blocker = MakeListener(address);
    const auto fds_before = CountFds();
    Engine engine(std::make_shared<NullHandler>());
    EngineConfig config = ServerConfig(address);
    const auto error = engine.Run(config);  // 端口占用，bind 失败
    CHECK(error);
    CHECK(engine.Phase() == EnginePhase::Stopped);
    CHECK(CountFds() == fds_before);  // 未泄漏 listener/fd
    (void)blocker.Close();
    std::cout << "install_failure_fd_baseline error=" << error.value() << " fds_clean=ok\n";
}

static void RunManagedDomainReject() {
    StartRuntime(1);
    auto engine = std::make_shared<Engine>(std::make_shared<NullHandler>());
    std::atomic<int> result{-1};
    auto task = Task([&] {
        EngineConfig config;
        config.runtime_progress = RuntimeProgress::Independent;
        const auto error = engine->Run(config);
        if (error == std::make_error_code(std::errc::operation_not_permitted)
            && engine->Phase() == EnginePhase::Idle) {
            result.store(0);
        }
    });
    Get(task);
    CHECK(result.load() == 0);
    std::cout << "run_managed_domain_reject no_side_effect=ok\n";
}


static void CoReadRapid() {
    StartRuntime(1);
    constexpr int kCount = 2000;
    auto state = std::make_shared<ConnectionState>(1, 1 << 20, 1024, 4, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);
    std::atomic<int> total{0};
    std::atomic<int> result{-1};
    auto task = Task([&] {
        CoTcpConnection conn{connection};
        for (;;) {
            const auto read = conn.Read(Until(5000ms));
            if (read.state != ReadState::Data) {
                result.store(read.error.value());
                break;
            }
            const auto size = static_cast<int>(read.data.Size());
            if (total.fetch_add(size) + size >= kCount) {
                result.store(0);
                break;
            }
        }
    });
    for (int i = 0; i < kCount; ++i) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
        for (;;) {
            const auto result = state->ring.TryAppend("x");
            if (result.status == ReadStatus::Ok) {
                break;
            }
            if (result.status == ReadStatus::Busy) {
                if (state->ring.AppendDeferred("x")) {
                    break;  // 非阻塞：Busy 时由调用者重试，不锁等
                }
                CHECK(std::chrono::steady_clock::now() < deadline);
                std::this_thread::yield();
                continue;
            }
            CHECK(false);
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(50));
    }
    Get(task);
    CHECK(result.load() == 0);
    CHECK(total.load() == kCount);
    std::cout << "co_read_rapid total=" << total.load() << "\n";
}


// ---------------- B6: F8 / F6 / F7 / fairness ----------------

static void WriteTerminalNoAlloc() {
    auto state = std::make_shared<ConnectionState>(1, 4096, 4096, 4, nullptr);
    WriteOperation operation;
    CHECK(!TryAccept(*state, "abcdef", operation));
    auto sink = [](const std::string_view* views, std::size_t, std::size_t budget) -> ssize_t {
        return static_cast<ssize_t>(std::min(views[0].size(), budget));
    };
    g_fail_alloc = true;
    const auto result = state->PumpWrites(8, 4096, sink);
    g_fail_alloc = false;
    CHECK(result.terminal_changed);
    CHECK(operation.Query().phase == WritePhase::Done && operation.Query().sent == 6);
    g_fail_alloc = true;
    CompletedWrite item;
    const bool popped = state->TryPopTerminal(item);
    g_fail_alloc = false;
    CHECK(popped && item.id == operation.Id() && item.sent == 6);

    // MarkClosed 在单次分配失败下仍逐项结算全部责任。
    auto closed = std::make_shared<ConnectionState>(2, 4096, 4096, 4, nullptr);
    WriteOperation a, b;
    CHECK(!TryAccept(*closed, "aa", a));
    CHECK(!TryAccept(*closed, "bb", b));
    g_fail_alloc = true;
    closed->MarkClosed(std::make_error_code(std::errc::operation_canceled));
    g_fail_alloc = false;
    CHECK(a.Query().phase == WritePhase::Done && b.Query().phase == WritePhase::Done);
    int count = 0;
    while (closed->TryPopTerminal(item)) { ++count; }
    CHECK(count == 2);
    CHECK(closed->Acceptance() == AcceptanceState::Closed);
    std::cout << "write_terminal_no_alloc terminal_transition=alloc_free\n";
}

static void WriteCapacityAfterDrain() {
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(1, 4096, 4096, 1, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);
    auto sink = [](const std::string_view* views, std::size_t, std::size_t budget) -> ssize_t {
        return static_cast<ssize_t>(std::min(views[0].size(), budget));
    };
    std::atomic<int> a_result{-1};
    std::atomic<int> b_result{-1};
    auto task_a = Task([&] {
        CoTcpConnection conn{connection};
        const auto report = conn.Write("a", Until(2000ms));
        if (!report.wait_error && report.outcome.phase == WritePhase::Done) {
            a_result.store(0);
        }
    });
    CHECK(WaitUntil([&] { return state->HasPendingWrites(); }, 1000ms));
    auto task_b = Task([&] {
        CoTcpConnection conn{connection};
        const auto report = conn.Write("b", Until(2000ms));
        if (!report.wait_error && report.outcome.phase == WritePhase::Done) {
            b_result.store(0);
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    CHECK(!state->CanAcceptWrite(1));  // op1 active, max_ops=1
    state->PumpWrites(8, 4096, sink);  // op1 terminal，但尚未 drain
    CHECK(WaitUntil([&] { return a_result.load() == 0; }, 1000ms));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CHECK(!state->CanAcceptWrite(1));  // terminal 仍占 max_ops
    CompletedWrite item;
    CHECK(state->TryPopTerminal(item));  // drain 释放 op 额度并通知
    CHECK(WaitUntil([&] { return state->HasPendingWrites(); }, 1000ms));  // op2 已接纳
    state->PumpWrites(8, 4096, sink);
    Get(task_a);
    Get(task_b);
    CHECK(a_result.load() == 0 && b_result.load() == 0);
    std::cout << "write_capacity_after_drain max_ops1=ok\n";
}

static void WriteCapacityAfterPartial() {
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(1, 4096, 8, 4, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);
    WriteOperation first;
    CHECK(!TryAccept(*state, std::string(8, 'x'), first));
    std::atomic<int> second_result{-1};
    auto task = Task([&] {
        CoTcpConnection conn{connection};
        const auto report = conn.Write("y", Until(2000ms));
        if (!report.wait_error && report.outcome.phase == WritePhase::Done) {
            second_result.store(0);
        }
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    CHECK(!state->CanAcceptWrite(1));  // 字节额度满
    auto one_byte = [](const std::string_view*, std::size_t, std::size_t) -> ssize_t {
        return 1;  // 部分发送，无 terminal
    };
    state->PumpWrites(1, 1, one_byte);  // 释放 1 字节额度（无 terminal）→ 必须通知容量 waiter
    // 判别点：必须在 op1 仍 InFlight（ActiveOpCount 从 1→2）时就接纳第二个写；
    // 若只在 terminal 后唤醒，这里会超时失败。
    CHECK(WaitUntil([&] { return state->ActiveOpCount() == 2; }, 1000ms));
    auto drain = [](const std::string_view* views, std::size_t, std::size_t budget) -> ssize_t {
        return static_cast<ssize_t>(std::min(views[0].size(), budget));
    };
    state->PumpWrites(8, 4096, drain);
    Get(task);
    CHECK(second_result.load() == 0);
    CHECK(first.Query().phase == WritePhase::Done && first.Query().sent == 8);
    std::cout << "write_capacity_after_partial small_request_woken=ok\n";
}

static void AcceptAfterClose() {
    StartRuntime(2);
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    engine.RequestClose();
    {
        std::uint64_t id = 0;
        CHECK(engine.AsyncConnect(addresses[0], id));  // 封口后零新命令
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    std::error_code error;
    auto client = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    if (!error) {
        (void)client.Connect(addresses[0]);
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        CHECK(handler->accepted.load() == 0);  // 封口后零新 accept 回调
        (void)client.Close();
    }
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "accept_after_close accepted=0\n";
}

static void ConnectCancelLateCallback() {
    StartRuntime(1);
    Address address;
    auto listener = MakeListener(address);
    struct RawConnect : RawHandler {
        std::atomic<int> completed{0};
        std::atomic<int> canceled{0};
        void OnConnect(Connection connection, std::error_code error,
                       std::uint64_t operation) override {
            (void)operation;
            if (error == std::make_error_code(std::errc::operation_canceled)) {
                canceled.fetch_add(1);
            }
            ++completed;
            if (connection.Valid()) {
                connection.RequestClose();
            }
        }
    };
    auto handler = std::make_shared<RawConnect>();
    Engine client(handler);
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    EngineTestAccess::SetConnectDeliveryDelay(client, std::chrono::milliseconds(200));
    std::uint64_t id = 0;
    CHECK(!client.AsyncConnect(address, id));
    std::this_thread::sleep_for(std::chrono::milliseconds(50));  // wait 已完成、交付延迟中
    client.RequestClose();
    runner.CloseAndJoin();  // Run 必须等延迟交付的 OnConnect 兑现后才解绑
    CHECK(!runner.error);
    CHECK(handler->completed.load() == 1);
    CHECK(handler->canceled.load() == 1);
    std::cout << "connect_cancel_late_callback completed=1 canceled=1\n";
}

static void FairnessOneSub() {
    StartRuntime(3);
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0), 1));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));

    auto hot = ConnectClient(addresses[0]);
    std::atomic<bool> stop{false};
    std::atomic<long long> hot_bytes{0};
    std::thread hot_thread([&] {
        const std::string payload(256, 'h');
        while (!stop.load()) {
            if (!SendAll(hot, payload, 100ms)) {
                break;
            }
            hot_bytes.fetch_add(static_cast<long long>(payload.size()));
            char buffer[512];
            (void)hot.Receive(buffer, sizeof(buffer));
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
    });
    auto cold = ConnectClient(addresses[0]);
    bool cold_ok = true;
    for (int i = 0; i < 50; ++i) {
        std::string echoed;
        if (!SendAll(cold, "c", 500ms) || !RecvExact(cold, 1, echoed, 500ms)) {
            cold_ok = false;
            break;
        }
    }
    std::atomic<int> business{0};
    auto task = Task([&] {
        for (int i = 0; i < 50; ++i) {
            bbt::coroutine::sync::CoWaiter waiter;
            waiter.WaitWithTimeout(1);
            business.fetch_add(1);
        }
    });
    Get(task);
    stop.store(true);
    hot_thread.join();
    CHECK(cold_ok);
    CHECK(business.load() == 50);
    CHECK(hot_bytes.load() > 0);
    std::set<std::uint64_t> owners;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        owners = handler->owners;
    }
    CHECK(owners.size() == 1);  // 1 sub：热/冷同 owner，业务协程同进程并发进展
    (void)hot.Close();
    (void)cold.Close();
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "fairness_one_sub owners=" << owners.size()
              << " hot_bytes=" << hot_bytes.load() << " cold=50 business=50\n";
}


static void StagedRecvBusyClose() {
    StartRuntime(3);
    auto handler = std::make_shared<StoreHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));

    std::atomic<bool> hook_entered{false};
    std::atomic<bool> holder_held{false};
    std::atomic<bool> release_hold{false};
    std::atomic<bool> holder_done{false};
    std::shared_ptr<Connection> conn1;
    // holder 自己取/放保护；无 3s 自动释放（旧写法到点自行释放会掩盖 owner 阻塞）。
    std::thread holder([&] {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!hook_entered.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        {
            std::lock_guard<std::mutex> lock(handler->mutex);
            if (handler->stored) {
                conn1 = std::make_shared<Connection>(*handler->stored);
            }
        }
        if (!conn1) {
            return;
        }
        auto hold = ConnectionTestAccess::HoldForTest(ConnectionTestAccess::StateOf(*conn1));
        if (!hold) {
            return;
        }
        holder_held.store(true);
        while (!release_hold.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        ConnectionTestAccess::Reset(hold);
        holder_done.store(true);
    });
    EngineTestAccess::SetReadAppendHook(engine, [&] {
        hook_entered.store(true);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!holder_held.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
    });
    auto client1 = ConnectClient(addresses[0]);
    CHECK(SendAll(client1, "first", 1000ms));
    CHECK(WaitUntil([&] { return holder_held.load(); }, 5000ms));
    // 判别：owner 不得因 conn1 的 Busy 阻塞——第二连接必须仍被接纳/交付到同一 sub。
    auto client2 = ConnectClient(addresses[0]);
    CHECK(WaitUntil([&] { return handler->accepted.load() == 2; }, 3000ms));
    // conn1 的字节在 holder 释放前不得被交付（deferred，不是阻塞后凑巧成功）。
    CHECK(conn1 && conn1->ReadableBytes() == 0);
    release_hold.store(true);
    holder.join();
    CHECK(holder_done.load());
    CHECK(WaitUntil([&] { return conn1->ReadableBytes() >= 5; }, 3000ms));
    std::string tail;
    for (int i = 0; i < 200 && tail.size() < 5; ++i) {
        std::string chunk;
        const auto result = conn1->TryRead(chunk);
        if (result.status == ReadStatus::Ok) {
            tail += chunk;
            continue;
        }
        if (result.status == ReadStatus::Busy) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;  // owner 短暂持锁：按 TryRead 契约重试
        }
        break;
    }
    CHECK(tail == "first");
    EngineTestAccess::SetReadAppendHook(engine, {});
    (void)client1.Close();
    (void)client2.Close();
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "staged_recv_busy_close tail=" << tail
              << " second_conn_accepted=1 no_owner_block=ok\n";
}


static void RingDeferredNonblockingTry() {
    // 保护由公共守卫经内部 TestAccess 取得：裸 ReceiveRing 不再提供 try-hold。
    auto state = std::make_shared<ConnectionState>(1, 1024, 1024, 4, nullptr);
    ReceiveRing& ring = state->ring;
    ring.ReserveDeferred(64);
    std::atomic<bool> held{false};
    std::atomic<bool> release{false};
    std::thread holder([&] {
        auto hold = ConnectionTestAccess::HoldForTest(state);
        if (!hold) {
            return;
        }
        held.store(true);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!release.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        ConnectionTestAccess::Reset(hold);
    });
    CHECK(WaitUntil([&] { return held.load(); }, 3000ms));
    const auto begin = std::chrono::steady_clock::now();
    CHECK(!ring.AppendDeferred("abc"));  // Busy：必须立即 false，不能等保护释放
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - begin).count();
    CHECK(elapsed < 500);
    release.store(true);
    holder.join();
    CHECK(ring.AppendDeferred("abc"));   // 释放后同一协议可提交
    std::string out;
    CHECK(ring.TryRead(out).status == ReadStatus::Ok);
    CHECK(out == "abc");
    std::cout << "ring_deferred_nonblocking_try elapsed_ms=" << elapsed << " ok\n";
}

static void RingDeferredBackingReuse() {
    ReceiveRing ring(1024);
    ring.ReserveDeferred(64);
    const std::string payload(32, 'a');
    for (int round = 0; round < 2; ++round) {
        if (round == 1) {
            g_fail_alloc = true;  // 第二轮 deferred append 必须复用预留 backing
        }
        const bool appended = ring.AppendDeferred(payload);
        g_fail_alloc = false;
        CHECK(appended);
        CHECK(ring.PendingSize() == payload.size());
        std::string out;
        CHECK(ring.TryRead(out).status == ReadStatus::Ok);
        CHECK(out == payload);
        CHECK(ring.PendingSize() == 0);
    }
    std::cout << "ring_deferred_backing_reuse rounds=2 no_alloc=ok\n";
}



static void RunTeardownNoResidue() {
    StartRuntime(2);
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    auto client = ConnectClient(addresses[0]);
    CHECK(SendAll(client, "z", 1000ms));
    std::string echoed;
    CHECK(RecvExact(client, 1, echoed, 1000ms));
    engine.RequestClose();
    runner.CloseAndJoin();
    CHECK(!runner.error);
    CHECK(engine.Phase() == EnginePhase::Stopped);
    CHECK(handler->accepted.load() == 1);
    CHECK(WaitUntil([&] { return handler->closed.load() == 1; }, 2000ms));  // 回调已清
    CHECK(EngineTestAccess::DirtyCount(engine) == 0);
    CHECK(EngineTestAccess::ConnectionCount(engine) == 0);
    CHECK(EngineTestAccess::ConnectTableSize(engine) == 0);
    (void)client.Close();
    std::cout << "run_teardown_no_residue dirty=0 conn=0 connect=0 closed=1\n";
}


// ---------------- Co 活性（已证 generation 采样根因） ----------------

static bbt::coroutine::WaitOptions At(std::chrono::steady_clock::time_point deadline) {
    return bbt::coroutine::WaitOptions{deadline};
}

static void CoBusyReleasePhases() {
    StartRuntime(1);
    auto make_state = [] {
        auto state = std::make_shared<ConnectionState>(1, 4096, 1024, 4, nullptr);
        CHECK(state->ring.TryAppend(std::string(256, 'x')).status
              == ReadStatus::Ok);
        return state;
    };
    // 1) 尝试前释放：TryAccess 直接可读。
    {
        auto state = make_state();
        Connection connection = ConnectionTestAccess::Make(state);
        auto hold = ConnectionTestAccess::HoldForTest(state);
        CHECK(bool(hold));
        ConnectionTestAccess::Reset(hold);  // 尝试前释放
        auto shared_out = std::make_shared<std::string>();
        std::atomic<int> read_result{-1};
        auto read_task = Task([&] {
            CoTcpConnection conn{connection};
            const auto read = conn.Read(Until(1000ms));
            if (read.state == ReadState::Data) {
                read.data.AppendTo(*shared_out);
                read_result.store(0);
            }
        });
        Get(read_task);
        CHECK(read_result.load() == 0);
        CHECK(shared_out->size() == 256);
    }
    // 2) Busy 之后、waiter 创建之前释放：必须靠「TryAccess 之前采的旧代次」
    //    在 on_registered 复查出差异；修复前采到已释放后的 G+1 → 超时。
    {
        auto state = make_state();
        Connection connection = ConnectionTestAccess::Make(state);
        std::atomic<bool> entered{false};
        std::atomic<bool> release_now{false};
        ReceiveRing::SetBusyBeforeWaiterHookForTest([&] {
            entered.store(true);
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
            while (!release_now.load() && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::yield();
            }
        });
        auto hold = ConnectionTestAccess::HoldForTest(state);
        CHECK(bool(hold));
        std::atomic<int> result{-1};
        auto task = Task([&] {
            CoTcpConnection conn{connection};
            const auto read = conn.Read(Until(2000ms));
            if (read.state == ReadState::Data && read.data.Size() == 256) {
                result.store(0);
            }
        });
        CHECK(WaitUntil([&] { return entered.load(); }, 1000ms));
        ConnectionTestAccess::Reset(hold);   // Busy 后、waiter 前释放并推进代次
        release_now.store(true);
        Get(task);
        ReceiveRing::SetBusyBeforeWaiterHookForTest({});
        CHECK(result.load() == 0);
    }
    // 3) waiter 加入 hub 之后释放：走 release hub Notify。
    {
        auto state = make_state();
        Connection connection = ConnectionTestAccess::Make(state);
        std::atomic<bool> added{false};
        ReceiveRing::SetAfterAddHookForTest([&] { added.store(true); });
        auto hold = ConnectionTestAccess::HoldForTest(state);
        CHECK(bool(hold));
        std::atomic<int> result{-1};
        auto task = Task([&] {
            CoTcpConnection conn{connection};
            const auto read = conn.Read(Until(2000ms));
            if (read.state == ReadState::Data && read.data.Size() == 256) {
                result.store(0);
            }
        });
        CHECK(WaitUntil([&] { return added.load(); }, 1000ms));
        ConnectionTestAccess::Reset(hold);   // Add 之后释放
        Get(task);
        ReceiveRing::SetAfterAddHookForTest({});
        CHECK(result.load() == 0);
    }
    std::cout << "co_busy_release_phases before/busy/add=ok\n";
}

static void CoWouldBlockClosedRace() {
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(1, 4096, 1024, 4, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);
    std::atomic<bool> entered{false};
    std::atomic<bool> closed{false};
    ReceiveRing::SetWouldBlockBeforeWaiterHookForTest([&] {
        entered.store(true);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (!closed.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
    });
    std::atomic<int> result{-1};
    auto task = Task([&] {
        CoTcpConnection conn{connection};
        const auto read = conn.Read(Until(2000ms));
        if (read.state == ReadState::Eof
            || (read.state == ReadState::Error
                && read.error == make_error_code(NetErrc::Closed))) {
            result.store(0);
        }
    });
    CHECK(WaitUntil([&] { return entered.load(); }, 1000ms));
    state->ring.MarkClosed();   // 空 ring WouldBlock 与 Add 之间发布终态
    closed.store(true);
    Get(task);
    ReceiveRing::SetWouldBlockBeforeWaiterHookForTest({});
    CHECK(result.load() == 0);
    std::cout << "co_wouldblock_closed_race closed_between=ok\n";
}

static void CoPingpongStrict() {
    StartRuntime(4);
    constexpr int kRounds = 500;
    constexpr std::size_t kPayload = 256;
    Engine server{std::shared_ptr<RawHandler>()};
    CHECK(!server.EnableAcceptHandoff(4));
    EngineRunner server_runner(server, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return server.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!server.LocalAddresses(addresses));
    Engine client{std::shared_ptr<RawHandler>()};
    EngineRunner client_runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));

    std::atomic<int> server_error{-1};
    std::atomic<int> client_error{-1};
    std::atomic<int> server_rounds{0};
    std::atomic<int> client_rounds{0};
    auto server_task = Task([&] {
        const auto res = server.CoAccept(Until(5000ms));
        if (res.error || !res.connection.Valid()) { server_error.store(1); return; }
        CoTcpConnection conn = res.connection;
        for (int i = 0; i < kRounds; ++i) {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
            std::string data;
            while (data.size() < kPayload) {
                const auto read = conn.Read(At(deadline));
                if (read.state != ReadState::Data) { server_error.store(2); return; }
                read.data.AppendTo(data);
            }
            const auto report = conn.Write(data, At(deadline));
            if (report.wait_error || report.outcome.phase != WritePhase::Done) {
                server_error.store(3);
                return;
            }
            server_rounds.fetch_add(1);
        }
        server_error.store(0);
    });
    auto client_task = Task([&] {
        const auto res = client.CoDial(addresses[0], Until(5000ms));
        if (res.error || !res.connection.Valid()) {
            client_error.store(1);
            return;
        }
        CoTcpConnection conn = res.connection;
        const std::string payload(kPayload, 'p');
        for (int i = 0; i < kRounds; ++i) {
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
            const auto report = conn.Write(payload, At(deadline));
            if (report.wait_error || report.outcome.phase != WritePhase::Done) {
                client_error.store(2);
                return;
            }
            std::string echoed;
            while (echoed.size() < kPayload) {
                const auto read = conn.Read(At(deadline));
                if (read.state != ReadState::Data) { client_error.store(3); return; }
                read.data.AppendTo(echoed);
            }
            client_rounds.fetch_add(1);
        }
        client_error.store(0);
    });
    Get(client_task);
    Get(server_task);
    CHECK(client_error.load() == 0);
    CHECK(server_error.load() == 0);
    CHECK(client_rounds.load() == kRounds);
    CHECK(server_rounds.load() == kRounds);
    client_runner.CloseAndJoin();
    server_runner.CloseAndJoin();
    CHECK(!client_runner.error && !server_runner.error);
    std::cout << "co_pingpong_strict rounds=" << client_rounds.load()
              << " errors=0 timeouts=0\n";
}


static void WriteTerminalInterleavedNoAlloc() {
    auto state = std::make_shared<ConnectionState>(1, 4096, 4096, 2, nullptr);
    auto sink = [](const std::string_view* views, std::size_t, std::size_t budget) -> ssize_t {
        return static_cast<ssize_t>(std::min(views[0].size(), budget));
    };
    WriteOperation a, b;
    CHECK(!TryAccept(*state, "a", a));
    CHECK(!TryAccept(*state, "b", b));
    state->PumpWrites(8, 4096, sink);  // pending=2（backing 满）
    CompletedWrite item;
    CHECK(state->TryPopTerminal(item));  // pop1 → pending=1，旧实现 backing 仍 2
    WriteOperation c;
    CHECK(!TryAccept(*state, "c", c));   // ops1 + pending1 合法
    // 交错点：再 push 一项终态。旧 vector+head 会 size=3 > capacity=2 触发分配；
    // circular_buffer 容量 2 原地复用被弹槽位。
    g_fail_alloc = true;
    const auto pumped = state->PumpWrites(8, 4096, sink);
    g_fail_alloc = false;
    CHECK(pumped.terminal_changed);
    // 交错累积：先弹回一项，零长终态再 push，仍在原容量内迁移。
    CHECK(state->TryPopTerminal(item));  // pop b → pending=1
    WriteOperation zero;
    // 零长接纳的记录分配是提交前允许的；终态迁移 push 本身必须无分配（由
    // circular_buffer 容量保证），故这里不再对接纳注入失败。
    const auto zero_error = TryAccept(*state, "", zero);
    CHECK(!zero_error && zero.Query().phase == WritePhase::Done);
    CHECK(a.Query().phase == WritePhase::Done && b.Query().phase == WritePhase::Done);
    CHECK(c.Query().phase == WritePhase::Done);
    int popped = 2;  // 已显式 pop a、b
    while (state->TryPopTerminal(item)) { ++popped; }
    CHECK(popped == 4);  // a/b/c/zero 全部结算且无覆盖
    std::cout << "write_terminal_interleaved_no_alloc popped=4\n";
}

static void RawBusyDeferredOnRead() {
    StartRuntime(2);
    struct BusyRetryHandler : RawHandler {
        std::atomic<bool> onread_entered{false};
        std::atomic<bool> hold_ready{false};
        std::atomic<bool> busy_seen{false};
        std::atomic<bool> got{false};
        std::atomic<int> busy{0};
        std::mutex mutex;
        std::shared_ptr<Connection> stored;
        std::string data;

        void OnAccept(Connection connection) override {
            std::lock_guard<std::mutex> lock(mutex);
            stored = std::make_shared<Connection>(connection);
        }
        void OnRead(Connection connection) override {
            onread_entered.store(true);
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
            while (!hold_ready.load() && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::yield();
            }
            std::string chunk;
            const auto result = connection.TryRead(chunk);
            if (result.status == ReadStatus::Busy) {
                busy.fetch_add(1);
                busy_seen.store(true);
                return;
            }
            if (result.status == ReadStatus::Ok) {
                std::lock_guard<std::mutex> lock(mutex);
                data += chunk;
                got.store(true);
            }
        }
    };
    auto handler = std::make_shared<BusyRetryHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    auto client = ConnectClient(addresses[0]);
    CHECK(SendAll(client, "payload", 1000ms));
    CHECK(WaitUntil([&] { return handler->onread_entered.load(); }, 3000ms));
    std::shared_ptr<Connection> stored;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        stored = handler->stored;
    }
    CHECK(stored && stored->Valid());
    // 短保护跨另一 native 线程：测试线程持锁，OnRead 的 TryRead 得到 Busy；
    // 不持锁挂起。
    auto hold = ConnectionTestAccess::HoldForTest(ConnectionTestAccess::StateOf(*stored));
    CHECK(bool(hold));
    handler->hold_ready.store(true);
    CHECK(WaitUntil([&] { return handler->busy_seen.load(); }, 1000ms));
    ConnectionTestAccess::Reset(hold);  // 释放
    // 无新网络字节：owner 必须补投 OnRead 取回原 ring 字节。
    CHECK(WaitUntil([&] { return handler->got.load(); }, 2000ms));
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        CHECK(handler->data == "payload");
    }
    CHECK(handler->busy.load() >= 1);
    (void)client.Close();
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "raw_busy_deferred_onread busy=" << handler->busy.load() << " data=ok\n";
}

static void SealPostFailureRetries() {
    StartRuntime(4);
    auto server_handler = std::make_shared<EchoHandler>();
    Engine server(server_handler);
    EngineRunner server_runner(server, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return server.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!server.LocalAddresses(addresses));

    struct SealHandler : RawHandler {
        std::mutex mutex;
        std::shared_ptr<Connection> conn;
        std::set<std::uint64_t> writes;
        std::atomic<int> closes{0};
        void OnConnect(Connection connection, std::error_code error,
                       std::uint64_t operation) override {
            (void)operation;
            if (error || !connection.Valid()) {
                return;
            }
            std::lock_guard<std::mutex> lock(mutex);
            conn = std::make_shared<Connection>(connection);
        }
        void OnWrite(Connection, WriteResult result) override {
            std::lock_guard<std::mutex> lock(mutex);
            writes.insert(result.operation);
        }
        void OnClose(Connection, std::error_code) override { closes.fetch_add(1); }
    };
    auto handler = std::make_shared<SealHandler>();
    Engine client(handler);
    EngineRunner client_runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    std::uint64_t id = 0;
    CHECK(!client.AsyncConnect(addresses[0], id));
    CHECK(WaitUntil([&] { return handler->conn != nullptr; }, 3000ms));
    std::shared_ptr<Connection> conn;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        conn = handler->conn;
    }
    CHECK(conn && conn->Valid());
    WriteOperation pending;
    CHECK(!conn->AsyncWrite("pending", pending));

    EngineTestAccess::SetSealPostFailureOnce(client);  // 单次 seal post 失败
    client.RequestClose();
    client_runner.CloseAndJoin();
    CHECK(!client_runner.error);                        // 补交后正常完成
    CHECK(client.Phase() == EnginePhase::Stopped);
    CHECK(WaitUntil([&] { return handler->closes.load() == 1; }, 2000ms));
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        CHECK(handler->writes.count(pending.Id()) == 1);  // 待结算写唯一兑现
    }
    CHECK(EngineTestAccess::ConnectTableSize(client) == 0);
    server_runner.CloseAndJoin();
    CHECK(!server_runner.error);
    std::cout << "seal_post_failure_retries close=1 write_settled=1\n";
}



static void WakeHubCapacityBounded() {
    WakeHub hub;
    std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>> keep;
    for (std::size_t i = 0; i < WakeHub::kMaxWaiters; ++i) {
        auto waiter = hub.Create();
        CHECK(hub.Add(waiter));
        keep.push_back(std::move(waiter));
    }
    auto extra = hub.Create();
    CHECK(!hub.Add(extra));           // 第 257 个：有界拒绝
    hub.NotifyAll();                  // 锁外 Notify，不越界
    keep.back().reset();              // 清理一个后 Add 可 prune 并成功
    CHECK(hub.Add(extra));
    keep.clear();
    std::cout << "wakehub_capacity_bounded add256=true add257=false\n";
}

static void CoReadHubFullBounded() {
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(1, 4096, 1024, 4, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);
    std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>> keep;
    for (std::size_t i = 0; i < WakeHub::kMaxWaiters; ++i) {
        auto waiter = state->ring.CreateDataWaiter();
        CHECK(state->ring.AddDataWaiter(waiter));
        keep.push_back(std::move(waiter));
    }
    std::atomic<int> result{-2};
    // 无 deadline：若满额被实现成库内自激重试，此 Get 会一直不返回。
    auto task = Task([&] {
        CoTcpConnection conn{connection};
        const auto read = conn.Read(bbt::coroutine::WaitOptions{});
        result.store(read.error.value());
    });
    Get(task);
    CHECK(result.load() == std::make_error_code(std::errc::no_buffer_space).value());
    keep.clear();  // 清理不污染进程
    CHECK(state->ring.TryAppend("z").status == ReadStatus::Ok);
    auto cleanup_out = std::make_shared<std::string>();
    std::atomic<int> cleanup_result{-1};
    auto cleanup_task = Task([&] {
        CoTcpConnection conn{connection};
        const auto read = conn.Read(Until(500ms));
        if (read.state == ReadState::Data) {
            read.data.AppendTo(*cleanup_out);
            cleanup_result.store(0);
        }
    });
    Get(cleanup_task);
    CHECK(cleanup_result.load() == 0);
    CHECK(*cleanup_out == "z");
    std::cout << "co_read_hub_full_bounded rejected=ok cleanup=ok\n";
}


static void DirtyLifetimeAcrossRemoveConn() {
    StartRuntime(2);
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    auto client = ConnectClient(addresses[0]);
    CHECK(WaitUntil([&] { return handler->accepted.load() == 1; }, 3000ms));
    std::shared_ptr<Connection> conn;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        conn = handler->stored;
    }
    CHECK(conn && conn->Valid());

    std::atomic<bool> barrier_entered{false};
    std::atomic<bool> release_barrier{false};
    EngineTestAccess::SetDirtyBarrierOnce(engine, [&] {
        barrier_entered.store(true);
        while (!release_barrier.load()) {
            std::this_thread::yield();
        }
    });
    // 外部线程：weak-lock core 后暂停在 MarkDirty 内；owner 正常关闭并 RemoveConn。
    std::thread external([&] {
        WriteOperation op;
        (void)conn->AsyncWrite("x", op);
    });
    struct JoinGuard {
        std::thread& thread;
        std::atomic<bool>& release;
        ~JoinGuard() {
            release.store(true);
            if (thread.joinable()) {
                thread.join();
            }
        }
    } join_guard{external, release_barrier};
    CHECK(WaitUntil([&] { return barrier_entered.load(); }, 3000ms));
    conn->RequestClose();
    EngineTestAccess::KickOwner(engine);  // owner 独立推进，移除该 conn
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectionCount(engine) == 0; }, 3000ms));
    release_barrier.store(true);
    external.join();
    // 关键判别：晚 Wake 在 registry 移除后必须被拒绝，DirtyCount 仍为 0
    //（若挂入裸节点，重链后计数为 1，且随后 Pump 会解引用已析构 core）。
    CHECK(EngineTestAccess::DirtyCount(engine) == 0);
    // Engine 仍 Running：另一连接仍可用。
    auto client2 = ConnectClient(addresses[0]);
    CHECK(SendAll(client2, "y", 1000ms));
    std::string echoed;
    CHECK(RecvExact(client2, 1, echoed, 1000ms));
    CHECK(handler->accepted.load() == 2);
    (void)client.Close();
    (void)client2.Close();
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "dirty_lifetime_across_removeconn late_wake_rejected=ok\n";
}


static void SubmitGatePausedBeforePost() {
    StartRuntime(4);
    auto server_handler = std::make_shared<EchoHandler>();
    Engine server(server_handler);
    EngineRunner server_runner(server, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return server.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!server.LocalAddresses(addresses));

    struct RecHandler : RawHandler {
        std::mutex mutex;
        std::set<std::uint64_t> terminals;
        void OnConnect(Connection connection, std::error_code, std::uint64_t operation) override {
            {
                std::lock_guard<std::mutex> lock(mutex);
                terminals.insert(operation);
            }
            if (connection.Valid()) {
                connection.RequestClose();
            }
        }
    };
    auto handler = std::make_shared<RecHandler>();
    Engine client(handler);
    EngineRunner client_runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));

    std::atomic<bool> barrier_entered{false};
    std::atomic<bool> release_barrier{false};
    EngineTestAccess::SetSubmitBarrierOnce(client, [&] {
        barrier_entered.store(true);
        while (!release_barrier.load()) {
            std::this_thread::yield();
        }
    });
    std::atomic<std::uint64_t> accepted_id{0};
    std::atomic<int> submit_result{-1};
    std::thread submitter([&] {
        std::uint64_t id = 0;
        const auto error = client.AsyncConnect(addresses[0], id);
        if (!error) {
            accepted_id.store(id);
            submit_result.store(0);
        } else {
            submit_result.store(error.value());
        }
    });
    CHECK(WaitUntil([&] { return barrier_entered.load(); }, 3000ms));
    client.RequestClose();                 // Run 线程需等该提交的 driver 租约释放
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    release_barrier.store(true);
    submitter.join();
    client_runner.CloseAndJoin();
    CHECK(submit_result.load() == 0);      // 已取得资格：封口前线性化接纳
    CHECK(!client_runner.error);
    const std::uint64_t id = accepted_id.load();
    CHECK(id != 0);
    // 已接纳命令必须由原 owner 兑现唯一终态 OnConnect，Run 返回后零待兑现。
    CHECK(WaitUntil([&] {
        std::lock_guard<std::mutex> lock(handler->mutex);
        return handler->terminals.count(id) != 0;
    }, 2000ms));
    server_runner.CloseAndJoin();
    CHECK(!server_runner.error);
    std::cout << "submit_gate_paused_before_post committed=ok terminal=1\n";
}


static void CoDialFastComplete() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);
    Engine client{std::shared_ptr<RawHandler>()};
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    std::atomic<bool> barrier_entered{false};
    CoTestAccess::SetDialBarrier([&] {
        barrier_entered.store(true);
        // 等 owner 完成（awaitable 意图保留），再进入 WaitConnect。
        CHECK(WaitUntil([&] { return EngineTestAccess::PendingConnectCount(client) == 0; },
                        2000ms));
    });
    std::atomic<int> result{-1};
    std::string failure;
    auto task = Task([&] {
        auto res = client.CoDial(address, Until(2000ms));
        if (!res.error && res.connection.Valid()) {
            result.store(0);
            res.connection.RequestClose();
        } else {
            result.store(res.error.value());
        }
    });
    Get(task);
    CoTestAccess::SetDialBarrier({});
    CHECK(barrier_entered.load());
    CHECK(result.load() == 0);
    CHECK(EngineTestAccess::ConnectTableSize(client) == 0);
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "codial_fast_complete delivered=ok\n";
}

static void CoDialTimeoutRecycles() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);
    Engine client{std::shared_ptr<RawHandler>()};
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    EngineTestAccess::SetConnectDeliveryDelay(client, std::chrono::milliseconds(200));
    std::atomic<int> result{-2};
    auto task = Task([&] {
        const auto res = client.CoDial(address, Until(30ms));
        result.store(res.error.value());
    });
    Get(task);
    CHECK(result.load() == std::make_error_code(std::errc::timed_out).value());
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 2000ms));
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectionCount(client) == 0; }, 2000ms));
    // 第二轮：迟到交付已被回收，正常连接仍可用。
    const auto fds_after_first = CountFds();
    std::atomic<int> second{-2};
    auto task2 = Task([&] {
        auto res = client.CoDial(address, Until(2000ms));
        second.store(res.error.value());
        if (res.connection.Valid()) {
            res.connection.RequestClose();
        }
    });
    Get(task2);
    CHECK(second.load() == 0);
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectionCount(client) == 0; }, 2000ms));
    CHECK(WaitUntil([&] { return CountFds() <= fds_after_first; }, 2000ms));
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "codial_timeout_recycles timeout_respected=ok\n";
}

static void CoDialCancelPostFailureRetries() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);
    Engine client{std::shared_ptr<RawHandler>()};
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    EngineTestAccess::SetConnectStartDelay(client, std::chrono::milliseconds(5000));
    EngineTestAccess::SetCancelPostFailureOnce(client);
    std::atomic<int> result{-2};
    auto task = Task([&] {
        const auto res = client.CoDial(address, Until(30ms));
        result.store(res.error.value());
    });
    Get(task);
    CHECK(result.load() == std::make_error_code(std::errc::timed_out).value());
    // 单次取消 post 失败后，控制循环持久责任必须撤销占位 pending（远早于 5s）。
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 2000ms));
    CHECK(EngineTestAccess::PendingConnectCount(client) == 0);
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "codial_cancel_post_failure persistent_retry=ok\n";
}


static void SealDelayBeyondDeadline() {
    StartRuntime(4);
    auto server_handler = std::make_shared<EchoHandler>();
    Engine server(server_handler);
    EngineRunner server_runner(server, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return server.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!server.LocalAddresses(addresses));
    struct Closer : RawHandler {
        std::atomic<int> closes{0};
        void OnClose(Connection, std::error_code) override { closes.fetch_add(1); }
    };
    auto handler = std::make_shared<Closer>();
    Engine client(handler);
    EngineRunner client_runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    std::uint64_t id = 0;
    CHECK(!client.AsyncConnect(addresses[0], id));
    CHECK(WaitUntil([&] { return server_handler->accepted.load() == 1; }, 3000ms));
    EngineTestAccess::SetSealDelay(client, std::chrono::milliseconds(2500));
    const auto begin = std::chrono::steady_clock::now();
    client.RequestClose();
    client_runner.CloseAndJoin();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - begin).count();
    CHECK(!client_runner.error);
    CHECK(handler->closes.load() == 1);
    CHECK(elapsed >= 2400);  // 不以旧 2 秒提前返回
    server_runner.CloseAndJoin();
    CHECK(!server_runner.error);
    std::cout << "seal_delay_beyond_deadline elapsed_ms=" << elapsed << " close=1\n";
}

static void RunAuthPhase(bool after_check) {
    struct Counter : EchoHandler {
        std::atomic<int> reads{0};
        void OnRead(Connection connection) override {
            reads.fetch_add(1);
            EchoHandler::OnRead(connection);
        }
    };
    auto handler = std::make_shared<Counter>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    std::atomic<bool> entered{false};
    std::atomic<bool> release_barrier{false};
    auto barrier = [&] {
        entered.store(true);
        while (!release_barrier.load()) {
            std::this_thread::yield();
        }
    };
    auto client = ConnectClient(addresses[0]);
    // 先让 accept 完成（Adopt 授权屏障也会用同一 hook），再对 OnRead 设屏障。
    CHECK(WaitUntil([&] { return handler->accepted.load() == 1; }, 3000ms));
    if (after_check) {
        EngineTestAccess::SetAuthBarriers(engine, {}, barrier);
    } else {
        EngineTestAccess::SetAuthBarriers(engine, barrier, {});
    }
    CHECK(SendAll(client, "x", 1000ms));
    CHECK(WaitUntil([&] { return entered.load(); }, 3000ms));
    engine.RequestClose();
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    release_barrier.store(true);
    runner.CloseAndJoin();
    CHECK(!runner.error);
    if (after_check) {
        CHECK(WaitUntil([&] { return handler->reads.load() == 1; }, 2000ms));
    } else {
        CHECK(handler->reads.load() == 0);
    }
    EngineTestAccess::SetAuthBarriers(engine, {}, {});
    (void)client.Close();
    std::cout << "callback_auth " << (after_check ? "after" : "before")
              << "_check reads=" << handler->reads.load() << "\n";
}

static void CallbackAuthBarriers() {
    StartRuntime(3);
    RunAuthPhase(false);
    RunAuthPhase(true);
}



// 公共建连单一交付（D5）：无 handler 的 Engine 上 AsyncConnect 零提交拒绝，
// out_id 保持原值、不产生 OnConnect；同一 Engine 上 CoDial 才是正例交付。
// 早到完成（owner 先完成、后领取）由 connect_complete_before_wait 经内部
// 两段式覆盖，Scenario 不再依赖已删除的公开 awaitable 提交。
static void AsyncConnectPublicFastComplete() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);
    Engine client{std::shared_ptr<RawHandler>()};
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    const std::uint64_t sentinel = 0x5A5A;
    std::uint64_t id = sentinel;
    CHECK(client.AsyncConnect(address, id)
          == std::make_error_code(std::errc::operation_not_supported));
    CHECK(id == sentinel);                               // 拒绝不改 out_id
    CHECK(EngineTestAccess::ConnectTableSize(client) == 0);  // 零提交
    CHECK(EngineTestAccess::PendingConnectCount(client) == 0);
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(50ms);
    CHECK(EngineTestAccess::ConnectionCount(client) == 0);   // 无隐式建连/无 fd 遗留

    std::atomic<int> result{-2};
    auto task = Task([&] {
        auto res = client.CoDial(address, Until(2000ms));
        if (!res.error && res.connection.Valid()) {
            result.store(0);
            res.connection.RequestClose();
        } else {
            result.store(res.error.value());
        }
    });
    Get(task);
    CHECK(result.load() == 0);  // CoDial 是唯一 Co 建连交付
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectionCount(client) == 0; }, 2000ms));
    CHECK(EngineTestAccess::ConnectTableSize(client) == 0);
    Address peer;
    std::error_code ignored;
    (void)listener.Accept(peer, ignored);
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "async_connect_public_fast_complete raw_no_handler=rejected codial=ok\n";
}

static void AsyncConnectRawNotWaitable() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);
    struct RawRec : RawHandler {
        std::atomic<int> connects{0};
        void OnConnect(Connection connection, std::error_code, std::uint64_t) override {
            connects.fetch_add(1);
            if (connection.Valid()) {
                connection.RequestClose();
            }
        }
    };
    auto handler = std::make_shared<RawRec>();
    Engine client(handler);
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    EngineTestAccess::SetConnectDeliveryDelay(client, std::chrono::milliseconds(200));
    std::uint64_t id = 0;
    CHECK(!client.AsyncConnect(address, id));
    // WaitConnect 先 attach，raw 交付后必须唯一 raw 回调，不得二次领取。
    std::atomic<int> result{-2};
    auto task = Task([&] {
        Connection connection;
        result.store(CoAccess::WaitConnect(client, id, connection, Until(2000ms)).value());
    });
    Get(task);
    CHECK(result.load() == std::make_error_code(std::errc::operation_not_supported).value());
    // WaitConnect 拒绝后 raw 仍按原责任完成一次 OnConnect（200ms 延迟交付）。
    CHECK(WaitUntil([&] { return handler->connects.load() == 1; }, 2000ms));
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 2000ms));
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "async_connect_raw_not_waitable raw_once=ok\n";
}


static void WaitConnectAddFailureRecycles() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);
    Engine client{std::shared_ptr<RawHandler>()};
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    // 延迟交付：先填满 op hub，再让 Add 失败；owner 稍后交付时无接收者必须回收。
    EngineTestAccess::SetConnectDeliveryDelay(client, std::chrono::milliseconds(2000));
    std::uint64_t id = 0;
    CHECK(!CoAccess::SubmitConnect(client, address, id));
    std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>> keep;
    CHECK(EngineTestAccess::FillConnectHub(client, id, keep) == WakeHub::kMaxWaiters);
    std::atomic<int> result{-2};
    auto task = Task([&] {
        Connection connection;
        result.store(CoAccess::WaitConnect(client, id, connection, Until(2000ms)).value());
        if (connection.Valid()) {
            connection.RequestClose();
        }
    });
    Get(task);
    CHECK(result.load() == std::make_error_code(std::errc::no_buffer_space).value());
    // 稍后 owner 交付：abandoned + 无 raw 接收者 → 回收，不遗留连接/fd。
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 4000ms));
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectionCount(client) == 0; }, 4000ms));
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "waitconnect_add_failure_recycles orphan=0\n";
}



static void RunAcceptAuthPhase(bool after_check) {
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    std::atomic<bool> entered{false};
    std::atomic<bool> release_barrier{false};
    auto barrier = [&] {
        entered.store(true);
        while (!release_barrier.load()) {
            std::this_thread::yield();
        }
    };
    if (after_check) {
        EngineTestAccess::SetAuthBarriers(engine, {}, barrier);
    } else {
        EngineTestAccess::SetAuthBarriers(engine, barrier, {});
    }
    auto client = ConnectClient(addresses[0]);
    CHECK(WaitUntil([&] { return entered.load(); }, 3000ms));
    engine.RequestClose();
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    release_barrier.store(true);
    runner.CloseAndJoin();
    CHECK(!runner.error);
    if (after_check) {
        CHECK(WaitUntil([&] { return handler->accepted.load() == 1; }, 2000ms));  // 已授权在途
    } else {
        CHECK(handler->accepted.load() == 0);  // 授权前封口不启动 OnAccept
    }
    EngineTestAccess::SetAuthBarriers(engine, {}, {});
    (void)client.Close();
    std::cout << "accept_auth " << (after_check ? "after" : "before")
              << "_check accepted=" << handler->accepted.load() << "\n";
}

static void AcceptAuthBarriers() {
    StartRuntime(2);
    RunAcceptAuthPhase(false);
    RunAcceptAuthPhase(true);
}

static void RunConnectAuthPhase(bool after_check) {
    Address address;
    auto listener = MakeListener(address);
    struct Rec : RawHandler {
        std::atomic<int> success{0};
        std::atomic<int> canceled{0};
        void OnConnect(Connection connection, std::error_code error,
                       std::uint64_t) override {
            if (error) {
                canceled.fetch_add(1);
            } else {
                success.fetch_add(1);
            }
            if (connection.Valid()) {
                connection.RequestClose();
            }
        }
    };
    auto handler = std::make_shared<Rec>();
    Engine client(handler);
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    std::atomic<bool> entered{false};
    std::atomic<bool> release_barrier{false};
    auto barrier = [&] {
        entered.store(true);
        while (!release_barrier.load()) {
            std::this_thread::yield();
        }
    };
    if (after_check) {
        EngineTestAccess::SetAuthBarriers(client, {}, barrier);
    } else {
        EngineTestAccess::SetAuthBarriers(client, barrier, {});
    }
    std::uint64_t id = 0;
    CHECK(!client.AsyncConnect(address, id));
    CHECK(WaitUntil([&] { return entered.load(); }, 3000ms));
    client.RequestClose();
    std::this_thread::sleep_for(std::chrono::milliseconds(30));
    release_barrier.store(true);
    runner.CloseAndJoin();
    CHECK(!runner.error);
    if (after_check) {
        CHECK(handler->success.load() == 1 && handler->canceled.load() == 0);
    } else {
        CHECK(handler->success.load() == 0 && handler->canceled.load() == 1);
    }
    EngineTestAccess::SetAuthBarriers(client, {}, {});
    std::cout << "connect_auth " << (after_check ? "after" : "before")
              << "_check success=" << handler->success.load()
              << " canceled=" << handler->canceled.load() << "\n";
}

static void ConnectAuthBarriers() {
    StartRuntime(2);
    RunConnectAuthPhase(false);
    RunConnectAuthPhase(true);
}


static void SealThrowReachesErrorExit() {
    StartRuntime(4);
    auto server_handler = std::make_shared<EchoHandler>();
    Engine server(server_handler);
    EngineRunner server_runner(server, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return server.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!server.LocalAddresses(addresses));
    struct Closer : RawHandler {
        std::atomic<int> connects{0};
        std::atomic<int> closes{0};
        void OnConnect(Connection connection, std::error_code error,
                       std::uint64_t) override {
            if (!error && connection.Valid()) {
                connects.fetch_add(1);  // 已交付 idle，等待 seal 关闭
            }
        }
        void OnClose(Connection, std::error_code) override { closes.fetch_add(1); }
    };
    auto handler = std::make_shared<Closer>();
    Engine client(handler);
    EngineRunner client_runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    std::uint64_t id = 0;
    CHECK(!client.AsyncConnect(addresses[0], id));
    CHECK(WaitUntil([&] { return server_handler->accepted.load() == 1; }, 3000ms));
    CHECK(WaitUntil([&] { return handler->connects.load() == 1; }, 3000ms));
    EngineTestAccess::SetSealThrowOnce(client);
    client.RequestClose();
    client_runner.CloseAndJoin();  // 必须返回（旧无 deadline + 异常会永久挂）
    CHECK(client_runner.error);   // 真实错误出口，不吞成成功
    CHECK(client.Phase() == EnginePhase::Stopped);
    CHECK(WaitUntil([&] { return handler->closes.load() == 1; }, 2000ms));  // 正常 owner 终态
    CHECK(EngineTestAccess::ConnectTableSize(client) == 0);
    server_runner.CloseAndJoin();
    CHECK(!server_runner.error);
    std::cout << "seal_throw_error_exit error=" << client_runner.error.value()
              << " phase=Stopped\n";
}

static void InstallDelayNoDeadline() {
    StartRuntime(4);
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineTestAccess::SetInstallDelay(engine, std::chrono::milliseconds(2500));
    const auto begin = std::chrono::steady_clock::now();
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 5000ms));
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - begin).count();
    CHECK(elapsed >= 2400);  // 旧 2s deadline 会提前返回
    CHECK(!runner.error);
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "install_delay_no_deadline elapsed_ms=" << elapsed << "\n";
}


static void DirtyRemoveEraseJoint() {
    StartRuntime(2);
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    auto client = ConnectClient(addresses[0]);
    CHECK(WaitUntil([&] { return handler->accepted.load() == 1; }, 3000ms));
    std::shared_ptr<Connection> conn;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        conn = handler->stored;
    }
    CHECK(conn && conn->Valid());

    // 屏障恰在「撤链后、registry 擦除前」：必须仍持 dirty 锁，晚 MarkDirty
    // 不能在此期间看到 registry 仍在而重新入链。
    std::atomic<bool> erase_entered{false};
    std::atomic<bool> release{false};
    EngineTestAccess::SetRemoveBarrierOnce(engine, [&] {
        erase_entered.store(true);
        while (!release.load()) {
            std::this_thread::yield();
        }
    });
    std::thread external([&] {
        WriteOperation op;
        (void)conn->AsyncWrite("x", op);  // 触发 WakeOwner → MarkDirty，与撤销交错
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    conn->RequestClose();
    EngineTestAccess::KickOwner(engine);  // owner 移除该 conn，进入撤链/擦表临界区
    CHECK(WaitUntil([&] { return erase_entered.load(); }, 3000ms));
    release.store(true);
    external.join();
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectionCount(engine) == 0; }, 2000ms));
    CHECK(EngineTestAccess::DirtyCount(engine) == 0);  // 晚 Wake 拒绝，无悬空节点
    (void)client.Close();
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "dirty_remove_erase_joint late_wake_rejected=ok\n";
}


static void RawPendingWaitRejected() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);
    struct RawRec : RawHandler {
        std::atomic<int> connects{0};
        std::atomic<int> canceled{0};
        void OnConnect(Connection connection, std::error_code error,
                       std::uint64_t) override {
            if (error) {
                canceled.fetch_add(1);
            } else {
                connects.fetch_add(1);
            }
            if (connection.Valid()) {
                connection.RequestClose();
            }
        }
    };
    auto handler = std::make_shared<RawRec>();
    Engine client(handler);
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    EngineTestAccess::SetConnectStartDelay(client, std::chrono::milliseconds(300));
    std::uint64_t id = 0;
    CHECK(!client.AsyncConnect(address, id));
    std::atomic<int> result{-2};
    auto task = Task([&] {
        Connection connection;
        result.store(CoAccess::WaitConnect(client, id, connection, Until(1ms)).value());
    });
    Get(task);
    CHECK(result.load() == std::make_error_code(std::errc::operation_not_supported).value());
    // raw-only：等待拒绝后底层 pending 建连继续，raw 唯一成功回调，不被取消。
    CHECK(WaitUntil([&] { return handler->connects.load() == 1; }, 3000ms));
    CHECK(handler->canceled.load() == 0);
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 2000ms));
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "raw_pending_wait_rejected raw_success=1 canceled=0\n";
}

static void CoDialRawRejected() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);
    struct RawRec : RawHandler {
        std::atomic<int> connects{0};
        void OnConnect(Connection connection, std::error_code, std::uint64_t) override {
            connects.fetch_add(1);
            if (connection.Valid()) {
                connection.RequestClose();
            }
        }
    };
    auto handler = std::make_shared<RawRec>();
    Engine client(handler);
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    std::atomic<int> result{-2};
    auto task = Task([&] {
        const auto res = client.CoDial(address, Until(1000ms));
        result.store(res.error.value());
    });
    Get(task);
    CHECK(result.load() == std::make_error_code(std::errc::operation_not_supported).value());
    CHECK(EngineTestAccess::ConnectTableSize(client) == 0);  // 未创建不可领取 op
    CHECK(handler->connects.load() == 0);
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "codial_raw_rejected no_op=1\n";
}

static void WaitConnectSingleReceiver() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);
    Engine client{std::shared_ptr<RawHandler>()};
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    EngineTestAccess::SetConnectDeliveryDelay(client, std::chrono::milliseconds(300));
    std::uint64_t id = 0;
    CHECK(!CoAccess::SubmitConnect(client, address, id));
    std::atomic<int> first_result{-2};
    auto first = Task([&] {
        Connection connection;
        const auto error = CoAccess::WaitConnect(client, id, connection, Until(2000ms));
        if (!error && connection.Valid()) {
            first_result.store(0);
            connection.RequestClose();
        } else {
            first_result.store(error.value());
        }
    });
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectReceiverClaimed(client, id); }, 2000ms));
    Connection second_conn;
    const auto second = CoAccess::WaitConnect(client, id, second_conn, Until(1000ms));
    CHECK(second == std::make_error_code(std::errc::device_or_resource_busy));
    Get(first);
    CHECK(first_result.load() == 0);  // 第一接收者在第二次拒绝后仍成功领取
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 2000ms));
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "waitconnect_single_receiver second_busy_first_ok\n";
}


static void RunStagedCloseBeforeRelease(bool engine_close) {
    auto handler = std::make_shared<StoreHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));

    std::atomic<bool> hook_entered{false};
    std::atomic<bool> holder_held{false};
    std::atomic<bool> release_hold{false};
    std::atomic<bool> holder_done{false};
    std::shared_ptr<Connection> conn1;
    std::thread holder([&] {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!hook_entered.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        {
            std::lock_guard<std::mutex> lock(handler->mutex);
            if (handler->stored) {
                conn1 = std::make_shared<Connection>(*handler->stored);
            }
        }
        if (!conn1) {
            return;
        }
        auto hold = ConnectionTestAccess::HoldForTest(ConnectionTestAccess::StateOf(*conn1));
        if (!hold) {
            return;
        }
        holder_held.store(true);
        while (!release_hold.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        ConnectionTestAccess::Reset(hold);
        holder_done.store(true);
    });
    EngineTestAccess::SetReadAppendHook(engine, [&] {
        hook_entered.store(true);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!holder_held.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
    });
    auto client = ConnectClient(addresses[0]);
    CHECK(SendAll(client, "first", 1000ms));
    CHECK(WaitUntil([&] { return holder_held.load(); }, 5000ms));
    // hold 尚未提交（数据锁被 holder 持有，owner 已非阻塞 defer）。
    CHECK(WaitUntil([&] { return EngineTestAccess::RecvHoldBytes(engine) == 5; }, 3000ms));
    if (engine_close) {
        engine.RequestClose();          // 关闭发生在 holder 仍持保护、hold 未提交时
        runner.CloseAndJoin();
        CHECK(!runner.error);
    } else {
        conn1->RequestClose();
        EngineTestAccess::KickOwner(engine);
        CHECK(WaitUntil([&] { return EngineTestAccess::ConnectionCount(engine) == 0; },
                        3000ms));
    }
    release_hold.store(true);
    holder.join();
    CHECK(holder_done.load());
    // backend 已关闭/收口后，残留 Conn 仍须读到完整 tail（detached → ring）。
    std::string tail;
    for (int i = 0; i < 200 && tail.size() < 5; ++i) {
        std::string chunk;
        const auto result = conn1->TryRead(chunk);
        if (result.status == ReadStatus::Ok) {
            tail += chunk;
            continue;
        }
        if (result.status == ReadStatus::Busy) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        break;
    }
    CHECK(tail == "first");
    EngineTestAccess::SetReadAppendHook(engine, {});
    (void)client.Close();
    if (!engine_close) {
        runner.CloseAndJoin();
        CHECK(!runner.error);
    }
    std::cout << "staged_close_before_release " << (engine_close ? "engine" : "conn")
              << "_close tail=first\n";
}

static void StagedCloseBeforeRelease() {
    StartRuntime(3);
    RunStagedCloseBeforeRelease(false);
    RunStagedCloseBeforeRelease(true);
}


static void InstallWaitDriverFailedExit() {
    StartRuntime(4);
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineTestAccess::SetInstallDelay(engine, std::chrono::milliseconds(500));
    EngineTestAccess::SetWaitDriverFailedOnce(engine);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    runner.CloseAndJoin();
    CHECK(runner.error);  // 安装 future 未 ready，但等待谓词命中 Failed → 失败出口
    CHECK(engine.Phase() == EnginePhase::Stopped
          || engine.Phase() == EnginePhase::Closing);
    std::cout << "install_wait_driver_failed exit=1 error=" << runner.error.value() << "\n";
}

static void SealWaitDriverFailedExit() {
    StartRuntime(4);
    auto server_handler = std::make_shared<EchoHandler>();
    Engine server(server_handler);
    EngineRunner server_runner(server, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return server.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!server.LocalAddresses(addresses));
    Engine client{std::shared_ptr<RawHandler>()};
    EngineRunner client_runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    std::uint64_t id = 0;
    CHECK(!CoAccess::SubmitConnect(client, addresses[0], id));
    CHECK(WaitUntil([&] { return server_handler->accepted.load() == 1; }, 3000ms));
    EngineTestAccess::SetSealDelay(client, std::chrono::milliseconds(500));
    EngineTestAccess::SetWaitDriverFailedOnce(client);
    client.RequestClose();
    client_runner.CloseAndJoin();  // seal future 未 ready，但谓词命中 → 不死等
    CHECK(client_runner.error);
    CHECK(client.Phase() == EnginePhase::Stopped
          || client.Phase() == EnginePhase::Closing);
    server_runner.CloseAndJoin();
    CHECK(!server_runner.error);
    std::cout << "seal_wait_driver_failed exit=1 error=" << client_runner.error.value() << "\n";
}


static void RequestCloseWriteErrorIntent() {
    auto sealed = std::make_shared<std::atomic<bool>>(false);
    ConnectionState requested(1001, 64, 64, 4, sealed);
    requested.owner_dirty = [] {};
    std::shared_ptr<WriteOpRecord> pending;
    CHECK(!requested.AcceptWrite("pending", pending));
    CHECK(!requested.CloseRequested());  // owner already passed its close gate
    const auto reason = std::make_error_code(std::errc::operation_canceled);
    requested.RequestClose(reason);
    requested.RequestClose(std::make_error_code(std::errc::permission_denied));
    CHECK(requested.CloseRequested());
    CHECK(!requested.CanAcceptWrite(1));
    requested.PumpWrites(1, 64,
        [](const std::string_view*, std::size_t, std::size_t) -> ssize_t {
            errno = EPIPE;
            return -1;
        });
    CHECK(requested.CloseRequested());
    CHECK(!requested.CanAcceptWrite(1));
    std::shared_ptr<WriteOpRecord> rejected;
    CHECK(requested.AcceptWrite("new", rejected) == make_error_code(NetErrc::Closed));
    {
        std::lock_guard<std::mutex> lock(pending->mutex);
        CHECK(pending->terminal && pending->error.value() == EPIPE);
    }
    CompletedWrite terminal;
    CHECK(requested.TryPopTerminal(terminal) && terminal.id == pending->id);
    CHECK(!requested.TryPopTerminal(terminal));
    requested.MarkClosed({});
    CHECK(requested.CloseReason() == reason);  // 默认/重复 reason 不覆盖首次请求
    CHECK(!requested.TryPopTerminal(terminal));  // owner final 不重复发布已结算写
    CHECK(ConnectionTestAccess::MakeWrite(pending).Query().error.value() == EPIPE);

    ConnectionState default_reason(1003, 64, 64, 4, sealed);
    default_reason.owner_dirty = [] {};
    default_reason.RequestClose({});
    default_reason.RequestClose(reason);
    default_reason.MarkClosed(reason);
    CHECK(!default_reason.CloseReason());  // 首次默认 reason 也是固定值

    ConnectionState no_owner(1005, 64, 64, 4, sealed);
    no_owner.RequestClose({});
    no_owner.RequestClose(reason);
    CHECK(no_owner.Closed() && no_owner.CloseRequested() && !no_owner.CloseReason());

    ConnectionState ordinary(1002, 64, 64, 4, sealed);
    ordinary.owner_dirty = [] {};
    std::shared_ptr<WriteOpRecord> failed;
    CHECK(!ordinary.AcceptWrite("pending", failed));
    ordinary.PumpWrites(1, 64,
        [](const std::string_view*, std::size_t, std::size_t) -> ssize_t {
            errno = EPIPE;
            return -1;
        });
    CHECK(!ordinary.CloseRequested());
    CHECK(ordinary.Acceptance() == AcceptanceState::Open);
    std::shared_ptr<WriteOpRecord> after_error;
    CHECK(!ordinary.AcceptWrite("still-open", after_error));
    std::cout << "request_close_write_error_intent retained=true rejected=true ordinary_error_open=true\n";
}

static void SealedWriteErrorReadPublication() {
    StartRuntime(1);
    auto sealed = std::make_shared<std::atomic<bool>>(false);
    ConnectionState state(1004, 64, 64, 4, sealed);
    state.owner_dirty = [] {};
    std::shared_ptr<WriteOpRecord> pending;
    CHECK(!state.AcceptWrite("pending", pending));
    sealed->store(true);
    state.PumpWrites(1, 64,
        [](const std::string_view*, std::size_t, std::size_t) -> ssize_t {
            errno = EPIPE;
            return -1;
        });
    CHECK(state.Acceptance() == AcceptanceState::Closed);
    CHECK(!state.ring.ReadEof());  // 写接纳 Closed 尚不等于 owner 已发布读终态
    std::promise<void> registered;
    auto ready = registered.get_future();
    auto waiter = state.RegisterCloseWaiter();
    std::atomic<bool> notified{false};
    auto task = Task([&] {
        const auto status = waiter->WaitWithCallback(Until(2000ms), [&] {
            registered.set_value();
            return true;
        });
        notified.store(status == bbt::coroutine::WaitStatus::Completed);
    });
    CHECK(ready.wait_for(1000ms) == std::future_status::ready);
    CHECK(state.ring.TryAppend("tail").status == ReadStatus::Ok);
    state.MarkClosed({});
    Get(task);
    CHECK(notified.load() && state.ring.ReadClosed());
    std::string tail;
    CHECK(state.ring.TryRead(tail).status == ReadStatus::Ok && tail == "tail");
    CHECK(state.ring.TryRead(tail).status == ReadStatus::Eof);
    state.MarkClosed(std::make_error_code(std::errc::io_error));
    CHECK(!state.CloseReason());
    std::cout << "sealed_write_error_read_publication close_waiter=completed tail_before_eof=1\n";
}

static void RequestCloseWriteReadiness() {
    StartRuntime(3);
    struct Handler : RawHandler {
        std::promise<Connection> accepted;
        std::atomic<int> writes{0};
        std::atomic<int> closes{0};
        std::atomic<bool> write_checked{false};
        std::atomic<bool> close_checked{false};
        void OnAccept(Connection connection) override { accepted.set_value(connection); }
        void OnWrite(Connection connection, WriteResult result) override {
            WriteOperation rejected;
            // 内部状态断言经 TestAccess：公共句柄不再暴露 State()。
            const auto state = ConnectionTestAccess::StateOf(connection);
            write_checked.store(result.error.value() == EPIPE && result.sent == 0
                && state && state->CloseRequested()
                && !state->CanAcceptWrite(1)
                && connection.AsyncWrite("must-reject", rejected) == make_error_code(NetErrc::Closed));
            writes.fetch_add(1);
        }
        void OnClose(Connection connection, std::error_code reason) override {
            close_checked.store(connection.Closed() && !reason && !connection.CloseReason());
            closes.fetch_add(1);
        }
    };
    auto handler = std::make_shared<Handler>();
    auto accepted = handler->accepted.get_future();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    auto client = ConnectClient(addresses[0]);
    CHECK(accepted.wait_for(3000ms) == std::future_status::ready);
    auto connection = accepted.get();
    CHECK(SendAll(client, "tail", 1000ms));
    CHECK(WaitUntil([&] { return connection.ReadableBytes() == 4; }, 3000ms));
    const int fd = EngineTestAccess::FirstConnectionFd(engine);
    CHECK(fd >= 0);
    std::promise<void> ready_entered;
    auto ready = ready_entered.get_future();
    std::promise<void> release_ready;
    auto release = release_ready.get_future().share();
    std::atomic<bool> barrier_released{false};
    EngineTestAccess::SetWriteReadyBarrierOnce(engine, [&] {
        ready_entered.set_value();
        barrier_released.store(release.wait_for(5000ms) == std::future_status::ready);
    });
    g_eagain_sendmsg_fd.store(fd);
    WriteOperation pending;
    CHECK(!connection.AsyncWrite("pending", pending));
    CHECK(ready.wait_for(3000ms) == std::future_status::ready);
    CHECK(g_eagain_sendmsg_fd.load() == -1);  // 真实 EAGAIN→ArmWrite→readiness
    connection.RequestClose();
    connection.RequestClose(std::make_error_code(std::errc::permission_denied));
    CHECK(!connection.Closed() && !connection.ReadEof());
    CHECK(::shutdown(fd, SHUT_WR) == 0);  // 下一次真实 sendmsg 必然 EPIPE
    release_ready.set_value();
    CHECK(WaitUntil([&] { return handler->closes.load() == 1; }, 3000ms));
    // 分别检查屏障、写终态和关闭终态，失败时保留具体归因。
    CHECK(barrier_released.load());
    CHECK(handler->write_checked.load());
    CHECK(handler->close_checked.load());
    CHECK(handler->writes.load() == 1);
    const auto outcome = pending.Query();
    CHECK(outcome.phase == WritePhase::Done && outcome.error.value() == EPIPE && outcome.sent == 0);
    std::string tail;
    CHECK(connection.TryRead(tail).status == ReadStatus::Ok && tail == "tail");
    CHECK(connection.TryRead(tail).status == ReadStatus::Eof);
    connection.RequestClose(std::make_error_code(std::errc::io_error));
    CHECK(connection.Closed() && !connection.CloseReason());
    CHECK(connection.WriteAcceptance() == AcceptanceState::Closed);
    runner.CloseAndJoin();
    CHECK(!runner.error && handler->closes.load() == 1 && handler->writes.load() == 1);
    CHECK(pending.Query().error == outcome.error);
    std::cout << "request_close_write_readiness eagain=1 epipe=1 rejected=1 onwrite=1 onclose=1 tail_before_eof=1\n";
}

static void RequestCloseReadTerminalOrder() {
    StartRuntime(3);
    auto handler = std::make_shared<StoreHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));

    std::atomic<bool> hook_entered{false};
    std::atomic<bool> holder_held{false};
    std::atomic<bool> release_hold{false};
    std::shared_ptr<Connection> conn1;
    std::thread holder([&] {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!hook_entered.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        {
            std::lock_guard<std::mutex> lock(handler->mutex);
            if (handler->stored) {
                conn1 = std::make_shared<Connection>(*handler->stored);
            }
        }
        if (!conn1) {
            return;
        }
        auto hold = ConnectionTestAccess::HoldForTest(ConnectionTestAccess::StateOf(*conn1));
        if (!hold) {
            return;
        }
        holder_held.store(true);
        while (!release_hold.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
        ConnectionTestAccess::Reset(hold);
    });
    EngineTestAccess::SetReadAppendHook(engine, [&] {
        hook_entered.store(true);
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (!holder_held.load() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::yield();
        }
    });
    auto client = ConnectClient(addresses[0]);
    CHECK(SendAll(client, "first", 1000ms));
    CHECK(WaitUntil([&] { return holder_held.load(); }, 5000ms));
    CHECK(WaitUntil([&] { return EngineTestAccess::RecvHoldBytes(engine) == 5; }, 3000ms));

    std::atomic<bool> shutdown_entered{false};
    std::atomic<bool> release_shutdown{false};
    EngineTestAccess::SetShutdownBarrierOnce(engine, [&] {
        shutdown_entered.store(true);
        while (!release_shutdown.load()) {
            std::this_thread::yield();
        }
    });
    conn1->RequestClose();  // 读侧最终 EOF 尚未由 owner 发布
    CHECK(WaitUntil([&] { return shutdown_entered.load(); }, 3000ms));
    release_hold.store(true);  // 短保护释放，owner 仍暂停在 stage 之前
    holder.join();
    std::string early;
    const auto early_result = conn1->TryRead(early);
    CHECK(early_result.status != ReadStatus::Eof);  // 不得 EOF
    CHECK(early_result.status != ReadStatus::Ok || early.empty());
    release_shutdown.store(true);
    CHECK(WaitUntil([&] { return handler->closed.load() == 1; }, 3000ms));
    std::string tail;
    for (int i = 0; i < 200 && tail.size() < 5; ++i) {
        std::string chunk;
        const auto result = conn1->TryRead(chunk);
        if (result.status == ReadStatus::Ok) {
            tail += chunk;
            continue;
        }
        if (result.status == ReadStatus::Busy) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            continue;
        }
        break;
    }
    CHECK(tail == "first");
    std::string eof;
    CHECK(conn1->TryRead(eof).status == ReadStatus::Eof);
    EngineTestAccess::SetReadAppendHook(engine, {});
    (void)client.Close();
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "request_close_read_terminal_order early=not_eof tail=first eof=last\n";
}

static void StageDetachedNotifyFaultNoDuplicate() {
    ReceiveRing ring(64);
    ring.ReserveDetached(8);
    ReceiveRing::SetNotifyFaultOnceForTest();
    CHECK(ring.StageDetached("abc"));  // 已提交；通知失败不逆转提交
    std::string out;
    CHECK(ring.TryRead(out).status == ReadStatus::Ok);
    CHECK(out == "abc");
    CHECK(ring.Size() == 0);
    std::string again;
    CHECK(ring.TryRead(again).status == ReadStatus::WouldBlock);
    std::cout << "stage_detached_notify_fault no_duplicate=ok\n";
}

static void SealCallbacksUniqueWithPendingWrite() {
    StartRuntime(4);
    auto server_handler = std::make_shared<EchoHandler>();
    Engine server(server_handler);
    EngineRunner server_runner(server, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return server.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!server.LocalAddresses(addresses));
    struct H : RawHandler {
        std::mutex mutex;
        std::shared_ptr<Connection> conn;
        std::set<std::uint64_t> writes;
        std::atomic<int> closes{0};
        void OnConnect(Connection connection, std::error_code error,
                       std::uint64_t) override {
            if (!error && connection.Valid()) {
                std::lock_guard<std::mutex> lock(mutex);
                conn = std::make_shared<Connection>(connection);
            }
        }
        void OnWrite(Connection, WriteResult result) override {
            std::lock_guard<std::mutex> lock(mutex);
            writes.insert(result.operation);
        }
        void OnClose(Connection, std::error_code) override { closes.fetch_add(1); }
    };
    auto handler = std::make_shared<H>();
    Engine client(handler);
    EngineRunner client_runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    std::uint64_t id = 0;
    CHECK(!client.AsyncConnect(addresses[0], id));
    CHECK(WaitUntil([&] { return handler->conn != nullptr; }, 3000ms));
    std::shared_ptr<Connection> conn;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        conn = handler->conn;
    }
    CHECK(conn && conn->Valid());
    WriteOperation pending;
    CHECK(!conn->AsyncWrite("pending", pending));
    ReceiveRing::SetNotifyFaultOnceForTest();  // 关闭通知路径一次失败
    client.RequestClose();
    client_runner.CloseAndJoin();
    CHECK(!client_runner.error);  // 通知失败被隔离，本地异常不冒 Failed
    CHECK(WaitUntil([&] { return handler->closes.load() == 1; }, 2000ms));
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        CHECK(handler->writes.count(pending.Id()) == 1);
    }
    server_runner.CloseAndJoin();
    CHECK(!server_runner.error);
    std::cout << "seal_notify_fault close=1 write=1\n";
}

static void WaitConnectClaimCreateFailureRecovers() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);
    Engine client{std::shared_ptr<RawHandler>()};
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    EngineTestAccess::SetConnectDeliveryDelay(client, std::chrono::milliseconds(300));
    std::uint64_t id = 0;
    CHECK(!CoAccess::SubmitConnect(client, address, id));
    EngineTestAccess::FailNextConnectWaiterCreateOnce(client);
    std::atomic<int> first{-2};
    auto task1 = Task([&] {
        Connection connection;
        first.store(CoAccess::WaitConnect(client, id, connection, Until(1000ms)).value());
    });
    Get(task1);
    CHECK(first.load() == std::make_error_code(std::errc::not_enough_memory).value());
    std::atomic<int> second{-2};
    auto task2 = Task([&] {
        Connection connection;
        const auto error = CoAccess::WaitConnect(client, id, connection, Until(2000ms));
        if (!error && connection.Valid()) {
            second.store(0);
            connection.RequestClose();
        } else {
            second.store(error.value());
        }
    });
    Get(task2);
    CHECK(second.load() == 0);  // claim 已回滚，同 id 可正常领取
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 2000ms));
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "waitconnect_claim_create_failure recovered=ok\n";
}

#include "ProcessRuntimeTestMain.hpp"

static void DumpBacktrace(int signal_number) {
    void* frames[64];
    const int count = backtrace(frames, 64);
    std::cerr << "caught signal " << signal_number << '\n';
    backtrace_symbols_fd(frames, count, 2);
    std::_Exit(139);
}

static int RunCases(int argc, char** argv) {
    try {
        CHECK(argc == 2);
        const std::string test = argv[1];
        if (test == "ring_alloc_no_consume") { RingAllocNoConsume(); }
        else if (test == "ring_release_notify") { RingReleaseNotify(); }
        else if (test == "hub_early_notify_retained") { HubEarlyNotifyRetained(); }
        else if (test == "write_total_ops_bound") { WriteTotalOpsBound(); }
        else if (test == "write_query_monotonic") { WriteQueryMonotonic(); }
        else if (test == "onwrite_throw_continues") { OnWriteThrowContinues(); }
        else if (test == "wait_accepted_existing_out") { WaitAcceptedExistingOut(); }
        else if (test == "run_preconditions") { RunPreconditions(); }
        else if (test == "engine_driver_failed") { EngineDriverFailed(); }
        else if (test == "invalid_options_rejected") { InvalidOptionsRejected(); }
        else if (test == "handoff_full_resume") { HandoffFullResume(); }
        else if (test == "lb_subs") { LbSubs(); }
        else if (test == "fairness") { Fairness(); }
        else if (test == "ring_append_fault_shared_out") { RingAppendFaultSharedOut(); }
        else if (test == "co_read_append_fault_nonempty_out") { CoReadAppendFaultNonEmptyOut(); }
        else if (test == "co_read_idle_no_spin") { CoReadIdleNoSpin(); }
        else if (test == "staged_close_tail") { StagedCloseTail(); }
        else if (test == "connect_complete_before_wait") { ConnectCompleteBeforeWait(); }
        else if (test == "connect_timeout_recycles_adopt") { ConnectTimeoutRecyclesAdopt(); }
        else if (test == "connect_hard_cap") { ConnectHardCap(); }
        else if (test == "connect_raw_completion_cleanup") { ConnectRawCompletionCleanup(); }
        else if (test == "lb_least_connections") { LbLeastConnections(); }
        else if (test == "socket_options_getsockopt") { SocketOptionsGetsockopt(); }
        else if (test == "install_failure_fd_baseline") { InstallFailureFdBaseline(); }
        else if (test == "run_managed_domain_reject") { RunManagedDomainReject(); }
        else if (test == "co_read_rapid") { CoReadRapid(); }
        else if (test == "write_terminal_no_alloc") { WriteTerminalNoAlloc(); }
        else if (test == "write_capacity_after_drain") { WriteCapacityAfterDrain(); }
        else if (test == "write_capacity_after_partial") { WriteCapacityAfterPartial(); }
        else if (test == "accept_after_close") { AcceptAfterClose(); }
        else if (test == "connect_cancel_late_callback") { ConnectCancelLateCallback(); }
        else if (test == "fairness_one_sub") { FairnessOneSub(); }
        else if (test == "staged_recv_busy_close") { StagedRecvBusyClose(); }
        else if (test == "ring_deferred_backing_reuse") { RingDeferredBackingReuse(); }
        else if (test == "ring_deferred_nonblocking_try") { RingDeferredNonblockingTry(); }
        else if (test == "async_connect_public_fast_complete") { AsyncConnectPublicFastComplete(); }
        else if (test == "async_connect_raw_not_waitable") { AsyncConnectRawNotWaitable(); }
        else if (test == "waitconnect_add_failure_recycles") { WaitConnectAddFailureRecycles(); }
        else if (test == "accept_auth_barriers") { AcceptAuthBarriers(); }
        else if (test == "connect_auth_barriers") { ConnectAuthBarriers(); }
        else if (test == "seal_throw_error_exit") { SealThrowReachesErrorExit(); }
        else if (test == "install_delay_no_deadline") { InstallDelayNoDeadline(); }
        else if (test == "dirty_remove_erase_joint") { DirtyRemoveEraseJoint(); }
        else if (test == "raw_pending_wait_rejected") { RawPendingWaitRejected(); }
        else if (test == "codial_raw_rejected") { CoDialRawRejected(); }
        else if (test == "waitconnect_single_receiver") { WaitConnectSingleReceiver(); }
        else if (test == "staged_close_before_release") { StagedCloseBeforeRelease(); }
        else if (test == "install_wait_driver_failed_exit") { InstallWaitDriverFailedExit(); }
        else if (test == "seal_wait_driver_failed_exit") { SealWaitDriverFailedExit(); }
        else if (test == "request_close_read_terminal_order") { RequestCloseReadTerminalOrder(); }
        else if (test == "request_close_write_error_intent") { RequestCloseWriteErrorIntent(); }
        else if (test == "request_close_write_readiness") { RequestCloseWriteReadiness(); }
        else if (test == "sealed_write_error_read_publication") { SealedWriteErrorReadPublication(); }
        else if (test == "stage_detached_notify_fault_no_duplicate") { StageDetachedNotifyFaultNoDuplicate(); }
        else if (test == "seal_notify_fault_callbacks") { SealCallbacksUniqueWithPendingWrite(); }
        else if (test == "waitconnect_claim_create_failure") { WaitConnectClaimCreateFailureRecovers(); }
        else if (test == "wakehub_capacity_bounded") { WakeHubCapacityBounded(); }
        else if (test == "co_read_hub_full_bounded") { CoReadHubFullBounded(); }
        else if (test == "dirty_lifetime_across_removeconn") { DirtyLifetimeAcrossRemoveConn(); }
        else if (test == "submit_gate_paused_before_post") { SubmitGatePausedBeforePost(); }
        else if (test == "codial_fast_complete") { CoDialFastComplete(); }
        else if (test == "codial_timeout_recycles") { CoDialTimeoutRecycles(); }
        else if (test == "codial_cancel_post_failure") { CoDialCancelPostFailureRetries(); }
        else if (test == "seal_delay_beyond_deadline") { SealDelayBeyondDeadline(); }
        else if (test == "callback_auth_barriers") { CallbackAuthBarriers(); }
        else if (test == "run_teardown_no_residue") { RunTeardownNoResidue(); }
        else if (test == "co_busy_release_phases") { CoBusyReleasePhases(); }
        else if (test == "co_wouldblock_closed_race") { CoWouldBlockClosedRace(); }
        else if (test == "co_pingpong_strict") { CoPingpongStrict(); }
        else if (test == "write_terminal_interleaved_no_alloc") { WriteTerminalInterleavedNoAlloc(); }
        else if (test == "raw_busy_deferred_onread") { RawBusyDeferredOnRead(); }
        else if (test == "seal_post_failure_retries") { SealPostFailureRetries(); }
        else { throw std::runtime_error("unknown test: " + test); }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}

int main(int argc, char** argv) {
    signal(SIGSEGV, DumpBacktrace);
    signal(SIGABRT, DumpBacktrace);
    return bbt_net_test::RunTestProcessExit([&] { return RunCases(argc, argv); });
}
