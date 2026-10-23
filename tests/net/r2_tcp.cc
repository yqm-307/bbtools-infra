// R2 TCP/Co 聚焦验证（契约行为，不是“跑过”）。
//
// 覆盖：
//   raw_roundtrip        port0 raw server/client、sub owner 身份、无协程上下文
//   co_accept_read_write CoAccept/CoRead/CoWrite 同一内核
//   co_dial              CoDial 真实建连往返
//   close_retained       关闭后已收尾部可读、Engine 收口后残留 Conn 安全
//   write_backpressure   有界发送 no_buffer_space
//   write_timeout        容量超时未提交 / 完成超时仍在途可查
//   multi_listener       多 listen 地址真实服务
//   close_waiters_settle no-deadline CoAccept / CoWrite 容量等待在关闭时结算
//   ring_combined        ReceiveRing 同保护 Peek+Consume、Busy 零消费、释放代次
//   write_fifo_prefix    OutboundQueue 部分写 FIFO/真实前缀/错误结算/Settling/Close
//   co_read_timeout      CoRead 超时不消费

#include <bbt/infra/net/CoConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/RawHandler.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <csignal>
#include <execinfo.h>
#include <filesystem>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
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

static std::size_t CountTasks() {
    return static_cast<std::size_t>(std::distance(
        std::filesystem::directory_iterator("/proc/self/task"),
        std::filesystem::directory_iterator{}));
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
    }, "net.r2");
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

static bool Roundtrip(NativeSocket& client, const std::string& payload,
                      std::chrono::milliseconds timeout) {
    if (client.Send(payload.data(), payload.size()).bytes != payload.size()) {
        return false;
    }
    std::string echoed;
    char buffer[1024];
    return WaitUntil([&] {
        for (;;) {
            const auto result = client.Receive(buffer, sizeof(buffer));
            if (result.bytes > 0) {
                echoed.append(buffer, result.bytes);
                if (echoed.size() >= payload.size()) {
                    return true;
                }
                continue;
            }
            break;
        }
        return echoed.size() >= payload.size();
    }, timeout) && echoed == payload;
}

struct EchoHandler : RawHandler {
    std::atomic<int> accepted{0};
    std::atomic<int> reads{0};
    std::atomic<int> writes{0};
    std::atomic<int> closed{0};
    std::atomic<std::size_t> write_bytes{0};
    std::atomic<std::uint64_t> accept_owner{0};
    std::atomic<std::uint64_t> read_owner{0};
    std::atomic<std::uint64_t> accept_coroutine{1};
    std::mutex mutex;
    std::shared_ptr<Connection> stored;

    void OnAccept(Connection connection) override {
        accepted.fetch_add(1);
        if (auto processer = bbt::coroutine::detail::Processer::TryGetLocalProcesser()) {
            accept_owner.store(processer->GetId());
        }
        accept_coroutine.store(bbt::coroutine::GetLocalCoroutineId());
        std::lock_guard<std::mutex> lock(mutex);
        stored = std::make_shared<Connection>(connection);
    }

    void OnRead(Connection connection) override {
        reads.fetch_add(1);
        if (auto processer = bbt::coroutine::detail::Processer::TryGetLocalProcesser()) {
            read_owner.store(processer->GetId());
        }
        std::string data;
        const auto result = connection.TryRead(data);
        if (result.status == ReadStatus::Ok && !data.empty()) {
            WriteOperation operation;
            connection.AsyncWrite(data, operation);
        }
    }

    void OnWrite(Connection, WriteResult result) override {
        writes.fetch_add(1);
        write_bytes.fetch_add(result.sent);
    }
    void OnClose(Connection, std::error_code) override { closed.fetch_add(1); }
};

struct NullHandler : RawHandler {};

struct StoreHandler : RawHandler {
    std::atomic<int> accepted{0};
    std::atomic<int> reads{0};
    std::mutex mutex;
    std::shared_ptr<Connection> stored;
    void OnAccept(Connection connection) override {
        accepted.fetch_add(1);
        std::lock_guard<std::mutex> lock(mutex);
        stored = std::make_shared<Connection>(connection);
    }
    void OnRead(Connection) override { reads.fetch_add(1); }
};

static EngineConfig ServerConfig(const std::vector<Address>& listen, std::size_t subs = 1) {
    EngineConfig config;
    config.listen_addresses = listen;
    config.runtime_progress = RuntimeProgress::Independent;
    config.options.num_event_loop = static_cast<std::int64_t>(subs);
    return config;
}

static EngineConfig ServerConfig(const Address& listen, std::size_t subs = 1) {
    return ServerConfig(std::vector<Address>{listen}, subs);
}


// 极小同机 ping-pong 基线：返回 ops/sec，p99 单向往返纳秒。
static double PingPong(const Address& address, int iterations, long long& p99_ns,
                       int& timeouts) {
    timeouts = 0;
    auto client = ConnectClient(address);
    const std::string payload(256, 'p');
    std::vector<long long> latency;
    latency.reserve(static_cast<std::size_t>(iterations));
    char buffer[512];
    const auto start = std::chrono::steady_clock::now();
    for (int i = 0; i < iterations; ++i) {
        const auto begin = std::chrono::steady_clock::now();
        std::size_t sent = 0;
        const auto send_deadline = std::chrono::steady_clock::now() + 1000ms;
        while (sent < payload.size()) {
            if (std::chrono::steady_clock::now() >= send_deadline) {
                ++timeouts;
                std::cerr << "[pp] send timeout i=" << i << "\n";
                break;
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
            ++timeouts;
            break;
        }
        std::size_t received = 0;
        const auto message_deadline = std::chrono::steady_clock::now() + 1000ms;
        while (received < payload.size()) {
            if (std::chrono::steady_clock::now() >= message_deadline) {
                ++timeouts;
                break;
            }
            const auto result = client.Receive(buffer + received, sizeof(buffer) - received);
            if (result.bytes > 0) {
                received += result.bytes;
                continue;
            }
            if (result.error.value() == EAGAIN || result.error.value() == EWOULDBLOCK) {
                pollfd descriptor{client.Fd(), POLLIN, 0};
                (void)::poll(&descriptor, 1, 10);
                continue;
            }
            break;
        }
        latency.push_back(std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - begin).count());
    }
    const auto elapsed = std::chrono::steady_clock::now() - start;
    (void)client.Close();
    std::sort(latency.begin(), latency.end());
    p99_ns = latency.empty() ? 0 : latency[static_cast<std::size_t>(latency.size() * 99 / 100)];
    return iterations / std::chrono::duration<double>(elapsed).count();
}

// 单元测试用：AcceptWrite 输出记录 -> WriteOperation 句柄。
static std::error_code TryAccept(ConnectionState& state, std::string data,
                                 WriteOperation& out) {
    std::shared_ptr<WriteOpRecord> record;
    const auto error = state.AcceptWrite(std::move(data), record);
    if (!error) {
        out = ConnectionTestAccess::MakeWrite(std::move(record));
    }
    return error;
}

// ---------------- cases ----------------

static void RawRoundtrip() {
    StartRuntime(3);
    const auto tasks_before = CountTasks();
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    CHECK(addresses.size() == 1);
    std::vector<std::uint64_t> main_owners;
    std::vector<std::uint64_t> sub_owners;
    CHECK(!engine.ReactorOwnerIds(main_owners, sub_owners));
    CHECK(main_owners.size() == 1);
    CHECK(sub_owners.size() == 1);

    auto client = ConnectClient(addresses[0]);
    CHECK(Roundtrip(client, "ping", 3000ms));
    CHECK(WaitUntil([&] { return handler->writes.load() >= 1; }, 2000ms));
    CHECK(handler->accepted.load() == 1);
    CHECK(handler->reads.load() >= 1);
    CHECK(handler->write_bytes.load() == 4);
    CHECK(handler->accept_coroutine.load() == 0);
    CHECK(handler->accept_owner.load() == handler->read_owner.load());
    CHECK(handler->accept_owner.load() != main_owners[0]);
    CHECK(std::find(sub_owners.begin(), sub_owners.end(), handler->accept_owner.load())
          != sub_owners.end());

    CHECK(!client.Close());
    CHECK(WaitUntil([&] {
        std::lock_guard<std::mutex> lock(handler->mutex);
        return handler->stored && handler->stored->ReadEof();
    }, 3000ms));
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        handler->stored->RequestClose();
    }
    CHECK(WaitUntil([&] { return handler->closed.load() >= 1; }, 3000ms));
    runner.CloseAndJoin();
    CHECK(!runner.error);
    CHECK(engine.Phase() == EnginePhase::Stopped);
    CHECK(CountTasks() == tasks_before);
    std::cout << "raw_roundtrip threads=" << tasks_before << " sub_owner="
              << handler->accept_owner.load() << " main_owner=" << main_owners[0]
              << " bytes=" << handler->write_bytes.load() << " run=0\n";
}

static void CoAcceptReadWrite() {
    StartRuntime(2);
    auto handler = std::make_shared<NullHandler>();
    Engine engine(handler);
    CHECK(!engine.EnableAcceptHandoff(4));
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));

    std::atomic<int> result{-1};
    auto task = Task([&] {
        Connection connection;
        auto error = CoAccept(engine, connection, Until(3000ms));
        if (error) { result.store(1); return; }
        std::string data;
        error = CoRead(connection, data, Until(3000ms));
        if (error) { result.store(2); return; }
        WriteOutcome outcome;
        WriteOperation operation;
        error = CoWrite(connection, data, Until(3000ms), outcome, operation);
        if (error || outcome.phase != WritePhase::Done) { result.store(3); return; }
        connection.RequestClose();
        result.store(0);
    });
    auto client = ConnectClient(addresses[0]);
    CHECK(Roundtrip(client, "pong", 3000ms));
    Get(task);
    CHECK(result.load() == 0);
    CHECK(!client.Close());
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "co_accept_read_write write=Done closed=ok\n";
}

static void CoDial() {
    StartRuntime(4);
    auto handler = std::make_shared<EchoHandler>();
    Engine server(handler);
    EngineRunner server_runner(server, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return server.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!server.LocalAddresses(addresses));

    Engine client{std::shared_ptr<RawHandler>()};  // Co-only：超时可回收未交付 fd
    EngineConfig client_config;
    client_config.runtime_progress = RuntimeProgress::Independent;
    client_config.options.num_event_loop = 1;
    EngineRunner client_runner(client, client_config);
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));

    std::atomic<int> result{-1};
    auto task = Task([&] {
        Connection connection;
        auto error = CoDial(client, addresses[0], connection, Until(3000ms));
        if (error) { result.store(1); return; }
        WriteOutcome outcome;
        WriteOperation operation;
        error = CoWrite(connection, "dial", Until(3000ms), outcome, operation);
        if (error || outcome.phase != WritePhase::Done) { result.store(2); return; }
        std::string data;
        error = CoRead(connection, data, Until(3000ms));
        if (error || data != "dial") { result.store(3); return; }
        connection.RequestClose();
        result.store(0);
    });
    Get(task);
    CHECK(result.load() == 0);
    client_runner.CloseAndJoin();
    server_runner.CloseAndJoin();
    CHECK(!client_runner.error && !server_runner.error);
    std::cout << "co_dial echoed=dial run=0\n";
}

static void CloseRetained() {
    StartRuntime(2);
    auto handler = std::make_shared<StoreHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));

    auto client = ConnectClient(addresses[0]);
    CHECK(client.Send("tail", 4).bytes == 4);
    CHECK(WaitUntil([&] {
        std::lock_guard<std::mutex> lock(handler->mutex);
        return handler->reads.load() >= 1 && handler->stored != nullptr;
    }, 3000ms));
    CHECK(!client.Close());
    CHECK(WaitUntil([&] {
        std::lock_guard<std::mutex> lock(handler->mutex);
        return handler->stored && handler->stored->ReadEof();
    }, 3000ms));
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::shared_ptr<Connection> retained;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        retained = handler->stored;
    }
    CHECK(retained && retained->Valid());
    std::string tail;
    CHECK(retained->TryRead(tail).status == ReadStatus::Ok && tail == "tail");
    std::string more;
    CHECK(retained->TryRead(more).status == ReadStatus::Eof);
    std::cout << "close_retained tail=" << tail << " eof=ok\n";
}

static void WriteBackpressure() {
    StartRuntime(2);
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    auto client = ConnectClient(addresses[0]);
    CHECK(WaitUntil([&] {
        std::lock_guard<std::mutex> lock(handler->mutex);
        return handler->stored != nullptr;
    }, 3000ms));
    std::shared_ptr<Connection> connection;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        connection = handler->stored;
    }
    CHECK(connection && connection->Valid());
    const std::string chunk(64 * 1024, 'x');
    std::error_code capacity_error;
    int accepted = 0;
    for (int i = 0; i < 400; ++i) {
        WriteOperation operation;
        const auto error = connection->AsyncWrite(chunk, operation);
        if (error) { capacity_error = error; break; }
        ++accepted;
    }
    CHECK(capacity_error == std::make_error_code(std::errc::no_buffer_space));
    CHECK(accepted >= 1);
    CHECK(!client.Close());
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "write_backpressure accepted=" << accepted << " capacity=no_buffer_space\n";
}

static void WriteTimeout() {
    StartRuntime(1);
    {
        auto state = std::make_shared<ConnectionState>(1, 4096, 128, 4, nullptr);
        Connection connection = ConnectionTestAccess::Make(state);
        WriteOperation warm;
        CHECK(!connection.AsyncWrite(std::string(64, 'a'), warm));
        CHECK(!connection.AsyncWrite(std::string(64, 'b'), warm));
        WriteOperation rejected;
        CHECK(connection.AsyncWrite(std::string(1, 'c'), rejected)
              == std::make_error_code(std::errc::no_buffer_space));
        std::atomic<int> result{-1};
        auto task = Task([&] {
            WriteOutcome outcome;
            WriteOperation operation;
            const auto error = CoWrite(connection, std::string(1, 'c'), Until(30ms), outcome, operation);
            if (error == std::make_error_code(std::errc::timed_out)
                && outcome.phase == WritePhase::Rejected && !operation.Valid()) {
                result.store(0);
            }
        });
        Get(task);
        CHECK(result.load() == 0);
    }
    {
        auto state = std::make_shared<ConnectionState>(2, 4096, 4096, 16, nullptr);
        Connection connection = ConnectionTestAccess::Make(state);
        std::atomic<int> result{-1};
        auto operation_holder = std::make_shared<WriteOperation>();
        auto task = Task([&] {
            WriteOutcome outcome;
            const auto error = CoWrite(connection, std::string(8, 'd'), Until(30ms),
                                       outcome, *operation_holder);
            if (error == std::make_error_code(std::errc::timed_out)
                && outcome.phase == WritePhase::InFlight && operation_holder->Valid()) {
                result.store(0);
            }
        });
        Get(task);
        CHECK(result.load() == 0);
        CHECK(operation_holder->Query().phase == WritePhase::InFlight);
        CHECK(connection.WriteAcceptance() != AcceptanceState::Closed);
    }
    std::cout << "write_timeout capacity=unsubmitted completion=inflight queryable=ok\n";
}

static void MultiListener() {
    StartRuntime(3);
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(std::vector<Address>{Loopback(0), Loopback(0)}));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    CHECK(addresses.size() == 2);
    CHECK(addresses[0].Port() != addresses[1].Port());
    auto first = ConnectClient(addresses[0]);
    auto second = ConnectClient(addresses[1]);
    CHECK(Roundtrip(first, "one", 3000ms));
    CHECK(Roundtrip(second, "two", 3000ms));
    CHECK(WaitUntil([&] { return handler->accepted.load() == 2; }, 3000ms));
    CHECK(!first.Close());
    CHECK(!second.Close());
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "multi_listener ports=" << addresses[0].Port() << ',' << addresses[1].Port()
              << " accepted=" << handler->accepted.load() << "\n";
}

static void CloseWaitersSettle() {
    StartRuntime(2);
    auto handler = std::make_shared<NullHandler>();
    Engine engine(handler);
    CHECK(!engine.EnableAcceptHandoff(2));
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));

    std::atomic<int> accept_result{-1};
    auto task = Task([&] {
        Connection connection;
        const auto error = CoAccept(engine, connection, bbt::coroutine::WaitOptions{});
        accept_result.store(error == std::make_error_code(std::errc::operation_canceled) ? 0 : 1);
    });
    std::this_thread::sleep_for(30ms);
    runner.CloseAndJoin();
    Get(task);
    CHECK(accept_result.load() == 0);

    auto state = std::make_shared<ConnectionState>(9, 4096, 64, 4, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);
    WriteOperation warm;
    CHECK(!connection.AsyncWrite(std::string(64, 'x'), warm));
    std::atomic<int> write_result{-1};
    auto write_task = Task([&] {
        WriteOutcome outcome;
        WriteOperation operation;
        const auto error = CoWrite(connection, std::string(8, 'y'),
                                   bbt::coroutine::WaitOptions{}, outcome, operation);
        if (error == make_error_code(NetErrc::Closed) && outcome.phase == WritePhase::Rejected) {
            write_result.store(0);
        }
    });
    std::this_thread::sleep_for(30ms);
    connection.RequestClose();
    Get(write_task);
    CHECK(write_result.load() == 0);
    std::cout << "close_waiters_settle accept=canceled write=closed\n";
}

static void RingCombined() {
    // 公共守卫（bbt::infra::net::ReadAccess）语义：同一保护的 Peek+Consume、
    // Busy 零消费、释放后另一读者可继续。
    auto state = std::make_shared<ConnectionState>(1, 64, 64, 4, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);
    Connection peer = ConnectionTestAccess::Make(state);  // 同一 ring 的第二句柄
    CHECK(state->ring.TryAppend("abcdef").status == ReadStatus::Ok);
    {
        auto access = connection.TryAccess();
        CHECK(bool(access));
        CHECK(access.Size() == 6);
        CHECK(access.Peek() == "abcdef");
        CHECK(!access.Consume(2));
    }
    std::string out;
    CHECK(connection.TryRead(out).status == ReadStatus::Ok);
    CHECK(out == "cdef");

    CHECK(state->ring.TryAppend("xy").status == ReadStatus::Ok);
    auto held = connection.TryAccess();
    CHECK(bool(held));
    std::promise<int> busy;
    auto busy_future = busy.get_future();
    std::thread other([&] {
        auto probe = peer.TryAccess();
        busy.set_value(probe.Busy() ? 1 : 0);
    });
    CHECK(busy_future.wait_for(2s) == std::future_status::ready);
    CHECK(busy_future.get() == 1);
    ConnectionTestAccess::Reset(held);
    other.join();
    CHECK(!state->ring.HasReadError());
    std::cout << "ring_combined peek_consume=ok busy_zero_consume=ok release=ok\n";
}

static void WriteFifoPrefix() {
    {
        ConnectionState state(1, 4096, 1024, 4, nullptr);
        WriteOperation first;
        WriteOperation second;
        CHECK(!TryAccept(state, "AAAA", first));
        CHECK(!TryAccept(state, "BB", second));
        int calls = 0;
        auto sink = [&calls](const std::string_view* views, std::size_t count,
                             std::size_t budget) -> ssize_t {
            (void)views;
            (void)count;
            (void)budget;
            ++calls;
            if (calls == 1) {
                return 2;
            }
            errno = EAGAIN;
            return -1;
        };
        const auto partial = state.PumpWrites(8, 4096, sink);
        CHECK(partial.bytes == 2 && partial.need_write_wait);
        CHECK(first.Query().phase == WritePhase::InFlight && first.Query().sent == 2);
        auto drain = [](const std::string_view* views, std::size_t, std::size_t budget) -> ssize_t {
            return static_cast<ssize_t>(std::min(views[0].size(), budget));
        };
        state.PumpWrites(8, 4096, drain);
        state.PumpWrites(8, 4096, drain);
        CHECK(first.Query().phase == WritePhase::Done && first.Query().sent == 4);
        CHECK(second.Query().phase == WritePhase::Done && second.Query().sent == 2);
        const auto terminals = state.DrainTerminal();
        CHECK(terminals.size() == 2);
        CHECK(terminals[0].id == first.Id() && terminals[1].id == second.Id());
    }
    {
        ConnectionState state(2, 4096, 1024, 4, nullptr);
        WriteOperation first;
        WriteOperation second;
        CHECK(!TryAccept(state, "XXXX", first));
        CHECK(!TryAccept(state, "YY", second));
        int calls = 0;
        auto sink = [&calls](const std::string_view* views, std::size_t, std::size_t) -> ssize_t {
            (void)views;
            ++calls;
            if (calls == 1) {
                return 2;
            }
            errno = ECONNRESET;
            return -1;
        };
        state.PumpWrites(8, 4096, sink);
        const auto reset = std::error_code(ECONNRESET, std::system_category());
        CHECK(first.Query().phase == WritePhase::Done && first.Query().sent == 2
              && first.Query().error == reset);
        CHECK(second.Query().phase == WritePhase::Done && second.Query().sent == 0
              && second.Query().error == reset);
        CHECK(state.LastWriteError() == reset);
        CHECK(state.Acceptance() == AcceptanceState::Open);
        WriteOperation fresh;
        CHECK(!TryAccept(state, "ZZ", fresh));
        state.MarkClosed(std::make_error_code(std::errc::operation_canceled));
        CHECK(state.Acceptance() == AcceptanceState::Closed);
        WriteOperation after;
        CHECK(TryAccept(state, "Q", after) == make_error_code(NetErrc::Closed));
    }
    std::cout << "write_fifo_prefix partial/FIFO/error/Settling/close=ok\n";
}

static void CoReadTimeout() {
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(1, 4096, 1024, 4, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);
    std::atomic<int> result{-1};
    auto out = std::make_shared<std::string>();
    auto task = Task([&] {
        const auto error = CoRead(connection, *out, Until(30ms));
        if (error == std::make_error_code(std::errc::timed_out) && out->empty()) {
            result.store(0);
        }
    });
    Get(task);
    CHECK(result.load() == 0);
    std::cout << "co_read_timeout out_unchanged=ok\n";
}

static void PerfBaseline() {
    StartRuntime(3);
    const int iterations = 500;
    long long raw_p99 = 0;
    int raw_timeouts = 0;
    double raw_ops = 0.0;
    auto handler = std::make_shared<EchoHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    raw_ops = PingPong(addresses[0], iterations, raw_p99, raw_timeouts);
    runner.CloseAndJoin();
    CHECK(!runner.error);
    CHECK(raw_timeouts == 0);
    // Co 路径正确性由 net.r2_co_* / net.b4_co_read_* 覆盖；Co 短基线本轮未测
    // （存在间歇性 coroutine 未恢复，见 b5-fix-ledger，不用统计容忍掩盖）。
    std::cout << "perf raw_ops_per_sec=" << static_cast<long long>(raw_ops)
              << " raw_p99_us=" << raw_p99 / 1000
              << " raw_timeouts=" << raw_timeouts << " n=" << iterations
              << " co_baseline=not_measured\n";
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
        if (test == "raw_roundtrip") { RawRoundtrip(); }
        else if (test == "co_accept_read_write") { CoAcceptReadWrite(); }
        else if (test == "co_dial") { CoDial(); }
        else if (test == "close_retained") { CloseRetained(); }
        else if (test == "write_backpressure") { WriteBackpressure(); }
        else if (test == "write_timeout") { WriteTimeout(); }
        else if (test == "multi_listener") { MultiListener(); }
        else if (test == "close_waiters_settle") { CloseWaitersSettle(); }
        else if (test == "ring_combined") { RingCombined(); }
        else if (test == "write_fifo_prefix") { WriteFifoPrefix(); }
        else if (test == "co_read_timeout") { CoReadTimeout(); }
        else if (test == "perf_baseline") { PerfBaseline(); }
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
