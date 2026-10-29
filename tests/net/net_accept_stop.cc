// net 接纳独立封口验证（RequestStopAccepting / AcceptingStopped）：
// 只封监听/交接授权，真实 listener 物理关闭后才 stopped，已交付连接读写
// 继续可用，CoAccept 被唤醒并取消，Run 前/Starting 封口不重开，client-only
// 拨号不受影响，Failed 与最终 RequestClose 的收口保留责任。
//
// 进程退出经 bbt_net_test::RunTestProcessExit 收口（同 tests/net 其它 runtime
// 用例）：每 case 业务资源在 _Exit 前已显式 close/join，不把 OS 代清当证据。

#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/RawHandler.hpp>
#include <bbt/infra/net/detail/EngineTestAccess.hpp>

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstddef>
#include <cstdlib>
#include <csignal>
#include <filesystem>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <poll.h>

#include <bbt/coroutine/coroutine.hpp>

#include "ProcessRuntimeTestMain.hpp"

using namespace bbt::infra::net;
using namespace std::chrono_literals;

namespace {

void Check(bool value, const char* expression, int line) {
    if (!value) {
        throw std::runtime_error("line " + std::to_string(line) + ": " + expression);
    }
}
#define CHECK(expression) Check(static_cast<bool>(expression), #expression, __LINE__)

static bbt::coroutine::WaitOptions Until(std::chrono::milliseconds duration) {
    return {std::chrono::steady_clock::now() + duration};
}

static bool WaitUntil(const std::function<bool()>& predicate,
                      std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

void StartRuntime(std::size_t workers) {
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
    }, "net.stop");
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

// 临时绑定后关闭，探得当前空闲端口（仅用于确定端口，不作为监听资源）。
static Address BindTempPort() {
    std::error_code error;
    NativeSocket listener = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream,
                                               error);
    CHECK(!error);
    CHECK(!listener.Bind(Loopback(0)));
    CHECK(!listener.Listen(8));
    Address address = listener.LocalAddress(error);
    CHECK(!error);
    (void)listener.Close();
    return address;
}

struct EngineRunner {
    Engine& engine;
    std::error_code error;
    std::atomic<bool> returned{false};
    std::thread thread;

    EngineRunner(Engine& target, EngineConfig config) : engine(target) {
        thread = std::thread([this, config]() mutable {
            error = engine.Run(config);
            returned.store(true);
        });
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
    NativeSocket client =
        NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    CHECK(!error);
    const auto connect_error = client.Connect(address);
    CHECK(!connect_error || connect_error.value() == EINPROGRESS ||
          connect_error.value() == EWOULDBLOCK);
    pollfd descriptor{client.Fd(), POLLOUT, 0};
    CHECK(::poll(&descriptor, 1, 3000) > 0);
    CHECK(!client.PendingError());
    return client;
}

// 监听已物理关闭：新连接被拒绝（ECONNREFUSED），而非可建立。
static bool PortRefused(const Address& address) {
    std::error_code error;
    NativeSocket sock = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream,
                                           error);
    if (error) {
        return false;
    }
    const auto connect_error = sock.Connect(address);
    if (!connect_error) {
        sock.Close();
        return false;
    }
    if (connect_error.value() == ECONNREFUSED) {
        sock.Close();
        return true;
    }
    if (connect_error.value() != EINPROGRESS &&
        connect_error.value() != EWOULDBLOCK) {
        sock.Close();
        return false;
    }
    pollfd descriptor{sock.Fd(), POLLOUT, 0};
    if (::poll(&descriptor, 1, 3000) <= 0) {
        sock.Close();
        return false;
    }
    const bool refused = static_cast<bool>(sock.PendingError());
    sock.Close();
    return refused;
}

static EngineConfig ServerConfig(const Address& listen) {
    EngineConfig config;
    config.listen_addresses = {listen};
    config.runtime_progress = RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    return config;
}

static EngineConfig ClientConfig() {
    EngineConfig config;
    config.runtime_progress = RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    return config;
}

struct NullHandler : RawHandler {};

struct EchoHandler : RawHandler {
    void OnRead(Connection connection) override {
        std::string data;
        auto result = connection.TryRead(data);
        if (result.status == ReadStatus::Ok && !data.empty()) {
            WriteOperation operation;
            connection.AsyncWrite(data, operation);
        }
    }
};

// ---------------- cases ----------------

void StopRunningDeliveredConnRW() {
    StartRuntime(2);
    const Address address = BindTempPort();
    auto handler = std::make_shared<NullHandler>();
    Engine engine(handler);
    CHECK(!engine.EnableAcceptHandoff(4));
    EngineRunner runner(engine, ServerConfig(address));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    CHECK(addresses.size() == 1);

    // 先交付一条连接给 CoAccept，再封监听。
    std::atomic<bool> accepted_ready{false};
    std::atomic<int> accept_result{-2};
    auto accepted_connection = std::make_shared<Connection>();
    auto task = Task([&] {
        auto res = engine.CoAccept(Until(5000ms));
        accept_result.store(res.error ? res.error.value() : 0);
        if (!res.error && res.connection.Valid()) {
            *accepted_connection = res.connection.Raw();
            accepted_ready.store(true);
        }
    });
    auto client = ConnectClient(addresses[0]);
    CHECK(WaitUntil([&] { return accepted_ready.load(); }, 5000ms));
    CHECK(accept_result.load() == 0);

    engine.RequestStopAccepting();
    CHECK(WaitUntil([&] { return engine.AcceptingStopped(); }, 5000ms));
    // listener 物理停用：原真实地址再连被拒绝，不是"Running/计数为0"的替代。
    CHECK(PortRefused(addresses[0]));

    // 已交付连接在 stop 后双向读写继续，对端真实收到：由协程任务在
    // 已交付句柄上先读"ping"再原文回写（Co 入口须在协程域）。
    std::atomic<int> rw_result{-2};
    auto rw_task = Task([&] {
        CoTcpConnection co_conn{*accepted_connection};
        std::string data;
        auto read = co_conn.Read(Until(5000ms));
        if (read.state != ReadState::Data || read.data.CopyTo() != "ping") {
            rw_result.store(read.error ? read.error.value() : -1);
            return;
        }
        data = read.data.CopyTo();
        auto report = co_conn.Write(data, Until(3000ms));
        if (report.wait_error || report.outcome.phase != WritePhase::Done || report.outcome.error) {
            rw_result.store(report.wait_error ? report.wait_error.value() : -2);
            return;
        }
        rw_result.store(0);
    });
    // 保齐发送 4 字节（非阻塞 socket 可能部分写）。
    std::size_t sent = 0;
    while (sent < 4) {
        const auto result = client.Send("ping" + sent, 4 - sent);
        if (result.bytes > 0) {
            sent += result.bytes;
            continue;
        }
        pollfd pd{client.Fd(), POLLOUT, 0};
        CHECK(::poll(&pd, 1, 1000) > 0);
    }
    Get(rw_task);
    CHECK(rw_result.load() == 0);
    std::string echoed;
    char buffer[16];
    CHECK(WaitUntil([&] {
        const auto result = client.Receive(buffer, sizeof(buffer));
        if (result.bytes > 0) {
            echoed.append(buffer, result.bytes);
        }
        return echoed == "ping";
    }, 3000ms));

    accepted_connection->RequestClose();
    CHECK(!client.Close());
    runner.CloseAndJoin();
    CHECK(!runner.error);
    CHECK(engine.Phase() == EnginePhase::Stopped);
    CHECK(engine.AcceptingStopped());
    std::cout << "stop_running_delivered_conn_rw ok" << std::endl;
}

void StopWakesCoAcceptWaiters() {
    StartRuntime(2);
    const Address address = BindTempPort();
    auto handler = std::make_shared<NullHandler>();
    Engine engine(handler);
    CHECK(!engine.EnableAcceptHandoff(4));
    EngineRunner runner(engine, ServerConfig(address));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));

    // 空 handoff 上的 CoAccept 阻塞，停止后必须被唤醒并返回 operation_canceled，
    // 失败不改 out。
    std::atomic<bool> waiter_registered{false};
    EngineTestAccess::SetAcceptWaiterHook(engine, [&] { waiter_registered = true; });
    std::atomic<int> result{-2};
    Connection out;
    auto task = Task([&] {
        auto res = engine.CoAccept(Until(5000ms));
        result.store(res.error ? res.error.value() : -1);
        if (!res.error) {
            out = res.connection.Raw();
        }
    });
    CHECK(WaitUntil([&] { return waiter_registered.load(); }, 3000ms));
    engine.RequestStopAccepting();
    Get(task);
    CHECK(result.load() ==
          static_cast<int>(std::errc::operation_canceled));
    CHECK(!out.Valid());  // out 未被覆盖
    CHECK(WaitUntil([&] { return engine.AcceptingStopped(); }, 5000ms));
    CHECK(PortRefused(addresses[0]));
    runner.CloseAndJoin();
    CHECK(!runner.error);
    CHECK(engine.Phase() == EnginePhase::Stopped);
    std::cout << "stop_wakes_coaccept_waiters ok" << std::endl;
}

void StopLateCoAcceptRejected() {
    StartRuntime(2);

    // A: Run 前（Idle）停止：CoAccept 返回 operation_not_permitted，out 不变；
    //    Run 之后不允许新开监听，直接返回错误。
    {
        const Address address = BindTempPort();
        Engine idle(std::make_shared<NullHandler>());
        CHECK(!idle.EnableAcceptHandoff(4));
        idle.RequestStopAccepting();
        CHECK(idle.AcceptingStopped());
        CHECK(idle.Phase() == EnginePhase::Idle);
        std::atomic<int> co_result{-2};
        Connection co_out;
        auto task = Task([&] {
            auto res = idle.CoAccept(Until(1000ms));
            co_result.store(res.error ? res.error.value() : -1);
            if (!res.error) {
                co_out = res.connection.Raw();
            }
        });
        Get(task);
        CHECK(co_result.load() ==
              static_cast<int>(std::errc::operation_not_permitted));
        CHECK(!co_out.Valid());
        EngineRunner runner(idle, ServerConfig(address));
        CHECK(WaitUntil([&] { return runner.returned.load(); },
                        5000ms));
        runner.CloseAndJoin();
        CHECK(runner.error ==
              std::make_error_code(std::errc::operation_canceled));
        CHECK(idle.Phase() == EnginePhase::Stopped);
        CHECK(idle.AcceptingStopped());
        CHECK(PortRefused(address));
        std::vector<Address> snap;
        CHECK(idle.LocalAddresses(snap) ==
              std::make_error_code(std::errc::operation_not_permitted));
    }

    // B: Running 中停止后，晚到的 CoAccept 直接 operation_canceled，out 不变。
    {
        const Address address = BindTempPort();
        auto handler = std::make_shared<NullHandler>();
        Engine engine(handler);
        CHECK(!engine.EnableAcceptHandoff(4));
        EngineRunner runner(engine, ServerConfig(address));
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; },
                        3000ms));
        std::vector<Address> addresses;
        CHECK(!engine.LocalAddresses(addresses));
        engine.RequestStopAccepting();
        CHECK(WaitUntil([&] { return engine.AcceptingStopped(); }, 5000ms));
        std::atomic<int> co_result{-2};
        Connection co_out;
        auto task = Task([&] {
            auto res = engine.CoAccept(Until(1000ms));
            co_result.store(res.error ? res.error.value() : -1);
            if (!res.error) {
                co_out = res.connection.Raw();
            }
        });
        Get(task);
        CHECK(co_result.load() ==
              static_cast<int>(std::errc::operation_canceled));
        CHECK(!co_out.Valid());
        runner.CloseAndJoin();
        CHECK(!runner.error);
        CHECK(engine.Phase() == EnginePhase::Stopped);
        CHECK(PortRefused(addresses[0]));
    }
    std::cout << "stop_late_coaccept_rejected ok" << std::endl;
}

void StopBeforeRunNoListener() {
    // 无 runtime 亦可停止：本代无 listener，停止即生效；随后 Run 不开监听。
    const Address address = BindTempPort();
    Engine engine(std::make_shared<NullHandler>());
    CHECK(!engine.EnableAcceptHandoff(4));
    engine.RequestStopAccepting();
    CHECK(engine.Phase() == EnginePhase::Idle);
    CHECK(engine.AcceptingStopped());
    EngineRunner runner(engine, ServerConfig(address));
    CHECK(WaitUntil([&] { return runner.returned.load(); }, 5000ms));
    runner.CloseAndJoin();
    CHECK(runner.error == std::make_error_code(std::errc::operation_canceled));
    CHECK(engine.Phase() == EnginePhase::Stopped);
    CHECK(engine.AcceptingStopped());
    CHECK(PortRefused(address));
    std::vector<Address> snap;
    CHECK(engine.LocalAddresses(snap) ==
          std::make_error_code(std::errc::operation_not_permitted));
    std::cout << "stop_before_run_no_listener ok" << std::endl;
}

void StopClientOnlyDialContinues(bool before_run = false) {
    StartRuntime(2);
    std::error_code error;
    auto listener = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    CHECK(!error);
    CHECK(!listener.Bind(Loopback(0)) && !listener.Listen(8));
    const auto address = listener.LocalAddress(error);
    CHECK(!error);

    Engine client{std::shared_ptr<RawHandler>()};
    if (before_run) { client.RequestStopAccepting(); CHECK(client.AcceptingStopped()); }
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    client.RequestStopAccepting();
    CHECK(client.AcceptingStopped());

    std::atomic<int> dial_result{-2}, exchange_result{-2};
    Connection connection;
    auto task = Task([&] {
        auto res = client.CoDial(address, Until(3000ms));
        dial_result.store(res.error ? res.error.value() : 0);
        if (res.error) { return; }
        connection = res.connection.Raw();
        CoTcpConnection co_conn{connection};
        auto report = co_conn.Write("ping", Until(3000ms));
        if (report.wait_error || report.outcome.phase != WritePhase::Done || report.outcome.error) {
            exchange_result.store(-1);
            return;
        }
        std::string response;
        while (response.size() < 4) {
            auto read = co_conn.Read(Until(3000ms));
            if (read.state != ReadState::Data || read.data.Empty()) {
                exchange_result.store(-1);
                return;
            }
            read.data.AppendTo(response);
        }
        exchange_result.store(response == "pong" ? 0 : -1);
    });

    // native 对端由控制线程推进，不引入第二个 Engine/额外线程。
    // 每个阶段都先观察真实 accept/请求字节，再回复。
    NativeSocket peer;
    CHECK(WaitUntil([&] {
        Address remote;
        peer = listener.Accept(remote, error);
        return !error && peer.Fd() >= 0;
    }, 3000ms));
    std::string request;
    CHECK(WaitUntil([&] {
        char buffer[16];
        const auto read = peer.Receive(buffer, sizeof(buffer));
        if (read.bytes > 0) { request.append(buffer, read.bytes); }
        return request.size() >= 4;
    }, 3000ms));
    CHECK(request == "ping");
    std::size_t sent = 0;
    CHECK(WaitUntil([&] {
        const auto write = peer.Send("pong" + sent, 4 - sent);
        sent += write.bytes;
        return sent == 4;
    }, 3000ms));
    Get(task);
    CHECK(dial_result.load() == 0 && exchange_result.load() == 0);
    connection.RequestClose();
    CHECK(!peer.Close() && !listener.Close());
    runner.CloseAndJoin();
    CHECK(!runner.error && client.Phase() == EnginePhase::Stopped);
    std::cout << "stop_client_only_dial_continues ok" << std::endl;
}

void StopPostFailureRetry() {
    StartRuntime(2);
    Engine engine(std::make_shared<NullHandler>());
    CHECK(!engine.EnableAcceptHandoff(4));
    std::atomic<bool> failure_observed{false};
    Address address;
    EngineTestAccess::SetAcceptStopPostFailureOnce(engine, [&] {
        // posted latch 仍持有：控制域不能抢先成功重交，能直接观察拒投后的物理事实。
        CHECK(!engine.AcceptingStopped());
        auto still_listening = ConnectClient(address);
        CHECK(!still_listening.Close());
        failure_observed = true;
    });
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses) && addresses.size() == 1);
    address = addresses.front();
    engine.RequestStopAccepting();
    CHECK(failure_observed);
    CHECK(WaitUntil([&] { return engine.AcceptingStopped(); }, 3000ms));
    CHECK(PortRefused(address));
    runner.CloseAndJoin();
    CHECK(!runner.error && engine.CountConnections() == 0);
    std::cout << "stop_post_failure_retry refusal_not_physical=1 owner_retry_closed_listener=1\n";
}

void StopRepeatAndConcurrent() {
    StartRuntime(2);
    const Address address = BindTempPort();
    auto handler = std::make_shared<NullHandler>();
    Engine engine(handler);
    CHECK(!engine.EnableAcceptHandoff(4));
    EngineRunner runner(engine, ServerConfig(address));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; },
                    3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    CHECK(!addresses.empty());

    std::atomic<int> calls{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&] { engine.RequestStopAccepting(); calls.fetch_add(1); });
    }
    for (auto& t : threads) {
        t.join();
    }
    CHECK(calls.load() == 8);
    CHECK(engine.AcceptingStopped());
    CHECK(PortRefused(addresses[0]));
    // 反复请求：幂等，不重开监听。
    engine.RequestStopAccepting();
    CHECK(engine.AcceptingStopped());
    CHECK(PortRefused(addresses[0]));
    runner.CloseAndJoin();
    CHECK(!runner.error);
    CHECK(engine.Phase() == EnginePhase::Stopped);
    std::cout << "stop_repeat_and_concurrent ok" << std::endl;
}

void StopDuringStartingRace() {
    StartRuntime(2);
    const Address address = BindTempPort();
    Engine engine(std::make_shared<NullHandler>());
    CHECK(!engine.EnableAcceptHandoff(4));
    // 拉长 owner 安装，制造"安装中停止"的竞态窗口。
    EngineTestAccess::SetInstallDelay(engine, 300ms);
    EngineRunner runner(engine, ServerConfig(address));
    // 先证实公共 Starting 相位，再停止；安装延迟只是扩大窗口，不是阶段证据。
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Starting; }, 3000ms));
    engine.RequestStopAccepting();
    CHECK(WaitUntil([&] { return runner.returned.load(); }, 10000ms));
    runner.CloseAndJoin();
    CHECK(runner.error == std::make_error_code(std::errc::operation_canceled));
    CHECK(engine.Phase() == EnginePhase::Stopped);
    CHECK(engine.AcceptingStopped());
    CHECK(PortRefused(address));
    std::cout << "stop_during_starting_race ok" << std::endl;
}

void StopWithDriverFailed() {
    StartRuntime(2);
    const Address address = BindTempPort();
    auto handler = std::make_shared<NullHandler>();
    Engine engine(handler);
    CHECK(!engine.EnableAcceptHandoff(4));
    EngineRunner runner(engine, ServerConfig(address));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; },
                    3000ms));
    engine.RequestStopAccepting();
    CHECK(!EngineTestAccess::InjectDriverFailure(engine));
    CHECK(WaitUntil([&] { return engine.Phase() != EnginePhase::Running; },
                    10000ms));
    runner.CloseAndJoin();
    CHECK(runner.error);  // 经真实 Failed 错误出口返回
    CHECK(engine.Phase() == EnginePhase::Stopped);
    // Failed 同代同步解绑后的控制域：全部本代 listener 已物理关。
    CHECK(engine.AcceptingStopped());
    CHECK(engine.CountConnections() == 0);
    CHECK(EngineTestAccess::FirstConnectionFd(engine) == -1);
    CHECK(PortRefused(address));
    std::cout << "stop_with_driver_failed ok" << std::endl;
}

void StopUndeliveredHandoffReclaimed() {
    StartRuntime(2);
    const Address address = BindTempPort();
    auto handler = std::make_shared<NullHandler>();
    Engine engine(handler);
    CHECK(!engine.EnableAcceptHandoff(4));
    EngineRunner runner(engine, ServerConfig(address));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; },
                    3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    // 真实建连并进入 handoff，但不由 CoAccept 领取：未交付连接。
    auto client = ConnectClient(addresses[0]);
    CHECK(WaitUntil([&] {
        return EngineTestAccess::ConnectionCount(engine) == 1;
    }, 3000ms));
    engine.RequestStopAccepting();
    // 门后未交付 handoff 连接被退役并物理关闭。
    CHECK(WaitUntil([&] {
        return EngineTestAccess::ConnectionCount(engine) == 0;
    }, 5000ms));
    CHECK(EngineTestAccess::FirstConnectionFd(engine) == -1);
    CHECK(engine.AcceptingStopped());
    CHECK(PortRefused(addresses[0]));
    // 客户端已建连接本身不因 stop 被关闭。
    CHECK(!client.PendingError());
    client.Close();
    runner.CloseAndJoin();
    CHECK(!runner.error);
    CHECK(engine.Phase() == EnginePhase::Stopped);
    std::cout << "stop_undelivered_handoff_reclaimed ok" << std::endl;
}

int RunCases(int argc, char** argv) {
    try {
        CHECK(argc == 2);
        const std::string test = argv[1];
        if (test == "stop_running_delivered_conn_rw") {
            StopRunningDeliveredConnRW();
        } else if (test == "stop_wakes_coaccept_waiters") {
            StopWakesCoAcceptWaiters();
        } else if (test == "stop_late_coaccept_rejected") {
            StopLateCoAcceptRejected();
        } else if (test == "stop_before_run_no_listener") {
            StopBeforeRunNoListener();
        } else if (test == "stop_client_only_dial_continues") {
            StopClientOnlyDialContinues();
        } else if (test == "stop_client_only_before_run") {
            StopClientOnlyDialContinues(true);
        } else if (test == "stop_post_failure_retry") {
            StopPostFailureRetry();
        } else if (test == "stop_repeat_and_concurrent") {
            StopRepeatAndConcurrent();
        } else if (test == "stop_during_starting_race") {
            StopDuringStartingRace();
        } else if (test == "stop_with_driver_failed") {
            StopWithDriverFailed();
        } else if (test == "stop_undelivered_handoff_reclaimed") {
            StopUndeliveredHandoffReclaimed();
        } else {
            std::cerr << "unknown case: " << test << "\n";
            return 2;
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << "\n";
        return 1;
    }
}

} // namespace

int main(int argc, char** argv) {
    std::signal(SIGSEGV, [](int) { std::_Exit(139); });
    std::signal(SIGABRT, [](int) { std::_Exit(134); });
    return bbt_net_test::RunTestProcessExit([&] { return RunCases(argc, argv); });
}
