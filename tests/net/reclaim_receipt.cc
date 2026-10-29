// net 物理回收观察句柄契约（ReclaimReceipt / Connection::Released / 带句柄 CoDial）。
//
// 覆盖（对应 httpx 独立审查 F5/F11 的 net 侧最小支撑）：
//   receipt_invalid_default          默认句柄无效即 Released；无效 Connection Released=true
//   codial_success_close_released    成功交付：句柄与连接同终态；真实 close 后才 Released；可续拨号
//   codial_timeout_undelivered       阶段屏障：已真实连接未交付时 Valid&&!Released；回收后可观察 true
//   dial_refused_released            fd 已建后拒连：真实 close 先于释放标记，返回时已 Released
//   dial_pre_submit_rejections       上下文/投递失败/容量拒绝：句柄保持无效；无 fd 占位最终收口
//   released_not_before_physical_close  ShutdownOwner 关 backend 前句柄/连接都未 Released
//   engine_stop_releases             Engine seal：已交付连接与未交付占位都可达释放
//   driver_failed_releases           driver 中止：pending 拨号责任仍收口
//
// 屏障语义：不靠固定 sleep 证明阶段；未交付窗口由 delivery-delay seam 与
// 服务端真实 accept 共同证明。

#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/net/ConnectionResult.hpp>
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/RawHandler.hpp>
#include <bbt/infra/net/detail/EngineTestAccess.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
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
#include <netinet/in.h>
#include <sys/stat.h>
#include <unistd.h>

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

bbt::coroutine::WaitOptions Until(std::chrono::milliseconds duration) {
    return {std::chrono::steady_clock::now() + duration};
}

bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout) {
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

std::future<void> Task(std::function<void()> callback) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    g_scheduler->RegistCoroutineTask(65536, [promise, callback] {
        try {
            callback();
            promise->set_value();
        } catch (...) {
            promise->set_exception(std::current_exception());
        }
    }, "net.reclaim");
    return future;
}

void Get(std::future<void>& future) {
    CHECK(future.wait_for(5s) == std::future_status::ready);
    future.get();
}

Address Loopback(int port) {
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

NativeSocket MakeListener(Address& address) {
    std::error_code error;
    NativeSocket listener = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    CHECK(!error);
    CHECK(!listener.Bind(Loopback(0)));
    CHECK(!listener.Listen(8));
    address = listener.LocalAddress(error);
    CHECK(!error);
    return listener;
}

EngineConfig ClientConfig() {
    EngineConfig config;
    config.runtime_progress = RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    return config;
}

std::size_t CountFds() {
    return static_cast<std::size_t>(std::distance(
        std::filesystem::directory_iterator("/proc/self/fd"),
        std::filesystem::directory_iterator{}));
}

// ---------------- cases ----------------

void ReceiptInvalidDefault() {
    ReclaimReceipt receipt;
    CHECK(!receipt.Valid());
    CHECK(receipt.Released());  // 无效句柄=没有已提交资源责任
    Connection connection;
    CHECK(!connection.Valid());
    CHECK(connection.Released());
    std::cout << "receipt_invalid_default invalid_released=ok\n";
}

void CoDialSuccessCloseReleased() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);
    Engine client{std::shared_ptr<RawHandler>()};
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    std::atomic<int> result{-2};
    std::atomic<bool> initially_released{true};
    auto task = Task([&] {
        auto res = client.CoDial(address, Until(2000ms));
        if (res.error || !res.connection.Valid()) {
            result.store(res.error ? res.error.value() : -1);
            return;
        }
        Connection connection = res.connection.Raw();
        ReclaimReceipt receipt = res.reclaim;
        // 已交付连接：句柄与连接共用同一回收记录，物理 fd 仍开 → 未释放。
        initially_released.store(receipt.Released());
        CHECK(receipt.Valid());
        CHECK(!receipt.Released());
        CHECK(!connection.Released());
        connection.RequestClose();
        result.store(0);
    });
    Get(task);
    CHECK(result.load() == 0);
    CHECK(!initially_released.load());
    // 真实 close + owner 责任清理后才 Released；以首拨后（context fd 已稳定）
    // 为基线观察 fd 真实收回。
    CHECK(WaitUntil([&] { return client.CountConnections() == 0; }, 2000ms));
    const auto fds_baseline = CountFds();
    // 收口可经句柄独立观察：再次拨号后关闭，同终态；释放后可续拨号。
    std::atomic<int> second{-2};
    auto task2 = Task([&] {
        auto res = client.CoDial(address, Until(2000ms));
        if (res.error || !res.connection.Valid()) {
            second.store(res.error ? res.error.value() : -1);
            return;
        }
        Connection connection = res.connection.Raw();
        ReclaimReceipt receipt = res.reclaim;
        second.store(0);
        connection.RequestClose();
        CHECK(WaitUntil([&] { return connection.Released() && receipt.Released(); }, 2000ms));
        CHECK(connection.Closed());
    });
    Get(task2);
    CHECK(second.load() == 0);
    CHECK(WaitUntil([&] { return client.CountConnections() == 0; }, 2000ms));
    CHECK(WaitUntil([&] { return CountFds() <= fds_baseline; }, 2000ms));
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "codial_success_close_released same_record=ok redial=ok\n";
}

void CoDialTimeoutUndelivered() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);
    Engine client{std::shared_ptr<RawHandler>()};
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    // 交付延迟拉长 owner 交付：连接已真实完成（内核 backlog/已建立），尚未交付。
    EngineTestAccess::SetConnectDeliveryDelay(client, std::chrono::milliseconds(800));
    std::atomic<int> result{-2};
    auto receipt = std::make_shared<ReclaimReceipt>();  // 跨协程存活，主线程可观察
    auto task = Task([&] {
        auto res = client.CoDial(address, Until(40ms));
        *receipt = res.reclaim;
        result.store(res.error.value());
    });
    Get(task);
    CHECK(result.load() == std::make_error_code(std::errc::timed_out).value());
    // 阶段屏障：返回后 fd 仍在 owner 交付路径中 → 责任存活、句柄可观察。
    CHECK(receipt->Valid());
    CHECK(!receipt->Released());
    // 阶段证据：TCP 已真实建立（服务端能 accept），但从未交付给调用者。
    std::error_code accept_error;
    Address peer;
    NativeSocket accepted = listener.Accept(peer, accept_error);
    CHECK(!accept_error);
    CHECK(accepted.Fd() >= 0);
    (void)accepted.Close();
    // 保留句柄可观察最终释放：交付定时器到期 → 无主回收 → close+标记。
    CHECK(WaitUntil([&] { return receipt->Released(); }, 3000ms));
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 3000ms));
    CHECK(WaitUntil([&] { return client.CountConnections() == 0; }, 3000ms));
    // fd 真实已关：以回收完成后的稳态为基线，正常拨号+关闭须回到基线。
    EngineTestAccess::SetConnectDeliveryDelay(client, std::chrono::milliseconds(0));
    const auto fds_steady = CountFds();
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
    CHECK(WaitUntil([&] { return client.CountConnections() == 0; }, 2000ms));
    CHECK(WaitUntil([&] { return CountFds() <= fds_steady; }, 2000ms));
    CHECK(EngineTestAccess::PendingConnectCount(client) == 0);
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "codial_timeout_undelivered established=ok reclaimed=ok\n";
}

void DialRefusedReleased() {
    StartRuntime(2);
    // 制造真实拒连端口：先绑定取得端口，再关闭 listener。
    Address refused;
    {
        auto dead = MakeListener(refused);
        (void)dead.Close();
    }
    Engine client{std::shared_ptr<RawHandler>()};
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    std::atomic<int> result{-2};
    auto receipt = std::make_shared<ReclaimReceipt>();  // 跨协程存活，主线程可观察
    auto task = Task([&] {
        auto res = client.CoDial(refused, Until(2000ms));
        *receipt = res.reclaim;
        result.store(res.error.value());
        CHECK(!res.connection.Valid());  // 失败时 connection 无效（不覆盖调用者原值）
    });
    Get(task);
    CHECK(result.load() != 0);  // 真实拒连错误（ECONNREFUSED），不是等待超时
    CHECK(receipt->Valid());
    // fd 建立后在 SO_ERROR 真实关闭并收口：owner notify 先于领取返回，已可观察。
    CHECK(receipt->Released());
    CHECK(client.CountConnections() == 0);
    CHECK(EngineTestAccess::FirstConnectionFd(client) == -1);
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "dial_refused_released refused_closed=ok receipt_settled=ok\n";
}

void DialPreSubmitRejections() {
    // 1) 执行域非法：gate 先于一切状态/责任，句柄保持无效。
    {
        Engine idle{std::shared_ptr<RawHandler>()};
        auto res = idle.CoDial(Loopback(1), Until(50ms));
        CHECK(res.error == std::make_error_code(std::errc::operation_not_permitted));
        CHECK(!res.reclaim.Valid());
        CHECK(res.reclaim.Released());
        CHECK(!res.connection.Valid());
    }
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);
    Engine client{std::shared_ptr<RawHandler>()};
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    // 2) 投递失败（真实回滚路径）：零接纳、句柄无效。
    EngineTestAccess::SetConnectPostFailureOnce(client);
    std::atomic<int> post_result{-2};
    auto post_task = Task([&] {
        auto res = client.CoDial(address, Until(500ms));
        post_result.store(res.error.value());
        CHECK(!res.reclaim.Valid());
        CHECK(res.reclaim.Released());
        CHECK(!res.connection.Valid());
    });
    Get(post_task);
    CHECK(post_result.load() == std::make_error_code(std::errc::operation_canceled).value());
    CHECK(EngineTestAccess::ConnectTableSize(client) == 0);
    // 3) 容量零接纳：占位 pending 未建 fd，第二拨号拒绝且句柄无效；
    //    占位者被 seal 收口后其句柄仍可达释放（主线程观察，不占 S 线程）。
    EngineTestAccess::SetConnectLimit(client, 1);
    EngineTestAccess::SetConnectStartDelay(client, std::chrono::milliseconds(5000));
    std::atomic<int> first_result{-2};
    auto first_receipt = std::make_shared<ReclaimReceipt>();
    auto first_task = Task([&] {
        auto res = client.CoDial(address, Until(3000ms));
        *first_receipt = res.reclaim;
        first_result.store(res.error.value());
    });
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 1; }, 2000ms));
    std::atomic<int> second_result{-2};
    auto second_task = Task([&] {
        auto res = client.CoDial(address, Until(200ms));
        second_result.store(res.error.value());
        CHECK(!res.reclaim.Valid());  // 提交前容量拒绝：无本次责任
        CHECK(res.reclaim.Released());
        CHECK(!res.connection.Valid());
    });
    Get(second_task);
    CHECK(second_result.load() == std::make_error_code(std::errc::no_buffer_space).value());
    runner.CloseAndJoin();
    Get(first_task);
    CHECK(first_result.load() == std::make_error_code(std::errc::operation_canceled).value());
    CHECK(first_receipt->Valid());
    // seal 取消占位（未建 fd）→ 责任即时收口可观察。
    CHECK(WaitUntil([&] { return first_receipt->Released(); }, 3000ms));
    CHECK(EngineTestAccess::ConnectTableSize(client) == 0);
    EngineTestAccess::ClearConnectInjection(client);
    CHECK(!runner.error);
    std::cout << "dial_pre_submit_rejections gate=ok post_fail=ok cap=ok\n";
}

void ReleasedNotBeforePhysicalClose() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);
    Engine client{std::shared_ptr<RawHandler>()};
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    std::atomic<bool> barrier_entered{false};
    std::atomic<bool> release_barrier{false};
    // 屏障：ShutdownOwner 进入点（尚未关 backend）——fd 必须仍开且未 Released。
    // 屏障在 owner 线程（非协程）执行：只用 yield 等待，不走 sleep 钩子。
    EngineTestAccess::SetShutdownBarrierOnce(client, [&] {
        barrier_entered.store(true);
        while (!release_barrier.load()) {
            std::this_thread::yield();
        }
    });
    std::atomic<int> result{-2};
    auto task = Task([&] {
        auto res = client.CoDial(address, Until(2000ms));
        if (res.error || !res.connection.Valid()) {
            result.store(res.error ? res.error.value() : -1);
            return;
        }
        Connection connection = res.connection.Raw();
        ReclaimReceipt receipt = res.reclaim;
        result.store(0);
        connection.RequestClose();
        // 关闭请求先于 owner 推进被屏障挡住：此刻 fd 仍开、责任未收口。
        CHECK(WaitUntil([&] { return barrier_entered.load(); }, 2000ms));
        CHECK(EngineTestAccess::FirstConnectionFd(client) >= 0);
        CHECK(!connection.Released());
        CHECK(!receipt.Released());
        release_barrier.store(true);
        // 屏障放行后 owner 关 backend、兑现责任，随后才允许 Released。
        CHECK(WaitUntil([&] { return connection.Released(); }, 2000ms));
        CHECK(receipt.Released());
        CHECK(connection.Closed());
    });
    Get(task);
    CHECK(result.load() == 0);
    CHECK(EngineTestAccess::FirstConnectionFd(client) == -1);
    CHECK(client.CountConnections() == 0);
    runner.CloseAndJoin();
    CHECK(!runner.error);
    std::cout << "released_not_before_physical_close fd_open_unreleased=ok\n";
}

void EngineStopReleases() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);
    Engine client{std::shared_ptr<RawHandler>()};
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    std::atomic<int> delivered_result{-2};
    std::atomic<int> pending_result{-2};
    auto delivered_connection = std::make_shared<Connection>();
    auto delivered_receipt = std::make_shared<ReclaimReceipt>();
    auto delivered_task = Task([&] {
        auto res = client.CoDial(address, Until(2000ms));
        if (res.error || !res.connection.Valid()) {
            delivered_result.store(res.error ? res.error.value() : -1);
            return;
        }
        *delivered_connection = res.connection.Raw();
        *delivered_receipt = res.reclaim;
        delivered_result.store(0);
        // 保持连接存活不关（协程随即退出，观察交给主线程）：Engine 收尾必须
        // 代为收口。
    });
    Get(delivered_task);
    CHECK(delivered_result.load() == 0);
    // 已交付连接保活到位后，再制造 5s 占位 pending。
    EngineTestAccess::SetConnectStartDelay(client, std::chrono::milliseconds(5000));
    auto pending_receipt = std::make_shared<ReclaimReceipt>();
    auto pending_task = Task([&] {
        auto res = client.CoDial(address, Until(3000ms));
        *pending_receipt = res.reclaim;
        pending_result.store(res.error.value());
    });
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 1; }, 2000ms));
    runner.CloseAndJoin();
    Get(pending_task);
    CHECK(pending_result.load() == std::make_error_code(std::errc::operation_canceled).value());
    // Engine 收尾后主线程统一观察：已交付连接与未交付占位都可达释放。
    CHECK(pending_receipt->Valid());
    CHECK(WaitUntil([&] {
        return delivered_receipt->Released() && delivered_connection->Released()
            && pending_receipt->Released();
    }, 3000ms));
    CHECK(client.Phase() == EnginePhase::Stopped);
    CHECK(client.CountConnections() == 0);
    CHECK(EngineTestAccess::FirstConnectionFd(client) == -1);
    EngineTestAccess::ClearConnectInjection(client);
    CHECK(!runner.error);
    std::cout << "engine_stop_releases delivered=ok pending=ok\n";
}

void DriverFailedReleases() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);
    Engine client{std::shared_ptr<RawHandler>()};
    EngineRunner runner(client, ClientConfig());
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
    EngineTestAccess::SetConnectStartDelay(client, std::chrono::milliseconds(5000));
    std::atomic<int> result{-2};
    auto task = Task([&] {
        auto res = client.CoDial(address, Until(3000ms));
        result.store(res.error.value());
        CHECK(res.reclaim.Valid());
        // driver 中止 → 控制域封口解绑，占位责任必须可达释放。
        CHECK(WaitUntil([&] { return res.reclaim.Released(); }, 3000ms));
    });
    CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 1; }, 2000ms));
    CHECK(!EngineTestAccess::InjectDriverFailure(client));
    runner.CloseAndJoin();
    Get(task);
    CHECK(result.load() != 0);  // 中止路径按取消终结，不伪报成功
    CHECK(client.Phase() == EnginePhase::Stopped);
    CHECK(EngineTestAccess::ConnectTableSize(client) == 0);
    EngineTestAccess::ClearConnectInjection(client);
    CHECK(runner.error);  // Run 经真实错误出口返回
    std::cout << "driver_failed_releases aborted_pending_settled=ok\n";
}

// 最后一次独立审查的真实 fd 反例：按远端端口锁定 fd，再用 inode 排除复用。
int FindConnectedFd(std::uint16_t port) {
    for (const auto& item : std::filesystem::directory_iterator("/proc/self/fd")) {
        const int fd = std::stoi(item.path().filename().string());
        sockaddr_in peer{};
        socklen_t size = sizeof(peer);
        if (::getpeername(fd, reinterpret_cast<sockaddr*>(&peer), &size) == 0 &&
            peer.sin_family == AF_INET && ntohs(peer.sin_port) == port) { return fd; }
    }
    return -1;
}

void DriverFailedWithLiveFd() {
    StartRuntime(2);
    unsigned violations = 0;
    for (unsigned round = 0; round != 32; ++round) {
        Address address;
        auto listener = MakeListener(address);
        Engine client{std::shared_ptr<RawHandler>()};
        EngineRunner runner(client, ClientConfig());
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
        EngineTestAccess::SetConnectDeliveryDelay(client, 5000ms);
        ReclaimReceipt receipt;
        std::error_code dial_error;
        auto task = Task([&] {
            auto res = client.CoDial(address, Until(50ms));
            receipt = res.reclaim;
            dial_error = res.error;
        });
        Get(task);
        CHECK(dial_error == std::errc::timed_out && receipt.Valid() && !receipt.Released());
        std::error_code error;
        Address remote;
        auto accepted = listener.Accept(remote, error);
        CHECK(!error && accepted.Fd() >= 0); // 真实 TCP 已完成，不是无 fd 的占位计时器。
        const int fd = FindConnectedFd(address.Port());
        struct stat before{};
        CHECK(fd >= 0 && ::fstat(fd, &before) == 0);
        std::atomic<bool> marked{false}, premature{false};
        std::thread observer([&] {
            const auto deadline = std::chrono::steady_clock::now() + 3s;
            while (!receipt.Released() && std::chrono::steady_clock::now() < deadline) {
                std::this_thread::yield();
            }
            if (receipt.Released()) {
                struct stat at_release{};
                premature = ::fstat(fd, &at_release) == 0 && at_release.st_ino == before.st_ino;
                marked = true;
            }
        });
        CHECK(!EngineTestAccess::InjectDriverFailure(client));
        runner.CloseAndJoin();
        observer.join();
        CHECK(marked && runner.error && client.Phase() == EnginePhase::Stopped);
        CHECK(client.CountConnections() == 0 && EngineTestAccess::ConnectTableSize(client) == 0);
        if (premature) { ++violations; }
        std::cout << "driver_fd round=" << round
                  << " fd_open_at_first_released=" << premature.load() << '\n';
        CHECK(!accepted.Close() && !listener.Close());
        CHECK(WaitUntil([&] {
            struct stat after{};
            return ::fstat(fd, &after) != 0 || after.st_ino != before.st_ino;
        }, 3000ms));
    }
    std::cout << "driver_fd premature_released_total=" << violations << "/32\n";
    CHECK(violations == 0);
}

int RunCases(int argc, char** argv) {
    try {
        CHECK(argc == 2);
        const std::string test = argv[1];
        if (test == "receipt_invalid_default") { ReceiptInvalidDefault(); }
        else if (test == "codial_success_close_released") { CoDialSuccessCloseReleased(); }
        else if (test == "codial_timeout_undelivered") { CoDialTimeoutUndelivered(); }
        else if (test == "dial_refused_released") { DialRefusedReleased(); }
        else if (test == "dial_pre_submit_rejections") { DialPreSubmitRejections(); }
        else if (test == "released_not_before_physical_close") { ReleasedNotBeforePhysicalClose(); }
        else if (test == "engine_stop_releases") { EngineStopReleases(); }
        else if (test == "driver_failed_releases") { DriverFailedReleases(); }
        else if (test == "driver_failed_live_fd") { DriverFailedWithLiveFd(); }
        else {
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
