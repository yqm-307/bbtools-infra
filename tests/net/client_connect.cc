// Client 最小 TCP Dial + owner 关闭安全（client-fix-102）：真实回环连接/拒绝/
// deadline 晚到清理/Stop 与未决 Dial 竞争/域拒绝。公共 API 面（Client/Engine/
// EventHandler/Address/NativeSocket）消费；未建 stub、未放宽断言。
//
// 真实未决 connect 窗口的制造（不依赖网络黑洞、不 sleep 猜阶段）：本地 listener
// listen(backlog=1)（与下方全部调用点一致）在
// Linux 语义下容纳 1 条已完成三次握手的连接；先用真实成功的 Dial 占满 accept 队列，
// 后续 Connect 的 SYN 未被接纳而停留在真实未决窗口，由 deadline/Stop 取消路径收口。
// 未决窗口的判定不猜超时：读 /proc/net/tcp 中指向对端端口的 SYN_SENT 记录；
// 「async_wait 已武装」另需 dup watcher 实证（同 inode 的 fd >= 2），只证「connect
// 未决」不足以证明武装（105 P2）。
//
// 一 case 一独立进程，经 tests/net/ProcessRuntimeTestMain.hpp 收口退出边界。
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <system_error>
#include <thread>
#include <unistd.h>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Client.hpp>
#include <bbt/infra/net/CoConnectionHandler.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/EventHandler.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/Options.hpp>
#include <bbt/infra/net/detail/EventLoop.hpp>

using namespace std::chrono_literals;
using bbt::infra::net::Action;
using bbt::infra::net::Address;
using bbt::infra::net::Client;
using bbt::infra::net::CoTcpConnection;
using bbt::infra::net::Engine;
using bbt::infra::net::EventHandler;
using bbt::infra::net::ICoConnection;
using bbt::infra::net::ICoConnectionHandler;
using bbt::infra::net::NativeSocket;
using bbt::infra::net::SocketType;

static int g_failures = 0;
#define CHECK(cond) do { if (!(cond)) { \
    std::cerr << "CHECK failed: " #cond " @ line " << __LINE__ << '\n'; ++g_failures; } } while (0)

static bool IsError(const std::error_code& error, std::errc expected) {
    return error == std::make_error_code(expected);
}

// ---- 回环 server（公共 Engine）：端口 0 由内核分配，回显 payload ----
class EchoHandler final : public EventHandler {
public:
    std::atomic<int> opens{0};
    Action OnOpen(ICoConnection&) override { opens.fetch_add(1); return Action::None; }
    Action OnTraffic(ICoConnection& connection, bool wake) override {
        if (!wake) {
            const auto view = connection.Peek();
            if (!view.empty()) {
                (void)connection.Write(std::string(view), std::chrono::steady_clock::now() + 2s);
            }
        }
        return Action::None;
    }
    void OnClose(ICoConnection&, const std::error_code&) override {}
};

// ---- 客户端 handler：记录 OnOpen/OnTraffic，OnOpen 内回写一次 ----
class ClientHandler final : public ICoConnectionHandler {
public:
    std::atomic<int> opens{0};
    std::string echoed;
    std::mutex m;
    ICoConnection* bound{nullptr};

    void OnOpen(ICoConnection& connection) override {
        {
            std::lock_guard<std::mutex> lock(m);
            bound = &connection;
        }
        opens.fetch_add(1);
        (void)connection.Write(std::string("PING"), std::chrono::steady_clock::now() + 2s);
    }
    void OnTraffic(ICoConnection& connection, bool) override {
        const auto view = connection.Peek();
        if (!view.empty()) {
            std::lock_guard<std::mutex> lock(m);
            echoed.append(view);
            (void)connection.Discard(view.size());
        }
    }
    void OnClose(ICoConnection&, const std::error_code&) override {}
};

static Address Loopback(int port) {
    std::error_code error;
    const auto address = Address::Ip("127.0.0.1", port, error);
    if (error || !address.Valid()) { std::cerr << "bad loopback address\n"; std::abort(); }
    return address;
}

// 动态端口监听 socket（不 accept）。backlog=1：Linux 语义下已完成三次握手的连接最多
// 排队 1 条，与顶部说明同一口径（本文件所有调用点均传 1）。
static NativeSocket ListenPort(int backlog, int& port) {
    std::error_code error;
    auto socket = NativeSocket::Open(bbt::infra::net::AddressFamily::IPv4, SocketType::Stream, error);
    if (error) { std::cerr << "open listener failed\n"; std::abort(); }
    if (const auto bind_error = socket.Bind(Loopback(0))) { std::cerr << "bind failed\n"; std::abort(); }
    if (const auto listen_error = socket.Listen(backlog)) { std::cerr << "listen failed\n"; std::abort(); }
    const auto bound = socket.LocalAddress(error);
    if (error) { std::cerr << "local address failed\n"; std::abort(); }
    port = bound.Port();
    return socket;
}

// 取一个当前无监听的端口（bind 后立即 close）。
static int ClosedPort() {
    std::error_code error;
    auto probe = NativeSocket::Open(bbt::infra::net::AddressFamily::IPv4, SocketType::Stream, error);
    if (error) { std::abort(); }
    if (probe.Bind(Loopback(0))) { std::abort(); }
    const auto local = probe.LocalAddress(error);
    (void)probe.Close();
    return local.Port();
}

static std::size_t CountFds() {
    DIR* dir = ::opendir("/proc/self/fd");
    if (dir == nullptr) { return 0; }
    std::size_t count = 0;
    while (auto* entry = ::readdir(dir)) {
        const std::string name{entry->d_name};
        if (name != "." && name != "..") { ++count; }
    }
    ::closedir(dir);
    return count;
}

// 外层（harness）超时的独立错误值：绝不等于 Dial 的 std::errc::timed_out，否则
// 「业务协程根本没返回」会被 IsError(dial, timed_out) 当成 deadline 命中而假绿。
class HarnessTimeoutCategory final : public std::error_category {
public:
    const char* name() const noexcept override { return "bbt.net.test.harness_timeout"; }
    std::string message(int) const override { return "test harness outer timeout"; }
};
static const std::error_category& HarnessTimeoutCategoryRef() {
    static const HarnessTimeoutCategory category;
    return category;
}
static std::error_code HarnessTimedOut() { return {7, HarnessTimeoutCategoryRef()}; }

// 真实未决 connect 的可观察收据：/proc/net/tcp 中存在指向 peer 端口且状态为
// SYN_SENT(0x02) 的条目 ⇒ owner 域已执行非阻塞 connect 并停在真实未决窗口
// （已建立的 filler 连接状态是 0x01，不会混淆）。不靠时间猜测阶段。
static bool HasSynSentTo(int port) {
    char remote[64];
    std::snprintf(remote, sizeof(remote), "0100007F:%04X", static_cast<unsigned>(port));
    FILE* file = std::fopen("/proc/net/tcp", "r");
    if (file == nullptr) { return false; }
    char line[512];
    bool found = false;
    (void)std::fgets(line, sizeof(line), file);   // 表头
    while (std::fgets(line, sizeof(line), file) != nullptr) {
        char local[64];
        char rem[64];
        unsigned state = 0;
        if (std::sscanf(line, "%*d: %63s %63s %x", local, rem, &state) != 3) { continue; }
        if (state == 0x02U && std::strcmp(rem, remote) == 0) { found = true; }
    }
    std::fclose(file);
    return found;
}

static bool WaitSynSent(int port, int tries = 300) {
    for (int i = 0; i < tries; ++i) {
        if (HasSynSentTo(port)) { return true; }
        std::this_thread::sleep_for(10ms);
    }
    return false;
}

// 读取 /proc/net/tcp 中指向 peer 端口的 SYN_SENT 条目的 socket inode（0 = 无该未决连接）。
static unsigned long PendingSocketInode(int port) {
    char remote[64];
    std::snprintf(remote, sizeof(remote), "0100007F:%04X", static_cast<unsigned>(port));
    FILE* file = std::fopen("/proc/net/tcp", "r");
    if (file == nullptr) { return 0; }
    char line[512];
    unsigned long inode = 0;
    (void)std::fgets(line, sizeof(line), file);   // 表头
    while (std::fgets(line, sizeof(line), file) != nullptr) {
        char local[64];
        char rem[64];
        unsigned state = 0;
        unsigned long candidate = 0;
        // 列序：sl local rem st tx:rx tr:when retrnsmt uid timeout inode
        if (std::sscanf(line, "%*d: %63s %63s %x %*s %*s %*s %*u %*u %lu",
                local, rem, &state, &candidate) != 4) { continue; }
        if (state == 0x02U && std::strcmp(rem, remote) == 0) { inode = candidate; break; }
    }
    std::fclose(file);
    return inode;
}

// 指向该 socket inode 的本进程 fd 个数（/proc/self/fd 的 readlink 目标）。dup() 出的
// watcher fd 与源 socket 共享同一 inode，故 >= 2 才证明 owner 域已 dup 出 watcher；
// 仅 1 个 fd 与「只 connect 未武装」不可区分（105 P2）。
static int CountFdsWithInode(unsigned long inode) {
    if (inode == 0) { return 0; }
    char needle[64];
    std::snprintf(needle, sizeof(needle), "socket:[%lu]", inode);
    DIR* dir = ::opendir("/proc/self/fd");
    if (dir == nullptr) { return 0; }
    int count = 0;
    while (const auto* entry = ::readdir(dir)) {
        char target[256];
        const std::string link = std::string("/proc/self/fd/") + entry->d_name;
        const auto len = ::readlink(link.c_str(), target, sizeof(target) - 1);
        if (len <= 0) { continue; }
        target[len] = '\0';
        if (std::strcmp(target, needle) == 0) { ++count; }
    }
    ::closedir(dir);
    return count;
}

// /proc/net/tcp 中指向该端口的 SYN_SENT 条目数（1 = 恰一条真实未决连接；上一轮 Dial
// 的晚到清理尚未完成时可能短暂 > 1，故收据要求恰 1 条）。
static int CountSynSentTo(int port) {
    char remote[64];
    std::snprintf(remote, sizeof(remote), "0100007F:%04X", static_cast<unsigned>(port));
    FILE* file = std::fopen("/proc/net/tcp", "r");
    if (file == nullptr) { return 0; }
    char line[512];
    int count = 0;
    (void)std::fgets(line, sizeof(line), file);
    while (std::fgets(line, sizeof(line), file) != nullptr) {
        char local[64];
        char rem[64];
        unsigned state = 0;
        if (std::sscanf(line, "%*d: %63s %63s %x", local, rem, &state) != 3) { continue; }
        if (state == 0x02U && std::strcmp(rem, remote) == 0) { ++count; }
    }
    std::fclose(file);
    return count;
}

// 「已武装」收据：恰一条真实未决连接（SYN_SENT）且该 inode 的 fd >= 2（dup watcher
// 存活）⇒ owner 域已执行到 async_wait 紧邻的 dup 之后。不依赖 fd 计数基线。
static bool ArmedWatcherHeld(int port) {
    if (CountSynSentTo(port) != 1) { return false; }
    return CountFdsWithInode(PendingSocketInode(port)) >= 2;
}

static bool WaitArmedWatcher(int port, int tries = 400) {
    for (int i = 0; i < tries; ++i) {
        if (ArmedWatcherHeld(port)) { return true; }
        std::this_thread::sleep_for(10ms);
    }
    return false;
}

// Dial 超时路径取消投递失败的注入点：linkwrap 真实的
// bbt::infra::net::EventLoop::Submit(std::function<void()>) —— 生产 Client.cc 就是经该
// 符号把取消投进 owner 域（同 co_bridge_service_gate 的测试私有 wrap 范式，不新增生产
// 注入接口）。布防后命中的那一次按真实「队列满」语义返回 Full（命令未入队）。
static std::atomic<bool> g_reject_next_submit{false};
static std::atomic<int>  g_rejected_submits{0};
using bbt::infra::net::EventLoop;
using bbt::infra::net::SubmitStatus;
extern "C" SubmitStatus __real__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEE(
    EventLoop* self, std::function<void()> command);
extern "C" SubmitStatus __wrap__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEE(
    EventLoop* self, std::function<void()> command) {
    if (g_reject_next_submit.exchange(false)) {
        ++g_rejected_submits;
        return SubmitStatus::Full;
    }
    return __real__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEE(self, std::move(command));
}

// Stop 控制路径（RunOwner）的提交来源：linkwrap 真实的双参数
// EventLoop::Submit(std::function<void()>, std::error_code&)。该重载只有 Client::Stop 的
// RunOwner 使用（Dial 侧投递走单参数重载），故布防后命中的恰是取消/关闭步骤。按真实
// 分配失败语义置 submit_error 并返回 Failed（命令未入队），恰一次。
static std::atomic<bool> g_fail_next_submit2{false};
static std::atomic<int>  g_rejected_submits2{0};
static std::atomic<unsigned long long> g_rejected_submit2_tid{0};
extern "C" SubmitStatus __real__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEERSt10error_code(
    EventLoop* self, std::function<void()> command, std::error_code& submit_error);
extern "C" SubmitStatus __wrap__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEERSt10error_code(
    EventLoop* self, std::function<void()> command, std::error_code& submit_error) {
    if (g_fail_next_submit2.exchange(false)) {
        ++g_rejected_submits2;
        g_rejected_submit2_tid.store(std::hash<std::thread::id>{}(std::this_thread::get_id()));
        submit_error = std::make_error_code(std::errc::not_enough_memory);
        return SubmitStatus::Failed;
    }
    return __real__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEERSt10error_code(
        self, std::move(command), submit_error);
}

// Start 的「owner 已起、尚未发布」同步闸：linkwrap 真实 EventLoop::Start（Client 起
// owner 后、发布前的必经真实调用；无生产钩子）。布防后首次命中的调用在真实 Start 返回
// 后阻塞，等测试线程先完成 Stop 再放行发布检查，从而确定性命中发布前的交错窗口。
static std::atomic<bool> g_start_gate_armed{false};
static std::atomic<int>  g_start_gate_hits{0};
static std::atomic<bool> g_start_gate_released{false};
static std::promise<void>* g_start_gate_entered{nullptr};
extern "C" bool __real__ZN3bbt5infra3net9EventLoop5StartEv(EventLoop* self);
extern "C" bool __wrap__ZN3bbt5infra3net9EventLoop5StartEv(EventLoop* self) {
    const bool started = __real__ZN3bbt5infra3net9EventLoop5StartEv(self);
    if (g_start_gate_armed.exchange(false)) {
        ++g_start_gate_hits;
        if (g_start_gate_entered != nullptr) { g_start_gate_entered->set_value(); }
        while (!g_start_gate_released.load()) { std::this_thread::sleep_for(1ms); }
    }
    return started;
}

template <typename Fn>
static std::error_code RunInCoroutine(Fn&& fn, const char* name, std::chrono::seconds timeout) {
    auto promise = std::make_shared<std::promise<std::error_code>>();
    auto done = promise->get_future();
    g_scheduler->RegistCoroutineTask(65536,
        [promise, fn = std::forward<Fn>(fn)]() mutable {
            try { promise->set_value(fn()); }
            catch (...) { promise->set_exception(std::current_exception()); }
        }, name);
    if (done.wait_for(timeout) != std::future_status::ready) {
        // 外层超时：独立错误值，任何 IsError(..., timed_out) 都不接受它。
        return HarnessTimedOut();
    }
    return done.get();
}

// 反复用真实成功的 Dial 占满 listener 的 accept 队列，直到某次 Dial 出现真实未决
// （短 deadline 到期）——此刻即为可观察的未决窗口；fillers 保活以维持队列占满。
static bool FillUntilPending(Client& client, int port,
    std::vector<std::shared_ptr<CoTcpConnection>>& fillers, int attempts = 8) {
    for (int i = 0; i < attempts; ++i) {
        std::shared_ptr<CoTcpConnection> candidate;
        const auto error = RunInCoroutine([&]() {
            return client.Dial(Loopback(port), candidate, std::chrono::steady_clock::now() + 200ms);
        }, "net.client_fill", 6s);
        if (IsError(error, std::errc::timed_out)) { return true; }
        if (error || !candidate) { return false; }
        fillers.push_back(candidate);
    }
    return false;
}

// 有界等待 fd 计数回落到基线。
static bool WaitFdReturn(std::size_t baseline, int tries = 300) {
    for (int i = 0; i < tries; ++i) {
        if (CountFds() <= baseline) { return true; }
        std::this_thread::sleep_for(10ms);
    }
    return false;
}

// case 1：真实回环成功 + 句柄可读 + OnOpen 最终派发 + 顺序回显。
static int CaseRoundtrip() {
    auto server_handler = std::make_shared<EchoHandler>();
    Engine server(server_handler);
    if (const auto error = server.Run(Loopback(0))) {
        std::cerr << "server run failed: " << error.message() << '\n';
        return 1;
    }
    std::vector<Address> bound;
    if (server.LocalAddresses(bound) || bound.empty()) { std::cerr << "no bound address\n"; return 1; }
    const int port = bound.front().Port();

    auto handler = std::make_shared<ClientHandler>();
    Client client(handler);
    if (const auto error = client.Start()) { std::cerr << "client start failed\n"; return 1; }

    std::shared_ptr<CoTcpConnection> connection;
    const auto dial = RunInCoroutine([&]() {
        return client.Dial(Loopback(port), connection, std::chrono::steady_clock::now() + 3s);
    }, "net.client_roundtrip", 6s);
    CHECK(!dial);
    CHECK(connection != nullptr);
    if (connection) { CHECK(connection->PeerAddress().Port() == port); }

    // OnOpen/OnTraffic 异步派发：有界等待回显（不断言相对 Dial 的顺序）。
    for (int i = 0; i < 300; ++i) {
        bool got = false;
        {
            std::lock_guard<std::mutex> lock(handler->m);
            got = handler->echoed == "PING";
        }
        if (got) { break; }
        std::this_thread::sleep_for(10ms);
    }
    CHECK(handler->opens.load() >= 1);
    {
        std::lock_guard<std::mutex> lock(handler->m);
        CHECK(handler->echoed == "PING");
    }
    CHECK(server_handler->opens.load() >= 1);
    CHECK(!client.Stop());
    CHECK(!server.Stop());
    return g_failures == 0 ? 0 : 1;
}

// case 2：已关闭端口 → ECONNREFUSED；out 不变；fd 回落（预热后再对比第二次尝试）。
static int CaseRefuse() {
    const int port = ClosedPort();
    auto handler = std::make_shared<ClientHandler>();
    Client client(handler);
    if (const auto error = client.Start()) { std::cerr << "start failed\n"; return 1; }
    // 预热：首次受管协程等待可能懒建 runtime fd，故以预热后的基线对比第二次失败。
    {
        std::shared_ptr<CoTcpConnection> warm_connection;
        (void)RunInCoroutine([&]() {
            return client.Dial(Loopback(port), warm_connection, std::chrono::steady_clock::now() + 3s);
        }, "net.client_refuse_warm", 6s);
    }
    const auto warm = CountFds();
    std::shared_ptr<CoTcpConnection> connection;
    const auto dial = RunInCoroutine([&]() {
        return client.Dial(Loopback(port), connection, std::chrono::steady_clock::now() + 3s);
    }, "net.client_refuse", 6s);
    CHECK(IsError(dial, std::errc::connection_refused));
    CHECK(connection == nullptr);
    CHECK(!client.Stop());
    const auto after = CountFds();
    CHECK(after <= warm);
    return g_failures == 0 ? 0 : 1;
}

// case 3：真实未决 connect 窗口 → deadline 到期 timed_out、out 不变、晚到清理回落。
// 判据分层，避免把外层 harness 超时当成 deadline 命中：外层用独立错误值；内层用 Dial
// 直接结果 + 实测耗时（必须落在 deadline 下限与明确总上界之间）。
static int CaseDeadline() {
    CHECK(HarnessTimedOut() != std::make_error_code(std::errc::timed_out));
    int port = 0;
    auto listener = ListenPort(1, port);   // 占满后后续 Connect 停留在真实未决窗口
    auto handler = std::make_shared<ClientHandler>();
    Client client(handler);
    if (const auto error = client.Start()) { std::cerr << "start failed\n"; return 1; }
    std::vector<std::shared_ptr<CoTcpConnection>> fillers;
    bool saw_timeout = false;
    for (int attempt = 0; attempt < 8 && !saw_timeout; ++attempt) {
        const std::size_t before = CountFds();
        std::shared_ptr<CoTcpConnection> connection;
        auto elapsed_ms = std::make_shared<std::atomic<long long>>(-1);
        const auto dial = RunInCoroutine([&]() {
            const auto begin = std::chrono::steady_clock::now();
            const auto error = client.Dial(Loopback(port), connection,
                std::chrono::steady_clock::now() + 250ms);
            elapsed_ms->store(std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - begin).count());
            return error;
        }, "net.client_deadline", 6s);
        if (IsError(dial, std::errc::timed_out)) {
            saw_timeout = true;
            CHECK(connection == nullptr);        // 失败/超时 out 不变
            // 内层执行收据：确实按 250ms deadline 返回并落在明确上界内；内层不返回时
            // elapsed 为 -1、外层独立值也不接受，两者都不会被放过。
            CHECK(elapsed_ms->load() >= 200);
            CHECK(elapsed_ms->load() <= 3000);
            CHECK(WaitFdReturn(before));         // watcher/socket fd 有界晚到清理并回落
            break;
        }
        CHECK(!dial);                            // 只允许「成功占满」或「超时」
        CHECK(connection != nullptr);
        fillers.push_back(connection);
    }
    CHECK(saw_timeout);
    CHECK(!client.Stop());
    (void)listener.Close();
    return g_failures == 0 ? 0 : 1;
}

// case 4：Dial 未决中 Stop → Stop 有界返回（不因 async_wait 卡住）；Dial 侧明确错误；
// 停止后新 Dial 被拒绝。
static int CaseStopPending() {
    int port = 0;
    auto listener = ListenPort(1, port);
    auto handler = std::make_shared<ClientHandler>();
    Client client(handler);
    if (const auto error = client.Start()) { std::cerr << "start failed\n"; return 1; }
    // 占满 accept 队列（真实成功连接保活），使被观察的 Dial 停留在真实未决窗口。
    std::vector<std::shared_ptr<CoTcpConnection>> fillers;
    CHECK(FillUntilPending(client, port, fillers));

    auto entered = std::make_shared<std::promise<void>>();
    auto entered_future = entered->get_future();
    auto dial_result = std::make_shared<std::atomic<int>>(9999);
    const std::size_t pending_baseline = CountFds();
    g_scheduler->RegistCoroutineTask(65536, [&client, port, entered, dial_result]() {
        entered->set_value();
        std::shared_ptr<CoTcpConnection> connection;
        const auto error = client.Dial(Loopback(port), connection,
            std::chrono::steady_clock::now() + 30s);
        dial_result->store(error ? error.value() : 0);
    }, "net.client_stop_pending");
    CHECK(entered_future.wait_for(3s) == std::future_status::ready);
    // 已武装的可观察收据（替换原先 sleep 80ms 的猜阶段）：owner 域执行非阻塞 connect
    // 后 /proc/net/tcp 恰一条指向该端口的 SYN_SENT，且该 inode 的 fd >= 2（dup watcher
    // 存活）⇒ 已执行到 async_wait 紧邻的 dup 之后，而非仅「connect 未决」。
    CHECK(WaitSynSent(port));
    CHECK(WaitArmedWatcher(port));

    std::error_code stop_error = std::make_error_code(std::errc::timed_out);
    std::atomic<bool> stop_done{false};
    std::thread stopper([&]() { stop_error = client.Stop(); stop_done.store(true); });
    bool stop_bounded = false;
    for (int i = 0; i < 800; ++i) {   // 8s 上界：Stop 不得被未决 async_wait 拖到 SYN 超时
        if (stop_done.load()) { stop_bounded = true; break; }
        std::this_thread::sleep_for(10ms);
    }
    CHECK(stop_bounded);               // Stop 有界返回
    stopper.join();
    CHECK(!stop_error);                // 且无错
    CHECK(dial_result->load() != 9999);        // Dial 侧已结算
    CHECK(dial_result->load() != 0);           // 且为明确错误（未成功）
    CHECK(WaitFdReturn(pending_baseline));     // Stop 后 fd 回落：无孤儿 socket/watcher
    // 停止后新 Dial 被拒绝。
    std::shared_ptr<CoTcpConnection> post;
    const auto post_dial = RunInCoroutine([&]() {
        return client.Dial(Loopback(port), post, std::chrono::steady_clock::now() + 1s);
    }, "net.client_stop_after", 4s);
    CHECK(IsError(post_dial, std::errc::operation_canceled));
    (void)listener.Close();
    return g_failures == 0 ? 0 : 1;
}

// case 5：域门禁——非受管协程调用 Dial 在任何副作用之前拒绝、out 不变、无 fd。
static int CaseDomainReject() {
    int port = 0;
    auto listener = ListenPort(4, port);
    auto handler = std::make_shared<ClientHandler>();
    Client client(handler);
    if (const auto error = client.Start()) { std::cerr << "start failed\n"; return 1; }
    const auto warm = CountFds();
    std::shared_ptr<CoTcpConnection> connection;
    const auto dial = client.Dial(Loopback(port), connection, std::chrono::steady_clock::now() + 1s);
    CHECK(IsError(dial, std::errc::operation_not_permitted));
    CHECK(connection == nullptr);
    CHECK(CountFds() <= warm);
    CHECK(!client.Stop());
    (void)listener.Close();
    return g_failures == 0 ? 0 : 1;
}

// case 6：并发 Start 的启动者唯一性，以及 Stop 进入终态后不可被启动复活。
static int CaseStartRace() {
    // 单 owner 的 num_event_loop 口径：默认/显式 1 接受，归一化 >1（显式多值或
    // multicore 推导）明确 operation_not_supported；失败后必须回落 Idle 可重试。
    {
        auto probe_handler = std::make_shared<ClientHandler>();
        Client probe(probe_handler);
        const std::size_t probe_fds = CountFds();
        bbt::infra::net::Options many_options;
        many_options.num_event_loop = 2;
        CHECK(IsError(probe.Start(many_options), std::errc::operation_not_supported));
        bbt::infra::net::Options multicore_options;
        multicore_options.multicore = true;
        CHECK(IsError(probe.Start(multicore_options), std::errc::operation_not_supported));
        CHECK(CountFds() <= probe_fds);          // 被拒时未起 owner、无 fd 残留
        bbt::infra::net::Options single_options;
        single_options.num_event_loop = 1;
        CHECK(!probe.Start(single_options));     // 显式 1 与默认等价（且证明失败已回落）
        CHECK(!probe.Stop());
    }
    auto handler = std::make_shared<ClientHandler>();
    Client client(handler);
    std::atomic<int> ok{0};
    std::atomic<int> busy{0};
    std::atomic<int> other{0};
    std::vector<std::thread> starters;
    for (int i = 0; i < 2; ++i) {
        starters.emplace_back([&client, &ok, &busy, &other]() {
            const auto error = client.Start();
            if (!error) { ok.fetch_add(1); return; }
            if (IsError(error, std::errc::device_or_resource_busy)) { busy.fetch_add(1); return; }
            other.fetch_add(1);
        });
    }
    for (auto& thread : starters) { thread.join(); }
    CHECK(ok.load() == 1);        // 恰好一位启动者：不存在两个 owner 同时被建
    CHECK(busy.load() == 1);      // 另一位被明确拒绝（不是静默失败）
    CHECK(other.load() == 0);
    const std::size_t running = CountFds();
    CHECK(!client.Stop());
    CHECK(!client.Stop());                                          // 幂等成功
    CHECK(IsError(client.Start(), std::errc::operation_canceled));   // 终态不可复活
    CHECK(CountFds() <= running);                                    // 也未残留 owner fd
    std::shared_ptr<CoTcpConnection> post;
    const auto post_dial = RunInCoroutine([&client, &post]() {
        return client.Dial(Loopback(ClosedPort()), post, std::chrono::steady_clock::now() + 1s);
    }, "net.client_start_race_post", 4s);
    CHECK(IsError(post_dial, std::errc::operation_canceled));
    CHECK(post == nullptr);
    return g_failures == 0 ? 0 : 1;
}

// case 7：合法控制域析构（析构契约前置成立：对本对象的 Dial 已全部返回）+ 存活 shell。
// 「owner 域仍有已武装 watcher」的合法窗口来自：Dial 的取消投递被拒（登记保留、watcher
// 未撤）后该 Dial 以 timed_out 返回。析构单次 Stop 先取消该 watcher 再收口；同一次 Stop
// 内再注入一次双参数提交故障 ⇒ 一次有界补偿恢复（不测持续失败）。已交付 shell 跨 Client
// 销毁存活并在受管协程内给出确定性拒绝/已关闭证据；handler 活过其最后一个回调。
static int CaseDestroyWithLiveShell() {
    int port = 0;
    auto listener = ListenPort(1, port);
    auto handler = std::make_shared<ClientHandler>();
    auto client = std::make_shared<Client>(handler);
    if (const auto error = client->Start()) { std::cerr << "start failed\n"; return 1; }
    std::shared_ptr<CoTcpConnection> shell;
    const auto dial = RunInCoroutine([&]() {
        return client->Dial(Loopback(port), shell, std::chrono::steady_clock::now() + 3s);
    }, "net.client_destroy_shell", 6s);
    CHECK(!dial);
    CHECK(shell != nullptr);
    for (int i = 0; i < 300 && handler->opens.load() == 0; ++i) { std::this_thread::sleep_for(10ms); }
    CHECK(handler->opens.load() >= 1);
    // 占满 accept 队列，再让一个 Dial 在真实未决窗口到期且取消投递被拒（登记保留）。
    std::vector<std::shared_ptr<CoTcpConnection>> fillers;
    CHECK(FillUntilPending(*client, port, fillers));
    auto dial_result = std::make_shared<std::atomic<int>>(9999);
    g_scheduler->RegistCoroutineTask(65536, [&client, port, dial_result]() {
        std::shared_ptr<CoTcpConnection> connection;
        const auto error = client->Dial(Loopback(port), connection,
            std::chrono::steady_clock::now() + 300ms);
        dial_result->store(error ? error.value() : 0);
    }, "net.client_destroy_retained");
    CHECK(WaitSynSent(port));                 // 已停在真实未决窗口
    g_reject_next_submit.store(true);         // 布防：该 Dial 的取消投递被拒（登记保留）
    bool settled = false;
    for (int i = 0; i < 300; ++i) {
        if (dial_result->load() != 9999) { settled = true; break; }
        std::this_thread::sleep_for(10ms);
    }
    CHECK(settled);                           // Dial 已返回 ⇒ 析构契约前置成立
    CHECK(IsError(std::error_code(dial_result->load(), std::generic_category()),
        std::errc::timed_out));
    CHECK(g_rejected_submits.load() == 1);    // 取消投递确被拒 ⇒ 登记保留、watcher 未撤
    // 「arm 之后、Stop 之前」同步窗口：dup watcher 仍存活（同 inode fd >= 2）⇒ 已武装。
    CHECK(WaitArmedWatcher(port));
    const std::size_t baseline = CountFds();
    // 控制域最后引用析构（另起的控制线程，非 owner、非受管协程）：单次 Stop + 单次故障。
    std::atomic<bool> destroyed{false};
    g_fail_next_submit2.store(true);          // 布防先于析构：命中必为 Stop 的取消步骤
    std::thread destroyer([&client, &destroyed]() { client.reset(); destroyed.store(true); });
    const auto destroyer_id = destroyer.get_id();   // join 后 std::thread::get_id() 归零
    bool bounded = false;
    for (int i = 0; i < 300; ++i) {           // 3s 上界：补偿未生效则落在无界成员 Join
        if (destroyed.load()) { bounded = true; break; }
        std::this_thread::sleep_for(10ms);
    }
    CHECK(bounded);
    if (!bounded) { return g_failures == 0 ? 0 : 1; }   // 未完成则不 join（不终止进程）
    destroyer.join();
    CHECK(g_rejected_submits2.load() == 1);   // 单次故障恰一次命中
    CHECK(g_rejected_submit2_tid.load()
        == std::hash<std::thread::id>{}(destroyer_id));   // 绑定析构线程
    // 已交付 shell 跨 Client 销毁存活：受管协程内的确定性拒绝/已关闭证据。
    CHECK(shell != nullptr);
    CHECK(shell->PeerAddress().Port() == port);
    const auto write_error = RunInCoroutine([&]() {
        return shell->Write(std::string("X"), std::chrono::steady_clock::now() + 1s);
    }, "net.client_shell_write_after_destroy", 4s);
    CHECK(IsError(write_error, std::errc::operation_canceled));   // 强停后业务面明确拒绝
    const auto wake_error = RunInCoroutine([&]() { return shell->Wake(); },
        "net.client_shell_wake_after_destroy", 4s);
    CHECK(IsError(wake_error, std::errc::operation_canceled));    // 封口后 Wake 明确拒绝
    const auto close_error = RunInCoroutine([&]() {
        return shell->Close(bbt::infra::net::CloseMode::Unblock, {},
            std::chrono::steady_clock::now() + 1s);
    }, "net.client_shell_close_after_destroy", 4s);
    CHECK(!close_error);                      // 已物理关闭：明确幂等观察（不重开）
    CHECK(handler->opens.load() >= 1);        // handler 活过其最后一个回调
    CHECK(WaitFdReturn(baseline));
    (void)listener.Close();
    return g_failures == 0 ? 0 : 1;
}

// case 9：Stop 的取消步骤遇单次真实提交故障（双参数 Submit 恰一次 Failed）⇒ 一次有界
// 补偿恢复；首错保留、资源真实收口（可资源收口不代表返回成功）。不测持续失败（承诺外，
// 见本包报告），不以假绿代替。
static int CaseStopCancelSubmitFault() {
    int port = 0;
    auto listener = ListenPort(1, port);
    auto handler = std::make_shared<ClientHandler>();
    Client client(handler);
    if (const auto error = client.Start()) { std::cerr << "start failed\n"; return 1; }
    std::vector<std::shared_ptr<CoTcpConnection>> fillers;
    CHECK(FillUntilPending(client, port, fillers));
    auto dial_result = std::make_shared<std::atomic<int>>(9999);
    g_scheduler->RegistCoroutineTask(65536, [&client, port, dial_result]() {
        std::shared_ptr<CoTcpConnection> connection;
        const auto error = client.Dial(Loopback(port), connection,
            std::chrono::steady_clock::now() + 30s);
        dial_result->store(error ? error.value() : 0);
    }, "net.client_stop_cancel_fault");
    CHECK(WaitSynSent(port));
    CHECK(WaitArmedWatcher(port));            // Stop 前：未决 watcher 已武装
    const std::size_t baseline = CountFds();
    g_fail_next_submit2.store(true);          // 布防先于 Stop：命中必为取消步骤
    std::error_code stop_error = std::make_error_code(std::errc::timed_out);
    std::atomic<bool> stop_done{false};
    std::thread stopper([&]() { stop_error = client.Stop(); stop_done.store(true); });
    const auto stopper_id = stopper.get_id();   // join 后 std::thread::get_id() 归零
    bool bounded = false;
    for (int i = 0; i < 400; ++i) {           // 4s 上界（补偿预算 2×2s 之外）
        if (stop_done.load()) { bounded = true; break; }
        std::this_thread::sleep_for(10ms);
    }
    CHECK(bounded);
    stopper.join();
    CHECK(g_rejected_submits2.load() == 1);   // 单次故障恰一次命中
    CHECK(g_rejected_submit2_tid.load() == std::hash<std::thread::id>{}(stopper_id));
    CHECK(IsError(stop_error, std::errc::not_enough_memory));   // 首错保留（不报假成功）
    CHECK(dial_result->load() != 9999);       // 在途 Dial 已结算
    CHECK(dial_result->load() != 0);          // 且为明确错误
    CHECK(!HasSynSentTo(port));               // 未决连接已被 owner 域取消收口
    CHECK(!client.Stop());                    // 资源真实收口 ⇒ 幂等成功（已发布终态）
    CHECK(WaitFdReturn(baseline));            // 无 fd 残留
    std::shared_ptr<CoTcpConnection> post;
    const auto post_dial = RunInCoroutine([&]() {
        return client.Dial(Loopback(port), post, std::chrono::steady_clock::now() + 1s);
    }, "net.client_stop_cancel_fault_post", 4s);
    CHECK(IsError(post_dial, std::errc::operation_canceled));
    (void)listener.Close();
    return g_failures == 0 ? 0 : 1;
}

// case 10：Start 在「owner 已起、尚未发布」窗口与 Stop 的确定性交错（非概率 race）。
// 同步点用真实既有调用 EventLoop::Start（Client 起 owner 后、发布前的必经调用）：
// linkwrap 在其返回后布闸，等测试线程先完成 Stop 再放行发布检查；Start 不得复活终态，
// 且调用线程回收自己未发布的 owner（无 fd 残留）。
static int CaseStartStopPrepublish() {
    {
        auto probe_handler = std::make_shared<ClientHandler>();
        Client probe(probe_handler);
        CHECK(!probe.Start());
        CHECK(!probe.Stop());
    }
    const std::size_t baseline = CountFds();
    auto handler = std::make_shared<ClientHandler>();
    auto client = std::make_shared<Client>(handler);
    auto entered = std::make_shared<std::promise<void>>();
    auto entered_future = entered->get_future();
    g_start_gate_entered = entered.get();
    g_start_gate_released.store(false);
    g_start_gate_armed.store(true);
    std::error_code start_error = std::make_error_code(std::errc::timed_out);
    std::thread starter([&]() { start_error = client->Start(); });
    CHECK(entered_future.wait_for(5s) == std::future_status::ready);   // 已在 Start 内、未发布
    std::error_code stop_error = std::make_error_code(std::errc::timed_out);
    std::atomic<bool> stop_done{false};
    std::thread stopper([&]() { stop_error = client->Stop(); stop_done.store(true); });
    bool bounded = false;
    for (int i = 0; i < 300; ++i) {
        if (stop_done.load()) { bounded = true; break; }
        std::this_thread::sleep_for(10ms);
    }
    CHECK(bounded);
    stopper.join();
    CHECK(!stop_error);                       // 未发布相位下 Stop 入终态成功
    g_start_gate_released.store(true);        // 放行 Start 的发布检查
    starter.join();
    CHECK(g_start_gate_hits.load() == 1);     // 闸确在发布前命中一次
    CHECK(IsError(start_error, std::errc::operation_canceled));   // 不得复活终态
    CHECK(IsError(client->Start(), std::errc::operation_canceled));
    CHECK(WaitFdReturn(baseline));
    return g_failures == 0 ? 0 : 1;
}

// case 8：Dial 超时路径的取消投递被 owner 拒绝时登记必须保留，Stop 的取消清单仍覆盖
// 它（103 P2：Submit 结果被丢弃 + 立即 Remove 会让未决 watcher 拖住无界 Join）。
// 注入点是真实提交来源（生产 Client.cc 的 EventLoop::Submit），不自指。
static int CaseCancelHandoffRetained() {
    int port = 0;
    auto listener = ListenPort(1, port);
    auto handler = std::make_shared<ClientHandler>();
    Client client(handler);
    if (const auto error = client.Start()) { std::cerr << "start failed\n"; return 1; }
    std::vector<std::shared_ptr<CoTcpConnection>> fillers;
    CHECK(FillUntilPending(client, port, fillers));   // 队列已满：后续 Dial 必停未决窗口
    const std::size_t baseline = CountFds();
    auto dial_result = std::make_shared<std::atomic<int>>(9999);
    g_scheduler->RegistCoroutineTask(65536, [&client, port, dial_result]() {
        std::shared_ptr<CoTcpConnection> connection;
        const auto error = client.Dial(Loopback(port), connection,
            std::chrono::steady_clock::now() + 300ms);
        dial_result->store(error ? error.value() : 0);
    }, "net.client_cancel_handoff");
    CHECK(WaitSynSent(port));          // start 已交付并停在真实未决窗口
    g_reject_next_submit.store(true);  // 布防：下一次 Submit 即本 Dial 的取消投递
    bool settled = false;
    for (int i = 0; i < 300; ++i) {
        if (dial_result->load() != 9999) { settled = true; break; }
        std::this_thread::sleep_for(10ms);
    }
    CHECK(settled);
    CHECK(IsError(std::error_code(dial_result->load(), std::generic_category()),
        std::errc::timed_out));
    CHECK(g_rejected_submits.load() == 1);   // 取消投递确实被拒（注入命中）
    // 登记保留 ⇒ Stop 的取消清单仍覆盖该未决 watcher：有界返回、无错、连接收口。
    std::error_code stop_error = std::make_error_code(std::errc::timed_out);
    std::atomic<bool> stop_done{false};
    std::thread stopper([&]() { stop_error = client.Stop(); stop_done.store(true); });
    bool stop_bounded = false;
    for (int i = 0; i < 300; ++i) {   // 3s 上界：登记丢失时 Join 会等 SYN 重传
        if (stop_done.load()) { stop_bounded = true; break; }
        std::this_thread::sleep_for(10ms);
    }
    CHECK(stop_bounded);
    stopper.join();
    CHECK(!stop_error);
    CHECK(!HasSynSentTo(port));        // 未决连接已被 owner 域取消收口
    CHECK(WaitFdReturn(baseline));
    (void)listener.Close();
    return g_failures == 0 ? 0 : 1;
}

#include "ProcessRuntimeTestMain.hpp"

static int RunClientCases(int argc, char** argv) {
    const std::string scenario = argc > 1 ? argv[1] : "";
    g_bbt_coroutine_config->m_cfg_static_thread_num = 1;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start();   // 每进程一次；无 Stop，按 ProcessRuntimeTestMain 边界收口。

    if (scenario == "roundtrip") { return CaseRoundtrip(); }
    if (scenario == "refuse") { return CaseRefuse(); }
    if (scenario == "deadline_late_cleanup") { return CaseDeadline(); }
    if (scenario == "stop_pending_dial") { return CaseStopPending(); }
    if (scenario == "domain_reject") { return CaseDomainReject(); }
    if (scenario == "start_race_single_owner") { return CaseStartRace(); }
    if (scenario == "destroy_with_live_shell") { return CaseDestroyWithLiveShell(); }
    if (scenario == "cancel_handoff_retained") { return CaseCancelHandoffRetained(); }
    if (scenario == "stop_cancel_submit_fault") { return CaseStopCancelSubmitFault(); }
    if (scenario == "start_stop_prepublish") { return CaseStartStopPrepublish(); }
    std::cerr << "usage: Test_net_client_connect "
                 "roundtrip|refuse|deadline_late_cleanup|stop_pending_dial|domain_reject|"
                 "start_race_single_owner|destroy_with_live_shell|cancel_handoff_retained|"
                 "stop_cancel_submit_fault|start_stop_prepublish\n";
    return 2;
}

int main(int argc, char** argv) {
    return bbt_net_test::RunTestProcessExit([&] { return RunClientCases(argc, argv); });
}
