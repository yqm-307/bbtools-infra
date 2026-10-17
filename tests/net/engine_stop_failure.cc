// F4 P3 切片回归：公共 Engine 一次性控制/关闭边界故障的安全收尾。
//
// 范围只本包（不扩持续故障/runtime 内部失败/整体 F4）：
//   1) stop_close_before_fault：真实 TCP 两连接，Stop 的物理关闭在 Close 入口前
//      一次性 bad_alloc（wrap 原子标志恰一命中）。断言：Stop 返回首错
//      not_enough_memory（不假成功）；owner 上有界推进一次后网络真实收口
//      （wrap 调用=2 连接+1 重试、双客户端 EOF）；未收口不假 Stopped（收口后
//      LocalAddresses 拒绝、二次 Stop 幂等）；FD/线程回基线。红差异：旧代码无
//      有界推进 ⇒ Stop 内部悬挂不返回（Detach 前置拒绝后仍 Reset/发布终态的
//      不安全收尾），wrap 调用数只有 2。强停按契约压掉未开始 OnClose，handler
//      计数不作断言。
//   2) stop_close_after_fault：Close 完整后一次性 bad_alloc（Closed 重放路径）。
//      黑盒行为新旧等价（物理关闭已在首次完成、Detach 前置成立），作为分类/
//      资源回落的重放语义绿回归；红证据以 case 1/3/4 为准。
//   3) stop_submit_alloc_fault：控制线程 operator new 恰 1 次 bad_alloc（命中
//      Stop 的 SubmitCommand 分配：promise/闭包/入队）。断言：异常不穿出 Stop、
//      分类为 not_enough_memory（首错保留）、网络随后真实收口、资源回基线。
//      红差异：旧代码分配在 SubmitCommand 调用点 try 之外，bad_alloc 抛给调用方。
//   4) rollback_close_faults_not_idle：listener dup 一次 EMFILE（F5 fcntl 范式）
//      进入 RollbackStart；回滚中该 listener fd 的前两次真实关闭 EBADF。断言：
//      Run 返回原错 emfile；清理未收口不假 Idle（重试 Run = busy）；随后显式
//      Stop 在仍活 owner 上有界推进并真实收口（返回空）；FD/线程回基线。
//      红差异：旧代码忽略关闭错误仍回落 Idle ⇒ 重试 Run 成功。
//
// 注入面全部为本测试可执行文件的 link-wrap / 全局 operator new，不新增生产钩子；
// handler 挂起不阻网络 Stop 由既有 net.engine_stop_with_suspended_handler、
// net.engine_onclose_suspended_stop_destroy 覆盖；arms==callbacks 由
// co_bridge_exception_safety 的 stop_wait_settled 证据覆盖。

#include <dirent.h>
#include <execinfo.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <new>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/net/Action.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/EventHandler.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/Options.hpp>
#include <bbt/infra/net/detail/StreamConnection.hpp>
#include <bbt/infra/net/detail/EventLoop.hpp>

#include "ProcessRuntimeTestMain.hpp"

using namespace std::chrono_literals;

#define CHECK(condition)                                                          \
    do {                                                                          \
        if (!(condition)) {                                                       \
            std::cerr << "CHECK failed: " #condition " @ " << __FILE__ << ":"     \
                      << __LINE__ << std::endl;                                   \
            std::abort();                                                         \
        }                                                                         \
    } while (0)

namespace {

using Clock = std::chrono::steady_clock;
using bbt::infra::net::Action;
using bbt::infra::net::Address;
using bbt::infra::net::AddressFamily;
using bbt::infra::net::Engine;
using bbt::infra::net::EventHandler;
using bbt::infra::net::ICoConnection;
using bbt::infra::net::NativeSocket;
using bbt::infra::net::Options;
using bbt::infra::net::SocketType;

// ---- 注入面（仅本测试可执行文件） -------------------------------------------

// StreamConnection::Close 一次性故障：1=入口前 bad_alloc；3=真实 Close 完整后
// bad_alloc。原子标志跨 owner 线程生效（两条连接可能在两个 sub owner 上并发关闭，
// 原子 exchange 保证恰一次命中，不依赖具体命中哪条连接）。
std::atomic<int> g_close_fault{0};
std::atomic<int> g_close_calls{0};
std::atomic<int> g_close_injections{0};

// rollback 场景：fcntl wrap 观测到 listener 的 F_DUPFD_CLOEXEC 时记录该 fd；
// NativeSocket::Close wrap 在 Release 前对该 fd 的前 N 次关闭返回 EBADF，
// 因而失败保持在对象仍拥有 fd 的边界（其余调用真实转发）。
std::atomic<int> g_watch_fd{-1};
std::atomic<int> g_watch_closes{0};
std::atomic<int> g_watch_reject_limit{0};

// fcntl dup 注入（复用 F5 范式）：窗口内首次 F_DUPFD_CLOEXEC 返回一次 EMFILE。
std::atomic<bool> g_dup_failure_armed{false};
std::atomic<int> g_dup_injections{0};

// 控制线程一次性分配故障（operator new 覆盖；Stop 在测试线程同步执行）。
thread_local bool g_post_alloc_fault = false;
thread_local int g_post_alloc_hits = 0;
thread_local int g_alloc_fail_after = -1;
thread_local std::size_t g_alloc_attempts = 0;

// 仅控制线程在 Stop 前启用；不拦截正常接纳/handoff。
thread_local bool g_late_submit = false;
bbt::infra::net::EventLoop* g_late_loop = nullptr;
std::mutex g_late_mutex;
std::condition_variable g_late_cv;
bool g_late_release = false;
std::atomic<bool> g_late_entered{false};
std::atomic<int> g_late_submits{0}, g_late_executions{0}, g_late_joins{0};

}  // namespace

void* operator new(std::size_t size) {
    if (g_alloc_fail_after >= 0) {
        ++g_alloc_attempts;
        if (g_alloc_fail_after-- == 0) {
            g_alloc_fail_after = -1;
            throw std::bad_alloc();
        }
    }
    if (void* value = std::malloc(size ? size : 1)) { return value; }
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* value) noexcept { std::free(value); }
void operator delete(void* value, std::size_t) noexcept { std::free(value); }
void operator delete[](void* value) noexcept { std::free(value); }
void operator delete[](void* value, std::size_t) noexcept { std::free(value); }

extern "C" void* __real_aligned_alloc(std::size_t alignment, std::size_t size);
extern "C" void* __wrap_aligned_alloc(std::size_t alignment, std::size_t size) {
    if (g_post_alloc_fault) {
        g_post_alloc_fault = false;
        ++g_post_alloc_hits;
        void* frames[32];
        const int count = ::backtrace(frames, 32);
        ::backtrace_symbols_fd(frames, count, STDERR_FILENO);
        errno = ENOMEM;
        return nullptr;
    }
    return __real_aligned_alloc(alignment, size);
}

extern "C" int __real_close(int fd);

extern "C" std::error_code RealNativeSocketClose(NativeSocket* socket)
    asm("__real__ZN3bbt5infra3net12NativeSocket5CloseEv");
extern "C" std::error_code WrapNativeSocketClose(NativeSocket* socket)
    asm("__wrap__ZN3bbt5infra3net12NativeSocket5CloseEv");

// 仅对被记录的 listener fd 的前 N 次 NativeSocket::Close 注入 EBADF。
// 该边界早于 NativeSocket::Release，因此失败仍由 NativeSocket 持有，重试的
// 语义是真实 owner 重试，而不是用已 Release 的整数 fd 自救。
extern "C" std::error_code WrapNativeSocketClose(NativeSocket* socket) {
    if (socket != nullptr && socket->Fd() == g_watch_fd.load(std::memory_order_relaxed)) {
        const int ordinal = g_watch_closes.fetch_add(1, std::memory_order_relaxed) + 1;
        if (ordinal <= g_watch_reject_limit.load(std::memory_order_relaxed)) {
            return std::error_code(EBADF, std::generic_category());
        }
    }
    return RealNativeSocketClose(socket);
}

// 仅对其它 libc close 保留真实转发；rollback 注入不再位于这里。
extern "C" int __wrap_close(int fd) {
    return __real_close(fd);
}

extern "C" std::error_code RealStreamClose(bbt::infra::net::StreamConnection*, std::error_code)
    asm("__real__ZN3bbt5infra3net16StreamConnection5CloseESt10error_code");
extern "C" std::error_code WrapStreamClose(bbt::infra::net::StreamConnection*, std::error_code)
    asm("__wrap__ZN3bbt5infra3net16StreamConnection5CloseESt10error_code");
extern "C" std::error_code WrapStreamClose(bbt::infra::net::StreamConnection* connection,
    std::error_code reason) {
    g_close_calls.fetch_add(1, std::memory_order_relaxed);
    const int fault = g_close_fault.exchange(0, std::memory_order_seq_cst);
    if (fault == 1) {
        g_close_injections.fetch_add(1, std::memory_order_seq_cst);
        throw std::bad_alloc();   // Close 入口前：物理 close 未发生，状态可重试
    }
    if (fault == 3) {
        g_close_injections.fetch_add(1, std::memory_order_seq_cst);
        (void)RealStreamClose(connection, reason);   // 物理关闭已完成
        throw std::bad_alloc();   // 完整 Close 后：Closed 重放路径
    }
    return RealStreamClose(connection, reason);
}

using bbt::infra::net::EventLoop;
using bbt::infra::net::SubmitStatus;
extern "C" SubmitStatus RealSubmit(EventLoop*, std::function<void()>, std::error_code&)
    asm("__real__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEERSt10error_code");
extern "C" SubmitStatus WrapSubmit(EventLoop*, std::function<void()>, std::error_code&)
    asm("__wrap__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEERSt10error_code");
extern "C" SubmitStatus WrapSubmit(EventLoop* loop, std::function<void()> command,
    std::error_code& error) {
    if (!g_late_submit) { return RealSubmit(loop, std::move(command), error); }
    const int ordinal = ++g_late_submits;
    if (ordinal == 1) {
        g_late_loop = loop;
        command = [loop, original = std::move(command)] {
            g_late_entered.store(true);
            std::unique_lock<std::mutex> lock(g_late_mutex);
            CHECK(g_late_cv.wait_for(lock, 40s, [] { return g_late_release; }));
            lock.unlock();
            CHECK(loop->IsOwner());
            CHECK(!loop->Context().stopped()); // 真 owner/context 仍活，非标签
            CHECK(::fcntl(g_watch_fd.load(), F_GETFD) >= 0); // listener 尚未被回收
            CHECK(g_late_executions.fetch_add(1) == 0);
            original(); // 未撤销、真实清理闭包恰一次
        };
    } else {
        CHECK(ordinal == 2 && loop == g_late_loop);
        CHECK(g_late_entered.load());
        CHECK(g_late_executions.load() == 0);
    }
    const auto status = RealSubmit(loop, std::move(command), error);
    CHECK(status == SubmitStatus::Accepted && !error);
    if (ordinal == 2) {
        // 同步 Stop 调用只有首个真实 wait 超时后才能走到这里，无 sleep 猜窗口。
        { std::lock_guard<std::mutex> lock(g_late_mutex); g_late_release = true; }
        g_late_cv.notify_one();
        g_late_submit = false;
    }
    return status;
}
extern "C" bool RealJoin(EventLoop*) asm("__real__ZN3bbt5infra3net9EventLoop4JoinEv");
extern "C" bool WrapJoin(EventLoop*) asm("__wrap__ZN3bbt5infra3net9EventLoop4JoinEv");
extern "C" bool WrapJoin(EventLoop* loop) {
    const bool joined = RealJoin(loop);
    if (loop == g_late_loop) {
        CHECK(joined && g_late_executions.load() == 1);
        ++g_late_joins;
    }
    return joined;
}

// Linux fcntl(2) 第三参数保真转发（同 engine_start_failure 范式）；仅当注入窗口
// 打开且命中 F_DUPFD_CLOEXEC 时返回一次 -1/EMFILE。观测到 listener dup 时记录 fd
// 供 close 注入定位。
bool CmdTakesPointer(int cmd) {
    switch (cmd) {
    case F_GETLK:
    case F_SETLK:
    case F_SETLKW:
#ifdef F_OFD_GETLK
    case F_OFD_GETLK:
    case F_OFD_SETLK:
    case F_OFD_SETLKW:
#endif
#ifdef F_GET_RW_HINT
    case F_GET_RW_HINT:
    case F_SET_RW_HINT:
    case F_GET_FILE_RW_HINT:
    case F_SET_FILE_RW_HINT:
#endif
        return true;
    default:
        return false;
    }
}

bool CmdTakesThirdArg(int cmd) {
    switch (cmd) {
    case F_GETFD:
    case F_GETFL:
    case F_GETOWN:
    case F_GETSIG:
    case F_GETLEASE:
#ifdef F_GETPIPE_SZ
    case F_GETPIPE_SZ:
#endif
#ifdef F_GET_SEALS
    case F_GET_SEALS:
#endif
        return false;
    default:
        return true;
    }
}

extern "C" int __real_fcntl(int fd, int cmd, ...);

extern "C" int __wrap_fcntl(int fd, int cmd, ...) {
    if (cmd == F_DUPFD || cmd == F_DUPFD_CLOEXEC) {
        va_list args;
        va_start(args, cmd);
        const int arg = va_arg(args, int);
        va_end(args);
        g_watch_fd.store(fd, std::memory_order_relaxed);   // listener dup 源 fd
        if (cmd == F_DUPFD_CLOEXEC && g_dup_failure_armed.load(std::memory_order_seq_cst)) {
            int expected = 0;
            if (g_dup_injections.compare_exchange_strong(expected, 1,
                    std::memory_order_seq_cst)) {
                errno = EMFILE;   // 精确一次真实 EMFILE
                return -1;
            }
        }
        return __real_fcntl(fd, cmd, arg);
    }
    if (!CmdTakesThirdArg(cmd)) { return __real_fcntl(fd, cmd); }
    va_list args;
    va_start(args, cmd);
    int result = 0;
    if (CmdTakesPointer(cmd)) {
        result = __real_fcntl(fd, cmd, va_arg(args, void*));
    } else {
        result = __real_fcntl(fd, cmd, va_arg(args, int));
    }
    va_end(args);
    return result;
}

namespace {

// ---- 进程级观测与 TCP 助手 --------------------------------------------------

int FdCount() {
    DIR* dir = ::opendir("/proc/self/fd");
    CHECK(dir != nullptr);
    int count = 0;
    while (::readdir(dir) != nullptr) { ++count; }
    ::closedir(dir);
    return count;
}

int ThreadCount() {
    std::ifstream status("/proc/self/status");
    CHECK(status.good());
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("Threads:", 0) == 0) { return std::atoi(line.c_str() + 8); }
    }
    CHECK(false);
    return 0;
}

template <typename Predicate>
bool WaitUntil(Predicate&& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = Clock::now() + timeout;
    while (!predicate()) {
        if (Clock::now() >= deadline) { return false; }
        std::this_thread::sleep_for(2ms);
    }
    return true;
}

// 测试私有 runtime 预热（单 Processer，真实挂起让出；每进程一次）。
void WarmRuntime() {
    static const bool started = [] {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 1;
        g_bbt_coroutine_config->m_cfg_stack_size = 65536;
        g_scheduler->Start();
        return true;
    }();
    CHECK(started);
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    const std::size_t stack = g_bbt_coroutine_config->m_cfg_stack_size;
    g_scheduler->RegistCoroutineTask(stack, [promise] {
        bbt::coroutine::sync::CoWaiter waiter;
        CHECK(waiter.Wait(bbt::coroutine::WaitOptions{Clock::now() + 5ms})
            == bbt::coroutine::WaitStatus::TimedOut);
        promise->set_value();
    }, "net.engine.stop.test.warm");
    CHECK(future.wait_for(10s) == std::future_status::ready);
    future.get();
}

Address Loopback(std::uint16_t port) {
    std::error_code error;
    auto address = Address::Ip("127.0.0.1", port, error);
    CHECK(!error);
    return address;
}

Options TwoLoopOptions() {
    Options options;
    options.num_event_loop = 2;
    return options;
}

bool IsError(const std::error_code& error, std::errc condition) {
    return error == std::make_error_code(condition);
}

NativeSocket ConnectTo(const Address& bound) {
    std::error_code error;
    auto client = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    CHECK(!error && client.Fd() >= 0);
    const auto connected = client.Connect(bound);
    CHECK(!connected || connected.value() == EINPROGRESS);
    const auto deadline = Clock::now() + 5s;
    while (client.PendingError()) {
        CHECK(Clock::now() < deadline);
        std::this_thread::sleep_for(1ms);
    }
    return client;
}

void SendAll(NativeSocket& socket, const std::string& data) {
    std::size_t sent = 0;
    const auto deadline = Clock::now() + 5s;
    while (sent < data.size()) {
        const auto result = socket.Send(data.data() + sent, data.size() - sent);
        if (result.bytes > 0) { sent += result.bytes; continue; }
        if (result.error) {
            CHECK(result.error.value() == EAGAIN || result.error.value() == EWOULDBLOCK);
        }
        CHECK(Clock::now() < deadline);
        std::this_thread::sleep_for(1ms);
    }
}

std::string ReceiveAtMost(NativeSocket& socket, std::size_t expected,
    std::chrono::milliseconds timeout) {
    std::string out;
    const auto deadline = Clock::now() + timeout;
    while (out.size() < expected && Clock::now() < deadline) {
        pollfd descriptor{socket.Fd(), POLLIN, 0};
        const int ready = ::poll(&descriptor, 1, 20);
        if (ready <= 0) { continue; }
        char buffer[512];
        const auto room = std::min(sizeof(buffer), expected - out.size());
        const auto result = socket.Receive(buffer, room);
        if (result.bytes > 0) { out.append(buffer, result.bytes); }
        else if (result.eof) { break; }
    }
    return out;
}

// 对端已物理关闭（EOF/RST）：强停物理收口的客户端侧观察。
bool PeerClosed(NativeSocket& socket, std::chrono::milliseconds timeout) {
    const auto deadline = Clock::now() + timeout;
    for (;;) {
        pollfd descriptor{socket.Fd(), POLLIN | POLLHUP, 0};
        const int ready = ::poll(&descriptor, 1, 20);
        if (ready > 0) {
            char buffer[64];
            const auto result = socket.Receive(buffer, sizeof(buffer));
            if (result.eof || result.bytes == 0 || result.error) { return true; }
        }
        if (Clock::now() >= deadline) { return false; }
    }
}

// ---- handler：真实回显 + OnClose 计数 ---------------------------------------

class EchoHandler final : public EventHandler {
public:
    Action OnOpen(ICoConnection&) override { return Action::None; }

    Action OnTraffic(ICoConnection& connection, bool wake) override {
        if (wake) { return Action::None; }
        const std::string data{connection.Peek()};
        if (data.empty()) { return Action::None; }
        const auto write_error = connection.Write(data, Clock::now() + 5s);
        CHECK(!write_error);
        const auto discard_error = connection.Discard(data.size());
        CHECK(!discard_error);
        traffic_.fetch_add(1);
        return Action::None;
    }

    void OnClose(ICoConnection&, const std::error_code&) override {
        close_.fetch_add(1);
    }

    int traffic() const { return traffic_.load(); }
    int close_count() const { return close_.load(); }

private:
    std::atomic<int> traffic_{0};
    std::atomic<int> close_{0};
};

// ---- case 1/2：Stop 物理 Close 一次性故障（入口前 / 完整后） ----------------

int CaseStopCloseFault(int fault_mode) {
    WarmRuntime();
    auto handler = std::make_shared<EchoHandler>();
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    {
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), TwoLoopOptions()));
        std::vector<Address> addresses;
        CHECK(!engine.LocalAddresses(addresses));
        const auto bound = addresses[0];

        std::vector<NativeSocket> clients;
        for (int index = 0; index < 2; ++index) {
            auto client = ConnectTo(bound);
            const auto payload = "stop-fault-" + std::to_string(index);
            SendAll(client, payload);
            CHECK(ReceiveAtMost(client, payload.size(), 5s) == payload);  // 真实接纳+回显
            clients.push_back(std::move(client));
        }
        CHECK(WaitUntil([&] { return handler->traffic() >= 2; }, 8s));

        g_close_calls.store(0, std::memory_order_relaxed);
        g_close_injections.store(0, std::memory_order_relaxed);
        g_close_fault.store(fault_mode, std::memory_order_seq_cst);
        const auto stop_error = engine.Stop();   // 不得向调用方抛出
        g_close_fault.store(0, std::memory_order_seq_cst);

        // 首错保留：一次性 Close 故障 → not_enough_memory（不是假成功，不吞分类）。
        CHECK(stop_error == std::errc::not_enough_memory);
        CHECK(g_close_injections.load(std::memory_order_relaxed) == 1);   // 恰一命中
        // 有界推进一次：入口前故障 ⇒ 2 连接 + 1 重试 = 3；完整后故障 ⇒ Closed
        // 重放不再调用 Close = 2。
        const int expected_calls = fault_mode == 1 ? 3 : 2;
        CHECK(g_close_calls.load(std::memory_order_relaxed) == expected_calls);

        for (auto& client : clients) { CHECK(PeerClosed(client, 8s)); }   // EOF：物理收口
        // 强停按契约压掉未开始的 OnClose（service_stopped_ 门），handler close
        // 计数不作为 Engine 强停断言；挂起/OnClose 语义由既有 72 名单覆盖。
        CHECK(engine.CountConnections() == 0);
        std::vector<Address> after;
        CHECK(IsError(engine.LocalAddresses(after), std::errc::operation_not_permitted));
        CHECK(!engine.Stop());   // 已真实收口：Stopped 幂等成功（不假 Stopped 的正证）
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);
    std::cout << "CASE OK " << (fault_mode == 1 ? "stop_close_before_fault"
                                                : "stop_close_after_fault") << '\n';
    return 0;
}

// ---- case 3：控制线程 Submit 分配一次性故障 ---------------------------------

void CheckPostRollbackResponsibility() {
    using bbt::infra::net::EventLoop;
    using bbt::infra::net::SubmitStatus;
    EventLoop loop(32, 1);
    CHECK(loop.Start());
    std::atomic<int> executions{0};
    g_post_alloc_hits = 0;
    g_post_alloc_fault = true;
    std::error_code submit_error;
    CHECK(loop.Submit([&] { ++executions; }, submit_error) == SubmitStatus::Failed);
    CHECK(submit_error == std::errc::not_enough_memory);
    CHECK(!g_post_alloc_fault && g_post_alloc_hits == 1);
    CHECK(loop.FailureCount() == 1);
    CHECK(executions.load() == 0);
    CHECK(loop.Submit([&] { ++executions; }, submit_error) == SubmitStatus::Accepted);
    CHECK(!submit_error);
    loop.RequestStop();
    CHECK(loop.Join());
    CHECK(executions.load() == 1); // rejected command never runs; recovery runs exactly once
    CHECK(loop.Submit([] {}) == SubmitStatus::Sealed);
    std::cerr << "post rollback hit=1 rejected_executed=0 recovery_executed=1\n";
    g_post_alloc_hits = 0;
}

int CaseStopSubmitAllocFault(bool post_fault = false) {
    if (post_fault) { CheckPostRollbackResponsibility(); }
    WarmRuntime();
    auto handler = std::make_shared<EchoHandler>();
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    {
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), TwoLoopOptions()));
        std::vector<Address> addresses;
        CHECK(!engine.LocalAddresses(addresses));
        const auto bound = addresses[0];

        std::vector<NativeSocket> clients;
        for (int index = 0; index < 2; ++index) {
            auto client = ConnectTo(bound);
            const auto payload = "submit-fault-" + std::to_string(index);
            SendAll(client, payload);
            CHECK(ReceiveAtMost(client, payload.size(), 5s) == payload);
            clients.push_back(std::move(client));
        }
        CHECK(WaitUntil([&] { return handler->traffic() >= 2; }, 8s));

        if (post_fault) { g_post_alloc_fault = true; }
        else { g_alloc_fail_after = 0; } // 原有promise/闭包分配用例保持
        std::error_code stop_error{};
        bool threw = false;
        try { stop_error = engine.Stop(); }
        catch (const std::bad_alloc&) { threw = true; }
        g_alloc_fail_after = -1;
        if (post_fault) {
            CHECK(!g_post_alloc_fault);
            CHECK(g_post_alloc_hits == 1);
            std::cerr << "post allocation hit=1 error=" << stop_error.message() << '\n';
        }
        else CHECK(g_alloc_attempts >= 1);                       // 注入确有命中（非零命中假绿）
        CHECK(!threw);                                      // 异常不穿出 Stop（红差异）
        CHECK(stop_error == std::errc::not_enough_memory);  // 分类 + 首错保留

        for (auto& client : clients) { CHECK(PeerClosed(client, 8s)); }
        CHECK(engine.CountConnections() == 0);
        CHECK(!engine.Stop());
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);
    std::cout << "CASE OK stop_submit_alloc_fault" << '\n';
    return 0;
}

// ---- case 4：Rollback 关闭未收口不假 Idle -----------------------------------

int CaseRollbackCloseFaultsNotIdle() {
    WarmRuntime();
    auto handler = std::make_shared<EchoHandler>();
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    {
        Engine engine(handler);
        // 注入：ArmAccept 的 listener dup 一次 EMFILE ⇒ 进入 RollbackStart；
        // 回滚窗口中该 listener fd 的前两次真实关闭 EBADF ⇒ 有界推进一次后仍失败。
        g_dup_failure_armed.store(true, std::memory_order_seq_cst);
        g_dup_injections.store(0, std::memory_order_seq_cst);
        g_watch_fd.store(-1, std::memory_order_relaxed);
        g_watch_closes.store(0, std::memory_order_relaxed);
        g_watch_reject_limit.store(2, std::memory_order_relaxed);

        const auto start_error = engine.Run(Loopback(0), TwoLoopOptions());
        g_dup_failure_armed.store(false, std::memory_order_seq_cst);
        g_watch_reject_limit.store(0, std::memory_order_relaxed);

        CHECK(start_error.value() == EMFILE);   // 启动原错原样透传（清理首错分别保留）
        CHECK(g_dup_injections.load(std::memory_order_seq_cst) == 1);
        CHECK(g_watch_closes.load(std::memory_order_relaxed) >= 2);   // 两次关闭尝试确有命中

        // 未收口 ⇒ 不假 Idle：重试 Run 明确 busy（红差异：旧代码回落 Idle 后成功）。
        CHECK(IsError(engine.Run(Loopback(0), TwoLoopOptions()),
            std::errc::device_or_resource_busy));

        // 显式 Stop 在仍活 owner 上推进清理并真实收口。
        CHECK(!engine.Stop());
        std::vector<Address> after;
        CHECK(IsError(engine.LocalAddresses(after), std::errc::operation_not_permitted));
        CHECK(!engine.Stop());   // Stopped 幂等
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);   // listener fd 已被后续推进真实关闭（不靠泄漏回落）

    // 收尾冒烟：注入解除后的新 Engine 正常 Run/roundtrip/Stop（正常路径未被改变）。
    {
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), TwoLoopOptions()));
        std::vector<Address> addresses;
        CHECK(!engine.LocalAddresses(addresses));
        auto client = ConnectTo(addresses[0]);
        SendAll(client, "after-rollback");
        CHECK(ReceiveAtMost(client, std::strlen("after-rollback"), 5s) == "after-rollback");
        CHECK(!engine.Stop());
    }
    std::cout << "CASE OK rollback_close_faults_not_idle" << '\n';
    return 0;
}

// T1：真实 Accepted 晚到；不修改产品 deadline，不伪造旧红。
int CaseStopAcceptedLate() {
    WarmRuntime();
    const int threads = ThreadCount(), fds = FdCount();
    {
        auto handler = std::make_shared<EchoHandler>();
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), TwoLoopOptions()));
        std::vector<Address> addresses;
        CHECK(!engine.LocalAddresses(addresses));
        auto client = ConnectTo(addresses[0]);
        SendAll(client, "late-stop");
        CHECK(ReceiveAtMost(client, 9, 5s) == "late-stop");
        g_late_submit = true;
        const auto started = Clock::now();
        const auto error = engine.Stop();
        CHECK(error == std::errc::timed_out);
        CHECK(Clock::now() - started >= 15s);
        CHECK(g_late_submits.load() == 2 && g_late_executions.load() == 1);
        CHECK(g_late_joins.load() >= 1);
        g_late_loop = nullptr; // 所有 owner 已 Join/回收，不保留悬空观察目标
        CHECK(PeerClosed(client, 5s));
        CHECK(engine.CountConnections() == 0);
        CHECK(!engine.Stop());
        std::cerr << "late Accepted=2 original=1 Join=success first=timed_out EOF=1\n";
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads; }, 10s));
    CHECK(FdCount() == fds);
    std::cout << "CASE OK stop_accepted_late\n";
    return 0;
}

class DestructorHoldHandler final : public EventHandler {
public:
    Action OnOpen(ICoConnection&) override { return Action::None; }
    Action OnTraffic(ICoConnection& connection, bool wake) override {
        if (wake || connection.Peek().empty()) { return Action::None; }
        const auto view = connection.Peek();
        const std::string expected(view);
        auto shell = dynamic_cast<bbt::infra::net::CoTcpConnection&>(connection).shared_from_this();
        { std::lock_guard<std::mutex> lock(mutex); shells.push_back(shell); }
        ++entered;
        while (!released.load()) {
            bbt::coroutine::sync::CoWaiter waiter;
            (void)waiter.Wait(bbt::coroutine::WaitOptions{Clock::now() + 10ms});
        }
        CHECK(view == expected && connection.Peek() == expected);
        CHECK(connection.InboundBuffered() == expected.size());
        CHECK(connection.Write("late", Clock::now() + 1s) == std::errc::operation_canceled);
        CHECK(connection.Wake() == std::errc::operation_canceled);
        CHECK(!connection.Close(bbt::infra::net::CloseMode::Block, {}, Clock::now() + 1s));
        CHECK(!connection.Discard(expected.size()));
        ++done;
        return Action::None;
    }
    void OnClose(ICoConnection&, const std::error_code&) override { ++closed; }
    std::mutex mutex;
    std::vector<std::weak_ptr<bbt::infra::net::CoTcpConnection>> shells;
    std::atomic<int> entered{0}, done{0}, closed{0};
    std::atomic<bool> released{false};
};

// T3：Engine 最后引用直接 reset，handler/snapshot/外部 shell 全部跨析构。
int CaseDestructorCloseBeforeFault() {
    WarmRuntime();
    const int threads = ThreadCount(), fds = FdCount();
    auto handler = std::make_shared<DestructorHoldHandler>();
    auto engine = std::make_shared<Engine>(handler);
    CHECK(engine.use_count() == 1);
    CHECK(!engine->Run(Loopback(0), TwoLoopOptions()));
    std::vector<Address> addresses;
    CHECK(!engine->LocalAddresses(addresses));
    std::vector<NativeSocket> clients;
    for (int i = 0; i < 2; ++i) {
        clients.push_back(ConnectTo(addresses[0]));
        SendAll(clients.back(), "destructor-" + std::to_string(i));
    }
    CHECK(WaitUntil([&] { return handler->entered.load() == 2; }, 8s));
    std::vector<std::shared_ptr<bbt::infra::net::CoTcpConnection>> shells;
    { std::lock_guard<std::mutex> lock(handler->mutex);
      for (const auto& weak : handler->shells) { shells.push_back(weak.lock()); } }
    CHECK(shells.size() == 2 && shells[0] && shells[1]);
    const auto first_id = shells[0]->Id(), second_id = shells[1]->Id();
    g_close_calls = 0;
    g_close_injections = 0;
    g_close_fault = 1;
    engine.reset(); // 唯一 Engine 引用；之前没有显式 Stop/Close
    CHECK(g_close_fault.load() == 0 && g_close_injections.load() == 1);
    CHECK(g_close_calls.load() == 3); // 双连接加恰一次入口前重试
    CHECK(!handler->released.load() && handler->done.load() == 0);
    for (auto& client : clients) { CHECK(PeerClosed(client, 5s)); }
    CHECK(WaitUntil([&] { return ThreadCount() == threads; }, 10s));
    CHECK(FdCount() == fds + 2); // 仅两个自有客户端尚活；shell/业务仍保留
    clients.clear();
    CHECK(FdCount() == fds);
    CHECK(shells[0]->Id() == first_id && shells[1]->Id() == second_id);
    handler->released = true;
    CHECK(WaitUntil([&] { return handler->done.load() == 2; }, 10s));
    CHECK(handler->closed.load() == 0);
    std::weak_ptr<bbt::infra::net::CoTcpConnection> first_shell = shells[0], second_shell = shells[1];
    std::weak_ptr<DestructorHoldHandler> weak_handler = handler;
    shells.clear();
    handler.reset();
    CHECK(WaitUntil([&] { return first_shell.expired() && second_shell.expired()
        && weak_handler.expired(); }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds; }, 5s));
    std::cerr << "destructor injections=1 Close=3 suspended=2 EOF=2 FD/thread=baseline late_snapshot_ops=ok\n";
    std::cout << "CASE OK destructor_close_before_fault\n";
    return 0;
}

}  // namespace

#include "ProcessRuntimeTestMain.hpp"

int main(int argc, char** argv) {
    return bbt_net_test::RunTestProcessExit([&] {
        try {
            CHECK(argc == 2);
            const std::string name = argv[1];
            if (name == "stop_accepted_late") { return CaseStopAcceptedLate(); }
            if (name == "destructor_close_before_fault") { return CaseDestructorCloseBeforeFault(); }
            if (name == "stop_close_before_fault") { return CaseStopCloseFault(1); }
            if (name == "stop_close_after_fault") { return CaseStopCloseFault(3); }
            if (name == "stop_post_alloc_fault") { return CaseStopSubmitAllocFault(true); }
            if (name == "stop_submit_alloc_fault") { return CaseStopSubmitAllocFault(); }
            if (name == "rollback_close_faults_not_idle") {
                return CaseRollbackCloseFaultsNotIdle();
            }
            return 2;
        } catch (const std::exception& error) {
            std::cerr << "RUN FAILED: " << error.what() << '\n';
            return 1;
        }
    });
}
