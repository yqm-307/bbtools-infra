// F5 回归：公共 Engine 启动阶段 accept 装配失败的真实传播与安全回落。
//
// 范围只本包 F5（不扩其他 Engine 生命周期问题）：
//   1) case start_dup_failure：真实动态端口上精确注入 listener dup 一次 EMFILE
//      （--wrap=fcntl，只命中 F_DUPFD_CLOEXEC 且仅在测试注入窗口内，其他 fcntl
//      命令按真实第三参数类型保真转发）。断言：Run 返回真实 EMFILE（不是假成功）；
//      失败后业务未开放（public LocalAddresses 明确拒绝 + 该端口真实 TCP 连接被拒）；
//      FD/线程/连接计数回基线；解除注入后同一 Engine 实例重试真实 TCP roundtrip
//      成功且 Stop 正常。
//   2) case start_retry_roundtrip：同一可执行文件未注入时的一次真实 Run +
//      roundtrip + Stop，证明包装器不改变正常路径（注入计数 0、真实 fcntl 仍经过
//      包装器并被转发）。
//
// 红证据：修复前 ArmAcceptOwner 只 ++ 私有计数、ArmAccept 恒返回 {}，本 case 在
// 「Run 假成功」处先安全 Stop 再断言失败（不遗留线程/FD/半服务）。
//
// assign / async_wait 登记失败分支：登记发生在 main owner 线程，测试线程无法用
// 线程局部/精确命令窗口命中，故本包只做静态链接验证，未做动态窄注入，不为此新造
// 产品配置钩子（见 OUT/engine-fix-45/report.md 的未验证项）。

#include <dirent.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdarg>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/net/Action.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/EventHandler.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/Options.hpp>

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

// ---- fcntl 注入面（仅本测试可执行文件，--wrap=fcntl）----
std::atomic<bool> g_dup_failure_armed{false};
std::atomic<int> g_dup_injections{0};
std::atomic<std::size_t> g_fcntl_calls{0};

// Linux fcntl(2) 的第三参数为指针的命令（其余有参命令按 int/long 传值）。
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

// 完全不取第三参数的命令。
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

void ArmDupFailure() {
    g_dup_injections.store(0, std::memory_order_seq_cst);
    g_dup_failure_armed.store(true, std::memory_order_seq_cst);
}
void DisarmDupFailure() { g_dup_failure_armed.store(false, std::memory_order_seq_cst); }
int DupInjections() { return g_dup_injections.load(std::memory_order_seq_cst); }
std::size_t WrappedFcntlCalls() { return g_fcntl_calls.load(std::memory_order_relaxed); }

}  // namespace

extern "C" int __real_fcntl(int fd, int cmd, ...);

// 变参保真：F_DUPFD*/有参命令按真实第三参数类型取用后原样转发；无参命令不带
// 第三参数。仅当注入窗口打开且命中 F_DUPFD_CLOEXEC 时返回一次 -1/EMFILE，
// 其余命令（含窗口内的其他 dup 命令、runtime/Boost 的 fcntl）保持真实调用。
extern "C" int __wrap_fcntl(int fd, int cmd, ...) {
    g_fcntl_calls.fetch_add(1, std::memory_order_relaxed);
    if (cmd == F_DUPFD || cmd == F_DUPFD_CLOEXEC) {
        va_list args;
        va_start(args, cmd);
        const int arg = va_arg(args, int);
        va_end(args);
        if (cmd == F_DUPFD_CLOEXEC && g_dup_failure_armed.load(std::memory_order_seq_cst)) {
            int expected = 0;
            if (g_dup_injections.compare_exchange_strong(
                    expected, 1, std::memory_order_seq_cst)) {
                errno = EMFILE;   // 精确一次：真实 EMFILE，其他调用不受影响
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

// ---- 进程级观测（真实基线，不靠 sleep 推断）----
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
        if (line.rfind("Threads:", 0) == 0) {
            return std::atoi(line.c_str() + sizeof("Threads:") - 1);
        }
    }
    CHECK(false);
    return -1;
}

bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = Clock::now() + timeout;
    for (;;) {
        if (predicate()) { return true; }
        if (Clock::now() >= deadline) { return false; }
        std::this_thread::sleep_for(2ms);
    }
}

// ---- 测试私有 runtime 预热（真实受管协程，OnBoot 需要）----
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
    }, "net.engine.start_failure.warm");
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
    options.reuse_addr = true;
    return options;
}

// 真实端口预留：绑定 port 0 取内核分配端口后释放，供失败/重试用同一地址。
std::uint16_t ReservePort() {
    std::error_code error;
    auto socket = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    CHECK(!error && socket.Fd() >= 0);
    CHECK(!socket.SetReuseAddr(true));
    CHECK(!socket.Bind(Loopback(0)));
    const auto bound = socket.LocalAddress(error);
    CHECK(!error && bound.Valid());
    const auto port = bound.Port();
    CHECK(port != 0);
    CHECK(!socket.Close());
    return port;
}

NativeSocket OpenStream() {
    std::error_code error;
    auto socket = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    CHECK(!error && socket.Fd() >= 0);
    return socket;
}

// 非阻塞 connect 的真实结果：poll(POLLOUT) + SO_ERROR；空 = 已建立。
std::error_code WaitConnect(NativeSocket& socket, const Address& target) {
    const auto connected = socket.Connect(target);
    if (connected && connected.value() != EINPROGRESS) { return connected; }
    const auto deadline = Clock::now() + 5s;
    int zero_reads = 0;
    for (;;) {
        pollfd descriptor{socket.Fd(), POLLOUT, 0};
        const int ready = ::poll(&descriptor, 1, 20);
        if (ready > 0) {
            const auto pending = socket.PendingError();
            if (pending) { return pending; }
            if (++zero_reads >= 2) { return {}; }
            continue;   // 拒绝/错误由同一 POLLOUT 上的 SO_ERROR 给出，再观测一次
        }
        if (Clock::now() >= deadline) {
            return std::make_error_code(std::errc::timed_out);
        }
    }
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
        if (result.bytes > 0) { out.append(buffer, static_cast<std::size_t>(result.bytes)); }
        else if (result.eof) { break; }
    }
    return out;
}

// 最小回显 handler（只观察 F5 需要的面：boot/open/traffic/close）。
class EchoHandler final : public EventHandler {
public:
    bool OnBoot() override {
        boot_calls_.fetch_add(1);
        return true;
    }

    Action OnOpen(ICoConnection& connection) override {
        (void)connection;
        open_count_.fetch_add(1);
        return Action::None;
    }

    Action OnTraffic(ICoConnection& connection, bool wake) override {
        if (!wake) {
            const std::string data{connection.Peek()};
            if (!data.empty()) {
                traffic_.fetch_add(1);
                const auto write_error = connection.Write(data, Clock::now() + 5s);
                if (write_error) { write_errors_.fetch_add(1); }
                (void)connection.Discard(data.size());
                // 服务端主动封口（与既有 roundtrip case 同一收尾方式），
                // 避免依赖客户端先关闭时服务端 EOF 的观察时延。
                if (data == "close") { return Action::Close; }
            }
        }
        return Action::None;
    }

    void OnClose(ICoConnection& connection, const std::error_code& reason) override {
        (void)connection;
        (void)reason;
        close_count_.fetch_add(1);
    }

    int boot_calls() const { return boot_calls_.load(); }
    int open_count() const { return open_count_.load(); }
    int traffic_count() const { return traffic_.load(); }
    int close_count() const { return close_count_.load(); }
    int write_errors() const { return write_errors_.load(); }

private:
    std::atomic<int> boot_calls_{0};
    std::atomic<int> open_count_{0};
    std::atomic<int> traffic_{0};
    std::atomic<int> close_count_{0};
    std::atomic<int> write_errors_{0};
};

// case 1：dup 失败真实传播 + 安全回落 + 同实例真实重试。
int CaseStartDupFailure() {
    WarmRuntime();
    auto handler = std::make_shared<EchoHandler>();
    const auto port = ReservePort();
    const auto address = Loopback(port);
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    {
        Engine engine(handler);
        const auto calls_before = WrappedFcntlCalls();
        ArmDupFailure();
        const auto run_error = engine.Run(address, TwoLoopOptions());
        const auto injections = DupInjections();
        const auto calls_after = WrappedFcntlCalls();
        DisarmDupFailure();

        if (!run_error) {
            // 修复前行为（F5 缺陷）：ArmAcceptOwner 只 ++ 私有计数、ArmAccept 恒
            // 返回 {}，Run 假成功。先安全强停（不留线程/FD/半服务），再断言失败。
            const auto stop_error = engine.Stop();
            std::cerr << "F5 RED path: Run() reported success although listener dup was "
                         "injected with EMFILE; stop_error=" << stop_error.value() << " ("
                      << stop_error.message() << ")" << std::endl;
            CHECK(run_error == std::errc::too_many_files_open);
        }

        CHECK(injections == 1);                 // 精确命中一次注入
        CHECK(calls_after > calls_before);      // listener dup 必经本包装器
        CHECK(run_error.value() == EMFILE);     // 真实 errno
        CHECK(run_error == std::errc::too_many_files_open);
        CHECK(handler->boot_calls() == 1);      // OnBoot 已完成（ArmAccept 之前）
        CHECK(handler->open_count() == 0);      // 未开放业务
        CHECK(engine.CountConnections() == 0);

        std::vector<Address> addresses;
        addresses.push_back(Loopback(1));
        CHECK(engine.LocalAddresses(addresses) == std::errc::operation_not_permitted);
        CHECK(addresses.size() == 1 && addresses[0].Port() == 1);   // out 未被改写
        {
            // 真实 TCP 探测：失败回落之后该端口无人监听（业务未开放）。
            auto probe = OpenStream();
            const auto refused = WaitConnect(probe, address);
            CHECK(refused == std::errc::connection_refused);
            CHECK(!probe.Close());
        }
        CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
        CHECK(FdCount() == fds_before);

        // 解除注入后同一实例真实重试：真实动态 TCP roundtrip + Stop。
        CHECK(!engine.Run(address, TwoLoopOptions()));
        CHECK(handler->boot_calls() == 2);
        addresses.clear();
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1 && addresses[0].Port() == port);
        auto client = OpenStream();
        CHECK(!WaitConnect(client, address));
        SendAll(client, "f5-retry");
        const auto echoed = ReceiveAtMost(client, 8, 5s);
        CHECK(echoed == "f5-retry");
        CHECK(WaitUntil([&] { return handler->traffic_count() >= 1; }, 8s));
        CHECK(WaitUntil([&] { return engine.CountConnections() == 1; }, 8s));
        SendAll(client, "close");
        CHECK(WaitUntil([&] { return handler->close_count() >= 1; }, 10s));
        CHECK(WaitUntil([&] { return engine.CountConnections() == 0; }, 8s));
        CHECK(!client.Close());
        CHECK(!engine.Stop());
        CHECK(!engine.Stop());                  // 幂等
        CHECK(engine.CountConnections() == 0);
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);
    CHECK(handler->write_errors() == 0);
    std::cout << "start_dup_failure: injections=1 duplicate_errno=EMFILE "
                 "business_not_open=1 retry_roundtrip=ok stop=ok fd_thread_baseline=restored"
              << std::endl;
    return 0;
}

// case 2：未注入时同一包装器下的完整正常路径（其他 fcntl 命令保真）。
int CaseStartRetryRoundtrip() {
    WarmRuntime();
    auto handler = std::make_shared<EchoHandler>();
    const auto port = ReservePort();
    const auto address = Loopback(port);
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    {
        const auto calls_before = WrappedFcntlCalls();
        Engine engine(handler);
        CHECK(!engine.Run(address, TwoLoopOptions()));
        CHECK(DupInjections() == 0);
        CHECK(WrappedFcntlCalls() > calls_before);   // 真实 dup 经过包装器并被转发
        std::vector<Address> addresses;
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1 && addresses[0].Port() == port);
        auto client = OpenStream();
        CHECK(!WaitConnect(client, address));
        SendAll(client, "f5-plain");
        const auto echoed = ReceiveAtMost(client, 8, 5s);
        CHECK(echoed == "f5-plain");
        CHECK(WaitUntil([&] { return engine.CountConnections() == 1; }, 8s));
        SendAll(client, "close");
        CHECK(WaitUntil([&] { return handler->close_count() >= 1; }, 10s));
        CHECK(WaitUntil([&] { return engine.CountConnections() == 0; }, 8s));
        CHECK(!client.Close());
        CHECK(!engine.Stop());
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);
    std::cout << "start_retry_roundtrip: run=ok roundtrip=ok stop=ok "
                 "injections=0 fcntl_forwarded=1 fd_thread_baseline=restored" << std::endl;
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    const std::string name = argc > 1 ? argv[1] : std::string{};
    return bbt_net_test::RunTestProcessExit([&name]() -> int {
        if (name == "start_dup_failure") { return CaseStartDupFailure(); }
        if (name == "start_retry_roundtrip") { return CaseStartRetryRoundtrip(); }
        std::cerr << "unknown case: " << name << std::endl;
        return 2;
    });
}
