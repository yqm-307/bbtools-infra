// 回归测试：accept4 内核成功后、Engine 接管 fd 前若被 RequestStopAccepting，
// fd 必须被物理关闭（不仅 CountConnections 为 0，真实 fd 句柄亦不可再用）。
// 通过 --wrap=syscall 在内核 accept4 返回成功但 Engine 尚未取 fd 的窗口暂停，
// 与原 accept-stop-fd-probe 同一路径，但以正式用例形式断言并 exit 0。
//
// 编译链接需加 -Wl,--wrap=syscall；本文件为唯一使用 wrapped syscall 的 TU，
// 不与其它测试共享进程，避免污染。
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/RawHandler.hpp>
#include <bbt/coroutine/coroutine.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <memory>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <netinet/in.h>
#include <thread>
#include <unistd.h>

using namespace std::chrono_literals;
namespace net = bbt::infra::net;

static std::atomic<bool> arm{false}, arrived{false}, release_accept{false};
static std::atomic<int> acquired{-1};

extern "C" long __real_syscall(long, ...);
extern "C" long __wrap_syscall(long number, ...) {
    va_list args;
    va_start(args, number);
    long result = -1;
    if (number == SYS_accept4) {
        const int fd = va_arg(args, int);
        auto* address = va_arg(args, sockaddr*);
        auto* length = va_arg(args, socklen_t*);
        const int flags = va_arg(args, int);
        result = __real_syscall(number, fd, address, length, flags);
        if (result >= 0 && arm.exchange(false)) {
            acquired = static_cast<int>(result);
            arrived = true;  // 内核已建立连接并返回 fd，Engine 尚未见到返回值
            const auto end = std::chrono::steady_clock::now() + 3s;
            while (!release_accept && std::chrono::steady_clock::now() < end) {
                timespec delay{0, 1000000};
                __real_syscall(SYS_nanosleep, &delay, nullptr);
            }
        }
    } else if (number == SYS_connect) {
        const int fd = va_arg(args, int);
        auto* address = va_arg(args, const sockaddr*);
        const auto length = va_arg(args, socklen_t);
        result = __real_syscall(number, fd, address, length);
    } else if (number == SYS_nanosleep) {
        auto* request = va_arg(args, const timespec*);
        auto* remaining = va_arg(args, timespec*);
        result = __real_syscall(number, request, remaining);
    } else {
        std::fprintf(stderr, "unexpected wrapped syscall %ld\n", number);
        std::_Exit(3);
    }
    va_end(args);
    return result;
}

template<class P> static bool Until(P predicate) {
    const auto end = std::chrono::steady_clock::now() + 3s;
    while (std::chrono::steady_clock::now() < end) {
        if (predicate()) { return true; }
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

int main() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
    g_scheduler->Start();

    net::Engine engine{std::shared_ptr<net::RawHandler>{}};
    if (engine.EnableAcceptHandoff(1)) { std::_Exit(2); }

    net::EngineConfig config;
    config.runtime_progress = net::RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    std::error_code error;
    config.listen_addresses = {net::Address::Ip("127.0.0.1", 0, error)};
    if (error) { std::_Exit(2); }

    std::error_code run_error;
    std::thread runner([&] { run_error = engine.Run(config); });

    int peer = -1;
    int result = 2;
    if (Until([&] { return engine.Phase() == net::EnginePhase::Running; })) {
        std::vector<net::Address> addresses;
        if (!engine.LocalAddresses(addresses) && !addresses.empty()) {
            peer = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
            sockaddr_in address{};
            address.sin_family = AF_INET;
            address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            address.sin_port = htons(addresses.at(0).Port());
            arm = true;
            if (!::connect(peer, reinterpret_cast<sockaddr*>(&address), sizeof(address)) &&
                Until([] { return arrived.load(); })) {
                std::printf("PHASE kernel_accept_succeeded=1 acquired_fd=%d before_engine_raii=1\n",
                            acquired.load());
                engine.RequestStopAccepting();
                release_accept = true;
                const bool stopped = Until([&] { return engine.AcceptingStopped(); });
                std::printf("PHASE accept_stopped=%d connections=%zu\n",
                            stopped, engine.CountConnections());

                // 核心断言：已 accept 但未交付的 fd 必须被物理关闭。
                const int fd = acquired.load();
                const bool fd_closed = (fd >= 0) && (::fcntl(fd, F_GETFD) < 0);
                std::printf("RESULT run_error=%d stopped=%d connections=%zu "
                            "accepted_fd_closed=%d\n",
                            run_error.value(),
                            engine.Phase() == net::EnginePhase::Stopped,
                            engine.CountConnections(),
                            fd_closed ? 1 : 0);

                result = (stopped && engine.CountConnections() == 0 && fd_closed) ? 0 : 2;
            }
        }
    }
    release_accept = true;
    engine.RequestClose();
    runner.join();
    if (peer >= 0) { ::close(peer); }
    std::fflush(nullptr);
    std::_Exit(result);
}
