#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

// R1 受控交错回归（NO_LOOP 自驱动边界）：
//
// 契约：TransportRuntime::Close() 返回当刻必须已物理关闭全部受管资源；
// 并发/重复 Close 返回当刻都观察 Closed。
//
// R1 风险假设：TransportRuntimeImpl::Close 在封口、唤醒挂起的受管 DialTCP 后
// 直接 m_pending_cv.wait(m_pending_factories == 0)。若 DialTCP 被唤醒后（在
// NO_LOOP/手动 Tick 调度模式下）必须依赖「同一个调用方继续驱动 poller」才能
// 走到 EndFactory，则当 Close 就运行在该控制线程上时会自等待。
//
// 本用例用 SCHE_START_OPT_SCHE_NO_LOOP 启动运行时，且**全程不调用任何
// LoopOnce/Tick**：只创建 processer 线程，事件循环不自驱。用例构造一个确定
// 在途的 connect（loopback listener 的 accept 队列被占满后，后续 connect 的
// SYN 被丢弃、停在 EINPROGRESS，完全不依赖外部网络），让受管 DialTCP 真实
// 挂起在 connect 等待段；随后由另一个线程调用 Close，本线程不再驱动任何
// 东西。断言 Close 返回、owner Closed、pending factories=0、inflight=0、
// 受管 transport 名额=0，且被唤醒的 dial 的 socket fd 已物理关闭。
//
// 关键机制证据（上游 bbtools-coroutine）：
//   owner Close -> CloseWaiters::CloseAndWakeAll -> CoWaiter::Notify
//   -> CoPoller::NotifyCustomEvent -> CoPollEvent::Trigger(POLL_EVENT_CUSTOM)
//   ->（PARKED）_Complete 同步回调 -> Coroutine::OnCoPollEvent
//   -> Scheduler::OnActiveCoroutine（入全局队列）
//   -> Processer::_Run 线程出队并 Resume（NO_LOOP 下 processer 线程由
//      Scheduler::Start 的 _CreateProcessers 创建并常驻）。
// 唤醒→恢复不经过 PollOnce/LoopOnce，也无需 Close 调用方继续 Tick，因此
// 「唤醒挂起 DialTCP 后等待在途工厂归零」不会自等待。

#include <atomic>
#include <chrono>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>
#include <bbt/infra/CoTCP.hpp>
#include <bbt/infra/TransportRuntime.hpp>

#include "detail/TransportWiring.hpp"   // src 内部装配面：R1 探针与 dial 等待入口 gate

using bbt::coroutine::SCHE_START_OPT_SCHE_NO_LOOP;
using bbt::infra::CallOptions;
using bbt::infra::ErrorCode;
using bbt::infra::NetworkLimits;
using bbt::infra::TcpEndpoint;
using bbt::infra::TransportRuntime;
using bbt::infra::detail::TransportWiring;

namespace {

// NO_LOOP：只创建常驻 processer 线程，事件循环必须由调用方手动 LoopOnce 驱动。
// 本文件刻意不调用任何 LoopOnce/Tick——存在的唯一 poller 驱动能力就是被测的
// Close 调用方，而它不该被需要。
void EnsureRuntimeNoLoop() {
    static const bool initialized = [] {
        auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
        if (!scheduler->IsInitialized())
            scheduler->Start(SCHE_START_OPT_SCHE_NO_LOOP);
        return scheduler->IsInitialized();
    }();
    BOOST_REQUIRE(initialized);
}

NetworkLimits Limits(std::size_t max_connections, std::size_t max_inflight) {
    NetworkLimits limits{};
    limits.max_connections  = max_connections;
    limits.max_inflight     = max_inflight;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = 64 * 1024;
    limits.incoming_timeout = std::chrono::milliseconds{3000};
    return limits;
}

CallOptions Options(int timeout_ms) {
    CallOptions options;
    options.deadline = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(timeout_ms);
    return options;
}

// 进程内打开的 socket fd 个数：用于断言在途 dial 的 socket 在 Close 返回后
// 已物理关闭（_DialTcp 的错误返回路径 ::close(fd)）。
std::size_t CountOpenSockets() {
    DIR* dir = ::opendir("/proc/self/fd");
    if (dir == nullptr)
        return 0;
    std::size_t count = 0;
    char target[PATH_MAX];
    while (dirent* entry = ::readdir(dir)) {
        const std::string name = entry->d_name;
        if (name == "." || name == "..")
            continue;
        const std::string path = "/proc/self/fd/" + name;
        const ssize_t len = ::readlink(path.c_str(), target, sizeof(target) - 1);
        if (len <= 0)
            continue;
        target[len] = '\0';
        if (std::string(target).rfind("socket:[", 0) == 0)
            ++count;
    }
    ::closedir(dir);
    return count;
}

// 合成「确定在途的 connect」目标：loopback listener(backlog=1) 的 accept 队列
// 被「已握手但不 accept」的连接占满后，后续 connect 的 SYN 被丢弃、停在
// EINPROGRESS。该挂起窗口只依赖内核 backlog 语义，不依赖任何外部网络，可复现。
struct SaturatedListener {
    int listen_fd{-1};
    std::vector<int> held;   // 保留的已握手连接（不 accept，用于占满队列）

    ~SaturatedListener() {
        for (int fd : held)
            if (fd >= 0) ::close(fd);
        if (listen_fd >= 0) ::close(listen_fd);
    }
};

bool MakeSaturatedListener(SaturatedListener& out, std::uint16_t& port) {
    const int srv = ::socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0)
        return false;
    int on = 1;
    ::setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = ::htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;
    if (::bind(srv, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
        ::listen(srv, 1) != 0) {
        ::close(srv);
        return false;
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(srv, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(srv);
        return false;
    }
    port = ::ntohs(addr.sin_port);
    out.listen_fd = srv;
    // 非阻塞 connect 连续填充：能完成握手进入 accept 队列的保留，其余（SYN 被
    // 丢弃、停在 EINPROGRESS）关闭。队列容量满后后续 connect 必然停在途。
    for (int i = 0; i < 16; ++i) {
        const int c = ::socket(AF_INET, SOCK_STREAM, 0);
        if (c < 0)
            break;
        const int flags = ::fcntl(c, F_GETFL, 0);
        ::fcntl(c, F_SETFL, flags | O_NONBLOCK);
        const int rc = ::connect(c, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
        if (rc != 0 && errno != EINPROGRESS) {
            ::close(c);
            continue;
        }
        if (rc != 0) {
            fd_set writable;
            FD_ZERO(&writable);
            FD_SET(c, &writable);
            timeval tv{0, 200 * 1000};
            if (::select(c + 1, nullptr, &writable, nullptr, &tv) <= 0) {
                ::close(c);   // 仍停在途：SYN 被丢弃，正是我们想制造的饱和
                continue;
            }
            int err = 0;
            socklen_t el = sizeof(err);
            if (::getsockopt(c, SOL_SOCKET, SO_ERROR, &err, &el) != 0 || err != 0) {
                ::close(c);
                continue;
            }
        }
        out.held.push_back(c);
    }
    return !out.held.empty();
}

} // namespace

BOOST_AUTO_TEST_SUITE(transport_runtime_no_loop)

BOOST_AUTO_TEST_CASE(t_close_unblocks_parked_dial_without_driver_tick) {
    EnsureRuntimeNoLoop();

    // 确定在途的 connect 目标。
    std::uint16_t port = 0;
    SaturatedListener target;
    BOOST_REQUIRE_MESSAGE(MakeSaturatedListener(target, port),
                          "无法构造确定在途的 connect 目标（accept 队列未饱和）");
    BOOST_REQUIRE_NE(port, 0);

    auto created = TransportRuntime::Create(Limits(4, 4));
    BOOST_REQUIRE(created);
    auto owner = std::move(created).value();
    BOOST_REQUIRE(owner);
    BOOST_REQUIRE(owner->Start());

    // 接缝：dial 协程在 connect 等待段挂起、且已登记进 owner 级 close_waiters
    // 之后触发一次（numeric 地址不走 DNS，gate 只作 connect_on_registered）。
    auto parked = std::make_shared<bbt::core::thread::CountDownLatch>(1);
    TransportWiring::SetDialWaitEntryGateForTest(*owner, [parked] { parked->Down(); });

    auto dial_done = std::make_shared<bbt::core::thread::CountDownLatch>(1);
    auto dial_code = std::make_shared<std::atomic_int>(-1);

    bbtco [owner, port, dial_done, dial_code]() {
        auto dialed = owner->DialTCP(TcpEndpoint{"127.0.0.1", port}, Options(30000));
        dial_code->store(dialed ? -2 : static_cast<int>(dialed.error().code));
        dial_done->Down();
    };

    // dial 已挂起在 connect 等待段：owner 记着 1 个在途工厂 + 1 个在途名额。
    BOOST_REQUIRE_MESSAGE(parked->WaitTimeout(5000) == 0,
        "dial 未在预算内进入 connect 等待段（合成在途 connect 未成立）");
    BOOST_TEST_MESSAGE("R1: dial parked in connect wait; pending_factories="
                       << TransportWiring::PendingFactoriesForTest(*owner)
                       << " inflight="
                       << TransportWiring::InflightQuotaHeldForTest(*owner));
    BOOST_CHECK_EQUAL(TransportWiring::PendingFactoriesForTest(*owner), 1u);
    BOOST_CHECK_EQUAL(TransportWiring::InflightQuotaHeldForTest(*owner), 1u);

    const std::size_t sockets_before = CountOpenSockets();

    // 另一个线程调用 Close。本线程（NO_LOOP 下唯一的 poller 驱动能力）在挂起
    // dial 之后就绪后不再调用任何 LoopOnce/Tick：唯一能把在途 dial 推过
    // EndFactory 的机制只能是 Close 的 close_waiters 唤醒本身。
    auto close_returned   = std::make_shared<bbt::core::thread::CountDownLatch>(1);
    auto closed_at_return = std::make_shared<std::atomic_int>(-1);
    std::thread closer([owner, close_returned, closed_at_return] {
        owner->Close();
        closed_at_return->store(owner->IsClosed() ? 1 : 0);
        close_returned->Down();
    });

    const bool returned = close_returned->WaitTimeout(8000) == 0;
    if (!returned) {
        // Close 自等待：join 会挂住本线程；脱离后失败，进程退出回收。
        closer.detach();
        BOOST_FAIL("Close 未在预算内返回，疑似自等待（pending factories="
                   << TransportWiring::PendingFactoriesForTest(*owner)
                   << " inflight="
                   << TransportWiring::InflightQuotaHeldForTest(*owner) << "）");
    }
    closer.join();

    // Close 返回当刻已物理收口：owner Closed、在途工厂/dev 名额归零。
    BOOST_CHECK_EQUAL(closed_at_return->load(), 1);
    BOOST_CHECK(owner->IsClosed());
    BOOST_CHECK_EQUAL(TransportWiring::PendingFactoriesForTest(*owner), 0u);
    BOOST_CHECK_EQUAL(TransportWiring::InflightQuotaHeldForTest(*owner), 0u);
    BOOST_CHECK_EQUAL(TransportWiring::TransportsHeldForTest(*owner), 0u);

    // 被唤醒的 dial 走正常返回路径（close_waiters 唤醒 -> Completed -> Closed），
    // 未交付任何 child。
    BOOST_REQUIRE_MESSAGE(dial_done->WaitTimeout(5000) == 0, "被唤醒的 dial 未落定");
    BOOST_CHECK_EQUAL(dial_code->load(), static_cast<int>(ErrorCode::Closed));

    // 在途 dial 的 socket fd 在 Close 返回后已物理关闭（_DialTcp 错误路径
    // ::close(fd)）：打开的 socket 数必须减少。
    const std::size_t sockets_after = CountOpenSockets();
    BOOST_CHECK_MESSAGE(sockets_after < sockets_before,
        "在途 dial 的 socket fd 未物理关闭（before=" << sockets_before
        << " after=" << sockets_after << "）");

    TransportWiring::SetDialWaitEntryGateForTest(*owner, {});
}

BOOST_AUTO_TEST_SUITE_END()
