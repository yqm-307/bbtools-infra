#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <memory>
#include <thread>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>
#include <bbt/infra/CoTCP.hpp>
#include <bbt/infra/CoUDP.hpp>

// P4-C：装配接缝（ClosedHook / 测试 gate / fd 探针）已移出公共头，位于
// src 内部装配面；本目标经 PRIVATE include 直接调用它们。
#include "detail/TransportWiring.hpp"

using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;
using bbt::infra::CallOptions;
using bbt::infra::MutableBytes;
using bbt::infra::SocketAddress;
using bbt::infra::tcp::CoTCP;
using bbt::infra::tcp::CoTCPListener;
using bbt::infra::udp::CoUDP;

// 内部装配面（src/detail/TransportWiring.hpp）：对象级硬停转发器（对象入口是
// private + friend，普通消费者不可调用）与 fd/gate 接缝。
// 用类型别名而非 namespace 别名——接缝是 TransportWiring 的静态成员。
using W = bbt::infra::detail::TransportWiring;

namespace {

CallOptions Options(int timeout_ms = 30000) {
    CallOptions options;
    options.deadline = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(timeout_ms);
    return options;
}

SocketAddress Loopback(std::uint16_t port) {
    SocketAddress addr;
    addr.ip   = "127.0.0.1";
    addr.port = port;
    return addr;
}

bool FdOpen(int fd) { return fd >= 0 && ::fcntl(fd, F_GETFL) != -1; }

// 关闭断言：返回 (fcntl 返回值, errno)——两者必须同点取样，见 A1。
struct FdProbe { int rc; int err; };
FdProbe ProbeFd(int fd) {
    errno = 0;
    const int rc = ::fcntl(fd, F_GETFL);
    return FdProbe{rc, errno};
}

// CoTCPListener 无 NativeFdForTest 接缝：用 getsockname 端口匹配在 fd 表里
// 定位监听 fd（只有监听 socket 在该端口上绑定）。
int FindSocketFdByPort(std::uint16_t port) {
    for (int fd = 3; fd < 4096; ++fd) {
        sockaddr_storage ss{};
        socklen_t len = sizeof(ss);
        if (::getsockname(fd, reinterpret_cast<sockaddr*>(&ss), &len) != 0)
            continue;
        if (ss.ss_family == AF_INET) {
            const auto* v4 = reinterpret_cast<const sockaddr_in*>(&ss);
            if (ntohs(v4->sin_port) == port) return fd;
        } else if (ss.ss_family == AF_INET6) {
            const auto* v6 = reinterpret_cast<const sockaddr_in6*>(&ss);
            if (ntohs(v6->sin6_port) == port) return fd;
        }
    }
    return -1;
}

int ConnectRaw(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    if (::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1) { ::close(fd); return -1; }
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

/* 协程闭包在硬停下不会被析构（Scheduler::Stop 不展开协程栈），因此被捕获的
 * 状态必须堆持有：栈上对象随用例结束销毁会留下悬垂引用。 */
struct HardStopCtx {
    bbt::core::thread::CountDownLatch entered{1};   // 等待循环入口（wait-entry gate）
    bbt::core::thread::CountDownLatch parked{1};    // 真实挂起（io-wait 注册 gate，仅 TCP 有）
    std::atomic_bool op_returned{false};            // 哨兵：op 返回即置真
    std::atomic_int  observed_fd{-1};
    std::shared_ptr<std::atomic_int> hook_count = std::make_shared<std::atomic_int>(0);
    CoTCP::SPtr         transport;
    CoTCPListener::SPtr listener;
    CoUDP::SPtr         udp;
};

} // namespace

/* ---------------------------------------------------------------------------
 * A2/A1/A3：受管 TCP 连接上挂起 ReadSome → 取消式硬停 → owner 强制物理关闭。
 * 判据来源：compiled-task.md 验收要求 + p4-lifecycle-research §9（A1/A2/A3）。
 * ------------------------------------------------------------------------- */
BOOST_AUTO_TEST_CASE(t_tcp_accepted_hardstop_force_close) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto ctx = std::make_shared<HardStopCtx>();
    auto listener = CoTCPListener::ListenTCP("127.0.0.1", 0);
    BOOST_REQUIRE(listener);
    ctx->listener = listener.value();
    const auto port = listener.value()->LocalAddress().port;
    BOOST_REQUIRE(port != 0);

    const int raw = ConnectRaw(port);
    BOOST_REQUIRE(raw >= 0);

    bbtco [ctx]() {
        auto accepted = ctx->listener->Accept(Options());
        if (!accepted) return;
        ctx->transport = accepted.value();
        auto hooks = ctx->hook_count;
        W::SetClosedHook(*ctx->transport, [hooks]() { hooks->fetch_add(1); });
        // F3 接缝：证明协程真实挂起在 fd 等待（事件已注册），而非仅进入循环。
        W::SetIoWaitRegisteredGateForTest(*ctx->transport, [ctx]() { ctx->parked.Down(); });
        ctx->observed_fd.store(W::NativeFdForTest(*ctx->transport));
        char buf[4]{};
        auto read = ctx->transport->ReadSome(MutableBytes{buf, sizeof(buf)}, Options());
        (void)read;
        ctx->op_returned.store(true);   // 哨兵：硬停后必须保持 false
    };

    BOOST_REQUIRE_EQUAL(ctx->parked.WaitTimeout(10000), 0);
    const int fd = ctx->observed_fd.load();
    BOOST_REQUIRE(fd >= 0);
    BOOST_CHECK(FdOpen(fd));                              // 硬停前 fd 开
    BOOST_CHECK(!ctx->transport->IsClosed());
    BOOST_CHECK_EQUAL(ctx->hook_count->load(), 0);

    scheduler->Stop();   // 取消式硬停：不复活执行、不展开挂起协程栈

    BOOST_CHECK_EQUAL(ctx->op_returned.load(), false);    // A2：_EndIo 从未执行
    BOOST_CHECK(FdOpen(fd));                              // 缺口复现：fd 仍开（基线行为）
    BOOST_CHECK(!ctx->transport->IsClosed());
    BOOST_CHECK_EQUAL(ctx->hook_count->load(), 0);        // closed_hook 未发布

    BOOST_CHECK(W::ForceCloseAfterQuiescence(*ctx->transport));  // A1（private 入口经装配面）
    const FdProbe after = ProbeFd(fd);
    BOOST_CHECK_EQUAL(after.rc, -1);
    BOOST_CHECK_EQUAL(after.err, EBADF);
    BOOST_CHECK(ctx->transport->IsClosed());
    BOOST_CHECK_EQUAL(ctx->hook_count->load(), 1);        // A3：恰好一次

    BOOST_CHECK(!W::ForceCloseAfterQuiescence(*ctx->transport));  // 幂等
    BOOST_CHECK_EQUAL(ctx->hook_count->load(), 1);

    // A3 FD 复用：新 socket 复用刚释放的 fd 号后，再次强制关闭与 owner 释放
    // 都不得 close 该号（victim 必须存活、hook 计数不变）。
    const int victim = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(victim >= 0);
    BOOST_TEST_MESSAGE("forced-closed fd=" << fd << " next-fd=" << victim
                       << (victim == fd ? " [同号复用]" : " [未复用]"));
    BOOST_CHECK(!W::ForceCloseAfterQuiescence(*ctx->transport));
    ctx->transport.reset();
    BOOST_CHECK(FdOpen(victim));
    BOOST_CHECK_EQUAL(ctx->hook_count->load(), 1);

    ::close(victim);
    ::close(raw);
    ctx->listener->RequestClose();
}

/* ---------------------------------------------------------------------------
 * 监听 socket：Accept 挂起 → 硬停 → 强制关闭（覆盖 ~CoTCPListener 只调受门控
 * RequestClose 的不对称，p4-lifecycle-research §2.3 / U5）。
 * CoTCPListener 无 io-wait 注册接缝，挂起证明弱于上一用例：wait-entry gate
 * 触发 + 短等待后断言 op 未返回（非确定性挂起证明，已如实标注）。
 * ------------------------------------------------------------------------- */
BOOST_AUTO_TEST_CASE(t_listener_hardstop_force_close) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto ctx = std::make_shared<HardStopCtx>();
    auto listener = CoTCPListener::ListenTCP("127.0.0.1", 0);
    BOOST_REQUIRE(listener);
    ctx->listener = listener.value();
    const int fd = FindSocketFdByPort(listener.value()->LocalAddress().port);
    BOOST_REQUIRE(fd >= 0);
    BOOST_CHECK(FdOpen(fd));

    bbtco [ctx]() {
        auto hooks = ctx->hook_count;
        W::SetClosedHook(*ctx->listener, [hooks]() { hooks->fetch_add(1); });
        W::SetWaitEntryGateForTest(*ctx->listener, [ctx]() { ctx->entered.Down(); });
        auto accepted = ctx->listener->Accept(Options());
        (void)accepted;
        ctx->op_returned.store(true);
    };

    BOOST_REQUIRE_EQUAL(ctx->entered.WaitTimeout(10000), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));  // 进等待
    BOOST_CHECK_EQUAL(ctx->op_returned.load(), false);

    scheduler->Stop();

    BOOST_CHECK_EQUAL(ctx->op_returned.load(), false);    // A2
    BOOST_CHECK(FdOpen(fd));                              // 缺口：监听 fd 仍开
    BOOST_CHECK_EQUAL(ctx->hook_count->load(), 0);

    BOOST_CHECK(W::ForceCloseAfterQuiescence(*ctx->listener));
    const FdProbe after = ProbeFd(fd);
    BOOST_CHECK_EQUAL(after.rc, -1);
    BOOST_CHECK_EQUAL(after.err, EBADF);
    BOOST_CHECK(ctx->listener->IsClosed());
    BOOST_CHECK_EQUAL(ctx->hook_count->load(), 1);
    BOOST_CHECK(!W::ForceCloseAfterQuiescence(*ctx->listener));
    BOOST_CHECK_EQUAL(ctx->hook_count->load(), 1);
}

/* ---------------------------------------------------------------------------
 * UDP：Receive 挂起 → 硬停 → 强制关闭（同一共享根因的第三对象）。
 * 同上一用例：无 io-wait 注册接缝，挂起证明为 gate + 短等待。
 * ------------------------------------------------------------------------- */
BOOST_AUTO_TEST_CASE(t_udp_hardstop_force_close) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto ctx = std::make_shared<HardStopCtx>();
    auto bound = CoUDP::BindUDP(Loopback(0));
    BOOST_REQUIRE(bound);
    ctx->udp = bound.value();
    char scratch[8]{};
    auto local = ctx->udp->LocalAddress();
    BOOST_REQUIRE(!local.ip.empty());

    bbtco [ctx]() {
        auto hooks = ctx->hook_count;
        W::SetClosedHook(*ctx->udp, [hooks]() { hooks->fetch_add(1); });
        W::SetWaitEntryGateForTest(*ctx->udp, [ctx]() { ctx->entered.Down(); });
        char buf[4]{};
        auto recv = ctx->udp->Receive(MutableBytes{buf, sizeof(buf)}, Options());
        (void)recv;
        ctx->op_returned.store(true);
    };
    (void)scratch;

    BOOST_REQUIRE_EQUAL(ctx->entered.WaitTimeout(10000), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    BOOST_CHECK_EQUAL(ctx->op_returned.load(), false);

    scheduler->Stop();

    BOOST_CHECK_EQUAL(ctx->op_returned.load(), false);    // A2
    BOOST_CHECK_EQUAL(ctx->hook_count->load(), 0);

    BOOST_CHECK(W::ForceCloseAfterQuiescence(*ctx->udp));
    BOOST_CHECK(ctx->udp->IsClosed());
    BOOST_CHECK_EQUAL(ctx->hook_count->load(), 1);
    BOOST_CHECK(!W::ForceCloseAfterQuiescence(*ctx->udp));
    BOOST_CHECK_EQUAL(ctx->hook_count->load(), 1);
    BOOST_TEST_MESSAGE("CoUDP 无 NativeFdForTest 接缝：本用例只断言 IsClosed+hook 一次性");
}
