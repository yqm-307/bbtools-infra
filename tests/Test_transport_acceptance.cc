#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

// Issue #31 收尾验收：两项此前没有直接证据的确定性用例。
//
//   A. RequestClose 紧邻 Scheduler::Stop 的资源收口
//      —— 现有 tcp.loopback/transport.runtime 的 RequestClose 用例都先等 op
//      落定再 Stop；hardstop.* 用例则是不经 RequestClose 直接 Stop。二者之间
//      「已 RequestClose（owner 已封口）但受管对象未落定就 Stop」这一状态没有
//      用例覆盖（src/transport/TransportRuntime.cc 的 ForceCloseAfterQuiescence
//      注释声称该状态下同样能收口，此前无测试）。
//   B. DNS 在途到期与晚到结果
//      —— 现有 deadline 用例用的都是「调用前已过期」的 deadline
//      （Test_tcp_runtime_factory::t_dial_hostname_deadline_bounded、
//      t_max_inflight_dial_cancel_and_deadline_release 的 deadline 段），
//      不是「DNS 已在途后到期」。本用例用 DNS worker 扣持接缝构造真正在途的
//      解析等待，再让 deadline 到期；对 worker 晚到的解析结果只做间接验证
//      （名额不重复归还、DNS 通道仍可用），不直接观察 worker 侧丢弃分支。
//
// 断言口径（不伪造结论）：A 用例不假设「被唤醒的 op 一定没来得及恢复执行」
// ——该竞态不由本接缝确定性控制，因此 A 断言的是收口不变量（Stop 后经 owner
// 硬停入口，fd/容量/落定全部收敛且幂等），竞态落点只记录不断言。
// B 用例的 worker 扣持接缝只保证「放行前解析等待确定在途」；放行之后 worker
// 晚到结果是否被丢弃没有外部可观察面，故 B 不断言该丢弃分支被走到，只断言
// 名额不重复归还与 DNS 通道仍可用。

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
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
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/TransportRuntime.hpp>

#include "detail/TransportWiring.hpp"     // src 内部装配面：探针 + 等待段接缝
#include "http/NetworkRuntimeImpl.hpp"    // 协议 owner 的 dial 等待入口接缝

using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;
using bbt::infra::CallOptions;
using bbt::infra::MutableBytes;
using bbt::infra::NetworkLimits;
using bbt::infra::NetworkRuntime;
using bbt::infra::SocketAddress;
using bbt::infra::TcpEndpoint;
using bbt::infra::TransportRuntime;
using bbt::infra::tcp::CoTCP;
using bbt::infra::tcp::CoTCPListener;
using bbt::infra::udp::CoUDP;

using W = bbt::infra::detail::TransportWiring;
using bbt::infra::http_detail::NetworkRuntimeImpl;

namespace {

CallOptions Options(int timeout_ms = 30000) {
    CallOptions options;
    options.deadline = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(timeout_ms);
    return options;
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

bool FdOpen(int fd) { return fd >= 0 && ::fcntl(fd, F_GETFL) != -1; }

struct FdProbe { int rc; int err; };
FdProbe ProbeFd(int fd) {
    errno = 0;
    const int rc = ::fcntl(fd, F_GETFL);
    return FdProbe{rc, errno};
}

// 受管 listener 无 fd 接缝：用 getsockname 端口匹配在 fd 表里定位监听 fd。
int FindSocketFdByPort(std::uint16_t port) {
    for (int fd = 3; fd < 4096; ++fd) {
        sockaddr_storage ss{};
        socklen_t len = sizeof(ss);
        if (::getsockname(fd, reinterpret_cast<sockaddr*>(&ss), &len) != 0)
            continue;
        if (ss.ss_family == AF_INET) {
            const auto* v4 = reinterpret_cast<const sockaddr_in*>(&ss);
            if (ntohs(v4->sin_port) == port) return fd;
        }
    }
    return -1;
}

// 测试自有的普通 listening socket：作为受管 DialTCP 的对端。
int MakeRawListener(std::uint16_t& port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int on = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = 0;
    if (::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1 ||
        ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
        ::listen(fd, 8) != 0) {
        ::close(fd);
        return -1;
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(fd);
        return -1;
    }
    port = ntohs(addr.sin_port);
    return fd;
}

// 断言失败也必须停掉调度器：残留运行中的调度线程会让进程不退出。
struct SchedulerStopGuard {
    bbt::coroutine::detail::Scheduler* scheduler;
    explicit SchedulerStopGuard(bbt::coroutine::detail::Scheduler* s)
        : scheduler(s) {}
    ~SchedulerStopGuard() {
        if (scheduler->IsRunning())
            scheduler->Stop();
    }
};

// 硬停不展开协程栈：被捕获状态必须堆持有。
struct ParkCtx {
    bbt::core::thread::CountDownLatch listener_entered{1};
    bbt::core::thread::CountDownLatch udp_entered{1};
    bbt::core::thread::CountDownLatch conn_parked{1};
    std::atomic_bool op_returned{false};
    std::atomic_int  conn_fd{-1};
    CoTCP::SPtr         conn;
    CoTCPListener::SPtr listener;
    CoUDP::SPtr         udp;
    std::uint16_t       listen_port{0};
    std::uint16_t       raw_port{0};
};

// 三种受管对象各挂一个在途 op（与 Test_hardstop_owner_convergence 同形）。
template <class Owner>
void ParkThreeOps(const std::shared_ptr<Owner>& owner,
                  const std::shared_ptr<ParkCtx>& ctx) {
    auto listener = owner->ListenTCP(SocketAddress{"127.0.0.1", 0}, 8);
    BOOST_REQUIRE(listener);
    ctx->listener = listener.value();
    ctx->listen_port = ctx->listener->LocalAddress().port;
    BOOST_REQUIRE(ctx->listen_port != 0);

    auto udp = owner->BindUDP(SocketAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(udp);
    ctx->udp = udp.value();

    bbtco [ctx]() {
        W::SetWaitEntryGateForTest(*ctx->listener,
                                  [ctx]() { ctx->listener_entered.Down(); });
        auto accepted = ctx->listener->Accept(Options());
        (void)accepted;
        ctx->op_returned.store(true);
    };
    bbtco [ctx]() {
        W::SetWaitEntryGateForTest(*ctx->udp,
                                  [ctx]() { ctx->udp_entered.Down(); });
        char buf[4]{};
        auto recv = ctx->udp->Receive(MutableBytes{buf, sizeof(buf)}, Options());
        (void)recv;
        ctx->op_returned.store(true);
    };
    bbtco [owner, ctx]() {
        auto dialed = owner->DialTCP(TcpEndpoint{"127.0.0.1", ctx->raw_port},
                                     Options());
        if (!dialed) return;
        ctx->conn = dialed.value();
        W::SetIoWaitRegisteredGateForTest(*ctx->conn,
                                         [ctx]() { ctx->conn_parked.Down(); });
        ctx->conn_fd.store(W::NativeFdForTest(*ctx->conn));
        char buf[4]{};
        auto read = ctx->conn->ReadSome(MutableBytes{buf, sizeof(buf)},
                                        Options());
        (void)read;
        ctx->op_returned.store(true);
    };
}

void WaitParked(const std::shared_ptr<ParkCtx>& ctx) {
    BOOST_REQUIRE_EQUAL(ctx->listener_entered.WaitTimeout(10000), 0);
    BOOST_REQUIRE_EQUAL(ctx->udp_entered.WaitTimeout(10000), 0);
    BOOST_REQUIRE_EQUAL(ctx->conn_parked.WaitTimeout(10000), 0);
}

// DNS worker 扣持：gate 在 worker 侧（getaddrinfo 之前）执行，扣住即证明
// 「调用方协程已挂起在 DNS 等待」且该等待不可能自行完成。
struct DnsHold {
    bbt::core::thread::CountDownLatch hold{1};
    std::atomic_bool released{false};
    void Release() noexcept {
        if (!released.exchange(true))
            hold.Down();
    }
};

// 断言失败（REQUIRE 抛异常）时也必须先放行 worker，否则 Stop 会等 join 而死锁。
// 声明位置必须晚于 SchedulerStopGuard：析构早于 Stop。
struct DnsHoldReleaser {
    std::shared_ptr<DnsHold> hold;
    ~DnsHoldReleaser() { hold->Release(); }
};

} // namespace

/* ---------------------------------------------------------------------------
 * A. RequestClose 紧邻 Scheduler::Stop：owner 已封口但受管对象未落定就硬停，
 *    经 owner 硬停入口必须完全收敛（fd 级关闭 / 容量归零 / owner 落定 / 幂等）。
 * ------------------------------------------------------------------------- */
BOOST_AUTO_TEST_CASE(t_owner_request_close_then_immediate_stop_converges) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    SchedulerStopGuard scheduler_guard{scheduler.get()};
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto created = TransportRuntime::Create(Limits(8, 8));
    BOOST_REQUIRE(created);
    auto owner = std::move(created).value();
    BOOST_REQUIRE(owner->Start());

    auto ctx = std::make_shared<ParkCtx>();
    const int raw = MakeRawListener(ctx->raw_port);
    BOOST_REQUIRE(raw >= 0);

    ParkThreeOps(owner, ctx);
    WaitParked(ctx);

    const int conn_fd     = ctx->conn_fd.load();
    const int listener_fd = FindSocketFdByPort(ctx->listen_port);
    BOOST_REQUIRE(conn_fd >= 0);
    BOOST_REQUIRE(listener_fd >= 0);
    BOOST_CHECK(FdOpen(conn_fd));
    BOOST_CHECK(FdOpen(listener_fd));
    // 三个受管 socket + 1 个挂起 Accept 预留的连接名额。
    BOOST_CHECK_EQUAL(W::TransportsHeldForTest(*owner), 4u);
    BOOST_CHECK(!owner->IsClosed());

    owner->RequestClose();     // 封口 + 唤醒在途等待
    scheduler->Stop();         // 紧邻硬停：不给被唤醒 op「先落定」的等待窗口

    BOOST_TEST_MESSAGE(
        "post-stop immediate: op_returned=" << ctx->op_returned.load()
        << " conn_closed=" << ctx->conn->IsClosed()
        << " listener_closed=" << ctx->listener->IsClosed()
        << " udp_closed=" << ctx->udp->IsClosed()
        << " held=" << W::TransportsHeldForTest(*owner)
        << " owner_closed=" << owner->IsClosed());

    // 收口不变量：无论竞态把哪些 op 落在 Stop 之前/之后，owner 硬停入口之后
    // 必须全部收敛且幂等。
    const std::size_t forced = W::ForceCloseAfterQuiescence(*owner);
    BOOST_TEST_MESSAGE("owner hard-stop after RequestClose: forced=" << forced);
    BOOST_CHECK(ctx->conn->IsClosed());
    BOOST_CHECK(ctx->listener->IsClosed());
    BOOST_CHECK(ctx->udp->IsClosed());
    BOOST_CHECK_EQUAL(ProbeFd(conn_fd).rc, -1);
    BOOST_CHECK_EQUAL(ProbeFd(conn_fd).err, EBADF);
    BOOST_CHECK_EQUAL(ProbeFd(listener_fd).rc, -1);
    BOOST_CHECK_EQUAL(ProbeFd(listener_fd).err, EBADF);
    BOOST_CHECK_EQUAL(W::TransportsHeldForTest(*owner), 0u);
    BOOST_CHECK(owner->IsClosed());

    BOOST_CHECK_EQUAL(W::ForceCloseAfterQuiescence(*owner), 0u);   // 幂等
    BOOST_CHECK(owner->IsClosed());
    BOOST_CHECK_EQUAL(W::TransportsHeldForTest(*owner), 0u);

    ::close(raw);
    ctx->conn.reset();
    ctx->listener.reset();
    ctx->udp.reset();
}

/* ---------------------------------------------------------------------------
 * B. DNS 在途到期与晚到结果（间接口径）：
 *    worker 被扣持 → 解析等待确定在途 → deadline 到期（不是调用前已过期）
 *    → TimedOut + 名额归还；随后放行 worker，间接验证晚到结果不重复归还名额，
 *    且 DNS 通道仍可用（后续 dial 成功）。worker 侧「丢弃晚到结果」的分支没有
 *    外部可观察面，本用例不断言其被走到。
 * ------------------------------------------------------------------------- */
BOOST_AUTO_TEST_CASE(t_dial_dns_inflight_deadline_and_late_result) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    SchedulerStopGuard scheduler_guard{scheduler.get()};   // 先析构（最后执行）
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto hold     = std::make_shared<DnsHold>();
    DnsHoldReleaser release_guard{hold};                   // 早于 Stop 析构

    auto created = NetworkRuntime::Create(Limits(8, 2));
    BOOST_REQUIRE(created);
    auto runtime = std::move(created).value();
    auto impl = std::dynamic_pointer_cast<NetworkRuntimeImpl>(runtime);
    BOOST_REQUIRE(impl);
    BOOST_REQUIRE(runtime->Start());

    auto parked  = std::make_shared<bbt::core::thread::CountDownLatch>(1);
    auto done    = std::make_shared<bbt::core::thread::CountDownLatch>(1);
    auto code    = std::make_shared<std::atomic_int>(-1);
    auto dns_cat = std::make_shared<std::atomic_bool>(false);

    impl->SetDialWaitEntryGateForTest([hold, parked] {
        parked->Down();          // worker 已起跑 ⇒ 调用方协程已挂起在等待
        hold->hold.Wait();       // 扣住 worker：等待段不可能自行完成
    });

    // localhost 在 /etc/hosts 中，放行后 getaddrinfo 快返成功 —— 用于构造
    // 「晚到的成功结果」以尝试触发 freeaddrinfo 丢弃分支；该分支是否走到
    // 不由本用例断言（无外部可观察面），只观察其后的名额与 DNS 通道状态。
    const std::string host = "localhost";
    bbtco [runtime, &host, done, code, dns_cat]() {
        CallOptions options;
        options.deadline = std::chrono::steady_clock::now() +
                           std::chrono::milliseconds{600};
        auto dialed = runtime->DialTCP(TcpEndpoint{host, 80}, options);
        if (!dialed) {
            code->store(static_cast<int>(dialed.error().code));
            dns_cat->store(dialed.error().backend_category == "dns");
        }
        done->Down();
    };

    BOOST_REQUIRE_EQUAL(parked->WaitTimeout(2000), 0);      // 解析确已在途
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 1u);

    BOOST_REQUIRE_EQUAL(done->WaitTimeout(8000), 0);        // 在途到期落定
    BOOST_CHECK_EQUAL(code->load(), static_cast<int>(bbt::infra::ErrorCode::TimedOut));
    BOOST_CHECK(dns_cat->load());
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);

    // 放行 worker：此刻才真正执行 getaddrinfo 并投递（可能的）晚到结果；等其
    // 投递窗口过去后间接验证名额未被重复归还——仍为 0，未变负或重新占用。
    impl->SetDialWaitEntryGateForTest({});
    hold->Release();
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);

    // DNS 通道未被晚到结果污染：后续解析 + 拨号必须成功。
    std::uint16_t raw_port = 0;
    const int raw = MakeRawListener(raw_port);
    BOOST_REQUIRE(raw >= 0);
    auto ok_done = std::make_shared<bbt::core::thread::CountDownLatch>(1);
    auto ok_flag = std::make_shared<std::atomic_bool>(false);
    bbtco [runtime, raw_port, ok_done, ok_flag]() {
        const std::string again = "localhost";
        auto dialed = runtime->DialTCP(TcpEndpoint{again, raw_port}, Options(5000));
        ok_flag->store(static_cast<bool>(dialed));
        ok_done->Down();
    };
    BOOST_REQUIRE_EQUAL(ok_done->WaitTimeout(8000), 0);
    BOOST_CHECK(ok_flag->load());

    ::close(raw);
    runtime->RequestClose();
    scheduler->Stop();
}
