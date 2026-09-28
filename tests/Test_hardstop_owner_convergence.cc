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
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/TransportRuntime.hpp>

#include "detail/TransportWiring.hpp"    // 内部装配面：owner 硬停入口 + fd/gate 接缝
#include "http/NetworkRuntimeImpl.hpp"   // 协议 owner 的硬停入口（src 内部头，不安装）

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

// 内部装配面（src/detail/TransportWiring.hpp）：owner 硬停入口与 fd/gate 接缝。
// 用类型别名而非 namespace 别名——接缝是 TransportWiring 的静态成员。
using W = bbt::infra::detail::TransportWiring;
using bbt::infra::http_detail::NetworkRuntimeImpl;

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

// 测试自有的普通 listening socket：作为受管 DialTCP 的对端（内核在 backlog 中
// 完成三次握手，因此拨号成功且连接保持打开）。
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

// 与 Test_tcp_runtime_factory::SmallLimits 同形：ValidateNetworkLimits 对
// 五个字段都做范围校验，只设两个会让 Create 返回 InvalidArgument。
NetworkLimits Limits(std::size_t max_connections, std::size_t max_inflight) {
    NetworkLimits limits{};
    limits.max_connections  = max_connections;
    limits.max_inflight     = max_inflight;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = 64 * 1024;
    limits.incoming_timeout = std::chrono::milliseconds{3000};
    return limits;
}

/* 用例在断言失败时也必须把调度器停掉：残留的运行中调度线程会让进程不退出
 * （ctest 只能等超时），失败现场反而不可读。 */
struct SchedulerStopGuard {
    bbt::coroutine::detail::Scheduler* scheduler;
    explicit SchedulerStopGuard(bbt::coroutine::detail::Scheduler* s)
        : scheduler(s) {}
    ~SchedulerStopGuard() {
        if (scheduler->IsRunning())
            scheduler->Stop();
    }
};

/* 硬停不展开协程栈：被捕获状态必须堆持有（栈上对象随用例结束销毁会留悬垂引用）。
 * 受管对象不安装测试 ClosedHook——它们的 ClosedHook 归 owner 记账，测试用
 * IsClosed()/fd/owner 名额观测，不覆盖 owner 的钩子。 */
struct OwnerCtx {
    bbt::core::thread::CountDownLatch conn_parked{1};       // 数据路径真实挂起
    bbt::core::thread::CountDownLatch listener_entered{1};  // Accept 进入等待
    bbt::core::thread::CountDownLatch udp_entered{1};       // Receive 进入等待
    std::atomic_bool op_returned{false};                    // 哨兵：任一 op 返回即置真
    std::atomic_int  conn_fd{-1};
    CoTCP::SPtr         conn;
    CoTCPListener::SPtr listener;
    CoUDP::SPtr         udp;
    std::uint16_t       listen_port{0};
    std::uint16_t       raw_port{0};
};

/* 三个受管对象各挂一个在途 op —— 硬停缺口的必要条件（门控路径永不收尾）。
 * owner 既可以是 TransportRuntime（transport owner 直测），也可以是
 * NetworkRuntime（协议 owner 转发面），两者工厂签名相同。 */
template <class Owner>
void ParkThreeOps(const std::shared_ptr<Owner>& owner,
                  const std::shared_ptr<OwnerCtx>& ctx) {
    auto listener = owner->ListenTCP(Loopback(0), 8);
    BOOST_REQUIRE(listener);
    ctx->listener = listener.value();
    ctx->listen_port = ctx->listener->LocalAddress().port;
    BOOST_REQUIRE(ctx->listen_port != 0);

    auto udp = owner->BindUDP(Loopback(0));
    BOOST_REQUIRE(udp);
    ctx->udp = udp.value();

    // 受管 listener 的 Accept 挂起（无人连接它）：gate 触发 = 已进入等待。
    bbtco [ctx]() {
        W::SetWaitEntryGateForTest(*ctx->listener,
                                  [ctx]() { ctx->listener_entered.Down(); });
        auto accepted = ctx->listener->Accept(Options());
        (void)accepted;
        ctx->op_returned.store(true);
    };
    // 受管 UDP 的 Receive 挂起。
    bbtco [ctx]() {
        W::SetWaitEntryGateForTest(*ctx->udp,
                                  [ctx]() { ctx->udp_entered.Down(); });
        char buf[4]{};
        auto recv = ctx->udp->Receive(MutableBytes{buf, sizeof(buf)}, Options());
        (void)recv;
        ctx->op_returned.store(true);
    };
    // 受管 DialTCP（对端是测试自有普通 listener）→ ReadSome 挂起。
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

void WaitParked(const std::shared_ptr<OwnerCtx>& ctx) {
    BOOST_REQUIRE_EQUAL(ctx->listener_entered.WaitTimeout(10000), 0);
    BOOST_REQUIRE_EQUAL(ctx->udp_entered.WaitTimeout(10000), 0);
    BOOST_REQUIRE_EQUAL(ctx->conn_parked.WaitTimeout(10000), 0);
}

/* 缺口复现断言（基线行为，不随本候选改变）：硬停后三个对象都未物理关闭、
 * fd 仍开、挂起 op 不返回。这是「RequestClose 门控路径在硬停下永不收尾」的
 * 确定性复现，也是本候选强制关闭入口的必要性证据。 */
void CheckHardStopGap(const std::shared_ptr<OwnerCtx>& ctx, int conn_fd,
                      int listener_fd) {
    BOOST_CHECK_EQUAL(ctx->op_returned.load(), false);
    BOOST_CHECK(FdOpen(conn_fd));
    BOOST_CHECK(FdOpen(listener_fd));
    BOOST_CHECK(!ctx->conn->IsClosed());
    BOOST_CHECK(!ctx->listener->IsClosed());
    BOOST_CHECK(!ctx->udp->IsClosed());
}

/* 强制关闭后的收敛断言：三个对象物理关闭（fd 级）、挂起 op 仍未返回。 */
void CheckForcedClosed(const std::shared_ptr<OwnerCtx>& ctx, int conn_fd,
                       int listener_fd) {
    BOOST_CHECK(ctx->conn->IsClosed());
    BOOST_CHECK(ctx->listener->IsClosed());
    BOOST_CHECK(ctx->udp->IsClosed());
    BOOST_CHECK_EQUAL(ProbeFd(conn_fd).rc, -1);
    BOOST_CHECK_EQUAL(ProbeFd(conn_fd).err, EBADF);
    BOOST_CHECK_EQUAL(ProbeFd(listener_fd).rc, -1);
    BOOST_CHECK_EQUAL(ProbeFd(listener_fd).err, EBADF);
    BOOST_CHECK_EQUAL(ctx->op_returned.load(), false);   // 未复活执行
}

} // namespace

/* ---------------------------------------------------------------------------
 * TransportRuntime（transport owner）批量硬停收口：
 * 受管 listener + 受管 UDP + 受管 dialed TCP 各挂一个在途 op → Scheduler::Stop()
 * → owner 批量强制物理关闭。判据：对象级 fd 级关闭、owner 名额归零、owner 落定。
 * ------------------------------------------------------------------------- */
BOOST_AUTO_TEST_CASE(t_transport_owner_batch_close_converges) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    SchedulerStopGuard scheduler_guard{scheduler.get()};
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto transport_res = TransportRuntime::Create(Limits(8, 8));
    BOOST_REQUIRE(transport_res);
    auto transport = transport_res.value();
    BOOST_REQUIRE(transport->Start());

    auto ctx = std::make_shared<OwnerCtx>();
    const int raw = MakeRawListener(ctx->raw_port);
    BOOST_REQUIRE(raw >= 0);

    ParkThreeOps(transport, ctx);
    WaitParked(ctx);

    const int conn_fd     = ctx->conn_fd.load();
    const int listener_fd = FindSocketFdByPort(ctx->listen_port);
    BOOST_REQUIRE(conn_fd >= 0);
    BOOST_REQUIRE(listener_fd >= 0);
    // 4 = 三个受管 socket（listener / UDP / dialed TCP）+ 1 个挂起 Accept 已预留
    // 的连接名额（Accept 在 m_accept_admit 成功后即占用一个连接容量）。
    BOOST_CHECK_EQUAL(W::TransportsHeldForTest(*transport), 4);

    scheduler->Stop();

    CheckHardStopGap(ctx, conn_fd, listener_fd);
    BOOST_CHECK(!transport->IsClosed());
    BOOST_CHECK_EQUAL(W::TransportsHeldForTest(*transport), 4);  // 名额不归还

    // owner 批量硬停入口（唯一生产调用点，前置条件：Stop 已返回）。
    const std::size_t forced = W::ForceCloseAfterQuiescence(*transport);
    BOOST_CHECK_EQUAL(forced, 3);
    CheckForcedClosed(ctx, conn_fd, listener_fd);
    BOOST_CHECK_EQUAL(W::TransportsHeldForTest(*transport), 0);  // hook/名额一一配对
    BOOST_CHECK(transport->IsClosed());                          // owner 落定

    BOOST_CHECK_EQUAL(W::ForceCloseAfterQuiescence(*transport), 0);  // 幂等
    BOOST_CHECK(transport->IsClosed());
    BOOST_CHECK_EQUAL(W::TransportsHeldForTest(*transport), 0);

    ::close(raw);
}

/* ---------------------------------------------------------------------------
 * 协议 owner（NetworkRuntime）硬停收敛：同一受管集合经 NetworkRuntime 转发面
 * 建立 → 硬停 → owner 硬停入口 → transport 自证落定 + 协议 owner 落定。
 * 同时记录 WaitClosed 在硬停后的实际返回口径（不预设结论）。
 * ------------------------------------------------------------------------- */
BOOST_AUTO_TEST_CASE(t_network_runtime_owner_hardstop_converges) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    SchedulerStopGuard scheduler_guard{scheduler.get()};
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto created = NetworkRuntime::Create(Limits(8, 8));
    BOOST_REQUIRE(created);
    auto runtime = created.value();
    auto impl = std::dynamic_pointer_cast<NetworkRuntimeImpl>(runtime);
    BOOST_REQUIRE(impl);
    BOOST_REQUIRE(runtime->Start());

    auto ctx = std::make_shared<OwnerCtx>();
    const int raw = MakeRawListener(ctx->raw_port);
    BOOST_REQUIRE(raw >= 0);

    ParkThreeOps(runtime, ctx);
    WaitParked(ctx);

    const int conn_fd     = ctx->conn_fd.load();
    const int listener_fd = FindSocketFdByPort(ctx->listen_port);
    BOOST_REQUIRE(conn_fd >= 0);
    BOOST_REQUIRE(listener_fd >= 0);

    scheduler->Stop();

    CheckHardStopGap(ctx, conn_fd, listener_fd);
    BOOST_CHECK(!runtime->IsClosed());   // 硬停后托管链未收敛（缺口复现）

    const std::size_t forced = impl->ForceCloseAfterQuiescence();
    BOOST_CHECK_EQUAL(forced, 3);
    CheckForcedClosed(ctx, conn_fd, listener_fd);
    BOOST_TEST_MESSAGE("after owner force close: transport_closed="
                       << impl->TransportClosedForTest()
                       << " unclosed_children=" << impl->UnclosedChildrenForTest()
                       << " runtime_closed=" << runtime->IsClosed());
    // runtime->IsClosed() 的前提是 transport owner 自证落定（MaybeFinalize 的
    // m_transport->IsClosed() 门控）——协议 owner 与 transport owner 同时收敛。
    BOOST_CHECK(impl->TransportClosedForTest());
    BOOST_CHECK(runtime->IsClosed());

    // WaitClosed 硬停后实测口径：Scheduler 已停（代际消失）且本线程无协程上下文，
    // 挂起等待不可能发生。记录实际返回，不伪造 Closed。
    const auto st = runtime->WaitClosed(
        std::chrono::steady_clock::now() + std::chrono::milliseconds(200), {});
    BOOST_TEST_MESSAGE("post-stop WaitClosed actual status="
                       << static_cast<int>(st));
    BOOST_CHECK(st != bbt::infra::CloseStatus::Closed);

    BOOST_CHECK_EQUAL(impl->ForceCloseAfterQuiescence(), 0);   // 幂等
    BOOST_CHECK(runtime->IsClosed());

    ::close(raw);
}


/* ---------------------------------------------------------------------------
 * HTTP 子对象（Agent/协议 owner 的子对象）硬停收敛：CreateHttpClient 登记一个
 * 未物理关闭的子对象（无在途 op）→ Scheduler::Stop() → owner 硬停入口。
 * 判据：硬停后子对象仍未 teardown、runtime 未落定；强制关闭后 unclosed children
 * 归零、transport owner 自证落定、runtime 落定。覆盖「硬停后 HTTP 子对象能否收口」
 * 这一此前未测路径（走 RequestClose 的 off-domain 降级实现，同 TeardownOffDomain）。
 * ------------------------------------------------------------------------- */
BOOST_AUTO_TEST_CASE(t_http_child_hardstop_converges) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    SchedulerStopGuard scheduler_guard{scheduler.get()};
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto created = NetworkRuntime::Create(Limits(8, 8));
    BOOST_REQUIRE(created);
    auto runtime = created.value();
    auto impl = std::dynamic_pointer_cast<NetworkRuntimeImpl>(runtime);
    BOOST_REQUIRE(impl);
    BOOST_REQUIRE(runtime->Start());

    // HTTP 出站子对象：仅登记（无在途 op），作为硬停下「未收口的 HTTP 子对象」。
    auto client_res = runtime->CreateHttpClient();
    BOOST_REQUIRE(client_res);
    auto client = client_res.value();
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 1);
    BOOST_CHECK(!impl->TransportClosedForTest());

    scheduler->Stop();   // 硬停：子对象 teardown 不再被 io 域执行

    // 缺口复现：HTTP 子对象未 teardown ⇒ runtime 未落定。
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 1);
    BOOST_CHECK(!runtime->IsClosed());

    const std::size_t forced = impl->ForceCloseAfterQuiescence();
    client.reset();   // 测试不持有子对象时 owner 仍须收口
    BOOST_TEST_MESSAGE("http-child hard-stop: forced=" << forced
                       << " unclosed_children=" << impl->UnclosedChildrenForTest()
                       << " transport_closed=" << impl->TransportClosedForTest()
                       << " runtime_closed=" << runtime->IsClosed());
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 0);
    BOOST_CHECK(impl->TransportClosedForTest());
    BOOST_CHECK(runtime->IsClosed());

    BOOST_CHECK_EQUAL(impl->ForceCloseAfterQuiescence(), 0);   // 幂等
    BOOST_CHECK(impl->TransportClosedForTest());
    BOOST_CHECK(runtime->IsClosed());
}
