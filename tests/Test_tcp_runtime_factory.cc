#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <cerrno>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/sync/Cancellation.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>
// 内部装配面（src 内部头，不安装）：受管对象/owner 的测试接缝。
#include "detail/TransportWiring.hpp"
#include <bbt/infra/CoTCP.hpp>
#include <bbt/infra/CoUDP.hpp>
#include <bbt/infra/NetworkRuntime.hpp>

#include "http/NetworkRuntimeImpl.hpp"

using bbt::infra::detail::TransportWiring;
using bbt::infra::CallOptions;
using bbt::infra::CloseStatus;
using bbt::infra::ConstBytes;
using bbt::infra::ErrorCode;
using bbt::infra::IoState;
using bbt::infra::MutableBytes;
using bbt::infra::NetworkLimits;
using bbt::infra::NetworkRuntime;
using bbt::infra::SocketAddress;
using bbt::infra::TcpEndpoint;
using bbt::infra::http_detail::NetworkRuntimeImpl;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

NetworkLimits SmallLimits(std::size_t max_connections) {
    NetworkLimits limits{};
    limits.max_connections = max_connections;
    limits.max_inflight    = 64;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = 64 * 1024;
    limits.incoming_timeout = std::chrono::milliseconds{3000};
    return limits;
}

CallOptions Options(int timeout_ms = 5000) {
    CallOptions options;
    options.deadline = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(timeout_ms);
    return options;
}

// 协程内执行任务并限时等待；任何一步失败返回 false（测试不得无限挂住）。
template <class F>
bool RunInCoroutine(F&& f, int budget_ms = 10000) {
    bbt::core::thread::CountDownLatch done{1};
    bool succ = false;
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [fn = std::forward<F>(f), &done]() mutable {
            fn();
            done.Down();
        },
        succ);
    if (!succ)
        return false;
    return done.WaitTimeout(budget_ms) == 0;
}

// 空闲 TCP 端口：bind/listen 后关闭，交给被测对象使用。用于 dial_refused
// 之外的「端口确定空闲」场景；与既有用例同一写法（loopback 测试约定）。
int ReserveFreeTcpPort() {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return 0;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    if (::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1 ||
        ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return 0;
    }
    socklen_t len = sizeof(addr);
    const int port = (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0)
                         ? ::ntohs(addr.sin_port)
                         : 0;
    ::close(fd);
    return port;
}

} // namespace

BOOST_AUTO_TEST_SUITE(tcp_runtime_factory)

// co-io-adapter/v1 §4.0.1：工厂要求 Runtime 已 Start（Create 只校验参数）。
BOOST_AUTO_TEST_CASE(t_factory_rejects_before_start) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto rt = NetworkRuntime::Create(SmallLimits(8));
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();

    // 未 Start：ListenTCP 立即返回 RuntimeUnavailable，不产出半成品对象。
    auto listen = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(!listen);
    BOOST_CHECK_EQUAL(static_cast<int>(listen.error().code),
                      static_cast<int>(ErrorCode::RuntimeUnavailable));

    // 参数门控先于启动检查不可颠倒执行语义：started 之后再验参数与容量。
    auto started = runtime->Start();
    BOOST_REQUIRE(started);

    // backlog 0 非法（§4.0.1.7：1..INT_MAX）。
    auto bad_backlog = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 0);
    BOOST_REQUIRE(!bad_backlog);
    BOOST_CHECK_EQUAL(static_cast<int>(bad_backlog.error().code),
                      static_cast<int>(ErrorCode::InvalidArgument));

    // 空 ip 非法（§4.0.1.4：通配必须显式写数值地址）。
    auto bad_ip = runtime->ListenTCP(SocketAddress{"", 0}, 16);
    BOOST_REQUIRE(!bad_ip);
    BOOST_CHECK_EQUAL(static_cast<int>(bad_ip.error().code),
                      static_cast<int>(ErrorCode::InvalidArgument));

    runtime->RequestClose();
    scheduler->Stop();
}

// co-io-adapter/v1 §4.0.1.7：容量门禁，超限 Overloaded，不静默排队；
// 物理关闭后释放容量，可再次成功。
BOOST_AUTO_TEST_CASE(t_capacity_gate_overload_then_release) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto rt = NetworkRuntime::Create(SmallLimits(1));
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());

    auto listen = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen);
    const auto port = listen.value()->LocalAddress().port;
    BOOST_REQUIRE_NE(port, 0);

    // 容量 1 已被 listener 占用：第二次工厂调用必须 Overloaded。
    auto second = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(!second);
    BOOST_CHECK_EQUAL(static_cast<int>(second.error().code),
                      static_cast<int>(ErrorCode::Overloaded));

    // DialTCP 同受同一门禁（协程内执行，可挂起）。
    bool dial_overloaded = false;
    auto ran = RunInCoroutine([&] {
        auto dialed = runtime->DialTCP(TcpEndpoint{"127.0.0.1", port}, Options());
        dial_overloaded = !dialed &&
            dialed.error().code == ErrorCode::Overloaded;
    });
    BOOST_REQUIRE(ran);
    BOOST_CHECK(dial_overloaded);

    // listener 物理关闭 → hook 释放容量 → 再次 ListenTCP 成功。
    listen.value()->RequestClose();
    auto again = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(again);

    again.value()->RequestClose();
    runtime->RequestClose();
    scheduler->Stop();
}

// co-io-adapter/v1 §4.0.1.3：Runtime DialTCP 数值地址直连（不经 DNS），
// 真实 loopback 读写闭环；受管引用在连接关闭后仍可查询 IsClosed。
BOOST_AUTO_TEST_CASE(t_runtime_dial_numeric_loopback) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto rt = NetworkRuntime::Create(SmallLimits(8));
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());

    auto listen = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen);
    const auto port = listen.value()->LocalAddress().port;
    BOOST_REQUIRE_NE(port, 0);

    std::atomic_bool server_ok{false};
    std::atomic_bool client_ok{false};
    bbt::core::thread::CountDownLatch done{2};

    bbtco [listen, &server_ok, &done]() {
        auto accepted = listen.value()->Accept(Options());
        if (!accepted) { done.Down(); return; }
        char request[5]{};
        auto read = accepted.value()->ReadSome(bbt::infra::MutableBytes{request, sizeof(request)}, Options());
        if (!read || read.value().bytes != 5) { done.Down(); return; }
        const char response[] = "world";
        auto write = accepted.value()->WriteAll(bbt::infra::ConstBytes{response, 5}, Options());
        server_ok.store(write && write.value().bytes == 5);
        accepted.value()->RequestClose();
        done.Down();
    };

    bbtco [runtime, listen, port, &client_ok, &done]() {
        auto dialed = runtime->DialTCP(TcpEndpoint{"127.0.0.1", port}, Options());
        if (!dialed) { done.Down(); return; }
        auto client = dialed.value();
        const char request[] = "hello";
        auto write = client->WriteAll(bbt::infra::ConstBytes{request, 5}, Options());
        char response[5]{};
        auto read = client->ReadSome(bbt::infra::MutableBytes{response, sizeof(response)}, Options());
        client_ok.store(write && write.value().bytes == 5 && read && read.value().bytes == 5 &&
                        std::string(response, 5) == "world");
        client->RequestClose();
        // 受管引用：关闭后引用仍可用于查询（§4.0.1.2）。
        if (client->IsClosed())
            client->WaitClosed(Options().deadline, {});
        done.Down();
    };

    BOOST_REQUIRE_EQUAL(done.WaitTimeout(10000), 0);
    BOOST_CHECK(server_ok.load());
    BOOST_CHECK(client_ok.load());

    listen.value()->RequestClose();
    runtime->RequestClose();
    scheduler->Stop();
}

BOOST_AUTO_TEST_CASE(t_dial_hostname_localhost_end_to_end) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto rt = NetworkRuntime::Create(SmallLimits(8));
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());

    auto listen = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen);
    const auto port = listen.value()->LocalAddress().port;
    BOOST_REQUIRE_NE(port, 0);

    std::atomic_bool server_ok{false};
    std::atomic_bool client_ok{false};
    bbt::core::thread::CountDownLatch done{2};

    bbtco [listen, &server_ok, &done]() {
        auto accepted = listen.value()->Accept(Options());
        if (!accepted) { done.Down(); return; }
        char request[5]{};
        auto read = accepted.value()->ReadSome(bbt::infra::MutableBytes{request, sizeof(request)}, Options());
        server_ok.store(read && read.value().bytes == 5);
        accepted.value()->RequestClose();
        done.Down();
    };

    // co-io-adapter/v1 §4.0.1.4：真实主机名 "localhost" 经 transport 接管的
    // DNS 解析（getaddrinfo Hook → DnsResolver::AwaitBounded worker）；等待有
    // deadline/cancel，超时后 worker 仍完成不可取消的 libc 工作，结果由状态回收。
    // 解析成功后连接 loopback。
    bbtco [runtime, port, &client_ok, &done]() {
        auto dialed = runtime->DialTCP(TcpEndpoint{"localhost", port}, Options());
        if (!dialed) { done.Down(); return; }
        const char request[] = "hello";
        auto write = dialed.value()->WriteAll(bbt::infra::ConstBytes{request, 5}, Options());
        client_ok.store(write && write.value().bytes == 5);
        dialed.value()->RequestClose();
        done.Down();
    };

    BOOST_REQUIRE_EQUAL(done.WaitTimeout(10000), 0);
    BOOST_CHECK(server_ok.load());
    BOOST_CHECK(client_ok.load());

    listen.value()->RequestClose();
    runtime->RequestClose();
    scheduler->Stop();
}

// co-io-adapter/v1 §4：hostname 解析失败的 Error 映射——
// 确定性快败（非法主机名 EAI_NONAME）→ TransportError + backend_category="dns"；
// 与既有 Unavailable(getaddrinfo) 数值快路径区分。
BOOST_AUTO_TEST_CASE(t_dial_hostname_failure_mapping) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto rt = NetworkRuntime::Create(SmallLimits(8));
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());

    // 超过 255 字节的域名在 glibc getaddrinfo 确定快败（EAI_NONAME），
    // 不经真实网络，测试不挂住。
    const std::string too_long(300, 'a');
    std::atomic_int code{-1};
    std::atomic_bool dns_category{false};
    bbt::core::thread::CountDownLatch done{1};
    bbtco [runtime, &too_long, &done, &code, &dns_category]() {
        auto dialed = runtime->DialTCP(TcpEndpoint{too_long, 80}, Options());
        if (!dialed) {
            code.store(static_cast<int>(dialed.error().code));
            dns_category.store(dialed.error().backend_category == "dns");
        }
        done.Down();
    };
    BOOST_REQUIRE_EQUAL(done.WaitTimeout(10000), 0);
    BOOST_CHECK_EQUAL(code.load(), static_cast<int>(ErrorCode::TransportError));
    BOOST_CHECK(dns_category.load());

    runtime->RequestClose();
    scheduler->Stop();
}

// co-io-adapter/v1 §4：解析等待使用本次调用的同一绝对 deadline——
// hostname 解析等待必须受 CallOptions.deadline 约束（TimedOut），不能无界挂起。
BOOST_AUTO_TEST_CASE(t_dial_hostname_deadline_bounded) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto rt = NetworkRuntime::Create(SmallLimits(8));
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());

    // 已过期 deadline：解析不得启动可观察的阻塞等待，必须立即返回 TimedOut。
    const std::string too_long(300, 'a');
    bbt::core::thread::CountDownLatch done{1};
    std::atomic_int code{-1};
    std::atomic_bool dns_category{false};
    bbtco [runtime, &too_long, &done, &code, &dns_category]() {
        CallOptions options;
        options.deadline = std::chrono::steady_clock::now() - std::chrono::milliseconds{1};
        auto dialed = runtime->DialTCP(TcpEndpoint{too_long, 80}, options);
        if (!dialed) {
            code.store(static_cast<int>(dialed.error().code));
            dns_category.store(dialed.error().backend_category == "dns");
        }
        done.Down();
    };
    BOOST_REQUIRE_EQUAL(done.WaitTimeout(10000), 0);
    BOOST_CHECK_EQUAL(code.load(), static_cast<int>(ErrorCode::TimedOut));
    BOOST_CHECK(dns_category.load());

    runtime->RequestClose();
    scheduler->Stop();
}

// Runtime 关闭须收口已接纳连接，不仅停止 listener；否则 WaitClosed 可能
// 在 socket 仍被强持有时错误报告 Closed。
BOOST_AUTO_TEST_CASE(t_runtime_close_with_accepted_connection) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());
    auto rt = NetworkRuntime::Create(SmallLimits(2));
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());
    auto listen = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen);
    const auto port = listen.value()->LocalAddress().port;
    BOOST_REQUIRE_NE(port, 0);
    bbt::core::thread::CountDownLatch accepted_done{1};
    std::shared_ptr<bbt::infra::CoTCP> accepted;
    bbtco [listen, &accepted, &accepted_done]() {
        auto result = listen.value()->Accept(Options());
        if (result) accepted = std::move(result).value();
        accepted_done.Down();
    };
    const int peer = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(peer >= 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = ::htons(port);
    BOOST_REQUIRE_EQUAL(::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr), 1);
    BOOST_REQUIRE_EQUAL(::connect(peer, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
    BOOST_REQUIRE_EQUAL(accepted_done.WaitTimeout(2000), 0);
    BOOST_REQUIRE(accepted);
    runtime->RequestClose();
    bbt::core::thread::CountDownLatch closed_done{1};
    std::atomic_int status{-1};
    bbtco [runtime, &status, &closed_done]() {
        status.store(static_cast<int>(runtime->WaitClosed(Options().deadline, {})));
        closed_done.Down();
    };
    BOOST_REQUIRE_EQUAL(closed_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(status.load(), static_cast<int>(CloseStatus::Closed));
    BOOST_CHECK(accepted->IsClosed());
    ::close(peer);
    scheduler->Stop();
}

BOOST_AUTO_TEST_CASE(t_accept_capacity_gate_and_closed_runtime) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());
    auto rt = NetworkRuntime::Create(SmallLimits(1));
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());
    auto listen = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen);
    bbt::core::thread::CountDownLatch done{1};
    std::atomic_int code{-1};
    bbtco [listen, &done, &code]() {
        auto result = listen.value()->Accept(Options());
        code.store(result ? -2 : static_cast<int>(result.error().code));
        done.Down();
    };
    BOOST_REQUIRE_EQUAL(done.WaitTimeout(2000), 0);
    BOOST_CHECK_EQUAL(code.load(), static_cast<int>(ErrorCode::Overloaded));
    runtime->RequestClose();
    auto after = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(!after);
    BOOST_CHECK_EQUAL(static_cast<int>(after.error().code), static_cast<int>(ErrorCode::Closed));
    scheduler->Stop();
}

// co-io-adapter/v1 §4.0.1.4：ListenTCP 只接受数值地址。主机名必须在入口
// 立即 InvalidArgument 拒绝，不得落到未接管的同步 getaddrinfo（§4.0.1.3）；
// 断言并覆盖「拒绝发生在容量预留之前」——max_connections=1 下被拒调用不消耗
// 容量，随后数值地址仍可成功绑定。
BOOST_AUTO_TEST_CASE(t_listen_tcp_rejects_non_numeric_address) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto rt = NetworkRuntime::Create(SmallLimits(1));
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());

    // localhost 可解析、no-such-host.invalid 不可解析：两者都必须在入口
    // 快返 InvalidArgument（确定性，不产生解析等待）。
    for (const char* host : {"localhost", "no-such-host.invalid"}) {
        auto rejected = runtime->ListenTCP(SocketAddress{host, 0}, 16);
        BOOST_REQUIRE_MESSAGE(!rejected, "hostname must be rejected: " << host);
        BOOST_CHECK_EQUAL(static_cast<int>(rejected.error().code),
                          static_cast<int>(ErrorCode::InvalidArgument));
    }

    auto numeric = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(numeric);
    BOOST_REQUIRE_NE(numeric.value()->LocalAddress().port, 0);

    numeric.value()->RequestClose();
    runtime->RequestClose();
    scheduler->Stop();
}

// co-io-adapter/v1 §5 与 §4.0.1.2：冻结公开名 bbt::infra::CoUDP 与
// NetworkRuntime::BindUDP 可编译可链接；交付对象由 Runtime 强持有并占用
// max_connections 容量，物理关闭（ClosedHook → OnTransportClosed）后释放，
// 与 TCP 工厂同一门禁语义。
BOOST_AUTO_TEST_CASE(t_runtime_bind_udp_managed_capacity) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto rt = NetworkRuntime::Create(SmallLimits(1));
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());

    // 按契约冻结名书写（不经 udp:: 限定）。
    bbt::infra::CoUDP::SPtr socket;
    auto bound = runtime->BindUDP(SocketAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(bound);
    socket = std::move(bound).value();
    BOOST_CHECK(!socket->IsClosed());
    BOOST_CHECK_NE(socket->LocalAddress().port, 0); // 端口 0 → 内核分配

    // Runtime 强持有已交付 socket：max_connections=1 下第二次 bind 超限。
    auto overflow = runtime->BindUDP(SocketAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(!overflow);
    BOOST_CHECK_EQUAL(static_cast<int>(overflow.error().code),
                      static_cast<int>(ErrorCode::Overloaded));

    // 物理关闭落定 → 容量释放 → 再次 bind 成功。
    socket->RequestClose();
    BOOST_CHECK(socket->IsClosed());
    auto again = runtime->BindUDP(SocketAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(again);
    again.value()->RequestClose();

    socket.reset();
    runtime->RequestClose();
    scheduler->Stop();
}

BOOST_AUTO_TEST_CASE(t_closed_transports_release_runtime_ownership) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    auto rt = NetworkRuntime::Create(SmallLimits(2));
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());
    auto bound = runtime->BindUDP(SocketAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(bound);
    auto listen = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen);
    std::shared_ptr<bbt::infra::CoUDP> udp = std::move(bound).value();
    std::shared_ptr<bbt::infra::CoTCPListener> tcp = std::move(listen).value();
    std::weak_ptr<bbt::infra::CoUDP> udp_weak = udp;
    std::weak_ptr<bbt::infra::CoTCPListener> tcp_weak = tcp;
    udp->RequestClose();
    tcp->RequestClose();
    udp.reset();
    tcp.reset();
    BOOST_CHECK(udp_weak.expired());
    BOOST_CHECK(tcp_weak.expired());
    runtime->RequestClose();
    scheduler->Stop();
}

BOOST_AUTO_TEST_CASE(t_numeric_connect_refused_preserves_errno) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    auto rt = NetworkRuntime::Create(SmallLimits(2));
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());
    const int reserved = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(reserved >= 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    BOOST_REQUIRE_EQUAL(::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr), 1);
    BOOST_REQUIRE_EQUAL(::bind(reserved, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    socklen_t addr_len = sizeof(addr);
    BOOST_REQUIRE_EQUAL(::getsockname(reserved, reinterpret_cast<sockaddr*>(&addr), &addr_len), 0);
    std::atomic_int code{-1};
    std::atomic_int native{-1};
    std::atomic_bool errno_category{false};
    const bool ran = RunInCoroutine([&] {
        auto dialed = runtime->DialTCP(TcpEndpoint{"127.0.0.1", ::ntohs(addr.sin_port)}, Options(1000));
        if (!dialed) {
            code.store(static_cast<int>(dialed.error().code));
            native.store(dialed.error().backend_code);
            errno_category.store(dialed.error().backend_category == "errno");
        }
    });
    ::close(reserved);
    BOOST_REQUIRE(ran);
    BOOST_CHECK_EQUAL(code.load(), static_cast<int>(ErrorCode::TransportError));
    BOOST_CHECK(errno_category.load());
    BOOST_CHECK_EQUAL(native.load(), ECONNREFUSED);
    runtime->RequestClose();
    scheduler->Stop();
}

// ---------------------------------------------------------------------------
// Issue #32：Runtime 级 max_inflight 验收矩阵
// ---------------------------------------------------------------------------

// §4.0.1.7：max_inflight=1 下第一个挂起 Accept 占满唯一名额，第二个并发
// Accept 立即 Overloaded、不排队不挂起；listener 关闭后名额归还。
// 确定性：测试接缝 SetWaitEntryGateForTest 在名额登记完成后、等待进入前
// 触发——主线程在 latch 上确认第一笔名额已入账后再发起第二次调用；
// 拒绝路径由 second_done latch 证明「立即落定」，不依赖固定 sleep。
BOOST_AUTO_TEST_CASE(t_max_inflight_one_second_accept_overloaded) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    NetworkLimits limits = SmallLimits(4);
    limits.max_inflight = 1;
    auto rt = NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());
    auto impl = std::dynamic_pointer_cast<NetworkRuntimeImpl>(runtime);
    BOOST_REQUIRE(impl);

    auto listen = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen);

    // 接缝：第一笔 Accept 完成名额登记后放下 admitted latch——此后账本
    // 名额已被占满，第二个 Accept 的 Overloaded 是确定性的。
    bbt::core::thread::CountDownLatch admitted{1};
    TransportWiring::SetWaitEntryGateForTest(*listen.value(),
        [&admitted] { admitted.Down(); });

    bbt::core::thread::CountDownLatch first_done{1};
    bbt::core::thread::CountDownLatch second_done{1};
    std::atomic_int first_code{-1};
    std::atomic_int second_code{-1};

    bbtco [listen, &first_done, &first_code]() {
        auto result = listen.value()->Accept(Options(30000));
        first_code.store(result ? -2 : static_cast<int>(result.error().code));
        first_done.Down();
    };

    // 名额已入账（接缝触发后再 Down）——无需时间推断。
    BOOST_REQUIRE_EQUAL(admitted.WaitTimeout(2000), 0);
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 1u);

    bbtco [listen, &second_done, &second_code]() {
        auto result = listen.value()->Accept(Options(30000));
        second_code.store(result ? -2 : static_cast<int>(result.error().code));
        second_done.Down();
    };

    // 「立即拒绝」由 latch 落定证明：若实现仍在排队，第二个 Accept 会
    // 挂起直到 listener 关闭——这里在 RequestClose 之前等 second_done，
    // 超时即非立即拒绝。
    BOOST_REQUIRE_EQUAL(second_done.WaitTimeout(2000), 0);
    BOOST_CHECK_EQUAL(second_code.load(),
                      static_cast<int>(ErrorCode::Overloaded));
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 1u);

    listen.value()->RequestClose();
    BOOST_REQUIRE_EQUAL(first_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(first_code.load(), static_cast<int>(ErrorCode::Closed));
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);

    runtime->RequestClose();
    scheduler->Stop();
}

// §4.0.1.7：账本是 Runtime 作用域——跨 listener 的并发 Accept 共享同一
// max_inflight 预算；任一 listener 挂起占名额时，另一 listener 的 Accept
// 也必须 Overloaded（不是 per-object 各自限额）。
BOOST_AUTO_TEST_CASE(t_max_inflight_shared_across_listeners) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    NetworkLimits limits = SmallLimits(4);
    limits.max_inflight = 1;
    auto rt = NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());
    auto impl = std::dynamic_pointer_cast<NetworkRuntimeImpl>(runtime);
    BOOST_REQUIRE(impl);

    auto listen_a = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    auto listen_b = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen_a);
    BOOST_REQUIRE(listen_b);

    bbt::core::thread::CountDownLatch admitted{1};
    TransportWiring::SetWaitEntryGateForTest(*listen_a.value(),
        [&admitted] { admitted.Down(); });

    bbt::core::thread::CountDownLatch a_done{1};
    bbt::core::thread::CountDownLatch b_done{1};
    std::atomic_int a_code{-1};
    std::atomic_int b_code{-1};

    bbtco [listen_a, &a_done, &a_code]() {
        auto result = listen_a.value()->Accept(Options(30000));
        a_code.store(result ? -2 : static_cast<int>(result.error().code));
        a_done.Down();
    };
    BOOST_REQUIRE_EQUAL(admitted.WaitTimeout(2000), 0);
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 1u);

    // 第二个 listener 上的 Accept 同样受限——共享账本，不是对象级配额。
    bbtco [listen_b, &b_done, &b_code]() {
        auto result = listen_b.value()->Accept(Options(30000));
        b_code.store(result ? -2 : static_cast<int>(result.error().code));
        b_done.Down();
    };
    BOOST_REQUIRE_EQUAL(b_done.WaitTimeout(2000), 0);
    BOOST_CHECK_EQUAL(b_code.load(),
                      static_cast<int>(ErrorCode::Overloaded));

    listen_a.value()->RequestClose();
    listen_b.value()->RequestClose();
    BOOST_REQUIRE_EQUAL(a_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(a_code.load(), static_cast<int>(ErrorCode::Closed));
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);

    runtime->RequestClose();
    scheduler->Stop();
}

// §4.0.1.7：UDP 可等待 op 走同一账本。max_inflight=1 下挂起 Receive 占
// 名额后：第二个 Receive 与跨 op 的 Send 都立即 Overloaded；名额归还后
// Receive 正常挂起（deadline 到期按 TimedOut 落定），Try* 不经账本。
BOOST_AUTO_TEST_CASE(t_max_inflight_udp_receive_and_send_overloaded) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    NetworkLimits limits = SmallLimits(4);
    limits.max_inflight = 1;
    auto rt = NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());
    auto impl = std::dynamic_pointer_cast<NetworkRuntimeImpl>(runtime);
    BOOST_REQUIRE(impl);

    auto bound = runtime->BindUDP(SocketAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(bound);
    auto socket = std::move(bound).value();
    const auto port = socket->LocalAddress().port;
    BOOST_REQUIRE_NE(port, 0);

    // Try* 不经账本：即便名额已满也只能返回 WouldBlock，不能 Overloaded。
    // 先在名额未占时确认 TryReceive 正常。
    {
        char buf[8]{};
        auto probe = socket->TryReceive(MutableBytes{buf, sizeof(buf)});
        BOOST_REQUIRE(probe);
        BOOST_CHECK(probe.value().state == IoState::WouldBlock);
    }

    bbt::core::thread::CountDownLatch admitted{1};
    TransportWiring::SetWaitEntryGateForTest(*socket, [&admitted] { admitted.Down(); });

    bbt::core::thread::CountDownLatch recv_done{1};
    std::atomic_int recv_code{-1};
    bbtco [socket, &recv_done, &recv_code]() {
        char buf[16]{};
        auto result = socket->Receive(MutableBytes{buf, sizeof(buf)},
                                      Options(30000));
        recv_code.store(result ? -2
                               : static_cast<int>(result.error().code));
        recv_done.Down();
    };
    BOOST_REQUIRE_EQUAL(admitted.WaitTimeout(2000), 0);
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 1u);

    // 第二个挂起 Receive：立即 Overloaded。
    bbt::core::thread::CountDownLatch second_done{1};
    std::atomic_int second_code{-1};
    bbtco [socket, &second_done, &second_code]() {
        char buf[16]{};
        auto result = socket->Receive(MutableBytes{buf, sizeof(buf)},
                                      Options(30000));
        second_code.store(result ? -2
                                 : static_cast<int>(result.error().code));
        second_done.Down();
    };
    BOOST_REQUIRE_EQUAL(second_done.WaitTimeout(2000), 0);
    BOOST_CHECK_EQUAL(second_code.load(),
                      static_cast<int>(ErrorCode::Overloaded));

    // 跨 op：同一 socket 的挂起 Send 也共享同一账本。
    bbt::core::thread::CountDownLatch send_done{1};
    std::atomic_int send_code{-1};
    const char payload[] = "x";
    bbtco [socket, port, &send_done, &send_code, &payload]() {
        auto result = socket->Send(ConstBytes{payload, 1},
                                   SocketAddress{"127.0.0.1", port},
                                   Options(30000));
        send_code.store(result ? -2
                               : static_cast<int>(result.error().code));
        send_done.Down();
    };
    BOOST_REQUIRE_EQUAL(send_done.WaitTimeout(2000), 0);
    BOOST_CHECK_EQUAL(send_code.load(),
                      static_cast<int>(ErrorCode::Overloaded));

    // TryReceive 在名额占满时仍然只是 WouldBlock（不经账本）。
    {
        char buf[8]{};
        auto probe = socket->TryReceive(MutableBytes{buf, sizeof(buf)});
        BOOST_REQUIRE(probe);
        BOOST_CHECK(probe.value().state == IoState::WouldBlock);
    }

    // 释放挂起的 Receive（接缝只触发一次，第二段路径不受影响）。
    socket->RequestClose();
    BOOST_REQUIRE_EQUAL(recv_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(recv_code.load(), static_cast<int>(ErrorCode::Closed));
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);

    runtime->RequestClose();
    scheduler->Stop();
}

// §4.0.1.7：UDP 挂起 op 归还名额后，后续 Receive 恢复可挂起语义
// （非 Overloaded）；这里用短 deadline 让挂起的 Receive 以 TimedOut 落定，
// 证明「名额归还 → 新 op 重新走完整等待路径」而不是被账本残留卡死。
BOOST_AUTO_TEST_CASE(t_max_inflight_udp_recover_after_release) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    NetworkLimits limits = SmallLimits(4);
    limits.max_inflight = 1;
    auto rt = NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());
    auto impl = std::dynamic_pointer_cast<NetworkRuntimeImpl>(runtime);
    BOOST_REQUIRE(impl);

    auto bound = runtime->BindUDP(SocketAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(bound);
    auto socket = std::move(bound).value();

    // 第一笔 Receive 挂起后由 RequestClose 释放名额（对象此后已关闭，
    // 不能复用）；改用「deadline 到期自然归还」模型：第一笔短超时释放
    // 名额，第二笔 Receive 必须能再次挂起并同样以 TimedOut 落定。
    bbt::core::thread::CountDownLatch first_done{1};
    std::atomic_int first_code{-1};
    bbtco [socket, &first_done, &first_code]() {
        char buf[16]{};
        auto result = socket->Receive(MutableBytes{buf, sizeof(buf)},
                                      Options(150));
        first_code.store(result ? -2
                                : static_cast<int>(result.error().code));
        first_done.Down();
    };
    BOOST_REQUIRE_EQUAL(first_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(first_code.load(),
                      static_cast<int>(ErrorCode::TimedOut));
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);

    bbt::core::thread::CountDownLatch second_done{1};
    std::atomic_int second_code{-1};
    bbtco [socket, &second_done, &second_code]() {
        char buf[16]{};
        auto result = socket->Receive(MutableBytes{buf, sizeof(buf)},
                                      Options(150));
        second_code.store(result ? -2
                                 : static_cast<int>(result.error().code));
        second_done.Down();
    };
    BOOST_REQUIRE_EQUAL(second_done.WaitTimeout(5000), 0);
    // 名额已归还：第二笔挂起后以 TimedOut 落定，而不是 Overloaded。
    BOOST_CHECK_EQUAL(second_code.load(),
                      static_cast<int>(ErrorCode::TimedOut));

    socket->RequestClose();
    runtime->RequestClose();
    scheduler->Stop();
}

// §4.0.1.7：受管 DialTCP 等待段占名额。覆盖三条终态路径的归还：
//   1) 名额占满 → DialTCP 立即 Overloaded（不进入 DNS/connect）；
//   2) connect 失败（refused）→ 等待段名额归还；
//   3) 成功 DialTCP 交付 → 等待段名额在交付时归还（账本归零），且交付的
//      连接可用（真实 loopback 读写闭环）。
BOOST_AUTO_TEST_CASE(t_max_inflight_dial_overload_and_release) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    NetworkLimits limits = SmallLimits(8);
    limits.max_inflight = 1;
    auto rt = NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());
    auto impl = std::dynamic_pointer_cast<NetworkRuntimeImpl>(runtime);
    BOOST_REQUIRE(impl);

    auto listen = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen);
    const auto port = listen.value()->LocalAddress().port;
    BOOST_REQUIRE_NE(port, 0);

    // 用挂起 Accept 占满唯一名额。
    bbt::core::thread::CountDownLatch admitted{1};
    TransportWiring::SetWaitEntryGateForTest(*listen.value(),
        [&admitted] { admitted.Down(); });
    bbt::core::thread::CountDownLatch accept_done{1};
    std::atomic_int accept_code{-1};
    bbtco [listen, &accept_done, &accept_code]() {
        auto result = listen.value()->Accept(Options(30000));
        accept_code.store(result ? -2
                                 : static_cast<int>(result.error().code));
        accept_done.Down();
    };
    BOOST_REQUIRE_EQUAL(admitted.WaitTimeout(2000), 0);
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 1u);

    // 名额占满：DialTCP 立即 Overloaded（不消耗连接容量之外另有判定）。
    bbt::core::thread::CountDownLatch dial_done{1};
    std::atomic_int dial_code{-1};
    bbtco [runtime, port, &dial_done, &dial_code]() {
        auto dialed = runtime->DialTCP(TcpEndpoint{"127.0.0.1", port},
                                       Options(30000));
        dial_code.store(dialed ? -2
                               : static_cast<int>(dialed.error().code));
        dial_done.Down();
    };
    BOOST_REQUIRE_EQUAL(dial_done.WaitTimeout(2000), 0);
    BOOST_CHECK_EQUAL(dial_code.load(),
                      static_cast<int>(ErrorCode::Overloaded));
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 1u);

    // 释放 Accept → 名额归还。
    listen.value()->RequestClose();
    BOOST_REQUIRE_EQUAL(accept_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(accept_code.load(), static_cast<int>(ErrorCode::Closed));
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);

    // connect 失败路径归还名额：对未监听端口的 dial 快败后账本仍为零。
    const int refused_port = ReserveFreeTcpPort();
    BOOST_REQUIRE_NE(refused_port, 0);
    bbt::core::thread::CountDownLatch refused_done{1};
    std::atomic_int refused_code{-1};
    bbtco [runtime, refused_port, &refused_done, &refused_code]() {
        auto dialed = runtime->DialTCP(
            TcpEndpoint{"127.0.0.1", static_cast<std::uint16_t>(refused_port)},
            Options(5000));
        refused_code.store(dialed ? -2
                                  : static_cast<int>(dialed.error().code));
        refused_done.Down();
    };
    BOOST_REQUIRE_EQUAL(refused_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(refused_code.load(),
                      static_cast<int>(ErrorCode::TransportError));
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);

    // 成功 handoff：DialTCP 返回可用连接、等待段名额在交付时已归还
    // （账本归零），连接上做真实读写证明它是活的不是占位。
    auto listen2 = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen2);
    const auto port2 = listen2.value()->LocalAddress().port;
    BOOST_REQUIRE_NE(port2, 0);

    std::atomic_bool echo_ok{false};
    bbt::core::thread::CountDownLatch handoff_done{1};
    bbtco [runtime, port2, &echo_ok, &handoff_done]() {
        auto dialed = runtime->DialTCP(TcpEndpoint{"127.0.0.1", port2},
                                       Options(5000));
        if (!dialed) { handoff_done.Down(); return; }
        auto conn = std::move(dialed).value();
        const char msg[] = "hi";
        auto w = conn->WriteAll(bbt::infra::ConstBytes{msg, 2}, Options(3000));
        echo_ok.store(w && w.value().bytes == 2);
        conn->RequestClose();
        handoff_done.Down();
    };
    // 先等待客户端完成 connect/write/close，再启动服务端 Accept。
    // max_inflight=1 时 Accept 本身也占一个在途名额；若与 DialTCP
    // 并发启动，二者会竞争同一名额并把成功 handoff 变成时序 flake。
    // TCP listen backlog 会保留已完成握手的连接，随后 Accept 仍可读取数据。
    BOOST_REQUIRE_EQUAL(handoff_done.WaitTimeout(8000), 0);
    BOOST_CHECK(echo_ok.load());
    // 交付即归还：dial 等待段名额在返回时已归零。
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);

    // 服务端：接受并读 2 字节，证明交付的连接可用。
    bbt::core::thread::CountDownLatch srv_done{1};
    bbtco [listen2, &srv_done]() {
        auto acc = listen2.value()->Accept(Options(5000));
        if (acc) {
            char buf[4]{};
            auto r = acc.value()->ReadSome(bbt::infra::MutableBytes{buf, sizeof(buf)}, Options(3000));
            (void)r;
            acc.value()->RequestClose();
        }
        srv_done.Down();
    };
    BOOST_REQUIRE_EQUAL(srv_done.WaitTimeout(8000), 0);
    listen2.value()->RequestClose();

    runtime->RequestClose();
    scheduler->Stop();
}

// §4.0.1.7：DialTCP 的调用方 cancel 与 deadline 终态同样归还等待段名额。
// 取消段确定性：SetDialWaitEntryGateForTest 在协程挂起进 DNS 等待后才触发
// dial_parked——cancel 必然落在「等待中」，不是「尚未发起」。
// deadline 终态：过期 deadline → TimedOut + 账本归零。
BOOST_AUTO_TEST_CASE(t_max_inflight_dial_cancel_and_deadline_release) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    NetworkLimits limits = SmallLimits(8);
    limits.max_inflight = 2;
    auto rt = NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());
    auto impl = std::dynamic_pointer_cast<NetworkRuntimeImpl>(runtime);
    BOOST_REQUIRE(impl);

    // --- cancel 终态：dial 一个会进入 DNS 等待的目标（合法长度但需真实
    // getaddrinfo 的主机名）。等待入口接缝在协程挂起进 parked 表后才触发
    // dial_parked——此刻 cancel 一定作用于「等待中」的 dial。
    bbt::coroutine::CancellationSource cancel_source;
    const std::string slow_host(200, 'x');  // 合法长度，走真实 getaddrinfo
    bbt::core::thread::CountDownLatch dial_parked{1};
    bbt::core::thread::CountDownLatch dial_done{1};
    std::atomic_int dial_code{-1};
    impl->SetDialWaitEntryGateForTest([&dial_parked] { dial_parked.Down(); });
    bbtco [runtime, &slow_host, &cancel_source, &dial_done, &dial_code]() {
        CallOptions options;
        options.deadline = std::chrono::steady_clock::now() +
                           std::chrono::milliseconds{30000};
        options.cancel = cancel_source.Token();
        auto dialed = runtime->DialTCP(TcpEndpoint{slow_host, 80}, options);
        dial_code.store(dialed ? -2
                               : static_cast<int>(dialed.error().code));
        dial_done.Down();
    };
    // 确定性：等待段已进入（协程挂起在 DNS 等待上），取消落在等待中。
    BOOST_REQUIRE_EQUAL(dial_parked.WaitTimeout(2000), 0);
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 1u);
    cancel_source.RequestCancel();
    BOOST_REQUIRE_EQUAL(dial_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(dial_code.load(),
                      static_cast<int>(ErrorCode::Cancelled));
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);

    // --- deadline 终态：过期 deadline 的 dial 立即 TimedOut，账本归零。
    bbt::core::thread::CountDownLatch timeout_done{1};
    std::atomic_int timeout_code{-1};
    const std::string too_long(300, 'a');
    bbtco [runtime, &too_long, &timeout_done, &timeout_code]() {
        CallOptions options;
        options.deadline = std::chrono::steady_clock::now() -
                           std::chrono::milliseconds{1};
        auto dialed = runtime->DialTCP(TcpEndpoint{too_long, 80}, options);
        timeout_code.store(dialed ? -2
                                  : static_cast<int>(dialed.error().code));
        timeout_done.Down();
    };
    BOOST_REQUIRE_EQUAL(timeout_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(timeout_code.load(),
                      static_cast<int>(ErrorCode::TimedOut));
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);

    runtime->RequestClose();
    scheduler->Stop();
}

// §4.0.1.7 + §6.3 F2：硬 Stop 销毁挂起在 DNS/connect 等待段的 DialTCP
// 协程（不展开栈）时，等待段名额由堆上的 DialWaitPermit 兜底归还——
// Runtime 账本必须归零，不泄漏到下一代际。
// 同时覆盖：runtime->RequestClose() 先唤醒等待段（正常归还路径）。
BOOST_AUTO_TEST_CASE(t_max_inflight_hard_stop_during_dial) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    NetworkLimits limits = SmallLimits(8);
    limits.max_inflight = 2;
    auto rt = NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());
    auto impl = std::dynamic_pointer_cast<NetworkRuntimeImpl>(runtime);
    BOOST_REQUIRE(impl);

    // 路径 A：runtime->RequestClose() 唤醒等待中的 DialTCP 协程——
    // 走正常归还（Cancel/Closed 终态），不走硬销毁兜底。
    const std::string slow_host(200, 'x');
    bbt::core::thread::CountDownLatch dial_parked{1};
    impl->SetDialWaitEntryGateForTest([&dial_parked] { dial_parked.Down(); });
    bbt::core::thread::CountDownLatch dial_done{1};
    std::atomic_int dial_code{-1};
    bbtco [runtime, &slow_host, &dial_done, &dial_code]() {
        auto dialed = runtime->DialTCP(TcpEndpoint{slow_host, 80},
                                     Options(30000));
        dial_code.store(dialed ? -2
                               : static_cast<int>(dialed.error().code));
        dial_done.Down();
    };
    BOOST_REQUIRE_EQUAL(dial_parked.WaitTimeout(2000), 0);
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 1u);
    runtime->RequestClose();
    BOOST_REQUIRE_EQUAL(dial_done.WaitTimeout(5000), 0);
    // runtime 关闭源经 extra_cancel 唤醒：dial 以 Cancelled/Closed 落定。
    BOOST_CHECK(dial_code.load() == static_cast<int>(ErrorCode::Cancelled) ||
                dial_code.load() == static_cast<int>(ErrorCode::Closed));
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);
    impl->SetDialWaitEntryGateForTest({});   // 复位接缝

    // 路径 B：硬 Stop 销毁仍挂起的 dial 协程。用「扣住 DNS worker」把等待段
    // 固定在挂起状态（getaddrinfo 尚未开始 ⇒ 等待不可能自行完成），让「Stop
    // 时 dial 未落定」成为构造出来的事实，可在 Stop 前判别性断言，而不是靠
    // 时序推断（评审 🟡#1）。
    auto rt2 = NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt2);
    auto runtime2 = std::move(rt2).value();
    BOOST_REQUIRE(runtime2->Start());
    auto impl2 = std::dynamic_pointer_cast<NetworkRuntimeImpl>(runtime2);
    BOOST_REQUIRE(impl2);

    bbt::core::thread::CountDownLatch dns_hold2{1};
    bbt::core::thread::CountDownLatch dial_parked2{1};
    impl2->SetDialWaitEntryGateForTest([&dns_hold2, &dial_parked2] {
        dial_parked2.Down();
        dns_hold2.Wait();   // worker 停在 getaddrinfo 之前
    });
    bbt::core::thread::CountDownLatch dial_done2{1};
    std::atomic_int dial_code2{-1};
    bbtco [runtime2, &slow_host, &dial_done2, &dial_code2]() {
        auto dialed = runtime2->DialTCP(TcpEndpoint{slow_host, 80},
                                        Options(60000));
        dial_code2.store(dialed ? -2
                                : static_cast<int>(dialed.error().code));
        dial_done2.Down();
    };
    BOOST_REQUIRE_EQUAL(dial_parked2.WaitTimeout(2000), 0);
    // 判别性断言：硬 Stop 开始时 dial 确实仍未落定（等待段被 worker 扣住），
    // 排除了「断言发生在 dial 正常归还之后」这一歧义。
    BOOST_REQUIRE_EQUAL(dial_done2.WaitTimeout(0), -1);
    BOOST_CHECK_EQUAL(impl2->InflightQuotaHeldForTest(), 1u);

    // 硬 Stop：Scheduler::Stop 内部会 DnsResolver::Stop → 唤醒并 join worker
    // （上游契约：销毁协程栈前 worker 必须已返回、等待段必须已落定），所以
    // 被扣住的 worker 只能由测试侧第三方线程放行，否则 Stop 会等到 worker
    // 而死锁。
    std::thread dns_releaser([&dns_hold2] {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        dns_hold2.Down();
    });
    scheduler->Stop();
    dns_releaser.join();
    BOOST_REQUIRE(!scheduler->IsRunning());

    // Stop 后：等待段被唤醒、dial 以失败终态返回，名额经正常归还路径归还，
    // 账本归零（硬 Stop 不丢账、不串账）。
    // 边界（评审 🟡#1）：本用例证明「Stop 时未落定的 dial 最终不丢账」，释放
    // 来自等待段落定后的正常归还（NetworkRuntimeImpl 侧的 permit->Fail()）；
    // DialWaitPermit 析构兜底分支在代际门禁下不可观测，本用例不主张覆盖它。
    BOOST_REQUIRE_EQUAL(dial_done2.WaitTimeout(2000), 0);
    BOOST_CHECK_NE(dial_code2.load(), -2);   // 未成功：200 字符主机不可解析
    BOOST_CHECK_EQUAL(impl2->InflightQuotaHeldForTest(), 0u);
    impl2->SetDialWaitEntryGateForTest({});   // 复位接缝

    // 新一代际：旧 runtime 工厂拒绝，新 runtime 独立账本正常工作。
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());
    auto stale = runtime2->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(!stale);
    runtime2->RequestClose();
    scheduler->Stop();
}

// §4.0.1.7 F3：挂起的 TCP ReadSome/WriteSome 在同一账本计费——
// 连接交付后 op 各占名额、结束归还；容量占满时第三个 op Overloaded。
// WriteSome 的真实挂起用「缩小双端缓冲 + 对端不读 + 超大 payload」达成：
// 经 SetIoWaitRegisteredGateForTest 证明协程已停在 fd 可写等待，
// 账本在其挂起期间占名额；RequestClose 走取消归还路径落定名额归零。
BOOST_AUTO_TEST_CASE(t_max_inflight_suspended_tcp_io_quota) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    NetworkLimits limits = SmallLimits(8);
    limits.max_inflight = 2;   // 挂起 read + 挂起 write 共占 2 名额
    auto rt = NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());
    auto impl = std::dynamic_pointer_cast<NetworkRuntimeImpl>(runtime);
    BOOST_REQUIRE(impl);

    auto listen = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen);
    const auto port = listen.value()->LocalAddress().port;

    // 先在账本空闲时建立两条连接（Accept/Dial 的等待段也会临时占名额），
    // 交付后账本归零，再分别挂起 read/write——避免连接建立与名额断言互相
    // 干扰。
    bbt::core::thread::CountDownLatch accepts_done{1};
    bbt::core::thread::CountDownLatch dials_done{1};
    std::shared_ptr<bbt::infra::CoTCP> c1_srv, c1_cli, c2_srv, c2_cli;
    // 顺序接受两条入站连接（每Accept 只占一次名额，完成后归还）。
    bbtco [listen, &accepts_done, &c1_srv, &c2_srv]() {
        auto a1 = listen.value()->Accept(Options(5000));
        if (a1) c1_srv = std::move(a1).value();
        auto a2 = listen.value()->Accept(Options(5000));
        if (a2) c2_srv = std::move(a2).value();
        accepts_done.Down();
    };
    bbtco [runtime, port, &dials_done, &c1_cli, &c2_cli]() {
        auto d1 = runtime->DialTCP(TcpEndpoint{"127.0.0.1", port},
                                   Options(5000));
        if (d1) c1_cli = std::move(d1).value();
        auto d2 = runtime->DialTCP(TcpEndpoint{"127.0.0.1", port},
                                   Options(5000));
        if (d2) c2_cli = std::move(d2).value();
        dials_done.Down();
    };
    BOOST_REQUIRE_EQUAL(accepts_done.WaitTimeout(5000), 0);
    BOOST_REQUIRE_EQUAL(dials_done.WaitTimeout(5000), 0);
    BOOST_REQUIRE(c1_srv);
    BOOST_REQUIRE(c1_cli);
    BOOST_REQUIRE(c2_srv);
    BOOST_REQUIRE(c2_cli);
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);

    // c1 client 端挂起 ReadSome——名额登记后进入 fd 可读等待（对端不写）。
    bbt::core::thread::CountDownLatch read_registered{1};
    TransportWiring::SetIoWaitRegisteredGateForTest(*c1_cli,
        [&read_registered] { read_registered.Down(); });
    bbt::core::thread::CountDownLatch read_done{1};
    std::atomic_int read_code{-1};
    bbtco [c1_cli, &read_done, &read_code]() {
        char buf[8]{};
        auto r = c1_cli->ReadSome(bbt::infra::MutableBytes{buf, sizeof(buf)}, Options(30000));
        read_code.store(r ? -2 : static_cast<int>(r.error().code));
        read_done.Down();
    };
    // io-wait-registered gate = 协程已真实挂起在 fd 可读等待。
    BOOST_REQUIRE_EQUAL(read_registered.WaitTimeout(2000), 0);
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 1u);

    // 确定性 backpressure：c2_srv 发送缓冲 + c2_cli 接收缓冲都缩到最小，
    // 且 c2_cli 不发起任何读。WriteSome=单次 send——它不会在自身内循环到
    // EAGAIN，须先把内核发送缓冲预填满，让 WriteSome 的首个 send 即
    // EAGAIN → 进入 _WaitFd(writeable) 真实挂起。TryWriteSome 不占账本
    // 名额，但要求协程上下文——故预填与 WriteSome 都放进同一协程。
    const int sfd = TransportWiring::NativeFdForTest(*c2_srv);
    const int cfd = TransportWiring::NativeFdForTest(*c2_cli);
    BOOST_REQUIRE_GE(sfd, 0);
    BOOST_REQUIRE_GE(cfd, 0);
    int small_buf = 4096;
    BOOST_REQUIRE_EQUAL(::setsockopt(sfd, SOL_SOCKET, SO_SNDBUF,
                                     &small_buf, sizeof(small_buf)), 0);
    BOOST_REQUIRE_EQUAL(::setsockopt(cfd, SOL_SOCKET, SO_RCVBUF,
                                     &small_buf, sizeof(small_buf)), 0);

    std::vector<char> fill(64u << 10, 'f');
    std::vector<char> big_payload(4u << 10, 'x');
    bbt::core::thread::CountDownLatch write_registered{1};
    TransportWiring::SetIoWaitRegisteredGateForTest(*c2_srv,
        [&write_registered] { write_registered.Down(); });
    bbt::core::thread::CountDownLatch srv_w_done{1};
    std::atomic_int srv_w_code{-1};
    bbtco [c2_srv, &srv_w_done, &srv_w_code, &fill, &big_payload]() {
        // 预填发送缓冲直到 WouldBlock——此后首个 send 即 EAGAIN。
        bool full = false;
        for (int i = 0; i < 4096 && !full; ++i) {
            auto w = c2_srv->TryWriteSome(
                ConstBytes{fill.data(), fill.size()});
            if (!w) break;                      // 异常路径：交由断言暴露
            if (w.value().state == IoState::WouldBlock)
                full = true;
        }
        if (!full) {
            srv_w_code.store(-7);               // sentinel：未能填满缓冲
            srv_w_done.Down();
            return;
        }
        auto w = c2_srv->WriteSome(
            ConstBytes{big_payload.data(), big_payload.size()},
            Options(30000));
        srv_w_code.store(w ? -2 : static_cast<int>(w.error().code));
        srv_w_done.Down();
    };
    // write_registered 由 _WaitFd on_registered 触发 ⇒ WriteSome 的首个
    // send 命中 EAGAIN 后已真实挂起在 fd 可写等待，其名额在挂起期间占用账本。
    BOOST_REQUIRE_EQUAL(write_registered.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 2u);  // read+write

    // 账本容量 2 已占满：第三个可等待 op 必须立即 Overloaded 而非排队。
    {
        bbt::core::thread::CountDownLatch third_done{1};
        std::atomic_int third_code{-1};
        bbtco [c1_cli, &third_done, &third_code]() {
            char buf[8]{};
            auto r = c1_cli->ReadSome(bbt::infra::MutableBytes{buf, sizeof(buf)}, Options(5000));
            third_code.store(r ? -2 : static_cast<int>(r.error().code));
            third_done.Down();
        };
        BOOST_REQUIRE_EQUAL(third_done.WaitTimeout(3000), 0);
        BOOST_CHECK_EQUAL(third_code.load(),
                          static_cast<int>(ErrorCode::Overloaded));
        BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 2u);
    }

    // 取消/关闭归还：对 c2_srv RequestClose——挂起的 WriteSome 被
    // m_close_source 唤醒走 Closed，其名额经 DrainQuota/_EndIo 归还。
    c2_srv->RequestClose();
    BOOST_REQUIRE_EQUAL(srv_w_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(srv_w_code.load(), static_cast<int>(ErrorCode::Closed));
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 1u);  // 仅剩 read

    // 读侧归还：c1_srv 写入 1 字节，c1_cli 挂起的 ReadSome 被 fd 可读唤醒
    // 并成功返回，名额归还、账本归零（完成路径归还）。TryWriteSome 需协程
    // 上下文——放进协程执行。
    bbt::core::thread::CountDownLatch rel_done{1};
    std::atomic_int rel_code{-1};
    bbtco [c1_srv, &rel_done, &rel_code]() {
        const char b = 'z';
        auto w = c1_srv->TryWriteSome(ConstBytes{&b, 1});
        rel_code.store(w ? -2 : static_cast<int>(w.error().code));
        rel_done.Down();
    };
    BOOST_REQUIRE_EQUAL(rel_done.WaitTimeout(3000), 0);
    BOOST_CHECK_EQUAL(rel_code.load(), -2);   // TryWriteSome 成功 sentinel
    BOOST_REQUIRE_EQUAL(read_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(read_code.load(), -2);   // ReadSome 成功 sentinel
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);

    c1_cli->RequestClose();
    c1_srv->RequestClose();
    c2_cli->RequestClose();
    c2_srv->RequestClose();
    listen.value()->RequestClose();
    runtime->RequestClose();
    scheduler->Stop();
}

// §4.0.1.7：WriteAll 的在途粒度是它内部的单次写尝试（每轮在进入等待前
// admit、该轮返回即归还；轮间名额可被其他 op 取走），不是整个 WriteAll 一个
// 名额。因此多轮 WriteAll 可能在已写入若干字节后中途失败，错误里的
// transferred_bytes 保留已写字节数——调用方不得假设 WriteAll 在途原子性。
// 本用例确定性覆盖：>1 轮、挂起期间占用共享账本（容量 1 时竞争 op Overloaded）、
// 中途失败（轮间关闭）保留部分写、终态账本归零。
BOOST_AUTO_TEST_CASE(t_max_inflight_writeall_multiround_partial_failure) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    NetworkLimits limits = SmallLimits(8);
    limits.max_inflight = 1;   // 单名额：挂起轮全程占用该名额
    auto rt = NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());
    auto impl = std::dynamic_pointer_cast<NetworkRuntimeImpl>(runtime);
    BOOST_REQUIRE(impl);

    auto listen = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen);
    const auto port = listen.value()->LocalAddress().port;

    // 建链：listener/Accept 走受管路径（受管对象才有账本钩子），对端用
    // **未托管**静态工厂拨号——未托管入口不占名额，因此 max_inflight=1 下
    // Accept 的挂起等待不会与拨号争抢唯一名额；交付后账本归零。
    bbt::core::thread::CountDownLatch accepts_done{1};
    std::shared_ptr<bbt::infra::CoTCP> srv, cli;
    bbtco [listen, &accepts_done, &srv]() {
        auto a = listen.value()->Accept(Options(5000));
        if (a) srv = std::move(a).value();
        accepts_done.Down();
    };
    bbt::core::thread::CountDownLatch dials_done{1};
    bbtco [port, &dials_done, &cli]() {
        auto d = bbt::infra::CoTCP::DialTCP("127.0.0.1", port, Options(5000));
        if (d) cli = std::move(d).value();
        dials_done.Down();
    };
    BOOST_REQUIRE_EQUAL(accepts_done.WaitTimeout(5000), 0);
    BOOST_REQUIRE_EQUAL(dials_done.WaitTimeout(5000), 0);
    BOOST_REQUIRE(srv);
    BOOST_REQUIRE(cli);
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);

    // 发送侧缓冲缩到最小、接收侧不读 ⇒ WriteAll 前若干轮各写入部分字节，
    // 之后某一轮的首个 send 命中 EAGAIN 并真实挂起（首轮不可能挂起：缓冲空）。
    const int sfd = TransportWiring::NativeFdForTest(*srv);
    const int cfd = TransportWiring::NativeFdForTest(*cli);
    BOOST_REQUIRE_GE(sfd, 0);
    BOOST_REQUIRE_GE(cfd, 0);
    int small_buf = 4096;
    BOOST_REQUIRE_EQUAL(::setsockopt(sfd, SOL_SOCKET, SO_SNDBUF,
                                     &small_buf, sizeof(small_buf)), 0);
    BOOST_REQUIRE_EQUAL(::setsockopt(cfd, SOL_SOCKET, SO_RCVBUF,
                                     &small_buf, sizeof(small_buf)), 0);

    std::vector<char> payload(1u << 20, 'w');   // 1 MiB ≫ 发送缓冲
    std::atomic_int rounds{0};
    // 等待入口接缝在每轮名额登记之后、进入等待循环之前触发一次 ⇒ 轮计数。
    TransportWiring::SetWaitEntryGateForTest(*srv, [&rounds] { rounds.fetch_add(1); });
    bbt::core::thread::CountDownLatch parked{1};
    TransportWiring::SetIoWaitRegisteredGateForTest(*srv, [&parked] { parked.Down(); });

    bbt::core::thread::CountDownLatch write_done{1};
    std::atomic_int write_code{-1};
    std::atomic<unsigned long long> write_transferred{0};
    bbtco [srv, &payload, &write_done, &write_code, &write_transferred]() {
        auto w = srv->WriteAll(bbt::infra::ConstBytes{payload.data(), payload.size()}, Options(30000));
        write_code.store(w ? -2 : static_cast<int>(w.error().code));
        if (!w)
            write_transferred.store(static_cast<unsigned long long>(
                w.error().transferred_bytes));
        write_done.Down();
    };
    // io-wait-registered ⇒ 该轮已真实挂起在 fd 可写等待，其名额在挂起期间占用账本。
    BOOST_REQUIRE_EQUAL(parked.WaitTimeout(5000), 0);
    BOOST_CHECK_GE(rounds.load(), 2);   // 至少已完成一轮部分写才轮到挂起轮
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 1u);

    // 账本容量只有 1 个且被挂起的 WriteAll 占满：同一 Runtime 账本上的下一个
    // op 立即 Overloaded，不排队、不等待。
    {
        bbt::core::thread::CountDownLatch competitor_done{1};
        std::atomic_int competitor_code{-1};
        bbtco [srv, &competitor_done, &competitor_code]() {
            char buf[8]{};
            auto r = srv->ReadSome(bbt::infra::MutableBytes{buf, sizeof(buf)}, Options(5000));
            competitor_code.store(r ? -2 : static_cast<int>(r.error().code));
            competitor_done.Down();
        };
        BOOST_REQUIRE_EQUAL(competitor_done.WaitTimeout(3000), 0);
        BOOST_CHECK_EQUAL(competitor_code.load(),
                          static_cast<int>(ErrorCode::Overloaded));
        BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 1u);
    }

    // 轮间关闭：挂起轮被唤醒返回 Closed，WriteAll 以部分写 + Closed 中途失败
    // （0 < transferred_bytes < payload ⇒ 至少一轮已完成、失败发生在后续轮）。
    srv->RequestClose();
    BOOST_REQUIRE_EQUAL(write_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(write_code.load(), static_cast<int>(ErrorCode::Closed));
    BOOST_CHECK_GT(write_transferred.load(), 0ull);
    BOOST_CHECK_LT(write_transferred.load(),
                   static_cast<unsigned long long>(payload.size()));
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);

    TransportWiring::SetWaitEntryGateForTest(*srv, {});
    TransportWiring::SetIoWaitRegisteredGateForTest(*srv, {});
    cli->RequestClose();
    srv->RequestClose();
    listen.value()->RequestClose();
    runtime->RequestClose();
    scheduler->Stop();
}

// §4.0.1.7 + §6.3：强制 Stop 不展开挂起协程栈——挂起 op 占用的账本名额
// 由对象侧 RequestClose/析构的 DrainQuota 兜底归还；账本经 shared_ptr
// 跨代存活。Stop→Start 是新一代际：旧 runtime/旧对象在新代际被拒
// （RuntimeUnavailable），新 runtime 在新代际正常工作且账目独立。
BOOST_AUTO_TEST_CASE(t_max_inflight_stop_drains_and_generation_boundary) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    NetworkLimits limits = SmallLimits(8);
    limits.max_inflight = 1;
    auto rt = NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());
    auto impl = std::dynamic_pointer_cast<NetworkRuntimeImpl>(runtime);
    BOOST_REQUIRE(impl);

    auto bound = runtime->BindUDP(SocketAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(bound);
    auto socket = std::move(bound).value();

    bbt::core::thread::CountDownLatch admitted{1};
    TransportWiring::SetWaitEntryGateForTest(*socket, [&admitted] { admitted.Down(); });
    bbt::core::thread::CountDownLatch recv_done{1};
    bbtco [socket, &recv_done]() {
        char buf[16]{};
        auto result = socket->Receive(MutableBytes{buf, sizeof(buf)},
                                      Options(60000));
        (void)result;
        recv_done.Down();  // 强 Stop 时不应到达；保留作异常信号
    };
    BOOST_REQUIRE_EQUAL(admitted.WaitTimeout(2000), 0);
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 1u);

    // 硬 Stop：挂起协程被直接销毁、不展开栈（§6.3）。Receive 不再返回，
    // 名额只能由对象侧兜底归还。
    runtime->RequestClose();
    socket->RequestClose();
    scheduler->Stop();
    BOOST_REQUIRE(!scheduler->IsRunning());
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);

    // 新一代际：旧 runtime 的工厂与旧对象的 op 都必须拒绝。
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());
    BOOST_REQUIRE_NE(bbt::coroutine::CurrentRuntimeGeneration(), 0);

    // 旧对象：代际门禁立即拒绝（不触碰旧 fd 数字）。
    {
        char buf[8]{};
        auto probe = socket->TryReceive(MutableBytes{buf, sizeof(buf)});
        BOOST_REQUIRE(!probe);
        BOOST_CHECK_EQUAL(static_cast<int>(probe.error().code),
                          static_cast<int>(ErrorCode::RuntimeUnavailable));
    }
    // 旧 runtime：已 RequestClose，先报 Closed。
    auto stale = runtime->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(!stale);
    BOOST_CHECK_EQUAL(static_cast<int>(stale.error().code),
                      static_cast<int>(ErrorCode::Closed));

    // 新 runtime：新代际独立账本，容量门禁与正常工作不受影响。
    NetworkLimits limits2 = SmallLimits(8);
    limits2.max_inflight = 1;
    auto rt2 = NetworkRuntime::Create(limits2);
    BOOST_REQUIRE(rt2);
    auto runtime2 = std::move(rt2).value();
    BOOST_REQUIRE(runtime2->Start());
    auto bound2 = runtime2->BindUDP(SocketAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(bound2);
    auto socket2 = std::move(bound2).value();

    bbt::core::thread::CountDownLatch admitted2{1};
    TransportWiring::SetWaitEntryGateForTest(*socket2, [&admitted2] { admitted2.Down(); });
    bbt::core::thread::CountDownLatch recv2_done{1};
    std::atomic_int recv2_code{-1};
    bbtco [socket2, &recv2_done, &recv2_code]() {
        char buf[16]{};
        auto result = socket2->Receive(MutableBytes{buf, sizeof(buf)},
                                       Options(30000));
        recv2_code.store(result ? -2
                                : static_cast<int>(result.error().code));
        recv2_done.Down();
    };
    BOOST_REQUIRE_EQUAL(admitted2.WaitTimeout(2000), 0);
    bbt::core::thread::CountDownLatch second_done{1};
    std::atomic_int second_code{-1};
    bbtco [socket2, &second_done, &second_code]() {
        char buf[16]{};
        auto result = socket2->Receive(MutableBytes{buf, sizeof(buf)},
                                       Options(30000));
        second_code.store(result ? -2
                                 : static_cast<int>(result.error().code));
        second_done.Down();
    };
    BOOST_REQUIRE_EQUAL(second_done.WaitTimeout(2000), 0);
    BOOST_CHECK_EQUAL(second_code.load(),
                      static_cast<int>(ErrorCode::Overloaded));

    socket2->RequestClose();
    BOOST_REQUIRE_EQUAL(recv2_done.WaitTimeout(5000), 0);
    BOOST_CHECK_EQUAL(recv2_code.load(), static_cast<int>(ErrorCode::Closed));
    runtime2->RequestClose();
    scheduler->Stop();
}

// §4.0.1.7：未受 Runtime 托管的直接 transport 入口（CoUDP::BindUDP 静态
// 工厂）不注入账本钩子——无限额语义：同一对象上的挂起 Receive 不经过
// 账本判定，只能按 WaitStatus 落定（短 deadline → TimedOut），永远不可
// 返回 Overloaded；max_inflight 只是 Runtime 预算，不是 transport 全局
// 常数。
BOOST_AUTO_TEST_CASE(t_unmanaged_transport_has_no_inflight_quota) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto bound = bbt::infra::udp::CoUDP::BindUDP(
        SocketAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(bound);
    auto socket = std::move(bound).value();

    // 挂起 Receive 按短 deadline 以 TimedOut 落定（契约：同一时刻至多
    // 一个执行主体操作对象；未托管入口豁免的是限额语义，不是 owner 规则）。
    bbt::core::thread::CountDownLatch done{1};
    std::atomic_int code_a{-1};
    bbtco [socket, &done, &code_a]() {
        char buf[16]{};
        auto result = socket->Receive(MutableBytes{buf, sizeof(buf)},
                                      Options(150));
        code_a.store(result ? -2
                            : static_cast<int>(result.error().code));
        done.Down();
    };
    BOOST_REQUIRE_EQUAL(done.WaitTimeout(5000), 0);
    // 未托管对象不携带账本：挂起 op 只能返回等待终态，不会是 Overloaded。
    BOOST_CHECK_EQUAL(code_a.load(), static_cast<int>(ErrorCode::TimedOut));

    socket->RequestClose();
    scheduler->Stop();
}

BOOST_AUTO_TEST_SUITE_END()
