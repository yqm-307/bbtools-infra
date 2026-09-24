#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <cerrno>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <chrono>
#include <string>
#include <thread>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>
#include <bbt/infra/CoTCP.hpp>
#include <bbt/infra/CoUDP.hpp>
#include <bbt/infra/NetworkRuntime.hpp>

using bbt::infra::CallOptions;
using bbt::infra::CloseStatus;
using bbt::infra::ErrorCode;
using bbt::infra::NetworkLimits;
using bbt::infra::NetworkRuntime;
using bbt::infra::SocketAddress;
using bbt::infra::TcpEndpoint;
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
        auto read = accepted.value()->ReadSome(request, sizeof(request), Options());
        if (!read || read.value() != 5) { done.Down(); return; }
        const char response[] = "world";
        auto write = accepted.value()->WriteAll(response, 5, Options());
        server_ok.store(write && write.value() == 5);
        accepted.value()->RequestClose();
        done.Down();
    };

    bbtco [runtime, listen, port, &client_ok, &done]() {
        auto dialed = runtime->DialTCP(TcpEndpoint{"127.0.0.1", port}, Options());
        if (!dialed) { done.Down(); return; }
        auto client = dialed.value();
        const char request[] = "hello";
        auto write = client->WriteAll(request, 5, Options());
        char response[5]{};
        auto read = client->ReadSome(response, sizeof(response), Options());
        client_ok.store(write && write.value() == 5 && read && read.value() == 5 &&
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
        auto read = accepted.value()->ReadSome(request, sizeof(request), Options());
        server_ok.store(read && read.value() == 5);
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
        auto write = dialed.value()->WriteAll(request, 5, Options());
        client_ok.store(write && write.value() == 5);
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

BOOST_AUTO_TEST_SUITE_END()
