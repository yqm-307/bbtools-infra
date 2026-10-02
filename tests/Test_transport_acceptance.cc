#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

// Issue #31 收尾验收剩余项：DNS 在途到期（非调用前已过期）。
//
// 现有 deadline 用例用的都是「调用前已过期」的 deadline
// （Test_tcp_runtime_factory::t_dial_hostname_deadline_bounded 等），不是
// 「DNS 已在途后到期」。本用例用 DNS worker 扣持接缝构造真正在途的解析等待，
// 再让 deadline 到期；对 worker 晚到的解析结果只做间接验证（名额不重复归还、
// DNS 通道仍可用），不直接观察 worker 侧丢弃分支。
//
// 断言口径（不伪造结论）：worker 扣持接缝只保证「放行前解析等待确定在途」；
// 放行之后 worker 晚到结果是否被丢弃没有外部可观察面，故不断言该丢弃分支被
// 走到，只断言名额不重复归还与 DNS 通道仍可用。
//
// 新契约（进程寿命运行时）：同一硬停期的「owner 封口紧邻 Stop 后收口不变量」
// 用例已随 Stop 语义删除；本进程内只初始化一次 runtime，靠进程边界隔离用例，
// 结束前显式 Close() 并断言物理收口。

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/TransportRuntime.hpp>

#include "detail/TransportWiring.hpp"     // src 内部装配面：DNS 扣持接缝
#include "http/NetworkRuntimeImpl.hpp"    // 协议 owner 的 dial 等待入口接缝

using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;
using bbt::infra::CallOptions;
using bbt::infra::NetworkLimits;
using bbt::infra::NetworkRuntime;
using bbt::infra::TcpEndpoint;

using bbt::infra::http_detail::NetworkRuntimeImpl;

namespace {

// 运行时只初始化一次（函数局部静态保证线程安全且恰好一次）。
void EnsureRuntime() {
    static const bool initialized = [] {
        auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
        if (!scheduler->IsInitialized())
            scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
        return scheduler->IsInitialized();
    }();
    BOOST_REQUIRE(initialized);
}

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

// 断言失败（REQUIRE 抛异常）时也必须放行 worker，否则用例退出时 worker 仍被
// 扣住。声明位置必须晚于被测对象，析构时先放行。
struct DnsHoldReleaser {
    std::shared_ptr<DnsHold> hold;
    ~DnsHoldReleaser() { hold->Release(); }
};

} // namespace

/* ---------------------------------------------------------------------------
 * DNS 在途到期与晚到结果（间接口径）：
 *    worker 被扣持 → 解析等待确定在途 → deadline 到期（不是调用前已过期）
 *    → TimedOut + 名额归还；随后放行 worker，间接验证晚到结果不重复归还名额，
 *    且 DNS 通道仍可用（后续 dial 成功）。worker 侧「丢弃晚到结果」的分支没有
 *    外部可观察面，本用例不断言其被走到。
 * ------------------------------------------------------------------------- */
BOOST_AUTO_TEST_CASE(t_dial_dns_inflight_deadline_and_late_result) {
    EnsureRuntime();

    auto hold = std::make_shared<DnsHold>();
    DnsHoldReleaser release_guard{hold};

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
    runtime->Close();
    BOOST_CHECK(runtime->IsClosed());
    BOOST_CHECK_EQUAL(impl->InflightQuotaHeldForTest(), 0u);
}
