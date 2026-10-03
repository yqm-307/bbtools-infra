// Issue #55 (F2) / #56 (H1) / #57 (H2)：HTTP client 失败路径物理收口、
// resolver 后台寿命边界、composed connect 串行化的决定性回归。
//
// 契约要点（与三张票的 R/S 对应）：
//  - #55 F2：任何失败出口（resolve 失败 / 发起异常 / 写读失败 / 等待失败）
//    都必须在 op 离开 owner m_ops（UnregisterOp→on_unregister）之前，先
//    物理释放所拥有的 socket/body/parser/flat_buffer（Abort→Finish 次序）。
//    取证不是 finished/IsClosed 标签，而是「在 op 离开 m_ops 的当刻」由
//    on_unregister 内直读 socket.is_open()/request.body().size()/serializer
//    是否仍在——这些状态由 Abort() 在同一 io 域线程先于 UnregisterOp 写下。
//  - #56 H1：resolver 后台 getaddrinfo 不因 cancel() 返回而同步停止，本
//    适配器不加阻塞 join 层；靠解析 handler 捕获的 self 保活 op，晚到完成项
//    经 !op_armed 早退。Close 返回时本 op 自有可控资源（socket/载荷）已释放，
//    与「后台查询状态」分开记录。
//  - #57 H2：多 endpoint 连接不用 range composed operation，改在 IoGate 内
//    逐条 close/open/async_connect，使其与跨线程 Abort/Close 串行；多
//    endpoint 语义与错误分类不变。
//
// 失败注入与门控都走内部 seam（io_fault / on_resolve / on_connect_attempt /
// on_read_armed / SetEndpointsForTest），不靠 sleep 猜时序，不把模拟 DNS
// 当真实服务可用性验收。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

#include "detail/IoSupport.hpp"
#include "http/HttpClientImpl.hpp"
#include "http/HttpIoEngine.hpp"

using namespace bbt::infra;
using tcp = boost::asio::ip::tcp;

namespace {

constexpr int kBudgetMs = 20000;

bool WaitUntil(const std::function<bool()>& pred, int budget_ms = kBudgetMs) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(budget_ms);
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::yield();
    }
    return true;
}

NetworkLimits MakeLimits(std::size_t max_conn, std::size_t max_inflight) {
    NetworkLimits limits{};
    limits.max_connections  = max_conn;
    limits.max_inflight     = max_inflight;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = 16 * 1024 * 1024;
    limits.incoming_timeout = std::chrono::milliseconds{5000};
    return limits;
}

// 注入式配额账本：模拟 owner 预算（上限 cap）。admit/release 次数与当前
// 占用都可观测，用于证明「恰好一次」与「物理收口前不复用」。
struct FakeBudget {
    explicit FakeBudget(std::size_t cap_) : cap(cap_) {}
    std::size_t      cap;
    std::mutex       mtx;
    std::size_t      held{0};
    std::atomic_int  admits{0};
    std::atomic_int  releases{0};

    result<void> Admit() {
        admits.fetch_add(1);
        std::lock_guard<std::mutex> lk(mtx);
        if (held >= cap)
            return result<void>::err(MakeError(
                ErrorCode::Overloaded, "fake budget: cap exceeded"));
        ++held;
        return result<void>::ok();
    }
    void Release() noexcept {
        releases.fetch_add(1);
        std::lock_guard<std::mutex> lk(mtx);
        if (held > 0)
            --held;
    }
    std::size_t Held() {
        std::lock_guard<std::mutex> lk(mtx);
        return held;
    }
};

// 直接构造 HttpClientImpl + 独立 HttpIoEngine（测试侧可 TryPost / 注入 seam）。
std::pair<std::shared_ptr<http_detail::HttpClientImpl>,
          std::shared_ptr<http_detail::HttpIoEngine>>
NewImplClient(const std::shared_ptr<FakeBudget>& budget,
              const NetworkLimits& limits) {
    auto engine = std::make_shared<http_detail::HttpIoEngine>(limits);
    BOOST_REQUIRE(engine->Start());
    auto info = bbt::infra::detail::NewObjectInfo("infra.http_client");
    BOOST_REQUIRE(info);
    auto impl = std::make_shared<http_detail::HttpClientImpl>(
        engine, std::move(info).value());
    impl->SetQuotaHooks(
        [budget] { return budget->Admit(); },
        [budget] { budget->Release(); });
    return {impl, engine};
}

// op 离开 owner m_ops 当刻（on_unregister 内）读取的物理状态 + 反登记次数。
struct LeaveProbe {
    std::atomic_int unregistered{0};
    std::atomic_int socket_open_at_leave{-1};   // -1=未观测
    std::atomic_int body_bytes_at_leave{-1};
    std::atomic_int serializer_at_leave{-1};
};

// 单连接裸对端：accept 一条连接后交给 fn(c)；fn 不负责 close——本结构接受
// 返回后统一 close（fn 若已设 SO_LINGER(0)，此处即注入 RST）。析构 join。
struct OneShotPeer {
    std::uint16_t port{0};
    std::thread   th;
    explicit OneShotPeer(std::function<void(int)> fn,
                         int accept_timeout_ms = 8000) {
        int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
        BOOST_REQUIRE(lfd >= 0);
        int one = 1;
        ::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        timeval tv{accept_timeout_ms / 1000,
                   (accept_timeout_ms % 1000) * 1000};
        ::setsockopt(lfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        sockaddr_in a{};
        a.sin_family      = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        a.sin_port        = htons(0);
        BOOST_REQUIRE(::bind(lfd, reinterpret_cast<sockaddr*>(&a),
                             sizeof(a)) == 0);
        BOOST_REQUIRE(::listen(lfd, 4) == 0);
        socklen_t len = sizeof(a);
        BOOST_REQUIRE(::getsockname(lfd, reinterpret_cast<sockaddr*>(&a),
                                    &len) == 0);
        port = ntohs(a.sin_port);
        th   = std::thread([lfd, fn = std::move(fn)]() mutable {
            int c = ::accept(lfd, nullptr, nullptr);
            if (c >= 0) {
                if (fn) fn(c);
                ::close(c);
            }
            ::close(lfd);
        });
    }
    ~OneShotPeer() { if (th.joinable()) th.join(); }
};

// 裸对端：accept → 读掉请求 → 回一条合法 200 → close。用于「连接成功」类出口。
void ServeOneOk(int c) {
    char buf[4096];
    (void)::recv(c, buf, sizeof(buf), 0);
    const char resp[] = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi";
    (void)::send(c, resp, sizeof(resp) - 1, MSG_NOSIGNAL);
}

// 直接驱动 ClientOp 的接线（与 Request 相同：admit → RegisterOp → post driver）。
// on_unregister 记为「离开 m_ops 当刻」状态（裸指针捕获避免自引用环）。
struct OpRun {
    std::shared_ptr<FakeBudget>                     budget;
    std::shared_ptr<http_detail::HttpClientImpl>    impl;
    std::shared_ptr<http_detail::HttpIoEngine>      engine;
    std::shared_ptr<LeaveProbe>                     probe;
    std::shared_ptr<http_detail::ClientOp>          op;
};

using Driver = std::function<void(const std::shared_ptr<http_detail::ClientOp>&)>;
using Configure = std::function<void(http_detail::ClientOp&)>;

OpRun DriveOp(const std::shared_ptr<FakeBudget>& budget,
              const NetworkLimits& limits,
              const std::string& method,
              const std::string& body,
              const Driver& driver,
              const Configure& configure = nullptr) {
    OpRun run;
    run.budget = budget;
    std::tie(run.impl, run.engine) = NewImplClient(budget, limits);
    run.probe = std::make_shared<LeaveProbe>();
    BOOST_REQUIRE(budget->Admit());
    auto op = std::make_shared<http_detail::ClientOp>(run.impl, run.engine);
    op->waiter = bbt::coroutine::sync::CoWaiter::Create();
    op->request.version(11);
    op->request.method_string(method);
    op->request.target("/");
    op->request.set(boost::beast::http::field::host, "127.0.0.1");
    op->request.body() = body;
    op->request.prepare_payload();
    http_detail::ClientOp* raw = op.get();
    auto probe = run.probe;
    op->on_unregister = [raw, probe, budget] {
        // 同一 io 域线程：Abort()（若已执行）的写入在此可见。
        probe->socket_open_at_leave.store(raw->socket.is_open() ? 1 : 0,
                                          std::memory_order_release);
        probe->body_bytes_at_leave.store(
            static_cast<int>(raw->request.body().size()),
            std::memory_order_release);
        probe->serializer_at_leave.store(
            raw->serializer.has_value() ? 1 : 0, std::memory_order_release);
        probe->unregistered.fetch_add(1);
        budget->Release();
    };
    if (configure)
        configure(*op);
    run.op = op;
    BOOST_REQUIRE(run.impl->RegisterOp(op));
    BOOST_REQUIRE(run.engine->TryPost([op, driver] { driver(op); }));
    return run;
}

// 失败出口的共用不变量：op 离开 m_ops 当刻资源已物理释放，名额恰好归还一次。
void ExpectReleasedBeforeUnregister(const OpRun& run,
                                    const char* exit_name) {
    BOOST_REQUIRE_MESSAGE(
        WaitUntil([&] { return run.op->finished.load(); }, 25000),
        std::string(exit_name) + ": op never settled");
    BOOST_REQUIRE_MESSAGE(
        WaitUntil([&] {
            return run.probe->unregistered.load() == 1 &&
                   run.budget->Held() == 0;
        }, 15000),
        std::string(exit_name) + ": never unregistered");
    BOOST_REQUIRE_MESSAGE(run.op->outcome.has_value(),
                          std::string(exit_name) + ": no outcome");
    BOOST_REQUIRE_MESSAGE(!*run.op->outcome,
                          std::string(exit_name) + ": failure became success");
    BOOST_CHECK_MESSAGE(run.probe->socket_open_at_leave.load() == 0,
                        std::string(exit_name) + ": socket open at leave");
    BOOST_CHECK_MESSAGE(run.probe->body_bytes_at_leave.load() == 0,
                        std::string(exit_name) + ": body retained at leave");
    BOOST_CHECK_MESSAGE(run.probe->serializer_at_leave.load() == 0,
                        std::string(exit_name) + ": serializer retained");
    BOOST_CHECK_EQUAL(run.budget->releases.load(), 1);
    BOOST_CHECK(!run.op->socket.is_open());
    BOOST_CHECK_EQUAL(run.op->request.body().size(), 0u);
    BOOST_CHECK_EQUAL(run.op->buffer.size(), 0u);
    BOOST_CHECK_EQUAL(run.op->inflight.load(), 0);
    BOOST_CHECK_EQUAL(run.budget->Held(), 0u);
    run.impl->Close();
    BOOST_CHECK(run.impl->IsClosed());
}

std::uint16_t DeadPort() {
    int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(lfd >= 0);
    sockaddr_in a{};
    a.sin_family      = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port        = htons(0);
    BOOST_REQUIRE(::bind(lfd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0);
    socklen_t len = sizeof(a);
    BOOST_REQUIRE(::getsockname(lfd, reinterpret_cast<sockaddr*>(&a), &len) == 0);
    const std::uint16_t port = ntohs(a.sin_port);
    ::close(lfd);
    return port;
}

} // namespace

BOOST_AUTO_TEST_SUITE(http_failure_close)

BOOST_AUTO_TEST_CASE(t_start_scheduler) {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 2;
    cfg->m_cfg_stack_size        = 256 * 1024;
    g_scheduler->Start(bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(g_scheduler->IsInitialized());
}

// ---------------------------------------------------------------------------
// #55 F2：七类失败出口。每条走真实路径/注入，断言「Abort→Finish 且名额/
// 资源在离开 m_ops 前已归还」。
// ---------------------------------------------------------------------------

// E1 resolve 失败：非法数值 host（getaddrinfo 直接失败，不依赖外部 DNS）。
BOOST_AUTO_TEST_CASE(t_f2_e1_resolve_failure_releases_before_unregister) {
    auto budget = std::make_shared<FakeBudget>(1);
    auto run = DriveOp(budget, MakeLimits(8, 1), "GET", "",
                       [](const std::shared_ptr<http_detail::ClientOp>& op) {
                           op->Begin("256.256.256.256", 80);
                       });
    ExpectReleasedBeforeUnregister(run, "E1 resolve-fail");
    BOOST_CHECK(run.op->connect_attempts == 0);
}

// E1b resolve 发起异常：Begin 内 async_resolve 之前注入同步异常。
BOOST_AUTO_TEST_CASE(t_f2_e1b_resolve_initiate_exception_releases) {
    auto budget = std::make_shared<FakeBudget>(1);
    auto run = DriveOp(budget, MakeLimits(8, 1), "GET", "",
        [](const std::shared_ptr<http_detail::ClientOp>& op) {
            op->Begin("127.0.0.1", 80);
        },
        [](http_detail::ClientOp& op) {
            op.io_fault = [](const char* site) {
                if (std::strcmp(site, "resolve") == 0)
                    throw std::runtime_error("inject resolve initiate");
            };
        });
    ExpectReleasedBeforeUnregister(run, "E1b resolve-initiate");
    BOOST_CHECK(run.op->outcome->error().message.find("resolve initiate") !=
                std::string::npos);
}

// E2 connect 发起异常：#57 H2 的单次 connect 发起处注入异常。
BOOST_AUTO_TEST_CASE(t_f2_e2_connect_initiate_exception_releases) {
    auto budget = std::make_shared<FakeBudget>(1);
    const std::uint16_t dead = DeadPort();
    auto run = DriveOp(budget, MakeLimits(8, 1), "GET", "",
        [dead](const std::shared_ptr<http_detail::ClientOp>& op) {
            std::vector<tcp::endpoint> eps{
                tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), dead)};
            op->SetEndpointsForTest(
                tcp::resolver::results_type::create(eps.begin(), eps.end(),
                                                    "127.0.0.1", "0"));
            op->StartConnectForTest();
        },
        [](http_detail::ClientOp& op) {
            op.io_fault = [](const char* site) {
                if (std::strcmp(site, "connect") == 0)
                    throw std::runtime_error("inject connect initiate");
            };
        });
    ExpectReleasedBeforeUnregister(run, "E2 connect-initiate");
    BOOST_CHECK(run.op->outcome->error().message.find("connect initiate") !=
                std::string::npos);
}

// E3 OnConnect 内部写发起异常：连接成功后 serializer 就绪处注入异常。
BOOST_AUTO_TEST_CASE(t_f2_e3_write_initiate_exception_releases) {
    OneShotPeer peer([](int c) { ServeOneOk(c); });
    auto budget = std::make_shared<FakeBudget>(1);
    auto run = DriveOp(budget, MakeLimits(8, 1), "GET", std::string(64, 'x'),
        [&peer](const std::shared_ptr<http_detail::ClientOp>& op) {
            std::vector<tcp::endpoint> eps{
                tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), peer.port)};
            op->SetEndpointsForTest(
                tcp::resolver::results_type::create(eps.begin(), eps.end(),
                                                    "127.0.0.1", "0"));
            op->StartConnectForTest();
        },
        [](http_detail::ClientOp& op) {
            op.io_fault = [](const char* site) {
                if (std::strcmp(site, "write") == 0)
                    throw std::runtime_error("inject write initiate");
            };
        });
    ExpectReleasedBeforeUnregister(run, "E3 write-initiate");
    BOOST_CHECK(run.op->outcome->error().message.find("write initiate") !=
                std::string::npos);
    BOOST_CHECK(run.op->connect_attempts == 1);
}

// E4 PumpWrite 非 EAGAIN 失败：对端 accept 后立即 RST 且不读请求，8MiB
// 请求体远超内核发送缓冲 ⇒ 写侧失败（读从未武装）。
BOOST_AUTO_TEST_CASE(t_f2_e4_write_failure_releases_before_unregister) {
    std::atomic_bool read_armed{false};
    OneShotPeer peer([](int c) {
        linger lg{1, 0};          // 返回后 OneShotPeer close ⇒ RST
        ::setsockopt(c, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
    });
    auto budget = std::make_shared<FakeBudget>(1);
    auto run = DriveOp(budget, MakeLimits(8, 1), "POST",
        std::string(8 * 1024 * 1024, 'x'),
        [&peer](const std::shared_ptr<http_detail::ClientOp>& op) {
            op->Begin("127.0.0.1", peer.port);
        },
        [&read_armed](http_detail::ClientOp& op) {
            op.on_read_armed = [&read_armed] {
                read_armed.store(true, std::memory_order_release);
            };
        });
    ExpectReleasedBeforeUnregister(run, "E4 write-fail");
    BOOST_CHECK(!read_armed.load(std::memory_order_acquire));
}

// E5 ArmWait 发起失败：连接成功后，读就绪等待武装处注入异常（写完成/或
// 写就绪等待都会经过同一 ArmWait 出口）。
BOOST_AUTO_TEST_CASE(t_f2_e5_armwait_initiate_exception_releases) {
    OneShotPeer peer([](int c) { ServeOneOk(c); });
    auto budget = std::make_shared<FakeBudget>(1);
    auto run = DriveOp(budget, MakeLimits(8, 1), "GET", std::string(32, 'y'),
        [&peer](const std::shared_ptr<http_detail::ClientOp>& op) {
            std::vector<tcp::endpoint> eps{
                tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), peer.port)};
            op->SetEndpointsForTest(
                tcp::resolver::results_type::create(eps.begin(), eps.end(),
                                                    "127.0.0.1", "0"));
            op->StartConnectForTest();
        },
        [](http_detail::ClientOp& op) {
            op.io_fault = [](const char* site) {
                if (std::strcmp(site, "armwait") == 0)
                    throw std::runtime_error("inject armwait initiate");
            };
        });
    ExpectReleasedBeforeUnregister(run, "E5 armwait-initiate");
    BOOST_CHECK(run.op->outcome->error().message.find("initiate failed") !=
                std::string::npos);
}

// E6a OnIoReady 错误：连接并写出请求后对端 RST，读就绪等待以错误落定。
BOOST_AUTO_TEST_CASE(t_f2_e6a_onio_error_releases_before_unregister) {
    std::atomic_bool read_armed{false};
    OneShotPeer peer([&read_armed](int c) {
        for (int i = 0; i < 8000 &&
                        !read_armed.load(std::memory_order_acquire); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        linger lg{1, 0};          // RST：读就绪等待以错误落定
        ::setsockopt(c, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
    });
    auto budget = std::make_shared<FakeBudget>(1);
    auto run = DriveOp(budget, MakeLimits(8, 1), "GET", std::string(64, 'z'),
        [&peer](const std::shared_ptr<http_detail::ClientOp>& op) {
            op->Begin("127.0.0.1", peer.port);
        },
        [&read_armed](http_detail::ClientOp& op) {
            op.on_read_armed = [&read_armed] {
                read_armed.store(true, std::memory_order_release);
            };
        });
    ExpectReleasedBeforeUnregister(run, "E6a onio-error");
}

// E6b OnIoReady 内部异常：读就绪成功后进入泵处注入异常。
BOOST_AUTO_TEST_CASE(t_f2_e6b_io_pump_exception_releases) {
    OneShotPeer peer([](int c) {
        // 发部分响应头让读就绪成功，但不构成完整消息；OnIoReady 成功后
        // 在进入读泵处注入异常。
        const char partial[] = "HTTP/1.1 200 OK\r\nContent-Length: 50\r\n\r\npart";
        (void)::send(c, partial, sizeof(partial) - 1, MSG_NOSIGNAL);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    });
    auto budget = std::make_shared<FakeBudget>(1);
    auto run = DriveOp(budget, MakeLimits(8, 1), "GET", std::string(32, 'q'),
        [&peer](const std::shared_ptr<http_detail::ClientOp>& op) {
            op->Begin("127.0.0.1", peer.port);
        },
        [](http_detail::ClientOp& op) {
            op.io_fault = [](const char* site) {
                if (std::strcmp(site, "io_pump") == 0)
                    throw std::runtime_error("inject io pump");
            };
        });
    ExpectReleasedBeforeUnregister(run, "E6b io-pump");
    BOOST_CHECK(run.op->outcome->error().message.find("io pump failed") !=
                std::string::npos);
}

// E7 PumpRead 失败：对端发「头完整、体不足」后优雅关闭（FIN），client 读泵
// 收到 EOF，按不完整响应收口。
BOOST_AUTO_TEST_CASE(t_f2_e7_pumpread_failure_releases) {
    OneShotPeer peer([](int c) {
        const char partial[] =
            "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nabc";  // 体不足
        (void)::send(c, partial, sizeof(partial) - 1, MSG_NOSIGNAL);
        // 不设 SO_LINGER ⇒ 返回后 close 走 FIN（非 RST）：client 读到 EOF。
    });
    auto budget = std::make_shared<FakeBudget>(1);
    auto run = DriveOp(budget, MakeLimits(8, 1), "GET", std::string(32, 'r'),
        [&peer](const std::shared_ptr<http_detail::ClientOp>& op) {
            op->Begin("127.0.0.1", peer.port);
        });
    ExpectReleasedBeforeUnregister(run, "E7 pumpread-fail");
}

// ---------------------------------------------------------------------------
// #56 H1：resolver 后台寿命边界。
// ---------------------------------------------------------------------------

// S1/S3：解析完成项被门控（backend 查询已完成、回调处理尚未推进）时与
// Close 竞争。Close 返回当刻：本 op 自有可控资源（socket/载荷）已释放、
// 名额已归还；后台解析完成项仍未被投递（op 由 handler 捕获的 self 保活）。
// 放行后晚回调只消费空壳：不再触碰资源、不重复清理。
BOOST_AUTO_TEST_CASE(t_h1_resolve_completion_gated_vs_close) {
    OneShotPeer peer([](int c) { ServeOneOk(c); });
    auto budget = std::make_shared<FakeBudget>(1);

    std::atomic_bool resolve_entered{false};
    std::atomic_bool release_resolve{false};

    auto run = DriveOp(budget, MakeLimits(8, 1), "GET", std::string(64, 'h'),
        [&peer](const std::shared_ptr<http_detail::ClientOp>& op) {
            op->Begin("127.0.0.1", peer.port);
        },
        [&](http_detail::ClientOp& op) {
            op.on_resolve = [&] {
                // 解析完成项已在处理中：backend 查询已完成。门控在此刻，
                // 尚未取 IoGate，Close 可从其它线程完成物理收口。
                resolve_entered.store(true, std::memory_order_release);
                while (!release_resolve.load(std::memory_order_acquire))
                    std::this_thread::yield();
            };
        });

    // 等解析完成项真正进入门控（backend 已完成、回调尚未推进）。
    BOOST_REQUIRE(WaitUntil(
        [&] { return resolve_entered.load(std::memory_order_acquire); }, 15000));
    // 此时本 op 仍在 m_ops：未收口、名额仍占；resolve 阶段 socket 尚未 open。
    BOOST_CHECK(!run.op->socket.is_open());
    BOOST_CHECK(!run.op->finished.load());
    BOOST_CHECK_EQUAL(run.probe->unregistered.load(), 0);
    BOOST_CHECK_EQUAL(budget->Held(), 1u);

    // Close 在调用线程同步收口：Abort 记账被中止的解析完成项 + 关 socket +
    // 释放载荷；名额经 UnregisterOp 归还。返回即可控资源已释放。
    run.impl->Close();
    BOOST_CHECK(run.impl->IsClosed());
    BOOST_CHECK(!run.op->socket.is_open());
    BOOST_CHECK_EQUAL(run.op->request.body().size(), 0u);
    BOOST_CHECK_EQUAL(run.op->inflight.load(), 0);
    BOOST_CHECK_EQUAL(budget->Held(), 0u);
    BOOST_CHECK_EQUAL(budget->releases.load(), 1);
    BOOST_CHECK_EQUAL(run.probe->unregistered.load(), 1);
    // 后台状态单独记录：解析完成项仍停在门控内（晚回调尚未被处理），
    // 与「本 op 可控资源已释放」是两笔独立事实。
    BOOST_CHECK(!release_resolve.load(std::memory_order_acquire));

    // 放行查询完成项：晚回调经 !op_armed 早退，不触碰已释放资源、不重复清理。
    release_resolve.store(true, std::memory_order_release);
    BOOST_REQUIRE(WaitUntil([&] { return run.probe->unregistered.load() == 1; }));
    std::this_thread::yield();
    BOOST_CHECK_EQUAL(run.probe->unregistered.load(), 1);   // 未二次反登记
    BOOST_CHECK_EQUAL(budget->releases.load(), 1);          // 未二次归还
    BOOST_CHECK(!run.op->socket.is_open());
}

// S2：解析失败与 Close 交错。无门控，二者竞争；无论谁先落定，终态唯一、
// 名额恰好归还一次、无重复清理。
BOOST_AUTO_TEST_CASE(t_h1_resolve_failure_vs_close_single_settlement) {
    auto budget = std::make_shared<FakeBudget>(1);
    auto run = DriveOp(budget, MakeLimits(8, 1), "GET", "",
        [](const std::shared_ptr<http_detail::ClientOp>& op) {
            op->Begin("256.256.256.256", 80);   // 解析必失败
        });
    // 立即从另一线程 Close，与解析失败竞争。
    std::thread closer([&] { run.impl->Close(); });
    closer.join();
    BOOST_REQUIRE(WaitUntil([&] { return run.op->finished.load(); }, 15000));
    BOOST_REQUIRE(WaitUntil([&] {
        return run.probe->unregistered.load() == 1 && budget->Held() == 0;
    }, 15000));
    BOOST_CHECK_EQUAL(run.probe->unregistered.load(), 1);
    BOOST_CHECK_EQUAL(budget->releases.load(), 1);
    BOOST_REQUIRE(run.op->outcome.has_value());
    // 失败语义保留：resolve 失败 → Unavailable；若 Close 抢先则 Closed。
    BOOST_CHECK(!*run.op->outcome);
    const auto code = run.op->outcome->error().code;
    BOOST_CHECK(code == ErrorCode::Unavailable || code == ErrorCode::Closed);
    BOOST_CHECK(!run.op->socket.is_open());
    BOOST_CHECK_EQUAL(run.op->inflight.load(), 0);
    BOOST_CHECK(run.impl->IsClosed());
}

// ---------------------------------------------------------------------------
// #57 H2：composed connect 串行化。
// ---------------------------------------------------------------------------

// S1：两 endpoint，第一个可控失败（死端口），第二个成功；断言回退到第二
// 条、请求成功、前一次资源不残留、名额恰好归还一次。
BOOST_AUTO_TEST_CASE(t_h2_two_endpoints_first_fails_second_succeeds) {
    OneShotPeer peer([](int c) { ServeOneOk(c); });
    const std::uint16_t dead = DeadPort();
    auto budget = std::make_shared<FakeBudget>(1);
    auto run = DriveOp(budget, MakeLimits(8, 1), "GET", "",
        [&peer, dead](const std::shared_ptr<http_detail::ClientOp>& op) {
            std::vector<tcp::endpoint> eps{
                tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), dead),
                tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), peer.port)};
            op->SetEndpointsForTest(
                tcp::resolver::results_type::create(eps.begin(), eps.end(),
                                                    "127.0.0.1", "0"));
            op->StartConnectForTest();
        });
    BOOST_REQUIRE_MESSAGE(
        WaitUntil([&] { return run.op->finished.load(); }, 20000),
        "two-endpoint connect never settled");
    BOOST_REQUIRE(WaitUntil([&] {
        return run.probe->unregistered.load() == 1 && budget->Held() == 0;
    }, 15000));
    // 成功：200，且回退恰好一次（两次 connect 尝试）。
    BOOST_REQUIRE(run.op->outcome.has_value());
    BOOST_REQUIRE(*run.op->outcome);
    BOOST_CHECK_EQUAL(run.op->outcome->value().status, 200u);
    BOOST_CHECK_EQUAL(run.op->connect_attempts, 2);
    BOOST_CHECK_EQUAL(run.probe->unregistered.load(), 1);
    BOOST_CHECK_EQUAL(budget->releases.load(), 1);
    BOOST_CHECK(!run.op->socket.is_open());
    run.impl->Close();
    BOOST_CHECK(run.impl->IsClosed());
}

// S2：门控「连接内部步骤」（同一 IoGate 内），并发 Close。Close 必须等该步
// 骤完成后再串行收口：中间 socket 不重开、终态一次、返回时资源已收口。
BOOST_AUTO_TEST_CASE(t_h2_internal_step_gated_vs_close_no_reopen) {
    OneShotPeer peer([](int c) { ServeOneOk(c); });
    auto budget = std::make_shared<FakeBudget>(1);

    std::atomic_bool step_entered{false};
    std::atomic_bool release_step{false};

    auto run = DriveOp(budget, MakeLimits(8, 1), "GET", std::string(64, 'g'),
        [&peer](const std::shared_ptr<http_detail::ClientOp>& op) {
            std::vector<tcp::endpoint> eps{
                tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), peer.port)};
            op->SetEndpointsForTest(
                tcp::resolver::results_type::create(eps.begin(), eps.end(),
                                                    "127.0.0.1", "0"));
            op->StartConnectForTest();
        },
        [&](http_detail::ClientOp& op) {
            op.on_connect_attempt = [&] {
                step_entered.store(true, std::memory_order_release);
                while (!release_step.load(std::memory_order_acquire))
                    std::this_thread::yield();
            };
        });

    BOOST_REQUIRE(WaitUntil(
        [&] { return step_entered.load(std::memory_order_acquire); }, 15000));
    // Close 在另一线程发起：内部步骤持 IoGate，Close 排队等它完成，绝不与
    // 该步骤并发触碰 socket。
    std::thread closer([&] { run.impl->Close(); });
    // 让 Close 有机会进入等待；随后放行内部步骤。
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    release_step.store(true, std::memory_order_release);
    closer.join();

    BOOST_CHECK(run.impl->IsClosed());
    BOOST_REQUIRE(WaitUntil([&] { return run.op->finished.load(); }, 15000));
    BOOST_REQUIRE(WaitUntil([&] {
        return run.probe->unregistered.load() == 1 && budget->Held() == 0;
    }, 15000));
    // 终态一次、恰好一次反登记/归还；内部步骤只发起了一次 connect（未重开）。
    BOOST_CHECK_EQUAL(run.probe->unregistered.load(), 1);
    BOOST_CHECK_EQUAL(budget->releases.load(), 1);
    BOOST_CHECK_EQUAL(run.op->connect_attempts, 1);
    BOOST_CHECK(!run.op->socket.is_open());
    BOOST_CHECK_EQUAL(run.op->inflight.load(), 0);
}

// S3：回退处发起异常（第二条 endpoint 发起时注入），与 Close 竞争；断言原
// 错误语义保留、资源账本安全、无重复释放。
BOOST_AUTO_TEST_CASE(t_h2_fallback_initiate_exception_ledger_safe) {
    const std::uint16_t dead_a = DeadPort();
    const std::uint16_t dead_b = DeadPort();
    auto budget = std::make_shared<FakeBudget>(1);
    auto run = DriveOp(budget, MakeLimits(8, 1), "GET", "",
        [dead_a, dead_b](const std::shared_ptr<http_detail::ClientOp>& op) {
            std::vector<tcp::endpoint> eps{
                tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), dead_a),
                tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), dead_b)};
            op->SetEndpointsForTest(
                tcp::resolver::results_type::create(eps.begin(), eps.end(),
                                                    "127.0.0.1", "0"));
            op->StartConnectForTest();
        },
        [](http_detail::ClientOp& op) {
            auto attempts = std::make_shared<std::atomic_int>(0);
            op.io_fault = [attempts](const char* site) {
                if (std::strcmp(site, "connect") == 0 &&
                    attempts->fetch_add(1) == 1) {
                    // 第二条 endpoint 的发起处注入异常。
                    throw std::runtime_error("inject fallback connect");
                }
            };
        });
    ExpectReleasedBeforeUnregister(run, "H2 fallback-initiate");
    BOOST_CHECK(run.op->connect_attempts == 2);
    BOOST_CHECK(run.op->outcome->error().message.find("connect initiate") !=
                std::string::npos);
}

// S4：无独立 loop 的合法调用模式——Close 从非 io（普通）线程同步调用即
// 完成物理收口，不要求调用方在 Close 返回后额外 Tick。
// 边界声明：infra 的 io 域由 coroutine 共享事件循环线程驱动；本用例证明
// 的是「Close 自身不依赖调用线程投递/推进」，非「无调度器运行」。
BOOST_AUTO_TEST_CASE(t_h2_close_from_plain_thread_no_caller_tick) {
    std::atomic_bool read_armed{false};
    OneShotPeer peer([&read_armed](int c) {
        (void)c;
        // accept 后不响应，压住 client 的读就绪等待。
        for (int i = 0; i < 8000 &&
                        !read_armed.load(std::memory_order_acquire); ++i)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    });
    auto budget = std::make_shared<FakeBudget>(1);
    auto run = DriveOp(budget, MakeLimits(8, 1), "GET", std::string(32, 't'),
        [&peer](const std::shared_ptr<http_detail::ClientOp>& op) {
            op->Begin("127.0.0.1", peer.port);
        },
        [&read_armed](http_detail::ClientOp& op) {
            op.on_read_armed = [&read_armed] {
                read_armed.store(true, std::memory_order_release);
            };
        });
    BOOST_REQUIRE(WaitUntil(
        [&] { return read_armed.load(std::memory_order_acquire); }, 15000));

    std::thread closer([&] { run.impl->Close(); });
    closer.join();
    BOOST_CHECK(run.impl->IsClosed());
    BOOST_REQUIRE(WaitUntil([&] {
        return run.probe->unregistered.load() == 1 && budget->Held() == 0;
    }, 15000));
    BOOST_CHECK(!run.op->socket.is_open());
    BOOST_CHECK_EQUAL(run.op->request.body().size(), 0u);
    BOOST_CHECK_EQUAL(budget->releases.load(), 1);
}

BOOST_AUTO_TEST_SUITE_END()
