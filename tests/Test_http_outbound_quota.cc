// Issue #37：HTTP 出站连接与在途配额的原子接纳及物理归还回归。
//
// 与 transport #32 的 CoTCP/CoUDP 在途门禁是不同入口：本套件只覆盖
// HTTP 出站路径（HttpClientImpl::Request → ClientOp），不复用传输层
// socket 计数，也不依赖 handler 端 max_inflight。
//
// 覆盖验收点：
//   t_overloaded_when_inflight_full     — max_inflight=1 时第二个在途
//                                          请求确定性 Overloaded，服务端
//                                          不接到超额请求（red/green 主线）。
//   t_overloaded_when_connections_full  — max_connections=1 单独隔离验证，
//                                          不依赖 handler 限流。
//   t_quota_shared_across_clients       — 同一 Runtime 下多个 HttpClient
//                                          共享同一预算（owner 作用域）。
//   t_released_on_normal_completion     — 正常完成后名额归还，可再接纳。
//   t_released_on_peer_failure          — 对端断开/畸形响应等发起失败后
//                                          名额归还。
//   t_released_on_deadline              — 本地 deadline 到点后名额归还；
//                                          归还发生在等待完成项物理收口后
//                                          才允许再接纳，不在调用方超时返回
//                                          的瞬间。
//   t_released_on_cancel                — 取消路径名额归还。
//   t_released_on_client_close          — client RequestClose 中止在途
//                                          请求后名额归还。
//   t_no_reuse_before_physical_close    — 名额在 op 物理收口（UnregisterOp，
//                                          finished && inflight==0）前不
//                                          复用：裸对端 accept 后持有不响应
//                                          压住 client 真实 async_read 完成
//                                          项；逻辑超时返回后先经 io 域
//                                          FIFO 探针确认 Abort 已执行、
//                                          OnRead 完成项仍在途，此时真实
//                                          TryAdmitHttpRequest 必须拒绝
//                                          （Overloaded），随后该完成项
//                                          落定归还名额、新物理连接可建立。
//   t_unregister_exactly_once           — 并发 Finish/IoAsyncDone 与重复
//                                          UnregisterOp 对同一 op 恰好归还
//                                          一次（erase 守门回归）。
//   t_register_op_failure_releases      — admit 成功后 RegisterOp 拒绝
//                                          （io_dead）时名额经 guard 归还，
//                                          不泄漏。
//   t_admit_failure_no_leak_no_double_release — release 复制先于 admit：
//                                          admit 拒绝/抛异常时 guard 不误
//                                          归还，覆盖 m_release 复制窗口两侧。
//   t_runtime_close_releases            — Runtime RequestClose 中止在途后
//                                          名额归还，不依赖协程栈析构。
//
// 同步纪律：跨线程/协程一律原子标志 + WaitUntil（带总预算）与
// CountDownLatch；handler 内的等待用 bbtco_sleep 轮询真实事件标志，
// 不靠 sleep 假设时序。所有等待都有超时上限，不会无限挂起。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Hook.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/sync/CompletionSignal.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

// Issue #37 复审回归：直接驱动 impl 层（注入配额钩子、io 域顺序探针、
// 并发收口），需要 src/ 内部头——经 CMake target_include_directories 引入，
// 仅本测试目标使用，不进公开契约面。
#include "detail/IoSupport.hpp"
#include "http/HttpClientImpl.hpp"
#include "http/HttpIoEngine.hpp"

using namespace bbt::infra;
using bbt::coroutine::Deadline;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;
using Latch  = bbt::core::thread::CountDownLatch;
using Signal = bbt::coroutine::CompletionSignal;

namespace {

constexpr std::size_t kTestMaxBody = 64 * 1024;
constexpr int         kBudgetMs    = 15000;

// ---------------------------------------------------------------------------
// 协程/同步助手
// ---------------------------------------------------------------------------

bool Spawn(std::function<void()> fn) {
    bool ok = false;
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        std::move(fn), ok);
    return ok;
}

template <class F>
bool RunInCoroutine(F&& f, int budget_ms = kBudgetMs) {
    Latch done{1};
    bool succ = Spawn(
        [fn = std::forward<F>(f), &done]() mutable {
            fn();
            done.Down();
        });
    if (!succ)
        return false;
    return done.WaitTimeout(budget_ms) == 0;
}

CallOptions DefaultOptions(int budget_ms = 10000) {
    CallOptions opt;
    opt.deadline = std::chrono::steady_clock::now() +
                   std::chrono::milliseconds(budget_ms);
    return opt;
}

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

bool Close(const std::shared_ptr<ICoCloseable>& obj) {
    obj->RequestClose();
    auto done = std::make_shared<Latch>(1);
    auto status = std::make_shared<std::atomic<int>>(-1);
    if (!Spawn([obj, done, status] {
            status->store(static_cast<int>(
                obj->WaitClosed(std::chrono::steady_clock::now() +
                                    std::chrono::seconds(8),
                                {})));
            done->Down();
        }))
        return false;
    return done->WaitTimeout(9000) == 0 &&
           status->load() == static_cast<int>(CloseStatus::Closed);
}

// ---------------------------------------------------------------------------
// 可控门闩服务端：handler 进入时 arrived 计数 +1，每个请求持有独立
// CompletionSignal，测试侧统一放行。auto_release=true 时 handler 立即
// 放行（用于「名额归还后可再接纳」的后续请求，不再压门闩）。
// 服务端 Runtime 给足容量，瓶颈只可能出在 client 出站侧——这是接纳
// 缺口测试，不是服务端压测。
// ---------------------------------------------------------------------------
struct Gate {
    std::mutex                                mutex;
    std::vector<std::shared_ptr<Signal>>      signals;
    std::atomic_int                           active{0};
    std::atomic_int                           total{0};
    std::atomic_bool                          auto_release{false};
    std::shared_ptr<Latch>                    arrived;   // 由用例按 N 构造

    // 返回该请求专属 signal；auto_release 模式下立即 Complete（不压门闩）。
    std::shared_ptr<Signal> Hold() {
        auto sig = std::make_shared<Signal>();
        {
            std::lock_guard<std::mutex> lk(mutex);
            signals.push_back(sig);
        }
        ++active;
        ++total;
        if (arrived)
            arrived->Down();
        if (auto_release.load())
            sig->Complete();
        return sig;
    }
    void ReleaseAll() {
        std::lock_guard<std::mutex> lk(mutex);
        for (auto& s : signals)
            s->Complete();
    }
};

// 每个请求独立 signal：释放只对已到达者生效，未到达的不占名额。
HttpHandler MakeGateHandler(const std::shared_ptr<Gate>& gate) {
    return [gate](IncomingCallContext, HttpRequest) {
        auto sig = gate->Hold();
        bbt::coroutine::WaitOptions wait;
        wait.deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(10);
        sig->Wait(wait);
        --gate->active;
        return result<HttpResponse>::ok(HttpResponse{200, {}, "ok"});
    };
}

// 立即返回 200 的轻量 handler（归还路径用例）。
result<HttpResponse> OkHandler(IncomingCallContext, HttpRequest) {
    return result<HttpResponse>::ok(HttpResponse{200, {}, "ok"});
}

NetworkLimits MakeLimits(std::size_t max_conn, std::size_t max_inflight) {
    NetworkLimits limits{};
    limits.max_connections  = max_conn;
    limits.max_inflight     = max_inflight;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = kTestMaxBody;
    limits.incoming_timeout = std::chrono::milliseconds{5000};
    return limits;
}

// 一次出站请求（协程内）；返回 result 便于断言 Overloaded/Closed 等。
result<HttpResponse> CallApi(const std::shared_ptr<HttpClient>& client,
                             const std::string& url, CallOptions options) {
    std::optional<result<HttpResponse>> out;
    bool ran = RunInCoroutine(
        [&] { out.emplace(client->Request(HttpRequest{"GET", url, {}, {}},
                                          options)); });
    if (!ran)
        return result<HttpResponse>::err(MakeError(
            ErrorCode::InternalError, "test: coroutine did not run"));
    return std::move(*out);
}

// 在独立 server runtime 上起一个可控服务端；返回 (runtime, server, url 前缀)。
// handler_factory 接收本 ServerSide 的 gate，产出真正的 HttpHandler——
// 这样门闩/计数与 handler 天然绑定到同一 gate。
struct ServerSide {
    std::shared_ptr<NetworkRuntime> rt;
    std::shared_ptr<HttpServer>     server;
    std::string                     base_url;
    std::shared_ptr<Gate>           gate;
};

ServerSide StartServer(std::function<HttpHandler(std::shared_ptr<Gate>)>
                           handler_factory,
                       std::size_t expect_arrivals = 0) {
    ServerSide s;
    s.gate = std::make_shared<Gate>();
    if (expect_arrivals > 0)
        s.gate->arrived = std::make_shared<Latch>(expect_arrivals);
    auto sr = NetworkRuntime::Create(MakeLimits(64, 64));
    BOOST_REQUIRE(sr);
    s.rt = std::move(sr).value();
    BOOST_REQUIRE(s.rt->Start());
    auto listen = s.rt->ListenHttp({"127.0.0.1", 0}, handler_factory(s.gate));
    BOOST_REQUIRE(listen);
    s.server = std::move(listen).value();
    s.base_url = "http://127.0.0.1:" +
                 std::to_string(s.server->LocalAddress().port);
    return s;
}

std::shared_ptr<NetworkRuntime> StartClientRuntime(NetworkLimits limits) {
    auto cr = NetworkRuntime::Create(limits);
    BOOST_REQUIRE(cr);
    auto rt = std::move(cr).value();
    BOOST_REQUIRE(rt->Start());
    return rt;
}

std::shared_ptr<HttpClient> NewClient(
    const std::shared_ptr<NetworkRuntime>& rt) {
    auto c = rt->CreateHttpClient();
    BOOST_REQUIRE(c);
    return std::move(c).value();
}

// 名额归还发生在 client op 物理收口（IoAsyncDone→UnregisterOp→
// on_unregister），与服务端 handler 退出存在调度竞态。本 helper
// 轮询真实 Request 直至被接纳：Overloaded 表示名额仍占（完成项未
// 落定），是正确行为继续等；其他错误说明名额已归还但链路失败。
// 返回最终 result；admitted 输出是否真实被接纳。
result<HttpResponse> CallApiUntilAdmitted(
    const std::shared_ptr<HttpClient>& client, const std::string& url,
    CallOptions options, bool& admitted, int budget_ms = 10000) {
    admitted = false;
    result<HttpResponse> res = result<HttpResponse>::err(
        MakeError(ErrorCode::InternalError, "not run"));
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(budget_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        res = CallApi(client, url, options);
        if (res) { admitted = true; break; }
        if (res.error().code != ErrorCode::Overloaded)
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return res;
}

// 在协程里发一个会被门闩压住的请求；hold_result 承载返回值（测试结束
// 前必须读，否则 optional 析构）。
void SpawnHeld(const std::shared_ptr<HttpClient>& client,
               const std::string& url,
               std::shared_ptr<std::optional<result<HttpResponse>>> out,
               std::shared_ptr<std::atomic_bool> out_ready,
               CallOptions options = {}) {
    if (!options.deadline.time_since_epoch().count())
        options = DefaultOptions(20000);
    bool succ = Spawn([client, url, out, out_ready, options] {
        out->emplace(client->Request(HttpRequest{"GET", url, {}, {}}, options));
        out_ready->store(true, std::memory_order_release);
    });
    BOOST_REQUIRE(succ);
}

} // namespace

BOOST_AUTO_TEST_SUITE(http_outbound_quota)

BOOST_AUTO_TEST_CASE(t_begin_start_scheduler) {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 2;
    cfg->m_cfg_stack_size        = 256 * 1024;
    g_scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(g_scheduler->IsRunning());
}

// 验收主线：上限=1 时第二个在途请求确定性 Overloaded，服务端不接到
// 超额请求（red：原实现会让 4 个全部到达）。
BOOST_AUTO_TEST_CASE(t_overloaded_when_inflight_full) {
    auto server = StartServer([](std::shared_ptr<Gate> g){ return MakeGateHandler(g); }, /*expect_arrivals=*/0);
    auto rt = StartClientRuntime(MakeLimits(/*max_conn=*/8, /*max_inflight=*/1));
    auto client = NewClient(rt);
    const std::string url = server.base_url + "/x";

    auto out1 = std::make_shared<std::optional<result<HttpResponse>>>();
    auto rdy1 = std::make_shared<std::atomic_bool>(false);
    SpawnHeld(client, url, out1, rdy1);
    // 第一个请求已被服务端接纳（handler 进入、被门闩压住）。
    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 1; }));

    // 第二个请求：在途名额已满，必须立即 Overloaded，服务端不接到。
    auto res2 = CallApi(client, url, DefaultOptions());
    BOOST_REQUIRE(!res2);
    BOOST_CHECK(res2.error().code == ErrorCode::Overloaded);
    // 服务端仍只见到 1 个并发。
    BOOST_CHECK_EQUAL(server.gate->active.load(), 1);
    BOOST_CHECK_EQUAL(server.gate->total.load(), 1);

    // 放行第一个：它应正常完成，名额归还后第三个请求能再被接纳。
    // 先开 auto_release，让第三个请求的 handler 立即放行（不再压门闩），
    // 再统一 Complete 已压住的 signal。
    server.gate->auto_release.store(true);
    server.gate->ReleaseAll();
    BOOST_REQUIRE(WaitUntil([&] {
        return rdy1->load(std::memory_order_acquire);
    }));
    BOOST_REQUIRE((*out1).has_value());
    BOOST_REQUIRE((*out1).value());   // result<HttpResponse> 是 ok
    BOOST_CHECK_EQUAL((*out1).value().value().status, 200u);

    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 0; }));
    auto res3 = CallApi(client, url, DefaultOptions());
    BOOST_REQUIRE(res3);
    BOOST_CHECK_EQUAL(res3.value().status, 200u);

    BOOST_REQUIRE(Close(client));
    server.server->StopAccepting();
    BOOST_REQUIRE(Close(server.rt));
    BOOST_REQUIRE(Close(rt));
}

// 单独隔离 max_connections：max_inflight 给足，max_connections=1。
BOOST_AUTO_TEST_CASE(t_overloaded_when_connections_full) {
    auto server = StartServer([](std::shared_ptr<Gate> g){ return MakeGateHandler(g); });
    auto rt = StartClientRuntime(MakeLimits(/*max_conn=*/1, /*max_inflight=*/8));
    auto client = NewClient(rt);
    const std::string url = server.base_url + "/x";

    auto out1 = std::make_shared<std::optional<result<HttpResponse>>>();
    auto rdy1 = std::make_shared<std::atomic_bool>(false);
    SpawnHeld(client, url, out1, rdy1);
    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 1; }));

    auto res2 = CallApi(client, url, DefaultOptions());
    BOOST_REQUIRE(!res2);
    BOOST_CHECK(res2.error().code == ErrorCode::Overloaded);
    BOOST_CHECK_EQUAL(server.gate->total.load(), 1);

    server.gate->ReleaseAll();
    BOOST_REQUIRE(WaitUntil([&] {
        return rdy1->load(std::memory_order_acquire);
    }));
    BOOST_REQUIRE((*out1).has_value());
    BOOST_REQUIRE((*out1).value());

    BOOST_REQUIRE(Close(client));
    server.server->StopAccepting();
    BOOST_REQUIRE(Close(server.rt));
    BOOST_REQUIRE(Close(rt));
}

// owner 作用域：同一 Runtime 下两个独立 HttpClient 共享同一预算；
// 第二个 client 的请求同样计进 owner 总量。
BOOST_AUTO_TEST_CASE(t_quota_shared_across_clients) {
    auto server = StartServer([](std::shared_ptr<Gate> g){ return MakeGateHandler(g); });
    auto rt = StartClientRuntime(MakeLimits(/*max_conn=*/1, /*max_inflight=*/1));
    auto client_a = NewClient(rt);
    auto client_b = NewClient(rt);
    const std::string url = server.base_url + "/x";

    auto out1 = std::make_shared<std::optional<result<HttpResponse>>>();
    auto rdy1 = std::make_shared<std::atomic_bool>(false);
    SpawnHeld(client_a, url, out1, rdy1);
    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 1; }));

    // 另一个 client 的请求也必须被 owner 配额拒绝。
    auto res2 = CallApi(client_b, url, DefaultOptions());
    BOOST_REQUIRE(!res2);
    BOOST_CHECK(res2.error().code == ErrorCode::Overloaded);
    BOOST_CHECK_EQUAL(server.gate->total.load(), 1);

    server.gate->ReleaseAll();
    BOOST_REQUIRE(WaitUntil([&] {
        return rdy1->load(std::memory_order_acquire);
    }));
    BOOST_REQUIRE((*out1).has_value());
    BOOST_REQUIRE((*out1).value());

    BOOST_REQUIRE(Close(client_a));
    BOOST_REQUIRE(Close(client_b));
    server.server->StopAccepting();
    BOOST_REQUIRE(Close(server.rt));
    BOOST_REQUIRE(Close(rt));
}

// 正常完成路径归还：上限=1 时连续多个串行请求全部成功。
BOOST_AUTO_TEST_CASE(t_released_on_normal_completion) {
    auto server = StartServer([](std::shared_ptr<Gate>){ return HttpHandler(OkHandler); });
    auto rt = StartClientRuntime(MakeLimits(/*max_conn=*/1, /*max_inflight=*/1));
    auto client = NewClient(rt);
    const std::string url = server.base_url + "/ok";

    for (int i = 0; i < 4; ++i) {
        auto res = CallApi(client, url, DefaultOptions());
        BOOST_REQUIRE(res);
        BOOST_CHECK_EQUAL(res.value().status, 200u);
    }

    BOOST_REQUIRE(Close(client));
    server.server->StopAccepting();
    BOOST_REQUIRE(Close(server.rt));
    BOOST_REQUIRE(Close(rt));
}

// 发起失败路径归还：对端立即断开（connect 成功但无响应），失败后
// 名额归还，后续请求可再被接纳。
BOOST_AUTO_TEST_CASE(t_released_on_peer_failure) {
    // 裸对端：accept 后立即 close，客户端见到 TransportError。
    std::atomic_bool peer_done{false};
    int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(lfd >= 0);
    int one = 1;
    ::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = htons(0);
    BOOST_REQUIRE(::bind(lfd, reinterpret_cast<sockaddr*>(&addr),
                         sizeof(addr)) == 0);
    BOOST_REQUIRE(::listen(lfd, 4) == 0);
    socklen_t len = sizeof(addr);
    BOOST_REQUIRE(::getsockname(lfd, reinterpret_cast<sockaddr*>(&addr),
                                &len) == 0);
    const std::uint16_t port = ntohs(addr.sin_port);
    BOOST_REQUIRE(Spawn([lfd, &peer_done] {
        int c = ::accept(lfd, nullptr, nullptr);
        if (c >= 0)
            ::close(c);
        ::close(lfd);
        peer_done.store(true);
    }));
    const std::string url =
        "http://127.0.0.1:" + std::to_string(port) + "/x";

    auto rt = StartClientRuntime(MakeLimits(/*max_conn=*/1, /*max_inflight=*/1));
    auto client = NewClient(rt);

    auto res1 = CallApi(client, url, DefaultOptions());
    BOOST_REQUIRE(!res1);   // 失败（断连/无响应）
    BOOST_REQUIRE(WaitUntil([&] { return peer_done.load(); }));

    // 名额已归还：换一个正常服务端，第二个请求能成功。
    auto server = StartServer([](std::shared_ptr<Gate>){ return HttpHandler(OkHandler); });
    auto res2 = CallApi(client, server.base_url + "/ok", DefaultOptions());
    BOOST_REQUIRE(res2);
    BOOST_CHECK_EQUAL(res2.value().status, 200u);

    BOOST_REQUIRE(Close(client));
    server.server->StopAccepting();
    BOOST_REQUIRE(Close(server.rt));
    BOOST_REQUIRE(Close(rt));
}

// deadline 路径归还，且归还发生在物理收口：用门闩压住服务端 handler，
// client 本地 200ms 超时返回后，名额在 op 真正离开 m_ops 时才归还。
// 直接验证「物理收口前不提前复用」需要观测 impl 内部计数；这里通过
// 行为断言：超时返回后立即可接纳新请求（名额已随 op 落定归还）。
BOOST_AUTO_TEST_CASE(t_released_on_deadline) {
    auto server = StartServer([](std::shared_ptr<Gate> g){ return MakeGateHandler(g); });
    auto rt = StartClientRuntime(MakeLimits(/*max_conn=*/1, /*max_inflight=*/1));
    auto client = NewClient(rt);
    const std::string url = server.base_url + "/x";

    auto out1 = std::make_shared<std::optional<result<HttpResponse>>>();
    auto rdy1 = std::make_shared<std::atomic_bool>(false);
    // 短 deadline：服务端 handler 被门闩压住，调用方 200ms 超时。
    SpawnHeld(client, url, out1, rdy1, DefaultOptions(200));
    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 1; }));
    BOOST_REQUIRE(WaitUntil([&] {
        return rdy1->load(std::memory_order_acquire);
    }));
    BOOST_REQUIRE((*out1).has_value());
    BOOST_REQUIRE(!(*out1).value());   // result 是 error
    BOOST_CHECK((*out1)->error().code == ErrorCode::TimedOut);

    // 超时返回不立即复用名额：op 仍在 m_ops（Abort 未落定）。
    // 等 Abort 催完成项落定（服务端连接被断开 → handler 完成 read 失败），
    // 名额归还后第二个请求可接纳。给服务端放行并等它把 active 归零。
    // 注意：gate->active==0 只证明服务端 handler 已退出，client op 的
    // OnRead 完成项落定（IoAsyncDone→UnregisterOp→名额归还）与其存在
    // 调度竞态——必须实际重试 Request 直至被接纳，不能单次断言。
    server.gate->auto_release.store(true);
    server.gate->ReleaseAll();
    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 0; }));

    // 名额归还绑定 client op 物理收口，不在服务端 handler 退出的瞬间。
    // 轮询直至真实 Request 被接纳（Overloaded 表示名额仍占=完成项未落定，
    // 是正确行为而非失败）；上限 10s 内必须转为可接纳。
    bool admitted = false;
    auto res2 = CallApiUntilAdmitted(client, url, DefaultOptions(), admitted);
    BOOST_REQUIRE(admitted);
    BOOST_REQUIRE(res2);
    BOOST_CHECK_EQUAL(res2.value().status, 200u);

    BOOST_REQUIRE(Close(client));
    server.server->StopAccepting();
    BOOST_REQUIRE(Close(server.rt));
    BOOST_REQUIRE(Close(rt));
}

// cancel 路径归还：预取消 token 使请求在接纳前就失败，不占名额；
// 再验证 in-flight 取消后名额归还。
BOOST_AUTO_TEST_CASE(t_released_on_cancel) {
    auto server = StartServer([](std::shared_ptr<Gate> g){ return MakeGateHandler(g); });
    auto rt = StartClientRuntime(MakeLimits(/*max_conn=*/1, /*max_inflight=*/1));
    auto client = NewClient(rt);
    const std::string url = server.base_url + "/x";

    // 预取消：请求在 Wait 前就见到取消，不占名额（admit 在 cancel 检查
    // 之前完成，但本用例验证取消返回后名额可用）。
    auto out1 = std::make_shared<std::optional<result<HttpResponse>>>();
    auto rdy1 = std::make_shared<std::atomic_bool>(false);
    bbt::coroutine::CancellationSource src;
    CallOptions opt = DefaultOptions(20000);
    opt.cancel = src.Token();
    SpawnHeld(client, url, out1, rdy1, opt);
    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 1; }));
    src.RequestCancel();
    BOOST_REQUIRE(WaitUntil([&] {
        return rdy1->load(std::memory_order_acquire);
    }));
    BOOST_REQUIRE((*out1).has_value());
    BOOST_REQUIRE(!(*out1).value());
    BOOST_CHECK((*out1)->error().code == ErrorCode::Cancelled);

    server.gate->auto_release.store(true);
    server.gate->ReleaseAll();
    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 0; }));

    // 名额归还绑定 client op 物理收口而非服务端 handler 退出瞬间；
    // 轮询直至真实 Request 被接纳（Overloaded=名额仍占，继续等）。
    bool admitted = false;
    auto res2 = CallApiUntilAdmitted(client, url, DefaultOptions(), admitted);
    BOOST_REQUIRE(admitted);
    BOOST_REQUIRE(res2);
    BOOST_CHECK_EQUAL(res2.value().status, 200u);

    BOOST_REQUIRE(Close(client));
    server.server->StopAccepting();
    BOOST_REQUIRE(Close(server.rt));
    BOOST_REQUIRE(Close(rt));
}

// client RequestClose 中止在途请求后名额归还（物理收口触发）。
BOOST_AUTO_TEST_CASE(t_released_on_client_close) {
    auto server = StartServer([](std::shared_ptr<Gate> g){ return MakeGateHandler(g); });
    auto rt = StartClientRuntime(MakeLimits(/*max_conn=*/1, /*max_inflight=*/1));
    auto client = NewClient(rt);
    const std::string url = server.base_url + "/x";

    auto out1 = std::make_shared<std::optional<result<HttpResponse>>>();
    auto rdy1 = std::make_shared<std::atomic_bool>(false);
    SpawnHeld(client, url, out1, rdy1);
    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 1; }));

    // client close：在途 op 被 Abort+Finish，物理收口后名额归还。
    BOOST_REQUIRE(Close(client));
    BOOST_REQUIRE(WaitUntil([&] {
        return rdy1->load(std::memory_order_acquire);
    }));
    BOOST_REQUIRE((*out1).has_value());
    BOOST_CHECK(!(*out1).value());   // 被中止的请求是 error

    server.gate->auto_release.store(true);
    server.gate->ReleaseAll();
    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 0; }));

    // 新 client 在同一 runtime 上能再接纳（名额确实回到了 owner）。
    auto client2 = NewClient(rt);
    auto res2 = CallApi(client2, url, DefaultOptions());
    BOOST_REQUIRE(res2);
    BOOST_CHECK_EQUAL(res2.value().status, 200u);

    BOOST_REQUIRE(Close(client2));
    server.server->StopAccepting();
    BOOST_REQUIRE(Close(server.rt));
    BOOST_REQUIRE(Close(rt));
}

// Runtime RequestClose（强制 Stop 等价路径）中止在途后名额归还；
// 不依赖挂起协程的栈析构（runtime 拥有 op 集合，teardown 在 io 域
// 物理收口）。
BOOST_AUTO_TEST_CASE(t_runtime_close_releases) {
    auto server = StartServer([](std::shared_ptr<Gate> g){ return MakeGateHandler(g); });
    auto rt = StartClientRuntime(MakeLimits(/*max_conn=*/1, /*max_inflight=*/1));
    auto client = NewClient(rt);
    const std::string url = server.base_url + "/x";

    auto out1 = std::make_shared<std::optional<result<HttpResponse>>>();
    auto rdy1 = std::make_shared<std::atomic_bool>(false);
    SpawnHeld(client, url, out1, rdy1);
    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 1; }));

    BOOST_REQUIRE(Close(rt));   // runtime close 收口所有子对象
    BOOST_CHECK(client->IsClosed());
    BOOST_REQUIRE(WaitUntil([&] {
        return rdy1->load(std::memory_order_acquire);
    }));
    BOOST_REQUIRE((*out1).has_value());
    BOOST_CHECK(!(*out1).value());

    server.gate->auto_release.store(true);
    server.gate->ReleaseAll();
    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 0; }));
    server.server->StopAccepting();
    BOOST_REQUIRE(Close(server.rt));
}

// ---------------------------------------------------------------------------
// 复审回归（t_41dbfb61）：以下用例直接驱动 HttpClientImpl，经 SetQuotaHooks
// 注入可计数的 admit/release，不依赖 NetworkRuntimeImpl 的内部计数。
// ---------------------------------------------------------------------------
namespace {

// 注入式配额账本：模拟 owner 预算（上限 cap）。admit/release 调用次数与
// 当前占用都可观测，用于证明「恰好一次」与「物理收口前不复用」。
struct FakeBudget {
    explicit FakeBudget(std::size_t cap_) : cap(cap_) {}
    std::size_t              cap;
    std::mutex             mtx;
    std::size_t            held{0};
    std::atomic_int        admits{0};
    std::atomic_int        releases{0};

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
        if (held > 0) --held;
    }
    std::size_t Held() {
        std::lock_guard<std::mutex> lk(mtx);
        return held;
    }
};

// 直接构造 HttpClientImpl + 独立 HttpIoEngine（测试侧可 TryPost blocker
// 占住同一 strand 执行域）。返回 (impl, engine)；budget 由调用方注入。
std::pair<std::shared_ptr<http_detail::HttpClientImpl>,
          std::shared_ptr<http_detail::HttpIoEngine>>
NewImplClient(const std::shared_ptr<FakeBudget>& budget,
              const NetworkLimits& limits) {
    auto engine = std::make_shared<http_detail::HttpIoEngine>(limits);
    BOOST_REQUIRE(engine->Start());
    auto info = bbt::infra::detail::NewObjectInfo("infra.http_client");
    BOOST_REQUIRE(info);
    auto sig = bbt::infra::detail::NewCompletionSignal();
    BOOST_REQUIRE(sig);
    auto impl = std::make_shared<http_detail::HttpClientImpl>(
        engine, std::move(info).value(), std::move(sig).value());
    impl->SetQuotaHooks(
        [budget] { return budget->Admit(); },
        [budget] { budget->Release(); });
    return {impl, engine};
}

} // namespace

// 物理收口前不复用名额（真实异步完成项版，t_434bb4d5/t_cf78b9a1/t_afc896ec
// 复审要求）：
// 用真实 HttpClientImpl::Request 驱动完整 async 链，而非手工模拟
// inflight——裸对端 accept 后挂起不写响应，client 发起真实
// async_resolve/connect/write/read，read 完成项确实在途未落定。
// 确定性时序（R5：把屏障建立在「OnRead 完成项不可能被派发」的真实
// 机制上，替代依赖 FIFO 排序推断的事后投递）：
//   1. 裸对端 accept 后持有不响应 → client 的连接/写已完成，async_read
//      在途。socket.close 是唯一会催出 OnRead 完成项的动作，而它只在
//      Request 尾路径的 TryPost(Abort) 里发生；
//   2. caller 仍在 sig->Wait 挂起时（Abort 尚未 post），先把一个「等待
//      放行」的 io_hold 投递进同一 io 域并等它开始执行——此刻 strand 被
//      冻结在 io_hold 内，其后任何完成项/投递都不可能被派发；
//   3. 在冻结期间向 caller 的 CancellationSource 发 RequestCancel：caller
//      以 Cancelled 返回，并在返回前把 Abort 经 TryPost 排进同一 strand
//      ——Abort 只能排在 io_hold 之后，但 socket.close 催出的 OnRead
//      完成项同样只能排在 io_hold 之后，io 域仍冻结；
//   4. rdy1（caller 已返回 → Abort 已 post）后窗口确定成立：read 完成项
//      必然在途（要么 reactor 已把它催出、排在 io_hold 之后；要么 close
//      仍在 io_hold 之后排队）。窗口内第二个真实 Request 的 TryAdmit
//      必须看到名额仍占 → Overloaded；
//   5. 放行 io_hold：Abort 执行 → socket.close → OnRead 完成项派发 →
//      IoAsyncDone 归零 → UnregisterOp 归还名额。轮询到名额复用且
//      accept 计数 =2 证明新物理连接建成。
// 与 R4 的差别：rdy1 只证明 Abort 已 post，不证明 io_hold 排在 Abort
// 之前；abort→close→OnRead→IoAsyncDone→UnregisterOp 的整链都在 io 域
// 串行执行，若 OnRead 先于 io_hold 被派发，名额可能已归还，旧断言不可证。
// 本版在 caller 返回前就冻结 strand：close 连被发起的机会都没有，完成项
// 不可能越过 io_hold。
// 同步纪律：CancellationSource 由测试线程写、caller 协程读（token 本就
// 跨线程设计）；io_hold 用 RAII 保证断言失败时仍放行、peer 线程仍 join。
BOOST_AUTO_TEST_CASE(t_no_reuse_before_physical_close) {
    // 裸对端：listen/accept，accept 后不写响应也不 close，让 client
    // 的 async_read 长时间在途（由 client 侧 Abort 催完成项落定）。
    std::atomic_int  accepted{0};
    std::atomic_bool peer_stop{false};
    int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(lfd >= 0);
    int one = 1;
    ::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = htons(0);
    BOOST_REQUIRE(::bind(lfd, reinterpret_cast<sockaddr*>(&addr),
                         sizeof(addr)) == 0);
    BOOST_REQUIRE(::listen(lfd, 8) == 0);
    socklen_t alen = sizeof(addr);
    BOOST_REQUIRE(::getsockname(lfd, reinterpret_cast<sockaddr*>(&addr),
                                &alen) == 0);
    const std::uint16_t port = ntohs(addr.sin_port);
    const std::string url =
        "http://127.0.0.1:" + std::to_string(port) + "/x";

    // accept 线程：接受后保持连接不响应，直到 peer_stop。线程持有
    // conn_fds 引用，用例收尾统一 close。peer_guard 保证断言失败
    // （BOOST_REQUIRE 长跳）时仍 join 线程并关闭 fd，不遗留 30s
    // 占位或 joinable 线程进入析构。
    std::vector<int> conn_fds;
    std::mutex       fds_mtx;
    std::thread peer([&] {
        while (!peer_stop.load(std::memory_order_acquire)) {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(lfd, &rfds);
            timeval tv{0, 100000};               // 100ms 轮询 peer_stop
            int r = ::select(lfd + 1, &rfds, nullptr, nullptr, &tv);
            if (r <= 0)
                continue;
            int c = ::accept(lfd, nullptr, nullptr);
            if (c >= 0) {
                std::lock_guard<std::mutex> lk(fds_mtx);
                conn_fds.push_back(c);
                accepted.fetch_add(1, std::memory_order_release);
            }
        }
    });
    // peer_guard 保证断言失败（BOOST_REQUIRE 长跳）时仍 join 线程并
    // 关闭 fd，不遗留 30s 占位或 joinable 线程进入析构。
    struct PeerGuard {
        std::thread&             peer;
        std::atomic_bool&        peer_stop;
        std::mutex&              fds_mtx;
        std::vector<int>&        conn_fds;
        int                      lfd;
        ~PeerGuard() {
            peer_stop.store(true, std::memory_order_release);
            if (peer.joinable())
                peer.join();
            std::lock_guard<std::mutex> lk(fds_mtx);
            for (int c : conn_fds)
                ::close(c);
            ::close(lfd);
        }
    } peer_guard{peer, peer_stop, fds_mtx, conn_fds, lfd};

    // max_inflight=1：单一名额观察「物理收口前不归还」。
    auto rt = StartClientRuntime(MakeLimits(/*max_conn=*/8, /*max_inflight=*/1));
    auto client = NewClient(rt);
    auto impl = std::static_pointer_cast<http_detail::HttpClientImpl>(client);

    // read-armed 屏障（本版修复点）：OnWritten 发起 async_read 成功后
    // 在 io 域内 Down 本 latch。等到它才冻结 strand，即可确定第一个
    // op 的 read 完成项已真实在途——这是「物理收口前名额仍占」成立的
    // 必要前提。R5 仅用 accepted==1 推断，accept 只证明对端已收连接，
    // 不证明 OnWritten/async_read 已执行；本 latch 把该前提变成直接
    // 观察。钩子只写给第一个 op（在 SpawnHeld 前注入，写先于 io 域
    // 派发，且测试只触发这一次真实 async_read）。
    auto read_armed = std::make_shared<Latch>(1);
    impl->SetReadArmedHookForTest(
        [read_armed] { read_armed->Down(); });

    // 第一个真实 Request：连接 + 发出请求后对端不响应，read 在途。
    // caller 用 CancellationSource（而非 deadline）由测试线程在窗口内
    // 确定唤醒——cancel 走到与 timeout 相同的尾路径（TryPost(Abort) 后
    // 返回 Cancelled），但唤醒时机由测试侧控制而非时钟，屏障才能建在
    // caller 返回之前。
    auto out1 = std::make_shared<std::optional<result<HttpResponse>>>();
    auto rdy1 = std::make_shared<std::atomic_bool>(false);
    auto cancel_src =
        std::make_shared<bbt::coroutine::CancellationSource>();
    {
        CallOptions opt = DefaultOptions(20000);   // 兜底 deadline
        opt.cancel = cancel_src->Token();
        SpawnHeld(client, url, out1, rdy1, opt);
    }

    // 连接确实建立且 OnWritten 已发起 async_read（read 完成项真实在途）。
    // read_armed 在 io 域内、OnWritten 尾部触发：它先于 io_hold 进入
    // strand，故到达时 io_hold 尚未冻结；此刻冻结 strand，read 完成项
    // 只能排在 io_hold 之后被派发（ Abort close 催出后亦然 ），名额在
    // 窗口内必然仍占。
    BOOST_REQUIRE(read_armed->WaitTimeout(5000) == 0);
    BOOST_REQUIRE(WaitUntil([&] {
        return accepted.load(std::memory_order_acquire) == 1;
    }));
    // 在 caller 仍在挂起/未返回前把 io_hold 排进 io 域并等它开始执行：
    // strand 串行派发，io_hold 运行期间其后的 Abort 与 OnRead 完成项
    // 都不可能越过它。屏障成立的时刻是「io_hold 已开始执行」，与 Abort
    // 是否已 post 无关——Abort 若尚未 post，socket.close 甚至还没发起，
    // OnRead 完成项连被催出的机会都没有。
    auto io_hold  = std::make_shared<Latch>(1);   // 放行占位 handler
    auto io_held  = std::make_shared<Latch>(1);   // 占位 handler 已进入 io 域
    // RAII：断言失败也必须放行 io 域（否则 io_hold 占住 strand 30s）。
    struct HoldGuard {
        std::shared_ptr<Latch> io_hold;
        ~HoldGuard() { io_hold->Down(); }
    } hold_release_guard{io_hold};
    BOOST_REQUIRE(impl->Engine()->TryPost([io_hold, io_held] {
        io_held->Down();
        io_hold->WaitTimeout(30000);   // 占住 strand，冻结后续完成项派发
    }));
    BOOST_REQUIRE(io_held->WaitTimeout(5000) == 0);

    // 窗口已确定成立（strand 冻结在 io_hold 内）。此刻 caller 仍挂在
    // sig->Wait：发 cancel 让它以 Cancelled 落定返回，尾路径
    // TryPost(Abort) 把 Abort 排在 io_hold 之后——Abort 与 OnRead 完成项
    // 都只能在 io_hold 放行后派发，物理收口（UnregisterOp→归还名额）
    // 在窗口内不可能发生。
    cancel_src->RequestCancel();
    BOOST_REQUIRE(WaitUntil([&] {
        return rdy1->load(std::memory_order_acquire);
    }));
    BOOST_REQUIRE((*out1).has_value());
    BOOST_REQUIRE(!(*out1).value());
    BOOST_CHECK((*out1)->error().code == ErrorCode::Cancelled);

    // 关键断言 A：caller 已返回、Abort 已 post、io 域仍冻结——名额必然
    // 仍占（完成项连被派发的机会都没有）。第二个真实 Request 必须
    // Overloaded；接纳成功说明名额在物理收口前被误复用（回归）。
    auto res2 = CallApi(client, url, DefaultOptions(1500));
    BOOST_REQUIRE(!res2);
    BOOST_CHECK(res2.error().code == ErrorCode::Overloaded);

    // 放行真实完成项链：io_hold 返回 → Abort 执行 → socket.close 催出
    // OnRead 完成项 → IoAsyncDone 归零 → UnregisterOp 归还名额。
    io_hold->Down();
    // hold_release_guard 析构时再次 Down：对同一 Latch 幂等，无副作用。

    // 轮询直至第二个真实 Request 不再被 Overloaded 拒绝——即名额已归还。
    // 对端持有不响应：被接纳的请求会建第二个物理连接后 read 在途，
    // caller 侧 1500ms 超时返回 TimedOut。因此「非 Overloaded 的落定」
    // （无论 ok 还是 TimedOut）都证明名额已复用；Overloaded 则说明完成项
    // 仍未落定、名额仍占，继续等。accepted>=2 证明新物理连接确实建成。
    bool admitted = false;   // 名额已归还（第二次请求不再 Overloaded）
    const auto retry_deadline = std::chrono::steady_clock::now() +
                                std::chrono::seconds(10);
    result<HttpResponse> res3 = result<HttpResponse>::err(
        MakeError(ErrorCode::InternalError, "not run"));
    while (std::chrono::steady_clock::now() < retry_deadline) {
        res3 = CallApi(client, url, DefaultOptions(1500));
        if (!res3 && res3.error().code == ErrorCode::Overloaded) {
            // 名额仍占（完成项未落定）：正确行为，继续等。
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            continue;
        }
        // 名额已归还，请求被接纳并走真实链路：对端不响应 → 通常
        // TimedOut（read 在途超时）；若对端此刻已放行则 ok。两者都
        // 证明名额可复用。
        admitted = true;
        break;
    }
    BOOST_REQUIRE(admitted);                 // 名额在物理收口后已复用
    BOOST_CHECK_GE(accepted.load(std::memory_order_acquire), 2);

    BOOST_REQUIRE(Close(client));
    BOOST_REQUIRE(Close(rt));
    // peer_guard 析构统一收尾：peer_stop + join + close fds/lfd。
}

// 恰好一次归还：同一 op 上并发 Finish（跨线程）与 IoAsyncDone（io 域）、
// 以及重复 UnregisterOp，release 钩子总计只被调一次。
BOOST_AUTO_TEST_CASE(t_unregister_exactly_once) {
    auto budget = std::make_shared<FakeBudget>(/*cap=*/64);
    NetworkLimits limits = MakeLimits(8, 8);
    auto [impl, engine] = NewImplClient(budget, limits);

    auto mk_op = [&]() -> std::shared_ptr<http_detail::ClientOp> {
        auto sig = bbt::infra::detail::NewCompletionSignal();
        BOOST_REQUIRE(sig);
        auto op = std::make_shared<http_detail::ClientOp>(impl, engine);
        op->sig = std::move(sig).value();
        op->on_unregister = [budget] { budget->Release(); };
        return op;
    };

    // 场景 A：并发 Finish + IoAsyncDone（read 在途）交错收口。
    for (int round = 0; round < 8; ++round) {
        auto op = mk_op();
        BOOST_REQUIRE(impl->RegisterOp(op));
        op->IoAsyncStart();                     // 模拟 read 在途
        std::thread t1([&] {
            op->Finish(result<HttpResponse>::err(
                MakeError(ErrorCode::Closed, "race")));
        });
        std::thread t2([&] { op->IoAsyncDone(); });
        t1.join();
        t2.join();
    }
    BOOST_CHECK_EQUAL(budget->releases.load(), 8);

    // 场景 B：已落定 op 被并发/重复 UnregisterOp——erase 守门，
    // 第二个进入者不得再归还。
    {
        auto op = mk_op();
        BOOST_REQUIRE(impl->RegisterOp(op));
        op->Finish(result<HttpResponse>::err(
            MakeError(ErrorCode::Closed, "done")));
        BOOST_CHECK_EQUAL(budget->releases.load(), 9);
        std::thread t3([&] { impl->UnregisterOp(op); });
        std::thread t4([&] { impl->UnregisterOp(op); });
        impl->UnregisterOp(op);                 // 调用线程再重复一次
        t3.join();
        t4.join();
        BOOST_CHECK_EQUAL(budget->releases.load(), 9);
    }

    BOOST_REQUIRE(Close(impl));
}

// admit 成功后 RegisterOp 拒绝（teardown 已置 io_dead）→ guard 归还名额：
// 在 admit 钩子内发起 client RequestClose 并协程让出，等 io_dead 落定后
// admit 返回 ok——此时 IsOpen() 检查已过、RegisterOp 必失败，名额经
// AdmitGuard 析构归还，不泄漏。
BOOST_AUTO_TEST_CASE(t_register_op_failure_releases) {
    auto budget = std::make_shared<FakeBudget>(/*cap=*/1);
    NetworkLimits limits = MakeLimits(8, 8);
    auto [impl, engine] = NewImplClient(budget, limits);
    std::shared_ptr<HttpClient> client = impl;

    // admit 内触发 close 并等待 teardown 落定（协程内 bbtco_sleep 让出，
    // 不阻塞 scheduler 线程）。
    impl->SetQuotaHooks(
        [&budget, client] {
            budget->admits.fetch_add(1);
            {
                std::lock_guard<std::mutex> lk(budget->mtx);
                ++budget->held;
            }
            client->RequestClose();
            const auto dl = std::chrono::steady_clock::now() +
                            std::chrono::seconds(8);
            while (!client->IsClosed()) {
                if (std::chrono::steady_clock::now() > dl)
                    break;
                bbtco_sleep(5);
            }
            return result<void>::ok();
        },
        [&budget] { budget->Release(); });

    // Request 需在协程上下文；out 承载结果。
    std::optional<result<HttpResponse>> out;
    BOOST_REQUIRE(RunInCoroutine([&] {
        out.emplace(client->Request(
            HttpRequest{"GET", "http://127.0.0.1:1/x", {}, {}},
            DefaultOptions()));
    }));
    BOOST_REQUIRE(out.has_value());
    BOOST_REQUIRE(!out.value());
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
    // admit 发生一次，RegisterOp 被拒后 guard 归还一次：不泄漏。
    BOOST_CHECK_EQUAL(budget->admits.load(), 1);
    BOOST_CHECK_EQUAL(budget->releases.load(), 1);
    BOOST_CHECK_EQUAL(budget->Held(), 0u);
    BOOST_CHECK(client->IsClosed());
}

// 异常窗口回归（t_434bb4d5 指出的 m_release 复制窗口）：
// std::function 复制可抛 bad_alloc——旧顺序「admit 成功后才复制
// release」在复制抛异常时名额无归还责任者而泄漏。修复后 release
// 复制先于 admit 完成：
//   A) admit 返回 Overloaded：release 复制已完成但 admit 未占名额，
//      guard 析构不应归还（releases==0），名额账本仍为空。
//   B) admit 自身抛异常（模拟 owner 侧钩子故障）：异常穿过 Request
//      边界前名额未占用，release 不误调（releases==0）。
// 两个方向共同证明「release 先备好 + admit 失败不误归还」，结合
// t_register_op_failure_releases 的「admit 成功后异常路径必归还」，
// 覆盖窗口两侧。
BOOST_AUTO_TEST_CASE(t_admit_failure_no_leak_no_double_release) {
    auto budget = std::make_shared<FakeBudget>(/*cap=*/0);   // 上限 0：必拒
    NetworkLimits limits = MakeLimits(8, 8);
    auto [impl, engine] = NewImplClient(budget, limits);
    std::shared_ptr<HttpClient> client = impl;

    // A) admit 确定性拒绝：release 已复制到 guard 但名额未占，
    //    guard 析构不得归还（否则 releases 误增/账本下溢）。
    std::optional<result<HttpResponse>> out;
    BOOST_REQUIRE(RunInCoroutine([&] {
        out.emplace(client->Request(
            HttpRequest{"GET", "http://127.0.0.1:1/x", {}, {}},
            DefaultOptions()));
    }));
    BOOST_REQUIRE(out.has_value());
    BOOST_REQUIRE(!out.value());
    BOOST_CHECK(out->error().code == ErrorCode::Overloaded);
    BOOST_CHECK_EQUAL(budget->admits.load(), 1);
    BOOST_CHECK_EQUAL(budget->releases.load(), 0);
    BOOST_CHECK_EQUAL(budget->Held(), 0u);

    // B) admit 抛异常：异常向外传播，名额未占，release 不误调。
    impl->SetQuotaHooks(
        []() -> result<void> { throw std::bad_alloc(); },
        [&budget] { budget->Release(); });
    std::optional<result<HttpResponse>> out2;
    bool threw = false;
    BOOST_REQUIRE(RunInCoroutine([&] {
        try {
            out2.emplace(client->Request(
                HttpRequest{"GET", "http://127.0.0.1:1/x", {}, {}},
                DefaultOptions()));
        } catch (const std::bad_alloc&) {
            threw = true;
        }
    }));
    BOOST_CHECK(threw);                    // 异常确实穿过 Request
    BOOST_CHECK_EQUAL(budget->releases.load(), 0);
    BOOST_CHECK_EQUAL(budget->Held(), 0u);

    BOOST_REQUIRE(Close(impl));
}

BOOST_AUTO_TEST_CASE(t_end_stop_scheduler) {
    g_scheduler->Stop();
}

BOOST_AUTO_TEST_SUITE_END()
