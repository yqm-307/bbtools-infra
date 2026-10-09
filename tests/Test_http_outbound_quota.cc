// Issue #37：HTTP 出站连接与在途配额的原子接纳及物理归还回归。
// 关闭面按进程寿命运行时修订（契约 §1）：owner 主动同步 Close()，
// 没有 RequestClose/WaitClosed/CloseStatus、没有取消令牌、没有
// CompletionSignal，也没有 Scheduler::Stop()。每个用例的 runtime 在
// 用例内显式 Close() 并以 IsClosed/账本归零断言物理收口。
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
//   t_quota_isolated_across_runtimes    — 两个独立 client Runtime 同时在途，
//                                          各自独立预算；一个满额不影响另一个
//                                          （多 Runtime 作用域隔离）。
//   t_released_on_normal_completion     — 正常完成后名额归还，可再接纳。
//   t_released_on_peer_failure          — 对端断开/畸形响应等发起失败后
//                                          名额归还。
//   t_released_on_deadline              — 本地 deadline 到点后名额归还；
//                                          归还发生在等待完成项物理收口后
//                                          才允许再接纳，不在调用方超时返回
//                                          的瞬间。
//   t_released_on_client_close          — client Close() 中止在途请求后
//                                          名额归还。
//   t_runtime_close_releases            — Runtime Close() 中止在途后名额
//                                          归还，不依赖协程栈析构。
//   t_no_reuse_before_physical_close    — 名额在 op 物理收口（UnregisterOp，
//                                          finished && inflight==0）前不
//                                          复用：裸对端 accept 后持有不响应
//                                          压住 client 真实 async_read 完成
//                                          项；io 域冻结期间直读注入账本
//                                          （held==1 / releases==0）且真实
//                                          TryAdmitHttpRequest 必须拒绝
//                                          （Overloaded），放行后 Close()
//                                          收口，名额恰好归还一次。
//   t_unregister_exactly_once           — 并发 Finish/IoAsyncDone 与重复
//                                          UnregisterOp 对同一 op 恰好归还
//                                          一次（erase 守门回归）。
//   t_register_op_failure_releases      — admit 成功后 RegisterOp 拒绝
//                                          （io_dead）时名额经 guard 归还，
//                                          不泄漏。
//   t_admit_failure_no_leak_no_double_release — release 复制先于 admit：
//                                          admit 拒绝/抛异常时 guard 不误
//                                          归还，覆盖 m_release 复制窗口两侧。
//
// 同步纪律：跨线程/协程一律原子标志 + WaitUntil（带总预算）与
// CountDownLatch；handler 内的等待用 CoWaiter（等待位 + 跨线程 Notify），
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
#include <bbt/coroutine/sync/CoWaiter.hpp>
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
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;
using Latch = bbt::core::thread::CountDownLatch;

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

// owner 主动同步 Close()：幂等、任意线程，返回即物理释放。旧口径的
// RequestClose + WaitClosed(CloseStatus) 已删除，这里只断言返回后
// IsClosed 成立（物理收口由调用点各自的账本/对端观察断言）。
bool Close(const std::shared_ptr<ICoCloseable>& obj) {
    obj->Close();
    return obj->IsClosed();
}

// ---------------------------------------------------------------------------
// 可控门闩服务端：handler 进入时 active 计数 +1，每个请求持有独立等待位
// （CoWaiter：只等/唤醒，无取消令牌），测试侧统一放行。
// auto_release=true 时 handler 立即放行（用于「名额归还后可再接纳」的后续
// 请求，不再压门闩）。
// 服务端 Runtime 给足容量，瓶颈只可能出在 client 出站侧——这是接纳
// 缺口测试，不是服务端压测。
// ---------------------------------------------------------------------------
struct GateSignal {
    std::atomic_bool released{false};
    bbt::coroutine::sync::CoWaiter::SPtr waiter{
        bbt::coroutine::sync::CoWaiter::Create()};

    void Release() {
        released.store(true, std::memory_order_release);
        waiter->Notify();   // 跨线程安全；未挂起时是安全的空操作
    }
};

struct Gate {
    std::mutex                                mutex;
    std::vector<std::shared_ptr<GateSignal>>  signals;
    std::atomic_int                           active{0};
    std::atomic_int                           total{0};
    std::atomic_bool                          auto_release{false};
    std::shared_ptr<Latch>                    arrived;   // 由用例按 N 构造

    // 返回该请求专属等待位；auto_release 模式下立即放行（不压门闩）。
    std::shared_ptr<GateSignal> Hold() {
        auto sig = std::make_shared<GateSignal>();
        {
            std::lock_guard<std::mutex> lk(mutex);
            signals.push_back(sig);
        }
        ++active;
        ++total;
        if (arrived)
            arrived->Down();
        if (auto_release.load())
            sig->Release();
        return sig;
    }
    void ReleaseAll() {
        std::lock_guard<std::mutex> lk(mutex);
        for (auto& s : signals)
            s->Release();
    }
    // 选择性放行第 idx 个到达者（到达顺序确定时用于只放行其中一个在途
    // 请求，做「释放 A 不影响 B」的交叉对照）；越界返回 false，不抛。
    bool ReleaseAt(std::size_t idx) {
        std::shared_ptr<GateSignal> sig;
        {
            std::lock_guard<std::mutex> lk(mutex);
            if (idx >= signals.size())
                return false;
            sig = signals[idx];
        }
        sig->Release();
        return true;
    }
};

// 每个请求独立等待位：释放只对已到达者生效，未到达的不占名额。
HttpHandler MakeGateHandler(const std::shared_ptr<Gate>& gate) {
    return [gate](IncomingCallContext, HttpRequest) {
        auto sig = gate->Hold();
        bbt::coroutine::WaitOptions wait;
        wait.deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(10);
        // 放行可能发生在 Hold 之后、真正 park 之前：on_registered 内复查
        // 并同步 Notify（登记成功后立即兑现，走 PENDING 早到路径），
        // 不丢唤醒也不靠 sleep 假设时序。
        sig->waiter->WaitWithCallback(wait, [sig]() -> bool {
            if (sig->released.load(std::memory_order_acquire))
                sig->waiter->Notify();
            return true;
        });
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
    // 进程寿命运行时：只有 IsInitialized，没有 Stop/代际。
    BOOST_REQUIRE(g_scheduler->IsInitialized());
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
    // 再统一放行已压住的等待位。
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

// 多 Runtime 配额作用域：两个独立 client Runtime 同时在途，一个满额不影响
// 另一个（原票第二项「多 client / 多 Runtime 配额作用域」的直接对照，补齐
// 「同 Runtime 多 client 共享」之外的隔离方向）。
// 与 t_quota_shared_across_clients 恰好相反的作用域断言：那里同 Runtime 下
// 两个 client 共享一份预算，这里两个 Runtime 各自独立、互不侵占。真实
// HttpClientImpl::Request 驱动 owner 预算门禁，不注入假账本。
//
// 判别力：若预算被错误地做成进程/全局共享（或两个 runtime 意外共用同一
// owner），B 的首个请求会被 A 的占用挤成 Overloaded——步骤「B 同时在途、
// 服务端 active==2」即失败；若归还越界触碰其他 runtime 的账本，末段
// 「A 释放后 B 仍 Overloaded」即失败。两个方向都靠事件门控（服务端 handler
// 到达计数 + 结果就绪标志），不靠固定 sleep。
BOOST_AUTO_TEST_CASE(t_quota_isolated_across_runtimes) {
    auto server = StartServer([](std::shared_ptr<Gate> g){ return MakeGateHandler(g); });
    // 两个独立 client Runtime，各自拥有独立预算（max_conn=1, max_inflight=1）。
    auto rt_a = StartClientRuntime(MakeLimits(/*max_conn=*/1, /*max_inflight=*/1));
    auto rt_b = StartClientRuntime(MakeLimits(/*max_conn=*/1, /*max_inflight=*/1));
    auto client_a = NewClient(rt_a);
    auto client_b = NewClient(rt_b);
    const std::string url = server.base_url + "/x";

    // A 占满自己的唯一名额：请求被服务端 handler 压住，真实在途。
    auto out_a = std::make_shared<std::optional<result<HttpResponse>>>();
    auto rdy_a = std::make_shared<std::atomic_bool>(false);
    SpawnHeld(client_a, url, out_a, rdy_a);
    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 1; }));

    // 方向①：A 满额时 A 的第二个请求确定性 Overloaded，服务端不接到超额。
    auto res_a2 = CallApi(client_a, url, DefaultOptions());
    BOOST_REQUIRE(!res_a2);
    BOOST_CHECK(res_a2.error().code == ErrorCode::Overloaded);
    BOOST_CHECK_EQUAL(server.gate->total.load(), 1);

    // 独立 Runtime B 同时在途：A 仍持有其唯一名额，B 的请求却被接纳并压住
    // ——两个 runtime 的预算互不侵占（共享账本下此处会 Overloaded、active 停在 1）。
    auto out_b = std::make_shared<std::optional<result<HttpResponse>>>();
    auto rdy_b = std::make_shared<std::atomic_bool>(false);
    SpawnHeld(client_b, url, out_b, rdy_b);
    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 2; }));
    BOOST_CHECK_EQUAL(server.gate->total.load(), 2);

    // 方向②：B 也满额后 B 的第二个请求同样 Overloaded，服务端仍只有 2。
    auto res_b2 = CallApi(client_b, url, DefaultOptions());
    BOOST_REQUIRE(!res_b2);
    BOOST_CHECK(res_b2.error().code == ErrorCode::Overloaded);
    BOOST_CHECK_EQUAL(server.gate->total.load(), 2);

    // 只放行 A 的在途请求：到达顺序确定（已等 active==1 才发起 B），
    // signals[0] 即 A。B 继续压住，用于后续交叉对照。
    BOOST_REQUIRE(server.gate->ReleaseAt(0));
    BOOST_REQUIRE(WaitUntil([&] { return rdy_a->load(std::memory_order_acquire); }));
    BOOST_REQUIRE((*out_a).has_value());
    BOOST_REQUIRE((*out_a).value());
    BOOST_CHECK_EQUAL((*out_a).value().value().status, 200u);
    // A 的 handler 已退出，B 仍被压住 ⇒ 服务端并发回到 1。
    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 1; }));

    // 后续被接纳者立即放行（不再压门闩）；B 已持有的等待位不受影响。
    server.gate->auto_release.store(true);

    // A 自己的名额经物理收口归还后可再接纳（轮询消化「server handler 退出」
    // 与「client op 完成项落定归还名额」之间的调度竞态，Overloaded 即仍占用）。
    bool admitted = false;
    auto res_a3 = CallApiUntilAdmitted(client_a, url, DefaultOptions(), admitted);
    BOOST_REQUIRE(admitted);
    BOOST_REQUIRE(res_a3);
    BOOST_CHECK_EQUAL(res_a3.value().status, 200u);

    // 关键交叉断言：A 已归还并再次完成，B 仍处于自身满额状态（其请求尚未
    // 收口）——B 的新请求必须仍 Overloaded，证明 A 的接纳与归还没有触碰 B 的
    // 预算（真作用域隔离，而非共享/全局账本）。服务端累计只多了 A 的第三个。
    auto res_b3 = CallApi(client_b, url, DefaultOptions());
    BOOST_REQUIRE(!res_b3);
    BOOST_CHECK(res_b3.error().code == ErrorCode::Overloaded);
    BOOST_CHECK_EQUAL(server.gate->total.load(), 3);

    // 收尾：放行 B 的在途请求，各自 runtime 同步 Close() 收口。
    server.gate->ReleaseAll();
    BOOST_REQUIRE(WaitUntil([&] { return rdy_b->load(std::memory_order_acquire); }));
    BOOST_REQUIRE((*out_b).has_value());
    BOOST_REQUIRE((*out_b).value());
    BOOST_CHECK_EQUAL((*out_b).value().value().status, 200u);

    BOOST_REQUIRE(Close(client_a));
    BOOST_REQUIRE(Close(client_b));
    server.server->StopAccepting();
    BOOST_REQUIRE(Close(server.rt));
    BOOST_REQUIRE(Close(rt_a));
    BOOST_REQUIRE(Close(rt_b));
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
    // #64：服务端 handler 已进入 ⇒ 请求已完整写出 ⇒ 本端期限先到是未知，
    // 不再是 TimedOut（期限不证明远端未执行）。
    BOOST_CHECK((*out1)->error().code == ErrorCode::OutcomeUnknown);
    BOOST_REQUIRE((*out1)->error().request_phase.has_value());
    BOOST_CHECK(*(*out1)->error().request_phase ==
                RequestPhase::RequestCommitted);

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

// client Close() 中止在途请求后名额归还（物理收口触发）。
BOOST_AUTO_TEST_CASE(t_released_on_client_close) {
    auto server = StartServer([](std::shared_ptr<Gate> g){ return MakeGateHandler(g); });
    auto rt = StartClientRuntime(MakeLimits(/*max_conn=*/1, /*max_inflight=*/1));
    auto client = NewClient(rt);
    const std::string url = server.base_url + "/x";

    auto out1 = std::make_shared<std::optional<result<HttpResponse>>>();
    auto rdy1 = std::make_shared<std::atomic_bool>(false);
    SpawnHeld(client, url, out1, rdy1);
    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 1; }));

    // client Close()：在途 op 被 Abort + Finish(Closed)，物理收口后名额
    // 归还；返回即 IsClosed。
    BOOST_REQUIRE(Close(client));
    BOOST_REQUIRE(WaitUntil([&] {
        return rdy1->load(std::memory_order_acquire);
    }));
    BOOST_REQUIRE((*out1).has_value());
    BOOST_CHECK(!(*out1).value());   // 被中止的请求是 error
    // #64：服务端 handler 已进入 ⇒ 请求已完整写出；owner Close 中止等待，
    // 但请求可能已生效，故是未知而不是 Closed。
    BOOST_CHECK((*out1)->error().code == ErrorCode::OutcomeUnknown);
    BOOST_REQUIRE((*out1)->error().request_phase.has_value());
    BOOST_CHECK(*(*out1)->error().request_phase ==
                RequestPhase::RequestCommitted);

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

// Runtime Close() 中止在途后名额归还；不依赖挂起协程的栈析构（runtime
// 拥有 op 集合，teardown 在 io 域物理收口）。
BOOST_AUTO_TEST_CASE(t_runtime_close_releases) {
    auto server = StartServer([](std::shared_ptr<Gate> g){ return MakeGateHandler(g); });
    auto rt = StartClientRuntime(MakeLimits(/*max_conn=*/1, /*max_inflight=*/1));
    auto client = NewClient(rt);
    const std::string url = server.base_url + "/x";

    auto out1 = std::make_shared<std::optional<result<HttpResponse>>>();
    auto rdy1 = std::make_shared<std::atomic_bool>(false);
    SpawnHeld(client, url, out1, rdy1);
    BOOST_REQUIRE(WaitUntil([&] { return server.gate->active.load() == 1; }));

    BOOST_REQUIRE(Close(rt));   // runtime close 同步收口所有子对象
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
// 构造函数不再接收 CompletionSignal：等待位由 op 自己持有（CoWaiter）。
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

} // namespace

// 物理收口前不复用名额（真实异步完成项版，t_434bb4d5/t_cf78b9a1/t_afc896ec
// 复审要求）：
// 用真实 HttpClientImpl::Request 驱动完整 async 链，而非手工模拟
// inflight——裸对端 accept 后挂起不写响应，client 发起真实
// async_resolve/connect/write/read，read 完成项确实在途未落定。
// 新契约下不再有取消令牌：窗口的确定性靠「冻结 io 域」而不是「唤醒
// 调用方」——调用方在窗口内保持挂起即可，不需要它先返回。
//   1. 裸对端 accept 后持有不响应 → client 的连接/写已完成，async_read
//      在途（SetReadArmedHookForTest 直接观测到「read 已真实发起」）；
//   2. 把 io_hold 投进同一 strand 并等它开始执行——strand 串行派发，
//      此后任何完成项/投递都不可能被派发，物理收口（UnregisterOp→
//      归还名额）在窗口内不可能发生；
//   3. 窗口内直读注入账本：held==1 且 releases==0（未物理收口 ⇒ 名额
//      仍占、未归还），且第二个真实 Request 必须 Overloaded
//      （TryAdmitHttpRequest 看到的仍是占用中的名额）；
//   4. 放行 io_hold → Close()：teardown 在 io 域 Abort/Finish 催出 read
//      完成项 → IoAsyncDone 归零 → UnregisterOp 恰好归还一次；Close
//      返回即 m_ops 空，账本 held==0、releases==1。
// 同步纪律：io_hold 用 RAII 保证断言失败时仍放行、peer 线程仍 join。
BOOST_AUTO_TEST_CASE(t_no_reuse_before_physical_close) {
    // 裸对端：listen/accept，accept 后不写响应也不 close，让 client
    // 的 async_read 长时间在途（由 client 侧 Close 的 Abort 催完成项落定）。
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
    // conn_fds 引用，用例收尾统一 close。
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
    // 关闭 fd，不遗留占位或 joinable 线程进入析构。
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

    // max_inflight=1：单一名额观察「物理收口前不归还」。注入式账本给出
    // 名额持有/归还的直接读数，不依赖「第二个请求是否被拒」的间接推断。
    auto budget = std::make_shared<FakeBudget>(/*cap=*/1);
    NetworkLimits limits = MakeLimits(/*max_conn=*/8, /*max_inflight=*/1);
    auto [impl, engine] = NewImplClient(budget, limits);
    std::shared_ptr<HttpClient> client = impl;

    // read-armed 屏障：OnWritten 发起 async_read 成功后在 io 域内 Down
    // 本 latch。等到它才冻结 strand，即可确定 op 的 read 完成项已真实
    // 在途——这是「物理收口前名额仍占」成立的直接前提。钩子只写给第一个
    // op（在 SpawnHeld 前注入，写先于 io 域派发）。
    auto read_armed = std::make_shared<Latch>(1);
    impl->SetReadArmedHookForTest(
        [read_armed] { read_armed->Down(); });

    // 第一个真实 Request：连接 + 发出请求后对端不响应，read 在途；
    // 调用方在窗口内一直挂起（deadline 兜底），窗口不依赖它先返回。
    auto out1 = std::make_shared<std::optional<result<HttpResponse>>>();
    auto rdy1 = std::make_shared<std::atomic_bool>(false);
    SpawnHeld(client, url, out1, rdy1, DefaultOptions(20000));

    BOOST_REQUIRE(read_armed->WaitTimeout(5000) == 0);
    BOOST_REQUIRE(WaitUntil([&] {
        return accepted.load(std::memory_order_acquire) == 1;
    }));
    // 在 caller 仍挂起时把 io_hold 排进 io 域并等它开始执行：strand
    // 串行派发，io_hold 运行期间其后的 Abort/完成项都不可能越过它。
    auto io_hold  = std::make_shared<Latch>(1);   // 放行占位 handler
    auto io_held  = std::make_shared<Latch>(1);   // 占位 handler 已进入 io 域
    struct HoldGuard {
        std::shared_ptr<Latch> io_hold;
        ~HoldGuard() { io_hold->Down(); }
    } hold_release_guard{io_hold};
    BOOST_REQUIRE(impl->Engine()->TryPost([io_hold, io_held] {
        io_held->Down();
        io_hold->WaitTimeout(30000);   // 占住 strand，冻结后续完成项派发
    }));
    BOOST_REQUIRE(io_held->WaitTimeout(5000) == 0);
    BOOST_CHECK_EQUAL(budget->admits.load(), 1);

    // 关键断言 A：io 域冻结、read 完成项在途 ⇒ 未物理收口 ⇒ 名额仍占、
    // 一次都没归还。任何「物理收口前复用名额」的回归都会在这里失败。
    BOOST_CHECK_EQUAL(budget->Held(), 1u);
    BOOST_CHECK_EQUAL(budget->releases.load(), 0);
    // 第二个真实 Request 必须 Overloaded（接纳判定看到名额仍占）。
    auto res2 = CallApi(client, url, DefaultOptions(1500));
    BOOST_REQUIRE(!res2);
    BOOST_CHECK(res2.error().code == ErrorCode::Overloaded);

    // 放行真实完成项链：io_hold 返回 → teardown Abort/Finish → 完成项
    // 落定 → IoAsyncDone 归零 → UnregisterOp 归还名额（恰好一次）。
    io_hold->Down();
    BOOST_REQUIRE(Close(client));
    BOOST_CHECK(client->IsClosed());
    BOOST_CHECK_EQUAL(budget->Held(), 0u);
    BOOST_CHECK_EQUAL(budget->releases.load(), 1);
    BOOST_REQUIRE(WaitUntil([&] {
        return rdy1->load(std::memory_order_acquire);
    }));
    BOOST_REQUIRE((*out1).has_value());
    BOOST_REQUIRE(!(*out1).value());
    // #64：read_armed 已触发 ⇒ 写侧已完成（请求完整写出）⇒ 本端放弃等待
    // （cancel）得到未知，而不是 Closed。
    BOOST_CHECK((*out1)->error().code == ErrorCode::OutcomeUnknown);
    BOOST_REQUIRE((*out1)->error().request_phase.has_value());
    BOOST_CHECK(*(*out1)->error().request_phase ==
                RequestPhase::RequestCommitted);

    // peer_guard 析构统一收尾：peer_stop + join + close fds/lfd。
}

// 恰好一次归还：同一 op 上并发 Finish（跨线程）与 IoAsyncDone（io 域）、
// 以及重复 UnregisterOp，release 钩子总计只被调一次。
BOOST_AUTO_TEST_CASE(t_unregister_exactly_once) {
    auto budget = std::make_shared<FakeBudget>(/*cap=*/64);
    NetworkLimits limits = MakeLimits(8, 8);
    auto [impl, engine] = NewImplClient(budget, limits);

    auto mk_op = [&]() -> std::shared_ptr<http_detail::ClientOp> {
        auto op = std::make_shared<http_detail::ClientOp>(impl, engine);
        // 每个 op 自带等待位（构造时不带 signal）；Finish 会经它唤醒
        // 等待者，未挂起的等待位收到 Notify 是安全的空操作。
        op->waiter = bbt::coroutine::sync::CoWaiter::Create();
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
// 在 admit 钩子内同步发起 client Close()（已收口的 client 不再接纳新 op），
// admit 返回 ok 后 RegisterOp 必失败，名额经 AdmitGuard 析构归还，不泄漏。
BOOST_AUTO_TEST_CASE(t_register_op_failure_releases) {
    auto budget = std::make_shared<FakeBudget>(/*cap=*/1);
    NetworkLimits limits = MakeLimits(8, 8);
    auto [impl, engine] = NewImplClient(budget, limits);
    std::shared_ptr<HttpClient> client = impl;

    // admit 内触发 Close()：此刻尚无登记 op（m_ops 为空），同步 Close 的
    // 在途等待立即返回，不阻塞调度线程。
    impl->SetQuotaHooks(
        [&budget, client] {
            budget->admits.fetch_add(1);
            {
                std::lock_guard<std::mutex> lk(budget->mtx);
                ++budget->held;
            }
            client->Close();
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
    BOOST_CHECK(Close(client));   // 幂等
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

// ---------------------------------------------------------------------------
// B1 根因回归（实际 connect 后的用户非阻塞态）。
// 修复前：Begin 在 socket 未 open 时 non_blocking(true) 无效；range
// async_connect 会对每个 endpoint close/reopen socket，令
// user_set_non_blocking 位丢失；写/读的 MSG_DONTWAIT 拿到 EAGAIN 时被
// Asio 在应用可见前吸收为阻塞 poll(-1)，全程持 IoGate → ArmWait 不可达、
// Close 阻塞。修复：OnConnect 连接成功后重建并检查该位。
// 判别：对端制造 EAGAIN（写背压 / 读部分响应）后，并发 Close 必须有界
// 返回；修复前 io 线程卡在 poll(-1) 持门，Close 永不返回。断言同时覆盖
// 物理资源（对端观测 FIN、注入账本归还）与等待/终态（Closed 落定），
// 不只看 IsClosed 或耗时。
// ---------------------------------------------------------------------------
namespace {

std::pair<int, std::uint16_t> MakeRawListener(int backlog) {
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
    BOOST_REQUIRE(::listen(lfd, backlog) == 0);
    socklen_t len = sizeof(addr);
    BOOST_REQUIRE(::getsockname(lfd, reinterpret_cast<sockaddr*>(&addr),
                                &len) == 0);
    return {lfd, ntohs(addr.sin_port)};
}

// r1 的 CloseBounded（起线程 + 超时 detach）已删除：超时分支 detach 后线程
// 仍会写已退出的调用者栈帧（父级 ASan 实证 stack-use-after-return），且无限
// 残留被卡线程。两个 B1 用例现在直接同步调用 Close()：真实卡住时整进程由
// CTest http.outbound_quota 的 TIMEOUT=120 判失败——确定性失败，不引入后台
// 线程或新的控制协议。

// 在对端 fd 上以有界超时排空并观测 FIN（read 返回 0 / reset）——
// 证明 client 侧 fd 真正关闭（物理资源释放），不是只置逻辑标志。
bool PeerObservedFin(int fd, int budget_ms) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(budget_ms);
    for (;;) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(fd, &rfds);
        timeval tv{0, 20000};
        if (::select(fd + 1, &rfds, nullptr, nullptr, &tv) > 0) {
            char buf[8192];
            const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n <= 0)
                return true;          // FIN 或 reset：对端连接已不可用
        }
        if (std::chrono::steady_clock::now() > deadline)
            return false;
    }
}

// 裸对端：listener + accept 线程；每个连接交给 on_conn 处置（可为空 =
// accept 后不读不写，制造写背压）。析构停线程、join、关全部 fd。
struct RawPeer {
    int                      lfd{-1};
    std::uint16_t            port{0};
    std::atomic_bool         stop{false};
    std::atomic_int          accepted{0};
    std::mutex               mtx;
    std::vector<int>         conn_fds;
    std::function<void(int)> on_conn;
    std::thread              th;

    explicit RawPeer(std::function<void(int)> cb, int backlog = 8)
        : on_conn(std::move(cb)) {
        auto lp = MakeRawListener(backlog);
        lfd  = lp.first;
        port = lp.second;
        th   = std::thread([this] {
            while (!stop.load(std::memory_order_acquire)) {
                fd_set rfds;
                FD_ZERO(&rfds);
                FD_SET(lfd, &rfds);
                timeval tv{0, 100000};
                if (::select(lfd + 1, &rfds, nullptr, nullptr, &tv) <= 0)
                    continue;
                int c = ::accept(lfd, nullptr, nullptr);
                if (c < 0)
                    continue;
                {
                    std::lock_guard<std::mutex> lk(mtx);
                    conn_fds.push_back(c);
                }
                accepted.fetch_add(1, std::memory_order_release);
                if (on_conn)
                    on_conn(c);
            }
        });
    }
    ~RawPeer() {
        stop.store(true, std::memory_order_release);
        if (th.joinable())
            th.join();
        std::lock_guard<std::mutex> lk(mtx);
        for (int c : conn_fds)
            ::close(c);
        ::close(lfd);
    }
    std::string Url(const std::string& path) const {
        return "http://127.0.0.1:" + std::to_string(port) + path;
    }
    bool FinAll(int budget_ms) {
        std::lock_guard<std::mutex> lk(mtx);
        for (int c : conn_fds)
            if (!PeerObservedFin(c, budget_ms))
                return false;
        return true;
    }
};

} // namespace

// 回归①：写背压（对端 accept 后不再读）下写侧无法完成时，Close 必须同步
// 返回并物理收口（卡住 → CTest TIMEOUT 判失败）。send EAGAIN→就绪等待的
// syscall 级判定由既有 --wrap=sendmsg/poll 探针覆盖，本用例不声称。
BOOST_AUTO_TEST_CASE(t_b1_write_backpressure_close_bounded) {
    // 对端：accept 后只读一次请求字节（证明写泵已物理运行），此后不再读；
    // 8MiB 请求体压满内核发送缓冲 → 写侧无法完成。
    std::atomic_int got_bytes{0};
    RawPeer peer([&got_bytes](int c) {
        char          buf[4096];
        const ssize_t n = ::recv(c, buf, sizeof(buf), 0);
        if (n > 0)
            got_bytes.store(static_cast<int>(n), std::memory_order_release);
    });
    auto budget = std::make_shared<FakeBudget>(/*cap=*/1);
    NetworkLimits limits = MakeLimits(8, 1);
    limits.max_body_bytes = 16 * 1024 * 1024;   // 8MiB 请求体需过发送校验
    auto [impl, engine] = NewImplClient(budget, limits);
    std::shared_ptr<HttpClient> client = impl;
    // 写完成（serializer 泵完 → ArmRead）才触发的 seam：用它断言「写侧在
    // 窗口内未完成」，而不是靠固定 sleep 声称已进入 EAGAIN。
    auto read_armed = std::make_shared<std::atomic_bool>(false);
    impl->SetReadArmedHookForTest(
        [read_armed] { read_armed->store(true, std::memory_order_release); });

    // 8MiB 请求体远超 loopback 内核缓冲，保证写侧必然进入 EAGAIN。
    HttpRequest req{"POST", peer.Url("/x"), {},
                    std::string(8 * 1024 * 1024, 'x')};
    auto out = std::make_shared<std::optional<result<HttpResponse>>>();
    auto rdy = std::make_shared<std::atomic_bool>(false);
    BOOST_REQUIRE(Spawn([client, req, out, rdy]() mutable {
        out->emplace(client->Request(req, DefaultOptions(30000)));
        rdy->store(true, std::memory_order_release);
    }));
    BOOST_REQUIRE(WaitUntil([&] { return budget->Held() == 1; }, 5000));
    BOOST_REQUIRE(WaitUntil([&] { return peer.accepted.load() == 1; }, 5000));
    // setup：真实握手替代固定 sleep——对端读到请求字节 ⇒ 写泵已物理运行；
    // 对端此后不再读 ⇒ 写侧在窗口内不可能完成，read_armed 必然不触发。
    // 这里断言的是「写侧未完成」这一可观察状态；「EAGAIN→就绪等待」的
    // syscall 级判定由既有 --wrap=sendmsg/poll 探针动态覆盖，本用例不声称。
    BOOST_REQUIRE_MESSAGE(WaitUntil([&] { return got_bytes.load() > 0; }, 5000),
        "peer never observed request bytes: write pump did not run");
    BOOST_REQUIRE_MESSAGE(!WaitUntil([&] { return read_armed->load(); }, 500),
        "write completed despite a non-reading peer: setup not backpressured");

    // Close 必须同步返回（物理收口）；卡住 → CTest TIMEOUT 判失败。
    client->Close();
    BOOST_REQUIRE(client->IsClosed());
    BOOST_CHECK_EQUAL(budget->Held(), 0u);          // 名额物理归还
    BOOST_CHECK_EQUAL(budget->releases.load(), 1);
    BOOST_REQUIRE(WaitUntil(
        [&] { return rdy->load(std::memory_order_acquire); }, 5000));
    BOOST_REQUIRE((*out).has_value());
    BOOST_REQUIRE(!(*out).value());
    BOOST_CHECK((*out)->error().code == ErrorCode::Closed);
    BOOST_CHECK(peer.FinAll(3000));                 // fd 真关闭：对端观测 FIN
}

// 回归②：对端发出不完整响应（头完整、体不足）后 Close 必须同步返回并物理
// 收口；不完整响应绝不伪造成成功。读 EAGAIN→就绪等待的 syscall 级判定由
// 既有 --wrap=recvmsg/recv/poll 探针覆盖，本用例不声称。
BOOST_AUTO_TEST_CASE(t_b1_read_partial_eagain_close_bounded) {
    std::atomic_bool served{false};
    RawPeer peer([&served](int c) {
        char buf[4096];
        (void)::recv(c, buf, sizeof(buf), 0);      // 读掉请求
        const char partial[] =
            "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nabc";  // 体不足
        (void)::send(c, partial, sizeof(partial) - 1, 0);
        served.store(true, std::memory_order_release);
        // 之后保持连接不关也不补数据：读泵必然落到 EAGAIN。
    });
    auto budget = std::make_shared<FakeBudget>(/*cap=*/1);
    auto [impl, engine] = NewImplClient(budget, MakeLimits(8, 1));
    std::shared_ptr<HttpClient> client = impl;
    auto read_armed = std::make_shared<std::atomic_bool>(false);
    impl->SetReadArmedHookForTest(
        [read_armed] { read_armed->store(true, std::memory_order_release); });

    HttpRequest req{"GET", peer.Url("/p"), {}, {}};
    auto out = std::make_shared<std::optional<result<HttpResponse>>>();
    auto rdy = std::make_shared<std::atomic_bool>(false);
    BOOST_REQUIRE(Spawn([client, req, out, rdy]() mutable {
        out->emplace(client->Request(req, DefaultOptions(30000)));
        rdy->store(true, std::memory_order_release);
    }));
    BOOST_REQUIRE(WaitUntil(
        [&] { return served.load(std::memory_order_acquire); }, 5000));
    // setup：真实握手替代固定 sleep——read_armed 只在写完成、读就绪等待真实
    // 武装后触发，等到它即证明请求已写出且读侧已在途（不靠 sleep 假设时序）。
    // 本用例不声称读泵已进入 EAGAIN（测试侧无可观察量）；「读 EAGAIN→就绪
    // 等待」的 syscall 级判定由既有 --wrap=recvmsg/recv/poll 探针覆盖。
    BOOST_REQUIRE_MESSAGE(
        WaitUntil([&] { return read_armed->load(); }, 5000),
        "read never armed: write/read handshake not reached");

    // Close 必须同步返回（物理收口）；卡住 → CTest TIMEOUT 判失败。
    client->Close();
    BOOST_REQUIRE(client->IsClosed());
    BOOST_CHECK_EQUAL(budget->Held(), 0u);
    BOOST_CHECK_EQUAL(budget->releases.load(), 1);
    BOOST_REQUIRE(WaitUntil(
        [&] { return rdy->load(std::memory_order_acquire); }, 5000));
    BOOST_REQUIRE((*out).has_value());
    BOOST_REQUIRE(!(*out).value());          // 不完整响应不得成功
    // #64：read_armed 已触发 ⇒ 请求已完整写出；不完整响应不构成可信终态，
    // 故 owner Close 收口后是未知而不是 Closed。
    BOOST_CHECK((*out)->error().code == ErrorCode::OutcomeUnknown);
    BOOST_REQUIRE((*out)->error().request_phase.has_value());
    BOOST_CHECK(*(*out)->error().request_phase ==
                RequestPhase::RequestCommitted);
    BOOST_CHECK(peer.FinAll(3000));
}

// ---------------------------------------------------------------------------
// B1-r2 直接回归：连接 op 的失败收口必须先物理释放再注销。
// 父验收实证（--wrap=ioctl 注入真实连接后的 FIONBIO 失败）：修复前 OnConnect
// 失败分支直接 Finish，op 先离开 m_ops，owner Close 快照为空，socket 与 8MiB
// body 无人物理释放，只能等 op 析构——违反「不能依赖析构」。本用例在既有
// 出站配额套件内直接驱动 ClientOp，走可直接触发的同一失败收口分支（真实
// Begin→resolve→connect 到已关闭端口 ⇒ connect 失败），断言修复后的物理
// 后置条件。
//
// 注入边界（诚实声明，不冒充完整覆盖）：本机实测一切「有效」fd 的 FIONBIO
// 都成功（/dev/null、普通文件、目录、pipe、eventfd、epoll、ptmx、socket 均
// rc=0），测试侧无法在不改 CMake/链接包装（--wrap 需改 tests/CMakeLists.txt）
// 的前提下让已连接 socket 的 FIONBIO 失败；OnConnect 又是 private，不能直接
// 调用。故设置用户非阻塞态失败分支由 scratch 探针
// （http-b1-r2/nonblocking-failure-probe.cc，--wrap=ioctl）动态覆盖 fd
// EBADF/无发送，本用例覆盖同一收口不变量的可直接触发分支。
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(t_b1_connect_failure_physical_release) {
    // 先占一个端口再立刻关闭：loopback 上该端口无监听者 ⇒ connect 必然
    // ECONNREFUSED（不依赖外部网络或时序）。
    std::uint16_t dead_port = 0;
    {
        int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
        BOOST_REQUIRE(lfd >= 0);
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port        = htons(0);
        BOOST_REQUIRE(::bind(lfd, reinterpret_cast<sockaddr*>(&addr),
                             sizeof(addr)) == 0);
        socklen_t len = sizeof(addr);
        BOOST_REQUIRE(::getsockname(lfd, reinterpret_cast<sockaddr*>(&addr),
                                    &len) == 0);
        dead_port = ntohs(addr.sin_port);
        ::close(lfd);
    }

    auto budget = std::make_shared<FakeBudget>(/*cap=*/1);
    auto [impl, engine] = NewImplClient(budget, MakeLimits(8, 1));
    std::shared_ptr<HttpClient> client = impl;

    // 与 Request 相同的接线：先 admit 预留名额，再把 release 挂到 op 的物理
    // 收口钩子（op 离开 m_ops 时归还一次）。
    BOOST_REQUIRE(budget->Admit());
    BOOST_REQUIRE_EQUAL(budget->Held(), 1u);
    auto op = std::make_shared<http_detail::ClientOp>(impl, engine);
    op->waiter = bbt::coroutine::sync::CoWaiter::Create();
    op->request.version(11);
    op->request.method_string("POST");
    op->request.target("/");
    op->request.set(boost::beast::http::field::host, "127.0.0.1");
    op->request.body() = std::string(8 * 1024 * 1024, 'x');  // 与父夹具同规模
    op->request.prepare_payload();
    int unregistered = 0;
    op->on_unregister = [&] {
        ++unregistered;
        budget->Release();
    };
    BOOST_REQUIRE(impl->RegisterOp(op));
    BOOST_REQUIRE(engine->TryPost([op, dead_port] {
        op->Begin("127.0.0.1", dead_port);
    }));
    BOOST_REQUIRE(WaitUntil([&] { return op->finished.load(); }, 10000));

    // 发布序同步（F1）：ClientOp::Finish 先 finished.exchange(true)，随后才写
    // outcome、经 MaybeUnregister→UnregisterOp→on_unregister 归还配额
    // （++unregistered、budget->Release()）。finished 的 exchange 只对「其之前」
    // 的写入建立 happens-before——恰好覆盖 Abort 写下的 body/buffer/socket/
    // inflight（故下方物理释放断言本就不 flaky），但 outcome/unregistered/quota
    // 都写在 finished 发布之后，与测试线程之间没有同步边（未参与者实测完整测试
    // 70 次有 4 次在本断言前假红）。故读这三类字段前，先等待反登记之后的真实
    // 同步点：on_unregister 内 budget->Release() 的 releases.fetch_add（seq_cst）
    // 与 held 递减（同一 mtx）。releases==1 与 outcome/unregistered 的写入建立
    // happens-before；held 递减发生在 fetch_add 之后且受 mtx 保护，故一并要求
    // Held()==0，取得该写入的可见性。逻辑终态仍由上面的 finished 等待保证。
    BOOST_REQUIRE(WaitUntil(
        [&] { return budget->releases.load() == 1 && budget->Held() == 0; },
        10000));

    // 失败语义保留：connect 失败不得变成成功或静默重试。
    BOOST_REQUIRE(op->outcome.has_value());
    BOOST_REQUIRE(!*op->outcome);
    BOOST_CHECK(op->outcome->error().message.find("connect failed") !=
                std::string::npos);
    // 失败收口在 op 离开 m_ops 前已物理释放（不等 Close、不依赖析构）。
    BOOST_CHECK_EQUAL(op->request.body().size(), 0u);  // 8MiB 不再占有
    BOOST_CHECK_EQUAL(op->buffer.size(), 0u);
    BOOST_CHECK_EQUAL(op->inflight.load(), 0);
    BOOST_CHECK(!op->serializer.has_value());          // 未进入 PumpWrite
    BOOST_CHECK(!op->socket.is_open());                // socket 已物理关闭
    BOOST_CHECK_EQUAL(unregistered, 1);                // 离开 m_ops 恰好一次
    BOOST_CHECK_EQUAL(budget->Held(), 0u);             // 配额恰好归还一次
    BOOST_CHECK_EQUAL(budget->releases.load(), 1);

    client->Close();
    BOOST_CHECK(client->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_end_runtime_convergence) {
    // 进程寿命运行时没有 Scheduler::Stop：各用例自有的 runtime 都在用例内
    // 显式 Close() 并断言 IsClosed；这里确认运行时仍在跑，且新建 runtime
    // 仍能创建→使用→同步 Close 收敛（收尾没有污染全局状态）。
    BOOST_CHECK(g_scheduler->IsInitialized());

    auto rt = StartClientRuntime(MakeLimits(1, 1));
    auto client = NewClient(rt);
    std::weak_ptr<NetworkRuntime> weak = rt;
    BOOST_CHECK(Close(client));
    BOOST_CHECK(Close(rt));
    BOOST_CHECK(rt->IsClosed());
    // 关闭后工厂拒绝新子对象（收口即封口，不重开）。
    auto late = rt->CreateHttpClient();
    BOOST_REQUIRE(!late);
    BOOST_CHECK(late.error().code == ErrorCode::Closed);
    // 物理收口 + 外部引用释放：不再有存活的强持有者（无泄漏托管）。
    rt.reset();
    client.reset();
    BOOST_REQUIRE(WaitUntil([&] { return weak.expired(); }));
}

BOOST_AUTO_TEST_SUITE_END()
