// N1b-2：取消与关闭的完整语义验收（契约 §116-133，含「逻辑结果与
// 物理清理分离」小节）。用例与语义一一对应：
//
//   t_ctx_uses_listener_budget_only   — ctx.deadline 只取 listener 本地
//                                       预算；不信任自定义 deadline 头；
//                                       peer_principal 未认证为空。
//   t_cancel_on_peer_disconnect       — 连接断开触发 ctx.cancel；token
//                                       在 handler 运行期间保持有效，
//                                       状态活到 handler 退出之后仍可查询。
//   t_cancel_on_listener_deadline     — 本地期限触发 ctx.cancel，且
//                                       回复路径被切断（client 见 Error）。
//   t_first_terminal_state_not_overwritten
//                                     — 完成/取消竞争时首次发布的逻辑
//                                       终态不被覆盖；取消不回滚业务。
//   t_timeout_caller_returns_cleanup_continues
//                                     — 超时让调用方及时返回，底层清理
//                                       继续（对端随后观测到断开）。
//   t_waitclosed_contention           — WaitClosed 单等待位：并发第二个
//                                       返回 AlreadyWaiting。
//   t_owner_close_honest_waitclosed   — owner close 触发在途取消；
//                                       handler 未退出时 WaitClosed 返回
//                                       TimedOut 而非伪造 Closed。
//   t_runtime_close_drains_children   — runtime 关闭等待所有子对象物理
//                                       清理；落定后工厂与请求一律 Closed。
//   t_client_close_drains_inflight_read — client 在途 read 时 RequestClose
//                                       必须等 completion 才 Closed，不挂死。
//   t_server_close_drains_idle_session — 无 handler 的在途 async_read 会话
//                                       也计入 Closed，不只等 pending_handlers。
//
// 同步纪律：跨线程/跨协程一律用原子标志 + WaitUntil（带总预算）与
// CountDownLatch 做屏障；handler 内的等待用 bbtco_sleep 轮询真实事件
// 标志，不靠 sleep 假设时序。所有等待都有超时上限，不会无限挂起。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Hook.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

using namespace bbt::infra;
using bbt::coroutine::Deadline;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

constexpr std::size_t kTestMaxBody = 64 * 1024;
constexpr int         kBudgetMs    = 15000;

std::shared_ptr<NetworkRuntime> g_runtime;
std::shared_ptr<HttpClient>     g_client;
std::shared_ptr<HttpServer>     g_server;
std::uint16_t                   g_port = 0;
NetworkLimits                   g_limits{};

NetworkLimits MakeLimits(std::chrono::milliseconds incoming) {
    NetworkLimits limits{};
    limits.max_connections  = 64;
    limits.max_inflight     = 64;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = kTestMaxBody;
    limits.incoming_timeout = incoming;
    return limits;
}

// handler 内部状态对外可见的门闩/旗标集合；每条门径用一份，互不干扰。
struct Gate {
    std::atomic_bool entered{false};
    std::atomic_bool saw_cancel{false};
    std::atomic_bool release{false};
    std::atomic_bool exited{false};
    std::atomic_bool has_ctx{false};
    Deadline                            seen_deadline{};
    bbt::coroutine::CancellationToken   seen_cancel;
    std::string                         seen_peer{"<unset>"};
    std::chrono::steady_clock::time_point entry_tp{};
};

Gate g_obs;    // /observe：只记录上下文后返回
Gate g_hold;   // /hold：观察取消→等放行→退出（disconnect 用例）
Gate g_hold2;  // /hold2：同形（owner-close 用例）
Gate g_hold3;  // /hold3：同形（runtime drain 用例）
Gate g_gate;   // /gate：等放行→返回（不检查取消，first-terminal 用例）
Gate g_dl;     // /dl：短预算 runtime 上的观察路径

// 协程内执行并限时等待；任何一步失败返回 false（测试不得无限挂住）。
template <class F>
bool RunInCoroutine(F&& f, int budget_ms = kBudgetMs) {
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

result<HttpResponse> CallApi(HttpRequest req, CallOptions options) {
    std::optional<result<HttpResponse>> out;
    bool ran = RunInCoroutine(
        [&] { out.emplace(g_client->Request(std::move(req), options)); });
    if (!ran)
        return result<HttpResponse>::err(
            MakeError(ErrorCode::InternalError, "test: coroutine did not run"));
    return std::move(*out);
}

CallOptions DefaultOptions(int budget_ms = 10000) {
    CallOptions opt;
    opt.deadline = std::chrono::steady_clock::now() +
                   std::chrono::milliseconds(budget_ms);
    return opt;
}

std::string Url(const std::string& target) {
    return "http://127.0.0.1:" + std::to_string(g_port) + target;
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

// handler 侧轮询真实事件：等 ctx.cancel 置位或外部放行，只受
// bbtco_sleep 调度粒度影响，不假设任何到达顺序；上限兜底防挂死。
void WaitCancelOrRelease(const bbt::coroutine::CancellationToken& token,
                         const std::atomic_bool& release) {
    for (int i = 0; i < 5000; ++i) {
        if (token.IsCancellationRequested() || release.load())
            return;
        bbtco_sleep(2);
    }
}

// /hold 系列公共形态：进入→等取消或放行→记录是否见到取消→等放行→退出。
result<HttpResponse> HoldHandler(Gate& g, IncomingCallContext& ctx) {
    g.seen_cancel = ctx.cancel;   // 保存 token：验证其在途状态活到 handler 退出后
    g.entered.store(true);
    WaitCancelOrRelease(ctx.cancel, g.release);
    g.saw_cancel.store(ctx.cancel.IsCancellationRequested());
    while (!g.release.load())
        bbtco_sleep(2);
    g.exited.store(true);
    return result<HttpResponse>::ok(HttpResponse{200, {}, "hold done"});
}

result<HttpResponse> TestHandler(IncomingCallContext ctx, HttpRequest req) {
    if (req.url == "/ok")
        return result<HttpResponse>::ok(HttpResponse{200, {}, "ok"});

    if (req.url == "/observe") {
        g_obs.entry_tp      = std::chrono::steady_clock::now();
        g_obs.seen_deadline = ctx.deadline;
        g_obs.seen_cancel   = ctx.cancel;
        g_obs.seen_peer     = ctx.peer_principal;
        g_obs.has_ctx.store(true);
        g_obs.entered.store(true);
        return result<HttpResponse>::ok(HttpResponse{200, {}, "obs"});
    }
    if (req.url == "/dl") {
        g_dl.entered.store(true);
        WaitCancelOrRelease(ctx.cancel, g_dl.release);
        g_dl.saw_cancel.store(ctx.cancel.IsCancellationRequested());
        g_dl.exited.store(true);
        return result<HttpResponse>::ok(HttpResponse{200, {}, "dl"});
    }
    if (req.url == "/gate") {
        g_gate.entered.store(true);
        while (!g_gate.release.load())
            bbtco_sleep(2);
        g_gate.exited.store(true);
        return result<HttpResponse>::ok(HttpResponse{200, {}, "gate done"});
    }
    if (req.url == "/hold")  return HoldHandler(g_hold, ctx);
    if (req.url == "/hold2") return HoldHandler(g_hold2, ctx);
    if (req.url == "/hold3") return HoldHandler(g_hold3, ctx);

    return result<HttpResponse>::ok(HttpResponse{404, {}, "not found"});
}

// POSIX 裸连接助手：充当对端发任意字节/主动断开，与实现解耦。
int TcpConnectTo(std::uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    timeval tv{5, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    ::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

// 裸服务端（协程内驱动，不占额外线程）：accept 一条连接后执行
// serve(fd) 再关闭。lfd 上的 SO_RCVTIMEO 经 Hook 约束 accept，
// 失败路径最多挂到超时；状态经 shared_ptr 传递，dtor 只做有界等待。
struct RawServer {
    std::uint16_t port = 0;
    std::shared_ptr<std::atomic_bool> done{
        std::make_shared<std::atomic_bool>(false)};

    explicit RawServer(std::function<void(int)> serve) {
        int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
        BOOST_REQUIRE(lfd >= 0);
        int one = 1;
        ::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        timeval tv{5, 0};
        ::setsockopt(lfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
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
        port = ntohs(addr.sin_port);
        auto flag = done;
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [lfd, serve = std::move(serve), flag] {
                int c = ::accept(lfd, nullptr, nullptr);
                if (c >= 0) {
                    serve(c);
                    ::close(c);
                }
                ::close(lfd);
                flag->store(true);
            },
            succ);
        if (!succ) {
            ::close(lfd);
            done->store(true);
        }
        BOOST_REQUIRE(succ);
    }
    ~RawServer() { WaitUntil([flag = done] { return flag->load(); }, 8000); }
};

} // namespace

BOOST_AUTO_TEST_SUITE(http_lifecycle)

BOOST_AUTO_TEST_CASE(t_begin_start_runtime) {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 2;
    // 本套件的 handler 链路较深（RunHandler→HoldHandler→轮询→
    // bbtco_sleep hook），上游默认 12KB 纤程栈不够，会撞保护页
    // 造成 SIGSEGV；测试侧把纤程栈调到 256KB（本进程全局配置，
    // 由测试持有 scheduler，属合理测试参数而非实现约束）。
    cfg->m_cfg_stack_size = 1024 * 256;
    g_scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(g_scheduler->IsRunning());

    g_limits = MakeLimits(std::chrono::milliseconds{10000});

    auto rt = NetworkRuntime::Create(g_limits);
    BOOST_REQUIRE(rt);
    g_runtime = std::move(rt).value();
    BOOST_REQUIRE(g_runtime->Start());

    auto cl = g_runtime->CreateHttpClient();
    BOOST_REQUIRE(cl);
    g_client = std::move(cl).value();

    auto srv = g_runtime->ListenHttp({"127.0.0.1", 0}, TestHandler);
    BOOST_REQUIRE(srv);
    g_server = std::move(srv).value();
    g_port = g_server->LocalAddress().port;
    BOOST_REQUIRE_NE(g_port, 0);
}

BOOST_AUTO_TEST_CASE(t_ctx_uses_listener_budget_only) {
    // 不信任自定义 deadline 头：客户端塞入伪造预算头，ctx.deadline
    // 仍必须等于 listener 本地预算（读请求时刻 + incoming_timeout）。
    const auto t0 = std::chrono::steady_clock::now();
    HttpRequest req{"GET", Url("/observe"),
                    {{"X-Deadline-Ms", "1"}, {"X-Request-Timeout", "0"}}, ""};
    auto res = CallApi(std::move(req), DefaultOptions());
    BOOST_REQUIRE(res);
    BOOST_REQUIRE(WaitUntil([&] { return g_obs.entered.load(); }));

    const Deadline dl = g_obs.seen_deadline;
    // deadline = 读请求起始时刻 + 本地预算。读起始必然晚于调用发起 t0，
    // 且不晚于 handler 进入时刻 entry_tp。
    BOOST_CHECK(dl >= t0 + g_limits.incoming_timeout);
    BOOST_CHECK(dl <= g_obs.entry_tp + g_limits.incoming_timeout);
    // handler 进入时期限仍在未来（10s 预算远大于 loopback 延迟）。
    BOOST_CHECK(dl > g_obs.entry_tp);
    // 未取消、未认证 peer 为空。
    BOOST_CHECK(!g_obs.seen_cancel.IsCancellationRequested());
    BOOST_CHECK(g_obs.seen_peer.empty());
}

BOOST_AUTO_TEST_CASE(t_cancel_on_peer_disconnect) {
    int fd = TcpConnectTo(g_port);
    BOOST_REQUIRE(fd >= 0);
    const char raw[] =
        "GET /hold HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    BOOST_REQUIRE(::send(fd, raw, sizeof(raw) - 1, MSG_NOSIGNAL) >= 0);
    // handler 已进入、正在运行；
    BOOST_REQUIRE(WaitUntil([&] { return g_hold.entered.load(); }));
    // 此刻断开连接：连接断开必须触发 ctx.cancel。
    ::close(fd);
    BOOST_REQUIRE(WaitUntil([&] { return g_hold.saw_cancel.load(); }));
    // 放行让 handler 正常退出；退出后 token 状态仍可查询——
    // 取消状态由在途请求状态持有到 handler 真正退出，未被提前释放。
    g_hold.release.store(true);
    BOOST_REQUIRE(WaitUntil([&] { return g_hold.exited.load(); }));
    BOOST_CHECK(g_hold.saw_cancel.load());
    BOOST_CHECK(g_hold.seen_cancel.IsCancellationRequested());
}

BOOST_AUTO_TEST_CASE(t_cancel_on_listener_deadline) {
    // 第二套 runtime：listener 本地预算 300ms，验证本地期限触发取消。
    auto limits2 = MakeLimits(std::chrono::milliseconds{300});
    auto rt2 = NetworkRuntime::Create(limits2);
    BOOST_REQUIRE(rt2);
    auto rt2p = std::move(rt2).value();
    BOOST_REQUIRE(rt2p->Start());
    auto srv2 = rt2p->ListenHttp({"127.0.0.1", 0}, TestHandler);
    BOOST_REQUIRE(srv2);
    auto srv2p = std::move(srv2).value();

    HttpRequest req{"GET",
        "http://127.0.0.1:" + std::to_string(srv2p->LocalAddress().port) +
            "/dl",
        {}, ""};
    auto res = CallApi(std::move(req), DefaultOptions());
    // 本地期限到点：handler 见到取消；回复路径已切断，client 收到 Error。
    BOOST_REQUIRE(WaitUntil([&] { return g_dl.exited.load(); }));
    BOOST_CHECK(g_dl.saw_cancel.load());
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::TransportError);

    // rt2 自身的完整关闭顺序：StopAccepting → handler 已退出 → close。
    srv2p->StopAccepting();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        rt2p->RequestClose();
        st.store(rt2p->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
    BOOST_CHECK(srv2p->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_first_terminal_state_not_overwritten) {
    // 响应被门闩压住：本地取消必然先发布；之后 handler 正常完成，
    // 晚到的成功结果不得覆盖已发布的 Cancelled，业务也不回滚。
    bbt::coroutine::CancellationSource src;
    CallOptions opt = DefaultOptions();
    opt.cancel = src.Token();

    std::optional<result<HttpResponse>> out;
    std::atomic_bool out_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            HttpRequest req{"GET", Url("/gate"), {}, ""};
            out.emplace(g_client->Request(std::move(req), opt));
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return g_gate.entered.load(); }));
    src.RequestCancel();
    BOOST_REQUIRE(WaitUntil(
        [&] { return out_ready.load(std::memory_order_acquire); }));
    // 首次发布的逻辑终态：Cancelled。
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Cancelled);
    // 放行后 handler 正常跑完：取消不等于业务回滚。
    g_gate.release.store(true);
    BOOST_REQUIRE(WaitUntil([&] { return g_gate.exited.load(); }));
    // 逻辑结果不被晚到完成覆盖。
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Cancelled);
}

BOOST_AUTO_TEST_CASE(t_timeout_caller_returns_cleanup_continues) {
    // 裸对端永不响应；recv 见到 EOF 时证明客户端底层清理已执行。
    std::atomic_bool peer_eof{false};
    RawServer silent([&](int c) {
        char buf[512];
        for (;;) {
            const ssize_t n = ::recv(c, buf, sizeof(buf), 0);
            if (n <= 0) {
                peer_eof.store(true);
                return;
            }
        }
    });
    CallOptions opt = DefaultOptions(300);   // 本地 300ms 期限
    HttpRequest req{"GET",
        "http://127.0.0.1:" + std::to_string(silent.port) + "/x", {}, ""};
    auto res = CallApi(std::move(req), opt);
    // 调用方按期限及时返回，不等待物理清理。
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::TimedOut);
    // 底层清理继续：对端随后观测到连接被中止。
    BOOST_REQUIRE(WaitUntil([&] { return peer_eof.load(); }));
    // 超时请求不损害后续正常请求（runtime 仍可用）。
    HttpRequest ok{"GET", Url("/ok"), {}, ""};
    auto res2 = CallApi(std::move(ok), DefaultOptions());
    BOOST_REQUIRE(res2);
    BOOST_CHECK_EQUAL(res2.value().status, 200u);
}

BOOST_AUTO_TEST_CASE(t_waitclosed_contention) {
    auto srv = g_runtime->ListenHttp({"127.0.0.1", 0}, TestHandler);
    BOOST_REQUIRE(srv);
    auto s = std::move(srv).value();

    // A、B 并发抢同一等待位：输家见 AlreadyWaiting 即重入，赢家占住
    // 后另一方的后续尝试必见 AlreadyWaiting；双侧重试使结果不依赖
    // 谁先进入 Wait 的竞态时序。
    std::atomic_bool        a_waiting{false};
    std::atomic_bool        a_done{false};
    std::atomic<CloseStatus> a_status{};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            a_waiting.store(true);
            for (;;) {
                const auto st = s->WaitClosed(
                    std::chrono::steady_clock::now() +
                        std::chrono::seconds(5),
                    {});
                // 等待位被 B 占住或本轮到期：继续抢位，直到拿到
                // Closed——测试保证随后必然 RequestClose。
                if (st != CloseStatus::Closed)
                    continue;
                a_status.store(st);
                break;
            }
            a_done.store(true);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return a_waiting.load(); }));

    std::atomic_bool b_saw_already_waiting{false};
    std::atomic_bool b_done{false};
    g_scheduler->RegistCoroutineTask(
        [&] {
            // 有界重试：A 若尚未真正占住等待位，B 会先拿到并以
            // 50ms 超时让出；下一轮 A 已就位，B 即见 AlreadyWaiting。
            for (int i = 0; i < 200 && !b_saw_already_waiting.load(); ++i) {
                const auto st = s->WaitClosed(
                    std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(50),
                    {});
                if (st == CloseStatus::AlreadyWaiting)
                    b_saw_already_waiting.store(true);
                else if (st == CloseStatus::Closed)
                    break;
            }
            b_done.store(true);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return b_done.load(); }));
    BOOST_CHECK(b_saw_already_waiting.load());

    s->RequestClose();
    BOOST_REQUIRE(WaitUntil([&] { return a_done.load(); }));
    BOOST_CHECK(a_status.load() == CloseStatus::Closed);
    BOOST_CHECK(s->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_owner_close_honest_waitclosed) {
    // /hold2 在途；owner close 必须触发取消且诚实上报未完成的清理。
    std::optional<result<HttpResponse>> out;
    std::atomic_bool out_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            HttpRequest req{"GET", Url("/hold2"), {}, ""};
            out.emplace(g_client->Request(std::move(req), DefaultOptions()));
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return g_hold2.entered.load(); }));

    g_server->RequestClose();   // owner close：不抢占，只发取消+断回复路径
    // handler 运行中见到取消——token 由在途状态持有，此刻仍有效。
    BOOST_REQUIRE(WaitUntil([&] { return g_hold2.saw_cancel.load(); }));
    // client 侧的在途请求被中止，返回 Error（不许假成功）。
    BOOST_REQUIRE(WaitUntil(
        [&] { return out_ready.load(std::memory_order_acquire); }));
    BOOST_CHECK(!*out);

    // handler 尚未退出：WaitClosed 必须如实返回 TimedOut，不是 Closed。
    std::atomic<CloseStatus> early{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        early.store(g_server->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::milliseconds(300),
            {}));
    }));
    BOOST_CHECK(early.load() == CloseStatus::TimedOut);
    BOOST_CHECK(!g_server->IsClosed());

    // handler 退出后物理清理落定：WaitClosed 才返回 Closed。
    g_hold2.release.store(true);
    BOOST_REQUIRE(WaitUntil([&] { return g_hold2.exited.load(); }));
    std::atomic<CloseStatus> fin{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        fin.store(g_server->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(fin.load() == CloseStatus::Closed);
    BOOST_CHECK(g_server->IsClosed());
    BOOST_CHECK(g_hold2.seen_cancel.IsCancellationRequested());
}

BOOST_AUTO_TEST_CASE(t_runtime_close_drains_children) {
    // 新 server + 两个在途：handler 被门闩压住 + 裸对端永不响应。
    auto srv2 = g_runtime->ListenHttp({"127.0.0.1", 0}, TestHandler);
    BOOST_REQUIRE(srv2);
    auto srv2p = std::move(srv2).value();

    std::atomic_bool peer_eof{false};
    RawServer silent([&](int c) {
        char buf[256];
        while (::recv(c, buf, sizeof(buf), 0) > 0) {
        }
        peer_eof.store(true);
    });

    std::optional<result<HttpResponse>> out_hold, out_silent;
    std::atomic_bool hold_ready{false}, silent_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            HttpRequest req{"GET",
                "http://127.0.0.1:" +
                    std::to_string(srv2p->LocalAddress().port) + "/hold3",
                {}, ""};
            out_hold.emplace(
                g_client->Request(std::move(req), DefaultOptions(30000)));
            hold_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    g_scheduler->RegistCoroutineTask(
        [&] {
            HttpRequest req{"GET",
                "http://127.0.0.1:" + std::to_string(silent.port) + "/x",
                {}, ""};
            out_silent.emplace(
                g_client->Request(std::move(req), DefaultOptions(30000)));
            silent_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return g_hold3.entered.load(); }));

    g_runtime->RequestClose();
    // handler 未退出 ⇒ server 物理清理未完 ⇒ runtime WaitClosed 如实
    // 返回 TimedOut，不伪造 Closed。
    std::atomic<CloseStatus> early{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        early.store(g_runtime->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::milliseconds(300),
            {}));
    }));
    BOOST_CHECK(early.load() == CloseStatus::TimedOut);
    BOOST_CHECK(!g_runtime->IsClosed());

    g_hold3.release.store(true);
    BOOST_REQUIRE(WaitUntil([&] { return g_hold3.exited.load(); }));
    std::atomic<CloseStatus> fin{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        fin.store(g_runtime->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(15), {}));
    }));
    BOOST_CHECK(fin.load() == CloseStatus::Closed);
    BOOST_CHECK(g_runtime->IsClosed());

    // 全部子对象物理关闭；在途请求均以 Error 落定；对端见到断开。
    BOOST_CHECK(g_server->IsClosed());
    BOOST_CHECK(srv2p->IsClosed());
    BOOST_CHECK(g_client->IsClosed());
    BOOST_REQUIRE(WaitUntil(
        [&] { return hold_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(WaitUntil(
        [&] { return silent_ready.load(std::memory_order_acquire); }));
    BOOST_CHECK(!*out_hold);
    BOOST_CHECK(!*out_silent);
    BOOST_REQUIRE(WaitUntil([&] { return peer_eof.load(); }));

    // 关闭后新提交一律拒绝；工厂不再产出能假成功的对象。
    HttpRequest req{"GET", Url("/ok"), {}, ""};
    auto res = CallApi(std::move(req), DefaultOptions());
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::Closed);
    auto cl2 = g_runtime->CreateHttpClient();
    BOOST_REQUIRE(!cl2);
    BOOST_CHECK(cl2.error().code == ErrorCode::Closed);
    auto srv3 = g_runtime->ListenHttp({"127.0.0.1", 0}, TestHandler);
    BOOST_REQUIRE(!srv3);
    BOOST_CHECK(srv3.error().code == ErrorCode::Closed);
}

BOOST_AUTO_TEST_CASE(t_client_close_drains_inflight_read) {
    // 独立 runtime：裸对端收完请求后不再回复，client 停在 async_read。
    // RequestClose 必须等这条 completion 落定才 Closed，且不得挂死。
    auto limits = MakeLimits(std::chrono::milliseconds{10000});
    auto rt = NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt);
    auto rtp = std::move(rt).value();
    BOOST_REQUIRE(rtp->Start());
    auto cl = rtp->CreateHttpClient();
    BOOST_REQUIRE(cl);
    auto client = std::move(cl).value();

    std::atomic_bool got_req{false};
    RawServer silent([&](int c) {
        char buf[512];
        const ssize_t n = ::recv(c, buf, sizeof(buf), 0);
        if (n > 0)
            got_req.store(true);
        while (::recv(c, buf, sizeof(buf), 0) > 0) {
        }
    });

    std::optional<result<HttpResponse>> out;
    std::atomic_bool out_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            HttpRequest req{"GET",
                "http://127.0.0.1:" + std::to_string(silent.port) + "/x",
                {}, ""};
            out.emplace(client->Request(std::move(req), DefaultOptions(30000)));
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return got_req.load(); }));

    client->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(client->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
    BOOST_REQUIRE(WaitUntil(
        [&] { return out_ready.load(std::memory_order_acquire); }));
    BOOST_CHECK(!*out);

    rtp->RequestClose();
    BOOST_REQUIRE(RunInCoroutine([&] {
        rtp->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {});
    }));
}

BOOST_AUTO_TEST_CASE(t_server_close_drains_idle_session) {
    // connect 成功意味着 accept+BeginRead 已发出；对端不发 HTTP，
    // 无 pending_handlers。Closed 必须等这条 async_read 落定。
    auto limits = MakeLimits(std::chrono::milliseconds{10000});
    auto rt = NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt);
    auto rtp = std::move(rt).value();
    BOOST_REQUIRE(rtp->Start());
    auto srv = rtp->ListenHttp({"127.0.0.1", 0}, TestHandler);
    BOOST_REQUIRE(srv);
    auto server = std::move(srv).value();

    int fd = TcpConnectTo(server->LocalAddress().port);
    BOOST_REQUIRE(fd >= 0);

    server->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(server->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
    BOOST_CHECK(server->IsClosed());
    ::close(fd);

    rtp->RequestClose();
    BOOST_REQUIRE(RunInCoroutine([&] {
        rtp->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {});
    }));
}

BOOST_AUTO_TEST_CASE(t_end_stop_scheduler) {
    g_client.reset();
    g_server.reset();
    g_runtime.reset();
    g_scheduler->Stop();
}

BOOST_AUTO_TEST_SUITE_END()
