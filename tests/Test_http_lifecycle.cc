// N1b-2：HTTP 关闭与终态交付语义验收（进程寿命运行时修订，契约 §1/§2）。
//
// 旧口径已被删除，本件不保留任何旧断言：取消令牌（在途 token 寿命、
// 取消源）、逻辑/物理清理分离下的 WaitClosed 等待、RequestClose/
// CloseStatus、运行时代际都不再存在。改到新口径后逐条对应：
//
//   t_begin_start_runtime
//       — 运行时只初始化一次（IsInitialized），无 Stop/代际。
//   t_ctx_uses_listener_budget_only
//       — ctx.deadline 只取 listener 本地预算；不信任自定义 deadline
//         头；peer_principal 未认证为空；无 cancel 字段。
//   t_unsent_request_never_reaches_wire
//       — 未发送请求直接丢弃，不上网：本地期限已过期的请求、以及已
//         Close 的 client 上的请求，都在建 socket 之前终止；对端在
//         有界窗口内没有接到任何连接、任何字节。
//   t_owner_close_returns_terminal_for_sent_request
//       — owner 主动同步 Close()：已发送请求立即返回终态（Closed），
//         且每个请求只交付一次终态。
//   t_late_response_consumed_not_delivered
//       — 终端先落定（本地期限），对端其后才写的合法响应不再产生
//         第二次交付；首次终态不被覆盖。
//   t_server_close_does_not_preempt_handler
//       — server Close() 同步返回（有界等在途归零）；已接纳 handler
//         不被抢占，其回复在已关闭的会话上只消费不交付。
//   t_client_close_drains_inflight_read
//       — client 在途 async_read 时 Close() 仍同步返回并物理收口。
//   t_server_close_drains_idle_session
//       — 无 handler 的空闲会话也计入物理收口：对端见到 FIN。
//   t_timeout_caller_returns_cleanup_continues
//       — 期限到点让调用方及时返回，底层清理继续（对端见 EOF）。
//   t_runtime_close_converges_all_children
//       — runtime Close() 收口全部子对象；落定后工厂与请求一律 Closed。
//   t_end_physical_convergence
//       — 收尾用显式 Close() + 物理收敛断言，不用 Scheduler::Stop()。
//
// 同步纪律：跨线程/跨协程一律用原子标志 + WaitUntil（带总预算）与
// CountDownLatch 做屏障；handler 内的等待用 bbtco_sleep 轮询真实事件
// 标志。所有等待都有超时上限，不会无限挂起。

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

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <thread>

using namespace bbt::infra;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

constexpr std::size_t kTestMaxBody = 64 * 1024;
constexpr int         kBudgetMs    = 15000;

std::shared_ptr<NetworkRuntime> g_runtime;
std::shared_ptr<HttpClient>     g_client;
std::shared_ptr<HttpServer>     g_server;
std::uint16_t                   g_port = 0;
NetworkLimits                   g_limits{};

std::atomic_int g_handler_calls{0};
std::atomic_int g_handler_done{0};

NetworkLimits MakeLimits(std::chrono::milliseconds incoming) {
    NetworkLimits limits{};
    limits.max_connections  = 64;
    limits.max_inflight     = 64;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = kTestMaxBody;
    limits.incoming_timeout = incoming;
    return limits;
}

// 独立 runtime（各用例自有，收尾各自显式 Close）。
std::shared_ptr<NetworkRuntime> MakeRuntime(NetworkLimits limits) {
    auto rt = NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt);
    auto p = std::move(rt).value();
    BOOST_REQUIRE(p->Start());
    return p;
}

// 真实 TCP connect：返回 0（连接成功）或 errno。Close 之后必须是
// ECONNREFUSED——这是「listener fd 已物理释放」的线级证据，不用 stub、
// 逻辑计数或 IsClosed 轮询充当替身。
int ConnectErrnoTo(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
        return -1;
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = htons(port);
    const int rc  = ::connect(fd, reinterpret_cast<sockaddr*>(&addr),
                              sizeof(addr));
    const int err = rc == 0 ? 0 : errno;
    ::close(fd);
    return err;
}

// handler 内部状态对外可见的门闩/旗标集合；每条门径用一份，互不干扰。
// deliveries 记录该 handler 产出的终态交付次数（每请求必须恰好一次）。
struct Gate {
    std::atomic_bool entered{false};
    std::atomic_bool release{false};
    std::atomic_bool exited{false};
    std::atomic_int  deliveries{0};
    bbt::coroutine::Deadline              seen_deadline{};
    std::string                           seen_peer{"<unset>"};
    std::chrono::steady_clock::time_point entry_tp{};
};

Gate g_obs;    // /observe：只记录上下文后返回
Gate g_hold;   // /hold：进入→等放行→退出（专用 server 关闭用例）
Gate g_dl;     // /dl：短预算 runtime 上的在途路径

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

result<HttpResponse> CallApiOn(const std::shared_ptr<HttpClient>& client,
                               HttpRequest req, CallOptions options) {
    std::optional<result<HttpResponse>> out;
    bool ran = RunInCoroutine(
        [&] { out.emplace(client->Request(std::move(req), options)); });
    if (!ran)
        return result<HttpResponse>::err(
            MakeError(ErrorCode::InternalError, "test: coroutine did not run"));
    return std::move(*out);
}

result<HttpResponse> CallApi(HttpRequest req, CallOptions options) {
    return CallApiOn(g_client, std::move(req), options);
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

// handler 侧轮询真实事件：等放行，只受 bbtco_sleep 调度粒度影响，不假设
// 任何到达顺序；上限兜底防挂死（不占用对端/I/O 线程）。
void WaitForRelease(const std::atomic_bool& release) {
    for (int i = 0; i < 8000 && !release.load(); ++i)
        bbtco_sleep(2);
}

// /hold 系列公共形态：进入→等放行→记录退出与终态交付次数→返回。
result<HttpResponse> HoldHandler(Gate& g) {
    g.entered.store(true);
    WaitForRelease(g.release);
    g.exited.store(true);
    g.deliveries.fetch_add(1);
    return result<HttpResponse>::ok(HttpResponse{200, {}, "hold done"});
}

result<HttpResponse> TestHandler(IncomingCallContext ctx, HttpRequest req) {
    g_handler_calls.fetch_add(1);
    struct DoneGuard {
        ~DoneGuard() { g_handler_done.fetch_add(1); }
    } guard;
    (void)guard;

    if (req.url == "/ok")
        return result<HttpResponse>::ok(HttpResponse{200, {}, "ok"});

    if (req.url == "/observe") {
        g_obs.entry_tp      = std::chrono::steady_clock::now();
        g_obs.seen_deadline = ctx.deadline;
        g_obs.seen_peer     = ctx.peer_principal;
        g_obs.entered.store(true);
        return result<HttpResponse>::ok(HttpResponse{200, {}, "obs"});
    }
    if (req.url == "/hold") return HoldHandler(g_hold);
    if (req.url == "/dl") {
        g_dl.entered.store(true);
        WaitForRelease(g_dl.release);
        g_dl.exited.store(true);
        g_dl.deliveries.fetch_add(1);
        return result<HttpResponse>::ok(HttpResponse{200, {}, "dl done"});
    }

    return result<HttpResponse>::ok(HttpResponse{404, {}, "not found"});
}

// 对端侧观察到的连接关闭是否属于「连接被中止」的合法分类。
bool IsShutdownError(const Error& e) {
    return e.code == ErrorCode::TransportError ||
           e.code == ErrorCode::ProtocolError ||
           e.code == ErrorCode::Cancelled;
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
    std::atomic_int bytes{0};
    std::atomic_bool got_conn{false};
    std::shared_ptr<std::atomic_bool> done{
        std::make_shared<std::atomic_bool>(false)};

    explicit RawServer(std::function<void(int)> serve,
                       int accept_timeout_ms = 5000) {
        int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
        BOOST_REQUIRE(lfd >= 0);
        int one = 1;
        ::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        timeval tv{accept_timeout_ms / 1000,
                   (accept_timeout_ms % 1000) * 1000};
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
        auto conn_seen = &got_conn;
        auto byte_count = &bytes;
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [lfd, serve = std::move(serve), flag, conn_seen, byte_count] {
                int c = ::accept(lfd, nullptr, nullptr);
                if (c >= 0) {
                    conn_seen->store(true);
                    char buf[1024];
                    ssize_t n = ::recv(c, buf, sizeof(buf), 0);
                    if (n > 0)
                        byte_count->fetch_add(static_cast<int>(n));
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
    std::string UrlFor(const std::string& target) const {
        return "http://127.0.0.1:" + std::to_string(port) + target;
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
    // 进程寿命运行时：只有 IsInitialized，没有代际、没有 Stop/重启。
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    g_limits = MakeLimits(std::chrono::milliseconds{10000});

    g_runtime = MakeRuntime(g_limits);

    auto cl = g_runtime->CreateHttpClient();
    BOOST_REQUIRE(cl);
    g_client = std::move(cl).value();

    auto srv = g_runtime->ListenHttp({"127.0.0.1", 0}, TestHandler);
    BOOST_REQUIRE(srv);
    g_server = std::move(srv).value();
    g_port = g_server->LocalAddress().port;
    BOOST_REQUIRE_NE(g_port, 0);
}

// 回归（父探针 F1 同根）：Close() 返回即 listener 物理释放。此前实现把
// teardown 投递到 strand 后只按逻辑计数判定 Closed——无在途连接时计数原为
// 0，等待立即通过，Close 在 acceptor fd 仍存活时返回（实测其返回后 TCP
// connect 仍成功，需额外 Tick 才拒绝）。本用例以真实 TCP connect 取证。
BOOST_AUTO_TEST_CASE(t_close_releases_listener_fd_before_return) {
    auto runtime = MakeRuntime(MakeLimits(std::chrono::milliseconds{1000}));
    auto listening = runtime->ListenHttp(
        {"127.0.0.1", 0}, [](IncomingCallContext, HttpRequest) {
            return result<HttpResponse>::ok(HttpResponse{200, {}, "unused"});
        });
    BOOST_REQUIRE(listening);
    auto server = std::move(listening).value();
    const auto port = server->LocalAddress().port;
    BOOST_REQUIRE_NE(port, 0);

    // 无用户 handler、无请求在途：Close 之前从未有过连接。
    server->Close();
    BOOST_CHECK(server->IsClosed());              // 契约：返回即已物理落定
    BOOST_CHECK_EQUAL(ConnectErrnoTo(port), ECONNREFUSED);  // 物理证据

    runtime->Close();
}

// 同一接口声明允许域内的重复/并发 Close：返回时给出同一物理事实。
BOOST_AUTO_TEST_CASE(t_concurrent_close_keeps_listener_released) {
    auto runtime = MakeRuntime(MakeLimits(std::chrono::milliseconds{1000}));
    auto listening = runtime->ListenHttp(
        {"127.0.0.1", 0}, [](IncomingCallContext, HttpRequest) {
            return result<HttpResponse>::ok(HttpResponse{200, {}, "unused"});
        });
    BOOST_REQUIRE(listening);
    auto server = std::move(listening).value();
    const auto port = server->LocalAddress().port;
    BOOST_REQUIRE_NE(port, 0);

    std::thread other([&] { server->Close(); });
    server->Close();
    other.join();

    BOOST_CHECK(server->IsClosed());
    BOOST_CHECK_EQUAL(ConnectErrnoTo(port), ECONNREFUSED);

    runtime->Close();
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

    const auto dl = g_obs.seen_deadline;
    // deadline = 读请求起始时刻 + 本地预算。读起始必然晚于调用发起 t0，
    // 且不晚于 handler 进入时刻 entry_tp。
    BOOST_CHECK(dl >= t0 + g_limits.incoming_timeout);
    BOOST_CHECK(dl <= g_obs.entry_tp + g_limits.incoming_timeout);
    // handler 进入时期限仍在未来（10s 预算远大于 loopback 延迟）。
    BOOST_CHECK(dl > g_obs.entry_tp);
    // 未认证 peer 为空（入站上下文不再携带任何取消身份）。
    BOOST_CHECK(g_obs.seen_peer.empty());
}

BOOST_AUTO_TEST_CASE(t_unsent_request_never_reaches_wire) {
    // 对端 accept 超时 600ms：用例只在有界窗口内观察「有没有连接」，
    // 不靠长等待推断。accept 一旦有连接会立即返回，故窗口内
    // got_conn 保持 false 即「没有任何请求上网」的线级证据。
    RawServer peer([](int) {}, /*accept_timeout_ms=*/600);

    // (a) 本地 deadline 在发起前就已过期：等待事件根本不登记、回调不
    //     投递，请求从未被登记/发出 → 直接丢弃。
    CallOptions expired;
    expired.deadline = std::chrono::steady_clock::now() -
                       std::chrono::milliseconds(1);
    auto res_expired = CallApi(
        HttpRequest{"GET", peer.UrlFor("/x"), {}, ""}, expired);
    BOOST_REQUIRE(!res_expired);
    BOOST_CHECK(res_expired.error().code == ErrorCode::TimedOut);

    // (b) 已 Close 的 client：Request 在建立 socket/resolver 之前就
    //     返回 Closed（终态），同样不上网。
    auto cl = g_runtime->CreateHttpClient();
    BOOST_REQUIRE(cl);
    auto closed_client = std::move(cl).value();
    closed_client->Close();
    BOOST_CHECK(closed_client->IsClosed());
    auto res_closed = CallApiOn(closed_client,
                                HttpRequest{"GET", peer.UrlFor("/x"), {}, ""},
                                DefaultOptions(500));
    BOOST_REQUIRE(!res_closed);
    BOOST_CHECK(res_closed.error().code == ErrorCode::Closed);

    // 两条路径都没有把任何字节送到对端：未发送请求直接丢弃。
    // 等到对端的 accept 窗口（600ms）自然结束再断言——整段窗口内对端
    // 都没有接到连接，是线级的「没有任何请求上网」证据（accept 一旦
    // 有连接立即返回，不存在迟到的连接被漏看的可能）。
    BOOST_REQUIRE(WaitUntil([&] { return peer.done->load(); }, 4000));
    BOOST_CHECK(!peer.got_conn.load());
    BOOST_CHECK_EQUAL(peer.bytes.load(), 0);
}

BOOST_AUTO_TEST_CASE(t_close_returns_terminal_for_sent_request) {
    // 对端收到请求后压住不响应：请求确实已发送。
    std::atomic_bool peer_release{false};
    std::atomic_bool peer_wrote{false};
    RawServer peer([&](int c) {
        for (int i = 0; i < 8000 && !peer_release.load(); ++i)
            bbtco_sleep(2);
        const char late[] = "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\nlate";
        ::send(c, late, sizeof(late) - 1, MSG_NOSIGNAL);
        peer_wrote.store(true);
    });

    auto cl = g_runtime->CreateHttpClient();
    BOOST_REQUIRE(cl);
    auto client = std::move(cl).value();

    std::optional<result<HttpResponse>> out;
    std::atomic_bool out_ready{false};
    std::atomic_int  deliveries{0};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            HttpRequest req{"GET", peer.UrlFor("/x"), {}, ""};
            out.emplace(client->Request(std::move(req), DefaultOptions(20000)));
            deliveries.fetch_add(1);
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return peer.got_conn.load(); }));
    BOOST_REQUIRE(WaitUntil([&] { return peer.bytes.load() > 0; }));

    // owner 主动同步 Close()：已发送请求立即以终态返回，不等对端响应。
    client->Close();
    BOOST_CHECK(client->IsClosed());
    BOOST_REQUIRE(
        WaitUntil([&] { return out_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(out.has_value());
    BOOST_REQUIRE(!out.value());
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
    BOOST_CHECK_EQUAL(deliveries.load(), 1);   // 每请求只交付一次终态

    // 关闭后新请求一律 Closed（未发送直接丢弃，不上网）。
    auto res2 = CallApiOn(client, HttpRequest{"GET", peer.UrlFor("/x"), {}, ""},
                          DefaultOptions(500));
    BOOST_REQUIRE(!res2);
    BOOST_CHECK(res2.error().code == ErrorCode::Closed);

    // 对端此刻才写的响应不产生第二次交付：终态不被覆盖、交付次数不变。
    peer_release.store(true);
    BOOST_REQUIRE(WaitUntil([&] { return peer_wrote.load(); }, 8000));
    BOOST_CHECK_EQUAL(deliveries.load(), 1);
    BOOST_REQUIRE(!out.value());
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
    BOOST_CHECK(client->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_late_response_consumed_not_delivered) {
    // 终端先落定（本地 400ms 期限），对端其后才写合法响应。
    std::atomic_bool peer_release{false};
    std::atomic_bool peer_wrote{false};
    RawServer peer([&](int c) {
        for (int i = 0; i < 8000 && !peer_release.load(); ++i)
            bbtco_sleep(2);
        const char late[] =
            "HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\nlate-ok";
        ::send(c, late, sizeof(late) - 1, MSG_NOSIGNAL);
        peer_wrote.store(true);
    });

    auto cl = g_runtime->CreateHttpClient();
    BOOST_REQUIRE(cl);
    auto client = std::move(cl).value();

    std::optional<result<HttpResponse>> out;
    std::atomic_bool out_ready{false};
    std::atomic_int  deliveries{0};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            HttpRequest req{"GET", peer.UrlFor("/x"), {}, ""};
            out.emplace(client->Request(std::move(req), DefaultOptions(400)));
            deliveries.fetch_add(1);
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return peer.got_conn.load(); }));
    BOOST_REQUIRE(
        WaitUntil([&] { return out_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(out.has_value());
    BOOST_REQUIRE(!out.value());
    BOOST_CHECK(out->error().code == ErrorCode::TimedOut);

    // 对端迟到的合法响应：只被消费（连接已中止），不再交付给业务。
    peer_release.store(true);
    BOOST_REQUIRE(WaitUntil([&] { return peer_wrote.load(); }, 8000));
    BOOST_CHECK_EQUAL(deliveries.load(), 1);
    BOOST_REQUIRE(!out.value());
    BOOST_CHECK(out->error().code == ErrorCode::TimedOut);

    // 超时请求不损害后续正常请求（runtime 与 client 仍可用）。
    auto res_ok = CallApi(HttpRequest{"GET", Url("/ok"), {}, ""},
                          DefaultOptions());
    BOOST_REQUIRE(res_ok);
    BOOST_CHECK_EQUAL(res_ok.value().status, 200u);
}

BOOST_AUTO_TEST_CASE(t_server_close_does_not_preempt_handler) {
    // 专用 server（不动套件共享的 g_server）：handler 被门闩压住。
    auto srv = g_runtime->ListenHttp({"127.0.0.1", 0}, TestHandler);
    BOOST_REQUIRE(srv);
    auto server = std::move(srv).value();
    const std::uint16_t port = server->LocalAddress().port;

    std::optional<result<HttpResponse>> out;
    std::atomic_bool out_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            HttpRequest req{"GET",
                "http://127.0.0.1:" + std::to_string(port) + "/hold",
                {}, ""};
            out.emplace(g_client->Request(std::move(req), DefaultOptions(20000)));
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return g_hold.entered.load(); }));

    // owner 主动同步 Close()：有界等在途归零（≤ 5s）后返回即 IsClosed。
    server->Close();
    BOOST_CHECK(server->IsClosed());
    // 已接纳 handler 不被抢占：Close 返回时它仍在运行（未 exited）。
    BOOST_CHECK(g_hold.entered.load());
    BOOST_CHECK(!g_hold.exited.load());
    // 关闭期客户端在途请求立即拿到终态（不许假成功）。
    BOOST_REQUIRE(
        WaitUntil([&] { return out_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(out.has_value());
    BOOST_REQUIRE(!out.value());
    BOOST_CHECK(IsShutdownError(out->error()));

    // 放行 handler：它照常跑完并产出唯一一次终态；会话已物理关闭，
    // 回复只被消费不再交付，也不产生崩溃或重复交付。
    g_hold.release.store(true);
    BOOST_REQUIRE(WaitUntil([&] { return g_hold.exited.load(); }));
    BOOST_CHECK_EQUAL(g_hold.deliveries.load(), 1);
    BOOST_CHECK(server->IsClosed());
}

// Finding B 回归（冻结契约 §6 同根）：handler 已产出响应（排队/正在发送）时
// owner Close()，返回当刻会话 socket 必须已物理关闭、未发送 payload 被丢弃。
// 此前 owner teardown 经 Abort→Close 命中 reply_posted 的「写完再关」分支：
// 只置 close_after_write 就返回，socket 仍存活且被后端继续写（会话仍在册还
// 会让 Close 阻塞整个 5s drain 窗口）。
// 差分取证用「不读」的对端 + 大于内核发送缓冲上界（tcp_wmem max 4MB）的响应体：
//   - 旧语义：Close 阻塞 ~5s 后返回且会话仍开着，对端一开始读就把整个响应体
//     读完（写路径把剩余字节继续推出），total == 响应体；
//   - 新语义：Close 在域门内同步断开并丢弃未发送数据 → 立即返回且 IsClosed，
//     对端读到的字节必然少于响应体（未发送部分不补发），随后 EOF，且 EOF 后
//     不再有新字节（晚到完成不再写资源）。
BOOST_AUTO_TEST_CASE(t_server_owner_close_discards_queued_response) {
    constexpr std::size_t kBigBody = 8 * 1024 * 1024;
    NetworkLimits limits = MakeLimits(std::chrono::milliseconds{10000});
    limits.max_body_bytes = kBigBody + 4096;
    auto rt = MakeRuntime(limits);

    std::atomic_bool produced{false};
    auto srv = rt->ListenHttp(
        {"127.0.0.1", 0}, [&](IncomingCallContext, HttpRequest) {
            produced.store(true, std::memory_order_release);
            return result<HttpResponse>::ok(
                HttpResponse{200, {}, std::string(kBigBody, 'x')});
        });
    BOOST_REQUIRE(srv);
    auto server = std::move(srv).value();
    const std::uint16_t port = server->LocalAddress().port;
    BOOST_REQUIRE_NE(port, 0);

    // 对端：小接收窗口 + Close 之前不读，使「在途发送」停在发送缓冲里。
    const int peer = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(peer >= 0);
    const int rcvbuf = 1024;
    ::setsockopt(peer, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
    timeval tv{2, 0};
    ::setsockopt(peer, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = htons(port);
    BOOST_REQUIRE(::connect(peer, reinterpret_cast<sockaddr*>(&addr),
                            sizeof(addr)) == 0);
    const char req[] =
        "GET /big HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    BOOST_REQUIRE(::send(peer, req, sizeof(req) - 1, MSG_NOSIGNAL) ==
                  static_cast<ssize_t>(sizeof(req) - 1));

    // handler 已产出响应；再给 io 域时间把 DeliverResult 落到写路径上。
    BOOST_REQUIRE(WaitUntil([&] { return produced.load(std::memory_order_acquire); }));
    std::this_thread::sleep_for(std::chrono::milliseconds{500});

    const auto t0 = std::chrono::steady_clock::now();
    server->Close();
    const auto close_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0).count();
    const bool closed_at_return = server->IsClosed();

    // 读到 EOF 才停：total < kBigBody 即「未发送 payload 随 Close 丢弃」。
    std::size_t total = 0;
    bool        eof   = false;
    const auto  deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds{5};
    while (std::chrono::steady_clock::now() < deadline) {
        char          buf[64 * 1024];
        const ssize_t n = ::recv(peer, buf, sizeof(buf), 0);
        if (n > 0) {
            total += static_cast<std::size_t>(n);
            continue;
        }
        if (n == 0)
            eof = true;
        break;
    }
    // 晚到完成不再写资源：EOF 之后再给一段窗口，确认没有新字节。
    std::this_thread::sleep_for(std::chrono::milliseconds{200});
    char          extra{};
    const ssize_t after = ::recv(peer, &extra, 1, MSG_DONTWAIT);

    BOOST_CHECK_MESSAGE(closed_at_return, "Close 返回当刻未 IsClosed");
    BOOST_CHECK_MESSAGE(eof, "Close 之后对端未看到 EOF：socket 未物理关闭");
    BOOST_CHECK_MESSAGE(total < kBigBody,
        "对端收到完整响应体，未发送数据未被丢弃 total=" << total);
    BOOST_CHECK_MESSAGE(after <= 0, "EOF 之后后端仍在写该 socket");
    BOOST_CHECK_MESSAGE(close_ms < 2000,
        "owner Close 未同步返回（疑似延迟到写完）close_ms=" << close_ms);

    ::close(peer);
    rt->Close();
    BOOST_CHECK(rt->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_client_close_drains_inflight_read) {
    // 独立 runtime：裸对端收完请求后不再回复，client 停在 async_read。
    // Close() 必须物理收口这条 completion，且不得挂死。
    auto rtp = MakeRuntime(MakeLimits(std::chrono::milliseconds{10000}));
    auto cl = rtp->CreateHttpClient();
    BOOST_REQUIRE(cl);
    auto client = std::move(cl).value();

    std::atomic_bool peer_eof{false};
    RawServer silent([&](int c) {
        char buf[512];
        while (::recv(c, buf, sizeof(buf), 0) > 0) {
        }
        peer_eof.store(true);
    });

    std::optional<result<HttpResponse>> out;
    std::atomic_bool out_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            HttpRequest req{"GET", silent.UrlFor("/x"), {}, ""};
            out.emplace(client->Request(std::move(req), DefaultOptions(20000)));
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return silent.bytes.load() > 0; }));

    client->Close();
    BOOST_CHECK(client->IsClosed());
    BOOST_REQUIRE(
        WaitUntil([&] { return out_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(out.has_value());
    BOOST_REQUIRE(!out.value());
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
    // 物理收口：对端见到连接被中止（EOF）。
    BOOST_REQUIRE(WaitUntil([&] { return peer_eof.load(); }, 8000));

    rtp->Close();
    BOOST_CHECK(rtp->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_server_close_drains_idle_session) {
    // 空闲会话也计入物理收口：先打一条 keep-alive 请求并读到 200
    // （证明会话真实存在、且此后停在 async_read 上、无 pending handler），
    // 再 Close()——对端必须在同一连接上读到 FIN。
    auto rtp = MakeRuntime(MakeLimits(std::chrono::milliseconds{10000}));
    auto srv = rtp->ListenHttp({"127.0.0.1", 0}, TestHandler);
    BOOST_REQUIRE(srv);
    auto server = std::move(srv).value();

    int fd = TcpConnectTo(server->LocalAddress().port);
    BOOST_REQUIRE(fd >= 0);

    const char raw[] = "GET /ok HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n";
    BOOST_REQUIRE(::send(fd, raw, sizeof(raw) - 1, MSG_NOSIGNAL) > 0);
    std::string got;
    char buf[1024];
    while (got.find("\r\n\r\n") == std::string::npos) {
        const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        BOOST_REQUIRE(n > 0);
        got.append(buf, static_cast<std::size_t>(n));
    }
    BOOST_CHECK(got.find("200") != std::string::npos);

    server->Close();
    BOOST_CHECK(server->IsClosed());

    // 物理关闭的直接证据：对端读到 FIN（recv == 0，不是超时/半开）。
    ssize_t n = -1;
    for (int i = 0; i < 64; ++i) {
        n = ::recv(fd, buf, sizeof(buf), 0);
        if (n <= 0)
            break;
    }
    BOOST_CHECK_EQUAL(n, 0);
    ::close(fd);

    rtp->Close();
    BOOST_CHECK(rtp->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_server_deadline_closes_session_not_handler) {
    // 第二套 runtime：listener 本地预算 300ms。期限到点只关会话，
    // 已接纳 handler 不被抢占，仍跑完后产出唯一一次终态。
    auto rtp = MakeRuntime(MakeLimits(std::chrono::milliseconds{300}));
    auto srv = rtp->ListenHttp({"127.0.0.1", 0}, TestHandler);
    BOOST_REQUIRE(srv);
    auto server = std::move(srv).value();

    HttpRequest req{"GET",
        "http://127.0.0.1:" + std::to_string(server->LocalAddress().port) +
            "/dl",
        {}, ""};
    auto res = CallApi(std::move(req), DefaultOptions());
    // 期限到点切断回复路径：client 见 Error（不是假成功）。
    BOOST_REQUIRE(!res);
    BOOST_CHECK(IsShutdownError(res.error()));
    // handler 未被抢占：期限只关会话，handler 仍在运行（未 exited）。
    BOOST_REQUIRE(WaitUntil([&] { return g_dl.entered.load(); }));
    BOOST_CHECK(!g_dl.exited.load());
    // 放行后它照常跑完并交付唯一一次终态。
    g_dl.release.store(true);
    BOOST_REQUIRE(WaitUntil([&] { return g_dl.exited.load(); }));
    BOOST_CHECK_EQUAL(g_dl.deliveries.load(), 1);

    rtp->Close();
    BOOST_CHECK(rtp->IsClosed());
    BOOST_CHECK(server->IsClosed());
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
    HttpRequest req{"GET", silent.UrlFor("/x"), {}, ""};
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

BOOST_AUTO_TEST_CASE(t_runtime_close_converges_all_children) {
    // 新 server + 在途出站请求（裸对端永不响应）。
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

    std::optional<result<HttpResponse>> out_silent;
    std::atomic_bool silent_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            HttpRequest req{"GET", silent.UrlFor("/x"), {}, ""};
            out_silent.emplace(
                g_client->Request(std::move(req), DefaultOptions(30000)));
            silent_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return silent.bytes.load() > 0; }));

    // runtime Close()：封口（拒新子对象）→ 逐个同步 Close 子对象 +
    // transport owner 收口 → 有界等在途归零 → 封口 io 域。返回即物理释放。
    g_runtime->Close();
    BOOST_CHECK(g_runtime->IsClosed());
    BOOST_CHECK(g_server->IsClosed());
    BOOST_CHECK(srv2p->IsClosed());
    BOOST_CHECK(g_client->IsClosed());

    // 在途请求以终态落定（不许挂住、不许假成功）。
    BOOST_REQUIRE(
        WaitUntil([&] { return silent_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(out_silent.has_value());
    BOOST_REQUIRE(!out_silent.value());
    BOOST_REQUIRE(WaitUntil([&] { return peer_eof.load(); }, 8000));

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

BOOST_AUTO_TEST_CASE(t_end_physical_convergence) {
    // 进程寿命运行时没有 Stop：收尾靠显式 Close() + 物理收敛断言。
    BOOST_CHECK(g_scheduler->IsInitialized());
    g_runtime->Close();   // 幂等
    BOOST_CHECK(g_runtime->IsClosed());
    BOOST_CHECK(g_server->IsClosed());
    BOOST_CHECK(g_client->IsClosed());
    // 托管集合收敛：handler 调用数与退出数配平，无在途 handler 遗留。
    BOOST_CHECK_EQUAL(g_handler_done.load(), g_handler_calls.load());
    BOOST_CHECK(!g_hold.entered.load() || g_hold.exited.load());

    g_client.reset();
    g_server.reset();
    g_runtime.reset();
}

BOOST_AUTO_TEST_SUITE_END()
