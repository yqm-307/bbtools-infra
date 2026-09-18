// N1b-1：真实 HTTP/1.1 loopback 闭环验收。
// 覆盖契约 §118 行边界：200/404 为有效响应、4xx/5xx 不是网络错误、
// 超限/畸形 framing/断连为 Error、重复 header 保留、https 显式拒绝、
// 非协程调用 InvalidContext、deadline/cancel 映射、关闭顺序与幂等。
//
// 与上游单测一致：Boost.Test 经 included/unit_test.hpp 静态内嵌，
// 不链接 libboost_unit_test_framework.so。

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
#include <dirent.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/asio/post.hpp>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/detail/Hook.hpp>
#include <bbt/coroutine/io/IoExecutor.hpp>
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

constexpr std::size_t kTestMaxBody   = 64 * 1024;
constexpr int         kBudgetMs      = 15000;

std::shared_ptr<NetworkRuntime> g_runtime;
std::shared_ptr<HttpClient>     g_client;
std::shared_ptr<HttpServer>     g_server;
std::uint16_t                   g_port = 0;

std::atomic_bool g_handler_in_coroutine{false};
std::atomic_int  g_handler_calls{0};
std::atomic_int  g_handler_done{0};

// 协程内执行任务并限时等待；任何一步失败返回 false（测试不得无限挂住）。
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

result<HttpResponse> TestHandler(IncomingCallContext ctx, HttpRequest req) {
    g_handler_in_coroutine.store(g_bbt_tls_coroutine_co != nullptr);
    g_handler_calls.fetch_add(1);
    struct DoneGuard { ~DoneGuard() { g_handler_done.fetch_add(1); } } guard;
    (void)guard;
    (void)ctx;

    if (req.url == "/ok")
        return result<HttpResponse>::ok(
            HttpResponse{200, {{"X-Test", "yes"}}, "hello world"});
    if (req.url == "/dup") {
        int count = 0;
        for (const auto& h : req.headers)
            if (h.first == "X-Dup")
                ++count;
        return result<HttpResponse>::ok(
            HttpResponse{200, {}, std::to_string(count)});
    }
    if (req.url == "/slow") {
        bbtco_sleep(800);   // handler 允许挂起：验证协程执行环境
        return result<HttpResponse>::ok(HttpResponse{200, {}, "slow done"});
    }
    if (req.url == "/fail")
        return result<HttpResponse>::err(
            MakeError(ErrorCode::InternalError, "simulated handler failure"));
    return result<HttpResponse>::ok(HttpResponse{404, {}, "not found"});
}

// POSIX 裸连接助手：充当对端发任意字节/构造畸形响应，与实现解耦。
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

std::string RawExchange(std::uint16_t port, const std::string& bytes) {
    int fd = TcpConnectTo(port);
    if (fd < 0)
        return {};
    std::string got;
    if (::send(fd, bytes.data(), bytes.size(), MSG_NOSIGNAL) >= 0) {
        char buf[4096];
        for (;;) {
            const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n <= 0)
                break;
            got.append(buf, static_cast<std::size_t>(n));
        }
    }
    ::close(fd);
    return got;
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

// /proc/self/task 线程计数：用于断言 HTTP I/O 不新增线程。
long TaskCount() {
    DIR* d = ::opendir("/proc/self/task");
    if (!d)
        return -1;
    long n = -2;   // 扣除 "." 与 ".."
    while (::readdir(d))
        ++n;
    ::closedir(d);
    return n;
}

} // namespace

BOOST_AUTO_TEST_SUITE(http_loopback)

BOOST_AUTO_TEST_CASE(t_begin_start_runtime) {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 2;
    g_scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(g_scheduler->IsRunning());

    NetworkLimits limits{};
    limits.max_connections = 64;
    limits.max_inflight    = 64;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = kTestMaxBody;
    limits.incoming_timeout = std::chrono::milliseconds{3000};

    auto rt = NetworkRuntime::Create(limits);
    BOOST_REQUIRE(rt);
    g_runtime = std::move(rt).value();
    auto started = g_runtime->Start();
    BOOST_REQUIRE(started);

    auto cl = g_runtime->CreateHttpClient();
    BOOST_REQUIRE(cl);
    g_client = std::move(cl).value();
}

BOOST_AUTO_TEST_CASE(t_listen_and_get_200) {
    auto srv = g_runtime->ListenHttp({"127.0.0.1", 0}, TestHandler);
    BOOST_REQUIRE(srv);
    g_server = std::move(srv).value();
    g_port = g_server->LocalAddress().port;
    BOOST_REQUIRE_NE(g_port, 0);

    HttpRequest req{"GET", Url("/ok"), {}, ""};
    auto res = CallApi(std::move(req), DefaultOptions());
    BOOST_REQUIRE(res);
    BOOST_CHECK_EQUAL(res.value().status, 200u);
    BOOST_CHECK_EQUAL(res.value().body, "hello world");
    bool saw_x_test = false;
    for (const auto& h : res.value().headers)
        if (h.first == "X-Test" && h.second == "yes")
            saw_x_test = true;
    BOOST_CHECK(saw_x_test);
    // handler 必须在 infra 协程执行环境运行
    BOOST_CHECK(g_handler_in_coroutine.load());
}

BOOST_AUTO_TEST_CASE(t_shared_executor_drives_io_no_new_thread) {
    // 探针直接投递到共享 executor：必须由既有事件循环线程执行，
    // 不依赖 infra 自带任何 I/O 线程。
    bbt::core::thread::CountDownLatch probe{1};
    auto io_tid = std::this_thread::get_id();
    boost::asio::post(bbt::coroutine::io::GetExecutor(),
        [&] { io_tid = std::this_thread::get_id(); probe.Down(); });
    BOOST_REQUIRE(probe.WaitTimeout(kBudgetMs) == 0);
    BOOST_CHECK(io_tid != std::this_thread::get_id());
    // io 域线程与协程 worker 也不是同一条（sche 线程驱动 PollOnce）。
    auto co_tid = std::this_thread::get_id();
    BOOST_REQUIRE(RunInCoroutine(
        [&] { co_tid = std::this_thread::get_id(); }));
    BOOST_CHECK(io_tid != co_tid);

    // 基线：首个用例已预热 resolver 内部线程池；此后 HTTP I/O
    // 全部由既有线程推进，引擎不得再新增任何线程。
    const long before = TaskCount();
    BOOST_REQUIRE(before > 0);
    // server 侧：裸 POSIX 对端打一轮真实请求（不经 resolver）。
    const std::string reply = RawExchange(g_port,
        "GET /ok HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n");
    BOOST_REQUIRE(reply.find("200") != std::string::npos);
    // client 侧：再压几轮 loopback 请求。
    for (int i = 0; i < 4; ++i) {
        auto res = CallApi(HttpRequest{"GET", Url("/ok"), {}, ""},
                           DefaultOptions());
        BOOST_REQUIRE(res);
        BOOST_CHECK_EQUAL(res.value().status, 200u);
    }
    BOOST_CHECK_EQUAL(TaskCount(), before);
}

BOOST_AUTO_TEST_CASE(t_404_is_valid_response) {
    HttpRequest req{"GET", Url("/no/such/path"), {}, ""};
    auto res = CallApi(std::move(req), DefaultOptions());
    // 契约：4xx 是有效 HttpResponse，不是 Error。
    BOOST_REQUIRE(res);
    BOOST_CHECK_EQUAL(res.value().status, 404u);
    BOOST_CHECK_EQUAL(res.value().body, "not found");
}

BOOST_AUTO_TEST_CASE(t_500_handler_error_is_valid_response) {
    HttpRequest req{"GET", Url("/fail"), {}, ""};
    auto res = CallApi(std::move(req), DefaultOptions());
    BOOST_REQUIRE(res);
    BOOST_CHECK_EQUAL(res.value().status, 500u);
}

BOOST_AUTO_TEST_CASE(t_duplicate_headers_preserved) {
    HttpRequest req{"GET", Url("/dup"),
                    {{"X-Dup", "a"}, {"X-Dup", "b"}, {"X-Other", "c"}}, ""};
    auto res = CallApi(std::move(req), DefaultOptions());
    BOOST_REQUIRE(res);
    // handler 侧逐条按序收到两个 X-Dup，未被 map 折叠。
    BOOST_CHECK_EQUAL(res.value().body, "2");
}

BOOST_AUTO_TEST_CASE(t_concurrent_requests) {
    constexpr int kN = 8;
    bbt::core::thread::CountDownLatch done{kN};
    std::atomic_int                 ok_count{0};
    for (int i = 0; i < kN; ++i) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [i, &done, &ok_count] {
                HttpRequest req{"GET", Url("/ok"), {}, ""};
                auto r = g_client->Request(std::move(req), DefaultOptions());
                if (r && r.value().status == 200)
                    ok_count.fetch_add(1);
                done.Down();
            },
            succ);
        BOOST_REQUIRE(succ);
    }
    BOOST_REQUIRE(done.WaitTimeout(kBudgetMs) == 0);
    BOOST_CHECK_EQUAL(ok_count.load(), kN);
}

BOOST_AUTO_TEST_CASE(t_oversized_body_rejected_as_error) {
    // 客户端侧：body 超 max_body_bytes，本地拒绝为 Error，不发上网。
    HttpRequest req{"POST", Url("/ok"), {},
                    std::string(kTestMaxBody + 1, 'x')};
    auto res = CallApi(std::move(req), DefaultOptions());
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::InvalidArgument);

    // 服务端侧：裸对端声明超限 Content-Length，收到可观察的 413 拒绝。
    const std::string raw =
        "POST /ok HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 200000\r\n"
        "\r\n" + std::string(256, 'y');
    const std::string reply = RawExchange(g_port, raw);
    BOOST_CHECK(reply.find("413") != std::string::npos);
}

BOOST_AUTO_TEST_CASE(t_malformed_response_is_error) {
    RawServer peer([](int fd) {
        const char junk[] = "GARBAGE-NOT-HTTP\r\n\r\n";
        ::send(fd, junk, sizeof(junk) - 1, MSG_NOSIGNAL);
    });
    HttpRequest req{"GET", "http://127.0.0.1:" + std::to_string(peer.port) +
                            "/x", {}, ""};
    auto res = CallApi(std::move(req), DefaultOptions());
    // 畸形 framing：Error 而非成功响应。
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::ProtocolError);
}

BOOST_AUTO_TEST_CASE(t_disconnect_is_error) {
    RawServer peer([](int) { /* accept 后直接关闭，不发一字节 */ });
    HttpRequest req{"GET", "http://127.0.0.1:" + std::to_string(peer.port) +
                            "/x", {}, ""};
    auto res = CallApi(std::move(req), DefaultOptions());
    // 断连且不足一条消息：Error，不伪造成成功。
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::TransportError);
}

BOOST_AUTO_TEST_CASE(t_https_explicitly_rejected) {
    HttpRequest req{"GET", "https://127.0.0.1/secure", {}, ""};
    auto res = CallApi(std::move(req), DefaultOptions());
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::InvalidArgument);
    BOOST_CHECK_EQUAL(res.error().domain_code, "https_unsupported");

    req.url = "ftp://127.0.0.1/x";
    res = CallApi(std::move(req), DefaultOptions());
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_CASE(t_crlf_injection_rejected) {
    HttpRequest req{"GET", Url("/ok"), {{"X-Bad", "a\r\nInjected: 1"}}, ""};
    auto res = CallApi(std::move(req), DefaultOptions());
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::InvalidArgument);

    req.headers = {{"X-Ok", "v"}, {"Bad\rName", "v"}};
    res = CallApi(std::move(req), DefaultOptions());
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::InvalidArgument);

    req.headers = {};
    req.url = std::string("http://127.0.0.1:") + std::to_string(g_port) +
              "/ok\r\nX: 1";
    res = CallApi(std::move(req), DefaultOptions());
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_CASE(t_request_off_coroutine_rejected) {
    HttpRequest req{"GET", Url("/ok"), {}, ""};
    auto res = g_client->Request(std::move(req), DefaultOptions());
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::InvalidContext);
}

BOOST_AUTO_TEST_CASE(t_expired_deadline_is_timeout) {
    CallOptions opt;
    opt.deadline = std::chrono::steady_clock::now() -
                   std::chrono::milliseconds(1);
    HttpRequest req{"GET", Url("/ok"), {}, ""};
    auto res = CallApi(std::move(req), opt);
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::TimedOut);
}

BOOST_AUTO_TEST_CASE(t_cancel_pre_cancelled) {
    bbt::coroutine::CancellationSource src;
    src.RequestCancel();
    CallOptions opt = DefaultOptions();
    opt.cancel = src.Token();
    HttpRequest req{"GET", Url("/ok"), {}, ""};
    auto res = CallApi(std::move(req), opt);
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::Cancelled);
}

BOOST_AUTO_TEST_CASE(t_cancel_inflight) {
    const int done_before = g_handler_done.load();
    bbt::coroutine::CancellationSource src;
    CallOptions opt = DefaultOptions(20000);
    opt.cancel = src.Token();

    std::optional<result<HttpResponse>> out;
    std::atomic_bool out_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            HttpRequest req{"GET", Url("/slow"), {}, ""};
            out.emplace(g_client->Request(std::move(req), opt));
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    // handler 需 800ms；100ms 后取消必在决议前到达 → Cancelled。
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    src.RequestCancel();
    BOOST_REQUIRE(WaitUntil(
        [&] { return out_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Cancelled);
    // 等 handler 协程真正退出再进关闭用例（在途 token 生命周期）。
    BOOST_REQUIRE(WaitUntil(
        [&] { return g_handler_done.load() > done_before; }));
}

BOOST_AUTO_TEST_CASE(t_wait_closed_off_coroutine_is_invalid_context) {
    BOOST_CHECK(g_server->WaitClosed(Deadline{},
                                     bbt::coroutine::CancellationToken{}) ==
                CloseStatus::InvalidContext);
}

BOOST_AUTO_TEST_CASE(t_close_sequence) {
    g_server->StopAccepting();
    g_server->StopAccepting();   // 幂等

    std::atomic<CloseStatus> server_status{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        g_server->RequestClose();
        server_status.store(g_server->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10),
            {}));
    }));
    BOOST_CHECK(server_status.load() == CloseStatus::Closed);
    BOOST_CHECK(g_server->IsClosed());

    std::atomic<CloseStatus> rt_status{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        g_runtime->RequestClose();
        rt_status.store(g_runtime->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10),
            {}));
    }));
    BOOST_CHECK(rt_status.load() == CloseStatus::Closed);
    BOOST_CHECK(g_runtime->IsClosed());
    BOOST_CHECK(g_client->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_request_after_close_is_closed) {
    HttpRequest req{"GET", Url("/ok"), {}, ""};
    auto res = CallApi(std::move(req), DefaultOptions());
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::Closed);
}

BOOST_AUTO_TEST_CASE(t_end_stop_scheduler) {
    g_client.reset();
    g_server.reset();
    g_runtime.reset();
    g_scheduler->Stop();
}

BOOST_AUTO_TEST_SUITE_END()
