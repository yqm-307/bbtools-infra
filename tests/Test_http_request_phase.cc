// Issue #64：出站请求阶段与 OutcomeUnknown 分界的动态验收（S1–S7）。
//
// 验收口径（全部机器可判定，不用错误文本、errno、日志顺序或固定 sleep）：
//   - 请求尚未完整写出（未连接/写中断）→ 原有确定错误，且 request_phase
//     未达 RequestCommitted；
//   - 请求已完整写出但没有可信回复终态（对端确认收全后断开、deadline、
//     Close）→ ErrorCode::OutcomeUnknown 且 request_phase ==
//     RequestCommitted；
//   - 可信回复（200/404/500）保持既有成功/远端错误语义，不被升级为未知；
//     对端字节已到达但 framing 不可用仍是对回复的确定判决（ProtocolError）。
//
// 事件控制：对端的「已完整收到请求」由服务端自己读到请求结束符（marker）后才置位，
// 客户端的阶段由 result 里的 request_phase 直接读出，两者交叉
// 断言；需要「对端保持连接不回复」的场景由测试侧的显式放行闸门（release）
// 控制，超时只作失败上限，不作完成依据。所有等待都是有界谓词/条件变量
// 等待，不放宽断言。
//
// 独立审查 F-1/F-2/F-3 修复后的对应口径：
//   - F-1（S8）：放弃路径与 io 域在同一 IoGate 内依次完成 Abort 与阶段读取，
//     故「返回确定失败后不会再完整写出」；已完整写出仍是 OutcomeUnknown。用例用
//     on_write_started 事件确认客户端已进入写出，再让 deadline 在「写侧必然停在
//     未提交」的状态到点；断言放弃返回确定失败、阶段未提交、且放行对端后仍收不到
//     完整请求。限制见 S8 处：本运行时放弃与「io 同步写出」不可能并发，故该用例
//     不能动态区分未修复实现，F-1 的线性化由源码线性化点证明。
//   - F-2：协程任务的完成态是堆上共享对象（CoRunState），调用方提前返回/
//     断言失败离开作用域都不会让任务写入已析构的栈对象（不再有 &done/&out）。
//   - F-3（S2/S5-b）：精确断言 Writing 之前先等客户端侧的因果事件
//     on_write_started（同一 IoGate 内、进入写出阶段且尚未写出任何字节），
//     不再用「服务端 accept 计数」猜 OnConnect 是否已发生。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

#include "http/HttpClientImpl.hpp"   // 测试 seam：SetWriteStartedHookForTest

using namespace bbt::infra;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

constexpr std::size_t kTestMaxBody = 64 * 1024;
constexpr int         kBudgetMs    = 15000;
// 失败上限（不是完成依据）：任何"等待事件"最多等这么久就判失败。
constexpr int         kGateBudgetMs = 20000;

std::shared_ptr<NetworkRuntime> g_runtime;
std::shared_ptr<HttpClient>     g_client;

// 协程任务的完成态（F-2）：写入方是协程任务，读取方是测试线程，两者持有同一
// 份堆上共享对象。调用方在预算内未等到完成时提前返回（或断言失败离开作用域）
// 都不会留下悬垂引用——任务仍只写自己通过 shared_ptr 持有的这份状态。
template <class T>
struct CoRunState {
    bbt::core::thread::CountDownLatch done{1};
    std::optional<T>                 value;      // 任务落定的返回值
    std::atomic_bool                 ok{false};  // 任务是否已执行完 f
};

// 发起协程任务（不等待）。f 必须按值捕获它需要的一切，不得引用调用方栈。
// 返回 nullptr 表示调度器未接受该任务。
template <class F, class T = std::invoke_result_t<F>>
std::shared_ptr<CoRunState<T>> SpawnCoTask(F&& f) {
    auto state = std::make_shared<CoRunState<T>>();
    bool succ  = false;
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [fn = std::forward<F>(f), state]() mutable {
            state->value.emplace(fn());
            state->ok.store(true, std::memory_order_release);
            state->done.Down();
        },
        succ);
    if (!succ)
        return nullptr;
    return state;
}

// 在协程内执行 f 并限时等待它真正结束；返回堆上完成态（nullptr = 未被接受）。
// 超时返回时 ok 可能仍为 false（任务仍在跑），但调用方不持有任何被任务引用的
// 对象：共享态由任务侧的 shared_ptr 保活，不做「超时即析构栈对象」的假设。
template <class F, class T = std::invoke_result_t<F>>
std::shared_ptr<CoRunState<T>> RunInCoroutine(F&& f, int budget_ms = kBudgetMs) {
    auto state = SpawnCoTask<F, T>(std::forward<F>(f));
    if (state)
        state->done.WaitTimeout(budget_ms);
    return state;
}

// 显式放行闸门（条件变量事件门）：测试在客户端侧/服务端侧的因果事件兑现后才
// 放行下一步，避免用固定等待猜时序。有界等待只作失败上限，保证断言提前失败
// 时对端线程仍能退出、不被挂死。
class ReleaseGate {
public:
    void Open() {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_open = true;
        }
        m_cv.notify_all();
    }
    bool Wait(int budget_ms) {
        std::unique_lock<std::mutex> lk(m_mtx);
        return m_cv.wait_for(lk, std::chrono::milliseconds(budget_ms),
                             [this] { return m_open; });
    }

private:
    std::mutex              m_mtx;
    std::condition_variable m_cv;
    bool                    m_open{false};
};

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

CallOptions DeadlineAfter(int budget_ms) {
    CallOptions opt;
    opt.deadline = std::chrono::steady_clock::now() +
                   std::chrono::milliseconds(budget_ms);
    return opt;
}

NetworkLimits MakeLimits(std::size_t max_conn, std::size_t max_inflight,
                         std::size_t max_body = kTestMaxBody) {
    NetworkLimits limits{};
    limits.max_connections  = max_conn;
    limits.max_inflight     = max_inflight;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = max_body;
    limits.incoming_timeout = std::chrono::milliseconds{3000};
    return limits;
}

// S7 消费者视角：只经公开结果读分界，不接触 Beast/Asio/socket 类型，
// 不解析 message。
bool ConsumerSawCommittedWrite(const Error& e) {
    return e.request_phase.has_value() &&
           IsRequestCommitted(*e.request_phase);
}

std::string UrlOf(std::uint16_t port, const std::string& target) {
    return "http://127.0.0.1:" + std::to_string(port) + target;
}

// 在自有 client 上发一次请求（协程内），返回 result 供断言。
result<HttpResponse> CallOn(const std::shared_ptr<HttpClient>& client,
                            HttpRequest                    req,
                            CallOptions                    options) {
    // f 按值捕获（绝不引用调用方栈）：任务即使晚于本函数返回，捕获对象仍在
    // 任务自己的闭包与共享态里存活（F-2）。
    auto state = RunInCoroutine(
        [client, req = std::move(req), options]() mutable {
            return client->Request(std::move(req), options);
        });
    if (!state || !state->ok.load(std::memory_order_acquire))
        return result<HttpResponse>::err(MakeError(
            ErrorCode::InternalError, "test: coroutine did not run"));
    return std::move(*state->value);
}

int ListenLoopback(std::uint16_t* port_out) {
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
    socklen_t len = sizeof(addr);
    BOOST_REQUIRE(::getsockname(lfd, reinterpret_cast<sockaddr*>(&addr),
                                &len) == 0);
    *port_out = ntohs(addr.sin_port);
    return lfd;
}

// 读到请求结束（"\r\n\r\n" + 声明长度 body）后返回 true：服务端侧的
// 「已完整收到请求」判据（本端真读到字节，不采信客户端自报）。
bool ReadFullRequest(int fd, int* body_bytes_seen) {
    std::string got;
    char        buf[2048];
    std::size_t content_length = 0;
    bool        have_header    = false;
    for (;;) {
        const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n <= 0)
            return false;
        got.append(buf, static_cast<std::size_t>(n));
        if (!have_header) {
            const auto head_end = got.find("\r\n\r\n");
            if (head_end == std::string::npos)
                continue;
            have_header = true;
            // Content-Length 由 adapter 自己写，是 framing 头，可安全解析。
            const auto cl = got.find("Content-Length:");
            if (cl != std::string::npos && cl < head_end)
                content_length = static_cast<std::size_t>(
                    std::strtoul(got.c_str() + cl + 15, nullptr, 10));
        }
        const auto        head_end  = got.find("\r\n\r\n");
        const std::size_t body_have = got.size() - (head_end + 4);
        if (body_have >= content_length) {
            *body_bytes_seen = static_cast<int>(body_have);
            return true;
        }
    }
}

// 脚本化对端（POSIX 线程，accept 循环 + select 轮询 stop）：每条连接执行
// serve(fd)，随后关闭。serve 内可自行收发；不使用固定等待。
struct ScriptedPeer {
    std::uint16_t                     port = 0;
    std::shared_ptr<std::atomic_int>  accepts{
        std::make_shared<std::atomic_int>(0)};
    std::shared_ptr<std::atomic_bool> stop{
        std::make_shared<std::atomic_bool>(false)};
    std::shared_ptr<std::atomic_bool> closed{
        std::make_shared<std::atomic_bool>(false)};

    explicit ScriptedPeer(std::function<void(int)> serve) {
        int lfd = ListenLoopback(&port);
        auto n_accept = accepts;
        auto flag     = stop;
        auto finished = closed;
        std::thread t([lfd, serve = std::move(serve), n_accept, flag,
                       finished] {
            while (!flag->load(std::memory_order_acquire)) {
                fd_set rfds;
                FD_ZERO(&rfds);
                FD_SET(lfd, &rfds);
                timeval tv{0, 50000};
                if (::select(lfd + 1, &rfds, nullptr, nullptr, &tv) > 0) {
                    int c = ::accept(lfd, nullptr, nullptr);
                    if (c >= 0) {
                        n_accept->fetch_add(1);
                        serve(c);
                        ::close(c);
                    }
                }
            }
            ::close(lfd);
            finished->store(true, std::memory_order_release);
        });
        m_thread = std::move(t);
    }
    ~ScriptedPeer() {
        stop->store(true, std::memory_order_release);
        if (m_thread.joinable())
            m_thread.join();
    }
    ScriptedPeer(const ScriptedPeer&)            = delete;
    ScriptedPeer& operator=(const ScriptedPeer&) = delete;

private:
    std::thread m_thread;
};

// 只 accept、不读不写不出错的持有型对端：用于「写侧无法完成」的未提交窗口。
struct HoldPeer {
    std::uint16_t                     port = 0;
    std::shared_ptr<std::atomic_int>  accepts{
        std::make_shared<std::atomic_int>(0)};
    std::shared_ptr<std::atomic_bool> stop{
        std::make_shared<std::atomic_bool>(false)};

    HoldPeer() {
        int lfd = ListenLoopback(&port);
        auto n_accept = accepts;
        auto flag     = stop;
        std::thread t([lfd, n_accept, flag] {
            std::vector<int> held;
            while (!flag->load(std::memory_order_acquire)) {
                fd_set rfds;
                FD_ZERO(&rfds);
                FD_SET(lfd, &rfds);
                timeval tv{0, 50000};
                if (::select(lfd + 1, &rfds, nullptr, nullptr, &tv) > 0) {
                    int c = ::accept(lfd, nullptr, nullptr);
                    if (c >= 0) {
                        held.push_back(c);   // 保持打开：既不读也不回
                        n_accept->fetch_add(1);
                    }
                }
            }
            for (int c : held)
                ::close(c);
            ::close(lfd);
        });
        m_thread = std::move(t);
    }
    ~HoldPeer() {
        stop->store(true, std::memory_order_release);
        if (m_thread.joinable())
            m_thread.join();
    }
    HoldPeer(const HoldPeer&)            = delete;
    HoldPeer& operator=(const HoldPeer&) = delete;

private:
    std::thread m_thread;
};

// 标记 + 放行闸门型对端：读到完整请求（marker）后保持连接打开、既不回复也
// 不关闭，直到测试显式 Release()（条件变量事件门）。用于需要「对端确认收全
// 但本端不返回」的场景：结果只可能来自 deadline/Close，而不是对端行为。
struct MarkerGatePeer {
    std::uint16_t                     port = 0;
    std::shared_ptr<std::atomic_bool> got_full{
        std::make_shared<std::atomic_bool>(false)};
    std::shared_ptr<std::atomic_int>  body_bytes{
        std::make_shared<std::atomic_int>(-1)};
    std::shared_ptr<std::atomic_int>  accepts{
        std::make_shared<std::atomic_int>(0)};
    std::shared_ptr<std::atomic_bool> release_seen{
        std::make_shared<std::atomic_bool>(false)};

    MarkerGatePeer() {
        int  lfd      = ListenLoopback(&port);
        auto got      = got_full;
        auto body     = body_bytes;
        auto n_accept = accepts;
        auto seen     = release_seen;
        std::thread t([this, lfd, got, body, n_accept, seen] {
            // 只服务一条连接（重试观测靠 accepts 计数）。
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(lfd, &rfds);
            timeval tv{5, 0};                     // 失败上限，非完成依据
            if (::select(lfd + 1, &rfds, nullptr, nullptr, &tv) <= 0) {
                ::close(lfd);
                return;
            }
            int c = ::accept(lfd, nullptr, nullptr);
            if (c < 0) {
                ::close(lfd);
                return;
            }
            n_accept->fetch_add(1);
            timeval rcv{5, 0};
            ::setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, &rcv, sizeof(rcv));
            int        body_seen = -1;
            const bool full      = ReadFullRequest(c, &body_seen);
            if (full) {
                body->store(body_seen, std::memory_order_release);
                got->store(true, std::memory_order_release);
            }
            // 闸门：等测试放行（超时只作失败上限，防断言失败时挂死）。
            {
                std::unique_lock<std::mutex> lk(m_mtx);
                m_cv.wait_for(lk, std::chrono::seconds(30),
                              [this] { return m_released; });
            }
            seen->store(true, std::memory_order_release);
            ::close(c);
            ::close(lfd);
        });
        m_thread = std::move(t);
    }
    void Release() {
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_released = true;
        }
        m_cv.notify_all();
    }
    ~MarkerGatePeer() {
        Release();
        if (m_thread.joinable())
            m_thread.join();
    }
    MarkerGatePeer(const MarkerGatePeer&)            = delete;
    MarkerGatePeer& operator=(const MarkerGatePeer&) = delete;

private:
    std::mutex              m_mtx;
    std::condition_variable m_cv;
    bool                    m_released{false};
    std::thread             m_thread;
};

// 取一个当前没有任何 listener 的本地端口（S1 连接失败对照）。
std::uint16_t UnusedLoopbackPort() {
    std::uint16_t port = 0;
    int fd = ListenLoopback(&port);
    ::close(fd);
    return port;
}

// 在协程内发起一次请求并等它返回；完成事件与结果都在堆上共享态里（F-2）：
// 测试侧断言失败提前离开作用域时，任务仍只写自己 shared_ptr 持有的对象，
// 不再以 this/&out 引用栈上的 PendingRequest。
struct PendingRequest {
    std::shared_ptr<CoRunState<result<HttpResponse>>> state;

    void Spawn(const std::shared_ptr<HttpClient>& client, HttpRequest req,
               CallOptions options) {
        state = SpawnCoTask(
            [client, req = std::move(req), options]() mutable {
                return client->Request(std::move(req), options);
            });
        BOOST_REQUIRE(state != nullptr);
    }
    bool Ready() const {
        return state != nullptr && state->ok.load(std::memory_order_acquire);
    }
    bool WaitReady(int budget_ms = kGateBudgetMs) {
        return WaitUntil([this] { return Ready(); }, budget_ms);
    }
    const std::optional<result<HttpResponse>>& out() const {
        return state->value;
    }
};

} // namespace

BOOST_AUTO_TEST_SUITE(http_request_phase)

BOOST_AUTO_TEST_CASE(t_begin_start_runtime) {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 2;
    cfg->m_cfg_stack_size        = 256 * 1024;
    g_scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    auto rt = NetworkRuntime::Create(MakeLimits(64, 64));
    BOOST_REQUIRE(rt);
    g_runtime = std::move(rt).value();
    BOOST_REQUIRE(g_runtime->Start());
    auto cl = g_runtime->CreateHttpClient();
    BOOST_REQUIRE(cl);
    g_client = std::move(cl).value();
}

// S1：连接根本没建立 —— 确定失败，绝不是未知。
BOOST_AUTO_TEST_CASE(t_s1_connection_refused_is_determinate) {
    const std::uint16_t port = UnusedLoopbackPort();
    auto res = CallOn(g_client, HttpRequest{"GET", UrlOf(port, "/x"), {}, ""},
                      DeadlineAfter(5000));
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::TransportError);
    BOOST_REQUIRE(res.error().request_phase.has_value());
    BOOST_CHECK(!IsRequestCommitted(*res.error().request_phase));
    BOOST_CHECK(*res.error().request_phase == RequestPhase::Connecting);
}

// deadline 在请求尚未完整写出时到点：确定失败（TimedOut），不是未知。
BOOST_AUTO_TEST_CASE(t_s5_deadline_pre_commit_is_determinate) {
    CallOptions opt;
    opt.deadline = std::chrono::steady_clock::now() -
                   std::chrono::milliseconds(1);     // 立即到点
    auto res = CallOn(g_client, HttpRequest{"GET", UrlOf(1, "/x"), {}, ""}, opt);
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::TimedOut);
    BOOST_REQUIRE(res.error().request_phase.has_value());
    BOOST_CHECK(!IsRequestCommitted(*res.error().request_phase));
}

// S2：写中断 —— 对端 accept 后既不读也不回，直到客户端侧「已进入写出阶段」
// 事件（on_write_started：同一 IoGate 内、phase=Writing、尚未写出任何字节）
// 兑现后才关闭连接；8MiB 请求体远超内核缓冲，写侧不可能完成。于是断言
// Writing 有因果依据：写中断发生在客户端确认已进入写出之后，且请求不可能
// 已完整写出 ⇒ 确定传输失败，不得升级为未知。
BOOST_AUTO_TEST_CASE(t_s2_write_interrupt_is_determinate) {
    ReleaseGate close_gate;   // 先声明：其寿命长于对端线程
    ScriptedPeer peer([&close_gate](int) { close_gate.Wait(kGateBudgetMs); });
    auto rt = NetworkRuntime::Create(MakeLimits(4, 4, 16 * 1024 * 1024));
    BOOST_REQUIRE(rt);
    auto rtp = std::move(rt).value();
    BOOST_REQUIRE(rtp->Start());
    auto cl = rtp->CreateHttpClient();
    BOOST_REQUIRE(cl);
    auto client = std::move(cl).value();
    auto impl = std::dynamic_pointer_cast<http_detail::HttpClientImpl>(client);
    BOOST_REQUIRE(impl != nullptr);
    auto write_started = std::make_shared<std::atomic_bool>(false);
    impl->SetWriteStartedHookForTest([write_started] {
        write_started->store(true, std::memory_order_release);
    });

    PendingRequest pending;
    pending.Spawn(client,
                  HttpRequest{"POST", UrlOf(peer.port, "/x"), {},
                              std::string(8 * 1024 * 1024, 'x')},
                  DeadlineAfter(20000));
    // 因果事件：客户端确已进入写出阶段（不做「只等 accept 就断言 Writing」）。
    BOOST_REQUIRE(WaitUntil(
        [&] { return write_started->load(std::memory_order_acquire); }));
    close_gate.Open();        // 此刻对端才关闭 ⇒ 写侧 send 必然失败
    BOOST_REQUIRE(pending.WaitReady());
    BOOST_REQUIRE(pending.out().has_value());
    BOOST_REQUIRE(!pending.out().value());
    BOOST_CHECK(pending.out()->error().code == ErrorCode::TransportError);
    BOOST_REQUIRE(pending.out()->error().request_phase.has_value());
    BOOST_CHECK(!IsRequestCommitted(*pending.out()->error().request_phase));
    BOOST_CHECK(*pending.out()->error().request_phase == RequestPhase::Writing);
    // 不自动重试：对端只被连接一次。
    BOOST_CHECK_EQUAL(peer.accepts->load(), 1);
    rtp->Close();
    BOOST_CHECK(rtp->IsClosed());
}

// S3：对端确认完整收到请求（服务端自己读到请求结束符）后不回复并断开 ——
// 未知，且客户端阶段事实与服务端收全标记交叉一致；无自动重试。
BOOST_AUTO_TEST_CASE(t_s3_full_request_then_close_is_outcome_unknown) {
    auto got_full = std::make_shared<std::atomic_bool>(false);
    auto body     = std::make_shared<std::atomic_int>(-1);
    ScriptedPeer peer([got_full, body](int fd) {
        int body_seen = -1;
        if (ReadFullRequest(fd, &body_seen)) {
            body->store(body_seen, std::memory_order_release);
            got_full->store(true, std::memory_order_release);
        }
        // 不发任何响应字节，直接返回 ⇒ 对端关闭（FIN）。
    });

    auto rt = NetworkRuntime::Create(MakeLimits(4, /*max_inflight=*/1));
    BOOST_REQUIRE(rt);
    auto rtp = std::move(rt).value();
    BOOST_REQUIRE(rtp->Start());
    auto cl = rtp->CreateHttpClient();
    BOOST_REQUIRE(cl);
    auto client = std::move(cl).value();

    HttpRequest req{"POST", UrlOf(peer.port, "/submit"), {},
                    std::string(64, 'p')};
    auto res = CallOn(client, std::move(req), DeadlineAfter(20000));

    // 服务端侧标记：完整收到请求（含 64 字节 body）后才断开。
    BOOST_REQUIRE(WaitUntil([&] { return got_full->load(); }));
    BOOST_CHECK_EQUAL(body->load(), 64);

    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::OutcomeUnknown);
    BOOST_REQUIRE(res.error().request_phase.has_value());
    BOOST_CHECK(*res.error().request_phase == RequestPhase::RequestCommitted);
    BOOST_CHECK(ConsumerSawCommittedWrite(res.error()));
    // 不自动重试：一次请求只建一条连接。
    BOOST_CHECK_EQUAL(peer.accepts->load(), 1);
    // 配额恰好归还在物理收口：max_inflight=1 下后续请求必须被"接纳"而非
    // Overloaded（该对端仍不回复，故结果本身仍是错误）。
    auto after = CallOn(client,
                        HttpRequest{"GET", UrlOf(peer.port, "/x"), {}, ""},
                        DeadlineAfter(20000));
    BOOST_REQUIRE(!after);
    BOOST_CHECK(after.error().code != ErrorCode::Overloaded);
    rtp->Close();
    BOOST_CHECK(rtp->IsClosed());
}

// S4：可信回复不被升级 —— 200 保持成功语义；畸形 framing 仍是对回复的确定
// 判决（ProtocolError），不是未知；阶段事实与终态码一致。
BOOST_AUTO_TEST_CASE(t_s4_trustworthy_reply_not_upgraded) {
    auto ok_sent = std::make_shared<std::atomic_bool>(false);
    ScriptedPeer ok_peer([ok_sent](int fd) {
        int body_seen = -1;
        if (!ReadFullRequest(fd, &body_seen))
            return;
        const char ok[] = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nhi";
        ::send(fd, ok, sizeof(ok) - 1, MSG_NOSIGNAL);
        ok_sent->store(true, std::memory_order_release);
    });
    HttpRequest req{"GET", UrlOf(ok_peer.port, "/ok"), {}, ""};
    auto res = CallOn(g_client, std::move(req), DeadlineAfter(10000));
    BOOST_REQUIRE(res);
    BOOST_CHECK_EQUAL(res.value().status, 200u);
    BOOST_CHECK_EQUAL(res.value().body, "hi");
    BOOST_REQUIRE(WaitUntil([&] { return ok_sent->load(); }));

    // 畸形 framing：字节已到达但对端协议违规 —— 确定判决，不升级为未知。
    ScriptedPeer junk([](int fd) {
        char buf[512];
        (void)::recv(fd, buf, sizeof(buf), 0);
        const char bad[] = "GARBAGE-NOT-HTTP\r\n\r\n";
        ::send(fd, bad, sizeof(bad) - 1, MSG_NOSIGNAL);
    });
    auto bad_res = CallOn(g_client,
                          HttpRequest{"GET", UrlOf(junk.port, "/x"), {}, ""},
                          DeadlineAfter(10000));
    BOOST_REQUIRE(!bad_res);
    BOOST_CHECK(bad_res.error().code == ErrorCode::ProtocolError);
    BOOST_REQUIRE(bad_res.error().request_phase.has_value());
    BOOST_CHECK(*bad_res.error().request_phase ==
                RequestPhase::RequestCommitted);
    BOOST_CHECK(bad_res.error().code != ErrorCode::OutcomeUnknown);
}

// S5-a：deadline 在请求已完整写出后到点 —— 未知。对端确认收全后由测试闸门
// 保持连接不回复，故结果只可能来自本端期限，不来自对端任何行为。
BOOST_AUTO_TEST_CASE(t_s5_deadline_post_commit_is_outcome_unknown) {
    MarkerGatePeer peer;
    PendingRequest  pending;
    pending.Spawn(g_client, HttpRequest{"GET", UrlOf(peer.port, "/slow"), {}, ""},
                  DeadlineAfter(300));
    // 服务端侧交叉断言：完整收到请求后才可能进入"等回复"。
    BOOST_REQUIRE(WaitUntil([&] { return peer.got_full->load(); }));
    BOOST_REQUIRE(pending.WaitReady());
    BOOST_REQUIRE(pending.out().has_value());
    BOOST_REQUIRE(!pending.out().value());
    BOOST_CHECK(pending.out()->error().code == ErrorCode::OutcomeUnknown);
    BOOST_REQUIRE(pending.out()->error().request_phase.has_value());
    BOOST_CHECK(*pending.out()->error().request_phase ==
                RequestPhase::RequestCommitted);
    BOOST_CHECK(ConsumerSawCommittedWrite(pending.out()->error()));
    BOOST_CHECK_EQUAL(peer.accepts->load(), 1);
    peer.Release();   // 放行对端关闭（清理），避免遗留连接
}

// S5-b：Close 在请求尚未完整写出时收口 —— 确定失败（Closed），阶段 Writing。
// 「已进入写出阶段」由客户端侧 on_write_started 事件证明后再 Close：服务端
// accept 计数与客户端 OnConnect 无因果顺序，不能用来猜 Writing/Connecting。
// 8MiB 体 + 对端不读 ⇒ 写不可能完成，故 Close 当刻阶段必为 Writing（未提交）。
BOOST_AUTO_TEST_CASE(t_s5_close_pre_commit_is_determinate) {
    HoldPeer peer;   // accept 后既不读也不回：8MiB 体写不出去
    auto rt = NetworkRuntime::Create(
        MakeLimits(4, /*max_inflight=*/1, 16 * 1024 * 1024));
    BOOST_REQUIRE(rt);
    auto rtp = std::move(rt).value();
    BOOST_REQUIRE(rtp->Start());
    auto cl = rtp->CreateHttpClient();
    BOOST_REQUIRE(cl);
    auto client = std::move(cl).value();
    auto impl = std::dynamic_pointer_cast<http_detail::HttpClientImpl>(client);
    BOOST_REQUIRE(impl != nullptr);
    auto write_started = std::make_shared<std::atomic_bool>(false);
    impl->SetWriteStartedHookForTest([write_started] {
        write_started->store(true, std::memory_order_release);
    });

    PendingRequest pending;
    pending.Spawn(client,
                  HttpRequest{"POST", UrlOf(peer.port, "/x"), {},
                              std::string(8 * 1024 * 1024, 'y')},
                  DeadlineAfter(30000));
    BOOST_REQUIRE(WaitUntil(
        [&] { return write_started->load(std::memory_order_acquire); }));

    client->Close();
    BOOST_CHECK(client->IsClosed());
    BOOST_REQUIRE(pending.WaitReady());
    BOOST_REQUIRE(pending.out().has_value());
    BOOST_REQUIRE(!pending.out().value());
    BOOST_CHECK(pending.out()->error().code == ErrorCode::Closed);
    BOOST_REQUIRE(pending.out()->error().request_phase.has_value());
    BOOST_CHECK(!IsRequestCommitted(*pending.out()->error().request_phase));
    BOOST_CHECK(*pending.out()->error().request_phase == RequestPhase::Writing);
    rtp->Close();
    BOOST_CHECK(rtp->IsClosed());
}

// S5-c：Close 在请求已完整写出后收口 —— 未知（不是 Closed）；物理收口与
// 配额归还仍成立。
BOOST_AUTO_TEST_CASE(t_s5_close_post_commit_is_outcome_unknown) {
    MarkerGatePeer peer;
    auto rt = NetworkRuntime::Create(MakeLimits(4, /*max_inflight=*/1));
    BOOST_REQUIRE(rt);
    auto rtp = std::move(rt).value();
    BOOST_REQUIRE(rtp->Start());
    auto cl = rtp->CreateHttpClient();
    BOOST_REQUIRE(cl);
    auto client = std::move(cl).value();

    PendingRequest pending;
    pending.Spawn(client, HttpRequest{"GET", UrlOf(peer.port, "/x"), {}, ""},
                  DeadlineAfter(30000));
    // 服务端确认收全 ⇒ 请求已完整写出；此时对端保持连接、不回复、不关闭。
    BOOST_REQUIRE(WaitUntil([&] { return peer.got_full->load(); }));

    client->Close();
    BOOST_CHECK(client->IsClosed());
    BOOST_REQUIRE(pending.WaitReady());
    BOOST_REQUIRE(pending.out().has_value());
    BOOST_REQUIRE(!pending.out().value());
    BOOST_CHECK(pending.out()->error().code == ErrorCode::OutcomeUnknown);
    BOOST_REQUIRE(pending.out()->error().request_phase.has_value());
    BOOST_CHECK(*pending.out()->error().request_phase ==
                RequestPhase::RequestCommitted);
    BOOST_CHECK(ConsumerSawCommittedWrite(pending.out()->error()));
    peer.Release();

    // 配额恰好在物理收口时归还一次：max_inflight=1 下新 client 可被接纳。
    auto cl2 = rtp->CreateHttpClient();
    BOOST_REQUIRE(cl2);
    auto client2 = std::move(cl2).value();
    auto after = CallOn(client2,
                        HttpRequest{"GET", UrlOf(peer.port, "/x"), {}, ""},
                        DeadlineAfter(1500));
    BOOST_REQUIRE(!after);
    BOOST_CHECK(after.error().code != ErrorCode::Overloaded);
    client2->Close();
    rtp->Close();
    BOOST_CHECK(rtp->IsClosed());
}

// S6：同一 client 上多 operation 不串用阶段状态；每个 op 独立计账。
BOOST_AUTO_TEST_CASE(t_s6_operations_do_not_share_phase_state) {
    const std::uint16_t dead = UnusedLoopbackPort();

    // op1：未提交失败（连接被拒）——确定错误、阶段未提交。
    auto r1 = CallOn(g_client, HttpRequest{"GET", UrlOf(dead, "/x"), {}, ""},
                     DeadlineAfter(5000));
    BOOST_REQUIRE(!r1);
    BOOST_CHECK(r1.error().code == ErrorCode::TransportError);
    BOOST_CHECK(!ConsumerSawCommittedWrite(r1.error()));

    // op2：完整写出后丢回复——未知、阶段已提交。
    auto got_full = std::make_shared<std::atomic_bool>(false);
    ScriptedPeer peer([got_full](int fd) {
        int body_seen = -1;
        got_full->store(ReadFullRequest(fd, &body_seen),
                        std::memory_order_release);
    });
    auto r2 = CallOn(g_client, HttpRequest{"GET", UrlOf(peer.port, "/x"), {}, ""},
                     DeadlineAfter(10000));
    BOOST_REQUIRE(WaitUntil([&] { return got_full->load(); }));
    BOOST_REQUIRE(!r2);
    BOOST_CHECK(r2.error().code == ErrorCode::OutcomeUnknown);
    BOOST_CHECK(ConsumerSawCommittedWrite(r2.error()));

    // op3：正常回复（明确 404）——成功语义，不残留未知/阶段污染。
    auto served = std::make_shared<std::atomic_bool>(false);
    ScriptedPeer ok_peer([served](int fd) {
        int body_seen = -1;
        if (!ReadFullRequest(fd, &body_seen))
            return;
        const char resp[] =
            "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        ::send(fd, resp, sizeof(resp) - 1, MSG_NOSIGNAL);
        served->store(true, std::memory_order_release);
    });
    auto r3 = CallOn(g_client,
                     HttpRequest{"GET", UrlOf(ok_peer.port, "/x"), {}, ""},
                     DeadlineAfter(10000));
    BOOST_REQUIRE(r3);
    BOOST_CHECK_EQUAL(r3.value().status, 404u);
    BOOST_REQUIRE(WaitUntil([&] { return served->load(); }));

    // op4：又一次未提交失败——仍与前面的未知互不影响。
    auto r4 = CallOn(g_client, HttpRequest{"GET", UrlOf(dead, "/x"), {}, ""},
                     DeadlineAfter(5000));
    BOOST_REQUIRE(!r4);
    BOOST_CHECK(r4.error().code == ErrorCode::TransportError);
    BOOST_CHECK(!ConsumerSawCommittedWrite(r4.error()));
}

// S8（F-1）：放弃（deadline）在「请求尚未完整写出」时返回确定失败，且此后后端
// 不可能再把请求完整写出。构造为状态门控、不靠墙钟竞态：
//   - 持有型对端 accept 后不读，请求体 8MiB 远超内核缓冲 ⇒ 写侧必然停在
//     Writing（未提交），且 io 域回到空闲等待（不像停放那样冻结事件循环）；
//   - 用 on_write_started 事件确认客户端确已进入写出阶段，随后 deadline 到点，
//     放弃在 Writing 落定 ⇒ 确定失败（TimedOut），绝不升级为 OutcomeUnknown；
//   - 放弃返回后放行对端排空：对端仍收不到完整请求（Abort 已在返回前关 socket），
//     也不出现第二条连接。
//
// 限制（不伪称覆盖）：本运行时 io handler 与 deadline 定时器同由单一 Scheduler
// 事件循环线程（CoPoller::PollOnce，见 bbtools-coroutine Scheduler.cc）驱动，
// 因此「放弃读阶段」与「io 正在同步写出」不可能并发——本用例验证的是放弃边界
// 的可观察不变量，**不能**在动态上区分未修复实现。F-1 的「放弃决定与 io 域
// 线性化」由源码线性化点证明：HttpClientImpl::Request 放弃路径在同一 IoGate 内
// Abort → 读阶段 → SealError → Finish（见 src/http/HttpClientImpl.cc 与决策 0008 §4）。
BOOST_AUTO_TEST_CASE(t_s8_abandon_pre_commit_is_determinate_no_late_write) {
    ReleaseGate drain_gate;   // 先声明：寿命长于对端线程
    auto got_full = std::make_shared<std::atomic_bool>(false);
    auto body     = std::make_shared<std::atomic_int>(-1);
    auto drained  = std::make_shared<std::atomic_bool>(false);
    ScriptedPeer peer([&drain_gate, got_full, body, drained](int fd) {
        timeval rcv{3, 0};   // 失败上限：放行后读不到就退出，防 join 挂死
        ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &rcv, sizeof(rcv));
        drain_gate.Wait(kGateBudgetMs);   // 先持有（不读）：写必然停在 Writing
        int body_seen = -1;
        if (ReadFullRequest(fd, &body_seen)) {
            body->store(body_seen, std::memory_order_release);
            got_full->store(true, std::memory_order_release);
        }
        drained->store(true, std::memory_order_release);
    });
    auto rt = NetworkRuntime::Create(MakeLimits(4, /*max_inflight=*/1,
                                                16 * 1024 * 1024));
    BOOST_REQUIRE(rt);
    auto rtp = std::move(rt).value();
    BOOST_REQUIRE(rtp->Start());
    auto cl = rtp->CreateHttpClient();
    BOOST_REQUIRE(cl);
    auto client = std::move(cl).value();
    auto impl = std::dynamic_pointer_cast<http_detail::HttpClientImpl>(client);
    BOOST_REQUIRE(impl != nullptr);
    auto write_started = std::make_shared<std::atomic_bool>(false);
    impl->SetWriteStartedHookForTest([write_started] {
        write_started->store(true, std::memory_order_release);
    });

    PendingRequest pending;
    pending.Spawn(client,
                  HttpRequest{"POST", UrlOf(peer.port, "/x"), {},
                              std::string(8 * 1024 * 1024, 'z')},
                  DeadlineAfter(1500));
    // 因果事件：客户端确已进入写出阶段（尚未写出任何字节）。
    BOOST_REQUIRE(WaitUntil(
        [&] { return write_started->load(std::memory_order_acquire); }));
    BOOST_REQUIRE(pending.WaitReady());
    BOOST_REQUIRE(pending.out().has_value());
    BOOST_REQUIRE(!pending.out().value());
    // 未完整写出：确定失败，绝不升级为未知。
    BOOST_CHECK(pending.out()->error().code == ErrorCode::TimedOut);
    BOOST_CHECK(pending.out()->error().code != ErrorCode::OutcomeUnknown);
    BOOST_REQUIRE(pending.out()->error().request_phase.has_value());
    BOOST_CHECK(!IsRequestCommitted(*pending.out()->error().request_phase));
    BOOST_CHECK(*pending.out()->error().request_phase == RequestPhase::Writing);
    // 放弃返回后放行对端排空：对端仍收不到完整请求（Abort 已在返回前关 socket）。
    drain_gate.Open();
    BOOST_REQUIRE(WaitUntil([&] { return drained->load(); }));
    BOOST_CHECK(!got_full->load());
    BOOST_CHECK_EQUAL(peer.accepts->load(), 1);   // 不自动重试
    rtp->Close();
    BOOST_CHECK(rtp->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_end_runtime_physical_convergence) {
    g_runtime->Close();
    BOOST_CHECK(g_runtime->IsClosed());
    BOOST_CHECK(g_client->IsClosed());
    g_client.reset();
    g_runtime.reset();
}

BOOST_AUTO_TEST_SUITE_END()
