// co-network/v1 N2 · waypoint rpc-infra-foundation 2-4/2-5：
// RPC 公开面的真实故障、限额与关闭矩阵。
//
// 全程只用公开面（RpcClient/RpcServer/NetworkRuntime/RequestPhase/
// IsRequestCommitted）与自有 raw 对端（POSIX socket），不经 framework，
// 不 include src/ 内部头，不用测试 seam：
//   - 请求已完整写出后对端不回复/断开 → OutcomeUnknown + RequestCommitted；
//   - 完整但畸形/超限的 HTTP 响应 → ProtocolError（对已到达字节的确定判决），
//     不升级为未知；HTTP 非成功状态 → Unavailable（保留 HTTP 事实）；
//   - 连接未建立/请求未提交 → 确定失败（阶段未提交）；
//   - 回复关联字段不匹配 → ProtocolError；
//   - 空 handler 拒绝、StopAccepting 只停新接纳、同步 Close 幂等与关闭后工厂拒绝、
//     并发 Close、关闭后迟到回复不再交付；
//   - 出站 max_inflight / max_connections 边界 → 确定性 Overloaded（有界拒绝）；
//   - 入站 header/body 限额 → 有界 400/413，且不派发 handler。
//
// 事件口径沿用既有 HTTP fault fixture（tests/Test_http_request_phase.cc）：
// 对端「已完整收到请求」由对端自己读到请求结束符后置位，客户端阶段从 result 的
// request_phase 读出，两者交叉断言；所有等待都是有界谓词/条件变量等待，超时只作
// 失败上限，不作完成依据。协程任务的完成态放在堆上共享对象里，不引用测试线程栈。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <cerrno>
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
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/RpcClient.hpp>
#include <bbt/infra/RpcServer.hpp>
#include <bbt/infra/rpc/RpcWire.hpp>

using namespace bbt::infra;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

constexpr int kBudgetMs     = 15000;   // 协程任务预算
constexpr int kGateBudgetMs = 20000;   // 失败上限（非完成依据）

void EnsureScheduler() {
    auto& scheduler = *bbt::coroutine::detail::Scheduler::GetInstance();
    if (!scheduler.IsInitialized()) {
        auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
        cfg->m_cfg_static_thread_num = 4;
        cfg->m_cfg_stack_size        = 256 * 1024;
        scheduler.Start(SCHE_START_OPT_SCHE_THREAD);
    }
    BOOST_REQUIRE(scheduler.IsInitialized());
}

NetworkLimits MakeLimits(std::size_t max_conn, std::size_t max_inflight,
                         std::size_t max_header = 16 * 1024,
                         std::size_t max_body   = 64 * 1024) {
    NetworkLimits limits{};
    limits.max_connections  = max_conn;
    limits.max_inflight     = max_inflight;
    limits.max_header_bytes = max_header;
    limits.max_body_bytes   = max_body;
    limits.incoming_timeout = std::chrono::milliseconds{3000};
    return limits;
}

std::shared_ptr<NetworkRuntime> MakeRuntime(std::size_t max_conn,
                                            std::size_t max_inflight,
                                            std::size_t max_header = 16 * 1024,
                                            std::size_t max_body   = 64 * 1024) {
    auto created = NetworkRuntime::Create(
        MakeLimits(max_conn, max_inflight, max_header, max_body));
    BOOST_REQUIRE(created);
    auto runtime = std::move(created).value();
    BOOST_REQUIRE(runtime->Start());
    return runtime;
}

// 协程任务的堆上完成态：任务只写自己 shared_ptr 持有的对象，测试线程提前
// 返回（或断言失败离开作用域）都不会留下悬垂引用。
template <class T>
struct CoRunState {
    bbt::core::thread::CountDownLatch done{1};
    std::optional<T>                 value;
    std::atomic_bool                 ok{false};
};

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

RpcEnvelope MakeRequest(std::string id, std::string method) {
    RpcEnvelope request;
    request.request_id      = std::move(id);
    request.service         = "svc.fault";
    request.method          = std::move(method);
    request.request_schema  = "bbt.fault.Req/v1";
    request.response_schema = "bbt.fault.Resp/v1";
    request.payload         = {'p', 'i', 'n', 'g'};
    return request;
}

// 在协程内发起一次 RPC 调用（不等待），完成态在堆上。
struct PendingCall {
    std::shared_ptr<CoRunState<result<RpcEnvelope>>> state;

    void Spawn(const std::shared_ptr<RpcClient>& client, RpcAddress address,
               RpcEnvelope request, CallOptions options) {
        state = SpawnCoTask(
            [client, address, request = std::move(request), options]() mutable {
                return client->Call(address, std::move(request), options);
            });
        BOOST_REQUIRE(state != nullptr);
    }
    bool Ready() const {
        return state != nullptr && state->ok.load(std::memory_order_acquire);
    }
    bool WaitReady(int budget_ms = kGateBudgetMs) {
        return WaitUntil([this] { return Ready(); }, budget_ms);
    }
    const std::optional<result<RpcEnvelope>>& out() const { return state->value; }
};

// 同步协程调用（等它真正结束）；仅用于已知会很快返回的路径。
result<RpcEnvelope> CallSync(const std::shared_ptr<RpcClient>& client,
                             RpcAddress address, RpcEnvelope request,
                             CallOptions options) {
    PendingCall pending;
    pending.Spawn(client, std::move(address), std::move(request), options);
    if (!pending.Ready() && !pending.WaitReady())
        return result<RpcEnvelope>::err(
            MakeError(ErrorCode::InternalError, "test: call did not return"));
    return std::move(*pending.state->value);
}

bool SawCommittedWrite(const Error& e) {
    return e.request_phase.has_value() && IsRequestCommitted(*e.request_phase);
}

// 拒绝/关闭竞态下的失败口径（M1）：结果不是成功回复，且 handler 未被派发
// （派发计数由调用方核对）。停接纳/连接名额拒绝的实际物理形态可能是「连接被
// 物理拒绝」或「请求已写出后被 RST」，两者都是自洽事实——只要求 code 与
// request_phase 相互一致，不强行要求某一种 code：
//   OutcomeUnknown 必须携带已提交阶段（请求可能已生效）；
//   TransportError 必须携带未提交阶段（请求一次都没发出）。
// 不把「对端 RST 发生在 RequestCommitted」误报为契约不一致。
void CheckFailureWithoutDispatch(const result<RpcEnvelope>& res) {
    BOOST_REQUIRE(!res);
    const auto code = res.error().code;
    BOOST_CHECK(code == ErrorCode::TransportError ||
                code == ErrorCode::OutcomeUnknown);
    BOOST_REQUIRE(res.error().request_phase.has_value());
    if (code == ErrorCode::OutcomeUnknown)
        BOOST_CHECK(IsRequestCommitted(*res.error().request_phase));
    else
        BOOST_CHECK(!IsRequestCommitted(*res.error().request_phase));
}

// --- raw 对端 fixture（口径同 tests/Test_http_request_phase.cc）--------------

int ListenLoopback(std::uint16_t* port_out) {
    int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(lfd >= 0);
    int one = 1;
    ::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = htons(0);
    BOOST_REQUIRE(::bind(lfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    BOOST_REQUIRE(::listen(lfd, 8) == 0);
    socklen_t len = sizeof(addr);
    BOOST_REQUIRE(::getsockname(lfd, reinterpret_cast<sockaddr*>(&addr), &len) == 0);
    *port_out = ntohs(addr.sin_port);
    return lfd;
}

// 读到请求结束（CRLFCRLF + Content-Length body）后返回 true：
// 对端侧「已完整收到请求」的判据（真读到字节，不采信客户端自报）。
bool ReadFullRequest(int fd, std::size_t* body_bytes_seen = nullptr) {
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
            const auto cl = got.find("Content-Length:");
            if (cl != std::string::npos && cl < head_end)
                content_length = static_cast<std::size_t>(
                    std::strtoul(got.c_str() + cl + 15, nullptr, 10));
        }
        const auto        head_end  = got.find("\r\n\r\n");
        const std::size_t body_have = got.size() - (head_end + 4);
        if (body_have >= content_length) {
            if (body_bytes_seen)
                *body_bytes_seen = body_have;
            return true;
        }
    }
}

std::uint16_t UnusedLoopbackPort() {
    std::uint16_t port = 0;
    int fd = ListenLoopback(&port);
    ::close(fd);
    return port;
}

// 脚本化对端：accept 循环（select 轮询 stop），每条连接执行 serve(fd) 后关闭。
struct RawPeer {
    std::uint16_t                     port = 0;
    std::shared_ptr<std::atomic_int>  accepts{std::make_shared<std::atomic_int>(0)};

    explicit RawPeer(std::function<void(int)> serve) {
        int lfd = ListenLoopback(&port);
        auto n_accept = accepts;
        auto flag = m_stop;
        std::thread t([lfd, serve = std::move(serve), n_accept, flag] {
            while (!flag->load(std::memory_order_acquire)) {
                fd_set rfds;
                FD_ZERO(&rfds);
                FD_SET(lfd, &rfds);
                timeval tv{0, 50000};
                if (::select(lfd + 1, &rfds, nullptr, nullptr, &tv) > 0) {
                    int c = ::accept(lfd, nullptr, nullptr);
                    if (c >= 0) {
                        n_accept->fetch_add(1);
                        timeval timeout{5, 0};
                        ::setsockopt(c, SOL_SOCKET, SO_RCVTIMEO,
                                     &timeout, sizeof(timeout));
                        ::setsockopt(c, SOL_SOCKET, SO_SNDTIMEO,
                                     &timeout, sizeof(timeout));
                        serve(c);
                        ::close(c);
                    }
                }
            }
            ::close(lfd);
        });
        m_thread = std::move(t);
    }
    ~RawPeer() {
        m_stop->store(true, std::memory_order_release);
        if (m_thread.joinable())
            m_thread.join();
    }
    RawPeer(const RawPeer&)            = delete;
    RawPeer& operator=(const RawPeer&) = delete;

private:
    std::shared_ptr<std::atomic_bool> m_stop{
        std::make_shared<std::atomic_bool>(false)};
    std::thread m_thread;
};

// 标记 + 闸门型对端：读到完整请求（marker）后保持连接打开、既不回复也不关闭，
// 直到测试显式 Release()。用于「对端确认收全但本端不返回」的场景。
//
// M3：Release() 后对端确认的是「本端连接已关闭」这一事实本身——bounded recv
// 得到 EOF(0) 或 ECONNRESET，或（可选）迟到回包 send 得到 EPIPE。记录哪一条
// 事实发生（close_fact），供用例断言。不采信「send 调用返回成功」当本端收到
// 字节的证据（那只说明字节进了本端发送缓冲）。
struct MarkerGatePeer {
    std::uint16_t                     port = 0;
    std::shared_ptr<std::atomic_bool> got_full{
        std::make_shared<std::atomic_bool>(false)};
    std::shared_ptr<std::atomic_int>  accepts{
        std::make_shared<std::atomic_int>(0)};

    // 0 未观察到；1 recv EOF；2 recv ECONNRESET；3 send EPIPE。
    std::shared_ptr<std::atomic_int>  close_fact{
        std::make_shared<std::atomic_int>(0)};

    bool ClosedObserved() const {
        return close_fact->load(std::memory_order_acquire) != 0;
    }
    int CloseFact() const {
        return close_fact->load(std::memory_order_acquire);
    }

    explicit MarkerGatePeer(std::string late_reply = {}) {
        int  lfd      = ListenLoopback(&port);
        auto got      = got_full;
        auto n_accept = accepts;
        auto fact     = close_fact;
        std::thread t([this, lfd, got, n_accept, fact,
                       late_reply = std::move(late_reply)] {
            fd_set rfds;
            FD_ZERO(&rfds);
            FD_SET(lfd, &rfds);
            timeval tv{5, 0};                 // 失败上限，非完成依据
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
            ::setsockopt(c, SOL_SOCKET, SO_SNDTIMEO, &rcv, sizeof(rcv));
            if (ReadFullRequest(c))
                got->store(true, std::memory_order_release);
            {
                std::unique_lock<std::mutex> lk(m_mtx);
                m_cv.wait_for(lk, std::chrono::seconds(30),
                              [this] { return m_released; });
            }
            // Release 之后确认本端连接状态：recv 返回 EOF/ECONNRESET 即对端
            // 已物理关闭；send 得到 EPIPE 同理。有界 recv 超时只作失败上限。
            char          probe[256];
            const ssize_t r = ::recv(c, probe, sizeof(probe), 0);
            if (r == 0)
                fact->store(1, std::memory_order_release);
            else if (r < 0 && errno == ECONNRESET)
                fact->store(2, std::memory_order_release);
            if (!late_reply.empty()) {
                std::size_t sent = 0;
                while (sent < late_reply.size()) {
                    const auto n = ::send(c, late_reply.data() + sent,
                                          late_reply.size() - sent,
                                          MSG_NOSIGNAL);
                    if (n <= 0) {
                        if (errno == EPIPE)
                            fact->store(3, std::memory_order_release);
                        break;
                    }
                    sent += static_cast<std::size_t>(n);
                }
            }
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

// 原始 socket 交互（测试侧主动发字节）：bounded recv，超时即返回已收内容。
std::string RawExchange(std::uint16_t port, const std::string& payload,
                        int timeout_ms) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(fd >= 0);
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = htons(port);
    BOOST_REQUIRE(::connect(fd, reinterpret_cast<sockaddr*>(&addr),
                            sizeof(addr)) == 0);
    timeval tv{timeout_ms / 1000, (timeout_ms % 1000) * 1000};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    std::size_t sent = 0;
    while (sent < payload.size()) {
        const ssize_t n = ::send(fd, payload.data() + sent,
                                 payload.size() - sent, MSG_NOSIGNAL);
        if (n <= 0)
            break;
        sent += static_cast<std::size_t>(n);
    }
    std::string got;
    char        buf[2048];
    for (;;) {
        const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
        if (n <= 0)
            break;
        got.append(buf, static_cast<std::size_t>(n));
        if (got.size() > 64 * 1024)
            break;
    }
    ::close(fd);
    return got;
}

// 构造一个合法的 wire 响应 body（公开 wire API，无 src 内部头）。
std::string WireReplyBody(const RpcEnvelope& reply, bool success,
                          std::uint32_t budget_ms = 1000) {
    auto wire = rpc::ToWireEnvelope(reply, budget_ms);
    BOOST_REQUIRE(wire);
    wire.value().success = success;
    auto bytes = rpc::EncodeRpcWireEnvelope(wire.value(), /*is_response=*/true);
    BOOST_REQUIRE(bytes);
    return std::string(reinterpret_cast<const char*>(bytes.value().data()),
                       bytes.value().size());
}

std::string Http200WithBody(const std::string& body) {
    return "HTTP/1.1 200 OK\r\nContent-Type: application/x-protobuf\r\n"
           "Content-Length: " +
           std::to_string(body.size()) + "\r\n\r\n" + body;
}

void SendAll(int fd, const std::string& bytes) {
    std::size_t sent = 0;
    while (sent < bytes.size()) {
        const ssize_t n = ::send(fd, bytes.data() + sent, bytes.size() - sent,
                                 MSG_NOSIGNAL);
        if (n <= 0)
            return;
        sent += static_cast<std::size_t>(n);
    }
}

// handler 闸门：已接纳 handler 停在 gate 上（有界等待），用于观察
// StopAccepting 不取消已接纳 handler。跨线程共享，堆上持有。
struct HandlerGate {
    std::mutex              mtx;
    std::condition_variable cv;
    bool                    entered{false};
    bool                    release{false};

    void Enter() {
        {
            std::lock_guard<std::mutex> lk(mtx);
            entered = true;
        }
        cv.notify_all();
    }
    bool WaitEntered(int budget_ms) {
        std::unique_lock<std::mutex> lk(mtx);
        return cv.wait_for(lk, std::chrono::milliseconds(budget_ms),
                           [this] { return entered; });
    }
    bool WaitReleased(int budget_ms) {
        std::unique_lock<std::mutex> lk(mtx);
        return cv.wait_for(lk, std::chrono::milliseconds(budget_ms),
                           [this] { return release; });
    }
    void Release() {
        {
            std::lock_guard<std::mutex> lk(mtx);
            release = true;
        }
        cv.notify_all();
    }
};

} // namespace

BOOST_AUTO_TEST_SUITE(rpc_runtime_faults)

// 2-4：请求已完整写出（对端读到请求结束符）后对端不回复并断开 ——
// 未知事实 + 阶段 RequestCommitted；无自动重试（只建一条连接）。
BOOST_AUTO_TEST_CASE(t_full_write_then_disconnect_is_outcome_unknown) {
    EnsureScheduler();
    auto runtime = MakeRuntime(8, 8);
    auto client_result = runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);
    auto client = std::move(client_result).value();

    auto got_full = std::make_shared<std::atomic_bool>(false);
    RawPeer peer([got_full](int fd) {
        if (ReadFullRequest(fd))
            got_full->store(true, std::memory_order_release);
        // 不发任何响应字节，直接返回 ⇒ 对端关闭（FIN）。
    });

    const RpcAddress address{"http", "127.0.0.1:" + std::to_string(peer.port)};
    auto res = CallSync(client, address, MakeRequest("fault-1", "Echo"),
                        DeadlineAfter(kBudgetMs));

    BOOST_REQUIRE(WaitUntil([&] { return got_full->load(); }));
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::OutcomeUnknown);
    BOOST_REQUIRE(res.error().request_phase.has_value());
    BOOST_CHECK(*res.error().request_phase == RequestPhase::RequestCommitted);
    BOOST_CHECK(SawCommittedWrite(res.error()));
    BOOST_CHECK_EQUAL(peer.accepts->load(), 1);   // 不自动重试

    client->Close();
    runtime->Close();
    BOOST_CHECK(runtime->IsClosed());
}

// 2-4：完整到达但畸形（非 HTTP）的响应是对已到达字节的确定判决 ——
// ProtocolError，且不升级为 OutcomeUnknown。
BOOST_AUTO_TEST_CASE(t_malformed_complete_response_is_protocol_error) {
    EnsureScheduler();
    auto runtime = MakeRuntime(8, 8);
    auto client_result = runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);
    auto client = std::move(client_result).value();

    RawPeer peer([](int fd) {
        if (!ReadFullRequest(fd))
            return;
        const char bad[] = "GARBAGE-NOT-HTTP\r\n\r\n";
        SendAll(fd, std::string(bad, sizeof(bad) - 1));
    });

    const RpcAddress address{"http", "127.0.0.1:" + std::to_string(peer.port)};
    auto res = CallSync(client, address, MakeRequest("fault-2", "Echo"),
                        DeadlineAfter(kBudgetMs));
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::ProtocolError);
    BOOST_CHECK(res.error().code != ErrorCode::OutcomeUnknown);
    BOOST_CHECK(SawCommittedWrite(res.error()));

    client->Close();
    runtime->Close();
}

// 2-4：完整但超出 max_body_bytes 的 HTTP 响应 —— 确定 ProtocolError
// （domain_code 保留 body_limit 物理原因），不谎报未知。
BOOST_AUTO_TEST_CASE(t_oversized_response_body_is_protocol_error) {
    EnsureScheduler();
    auto runtime = MakeRuntime(8, 8, /*max_header=*/16 * 1024,
                               /*max_body=*/4096);
    auto client_result = runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);
    auto client = std::move(client_result).value();

    RawPeer peer([](int fd) {
        if (!ReadFullRequest(fd))
            return;
        // 声明并实际发送超过本端 max_body_bytes 的响应体。
        SendAll(fd, "HTTP/1.1 200 OK\r\nContent-Length: 20000\r\n\r\n" +
                        std::string(20000, 'x'));
    });

    const RpcAddress address{"http", "127.0.0.1:" + std::to_string(peer.port)};
    auto res = CallSync(client, address, MakeRequest("fault-3", "Echo"),
                        DeadlineAfter(kBudgetMs));
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::ProtocolError);
    BOOST_CHECK(res.error().code != ErrorCode::OutcomeUnknown);
    BOOST_CHECK_EQUAL(res.error().domain_code, "body_limit");

    client->Close();
    runtime->Close();
}

// 2-4：连接根本没建立 —— 确定失败，阶段未提交，绝不是未知。
BOOST_AUTO_TEST_CASE(t_connect_refused_is_determinate) {
    EnsureScheduler();
    auto runtime = MakeRuntime(8, 8);
    auto client_result = runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);
    auto client = std::move(client_result).value();

    const std::uint16_t port = UnusedLoopbackPort();
    auto res = CallSync(client, {"http", "127.0.0.1:" + std::to_string(port)},
                        MakeRequest("fault-4", "Echo"), DeadlineAfter(5000));
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::TransportError);
    BOOST_CHECK(res.error().code != ErrorCode::OutcomeUnknown);
    BOOST_REQUIRE(res.error().request_phase.has_value());
    BOOST_CHECK(!IsRequestCommitted(*res.error().request_phase));

    client->Close();
    runtime->Close();
}

// 2-4/2-5：回复的关联字段与请求不一致 —— 客户端关联校验拒绝为
// ProtocolError（不是未知，也不是成功 payload）。
BOOST_AUTO_TEST_CASE(t_reply_association_mismatch_is_protocol_error) {
    EnsureScheduler();
    auto runtime = MakeRuntime(8, 8);
    auto client_result = runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);
    auto client = std::move(client_result).value();

    RawPeer peer([](int fd) {
        if (!ReadFullRequest(fd))
            return;
        // 结构合法、但 request_id 不是本次请求值的 wire 响应。
        RpcEnvelope reply = MakeRequest("someone-else", "Echo");
        SendAll(fd, Http200WithBody(WireReplyBody(reply, /*success=*/true)));
    });

    const RpcAddress address{"http", "127.0.0.1:" + std::to_string(peer.port)};
    auto res = CallSync(client, address, MakeRequest("fault-5", "Echo"),
                        DeadlineAfter(kBudgetMs));
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::ProtocolError);
    BOOST_CHECK(res.error().code != ErrorCode::OutcomeUnknown);

    // 同一 request_id 但 response_schema 不同：关联校验逐字段生效，
    // 不能只校验 request_id。
    RawPeer schema_peer([](int fd) {
        if (!ReadFullRequest(fd))
            return;
        RpcEnvelope reply = MakeRequest("fault-5b", "Echo");
        reply.response_schema = "bbt.fault.OtherResp/v1";
        SendAll(fd, Http200WithBody(WireReplyBody(reply, /*success=*/true)));
    });
    auto res2 = CallSync(client,
                         {"http", "127.0.0.1:" + std::to_string(schema_peer.port)},
                         MakeRequest("fault-5b", "Echo"), DeadlineAfter(kBudgetMs));
    BOOST_REQUIRE(!res2);
    BOOST_CHECK(res2.error().code == ErrorCode::ProtocolError);
    BOOST_CHECK(res2.error().code != ErrorCode::OutcomeUnknown);

    client->Close();
    runtime->Close();
}

// 2-5：HTTP 非成功状态不当成功 —— Unavailable 且保留 HTTP 事实；
// 明确 5xx 是可信回复终态，不升级为未知。
BOOST_AUTO_TEST_CASE(t_http_non_success_status_is_unavailable) {
    EnsureScheduler();
    auto runtime = MakeRuntime(8, 8);
    auto client_result = runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);
    auto client = std::move(client_result).value();

    RawPeer peer([](int fd) {
        if (!ReadFullRequest(fd))
            return;
        SendAll(fd, "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n");
    });

    const RpcAddress address{"http", "127.0.0.1:" + std::to_string(peer.port)};
    auto res = CallSync(client, address, MakeRequest("fault-6", "Echo"),
                        DeadlineAfter(kBudgetMs));
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::Unavailable);
    BOOST_CHECK(res.error().code != ErrorCode::OutcomeUnknown);
    BOOST_CHECK_EQUAL(res.error().backend_category, "http");
    BOOST_CHECK_EQUAL(res.error().backend_code, 503);

    client->Close();
    runtime->Close();
}

// 2-5：空 handler 是装配期错误 —— 拒绝，不产生假成功服务端。
BOOST_AUTO_TEST_CASE(t_null_rpc_handler_rejected) {
    EnsureScheduler();
    auto runtime = MakeRuntime(8, 8);

    RpcHandler empty;
    auto rejected = runtime->ListenRpc({"127.0.0.1", 0}, empty);
    BOOST_REQUIRE(!rejected);
    BOOST_CHECK(rejected.error().code == ErrorCode::InvalidArgument);

    auto null_fn = runtime->ListenRpc({"127.0.0.1", 0}, nullptr);
    BOOST_REQUIRE(!null_fn);
    BOOST_CHECK(null_fn.error().code == ErrorCode::InvalidArgument);

    runtime->Close();
}

// 2-5：StopAccepting 只停新接纳 —— 已接纳 handler 不被取消、其回复仍投递；
// 之后的调用不是成功回复，且 handler 派发计数不再增加（M2：用服务端共享
// atomic 计数直接作证「未派发」，不靠错误码形态倒推）。
BOOST_AUTO_TEST_CASE(t_stop_accepting_keeps_accepted_handler) {
    EnsureScheduler();
    auto runtime = MakeRuntime(8, 8);

    auto gate = std::make_shared<HandlerGate>();
    auto handled = std::make_shared<std::atomic_int>(0);
    auto server_result = runtime->ListenRpc(
        {"127.0.0.1", 0},
        [gate, handled](IncomingCallContext,
                        RpcEnvelope request) -> result<RpcEnvelope> {
            handled->fetch_add(1);
            gate->Enter();
            const bool released = gate->WaitReleased(kGateBudgetMs);
            if (!released)
                return result<RpcEnvelope>::err(
                    MakeError(ErrorCode::TimedOut, "test gate timeout"));
            request.payload.push_back('!');
            return result<RpcEnvelope>::ok(std::move(request));
        });
    BOOST_REQUIRE(server_result);
    auto server = std::move(server_result).value();

    auto client_result = runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);
    auto client = std::move(client_result).value();

    const RpcAddress address{
        "http", "127.0.0.1:" + std::to_string(server->LocalAddress().port)};

    // 已接纳的在途调用：handler 已进入并停住。
    PendingCall in_flight;
    in_flight.Spawn(client, address, MakeRequest("fault-7", "Echo"),
                    DeadlineAfter(kBudgetMs));
    BOOST_REQUIRE(gate->WaitEntered(kBudgetMs));
    BOOST_CHECK_EQUAL(handled->load(), 1);   // 已接纳 = 1

    server->StopAccepting();     // 幂等
    server->StopAccepting();

    // 已接纳 handler 不取消：放行后仍拿到匹配的成功回复。
    gate->Release();
    BOOST_REQUIRE(in_flight.WaitReady());
    BOOST_REQUIRE(in_flight.out().has_value());
    BOOST_REQUIRE(in_flight.out().value());
    BOOST_CHECK_EQUAL(in_flight.out()->value().request_id, "fault-7");
    BOOST_CHECK(in_flight.out()->value().payload ==
                (std::vector<std::uint8_t>{'p', 'i', 'n', 'g', '!'}));

    // 使用合法小请求，不能靠本地 body 限额先拒绝来证明停止接纳。
    // StopAccepting 是「post 到 io 域关闭 acceptor」的异步动作：拒绝形态可能是
    // 连接被物理关闭（未提交）或请求已写出后被 RST（已提交 → OutcomeUnknown），
    // 两者都自洽；确定的事实是「不是成功回复」且「handler 计数仍为 1」。
    auto after = CallSync(client, address, MakeRequest("fault-7b", "Echo"),
                          DeadlineAfter(3000));
    CheckFailureWithoutDispatch(after);
    BOOST_CHECK_EQUAL(handled->load(), 1);   // stop 之后不派发

    server->Close();
    BOOST_CHECK(server->IsClosed());
    client->Close();
    runtime->Close();
    BOOST_CHECK(runtime->IsClosed());
}

// 2-5：同步 Close 幂等；关闭后工厂必须拒绝（不是假成功），
// 且关闭后的调用是确定失败。
BOOST_AUTO_TEST_CASE(t_close_idempotent_and_factories_reject) {
    EnsureScheduler();
    auto runtime = MakeRuntime(8, 8);

    auto server_result = runtime->ListenRpc(
        {"127.0.0.1", 0},
        [](IncomingCallContext, RpcEnvelope request) -> result<RpcEnvelope> {
            return result<RpcEnvelope>::ok(std::move(request));
        });
    BOOST_REQUIRE(server_result);
    auto server = std::move(server_result).value();
    auto client_result = runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);
    auto client = std::move(client_result).value();

    const RpcAddress address{
        "http", "127.0.0.1:" + std::to_string(server->LocalAddress().port)};

    client->Close();
    client->Close();                 // 幂等
    BOOST_CHECK(client->IsClosed());
    // client 关闭后调用：确定拒绝，不是未知。
    auto closed_call = CallSync(client, address, MakeRequest("fault-8", "Echo"),
                                DeadlineAfter(2000));
    BOOST_REQUIRE(!closed_call);
    BOOST_CHECK(closed_call.error().code == ErrorCode::Closed);
    BOOST_CHECK(closed_call.error().code != ErrorCode::OutcomeUnknown);

    server->Close();
    server->Close();                 // 幂等
    BOOST_CHECK(server->IsClosed());

    runtime->Close();
    BOOST_CHECK(runtime->IsClosed());

    // owner 关闭后工厂拒绝（不产生假成功子对象）。
    auto late_client = runtime->CreateRpcClient();
    BOOST_REQUIRE(!late_client);
    BOOST_CHECK(late_client.error().code == ErrorCode::Closed);
    auto late_server = runtime->ListenRpc(
        {"127.0.0.1", 0},
        [](IncomingCallContext, RpcEnvelope request) -> result<RpcEnvelope> {
            return result<RpcEnvelope>::ok(std::move(request));
        });
    BOOST_REQUIRE(!late_server);
    BOOST_CHECK(late_server.error().code == ErrorCode::Closed);

    runtime->Close();
    runtime->Close();                 // 幂等
    BOOST_CHECK(runtime->IsClosed());
}

// 2-5 G6：并发 Close（client/server/runtime 各自多线程）只证明有界返回、
// 最终关闭与拒新工厂；「不悬空 / 回调次数 / 无竞态」是内部事实，本 public
// 用例不宣称观测到它们，也不在此刷内部断言。
BOOST_AUTO_TEST_CASE(t_concurrent_close_is_idempotent) {
    EnsureScheduler();
    auto runtime = MakeRuntime(8, 8);

    auto server_result = runtime->ListenRpc(
        {"127.0.0.1", 0},
        [](IncomingCallContext, RpcEnvelope request) -> result<RpcEnvelope> {
            return result<RpcEnvelope>::ok(std::move(request));
        });
    BOOST_REQUIRE(server_result);
    auto server = std::move(server_result).value();
    auto client_result = runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);
    auto client = std::move(client_result).value();

    std::vector<std::thread> closers;
    for (int i = 0; i < 4; ++i) {
        closers.emplace_back([&client, &server, &runtime] {
            client->Close();
            server->Close();
            runtime->Close();
        });
    }
    for (auto& t : closers)
        t.join();
    BOOST_CHECK(client->IsClosed());
    BOOST_CHECK(server->IsClosed());
    BOOST_CHECK(runtime->IsClosed());
    auto late = runtime->CreateRpcClient();
    BOOST_REQUIRE(!late);
    BOOST_CHECK(late.error().code == ErrorCode::Closed);
}

// 2-5：出站 max_inflight / max_connections 边界 —— 名额不足立即
// Overloaded（有界拒绝，不排队）；名额在物理收口后归还可观察。
//   G3：max_inflight=1 —— 首请求被对端确认收全（在途占名额）⇒ 第二请求
//       Overloaded；先 Release 再等首请求收口；第三请求必须打到另一个
//       能回合法 200 成功的 live peer 并成功，才是「名额确已归还」的证据。
//   G4：max_connections=1 —— 首连接占满名额 ⇒ 第二请求 Overloaded；
//       client.Close() 后同一 runtime 的新 client 必须成功。
BOOST_AUTO_TEST_CASE(t_outbound_inflight_and_connection_bounds) {
    EnsureScheduler();

    // 变体 A：max_inflight=1（链路在途账本）。
    {
        auto runtime = MakeRuntime(/*max_conn=*/8, /*max_inflight=*/1);
        auto client_result = runtime->CreateRpcClient();
        BOOST_REQUIRE(client_result);
        auto client = std::move(client_result).value();
        MarkerGatePeer peer;   // 收全请求后持有、不回复
        const RpcAddress address{"http",
                                 "127.0.0.1:" + std::to_string(peer.port)};

        PendingCall first;
        first.Spawn(client, address, MakeRequest("fault-9", "Echo"),
                    DeadlineAfter(kBudgetMs));   // 预算 >= 内层裕量，见 CallOnce
        // 对端确认收全 ⇒ 首请求已在途并持有名额（有因果依据，不靠 sleep）。
        BOOST_REQUIRE(WaitUntil([&] { return peer.got_full->load(); }));

        auto second = CallSync(client, address, MakeRequest("fault-9b", "Echo"),
                               DeadlineAfter(1000));
        BOOST_REQUIRE(!second);
        BOOST_CHECK(second.error().code == ErrorCode::Overloaded);

        // 先 Release（让对端关闭连接）再等首请求收口，避免依赖 15s 预算兜底。
        peer.Release();
        BOOST_REQUIRE(first.WaitReady());
        BOOST_REQUIRE(first.out().has_value());
        BOOST_REQUIRE(!first.out().value());
        BOOST_CHECK(first.out()->error().code == ErrorCode::OutcomeUnknown);

        // 名额在物理收口后归还：第三请求到另一条 live peer 并拿到合法成功回复。
        RawPeer live_peer([](int fd) {
            if (!ReadFullRequest(fd))
                return;
            RpcEnvelope reply = MakeRequest("fault-9c", "Echo");
            SendAll(fd, Http200WithBody(WireReplyBody(reply, /*success=*/true)));
        });
        auto third = CallSync(client,
                              {"http", "127.0.0.1:" + std::to_string(live_peer.port)},
                              MakeRequest("fault-9c", "Echo"),
                              DeadlineAfter(kBudgetMs));
        BOOST_REQUIRE(third);
        BOOST_CHECK_EQUAL(third.value().request_id, "fault-9c");

        client->Close();
        runtime->Close();
    }

    // 变体 B：max_connections=1 —— 连接名额边界同样确定性 Overloaded，
    // 且 client.Close() 把连接名额归还给所属 runtime（新 client 成功）。
    {
        auto runtime = MakeRuntime(/*max_conn=*/1, /*max_inflight=*/8);
        auto client_result = runtime->CreateRpcClient();
        BOOST_REQUIRE(client_result);
        auto client = std::move(client_result).value();
        MarkerGatePeer peer;
        const RpcAddress address{"http",
                                 "127.0.0.1:" + std::to_string(peer.port)};

        PendingCall first;
        first.Spawn(client, address, MakeRequest("fault-10", "Echo"),
                    DeadlineAfter(kBudgetMs));
        BOOST_REQUIRE(WaitUntil([&] { return peer.got_full->load(); }));

        auto second = CallSync(client, address, MakeRequest("fault-10b", "Echo"),
                               DeadlineAfter(1000));
        BOOST_REQUIRE(!second);
        BOOST_CHECK(second.error().code == ErrorCode::Overloaded);

        // Close 物理释放：在途首请求立即以终态收口，连接名额归还 runtime。
        client->Close();
        BOOST_REQUIRE(first.WaitReady());
        BOOST_REQUIRE(first.out().has_value());
        BOOST_CHECK(!first.out().value());
        peer.Release();

        auto second_client_result = runtime->CreateRpcClient();
        BOOST_REQUIRE(second_client_result);
        auto second_client = std::move(second_client_result).value();
        RawPeer live_peer([](int fd) {
            if (!ReadFullRequest(fd))
                return;
            RpcEnvelope reply = MakeRequest("fault-10c", "Echo");
            SendAll(fd, Http200WithBody(WireReplyBody(reply, /*success=*/true)));
        });
        auto third = CallSync(second_client,
                              {"http", "127.0.0.1:" + std::to_string(live_peer.port)},
                              MakeRequest("fault-10c", "Echo"),
                              DeadlineAfter(kBudgetMs));
        BOOST_REQUIRE(third);
        BOOST_CHECK_EQUAL(third.value().request_id, "fault-10c");

        second_client->Close();
        runtime->Close();
    }
}

// 2-5：body 限额的两侧边界都必须真实可见：
//   (a) 出站：请求体超过所属 runtime 的 max_body_bytes 时，在发起任何 I/O
//       之前确定拒绝（InvalidArgument + domain_code=body_too_large，且不携带
//       请求阶段——它不是出站 operation 的失败）；
//   (b) 入站：服务端预算更小时，超限请求被有界拒绝（413）且不派发 handler，
//       客户端看到明确 HTTP 状态而不是未知结果。
BOOST_AUTO_TEST_CASE(t_body_limit_outbound_and_inbound_are_bounded) {
    EnsureScheduler();

    auto oversized = [](const char* id) {
        RpcEnvelope request = MakeRequest(id, "Echo");
        request.payload.assign(4096, 'x');
        return request;
    };

    // (a) 出站预算拒绝（未连接、未产生任何请求阶段）。
    {
        auto runtime = MakeRuntime(8, 8, /*max_header=*/16 * 1024,
                                   /*max_body=*/2048);
        auto client_result = runtime->CreateRpcClient();
        BOOST_REQUIRE(client_result);
        auto client = std::move(client_result).value();

        auto refused = CallSync(client, {"http", "127.0.0.1:1"},
                                oversized("fault-11a"), DeadlineAfter(2000));
        BOOST_REQUIRE(!refused);
        BOOST_CHECK(refused.error().code == ErrorCode::InvalidArgument);
        BOOST_CHECK_EQUAL(refused.error().domain_code, "body_too_large");
        BOOST_CHECK(!refused.error().request_phase.has_value());

        client->Close();
        runtime->Close();
    }

    // (b) 入站 413：客户端 runtime 预算大、服务端 runtime 预算小。
    {
        auto server_runtime = MakeRuntime(8, 8, /*max_header=*/16 * 1024,
                                          /*max_body=*/2048);
        auto handled = std::make_shared<std::atomic_int>(0);
        auto server_result = server_runtime->ListenRpc(
            {"127.0.0.1", 0},
            [handled](IncomingCallContext, RpcEnvelope request)
                -> result<RpcEnvelope> {
                handled->fetch_add(1);
                return result<RpcEnvelope>::ok(std::move(request));
            });
        BOOST_REQUIRE(server_result);
        auto server = std::move(server_result).value();

        auto client_runtime = MakeRuntime(8, 8, /*max_header=*/16 * 1024,
                                          /*max_body=*/64 * 1024);
        auto client_result = client_runtime->CreateRpcClient();
        BOOST_REQUIRE(client_result);
        auto client = std::move(client_result).value();

        auto res = CallSync(client,
                            {"http", "127.0.0.1:" + std::to_string(
                                         server->LocalAddress().port)},
                            oversized("fault-11b"), DeadlineAfter(kBudgetMs));
        BOOST_REQUIRE(!res);
        BOOST_CHECK(res.error().code == ErrorCode::Unavailable);
        BOOST_CHECK(res.error().code != ErrorCode::OutcomeUnknown);
        BOOST_CHECK_EQUAL(res.error().backend_category, "http");
        BOOST_CHECK_EQUAL(res.error().backend_code, 413);
        BOOST_CHECK_EQUAL(handled->load(), 0);   // 超限请求绝不进业务 handler

        server->Close();
        client->Close();
        client_runtime->Close();
        server_runtime->Close();
    }
}

// 2-5：入站 header 限额 —— 超长 header 被有界拒绝（400 + 断开），
// 且不派发 handler、不把不足一条的消息当成功请求。
BOOST_AUTO_TEST_CASE(t_inbound_header_limit_is_bounded_rejection) {
    EnsureScheduler();
    auto runtime = MakeRuntime(8, 8, /*max_header=*/4096,
                               /*max_body=*/64 * 1024);
    auto handled = std::make_shared<std::atomic_int>(0);
    auto server_result = runtime->ListenRpc(
        {"127.0.0.1", 0},
        [handled](IncomingCallContext, RpcEnvelope request) -> result<RpcEnvelope> {
            handled->fetch_add(1);
            return result<RpcEnvelope>::ok(std::move(request));
        });
    BOOST_REQUIRE(server_result);
    auto server = std::move(server_result).value();

    // 单条超长 header（无结束符）⇒ 解析期 header_limit，可观察 400。
    std::string payload = "POST /rpc HTTP/1.1\r\nHost: 127.0.0.1\r\nX-Pad: ";
    payload.append(32 * 1024, 'a');
    const std::string reply =
        RawExchange(server->LocalAddress().port, payload, 5000);
    BOOST_REQUIRE(!reply.empty());
    BOOST_CHECK(reply.rfind("HTTP/1.1 400", 0) == 0);
    BOOST_CHECK_EQUAL(handled->load(), 0);

    server->Close();
    runtime->Close();
}

// 2-5 G1：入站 max_connections 限额 —— 用「首个连接已进入 RPC handler」作为
// 占住唯一连接名额的因果 marker（不是闲置探针连接），第二个连接被拒绝且
// handler 派发计数不增；首个 handler 完成后新请求可用（名额未泄漏）。
BOOST_AUTO_TEST_CASE(t_inbound_connection_limit_rejects_second_connection) {
    EnsureScheduler();

    auto server_runtime = MakeRuntime(/*max_conn=*/1, /*max_inflight=*/8);
    auto gate    = std::make_shared<HandlerGate>();
    auto handled = std::make_shared<std::atomic_int>(0);
    auto server_result = server_runtime->ListenRpc(
        {"127.0.0.1", 0},
        [gate, handled](IncomingCallContext,
                        RpcEnvelope request) -> result<RpcEnvelope> {
            handled->fetch_add(1);
            gate->Enter();
            if (!gate->WaitReleased(kGateBudgetMs))
                return result<RpcEnvelope>::err(
                    MakeError(ErrorCode::TimedOut, "test gate timeout"));
            request.payload.push_back('!');
            return result<RpcEnvelope>::ok(std::move(request));
        });
    BOOST_REQUIRE(server_result);
    auto server = std::move(server_result).value();

    // 客户端 runtime 出站名额充足，避免把出站配额混入本用例。
    auto client_runtime = MakeRuntime(8, 8);
    auto client_result = client_runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);
    auto client = std::move(client_result).value();

    const RpcAddress address{
        "http", "127.0.0.1:" + std::to_string(server->LocalAddress().port)};

    PendingCall first;
    first.Spawn(client, address, MakeRequest("fault-14", "Echo"),
                DeadlineAfter(kBudgetMs));
    // 已进入 handler ⇒ 该连接已真实占用唯一连接名额（因果事件，不靠闲置探针）。
    BOOST_REQUIRE(gate->WaitEntered(kBudgetMs));
    BOOST_CHECK_EQUAL(handled->load(), 1);

    // 第二条连接：连接名额已满 ⇒ 拒绝（对端丢弃连接），不派发 handler。
    auto second = CallSync(client, address, MakeRequest("fault-14b", "Echo"),
                           DeadlineAfter(5000));
    CheckFailureWithoutDispatch(second);
    BOOST_CHECK_EQUAL(handled->load(), 1);   // 超限连接不派发

    // 已接纳 handler 不被抢占：放行后仍成功回复。
    gate->Release();
    BOOST_REQUIRE(first.WaitReady());
    BOOST_REQUIRE(first.out().has_value());
    BOOST_REQUIRE(first.out().value());
    BOOST_CHECK_EQUAL(first.out()->value().request_id, "fault-14");

    // 首连接完成后名额归还：新请求最终可用（有界重试，成功本身是证据）。
    bool accepted = false;
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds{kGateBudgetMs};
    while (!accepted && std::chrono::steady_clock::now() < deadline) {
        auto retry = CallSync(client, address, MakeRequest("fault-14c", "Echo"),
                              DeadlineAfter(3000));
        accepted = static_cast<bool>(retry);
    }
    BOOST_CHECK(accepted);
    BOOST_CHECK(handled->load() >= 2);

    server->Close();
    client->Close();
    client_runtime->Close();
    server_runtime->Close();
}

// 2-5 G2：已接纳 handler 停在闸门时 Close 服务端 —— Close 有界返回且不抢占
// handler；socket 已物理关闭，放行后该 handler 的回复不得以成功交付。
// RAII/显式 Close + 有界 gate：不让用例析构无界阻塞。
BOOST_AUTO_TEST_CASE(t_server_close_while_handler_gated_is_bounded) {
    EnsureScheduler();
    auto runtime = MakeRuntime(8, 8);

    auto gate = std::make_shared<HandlerGate>();
    auto server_result = runtime->ListenRpc(
        {"127.0.0.1", 0},
        [gate](IncomingCallContext, RpcEnvelope request) -> result<RpcEnvelope> {
            gate->Enter();
            if (!gate->WaitReleased(kGateBudgetMs))
                return result<RpcEnvelope>::err(
                    MakeError(ErrorCode::TimedOut, "test gate timeout"));
            request.payload.push_back('!');
            return result<RpcEnvelope>::ok(std::move(request));
        });
    BOOST_REQUIRE(server_result);
    auto server = std::move(server_result).value();

    auto client_result = runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);
    auto client = std::move(client_result).value();

    const RpcAddress address{
        "http", "127.0.0.1:" + std::to_string(server->LocalAddress().port)};

    PendingCall in_flight;
    in_flight.Spawn(client, address, MakeRequest("fault-15", "Echo"),
                    DeadlineAfter(kBudgetMs));
    BOOST_REQUIRE(gate->WaitEntered(kBudgetMs));

    // handler 仍停在闸门上时 Close：必须在有界窗口内返回（drain ≤ 5s + 裕量），
    // 且不抢占 handler（handler 不被取消，只是回复路径已随 teardown 关闭）。
    const auto close_start = std::chrono::steady_clock::now();
    server->Close();
    const auto close_elapsed = std::chrono::steady_clock::now() - close_start;
    BOOST_CHECK(server->IsClosed());
    BOOST_CHECK(close_elapsed < std::chrono::seconds{15});

    // 放行 handler：socket 已关，放行后不得拿到成功回复。
    gate->Release();
    BOOST_REQUIRE(in_flight.WaitReady());
    BOOST_REQUIRE(in_flight.out().has_value());
    BOOST_REQUIRE(!in_flight.out().value());   // 不是成功交付
    BOOST_CHECK(in_flight.out()->error().code == ErrorCode::OutcomeUnknown);
    BOOST_CHECK(SawCommittedWrite(in_flight.out()->error()));

    server->Close();                 // 幂等
    client->Close();
    runtime->Close();
    BOOST_CHECK(runtime->IsClosed());
}

// 2-5：入站 in-flight 限额 —— 已接纳 handler 占满名额时，后续请求被有界
// 拒绝（503）而不是无界排队；已接纳 handler 本身不受影响，放行后正常回复。
BOOST_AUTO_TEST_CASE(t_inbound_inflight_limit_is_bounded_rejection) {
    EnsureScheduler();

    // 服务端 runtime：入站名额 1；客户端 runtime：出站名额充足。
    auto server_runtime = MakeRuntime(8, /*max_inflight=*/1);
    auto gate = std::make_shared<HandlerGate>();
    auto server_result = server_runtime->ListenRpc(
        {"127.0.0.1", 0},
        [gate](IncomingCallContext, RpcEnvelope request) -> result<RpcEnvelope> {
            gate->Enter();
            if (!gate->WaitReleased(kGateBudgetMs))
                return result<RpcEnvelope>::err(
                    MakeError(ErrorCode::TimedOut, "test gate timeout"));
            request.payload.push_back('!');
            return result<RpcEnvelope>::ok(std::move(request));
        });
    BOOST_REQUIRE(server_result);
    auto server = std::move(server_result).value();

    auto client_runtime = MakeRuntime(8, 8);
    auto client_result = client_runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);
    auto client = std::move(client_result).value();

    const RpcAddress address{
        "http", "127.0.0.1:" + std::to_string(server->LocalAddress().port)};

    PendingCall occupying;   // 占满唯一的入站名额（handler 停在闸门上）
    occupying.Spawn(client, address, MakeRequest("fault-13", "Echo"),
                    DeadlineAfter(kBudgetMs));
    BOOST_REQUIRE(gate->WaitEntered(kBudgetMs));

    // 第二个请求：服务端名额已满 ⇒ 明确 503（可信回复终态），不是未知。
    auto rejected = CallSync(client, address, MakeRequest("fault-13b", "Echo"),
                             DeadlineAfter(5000));
    BOOST_REQUIRE(!rejected);
    BOOST_CHECK(rejected.error().code == ErrorCode::Unavailable);
    BOOST_CHECK(rejected.error().code != ErrorCode::OutcomeUnknown);
    BOOST_CHECK_EQUAL(rejected.error().backend_code, 503);

    // 已接纳 handler 不被抢占：放行后仍成功回复。
    gate->Release();
    BOOST_REQUIRE(occupying.WaitReady());
    BOOST_REQUIRE(occupying.out().has_value());
    BOOST_REQUIRE(occupying.out().value());
    BOOST_CHECK_EQUAL(occupying.out()->value().request_id, "fault-13");

    server->Close();
    client->Close();
    client_runtime->Close();
    server_runtime->Close();
}

// 2-5：请求已完整写出后 Close —— 在途调用立即以只交付一次的终态收口
// （未知事实，不是「未发送」的确定失败）；Close 后 peer 侧确认连接已关闭
// （EOF/ECONNRESET/EPIPE），已发布终态不再改变，之后的调用确定拒绝。
BOOST_AUTO_TEST_CASE(t_close_post_commit_terminal_once_and_late_reply) {
    EnsureScheduler();
    auto runtime = MakeRuntime(/*max_conn=*/8, /*max_inflight=*/1);
    auto client_result = runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);
    auto client = std::move(client_result).value();

    const auto late_body = WireReplyBody(MakeRequest("fault-12", "Echo"), true);
    MarkerGatePeer peer(Http200WithBody(late_body));
    const RpcAddress address{"http", "127.0.0.1:" + std::to_string(peer.port)};
    PendingCall pending;
    pending.Spawn(client, address, MakeRequest("fault-12", "Echo"),
                  DeadlineAfter(kBudgetMs));
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

    // Close 后 peer 确认连接已关闭：bounded recv 看到 EOF/ECONNRESET，或迟到
    // 回包 send 得到 EPIPE。这是本端物理收口的对端侧事实记录，不声称本端收到
    // 了迟到字节（send 成功也只说明字节进了对端发送缓冲）。
    peer.Release();
    BOOST_REQUIRE(WaitUntil([&] { return peer.ClosedObserved(); }));
    BOOST_CHECK(peer.CloseFact() >= 1 && peer.CloseFact() <= 3);
    BOOST_CHECK(pending.out()->error().code == ErrorCode::OutcomeUnknown);

    // 关闭后的新调用是确定拒绝，不是未知。
    auto closed_call = CallSync(client, address, MakeRequest("fault-12b", "Echo"),
                                DeadlineAfter(2000));
    BOOST_REQUIRE(!closed_call);
    BOOST_CHECK(closed_call.error().code == ErrorCode::Closed);

    runtime->Close();
    BOOST_CHECK(runtime->IsClosed());
}

BOOST_AUTO_TEST_SUITE_END()
