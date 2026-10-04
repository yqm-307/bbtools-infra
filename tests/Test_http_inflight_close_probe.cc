// Issue #62 补充回归探针：在途 handler 与 peer-FIN / owner Close/Teardown 并发。
//
// 覆盖 fix review 明确列出的两个缺口（现有 timer-rearm 探针未覆盖）：
//   Case A（peer-FIN in-flight）：请求已被接纳、handler 尚未返回时，客户端半关闭
//     （send 后 shutdown(SHUT_WR)）让服务端看到对端 FIN。断言：OnPeerWatch 走完
//     收口（会话反登记、DebugSessionCount→0）、服务端 listener 仍 OPEN（只有会话被
//     关，不是全局 Close）、handler 退出后其回投不触碰已关 socket（sanitizer 无报错）。
//   Case B（Close vs in-flight）：handler 在途时另一线程调用 server->Close()。
//     断言：同步 Close 契约（返回即封口+物理 teardown+有界 drain）、handler 退出、
//     会话归零、连接被拒、且 Close 是「drain 归零」返回而不是 5s 超时返回。
//   Case B2（Teardown vs in-flight）：直接并发调用 Teardown()（不走 Close），
//     断言同步中止在途会话（计数归零）但对象未 MarkClosed（IsClosed 仍 false），
//     随后 handler 退出、Close 收口；Teardown 幂等重入无副作用。
//
// 受控在途窗口：handler 进入后经条件变量挂起，由测试线程显式释放；不用固定 sleep，
// 全部等待都有预算（Conns 阻塞有界，不会永久挂起），不把永久阻塞 handler 当成功。
//
// 无第三方测试框架：失败打印 stderr 并以独立非零退出；等待全部有界。
// sanitizer 覆盖：本目标以 ASan/UBSan（及 TSan 尝试）构建运行时，取消/关闭/晚到
//   回投期间若出现悬垂访问由 sanitizer 直接报出（扫描 stderr 日志，不只看退出码）。

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>

#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

// 探针：直接读取已注册会话数与封口态（impl 层只读接口），src/ 内部头仅本目标可见。
#include "http/HttpServerImpl.hpp"

using namespace bbt::infra;

namespace {

int g_checks   = 0;
int g_failures = 0;

void Check(bool cond, const char* expr, int line) {
    ++g_checks;
    if (!cond) {
        ++g_failures;
        std::fprintf(stderr, "FAIL inflight-probe line %d: %s\n", line, expr);
    }
}
#define CHECK(cond) Check(static_cast<bool>(cond), #cond, __LINE__)

// 受控在途窗口：handler 进入后在此挂起，直到测试线程显式释放（有界，防永久挂起）。
struct HandlerGate {
    std::mutex              mtx;
    std::condition_variable cv;
    bool                    entered{false};
    bool                    release{false};
    std::atomic_int         calls{0};
    std::atomic_int         exits{0};
    std::chrono::milliseconds hold_max{15000};
};

HandlerGate g_gate;

void ResetGate() {
    std::lock_guard<std::mutex> lk(g_gate.mtx);
    g_gate.entered = false;
    g_gate.release = false;
    g_gate.calls.store(0);
    g_gate.exits.store(0);
}

result<HttpResponse> GatedHandler(IncomingCallContext, HttpRequest) {
    g_gate.calls.fetch_add(1);
    {
        std::unique_lock<std::mutex> lk(g_gate.mtx);
        g_gate.entered = true;
        g_gate.cv.notify_all();
        g_gate.cv.wait_for(lk, g_gate.hold_max,
                           [] { return g_gate.release; });
    }
    g_gate.exits.fetch_add(1);
    return result<HttpResponse>::ok(HttpResponse{200, {}, "ok"});
}

bool WaitEntered(int budget_ms) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(budget_ms);
    for (;;) {
        {
            std::lock_guard<std::mutex> lk(g_gate.mtx);
            if (g_gate.entered)
                return true;
        }
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::yield();
    }
}

void ReleaseHandler() {
    std::lock_guard<std::mutex> lk(g_gate.mtx);
    g_gate.release = true;
    g_gate.cv.notify_all();
}

template <class Pred>
bool WaitUntil(Pred pred, int budget_ms) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(budget_ms);
    for (;;) {
        if (pred())
            return true;
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::yield();
    }
}

int ConnectTo(std::uint16_t port) {
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

// 发送一条 keep-alive 请求，有界；不读响应（Case 目标是在途窗口）。
bool SendRequest(int fd) {
    static const char kReq[] =
        "GET /inflight HTTP/1.1\r\nHost: probe\r\n"
        "Connection: keep-alive\r\n\r\n";
    const ssize_t n = ::send(fd, kReq, sizeof(kReq) - 1, MSG_NOSIGNAL);
    return n == static_cast<ssize_t>(sizeof(kReq) - 1);
}

// 关掉客户端 fd 时读掉残余：服务端 Close 后应迅速 EOF（0）；仅作辅助观测。
void DrainClose(int fd) {
    if (fd < 0)
        return;
    ::close(fd);
}

std::shared_ptr<HttpServer> MakeServer(const std::shared_ptr<NetworkRuntime>& rt) {
    auto srv = rt->ListenHttp({"127.0.0.1", 0}, GatedHandler);
    if (!srv)
        return nullptr;
    return std::move(srv).value();
}

// ---- Case A：peer-FIN 发生在 handler 在途期间 ----
void RunCaseA() {
    std::fprintf(stderr, "case A: peer-FIN during in-flight handler\n");
    ResetGate();

    NetworkLimits limits{};
    limits.max_connections  = 64;
    limits.max_inflight     = 64;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = 64 * 1024;
    limits.incoming_timeout = std::chrono::milliseconds(30000);   // 不依赖期限触发

    auto rt = NetworkRuntime::Create(limits);
    CHECK(rt);
    auto runtime = std::move(rt).value();
    CHECK(runtime->Start());

    auto server = MakeServer(runtime);
    CHECK(server != nullptr);
    if (!server)
        return;
    auto impl = std::dynamic_pointer_cast<http_detail::HttpServerImpl>(server);
    CHECK(impl != nullptr);
    if (!impl)
        return;
    const auto port = server->LocalAddress().port;
    CHECK(port != 0);

    const int fd = ConnectTo(port);
    CHECK(fd >= 0);
    if (fd < 0)
        return;
    CHECK(SendRequest(fd));
    // handler 被接纳并在途（受控窗口）。
    CHECK(WaitEntered(10000));
    CHECK(impl->DebugSessionCount() >= 1);
    // 客户端 FIN（半关闭写端）：服务端 OnPeerWatch 应观察到对端断开。
    ::shutdown(fd, SHUT_WR);

    // 会话必须由 peer-watch 收口并反登记（记账归零）。修复前 deadline 记账泄漏会
    // 使已关会话无法反登记，此断言永不为真。
    const bool drained =
        WaitUntil([&] { return impl->DebugSessionCount() == 0; }, 8000);
    std::fprintf(stderr,
                 "case A: entered=1 calls=%d exits=%d registered_sessions=%zu "
                 "drained=%d server_open=%d\n",
                 g_gate.calls.load(), g_gate.exits.load(),
                 impl->DebugSessionCount(), drained ? 1 : 0,
                 server->IsClosed() ? 0 : 1);
    CHECK(drained);
    // 只有该会话被关：listener 未 Close（区分「会话级 peer-FIN 收口」与「全局 Close」）。
    CHECK(!server->IsClosed());

    // 释放在途 handler：其在途回投走 DeliverResult，会话已 closed → 不写任何字节，
    // 不触碰已释放资源（ASan/UBSan 在此路径报悬垂即失败）。
    ReleaseHandler();
    CHECK(WaitUntil([&] { return g_gate.exits.load() >= 1; }, 8000));
    CHECK(g_gate.calls.load() >= 1);

    DrainClose(fd);

    // 干净收口：listener 物理释放，新连接被拒。
    server->Close();
    CHECK(server->IsClosed());
    const int refused = ConnectTo(port);
    CHECK(refused < 0);
    if (refused >= 0)
        ::close(refused);

    runtime->Close();
}

// ---- Case B：另一线程在 handler 在途时调用 server->Close() ----
void RunCaseB() {
    std::fprintf(stderr, "case B: owner Close() concurrent with in-flight handler\n");
    ResetGate();

    NetworkLimits limits{};
    limits.max_connections  = 64;
    limits.max_inflight     = 64;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = 64 * 1024;
    limits.incoming_timeout = std::chrono::milliseconds(30000);

    auto rt = NetworkRuntime::Create(limits);
    CHECK(rt);
    auto runtime = std::move(rt).value();
    CHECK(runtime->Start());

    auto server = MakeServer(runtime);
    CHECK(server != nullptr);
    if (!server)
        return;
    auto impl = std::dynamic_pointer_cast<http_detail::HttpServerImpl>(server);
    CHECK(impl != nullptr);
    if (!impl)
        return;
    const auto port = server->LocalAddress().port;

    const int fd = ConnectTo(port);
    CHECK(fd >= 0);
    if (fd < 0)
        return;
    CHECK(SendRequest(fd));
    CHECK(WaitEntered(10000));   // handler 在途，尚未返回

    long long  close_ms      = -1;
    int        exits_at_ret  = -1;
    std::thread closer([&] {
        const auto t0 = std::chrono::steady_clock::now();
        server->Close();   // 任意线程、幂等、返回即物理释放
        close_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - t0)
                       .count();
        exits_at_ret = g_gate.exits.load();
    });

    // 握手：观察到 Close 已线性化封口（m_close 离开 Open）后，再释放在途 handler。
    // 这保证 Close 的 teardown/drain 与在途 handler 真实重叠，而不是先后串行。
    CHECK(WaitUntil([&] { return !impl->OpenForIo(); }, 5000));
    ReleaseHandler();
    closer.join();

    std::fprintf(stderr,
                 "case B: close_ms=%lld exits_at_close_return=%d calls=%d "
                 "exits=%d registered_sessions=%zu closed=%d\n",
                 close_ms, exits_at_ret, g_gate.calls.load(),
                 g_gate.exits.load(), impl->DebugSessionCount(),
                 server->IsClosed() ? 1 : 0);
    // 同步 Close 契约：返回即已 MarkClosed。
    CHECK(server->IsClosed());
    // Close 经 drain 谓词归零返回（pending_handlers==0 需 handler 已退出），
    // 而不是在途 handler 未退、被 kCloseDrainTimeout(5s) 超时放行。
    CHECK(exits_at_ret >= 1);
    CHECK(close_ms >= 0 && close_ms < 3000);
    // 会话与在途 handler 均已收口。
    CHECK(impl->DebugSessionCount() == 0);
    CHECK(g_gate.exits.load() >= 1);

    // listener 物理释放：新连接被拒。
    const int refused = ConnectTo(port);
    CHECK(refused < 0);
    if (refused >= 0)
        ::close(refused);
    DrainClose(fd);

    runtime->Close();
}

// ---- Case B2：直接并发调用 Teardown()（不经 Close）----
void RunCaseB2() {
    std::fprintf(stderr, "case B2: Teardown() concurrent with in-flight handler\n");
    ResetGate();

    NetworkLimits limits{};
    limits.max_connections  = 64;
    limits.max_inflight     = 64;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = 64 * 1024;
    limits.incoming_timeout = std::chrono::milliseconds(30000);

    auto rt = NetworkRuntime::Create(limits);
    CHECK(rt);
    auto runtime = std::move(rt).value();
    CHECK(runtime->Start());

    auto server = MakeServer(runtime);
    CHECK(server != nullptr);
    if (!server)
        return;
    auto impl = std::dynamic_pointer_cast<http_detail::HttpServerImpl>(server);
    CHECK(impl != nullptr);
    if (!impl)
        return;
    const auto port = server->LocalAddress().port;

    const int fd = ConnectTo(port);
    CHECK(fd >= 0);
    if (fd < 0)
        return;
    CHECK(SendRequest(fd));
    CHECK(WaitEntered(10000));   // handler 在途

    std::thread teardown([&] { impl->Teardown(); });
    teardown.join();

    // Teardown 同步中止在途会话：会话记账归零（handler 仍挂起，但已不拥有 fd/资源）。
    const bool aborted =
        WaitUntil([&] { return impl->DebugSessionCount() == 0; }, 5000);
    std::fprintf(stderr,
                 "case B2: aborted_sessions=%d registered_sessions=%zu "
                 "closed=%d calls=%d\n",
                 aborted ? 1 : 0, impl->DebugSessionCount(),
                 server->IsClosed() ? 1 : 0, g_gate.calls.load());
    CHECK(aborted);
    // Teardown 不是 Close：对象未 MarkClosed。
    CHECK(!server->IsClosed());
    // 幂等重入无副作用。
    impl->Teardown();

    ReleaseHandler();
    CHECK(WaitUntil([&] { return g_gate.exits.load() >= 1; }, 8000));
    DrainClose(fd);

    // 用 Close 完成最终收口。
    server->Close();
    CHECK(server->IsClosed());
    runtime->Close();
}

} // namespace

int main() {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    // 4 个调度线程：probe 的 handler 会阻塞一个调度线程等待释放，其余线程继续
    // 推进 io 域（peer-watch 完成项 / DeliverResult），避免自持死锁。
    cfg->m_cfg_static_thread_num = 4;
    // 与既有 HTTP 套件一致：handler 链路较深，上游默认纤程栈不够。
    cfg->m_cfg_stack_size = 1024 * 256;
    auto& sche = bbt::coroutine::detail::Scheduler::GetInstance();
    sche->Start(bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);
    CHECK(sche->IsInitialized());

    RunCaseA();
    RunCaseB();
    RunCaseB2();

    std::fprintf(stderr, "inflight-probe: checks=%d failures=%d\n", g_checks,
                 g_failures);
    return g_failures == 0 ? 0 : 1;
}
