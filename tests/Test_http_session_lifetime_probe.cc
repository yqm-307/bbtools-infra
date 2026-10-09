// Issue #62 验收第 5 项回归探针：HttpSession lifetime/teardown 的
// 「m_sessions 反登记先于最后一次 shared_ptr 释放」顺序 + 活跃 keep-alive
// 会话在另一线程 owner Close() 下的收口回归。
//
// 断言目标（可机器判定，不靠时序巧合）：
//
//   Case 1（受控交错 / 顺序本身）：
//     K 条会话每条完成 1 次 keep-alive 请求后进入空闲（下一次 BeginRead 的
//     deadline 看守与读就绪等待都已 armed，各自持有一份 self 强引用）。随后
//     【测试线程先持有 io 域门】再调用 owner Close()：持门期间 io 域无法执行任何
//     完成 handler，故三条 armed completion 路径（deadline / 读写就绪 / peer 观测）
//     持有的 self 都不会被释放。前提：本用例每条连接都读到响应，handler 已退出，
//     断言窗口内不存在可释放的协程侧 self。Close() 返回当刻断言：
//       a) 登记会话数已为 0       —— 反登记在 Close 返回前完成；
//       b) 会话析构数增量为 0     —— 最后一次 shared_ptr 尚未释放（防「Close 内
//          提前释放最后一次强引用」这类回归；本用例的负向对照未触发该断言）；
//       即「反登记严格先于最后一次强引用释放」，且 Close 不是被 5s drain 超时
//       放行；listener 与全部会话 fd 已物理释放（新连接被拒 + socket fd 计数回落）。
//     释放门后 io 域跑完中止的完成项：断言 K 条会话全部析构、
//     destroyed_while_registered 恒为 0（无「仍登记即析构」的结构性缺陷）。
//
//   Case 2a（空闲 keep-alive + 并发 Close）：
//     N 条连接各完成 >=2 次 keep-alive 请求（强制 deadline timer cancel→re-arm），
//     随后在客户端保持连接、服务端会话空闲时由另一线程调用 Close()。断言 Close
//     返回当刻登记数已为 0、随后 N 条会话全部析构且无「仍登记即析构」。
//
//   Case 2b（在途 handler + 并发 Close）：
//     4 条 keep-alive 连接，其中 2 条的末次请求被受控 gated handler 挂起在途，
//     另 2 条空闲；另一线程并发 Close()。断言 Close 返回当刻登记数已为 0
//     （teardown 先于 drain 等待），handler 退出后全部会话析构且无「仍登记即析构」。
//
// 两个验证面（PR #70 review 整改：debug 组件与数据独立、仅 debug 构建存在）：
//   会话生命周期计数已迁入独立 debug 组件 src/debug/InfraDebug.hpp，仅在
//   BBT_INFRA_STRINGENT_DEBUG 下存在。因此：
//     Debug 面（宏 ON）  — 在上述生产可观测断言之外，追加计数断言（析构增量 /
//                          「仍登记即析构」= 0），可机器判定「反登记严格先于最后
//                          一次强引用释放」这一顺序本身；
//     Release 面（宏 OFF）— 无 debug 计数：DEBUG 组件的计数与访问器在本构建里
//                          根本不存在（不是返回常数 0 的假接口）。本探针退回全部
//                          可生产观测的等价断言：Close 返回即反登记归零、物理 fd
//                          回收且新连接被拒、未走 5s drain 超时、受控在途 handler
//                          真退出、drain 归零。故关掉宏不会让本用例恒过。
//
// 反证（探针有效性）：把 CancelDeadlineWatch 的记账递减去掉（复现 #63 之前的
//   inflight 多计），Case 1/2 的 (a) 断言即失败——两个验证面都能捕获该缺陷类
//   （(a) 用的是生产状态 DebugSessionCount()，非 debug 计数）。
//
// sanitizer 覆盖：本目标以 ASan/UBSan 构建运行；取消/反登记/析构交错期间出现
//   悬垂访问由 sanitizer 直接报出（扫描 stderr 日志，不只看退出码）。
//
// 无第三方测试框架：失败打印 stderr 并以非零退出；等待全部有界（无 sleep 驱动
// 时序，只用有预算的轮询），不会无限挂起。

#include <arpa/inet.h>
#include <dirent.h>
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
#include <vector>

#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

// 探针：直接读取 impl 层只读接口（生产状态的登记会话数 / io 域门）与独立 debug
// 观测组件（仅 DEBUG 构建存在）；src/ 内部头仅本目标可见。
#include "http/HttpServerImpl.hpp"

using namespace bbt::infra;

#ifdef BBT_INFRA_STRINGENT_DEBUG
// Debug 面：独立 debug 观测组件（src/debug/InfraDebug.hpp）。
namespace dbg = bbt::infra::debug;
#endif

namespace {

int g_checks   = 0;
int g_failures = 0;

void Check(bool cond, const char* expr, int line) {
    ++g_checks;
    if (!cond) {
        ++g_failures;
        std::fprintf(stderr, "FAIL lifetime-probe line %d: %s\n", line, expr);
    }
}
#define CHECK(cond) Check(static_cast<bool>(cond), #cond, __LINE__)

using Session = http_detail::HttpSession;

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

// 物理 fd 观测：/proc/self/fd 中目标为 socket: 的项（eventfd/epoll/timerfd
// 是 runtime 自身基建，不计入）。
int CountSockets() {
    DIR* d = ::opendir("/proc/self/fd");
    if (!d)
        return -1;
    int         n = 0;
    dirent*     e = nullptr;
    while ((e = ::readdir(d)) != nullptr) {
        char path[320];
        char buf[320];
        std::snprintf(path, sizeof(path), "/proc/self/fd/%s", e->d_name);
        const ssize_t len = ::readlink(path, buf, sizeof(buf) - 1);
        if (len <= 0)
            continue;
        buf[len] = '\0';
        if (std::strncmp(buf, "socket:", 7) == 0)
            ++n;
    }
    ::closedir(d);
    return n;
}

bool SendKeepAlive(int fd, const char* path) {
    char req[192];
    const int n = std::snprintf(req, sizeof(req),
                                "GET %s HTTP/1.1\r\nHost: probe\r\n"
                                "Connection: keep-alive\r\n\r\n",
                                path);
    const ssize_t sent = ::send(fd, req, static_cast<std::size_t>(n),
                                MSG_NOSIGNAL);
    return sent == static_cast<ssize_t>(n);
}

// 读一条完整 HTTP/1.1 响应（头部 + Content-Length 正文），有界。
bool ReadResponse(int fd) {
    std::string buf;
    std::size_t header_end  = std::string::npos;
    std::size_t content_len = 0;
    char        tmp[1024];
    const auto  deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    for (;;) {
        if (header_end != std::string::npos &&
            buf.size() >= header_end + content_len)
            return true;
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        const ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
        if (n <= 0)
            return false;
        buf.append(tmp, static_cast<std::size_t>(n));
        if (header_end == std::string::npos) {
            const auto he = buf.find("\r\n\r\n");
            if (he != std::string::npos) {
                header_end = he + 4;
                const auto cl = buf.find("Content-Length:");
                if (cl != std::string::npos && cl < header_end)
                    content_len = static_cast<std::size_t>(
                        std::strtoul(buf.c_str() + cl + 15, nullptr, 10));
            }
        }
    }
}

result<HttpResponse> EchoHandler(IncomingCallContext, HttpRequest) {
    return result<HttpResponse>::ok(HttpResponse{200, {}, "ok"});
}

// 受控在途窗口（Case 2b）：handler 进入后挂起，由测试线程显式释放（有界）。
struct HandlerGate {
    std::mutex              mtx;
    std::condition_variable cv;
    bool                    entered{false};
    bool                    release{false};
    std::atomic_int         calls{0};
    std::atomic_int         exits{0};
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
        g_gate.cv.wait_for(lk, std::chrono::milliseconds(15000),
                           [] { return g_gate.release; });
    }
    g_gate.exits.fetch_add(1);
    return result<HttpResponse>::ok(HttpResponse{200, {}, "ok"});
}

bool WaitEntered(int budget_ms) {
    return WaitUntil(
        [] {
            std::lock_guard<std::mutex> lk(g_gate.mtx);
            return g_gate.entered;
        },
        budget_ms);
}

void ReleaseHandler() {
    std::lock_guard<std::mutex> lk(g_gate.mtx);
    g_gate.release = true;
    g_gate.cv.notify_all();
}

// Case 2b 混用：/inflight 走受控挂起的 handler，其余路径立即返回。
result<HttpResponse> MixedHandler(IncomingCallContext ctx, HttpRequest req) {
    if (req.url == "/inflight")
        return GatedHandler(std::move(ctx), std::move(req));
    return EchoHandler(std::move(ctx), std::move(req));
}

NetworkLimits ProbeLimits() {
    NetworkLimits limits{};
    limits.max_connections  = 64;
    limits.max_inflight     = 64;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = 64 * 1024;
    // 期限预算给足：本探针只针对反登记/析构顺序，不依赖期限触发。
    limits.incoming_timeout = std::chrono::milliseconds(30000);
    return limits;
}

// ---------------------------------------------------------------- Case 1

void RunCaseOrdering() {
    constexpr int kIdleConns = 6;
    std::fprintf(stderr, "case 1: unregister strictly precedes last release\n");

#ifdef BBT_INFRA_STRINGENT_DEBUG
    const int destroyed_base = dbg::HttpSessionDestroyed();
    const int tripwire_base  = dbg::HttpSessionDestroyedWhileRegistered();
#endif

    auto rt = NetworkRuntime::Create(ProbeLimits());
    CHECK(rt);
    if (!rt)
        return;
    auto runtime = std::move(rt).value();
    CHECK(runtime->Start());

    auto srv = runtime->ListenHttp({"127.0.0.1", 0}, EchoHandler);
    CHECK(srv);
    if (!srv) {
        runtime->Close();
        return;
    }
    auto server = std::move(srv).value();
    auto impl   = std::dynamic_pointer_cast<http_detail::HttpServerImpl>(server);
    CHECK(impl != nullptr);
    if (!impl) {
        runtime->Close();
        return;
    }
    const auto port = server->LocalAddress().port;
    CHECK(port != 0);

    std::vector<int> fds;
    for (int i = 0; i < kIdleConns; ++i) {
        const int fd = ConnectTo(port);
        CHECK(fd >= 0);
        if (fd < 0)
            break;
        fds.push_back(fd);
        CHECK(SendKeepAlive(fd, "/idle"));
        CHECK(ReadResponse(fd));
    }
    const int established = static_cast<int>(fds.size());
    CHECK(established == kIdleConns);
    // 全部会话已登记并进入「空闲等待下一条请求」（deadline 看守 + 读就绪等待）。
    const bool all_registered =
        WaitUntil([&] { return impl->DebugSessionCount() == static_cast<std::size_t>(established); }, 8000);
    CHECK(all_registered);

#ifdef BBT_INFRA_STRINGENT_DEBUG
    const int destroyed_before_close = dbg::HttpSessionDestroyed();
#endif
    const int sockets_before_close   = CountSockets();

    int       registered_at_close = -1;
    int       sockets_after_close = -1;
    bool      closed_at_close     = false;
    bool      refused_at_close    = false;
    long long close_ms            = -1;
#ifdef BBT_INFRA_STRINGENT_DEBUG
    int       destroyed_at_close  = -1;
#endif
    {
        // 先持 io 域门：持门期间 io 完成线程无法执行完成 handler，也无法释放
        // 任何 self 强引用。Close() 在本线程内经可递归门同步执行。
        std::unique_lock<std::recursive_mutex> hold(impl->Engine()->IoGate());
        const auto t0 = std::chrono::steady_clock::now();
        server->Close();
        close_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - t0)
                       .count();
        registered_at_close = static_cast<int>(impl->DebugSessionCount());
#ifdef BBT_INFRA_STRINGENT_DEBUG
        destroyed_at_close  = dbg::HttpSessionDestroyed();
#endif
        sockets_after_close = CountSockets();
        closed_at_close     = server->IsClosed();
        const int refused   = ConnectTo(port);
        refused_at_close    = refused < 0;
        if (refused >= 0)
            ::close(refused);
    }
#ifdef BBT_INFRA_STRINGENT_DEBUG
    std::fprintf(stderr,
                 "case 1: close_ms=%lld registered_at_close=%d "
                 "destroyed_delta_at_close=%d sockets %d->%d closed=%d "
                 "refused=%d\n",
                 close_ms, registered_at_close,
                 destroyed_at_close - destroyed_before_close,
                 sockets_before_close, sockets_after_close,
                 closed_at_close ? 1 : 0, refused_at_close ? 1 : 0);
#else
    std::fprintf(stderr,
                 "case 1 (release surface): close_ms=%lld "
                 "registered_at_close=%d sockets %d->%d closed=%d refused=%d\n",
                 close_ms, registered_at_close, sockets_before_close,
                 sockets_after_close, closed_at_close ? 1 : 0,
                 refused_at_close ? 1 : 0);
#endif

    // (a) 反登记在 Close 返回前完成，且不是被 5s drain 超时放行。
    CHECK(registered_at_close == 0);
    CHECK(closed_at_close);
    CHECK(close_ms >= 0 && close_ms < 2500);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    // (b) 最后一次 shared_ptr 尚未释放 —— 顺序断言的核心（Debug 面专属）。
    CHECK(destroyed_at_close - destroyed_before_close == 0);
#endif
    // 物理释放：listener + 全部会话 fd（会话 fd 数 + acceptor）在返回当刻已回收。
    CHECK(sockets_before_close > 0);
    CHECK(sockets_after_close >= 0);
    if (sockets_before_close > 0 && sockets_after_close >= 0)
        CHECK(sockets_before_close - sockets_after_close >= established + 1);
    CHECK(refused_at_close);

#ifdef BBT_INFRA_STRINGENT_DEBUG
    // 释放门后 io 域跑完中止完成项：全部会话析构，且没有一个是「仍登记即析构」。
    const bool all_destroyed = WaitUntil(
        [&] {
            return dbg::HttpSessionDestroyed() - destroyed_before_close ==
                   established;
        },
        8000);
    std::fprintf(stderr,
                 "case 1: destroyed_total=%d (expect +%d) "
                 "destroyed_while_registered_delta=%d\n",
                 dbg::HttpSessionDestroyed() - destroyed_before_close,
                 established,
                 dbg::HttpSessionDestroyedWhileRegistered() - tripwire_base);
    CHECK(all_destroyed);
    CHECK(dbg::HttpSessionDestroyedWhileRegistered() - tripwire_base == 0);
    CHECK(destroyed_base + established <= dbg::HttpSessionDestroyed());
#else
    // Release 面：无计数。等 io 域跑完中止完成项后断言生产状态归零。
    const bool drained =
        WaitUntil([&] { return impl->DebugSessionCount() == 0; }, 8000);
    CHECK(drained);
#endif
    CHECK(impl->DebugSessionCount() == 0);

    for (int fd : fds)
        ::close(fd);
    runtime->Close();
}

// ---------------------------------------------------------------- Case 2a

// 空闲 keep-alive 会话 + 另一线程并发 owner Close()（客户端随后 FIN）。
void RunCaseConcurrentIdle() {
    constexpr int kConns = 8;
    constexpr int kReqs  = 2;   // >=2：强制 deadline timer cancel→re-arm
    std::fprintf(stderr,
                 "case 2a: idle keep-alive sessions vs concurrent Close()\n");

#ifdef BBT_INFRA_STRINGENT_DEBUG
    const int destroyed_base = dbg::HttpSessionDestroyed();
    const int tripwire_base  = dbg::HttpSessionDestroyedWhileRegistered();
#endif

    auto rt = NetworkRuntime::Create(ProbeLimits());
    CHECK(rt);
    if (!rt)
        return;
    auto runtime = std::move(rt).value();
    CHECK(runtime->Start());

    auto srv = runtime->ListenHttp({"127.0.0.1", 0}, EchoHandler);
    CHECK(srv);
    if (!srv) {
        runtime->Close();
        return;
    }
    auto server = std::move(srv).value();
    auto impl   = std::dynamic_pointer_cast<http_detail::HttpServerImpl>(server);
    CHECK(impl != nullptr);
    if (!impl) {
        runtime->Close();
        return;
    }
    const auto port = server->LocalAddress().port;

    std::atomic_bool go{false};
    std::atomic_bool fin_go{false};   // 客户端保持连接，直到并发 Close 期间才 FIN
    std::vector<int> fds(kConns, -1);
    std::vector<std::thread> clients;
    for (int i = 0; i < kConns; ++i) {
        clients.emplace_back([&, i] {
            while (!go.load())
                std::this_thread::yield();
            const int fd = ConnectTo(port);
            if (fd < 0)
                return;
            fds[static_cast<std::size_t>(i)] = fd;
            for (int r = 0; r < kReqs; ++r) {
                if (!SendKeepAlive(fd, "/ka") || !ReadResponse(fd))
                    break;
            }
            // 保持连接空闲，等测试线程发起并发 Close 后再 FIN。
            while (!fin_go.load(std::memory_order_acquire))
                std::this_thread::yield();
            if (i % 2 == 0)
                ::close(fd);
            else {
                ::shutdown(fd, SHUT_WR);
                ::close(fd);
            }
            fds[static_cast<std::size_t>(i)] = -1;
        });
    }
    go.store(true);
    // 全部会话登记并完成 >=2 次请求（timer 已重挂），仍保持连接。
    const bool all_registered =
        WaitUntil([&] { return impl->DebugSessionCount() == kConns; }, 10000);
    CHECK(all_registered);

    int  registered_at_close = -1;
    bool closed_at_close     = false;
    std::thread closer([&] {
        server->Close();
        registered_at_close = static_cast<int>(impl->DebugSessionCount());
        closed_at_close     = server->IsClosed();
    });
    // 客户端 FIN / 半关闭与 Close 的 teardown 并发。
    fin_go.store(true, std::memory_order_release);
    closer.join();
    for (auto& t : clients)
        t.join();

    std::fprintf(stderr,
                 "case 2a: registered_at_close=%d closed=%d\n",
                 registered_at_close, closed_at_close ? 1 : 0);
    // Close 返回当刻 m_sessions 必须已空（teardown 在 drain 等待之前完成中止）。
    CHECK(registered_at_close == 0);
    CHECK(closed_at_close);
    const bool drained =
        WaitUntil([&] { return impl->DebugSessionCount() == 0; }, 5000);
    CHECK(drained);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    const bool all_destroyed = WaitUntil(
        [&] {
            return dbg::HttpSessionDestroyed() - destroyed_base == kConns;
        },
        8000);
    std::fprintf(stderr,
                 "case 2a: destroyed_delta=%d (expect %d) "
                 "destroyed_while_registered_delta=%d\n",
                 dbg::HttpSessionDestroyed() - destroyed_base, kConns,
                 dbg::HttpSessionDestroyedWhileRegistered() - tripwire_base);
    CHECK(all_destroyed);
    CHECK(dbg::HttpSessionDestroyedWhileRegistered() - tripwire_base == 0);
#endif

    const int refused = ConnectTo(port);
    CHECK(refused < 0);
    if (refused >= 0)
        ::close(refused);
    for (int fd : fds)
        if (fd >= 0)
            ::close(fd);
    runtime->Close();
}

// ---------------------------------------------------------------- Case 2b

// 在途 handler（受控挂起）+ 空闲会话，与另一线程 Close() 并发。
void RunCaseConcurrentInflight() {
    constexpr int kGated  = 2;
    constexpr int kIdle   = 2;
    std::fprintf(stderr,
                 "case 2b: in-flight handlers + idle sessions vs Close()\n");

#ifdef BBT_INFRA_STRINGENT_DEBUG
    const int destroyed_base = dbg::HttpSessionDestroyed();
    const int tripwire_base  = dbg::HttpSessionDestroyedWhileRegistered();
#endif
    ResetGate();

    auto rt = NetworkRuntime::Create(ProbeLimits());
    CHECK(rt);
    if (!rt)
        return;
    auto runtime = std::move(rt).value();
    CHECK(runtime->Start());

    auto srv = runtime->ListenHttp({"127.0.0.1", 0}, MixedHandler);
    CHECK(srv);
    if (!srv) {
        runtime->Close();
        return;
    }
    auto server = std::move(srv).value();
    auto impl   = std::dynamic_pointer_cast<http_detail::HttpServerImpl>(server);
    CHECK(impl != nullptr);
    if (!impl) {
        runtime->Close();
        return;
    }
    const auto port = server->LocalAddress().port;

    std::vector<int> fds;
    // 空闲会话：完成 1 次请求后保持连接（deadline 看守 + 读就绪等待 armed）。
    for (int i = 0; i < kIdle; ++i) {
        const int fd = ConnectTo(port);
        CHECK(fd >= 0);
        if (fd < 0)
            break;
        fds.push_back(fd);
        CHECK(SendKeepAlive(fd, "/idle"));
        CHECK(ReadResponse(fd));
    }
    // 在途会话：末次请求被 gated handler 挂起。
    for (int i = 0; i < kGated; ++i) {
        const int fd = ConnectTo(port);
        CHECK(fd >= 0);
        if (fd < 0)
            break;
        fds.push_back(fd);
        CHECK(SendKeepAlive(fd, "/inflight"));
    }
    const int total = static_cast<int>(fds.size());
    CHECK(total == kIdle + kGated);
    CHECK(WaitUntil([&] { return impl->DebugSessionCount() == static_cast<std::size_t>(total); }, 8000));
    CHECK(WaitEntered(10000));

    int  registered_at_close = -1;
    bool closed_at_close     = false;
    std::thread closer([&] {
        server->Close();
        registered_at_close = static_cast<int>(impl->DebugSessionCount());
        closed_at_close     = server->IsClosed();
    });
    // 握手：Close 已线性化封口后释放 gated handler，使 drain 与在途 handler
    // 真实重叠，而不是先后串行。
    CHECK(WaitUntil([&] { return !impl->OpenForIo(); }, 5000));
    ReleaseHandler();
    closer.join();

    std::fprintf(stderr,
                 "case 2b: registered_at_close=%d closed=%d calls=%d exits=%d\n",
                 registered_at_close, closed_at_close ? 1 : 0,
                 g_gate.calls.load(), g_gate.exits.load());
    CHECK(registered_at_close == 0);
    CHECK(closed_at_close);
    CHECK(WaitUntil([&] { return g_gate.exits.load() >= kGated; }, 8000));
#ifdef BBT_INFRA_STRINGENT_DEBUG
    CHECK(dbg::HttpSessionDestroyedWhileRegistered() - tripwire_base == 0);
    const bool all_destroyed = WaitUntil(
        [&] {
            return dbg::HttpSessionDestroyed() - destroyed_base == total;
        },
        8000);
    std::fprintf(stderr,
                 "case 2b: destroyed_delta=%d (expect %d) "
                 "destroyed_while_registered_delta=%d\n",
                 dbg::HttpSessionDestroyed() - destroyed_base, total,
                 dbg::HttpSessionDestroyedWhileRegistered() - tripwire_base);
    CHECK(all_destroyed);
#else
    // Release 面：无计数。等全部会话反登记归零（生产状态）。
    CHECK(WaitUntil([&] { return impl->DebugSessionCount() == 0; }, 8000));
#endif
    CHECK(impl->DebugSessionCount() == 0);

    const int refused = ConnectTo(port);
    CHECK(refused < 0);
    if (refused >= 0)
        ::close(refused);
    for (int fd : fds)
        ::close(fd);
    runtime->Close();
}

} // namespace

int main() {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    // 4 个调度线程：Case 2b 的 gated handler 会阻塞调度线程等待释放，其余线程
    // 继续推进 io 域（完成项 / 回投），避免自持死锁。
    cfg->m_cfg_static_thread_num = 4;
    // 与既有 HTTP 套件一致：handler 链路较深，上游默认纤程栈不够。
    cfg->m_cfg_stack_size = 1024 * 256;
    auto& sche = bbt::coroutine::detail::Scheduler::GetInstance();
    sche->Start(bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);
    CHECK(sche->IsInitialized());

#ifdef BBT_INFRA_STRINGENT_DEBUG
    std::fprintf(stderr, "lifetime-probe: debug surface (counters enabled)\n");
#else
    std::fprintf(stderr,
                 "lifetime-probe: release surface (no debug counters)\n");
#endif

    RunCaseOrdering();
    RunCaseConcurrentIdle();
    RunCaseConcurrentInflight();

    std::fprintf(stderr, "lifetime-probe: checks=%d failures=%d\n", g_checks,
                 g_failures);
    return g_failures == 0 ? 0 : 1;
}
