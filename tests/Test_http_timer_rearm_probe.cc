// Issue #62 回归探针：keep-alive deadline timer cancel/re-arm 的记账不变式。
//
// 断言目标（每个 outstanding timer completion 恰好递减一次 inflight）：
//   N 条并发入站连接，每条在【同一条连接】上顺序发 >=2 条 keep-alive 请求。
//   第 2 条请求的回复写完后，PumpWrite 会在同一个 io handler 内同步调用
//   BeginRead()，此时上一条 deadline timer 的 cancel 产生的 aborted
//   completion 尚未派发、而新 timer 已 arm——这是「旧完成项 / 新 watchdog
//   共用状态」的确定性重叠窗口。随后对端主动 FIN（一半先 shutdown(WR)，
//   一半直接 close），再调用 owner Close()。
//
// 修复前：旧 aborted completion 消费新 timer 的记账，inflight 永久多计，
//   已关闭会话无法反登记 -> DebugSessionCount() 永不为 0，Close() 被
//   kCloseDrainTimeout(5s) 卡满。修复后：注册会话数归 0，Close() 快速返回。
//
// sanitizer 覆盖：本目标以 ASan/UBSan 构建运行时，取消/重挂期间若出现
//   悬垂访问由 sanitizer 直接报出（扫描 stderr 日志，不只看退出码）。
//
// 无第三方测试框架：失败打印 stderr 并以非零退出；等待全部有界（无 sleep
// 驱动时序，只用有预算的轮询），不会无限挂起。

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
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

// 探针：直接读取已注册会话数（impl 层只读接口），src/ 内部头仅本目标可见。
#include "http/HttpServerImpl.hpp"

using namespace bbt::infra;

namespace {

int g_checks   = 0;
int g_failures = 0;

void Check(bool cond, const char* expr, int line) {
    ++g_checks;
    if (!cond) {
        ++g_failures;
        std::fprintf(stderr, "FAIL probe line %d: %s\n", line, expr);
    }
}
#define CHECK(cond) Check(static_cast<bool>(cond), #cond, __LINE__)

constexpr int kConns       = 12;   // 并发入站连接数
constexpr int kReqsPerConn = 3;    // 同连接顺序请求数（>=2 即强制 timer 重挂）

std::atomic_int g_handler_calls{0};

result<HttpResponse> EchoHandler(IncomingCallContext, HttpRequest) {
    g_handler_calls.fetch_add(1);
    return result<HttpResponse>::ok(HttpResponse{200, {}, "ok"});
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

} // namespace

int main() {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 2;
    // 与既有 HTTP 套件一致：handler 链路较深，上游默认纤程栈不够。
    cfg->m_cfg_stack_size = 1024 * 256;
    auto& sche = bbt::coroutine::detail::Scheduler::GetInstance();
    sche->Start(bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);
    CHECK(sche->IsInitialized());

    NetworkLimits limits{};
    limits.max_connections  = 64;
    limits.max_inflight     = 64;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = 64 * 1024;
    // 期限预算给足：本探针只针对 cancel/re-arm 记账，不依赖期限触发。
    limits.incoming_timeout = std::chrono::milliseconds(30000);

    auto rt = NetworkRuntime::Create(limits);
    CHECK(rt);
    auto runtime = std::move(rt).value();
    CHECK(runtime->Start());

    auto srv = runtime->ListenHttp({"127.0.0.1", 0}, EchoHandler);
    CHECK(srv);
    auto server = std::move(srv).value();
    const auto port = server->LocalAddress().port;
    CHECK(port != 0);

    auto impl = std::dynamic_pointer_cast<http_detail::HttpServerImpl>(server);
    CHECK(impl != nullptr);
    if (!impl)
        return 1;

    std::atomic_bool go{false};
    std::atomic_int  served{0};
    std::vector<std::thread> clients;
    for (int i = 0; i < kConns; ++i) {
        clients.emplace_back([&, i] {
            while (!go.load())
                std::this_thread::yield();
            const int fd = ConnectTo(port);
            if (fd < 0)
                return;
            bool ok = true;
            for (int r = 0; r < kReqsPerConn && ok; ++r) {
                static const char kReq[] =
                    "GET /ok HTTP/1.1\r\nHost: probe\r\n"
                    "Connection: keep-alive\r\n\r\n";
                const ssize_t sent = ::send(fd, kReq, sizeof(kReq) - 1,
                                            MSG_NOSIGNAL);
                ok = sent == static_cast<ssize_t>(sizeof(kReq) - 1) &&
                     ReadResponse(fd);
            }
            if (ok)
                served.fetch_add(1);
            if (i % 2 == 0) {
                ::close(fd);            // 直接 FIN
            } else {
                ::shutdown(fd, SHUT_WR); // 先半关让服务端看到 FIN
                ::close(fd);
            }
        });
    }
    go.store(true);

    // 所有连接都必须完成 >=2 次请求（每次请求都触发一轮 timer cancel→re-arm）。
    CHECK(WaitUntil([&] { return served.load() >= kConns; }, 30000));
    for (auto& t : clients)
        t.join();

    // 关键断言：所有已关闭会话必须反登记。修复前 inflight 泄漏使计数永不为 0。
    const bool drained =
        WaitUntil([&] { return impl->DebugSessionCount() == 0; }, 5000);
    std::fprintf(stderr,
                 "probe: handler_calls=%d registered_sessions=%zu drained=%d\n",
                 g_handler_calls.load(), impl->DebugSessionCount(),
                 drained ? 1 : 0);
    CHECK(drained);
    CHECK(g_handler_calls.load() >= kConns * kReqsPerConn);

    // Close() 必须同步返回且不被 drain 超时卡住（kCloseDrainTimeout = 5s）。
    const auto t0 = std::chrono::steady_clock::now();
    server->Close();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - t0)
                        .count();
    std::fprintf(stderr, "probe: Close() took %lld ms\n",
                 static_cast<long long>(ms));
    CHECK(ms < 2500);
    CHECK(server->IsClosed());

    // listener 物理释放：新连接必须被拒绝。
    const int refused = ConnectTo(port);
    CHECK(refused < 0);
    if (refused >= 0)
        ::close(refused);

    runtime->Close();
    std::fprintf(stderr, "probe: checks=%d failures=%d\n", g_checks,
                 g_failures);
    return g_failures == 0 ? 0 : 1;
}
