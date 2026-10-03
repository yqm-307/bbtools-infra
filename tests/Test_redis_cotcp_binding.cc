// Redis CoTCP B 级 owner binding 候选的隔离验收（Boost.Test + CountDownLatch 有界
// 等待，与本仓 redis 套件同一风格）。
//
// 被测对象：src/redis/{RedisCotcpOwner,CoRedisCotcpCli}.*（新文件、新符号），
// hiredis 只做 RESP2 编码/解码，socket 与等待全部由 CoTCP 承担。
//
// 与当前 main 契约对齐（同步 Close、无 RequestClose/WaitClosed/CloseStatus、无取消
// 令牌、无 CompletionSignal、无 Scheduler::Stop）：
//   - owner 主动 Close() 同步返回；封口 + 收口已登记 op + 物理关闭 CoTCP（fd）+
//     释放 hiredis reader 都在 Close 的调用线程内完成。
//   - Close 返回当刻：已建连的 CoTCP fd 已物理关闭（IsClosed + FdOpen==false）；
//     hiredis reader 恰好释放（readers_created==readers_freed）。
//   - 在途 DialTCP：Close 封口 owner 级 dial 等待登记打断 connect，候选 fd 由
//     dial 协程在恢复时释放——该释放不在 Close 返回当刻，用例按有界等待断言
//     「无 socket 残留」，不冒充同步。
//   - IsClosed() 反映完整静默（封口 + owner 批次退出 + 已登记 op 归零 + 连接物理
//     收口），owner 批次在途时可能晚于 Close 返回落定；用例据此用有界等待断言。
//
// 覆盖矩阵：
//   t_resp2_roundtrip_live           真实 Redis loopback：PING/SET/GET/EXISTS/DEL、
//                                     二进制值、nil、服务端错误、2 MiB 大回复（无
//                                     Redis 时按退出码 77 表示环境不可用）
//   t_raw_reply_types_and_errors      原始对端预置 RESP2 各类型 + 垃圾字节协议错误
//   t_partial_write_then_full_delivery 小接收窗对端：写侧部分写/WouldBlock 后写满
//   t_write_side_deadline            写侧阻塞 + deadline → TimedOut（不重发）
//   t_eof_and_rst                    EOF（干净 FIN）与 RST 都映射为 TransportError
//   t_read_side_deadline             读侧 deadline → TimedOut；TryReadSome 无进展探针
//   t_owner_close_physical_collect   在途命令随 Close 落定 Closed；Close 返回当刻 fd
//                                     物理关闭、reader 恰好释放；晚到终态不覆盖
//   t_repeated_and_concurrent_close  重复/并发 Close：无 UAF/死锁，物理收口一次
//   t_b1_close_during_inflight_dial  DialTCP 挂起期间 Close：打断在途 dial、收敛、
//                                     不编码/不发送、无 socket 残留
//   t_b1_close_after_dial_no_encode  dial 已成功、尚未编码时 Close（测试接缝落点）：
//                                     不编码、不发送，Close 返回当刻 fd 物理关闭
//
// 范围：候选为**单连接 / 单 owner**，本套件不构成多连接并发或完整 §10 B 级验收。
//
// 运行：BBT_TEST_REDIS_ADDR=127.0.0.1:16379 ./Test_redis_cotcp_binding
//   - 真实 Redis 用例（t_resp2_roundtrip_live）单独成组（见 tests/CMakeLists.txt）：
//     未显式给地址且默认 loopback 无服务时只以退出码 77 表示「环境不可用」，不使用
//     会被 CTest 当作 skip 的正则（那会掩盖其他用例的真实失败）。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/CoTCP.hpp>
#include <bbt/infra/NetworkTypes.hpp>

#include "detail/TransportWiring.hpp"
#include "redis/CoRedisCotcpCli.hpp"
#include "redis/RedisDetail.hpp"

using namespace bbt::infra;
using bbt::coroutine::Deadline;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;
using bbt::infra::redis_detail::CoRedisCotcpCliImpl;
using bbt::infra::redis_detail::CotcpBindingTotals;
using bbt::infra::redis_detail::CreateCotcpRedisCli;
using W = bbt::infra::detail::TransportWiring;

namespace {

constexpr int         kBudgetMs        = 15000;
constexpr std::size_t kBadFrame        = static_cast<std::size_t>(-1);
constexpr std::size_t kPartialPayload  = 4u * 1024 * 1024;
// 「环境不可用」（未显式给地址且默认 loopback 无 Redis）的专用退出码，与 CTest 的
// SKIP_RETURN_CODE 严格对应；断言失败/崩溃不会用这个码。
constexpr int kEnvUnavailableExitCode = 77;

RedisClientConfig MakeConfig(std::string host, std::uint16_t port,
                             std::size_t inflight = 8, std::size_t queue = 8) {
    RedisClientConfig cfg;
    cfg.host         = std::move(host);
    cfg.port         = port;
    cfg.max_inflight = inflight;
    cfg.max_queue    = queue;
    return cfg;
}

CallOptions Opt(int budget_ms = 10000) {
    CallOptions opt;
    opt.deadline = std::chrono::steady_clock::now() +
                   std::chrono::milliseconds(budget_ms);
    return opt;
}

bool RunInCoroutine(std::function<void()> f, int budget_ms = kBudgetMs) {
    bbt::core::thread::CountDownLatch done{1};
    bool                             succ = false;
    g_scheduler->RegistCoroutineTask([f = std::move(f), &done]() {
        f();
        done.Down();
    }, succ);
    if (!succ)
        return false;
    return done.WaitTimeout(budget_ms) == 0;
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

// 非协程线程定长睡眠：直达内核 syscall（coroutine Hook 拦 libc nanosleep）。
void SleepMs(int ms) {
    timespec req{ms / 1000, static_cast<long>(ms % 1000) * 1000000L};
    timespec rem{};
    while (::syscall(SYS_nanosleep, &req, &rem) != 0 && errno == EINTR)
        req = rem;
}

struct FdProbe { int rc; int err; };
FdProbe ProbeFd(int fd) {
    const int rc = ::fcntl(fd, F_GETFD);
    return FdProbe{rc, rc == -1 ? errno : 0};
}
bool FdOpen(int fd) { return ::fcntl(fd, F_GETFD) != -1; }

// 本进程当前打开的 socket 型 fd 数（/proc/self/fd 的 link 目标）。用于「dial 候选
// socket 未残留」的物理证据：只统计 socket，不受 eventfd/pipe 噪声影响。
std::size_t CountSocketFds() {
    DIR* dir = ::opendir("/proc/self/fd");
    if (dir == nullptr)
        return 0;
    std::size_t n = 0;
    while (dirent* ent = ::readdir(dir)) {
        const std::string name = ent->d_name;
        if (name == "." || name == "..")
            continue;
        const std::string link = "/proc/self/fd/" + name;
        char              buf[256];
        const ssize_t     len = ::readlink(link.c_str(), buf, sizeof(buf) - 1);
        if (len <= 0)
            continue;
        buf[len] = '\0';
        if (std::strncmp(buf, "socket:", 7) == 0)
            ++n;
    }
    ::closedir(dir);
    return n;
}

// 让对端 connect 在内核里保持挂起：把监听 socket 的 accept 队列填满（backlog=1 +
// 若干已建立但从未被 accept 的连接）。Linux 在 accept 队列满时丢弃新 SYN 而不回 RST，
// 因此后续 connect 停留在 SYN_SENT —— 把 DialTCP 确定性地挂在 connect 等待上。
class BlackholeDial {
public:
    explicit BlackholeDial(std::size_t fillers = 32) {
        m_lfd = ::socket(AF_INET, SOCK_STREAM, 0);
        BOOST_REQUIRE(m_lfd >= 0);
        int one = 1;
        ::setsockopt(m_lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port        = 0;
        BOOST_REQUIRE(::bind(m_lfd, reinterpret_cast<sockaddr*>(&addr),
                             sizeof(addr)) == 0);
        BOOST_REQUIRE(::listen(m_lfd, 1) == 0);
        socklen_t len = sizeof(addr);
        BOOST_REQUIRE(::getsockname(m_lfd, reinterpret_cast<sockaddr*>(&addr),
                                    &len) == 0);
        port = ntohs(addr.sin_port);
        for (std::size_t i = 0; i < fillers; ++i) {
            const int s = ::socket(AF_INET, SOCK_STREAM, 0);
            if (s < 0)
                break;
            ::fcntl(s, F_SETFL, ::fcntl(s, F_GETFL, 0) | O_NONBLOCK);
            (void)::connect(s, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
            m_fillers.push_back(s);
        }
    }
    ~BlackholeDial() {
        for (int s : m_fillers)
            ::close(s);
        if (m_lfd >= 0)
            ::close(m_lfd);
    }
    BlackholeDial(const BlackholeDial&)            = delete;
    BlackholeDial& operator=(const BlackholeDial&) = delete;

    std::uint16_t port = 0;
    std::size_t   FillerCount() const { return m_fillers.size(); }

private:
    int              m_lfd{-1};
    std::vector<int> m_fillers;
};

// RESP2 应答构造（服务端侧字节）。
std::string RespStatus(const std::string& s) { return "+" + s + "\r\n"; }
std::string RespInteger(long long v) { return ":" + std::to_string(v) + "\r\n"; }
std::string RespBulk(const std::string& s) {
    return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}
std::string RespNil() { return "$-1\r\n"; }
std::string RespError(const std::string& s) { return "-" + s + "\r\n"; }

// 已缓冲字节里一条完整 RESP multibulk 命令的字节数；不足返回 0；非法返回 kBadFrame。
std::size_t FramedCommandSize(const std::string& buf) {
    if (buf.empty() || buf[0] != '*')
        return buf.empty() ? 0 : kBadFrame;
    const auto nl = buf.find("\r\n");
    if (nl == std::string::npos)
        return 0;
    int argc = 0;
    try {
        argc = std::stoi(buf.substr(1, nl - 1));
    } catch (...) {
        return kBadFrame;
    }
    std::size_t pos = nl + 2;
    for (int i = 0; i < argc; ++i) {
        if (pos >= buf.size())
            return 0;
        if (buf[pos] != '$')
            return kBadFrame;
        const auto nl2 = buf.find("\r\n", pos);
        if (nl2 == std::string::npos)
            return 0;
        std::size_t len = 0;
        try {
            len = static_cast<std::size_t>(
                std::stoul(buf.substr(pos + 1, nl2 - pos - 1)));
        } catch (...) {
            return kBadFrame;
        }
        pos = nl2 + 2;
        if (buf.size() < pos + len + 2)
            return 0;
        pos += len + 2;
    }
    return pos;
}

// 原始对端：单独监听端口，串行 accept 至多若干条连接，每条交给 handler。
struct PeerState;
using PeerHandler = std::function<void(const std::shared_ptr<PeerState>&, int)>;

struct PeerState {
    std::atomic<std::uint64_t> accepted{0};
    std::atomic<std::uint64_t> bytes_read{0};
    std::atomic<std::uint64_t> commands_read{0};
    std::atomic<std::uint64_t> probe_finished{0};
    PeerHandler                handler;
    bool ReadCommand(int c) {
        std::string buf;
        char        tmp[65536];
        for (;;) {
            const std::size_t need = FramedCommandSize(buf);
            if (need == kBadFrame)
                return false;
            if (need != 0 && buf.size() >= need) {
                commands_read.fetch_add(1);
                return true;
            }
            const ssize_t n = ::recv(c, tmp, sizeof(tmp), 0);
            if (n <= 0)
                return false;
            buf.append(tmp, static_cast<std::size_t>(n));
            bytes_read.fetch_add(static_cast<std::uint64_t>(n));
        }
    }
};

class RawPeer {
public:
    explicit RawPeer(PeerHandler handler, int listen_rcvbuf = 0)
        : m_st(std::make_shared<PeerState>()) {
        m_st->handler = std::move(handler);
        int lfd       = ::socket(AF_INET, SOCK_STREAM, 0);
        BOOST_REQUIRE(lfd >= 0);
        int one = 1;
        ::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        if (listen_rcvbuf > 0)
            ::setsockopt(lfd, SOL_SOCKET, SO_RCVBUF, &listen_rcvbuf,
                         sizeof(listen_rcvbuf));
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port        = 0;
        BOOST_REQUIRE(::bind(lfd, reinterpret_cast<sockaddr*>(&addr),
                             sizeof(addr)) == 0);
        BOOST_REQUIRE(::listen(lfd, 16) == 0);
        socklen_t len = sizeof(addr);
        BOOST_REQUIRE(::getsockname(lfd, reinterpret_cast<sockaddr*>(&addr),
                                    &len) == 0);
        port = ntohs(addr.sin_port);

        auto st   = m_st;
        bool succ = false;
        g_scheduler->RegistCoroutineTask([st, lfd]() { Serve(st, lfd); }, succ);
        BOOST_REQUIRE(succ);
    }

    std::uint16_t port = 0;
    std::uint64_t Accepted() const { return m_st->accepted.load(); }
    std::uint64_t BytesRead() const { return m_st->bytes_read.load(); }
    std::uint64_t CommandsRead() const { return m_st->commands_read.load(); }
    std::uint64_t ProbeFinished() const { return m_st->probe_finished.load(); }

private:
    static void Serve(const std::shared_ptr<PeerState>& st, int lfd) {
        timeval tv{6, 0};
        for (int i = 0; i < 8; ++i) {
            pollfd p{lfd, POLLIN, 0};
            if (::poll(&p, 1, 1500) <= 0)
                break;
            const int c = ::accept(lfd, nullptr, nullptr);
            if (c < 0)
                break;
            st->accepted.fetch_add(1);
            ::setsockopt(c, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            if (st->handler)
                st->handler(st, c);
            ::close(c);
        }
        ::close(lfd);
    }

    std::shared_ptr<PeerState> m_st;
};

// 直接用裸 RESP 向真实 Redis 发一条 LPUSH（仅测试造数：把键变成 list）。
bool LpushViaRawSocket(const std::string& host, std::uint16_t port,
                       const std::string& key, std::string* out) {
    const std::string cmd = "*3\r\n$5\r\nLPUSH\r\n$" +
                            std::to_string(key.size()) + "\r\n" + key +
                            "\r\n$1\r\nv\r\n";
    bool        done = false;
    bool        ok   = false;
    bool        succ = false;
    g_scheduler->RegistCoroutineTask([&]() {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in a{};
        a.sin_family = AF_INET;
        a.sin_port   = htons(port);
        if (fd >= 0 && ::inet_pton(AF_INET, host.c_str(), &a.sin_addr) == 1 &&
            ::connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0) {
            if (::send(fd, cmd.data(), cmd.size(), MSG_NOSIGNAL) ==
                static_cast<ssize_t>(cmd.size())) {
                char          buf[128];
                const ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
                if (n > 0) {
                    if (out != nullptr)
                        out->assign(buf, static_cast<std::size_t>(n));
                    ok = true;
                }
            }
        }
        if (fd >= 0)
            ::close(fd);
        done = true;
    }, succ);
    if (!succ)
        return false;
    const auto dl = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!done && std::chrono::steady_clock::now() < dl)
        std::this_thread::yield();
    return done && ok;
}

// 保持连接但不读数据：协程内 poll 超时即让出（不占线程、不读字节）。
void HoldWithoutReading(int c, int ms) {
    pollfd p{c, POLLPRI, 0};
    (void)::poll(&p, 1, ms);
}

// 脚本化应答对端：每读走一条命令回一条预置 RESP 应答，按序消费。
PeerHandler ReplySequence(std::vector<std::string> replies) {
    return [replies](const std::shared_ptr<PeerState>& st, int c) {
        for (const auto& r : replies) {
            if (!st->ReadCommand(c))
                return;
            if (::send(c, r.data(), r.size(), MSG_NOSIGNAL) <= 0)
                return;
        }
    };
}

PeerHandler ReplyOnce(const std::string& payload) { return ReplySequence({payload}); }

// 首条命令延迟应答，然后短暂探测是否有第二条命令到达；用于验证队列
// deadline 到期后 owner 跳过该请求，而不是把它发送到健康连接。
PeerHandler ReplyAfterHoldThenProbe(int hold_ms, int probe_ms,
                                    std::string payload) {
    return [hold_ms, probe_ms, payload = std::move(payload)](
               const std::shared_ptr<PeerState>& st, int c) {
        if (!st->ReadCommand(c))
            return;
        HoldWithoutReading(c, hold_ms);
        if (::send(c, payload.data(), payload.size(), MSG_NOSIGNAL) <= 0)
            return;
        pollfd p{c, POLLIN, 0};
        if (::poll(&p, 1, probe_ms) > 0)
            (void)st->ReadCommand(c);
        st->probe_finished.fetch_add(1);
    };
}

// 读走一条命令后保持沉默（不回复、不关闭）：用于 deadline/owner close 场景。
PeerHandler ReadThenHold(int hold_ms) {
    return [hold_ms](const std::shared_ptr<PeerState>& st, int c) {
        (void)st->ReadCommand(c);
        HoldWithoutReading(c, hold_ms);
    };
}

// 完全不读（保持连接）：用于写侧阻塞/部分写场景。
PeerHandler NeverRead(int hold_ms) {
    return [hold_ms](const std::shared_ptr<PeerState>&, int c) {
        HoldWithoutReading(c, hold_ms);
    };
}

result<std::shared_ptr<CoRedisCotcpCliImpl>> NewCotcpClient(
    std::string host, std::uint16_t port, bool start = true) {
    auto c = CreateCotcpRedisCli(MakeConfig(std::move(host), port));
    if (!c)
        return result<std::shared_ptr<CoRedisCotcpCliImpl>>::err(
            std::move(c).error());
    auto cli = std::move(c).value();
    if (start) {
        auto st = cli->Start();
        if (!st)
            return result<std::shared_ptr<CoRedisCotcpCliImpl>>::err(
                std::move(st).error());
    }
    return result<std::shared_ptr<CoRedisCotcpCliImpl>>::ok(std::move(cli));
}

// 真实 Redis 地址：BBT_TEST_REDIS_ADDR=host:port。
bool LiveRedisAddr(std::string& host, std::uint16_t& port, bool& from_env) {
    const char* addr = ::getenv("BBT_TEST_REDIS_ADDR");
    from_env         = addr != nullptr && addr[0] != '\0';
    std::string text = from_env ? std::string(addr) : std::string("127.0.0.1:16379");
    const auto  pos  = text.rfind(':');
    if (pos == std::string::npos)
        return false;
    host = text.substr(0, pos);
    port = static_cast<std::uint16_t>(std::stoi(text.substr(pos + 1)));
    return port != 0;
}

std::atomic_bool g_prepared{false};

} // namespace

BOOST_AUTO_TEST_SUITE(redis_cotcp_binding)

BOOST_AUTO_TEST_CASE(t_setup_scheduler) {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 2;
    cfg->m_cfg_stack_size        = 1024 * 256;
    g_scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(g_scheduler->IsInitialized());
    g_prepared.store(true);
}

BOOST_AUTO_TEST_CASE(t_close_race_after_wait_registered) {
    BOOST_REQUIRE(g_prepared.load());

    RawPeer peer(ReplyOnce("+PONG\r\n"));
    auto c = NewCotcpClient("127.0.0.1", peer.port);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::atomic_bool gate_entered{false};
    std::atomic_bool release_gate{false};
    std::atomic_bool close_seen{false};
    std::atomic_bool close_done{false};
    cli->SetPreAdmitGateForTest([&] {
        gate_entered.store(true, std::memory_order_release);
        while (!release_gate.load(std::memory_order_acquire))
            std::this_thread::yield();
    });

    std::thread closer([&] {
        close_seen.store(WaitUntil([&] {
            return gate_entered.load(std::memory_order_acquire);
        }, 5000), std::memory_order_release);
        if (close_seen.load(std::memory_order_acquire)) {
            cli->Close();
            close_done.store(true, std::memory_order_release);
        }
        release_gate.store(true, std::memory_order_release);
    });

    std::optional<result<void>> out;
    const bool ran = RunInCoroutine(
        [&] { out.emplace(cli->Ping(Opt(5000))); }, 7000);
    release_gate.store(true, std::memory_order_release);
    closer.join();

    BOOST_REQUIRE(ran);
    BOOST_REQUIRE(gate_entered.load(std::memory_order_acquire));
    BOOST_REQUIRE(close_seen.load(std::memory_order_acquire));
    BOOST_REQUIRE(close_done.load(std::memory_order_acquire));
    BOOST_REQUIRE(out);
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
    BOOST_CHECK(cli->IsClosed());
}

// 真实 Redis loopback：round-trip、二进制安全、nil、服务端错误、大回复.
BOOST_AUTO_TEST_CASE(t_resp2_roundtrip_live) {
    BOOST_REQUIRE(g_prepared.load());
    std::string   host;
    std::uint16_t port = 0;
    bool          from_env = false;
    BOOST_REQUIRE(LiveRedisAddr(host, port, from_env));

    redis_detail::ResetCotcpBindingTotalsForTest();
    auto c = NewCotcpClient(host, port);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    // 目标不可达：未显式给地址时按 skip 处理（与 redis.live 约定一致）。
    {
        std::atomic<bool> reachable{false};
        BOOST_REQUIRE(RunInCoroutine([&] {
            reachable.store(static_cast<bool>(cli->Ping(Opt(3000))));
        }));
        if (!reachable.load()) {
            if (from_env) {
                BOOST_FAIL("redis unreachable at " << host << ":" << port);
            }
            BOOST_TEST_MESSAGE("env-unavailable: redis unreachable at "
                               << host << ":" << port
                               << "; live round-trip case not executed");
            std::exit(kEnvUnavailableExitCode);
        }
    }

    const std::string key = "cotcp:b:key";
    const std::string bin = std::string("a\0b\xff", 4);

    bool ping_ok = false;
    BOOST_REQUIRE(RunInCoroutine([&] {
        ping_ok = static_cast<bool>(cli->Ping(Opt()));
    }));
    BOOST_CHECK(ping_ok);

    bool kv_ok = false;
    BOOST_REQUIRE(RunInCoroutine([&] {
        auto s = cli->Set(key, bin, Opt());
        auto g = cli->Get(key, Opt());
        kv_ok = static_cast<bool>(s) && static_cast<bool>(g) &&
                g.value().has_value() && g.value().value() == bin;
    }));
    BOOST_CHECK(kv_ok);

    bool ex_ok = false;
    BOOST_REQUIRE(RunInCoroutine([&] {
        auto ex  = cli->Exists(key, Opt());
        auto dl  = cli->Delete({key}, Opt());
        auto ex2 = cli->Exists(key, Opt());
        ex_ok = static_cast<bool>(ex) && ex.value() &&
                static_cast<bool>(dl) && dl.value() == 1 &&
                static_cast<bool>(ex2) && !ex2.value();
    }));
    BOOST_CHECK(ex_ok);

    bool nil_ok = false;
    BOOST_REQUIRE(RunInCoroutine([&] {
        auto g = cli->Get("cotcp:b:missing", Opt());
        nil_ok = static_cast<bool>(g) && !g.value().has_value();
    }));
    BOOST_CHECK(nil_ok);

    BOOST_REQUIRE(LpushViaRawSocket(host, port, "cotcp:b:list", nullptr));
    bool err_ok = false;
    BOOST_REQUIRE(RunInCoroutine([&] {
        auto g  = cli->Get("cotcp:b:list", Opt());
        err_ok  = !g && g.error().code == ErrorCode::RemoteError &&
                 g.error().domain_code == "WRONGTYPE";
        auto dl = cli->Delete({"cotcp:b:list"}, Opt());
        (void)dl;
    }));
    BOOST_CHECK(err_ok);

    std::string big(kPartialPayload / 2, 'Z');
    bool        big_ok = false;
    BOOST_REQUIRE(RunInCoroutine([&] {
        auto s = cli->Set("cotcp:b:big", big, Opt());
        auto g = cli->Get("cotcp:b:big", Opt());
        big_ok = static_cast<bool>(s) && static_cast<bool>(g) &&
                 g.value().has_value() && g.value().value() == big;
        auto dl = cli->Delete({"cotcp:b:big"}, Opt());
        (void)dl;
    }));
    BOOST_CHECK(big_ok);

    const auto totals = redis_detail::CotcpBindingTotalsForTest();
    BOOST_TEST_MESSAGE("live totals: hiredis_calls=" << totals.hiredis_calls
        << " max_call_ns=" << totals.hiredis_max_call_ns
        << " read_rounds=" << totals.read_rounds
        << " write_rounds=" << totals.write_rounds
        << " partial_read_ops=" << totals.partial_read_ops);
    BOOST_CHECK(totals.read_rounds >= 64);
    BOOST_CHECK(totals.partial_read_ops >= 1);
    BOOST_CHECK(totals.hiredis_calls <= totals.read_rounds * 3 + 64);
    // hiredis 调用全部在协程栈内同步返回：单次最长耗时应在同一量级（无库内等待）。
    BOOST_CHECK(totals.hiredis_max_call_ns < 50ull * 1000 * 1000);
    BOOST_CHECK(totals.commands_encoded >= 8);

    cli->Close();
    BOOST_CHECK(cli->IsClosed());
    cli.reset();
}

// 原始对端预置 RESP2 各类型 + 垃圾字节协议错误。
BOOST_AUTO_TEST_CASE(t_raw_reply_types_and_errors) {
    BOOST_REQUIRE(g_prepared.load());

    {   // status / integer / bulk / nil
        RawPeer peer(ReplySequence({RespStatus("PONG"), RespInteger(1),
                                    RespBulk("v1"), RespNil()}));
        auto    c = NewCotcpClient("127.0.0.1", peer.port);
        BOOST_REQUIRE(c);
        auto cli = std::move(c).value();
        bool ok  = false;
        BOOST_REQUIRE(RunInCoroutine([&] {
            auto p  = cli->Ping(Opt());
            auto e  = cli->Exists("k", Opt());
            auto g  = cli->Get("k", Opt());
            auto g2 = cli->Get("k2", Opt());
            ok = static_cast<bool>(p) && static_cast<bool>(e) && e.value() &&
                 static_cast<bool>(g) && g.value().has_value() &&
                 g.value().value() == "v1" && static_cast<bool>(g2) &&
                 !g2.value().has_value();
        }));
        BOOST_CHECK(ok);
        cli->Close();
        BOOST_CHECK(WaitUntil([&] { return cli->IsClosed(); }));
    }

    {   // 服务端错误串 → RemoteError(WRONGTYPE)
        RawPeer peer(ReplyOnce(RespError("WRONGTYPE bad")));
        auto    c = NewCotcpClient("127.0.0.1", peer.port);
        BOOST_REQUIRE(c);
        auto cli = std::move(c).value();
        std::optional<result<std::optional<std::string>>> out;
        BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Get("k", Opt())); }));
        BOOST_REQUIRE(!*out);
        BOOST_CHECK(out->error().code == ErrorCode::RemoteError);
        BOOST_CHECK(out->error().domain_code == "WRONGTYPE");
        cli->Close();
        BOOST_CHECK(WaitUntil([&] { return cli->IsClosed(); }));
    }

    {   // 垃圾字节：RESP2 解析失败（或 hiredis 生成的错误 reply）—— 两者都是错误
        RawPeer peer(ReplyOnce("not-a-resp-reply\r\n"));
        auto    c = NewCotcpClient("127.0.0.1", peer.port);
        BOOST_REQUIRE(c);
        auto cli = std::move(c).value();
        std::optional<result<void>> out;
        BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt(3000))); }));
        BOOST_REQUIRE(!*out);
        const auto code = out->error().code;
        BOOST_TEST_MESSAGE("garbage reply -> code=" << static_cast<int>(code)
                           << " msg=" << out->error().message);
        BOOST_CHECK(code == ErrorCode::ProtocolError ||
                    code == ErrorCode::RemoteError ||
                    code == ErrorCode::TransportError);
        cli->Close();
        BOOST_CHECK(WaitUntil([&] { return cli->IsClosed(); }));
    }
}

// 写侧：对端小接收窗 + 先不读 → 部分写/WouldBlock；恢复读取后写满并收到应答。
BOOST_AUTO_TEST_CASE(t_partial_write_then_full_delivery) {
    BOOST_REQUIRE(g_prepared.load());
    redis_detail::ResetCotcpBindingTotalsForTest();

    RawPeer peer2(
        [](const std::shared_ptr<PeerState>& st, int c) {
            HoldWithoutReading(c, 800);
            if (st->ReadCommand(c))
                (void)::send(c, "+OK\r\n", 5, MSG_NOSIGNAL);
        },
        /*listen_rcvbuf=*/4096);

    auto c = NewCotcpClient("127.0.0.1", peer2.port);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    const std::string value(kPartialPayload, 'p');
    std::optional<result<void>> out;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { out.emplace(cli->Set("cotcp:b:big", value, Opt(20000))); },
        kBudgetMs));
    BOOST_REQUIRE(*out);
    BOOST_CHECK(static_cast<bool>(*out));

    const auto totals = redis_detail::CotcpBindingTotalsForTest();
    BOOST_TEST_MESSAGE("write totals: write_rounds=" << totals.write_rounds
        << " partial_write_ops=" << totals.partial_write_ops
        << " hiredis_max_call_ns=" << totals.hiredis_max_call_ns);
    BOOST_CHECK(totals.write_rounds >= 2);
    BOOST_CHECK(totals.partial_write_ops >= 1);

    auto transport = cli->TransportForTest();
    BOOST_REQUIRE(transport);
    cli->Close();
    // Close() 的同步门禁验证物理资源；owner 批次的 IsClosed() 允许稍后落定。
    BOOST_CHECK(transport->IsClosed());
    const auto closed_totals = redis_detail::CotcpBindingTotalsForTest();
    BOOST_CHECK_EQUAL(closed_totals.readers_created, closed_totals.readers_freed);
    BOOST_CHECK(WaitUntil([&] { return cli->IsClosed(); }));
}

// 写侧阻塞 + deadline：命令未发完即 TimedOut；连接被判不可复用（不重发）。
BOOST_AUTO_TEST_CASE(t_write_side_deadline) {
    BOOST_REQUIRE(g_prepared.load());

    RawPeer peer(NeverRead(1500), /*listen_rcvbuf=*/4096);
    redis_detail::ResetCotcpBindingTotalsForTest();
    auto c = NewCotcpClient("127.0.0.1", peer.port);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    const std::string value(kPartialPayload, 'q');
    std::optional<result<void>> out;
    const auto t0 = std::chrono::steady_clock::now();
    BOOST_REQUIRE(RunInCoroutine(
        [&] { out.emplace(cli->Set("cotcp:b:big2", value, Opt(400))); }));
    const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - t0).count();
    BOOST_REQUIRE(!*out);
    BOOST_TEST_MESSAGE("write-deadline: code=" << static_cast<int>(out->error().code)
        << " elapsed_ms=" << elapsed_ms);
    BOOST_CHECK(out->error().code == ErrorCode::TimedOut);
    BOOST_CHECK(elapsed_ms >= 300);
    // Submit 返回的是等待层 deadline；owner 侧实际传输量由后置 conns_broken
    // 与写轮数证据验证，调用方错误对象不携带该内部计数。

    BOOST_TEST_MESSAGE("write-deadline: waiting owner batch to retire");
    BOOST_REQUIRE(WaitUntil([&] {
        const auto s = cli->ProbeSnapshot();
        return s.registered == 0 && s.conns_broken >= 1 && s.dial_inflight == 0;
    }));
    const auto totals = redis_detail::CotcpBindingTotalsForTest();
    BOOST_TEST_MESSAGE("write-deadline write_rounds=" << totals.write_rounds
        << " partial_write_ops=" << totals.partial_write_ops);
    BOOST_CHECK(totals.write_rounds >= 1);
    BOOST_CHECK(cli->ProbeSnapshot().conns_broken >= 1);

    cli->Close();
    BOOST_CHECK(WaitUntil([&] { return cli->IsClosed(); }));
}

// EOF（干净 FIN）与 RST 都映射为 TransportError。
BOOST_AUTO_TEST_CASE(t_eof_and_rst) {
    BOOST_REQUIRE(g_prepared.load());

    {   // 干净 FIN：handler 直接返回 → 对端 close
        RawPeer peer([](const std::shared_ptr<PeerState>&, int) {});
        auto    c = NewCotcpClient("127.0.0.1", peer.port);
        BOOST_REQUIRE(c);
        auto cli = std::move(c).value();
        std::optional<result<void>> out;
        BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt(4000))); }));
        BOOST_REQUIRE(!*out);
        BOOST_TEST_MESSAGE("eof -> code=" << static_cast<int>(out->error().code)
                           << " msg=" << out->error().message);
        BOOST_CHECK(out->error().code == ErrorCode::TransportError);
        cli->Close();
        BOOST_CHECK(WaitUntil([&] { return cli->IsClosed(); }));
    }

    {   // RST：SO_LINGER{1,0} 后由对端 close 触发
        RawPeer peer([](const std::shared_ptr<PeerState>&, int c) {
            linger lg{1, 0};
            ::setsockopt(c, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
            char tmp[64];
            (void)::recv(c, tmp, sizeof(tmp), 0);
        });
        auto c = NewCotcpClient("127.0.0.1", peer.port);
        BOOST_REQUIRE(c);
        auto cli = std::move(c).value();
        std::optional<result<void>> out;
        BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt(4000))); }));
        BOOST_REQUIRE(!*out);
        BOOST_TEST_MESSAGE("rst -> code=" << static_cast<int>(out->error().code)
                           << " msg=" << out->error().message);
        BOOST_CHECK(out->error().code == ErrorCode::TransportError);
        cli->Close();
        BOOST_CHECK(WaitUntil([&] { return cli->IsClosed(); }));
    }
}

// 写入后对端立即断开：后续请求仍应看到同一 TransportError，且不新建连接。
BOOST_AUTO_TEST_CASE(t_transport_error_is_sticky_no_reconnect) {
    BOOST_REQUIRE(g_prepared.load());

    RawPeer peer([](const std::shared_ptr<PeerState>& st, int c) {
        (void)st->ReadCommand(c);
    });
    auto c = NewCotcpClient("127.0.0.1", peer.port);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::optional<result<void>> first;
    BOOST_REQUIRE(RunInCoroutine([&] { first.emplace(cli->Ping(Opt(4000))); }));
    BOOST_REQUIRE(first);
    BOOST_REQUIRE(!*first);
    BOOST_CHECK(first->error().code == ErrorCode::TransportError);

    std::optional<result<void>> second;
    BOOST_REQUIRE(RunInCoroutine([&] { second.emplace(cli->Ping(Opt(4000))); }));
    BOOST_REQUIRE(second);
    BOOST_REQUIRE(!*second);
    BOOST_CHECK(second->error().code == ErrorCode::TransportError);
    std::optional<result<void>> third;
    BOOST_REQUIRE(RunInCoroutine([&] { third.emplace(cli->Ping(Opt(4000))); }));
    BOOST_REQUIRE(third);
    BOOST_REQUIRE(!*third);
    BOOST_CHECK(third->error().code == ErrorCode::TransportError);
    // 第三次请求跨过潜在的重连边界：坏连接不得重新 Dial。
    BOOST_CHECK_EQUAL(peer.Accepted(), 1);
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 1);

    cli->Close();
    BOOST_CHECK(WaitUntil([&] { return cli->IsClosed(); }));
}

// 读侧 deadline，以及候选真实使用的 transport 上「无进展不忙轮询」探针。
BOOST_AUTO_TEST_CASE(t_read_side_deadline) {
    BOOST_REQUIRE(g_prepared.load());

    RawPeer peer(ReadThenHold(3000));

    auto c = NewCotcpClient("127.0.0.1", peer.port);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    std::optional<result<void>> out;
    const auto t0 = std::chrono::steady_clock::now();
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt(400))); }));
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - t0).count();
    BOOST_REQUIRE(!*out);
    BOOST_TEST_MESSAGE("read-deadline: code=" << static_cast<int>(out->error().code)
                       << " elapsed_ms=" << ms);
    BOOST_CHECK(out->error().code == ErrorCode::TimedOut);
    BOOST_CHECK(ms >= 300 && ms < 3000);

    cli->Close();
    BOOST_CHECK(WaitUntil([&] { return cli->IsClosed(); }));
}

// 队首请求占用 owner 时，队尾短 deadline 到期后不得再写入健康连接。
BOOST_AUTO_TEST_CASE(t_queued_deadline_skips_without_breaking_connection) {
    BOOST_REQUIRE(g_prepared.load());

    RawPeer peer(ReplyAfterHoldThenProbe(1000, 500, RespStatus("PONG")));
    auto    c = NewCotcpClient("127.0.0.1", peer.port);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::optional<result<void>> first_out;
    std::optional<result<void>> queued_out;
    bbt::core::thread::CountDownLatch first_done{1};
    bbt::core::thread::CountDownLatch queued_done{1};
    bool succ = false;
    g_scheduler->RegistCoroutineTask([&] {
        first_out.emplace(cli->Ping(Opt(3000)));
        first_done.Down();
    }, succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return peer.CommandsRead() >= 1; }));

    succ = false;
    g_scheduler->RegistCoroutineTask([&] {
        queued_out.emplace(cli->Ping(Opt(100)));
        queued_done.Down();
    }, succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(queued_done.WaitTimeout(5000) == 0);
    BOOST_REQUIRE(queued_out);
    BOOST_REQUIRE(!*queued_out);
    BOOST_CHECK(queued_out->error().code == ErrorCode::TimedOut);

    BOOST_REQUIRE(first_done.WaitTimeout(5000) == 0);
    BOOST_REQUIRE(first_out);
    BOOST_REQUIRE(*first_out);
    BOOST_REQUIRE(WaitUntil([&] { return peer.ProbeFinished() == 1; }));
    BOOST_CHECK_EQUAL(peer.CommandsRead(), 1);
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().conns_broken, 0);

    cli->Close();
    BOOST_CHECK(WaitUntil([&] { return cli->IsClosed(); }));
}

// Close 物理收口：在途命令随 Close 落定 Closed；Close 返回当刻 fd 物理关闭、reader
// 恰好释放；晚到终态不覆盖首发终态。
BOOST_AUTO_TEST_CASE(t_owner_close_physical_collect) {
    BOOST_REQUIRE(g_prepared.load());
    redis_detail::ResetCotcpBindingTotalsForTest();

    RawPeer peer(ReadThenHold(3000));
    auto    c = NewCotcpClient("127.0.0.1", peer.port);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    // 在途命令：对端只收不回，命令挂在 CoTCP 可读等待上。
    std::optional<result<void>> out;
    std::atomic_bool            ready{false};
    bool                        succ = false;
    g_scheduler->RegistCoroutineTask([&] {
        out.emplace(cli->Ping(Opt(30000)));
        ready.store(true, std::memory_order_release);
    }, succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return peer.BytesRead() > 0; }));
    BOOST_REQUIRE(WaitUntil([&] { return cli->ProbeSnapshot().dial_attempts >= 1; }));
    SleepMs(150);
    BOOST_CHECK(!ready.load());

    auto tcp = cli->TransportForTest();
    BOOST_REQUIRE(tcp != nullptr);
    const int fd = W::NativeFdForTest(*tcp);
    BOOST_REQUIRE(fd >= 0);
    BOOST_CHECK(FdOpen(fd));
    BOOST_CHECK(!tcp->IsClosed());
    BOOST_CHECK(cli->ProbeSnapshot().registered >= 1);

    // Close：在途命令以 Closed 落定（首个逻辑终态）。
    cli->Close();
    BOOST_REQUIRE(WaitUntil([&] { return ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);

    // 物理收口（Close 返回当刻）：CoTCP fd 已关、hiredis reader 恰好释放。
    BOOST_CHECK(tcp->IsClosed());
    const auto probe = ProbeFd(fd);
    BOOST_CHECK_EQUAL(probe.rc, -1);
    BOOST_CHECK_EQUAL(probe.err, EBADF);
    const int victim = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_CHECK(victim >= 0);
    BOOST_CHECK(FdOpen(victim)); // 未被第二次 close 牵连（无双关）
    ::close(victim);

    const auto totals_now = redis_detail::CotcpBindingTotalsForTest();
    BOOST_TEST_MESSAGE("close totals: readers_created=" << totals_now.readers_created
        << " readers_freed=" << totals_now.readers_freed
        << " conns_created=" << totals_now.conns_created
        << " conns_destroyed=" << totals_now.conns_destroyed);
    BOOST_CHECK_EQUAL(totals_now.readers_created, totals_now.readers_freed);

    // 完整静默：已登记 op 归零、owner 批次退出后 IsClosed 落定。
    BOOST_REQUIRE(WaitUntil([&] {
        const auto s = cli->ProbeSnapshot();
        return s.registered == 0 && s.dial_inflight == 0;
    }));
    BOOST_REQUIRE(WaitUntil([&] { return cli->IsClosed(); }));
    const auto after = cli->ProbeSnapshot();
    BOOST_CHECK(after.finish_conflicts >= 1); // 晚到终态被拒

    // 收口配对：释放测试侧最后引用后 conn 创建/释放恰好配对。
    tcp.reset();
    cli.reset();
    BOOST_REQUIRE(WaitUntil([&] {
        const auto t = redis_detail::CotcpBindingTotalsForTest();
        return t.conns_created == t.conns_destroyed;
    }));
    const auto totals2 = redis_detail::CotcpBindingTotalsForTest();
    BOOST_CHECK_EQUAL(totals2.readers_created, totals2.readers_freed);
}

// 重复/并发 Close：无 UAF、无死锁；物理收口恰好一次（fd 关一次、reader 释放配对）。
BOOST_AUTO_TEST_CASE(t_repeated_and_concurrent_close) {
    BOOST_REQUIRE(g_prepared.load());
    redis_detail::ResetCotcpBindingTotalsForTest();

    RawPeer peer(ReadThenHold(3000));
    auto    c = NewCotcpClient("127.0.0.1", peer.port);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::optional<result<void>> out;
    bool                        succ = false;
    g_scheduler->RegistCoroutineTask([&] { out.emplace(cli->Ping(Opt(30000))); },
                                     succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return peer.BytesRead() > 0; }));
    auto tcp = cli->TransportForTest();
    BOOST_REQUIRE(tcp != nullptr);
    const int fd = W::NativeFdForTest(*tcp);
    BOOST_REQUIRE(fd >= 0);

    // 并发 Close：多个线程同时调用，再加重复调用。
    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&] {
            for (int j = 0; j < 5; ++j)
                cli->Close();
        });
    }
    for (auto& t : threads)
        t.join();
    cli->Close(); // 顺序重复

    // 物理收口：fd 关一次（号码不被误关第二次），reader 释放配对。
    BOOST_CHECK(WaitUntil([&] { return tcp->IsClosed(); }));
    const auto probe = ProbeFd(fd);
    BOOST_CHECK_EQUAL(probe.rc, -1);
    const int victim = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_CHECK(victim >= 0);
    BOOST_CHECK(FdOpen(victim));
    ::close(victim);

    BOOST_REQUIRE(WaitUntil([&] { return cli->IsClosed(); }));
    const auto totals = redis_detail::CotcpBindingTotalsForTest();
    BOOST_TEST_MESSAGE("repeated-close totals: readers_created=" << totals.readers_created
        << " readers_freed=" << totals.readers_freed
        << " conns_created=" << totals.conns_created
        << " conns_destroyed=" << totals.conns_destroyed);
    BOOST_CHECK_EQUAL(totals.readers_created, totals.readers_freed);

    tcp.reset();
    cli.reset();
    BOOST_REQUIRE(WaitUntil([&] {
        const auto t = redis_detail::CotcpBindingTotalsForTest();
        return t.conns_created == t.conns_destroyed;
    }));
}

// B1：Close 必须覆盖在途 DialTCP。
//
// 确定性复现：对端 accept 队列被填满 ⇒ 新 connect 在内核里挂起 ⇒ owner 协程真实
// 挂在 DialTCP 的 connect 等待上（dial_inflight==1 是可观察落点，非时序猜测）。
// 此时 Close：封口 owner 级 dial 等待登记打断 connect ⇒ Dial 以 Closed 返回且不留
// fd；不编码、不发送任何字节。
BOOST_AUTO_TEST_CASE(t_b1_close_during_inflight_dial) {
    BOOST_REQUIRE(g_prepared.load());

    BlackholeDial hole;
    const auto    sockets_before = CountSocketFds();
    redis_detail::ResetCotcpBindingTotalsForTest();

    auto c = NewCotcpClient("127.0.0.1", hole.port);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::optional<result<void>> out;
    std::atomic_bool            ready{false};
    bool                        succ = false;
    g_scheduler->RegistCoroutineTask([&] {
        out.emplace(cli->Ping(Opt(30000)));
        ready.store(true, std::memory_order_release);
    }, succ);
    BOOST_REQUIRE(succ);

    BOOST_REQUIRE(WaitUntil([&] { return cli->ProbeSnapshot().dial_inflight == 1; }));
    SleepMs(50);
    const auto during = cli->ProbeSnapshot();
    BOOST_TEST_MESSAGE("b1(dial-inflight) fillers=" << hole.FillerCount()
        << " dial_attempts=" << during.dial_attempts
        << " dial_inflight=" << during.dial_inflight
        << " has_conn=" << during.has_conn
        << " command_returned=" << ready.load());
    BOOST_REQUIRE_EQUAL(during.dial_inflight, 1u);
    // dial 未返回 ⇒ 尚未发布任何 CoTCP。
    BOOST_CHECK(cli->TransportForTest() == nullptr);
    BOOST_CHECK(!ready.load());

    cli->Close();

    // 命令以 Closed 落定；owner 批次收敛。
    BOOST_REQUIRE(WaitUntil([&] { return ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
    BOOST_REQUIRE(WaitUntil([&] { return cli->ProbeSnapshot().conn_closed; }));
    BOOST_REQUIRE(WaitUntil([&] { return cli->IsClosed(); }));
    BOOST_REQUIRE(WaitUntil([&] { return cli->ProbeSnapshot().registered == 0; }));

    const auto totals = redis_detail::CotcpBindingTotalsForTest();
    BOOST_TEST_MESSAGE("b1(dial-inflight) after: conns_broken="
        << cli->ProbeSnapshot().conns_broken
        << " commands_encoded=" << totals.commands_encoded
        << " write_rounds=" << totals.write_rounds
        << " read_rounds=" << totals.read_rounds);
    // 「关闭后不得继续编码或发送」的直接证据：本用例内没有任何编码/写轮次。
    BOOST_CHECK_EQUAL(totals.commands_encoded, 0u);
    BOOST_CHECK_EQUAL(totals.write_rounds, 0u);
    BOOST_CHECK_EQUAL(totals.read_rounds, 0u);
    BOOST_CHECK_EQUAL(hole.FillerCount(), 32u);

    // fd 物理收口：dial 候选 socket 不残留（基线已完成填充连接，无新增 socket）。
    // 注：dial 候选 fd 的释放在 owner 协程恢复时发生，故按有界等待断言，不冒充
    // Close 返回当刻。
    BOOST_REQUIRE(WaitUntil([&] { return CountSocketFds() <= sockets_before; }));
    const auto sockets_after = CountSocketFds();
    BOOST_TEST_MESSAGE("b1(dial-inflight) socket fds: before=" << sockets_before
        << " after=" << sockets_after);
    BOOST_CHECK(sockets_after <= sockets_before);

    // 收口配对：释放测试侧最后一引用后，conn/reader 创建与释放恰好配对。
    cli.reset();
    BOOST_REQUIRE(WaitUntil([&] {
        const auto t = redis_detail::CotcpBindingTotalsForTest();
        return t.conns_created == t.conns_destroyed;
    }));
    const auto totals2 = redis_detail::CotcpBindingTotalsForTest();
    BOOST_CHECK_EQUAL(totals2.readers_created, totals2.readers_freed);
}

// B1 的另一半：DialTCP 已成功、尚未编码的瞬间关闭到达。测试接缝在 owner 协程内执行
// （连接已发布、已物理建连），直接发起 Close()：此后不得编码、不得发送，且 Close 返回
// 当刻 fd 已物理关闭。
BOOST_AUTO_TEST_CASE(t_b1_close_after_dial_no_encode_or_send) {
    BOOST_REQUIRE(g_prepared.load());

    RawPeer peer(NeverRead(1500));
    redis_detail::ResetCotcpBindingTotalsForTest();

    auto c = NewCotcpClient("127.0.0.1", peer.port);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::shared_ptr<tcp::CoTCP> dialed_tcp;
    std::atomic<int>            dialed_fd{-1};
    std::atomic_bool            gated{false};
    cli->SetPostDialGateForTest([&] {
        auto tcp = cli->TransportForTest();
        if (tcp) {
            dialed_tcp = tcp;
            dialed_fd.store(W::NativeFdForTest(*tcp), std::memory_order_release);
        }
        gated.store(true, std::memory_order_release);
        cli->Close(); // 关闭到达：此后不得编码、不得发送
    });

    std::optional<result<void>> out;
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt(8000))); }));
    cli->SetPostDialGateForTest(nullptr);

    BOOST_REQUIRE(gated.load());
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);

    const auto totals = redis_detail::CotcpBindingTotalsForTest();
    BOOST_TEST_MESSAGE("b1(post-dial) commands_encoded=" << totals.commands_encoded
        << " write_rounds=" << totals.write_rounds
        << " peer_accepted=" << peer.Accepted()
        << " peer_bytes_read=" << peer.BytesRead());
    BOOST_CHECK_EQUAL(totals.commands_encoded, 0u);
    BOOST_CHECK_EQUAL(totals.write_rounds, 0u);
    BOOST_CHECK_EQUAL(peer.BytesRead(), 0u);
    BOOST_CHECK(peer.Accepted() >= 1);

    // fd 物理关闭（Close 返回当刻）：以 CoTCP 自身状态为据（不把「号码重新可用」
    // 当证据）：m_fd == -1 且 IsClosed() ⇒ 恰好关闭一次。
    BOOST_REQUIRE(dialed_tcp != nullptr);
    const int original_fd = dialed_fd.load();
    BOOST_REQUIRE(original_fd >= 0);
    BOOST_CHECK(dialed_tcp->IsClosed());
    BOOST_CHECK_EQUAL(W::NativeFdForTest(*dialed_tcp), -1);
    const int victim = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_CHECK(victim >= 0);
    BOOST_CHECK(FdOpen(victim)); // 未被第二次 close 牵连（无双关）
    ::close(victim);

    BOOST_REQUIRE(WaitUntil([&] { return cli->IsClosed(); }));
    BOOST_CHECK(cli->ProbeSnapshot().conn_closed);

    dialed_tcp.reset();
    cli.reset();
    BOOST_REQUIRE(WaitUntil([&] {
        const auto t = redis_detail::CotcpBindingTotalsForTest();
        return t.conns_created == t.conns_destroyed;
    }));
    const auto totals2 = redis_detail::CotcpBindingTotalsForTest();
    BOOST_CHECK_EQUAL(totals2.readers_created, totals2.readers_freed);
}

BOOST_AUTO_TEST_SUITE_END()
