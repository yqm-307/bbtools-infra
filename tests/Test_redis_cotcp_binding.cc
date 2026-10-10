// Redis CoTCP B 级 owner binding CoTCP 内部实现的定向验收（Boost.Test + CountDownLatch 有界
// 等待，与本仓 redis 套件同一风格）。
//
// 被测对象：src/redis/{RedisCotcpOwner,CoRedisCliImpl}.*（正式实现），
// hiredis 只做 RESP2 编码/解码，socket 与等待全部由 CoTCP 承担。
//
// 与当前 main 契约对齐（同步 Close、无 RequestClose/WaitClosed/CloseStatus、无取消
// 令牌、无 CompletionSignal、无 Scheduler::Stop）：
//   - 显式生命周期：Connect(options) 在协程内真实等待 TCP 建连完成；Disconnect()
//     同步释放连接但不进终态、可再次 Connect；Close() 是 ICoCloseable 终态。
//     ConnectStatus() 只读本地状态、不发网络探测。命令不得隐式建连（不做 Lazy）。
//   - owner 主动 Close() 同步返回；封口 + 收口已登记 op + 物理关闭 CoTCP（fd）+
//     释放 hiredis reader 都在 Close 的调用线程内完成。
//   - Close 返回当刻：已建连的 CoTCP fd 已物理关闭（IsClosed + FdOpen==false）；
//     hiredis reader 恰好释放（readers_created==readers_freed）。
//   - 在途 DialTCP：Close/Disconnect 封口 owner 级 dial 等待登记打断 connect，候选 fd
//     由 owner 关闭原语在封口后同步收口——返回当刻对象候选 fd 已释放（进程级 socket
//     数至多因协程内部 dup 残留一个；该 dup 随等待在协程恢复时释放）。
//   - IsClosed() 反映物理收口（封口 + 已登记 op 归零 + 连接物理收口），Close 返回
//     当刻即成立（owner 批次退出不再是落定条件）。
//   - reconnect_on_new_command=true 仅「故障（Failed）之后提交」的新命令可重建连接；
//     故障前排队命令以 TransportError 落定、不迁移、不重发（见
//     t_reconnect_only_for_new_commands_after_failure）。
//
// 连接故障后新命令的口径由 RedisClientConfig::reconnect_on_new_command 决定（默认
// false = sticky TransportError；true = 每条新命令允许一次新连接，已失败命令不重发）；
// 该选项不绕过显式生命周期（首次未 Connect / 主动 Disconnect / Close 均不隐式建连）。
//
// 覆盖矩阵：
//   t_resp2_roundtrip_live           真实 Redis loopback：Connect + PING/SET/GET/
//                                    EXISTS/DEL、二进制值、nil、服务端错误、2 MiB
//                                    大回复（无 Redis 时按退出码 77 表示环境不可用）
//   t_no_connect_command_rejected    未 Connect 的命令拒绝且服务端零连接（无字节）
//   t_explicit_connect_lifecycle     显式状态机：Connect 幂等 / ConnectStatus 只读无
//                                    网络请求 / Disconnect 同步收口且可再 Connect /
//                                    Close 拒绝 Connect；Ping 真实发送 PING 字节
//   t_connect_failure_state          连不上/超时 → Failed；默认 sticky TransportError，
//                                    显式重连每条新命令恰一次
//   t_connect_while_connecting       Connecting 中重复 Connect → Overloaded
//   t_disconnect_interrupts_inflight_dial Disconnect 打断在途 dial 并同步收口候选 fd
//   t_disconnect_during_inflight_command  Disconnect 与在途命令竞争：命令 TransportError、
//                                    同步收口、同实例可再 Connect
//   t_raw_reply_types_and_errors      原始对端预置 RESP2 各类型 + 垃圾字节协议错误
//   t_partial_write_then_full_delivery 小接收窗对端：写侧部分写/WouldBlock 后写满
//   t_write_side_deadline            写侧阻塞 + deadline → TimedOut（不重发）
//   t_eof_and_rst                    EOF（干净 FIN）与 RST 都映射为 TransportError
//   t_read_side_deadline             读侧 deadline → TimedOut；TryReadSome 无进展探针
//   t_owner_close_physical_collect   在途命令随 Close 落定 Closed；Close 返回当刻 fd
//                                    物理关闭、reader 恰好释放；晚到终态不覆盖
//   t_repeated_and_concurrent_close  重复/并发 Close：无 UAF/死锁，物理收口一次
//   t_b1_close_during_inflight_dial  DialTCP 挂起期间 Close：打断在途 dial、收敛、
//                                    不编码/不发送、无 socket 残留
//   t_b1_close_after_dial_no_encode  dial 已成功、尚未编码时 Close（测试接缝落点）：
//                                    不编码、不发送，Close 返回当刻 fd 物理关闭
//   t_reconnect_new_command_true     显式开启重连：故障后新命令重建连接成功、已失败
//                                    命令不重发（Accepted/dial_attempts/CommandsRead）
//   t_reconnect_dial_failure_exposed 显式开启重连且目标不可达：对应 dial/命令恰尝试
//                                    一次，失败直接暴露，无后台循环
//   t_close_inflight_isolated_synchronous 在途命令下 Close 同步落定（Close 返回当刻
//                                    IsClosed + fd 关闭 + registered 归零）
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
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/resource.h>
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
#include "redis/CoRedisCliImpl.hpp"
#include "redis/RedisDetail.hpp"

using namespace bbt::infra;
using bbt::coroutine::Deadline;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;
using bbt::infra::redis_detail::CoRedisCliImpl;
#ifdef BBT_INFRA_STRINGENT_DEBUG
using bbt::infra::redis_detail::RedisBindingTotals;
#endif
using W = bbt::infra::detail::TransportWiring;

namespace {

constexpr int         kBudgetMs        = 15000;
constexpr std::size_t kBadFrame        = static_cast<std::size_t>(-1);
constexpr std::size_t kPartialPayload  = 4u * 1024 * 1024;
// 「环境不可用」（未显式给地址且默认 loopback 无 Redis）的专用退出码，与 CTest 的
// SKIP_RETURN_CODE 严格对应；断言失败/崩溃不会用这个码。
constexpr int kEnvUnavailableExitCode = 77;

RedisClientConfig MakeConfig(std::string host, std::uint16_t port,
                             std::size_t inflight = 8, std::size_t queue = 8,
                             bool reconnect_on_new_command = false) {
    RedisClientConfig cfg;
    cfg.host         = std::move(host);
    cfg.port         = port;
    cfg.max_inflight = inflight;
    cfg.max_queue    = queue;
    cfg.reconnect_on_new_command = reconnect_on_new_command;
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

// 受控同步：等待受管 dial 的 connect 等待段真正挂起——「dial 已进入 native
// 等待」的真实落点证据（不用观测计数冒充、不用 sleep 代替同步点）。
// 接缝来自 DialWaitOptions::connect_on_registered：connect 等待段（EINPROGRESS
// 的 fd 可写等待）在协程挂起后回调一次；Debug/Release 走同一条路径，无宏分叉。
// 注意：has_conn 由 PublishSlot 在 Dial 之前发布，早于 dial 挂起，不作为
// 「dial 在途」证据。必须在发起 Connect 之前 Install（回调仅在挂起登记时触发一次）。
class DialWaitProbe {
public:
    void Install(const std::shared_ptr<CoRedisCliImpl>& cli) {
        // 回调按值捕获共享闩：dial 协程可能跨地址重试、在探针栈帧析构后仍被
        // 唤醒（例如 localhost 多地址），捕获 this 会悬垂；共享所有权避免。
        auto parked = m_parked;
        cli->SetDialWaitEntryGateForTest([parked] {
            parked->store(true, std::memory_order_release);
        });
    }
    bool Wait(int budget_ms = kBudgetMs) const {
        return WaitUntil(
            [&] { return m_parked->load(std::memory_order_acquire); },
            budget_ms);
    }

private:
    std::shared_ptr<std::atomic_bool> m_parked =
        std::make_shared<std::atomic_bool>(false);
};

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
//
// 前提必须自证：非阻塞 connect 的握手完成在内核异步进行，批量 connect 之后、队列
// 真正填满之前，紧邻的 connect 可能先占位而成功（疑似对应
// t_disconnect_interrupts_inflight_dial 的 TransportForTest/!ready/!*out 三项连锁失败；
// 该因果未在 CI 复核，仅为与现象一致的推断）。因此填充后必须用一次探测 connect 确认
// 「新 connect 不会完成」；未确认则继续补充填充，有界重试后仍不成立即显式失败，不做
// 静默继续。
//
// 探测结论只采信真实内核信息：判「完成」必须 POLLOUT 且 SO_ERROR==0（真实
// ESTABLISHED）；判「悬停」须以内核 TCP 态仍为 SYN_SENT 佐证，不以遍历时长为判据；
// poll 出错 / POLLERR / POLLHUP / SO_ERROR!=0 / 非阻塞设置失败一律判「不可用」，绝不
// 当成前提成立。
//
// 构造失败（含新增的前提不成立）抛 BlackholeDialError；由成员 FdCleanup 在栈回卷中
// 真实回收全部 fd，避免 ctor 未完成时 ~BlackholeDial 不执行导致泄漏。

struct BlackholeDialError : std::runtime_error {
    explicit BlackholeDialError(const std::string& m) : std::runtime_error(m) {}
};

// 探测结论：
//   Hangs      悬停（前提成立）——须内核态佐证 SYN_SENT
//   Completed  真实完成（占住 accept 队列位）
//   Unusable   不可用（错误/读不到状态，不得据此确认前提）
//   InProgress 本 slice 无事件，继续有界等待
enum class ProbeResult { Hangs, Completed, Unusable, InProgress };

// 一次 poll 结果 + SO_ERROR → 结论（纯判据，便于正反例直接验证）。poll 失败不得判
// 悬停；POLLERR/POLLHUP/SO_ERROR!=0 不得判完成。
ProbeResult ClassifyProbePoll(int poll_rc, short revents, int so_error) {
    if (poll_rc < 0)
        return ProbeResult::Unusable;
    if (poll_rc == 0)
        return ProbeResult::InProgress;
    const short bad = static_cast<short>(POLLERR | POLLHUP | POLLNVAL);
    if ((revents & POLLOUT) != 0 && (revents & bad) == 0 && so_error == 0)
        return ProbeResult::Completed;
    return ProbeResult::Unusable;
}

// 置非阻塞；失败返回 false（调用方不得据此认为该 fd 可用）。
bool SetNonBlocking(int fd) {
    const int fl = ::fcntl(fd, F_GETFL, 0);
    return fl >= 0 && ::fcntl(fd, F_SETFL, fl | O_NONBLOCK) == 0;
}

// 内核 TCP 连接态（TCP_INFO.tcpi_state）；读不到返回 -1。
int TcpInfoState(int fd) {
    tcp_info info{};
    socklen_t len = sizeof(info);
    if (::getsockopt(fd, IPPROTO_TCP, TCP_INFO, &info, &len) != 0 ||
        len < sizeof(info))
        return -1;
    return static_cast<int>(info.tcpi_state);
}

class BlackholeDial {
    // 构造期即已构建的回收器：任意一步失败（含抛异常）都会运行其析构，真实关闭
    // listener 与全部 filler/probe fd。声明在成员末尾 → 最先析构，届时容器仍存活。
    struct FdCleanup {
        int*              lfd;
        std::vector<int>* fillers;
        std::vector<int>* probes;
        ~FdCleanup() {
            for (int s : *fillers)
                ::close(s);
            for (int s : *probes)
                ::close(s);
            if (*lfd >= 0)
                ::close(*lfd);
        }
    };

public:
    explicit BlackholeDial(std::size_t fillers = 32)
        : m_cleanup{&m_lfd, &m_fillers, &m_probes} {
        m_lfd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (m_lfd < 0)
            Fail("BlackholeDial: socket() 失败");
        int one = 1;
        ::setsockopt(m_lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port        = 0;
        if (::bind(m_lfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0)
            Fail("BlackholeDial: bind() 失败");
        if (::listen(m_lfd, 1) != 0)
            Fail("BlackholeDial: listen() 失败");
        socklen_t len = sizeof(addr);
        if (::getsockname(m_lfd, reinterpret_cast<sockaddr*>(&addr), &len) != 0)
            Fail("BlackholeDial: getsockname() 失败");
        port   = ntohs(addr.sin_port);
        m_addr = addr;

        FillBatch(fillers);
        // 探测 socket 不计入 FillerCount 契约（== fillers）。判为已完成的探测必然已
        // POLLOUT 且 SO_ERROR==0（真实 ESTABLISHED），它占住一个 accept 队列位，故保留
        // 在 m_probes 里不关闭；队列趋满后下一轮探测才会悬停。
        bool holds = false;
        for (std::size_t i = 0; i < kMaxProbes && !holds; ++i) {
            const ProbeResult r = ProbePremise();
            if (r == ProbeResult::Hangs)
                holds = true;
            else if (r == ProbeResult::Unusable)
                break;  // 立即建立/立即失败：前提不可能成立，不必再试
        }
        if (!holds)
            Fail("blackhole dial 前提不成立：accept 队列未填满，connect 不会悬停");
    }
    BlackholeDial(const BlackholeDial&)            = delete;
    BlackholeDial& operator=(const BlackholeDial&) = delete;

    std::uint16_t port = 0;
    std::size_t   FillerCount() const { return m_fillers.size(); }

private:
    // 构造失败统一出口：消息带上已分配计数，便于测试区分「一开头就失败」与「已分配若干
    // filler 后才失败」（后者才是 fd 泄漏风险路径）。
    [[noreturn]] void Fail(const char* what) const {
        throw BlackholeDialError(std::string(what) + "（fillers=" +
                                 std::to_string(m_fillers.size()) + " probes=" +
                                 std::to_string(m_probes.size()) + "）");
    }

    // 一批非阻塞 connect（握手完成前占 SYN 队列；已建立的会占 accept 队列）。
    void FillBatch(std::size_t n) {
        for (std::size_t i = 0; i < n; ++i) {
            const int s = ::socket(AF_INET, SOCK_STREAM, 0);
            if (s < 0)
                return;
            if (!SetNonBlocking(s)) {  // 阻塞 connect 会挂死：失败即弃用该 fd
                ::close(s);
                return;
            }
            (void)::syscall(SYS_connect, s, reinterpret_cast<sockaddr*>(&m_addr),
                            static_cast<socklen_t>(sizeof(m_addr)));
            m_fillers.push_back(s);
        }
    }
    // 一次新 connect 的判定：悬停（前提成立）/ 完成（占位后重试）/ 不可用（前提不
    // 可能成立）。完成须真实 POLLOUT 且 SO_ERROR==0；悬停以内核态 SYN_SENT 佐证，
    // 不以遍历时长为判据（有界 poll 只用于区分「在途」与「已完成」）。
    ProbeResult ProbePremise() {
        const int s = ::socket(AF_INET, SOCK_STREAM, 0);
        if (s < 0)
            return ProbeResult::Unusable;
        if (!SetNonBlocking(s)) {  // 非阻塞设置失败：不得假确认
            ::close(s);
            return ProbeResult::Unusable;
        }
        const int rc = static_cast<int>(::syscall(
            SYS_connect, s, reinterpret_cast<sockaddr*>(&m_addr),
            static_cast<socklen_t>(sizeof(m_addr))));
        if (rc == 0) {  // 立即建立：确已完成，保留占位
            m_probes.push_back(s);
            return ProbeResult::Completed;
        }
        if (errno != EINPROGRESS) {  // 立即失败（如 RST/拒绝）：前提不可用
            ::close(s);
            return ProbeResult::Unusable;
        }
        for (int i = 0; i < kProbeSlices; ++i) {
            pollfd    p{s, POLLOUT, 0};
            const int pr =
                static_cast<int>(::syscall(SYS_poll, &p, 1, kProbeSliceMs));
            if (pr < 0 && errno == EINTR)
                continue;  // 被信号打断：重试本 slice
            int so_error = 0;
            if (pr > 0) {
                socklen_t el = sizeof(so_error);
                if (::getsockopt(s, SOL_SOCKET, SO_ERROR, &so_error, &el) != 0)
                    so_error = -1;  // 读不到错误码：不当作 0
            }
            const ProbeResult v = ClassifyProbePoll(pr, p.revents, so_error);
            if (v == ProbeResult::Completed) {
                m_probes.push_back(s);
                return ProbeResult::Completed;
            }
            if (v == ProbeResult::Unusable) {  // poll 失败/POLLERR/SO_ERROR!=0
                ::close(s);
                return ProbeResult::Unusable;
            }
            // InProgress：继续有界条件等待
        }
        // 等待内无 POLLOUT：以内核连接态确认是否确为悬停，而非以时长为判据。
        const int st = TcpInfoState(s);
        if (st == TCP_SYN_SENT) {  // 内核仍在 SYN_SENT：确为悬停
            m_probes.push_back(s);
            return ProbeResult::Hangs;
        }
        if (st == TCP_ESTABLISHED) {  // 已建立但事件未到：按已完成占位
            m_probes.push_back(s);
            return ProbeResult::Completed;
        }
        ::close(s);
        return ProbeResult::Unusable;  // 状态不可读/已关闭：不假确认
    }
    static constexpr std::size_t kMaxProbes   = 8;
    static constexpr int         kProbeSlices = 5;
    static constexpr int         kProbeSliceMs = 10;

    int              m_lfd{-1};
    std::vector<int> m_fillers;
    std::vector<int> m_probes;
    sockaddr_in      m_addr{};
    FdCleanup        m_cleanup;
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

// 捕获连接收到的首段字节（用于证明 Ping 真实发送了 RESP2 PING 帧），随后回一条
// 预置应答。
PeerHandler CaptureFirstCommand(const std::shared_ptr<std::string>& out,
                                const std::string&                 reply) {
    return [out, reply](const std::shared_ptr<PeerState>&, int c) {
        char          buf[256];
        const ssize_t n = ::recv(c, buf, sizeof(buf), 0);
        if (n > 0)
            out->assign(buf, static_cast<std::size_t>(n));
        if (!reply.empty())
            (void)::send(c, reply.data(), reply.size(), MSG_NOSIGNAL);
    };
}

// 等待服务端 accept 计数达到 n：accept 在 RawPeer 的协程里执行，客户端 Connect
// 返回（TCP 已建立）后该计数才可能被观察到——有界等待，不是时序假设。
bool WaitAccepted(const RawPeer& peer, std::uint64_t n, int budget_ms = 5000) {
    return WaitUntil([&] { return peer.Accepted() >= n; }, budget_ms);
}

result<std::shared_ptr<CoRedisCliImpl>> NewCotcpClient(
    std::string host, std::uint16_t port, bool connect = true,
    bool reconnect_on_new_command = false) {
    auto c = CoRedisCli::Create(MakeConfig(std::move(host), port, 8, 8,
                                           reconnect_on_new_command));
    if (!c)
        return result<std::shared_ptr<CoRedisCliImpl>>::err(
            std::move(c).error());
    auto cli = std::dynamic_pointer_cast<CoRedisCliImpl>(std::move(c).value());
    if (!cli)
        return result<std::shared_ptr<CoRedisCliImpl>>::err(
            MakeError(ErrorCode::InternalError, "redis: unexpected implementation"));
    if (connect) {
        // 显式生命周期：命令不再隐式建连，需要连接的用例必须显式 Connect 成功。
        std::optional<result<void>> st;
        if (!RunInCoroutine([&] { st.emplace(cli->Connect(Opt())); }))
            return result<std::shared_ptr<CoRedisCliImpl>>::err(MakeError(
                ErrorCode::InternalError, "redis: connect coroutine not run"));
        if (!st)
            return result<std::shared_ptr<CoRedisCliImpl>>::err(MakeError(
                ErrorCode::InternalError, "redis: connect produced no result"));
        if (!*st)
            return result<std::shared_ptr<CoRedisCliImpl>>::err(
                std::move(*st).error());
    }
    return result<std::shared_ptr<CoRedisCliImpl>>::ok(std::move(cli));
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

// S1（不做 Lazy）：未 Connect 的命令一律被拒绝且不得隐式建连——服务端从未建立连接
// （零接受、零字节）；ConnectStatus 只读、不产生任何网络请求。
BOOST_AUTO_TEST_CASE(t_no_connect_command_rejected) {
    BOOST_REQUIRE(g_prepared.load());
#ifdef BBT_INFRA_STRINGENT_DEBUG
    redis_detail::ResetRedisBindingTotalsForTest();
#endif

    RawPeer peer(ReplyOnce("+PONG\r\n"));
    auto    c = NewCotcpClient("127.0.0.1", peer.port, /*connect=*/false);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    // ConnectStatus 只读：重复读取不拨号、不建连。
    for (int i = 0; i < 8; ++i)
        BOOST_CHECK(cli->ConnectStatus() == ConnectState::Disconnected);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 0u);
#endif
    BOOST_CHECK_EQUAL(peer.Accepted(), 0u);

    std::optional<result<void>> out;
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt(1000))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::RuntimeUnavailable);
    // 命令没有隐式建连：无拨号、服务端零接受、零字节。
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 0u);
#endif
    BOOST_CHECK_EQUAL(peer.Accepted(), 0u);
    BOOST_CHECK_EQUAL(peer.BytesRead(), 0u);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Disconnected);

    // 显式 Connect 后才建立连接并可用。
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Connect(Opt())); }));
    BOOST_REQUIRE(*out);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Connected);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 1u);
#endif
    BOOST_REQUIRE(WaitAccepted(peer, 1));
    BOOST_CHECK_EQUAL(peer.Accepted(), 1u);
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(*out);

    cli->Close();
    BOOST_CHECK(cli->IsClosed());
}

// S2/S3/S4/S5 + Ping 真实 PING：显式生命周期状态机（Connect 幂等 / ConnectStatus
// 只读 / Disconnect 同步收口且可再次 Connect / Close 终态拒绝 Connect）。
BOOST_AUTO_TEST_CASE(t_explicit_connect_lifecycle) {
    BOOST_REQUIRE(g_prepared.load());
#ifdef BBT_INFRA_STRINGENT_DEBUG
    redis_detail::ResetRedisBindingTotalsForTest();
#endif

    auto    ping_bytes = std::make_shared<std::string>();
    RawPeer peer(CaptureFirstCommand(ping_bytes, "+PONG\r\n"));
    auto    c = NewCotcpClient("127.0.0.1", peer.port, /*connect=*/false);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::optional<result<void>> out;
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Connect(Opt())); }));
    BOOST_REQUIRE(*out);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Connected);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 1u);
#endif
    BOOST_REQUIRE(WaitAccepted(peer, 1));
    BOOST_CHECK_EQUAL(peer.Accepted(), 1u);
    auto tcp = cli->TransportForTest();
    BOOST_REQUIRE(tcp != nullptr);
    BOOST_REQUIRE(W::NativeFdForTest(*tcp) >= 0);

    // 重复 Connect：幂等 ok，不重新拨号、不新建连接。
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Connect(Opt())); }));
    BOOST_REQUIRE(*out);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 1u);
#endif
    BOOST_CHECK_EQUAL(peer.Accepted(), 1u);

    // Ping 必须真实发送 RESP2 PING 帧。
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(*out);
    BOOST_CHECK(!ping_bytes->empty());
    BOOST_CHECK_EQUAL(*ping_bytes, std::string("*1\r\n$4\r\nPING\r\n"));

    // Disconnect：同步收口 fd/reader/登记请求，但非终态、配置保留。
    cli->Disconnect();
    BOOST_CHECK(!cli->IsClosed());
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Disconnected);
    BOOST_CHECK(tcp->IsClosed()); // 物理 fd 已同步关闭
    BOOST_CHECK_EQUAL(cli->BindingStateSnapshot().registered, 0u);
    BOOST_CHECK(cli->BindingStateSnapshot().conn_closed);
    BOOST_CHECK(cli->TransportForTest() == nullptr);

    // 主动 Disconnect 之后命令被拒且不建连（未再发送任何字节）。
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt(1000))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::RuntimeUnavailable);
    BOOST_CHECK_EQUAL(*ping_bytes, std::string("*1\r\n$4\r\nPING\r\n"));
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 1u);
#endif

    // 同实例再次显式 Connect：新代际、新连接。
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Connect(Opt())); }));
    BOOST_REQUIRE(*out);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Connected);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 2u);
#endif
    BOOST_REQUIRE(WaitAccepted(peer, 2));
    BOOST_CHECK_EQUAL(peer.Accepted(), 2u);
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(*out);

    // Close 终态：拒绝 Connect，不再拨号。
    auto tcp2 = cli->TransportForTest();
    BOOST_REQUIRE(tcp2 != nullptr);
    cli->Close();
    BOOST_CHECK(cli->IsClosed());
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Closed);
    BOOST_CHECK(tcp2->IsClosed());
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Connect(Opt())); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 2u);
#endif

    tcp.reset();
    tcp2.reset();
    cli.reset();
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_REQUIRE(WaitUntil([&] {
        const auto t = redis_detail::RedisBindingTotalsForTest();
        return t.conns_created == t.conns_destroyed;
    }));
    const auto totals = redis_detail::RedisBindingTotalsForTest();
    BOOST_CHECK_EQUAL(totals.readers_created, totals.readers_freed);
#endif
}

// A1：探测判据正反例（含真实内核 RST）。任何错误事件都不得被当成「前提成立」。
BOOST_AUTO_TEST_CASE(t_blackhole_probe_classification) {
    // 纯判据
    BOOST_CHECK(ClassifyProbePoll(1, POLLOUT, 0) == ProbeResult::Completed);
    BOOST_CHECK(ClassifyProbePoll(-1, 0, 0) == ProbeResult::Unusable);  // poll 失败
    BOOST_CHECK(ClassifyProbePoll(1, POLLERR, ECONNRESET) == ProbeResult::Unusable);
    BOOST_CHECK(ClassifyProbePoll(1, POLLOUT, ECONNREFUSED) == ProbeResult::Unusable);
    BOOST_CHECK(ClassifyProbePoll(1, static_cast<short>(POLLOUT | POLLHUP), 0) ==
                ProbeResult::Unusable);
    BOOST_CHECK(ClassifyProbePoll(0, 0, 0) == ProbeResult::InProgress);
    // fcntl 失败不得被当作可用：无效 fd 置非阻塞失败。
    BOOST_CHECK(!SetNonBlocking(-1));

    // 真实内核：可建立连接 → Completed；对端 RST → Unusable(SO_ERROR!=0)。
    const int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(lfd >= 0);
    int one = 1;
    ::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port        = 0;
    BOOST_REQUIRE(::bind(lfd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    BOOST_REQUIRE(::listen(lfd, 8) == 0);
    socklen_t alen = sizeof(addr);
    BOOST_REQUIRE(::getsockname(lfd, reinterpret_cast<sockaddr*>(&addr), &alen) == 0);

    const int c = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(c >= 0);
    BOOST_REQUIRE(SetNonBlocking(c));
    const int crc = static_cast<int>(::syscall(
        SYS_connect, c, reinterpret_cast<sockaddr*>(&addr),
        static_cast<socklen_t>(sizeof(addr))));
    BOOST_REQUIRE(crc == 0 || errno == EINPROGRESS);
    pollfd cp{c, POLLOUT, 0};
    BOOST_REQUIRE(::syscall(SYS_poll, &cp, 1, 1000) > 0);
    int       serr = 0;
    socklen_t sl   = sizeof(serr);
    BOOST_REQUIRE(::getsockopt(c, SOL_SOCKET, SO_ERROR, &serr, &sl) == 0);
    BOOST_CHECK(ClassifyProbePoll(1, cp.revents, serr) == ProbeResult::Completed);

    const int a = ::accept(lfd, nullptr, nullptr);
    BOOST_REQUIRE(a >= 0);
    linger lg{1, 0};  // SO_LINGER{on,0} → close 发 RST
    ::setsockopt(a, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
    ::close(a);

    int   rres     = 0;
    short rrevents = 0;
    for (int i = 0; i < 200 && rres == 0; ++i) {  // 有界等待错误事件
        pollfd    ep{c, POLLOUT, 0};
        const int pr = static_cast<int>(::syscall(SYS_poll, &ep, 1, 10));
        if (pr > 0 && (ep.revents & (POLLERR | POLLHUP)) != 0) {
            rres     = pr;
            rrevents = ep.revents;
        }
    }
    BOOST_REQUIRE(rres > 0);  // loopback 上 RST 应立即到达
    int       e2  = 0;
    socklen_t e2l = sizeof(e2);
    BOOST_REQUIRE(::getsockopt(c, SOL_SOCKET, SO_ERROR, &e2, &e2l) == 0);
    BOOST_CHECK(ClassifyProbePoll(rres, rrevents, e2) == ProbeResult::Unusable);

    ::close(c);
    ::close(lfd);
}

// A2：构造失败路径（含新增的前提不成立）必须真实回收 fd 回基线，不得泄漏。
// 用有界 fd 软限额强制 BlackholeDial 内部 socket() 中途 EMFILE：FillBatch 已分配若干
// filler 后，probe 也拿不到 fd ⇒ 前提无法确认 ⇒ 构造抛 BlackholeDialError。若 ctor 不
// 回收，这些 fd 会残留在进程里。只降软限（硬限不变）并在构造后立即恢复。
BOOST_AUTO_TEST_CASE(t_blackhole_dial_ctor_fail_releases_fds) {
    const std::size_t before = CountSocketFds();

    rlimit old{};
    BOOST_REQUIRE(::getrlimit(RLIMIT_NOFILE, &old) == 0);
    std::size_t open_fds = 0;
    if (DIR* d = ::opendir("/proc/self/fd")) {
        while (dirent* e = ::readdir(d))
            if (std::strcmp(e->d_name, ".") != 0 &&
                std::strcmp(e->d_name, "..") != 0)
                ++open_fds;
        ::closedir(d);
    }
    rlimit lim   = old;
    lim.rlim_cur = open_fds + 8;  // 只降软限，硬限不变，可恢复
    BOOST_REQUIRE(::setrlimit(RLIMIT_NOFILE, &lim) == 0);

    bool        threw = false;
    std::string msg;
    try {
        BlackholeDial hole(64);
    } catch (const BlackholeDialError& e) {
        threw = true;
        msg   = e.what();
    } catch (...) {
    }
    BOOST_REQUIRE(::setrlimit(RLIMIT_NOFILE, &old) == 0);

    BOOST_CHECK_MESSAGE(threw, "构造失败未抛 BlackholeDialError（fd 限额未生效？）");
    // 失败须发生在 FillBatch 已分配若干 filler 之后（fillers>0），否则本用例未覆盖
    // 「已分配 fd 后失败」这一泄漏风险路径。
    BOOST_TEST_MESSAGE("构造失败信息: " << msg);
    BOOST_CHECK(msg.find("fillers=0 probes=0") == std::string::npos);
    BOOST_CHECK_EQUAL(CountSocketFds(), before);
}

// S2：Connect 超时/失败 → Failed 状态；默认 sticky（命令 TransportError 且不再拨号），
// 显式 Connect 仍可再试。
BOOST_AUTO_TEST_CASE(t_connect_timeout_failed_state) {
    BOOST_REQUIRE(g_prepared.load());
    BlackholeDial hole;
#ifdef BBT_INFRA_STRINGENT_DEBUG
    redis_detail::ResetRedisBindingTotalsForTest();
#endif

    auto c = NewCotcpClient("127.0.0.1", hole.port, /*connect=*/false);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::optional<result<void>> out;
    const auto t0 = std::chrono::steady_clock::now();
    BOOST_REQUIRE(
        RunInCoroutine([&] { out.emplace(cli->Connect(Opt(300))); }, 8000));
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - t0).count();
    BOOST_REQUIRE(!*out);
    BOOST_TEST_MESSAGE("connect-timeout: code=" << static_cast<int>(out->error().code)
                       << " elapsed_ms=" << ms);
    BOOST_CHECK(out->error().code == ErrorCode::TimedOut);
    BOOST_CHECK(ms >= 250);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Failed);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 1u);
#endif
    BOOST_CHECK(cli->TransportForTest() == nullptr);
    BOOST_CHECK(cli->BindingStateSnapshot().conn_closed);

    // 默认 reconnect_on_new_command=false：Failed 下命令 TransportError，不重连。
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt(1000))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TransportError);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 1u);
#endif
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Failed);

    // 显式 Connect 可再试（仍超时）——Failed 不是终态。
    BOOST_REQUIRE(
        RunInCoroutine([&] { out.emplace(cli->Connect(Opt(300))); }, 8000));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TimedOut);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 2u);
#endif
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Failed);

    cli->Close();
    BOOST_CHECK(cli->IsClosed());
}

// S2/S6：Connecting 中重复 Connect → Overloaded；期间命令被拒。
BOOST_AUTO_TEST_CASE(t_connect_while_connecting) {
    BOOST_REQUIRE(g_prepared.load());
    BlackholeDial hole;

    auto c = NewCotcpClient("127.0.0.1", hole.port, /*connect=*/false);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    DialWaitProbe dial;
    dial.Install(cli);
    std::optional<result<void>> first;
    std::atomic_bool            first_ready{false};
    bool                        succ = false;
    g_scheduler->RegistCoroutineTask([&] {
        first.emplace(cli->Connect(Opt(20000)));
        first_ready.store(true, std::memory_order_release);
    }, succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(dial.Wait());
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Connecting);

    // 连接中重复 Connect → Overloaded（不引入排队/等待架构）。
    std::optional<result<void>> second;
    BOOST_REQUIRE(RunInCoroutine([&] { second.emplace(cli->Connect(Opt(1000))); }));
    BOOST_REQUIRE(!*second);
    BOOST_CHECK(second->error().code == ErrorCode::Overloaded);

    // 连接中命令被拒（不隐式建连、不排队）。
    std::optional<result<void>> cmd;
    BOOST_REQUIRE(RunInCoroutine([&] { cmd.emplace(cli->Ping(Opt(1000))); }));
    BOOST_REQUIRE(!*cmd);
    BOOST_CHECK(cmd->error().code == ErrorCode::RuntimeUnavailable);

    cli->Close();
    BOOST_REQUIRE(WaitUntil([&] { return first_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(first);
    BOOST_REQUIRE(!*first);
    BOOST_CHECK(first->error().code == ErrorCode::Closed);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Closed);
}

// S6：Disconnect 打断在途 dial——同步收口候选 fd、回到 Disconnected（不是终态），
// 被中断的 Connect 以错误返回。
BOOST_AUTO_TEST_CASE(t_disconnect_interrupts_inflight_dial) {
    BOOST_REQUIRE(g_prepared.load());
    BlackholeDial hole;
    const auto    sockets_before = CountSocketFds();
#ifdef BBT_INFRA_STRINGENT_DEBUG
    redis_detail::ResetRedisBindingTotalsForTest();
#endif

    auto c = NewCotcpClient("127.0.0.1", hole.port, /*connect=*/false);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::optional<result<void>> out;
    std::atomic_bool            ready{false};
    DialWaitProbe dial;
    dial.Install(cli);
    bool                        succ = false;
    g_scheduler->RegistCoroutineTask([&] {
        out.emplace(cli->Connect(Opt(30000)));
        ready.store(true, std::memory_order_release);
    }, succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(dial.Wait());
    const auto sockets_during = CountSocketFds();
    BOOST_CHECK(cli->TransportForTest() == nullptr);
    BOOST_CHECK(!ready.load());

    // Disconnect（非协程线程）：同步打断在途 dial 并收口候选 fd。
    cli->Disconnect();
    const auto sockets_after = CountSocketFds();
    BOOST_TEST_MESSAGE("disconnect-vs-dial socket fds: before=" << sockets_before
        << " during=" << sockets_during << " after=" << sockets_after);
    BOOST_CHECK(sockets_after <= sockets_before + 1);
    BOOST_CHECK(sockets_during > sockets_after);
    BOOST_CHECK(!cli->IsClosed());
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Disconnected);
    BOOST_CHECK_EQUAL(cli->BindingStateSnapshot().registered, 0u);
    BOOST_CHECK(cli->BindingStateSnapshot().conn_closed);

    BOOST_REQUIRE(WaitUntil([&] { return ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(out);
    BOOST_CHECK(!*out); // 被 Disconnect 打断的 Connect 以错误返回

#ifdef BBT_INFRA_STRINGENT_DEBUG
    const auto totals = redis_detail::RedisBindingTotalsForTest();
    BOOST_CHECK_EQUAL(totals.commands_encoded, 0u);
    BOOST_CHECK_EQUAL(totals.write_rounds, 0u);
#endif

    // 协程恢复后内部 dup 释放，socket 数回基线（运行时伪影，非对象资源）。
    BOOST_REQUIRE(WaitUntil([&] { return CountSocketFds() <= sockets_before; }));

    cli->Close();
    BOOST_CHECK(cli->IsClosed());
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Closed);
}

// S6：Disconnect 与在途命令竞争——命令以 TransportError 落定（非悬挂、非 Closed），
// Disconnect 返回当刻连接已物理收口、registered 归零；同实例可再次 Connect。
BOOST_AUTO_TEST_CASE(t_disconnect_during_inflight_command) {
    BOOST_REQUIRE(g_prepared.load());
    RawPeer peer(ReadThenHold(2000));
    auto    c = NewCotcpClient("127.0.0.1", peer.port); // connect=true
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::optional<result<void>> out;
    std::atomic_bool            ready{false};
    bool                        succ = false;
    g_scheduler->RegistCoroutineTask([&] {
        out.emplace(cli->Ping(Opt(20000)));
        ready.store(true, std::memory_order_release);
    }, succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return peer.BytesRead() > 0; }));
    auto tcp = cli->TransportForTest();
    BOOST_REQUIRE(tcp != nullptr);
    BOOST_CHECK(!tcp->IsClosed());
    BOOST_CHECK(cli->BindingStateSnapshot().registered >= 1);

    // Disconnect（非协程线程）：同步落定在途命令并收口连接。
    cli->Disconnect();
    BOOST_CHECK(tcp->IsClosed()); // 物理 fd 已同步关闭
    BOOST_CHECK_EQUAL(cli->BindingStateSnapshot().registered, 0u);
    BOOST_CHECK(cli->TransportForTest() == nullptr);
    BOOST_CHECK(!cli->IsClosed());
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Disconnected);

    BOOST_REQUIRE(WaitUntil([&] { return ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(out);
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TransportError);

    // 同实例再次显式 Connect 后命令可用。
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Connect(Opt())); }));
    BOOST_REQUIRE(*out);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Connected);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 2u);
#endif

    cli->Close();
    BOOST_CHECK(cli->IsClosed());
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Closed);
}

// 真实 Redis loopback：round-trip、二进制安全、nil、服务端错误、大回复.
BOOST_AUTO_TEST_CASE(t_resp2_roundtrip_live) {
    BOOST_REQUIRE(g_prepared.load());
    std::string   host;
    std::uint16_t port = 0;
    bool          from_env = false;
    BOOST_REQUIRE(LiveRedisAddr(host, port, from_env));

#ifdef BBT_INFRA_STRINGENT_DEBUG
    redis_detail::ResetRedisBindingTotalsForTest();
#endif
    auto c = NewCotcpClient(host, port, /*connect=*/false);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    // 目标不可达：未显式给地址时按 skip 处理（与 redis.live 约定一致）。显式
    // Connect 真实建连——成功即 TCP 已建立。
    {
        std::atomic<bool> reachable{false};
        BOOST_REQUIRE(RunInCoroutine([&] {
            reachable.store(static_cast<bool>(cli->Connect(Opt(3000))));
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

#ifdef BBT_INFRA_STRINGENT_DEBUG
    const auto totals = redis_detail::RedisBindingTotalsForTest();
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
#endif

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

// 服务端 ERROR 帧（RemoteError）是「完整帧已消费、RESP 流仍同步」的唯一读失败：
// 连接健康可续用，不得拆。同一连接上「错误帧 → PONG → OK」必须逐条成功，且不再
// dial（Accepted==1）。修复前错误后即拆连接，紧随的 Ping 落到 "redis: not
// connected"（TransportError）→ 本用例红。
BOOST_AUTO_TEST_CASE(t_server_error_preserves_connection) {
    BOOST_REQUIRE(g_prepared.load());
#ifdef BBT_INFRA_STRINGENT_DEBUG
    redis_detail::ResetRedisBindingTotalsForTest();
#endif

    RawPeer peer(ReplySequence(
        {RespError("WRONGTYPE bad"), RespStatus("PONG"), RespStatus("OK")}));
    auto    c = NewCotcpClient("127.0.0.1", peer.port);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    // 1) 服务端错误帧 → RemoteError(WRONGTYPE)：错误分类不得回退。
    std::optional<result<std::optional<std::string>>> g;
    BOOST_REQUIRE(RunInCoroutine([&] { g.emplace(cli->Get("k", Opt())); }));
    BOOST_REQUIRE(!*g);
    BOOST_CHECK(g->error().code == ErrorCode::RemoteError);
    BOOST_CHECK(g->error().domain_code == "WRONGTYPE");

    // 2) 同一连接续用：错误帧后 Ping 必须成功。
    std::optional<result<void>> p;
    BOOST_REQUIRE(RunInCoroutine([&] { p.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(*p);

    // 3) 同连接继续 Set 成功。
    std::optional<result<void>> s;
    BOOST_REQUIRE(RunInCoroutine([&] { s.emplace(cli->Set("k", "v", Opt())); }));
    BOOST_REQUIRE(*s);

    // 4) 无再 dial：同一连接服务三条命令。
    BOOST_CHECK_EQUAL(peer.Accepted(), 1u);
    BOOST_CHECK_EQUAL(peer.CommandsRead(), 3u);

#ifdef BBT_INFRA_STRINGENT_DEBUG
    // 5) 故障未拆连接：无 conns_broken、仅一次 dial。
    const auto snap = cli->ProbeSnapshot();
    BOOST_CHECK_EQUAL(snap.conns_broken, 0u);
    BOOST_CHECK_EQUAL(snap.dial_attempts, 1u);
#endif

    cli->Close();
    BOOST_CHECK(WaitUntil([&] { return cli->IsClosed(); }));
}

// 写侧：对端小接收窗 + 先不读 → 部分写/WouldBlock；恢复读取后写满并收到应答。
BOOST_AUTO_TEST_CASE(t_partial_write_then_full_delivery) {
    BOOST_REQUIRE(g_prepared.load());
#ifdef BBT_INFRA_STRINGENT_DEBUG
    redis_detail::ResetRedisBindingTotalsForTest();
#endif

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

#ifdef BBT_INFRA_STRINGENT_DEBUG
    const auto totals = redis_detail::RedisBindingTotalsForTest();
    BOOST_TEST_MESSAGE("write totals: write_rounds=" << totals.write_rounds
        << " partial_write_ops=" << totals.partial_write_ops
        << " hiredis_max_call_ns=" << totals.hiredis_max_call_ns);
    BOOST_CHECK(totals.write_rounds >= 2);
    BOOST_CHECK(totals.partial_write_ops >= 1);
#endif

    auto transport = cli->TransportForTest();
    BOOST_REQUIRE(transport);
    cli->Close();
    // Close() 的同步门禁验证物理资源；owner 批次的 IsClosed() 允许稍后落定。
    BOOST_CHECK(transport->IsClosed());
#ifdef BBT_INFRA_STRINGENT_DEBUG
    const auto closed_totals = redis_detail::RedisBindingTotalsForTest();
    BOOST_CHECK_EQUAL(closed_totals.readers_created, closed_totals.readers_freed);
#endif
    BOOST_CHECK(WaitUntil([&] { return cli->IsClosed(); }));
}

// 写侧阻塞 + deadline：命令未发完即 TimedOut；连接被判不可复用（不重发）。
BOOST_AUTO_TEST_CASE(t_write_side_deadline) {
    BOOST_REQUIRE(g_prepared.load());

    RawPeer peer(NeverRead(1500), /*listen_rcvbuf=*/4096);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    redis_detail::ResetRedisBindingTotalsForTest();
#endif
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
    // 真实状态同步（两种构建都成立）：故障连接已摘除、已登记请求归零。
    BOOST_REQUIRE(WaitUntil([&] {
        return cli->BindingStateSnapshot().registered == 0;
    }));
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_REQUIRE(WaitUntil([&] {
        const auto s = cli->ProbeSnapshot();
        return s.conns_broken >= 1 && s.dial_inflight == 0;
    }));
    const auto totals = redis_detail::RedisBindingTotalsForTest();
    BOOST_TEST_MESSAGE("write-deadline write_rounds=" << totals.write_rounds
        << " partial_write_ops=" << totals.partial_write_ops);
    BOOST_CHECK(totals.write_rounds >= 1);
    BOOST_CHECK(cli->ProbeSnapshot().conns_broken >= 1);
#endif

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
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 1);
#endif

    cli->Close();
    BOOST_CHECK(WaitUntil([&] { return cli->IsClosed(); }));
}

// 读侧 deadline，以及正式实现真实使用的 transport 上「无进展不忙轮询」探针。
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
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().conns_broken, 0);
#endif

    cli->Close();
    BOOST_CHECK(WaitUntil([&] { return cli->IsClosed(); }));
}

// Close 物理收口：在途命令随 Close 落定 Closed；Close 返回当刻 fd 物理关闭、reader
// 恰好释放；晚到终态不覆盖首发终态。
BOOST_AUTO_TEST_CASE(t_owner_close_physical_collect) {
    BOOST_REQUIRE(g_prepared.load());
#ifdef BBT_INFRA_STRINGENT_DEBUG
    redis_detail::ResetRedisBindingTotalsForTest();
#endif

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
    // 真实状态同步：当前代际连接槽已发布（dial 已完成，命令在途）。
    BOOST_REQUIRE(WaitUntil([&] { return cli->BindingStateSnapshot().has_conn; }));
    SleepMs(150);
    BOOST_CHECK(!ready.load());

    auto tcp = cli->TransportForTest();
    BOOST_REQUIRE(tcp != nullptr);
    const int fd = W::NativeFdForTest(*tcp);
    BOOST_REQUIRE(fd >= 0);
    BOOST_CHECK(FdOpen(fd));
    BOOST_CHECK(!tcp->IsClosed());
    BOOST_CHECK(cli->BindingStateSnapshot().registered >= 1);

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

#ifdef BBT_INFRA_STRINGENT_DEBUG
    const auto totals_now = redis_detail::RedisBindingTotalsForTest();
    BOOST_TEST_MESSAGE("close totals: readers_created=" << totals_now.readers_created
        << " readers_freed=" << totals_now.readers_freed
        << " conns_created=" << totals_now.conns_created
        << " conns_destroyed=" << totals_now.conns_destroyed);
    BOOST_CHECK_EQUAL(totals_now.readers_created, totals_now.readers_freed);
#endif

    // 物理收口不依赖 owner 批次退出：真实状态「已登记请求归零」在两种构建都成立。
    BOOST_REQUIRE(WaitUntil([&] {
        return cli->BindingStateSnapshot().registered == 0;
    }));
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_REQUIRE(WaitUntil([&] { return cli->ProbeSnapshot().dial_inflight == 0; }));
#endif
    BOOST_REQUIRE(WaitUntil([&] { return cli->IsClosed(); }));
#ifdef BBT_INFRA_STRINGENT_DEBUG
    // owner 恢复后才记录晚到终态被拒；独立有界等待，不推迟上面的物理收口检查。
    BOOST_REQUIRE(WaitUntil([&] { return cli->ProbeSnapshot().finish_conflicts >= 1; }));
    const auto after = cli->ProbeSnapshot();
    BOOST_CHECK(after.finish_conflicts >= 1); // 晚到终态被拒
#endif
    BOOST_CHECK(out->error().code == ErrorCode::Closed); // 首发终态不被晚到者覆盖

    // 收口配对：释放测试侧最后引用后 conn 创建/释放恰好配对。
    tcp.reset();
    cli.reset();
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_REQUIRE(WaitUntil([&] {
        const auto t = redis_detail::RedisBindingTotalsForTest();
        return t.conns_created == t.conns_destroyed;
    }));
    const auto totals2 = redis_detail::RedisBindingTotalsForTest();
    BOOST_CHECK_EQUAL(totals2.readers_created, totals2.readers_freed);
#endif
}

// 重复/并发 Close：无 UAF、无死锁；物理收口恰好一次（fd 关一次、reader 释放配对）。
BOOST_AUTO_TEST_CASE(t_repeated_and_concurrent_close) {
    BOOST_REQUIRE(g_prepared.load());
#ifdef BBT_INFRA_STRINGENT_DEBUG
    redis_detail::ResetRedisBindingTotalsForTest();
#endif

    RawPeer peer(ReadThenHold(3000));
    auto    c = NewCotcpClient("127.0.0.1", peer.port);
    // 建连前置失败不能被误判成后续 Close 失败；不输出可能带敏感内容的错误文本。
    BOOST_REQUIRE_MESSAGE(c,
        "Redis client initialization failed: code="
        << (c ? 0 : static_cast<int>(c.error().code))
        << ", backend_code=" << (c ? 0 : c.error().backend_code));
    auto cli = std::move(c).value();

    std::optional<result<void>> out;
    std::atomic_bool            ready{false};
    bool                        succ = false;
    g_scheduler->RegistCoroutineTask([&] {
        out.emplace(cli->Ping(Opt(30000)));
        ready.store(true, std::memory_order_release);
    }, succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return peer.BytesRead() > 0; }));
    auto tcp = cli->TransportForTest();
    BOOST_REQUIRE(tcp != nullptr);
    const int fd = W::NativeFdForTest(*tcp);
    BOOST_REQUIRE(fd >= 0);

    // 并发 Close：多个线程同时调用，再加重复调用。每个调用者返回当刻都应观察
    // 到同一物理收口终态（IsClosed 为真、连接已物理关闭、registered 归零）。
    // 注：按对象状态断言（tcp->IsClosed/registered），不按 fd 号，避免 fd 号被
    // 运行时内部 dup/复用干扰。
    std::atomic_int          callers_closed{0};
    std::atomic_int          callers_phys_closed{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back([&] {
            for (int j = 0; j < 5; ++j)
                cli->Close();
            if (cli->IsClosed())
                callers_closed.fetch_add(1);
            if (tcp->IsClosed() && cli->BindingStateSnapshot().registered == 0)
                callers_phys_closed.fetch_add(1);
        });
    }
    for (auto& t : threads)
        t.join();
    cli->Close(); // 顺序重复
    // F-2：每个并发调用者返回当刻均已物理收口（无 WaitUntil）。
    BOOST_CHECK_EQUAL(callers_closed.load(), 4);
    BOOST_CHECK_EQUAL(callers_phys_closed.load(), 4);

    // 物理收口：fd 关一次（号码不被误关第二次），reader 释放配对。
    BOOST_CHECK(WaitUntil([&] { return tcp->IsClosed(); }));
    const auto probe = ProbeFd(fd);
    BOOST_CHECK_EQUAL(probe.rc, -1);
    const int victim = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_CHECK(victim >= 0);
    BOOST_CHECK(FdOpen(victim));
    ::close(victim);

    BOOST_REQUIRE(WaitUntil([&] { return cli->IsClosed(); }));

    // 完成握手：在途 Ping 协程按引用写本用例栈上的 out/cli；Close 只保证资源收口，
    // 不承诺调用方所有挂起协程已归来，故必须先等该协程归还，再释放测试侧最后引用。
    BOOST_REQUIRE(WaitUntil([&] { return ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(out);
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed); // Close 落定在途命令

    tcp.reset();
    cli.reset();
#ifdef BBT_INFRA_STRINGENT_DEBUG
    const auto totals = redis_detail::RedisBindingTotalsForTest();
    BOOST_TEST_MESSAGE("repeated-close totals: readers_created=" << totals.readers_created
        << " readers_freed=" << totals.readers_freed
        << " conns_created=" << totals.conns_created
        << " conns_destroyed=" << totals.conns_destroyed);
    BOOST_CHECK_EQUAL(totals.readers_created, totals.readers_freed);
    BOOST_REQUIRE(WaitUntil([&] {
        const auto t = redis_detail::RedisBindingTotalsForTest();
        return t.conns_created == t.conns_destroyed;
    }));
#endif
}

// F-1 DNS 段界限：主机名 dial 在途时 Close——封口后不再发起 socket/connect（对象
// 候选 fd 若已建立则同步收口）。确定性依赖 BlackholeDial 让 127.0.0.1 那一跳停留在
// connect 等待；无论 Close 落在 DNS 等待还是 connect 等待，进程级 socket 数在 Close
// 当刻都不得因在途 dial 新增残留（至多协程内部 dup 一个，见数值用例说明）。
BOOST_AUTO_TEST_CASE(t_b1_close_during_inflight_dial_hostname) {
    BOOST_REQUIRE(g_prepared.load());

    BlackholeDial hole;
    const auto    sockets_before = CountSocketFds();
#ifdef BBT_INFRA_STRINGENT_DEBUG
    redis_detail::ResetRedisBindingTotalsForTest();
#endif

    auto c = NewCotcpClient("localhost", hole.port, /*connect=*/false);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::optional<result<void>> out;
    std::atomic_bool            ready{false};
    DialWaitProbe dial;
    dial.Install(cli);
    bool                        succ = false;
    g_scheduler->RegistCoroutineTask([&] {
        out.emplace(cli->Connect(Opt(30000)));
        ready.store(true, std::memory_order_release);
    }, succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(dial.Wait());

    cli->Close();

    // Close 返回当刻：至多协程内部 dup 一个残留 socket；对象候选 fd（若有）已收口。
    const auto sockets_after = CountSocketFds();
    BOOST_TEST_MESSAGE("b1(dial-inflight hostname) socket fds: before="
        << sockets_before << " after=" << sockets_after);
    BOOST_CHECK(sockets_after <= sockets_before + 1);
    BOOST_CHECK(cli->IsClosed());
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Closed);

    BOOST_REQUIRE(WaitUntil([&] { return ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(!*out);
    // Close 打断在途 dial：Connect 以 Closed 落定（dial 等待段被封口唤醒）。
    BOOST_CHECK(out->error().code == ErrorCode::Closed);

#ifdef BBT_INFRA_STRINGENT_DEBUG
    const auto totals = redis_detail::RedisBindingTotalsForTest();
    BOOST_CHECK_EQUAL(totals.commands_encoded, 0u);
    BOOST_CHECK_EQUAL(totals.write_rounds, 0u);
#endif

    // 协程恢复后内部 dup 释放，进程级 socket 数回基线。
    BOOST_REQUIRE(WaitUntil([&] { return CountSocketFds() <= sockets_before; }));
}

// B1：Close 必须覆盖在途 DialTCP。
//
// 确定性复现：对端 accept 队列被填满 ⇒ 新 connect 在内核里挂起 ⇒ owner 协程真实
// 挂在 DialTCP 的 connect 等待上（connect 等待段真实挂起接缝 dial.Wait() 是
// 可观察落点，非时序猜测、非 has_conn 占位）。
// 此时 Close：封口 owner 级 dial 等待登记打断 connect ⇒ Dial 以 Closed 返回且不留
// fd；不编码、不发送任何字节。
BOOST_AUTO_TEST_CASE(t_b1_close_during_inflight_dial) {
    BOOST_REQUIRE(g_prepared.load());

    BlackholeDial hole;
    const auto    sockets_before = CountSocketFds();
#ifdef BBT_INFRA_STRINGENT_DEBUG
    redis_detail::ResetRedisBindingTotalsForTest();
#endif

    auto c = NewCotcpClient("127.0.0.1", hole.port, /*connect=*/false);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    DialWaitProbe dial;
    dial.Install(cli);

    std::optional<result<void>> out;
    std::atomic_bool            ready{false};
    bool                        succ = false;
    g_scheduler->RegistCoroutineTask([&] {
        out.emplace(cli->Connect(Opt(30000)));
        ready.store(true, std::memory_order_release);
    }, succ);
    BOOST_REQUIRE(succ);

    BOOST_REQUIRE(dial.Wait());
    const auto sockets_during = CountSocketFds();
#ifdef BBT_INFRA_STRINGENT_DEBUG
    const auto during = cli->ProbeSnapshot();
    BOOST_TEST_MESSAGE("b1(dial-inflight) fillers=" << hole.FillerCount()
        << " dial_attempts=" << during.dial_attempts
        << " dial_inflight=" << during.dial_inflight
        << " has_conn=" << cli->BindingStateSnapshot().has_conn
        << " command_returned=" << ready.load());
    BOOST_REQUIRE_EQUAL(during.dial_inflight, 1u);
#else
    // Release 面：dial 在途前提由 dial.Wait()（connect 等待段真实挂起接缝）证明；
    // has_conn 由 PublishSlot 在 Dial 之前发布，不作为 dial 在途证据。
    BOOST_TEST_MESSAGE("b1(dial-inflight) fillers=" << hole.FillerCount()
        << " command_returned=" << ready.load());
#endif
    // dial 未返回 ⇒ 尚未发布任何 CoTCP；此时候选 fd 已建立并在 connect 等待中。
    BOOST_CHECK(cli->TransportForTest() == nullptr);
    BOOST_CHECK(!ready.load());

    cli->Close();

    // F-1 同步门禁（无 WaitUntil）：Close 返回当刻候选 dial fd 已物理收口——
    // 相对「dial 挂起中」的 socket 数恰好收回候选 fd 一个（during-after==1）；
    // 连接物理收口、registered 归零、IsClosed 为真。协程 poller 为 fd 等待建立
    // 的内部 dup（bbt/pollevent/Event.cc）随等待在协程恢复时才释放，不属本对象
    // 资源，故进程级 socket 数在 Close 当刻至多为基线 +1（对象候选 fd 已释放）。
    const auto sockets_after = CountSocketFds();
    BOOST_TEST_MESSAGE("b1(dial-inflight) socket fds at Close: before="
        << sockets_before << " during=" << sockets_during
        << " after=" << sockets_after);
    // 至多协程内部 dup 一个残留；相对 dial 挂起中至少收回候选 fd 一个。
    BOOST_CHECK(sockets_after <= sockets_before + 1);
    BOOST_CHECK(sockets_during > sockets_after);
    BOOST_CHECK(cli->BindingStateSnapshot().conn_closed);
    BOOST_CHECK_EQUAL(cli->BindingStateSnapshot().registered, 0u);
    BOOST_CHECK(cli->IsClosed());
    // 双关哨兵：Close 未把候选 fd 号二次 close 到新 socket 上。
    {
        const int victim = ::socket(AF_INET, SOCK_STREAM, 0);
        BOOST_CHECK(victim >= 0);
        BOOST_CHECK(FdOpen(victim));
        ::close(victim);
    }

    // Connect 以 Closed 落定（调用方协程恢复异步于 Close 返回）。
    BOOST_REQUIRE(WaitUntil([&] { return ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Closed);

#ifdef BBT_INFRA_STRINGENT_DEBUG
    const auto totals = redis_detail::RedisBindingTotalsForTest();
    BOOST_TEST_MESSAGE("b1(dial-inflight) after: conns_broken="
        << cli->ProbeSnapshot().conns_broken
        << " commands_encoded=" << totals.commands_encoded
        << " write_rounds=" << totals.write_rounds
        << " read_rounds=" << totals.read_rounds);
    // 「关闭后不得继续编码或发送」的直接证据：本用例内没有任何编码/写轮次。
    BOOST_CHECK_EQUAL(totals.commands_encoded, 0u);
    BOOST_CHECK_EQUAL(totals.write_rounds, 0u);
    BOOST_CHECK_EQUAL(totals.read_rounds, 0u);
#endif
    BOOST_CHECK_EQUAL(hole.FillerCount(), 32u);

    // 协程恢复后内部 dup 释放，进程级 socket 数回基线（运行时伪影，非对象资源）。
    BOOST_REQUIRE(WaitUntil([&] { return CountSocketFds() <= sockets_before; }));

    // 收口配对：释放测试侧最后一引用后，conn/reader 创建与释放恰好配对。
    cli.reset();
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_REQUIRE(WaitUntil([&] {
        const auto t = redis_detail::RedisBindingTotalsForTest();
        return t.conns_created == t.conns_destroyed;
    }));
    const auto totals2 = redis_detail::RedisBindingTotalsForTest();
    BOOST_CHECK_EQUAL(totals2.readers_created, totals2.readers_freed);
#endif
}

// B1 的另一半：DialTCP 已成功、尚未编码的瞬间关闭到达。测试接缝在 Connect 内执行
// （连接已发布、已物理建连），直接发起 Close()：此后不得编码、不得发送，且 Close
// 返回当刻 fd 已物理关闭。
BOOST_AUTO_TEST_CASE(t_b1_close_after_dial_no_encode_or_send) {
    BOOST_REQUIRE(g_prepared.load());

    RawPeer peer(NeverRead(1500));
#ifdef BBT_INFRA_STRINGENT_DEBUG
    redis_detail::ResetRedisBindingTotalsForTest();
#endif

    auto c = NewCotcpClient("127.0.0.1", peer.port, /*connect=*/false);
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

    std::optional<result<void>> conn_out;
    BOOST_REQUIRE(RunInCoroutine([&] { conn_out.emplace(cli->Connect(Opt(8000))); }));
    cli->SetPostDialGateForTest(nullptr);

    BOOST_REQUIRE(gated.load());
    BOOST_REQUIRE(!*conn_out);
    BOOST_CHECK(conn_out->error().code == ErrorCode::Closed);

    // 关闭后的命令一律被拒且不编码/不发送。
    std::optional<result<void>> out;
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt(2000))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);

#ifdef BBT_INFRA_STRINGENT_DEBUG
    const auto totals = redis_detail::RedisBindingTotalsForTest();
    BOOST_TEST_MESSAGE("b1(post-dial) commands_encoded=" << totals.commands_encoded
        << " write_rounds=" << totals.write_rounds
        << " peer_accepted=" << peer.Accepted()
        << " peer_bytes_read=" << peer.BytesRead());
    BOOST_CHECK_EQUAL(totals.commands_encoded, 0u);
    BOOST_CHECK_EQUAL(totals.write_rounds, 0u);
#endif
    BOOST_CHECK_EQUAL(peer.BytesRead(), 0u);
    BOOST_REQUIRE(WaitAccepted(peer, 1));

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
    BOOST_CHECK(cli->BindingStateSnapshot().conn_closed);

    dialed_tcp.reset();
    cli.reset();
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_REQUIRE(WaitUntil([&] {
        const auto t = redis_detail::RedisBindingTotalsForTest();
        return t.conns_created == t.conns_destroyed;
    }));
    const auto totals2 = redis_detail::RedisBindingTotalsForTest();
    BOOST_CHECK_EQUAL(totals2.readers_created, totals2.readers_freed);
#endif
}

// reconnect_on_new_command = true：故障之后的新命令允许一次新连接；已失败命令
// 不重发，连接确实被重建（Accepted/dial_attempts 证据）。
BOOST_AUTO_TEST_CASE(t_reconnect_new_command_true) {
    BOOST_REQUIRE(g_prepared.load());
#ifdef BBT_INFRA_STRINGENT_DEBUG
    redis_detail::ResetRedisBindingTotalsForTest();
#endif

    auto conn_idx = std::make_shared<std::atomic<int>>(0);
    RawPeer peer([conn_idx](const std::shared_ptr<PeerState>& st, int c) {
        const int i = conn_idx->fetch_add(1);
        if (i == 0) {
            // 第一条连接：读走命令即返回 ⇒ 对端 close（EOF）⇒ 客户端 TransportError。
            (void)st->ReadCommand(c);
            return;
        }
        // 第二条连接：正常回 +PONG。
        if (st->ReadCommand(c))
            (void)::send(c, "+PONG\r\n", 7, MSG_NOSIGNAL);
    });
    auto c = NewCotcpClient("127.0.0.1", peer.port, /*start=*/true,
                            /*reconnect_on_new_command=*/true);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::optional<result<void>> first;
    BOOST_REQUIRE(RunInCoroutine([&] { first.emplace(cli->Ping(Opt(4000))); }));
    BOOST_REQUIRE(!*first);
    BOOST_CHECK(first->error().code == ErrorCode::TransportError);

    // 故障后的新命令：允许尝试一次新连接并成功。
    std::optional<result<void>> second;
    BOOST_REQUIRE(RunInCoroutine([&] { second.emplace(cli->Ping(Opt(4000))); }));
    BOOST_REQUIRE(second);
    BOOST_REQUIRE(*second);   // PONG

    // 新命令建了新连接；已失败命令未被重发（两条命令各发一次）。
    BOOST_CHECK_EQUAL(peer.Accepted(), 2);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 2);
#endif
    BOOST_CHECK_EQUAL(peer.CommandsRead(), 2);

    cli->Close();
    BOOST_CHECK(cli->IsClosed());
}

// reconnect_on_new_command = true 且目标不可达：显式 Connect 失败进入 Failed 后，
// 每条新命令仍只尝试一次重建，失败直接暴露（不循环），空转次数由 dial_attempts 精确刻画。
BOOST_AUTO_TEST_CASE(t_reconnect_dial_failure_exposed) {
    BOOST_REQUIRE(g_prepared.load());

    // 127.0.0.1:1 惯例无监听：connect 拒绝。
    auto c = NewCotcpClient("127.0.0.1", 1, /*connect=*/false,
                            /*reconnect_on_new_command=*/true);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    // 首次显式 Connect 失败 → Failed（拨号 1 次）。
    std::optional<result<void>> conn_out;
    BOOST_REQUIRE(
        RunInCoroutine([&] { conn_out.emplace(cli->Connect(Opt(4000))); }));
    BOOST_REQUIRE(!*conn_out);
    BOOST_CHECK(conn_out->error().code == ErrorCode::TransportError);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Failed);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 1u);
#endif

    for (int i = 0; i < 3; ++i) {
        std::optional<result<void>> out;
        BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt(4000))); }));
        BOOST_REQUIRE(!*out);
        BOOST_CHECK(out->error().code == ErrorCode::TransportError);
    }
    // 三次新命令 = 三次重建尝试（每条恰好一次，无后台循环）+ 首次 Connect = 4 次。
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 4u);
#endif
    // 失败被暴露、不进入终态：仍可用显式 Connect 再试。
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Failed);

    cli->Close();
    BOOST_CHECK(cli->IsClosed());
}

// F-3：reconnect_on_new_command=true 时，仅「故障之后提交」的新命令允许重建连接；
// 故障前已排队（尚未发送）的命令不得跨连接迁移——以 TransportError 落定，且服务端
// 不读到其字节。dial_attempts 只由「首条 + 故障后新命令」计入。
BOOST_AUTO_TEST_CASE(t_reconnect_only_for_new_commands_after_failure) {
    BOOST_REQUIRE(g_prepared.load());

    auto release_close = std::make_shared<std::atomic_bool>(false);
    auto conn_idx      = std::make_shared<std::atomic<int>>(0);
    RawPeer peer([conn_idx, release_close](const std::shared_ptr<PeerState>& st,
                                           int c) {
        const int i = conn_idx->fetch_add(1);
        if (i == 0) {
            if (!st->ReadCommand(c))
                return;
            // 保持命令 1 在途：等测试信号后返回 ⇒ 对端 close（EOF）。
            pollfd p{c, POLLIN, 0};
            for (int k = 0; k < 1000 && !release_close->load(); ++k)
                ::poll(&p, 1, 10);
            return;
        }
        if (st->ReadCommand(c))
            (void)::send(c, "+PONG\r\n", 7, MSG_NOSIGNAL);
    });
    auto c = NewCotcpClient("127.0.0.1", peer.port, /*start=*/true,
                            /*reconnect_on_new_command=*/true);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    auto spawn_ping = [](const std::shared_ptr<CoRedisCliImpl>& client,
                         std::optional<result<void>>* out,
                         bbt::core::thread::CountDownLatch* done, int budget_ms) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [client, out, done, budget_ms] {
                out->emplace(client->Ping(Opt(budget_ms)));
                if (done)
                    done->Down();
            },
            succ);
        return succ;
    };

    // 命令 1：dial 建连后发送并在途（对端持有不回、不关）。
    std::optional<result<void>>              first;
    bbt::core::thread::CountDownLatch        first_done{1};
    BOOST_REQUIRE(spawn_ping(cli, &first, &first_done, 8000));
    BOOST_REQUIRE(WaitUntil([&] { return peer.CommandsRead() >= 1; }, 8000));

    // 命令 2：故障前提交并排队（owner 仍挂在命令 1）。
    std::optional<result<void>>              second;
    bbt::core::thread::CountDownLatch        second_done{1};
    BOOST_REQUIRE(spawn_ping(cli, &second, &second_done, 8000));
    // 真实状态同步：故障前提交的命令已入队（Release 面同样成立）。
    BOOST_REQUIRE(WaitUntil([&] { return cli->BindingStateSnapshot().queued >= 1; }, 8000));

    // 触发故障：对端关闭连接 ⇒ 命令 1 读侧 EOF。
    release_close->store(true);
    BOOST_REQUIRE(first_done.WaitTimeout(8000) == 0);
    BOOST_REQUIRE(first);
    BOOST_REQUIRE(!*first);
    BOOST_CHECK(first->error().code == ErrorCode::TransportError);

    // 故障前排队的命令 2 必须以 TransportError 落定（不迁移到新连接）。
    BOOST_REQUIRE(second_done.WaitTimeout(8000) == 0);
    BOOST_REQUIRE(second);
    BOOST_REQUIRE(!*second);
    BOOST_CHECK(second->error().code == ErrorCode::TransportError);

    // 故障后提交的命令 3：允许尝试一次新连接并成功。
    std::optional<result<void>> third;
    BOOST_REQUIRE(spawn_ping(cli, &third, nullptr, 4000));
    BOOST_REQUIRE(WaitUntil([&] { return third.has_value(); }, 6000));
    BOOST_REQUIRE(third);
    BOOST_REQUIRE(*third);

    // 命令 2 从未发送：服务端只读到命令 1 与命令 3；拨号共 2 次（命令 1、命令 3）。
    BOOST_CHECK_EQUAL(peer.CommandsRead(), 2);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(cli->ProbeSnapshot().dial_attempts, 2);
#endif
    BOOST_CHECK_EQUAL(peer.Accepted(), 2);

    cli->Close();
    BOOST_CHECK(cli->IsClosed());
}

// 在途命令（owner 挂起在可读等待）下 Close() 同步落定：Close 返回当刻即
// IsClosed、fd 已关、已登记 op 归零，不需等待 owner 批次退出。
BOOST_AUTO_TEST_CASE(t_close_inflight_isolated_synchronous) {
    BOOST_REQUIRE(g_prepared.load());

    RawPeer peer(ReadThenHold(3000));
    auto    c = NewCotcpClient("127.0.0.1", peer.port);
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
    BOOST_REQUIRE(WaitUntil([&] { return peer.BytesRead() > 0; }));
    auto tcp = cli->TransportForTest();
    BOOST_REQUIRE(tcp != nullptr);
    BOOST_CHECK(!tcp->IsClosed());

    cli->Close();

    // 同步门禁（不 WaitUntil）：Close 返回当刻物理收口与逻辑收口均已成立。
    BOOST_CHECK(cli->IsClosed());
    BOOST_CHECK(tcp->IsClosed());
    BOOST_CHECK_EQUAL(cli->BindingStateSnapshot().registered, 0u);

    // 在途命令以 Closed 落定。
    BOOST_REQUIRE(WaitUntil([&] { return ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
}

// F-1a 回归：候选 fd 的物理 close 必须在关闭门（CloseWaiters::m_mtx）内、且先于
// 封口排空谓词（registrations==0 && handoffs==0）归零——保证并发 Close/Disconnect
// 返回当刻候选 fd 已 close（ICoCloseable「返回即物理释放」）。
//
// 确定性（不依赖 sleep/时序）：seal 与 dial 侧收口两线程并发竞争，在 seal 返回当刻
// 断言候选 fd 必已 close。修复后恒成立；旧实现「先 Disarm/TakeCandidate 取出 fd 再
// 下一语句 close」会让该断言可能失败（正是 F-1a 的窗口）。
BOOST_AUTO_TEST_CASE(t_f1a_candidate_close_in_gate) {
    BOOST_REQUIRE(g_prepared.load());
    using bbt::infra::detail::CloseWaiters;

    // 1) 候选槽路径（立即 connect 失败 / 等待失败）：CloseCandidateInGate。
    for (int iter = 0; iter < 256; ++iter) {
        auto cw = std::make_shared<CloseWaiters>();
        int  s  = -1;
        BOOST_REQUIRE(cw->CreateCandidate([&] {
            s = ::socket(AF_INET, SOCK_STREAM, 0);
            return s;
        }));
        BOOST_REQUIRE(s >= 0);

        bool        fd_closed_at_seal = true;
        std::thread owner([&] {
            cw->SealWakeAndDrainRegistrations();
            fd_closed_at_seal = !FdOpen(s);   // seal 返回当刻候选 fd 必须已 close
        });
        std::thread dial([&] { (void)cw->CloseCandidateInGate(); });
        dial.join();
        owner.join();
        BOOST_CHECK_MESSAGE(fd_closed_at_seal,
            "F-1a candidate path: seal returned with candidate fd still open, iter=" << iter);
        BOOST_CHECK_MESSAGE(!FdOpen(s),
            "F-1a candidate path: candidate fd leaked, iter=" << iter);
        // 双关哨兵：fd 号未被二次 close 到复用 socket 上。
        const int victim = ::socket(AF_INET, SOCK_STREAM, 0);
        BOOST_CHECK(victim >= 0);
        BOOST_CHECK(FdOpen(victim));
        ::close(victim);
    }

    // 2) 已移交在途路径（成功 connect 后 SO_ERROR 失败）：CloseHandoffFdAndEndHandoff。
    for (int iter = 0; iter < 256; ++iter) {
        auto cw = std::make_shared<CloseWaiters>();
        int  s  = -1;
        BOOST_REQUIRE(cw->CreateCandidate([&] {
            s = ::socket(AF_INET, SOCK_STREAM, 0);
            return s;
        }));
        BOOST_REQUIRE(s >= 0);
        int owned = -1;
        BOOST_REQUIRE(cw->BeginHandoff(&owned));
        BOOST_CHECK_EQUAL(owned, s);

        bool        fd_closed_at_seal = true;
        std::thread owner([&] {
            cw->SealWakeAndDrainRegistrations();
            fd_closed_at_seal = !FdOpen(s);
        });
        std::thread dial([&] { cw->CloseHandoffFdAndEndHandoff(owned); });
        dial.join();
        owner.join();
        BOOST_CHECK_MESSAGE(fd_closed_at_seal,
            "F-1a handoff path: seal returned with handed-off fd still open, iter=" << iter);
        BOOST_CHECK_MESSAGE(!FdOpen(s),
            "F-1a handoff path: handed-off fd leaked, iter=" << iter);
    }

    // 3) 域级：受管 dial 打到不可达数值地址（立即 connect 失败路径）后候选 fd 已收口，
    //    socket 数回基线（无残留、无双关）。取一个当前无监听者的 loopback 端口。
    {
        int probe = ::socket(AF_INET, SOCK_STREAM, 0);
        BOOST_REQUIRE(probe >= 0);
        sockaddr_in a{};
        a.sin_family      = AF_INET;
        a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        a.sin_port        = 0;
        BOOST_REQUIRE(::bind(probe, reinterpret_cast<sockaddr*>(&a), sizeof(a)) == 0);
        socklen_t alen = sizeof(a);
        BOOST_REQUIRE(::getsockname(probe, reinterpret_cast<sockaddr*>(&a), &alen) == 0);
        const std::uint16_t dead_port = ntohs(a.sin_port);
        ::close(probe);

        const auto sockets_before = CountSocketFds();
        auto c = NewCotcpClient("127.0.0.1", dead_port, /*connect=*/false);
        BOOST_REQUIRE(c);
        auto cli = std::move(c).value();

        std::optional<result<void>> out;
        std::atomic_bool            ready{false};
        bool                        succ = false;
        g_scheduler->RegistCoroutineTask([&] {
            out.emplace(cli->Connect(Opt(5000)));
            ready.store(true, std::memory_order_release);
        }, succ);
        BOOST_REQUIRE(succ);
        BOOST_REQUIRE(WaitUntil([&] { return ready.load(std::memory_order_acquire); }));
        BOOST_REQUIRE(!*out);
        BOOST_CHECK(out->error().code == ErrorCode::TransportError);
        // 立即失败路径无 fd 等待、无内部 dup：候选 fd 已由关闭门原语收口，socket 数
        // 不得高于基线（体现「失败路径 close 先于排空谓词归零」的可见后果）。
        BOOST_CHECK_MESSAGE(CountSocketFds() <= sockets_before,
            "F-1a domain: managed dial failure left a residual socket fd (before="
                << sockets_before << " after=" << CountSocketFds() << ")");
        cli->Close();
        BOOST_CHECK(cli->IsClosed());
    }
}

BOOST_AUTO_TEST_SUITE_END()
