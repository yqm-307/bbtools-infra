// Issue #6 单元验收（不依赖真实 Redis/容器）：
//
//   t_setup_scheduler             — 启动 Scheduler（共享 executor 来源）。
//   t_create_before_scheduler     — 基线差异固定：Scheduler 未 Start 时
//                                   IsRunning/generation 已就绪，Create
//                                   成功；RuntimeUnavailable 由未 Start
//                                   client 路径覆盖。
//   t_config_validation           — 非法装配逐项 InvalidArgument。
//   t_command_prechecks           — 非协程 InvalidContext；未 Start
//                                   RuntimeUnavailable；空键/空表
//                                   InvalidArgument。
//   t_fake_redis_commands         — FakeRedis 预置 RESP 应答：PING/GET
//                                   （bulk+nil）/SET/EXISTS/DEL 全链路
//                                   解码与服务端错误映射（RemoteError）。
//   t_capacity_overloaded         — 静默服务端占满 inflight 与 pending：
//                                   确定性 Overloaded。
//   t_deadline_and_cancel         — 静默服务端下超时 TimedOut、取消
//                                   Cancelled；取消后再 close 不覆盖
//                                   首个逻辑终态。
//   t_refused_connect             — 连接拒绝 → TransportError，连接失败
//                                   后队列命令统一失败。
//   t_close_during_inflight       — 在途命令随 owner close 落定 Closed，
//                                   WaitClosed → Closed（operation/
//                                   connection 归零）。
//   t_waitclosed_contention       — 单等待位：并发第二个 AlreadyWaiting。
//   t_thread_count_stable         — 4 个 client 不新增线程（共享
//                                   executor，无 io_thread）。
//   t_invalid_context_plain       — 普通线程直接调命令 → InvalidContext。
//
// 同步纪律与 http 套件一致：CountDownLatch + WaitUntil 有界等待，
// 不 sleep 假设时序；FakeRedis/静默服务端在协程内经 Hook 驱动，
// 不占额外线程。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <deque>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/CoRedisCli.hpp>

using namespace bbt::infra;
using bbt::coroutine::Deadline;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

constexpr int kBudgetMs = 15000;

RedisClientConfig MakeConfig(std::string host, std::uint16_t port,
                             std::size_t inflight = 8,
                             std::size_t queue = 8) {
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

// 协程内执行并限时等待；任何一步失败返回 false（测试不得无限挂住）。
template <class F>
bool RunInCoroutine(F&& f, int budget_ms = kBudgetMs) {
    bbt::core::thread::CountDownLatch done{1};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [fn = std::forward<F>(f), &done]() mutable {
            fn();
            done.Down();
        },
        succ);
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

// 非协程线程上的定长睡眠：必须直达内核 syscall。std::this_thread::sleep_for
// 在本工具链上经 libc nanosleep，而 coroutine Hook 全进程拦截 nanosleep 并
// 断言“必须在协程上下文”——测试线程命中即断言+空指针解引用（基线行为）。
void SleepMs(int ms) {
    timespec req{ms / 1000, static_cast<long>(ms % 1000) * 1000000L};
    timespec rem{};
    while (::syscall(SYS_nanosleep, &req, &rem) != 0 && errno == EINTR)
        req = rem;
}

// 极简 RESP multibulk 应答机：accept 一条连接后按预置队列逐命令弹出
// 回复（队列空则静默不回——命令自然在途/排队直到超时或被关闭）。
// 只解析 *N\r\n$len\r\n<arg>\r\n 边界，足够本切片命令使用。
class FakeRedis {
public:
    explicit FakeRedis(std::deque<std::string> replies,
                       bool silent = false) {
        int lfd = ::socket(AF_INET, SOCK_STREAM, 0);
        BOOST_REQUIRE(lfd >= 0);
        int one = 1;
        ::setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        timeval tv{10, 0};
        ::setsockopt(lfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
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
        port = ntohs(addr.sin_port);

        auto rs = std::make_shared<std::deque<std::string>>(std::move(replies));
        auto dn = done;
        auto sd = saw_data;
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [lfd, rs, dn, sd, silent] {
                int c = ::accept(lfd, nullptr, nullptr);
                ::close(lfd);
                if (c >= 0 && !silent)
                    Serve(c, rs, sd);
                else if (c >= 0)
                    HoldSilent(c, sd);
                dn->store(true);
            },
            succ);
        if (!succ) {
            ::close(lfd);
            done->store(true);
        }
        BOOST_REQUIRE(succ);
    }
    ~FakeRedis() { WaitUntil([dn = done] { return dn->load(); }, 10000); }

    std::uint16_t port = 0;
    std::shared_ptr<std::atomic_bool> done{
        std::make_shared<std::atomic_bool>(false)};
    // 服务端已收到任意字节 ⇒ 客户端连接建立且首个命令已真正发出（在途）。
    std::shared_ptr<std::atomic_bool> saw_data{
        std::make_shared<std::atomic_bool>(false)};

private:
    // 静默持有：读走数据但从不回包直到对端关闭（inflight/排队由此占住）。
    static void HoldSilent(int c, const std::shared_ptr<std::atomic_bool>& sd) {
        char buf[256];
        for (;;) {
            const ssize_t n = ::recv(c, buf, sizeof(buf), 0);
            if (n <= 0)
                break;
            sd->store(true);
        }
        ::close(c);
    }

    // 从缓冲解析一条完整 multibulk 命令；不足返回 false。
    static bool PopCommand(std::string& buf) {
        if (buf.empty() || buf[0] != '*')
            return false;
        const auto nl = buf.find("\r\n");
        if (nl == std::string::npos)
            return false;
        int argc = 0;
        try {
            argc = std::stoi(buf.substr(1, nl - 1));
        } catch (...) {
            return false;
        }
        std::size_t pos = nl + 2;
        for (int i = 0; i < argc; ++i) {
            if (pos >= buf.size() || buf[pos] != '$')
                return false;
            const auto nl2 = buf.find("\r\n", pos);
            if (nl2 == std::string::npos)
                return false;
            std::size_t len = 0;
            try {
                len = static_cast<std::size_t>(
                    std::stoul(buf.substr(pos + 1, nl2 - pos - 1)));
            } catch (...) {
                return false;
            }
            pos = nl2 + 2;
            if (buf.size() < pos + len + 2)
                return false;
            pos += len + 2;
        }
        buf.erase(0, pos);
        return true;
    }

    static void Serve(int c, const std::shared_ptr<std::deque<std::string>>& rs,
                      const std::shared_ptr<std::atomic_bool>& sd) {
        std::string buf;
        char tmp[4096];
        for (;;) {
            const ssize_t n = ::recv(c, tmp, sizeof(tmp), 0);
            if (n <= 0)
                break;
            sd->store(true);
            buf.append(tmp, static_cast<std::size_t>(n));
            while (PopCommand(buf)) {
                if (rs->empty())
                    continue;   // 无预置回复：命令被吞掉不回包
                const std::string r = rs->front();
                rs->pop_front();
                if (::send(c, r.data(), r.size(), MSG_NOSIGNAL) <= 0) {
                    ::close(c);
                    return;
                }
            }
        }
        ::close(c);
    }
};

std::size_t ThreadCount() {
    std::ifstream f("/proc/self/status");
    std::string line;
    while (std::getline(f, line))
        if (line.rfind("Threads:", 0) == 0)
            return static_cast<std::size_t>(std::stoul(line.substr(8)));
    return 0;
}

// client 工厂：成功则返回托管对象。
result<std::shared_ptr<CoRedisCli>> NewClient(const RedisClientConfig& cfg,
                                            bool start = true) {
    auto c = CoRedisCli::Create(cfg);
    if (!c)
        return result<std::shared_ptr<CoRedisCli>>::err(
            std::move(c).error());
    auto cli = std::move(c).value();
    if (start) {
        auto st = cli->Start();
        if (!st)
            return result<std::shared_ptr<CoRedisCli>>::err(
                std::move(st).error());
    }
    return result<std::shared_ptr<CoRedisCli>>::ok(std::move(cli));
}

std::atomic_bool g_prepared{false};

} // namespace

// 套件收尾把 Scheduler 单例的所有权释放走漏：~Scheduler 会经 Stop()→
// Processer::Stop→sleep_for→nanosleep 命中 coroutine 自身导出的 Hook 符号，
// 在非协程线程上断言后空指针解引用（上游基线竞态，测试侧不可修复）。
// release 后静态 UPtr 为空，进程退出不再触发 Stop 路径。
struct SuiteTeardown {
    ~SuiteTeardown() { g_scheduler.release(); }
};
BOOST_GLOBAL_FIXTURE(SuiteTeardown);

BOOST_AUTO_TEST_SUITE(redis_unit)

// Scheduler 未启动时 Create → RuntimeUnavailable（CompletionSignal/executor
// 需要已启动的运行时）。注意基线差异：m_is_running 默认即为 true，IsRunning()
// 不能用来区分“已启动”，故本用例只断言 Create 的失败结果。
BOOST_AUTO_TEST_CASE(t_create_before_scheduler) {
    auto c = CoRedisCli::Create(MakeConfig("127.0.0.1", 6379));
    BOOST_REQUIRE(!c);
    BOOST_CHECK(c.error().code == ErrorCode::RuntimeUnavailable);
}

BOOST_AUTO_TEST_CASE(t_setup_scheduler) {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 2;
    cfg->m_cfg_stack_size        = 1024 * 256;
    g_scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(g_scheduler->IsRunning());
    g_prepared.store(true);
}

BOOST_AUTO_TEST_CASE(t_config_validation) {
    BOOST_REQUIRE(g_prepared.load());
    // 空 host / 0 port / 0 与超限容量逐项 InvalidArgument。
    auto bad1 = CoRedisCli::Create(MakeConfig("", 6379));
    BOOST_REQUIRE(!bad1);
    BOOST_CHECK(bad1.error().code == ErrorCode::InvalidArgument);
    auto bad2 = CoRedisCli::Create(MakeConfig("127.0.0.1", 0));
    BOOST_REQUIRE(!bad2);
    BOOST_CHECK(bad2.error().code == ErrorCode::InvalidArgument);
    auto bad3 = CoRedisCli::Create(MakeConfig("127.0.0.1", 6379, 0, 8));
    BOOST_REQUIRE(!bad3);
    BOOST_CHECK(bad3.error().code == ErrorCode::InvalidArgument);
    auto bad4 = CoRedisCli::Create(
        MakeConfig("127.0.0.1", 6379, 8, kRedisLimitsMaxQueue + 1));
    BOOST_REQUIRE(!bad4);
    BOOST_CHECK(bad4.error().code == ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_CASE(t_command_prechecks) {
    auto c = NewClient(MakeConfig("127.0.0.1", 1), /*start=*/false);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    // 未 Start：协程内调用 → RuntimeUnavailable。
    std::optional<result<void>> ping_out;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { ping_out.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(!*ping_out);
    BOOST_CHECK(ping_out->error().code == ErrorCode::RuntimeUnavailable);

    // 启动后再校验参数形态：空键/空表 InvalidArgument。
    BOOST_REQUIRE(cli->Start());
    std::optional<result<std::optional<std::string>>> get_out;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { get_out.emplace(cli->Get("", Opt())); }));
    BOOST_REQUIRE(!*get_out);
    BOOST_CHECK(get_out->error().code == ErrorCode::InvalidArgument);
    std::optional<result<std::uint64_t>> del_out;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { del_out.emplace(cli->Delete({}, Opt())); }));
    BOOST_REQUIRE(!*del_out);
    BOOST_CHECK(del_out->error().code == ErrorCode::InvalidArgument);

    cli->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);

    // 已关闭：新命令 → Closed。
    BOOST_REQUIRE(RunInCoroutine(
        [&] { ping_out.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(!*ping_out);
    BOOST_CHECK(ping_out->error().code == ErrorCode::Closed);
}

BOOST_AUTO_TEST_CASE(t_invalid_context_plain) {
    // 普通线程（非协程）调命令：不启动协程直接调用。
    auto c = NewClient(MakeConfig("127.0.0.1", 6379));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    auto res = cli->Ping(Opt());
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::InvalidContext);
    cli->RequestClose();
    BOOST_REQUIRE(RunInCoroutine([&] {
        return cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {});
    }));
}

BOOST_AUTO_TEST_CASE(t_fake_redis_commands) {
    FakeRedis fake({"+PONG\r\n", "$5\r\nhello\r\n", "$-1\r\n", "+OK\r\n",
                    ":1\r\n", ":2\r\n", "-WRONGTYPE bad type\r\n"});
    auto c = NewClient(MakeConfig("127.0.0.1", fake.port));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::optional<result<void>> ping;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { ping.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(ping && *ping);

    std::optional<result<std::optional<std::string>>> get;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { get.emplace(cli->Get("k1", Opt())); }));
    BOOST_REQUIRE(get && *get);
    BOOST_REQUIRE(get->value().has_value());
    BOOST_CHECK_EQUAL(*get->value(), "hello");

    std::optional<result<std::optional<std::string>>> get_nil;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { get_nil.emplace(cli->Get("k2", Opt())); }));
    BOOST_REQUIRE(get_nil && *get_nil);
    BOOST_CHECK(!get_nil->value().has_value());

    std::optional<result<void>> set;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { set.emplace(cli->Set("k", "v", Opt())); }));
    BOOST_REQUIRE(set && *set);

    std::optional<result<bool>> ex;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { ex.emplace(cli->Exists("k", Opt())); }));
    BOOST_REQUIRE(ex && *ex);
    BOOST_CHECK(ex->value());

    std::optional<result<std::uint64_t>> del;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { del.emplace(cli->Delete({"k"}, Opt())); }));
    BOOST_REQUIRE(del && *del);
    BOOST_CHECK_EQUAL(del->value(), 2u);

    // 服务端错误回复 → RemoteError，domain_code 为错误串首词。
    std::optional<result<std::optional<std::string>>> bad;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { bad.emplace(cli->Get("listk", Opt())); }));
    BOOST_REQUIRE(bad && !*bad);
    BOOST_CHECK(bad->error().code == ErrorCode::RemoteError);
    BOOST_CHECK_EQUAL(bad->error().domain_code, "WRONGTYPE");

    cli->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
}

BOOST_AUTO_TEST_CASE(t_capacity_overloaded) {
    // 确定性容量验收：先提交一个长超时命令并等 saw_data——服务端见到字节
    // 即证明连接建立且该命令真正在途（inflight=1 被占住）。此后容量只剩
    // queue=2：再提交 3 个命令，恰好第 3 个 Overloaded，其余随 close 落定。
    FakeRedis silent({}, /*silent=*/true);
    auto c = NewClient(MakeConfig("127.0.0.1", silent.port, 1, 2));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::atomic_int    overloaded{0};
    std::atomic_int    other_err{0};
    std::atomic_int    done{0};
    auto submit = [&](int budget_ms) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [&] {
                auto r = cli->Ping(Opt(budget_ms));
                if (!r) {
                    if (r.error().code == ErrorCode::Overloaded)
                        overloaded.fetch_add(1);
                    else
                        other_err.fetch_add(1);
                }
                done.fetch_add(1);
            },
            succ);
        BOOST_REQUIRE(succ);
    };

    submit(30000);
    BOOST_REQUIRE(WaitUntil([&] { return silent.saw_data->load(); }, 10000));
    for (int i = 0; i < 3; ++i)
        submit(30000);

    // 恰好 1 个 Overloaded：1 在途 + 2 排队 = 3 个占满容量，第 4 个被拒。
    BOOST_REQUIRE(WaitUntil([&] { return overloaded.load() == 1; }, 12000));
    BOOST_CHECK_EQUAL(overloaded.load(), 1);

    // 其余命令仍在等回包/排队：以 owner close 收口，应全部 Closed。
    cli->RequestClose();
    BOOST_REQUIRE(WaitUntil([&] { return done.load() == 4; }));
    BOOST_CHECK_EQUAL(overloaded.load(), 1);
    BOOST_CHECK_EQUAL(other_err.load(), 3);

    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
    BOOST_CHECK(cli->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_deadline_and_cancel) {
    FakeRedis silent({}, /*silent=*/true);
    auto c = NewClient(MakeConfig("127.0.0.1", silent.port, 4, 4));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    // deadline 300ms：命令已在途但无回包 → TimedOut。
    std::optional<result<void>> out;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { out.emplace(cli->Ping(Opt(300))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TimedOut);

    // cancel：首个逻辑终态 Cancelled；随后的 owner close 不覆盖。
    bbt::coroutine::CancellationSource src;
    CallOptions opt = Opt(10000);
    opt.cancel      = src.Token();
    std::optional<result<void>> out2;
    std::atomic_bool out2_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            out2.emplace(cli->Ping(opt));
            out2_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    SleepMs(50);
    src.RequestCancel();
    BOOST_REQUIRE(WaitUntil(
        [&] { return out2_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(!*out2);
    BOOST_CHECK(out2->error().code == ErrorCode::Cancelled);

    cli->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
    // 首个逻辑终态不被晚到的 close 覆盖。
    BOOST_REQUIRE(!*out2);
    BOOST_CHECK(out2->error().code == ErrorCode::Cancelled);
}

BOOST_AUTO_TEST_CASE(t_refused_connect) {
    // 127.0.0.1:1 惯例无监听：connect 拒绝 → 命令 TransportError，
    // 且失败统一落定（不悬挂）。
    auto c = NewClient(MakeConfig("127.0.0.1", 1));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    std::optional<result<void>> out;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { out.emplace(cli->Ping(Opt(8000))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TransportError);

    // 再次提交：重新尝试连接并再次失败（断开后可重试语义）。
    BOOST_REQUIRE(RunInCoroutine(
        [&] { out.emplace(cli->Ping(Opt(8000))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TransportError);

    cli->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
}

BOOST_AUTO_TEST_CASE(t_close_during_inflight) {
    FakeRedis silent({}, /*silent=*/true);
    auto c = NewClient(MakeConfig("127.0.0.1", silent.port, 4, 4));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::optional<result<void>> out;
    std::atomic_bool out_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            out.emplace(cli->Ping(Opt(30000)));
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    // 等命令真正在途：服务端见到字节 ⇒ 已发送且连接已建立（确定性）。
    BOOST_REQUIRE(
        WaitUntil([&] { return silent.saw_data->load(); }, 10000));

    cli->RequestClose();
    BOOST_REQUIRE(WaitUntil(
        [&] { return out_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);

    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    // WaitClosed → Closed：在途 operation 与连接均已归零。
    BOOST_CHECK(st.load() == CloseStatus::Closed);
    BOOST_CHECK(cli->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_waitclosed_contention) {
    auto c = NewClient(MakeConfig("127.0.0.1", 1));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::atomic_bool        a_waiting{false};
    std::atomic_bool        a_done{false};
    std::atomic<CloseStatus> a_status{};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            a_waiting.store(true);
            for (;;) {
                const auto st = cli->WaitClosed(
                    std::chrono::steady_clock::now() +
                        std::chrono::seconds(5),
                    {});
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
            for (int i = 0; i < 200 && !b_saw_already_waiting.load(); ++i) {
                const auto st = cli->WaitClosed(
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

    cli->RequestClose();
    BOOST_REQUIRE(WaitUntil([&] { return a_done.load(); }));
    BOOST_CHECK(a_status.load() == CloseStatus::Closed);
    BOOST_CHECK(cli->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_thread_count_stable) {
    const auto before = ThreadCount();
    BOOST_REQUIRE(before > 0);
    std::vector<std::shared_ptr<CoRedisCli>> clients;
    for (int i = 0; i < 4; ++i) {
        auto c = NewClient(MakeConfig("127.0.0.1", 1));
        BOOST_REQUIRE(c);
        clients.push_back(std::move(c).value());
    }
    // client/strand/连接管理不新增线程：线程数与 Scheduler 启动时一致。
    BOOST_CHECK_EQUAL(ThreadCount(), before);
    for (auto& cli : clients) {
        cli->RequestClose();
        std::atomic<CloseStatus> st{};
        BOOST_REQUIRE(RunInCoroutine([&] {
            st.store(cli->WaitClosed(
                std::chrono::steady_clock::now() +
                    std::chrono::seconds(10),
                {}));
        }));
        BOOST_CHECK(st.load() == CloseStatus::Closed);
    }
}

BOOST_AUTO_TEST_SUITE_END()
