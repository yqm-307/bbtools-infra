// Issue #6 单元验收（不依赖真实 Redis/容器）：
//
//   t_create_before_scheduler     — 运行时未初始化（从未 Start）时 Create →
//                                   RuntimeUnavailable（对象身份需要已
//                                   初始化的运行时）。
//   t_setup_scheduler             — 启动 Scheduler（共享 executor 来源）。
//                                  进程寿命运行时：整个可执行文件只 Start
//                                   一次，无 Stop/重启。
//   t_config_validation           — 非法装配逐项 InvalidArgument。
//   t_command_prechecks           — 非协程 InvalidContext；未 Connect
//                                   RuntimeUnavailable；空键/空表
//                                   InvalidArgument；Close 后 Closed。
//   t_no_lazy_connect_without_connect — 未 Connect 的命令返回错误且服务端无连接
//                                   （命令不得隐式建连）；显式 Connect 后才可用。
//   t_connect_disconnect_lifecycle — 显式生命周期状态机：Connect（幂等）/
//                                   ConnectStatus（只读）/Disconnect（同步收口、
//                                   可再次 Connect）/Close（终态、拒绝 Connect）。
//   t_fake_redis_commands         — FakeRedis 预置 RESP 应答：PING/GET
//                                   （bulk+nil）/SET/EXISTS/DEL 全链路
//                                   解码与服务端错误映射（RemoteError）。
//   t_capacity_overloaded         — 静默服务端占满 inflight 与 pending：
//                                   确定性 Overloaded。
//   t_deadline_only               — 静默服务端下超时 TimedOut；Close 不覆盖
//                                   首个逻辑终态。
//   t_refused_connect             — 连接拒绝 → Connect 报 TransportError 且状态
//                                   Failed；后续命令 sticky TransportError；
//                                   再次显式 Connect 仍可尝试（不进入终态）。
//   t_close_during_inflight       — 在途命令随 owner Close() 落定 Closed；
//                                   Close() 返回即封口且在途 operation/
//                                   connection 归零（IsClosed）。
//   t_thread_count_stable         — 4 个 client 不新增线程（共享
//                                   executor，无 io_thread）。
//   t_invalid_context_plain       — 普通线程直接调命令 → InvalidContext。
//
// 显式生命周期（不做 Lazy）：连接只在协程内显式 Connect 时建立；Disconnect 不是
// Close 别名（同步收口连接但保留配置、可再次 Connect）；Close 是 ICoCloseable 终态。
// 关闭语义（进程寿命修订）：RequestClose/WaitClosed/CloseStatus/取消令牌全部删除——
// Close() 幂等、任意线程可调用，返回即在途归零、连接物理回收；IsClosed() 只读查询。
// 每个用例结束显式 Close() 并断言 IsClosed()。
//
// 同步纪律与 http 套件一致：CountDownLatch + WaitUntil 有界等待，
// 不 sleep 假设时序；FakeRedis/静默服务端在协程内经 Hook 驱动，
// 不占额外线程。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
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
#include <poll.h>
#include <sys/socket.h>
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

// 极简 RESP multibulk 应答机：accept 一条连接后按预置队列逐命令弹出
// 回复（队列空则静默不回——命令自然在途/排队直到超时或被关闭）。
// 只解析 *N\r\n$len\r\n<arg>\r\n 边界，足够本切片命令使用。
class FakeRedis {
public:
    explicit FakeRedis(std::deque<std::string> replies,
                       bool silent = false, int max_conns = 1) {
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

        // 多连接支持（显式生命周期会 Disconnect/重连同一实例）：accept 循环用有限
        // poll 检查 stop 标志，析构无需等待完整 accept 超时。
        auto rs  = std::make_shared<std::deque<std::string>>(std::move(replies));
        auto dn  = done;
        auto sd  = saw_data;
        auto stp = stop;
        auto lh  = std::make_shared<int>(lfd);
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [lh, rs, dn, sd, stp, silent, max_conns] {
                for (int i = 0; i < max_conns; ++i) {
                    pollfd p{*lh, POLLIN, 0};
                    const int pr = ::poll(&p, 1, 100);
                    if (pr < 0)
                        break;
                    if (pr == 0) {
                        if (stp->load())
                            break;
                        --i; // 尚未有连接：保持待 accept 数不变
                        continue;
                    }
                    const int c = ::accept(*lh, nullptr, nullptr);
                    if (c < 0)
                        break;
                    if (!silent)
                        Serve(c, rs, sd);
                    else
                        HoldSilent(c, sd);
                }
                ::close(*lh);
                dn->store(true);
            },
            succ);
        if (!succ) {
            ::close(lfd);
            done->store(true);
        }
        BOOST_REQUIRE(succ);
    }
    ~FakeRedis() {
        stop->store(true);
        WaitUntil([dn = done] { return dn->load(); }, 10000);
    }

    std::uint16_t port = 0;
    std::shared_ptr<std::atomic_bool> done{
        std::make_shared<std::atomic_bool>(false)};
    std::shared_ptr<std::atomic_bool> stop{
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

// client 工厂：Create；connect=true 时在协程内显式 Connect（显式生命周期——
// 命令不再隐式建连，故需要连接的场景必须显式建连并成功）。
result<std::shared_ptr<CoRedisCli>> NewClient(const RedisClientConfig& cfg,
                                            bool connect = true) {
    auto c = CoRedisCli::Create(cfg);
    if (!c)
        return result<std::shared_ptr<CoRedisCli>>::err(
            std::move(c).error());
    auto cli = std::move(c).value();
    if (connect) {
        std::optional<result<void>> st;
        if (!RunInCoroutine([&] { st.emplace(cli->Connect(Opt())); }))
            return result<std::shared_ptr<CoRedisCli>>::err(MakeError(
                ErrorCode::InternalError, "redis: connect coroutine not run"));
        if (!st)
            return result<std::shared_ptr<CoRedisCli>>::err(MakeError(
                ErrorCode::InternalError, "redis: connect produced no result"));
        if (!*st)
            return result<std::shared_ptr<CoRedisCli>>::err(
                std::move(*st).error());
    }
    return result<std::shared_ptr<CoRedisCli>>::ok(std::move(cli));
}

// 显式 Close 并断言物理收口：redis 的迟到 NULL 回包在 redisAsyncFree 内
// 同步催出，teardown 在 kCloseDrainTimeout 内必完成，Close 返回后 IsClosed
// 即为真（在途 operation 与连接均已归零）。
void CloseClient(const std::shared_ptr<CoRedisCli>& cli) {
    cli->Close();
    BOOST_CHECK(cli->IsClosed());
}

std::atomic_bool g_prepared{false};

} // namespace

// 进程寿命运行时：无 Stop/restart，实例持有者被 coroutine 故意泄漏，
// 静态退出期不析构 Scheduler——测试侧无需（也不得）用 Stop 做收尾。
// 资源收口由每个 client 自己的 Close() 承担。

BOOST_AUTO_TEST_SUITE(redis_unit)

// 运行时未初始化（从未 Start）时 Create → RuntimeUnavailable（对象身份
// 需要已初始化的运行时）。进程寿命契约下「未初始化」只有「从未 Start」
// 一种，故本用例只断言 Create 的失败结果。
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
    BOOST_REQUIRE(g_scheduler->IsInitialized());
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
    auto c = NewClient(MakeConfig("127.0.0.1", 1), /*connect=*/false);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    // 未 Connect：协程内调用 → RuntimeUnavailable（且不得隐式建连）。
    std::optional<result<void>> ping_out;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { ping_out.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(!*ping_out);
    BOOST_CHECK(ping_out->error().code == ErrorCode::RuntimeUnavailable);

    // 参数形态校验先于连接检查：空键/空表 InvalidArgument。
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

    CloseClient(cli);

    // 已关闭：新命令 → Closed。
    BOOST_REQUIRE(RunInCoroutine(
        [&] { ping_out.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(!*ping_out);
    BOOST_CHECK(ping_out->error().code == ErrorCode::Closed);
}

// S1（不做 Lazy）：未 Connect 的命令返回错误且不得隐式建连——服务端从未建立连接（无字节）。
BOOST_AUTO_TEST_CASE(t_no_lazy_connect_without_connect) {
    BOOST_REQUIRE(g_prepared.load());
    FakeRedis fake({"+PONG\r\n"});
    auto c = NewClient(MakeConfig("127.0.0.1", fake.port), /*connect=*/false);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Disconnected);

    std::optional<result<void>> out;
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt(1000))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::RuntimeUnavailable);
    // 命令不得隐式建连：服务端未收到任何字节。
    BOOST_CHECK(!fake.saw_data->load());

    // 显式 Connect 后才可用（真实 PING）。
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Connect(Opt())); }));
    BOOST_REQUIRE(*out);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Connected);
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(*out);
    BOOST_CHECK(fake.saw_data->load());

    CloseClient(cli);
}

// S2/S3/S4/S5：显式生命周期状态机（Connect 幂等、ConnectStatus 只读、
// Disconnect 同步收口且可再次 Connect、Close 终态拒绝 Connect）。
BOOST_AUTO_TEST_CASE(t_connect_disconnect_lifecycle) {
    BOOST_REQUIRE(g_prepared.load());
    // 多连接：Disconnect 后再次 Connect 会建立第二条连接。
    FakeRedis fake({"+PONG\r\n", "+PONG\r\n"}, /*silent=*/false,
                   /*max_conns=*/2);
    auto c = NewClient(MakeConfig("127.0.0.1", fake.port), /*connect=*/false);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    // ConnectStatus 只读、不发网络请求：重复读取不改变状态。
    for (int i = 0; i < 5; ++i)
        BOOST_CHECK(cli->ConnectStatus() == ConnectState::Disconnected);

    std::optional<result<void>> out;
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Connect(Opt())); }));
    BOOST_REQUIRE(*out);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Connected);

    // 重复 Connect：幂等 ok（不重新拨号）。
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Connect(Opt())); }));
    BOOST_REQUIRE(*out);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Connected);

    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(*out);

    // Disconnect：同步收口、非终态、保留配置。
    cli->Disconnect();
    BOOST_CHECK(!cli->IsClosed());
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Disconnected);
    // 主动 Disconnect 之后命令被拒，且不得隐式建连。
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt(1000))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::RuntimeUnavailable);
    // 重复 Disconnect：幂等空操作。
    cli->Disconnect();
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Disconnected);

    // 同实例再次显式 Connect。
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Connect(Opt())); }));
    BOOST_REQUIRE(*out);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Connected);
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(*out);

    // Close 终态：拒绝 Connect，命令 Closed。
    CloseClient(cli);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Closed);
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Connect(Opt())); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
}

BOOST_AUTO_TEST_CASE(t_invalid_context_plain) {
    // 普通线程（非协程）调命令：不启动协程直接调用。
    auto c = NewClient(MakeConfig("127.0.0.1", 6379), /*connect=*/false);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    auto res = cli->Ping(Opt());
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::InvalidContext);
    // Connect 同样要求协程上下文：普通线程直接调用 → InvalidContext。
    auto conn = cli->Connect(Opt());
    BOOST_REQUIRE(!conn);
    BOOST_CHECK(conn.error().code == ErrorCode::InvalidContext);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Disconnected);
    // Close 可在任意线程调用（含非协程线程）。
    CloseClient(cli);
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

    CloseClient(cli);
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
            // budget_ms 按值捕获：协程在 submit 返回之后才执行，引用捕获会
            // stack-use-after-return。
            [&, budget_ms] {
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

    // 其余命令仍在等回包/排队：以 owner Close() 收口，应全部 Closed。
    cli->Close();
    BOOST_REQUIRE(WaitUntil([&] { return done.load() == 4; }));
    BOOST_CHECK_EQUAL(overloaded.load(), 1);
    BOOST_CHECK_EQUAL(other_err.load(), 3);
    BOOST_CHECK(cli->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_deadline_only) {
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

    // 随后的 owner Close 不覆盖首个逻辑终态。
    cli->Close();
    BOOST_CHECK(cli->IsClosed());
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TimedOut);
}

BOOST_AUTO_TEST_CASE(t_refused_connect) {
    // 127.0.0.1:1 惯例无监听：connect 拒绝 → Connect 报 TransportError 且状态
    // Failed；默认不重连（后续命令 sticky TransportError）；再次显式 Connect
    // 仍可尝试（Failed 不是终态）。
    auto c = NewClient(MakeConfig("127.0.0.1", 1), /*connect=*/false);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::optional<result<void>> out;
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Connect(Opt(8000))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TransportError);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Failed);

    // 默认 reconnect_on_new_command=false：后续命令直接 TransportError，状态保持 Failed。
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Ping(Opt(8000))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TransportError);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Failed);

    // 显式重建：再次 Connect 仍失败（目标不可达）但可再次尝试，不进入终态。
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->Connect(Opt(8000))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TransportError);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Failed);

    CloseClient(cli);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Closed);
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

    cli->Close();
    BOOST_REQUIRE(WaitUntil(
        [&] { return out_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);

    // Close 返回 = 在途 operation 与连接均已归零。
    BOOST_CHECK(cli->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_thread_count_stable) {
    const auto before = ThreadCount();
    BOOST_REQUIRE(before > 0);
    std::vector<std::shared_ptr<CoRedisCli>> clients;
    for (int i = 0; i < 4; ++i) {
        auto c = NewClient(MakeConfig("127.0.0.1", 1), /*connect=*/false);
        BOOST_REQUIRE(c);
        clients.push_back(std::move(c).value());
    }
    // client/strand/连接管理不新增线程：线程数与 Scheduler 启动时一致。
    BOOST_CHECK_EQUAL(ThreadCount(), before);
    for (auto& cli : clients)
        CloseClient(cli);
}

BOOST_AUTO_TEST_SUITE_END()
