// Issue #6 真实 Redis 容器验收（docker compose 起 redis:7-alpine，
// cpus<=0.5 / mem_limit<=256m / 动态项目名，编排见 tests/redis-live.sh）：
//
//   BBT_TEST_REDIS_ADDR=host:port   必填，未设置整件跳过（不伪造通过）
//   BBT_REDIS_PHASE=A|B|C           默认 A：
//     A = 容器在线（功能、错误、并发、close、进程内断连恢复）
//     B = 容器已停（命令必须失败而非悬挂/假成功）
//     C = 容器重启后（新 client 亦可用）
//   BBT_REDIS_CTL='docker compose -p P -f FILE'  容器 stop/start 命令
//     前缀（A 阶段断连恢复用），缺省则跳过 t_reconnect。
//   BBT_REDIS_RCLI='... exec -T redis redis-cli' 容器内 redis-cli 命令
//     前缀（WRONGTYPE 种子用），缺省则跳过 t_server_error。
//
// 并发上限 32、单命令 deadline <=2s、套件总预算有界。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/CoRedisCli.hpp>

using namespace bbt::infra;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

constexpr int kBudgetMs = 25000;

const char* EnvOr(const char* k) { return std::getenv(k); }

bool LiveEnabled() {
    const char* a = EnvOr("BBT_TEST_REDIS_ADDR");
    return a != nullptr && a[0] != '\0';
}

bool ParseAddr(std::string& host, std::uint16_t& port) {
    const char* a = EnvOr("BBT_TEST_REDIS_ADDR");
    if (a == nullptr)
        return false;
    std::string s(a);
    const auto  colon = s.rfind(':');
    if (colon == std::string::npos)
        return false;
    host = s.substr(0, colon);
    port = static_cast<std::uint16_t>(std::stoul(s.substr(colon + 1)));
    return port != 0;
}

bool PhaseIs(char p) {
    const char* v = EnvOr("BBT_REDIS_PHASE");
    return v == nullptr || v[0] == '\0' ? p == 'A' : v[0] == p;
}

RedisClientConfig LiveConfig() {
    std::string   host;
    std::uint16_t port = 0;
    BOOST_REQUIRE(ParseAddr(host, port));
    RedisClientConfig cfg;
    cfg.host         = host;
    cfg.port         = port;
    cfg.max_inflight = 32;
    cfg.max_queue    = 64;
    return cfg;
}

CallOptions Opt(int ms = 2000) {
    CallOptions opt;
    opt.deadline = std::chrono::steady_clock::now() +
                   std::chrono::milliseconds(ms);
    return opt;
}

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

// 非协程线程上的定长睡眠：必须直达内核 syscall。coroutine Hook 全进程
// 拦截 libc nanosleep 并断言协程上下文——测试线程命中即崩（基线行为）。
void SleepMs(int ms) {
    timespec req{ms / 1000, static_cast<long>(ms % 1000) * 1000000L};
    timespec rem{};
    while (::syscall(SYS_nanosleep, &req, &rem) != 0 && errno == EINTR)
        req = rem;
}

int Sys(const std::string& prefix, const char* args) {
    return std::system((prefix + " " + args).c_str());
}

std::shared_ptr<CoRedisCli> NewLiveClient() {
    auto c = CoRedisCli::Create(LiveConfig());
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    BOOST_REQUIRE(cli->Start());
    return cli;
}

// 生成二进制安全 payload：含 NUL、0xFF、多字节与可变长度。
std::string BinaryPayload(int seed) {
    std::string v;
    v.reserve(64);
    for (int i = 0; i < 64; ++i)
        v.push_back(static_cast<char>((seed + i * 37) & 0xFF));
    v[3]  = '\0';
    v[17] = '\0';
    v[40] = static_cast<char>(0xFF);
    return v;
}

std::atomic_bool g_prepared{false};

} // namespace

// 与 redis.unit 同理：~Scheduler→Stop 会经 Processer::Stop→sleep_for→
// nanosleep 命中 coroutine 自身 Hook 断言（基线竞态）。套件收尾释放单例
// 所有权，使进程退出不再触发 Stop 路径。
struct SuiteTeardown {
    ~SuiteTeardown() { g_scheduler.release(); }
};
BOOST_GLOBAL_FIXTURE(SuiteTeardown);

BOOST_AUTO_TEST_SUITE(redis_live)

BOOST_AUTO_TEST_CASE(t_setup_scheduler) {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 4;
    cfg->m_cfg_stack_size        = 1024 * 256;
    g_scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(g_scheduler->IsRunning());
    g_prepared.store(true);
}

BOOST_AUTO_TEST_CASE(t_ping_and_binary_kv) {
    if (!LiveEnabled() || !PhaseIs('A')) {
        BOOST_TEST_MESSAGE("skip: 需要 BBT_TEST_REDIS_ADDR 且 PHASE=A");
        return;
    }
    auto cli = NewLiveClient();

    std::optional<result<void>> ping;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { ping.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(ping && *ping);

    // 二进制安全：含 NUL/0xFF 的 key 与 value 原样往返。
    const std::string key = std::string("bbt:test:bin\0key", 14);
    const std::string val = BinaryPayload(7);
    std::optional<result<void>> set;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { set.emplace(cli->Set(key, val, Opt())); }));
    BOOST_REQUIRE(set && *set);

    std::optional<result<std::optional<std::string>>> get;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { get.emplace(cli->Get(key, Opt())); }));
    BOOST_REQUIRE(get && *get);
    BOOST_REQUIRE(get->value().has_value());
    BOOST_CHECK_EQUAL(*get->value(), val);

    // 未命中：ok(nullopt) 而非错误。
    std::optional<result<std::optional<std::string>>> miss;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { miss.emplace(cli->Get("bbt:test:absent", Opt())); }));
    BOOST_REQUIRE(miss && *miss);
    BOOST_CHECK(!miss->value().has_value());

    std::optional<result<bool>> ex;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { ex.emplace(cli->Exists(key, Opt())); }));
    BOOST_REQUIRE(ex && *ex && ex->value());

    std::optional<result<std::uint64_t>> del;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { del.emplace(cli->Delete({key}, Opt())); }));
    BOOST_REQUIRE(del && *del);
    BOOST_CHECK_EQUAL(del->value(), 1u);

    std::optional<result<bool>> ex2;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { ex2.emplace(cli->Exists(key, Opt())); }));
    BOOST_REQUIRE(ex2 && *ex2 && !ex2->value());

    cli->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
}

BOOST_AUTO_TEST_CASE(t_server_error) {
    if (!LiveEnabled() || !PhaseIs('A') || EnvOr("BBT_REDIS_RCLI") == nullptr) {
        BOOST_TEST_MESSAGE("skip: 需要 PHASE=A 与 BBT_REDIS_RCLI");
        return;
    }
    // 种子一个非 string 类型键：GET 触发服务端 WRONGTYPE。
    const char* seed = "bbt:test:wrongtype";
    Sys(EnvOr("BBT_REDIS_RCLI"), "DEL bbt:test:wrongtype");
    BOOST_REQUIRE_EQUAL(Sys(EnvOr("BBT_REDIS_RCLI"),
                            "LPUSH bbt:test:wrongtype v"),
                        0);

    auto cli = NewLiveClient();
    std::optional<result<std::optional<std::string>>> bad;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { bad.emplace(cli->Get(seed, Opt())); }));
    BOOST_REQUIRE(bad && !*bad);
    BOOST_CHECK(bad->error().code == ErrorCode::RemoteError);
    BOOST_CHECK_EQUAL(bad->error().domain_code, "WRONGTYPE");
    Sys(EnvOr("BBT_REDIS_RCLI"), "DEL bbt:test:wrongtype");

    cli->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
}

BOOST_AUTO_TEST_CASE(t_concurrent_smoke_32) {
    if (!LiveEnabled() || !PhaseIs('A')) {
        BOOST_TEST_MESSAGE("skip: 需要 BBT_TEST_REDIS_ADDR 且 PHASE=A");
        return;
    }
    auto cli = NewLiveClient();

    constexpr int     kWorkers = 32;
    std::atomic_int   done{0};
    std::atomic_int   errors{0};
    std::atomic_int   mismatches{0};
    const auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < kWorkers; ++i) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [&, i] {
                const std::string key = "bbt:test:co:" + std::to_string(i);
                const std::string val = BinaryPayload(i);
                auto s = cli->Set(key, val, Opt());
                if (!s) {
                    errors.fetch_add(1);
                } else {
                    auto g = cli->Get(key, Opt());
                    if (!g)
                        errors.fetch_add(1);
                    else if (!g.value().has_value() || *g.value() != val)
                        mismatches.fetch_add(1);
                    else {
                        auto d = cli->Delete({key}, Opt());
                        if (!d || d.value() != 1u)
                            errors.fetch_add(1);
                    }
                }
                done.fetch_add(1);
            },
            succ);
        BOOST_REQUIRE(succ);
    }
    BOOST_REQUIRE(WaitUntil([&] { return done.load() == kWorkers; }, 30000));
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - t0)
                        .count();
    BOOST_CHECK_LT(ms, 30000);
    BOOST_CHECK_EQUAL(errors.load(), 0);
    BOOST_CHECK_EQUAL(mismatches.load(), 0);

    cli->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
}

BOOST_AUTO_TEST_CASE(t_reconnect) {
    const char* ctl = EnvOr("BBT_REDIS_CTL");
    if (!LiveEnabled() || !PhaseIs('A') || ctl == nullptr) {
        BOOST_TEST_MESSAGE("skip: 需要 PHASE=A 与 BBT_REDIS_CTL");
        return;
    }
    auto cli = NewLiveClient();

    // 在线基线。
    std::optional<result<void>> ping;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { ping.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(ping && *ping);

    // 停容器：既有连接断开，后续命令失败而非悬挂。
    BOOST_REQUIRE_EQUAL(Sys(ctl, "stop redis"), 0);
    std::optional<result<void>> down;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { down.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(!*down);
    BOOST_CHECK(down->error().code == ErrorCode::TransportError ||
                down->error().code == ErrorCode::Unavailable);

    // 起容器：同一 client 重连后恢复（新命令触发重连，不迁移旧命令）。
    BOOST_REQUIRE_EQUAL(Sys(ctl, "start redis"), 0);
    std::atomic_bool recovered{false};
    BOOST_REQUIRE(RunInCoroutine([&] {
        const auto dl = std::chrono::steady_clock::now() +
                        std::chrono::seconds(15);
        while (std::chrono::steady_clock::now() < dl) {
            auto r = cli->Ping(Opt(1500));
            if (r) {
                recovered.store(true);
                return;
            }
        }
    }, 20000));
    BOOST_CHECK(recovered.load());

    // 恢复后读写仍正确。
    const std::string key = "bbt:test:reconn";
    const std::string val = BinaryPayload(3);
    std::optional<result<void>> set;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { set.emplace(cli->Set(key, val, Opt())); }));
    BOOST_REQUIRE(set && *set);
    std::optional<result<std::optional<std::string>>> get;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { get.emplace(cli->Get(key, Opt())); }));
    BOOST_REQUIRE(get && *get && get->value().has_value());
    BOOST_CHECK_EQUAL(*get->value(), val);
    std::optional<result<std::uint64_t>> del;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { del.emplace(cli->Delete({key}, Opt())); }));
    BOOST_REQUIRE(del && *del && del->value() == 1u);

    cli->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
}

BOOST_AUTO_TEST_CASE(t_server_down_fails) {
    if (!LiveEnabled() || !PhaseIs('B')) {
        BOOST_TEST_MESSAGE("skip: 需要 PHASE=B（容器停止态）");
        return;
    }
    auto cli = NewLiveClient();
    std::optional<result<void>> out;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { out.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TransportError ||
                out->error().code == ErrorCode::Unavailable);
    cli->RequestClose();
}

BOOST_AUTO_TEST_CASE(t_close_drains_inflight) {
    if (!LiveEnabled() || !PhaseIs('A')) {
        BOOST_TEST_MESSAGE("skip: 需要 BBT_TEST_REDIS_ADDR 且 PHASE=A");
        return;
    }
    auto cli = NewLiveClient();

    // 16 个并发命令在途时 owner close：全部落定且 WaitClosed→Closed，
    // 逻辑结果只能是 ok/Closed 之一，不得悬挂。
    constexpr int   kOps = 16;
    std::atomic_int done{0};
    std::atomic_int settled{0};
    for (int i = 0; i < kOps; ++i) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [&, i] {
                auto r = cli->Set("bbt:test:drain:" + std::to_string(i),
                                  BinaryPayload(i), Opt(30000));
                if (r || r.error().code == ErrorCode::Closed)
                    settled.fetch_add(1);
                done.fetch_add(1);
            },
            succ);
        BOOST_REQUIRE(succ);
    }
    SleepMs(30);
    cli->RequestClose();
    BOOST_REQUIRE(WaitUntil([&] { return done.load() == kOps; }));
    BOOST_CHECK_EQUAL(settled.load(), kOps);

    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
    BOOST_CHECK(cli->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_fresh_client_after_restart) {
    if (!LiveEnabled() || !PhaseIs('C')) {
        BOOST_TEST_MESSAGE("skip: 需要 PHASE=C（容器重启态）");
        return;
    }
    // 新进程、新 client：连接重启后的服务，读写必须正常。
    auto cli = NewLiveClient();
    std::optional<result<void>> ping;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { ping.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(ping && *ping);
    const std::string key = "bbt:test:phasec";
    const std::string val = BinaryPayload(11);
    std::optional<result<void>> set;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { set.emplace(cli->Set(key, val, Opt())); }));
    BOOST_REQUIRE(set && *set);
    std::optional<result<std::optional<std::string>>> get;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { get.emplace(cli->Get(key, Opt())); }));
    BOOST_REQUIRE(get && *get && get->value().has_value());
    BOOST_CHECK_EQUAL(*get->value(), val);
    std::optional<result<std::uint64_t>> del;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { del.emplace(cli->Delete({key}, Opt())); }));
    BOOST_REQUIRE(del && *del && del->value() == 1u);

    cli->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
}

BOOST_AUTO_TEST_SUITE_END()
