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
// 关闭语义（进程寿命修订）：Close() 幂等、任意线程可调用，返回即封口且
// 在途 operation/connection 归零；IsClosed() 只读查询。无 RequestClose/
// WaitClosed/CloseStatus。
//
// 显式生命周期：连接只在协程内显式 Connect 时建立（命令不隐式建连）；断连恢复
// 由调用方显式 Connect 触发（默认 reconnect_on_new_command=false）；Disconnect
// 同步收口连接但保留配置、可再次 Connect；Close 是终态。
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
#include <bbt/infra/CoTCP.hpp>

// Issue #34：真实在途断言复用 src/redis 内部只读探针（BindingStateSnapshot /
// TransportForTest 均为生产始终存在的真实状态面，不受 debug 开关影响）。
// 仅本测试目标可见（tests/CMakeLists.txt 内 Test_redis_live 专属 include）。
#include "redis/CoRedisCliImpl.hpp"
// Issue #34：复用生产已有的 F3 数据路径「协程真实挂起在 fd 等待」接缝
// （detail::TransportWiring::SetIoWaitRegisteredGateForTest），仅测试安装。
#include "detail/TransportWiring.hpp"

using namespace bbt::infra;
using bbt::infra::redis_detail::CoRedisCliImpl;
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

std::shared_ptr<CoRedisCli> NewLiveClient(bool connect = true) {
    auto c = CoRedisCli::Create(LiveConfig());
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    if (connect) {
        // 显式生命周期：真实 Redis 下必须显式 Connect 成功（命令不再隐式建连）。
        std::optional<result<void>> st;
        BOOST_REQUIRE(RunInCoroutine([&] { st.emplace(cli->Connect(Opt())); }));
        BOOST_REQUIRE(st && *st);
    }
    return cli;
}

// 显式 Close 并断言物理收口（封口 + 在途归零 + 连接回收）。
void CloseAndCheck(const std::shared_ptr<CoRedisCli>& cli) {
    cli->Close();
    BOOST_CHECK(cli->IsClosed());
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

// ---- Issue #34：真实服务端等待（CLIENT PAUSE）下的强制超时 / owner Close 中断 ----
// 因果握手：Redis 原生 `CLIENT PAUSE <ms> ALL` 让服务端在窗口内挂起所有命令；
// 命令在窗口内真实发出（TCP 已接受、服务端收到但不回复），故等待发生在服务端，
// 不是「已过期入参/排队超时」冒充。观测复用生产真实状态面
// （BindingStateSnapshot / TransportForTest），不新增生产接缝、不用固定 sleep 判定状态。
// 共享结果一律堆 state（shared_ptr 由协程按值持有）并在失败路径 join/等待完成，
// 不引用已结束的测试栈；既有 RunInCoroutine 保留不动。

constexpr int kServerPauseMs = 3000;

// #34 R2：pause 窗口长度单一常量，避免字面量与 kServerPauseMs 漂移。
std::string PauseArgs() {
    return std::string("CLIENT PAUSE ") + std::to_string(kServerPauseMs) + " ALL";
}

// #34 R4：新增控制命令自带 timeout（不改既有 Sys helper 的语义与调用点）。
int SysBounded(const std::string& prefix, const std::string& args) {
    return std::system(("timeout --kill-after=5 30 " + prefix + " " + args).c_str());
}

// #34：控制/等待失败要 fail closed——不论用例正常返回还是 BOOST_REQUIRE 抛错，
// 退出本用例时都 Close 本用例自有的 client（Close 幂等、任意线程可调用）。
struct CloseOwnedClientsOnExit {
    std::vector<std::shared_ptr<CoRedisCli>> clients;
    void Own(const std::shared_ptr<CoRedisCli>& c) {
        if (c)
            clients.push_back(c);
    }
    ~CloseOwnedClientsOnExit() {
        for (auto& c : clients)
            if (c)
                c->Close();
    }
};

struct PingOutcome {
    std::atomic_bool            done{false};
    std::optional<result<void>> r;
    std::atomic<long long>      ms{0};
};

struct VoidOutcome {
    std::atomic_bool            done{false};
    std::optional<result<void>> r;
};

long long MsSince(const std::chrono::steady_clock::time_point& t0) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::steady_clock::now() - t0)
        .count();
}

// 只读真实状态：旧 transport 物理已收口（连接对象已释放，或 Close 已发布且
// CoTCP 已物理关闭）。
bool TransportPhysicallyClosed(const std::shared_ptr<CoRedisCliImpl>& impl) {
    return impl->BindingStateSnapshot().conn_closed &&
           impl->TransportForTest() == nullptr;
}

} // namespace

// Issue #34：live 套件无 BBT_TEST_REDIS_ADDR 时必须显式标记 Skipped，不能
// 以退出码 0 冒充通过（CTest 会把 0 记为 Passed）。套件内每个 case 用
// BOOST_TEST_MESSAGE("skip: ...")+return 提前返回；Boost.Test 默认日志
// 级别下该消息不打印，SKIP_REGULAR_EXPRESSION 匹配不到文本。故在全局
// fixture 构造时检测环境，无环境直接 std::exit(77)——配合 CTest
// SKIP_RETURN_CODE=77 标为 Skipped，同时不伪造通过、也不进入任何用例。
struct RequireLiveEnv {
    RequireLiveEnv() {
        const char* a = std::getenv("BBT_TEST_REDIS_ADDR");
        if (a == nullptr || a[0] == '\0') {
            std::fputs("redis.live: BBT_TEST_REDIS_ADDR 未设置，标记 Skipped\n",
                       stderr);
            std::exit(77);
        }
    }
};
BOOST_GLOBAL_FIXTURE(RequireLiveEnv);

// 进程寿命运行时：无 Stop/restart；实例持有者被 coroutine 故意泄漏，
// 静态退出期不析构 Scheduler，测试侧不做任何 Stop 收尾。

BOOST_AUTO_TEST_SUITE(redis_live)

BOOST_AUTO_TEST_CASE(t_setup_scheduler) {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 4;
    cfg->m_cfg_stack_size        = 1024 * 256;
    g_scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(g_scheduler->IsInitialized());
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

    CloseAndCheck(cli);
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

    CloseAndCheck(cli);
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

    CloseAndCheck(cli);
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

    // 起容器：同一 client 显式 Connect 重建连接后恢复（默认不做隐式重连，不迁移旧命令）。
    BOOST_REQUIRE_EQUAL(Sys(ctl, "start redis"), 0);
    // start/TCP 握手成功不等于命令就绪；复用 compose 的 redis-cli PING
    // 健康检查等待。只等待后端前置条件，不重试被测 SET/GET/DEL。
    BOOST_REQUIRE_EQUAL(Sys(std::string("timeout --kill-after=2 20 ") + ctl,
                           "up -d --wait --wait-timeout 15 redis"), 0);
    std::atomic_bool recovered{false};
    BOOST_REQUIRE(RunInCoroutine([&] {
        const auto dl = std::chrono::steady_clock::now() +
                        std::chrono::seconds(15);
        while (std::chrono::steady_clock::now() < dl) {
            if (cli->Connect(Opt(1500))) {
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

    CloseAndCheck(cli);
}

BOOST_AUTO_TEST_CASE(t_server_down_fails) {
    if (!LiveEnabled() || !PhaseIs('B')) {
        BOOST_TEST_MESSAGE("skip: 需要 PHASE=B（容器停止态）");
        return;
    }
    auto cli = NewLiveClient(/*connect=*/false);
    // 容器停止态：显式 Connect 必须失败而非悬挂/假成功。
    std::optional<result<void>> conn;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { conn.emplace(cli->Connect(Opt())); }));
    BOOST_REQUIRE(!*conn);
    BOOST_CHECK(conn->error().code == ErrorCode::TransportError ||
                conn->error().code == ErrorCode::Unavailable ||
                conn->error().code == ErrorCode::TimedOut);
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Failed);
    // 未连接的命令立即失败（不隐式建连、不悬挂）。
    std::optional<result<void>> out;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { out.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TransportError);
    CloseAndCheck(cli);
}

BOOST_AUTO_TEST_CASE(t_close_drains_inflight) {
    if (!LiveEnabled() || !PhaseIs('A')) {
        BOOST_TEST_MESSAGE("skip: 需要 BBT_TEST_REDIS_ADDR 且 PHASE=A");
        return;
    }
    auto cli = NewLiveClient();

    // 16 个并发命令在途时 owner Close()：全部落定，逻辑结果只能是
    // ok/Closed 之一，不得悬挂；Close 返回即在途归零。
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
    cli->Close();
    BOOST_REQUIRE(WaitUntil([&] { return done.load() == kOps; }));
    BOOST_CHECK_EQUAL(settled.load(), kOps);

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

    CloseAndCheck(cli);
}

// ---- Issue #34：显式 Disconnect/Connect 恢复（无需服务端故障注入）--------------
BOOST_AUTO_TEST_CASE(t_explicit_disconnect_reconnect_recovers) {
    if (!LiveEnabled() || !PhaseIs('A')) {
        BOOST_TEST_MESSAGE("skip: 需要 BBT_TEST_REDIS_ADDR 且 PHASE=A");
        return;
    }
    auto cli = NewLiveClient();
    std::optional<result<void>> ping;
    BOOST_REQUIRE(RunInCoroutine([&] { ping.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(ping && *ping);

    const std::string key = "bbt:test:disc";
    const std::string val = BinaryPayload(9);
    std::optional<result<void>> set;
    BOOST_REQUIRE(RunInCoroutine([&] { set.emplace(cli->Set(key, val, Opt())); }));
    BOOST_REQUIRE(set && *set);

    // 显式 Disconnect：同步收口连接、保留配置、可再次 Connect（非终态）。
    cli->Disconnect();
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Disconnected);

    // 未连接命令立即以 RuntimeUnavailable 失败，不隐式建连。
    std::optional<result<void>> off;
    BOOST_REQUIRE(RunInCoroutine([&] { off.emplace(cli->Ping(Opt(500))); }));
    BOOST_REQUIRE(off && !*off);
    BOOST_CHECK(off->error().code == ErrorCode::RuntimeUnavailable);

    // 显式 Connect 重建后 PING/KV 恢复。
    std::optional<result<void>> rc;
    BOOST_REQUIRE(RunInCoroutine([&] { rc.emplace(cli->Connect(Opt(3000))); }));
    BOOST_REQUIRE(rc && *rc);
    std::optional<result<void>> ping2;
    BOOST_REQUIRE(RunInCoroutine([&] { ping2.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(ping2 && *ping2);
    std::optional<result<std::optional<std::string>>> get;
    BOOST_REQUIRE(RunInCoroutine([&] { get.emplace(cli->Get(key, Opt())); }));
    BOOST_REQUIRE(get && *get && get->value().has_value());
    BOOST_CHECK_EQUAL(*get->value(), val);
    std::optional<result<std::uint64_t>> del;
    BOOST_REQUIRE(RunInCoroutine([&] { del.emplace(cli->Delete({key}, Opt())); }));
    BOOST_REQUIRE(del && *del && del->value() == 1u);

    CloseAndCheck(cli);
}

// ---- Issue #34：owner Close 与真实在途命令竞争 → Closed ------------------------
BOOST_AUTO_TEST_CASE(t_close_race_during_server_pause_settles_closed) {
    if (!LiveEnabled() || !PhaseIs('A')) {
        BOOST_TEST_MESSAGE("skip: 需要 BBT_TEST_REDIS_ADDR 且 PHASE=A");
        return;
    }
    const char* rcli = EnvOr("BBT_REDIS_RCLI");
    // #34：真实服务端在途等待用例必须实跑；缺 RCLI 硬失败，不静默成绿。
    BOOST_REQUIRE_MESSAGE(rcli != nullptr,
        "redis.live #34: 缺 BBT_REDIS_RCLI，无法制造真实服务端在途等待");

    CloseOwnedClientsOnExit owned;
    auto cli = NewLiveClient();
    owned.Own(cli);
    std::optional<result<void>> base;
    BOOST_REQUIRE(RunInCoroutine([&] { base.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(base && *base);

    auto impl = std::dynamic_pointer_cast<CoRedisCliImpl>(cli);
    BOOST_REQUIRE(impl);
    // 采集旧 transport（按值持有）：Close 后直接断言其物理 closed，null 槽不独自作证。
    auto old_transport = impl->TransportForTest();
    BOOST_REQUIRE_MESSAGE(old_transport, "健康 PING 后必须有真实 transport");

    // R1：不以 registered>=1 作「已写出」证据——registered 与入队同刻成立，owner 可能
    // 尚未出队/编码/写出（review 建议的 queued==0 && registered>=1 亦不充分：出队后仍未
    // 编码）。改用生产已有 F3 接缝：transport 数据路径协程真正挂起在 fd 等待
    // （on_registered）时回调一次，只写堆原子、不阻塞。单条小 SET + 无其他 op ⇒ 小写一次
    // 写完（无 EAGAIN），ReadReply 才是唯一会挂起的 fd 等待，故命中即「命令已真实挂起在读等待」。
    auto io_wait = std::make_shared<std::atomic_bool>(false);
    detail::TransportWiring::SetIoWaitRegisteredGateForTest(
        *old_transport, [io_wait] { io_wait->store(true); });

    // 因果握手：服务端声明在 kServerPauseMs 内挂起所有客户端命令（单一常量）。
    BOOST_REQUIRE_EQUAL(SysBounded(rcli, PauseArgs()), 0);

    // 在途：长 deadline 的 SET 在 pause 窗口内真实发出，服务端收到但不回复。
    auto op      = std::make_shared<VoidOutcome>();
    bool started = false;
    g_scheduler->RegistCoroutineTask(
        [op, cli] {
            op->r.emplace(cli->Set("bbt:test:close:pause", BinaryPayload(5),
                                   Opt(30000)));
            op->done.store(true);
        },
        started);
    BOOST_REQUIRE(started);
    // 确认既有 gate 已触发：SET 已真实挂起在 fd 读等待（非仅入队/未写出）。
    BOOST_REQUIRE_MESSAGE(
        WaitUntil([&] { return io_wait->load(); }, kServerPauseMs),
        "服务端 pause 下 SET 必须已真实挂起在 fd 等待（非仅入队）");

    // owner 主动同步 Close：与在途命令竞争，终态只能是 Closed。
    cli->Close();
    BOOST_CHECK(cli->IsClosed());
    BOOST_REQUIRE_MESSAGE(WaitUntil([&] { return op->done.load(); }, 5000),
                          "Close 后调用协程必须在界内落定");
    BOOST_REQUIRE(op->r.has_value());
    BOOST_CHECK_MESSAGE(!*op->r, "Close 与在途命令竞争：命令不得成功");
    BOOST_CHECK(op->r->error().code == ErrorCode::Closed);

    // 物理收口：直接断言旧 transport 物理 closed + 登记归零。
    BOOST_CHECK_MESSAGE(old_transport->IsClosed(),
                        "Close 返回后旧 transport 必须物理 closed");
    {
        const auto snap = impl->BindingStateSnapshot();
        BOOST_CHECK_EQUAL(snap.registered, 0u);
        BOOST_CHECK_MESSAGE(snap.conn_closed,
                            "Close 返回后旧 transport 必须物理 closed");
    }
    BOOST_CHECK(impl->TransportForTest() == nullptr);

    // 幂等：重复 Close 仍 Closed；Close 是终态，不可再 Connect。
    cli->Close();
    BOOST_CHECK(cli->IsClosed());
    std::optional<result<void>> after;
    BOOST_REQUIRE(RunInCoroutine([&] { after.emplace(cli->Connect(Opt(500))); }));
    BOOST_REQUIRE(after && !*after);
    BOOST_CHECK(after->error().code == ErrorCode::Closed);

    // 等待 pause 结束（可观测：新 client PING 成功即服务端恢复）并验证新 client 恢复 PING/KV。
    auto fresh = NewLiveClient();
    owned.Own(fresh);
    auto back  = std::make_shared<PingOutcome>();
    BOOST_REQUIRE(RunInCoroutine([back, fresh] {
        const auto t0 = std::chrono::steady_clock::now();
        back->r.emplace(fresh->Ping(Opt(9000)));
        back->ms.store(MsSince(t0));
        back->done.store(true);
    }));
    BOOST_REQUIRE(back->r.has_value());
    BOOST_CHECK_MESSAGE(*back->r, "pause 结束后新 client 必须恢复");
    const std::string key = "bbt:test:close:pause:rec";
    const std::string val = BinaryPayload(6);
    std::optional<result<void>> set2;
    BOOST_REQUIRE(RunInCoroutine([&] { set2.emplace(fresh->Set(key, val, Opt())); }));
    BOOST_REQUIRE(set2 && *set2);
    std::optional<result<std::optional<std::string>>> get2;
    BOOST_REQUIRE(RunInCoroutine([&] { get2.emplace(fresh->Get(key, Opt())); }));
    BOOST_REQUIRE(get2 && *get2 && get2->value().has_value());
    BOOST_CHECK_EQUAL(*get2->value(), val);
    std::optional<result<std::uint64_t>> del2;
    BOOST_REQUIRE(RunInCoroutine([&] { del2.emplace(fresh->Delete({key}, Opt())); }));
    BOOST_REQUIRE(del2 && *del2 && del2->value() == 1u);

    CloseAndCheck(fresh);
}

// ---- Issue #34：真实服务端在途等待 → 强制 TimedOut、物理收口、不复用、可恢复 ----
BOOST_AUTO_TEST_CASE(t_server_pause_forces_timedout_then_recover) {
    if (!LiveEnabled() || !PhaseIs('A')) {
        BOOST_TEST_MESSAGE("skip: 需要 BBT_TEST_REDIS_ADDR 且 PHASE=A");
        return;
    }
    const char* rcli = EnvOr("BBT_REDIS_RCLI");
    BOOST_REQUIRE_MESSAGE(rcli != nullptr,
        "redis.live #34: 缺 BBT_REDIS_RCLI，无法制造真实服务端在途等待");

    CloseOwnedClientsOnExit owned;
    auto cli = NewLiveClient();
    owned.Own(cli);
    std::optional<result<void>> base;
    BOOST_REQUIRE(RunInCoroutine([&] { base.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE_MESSAGE(base && *base, "健康基线 Connect+PING 必须先通过");

    auto impl = std::dynamic_pointer_cast<CoRedisCliImpl>(cli);
    BOOST_REQUIRE(impl);
    // 采集旧 transport（按值持有）：超时后直接断言其物理 closed，null 槽不独自作证。
    auto old_transport = impl->TransportForTest();
    BOOST_REQUIRE_MESSAGE(old_transport, "健康 PING 后必须有真实 transport");

    // R1：同 Close 用例——用既有 F3 fd 等待接缝证明「命令已真实挂起在读等待」，不用 registered。
    auto timed_wait = std::make_shared<std::atomic_bool>(false);
    detail::TransportWiring::SetIoWaitRegisteredGateForTest(
        *old_transport, [timed_wait] { timed_wait->store(true); });

    // 见证 client（独立连接，先于 pause 建连），与超时命令在同一 pause 窗口内并发发起。
    auto witness = NewLiveClient();
    owned.Own(witness);
    auto wimpl = std::dynamic_pointer_cast<CoRedisCliImpl>(witness);
    BOOST_REQUIRE(wimpl);
    auto w_transport = wimpl->TransportForTest();
    BOOST_REQUIRE_MESSAGE(w_transport, "见证 client 健康 PING 后必须有真实 transport");
    auto w_wait = std::make_shared<std::atomic_bool>(false);
    detail::TransportWiring::SetIoWaitRegisteredGateForTest(
        *w_transport, [w_wait] { w_wait->store(true); });

    // 因果握手：服务端声明挂起命令窗口（窗口 > 命令 deadline；单一常量）。
    BOOST_REQUIRE_EQUAL(SysBounded(rcli, PauseArgs()), 0);

    // R3：见证命令与超时命令并发发起并绑定同一 pause 窗口——其成功返回即「服务端已恢复」的
    // 可观测信号，不再用 came->ms 的固定时序下限（易受 pause 尾段抖动影响）。
    auto came     = std::make_shared<PingOutcome>();
    bool wstarted = false;
    g_scheduler->RegistCoroutineTask(
        [came, witness] {
            const auto t0 = std::chrono::steady_clock::now();
            came->r.emplace(witness->Ping(Opt(9000)));
            came->ms.store(MsSince(t0));
            came->done.store(true);
        },
        wstarted);
    BOOST_REQUIRE(wstarted);

    // 真实在途：PING 在窗口内发出，服务端不回复；客户端在自身 deadline（700ms）到期。
    auto timed = std::make_shared<PingOutcome>();
    BOOST_REQUIRE(RunInCoroutine([timed, cli] {
        const auto t0 = std::chrono::steady_clock::now();
        timed->r.emplace(cli->Ping(Opt(700)));
        timed->ms.store(MsSince(t0));
        timed->done.store(true);
    }));
    BOOST_REQUIRE(timed->r.has_value());
    BOOST_CHECK_MESSAGE(!*timed->r, "服务端被 pause 时在途命令必须超时而非假成功");
    BOOST_CHECK(timed->r->error().code == ErrorCode::TimedOut);
    // 在自身 deadline 到期（~700ms），早于 pause 结束（kServerPauseMs）：不是服务端回复。
    BOOST_CHECK_GE(timed->ms.load(), 600);
    BOOST_CHECK_LT(timed->ms.load(), kServerPauseMs);
    // R1：超时命令确实已进入真实 fd 读等待（非仅入队/空转）。
    BOOST_CHECK_MESSAGE(timed_wait->load(),
        "超时命令必须已真实挂起在 fd 读等待（非仅入队）");
    // R3 因果断言：超时命令落定当刻，见证命令仍被服务端挂起（同一 pause 窗口 ⇒ 不可能已回），
    // 不依赖固定时序下限。
    BOOST_CHECK_MESSAGE(!came->done.load(),
        "见证命令在超时时刻必须仍被服务端挂起（同一 pause 窗口）");

    // 读超时在调用方与 owner 侧并发落定：以可观测真实状态等待 owner 线性化收口
    // （registered 归零 + Failed），不靠 sleep 判定状态。
    BOOST_REQUIRE_MESSAGE(
        WaitUntil([&] {
            return impl->BindingStateSnapshot().registered == 0 &&
                   cli->ConnectStatus() == ConnectState::Failed;
        }, 3000),
        "读超时后连接必须线性化为 Failed 且在途登记归零");

    // 读超时后连接被判不可复用（sticky Failed）。
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Failed);

    // 物理收口：直接断言旧 transport 物理 closed + 无在途登记（不靠 null 槽单独作证）。
    BOOST_CHECK_MESSAGE(old_transport->IsClosed(),
                        "读超时后旧 transport 必须物理 closed");
    {
        const auto snap = impl->BindingStateSnapshot();
        BOOST_CHECK_MESSAGE(snap.conn_closed, "读超时后旧 transport 必须物理 closed");
        BOOST_CHECK_EQUAL(snap.registered, 0u);
    }
    BOOST_CHECK(TransportPhysicallyClosed(impl));

    // 不复用坏连接：默认不隐式重连，Failed 下新命令 sticky TransportError。
    std::optional<result<void>> reuse;
    BOOST_REQUIRE(RunInCoroutine([&] { reuse.emplace(cli->Ping(Opt(500))); }));
    BOOST_REQUIRE(reuse && !*reuse);
    BOOST_CHECK(reuse->error().code == ErrorCode::TransportError);

    // 见证命令：等其成功返回（pause 结束 ⇒ 服务端恢复）。其成功即「服务端已恢复」信号；
    // 其亦须已真实挂起在 fd 读等待（证明被服务端挂起而非空转）。
    BOOST_REQUIRE_MESSAGE(
        WaitUntil([&] { return came->done.load(); }, 2 * kServerPauseMs),
        "见证命令必须在界内落定（服务端恢复）");
    BOOST_REQUIRE(came->r.has_value());
    BOOST_CHECK_MESSAGE(*came->r, "pause 结束后服务端恢复：见证命令必须成功");
    BOOST_CHECK_MESSAGE(w_wait->load(),
        "见证命令必须已真实挂起在 fd 读等待（证明被服务端挂起而非空转）");
    BOOST_TEST_MESSAGE("witness elapsed_ms=" << came->ms.load() << " (仅诊断，不作断言)");

    // 服务端恢复（迟到回复窗口已过）后：先前超时请求终态仍 TimedOut、连接仍 Failed。
    BOOST_CHECK_MESSAGE(
        !*timed->r && timed->r->error().code == ErrorCode::TimedOut,
        "服务端恢复后，先前超时请求的终态不得被改成功");
    BOOST_CHECK(cli->ConnectStatus() == ConnectState::Failed);

    // 显式恢复：Failed 客户端显式 Connect 重建后 PING/KV 正常。
    std::optional<result<void>> rec;
    BOOST_REQUIRE(RunInCoroutine([&] { rec.emplace(cli->Connect(Opt(3000))); }));
    BOOST_REQUIRE(rec && *rec);
    std::optional<result<void>> ping2;
    BOOST_REQUIRE(RunInCoroutine([&] { ping2.emplace(cli->Ping(Opt())); }));
    BOOST_REQUIRE(ping2 && *ping2);
    const std::string key = "bbt:test:timeout:rec";
    const std::string val = BinaryPayload(8);
    std::optional<result<void>> set;
    BOOST_REQUIRE(RunInCoroutine([&] { set.emplace(cli->Set(key, val, Opt())); }));
    BOOST_REQUIRE(set && *set);
    std::optional<result<std::optional<std::string>>> get;
    BOOST_REQUIRE(RunInCoroutine([&] { get.emplace(cli->Get(key, Opt())); }));
    BOOST_REQUIRE(get && *get && get->value().has_value());
    BOOST_CHECK_EQUAL(*get->value(), val);
    std::optional<result<std::uint64_t>> del;
    BOOST_REQUIRE(RunInCoroutine([&] { del.emplace(cli->Delete({key}, Opt())); }));
    BOOST_REQUIRE(del && *del && del->value() == 1u);

    CloseAndCheck(cli);
    CloseAndCheck(witness);
}

BOOST_AUTO_TEST_SUITE_END()
