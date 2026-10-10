// Issue #7 真实 MongoDB 容器验收（docker compose 起 mongo:8.0，
// cpus<=0.75 / mem_limit<=512m / wiredTigerCacheSizeGB=0.25 / 动态项目名，
// 编排见 tests/mongo-live/run.sh）：
//
//   BBT_TEST_MONGO_URI=mongodb://host:port/   必填，未设置整件跳过
//   BBT_MONGO_PHASE=A|B|C                     默认 A：
//     A = 容器在线（CRUD、not-found、duplicate-key、driver 错误映射、
//         16 并发有界 smoke、close drain）
//     B = 容器已停（命令必须失败而非悬挂/假成功）
//     C = 容器重启后（新 client 亦可用）
//
// 有界 smoke 上限（Issue #7 验收）：并发 ≤16、单请求 deadline ≤3s、
// 总时长 ≤30s，成功/失败计数且 0 数据错误。
// BSON 文档构造经 bsoncxx::from_json（仅测试侧依赖，不进公开面）。
//
// Issue #34 A 阶段追加「真实后端可观察在途命令」的超时与 owner Close 用例：
//   BBT_MONGO_CTL_EVAL='docker compose ... exec -T mongo mongosh --quiet --eval'
//   由 tests/mongo-live/run.sh 注入容器内 mongosh 控制通道，用例经它注入
//   failCommand(blockConnection) 并以 waitForFailPoint 握手确认目标命令确实
//   停在服务端应答上（不靠 sleep 猜测在途）；缺该通道时相关用例显式失败，
//   不得静默 skip 变绿。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <sys/syscall.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <bsoncxx/document/element.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/CoMongoCli.hpp>
#include <bbt/infra/mongo/Client.hpp>

// Issue #34：用例需要真实生产状态观测（runtime 的 worker/执行槽计数、
// MongoCollImpl::Runtime）——读取 src/mongo/ 内部头（仅本测试目标引入内部
// include，不新增生产接缝，也不进入被测公共面）。
#include "mongo/MongoRuntime.hpp"

using namespace bbt::infra;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

constexpr int kBudgetMs = 25000;

const char* EnvOr(const char* k) { return std::getenv(k); }

bool LiveEnabled() {
    const char* a = EnvOr("BBT_TEST_MONGO_URI");
    return a != nullptr && a[0] != '\0';
}

bool PhaseIs(char p) {
    const char* v = EnvOr("BBT_MONGO_PHASE");
    return v == nullptr || v[0] == '\0' ? p == 'A' : v[0] == p;
}

MongoClientConfig LiveConfig(std::size_t workers = 2,
                             std::size_t queue   = 32) {
    const char* uri = EnvOr("BBT_TEST_MONGO_URI");
    BOOST_REQUIRE(uri != nullptr);
    MongoClientConfig cfg;
    cfg.uri                      = uri;
    cfg.database                 = "bbt_live";
    cfg.collection               = "c";
    cfg.worker_threads           = workers;
    cfg.max_queue                = queue;
    cfg.server_selection_timeout = std::chrono::milliseconds(5000);
    cfg.connect_timeout          = std::chrono::milliseconds(3000);
    cfg.socket_timeout           = std::chrono::milliseconds(3000);
    cfg.wait_queue_timeout       = std::chrono::milliseconds(3000);
    return cfg;
}

CallOptions Opt(int ms = 3000) {
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

void SleepMs(int ms) {
    timespec req{ms / 1000, static_cast<long>(ms % 1000) * 1000000L};
    timespec rem{};
    while (::syscall(SYS_nanosleep, &req, &rem) != 0 && errno == EINTR)
        req = rem;
}

// JSON → infra BSON 字节载体（bsoncxx 仅测试侧使用）。
MongoDocument JsonDoc(const std::string& j) {
    auto v  = bsoncxx::from_json(j);
    auto vw = v.view();
    MongoDocument d;
    d.bytes.assign(vw.data(), vw.data() + vw.length());
    return d;
}

// 读回文档的 string 字段（校验数据正确性，不只是存活）。
std::optional<std::string> FieldString(const MongoDocument& d,
                                     const char*           key) {
    bsoncxx::document::view v{d.bytes.data(), d.bytes.size()};
    const auto e = v[key];
    if (!e || e.type() != bsoncxx::type::k_string)
        return std::nullopt;
    const auto sv = e.get_string().value;
    return std::string(sv.data(), sv.size());
}

std::shared_ptr<CoMongoCli> NewLiveClient(std::size_t workers = 2) {
    auto c = CoMongoCli::Create(LiveConfig(workers));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    BOOST_REQUIRE(cli->Start());
    return cli;
}

// Issue #40 owner 配置的 live 形态：与 LiveConfig 相同的 URI/超时上界，
// 但不含 database/collection（目标由集合句柄携带）。
mongo::MongoRuntimeConfig LiveRuntimeConfig(std::size_t workers = 2,
                                            std::size_t queue   = 32) {
    const char* uri = EnvOr("BBT_TEST_MONGO_URI");
    BOOST_REQUIRE(uri != nullptr);
    mongo::MongoRuntimeConfig cfg;
    cfg.uri                      = uri;
    cfg.worker_threads           = workers;
    cfg.max_queue                = queue;
    cfg.server_selection_timeout = std::chrono::milliseconds(5000);
    cfg.connect_timeout          = std::chrono::milliseconds(3000);
    cfg.socket_timeout           = std::chrono::milliseconds(3000);
    cfg.wait_queue_timeout       = std::chrono::milliseconds(3000);
    return cfg;
}

// ---- Issue #34：真实后端可观察阻塞的测试控制通道 ----
//
// 被测命令是否「在途（已进入 driver 且在服务端受阻）」必须在真实后端可观察，
// 不能靠 sleep 猜测。这里用 mongod 自带的 failCommand failpoint：
//   mode{times:1} + blockConnection + blockTimeMS
// 让命中命令停在服务端连接的应答上（到点后照常执行，是延迟而非失败注入），
// waitForFailPoint 则作为「目标命令确已进入该阻塞」的后端握手。控制通道经
// run.sh 注入的容器内 mongosh 前缀执行（与 Redis live 的容器内 redis-cli
// 控制同源），只作用于本编排的临时容器，不改变被测公共面。
const char* ControlPrefix() { return std::getenv("BBT_MONGO_CTL_EVAL"); }

// 从控制命令的 JSON 输出取整数域：复用测试侧已用的 bsoncxx JSON 解析，
// 不新加 JSON 依赖、不自造 parser。非法 JSON / 缺字段返回 nullopt
// （调用方 fail closed，不把解析失败当通过）。
std::optional<int> JsonIntField(const std::string& text, const char* key) {
    try {
        auto doc = bsoncxx::from_json(text);
        auto e   = doc.view()[key];
        if (!e)
            return std::nullopt;
        switch (e.type()) {
            case bsoncxx::type::k_int32:
                return e.get_int32().value;
            case bsoncxx::type::k_int64:
                return static_cast<int>(e.get_int64().value);
            case bsoncxx::type::k_double:
                return static_cast<int>(e.get_double().value);
            default:
                return std::nullopt;
        }
    } catch (...) {
        return std::nullopt;
    }
}

// 安全 shell 单引号引用（JS 参数用；含单引号时按标准 '\'' 转义）。
std::string ShellQuote(const std::string& s) {
    std::string q = "'";
    for (char c : s) {
        if (c == '\'')
            q += "'\\''";
        else
            q += c;
    }
    q += "'";
    return q;
}

struct MongoCtl {
    int         rc = -1;   // -1 = 缺控制通道/未能执行
    std::string out;       // stdout（JSON）
};

// 控制命令自身的有界上界：必须覆盖容器内 mongosh 启动 + 一次
// waitForFailPoint(maxTimeMS=15000)，否则握手会被前端 timeout 误杀。
// --kill-after 保证超时后进程被强杀，docker/mongosh 挂起不会拖满外层
// PHASE_TIMEOUT。stderr 丢弃以保持 stdout 为纯 JSON。
constexpr int kCtlTimeoutS   = 45;
constexpr int kCtlKillAfterS = 10;

// 在容器内经 mongosh 执行一段 admin JS，返回真实 rc 与 stdout（JSON）。
//   - 走 popen（/bin/sh -c）：stdout 直接捕获、pclose 给真实退出码，
//     不落可预测临时文件、不用未引号的 shell 重定向；
//   - 前置 timeout --kill-after：控制命令真正有界；
//   - 缺控制通道时 rc 保持 -1：调用方必须显式失败，不得当作通过。
MongoCtl MongoExec(const std::string& js) {
    MongoCtl    r;
    const char* prefix = ControlPrefix();
    if (prefix == nullptr)
        return r;
    const std::string cmd = "timeout --kill-after=" +
                            std::to_string(kCtlKillAfterS) + " " +
                            std::to_string(kCtlTimeoutS) + " " + prefix + " " +
                            ShellQuote(js) + " 2>/dev/null";
    FILE* p = ::popen(cmd.c_str(), "r");
    if (p == nullptr)
        return r;
    char        buf[4096];
    std::size_t n;
    while ((n = std::fread(buf, 1, sizeof(buf), p)) > 0)
        r.out.append(buf, n);
    const int st = ::pclose(p);   // 正常 close：真实退出码
    if (st != -1 && WIFEXITED(st))
        r.rc = WEXITSTATUS(st);
    return r;
}

// 注入一次性阻塞故障：命中 command 的下一条命令在服务端阻塞 block_ms。
// 输出 JSON（JSON.stringify）以便用 bsoncxx 解析 arm 返回的累计 count 基线。
std::string ArmBlockingFailpoint(const char* command, int block_ms) {
    return std::string(
               "JSON.stringify(db.adminCommand({configureFailPoint:'failCommand',"
               "mode:{times:1},data:{failCommands:['") +
           command + "'],blockConnection:true,blockTimeMS:" +
           std::to_string(block_ms) + "}}))";
}

// 后端进入握手：armed 的 failCommand 仅在确有匹配命令进入后才返回。
// timesEntered 必须取「本次 arm 返回的累计 count + 1」——failpoint 计数是
// 累计值，同一 mongod 上第二次 arm 时基线已 >=1，写死 timesEntered:1 的
// 握手会立即假返回（把「未进入阻塞」误判成握手成功）。
std::string WfpHandshake(int times_entered) {
    return std::string(
               "JSON.stringify(db.adminCommand({waitForFailPoint:'failCommand',"
               "timesEntered:") +
           std::to_string(times_entered) + ",maxTimeMS:15000}))";
}
// 解除并回读累计注入计数（mode:'off' 不重置计数；用于核准 delta==1 与兜底解除）。
const char* kFailpointOff =
    "JSON.stringify(db.adminCommand({configureFailPoint:'failCommand',mode:'off'}))";

// #34 阻塞注入用 owner 配置：socket_timeout 必须大于注入的阻塞上界，
// 否则 driver 会在服务端阻塞结束前以 socket 超时返回，迟到结果就不是
// 「服务端真实成功」而是 driver 错误，观测失去意义。
mongo::MongoRuntimeConfig BlockRuntimeConfig(std::size_t workers = 1) {
    auto cfg           = LiveRuntimeConfig(workers, 4);
    cfg.socket_timeout = std::chrono::milliseconds(15000);
    return cfg;
}

// 经集合句柄取回 owner 内部 runtime（读取真实生产状态观测用）。
std::shared_ptr<mongo_detail::MongoRuntime> RuntimeOfColl(
    const std::shared_ptr<mongo::CoMongoColl>& h) {
    return std::static_pointer_cast<mongo_detail::MongoCollImpl>(h)
        ->Runtime();
}

long long NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// 显式 Close 并在有界窗口内确认真实收口（driver 同步调用不可强杀：
// socket_timeout/server_selection_timeout 接近 kCloseDrainTimeout 时 Close
// 可能提前返回，收口由 worker 完成路径补齐，故以有界轮询断言）。
void CloseAndCheck(const std::shared_ptr<CoMongoCli>& cli) {
    cli->Close();
    BOOST_REQUIRE(WaitUntil([&] { return cli->IsClosed(); }, 15000));
    BOOST_CHECK(cli->IsClosed());
}

// ---- Issue #34 修复：异步结果的堆所有权与失败路径收口 ----
//
// 新增两例的所有异步状态一律 shared_ptr 堆所有权、协程按值捕获：即使某次
// BOOST_REQUIRE 失败（Boost.Test 抛 execution_aborted）导致测试栈展开，协程
// 仍只访问堆对象，不悬空。旧的 RunInCoroutine([&]{...}, budget) 在超预算时
// 同样会栈展开而协程仍活（review 称其不受影响是错的），故新用例不再用它承载
// 可能长驻的在途操作——改用下面的 RunHeapOp（堆 state + 有界等待）。

// 在协程中执行 fn(state)：state 按值捕获；测试线程在有界窗口内等待 done。
// 返回 false 表示未启动或超预算——调用方 fail closed（不得当作通过）。
template <class State, class Fn>
bool RunHeapOp(const std::shared_ptr<State>& st, Fn fn,
               int budget_ms = kBudgetMs) {
    bool started = false;
    g_scheduler->RegistCoroutineTask(
        [st, fn]() mutable {
            fn(*st);
            st->done.store(true);
        },
        started);
    if (!started)
        return false;
    return WaitUntil([&] { return st->done.load(); }, budget_ms);
}

// 析构即 join 的线程守卫：任何 BOOST_REQUIRE 失败展开时先 join 再继续，
// 不以 detach 逃生、不因未 join 触发 std::terminate；join 会等 closer
// 内部的真实收口（driver 物理返回）完成后才返回。
class JoinGuard {
public:
    JoinGuard() = default;
    explicit JoinGuard(std::thread t) : m_t(std::move(t)) {}
    void Join() noexcept {
        if (m_t.joinable())
            m_t.join();
    }
    ~JoinGuard() { Join(); }
    JoinGuard(const JoinGuard&)            = delete;
    JoinGuard& operator=(const JoinGuard&) = delete;

private:
    std::thread m_t;
};

// 失败路径也必须收口 owner：析构即 Close（幂等 noexcept），保证已启动的
// 在途 op 与 worker 归零；不抛异常、不掩盖原始失败。
class OwnerCloseGuard {
public:
    explicit OwnerCloseGuard(std::shared_ptr<mongo::CoMongoDb> db)
        : m_db(std::move(db)) {}
    ~OwnerCloseGuard() {
        if (m_db)
            m_db->Close();
    }
    OwnerCloseGuard(const OwnerCloseGuard&)            = delete;
    OwnerCloseGuard& operator=(const OwnerCloseGuard&) = delete;

private:
    std::shared_ptr<mongo::CoMongoDb> m_db;
};

// armed 后无论正常或异常离开作用域都显式解除 failpoint（只解除、不断言，
// 不抛、不掩盖原失败）；后端阻塞由 blockTimeMS + times:1 双重有界。
class FailpointGuard {
public:
    void Armed() noexcept { m_armed = true; }
    ~FailpointGuard() {
        if (m_armed)
            MongoExec(kFailpointOff);
    }
    FailpointGuard()                                 = default;
    FailpointGuard(const FailpointGuard&)            = delete;
    FailpointGuard& operator=(const FailpointGuard&) = delete;

private:
    bool m_armed = false;
};

std::atomic_bool g_prepared{false};

} // namespace

// Issue #34：live 套件无 BBT_TEST_MONGO_URI 时必须显式标记 Skipped，不能
// 以退出码 0 冒充通过（CTest 会把 0 记为 Passed）。套件内每个 case 用
// BOOST_TEST_MESSAGE("skip: ...")+return 提前返回；Boost.Test 默认日志
// 级别下该消息不打印，SKIP_REGULAR_EXPRESSION 匹配不到文本。故在全局
// fixture 构造时检测环境，无环境直接 std::exit(77)——配合 CTest
// SKIP_RETURN_CODE=77 标为 Skipped，同时不伪造通过、也不进入任何用例。
struct RequireLiveEnv {
    RequireLiveEnv() {
        const char* a = std::getenv("BBT_TEST_MONGO_URI");
        if (a == nullptr || a[0] == '\0') {
            std::fputs("mongo.live: BBT_TEST_MONGO_URI 未设置，标记 Skipped\n",
                       stderr);
            std::exit(77);
        }
    }
};
BOOST_GLOBAL_FIXTURE(RequireLiveEnv);

// 进程寿命运行时：无 Stop/restart；实例持有者被 coroutine 故意泄漏，
// 静态退出期不析构 Scheduler，测试侧不做任何 Stop 收尾。

BOOST_AUTO_TEST_SUITE(mongo_live)

BOOST_AUTO_TEST_CASE(t_setup_scheduler) {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 4;
    cfg->m_cfg_stack_size        = 1024 * 256;
    g_scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(g_scheduler->IsInitialized());
    g_prepared.store(true);
}

BOOST_AUTO_TEST_CASE(t_crud_cycle) {
    if (!LiveEnabled() || !PhaseIs('A')) {
        BOOST_TEST_MESSAGE("skip: 需要 BBT_TEST_MONGO_URI 且 PHASE=A");
        return;
    }
    auto cli = NewLiveClient();

    const MongoDocument doc =
        JsonDoc(R"({"_id":"bbt:t:crud","v":"hello","n":7})");
    const MongoDocument filter = JsonDoc(R"({"_id":"bbt:t:crud"})");

    std::optional<result<void>> ins;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { ins.emplace(cli->InsertOne(doc, Opt())); }));
    BOOST_REQUIRE(ins && *ins);

    std::optional<result<std::optional<MongoDocument>>> f;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { f.emplace(cli->FindOne(filter, Opt())); }));
    BOOST_REQUIRE(f && *f && f->value().has_value());
    auto fv = FieldString(*f->value(), "v");
    BOOST_REQUIRE(fv.has_value());
    BOOST_CHECK_EQUAL(*fv, "hello");

    std::optional<result<MongoUpdateResult>> u;
    BOOST_REQUIRE(RunInCoroutine([&] {
        u.emplace(cli->UpdateOne(filter, JsonDoc(R"({"$set":{"v":"world"}})"),
                                 Opt()));
    }));
    BOOST_REQUIRE(u && *u);
    BOOST_CHECK_EQUAL(u->value().matched, 1);
    BOOST_CHECK_EQUAL(u->value().modified, 1);

    std::optional<result<std::optional<MongoDocument>>> f2;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { f2.emplace(cli->FindOne(filter, Opt())); }));
    BOOST_REQUIRE(f2 && *f2 && f2->value().has_value());
    auto fv2 = FieldString(*f2->value(), "v");
    BOOST_REQUIRE(fv2.has_value());
    BOOST_CHECK_EQUAL(*fv2, "world");

    std::optional<result<std::uint64_t>> del;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { del.emplace(cli->DeleteOne(filter, Opt())); }));
    BOOST_REQUIRE(del && *del);
    BOOST_CHECK_EQUAL(del->value(), 1u);

    // not-found：ok(nullopt) 而非错误。
    std::optional<result<std::optional<MongoDocument>>> miss;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { miss.emplace(cli->FindOne(filter, Opt())); }));
    BOOST_REQUIRE(miss && *miss);
    BOOST_CHECK(!miss->value().has_value());

    CloseAndCheck(cli);
}

BOOST_AUTO_TEST_CASE(t_duplicate_key) {
    if (!LiveEnabled() || !PhaseIs('A')) {
        BOOST_TEST_MESSAGE("skip: 需要 BBT_TEST_MONGO_URI 且 PHASE=A");
        return;
    }
    auto cli = NewLiveClient();

    const MongoDocument doc = JsonDoc(R"({"_id":"bbt:t:dup","v":1})");
    std::optional<result<void>> first;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { first.emplace(cli->InsertOne(doc, Opt())); }));
    BOOST_REQUIRE(first && *first);

    std::optional<result<void>> dup;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { dup.emplace(cli->InsertOne(doc, Opt())); }));
    BOOST_REQUIRE(dup && !*dup);
    BOOST_CHECK(dup->error().code == ErrorCode::RemoteError);
    BOOST_CHECK_EQUAL(dup->error().domain_code, "DuplicateKey");
    BOOST_CHECK_EQUAL(dup->error().backend_code, 11000);

    const MongoDocument filter = JsonDoc(R"({"_id":"bbt:t:dup"})");
    std::optional<result<std::uint64_t>> del;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { del.emplace(cli->DeleteOne(filter, Opt())); }));
    BOOST_REQUIRE(del && *del && del->value() == 1u);

    CloseAndCheck(cli);
}

BOOST_AUTO_TEST_CASE(t_driver_error_mapping) {
    if (!LiveEnabled() || !PhaseIs('A')) {
        BOOST_TEST_MESSAGE("skip: 需要 BBT_TEST_MONGO_URI 且 PHASE=A");
        return;
    }
    // 指向无监听端口的第二个 client：server selection 超时 → Unavailable，
    // 与同 client 的真实读写互不干扰。
    MongoClientConfig dead = LiveConfig(1);
    dead.uri = "mongodb://127.0.0.1:1/bbt_live";
    dead.server_selection_timeout = std::chrono::milliseconds(2000);
    auto c = CoMongoCli::Create(dead);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    BOOST_REQUIRE(cli->Start());

    std::optional<result<std::optional<MongoDocument>>> out;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { out.emplace(cli->FindOne(JsonDoc(R"({"_id":"x"})"), Opt())); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Unavailable);

    CloseAndCheck(cli);
}

BOOST_AUTO_TEST_CASE(t_concurrent_smoke_16) {
    if (!LiveEnabled() || !PhaseIs('A')) {
        BOOST_TEST_MESSAGE("skip: 需要 BBT_TEST_MONGO_URI 且 PHASE=A");
        return;
    }
    auto cli = NewLiveClient(2);

    constexpr int   kWorkers = 16;
    std::atomic_int done{0};
    std::atomic_int errors{0};
    std::atomic_int mismatches{0};
    const auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < kWorkers; ++i) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [&, i] {
                const std::string id =
                    "bbt:t:smoke:" + std::to_string(i);
                const auto doc = JsonDoc(
                    "{\"_id\":\"" + id + "\",\"v\":\"payload" +
                    std::to_string(i) + "\"}");
                const auto filter = JsonDoc("{\"_id\":\"" + id + "\"}");
                // 单请求 deadline ≤3s（Issue #7 有界上限）。
                auto s = cli->InsertOne(doc, Opt(3000));
                if (!s) {
                    errors.fetch_add(1);
                } else {
                    auto g = cli->FindOne(filter, Opt(3000));
                    if (!g) {
                        errors.fetch_add(1);
                    } else if (!g.value().has_value()) {
                        mismatches.fetch_add(1);
                    } else {
                        auto fv = FieldString(*g.value(), "v");
                        if (!fv || *fv != "payload" + std::to_string(i)) {
                            mismatches.fetch_add(1);
                        } else {
                            auto d = cli->DeleteOne(filter, Opt(3000));
                            if (!d || d.value() != 1u)
                                errors.fetch_add(1);
                        }
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
    BOOST_TEST_MESSAGE("smoke: elapsed_ms=" << ms << " errors=" << errors
                       << " mismatches=" << mismatches);
    BOOST_CHECK_LT(ms, 30000);
    BOOST_CHECK_EQUAL(errors.load(), 0);
    BOOST_CHECK_EQUAL(mismatches.load(), 0);

    CloseAndCheck(cli);
}

BOOST_AUTO_TEST_CASE(t_close_drains_inflight) {
    if (!LiveEnabled() || !PhaseIs('A')) {
        BOOST_TEST_MESSAGE("skip: 需要 BBT_TEST_MONGO_URI 且 PHASE=A");
        return;
    }
    auto cli = NewLiveClient(2);

    // 并发在途时 owner Close()：全部落定，逻辑结果只能是 ok/Closed
    // 之一，不得悬挂；Close 返回后在有界窗口内物理收口。
    constexpr int   kOps = 16;
    std::atomic_int done{0};
    std::atomic_int settled{0};
    for (int i = 0; i < kOps; ++i) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [&, i] {
                const std::string id =
                    "bbt:t:drain:" + std::to_string(i);
                auto r = cli->InsertOne(
                    JsonDoc("{\"_id\":\"" + id + "\"}"), Opt(30000));
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

    BOOST_REQUIRE(WaitUntil([&] { return cli->IsClosed(); }, 15000));
    BOOST_CHECK(cli->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_server_down_fails) {
    if (!LiveEnabled() || !PhaseIs('B')) {
        BOOST_TEST_MESSAGE("skip: 需要 PHASE=B（容器停止态）");
        return;
    }
    auto cli = NewLiveClient(1);
    std::optional<result<std::optional<MongoDocument>>> out;
    BOOST_REQUIRE(RunInCoroutine([&] {
        out.emplace(cli->FindOne(JsonDoc(R"({"_id":"x"})"), Opt(30000)));
    }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Unavailable);
    cli->Close();
    BOOST_CHECK(cli->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_fresh_client_after_restart) {
    if (!LiveEnabled() || !PhaseIs('C')) {
        BOOST_TEST_MESSAGE("skip: 需要 PHASE=C（容器重启态）");
        return;
    }
    auto cli = NewLiveClient();
    const MongoDocument doc = JsonDoc(R"({"_id":"bbt:t:phasec","v":"ok"})");
    const MongoDocument filter = JsonDoc(R"({"_id":"bbt:t:phasec"})");

    std::optional<result<void>> ins;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { ins.emplace(cli->InsertOne(doc, Opt())); }));
    BOOST_REQUIRE(ins && *ins);
    std::optional<result<std::optional<MongoDocument>>> f;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { f.emplace(cli->FindOne(filter, Opt())); }));
    BOOST_REQUIRE(f && *f && f->value().has_value());
    auto fv = FieldString(*f->value(), "v");
    BOOST_REQUIRE(fv.has_value());
    BOOST_CHECK_EQUAL(*fv, "ok");
    std::optional<result<std::uint64_t>> del;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { del.emplace(cli->DeleteOne(filter, Opt())); }));
    BOOST_REQUIRE(del && *del && del->value() == 1u);

    CloseAndCheck(cli);
}

BOOST_AUTO_TEST_CASE(t_owner_multi_collection_live) {
    if (!LiveEnabled() || !PhaseIs('A')) {
        BOOST_TEST_MESSAGE("skip: 需要 BBT_TEST_MONGO_URI 且 PHASE=A");
        return;
    }
    // Issue #40 live 验收：一个 owner（2 worker）上两个集合句柄并行
    // 真实 CRUD，共享同一组执行资源。
    auto d = mongo::CoMongoDb::Create(LiveRuntimeConfig(2, 16));
    BOOST_REQUIRE(d);
    auto db = std::move(d).value();
    BOOST_REQUIRE(db->Start());
    auto ha = db->Collection({"bbt_live", "c"});
    auto hb = db->Collection({"bbt_live", "c2"});
    BOOST_REQUIRE(ha && hb);
    auto ca = ha.value();
    auto cb = hb.value();

    // 两个集合各做一次真实写读回：数据不串集合（c 写 "a"、c2 写 "b"）。
    const MongoDocument da = JsonDoc(R"({"_id":"bbt:t:own:a","v":"a"})");
    const MongoDocument db_ = JsonDoc(R"({"_id":"bbt:t:own:b","v":"b"})");
    const MongoDocument fa = JsonDoc(R"({"_id":"bbt:t:own:a"})");
    const MongoDocument fb = JsonDoc(R"({"_id":"bbt:t:own:b"})");

    std::atomic_int ok{0};
    std::atomic_int errs{0};
    bool s1 = false, s2 = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            if (ca->InsertOne(da, Opt(3000))) {
                auto g = ca->FindOne(fa, Opt(3000));
                if (g && g.value().has_value()) {
                    auto fv = FieldString(*g.value(), "v");
                    if (fv && *fv == "a") ok.fetch_add(1);
                    else                errs.fetch_add(1);
                } else errs.fetch_add(1);
                auto d = ca->DeleteOne(fa, Opt(3000));
                if (!d || d.value() != 1u) errs.fetch_add(1);
            } else errs.fetch_add(1);
        },
        s1);
    g_scheduler->RegistCoroutineTask(
        [&] {
            if (cb->InsertOne(db_, Opt(3000))) {
                auto g = cb->FindOne(fb, Opt(3000));
                if (g && g.value().has_value()) {
                    auto fv = FieldString(*g.value(), "v");
                    if (fv && *fv == "b") ok.fetch_add(1);
                    else                errs.fetch_add(1);
                } else errs.fetch_add(1);
                auto d = cb->DeleteOne(fb, Opt(3000));
                if (!d || d.value() != 1u) errs.fetch_add(1);
            } else errs.fetch_add(1);
        },
        s2);
    BOOST_REQUIRE(s1 && s2);
    BOOST_REQUIRE(WaitUntil([&] { return ok.load() == 2; }, 20000));
    BOOST_CHECK_EQUAL(errs.load(), 0);

    ca->Close();
    cb->Close();
    db->Close();
    BOOST_REQUIRE(WaitUntil([&] { return db->IsClosed(); }, 15000));
    BOOST_CHECK(db->IsClosed());
}

// Issue #34：真实后端可观察在途命令的 deadline 超时。
//   - failCommand(blockConnection) 让目标 insert 真正停在服务端应答上，
//     waitForFailPoint 握手（timesEntered = 本次 arm 返回 count + 1）确认
//     「命令已进入该阻塞」，不靠 sleep 猜测在途；
//   - 此时 driver 调用仍占用执行槽，CallOptions.deadline 到点只交付一次逻辑
//     终态 TimedOut，不冒充物理收口；
//   - 服务端阻塞结束后 driver 返回真实成功：迟到结果只消费不交付，客户端
//     逻辑终态仍是 TimedOut，而该 insert 已在后端生效（两者同时成立）；
//   - 原 owner 恢复真实 CRUD（worker 已释放，未进入关闭态）。
// 所有异步状态堆所有权 + 协程按值捕获（栈展开不悬空）；owner/failpoint 由
// RAII 守卫在任意失败路径收口。
BOOST_AUTO_TEST_CASE(t_backend_blocked_deadline_timedout) {
    if (!LiveEnabled() || !PhaseIs('A')) {
        BOOST_TEST_MESSAGE("skip: 需要 BBT_TEST_MONGO_URI 且 PHASE=A");
        return;
    }
    // 缺控制通道不得静默 skip：否则无编排环境下新增验收会变绿却没跑。
    BOOST_REQUIRE_MESSAGE(
        ControlPrefix() != nullptr,
        "BBT_MONGO_CTL_EVAL 未注入：无法在后端确认命令进入阻塞，不得 skip 变绿");

    constexpr int kBlockMs = 8000;
    // 1 worker：唯一执行槽被在途 driver 调用占满，观测无歧义。
    auto d = mongo::CoMongoDb::Create(BlockRuntimeConfig(1));
    BOOST_REQUIRE(d);
    auto db = std::move(d).value();
    OwnerCloseGuard close_guard{db};
    BOOST_REQUIRE(db->Start());
    auto hres = db->Collection({"bbt_live", "c"});
    BOOST_REQUIRE(hres);
    auto h  = hres.value();
    auto rt = RuntimeOfColl(h);
    BOOST_REQUIRE(rt);

    // 健康基线：注入前该 owner 上真实 CRUD 闭环。
    {
        struct BaseState {
            std::atomic_bool            done{false};
            std::optional<result<void>> ins;
            std::optional<result<std::uint64_t>> del;
        };
        const MongoDocument base =
            JsonDoc(R"({"_id":"bbt:t:blkbase","v":"base"})");
        const MongoDocument basef = JsonDoc(R"({"_id":"bbt:t:blkbase"})");
        auto                bst   = std::make_shared<BaseState>();
        BOOST_REQUIRE(RunHeapOp(bst, [h, base, basef](BaseState& s) {
            s.ins.emplace(h->InsertOne(base, Opt()));
            s.del.emplace(h->DeleteOne(basef, Opt()));
        }));
        BOOST_REQUIRE(bst->ins && *bst->ins);
        BOOST_REQUIRE(bst->del && *bst->del && bst->del->value() == 1u);
    }

    // 注入：下一条 insert 在服务端阻塞 kBlockMs（一次性，到点自动解除并照常执行）。
    // 握手目标取本次 arm 返回的累计 count + 1；guard 保证任意失败路径显式解除。
    FailpointGuard fp;
    auto           arm = MongoExec(ArmBlockingFailpoint("insert", kBlockMs));
    fp.Armed();
    const auto base_count =
        arm.rc == 0 ? JsonIntField(arm.out, "count") : std::nullopt;
    BOOST_REQUIRE_MESSAGE(
        arm.rc == 0 && JsonIntField(arm.out, "ok") == 1 && base_count.has_value(),
        "注入 failCommand 失败: rc=" << arm.rc << " out=" << arm.out);
    const int target = *base_count + 1;
    BOOST_TEST_MESSAGE("m1[A] arm_count=" << *base_count
                                          << " wait_target=" << target);

    const MongoDocument blk  = JsonDoc(R"({"_id":"bbt:t:blk","v":"late"})");
    const MongoDocument blkf = JsonDoc(R"({"_id":"bbt:t:blk"})");
    struct TimeoutState {
        std::atomic_bool            done{false};
        std::atomic_bool            inflight_at_return{false};
        std::optional<result<void>> out;
    };
    auto st = std::make_shared<TimeoutState>();
    BOOST_REQUIRE(RunHeapOp(
        st,
        [h, rt, blk](TimeoutState& s) {
            s.out.emplace(h->InsertOne(blk, Opt(1200)));
            // 逻辑终态交付当刻：该 op 仍在执行槽内（driver 未返回）。
            s.inflight_at_return.store(rt->DriverCallsInFlightForTest() == 1);
        },
        30000));

    // 后端握手：waitForFailPoint 只在目标 insert 已进入阻塞 failpoint 后返回。
    auto hs = MongoExec(WfpHandshake(target));
    BOOST_TEST_MESSAGE("m1[A] handshake out=" << hs.out);
    BOOST_REQUIRE_MESSAGE(
        hs.rc == 0 && JsonIntField(hs.out, "ok") == 1,
        "后端未确认目标命令进入阻塞: rc=" << hs.rc << " out=" << hs.out);
    BOOST_CHECK_EQUAL(rt->DriverCallsInFlightForTest(), std::size_t{1});

    BOOST_REQUIRE(st->out.has_value() && !*st->out);
    BOOST_CHECK(st->out->error().code == ErrorCode::TimedOut);
    // TimedOut 是逻辑终态，不冒充物理收口：driver 仍在执行槽内。
    BOOST_CHECK(st->inflight_at_return.load());

    // driver 真实落定（服务端阻塞结束、应答返回）后执行槽归零。
    BOOST_REQUIRE(
        WaitUntil([&] { return rt->DriverCallsInFlightForTest() == 0; }, 20000));
    // 迟到结果只消费不交付：客户端逻辑终态仍是 TimedOut。
    BOOST_CHECK(st->out->error().code == ErrorCode::TimedOut);

    // 迟到结果在后端真实生效（延迟不等于失败），但未改写逻辑终态。
    {
        struct LandState {
            std::atomic_bool                                    done{false};
            std::optional<result<std::optional<MongoDocument>>> landed;
            std::optional<result<std::uint64_t>>                sweep;
        };
        auto lst = std::make_shared<LandState>();
        BOOST_REQUIRE(RunHeapOp(lst, [h, blkf](LandState& s) {
            s.landed.emplace(h->FindOne(blkf, Opt()));
            s.sweep.emplace(h->DeleteOne(blkf, Opt()));
        }));
        BOOST_REQUIRE(lst->landed && *lst->landed);
        BOOST_CHECK_MESSAGE(lst->landed->value().has_value(),
                            "迟到 insert 应在后端生效（failCommand 只延迟不失败）");
        BOOST_REQUIRE(lst->sweep && *lst->sweep && lst->sweep->value() == 1u);
    }

    // 注入计数核准：本次 arm 到 disarm 恰好拦截一次（delta==1）。
    auto       off       = MongoExec(kFailpointOff);
    const auto off_count = JsonIntField(off.out, "count");
    BOOST_TEST_MESSAGE("m1[A] disarm out=" << off.out);
    BOOST_REQUIRE_MESSAGE(off_count.has_value(),
                          "disarm 回读失败: rc=" << off.rc << " out=" << off.out);
    BOOST_CHECK_EQUAL(*off_count - *base_count, 1);

    // 原 timeout-owner 恢复真实 CRUD。
    {
        struct RecState {
            std::atomic_bool                                    done{false};
            std::optional<result<void>>                         ins;
            std::optional<result<std::optional<MongoDocument>>> g;
            std::optional<result<std::uint64_t>>                del;
        };
        const MongoDocument rec =
            JsonDoc(R"({"_id":"bbt:t:blkrec","v":"rec"})");
        const MongoDocument recf = JsonDoc(R"({"_id":"bbt:t:blkrec"})");
        auto                rst  = std::make_shared<RecState>();
        BOOST_REQUIRE(RunHeapOp(rst, [h, rec, recf](RecState& s) {
            s.ins.emplace(h->InsertOne(rec, Opt()));
            s.g.emplace(h->FindOne(recf, Opt()));
            s.del.emplace(h->DeleteOne(recf, Opt()));
        }));
        BOOST_REQUIRE(rst->ins && *rst->ins);
        BOOST_REQUIRE(rst->g && *rst->g && rst->g->value().has_value());
        BOOST_REQUIRE(rst->del && *rst->del && rst->del->value() == 1u);
    }
    BOOST_CHECK(!db->IsClosed());

    h->Close();
    db->Close();
    BOOST_REQUIRE(WaitUntil([&] { return db->IsClosed(); }, 15000));
    BOOST_CHECK(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), std::size_t{0});
}

// Issue #34：真实后端可观察在途命令遇 owner Close。
//   - 目标 find 被 failCommand(blockConnection) 停在服务端应答，握手（目标 =
//     本次 arm 返回 count + 1）确认进入；
//   - owner Close 从独立控制线程发起：该在途 op 先收到 Closed 逻辑终态
//     （Close 返回前、driver 仍在执行槽内），Close 再等 driver 真实返回；
//   - Close 返回当刻 IsClosed 成立、worker 归零、执行槽归零（非提前宣告）；
//   - 迟到 driver 结果不覆盖 Closed；新 owner 真实 CRUD 恢复。
// 所有异步状态堆所有权 + 协程/线程按值捕获（栈展开不悬空）；closer 用析构即
// join 的 JoinGuard（任何 BOOST_REQUIRE 失败都不 terminate、不 detach），
// owner 用 OwnerCloseGuard、failpoint 用 FailpointGuard 在任意失败路径收口。
BOOST_AUTO_TEST_CASE(t_backend_blocked_owner_close_closed) {
    if (!LiveEnabled() || !PhaseIs('A')) {
        BOOST_TEST_MESSAGE("skip: 需要 BBT_TEST_MONGO_URI 且 PHASE=A");
        return;
    }
    BOOST_REQUIRE_MESSAGE(
        ControlPrefix() != nullptr,
        "BBT_MONGO_CTL_EVAL 未注入：无法在后端确认命令进入阻塞，不得 skip 变绿");

    constexpr int kBlockMs = 8000;
    auto d = mongo::CoMongoDb::Create(BlockRuntimeConfig(1));
    BOOST_REQUIRE(d);
    auto db = std::move(d).value();
    OwnerCloseGuard close_guard{db};
    BOOST_REQUIRE(db->Start());
    auto hres = db->Collection({"bbt_live", "c"});
    BOOST_REQUIRE(hres);
    auto h  = hres.value();
    auto rt = RuntimeOfColl(h);
    BOOST_REQUIRE(rt);

    // 健康基线。
    {
        struct BaseState {
            std::atomic_bool                     done{false};
            std::optional<result<void>>          ins;
            std::optional<result<std::uint64_t>> del;
        };
        const MongoDocument base =
            JsonDoc(R"({"_id":"bbt:t:closebase","v":"base"})");
        const MongoDocument basef = JsonDoc(R"({"_id":"bbt:t:closebase"})");
        auto                bst   = std::make_shared<BaseState>();
        BOOST_REQUIRE(RunHeapOp(bst, [h, base, basef](BaseState& s) {
            s.ins.emplace(h->InsertOne(base, Opt()));
            s.del.emplace(h->DeleteOne(basef, Opt()));
        }));
        BOOST_REQUIRE(bst->ins && *bst->ins);
        BOOST_REQUIRE(bst->del && *bst->del && bst->del->value() == 1u);
    }

    // 在途 op 状态（堆所有权、按值捕获）——测试栈展开也不悬空。
    struct CloseOpState {
        std::atomic_bool                                    done{false};
        std::atomic_bool                                    inflight_at_return{false};
        std::atomic<long long>                              t_op_done_ns{0};
        std::optional<result<std::optional<MongoDocument>>> out;
    };
    const MongoDocument flt = JsonDoc(R"({"_id":"bbt:t:blkclose"})");
    auto                op  = std::make_shared<CloseOpState>();

    FailpointGuard fp;
    auto           arm = MongoExec(ArmBlockingFailpoint("find", kBlockMs));
    fp.Armed();
    const auto base_count =
        arm.rc == 0 ? JsonIntField(arm.out, "count") : std::nullopt;
    BOOST_REQUIRE_MESSAGE(
        arm.rc == 0 && JsonIntField(arm.out, "ok") == 1 && base_count.has_value(),
        "注入 failCommand 失败: rc=" << arm.rc << " out=" << arm.out);
    const int target = *base_count + 1;
    BOOST_TEST_MESSAGE("m1[B] arm_count=" << *base_count
                                          << " wait_target=" << target);

    // 启动在途 find（长 deadline：只能由 owner Close 结束）。
    bool launched = false;
    g_scheduler->RegistCoroutineTask(
        [op, h, rt, flt] {
            op->out.emplace(h->FindOne(flt, Opt(30000)));
            // Closed 逻辑终态交付当刻：driver 仍在执行槽内。
            op->inflight_at_return.store(rt->DriverCallsInFlightForTest() == 1);
            op->t_op_done_ns.store(NowNs());
            op->done.store(true);
        },
        launched);
    BOOST_REQUIRE(launched);

    // 后端握手 + 在途确认：确保 Close 发生在该 op 已被 driver 占用期间。
    auto hs = MongoExec(WfpHandshake(target));
    BOOST_TEST_MESSAGE("m1[B] handshake out=" << hs.out);
    BOOST_REQUIRE_MESSAGE(
        hs.rc == 0 && JsonIntField(hs.out, "ok") == 1,
        "后端未确认目标命令进入阻塞: rc=" << hs.rc << " out=" << hs.out);
    BOOST_CHECK_EQUAL(rt->DriverCallsInFlightForTest(), std::size_t{1});

    // owner Close 从独立控制线程发起（与被测协程线程分离）。
    // 状态堆所有权、线程按值捕获；JoinGuard 保证任何失败先 join 再展开。
    struct CloserState {
        std::atomic_bool       closed_done{false};
        std::atomic_bool       closed_at_return{false};
        std::atomic<long long> t_close_start_ns{0};
        std::atomic<long long> t_close_end_ns{0};
        std::atomic<long long> workers_at_return{-1};
        std::atomic<long long> inflight_at_close_return{-1};
    };
    auto     cst = std::make_shared<CloserState>();
    JoinGuard joiner{std::thread([db, rt, cst] {
        cst->t_close_start_ns.store(NowNs());
        db->Close();
        cst->t_close_end_ns.store(NowNs());
        // 「返回当刻」的物理收口事实。
        cst->closed_at_return.store(db->IsClosed());
        cst->workers_at_return.store(
            static_cast<long long>(rt->LiveWorkersForTest()));
        cst->inflight_at_close_return.store(
            static_cast<long long>(rt->DriverCallsInFlightForTest()));
        cst->closed_done.store(true);
    })};

    // Closed 逻辑终态先于 Close 返回交付（在途 driver 仍占用执行槽）。
    BOOST_REQUIRE(WaitUntil([&] { return op->done.load(); }, 30000));
    BOOST_REQUIRE(op->out.has_value() && !*op->out);
    BOOST_CHECK(op->out->error().code == ErrorCode::Closed);
    BOOST_CHECK(op->inflight_at_return.load());

    BOOST_REQUIRE(WaitUntil([&] { return cst->closed_done.load(); }, 30000));
    joiner.Join();   // 显式 join：等真实 driver 物理落定；析构兜底

    // Close 返回当刻：物理收口已完成（IsClosed + worker/执行槽归零）。
    BOOST_CHECK(cst->closed_at_return.load());
    BOOST_CHECK(db->IsClosed());
    BOOST_CHECK_EQUAL(cst->workers_at_return.load(), static_cast<long long>(0));
    BOOST_CHECK_EQUAL(cst->inflight_at_close_return.load(),
                      static_cast<long long>(0));
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), std::size_t{0});
    // 逻辑 Closed 先于 Close 返回；Close 等待了 driver 真实返回（非提前宣告）。
    BOOST_CHECK(op->t_op_done_ns.load() < cst->t_close_end_ns.load());
    const auto waited_ms =
        (cst->t_close_end_ns.load() - cst->t_close_start_ns.load()) / 1000000;
    BOOST_TEST_MESSAGE("owner close waited_ms=" << waited_ms);
    BOOST_CHECK(waited_ms >= 1000);
    // 迟到 driver 结果不覆盖已交付的 Closed。
    BOOST_CHECK(op->out->error().code == ErrorCode::Closed);

    // 注入计数核准：本次 arm 到 disarm 恰好拦截一次（delta==1），并显式解除。
    auto       off       = MongoExec(kFailpointOff);
    const auto off_count = JsonIntField(off.out, "count");
    BOOST_TEST_MESSAGE("m1[B] disarm out=" << off.out);
    BOOST_REQUIRE_MESSAGE(off_count.has_value(),
                          "disarm 回读失败: rc=" << off.rc << " out=" << off.out);
    BOOST_CHECK_EQUAL(*off_count - *base_count, 1);

    // 新 owner 真实 CRUD 恢复（容器在线、failpoint 已解除）。
    auto d2 = mongo::CoMongoDb::Create(BlockRuntimeConfig(1));
    BOOST_REQUIRE(d2);
    auto db2 = std::move(d2).value();
    OwnerCloseGuard close_guard2{db2};
    BOOST_REQUIRE(db2->Start());
    auto h2r = db2->Collection({"bbt_live", "c"});
    BOOST_REQUIRE(h2r);
    auto h2  = h2r.value();
    auto rt2 = RuntimeOfColl(h2);
    BOOST_REQUIRE(rt2);

    {
        struct RecState2 {
            std::atomic_bool                                    done{false};
            std::optional<result<void>>                         ins;
            std::optional<result<std::optional<MongoDocument>>> g;
            std::optional<result<std::uint64_t>>                del;
        };
        const MongoDocument rec2 =
            JsonDoc(R"({"_id":"bbt:t:closefresh","v":"ok"})");
        const MongoDocument rec2f =
            JsonDoc(R"({"_id":"bbt:t:closefresh"})");
        auto rst2 = std::make_shared<RecState2>();
        BOOST_REQUIRE(RunHeapOp(rst2, [h2, rec2, rec2f](RecState2& s) {
            s.ins.emplace(h2->InsertOne(rec2, Opt()));
            s.g.emplace(h2->FindOne(rec2f, Opt()));
            s.del.emplace(h2->DeleteOne(rec2f, Opt()));
        }));
        BOOST_REQUIRE(rst2->ins && *rst2->ins);
        BOOST_REQUIRE(rst2->g && *rst2->g && rst2->g->value().has_value());
        auto fv2 = FieldString(*rst2->g->value(), "v");
        BOOST_REQUIRE(fv2.has_value());
        BOOST_CHECK_EQUAL(*fv2, "ok");
        BOOST_REQUIRE(rst2->del && *rst2->del && rst2->del->value() == 1u);
    }

    h2->Close();
    db2->Close();
    BOOST_REQUIRE(WaitUntil([&] { return db2->IsClosed(); }, 15000));
    BOOST_CHECK(db2->IsClosed());
    BOOST_CHECK_EQUAL(rt2->LiveWorkersForTest(), std::size_t{0});
}

BOOST_AUTO_TEST_SUITE_END()
