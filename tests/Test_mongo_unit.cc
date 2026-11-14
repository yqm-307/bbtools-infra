#include <bbt/infra/mongo/detail/CoMongoCliImpl.hpp>
#include <type_traits>

static_assert(!std::is_abstract_v<bbt::infra::CoMongoCli>);
static_assert(std::is_final_v<bbt::infra::CoMongoCli>);
static_assert(!std::is_copy_constructible_v<bbt::infra::CoMongoCli>);
static_assert(!std::is_base_of_v<bbt::infra::CoMongoCli, bbt::infra::mongo_detail::CoMongoCliImpl>);

/**
 * docs/decisions/0004-mongo-client-mongocxx-dependency.md 单元验收（不依赖真实 MongoDB/容器）：
 *
 * t_setup_scheduler — 启动 Scheduler（共享 executor 来源）。
 * t_create_before_scheduler — 运行时未初始化（从未 Start）时 Create →
 * RuntimeUnavailable（对象身份需要已
 * 初始化的运行时）。
 * t_config_validation — 非法装配逐项 InvalidArgument。
 * t_command_prechecks — 未 Start RuntimeUnavailable；空文档/
 * 空 filter 空 update InvalidArgument；
 * Close 后 Closed。
 * t_invalid_context_plain — 普通线程直接调命令 → InvalidContext。
 * t_invalid_bson — 不良构 BSON → InvalidArgument（校验在
 * worker 内、driver 调用之前，不需要服务端）。
 * t_server_unreachable — server selection 超时 → Unavailable
 * （driver 侧失败映射）。
 * t_capacity_overloaded — worker_threads=1 时一个 op 占住 worker
 * （RunningDriverCalls 观测），queue=2 占满
 * 后第 4 个命令确定性 Overloaded。
 * t_deadline_only — 不可达地址下 deadline → TimedOut；逻辑
 * 返回时物理 driver 调用仍在进行（计数
 * 不变），不冒充物理收口。
 * t_close_during_inflight — 在途 driver 调用随 owner Close() 落定
 * Closed；物理收口（driver 返回 + worker
 * 退出）才 IsClosed。
 * t_worker_threads_bound — 4 个并发 op 峰值 driver 调用 ≤ 2。
 * t_owner_thread_identity — 单 worker 跨两个集合的 acquire/use/release
 * 线程 identity 均相同（内部观测证据）。
 *
 * 关闭语义：Close() 幂等、任意线程可调用——封口 + 唤醒等待者 + 有界等待在途
 * 归零；driver 同步调用不可强杀，故 Close 可能先返回、IsClosed() 在 driver
 * 返回且 worker 全退之后才成立。用例统一经 CloseClientDrained() 在有界窗口内
 * 确认真实收口。
 *
 * 两个验证面（观测数据已迁入 bbt/infra/debug/InfraDebug.hpp）：
 * Debug 面（BBT_INFRA_STRINGENT_DEBUG=ON）— 追加 debug 观测断言：运行中
 * driver 调用计数/峰值（并发上限）、执行域取证（同一次调用
 * acquire/use/release 同 worker）、以及「逻辑返回≠物理收口」的在途计数；
 * Release 面（宏 OFF）— debug 组件与本用例的 debug 访问器根本不存在。用例
 * 退回真实生产状态与结果断言：LiveWorkersForTest()、QueuedOpsForTest()
 * （真实队列深度，用作同步）、IsClosed()、错误码、结果计数。故关掉宏不会
 * 让任何用例恒过。同步点一律用真实事件（队列深度/结果闩），不用 sleep。
 */
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

#include <dirent.h>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Hook.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/CoMongoCli.hpp>

/**
 * impl 观测钩子分两类：
 * - 真实生产状态（Release 下同样可用）：LiveWorkersForTest /
 * QueuedOpsForTest（读 m_live_workers / m_queue）；
 * - debug 观测层（bbt/infra/debug/InfraDebug.hpp，仅 BBT_INFRA_STRINGENT_DEBUG
 * 下存在）：RunningDriverCallsForTest / PeakDriverCallsForTest /
 * LastDriverThreadEvidenceForTest。
 * 各用例按「Debug 面（宏 ON，追加观测断言）/ Release 面（宏 OFF，退回真实
 * 状态与结果断言，不得恒过）」两面组织，见用例内 #ifdef。MongoRuntime 是
 * docs/decisions/0007-mongo-owner-handles.md 的资源 owner。
 */
#include "bbt/infra/mongo/detail/MongoRuntime.hpp"

/**
 * 直接经真实 driver 构造非法 URI，核对 ClassifyDriverError
 * 的输入错误分类（URI 解析离线完成，不需要服务端）。
 */
#include <mongocxx/v1/uri.hpp>

using namespace bbt::infra;
using bbt::coroutine::Deadline;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

constexpr int kBudgetMs = 45000;

/**
 * 调度器静态 worker 数：默认 2 保持既有二进制行为；CTest mongo.close_single_worker
 * 用环境变量固定为 1，在独立进程里证明 Close 等待不会阻塞唯一的 Co worker。
 */
int StaticThreadCount()
{
    if (const char* env = std::getenv("BBT_MONGO_TEST_STATIC_THREADS")) {
        char* end = nullptr;
        const long n = std::strtol(env, &end, 10);
        if (end != env && *end == '\0' && n >= 1 && n <= 64)
            return static_cast<int>(n);
    }
    return 2;
}

mongo::MongoRuntimeConfig MakeUriCfg(std::string uri)
{
    mongo::MongoRuntimeConfig cfg;
    cfg.uri = std::move(uri);
    cfg.worker_threads = 2;
    cfg.max_queue = 8;
    cfg.server_selection_timeout = std::chrono::milliseconds(15000);
    cfg.connect_timeout = std::chrono::milliseconds(2000);
    cfg.socket_timeout = std::chrono::milliseconds(2000);
    cfg.wait_queue_timeout = std::chrono::milliseconds(2000);
    return cfg;
}

/**
 * 127.0.0.1:1 惯例无监听：server selection 必然在 serverSelectionTimeoutMS
 * 后失败——driver 侧阻塞有确定上界，且期间 worker 被物理占用。
 */
MongoClientConfig MakeDeadCfg(std::size_t workers = 2, std::size_t queue = 8, int sst_ms = 15000)
{
    MongoClientConfig cfg;
    cfg.uri = "mongodb://127.0.0.1:1/bbt_ut";
    cfg.database = "bbt_ut";
    cfg.collection = "c";
    cfg.worker_threads = workers;
    cfg.max_queue = queue;
    cfg.server_selection_timeout = std::chrono::milliseconds(sst_ms);
    cfg.connect_timeout = std::chrono::milliseconds(2000);
    cfg.socket_timeout = std::chrono::milliseconds(2000);
    cfg.wait_queue_timeout = std::chrono::milliseconds(2000);
    return cfg;
}

/**
 * 良构最小 BSON：空文档 {}（int32 长度 + 终止符）。
 */
MongoDocument EmptyDoc()
{
    return MongoDocument{{0x05, 0x00, 0x00, 0x00, 0x00}};
}

/**
 * 非空谓词/update 文档（公共头 JSON 构造；unit 侧无 bsoncxx，走
 * MongoDocument::FromJson——新入口用例需要非空 filter/update）。
 */
MongoDocument NonEmptyDoc()
{
    auto d = MongoDocument::FromJson(R"({"k":{"$gt":0}})");
    BOOST_REQUIRE(d);
    return std::move(d).value();
}

MongoDocument UpdateDoc()
{
    auto d = MongoDocument::FromJson(R"({"$set":{"v":1}})");
    BOOST_REQUIRE(d);
    return std::move(d).value();
}

/**
 * 不良构 BSON：声明长度超出实际缓冲。
 */
MongoDocument BadDoc()
{
    return MongoDocument{{0x10, 0x00, 0x00, 0x00, 0x00}};
}

CallOptions Opt(int budget_ms = 30000)
{
    CallOptions opt;
    opt.deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(budget_ms);
    return opt;
}

/**
 * 协程内执行并限时等待；任何一步失败返回 false（测试不得无限挂住）。
 */
template <class F> bool RunInCoroutine(F&& f, int budget_ms = kBudgetMs)
{
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

bool WaitUntil(const std::function<bool()>& pred, int budget_ms = kBudgetMs)
{
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(budget_ms);
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::yield();
    }
    return true;
}

/**
 * 物理线程数（/proc/self/task）： M1 用它确定性观察 worker 组
 * 收口，替代「猜时序」的 sleep 断言。
 */
std::size_t ThreadCount()
{
    std::size_t n = 0;
    DIR* d = ::opendir("/proc/self/task");
    if (d == nullptr)
        return 0;
    while (const auto* e = ::readdir(d)) {
        const char* name = e->d_name;
        if (name[0] == '.' && (name[1] == '\0' || (name[1] == '.' && name[2] == '\0')))
            continue;
        ++n;
    }
    ::closedir(d);
    return n;
}

/**
 * client 工厂：成功则返回托管对象。
 */
result<std::shared_ptr<CoMongoCli>> NewClient(const MongoClientConfig& cfg, bool start = true)
{
    auto c = CoMongoCli::Create(cfg);
    if (!c)
        return result<std::shared_ptr<CoMongoCli>>::err(std::move(c).error());
    auto cli = std::move(c).value();
    if (start) {
        auto st = cli->Start();
        if (!st)
            return result<std::shared_ptr<CoMongoCli>>::err(std::move(st).error());
    }
    return result<std::shared_ptr<CoMongoCli>>::ok(std::move(cli));
}

/**
 * 取 impl 观测钩子（测试只读计数，不触碰内部状态）。
 */
std::shared_ptr<mongo_detail::CoMongoCliImpl> ImplOf(const std::shared_ptr<CoMongoCli>& cli)
{
    return mongo_detail::CoMongoCliImpl::ForTest(cli);
}

/**
 * 显式 Close 并断言「返回即物理落定」：Close 同步等待在途 driver 调用返回
 * 与 worker 全退，返回时 IsClosed() 必须已为真。不再用「Close 后轮询
 * IsClosed」的隐性两阶段替身掩盖同步承诺。
 */
void CloseClientDrained(const std::shared_ptr<CoMongoCli>& cli)
{
    (void)cli->Close();
    BOOST_REQUIRE(cli->IsClosed());
}

std::atomic_bool g_prepared{false};

mongo::MongoRuntimeConfig MakeRtCfg(std::size_t workers = 2, std::size_t queue = 8, int sst_ms = 15000)
{
    mongo::MongoRuntimeConfig cfg;
    cfg.uri = "mongodb://127.0.0.1:1/bbt_ut";
    cfg.worker_threads = workers;
    cfg.max_queue = queue;
    cfg.server_selection_timeout = std::chrono::milliseconds(sst_ms);
    cfg.connect_timeout = std::chrono::milliseconds(2000);
    cfg.socket_timeout = std::chrono::milliseconds(2000);
    cfg.wait_queue_timeout = std::chrono::milliseconds(2000);
    return cfg;
}

/**
 * Close(Immediate) 等待在途 driver 调用物理落定的预算：须覆盖不可达地址的
 * server selection 上限并留出调度抖动余量。与 server selection 超时同值时
 * 两者零余量竞态——driver 落定略晚于名义超时，Close 就超时（负载下偶发）。
 */
constexpr auto kInflightCloseBudget = std::chrono::seconds{20};

std::shared_ptr<mongo::CoMongoDb> NewOwner(const mongo::MongoRuntimeConfig& cfg)
{
    auto d = mongo::CoMongoDb::Create(cfg);
    BOOST_REQUIRE(d);
    auto db = std::move(d).value();
    BOOST_REQUIRE(db->Start());
    return db;
}

/**
 * owner 对象 → 内部 runtime（观测 worker/计数）。CoMongoDbImpl 定义在
 * MongoRuntime.cc 匿名命名空间，测试经 GetObjectInfo 拿不到 runtime；
 * 这里用 Collection 的 impl 反查 owner runtime。
 */
std::shared_ptr<mongo_detail::MongoRuntime> RuntimeOf(const std::shared_ptr<mongo::CoMongoDb>& db)
{
    auto h = db->Collection({"bbt_ut", "probe"});
    BOOST_REQUIRE(h);
    auto impl = std::static_pointer_cast<mongo_detail::MongoCollImpl>(h.value());
    return impl->Runtime();
}

/**
 * owner Close + 有界等待物理收口，并断言 worker 全退（真实生产状态，
 * Release 面同样成立）。Debug 面追加在途 driver 调用计数归零。
 */
void CloseOwnerAndDrain(
    const std::shared_ptr<mongo::CoMongoDb>& db, const std::shared_ptr<mongo_detail::MongoRuntime>& rt)
{
    BOOST_REQUIRE(db->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + std::chrono::seconds{8}}));
    BOOST_REQUIRE(db->IsClosed());
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(rt->RunningDriverCallsForTest(), 0);
#endif
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
}

// clang-format off
}
// clang-format on

/**
 * 进程寿命运行时：无 Stop/restart；实例持有者被 coroutine 故意泄漏，
 * 静态退出期不析构 Scheduler，测试侧不做任何 Stop 收尾。
 */
BOOST_AUTO_TEST_SUITE(mongo_unit)

BOOST_AUTO_TEST_CASE(t_effective_uri_decoy_in_value)
{

    /**
     * 参数值中含诱饵子串（connectTimeoutMS=/socketTimeoutMS）：真实 query
     * key 未出现，仍须按序注入全部缺省项，且诱饵值原样保留。
     */
    const auto out =
        mongo_detail::EffectiveUri(MakeUriCfg("mongodb://host/db?replicaSet=connectTimeoutMS=999&foo=socketTimeoutMS"));
    BOOST_CHECK_EQUAL(
        out,
        "mongodb://host/db?replicaSet=connectTimeoutMS=999&foo=socketTimeoutMS"
        "&serverselectiontimeoutms=15000&connecttimeoutms=2000"
        "&sockettimeoutms=2000&waitqueuetimeoutms=2000&maxpoolsize=2");
}

BOOST_AUTO_TEST_CASE(t_effective_uri_case_insensitive_existing)
{

    /**
     * 已有同名 key（任意大小写）不重复追加；其余按既有 append 顺序以
     * '&' 追加；URI 原有大小写保留。
     */
    const auto out =
        mongo_detail::EffectiveUri(MakeUriCfg("mongodb://host/db?CONNECTTIMEOUTMS=4000&ServerSelectionTimeoutMS=9000"));
    BOOST_CHECK_EQUAL(
        out,
        "mongodb://host/db?CONNECTTIMEOUTMS=4000&ServerSelectionTimeoutMS=9000"
        "&sockettimeoutms=2000&waitqueuetimeoutms=2000&maxpoolsize=2");
}

BOOST_AUTO_TEST_CASE(t_effective_uri_no_query_uses_question)
{

    /**
     * 无 query：首个注入项接 '?'，其余以 '&' 分隔。
     */
    const auto out = mongo_detail::EffectiveUri(MakeUriCfg("mongodb://host:27017/bbt_ut"));
    BOOST_CHECK_EQUAL(
        out,
        "mongodb://host:27017/bbt_ut?serverselectiontimeoutms=15000"
        "&connecttimeoutms=2000&sockettimeoutms=2000"
        "&waitqueuetimeoutms=2000&maxpoolsize=2");
}

BOOST_AUTO_TEST_CASE(t_effective_uri_userinfo_decoy)
{

    /**
     * userinfo 与参数值中的 "connectTimeoutMS=" 诱饵不得误判为真实 key：
     * 仍以 '?' 追加注入项，诱饵原样保留。
     */
    const auto out =
        mongo_detail::EffectiveUri(MakeUriCfg("mongodb://connectTimeoutMS=999:pw999@host/bb?t=connectTimeoutMS"));
    BOOST_CHECK_EQUAL(
        out,
        "mongodb://connectTimeoutMS=999:pw999@host/bb?t=connectTimeoutMS"
        "&serverselectiontimeoutms=15000&connecttimeoutms=2000"
        "&sockettimeoutms=2000&waitqueuetimeoutms=2000&maxpoolsize=2");
}

BOOST_AUTO_TEST_CASE(t_driver_invalid_uri_is_invalid_argument)
{

    /**
     * 真实 driver 对非法 URI/选项抛 mongocxx::v1::exception
     * （mongoc 域，码 22 = MONGOC_ERROR_COMMAND_INVALID_ARG；mongoc 的
     * MONGOC_URI_ERROR 用它汇报无效 host specifier / 选项值非法）。经
     * ClassifyDriverError 必须稳定映射为调用方输入错误 InvalidArgument，
     * 而非 mongoc 通用 Unavailable。URI 解析离线完成，不需要服务端。
     */
    const std::vector<std::string> rejected = {

        /**
         * 端口越界
         */
        "mongodb://127.0.0.1:99999999/bbt_ut",

        /**
         * 端口非数字
         */
        "mongodb://127.0.0.1:abc/bbt_ut",

        /**
         * 选项值非数字
         */
        "mongodb://127.0.0.1:27017/bbt_ut?maxPoolSize=notanum",
    };
    for (const auto& uri : rejected) {
        bool threw = false;
        try {
            mongocxx::v1::uri parsed{bsoncxx::v1::stdx::string_view{uri}};
            (void)parsed;
        } catch (const mongocxx::v1::exception& e) {
            threw = true;
            const auto err = mongo_detail::ClassifyDriverError(e, "mongo: init driver pool");
            BOOST_CHECK_MESSAGE(err.code == ErrorCode::InvalidArgument, "uri=" << uri << " code=" << (int)err.code);
            BOOST_CHECK_EQUAL(err.backend_category, "mongocxx");
        }
        BOOST_CHECK_MESSAGE(threw, "driver 未拒绝: " << uri);
    }

    /**
     * 对照：真实 driver 接受（未知选项仅警告、负值 / 畸形 IPv6 不报错）——
     * 不得盲目断言这些应被拒绝。
     */
    const std::vector<std::string> accepted = {
        "mongodb://127.0.0.1:27017/bbt_ut?connectTimeoutMS=-5",
        "mongodb://127.0.0.1:27017/bbt_ut?bogusOption=1",
        "mongodb://[badipv6/bbt_ut",
    };
    for (const auto& uri : accepted) {
        bool threw = false;
        try {
            mongocxx::v1::uri parsed{bsoncxx::v1::stdx::string_view{uri}};
            (void)parsed;
        } catch (const mongocxx::v1::exception&) {
            threw = true;
        }
        BOOST_CHECK_MESSAGE(!threw, "driver 意外拒绝: " << uri);
    }

    /**
     * 脱敏：非法 URI 的错误文本不得泄露 userinfo 中的口令。
     */
    try {
        mongocxx::v1::uri parsed{
            bsoncxx::v1::stdx::string_view{"mongodb://user:secretpw@127.0.0.1:27017/bbt_ut?maxPoolSize=bad"}};
        (void)parsed;
        BOOST_CHECK_MESSAGE(false, "driver 未拒绝口令诱饵 URI");
    } catch (const mongocxx::v1::exception& e) {
        const auto err = mongo_detail::ClassifyDriverError(e, "mongo: init driver pool");
        BOOST_CHECK(err.code == ErrorCode::InvalidArgument);
        BOOST_CHECK_MESSAGE(err.message.find("secretpw") == std::string::npos, "错误文本泄露口令: " << err.message);
    }
}

BOOST_AUTO_TEST_CASE(t_create_before_scheduler)
{
    auto c = CoMongoCli::Create(MakeDeadCfg());
    BOOST_REQUIRE(!c);
    BOOST_CHECK(c.error().code == ErrorCode::RuntimeUnavailable);
}

BOOST_AUTO_TEST_CASE(t_setup_scheduler)
{
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = StaticThreadCount();
    cfg->m_cfg_stack_size = 1024 * 256;
    g_scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(g_scheduler->IsInitialized());
    g_prepared.store(true);
}

/**
 * 单 worker 进程验证（CTest `mongo.close_single_worker`，环境变量
 * BBT_MONGO_TEST_STATIC_THREADS=1）：Close 的等待必须挂起，而不是阻塞唯一的
 * Co worker。两 worker 进程里的 pulse 推进不能排除「Close 阻塞了另一个 worker」
 * 的解释，故本用例只在单 worker 独立进程中执行；普通 mongo.unit 运行跳过。
 */
BOOST_AUTO_TEST_CASE(t_owner_close_single_worker_wait_release)
{
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    if (cfg->m_cfg_static_thread_num != 1) {
        BOOST_TEST_MESSAGE("skip: single-worker process only (CTest mongo.close_single_worker)");
        return;
    }
    BOOST_REQUIRE(cfg->m_cfg_static_thread_num == 1);
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    /**
     * driver 在途 1500ms：Close 等待真实回收的窗口足够长。
     */
    auto db = NewOwner(MakeRtCfg(1, 4, 1500));
    auto rt = RuntimeOf(db);
    auto h = db->Collection({"bbt_ut", "single_worker_wait"});
    BOOST_REQUIRE(h);
    auto coll = h.value();

    mongo::FindOptions fo;
    fo.limit = 1;
    std::optional<result<std::vector<MongoDocument>>> out;
    std::atomic_bool out_ready{false};
    bool find_task = false;
    g_scheduler->RegistCoroutineTask(
        [&, coll] {
            out.emplace(bbt_co_await coll->Find(EmptyDoc(), fo, Opt(40000)));
            out_ready.store(true, std::memory_order_release);
        },
        find_task);
    BOOST_REQUIRE(find_task);

    /**
     * 阶段握手：driver 已在途后再进入关闭等待窗口。
     */
    BOOST_REQUIRE(WaitUntil([&] { return rt->DriverCallsInFlightForTest() == 1; }, 10000));

    /**
     * 关闭协程先发布「即将等待」，pulse 只在该窗口内计数：唯一的 Co worker
     * 若被 Close 阻塞，pulse 无法推进，ticks 到不了阈值。
     */
    std::atomic_bool in_wait{false};
    std::atomic_bool close_returned{false};
    std::atomic_int ticks{0};
    std::atomic_bool pulse_done{false};
    bool pulse_task = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            while (!in_wait.load(std::memory_order_acquire))
                bbtco_sleep(1);
            while (!close_returned.load(std::memory_order_acquire)) {
                ticks.fetch_add(1);
                bbtco_sleep(5);
            }
            pulse_done.store(true, std::memory_order_release);
        },
        pulse_task);
    BOOST_REQUIRE(pulse_task);

    std::optional<result<void>> closed;
    std::atomic_bool closed_ready{false};
    std::atomic<long long> close_ms{0};
    bool close_task = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            CloseOptions options;
            options.mode = CloseMode::Immediate;
            options.wait_release = true;
            options.deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{8000};
            const auto begin = std::chrono::steady_clock::now();
            in_wait.store(true, std::memory_order_release);
            closed.emplace(db->Close(options));
            close_ms.store(
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin)
                    .count());
            close_returned.store(true, std::memory_order_release);
            closed_ready.store(true, std::memory_order_release);
        },
        close_task);
    BOOST_REQUIRE(close_task);

    /**
     * 在 Close 返回之前：同一唯一 worker 上的 pulse 持续推进（挂起而非阻塞）。
     */
    BOOST_REQUIRE(WaitUntil([&] { return ticks.load() >= 5; }, 10000));
    BOOST_CHECK(!closed_ready.load(std::memory_order_acquire));
    BOOST_REQUIRE(WaitUntil([&] { return closed_ready.load(std::memory_order_acquire); }, 15000));
    BOOST_REQUIRE(*closed);

    /**
     * 真等 driver 返回，不是提前成功
     */
    BOOST_CHECK_GE(close_ms.load(), 1000);
    BOOST_CHECK(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
    BOOST_REQUIRE(WaitUntil([&] { return pulse_done.load(std::memory_order_acquire); }, 5000));
    BOOST_REQUIRE(WaitUntil([&] { return out_ready.load(std::memory_order_acquire); }, 10000));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
}

BOOST_AUTO_TEST_CASE(t_config_validation)
{
    BOOST_REQUIRE(g_prepared.load());
    MongoClientConfig base = MakeDeadCfg();

    auto bad_uri = base;
    bad_uri.uri = "http://127.0.0.1:1/";
    BOOST_CHECK(CoMongoCli::Create(bad_uri).error().code == ErrorCode::InvalidArgument);
    auto empty_db = base;
    empty_db.database.clear();
    BOOST_CHECK(CoMongoCli::Create(empty_db).error().code == ErrorCode::InvalidArgument);
    auto empty_coll = base;
    empty_coll.collection.clear();
    BOOST_CHECK(CoMongoCli::Create(empty_coll).error().code == ErrorCode::InvalidArgument);
    auto zero_workers = base;
    zero_workers.worker_threads = 0;
    BOOST_CHECK(CoMongoCli::Create(zero_workers).error().code == ErrorCode::InvalidArgument);
    auto over_workers = base;
    over_workers.worker_threads = kMongoLimitsMaxWorkerThreads + 1;
    BOOST_CHECK(CoMongoCli::Create(over_workers).error().code == ErrorCode::InvalidArgument);
    auto zero_queue = base;
    zero_queue.max_queue = 0;
    BOOST_CHECK(CoMongoCli::Create(zero_queue).error().code == ErrorCode::InvalidArgument);
    auto zero_sst = base;
    zero_sst.server_selection_timeout = std::chrono::milliseconds{0};
    BOOST_CHECK(CoMongoCli::Create(zero_sst).error().code == ErrorCode::InvalidArgument);
    auto over_timeout = base;
    over_timeout.socket_timeout = kMongoLimitsMaxTimeout + std::chrono::milliseconds{1};
    BOOST_CHECK(CoMongoCli::Create(over_timeout).error().code == ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_CASE(t_command_prechecks)
{

    /**
     * NewClient 的 start 传 false：只取对象身份，不启动 worker。
     */
    auto c = NewClient(MakeDeadCfg(), false);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    /**
     * 未 Start：协程内调用 → RuntimeUnavailable。
     */
    std::optional<result<std::optional<MongoDocument>>> out;
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->FindOne(EmptyDoc(), Opt())); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::RuntimeUnavailable);

    /**
     * 启动后再校验参数形态：空文档/空 filter/空 update InvalidArgument。
     */
    BOOST_REQUIRE(cli->Start());
    std::optional<result<void>> ins;
    BOOST_REQUIRE(RunInCoroutine([&] { ins.emplace(cli->InsertOne(MongoDocument{}, Opt())); }));
    BOOST_REQUIRE(!*ins);
    BOOST_CHECK(ins->error().code == ErrorCode::InvalidArgument);
    std::optional<result<std::optional<MongoDocument>>> f;
    BOOST_REQUIRE(RunInCoroutine([&] { f.emplace(cli->FindOne(MongoDocument{}, Opt())); }));
    BOOST_REQUIRE(!*f);
    BOOST_CHECK(f->error().code == ErrorCode::InvalidArgument);
    std::optional<result<MongoUpdateResult>> u;
    BOOST_REQUIRE(RunInCoroutine([&] { u.emplace(cli->UpdateOne(EmptyDoc(), MongoDocument{}, Opt())); }));
    BOOST_REQUIRE(!*u);
    BOOST_CHECK(u->error().code == ErrorCode::InvalidArgument);
    std::optional<result<std::uint64_t>> d;
    BOOST_REQUIRE(RunInCoroutine([&] { d.emplace(cli->DeleteOne(MongoDocument{}, Opt())); }));
    BOOST_REQUIRE(!*d);
    BOOST_CHECK(d->error().code == ErrorCode::InvalidArgument);

    CloseClientDrained(cli);

    /**
     * 已关闭：新命令 → Closed。
     */
    BOOST_REQUIRE(RunInCoroutine([&] { f.emplace(cli->FindOne(EmptyDoc(), Opt())); }));
    BOOST_REQUIRE(!*f);
    BOOST_CHECK(f->error().code == ErrorCode::Closed);
}

BOOST_AUTO_TEST_CASE(t_invalid_context_plain)
{

    /**
     * 普通线程（非协程）调命令：不启动协程直接调用。
     */
    auto c = NewClient(MakeDeadCfg());
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    auto res = cli->FindOne(EmptyDoc(), Opt());
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::InvalidContext);
    (void)cli->Close();
    BOOST_CHECK(cli->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_invalid_context_precedence_legacy)
{

    /**
     * 错误优先级：协程外调用一律 InvalidContext，不区分未 Start / 已关——前置
     * 校验首行即查协程上下文。未 Start 时资源为空，也不能因此短路成
     * RuntimeUnavailable。
     */
    auto c1 = CoMongoCli::Create(MakeDeadCfg());
    BOOST_REQUIRE(c1);
    auto not_started = std::move(c1).value();
    auto r1 = not_started->FindOne(EmptyDoc(), Opt());
    BOOST_REQUIRE(!r1);
    BOOST_CHECK(r1.error().code == ErrorCode::InvalidContext);
    (void)not_started->Close();

    /**
     * 已关（Start 后 Close）：协程外仍 InvalidContext 而非 Closed
     * ——与前置校验「协程外优先」一致。
     */
    auto c2 = NewClient(MakeDeadCfg());
    BOOST_REQUIRE(c2);
    auto closed = std::move(c2).value();
    (void)closed->Close();
    BOOST_REQUIRE(closed->IsClosed());
    auto r2 = closed->FindOne(EmptyDoc(), Opt());
    BOOST_REQUIRE(!r2);
    BOOST_CHECK(r2.error().code == ErrorCode::InvalidContext);
}

BOOST_AUTO_TEST_CASE(t_invalid_bson)
{
    auto c = NewClient(MakeDeadCfg());
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    /**
     * 不良构 BSON：校验在 driver 调用之前 → InvalidArgument（即便
     * 服务端不可达也不会走到 server selection）。
     */
    std::optional<result<void>> ins;
    BOOST_REQUIRE(RunInCoroutine([&] { ins.emplace(cli->InsertOne(BadDoc(), Opt(5000))); }));
    BOOST_REQUIRE(!*ins);
    BOOST_CHECK(ins->error().code == ErrorCode::InvalidArgument);

    CloseClientDrained(cli);
}

BOOST_AUTO_TEST_CASE(t_server_unreachable)
{

    /**
     * server selection 超时（2s）后 driver 失败 → Unavailable。
     */
    auto c = NewClient(MakeDeadCfg(1, 4, 2000));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::optional<result<std::optional<MongoDocument>>> out;
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(cli->FindOne(EmptyDoc(), Opt(20000))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Unavailable);
    BOOST_CHECK_EQUAL(out->error().backend_category, "mongocxx");

    CloseClientDrained(cli);
}

BOOST_AUTO_TEST_CASE(t_capacity_overloaded)
{

    /**
     * worker_threads=1、max_queue=2：先提交的 op 在不可达地址的 server
     * selection 中占住唯一 worker，随后 2 个 op 占满队列，第 4 个确定性
     * Overloaded。同步点用真实队列状态（不 sleep）：提交前两个 op 后，
     * 接纳队列深度稳定为 1 ⇔ 恰有 1 个已被 worker 取出进入 driver、1 个
     * 仍在排队——两构建面均可用。
     */
    auto c = NewClient(MakeDeadCfg(1, 2, 8000));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    auto impl = ImplOf(cli);

    std::atomic_int overloaded{0};
    std::atomic_int done{0};
    auto submit = [&](int budget_ms) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [&, budget_ms] {
                auto r = cli->FindOne(EmptyDoc(), Opt(budget_ms));
                if (!r && r.error().code == ErrorCode::Overloaded)
                    overloaded.fetch_add(1);
                done.fetch_add(1);
            },
            succ);
        BOOST_REQUIRE(succ);
    };

    submit(30000);
    submit(30000);

    /**
     * 真实状态同步：1 个被 worker 占用（phase==kRunning）、1 个排队，
     * 队列深度稳定为 1。
     */
    BOOST_REQUIRE(
        WaitUntil([&] { return impl->DriverCallsInFlightForTest() == 1 && impl->QueuedOpsForTest() == 1; }, 10000));
    submit(30000);
    submit(30000);

    /**
     * 恰好 1 个 Overloaded：1 在 driver 中 + 2 排队 = 容量占满，第 4 被拒。
     */
    BOOST_REQUIRE(WaitUntil([&] { return overloaded.load() == 1; }, 10000));
    BOOST_CHECK_EQUAL(overloaded.load(), 1);
#ifdef BBT_INFRA_STRINGENT_DEBUG

    /**
     * 运行中 driver 调用峰值不超 worker_threads（debug 观测）。
     */
    BOOST_CHECK(impl->PeakDriverCallsForTest() <= 1);
#endif

    (void)cli->Close();
    BOOST_REQUIRE(WaitUntil([&] { return done.load() == 4; }, 30000));
    BOOST_CHECK_EQUAL(overloaded.load(), 1);

    BOOST_REQUIRE(cli->IsClosed());
    BOOST_CHECK(cli->IsClosed());

    /**
     * 真实生产状态：worker 全退（Release 面同样成立）。
     */
    BOOST_CHECK_EQUAL(impl->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(impl->RunningDriverCallsForTest(), 0);
#endif
}

BOOST_AUTO_TEST_CASE(t_deadline_only)
{
    auto c = NewClient(MakeDeadCfg(1, 4, 10000));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    auto impl = ImplOf(cli);

    /**
     * deadline 500ms：out_ready 结果闩自同步——FindOne 返回即证明逻辑终态
     * 已交付（且该逻辑返回发生在其物理 driver 调用仍在进行时）。
     */
    std::optional<result<std::optional<MongoDocument>>> out;
    std::atomic_bool out_ready{false};
    bool succ0 = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            out.emplace(cli->FindOne(EmptyDoc(), Opt(500)));
            out_ready.store(true, std::memory_order_release);
        },
        succ0);
    BOOST_REQUIRE(succ0);
#ifdef BBT_INFRA_STRINGENT_DEBUG

    /**
     * Debug 面：先确认 op 已进入 server selection（running==1 确定性观测）。
     */
    BOOST_REQUIRE(WaitUntil([&] { return impl->RunningDriverCallsForTest() == 1; }, 10000));
#endif
    BOOST_REQUIRE(WaitUntil([&] { return out_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TimedOut);
#ifdef BBT_INFRA_STRINGENT_DEBUG

    /**
     * 逻辑返回后物理 driver 调用仍在进行——逻辑返回不冒充物理收口。
     */
    BOOST_CHECK_EQUAL(impl->RunningDriverCallsForTest(), 1);
#endif

    /**
     * Close：首个逻辑终态（TimedOut）不被晚到的关闭覆盖；物理调用继续到
     * driver 超时，收口后 IsClosed 成立、worker 全退。
     */
    (void)cli->Close();
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TimedOut);
    BOOST_REQUIRE(cli->IsClosed());
    BOOST_CHECK_EQUAL(impl->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(impl->RunningDriverCallsForTest(), 0);
#endif
}

BOOST_AUTO_TEST_CASE(t_close_during_inflight)
{

    /**
     * server selection 上界 15s：Close 发生在 driver 调用进行中——Close
     * 不得提前宣告 Closed；物理收口（driver 返回 + worker 全退）才 IsClosed。
     */
    auto c = NewClient(MakeDeadCfg(1, 4, 15000));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    auto impl = ImplOf(cli);

    std::optional<result<std::optional<MongoDocument>>> out;
    std::atomic_bool out_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            out.emplace(cli->FindOne(EmptyDoc(), Opt(40000)));
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);

    /**
     * 前置同步（两构建面一致）：等该 op 已被 worker 从队列取出、进入执行槽
     * （读既有 m_ops + phase==kRunning 真实状态，非新增观测计数），确保 Close
     * 确实发生在在途处理期间，而非 op 仍在队列时（不靠 sleep 猜时序）。
     */
    BOOST_REQUIRE(WaitUntil([&] { return impl->DriverCallsInFlightForTest() == 1; }, 10000));
#ifdef BBT_INFRA_STRINGENT_DEBUG

    /**
     * Debug 面追加：native driver 调用已在途（既有 RunningGuard 观测）。
     */
    BOOST_REQUIRE(WaitUntil([&] { return impl->RunningDriverCallsForTest() == 1; }, 10000));
#endif

    (void)cli->Close();
    BOOST_REQUIRE(WaitUntil([&] { return out_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);

    /**
     * 同步契约：Close 返回即物理收口（在途 driver 调用返回 + worker 全退），
     * 因此返回时 IsClosed() 必须为真；不断言「Close 提前返回、IsClosed 仍为
     * false」这类有界窗口行为。
     */
    BOOST_REQUIRE(cli->IsClosed());
    BOOST_CHECK(cli->IsClosed());
    BOOST_CHECK_EQUAL(impl->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(impl->RunningDriverCallsForTest(), 0);
#endif
}

BOOST_AUTO_TEST_CASE(t_worker_threads_bound)
{

    /**
     * 2 workers + 4 并发 op：运行中 driver 调用峰值 ≤ worker_threads。
     */
    auto c = NewClient(MakeDeadCfg(2, 8, 8000));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    auto impl = ImplOf(cli);

    bbt::core::thread::CountDownLatch all{4};
    for (int i = 0; i < 4; ++i) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [&] {
                // clang-format off

                /**
                 * 不可达地址：每个 op 在 server selection 中停留约 8s，
                 * 足以让 2 个 worker 同时各持一个在途调用。
                 */
                // clang-format on
                auto r = cli->FindOne(EmptyDoc(), Opt(20000));
                (void)r;
                all.Down();
            },
            succ);
        BOOST_REQUIRE(succ);
    }

    /**
     * 真实队列状态同步（Debug/Release 均可用）：2 worker + 4 在途 op ⇒
     * 2 个已进入 driver、2 个排队，接纳队列深度稳定为 2。
     */
    BOOST_REQUIRE(WaitUntil([&] { return impl->QueuedOpsForTest() == 2; }, 10000));
    BOOST_CHECK(all.WaitTimeout(30000) == 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG

    /**
     * debug 观测：运行中 driver 调用峰值 ≤ worker_threads。
     */
    BOOST_CHECK(impl->RunningDriverCallsForTest() <= 2);
    BOOST_CHECK(impl->PeakDriverCallsForTest() <= 2);
#endif

    CloseClientDrained(cli);
}

BOOST_AUTO_TEST_CASE(t_client_start_close_publish_pairing)
{

    /**
     * Start 发布与 Close 封口必须配对：并发时不得出现「Close 已返回、Start 事后
     * 发布 worker/lease、IsClosed 回退」；Close 返回当刻本 wrapper 已无资源，
     * 关闭后 Start 一律 Closed、重复 Close 幂等。
     */
    for (int i = 0; i < 200; ++i) {
        auto c = CoMongoCli::Create(MakeDeadCfg(1, 2, 3000));
        BOOST_REQUIRE(c);
        auto cli = std::move(c).value();
        auto impl = ImplOf(cli);

        /**
         * -1 = Start 成功
         */
        std::atomic_int start_code{-2};
        std::atomic_bool start_returned{false};
        std::atomic_bool close_returned{false};
        std::thread starter([&] {
            auto r = cli->Start();
            start_code.store(r ? -1 : static_cast<int>(r.error().code), std::memory_order_relaxed);
            start_returned.store(true, std::memory_order_release);
        });
        std::thread closer([&] {
            (void)cli->Close();
            close_returned.store(true, std::memory_order_release);
        });
        starter.join();
        closer.join();
        BOOST_REQUIRE(start_returned.load() && close_returned.load());

        /**
         * Close 返回后：终态不回退、本 wrapper 无 worker 残留。
         */
        BOOST_CHECK(cli->IsClosed());
        BOOST_CHECK_EQUAL(impl->LiveWorkersForTest(), 0);

        /**
         * 关闭后 Start 一律 Closed，不重开、不产生新资源。
         */
        auto again = cli->Start();
        BOOST_REQUIRE(!again);
        BOOST_CHECK(again.error().code == ErrorCode::Closed);
        BOOST_CHECK(cli->IsClosed());
        BOOST_CHECK_EQUAL(impl->LiveWorkersForTest(), 0);

        /**
         * 并发 Start 只允许两种合法结局：成功（随即被 Close 同步收口）或
         * 被封口明确拒绝为 Closed；不得有第三种「事后发布」。
         */
        const int code = start_code.load(std::memory_order_relaxed);
        BOOST_CHECK(code == -1 || code == static_cast<int>(ErrorCode::Closed));

        /**
         * 重复 Close 幂等。
         */
        (void)cli->Close();
        BOOST_CHECK(cli->IsClosed());
        BOOST_CHECK_EQUAL(impl->LiveWorkersForTest(), 0);
    }
}

BOOST_AUTO_TEST_CASE(t_worker_spawn_failure_finalizes)
{

    /**
     * worker 组启动失败（reserve 抛 bad_alloc 或第 N 个
     * worker 创建抛 system_error）必须与 Close/析构共用同一 finalizer 完整
     * 收口——已起 worker 全部退出并 join、m_io_dead 置位、本 owner 的 pool
     * lease 归还、终态 Closed 发布；不得留下「线程已起但未收口」或半关闭态。
     * 经接缝确定性注入，失败发生在任何 driver 调用之前，不需要真实 MongoDB。
     */
    BOOST_REQUIRE(g_prepared.load());

    /**
     * 预热并保活一个同 URI 的 pool lease：进程级 pool 与驱动后台线程就位
     * 后，后续线程计数变化只反映本用例的 worker 组，不把 pool 后台线程
     * 误计为 worker 泄漏。
     */
    auto warm_info = mongo_detail::NewObjectInfo("test.rt.warm");
    BOOST_REQUIRE(warm_info);
    auto warm = mongo_detail::MongoRuntime::Create(MakeUriCfg("mongodb://127.0.0.1:1/bbt_ut"), warm_info.value());
    BOOST_REQUIRE(warm->Start());
    const std::size_t base_threads = ThreadCount();
    BOOST_REQUIRE(base_threads > 1);

    const std::size_t fail_points[] = {mongo_detail::MongoRuntime::kSpawnFailReserve, 0, 1, 3};
    for (const auto at : fail_points) {
        auto info = mongo_detail::NewObjectInfo("test.rt.spawn_fail");
        BOOST_REQUIRE(info);
        auto cfg = MakeUriCfg("mongodb://127.0.0.1:1/bbt_ut");
        cfg.worker_threads = 4;
        auto rt = mongo_detail::MongoRuntime::Create(cfg, info.value());
        rt->SetWorkerSpawnFailForTest(at);

        auto st = rt->Start();
        BOOST_REQUIRE_MESSAGE(!st, "spawn fail point " << at << " must fail");
        BOOST_CHECK(st.error().code == ErrorCode::InternalError);

        /**
         * 未成功发布资源：不进 Running。
         */
        BOOST_CHECK(!rt->IsRunning());

        /**
         * 已起 worker 全部退出（物理线程回基线），无 worker 线程泄漏。
         */
        BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
        BOOST_CHECK(WaitUntil([&] { return ThreadCount() <= base_threads; }, 5000));

        /**
         * 同一 finalizer 已收口：io 域置死、终态 Closed。
         */
        BOOST_CHECK(rt->IoDeadForTest());
        BOOST_CHECK(rt->IsClosed());

        /**
         * 收口幂等：重复 Close 不重复 join/reset，终态不回退。
         */
        (void)rt->Close();
        BOOST_CHECK(rt->IsClosed());
        BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);

        /**
         * 失败即终态，不可复活：再次 Start 明确拒绝。
         */
        auto again = rt->Start();
        BOOST_REQUIRE(!again);
        BOOST_CHECK(again.error().code == ErrorCode::Closed);
    }

    /**
     * 释放预热 lease：同一 finalizer 收口，线程回基线。
     */
    (void)warm->Close();
    BOOST_CHECK(warm->IsClosed());
    BOOST_CHECK(WaitUntil([&] { return ThreadCount() <= base_threads; }, 5000));
}

/**
 * 前置 finalizer 创建失败（等价线程资源耗尽）：Start 必须在创建任何 worker 之前
 * 失败返回——无 worker 可 join、lease 随局部 pool 归还、不假装 Closed；之后的
 * 关闭/最后引用在 Co worker 上也必须有界完成、真实收口、不泄漏。
 */
BOOST_AUTO_TEST_CASE(t_finalizer_spawn_failure_bounded)
{
    BOOST_REQUIRE(g_prepared.load());

    /**
     * 专用 URI：lease 探针只观察本 owner 的引用。
     */
    auto cfg = MakeRtCfg(1, 4, 2000);
    cfg.uri = "mongodb://127.0.0.1:1/finalizer_fail";
    const std::string eff = mongo_detail::EffectiveUri(cfg);
    auto pr = mongo_detail::AcquireMongoPool(eff);
    BOOST_REQUIRE(pr);
    auto probe = std::move(pr).value();
    BOOST_CHECK_EQUAL(probe.use_count(), 1);

    auto info = mongo_detail::NewObjectInfo("test.rt.finalizer_fail");
    BOOST_REQUIRE(info);
    auto rt = mongo_detail::MongoRuntime::Create(cfg, info.value());
    rt->SetFinalizerSpawnFailForTest(true);
    auto st = rt->Start();
    BOOST_REQUIRE(!st);
    BOOST_CHECK(st.error().code == ErrorCode::InternalError);
    BOOST_CHECK(!rt->IsRunning());

    /**
     * 失败发生在发布 pool / 创建 worker 之前：不假装收口、无 worker、lease 已归还。
     */
    BOOST_CHECK(!rt->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
    BOOST_CHECK_EQUAL(probe.use_count(), 1);

    /**
     * Co worker 上关闭：无 finalizer 上下文时封口线程同步完成幂等收口（无 worker
     * 可 join），wait_release 仍按同一 deadline 有界返回成功。
     */
    std::optional<result<void>> closed;
    std::atomic_bool closed_ready{false};
    bool close_task = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            CloseOptions options;
            options.mode = CloseMode::Immediate;
            options.wait_release = true;
            options.deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{300};
            closed.emplace(rt->Close(options));
            closed_ready.store(true, std::memory_order_release);
        },
        close_task);
    BOOST_REQUIRE(close_task);
    BOOST_REQUIRE(WaitUntil([&] { return closed_ready.load(std::memory_order_acquire); }, 5000));
    BOOST_REQUIRE(*closed);
    BOOST_CHECK(rt->IsClosed());
    BOOST_CHECK(rt->IoDeadForTest());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
    BOOST_CHECK_EQUAL(probe.use_count(), 1);

    /**
     * 物理凭据仍可兑现；重复关闭幂等。
     */
    auto stopped = rt->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate,
            true,
            {},
            std::chrono::steady_clock::now() + std::chrono::milliseconds{1000}});
    BOOST_REQUIRE(stopped);
    (void)rt->Close();
    BOOST_CHECK(rt->IsClosed());

    /**
     * 删除器无 finalizer 上下文且已收口：直接释放，不产生线程/join。
     */
    rt.reset();
    probe.reset();
}

BOOST_AUTO_TEST_SUITE_END()

/**
 * 多文档数据面
 * 这些入口只在 mongo::CoMongoColl（owner 句柄）上，CoMongoCli 没有；用例一律
 * 经 CoMongoDb + Collection() 取得句柄。不依赖真实 Mongo：参数边界
 * 在句柄层先于一切上下文/生命周期校验（零接纳），协程外/队列满/Close 竞争
 * 沿用本文件既有分发约定与同步手法。
 */
BOOST_AUTO_TEST_SUITE(mongo_data_plane)

namespace {

/**
 * 取一只可接纳的集合句柄（owner 已 Start）；调用方负责收尾。
 */
struct OwnerHandle {
    std::shared_ptr<mongo::CoMongoDb> db;
    std::shared_ptr<mongo::CoMongoColl> coll;
    std::shared_ptr<mongo_detail::MongoRuntime> rt;
};

OwnerHandle NewHandle(const mongo::MongoRuntimeConfig& cfg, const char* coll_name)
{
    OwnerHandle out;
    auto d = mongo::CoMongoDb::Create(cfg);
    BOOST_REQUIRE(d);
    out.db = std::move(d).value();
    BOOST_REQUIRE(out.db->Start());
    auto h = out.db->Collection({"bbt_ut", coll_name});
    BOOST_REQUIRE(h);
    out.coll = std::move(h).value();
    out.rt = std::static_pointer_cast<mongo_detail::MongoCollImpl>(out.coll)->Runtime();
    return out;
}

void CloseOwner(OwnerHandle& oh)
{
    (void)oh.coll->Close();
    BOOST_REQUIRE(oh.db->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + std::chrono::seconds{8}}));
    BOOST_REQUIRE(oh.db->IsClosed());
    BOOST_CHECK_EQUAL(oh.rt->LiveWorkersForTest(), 0);
}

// clang-format off
}
// clang-format on

BOOST_AUTO_TEST_CASE(t_dp_param_validation)
{
    BOOST_REQUIRE(g_prepared.load());

    /**
     * 参数边界在 CollSubmit 之前（零接纳）：所有非法输入立即返回
     * InvalidArgument，不接触接纳队列、worker 或 driver——用真实队列深度
     * 与执行槽计数证明「什么都没有被发出」。
     */
    auto oh = NewHandle(MakeRtCfg(1, 4, 8000), "dp_param");
    auto* coll = oh.coll.get();

    /**
     * InsertMany：空列表。
     */
    std::optional<result<mongo::MongoInsertManyResult>> im_empty;
    BOOST_REQUIRE(RunInCoroutine([&] { im_empty.emplace(bbt_co_await coll->InsertMany({}, Opt())); }));
    BOOST_REQUIRE(!*im_empty);
    BOOST_CHECK(im_empty->error().code == ErrorCode::InvalidArgument);

    /**
     * InsertMany：文档数 > kMongoLimitsMaxInsertDocs（500）。
     */
    std::vector<MongoDocument> too_many(mongo::kMongoLimitsMaxInsertDocs + 1, EmptyDoc());
    std::optional<result<mongo::MongoInsertManyResult>> im_over;
    BOOST_REQUIRE(RunInCoroutine([&] { im_over.emplace(bbt_co_await coll->InsertMany(too_many, Opt())); }));
    BOOST_REQUIRE(!*im_over);
    BOOST_CHECK(im_over->error().code == ErrorCode::InvalidArgument);

    /**
     * InsertMany：列表内含空文档（列表非空、未超数 → 单文档空值分支）。
     */
    std::vector<MongoDocument> with_empty{EmptyDoc(), MongoDocument{}};
    std::optional<result<mongo::MongoInsertManyResult>> im_bad;
    BOOST_REQUIRE(RunInCoroutine([&] { im_bad.emplace(bbt_co_await coll->InsertMany(with_empty, Opt())); }));
    BOOST_REQUIRE(!*im_bad);
    BOOST_CHECK(im_bad->error().code == ErrorCode::InvalidArgument);

    /**
     * InsertMany 总字节上限（kMongoLimitsMaxInsertTotalBytes=8 MiB）：
     * 合法 BSON 累计刚好越界 → InvalidArgument 且零接纳；文档数 ≤500 时
     * 必须走字节分支才拦得住（文档数分支无法区分这两条路径）。
     * 构造：{"pad":"x"*N} 良构 BSON——unit 侧无 bsoncxx，手工填字节面
     * （int32 总长 + 0x02 元素 + cstring "pad\0" + int32 串长 + 载荷
     * + 串 NUL + 文档终止符）。libbson 会做全结构校验，长度/边界必须精确。
     */
    const auto pad_doc = [](std::size_t pad) {
        // clang-format off

        /**
         * 布局：hdr(4) + type(1) + name(4) + slen(4) + str(pad+1) + term(1)
         */
        // clang-format on
        const std::size_t len = 4 + 1 + 4 + 4 + pad + 1 + 1;
        MongoDocument d;
        d.bytes.resize(len);
        d.bytes[0] = static_cast<std::uint8_t>(len & 0xff);
        d.bytes[1] = static_cast<std::uint8_t>((len >> 8) & 0xff);
        d.bytes[2] = static_cast<std::uint8_t>((len >> 16) & 0xff);
        d.bytes[3] = static_cast<std::uint8_t>((len >> 24) & 0xff);

        /**
         * string
         */
        d.bytes[4] = 0x02;
        d.bytes[5] = 'p';
        d.bytes[6] = 'a';
        d.bytes[7] = 'd';
        d.bytes[8] = 0x00;

        /**
         * 含串 NUL
         */
        const std::uint32_t slen = static_cast<std::uint32_t>(pad + 1);
        d.bytes[9] = static_cast<std::uint8_t>(slen & 0xff);
        d.bytes[10] = static_cast<std::uint8_t>((slen >> 8) & 0xff);
        d.bytes[11] = static_cast<std::uint8_t>((slen >> 16) & 0xff);
        d.bytes[12] = static_cast<std::uint8_t>((slen >> 24) & 0xff);
        std::memset(d.bytes.data() + 13, 'x', pad);

        /**
         * 串 NUL
         */
        d.bytes[13 + pad] = 0x00;

        /**
         * 文档终止符
         */
        d.bytes[len - 1] = 0x00;
        return d;
    };

    /**
     * 每份 ~1 MiB（单份远低于上限 → 文档数与空文档分支都拦不住，只能
     * 被字节累计分支区分）。n=8 累计 8 MiB + 8×15B > 上限；n=7 < 上限。
     */
    const std::size_t kPadPer = 1024 * 1024;
    const std::size_t kDocLen = 4 + 1 + 4 + 4 + kPadPer + 1 + 1;
    {
        std::vector<MongoDocument> over_bytes;
        const std::size_t n = mongo::kMongoLimitsMaxInsertTotalBytes / kDocLen + 1;
        for (std::size_t i = 0; i < n; ++i)
            over_bytes.push_back(pad_doc(kPadPer));
        std::optional<result<mongo::MongoInsertManyResult>> im_bytes;
        BOOST_REQUIRE(RunInCoroutine([&] { im_bytes.emplace(bbt_co_await coll->InsertMany(over_bytes, Opt())); }));
        BOOST_REQUIRE(!*im_bytes);
        BOOST_CHECK(im_bytes->error().code == ErrorCode::InvalidArgument);
    }

    /**
     * 对照：累计在上限内（n=7，~7 MiB）不被字节分支拒绝——校验穿过后被
     * 接纳进管线，在不可达地址确定性以 Unavailable 终态（不是
     * InvalidArgument：后者证明误伤合法大小）。
     */
    {
        std::vector<MongoDocument> under_bytes;
        const std::size_t n = mongo::kMongoLimitsMaxInsertTotalBytes / kDocLen;
        for (std::size_t i = 0; i < n; ++i)
            under_bytes.push_back(pad_doc(kPadPer));
        std::optional<result<mongo::MongoInsertManyResult>> im_ok;
        BOOST_REQUIRE(RunInCoroutine([&] { im_ok.emplace(bbt_co_await coll->InsertMany(under_bytes, Opt(30000))); }));
        BOOST_REQUIRE(!*im_ok);
        BOOST_CHECK(im_ok->error().code == ErrorCode::Unavailable);
    }

    /**
     * Find：limit 必填——0 与 >kMongoLimitsMaxFindLimit(1000) 均拒绝。
     *
     * limit 默认 0
     */
    mongo::FindOptions fo0;
    std::optional<result<std::vector<MongoDocument>>> f0;
    BOOST_REQUIRE(RunInCoroutine([&] { f0.emplace(bbt_co_await coll->Find(EmptyDoc(), fo0, Opt())); }));
    BOOST_REQUIRE(!*f0);
    BOOST_CHECK(f0->error().code == ErrorCode::InvalidArgument);

    mongo::FindOptions fo_over;
    fo_over.limit = mongo::kMongoLimitsMaxFindLimit + 1;
    std::optional<result<std::vector<MongoDocument>>> f_over;
    BOOST_REQUIRE(RunInCoroutine([&] { f_over.emplace(bbt_co_await coll->Find(EmptyDoc(), fo_over, Opt())); }));
    BOOST_REQUIRE(!*f_over);
    BOOST_CHECK(f_over->error().code == ErrorCode::InvalidArgument);

    /**
     * UpdateMany：空 filter 一律拒绝（bytes 空 与 良构 `{}` 两种形态）。
     */
    for (const MongoDocument& empty_f : {MongoDocument{}, EmptyDoc()}) {
        std::optional<result<MongoUpdateResult>> u;
        BOOST_REQUIRE(RunInCoroutine([&] { u.emplace(bbt_co_await coll->UpdateMany(empty_f, UpdateDoc(), Opt())); }));
        BOOST_REQUIRE(!*u);
        BOOST_CHECK(u->error().code == ErrorCode::InvalidArgument);
    }

    /**
     * DeleteMany：空 filter 同上（两种空形态各一次）。
     */
    for (const MongoDocument& empty_f : {MongoDocument{}, EmptyDoc()}) {
        std::optional<result<std::uint64_t>> d;
        BOOST_REQUIRE(RunInCoroutine([&] { d.emplace(bbt_co_await coll->DeleteMany(empty_f, Opt())); }));
        BOOST_REQUIRE(!*d);
        BOOST_CHECK(d->error().code == ErrorCode::InvalidArgument);
    }

    /**
     * BSON 输入预算与 skip 边界
     * 越界输入一律在 CollSubmit 之前 InvalidArgument，零接纳。
     * 超 kMongoLimitsMaxInputDocBytes（8 MiB）的单份 BSON：Find 的
     * filter/sort/projection、UpdateMany 的 filter/update、DeleteMany 与
     * CountDocuments 的 filter 全部覆盖。
     */
    const auto oversized = pad_doc(mongo::kMongoLimitsMaxInputDocBytes);
    {
        mongo::FindOptions fo_big;
        fo_big.limit = 1;
        std::optional<result<std::vector<MongoDocument>>> f_big;
        BOOST_REQUIRE(RunInCoroutine([&] { f_big.emplace(bbt_co_await coll->Find(oversized, fo_big, Opt())); }));
        BOOST_REQUIRE(!*f_big);
        BOOST_CHECK(f_big->error().code == ErrorCode::InvalidArgument);

        mongo::FindOptions fo_sort;
        fo_sort.limit = 1;
        fo_sort.sort = oversized;
        BOOST_REQUIRE(RunInCoroutine([&] { f_big.emplace(bbt_co_await coll->Find(EmptyDoc(), fo_sort, Opt())); }));
        BOOST_REQUIRE(!*f_big);
        BOOST_CHECK(f_big->error().code == ErrorCode::InvalidArgument);

        mongo::FindOptions fo_proj;
        fo_proj.limit = 1;
        fo_proj.projection = oversized;
        BOOST_REQUIRE(RunInCoroutine([&] { f_big.emplace(bbt_co_await coll->Find(EmptyDoc(), fo_proj, Opt())); }));
        BOOST_REQUIRE(!*f_big);
        BOOST_CHECK(f_big->error().code == ErrorCode::InvalidArgument);

        std::optional<result<MongoUpdateResult>> u_big;
        BOOST_REQUIRE(
            RunInCoroutine([&] { u_big.emplace(bbt_co_await coll->UpdateMany(oversized, UpdateDoc(), Opt())); }));
        BOOST_REQUIRE(!*u_big);
        BOOST_CHECK(u_big->error().code == ErrorCode::InvalidArgument);

        BOOST_REQUIRE(
            RunInCoroutine([&] { u_big.emplace(bbt_co_await coll->UpdateMany(NonEmptyDoc(), oversized, Opt())); }));
        BOOST_REQUIRE(!*u_big);
        BOOST_CHECK(u_big->error().code == ErrorCode::InvalidArgument);

        std::optional<result<std::uint64_t>> d_big;
        BOOST_REQUIRE(RunInCoroutine([&] { d_big.emplace(bbt_co_await coll->DeleteMany(oversized, Opt())); }));
        BOOST_REQUIRE(!*d_big);
        BOOST_CHECK(d_big->error().code == ErrorCode::InvalidArgument);

        BOOST_REQUIRE(RunInCoroutine([&] { d_big.emplace(bbt_co_await coll->CountDocuments(oversized, Opt())); }));
        BOOST_REQUIRE(!*d_big);
        BOOST_CHECK(d_big->error().code == ErrorCode::InvalidArgument);
    }

    /**
     * skip > INT64_MAX（driver 的 int64 表示范围外）→ InvalidArgument，
     * 不做实现相关的有符号收缩转换。
     */
    {
        mongo::FindOptions fo_skip;
        fo_skip.limit = 1;
        fo_skip.skip = static_cast<std::size_t>(INT64_MAX) + 1;
        std::optional<result<std::vector<MongoDocument>>> f_skip;
        BOOST_REQUIRE(RunInCoroutine([&] { f_skip.emplace(bbt_co_await coll->Find(EmptyDoc(), fo_skip, Opt())); }));
        BOOST_REQUIRE(!*f_skip);
        BOOST_CHECK(f_skip->error().code == ErrorCode::InvalidArgument);
    }

    /**
     * 对照：skip == INT64_MAX 本身合法（进入管线，在不可达地址
     * 以 Unavailable 终态——证明校验没有误伤边界值）。
     */
    {
        mongo::FindOptions fo_edge;
        fo_edge.limit = 1;
        fo_edge.skip = static_cast<std::size_t>(INT64_MAX);
        std::optional<result<std::vector<MongoDocument>>> f_edge;
        BOOST_REQUIRE(
            RunInCoroutine([&] { f_edge.emplace(bbt_co_await coll->Find(EmptyDoc(), fo_edge, Opt(30000))); }));
        BOOST_REQUIRE(!*f_edge);
        BOOST_CHECK(f_edge->error().code == ErrorCode::Unavailable);
    }

    /**
     * 零接纳的硬证据：全程没有 op 被登记/入队/占用执行槽。
     */
    BOOST_CHECK_EQUAL(oh.rt->QueuedOpsForTest(), 0);
    BOOST_CHECK_EQUAL(oh.rt->DriverCallsInFlightForTest(), 0);

    CloseOwner(oh);
}

BOOST_AUTO_TEST_CASE(t_dp_invalid_context_plain)
{
    BOOST_REQUIRE(g_prepared.load());

    /**
     * 协程外（普通线程）调 5 个新入口：bbt_co_await 的域前置检查先于函数体，
     * 非协程直调抛 std::logic_error（上游 async/await 的实际语义，见
     * docs/decisions/0007 对非协程入口口径的裁决）：不退化成长返回
     * InvalidContext；CoMongoCli 的裸返回才是 InvalidContext。
     */
    auto oh = NewHandle(MakeRtCfg(1, 4, 8000), "dp_ctx");
    auto* coll = oh.coll.get();

    mongo::FindOptions fo;
    fo.limit = 1;
    const std::vector<MongoDocument> one{EmptyDoc()};
    auto expect_logic_error = [](auto&& call) {
        bool thrown = false;
        try {
            call();
        } catch (const std::logic_error&) {
            thrown = true;
        }
        BOOST_CHECK(thrown);
    };
    expect_logic_error([&] { bbt_co_await coll->InsertMany(one, Opt()); });
    expect_logic_error([&] { bbt_co_await coll->Find(EmptyDoc(), fo, Opt()); });
    expect_logic_error([&] { bbt_co_await coll->UpdateMany(NonEmptyDoc(), UpdateDoc(), Opt()); });
    expect_logic_error([&] { bbt_co_await coll->DeleteMany(NonEmptyDoc(), Opt()); });
    expect_logic_error([&] { bbt_co_await coll->CountDocuments(EmptyDoc(), Opt()); });

    /**
     * 对照：CoMongoCli 的裸返回在协程外仍是 InvalidContext（不受上面的 async
     * 标记口径影响；同一 owner 的 CoMongoColl::FindOne 也是 async 标记，不能
     * 拿它当裸返回对照）。
     */
    auto c2 = NewClient(MakeDeadCfg());
    BOOST_REQUIRE(c2);
    auto cli2 = std::move(c2).value();
    auto r = cli2->FindOne(EmptyDoc(), Opt());
    BOOST_REQUIRE(!r);
    BOOST_CHECK(r.error().code == ErrorCode::InvalidContext);
    (void)cli2->Close();

    CloseOwner(oh);
}

BOOST_AUTO_TEST_CASE(t_dp_queue_overloaded)
{
    BOOST_REQUIRE(g_prepared.load());

    /**
     * 队列满 Overloaded 对新入口同样成立：worker=1 queue=2，不可达地址的
     * server selection 让首个 op 占住唯一 worker、随后 2 个占满队列，
     * 第 4 个确定性 Overloaded（沿用 t_capacity_overloaded 的确定性同步）。
     */
    auto oh = NewHandle(MakeRtCfg(1, 2, 8000), "dp_ovl");
    auto* coll = oh.coll.get();

    mongo::FindOptions fo;
    fo.limit = 1;
    std::atomic_int overloaded{0};
    std::atomic_int done{0};
    auto submit_find = [&](int budget_ms) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [&, budget_ms] {
                auto r = bbt_co_await coll->Find(EmptyDoc(), fo, Opt(budget_ms));
                if (!r && r.error().code == ErrorCode::Overloaded)
                    overloaded.fetch_add(1);
                done.fetch_add(1);
            },
            succ);
        BOOST_REQUIRE(succ);
    };

    /**
     * 占住 worker（server selection ~8s）
     */
    submit_find(30000);

    /**
     * 占满队列（容量 2：1 running + 1 queued）
     */
    submit_find(30000);
    BOOST_REQUIRE(
        WaitUntil([&] { return oh.rt->DriverCallsInFlightForTest() == 1 && oh.rt->QueuedOpsForTest() == 1; }, 10000));

    /**
     * 队列 +1（2 在队）
     */
    submit_find(30000);

    /**
     * 第 4 个：确定性 Overloaded
     */
    submit_find(30000);

    BOOST_REQUIRE(WaitUntil([&] { return overloaded.load() == 1; }, 10000));
    BOOST_CHECK_EQUAL(overloaded.load(), 1);

    /**
     * 收尾：Close 后在途 op 落定，worker 全退。
     */
    (void)oh.coll->Close();
    BOOST_REQUIRE(oh.db->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + kInflightCloseBudget}));
    BOOST_REQUIRE(WaitUntil([&] { return done.load() == 4; }, 30000));
    BOOST_REQUIRE(oh.db->IsClosed());
    BOOST_CHECK_EQUAL(oh.rt->LiveWorkersForTest(), 0);
}

BOOST_AUTO_TEST_CASE(t_dp_handle_close_scoped)
{
    BOOST_REQUIRE(g_prepared.load());

    /**
     * 句柄 Close 只封该句柄：兄弟句柄与 owner 仍接纳新命令（沿用
     * t_handle_close_scoped 的不变量），新入口同样服从。
     */
    auto db = NewOwner(MakeRtCfg(1, 4, 8000));
    auto rt = RuntimeOf(db);
    auto h1 = db->Collection({"bbt_ut", "dp_h1"});
    auto h2 = db->Collection({"bbt_ut", "dp_h2"});
    BOOST_REQUIRE(h1 && h2);
    auto c1 = h1.value();
    auto c2 = h2.value();

    /**
     * h1 在途 op（新入口 Find，server selection 中）——随后关闭 h1。
     */
    mongo::FindOptions fo;
    fo.limit = 1;
    std::atomic_bool h1_done{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            auto r = bbt_co_await c1->Find(EmptyDoc(), fo, Opt(30000));
            (void)r;
            h1_done.store(true);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return rt->DriverCallsInFlightForTest() == 1; }, 10000));

    (void)c1->Close();
    BOOST_CHECK(c1->IsClosed());

    /**
     * 已关句柄的新命令（多文档入口）→ Closed，不接触 owner 队列。
     */
    std::optional<result<std::vector<MongoDocument>>> out;
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(bbt_co_await c1->Find(EmptyDoc(), fo, Opt())); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
    std::optional<result<std::uint64_t>> out_cnt;
    BOOST_REQUIRE(RunInCoroutine([&] { out_cnt.emplace(bbt_co_await c1->CountDocuments(EmptyDoc(), Opt())); }));
    BOOST_REQUIRE(!*out_cnt);
    BOOST_CHECK(out_cnt->error().code == ErrorCode::Closed);

    /**
     * 兄弟句柄 h2 仍正常接纳（新入口 InsertMany 在不可达地址会进到
     * driver 后以 Unavailable 失败——证明它确实穿过了 owner 管线）。
     */
    std::optional<result<mongo::MongoInsertManyResult>> sib;
    const std::vector<MongoDocument> one{EmptyDoc()};
    BOOST_REQUIRE(RunInCoroutine([&] { sib.emplace(bbt_co_await c2->InsertMany(one, Opt(30000))); }));
    BOOST_REQUIRE(!*sib);
    BOOST_CHECK(sib->error().code == ErrorCode::Unavailable);

    (void)c2->Close();
    BOOST_REQUIRE(db->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + std::chrono::seconds{8}}));
    BOOST_REQUIRE(WaitUntil([&] { return h1_done.load(); }, 30000));
    BOOST_REQUIRE(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
}

BOOST_AUTO_TEST_CASE(t_dp_owner_close_drains_inflight)
{
    BOOST_REQUIRE(g_prepared.load());

    /**
     * 「Close 对已进入执行槽的在途 op 发布 Closed」：执行阶段屏障用既有
     * DriverCallsInFlightForTest（读 m_ops + phase==kRunning 的真实生产
     * 状态——op 已从队列取出、被 worker 占用执行槽；它诚实证明执行槽占用，
     * 不证明已进入 native driver 调用）。「已进入 native driver 后再 Close」
     * 的阶段证据由 mongo.live 的 failCommand 阻塞握手用例
     * （t_dp_find_blocked_owner_close）承载：waitForFailPoint 确认目标
     * find 已进入服务端阻塞后再发起 owner Close。driver 侧返回下界
     * 是 server_selection_timeout=15s（127.0.0.1:1 无监听，selection
     * 必满 15s 才失败），op deadline 40s——Close 发生在 op 已进入执行槽
     * 后、driver 真实返回前，故 Close 的 Finish(Closed) 必然先于任何
     * Unavailable/TimedOut 胜出：断言唯一终态 Closed（非「任一皆可」）。
     */
    auto db = NewOwner(MakeRtCfg(1, 4, 15000));
    auto rt = RuntimeOf(db);
    auto h = db->Collection({"bbt_ut", "dp_drain"});
    BOOST_REQUIRE(h);
    auto coll = h.value();

    mongo::FindOptions fo;
    fo.limit = 1;
    std::optional<result<std::vector<MongoDocument>>> out;
    std::atomic_bool out_ready{false};
    std::atomic_bool inflight_at_return{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            out.emplace(bbt_co_await coll->Find(EmptyDoc(), fo, Opt(40000)));

            /**
             * 逻辑终态交付当刻：该 op 仍在执行槽内（driver 未返回）。
             */
            inflight_at_return.store(rt->DriverCallsInFlightForTest() == 1);
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);

    /**
     * 执行阶段屏障：等 op 已离开队列、被 worker 占用执行槽后再 Close——
     * 不依赖 sleep 猜时序；此时 driver server selection 仍在 ~15s 窗口内。
     */
    BOOST_REQUIRE(WaitUntil([&] { return rt->DriverCallsInFlightForTest() == 1; }, 10000));

    BOOST_REQUIRE(db->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + std::chrono::seconds{20}}));
    BOOST_REQUIRE(WaitUntil([&] { return out_ready.load(std::memory_order_acquire); }, 30000));
    BOOST_REQUIRE(!*out);

    /**
     * 唯一终态必须是 Closed：Close 先于 driver 返回与 deadline，Finish
     * CAS 先落定者胜。Unavailable/TimedOut 在本时序下不可能合法先到——
     * 若断言变红，说明 close 收口顺序被破坏（如 driver 结果覆盖了
     * Close 的落定）。
     */
    BOOST_CHECK(out->error().code == ErrorCode::Closed);

    /**
     * 晚到的 driver 结果不覆盖已发布的 Closed。
     */
    BOOST_CHECK(inflight_at_return.load());
    BOOST_REQUIRE(db->IsClosed());

    /**
     * 物理收口：worker 全退、执行槽归零（Close 返回即成立，非提前宣告）。
     */
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
    BOOST_CHECK_EQUAL(rt->DriverCallsInFlightForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(rt->RunningDriverCallsForTest(), 0);
#endif
}

/**
 * CloseOptions 分层关闭（T5/D10 adapters）
 *
 * 三条终态在本节点分别取证：
 * - 逻辑封口：Close/RequestClose 后新命令 Closed、IsClosed()/CloseReason() 可读；
 * - 已提交 op 发布结果（Graceful 的责任面）：op 立刻拿到 Closed；
 * - 物理回收（WaitStopped）：worker 全退 + join + lease 归还后才 IsClosed()。
 * 关键不变量：Graceful 不把「driver 还在原生 worker 上跑」当成未完成，也绝不
 * 用有界窗口冒充物理释放；Co 域不得做 pthread join。
 *
 * 本组用例自足启动运行时：既可在整件顺序执行时复用 t_setup_scheduler，
 * 也可被 `--run_test` 单独选中（不依赖用例顺序）。
 */
static void EnsureCloseOptionsRuntime()
{
    if (g_scheduler->IsInitialized())
        return;
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = StaticThreadCount();
    cfg->m_cfg_stack_size = 1024 * 256;
    g_scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
}

BOOST_AUTO_TEST_CASE(t_owner_close_options_graceful_then_physical)
{
    EnsureCloseOptionsRuntime();
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    /**
     * server selection 上界 1500ms：driver 调用在途窗口足以覆盖 Graceful 判定，
     * 又能在 WaitStopped 期限内真实返回。
     */
    auto db = NewOwner(MakeRtCfg(1, 4, 1500));
    auto rt = RuntimeOf(db);
    auto h = db->Collection({"bbt_ut", "close_options"});
    BOOST_REQUIRE(h);
    auto coll = h.value();

    mongo::FindOptions fo;
    fo.limit = 1;
    std::optional<result<std::vector<MongoDocument>>> out;
    std::atomic_bool out_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            out.emplace(bbt_co_await coll->Find(EmptyDoc(), fo, Opt(40000)));
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return rt->DriverCallsInFlightForTest() == 1; }, 10000));

    /**
     * Co 域 Close(Graceful)：等「已提交 op 发布结果」，不等 driver 物理返回。
     * 完成标志用 release/acquire 发布：测试线程只在 acquire 到 ready 后读 optional
     * 与耗时，避免协程写、测试线程读的无同步竞争。
     */
    std::optional<result<void>> graceful;
    std::atomic_bool graceful_ready{false};
    std::atomic<long long> graceful_ms{0};
    bool close_task = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            const auto begin = std::chrono::steady_clock::now();
            graceful.emplace(
                db->Close(CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::milliseconds{1000})));
            graceful_ms.store(
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin)
                    .count());
            graceful_ready.store(true, std::memory_order_release);
        },
        close_task);
    BOOST_REQUIRE(close_task);
    BOOST_REQUIRE(WaitUntil([&] { return graceful_ready.load(std::memory_order_acquire); }, 10000));
    BOOST_REQUIRE(*graceful);

    /**
     * 不随 driver 的 1500ms 上界增长
     */
    BOOST_CHECK_LE(graceful_ms.load(), 900);

    /**
     * 已提交 op 立刻拿到唯一终态 Closed（逻辑责任已结清）。
     */
    BOOST_REQUIRE(WaitUntil([&] { return out_ready.load(std::memory_order_acquire); }, 10000));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);

    /**
     * 首因固定：之后再关闭不覆盖。
     */
    const auto reason = db->CloseReason();
    BOOST_REQUIRE(reason.has_value());
    BOOST_CHECK(static_cast<bool>(db->Close(CloseOptions::Immediate())));

    /**
     * 物理回收尚未发生：driver 仍在原生 worker 上，IsClosed 仍为假。
     */
    BOOST_CHECK(!db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 1);

    /**
     * native 宿主 WaitStopped 才是物理凭据：driver 返回 + worker 全退 + join。
     */
    auto stopped = db->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate,
            true,
            {},
            std::chrono::steady_clock::now() + std::chrono::milliseconds{8000}});
    BOOST_REQUIRE(stopped);
    BOOST_CHECK(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
    BOOST_CHECK_EQUAL(rt->DriverCallsInFlightForTest(), 0);
    BOOST_CHECK(db->CloseReason()->code == reason->code);
}

BOOST_AUTO_TEST_CASE(t_owner_close_immediate_keeps_driver_thread)
{
    EnsureCloseOptionsRuntime();
    BOOST_REQUIRE(g_scheduler->IsInitialized());
    auto db = NewOwner(MakeRtCfg(1, 4, 1500));
    auto rt = RuntimeOf(db);
    auto h = db->Collection({"bbt_ut", "close_immediate"});
    BOOST_REQUIRE(h);
    auto coll = h.value();

    mongo::FindOptions fo;
    fo.limit = 1;
    std::optional<result<std::vector<MongoDocument>>> out;
    std::atomic_bool out_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            out.emplace(bbt_co_await coll->Find(EmptyDoc(), fo, Opt(40000)));
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return rt->DriverCallsInFlightForTest() == 1; }, 10000));

    /**
     * Immediate 从 native 宿主调用：只封口，不等 driver。
     */
    const auto begin = std::chrono::steady_clock::now();
    auto immediate = db->Close(CloseOptions::Immediate());
    const auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin).count();
    BOOST_REQUIRE(immediate);

    /**
     * 远小于 1500ms 的 driver 上界
     */
    BOOST_CHECK_LT(elapsed, 800);

    /**
     * driver 调用仍在原生 worker 上推进（Mongo 保持自有工作线程，Co 路径不接管）。
     */
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 1);
    BOOST_CHECK(!db->IsClosed());

    /**
     * 新命令被拒绝（逻辑封口已生效）。
     */
    auto late = db->Collection({"bbt_ut", "late"});
    BOOST_CHECK(!late);
    BOOST_CHECK(late.error().code == ErrorCode::Closed);

    BOOST_REQUIRE(WaitUntil([&] { return out_ready.load(std::memory_order_acquire); }, 10000));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);

    auto stopped = db->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate,
            true,
            {},
            std::chrono::steady_clock::now() + std::chrono::milliseconds{8000}});
    BOOST_REQUIRE(stopped);
    BOOST_CHECK(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
}

BOOST_AUTO_TEST_CASE(t_owner_wait_stopped_preconditions)
{
    EnsureCloseOptionsRuntime();
    BOOST_REQUIRE(g_scheduler->IsInitialized());
    auto db = NewOwner(MakeRtCfg(1, 2, 1500));

    /**
     * 未封口：没有可等的物理事实。
     */
    auto early = db->Close(
        bbt::infra::CloseOptions{bbt::infra::CloseMode::Immediate, true, {}, bbt::coroutine::Deadline::max()});
    BOOST_CHECK(!early);
    BOOST_CHECK(early.error().code == ErrorCode::InvalidArgument);

    (void)db->Close(bbt::infra::CloseOptions::Immediate(MakeError(ErrorCode::Closed, "test: owner request close")));

    /**
     * join 会阻塞宿主线程：协程域调用必须被拒绝，而不是把 worker 一起挂住。
     */
    std::optional<result<void>> from_co;
    std::atomic_bool from_co_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            from_co.emplace(db->Close(
                bbt::infra::CloseOptions{
                    bbt::infra::CloseMode::Immediate,
                    true,
                    {},
                    std::chrono::steady_clock::now() + std::chrono::milliseconds{1000}}));
            from_co_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return from_co_ready.load(std::memory_order_acquire); }, 10000));
    BOOST_REQUIRE(*from_co);

    auto stopped = db->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate,
            true,
            {},
            std::chrono::steady_clock::now() + std::chrono::milliseconds{8000}});
    BOOST_REQUIRE(stopped);
    BOOST_CHECK(db->IsClosed());
}

/**
 * 关闭等待窗口的进展探针：只有等待方真正挂起（不占住协程 worker）才能推进。
 * 与 Close 协程共用同一原子窗口，避免用固定 sleep 猜时序。
 */
enum class CloseWaitWindow { kNotStarted, kInFlight, kReturned };

BOOST_AUTO_TEST_CASE(t_owner_wait_release_co_suspends_until_physical)
{
    EnsureCloseOptionsRuntime();
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    /**
     * driver 侧 server selection 上界 1500ms：在途窗口覆盖 Close 的等待，Co 调用
     * 必须挂起到 driver 真实返回 + worker 退出 + join + lease 归还之后才成功。
     */
    auto db = NewOwner(MakeRtCfg(1, 4, 1500));
    auto rt = RuntimeOf(db);
    auto h = db->Collection({"bbt_ut", "wait_release_co"});
    BOOST_REQUIRE(h);
    auto coll = h.value();

    mongo::FindOptions fo;
    fo.limit = 1;
    std::optional<result<std::vector<MongoDocument>>> out;
    std::atomic_bool out_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            out.emplace(bbt_co_await coll->Find(EmptyDoc(), fo, Opt(40000)));
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);

    /**
     * 阶段握手：先在途（op 已占用 worker 执行槽、driver 未返回），再触发关闭。
     */
    BOOST_REQUIRE(WaitUntil([&] { return rt->DriverCallsInFlightForTest() == 1; }, 10000));

    std::atomic<CloseWaitWindow> window{CloseWaitWindow::kNotStarted};
    std::atomic_int ticks{0};
    std::atomic_bool pulse_done{false};
    bool pulse_task = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            while (window.load(std::memory_order_acquire) == CloseWaitWindow::kNotStarted)
                bbtco_sleep(1);
            while (window.load(std::memory_order_acquire) == CloseWaitWindow::kInFlight) {
                ticks.fetch_add(1);
                bbtco_sleep(5);
            }
            pulse_done.store(true, std::memory_order_release);
        },
        pulse_task);
    BOOST_REQUIRE(pulse_task);

    std::optional<result<void>> closed;
    std::atomic_bool closed_ready{false};
    std::atomic<long long> close_ms{0};
    bool close_task = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            CloseOptions options;
            options.mode = CloseMode::Immediate;
            options.wait_release = true;
            options.deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{8000};
            const auto begin = std::chrono::steady_clock::now();
            window.store(CloseWaitWindow::kInFlight, std::memory_order_release);
            closed.emplace(db->Close(options));
            close_ms.store(
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin)
                    .count());
            window.store(CloseWaitWindow::kReturned, std::memory_order_release);
            closed_ready.store(true, std::memory_order_release);
        },
        close_task);
    BOOST_REQUIRE(close_task);

    /**
     * 等待期间：已提交 op 先拿到逻辑终态 Closed，物理回收尚未发生（worker 仍在），
     * 且同一调度器上的其它协程持续推进——等待是挂起，不是阻塞 worker。
     */
    BOOST_REQUIRE(WaitUntil([&] { return out_ready.load(std::memory_order_acquire); }, 10000));
    BOOST_CHECK(!db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 1);
    BOOST_REQUIRE(WaitUntil([&] { return ticks.load() >= 5; }, 10000));

    BOOST_REQUIRE(WaitUntil([&] { return closed_ready.load(std::memory_order_acquire); }, 15000));
    BOOST_REQUIRE(*closed);

    /**
     * 真等到了 driver 返回（不是提前成功）：耗时覆盖 1500ms 上界。
     */
    BOOST_CHECK_GE(close_ms.load(), 1000);

    /**
     * 返回即真实回收：worker 全退、join 与 lease 归还已完成。
     */
    BOOST_CHECK(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
    BOOST_CHECK_EQUAL(rt->DriverCallsInFlightForTest(), 0);
    BOOST_CHECK(window.load() == CloseWaitWindow::kReturned);

    /**
     * 进展探针协程自行退出，不在用例结束后继续引用栈上变量。
     */
    BOOST_REQUIRE(WaitUntil([&] { return pulse_done.load(std::memory_order_acquire); }, 5000));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
}

BOOST_AUTO_TEST_CASE(t_owner_wait_release_timeout_keeps_worker)
{
    EnsureCloseOptionsRuntime();
    BOOST_REQUIRE(g_scheduler->IsInitialized());
    auto db = NewOwner(MakeRtCfg(1, 4, 1500));
    auto rt = RuntimeOf(db);
    auto h = db->Collection({"bbt_ut", "wait_release_timeout"});
    BOOST_REQUIRE(h);
    auto coll = h.value();

    mongo::FindOptions fo;
    fo.limit = 1;
    std::optional<result<std::vector<MongoDocument>>> out;
    std::atomic_bool out_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            out.emplace(bbt_co_await coll->Find(EmptyDoc(), fo, Opt(40000)));
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return rt->DriverCallsInFlightForTest() == 1; }, 10000));

    /**
     * driver 上界 1500ms 而期限 300ms：Co 等待到期必须如实 TimedOut。
     */
    std::optional<result<void>> closed;
    std::atomic_bool closed_ready{false};
    bool close_task = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            CloseOptions options;
            options.mode = CloseMode::Immediate;
            options.wait_release = true;
            options.deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds{300};
            closed.emplace(db->Close(options));
            closed_ready.store(true, std::memory_order_release);
        },
        close_task);
    BOOST_REQUIRE(close_task);
    BOOST_REQUIRE(WaitUntil([&] { return closed_ready.load(std::memory_order_acquire); }, 10000));
    BOOST_REQUIRE(!*closed);
    BOOST_CHECK(closed->error().code == ErrorCode::TimedOut);

    /**
     * 到期不升级、不假回收：封口与首因保留，真实 worker 与在途 driver 责任保留。
     */
    BOOST_CHECK(db->CloseReason().has_value());
    BOOST_CHECK(!db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 1);

    /**
     * 同一份责任由 native 物理凭据继续兑现，超时不改写关闭模式。
     */
    auto stopped = db->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate,
            true,
            {},
            std::chrono::steady_clock::now() + std::chrono::milliseconds{8000}});
    BOOST_REQUIRE(stopped);
    BOOST_CHECK(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
    BOOST_REQUIRE(WaitUntil([&] { return out_ready.load(std::memory_order_acquire); }, 10000));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
}

BOOST_AUTO_TEST_CASE(t_owner_close_options_rejected_without_side_effects)
{
    EnsureCloseOptionsRuntime();
    BOOST_REQUIRE(g_scheduler->IsInitialized());
    auto db = NewOwner(MakeRtCfg(1, 4, 1500));
    auto rt = RuntimeOf(db);

    /**
     * 无期限 wait_release（Immediate + Deadline::max()）：值校验先于封口——
     * 不得置位关闭位、首因或 io 域，也不得置位组件层接纳门禁。
     */
    CloseOptions unbounded;
    unbounded.mode = CloseMode::Immediate;
    unbounded.wait_release = true;
    auto no_release = db->Close(unbounded);
    BOOST_REQUIRE(!no_release);
    BOOST_CHECK(no_release.error().code == ErrorCode::InvalidArgument);
    BOOST_CHECK(!db->IsClosed());
    BOOST_CHECK(!rt->IoDeadForTest());
    BOOST_CHECK(!db->CloseReason().has_value());

    /**
     * 无期限 Graceful：同一拒绝路径，同样零副作用。
     */
    auto no_graceful = db->Close(CloseOptions::Graceful(Deadline::max()));
    BOOST_REQUIRE(!no_graceful);
    BOOST_CHECK(no_graceful.error().code == ErrorCode::InvalidArgument);
    BOOST_CHECK(!rt->IoDeadForTest());
    BOOST_CHECK(!db->CloseReason().has_value());

    /**
     * 接纳面仍可用：非法调用没有污染关闭位。
     */
    auto still = db->Collection({"bbt_ut", "still_accepting"});
    BOOST_REQUIRE(still);

    /**
     * 合法 Immediate 仍然封口并交后台 finalizer；物理凭据由 WaitStopped 兑现。
     */
    BOOST_REQUIRE(db->Close(CloseOptions::Immediate()));
    BOOST_CHECK(db->CloseReason().has_value());
    auto late = db->Collection({"bbt_ut", "late"});
    BOOST_CHECK(!late);
    BOOST_CHECK(late.error().code == ErrorCode::Closed);
    auto stopped = db->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate,
            true,
            {},
            std::chrono::steady_clock::now() + std::chrono::milliseconds{8000}});
    BOOST_REQUIRE(stopped);
    BOOST_CHECK(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
}

BOOST_AUTO_TEST_CASE(t_owner_release_last_ref_in_coroutine_no_join)
{
    EnsureCloseOptionsRuntime();
    BOOST_REQUIRE(g_scheduler->IsInitialized());
    auto db = NewOwner(MakeRtCfg(1, 4, 1500));
    auto rt = RuntimeOf(db);
    auto h = db->Collection({"bbt_ut", "drop_in_co"});
    BOOST_REQUIRE(h);
    auto coll = h.value();

    mongo::FindOptions fo;
    fo.limit = 1;
    std::optional<result<std::vector<MongoDocument>>> out;
    std::atomic_bool out_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            out.emplace(bbt_co_await coll->Find(EmptyDoc(), fo, Opt(40000)));
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return rt->DriverCallsInFlightForTest() == 1; }, 10000));

    /**
     * 后台回收时段（driver 仍在途的 ~1.5s 窗口）内的进展探针：析构返回后
     * 调度器仍能推进其它协程，物理收口由后台 finalizer 承担。
     */
    std::atomic_bool dropped{false};
    std::atomic_int post_drop_ticks{0};
    std::atomic_bool pulse_done{false};
    bool pulse_task = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            while (!dropped.load(std::memory_order_acquire))
                bbtco_sleep(1);
            while (!rt->IsClosed()) {
                post_drop_ticks.fetch_add(1);
                bbtco_sleep(5);
            }
            pulse_done.store(true, std::memory_order_release);
        },
        pulse_task);
    BOOST_REQUIRE(pulse_task);

    /**
     * 在 Co worker 上丢弃 owner 对象（db）的引用（析构 → Close）。本用例仍由 rt
     * 保活 runtime，只证明 owner 析构路径；runtime 最后外部引用的真实回收由下一
     * 用例用 weak_ptr + pool lease 兑现。
     */
    std::atomic<long long> drop_ms{0};
    std::atomic_bool workers_alive_at_drop{false};
    bool drop_task = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            const auto begin = std::chrono::steady_clock::now();
            db.reset();
            drop_ms.store(
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin)
                    .count());

            /**
             * 返回当刻 worker 仍在：join 没有落在本协程 worker 上。
             */
            workers_alive_at_drop.store(rt->LiveWorkersForTest() == 1);
            dropped.store(true, std::memory_order_release);
        },
        drop_task);
    BOOST_REQUIRE(drop_task);
    BOOST_REQUIRE(WaitUntil([&] { return dropped.load(std::memory_order_acquire); }, 15000));

    /**
     * 远小于 driver 的 1500ms 在途窗口
     */
    BOOST_CHECK_LE(drop_ms.load(), 800);
    BOOST_CHECK(workers_alive_at_drop.load());
    BOOST_CHECK(!rt->IsClosed());
    BOOST_REQUIRE(WaitUntil([&] { return post_drop_ticks.load() >= 5; }, 10000));

    /**
     * 物理回收仍会完成（后台 finalizer），不是「返回即泄漏」。
     */
    BOOST_REQUIRE(WaitUntil([&] { return rt->IsClosed(); }, 15000));
    BOOST_CHECK(WaitUntil([&] { return rt->LiveWorkersForTest() == 0; }, 10000));
    BOOST_REQUIRE(WaitUntil([&] { return pulse_done.load(std::memory_order_acquire); }, 5000));
    BOOST_REQUIRE(WaitUntil([&] { return out_ready.load(std::memory_order_acquire); }, 10000));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
}

/**
 * 真正的 runtime 最后外部引用回收：上例由测试线程持有的 rt 保活 runtime，只证明
 * owner(db) 析构。本例不保留任何 runtime/handle 强引用（仅 weak_ptr 观察），在途
 * op 是最后的内部持有者；回收凭据用「weak 失效 + 本 owner 的 pool lease 归还」，
 * 不是 IsClosed 单指标，且 Co worker 上的丢弃耗时不随 driver 在途窗口增长。
 */
BOOST_AUTO_TEST_CASE(t_owner_runtime_last_ref_reclaimed)
{
    EnsureCloseOptionsRuntime();
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    /**
     * 专用 URI：探针是该 URI lease 的外部观察者，引用计数只反映本 owner。
     */
    auto cfg = MakeRtCfg(1, 4, 1500);
    cfg.uri = "mongodb://127.0.0.1:1/runtime_last_ref";
    const std::string eff = mongo_detail::EffectiveUri(cfg);
    auto pr = mongo_detail::AcquireMongoPool(eff);
    BOOST_REQUIRE(pr);
    auto probe = std::move(pr).value();
    BOOST_CHECK_EQUAL(probe.use_count(), 1);

    auto db = NewOwner(cfg);
    auto rt = RuntimeOf(db);
    auto h = db->Collection({"bbt_ut", "runtime_last_ref"});
    BOOST_REQUIRE(h);

    /**
     * 移出，避免测试结果对象长期保活句柄
     */
    auto coll = std::move(h).value();

    /**
     * 探针 + owner
     */
    BOOST_CHECK_EQUAL(probe.use_count(), 2);
    std::weak_ptr<mongo_detail::MongoRuntime> weak_rt = rt;

    /**
     * 在途 op 保活 handle 与 runtime；协程只捕获裸句柄指针（op 内已有 shared_ptr），
     * 不让协程对象/闭包额外保活 handle——否则 runtime 的最后引用不会真正归零。
     */
    mongo::FindOptions fo;
    fo.limit = 1;
    std::optional<result<std::vector<MongoDocument>>> out;
    std::atomic_bool out_ready{false};
    bool find_task = false;
    auto* hp = coll.get();
    g_scheduler->RegistCoroutineTask(
        [&, hp] {
            out.emplace(bbt_co_await hp->Find(EmptyDoc(), fo, Opt(40000)));
            out_ready.store(true, std::memory_order_release);
        },
        find_task);
    BOOST_REQUIRE(find_task);
    BOOST_REQUIRE(WaitUntil([&] { return rt->DriverCallsInFlightForTest() == 1; }, 10000));

    /**
     * Co worker 上释放全部外部引用（db/coll/rt）：若删除器在调用线程等 driver 或
     * join，这里会被 1500ms 在途窗口挡住。worker 当刻仍在 ⇒ join 未落在本协程。
     */
    std::atomic_bool dropped{false};
    std::atomic_bool workers_alive_at_drop{false};
    std::atomic<long long> drop_ms{0};
    bool drop_task = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            const auto begin = std::chrono::steady_clock::now();
            workers_alive_at_drop.store(rt->LiveWorkersForTest() == 1);
            db.reset();
            coll.reset();
            rt.reset();
            drop_ms.store(
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin)
                    .count());
            dropped.store(true, std::memory_order_release);
        },
        drop_task);
    BOOST_REQUIRE(drop_task);
    BOOST_REQUIRE(WaitUntil([&] { return dropped.load(std::memory_order_acquire); }, 15000));
    BOOST_CHECK_LE(drop_ms.load(), 800);
    BOOST_CHECK(workers_alive_at_drop.load());

    /**
     * 回收凭据：先等本 owner 的 pool lease 归还（finalizer 完成 join/lease 的物理
     * 事实），再确认最后外部引用已释放（删除器路径已进入）。weak::expired 只看
     * shared 计数归零，可能与「收口后 delete」交接同刻发生，故不单独当回收凭据。
     */
    BOOST_REQUIRE(WaitUntil([&] { return probe.use_count() == 1; }, 20000));
    BOOST_REQUIRE(WaitUntil([&] { return weak_rt.expired(); }, 20000));
    BOOST_REQUIRE(WaitUntil([&] { return out_ready.load(std::memory_order_acquire); }, 10000));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
    probe.reset();
}

/**
 * finalizer 与「最后引用归零」的交接阶段重叠：finalizer 完成 Finalize 之后停在
 * 「delete 责任尚未交接」那一刻（detail-only 停靠门，不靠概率撞窗口），此时在 Co
 * worker 上丢弃最后外部引用。断言归零线程只交出 delete 责任（当刻没有回收、也没有
 * 等 finalizer），放行后由 finalizer 线程真实回收恰好一次。回收计数同时覆盖
 * Dispose / FinalizerLoop 两条候选删除路径，读到 1 即「只删一次」。
 */
BOOST_AUTO_TEST_CASE(t_owner_finalizer_handoff_overlaps_last_ref)
{
    EnsureCloseOptionsRuntime();
    BOOST_REQUIRE(g_scheduler->IsInitialized());
    using mongo_detail::MongoRuntime;

    /**
     * 停靠门与回收观察是进程级静态量：用例退出（含断言失败抛出）时复位，避免
     * 其它 owner 的 finalizer 停在无人放行的门上。
     */
    struct SeamReset {
        ~SeamReset()
        {
            MongoRuntime::SetFinalizerHandoffPauseForTest(false);
            MongoRuntime::WatchFinalizerReclaimForTest(nullptr);
        }
    } seam_reset;

    auto cfg = MakeRtCfg(1, 4, 1500);
    cfg.uri = "mongodb://127.0.0.1:1/handoff_overlap";
    auto db = NewOwner(cfg);
    auto rt = RuntimeOf(db);
    auto h = db->Collection({"bbt_ut", "handoff_overlap"});
    BOOST_REQUIRE(h);
    auto coll = std::move(h).value();
    std::weak_ptr<MongoRuntime> weak_rt = rt;
    MongoRuntime::WatchFinalizerReclaimForTest(rt.get());
    BOOST_CHECK_EQUAL(MongoRuntime::ObservedFinalizerReclaimForTest().count, 0);

    /**
     * 阶段握手：finalizer 完成封口 + join + lease 归还后停在交接 delete 责任之前。
     * 此刻 worker 已全退、IsClosed 为真，唯一未落定的事实就是 delete。
     */
    MongoRuntime::SetFinalizerHandoffPauseForTest(true);
    BOOST_REQUIRE(db->Close(CloseOptions::Immediate()));
    BOOST_REQUIRE(MongoRuntime::WaitFinalizerHandoffStageForTest(20000));
    BOOST_CHECK(rt->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
    const std::thread::id test_thread = std::this_thread::get_id();

    /**
     * finalizer 停在交接前一刻时丢弃最后外部引用（Co worker）。
     */
    std::atomic_bool dropped{false};
    std::atomic<long long> drop_ms{0};
    std::atomic<std::thread::id> drop_thread{};
    bool drop_task = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            const auto begin = std::chrono::steady_clock::now();
            db.reset();
            coll.reset();
            rt.reset();
            drop_ms.store(
                std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - begin)
                    .count());
            drop_thread.store(std::this_thread::get_id());
            dropped.store(true, std::memory_order_release);
        },
        drop_task);
    BOOST_REQUIRE(drop_task);
    BOOST_REQUIRE(WaitUntil([&] { return dropped.load(std::memory_order_acquire); }, 15000));

    /**
     * 归零线程只交权：既没有执行回收（计数仍为 0，对象活在交接里），也没有等
     * finalizer 完成（停靠未被放行，等待会让本用例超时）。
     */
    BOOST_CHECK_LE(drop_ms.load(), 800);

    /**
     * 归零发生在 Co worker，不是测试线程
     */
    BOOST_CHECK(drop_thread.load() != test_thread);
    BOOST_CHECK_EQUAL(MongoRuntime::ObservedFinalizerReclaimForTest().count, 0);

    /**
     * 外部引用确已归零，回收责任在 finalizer 手里
     */
    BOOST_CHECK(weak_rt.expired());

    /**
     * 放行停靠：delete 只能由 finalizer 线程完成，且恰好一次。
     */
    MongoRuntime::SetFinalizerHandoffPauseForTest(false);
    BOOST_REQUIRE(WaitUntil([] { return MongoRuntime::ObservedFinalizerReclaimForTest().count == 1; }, 10000));
    const auto reclaimed = MongoRuntime::ObservedFinalizerReclaimForTest();
    BOOST_CHECK_EQUAL(reclaimed.count, 1);
    BOOST_CHECK(reclaimed.thread != drop_thread.load());
    BOOST_CHECK(reclaimed.thread != test_thread);
}

BOOST_AUTO_TEST_SUITE_END()

/**
 * docs/decisions/0007-mongo-owner-handles.md：owner/句柄归属
 *
 * 资源归属不变式（确定性探针，不靠压测）：
 * - 同一 owner 的多个集合句柄共享同一 worker 组：句柄数增长时
 * LiveWorkers 不变；
 * - 不同 owner 各自持有 worker 组/队列/ops 表：互不可见；
 * - max_queue=1 时跨集合共享背压：一个集合占住队列后，另一集合
 * 确定性 Overloaded；
 * - 句柄关闭只停自身接纳，不动兄弟与 owner；owner Close() 在 native 宿主
 * 等物理收口（ops 清空 + worker 全退），在受管协程则交后台 finalizer。
 */
BOOST_AUTO_TEST_SUITE(mongo_owner)

BOOST_AUTO_TEST_CASE(t_owner_config_validation)
{
    mongo::MongoRuntimeConfig base = MakeRtCfg();
    auto bad_uri = base;
    bad_uri.uri = "http://127.0.0.1:1/";
    BOOST_CHECK(mongo::CoMongoDb::Create(bad_uri).error().code == ErrorCode::InvalidArgument);
    auto zero_workers = base;
    zero_workers.worker_threads = 0;
    BOOST_CHECK(mongo::CoMongoDb::Create(zero_workers).error().code == ErrorCode::InvalidArgument);
    auto zero_queue = base;
    zero_queue.max_queue = 0;
    BOOST_CHECK(mongo::CoMongoDb::Create(zero_queue).error().code == ErrorCode::InvalidArgument);
    auto bad_target = mongo::MongoTarget{"", "c"};
    auto db = NewOwner(MakeRtCfg(1, 4));
    BOOST_CHECK(db->Collection(bad_target).error().code == ErrorCode::InvalidArgument);
    auto rt = RuntimeOf(db);
    CloseOwnerAndDrain(db, rt);
}

BOOST_AUTO_TEST_CASE(t_owner_multi_handles_share_workers)
{

    /**
     * 同一 owner 建 3 个集合句柄：worker 数恒等于配置值（2），
     * 不随句柄数增长。
     */
    auto db = NewOwner(MakeRtCfg(2, 8, 8000));
    auto rt = RuntimeOf(db);
    BOOST_REQUIRE(WaitUntil([&] { return rt->LiveWorkersForTest() == 2; }, 10000));

    std::vector<std::shared_ptr<mongo::CoMongoColl>> handles;
    for (const char* coll : {"c1", "c2", "c3"}) {
        auto h = db->Collection({"bbt_ut", coll});
        BOOST_REQUIRE(h);
        handles.push_back(std::move(h).value());
    }

    /**
     * 句柄建立后 worker 数仍 = 2。
     */
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 2);

    /**
     * 三个集合各发一个在途 op（不可达地址 server selection 占住），
     * worker 峰值仍 ≤ 2——集合数不放大执行资源。
     */
    std::atomic_int submitted{0};
    for (auto& h : handles) {
        bool succ = false;
        auto* hp = h.get();
        g_scheduler->RegistCoroutineTask(
            [hp, &submitted] {
                auto r = bbt_co_await hp->FindOne(EmptyDoc(), Opt(30000));
                (void)r;
                submitted.fetch_add(1);
            },
            succ);
        BOOST_REQUIRE(succ);
    }

    /**
     * 真实状态同步：3 在途 op、2 worker ⇒ 2 个被 worker 占用（phase==kRunning）、
     * 1 个排队，队列深度稳定为 1（Debug/Release 均可用，读既有 state）。
     */
    BOOST_REQUIRE(
        WaitUntil([&] { return rt->DriverCallsInFlightForTest() == 2 && rt->QueuedOpsForTest() == 1; }, 10000));
#ifdef BBT_INFRA_STRINGENT_DEBUG

    /**
     * debug 观测：无论集合句柄多少，运行中 driver 调用峰值 ≤ worker_threads。
     */
    BOOST_CHECK(rt->PeakDriverCallsForTest() <= 2);
#endif
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 2);

    for (auto& h : handles)
        (void)h->Close();
    BOOST_REQUIRE(db->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + kInflightCloseBudget}));
    BOOST_REQUIRE(WaitUntil([&] { return submitted.load() == 3; }, 30000));
    BOOST_REQUIRE(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(rt->RunningDriverCallsForTest(), 0);
#endif
}

BOOST_AUTO_TEST_CASE(t_owner_thread_identity)
{

    /**
     * 单 worker 依次服务两个不同集合；不可达地址让调用进入同步 driver
     * 路径，但不依赖真实 Mongo。每次调用收口后读取最后一项内部线程证据，
     * 直接断言目标集合及 worker、acquire、use、release 线程均正确。
     */
    auto db = NewOwner(MakeRtCfg(1, 4, 1000));
    auto rt = RuntimeOf(db);
    auto h1 = db->Collection({"bbt_ut", "identity_a"});
    auto h2 = db->Collection({"bbt_ut", "identity_b"});
    BOOST_REQUIRE(h1 && h2);
    BOOST_REQUIRE(WaitUntil([&] { return rt->LiveWorkersForTest() == 1; }, 10000));

    auto check_call = [&](const std::shared_ptr<mongo::CoMongoColl>& handle, const char* collection) {
        std::optional<result<std::optional<MongoDocument>>> out;
        BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(bbt_co_await handle->FindOne(EmptyDoc(), Opt(5000))); }));
        BOOST_REQUIRE(out);
        BOOST_REQUIRE(!*out);

        /**
         * 真实结果：调用确实经该 owner 的 worker 到达 driver 并以
         * Unavailable 失败（两构建面均成立）。
         */
        BOOST_CHECK(out->error().code == ErrorCode::Unavailable);

#ifdef BBT_INFRA_STRINGENT_DEBUG

        /**
         * Debug 面：读取内部执行域取证，断言目标集合与 worker/acquire/
         * use/release 线程 identity 均相同（同一次调用同一 worker）。
         */
        const auto evidence = rt->LastDriverThreadEvidenceForTest();
        BOOST_REQUIRE(evidence);
        BOOST_CHECK(evidence->worker_thread != std::thread::id{});
        BOOST_CHECK(evidence->worker_thread == evidence->acquire_thread);
        BOOST_CHECK(evidence->worker_thread == evidence->use_thread);
        BOOST_CHECK(evidence->worker_thread == evidence->release_thread);
        BOOST_CHECK_EQUAL(evidence->database, "bbt_ut");
        BOOST_CHECK_EQUAL(evidence->collection, collection);
#else

        /**
         * Release 面：执行域取证属 debug 观测层，本构建下不存在。
         */
        (void)collection;
#endif
    };

    check_call(h1.value(), "identity_a");
    check_call(h2.value(), "identity_b");
    CloseOwnerAndDrain(db, rt);
}

BOOST_AUTO_TEST_CASE(t_owner_isolation)
{

    /**
     * 两个 owner 各自 1 worker：资源隔离可验证。
     */
    auto db1 = NewOwner(MakeRtCfg(1, 4, 8000));
    auto db2 = NewOwner(MakeRtCfg(1, 4, 8000));
    auto rt1 = RuntimeOf(db1);
    auto rt2 = RuntimeOf(db2);
    BOOST_REQUIRE(WaitUntil([&] { return rt1->LiveWorkersForTest() == 1; }, 10000));
    BOOST_REQUIRE(WaitUntil([&] { return rt2->LiveWorkersForTest() == 1; }, 10000));
    BOOST_CHECK(rt1 != rt2);

    auto h1 = db1->Collection({"bbt_ut", "a"});
    auto h2 = db2->Collection({"bbt_ut", "a"});
    BOOST_REQUIRE(h1 && h2);
    BOOST_CHECK(std::static_pointer_cast<mongo_detail::MongoCollImpl>(h1.value())->Runtime() == rt1);
    BOOST_CHECK(std::static_pointer_cast<mongo_detail::MongoCollImpl>(h2.value())->Runtime() == rt2);

    CloseOwnerAndDrain(db1, rt1);
    CloseOwnerAndDrain(db2, rt2);
}

/**
 * 跨集合共享背压：owner worker=1、max_queue=1 下集合 A 占住唯一 worker、集合 B
 * 占满队列、集合 C 确定性 Overloaded——跨集合共享同一 owner 背压而非句柄各自上限。
 * 前置同步用真实状态 DriverCallsInFlightForTest（A 的 op 已离开队列被 worker 接管），
 * 两构建面一致；Release 面同样运行本用例（不再整体 ifdef 删除行为覆盖）。
 */
BOOST_AUTO_TEST_CASE(t_shared_backpressure_queue_one)
{

    /**
     * owner worker=1、max_queue=1：集合 A 占住唯一 worker 后，集合 B
     * 的 op 进队列占满容量，集合 C 的 op 确定性 Overloaded——跨集合
     * 共享背压而非句柄各自上限。
     */
    auto db = NewOwner(MakeRtCfg(1, 1, 8000));
    auto rt = RuntimeOf(db);
    auto ha = db->Collection({"bbt_ut", "a"});
    auto hb = db->Collection({"bbt_ut", "b"});
    auto hc = db->Collection({"bbt_ut", "c"});
    BOOST_REQUIRE(ha && hb && hc);
    auto* pa = ha.value().get();
    auto* pb = hb.value().get();
    auto* pc = hc.value().get();

    std::atomic_int b_overloaded{0};
    std::atomic_int c_overloaded{0};
    std::atomic_int b_done{0};
    std::atomic_int c_done{0};
    auto submit = [&](mongo::CoMongoColl* h, std::atomic_int& overloaded, std::atomic_int& done, int budget_ms) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [h, budget_ms, &overloaded, &done] {
                auto r = bbt_co_await h->FindOne(EmptyDoc(), Opt(budget_ms));
                if (!r && r.error().code == ErrorCode::Overloaded)
                    overloaded.fetch_add(1);
                done.fetch_add(1);
            },
            succ);
        BOOST_REQUIRE(succ);
    };

    /**
     * 占住唯一 worker（server selection 8s）
     */
    submit(pa, b_overloaded, b_done, 30000);

    /**
     * 等 A 的 op 已被 worker 从队列取出、进入执行槽（真实状态 phase==kRunning，
     * 读既有 m_ops），确保后续 B 进入队列时容量被 A 占住（两构建面一致）。
     */
    BOOST_REQUIRE(
        WaitUntil([&] { return rt->DriverCallsInFlightForTest() == 1 && rt->QueuedOpsForTest() == 0; }, 10000));

    /**
     * B：进队列占满容量 1
     */
    submit(pb, b_overloaded, b_done, 30000);

    /**
     * 可观察地确认 B 已在队列（而非仅「某协程跑过」）：QueuedOpsForTest
     * 读 m_queue_mtx 保护的 m_queue.size()，与接纳判定同临界区。
     */
    BOOST_REQUIRE(WaitUntil([&] { return rt->QueuedOpsForTest() == 1; }, 10000));

    /**
     * C：确定性 Overloaded
     */
    submit(pc, c_overloaded, c_done, 30000);

    /**
     * 队列容量=1 已被 B 占住：被拒的是 C 而不是 B。
     */
    BOOST_REQUIRE(WaitUntil([&] { return c_done.load() == 1; }, 10000));
    BOOST_CHECK_EQUAL(c_overloaded.load(), 1);
    BOOST_CHECK_EQUAL(b_overloaded.load(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK(rt->PeakDriverCallsForTest() <= 1);
#endif

    (void)ha.value()->Close();
    (void)hb.value()->Close();
    (void)hc.value()->Close();
    BOOST_REQUIRE(db->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + kInflightCloseBudget}));
    BOOST_REQUIRE(WaitUntil([&] { return b_done.load() + c_done.load() == 3; }, 30000));
    BOOST_REQUIRE(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(rt->RunningDriverCallsForTest(), 0);
#endif
}

BOOST_AUTO_TEST_CASE(t_handle_close_scoped)
{

    /**
     * 句柄关闭：自身新命令 Closed，兄弟句柄与 owner 不受影响；
     * 在途 op 保活到物理收口。
     */
    auto db = NewOwner(MakeRtCfg(1, 4, 8000));
    auto rt = RuntimeOf(db);
    auto h1 = db->Collection({"bbt_ut", "a"});
    auto h2 = db->Collection({"bbt_ut", "b"});
    BOOST_REQUIRE(h1 && h2);
    auto c1 = h1.value();
    auto c2 = h2.value();

    /**
     * owner 已 Running（NewOwner 内 Start）；句柄创建后经命令路径校验，
     * 不良构 BSON 在 worker 内、driver 调用前被拒 → InvalidArgument；
     * 证明 owner 在正常工作。
     */
    std::optional<result<std::optional<MongoDocument>>> noop;
    BOOST_REQUIRE(RunInCoroutine([&] { noop.emplace(bbt_co_await c2->FindOne(BadDoc(), Opt(2000))); }));
    BOOST_REQUIRE(!*noop);
    BOOST_CHECK(noop->error().code == ErrorCode::InvalidArgument);

    /**
     * h1 在途 op（server selection 中），随后关闭 h1：逻辑结果可以是
     * Closed/TimedOut/Unavailable（首个落定者胜出），但不能悬挂。
     */
    std::atomic_bool h1_done{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            auto r = bbt_co_await c1->FindOne(EmptyDoc(), Opt(30000));
            (void)r;
            h1_done.store(true);
        },
        succ);
    BOOST_REQUIRE(succ);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_REQUIRE(WaitUntil([&] { return rt->RunningDriverCallsForTest() == 1; }, 10000));
#endif

    /**
     * Release 面：无 running 计数；本用例不断言 h1 的具体结果（仅等其落定），
     * 句柄关闭不取消在途 op，收口断言与 op 是否已进入 driver 无关（逻辑终态
     * Closed/TimedOut/Unavailable 均可，用例不作区分），故不设前置同步。
     */
    (void)c1->Close();
    BOOST_CHECK(c1->IsClosed());

    /**
     * 已关句柄的新命令 → Closed（不接触 owner 队列）。
     */
    std::optional<result<std::optional<MongoDocument>>> out;
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(bbt_co_await c1->FindOne(EmptyDoc(), Opt())); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);

    /**
     * 兄弟句柄与 owner 仍可用：h2 命令进入同一 worker（排队等 h1 的
     * 在途收口后正常执行）。
     */
    std::atomic_bool h2_done{false};
    g_scheduler->RegistCoroutineTask(
        [&] {
            auto r = bbt_co_await c2->FindOne(EmptyDoc(), Opt(30000));
            (void)r;
            h2_done.store(true);
        },
        succ);
    BOOST_REQUIRE(succ);

    (void)c2->Close();
    BOOST_REQUIRE(db->Close(
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + kInflightCloseBudget}));
    BOOST_REQUIRE(WaitUntil([&] { return h1_done.load() && h2_done.load(); }, 30000));
    BOOST_REQUIRE(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(rt->RunningDriverCallsForTest(), 0);
#endif
}

BOOST_AUTO_TEST_CASE(t_owner_collection_requires_running)
{

    /**
     * 公共契约「owner 必须 Running 才接纳句柄」的生命周期门禁：
     * Create 未 Start → Collection → RuntimeUnavailable；
     * Start 之后 → Collection 成功；
     * Close → Collection → Closed。
     */
    auto d = mongo::CoMongoDb::Create(MakeRtCfg(1, 4));
    BOOST_REQUIRE(d);
    auto db = std::move(d).value();

    auto not_started = db->Collection({"bbt_ut", "x"});
    BOOST_REQUIRE(!not_started);
    BOOST_CHECK(not_started.error().code == ErrorCode::RuntimeUnavailable);

    BOOST_REQUIRE(db->Start());
    auto h = db->Collection({"bbt_ut", "x"});
    BOOST_REQUIRE(h);

    auto rt = RuntimeOf(db);
    (void)h.value()->Close();
    CloseOwnerAndDrain(db, rt);

    auto after_close = db->Collection({"bbt_ut", "y"});
    BOOST_REQUIRE(!after_close);
    BOOST_CHECK(after_close.error().code == ErrorCode::Closed);
}

/**
 * 关闭 finalizer 的确定性要求
 *
 * 覆盖「提前 Closed / finalize 并发」的四种时序：
 * - Close 取关闭权但 driver 仍在途时不得提前 IsClosed；
 * - 两个线程并发 Close 都在同一真实终态（join + lease 归还）之后返回，
 * 不重复 join/reset、不互相破坏；
 * - Start 后立即 Close（无在途）可收口且不再接纳；
 * - 同 URI 关闭其一不破坏兄弟 owner 仍持有的进程级 pool lease。
 */
BOOST_AUTO_TEST_CASE(t_owner_close_not_premature_during_inflight)
{
    auto db = NewOwner(MakeRtCfg(1, 8, 3000));
    auto rt = RuntimeOf(db);
    auto h = db->Collection({"bbt_ut", "c"});
    BOOST_REQUIRE(h);
    auto* hp = h.value().get();

    /**
     * 提交 2 个在途 op（不可达地址长调用）：第一个为目标在途 driver 调用，
     * 第二个使 Release 面可用真实队列深度（稳定为 1）做派发同步。
     */
    for (int i = 0; i < 2; ++i) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [hp] {
                auto r = bbt_co_await hp->FindOne(EmptyDoc(), Opt(30000));
                (void)r;
            },
            succ);
        BOOST_REQUIRE(succ);
    }
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_REQUIRE(WaitUntil([&] { return rt->RunningDriverCallsForTest() == 1; }, 10000));
#else

    /**
     * Release 面：真实状态同步——op 已被 worker 从队列取出、进入执行槽
     * （phase==kRunning），且队列深度稳定为 1（1 在 worker、1 排队）。
     * 读既有 state（m_ops/m_queue），不新增 Release 计数。
     */
    BOOST_REQUIRE(
        WaitUntil([&] { return rt->DriverCallsInFlightForTest() == 1 && rt->QueuedOpsForTest() == 1; }, 10000));
#endif

    std::atomic_bool closer_done{false};
    std::atomic_bool closed_at_return{false};
    std::thread closer([&] {
        BOOST_REQUIRE(db->Close(
            bbt::infra::CloseOptions{
                bbt::infra::CloseMode::Immediate,
                true,
                {},
                std::chrono::steady_clock::now() + std::chrono::seconds{8}}));
        closed_at_return.store(db->IsClosed());
        closer_done.store(true);
    });

    /**
     * Close 已取关闭权（IsRunning 转假）但 driver 调用仍在途：此刻
     * IsClosed 必须仍为假——终态只在 join + lease 归还之后发布。
     */
    BOOST_REQUIRE(WaitUntil([&] { return !rt->IsRunning(); }, 10000));
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(rt->RunningDriverCallsForTest(), 1);
#endif
    BOOST_CHECK(!closer_done.load());
    BOOST_CHECK(!db->IsClosed());
    closer.join();
    BOOST_CHECK(closer_done.load());
    BOOST_CHECK(closed_at_return.load());
    BOOST_REQUIRE(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(rt->RunningDriverCallsForTest(), 0);
#endif
}

BOOST_AUTO_TEST_CASE(t_owner_concurrent_close_two_threads)
{
    auto db = NewOwner(MakeRtCfg(2, 8, 2000));
    auto rt = RuntimeOf(db);
    auto h = db->Collection({"bbt_ut", "c"});
    BOOST_REQUIRE(h);
    auto* hp = h.value().get();

    /**
     * 占住 worker（不可达地址 server selection 2s）。多投 1 个 op 使
     * Release 面可用真实队列深度做确定性同步。
     */
    for (int i = 0; i < 3; ++i) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [hp] {
                auto r = bbt_co_await hp->FindOne(EmptyDoc(), Opt(30000));
                (void)r;
            },
            succ);
        BOOST_REQUIRE(succ);
    }
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_REQUIRE(WaitUntil([&] { return rt->RunningDriverCallsForTest() == 2; }, 10000));
#else

    /**
     * Release 面：真实状态同步——2 worker 均被占用（2 个 op phase==kRunning）。
     * 仅队列深度==1 无法区分「已完成 2 次取出」与「仅 1 个被取出」，故以 worker
     * 占用数为主判据，队列深度==1 为辅（第 3 个排队）。
     */
    BOOST_REQUIRE(
        WaitUntil([&] { return rt->DriverCallsInFlightForTest() == 2 && rt->QueuedOpsForTest() == 1; }, 10000));
#endif

    std::atomic_int returns{0};
    std::atomic_int closed_observed{0};
    auto close_once = [&] {
        BOOST_REQUIRE(db->Close(
            bbt::infra::CloseOptions{
                bbt::infra::CloseMode::Immediate,
                true,
                {},
                std::chrono::steady_clock::now() + std::chrono::seconds{8}}));
        if (db->IsClosed())
            closed_observed.fetch_add(1);
        returns.fetch_add(1);
    };
    std::thread a(close_once);
    std::thread b(close_once);
    a.join();
    b.join();

    /**
     * 两个并发 Close 都返回，且都观察到同一真实终态（同一 finalizer 事实）。
     */
    BOOST_CHECK_EQUAL(returns.load(), 2);
    BOOST_CHECK_EQUAL(closed_observed.load(), 2);
    BOOST_REQUIRE(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(rt->RunningDriverCallsForTest(), 0);
#endif
}

BOOST_AUTO_TEST_CASE(t_owner_start_then_immediate_close)
{
    auto d = mongo::CoMongoDb::Create(MakeRtCfg(2, 4));
    BOOST_REQUIRE(d);
    auto db = std::move(d).value();
    BOOST_REQUIRE(db->Start());

    /**
     * 经句柄反查 runtime（须在 Close 前）
     */
    auto rt = RuntimeOf(db);
    BOOST_REQUIRE(db->Close(

        /**
         * 无在途 op：直接收口
         */
        bbt::infra::CloseOptions{
            bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + std::chrono::seconds{8}}));
    BOOST_REQUIRE(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(rt->RunningDriverCallsForTest(), 0);
#endif
    auto after_close = db->Collection({"bbt_ut", "x"});
    BOOST_REQUIRE(!after_close);
    BOOST_CHECK(after_close.error().code == ErrorCode::Closed);
}

BOOST_AUTO_TEST_CASE(t_owner_same_uri_sibling_lease_preserved)
{

    /**
     * 同 URI 两 owner 共享进程级 pool lease：关闭其一不得归还/销毁兄弟
     * 仍持有的 lease（其他 owner 保持 Running 且 worker/pool 正常工作）。
     */
    auto db1 = NewOwner(MakeRtCfg(1, 4, 2000));
    auto rt1 = RuntimeOf(db1);
    auto db2 = NewOwner(MakeRtCfg(1, 4, 2000));
    auto rt2 = RuntimeOf(db2);
    BOOST_REQUIRE(rt1 != rt2);

    CloseOwnerAndDrain(db1, rt1);

    BOOST_CHECK(!db2->IsClosed());
    auto h2 = db2->Collection({"bbt_ut", "sibling"});
    BOOST_REQUIRE(h2);

    /**
     * 兄弟 owner 的 worker/pool 路径仍可用：良构校验在 worker 内、driver
     * 之前确定性拒绝不良构 BSON → InvalidArgument（证明未触碰已归还 lease）。
     */
    std::optional<result<std::optional<MongoDocument>>> out;
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(bbt_co_await h2.value()->FindOne(BadDoc(), Opt(2000))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::InvalidArgument);
    CloseOwnerAndDrain(db2, rt2);
}

/**
 * pool lease 失败归还 / 共享 owner 隔离 / 获取失败门禁
 *
 * 现有 t_worker_spawn_failure_finalizes 以 warm owner 保活同 URI pool，故只能
 * 证明「worker 组已收口」，无法证明「失败 owner 自己的 pool lease 已归还」
 * 或「兄弟 owner 的 lease 未被强清」——warm 恰好掩盖了这两条不变式。下列
 * 用例改用真实共享资源观察：测试直接经 AcquireMongoPool 取进程级 lease 的
 * 强引用，以 shared_ptr 引用计数 + weak_ptr 实际寿命判别失败 owner 是否归还
 * 引用、最后持有者销毁后 pool 是否真正析构、兄弟 owner 是否被误清。不新增
 * 任何测试开关（沿用既有可内部触达的获取路径）。
 *
 * (1) worker 组启动失败后，失败 owner 必须归还自己的 pool lease；lease 的
 * 最后持有者销毁后，进程级 pool 真正析构。
 */
BOOST_AUTO_TEST_CASE(t_spawn_failure_returns_pool_lease)
{
    BOOST_REQUIRE(g_prepared.load());

    /**
     * 专用 URI：除测试探针外，本用例是该 URI 的唯一持有者，故引用计数
     * 变化只反映失败 owner 的归还动作。
     */
    auto cfg = MakeUriCfg("mongodb://127.0.0.1:1/lease_probe");
    cfg.worker_threads = 4;
    const std::string eff = mongo_detail::EffectiveUri(cfg);
    auto pr = mongo_detail::AcquireMongoPool(eff);
    BOOST_REQUIRE(pr);
    auto probe = std::move(pr).value();

    /**
     * 仅测试探针持有
     */
    BOOST_CHECK_EQUAL(probe.use_count(), 1);

    /**
     * 逐失败点（reserve / 第 0/1/3 个 worker）：每次启动失败后 lease 引用
     * 都必须归还到探针独占（use_count 回到 1）。
     */
    const std::size_t fail_points[] = {mongo_detail::MongoRuntime::kSpawnFailReserve, 0, 1, 3};
    for (const auto at : fail_points) {
        auto info = mongo_detail::NewObjectInfo("test.rt.lease_fail");
        BOOST_REQUIRE(info);
        auto rt = mongo_detail::MongoRuntime::Create(cfg, info.value());
        rt->SetWorkerSpawnFailForTest(at);
        auto st = rt->Start();
        BOOST_REQUIRE_MESSAGE(!st, "spawn fail point " << at << " must fail");
        BOOST_CHECK(st.error().code == ErrorCode::InternalError);
        BOOST_CHECK(!rt->IsRunning());
        BOOST_CHECK(rt->IsClosed());

        /**
         * 关键判别：失败 owner 已归还 lease（若有缺陷则计数 > 1）。
         */
        BOOST_CHECK_EQUAL(probe.use_count(), 1);
    }

    /**
     * 是「归还」而非「销毁」：进程级 lease 仍是同一对象，未被重建或泄漏。
     */
    auto again = mongo_detail::AcquireMongoPool(eff);
    BOOST_REQUIRE(again);
    BOOST_CHECK_EQUAL(again.value().get(), probe.get());
    BOOST_CHECK_EQUAL(probe.use_count(), 2);

    /**
     * 最后持有者销毁：释放全部强引用后 lease 真正析构（weak 失效）。
     */
    std::weak_ptr<mongo_detail::MongoPoolLease> w = probe;
    probe.reset();
    again.value().reset();
    BOOST_CHECK(w.expired());

    /**
     * 之后仍可重新建立进程级 lease（旧 weak 不复活，新 pool 独立成立）。
     */
    auto fresh = mongo_detail::AcquireMongoPool(eff);
    BOOST_REQUIRE(fresh);
    BOOST_CHECK_EQUAL(fresh.value().use_count(), 1);
    fresh.value().reset();
}

/**
 * (2) 同 URI 共享进程级 lease：一个 owner 的 worker 组启动失败只归还它自己的
 * lease 引用，不得强清仍存活的兄弟 owner 的 lease，也不得泄漏自身引用。
 */
BOOST_AUTO_TEST_CASE(t_spawn_failure_lease_isolated_from_sibling)
{
    BOOST_REQUIRE(g_prepared.load());

    auto cfg = MakeRtCfg(1, 4, 2000);
    const std::string eff = mongo_detail::EffectiveUri(cfg);

    /**
     * 健康 owner A：Running 且持有该 URI 的 lease。
     */
    auto db = NewOwner(cfg);
    auto rt = RuntimeOf(db);
    BOOST_REQUIRE(rt->IsRunning());
    BOOST_REQUIRE(WaitUntil([&] { return rt->LiveWorkersForTest() == 1; }, 10000));

    auto pr = mongo_detail::AcquireMongoPool(eff);
    BOOST_REQUIRE(pr);
    auto probe = std::move(pr).value();

    /**
     * 探针 + owner A
     */
    BOOST_CHECK_EQUAL(probe.use_count(), 2);

    /**
     * 失败 owner B：同 URI，worker 组启动失败。
     */
    auto info = mongo_detail::NewObjectInfo("test.rt.sibling_fail");
    BOOST_REQUIRE(info);
    auto rt_b = mongo_detail::MongoRuntime::Create(cfg, info.value());
    rt_b->SetWorkerSpawnFailForTest(0);
    auto st = rt_b->Start();
    BOOST_REQUIRE(!st);
    BOOST_CHECK(st.error().code == ErrorCode::InternalError);
    BOOST_CHECK(rt_b->IsClosed());

    /**
     * B 只归还自己的引用：A 的引用仍在（计数回到 2，而非被强清为 1）。
     */
    BOOST_CHECK_EQUAL(probe.use_count(), 2);

    /**
     * A 不受影响：仍 Running、未关闭，可继续建句柄并派发命令。
     */
    BOOST_CHECK(rt->IsRunning());
    BOOST_CHECK(!db->IsClosed());
    auto h = db->Collection({"bbt_ut", "sibling"});
    BOOST_REQUIRE(h);
    std::optional<result<std::optional<MongoDocument>>> out;
    BOOST_REQUIRE(RunInCoroutine([&] { out.emplace(bbt_co_await h.value()->FindOne(BadDoc(), Opt(2000))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::InvalidArgument);

    /**
     * 关闭 A：A 归还自己的引用后，仅探针持有。
     */
    CloseOwnerAndDrain(db, rt);
    BOOST_CHECK_EQUAL(probe.use_count(), 1);
    probe.reset();
}

/**
 * (3) pool 创建/获取失败的可控门禁：失败留在 kCreated（可重试），不泄漏
 * worker；释放占用后同一 owner 可重试成功。
 */
BOOST_AUTO_TEST_CASE(t_pool_acquire_failure_gates_start)
{
    BOOST_REQUIRE(g_prepared.load());

    /**
     * (A) 驱动侧创建失败：URI 通过装配校验（scheme 合法）但驱动拒绝解析/
     * 选项 → Start 如实返回 InvalidArgument，owner 留在 kCreated（可
     * 重试），未起任何 worker。真实驱动抛出点，非注入开关。
     */
    {
        auto cfg = MakeUriCfg("mongodb://127.0.0.1:99999999/bbt_ut");
        auto info = mongo_detail::NewObjectInfo("test.rt.bad_uri");
        BOOST_REQUIRE(info);
        auto rt = mongo_detail::MongoRuntime::Create(cfg, info.value());
        auto st = rt->Start();
        BOOST_REQUIRE(!st);
        BOOST_CHECK(st.error().code == ErrorCode::InvalidArgument);
        BOOST_CHECK(!rt->IsRunning());

        /**
         * 仍在 kCreated → 允许条件就绪后重试
         */
        BOOST_CHECK(!rt->IsClosed());
        BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
    }

    /**
     * (B) 进程级 pool 上限：占满 kMongoMaxPools 个 distinct 存活 lease 后，
     * 第 kMongoMaxPools+1 个获取被拒绝（真实计数门禁）；释放一个槽位后
     * 同一 owner 重试 Start 成功。
     */
    std::vector<std::shared_ptr<mongo_detail::MongoPoolLease>> fills;
    for (std::size_t i = 0; i < mongo_detail::kMongoMaxPools; ++i) {
        auto cfg = MakeUriCfg("mongodb://127.0.0.1:1/fill_" + std::to_string(i));
        auto r = mongo_detail::AcquireMongoPool(mongo_detail::EffectiveUri(cfg));
        BOOST_REQUIRE_MESSAGE(r, "fill slot " << i << " must be available");
        fills.push_back(std::move(r).value());
    }

    auto over = MakeUriCfg("mongodb://127.0.0.1:1/over_limit");
    over.worker_threads = 1;
    auto info = mongo_detail::NewObjectInfo("test.rt.over_limit");
    BOOST_REQUIRE(info);
    auto rt = mongo_detail::MongoRuntime::Create(over, info.value());
    auto st = rt->Start();
    BOOST_REQUIRE(!st);
    BOOST_CHECK(st.error().code == ErrorCode::InvalidArgument);
    BOOST_CHECK(!rt->IsRunning());

    /**
     * 仍在 kCreated，可重试
     */
    BOOST_CHECK(!rt->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);

    /**
     * 释放一个占用槽位：同一 owner 重试 Start 即可成功（可重试契约）。
     */
    fills.pop_back();
    auto retry = rt->Start();
    BOOST_REQUIRE(retry);
    BOOST_CHECK(rt->IsRunning());
    (void)rt->Close();
    BOOST_CHECK(rt->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
    fills.clear();
}

BOOST_AUTO_TEST_SUITE_END()

/**
 * mongo_bson：BSON 公共入口
 * 只经公开面（bbt/infra/mongo/Bson.hpp）验证构造/读取语义，不引入
 * bsoncxx 类型断言；JSON 形态断言用 canonical/relaxed 输出里的 Extended JSON
 * 标记（$numberInt/$numberLong/$numberDouble），不绑定空格排版。
 */
BOOST_AUTO_TEST_SUITE(mongo_bson)

BOOST_AUTO_TEST_CASE(t_bson_from_json_canonical_roundtrip)
{
    auto doc = mongo::MongoDocument::FromJson(R"({"_id":"demo-1","n":7,"ratio":1.5,"ok":true,"tag":"t"})");
    BOOST_REQUIRE(doc);
    const auto& d = doc.value();
    BOOST_CHECK(d.Has("_id"));
    BOOST_CHECK(d.Has("n"));
    BOOST_CHECK(!d.Has("missing"));
    BOOST_CHECK(d.GetString("_id") && d.GetString("_id").value() == "demo-1");
    BOOST_CHECK(d.GetString("tag") && d.GetString("tag").value() == "t");
    BOOST_CHECK(d.GetInt64("n") && d.GetInt64("n").value() == 7);
    BOOST_CHECK(d.GetDouble("ratio") && d.GetDouble("ratio").value() == 1.5);
    BOOST_CHECK(d.GetBool("ok") && d.GetBool("ok").value());

    /**
     * 默认 canonical
     */
    auto json = d.ToJson();
    BOOST_REQUIRE(json);
    BOOST_CHECK(json.value().find("$numberInt") != std::string::npos);
    BOOST_CHECK(json.value().find("$numberDouble") != std::string::npos);
    BOOST_CHECK(json.value().find("demo-1") != std::string::npos);

    /**
     * canonical 输出可作为输入回环：类型与值都保持。
     */
    auto again = mongo::MongoDocument::FromJson(json.value());
    BOOST_REQUIRE(again);
    BOOST_CHECK(again.value().GetInt64("n") && again.value().GetInt64("n").value() == 7);
    BOOST_CHECK(again.value().GetDouble("ratio") && again.value().GetDouble("ratio").value() == 1.5);
    BOOST_CHECK(again.value().GetString("tag") && again.value().GetString("tag").value() == "t");

    /**
     * relaxed 也保留值（数值类型见 t_bson_numeric_fidelity）。
     */
    auto relaxed = d.ToJson(mongo::MongoJsonMode::k_relaxed);
    BOOST_REQUIRE(relaxed);
    BOOST_CHECK(relaxed.value().find("$numberInt") == std::string::npos);
}

/**
 * 契约 的五行数值保真，逐条用实测断言固化。
 */
BOOST_AUTO_TEST_CASE(t_bson_numeric_fidelity)
{

    /**
     * 1) JSON 写 7 → int32：canonical 出 $numberInt，relaxed 出裸 7。
     */
    auto i32 = mongo::MongoDocument::FromJson(R"({"v":7})");
    BOOST_REQUIRE(i32);
    auto i32_canon = i32.value().ToJson();
    BOOST_REQUIRE(i32_canon);
    BOOST_CHECK(i32_canon.value().find("$numberInt") != std::string::npos);
    auto i32_relaxed = i32.value().ToJson(mongo::MongoJsonMode::k_relaxed);
    BOOST_REQUIRE(i32_relaxed);
    BOOST_CHECK(i32_relaxed.value().find("$numberInt") == std::string::npos);
    BOOST_CHECK(i32.value().GetInt64("v") && i32.value().GetInt64("v").value() == 7);

    /**
     * 只接受 double，不把 int 隐式当 double。
     */
    BOOST_CHECK(!i32.value().GetDouble("v"));
    BOOST_CHECK(i32.value().GetDouble("v").error().code == ErrorCode::TypeMismatch);

    /**
     * 2) 需要 int64 必须写 $numberLong；canonical 保持 $numberLong。
     */
    auto i64 = mongo::MongoDocument::FromJson(R"({"v":{"$numberLong":"7"}})");
    BOOST_REQUIRE(i64);
    auto i64_canon = i64.value().ToJson();
    BOOST_REQUIRE(i64_canon);
    BOOST_CHECK(i64_canon.value().find("$numberLong") != std::string::npos);
    BOOST_CHECK(i64.value().GetInt64("v") && i64.value().GetInt64("v").value() == 7);

    /**
     * 3) $numberLong + relaxed 往返 → 回环后变成 int32（relaxed 输出裸 7）。
     */
    auto i64_relaxed = i64.value().ToJson(mongo::MongoJsonMode::k_relaxed);
    BOOST_REQUIRE(i64_relaxed);
    BOOST_CHECK(i64_relaxed.value().find("$numberLong") == std::string::npos);
    auto degraded = mongo::MongoDocument::FromJson(i64_relaxed.value());
    BOOST_REQUIRE(degraded);
    auto degraded_canon = degraded.value().ToJson();
    BOOST_REQUIRE(degraded_canon);
    BOOST_CHECK(degraded_canon.value().find("$numberInt") != std::string::npos);
    BOOST_CHECK(degraded.value().GetInt64("v") && degraded.value().GetInt64("v").value() == 7);

    /**
     * 4) int64 值域：超出 int32 的值经 canonical 往返仍是同一个 int64。
     */
    auto wide = mongo::MongoDocument::FromJson(R"({"v":{"$numberLong":"2147483648"}})");
    BOOST_REQUIRE(wide);
    BOOST_CHECK(wide.value().GetInt64("v") && wide.value().GetInt64("v").value() == 2147483648LL);
    auto wide_json = wide.value().ToJson();
    BOOST_REQUIRE(wide_json);
    auto wide_again = mongo::MongoDocument::FromJson(wide_json.value());
    BOOST_REQUIRE(wide_again);
    BOOST_CHECK(wide_again.value().GetInt64("v") && wide_again.value().GetInt64("v").value() == 2147483648LL);

    /**
     * 5) double 字面量保留 double：canonical 出 $numberDouble，relaxed 不塌成整数。
     */
    auto dbl = mongo::MongoDocument::FromJson(R"({"v":7.0})");
    BOOST_REQUIRE(dbl);
    BOOST_CHECK(dbl.value().GetDouble("v") && dbl.value().GetDouble("v").value() == 7.0);
    BOOST_CHECK(!dbl.value().GetInt64("v"));
    BOOST_CHECK(dbl.value().GetInt64("v").error().code == ErrorCode::TypeMismatch);
    auto dbl_canon = dbl.value().ToJson();
    BOOST_REQUIRE(dbl_canon);
    BOOST_CHECK(dbl_canon.value().find("$numberDouble") != std::string::npos);
    auto dbl_relaxed = dbl.value().ToJson(mongo::MongoJsonMode::k_relaxed);
    BOOST_REQUIRE(dbl_relaxed);
    BOOST_CHECK(dbl_relaxed.value().find("7.0") != std::string::npos);

    /**
     * 6) 超出 int64 的整数 → InvalidArgument，不静默变 double。
     */
    auto overflow = mongo::MongoDocument::FromJson(R"({"v":9223372036854775808})");
    BOOST_REQUIRE(!overflow);
    BOOST_CHECK(overflow.error().code == ErrorCode::InvalidArgument);
    BOOST_CHECK(overflow.error().backend_category == "bsoncxx");
}

BOOST_AUTO_TEST_CASE(t_bson_invalid_json)
{

    /**
     * 空串 / 仅空白 / 畸形 / 顶层非对象 / 多份对象 / 尾部内容：都在构造侧拒绝，
     * 不接触 driver。libbson 的 JSON 前端只消费第一份文档、不报后续内容，故
     * 单文档完整性由 FromJson 自己核对（相邻与空白分隔的第二份对象都算畸形）。
     */
    for (const char* bad :
         {"",
          "   ",
          "not json",
          R"({"a":)",
          R"({"a" 1})",
          R"({"v":1}{"w":2})",
          R"({"v":1} {"w":2})",
          "{\"v\":1}\n\t {\"w\":2}",
          R"({"v":1} trailing)",
          R"({"v":1}})"}) {
        auto doc = mongo::MongoDocument::FromJson(bad);
        BOOST_REQUIRE_MESSAGE(!doc, "应拒绝的 JSON 被接受: " << bad);
        BOOST_CHECK(doc.error().code == ErrorCode::InvalidArgument);
    }

    /**
     * 解析期失败携带 bsoncxx 后端原因（空串/空白在前置分支，无后端码）。
     */
    auto malformed = mongo::MongoDocument::FromJson(R"({"a":)");
    BOOST_REQUIRE(!malformed);
    BOOST_CHECK(malformed.error().backend_category == "bsoncxx");

    /**
     * 多文档是本组件的完整性检查，不是库报错：不得冒充 backend 元数据。
     */
    auto joined = mongo::MongoDocument::FromJson(R"({"v":1}{"w":2})");
    BOOST_REQUIRE(!joined);
    BOOST_CHECK(joined.error().code == ErrorCode::InvalidArgument);
    BOOST_CHECK(joined.error().backend_category.empty());

    /**
     * 顶层数组不是文档：libbson 会把它当数字键文档，这里显式拒绝。
     */
    auto array = mongo::MongoDocument::FromJson("[1,2]");
    BOOST_REQUIRE(!array);
    BOOST_CHECK(array.error().code == ErrorCode::InvalidArgument);

    /**
     * 正向对照：正常前后空白、字符串内的括号与转义不算第二份文档。
     */
    auto spaced = mongo::MongoDocument::FromJson(
        " \n\t"
        R"({"s":"a}b{ c \" }"})"
        "  \r\n");
    BOOST_REQUIRE(spaced);
    BOOST_CHECK(spaced.value().GetString("s").value() == "a}b{ c \" }");
}

BOOST_AUTO_TEST_CASE(t_bson_missing_field_not_found)
{
    auto doc = mongo::MongoDocument::FromJson(R"({"a":1})");
    BOOST_REQUIRE(doc);
    const auto& d = doc.value();
    BOOST_CHECK(!d.Has("b"));
    BOOST_CHECK(d.GetString("b").error().code == ErrorCode::NotFound);
    BOOST_CHECK(d.GetInt64("b").error().code == ErrorCode::NotFound);
    BOOST_CHECK(d.GetDouble("b").error().code == ErrorCode::NotFound);
    BOOST_CHECK(d.GetBool("b").error().code == ErrorCode::NotFound);
}

BOOST_AUTO_TEST_CASE(t_bson_type_mismatch)
{
    auto doc = mongo::MongoDocument::FromJson(R"({"s":"x","i":1,"d":1.5,"b":true})");
    BOOST_REQUIRE(doc);
    const auto& d = doc.value();

    /**
     * 每个 getter 只接受自己的类型（GetInt64 另接受 int32/int64）。
     */
    BOOST_CHECK(d.GetInt64("s").error().code == ErrorCode::TypeMismatch);
    BOOST_CHECK(d.GetDouble("i").error().code == ErrorCode::TypeMismatch);
    BOOST_CHECK(d.GetInt64("d").error().code == ErrorCode::TypeMismatch);
    BOOST_CHECK(d.GetBool("i").error().code == ErrorCode::TypeMismatch);
    BOOST_CHECK(d.GetString("b").error().code == ErrorCode::TypeMismatch);
    BOOST_CHECK(d.GetString("i").error().code == ErrorCode::TypeMismatch);

    /**
     * 正向对照：同字段用正确 getter 读取成功。
     */
    BOOST_CHECK(d.GetString("s") && d.GetString("s").value() == "x");
    BOOST_CHECK(d.GetInt64("i") && d.GetInt64("i").value() == 1);
    BOOST_CHECK(d.GetDouble("d") && d.GetDouble("d").value() == 1.5);
    BOOST_CHECK(d.GetBool("b") && d.GetBool("b").value());
}

BOOST_AUTO_TEST_CASE(t_bson_empty_document)
{

    /**
     * 空字节载体：读取/序列化都按 InvalidArgument 拒绝，Has 为假。
     */
    const MongoDocument empty{};
    BOOST_CHECK(!empty.Has("a"));
    BOOST_CHECK(empty.ToJson().error().code == ErrorCode::InvalidArgument);
    BOOST_CHECK(empty.GetString("a").error().code == ErrorCode::InvalidArgument);
    BOOST_CHECK(empty.GetInt64("a").error().code == ErrorCode::InvalidArgument);

    /**
     * 手写最小良构文档 {} 仍可用（bytes 是唯一真源）：无字段但可序列化。
     */
    const MongoDocument empty_doc{{0x05, 0x00, 0x00, 0x00, 0x00}};
    auto json = empty_doc.ToJson();
    BOOST_REQUIRE(json);
    BOOST_CHECK(!empty_doc.Has("a"));
    BOOST_CHECK(empty_doc.GetInt64("a").error().code == ErrorCode::NotFound);

    /**
     * 不良构字节（声明长度超出缓冲）→ InvalidArgument，不冒充 NotFound。
     */
    const MongoDocument bad{{0x10, 0x00, 0x00, 0x00, 0x00}};
    BOOST_CHECK(bad.ToJson().error().code == ErrorCode::InvalidArgument);
    BOOST_CHECK(bad.GetInt64("a").error().code == ErrorCode::InvalidArgument);
    BOOST_CHECK(!bad.Has("a"));
}

BOOST_AUTO_TEST_SUITE_END()
