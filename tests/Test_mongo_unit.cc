// Issue #7 单元验收（不依赖真实 MongoDB/容器）：
//
//   t_setup_scheduler             — 启动 Scheduler（共享 executor 来源）。
//   t_create_before_scheduler     — 运行时未初始化（从未 Start）时 Create →
//                                   RuntimeUnavailable（对象身份需要已
//                                   初始化的运行时）。
//   t_config_validation           — 非法装配逐项 InvalidArgument。
//   t_command_prechecks           — 未 Start RuntimeUnavailable；空文档/
//                                   空 filter 空 update InvalidArgument；
//                                   Close 后 Closed。
//   t_invalid_context_plain       — 普通线程直接调命令 → InvalidContext。
//   t_invalid_bson                — 不良构 BSON → InvalidArgument（校验在
//                                   worker 内、driver 调用之前，不需要服务端）。
//   t_server_unreachable          — server selection 超时 → Unavailable
//                                   （driver 侧失败映射）。
//   t_capacity_overloaded         — worker_threads=1 时一个 op 占住 worker
//                                   （RunningDriverCalls 观测），queue=2 占满
//                                   后第 4 个命令确定性 Overloaded。
//   t_deadline_only               — 不可达地址下 deadline → TimedOut；逻辑
//                                   返回时物理 driver 调用仍在进行（计数
//                                   不变），不冒充物理收口。
//   t_close_during_inflight       — 在途 driver 调用随 owner Close() 落定
//                                   Closed；物理收口（driver 返回 + worker
//                                   退出）才 IsClosed。
//   t_worker_threads_bound        — 4 个并发 op 峰值 driver 调用 ≤ 2。
//   t_owner_thread_identity       — 单 worker 跨两个集合的 acquire/use/release
//                                   线程 identity 均相同（内部观测证据）。
//
// 关闭语义（进程寿命修订）：RequestClose/WaitClosed/CloseStatus/取消令牌
// 全部删除——Close() 幂等、任意线程可调用，封口 + 唤醒等待者 + 有界等待在途
// 归零；driver 同步调用不可强杀，超过 kCloseDrainTimeout 的配置下 Close 提前
// 返回、IsClosed() 在 driver 返回后才成立。用例统一经 CloseClientDrained()
// 在有界窗口内确认真实收口。
//
// 两个验证面（观测数据已迁入 src/debug/InfraDebug.hpp）：
//   Debug 面（BBT_INFRA_STRINGENT_DEBUG=ON）— 追加 debug 观测断言：运行中
//     driver 调用计数/峰值（并发上限）、执行域取证（同一次调用
//     acquire/use/release 同 worker）、以及「逻辑返回≠物理收口」的在途计数；
//   Release 面（宏 OFF）— debug 组件与本用例的 debug 访问器根本不存在。用例
//     退回真实生产状态与结果断言：LiveWorkersForTest() 、QueuedOpsForTest()
//     （真实队列深度，用作同步）、IsClosed()、错误码、结果计数。故关掉宏不会
//     让任何用例恒过。同步点一律用真实事件（队列深度/结果闩），不用 sleep。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
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
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/CoMongoCli.hpp>

// impl 观测钩子分两类：
//   - 真实生产状态（Release 下同样可用）：LiveWorkersForTest /
//     QueuedOpsForTest（读 m_live_workers / m_queue）；
//   - debug 观测层（src/debug/InfraDebug.hpp，仅 BBT_INFRA_STRINGENT_DEBUG
//     下存在）：RunningDriverCallsForTest / PeakDriverCallsForTest /
//     LastDriverThreadEvidenceForTest。
// 各用例按「Debug 面（宏 ON，追加观测断言）/ Release 面（宏 OFF，退回真实
// 状态与结果断言，不得恒过）」两面组织，见用例内 #ifdef。MongoRuntime 是
// Issue #40 的资源 owner。
#include "mongo/MongoRuntime.hpp"

// Issue #12 M4：直接经真实 driver 构造非法 URI，核对 ClassifyDriverError
// 的输入错误分类（URI 解析离线完成，不需要服务端）。
#include <mongocxx/v1/uri.hpp>

using namespace bbt::infra;
using bbt::coroutine::Deadline;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

constexpr int kBudgetMs = 45000;

mongo::MongoRuntimeConfig MakeUriCfg(std::string uri) {
    mongo::MongoRuntimeConfig cfg;
    cfg.uri                      = std::move(uri);
    cfg.worker_threads           = 2;
    cfg.max_queue                = 8;
    cfg.server_selection_timeout = std::chrono::milliseconds(15000);
    cfg.connect_timeout          = std::chrono::milliseconds(2000);
    cfg.socket_timeout           = std::chrono::milliseconds(2000);
    cfg.wait_queue_timeout       = std::chrono::milliseconds(2000);
    return cfg;
}

// 127.0.0.1:1 惯例无监听：server selection 必然在 serverSelectionTimeoutMS
// 后失败——driver 侧阻塞有确定上界，且期间 worker 被物理占用。
MongoClientConfig MakeDeadCfg(std::size_t workers = 2,
                              std::size_t queue   = 8,
                              int         sst_ms  = 15000) {
    MongoClientConfig cfg;
    cfg.uri                      = "mongodb://127.0.0.1:1/bbt_ut";
    cfg.database                 = "bbt_ut";
    cfg.collection               = "c";
    cfg.worker_threads           = workers;
    cfg.max_queue                = queue;
    cfg.server_selection_timeout = std::chrono::milliseconds(sst_ms);
    cfg.connect_timeout          = std::chrono::milliseconds(2000);
    cfg.socket_timeout           = std::chrono::milliseconds(2000);
    cfg.wait_queue_timeout       = std::chrono::milliseconds(2000);
    return cfg;
}

// 良构最小 BSON：空文档 {}（int32 长度 + 终止符）。
MongoDocument EmptyDoc() {
    return MongoDocument{{0x05, 0x00, 0x00, 0x00, 0x00}};
}

// 不良构 BSON：声明长度超出实际缓冲。
MongoDocument BadDoc() {
    return MongoDocument{{0x10, 0x00, 0x00, 0x00, 0x00}};
}

CallOptions Opt(int budget_ms = 30000) {
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

// 物理线程数（/proc/self/task）：Issue #12 M1 用它确定性观察 worker 组
// 收口，替代「猜时序」的 sleep 断言。
std::size_t ThreadCount() {
    std::size_t n = 0;
    DIR*        d = ::opendir("/proc/self/task");
    if (d == nullptr)
        return 0;
    while (const auto* e = ::readdir(d)) {
        const char* name = e->d_name;
        if (name[0] == '.' &&
            (name[1] == '\0' || (name[1] == '.' && name[2] == '\0')))
            continue;
        ++n;
    }
    ::closedir(d);
    return n;
}



// client 工厂：成功则返回托管对象。
result<std::shared_ptr<CoMongoCli>> NewClient(const MongoClientConfig& cfg,
                                            bool start = true) {
    auto c = CoMongoCli::Create(cfg);
    if (!c)
        return result<std::shared_ptr<CoMongoCli>>::err(
            std::move(c).error());
    auto cli = std::move(c).value();
    if (start) {
        auto st = cli->Start();
        if (!st)
            return result<std::shared_ptr<CoMongoCli>>::err(
                std::move(st).error());
    }
    return result<std::shared_ptr<CoMongoCli>>::ok(std::move(cli));
}

// 取 impl 观测钩子（测试只读计数，不触碰内部状态）。
std::shared_ptr<mongo_detail::CoMongoCliImpl> ImplOf(
    const std::shared_ptr<CoMongoCli>& cli) {
    return std::static_pointer_cast<mongo_detail::CoMongoCliImpl>(cli);
}

// 显式 Close 并断言「返回即物理落定」：Close 同步等待在途 driver 调用返回
// 与 worker 全退，返回时 IsClosed() 必须已为真。不再用「Close 后轮询
// IsClosed」的隐性两阶段替身掩盖同步承诺。
void CloseClientDrained(const std::shared_ptr<CoMongoCli>& cli) {
    cli->Close();
    BOOST_REQUIRE(cli->IsClosed());
}

std::atomic_bool g_prepared{false};

} // namespace

// 进程寿命运行时：无 Stop/restart；实例持有者被 coroutine 故意泄漏，
// 静态退出期不析构 Scheduler，测试侧不做任何 Stop 收尾。

BOOST_AUTO_TEST_SUITE(mongo_unit)

BOOST_AUTO_TEST_CASE(t_effective_uri_decoy_in_value) {
    // 参数值中含诱饵子串（connectTimeoutMS=/socketTimeoutMS）：真实 query
    // key 未出现，仍须按序注入全部缺省项，且诱饵值原样保留。
    const auto out = mongo_detail::EffectiveUri(MakeUriCfg(
        "mongodb://host/db?replicaSet=connectTimeoutMS=999&foo=socketTimeoutMS"));
    BOOST_CHECK_EQUAL(
        out,
        "mongodb://host/db?replicaSet=connectTimeoutMS=999&foo=socketTimeoutMS"
        "&serverselectiontimeoutms=15000&connecttimeoutms=2000"
        "&sockettimeoutms=2000&waitqueuetimeoutms=2000&maxpoolsize=2");
}

BOOST_AUTO_TEST_CASE(t_effective_uri_case_insensitive_existing) {
    // 已有同名 key（任意大小写）不重复追加；其余按既有 append 顺序以
    // '&' 追加；URI 原有大小写保留。
    const auto out = mongo_detail::EffectiveUri(MakeUriCfg(
        "mongodb://host/db?CONNECTTIMEOUTMS=4000&ServerSelectionTimeoutMS=9000"));
    BOOST_CHECK_EQUAL(
        out,
        "mongodb://host/db?CONNECTTIMEOUTMS=4000&ServerSelectionTimeoutMS=9000"
        "&sockettimeoutms=2000&waitqueuetimeoutms=2000&maxpoolsize=2");
}

BOOST_AUTO_TEST_CASE(t_effective_uri_no_query_uses_question) {
    // 无 query：首个注入项接 '?'，其余以 '&' 分隔。
    const auto out = mongo_detail::EffectiveUri(
        MakeUriCfg("mongodb://host:27017/bbt_ut"));
    BOOST_CHECK_EQUAL(
        out,
        "mongodb://host:27017/bbt_ut?serverselectiontimeoutms=15000"
        "&connecttimeoutms=2000&sockettimeoutms=2000"
        "&waitqueuetimeoutms=2000&maxpoolsize=2");
}

BOOST_AUTO_TEST_CASE(t_effective_uri_userinfo_decoy) {
    // userinfo 与参数值中的 "connectTimeoutMS=" 诱饵不得误判为真实 key：
    // 仍以 '?' 追加注入项，诱饵原样保留。
    const auto out = mongo_detail::EffectiveUri(MakeUriCfg(
        "mongodb://connectTimeoutMS=999:pw999@host/bb?t=connectTimeoutMS"));
    BOOST_CHECK_EQUAL(
        out,
        "mongodb://connectTimeoutMS=999:pw999@host/bb?t=connectTimeoutMS"
        "&serverselectiontimeoutms=15000&connecttimeoutms=2000"
        "&sockettimeoutms=2000&waitqueuetimeoutms=2000&maxpoolsize=2");
}

BOOST_AUTO_TEST_CASE(t_driver_invalid_uri_is_invalid_argument) {
    // Issue #12 M4：真实 driver 对非法 URI/选项抛 mongocxx::v1::exception
    // （mongoc 域，码 22 = MONGOC_ERROR_COMMAND_INVALID_ARG；mongoc 的
    // MONGOC_URI_ERROR 用它汇报无效 host specifier / 选项值非法）。经
    // ClassifyDriverError 必须稳定映射为调用方输入错误 InvalidArgument，
    // 而非 mongoc 通用 Unavailable。URI 解析离线完成，不需要服务端。
    const std::vector<std::string> rejected = {
        "mongodb://127.0.0.1:99999999/bbt_ut",                  // 端口越界
        "mongodb://127.0.0.1:abc/bbt_ut",                       // 端口非数字
        "mongodb://127.0.0.1:27017/bbt_ut?maxPoolSize=notanum", // 选项值非数字
    };
    for (const auto& uri : rejected) {
        bool threw = false;
        try {
            mongocxx::v1::uri parsed{bsoncxx::v1::stdx::string_view{uri}};
            (void)parsed;
        } catch (const mongocxx::v1::exception& e) {
            threw = true;
            const auto err =
                mongo_detail::ClassifyDriverError(e, "mongo: init driver pool");
            BOOST_CHECK_MESSAGE(err.code == ErrorCode::InvalidArgument,
                                "uri=" << uri << " code=" << (int)err.code);
            BOOST_CHECK_EQUAL(err.backend_category, "mongocxx");
        }
        BOOST_CHECK_MESSAGE(threw, "driver 未拒绝: " << uri);
    }

    // 对照：真实 driver 接受（未知选项仅警告、负值 / 畸形 IPv6 不报错）——
    // 不得盲目断言这些应被拒绝。
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

    // 脱敏：非法 URI 的错误文本不得泄露 userinfo 中的口令。
    try {
        mongocxx::v1::uri parsed{bsoncxx::v1::stdx::string_view{
            "mongodb://user:secretpw@127.0.0.1:27017/bbt_ut?maxPoolSize=bad"}};
        (void)parsed;
        BOOST_CHECK_MESSAGE(false, "driver 未拒绝口令诱饵 URI");
    } catch (const mongocxx::v1::exception& e) {
        const auto err =
            mongo_detail::ClassifyDriverError(e, "mongo: init driver pool");
        BOOST_CHECK(err.code == ErrorCode::InvalidArgument);
        BOOST_CHECK_MESSAGE(err.message.find("secretpw") == std::string::npos,
                            "错误文本泄露口令: " << err.message);
    }
}

BOOST_AUTO_TEST_CASE(t_create_before_scheduler) {
    auto c = CoMongoCli::Create(MakeDeadCfg());
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
    MongoClientConfig base = MakeDeadCfg();

    auto bad_uri = base;
    bad_uri.uri  = "http://127.0.0.1:1/";
    BOOST_CHECK(CoMongoCli::Create(bad_uri).error().code ==
                ErrorCode::InvalidArgument);
    auto empty_db = base;
    empty_db.database.clear();
    BOOST_CHECK(CoMongoCli::Create(empty_db).error().code ==
                ErrorCode::InvalidArgument);
    auto empty_coll = base;
    empty_coll.collection.clear();
    BOOST_CHECK(CoMongoCli::Create(empty_coll).error().code ==
                ErrorCode::InvalidArgument);
    auto zero_workers = base;
    zero_workers.worker_threads = 0;
    BOOST_CHECK(CoMongoCli::Create(zero_workers).error().code ==
                ErrorCode::InvalidArgument);
    auto over_workers = base;
    over_workers.worker_threads = kMongoLimitsMaxWorkerThreads + 1;
    BOOST_CHECK(CoMongoCli::Create(over_workers).error().code ==
                ErrorCode::InvalidArgument);
    auto zero_queue = base;
    zero_queue.max_queue = 0;
    BOOST_CHECK(CoMongoCli::Create(zero_queue).error().code ==
                ErrorCode::InvalidArgument);
    auto zero_sst = base;
    zero_sst.server_selection_timeout = std::chrono::milliseconds{0};
    BOOST_CHECK(CoMongoCli::Create(zero_sst).error().code ==
                ErrorCode::InvalidArgument);
    auto over_timeout = base;
    over_timeout.socket_timeout = kMongoLimitsMaxTimeout +
                                  std::chrono::milliseconds{1};
    BOOST_CHECK(CoMongoCli::Create(over_timeout).error().code ==
                ErrorCode::InvalidArgument);
}

BOOST_AUTO_TEST_CASE(t_command_prechecks) {
    auto c = NewClient(MakeDeadCfg(), /*start=*/false);
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    // 未 Start：协程内调用 → RuntimeUnavailable。
    std::optional<result<std::optional<MongoDocument>>> out;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { out.emplace(cli->FindOne(EmptyDoc(), Opt())); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::RuntimeUnavailable);

    // 启动后再校验参数形态：空文档/空 filter/空 update InvalidArgument。
    BOOST_REQUIRE(cli->Start());
    std::optional<result<void>> ins;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { ins.emplace(cli->InsertOne(MongoDocument{}, Opt())); }));
    BOOST_REQUIRE(!*ins);
    BOOST_CHECK(ins->error().code == ErrorCode::InvalidArgument);
    std::optional<result<std::optional<MongoDocument>>> f;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { f.emplace(cli->FindOne(MongoDocument{}, Opt())); }));
    BOOST_REQUIRE(!*f);
    BOOST_CHECK(f->error().code == ErrorCode::InvalidArgument);
    std::optional<result<MongoUpdateResult>> u;
    BOOST_REQUIRE(RunInCoroutine([&] {
        u.emplace(cli->UpdateOne(EmptyDoc(), MongoDocument{}, Opt()));
    }));
    BOOST_REQUIRE(!*u);
    BOOST_CHECK(u->error().code == ErrorCode::InvalidArgument);
    std::optional<result<std::uint64_t>> d;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { d.emplace(cli->DeleteOne(MongoDocument{}, Opt())); }));
    BOOST_REQUIRE(!*d);
    BOOST_CHECK(d->error().code == ErrorCode::InvalidArgument);

    CloseClientDrained(cli);

    // 已关闭：新命令 → Closed。
    BOOST_REQUIRE(RunInCoroutine(
        [&] { f.emplace(cli->FindOne(EmptyDoc(), Opt())); }));
    BOOST_REQUIRE(!*f);
    BOOST_CHECK(f->error().code == ErrorCode::Closed);
}

BOOST_AUTO_TEST_CASE(t_invalid_context_plain) {
    // 普通线程（非协程）调命令：不启动协程直接调用。
    auto c = NewClient(MakeDeadCfg());
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    auto res = cli->FindOne(EmptyDoc(), Opt());
    BOOST_REQUIRE(!res);
    BOOST_CHECK(res.error().code == ErrorCode::InvalidContext);
    cli->Close();
    BOOST_CHECK(cli->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_invalid_context_precedence_legacy) {
    // 旧 API 错误优先级回归：协程外调用一律 InvalidContext，不区分
    // 未 Start / 已关——旧 PreCheck 首行即查 g_bbt_tls_coroutine_co。
    // 未 Start：m_coll 为空也不能短路成 RuntimeUnavailable。
    auto c1 = CoMongoCli::Create(MakeDeadCfg());
    BOOST_REQUIRE(c1);
    auto not_started = std::move(c1).value();
    auto r1 = not_started->FindOne(EmptyDoc(), Opt());
    BOOST_REQUIRE(!r1);
    BOOST_CHECK(r1.error().code == ErrorCode::InvalidContext);
    not_started->Close();

    // 已关（Start 后 Close）：协程外仍 InvalidContext 而非 Closed
    // ——与前置校验「协程外优先」一致。
    auto c2 = NewClient(MakeDeadCfg());
    BOOST_REQUIRE(c2);
    auto closed = std::move(c2).value();
    closed->Close();
    BOOST_REQUIRE(closed->IsClosed());
    auto r2 = closed->FindOne(EmptyDoc(), Opt());
    BOOST_REQUIRE(!r2);
    BOOST_CHECK(r2.error().code == ErrorCode::InvalidContext);
}

BOOST_AUTO_TEST_CASE(t_invalid_bson) {
    auto c = NewClient(MakeDeadCfg());
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    // 不良构 BSON：校验在 driver 调用之前 → InvalidArgument（即便
    // 服务端不可达也不会走到 server selection）。
    std::optional<result<void>> ins;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { ins.emplace(cli->InsertOne(BadDoc(), Opt(5000))); }));
    BOOST_REQUIRE(!*ins);
    BOOST_CHECK(ins->error().code == ErrorCode::InvalidArgument);

    CloseClientDrained(cli);
}

BOOST_AUTO_TEST_CASE(t_server_unreachable) {
    // server selection 超时（2s）后 driver 失败 → Unavailable。
    auto c = NewClient(MakeDeadCfg(1, 4, 2000));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::optional<result<std::optional<MongoDocument>>> out;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { out.emplace(cli->FindOne(EmptyDoc(), Opt(20000))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Unavailable);
    BOOST_CHECK_EQUAL(out->error().backend_category, "mongocxx");

    CloseClientDrained(cli);
}

BOOST_AUTO_TEST_CASE(t_capacity_overloaded) {
    // worker_threads=1、max_queue=2：先提交的 op 在不可达地址的 server
    // selection 中占住唯一 worker，随后 2 个 op 占满队列，第 4 个确定性
    // Overloaded。同步点用真实队列状态（不 sleep）：提交前两个 op 后，
    // 接纳队列深度稳定为 1 ⇔ 恰有 1 个已被 worker 取出进入 driver、1 个
    // 仍在排队——两构建面均可用。
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
    // 真实状态同步：1 个被 worker 占用（phase==kRunning）、1 个排队，
    // 队列深度稳定为 1。
    BOOST_REQUIRE(WaitUntil(
        [&] { return impl->DriverCallsInFlightForTest() == 1 &&
                     impl->QueuedOpsForTest() == 1; }, 10000));
    submit(30000);
    submit(30000);

    // 恰好 1 个 Overloaded：1 在 driver 中 + 2 排队 = 容量占满，第 4 被拒。
    BOOST_REQUIRE(WaitUntil([&] { return overloaded.load() == 1; }, 10000));
    BOOST_CHECK_EQUAL(overloaded.load(), 1);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    // 运行中 driver 调用峰值不超 worker_threads（debug 观测）。
    BOOST_CHECK(impl->PeakDriverCallsForTest() <= 1);
#endif

    cli->Close();
    BOOST_REQUIRE(WaitUntil([&] { return done.load() == 4; }, 30000));
    BOOST_CHECK_EQUAL(overloaded.load(), 1);

    BOOST_REQUIRE(cli->IsClosed());
    BOOST_CHECK(cli->IsClosed());
    // 真实生产状态：worker 全退（Release 面同样成立）。
    BOOST_CHECK_EQUAL(impl->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(impl->RunningDriverCallsForTest(), 0);
#endif
}

BOOST_AUTO_TEST_CASE(t_deadline_only) {
    auto c = NewClient(MakeDeadCfg(1, 4, 10000));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    auto impl = ImplOf(cli);

    // deadline 500ms：out_ready 结果闩自同步——FindOne 返回即证明逻辑终态
    // 已交付（且该逻辑返回发生在其物理 driver 调用仍在进行时）。
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
    // Debug 面：先确认 op 已进入 server selection（running==1 确定性观测）。
    BOOST_REQUIRE(WaitUntil(
        [&] { return impl->RunningDriverCallsForTest() == 1; }, 10000));
#endif
    BOOST_REQUIRE(WaitUntil(
        [&] { return out_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TimedOut);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    // 逻辑返回后物理 driver 调用仍在进行——逻辑返回不冒充物理收口。
    BOOST_CHECK_EQUAL(impl->RunningDriverCallsForTest(), 1);
#endif

    // Close：首个逻辑终态（TimedOut）不被晚到的关闭覆盖；物理调用继续到
    // driver 超时，收口后 IsClosed 成立、worker 全退。
    cli->Close();
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TimedOut);
    BOOST_REQUIRE(cli->IsClosed());
    BOOST_CHECK_EQUAL(impl->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(impl->RunningDriverCallsForTest(), 0);
#endif
}

BOOST_AUTO_TEST_CASE(t_close_during_inflight) {
    // server selection 上界 15s：Close 发生在 driver 调用进行中——Close
    // 不得提前宣告 Closed；物理收口（driver 返回 + worker 全退）才 IsClosed。
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
    // 前置同步（两构建面一致）：等该 op 已被 worker 从队列取出、进入执行槽
    // （读既有 m_ops + phase==kRunning 真实状态，非新增观测计数），确保 Close
    // 确实发生在在途处理期间，而非 op 仍在队列时（不靠 sleep 猜时序）。
    BOOST_REQUIRE(WaitUntil(
        [&] { return impl->DriverCallsInFlightForTest() == 1; }, 10000));
#ifdef BBT_INFRA_STRINGENT_DEBUG
    // Debug 面追加：native driver 调用已在途（既有 RunningGuard 观测）。
    BOOST_REQUIRE(WaitUntil(
        [&] { return impl->RunningDriverCallsForTest() == 1; }, 10000));
#endif

    cli->Close();
    BOOST_REQUIRE(WaitUntil(
        [&] { return out_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
    // 同步契约：Close 返回即物理收口（在途 driver 调用返回 + worker 全退），
    // 因此返回时 IsClosed() 必须为真；不再断言「Close 提前返回、IsClosed
    // 仍为 false」的旧有界窗口行为。
    BOOST_REQUIRE(cli->IsClosed());
    BOOST_CHECK(cli->IsClosed());
    BOOST_CHECK_EQUAL(impl->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(impl->RunningDriverCallsForTest(), 0);
#endif
}

BOOST_AUTO_TEST_CASE(t_worker_threads_bound) {
    // 2 workers + 4 并发 op：运行中 driver 调用峰值 ≤ worker_threads。
    auto c = NewClient(MakeDeadCfg(2, 8, 8000));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    auto impl = ImplOf(cli);

    bbt::core::thread::CountDownLatch all{4};
    for (int i = 0; i < 4; ++i) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [&] {
                // 不可达地址：每个 op 在 server selection 中停留约 8s，
                // 足以让 2 个 worker 同时各持一个在途调用。
                auto r = cli->FindOne(EmptyDoc(), Opt(20000));
                (void)r;
                all.Down();
            },
            succ);
        BOOST_REQUIRE(succ);
    }
    // 真实队列状态同步（Debug/Release 均可用）：2 worker + 4 在途 op ⇒
    // 2 个已进入 driver、2 个排队，接纳队列深度稳定为 2。
    BOOST_REQUIRE(WaitUntil(
        [&] { return impl->QueuedOpsForTest() == 2; }, 10000));
    BOOST_CHECK(all.WaitTimeout(30000) == 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    // debug 观测：运行中 driver 调用峰值 ≤ worker_threads。
    BOOST_CHECK(impl->RunningDriverCallsForTest() <= 2);
    BOOST_CHECK(impl->PeakDriverCallsForTest() <= 2);
#endif

    CloseClientDrained(cli);
}

BOOST_AUTO_TEST_CASE(t_client_start_close_publish_pairing) {
    // M2 回归：旧公开 client 的 Start 发布与 Close 封口必须配对。Start 与
    // Close 并发时不得出现「Close 已返回、Start 事后发布 worker/lease、
    // IsClosed 回退」；Close 返回当刻无本 wrapper 资源，关闭后 Start 一律
    // Closed、重复 Close 幂等。（带 spawn 握手与独立放行方的确定性版本见
    // scratch deliver-infra/mongo-public/mongo-public-start-close-probe.cc。）
    for (int i = 0; i < 200; ++i) {
        auto c = CoMongoCli::Create(MakeDeadCfg(1, 2, 3000));
        BOOST_REQUIRE(c);
        auto cli  = std::move(c).value();
        auto impl = ImplOf(cli);

        std::atomic_int  start_code{-2};   // -1 = Start 成功
        std::atomic_bool start_returned{false};
        std::atomic_bool close_returned{false};
        std::thread starter([&] {
            auto r = cli->Start();
            start_code.store(r ? -1 : static_cast<int>(r.error().code),
                             std::memory_order_relaxed);
            start_returned.store(true, std::memory_order_release);
        });
        std::thread closer([&] {
            cli->Close();
            close_returned.store(true, std::memory_order_release);
        });
        starter.join();
        closer.join();
        BOOST_REQUIRE(start_returned.load() && close_returned.load());

        // Close 返回后：终态不回退、本 wrapper 无 worker 残留。
        BOOST_CHECK(cli->IsClosed());
        BOOST_CHECK_EQUAL(impl->LiveWorkersForTest(), 0);
        // 关闭后 Start 一律 Closed，不重开、不产生新资源。
        auto again = cli->Start();
        BOOST_REQUIRE(!again);
        BOOST_CHECK(again.error().code == ErrorCode::Closed);
        BOOST_CHECK(cli->IsClosed());
        BOOST_CHECK_EQUAL(impl->LiveWorkersForTest(), 0);
        // 并发 Start 只允许两种合法结局：成功（随即被 Close 同步收口）或
        // 被封口明确拒绝为 Closed；不得有第三种「事后发布」。
        const int code = start_code.load(std::memory_order_relaxed);
        BOOST_CHECK(code == -1 || code == static_cast<int>(ErrorCode::Closed));
        // 重复 Close 幂等。
        cli->Close();
        BOOST_CHECK(cli->IsClosed());
        BOOST_CHECK_EQUAL(impl->LiveWorkersForTest(), 0);
    }
}

BOOST_AUTO_TEST_CASE(t_worker_spawn_failure_finalizes) {
    // Issue #12 M1 回归：worker 组启动失败（reserve 抛 bad_alloc 或第 N 个
    // worker 创建抛 system_error）必须与 Close/析构共用同一 finalizer 完整
    // 收口——已起 worker 全部退出并 join、m_io_dead 置位、本 owner 的 pool
    // lease 归还、终态 Closed 发布；不得留下「线程已起但未收口」或半关闭态。
    // 经接缝确定性注入，失败发生在任何 driver 调用之前，不需要真实 MongoDB。
    BOOST_REQUIRE(g_prepared.load());

    // 预热并保活一个同 URI 的 pool lease：进程级 pool 与驱动后台线程就位
    // 后，后续线程计数变化只反映本用例的 worker 组，不把 pool 后台线程
    // 误计为 worker 泄漏。
    auto warm_info = mongo_detail::NewObjectInfo("test.rt.warm");
    BOOST_REQUIRE(warm_info);
    auto warm = std::make_shared<mongo_detail::MongoRuntime>(
        MakeUriCfg("mongodb://127.0.0.1:1/bbt_ut"), warm_info.value());
    BOOST_REQUIRE(warm->Start());
    const std::size_t base_threads = ThreadCount();
    BOOST_REQUIRE(base_threads > 1);

    const std::size_t fail_points[] = {
        mongo_detail::MongoRuntime::kSpawnFailReserve, 0, 1, 3};
    for (const auto at : fail_points) {
        auto info = mongo_detail::NewObjectInfo("test.rt.spawn_fail");
        BOOST_REQUIRE(info);
        auto cfg           = MakeUriCfg("mongodb://127.0.0.1:1/bbt_ut");
        cfg.worker_threads = 4;
        auto rt = std::make_shared<mongo_detail::MongoRuntime>(cfg,
                                                               info.value());
        rt->SetWorkerSpawnFailForTest(at);

        auto st = rt->Start();
        BOOST_REQUIRE_MESSAGE(!st, "spawn fail point " << at << " must fail");
        BOOST_CHECK(st.error().code == ErrorCode::InternalError);
        // 未成功发布资源：不进 Running。
        BOOST_CHECK(!rt->IsRunning());
        // 已起 worker 全部退出（物理线程回基线），无 worker 线程泄漏。
        BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
        BOOST_CHECK(WaitUntil([&] { return ThreadCount() <= base_threads; },
                              5000));
        // 同一 finalizer 已收口：io 域置死、终态 Closed。
        BOOST_CHECK(rt->IoDeadForTest());
        BOOST_CHECK(rt->IsClosed());
        // 收口幂等：重复 Close 不重复 join/reset，终态不回退。
        rt->Close();
        BOOST_CHECK(rt->IsClosed());
        BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
        // 失败即终态，不可复活：再次 Start 明确拒绝。
        auto again = rt->Start();
        BOOST_REQUIRE(!again);
        BOOST_CHECK(again.error().code == ErrorCode::Closed);
    }

    // 释放预热 lease：同一 finalizer 收口，线程回基线。
    warm->Close();
    BOOST_CHECK(warm->IsClosed());
    BOOST_CHECK(WaitUntil([&] { return ThreadCount() <= base_threads; }, 5000));
}

BOOST_AUTO_TEST_SUITE_END()

// ==================== Issue #40：owner/句柄归属 ====================
//
// 资源归属不变式（确定性探针，不靠压测）：
//   - 同一 owner 的多个集合句柄共享同一 worker 组：句柄数增长时
//     LiveWorkers 不变；
//   - 不同 owner 各自持有 worker 组/队列/ops 表：互不可见；
//   - max_queue=1 时跨集合共享背压：一个集合占住队列后，另一集合
//     确定性 Overloaded；
//   - 句柄关闭只停自身接纳，不动兄弟与 owner；owner Close() 在有界窗口
//     内等物理收口（ops 清空 + worker 全退），不提前宣告 IsClosed。

BOOST_AUTO_TEST_SUITE(mongo_owner)

namespace {

mongo::MongoRuntimeConfig MakeRtCfg(std::size_t workers = 2,
                                    std::size_t queue   = 8,
                                    int         sst_ms  = 15000) {
    mongo::MongoRuntimeConfig cfg;
    cfg.uri                      = "mongodb://127.0.0.1:1/bbt_ut";
    cfg.worker_threads           = workers;
    cfg.max_queue                = queue;
    cfg.server_selection_timeout = std::chrono::milliseconds(sst_ms);
    cfg.connect_timeout          = std::chrono::milliseconds(2000);
    cfg.socket_timeout           = std::chrono::milliseconds(2000);
    cfg.wait_queue_timeout       = std::chrono::milliseconds(2000);
    return cfg;
}

std::shared_ptr<mongo::CoMongoDb> NewOwner(
    const mongo::MongoRuntimeConfig& cfg) {
    auto d = mongo::CoMongoDb::Create(cfg);
    BOOST_REQUIRE(d);
    auto db = std::move(d).value();
    BOOST_REQUIRE(db->Start());
    return db;
}

// owner 对象 → 内部 runtime（观测 worker/计数）。CoMongoDbImpl 定义在
// MongoRuntime.cc 匿名命名空间，测试经 GetObjectInfo 拿不到 runtime；
// 这里用 Collection 的 impl 反查 owner runtime。
std::shared_ptr<mongo_detail::MongoRuntime> RuntimeOf(
    const std::shared_ptr<mongo::CoMongoDb>& db) {
    auto h = db->Collection({"bbt_ut", "probe"});
    BOOST_REQUIRE(h);
    auto impl = std::static_pointer_cast<mongo_detail::MongoCollImpl>(
        h.value());
    return impl->Runtime();
}

// owner Close + 有界等待物理收口，并断言 worker 全退（真实生产状态，
// Release 面同样成立）。Debug 面追加在途 driver 调用计数归零。
void CloseOwnerAndDrain(const std::shared_ptr<mongo::CoMongoDb>& db,
                        const std::shared_ptr<mongo_detail::MongoRuntime>& rt) {
    db->Close();
    BOOST_REQUIRE(db->IsClosed());
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(rt->RunningDriverCallsForTest(), 0);
#endif
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
}

} // namespace

BOOST_AUTO_TEST_CASE(t_owner_config_validation) {
    mongo::MongoRuntimeConfig base = MakeRtCfg();
    auto bad_uri = base;  bad_uri.uri = "http://127.0.0.1:1/";
    BOOST_CHECK(mongo::CoMongoDb::Create(bad_uri).error().code ==
                ErrorCode::InvalidArgument);
    auto zero_workers = base;  zero_workers.worker_threads = 0;
    BOOST_CHECK(mongo::CoMongoDb::Create(zero_workers).error().code ==
                ErrorCode::InvalidArgument);
    auto zero_queue = base;  zero_queue.max_queue = 0;
    BOOST_CHECK(mongo::CoMongoDb::Create(zero_queue).error().code ==
                ErrorCode::InvalidArgument);
    auto bad_target = mongo::MongoTarget{"", "c"};
    auto db = NewOwner(MakeRtCfg(1, 4));
    BOOST_CHECK(db->Collection(bad_target).error().code ==
                ErrorCode::InvalidArgument);
    auto rt = RuntimeOf(db);
    CloseOwnerAndDrain(db, rt);
}

BOOST_AUTO_TEST_CASE(t_owner_multi_handles_share_workers) {
    // 同一 owner 建 3 个集合句柄：worker 数恒等于配置值（2），
    // 不随句柄数增长。
    auto db = NewOwner(MakeRtCfg(2, 8, 8000));
    auto rt = RuntimeOf(db);
    BOOST_REQUIRE(WaitUntil(
        [&] { return rt->LiveWorkersForTest() == 2; }, 10000));

    std::vector<std::shared_ptr<mongo::CoMongoColl>> handles;
    for (const char* coll : {"c1", "c2", "c3"}) {
        auto h = db->Collection({"bbt_ut", coll});
        BOOST_REQUIRE(h);
        handles.push_back(std::move(h).value());
    }
    // 句柄建立后 worker 数仍 = 2。
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 2);

    // 三个集合各发一个在途 op（不可达地址 server selection 占住），
    // worker 峰值仍 ≤ 2——集合数不放大执行资源。
    std::atomic_int submitted{0};
    for (auto& h : handles) {
        bool succ = false;
        auto* hp = h.get();
        g_scheduler->RegistCoroutineTask(
            [hp, &submitted] {
                auto r = hp->FindOne(EmptyDoc(), Opt(30000));
                (void)r;
                submitted.fetch_add(1);
            },
            succ);
        BOOST_REQUIRE(succ);
    }
    // 真实状态同步：3 在途 op、2 worker ⇒ 2 个被 worker 占用（phase==kRunning）、
    // 1 个排队，队列深度稳定为 1（Debug/Release 均可用，读既有 state）。
    BOOST_REQUIRE(WaitUntil(
        [&] { return rt->DriverCallsInFlightForTest() == 2 &&
                     rt->QueuedOpsForTest() == 1; }, 10000));
#ifdef BBT_INFRA_STRINGENT_DEBUG
    // debug 观测：无论集合句柄多少，运行中 driver 调用峰值 ≤ worker_threads。
    BOOST_CHECK(rt->PeakDriverCallsForTest() <= 2);
#endif
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 2);

    for (auto& h : handles)
        h->Close();
    db->Close();
    BOOST_REQUIRE(WaitUntil(
        [&] { return submitted.load() == 3; }, 30000));
    BOOST_REQUIRE(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(rt->RunningDriverCallsForTest(), 0);
#endif
}

BOOST_AUTO_TEST_CASE(t_owner_thread_identity) {
    // 单 worker 依次服务两个不同集合；不可达地址让调用进入同步 driver
    // 路径，但不依赖真实 Mongo。每次调用收口后读取最后一项内部线程证据，
    // 直接断言目标集合及 worker、acquire、use、release 线程均正确。
    auto db = NewOwner(MakeRtCfg(1, 4, 1000));
    auto rt = RuntimeOf(db);
    auto h1 = db->Collection({"bbt_ut", "identity_a"});
    auto h2 = db->Collection({"bbt_ut", "identity_b"});
    BOOST_REQUIRE(h1 && h2);
    BOOST_REQUIRE(WaitUntil(
        [&] { return rt->LiveWorkersForTest() == 1; }, 10000));

    auto check_call = [&](const std::shared_ptr<mongo::CoMongoColl>& handle,
                          const char* collection) {
        std::optional<result<std::optional<MongoDocument>>> out;
        BOOST_REQUIRE(RunInCoroutine(
            [&] { out.emplace(handle->FindOne(EmptyDoc(), Opt(5000))); }));
        BOOST_REQUIRE(out);
        BOOST_REQUIRE(!*out);
        // 真实结果：调用确实经该 owner 的 worker 到达 driver 并以
        // Unavailable 失败（两构建面均成立）。
        BOOST_CHECK(out->error().code == ErrorCode::Unavailable);

#ifdef BBT_INFRA_STRINGENT_DEBUG
        // Debug 面：读取内部执行域取证，断言目标集合与 worker/acquire/
        // use/release 线程 identity 均相同（同一次调用同一 worker）。
        const auto evidence = rt->LastDriverThreadEvidenceForTest();
        BOOST_REQUIRE(evidence);
        BOOST_CHECK(evidence->worker_thread != std::thread::id{});
        BOOST_CHECK(evidence->worker_thread == evidence->acquire_thread);
        BOOST_CHECK(evidence->worker_thread == evidence->use_thread);
        BOOST_CHECK(evidence->worker_thread == evidence->release_thread);
        BOOST_CHECK_EQUAL(evidence->database, "bbt_ut");
        BOOST_CHECK_EQUAL(evidence->collection, collection);
#else
        // Release 面：执行域取证属 debug 观测层，本构建下不存在。
        (void)collection;
#endif
    };

    check_call(h1.value(), "identity_a");
    check_call(h2.value(), "identity_b");
    CloseOwnerAndDrain(db, rt);
}

BOOST_AUTO_TEST_CASE(t_owner_isolation) {
    // 两个 owner 各自 1 worker：资源隔离可验证。
    auto db1 = NewOwner(MakeRtCfg(1, 4, 8000));
    auto db2 = NewOwner(MakeRtCfg(1, 4, 8000));
    auto rt1 = RuntimeOf(db1);
    auto rt2 = RuntimeOf(db2);
    BOOST_REQUIRE(WaitUntil(
        [&] { return rt1->LiveWorkersForTest() == 1; }, 10000));
    BOOST_REQUIRE(WaitUntil(
        [&] { return rt2->LiveWorkersForTest() == 1; }, 10000));
    BOOST_CHECK(rt1 != rt2);

    auto h1 = db1->Collection({"bbt_ut", "a"});
    auto h2 = db2->Collection({"bbt_ut", "a"});
    BOOST_REQUIRE(h1 && h2);
    BOOST_CHECK(std::static_pointer_cast<mongo_detail::MongoCollImpl>(
                    h1.value())->Runtime() == rt1);
    BOOST_CHECK(std::static_pointer_cast<mongo_detail::MongoCollImpl>(
                    h2.value())->Runtime() == rt2);

    CloseOwnerAndDrain(db1, rt1);
    CloseOwnerAndDrain(db2, rt2);
}

// 跨集合共享背压：owner worker=1、max_queue=1 下集合 A 占住唯一 worker、集合 B
// 占满队列、集合 C 确定性 Overloaded——跨集合共享同一 owner 背压而非句柄各自上限。
// 前置同步用真实状态 DriverCallsInFlightForTest（A 的 op 已离开队列被 worker 接管），
// 两构建面一致；Release 面同样运行本用例（不再整体 ifdef 删除行为覆盖）。
BOOST_AUTO_TEST_CASE(t_shared_backpressure_queue_one) {
    // owner worker=1、max_queue=1：集合 A 占住唯一 worker 后，集合 B
    // 的 op 进队列占满容量，集合 C 的 op 确定性 Overloaded——跨集合
    // 共享背压而非句柄各自上限。
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
    auto submit = [&](mongo::CoMongoColl* h,
                      std::atomic_int& overloaded,
                      std::atomic_int& done, int budget_ms) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [h, budget_ms, &overloaded, &done] {
                auto r = h->FindOne(EmptyDoc(), Opt(budget_ms));
                if (!r && r.error().code == ErrorCode::Overloaded)
                    overloaded.fetch_add(1);
                done.fetch_add(1);
            },
            succ);
        BOOST_REQUIRE(succ);
    };

    submit(pa, b_overloaded, b_done, 30000);  // 占住唯一 worker（server selection 8s）
    // 等 A 的 op 已被 worker 从队列取出、进入执行槽（真实状态 phase==kRunning，
    // 读既有 m_ops），确保后续 B 进入队列时容量被 A 占住（两构建面一致）。
    BOOST_REQUIRE(WaitUntil(
        [&] { return rt->DriverCallsInFlightForTest() == 1 &&
                     rt->QueuedOpsForTest() == 0; }, 10000));
    submit(pb, b_overloaded, b_done, 30000);  // B：进队列占满容量 1
    // 可观察地确认 B 已在队列（而非仅「某协程跑过」）：QueuedOpsForTest
    // 读 m_queue_mtx 保护的 m_queue.size()，与接纳判定同临界区。
    BOOST_REQUIRE(WaitUntil(
        [&] { return rt->QueuedOpsForTest() == 1; }, 10000));
    submit(pc, c_overloaded, c_done, 30000);  // C：确定性 Overloaded

    // 队列容量=1 已被 B 占住：被拒的是 C 而不是 B。
    BOOST_REQUIRE(WaitUntil([&] { return c_done.load() == 1; }, 10000));
    BOOST_CHECK_EQUAL(c_overloaded.load(), 1);
    BOOST_CHECK_EQUAL(b_overloaded.load(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK(rt->PeakDriverCallsForTest() <= 1);
#endif

    ha.value()->Close();
    hb.value()->Close();
    hc.value()->Close();
    db->Close();
    BOOST_REQUIRE(WaitUntil(
        [&] { return b_done.load() + c_done.load() == 3; }, 30000));
    BOOST_REQUIRE(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(rt->RunningDriverCallsForTest(), 0);
#endif
}

BOOST_AUTO_TEST_CASE(t_handle_close_scoped) {
    // 句柄关闭：自身新命令 Closed，兄弟句柄与 owner 不受影响；
    // 在途 op 保活到物理收口。
    auto db = NewOwner(MakeRtCfg(1, 4, 8000));
    auto rt = RuntimeOf(db);
    auto h1 = db->Collection({"bbt_ut", "a"});
    auto h2 = db->Collection({"bbt_ut", "b"});
    BOOST_REQUIRE(h1 && h2);
    auto c1 = h1.value();
    auto c2 = h2.value();

    // owner 已 Running（NewOwner 内 Start）；句柄创建后经命令路径校验，
    // 不良构 BSON 在 worker 内、driver 调用前被拒 → InvalidArgument；
    // 证明 owner 在正常工作。
    std::optional<result<std::optional<MongoDocument>>> noop;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { noop.emplace(c2->FindOne(BadDoc(), Opt(2000))); }));
    BOOST_REQUIRE(!*noop);
    BOOST_CHECK(noop->error().code == ErrorCode::InvalidArgument);

    // h1 在途 op（server selection 中），随后关闭 h1：逻辑结果可以是
    // Closed/TimedOut/Unavailable（首个落定者胜出），但不能悬挂。
    std::atomic_bool h1_done{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            auto r = c1->FindOne(EmptyDoc(), Opt(30000));
            (void)r;
            h1_done.store(true);
        },
        succ);
    BOOST_REQUIRE(succ);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_REQUIRE(WaitUntil(
        [&] { return rt->RunningDriverCallsForTest() == 1; }, 10000));
#endif
    // Release 面：无 running 计数；本用例不断言 h1 的具体结果（仅等其落定），
    // 句柄关闭不取消在途 op，收口断言与 op 是否已进入 driver 无关（逻辑终态
    // Closed/TimedOut/Unavailable 均可，用例不作区分），故不设前置同步。

    c1->Close();
    BOOST_CHECK(c1->IsClosed());
    // 已关句柄的新命令 → Closed（不接触 owner 队列）。
    std::optional<result<std::optional<MongoDocument>>> out;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { out.emplace(c1->FindOne(EmptyDoc(), Opt())); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);

    // 兄弟句柄与 owner 仍可用：h2 命令进入同一 worker（排队等 h1 的
    // 在途收口后正常执行）。
    std::atomic_bool h2_done{false};
    g_scheduler->RegistCoroutineTask(
        [&] {
            auto r = c2->FindOne(EmptyDoc(), Opt(30000));
            (void)r;
            h2_done.store(true);
        },
        succ);
    BOOST_REQUIRE(succ);

    c2->Close();
    db->Close();
    BOOST_REQUIRE(WaitUntil([&] {
        return h1_done.load() && h2_done.load();
    }, 30000));
    BOOST_REQUIRE(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(rt->RunningDriverCallsForTest(), 0);
#endif
}

BOOST_AUTO_TEST_CASE(t_owner_collection_requires_running) {
    // 公共契约「owner 必须 Running 才接纳句柄」的生命周期门禁：
    //   Create 未 Start → Collection → RuntimeUnavailable；
    //   Start 之后     → Collection 成功；
    //   Close          → Collection → Closed。
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
    h.value()->Close();
    CloseOwnerAndDrain(db, rt);

    auto after_close = db->Collection({"bbt_ut", "y"});
    BOOST_REQUIRE(!after_close);
    BOOST_CHECK(after_close.error().code == ErrorCode::Closed);
}

// ==================== 关闭 finalizer 确定性回归 ====================
//
// 覆盖 r4 未闭合的「提前 Closed / finalize 并发」：
//   - Close 取关闭权但 driver 仍在途时不得提前 IsClosed；
//   - 两个线程并发 Close 都在同一真实终态（join + lease 归还）之后返回，
//     不重复 join/reset、不互相破坏；
//   - Start 后立即 Close（无在途）可收口且不再接纳；
//   - 同 URI 关闭其一不破坏兄弟 owner 仍持有的进程级 pool lease。

BOOST_AUTO_TEST_CASE(t_owner_close_not_premature_during_inflight) {
    auto db = NewOwner(MakeRtCfg(1, 8, 3000));
    auto rt = RuntimeOf(db);
    auto h  = db->Collection({"bbt_ut", "c"});
    BOOST_REQUIRE(h);
    auto* hp = h.value().get();

    // 提交 2 个在途 op（不可达地址长调用）：第一个为目标在途 driver 调用，
    // 第二个使 Release 面可用真实队列深度（稳定为 1）做派发同步。
    for (int i = 0; i < 2; ++i) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [hp] {
                auto r = hp->FindOne(EmptyDoc(), Opt(30000));
                (void)r;
            },
            succ);
        BOOST_REQUIRE(succ);
    }
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_REQUIRE(WaitUntil(
        [&] { return rt->RunningDriverCallsForTest() == 1; }, 10000));
#else
    // Release 面：真实状态同步——op 已被 worker 从队列取出、进入执行槽
    // （phase==kRunning），且队列深度稳定为 1（1 在 worker、1 排队）。
    // 读既有 state（m_ops/m_queue），不新增 Release 计数。
    BOOST_REQUIRE(WaitUntil(
        [&] { return rt->DriverCallsInFlightForTest() == 1 &&
                     rt->QueuedOpsForTest() == 1; }, 10000));
#endif

    std::atomic_bool closer_done{false};
    std::atomic_bool closed_at_return{false};
    std::thread closer([&] {
        db->Close();
        closed_at_return.store(db->IsClosed());
        closer_done.store(true);
    });
    // Close 已取关闭权（IsRunning 转假）但 driver 调用仍在途：此刻
    // IsClosed 必须仍为假——终态只在 join + lease 归还之后发布。
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

BOOST_AUTO_TEST_CASE(t_owner_concurrent_close_two_threads) {
    auto db = NewOwner(MakeRtCfg(2, 8, 2000));
    auto rt = RuntimeOf(db);
    auto h  = db->Collection({"bbt_ut", "c"});
    BOOST_REQUIRE(h);
    auto* hp = h.value().get();

    // 占住 worker（不可达地址 server selection 2s）。多投 1 个 op 使
    // Release 面可用真实队列深度做确定性同步。
    for (int i = 0; i < 3; ++i) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [hp] {
                auto r = hp->FindOne(EmptyDoc(), Opt(30000));
                (void)r;
            },
            succ);
        BOOST_REQUIRE(succ);
    }
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_REQUIRE(WaitUntil(
        [&] { return rt->RunningDriverCallsForTest() == 2; }, 10000));
#else
    // Release 面：真实状态同步——2 worker 均被占用（2 个 op phase==kRunning）。
    // 仅队列深度==1 无法区分「已完成 2 次取出」与「仅 1 个被取出」，故以 worker
    // 占用数为主判据，队列深度==1 为辅（第 3 个排队）。
    BOOST_REQUIRE(WaitUntil(
        [&] { return rt->DriverCallsInFlightForTest() == 2 &&
                     rt->QueuedOpsForTest() == 1; }, 10000));
#endif

    std::atomic_int returns{0};
    std::atomic_int closed_observed{0};
    auto close_once = [&] {
        db->Close();
        if (db->IsClosed())
            closed_observed.fetch_add(1);
        returns.fetch_add(1);
    };
    std::thread a(close_once);
    std::thread b(close_once);
    a.join();
    b.join();
    // 两个并发 Close 都返回，且都观察到同一真实终态（同一 finalizer 事实）。
    BOOST_CHECK_EQUAL(returns.load(), 2);
    BOOST_CHECK_EQUAL(closed_observed.load(), 2);
    BOOST_REQUIRE(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(rt->RunningDriverCallsForTest(), 0);
#endif
}

BOOST_AUTO_TEST_CASE(t_owner_start_then_immediate_close) {
    auto d = mongo::CoMongoDb::Create(MakeRtCfg(2, 4));
    BOOST_REQUIRE(d);
    auto db = std::move(d).value();
    BOOST_REQUIRE(db->Start());
    auto rt = RuntimeOf(db);   // 经句柄反查 runtime（须在 Close 前）
    db->Close();               // 无在途 op：直接收口
    BOOST_REQUIRE(db->IsClosed());
    BOOST_CHECK_EQUAL(rt->LiveWorkersForTest(), 0);
#ifdef BBT_INFRA_STRINGENT_DEBUG
    BOOST_CHECK_EQUAL(rt->RunningDriverCallsForTest(), 0);
#endif
    auto after_close = db->Collection({"bbt_ut", "x"});
    BOOST_REQUIRE(!after_close);
    BOOST_CHECK(after_close.error().code == ErrorCode::Closed);
}

BOOST_AUTO_TEST_CASE(t_owner_same_uri_sibling_lease_preserved) {
    // 同 URI 两 owner 共享进程级 pool lease：关闭其一不得归还/销毁兄弟
    // 仍持有的 lease（其他 owner 保持 Running 且 worker/pool 正常工作）。
    auto db1 = NewOwner(MakeRtCfg(1, 4, 2000));
    auto rt1 = RuntimeOf(db1);
    auto db2 = NewOwner(MakeRtCfg(1, 4, 2000));
    auto rt2 = RuntimeOf(db2);
    BOOST_REQUIRE(rt1 != rt2);

    CloseOwnerAndDrain(db1, rt1);

    BOOST_CHECK(!db2->IsClosed());
    auto h2 = db2->Collection({"bbt_ut", "sibling"});
    BOOST_REQUIRE(h2);
    // 兄弟 owner 的 worker/pool 路径仍可用：良构校验在 worker 内、driver
    // 之前确定性拒绝不良构 BSON → InvalidArgument（证明未触碰已归还 lease）。
    std::optional<result<std::optional<MongoDocument>>> out;
    BOOST_REQUIRE(RunInCoroutine(
        [&] { out.emplace(h2.value()->FindOne(BadDoc(), Opt(2000))); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::InvalidArgument);
    CloseOwnerAndDrain(db2, rt2);
}

BOOST_AUTO_TEST_SUITE_END()
