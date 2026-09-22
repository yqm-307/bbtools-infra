// Issue #7 单元验收（不依赖真实 MongoDB/容器）：
//
//   t_setup_scheduler             — 启动 Scheduler（共享 executor 来源）。
//   t_create_before_scheduler     — Scheduler 未 Start 时 Create →
//                                   RuntimeUnavailable（CompletionSignal/
//                                   对象身份需要运行时代际）。
//   t_config_validation           — 非法装配逐项 InvalidArgument。
//   t_command_prechecks           — 未 Start RuntimeUnavailable；空文档/
//                                   空 filter 空 update InvalidArgument；
//                                   close 后 Closed。
//   t_invalid_context_plain       — 普通线程直接调命令 → InvalidContext。
//   t_invalid_bson                — 不良构 BSON → InvalidArgument（校验在
//                                   worker 内、driver 调用之前，不需要服务端）。
//   t_server_unreachable          — server selection 超时 → Unavailable
//                                   （driver 侧失败映射）。
//   t_capacity_overloaded         — worker_threads=1 时一个 op 占住 worker
//                                   （RunningDriverCalls 观测），queue=2 占满
//                                   后第 4 个命令确定性 Overloaded。
//   t_deadline_and_cancel         — 不可达地址下 deadline → TimedOut、
//                                   cancel → Cancelled；逻辑返回时物理
//                                   driver 调用仍在进行（计数不变）。
//   t_close_during_inflight       — 在途 driver 调用随 owner close 落定
//                                   Closed；WaitClosed 等物理调用归零才
//                                   返回 Closed。
//   t_worker_threads_bound        — 4 个并发 op 峰值 driver 调用 ≤ 2。
//   t_waitclosed_contention       — 单等待位：并发第二个 AlreadyWaiting。
//
// 同步纪律与 redis/http 套件一致：CountDownLatch + WaitUntil 有界等待 +
// impl 运行计数观测，不 sleep 假设时序；不可达地址的 server selection
// 受 serverSelectionTimeoutMS 上界约束，用例预算均大于该上界。

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
#include <vector>


#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/CoMongoCli.hpp>

// impl 观测钩子（RunningDriverCallsForTest/PeakDriverCallsForTest）用于
// worker 并发上限与「逻辑返回≠物理收口」的确定性核验。
#include "mongo/CoMongoCliImpl.hpp"

using namespace bbt::infra;
using bbt::coroutine::Deadline;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

constexpr int kBudgetMs = 45000;

MongoClientConfig MakeUriCfg(std::string uri) {
    MongoClientConfig cfg;
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
    BOOST_REQUIRE(g_scheduler->IsRunning());
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

    cli->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);

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
    cli->RequestClose();
    BOOST_REQUIRE(RunInCoroutine([&] {
        return cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {});
    }));
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

    cli->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
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

    cli->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(20), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
}

BOOST_AUTO_TEST_CASE(t_capacity_overloaded) {
    // worker_threads=1、max_queue=2：先提交的 op 在不可达地址的 server
    // selection 中占住唯一 worker（RunningDriverCalls==1 为确定性证据），
    // 随后 2 个 op 占满队列，第 4 个确定性 Overloaded。
    auto c = NewClient(MakeDeadCfg(1, 2, 8000));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    auto impl = ImplOf(cli);

    std::atomic_int overloaded{0};
    std::atomic_int done{0};
    auto submit = [&](int budget_ms) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask(
            [&] {
                auto r = cli->FindOne(EmptyDoc(), Opt(budget_ms));
                if (!r && r.error().code == ErrorCode::Overloaded)
                    overloaded.fetch_add(1);
                done.fetch_add(1);
            },
            succ);
        BOOST_REQUIRE(succ);
    };

    submit(30000);
    BOOST_REQUIRE(WaitUntil(
        [&] { return impl->RunningDriverCallsForTest() == 1; }, 10000));
    submit(30000);
    submit(30000);
    submit(30000);

    // 恰好 1 个 Overloaded：1 在 driver 中 + 2 排队 = 容量占满，第 4 被拒。
    BOOST_REQUIRE(WaitUntil([&] { return overloaded.load() == 1; }, 10000));
    BOOST_CHECK_EQUAL(overloaded.load(), 1);
    // 运行中 driver 调用峰值不超 worker_threads。
    BOOST_CHECK(impl->PeakDriverCallsForTest() <= 1);

    cli->RequestClose();
    BOOST_REQUIRE(WaitUntil([&] { return done.load() == 4; }, 30000));
    BOOST_CHECK_EQUAL(overloaded.load(), 1);

    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(30), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
    BOOST_CHECK(cli->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_deadline_and_cancel) {
    auto c = NewClient(MakeDeadCfg(1, 4, 10000));
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();
    auto impl = ImplOf(cli);

    // deadline 500ms：先确认 op 已进入 server selection（running==1，
    // 确定性观测），再断言逻辑 TimedOut 返回且物理调用仍在进行——
    // 逻辑返回不冒充物理收口。
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
    BOOST_REQUIRE(WaitUntil(
        [&] { return impl->RunningDriverCallsForTest() == 1; }, 10000));
    BOOST_REQUIRE(WaitUntil(
        [&] { return out_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::TimedOut);
    BOOST_CHECK_EQUAL(impl->RunningDriverCallsForTest(), 1);

    // cancel：首个逻辑终态 Cancelled；物理调用继续到 driver 超时。
    bbt::coroutine::CancellationSource src;
    CallOptions opt = Opt(30000);
    opt.cancel      = src.Token();
    std::optional<result<std::optional<MongoDocument>>> out2;
    std::atomic_bool out2_ready{false};
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&] {
            out2.emplace(cli->FindOne(EmptyDoc(), opt));
            out2_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil(
        [&] { return impl->RunningDriverCallsForTest() >= 1; }, 10000));
    src.RequestCancel();
    BOOST_REQUIRE(WaitUntil(
        [&] { return out2_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(!*out2);
    BOOST_CHECK(out2->error().code == ErrorCode::Cancelled);

    cli->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(30), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
    // 首个逻辑终态不被晚到的 close 覆盖。
    BOOST_REQUIRE(!*out2);
    BOOST_CHECK(out2->error().code == ErrorCode::Cancelled);
    BOOST_CHECK_EQUAL(impl->RunningDriverCallsForTest(), 0);
}

BOOST_AUTO_TEST_CASE(t_close_during_inflight) {
    // server selection 上界 15s：close 发生在 driver 调用进行中——
    // WaitClosed 必须等物理调用返回才 Closed（不得提前假收口）。
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
    // 等 op 真正进入 driver 调用（确定性观测，不 sleep 猜时序）。
    BOOST_REQUIRE(WaitUntil(
        [&] { return impl->RunningDriverCallsForTest() == 1; }, 10000));

    cli->RequestClose();
    BOOST_REQUIRE(WaitUntil(
        [&] { return out_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(!*out);
    BOOST_CHECK(out->error().code == ErrorCode::Closed);
    // 物理调用尚未返回（15s server selection 上界内）→ 不得已 Closed。
    BOOST_CHECK(!cli->IsClosed());

    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(40), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
    BOOST_CHECK(cli->IsClosed());
    BOOST_CHECK_EQUAL(impl->RunningDriverCallsForTest(), 0);
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
    BOOST_REQUIRE(WaitUntil(
        [&] { return impl->RunningDriverCallsForTest() == 2; }, 10000));
    BOOST_CHECK(impl->RunningDriverCallsForTest() <= 2);
    BOOST_CHECK(all.WaitTimeout(30000) == 0);
    BOOST_CHECK(impl->PeakDriverCallsForTest() <= 2);

    cli->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(30), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
}

BOOST_AUTO_TEST_CASE(t_waitclosed_contention) {
    auto c = NewClient(MakeDeadCfg());
    BOOST_REQUIRE(c);
    auto cli = std::move(c).value();

    std::atomic_bool         a_waiting{false};
    std::atomic_bool         a_done{false};
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

BOOST_AUTO_TEST_SUITE_END()
