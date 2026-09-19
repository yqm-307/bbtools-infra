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

#include <sys/syscall.h>
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

void CloseAndWait(const std::shared_ptr<CoMongoCli>& cli) {
    cli->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(cli->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
}

std::atomic_bool g_prepared{false};

} // namespace

// ~Scheduler→Stop 命中 coroutine Hook 断言（上游基线竞态，测试侧不可
// 修复）；release 走漏单例所有权，进程退出不再触发 Stop 路径。
struct SuiteTeardown {
    ~SuiteTeardown() { g_scheduler.release(); }
};
BOOST_GLOBAL_FIXTURE(SuiteTeardown);

BOOST_AUTO_TEST_SUITE(mongo_live)

BOOST_AUTO_TEST_CASE(t_setup_scheduler) {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 4;
    cfg->m_cfg_stack_size        = 1024 * 256;
    g_scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(g_scheduler->IsRunning());
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

    CloseAndWait(cli);
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

    CloseAndWait(cli);
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

    CloseAndWait(cli);
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

    CloseAndWait(cli);
}

BOOST_AUTO_TEST_CASE(t_close_drains_inflight) {
    if (!LiveEnabled() || !PhaseIs('A')) {
        BOOST_TEST_MESSAGE("skip: 需要 BBT_TEST_MONGO_URI 且 PHASE=A");
        return;
    }
    auto cli = NewLiveClient(2);

    // 并发在途时 owner close：全部落定且 WaitClosed→Closed，
    // 逻辑结果只能是 ok/Closed 之一，不得悬挂。
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
    cli->RequestClose();
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

    CloseAndWait(cli);
}

BOOST_AUTO_TEST_SUITE_END()
