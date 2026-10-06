// Issue #38：HTTP 子对象物理关闭后从 NetworkRuntime 托管集合安全反登记。
//
// 关闭面按进程寿命运行时修订（契约 §1）：ICoCloseable 只有
// Close()/IsClosed()。已删除语义不保留旧断言：RequestClose/WaitClosed、
// CloseStatus、取消令牌、CompletionSignal 都不再使用；每个用例的 runtime
// 在用例内显式 Close() 并断言物理收口（IsClosed + 托管集合/transport
// 落定探针 + weak_ptr 失效），不用 Scheduler::Stop()。
//
// 验收映射（infra-review-issues-20260924/02-http-owner.md）：
//   t_children_released_while_runtime_alive
//       — Runtime 存活期间反复创建/关闭/释放有限数量 client 与 server；
//         关闭回调排空且外部引用退出后 weak_ptr 失效，不等 Runtime 析构。
//   t_close_midflight_unregisters_child
//       — 在途请求 + owner 同步 Close()：已发送请求立即拿终态；物理关闭
//         后从托管集合反登记（外部引用释放即 weak 失效）；已接纳 handler
//         不被抢占，其回复只消费不交付。
//   t_idle_client_released_still_managed
//       — 空闲活跃对象（无在途 op、未 Close）释放唯一外部引用后仍由
//         Runtime 强托管；隔离 ClientOp::owner 反向强引用，Runtime
//         teardown 落定后 weak_ptr 失效（client 与 server 双侧）。
//   t_runtime_close_with_early_released_children
//       — 子对象先关、Runtime 后关、外部引用提前释放的组合路径：
//         计数不丢、不双删、不 UAF。
//   t_repeat_and_factory_race_close
//       — 重复 Close、并发 Close、工厂与 Close 竞争：Close 幂等，
//         关闭中/后的工厂调用一律拒绝，已登记对象必被收口。
//   t_handler_captured_resource_released
//       — server handler 捕获资源以析构计数证明可释放，不靠 RSS 判断。
//   t_end_runtime_convergence
//       — 收尾：Runtime 显式 Close() + 托管集合/transport 物理落定。
//
// 同步纪律：跨线程/跨协程一律原子标志 + WaitUntil（有界预算）与
// CountDownLatch；handler 内等待用 bbtco_sleep 轮询真实事件标志。
// 所有等待都有超时上限，不无限挂起。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Hook.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

// 竞态证据所需的 impl 内测试接缝（SetAdoptCommitGateForTest）、托管集合
// 落定探针，与 tests/Test_mongo_unit.cc 消费 src/ 内 impl 头同一约定。
#include "http/NetworkRuntimeImpl.hpp"

using namespace bbt::infra;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

constexpr int kBudgetMs = 15000;

std::shared_ptr<NetworkRuntime> g_runtime;
std::shared_ptr<HttpServer>     g_server;
std::uint16_t                   g_port = 0;

// handler 捕获资源的析构计数：shared_ptr 进入 HttpHandler 闭包后，
// 仅当 server impl 真正销毁（m_handler 随之析构）才递减。
std::shared_ptr<std::atomic_int> g_captured;
std::atomic_bool                 g_hold_entered{false};
std::atomic_bool                 g_hold_release{false};
std::atomic_bool                 g_hold_exited{false};
std::atomic_int                  g_hold_deliveries{0};

NetworkLimits MakeLimits() {
    NetworkLimits limits{};
    limits.max_connections  = 64;
    limits.max_inflight     = 64;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = 64 * 1024;
    limits.incoming_timeout = std::chrono::milliseconds{5000};
    return limits;
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

// 协程内执行并限时等待；失败返回 false（测试不得无限挂住）。
template <class F>
bool RunInCoroutine(F&& f, int budget_ms = kBudgetMs) {
    bbt::core::thread::CountDownLatch done{1};
    bool succ = false;
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [fn = std::forward<F>(f), &done]() mutable {
            fn();
            done.Down();
        },
        succ);
    if (!succ)
        return false;
    return done.WaitTimeout(budget_ms) == 0;
}

result<HttpResponse> RoundTrip(const std::shared_ptr<HttpClient>& client,
                               const std::string& target) {
    std::optional<result<HttpResponse>> out;
    bool ran = RunInCoroutine([&] {
        CallOptions opt;
        opt.deadline = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(5000);
        HttpRequest req{"GET",
                        "http://127.0.0.1:" + std::to_string(g_port) + target,
                        {}, ""};
        out.emplace(client->Request(std::move(req), opt));
    });
    if (!ran)
        return result<HttpResponse>::err(
            MakeError(ErrorCode::InternalError, "test: coroutine did not run"));
    return std::move(*out);
}

result<HttpResponse> TestHandler(IncomingCallContext, HttpRequest req) {
    if (req.url == "/ok")
        return result<HttpResponse>::ok(HttpResponse{200, {}, "ok"});
    if (req.url == "/hold") {
        g_hold_entered.store(true);
        for (int i = 0; i < 8000 && !g_hold_release.load(); ++i)
            bbtco_sleep(2);
        g_hold_exited.store(true);
        g_hold_deliveries.fetch_add(1);
        return result<HttpResponse>::ok(HttpResponse{200, {}, "hold done"});
    }
    return result<HttpResponse>::ok(HttpResponse{404, {}, "not found"});
}

// owner 主动同步 Close()：幂等、任意线程，返回即物理释放（IsClosed）。
// 旧口径的 RequestClose + WaitClosed 已删除，不再有「同步等待关闭完成」
// 的独立入口。
bool CloseAndAssert(const std::shared_ptr<ICoCloseable>& obj) {
    obj->Close();
    return obj->IsClosed();
}

} // namespace

BOOST_AUTO_TEST_SUITE(http_child_unregister)

BOOST_AUTO_TEST_CASE(t_begin_start_runtime) {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 2;
    cfg->m_cfg_stack_size        = 1024 * 256;
    g_scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    // 进程寿命运行时：只有 IsInitialized，没有 Stop/代际。
    BOOST_REQUIRE(g_scheduler->IsInitialized());

    g_captured = std::make_shared<std::atomic_int>(0);
    struct Guard {
        std::shared_ptr<std::atomic_int> cnt;
        explicit Guard(std::shared_ptr<std::atomic_int> c)
            : cnt(std::move(c)) { cnt->fetch_add(1); }
        ~Guard() { cnt->fetch_sub(1); }
    };
    auto captured_guard = std::make_shared<Guard>(g_captured);

    auto rt = NetworkRuntime::Create(MakeLimits());
    BOOST_REQUIRE(rt);
    g_runtime = std::move(rt).value();
    BOOST_REQUIRE(g_runtime->Start());

    // server handler 捕获资源：析构计数证明捕获随 impl 释放。
    auto srv = g_runtime->ListenHttp(
        {"127.0.0.1", 0},
        [guard = std::move(captured_guard)](IncomingCallContext ctx,
                                            HttpRequest req) mutable {
            return TestHandler(std::move(ctx), std::move(req));
        });
    BOOST_REQUIRE(srv);
    g_server = std::move(srv).value();
    g_port   = g_server->LocalAddress().port;
    BOOST_REQUIRE_NE(g_port, 0);
    BOOST_CHECK_EQUAL(g_captured->load(), 1);
}

BOOST_AUTO_TEST_CASE(t_children_released_while_runtime_alive) {
    // Runtime 存活：循环创建 client→真实请求→Close→释放外部引用；
    // weak_ptr 必须在 Runtime 析构前失效。
    for (int i = 0; i < 4; ++i) {
        auto cl = g_runtime->CreateHttpClient();
        BOOST_REQUIRE(cl);
        auto client = std::move(cl).value();
        std::weak_ptr<HttpClient> weak = client;

        auto res = RoundTrip(client, "/ok");
        BOOST_REQUIRE(res);
        BOOST_CHECK_EQUAL(res.value().status, 200u);

        BOOST_CHECK(CloseAndAssert(client));
        client.reset();
        // 关闭回调排空 + 外部引用退出：反登记后 weak_ptr 失效。
        BOOST_REQUIRE(WaitUntil([&] { return weak.expired(); }));
    }

    // server 同样覆盖：ListenHttp→StopAccepting→Close→释放。
    for (int i = 0; i < 2; ++i) {
        auto srv = g_runtime->ListenHttp({"127.0.0.1", 0}, TestHandler);
        BOOST_REQUIRE(srv);
        auto server = std::move(srv).value();
        std::weak_ptr<HttpServer> weak = server;

        server->StopAccepting();
        BOOST_CHECK(CloseAndAssert(server));
        server.reset();
        BOOST_REQUIRE(WaitUntil([&] { return weak.expired(); }));
    }

    // Runtime 仍可用：反登记没有破坏工厂。
    auto cl = g_runtime->CreateHttpClient();
    BOOST_REQUIRE(cl);
    auto client = std::move(cl).value();
    auto res = RoundTrip(client, "/ok");
    BOOST_REQUIRE(res);
    BOOST_CHECK(CloseAndAssert(client));
    client.reset();
}

BOOST_AUTO_TEST_CASE(t_close_midflight_unregisters_child) {
    // 在途请求 + owner 主动同步 Close()：已发送请求立即拿终态；client
    // 物理关闭后从 Runtime 托管集合反登记（外部引用释放即 weak 失效，
    // 不等 Runtime 析构）；已接纳 handler 不被抢占，其后回复只消费不交付。
    auto cl = g_runtime->CreateHttpClient();
    BOOST_REQUIRE(cl);
    auto client = std::move(cl).value();
    std::weak_ptr<HttpClient> weak = client;

    std::optional<result<HttpResponse>> out;
    std::atomic_bool out_ready{false};
    // 协程按值捕获 client 的 shared_ptr 副本：与主线程的 client 变量不
    // 共享同一份 shared_ptr 变量，无并发读写；对象有效性由「在途期间
    // Runtime/op 必强托管」的被测契约保证。
    std::shared_ptr<HttpClient> in_flight = client;
    bool succ = false;
    g_scheduler->RegistCoroutineTask(
        [&out, &out_ready, in_flight] {
            CallOptions opt;
            opt.deadline = std::chrono::steady_clock::now() +
                           std::chrono::seconds(20);
            HttpRequest req{
                "GET",
                "http://127.0.0.1:" + std::to_string(g_port) + "/hold",
                {}, ""};
            out.emplace(in_flight->Request(std::move(req), opt));
            out_ready.store(true, std::memory_order_release);
        },
        succ);
    BOOST_REQUIRE(succ);
    BOOST_REQUIRE(WaitUntil([&] { return g_hold_entered.load(); }));

    // owner 同步 Close()：在途已发送请求立即以终态返回（返回即物理收口）。
    client->Close();
    BOOST_CHECK(client->IsClosed());
    BOOST_REQUIRE(
        WaitUntil([&] { return out_ready.load(std::memory_order_acquire); }));
    BOOST_REQUIRE(out.has_value());
    BOOST_REQUIRE(!out.value());
    // #64：服务端 handler 已进入 ⇒ 请求已完整写出 ⇒ owner Close 后是未知。
    BOOST_CHECK(out->error().code == ErrorCode::OutcomeUnknown);
    BOOST_REQUIRE(out->error().request_phase.has_value());
    BOOST_CHECK(*out->error().request_phase == RequestPhase::RequestCommitted);

    // 放行 handler：它不被抢占，照常退出并产出唯一一次终态；连接已中止，
    // 回复只被消费不再交付。
    g_hold_release.store(true);
    BOOST_REQUIRE(WaitUntil([&] { return g_hold_exited.load(); }));
    BOOST_CHECK_EQUAL(g_hold_deliveries.load(), 1);
    BOOST_CHECK(client->IsClosed());

    // 物理关闭落定后反登记：外部引用释放即 weak 失效（不等 Runtime 析构）。
    client.reset();
    in_flight.reset();
    BOOST_REQUIRE(WaitUntil([&] { return weak.expired(); }));
}

BOOST_AUTO_TEST_CASE(t_idle_client_released_still_managed) {
    // 空闲活跃 client（无在途 op、未 Close）释放唯一外部引用：
    // 必须由 Runtime m_children 强托管——若实现退化为不托管，weak 立即
    // 失效。隔离 ClientOp::owner 路径：本用例全程无 Request，不存在
    // op 对 impl 的反向强引用。
    auto rt = NetworkRuntime::Create(MakeLimits());
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());

    std::weak_ptr<HttpClient> weak;
    {
        auto cl = runtime->CreateHttpClient();
        BOOST_REQUIRE(cl);
        auto client = std::move(cl).value();
        weak = client;
        client.reset();   // 唯一外部引用释放；Runtime 仍为托管者
    }
    // 空闲对象不得提前释放：weak 仍有效，lock 出的 impl 未 Closed。
    BOOST_REQUIRE(!weak.expired());
    {
        auto held = weak.lock();
        BOOST_REQUIRE(held);
        BOOST_CHECK(!held->IsClosed());
    }

    // Runtime Close()：teardown 物理关闭空闲 child → MarkClosed → hook →
    // 反登记移除最后一个强引用，weak 随之失效。
    runtime->Close();
    BOOST_CHECK(runtime->IsClosed());
    BOOST_REQUIRE(WaitUntil([&] { return weak.expired(); }));

    // server 变体：ListenHttp 后立即释放外部引用（accept 已在跑但无
    // 连接），同样由 Runtime 托管到 teardown 落定。
    auto rt2 = NetworkRuntime::Create(MakeLimits());
    BOOST_REQUIRE(rt2);
    auto runtime2 = std::move(rt2).value();
    BOOST_REQUIRE(runtime2->Start());
    std::weak_ptr<HttpServer> weak_srv;
    {
        auto srv = runtime2->ListenHttp({"127.0.0.1", 0}, TestHandler);
        BOOST_REQUIRE(srv);
        auto server = std::move(srv).value();
        weak_srv = server;
        server.reset();
    }
    BOOST_REQUIRE(!weak_srv.expired());
    runtime2->Close();
    BOOST_CHECK(runtime2->IsClosed());
    BOOST_REQUIRE(WaitUntil([&] { return weak_srv.expired(); }));
}

BOOST_AUTO_TEST_CASE(t_runtime_close_with_early_released_children) {
    // 独立 runtime：子对象先 Close、外部引用提前释放，随后 Runtime
    // 再关；验证计数配对、不双删、不 UAF。
    auto rt = NetworkRuntime::Create(MakeLimits());
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());

    std::vector<std::weak_ptr<HttpClient>> clients;
    for (int i = 0; i < 3; ++i) {
        auto cl = runtime->CreateHttpClient();
        BOOST_REQUIRE(cl);
        auto client = std::move(cl).value();
        clients.push_back(client);
        client->Close();              // 子对象先关（同步物理收口）
        BOOST_CHECK(client->IsClosed());
        client.reset();               // 外部引用提前释放
    }
    // 重复 close 幂等：再次对已关闭对象发起不崩溃不双计。
    {
        auto cl = runtime->CreateHttpClient();
        BOOST_REQUIRE(cl);
        auto client = std::move(cl).value();
        client->Close();
        client->Close();
        client->Close();
        BOOST_CHECK(client->IsClosed());
        client.reset();
    }
    runtime->Close();
    BOOST_CHECK(runtime->IsClosed());
    for (auto& w : clients)
        BOOST_REQUIRE(WaitUntil([&] { return w.expired(); }));
}

BOOST_AUTO_TEST_CASE(t_repeat_and_factory_race_close) {
    // 并发 close：多线程同时 Close 同一对象，幂等且不丢计数。
    auto rt = NetworkRuntime::Create(MakeLimits());
    BOOST_REQUIRE(rt);
    auto runtime = std::move(rt).value();
    BOOST_REQUIRE(runtime->Start());
    auto cl = runtime->CreateHttpClient();
    BOOST_REQUIRE(cl);
    auto client = std::move(cl).value();
    std::weak_ptr<HttpClient> weak = client;

    std::vector<std::thread> closers;
    for (int i = 0; i < 4; ++i)
        closers.emplace_back([&] { client->Close(); });
    for (auto& t : closers)
        t.join();
    BOOST_CHECK(CloseAndAssert(client));
    client.reset();
    BOOST_REQUIRE(WaitUntil([&] { return weak.expired(); }));

    // 工厂与 close 真竞态：两个子段分别真实命中 adopt/rejected 分支，
    // 两侧胜出者都来自与 Close 同一并发窗口的竞态线程结果，
    // 不使用任何在屏障前预登记的对象充当成功样本。
    // 线性化点在 CheckAndAdoptLocked 的锁内复检（m_state/m_sealed/IsOpen）：
    //   adopt 成功 ⇒ 对象登记早于 Close 的 m_state 提交点 ⇒
    //     必在 teardown 快照中被收口：Close 后 IsClosed 成立、
    //     外部引用释放后 weak 失效；
    //   adopt 失败 ⇒ Closed 错误，不假成功、不产生孤儿。
    int saw_adopted  = 0;
    int saw_rejected = 0;

    // 子段 A：adopt 胜出必须来自与 Close 的真实重叠调用窗口，
    // 不靠 sleep/时序推断。确定性构造（不碰运气）：
    //   1) 经 impl 测试接缝 SetAdoptCommitGateForTest 把 factory 的
    //      CreateHttpClient 停在「CheckAndAdoptLocked 复检已过、
    //      登记提交未发生」的临界段内（此时仍持 m_lifecycle_mtx）；
    //   2) 主线程观察到 factory 停在调用内部后，才发起
    //      runtime2->Close()——factory 调用窗口 ⊃ Close
    //      调用窗口，重叠是构造保证的，不是时序猜测；
    //   3) 放行 gate：登记在 Close 已提交 BeginClose/
    //      m_state、io 域/关闭线程正等 m_lifecycle_mtx 时提交——
    //      sealed 尚未置位 ⇒ 登记先于 sealed ⇒ teardown 快照必含
    //      此对象 ⇒ Close 后必 IsClosed、外部引用释放后 weak
    //      失效。若实现丢这份通知，saw_adopted 断言失败。
    {
        auto rt2 = NetworkRuntime::Create(MakeLimits());
        BOOST_REQUIRE(rt2);
        auto runtime2 = std::move(rt2).value();
        BOOST_REQUIRE(runtime2->Start());
        auto impl2 =
            std::static_pointer_cast<http_detail::NetworkRuntimeImpl>(
                runtime2);

        std::atomic<bool> adopt_gate_entered{false};
        std::atomic<bool> release_adopt_gate{false};
        impl2->SetAdoptCommitGateForTest([&] {
            adopt_gate_entered.store(true, std::memory_order_release);
            while (!release_adopt_gate.load(std::memory_order_acquire))
                std::this_thread::yield();
        });

        std::optional<result<std::shared_ptr<HttpClient>>> res;
        std::weak_ptr<HttpClient>                        res_weak;
        std::thread factory([&] {
            res = runtime2->CreateHttpClient();   // 内部停在 adopt gate
            if (res && *res)
                res_weak = res->value();
        });

        // factory 调用确实在飞（停在 adopt 临界段内、持 m_lifecycle_mtx）。
        BOOST_REQUIRE(WaitUntil(
            [&] { return adopt_gate_entered.load(
                       std::memory_order_acquire); }));

        // 此刻发起 Close：closer 在紧贴调用点置 close_entered；
        // 观察到该标志后再放行 gate——factory 调用在 Close
        // 发起时仍在飞，登记提交也必然晚于 Close 发起点：
        // 两次调用真实重叠，不依赖任何 sleep/时序推断。
        // （不先 join closer：Close 会阻塞在 factory 所持的
        //   m_lifecycle_mtx 上，先 join 将死锁。）
        std::atomic<bool> close_entered{false};
        std::thread closer([&] {
            close_entered.store(true, std::memory_order_release);
            runtime2->Close();
        });
        BOOST_REQUIRE(WaitUntil(
            [&] { return close_entered.load(
                       std::memory_order_acquire); }));
        release_adopt_gate.store(true, std::memory_order_release);
        closer.join();
        factory.join();

        BOOST_CHECK(runtime2->IsClosed());

        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE(*res);   // adopt gate 放行 ⇒ 必为成功结果
        ++saw_adopted;
        auto adopted_client = std::move(res->value());
        BOOST_CHECK(adopted_client->IsClosed());
        adopted_client.reset();
        BOOST_REQUIRE(WaitUntil([&] { return res_weak.expired(); }));
    }

    // 子段 B：rejected 分支——sealed 后的 factory 一律拒绝。
    // closer 与 factory 同屏障零延迟起跑，Close 的 CAS 先提交，
    // 竞态 factory 的 CheckAndAdoptLocked 复检必然观察到
    // m_state != kRunning 或 !IsOpen()——返回 Closed 不假成功。
    {
        constexpr int kRejectRounds = 24;
        constexpr int kFactories    = 4;
        for (int round = 0; round < kRejectRounds; ++round) {
            auto rt2 = NetworkRuntime::Create(MakeLimits());
            BOOST_REQUIRE(rt2);
            auto runtime2 = std::move(rt2).value();
            BOOST_REQUIRE(runtime2->Start());

            std::atomic<int>  ready{0};
            std::atomic<bool> go{false};
            std::vector<std::optional<
                result<std::shared_ptr<HttpClient>>>> results(kFactories);
            std::vector<std::weak_ptr<HttpClient>>    weaks(kFactories);
            std::vector<std::thread>                  workers;
            workers.reserve(kFactories + 1);
            for (int i = 0; i < kFactories; ++i) {
                workers.emplace_back([&, i] {
                    ready.fetch_add(1, std::memory_order_release);
                    while (!go.load(std::memory_order_acquire))
                        std::this_thread::yield();
                    results[i] = runtime2->CreateHttpClient();
                    if (results[i] && *results[i])
                        weaks[i] = results[i]->value();
                });
            }
            // closer 与 factory 同一起跑线，无受控延迟：谁的线性化点
            // 先由真实调度决定；多数轮次 closer 先提交，factory 全拒。
            workers.emplace_back([&] {
                ready.fetch_add(1, std::memory_order_release);
                while (!go.load(std::memory_order_acquire))
                    std::this_thread::yield();
                runtime2->Close();
            });
            BOOST_REQUIRE(WaitUntil([&] {
                return ready.load(std::memory_order_acquire) ==
                       kFactories + 1;
            }));
            go.store(true, std::memory_order_release);
            for (auto& t : workers)
                t.join();

            runtime2->Close();   // 幂等；覆盖 closer 未取胜的轮次
            BOOST_CHECK(runtime2->IsClosed());

            for (int i = 0; i < kFactories; ++i) {
                BOOST_REQUIRE(results[i].has_value());
                if (*results[i]) {
                    // 无延迟竞态下偶发 adopt 胜出：同样必须被收口。
                    ++saw_adopted;
                    auto adopted_client =
                        std::move(results[i]->value());
                    BOOST_CHECK(adopted_client->IsClosed());
                    adopted_client.reset();
                    BOOST_REQUIRE(WaitUntil(
                        [&] { return weaks[i].expired(); }));
                } else {
                    ++saw_rejected;
                    BOOST_CHECK((*results[i]).error().code ==
                                ErrorCode::Closed);
                }
            }
        }
    }

    // 两个分支都必须真实命中于竞态结果槽：任一侧为零则本用例失败，
    // 不以预登记对象或计数造假充数。
    BOOST_TEST_MESSAGE("t_repeat_and_factory_race_close: saw_adopted="
                       << saw_adopted << " saw_rejected=" << saw_rejected);
    BOOST_CHECK_GT(saw_adopted, 0);
    BOOST_CHECK_GT(saw_rejected, 0);

    // 关闭后拒绝：sealed 后工厂一律 Closed 拒绝，不假成功。
    runtime->Close();
    BOOST_CHECK(runtime->IsClosed());
    auto late_cl = runtime->CreateHttpClient();
    BOOST_REQUIRE(!late_cl);
    BOOST_CHECK(late_cl.error().code == ErrorCode::Closed);
    auto late_srv = runtime->ListenHttp({"127.0.0.1", 0}, TestHandler);
    BOOST_REQUIRE(!late_srv);
    BOOST_CHECK(late_srv.error().code == ErrorCode::Closed);
}

BOOST_AUTO_TEST_CASE(t_handler_captured_resource_released) {
    // g_server 的 handler 捕获 Guard：析构计数在 impl 销毁时归零。
    // 反登记缺失时 Runtime 存活期间 impl 不析构，计数保持 1。
    BOOST_CHECK_EQUAL(g_captured->load(), 1);
    g_server->StopAccepting();
    BOOST_CHECK(CloseAndAssert(g_server));
    g_server.reset();
    // 物理关闭 + 反登记后：handler 捕获资源随 impl 析构释放。
    BOOST_REQUIRE(WaitUntil([&] { return g_captured->load() == 0; }));
}

BOOST_AUTO_TEST_CASE(t_end_runtime_convergence) {
    // 进程寿命运行时没有 Scheduler::Stop：收尾靠显式 Close() + 物理落定
    // 探针（托管集合归零、transport owner 已落定）。
    auto impl = std::static_pointer_cast<http_detail::NetworkRuntimeImpl>(
        g_runtime);
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 0u);

    g_runtime->Close();
    BOOST_CHECK(g_runtime->IsClosed());
    BOOST_CHECK(impl->TransportClosedForTest());
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 0u);

    g_runtime->Close();   // 幂等
    BOOST_CHECK(g_runtime->IsClosed());
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 0u);

    // 关闭后不再产出子对象（收口即封口，不重开）。
    auto late_cl = g_runtime->CreateHttpClient();
    BOOST_REQUIRE(!late_cl);
    BOOST_CHECK(late_cl.error().code == ErrorCode::Closed);

    g_runtime.reset();
}

BOOST_AUTO_TEST_SUITE_END()
