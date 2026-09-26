// Issue #38：HTTP 子对象物理关闭后从 NetworkRuntime 托管集合安全反登记。
//
// 验收映射（infra-review-issues-20260924/02-http-owner.md）：
//   t_children_released_while_runtime_alive
//       — Runtime 存活期间反复创建/关闭/释放有限数量 client 与 server；
//         关闭回调排空且外部引用退出后 weak_ptr 失效，不等 Runtime 析构。
//   t_caller_early_release_still_managed
//       — 调用者提前释放外部引用时对象仍被强托管到真实清理完成；
//         清理落定前不提前 Closed，落定后经 weak_ptr 可观察到失效。
//   t_idle_client_released_still_managed
//       — 空闲活跃对象（无在途 op、未 RequestClose）释放唯一外部引用
//         后仍由 Runtime 强托管；隔离 ClientOp::owner 反向强引用，
//         Runtime teardown 落定后 weak_ptr 失效（client 与 server 双侧）。
//   t_runtime_close_with_early_released_children
//       — 子对象先关、Runtime 后关、外部引用提前释放的组合路径：
//         计数不丢、不双删、不 UAF。
//   t_repeat_and_factory_race_close
//       — 重复 close、并发 close、工厂与 close 竞争：RequestClose 幂等，
//         关闭中/后的工厂调用一律拒绝，已登记对象必被收口。
//   t_handler_captured_resource_released
//       — server handler 捕获资源以析构计数证明可释放，不靠 RSS 判断。
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

// 竞态证据所需的 impl 内测试接缝（SetAdoptCommitGateForTest），
// 与 tests/Test_mongo_unit.cc 消费 src/ 内 impl 头同一约定。
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
        for (int i = 0; i < 5000 && !g_hold_release.load(); ++i)
            bbtco_sleep(2);
        g_hold_exited.store(true);
        return result<HttpResponse>::ok(HttpResponse{200, {}, "hold done"});
    }
    return result<HttpResponse>::ok(HttpResponse{404, {}, "not found"});
}

// 关闭并等待一个子对象落定；返回 CloseStatus 供断言。
CloseStatus CloseAndWait(const std::shared_ptr<ICoCloseable>& obj,
                         int wait_ms = 10000) {
    obj->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(obj->WaitClosed(
            std::chrono::steady_clock::now() +
                std::chrono::milliseconds(wait_ms),
            {}));
    }));
    return st.load();
}

} // namespace

BOOST_AUTO_TEST_SUITE(http_child_unregister)

BOOST_AUTO_TEST_CASE(t_begin_start_runtime) {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 2;
    cfg->m_cfg_stack_size        = 1024 * 256;
    g_scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(g_scheduler->IsRunning());

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
    // Runtime 存活：循环创建 client→真实请求→RequestClose→WaitClosed→
    // 释放外部引用；weak_ptr 必须在 Runtime 析构前失效。
    for (int i = 0; i < 4; ++i) {
        auto cl = g_runtime->CreateHttpClient();
        BOOST_REQUIRE(cl);
        auto client = std::move(cl).value();
        std::weak_ptr<HttpClient> weak = client;

        auto res = RoundTrip(client, "/ok");
        BOOST_REQUIRE(res);
        BOOST_CHECK_EQUAL(res.value().status, 200u);

        BOOST_CHECK(CloseAndWait(client) == CloseStatus::Closed);
        client.reset();
        // 关闭回调排空 + 外部引用退出：反登记后 weak_ptr 失效。
        BOOST_REQUIRE(WaitUntil([&] { return weak.expired(); }));
    }

    // server 同样覆盖：ListenHttp→StopAccepting→RequestClose→释放。
    for (int i = 0; i < 2; ++i) {
        auto srv = g_runtime->ListenHttp({"127.0.0.1", 0}, TestHandler);
        BOOST_REQUIRE(srv);
        auto server = std::move(srv).value();
        std::weak_ptr<HttpServer> weak = server;

        server->StopAccepting();
        BOOST_CHECK(CloseAndWait(server) == CloseStatus::Closed);
        server.reset();
        BOOST_REQUIRE(WaitUntil([&] { return weak.expired(); }));
    }

    // Runtime 仍可用：反登记没有破坏工厂。
    auto cl = g_runtime->CreateHttpClient();
    BOOST_REQUIRE(cl);
    auto client = std::move(cl).value();
    auto res = RoundTrip(client, "/ok");
    BOOST_REQUIRE(res);
    BOOST_CHECK(CloseAndWait(client) == CloseStatus::Closed);
}

BOOST_AUTO_TEST_CASE(t_caller_early_release_still_managed) {
    // 在途请求 + 外部引用提前释放：对象必须由 Runtime 强托管至真实
    // 清理完成；清理落定前不得提前 Closed。
    auto cl = g_runtime->CreateHttpClient();
    BOOST_REQUIRE(cl);
    std::weak_ptr<HttpClient> weak;
    std::atomic_bool out_ready{false};
    {
        auto client = std::move(cl).value();
        weak = client;

        bool succ = false;
        // 同步生命周期交接：裸指针在注册前于主线程取出并随 lambda
        // 按值传递，协程不再引用 client 变量本身——主线程随后的
        // client.reset() 与协程对对象的使用之间不存在对同一
        // shared_ptr 变量的并发读写，也不产生悬垂引用。
        // 指针有效性由被测契约保证：Request 在途期间对象必须由
        // Runtime 托管（m_children / 在途 op 强引用）；若实现丢失
        // 托管，raw 失效会以崩溃而非静默数据竞争暴露缺陷。
        HttpClient* raw = client.get();
        g_scheduler->RegistCoroutineTask(
            [raw, &out_ready] {
                CallOptions opt;
                opt.deadline = std::chrono::steady_clock::now() +
                               std::chrono::seconds(20);
                HttpRequest req{
                    "GET",
                    "http://127.0.0.1:" + std::to_string(g_port) + "/hold",
                    {}, ""};
                raw->Request(std::move(req), opt);
                out_ready.store(true, std::memory_order_release);
            },
            succ);
        BOOST_REQUIRE(succ);
        BOOST_REQUIRE(WaitUntil([&] { return g_hold_entered.load(); }));

        // 请求在途时调用方释放外部引用：不立即触发物理清理。
        client->RequestClose();
        client.reset();   // 唯一外部强引用释放；Runtime 仍托管
        BOOST_CHECK(!weak.expired());   // m_children 仍持有
    }
    // 清理落定前 IsClosed 不成立也观察不到 weak 失效；放行 handler。
    BOOST_CHECK(!weak.expired());
    g_hold_release.store(true);
    BOOST_REQUIRE(WaitUntil([&] { return g_hold_exited.load(); }));
    // teardown 完成后 MarkClosed→hook→反登记：weak_ptr 失效，
    // 说明捕获的 op/handler 资源随 impl 释放（不等 Runtime 析构）。
    BOOST_REQUIRE(WaitUntil([&] { return weak.expired(); }));
    BOOST_REQUIRE(WaitUntil(
        [&] { return out_ready.load(std::memory_order_acquire); }));
}

BOOST_AUTO_TEST_CASE(t_idle_client_released_still_managed) {
    // 空闲活跃 client（无在途 op、未 RequestClose）释放唯一外部引用：
    // 必须由 Runtime m_children 强托管——若实现退化为不托管，weak 立即
    // 失效。隔离 ClientOp::owner 路径：本用例全程无 Request，不存在
    // op 对 impl 的反向强引用。
    auto rt = NetworkRuntime::Create(MakeLimits());
    BOOST_REQUIRE(rt);
    auto rtp = std::move(rt).value();
    BOOST_REQUIRE(rtp->Start());

    std::weak_ptr<HttpClient> weak;
    {
        auto cl = rtp->CreateHttpClient();
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

    // Runtime 关闭：teardown 物理关闭空闲 child → MarkClosed → hook →
    // 反登记移除最后一个强引用，weak 随之失效。
    rtp->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(rtp->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10),
            {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
    BOOST_REQUIRE(WaitUntil([&] { return weak.expired(); }));

    // server 变体：ListenHttp 后立即释放外部引用（accept 已在跑但无
    // 连接），同样由 Runtime 托管到 teardown 落定。
    auto rt2 = NetworkRuntime::Create(MakeLimits());
    BOOST_REQUIRE(rt2);
    auto rtp2 = std::move(rt2).value();
    BOOST_REQUIRE(rtp2->Start());
    std::weak_ptr<HttpServer> weak_srv;
    {
        auto srv = rtp2->ListenHttp({"127.0.0.1", 0}, TestHandler);
        BOOST_REQUIRE(srv);
        auto server = std::move(srv).value();
        weak_srv = server;
        server.reset();
    }
    BOOST_REQUIRE(!weak_srv.expired());
    rtp2->RequestClose();
    std::atomic<CloseStatus> st2{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st2.store(rtp2->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10),
            {}));
    }));
    BOOST_CHECK(st2.load() == CloseStatus::Closed);
    BOOST_REQUIRE(WaitUntil([&] { return weak_srv.expired(); }));
}

BOOST_AUTO_TEST_CASE(t_runtime_close_with_early_released_children) {
    // 独立 runtime：子对象先 RequestClose、外部引用提前释放，随后
    // Runtime 再关；验证计数配对、不双删、不 UAF。
    auto rt = NetworkRuntime::Create(MakeLimits());
    BOOST_REQUIRE(rt);
    auto rtp = std::move(rt).value();
    BOOST_REQUIRE(rtp->Start());

    std::vector<std::weak_ptr<HttpClient>> clients;
    for (int i = 0; i < 3; ++i) {
        auto cl = rtp->CreateHttpClient();
        BOOST_REQUIRE(cl);
        auto client = std::move(cl).value();
        clients.push_back(client);
        client->RequestClose();       // 子对象先关
        client.reset();               // 外部引用提前释放
    }
    // 重复 close 幂等：再次对已关闭对象发起不崩溃不双计。
    {
        auto cl = rtp->CreateHttpClient();
        BOOST_REQUIRE(cl);
        auto client = std::move(cl).value();
        client->RequestClose();
        client->RequestClose();
        client->RequestClose();
        client.reset();
    }
    rtp->RequestClose();
    std::atomic<CloseStatus> st{};
    BOOST_REQUIRE(RunInCoroutine([&] {
        st.store(rtp->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds(10), {}));
    }));
    BOOST_CHECK(st.load() == CloseStatus::Closed);
    for (auto& w : clients)
        BOOST_CHECK(w.expired());
}

BOOST_AUTO_TEST_CASE(t_repeat_and_factory_race_close) {
    // 并发 close：多线程同时 RequestClose 同一对象，幂等且不丢计数。
    auto rt = NetworkRuntime::Create(MakeLimits());
    BOOST_REQUIRE(rt);
    auto rtp = std::move(rt).value();
    BOOST_REQUIRE(rtp->Start());
    auto cl = rtp->CreateHttpClient();
    BOOST_REQUIRE(cl);
    auto client = std::move(cl).value();
    std::weak_ptr<HttpClient> weak = client;

    std::vector<std::thread> closers;
    for (int i = 0; i < 4; ++i)
        closers.emplace_back([&] { client->RequestClose(); });
    for (auto& t : closers)
        t.join();
    BOOST_CHECK(CloseAndWait(client) == CloseStatus::Closed);
    client.reset();
    BOOST_REQUIRE(WaitUntil([&] { return weak.expired(); }));

    // 工厂与 close 真竞态：两个子段分别真实命中 adopt/rejected 分支，
    // 两侧胜出者都来自与 RequestClose 同一并发窗口的竞态线程结果，
    // 不使用任何在屏障前预登记的对象充当成功样本。
    // 线性化点在 CheckAndAdoptLocked 的锁内复检（m_state/m_sealed/IsOpen）：
    //   adopt 成功 ⇒ 对象登记早于 RequestClose 的 m_state 提交点 ⇒
    //     必在 teardown 快照中被收口：WaitClosed 后 IsClosed 成立、
    //     外部引用释放后 weak 失效；
    //   adopt 失败 ⇒ Closed 错误，不假成功、不产生孤儿。
    int saw_adopted  = 0;
    int saw_rejected = 0;

    // 子段 A：adopt 胜出必须来自与 RequestClose 的真实重叠调用窗口，
    // 不靠 sleep/时序推断。确定性构造（不碰运气）：
    //   1) 经 impl 测试接缝 SetAdoptCommitGateForTest 把 factory 的
    //      CreateHttpClient 停在「CheckAndAdoptLocked 复检已过、
    //      登记提交未发生」的临界段内（此时仍持 m_lifecycle_mtx）；
    //   2) 主线程观察到 factory 停在调用内部后，才发起
    //      rtp2->RequestClose()——factory 调用窗口 ⊃ RequestClose
    //      调用窗口，重叠是构造保证的，不是时序猜测；
    //   3) 放行 gate：登记在 RequestClose 已提交 BeginClose/
    //      m_state、io 域 teardown 正等 m_lifecycle_mtx 时提交——
    //      sealed 尚未置位 ⇒ 登记先于 sealed ⇒ teardown 快照必含
    //      此对象 ⇒ WaitClosed 后必 IsClosed、外部引用释放后 weak
    //      失效。若实现丢这份通知，saw_adopted 断言失败。
    {
        auto rt2 = NetworkRuntime::Create(MakeLimits());
        BOOST_REQUIRE(rt2);
        auto rtp2 = std::move(rt2).value();
        BOOST_REQUIRE(rtp2->Start());
        auto impl2 =
            std::static_pointer_cast<http_detail::NetworkRuntimeImpl>(
                rtp2);

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
            res = rtp2->CreateHttpClient();   // 内部停在 adopt gate
            if (res && *res)
                res_weak = res->value();
        });

        // factory 调用确实在飞（停在 adopt 临界段内、持 m_lifecycle_mtx）。
        BOOST_REQUIRE(WaitUntil(
            [&] { return adopt_gate_entered.load(
                       std::memory_order_acquire); }));

        // 此刻发起 RequestClose：closer 在紧贴调用点置 close_entered；
        // 观察到该标志后再放行 gate——factory 调用在 RequestClose
        // 发起时仍在飞，登记提交也必然晚于 RequestClose 发起点：
        // 两次调用真实重叠，不依赖任何 sleep/时序推断。
        // （不先 join closer：若 TryPost 失败，TeardownOffDomain 会在
        //   closer 线程上阻塞等待 factory 所持的锁，先 join 将死锁。）
        std::atomic<bool> close_entered{false};
        std::thread closer([&] {
            close_entered.store(true, std::memory_order_release);
            rtp2->RequestClose();
        });
        BOOST_REQUIRE(WaitUntil(
            [&] { return close_entered.load(
                       std::memory_order_acquire); }));
        release_adopt_gate.store(true, std::memory_order_release);
        closer.join();
        factory.join();

        std::atomic<CloseStatus> st{};
        BOOST_REQUIRE(RunInCoroutine([&] {
            st.store(rtp2->WaitClosed(
                std::chrono::steady_clock::now() +
                    std::chrono::seconds(10),
                {}));
        }));
        BOOST_CHECK(st.load() == CloseStatus::Closed);

        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE(*res);   // adopt gate 放行 ⇒ 必为成功结果
        ++saw_adopted;
        auto adopted_client = std::move(res->value());
        BOOST_CHECK(adopted_client->IsClosed());
        adopted_client.reset();
        BOOST_REQUIRE(WaitUntil([&] { return res_weak.expired(); }));
    }

    // 子段 B：rejected 分支——sealed 后的 factory 一律拒绝。
    // closer 与 factory 同屏障零延迟起跑，RequestClose 的 CAS 先提交，
    // 竞态 factory 的 CheckAndAdoptLocked 复检必然观察到
    // m_state != kRunning 或 !IsOpen()——返回 Closed 不假成功。
    {
        constexpr int kRejectRounds = 24;
        constexpr int kFactories    = 4;
        for (int round = 0; round < kRejectRounds; ++round) {
            auto rt2 = NetworkRuntime::Create(MakeLimits());
            BOOST_REQUIRE(rt2);
            auto rtp2 = std::move(rt2).value();
            BOOST_REQUIRE(rtp2->Start());

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
                    results[i] = rtp2->CreateHttpClient();
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
                rtp2->RequestClose();
            });
            BOOST_REQUIRE(WaitUntil([&] {
                return ready.load(std::memory_order_acquire) ==
                       kFactories + 1;
            }));
            go.store(true, std::memory_order_release);
            for (auto& t : workers)
                t.join();

            rtp2->RequestClose();  // 幂等；覆盖 closer 未取胜的轮次
            std::atomic<CloseStatus> st{};
            BOOST_REQUIRE(RunInCoroutine([&] {
                st.store(rtp2->WaitClosed(
                    std::chrono::steady_clock::now() +
                        std::chrono::seconds(10),
                    {}));
            }));
            BOOST_CHECK(st.load() == CloseStatus::Closed);

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
    rtp->RequestClose();
    BOOST_REQUIRE(RunInCoroutine([&] {
        rtp->WaitClosed(std::chrono::steady_clock::now() +
                            std::chrono::seconds(10),
                        {});
    }));
    auto late_cl = rtp->CreateHttpClient();
    BOOST_REQUIRE(!late_cl);
    BOOST_CHECK(late_cl.error().code == ErrorCode::Closed);
    auto late_srv = rtp->ListenHttp({"127.0.0.1", 0}, TestHandler);
    BOOST_REQUIRE(!late_srv);
    BOOST_CHECK(late_srv.error().code == ErrorCode::Closed);
}

BOOST_AUTO_TEST_CASE(t_handler_captured_resource_released) {
    // g_server 的 handler 捕获 Guard：析构计数在 impl 销毁时归零。
    // 反登记缺失时 Runtime 存活期间 impl 不析构，计数保持 1。
    BOOST_CHECK_EQUAL(g_captured->load(), 1);
    g_server->StopAccepting();
    BOOST_CHECK(CloseAndWait(g_server) == CloseStatus::Closed);
    g_server.reset();
    // 物理关闭 + 反登记后：handler 捕获资源随 impl 析构释放。
    BOOST_REQUIRE(WaitUntil([&] { return g_captured->load() == 0; }));
}

BOOST_AUTO_TEST_CASE(t_end_stop_scheduler) {
    g_server.reset();
    g_runtime->RequestClose();
    BOOST_REQUIRE(RunInCoroutine([&] {
        g_runtime->WaitClosed(std::chrono::steady_clock::now() +
                                  std::chrono::seconds(10),
                              {});
    }));
    g_runtime.reset();
    g_scheduler->Stop();
}

BOOST_AUTO_TEST_SUITE_END()
