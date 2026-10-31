// Processer driver 私有适配聚焦测试（批次）。
//
// 覆盖：
// bind —— 真实 driver 绑定已登记 P 槽；OnTick 在目标 worker（非测试线程）
// 且 LastTickProcesserId 与绑定槽一致；owner context 的 Asio handler
// 由 P 推进；绑定不新建线程；driver 与业务协程并存；同步解绑后停 tick。
// wake —— 早到 latch 短路；外线程打断阻塞 Wait；并发/嵌套 Wake；
// 唤醒计数与 latch 持久。
// budget —— 单次 OnTick handler 数预算；AfterSchedule 不推进 context。
// time_budget —— 回调内部耗时计入时间预算，预算耗尽即停。
// rollback —— 槽不足整体回滚，且不扰动他人已有绑定；回滚后空槽仍可用。
// unbind —— owner 线程内自解绑被拒（SelfDetach）、绑定保留；控制线程同步解绑；
// stale token 被拒；解绑后不再 tick。
// failed —— 注入 OnTick 异常走 runtime 既有 Failed 出口并可查询。
//
// 本测试使用动态端口以外的纯 driver 路径（不伪造 fd/TCP 能力）；raw/Conn/Co
// 真实收发由后续批次补齐。

#include <bbt/infra/net/detail/DriverBinder.hpp>
#include <bbt/infra/net/detail/ReactorDriver.hpp>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <functional>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>

#include <bbt/coroutine/coroutine.hpp>

using namespace bbt::infra::net;
using namespace std::chrono_literals;
using bbt::coroutine::detail::ProcesserDriverFailure;

static void Check(bool value, const char* expression, int line) {
    if (!value) {
        throw std::runtime_error("line " + std::to_string(line) + ": " + expression);
    }
}
#define CHECK(expression) Check(static_cast<bool>(expression), #expression, __LINE__)

static std::future<void> Task(std::function<void()> callback) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    g_scheduler->RegistCoroutineTask(65536, [promise, callback] {
        try { callback(); promise->set_value(); }
        catch (...) { promise->set_exception(std::current_exception()); }
    }, "net.reactor_driver");
    return future;
}

static void Get(std::future<void>& future) {
    CHECK(future.wait_for(5s) == std::future_status::ready);
    future.get();
}

static bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) { return true; }
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

static std::size_t CountTasks() {
    return static_cast<std::size_t>(std::distance(
        std::filesystem::directory_iterator("/proc/self/task"),
        std::filesystem::directory_iterator{}));
}

static void StartRuntime(std::size_t workers) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = workers;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start();  // 每进程一次；无 Stop，进程退出走 RunTestProcessExit 边界。
}

static void BindCase() {
    StartRuntime(2);
    const auto threads_before = CountTasks();

    std::vector<DriverSlot> slots;
    CHECK(!DriverBinder::BindSlots(1, slots));
    CHECK(slots.size() == 1);
    CHECK(slots[0].processer_id != BBT_COROUTINE_INVALID_PROCESSER_ID);
    const auto driver = slots[0].driver;
    const auto main_thread = std::this_thread::get_id();

    // 不新建任何线程：driver 只占已登记 P 的槽。
    CHECK(CountTasks() == threads_before);

    // OnTick 真在绑定 P 的 worker、非测试线程执行。
    CHECK(WaitUntil([&] { return driver->TickCount() > 0; }, 3000ms));
    CHECK(driver->LastTickThread() != main_thread);
    CHECK(driver->LastTickProcesserId() == slots[0].processer_id);

    // owner context 的 Asio handler 由该 P 推进。
    std::atomic<bool> handler_ran{false};
    boost::asio::post(driver->Context(), [&handler_ran] { handler_ran.store(true); });
    CHECK(WaitUntil([&] { return handler_ran.load(); }, 3000ms));

    // driver 与业务协程并存：业务协程真实让出 P 并推进。
    std::atomic<int> business{0};
    auto task = Task([&business] {
        for (int i = 0; i < 50; ++i) {
            bbt::coroutine::sync::CoWaiter waiter;
            CHECK(waiter.WaitWithTimeout(1) == 1);
            business.fetch_add(1);
        }
    });
    Get(task);
    CHECK(business.load() == 50);
    CHECK(driver->TickCount() > 0);

    // 同代同步解绑：控制线程调用，等全部 Tick/Wait/Wake lease 退出。
    CHECK(!DriverBinder::UnbindSlots(slots));
    CHECK(slots.empty());
    const auto ticks_at_unbind = driver->TickCount();
    std::this_thread::sleep_for(50ms);
    CHECK(driver->TickCount() == ticks_at_unbind);

    std::cout << "bind processer=" << driver->LastTickProcesserId()
              << " ticks=" << driver->TickCount()
              << " threads=" << threads_before << "->" << CountTasks()
              << " business=" << business.load() << " unbind=ok\n";
}

static void WakeCase() {
    ReactorDriver driver(4);

    // 早到：Wake 在 Wait 之前，latch 持久，Wait 不睡满 timeout。
    driver.Wake();
    auto begin = std::chrono::steady_clock::now();
    CHECK(driver.Wait(200ms) == bbt::coroutine::detail::ProcesserDriverResult::Ok);
    CHECK(std::chrono::steady_clock::now() - begin < 100ms);

    // 外线程在 Wait 阻塞期间打断。
    std::thread waker([&driver] {
        std::this_thread::sleep_for(30ms);
        driver.Wake();
    });
    begin = std::chrono::steady_clock::now();
    CHECK(driver.Wait(500ms) == bbt::coroutine::detail::ProcesserDriverResult::Ok);
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    waker.join();
    CHECK(elapsed < 300ms);

    // 并发/嵌套 Wake：noexcept、有界、线程安全。
    const auto wakes_before = driver.WakeCount();
    std::vector<std::thread> wakers;
    for (int w = 0; w < 4; ++w) {
        wakers.emplace_back([&driver] {
            for (int i = 0; i < 50; ++i) {
                driver.Wake();
                driver.Wake();
            }
        });
    }
    for (auto& thread : wakers) { thread.join(); }
    CHECK(driver.WakeCount() >= wakes_before + 400);

    // 并发 Wake 后 latch 仍记忆：下一次 Wait 立即返回。
    begin = std::chrono::steady_clock::now();
    CHECK(driver.Wait(200ms) == bbt::coroutine::detail::ProcesserDriverResult::Ok);
    CHECK(std::chrono::steady_clock::now() - begin < 100ms);

    // burst 已被消费（上一步 Wait 已清 latch）后，新的阻塞 Wait 中再来
    // 一个 Wake：latch 重新置位 + stop 打断当前 run_one_for，不睡满预算。
    // 这验证"合并重复 stop"不削弱后续独立唤醒的实时性。
    std::atomic<bool> second_wake_sent{false};
    std::thread late_waker([&driver, &second_wake_sent] {
        std::this_thread::sleep_for(30ms);
        second_wake_sent.store(true);
        driver.Wake();
    });
    begin = std::chrono::steady_clock::now();
    CHECK(driver.Wait(500ms) == bbt::coroutine::detail::ProcesserDriverResult::Ok);
    const auto second_elapsed = std::chrono::steady_clock::now() - begin;
    const bool wake_sent_before_return = second_wake_sent.load();
    late_waker.join();
    CHECK(wake_sent_before_return);  // 不能由旧状态提前返回，再靠 join 等到新 Wake。
    CHECK(second_elapsed < 300ms);

    std::cout << "wake wakes=" << driver.WakeCount() << " waits=" << driver.WaitCount()
              << " early/cross-thread/concurrent=ok\n";
}

static void BudgetCase() {
    // 时间预算置 0 关闭，单独验证 handler 数预算。
    ReactorDriver driver(4, std::chrono::microseconds(0));
    std::atomic<int> ran{0};
    for (int i = 0; i < 100; ++i) {
        boost::asio::post(driver.Context(), [&ran] { ran.fetch_add(1); });
    }
    CHECK(driver.OnTick(bbt::coroutine::detail::ProcesserDriverPhase::BeforeSchedule)
        == bbt::coroutine::detail::ProcesserDriverResult::Ok);
    CHECK(ran.load() == 4);
    CHECK(driver.LastTickHandlers() == 4);
    CHECK(driver.OnTick(bbt::coroutine::detail::ProcesserDriverPhase::BeforeSchedule)
        == bbt::coroutine::detail::ProcesserDriverResult::Ok);
    CHECK(ran.load() == 8);

    for (int i = 0; i < 40 && ran.load() < 100; ++i) {
        driver.OnTick(bbt::coroutine::detail::ProcesserDriverPhase::BeforeSchedule);
    }
    CHECK(ran.load() == 100);

    // AfterSchedule 不推进 context，只计数。
    boost::asio::post(driver.Context(), [&ran] { ran.fetch_add(1); });
    CHECK(driver.OnTick(bbt::coroutine::detail::ProcesserDriverPhase::AfterSchedule)
        == bbt::coroutine::detail::ProcesserDriverResult::Ok);
    CHECK(ran.load() == 100);
    CHECK(driver.OnTick(bbt::coroutine::detail::ProcesserDriverPhase::BeforeSchedule)
        == bbt::coroutine::detail::ProcesserDriverResult::Ok);
    CHECK(ran.load() == 101);

    // 回调内部耗时计入时间预算：单次 OnTick 不越过预算继续抽干。
    ReactorDriver slow(16, 100us);
    std::atomic<int> slow_ran{0};
    boost::asio::post(slow.Context(), [&slow_ran] {
        std::this_thread::sleep_for(5ms);
        slow_ran.fetch_add(1);
    });
    for (int i = 0; i < 10; ++i) {
        boost::asio::post(slow.Context(), [&slow_ran] { slow_ran.fetch_add(1); });
    }
    const auto begin = std::chrono::steady_clock::now();
    CHECK(slow.OnTick(bbt::coroutine::detail::ProcesserDriverPhase::BeforeSchedule)
        == bbt::coroutine::detail::ProcesserDriverResult::Ok);
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    CHECK(slow_ran.load() == 1);
    CHECK(elapsed >= 5ms);

    std::cout << "budget handlers=" << ran.load() << " slow_ran=" << slow_ran.load()
              << " elapsed_ms=" << std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
              << " after_schedule_no_poll=ok\n";
}

static void RollbackCase() {
    StartRuntime(3);

    std::vector<DriverSlot> first;
    CHECK(!DriverBinder::BindSlots(2, first));
    CHECK(first.size() == 2);
    CHECK(WaitUntil([&] { return first[0].driver->TickCount() > 0; }, 3000ms));

    // 只剩 1 个空槽，请求 2：本次已绑定的 1 个必须回滚，不留半成功。
    std::vector<DriverSlot> second;
    const auto error = DriverBinder::BindSlots(2, second);
    CHECK(error == std::errc::no_buffer_space);
    CHECK(second.empty());

    // 回滚后那个空槽仍可用；first 的绑定未被扰动。
    std::vector<DriverSlot> third;
    CHECK(!DriverBinder::BindSlots(1, third));
    CHECK(third.size() == 1);
    CHECK(third[0].processer_id != first[0].processer_id);
    CHECK(third[0].processer_id != first[1].processer_id);
    CHECK(WaitUntil([&] { return third[0].driver->TickCount() > 0; }, 3000ms));

    // 请求超过全部槽：整体回滚、零绑定。
    std::vector<DriverSlot> fourth;
    CHECK(DriverBinder::BindSlots(4, fourth) == std::errc::no_buffer_space);
    CHECK(fourth.empty());

    CHECK(!DriverBinder::UnbindSlots(third));
    CHECK(first[0].driver->TickCount() > 0);
    CHECK(!DriverBinder::UnbindSlots(first));

    std::cout << "rollback slots=3 partial/over-request rollback=ok untouched=ok\n";
}

static void UnbindCase() {
    StartRuntime(2);

    std::vector<DriverSlot> slots;
    CHECK(!DriverBinder::BindSlots(1, slots));
    const auto driver = slots[0].driver;
    const auto token = slots[0].token;
    CHECK(WaitUntil([&] { return driver->TickCount() > 0; }, 3000ms));

    // 在绑定自己的 worker 回调内解绑：任何副作用前被拒，绑定保留。
    std::atomic<int> self_status{-1};
    boost::asio::post(driver->Context(), [&self_status, &token] {
        const auto status =
            bbt::coroutine::detail::Scheduler::GetInstance()->UnregisterProcesserDriver(token);
        self_status.store(static_cast<int>(status));
    });
    CHECK(WaitUntil([&] { return self_status.load() >= 0; }, 3000ms));
    CHECK(self_status.load() == static_cast<int>(bbt::coroutine::detail::ProcesserDriverStatus::SelfDetach));

    const auto ticks_after_self = driver->TickCount();
    CHECK(WaitUntil([&] { return driver->TickCount() > ticks_after_self; }, 3000ms));

    // 控制线程同代同步解绑。
    CHECK(!DriverBinder::UnbindSlots(slots));
    CHECK(slots.empty());

    // stale token 不能解绑新一代/已解绑代。
    const auto stale = bbt::coroutine::detail::Scheduler::GetInstance()->UnregisterProcesserDriver(token);
    CHECK(stale == bbt::coroutine::detail::ProcesserDriverStatus::StaleToken);

    const auto ticks_at_unbind = driver->TickCount();
    std::this_thread::sleep_for(50ms);
    CHECK(driver->TickCount() == ticks_at_unbind);

    std::cout << "unbind self_detach=ok ticks_stop=" << driver->TickCount()
              << " stale_token=ok\n";
}

static void FailedCase() {
    StartRuntime(2);

    std::vector<DriverSlot> slots;
    CHECK(!DriverBinder::BindSlots(1, slots));
    const auto driver = slots[0].driver;
    const auto processer_id = slots[0].processer_id;
    CHECK(WaitUntil([&] { return driver->TickCount() > 0; }, 3000ms));

    driver->InjectTickFailureOnce();
    bbt::coroutine::detail::ProcesserDriverSnapshot snapshot;
    CHECK(WaitUntil([&] {
        bbt::coroutine::detail::ProcesserDriverSnapshot current;
        const auto status =
            bbt::coroutine::detail::Scheduler::GetInstance()->QueryProcesserDriver(processer_id, current);
        if (status != bbt::coroutine::detail::ProcesserDriverStatus::Ok) { return false; }
        if (current.m_state == bbt::coroutine::detail::ProcesserDriverState::Failed) {
            snapshot = current;
            return true;
        }
        return false;
    }, 3000ms));
    CHECK(snapshot.m_state == bbt::coroutine::detail::ProcesserDriverState::Failed);
    CHECK(snapshot.m_failure == ProcesserDriverFailure::Tick);
    CHECK(snapshot.m_token == slots[0].token);

    // Failed 仍可合法解绑；解绑后不再 tick。
    CHECK(!DriverBinder::UnbindSlots(slots));
    const auto ticks_at_unbind = driver->TickCount();
    std::this_thread::sleep_for(50ms);
    CHECK(driver->TickCount() == ticks_at_unbind);

    std::cout << "failed state=Failed source=Tick query=ok unbind_after_failed=ok\n";
}

static void BindPreserveCase() {
    StartRuntime(2);
    std::vector<DriverSlot> out;
    CHECK(!DriverBinder::BindSlots(1, out));
    CHECK(out.size() == 1);
    const auto first_token = out[0].token;
    const auto first_id = out[0].processer_id;

    // 第二次调用追加，不丢弃调用者既存绑定。
    CHECK(!DriverBinder::BindSlots(1, out));
    CHECK(out.size() == 2);
    CHECK(out[0].token == first_token);
    CHECK(out[0].processer_id == first_id);

    // 槽不足失败时 out 保持原样（不丢既存、不半追加）。
    CHECK(DriverBinder::BindSlots(1, out) == std::errc::no_buffer_space);
    CHECK(out.size() == 2);
    CHECK(out[0].token == first_token);
    CHECK(!DriverBinder::UnbindSlots(out));
    CHECK(out.empty());

    std::cout << "bind_preserve append=" << 2 << " fail_keeps_existing=ok\n";
}

static void UnbindRefusalCase() {
    StartRuntime(2);
    std::vector<DriverSlot> slots;
    CHECK(!DriverBinder::BindSlots(1, slots));
    const auto token = slots[0].token;

    // 外部先行同代解绑（模拟竞争/已脱离）：该代归属已不存在，DriverBinder
    // 依核心契约 / 把 StaleToken 视为安全移出，不伪报仍绑定；
    // 不存在的旧 token 不能解绑后续新代（另测）。
    CHECK(bbt::coroutine::detail::Scheduler::GetInstance()->UnregisterProcesserDriver(token)
        == bbt::coroutine::detail::ProcesserDriverStatus::Ok);
    CHECK(!DriverBinder::UnbindSlots(slots));
    CHECK(slots.empty());

    std::cout << "unbind_refusal stale_safe_drop=1 remaining=0\n";
}

static void ExceptionCase() {
    // 逃逸到 driver 层的未分类异常进入 Failed 可观察出口，不被计数抹平。
    ReactorDriver driver(4);
    boost::asio::post(driver.Context(), [] { throw std::runtime_error("escaped handler"); });
    CHECK(driver.OnTick(bbt::coroutine::detail::ProcesserDriverPhase::BeforeSchedule)
        == bbt::coroutine::detail::ProcesserDriverResult::Failed);
    CHECK(driver.UnhandledFailureCount() == 1);
    CHECK(driver.LastFailure() != nullptr);

    std::cout << "exception unhandled=" << driver.UnhandledFailureCount()
              << " last_failure=set Failed=ok\n";
}

static void StopRestartCase() {
    // Wake 的 stop/restart 不是丢 callback 手段：已登记 readiness 在 restart 后兑现。
    ReactorDriver driver(4);
    std::atomic<int> fired{0};
    auto timer = std::make_shared<boost::asio::steady_timer>(driver.Context(), 40ms);
    timer->async_wait([&fired](const boost::system::error_code& error) {
        if (!error) { fired.fetch_add(1); }
    });

    driver.Wake();  // timer 到期前 stop
    auto begin = std::chrono::steady_clock::now();
    CHECK(driver.Wait(200ms) == bbt::coroutine::detail::ProcesserDriverResult::Ok);
    const auto elapsed = std::chrono::steady_clock::now() - begin;
    CHECK(elapsed < 100ms);   // 早到 latch 立即返回，不靠 timeout
    CHECK(fired.load() == 0); // timer 未被提前触发，也未被丢弃

    // restart 后 handler 仍会兑现（不是靠 timeout 轮询当正确性证明：这里显式推进）。
    CHECK(WaitUntil([&] {
        driver.OnTick(bbt::coroutine::detail::ProcesserDriverPhase::BeforeSchedule);
        return fired.load() > 0;
    }, 2000ms));
    CHECK(fired.load() == 1);
    CHECK(driver.UnhandledFailureCount() == 0);

    std::cout << "stop_restart fired=" << fired.load() << " early_return=ok handler_preserved=ok\n";
}

#include "ProcessRuntimeTestMain.hpp"

static int RunCases(int argc, char** argv) {
    try {
        CHECK(argc == 2);
        const std::string test = argv[1];
        if (test == "bind") { BindCase(); }
        else if (test == "wake") { WakeCase(); }
        else if (test == "budget") { BudgetCase(); }
        else if (test == "rollback") { RollbackCase(); }
        else if (test == "unbind") { UnbindCase(); }
        else if (test == "failed") { FailedCase(); }
        else if (test == "bind_preserve") { BindPreserveCase(); }
        else if (test == "unbind_refusal") { UnbindRefusalCase(); }
        else if (test == "exception") { ExceptionCase(); }
        else if (test == "stop_restart") { StopRestartCase(); }
        else { throw std::runtime_error("unknown test: " + test); }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}

int main(int argc, char** argv) {
    return bbt_net_test::RunTestProcessExit([&] { return RunCases(argc, argv); });
}
