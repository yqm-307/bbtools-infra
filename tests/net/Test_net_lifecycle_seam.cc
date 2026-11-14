
/**
 * Engine 主线程宿主定向验证（真实 Engine/真实 runtime）。
 *
 * 覆盖：ready_release 在测试进程 native main 上直接调用 Run（无 runner 线程冒充
 * 主线程），Co 任务观察真实 Running 并 RequestClose，由 main 的 Run 返回确认
 * Stopped；Run 提前失败/关闭先于 Run 的失败观察；控制门拒绝第二次 Run 与非法域
 * 调用；单 worker 下 pulse 只在 CoWaitRunning 尚未返回的窗口内计数，证明等待
 * 真正挂起后其它 Co 仍有进展。EngineLifecycle 的 pthread runner/伪
 * physical_released 已删除，替代凭据只有 Run 返回 + Phase。
 */
#include <bbt/coroutine/coroutine.hpp>
#include <bbt/infra/net/Engine.hpp>
#include "ProcessRuntimeTestMain.hpp"

#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

using namespace bbt::infra;
using namespace bbt::infra::net;
using namespace std::chrono_literals;

namespace {

/**
 * 进程 native main 的线程身份：ready_release 用它证明 Run 由 main 直接调用，
 * 而非测试另建 runner 线程冒充。
 */
std::thread::id g_main_thread_id;

void Check(bool value, const char* expression, int line)
{
    if (!value) {
        throw std::runtime_error("line " + std::to_string(line) + ": " + expression);
    }
}

#define CHECK(expression) Check(static_cast<bool>(expression), #expression, __LINE__)

bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout = 3s)
{
    const auto end = std::chrono::steady_clock::now() + timeout;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= end) {
            return false;
        }
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

std::future<void> Task(std::function<void()> callback)
{
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    g_scheduler->RegistCoroutineTask(
        128 * 1024,
        [promise, callback = std::move(callback)]() mutable {
            try {
                callback();
                promise->set_value();
            } catch (...) {
                promise->set_exception(std::current_exception());
            }
        },
        "net.lifecycle");
    return future;
}

void Get(std::future<void>& future)
{
    CHECK(future.wait_for(10s) == std::future_status::ready);
    future.get();
}

EngineConfig ClientOnlyConfig()
{
    EngineConfig config;
    config.runtime_progress = RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    return config;
}

bbt::coroutine::Deadline Soon()
{
    return std::chrono::steady_clock::now() + 3s;
}

/**
 * 真实 Running → CoWaitRunning 成功；Ready 期间 Run 阻塞在本进程 native main 上；
 * 由该 main 的 Run 返回确认 Stopped。不使用 runner 线程代替主线程。
 */
void ReadyRelease()
{
    CHECK(std::this_thread::get_id() == g_main_thread_id);
    auto engine = Engine::Create(nullptr);
    auto config = ClientOnlyConfig();
    std::atomic<bool> run_entered{false};
    std::atomic<bool> run_returned{false};

    /**
     * 调 Run 的线程；由 release/acquire 与 run_entered 配对发布
     */
    std::thread::id run_thread;
    std::error_code run_error;

    /**
     * 先提交 Co 任务：等 Run 发布真实 Running 后检查主线程仍阻塞在 Run，
     * 再由该 Co 任务请求关闭。main 的 Run 只有在请求之后才可能返回。
     */
    auto waiting = Task([&] {
        // clang-format off

        /**
         * Co worker 不是主线程
         */
        // clang-format on
        CHECK(std::this_thread::get_id() != g_main_thread_id);
        try {
            auto ready = engine->CoWaitRunning({Soon()});
            CHECK(ready);
            CHECK(run_entered.load(std::memory_order_acquire));

            /**
             * Run 的调用线程就是本进程 main
             */
            CHECK(run_thread == g_main_thread_id);

            /**
             * 主线程还在 Run 内
             */
            CHECK(!run_returned.load(std::memory_order_acquire));
            CHECK(engine->Phase() == EnginePhase::Running);
            (void)engine->Close();
        } catch (...) {

            /**
             * 断言失败也必须放行 main 的 Run，避免把失败伪装成等待超时。
             */
            (void)engine->Close();
            throw;
        }
    });

    run_thread = std::this_thread::get_id();
    run_entered.store(true, std::memory_order_release);

    /**
     * 本进程 native main 直接调用，无额外 runner 线程
     */
    run_error = engine->Run(config);
    run_returned.store(true, std::memory_order_release);

    CHECK(std::this_thread::get_id() == g_main_thread_id);
    CHECK(!run_error);
    CHECK(engine->Phase() == EnginePhase::Stopped);
    Get(waiting);
    CHECK(engine->CountConnections() == 0);

    auto after = Task([&] {
        auto late = engine->CoWaitRunning({std::chrono::steady_clock::now() + 200ms});
        CHECK(!late);
        CHECK(late.error().code == ErrorCode::Closed);
    });
    Get(after);
    std::cout << "ready-release run returned on process main thread after co close request\n";
}

/**
 * Run 在发布 Running 前失败：等待者必须观察 Stopped（映射具体原因），不是 Ready。
 */
void RunFailure()
{
    auto engine = Engine::Create(nullptr);
    auto config = ClientOnlyConfig();
    config.runtime_progress = RuntimeProgress::Unspecified;
    auto waiting = Task([&] {
        auto ready = engine->CoWaitRunning({Soon()});
        CHECK(!ready);
        CHECK(ready.error().code == ErrorCode::InvalidContext);
    });
    std::error_code run_error;
    std::thread runner([&] { run_error = engine->Run(config); });
    Get(waiting);
    runner.join();
    CHECK(run_error == std::errc::operation_not_permitted);
    CHECK(engine->Phase() == EnginePhase::Stopped);
    CHECK(engine->LastError() == std::errc::operation_not_permitted);

    /**
     * 同一实例不可重开：拒绝先于副作用，原错误与阶段不变。
     */
    CHECK(engine->Run(config) == std::errc::device_or_resource_busy);
    CHECK(engine->Phase() == EnginePhase::Stopped);
    CHECK(engine->LastError() == std::errc::operation_not_permitted);
}

/**
 * 关闭先于 Run：关闭请求不丢失；Run 不伪造 Running，按真实 Stopped + 首因收口。
 */
void CloseBeforeRun()
{
    auto engine = Engine::Create(nullptr);
    (void)engine->Close();
    CHECK(engine->Phase() == EnginePhase::Idle);

    std::error_code run_error;
    std::thread runner([&] { run_error = engine->Run(ClientOnlyConfig()); });
    runner.join();
    CHECK(run_error == std::errc::operation_canceled);
    CHECK(engine->Phase() == EnginePhase::Stopped);
    CHECK(engine->LastError() == std::errc::operation_canceled);
    std::vector<Address> addresses;
    CHECK(engine->LocalAddresses(addresses) == std::errc::operation_not_permitted);

    auto waiting = Task([&] {
        auto ready = engine->CoWaitRunning({std::chrono::steady_clock::now() + 200ms});
        CHECK(!ready);
        CHECK(ready.error().code == ErrorCode::Cancelled);
    });
    Get(waiting);
}

/**
 * 执行域与控制门：Co 域 Run、native CoWaitRunning、无限期限与第二次 Run 都在
 * 副作用之前被拒绝；真实 Run 成功后仍不可重开。
 */
void ControlGate()
{
    auto engine = Engine::Create(nullptr);
    auto config = ClientOnlyConfig();

    auto in_co = Task([&] {
        CHECK(engine->Run(config) == std::errc::operation_not_permitted);
        auto infinite = engine->CoWaitRunning({bbt::coroutine::Deadline::max()});
        CHECK(!infinite);
        CHECK(infinite.error().code == ErrorCode::InvalidArgument);
    });
    Get(in_co);
    CHECK(engine->Phase() == EnginePhase::Idle);

    auto native_wait = engine->CoWaitRunning({std::chrono::steady_clock::now() + 100ms});
    CHECK(!native_wait);
    CHECK(native_wait.error().code == ErrorCode::InvalidContext);
    CHECK(engine->Phase() == EnginePhase::Idle);

    std::atomic<bool> run_returned{false};
    std::error_code run_error;
    std::thread runner([&] {
        run_error = engine->Run(config);
        run_returned.store(true);
    });
    CHECK(WaitUntil([&] { return engine->Phase() == EnginePhase::Running || run_returned.load(); }));
    CHECK(engine->Phase() == EnginePhase::Running);
    CHECK(engine->Run(config) == std::errc::device_or_resource_busy);
    CHECK(engine->Phase() == EnginePhase::Running);

    (void)engine->Close();
    runner.join();
    CHECK(!run_error);
    CHECK(engine->Phase() == EnginePhase::Stopped);
    CHECK(engine->Run(config) == std::errc::device_or_resource_busy);
    CHECK(engine->Phase() == EnginePhase::Stopped);
}

/**
 * CoWaitRunning 是否已经返回：pulse 只在该调用尚未返回的窗口内计数。
 */
enum class WaitWindow { NotStarted, InFlight, Returned };

/**
 * 单 worker：pulse 只在 CoWaitRunning 尚未返回的窗口内计数（阶段握手，不做
 * 事后统计或固定睡眠猜窗口），因此若等待堵住唯一 worker 直到超时，pulse 不可能
 * 在该窗口有进展。到期返回 TimedOut 且不清状态；随后同一等待路径可见真实 Running。
 */
void SingleWorkerProgress()
{
    auto engine = Engine::Create(nullptr);
    std::atomic<WaitWindow> wait_window{WaitWindow::NotStarted};
    std::atomic<int> ticks_in_flight{0};

    auto waiting = Task([&] {
        wait_window.store(WaitWindow::InFlight, std::memory_order_release);
        auto ready = engine->CoWaitRunning({std::chrono::steady_clock::now() + 300ms});

        /**
         * 等待调用一返回立即冻结窗口，之后 pulse 不再计数。
         */
        wait_window.store(WaitWindow::Returned, std::memory_order_release);
        CHECK(!ready);
        CHECK(ready.error().code == ErrorCode::TimedOut);
    });
    auto pulse = Task([&] {
        while (wait_window.load(std::memory_order_acquire) == WaitWindow::NotStarted) {
            bbtco_sleep(1);
        }
        while (wait_window.load(std::memory_order_acquire) == WaitWindow::InFlight) {
            ++ticks_in_flight;
            bbtco_sleep(1);
        }
    });
    Get(waiting);
    Get(pulse);

    /**
     * 只有等待真正挂起、唯一 worker 可继续调度 pulse 时才可能计数。
     */
    CHECK(ticks_in_flight.load() >= 5);
    CHECK(wait_window.load() == WaitWindow::Returned);
    CHECK(engine->Phase() == EnginePhase::Idle);
    CHECK(!engine->LastError());

    std::error_code run_error;
    std::thread runner([&] { run_error = engine->Run(ClientOnlyConfig()); });
    auto ready = Task([&] { CHECK(engine->CoWaitRunning({Soon()})); });
    Get(ready);
    CHECK(engine->Phase() == EnginePhase::Running);
    (void)engine->Close();
    runner.join();
    CHECK(!run_error);
    CHECK(engine->Phase() == EnginePhase::Stopped);
    std::cout << "single-worker wait in-flight pulse ticks=" << ticks_in_flight.load() << "\n";
}

int RunCases(const std::string& name)
{
    if (name == "ready_release") {
        ReadyRelease();
    } else if (name == "run_failure") {
        RunFailure();
    } else if (name == "close_before_run") {
        CloseBeforeRun();
    } else if (name == "control_gate") {
        ControlGate();
    } else if (name == "single_worker") {
        SingleWorkerProgress();
    } else {
        throw std::runtime_error("unknown case: " + name);
    }
    return 0;
}

// clang-format off
}
// clang-format on

int main(int argc, char** argv)
{
    g_main_thread_id = std::this_thread::get_id();
    return bbt_net_test::RunTestProcessExit([&] {
        if (argc != 2) {
            return 2;
        }
        g_bbt_coroutine_config->m_cfg_static_thread_num = 1;
        g_bbt_coroutine_config->m_cfg_stack_size = 128 * 1024;
        g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
        g_scheduler->Start();
        try {
            return RunCases(argv[1]);
        } catch (const std::exception& error) {
            std::cerr << "FAIL " << error.what() << "\n";
            return 1;
        }
    });
}
