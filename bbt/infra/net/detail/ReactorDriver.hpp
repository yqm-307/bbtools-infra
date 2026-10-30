#pragma once

// Reactor driver（detail 私有部件，不是公共 API）。
//
// 契约：docs/architecture/gnet-network-execution.md 与上游 coroutine 核心契约
// （依赖树 HEAD ae2cc1a0ad02de0027c3cab5d36402139f7aabd3）。
// 每个 Reactor owner 占一个已登记 Processer 的 driver 槽，由该 P 的
// OnTick/Wait（非业务协程边界、worker 线程）推进自有 Asio context；
// 本类不创建任何线程。Wake 允许任意线程并发/嵌套调用，为 noexcept、
// 有界、线程安全，用持久 latch + io_context::stop 记忆早到通知；stop
// 只暂停 run 而不丢弃已登记 handler，restart 后继续兑现。
//
// 预算：单次 OnTick / Wait 都最多推进 kDefaultTickBudget 个 Asio handler，
// 且回调内部耗时计入时间预算；预算或时间到即归还 runtime，不用一次
// run_for 抽干整个 timeout。
//
// 异常：用户原始回调由上层（Engine/Conn）就地 try/catch 并写入连接/引擎
// 可观察状态，不允许逃逸到本层。逃逸到本层的异常视为未分类内部故障：
// 记录 LastFailure() 并返回 Failed，进入 runtime 既有 driver Failed 出口，
// 不以计数静默抹平。

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <system_error>
#include <thread>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

#include <bbt/coroutine/detail/interface/IProcesserDriver.hpp>

namespace bbt::infra::net {

class ReactorDriver final : public bbt::coroutine::detail::IProcesserDriver {
public:
    static constexpr std::size_t kDefaultTickBudget = 16;
    static constexpr std::chrono::microseconds kDefaultTickTimeBudget{500};

    explicit ReactorDriver(std::size_t tick_budget = kDefaultTickBudget,
                           std::chrono::microseconds tick_time_budget = kDefaultTickTimeBudget);
    ~ReactorDriver() override;

    ReactorDriver(const ReactorDriver&) = delete;
    ReactorDriver& operator=(const ReactorDriver&) = delete;

    bbt::coroutine::detail::ProcesserDriverResult OnTick(
        bbt::coroutine::detail::ProcesserDriverPhase phase) override;
    bbt::coroutine::detail::ProcesserDriverResult Wait(std::chrono::microseconds timeout) override;
    void Wake() noexcept override;

    // 只许绑定的 owner 线程登记/取消 Asio 资源；不得跨线程保存使用。
    boost::asio::io_context& Context() noexcept { return m_context; }

    // 每轮调度前（BeforeSchedule）在 owner 线程调用的推进钩子（写泵/读恢复）。
    // 必须在绑定后、首次 Tick 前设置；设置后不再变更。
    void SetTickHook(std::function<void()> hook) { m_tick_hook = std::move(hook); }

    // ---- 观察/测试口（detail，不是公共契约） ----
    std::size_t TickCount() const noexcept { return m_ticks.load(std::memory_order_relaxed); }
    std::size_t WaitCount() const noexcept { return m_waits.load(std::memory_order_relaxed); }
    std::size_t WakeCount() const noexcept { return m_wakes.load(std::memory_order_relaxed); }
    std::size_t LastTickHandlers() const noexcept {
        return m_last_tick_handlers.load(std::memory_order_relaxed);
    }
    std::size_t UnhandledFailureCount() const noexcept {
        return m_unhandled_failures.load(std::memory_order_relaxed);
    }
    std::exception_ptr LastFailure() const {
        std::lock_guard<std::mutex> lock(m_observe_mutex);
        return m_last_failure;
    }
    std::thread::id LastTickThread() const {
        std::lock_guard<std::mutex> lock(m_observe_mutex);
        return m_last_tick_thread;
    }
    // OnTick 执行时当前 worker 的 ProcesserId；未命中的登记槽为 0。
    std::uint64_t LastTickProcesserId() const noexcept {
        return m_last_tick_processer_id.load(std::memory_order_relaxed);
    }
    // 当前是否正由绑定的 owner 执行 OnTick/Wait（用于避免自身 Wake 造成的
    // stop/restart churn）。
    bool InOwnerCall() const noexcept {
        return m_owner_call_depth.load(std::memory_order_acquire) != 0;
    }
    // 下一次 OnTick 抛异常，验证 runtime 既有 Failed 出口；一次性。
    void InjectTickFailureOnce() noexcept {
        m_inject_tick_failure.store(true, std::memory_order_release);
    }

private:
    bbt::coroutine::detail::ProcesserDriverResult RunBounded(bool can_block,
                                                             std::chrono::steady_clock::duration budget);
    void RecordUnhandledFailure(std::exception_ptr error);

    const std::size_t m_tick_budget;
    const std::chrono::microseconds m_tick_time_budget;

    boost::asio::io_context m_context;
    using Guard = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;
    Guard m_guard;

    std::atomic<bool> m_wake_latch{false};
    std::atomic<bool> m_inject_tick_failure{false};
    std::atomic<int> m_owner_call_depth{0};

    std::atomic<std::size_t> m_ticks{0};
    std::atomic<std::size_t> m_waits{0};
    std::atomic<std::size_t> m_wakes{0};
    std::atomic<std::size_t> m_unhandled_failures{0};
    std::atomic<std::size_t> m_last_tick_handlers{0};
    std::atomic<std::uint64_t> m_last_tick_processer_id{0};

    mutable std::mutex m_observe_mutex;
    std::thread::id m_last_tick_thread{};
    std::exception_ptr m_last_failure;
    std::function<void()> m_tick_hook;
};

} // namespace bbt::infra::net
