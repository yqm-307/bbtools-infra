#include <bbt/infra/net/detail/ReactorDriver.hpp>

#include <bbt/coroutine/detail/Processer.hpp>

namespace bbt::infra::net {

ReactorDriver::ReactorDriver(std::size_t tick_budget, std::chrono::microseconds tick_time_budget)
    : m_tick_budget(tick_budget),
      m_tick_time_budget(tick_time_budget),
      m_guard(boost::asio::make_work_guard(m_context)) {
    if (tick_budget == 0) {
        throw std::invalid_argument("ReactorDriver tick budget must be positive");
    }
    if (tick_time_budget < std::chrono::microseconds::zero()) {
        throw std::invalid_argument("ReactorDriver tick time budget must not be negative");
    }
}

ReactorDriver::~ReactorDriver() {
    // 析构前该 driver 必须已同代解绑（Engine 负责）；此处只保证不再有阻塞 run。
    m_context.stop();
}

void ReactorDriver::RecordUnhandledFailure(std::exception_ptr error) {
    m_unhandled_failures.fetch_add(1, std::memory_order_relaxed);
    std::lock_guard<std::mutex> lock(m_observe_mutex);
    if (!m_last_failure) {
        m_last_failure = std::move(error);
    }
}

bbt::coroutine::detail::ProcesserDriverResult ReactorDriver::RunBounded(
    bool can_block, std::chrono::steady_clock::duration budget) {
    if (m_context.stopped()) {
        m_context.restart();
    }
    const auto begin = std::chrono::steady_clock::now();
    const bool bounded_time = m_tick_time_budget > std::chrono::microseconds::zero();
    std::size_t ran = 0;

    while (ran < m_tick_budget) {
        // 早到通知持久记忆：检查后、入睡前到达的 Wake 也让本次立即归还。
        if (m_wake_latch.exchange(false, std::memory_order_acq_rel)) {
            break;
        }
        const auto now = std::chrono::steady_clock::now();
        if (can_block && budget <= std::chrono::steady_clock::duration::zero()) {
            break;
        }
        if (bounded_time && now - begin >= m_tick_time_budget) {
            break;
        }

        std::size_t executed = 0;
        try {
            if (can_block) {
                auto remaining = budget - (now - begin);
                if (remaining < std::chrono::steady_clock::duration::zero()) {
                    remaining = std::chrono::steady_clock::duration::zero();
                }
                // 阻塞至多一个 handler 或到预算界限；不一次 run_for 抽干 timeout。
                executed = m_context.run_one_for(remaining);
            } else {
                executed = m_context.poll_one();
            }
        } catch (...) {
            // 用户原始回调在 Engine/Conn 层就地隔离；逃逸到本层的异常是
            // 未分类内部故障，进入 runtime 既有 Failed 出口，不静默吞掉。
            RecordUnhandledFailure(std::current_exception());
            m_last_tick_handlers.store(ran, std::memory_order_relaxed);
            return bbt::coroutine::detail::ProcesserDriverResult::Failed;
        }
        if (executed == 0) {
            break;  // 预算内无可运行 handler（block 分支为 timeout 到点）
        }
        ++ran;
        // handler 内新入队的 owner 工作（写泵/关闭）立即推进，不等 Wait 超时。
        if (m_tick_hook) {
            m_tick_hook();
        }
        if (bounded_time && std::chrono::steady_clock::now() - begin >= m_tick_time_budget) {
            break;
        }
    }
    m_last_tick_handlers.store(ran, std::memory_order_relaxed);
    return bbt::coroutine::detail::ProcesserDriverResult::Ok;
}

namespace {
struct OwnerCallGuard {
    std::atomic<int>* depth;
    explicit OwnerCallGuard(std::atomic<int>* d) : depth(d) {
        depth->fetch_add(1, std::memory_order_acq_rel);
    }
    ~OwnerCallGuard() { depth->fetch_sub(1, std::memory_order_acq_rel); }
};
} // namespace

bbt::coroutine::detail::ProcesserDriverResult ReactorDriver::OnTick(
    bbt::coroutine::detail::ProcesserDriverPhase phase) {
    OwnerCallGuard owner_guard{&m_owner_call_depth};
    if (m_inject_tick_failure.exchange(false, std::memory_order_acq_rel)) {
        throw std::runtime_error("ReactorDriver injected tick failure");
    }
    m_ticks.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lock(m_observe_mutex);
        m_last_tick_thread = std::this_thread::get_id();
    }
    if (auto local = bbt::coroutine::detail::Processer::TryGetLocalProcesser()) {
        m_last_tick_processer_id.store(local->GetId(), std::memory_order_relaxed);
    } else {
        m_last_tick_processer_id.store(0, std::memory_order_relaxed);
    }

    // AfterSchedule 只计数：真正推进 context 只在每轮调度前一次。
    if (phase == bbt::coroutine::detail::ProcesserDriverPhase::AfterSchedule) {
        return bbt::coroutine::detail::ProcesserDriverResult::Ok;
    }
    // owner 推进钩子：写泵/读恢复。派发前 + 派发后各一次——handler 内新
    // 入队的写/关闭在同一轮 OnTick 内推进，不等到下轮 Wait 超时。
    if (m_tick_hook) {
        m_tick_hook();
    }
    return RunBounded(/*can_block=*/false, std::chrono::steady_clock::duration::zero());
}

bbt::coroutine::detail::ProcesserDriverResult ReactorDriver::Wait(std::chrono::microseconds timeout) {
    OwnerCallGuard owner_guard{&m_owner_call_depth};
    m_waits.fetch_add(1, std::memory_order_relaxed);
    const auto budget =
        std::chrono::duration_cast<std::chrono::steady_clock::duration>(timeout);
    return RunBounded(/*can_block=*/true, budget);
}

void ReactorDriver::Wake() noexcept {
    m_wakes.fetch_add(1, std::memory_order_relaxed);
    // 持久 latch 保证早到；stop 保证阻塞中的 run 及时返回。stop 不丢弃
    // 已登记 handler，restart（RunBounded 开头）后继续兑现。
    m_wake_latch.store(true, std::memory_order_release);
    try {
        m_context.stop();
    } catch (...) {
        // 有界且 noexcept：stop 的异常不外溢。
    }
}

} // namespace bbt::infra::net
