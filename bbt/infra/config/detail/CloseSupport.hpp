/**
 * @file CloseSupport.hpp
 * @brief config watch 私有关闭支撑；登记与封口共用一把锁，关闭不丢失已登记唤醒。
 */
#pragma once

#include <atomic>
#include <mutex>
#include <vector>
#include <bbt/coroutine/sync/CoWaiter.hpp>

namespace bbt::infra::config_detail {
class CloseWaiters {
public:
    bool Add(const bbt::coroutine::sync::CoWaiter::SPtr& waiter);
    void Remove(const bbt::coroutine::sync::CoWaiter* waiter) noexcept;
    void CloseAndWakeAll() noexcept;

private:
    std::mutex m_mtx;
    bool m_closed{false};
    std::vector<bbt::coroutine::sync::CoWaiter::SPtr> m_waiters;
};

class ManagedCloseState {
public:
    bool BeginClose() noexcept;

    void MarkClosed() noexcept { m_phase.store(kClosed); }

    bool IsOpen() const noexcept { return m_phase.load() == kOpen; }

private:
    enum Phase : int { kOpen, kClosing, kClosed };

    std::atomic<int> m_phase{kOpen};
};

// clang-format off
}
// clang-format on
