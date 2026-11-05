#pragma once
// config watch 私有关闭支撑；登记与封口共用一把锁，关闭不丢失已登记唤醒。
#include <atomic>
#include <mutex>
#include <vector>
#include <bbt/coroutine/sync/CoWaiter.hpp>

namespace bbt::infra::config_detail {
class CloseWaiters {
public:
    bool Add(const bbt::coroutine::sync::CoWaiter::SPtr& waiter) {
        std::lock_guard<std::mutex> lock(m_mtx);
        if (m_closed)
            return false;
        m_waiters.push_back(waiter);
        return true;
    }
    void Remove(const bbt::coroutine::sync::CoWaiter* waiter) noexcept {
        std::lock_guard<std::mutex> lock(m_mtx);
        for (auto it = m_waiters.begin(); it != m_waiters.end(); ++it) {
            if (it->get() == waiter) {
                m_waiters.erase(it);
                return;
            }
        }
    }
    void CloseAndWakeAll() noexcept {
        std::vector<bbt::coroutine::sync::CoWaiter::SPtr> wake;
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            m_closed = true;
            wake.swap(m_waiters);
        }
        for (auto& waiter : wake)
            waiter->Notify();
    }
private:
    std::mutex m_mtx;
    bool m_closed{false};
    std::vector<bbt::coroutine::sync::CoWaiter::SPtr> m_waiters;
};

class ManagedCloseState {
public:
    bool BeginClose() noexcept {
        int expected = kOpen;
        return m_phase.compare_exchange_strong(expected, kClosing);
    }
    void MarkClosed() noexcept { m_phase.store(kClosed); }
    bool IsOpen() const noexcept { return m_phase.load() == kOpen; }

private:
    enum Phase : int { kOpen, kClosing, kClosed };
    std::atomic<int> m_phase{kOpen};
};

} // namespace bbt::infra::config_detail
