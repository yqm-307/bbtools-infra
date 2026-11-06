#include <bbt/infra/config/detail/CloseSupport.hpp>

namespace bbt::infra::config_detail {

bool CloseWaiters::Add(const bbt::coroutine::sync::CoWaiter::SPtr& waiter)
{
    std::lock_guard<std::mutex> lock(m_mtx);
    if (m_closed)
        return false;
    m_waiters.push_back(waiter);
    return true;
}

void CloseWaiters::Remove(const bbt::coroutine::sync::CoWaiter* waiter) noexcept
{
    std::lock_guard<std::mutex> lock(m_mtx);
    for (auto it = m_waiters.begin(); it != m_waiters.end(); ++it) {
        if (it->get() == waiter) {
            m_waiters.erase(it);
            return;
        }
    }
}

void CloseWaiters::CloseAndWakeAll() noexcept
{
    std::vector<bbt::coroutine::sync::CoWaiter::SPtr> wake;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        m_closed = true;
        wake.swap(m_waiters);
    }
    for (auto& waiter : wake)
        waiter->Notify();
}

bool ManagedCloseState::BeginClose() noexcept
{
    int expected = kOpen;
    return m_phase.compare_exchange_strong(expected, kClosing);
}

} // namespace bbt::infra::config_detail
