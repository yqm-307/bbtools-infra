#include <bbt/infra/net/detail/WaitSet.hpp>

#include <algorithm>
#include <new>

namespace bbt::infra::net {

bool WaitSet::Add(WaitLane lane, const bbt::coroutine::sync::CoWaiter::SPtr& waiter)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    const auto index = static_cast<std::size_t>(lane);
    auto& first = m_first.at(index);
    auto& overflow = m_overflow.at(index);
    overflow.erase(
        std::remove_if(overflow.begin(), overflow.end(), [](const auto& item) { return item.expired(); }),
        overflow.end());
    if (first.expired()) {
        first = waiter;
        return true;
    }
    if (overflow.size() + 1 >= kMaxWaiters)
        return false;
    overflow.push_back(waiter);
    return true;
}

void WaitSet::Remove(WaitLane lane, const bbt::coroutine::sync::CoWaiter::SPtr& waiter)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    const auto index = static_cast<std::size_t>(lane);
    if (m_first.at(index).lock() == waiter)
        m_first.at(index).reset();
    auto& overflow = m_overflow.at(index);
    overflow.erase(
        std::remove_if(
            overflow.begin(),
            overflow.end(),
            [&waiter](const auto& item) {
                const auto current = item.lock();
                return !current || current == waiter;
            }),
        overflow.end());
}

void WaitSet::Notify(WaitLane lane)
{
    std::array<bbt::coroutine::sync::CoWaiter::SPtr, kMaxWaiters> snapshot;
    std::size_t count = 0;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        const auto index = static_cast<std::size_t>(lane);
        if (auto first = m_first.at(index).lock())
            snapshot[count++] = std::move(first);
        for (const auto& item : m_overflow.at(index)) {
            if (auto current = item.lock())
                snapshot[count++] = std::move(current);
        }
    }
    for (std::size_t i = 0; i < count; ++i)
        (void)snapshot[i]->Notify();
}

bbt::coroutine::sync::CoWaiter::SPtr CreateWaiter(const std::shared_ptr<TestHooks>& hooks)
{
    if (hooks && hooks->TakeFault(FaultPoint::CreateWaiter))
        throw std::bad_alloc();
    return std::make_shared<bbt::coroutine::sync::CoWaiter>();
}

WaitChangeResult WaitChange(
    WaitSet& set,
    WaitLane lane,
    const bbt::coroutine::WaitOptions& options,
    const std::function<bool()>& ready,
    const std::shared_ptr<TestHooks>& hooks,
    const std::function<void()>& on_added)
{
    auto waiter = CreateWaiter(hooks);
    WaitChangeResult result;
    if (!waiter) {
        result.problem = WaitProblem::CreateFailed;
        return result;
    }
    bool added = false;
    try {
        result.status = waiter->WaitWithCallback(options, [&] {
            added = set.Add(lane, waiter);
            if (!added) {
                result.problem = WaitProblem::RegisterFull;
                (void)waiter->Notify();
                return true;
            }
            if (on_added)
                on_added();
            if (ready())
                (void)waiter->Notify();
            return true;
        });
    } catch (...) {
        if (added)
            set.Remove(lane, waiter);
        throw;
    }
    if (added)
        set.Remove(lane, waiter);
    return result;
}

// clang-format off
}
// clang-format on
