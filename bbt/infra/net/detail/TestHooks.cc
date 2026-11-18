#include <bbt/infra/net/detail/TestHooks.hpp>

#include <utility>

namespace bbt::infra::net {

TestHooks::TestHooks()
{
    for (auto& fault : m_faults)
        fault.store(false);
    for (auto& delay : m_delays)
        delay.store(0);
}

void TestHooks::Arm(HookPoint point, std::function<void()> hook, bool once)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_hooks.at(static_cast<std::size_t>(point)) = {std::move(hook), once};
}

std::function<void()> TestHooks::TakeLocked(Slot& slot)
{
    if (!slot.m_once)
        return slot.m_hook;
    auto hook = std::move(slot.m_hook);
    slot.m_hook = {};
    return hook;
}

void TestHooks::Invoke(HookPoint point)
{
    std::function<void()> hook;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        hook = TakeLocked(m_hooks.at(static_cast<std::size_t>(point)));
    }
    if (hook)
        hook();
}

void TestHooks::ArmOwner(HookPoint point, std::size_t owner, std::function<void()> hook, bool once)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_ownerHooks[owner * static_cast<std::size_t>(HookPoint::Count) + static_cast<std::size_t>(point)] = {
        std::move(hook), once};
}

void TestHooks::InvokeOwner(HookPoint point, std::size_t owner)
{
    std::function<void()> hook;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        const auto it =
            m_ownerHooks.find(owner * static_cast<std::size_t>(HookPoint::Count) + static_cast<std::size_t>(point));
        if (it != m_ownerHooks.end())
            hook = TakeLocked(it->second);
    }
    if (hook)
        hook();
}

void TestHooks::FailOnce(FaultPoint point)
{
    m_faults.at(static_cast<std::size_t>(point)).store(true);
}

bool TestHooks::TakeFault(FaultPoint point)
{
    return m_faults.at(static_cast<std::size_t>(point)).exchange(false);
}

void TestHooks::DelayAt(DelayPoint point, std::chrono::milliseconds delay)
{
    m_delays.at(static_cast<std::size_t>(point)).store(delay.count());
}

std::chrono::milliseconds TestHooks::Delay(DelayPoint point) const
{
    return std::chrono::milliseconds(m_delays.at(static_cast<std::size_t>(point)).load());
}

void TestHooks::OverrideConnectWaitOnce(int status)
{
    m_connectWaitStatus.store(status < 0 ? -1 : status);
}

int TestHooks::TakeConnectWaitStatus()
{
    return m_connectWaitStatus.exchange(-1);
}

void TestHooks::ClearConnectInjection()
{
    m_faults[static_cast<std::size_t>(FaultPoint::ConnectPost)].store(false);
    m_connectWaitStatus.store(-1);
    Arm(HookPoint::AfterConnectPostBeforeOutput, {}, true);
}

} // namespace bbt::infra::net
