#include <bbt/infra/net/detail/WakeHub.hpp>

namespace bbt::infra::net {

std::shared_ptr<bbt::coroutine::sync::CoWaiter> WakeHub::Create()
{
    if (s_create_fault_once.exchange(false)) {

        /**
         * 私有 test：等待对象建立单次失败
         */
        throw std::bad_alloc();
    }
    return std::make_shared<bbt::coroutine::sync::CoWaiter>();
}

bool WakeHub::Add(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter)
{
    std::lock_guard<std::mutex> lock(m_mutex);
    PruneLocked();
    if (m_waiters.size() >= kMaxWaiters) {
        return false;
    }
    m_waiters.push_back(waiter);
    return true;
}

std::shared_ptr<bbt::coroutine::sync::CoWaiter> WakeHub::Register()
{
    auto waiter = Create();
    if (!Add(waiter)) {
        return nullptr;
    }
    return waiter;
}

void WakeHub::NotifyAll()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    if (m_waiters.empty()) {

        /**
         * 登记后复查谓词；空 hub 不需要构造 snapshot 或再次加锁。
         */
        return;
    }
    std::array<std::shared_ptr<bbt::coroutine::sync::CoWaiter>, kMaxWaiters> snapshot;
    std::size_t count = 0;
    for (auto& weak : m_waiters) {
        if (count >= kMaxWaiters) {
            break;
        }
        if (auto waiter = weak.lock()) {
            snapshot[count++] = std::move(waiter);
        }
    }
    lock.unlock();
    for (std::size_t i = 0; i < count; ++i) {
        (void)snapshot[i]->Notify();
    }
    std::lock_guard<std::mutex> prune_lock(m_mutex);
    PruneLocked();
}

void WakeHub::PruneLocked()
{
    std::size_t write = 0;
    for (std::size_t read = 0; read < m_waiters.size(); ++read) {
        if (!m_waiters[read].expired()) {
            if (write != read) {
                m_waiters[write] = std::move(m_waiters[read]);
            }
            ++write;
        }
    }
    m_waiters.resize(write);
}

// clang-format off
}
// clang-format on
