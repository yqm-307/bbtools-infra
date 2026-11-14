#include <bbt/infra/mongo/detail/MongoSupport.hpp>

namespace bbt::infra::mongo_detail {

result<void> MongoIoEngine::Start()
{
    boost::asio::any_io_executor ex;
    try {
        ex = bbt::coroutine::io::GetExecutor();
    } catch (...) {
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable, "coroutine shared io executor unavailable"));
    }
    if (!ex)
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable, "coroutine shared io executor is empty"));

    std::lock_guard<std::mutex> lk(m_post_mtx);

    /**
     * 幂等：并发 Start 只建立一次
     */
    if (m_io)
        return result<void>::ok();
    m_io.emplace(ex);
    return result<void>::ok();
}

void MongoIoEngine::Seal() noexcept
{
    std::lock_guard<std::mutex> lk(m_post_mtx);
    m_stopped = true;
}

bool MongoIoEngine::Started() const noexcept
{
    std::lock_guard<std::mutex> lk(m_post_mtx);
    return m_io.has_value();
}

bool CloseWaiters::Add(const bbt::coroutine::sync::CoWaiter::SPtr& waiter)
{
    std::lock_guard<std::mutex> lk(m_mtx);
    if (m_closed)
        return false;
    m_waiters.push_back(waiter);
    return true;
}

void CloseWaiters::Remove(const bbt::coroutine::sync::CoWaiter* waiter) noexcept
{
    std::lock_guard<std::mutex> lk(m_mtx);
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
        std::lock_guard<std::mutex> lk(m_mtx);
        m_closed = true;
        wake.swap(m_waiters);
    }
    for (auto& waiter : wake)
        waiter->Notify();
}

bool CloseWaiters::Closed() const noexcept
{
    std::lock_guard<std::mutex> lk(m_mtx);
    return m_closed;
}

bool ManagedCloseState::BeginClose() noexcept
{
    int expected = kOpen;
    return m_phase.compare_exchange_strong(expected, kClosing);
}

void ManagedCloseState::MarkClosed() noexcept
{
    if (m_phase.exchange(kClosed) == kClosed)
        return;
    if (m_hook)
        m_hook();
}

Error WaitStatusToError(bbt::coroutine::WaitStatus status)
{
    using bbt::coroutine::WaitStatus;
    switch (status) {
    case WaitStatus::TimedOut:
        return MakeError(ErrorCode::TimedOut, "request deadline exceeded");
    case WaitStatus::Cancelled:
        return MakeError(ErrorCode::Cancelled, "request cancelled");
    case WaitStatus::InvalidContext:
        return MakeError(ErrorCode::InvalidContext, "request must run in coroutine context");
    case WaitStatus::AlreadyWaiting:
        return MakeError(ErrorCode::InternalError, "internal: duplicate waiter on request signal");
    case WaitStatus::RuntimeUnavailable:
        return MakeError(ErrorCode::RuntimeUnavailable, "coroutine runtime unavailable for request wait");
    case WaitStatus::Completed:
        break;
    }
    return MakeError(ErrorCode::InternalError, "internal: unexpected wait status");
}

result<bbt::coroutine::CoObjectInfo> NewObjectInfo(std::string kind)
{
    try {
        return result<bbt::coroutine::CoObjectInfo>::ok(bbt::coroutine::CreateObjectInfo(std::move(kind), ""));
    } catch (const std::logic_error&) {
        return result<bbt::coroutine::CoObjectInfo>::err(
            MakeError(ErrorCode::RuntimeUnavailable, "coroutine runtime not initialized"));
    }
}

// clang-format off
}
// clang-format on
