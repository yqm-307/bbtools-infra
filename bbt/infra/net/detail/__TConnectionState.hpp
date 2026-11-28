#pragma once

namespace bbt::infra::net {

template <class Sink>
inline ConnectionState::SendResult ConnectionState::PumpWrites(
    std::size_t max_rounds,
    std::size_t byte_budget,
    Sink&& sink)
{
    return _PumpWrites(max_rounds, byte_budget, std::forward<Sink>(sink), false);
}

template <class Sink>
inline ConnectionState::SendResult ConnectionState::TryPumpWrites(
    std::size_t max_rounds,
    std::size_t byte_budget,
    Sink&& sink)
{
    return _PumpWrites(max_rounds, byte_budget, std::forward<Sink>(sink), true);
}

template <class Sink>
inline ConnectionState::SendResult ConnectionState::_PumpWrites(
    std::size_t max_rounds,
    std::size_t byte_budget,
    Sink&& sink,
    bool try_lock)
{
    SendResult result;
    for (std::size_t round = 0; round == 0 || (round < max_rounds && result.bytes < byte_budget); ++round) {
        if (round != 0 && m_testHooks) {
            m_testHooks->Invoke(HookPoint::BetweenWriteRounds);
        }
        /**
         * 撤销取同一把锁：每次持锁只覆盖一轮发送及其归属，借用 views
         * 不跨解锁；撤销取得锁时没有在途发送，已发前缀为终值。
         */
        WriteGuard lock(*this, try_lock);
        if (!lock.OwnsLock()) {
            result.lock_busy = true;
            break;
        }
        result.terminal_changed |= (PendingCountLocked() != 0);
        if (max_rounds == 0 || byte_budget == 0) {
            break;
        }
        std::string_view views[kMaxViews];
        const std::size_t count = m_queue.GatherViews(views, kMaxViews);
        if (count == 0) {
            break;
        }
        std::size_t offered = 0;
        for (std::size_t i = 0; i < count; ++i) {
            offered += views[i].size();
        }
        const std::size_t allowed = std::min(offered, byte_budget - result.bytes);
        const ssize_t sent = sink(views, count, allowed);
        if (sent > 0) {
            m_queue.ConsumeSent(static_cast<std::size_t>(sent));
            result.terminal_changed |= AttributeSentLocked(static_cast<std::size_t>(sent));
            result.bytes += static_cast<std::size_t>(sent);

            /**
             * 部分发送也释放字节额度
             */
            result.capacity_freed = true;
            continue;
        }
        if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            result.need_write_wait = true;
            break;
        }
        if (sent < 0 && errno == EINTR) {
            continue;
        }
        FailCurrentLocked(std::error_code(errno, std::system_category()));
        result.terminal_changed = true;
        break;
    }

    /**
     * terminal（操作/容量）或部分发送释放的字节额度都要锁外通知；容量
     * waiter 与完成 waiter 共用 write hub，按各自谓词复查。
     */
    if (result.terminal_changed || result.capacity_freed) {
        m_waiters.Notify(WaitLane::Write);
    }
    if (result.terminal_changed) {
        WakeOwner();
    }
    if (result.lock_busy && m_testHooks) {
        m_testHooks->Invoke(HookPoint::AfterWriteTryLockBusy);
    }
    return result;
}

// clang-format off
}
// clang-format on
