#include <bbt/infra/net/detail/ConnectionState.hpp>

#include <bbt/infra/net/detail/CoInternal.hpp>

namespace bbt::infra::net {

std::tuple<std::uintptr_t, std::size_t, std::size_t, std::uint64_t, bool, bool> ConnectionState::InspectForTest()
{
    const auto observation = Inspect();
    return {
        reinterpret_cast<std::uintptr_t>(this),
        observation.readable_bytes,
        observation.pending_bytes,
        observation.access_generation,
        CloseRequested(),
        CanAcceptWrite(1)};
}

bool ConnectionState::ExerciseReadContentionForTest(const std::function<void()>& during)
{
    return ring.ExerciseReadContentionForTest(during);
}

void ConnectionState::AttachOwner(std::weak_ptr<Reactor> reactor, std::weak_ptr<ConnCore> core)
{
    m_ownerWake._Attach(std::move(reactor), std::move(core));
}

ReadResult ConnectionState::TryRead(std::string& out)
{
    return ring.TryRead(out);
}

std::error_code ConnectionState::TryRead(Buffer& out)
{
    return ring.TryRead(out);
}

ReadView ConnectionState::View()
{
    return ring.TryView();
}

std::error_code ConnectionState::Read(Buffer& out, const bbt::coroutine::WaitOptions& options)
{
    return ring.Read(out, options);
}

std::tuple<std::size_t, std::error_code>
ConnectionState::Write(std::string data, const bbt::coroutine::WaitOptions& options)
{
    const auto wanted = data.size();
    const auto no_buffer = std::make_error_code(std::errc::no_buffer_space);
    const auto settling = make_error_code(NetErrc::WriteSettling);
    WriteWait result;
    std::uint64_t id = 0;
    for (;;) {
        const auto error = SubmitWrite(data, id, &result);
        if (!error)
            break;
        if (error != no_buffer && error != settling)
            return {0, error};
        if (wanted > MaxOutboundBytes())
            return {0, no_buffer};
        if (Closed())
            return {0, make_error_code(NetErrc::Closed)};
        const auto wait = WaitChange(
            m_waiters,
            WaitLane::Write,
            options,
            [&] { return CanAcceptWrite(wanted) || Closed(); },
            m_testHooks,
            [&] {
                if (m_testHooks)
                    m_testHooks->Invoke(HookPoint::AfterWriteRegistered);
            });
        if (wait.problem != WaitProblem::None)
            return {0, no_buffer};
        if (wait.status != bbt::coroutine::WaitStatus::Completed)
            return {0, WaitStatusError(wait.status)};
    }
    /**
     * 接纳后的每个出口均在写锁内完成或撤销，解除对本调用栈的借用。
     * 容量与完成等待复用同一绝对 deadline；已发布终态先于撤销。
     */
    try {
        for (;;) {
            const auto sample = QueryWrite(result);
            if (sample.terminal)
                return {sample.sent, sample.error};
            const auto wait = WaitChange(
                m_waiters,
                WaitLane::Write,
                options,
                [&] { return QueryWrite(result).terminal; },
                m_testHooks,
                [&] {
                    if (m_testHooks)
                        m_testHooks->Invoke(HookPoint::AfterWriteRegistered);
                });
            if (wait.problem != WaitProblem::None)
                return CancelWrite(id, result, no_buffer);
            if (wait.status != bbt::coroutine::WaitStatus::Completed) {
                return CancelWrite(id, result, WaitStatusError(wait.status));
            }
        }
    } catch (...) {
        CancelWrite(id, result, std::make_error_code(std::errc::operation_canceled));
        throw;
    }
}

std::error_code ConnectionState::WaitClosed(const bbt::coroutine::WaitOptions& options)
{
    for (;;) {
        if (Closed()) {
            return {};
        }

        const auto wait = WaitChange(
            m_waiters,
            WaitLane::Close,
            options,
            [&] { return Closed(); },
            m_testHooks,
            [&] {
                if (m_testHooks)
                    m_testHooks->Invoke(HookPoint::AfterCloseRegistered);
            });
        if (wait.problem != WaitProblem::None) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        if (wait.status != bbt::coroutine::WaitStatus::Completed) {
            /**
             * 超时/取消不关闭连接。
             */
            return WaitStatusError(wait.status);
        }
    }
}

void ConnectionState::PrepareReceiveStorage(std::size_t capacity)
{
    ring.ReserveDeferred(capacity);
    ring.ReserveDetached(capacity);
}

ReceiveRing::AppendGrant ConnectionState::ReserveReceive(std::size_t want)
{
    return ring.TryReserveForAppend(want);
}

ReadResult ConnectionState::AppendReceived(std::string_view data)
{
    return ring.TryAppend(data);
}

bool ConnectionState::DeferReceived(std::string_view data)
{
    return ring.AppendDeferred(data);
}

bool ConnectionState::PreserveCloseTail(std::string_view data)
{
    return ring.StageDetached(data);
}

void ConnectionState::FinishRead(std::error_code error)
{
    if (error)
        ring.MarkError(error);
    else
        ring.MarkEof();
}

ReadResult ConnectionState::CopyConsume(Buffer& out)
{
    return ring.TryCopyConsume(out);
}

void ConnectionState::FinishReadForTest()
{
    ring.MarkClosed();
}

std::shared_ptr<bbt::coroutine::sync::CoWaiter> ConnectionState::CreateReadWaiterForTest()
{
    return ring.CreateDataWaiter();
}

bool ConnectionState::AddReadWaiterForTest(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter)
{
    return ring.AddDataWaiter(waiter);
}

bool ConnectionState::TakeReadRetry()
{
    return ring.TakeRawRetry();
}

ConnectionState::Observation ConnectionState::Inspect() const
{
    return {
        ring.Size(),
        ring.PendingSize(),
        ring.MaxBytes(),
        ring.AccessGeneration(),
        ring.ReadError(),
        ring.ReadEof(),
        ring.ReadClosed()};
}

ConnectionState::ConnectionState(
    std::uint64_t id,
    std::size_t inbound_max,
    std::size_t outbound_max,
    std::size_t max_ops,
    std::shared_ptr<std::atomic<bool>> service_sealed,
    std::shared_ptr<ReclaimRecord> owner_released,
    std::shared_ptr<TestHooks> hooks)
    : m_id(id)
    , ring(inbound_max, m_waiters, hooks, &m_ownerWake)
    , m_queue(std::min<std::size_t>(outbound_max, 64 * 1024), outbound_max, max_ops)
    , m_outbound_max(outbound_max)
    , m_max_ops(max_ops)
    , m_service_sealed(std::move(service_sealed))
    , m_owner_released(owner_released ? std::move(owner_released) : std::make_shared<ReclaimRecord>(true))
    , m_testHooks(std::move(hooks))
{
    m_writeIntervals.set_capacity(max_ops);
    m_pending_terminal.set_capacity(std::max<std::size_t>(1, max_ops));
}

std::error_code ConnectionState::AcceptFire(std::string_view data)
{
    std::uint64_t id = 0;
    return SubmitWrite(data, id);
}

std::error_code ConnectionState::SubmitWrite(std::string_view data, std::uint64_t& out_id, WriteWait* waiter)
{
    if (m_service_sealed && m_service_sealed->load(std::memory_order_acquire)) {
        return make_error_code(NetErrc::Closed);
    }
    std::unique_lock<std::mutex> lock(m_write_mutex);
    if (m_close_requested || m_acceptance == AcceptanceState::Closed) {
        return make_error_code(NetErrc::Closed);
    }
    if (m_acceptance == AcceptanceState::Settling) {
        return make_error_code(NetErrc::WriteSettling);
    }
    if (m_writeIntervals.size() + PendingCountLocked() >= m_max_ops
        || data.size() > m_outbound_max - (m_queue.TotalBytes() - m_queue.RevokedBytes())) {
        return std::make_error_code(std::errc::no_buffer_space);
    }
    std::size_t begin = 0, end = 0;
    try {
        if (waiter && !data.empty()) {
            std::size_t observed = 1;
            for (const auto& entry : m_writeIntervals)
                observed += entry.waiter != nullptr;
            m_queue.ReserveRevocations(observed);
        }
        if (const auto error = m_queue.Append(data, &begin, &end))
            return error;
    } catch (const std::bad_alloc&) {
        return std::make_error_code(std::errc::not_enough_memory);
    }
    WriteEntry entry{m_next_op++, begin, end, 0, waiter};
    out_id = entry.id;
    if (waiter)
        *waiter = {};
    if (data.empty()) {
        CompleteLocked(entry, {});
    } else {
        m_writeIntervals.push_back(entry);
    }
    lock.unlock();
    if (data.empty())
        m_waiters.Notify(WaitLane::Write);
    WakeOwner();
    return {};
}

WriteWait ConnectionState::QueryWrite(const WriteWait& waiter)
{
    std::lock_guard<std::mutex> lock(m_write_mutex);
    return waiter;
}

std::tuple<std::size_t, std::error_code>
ConnectionState::CancelWrite(std::uint64_t id, WriteWait& waiter, std::error_code error)
{
    std::unique_lock<std::mutex> lock(m_write_mutex);
    if (!waiter.terminal) {
        /**
         * ponytail: 撤销冷路径在 max_ops 有界账本内线性查找；只有实测该扫描
         * 成为瓶颈时再加 id 索引，避免 raw 提交引入逐写分配。
         */
        const auto entry = std::find_if(
            m_writeIntervals.begin(), m_writeIntervals.end(), [id](const auto& write) { return write.id == id; });
        if (entry != m_writeIntervals.end()) {
            /**
             * PumpWrites 的整个发送批次与终态发布均持同一写锁；取得锁后
             * 没有在途发送。撤销槽在接纳时预留，撤销与解绑不分配也不抛。
             */
            m_queue.RevokeStreamRange(entry->begin + entry->sent, entry->end);
            CompleteLocked(*entry, error);
            m_writeIntervals.erase(entry);
        }
    }
    const auto result = std::make_tuple(waiter.sent, waiter.error);
    lock.unlock();
    m_waiters.Notify(WaitLane::Write);
    WakeOwner();
    return result;
}

void ConnectionState::CompleteLocked(WriteEntry& entry, std::error_code error)
{
    if (entry.waiter)
        *entry.waiter = WriteWait{true, entry.sent, error};
    m_pending_terminal.push_back(CompletedWrite{entry.id, entry.sent, error});
}

bool ConnectionState::TryPopTerminal(CompletedWrite& out)
{
    bool popped = false;
    {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        if (!m_pending_terminal.empty()) {
            out = m_pending_terminal.front();
            m_pending_terminal.pop_front();
            popped = true;
        }
    }
    if (popped) {
        m_waiters.Notify(WaitLane::Write);
    }
    return popped;
}

std::vector<CompletedWrite> ConnectionState::DrainTerminal()
{
    std::vector<CompletedWrite> out;
    CompletedWrite item;
    while (TryPopTerminal(item)) {
        out.push_back(item);
    }
    return out;
}

bool ConnectionState::HasPendingWrites()
{
    std::lock_guard<std::mutex> lock(m_write_mutex);
    return !m_writeIntervals.empty();
}

std::size_t ConnectionState::ActiveOpCount()
{
    std::lock_guard<std::mutex> lock(m_write_mutex);
    return m_writeIntervals.size();
}

bool ConnectionState::CanAcceptWrite(std::size_t bytes)
{
    if (m_service_sealed && m_service_sealed->load(std::memory_order_acquire)) {
        return false;
    }
    std::lock_guard<std::mutex> lock(m_write_mutex);
    if (m_close_requested || m_acceptance != AcceptanceState::Open) {
        return false;
    }
    return m_writeIntervals.size() + PendingCountLocked() < m_max_ops
           && bytes <= m_outbound_max - (m_queue.TotalBytes() - m_queue.RevokedBytes());
}

void ConnectionState::RequestClose(std::error_code reason)
{
    {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        if (m_read_closed_published) {

            /**
             * 最终关闭后保留已经发布的原因
             */
            return;
        }
        if (!m_close_requested) {

            /**
             * 首次请求：即使 reason 默认 {} 也固定为首请求 reason，后续不覆盖。
             */
            m_close_requested = true;
            m_close_reason = reason;
        }
        if (m_acceptance == AcceptanceState::Open) {
            m_acceptance = AcceptanceState::Settling;
        }
    }

    /**
     * 无 owner 的独立 state（detail 单测/无后端）没有 hold/stage 责任，
     * 直接发布最终 closed；Engine 连接由 owner 在 stage 后发布。
     */
    if (!m_ownerWake.Attached()) {
        MarkClosed(reason);
        return;
    }
    WakeOwner();
}

bool ConnectionState::CloseRequested()
{
    std::lock_guard<std::mutex> lock(m_write_mutex);

    /**
     * 持久 intent，不被写结算暂态清除
     */
    return m_close_requested;
}

void ConnectionState::MarkClosed(std::error_code reason)
{
    bool notify = false;
    bool publish_read_closed = false;
    {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        if (m_acceptance == AcceptanceState::Closed && m_writeIntervals.empty() && m_read_closed_published) {
            return;
        }
        if (m_close_requested) {

            /**
             * owner 发布不能替换首次请求原因
             */
            reason = m_close_reason;
        } else {
            m_close_reason = reason;
        }
        m_close_requested = true;

        /**
         * 未发完写必须以 Done+非空错误结算：默认空 reason 只描述关闭
         * 意图，不能让未完整发送的写冒充本地成功（已完成写早已带着
         * 空错误进 m_pending_terminal，不受此影响）。
         */
        const std::error_code settle = reason ? reason : std::make_error_code(std::errc::operation_canceled);
        bool settled_any = false;
        while (!m_writeIntervals.empty()) {
            auto& op = m_writeIntervals.front();
            CompleteLocked(op, settle);
            m_writeIntervals.pop_front();
            settled_any = true;
        }

        /**
         * 所有已接纳责任结算后才发布 Closed；若中途异常可重入继续。
         */
        m_acceptance = AcceptanceState::Closed;
        if (m_queue.TotalBytes() != 0) {
            m_queue.ConsumeSent(m_queue.TotalBytes() - m_queue.RevokedBytes());
        }

        /**
         * 没有新写被结算时，关闭不能抹去此前的发送错误；fire 调用者仍需查询失败事实。
         */
        if (settled_any || !m_last_error) {
            m_last_error = settled_any ? settle : reason;
        }
        notify = true;
        publish_read_closed = !m_read_closed_published;
    }
    if (publish_read_closed) {

        /**
         * 即使 acceptance 已因 sealed 写失败置 Closed，也要发布读 EOF
         */
        ring.MarkClosed();
        std::lock_guard<std::mutex> lock(m_write_mutex);
        m_read_closed_published = true;
    }
    if (notify || publish_read_closed) {
        m_waiters.Notify(WaitLane::Write);
        m_waiters.Notify(WaitLane::Close);
    }
    WakeOwner();
}

bool ConnectionState::Closed()
{
    std::lock_guard<std::mutex> lock(m_write_mutex);
    return m_acceptance == AcceptanceState::Closed;
}

std::error_code ConnectionState::CloseReason()
{
    std::lock_guard<std::mutex> lock(m_write_mutex);
    return m_close_reason;
}

AcceptanceState ConnectionState::Acceptance()
{
    std::lock_guard<std::mutex> lock(m_write_mutex);
    return m_acceptance;
}

std::error_code ConnectionState::LastWriteError(std::uint64_t* operation)
{
    std::lock_guard<std::mutex> lock(m_write_mutex);
    if (operation != nullptr) {
        *operation = m_last_error_op;
    }
    return m_last_error;
}

bool ConnectionState::AddWriteWaiter(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter)
{
    const bool added = m_waiters.Add(WaitLane::Write, waiter);
    if (added) {
        if (m_testHooks)
            m_testHooks->Invoke(HookPoint::AfterWriteRegistered);
    }
    return added;
}

bool ConnectionState::AddCloseWaiter(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter)
{
    const bool added = m_waiters.Add(WaitLane::Close, waiter);
    if (added) {
        if (m_testHooks)
            m_testHooks->Invoke(HookPoint::AfterCloseRegistered);
    }
    return added;
}

void ConnectionState::ArmReadHookForTest(HookPoint point, std::function<void()> hook)
{
    m_testHooks->Arm(point, std::move(hook), false);
}

void ConnectionState::SetWriteAddHookForTest(std::function<void()> hook)
{
    m_testHooks->Arm(HookPoint::AfterWriteRegistered, std::move(hook), false);
}

void ConnectionState::SetCloseAddHookForTest(std::function<void()> hook)
{
    m_testHooks->Arm(HookPoint::AfterCloseRegistered, std::move(hook), false);
}

void ConnectionState::WakeOwner()
{
    m_ownerWake.Wake();
}

bool ConnectionState::AttributeSentLocked(std::size_t bytes)
{
    bool changed = false;
    while (bytes != 0 && !m_writeIntervals.empty()) {
        auto& entry = m_writeIntervals.front();
        const auto take = std::min(bytes, entry.end - entry.begin - entry.sent);
        entry.sent += take;
        bytes -= take;
        if (entry.waiter)
            entry.waiter->sent = entry.sent;
        if (entry.sent == entry.end - entry.begin) {
            CompleteLocked(entry, {});
            m_writeIntervals.pop_front();
            changed = true;
        }
    }
    return changed;
}

void ConnectionState::FailCurrentLocked(std::error_code error)
{
    if (m_writeIntervals.empty())
        return;
    m_acceptance = AcceptanceState::Settling;
    m_last_error = error;
    while (!m_writeIntervals.empty()) {
        auto& entry = m_writeIntervals.front();
        m_last_error_op = entry.id;
        CompleteLocked(entry, error);
        m_writeIntervals.pop_front();
    }
    m_queue.ConsumeSent(m_queue.TotalBytes() - m_queue.RevokedBytes());
    if (!m_close_requested) {
        const bool sealed = m_service_sealed && m_service_sealed->load(std::memory_order_acquire);
        m_acceptance = sealed ? AcceptanceState::Closed : AcceptanceState::Open;
    }
}

std::shared_ptr<bbt::coroutine::sync::CoWaiter> ConnectionState::CreateWriteWaiter()
{
    return CreateWaiter(m_testHooks);
}

std::shared_ptr<bbt::coroutine::sync::CoWaiter> ConnectionState::CreateCloseWaiter()
{
    return CreateWaiter(m_testHooks);
}

// clang-format off
}
// clang-format on
