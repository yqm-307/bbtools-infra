#include <bbt/infra/net/detail/ConnectionState.hpp>

namespace bbt::infra::net {

ConnectionState::ConnectionState(std::uint64_t id, std::size_t inbound_max, std::size_t outbound_max,
                                 std::size_t max_ops, std::shared_ptr<std::atomic<bool>> service_sealed,
                                 std::shared_ptr<ReclaimRecord> owner_released)
    : m_id(id), ring(inbound_max), m_queue(std::min<std::size_t>(outbound_max, 64 * 1024), outbound_max, max_ops),
      m_outbound_max(outbound_max), m_max_ops(max_ops), m_service_sealed(std::move(service_sealed)),
      m_owner_released(owner_released ? std::move(owner_released) : std::make_shared<ReclaimRecord>(true))
{
    m_pending_terminal.set_capacity(std::max<std::size_t>(1, max_ops));
}

std::error_code ConnectionState::AcceptWrite(std::string data, std::shared_ptr<WriteOpRecord>& out)
{
    if (m_service_sealed && m_service_sealed->load(std::memory_order_acquire)) {
        return make_error_code(NetErrc::Closed);
    }
    std::unique_lock<std::mutex> lock(m_write_mutex);
    if (m_close_requested) {
        return make_error_code(NetErrc::Closed); // 关闭意图从首次请求起永久拒绝
    }
    if (m_acceptance == AcceptanceState::Closed) {
        return make_error_code(NetErrc::Closed);
    }
    if (m_acceptance == AcceptanceState::Settling) {
        return make_error_code(NetErrc::WriteSettling);
    }
    // 普通 + 零长 + 已终结未派发合计责任上限。
    if (m_ops.size() + PendingCountLocked() >= m_max_ops) {
        return std::make_error_code(std::errc::no_buffer_space);
    }
    if (data.empty()) {
        std::shared_ptr<WriteOpRecord> record;
        try {
            record = std::make_shared<WriteOpRecord>();
            record->id = m_next_op++;
            {
                std::lock_guard<std::mutex> record_lock(record->mutex);
                record->terminal = true;
            }
            m_pending_terminal.push_back(CompletedWrite{record->id, 0, {}});
        } catch (...) {
            return std::make_error_code(std::errc::not_enough_memory);
        }
        out = std::move(record);
        lock.unlock();
        m_write_hub.NotifyAll();
        WakeOwner();
        return {};
    }
    const std::size_t bytes = data.size();
    if (bytes > m_outbound_max - m_queue.TotalBytes()) {
        return std::make_error_code(std::errc::no_buffer_space);
    }
    std::shared_ptr<WriteOpRecord> record;
    try {
        record = std::make_shared<WriteOpRecord>();
        record->id = m_next_op++;
        record->total = bytes;
        record->remaining = bytes;
        m_ops.push_back(record); // 先占活动责任，再接纳字节
    } catch (...) {
        return std::make_error_code(std::errc::not_enough_memory);
    }
    std::error_code append_error;
    try {
        append_error = m_queue.Append(data);
    } catch (...) {
        append_error = std::make_error_code(std::errc::not_enough_memory);
    }
    if (append_error) {
        m_ops.pop_back(); // 事务撤销：没有字节入队，记录未发布
        return append_error;
    }
    out = std::move(record);
    lock.unlock();
    WakeOwner();
    return {};
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
        m_write_hub.NotifyAll();
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
    return !m_ops.empty();
}

std::size_t ConnectionState::ActiveOpCount()
{
    std::lock_guard<std::mutex> lock(m_write_mutex);
    return m_ops.size();
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
    return m_ops.size() + PendingCountLocked() < m_max_ops && bytes <= m_outbound_max - m_queue.TotalBytes();
}

void ConnectionState::RequestClose(std::error_code reason)
{
    {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        if (m_read_closed_published) {
            return; // 最终关闭后保留已经发布的原因
        }
        if (!m_close_requested) {
            // 首次请求：即使 reason 默认 {} 也固定为首请求 reason，后续不覆盖。
            m_close_requested = true;
            m_close_reason = reason;
        }
        if (m_acceptance == AcceptanceState::Open) {
            m_acceptance = AcceptanceState::Settling;
        }
    }
    // 无 owner 的独立 state（detail 单测/无后端）没有 hold/stage 责任，
    // 直接发布最终 closed；Engine 连接由 owner 在 stage 后发布。
    if (!owner_dirty) {
        MarkClosed(reason);
        return;
    }
    WakeOwner();
}

bool ConnectionState::CloseRequested()
{
    std::lock_guard<std::mutex> lock(m_write_mutex);
    return m_close_requested; // 持久 intent，不被写结算暂态清除
}

void ConnectionState::MarkClosed(std::error_code reason)
{
    bool notify = false;
    bool publish_read_closed = false;
    {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        if (m_acceptance == AcceptanceState::Closed && m_ops.empty() && m_read_closed_published) {
            return;
        }
        if (m_close_requested) {
            reason = m_close_reason; // owner 发布不能替换首次请求原因
        } else {
            m_close_reason = reason;
        }
        m_close_requested = true;
        // 未发完写必须以 Done+非空错误结算：默认空 reason 只描述关闭
        // 意图，不能让未完整发送的写冒充本地成功（已完成写早已带着
        // 空错误进 m_pending_terminal，不受此影响）。
        const std::error_code settle = reason ? reason : std::make_error_code(std::errc::operation_canceled);
        bool settled_any = false;
        while (!m_ops.empty()) {
            auto& op = m_ops.front();
            {
                std::lock_guard<std::mutex> record_lock(op->mutex);
                op->terminal = true;
                op->error = settle;
            }
            m_pending_terminal.push_back(CompletedWrite{op->id, op->sent, settle});
            m_ops.pop_front();
            settled_any = true;
        }
        // 所有已接纳责任结算后才发布 Closed；若中途异常可重入继续。
        m_acceptance = AcceptanceState::Closed;
        if (m_queue.TotalBytes() != 0) {
            m_queue.ConsumeSent(m_queue.TotalBytes());
        }
        // 没有新写被结算时，关闭不能抹去此前的发送错误；fire 调用者仍需查询失败事实。
        if (settled_any || !m_last_error) {
            m_last_error = settled_any ? settle : reason;
        }
        notify = true;
        publish_read_closed = !m_read_closed_published;
    }
    if (publish_read_closed) {
        ring.MarkClosed(); // 即使 acceptance 已因 sealed 写失败置 Closed，也要发布读 EOF
        std::lock_guard<std::mutex> lock(m_write_mutex);
        m_read_closed_published = true;
    }
    if (notify || publish_read_closed) {
        m_write_hub.NotifyAll();
        m_close_hub.NotifyAll();
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
    const bool added = m_write_hub.Add(waiter);
    if (added) {
        if (auto& hook = WriteAddHookForTest()) {
            hook();
        }
    }
    return added;
}

bool ConnectionState::AddCloseWaiter(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter)
{
    const bool added = m_close_hub.Add(waiter);
    if (added) {
        if (auto& hook = CloseAddHookForTest()) {
            hook();
        }
    }
    return added;
}

void ConnectionState::SetWriteAddHookForTest(std::function<void()> hook)
{
    WriteAddHookForTest() = std::move(hook);
}

void ConnectionState::SetCloseAddHookForTest(std::function<void()> hook)
{
    CloseAddHookForTest() = std::move(hook);
}

void ConnectionState::WakeOwner()
{
    if (owner_dirty) {
        owner_dirty();
    }
}

std::function<void()>& ConnectionState::WriteAddHookForTest()
{
    static std::function<void()> hook;
    return hook;
}

std::function<void()>& ConnectionState::CloseAddHookForTest()
{
    static std::function<void()> hook;
    return hook;
}

bool ConnectionState::AttributeSentLocked(std::size_t bytes)
{
    bool changed = false;
    while (bytes != 0 && !m_ops.empty()) {
        auto& op = m_ops.front();
        const std::size_t take = bytes < op->remaining ? bytes : op->remaining;
        bool complete = false;
        {
            std::lock_guard<std::mutex> record_lock(op->mutex);
            op->remaining -= take;
            op->sent += take;
            if (op->remaining == 0) {
                op->terminal = true;
                op->error = {};
                complete = true;
            }
        }
        bytes -= take;
        if (complete) {
            m_pending_terminal.push_back(CompletedWrite{op->id, op->sent, {}});
            m_ops.pop_front();
            changed = true;
        }
    }
    return changed;
}

void ConnectionState::FailCurrentLocked(std::error_code error)
{
    if (m_ops.empty()) {
        return;
    }
    m_acceptance = AcceptanceState::Settling;
    m_last_error = error;
    while (!m_ops.empty()) {
        auto& op = m_ops.front();
        {
            std::lock_guard<std::mutex> record_lock(op->mutex);
            op->terminal = true;
            op->error = error;
            op->remaining = 0;
        }
        m_last_error_op = op->id;
        m_pending_terminal.push_back(CompletedWrite{op->id, op->sent, error});
        m_ops.pop_front();
    }
    if (m_queue.TotalBytes() != 0) {
        m_queue.ConsumeSent(m_queue.TotalBytes());
    }
    // 有关闭意图时保持 Settling，等 owner stage 后发布最终终态；普通 I/O
    // 错误只结算写责任，不升级为自动关闭。
    if (!m_close_requested) {
        const bool sealed = m_service_sealed && m_service_sealed->load(std::memory_order_acquire);
        m_acceptance = sealed ? AcceptanceState::Closed : AcceptanceState::Open;
    }
}

std::shared_ptr<bbt::coroutine::sync::CoWaiter> ConnectionState::CreateWriteWaiter()
{
    return m_write_hub.Create();
}

std::shared_ptr<bbt::coroutine::sync::CoWaiter> ConnectionState::RegisterWriteWaiter()
{
    return m_write_hub.Register();
}

std::shared_ptr<bbt::coroutine::sync::CoWaiter> ConnectionState::CreateCloseWaiter()
{
    return m_close_hub.Create();
}

std::shared_ptr<bbt::coroutine::sync::CoWaiter> ConnectionState::RegisterCloseWaiter()
{
    return m_close_hub.Register();
}

std::shared_ptr<bbt::coroutine::sync::CoWaiter> ConnectionState::RegisterReadWaiter()
{
    return ring.RegisterWaiter();
}

} // namespace bbt::infra::net
