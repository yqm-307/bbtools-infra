#include <bbt/infra/net/detail/ReceiveRing.hpp>

namespace bbt::infra::net {

ReceiveRing::ReceiveRing(std::size_t max_bytes) : m_ring(max_bytes), m_max_bytes(max_bytes)
{
}

void ReceiveRing::SetAppendFaultForTest(std::function<void()> hook)
{
    AppendFaultHook() = std::move(hook);
}

void ReceiveRing::ReserveDeferred(std::size_t capacity)
{
    // 预留真实 backing，且与 ring/计数同一协议锁；flush 后仍复用该存储。
    std::lock_guard<std::mutex> lock(m_mutex);
    m_pending.reserve(capacity);
}

void ReceiveRing::ReserveDetached(std::size_t capacity)
{
    std::lock_guard<std::mutex> lock(m_detached_mutex);
    m_detached.reserve(capacity);
}

bool ReceiveRing::StageDetached(std::string_view data)
{
    if (data.empty()) {
        return true;
    }
    {
        std::lock_guard<std::mutex> lock(m_detached_mutex);
        try {
            m_detached.append(data.data(), data.size());
        } catch (...) {
            return false; // 调用者保留 m_recv_hold 责任
        }
        m_detached_size.store(m_detached.size(), std::memory_order_release);
    }
    // 数据已提交：通知失败不得逆转提交事务（不重复提交/不向调用者报未提交）。
    NotifyDataChanged();
    return true;
}

ReceiveRing::AppendGrant ReceiveRing::TryReserveForAppend(std::size_t want)
{
    std::unique_lock<std::mutex> lock(m_mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        return {ReadStatus::Busy, 0, {}};
    }
    const std::size_t used = m_ring.Size() + m_pending.size() + m_detached_size.load(std::memory_order_acquire);
    const std::size_t available = m_max_bytes > used ? m_max_bytes - used : 0;
    const std::size_t granted = want < available ? want : available;
    std::error_code error;
    if (granted == 0) {
        error = std::make_error_code(std::errc::no_buffer_space);
    } else {
        error = m_ring.Reserve(used + granted);
    }
    lock.unlock();
    NotifyRelease();
    if (granted == 0 || error) {
        return {ReadStatus::WouldBlock, 0, error ? error : std::make_error_code(std::errc::no_buffer_space)};
    }
    return {ReadStatus::Ok, granted, {}};
}

ReadResult ReceiveRing::TryAppend(std::string_view data)
{
    if (data.empty()) {
        return {ReadStatus::Ok, 0, {}};
    }
    std::unique_lock<std::mutex> lock(m_mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        return {ReadStatus::Busy, 0, {}};
    }
    std::error_code error;
    try {
        error = m_ring.Append(data);
    } catch (...) {
        error = std::make_error_code(std::errc::not_enough_memory);
    }
    if (!error) {
        m_size.store(m_ring.Size() + m_pending.size(), std::memory_order_release);
    }
    lock.unlock();
    NotifyRelease();
    if (error) {
        return {ReadStatus::WouldBlock, 0, error};
    }
    NotifyDataChanged();
    return {ReadStatus::Ok, data.size(), {}};
}

bool ReceiveRing::AppendDeferred(std::string_view data)
{
    if (data.empty()) {
        return true;
    }
    // 非阻塞：owner 读泵不在 driver handler 里等外部读者保护。Busy 立即
    // 返回 false，由 SubmitReceivedOwner 保留 m_recv_hold，消费者释放
    // （WakeOwnerOnly）后下一轮 owner 推进重试；不锁外读 underlying ring。
    std::unique_lock<std::mutex> lock(m_mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        return false; // Busy：字节仍由 owner 持有，绝不覆盖/丢弃
    }
    try {
        m_pending.append(data.data(), data.size());
    } catch (...) {
        return false; // 真实分配失败：同样保留 owner hold
    }
    m_pending_size.store(m_pending.size(), std::memory_order_release);
    m_size.store(m_ring.Size() + m_pending.size(), std::memory_order_release);
    lock.unlock();
    NotifyDataChanged();
    return true;
}

void ReceiveRing::MarkEof()
{
    m_eof.store(true, std::memory_order_release);
    NotifyDataChanged();
    NotifyRelease();
}

void ReceiveRing::MarkError(std::error_code error)
{
    const std::uint64_t kind = error.category() == NetErrorCategory() ? 2u : 1u;
    m_error.store((kind << 32) | static_cast<std::uint32_t>(error.value()), std::memory_order_release);
    NotifyDataChanged();
    NotifyRelease();
}

void ReceiveRing::MarkClosed()
{
    m_closed.store(true, std::memory_order_release);
    NotifyDataChanged();
    NotifyRelease();
}

ReadResult ReceiveRing::TryRead(std::string& out)
{
    RetryPendingNotification();
    std::unique_lock<std::mutex> lock(m_mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        RequestRawRetry(); // 保护释放后由 owner 补投 OnRead（合并非递归）
        return {ReadStatus::Busy, 0, {}};
    }
    FlushPendingLocked();
    ReadResult result;
    if (m_ring.Size() == 0) {
        result = TerminalLocked();
    } else {
        std::size_t consumed = 0;
        std::error_code error;
        try {
            error = CopyConsumeLocked(out, consumed);
        } catch (...) {
            error = std::make_error_code(std::errc::not_enough_memory);
        }
        if (error) {
            result = {ReadStatus::Error, 0, error};
        } else {
            m_size.store(m_ring.Size() + m_pending_size.load(), std::memory_order_release);
            result = {ReadStatus::Ok, consumed, {}};
        }
    }
    lock.unlock();
    NotifyRelease();
    if (result.status == ReadStatus::Ok) {
        NotifyDataChanged();
    } else {
        WakeOwnerOnly();
    }
    return result;
}

ReadResult ReceiveRing::TryCopyConsume(Buffer& out)
{
    RetryPendingNotification();
    std::unique_lock<std::mutex> lock(m_mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        RequestRawRetry();
        return {ReadStatus::Busy, 0, {}};
    }
    FlushPendingLocked();
    ReadResult result;
    if (m_ring.Size() == 0) {
        result = TerminalLocked();
    } else {
        std::size_t consumed = 0;
        std::error_code error;
        try {
            error = CopyConsumeLocked(out, consumed);
        } catch (...) {
            error = std::make_error_code(std::errc::not_enough_memory);
        }
        if (error) {
            result = {ReadStatus::Error, 0, error};
        } else {
            m_size.store(m_ring.Size() + m_pending_size.load(), std::memory_order_release);
            result = {ReadStatus::Ok, consumed, {}};
        }
    }
    lock.unlock();
    NotifyRelease();
    if (result.status == ReadStatus::Ok) {
        NotifyDataChanged();
    } else {
        WakeOwnerOnly();
    }
    return result;
}

ReadView ReceiveRing::TryView()
{
    RetryPendingNotification();
    std::unique_lock<std::mutex> lock(m_mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        return ReadView(ReadStatus::Busy, {}, {});
    }
    FlushPendingLocked();
    if (m_ring.Size() == 0) {
        const ReadResult terminal = TerminalLocked();
        lock.unlock();
        NotifyRelease();
        return ReadView(terminal.status, {}, terminal.error);
    }
    // 视图只借用 ring 当前连续区域；调用方不得跨越下一次 I/O 或并发 owner 写入。
    const std::string_view view = m_ring.Peek();
    lock.unlock();
    NotifyRelease();
    return ReadView(ReadStatus::Ok, view, {});
}

std::unique_lock<std::mutex> ReceiveRing::TryHoldForTest()
{
    return std::unique_lock<std::mutex>(m_mutex, std::try_to_lock);
}

void ReceiveRing::SetBusyBeforeWaiterHookForTest(std::function<void()> hook)
{
    BusyBeforeWaiterHookForTest() = std::move(hook);
}

void ReceiveRing::SetWouldBlockBeforeWaiterHookForTest(std::function<void()> hook)
{
    WouldBlockBeforeWaiterHookForTest() = std::move(hook);
}

void ReceiveRing::SetAfterAddHookForTest(std::function<void()> hook)
{
    AfterAddHookForTest() = std::move(hook);
}

void ReceiveRing::InvokeBusyBeforeWaiterHookForTest()
{
    if (auto& hook = BusyBeforeWaiterHookForTest()) {
        hook();
    }
}

void ReceiveRing::InvokeWouldBlockBeforeWaiterHookForTest()
{
    if (auto& hook = WouldBlockBeforeWaiterHookForTest()) {
        hook();
    }
}

void ReceiveRing::InvokeAfterAddHookForTest()
{
    if (auto& hook = AfterAddHookForTest()) {
        hook();
    }
}

void ReceiveRing::SetOwnerWake(std::function<void()> wake)
{
    // 启动期只构造一次稳定句柄；通知热路径仅复制 shared_ptr（不分配）。
    auto handle = std::make_shared<const std::function<void()>>(std::move(wake));
    std::lock_guard<std::mutex> lock(m_wake_mutex);
    m_owner_wake = std::move(handle);
}

std::error_code ReceiveRing::ReadError() const
{
    const std::uint64_t packed = m_error.load(std::memory_order_acquire);
    const std::uint64_t kind = packed >> 32;
    if (kind == 0) {
        return {};
    }
    const int value = static_cast<int>(static_cast<std::uint32_t>(packed));
    if (kind == 2) {
        return make_error_code(static_cast<NetErrc>(value));
    }
    return std::error_code(value, std::system_category());
}

std::function<void()>& ReceiveRing::BusyBeforeWaiterHookForTest()
{
    static std::function<void()> hook;
    return hook;
}

std::function<void()>& ReceiveRing::WouldBlockBeforeWaiterHookForTest()
{
    static std::function<void()> hook;
    return hook;
}

std::function<void()>& ReceiveRing::AfterAddHookForTest()
{
    static std::function<void()> hook;
    return hook;
}

std::function<void()>& ReceiveRing::AppendFaultHook()
{
    static std::function<void()> hook;
    return hook;
}

std::error_code ReceiveRing::CopyConsumeLocked(std::string& out, std::size_t& consumed)
{
    const std::size_t available = m_ring.Size();
    EventSnapshot snapshot = m_ring.TakeSnapshot();
    const auto view = snapshot.Peek();
    if (auto& hook = AppendFaultHook()) {
        hook();
    }
    out.append(view.data(), view.size());
    const auto error = m_ring.Discard(available);
    if (!error) {
        consumed = available;
    }
    return error;
}

std::error_code ReceiveRing::CopyConsumeLocked(Buffer& out, std::size_t& consumed)
{
    const std::size_t available = m_ring.Size();
    EventSnapshot snapshot = m_ring.TakeSnapshot();
    const auto view = snapshot.Peek();
    if (!out.Reserve(view.size()) || !out.WriteBytes(view)) {
        return std::make_error_code(std::errc::not_enough_memory);
    }
    const auto error = m_ring.Discard(available);
    if (!error) {
        consumed = available;
    }
    return error;
}

void ReceiveRing::FlushPendingLocked()
{
    if (!m_pending.empty()) {
        const auto error = m_ring.Append(std::string_view(m_pending));
        if (!error) {
            m_pending.clear(); // capacity retained
        }
    }
    // 关闭前 detached（已 recv 的 hold 尾部）在 pending 之后合入，保持 FIFO。
    if (m_detached_size.load(std::memory_order_acquire) != 0) {
        std::lock_guard<std::mutex> lock(m_detached_mutex);
        if (!m_detached.empty()) {
            try {
                if (!m_ring.Append(std::string_view(m_detached))) {
                    m_detached.clear();
                    m_detached_size.store(0, std::memory_order_release);
                }
            } catch (...) {
                // 保留 detached 责任，下一次读取重试
            }
        }
    }
    m_pending_size.store(m_pending.size(), std::memory_order_release);
    m_size.store(m_ring.Size() + m_pending.size(), std::memory_order_release);
}

ReadResult ReceiveRing::TerminalLocked() const
{
    const std::uint64_t packed = m_error.load(std::memory_order_acquire);
    if ((packed >> 32) != 0) {
        return {ReadStatus::Error, 0, ReadError()};
    }
    if (m_eof.load(std::memory_order_acquire) || m_closed.load(std::memory_order_acquire)) {
        return {ReadStatus::Eof, 0, {}};
    }
    return {ReadStatus::WouldBlock, 0, {}};
}

void ReceiveRing::WakeOwnerOnly()
{
    std::shared_ptr<const std::function<void()>> owner_wake;
    {
        std::lock_guard<std::mutex> lock(m_wake_mutex);
        owner_wake = m_owner_wake; // shared_ptr copy：不分配
    }
    if (owner_wake) {
        try {
            (*owner_wake)();
        } catch (...) {
            m_wake_pending.store(true); // 可达推进责任：下次读/终态通知重试
        }
    }
}

void ReceiveRing::RetryPendingNotification()
{
    if (m_notify_pending.exchange(false)) {
        try {
            m_data_hub.NotifyAll();
        } catch (...) {
            m_notify_pending.store(true);
        }
    }
    if (m_wake_pending.exchange(false)) {
        WakeOwnerOnly();
    }
}

void ReceiveRing::NotifyRelease()
{
    m_access_generation.fetch_add(1, std::memory_order_acq_rel);
    m_release_hub.NotifyAll(); // Busy 等待者按代次复查
}

void ReceiveRing::NotifyDataChanged()
{
    std::shared_ptr<const std::function<void()>> owner_wake;
    {
        std::lock_guard<std::mutex> lock(m_wake_mutex);
        owner_wake = m_owner_wake; // 不复制 std::function/不分配
    }
    if (s_notify_fault_once.exchange(false)) {
        m_notify_pending.store(true); // 测试：通知失败仍不影响已提交数据
    } else {
        try {
            m_data_hub.NotifyAll();
        } catch (...) {
            m_notify_pending.store(true);
        }
    }
    if (owner_wake) {
        try {
            (*owner_wake)();
        } catch (...) {
            m_wake_pending.store(true);
        }
    }
}

std::shared_ptr<bbt::coroutine::sync::CoWaiter> ReceiveRing::CreateDataWaiter()
{
    return m_data_hub.Create();
}

bool ReceiveRing::AddDataWaiter(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter)
{
    return m_data_hub.Add(waiter);
}

std::shared_ptr<bbt::coroutine::sync::CoWaiter> ReceiveRing::CreateReleaseWaiter()
{
    return m_release_hub.Create();
}

bool ReceiveRing::AddReleaseWaiter(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter)
{
    return m_release_hub.Add(waiter);
}

std::shared_ptr<bbt::coroutine::sync::CoWaiter> ReceiveRing::RegisterDataWaiter()
{
    return m_data_hub.Register();
}

std::shared_ptr<bbt::coroutine::sync::CoWaiter> ReceiveRing::RegisterWaiter()
{
    return RegisterDataWaiter();
}

std::shared_ptr<bbt::coroutine::sync::CoWaiter> ReceiveRing::RegisterReleaseWaiter()
{
    return m_release_hub.Register();
}

} // namespace bbt::infra::net
