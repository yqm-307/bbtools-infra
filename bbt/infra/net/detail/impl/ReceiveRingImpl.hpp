#pragma once

// 成员实现由声明头末尾包含，保留调用点可见性与 inline。

namespace bbt::infra::net {

inline ReceiveRing::ReceiveRing(std::size_t max_bytes) : m_ring(max_bytes), m_max_bytes(max_bytes)
{
}

inline void ReceiveRing::SetAppendFaultForTest(std::function<void()> hook)
{
    AppendFaultHook() = std::move(hook);
}

inline void ReceiveRing::ReserveDeferred(std::size_t capacity)
{
    // 预留真实 backing，且与 ring/计数同一协议锁；flush 后仍复用该存储。
    std::lock_guard<std::mutex> lock(m_mutex);
    m_pending.reserve(capacity);
}

inline void ReceiveRing::ReserveDetached(std::size_t capacity)
{
    std::lock_guard<std::mutex> lock(m_detached_mutex);
    m_detached.reserve(capacity);
}

inline bool ReceiveRing::StageDetached(std::string_view data)
{
    if (data.empty()) {
        return true;
    }
    {
        std::lock_guard<std::mutex> lock(m_detached_mutex);
        try {
            EnsureTransferableStorage(m_detached, data.size());
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

inline ReceiveRing::AppendGrant ReceiveRing::TryReserveForAppend(std::size_t want)
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

inline ReadResult ReceiveRing::TryAppend(std::string_view data)
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

inline bool ReceiveRing::AppendDeferred(std::string_view data)
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
        EnsureTransferableStorage(m_pending, data.size());
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

inline void ReceiveRing::MarkEof()
{
    m_eof.store(true, std::memory_order_release);
    NotifyDataChanged();
    NotifyRelease();
}

inline void ReceiveRing::MarkError(std::error_code error)
{
    const std::uint64_t kind = error.category() == NetErrorCategory() ? 2u : 1u;
    m_error.store((kind << 32) | static_cast<std::uint32_t>(error.value()), std::memory_order_release);
    NotifyDataChanged();
    NotifyRelease();
}

inline void ReceiveRing::MarkClosed()
{
    m_closed.store(true, std::memory_order_release);
    NotifyDataChanged();
    NotifyRelease();
}

inline ReadResult ReceiveRing::TryRead(std::string& out)
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

inline ReceiveRing::PopAllResult ReceiveRing::TryPopAll()
{
    RetryPendingNotification();
    std::unique_lock<std::mutex> lock(m_mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        RequestRawRetry(); // 与 TryRead 同责：保护释放后补投 OnRead
        return {ReadStatus::Busy, {}, {}};
    }
    // 不做 FlushPendingLocked：pending/detached 以自有 backing 一并移交；
    // FIFO 不变量——pending 非空时 owner 已停 recv，ring 内必然更旧；
    // detached 是关闭前 hold 尾部，最新。
    PopAllResult result;
    const std::size_t total = m_ring.Size() + m_pending.size() + m_detached_size.load(std::memory_order_acquire);
    if (total == 0) {
        const ReadResult terminal = TerminalLocked();
        result.status = terminal.status;
        result.error = terminal.error;
    } else {
        try {
            result.takeout = BuildTakeoutLocked(total);
            result.status = ReadStatus::Ok;
        } catch (...) {
            result.status = ReadStatus::Error;
            result.error = std::make_error_code(std::errc::not_enough_memory);
        }
    }
    lock.unlock();
    NotifyRelease();
    if (result.status == ReadStatus::Ok) {
        NotifyDataChanged(); // 容量释放 + 新真源形态：owner 与 data 等待者都醒
    } else {
        WakeOwnerOnly();
    }
    return result;
}

inline ReceiveRing::AccessAttempt ReceiveRing::TryAccessAttempt()
{
    RetryPendingNotification();
    std::unique_lock<std::mutex> lock(m_mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        return {std::unique_lock<std::mutex>(), ReadStatus::Busy, {}};
    }
    FlushPendingLocked();
    if (m_ring.Size() == 0) {
        const ReadResult terminal = TerminalLocked();
        lock.unlock();
        NotifyRelease();
        WakeOwnerOnly(); // owner 可能在 reserve/append Busy 后退回
        return {std::unique_lock<std::mutex>(), terminal.status, terminal.error};
    }
    return {std::move(lock), ReadStatus::Ok, {}};
}

inline ReceiveRing::AccessAttempt ReceiveRing::TryHoldAttemptForTest()
{
    std::unique_lock<std::mutex> lock(m_mutex, std::try_to_lock);
    if (!lock.owns_lock()) {
        return {std::unique_lock<std::mutex>(), ReadStatus::Busy, {}};
    }
    return {std::move(lock), ReadStatus::Ok, {}};
}

inline std::error_code ReceiveRing::LockedDiscard(std::size_t count)
{
    const auto error = m_ring.Discard(count);
    if (!error) {
        m_size.store(m_ring.Size() + m_pending_size.load(), std::memory_order_release);
    }
    return error;
}

inline std::error_code ReceiveRing::LockedCopyAll(std::string& out) const
{
    EventSnapshot snapshot = m_ring.TakeSnapshot();
    const auto view = snapshot.Peek();
    out.append(view.data(), view.size());
    return {};
}

inline std::error_code ReceiveRing::LockedConsumeInto(std::string& out)
{
    std::size_t consumed = 0;
    std::error_code error;
    try {
        error = CopyConsumeLocked(out, consumed);
    } catch (...) {
        error = std::make_error_code(std::errc::not_enough_memory);
    }
    if (!error) {
        m_size.store(m_ring.Size() + m_pending_size.load(), std::memory_order_release);
    }
    return error;
}

inline void ReceiveRing::AfterProtectionRelease(bool changed)
{
    if (changed) {
        NotifyDataChanged();
    } else {
        // 消费者持锁期间 owner 可能在 reserve/append 上 Busy 而退回；
        // 释放必须唤醒 owner 重试（owner 自身释放不走这里）。
        WakeOwnerOnly();
    }
    NotifyRelease();
}

inline void ReceiveRing::SetBusyBeforeWaiterHookForTest(std::function<void()> hook)
{
    BusyBeforeWaiterHookForTest() = std::move(hook);
}

inline void ReceiveRing::SetWouldBlockBeforeWaiterHookForTest(std::function<void()> hook)
{
    WouldBlockBeforeWaiterHookForTest() = std::move(hook);
}

inline void ReceiveRing::SetAfterAddHookForTest(std::function<void()> hook)
{
    AfterAddHookForTest() = std::move(hook);
}

inline void ReceiveRing::InvokeBusyBeforeWaiterHookForTest()
{
    if (auto& hook = BusyBeforeWaiterHookForTest()) {
        hook();
    }
}

inline void ReceiveRing::InvokeWouldBlockBeforeWaiterHookForTest()
{
    if (auto& hook = WouldBlockBeforeWaiterHookForTest()) {
        hook();
    }
}

inline void ReceiveRing::InvokeAfterAddHookForTest()
{
    if (auto& hook = AfterAddHookForTest()) {
        hook();
    }
}

inline void ReceiveRing::SetOwnerWake(std::function<void()> wake)
{
    // 启动期只构造一次稳定句柄；通知热路径仅复制 shared_ptr（不分配）。
    auto handle = std::make_shared<const std::function<void()>>(std::move(wake));
    std::lock_guard<std::mutex> lock(m_wake_mutex);
    m_owner_wake = std::move(handle);
}

inline std::error_code ReceiveRing::ReadError() const
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

inline std::function<void()>& ReceiveRing::BusyBeforeWaiterHookForTest()
{
    static std::function<void()> hook;
    return hook;
}

inline std::function<void()>& ReceiveRing::WouldBlockBeforeWaiterHookForTest()
{
    static std::function<void()> hook;
    return hook;
}

inline std::function<void()>& ReceiveRing::AfterAddHookForTest()
{
    static std::function<void()> hook;
    return hook;
}

inline std::function<void()>& ReceiveRing::AppendFaultHook()
{
    static std::function<void()> hook;
    return hook;
}

inline std::error_code ReceiveRing::CopyConsumeLocked(std::string& out, std::size_t& consumed)
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

inline void ReceiveRing::EnsureTransferableStorage(std::string& storage, std::size_t need)
{
    const auto* obj = reinterpret_cast<const unsigned char*>(&storage);
    auto is_inline = [obj](const std::string& s) {
        const auto* p = reinterpret_cast<const unsigned char*>(s.data());
        return p >= obj && p < obj + sizeof(s);
    };
    // 先按 need 预备容量（短串的常见路径只需要一次 reserve）；之后按
    // 「data() 是否仍在对象内」判定内联缓冲，与实现无关地收敛到堆——
    // 每次失败只把容量抬高越过当前上限，若干步内必出 SSO。
    storage.reserve(need);
    while (is_inline(storage)) {
        storage.reserve(storage.capacity() + 1);
    }
}

inline std::shared_ptr<ReceiveRing::PopAllTakeout> ReceiveRing::BuildTakeoutLocked(std::size_t total)
{
    auto takeout = std::make_shared<PopAllTakeout>();
    // 固定 4（ring 两段 + pending + detached）：reserve 之后所有 push_back
    // 都不抛——必须在首个变更步骤（TakeStorage）之前完成全部可能分配。
    takeout->segments.reserve(4);
    // ring 存储整体移交（空 ring 时 TakeStorage 也合法，但跳过可省一次分配）。
    if (m_ring.Size() != 0) {
        takeout->ring = m_ring.TakeStorage(); // 此后无更多可失败步骤
        const auto one = takeout->ring->array_one();
        const auto two = takeout->ring->array_two();
        if (one.second != 0) {
            takeout->segments.push_back({one.first, one.second});
        }
        if (two.second != 0) {
            takeout->segments.push_back({two.first, two.second});
        }
    }
    if (!m_pending.empty()) {
        takeout->pending = std::move(m_pending); // 堆存储零拷贝；SSO 内联移动
        takeout->segments.push_back({takeout->pending.data(), takeout->pending.size()});
        // deferred 追加的预分配契约在移交后延续：尽力回留容量，失败只丢
        // 快路径（AppendDeferred 自带 not_enough_memory → owner hold 重试）。
        try {
            m_pending.reserve(takeout->pending.capacity());
        } catch (...) {
        }
    }
    {
        std::lock_guard<std::mutex> detached_lock(m_detached_mutex);
        if (!m_detached.empty()) {
            takeout->detached = std::move(m_detached);
            takeout->segments.push_back({takeout->detached.data(), takeout->detached.size()});
            m_detached_size.store(0, std::memory_order_release);
        }
    }
    m_pending_size.store(0, std::memory_order_release);
    m_size.store(0, std::memory_order_release); // 额度立即释放
    (void)total;                                // 校验用：segments 总长应等于 total（debug 断言留给测试核）
    return takeout;
}

inline void ReceiveRing::FlushPendingLocked()
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

inline ReadResult ReceiveRing::TerminalLocked() const
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

inline void ReceiveRing::WakeOwnerOnly()
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

inline void ReceiveRing::RetryPendingNotification()
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

inline void ReceiveRing::NotifyRelease()
{
    m_access_generation.fetch_add(1, std::memory_order_acq_rel);
    m_release_hub.NotifyAll(); // Busy 等待者按代次复查
}

inline void ReceiveRing::NotifyDataChanged()
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

inline std::shared_ptr<bbt::coroutine::sync::CoWaiter> ReceiveRing::CreateDataWaiter()
{
    return m_data_hub.Create();
}

inline bool ReceiveRing::AddDataWaiter(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter)
{
    return m_data_hub.Add(waiter);
}

inline std::shared_ptr<bbt::coroutine::sync::CoWaiter> ReceiveRing::CreateReleaseWaiter()
{
    return m_release_hub.Create();
}

inline bool ReceiveRing::AddReleaseWaiter(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter)
{
    return m_release_hub.Add(waiter);
}

inline std::shared_ptr<bbt::coroutine::sync::CoWaiter> ReceiveRing::RegisterDataWaiter()
{
    return m_data_hub.Register();
}

inline std::shared_ptr<bbt::coroutine::sync::CoWaiter> ReceiveRing::RegisterWaiter()
{
    return RegisterDataWaiter();
}

inline std::shared_ptr<bbt::coroutine::sync::CoWaiter> ReceiveRing::RegisterReleaseWaiter()
{
    return m_release_hub.Register();
}

} // namespace bbt::infra::net
