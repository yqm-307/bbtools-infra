#pragma once

// 并发安全接收缓冲（detail 私有部件）：复用 EventRingBuffer 的分段存储与消费，
// 增加最小同步访问面。
//
// 数据/通知分类：
//   - 数据变化（append/EOF/error/close/消费释放容量）通知 data hub + owner wake；
//   - 仅保护释放（TryReserve/空读/一次性操作结束）只递增访问代次
//     并通知 release hub，不通知 data hub、不唤醒 owner。Busy 等待者按代次复查，
//     避免“自己释放保护把自己唤醒”的自激。
//   - 所有 Notify 都在数据锁之外。
//
// 强保证：TryRead 与 Read 都走同一 CopyConsume：先在锁内构造独立全量
// 快照（跨绕环），再追加到调用者 out（std::string 强异常保证），成功后才
// Discard；任一步失败 out/ring 都不变。
//
// staged 责任：owner 取走内核字节后若追加遇 Busy，调用 AppendDeferred 把字节
// 放入本 ring 的 pending 区（预分配容量，不丢）；后续任一消费者取得数据锁时
// 先把 pending 合入 ring。关闭只置终态，pending 尾部仍可被残留 Conn 消费。
//
// 公共面只暴露 ReadStatus/ReadResult/ReadView（Types.hpp 与 ReadView.hpp）；
// 本头是内部部件，不随公共契约冻结。

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/net/Buffer.hpp>
#include <bbt/infra/net/Error.hpp>
#include <bbt/infra/net/ReadView.hpp>
#include <bbt/infra/net/Types.hpp>
#include <bbt/infra/net/detail/EventRingBuffer.hpp>
#include <bbt/infra/net/detail/WakeHub.hpp>

namespace bbt::infra::net {

class ReceiveRing {
public:
    struct AppendGrant {
        ReadStatus status{ReadStatus::WouldBlock};
        std::size_t granted{0};
        std::error_code error;
    };

    explicit ReceiveRing(std::size_t max_bytes);
    ReceiveRing(const ReceiveRing&) = delete;
    ReceiveRing& operator=(const ReceiveRing&) = delete;

    std::size_t MaxBytes() const noexcept
    {
        return m_max_bytes;
    }
    // 逻辑大小 = ring 内 + pending 未合入。
    std::size_t Size() const noexcept
    {
        return m_size.load(std::memory_order_acquire) + m_detached_size.load(std::memory_order_acquire);
    }
    bool Empty() const noexcept
    {
        return Size() == 0;
    }
    std::size_t PendingSize() const noexcept
    {
        return m_pending_size.load(std::memory_order_acquire);
    }

    static void SetNotifyFaultOnceForTest()
    {
        s_notify_fault_once.store(true);
    }

    // 测试接缝：在最终 out.append 前调用；测试抛出以验证 out/ring 双不变。
    static void SetAppendFaultForTest(std::function<void()> hook);

    // owner：为 deferred 追加预留容量（在 StartOwner 一次完成，热路径不再分配）。
    void ReserveDeferred(std::size_t capacity);

    // 与 ConnectionState 同寿命的关闭前暂存：owner 已 recv、但 ring 数据锁被
    // 读者持有时，可在关闭路径无数据锁、无阻塞地提交；读者取锁时合入真源。
    void ReserveDetached(std::size_t capacity);

    bool StageDetached(std::string_view data);

    // owner：按可用额度预留可追加容量。
    AppendGrant TryReserveForAppend(std::size_t want);

    // owner：追加已读字节；Busy 时调用者应改走 AppendDeferred（不丢字节）。
    // 分配失败（grant→pop→append 交错下需要重新 grow backing）折成
    // Error+not_enough_memory 返回，不向 owner 抛异常——异常会绕过
    // SubmitReceivedOwner 的 m_recv_hold 登记分支导致已 recv 字节丢失。
    ReadResult TryAppend(std::string_view data);

    // owner：Busy 时把已从内核取走的字节放入 pending（预分配，热路径不分配）。
    // 返回 false 表示 pending 容量不足（调用者应在下次读前先让消费者合入）。
    bool AppendDeferred(std::string_view data);

    void MarkEof();
    void MarkError(std::error_code error);
    void MarkClosed();

    // 一次性强保证复制+消费。
    ReadResult TryRead(std::string& out);
    ReadResult TryCopyConsume(Buffer& out);

    // 非等待只读查看。返回的 string_view 只借用当前 ring，不跨下一次操作保活。
    ReadView TryView();

    // 仅供仓内并发测试制造 owner/消费者竞争，不属于产品读取面。
    std::unique_lock<std::mutex> TryHoldForTest();

    // 确定性时序测试接缝（正常为空）：分别落在 Busy/WouldBlock 分支创建 waiter
    // 之前、以及 on_registered 加入 hub 之后、复查谓词之前。
    static void SetBusyBeforeWaiterHookForTest(std::function<void()> hook);
    static void SetWouldBlockBeforeWaiterHookForTest(std::function<void()> hook);
    static void SetAfterAddHookForTest(std::function<void()> hook);
    static void InvokeBusyBeforeWaiterHookForTest();
    static void InvokeWouldBlockBeforeWaiterHookForTest();
    static void InvokeAfterAddHookForTest();

    // raw 消费者 TryRead 遇 Busy：登记一次合并非递归的补通知责任，owner 在
    // 保护释放事件上（消费者 Release 已唤 owner）重投 OnRead；即使无新网络字节、
    // recv=EAGAIN 也能取回原 ring 字节。
    void RequestRawRetry()
    {
        m_raw_retry.store(true, std::memory_order_release);
    }
    bool TakeRawRetry()
    {
        return m_raw_retry.exchange(false, std::memory_order_acq_rel);
    }

    std::shared_ptr<bbt::coroutine::sync::CoWaiter> CreateDataWaiter();
    bool AddDataWaiter(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter);
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> CreateReleaseWaiter();
    bool AddReleaseWaiter(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter);
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> RegisterDataWaiter();
    // 兼容旧调用者：默认登记数据等待。
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> RegisterWaiter();
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> RegisterReleaseWaiter();

    std::uint64_t AccessGeneration() const noexcept
    {
        return m_access_generation.load(std::memory_order_acquire);
    }
    void SetOwnerWake(std::function<void()> wake);

    std::error_code ReadError() const;
    bool ReadEof() const noexcept
    {
        return m_eof.load(std::memory_order_acquire);
    }
    bool ReadClosed() const noexcept
    {
        return m_closed.load(std::memory_order_acquire);
    }
    bool HasReadError() const noexcept
    {
        return (m_error.load(std::memory_order_acquire) >> 32) != 0;
    }

private:
    static std::function<void()>& BusyBeforeWaiterHookForTest();
    static std::function<void()>& WouldBlockBeforeWaiterHookForTest();
    static std::function<void()>& AfterAddHookForTest();
    static std::function<void()>& AppendFaultHook();

    // 调用者持数据锁。先全量快照（本地、跨绕环），再 out.append（强保证），
    // 最后 Discard；任一步失败 out/ring 均不变。
    std::error_code CopyConsumeLocked(std::string& out, std::size_t& consumed);
    std::error_code CopyConsumeLocked(Buffer& out, std::size_t& consumed);

    // 调用者持数据锁：把 pending 合入 ring（已收字节，不丢）；成功 clear 但
    // 保留 backing，使下一轮 deferred append 不首次分配。
    void FlushPendingLocked();

    ReadResult TerminalLocked() const;

    void WakeOwnerOnly();

    void RetryPendingNotification();

    // 必须在数据锁之外调用。
    void NotifyRelease();

    // 数据/终态/容量变化：通知 data 等待者并唤醒 owner（恢复读泵）。
    void NotifyDataChanged();

    std::mutex m_mutex;
    EventRingBuffer m_ring;
    const std::size_t m_max_bytes;
    std::atomic<std::size_t> m_size{0};
    std::atomic<bool> m_eof{false};
    std::atomic<bool> m_closed{false};
    std::atomic<std::uint64_t> m_error{0}; // (kind<<32)|value，单次一致发布
    std::atomic<std::uint64_t> m_access_generation{0};
    std::atomic<bool> m_raw_retry{false};
    WakeHub m_data_hub;
    WakeHub m_release_hub;
    std::mutex m_wake_mutex;
    std::shared_ptr<const std::function<void()>> m_owner_wake;
    std::atomic<bool> m_notify_pending{false};
    std::atomic<bool> m_wake_pending{false};
    inline static std::atomic<bool> s_notify_fault_once{false};

    std::string m_pending; // 与 m_ring 同受 m_mutex
    std::atomic<std::size_t> m_pending_size{0};
    std::mutex m_detached_mutex;
    std::string m_detached; // 关闭前已 recv 尾部；与 state 同寿命
    std::atomic<std::size_t> m_detached_size{0};
};

} // namespace bbt::infra::net
