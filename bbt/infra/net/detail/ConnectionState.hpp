#pragma once

// 连接共享状态（detail 私有部件，Asio-free）：接收 ring 真源 + 有界发送
// 记账（复用 OutboundQueue 的 FIFO/部分发送）+ 持久状态 + Co 等待订阅。
//
// 责任划分：
//   - 任意线程：AcceptWrite（全量/零接纳）、TryRead/View、状态查询、
//     RequestClose。
//   - owner 线程：PumpWrites 推真实发送、AttributeSent 归属前缀、FailCurrent
//     结算、DrainTerminal 兑现 OnWrite 终态。
//
// 操作记录同步：每个 WriteOpRecord 的字段由 `record.mutex` 保护；writers 在
// 持 `m_write_mutex` 时再锁 record（锁序 m_write_mutex → record.mutex），
// `WriteOperation::Query` 只锁 record.mutex，保证读写共锁、终态只一次。
// 普通 + 零长 + 已终结未派发合计受 max_ops 约束。
//
// 接纳事务：先分配操作记录并入活动表，再 Append 字节；Append 失败弹出记录，
// 不做“从头 ConsumeSent”的错误回滚。

#include <algorithm>

#include <boost/circular_buffer.hpp>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/net/Error.hpp>
#include <bbt/infra/net/Types.hpp>
#include <bbt/infra/net/detail/OutboundQueue.hpp>
#include <bbt/infra/net/detail/ReclaimRecord.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>
#include <bbt/infra/net/detail/WakeHub.hpp>

namespace bbt::infra::net {

struct WriteOpRecord {
    std::uint64_t id{0};
    std::size_t total{0};
    mutable std::mutex mutex;
    bool terminal{false};
    std::size_t sent{0};
    std::size_t remaining{0};
    std::error_code error;
};

struct CompletedWrite {
    std::uint64_t id{0};
    std::size_t sent{0};
    std::error_code error;
};

class ConnectionState {
public:
    // owner_released：本连接物理收口的共享释放记录。Engine 采用路径传入活跃
    // 记录（released=false，责任存活到 fd 真实关闭且 owner 清理）；带句柄
    // CoDial 传入本次预分配的同一记录（与 ReclaimReceipt 同终态）。缺省（如
    // detail 单测的独立 state）从无 backend fd，视为已无责任。
    ConnectionState(std::uint64_t id, std::size_t inbound_max, std::size_t outbound_max, std::size_t max_ops,
                    std::shared_ptr<std::atomic<bool>> service_sealed,
                    std::shared_ptr<ReclaimRecord> owner_released = {});

    // ---- 物理回收观察 ----
    bool OwnerReleased() const noexcept
    {
        return m_owner_released->released.load(std::memory_order_acquire);
    }
    // 本连接回收责任的共享记录：ReclaimReceipt 凭它观察同一真源
    // （Released() 与 Connection::Released() 同终态）。
    const std::shared_ptr<ReclaimRecord>& OwnerReleasedRecord() const noexcept
    {
        return m_owner_released;
    }
    // 仅 owner/控制域在 fd 真实关闭且本连接责任清理后调用；幂等。
    void MarkOwnerReleased() noexcept
    {
        m_owner_released->released.store(true, std::memory_order_release);
    }

    const std::uint64_t m_id;

    ReceiveRing ring;
    std::function<void()> owner_dirty; // owner 设置：把本连接加入可达推进集合

    // ---- 接纳（任意线程）----
    std::error_code AcceptWrite(std::string data, std::shared_ptr<WriteOpRecord>& out);

    // ---- owner：有界发送 ----
    struct SendResult {
        std::size_t bytes{0};
        bool need_write_wait{false};
        bool terminal_changed{false};
        bool capacity_freed{false};
    };
    static constexpr std::size_t kMaxViews = 4;

    // sink(views,count) 必须非阻塞，返回实际发送字节数或 -1（errno 表达）。
    // 单次调用受 rounds 与 byte_budget 双重约束。
    template <class Sink> SendResult PumpWrites(std::size_t max_rounds, std::size_t byte_budget, Sink&& sink);

    // 无分配弹出（vector capacity 在构造时预留）；每次释放 op 额度后锁外通知
    // 容量 waiter，确保 max_ops=1 在 terminal notify 与 drain 之间复查失败后，
    // drain 仍有新事件唤醒。
    bool TryPopTerminal(CompletedWrite& out);

    std::vector<CompletedWrite> DrainTerminal();

    bool HasPendingWrites();

    std::size_t ActiveOpCount();

    bool CanAcceptWrite(std::size_t bytes);

    // 任意线程：请求封口（立即拒绝新写、保留 reason、唤 owner）。读侧最终 EOF
    // 由 owner 在已 recv hold 全部转入 state 真源后调用 MarkClosed 发布。
    void RequestClose(std::error_code reason);

    bool CloseRequested();

    // owner/控制域：已 recv hold 已 stage 后才发布最终读 closed 并结算写。
    void MarkClosed(std::error_code reason);

    bool Closed();
    std::error_code CloseReason();
    AcceptanceState Acceptance();
    std::size_t MaxOutboundBytes() const noexcept
    {
        return m_outbound_max;
    }

    std::error_code LastWriteError(std::uint64_t* operation = nullptr);
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> CreateWriteWaiter();
    bool AddWriteWaiter(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter);
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> RegisterWriteWaiter();
    // close hub 的等待订阅必须走 Create/Add 两段：Add 放进
    // WaitWithCallback 的 on_registered（此时 runtime 已登记事件），
    // 不能直接拿 RegisterCloseWaiter 在登记前注册——否则早到
    // MarkClosed 的 NotifyAll 落在未登记事件上丢失（与 ring/write hub
    // 同一模式）。
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> CreateCloseWaiter();
    bool AddCloseWaiter(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter);
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> RegisterCloseWaiter();
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> RegisterReadWaiter();
    // 测试接缝：对应 hub Add 成功后调用（在 on_registered 内、等待者
    // 已随 runtime 登记），用于确定性制造「等待已挂起后再触发事件」的
    // 屏障，替代 sleep（正常为空）。与 ReceiveRing 的 AfterAddHook 同模式。
    static void SetWriteAddHookForTest(std::function<void()> hook);
    static void SetCloseAddHookForTest(std::function<void()> hook);
    void WakeOwner();

private:
    static std::function<void()>& WriteAddHookForTest();
    static std::function<void()>& CloseAddHookForTest();

    std::size_t PendingCountLocked() const noexcept
    {
        return m_pending_terminal.size();
    }

    bool AttributeSentLocked(std::size_t bytes);

    void FailCurrentLocked(std::error_code error);

    std::mutex m_write_mutex;
    std::deque<std::shared_ptr<WriteOpRecord>> m_ops;
    OutboundQueue m_queue;
    std::size_t m_outbound_max;
    std::size_t m_max_ops;
    std::uint64_t m_next_op{1};
    AcceptanceState m_acceptance{AcceptanceState::Open};
    bool m_close_requested{false};       // 持久关闭 intent（RequestClose 起永久）
    bool m_read_closed_published{false}; // ring 读 EOF 是否已发布
    std::error_code m_last_error;
    std::uint64_t m_last_error_op{0};
    std::error_code m_close_reason;
    boost::circular_buffer<CompletedWrite> m_pending_terminal;
    std::shared_ptr<std::atomic<bool>> m_service_sealed;
    std::shared_ptr<ReclaimRecord> m_owner_released;
    WakeHub m_write_hub;
    WakeHub m_close_hub;
};

} // namespace bbt::infra::net

#include <bbt/infra/net/detail/__TConnectionState.hpp>
