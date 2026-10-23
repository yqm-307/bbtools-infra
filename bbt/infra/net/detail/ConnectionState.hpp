#pragma once

// 连接共享状态（detail 私有部件，Asio-free）：接收 ring 真源 + 有界发送
// 记账（复用 OutboundQueue 的 FIFO/部分发送）+ 持久状态 + Co 等待订阅。
//
// 责任划分：
//   - 任意线程：AcceptWrite（全量/零接纳）、TryRead/ReadAccess、状态查询、
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
    ConnectionState(std::uint64_t id, std::size_t inbound_max, std::size_t outbound_max,
                    std::size_t max_ops, std::shared_ptr<std::atomic<bool>> service_sealed)
        : m_id(id),
          ring(inbound_max),
          m_queue(std::min<std::size_t>(outbound_max, 64 * 1024), outbound_max, max_ops),
          m_outbound_max(outbound_max),
          m_max_ops(max_ops),
          m_service_sealed(std::move(service_sealed)) {
        m_pending_terminal.set_capacity(std::max<std::size_t>(1, max_ops));
    }

    const std::uint64_t m_id;

    ReceiveRing ring;
    std::function<void()> owner_dirty;  // owner 设置：把本连接加入可达推进集合

    // ---- 接纳（任意线程）----
    std::error_code AcceptWrite(std::string data, std::shared_ptr<WriteOpRecord>& out) {
        if (m_service_sealed && m_service_sealed->load(std::memory_order_acquire)) {
            return make_error_code(NetErrc::Closed);
        }
        std::unique_lock<std::mutex> lock(m_write_mutex);
        if (m_close_requested) {
            return make_error_code(NetErrc::Closed);  // 关闭意图从首次请求起永久拒绝
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
            m_ops.push_back(record);  // 先占活动责任，再接纳字节
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
            m_ops.pop_back();  // 事务撤销：没有字节入队，记录未发布
            return append_error;
        }
        out = std::move(record);
        lock.unlock();
        WakeOwner();
        return {};
    }

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
    template <class Sink>
    SendResult PumpWrites(std::size_t max_rounds, std::size_t byte_budget, Sink&& sink) {
        SendResult result;
        {
            std::unique_lock<std::mutex> lock(m_write_mutex);
            for (std::size_t round = 0; round < max_rounds && result.bytes < byte_budget; ++round) {
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
                    result.capacity_freed = true;  // 部分发送也释放字节额度
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
            result.terminal_changed |= (PendingCountLocked() != 0);
        }
        // terminal（操作/容量）或部分发送释放的字节额度都要锁外通知；容量
        // waiter 与完成 waiter 共用 write hub，按各自谓词复查。
        if (result.terminal_changed || result.capacity_freed) {
            m_write_hub.NotifyAll();
        }
        if (result.terminal_changed) {
            WakeOwner();
        }
        return result;
    }

    // 无分配弹出（vector capacity 在构造时预留）；每次释放 op 额度后锁外通知
    // 容量 waiter，确保 max_ops=1 在 terminal notify 与 drain 之间复查失败后，
    // drain 仍有新事件唤醒。
    bool TryPopTerminal(CompletedWrite& out) {
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

    std::vector<CompletedWrite> DrainTerminal() {
        std::vector<CompletedWrite> out;
        CompletedWrite item;
        while (TryPopTerminal(item)) {
            out.push_back(item);
        }
        return out;
    }

    bool HasPendingWrites() {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        return !m_ops.empty();
    }

    std::size_t ActiveOpCount() {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        return m_ops.size();
    }

    bool CanAcceptWrite(std::size_t bytes) {
        if (m_service_sealed && m_service_sealed->load(std::memory_order_acquire)) {
            return false;
        }
        std::lock_guard<std::mutex> lock(m_write_mutex);
        if (m_close_requested || m_acceptance != AcceptanceState::Open) {
            return false;
        }
        return m_ops.size() + PendingCountLocked() < m_max_ops
            && bytes <= m_outbound_max - m_queue.TotalBytes();
    }

    // 任意线程：请求封口（立即拒绝新写、保留 reason、唤 owner）。读侧最终 EOF
    // 由 owner 在已 recv hold 全部转入 state 真源后调用 MarkClosed 发布。
    void RequestClose(std::error_code reason) {
        {
            std::lock_guard<std::mutex> lock(m_write_mutex);
            if (m_read_closed_published) {
                return;  // 最终关闭后保留已经发布的原因
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

    bool CloseRequested() {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        return m_close_requested;  // 持久 intent，不被写结算暂态清除
    }

    // owner/控制域：已 recv hold 已 stage 后才发布最终读 closed 并结算写。
    void MarkClosed(std::error_code reason) {
        bool notify = false;
        bool publish_read_closed = false;
        {
            std::lock_guard<std::mutex> lock(m_write_mutex);
            if (m_acceptance == AcceptanceState::Closed && m_ops.empty()
                && m_read_closed_published) {
                return;
            }
            if (m_close_requested) {
                reason = m_close_reason;  // owner 发布不能替换首次请求原因
            } else {
                m_close_reason = reason;
            }
            m_close_requested = true;
            while (!m_ops.empty()) {
                auto& op = m_ops.front();
                {
                    std::lock_guard<std::mutex> record_lock(op->mutex);
                    op->terminal = true;
                    op->error = reason;
                }
                m_pending_terminal.push_back(CompletedWrite{op->id, op->sent, reason});
                m_ops.pop_front();
            }
            // 所有已接纳责任结算后才发布 Closed；若中途异常可重入继续。
            m_acceptance = AcceptanceState::Closed;
            if (m_queue.TotalBytes() != 0) {
                m_queue.ConsumeSent(m_queue.TotalBytes());
            }
            m_last_error = reason;
            notify = true;
            publish_read_closed = !m_read_closed_published;
        }
        if (publish_read_closed) {
            ring.MarkClosed();  // 即使 acceptance 已因 sealed 写失败置 Closed，也要发布读 EOF
            std::lock_guard<std::mutex> lock(m_write_mutex);
            m_read_closed_published = true;
        }
        if (notify || publish_read_closed) {
            m_write_hub.NotifyAll();
            m_close_hub.NotifyAll();
        }
        WakeOwner();
    }

    bool Closed() {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        return m_acceptance == AcceptanceState::Closed;
    }
    std::error_code CloseReason() {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        return m_close_reason;
    }
    AcceptanceState Acceptance() {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        return m_acceptance;
    }
    std::size_t MaxOutboundBytes() const noexcept { return m_outbound_max; }

    std::error_code LastWriteError(std::uint64_t* operation = nullptr) {
        std::lock_guard<std::mutex> lock(m_write_mutex);
        if (operation != nullptr) {
            *operation = m_last_error_op;
        }
        return m_last_error;
    }
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> CreateWriteWaiter() {
        return m_write_hub.Create();
    }
    bool AddWriteWaiter(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter) {
        return m_write_hub.Add(waiter);
    }
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> RegisterWriteWaiter() {
        return m_write_hub.Register();
    }
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> RegisterCloseWaiter() {
        return m_close_hub.Register();
    }
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> RegisterReadWaiter() {
        return ring.RegisterWaiter();
    }
    void WakeOwner() {
        if (owner_dirty) {
            owner_dirty();
        }
    }

private:
    std::size_t PendingCountLocked() const noexcept {
        return m_pending_terminal.size();
    }

    bool AttributeSentLocked(std::size_t bytes) {
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

    void FailCurrentLocked(std::error_code error) {
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
            const bool sealed = m_service_sealed
                && m_service_sealed->load(std::memory_order_acquire);
            m_acceptance = sealed ? AcceptanceState::Closed : AcceptanceState::Open;
        }
    }

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
    WakeHub m_write_hub;
    WakeHub m_close_hub;
};

} // namespace bbt::infra::net
