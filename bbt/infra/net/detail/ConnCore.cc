#include <bbt/infra/net/detail/ConnCore.hpp>
#include <bbt/infra/net/detail/Reactor.hpp>
#include <bbt/infra/net/Engine.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <exception>
#include <functional>
#include <stdexcept>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

#include <boost/asio/post.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/steady_timer.hpp>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/detail/CloseWait.hpp>
#include <bbt/infra/net/LoadBalancing.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/detail/CoInternal.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/DriverBinder.hpp>
#include <bbt/infra/net/detail/OptionsNormalize.hpp>
#include <bbt/infra/net/detail/ReactorDriver.hpp>
#include <bbt/infra/net/detail/ReclaimRecord.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>
#include <bbt/infra/net/detail/WakeHub.hpp>

namespace bbt::infra::net {

namespace {

constexpr std::size_t kReadChunk = 16 * 1024;
constexpr int kReadBudget = 8;
constexpr std::size_t kWriteRounds = 8;
constexpr std::size_t kWriteByteBudget = 64 * 1024;

ssize_t SendViewsRaw(int fd, const std::string_view* views, std::size_t count, std::size_t budget)
{
    struct iovec iov[ConnectionState::kMaxViews];
    std::size_t used = 0;
    std::size_t remaining = budget;
    for (std::size_t i = 0; i < count && i < ConnectionState::kMaxViews && remaining != 0; ++i) {
        if (!views[i].empty()) {
            const std::size_t take = std::min(views[i].size(), remaining);
            iov[used].iov_base = const_cast<char*>(views[i].data());
            iov[used].iov_len = take;
            remaining -= take;
            ++used;
        }
    }
    if (used == 0) {
        return 0;
    }
    msghdr message{};
    message.msg_iov = iov;
    message.msg_iovlen = used;
    return ::sendmsg(fd, &message, MSG_DONTWAIT | MSG_NOSIGNAL);
}

// clang-format off
}
// clang-format on

ConnCore::ConnCore(
    std::shared_ptr<ConnectionState> state, TcpWait descriptor, Reactor* owner, std::shared_ptr<RawHandler> handler)
    : m_state(std::move(state))
    , m_desc(std::move(descriptor))
    , m_owner(owner)
    , m_handler(std::move(handler))
{
}

ConnCore::~ConnCore()
{

    /**
     * 兜底收口：正常路径 ShutdownOwner/CancelControl 已关闭 backend 并标记
     * 释放；若核心仍带未关 fd 被析构（异常/收口竞态），这里显式关闭并标记，
     * 不把「released」留给 native 对象析构的隐式时序。
     */
    if (!m_backend_closed) {
        boost::system::error_code ignored;
        m_desc.cancel(ignored);
        m_desc.close(ignored);
        m_backend_closed = true;
    }
    m_state->MarkOwnerReleased();
}

const std::shared_ptr<ConnectionState>& ConnCore::State() const
{
    return m_state;
}

std::size_t ConnCore::RecvHold() const
{
    return m_recv_hold.load(std::memory_order_acquire);
}

bool ConnCore::Closed() const
{
    return m_closed;
}

int ConnCore::Fd()
{
    return m_desc.native_handle();
}

void ConnCore::StartOwner()
{

    /**
     * recv/延迟追加都在读取前预留好存储，取走内核字节后不再首次分配。
     */
    try {
        m_recv_buf = std::make_unique<char[]>(kReadChunk);
        m_state->ring.ReserveDeferred(kReadChunk);
        m_state->ring.ReserveDetached(kReadChunk);
    } catch (...) {
        ShutdownOwner(std::make_error_code(std::errc::not_enough_memory));
        return;
    }
    boost::system::error_code error;
    m_desc.non_blocking(true, error);
    PumpOwner();
}

void ConnCore::PumpOwner()
{
    if (m_closed) {
        return;
    }
    if (m_state->CloseRequested()) {
        ShutdownOwner(m_state->CloseReason());
        return;
    }
    if (m_owner->ImplSealed()) {
        ShutdownOwner(std::make_error_code(std::errc::operation_canceled));
        return;
    }
    PumpWritesOwner();
    if (m_read_paused && m_state->ring.Size() < m_owner->ReadLowWater()) {
        m_read_paused = false;
    }
    if (!m_read_paused && !m_read_stopped) {
        DoReadsOwner();
        if (!m_read_stopped) {
            ArmReadOwner();
        }
    }

    /**
     * raw 消费者此前 TryRead Busy：保护释放已唤 owner，这里合并非递归补投
     * OnRead（无新字节也可取原 ring 数据）。
     */
    if (m_state->ring.TakeRawRetry()) {
        NotifyReadOwner();
    }
}

bool ConnCore::StageHoldToRing()
{
    const std::size_t held = m_recv_hold.load(std::memory_order_acquire);
    if (held == 0) {
        return true;
    }
    if (!m_state->ring.StageDetached(std::string_view(m_recv_buf.get(), held))) {
        return false;
    }
    m_recv_hold.store(0, std::memory_order_release);
    return true;
}

void ConnCore::ShutdownOwner(std::error_code reason)
{
    if (m_closed) {
        return;
    }

    /**
     * 测试：stage 之前
     */
    m_owner->InvokeShutdownBarrier();
    if (!m_backend_closed) {
        if (!StageHoldToRing()) {

            /**
             * 不丢已 recv 字节；下次 owner 推进重试
             */
            return;
        }
        boost::system::error_code ignored;
        m_desc.cancel(ignored);
        m_desc.close(ignored);
        m_backend_closed = true;
    }
    if (!m_ring_closed) {

        /**
         * hold 已在真源后才发布 EOF/结算写
         */
        m_state->MarkClosed(reason);
        m_ring_closed = true;
    }
    reason = m_state->CloseReason();
    DeliverTerminalOwner();
    if (m_handler && !m_onclose_done) {
        Connection connection(m_state);
        m_owner->SafeHandler([this, connection, reason] { m_handler->OnClose(connection, reason); });
        m_onclose_done = true;
    }
    m_closed = true;

    /**
     * fd 已在上方 m_desc.close() 物理关闭，终态与 OnClose 责任已兑现：
     * 此刻起本连接无存活物理资源责任。
     */
    m_state->MarkOwnerReleased();
}

void ConnCore::CancelControl()
{
    if (m_closed) {
        return;
    }
    if (!m_backend_closed) {
        if (!StageHoldToRing()) {

            /**
             * 控制域失败也保留已 recv 字节，不静默丢
             */
            return;
        }
        boost::system::error_code ignored;
        m_desc.cancel(ignored);
        m_desc.close(ignored);
        m_backend_closed = true;
    }
    if (!m_ring_closed) {
        m_state->MarkClosed(std::make_error_code(std::errc::operation_canceled));
        m_ring_closed = true;
    }
    m_closed = true;

    /**
     * 控制域不调用户回调：fd 已物理关闭且内部责任收口，标记释放。
     */
    m_state->MarkOwnerReleased();
}

void ConnCore::ArmReadOwner()
{
    if (m_read_waiting || m_read_stopped || m_closed) {
        return;
    }
    m_read_waiting = true;
    auto self = shared_from_this();
    m_desc.async_wait(TcpWait::wait_read, [self](const boost::system::error_code& error) {
        self->m_read_waiting = false;
        if (self->m_closed) {
            return;
        }
        if (error) {
            if (error == asio::error::operation_aborted) {
                return;
            }
            self->m_read_stopped = true;
            self->m_state->ring.MarkError(FromBoost(error));
            self->NotifyReadOwner();
            return;
        }
        self->DoReadsOwner();
        if (!self->m_read_paused && !self->m_read_stopped) {
            self->ArmReadOwner();
        }
    });
}

void ConnCore::ArmWriteOwner()
{
    if (m_write_waiting || m_closed) {
        return;
    }
    m_write_waiting = true;
    auto self = shared_from_this();
    m_desc.async_wait(TcpWait::wait_write, [self](const boost::system::error_code& error) {
        self->m_write_waiting = false;
        if (self->m_closed) {
            return;
        }
        if (error) {
            if (error == asio::error::operation_aborted) {
                return;
            }
            self->ShutdownOwner(FromBoost(error));
            return;
        }

        /**
         * 测试：readiness 写泵持锁之前
         */
        self->m_owner->InvokeWriteReadyBarrier();
        self->PumpWritesOwner();
    });
}

bool ConnCore::SubmitReceivedOwner(const char* data, std::size_t size)
{
    const std::string_view received(data, size);

    /**
     * TryAppend 在 grant→pop→append 交错下可能重新分配 backing；分配失败
     * 抛 bad_alloc 而非返回 error——异常必须与错误返回同责登记 m_recv_hold，
     * 不能让已 recv 字节随异常穿过 owner 读泵丢失。
     */
    ReadResult appended;
    try {
        appended = m_state->ring.TryAppend(received);
    } catch (...) {
        m_recv_hold.store(size, std::memory_order_release);
        return false;
    }
    if (appended.status == ReadStatus::Busy) {

        /**
         * 测试：Busy 后/deferred 前（正常为空）
         */
        m_owner->InvokeAppendBusyHook();
        if (!m_state->ring.AppendDeferred(received)) {
            m_recv_hold.store(size, std::memory_order_release);
            return false;
        }
    } else if (appended.error) {
        m_recv_hold.store(size, std::memory_order_release);
        return false;
    }
    m_recv_hold.store(0, std::memory_order_release);
    return true;
}

void ConnCore::DoReadsOwner()
{
    if (m_closed || m_read_paused || m_read_stopped || !m_recv_buf) {
        return;
    }
    bool notified = false;
    const std::size_t held = m_recv_hold.load(std::memory_order_acquire);
    if (held > 0) {
        if (!SubmitReceivedOwner(m_recv_buf.get(), held)) {

            /**
             * 保护/容量仍不可用：字节仍可达，等下次推进
             */
            return;
        }
        notified = true;
    }

    /**
     * pending 尚未被消费者合入时不再读内核；不 spin，等消费者取锁 flush。
     */
    if (m_state->ring.PendingSize() != 0) {
        if (notified) {
            NotifyReadOwner();
        }
        return;
    }
    for (int i = 0; i < kReadBudget; ++i) {
        const std::size_t max_bytes = m_state->ring.MaxBytes();

        /**
         * 含 pending
         */
        const std::size_t used = m_state->ring.Size();
        const std::size_t available = max_bytes > used ? max_bytes - used : 0;
        if (available == 0) {

            /**
             * ring 满：消费释放后由 data 通知恢复
             */
            m_read_paused = true;
            break;
        }
        const std::size_t want = std::min(kReadChunk, available);
        const auto grant = m_state->ring.TryReserveForAppend(want);
        if (grant.status == ReadStatus::Busy) {
            break;
        }
        if (grant.granted == 0) {
            m_read_paused = true;
            break;
        }
        const ssize_t bytes = ::recv(m_desc.native_handle(), m_recv_buf.get(), grant.granted, MSG_DONTWAIT);
        if (bytes > 0) {

            /**
             * 测试：在 append 前取得保护
             */
            m_owner->InvokeReadAppendHook();
            if (!SubmitReceivedOwner(m_recv_buf.get(), static_cast<std::size_t>(bytes))) {
                notified = true;

                /**
                 * 字节保留在 m_recv_buf，等保护/容量恢复后再提交
                 */
                break;
            }
            notified = true;

            /**
             * 可能刚把字节放进 pending（append 时 Busy）。pending 是更早
             * 收到的数据，必须等消费者先合入，否则下一次 recv 的新字节会
             * 落在它前面，读序被破坏。入口的同类检查只在循环外，够不着这里。
             *
             * 置 m_read_paused 而不是只 break：pending 未清时内核通常仍可读，
             * 单纯 break 会让 owner 立刻被 level-triggered 读事件重新唤醒，
             * 形成热自旋。暂停后由消费者合入时的 data 通知唤醒 PumpOwner，
             * 经 ring.Size() < ReadLowWater 解除（见 PumpOwner 的恢复分支）。
             */
            if (m_state->ring.PendingSize() != 0) {
                m_read_paused = true;
                break;
            }
            continue;
        }
        if (bytes == 0) {
            m_read_stopped = true;
            m_state->ring.MarkEof();
            NotifyReadOwner();
            return;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }
        if (errno == EINTR) {
            continue;
        }
        m_read_stopped = true;
        m_state->ring.MarkError(std::error_code(errno, std::system_category()));
        NotifyReadOwner();
        return;
    }
    if (notified) {
        NotifyReadOwner();
    }
}

void ConnCore::NotifyReadOwner()
{
    if (!m_handler) {
        return;
    }

    /**
     * 测试：授权点之前
     */
    m_owner->InvokePreAuthBarrier();

    /**
     * 普通用户回调开始授权点：与封口共同线性化的同一原子检查。返回 true
     * 即视为在途，短回调必须完成；此点之后不再有可撤销的无限推迟。
     */
    if (m_owner->ImplSealed()) {
        return;
    }

    /**
     * 测试：已授权
     */
    m_owner->InvokePostAuthBarrier();
    Connection connection(m_state);
    m_owner->SafeHandler([this, connection] { m_handler->OnRead(connection); });
}

void ConnCore::PumpWritesOwner()
{
    if (m_closed) {
        return;
    }
    auto sink = [this](const std::string_view* views, std::size_t count, std::size_t budget) -> ssize_t {
        return SendViewsRaw(m_desc.native_handle(), views, count, budget);
    };
    const auto result = m_state->PumpWrites(kWriteRounds, kWriteByteBudget, sink);
    DeliverTerminalOwner();
    if (m_closed) {
        return;
    }
    if (result.need_write_wait && m_state->HasPendingWrites()) {
        ArmWriteOwner();
    } else if (m_state->HasPendingWrites() && !m_write_waiting) {

        /**
         * 预算耗尽但连接仍可写：重新入 dirty，下一轮继续，不睡死。
         */
        m_owner->MarkDirty(shared_from_this());
    }
}

void ConnCore::DeliverTerminalOwner()
{

    /**
     * 逐项无分配弹出；每项独立隔离用户异常，不吞后续终态/OnClose。
     */
    CompletedWrite item;
    while (m_state->TryPopTerminal(item)) {
        if (!m_handler) {
            continue;
        }
        Connection connection(m_state);
        m_owner->SafeHandler(
            [this, connection, item] { m_handler->OnWrite(connection, WriteResult{item.id, item.sent, item.error}); });
    }
}

// clang-format off
}
// clang-format on
