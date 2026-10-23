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

#include <bbt/infra/net/LoadBalancing.hpp>
#include <bbt/infra/net/detail/EngineTestAccess.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/detail/CoInternal.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/DriverBinder.hpp>
#include <bbt/infra/net/detail/OptionsNormalize.hpp>
#include <bbt/infra/net/detail/ReactorDriver.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>
#include <bbt/infra/net/detail/WakeHub.hpp>

namespace bbt::infra::net {

namespace {

namespace asio = boost::asio;
using TcpWait = asio::posix::stream_descriptor;

constexpr std::size_t kReadChunk = 16 * 1024;
constexpr int kReadBudget = 8;
constexpr std::size_t kWriteRounds = 8;
constexpr std::size_t kWriteByteBudget = 64 * 1024;
constexpr std::size_t kPumpBudget = 32;
constexpr std::size_t kMaxConnectOps = 1024;

std::error_code FromBoost(const boost::system::error_code& error) {
    if (!error) {
        return {};
    }
    return std::error_code(error.value(), std::system_category());
}

int GetSocketError(int fd) {
    int value = 0;
    socklen_t length = sizeof(value);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &value, &length) != 0) {
        return errno;
    }
    return value;
}

ssize_t SendViewsRaw(int fd, const std::string_view* views, std::size_t count,
                     std::size_t budget) {
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

// 可移动 fd RAII：post 失败/关闭竞争时兜底关闭，交接时 Release。
class FdHandle {
public:
    FdHandle() = default;
    explicit FdHandle(int fd) : m_fd(fd) {}
    ~FdHandle() {
        if (m_fd >= 0) {
            ::close(m_fd);
        }
    }
    FdHandle(const FdHandle&) = delete;
    FdHandle& operator=(const FdHandle&) = delete;
    FdHandle(FdHandle&& other) noexcept : m_fd(other.m_fd) { other.m_fd = -1; }
    FdHandle& operator=(FdHandle&& other) noexcept {
        if (this != &other) {
            if (m_fd >= 0) {
                ::close(m_fd);
            }
            m_fd = other.m_fd;
            other.m_fd = -1;
        }
        return *this;
    }
    int Get() const noexcept { return m_fd; }
    int Release() noexcept {
        const int fd = m_fd;
        m_fd = -1;
        return fd;
    }

private:
    int m_fd{-1};
};

std::error_code ApplyStreamOptions(int fd, const NormalizedOptions& options) {
    std::error_code first;
    auto note = [&first](std::error_code error) {
        if (error && !first) {
            first = error;
        }
    };
    if (options.tcp_no_delay) {
        int one = 1;
        if (::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) != 0) {
            note(std::error_code(errno, std::system_category()));
        }
    }
    if (options.socket_recv_buffer != 0) {
        int value = static_cast<int>(std::min<std::size_t>(options.socket_recv_buffer,
                                                           static_cast<std::size_t>(INT_MAX)));
        if (::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &value, sizeof(value)) != 0) {
            note(std::error_code(errno, std::system_category()));
        }
    }
    if (options.socket_send_buffer != 0) {
        int value = static_cast<int>(std::min<std::size_t>(options.socket_send_buffer,
                                                           static_cast<std::size_t>(INT_MAX)));
        if (::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &value, sizeof(value)) != 0) {
            note(std::error_code(errno, std::system_category()));
        }
    }
    if (options.tcp_keep_alive.count() > 0) {
        int one = 1;
        if (::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one)) != 0) {
            note(std::error_code(errno, std::system_category()));
        }
        int idle = static_cast<int>(std::min<std::int64_t>(options.tcp_keep_alive.count(), INT_MAX));
        if (::setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle)) != 0) {
            note(std::error_code(errno, std::system_category()));
        }
        int interval = static_cast<int>(
            std::min<std::int64_t>(std::max<std::int64_t>(1, options.tcp_keep_interval.count()), INT_MAX));
        if (::setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &interval, sizeof(interval)) != 0) {
            note(std::error_code(errno, std::system_category()));
        }
        int count = static_cast<int>(std::min<std::size_t>(options.tcp_keep_count,
                                                          static_cast<std::size_t>(INT_MAX)));
        if (count > 0 && ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &count, sizeof(count)) != 0) {
            note(std::error_code(errno, std::system_category()));
        }
    }
    return first;
}

std::error_code ApplyListenerOptions(NativeSocket& socket, const NormalizedOptions& options) {
    std::error_code first;
    auto note = [&first](std::error_code error) {
        if (error && !first) {
            first = error;
        }
    };
    if (options.reuse_addr) {
        note(socket.SetReuseAddr(true));
    }
    if (options.reuse_port) {
        note(socket.SetReusePort(true));
    }
    if (options.socket_recv_buffer != 0) {
        note(socket.SetReadBuffer(static_cast<int>(
            std::min<std::size_t>(options.socket_recv_buffer, static_cast<std::size_t>(INT_MAX)))));
    }
    if (options.socket_send_buffer != 0) {
        note(socket.SetWriteBuffer(static_cast<int>(
            std::min<std::size_t>(options.socket_send_buffer, static_cast<std::size_t>(INT_MAX)))));
    }
    return first;
}

} // namespace

class Reactor;
class ConnCore;
struct Listener;
struct ConnectPending;
struct EngineImpl;
struct HandoffReservation;

void DetailNoteHandlerError(EngineImpl* impl);

// 单个 Reactor owner（main 或 sub）。
class Reactor : public std::enable_shared_from_this<Reactor> {
public:
    Reactor(EngineImpl* impl, std::size_t index, std::shared_ptr<ReactorDriver> driver);
    ~Reactor();

    // 端点租约：外部提交只经 TryPost 安全获取当前 driver；解绑后返回 false。
    // 定义在 EngineImpl 完整之后（需要 IsSealed 的门禁）。
    template <class Handler>
    bool TryPost(Handler&& handler);
    // 内部收口命令：允许在 service_sealed 之后由控制域投递（仅 Run 使用）。
    template <class Handler>
    bool TryPostInternal(Handler&& handler);

    // owner 安装：设置 tick 钩子 + 创建/挂 listener。
    std::error_code InstallOnOwner(const std::vector<Address>& addresses);
    std::vector<Address> LocalAddresses() const;
    bool HasListener() const { return !m_listeners.empty(); }

    // owner 封服务 / 控制域冻结 / 解绑后释放 driver。
    void SealServiceOnOwner(std::function<void()> on_done = {});
    void FinishConnectCallback();
    void FinalizeControl();
    void ReleaseDriver();

    // owner：从 native fd 接管（handoff / connect 完成）。
    void AdoptFdOnOwner(FdHandle fd, bool is_connect, std::uint64_t connect_id,
                        std::shared_ptr<HandoffReservation> reservation,
                        bool delayed_delivery = false);
    void BeginConnectOnOwner(const Address& address, std::uint64_t connect_id);
    void StartConnectSocketOnOwner(const Address& address, std::uint64_t connect_id);
    void CancelAllRequestedOnOwner();

    void MarkDirty(const std::shared_ptr<ConnCore>& core);
    void SetDirtyBarrier(std::function<void()> barrier) {
        std::lock_guard<std::mutex> lock(m_barrier_mutex);
        m_dirty_barrier = std::move(barrier);
    }
    void SetShutdownBarrier(std::function<void()> barrier) {
        std::lock_guard<std::mutex> lock(m_shutdown_barrier_mutex);
        m_shutdown_barrier = std::move(barrier);
    }
    void SetWriteReadyBarrier(std::function<void()> barrier) {
        std::lock_guard<std::mutex> lock(m_write_ready_barrier_mutex);
        m_write_ready_barrier = std::move(barrier);
    }
    void InvokeWriteReadyBarrier() {
        std::function<void()> barrier;
        {
            std::lock_guard<std::mutex> lock(m_write_ready_barrier_mutex);
            barrier = std::move(m_write_ready_barrier);
            m_write_ready_barrier = {};
        }
        if (barrier) {
            barrier();
        }
    }
    void InvokeShutdownBarrier() {
        std::function<void()> barrier;
        {
            std::lock_guard<std::mutex> lock(m_shutdown_barrier_mutex);
            barrier = std::move(m_shutdown_barrier);
            m_shutdown_barrier = {};
        }
        if (barrier) {
            barrier();
        }
    }
    void SetRemoveBarrier(std::function<void()> barrier) {
        std::lock_guard<std::mutex> lock(m_remove_barrier_mutex);
        m_remove_barrier = std::move(barrier);
    }
    void InvokeRemoveBarrier() {
        std::function<void()> barrier;
        {
            std::lock_guard<std::mutex> lock(m_remove_barrier_mutex);
            barrier = std::move(m_remove_barrier);
            m_remove_barrier = {};
        }
        if (barrier) {
            barrier();
        }
    }
    void SetPreAuthBarrier(std::function<void()> barrier) {
        std::lock_guard<std::mutex> lock(m_auth_barrier_mutex);
        m_pre_auth_barrier = std::move(barrier);
    }
    void SetPostAuthBarrier(std::function<void()> barrier) {
        std::lock_guard<std::mutex> lock(m_auth_barrier_mutex);
        m_post_auth_barrier = std::move(barrier);
    }
    void InvokePreAuthBarrier() {
        std::function<void()> barrier;
        {
            std::lock_guard<std::mutex> lock(m_auth_barrier_mutex);
            barrier = std::move(m_pre_auth_barrier);
            m_pre_auth_barrier = {};
        }
        if (barrier) {
            barrier();
        }
    }
    void InvokePostAuthBarrier() {
        std::function<void()> barrier;
        {
            std::lock_guard<std::mutex> lock(m_auth_barrier_mutex);
            barrier = std::move(m_post_auth_barrier);
            m_post_auth_barrier = {};
        }
        if (barrier) {
            barrier();
        }
    }
    void SetSubmitBarrier(std::function<void()> barrier) {
        std::lock_guard<std::mutex> lock(m_submit_barrier_mutex);
        m_submit_barrier = std::move(barrier);
    }
    void KickPumpOwner() {
        auto self = shared_from_this();
        (void)TryPostInternal([self] { self->PumpAllConnsOwner(); });
    }
    void CancelConnectOnOwner(std::uint64_t connect_id);
    void ResumeMainAccept();
    std::size_t DirtyCount();
    std::size_t RecvHoldBytes();
    int FirstConnFd();
    // 私有 test 屏障：owner recv 后、ring append 前调用（正常为空）。
    void SetReadAppendHook(std::function<void()> hook) {
        m_read_append_hook = std::move(hook);
    }
    void InvokeReadAppendHook() {
        if (m_read_append_hook) {
            m_read_append_hook();
        }
    }
    // 不构造 std::function（避免回调派发前的分配/抛出）；用户异常仍隔离。
    template <class Callback>
    void SafeHandler(Callback&& callback) {
        try {
            callback();
        } catch (...) {
            DetailNoteHandlerError(m_impl);
        }
    }

    void ArmAcceptAll();
    void ArmAccept(const std::shared_ptr<Listener>& listener);
    void RecordAcceptError(std::error_code error);
    bool ImplSealed() const;
    std::size_t ReadLowWater() const;
    std::size_t ConnCount();
    EngineImpl* Impl() { return m_impl; }

private:
    asio::io_context& Ctx() { return m_driver->Context(); }  // owner 内部使用
    void DoAccept(const std::shared_ptr<Listener>& listener,
                  std::shared_ptr<HandoffReservation> reservation);
    void RemoveConn(const std::shared_ptr<ConnCore>& core);
    void PumpOwner();
    void PumpAllConnsOwner();

    EngineImpl* m_impl;
    std::size_t m_index;
    std::mutex m_driver_mutex;
    std::shared_ptr<ReactorDriver> m_driver;
    std::atomic<bool> m_installed{false};
    std::atomic<bool> m_sealed{false};
    std::vector<std::shared_ptr<Listener>> m_listeners;
    std::vector<std::shared_ptr<ConnectPending>> m_pending_connects;  // owner-only
    std::mutex m_mutex;
    std::unordered_map<std::uint64_t, std::shared_ptr<ConnCore>> m_conns;
    std::mutex m_dirty_mutex;
    ConnCore* m_dirty_head{nullptr};
    ConnCore* m_dirty_tail{nullptr};
    std::size_t m_pending_connect_callbacks{0};  // owner-only
    std::function<void()> m_on_pending_zero;
    std::mutex m_barrier_mutex;
    std::function<void()> m_dirty_barrier;  // 私有 test 屏障（一次性）
    std::mutex m_remove_barrier_mutex;
    std::function<void()> m_remove_barrier;  // 撤链后/擦表前（一次性）
    std::mutex m_shutdown_barrier_mutex;
    std::function<void()> m_shutdown_barrier;  // Shutdown stage 前（一次性）
    std::mutex m_write_ready_barrier_mutex;
    std::function<void()> m_write_ready_barrier;  // 写泵持锁前（一次性）
    std::mutex m_submit_barrier_mutex;
    std::function<void()> m_submit_barrier;  // 私有 test 屏障（一次性）
    std::mutex m_auth_barrier_mutex;
    std::function<void()> m_pre_auth_barrier;
    std::function<void()> m_post_auth_barrier;
    std::function<void()> m_read_append_hook;
};

// owner 侧连接核心：唯一 fd、读/写泵与原始回调派发。
class ConnCore : public std::enable_shared_from_this<ConnCore> {
public:
    ConnCore(std::shared_ptr<ConnectionState> state, TcpWait descriptor, Reactor* owner,
             std::shared_ptr<RawHandler> handler)
        : m_state(std::move(state)),
          m_desc(std::move(descriptor)),
          m_owner(owner),
          m_handler(std::move(handler)) {}

    const std::shared_ptr<ConnectionState>& State() const { return m_state; }
    std::size_t RecvHold() const { return m_recv_hold.load(std::memory_order_acquire); }
    bool Closed() const { return m_closed; }
    int Fd() { return m_desc.native_handle(); }
    std::atomic<bool> m_dirty_queued{false};
    ConnCore* m_dirty_next{nullptr};
    bool m_dirty_linked{false};

    void StartOwner() {
        // recv/延迟追加都在读取前预留好存储，取走内核字节后不再首次分配。
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

    void PumpOwner() {
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
        // raw 消费者此前 TryRead Busy：保护释放已唤 owner，这里合并非递归补投
        // OnRead（无新字节也可取原 ring 数据）。
        if (m_state->ring.TakeRawRetry()) {
            NotifyReadOwner();
        }
    }

    // 关闭前把 m_recv_hold 以不取 ring 数据锁的方式转入 state 寿命的 detached；
    // 失败则保留 hold 并返回 false（调用者本次不关闭，等下一次推进重试）。
    bool StageHoldToRing() {
        const std::size_t held = m_recv_hold.load(std::memory_order_acquire);
        if (held == 0) {
            return true;
        }
        if (!m_state->ring.StageDetached(
                std::string_view(m_recv_buf.get(), held))) {
            return false;
        }
        m_recv_hold.store(0, std::memory_order_release);
        return true;
    }

    // owner：正常收口。分阶段幂等：stage hold → 关 backend → 发布读终态/结算写
    // → 逐项 OnWrite → 唯一 OnClose。中途异常可由下次进入从失败阶段继续，不因
    // 先标 closed/先擦表丢责任。
    void ShutdownOwner(std::error_code reason) {
        if (m_closed) {
            return;
        }
        m_owner->InvokeShutdownBarrier();  // 测试：stage 之前
        if (!m_backend_closed) {
            if (!StageHoldToRing()) {
                return;  // 不丢已 recv 字节；下次 owner 推进重试
            }
            boost::system::error_code ignored;
            m_desc.cancel(ignored);
            m_desc.close(ignored);
            m_backend_closed = true;
        }
        if (!m_ring_closed) {
            m_state->MarkClosed(reason);  // hold 已在真源后才发布 EOF/结算写
            m_ring_closed = true;
        }
        reason = m_state->CloseReason();
        DeliverTerminalOwner();
        if (m_handler && !m_onclose_done) {
            Connection connection(m_state);
            m_owner->SafeHandler([this, connection, reason] {
                m_handler->OnClose(connection, reason);
            });
            m_onclose_done = true;
        }
        m_closed = true;
    }

    // 控制域（解绑后）：关闭 fd、冻结状态、唤醒内部等待者，不调用户回调。
    void CancelControl() {
        if (m_closed) {
            return;
        }
        if (!m_backend_closed) {
            if (!StageHoldToRing()) {
                return;  // 控制域失败也保留已 recv 字节，不静默丢
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
    }

private:
    void ArmReadOwner() {
        if (m_read_waiting || m_read_stopped || m_closed) {
            return;
        }
        m_read_waiting = true;
        auto self = shared_from_this();
        m_desc.async_wait(TcpWait::wait_read,
            [self](const boost::system::error_code& error) {
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

    void ArmWriteOwner() {
        if (m_write_waiting || m_closed) {
            return;
        }
        m_write_waiting = true;
        auto self = shared_from_this();
        m_desc.async_wait(TcpWait::wait_write,
            [self](const boost::system::error_code& error) {
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
                self->m_owner->InvokeWriteReadyBarrier();  // 测试：readiness 写泵持锁之前
                self->PumpWritesOwner();
            });
    }

    // 提交已取内核字节：TryAppend，Busy 走 AppendDeferred；均失败则保留在
    // m_recv_buf（m_recv_hold），下次 owner 推进重试，绝不再 recv 覆盖。
    bool SubmitReceivedOwner(const char* data, std::size_t size) {
        const std::string_view received(data, size);
        const auto appended = m_state->ring.TryAppend(received);
        if (appended.status == ReadStatus::Busy) {
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

    void DoReadsOwner() {
        if (m_closed || m_read_paused || m_read_stopped || !m_recv_buf) {
            return;
        }
        bool notified = false;
        const std::size_t held = m_recv_hold.load(std::memory_order_acquire);
        if (held > 0) {
            if (!SubmitReceivedOwner(m_recv_buf.get(), held)) {
                return;  // 保护/容量仍不可用：字节仍可达，等下次推进
            }
            notified = true;
        }
        // pending 尚未被消费者合入时不再读内核；不 spin，等消费者取锁 flush。
        if (m_state->ring.PendingSize() != 0) {
            if (notified) {
                NotifyReadOwner();
            }
            return;
        }
        for (int i = 0; i < kReadBudget; ++i) {
            const std::size_t max_bytes = m_state->ring.MaxBytes();
            const std::size_t used = m_state->ring.Size();  // 含 pending
            const std::size_t available = max_bytes > used ? max_bytes - used : 0;
            if (available == 0) {
                m_read_paused = true;  // ring 满：消费释放后由 data 通知恢复
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
            const ssize_t bytes = ::recv(m_desc.native_handle(), m_recv_buf.get(),
                                         grant.granted, MSG_DONTWAIT);
            if (bytes > 0) {
                m_owner->InvokeReadAppendHook();  // 测试：在 append 前取得保护
                if (!SubmitReceivedOwner(m_recv_buf.get(), static_cast<std::size_t>(bytes))) {
                    notified = true;
                    break;  // 字节保留在 m_recv_buf，等保护/容量恢复后再提交
                }
                notified = true;
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

    void NotifyReadOwner() {
        if (!m_handler) {
            return;
        }
        m_owner->InvokePreAuthBarrier();   // 测试：授权点之前
        // 普通用户回调开始授权点：与封口共同线性化的同一原子检查。返回 true
        // 即视为在途，短回调必须完成；此点之后不再有可撤销的无限推迟。
        if (m_owner->ImplSealed()) {
            return;
        }
        m_owner->InvokePostAuthBarrier();  // 测试：已授权
        Connection connection(m_state);
        m_owner->SafeHandler([this, connection] { m_handler->OnRead(connection); });
    }

    void PumpWritesOwner() {
        if (m_closed) {
            return;
        }
        auto sink = [this](const std::string_view* views, std::size_t count,
                           std::size_t budget) -> ssize_t {
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
            // 预算耗尽但连接仍可写：重新入 dirty，下一轮继续，不睡死。
            m_owner->MarkDirty(shared_from_this());
        }
    }

    void DeliverTerminalOwner() {
        // 逐项无分配弹出；每项独立隔离用户异常，不吞后续终态/OnClose。
        CompletedWrite item;
        while (m_state->TryPopTerminal(item)) {
            if (!m_handler) {
                continue;
            }
            Connection connection(m_state);
            m_owner->SafeHandler([this, connection, item] {
                m_handler->OnWrite(connection, WriteResult{item.id, item.sent, item.error});
            });
        }
    }

    std::shared_ptr<ConnectionState> m_state;
    TcpWait m_desc;
    Reactor* m_owner;
    std::shared_ptr<RawHandler> m_handler;
    bool m_read_waiting{false};
    bool m_write_waiting{false};
    bool m_read_paused{false};
    bool m_read_stopped{false};
    bool m_closed{false};  // terminal done：OnWrite/OnClose 已兑现
    bool m_backend_closed{false};
    bool m_ring_closed{false};
    bool m_onclose_done{false};
    std::unique_ptr<char[]> m_recv_buf;
    std::atomic<std::size_t> m_recv_hold{0};
};

struct Listener {
    explicit Listener(asio::io_context& context) : desc(context) {}
    TcpWait desc;
    Address local;
    std::atomic<bool> armed{false};
};

struct ConnectPending {
    std::shared_ptr<TcpWait> desc;
    std::shared_ptr<asio::steady_timer> start_timer;
    std::uint64_t connect_id{0};
    bool canceled{false};
};

// handoff 预留：先预留容量再 accept；析构未提交则释放预留。
struct HandoffReservation {
    EngineImpl* impl{nullptr};
    bool counted{false};
    bool committed{false};
    ~HandoffReservation();
    void Commit() { committed = true; }
};

struct EngineImpl : public std::enable_shared_from_this<EngineImpl> {
    explicit EngineImpl(std::shared_ptr<RawHandler> raw_handler)
        : handler(std::move(raw_handler)),
          service_sealed(std::make_shared<std::atomic<bool>>(false)) {}

    std::shared_ptr<RawHandler> handler;
    mutable std::mutex mutex;
    std::condition_variable control_cv;
    EnginePhase phase{EnginePhase::Idle};
    bool close_requested{false};
    std::shared_ptr<std::atomic<bool>> service_sealed;

    mutable std::mutex error_mutex;
    std::error_code last_error;
    std::exception_ptr control_failure;

    std::vector<DriverSlot> slots;
    std::vector<DriverSlot> residual_slots;  // 解绑失败保留的责任
    std::vector<std::shared_ptr<Reactor>> reactors;
    std::shared_ptr<Reactor> main_reactor;
    std::vector<std::shared_ptr<Reactor>> subs;
    std::vector<Address> local_addresses;

    std::atomic<std::uint64_t> next_conn_id{1};
    std::atomic<std::uint64_t> next_connect_id{1};
    std::atomic<std::size_t> conn_count{0};

    std::size_t max_inbound_bytes{1024 * 1024};
    std::size_t max_outbound_bytes{1024 * 1024};
    std::size_t max_ops{1024};
    NormalizedOptions options;
    std::unique_ptr<LoadBalancer> balancer;
    std::mutex balancer_mutex;
    std::size_t connect_limit{1024};
    std::atomic<std::int64_t> connect_delivery_delay_ms{0};
    std::atomic<std::int64_t> connect_start_delay_ms{0};
    std::atomic<bool> cancel_retry_needed{false};
    std::atomic<bool> cancel_post_fail_once{false};  // 私有 test 故障注入
    std::atomic<bool> connect_post_fail_once{false};  // 私有 test：本 Engine 下一次 connect 提交不投递
    std::atomic<int> connect_wait_status_once{-1};    // 私有 test：下一次领取的等待返回值（-1 未武装）
    std::mutex connect_submit_barrier_mutex;          // 私有 test 屏障（一次性）
    std::function<void()> connect_submit_barrier;     // post 成功、写 out_id 之前
    std::atomic<std::int64_t> seal_delay_ms{0};      // 私有 test 延迟
    std::atomic<bool> seal_post_fail_once{false};  // 私有 test 故障注入
    std::atomic<bool> seal_throw_once{false};      // 私有 test：seal handler 抛一次
    std::atomic<bool> seal_error{false};           // seal handler 捕获到异常
    std::atomic<bool> test_wait_driver_failed{false};  // 私有 test：等待期真实 Failed 谓词
    std::atomic<std::int64_t> install_delay_ms{0}; // 私有 test：install owner 延迟

    std::mutex handoff_mutex;
    std::deque<Connection> handoff;
    std::size_t handoff_capacity{0};
    std::size_t handoff_reserved{0};
    bool handoff_enabled{false};
    bool handoff_closed{false};
    std::atomic<bool> resume_pending{false};
    WakeHub accept_hub;

    struct ConnectOp {
        std::mutex mutex;
        bool done{false};
        bool abandoned{false};
        bool waiter_attached{false};
        bool raw_delivered{false};  // raw handler 已真实交付（不得再由 WaitConnect 领取）
        bool receiver_claimed{false};  // 每个 op 只允许一个 WaitConnect 接收者
        std::atomic<bool> cancel_requested{false};
        bool owner_delivering{false};  // owner 已进入授权交付段（seal 不得抢先终态）
        std::shared_ptr<Reactor> reactor;
        Connection connection;
        std::error_code error;
        WakeHub hub;
    };
    std::mutex connect_mutex;
    std::unordered_map<std::uint64_t, std::shared_ptr<ConnectOp>> connects;

    std::atomic<std::size_t> handler_errors{0};

    // ---- service seal ----
    void SealService() {
        if (service_sealed) {
            service_sealed->store(true, std::memory_order_release);
        }
        std::deque<Connection> undelivered;
        {
            std::lock_guard<std::mutex> lock(handoff_mutex);
            handoff_closed = true;
            handoff_reserved = 0;  // 关闭对 queue 与 inflight 共同封口
            undelivered.swap(handoff);
        }
        for (auto& connection : undelivered) {
            connection.RequestClose(std::make_error_code(std::errc::operation_canceled));
        }
        {
            // 不建临时 vector：同一 connect_mutex 下逐项结算并 Notify（不重入
            // connect_mutex），避免 seal 前置路径的分配抛异常。
            std::lock_guard<std::mutex> lock(connect_mutex);
            for (auto& entry : connects) {
                auto& op = entry.second;
                {
                    std::lock_guard<std::mutex> op_lock(op->mutex);
                    if (op->done || op->owner_delivering) {
                        // 已完成/已进入 owner 授权交付段：由原 owner 兑现终态。
                        continue;
                    }
                    op->done = true;
                    op->error = std::make_error_code(std::errc::operation_canceled);
                }
                op->hub.NotifyAll();
            }
        }
        accept_hub.NotifyAll();
    }

    // ---- handoff reservation ----
    std::shared_ptr<HandoffReservation> TryReserveHandoff() {
        std::lock_guard<std::mutex> lock(handoff_mutex);
        if (handoff_closed || IsSealed()) {
            return nullptr;  // RequestClose/Seal 与 accept 共门
        }
        if (!handoff_enabled) {
            auto reservation = std::make_shared<HandoffReservation>();
            reservation->impl = this;
            reservation->counted = false;
            return reservation;
        }
        if (handoff_closed || handoff.size() + handoff_reserved >= handoff_capacity) {
            return nullptr;
        }
        ++handoff_reserved;
        auto reservation = std::make_shared<HandoffReservation>();
        reservation->impl = this;
        reservation->counted = true;
        return reservation;
    }
    void ReleaseHandoffReservation() {
        {
            std::lock_guard<std::mutex> lock(handoff_mutex);
            if (handoff_closed) {
                return;
            }
            if (handoff_reserved > 0) {
                --handoff_reserved;
            }
        }
        ResumeAcceptIfFree();
    }
    bool CommitHandoff(Connection connection, const std::shared_ptr<HandoffReservation>& reservation) {
        {
            std::lock_guard<std::mutex> lock(handoff_mutex);
            if (!handoff_enabled || handoff_closed) {
                return false;
            }
            if (reservation) {
                reservation->committed = true;  // 只扣减一次
            }
            if (handoff_reserved > 0) {
                --handoff_reserved;
            }
            handoff.push_back(std::move(connection));
        }
        accept_hub.NotifyAll();
        return true;
    }
    void ResumeAcceptIfFree() {
        std::shared_ptr<Reactor> main;
        {
            std::lock_guard<std::mutex> lock(mutex);
            main = main_reactor;
        }
        if (!main) {
            return;
        }
        if (resume_pending.exchange(true)) {
            return;
        }
        if (!main->TryPost([main, this] {
                resume_pending.store(false);
                main->ArmAcceptAll();
            })) {
            resume_pending.store(false);
        }
    }

    std::shared_ptr<ConnectOp> FindConnect(std::uint64_t id) {
        std::lock_guard<std::mutex> lock(connect_mutex);
        const auto it = connects.find(id);
        return it == connects.end() ? nullptr : it->second;
    }

    // 同一 op 锁内仲裁「交付 / 回收」。返回是否已交付（raw 或等待者）。
    bool ResolveConnect(std::uint64_t id, Connection connection, std::error_code error) {
        auto op = FindConnect(id);
        if (!op) {
            if (connection.Valid()) {
                connection.RequestClose(std::make_error_code(std::errc::operation_canceled));
            }
            return false;
        }
        bool recycle = false;
        bool delivered = false;
        {
            std::lock_guard<std::mutex> lock(op->mutex);
            if (op->done) {
                // 等待者已超时/取消赢，本轮完成的连接无主 → 回收已交付 fd。
                recycle = connection.Valid();
            } else if (op->abandoned && !op->waiter_attached && handler == nullptr) {
                op->done = true;
                op->error = std::make_error_code(std::errc::operation_canceled);
                recycle = connection.Valid();
            } else {
                op->done = true;
                op->connection = std::move(connection);
                op->error = error;
                if (handler != nullptr) {
                    // raw 已真实交付：清可等待意图，raw-only 清理不被 Co 等待阻塞；
                    // 也不得再由 WaitConnect 二次领取。
                    op->raw_delivered = true;
                    op->waiter_attached = false;
                }
                delivered = true;
            }
        }
        if (recycle) {
            connection.RequestClose(std::make_error_code(std::errc::operation_canceled));
        }
        op->hub.NotifyAll();
        MaybeEraseConnect(id, op);
        return delivered;
    }

    void MaybeEraseConnect(std::uint64_t id, const std::shared_ptr<ConnectOp>& op) {
        bool erase = false;
        {
            std::lock_guard<std::mutex> lock(op->mutex);
            erase = op->done && !op->waiter_attached;
        }
        if (!erase) {
            return;
        }
        std::lock_guard<std::mutex> lock(connect_mutex);
        const auto it = connects.find(id);
        if (it != connects.end() && it->second == op) {
            connects.erase(it);  // 完成且无人等待：释放活动登记
        }
    }

    void RecordError(std::error_code error) {
        if (!error) {
            return;
        }
        std::lock_guard<std::mutex> lock(error_mutex);
        if (!last_error) {
            last_error = error;
        }
    }
    void NoteHandlerError() {
        // 用户原始回调异常：隔离并计数，不升级为 engine/ driver 故障，
        // 不影响其它终态/关闭回调兑现。
        handler_errors.fetch_add(1, std::memory_order_relaxed);
    }
    std::size_t HandlerErrorCount() const {
        return handler_errors.load(std::memory_order_relaxed);
    }
    std::error_code LastError() const {
        std::lock_guard<std::mutex> lock(error_mutex);
        return last_error;
    }
    void RecordControlFailure(std::exception_ptr failure) {
        std::lock_guard<std::mutex> lock(error_mutex);
        if (!control_failure) {
            control_failure = std::move(failure);
        }
    }
    std::exception_ptr LastFailure() const {
        std::lock_guard<std::mutex> lock(error_mutex);
        return control_failure;
    }

    std::shared_ptr<Reactor> PickSub(const Address& peer) {
        std::vector<std::shared_ptr<Reactor>> snapshot;
        {
            std::lock_guard<std::mutex> lock(mutex);
            snapshot = subs;
        }
        if (snapshot.empty()) {
            return nullptr;
        }
        std::vector<std::uint64_t> counts;
        counts.reserve(snapshot.size());
        for (auto& reactor : snapshot) {
            counts.push_back(reactor->ConnCount());
        }
        std::lock_guard<std::mutex> balancer_lock(balancer_mutex);
        if (balancer) {
            std::error_code error;
            const std::size_t index = balancer->Next(peer, counts, error);
            if (!error && index < snapshot.size()) {
                return snapshot[index];
            }
        }
        return snapshot[next_conn_id.load() % snapshot.size()];
    }
    bool IsSealed() const {
        return service_sealed && service_sealed->load(std::memory_order_acquire);
    }
};

void DetailNoteHandlerError(EngineImpl* impl) {
    impl->NoteHandlerError();
}

HandoffReservation::~HandoffReservation() {
    if (impl != nullptr && counted && !committed) {
        impl->ReleaseHandoffReservation();
    }
}

template <class Handler>
bool Reactor::TryPostInternal(Handler&& handler) {
    // 提交资格与 driver 租约同一窄门：持锁跨 post，ReleaseDriver 只能在
    // post 完成后 reset，已接纳命令必进入封口前的 context 队列。
    std::lock_guard<std::mutex> lock(m_driver_mutex);
    if (!m_driver) {
        return false;
    }
    try {
        asio::post(m_driver->Context(), std::forward<Handler>(handler));
        return true;
    } catch (...) {
        return false;
    }
}

template <class Handler>
bool Reactor::TryPost(Handler&& handler) {
    // 提交门：持 driver 租约跨 sealed 检查与 post，使「资格→入队」与
    // ReleaseDriver 的 reset 线性化。hold 只覆盖一次短 post（Asio do_run_one
    // 在 complete 前解锁，不构成 scheduler 反序死锁；独立复审已确认），
    // 不会跨同步解绑：ReleaseDriver 等该锁，post 返回即释放。
    std::lock_guard<std::mutex> lock(m_driver_mutex);
    if (m_sealed.load() || m_impl->IsSealed() || !m_driver) {
        return false;  // 封口后/已解绑：零接纳
    }
    {
        std::function<void()> barrier;
        {
            std::lock_guard<std::mutex> barrier_lock(m_submit_barrier_mutex);
            barrier = std::move(m_submit_barrier);
            m_submit_barrier = {};
        }
        if (barrier) {
            barrier();  // 测试屏障：已取得提交资格、尚未 post（仍持 driver 锁）
        }
    }
    try {
        asio::post(m_driver->Context(), std::forward<Handler>(handler));
        return true;
    } catch (...) {
        return false;
    }
}

// ---- Reactor definitions ----

Reactor::Reactor(EngineImpl* impl, std::size_t index, std::shared_ptr<ReactorDriver> driver)
    : m_impl(impl), m_index(index), m_driver(std::move(driver)) {}

Reactor::~Reactor() = default;

bool Reactor::ImplSealed() const { return m_impl->IsSealed(); }
std::size_t Reactor::ConnCount() {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_conns.size();
}
std::size_t Reactor::ReadLowWater() const { return m_impl->max_inbound_bytes / 2; }

std::error_code Reactor::InstallOnOwner(const std::vector<Address>& addresses) {
    const auto install_delay = m_impl->install_delay_ms.load();
    if (install_delay > 0) {
        timespec ts{};
        ts.tv_sec = install_delay / 1000;
        ts.tv_nsec = (install_delay % 1000) * 1000000L;
        (void)::syscall(SYS_nanosleep, &ts, nullptr);
    }
    if (m_installed.load()) {
        return {};
    }
    m_driver->SetTickHook([this] { PumpOwner(); });
    std::error_code first;
    for (const auto& address : addresses) {
        std::error_code error;
        NativeSocket socket = NativeSocket::Open(address.Family(), SocketType::Stream, error);
        if (!error) {
            error = ApplyListenerOptions(socket, m_impl->options);
        }
        if (!error) {
            error = socket.Bind(address);
        }
        if (!error) {
            error = socket.Listen(256);
        }
        if (error) {
            first = error;
            break;
        }
        auto listener = std::make_shared<Listener>(Ctx());
        listener->local = socket.LocalAddress(error);
        if (error) {
            first = error;
            break;
        }
        const int fd = socket.Release();
        boost::system::error_code boost_error;
        listener->desc.assign(fd, boost_error);
        if (boost_error) {
            ::close(fd);
            first = FromBoost(boost_error);
            break;
        }
        listener->desc.non_blocking(true, boost_error);
        if (boost_error) {
            first = FromBoost(boost_error);
            break;
        }
        m_listeners.push_back(std::move(listener));
    }
    if (first) {
        boost::system::error_code ignored;
        for (auto& listener : m_listeners) {
            listener->desc.close(ignored);
        }
        m_listeners.clear();
        return first;
    }
    m_installed.store(true);
    ArmAcceptAll();
    return {};
}

std::vector<Address> Reactor::LocalAddresses() const {
    std::vector<Address> out;
    out.reserve(m_listeners.size());
    for (const auto& listener : m_listeners) {
        out.push_back(listener->local);
    }
    return out;
}

void Reactor::ArmAcceptAll() {
    if (!m_installed.load() || m_sealed.load() || m_impl->IsSealed()) {
        return;
    }
    for (auto& listener : m_listeners) {
        ArmAccept(listener);
    }
}

void Reactor::ArmAccept(const std::shared_ptr<Listener>& listener) {
    if (!m_installed.load() || m_sealed.load() || m_impl->IsSealed()) {
        return;
    }
    bool expected = false;
    if (!listener->armed.compare_exchange_strong(expected, true)) {
        return;
    }
    // 先预留有界 handoff 容量，再做 native accept；满则暂停（不 arm）。
    auto reservation = m_impl->TryReserveHandoff();
    if (!reservation) {
        listener->armed.store(false);
        return;
    }
    auto self = shared_from_this();
    listener->desc.async_wait(TcpWait::wait_read,
        [self, listener, reservation](const boost::system::error_code& error) mutable {
            listener->armed.store(false);
            if (error) {
                if (error != asio::error::operation_aborted) {
                    self->RecordAcceptError(FromBoost(error));
                }
                return;
            }
            self->DoAccept(listener, std::move(reservation));
        });
}

void Reactor::DoAccept(const std::shared_ptr<Listener>& listener,
                       std::shared_ptr<HandoffReservation> reservation) {
    if (m_sealed.load() || !m_installed.load() || m_impl->IsSealed()) {
        return;
    }
    sockaddr_storage storage{};
    socklen_t length = sizeof(storage);
    // 每次只接纳一个：等 sub 完成登记后再重挂 main（LB 看到真实活跃数）。
    const int fd = static_cast<int>(::syscall(SYS_accept4, listener->desc.native_handle(),
                                              reinterpret_cast<sockaddr*>(&storage), &length,
                                              SOCK_NONBLOCK | SOCK_CLOEXEC));
    if (fd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            ArmAccept(listener);  // 虚假就绪：释放预留后重挂
            return;
        }
        if (errno == EINTR) {
            ArmAccept(listener);
            return;
        }
        RecordAcceptError(std::error_code(errno, std::system_category()));
        return;
    }
    FdHandle handle(fd);
    Address peer = Address::FromNative(reinterpret_cast<sockaddr*>(&storage), length);
    std::shared_ptr<Reactor> target = m_impl->PickSub(peer);
    if (!target) {
        // 无 sub owner：不接纳，fd 由 FdHandle 关闭，预留释放。
        return;
    }
    if (!target->TryPost([target, handle = std::move(handle), reservation]() mutable {
            target->AdoptFdOnOwner(std::move(handle), /*is_connect=*/false, 0, reservation);
        })) {
        m_impl->RecordError(std::make_error_code(std::errc::not_enough_memory));
        ResumeMainAccept();  // post 失败：本次未移交，立即恢复接纳
    }
    // 成功路径由 sub 在 ConnCount 更新后 ResumeMainAccept() 重挂（串行化）。
}

void Reactor::RecordAcceptError(std::error_code error) {
    m_impl->RecordError(error);
}

void Reactor::AdoptFdOnOwner(FdHandle fd, bool is_connect, std::uint64_t connect_id,
                             std::shared_ptr<HandoffReservation> reservation,
                             bool delayed_delivery) {
    if (m_sealed.load() || m_impl->IsSealed() || fd.Get() < 0) {
        if (is_connect) {
            const auto error = std::make_error_code(std::errc::operation_canceled);
            if (m_impl->handler) {
                SafeHandler([this, connect_id, error] {
                    m_impl->handler->OnConnect(Connection(), error, connect_id);
                });
            }
            m_impl->ResolveConnect(connect_id, Connection(), error);
            FinishConnectCallback();
        } else {
            ResumeMainAccept();
        }
        return;  // fd 由 FdHandle 关闭；reservation 析构释放
    }
    if (is_connect && !delayed_delivery) {
        const auto delay_ms = m_impl->connect_delivery_delay_ms.load();
        if (delay_ms > 0) {
            // 私有 test seam：延后「已完成 connect 的交付」，用于确定性
            // timeout/adopt 竞态；fd 由 holder RAII 覆盖关闭竞争。
            auto holder = std::make_shared<FdHandle>(std::move(fd));
            auto timer = std::make_shared<asio::steady_timer>(
                Ctx(), std::chrono::milliseconds(delay_ms));
            auto self = shared_from_this();
            timer->async_wait([self, timer, holder, connect_id](
                                  const boost::system::error_code& error) {
                if (error) {
                    return;
                }
                self->AdoptFdOnOwner(FdHandle(std::move(*holder)), true, connect_id, nullptr,
                                     /*delayed_delivery=*/true);
            });
            return;
        }
    }
    TcpWait descriptor(Ctx());
    boost::system::error_code boost_error;
    descriptor.assign(fd.Release(), boost_error);
    if (boost_error) {
        m_impl->RecordError(FromBoost(boost_error));
        if (is_connect) {
            if (m_impl->handler) {
                SafeHandler([this, connect_id, boost_error] {
                    m_impl->handler->OnConnect(Connection(), FromBoost(boost_error), connect_id);
                });
            }
            m_impl->ResolveConnect(connect_id, Connection(), FromBoost(boost_error));
            FinishConnectCallback();
        }
        return;
    }
    descriptor.non_blocking(true, boost_error);
    if (boost_error) {
        m_impl->RecordError(FromBoost(boost_error));
        if (is_connect) {
            if (m_impl->handler) {
                SafeHandler([this, connect_id, boost_error] {
                    m_impl->handler->OnConnect(Connection(), FromBoost(boost_error), connect_id);
                });
            }
            m_impl->ResolveConnect(connect_id, Connection(), FromBoost(boost_error));
            FinishConnectCallback();
        }
        return;
    }
    if (const auto option_error = ApplyStreamOptions(descriptor.native_handle(), m_impl->options)) {
        m_impl->RecordError(option_error);
    }
    const std::uint64_t id = m_impl->next_conn_id.fetch_add(1, std::memory_order_relaxed);
    auto state = std::make_shared<ConnectionState>(id, m_impl->max_inbound_bytes,
                                                   m_impl->max_outbound_bytes,
                                                   m_impl->max_ops, m_impl->service_sealed);
    auto core = std::make_shared<ConnCore>(state, std::move(descriptor), this, m_impl->handler);
    std::weak_ptr<Reactor> weak_self = weak_from_this();
    std::weak_ptr<ConnCore> weak_core = core;
    auto mark_dirty = [weak_self, weak_core] {
        if (auto reactor = weak_self.lock()) {
            if (auto conn = weak_core.lock()) {
                reactor->MarkDirty(conn);
            }
        }
    };
    state->owner_dirty = mark_dirty;
    state->ring.SetOwnerWake(mark_dirty);  // 消费释放容量/终态也唤醒 owner 恢复读
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_conns.emplace(id, core);
    }
    m_impl->conn_count.fetch_add(1, std::memory_order_relaxed);
    Connection connection(state);

    if (is_connect) {
        if (auto pending_op = m_impl->FindConnect(connect_id)) {
            std::lock_guard<std::mutex> lock(pending_op->mutex);
            pending_op->owner_delivering = true;  // 授权交付段开始，seal 让位
        }
        // 开始授权点：紧邻交付的同一原子检查（构造窗口内封口不启动成功回调）。
        InvokePreAuthBarrier();
        if (m_impl->IsSealed()) {
            const auto canceled = std::make_error_code(std::errc::operation_canceled);
            if (m_impl->handler) {
                SafeHandler([this, connect_id, canceled] {
                    m_impl->handler->OnConnect(Connection(), canceled, connect_id);
                });
            }
            m_impl->ResolveConnect(connect_id, Connection(), canceled);
            core->CancelControl();
            FinishConnectCallback();
            return;
        }
        InvokePostAuthBarrier();
        if (!m_impl->ResolveConnect(connect_id, connection, {})) {
            // 等待者已放弃且无 raw 接收者：owner 回收未交付 fd，不回调。
            core->ShutdownOwner(std::make_error_code(std::errc::operation_canceled));
            FinishConnectCallback();
            return;
        }
        if (m_impl->handler) {
            SafeHandler([this, connection, connect_id] {
                m_impl->handler->OnConnect(connection, {}, connect_id);
            });
        }
        FinishConnectCallback();
    } else {
        // OnAccept 的同一开始授权点：入口早查不是授权，最后一次紧邻派发的
        // 原子检查才与封口线性化。
        InvokePreAuthBarrier();
        if (m_impl->IsSealed()) {
            core->CancelControl();
            RemoveConn(core);
            return;
        }
        InvokePostAuthBarrier();
        if (m_impl->handoff_enabled && reservation) {
            if (!m_impl->CommitHandoff(connection, reservation)) {
                core->ShutdownOwner(std::make_error_code(std::errc::operation_canceled));
                return;
            }
        } else if (reservation) {
            reservation->Commit();
        }
        // handoff 与 raw 不互吞：raw 有 handler 仍交付 OnAccept。
        if (m_impl->handler) {
            SafeHandler([this, connection] { m_impl->handler->OnAccept(connection); });
        }
    }
    core->StartOwner();
    if (!is_connect) {
        ResumeMainAccept();
    }
}

void Reactor::BeginConnectOnOwner(const Address& address, std::uint64_t connect_id) {
    if (m_sealed.load() || m_impl->IsSealed()) {
        if (m_impl->handler) {
            SafeHandler([this, connect_id] {
                m_impl->handler->OnConnect(Connection(),
                    std::make_error_code(std::errc::operation_canceled), connect_id);
            });
        }
        m_impl->ResolveConnect(connect_id, Connection(),
                               std::make_error_code(std::errc::operation_canceled));
        return;
    }
    const auto start_delay = m_impl->connect_start_delay_ms.load();
    if (start_delay > 0) {
        // 私有 test 屏障：占位 pending（尚未建 socket），取消/封口可及时撤销。
        auto pending = std::make_shared<ConnectPending>();
        pending->connect_id = connect_id;
        auto timer = std::make_shared<asio::steady_timer>(
            Ctx(), std::chrono::milliseconds(start_delay));
        pending->start_timer = timer;
        m_pending_connects.push_back(pending);
        auto self = shared_from_this();
        ++m_pending_connect_callbacks;
        timer->async_wait([self, pending, connect_id, address](
                              const boost::system::error_code& error) {
            auto& list = self->m_pending_connects;
            list.erase(std::remove(list.begin(), list.end(), pending), list.end());
            if (error && error != asio::error::operation_aborted) {
                const auto from_boost = FromBoost(error);
                if (self->m_impl->handler) {
                    self->SafeHandler([self, connect_id, from_boost] {
                        self->m_impl->handler->OnConnect(Connection(), from_boost, connect_id);
                    });
                }
                self->m_impl->ResolveConnect(connect_id, Connection(), from_boost);
                self->FinishConnectCallback();
                return;
            }
            if (self->m_sealed.load() || self->m_impl->IsSealed() || pending->canceled) {
                const auto canceled = std::make_error_code(std::errc::operation_canceled);
                if (self->m_impl->handler) {
                    self->SafeHandler([self, connect_id, canceled] {
                        self->m_impl->handler->OnConnect(Connection(), canceled, connect_id);
                    });
                }
                self->m_impl->ResolveConnect(connect_id, Connection(), canceled);
                self->FinishConnectCallback();
                return;
            }
            self->StartConnectSocketOnOwner(address, connect_id);
        });
        return;
    }
    ++m_pending_connect_callbacks;
    StartConnectSocketOnOwner(address, connect_id);
}

void Reactor::StartConnectSocketOnOwner(const Address& address, std::uint64_t connect_id) {
    if (m_sealed.load() || m_impl->IsSealed()) {
        if (m_impl->handler) {
            SafeHandler([this, connect_id] {
                m_impl->handler->OnConnect(Connection(),
                    std::make_error_code(std::errc::operation_canceled), connect_id);
            });
        }
        m_impl->ResolveConnect(connect_id, Connection(),
                               std::make_error_code(std::errc::operation_canceled));
        return;
    }
    std::error_code error;
    NativeSocket socket = NativeSocket::Open(address.Family(), SocketType::Stream, error);
    if (!error) {
        error = ApplyStreamOptions(socket.Fd(), m_impl->options);
    }
    if (!error) {
        error = socket.Connect(address);
        if (error && error.value() != EINPROGRESS && error.value() != EWOULDBLOCK) {
            // fallthrough: close and report
        } else {
            error.clear();
        }
    }
    if (error) {
        if (m_impl->handler) {
            SafeHandler([this, connect_id, error] {
                m_impl->handler->OnConnect(Connection(), error, connect_id);
            });
        }
        m_impl->ResolveConnect(connect_id, Connection(), error);
        FinishConnectCallback();
        return;
    }
    const int fd = socket.Release();
    auto descriptor = std::make_shared<TcpWait>(Ctx());
    boost::system::error_code boost_error;
    descriptor->assign(fd, boost_error);
    if (boost_error) {
        ::close(fd);
        if (m_impl->handler) {
            SafeHandler([this, connect_id, boost_error] {
                m_impl->handler->OnConnect(Connection(), FromBoost(boost_error), connect_id);
            });
        }
        m_impl->ResolveConnect(connect_id, Connection(), FromBoost(boost_error));
        FinishConnectCallback();
        return;
    }
    descriptor->non_blocking(true, boost_error);
    auto pending = std::make_shared<ConnectPending>();
    pending->desc = descriptor;
    pending->connect_id = connect_id;
    m_pending_connects.push_back(pending);
    auto self = shared_from_this();
    try {
        descriptor->async_wait(TcpWait::wait_write,
        [self, descriptor, pending, connect_id](const boost::system::error_code& wait_error) {
            // 从 pending 列表移除。
            auto& list = self->m_pending_connects;
            list.erase(std::remove(list.begin(), list.end(), pending), list.end());
            if (pending->canceled) {
                if (self->m_impl->handler) {
                    self->SafeHandler([self, connect_id] {
                        self->m_impl->handler->OnConnect(Connection(),
                            std::make_error_code(std::errc::operation_canceled), connect_id);
                    });
                }
                self->m_impl->ResolveConnect(connect_id, Connection(),
                                             std::make_error_code(std::errc::operation_canceled));
                self->FinishConnectCallback();
                return;
            }
            if (wait_error) {
                const auto error = FromBoost(wait_error);
                if (self->m_impl->handler) {
                    self->SafeHandler([self, connect_id, error] {
                        self->m_impl->handler->OnConnect(Connection(), error, connect_id);
                    });
                }
                self->m_impl->ResolveConnect(connect_id, Connection(), error);
                self->FinishConnectCallback();
                return;
            }
            const int socket_error = GetSocketError(descriptor->native_handle());
            if (socket_error != 0) {
                const auto error = std::error_code(socket_error, std::system_category());
                if (self->m_impl->handler) {
                    self->SafeHandler([self, connect_id, error] {
                        self->m_impl->handler->OnConnect(Connection(), error, connect_id);
                    });
                }
                self->m_impl->ResolveConnect(connect_id, Connection(), error);
                self->FinishConnectCallback();
                return;
            }
            const int connected_fd = descriptor->release();
            self->AdoptFdOnOwner(FdHandle(connected_fd), /*is_connect=*/true, connect_id, nullptr);
        });
    } catch (...) {
        FinishConnectCallback();
        const auto error = std::make_error_code(std::errc::not_enough_memory);
        if (m_impl->handler) {
            SafeHandler([this, connect_id, error] {
                m_impl->handler->OnConnect(Connection(), error, connect_id);
            });
        }
        m_impl->ResolveConnect(connect_id, Connection(), error);
    }
}

void Reactor::MarkDirty(const std::shared_ptr<ConnCore>& core) {
    if (!core || m_sealed.load() || m_impl->IsSealed()) {
        return;
    }
    bool expected = false;
    if (!core->m_dirty_queued.compare_exchange_strong(expected, true)) {
        return;
    }
    {
        std::function<void()> barrier;
        {
            std::lock_guard<std::mutex> barrier_lock(m_barrier_mutex);
            barrier = std::move(m_dirty_barrier);
            m_dirty_barrier = {};
        }
        if (barrier) {
            barrier();  // 测试屏障：weak-lock 之后、登记/入链之前
        }
    }
    {
        // 复用连接登记的窄节点：入队不分配（F8.1）。registry 移除与 dirty 入链
        // 用同一锁序（dirty -> m_mutex）共同登记/撤销：节点已 RemoveConn 时拒绝
        // 入链，避免晚 Wake 把已析构 core 裸指针挂链（R5）。
        std::lock_guard<std::mutex> dirty_lock(m_dirty_mutex);
        if (m_sealed.load()) {
            core->m_dirty_queued.store(false);
            return;
        }
        {
            std::lock_guard<std::mutex> reg_lock(m_mutex);
            if (m_conns.find(core->State()->m_id) == m_conns.end()) {
                core->m_dirty_queued.store(false);
                return;  // 已撤销登记：明确拒绝晚 Wake
            }
            core->m_dirty_next = nullptr;
            if (m_dirty_tail != nullptr) {
                m_dirty_tail->m_dirty_next = core.get();
            } else {
                m_dirty_head = core.get();
            }
            m_dirty_tail = core.get();
            core->m_dirty_linked = true;
        }
    }
    {
        std::lock_guard<std::mutex> lock(m_driver_mutex);
        if (m_driver) {
            m_driver->Wake();
        }
    }
}

void Reactor::PumpOwner() {
    if (!m_installed.load()) {
        return;
    }
    const auto begin = std::chrono::steady_clock::now();
    std::size_t pumped = 0;
    while (pumped < kPumpBudget) {
        ConnCore* raw = nullptr;
        {
            std::lock_guard<std::mutex> lock(m_dirty_mutex);
            raw = m_dirty_head;
            if (raw == nullptr) {
                break;
            }
            m_dirty_head = raw->m_dirty_next;
            if (m_dirty_head == nullptr) {
                m_dirty_tail = nullptr;
            }
            raw->m_dirty_next = nullptr;
            raw->m_dirty_linked = false;
            raw->m_dirty_queued.store(false);
        }
        std::shared_ptr<ConnCore> core;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto it = m_conns.find(raw->State()->m_id);
            if (it != m_conns.end()) {
                core = it->second;
            }
        }
        if (!core) {
            continue;
        }
        core->PumpOwner();
        if (core->Closed()) {
            RemoveConn(core);
        }
        ++pumped;
        // 跨连接批次时间预算：剩余留在 dirty 链表，下一轮继续（不丢不睡死）。
        if (std::chrono::steady_clock::now() - begin >= std::chrono::milliseconds(1)) {
            break;
        }
    }
}

void Reactor::PumpAllConnsOwner() {
    std::vector<std::shared_ptr<ConnCore>> conns;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        for (auto& entry : m_conns) {
            conns.push_back(entry.second);
        }
    }
    for (auto& core : conns) {
        core->PumpOwner();
        if (core->Closed()) {
            RemoveConn(core);
        }
    }
}

void Reactor::RemoveConn(const std::shared_ptr<ConnCore>& core) {
    if (!core) {
        return;
    }
    const std::uint64_t id = core->State()->m_id;
    bool removed = false;
    {
        // 与 MarkDirty 真正共门：解链与 registry 擦除在同一 dirty→registry
        // 锁域内原子完成。若两段之间释放 dirty 锁，MarkDirty 可看到 registry
        // 仍在而重新入链，随后 erase 使裸节点悬空（R5 父级补充）。
        std::lock_guard<std::mutex> dirty_lock(m_dirty_mutex);
        if (core->m_dirty_linked) {
            ConnCore* node = m_dirty_head;
            ConnCore* prev = nullptr;
            while (node != nullptr && node != core.get()) {
                prev = node;
                node = node->m_dirty_next;
            }
            if (node == core.get()) {
                if (prev != nullptr) {
                    prev->m_dirty_next = node->m_dirty_next;
                } else {
                    m_dirty_head = node->m_dirty_next;
                }
                if (m_dirty_tail == node) {
                    m_dirty_tail = prev;
                }
                node->m_dirty_next = nullptr;
                node->m_dirty_linked = false;
            }
        }
        core->m_dirty_queued.store(false);
        std::lock_guard<std::mutex> reg_lock(m_mutex);
        InvokeRemoveBarrier();  // 测试：撤链后、擦表前（仍持 dirty+registry）
        removed = m_conns.erase(id) != 0;
    }
    if (removed && m_impl->conn_count.load() > 0) {
        m_impl->conn_count.fetch_sub(1);
    }
}

void Reactor::FinishConnectCallback() {
    if (m_pending_connect_callbacks == 0) {
        return;
    }
    --m_pending_connect_callbacks;
    if (m_pending_connect_callbacks == 0 && m_on_pending_zero) {
        auto done = std::move(m_on_pending_zero);
        m_on_pending_zero = nullptr;
        done();  // 最后一个 pending connect 终态已在原 owner 兑现
    }
}

void Reactor::SealServiceOnOwner(std::function<void()> on_done) {
    const auto seal_delay = m_impl->seal_delay_ms.load();
    if (seal_delay > 0) {
        // owner 线程在 coroutine hook 覆盖下，用原始 nanosleep 避免 hook 断言。
        timespec ts{};
        ts.tv_sec = seal_delay / 1000;
        ts.tv_nsec = (seal_delay % 1000) * 1000000L;
        (void)::syscall(SYS_nanosleep, &ts, nullptr);
    }
    {
        // 先解除 dirty 链（不分配），再关闭/清空 conns，避免裸链节点悬空。
        std::lock_guard<std::mutex> lock(m_dirty_mutex);
        ConnCore* node = m_dirty_head;
        while (node != nullptr) {
            ConnCore* next = node->m_dirty_next;
            node->m_dirty_next = nullptr;
            node->m_dirty_linked = false;
            node->m_dirty_queued.store(false);
            node = next;
        }
        m_dirty_head = nullptr;
        m_dirty_tail = nullptr;
    }
    m_sealed.store(true);
    // 幂等：重试/重复调用仍完成剩余 conns 与 pending 收口（ShutdownOwner 幂等），
    // 不因第一次中途异常而丢掉未兑现责任。
    boost::system::error_code ignored;
    for (auto& listener : m_listeners) {
        listener->desc.cancel(ignored);
        listener->desc.close(ignored);
    }
    for (auto& pending : m_pending_connects) {
        pending->canceled = true;
        if (pending->desc) {
            pending->desc->cancel(ignored);
        }
    }
    // 逐个 owner 结算后再从 registry erase：先 erase/先 closed 会丢未兑现的
    // OnWrite/OnClose 责任。ShutdownOwner 分阶段幂等，异常可由重试续做。
    for (;;) {
        std::shared_ptr<ConnCore> core;
        std::uint64_t id = 0;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_conns.empty()) {
                break;
            }
            auto it = m_conns.begin();
            id = it->first;
            core = it->second;
        }
        core->ShutdownOwner(std::make_error_code(std::errc::operation_canceled));
        if (!core->Closed()) {
            break;  // 责任未完成：保留 registry，下次 seal 重试
        }
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            const auto it = m_conns.find(id);
            if (it != m_conns.end() && it->second == core) {
                m_conns.erase(it);
            }
        }
    }
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_impl->conn_count.store(m_conns.size());
    }
    if (m_pending_connect_callbacks == 0) {
        if (on_done) {
            on_done();
        }
    } else {
        // 等 cancel completion 在原 owner 交付 OnConnect(canceled) 后再完成 seal。
        m_on_pending_zero = std::move(on_done);
    }
}

void Reactor::FinalizeControl() {
    {
        // 先解除 dirty 链（不分配），再关闭/清空 conns，避免裸链节点悬空。
        std::lock_guard<std::mutex> lock(m_dirty_mutex);
        ConnCore* node = m_dirty_head;
        while (node != nullptr) {
            ConnCore* next = node->m_dirty_next;
            node->m_dirty_next = nullptr;
            node->m_dirty_linked = false;
            node->m_dirty_queued.store(false);
            node = next;
        }
        m_dirty_head = nullptr;
        m_dirty_tail = nullptr;
    }
    if (m_sealed.exchange(true)) {
        // 仍确保资源关闭（幂等）。
    }
    boost::system::error_code ignored;
    for (auto& listener : m_listeners) {
        listener->desc.close(ignored);
    }
    m_listeners.clear();
    m_pending_connects.clear();
    std::vector<std::shared_ptr<ConnCore>> snapshot;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        for (auto& entry : m_conns) {
            snapshot.push_back(entry.second);
        }
        m_conns.clear();
    }
    for (auto& core : snapshot) {
        core->CancelControl();
    }
    m_impl->conn_count.store(0);
}

void Reactor::ReleaseDriver() {
    // 必须在 FinalizeControl 之后、解绑成功之后调用；此后不再访问 context。
    std::lock_guard<std::mutex> lock(m_driver_mutex);
    m_driver.reset();
}


void Reactor::CancelConnectOnOwner(std::uint64_t connect_id) {
    for (auto& pending : m_pending_connects) {
        if (pending->connect_id == connect_id) {
            pending->canceled = true;
            if (pending->start_timer) {
                pending->start_timer->cancel();
            }
            if (pending->desc) {
                boost::system::error_code ignored;
                pending->desc->cancel(ignored);
            }
            return;
        }
    }
}

void Reactor::CancelAllRequestedOnOwner() {
    for (auto& pending : m_pending_connects) {
        auto op = m_impl->FindConnect(pending->connect_id);
        if (!op || !op->cancel_requested.load()) {
            continue;
        }
        pending->canceled = true;
        if (pending->start_timer) {
            pending->start_timer->cancel();
        }
        if (pending->desc) {
            boost::system::error_code ignored;
            pending->desc->cancel(ignored);
        }
    }
}

void Reactor::ResumeMainAccept() {
    std::shared_ptr<Reactor> main;
    {
        std::lock_guard<std::mutex> lock(m_impl->mutex);
        main = m_impl->main_reactor;
    }
    if (main) {
        (void)main->TryPost([main] { main->ArmAcceptAll(); });
    }
}

std::size_t Reactor::RecvHoldBytes() {
    std::lock_guard<std::mutex> lock(m_mutex);
    std::size_t total = 0;
    for (auto& entry : m_conns) {
        total += entry.second->RecvHold();
    }
    return total;
}

std::size_t Reactor::DirtyCount() {
    std::lock_guard<std::mutex> lock(m_dirty_mutex);
    std::size_t count = 0;
    for (ConnCore* node = m_dirty_head; node != nullptr; node = node->m_dirty_next) {
        ++count;
    }
    return count;
}

int Reactor::FirstConnFd() {
    std::lock_guard<std::mutex> lock(m_mutex);
    for (auto& entry : m_conns) {
        return entry.second->Fd();
    }
    return -1;
}


// ---- Engine ----

Engine::Engine(std::shared_ptr<RawHandler> handler)
    : m_impl(std::make_shared<EngineImpl>(std::move(handler))) {}

Engine::~Engine() {
    // 合法控制域上的最后兜底：Run 因解绑失败保留 Closing 时，析构前再尝试
    // 同代同步解绑 + 冻结收口；成功才释放责任，失败则无 reaper 可再保证。
    auto impl = m_impl;
    if (!impl) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        if (impl->phase != EnginePhase::Closing) {
            return;
        }
    }
    std::vector<DriverSlot> to_unbind;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        to_unbind = std::move(impl->slots);
        impl->slots.clear();
        for (auto& slot : impl->residual_slots) {
            to_unbind.push_back(std::move(slot));
        }
        impl->residual_slots.clear();
        reactors = impl->reactors;
    }
    if (!to_unbind.empty()) {
        const auto error = DriverBinder::UnbindSlots(to_unbind);
        if (error || !to_unbind.empty()) {
            std::lock_guard<std::mutex> lock(impl->mutex);
            impl->slots = std::move(to_unbind);
            return;
        }
    }
    for (auto& reactor : reactors) {
        reactor->FinalizeControl();
        reactor->ReleaseDriver();
    }
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        impl->reactors.clear();
        impl->main_reactor.reset();
        impl->subs.clear();
        impl->phase = EnginePhase::Stopped;
    }
}

void Engine::RequestClose() noexcept {
    auto impl = m_impl;
    if (!impl) {
        return;
    }
    try {
        {
            std::lock_guard<std::mutex> lock(impl->mutex);
            impl->close_requested = true;
        }
        impl->SealService();
        impl->control_cv.notify_all();
    } catch (...) {
    }
}

EnginePhase Engine::Phase() const noexcept {
    std::lock_guard<std::mutex> lock(m_impl->mutex);
    return m_impl->phase;
}

std::error_code Engine::LastError() const {
    return m_impl->LastError();
}

std::exception_ptr Engine::LastFailure() const {
    return m_impl->LastFailure();
}

std::error_code Engine::EnableAcceptHandoff(std::size_t capacity) {
    if (capacity == 0) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    std::lock_guard<std::mutex> lock(m_impl->mutex);
    if (m_impl->phase != EnginePhase::Idle) {
        return std::make_error_code(std::errc::device_or_resource_busy);
    }
    m_impl->handoff_capacity = capacity;
    m_impl->handoff_enabled = true;
    return {};
}

std::error_code Engine::Run(const EngineConfig& config) {
    auto impl = m_impl;
    // 前置域检查在 Starting/绑定/socket 任何副作用之前。
    if (bbt::coroutine::GetLocalCoroutineId() != 0
        || bbt::coroutine::detail::Processer::TryGetLocalProcesser() != nullptr) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        if (impl->phase != EnginePhase::Idle) {
            return impl->phase == EnginePhase::Stopped
                ? std::make_error_code(std::errc::operation_canceled)
                : std::make_error_code(std::errc::device_or_resource_busy);
        }
        impl->phase = EnginePhase::Starting;
    }

    auto fail = [&](std::error_code error) {
        std::lock_guard<std::mutex> lock(impl->mutex);
        impl->RecordError(error);
        impl->phase = EnginePhase::Stopped;
        impl->control_cv.notify_all();
        return error;
    };

    auto* scheduler = bbt::coroutine::detail::Scheduler::GetInstance().get();
    if (scheduler == nullptr || !scheduler->IsInitialized()) {
        return fail(std::make_error_code(std::errc::operation_not_supported));
    }
    if (config.runtime_progress != RuntimeProgress::Independent) {
        return fail(std::make_error_code(std::errc::operation_not_permitted));
    }

    NormalizedOptions normalized;
    // 归一化只做值验证，不再依赖 CPU 数，也没有历史段容量字段可交叉校验。
    if (const auto error = NormalizeOptions(config.options, normalized)) {
        return fail(error);
    }
    const bool server = !config.listen_addresses.empty();
    if (server && impl->handler == nullptr && !impl->handoff_enabled) {
        // 保持 handler+handoff 共存，但拒绝"两者都无"的无人接收布局：
        // 仍在 driver 绑定/socket 副作用之前。
        return fail(std::make_error_code(std::errc::operation_not_permitted));
    }
    impl->max_inbound_bytes = normalized.max_inbound_bytes;
    impl->max_outbound_bytes = normalized.max_outbound_bytes;
    impl->max_ops = normalized.max_pending_events;
    impl->options = normalized;

    const std::size_t sub_count = std::max<std::size_t>(1, normalized.num_event_loop);
    const std::size_t slot_count = server ? sub_count + 1 : sub_count;

    std::error_code balancer_error;
    std::unique_ptr<LoadBalancer> balancer;
    try {
        balancer = std::make_unique<LoadBalancer>(normalized.lb, sub_count, balancer_error);
    } catch (...) {
        return fail(std::make_error_code(std::errc::not_enough_memory));
    }
    if (balancer_error) {
        return fail(balancer_error);
    }

    std::vector<DriverSlot> slots;
    if (const auto error = DriverBinder::BindSlots(slot_count, slots, &impl->residual_slots)) {
        if (!impl->residual_slots.empty()) {
            // 回滚失败：责任仍可达，保持 Closing，不伪报 Stopped。
            std::lock_guard<std::mutex> lock(impl->mutex);
            impl->RecordError(error);
            impl->phase = EnginePhase::Closing;
            return error;
        }
        return fail(error);
    }
    std::vector<std::shared_ptr<Reactor>> reactors;
    std::shared_ptr<Reactor> main_reactor;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        impl->slots = std::move(slots);
        impl->balancer = std::move(balancer);
        impl->max_inbound_bytes = normalized.max_inbound_bytes;
        impl->max_outbound_bytes = normalized.max_outbound_bytes;
        impl->max_ops = normalized.max_pending_events;
        for (std::size_t i = 0; i < impl->slots.size(); ++i) {
            impl->reactors.push_back(
                std::make_shared<Reactor>(impl.get(), i, impl->slots[i].driver));
        }
        if (server) {
            impl->main_reactor = impl->reactors.front();
            impl->subs.assign(impl->reactors.begin() + 1, impl->reactors.end());
        } else {
            impl->main_reactor = nullptr;
            impl->subs = impl->reactors;
        }
        reactors = impl->reactors;
        main_reactor = impl->main_reactor;
    }

    // owner 安装：post 到各自 context 并等 owner 返回结果，然后才发布 Running。
    std::vector<std::shared_future<std::error_code>> installs;
    for (auto& reactor : reactors) {
        auto promise = std::make_shared<std::promise<std::error_code>>();
        installs.push_back(promise->get_future().share());
        std::vector<Address> addresses;
        if (server && reactor == main_reactor) {
            addresses = config.listen_addresses;
        }
        if (!reactor->TryPost([reactor, addresses, promise] {
                promise->set_value(reactor->InstallOnOwner(addresses));
            })) {
            promise->set_value(std::make_error_code(std::errc::operation_canceled));
        }
    }
    // 无隐藏 deadline：等 owner 安装完成；期间若 driver 真实 Failed 由 Query
    // 控制域识别并走既定错误出口（不把超时冒 Failed）。
    auto driver_failed = [&]() -> bool {
        if (impl->test_wait_driver_failed.exchange(false)) {
            impl->RecordError(std::make_error_code(std::errc::io_error));
            return true;
        }
        std::error_code control_error;
        std::exception_ptr control_failure;
        {
            std::lock_guard<std::mutex> lock(impl->mutex);
            for (auto& slot : impl->slots) {
                bbt::coroutine::detail::ProcesserDriverSnapshot snapshot;
                const auto status =
                    scheduler->QueryProcesserDriver(slot.processer_id, snapshot);
                if (status != bbt::coroutine::detail::ProcesserDriverStatus::Ok) {
                    control_error = std::make_error_code(std::errc::io_error);
                    break;
                }
                if (snapshot.m_token != slot.token) {
                    control_error = std::make_error_code(std::errc::operation_canceled);
                    break;
                }
                if (snapshot.m_state
                    == bbt::coroutine::detail::ProcesserDriverState::Failed) {
                    control_failure = snapshot.m_exception;
                    control_error = std::make_error_code(std::errc::io_error);
                    break;
                }
            }
        }
        if (control_error) {
            if (control_failure) {
                impl->RecordControlFailure(control_failure);
            }
            impl->RecordError(control_error);
            return true;
        }
        return false;
    };
    std::error_code install_error;
    for (auto& future : installs) {
        while (future.wait_for(std::chrono::milliseconds(5)) != std::future_status::ready) {
            if (driver_failed()) {
                break;
            }
        }
        if (future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
            install_error = std::make_error_code(std::errc::operation_canceled);
            break;
        }
        install_error = future.get();
        if (install_error) {
            break;
        }
    }
    if (install_error) {
        if (install_error == std::make_error_code(std::errc::timed_out)) {
            // owner 安装结果未在界内返回：可能仍有 queued install；不得解绑/
            // 销毁 context 或伪报 Stopped，保留 Closing 由合法控制域析构兜底。
            std::lock_guard<std::mutex> lock(impl->mutex);
            impl->RecordError(install_error);
            impl->phase = EnginePhase::Closing;
            return install_error;
        }
        // 同步安装失败：同代解绑；失败/残留则保留责任对象与 Closing，不伪报 Stopped。
        std::vector<DriverSlot> to_unbind;
        {
            std::lock_guard<std::mutex> lock(impl->mutex);
            to_unbind = std::move(impl->slots);
            impl->slots.clear();
            for (auto& slot : impl->residual_slots) {
                to_unbind.push_back(std::move(slot));
            }
            impl->residual_slots.clear();
        }
        const auto unbind_error = DriverBinder::UnbindSlots(to_unbind);
        if (unbind_error || !to_unbind.empty()) {
            std::lock_guard<std::mutex> lock(impl->mutex);
            impl->slots = std::move(to_unbind);
            impl->RecordError(unbind_error ? unbind_error : install_error);
            impl->phase = EnginePhase::Closing;
            return unbind_error ? unbind_error : install_error;
        }
        for (auto& reactor : reactors) {
            reactor->FinalizeControl();
            reactor->ReleaseDriver();
        }
        {
            std::lock_guard<std::mutex> lock(impl->mutex);
            impl->reactors.clear();
            impl->main_reactor.reset();
            impl->subs.clear();
        }
        return fail(install_error);
    }

    {
        std::vector<Address> local;
        if (server && main_reactor) {
            local = main_reactor->LocalAddresses();
        }
        std::lock_guard<std::mutex> lock(impl->mutex);
        impl->local_addresses = std::move(local);
        impl->phase = EnginePhase::Running;
    }

    bool failed = false;
    for (;;) {
        bool close = false;
        {
            std::unique_lock<std::mutex> lock(impl->mutex);
            impl->control_cv.wait_for(lock, std::chrono::milliseconds(50),
                                      [&] { return impl->close_requested; });
            close = impl->close_requested;
        }
        if (close) {
            break;
        }
        if (impl->cancel_retry_needed.exchange(false)) {
            // 一次性取消投递失败后的持久重试：控制循环补交，不等网络无限终态。
            for (auto& reactor : reactors) {
                if (!reactor->TryPostInternal([reactor] {
                        reactor->CancelAllRequestedOnOwner();
                    })) {
                    impl->cancel_retry_needed.store(true);
                }
            }
        }
        std::vector<DriverSlot> current;
        {
            std::lock_guard<std::mutex> lock(impl->mutex);
            current = impl->slots;
        }
        bool control_error = false;
        for (const auto& slot : current) {
            bbt::coroutine::detail::ProcesserDriverSnapshot snapshot;
            const auto status = scheduler->QueryProcesserDriver(slot.processer_id, snapshot);
            if (status != bbt::coroutine::detail::ProcesserDriverStatus::Ok) {
                impl->RecordError(std::make_error_code(std::errc::io_error));
                control_error = true;
                break;
            }
            if (snapshot.m_token != slot.token) {
                // 代次不符：诚实报控制错误，绝不用新 token 解绑他人。
                impl->RecordError(std::make_error_code(std::errc::operation_canceled));
                control_error = true;
                break;
            }
            if (snapshot.m_state == bbt::coroutine::detail::ProcesserDriverState::Failed) {
                failed = true;
                impl->RecordControlFailure(snapshot.m_exception);
                impl->RecordError(std::make_error_code(std::errc::io_error));
                break;
            }
        }
        if (failed || control_error) {
            failed = true;  // 诚实收口，不无限只记录后继续等
            break;
        }
    }

    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        impl->phase = EnginePhase::Closing;
    }
    impl->SealService();

    // 正常路径：owner 撤 listener/取消 connect/关闭连接并兑现用户终态。
    bool owner_seal_complete = !failed;
    std::error_code final_error;
    if (!failed) {
        std::vector<std::shared_future<std::error_code>> seals;
        for (auto& reactor : reactors) {
            auto promise = std::make_shared<std::promise<std::error_code>>();
            seals.push_back(promise->get_future().share());
            // 单次可恢复 post 失败：补交同一条 seal 责任，不设隐藏关闭 deadline，
            // 直到原 owner 兑现全部已接纳回调（正常完成条件是 owner 结算，而非 2s）。
            for (;;) {
                if (impl->seal_post_fail_once.exchange(false)) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }
                if (reactor->TryPostInternal([reactor, promise, impl] {
                        try {
                            if (impl->seal_throw_once.exchange(false)) {
                                throw std::runtime_error("seal-throw-test");
                            }
                            reactor->SealServiceOnOwner([promise] {
                                promise->set_value(std::error_code{});
                            });
                        } catch (...) {
                            // 本地可恢复异常：保留首错，但先重试完成正常 owner
                            // 收口（OnClose/OnWrite/OnConnect），不得用冻结替代。
                            impl->seal_error.store(true);
                            try {
                                reactor->SealServiceOnOwner([promise] {
                                    promise->set_value(std::error_code{});
                                });
                            } catch (...) {
                                promise->set_value(
                                    std::make_error_code(std::errc::not_enough_memory));
                            }
                        }
                    })) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        for (auto& future : seals) {
            while (future.wait_for(std::chrono::milliseconds(5))
                   != std::future_status::ready) {
                if (driver_failed()) {
                    break;  // driver 真实 Failed：不再等一个不会执行的 handler
                }
            }
            if (future.wait_for(std::chrono::milliseconds(0))
                != std::future_status::ready) {
                owner_seal_complete = false;
                final_error = std::make_error_code(std::errc::operation_canceled);
            } else if (const auto seal_result = future.get()) {
                owner_seal_complete = false;
                final_error = seal_result;
            }
        }
        if (impl->seal_error.load()) {
            impl->RecordError(std::make_error_code(std::errc::not_enough_memory));
            if (!final_error) {
                final_error = std::make_error_code(std::errc::not_enough_memory);
            }
        }
        if (!owner_seal_complete) {
            std::lock_guard<std::mutex> lock(impl->mutex);
            impl->RecordError(final_error);
        }
    }

    // 同代同步解绑；失败/残留则保留责任对象、保持 Closing、可观测错误，不发布 Stopped。
    std::vector<DriverSlot> to_unbind;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        to_unbind = std::move(impl->slots);
        impl->slots.clear();
        for (auto& slot : impl->residual_slots) {
            to_unbind.push_back(std::move(slot));
        }
        impl->residual_slots.clear();
    }
    const auto unbind_error = DriverBinder::UnbindSlots(to_unbind);
    if (unbind_error || !to_unbind.empty()) {
        std::lock_guard<std::mutex> lock(impl->mutex);
        impl->slots = std::move(to_unbind);
        impl->RecordError(unbind_error ? unbind_error
                                       : std::make_error_code(std::errc::io_error));
        impl->phase = EnginePhase::Closing;
        return LastError();
    }

    // 解绑成功：控制域独占清理旧 context（不再触发用户回调），再释放 driver 环。
    for (auto& reactor : reactors) {
        reactor->FinalizeControl();
        reactor->ReleaseDriver();
    }
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        impl->reactors.clear();
        impl->main_reactor.reset();
        impl->subs.clear();
        impl->phase = EnginePhase::Stopped;
        impl->control_cv.notify_all();
    }
    return final_error ? final_error : LastError();
}

std::error_code Engine::LocalAddresses(std::vector<Address>& out) const {
    std::lock_guard<std::mutex> lock(m_impl->mutex);
    if (m_impl->phase == EnginePhase::Idle || m_impl->phase == EnginePhase::Stopped) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    out = m_impl->local_addresses;
    return {};
}

std::size_t Engine::HandlerErrorCount() const {
    return m_impl->HandlerErrorCount();
}

std::size_t Engine::CountConnections() const {
    return m_impl->conn_count.load();
}

std::error_code Engine::ReactorOwnerIds(std::vector<std::uint64_t>& main_owners,
                                        std::vector<std::uint64_t>& sub_owners) const {
    std::lock_guard<std::mutex> lock(m_impl->mutex);
    main_owners.clear();
    sub_owners.clear();
    if (m_impl->phase == EnginePhase::Idle || m_impl->phase == EnginePhase::Stopped) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    std::size_t index = 0;
    if (m_impl->main_reactor != nullptr && !m_impl->slots.empty()) {
        main_owners.push_back(m_impl->slots[0].processer_id);
        index = 1;
    }
    for (; index < m_impl->slots.size(); ++index) {
        sub_owners.push_back(m_impl->slots[index].processer_id);
    }
    return {};
}


namespace {

std::error_code StartConnectImpl(const std::shared_ptr<EngineImpl>& impl,
                                 const Address& address, std::uint64_t& out_connect_id,
                                 bool awaitable) {
    // awaitable=true 是 CoDial 的提交步骤：要求无 raw handler（raw 配置下
    // OnConnect 是唯一交付，Co 不得提交一个无法领取的 op）。
    // awaitable=false 是公共 raw 提交：要求有 handler。
    if (awaitable == (impl->handler != nullptr)) {
        return std::make_error_code(std::errc::operation_not_supported);
    }
    std::uint64_t id = 0;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        if (impl->phase != EnginePhase::Running || impl->IsSealed()) {
            return std::make_error_code(std::errc::operation_canceled);
        }
        if (impl->subs.empty()) {
            return std::make_error_code(std::errc::operation_not_supported);
        }
        id = impl->next_connect_id.fetch_add(1, std::memory_order_relaxed);
    }
    auto reactor = impl->PickSub(address);
    if (!reactor) {
        return std::make_error_code(std::errc::operation_not_supported);
    }
    {
        std::lock_guard<std::mutex> lock(impl->connect_mutex);
        // 每次先清理已完结且无人等待项；若仍达硬上限（全 pending）则零接纳。
        for (auto it = impl->connects.begin(); it != impl->connects.end();) {
            bool removable = false;
            {
                std::lock_guard<std::mutex> op_lock(it->second->mutex);
                removable = it->second->done && !it->second->waiter_attached;
            }
            if (removable) {
                it = impl->connects.erase(it);
            } else {
                ++it;
            }
        }
        if (impl->connects.size() >= impl->connect_limit) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        auto op = std::make_shared<EngineImpl::ConnectOp>();
        op->reactor = reactor;
        op->waiter_attached = awaitable;  // Co 提交即登记接收意图，完成早到不丢
        impl->connects[id] = std::move(op);
    }
    // 私有 test：一次性"零接纳"投递失败注入。与真实失败走同一段回滚（清
    // receiver 意图、ResolveConnect 取消、清表项、out_id 不变、零回调），
    // 不构造假网络响应；未武装时行为与产品完全一致。
    const bool force_post_failure = impl->connect_post_fail_once.exchange(false);
    if (force_post_failure
        || !reactor->TryPost([impl, reactor, address, id] {
               reactor->BeginConnectOnOwner(address, id);
           })) {
        {
            std::lock_guard<std::mutex> lock(impl->connect_mutex);
            const auto it = impl->connects.find(id);
            if (it != impl->connects.end()) {
                std::lock_guard<std::mutex> op_lock(it->second->mutex);
                it->second->waiter_attached = false;
            }
        }
        impl->ResolveConnect(id, Connection(),
                             std::make_error_code(std::errc::operation_canceled));
        return std::make_error_code(std::errc::operation_canceled);
    }
    {
        // 私有 test 屏障：已成功 TryPost（owner 可能立即完成并回调）、尚未写
        // out_connect_id。不持任何 impl 锁，owner 线程可自由推进。
        std::function<void()> barrier;
        {
            std::lock_guard<std::mutex> barrier_lock(impl->connect_submit_barrier_mutex);
            barrier = std::move(impl->connect_submit_barrier);
            impl->connect_submit_barrier = {};
        }
        if (barrier) {
            barrier();
        }
    }
    out_connect_id = id;
    return {};
}

} // namespace

std::error_code Engine::AsyncConnect(const Address& address, std::uint64_t& out_connect_id) {
    // 只用于 raw 配置：OnConnect 是唯一交付，成功/失败恰一次且 operation 等于
    // 本次 out_connect_id；任何拒绝路径都不改 out_connect_id、不产生本次回调。
    return StartConnectImpl(m_impl, address, out_connect_id, /*awaitable=*/false);
}

std::error_code Engine::CoSubmitConnect(const Address& address, std::uint64_t& out_connect_id) {
    // CoDial 的提交步骤：登记接收意图，完成早到也保留结果供领取。
    return StartConnectImpl(m_impl, address, out_connect_id, /*awaitable=*/true);
}

// ---- EngineTestAccess（私有测试接缝，非产品 API）----

std::error_code EngineTestAccess::InjectDriverFailure(Engine& engine) {
    auto impl = engine.m_impl;
    std::lock_guard<std::mutex> lock(impl->mutex);
    if (impl->phase != EnginePhase::Running || impl->slots.empty()) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    impl->slots.back().driver->InjectTickFailureOnce();
    return {};
}

void EngineTestAccess::SetConnectLimit(Engine& engine, std::size_t limit) {
    auto impl = engine.m_impl;
    std::lock_guard<std::mutex> lock(impl->mutex);
    impl->connect_limit = limit == 0 ? 1 : limit;
}

std::size_t EngineTestAccess::ConnectTableSize(Engine& engine) {
    std::lock_guard<std::mutex> lock(engine.m_impl->connect_mutex);
    return engine.m_impl->connects.size();
}

std::size_t EngineTestAccess::PendingConnectCount(Engine& engine) {
    auto impl = engine.m_impl;
    std::lock_guard<std::mutex> lock(impl->connect_mutex);
    std::size_t pending = 0;
    for (auto& entry : impl->connects) {
        std::lock_guard<std::mutex> op_lock(entry.second->mutex);
        if (!entry.second->done) {
            ++pending;
        }
    }
    return pending;
}

void EngineTestAccess::SetConnectDeliveryDelay(Engine& engine,
                                               std::chrono::milliseconds delay) {
    engine.m_impl->connect_delivery_delay_ms.store(delay.count());
}

std::size_t EngineTestAccess::DirtyCount(Engine& engine) {
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    std::size_t total = 0;
    for (auto& reactor : reactors) {
        total += reactor->DirtyCount();
    }
    return total;
}

std::size_t EngineTestAccess::ConnectionCount(Engine& engine) {
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    std::size_t total = 0;
    for (auto& reactor : reactors) {
        total += reactor->ConnCount();
    }
    return total;
}

void EngineTestAccess::SetSubmitBarrierOnce(Engine& engine, std::function<void()> barrier) {
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->SetSubmitBarrier(barrier);
    }
}

void EngineTestAccess::SetConnectSubmitBarrierOnce(Engine& engine, std::function<void()> barrier) {
    auto impl = engine.m_impl;
    std::lock_guard<std::mutex> lock(impl->connect_submit_barrier_mutex);
    impl->connect_submit_barrier = std::move(barrier);
}

void EngineTestAccess::SetConnectPostFailureOnce(Engine& engine) {
    engine.m_impl->connect_post_fail_once.store(true);
}

void EngineTestAccess::SetConnectWaitStatusOnce(Engine& engine, int status) {
    engine.m_impl->connect_wait_status_once.store(status < 0 ? -1 : status);
}

void EngineTestAccess::ClearConnectInjection(Engine& engine) {
    auto impl = engine.m_impl;
    impl->connect_post_fail_once.store(false);
    impl->connect_wait_status_once.store(-1);
    std::lock_guard<std::mutex> lock(impl->connect_submit_barrier_mutex);
    impl->connect_submit_barrier = {};
}

void EngineTestAccess::KickOwner(Engine& engine) {
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->KickPumpOwner();
    }
}

void EngineTestAccess::SetSealDelay(Engine& engine, std::chrono::milliseconds delay) {
    engine.m_impl->seal_delay_ms.store(delay.count());
}

void EngineTestAccess::SetAuthBarriers(Engine& engine, std::function<void()> pre_auth,
                                       std::function<void()> post_auth) {
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->SetPreAuthBarrier(pre_auth);
        reactor->SetPostAuthBarrier(post_auth);
    }
}




bool EngineTestAccess::ConnectReceiverClaimed(Engine& engine, std::uint64_t id) {
    auto op = engine.m_impl->FindConnect(id);
    if (!op) {
        return false;
    }
    std::lock_guard<std::mutex> lock(op->mutex);
    return op->receiver_claimed;
}

std::size_t EngineTestAccess::RecvHoldBytes(Engine& engine) {
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    std::size_t total = 0;
    for (auto& reactor : reactors) {
        total += reactor->RecvHoldBytes();
    }
    return total;
}

std::size_t EngineTestAccess::FillConnectHub(
    Engine& engine, std::uint64_t id,
    std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>>& keep) {
    auto op = engine.m_impl->FindConnect(id);
    if (!op) {
        return 0;
    }
    std::size_t added = 0;
    for (std::size_t i = 0; i < WakeHub::kMaxWaiters + 1; ++i) {
        auto waiter = op->hub.Create();
        if (!op->hub.Add(waiter)) {
            break;
        }
        keep.push_back(std::move(waiter));
        ++added;
    }
    return added;
}

void EngineTestAccess::SetWriteReadyBarrierOnce(Engine& engine, std::function<void()> barrier) {
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->SetWriteReadyBarrier(barrier);
    }
}

void EngineTestAccess::SetConnectStartDelay(Engine& engine, std::chrono::milliseconds delay) {
    engine.m_impl->connect_start_delay_ms.store(delay.count());
}

void EngineTestAccess::SetCancelPostFailureOnce(Engine& engine) {
    engine.m_impl->cancel_post_fail_once.store(true);
}

void EngineTestAccess::SetShutdownBarrierOnce(Engine& engine, std::function<void()> barrier) {
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->SetShutdownBarrier(barrier);
    }
}

void EngineTestAccess::SetRemoveBarrierOnce(Engine& engine, std::function<void()> barrier) {
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->SetRemoveBarrier(barrier);
    }
}

void EngineTestAccess::SetDirtyBarrierOnce(Engine& engine, std::function<void()> barrier) {
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->SetDirtyBarrier(barrier);
    }
}

void EngineTestAccess::FailNextConnectWaiterCreateOnce(Engine& engine) {
    (void)engine;
    WakeHub::SetCreateFaultOnceForTest();
}

void EngineTestAccess::SetWaitDriverFailedOnce(Engine& engine) {
    engine.m_impl->test_wait_driver_failed.store(true);
}

void EngineTestAccess::SetSealThrowOnce(Engine& engine) {
    engine.m_impl->seal_throw_once.store(true);
}

void EngineTestAccess::SetInstallDelay(Engine& engine, std::chrono::milliseconds delay) {
    engine.m_impl->install_delay_ms.store(delay.count());
}

void EngineTestAccess::SetSealPostFailureOnce(Engine& engine) {
    engine.m_impl->seal_post_fail_once.store(true);
}

void EngineTestAccess::SetReadAppendHook(Engine& engine, std::function<void()> hook) {
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        reactor->SetReadAppendHook(hook);
    }
}

int EngineTestAccess::FirstConnectionFd(Engine& engine) {
    auto impl = engine.m_impl;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        reactors = impl->reactors;
    }
    for (auto& reactor : reactors) {
        const int fd = reactor->FirstConnFd();
        if (fd >= 0) {
            return fd;
        }
    }
    return -1;
}

// CoDial 的领取步骤（内部）：成功才写 out；任何未交付失败都归还 claim 并让
// owner 回收 fd。有 handler 的 Engine 在写任何等待/取消状态之前拒绝。
std::error_code Engine::CoWaitConnect(std::uint64_t connect_id, Connection& out,
                                      const bbt::coroutine::WaitOptions& options) {
    auto impl = m_impl;
    if (impl->handler) {
        // raw-only：不得在任何等待/取消状态写入之前改变 raw 建连责任。
        return std::make_error_code(std::errc::operation_not_supported);
    }
    auto op = impl->FindConnect(connect_id);
    if (!op) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    {
        std::lock_guard<std::mutex> lock(op->mutex);
        if (op->receiver_claimed) {
            return std::make_error_code(std::errc::device_or_resource_busy);
        }
        op->receiver_claimed = true;  // 单接收者；超时/取消后不自动释放
    }
    // 离开 CoDial 且未交付的唯一归还点：清 claim/attach、登记放弃意图；需要
    // 中断真实连接尝试时同时持久化取消（post 失败由控制循环重试）。owner 在
    // 真实完成或取消时看到「已放弃且无接收者」即回收未交付 fd，不调 owner 回调。
    auto abandon = [&](bool cancel_owner) {
        std::shared_ptr<Reactor> reactor;
        Connection orphan;
        {
            std::lock_guard<std::mutex> lock(op->mutex);
            op->receiver_claimed = false;
            op->waiter_attached = false;
            if (op->done) {
                // 已完成但本次领取已终结：连接此刻无主，直接回收。
                if (!op->raw_delivered) {
                    orphan = op->connection;
                }
                op->abandoned = false;
            } else {
                // 未完成：放弃接收意图，owner 稍后交付或取消时回收 fd。
                op->abandoned = true;
            }
            reactor = op->reactor;
            if (cancel_owner) {
                op->cancel_requested.store(true);
            }
        }
        if (orphan.Valid()) {
            orphan.RequestClose(std::make_error_code(std::errc::operation_canceled));
            impl->MaybeEraseConnect(connect_id, op);
        }
        if (!cancel_owner) {
            return;
        }
        if (reactor) {
            if (impl->cancel_post_fail_once.exchange(false)
                || !reactor->TryPost([reactor, connect_id] {
                       reactor->CancelConnectOnOwner(connect_id);
                   })) {
                impl->cancel_retry_needed.store(true);
            }
        } else {
            impl->cancel_retry_needed.store(true);
        }
    };

    // 终态 op 的收尾：只有无错误且确有连接的结果才算本次交付；否则不覆盖 out。
    auto settle_done = [&](Connection delivered, std::error_code done_error) {
        impl->MaybeEraseConnect(connect_id, op);
        if (done_error) {
            if (!impl->handler && delivered.Valid()) {
                // 无接收者的已交付连接：owner 交付时已登记，这里回收。
                delivered.RequestClose(std::make_error_code(std::errc::operation_canceled));
            }
            return done_error;
        }
        if (!delivered.Valid()) {
            return std::make_error_code(std::errc::operation_canceled);
        }
        out = std::move(delivered);
        return std::error_code{};
    };

    for (;;) {
        bool already_done = false;
        bool raw_conflict = false;
        Connection delivered;
        std::error_code done_error;
        {
            std::lock_guard<std::mutex> lock(op->mutex);
            if (op->done) {
                if (op->raw_delivered) {
                    raw_conflict = true;  // 已由 raw handler 唯一交付，不重复领取
                } else {
                    delivered = op->connection;
                    done_error = op->error;
                }
                op->waiter_attached = false;
                op->abandoned = false;
                already_done = true;
            } else {
                op->waiter_attached = true;
                op->abandoned = false;
            }
        }
        if (already_done) {
            if (raw_conflict) {
                impl->MaybeEraseConnect(connect_id, op);
                return std::make_error_code(std::errc::operation_not_supported);
            }
            return settle_done(std::move(delivered), done_error);
        }
        // 登记 → 复查 → 挂起。登记失败与建立失败都在下方向外归还 claim。
        bbt::coroutine::WaitStatus status = bbt::coroutine::WaitStatus::InvalidContext;
        bool add_failed = false;
        // 私有 test：一次性把"本次等待的返回值"固定成指定 WaitStatus，用于确定性
        // 覆盖非 Completed 的退出路径（InvalidContext / RuntimeUnavailable /
        // AlreadyWaiting）。注入点就在等待返回边界：不建立等待者、不等真实事件，
        // 其余（claim 已取得、op 仍 pending、下方 abandon(true) 的取消投递与 owner
        // 回收）全部走产品既有代码，不构造假网络响应。未武装时为纯产品路径。
        // Completed 不可注入：它表示"被通知后复查"，注入会在 op 仍 pending 时
        // 原地重试；夹带 Completed 的调用退化为真实等待，避免测试写出死循环。
        const int injected_wait_status = impl->connect_wait_status_once.exchange(-1);
        const bool inject_wait = injected_wait_status >= 0
            && injected_wait_status != static_cast<int>(bbt::coroutine::WaitStatus::Completed);
        if (inject_wait) {
            status = static_cast<bbt::coroutine::WaitStatus>(injected_wait_status);
        } else {
            try {
                auto waiter = op->hub.Create();
                status = waiter->WaitWithCallback(options, [&] {
                    if (!op->hub.Add(waiter)) {
                        add_failed = true;  // 满额：有界拒绝，不自激重试
                        waiter->Notify();
                        return true;
                    }
                    bool done = false;
                    {
                        std::lock_guard<std::mutex> lock(op->mutex);
                        done = op->done;
                    }
                    if (done) {
                        waiter->Notify();  // 锁外 Notify
                    }
                    return true;
                });
            } catch (...) {
                // 等待对象建立失败：本条领取没有等待者，必须放弃而不是留着 claim。
                abandon(false);
                return std::make_error_code(std::errc::not_enough_memory);
            }
        }
        if (add_failed) {
            // 满额时结果可能已就绪：那就直接交付；否则放弃并让 owner 回收。
            bool done = false;
            Connection ready;
            std::error_code ready_error;
            {
                std::lock_guard<std::mutex> lock(op->mutex);
                done = op->done && !op->raw_delivered;
                if (done) {
                    ready = op->connection;
                    ready_error = op->error;
                }
                op->waiter_attached = false;
            }
            if (done) {
                std::lock_guard<std::mutex> lock(op->mutex);
                op->receiver_claimed = false;
                op->abandoned = false;
            }
            if (done && !ready_error && ready.Valid()) {
                impl->MaybeEraseConnect(connect_id, op);
                out = std::move(ready);
                return {};
            }
            if (done) {
                impl->MaybeEraseConnect(connect_id, op);
                return std::make_error_code(std::errc::no_buffer_space);
            }
            abandon(false);
            return std::make_error_code(std::errc::no_buffer_space);
        }
        if (status == bbt::coroutine::WaitStatus::Completed) {
            continue;  // 被通知：回到顶部按真实结果复查
        }
        // 其余 WaitStatus 全部终结本次领取，且一律归还责任后再返回。
        bool done = false;
        Connection finished;
        std::error_code finished_error;
        {
            std::lock_guard<std::mutex> lock(op->mutex);
            done = op->done;
            if (done && !op->raw_delivered) {
                finished = op->connection;
                finished_error = op->error;
            }
            op->waiter_attached = false;
        }
        const auto mapped = WaitStatusError(status);
        if (done) {
            if (finished.Valid()) {
                // 等待已按本次仲裁终结：结果无主 → 回收（raw 已交付时不动）。
                finished.RequestClose(std::make_error_code(std::errc::operation_canceled));
            }
            impl->MaybeEraseConnect(connect_id, op);
            return mapped;
        }
        // 尚未完成：交还责任；超时/取消/上下文失效都要求中断真实尝试。
        abandon(true);
        return mapped;
    }
}

std::error_code Engine::CoWaitAccepted(Connection& out,
                                       const bbt::coroutine::WaitOptions& options) {
    auto impl = m_impl;
    // 配置与生命周期错误立即返回，不等一个永远不会来的 listener。
    // 判定优先级是契约（见 CoConnection.hpp）：终态先于配置/布局判定。Stopped 时
    // main accept owner 已解绑，但那是收口带来的结果，不能据此把 Engine 判成
    // client-only（否则把"已结束"误报成"这类 Engine 不支持 accept"）。
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        const bool sealed = impl->IsSealed();
        const auto phase = impl->phase;
        if (sealed || phase == EnginePhase::Closing || phase == EnginePhase::Stopped) {
            // 已封口或已开始/完成收口：队列已清空，不会有新连接交付。
            return std::make_error_code(std::errc::operation_canceled);
        }
        if (phase == EnginePhase::Idle || phase == EnginePhase::Starting) {
            // 尚未发布 Running：还没有 accept 生产者，也不是配置错误。
            return std::make_error_code(std::errc::operation_not_permitted);
        }
        if (!impl->handoff_enabled) {
            return std::make_error_code(std::errc::operation_not_permitted);
        }
        if (!impl->main_reactor) {
            // Running + handoff 但没有 main accept owner：真正的 client-only 布局，
            // handoff 永远不会有生产者。
            return std::make_error_code(std::errc::operation_not_supported);
        }
    }
    for (;;) {
        Connection popped;
        bool sealed = false;
        {
            std::lock_guard<std::mutex> lock(impl->handoff_mutex);
            if (!impl->handoff.empty()) {
                popped = impl->handoff.front();
                impl->handoff.pop_front();
            } else {
                sealed = impl->IsSealed();
            }
        }
        // 用本轮实际弹出的连接判断成功；调用者原有 out 有效不算。
        if (popped.Valid()) {
            out = popped;
            impl->ResumeAcceptIfFree();
            return {};
        }
        if (sealed) {
            return std::make_error_code(std::errc::operation_canceled);
        }
        auto waiter = impl->accept_hub.Create();
        if (!waiter) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        bool add_failed = false;
        const auto status = waiter->WaitWithCallback(options, [&] {
            if (!impl->accept_hub.Add(waiter)) {
                add_failed = true;
                waiter->Notify();
                return true;
            }
            bool ready = false;
            {
                std::lock_guard<std::mutex> lock(impl->handoff_mutex);
                ready = !impl->handoff.empty() || impl->IsSealed();
            }
            if (ready) {
                waiter->Notify();  // 锁外 Notify
            }
            return true;
        });
        if (add_failed) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        if (status != bbt::coroutine::WaitStatus::Completed) {
            return WaitStatusError(status);
        }
    }
}

} // namespace bbt::infra::net
