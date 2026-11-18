#include <bbt/infra/net/detail/Reactor.hpp>
#include <bbt/infra/net/detail/ConnCore.hpp>
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
#include <bbt/infra/net/detail/WaitSet.hpp>

namespace bbt::infra::net {

std::error_code FromBoost(const boost::system::error_code& error)
{
    if (!error) {
        return {};
    }
    return std::error_code(error.value(), std::system_category());
}

namespace {

constexpr std::size_t kPumpBudget = 32;

int GetSocketError(int fd)
{
    int value = 0;
    socklen_t length = sizeof(value);
    if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &value, &length) != 0) {
        return errno;
    }
    return value;
}

std::error_code ApplyStreamOptions(int fd, const NormalizedOptions& options)
{
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
        int value =
            static_cast<int>(std::min<std::size_t>(options.socket_recv_buffer, static_cast<std::size_t>(INT_MAX)));
        if (::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &value, sizeof(value)) != 0) {
            note(std::error_code(errno, std::system_category()));
        }
    }
    if (options.socket_send_buffer != 0) {
        int value =
            static_cast<int>(std::min<std::size_t>(options.socket_send_buffer, static_cast<std::size_t>(INT_MAX)));
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
        int count = static_cast<int>(std::min<std::size_t>(options.tcp_keep_count, static_cast<std::size_t>(INT_MAX)));
        if (count > 0 && ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &count, sizeof(count)) != 0) {
            note(std::error_code(errno, std::system_category()));
        }
    }
    return first;
}

std::error_code ApplyListenerOptions(NativeSocket& socket, const NormalizedOptions& options)
{
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
        note(socket.SetReadBuffer(
            static_cast<int>(std::min<std::size_t>(options.socket_recv_buffer, static_cast<std::size_t>(INT_MAX)))));
    }
    if (options.socket_send_buffer != 0) {
        note(socket.SetWriteBuffer(
            static_cast<int>(std::min<std::size_t>(options.socket_send_buffer, static_cast<std::size_t>(INT_MAX)))));
    }
    return first;
}

// clang-format off
}
// clang-format on

/**
 * ---- Reactor definitions ----
 */
Reactor::Reactor(EngineImpl* impl, std::size_t index, std::shared_ptr<ReactorDriver> driver)
    : m_impl(impl)
    , m_index(index)
    , m_driver(std::move(driver))
{
}

Reactor::~Reactor() = default;

bool Reactor::ImplSealed() const
{
    return m_impl->IsSealed();
}

std::size_t Reactor::ConnCount()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_conns.size();
}

std::size_t Reactor::ReadLowWater() const
{
    return m_impl->Options().max_inbound_bytes / 2;
}

std::error_code Reactor::InstallOnOwner(const std::vector<Address>& addresses)
{
    const auto install_delay = m_impl->DelayCount(DelayPoint::Install);
    if (install_delay > 0) {
        timespec ts{};
        ts.tv_sec = install_delay / 1000;
        ts.tv_nsec = (install_delay % 1000) * 1000000L;
        (void)::syscall(SYS_nanosleep, &ts, nullptr);
    }
    if (!addresses.empty() && m_impl->AcceptStopRequested()) {
        return std::make_error_code(std::errc::operation_canceled);
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
            error = ApplyListenerOptions(socket, m_impl->Options());
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
    if (!addresses.empty() && m_impl->AcceptStopRequested()) {

        /**
         * Run 前/安装中已封口：本次安装不得留存监听，把已打开的 fd 关闭、
         * 返回错误，由 Run 走安装失败收口、不发布 Running。
         */
        boost::system::error_code ignored;
        for (auto& listener : m_listeners) {
            listener->desc.close(ignored);
        }
        m_listeners.clear();
        return std::make_error_code(std::errc::operation_canceled);
    }
    ArmAcceptAll();
    return {};
}

std::vector<Address> Reactor::LocalAddresses() const
{
    std::vector<Address> out;
    out.reserve(m_listeners.size());
    for (const auto& listener : m_listeners) {
        out.push_back(listener->local);
    }
    return out;
}

void Reactor::ArmAcceptAll()
{
    if (!m_installed.load() || m_sealed.load() || m_impl->IsSealed() || m_impl->AcceptStopRequested()) {

        /**
         * 接收域已独立封口：不再重挂监听
         */
        return;
    }
    for (auto& listener : m_listeners) {
        ArmAccept(listener);
    }
}

void Reactor::ArmAccept(const std::shared_ptr<Listener>& listener)
{
    if (!m_installed.load() || m_sealed.load() || m_impl->IsSealed() || m_impl->AcceptStopRequested()) {

        /**
         * 封口后不再 arm，释放的预留由 Reservation 析构回收
         */
        return;
    }
    bool expected = false;
    if (!listener->armed.compare_exchange_strong(expected, true)) {
        return;
    }

    /**
     * 先预留有界 handoff 容量，再做 native accept；满则暂停（不 arm）。
     */
    auto reservation = m_impl->TryReserveHandoff();
    if (!reservation) {
        listener->armed.store(false);
        return;
    }
    auto self = shared_from_this();
    listener->desc.async_wait(
        TcpWait::wait_read, [self, listener, reservation](const boost::system::error_code& error) mutable {
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

void Reactor::DoAccept(const std::shared_ptr<Listener>& listener, std::shared_ptr<HandoffReservation> reservation)
{
    if (m_sealed.load() || !m_installed.load() || m_impl->IsSealed()) {
        return;
    }
    sockaddr_storage storage{};
    socklen_t length = sizeof(storage);

    /**
     * 每次只接纳一个：等 sub 完成登记后再重挂 main（LB 看到真实活跃数）。
     */
    const int fd = static_cast<int>(::syscall(
        SYS_accept4,
        listener->desc.native_handle(),
        reinterpret_cast<sockaddr*>(&storage),
        &length,
        SOCK_NONBLOCK | SOCK_CLOEXEC));
    if (fd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {

            /**
             * 虚假就绪：释放预留后重挂（封口后 ArmAccept 不再重挂）
             */
            ArmAccept(listener);
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
    if (m_impl->AcceptStopRequested()) {

        /**
         * fd 由 handle 析构物理关闭，预留随 reservation 析构释放，不重挂监听。
         */
        return;
    }
    Address peer = Address::FromNative(reinterpret_cast<sockaddr*>(&storage), length);
    std::shared_ptr<Reactor> target = m_impl->PickSub(peer);
    if (!target) {

        /**
         * 无 sub owner：不接纳，fd 由 FdHandle 关闭，预留释放。
         */
        return;
    }
    if (!target->TryPost([target, handle = std::move(handle), reservation]() mutable {
            // clang-format off

            /**
             * is_connect=
             */
            // clang-format on
            target->AdoptFdOnOwner(std::move(handle), false, 0, reservation);
        })) {
        m_impl->RecordError(std::make_error_code(std::errc::not_enough_memory));

        /**
         * post 失败：本次未移交，立即恢复接纳
         */
        ResumeMainAccept();
    }

    /**
 * 成功路径由 sub 在 ConnCount 更新后 ResumeMainAccept() 重挂（串行化）。
 */
}

void Reactor::RecordAcceptError(std::error_code error)
{
    m_impl->RecordError(error);
}

void Reactor::AdoptFdOnOwner(
    FdHandle fd,
    bool is_connect,
    std::uint64_t connect_id,
    std::shared_ptr<HandoffReservation> reservation,
    bool delayed_delivery,
    const std::shared_ptr<ReclaimRecord>& reclaim)
{
    if (m_sealed.load() || m_impl->IsSealed() || fd.Get() < 0) {
        if (is_connect) {
            const auto error = std::make_error_code(std::errc::operation_canceled);

            /**
             * 真实 close 点：显式关闭（原路径依赖 FdHandle 析构的隐式关闭），
             * 之后才允许把本次责任标记为释放。
             */
            (void)fd.Close();
            MarkReclaimReleased(reclaim);
            if (m_impl->Handler()) {
                SafeHandler(
                    [this, connect_id, error] { m_impl->Handler()->OnConnect(Connection(), error, connect_id); });
            }
            m_impl->ResolveConnect(connect_id, Connection(), error);
            FinishConnectCallback();
        } else {
            ResumeMainAccept();
        }

        /**
         * fd 已显式关闭或从未持有；reservation 析构释放
         */
        return;
    }
    if (is_connect && !delayed_delivery) {
        const auto delay_ms = m_impl->DelayCount(DelayPoint::ConnectDelivery);
        if (delay_ms > 0) {

            /**
             * 私有 test seam：延后「已完成 connect 的交付」，用于确定性
             * timeout/adopt 竞态；fd 由 holder RAII 覆盖关闭竞争，记录随
             * holder 同行，取消/失败关闭 fd 后仍可标记释放（不依赖 op 查表）。
             */
            auto holder = std::make_shared<FdHandle>(std::move(fd));
            m_delivery_fds.push_back(holder);
            auto timer = std::make_shared<asio::steady_timer>(Ctx(), std::chrono::milliseconds(delay_ms));
            auto self = shared_from_this();
            timer->async_wait([self, timer, holder, connect_id, reclaim](const boost::system::error_code& error) {
                auto& held = self->m_delivery_fds;
                held.erase(std::remove(held.begin(), held.end(), holder), held.end());
                if (error) {

                    /**
                     * 交付等待被撤销：真实 close 点（holder 显式关闭）后收口。
                     */
                    (void)holder->Close();
                    MarkReclaimReleased(reclaim);
                    return;
                }
                self->AdoptFdOnOwner(
                    FdHandle(std::move(*holder)),
                    true,
                    connect_id,
                    nullptr,

                    /**
                     * delayed_delivery=
                     */
                    true,
                    reclaim);
            });
            return;
        }
    }
    TcpWait descriptor(Ctx());
    boost::system::error_code boost_error;
    const int adopted_fd = fd.Release();
    descriptor.assign(adopted_fd, boost_error);
    if (boost_error) {

        /**
         * assign 失败：descriptor 未取得所有权，此处是真实 close 点。
         */
        ::close(adopted_fd);
        m_impl->RecordError(FromBoost(boost_error));
        if (is_connect) {
            MarkReclaimReleased(reclaim);
            if (m_impl->Handler()) {
                SafeHandler([this, connect_id, boost_error] {
                    m_impl->Handler()->OnConnect(Connection(), FromBoost(boost_error), connect_id);
                });
            }
            m_impl->ResolveConnect(connect_id, Connection(), FromBoost(boost_error));
            FinishConnectCallback();
        }
        return;
    }
    descriptor.non_blocking(true, boost_error);
    if (boost_error) {
        boost::system::error_code close_ignored;

        /**
         * 真实 close 点；RecordError 仍用原错误
         */
        descriptor.close(close_ignored);
        m_impl->RecordError(FromBoost(boost_error));
        if (is_connect) {
            MarkReclaimReleased(reclaim);
            if (m_impl->Handler()) {
                SafeHandler([this, connect_id, boost_error] {
                    m_impl->Handler()->OnConnect(Connection(), FromBoost(boost_error), connect_id);
                });
            }
            m_impl->ResolveConnect(connect_id, Connection(), FromBoost(boost_error));
            FinishConnectCallback();
        }
        return;
    }
    if (const auto option_error = ApplyStreamOptions(descriptor.native_handle(), m_impl->Options())) {
        m_impl->RecordError(option_error);
    }
    const std::uint64_t id = m_impl->NextConnectionId();

    /**
     * 责任转交：带句柄 CoDial 的记录原样随连接转入 owner 责任（不提前标释放）；
     * accept/raw 连接另建活跃记录供 Connection::Released() 观察。
     */
    auto owner_record = reclaim ? reclaim : std::make_shared<ReclaimRecord>(false);
    auto state = std::make_shared<ConnectionState>(
        id,
        m_impl->Options().max_inbound_bytes,
        m_impl->Options().max_outbound_bytes,
        m_impl->Options().max_pending_events,
        m_impl->SealToken(),
        std::move(owner_record),
        m_impl->Hooks());
    auto core = std::make_shared<ConnCore>(state, std::move(descriptor), this, m_impl->Handler());
    std::weak_ptr<Reactor> weak_self = weak_from_this();
    std::weak_ptr<ConnCore> weak_core = core;
    auto mark_dirty = [weak_self, weak_core] {
        if (auto reactor = weak_self.lock()) {
            if (auto conn = weak_core.lock()) {
                reactor->MarkDirty(conn);
            }
        }
    };
    state->AttachOwner(mark_dirty);
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_conns.emplace(id, core);
    }
    m_impl->AddConnection();
    Connection connection(state);

    if (is_connect) {
        m_impl->AuthorizeConnect(connect_id);

        /**
         * 开始授权点：紧邻交付的同一原子检查（构造窗口内封口不启动成功回调）。
         */
        InvokePreAuthBarrier();
        if (m_impl->IsSealed()) {
            const auto canceled = std::make_error_code(std::errc::operation_canceled);
            if (m_impl->Handler()) {
                SafeHandler(
                    [this, connect_id, canceled] { m_impl->Handler()->OnConnect(Connection(), canceled, connect_id); });
            }
            m_impl->ResolveConnect(connect_id, Connection(), canceled);
            core->CancelControl();
            FinishConnectCallback();
            return;
        }
        InvokePostAuthBarrier();
        if (!m_impl->ResolveConnect(connect_id, connection, {})) {

            /**
             * 等待者已放弃且无 raw 接收者：owner 回收未交付 fd，不回调。
             */
            core->ShutdownOwner(std::make_error_code(std::errc::operation_canceled));
            FinishConnectCallback();
            return;
        }
        if (m_impl->Handler()) {
            SafeHandler([this, connection, connect_id] { m_impl->Handler()->OnConnect(connection, {}, connect_id); });
        }
        FinishConnectCallback();
    } else {

        /**
         * OnAccept 的同一开始授权点：入口早查不是授权，最后一次紧邻派发的
         * 原子检查才与封口线性化。
         */
        InvokePreAuthBarrier();
        const bool authorized = m_impl->AuthorizeAccept();
        if (!authorized) {
            core->CancelControl();
            RemoveConn(core);
            return;
        }
        InvokePostAuthBarrier();
        if (m_impl->AcceptEnabled() && reservation) {
            if (!m_impl->CommitHandoff(connection, reservation)) {
                core->ShutdownOwner(std::make_error_code(std::errc::operation_canceled));
                return;
            }
        } else if (reservation) {
            reservation->Commit();
        }

        /**
         * handoff 与 raw 不互吞：raw 有 handler 仍交付 OnAccept。
         */
        if (m_impl->Handler()) {
            SafeHandler([this, connection] { m_impl->Handler()->OnAccept(connection); });
        }
    }
    core->StartOwner();
    if (!is_connect) {
        ResumeMainAccept();
    }
}

void Reactor::BeginConnectOnOwner(
    const Address& address, std::uint64_t connect_id, const std::shared_ptr<ReclaimRecord>& reclaim)
{
    if (m_sealed.load() || m_impl->IsSealed()) {

        /**
         * 尚未创建任何 fd：本次责任即时收口。
         */
        MarkReclaimReleased(reclaim);
        if (m_impl->Handler()) {
            SafeHandler([this, connect_id] {
                m_impl->Handler()->OnConnect(
                    Connection(), std::make_error_code(std::errc::operation_canceled), connect_id);
            });
        }
        m_impl->ResolveConnect(connect_id, Connection(), std::make_error_code(std::errc::operation_canceled));
        return;
    }
    const auto start_delay = m_impl->DelayCount(DelayPoint::ConnectStart);
    if (start_delay > 0) {

        /**
         * 私有 test 屏障：占位 pending（尚未建 socket），取消/封口可及时撤销。
         */
        auto pending = std::make_shared<ConnectPending>();
        pending->connect_id = connect_id;
        auto timer = std::make_shared<asio::steady_timer>(Ctx(), std::chrono::milliseconds(start_delay));
        pending->start_timer = timer;
        m_pending_connects.push_back(pending);
        auto self = shared_from_this();
        ++m_pending_connect_callbacks;
        timer->async_wait([self, pending, connect_id, address, reclaim](const boost::system::error_code& error) {
            auto& list = self->m_pending_connects;
            list.erase(std::remove(list.begin(), list.end(), pending), list.end());
            if (error && error != asio::error::operation_aborted) {
                const auto from_boost = FromBoost(error);

                /**
                 * 定时器真实失败：未创建 fd，责任即时收口。
                 */
                MarkReclaimReleased(reclaim);
                if (self->m_impl->Handler()) {
                    self->SafeHandler([self, connect_id, from_boost] {
                        self->m_impl->Handler()->OnConnect(Connection(), from_boost, connect_id);
                    });
                }
                self->m_impl->ResolveConnect(connect_id, Connection(), from_boost);
                self->FinishConnectCallback();
                return;
            }
            if (self->m_sealed.load() || self->m_impl->IsSealed() || pending->canceled) {
                const auto canceled = std::make_error_code(std::errc::operation_canceled);

                /**
                 * 占位被取消/封口：未创建 fd，责任即时收口。
                 */
                MarkReclaimReleased(reclaim);
                if (self->m_impl->Handler()) {
                    self->SafeHandler([self, connect_id, canceled] {
                        self->m_impl->Handler()->OnConnect(Connection(), canceled, connect_id);
                    });
                }
                self->m_impl->ResolveConnect(connect_id, Connection(), canceled);
                self->FinishConnectCallback();
                return;
            }
            self->StartConnectSocketOnOwner(address, connect_id, reclaim);
        });
        return;
    }
    ++m_pending_connect_callbacks;
    StartConnectSocketOnOwner(address, connect_id, reclaim);
}

void Reactor::StartConnectSocketOnOwner(
    const Address& address, std::uint64_t connect_id, const std::shared_ptr<ReclaimRecord>& reclaim)
{

    /**
     * 测试：已在起点、尚未做最终封口检查
     */
    InvokeConnectStartBarrier();
    if (m_sealed.load() || m_impl->IsSealed()) {

        /**
         * 未创建 fd：责任即时收口。
         */
        MarkReclaimReleased(reclaim);
        if (m_impl->Handler()) {
            SafeHandler([this, connect_id] {
                m_impl->Handler()->OnConnect(
                    Connection(), std::make_error_code(std::errc::operation_canceled), connect_id);
            });
        }
        m_impl->ResolveConnect(connect_id, Connection(), std::make_error_code(std::errc::operation_canceled));

        /**
         * 两个调用方（定时器路径与直通路径）都已先占用一次 pending connect
         * 回调预算；本路径从未登记 pending 描述符，seal 只能等预算归零，
         * 因此必须在此归还，否则 SealServiceOnOwner 永久 defer、Run 收口
         * 无限等待。
         */
        FinishConnectCallback();
        return;
    }
    std::error_code error;
    NativeSocket socket = NativeSocket::Open(address.Family(), SocketType::Stream, error);
    if (!error) {
        error = ApplyStreamOptions(socket.Fd(), m_impl->Options());
    }
    if (!error) {
        error = socket.Connect(address);
        if (error && error.value() != EINPROGRESS && error.value() != EWOULDBLOCK) {

            /**
         * fallthrough: close and report
         */
        } else {
            error.clear();
        }
    }
    if (error) {

        /**
         * 真实 close 点：显式关闭已打开的 socket（原路径依赖析构隐式关闭），
         * 之后才把本次责任标记为释放。
         */
        (void)socket.Close();
        MarkReclaimReleased(reclaim);
        if (m_impl->Handler()) {
            SafeHandler([this, connect_id, error] { m_impl->Handler()->OnConnect(Connection(), error, connect_id); });
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

        /**
         * assign 失败：descriptor 未取得所有权，此处是真实 close 点
         */
        ::close(fd);
        MarkReclaimReleased(reclaim);
        if (m_impl->Handler()) {
            SafeHandler([this, connect_id, boost_error] {
                m_impl->Handler()->OnConnect(Connection(), FromBoost(boost_error), connect_id);
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
        descriptor->async_wait(
            TcpWait::wait_write,
            [self, descriptor, pending, connect_id, reclaim](const boost::system::error_code& wait_error) {
                // clang-format off

                /**
                 * 从 pending 列表移除。
                 */
                // clang-format on
                auto& list = self->m_pending_connects;
                list.erase(std::remove(list.begin(), list.end(), pending), list.end());
                if (pending->canceled) {

                    /**
                     * 真实 close 点：取消后不再使用该描述符，显式关闭并收口记录
                     * （不把 released 留给捕获析构的隐式时序）。
                     */
                    boost::system::error_code close_ignored;
                    descriptor->close(close_ignored);
                    MarkReclaimReleased(reclaim);
                    if (self->m_impl->Handler()) {
                        self->SafeHandler([self, connect_id] {
                            self->m_impl->Handler()->OnConnect(
                                Connection(), std::make_error_code(std::errc::operation_canceled), connect_id);
                        });
                    }
                    self->m_impl->ResolveConnect(
                        connect_id, Connection(), std::make_error_code(std::errc::operation_canceled));
                    self->FinishConnectCallback();
                    return;
                }
                if (wait_error) {
                    const auto error = FromBoost(wait_error);

                    /**
                     * 真实 close 点：等待失败，显式关闭并收口记录。
                     */
                    boost::system::error_code close_ignored;
                    descriptor->close(close_ignored);
                    MarkReclaimReleased(reclaim);
                    if (self->m_impl->Handler()) {
                        self->SafeHandler([self, connect_id, error] {
                            self->m_impl->Handler()->OnConnect(Connection(), error, connect_id);
                        });
                    }
                    self->m_impl->ResolveConnect(connect_id, Connection(), error);
                    self->FinishConnectCallback();
                    return;
                }
                const int socket_error = GetSocketError(descriptor->native_handle());
                if (socket_error != 0) {
                    const auto error = std::error_code(socket_error, std::system_category());

                    /**
                     * 真实 close 点：SO_ERROR 失败（如拒连），显式关闭并收口记录。
                     */
                    boost::system::error_code close_ignored;
                    descriptor->close(close_ignored);
                    MarkReclaimReleased(reclaim);
                    if (self->m_impl->Handler()) {
                        self->SafeHandler([self, connect_id, error] {
                            self->m_impl->Handler()->OnConnect(Connection(), error, connect_id);
                        });
                    }
                    self->m_impl->ResolveConnect(connect_id, Connection(), error);
                    self->FinishConnectCallback();
                    return;
                }
                const int connected_fd = descriptor->release();

                /**
                 * fd 转交成功：记录随 fd 进入 AdoptFdOnOwner，不在此标释放。
                 */
                self->AdoptFdOnOwner(
                    FdHandle(connected_fd),

                    /**
                     * is_connect=
                     */
                    true,
                    connect_id,
                    nullptr,

                    /**
                     * delayed_delivery=
                     */
                    false,
                    reclaim);
            });
    } catch (...) {

        /**
         * 真实 close 点：等待未建立，显式关闭并收口记录（原路径由 finalize 兜底）。
         */
        boost::system::error_code close_ignored;
        descriptor->close(close_ignored);
        MarkReclaimReleased(reclaim);
        FinishConnectCallback();
        const auto error = std::make_error_code(std::errc::not_enough_memory);
        if (m_impl->Handler()) {
            SafeHandler([this, connect_id, error] { m_impl->Handler()->OnConnect(Connection(), error, connect_id); });
        }
        m_impl->ResolveConnect(connect_id, Connection(), error);
    }
}

void Reactor::MarkDirty(const std::shared_ptr<ConnCore>& core)
{
    if (!core || m_sealed.load() || m_impl->IsSealed()) {
        return;
    }
    bool expected = false;
    if (!core->m_dirty_queued.compare_exchange_strong(expected, true)) {
        return;
    }
    InvokeHook(HookPoint::AfterDirtyWeakLock);
    {

        /**
         * 复用连接登记的窄节点：入队不分配。registry 移除与 dirty 入链
         * 用同一锁序（dirty -> m_mutex）共同登记/撤销：节点已 RemoveConn 时拒绝
         * 入链，避免晚 Wake 把已析构 core 裸指针挂链。
         */
        std::lock_guard<std::mutex> dirty_lock(m_dirty_mutex);
        if (m_sealed.load()) {
            core->m_dirty_queued.store(false);
            return;
        }
        {
            std::lock_guard<std::mutex> reg_lock(m_mutex);
            if (m_conns.find(core->Identity()) == m_conns.end()) {
                core->m_dirty_queued.store(false);

                /**
                 * 已撤销登记：明确拒绝晚 Wake
                 */
                return;
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

void Reactor::PumpOwner()
{
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
            const auto it = m_conns.find(raw->Identity());
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

        /**
         * 跨连接批次时间预算：剩余留在 dirty 链表，下一轮继续（不丢不睡死）。
         */
        if (std::chrono::steady_clock::now() - begin >= std::chrono::milliseconds(1)) {
            break;
        }
    }
}

void Reactor::PumpAllConnsOwner()
{
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

void Reactor::RemoveConn(const std::shared_ptr<ConnCore>& core)
{
    if (!core) {
        return;
    }
    const std::uint64_t id = core->Identity();
    bool removed = false;
    {

        /**
         * 与 MarkDirty 真正共门：解链与 registry 擦除在同一 dirty→registry
         * 锁域内原子完成。若两段之间释放 dirty 锁，MarkDirty 可看到 registry
         * 仍在而重新入链，随后 erase 使裸节点悬空。
         */
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

        /**
         * 测试：撤链后、擦表前（仍持 dirty+registry）
         */
        InvokeRemoveBarrier();
        removed = m_conns.erase(id) != 0;
    }
    if (removed) {
        m_impl->RemoveConnection();
    }
}

void Reactor::FinishConnectCallback()
{
    if (m_pending_connect_callbacks == 0) {
        return;
    }
    --m_pending_connect_callbacks;
    if (m_pending_connect_callbacks == 0 && m_on_pending_zero) {
        auto done = std::move(m_on_pending_zero);
        m_on_pending_zero = nullptr;

        /**
         * 最后一个 pending connect 终态已在原 owner 兑现
         */
        done();
    }
}

void Reactor::SealServiceOnOwner(std::function<void()> on_done)
{
    const auto seal_delay = m_impl->DelayCount(DelayPoint::Seal);
    if (seal_delay > 0) {

        /**
         * owner 线程在 coroutine hook 覆盖下，用原始 nanosleep 避免 hook 断言。
         */
        timespec ts{};
        ts.tv_sec = seal_delay / 1000;
        ts.tv_nsec = (seal_delay % 1000) * 1000000L;
        (void)::syscall(SYS_nanosleep, &ts, nullptr);
    }
    {

        /**
         * 先解除 dirty 链（不分配），再关闭/清空 conns，避免裸链节点悬空。
         */
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

    /**
     * 幂等：重试/重复调用仍完成剩余 conns 与 pending 收口（ShutdownOwner 幂等），
     * 不因第一次中途异常而丢掉未兑现责任。
     */
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

    /**
     * 逐个 owner 结算后再从 registry erase：先 erase/先 closed 会丢未兑现的
     * OnWrite/OnClose 责任。ShutdownOwner 分阶段幂等，异常可由重试续做。
     */
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

            /**
             * 责任未完成：保留 registry，下次 seal 重试
             */
            break;
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
        m_impl->PublishConnectionCount(m_conns.size());
    }
    if (m_pending_connect_callbacks == 0) {
        if (on_done) {
            on_done();
        }
    } else {

        /**
         * 等 cancel completion 在原 owner 交付 OnConnect(canceled) 后再完成 seal。
         */
        m_on_pending_zero = std::move(on_done);
    }
}

void Reactor::StopAcceptingOnOwner()
{

    /**
     * 接收域独立封口：撤该 owner 的全部 listener 并物理关闭 fd。cancel 先让
     * pending async_wait 以 operation_aborted 返回（arm 时已复位 armed，不再
     * 重挂），close 即物理停用；失败不抛，fd 关闭责任在此兑现。
     */
    boost::system::error_code ignored;
    for (auto& listener : m_listeners) {
        listener->desc.cancel(ignored);
        listener->desc.close(ignored);
    }

    /**
     * 保留不可变地址元数据，避免与启动控制线程读取地址快照并发擦除；
     * 物理 fd 已关闭，最终解绑后由 FinalizeControl 释放对象。
     * 物理关闭确认后才置位：AcceptingStopped()==true 等价于本代 listener 全部
     * 真实关闭（并行路径：控制域同代解绑收口亦调用 MarkAcceptStopDone）。
     */
    m_impl->MarkAcceptStopDone();
}

void Reactor::FinalizeControl()
{
    {

        /**
         * 先解除 dirty 链（不分配），再关闭/清空 conns，避免裸链节点悬空。
         */
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

        /**
     * 仍确保资源关闭（幂等）。
     */
    }
    boost::system::error_code ignored;
    for (auto& listener : m_listeners) {
        listener->desc.close(ignored);
    }
    m_listeners.clear();

    /**
     * 解绑只保证 owner 不再执行，不保证异步回调捕获已析构。
     * 先关闭那些仍可能被回调强引用的真实资源，再清容器及发布回执。
     */
    for (auto& pending : m_pending_connects) {
        pending->canceled = true;

        /**
         * start_timer 此时尚无 fd；owner 已解绑，不靠其取消或析构证明回收。
         */
        if (pending->desc) {
            pending->desc->close(ignored);
        }
    }
    m_pending_connects.clear();
    for (auto& holder : m_delivery_fds) {
        (void)holder->Close();
    }
    m_delivery_fds.clear();
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
    m_impl->PublishConnectionCount(0);
}

void Reactor::ReleaseDriver()
{

    /**
     * 必须在 FinalizeControl 之后、解绑成功之后调用；此后不再访问 context。
     */
    std::lock_guard<std::mutex> lock(m_driver_mutex);
    m_driver.reset();
}

void Reactor::CancelConnectOnOwner(std::uint64_t connect_id)
{
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

void Reactor::CancelAllRequestedOnOwner()
{
    for (auto& pending : m_pending_connects) {
        if (!m_impl->ConnectCancelRequested(pending->connect_id)) {
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

void Reactor::ResumeMainAccept()
{
    auto main = m_impl->MainReactor();
    if (main) {
        (void)main->TryPost([main] { main->ArmAcceptAll(); });
    }
}

std::size_t Reactor::RecvHoldBytes()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    std::size_t total = 0;
    for (auto& entry : m_conns) {
        total += entry.second->RecvHold();
    }
    return total;
}

std::size_t Reactor::DirtyCount()
{
    std::lock_guard<std::mutex> lock(m_dirty_mutex);
    std::size_t count = 0;
    for (ConnCore* node = m_dirty_head; node != nullptr; node = node->m_dirty_next) {
        ++count;
    }
    return count;
}

int Reactor::FirstConnFd()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    for (auto& entry : m_conns) {
        return entry.second->Fd();
    }
    return -1;
}

// clang-format off
}
// clang-format on
