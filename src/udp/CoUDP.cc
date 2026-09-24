#include <bbt/infra/CoUDP.hpp>

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <string>

#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>

#include "detail/IoSupport.hpp"

namespace bbt::infra::udp {
namespace {

using bbt::coroutine::sync::CoWaiter;
using bbt::coroutine::sync::CombinedWaitOptions;
using bbt::coroutine::sync::CombinedWaitStatus;

int _SetNonBlocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        return -1;
    return 0;
}

Error _Errno(ErrorCode code, const char* message) {
    Error error = MakeError(code, message);
    error.backend_category = "errno";
    error.backend_code = errno;
    return error;
}

// 值类型 ⇄ sockaddr_in（最小切片仅 IPv4）。
result<sockaddr_in> _ToSockaddr(const SocketAddress& address) {
    if (address.ip.empty())
        return result<sockaddr_in>::err(MakeError(ErrorCode::InvalidArgument,
            "SocketAddress.ip must not be empty"));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    if (::inet_pton(AF_INET, address.ip.c_str(), &addr.sin_addr) != 1)
        return result<sockaddr_in>::err(MakeError(ErrorCode::InvalidArgument,
            "SocketAddress.ip is not a numeric IPv4 address"));
    addr.sin_port = ::htons(address.port);
    return result<sockaddr_in>::ok(addr);
}

SocketAddress _FromSockaddr(const sockaddr_in& addr) {
    SocketAddress out;
    char ip[INET_ADDRSTRLEN]{};
    if (::inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip)) != nullptr)
        out.ip = ip;
    out.port = ::ntohs(addr.sin_port);
    return out;
}

// 一次 recvmsg（MSG_TRUNC 检测截断）：Ok/WouldBlock/错误三态。
// 零长度 datagram 合法：bytes=0、truncated=false、peer 有效。
// dst.size=0 且有报文：bytes=0、truncated=true（报头边界仍返回）。
enum class RecvOutcome { Ok, WouldBlock, Error };

RecvOutcome _RecvOnce(int fd, MutableBytes dst, SocketAddress& peer,
                      std::size_t& bytes, bool& truncated, Error& error) {
    iovec iov{};
    iov.iov_base = dst.data;
    iov.iov_len  = dst.size;
    sockaddr_in peer_addr{};
    msghdr msg{};
    msg.msg_name    = &peer_addr;
    msg.msg_namelen = sizeof(peer_addr);
    msg.msg_iov     = &iov;
    msg.msg_iovlen  = 1;
    // The adapter owns the wait: bypass the coroutine's transparent hook.
    const ssize_t n = ::recvmsg(fd, &msg, MSG_TRUNC | MSG_DONTWAIT);
    if (n >= 0) {
        peer  = _FromSockaddr(peer_addr);
        bytes = std::min(static_cast<std::size_t>(n), dst.size);
        // MSG_TRUNC 置位：报文超 dst.size，超出部分已被内核丢弃；
        // 报文恰好等于容量时 MSG_TRUNC 不置位（Linux 语义）。
        truncated = (msg.msg_flags & MSG_TRUNC) != 0;
        return RecvOutcome::Ok;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK)
        return RecvOutcome::WouldBlock;
    if (errno == EINTR)
        return RecvOutcome::WouldBlock; // 可等待方法由上层检查终止条件后重试
    error = _Errno(ErrorCode::TransportError, "UDP receive failed");
    return RecvOutcome::Error;
}

RecvOutcome _SendOnce(int fd, ConstBytes packet, const sockaddr* addr,
                      socklen_t addr_len, std::size_t& bytes, Error& error) {
    const ssize_t n = ::sendto(fd, packet.data, packet.size, MSG_DONTWAIT, addr, addr_len);
    if (n >= 0) {
        bytes = static_cast<std::size_t>(n);
        return RecvOutcome::Ok;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK)
        return RecvOutcome::WouldBlock;
    if (errno == EINTR)
        return RecvOutcome::WouldBlock; // 同上，由可等待方法重试
    error = _Errno(ErrorCode::TransportError, "UDP send failed");
    return RecvOutcome::Error;
}

} // namespace

CoUDP::CoUDP(int fd, bbt::coroutine::CoObjectInfo info) noexcept
    : m_fd(fd), m_info(std::move(info)) {}

CoUDP::~CoUDP() {
    std::lock_guard<std::mutex> lock(m_mtx);
    _CloseFd();
}

bbt::coroutine::CoObjectInfo CoUDP::GetObjectInfo() const { return m_info; }

result<CoUDP::SPtr> CoUDP::BindUDP(SocketAddress local) {
    // §4.0.1：控制线程配置操作；要求 Scheduler 已 Start（对象身份与
    // 关闭信号需要有效运行时代际）。
    if (local.ip.empty())
        return result<SPtr>::err(MakeError(ErrorCode::InvalidArgument,
            "BindUDP: SocketAddress.ip must not be empty (use explicit wildcard)"));

    auto addr = _ToSockaddr(local);
    if (!addr)
        return result<SPtr>::err(std::move(addr).error());
    const auto gen = bbt::coroutine::CurrentRuntimeGeneration();
    if (gen == 0)
        return result<SPtr>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "BindUDP requires a started Scheduler"));

    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0)
        return result<SPtr>::err(_Errno(ErrorCode::TransportError,
            "UDP socket creation failed"));
    if (_SetNonBlocking(fd) != 0) {
        const Error error = _Errno(ErrorCode::TransportError,
            "UDP non-blocking setup failed");
        ::close(fd);
        return result<SPtr>::err(error);
    }
    int reuse = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    if (::bind(fd, reinterpret_cast<const sockaddr*>(&addr.value()),
               sizeof(addr.value())) != 0) {
        const Error error = _Errno(ErrorCode::TransportError, "UDP bind failed");
        ::close(fd); // §4.0.1.6：失败即无对象，内部负责 close 已创建的 FD
        return result<SPtr>::err(error);
    }

    auto info = bbt::infra::detail::NewObjectInfo("udp");
    if (!info) {
        ::close(fd);
        return result<SPtr>::err(std::move(info).error());
    }
    auto object = std::shared_ptr<CoUDP>(new CoUDP(fd, std::move(info).value()));
    // 读取实际绑定地址（端口 0 时由内核分配）。
    sockaddr_in actual{};
    socklen_t actual_len = sizeof(actual);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&actual), &actual_len) == 0)
        object->m_local = _FromSockaddr(actual);
    return result<SPtr>::ok(std::move(object));
}

// §4 入口检查：参数 → 受管协程上下文 → Runtime 代际；成立即返回，不挂起。
// §4 的 Try* 规则：Try* 只检查参数、上下文、代际和关闭状态——代际门禁对
// 两类入口都成立，但只有协程方法要求「当前处于受管协程内」。
result<void> CoUDP::_CheckEntry(bool in_coroutine_only) const {
    if (in_coroutine_only && g_bbt_tls_coroutine_co == nullptr)
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
            "CoUDP operation must run in coroutine context"));
    // 代际检查两条路径共用：Runtime 未 Start 或对象归属其他运行代（例如
    // Stop → Start 之后的旧引用）→ RuntimeUnavailable，立即返回，不挂起。
    // 这同时是 §6.4.5 的 FD 代际防护入口：旧代对象不得再触碰旧 fd 数字。
    const auto generation = bbt::coroutine::CurrentRuntimeGeneration();
    if (generation == 0 || generation != m_info.generation)
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "CoUDP operation belongs to another runtime generation"));
    return result<void>::ok();
}

SocketAddress CoUDP::LocalAddress() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_local;
}

result<DatagramRead> CoUDP::TryReceive(MutableBytes dst) {
    if (dst.size > 0 && dst.data == nullptr)
        return result<DatagramRead>::err(MakeError(ErrorCode::InvalidArgument,
            "TryReceive: null buffer with non-zero size"));
    auto entry = _CheckEntry(/*in_coroutine_only=*/false);
    if (!entry)
        return result<DatagramRead>::err(std::move(entry).error());

    std::lock_guard<std::mutex> lock(m_mtx);
    if (m_close_requested || m_fd < 0)
        return result<DatagramRead>::err(MakeError(ErrorCode::Closed,
            "UDP socket closed"));

    SocketAddress peer;
    std::size_t bytes = 0;
    bool truncated = false;
    Error error;
    const auto outcome = _RecvOnce(m_fd, dst, peer, bytes, truncated, error);
    if (outcome == RecvOutcome::Ok) {
        DatagramRead read;
        read.state     = IoState::Ok;
        read.bytes     = bytes;
        read.peer      = peer;
        read.truncated = truncated;
        return result<DatagramRead>::ok(read);
    }
    if (outcome == RecvOutcome::WouldBlock) {
        DatagramRead read;
        read.state     = IoState::WouldBlock;
        read.bytes     = 0;
        read.truncated = false;
        return result<DatagramRead>::ok(read);
    }
    return result<DatagramRead>::err(std::move(error));
}

IoResult CoUDP::TrySend(ConstBytes packet, const SocketAddress& peer) {
    if (packet.size > 0 && packet.data == nullptr)
        return IoResult::err(MakeError(ErrorCode::InvalidArgument,
            "TrySend: null buffer with non-zero size"));
    auto addr = _ToSockaddr(peer);
    if (!addr)
        return IoResult::err(std::move(addr).error());
    auto entry = _CheckEntry(/*in_coroutine_only=*/false);
    if (!entry)
        return IoResult::err(std::move(entry).error());

    std::lock_guard<std::mutex> lock(m_mtx);
    if (m_close_requested || m_fd < 0)
        return IoResult::err(MakeError(ErrorCode::Closed, "UDP socket closed"));

    std::size_t bytes = 0;
    Error error;
    const auto outcome = _SendOnce(m_fd, packet,
        reinterpret_cast<const sockaddr*>(&addr.value()), sizeof(addr.value()),
        bytes, error);
    if (outcome == RecvOutcome::Ok)
        return IoResult::ok(IoProgress{IoState::Ok, bytes});
    if (outcome == RecvOutcome::WouldBlock)
        return IoResult::ok(IoProgress{IoState::WouldBlock, 0});
    return IoResult::err(std::move(error));
}

// §5.2 组合等待：readable/writable interest + 绝对 deadline + cancel +
// close（经 m_close_source 组合令牌）单轮一次性挂起。等待注册由上游
// CoWaiter 完成（先登记后挂起、已取消令牌同步触发，唤醒不丢失）；
// infra 不复制等待状态机。fd 就绪只表示「可以重试」，不是操作成功。
CoUDP::WaitOutcome CoUDP::_Wait(bool readable, const CallOptions& options) {
    if (g_bbt_tls_coroutine_co == nullptr)
        return WaitOutcome::InvalidContext;

    auto waiter = CoWaiter::Create();
    CombinedWaitOptions wait;
    wait.fd             = m_fd;
    wait.want_readable  = readable;
    wait.want_writeable = !readable;
    wait.deadline       = options.deadline;
    // close 条件：对象 close 源与调用方 cancel 组合成 OR 视图；
    // RequestClose（任意线程）经 RequestCancel 走同一唤醒登记路径。
    // 注意：读 m_fd 时对象由在途计数保护（RequestClose 等计数归零才
    // 物理 close），等待期间 fd 不会被关闭，无 FD 复用风险。
    wait.cancel = bbt::coroutine::CancellationToken::Combine(
        m_close_source->Token(), options.cancel);
    const auto status = waiter->Wait(wait);
    switch (status) {
    case CombinedWaitStatus::FdReadable:
    case CombinedWaitStatus::FdWriteable:
        return WaitOutcome::Ready;
    case CombinedWaitStatus::Completed: // close 源触发（custom 唤醒）
    case CombinedWaitStatus::Cancelled:
        // Completed/Cancelled 同为 custom/cancel 唤醒位；复查关闭源
        // 区分 close 与调用方 cancel，给出确定语义。
        if (m_close_source->Token().IsCancellationRequested())
            return WaitOutcome::Closed;
        return WaitOutcome::Cancelled;
    case CombinedWaitStatus::TimedOut:
        return WaitOutcome::TimedOut;
    case CombinedWaitStatus::InvalidContext:
        return WaitOutcome::InvalidContext;
    default:
        return WaitOutcome::RuntimeUnavailable;
    }
}

result<DatagramRead> CoUDP::Receive(MutableBytes dst, const CallOptions& options) {
    auto entry = _CheckEntry(/*in_coroutine_only=*/true);
    if (!entry)
        return result<DatagramRead>::err(std::move(entry).error());
    if (dst.size > 0 && dst.data == nullptr)
        return result<DatagramRead>::err(MakeError(ErrorCode::InvalidArgument,
            "Receive: null buffer with non-zero size"));

    // §6.4.3：在途计数先于等待登记，物理 close 在计数归零后才发生；
    // 封口后入口立即拒绝（Closed），等待中封口则由组合令牌唤醒 Closed。
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        if (m_close_requested || m_fd < 0)
            return result<DatagramRead>::err(MakeError(ErrorCode::Closed,
                "UDP socket closed"));
        ++m_inflight;
    }

    result<DatagramRead> output = result<DatagramRead>::err(
        MakeError(ErrorCode::Closed, "UDP socket closed"));
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_close_requested || m_fd < 0)
                break; // 封口：在途等待返回 Closed
        }
        const auto wait = _Wait(/*readable=*/true, options);
        if (wait != WaitOutcome::Ready) {
            Error error =
                wait == WaitOutcome::Closed
                    ? MakeError(ErrorCode::Closed, "UDP socket closed")
                    : wait == WaitOutcome::Cancelled
                        ? MakeError(ErrorCode::Cancelled, "operation cancelled")
                        : wait == WaitOutcome::TimedOut
                            ? MakeError(ErrorCode::TimedOut,
                                "operation timed out")
                            : wait == WaitOutcome::InvalidContext
                                ? MakeError(ErrorCode::InvalidContext,
                                    "invalid coroutine context")
                                : MakeError(ErrorCode::RuntimeUnavailable,
                                    "coroutine wait unavailable");
            output = result<DatagramRead>::err(std::move(error));
            break;
        }
        // Ready：重试一次 syscall。等待期间对象受在途计数保护，fd 不会
        // 被物理 close；锁内复查封口状态后再触碰 fd。
        SocketAddress peer;
        std::size_t bytes = 0;
        bool truncated = false;
        Error error;
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_close_requested || m_fd < 0)
                break;
            const auto outcome =
                _RecvOnce(m_fd, dst, peer, bytes, truncated, error);
            if (outcome == RecvOutcome::Ok) {
                DatagramRead read;
                read.state     = IoState::Ok;
                read.bytes     = bytes;
                read.peer      = peer;
                read.truncated = truncated;
                output = result<DatagramRead>::ok(read);
                break;
            }
            if (outcome == RecvOutcome::Error) {
                output = result<DatagramRead>::err(std::move(error));
                break;
            }
            // WouldBlock：虚假唤醒或 EINTR，回到等待（同一绝对 deadline，
            // 重试不重置超时）。
        }
    }

    bool closed_now = false;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        --m_inflight;
        if (m_close_requested && m_inflight == 0 && !m_physically_closed) {
            _CloseFd();
            closed_now = true;
        }
    }
    if (closed_now) {
        m_closed_source->RequestCancel();
        if (m_closed_hook)
            m_closed_hook();
    }
    return output;
}

IoResult CoUDP::Send(ConstBytes packet, const SocketAddress& peer,
                     const CallOptions& options) {
    if (packet.size > 0 && packet.data == nullptr)
        return IoResult::err(MakeError(ErrorCode::InvalidArgument,
            "Send: null buffer with non-zero size"));
    auto addr = _ToSockaddr(peer);
    if (!addr)
        return IoResult::err(std::move(addr).error());
    auto entry = _CheckEntry(/*in_coroutine_only=*/true);
    if (!entry)
        return IoResult::err(std::move(entry).error());

    {
        std::lock_guard<std::mutex> lock(m_mtx);
        if (m_close_requested || m_fd < 0)
            return IoResult::err(MakeError(ErrorCode::Closed,
                "UDP socket closed"));
        ++m_inflight;
    }

    IoResult output =
        IoResult::err(MakeError(ErrorCode::Closed, "UDP socket closed"));
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_close_requested || m_fd < 0)
                break;
        }
        const auto wait = _Wait(/*readable=*/false, options);
        if (wait != WaitOutcome::Ready) {
            Error error =
                wait == WaitOutcome::Closed
                    ? MakeError(ErrorCode::Closed, "UDP socket closed")
                    : wait == WaitOutcome::Cancelled
                        ? MakeError(ErrorCode::Cancelled, "operation cancelled")
                        : wait == WaitOutcome::TimedOut
                            ? MakeError(ErrorCode::TimedOut,
                                "operation timed out")
                            : wait == WaitOutcome::InvalidContext
                                ? MakeError(ErrorCode::InvalidContext,
                                    "invalid coroutine context")
                                : MakeError(ErrorCode::RuntimeUnavailable,
                                    "coroutine wait unavailable");
            output = IoResult::err(std::move(error));
            break;
        }
        std::size_t bytes = 0;
        Error error;
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_close_requested || m_fd < 0)
                break;
            const auto outcome = _SendOnce(m_fd, packet,
                reinterpret_cast<const sockaddr*>(&addr.value()),
                sizeof(addr.value()), bytes, error);
            if (outcome == RecvOutcome::Ok) {
                output = IoResult::ok(IoProgress{IoState::Ok, bytes});
                break;
            }
            if (outcome == RecvOutcome::Error) {
                output = IoResult::err(std::move(error));
                break;
            }
        }
    }

    bool closed_now = false;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        --m_inflight;
        if (m_close_requested && m_inflight == 0 && !m_physically_closed) {
            _CloseFd();
            closed_now = true;
        }
    }
    if (closed_now) {
        m_closed_source->RequestCancel();
        if (m_closed_hook)
            m_closed_hook();
    }
    return output;
}

void CoUDP::_CloseFd() noexcept {
    // 物理 close：仅析构与 RequestClose 的 in-flight 归零路径调用。
    if (m_fd >= 0) {
        ::close(m_fd);
        m_fd = -1;
    }
    m_physically_closed = true;
}

// §6.4.1：封口与通知（任意线程）：置关闭标志、使在途等待失效（唤醒等待者
// 返回 Closed）、拒绝新调用；不等待物理完成。
// §6.4.3：在途者退出时执行物理 close；调用线程不能自旋等待协程
// （调用线程也可能恰是唯一的 scheduler worker）。
// §6.4.5：FD 代际防护——等待期间 fd 受在途计数保护不被关闭，poller 注册
// 的事件对象随等待者退出自然失效，不存在旧代际事件触达新资源的窗口。
void CoUDP::RequestClose() noexcept {
    // 幂等封口：仅首次执行 teardown。
    bool closed_now = false;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        if (m_close_requested)
            return;
        m_close_requested = true;
        if (m_inflight == 0) {
            _CloseFd();
            closed_now = true;
        }
    }
    // 任意线程：唤醒所有经组合令牌登记的在途 I/O。
    m_close_source->RequestCancel();
    if (closed_now) {
        m_closed_source->RequestCancel();
        if (m_closed_hook)
            m_closed_hook();
    }
}

bool CoUDP::IsClosed() const noexcept {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_physically_closed;
}

CloseStatus CoUDP::WaitClosed(bbt::coroutine::Deadline deadline,
                              bbt::coroutine::CancellationToken cancel) {
    if (g_bbt_tls_coroutine_co == nullptr)
        return CloseStatus::InvalidContext;
    const auto gen = bbt::coroutine::CurrentRuntimeGeneration();
    if (gen == 0 || gen != m_info.generation)
        return CloseStatus::RuntimeUnavailable;
    if (IsClosed())
        return CloseStatus::Closed;
    if (m_close_waiting.exchange(true))
        return CloseStatus::AlreadyWaiting;
    struct WaitGuard {
        std::atomic_bool& waiting;
        ~WaitGuard() { waiting.store(false); }
    } guard{m_close_waiting};

    // 经组合令牌真实等待关闭（不读 bool）：close 源与调用方 cancel 任一
    // 触发即唤醒。close 源触发早于物理 close（RequestClose 等 in-flight
    // 归零才 close），故唤醒后循环复查 IsClosed 直至真实清理完成；调用方
    // cancel 触发且未封口时才返回 Cancelled。deadline 为绝对时间点，
    // 循环不重置超时。
    for (;;) {
        if (IsClosed())
            return CloseStatus::Closed;
        if (cancel.IsCancellationRequested())
            return CloseStatus::Cancelled;
        auto waiter = CoWaiter::Create();
        CombinedWaitOptions wait;
        wait.deadline = deadline;
        wait.cancel   = bbt::coroutine::CancellationToken::Combine(
            m_closed_source->Token(), cancel);
        const auto status = waiter->Wait(wait);
        switch (status) {
        case CombinedWaitStatus::Completed:
        case CombinedWaitStatus::Cancelled:
            // close 或调用方 cancel：回循环复查（Close/Cancelled 分流）。
            continue;
        case CombinedWaitStatus::TimedOut:
            return CloseStatus::TimedOut;
        case CombinedWaitStatus::InvalidContext:
            return CloseStatus::InvalidContext;
        default:
            return CloseStatus::RuntimeUnavailable;
        }
    }
}

} // namespace bbt::infra::udp
