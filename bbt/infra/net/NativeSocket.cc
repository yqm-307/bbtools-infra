#include <bbt/infra/net/NativeSocket.hpp>

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <climits>
#include <limits>
#include <utility>
#include <vector>

namespace {
std::error_code SysError() noexcept { return {errno, std::generic_category()}; }
std::error_code Invalid() noexcept { return std::make_error_code(std::errc::invalid_argument); }
std::error_code Unsupported() noexcept { return std::make_error_code(std::errc::operation_not_supported); }

// flags 契约：窄 primitive 只承诺保守子集；未列出位在任何系统调用前拒绝。
constexpr int kSendAllow = MSG_DONTWAIT | MSG_NOSIGNAL;
constexpr int kStreamRecvAllow = MSG_DONTWAIT | MSG_PEEK;
constexpr int kDatagramRecvAllow = kStreamRecvAllow | MSG_TRUNC;

bbt::infra::net::IoResult TransferResult(ssize_t bytes) noexcept {
    if (bytes < 0) { return {0, SysError()}; }
    return {static_cast<std::size_t>(bytes), {}};
}

bool ValidBuffer(const void* data, std::size_t size) noexcept {
    return (data || size == 0) && size <= static_cast<std::size_t>(std::numeric_limits<ssize_t>::max());
}
} // namespace

namespace bbt::infra::net {

NativeSocket::NativeSocket(int fd, AddressFamily family, SocketType type) noexcept
    : m_fd(fd), m_family(family), m_type(type) {}

NativeSocket::~NativeSocket() { (void)Close(); }

NativeSocket::NativeSocket(NativeSocket&& other) noexcept
    : m_fd(other.Release()), m_family(other.m_family), m_type(other.m_type) {}

NativeSocket& NativeSocket::operator=(NativeSocket&& other) noexcept {
    if (this != &other) {
        (void)Close();
        m_fd = other.Release();
        m_family = other.m_family;
        m_type = other.m_type;
    }
    return *this;
}

NativeSocket NativeSocket::Open(AddressFamily family, SocketType type, std::error_code& error) noexcept {
    error.clear();
    int domain = 0;
    switch (family) {
    case AddressFamily::IPv4: domain = AF_INET; break;
    case AddressFamily::IPv6: domain = AF_INET6; break;
    case AddressFamily::Unix: domain = AF_UNIX; break;
    default: error = Invalid(); return {};
    }
    if (type != SocketType::Stream && type != SocketType::Datagram) {
        error = Invalid();
        return {};
    }
    const int fd = ::socket(domain, (type == SocketType::Stream ? SOCK_STREAM : SOCK_DGRAM) | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        error = SysError();
        return {};
    }
    return NativeSocket(fd, family, type);
}

int NativeSocket::Release() noexcept { return std::exchange(m_fd, -1); }

std::error_code NativeSocket::Close() noexcept {
    const int fd = Release();
    // Linux close(EINTR) 可能已回收 fd；绝不重试一个可被其他资源复用的编号。
    return fd >= 0 && ::close(fd) < 0 ? SysError() : std::error_code{};
}

NativeSocket NativeSocket::Dup(std::error_code& error) const noexcept {
    error.clear();
    const int fd = ::fcntl(m_fd, F_DUPFD_CLOEXEC, 0);
    if (fd < 0) {
        error = SysError();
        return {};
    }
    return NativeSocket(fd, m_family, m_type);
}

std::error_code NativeSocket::Bind(const Address& address) noexcept {
    if (!address.Valid() || address.Family() != m_family) { return Invalid(); }
    return ::bind(m_fd, reinterpret_cast<const sockaddr*>(&address.m_storage), address.m_size) < 0
        ? SysError() : std::error_code{};
}

std::error_code NativeSocket::Listen(int backlog) noexcept {
    if (backlog <= 0) { return Invalid(); }
    if (m_type != SocketType::Stream) { return Unsupported(); }
    return ::listen(m_fd, backlog) < 0 ? SysError() : std::error_code{};
}

NativeSocket NativeSocket::Accept(Address& peer, std::error_code& error) const noexcept {
    error.clear();
    Address accepted_peer;
    accepted_peer.m_size = sizeof(accepted_peer.m_storage);
    // L1：上游 Hook.cc 介入 ::accept4 符号且不因 O_NONBLOCK 直通，会把非挂起
    // primitive 隐式推进协程等待；Linux 原生边界走 SYS_accept4 绕开符号介入。
    // errno/NONBLOCK/CLOEXEC/单 fd 所有权不变；close 仍走 Hook 路径（fd waiter/epoch）。
#if !defined(SYS_accept4)
#error "Linux SYS_accept4 缺失：无法保证 Accept 不经 Hook 隐式挂起"
#endif
    const int fd = static_cast<int>(::syscall(SYS_accept4, m_fd,
        reinterpret_cast<sockaddr*>(&accepted_peer.m_storage),
        &accepted_peer.m_size, SOCK_NONBLOCK | SOCK_CLOEXEC));
    if (fd < 0) {
        error = SysError();
        return {};
    }
    peer = accepted_peer;
    return NativeSocket(fd, m_family, m_type);
}

std::error_code NativeSocket::Connect(const Address& address) noexcept {
    if (!address.Valid() || address.Family() != m_family) { return Invalid(); }
    // L1：同 Accept 走 SYS_connect，保留真实非阻塞 pending（loopback 通常 EINPROGRESS），
    // 之后由调用者 readiness + PendingError 完成连接；Hook 不得隐式等待或改写结果。
#if !defined(SYS_connect)
#error "Linux SYS_connect 缺失：无法保证 Connect 不经 Hook 隐式等待/改写结果"
#endif
    return static_cast<int>(::syscall(SYS_connect, m_fd,
        reinterpret_cast<const sockaddr*>(&address.m_storage), address.m_size)) < 0
        ? SysError() : std::error_code{};
}

std::error_code NativeSocket::PendingError() const noexcept {
    int value = 0;
    socklen_t size = sizeof(value);
    if (::getsockopt(m_fd, SOL_SOCKET, SO_ERROR, &value, &size) < 0) { return SysError(); }
    return {value, std::generic_category()};
}

Address NativeSocket::LocalAddress(std::error_code& error) const noexcept {
    error.clear();
    Address result;
    result.m_size = sizeof(result.m_storage);
    if (::getsockname(m_fd, reinterpret_cast<sockaddr*>(&result.m_storage), &result.m_size) < 0) {
        error = SysError();
        return {};
    }
    return result;
}

Address NativeSocket::RemoteAddress(std::error_code& error) const noexcept {
    error.clear();
    Address result;
    result.m_size = sizeof(result.m_storage);
    if (::getpeername(m_fd, reinterpret_cast<sockaddr*>(&result.m_storage), &result.m_size) < 0) {
        error = SysError();
        return {};
    }
    return result;
}

std::error_code NativeSocket::Shutdown(int how) noexcept {
    return ::shutdown(m_fd, how) < 0 ? SysError() : std::error_code{};
}

IoResult NativeSocket::Send(const void* data, std::size_t size, int flags) noexcept {
    if (!ValidBuffer(data, size)) { return {0, Invalid()}; }
    if (flags & ~kSendAllow) { return {0, Unsupported()}; }
    return TransferResult(::send(m_fd, data, size, flags | MSG_DONTWAIT | MSG_NOSIGNAL));
}

IoResult NativeSocket::Receive(void* data, std::size_t size, int flags) noexcept {
    if (!ValidBuffer(data, size)) { return {0, Invalid()}; }
    if (m_type == SocketType::Datagram) {
        Address unused;
        return ReceiveFrom(data, size, unused, flags);
    }
    // stream 拒绝 MSG_TRUNC：防 bytes>0 而 buffer 未填的内核 discard 语义。
    if (flags & ~kStreamRecvAllow) { return {0, Unsupported()}; }
    auto result = TransferResult(::recv(m_fd, data, size, flags | MSG_DONTWAIT));
    result.eof = !result.error && size != 0 && result.bytes == 0;
    return result;
}

IoResult NativeSocket::Sendv(const ConstBuffer* buffers, std::size_t count, int flags) {
    if ((!buffers && count) || count > 1024) { return {0, Invalid()}; }
    if (flags & ~kSendAllow) { return {0, Unsupported()}; }
    std::vector<iovec> parts;
    parts.reserve(count);
    std::size_t total = 0;
    const auto maximum = static_cast<std::size_t>(std::numeric_limits<ssize_t>::max());
    for (std::size_t i = 0; i < count; ++i) {
        if (!ValidBuffer(buffers[i].data, buffers[i].size) || buffers[i].size > maximum - total) {
            return {0, Invalid()};
        }
        total += buffers[i].size;
        parts.push_back({const_cast<void*>(buffers[i].data), buffers[i].size});
    }
    msghdr message{};
    message.msg_iov = parts.data();
    message.msg_iovlen = parts.size();
    return TransferResult(::sendmsg(m_fd, &message, flags | MSG_DONTWAIT | MSG_NOSIGNAL));
}

IoResult NativeSocket::SendTo(const void* data, std::size_t size, const Address& peer, int flags) noexcept {
    if (m_type != SocketType::Datagram) { return {0, Unsupported()}; }
    if (!peer.Valid() || peer.Family() != m_family || !ValidBuffer(data, size)) { return {0, Invalid()}; }
    if (flags & ~kSendAllow) { return {0, Unsupported()}; }
    return TransferResult(::sendto(m_fd, data, size, flags | MSG_DONTWAIT | MSG_NOSIGNAL,
        reinterpret_cast<const sockaddr*>(&peer.m_storage), peer.m_size));
}

IoResult NativeSocket::ReceiveFrom(void* data, std::size_t size, Address& peer, int flags) noexcept {
    if (m_type != SocketType::Datagram) { return {0, Unsupported()}; }
    if (!ValidBuffer(data, size)) { return {0, Invalid()}; }
    if (flags & ~kDatagramRecvAllow) { return {0, Unsupported()}; }  // 拒绝发生在消费报文之前
    Address source;
    source.m_size = sizeof(source.m_storage);
    iovec part{data, size};
    msghdr message{};
    message.msg_name = &source.m_storage;
    message.msg_namelen = source.m_size;
    message.msg_iov = &part;
    message.msg_iovlen = 1;
    const auto bytes = ::recvmsg(m_fd, &message, flags | MSG_DONTWAIT);
    auto result = TransferResult(bytes);
    if (result.error) { return result; }
    result.bytes = std::min(result.bytes, size);
    result.truncated = (message.msg_flags & MSG_TRUNC) != 0;
    source.m_size = message.msg_namelen;
    peer = source;
    return result;
}

std::error_code NativeSocket::SetIntOption(int level, int option, int value) noexcept {
    return ::setsockopt(m_fd, level, option, &value, sizeof(value)) < 0 ? SysError() : std::error_code{};
}

bool NativeSocket::IsTcp() const noexcept {
    return m_type == SocketType::Stream && (m_family == AddressFamily::IPv4 || m_family == AddressFamily::IPv6);
}

std::error_code NativeSocket::SetReuseAddr(bool enabled) noexcept {
    return SetIntOption(SOL_SOCKET, SO_REUSEADDR, enabled);
}

std::error_code NativeSocket::SetReusePort(bool enabled) noexcept {
    if (m_family != AddressFamily::IPv4 && m_family != AddressFamily::IPv6) { return Unsupported(); }
    return SetIntOption(SOL_SOCKET, SO_REUSEPORT, enabled);
}

std::error_code NativeSocket::SetIpv6Only(bool enabled) noexcept {
    if (m_family != AddressFamily::IPv6) { return Unsupported(); }
    return SetIntOption(IPPROTO_IPV6, IPV6_V6ONLY, enabled);
}

std::error_code NativeSocket::SetNoDelay(bool enabled) noexcept {
    return IsTcp() ? SetIntOption(IPPROTO_TCP, TCP_NODELAY, enabled) : Unsupported();
}

std::error_code NativeSocket::SetReadBuffer(int size) noexcept {
    return size > 0 ? SetIntOption(SOL_SOCKET, SO_RCVBUF, size) : Invalid();
}

std::error_code NativeSocket::SetWriteBuffer(int size) noexcept {
    return size > 0 ? SetIntOption(SOL_SOCKET, SO_SNDBUF, size) : Invalid();
}

std::error_code NativeSocket::SetLinger(int seconds) noexcept {
    if (seconds > 0) { return Unsupported(); }
    const linger option{seconds == 0, 0};
    return ::setsockopt(m_fd, SOL_SOCKET, SO_LINGER, &option, sizeof(option)) < 0 ? SysError() : std::error_code{};
}

std::error_code NativeSocket::SetKeepAlive(bool enabled, std::chrono::seconds idle,
    std::chrono::seconds interval, int count) noexcept {
    if (!IsTcp()) { return Unsupported(); }
    if (enabled && (idle.count() < 1 || interval.count() < 1 || idle.count() > INT_MAX || interval.count() > INT_MAX || count < 1)) {
        return Invalid();
    }
    auto error = SetIntOption(SOL_SOCKET, SO_KEEPALIVE, enabled);
    if (error || !enabled) { return error; }
    // 多 syscall 不是事务；任意一步失败立即可见，已成功的选项可能保留。
    error = SetIntOption(IPPROTO_TCP, TCP_KEEPIDLE, static_cast<int>(idle.count()));
    if (error) { return error; }
    error = SetIntOption(IPPROTO_TCP, TCP_KEEPINTVL, static_cast<int>(interval.count()));
    return error ? error : SetIntOption(IPPROTO_TCP, TCP_KEEPCNT, count);
}

} // namespace bbt::infra::net
