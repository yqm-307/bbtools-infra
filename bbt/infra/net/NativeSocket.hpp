#pragma once

#include <chrono>
#include <cstddef>
#include <system_error>

#include <bbt/infra/net/Address.hpp>

namespace bbt::infra::net {

enum class SocketType { Stream, Datagram };

struct IoResult {
    std::size_t bytes{0};
    std::error_code error;
    bool eof{false};
    bool truncated{false};
};

struct ConstBuffer {
    const void* data{nullptr};
    std::size_t size{0};
};

// Linux 单 owner fd primitive。不是 CoObject，不提供协程等待/线程安全承诺。
class NativeSocket {
public:
    NativeSocket() noexcept = default;
    ~NativeSocket();
    NativeSocket(const NativeSocket&) = delete;
    NativeSocket& operator=(const NativeSocket&) = delete;
    NativeSocket(NativeSocket&& other) noexcept;
    NativeSocket& operator=(NativeSocket&& other) noexcept;

    static NativeSocket Open(AddressFamily family, SocketType type, std::error_code& error) noexcept;
    int                 Fd() const noexcept { return m_fd; } // 仅借用，禁止私自 close/启用阻塞选项。
    int                 Release() noexcept;
    std::error_code     Close() noexcept;
    NativeSocket        Dup(std::error_code& error) const noexcept;
    std::error_code     Bind(const Address& address) noexcept;
    std::error_code     Listen(int backlog) noexcept;
    // Linux 经 SYS_accept4/SYS_connect 原生 syscall，不经 coroutine Hook：
    // 空 listener Accept 立即 EAGAIN 不隐式挂起；非阻塞 Connect 保留 EINPROGRESS
    // 真实 pending，由 readiness + PendingError 完成。
    NativeSocket        Accept(Address& peer, std::error_code& error) const noexcept;
    std::error_code     Connect(const Address& address) noexcept;
    // 可写就绪后调用；SO_ERROR 被读取后清零。不把零当成等待已发生。
    std::error_code     PendingError() const noexcept;
    Address             LocalAddress(std::error_code& error) const noexcept;
    Address             RemoteAddress(std::error_code& error) const noexcept;
    std::error_code     Shutdown(int how) noexcept;

    // flags 契约：Send/Sendv/SendTo 仅接受 MSG_DONTWAIT|MSG_NOSIGNAL（内部始终附加）；
    // Receive 仅接受 MSG_DONTWAIT|MSG_PEEK；数据报 Receive/ReceiveFrom 另接受 MSG_TRUNC，
    // bytes 只计拷贝到 buffer 的字节、truncated 明确反映截断。未列出 flags（含 stream
    // TRUNC、OOB/ZEROCOPY/ERRQUEUE/WAITALL/MORE 与未知位）在任何系统调用前以
    // operation_not_supported 拒绝：返回 bytes=0，不发出/不消费报文。
    IoResult            Send(const void* data, std::size_t size, int flags = 0) noexcept;
    IoResult            Receive(void* data, std::size_t size, int flags = 0) noexcept;
    IoResult            Sendv(const ConstBuffer* buffers, std::size_t count, int flags = 0);
    IoResult            SendTo(const void* data, std::size_t size, const Address& peer, int flags = 0) noexcept;
    IoResult            ReceiveFrom(void* data, std::size_t size, Address& peer, int flags = 0) noexcept;

    std::error_code     SetReuseAddr(bool enabled) noexcept;
    std::error_code     SetReusePort(bool enabled) noexcept;
    std::error_code     SetIpv6Only(bool enabled) noexcept;
    std::error_code     SetNoDelay(bool enabled) noexcept;
    std::error_code     SetReadBuffer(int size) noexcept;
    std::error_code     SetWriteBuffer(int size) noexcept;
    std::error_code     SetLinger(int seconds) noexcept; // >0 副作用前拒绝，避免阻塞 close。
    std::error_code     SetKeepAlive(bool enabled, std::chrono::seconds idle,
                                    std::chrono::seconds interval, int count) noexcept;

private:
    NativeSocket(int fd, AddressFamily family, SocketType type) noexcept;
    std::error_code     SetIntOption(int level, int option, int value) noexcept;
    bool                IsTcp() const noexcept;
    int                 m_fd{-1};
    AddressFamily       m_family{AddressFamily::Unspecified};
    SocketType          m_type{SocketType::Stream};
};

} // namespace bbt::infra::net
