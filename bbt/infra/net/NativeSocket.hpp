#pragma once

/**
 * @file NativeSocket.hpp
 * @brief Linux 原生 socket 的薄封装：非阻塞、单 owner fd，不参与协程 Hook。
 *
 * 所有 I/O 与选项调用都直接映射到系统调用，返回错误码而不抛异常。它不是 CoObject，
 * 不提供协程等待，也不承诺线程安全：同一对象同一时刻只应由一个 owner 使用。
 */

#include <chrono>
#include <cstddef>
#include <system_error>

#include <bbt/infra/net/Address.hpp>

namespace bbt::infra::net {

/// @brief socket 类型。Stream 为 TCP 类字节流，Datagram 为 UDP 类报文。
enum class SocketType { Stream, Datagram };

/// @brief 一次收发系统调用的结果。
struct IoResult {
    std::size_t bytes{0};  ///< 实际拷贝到用户缓冲的字节数
    std::error_code error; ///< 系统调用错误；空表示本次调用成功
    bool eof{false};       ///< stream 上请求非零长度却读到 0 字节
    bool truncated{false}; ///< 数据报被截断（内核丢弃了超出缓冲的尾部）
};

/// @brief 只读字节片段，用于 scatter/gather 发送。
struct ConstBuffer {
    const void* data{nullptr}; ///< 片段首地址
    std::size_t size{0};       ///< 片段字节数
};

/**
 * @brief Linux 单 owner fd 原语。
 *
 * 它不是 CoObject，不提供协程等待，也不承诺线程安全。fd 一律以非阻塞方式创建；
 * Accept 与 Connect 走 SYS_accept4/SYS_connect，绕过 coroutine Hook，不会隐式挂起。
 */
class NativeSocket {
public:
    /// @brief 构造空对象，不持 fd。
    NativeSocket() noexcept = default;
    /// @brief 析构时关闭仍持有的 fd，忽略关闭错误。
    ~NativeSocket();
    /// @brief 拷贝已删除：fd 所有权唯一。
    NativeSocket(const NativeSocket&) = delete;
    /// @brief 拷贝赋值已删除，原因同拷贝构造。
    NativeSocket& operator=(const NativeSocket&) = delete;
    /// @brief 移动构造：接管 fd，源对象变为空。
    NativeSocket(NativeSocket&& other) noexcept;
    /// @brief 移动赋值：先关闭自身 fd，再接管源对象；源对象变为空。
    NativeSocket& operator=(NativeSocket&& other) noexcept;

    /**
     * @brief 创建非阻塞、CLOEXEC 的 socket。
     *
     * family 或 type 不受支持时返回空对象并把 error 置为 invalid_argument；系统调用失败时
     * error 为 errno 对应的系统错误。成功时 error 被清空。
     */
    static NativeSocket Open(AddressFamily family, SocketType type, std::error_code& error) noexcept;
    /// @brief 借用当前 fd，不转移所有权。调用方不得 close 或启用阻塞选项。
    int Fd() const noexcept
    {
        return m_fd;
    }
    /// @brief 放弃所有权并返回 fd；之后对象为空，调用方负责关闭。
    int Release() noexcept;
    /// @brief 关闭 fd 并返回错误。不重试 EINTR：该编号可能已被其它资源复用。
    std::error_code Close() noexcept;
    /// @brief 以 F_DUPFD_CLOEXEC 复制 fd，新对象继承 family 与 type；失败时 error 非空并返回空对象。
    NativeSocket Dup(std::error_code& error) const noexcept;
    /// @brief 绑定地址。地址无效或 family 不匹配时返回 invalid_argument，不进入系统调用。
    std::error_code Bind(const Address& address) noexcept;
    /// @brief 进入监听。backlog <= 0 返回 invalid_argument，非 Stream 返回 operation_not_supported。
    std::error_code Listen(int backlog) noexcept;
    /**
     * @brief 立即尝试接受一个连接，不挂起。
     *
     * Linux 上 Accept 与 Connect 直接调用 SYS_accept4 与 SYS_connect，不经过 coroutine Hook。
     * 空 listener 上的 Accept 立即返回 EAGAIN，不会隐式挂起。非阻塞 Connect 保留 EINPROGRESS，
     * 真实的 pending 状态由就绪事件与 PendingError 完成。成功时把对端地址写回 peer。
     */
    NativeSocket Accept(Address& peer, std::error_code& error) const noexcept;
    /// @brief 发起非阻塞连接。地址无效或 family 不匹配时返回 invalid_argument；EINPROGRESS 原样返回。
    std::error_code Connect(const Address& address) noexcept;
    /// @brief 可写就绪后调用。读取 SO_ERROR 之后它会被清零。返回零不代表等待已经发生。
    std::error_code PendingError() const noexcept;
    /// @brief 查询本端地址（getsockname）；失败时 error 非空并返回空地址。
    Address LocalAddress(std::error_code& error) const noexcept;
    /// @brief 查询对端地址（getpeername）；失败时 error 非空并返回空地址。
    Address RemoteAddress(std::error_code& error) const noexcept;
    /// @brief 按 how（SHUT_RD/SHUT_WR/SHUT_RDWR）关闭读或写方向，不释放 fd。
    std::error_code Shutdown(int how) noexcept;

    /**
     * @brief 非阻塞发送一段数据。
     *
     * flags 约定：Send、Sendv、SendTo 只接受 MSG_DONTWAIT 与 MSG_NOSIGNAL，两者始终附加。
     * 其余标志在任何系统调用之前以 operation_not_supported 拒绝，返回 bytes=0 且不发出报文。
     */
    IoResult Send(const void* data, std::size_t size, int flags = 0) noexcept;
    /**
     * @brief 非阻塞接收数据。数据报 socket 委托 ReceiveFrom 并丢弃来源地址。
     *
     * Receive 只接受 MSG_DONTWAIT 与 MSG_PEEK。stream 拒绝 MSG_TRUNC，避免内核丢弃数据却报告
     * bytes>0。stream 上请求非零长度却读到 0 字节时置 eof。
     */
    IoResult Receive(void* data, std::size_t size, int flags = 0) noexcept;
    /// @brief 以 sendmsg 发送多个片段；count 上限 1024，总长超过 ssize_t 上限时返回 invalid_argument。
    IoResult Sendv(const ConstBuffer* buffers, std::size_t count, int flags = 0);
    /// @brief 向指定地址发送一个数据报；非 Datagram socket 返回 operation_not_supported。
    IoResult SendTo(const void* data, std::size_t size, const Address& peer, int flags = 0) noexcept;
    /**
     * @brief 接收一个数据报并写回来源地址。
     *
     * 数据报的 Receive 与 ReceiveFrom 另外接受 MSG_TRUNC，bytes 只统计拷贝到 buffer 的字节，
     * truncated 反映截断。OOB、ZEROCOPY、ERRQUEUE、WAITALL、MORE 和未知标志位都在任何系统调用
     * 之前以 operation_not_supported 拒绝，此时返回 bytes=0，不消费报文。
     */
    IoResult ReceiveFrom(void* data, std::size_t size, Address& peer, int flags = 0) noexcept;

    /// @brief 设置 SO_REUSEADDR。
    std::error_code SetReuseAddr(bool enabled) noexcept;
    /// @brief 设置 SO_REUSEPORT；仅 IPv4/IPv6 socket 支持，其它 family 返回 operation_not_supported。
    std::error_code SetReusePort(bool enabled) noexcept;
    /// @brief 设置 IPV6_V6ONLY；仅 IPv6 socket 支持，其它 family 返回 operation_not_supported。
    std::error_code SetIpv6Only(bool enabled) noexcept;
    /// @brief 设置 TCP_NODELAY；非 TCP socket 返回 operation_not_supported。
    std::error_code SetNoDelay(bool enabled) noexcept;
    /// @brief 设置 SO_RCVBUF；size <= 0 返回 invalid_argument。
    std::error_code SetReadBuffer(int size) noexcept;
    /// @brief 设置 SO_SNDBUF；size <= 0 返回 invalid_argument。
    std::error_code SetWriteBuffer(int size) noexcept;
    /// @brief 设置 SO_LINGER：0 为关闭时立即丢弃未发数据，<0 为关闭 linger；> 0 在系统调用前拒绝，避免阻塞 close。
    std::error_code SetLinger(int seconds) noexcept;
    /// @brief 设置 TCP keepalive 的空闲、间隔与探测次数；非 TCP 返回 operation_not_supported。
    std::error_code SetKeepAlive(bool enabled, std::chrono::seconds idle, std::chrono::seconds interval,
                                 int count) noexcept;

private:
    NativeSocket(int fd, AddressFamily family, SocketType type) noexcept;
    std::error_code SetIntOption(int level, int option, int value) noexcept;
    bool IsTcp() const noexcept;
    int m_fd{-1};
    AddressFamily m_family{AddressFamily::Unspecified};
    SocketType m_type{SocketType::Stream};
};

} // namespace bbt::infra::net
