#include <bbt/infra/net/CoTcpConnection.hpp>

#include <string>
#include <system_error>
#include <utility>

#include <bbt/infra/net/Error.hpp>
#include <bbt/infra/net/detail/CoInternal.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>

/**
 * 实现放 .cc：Read/Write/WaitClosed 的等待路径要复用 ConnectionState/
 * ReceiveRing/close hub 与统一 gate；这些是 detail 部件，不进公共头。
 *
 * Read 复制到调用方 Buffer 后才消费接收数据；Busy/WouldBlock 仍使用
 * release 代次、on_registered 登记及谓词复查的原等待协议。
 */
namespace bbt::infra::net {

CoTcpConnection::CoTcpConnection(Connection connection) noexcept
    : m_conn(std::move(connection))
{
}

std::error_code CoTcpConnection::TryRead(Buffer& out)
{
    return m_conn.TryRead(out);
}

ReadView CoTcpConnection::View()
{
    return m_conn.View();
}

const Connection& CoTcpConnection::Raw() const noexcept
{
    return m_conn;
}

ConnectionStatus CoTcpConnection::Status() const
{
    ConnectionStatus status;
    status.valid = m_conn.Valid();
    if (!status.valid) {
        return status;
    }
    status.readable_bytes = const_cast<Connection&>(m_conn).ReadableBytes();
    status.read_eof = m_conn.ReadEof();
    status.read_error = m_conn.ReadError();
    status.closed = m_conn.Closed();
    status.close_reason = m_conn.CloseReason();
    status.released = m_conn.Released();

    /**
     * 写接纳状态真源采样
     */
    status.write = m_conn.WriteAcceptance();
    return status;
}

std::error_code CoTcpConnection::Read(Buffer& out, const bbt::coroutine::WaitOptions& options)
{
    return CoAccess::Read(m_conn, out, options);
}

std::error_code CoTcpConnection::AsyncWrite(std::string data)
{
    return m_conn.AsyncWrite(std::move(data));
}

std::tuple<std::size_t, std::error_code>
CoTcpConnection::Write(std::string data, const bbt::coroutine::WaitOptions& options)
{
    return CoAccess::Write(m_conn, std::move(data), options);
}

std::error_code CoTcpConnection::WaitClosed(const bbt::coroutine::WaitOptions& options)
{
    return CoAccess::WaitClosed(m_conn, options);
}

std::error_code CoTcpConnection::Close(const CloseOptions& options, std::error_code reason)
{
    return m_conn.Close(options, reason);
}

// clang-format off
}
// clang-format on
