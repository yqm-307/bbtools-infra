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

    /**
     * gate 先于任何状态判定与保护的获取。
     */
    if (const auto gate = RequireCoroutineContext()) {
        return gate;
    }
    for (;;) {
        auto state = CoAccess::StateOf(m_conn);
        if (!state) {
            return make_error_code(NetErrc::Closed);
        }

        /**
         * 关键时序：在导致 Busy 的尝试之前采 release 代次，
         * 否则持锁方可能在「Busy 返回」与「采样」之间释放并推进代次，造成
         * on_registered 复查相等而永久等下一次 release。
         */
        const std::uint64_t generation = state->ring.AccessGeneration();
        const auto error = m_conn.TryRead(out);
        if (error != make_error_code(NetErrc::WouldBlock) && error != make_error_code(NetErrc::Busy)) {
            return error;
        }
        const bool busy = error == make_error_code(NetErrc::Busy);
        if (busy) {
            state->ring.InvokeBusyBeforeWaiterHookForTest();
        } else {
            state->ring.InvokeWouldBlockBeforeWaiterHookForTest();
        }
        auto waiter = busy ? state->ring.CreateReleaseWaiter() : state->ring.CreateDataWaiter();
        if (!waiter) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        bool add_failed = false;
        const auto status = waiter->WaitWithCallback(options, [&] {
            const bool added = busy ? state->ring.AddReleaseWaiter(waiter) : state->ring.AddDataWaiter(waiter);
            if (!added) {
                add_failed = true;
                waiter->Notify();
                return true;
            }
            state->ring.InvokeAfterAddHookForTest();
            const bool ready = busy ? (state->ring.AccessGeneration() != generation)
                                    : (state->ring.Size() > 0 || state->ring.ReadEof() || state->ring.HasReadError()
                                       || state->ring.ReadClosed());
            if (ready) {
                waiter->Notify();
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

std::error_code CoTcpConnection::AsyncWrite(std::string data)
{
    return m_conn.AsyncWrite(std::move(data));
}

std::tuple<std::size_t, std::error_code>
CoTcpConnection::Write(std::string data, const bbt::coroutine::WaitOptions& options)
{
    if (const auto gate = RequireCoroutineContext())
        return {0, gate};
    const auto state = CoAccess::StateOf(m_conn);
    if (!state)
        return {0, make_error_code(NetErrc::Closed)};
    const auto wanted = data.size();
    const auto no_buffer = std::make_error_code(std::errc::no_buffer_space);
    const auto settling = make_error_code(NetErrc::WriteSettling);
    WriteWait result;
    std::uint64_t id = 0;
    for (;;) {
        const auto error = state->SubmitWrite(data, id, &result);
        if (!error)
            break;
        if (error != no_buffer && error != settling)
            return {0, error};
        if (wanted > state->MaxOutboundBytes())
            return {0, no_buffer};
        if (state->Closed())
            return {0, make_error_code(NetErrc::Closed)};
        auto waiter = state->CreateWriteWaiter();
        if (!waiter)
            return {0, no_buffer};
        bool add_failed = false;
        const auto status = waiter->WaitWithCallback(options, [&] {
            if (!state->AddWriteWaiter(waiter)) {
                add_failed = true;
                waiter->Notify();
                return true;
            }
            if (state->CanAcceptWrite(wanted) || state->Closed())
                waiter->Notify();
            return true;
        });
        if (add_failed)
            return {0, no_buffer};
        if (status != bbt::coroutine::WaitStatus::Completed)
            return {0, WaitStatusError(status)};
    }
    /**
     * 接纳后的每个出口均在写锁内完成或撤销，解除对本调用栈的借用。
     * 容量与完成等待复用同一绝对 deadline；已发布终态先于撤销。
     */
    try {
        for (;;) {
            const auto sample = state->QueryWrite(result);
            if (sample.terminal)
                return {sample.sent, sample.error};
            auto waiter = state->CreateWriteWaiter();
            if (!waiter)
                return state->CancelWrite(id, result, no_buffer);
            bool add_failed = false;
            const auto status = waiter->WaitWithCallback(options, [&] {
                if (!state->AddWriteWaiter(waiter)) {
                    add_failed = true;
                    waiter->Notify();
                    return true;
                }
                if (state->QueryWrite(result).terminal)
                    waiter->Notify();
                return true;
            });
            if (add_failed)
                return state->CancelWrite(id, result, no_buffer);
            if (status != bbt::coroutine::WaitStatus::Completed) {
                return state->CancelWrite(id, result, WaitStatusError(status));
            }
        }
    } catch (...) {
        state->CancelWrite(id, result, std::make_error_code(std::errc::operation_canceled));
        throw;
    }
}

std::error_code CoTcpConnection::WaitClosed(const bbt::coroutine::WaitOptions& options)
{

    /**
     * gate 先于状态判定。
     */
    if (const auto gate = RequireCoroutineContext()) {
        return gate;
    }
    const auto state = CoAccess::StateOf(m_conn);
    if (!state) {

        /**
         * 无效句柄视为已关闭终态
         */
        return {};
    }
    for (;;) {
        if (m_conn.Closed()) {
            return {};
        }

        /**
         * close hub 专用等待：Create/Add 两段（Add 在 on_registered 内），
         * 不直接用 RegisterCloseWaiter——后者在等待前注册，早到
         * MarkClosed 的 NotifyAll 会落在未登记事件上丢失。
         */
        auto waiter = state->CreateCloseWaiter();
        if (!waiter) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        bool add_failed = false;
        const auto status = waiter->WaitWithCallback(options, [&] {
            if (!state->AddCloseWaiter(waiter)) {
                add_failed = true;
                waiter->Notify();
                return true;
            }
            if (m_conn.Closed()) {
                waiter->Notify();
            }
            return true;
        });
        if (add_failed) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        if (status != bbt::coroutine::WaitStatus::Completed) {

            /**
             * 超时/取消不关闭连接
             */
            return WaitStatusError(status);
        }
    }
}

std::error_code CoTcpConnection::Close(const CloseOptions& options, std::error_code reason)
{
    return m_conn.Close(options, reason);
}

// clang-format off
}
// clang-format on
