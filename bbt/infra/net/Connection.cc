#include <bbt/infra/net/Connection.hpp>

#include <utility>
#include <bbt/infra/detail/CloseWait.hpp>

#include <bbt/infra/net/Error.hpp>
#include <bbt/infra/net/detail/CoInternal.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/ReclaimRecord.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>

/**
 * 实现放 .cc：Connection 的转发需要 ConnectionState/ReceiveRing
 * 完整定义，放在公共头会把 state/ring 布局与内部枚举经公共面传出。
 */
namespace bbt::infra::net {

Connection::Connection(std::shared_ptr<ConnectionState> state)
    : m_state(std::move(state))
{
}

std::error_code Connection::ReadCo(Buffer& out, const bbt::coroutine::WaitOptions& options) const
{
    if (const auto gate = RequireCoroutineContext())
        return gate;
    const auto state = m_state;
    return state ? state->Read(out, options) : make_error_code(NetErrc::Closed);
}

std::tuple<std::size_t, std::error_code> Connection::WriteCo(
    std::string data,
    const bbt::coroutine::WaitOptions& options) const
{
    if (const auto gate = RequireCoroutineContext())
        return {0, gate};
    const auto state = m_state;
    if (!state)
        return {0, make_error_code(NetErrc::Closed)};
    return state->Write(std::move(data), options);
}

std::error_code Connection::WaitClosedCo(const bbt::coroutine::WaitOptions& options) const
{
    if (const auto gate = RequireCoroutineContext())
        return gate;
    const auto state = m_state;
    return state ? state->WaitClosed(options) : std::error_code{};
}

std::shared_ptr<ReclaimRecord> Connection::ReclaimForOwner() const
{
    return m_state ? m_state->OwnerReleasedRecord() : nullptr;
}

std::tuple<std::uintptr_t, std::size_t, std::size_t, std::uint64_t, bool, bool> Connection::InspectForTest() const
{
    return m_state ? m_state->InspectForTest()
                   : std::tuple<std::uintptr_t, std::size_t, std::size_t, std::uint64_t, bool, bool>{};
}

bool Connection::ExerciseReadContentionForTest(void (*during)(const void*), const void* context) const
{
    return m_state && m_state->ExerciseReadContentionForTest([&] { during(context); });
}

bool Connection::DeferReceivedForTest(const std::string& data) const
{
    return m_state && m_state->DeferReceived(data);
}

bool Connection::Valid() const noexcept
{
    return m_state != nullptr;
}

std::uint64_t Connection::Id() const noexcept
{
    return m_state ? m_state->Id() : 0;
}

std::error_code Connection::AsyncWrite(std::string data)
{
    if (!m_state) {
        return make_error_code(NetErrc::Closed);
    }

    /**
     * 记录只供 owner 做 FIFO/部分发送及 OnWrite 结算，不创建等待者或交付票据。
     */
    return m_state->AcceptFire(data);
}

ReadResult Connection::TryRead(std::string& out)
{
    if (!m_state) {
        return {ReadStatus::Error, 0, make_error_code(NetErrc::Closed)};
    }
    return m_state->TryRead(out);
}

std::error_code Connection::TryRead(Buffer& out)
{
    if (!m_state) {
        return make_error_code(NetErrc::Closed);
    }
    return m_state->TryRead(out);
}

std::tuple<Buffer, std::error_code> Connection::TryRead()
{
    Buffer out;
    auto error = TryRead(out);
    return {std::move(out), error};
}

ReadView Connection::View()
{
    if (!m_state) {
        return ReadView(ReadStatus::Error, {}, make_error_code(NetErrc::Closed));
    }
    return m_state->View();
}

std::size_t Connection::ReadableBytes()
{
    return m_state ? m_state->Inspect().readable_bytes : 0;
}

std::error_code Connection::ReadError() const
{
    return m_state ? m_state->Inspect().read_error : std::error_code{};
}

bool Connection::ReadEof() const
{
    return m_state ? m_state->Inspect().read_eof : true;
}

std::error_code Connection::LastWriteError(std::uint64_t* operation)
{
    return m_state ? m_state->LastWriteError(operation) : make_error_code(NetErrc::Closed);
}

AcceptanceState Connection::WriteAcceptance() const
{
    return m_state ? m_state->Acceptance() : AcceptanceState::Closed;
}

bool Connection::Closed() const
{
    return m_state ? m_state->Closed() : true;
}

bool Connection::Released() const noexcept
{

    /**
     * 无效句柄没有资源责任；有效句柄以 owner 物理收口标记为准（conn_state
     * 记录在 fd 真实关闭且 owner 责任清理后置位）。
     */
    return !m_state || m_state->OwnerReleased();
}

std::error_code Connection::CloseReason() const
{
    return m_state ? m_state->CloseReason() : make_error_code(NetErrc::Closed);
}

std::error_code Connection::Close(const CloseOptions& options, std::error_code reason)
{
    if (!bbt::infra::detail::ValidateCloseOptions(options) || options.mode != CloseMode::Immediate
        || (options.reason && !options.reason->message.empty()))
        return std::make_error_code(std::errc::invalid_argument);

    /**
     * Raw callbacks must not wait for their owner. Co waiting only suspends the caller.
     */
    if (options.wait_release && !bbt::coroutine::IsInCoroutineContext())
        return std::make_error_code(std::errc::operation_not_permitted);
    auto signal = options.wait_release ? bbt::coroutine::sync::CoWaiter::Create() : nullptr;
    if (options.wait_release && !signal)
        return std::make_error_code(std::errc::resource_unavailable_try_again);
    if (m_state)
        m_state->RequestClose(reason);
    while (options.wait_release && !Released()) {
        if (std::chrono::steady_clock::now() >= options.deadline)
            return std::make_error_code(std::errc::timed_out);
        auto waited = bbt::infra::detail::WaitCloseSlice(signal, bbt::infra::detail::NextCloseSlice(options.deadline));
        if (!waited)
            return std::make_error_code(std::errc::operation_canceled);
    }
    return {};
}

// clang-format off
}
// clang-format on
