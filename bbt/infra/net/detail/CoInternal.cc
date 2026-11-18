#include <bbt/infra/net/detail/CoInternal.hpp>

#include <utility>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/detail/ReclaimRecord.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>
#include <bbt/infra/net/detail/EngineTestAccess.hpp>
#include <bbt/infra/net/detail/TestHooks.hpp>

namespace bbt::infra::net {

namespace {

std::error_code InvalidContext()
{
    return std::make_error_code(std::errc::operation_not_permitted);
}

// clang-format off
}
// clang-format on

std::error_code RequireCoroutineContext()
{

    /**
     * 与 ICoObject::RequireCoroutine 同源判定：不构造 CoObject、不缓存 TLS。
     */
    return bbt::coroutine::IsInCoroutineContext() ? std::error_code{} : InvalidContext();
}

std::error_code WaitStatusError(bbt::coroutine::WaitStatus status)
{
    switch (status) {
    case bbt::coroutine::WaitStatus::Completed:
        return {};
    case bbt::coroutine::WaitStatus::TimedOut:
        return std::make_error_code(std::errc::timed_out);
    case bbt::coroutine::WaitStatus::Cancelled:
        return std::make_error_code(std::errc::operation_canceled);
    case bbt::coroutine::WaitStatus::InvalidContext:
        return InvalidContext();
    case bbt::coroutine::WaitStatus::AlreadyWaiting:

        /**
         * 唯一等待位被占用：本地并发冲突，不是 I/O 结果。
         */
        return std::make_error_code(std::errc::device_or_resource_busy);
    case bbt::coroutine::WaitStatus::RuntimeUnavailable:
        return std::make_error_code(std::errc::operation_not_supported);
    }
    return std::make_error_code(std::errc::operation_not_supported);
}

void CoTestAccess::SetDialBarrier(Engine& engine, std::function<void()> barrier)
{
    EngineTestAccess::ArmHook(engine, HookPoint::AfterDialSubmitBeforeAttach, std::move(barrier));
}

std::error_code CoAccess::Read(const Connection& connection, Buffer& out, const bbt::coroutine::WaitOptions& options)
{
    return connection.ReadCo(out, options);
}

std::tuple<std::size_t, std::error_code>
CoAccess::Write(const Connection& connection, std::string data, const bbt::coroutine::WaitOptions& options)
{
    return connection.WriteCo(std::move(data), options);
}

std::error_code CoAccess::WaitClosed(const Connection& connection, const bbt::coroutine::WaitOptions& options)
{
    return connection.WaitClosedCo(options);
}

std::error_code CoAccess::SubmitConnect(
    Engine& engine,
    const Address& address,
    std::uint64_t& out_connect_id,
    const std::shared_ptr<ReclaimRecord>& reclaim)
{
    if (reclaim) {
        return engine.CoSubmitConnect(address, out_connect_id, reclaim);
    }
    return engine.CoSubmitConnect(address, out_connect_id);
}

ReclaimReceipt CoAccess::MakeReceipt(std::shared_ptr<ReclaimRecord> record)
{
    return ReclaimReceipt(std::move(record));
}

std::error_code CoAccess::WaitConnect(
    Engine& engine, std::uint64_t connect_id, Connection& out, const bbt::coroutine::WaitOptions& options)
{
    return engine.CoWaitConnect(connect_id, out, options);
}

std::error_code CoAccess::WaitAccepted(Engine& engine, Connection& out, const bbt::coroutine::WaitOptions& options)
{
    return engine.CoWaitAccepted(out, options);
}

// clang-format off
}
// clang-format on
