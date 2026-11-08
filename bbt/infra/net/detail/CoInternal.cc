#include <bbt/infra/net/detail/CoInternal.hpp>

#include <utility>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/detail/ReclaimRecord.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>

namespace bbt::infra::net {

namespace {

std::error_code InvalidContext()
{
    return std::make_error_code(std::errc::operation_not_permitted);
}

} // namespace

// 拨号私有 test 屏障（正常为空）：Engine::CoDial 提交之后、领取 attach 之前调用。
std::function<void()>& DialBarrierForTest()
{
    static std::function<void()> barrier;
    return barrier;
}

std::error_code RequireCoroutineContext()
{
    // 与 ICoObject::RequireCoroutine 同源判定：不构造 CoObject、不缓存 TLS。
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
        // 唯一等待位被占用：本地并发冲突，不是 I/O 结果。
        return std::make_error_code(std::errc::device_or_resource_busy);
    case bbt::coroutine::WaitStatus::RuntimeUnavailable:
        return std::make_error_code(std::errc::operation_not_supported);
    }
    return std::make_error_code(std::errc::operation_not_supported);
}

void CoTestAccess::SetDialBarrier(std::function<void()> barrier)
{
    DialBarrierForTest() = std::move(barrier);
}

std::shared_ptr<ConnectionState> CoAccess::StateOf(const Connection& connection)
{
    return connection.m_state;
}

std::error_code CoAccess::SubmitConnect(Engine& engine, const Address& address, std::uint64_t& out_connect_id,
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

std::error_code CoAccess::WaitConnect(Engine& engine, std::uint64_t connect_id, Connection& out,
                                      const bbt::coroutine::WaitOptions& options)
{
    return engine.CoWaitConnect(connect_id, out, options);
}

std::error_code CoAccess::WaitAccepted(Engine& engine, Connection& out, const bbt::coroutine::WaitOptions& options)
{
    return engine.CoWaitAccepted(out, options);
}

} // namespace bbt::infra::net
