#include <bbt/infra/net/CoConnection.hpp>

#include <cstdint>
#include <functional>
#include <system_error>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/Error.hpp>
#include <bbt/infra/net/detail/CoInternal.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/ReclaimRecord.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>

// 实现放 .cc：WriteOperation::Wait、Engine::CoAccept/CoDial 依赖的内部装配
// （统一 gate、ConnectionState、Engine 私有等待步骤、拨号测试屏障）不能经
// 公共头传出；同时避免把 <bbt/coroutine/coroutine.hpp> 带进消费 TU。
// CoRead/CoWrite/CoAccept/CoDial 自由函数已删除：协程消费面收口为
// Engine::CoAccept/CoDial 与 CoTcpConnection 成员（docs/decisions/0012-icoconn-member-interface.md）。

namespace bbt::infra::net {

namespace {

std::error_code InvalidContext()
{
    return std::make_error_code(std::errc::operation_not_permitted);
}

// WaitStatus 完整枚举逐项映射（不设 io_error 默认项）：上游该 SHA 恰有
// Completed/TimedOut/Cancelled/InvalidContext/AlreadyWaiting/RuntimeUnavailable。
// 返回 true 表示已是终态（调用者应把 status_error 交回或终止等待）。
struct WaitMapping {
    bool terminal{false};
    std::error_code error;
};

WaitMapping MapWaitStatus(bbt::coroutine::WaitStatus status)
{
    // Completed 表示本次等待被通知，还需调用者复查谓词；其余状态一律终结。
    if (status == bbt::coroutine::WaitStatus::Completed) {
        return {false, {}};
    }
    return {true, WaitStatusError(status)};
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
    return bbt::coroutine::GetLocalCoroutineId() != 0 ? std::error_code{} : InvalidContext();
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

std::error_code CoAccess::SubmitConnect(Engine& engine, const Address& address,
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

std::error_code CoAccess::WaitConnect(Engine& engine, std::uint64_t connect_id, Connection& out,
                                     const bbt::coroutine::WaitOptions& options)
{
    return engine.CoWaitConnect(connect_id, out, options);
}

std::error_code CoAccess::WaitAccepted(Engine& engine, Connection& out,
                                      const bbt::coroutine::WaitOptions& options)
{
    return engine.CoWaitAccepted(out, options);
}

std::error_code WriteOperation::Wait(const bbt::coroutine::WaitOptions& options,
                                     WriteOutcome* outcome) const
{
    // gate 在查任何快路径之前（与 CoRead/CoWrite 同契约）：非法域即使
    // 票已落定也返回 operation_not_permitted。
    if (const auto gate = RequireCoroutineContext()) {
        if (outcome) {
            *outcome = Query();
        }
        return gate;
    }
    // 拒绝票已落定：等待立刻完成，outcome 保留拒绝采样。
    if (m_rejection) {
        if (outcome) {
            *outcome = Query();
        }
        return {};
    }
    if (!m_record) {
        return std::make_error_code(std::errc::invalid_argument);  // 无身份票
    }
    const auto state = m_state.lock();
    for (;;) {
        const auto sample = Query();
        if (sample.phase != WritePhase::InFlight) {
            if (outcome) {
                *outcome = sample;
            }
            return {};
        }
        if (!state) {
            // state 已回收的 record 不再会被推进：采样即最终事实，
            // 返回空错误与当前采样（通常是 InFlight 冻结态）。
            if (outcome) {
                *outcome = sample;
            }
            return {};
        }
        auto waiter = state->CreateWriteWaiter();
        if (!waiter) {
            if (outcome) {
                *outcome = Query();
            }
            return std::make_error_code(std::errc::no_buffer_space);
        }
        bool add_failed = false;
        const auto status = waiter->WaitWithCallback(options, [&] {
            if (!state->AddWriteWaiter(waiter)) {
                add_failed = true;
                waiter->Notify();
                return true;
            }
            if (Query().phase != WritePhase::InFlight) {
                waiter->Notify();
            }
            return true;
        });
        if (add_failed) {
            if (outcome) {
                *outcome = Query();
            }
            return std::make_error_code(std::errc::no_buffer_space);
        }
        const auto mapped = MapWaitStatus(status);
        if (mapped.terminal) {
            // 超时/取消只结束本次等待，不撤销已接纳发送；outcome 是返回
            // 时采样，可能已 Done。
            if (outcome) {
                *outcome = Query();
            }
            return mapped.error;
        }
    }
}

} // namespace bbt::infra::net
