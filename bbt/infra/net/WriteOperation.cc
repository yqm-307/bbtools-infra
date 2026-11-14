#include <bbt/infra/net/WriteOperation.hpp>

#include <bbt/infra/net/detail/CoInternal.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>

namespace bbt::infra::net {

namespace {

/**
 * WaitStatus 完整枚举逐项映射（不设 io_error 默认项）：上游该 SHA 恰有
 * Completed/TimedOut/Cancelled/InvalidContext/AlreadyWaiting/RuntimeUnavailable。
 * 返回 true 表示已是终态（调用者应把 status_error 交回或终止等待）。
 */
struct WaitMapping {
    bool terminal{false};
    std::error_code error;
};

WaitMapping MapWaitStatus(bbt::coroutine::WaitStatus status)
{

    /**
     * Completed 表示本次等待被通知，还需调用者复查谓词；其余状态一律终结。
     */
    if (status == bbt::coroutine::WaitStatus::Completed) {
        return {false, {}};
    }
    return {true, WaitStatusError(status)};
}

// clang-format off
}
// clang-format on

std::error_code WriteOperation::Wait(const bbt::coroutine::WaitOptions& options, WriteOutcome* outcome) const
{

    /**
     * gate 在查任何快路径之前（与 ICoConnection::Write 同契约）：非法域即使
     * 票已落定也返回 operation_not_permitted。
     */
    if (const auto gate = RequireCoroutineContext()) {
        if (outcome) {
            *outcome = Query();
        }
        return gate;
    }
    if (!m_record) {

        /**
         * 无身份票
         */
        return std::make_error_code(std::errc::invalid_argument);
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

            /**
             * state 已回收的 record 不再会被推进：采样即最终事实，
             * 返回空错误与当前采样（通常是 InFlight 冻结态）。
             */
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

            /**
             * 超时/取消只结束本次等待，不撤销已接纳发送；outcome 是返回
             * 时采样，可能已 Done。
             */
            if (outcome) {
                *outcome = Query();
            }
            return mapped.error;
        }
    }
}

// clang-format off
}
// clang-format on

namespace bbt::infra::net {
WriteOperation::WriteOperation(std::shared_ptr<WriteOpRecord> record, std::weak_ptr<ConnectionState> state)
    : m_record(std::move(record))
    , m_state(std::move(state))
{
}

bool WriteOperation::Valid() const noexcept
{
    return m_record != nullptr;
}

std::uint64_t WriteOperation::Id() const noexcept
{
    return m_record ? m_record->id : 0;
}

WriteOutcome WriteOperation::Query() const
{
    if (!m_record) {
        return {};
    }
    std::lock_guard<std::mutex> lock(m_record->mutex);
    if (!m_record->terminal) {
        return WriteOutcome{WritePhase::InFlight, m_record->sent, {}};
    }
    return WriteOutcome{WritePhase::Done, m_record->sent, m_record->error};
}

// clang-format off
}
// clang-format on
