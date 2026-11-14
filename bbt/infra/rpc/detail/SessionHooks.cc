/**
 * RPC 检查点（SessionHooks）的实现：未注册直过、Pause 分片等待、ThrowOnce
 * 固定标签抛出。挂起只在库自己的 CoWaiter 上，不执行用户代码。
 */
#include <bbt/infra/rpc/detail/RpcSessionState.hpp>
#include <bbt/infra/rpc/detail/Session.hpp>

#include <algorithm>

namespace bbt::infra::rpc {
namespace detail {

result<void> SessionHookAccess::Checkpoint(SessionHooks& hooks, HookPoint point, const HookGate& gate)
{
    const auto index = SessionHooks::IndexOf(point);
    if (index == SessionHooks::kHookPointCount) {
        return ErrV(ErrorCode::InvalidArgument, "unknown hook point");
    }
    std::uint64_t generation = 0;
    HookAction action = HookAction::Pause;
    bbt::coroutine::Deadline expires = bbt::coroutine::Deadline::max();
    {
        std::lock_guard<std::mutex> lock(hooks.m_mu);
        if (hooks.m_closed) {
            return ErrV(ErrorCode::Closed, "hook owner closed");
        }
        auto& state = hooks.m_points[index];
        if (state.armed && RpcClock::now() >= state.expires_at) {

            /**
             * 过期未命中的注册（含 ThrowOnce）失效
             */
            state.armed = false;
        }
        if (!state.armed) {
            return result<void>::ok();
        }
        generation = state.generation;
        action = state.action;
        expires = state.expires_at;
        ++state.entered;
        ++state.in_flight;
    }

    /**
     * 所有出口（正常、超时、取消、封口、抛出）都归还 in_flight。
     */
    Flight flight{hooks, index};
    if (action == HookAction::ThrowOnce) {
        {
            std::lock_guard<std::mutex> lock(hooks.m_mu);
            auto& state = hooks.m_points[index];
            if (!state.armed || state.generation != generation) {

                /**
                 * 已被替换/注销：本次通过
                 */
                return result<void>::ok();
            }

            /**
             * 锁内唯一认领并消费
             */
            state.armed = false;
        }
        throw HookInjectedFailure{};
    }
    return PauseUntil(hooks, index, generation, expires, gate);
}

void SessionHookAccess::Close(SessionHooks& hooks) noexcept
{
    std::lock_guard<std::mutex> lock(hooks.m_mu);
    hooks.m_closed = true;
    for (auto& state : hooks.m_points) {
        state.armed = false;
    }
}

void SessionHookAccess::Leave(SessionHooks& hooks, std::size_t index) noexcept
{
    std::lock_guard<std::mutex> lock(hooks.m_mu);
    auto& state = hooks.m_points[index];
    if (state.in_flight != 0) {
        --state.in_flight;
    }
}

/**
 * Pause 的等待循环：每片重新取 min(操作期限, 注册到期, now + 10ms)，
 * 并在每片前后检查库对象终态。每片都不持 hook 锁。
 */
result<void> SessionHookAccess::PauseUntil(
    SessionHooks& hooks,
    std::size_t index,
    std::uint64_t generation,
    bbt::coroutine::Deadline expires,
    const HookGate& gate)
{

    /**
     * 库对象终态：任一所属边界关闭都直接退出，不等注册被 Clear。
     */
    const auto closed_reason = [&gate]() -> const char* {
        if (gate.owner && SessionOwnerClosed(*gate.owner)) {
            return "hook pause: owner closed";
        }
        if (gate.server && ServerV2Closed(*gate.server)) {
            return "hook pause: server closed";
        }
        if (gate.session && gate.session->dead.load()) {
            return "hook pause: session closed";
        }
        if (gate.channel && gate.channel->IsClosed()) {
            return "hook pause: connection closed";
        }
        return nullptr;
    };
    auto waiter = bbt::coroutine::sync::CoWaiter::Create();
    for (;;) {
        if (const char* reason = closed_reason()) {
            return ErrV(ErrorCode::Closed, reason);
        }
        bool paused = false;
        {
            std::lock_guard<std::mutex> lock(hooks.m_mu);
            if (hooks.m_closed) {
                return ErrV(ErrorCode::Closed, "hook owner closed");
            }
            auto& state = hooks.m_points[index];
            if (state.armed && RpcClock::now() >= state.expires_at) {
                state.armed = false;
            }
            paused = state.armed && state.generation == generation;
        }

        /**
         * 注销/替换/到期：解除暂停。
         */
        if (!paused) {
            break;
        }
        const auto now = RpcClock::now();
        if (now >= gate.deadline) {
            return ErrV(ErrorCode::TimedOut, "hook pause: operation deadline");
        }
        const auto slice = std::min({gate.deadline, expires, now + SessionHooks::kPauseSlice});
        const auto status = waiter->Wait({slice});
        switch (status) {
        case bbt::coroutine::WaitStatus::Completed:
        case bbt::coroutine::WaitStatus::TimedOut:

            /**
             * 本片结束，下一片重新检查注册与终态
             */
            break;
        case bbt::coroutine::WaitStatus::Cancelled:
            return ErrV(ErrorCode::Cancelled, "hook pause: cancelled");
        case bbt::coroutine::WaitStatus::InvalidContext:
            return ErrV(ErrorCode::InvalidContext, "hook pause requires coroutine context");
        case bbt::coroutine::WaitStatus::AlreadyWaiting:
        case bbt::coroutine::WaitStatus::RuntimeUnavailable:
            return ErrV(ErrorCode::RuntimeUnavailable, "hook pause: wait unavailable");
        }
    }

    /**
     * 解除后再次检查关闭与操作期限，随后才允许提交 I/O 或发布 link。
     */
    if (const char* reason = closed_reason()) {
        return ErrV(ErrorCode::Closed, reason);
    }
    if (RpcClock::now() >= gate.deadline) {
        return ErrV(ErrorCode::TimedOut, "hook pause: operation deadline");
    }
    return result<void>::ok();
}

// clang-format off
}
// clang-format on
// clang-format off
}
// clang-format on
