#include <bbt/infra/rpc/Hooks.hpp>

namespace bbt::infra::rpc {

result<std::uint64_t> SessionHooks::Arm(HookPoint point, HookAction action, bbt::coroutine::Deadline expires_at)
{
    const auto index = IndexOf(point);
    if (index == kHookPointCount) {
        return result<std::uint64_t>::err(MakeError(ErrorCode::InvalidArgument, "rpc hook: unknown hook point"));
    }
    if (action != HookAction::Pause && action != HookAction::ThrowOnce) {
        return result<std::uint64_t>::err(MakeError(ErrorCode::InvalidArgument, "rpc hook: unknown hook action"));
    }
    const auto now = std::chrono::steady_clock::now();
    if (expires_at == bbt::coroutine::Deadline::max() || expires_at <= now || expires_at > now + kMaxArmLifetime) {
        return result<std::uint64_t>::err(
            MakeError(ErrorCode::InvalidArgument, "rpc hook: arm window must be (now, now + 10s]"));
    }
    std::lock_guard<std::mutex> lock(m_mu);
    if (m_closed) {
        return result<std::uint64_t>::err(MakeError(ErrorCode::Closed, "rpc hook: owner closed"));
    }
    auto& state = m_points[index];
    if (state.generation == std::numeric_limits<std::uint64_t>::max()) {
        return result<std::uint64_t>::err(MakeError(ErrorCode::RuntimeUnavailable, "rpc hook: generation exhausted"));
    }
    ++state.generation;
    state.action = action;
    state.expires_at = expires_at;
    state.armed = true;
    state.entered = 0;
    return result<std::uint64_t>::ok(state.generation);
}

result<void> SessionHooks::Clear(HookPoint point, std::uint64_t generation)
{
    const auto index = IndexOf(point);
    if (index == kHookPointCount) {
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "rpc hook: unknown hook point"));
    }
    std::lock_guard<std::mutex> lock(m_mu);
    auto& state = m_points[index];
    if (!m_closed && state.armed && state.generation == generation) {
        state.armed = false;
    }
    return result<void>::ok();
}

result<HookSnapshot> SessionHooks::Snapshot(HookPoint point) const
{
    const auto index = IndexOf(point);
    if (index == kHookPointCount) {
        return result<HookSnapshot>::err(MakeError(ErrorCode::InvalidArgument, "rpc hook: unknown hook point"));
    }
    std::lock_guard<std::mutex> lock(m_mu);
    const auto& state = m_points[index];
    HookSnapshot out;
    out.generation = state.generation;
    out.action = state.action;
    out.armed = state.armed;
    out.closed = m_closed;
    out.entered = state.entered;
    out.in_flight = state.in_flight;
    return result<HookSnapshot>::ok(out);
}

} // namespace bbt::infra::rpc
