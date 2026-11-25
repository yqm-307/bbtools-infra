/**
 * 会话服务端的装配与生命周期入口，关闭排序由 ServerState 维护。
 */
#include <bbt/infra/rpc/CoRpcServer.hpp>
#include <bbt/infra/rpc/CoRpcClient.hpp>

#include <cmath>
#include <utility>

#include <bbt/infra/rpc/detail/RpcSessionState.hpp>
#include <bbt/infra/rpc/detail/Session.hpp>

namespace bbt::infra::rpc {

CoRpcServer::CoRpcServer(std::shared_ptr<ServerState> state) noexcept
    : m_state(std::move(state))
{
}

result<std::shared_ptr<CoRpcServer>> CoRpcServer::Create(
    std::shared_ptr<net::Engine> engine,
    std::shared_ptr<CoRpcClient> session_owner,
    AdmissionHandler admission_handler,
    ServerOptions options)
{
    using R = result<std::shared_ptr<CoRpcServer>>;
    if (!session_owner) {
        return R::err(Err(ErrorCode::InvalidArgument, "session_owner required"));
    }

    /**
     * CoRpcClient 的 m_owner 访问经 friend class CoRpcServer 授权。
     */
    auto owner = session_owner->m_owner;
    auto made = detail::ServerCreate(std::move(engine), owner, std::move(admission_handler), std::move(options));
    if (!made) {
        return R::err(made.error());
    }
    auto state = std::move(made.value());
    return R::ok(bbt::coroutine::MakeCoObject<CoRpcServer>("infra.rpc.CoRpcServer", "", [&] {
        return std::shared_ptr<CoRpcServer>(new CoRpcServer(std::move(state)));
    }));
}

result<void> CoRpcServer::RegisterRpc(std::string rpc_name, RpcHandler handler)
{
    return RegisterRpc(std::move(rpc_name), std::move(handler), RouteOptions{});
}

result<void> CoRpcServer::RegisterRpc(std::string rpc_name, RpcHandler handler, RouteOptions options)
{
    if (rpc_name.empty() || rpc_name.find('/') == std::string::npos || !handler) {
        return ErrV(ErrorCode::InvalidArgument, "rpc route requires name and handler");
    }
    if ((options.burst_tokens == 0) != (options.refill_per_second == 0.0)
        || (options.burst_tokens != 0
            && (!std::isfinite(options.refill_per_second) || options.refill_per_second <= 0.0))) {
        return ErrV(ErrorCode::InvalidArgument, "rpc route rate limit requires burst and positive refill");
    }
    std::lock_guard<std::mutex> lock(m_state->mu);
    if (m_state->closed) {
        return ErrV(ErrorCode::Closed, "rpc server is closed");
    }
    if (m_state->routes.find(rpc_name) != m_state->routes.end()) {
        return ErrV(ErrorCode::InvalidArgument, "rpc route already registered");
    }
    ServerState::Route route;
    route.handler = std::move(handler);
    route.options = options;
    route.tokens = static_cast<double>(options.burst_tokens);
    route.last_refill = RpcClock::now();
    m_state->routes.emplace(std::move(rpc_name), std::move(route));
    return result<void>::ok();
}

result<void> CoRpcServer::UnregisterRpc(std::string_view rpc_name)
{
    std::lock_guard<std::mutex> lock(m_state->mu);
    if (m_state->routes.erase(std::string(rpc_name)) == 0) {
        return ErrV(ErrorCode::NotFound, "rpc route not found");
    }
    return result<void>::ok();
}

result<void> CoRpcServer::Start()
{
    return detail::ServerStart(m_state);
}

bbt_co_async(result<void>) CoRpcServer::Close(const CloseOptions& options)
{
    return bbt_co_bind([state = m_state, options]() -> result<void> { return detail::ServerClose(state, options); });
}

result<void> CoRpcServer::CloseImmediate(const CloseOptions& options)
{
    return detail::ServerClose(m_state, CloseOptions::Immediate(options.reason.value_or(Error{})));
}

CoRpcServer::~CoRpcServer() noexcept
{
    detail::ServerSeal(m_state, SettleCloseReason(Error{}, "rpc server: closed by owner"));
}

bool CoRpcServer::IsClosed() const noexcept
{
    return detail::ServerClosed(*m_state);
}

std::optional<Error> CoRpcServer::CloseReason() const noexcept
{
    return detail::ServerCloseReason(*m_state);
}

result<bbt::infra::net::Address> CoRpcServer::LocalAddress() const
{
    using R = result<bbt::infra::net::Address>;
    {
        std::lock_guard<std::mutex> lock(m_state->mu);
        if (!m_state->started) {
            return R::err(Err(ErrorCode::InvalidContext, "state server not started"));
        }
    }
    std::vector<net::Address> addresses;
    if (auto error = m_state->engine->LocalAddresses(addresses); error || addresses.empty()) {
        return R::err(Err(ErrorCode::Unavailable, "listen address not observable"));
    }
    return R::ok(addresses.front());
}

ServerStats CoRpcServer::Stats() const noexcept
{
    ServerStats out;
    auto& state = *m_state;
    std::lock_guard<std::mutex> lock(state.mu);
    out.accepted_connections = state.accepted;
    std::uint64_t active = 0;
    state.ReapReleasedLocked();
    for (const auto& lease : state.links) {
        if (auto session = lease.session.lock(); session && !session->dead.load()) {
            ++active;
        }
    }
    out.active_connections = active;
    out.retiring_connections = state.links.size() > active ? state.links.size() - active : 0;
    out.callbacks = state.callbacks;

    /**
     * queue 计数经 AdmissionQueue 同步观察面读（impl->mu 内取），不得在
     * 只持 state.mu 时直读 impl 字段——避免跨锁直读未同步字段。注意 Queued(i)
     * 与 BytesQueued(i) 是两次独立取锁：本快照只保证每个字段自身读数合法，
     * 不构成同一时刻的一致视图（见 Stats.hpp 文件头「取样语义」）。
     */
    auto queue = state.queue;
    std::array<std::uint64_t, kLaneCount> queued_items{};
    std::array<std::uint64_t, kLaneCount> queued_bytes{};
    if (queue) {
        for (std::size_t i = 0; i != kLaneCount; ++i) {
            queued_items[i] = queue->Queued(i);
            queued_bytes[i] = queue->BytesQueued(i);
        }
    }
    for (std::size_t i = 0; i != kLaneCount; ++i) {
        auto& dst = i == 0 ? out.system : i == 1 ? out.service : out.actor;
        dst.inflight = state.inflight[i];
        dst.finished = state.finished[i];
        dst.dropped = state.dropped[i];
        dst.queued = queued_items[i];
        dst.bytes_queued = queued_bytes[i];
    }
    return out;
}

// clang-format off
}
// clang-format on
