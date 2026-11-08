// 会话服务端生命周期；装配合法性与关闭排序仍由 ServerV2State 台账承担，
// 公共面只有 CoRpcServer：Create/Start/RegisterRpc/Close/Shutdown/观察入口。
#include <bbt/infra/rpc/CoRpcServer.hpp>
#include <bbt/infra/rpc/CoRpcClient.hpp>

#include <cmath>
#include <utility>

#include <bbt/infra/rpc/detail/RpcSessionState.hpp>
#include <bbt/infra/rpc/detail/Session.hpp>

namespace bbt::infra::rpc {

CoRpcServer::CoRpcServer(std::shared_ptr<ServerV2State> v2) noexcept : m_v2(std::move(v2))
{
}

result<std::shared_ptr<CoRpcServer>> CoRpcServer::Create(std::shared_ptr<net::Engine> engine,
                                                         std::shared_ptr<CoRpcClient> session_owner,
                                                         AdmissionHandler admission_handler, ServerOptions options)
{
    using R = result<std::shared_ptr<CoRpcServer>>;
    if (!session_owner) {
        return R::err(Err(ErrorCode::InvalidArgument, "session_owner required"));
    }
    // CoRpcClient 的 m_owner 访问经 friend class CoRpcServer 授权。
    auto owner = session_owner->m_owner;
    auto made = detail::ServerV2Create(std::move(engine), owner, std::move(admission_handler), std::move(options));
    if (!made) {
        return R::err(made.error());
    }
    auto v2 = std::move(made.value());
    return R::ok(bbt::coroutine::MakeCoObject<CoRpcServer>(
        "infra.rpc.CoRpcServer", "", [&] { return std::shared_ptr<CoRpcServer>(new CoRpcServer(std::move(v2))); }));
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
    std::lock_guard<std::mutex> lock(m_v2->mu);
    if (m_v2->closed) {
        return ErrV(ErrorCode::Closed, "rpc server is closed");
    }
    if (m_v2->routes.find(rpc_name) != m_v2->routes.end()) {
        return ErrV(ErrorCode::InvalidArgument, "rpc route already registered");
    }
    ServerV2State::Route route;
    route.handler = std::move(handler);
    route.options = options;
    route.tokens = static_cast<double>(options.burst_tokens);
    route.last_refill = RpcClock::now();
    m_v2->routes.emplace(std::move(rpc_name), std::move(route));
    return result<void>::ok();
}

result<void> CoRpcServer::UnregisterRpc(std::string_view rpc_name)
{
    std::lock_guard<std::mutex> lock(m_v2->mu);
    if (m_v2->routes.erase(std::string(rpc_name)) == 0) {
        return ErrV(ErrorCode::NotFound, "rpc route not found");
    }
    return result<void>::ok();
}

result<void> CoRpcServer::Start()
{
    return detail::ServerV2Start(m_v2);
}

bbt_co_async(result<void>) CoRpcServer::Close(CloseFlag flag, std::int64_t timeout_ms, Error reason)
{
    return detail::ServerV2Close(m_v2, flag, timeout_ms, std::move(reason));
}

void CoRpcServer::Shutdown(Error reason) noexcept
{
    detail::ServerV2Seal(m_v2, SettleCloseReason(std::move(reason), "rpc server: closed by owner"));
}

CoRpcServer::~CoRpcServer() noexcept
{
    detail::ServerV2Seal(m_v2, SettleCloseReason(Error{}, "rpc server: closed by owner"));
}

bool CoRpcServer::IsClosed() const noexcept
{
    return detail::ServerV2Closed(*m_v2);
}

std::optional<Error> CoRpcServer::CloseReason() const noexcept
{
    return detail::ServerV2CloseReason(*m_v2);
}

result<bbt::infra::net::Address> CoRpcServer::LocalAddress() const
{
    using R = result<bbt::infra::net::Address>;
    {
        std::lock_guard<std::mutex> lock(m_v2->mu);
        if (!m_v2->started) {
            return R::err(Err(ErrorCode::InvalidContext, "v2 server not started"));
        }
    }
    std::vector<net::Address> addresses;
    if (auto error = m_v2->engine->LocalAddresses(addresses); error || addresses.empty()) {
        return R::err(Err(ErrorCode::Unavailable, "listen address not observable"));
    }
    return R::ok(addresses.front());
}

ServerStats CoRpcServer::Stats() const noexcept
{
    ServerStats out;
    auto& v2 = *m_v2;
    std::lock_guard<std::mutex> lock(v2.mu);
    out.accepted_connections = v2.accepted;
    std::uint64_t active = 0;
    for (const auto& weak : v2.links) {
        if (auto session = weak.lock(); session && !session->dead.load()) {
            ++active;
        }
    }
    out.active_connections = active;
    out.retiring_connections = v2.links.size() > active ? v2.links.size() - active : 0;
    out.callbacks = v2.callbacks;
    // queue 计数经 AdmissionQueue 同步观察面读（impl->mu 内取），不得在
    // 只持 v2.mu 时直读 impl 字段——避免跨锁直读未同步字段。注意 Queued(i)
    // 与 BytesQueued(i) 是两次独立取锁：本快照只保证每个字段自身读数合法，
    // 不构成同一时刻的一致视图（见 Stats.hpp 文件头「取样语义」）。
    auto queue = v2.queue;
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
        dst.inflight = v2.inflight[i];
        dst.finished = v2.finished[i];
        dst.dropped = v2.dropped[i];
        dst.queued = queued_items[i];
        dst.bytes_queued = queued_bytes[i];
    }
    return out;
}

detail::ServerV2Ledger detail::ServerV2LedgerForTest(const CoRpcServer& server) noexcept
{
    return detail::ServerV2LedgerOf(server.m_v2);
}

} // namespace bbt::infra::rpc
