#include <bbt/infra/rpc/detail/SessionTestAccess.hpp>

#include <atomic>
#include <mutex>
#include <utility>

#include <bbt/infra/rpc/CoRpcClient.hpp>
#include <bbt/infra/rpc/CoRpcServer.hpp>
#include <bbt/infra/rpc/detail/RpcSessionState.hpp>

namespace bbt::infra::rpc {
namespace {

std::atomic<bool> g_fail_next_start_registration{false};
std::mutex g_start_barrier_mu;
std::function<void()> g_start_barrier;

#ifdef BBT_INFRA_RPC_TEST_ATTEMPT_BARRIER
std::atomic<bool> g_attempt_id_barrier_armed{false};
std::mutex g_attempt_id_barrier_mu;
std::function<void(std::string_view)> g_attempt_id_barrier;
#endif

} // namespace

std::optional<detail::LinkReleaseProbe> detail::LinkReleaseProbe::ForPeer(
    const CoRpcClient& client,
    const std::string& peer)
{
    const auto& owner = client.m_owner;
    std::lock_guard<std::mutex> lock(owner->mu);
    const auto it = owner->by_identity.find(peer);
    if (it != owner->by_identity.end()) {
        if (auto session = it->second.lock()) {
            return LinkReleaseProbe(session->channel->Raw());
        }
    }
    return std::nullopt;
}

std::optional<net::ReclaimReceipt> detail::LinkReleaseProbe::EstablishingReceipt(
    const CoRpcClient& client,
    const std::string& endpoint)
{
    const auto& owner = client.m_owner;
    std::lock_guard<std::mutex> lock(owner->mu);
    const auto it = owner->establishing.find(endpoint);
    if (it != owner->establishing.end() && it->second->reclaim.Valid()) {
        return it->second->reclaim;
    }
    return std::nullopt;
}

std::vector<detail::LinkReleaseProbe> detail::LinkReleaseProbe::ForHandshakes(const CoRpcClient& client)
{
    const auto& owner = client.m_owner;
    std::shared_ptr<ServerState> server;
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        server = owner->inbound_server.lock();
    }
    std::vector<LinkReleaseProbe> probes;
    if (server) {
        std::lock_guard<std::mutex> lock(server->mu);
        for (const auto& entry : server->handshakes) {
            const auto& conn = entry.second.conn;
            if (conn.Valid()) {
                probes.push_back(LinkReleaseProbe(conn));
            }
        }
    }
    return probes;
}

bool detail::ConsumeStartRegistrationFailureForTest()
{
    return g_fail_next_start_registration.exchange(false, std::memory_order_acq_rel);
}

std::function<void()> detail::TakeStartRegistrationBarrierForTest()
{
    std::lock_guard<std::mutex> lock(g_start_barrier_mu);
    return std::move(g_start_barrier);
}

void detail::ServerTestAccess::FailNextStartRegistration() noexcept
{
    g_fail_next_start_registration.store(true, std::memory_order_release);
}

void detail::ServerTestAccess::SetStartRegistrationBarrierOnce(std::function<void()> barrier)
{
    std::lock_guard<std::mutex> lock(g_start_barrier_mu);
    g_start_barrier = std::move(barrier);
}

#ifdef BBT_INFRA_RPC_TEST_ATTEMPT_BARRIER
void detail::SetAttemptIdBarrierOnceForTest(std::function<void(std::string_view)> barrier)
{
    std::lock_guard<std::mutex> lock(g_attempt_id_barrier_mu);
    g_attempt_id_barrier.swap(barrier);
    g_attempt_id_barrier_armed.store(static_cast<bool>(g_attempt_id_barrier), std::memory_order_release);
}

void detail::RunAttemptIdBarrierForTest(std::string_view id)
{
    if (!g_attempt_id_barrier_armed.load(std::memory_order_acquire)) {
        return;
    }
    std::function<void(std::string_view)> barrier;
    {
        std::lock_guard<std::mutex> lock(g_attempt_id_barrier_mu);
        g_attempt_id_barrier_armed.store(false, std::memory_order_release);
        barrier.swap(g_attempt_id_barrier);
    }
    if (barrier) {
        barrier(id);
    }
}
#endif

detail::ServerLedger detail::ServerLedgerForTest(const CoRpcServer& server) noexcept
{
    const auto& state = server.m_state;
    detail::ServerLedger out;
    if (!state) {
        return out;
    }
    std::lock_guard<std::mutex> lock(state->mu);
    state->ReapReleasedLocked();
    for (const auto& lease : state->links) {
        auto session = lease.session.lock();
        if (!session) {
            continue;
        }
        ++out.links;
        out.writes_outstanding += session->OutstandingWrites();
    }
    out.handshakes = state->handshakes.size();
    return out;
}

} // namespace bbt::infra::rpc
