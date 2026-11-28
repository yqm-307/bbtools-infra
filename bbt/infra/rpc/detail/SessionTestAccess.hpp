#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/ReclaimReceipt.hpp>

namespace bbt::infra::rpc {

class CoRpcClient;
class CoRpcServer;

namespace detail {

// 捕获后始终观察原连接，不持有 RPC 会话。
class LinkReleaseProbe {
public:
    static std::optional<LinkReleaseProbe> ForPeer(const CoRpcClient& client, const std::string& peer);
    // endpoint 与 Call/Send 的 target 相同；仅返回仍在建立台账中的有效凭据。
    static std::optional<net::ReclaimReceipt> EstablishingReceipt(
        const CoRpcClient& client,
        const std::string& endpoint);
    // 从此 owner 当前绑定的 server 捕获尚未移交给 link 的握手连接。
    static std::vector<LinkReleaseProbe> ForHandshakes(const CoRpcClient& client);

    std::uint64_t Id() const noexcept { return m_conn.Id(); }

    bool Released() const noexcept { return m_conn.Released(); }

private:
    explicit LinkReleaseProbe(const net::Connection& conn)
        : m_conn(conn)
    {
    }

    net::Connection m_conn;
};

// The snapshot includes outstanding writes on retired links still held by tasks.
struct ServerLedger {
    std::uint64_t links{0};
    std::uint64_t handshakes{0};
    std::uint64_t writes_outstanding{0};
};

ServerLedger ServerLedgerForTest(const CoRpcServer& server) noexcept;

class ServerTestAccess {
public:
    // Consumed at accept task registration; no effect if Start returns before it.
    static void FailNextStartRegistration() noexcept;
    // Runs after claiming Start, before queue creation, without the server lock.
    static void SetStartRegistrationBarrierOnce(std::function<void()> barrier);
};

// Implementation side of the one-shot Start injections.
bool ConsumeStartRegistrationFailureForTest();
std::function<void()> TakeStartRegistrationBarrierForTest();

// BUILD_TESTING only. The next Call/Send to select an ID takes the callback.
// It runs without RPC locks; the ID view is valid only during the callback.
// Pass {} to disarm a callback that has not been taken.
void SetAttemptIdBarrierOnceForTest(std::function<void(std::string_view)> barrier);
void RunAttemptIdBarrierForTest(std::string_view id);

} // namespace detail
} // namespace bbt::infra::rpc
