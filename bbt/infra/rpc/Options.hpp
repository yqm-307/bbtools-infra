#pragma once
// bbtools-infra v2：RPC 装配值层 Options（ClientOptions/SendOptions/
// ServerOptions）。契约来源：docs/decisions/0014-app-session-v2.md §2.2/§2.3。
//
// 纯值类型 + Validate()：setter 只存值，Validate 在 Create/Start 边界由
// owner 调用；本头不联网、不创建线程。net::Address 是值类型（sockaddr 快照，
// 无物理栈泄漏，见 cmake 的 rpc_public_header_leak_check 白名单注释）。
//
// trusted-network 明文模式（ADR §3.2）：未显式 SetTrustedNetworkPlaintext(true)
// 且当前没有 verifier API 时，Client::Create/Server::Start 返回
// UnsupportedRoute；peer_principal 恒为空，identity 不用于授权。

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>

#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Address.hpp>

namespace bbt::infra::rpc {

// Client 会话表/帧/lane 上限的默认值与硬上限（ADR §3.2、§5.2）。
inline constexpr std::size_t kDefaultMaxSessions    = 64;
inline constexpr std::size_t kMaxMaxSessions        = 4096;
inline constexpr std::size_t kDefaultMaxFrameBytes  = 8 * 1024 * 1024;  // 8 MiB
inline constexpr std::size_t kMinFrameBytes         = 1024;
// 帧硬上限沿用 kNetworkLimitsMaxBodyBytes 的 1 GiB 边界。
inline constexpr std::size_t kMaxFrameBytes         = kNetworkLimitsMaxBodyBytes;
inline constexpr std::size_t kDefaultQueueBytes     = 16 * 1024 * 1024;
inline constexpr std::size_t kMaxQueueBytes         = 256 * 1024 * 1024;
// Hello/HelloAck payload 上限（ADR §5.2）：握手阶段 min(max_frame,64KiB)。
inline constexpr std::size_t kMaxHelloFrameBytes    = 64 * 1024;

class ClientOptions {
public:
    ClientOptions& SetLocalIdentity(std::string value) {
        m_local_identity = std::move(value); return *this;
    }
    ClientOptions& SetTrustedNetworkPlaintext(bool value) noexcept {
        m_trusted_network_plaintext = value; return *this;
    }
    bool TrustedNetworkPlaintext() const noexcept {
        return m_trusted_network_plaintext;
    }
    const std::string& LocalIdentity() const noexcept { return m_local_identity; }
    ClientOptions& SetMaxSessions(std::size_t value) { m_max_sessions = value; return *this; }
    ClientOptions& SetMaxFrameBytes(std::size_t value) { m_max_frame_bytes = value; return *this; }
    ClientOptions& SetSystemQueueBytes(std::size_t value) { m_system_queue_bytes = value; return *this; }
    ClientOptions& SetServiceQueueBytes(std::size_t value) { m_service_queue_bytes = value; return *this; }
    ClientOptions& SetActorQueueBytes(std::size_t value) { m_actor_queue_bytes = value; return *this; }
    ClientOptions& SetServiceWeight(std::uint32_t value) { m_service_weight = value; return *this; }
    ClientOptions& SetActorWeight(std::uint32_t value) { m_actor_weight = value; return *this; }
    ClientOptions& SetControlDataSplit(bool value) { m_control_data_split = value; return *this; }

    std::size_t   MaxSessions() const noexcept       { return m_max_sessions; }
    std::size_t   MaxFrameBytes() const noexcept     { return m_max_frame_bytes; }
    std::size_t   SystemQueueBytes() const noexcept  { return m_system_queue_bytes; }
    std::size_t   ServiceQueueBytes() const noexcept { return m_service_queue_bytes; }
    std::size_t   ActorQueueBytes() const noexcept   { return m_actor_queue_bytes; }
    std::uint32_t ServiceWeight() const noexcept     { return m_service_weight; }
    std::uint32_t ActorWeight() const noexcept       { return m_actor_weight; }
    bool          ControlDataSplit() const noexcept  { return m_control_data_split; }

    // Create 边界校验：各项有限正数且不超上限；local_identity 非空、UTF-8、
    // ≤256 字节（wire peer_identity 约束，Codec.hpp 同源）。违规 InvalidArgument。
    result<void> Validate() const {
        if (m_local_identity.empty() || m_local_identity.size() > 256 ||
            !infra::detail::IsValidUtf8(m_local_identity))
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "local_identity empty/too long/not UTF-8"));
        if (m_max_sessions == 0 || m_max_sessions > kMaxMaxSessions)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "max_sessions out of range"));
        if (m_max_frame_bytes < kMinFrameBytes || m_max_frame_bytes > kMaxFrameBytes)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "max_frame_bytes out of range"));
        if (m_system_queue_bytes == 0 || m_system_queue_bytes > kMaxQueueBytes ||
            m_service_queue_bytes == 0 || m_service_queue_bytes > kMaxQueueBytes ||
            m_actor_queue_bytes == 0 || m_actor_queue_bytes > kMaxQueueBytes)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "queue bytes out of range"));
        if (m_service_weight == 0 || m_actor_weight == 0)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "lane weight must be non-zero"));
        return result<void>::ok();
    }

private:
    std::string   m_local_identity;
    bool          m_trusted_network_plaintext = false;
    std::size_t   m_max_sessions        = kDefaultMaxSessions;
    std::size_t   m_max_frame_bytes     = kDefaultMaxFrameBytes;
    std::size_t   m_system_queue_bytes  = kDefaultQueueBytes;
    std::size_t   m_service_queue_bytes = kDefaultQueueBytes;
    std::size_t   m_actor_queue_bytes   = kDefaultQueueBytes;
    std::uint32_t m_service_weight      = 1;
    std::uint32_t m_actor_weight        = 1;
    bool          m_control_data_split  = false;
};

// Send 调用选项：deadline 与远端接纳确认。RequireRemoteAcceptance 只编码为
// Send frame 的 acceptance_requested presence/value——false 显式不请求
// 确认（wire 上 presence=false），true 等待一次 SendAccepted。
// 注意：SendOptions 不再携带 traffic class（Request::Traffic() 唯一真源）。
class SendOptions {
public:
    SendOptions& SetDeadline(bbt::coroutine::Deadline value) noexcept {
        m_deadline = value; return *this;
    }
    SendOptions& RequireRemoteAcceptance(bool value = true) noexcept {
        m_require_remote_acceptance = value; return *this;
    }
    bbt::coroutine::Deadline Deadline() const noexcept { return m_deadline; }
    bool RequiresRemoteAcceptance() const noexcept { return m_require_remote_acceptance; }

    // 与 CallOptions 同规则：必须有有限、未过期的 deadline（出站边界拒绝
    // epoch/已过期/远期 max，不让调用进入无界等待）。
    result<void> Validate() const {
        if (m_deadline == bbt::coroutine::Deadline::max() ||
            m_deadline <= std::chrono::steady_clock::now())
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "deadline must be finite and in the future"));
        return result<void>::ok();
    }

private:
    bbt::coroutine::Deadline m_deadline{bbt::coroutine::Deadline::max()};
    bool                     m_require_remote_acceptance = false;
};

class ServerOptions {
public:
    ServerOptions& SetListenAddress(bbt::infra::net::Address value) {
        m_listen_address = value; return *this;
    }
    ServerOptions& SetMaxFrameBytes(std::size_t value) { m_max_frame_bytes = value; return *this; }
    ServerOptions& SetMaxConnections(std::size_t value) { m_max_connections = value; return *this; }
    ServerOptions& SetMaxInflight(std::size_t value) { m_max_inflight = value; return *this; }
    ServerOptions& SetMaxHandlerBytes(std::size_t value) { m_max_handler_bytes = value; return *this; }
    ServerOptions& SetIncomingTimeout(std::chrono::milliseconds value) {
        m_incoming_timeout = value; return *this;
    }

    const bbt::infra::net::Address& ListenAddress() const noexcept { return m_listen_address; }
    std::size_t               MaxFrameBytes() const noexcept    { return m_max_frame_bytes; }
    std::size_t               MaxConnections() const noexcept   { return m_max_connections; }
    std::size_t               MaxInflight() const noexcept      { return m_max_inflight; }
    std::size_t               MaxHandlerBytes() const noexcept  { return m_max_handler_bytes; }
    std::chrono::milliseconds IncomingTimeout() const noexcept { return m_incoming_timeout; }

    // Create 边界校验：各项有限正数且不超 NetworkLimits 硬上限；
    // incoming_timeout 必须有限且 ≤24h；listen_address 必须 Valid()。
    result<void> Validate() const {
        if (!m_listen_address.Valid())
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "listen_address invalid"));
        if (m_max_frame_bytes < kMinFrameBytes || m_max_frame_bytes > kMaxFrameBytes)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "max_frame_bytes out of range"));
        if (m_max_connections == 0 || m_max_connections > kNetworkLimitsMaxConnections)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "max_connections out of range"));
        if (m_max_inflight == 0 || m_max_inflight > kNetworkLimitsMaxInflight)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "max_inflight out of range"));
        if (m_max_handler_bytes == 0 || m_max_handler_bytes > kNetworkLimitsMaxBodyBytes)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "max_handler_bytes out of range"));
        if (m_incoming_timeout <= std::chrono::milliseconds{0} ||
            m_incoming_timeout > kNetworkLimitsMaxIncomingTimeout)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                "incoming_timeout out of range"));
        return result<void>::ok();
    }

private:
    bbt::infra::net::Address m_listen_address;
    std::size_t              m_max_frame_bytes   = kDefaultMaxFrameBytes;
    std::size_t              m_max_connections   = 1024;
    std::size_t              m_max_inflight      = 4096;
    std::size_t              m_max_handler_bytes = kNetworkLimitsMaxBodyBytes;
    std::chrono::milliseconds m_incoming_timeout{std::chrono::seconds{30}};
};

} // namespace bbt::infra::rpc
