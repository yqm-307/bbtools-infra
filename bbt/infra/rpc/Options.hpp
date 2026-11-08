/**
 * @file Options.hpp
 * @brief RPC 装配值层：ClientOptions / SendOptions / ServerOptions。
 *
 * 纯值类型 + Validate()：setter 只存值，Validate 在 Create/Start 边界由
 * owner 调用；本头不联网、不创建线程。net::Address 是值类型（sockaddr 快照，
 * 无物理栈泄漏，见 cmake 的 rpc_public_header_leak_check 白名单注释）。
 *
 * trusted-network 明文模式（ADR 0014 §3.2）：未显式 SetTrustedNetworkPlaintext(true)
 * 且当前没有 verifier API 时，CoRpcClient::Create/CoRpcServer::Start 返回
 * UnsupportedRoute；peer_principal 恒为空，identity 不用于授权。
 *
 * 契约来源：docs/decisions/0014-app-session.md §2.2/§2.3。
 */
#pragma once

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

/**
 * @brief Client 会话表/帧/lane 上限的默认值与硬上限（ADR 0014 §3.2、§5.2）。
 */
inline constexpr std::size_t kDefaultMaxSessions = 64;                ///< 会话表默认上限
inline constexpr std::size_t kMaxMaxSessions = 4096;                  ///< 会话表硬上限
inline constexpr std::size_t kDefaultMaxFrameBytes = 8 * 1024 * 1024; ///< 帧长默认上限（8 MiB）
inline constexpr std::size_t kMinFrameBytes = 1024;                   ///< 帧长下限
/// 帧硬上限沿用 kNetworkLimitsMaxBodyBytes 的 1 GiB 边界。
inline constexpr std::size_t kMaxFrameBytes = kNetworkLimitsMaxBodyBytes;
inline constexpr std::size_t kDefaultQueueBytes = 16 * 1024 * 1024; ///< lane 待发默认上限
inline constexpr std::size_t kMaxQueueBytes = 256 * 1024 * 1024;    ///< lane 待发硬上限
/// Hello/HelloAck payload 上限（ADR 0014 §5.2）：握手阶段 min(max_frame,64KiB)。
inline constexpr std::size_t kMaxHelloFrameBytes = 64 * 1024;

/**
 * @brief 统一 Close 入口的关闭模式。
 *
 * CoRpcClient/CoRpcServer 的 Close 只经本枚举表达语义，不再提供
 * Drain/WaitStopped/StopAccepting 等独立生命周期阶段：
 *   - Immediate：立即逻辑封口并发布原因；强制关闭 session/link，不等待。
 *   - Timeout：先强制收口，再有界等待 RPC 自身资源物理收口；timeout_ms 为
 *     毫秒预算。
 *   - Graceful：先拒新，等已接纳业务责任真实归还后封口；timeout_ms=-1
 *     表示无限等待（-1 只对 Graceful 有效）。
 */
enum class CloseFlag : std::uint8_t {
    Immediate,
    Timeout,
    Graceful,
};

/**
 * @brief Client 出站配置。
 *
 * setter 只存值，不校验；调用 Validate() 或通过 CoRpcClient::Create 边界时统一校验。
 * local_identity 须非空 UTF-8 且 ≤256 字节（wire peer_identity 约束）。
 * SetTrustedNetworkPlaintext(true) 是启用明文模式的唯一入口；未设置时
 * CoRpcClient::Create 返回 UnsupportedRoute（ADR 0014 §3.2）。
 */
class ClientOptions {
public:
    /// 设置本地身份（wire peer_identity；非空 UTF-8，≤256 字节）。
    ClientOptions& SetLocalIdentity(std::string value)
    {
        m_local_identity = std::move(value);
        return *this;
    }
    /// 启用/关闭 trusted-network 明文模式；这是启用明文模式的唯一入口。
    ClientOptions& SetTrustedNetworkPlaintext(bool value) noexcept
    {
        m_trusted_network_plaintext = value;
        return *this;
    }
    /// 是否已启用 trusted-network 明文模式。
    bool TrustedNetworkPlaintext() const noexcept
    {
        return m_trusted_network_plaintext;
    }
    /// 当前本地身份。
    const std::string& LocalIdentity() const noexcept
    {
        return m_local_identity;
    }
    /// 会话表上限（kMaxMaxSessions 为硬上限）。
    ClientOptions& SetMaxSessions(std::size_t value)
    {
        m_max_sessions = value;
        return *this;
    }
    /// 单帧字节上限（kMinFrameBytes..kMaxFrameBytes）。
    ClientOptions& SetMaxFrameBytes(std::size_t value)
    {
        m_max_frame_bytes = value;
        return *this;
    }
    /// System lane 待发字节上限。
    ClientOptions& SetSystemQueueBytes(std::size_t value)
    {
        m_system_queue_bytes = value;
        return *this;
    }
    /// Service lane 待发字节上限。
    ClientOptions& SetServiceQueueBytes(std::size_t value)
    {
        m_service_queue_bytes = value;
        return *this;
    }
    /// Actor lane 待发字节上限。
    ClientOptions& SetActorQueueBytes(std::size_t value)
    {
        m_actor_queue_bytes = value;
        return *this;
    }
    /// Service lane 权重（须非零）。
    ClientOptions& SetServiceWeight(std::uint32_t value)
    {
        m_service_weight = value;
        return *this;
    }
    /// Actor lane 权重（须非零）。
    ClientOptions& SetActorWeight(std::uint32_t value)
    {
        m_actor_weight = value;
        return *this;
    }
    /// 是否把控制帧与数据帧拆到独立 lane。
    ClientOptions& SetControlDataSplit(bool value)
    {
        m_control_data_split = value;
        return *this;
    }

    /// 会话表上限。
    std::size_t MaxSessions() const noexcept
    {
        return m_max_sessions;
    }
    /// 单帧字节上限。
    std::size_t MaxFrameBytes() const noexcept
    {
        return m_max_frame_bytes;
    }
    /// System lane 待发字节上限。
    std::size_t SystemQueueBytes() const noexcept
    {
        return m_system_queue_bytes;
    }
    /// Service lane 待发字节上限。
    std::size_t ServiceQueueBytes() const noexcept
    {
        return m_service_queue_bytes;
    }
    /// Actor lane 待发字节上限。
    std::size_t ActorQueueBytes() const noexcept
    {
        return m_actor_queue_bytes;
    }
    /// Service lane 权重。
    std::uint32_t ServiceWeight() const noexcept
    {
        return m_service_weight;
    }
    /// Actor lane 权重。
    std::uint32_t ActorWeight() const noexcept
    {
        return m_actor_weight;
    }
    /// 控制/数据是否分 lane。
    bool ControlDataSplit() const noexcept
    {
        return m_control_data_split;
    }

    /// @brief Create 边界校验。
    ///
    /// 各项须为有限正数且不超上限；local_identity 非空、UTF-8、≤256 字节
    /// （wire peer_identity 约束，SessionCodec.hpp 同源）。
    /// @return 违规返回 InvalidArgument（message 指明首个越界字段）。
    result<void> Validate() const;

private:
    std::string m_local_identity;
    bool m_trusted_network_plaintext = false;
    std::size_t m_max_sessions = kDefaultMaxSessions;
    std::size_t m_max_frame_bytes = kDefaultMaxFrameBytes;
    std::size_t m_system_queue_bytes = kDefaultQueueBytes;
    std::size_t m_service_queue_bytes = kDefaultQueueBytes;
    std::size_t m_actor_queue_bytes = kDefaultQueueBytes;
    std::uint32_t m_service_weight = 1;
    std::uint32_t m_actor_weight = 1;
    bool m_control_data_split = false;
};

/**
 * @brief Send 调用选项：deadline 与远端接纳确认。
 *
 * RequireRemoteAcceptance(true) 时等待一次远端 SendAccepted 才返回；
 * false（默认）在本地 lane 接纳后立即返回。
 * 该值只编码为 Send frame 的 acceptance_requested presence/value（false 时 wire 上 presence=false）。
 * traffic class 不在本类型携带，唯一真源是 Request::Traffic()。
 */
class SendOptions {
public:
    /// 本次 Send 的总期限；出站前由 Validate 要求有限且未过期。
    SendOptions& SetDeadline(bbt::coroutine::Deadline value) noexcept
    {
        m_deadline = value;
        return *this;
    }
    /// 是否等待远端 SendAccepted 才返回；默认不等待。
    SendOptions& RequireRemoteAcceptance(bool value = true) noexcept
    {
        m_require_remote_acceptance = value;
        return *this;
    }
    /// 当前期限。
    bbt::coroutine::Deadline Deadline() const noexcept
    {
        return m_deadline;
    }
    /// 是否需要远端接纳确认。
    bool RequiresRemoteAcceptance() const noexcept
    {
        return m_require_remote_acceptance;
    }

    /// @brief 出站边界校验：deadline 必须有限且未过期。
    ///
    /// 拒绝 epoch/已过期/远期 max 的 deadline，不让调用进入无界等待。
    /// @return 违规返回 InvalidArgument。
    result<void> Validate() const;

private:
    bbt::coroutine::Deadline m_deadline{bbt::coroutine::Deadline::max()};
    bool m_require_remote_acceptance = false;
};

/**
 * @brief Server 入站配置。
 *
 * setter 只存值，不校验；调用 Validate() 或通过 CoRpcServer::Create 边界时统一校验。
 * listen_address 须 Valid()，incoming_timeout 须有限且 ≤24h；其余字段须有限
 * 正数且不超 NetworkLimits 硬上限。
 */
class ServerOptions {
public:
    /// 监听地址（须 Valid()，否则 Validate 失败）。
    ServerOptions& SetListenAddress(bbt::infra::net::Address value)
    {
        m_listen_address = value;
        return *this;
    }
    /// 单帧字节上限（kMinFrameBytes..kMaxFrameBytes）。
    ServerOptions& SetMaxFrameBytes(std::size_t value)
    {
        m_max_frame_bytes = value;
        return *this;
    }
    /// 并发连接上限（≤kNetworkLimitsMaxConnections）。
    ServerOptions& SetMaxConnections(std::size_t value)
    {
        m_max_connections = value;
        return *this;
    }
    /// 在飞请求上限（≤kNetworkLimitsMaxInflight）。
    ServerOptions& SetMaxInflight(std::size_t value)
    {
        m_max_inflight = value;
        return *this;
    }
    /// 单请求 handler 可消费的字节上限（≤kNetworkLimitsMaxBodyBytes）。
    ServerOptions& SetMaxHandlerBytes(std::size_t value)
    {
        m_max_handler_bytes = value;
        return *this;
    }
    /// 入站请求期限（须有限且 ≤kNetworkLimitsMaxIncomingTimeout）。
    ServerOptions& SetIncomingTimeout(std::chrono::milliseconds value)
    {
        m_incoming_timeout = value;
        return *this;
    }

    /// 当前监听地址。
    const bbt::infra::net::Address& ListenAddress() const noexcept
    {
        return m_listen_address;
    }
    /// 单帧字节上限。
    std::size_t MaxFrameBytes() const noexcept
    {
        return m_max_frame_bytes;
    }
    /// 并发连接上限。
    std::size_t MaxConnections() const noexcept
    {
        return m_max_connections;
    }
    /// 在飞请求上限。
    std::size_t MaxInflight() const noexcept
    {
        return m_max_inflight;
    }
    /// 单请求 handler 字节上限。
    std::size_t MaxHandlerBytes() const noexcept
    {
        return m_max_handler_bytes;
    }
    /// 入站请求期限。
    std::chrono::milliseconds IncomingTimeout() const noexcept
    {
        return m_incoming_timeout;
    }

    /// @brief Create/Start 边界校验。
    ///
    /// 各项须为有限正数且不超 NetworkLimits 硬上限；incoming_timeout 须有限
    /// 且 ≤24h；listen_address 须 Valid()。
    /// @return 违规返回 InvalidArgument（message 指明首个越界字段）。
    result<void> Validate() const;

private:
    bbt::infra::net::Address m_listen_address;
    std::size_t m_max_frame_bytes = kDefaultMaxFrameBytes;
    std::size_t m_max_connections = 1024;
    std::size_t m_max_inflight = 4096;
    std::size_t m_max_handler_bytes = kNetworkLimitsMaxBodyBytes;
    std::chrono::milliseconds m_incoming_timeout{std::chrono::seconds{30}};
};

} // namespace bbt::infra::rpc
