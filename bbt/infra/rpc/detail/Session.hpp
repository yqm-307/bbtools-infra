#pragma once

// RPC 会话和接纳的私有入口。
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/infra/CloseOptions.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/Admission.hpp>
#include <bbt/infra/rpc/Stats.hpp>
#include <bbt/infra/rpc/detail/SessionTestAccess.hpp> // Existing private tests include Session.hpp.
#include <bbt/infra/rpc/SessionCodec.hpp>
#include <bbt/infra/rpc/Options.hpp>

namespace bbt::infra::net {
class Engine;
}

namespace bbt::infra::rpc {

class CoRpcClient;
class CoRpcServer;
struct SessionOwnerState;
struct ServerState;

namespace detail {

class ICoChannel;

/**
 * @brief admission 不透明实现的构造/收口入口。
 *
 * 只有这里能构造 `ReplyCompletion` / `AdmissionLease` / `AdmissionQueue`，
 * 所以公共头不必暴露 impl 指针，业务也拿不到有效对象。
 */
class AdmissionAccess {
public:
    static std::shared_ptr<AdmissionQueue> MakeQueue(std::shared_ptr<AdmissionQueueImpl> impl);
    static std::shared_ptr<ReplyCompletion> MakeReply(std::shared_ptr<ReplyCompletionImpl> impl);
    static std::shared_ptr<AdmissionLease> MakeLease(std::shared_ptr<AdmissionLeaseImpl> impl);

    /**
     * @brief 把 reply 标记为 terminal 并交出 impl；非首次/无效返回 nullptr。
     *
     * 给 reader 上的拒绝路径用：reader 不能挂起等写终态，只能把错误帧入队，
     * 写责任仍由 writer/Kill 在真实终态结算。
     */
    static std::shared_ptr<ReplyCompletionImpl> ClaimTerminal(ReplyCompletion& reply) noexcept;

    /**
     * @brief 把队列挂到 server 台账（只由 ServerStart 调一次）。
     */
    static void AttachQueue(const std::shared_ptr<AdmissionQueue>& queue, const std::shared_ptr<ServerState>& server);
};

/**
 * @brief 在一个注入的 `ICoChannel` 上跑一次入站握手。
 *
 * 握手步骤的正式入口：只依赖 `ICoChannel`，伪 channel 与真实 `TcpChannel`
 * 走同一条代码路径（生产 ServeInbound 也调它）。调用方须在受管协程内调用
 * （内部会 Read/Write 挂起），并自己持有 channel 的寿命——本函数不接管、
 * 不关闭连接。返回选定的 epoch，`out_decoder` 收编握手后同批余帧。
 */
result<std::uint64_t> InboundHandshakeOnChannel(
    detail::ICoChannel& channel,
    const ClientOptions& options,
    std::uint64_t proposal,
    std::uint64_t nonce,
    std::string& out_peer_identity,
    SessionFrameDecoder& out_decoder,
    bbt::coroutine::Deadline deadline);

result<std::uint64_t> OutboundHandshakeOnChannel(
    detail::ICoChannel& channel,
    const ClientOptions& options,
    std::uint64_t proposal,
    std::uint64_t nonce,
    std::string& out_peer_identity,
    SessionFrameDecoder& out_decoder,
    bbt::coroutine::Deadline deadline);

/**
 * @brief owner 逻辑封口：幂等、不可重开，发布首个关闭原因。
 *
 * 立即拒绝新 Call/Send，对在途 link RequestClose 并结算全部未终结调用
 * （「失败在途」恰好一次，Draining 下同样执行，与 Graceful 不共用位）；
 * 已提交但无可信终态的调用结算为 OutcomeUnknown。不等待在途调用：
 * 等待顺序与业务任务停止由用户控制。关闭原因不在内部出口附加，统一由
 * Call/Send 的调用方可见返回点读取 owner 原因后附加到 Error.details。
 */
void SessionOwnerSeal(const std::shared_ptr<SessionOwnerState>& owner, Error reason) noexcept;
bool SessionOwnerClosed(const SessionOwnerState& owner) noexcept;
bool SessionOwnerSealed(const SessionOwnerState& owner) noexcept;
std::optional<Error> SessionOwnerCloseReason(const SessionOwnerState& owner);

/**
 * @brief owner 统一关闭入口（实现唯一在 Session.cc）。
 *
 * 与公共 CloseOptions 同义：
 *   - Immediate：立即 Seal；仅在 wait_release 时再等物理收口。
 *   - Graceful：先置 Draining（与 Call/Send 接纳同门拒新）并发布首个原因，
 *     等 active_ops 与建立中记录全部归还后 Seal；到期返回 TimedOut 并保留
 *     责任，不自动升级。
 *
 * 等待经 owner->close_signal（CoWaiter）以绝对期限 + 每轮
 * min(deadline, now+50ms) 分片；调用方须在受管协程内调用，非协程上下文在
 * 无副作用前返回 InvalidContext。本 owner 已接纳调用的协程内调用（自等待）
 * 在副作用前返回 InvalidContext。
 */
result<void> SessionOwnerClose(const std::shared_ptr<SessionOwnerState>& owner, const CloseOptions& options);

/**
 * @brief owner 物理收口等待：已封口后等台账 link 租约与建立中记录归还。
 * 逐轮重驱各 link 的真实 Released 观察；到期返回 TimedOut 并保留责任。
 */
result<void> SessionOwnerWaitStopped(
    const std::shared_ptr<SessionOwnerState>& owner,
    bbt::coroutine::Deadline deadline);

/**
 * Call/Send 的接纳责任：与 Closed/Draining 同门（owner->mu）拒绝或计数；
 * 每个成功接纳必须恰好一次 EndOperation 归还。EndOperation 唤醒 Close
 * 等待者。
 */
bool SessionOwnerBeginOperation(const std::shared_ptr<SessionOwnerState>& owner, Error& closed_error);
void SessionOwnerEndOperation(const std::shared_ptr<SessionOwnerState>& owner) noexcept;

/**
 * @brief Server 生命周期入口（实现唯一在 Session.cc）。
 *
 * Seal 是立即逻辑封口并发布原因，不等待（RequestClose）。Close 与
 * SessionOwnerClose 同构：Immediate 立即 Seal；Graceful 先 StopAccepting +
 * Draining 拒新并发布首因，等 `DrainedLocked()` 逻辑排空（admission、队列和
 * 已登记 link 的写责任归零）后 Seal；到期返回 TimedOut 并保留责任。
 * WaitStopped 只等 `StoppedLocked()` 物理收口（listener/accept task、握手、
 * session 责任与 connection Released）。
 */
void ServerSeal(const std::shared_ptr<ServerState>& state, Error reason) noexcept;
bool ServerClosed(const ServerState& state) noexcept;
std::optional<Error> ServerCloseReason(const ServerState& state);
void ServerStopAccepting(const std::shared_ptr<ServerState>& state) noexcept;
result<void> ServerStart(const std::shared_ptr<ServerState>& state);
result<void> ServerClose(const std::shared_ptr<ServerState>& state, const CloseOptions& options);
result<void> ServerWaitStopped(const std::shared_ptr<ServerState>& state, bbt::coroutine::Deadline deadline);

/**
 * @brief Server 入站状态装配（实现唯一在 Session.cc）。
 *
 * 校验 options、Engine Idle、accept-handoff 容量，构造 ServerState 并把
 * 完成握手的入站 link 挂回同一 session_owner（inbound_server）。CoRpcServer
 * 的公共 Create 负责从 CoRpcClient 取 owner 并做 CoObject 包装。
 */
result<std::shared_ptr<ServerState>> ServerCreate(
    std::shared_ptr<net::Engine> engine,
    const std::shared_ptr<SessionOwnerState>& owner,
    AdmissionHandler admission_handler,
    ServerOptions options);

/**
 * @brief 出站调用的会话层入口（实现在 detail/RpcSessionOwner.cc）。
 *
 * CoRpcClient::Call/Send 的薄包装经它们落到 CallImpl/SendImpl；关闭原因的
 * 附加（AttachOwnerCloseReason）已在内部完成。
 */
result<Response> SessionOwnerCall(
    const std::shared_ptr<SessionOwnerState>& owner,
    std::string_view target,
    Request request,
    const CallOptions& options);
result<void> SessionOwnerSend(
    const std::shared_ptr<SessionOwnerState>& owner,
    std::string_view target,
    Request message,
    const SendOptions& options);

/**
 * @brief owner 汇总观测快照（实现在 detail/RpcSessionOwner.cc）。
 */
ClientStats SessionOwnerStats(const std::shared_ptr<SessionOwnerState>& owner) noexcept;

// clang-format off
}
// clang-format on
// clang-format off
}
// clang-format on
