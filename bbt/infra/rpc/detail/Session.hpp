#pragma once
// RPC v2 session 内部接缝（bbt/infra/rpc 实现 TU 专用，非公开头）。
//
// CoRpcClient/CoRpcServer 的生命周期入口与各实现 TU 中的不透明状态跨
// TU 共享。这里只放这条边界真正需要的东西——不透明实现的构造入口、
// 实例级观察钩子、台账快照，以及 v2 生命周期入口。任何「只是把另一处
// 调用转发一次」的包装都不放在这里。
//
// 会话层状态归属见 detail/RpcSessionState.hpp；Channel/帧协议接缝见
// detail/RpcChannel.hpp 与 detail/RpcFrameProtocol.hpp。

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/Admission.hpp>
#include <bbt/infra/rpc/Stats.hpp>
#include <bbt/infra/rpc/SessionCodec.hpp>
#include <bbt/infra/rpc/Options.hpp>

namespace bbt::infra::net {
class Engine;
}

namespace bbt::infra::rpc {

class CoRpcClient;
class CoRpcServer;
struct SessionOwnerState;
struct ServerV2State;

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

    /// @brief 把 reply 标记为 terminal 并交出 impl；非首次/无效返回 nullptr。
    ///
    /// 给 reader 上的拒绝路径用：reader 不能挂起等写终态，只能把错误帧入队，
    /// 写责任仍由 writer/Kill 在真实终态结算。
    static std::shared_ptr<ReplyCompletionImpl> ClaimTerminal(ReplyCompletion& reply) noexcept;

    /// @brief 把队列挂到 server 台账（只由 ServerV2Start 调一次）。
    static void AttachQueue(const std::shared_ptr<AdmissionQueue>& queue, const std::shared_ptr<ServerV2State>& server);
};

// 受控检查点控制器 SessionHooks 是公开类型（bbt/infra/rpc/Hooks.hpp），
// 只经 CoRpcClient::Hooks() 取得；本头不重复声明。

/**
 * @brief v2 server 台账快照，三项在同一把 server 锁内取样。
 *
 * `writes_outstanding` 含 dead link 的残留（writer 手中正在写的条目不属于
 * 「排队丢弃」口径），所以收口断言不能只看 links。
 */
struct ServerV2Ledger {
    std::uint64_t links{0};
    std::uint64_t handshakes{0};
    std::uint64_t writes_outstanding{0};
};

/// @brief CoRpcServer 句柄版本的台账快照。
ServerV2Ledger ServerV2LedgerForTest(const CoRpcServer& server) noexcept;

/// @brief ServerV2State 直参版本的台账快照（供 CoRpcServer.cc 的句柄版本转调）。
ServerV2Ledger ServerV2LedgerOf(const std::shared_ptr<ServerV2State>& v2) noexcept;

/**
 * @brief 在一个注入的 `ICoChannel` 上跑一次入站握手。
 *
 * 握手步骤的正式入口：只依赖 `ICoChannel`，伪 channel 与真实 `TcpChannel`
 * 走同一条代码路径（生产 ServeInbound 也调它）。调用方须在受管协程内调用
 * （内部会 Read/Write 挂起），并自己持有 channel 的寿命——本函数不接管、
 * 不关闭连接。返回选定的 epoch，`out_decoder` 收编握手后同批余帧。
 */
result<std::uint64_t> InboundHandshakeOnChannel(detail::ICoChannel& channel, const ClientOptions& options,
                                                std::uint64_t proposal, std::uint64_t nonce,
                                                std::string& out_peer_identity, SessionFrameDecoder& out_decoder,
                                                bbt::coroutine::Deadline deadline);

/**
 * @brief v2 owner 逻辑封口：幂等、不可重开，发布首个关闭原因。
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
 * @brief v2 owner 统一关闭入口（实现唯一在 Session.cc）。
 *
 *   - Immediate：立即 Seal 并返回 ok。
 *   - Timeout：先 Seal，再在 timeout_ms 预算内等 owner 台账物理收口
 *     （全部 link 映射与建立中记录归还）；超时返回 TimedOut。
 *   - Graceful：先置 Draining（与 Call/Send 接纳同门拒新、不发布原因），
 *     等 active_ops 与建立中记录全部归还后 Seal；timeout_ms<0 无限等待，
 *     有限期限超时返回 TimedOut 并立即 Seal。等待期间被其它 Close 打断
 *     时返回已发布的首个关闭原因；重复 Graceful 且未排空返回 Overloaded。
 *
 * 等待经 owner->close_signal（CoWaiter）分片，调用方须在受管协程内调用
 * 等待模式；非协程上下文返回 InvalidContext（Seal 已执行的按序返回）。
 */
result<void> SessionOwnerClose(const std::shared_ptr<SessionOwnerState>& owner, CloseFlag flag, std::int64_t timeout_ms,
                               Error reason);

// Call/Send 的接纳责任：与 Closed/Draining 同门（owner->mu）拒绝或计数；
// 每个成功接纳必须恰好一次 EndOperation 归还。EndOperation 唤醒 Close
// 等待者。
bool SessionOwnerBeginOperation(const std::shared_ptr<SessionOwnerState>& owner, Error& closed_error);
void SessionOwnerEndOperation(const std::shared_ptr<SessionOwnerState>& owner) noexcept;

/**
 * @brief v2 Server 生命周期入口（实现唯一在 Session.cc）。
 *
 * Seal 是立即逻辑封口并发布原因，不等待。Close 三种模式与
 * SessionOwnerClose 同构：Immediate 立即 Seal；Timeout 先 Seal 再按
 * `StoppedLocked()` 有界等待物理收口（listener/accept task、握手、
 * session 责任与 connection Released）；Graceful 先 StopAccepting +
 * Draining 拒新，等 `DrainedLocked()` 逻辑排空（admission、队列和已登记
 * link 的写责任归零）后 Seal，timeout_ms<0 无限等待。
 */
void ServerV2Seal(const std::shared_ptr<ServerV2State>& v2, Error reason) noexcept;
bool ServerV2Closed(const ServerV2State& v2) noexcept;
std::optional<Error> ServerV2CloseReason(const ServerV2State& v2);
void ServerV2StopAccepting(const std::shared_ptr<ServerV2State>& v2) noexcept;
result<void> ServerV2Start(const std::shared_ptr<ServerV2State>& v2);
result<void> ServerV2Close(const std::shared_ptr<ServerV2State>& v2, CloseFlag flag, std::int64_t timeout_ms,
                           Error reason);

/**
 * @brief Server 入站状态装配（实现唯一在 Session.cc）。
 *
 * 校验 options、Engine Idle、accept-handoff 容量，构造 ServerV2State 并把
 * 完成握手的入站 link 挂回同一 session_owner（inbound_server）。CoRpcServer
 * 的公共 Create 负责从 CoRpcClient 取 owner 并做 CoObject 包装。
 */
result<std::shared_ptr<ServerV2State>> ServerV2Create(std::shared_ptr<net::Engine> engine,
                                                      const std::shared_ptr<SessionOwnerState>& owner,
                                                      AdmissionHandler admission_handler, ServerOptions options);

/**
 * @brief 出站调用的会话层入口（实现唯一在 detail/RpcSession.cc）。
 *
 * CoRpcClient::Call/Send 的薄包装经它们落到 CallImpl/SendImpl；关闭原因的
 * 附加（AttachOwnerCloseReason）已在内部完成。
 */
result<Response> SessionOwnerCall(const std::shared_ptr<SessionOwnerState>& owner, std::string_view target,
                                  Request request, const CallOptions& options);
result<void> SessionOwnerSend(const std::shared_ptr<SessionOwnerState>& owner, std::string_view target, Request message,
                              const SendOptions& options);
/// @brief owner 汇总观测快照（实现唯一在 detail/RpcSession.cc）。
ClientStats SessionOwnerStats(const std::shared_ptr<SessionOwnerState>& owner) noexcept;

} // namespace detail
} // namespace bbt::infra::rpc
