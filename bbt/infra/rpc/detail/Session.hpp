#pragma once
// RPC v2 session 内部接缝（bbt/infra/rpc 实现 TU 专用，非公开头）。
//
// 存在的理由只有一个：v1 的 Client/Server 方法体在 Client.cc/Server.cc，而 v2
// 的状态定义与实现都在 Session.cc。这里只放这条跨 TU 边界真正需要的东西——
// 不透明实现的构造入口、实例级观察钩子、台账快照，以及 v2 生命周期入口。
// 任何「只是把另一处调用转发一次」的包装都不放在这里。

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/rpc/Admission.hpp>
#include <bbt/infra/rpc/Codec.hpp>
#include <bbt/infra/rpc/Options.hpp>

namespace bbt::infra::rpc {

class Client;
class Server;
struct SessionOwnerState;
struct ServerV2State;

namespace detail {

/**
 * @brief admission 不透明实现的构造/收口入口。
 *
 * 只有这里能构造 `ReplyCompletion` / `AdmissionLease` / `AdmissionQueue`，
 * 所以公共头不必暴露 impl 指针，业务也拿不到有效对象。
 */
class AdmissionAccess {
public:
    static std::shared_ptr<AdmissionQueue> MakeQueue(
        std::shared_ptr<AdmissionQueueImpl> impl);
    static std::shared_ptr<ReplyCompletion> MakeReply(
        std::shared_ptr<ReplyCompletionImpl> impl);
    static std::shared_ptr<AdmissionLease> MakeLease(
        std::shared_ptr<AdmissionLeaseImpl> impl);

    /// @brief 把 reply 标记为 terminal 并交出 impl；非首次/无效返回 nullptr。
    ///
    /// 给 reader 上的拒绝路径用：reader 不能挂起等写终态，只能把错误帧入队，
    /// 写责任仍由 writer/Kill 在真实终态结算。
    static std::shared_ptr<ReplyCompletionImpl> ClaimTerminal(
        ReplyCompletion& reply) noexcept;

    /// @brief 把队列挂到 server 台账（只由 ServerV2Start 调一次）。
    static void AttachQueue(const std::shared_ptr<AdmissionQueue>& queue,
                            const std::shared_ptr<ServerV2State>& server);
};

// 受控检查点控制器 SessionHooks 是公开类型（bbt/infra/rpc/Hooks.hpp），
// 只经 Client::Hooks() 取得；本头不重复声明。

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

/// @brief Server 句柄版本的台账快照（v1 装配返回全零）。
ServerV2Ledger ServerV2LedgerForTest(const Server& server) noexcept;

/// @brief ServerV2State 直参版本的台账快照（供 Server.cc 的句柄版本转调）。
ServerV2Ledger ServerV2LedgerOf(
    const std::shared_ptr<ServerV2State>& v2) noexcept;

/**
 * @brief 在一个注入的 `ICoConnection` 上跑一次入站握手。
 *
 * 握手步骤的正式入口：只依赖 `ICoConnection`，伪连接与真实
 * `CoTcpConnection` 走同一条代码路径（生产 ServeInbound 也调它）。调用方须
 * 在受管协程内调用（内部会 Read/Write 挂起），并自己持有 conn 的寿命——本
 * 函数不接管、不关闭连接。返回选定的 epoch，`out_decoder` 收编握手后同批
 * 余帧。
 */
result<std::uint64_t> InboundHandshakeOnConnection(
    net::ICoConnection& conn, const ClientOptions& options,
    std::uint64_t proposal, std::uint64_t nonce,
    std::string& out_peer_identity, SessionFrameDecoder& out_decoder,
    bbt::coroutine::Deadline deadline);

/**
 * @brief v2 owner 逻辑封口：幂等、不可重开，发布首个关闭原因。
 *
 * 立即拒绝新 Call/Send，对在途 link RequestClose 并结算全部未终结调用
 * （「失败在途」恰好一次，Draining 下同样执行，与 Shutdown 不共用位）；
 * 已提交但无可信终态的调用结算为 OutcomeUnknown。不等待在途调用：
 * 等待顺序与业务任务停止由用户控制。关闭原因不在内部出口附加，统一由
 * Call/Send 的调用方可见返回点读取 owner 原因后附加到 Error.details。
 */
void SessionOwnerSeal(const std::shared_ptr<SessionOwnerState>& owner,
                      Error reason) noexcept;
bool SessionOwnerClosed(const SessionOwnerState& owner) noexcept;
std::optional<Error> SessionOwnerCloseReason(const SessionOwnerState& owner);

/**
 * @brief v2 owner 置 Draining（拒新）+ 等 active_ops 归零。
 *
 * 先与 Call/Send 的接纳同门拒新（置 draining、不占用 closed 位、不发布
 * 关闭原因），再等已接纳调用全部归还责任；不要求连接物理 Released。
 * 有限期限超时 TimedOut 且保持拒新；等待期间被 Close 且仍有在途返回其
 * 关闭原因，已排空先胜返回 ok；重复调用仍 Draining 且有在途返回
 * Overloaded（Busy）；已 Closed 返回首个关闭原因。
 */
result<void> SessionOwnerShutdown(
    const std::shared_ptr<SessionOwnerState>& owner,
    bbt::coroutine::Deadline deadline);

// Call/Send 的接纳责任：与 Closed/Draining 同门（owner->mu）拒绝或计数；
// 每个成功接纳必须恰好一次 EndOperation 归还。EndOperation 唤醒 Shutdown
// 等待者。
bool SessionOwnerBeginOperation(const std::shared_ptr<SessionOwnerState>& owner,
                                Error& closed_error);
void SessionOwnerEndOperation(
    const std::shared_ptr<SessionOwnerState>& owner) noexcept;

/**
 * @brief v2 Server 生命周期入口（实现唯一在 Session.cc）。
 *
 * Seal 是立即逻辑封口并发布原因，不等待。Shutdown 封口后等
 * `DrainedLocked()` 的逻辑排空：admission、队列和已登记 link 的写责任归零；
 * WaitStopped 等待 `StoppedLocked()` 的物理收口：listener/accept task、握手、
 * session 责任和 connection Released，并同时满足逻辑排空。两者不是同一收口
 * 谓词，Shutdown 成功不代表所有连接已经物理 Released。
 */
void ServerV2Seal(const std::shared_ptr<ServerV2State>& v2, Error reason) noexcept;
bool ServerV2Closed(const ServerV2State& v2) noexcept;
std::optional<Error> ServerV2CloseReason(const ServerV2State& v2);
void ServerV2StopAccepting(const std::shared_ptr<ServerV2State>& v2) noexcept;
result<void> ServerV2Start(const std::shared_ptr<ServerV2State>& v2);
result<void> ServerV2Shutdown(const std::shared_ptr<ServerV2State>& v2,
                              bbt::coroutine::Deadline deadline);
result<void> ServerV2Drain(const std::shared_ptr<ServerV2State>& v2,
                           bbt::coroutine::Deadline deadline);
result<void> ServerV2WaitStopped(const std::shared_ptr<ServerV2State>& v2,
                                 bbt::coroutine::Deadline deadline);

} // namespace detail
} // namespace bbt::infra::rpc
