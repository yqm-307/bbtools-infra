#pragma once
// RPC 入站 admission 公共类型（ReplyCompletion /
// AdmissionLease / AdmissionItem / AdmissionHandler / AdmissionQueue）。
//
// 一次入站帧的生命周期，谁在什么时候负责什么：
//
//   reader（每 session 一个）
//     └─ AdmissionQueue::Admit(item)          满 → Overloaded，立即返回
//          └─ FIFO worker（Create 时登记）
//               └─ AdmissionHandler(item)     同步校验 + 有界转移，不挂起
//                    ├─ 成功：item 归 framework
//                    │    ├─ Call：业务协程 Complete/Fail → 真实写终态 → lease->Release()
//                    │    └─ Send：infra 写一次 SendAccepted（若要求），lease 由 infra 收口
//                    └─ 失败：infra 发布一次错误终态（Call 错误 / SendAccepted 错误）
//
// 两条硬约束：
//   * reader 只有一个，它绝不挂起等写终态——写背压会把后续 Response/System
//     帧一起堵住，所以拒绝路径只把错误帧入队，不等它写完。
//   * 一个 item 的终态只发布一次。reply 的 terminal 位与 lease 的 released 位
//     各自保证这一点，重复调用返回错误而不是重发。
//
// 域：Complete/Fail 是 bbt_co_async，只能在受管协程内 bbt_co_await；Release
// 任意域可用。Admit 与 AdmissionHandler 都不可挂起。
//
// 契约来源：docs/decisions/0014-app-session.md §2.3/§4.2。

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>

namespace bbt::infra::rpc {

namespace detail {
// 不透明实现：定义在 detail/RpcSessionState.hpp，业务不能直接构造有效对象。
struct ReplyCompletionImpl;
struct AdmissionLeaseImpl;
struct AdmissionQueueImpl;
class AdmissionAccess; // 内部接缝的唯一入口，见 detail/Session.hpp
} // namespace detail

/**
 * @brief Call 与带确认 Send 的异步完成凭据。
 *
 * `Complete` 按 kind 生成 RESPONSE 或 SEND_ACCEPTED 终态，`Fail` 生成错误
 * 终态；两者都要等到 socket 真实写终态才返回，因此「Complete 成功」等价于
 * 「对端可能已经收到」。`IsTerminal()` 只观测是否已发布终态，不是成功凭据。
 */
class ReplyCompletion {
public:
    ReplyCompletion() = default;

    /// @brief 发布成功终态（按 kind 生成 RESPONSE / SEND_ACCEPTED）。
    ///
    /// 等到 socket 真实写终态才返回：成功等价于「对端可能已经收到」。
    /// 仅受管协程内可调用；非首次调用或无效凭据返回 InvalidArgument。
    bbt_co_async(result<void>) Complete(Response response);
    /// @brief 发布错误终态（Call 错误 Response / SendAccepted 错误）。
    ///
    /// 与 Complete 同域、同等待语义；同样只有第一次调用生效。
    bbt_co_async(result<void>) Fail(Error error);
    /// @brief 是否已发布过终态（观测事实，不是成功凭据）。
    bool IsTerminal() const noexcept;

    /// @brief 本凭据是 Send 的远端接纳确认（而非 Call 回复）。
    ///
    /// true 时 admission 成功后由 infra worker 写一次 SendAccepted；false 时
    /// 回复归 framework 发布。业务不需要消费本判别。
    bool ForSendAcceptance() const noexcept;

private:
    friend class detail::AdmissionAccess;

    explicit ReplyCompletion(std::shared_ptr<detail::ReplyCompletionImpl> impl) : m_impl(std::move(impl))
    {
    }

    /// @brief 供 detail::AdmissionAccess 使用：标记 terminal 并交出 impl。
    ///
    /// 语义就是「唯一一次成功」——非首次或无效 impl 返回 nullptr。放在私有
    /// 段是为了让公共面只有 Complete/Fail 两个终态入口。
    std::shared_ptr<detail::ReplyCompletionImpl> MarkTerminalForReject() noexcept;

    std::shared_ptr<detail::ReplyCompletionImpl> m_impl;
};

/**
 * @brief 入站责任 lease：一个 item 从「已接纳」到「责任结清」的凭据。
 *
 * Call 在 reply 终态后由 framework Release；Send 由 infra 在接纳并写完所需
 * 确认后自行收口。只有第一次 Release 成功，重复调用返回 false。
 */
class AdmissionLease {
public:
    AdmissionLease() = default;

    /// @brief 结清本 item 的入站责任；只有第一次返回 true。
    ///
    /// 任意域可用（不要求协程）。重复调用返回 false，不重复扣减。
    bool Release() noexcept;
    /// @brief 是否已结清（观测事实）。
    bool IsReleased() const noexcept;

private:
    friend class detail::AdmissionAccess;

    explicit AdmissionLease(std::shared_ptr<detail::AdmissionLeaseImpl> impl) : m_impl(std::move(impl))
    {
    }

    std::shared_ptr<detail::AdmissionLeaseImpl> m_impl;
};

/**
 * @brief 唯一按序接纳入口的载荷。
 *
 * `request` 是 owning 的，Context（deadline local-min / peer_identity /
 * peer_principal）已由 infra 在交付前填好。`reply`/`lease` 由 session 为每项
 * 创建，业务不能自己造。
 */
struct AdmissionItem {
    Request request;                        ///< owning 请求，Context 已由 infra 填好
    std::shared_ptr<ReplyCompletion> reply; ///< 本项的回复终态结算器
    std::shared_ptr<AdmissionLease> lease;  ///< 本项的在飞名额租约
};

/**
 * @brief framework 侧接纳回调：只做同步校验和有界转移。
 *
 * 返回成功之前必须已经把 owning item 转进自己的队列；返回错误表示未接纳，
 * 由 infra 依 ReplyMode / acceptance_requested 生成一次 Call 错误或一次
 * SendAccepted 错误。实现不得挂起、不得调用普通业务 handler。
 */
using AdmissionHandler = std::function<result<void>(AdmissionItem&&)>;

/**
 * @brief 单 FIFO admission 队列：Call 与 Send 共用的唯一按序接纳入口。
 *
 * `Admit` 不挂起：队列满立即 `Overloaded`，封口后 `Closed`。内部一个 FIFO
 * worker 在受管协程里按序调用 `AdmissionHandler`，把 item 交给 framework。
 * 队列长度与字节数都按 `item.request.Traffic()` 分 lane 记账，方便按业务
 * 通道观察；总量上限才是接纳判据。
 */
class AdmissionQueue {
public:
    /**
     * @brief 建立队列并登记其 FIFO worker。
     *
     * @param max_items 条目上限，必须 > 0。
     * @param max_bytes 字节上限，必须 > 0。
     * @param handler   接纳回调，必须非空。
     * @return 队列；参数非法为 InvalidArgument，worker 登记失败为
     *         RuntimeUnavailable（此时没有留下常驻任务）。
     */
    static result<std::shared_ptr<AdmissionQueue>> Create(std::size_t max_items, std::size_t max_bytes,
                                                          AdmissionHandler handler);

    /// @brief 不可挂起；满 → Overloaded，封口后 → Closed。
    result<void> Admit(AdmissionItem item);
    /// @brief 封口：拒绝后续 Admit，worker 退出；已接纳的 item 不受影响。
    void Close() noexcept;
    /// @brief 是否已封口（观测事实）。
    bool IsClosed() const noexcept;

    // 观察面：计数读都在队列自己的锁内完成。ServerV2Stats/Close 只经这里拿
    // 快照，不直读 impl 字段——server 台账锁与队列锁因此保持单向顺序。
    // per-lane 重载供分 lane 快照使用，lane 越界按 0 计。
    /// @brief 当前队列条目总数（gauge，单次取锁）。
    std::size_t Queued() const noexcept;
    /// @brief 当前队列字节总数（gauge，单次取锁）。
    std::size_t BytesQueued() const noexcept;
    /// @brief 指定 lane 的当前排队条目数（gauge；越界 lane 按 0 计）。
    std::uint64_t Queued(std::size_t lane) const noexcept;
    /// @brief 指定 lane 的当前排队字节数（gauge；越界 lane 按 0 计）。
    ///
    /// 与 Queued(lane) 是两次独立取锁：两者不构成同一时刻的一致视图。
    std::uint64_t BytesQueued(std::size_t lane) const noexcept;

private:
    friend class detail::AdmissionAccess;

    explicit AdmissionQueue(std::shared_ptr<detail::AdmissionQueueImpl> impl) : m_impl(std::move(impl))
    {
    }

    std::shared_ptr<detail::AdmissionQueueImpl> m_impl;
};

} // namespace bbt::infra::rpc
