#pragma once
// bbtools-infra v2：RPC 入站 admission 公共类型（ReplyCompletion /
// AdmissionLease / AdmissionItem / AdmissionHandler / AdmissionQueue）。
// 契约来源：docs/decisions/0014-app-session.md §2.3/§4.2。
//
// 职责边界：AdmissionItem 是 Call 与 Send 共用的唯一按序接纳入口载荷；
// framework 的 AdmissionHandler 只做同步校验和有界转移（不挂起、不调普通
// 业务 handler）。infra 为每项创建 ReplyCompletion/AdmissionLease——业务
// 不能构造这两个类型（实现类型是不透明 detail，仅 infra TU 有定义）。
//
// 责任终态（ADR §2.3/§4.2）：
//   - Call：framework 业务协程完成后 bbt_co_await reply->Complete/Fail
//     （异步回复写入终态），随后 lease->Release()；两终态各只发布一次。
//   - Send：mailbox/admission 接纳成功后 infra 自行结束投递责任并释放
//     lease；RequireRemoteAcceptance 时 SendAccepted 由 infra 在接纳边界
//     写一次。
//   - 关联字段（request_id/service/method/request_schema/response_schema）
//     由 infra 在 Complete 边界校验，不一致返回 ProtocolError 并把回复
//     结算为 ProtocolError 终态（不让 handler 改写另一个请求的回复）。
//
// 域约束：Complete/Fail 是 bbt_co_async——只能在受管协程内 bbt_co_await；
// Release 任意域可用、仅第一次成功。AdmissionQueue::Admit 不可挂起。

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
// 不透明实现类型：仅 infra Session TU 提供定义，业务无法构造。
struct ReplyCompletionImpl;
struct AdmissionLeaseImpl;
struct AdmissionQueueImpl;
} // namespace detail

// Call/带确认 Send 的异步完成凭据。Complete 按 kind 生成 RESPONSE 或
// SEND_ACCEPTED 终态；Fail 生成错误终态。IsTerminal 观测是否已发布终态。
// 对无效（空）对象调用返回 InvalidArgument，IsTerminal()=true。
class ReplyCompletion {
public:
    ReplyCompletion() = default;
    // 实现类型不透明：公共构造入口存在但业务无法构造有效 impl（仅 infra
    // TU 有 detail::ReplyCompletionImpl 定义）。
    explicit ReplyCompletion(std::shared_ptr<detail::ReplyCompletionImpl> impl)
        : m_impl(std::move(impl)) {}

    bbt_co_async(result<void>) Complete(Response response);
    bbt_co_async(result<void>) Fail(Error error);
    bool IsTerminal() const noexcept;
    // infra 内部判别：本 completion 是否为 Send 的远端接纳确认（true →
    // admission 成功后由 infra worker 写一次 SendAccepted；false →
    // Call 回复，归 framework 发布）。业务不消费本判别。
    bool ForSendAcceptance() const noexcept;

    // infra 内部接缝（P2-5）：reader 上的 admission 拒绝路径不能挂起等
    // 写终态。把 reply 标记为 terminal（唯一一次成功）并返回 impl 供
    // infra 构造错误终态帧直接入队——写责任仍由 infra 在真实终态结算，
    // 业务不应消费本接缝。非首次/无效 impl 返回 nullptr。
    std::shared_ptr<detail::ReplyCompletionImpl> MarkTerminalForReject() noexcept;

private:
    std::shared_ptr<detail::ReplyCompletionImpl> m_impl;
};

// 入站责任 lease：framework 在 reply 终态后 Release。仅第一次调用成功。
// infra 在 Send 接纳+确认写终态后自行收口本 lease（不经业务）。
class AdmissionLease {
public:
    AdmissionLease() = default;
    explicit AdmissionLease(std::shared_ptr<detail::AdmissionLeaseImpl> impl)
        : m_impl(std::move(impl)) {}

    bool Release() noexcept;              // 仅第一次调用成功
    bool IsReleased() const noexcept;

private:
    std::shared_ptr<detail::AdmissionLeaseImpl> m_impl;
};

// 唯一按序接纳入口载荷：Request 为 owning（Context 已由 infra 在交付
// admission 前填充：deadline local-min / peer_identity / peer_principal），
// reply/lease 由 session 为每项创建，业务不能构造。
struct AdmissionItem {
    Request                          request;
    std::shared_ptr<ReplyCompletion> reply;
    std::shared_ptr<AdmissionLease>  lease;
};

// 只做同步校验和有界转移；不得挂起或调用普通业务 handler。返回成功前必须
// 完成 owning item 的框架队列转移；返回错误表示未接纳，由 infra 依
// ReplyMode/acceptance_requested 生成 Call 错误或一次性 SendAccepted 错误。
using AdmissionHandler = std::function<result<void>(AdmissionItem&&)>;

// 单 FIFO admission 队列：唯一按序接纳入口。Admit 不可挂起——队列满立即
// Overloaded。一个内部 FIFO worker 按序调用 AdmissionHandler 并把 item
// 转移给 framework；worker 在受管协程内运行（Create 时登记）。
class AdmissionQueue {
public:
    // max_items/max_bytes > 0 才有界；handler 必须非空。worker 登记失败
    // 返回 RuntimeUnavailable。
    static result<std::shared_ptr<AdmissionQueue>> Create(
        std::size_t max_items, std::size_t max_bytes,
        AdmissionHandler handler);

    // 不可挂起；按 item.request.Traffic() 记账（System 由 infra 内部使用，
    // 业务 Call/Send 为 Service/Actor）。满 → Overloaded；Close 后 → Closed。
    result<void> Admit(AdmissionItem item);
    void Close() noexcept;
    bool IsClosed() const noexcept;

    // infra 内部接缝：queue impl 仅 infra TU 有定义（ServerV2State 快照用）。
    const std::shared_ptr<detail::AdmissionQueueImpl>& Impl() const noexcept {
        return m_impl;
    }

    // 观察快照（gauge）。唯一同步观察面：计数读全部在 impl->mu 下完成，
    // ServerV2Stats/Drain 只能经 Queued/BytesQueued 拿同步快照，不得直读
    // impl 字段（queue 计数与 server 台账锁序解耦）。per-lane 重载供
    // ServerV2Stats 分 lane 快照；lane 越界按 0 计。
    std::size_t Queued() const noexcept;
    std::size_t BytesQueued() const noexcept;
    std::uint64_t Queued(std::size_t lane) const noexcept;
    std::uint64_t BytesQueued(std::size_t lane) const noexcept;

private:
    explicit AdmissionQueue(std::shared_ptr<detail::AdmissionQueueImpl> impl)
        : m_impl(std::move(impl)) {}
    std::shared_ptr<detail::AdmissionQueueImpl> m_impl;
};

} // namespace bbt::infra::rpc
