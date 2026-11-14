/**
 * RPC v2 单 link 会话实现：SessionCore 的收发缓存、拆包、pending、
 * 握手进度、lane 顺序与关闭状态全部归本 TU；Channel（detail/RpcChannel.hpp）
 * 只做无状态字节转发，帧协议（detail/RpcFrameProtocol.hpp）只做纯编解码。
 * owner/server 生命周期入口仍在 Session.cc；admission 公共类型在
 * detail/AdmissionImpl.cc；检查点在 detail/SessionHooks.cc。
 *
 * SessionFrame wire 编解码复用 SessionCodec.hpp
 * （profile 2 + u32be 分帧 + decoder 分配前硬上限）。
 */
#include <bbt/infra/rpc/CoRpcClient.hpp>
#include <bbt/infra/rpc/CoRpcServer.hpp>
#include <bbt/infra/rpc/detail/Session.hpp>
#include <bbt/infra/rpc/detail/RpcSessionState.hpp>
#include <bbt/infra/rpc/detail/RpcFrameProtocol.hpp>

#include <algorithm>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

namespace bbt::infra::rpc {

using namespace std::chrono_literals;
using bbt::coroutine::WaitStatus;
using bbt::coroutine::sync::CoWaiter;

namespace {

std::uint64_t NextEpoch(SessionOwnerState& owner)
{
    std::lock_guard<std::mutex> lock(owner.mu);
    if (++owner.epoch_counter == 0) {
        ++owner.epoch_counter;
    }
    return owner.epoch_counter;
}

std::uint64_t NextNonce(SessionOwnerState& owner)
{
    std::lock_guard<std::mutex> lock(owner.mu);
    std::uint64_t nonce = 0;
    do {
        nonce = owner.random();
    } while (nonce == 0);
    return nonce;
}

// clang-format off
}
// clang-format on

namespace {

const char* HookPointLabel(HookPoint point) noexcept
{
    switch (point) {
    case HookPoint::BeforeWrite:
        return "before_write";
    case HookPoint::OutboundBeforeHandshake:
        return "outbound_handshake";
    case HookPoint::InboundBeforeHandshake:
        return "inbound_handshake";
    case HookPoint::InboundBeforePublish:
        return "inbound_publish";
    }
    return "unknown";
}

/**
 * 把库内部固定标签转成可见错误：四个调用点都只处理 result，各自完成
 * pending/记录/额度/连接的收尾，异常不越过各自的责任边界。
 */
result<void> RunHookCheckpoint(SessionHooks& hooks, HookPoint point, const HookGate& gate)
{
    try {
        return detail::SessionHookAccess::Checkpoint(hooks, point, gate);
    } catch (const HookInjectedFailure&) {
        return ErrV(
            ErrorCode::InternalError, std::string("hook checkpoint injected failure: ") + HookPointLabel(point));
    }
}

// clang-format off
}
// clang-format on

namespace {

/**
 * Outbound lane helpers
 */
std::size_t LaneCap(const ClientOptions& options, TrafficClass traffic)
{
    switch (traffic) {
    case TrafficClass::System:
        return options.SystemQueueBytes();
    case TrafficClass::Service:
        return options.ServiceQueueBytes();
    case TrafficClass::Actor:
        return options.ActorQueueBytes();
    }
    return options.ServiceQueueBytes();
}

// clang-format off
}
// clang-format on

/**
 * SessionCore 方法
 * 会话任务提交统一入口：每份协程任务计一份退出责任，fn 真实返回才结算。
 * RegistCoroutineTask 只保证任务被接管，被接管任务必运行到 fn 返回（含
 * 异常路径），责任随 fn 边界结算——不会把「已提交未运行」误报成完成。
 */
bool SessionCore::Submit(std::size_t stack, std::function<void()> fn)
{
    auto session = shared_from_this();
    responsibilities.fetch_add(1);
    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        stack,
        [session, fn = std::move(fn)]() mutable {
            try {
                fn();
            } catch (...) {
                session->Kill({});
            }
            session->ExitDone();
        },
        registered);
    if (!registered) {
        responsibilities.fetch_sub(1);
    }
    return registered;
}

result<void> SessionCore::Enqueue(OutItem item)
{
    const auto lane = LaneIndex(item.lane);
    {
        std::lock_guard<std::mutex> lock(out_mu);
        if (out_sealed || dead.load()) {
            return ErrV(ErrorCode::Closed, "session sealed");
        }
        auto& stats = out_lanes[lane];
        const auto cap = LaneCap(owner->options, item.lane);
        const auto bytes = item.bytes.size();
        if (stats.bytes_queued + bytes > cap) {
            ++stats.dropped;

            /**
             * 满队列隔离：只拒本 lane 本条，不阻其他 lane。
             */
            return ErrV(ErrorCode::Overloaded, "lane queue full");
        }
        out_q[lane].push_back(std::move(item));
        ++stats.queued;
        stats.bytes_queued += bytes;
        writes_outstanding.fetch_add(1);
    }
    if (out_signal) {
        out_signal->Notify();
    }
    return result<void>::ok();
}

SessionCore::OutItem SessionCore::TakeQueuedLocked(std::size_t lane)
{
    auto item = std::move(out_q[lane].front());
    out_q[lane].pop_front();
    auto& stats = out_lanes[lane];
    --stats.queued;
    stats.bytes_queued -= item.bytes.size();
    return item;
}

/**
 * 一条 OutItem 到达真实写终态（写出/写失败/被 Kill 丢弃）后的唯一
 * 结算点：向回复凭据发布 outcome（settled 保证唯一），writes_outstanding
 * 恰好 -1，归零时唤醒 server 收口观察。
 */
void SessionCore::NoteWriteSettled(OutItem& item, result<void> outcome)
{
    if (item.settle && !item.settle->settled.exchange(true)) {
        item.settle->outcome = std::move(outcome);
        item.settle->waiter->Notify();
    }
    if (writes_outstanding.fetch_sub(1) == 1) {
        if (auto server = this->server.lock()) {
            std::lock_guard<std::mutex> lock(server->mu);
            server->close_signal->Notify();
        } else if (auto server = owner->inbound_server.lock()) {
            std::lock_guard<std::mutex> lock(server->mu);
            server->close_signal->Notify();
        }
    }
}

/**
 * 把本条关联的 pending 结算成 error 并唤醒等待方。写失败与「写前钩子抛出」
 * 共用同一条责任路径：两者都发生在真实写终态之前，等待方必须拿到确定失败
 * 而不是等到自己的 deadline。
 */
void SessionCore::FailItemPending(const OutItem& item, const Error& error)
{
    if (item.key.empty()) {
        return;
    }
    std::shared_ptr<CoWaiter> waiter;
    {
        std::lock_guard<std::mutex> lock(pending_mu);
        if (auto it = calls.find(item.key); it != calls.end()) {
            it->second->outcome = result<Response>::err(error);
            it->second->done = true;
            waiter = it->second->waiter;
            calls.erase(it);
        } else if (auto si = sends.find(item.key); si != sends.end()) {
            si->second->outcome = result<void>::err(error);
            si->second->done = true;
            waiter = si->second->waiter;
            sends.erase(si);
        }
    }
    if (waiter) {
        waiter->Notify();
    }
}

bool SessionCore::Dequeue(OutItem& out)
{
    std::lock_guard<std::mutex> lock(out_mu);

    /**
     * 调度顺序：System 保留容量优先；Service/Actor 按权重轮询。
     */
    auto pop = [&](std::size_t lane) -> bool {
        if (out_q[lane].empty()) {
            return false;
        }
        out = TakeQueuedLocked(lane);
        ++out_lanes[lane].dequeued;
        return true;
    };
    if (pop(0)) {
        return true;
    }
    const std::uint32_t weights[kLaneCount - 1] = {owner->options.ServiceWeight(), owner->options.ActorWeight()};

    /**
     * 权重轮询：rr_credit[lane]=剩余可出队次数，两侧都耗尽时按
     * 权重整体重载。Service 连续取满自身份额后 credit=0，下一条落到还有
     * credit 的 Actor——Service 恒有流量也不会饿死 Actor。某 lane 队列空
     * 时 credit 保留但不消费；本条两侧都无有效 pop 走兜底任取。
     */
    if (rr_credit[1] == 0 && rr_credit[2] == 0) {
        rr_credit[1] = weights[0];
        rr_credit[2] = weights[1];
    }
    for (unsigned attempt = 0; attempt != 2; ++attempt) {
        const std::size_t lane = 1 + attempt;
        if (rr_credit[lane] != 0 && pop(lane)) {
            --rr_credit[lane];
            return true;
        }
    }

    /**
     * 两 lane 都无有效出队（空队列或 credit 耗尽且空）：兜底任取一条，
     * 不重置 credit——credit 描述的是份额，不是队列是否有货。
     */
    return pop(1) || pop(2);
}

void SessionCore::WriterLoop()
{
    for (;;) {
        OutItem item;
        if (!Dequeue(item)) {
            if (dead.load()) {
                return;
            }
            const auto status = out_signal->Wait({RpcClock::now() + 25ms});
            if (dead.load()) {
                return;
            }
            if (status == bbt::coroutine::WaitStatus::AlreadyWaiting
                || status == bbt::coroutine::WaitStatus::InvalidContext) {
                bbtco_sleep(5);
            }
            continue;
        }
        auto deadline = item.deadline == RpcClock::time_point::max() ? RpcClock::now() + 30s : item.deadline;

        /**
         * 已出队项的写责任区间：检查点、committed 标记、conn.Write 与结果
         * 映射同属一份责任。任何出口都先结算这一条（关联 pending + reply
         * settle + writes_outstanding 恰好一次）；Kill 只清扫仍在队列里的
         * 条目，替已出队项结不了账，漏掉这一笔会让 writes_outstanding 永不
         * 归零，Drain/Close 只能等到超时。
         */
        std::optional<Error> failure;
        bool write_attempted = false;
        try {

            /**
             * 固定动作检查点：未注册直接通过；Pause 在库内分片等待，不执行
             * 任何用户代码。失败（期限/取消/封口/注入）都是提交前确定失败。
             */
            const auto gate = RunHookCheckpoint(
                *owner->hooks, HookPoint::BeforeWrite, HookGate{owner.get(), nullptr, this, channel.get(), deadline});
            if (!gate) {
                failure = gate.error();
            } else {

                /**
                 * writer 已把本条从队列取出并即将交给 conn.Write——责任自
                 * 此刻由 writer 承担，不再属「排队中」。先把关联 pending 标
                 * committed（承担发送责任，结果可能未知），随后挂起期间若
                 * link 被 Kill，FailPending 的 committed_only 结算按「已承担」
                 * 返回 OutcomeUnknown 而非「未提交」Closed。
                 */
                if (!item.key.empty()) {
                    std::lock_guard<std::mutex> lock(pending_mu);
                    if (auto it = calls.find(item.key); it != calls.end()) {
                        it->second->committed = true;
                    } else if (auto si = sends.find(item.key); si != sends.end()) {
                        si->second->committed = true;
                    }
                }
                write_attempted = true;
                const auto report = channel->WriteBytes(std::move(item.bytes), {deadline});
                const auto settled = detail::TcpSettleWrite(report, "frame write failed");
                if (!settled) {
                    failure = settled.error();
                }
            }
        } catch (...) {

            /**
             * 提交前：确定失败。已承担 conn.Write：字节可能已发出，按既有
             * RequestCommitted/OutcomeUnknown 语义结算，不假装零提交、不重试。
             */
            failure =
                write_attempted
                    ? ErrPhase(
                          ErrorCode::OutcomeUnknown, "write path threw after commit", RequestPhase::RequestCommitted)
                    : Err(ErrorCode::InternalError, "write path threw before commit");
        }

        /**
         * 请求关联只在失败时结束；写成功仍须等可信回复。无论成功或失败，
         * writer 持有项都在同一处交还写责任，Kill 只负责队列中其余条目。
         */
        if (failure) {
            FailItemPending(item, *failure);
        }
        NoteWriteSettled(item, failure ? result<void>::err(*failure) : result<void>::ok());
        if (failure) {
            Kill(std::make_error_code(std::errc::io_error));
            return;
        }
    }
}

namespace {

/**
 * pending 唯一终态仲裁：committed_only 下未提交按 Closed、已提交按
 * OutcomeUnknown + RequestCommitted；关闭原因不在此附加（见 AttachCloseReason）。
 */
Error SettledPendingError(bool committed_only, bool committed, const Error& error)
{
    if (committed_only && !committed) {
        return Err(ErrorCode::Closed, "session closed before commit");
    }
    if (!committed_only) {
        return error;
    }
    return ErrPhase(ErrorCode::OutcomeUnknown, "session lost after commit", RequestPhase::RequestCommitted);
}

// clang-format off
}
// clang-format on

void SessionCore::FailPending(Error error, bool committed_only)
{
    std::vector<std::shared_ptr<CoWaiter>> wake;
    {
        std::lock_guard<std::mutex> lock(pending_mu);
        for (auto it = calls.begin(); it != calls.end();) {
            auto& pending = it->second;
            pending->outcome = result<Response>::err(SettledPendingError(committed_only, pending->committed, error));
            pending->done = true;
            wake.push_back(pending->waiter);
            it = calls.erase(it);
        }
        for (auto it = sends.begin(); it != sends.end();) {
            auto& pending = it->second;
            pending->outcome = result<void>::err(SettledPendingError(committed_only, pending->committed, error));
            pending->done = true;
            wake.push_back(pending->waiter);
            it = sends.erase(it);
        }
    }
    for (auto& waiter : wake) {
        waiter->Notify();
    }
}

void SessionCore::Kill(std::error_code reason)
{
    if (dead.exchange(true)) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(out_mu);
        out_sealed = true;
        for (std::size_t lane = 0; lane != kLaneCount; ++lane) {
            while (!out_q[lane].empty()) {
                auto item = TakeQueuedLocked(lane);
                ++out_lanes[lane].dropped;

                /**
                 * 队列项被 Kill 丢弃：发布 Closed 终态（未写出不能保留
                 * 「成功终态已发布」的假象）并结算本条写责任。writer 手中
                 * 在写的那条不在队列内，由 WriterLoop 终态路径正常结算。
                 */
                NoteWriteSettled(item, result<void>::err(Err(ErrorCode::Closed, "session killed before reply write")));
            }
        }
    }
    if (out_signal) {
        out_signal->Notify();
    }
    if (channel->Raw().Valid()) {
        channel->RequestClose(reason == std::error_code{} ? std::make_error_code(std::errc::io_error) : reason);
    }

    /**
     * committed 但无可信终态 → OutcomeUnknown；未提交 → Closed（committed_only=true）。
     */
    FailPending({}, true);

    /**
     * owner 链接数/会话额度与映射不归这里结算：session.dead 只是逻辑死亡，
     * 物理完成凭据是「退出责任归零 + conn Released」——唯一归还点在
     * Reader/WriterLoop 尾部的 ReleasedLocked()。
     */
    if (auto server = this->server.lock()) {
        std::lock_guard<std::mutex> lock(server->mu);
        server->close_signal->Notify();
    } else if (auto server = owner->inbound_server.lock()) {
        std::lock_guard<std::mutex> lock(server->mu);
        server->close_signal->Notify();
    }

    /**
     * 物理连接责任在逻辑死亡点结算：Kill 只进一次（dead.exchange 保证），
     * 此后 reader/writer 各自退出再结算自己的那份——责任归零点即本 link
     * 物理收口观察点。
     */
    ExitDone();
}

/**
 * 一份退出责任到达终态。到达 0 时若物理连接也已 Released，在本调用内
 * 完成额度归还（ReleasedLocked 幂等，重复收口无效果）。
 */
bool SessionCore::ExitDone()
{
    if (responsibilities.fetch_sub(1) != 1) {
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        ReleasedLocked();
    }
    if (auto server = this->server.lock()) {
        std::lock_guard<std::mutex> lock(server->mu);
        server->close_signal->Notify();
    } else if (auto server = owner->inbound_server.lock()) {
        std::lock_guard<std::mutex> lock(server->mu);
        server->close_signal->Notify();
    }
    return true;
}

/**
 * Released 唯一归还点（持 owner->mu）：物理收口真实完成后才归还额度——
 * 退役未完成的 link 持续占用 MaxSessions，防止「逻辑死亡即释放额度」
 * 让新建立绕过上限。物理 conn 未 Released 前不得归还——conn 仍
 * 承担在途读写/物理关闭责任时，session/link 计数与 by_identity 台账必须
 * 保留，否则失去物理 Released 凭据。返回是否有额外交付。
 */
bool SessionCore::ReleasedLocked()
{

    /**
     * 唯一归还点：物理 conn 必须已 Released（fd 关 + owner 责任清）。
     */
    if (!channel->IsReleased()) {
        return false;
    }
    if (!quota_held.exchange(false)) {
        return false;
    }
    owner->links.fetch_sub(1);
    owner->sessions.fetch_sub(1);
    for (auto it = owner->by_identity.begin(); it != owner->by_identity.end();) {
        if (it->second.get() == this) {
            it = owner->by_identity.erase(it);
        } else {
            ++it;
        }
    }
    for (auto it = owner->by_endpoint.begin(); it != owner->by_endpoint.end();) {
        if (auto session = it->second.lock(); !session || session.get() == this) {
            it = owner->by_endpoint.erase(it);
        } else {
            ++it;
        }
    }
    return true;
}

void SessionCore::PostSessionError(const Error& error)
{
    SessionFrame frame;
    frame.kind = FrameKind::SessionError;
    frame.traffic = TrafficClass::System;
    frame.session_epoch = epoch;
    frame.has_outcome = true;
    frame.success = false;
    frame.error = error;
    auto bytes = detail::RpcFrameProtocol::Encode(frame, owner->options.MaxFrameBytes());
    if (bytes) {
        OutItem item;
        item.bytes.assign(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
        item.lane = TrafficClass::System;
        item.control_response = true;
        item.deadline = RpcClock::now() + 5s;
        (void)Enqueue(std::move(item));
    }
}

void SessionCore::RejectProtocol(const std::string& message)
{
    if (dead.load()) {
        return;
    }
    SessionFrame frame;
    frame.kind = FrameKind::SessionError;
    frame.traffic = TrafficClass::System;
    frame.session_epoch = epoch;
    frame.has_outcome = true;
    frame.success = false;
    frame.error = Err(ErrorCode::ProtocolError, message);
    auto bytes = detail::RpcFrameProtocol::Encode(frame, owner->options.MaxFrameBytes());
    if (bytes) {
        OutItem item;
        item.bytes.assign(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
        item.lane = TrafficClass::System;
        item.control_response = true;
        item.deadline = RpcClock::now() + 5s;
        item.settle = std::make_shared<OutItem::Settle>();
        auto settle = item.settle;
        if (Enqueue(std::move(item))) {

            /**
             * 终态控制帧有界等待真实写结算；写不出也不阻塞退役。
             */
            (void)settle->waiter->Wait({RpcClock::now() + 500ms});
        }
    }
    Kill(std::make_error_code(std::errc::protocol_error));
}

namespace {

/**
 * reader 上的 admission 拒绝路径不等写终态（定义在 TU 后部）。
 */
void PostReplyErrorNoWait(
    const std::shared_ptr<SessionCore>& session, const detail::ReplyCompletionImpl& impl, Error error);
// clang-format off
}
// clang-format on

void SessionCore::AdmitInbound(const SessionFrame& frame, Request request)
{
    auto server = this->server.lock();
    if (!server) {
        server = owner->inbound_server.lock();
    }
    auto& stats = out_lanes[LaneIndex(request.Traffic())];
    std::shared_ptr<AdmissionQueue> queue;
    if (server) {

        /**
         * queue 指针写入受 server->mu 保护（ServerV2Start）；观察面取同一
         * 临界区快照，不与 Start/Close 的台账写并发裸读。
         */
        std::lock_guard<std::mutex> lock(server->mu);
        queue = server->queue;
    }
    if (!server || !queue) {
        PostSessionError(Err(ErrorCode::Unavailable, "no inbound admission target"));
        {
            std::lock_guard<std::mutex> lock(out_mu);
            ++stats.dropped;
        }
        return;
    }
    AdmissionItem item;
    item.request = std::move(request);
    auto lease_impl = std::make_shared<detail::AdmissionLeaseImpl>();
    lease_impl->server = server;
    lease_impl->lane = item.request.Traffic();
    item.lease = detail::AdmissionAccess::MakeLease(lease_impl);
    if (frame.kind == FrameKind::Call
        || (frame.kind == FrameKind::Send && frame.acceptance_requested.has_value()
            && frame.acceptance_requested.value())) {
        auto impl = std::make_shared<detail::ReplyCompletionImpl>();
        impl->session = weak_from_this();
        impl->server = server;
        impl->lane = item.request.Traffic();
        impl->reply_deadline = item.request.Context().deadline;
        if (frame.kind == FrameKind::Call) {
            impl->kind = FrameKind::Response;
            impl->request_id = frame.request_id;
            impl->service = frame.service;
            impl->method = frame.method;
            impl->request_schema = frame.request_schema;
            impl->response_schema = frame.response_schema;
        } else {
            impl->kind = FrameKind::SendAccepted;
            impl->message_id = frame.message_id;
        }
        item.reply = detail::AdmissionAccess::MakeReply(impl);
    }
    {
        std::lock_guard<std::mutex> lock(out_mu);
        ++stats.inflight;
    }
    const auto item_lane = LaneIndex(item.request.Traffic());

    /**
     * MaxInflight 覆盖「已交 admission 仍未到责任终态」的持有 lease——
     * 在唯一 +1 之前按容量拒绝（拒绝立即 Overloaded，不等 deadline）。
     * 计数与额度检查在同一 server->mu 临界区，杜绝越过上限的竞态。
     */
    bool over_cap = false;
    {
        std::lock_guard<std::mutex> lock(server->mu);
        if (server->inflight[item_lane] >= server->options.MaxInflight()) {
            ++server->dropped[item_lane];
            over_cap = true;
        } else {
            server->NoteInflight(item_lane, +1);

            /**
             * 只有会写回复终态的 Call / 要求远端确认的 Send 才占排空计数；
             * 不要求确认的 Send 没有可结算的回复写责任，不能计入。
             */
            if (item.reply) {
                ++server->open_replies;
            }
        }
    }
    if (over_cap) {
        {
            std::lock_guard<std::mutex> slock(out_mu);
            ++stats.dropped;
            --stats.inflight;
        }

        /**
         * 保留 reply 凭据发布唯一拒绝终态，连接仍可进展。本路径在
         * 唯一 reader 上——不能调用会挂起等写终态的 Fail（写背压会阻塞
         * reader，无法继续解后续帧）。标 terminal 后直接入队错误帧不等待；
         * 写责任仍由 WriterLoop/Kill 真实结算。
         */
        if (item.reply) {
            if (auto impl = detail::AdmissionAccess::ClaimTerminal(*item.reply)) {
                auto session = impl->session.lock();
                if (session) {
                    PostReplyErrorNoWait(session, *impl, Err(ErrorCode::Overloaded, "inflight limit reached"));
                }
            }
        }

        /**
         * lease 从未 +1（额度检查先于唯一记账）——不 Release，
         * 随 item 析构，不欠账。
         */
        return;
    }

    /**
     * 保留完成凭据/lease 的本地副本——Admit 按值转移 item，拒绝路径
     * 仍需可用 reply/lease 结算唯一终态。
     */
    auto reply = item.reply;
    auto lease = item.lease;
    const auto admitted = queue->Admit(std::move(item));
    if (!admitted) {

        /**
         * Admit 失败（满/封口）：item 未入队，infra 直接发布一次终态——
         * Call → Response error；带确认 Send → SendAccepted error。本路径
         * 同在唯一 reader 上，不等写终态。
         */
        if (reply) {
            if (auto impl = detail::AdmissionAccess::ClaimTerminal(*reply)) {
                auto session = impl->session.lock();
                if (session) {
                    PostReplyErrorNoWait(session, *impl, admitted.error());
                }
            }
        }

        /**
         * lease 单次结算：Release 已完成 inflight-1，这里不再重复扣账。
         */
        lease->Release();
        {
            std::lock_guard<std::mutex> lock(server->mu);
            ++server->dropped[item_lane];

            /**
             * 与入账同一判据：只有持有回复写责任的项才归还 open_replies。
             */
            if (reply && server->open_replies != 0) {
                --server->open_replies;
            }
            if (server->close_signal) {
                server->close_signal->Notify();
            }
        }
        {
            std::lock_guard<std::mutex> lock(out_mu);
            ++stats.dropped;
            --stats.inflight;
        }
    }
}

void SessionCore::DispatchInbound(const SessionFrame& frame)
{
    if (frame.session_epoch != 0 && frame.session_epoch != epoch) {

        /**
         * 旧 epoch 帧丢弃并计数（不改写已发布终态）。
         */
        auto& stats = out_lanes[LaneIndex(frame.traffic)];
        std::lock_guard<std::mutex> lock(out_mu);
        ++stats.dropped;
        return;
    }
    switch (frame.kind) {
    case FrameKind::Response: {
        std::shared_ptr<PendingCall> pending;
        {
            std::lock_guard<std::mutex> lock(pending_mu);
            auto it = calls.find(frame.request_id);
            if (it != calls.end()) {
                pending = it->second;
                calls.erase(it);
            }
        }
        if (!pending) {
            return;

            /**
         * 晚到/超时后回复丢弃
         */
        }
        result<Response> outcome = [&]() -> result<Response> {
            if (frame.has_outcome && frame.success) {
                auto response = ResponseFromSessionFrame(frame);
                if (response && frame.service == pending->service && frame.method == pending->method
                    && frame.request_schema == pending->request_schema
                    && frame.response_schema == pending->response_schema) {
                    return response;
                }
                return result<Response>::err(ErrPhase(
                    ErrorCode::ProtocolError,
                    response ? "response correlation mismatch" : "response decode failed",
                    RequestPhase::ReplyTerminal));
            }
            if (frame.has_outcome) {
                auto error = frame.error;
                if (!error.request_phase.has_value()) {
                    error.request_phase = RequestPhase::ReplyTerminal;
                }
                return result<Response>::err(std::move(error));
            }
            return result<Response>::err(
                ErrPhase(ErrorCode::ProtocolError, "response missing outcome", RequestPhase::ReplyTerminal));
        }();
        {
            std::lock_guard<std::mutex> lock(pending_mu);
            pending->outcome = std::move(outcome);
            pending->done = true;
        }
        pending->waiter->Notify();
        return;
    }
    case FrameKind::SendAccepted: {
        std::shared_ptr<PendingSend> pending;
        {
            std::lock_guard<std::mutex> lock(pending_mu);
            auto it = sends.find(frame.message_id);
            if (it != sends.end()) {
                pending = it->second;
                sends.erase(it);
            }
        }
        if (!pending) {
            return;
        }
        result<void> outcome = [&]() -> result<void> {
            if (frame.has_outcome && frame.success) {
                return result<void>::ok();
            }
            if (frame.has_outcome) {
                return result<void>::err(frame.error);
            }
            return result<void>::err(
                ErrPhase(ErrorCode::ProtocolError, "send-accepted missing outcome", RequestPhase::ReplyTerminal));
        }();
        {
            std::lock_guard<std::mutex> lock(pending_mu);
            pending->outcome = std::move(outcome);
            pending->done = true;
        }
        pending->waiter->Notify();
        return;
    }
    case FrameKind::Call:
    case FrameKind::Send: {

        /**
         * 同 session 同 ordering_key 的入站顺序台账：首个有序帧即基值，此后
         * 必须逐帧连续递增。重复/逆序（seq <= last）与缺口（seq != last+1）
         * 都按协议违规退役本 link——单 reader 的 wire 到达序是唯一事实源，
         * 缺口在可靠字节流上不会自愈，等待没有对象可等。
         */
        if (frame.sequence != 0) {
            const auto it = inbound_sequence.find(frame.ordering_key);
            if (it != inbound_sequence.end() && (frame.sequence <= it->second || frame.sequence != it->second + 1)) {
                RejectProtocol("ordering sequence violation");
                return;
            }
            inbound_sequence[frame.ordering_key] = frame.sequence;
        }
        auto request = RequestFromSessionFrame(frame);
        if (!request) {
            PostSessionError(request.error());
            return;
        }
        RequestContext context;
        const auto now = RpcClock::now();
        const auto remote = now + std::chrono::milliseconds{frame.remaining_budget_ms};
        const auto local = [this, now] {
            if (auto server = this->server.lock()) {
                return now + server->options.IncomingTimeout();
            }
            return RpcClock::time_point::max();
        }();
        context.deadline = std::min(remote, local);
        context.peer_identity = peer_identity;
        detail::RequestContextAccess::Set(request.value(), std::move(context));
        AdmitInbound(frame, std::move(request.value()));
        return;
    }
    case FrameKind::SessionError:
        Kill(std::make_error_code(std::errc::connection_reset));
        return;
    case FrameKind::Close:
        Kill({});
        return;
    case FrameKind::Hello:
    case FrameKind::HelloAck:
    case FrameKind::Unspecified:
        RejectProtocol("unexpected frame kind on live session");
        return;
    }
}

void SessionCore::ReaderLoop()
{
    for (;;) {
        if (dead.load()) {
            return;
        }

        /**
         * 先耗 decoder 缓存（含握手移交的合包余帧），再阻塞 Read。
         */
        for (;;) {
            auto next = decoder.Next();
            if (!next) {
                RejectProtocol("inbound frame decode rejected");
                return;
            }
            if (!next.value().has_value()) {
                break;
            }
            DispatchInbound(*next.value());
            if (dead.load()) {
                return;
            }
        }
        auto [read_data, read_error] = channel->ReadBytes({RpcClock::time_point::max()});
        if (!read_error) {
            for (const auto& seg : read_data.Segments()) {
                const auto fed = decoder.Feed(reinterpret_cast<const std::uint8_t*>(seg.data), seg.size);
                if (!fed) {
                    RejectProtocol("inbound frame feed rejected");
                    return;
                }
            }
            continue;
        }
        if (read_error == bbt::infra::net::make_error_code(bbt::infra::net::NetErrc::Eof)) {
            Kill(std::make_error_code(std::errc::connection_reset));
        } else {
            Kill(read_error);
        }
        return;
    }
}

namespace {

/**
 * Handshake helpers（trusted-network plaintext，ADR §3.2/§3.4）
 * 受限握手阶段 I/O：直接在 conn 上 Write/Read（未建 lane worker）。
 * Hello/HelloAck 帧上限 min(max_frame,64KiB) 由 codec 在 hello_phase
 * 前缀阶段强制。先耗 decoder 缓存再 Read；同批 Read 只消费到「下一个
 * 完整握手帧」为止，排在其后的字节经 HandshakeStash 移交调用方——调用
 * 方在验证握手边界的时刻切数据态后按数据限额补喂（合法最终
 * HelloAck+大首数据帧合包不被握手 pending 界误拒；未验证前这些字节
 * 尚未进 decoder，大声明仍按握手限额拒绝）。
 *
 * HandshakeStash：同批 Read 解出握手帧后、decoder 里尚未耗尽的余字节
 * （排在握手帧之后的合包数据）。由调用方持有到握手边界验证完成，再按
 * 数据态补喂——验证前这些字节不属任何阶段限额，未验证不进入 decoder。
 */
struct HandshakeStash {

    /**
     * owning 余量（可为空）
     */
    std::vector<std::uint8_t> bytes;

    bool Empty() const noexcept { return bytes.empty(); }
};

/**
 * ReadHandshakeFrame 的增量补喂块大小：两次 Next() 之间 decoder 缓冲只
 * 容纳不完整单帧（声明≤64KiB+4），32KiB 块使 buffer+take 恒小于握手
 * pending 界（4+2*64KiB），合法帧头不触硬界、非法大声明仍在解码点被拒。
 */
constexpr std::size_t kHandshakeFeedChunk = 32 * 1024;

/**
 * 读一帧握手帧。同批 Read 只消费到「下一个完整握手帧」为止——解码成功
 * 的瞬间把 decoder 里尚未耗尽的同批余字节 TakePending 进 stash 返回，
 * 由调用方在握手边界验证完成后按数据态补喂（合法最终 HelloAck+
 * 大首数据帧合包不被握手 pending 界误拒；未验证前这些字节未进
 * decoder，大声明仍按握手限额拒绝）。stash 无跨批语义，每帧调用清零。
 */
result<SessionFrame> ReadHandshakeFrame(
    detail::ICoChannel& channel, SessionFrameDecoder& decoder, bbt::coroutine::Deadline deadline, HandshakeStash& stash)
{

    /**
     * 帧接受门禁：kind 校验 + 已喂未解余字节移交 stash。stash 内字节序 =
     * 字节流序（decoder 余量在前，其后是调用方补喂的未喂输入）。
     */
    const auto accept = [&](SessionFrame&& frame) -> result<SessionFrame> {
        if (frame.kind != FrameKind::Hello && frame.kind != FrameKind::HelloAck
            && frame.kind != FrameKind::SessionError) {
            return result<SessionFrame>::err(Err(ErrorCode::ProtocolError, "non-handshake frame during handshake"));
        }
        if (decoder.PendingBytes() != 0) {

            /**
             * decoder 余量在字节流序中先于 stash 中尚未喂入的尾部——前置
             * 而非覆盖，否则 stash 分支（本调用已从 stash 喂过一块）丢字节。
             */
            auto pending = decoder.TakePending();
            if (!stash.bytes.empty()) {
                pending.insert(
                    pending.end(),
                    std::make_move_iterator(stash.bytes.begin()),
                    std::make_move_iterator(stash.bytes.end()));
            }
            stash.bytes = std::move(pending);
        }
        return result<SessionFrame>::ok(std::move(frame));
    };
    for (;;) {
        if (const auto gate = CheckCall(deadline)) {
            return result<SessionFrame>::err(CallError(gate, "handshake timed out", false));
        }

        /**
         * 先耗缓存：上次已移交的字节先解。一帧到手即返回。
         */
        {
            auto cached = decoder.Next();
            if (!cached) {
                return result<SessionFrame>::err(cached.error());
            }
            if (cached.value().has_value()) {
                return accept(std::move(cached.value().value()));
            }
        }

        /**
         * 再耗 stash：同批未验证余字节仍按握手限额增量补喂（与下方 Read
         * 批同规则）；解出首个帧即停，未喂余量留在 stash 交调用方裁决。
         */
        while (!stash.Empty()) {
            const std::size_t take = std::min(stash.bytes.size(), kHandshakeFeedChunk);
            const auto fed = decoder.Feed(stash.bytes.data(), take);
            if (!fed) {
                return result<SessionFrame>::err(Err(ErrorCode::ProtocolError, "handshake stash decode rejected"));
            }
            stash.bytes.erase(stash.bytes.begin(), stash.bytes.begin() + take);
            auto nxt = decoder.Next();
            if (!nxt) {
                return result<SessionFrame>::err(nxt.error());
            }
            if (nxt.value().has_value()) {
                return accept(std::move(nxt.value().value()));
            }
        }
        auto [read_data, read_error] = channel.ReadBytes({deadline});
        if (!read_error) {

            /**
             * 同一 Read 批不整批先按 hello_phase Feed——分段内按
             * kHandshakeFeedChunk 块补喂，每块后尝试解帧；解出首个握手帧即
             * 停喂，本批未喂字节排在 decoder 余量之后移交 stash（由调用方
             * 在边界验证后按数据态补喂）。decoder 缓冲在两次 Next 之间只
             * 容纳不完整的单帧（≤64KiB+4），单块补喂不会触达握手 pending
             * 界；合法最终 HelloAck+大首数据帧合包因此不被误拒。
             */
            const auto& segments = read_data.Segments();
            for (std::size_t si = 0; si < segments.size(); ++si) {
                const auto* base = reinterpret_cast<const std::uint8_t*>(segments[si].data);
                std::size_t offset = 0;
                while (offset < segments[si].size) {
                    const std::size_t take = std::min(segments[si].size - offset, kHandshakeFeedChunk);
                    const auto fed = decoder.Feed(base + offset, take);
                    if (!fed) {
                        return result<SessionFrame>::err(
                            Err(ErrorCode::ProtocolError, "handshake frame decode rejected"));
                    }
                    offset += take;
                    auto nxt = decoder.Next();
                    if (!nxt) {
                        return result<SessionFrame>::err(nxt.error());
                    }
                    if (nxt.value().has_value()) {
                        auto accepted = accept(std::move(nxt.value().value()));
                        if (!accepted) {
                            return accepted;
                        }

                        /**
                         * 未喂输入排在 decoder 余量之后（字节流序）。
                         */
                        stash.bytes.insert(stash.bytes.end(), base + offset, base + segments[si].size);
                        for (std::size_t sj = si + 1; sj < segments.size(); ++sj) {
                            const auto* tail = reinterpret_cast<const std::uint8_t*>(segments[sj].data);
                            stash.bytes.insert(stash.bytes.end(), tail, tail + segments[sj].size);
                        }
                        return accepted;
                    }
                }
            }
            continue;
        }
        if (read_error == bbt::infra::net::make_error_code(bbt::infra::net::NetErrc::Eof)) {
            return result<SessionFrame>::err(Err(ErrorCode::Unavailable, "peer closed during handshake"));
        }
        return result<SessionFrame>::err(CallError(read_error, "handshake read failed", false));
    }
}

/**
 * 验证最终握手帧完成后的边界：切数据态并把 stash 余字节按数据限额
 * 一次性补喂回 decoder。调用前提：ack/echo 已逐项验证——此后这些字节
 * 按数据帧上限判定（数据 pending 界 4+2*max_frame 可容纳整批）。握手
 * 中途的 stash 补喂不经过本函数：ReadHandshakeFrame 内按
 * kHandshakeFeedChunk 增量消耗，超界 stash 不会撞握手 pending 硬界。
 */
result<void> CommitStashAfterFinalAck(SessionFrameDecoder& decoder, HandshakeStash& stash)
{
    decoder.SetHelloPhase(false);
    if (stash.Empty()) {
        return result<void>::ok();
    }
    const auto fed = decoder.Feed(stash.bytes.data(), stash.bytes.size());
    stash.bytes.clear();
    if (!fed) {
        return result<void>::err(Err(ErrorCode::ProtocolError, "handshake stash decode rejected"));
    }
    return result<void>::ok();
}

result<void> WriteHandshakeFrame(
    detail::ICoChannel& channel, const SessionFrame& frame, bbt::coroutine::Deadline deadline, std::size_t max_frame)
{
    auto bytes = detail::RpcFrameProtocol::Encode(frame, max_frame);
    if (!bytes) {
        return result<void>::err(bytes.error());
    }
    std::string out(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
    const auto report = channel.WriteBytes(std::move(out), {deadline});
    return detail::TcpSettleWrite(report, "handshake write failed");
}

SessionFrame MakeHello(const ClientOptions& options, std::uint64_t epoch, std::uint64_t nonce)
{
    SessionFrame frame;
    frame.kind = FrameKind::Hello;
    frame.traffic = TrafficClass::System;
    frame.peer_identity = options.LocalIdentity();
    frame.session_epoch = epoch;
    frame.connection_nonce = nonce;
    return frame;
}

SessionFrame MakeHelloAck(const ClientOptions& options, std::uint64_t epoch, std::uint64_t peer_nonce)
{
    SessionFrame frame;
    frame.kind = FrameKind::HelloAck;
    frame.traffic = TrafficClass::System;
    frame.peer_identity = options.LocalIdentity();
    frame.session_epoch = epoch;
    frame.connection_nonce = peer_nonce;
    return frame;
}

/**
 * 出站方向（拨号侧）：写 Hello → 读 Hello → epoch owner 决定 epoch →
 * 写 HelloAck → 读 HelloAck echo。identity 较小一侧是 epoch owner。
 * 成功时 out_decoder 收编握手 decoder（含未耗尽的合包余帧），供 session
 * live reader 继续消费（余帧移交）。
 */
result<std::uint64_t> OutboundHandshake(
    detail::ICoChannel& channel,
    const ClientOptions& options,
    std::uint64_t proposal,
    std::uint64_t nonce,
    std::string& out_peer_identity,
    SessionFrameDecoder& out_decoder,
    bbt::coroutine::Deadline deadline)
{
    using R = result<std::uint64_t>;
    HandshakeStash stash;
    auto hello = MakeHello(options, proposal, nonce);
    if (auto wrote = WriteHandshakeFrame(channel, hello, deadline, options.MaxFrameBytes()); !wrote) {
        return R::err(wrote.error());
    }
    SessionFrameDecoder decoder(options.MaxFrameBytes());
    auto peer_hello = ReadHandshakeFrame(channel, decoder, deadline, stash);
    if (!peer_hello) {
        return R::err(peer_hello.error());
    }
    if (peer_hello.value().kind != FrameKind::Hello) {
        return R::err(Err(ErrorCode::ProtocolError, "expected Hello from peer"));
    }
    const auto& remote = peer_hello.value();
    if (remote.peer_identity == options.LocalIdentity()) {
        return R::err(Err(ErrorCode::ProtocolError, "self-connection rejected"));
    }
    out_peer_identity = remote.peer_identity;
    const bool i_own_epoch = options.LocalIdentity() < remote.peer_identity;
    std::uint64_t epoch = 0;
    if (i_own_epoch) {

        /**
         * epoch owner：本端 proposal 即 selected。
         */
        epoch = proposal;
        auto ack = MakeHelloAck(options, epoch, remote.connection_nonce);
        if (auto wrote = WriteHandshakeFrame(channel, ack, deadline, options.MaxFrameBytes()); !wrote) {
            return R::err(wrote.error());
        }
    }

    /**
     * 第一帧取出时若有同批余字节（合包 HelloAck 前缀/数据帧等），留在
     * stash——ReadHandshakeFrame 内按握手限额增量补喂，不整批硬喂。
     */
    auto peer_ack = ReadHandshakeFrame(channel, decoder, deadline, stash);
    if (!peer_ack) {
        return R::err(peer_ack.error());
    }
    if (peer_ack.value().kind != FrameKind::HelloAck) {
        return R::err(Err(ErrorCode::ProtocolError, "expected HelloAck from peer"));
    }
    const auto& ack = peer_ack.value();
    if (ack.peer_identity != remote.peer_identity) {
        return R::err(Err(ErrorCode::ProtocolError, "HelloAck identity mismatch"));
    }
    if (i_own_epoch) {
        if (ack.session_epoch != epoch || ack.connection_nonce != nonce) {
            return R::err(Err(ErrorCode::ProtocolError, "HelloAck echo mismatch"));
        }

        /**
         * 验证完成的边界：切数据态后 stash 余字节按数据限额补喂。
         */
        if (auto committed = CommitStashAfterFinalAck(decoder, stash); !committed) {
            return R::err(committed.error());
        }
        out_decoder = std::move(decoder);
        return R::ok(epoch);
    }

    /**
     * 对端是 epoch owner：原样 echo selected epoch。
     */
    epoch = ack.session_epoch;
    auto echo = MakeHelloAck(options, epoch, remote.connection_nonce);
    if (auto wrote = WriteHandshakeFrame(channel, echo, deadline, options.MaxFrameBytes()); !wrote) {
        return R::err(wrote.error());
    }
    if (auto committed = CommitStashAfterFinalAck(decoder, stash); !committed) {
        return R::err(committed.error());
    }
    out_decoder = std::move(decoder);
    return R::ok(epoch);
}

/**
 * 入站方向（接纳侧）：读 Hello → 写 Hello → 同样规则。
 * out_decoder 同 OutboundHandshake：成功时收编含余帧的握手 decoder。
 */
result<std::uint64_t> InboundHandshake(
    detail::ICoChannel& channel,
    const ClientOptions& options,
    std::uint64_t proposal,
    std::uint64_t nonce,
    std::string& out_peer_identity,
    SessionFrameDecoder& out_decoder,
    bbt::coroutine::Deadline deadline)
{
    using R = result<std::uint64_t>;
    HandshakeStash stash;
    SessionFrameDecoder decoder(options.MaxFrameBytes());
    auto peer_hello = ReadHandshakeFrame(channel, decoder, deadline, stash);
    if (!peer_hello) {
        return R::err(peer_hello.error());
    }
    if (peer_hello.value().kind != FrameKind::Hello) {
        return R::err(Err(ErrorCode::ProtocolError, "expected Hello from inbound peer"));
    }
    const auto& remote = peer_hello.value();
    if (remote.peer_identity == options.LocalIdentity()) {
        return R::err(Err(ErrorCode::ProtocolError, "self-connection rejected"));
    }
    out_peer_identity = remote.peer_identity;
    auto hello = MakeHello(options, proposal, nonce);
    if (auto wrote = WriteHandshakeFrame(channel, hello, deadline, options.MaxFrameBytes()); !wrote) {
        return R::err(wrote.error());
    }
    const bool i_own_epoch = options.LocalIdentity() < remote.peer_identity;
    std::uint64_t epoch = 0;
    if (i_own_epoch) {
        epoch = proposal;
        auto ack = MakeHelloAck(options, epoch, remote.connection_nonce);
        if (auto wrote = WriteHandshakeFrame(channel, ack, deadline, options.MaxFrameBytes()); !wrote) {
            return R::err(wrote.error());
        }
        auto peer_ack = ReadHandshakeFrame(channel, decoder, deadline, stash);
        if (!peer_ack) {
            return R::err(peer_ack.error());
        }
        if (peer_ack.value().kind != FrameKind::HelloAck || peer_ack.value().session_epoch != epoch
            || peer_ack.value().connection_nonce != nonce || peer_ack.value().peer_identity != remote.peer_identity) {
            return R::err(Err(ErrorCode::ProtocolError, "HelloAck echo mismatch"));
        }

        /**
         * 验证完成边界：切数据态，stash 余字节按数据限额补喂。
         */
        if (auto committed = CommitStashAfterFinalAck(decoder, stash); !committed) {
            return R::err(committed.error());
        }
        out_decoder = std::move(decoder);
        return R::ok(epoch);
    }

    /**
     * 对端是 epoch owner：先读 selected epoch，再 echo。
     */
    auto peer_ack = ReadHandshakeFrame(channel, decoder, deadline, stash);
    if (!peer_ack) {
        return R::err(peer_ack.error());
    }
    if (peer_ack.value().kind != FrameKind::HelloAck || peer_ack.value().connection_nonce != nonce
        || peer_ack.value().peer_identity != remote.peer_identity) {
        return R::err(Err(ErrorCode::ProtocolError, "HelloAck echo mismatch"));
    }
    epoch = peer_ack.value().session_epoch;
    auto echo = MakeHelloAck(options, epoch, remote.connection_nonce);
    if (auto wrote = WriteHandshakeFrame(channel, echo, deadline, options.MaxFrameBytes()); !wrote) {
        return R::err(wrote.error());
    }
    if (auto committed = CommitStashAfterFinalAck(decoder, stash); !committed) {
        return R::err(committed.error());
    }
    out_decoder = std::move(decoder);
    return R::ok(epoch);
}

/**
 * 会话启动：握手完成后装注册表 + 去重，再启动 reader/writer 协程。
 * 返回可交付调用的 session；输家 link 已关闭时返回 err。
 * 会话额度（MaxSessions）在这里同一 owner->mu 检查+占位——by_identity
 * 成员与建立中的占位 session 都计入；未过检查不产生任何长期 session。
 * 同时拨号选举（ADR §3.3）：identity 较小侧保留其本地发起 link。
 */
result<std::shared_ptr<SessionCore>> InstallSession(
    std::shared_ptr<SessionOwnerState> owner,
    std::shared_ptr<SessionCore> session,
    const std::string& endpoint_key,
    SessionFrameDecoder&& decoder,
    const std::shared_ptr<SessionOwnerState::Establishing>& establishing = nullptr)
{
    using R = result<std::shared_ptr<SessionCore>>;
    std::shared_ptr<SessionCore> evicted;
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        if (owner->closed) {
            return R::err(Err(ErrorCode::Closed, "owner closed"));
        }
        if (auto existing = owner->by_identity.find(session->peer_identity); existing != owner->by_identity.end()) {
            auto& winner = existing->second;
            const bool keep_outbound = owner->options.LocalIdentity() < session->peer_identity;
            if (keep_outbound == session->outbound) {

                /**
                 * 本 link 胜出：旧 session 退役。退役额度先不移交（仍由
                 * evicted 持有到其 Released）；替换本就不新增净占用。
                 */
                evicted = winner;
                winner = session;
            } else {

                /**
                 * 本 link 输家：关闭（fencing），映射到胜者。握手余帧随
                 * 输家 decoder 一并销毁——不再属于任何 live reader。
                 */
                session->dead.store(true);
                if (session->channel->Raw().Valid()) {
                    session->channel->RequestClose(std::make_error_code(std::errc::connection_reset));
                }
                if (!endpoint_key.empty()) {
                    owner->by_endpoint[endpoint_key] = winner;
                }
                return R::ok(winner);
            }
        } else {

            /**
             * MaxSessions 真实占用——已安装成员 + 其他建立中占位都
             * 计入；本 establishing 的 reserved 槽位在安装成功时转化为
             * by_identity 成员（-1 排除自身），不占双份。退役实际完成
             * （ReleasedLocked）才归还额度。
             */
            std::size_t occupied = owner->by_identity.size();
            for (const auto& [key, est] : owner->establishing) {
                (void)key;
                if (est->reserved) {
                    ++occupied;
                }
            }
            if (establishing && establishing->reserved) {
                --occupied;
            }
            if (occupied >= owner->options.MaxSessions()) {
                return R::err(Err(ErrorCode::Overloaded, "max sessions reached"));
            }
            if (establishing) {
                establishing->reserved = false;
            }
            owner->by_identity[session->peer_identity] = session;
        }
        session->quota_held.store(true);
        if (!endpoint_key.empty()) {
            owner->by_endpoint[endpoint_key] = session;
        }

        /**
         * 胜者接管被退役者的 endpoint 映射：被 fencing 的 session 曾占用
         * 的拨号键改指赢家——调用方重取会话时落到存活 link，不会因旧键
         * 失效触发对同一 PeerKey 的第三次拨号。
         */
        if (evicted) {
            for (auto& [key, weak] : owner->by_endpoint) {
                if (auto prev = weak.lock(); prev && prev.get() == evicted.get()) {
                    weak = session;
                }
            }
        }

        /**
         * 胜者收编握手 decoder（含合包余帧）——输家路径上面已提前返回。
         */
        session->decoder = std::move(decoder);
        session->decoder.SetHelloPhase(false);
        owner->links.fetch_add(1);
        owner->sessions.fetch_add(1);
    }
    if (evicted) {
        evicted->Kill({});
    }

    /**
     * reader+writer 两份协程责任 + 一份物理连接责任（任一路退出时一并
     * 结算——写失败 Kill 与读终态 Kill 同义）。物理责任 ExitDone 结算在
     * 最后一份协程责任之后：channel->IsReleased() 由该点观察。
     */
    session->responsibilities.fetch_add(1);
    if (!session->Submit(128 * 1024, [s = session] { s->ReaderLoop(); })
        || !session->Submit(128 * 1024, [s = session] { s->WriterLoop(); })) {
        session->Kill({});
        return R::err(Err(ErrorCode::RuntimeUnavailable, "session task registration failed"));
    }
    return R::ok(session);
}

/**
 * 拨号 + 握手（在调用方受管协程内执行）。establishing 记录由 GetSession
 * 同事务创建并置 reserved（建立中额度占位），这里透传给 InstallSession——
 * 安装成功时 reserved 转化为 by_identity 成员额度。
 */
result<std::shared_ptr<SessionCore>> EstablishOutbound(
    std::shared_ptr<SessionOwnerState> owner,
    const net::Address& target,
    const std::string& endpoint_key,
    const std::shared_ptr<SessionOwnerState::Establishing>& establishing,
    bbt::coroutine::Deadline deadline)
{
    using R = result<std::shared_ptr<SessionCore>>;
    const auto dialed = owner->engine->CoDial(target, {deadline});
    if (dialed.error) {
        return R::err(CallError(dialed.error, "connect failed", false));
    }
    detail::TcpChannel channel(dialed.connection);
    {

        /**
         * 建立中 conn 挂进占位记录：owner 封口可即时 RequestClose（物理关闭责任）。
         */
        std::lock_guard<std::mutex> lock(owner->mu);
        establishing->conn = channel.Raw();
        if (owner->closed) {
            channel.RequestClose();
            return R::err(Err(ErrorCode::Closed, "owner closed"));
        }
    }

    /**
     * 固定动作检查点：拨号已成功、额度已占，link 还没发布。Pause 在库内
     * 分片等待；失败/注入转成可见错误后，establishing 记录、额度与 conn 由
     * GetSession 的共同收尾归还——异常不越过该收尾块，也不在这里另造一条
     * 半截清理路径。
     */
    if (const auto gate = RunHookCheckpoint(
            *owner->hooks,
            HookPoint::OutboundBeforeHandshake,
            HookGate{owner.get(), nullptr, nullptr, &channel, deadline});
        !gate) {
        channel.RequestClose();
        return R::err(gate.error());
    }
    std::string peer_identity;
    const auto proposal = NextEpoch(*owner);
    const auto nonce = NextNonce(*owner);
    SessionFrameDecoder decoder(owner->options.MaxFrameBytes());
    auto epoch = OutboundHandshake(channel, owner->options, proposal, nonce, peer_identity, decoder, deadline);
    if (!epoch) {
        channel.RequestClose();
        return R::err(epoch.error());
    }
    auto session = std::make_shared<SessionCore>();
    session->owner = owner;
    session->channel = std::make_shared<detail::TcpChannel>(std::move(channel));
    session->peer_identity = std::move(peer_identity);
    session->endpoint = endpoint_key;
    session->outbound = true;
    session->epoch = epoch.value();
    auto installed = InstallSession(owner, session, endpoint_key, std::move(decoder), establishing);
    if (!installed) {

        /**
         * 安装拒绝（owner 封口/额度满）：拨号 conn 未发布，本端唯一责任
         * 人直接 RequestClose——不留给 engine 兜底。
         */
        session->channel->RequestClose();
    }
    return installed;
}

/**
 * 获取或建立 session（在调用方协程内；并发同 target 只建一次）。
 * 并发等待：CoWaiter 单等待位，不能放共享 Establishing 上——每个等待者
 * 用自己的 CoWaiter 轮询 done 位（建立通常毫秒级完成）。
 */
result<std::shared_ptr<SessionCore>> GetSession(
    std::shared_ptr<SessionOwnerState> owner,
    const std::string& endpoint_key,
    const net::Address& target,
    bbt::coroutine::Deadline deadline)
{
    using R = result<std::shared_ptr<SessionCore>>;
    std::shared_ptr<SessionOwnerState::Establishing> mine;
    bool establisher = false;
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        if (owner->closed) {
            return R::err(Err(ErrorCode::Closed, "owner closed"));
        }

        /**
         * 每次取会话都在 owner->mu 下顺带重驱各 link 的物理 Released
         * 归还（幂等）——conn 物理 Released 晚于 ExitDone 归零时，新调用
         * 是天然的收口观察点，不必另起清扫线程。先收集再驱动：ReleasedLocked
         * 会从 by_identity 擦除条目，持锁遍历中不能边迭代边擦除。
         */
        std::vector<std::shared_ptr<SessionCore>> live;
        live.reserve(owner->by_identity.size());
        for (auto& [id, s] : owner->by_identity) {
            (void)id;
            live.push_back(s);
        }
        for (auto& s : live) {
            s->DriveReleaseLocked();
        }
        if (auto it = owner->by_endpoint.find(endpoint_key); it != owner->by_endpoint.end()) {
            if (auto session = it->second.lock(); session && !session->dead.load()) {
                return R::ok(session);
            }
            owner->by_endpoint.erase(it);
        }
        if (auto it = owner->establishing.find(endpoint_key); it == owner->establishing.end()) {

            /**
             * 建立责任在拨号前的同一事务内先占额度：已安装成员 +
             * 其他 reserved 建立中记录达到 MaxSessions 时直接
             * Overloaded——不拨号、不产生物理连接、不消耗握手责任。
             * 同 endpoint 的等待者共用本条记录不另占位（不双占）。
             */
            std::size_t occupied = owner->by_identity.size();
            for (const auto& [key, est] : owner->establishing) {
                (void)key;
                if (est->reserved) {
                    ++occupied;
                }
            }
            if (occupied >= owner->options.MaxSessions()) {
                return R::err(Err(ErrorCode::Overloaded, "max sessions reached (pre-dial)"));
            }
            mine = std::make_shared<SessionOwnerState::Establishing>();

            /**
             * 建立中额度占位（同事务）
             */
            mine->reserved = true;
            owner->establishing[endpoint_key] = mine;
            establisher = true;
        } else {
            mine = it->second;
        }
    }
    if (!establisher) {
        auto self = bbt::coroutine::sync::CoWaiter::Create();
        for (;;) {
            {
                std::lock_guard<std::mutex> lock(owner->mu);
                if (mine->done) {
                    if (mine->session && !mine->session->dead.load()) {
                        return R::ok(mine->session);
                    }
                    return R::err(mine->error);
                }
            }
            if (RpcClock::now() >= deadline) {
                return R::err(
                    CallError(std::make_error_code(std::errc::timed_out), "session establish wait timed out", false));
            }
            const auto status = self->Wait({std::min(deadline, RpcClock::now() + 10ms)});
            if (status == bbt::coroutine::WaitStatus::Cancelled) {
                return R::err(Err(ErrorCode::Cancelled, "session establish wait cancelled"));
            }
        }
    }

    /**
     * 本协程执行拨号。
     * 异常全路径归还：EstablishOutbound 内部已把检查点注入与失败转成可见
     * 错误，这里再兜一层——任何逃出的异常都不能跳过下面的收尾块，否则
     * establishing 记录、reserved 额度与 done 位会永久留下。异常转成
     * InternalError 返回，收尾块照常执行。
     */
    result<std::shared_ptr<SessionCore>> made = R::err(Err(ErrorCode::InternalError, "establish did not run"));
    try {
        made = EstablishOutbound(owner, target, endpoint_key, mine, deadline);
    } catch (const std::exception& error) {
        made = R::err(Err(ErrorCode::InternalError, std::string("session establish threw: ") + error.what()));
    } catch (...) {
        made = R::err(Err(ErrorCode::InternalError, "session establish threw"));
    }
    net::Connection abandoned;
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        if (made) {
            mine->session = made.value();
        } else {
            mine->error = made.error();

            /**
             * 失败/异常路径的物理关闭责任兜底：EstablishOutbound 的正常失败
             * 分支已自行 RequestClose（幂等），这里覆盖「异常逃出、还没关」
             * 的情况——建立中 conn 不能因为记录被擦除而失去关闭责任人。
             */
            abandoned = mine->conn;
        }

        /**
         * 归还顺序（建立中责任）：conn 句柄先从记录上移除，使本条
         * shared_ptr 在临界区外随擦除析构——物理释放在额度归还前
         * 完成；reserved 占位随记录消亡，不是独立计数。擦除只匹配本次
         * 建立记录，不误删同名键上后来的建立者。
         */
        mine->conn = net::Connection{};
        if (auto it = owner->establishing.find(endpoint_key); it != owner->establishing.end() && it->second == mine) {
            owner->establishing.erase(it);
        }
        mine->done = true;
        owner->close_signal->Notify();
    }
    if (abandoned.Valid()) {
        (void)abandoned.Close();
    }
    return made;
}

// clang-format off
}
// clang-format on

/**
 * ServerV2 accept（入站 link → 握手 → owner session）
 */
void detail::ServeInbound(std::shared_ptr<ServerV2State> server, detail::TcpChannel channel)
{
    const auto conn_id = channel.Raw().Id();
    const auto deadline = RpcClock::now() + server->options.IncomingTimeout();
    auto owner = server->owner;

    /**
     * 固定动作检查点：conn 已 accept、握手还没开始。失败时本条握手责任仍
     * 在本协程——关闭 conn 并移除台账记录，不把退役留给更外层的兜底 catch。
     */
    if (const auto gate = RunHookCheckpoint(
            *owner->hooks,
            HookPoint::InboundBeforeHandshake,
            HookGate{owner.get(), server.get(), nullptr, &channel, deadline});
        !gate) {
        channel.RequestClose();
        std::lock_guard<std::mutex> lock(server->mu);
        server->handshakes.erase(conn_id);
        server->close_signal->Notify();
        return;
    }
    std::string peer_identity;
    const auto proposal = NextEpoch(*owner);
    const auto nonce = NextNonce(*owner);
    SessionFrameDecoder decoder(owner->options.MaxFrameBytes());
    auto epoch =
        detail::InboundHandshakeOnChannel(channel, owner->options, proposal, nonce, peer_identity, decoder, deadline);

    /**
     * 握手 conn 责任随本协程结束（任一路径）：台账唯一移除点。
     */
    const auto finish = [&] {
        std::lock_guard<std::mutex> lock(server->mu);
        server->handshakes.erase(conn_id);
        server->close_signal->Notify();
    };
    if (!epoch) {
        channel.RequestClose();
        finish();
        return;
    }
    auto session = std::make_shared<SessionCore>();
    session->owner = owner;
    session->server = server;
    session->channel = std::make_shared<detail::TcpChannel>(std::move(channel));
    session->peer_identity = std::move(peer_identity);
    session->outbound = false;
    session->epoch = epoch.value();

    /**
     * 第二个挂点：握手完成、link 还没进台账——封口与发布在这里竞争。
     * 失败时 conn 已在 session 上、台账记录仍在，两者都要退；这里就地收尾，
     * 因为外层 catch 只持有原始的 conn 局部量，不能替已移交的 session 关闭。
     */
    if (const auto gate = RunHookCheckpoint(
            *owner->hooks,
            HookPoint::InboundBeforePublish,
            HookGate{owner.get(), server.get(), nullptr, session->channel.get(), deadline});
        !gate) {
        session->channel->RequestClose();
        finish();
        return;
    }

    /**
     * 封口后不发布 link（物理关闭责任）：Close/Seal 与安装竞争时，本握手
     * 必须退役而不是溜进台账成为漏管连接。检查与 InstallSession 同一
     * owner->mu 事务之外不可行（InstallSession 内部已查 owner->closed）；
     * 这里先查 server.closed——Seal 置 closed 后到达的握手直接退役。
     */
    bool publish = false;
    {
        std::lock_guard<std::mutex> lock(server->mu);
        if (server->closed || server->stopping) {

            /**
             * finish() 自身取 server->mu——不能在持锁内联调用（自死锁）。
             */
            server->handshakes.erase(conn_id);
            server->close_signal->Notify();
        } else {
            publish = true;
        }
    }
    if (!publish) {
        session->channel->RequestClose();
        return;
    }
    auto installed = InstallSession(owner, session, {}, std::move(decoder));
    if (!installed) {
        session->channel->RequestClose();
        finish();
        return;
    }
    auto live = installed.value();
    {
        std::lock_guard<std::mutex> lock(server->mu);
        ++server->accepted;
        server->links.push_back(live);

        /**
         * 顺带清扫已死弱引用。
         */
        server->links.erase(
            std::remove_if(server->links.begin(), server->links.end(), [](const auto& w) { return w.expired(); }),
            server->links.end());
        server->handshakes.erase(conn_id);
        server->close_signal->Notify();
    }
}

namespace {

/**
 * reader 上的 admission 拒绝路径——唯一 reader 不能调用会挂起等写终态
 * 的 Complete/Fail（背压会阻塞 reader，无法继续解后续 Response/System 帧）。
 * 本函数直接构造错误终态帧入队，不等待写终态：写责任仍由 WriterLoop/Kill
 * 在真实终态唯一结算，reader 立即返回继续 Read。terminal 已在本路径
 * 置位（调用方先取 IsTerminal），这里不再二次判定。
 */
void PostReplyErrorNoWait(
    const std::shared_ptr<SessionCore>& session, const detail::ReplyCompletionImpl& impl, Error error)
{
    if (!session || session->dead.load()) {
        return;
    }
    SessionFrame frame;
    frame.session_epoch = session->epoch;
    frame.has_outcome = true;
    frame.success = false;
    frame.error = std::move(error);
    frame.reply = ReplyMode::Required;
    if (impl.kind == FrameKind::SendAccepted) {
        frame.kind = FrameKind::SendAccepted;
        frame.traffic = TrafficClass::System;
        frame.message_id = impl.message_id;
        frame.reply = ReplyMode::None;
    } else {
        frame.kind = FrameKind::Response;
        frame.traffic = impl.lane;
        frame.request_id = impl.request_id;
        frame.service = impl.service;
        frame.method = impl.method;
        frame.request_schema = impl.request_schema;
        frame.response_schema = impl.response_schema;
    }
    auto bytes = detail::RpcFrameProtocol::Encode(frame, session->owner->options.MaxFrameBytes());
    if (!bytes) {
        return;
    }
    SessionCore::OutItem item;
    item.bytes.assign(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
    item.lane = frame.traffic;
    item.control_response = true;
    item.deadline = RpcClock::now() + 5s;
    item.settle = std::make_shared<SessionCore::OutItem::Settle>();

    /**
     * 不挂 reader：写终态由 settle/WriterLoop 结算，不入队失败也只是
     * 该条回复终态丢失——link 与后续帧继续进展。
     */
    (void)session->Enqueue(std::move(item));
}

// clang-format off
}
// clang-format on

/**
 * detail/Session.hpp 测试接缝（非成员，转调匿名 InboundHandshake）
 */
result<std::uint64_t> detail::InboundHandshakeOnChannel(
    detail::ICoChannel& channel,
    const ClientOptions& options,
    std::uint64_t proposal,
    std::uint64_t nonce,
    std::string& out_peer_identity,
    SessionFrameDecoder& out_decoder,
    bbt::coroutine::Deadline deadline)
{
    return InboundHandshake(channel, options, proposal, nonce, out_peer_identity, out_decoder, deadline);
}

namespace {

/**
 * "tcp://host:port" → numeric host/port（file-local，非成员）。
 */
result<void> ParseTarget(std::string_view target, std::string& host, std::uint16_t& port)
{
    const std::string value(target);
    if (value.size() < 8 || value.compare(0, 6, "tcp://") != 0) {
        return ErrV(ErrorCode::InvalidArgument, "target must be tcp://host:port");
    }
    return detail::ParseTcpEndpoint(value.substr(6), host, port);
}

/**
 * 已接纳调用的一次性责任：与 owner 封口同门登记，作用域结束（含所有
 * 早退/异常路径）归还并唤醒 Shutdown 等待者。
 */
struct OwnerOperationGuard {
    std::shared_ptr<SessionOwnerState> owner;

    ~OwnerOperationGuard() { detail::SessionOwnerEndOperation(owner); }
};

// clang-format off
}
// clang-format on

namespace {

/**
 * v2 出站关闭原因的统一附加点：Call/Send 返回失败且 owner 已发布原因时，
 * 在调用方可见的返回点把首个关闭原因附到 Error.details；内部各失败出口
 * 只保留失败分类（未提交 Closed / 已提交 OutcomeUnknown+request_phase /
 * 超时 / 取消），不分别补原因。错误本身即关闭原因或已含 close_reason 键
 * 时不重复附加；成功结果不改。
 */
void AttachOwnerCloseReason(const std::shared_ptr<SessionOwnerState>& owner, Error& error)
{
    const auto reason = detail::SessionOwnerCloseReason(*owner);
    if (!reason) {
        return;
    }
    if (error.code == reason->code && error.message == reason->message) {
        return;
    }
    for (const auto& detail : error.details) {
        if (detail.first == "close_reason") {
            return;
        }
    }
    AttachCloseReason(error, *reason);
}

result<Response> CallImpl(
    const std::shared_ptr<SessionOwnerState>& owner,
    std::string_view target,
    Request request,
    const CallOptions& options)
{
    using R = result<Response>;
    if (!bbt::coroutine::IsInCoroutineContext())
        return R::err(Err(ErrorCode::InvalidContext, "coroutine context required"));
    Error closed_error;
    if (!detail::SessionOwnerBeginOperation(owner, closed_error)) {
        return R::err(std::move(closed_error));
    }
    OwnerOperationGuard operation_guard{owner};
    if (const auto gate = CheckCall(options.deadline)) {
        if (gate == std::errc::operation_not_permitted) {
            return R::err(Err(ErrorCode::InvalidContext, "coroutine context required"));
        }
        return R::err(CallError(gate, "call gate", false));
    }
    if (request.Traffic() == TrafficClass::System || request.Reply() != ReplyMode::Required) {
        return R::err(Err(ErrorCode::InvalidArgument, "Call requires Service/Actor + ReplyRequired"));
    }
    std::string host;
    std::uint16_t port = 0;
    if (auto parsed = ParseTarget(target, host, port); !parsed) {
        return R::err(parsed.error());
    }
    std::error_code addr_error;
    const auto address = net::Address::Ip(host, static_cast<int>(port), addr_error);
    if (addr_error || !address.Valid()) {
        return R::err(Err(ErrorCode::InvalidArgument, "invalid target address"));
    }

    /**
     * 同时拨号 fencing 竞速：GetSession 返回的 session 可能在交付/登记前
     * 被选举退役（dead）。by_endpoint 已被胜者接管，重取落到存活 link；
     * pending 登记的 dead 拒绝同样回环一次，不循环拨号。
     */
    std::shared_ptr<SessionCore> session;
    std::shared_ptr<SessionCore::PendingCall> pending;
    bool inserted = false;
    for (unsigned attempt = 0; attempt != 2 && !inserted; ++attempt) {
        auto session_result = GetSession(owner, std::string(target), address, options.deadline);
        if (!session_result) {
            return R::err(session_result.error());
        }
        session = std::move(session_result.value());
        if (session->dead.load()) {
            continue;
        }

        /**
         * request_id：空时 session 生成唯一值。
         */
        if (request.RequestId().empty()) {
            request.SetRequestId("r" + std::to_string(session->seq_counter.fetch_add(1)));
        }
        const auto request_id = request.RequestId();
        auto budget = Budget(options.deadline);
        if (!budget) {
            return R::err(budget.error());
        }
        auto frame = ToSessionFrame(request, FrameKind::Call, session->epoch, budget.value());
        if (!frame) {
            return R::err(frame.error());
        }
        auto bytes = detail::RpcFrameProtocol::Encode(frame.value(), owner->options.MaxFrameBytes());
        if (!bytes) {
            return R::err(bytes.error());
        }

        pending = std::make_shared<SessionCore::PendingCall>();
        pending->service = request.Service();
        pending->method = request.Method();
        pending->request_schema = request.RequestSchema();
        pending->response_schema = request.ResponseSchema();
        {
            std::lock_guard<std::mutex> lock(session->pending_mu);
            if (session->dead.load()) {
                continue;
            }
            if (session->calls.count(request_id)) {
                return R::err(Err(ErrorCode::InvalidArgument, "duplicate request_id on session"));
            }
            session->calls[request_id] = pending;
        }
        inserted = true;
        {
            std::lock_guard<std::mutex> lock(session->out_mu);
            ++session->out_lanes[LaneIndex(request.Traffic())].inflight;
        }
        SessionCore::OutItem item;
        item.bytes.assign(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
        item.lane = request.Traffic();
        item.key = request_id;
        item.deadline = options.deadline;
        auto enqueued = session->Enqueue(std::move(item));
        if (!enqueued) {
            std::lock_guard<std::mutex> lock(session->pending_mu);
            session->calls.erase(request_id);
            {
                std::lock_guard<std::mutex> slock(session->out_mu);
                --session->out_lanes[LaneIndex(request.Traffic())].inflight;
            }

            /**
             * 退役会话的 Enqueue Closed：同样重取一次落到存活 link。
             */
            if (enqueued.error().code == ErrorCode::Closed && session->dead.load()) {
                inserted = false;
                continue;
            }
            return R::err(enqueued.error());
        }
    }
    if (!inserted) {
        return R::err(Err(ErrorCode::Closed, "session fenced during dial"));
    }
    const auto request_id = request.RequestId();
    owner->acceptance_pending.fetch_add(1);

    /**
     * 先通知后等待的丢唤醒窗口由持久 done 谓词 + 登记后复查关闭。
     * Dispatch/写终态在 pending_mu 下先写 outcome/done 再 Notify；on_registered 在
     * waiter 已挂起登记后于同一锁复查 done——done 已真即同步 Notify 走
     * PENDING 早到路径，不依赖那次 Notify 的时序，永不丢唤醒。
     */
    const auto status = pending->waiter->WaitWithCallback({options.deadline}, [&] {
        std::lock_guard<std::mutex> lock(session->pending_mu);
        if (pending->done) {
            pending->waiter->Notify();
        }
        return true;
    });
    owner->acceptance_pending.fetch_sub(1);
    if (status == bbt::coroutine::WaitStatus::Completed) {
        result<Response> outcome = [&]() -> result<Response> {
            std::lock_guard<std::mutex> lock(session->pending_mu);
            return std::move(pending->outcome);
        }();
        std::lock_guard<std::mutex> lock(session->out_mu);
        auto& lane = session->out_lanes[LaneIndex(request.Traffic())];
        --lane.inflight;
        ++lane.finished;
        return outcome;
    }

    /**
     * 超时/取消：移除 pending，阶段事实结算。
     */
    bool committed;
    {
        std::lock_guard<std::mutex> lock(session->pending_mu);
        session->calls.erase(request_id);
        committed = pending->committed;
    }
    {
        std::lock_guard<std::mutex> lock(session->out_mu);
        auto& lane = session->out_lanes[LaneIndex(request.Traffic())];
        --lane.inflight;
        ++lane.dropped;
    }
    if (status == bbt::coroutine::WaitStatus::Cancelled) {
        return R::err(ErrPhase(
            ErrorCode::Cancelled,
            "call cancelled",
            committed ? RequestPhase::RequestCommitted : RequestPhase::NotStarted));
    }
    if (committed) {
        return R::err(
            ErrPhase(ErrorCode::OutcomeUnknown, "call timed out after commit", RequestPhase::RequestCommitted));
    }
    return R::err(Err(ErrorCode::TimedOut, "call timed out"));
}

result<void> SendImpl(
    const std::shared_ptr<SessionOwnerState>& owner,
    std::string_view target,
    Request message,
    const SendOptions& options)
{
    using R = result<void>;
    if (!bbt::coroutine::IsInCoroutineContext())
        return R::err(Err(ErrorCode::InvalidContext, "coroutine context required"));
    Error closed_error;
    if (!detail::SessionOwnerBeginOperation(owner, closed_error)) {
        return R::err(std::move(closed_error));
    }
    OwnerOperationGuard operation_guard{owner};
    if (auto valid = options.Validate(); !valid) {
        return R::err(valid.error());
    }
    if (const auto gate = CheckCall(options.Deadline())) {
        if (gate == std::errc::operation_not_permitted) {
            return R::err(Err(ErrorCode::InvalidContext, "coroutine context required"));
        }
        return R::err(CallError(gate, "send gate", false));
    }
    if (message.Traffic() == TrafficClass::System || message.Reply() != ReplyMode::None) {
        return R::err(Err(ErrorCode::InvalidArgument, "Send requires Service/Actor + ReplyNone"));
    }
    std::string host;
    std::uint16_t port = 0;
    if (auto parsed = ParseTarget(target, host, port); !parsed) {
        return R::err(parsed.error());
    }
    std::error_code addr_error;
    const auto address = net::Address::Ip(host, static_cast<int>(port), addr_error);
    if (addr_error || !address.Valid()) {
        return R::err(Err(ErrorCode::InvalidArgument, "invalid target address"));
    }

    /**
     * 同 Call：fencing 竞速下重取一次落到存活 link；pending 登记的
     * dead 拒绝同样回环一次，不循环拨号。
     */
    std::shared_ptr<SessionCore> session;
    std::shared_ptr<SessionCore::PendingSend> pending;
    bool inserted = false;
    for (unsigned attempt = 0; attempt != 2 && !inserted; ++attempt) {
        auto session_result = GetSession(owner, std::string(target), address, options.Deadline());
        if (!session_result) {
            return R::err(session_result.error());
        }
        session = std::move(session_result.value());
        if (session->dead.load()) {
            continue;
        }

        if (message.MessageId().empty()) {
            message.SetMessageId("m" + std::to_string(session->seq_counter.fetch_add(1)));
        }
        const auto message_id = message.MessageId();
        auto budget = Budget(options.Deadline());
        if (!budget) {
            return R::err(budget.error());
        }
        auto frame = ToSessionFrame(
            message,
            FrameKind::Send,
            session->epoch,
            budget.value(),
            options.RequiresRemoteAcceptance() ? std::optional<bool>{true} : std::nullopt);
        if (!frame) {
            return R::err(frame.error());
        }
        auto bytes = detail::RpcFrameProtocol::Encode(frame.value(), owner->options.MaxFrameBytes());
        if (!bytes) {
            return R::err(bytes.error());
        }

        const auto lane_index = LaneIndex(message.Traffic());
        if (options.RequiresRemoteAcceptance()) {
            pending = std::make_shared<SessionCore::PendingSend>();
            std::lock_guard<std::mutex> lock(session->pending_mu);
            if (session->dead.load()) {
                pending.reset();
                continue;
            }
            if (session->sends.count(message_id)) {
                return R::err(Err(ErrorCode::InvalidArgument, "duplicate message_id on session"));
            }
            session->sends[message_id] = pending;
        }
        inserted = true;
        {
            std::lock_guard<std::mutex> lock(session->out_mu);
            ++session->out_lanes[lane_index].inflight;
        }
        SessionCore::OutItem item;
        item.bytes.assign(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size());
        item.lane = message.Traffic();
        item.key = message_id;
        item.deadline = options.Deadline();
        auto enqueued = session->Enqueue(std::move(item));
        if (!enqueued) {
            if (pending) {
                std::lock_guard<std::mutex> lock(session->pending_mu);
                session->sends.erase(message_id);
            }
            {
                std::lock_guard<std::mutex> lock(session->out_mu);
                --session->out_lanes[lane_index].inflight;
            }
            if (enqueued.error().code == ErrorCode::Closed && session->dead.load()) {
                inserted = false;
                pending.reset();
                continue;
            }
            return R::err(enqueued.error());
        }
    }
    if (!inserted) {
        return R::err(Err(ErrorCode::Closed, "session fenced during dial"));
    }
    const auto message_id = message.MessageId();
    const auto lane_index = LaneIndex(message.Traffic());
    if (!pending) {

        /**
         * 本地接纳即成功（ADR §4.1：不要求远端确认）。
         */
        std::lock_guard<std::mutex> lock(session->out_mu);
        --session->out_lanes[lane_index].inflight;
        ++session->out_lanes[lane_index].finished;
        return R::ok();
    }

    /**
     * 同 Call——on_registered 在 waiter 登记后于 pending_mu 复查
     * 持久 done，关闭先通知后等待的丢唤醒窗口。
     */
    const auto status = pending->waiter->WaitWithCallback({options.Deadline()}, [&] {
        std::lock_guard<std::mutex> lock(session->pending_mu);
        if (pending->done) {
            pending->waiter->Notify();
        }
        return true;
    });
    if (status == bbt::coroutine::WaitStatus::Completed) {
        result<void> outcome = [&]() -> result<void> {
            std::lock_guard<std::mutex> lock(session->pending_mu);
            return std::move(pending->outcome);
        }();
        std::lock_guard<std::mutex> lock(session->out_mu);
        auto& lane = session->out_lanes[lane_index];
        --lane.inflight;
        ++lane.finished;
        return outcome;
    }
    bool committed = false;
    {
        std::lock_guard<std::mutex> lock(session->pending_mu);
        session->sends.erase(message_id);
        committed = pending->committed;
    }
    {
        std::lock_guard<std::mutex> lock(session->out_mu);
        auto& lane = session->out_lanes[lane_index];
        --lane.inflight;
        ++lane.dropped;
    }
    if (status == bbt::coroutine::WaitStatus::Cancelled) {
        return R::err(ErrPhase(
            ErrorCode::Cancelled,
            "send cancelled",
            committed ? RequestPhase::RequestCommitted : RequestPhase::NotStarted));
    }
    if (committed) {
        return R::err(ErrPhase(
            ErrorCode::OutcomeUnknown, "send acceptance timed out after commit", RequestPhase::RequestCommitted));
    }
    return R::err(Err(ErrorCode::TimedOut, "send acceptance timed out"));
}

// clang-format off
}
// clang-format on

result<Response> detail::SessionOwnerCall(
    const std::shared_ptr<SessionOwnerState>& owner,
    std::string_view target,
    Request request,
    const CallOptions& options)
{
    auto outcome = CallImpl(owner, target, std::move(request), options);
    if (!outcome) {
        AttachOwnerCloseReason(owner, outcome.error());
    }
    return outcome;
}

result<void> detail::SessionOwnerSend(
    const std::shared_ptr<SessionOwnerState>& owner,
    std::string_view target,
    Request message,
    const SendOptions& options)
{
    auto outcome = SendImpl(owner, target, std::move(message), options);
    if (!outcome) {
        AttachOwnerCloseReason(owner, outcome.error());
    }
    return outcome;
}

/**
 * CoRpcClient::Stats 的真源在 CoRpcClient.cc；本函数由 CoRpcClient.cc 转调。
 */
ClientStats detail::SessionOwnerStats(const std::shared_ptr<SessionOwnerState>& owner) noexcept
{
    ClientStats out;
    out.sessions = owner->sessions.load();
    out.links = owner->links.load();
    out.remote_acceptance_pending = owner->acceptance_pending.load();
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        for (const auto& [id, session] : owner->by_identity) {
            (void)id;
            std::lock_guard<std::mutex> slock(session->out_mu);
            for (std::size_t i = 0; i != kLaneCount; ++i) {
                auto& dst = i == 0 ? out.system : i == 1 ? out.service : out.actor;
                const auto& src = session->out_lanes[i];
                dst.queued += src.queued;
                dst.dequeued += src.dequeued;
                dst.inflight += src.inflight;
                dst.finished += src.finished;
                dst.dropped += src.dropped;
                dst.bytes_queued += src.bytes_queued;
            }
        }
    }
    return out;
}

// clang-format off
}
// clang-format on
