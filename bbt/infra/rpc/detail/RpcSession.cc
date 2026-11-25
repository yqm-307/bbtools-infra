// 单条 link 的收发泵、写终态和退役。
#include <bbt/infra/rpc/detail/RpcSessionState.hpp>
#include <bbt/infra/rpc/detail/Session.hpp>

#include <algorithm>
#include <cassert>
#include <functional>
#include <utility>
#include <vector>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

namespace bbt::infra::rpc {

using namespace std::chrono_literals;
using bbt::coroutine::WaitStatus;
using bbt::coroutine::sync::CoWaiter;

namespace {

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

} // namespace

// Only the task's exit (or failed registration) returns its responsibility.
bool SessionCore::Submit(std::size_t stack, std::function<void()> fn)
{
    auto session = shared_from_this();
    responsibilities.fetch_add(1);
    bool registered = false;
    try {
        // bbt-lint: ignore-next-line coroutine-sugar -- session tasks use the caller-selected stack size
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
    } catch (...) {
        ExitDone();
        throw;
    }
    if (!registered) {
        ExitDone();
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

void SessionCore::OutItem::Settle::Publish(result<void> outcome)
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_outcome) {
            return;
        }
        m_outcome.emplace(std::move(outcome));
    }
    m_waiter->Notify();
}

result<void> SessionCore::OutItem::Settle::Wait(bbt::coroutine::Deadline deadline)
{
    // 先由 waiter 裁定预取消和期限，登记后再取已发布的写结果。
    const auto status = m_waiter->WaitWithCallback({deadline}, [this] {
        bool ready;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            ready = m_outcome.has_value();
        }
        if (ready) {
            m_waiter->Notify();
        }
        return true;
    });
    switch (status) {
    case WaitStatus::Completed: {
        std::lock_guard<std::mutex> lock(m_mutex);
        assert(m_outcome.has_value());
        return *m_outcome;
    }
    case WaitStatus::Cancelled:
        return ErrV(ErrorCode::Cancelled, "reply write wait cancelled");
    case WaitStatus::TimedOut:
        return ErrV(ErrorCode::TimedOut, "reply write not settled in budget");
    default:
        return result<void>::err(WaitFailure(status, "reply write wait failed"));
    }
}

/* 队列或 writer 中的写责任在真实终态归还一次。等待方超时不会代为出账。 */
void SessionCore::NoteWriteSettled(OutItem& item, result<void> outcome)
{
    if (item.settle) {
        item.settle->Publish(std::move(outcome));
    }
    const auto previous = writes_outstanding.fetch_sub(1);
    assert(previous > 0);
    if (previous == 1) {
        if (auto server = this->server.lock()) {
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
    if (!item.pending) {
        return;
    }
    std::shared_ptr<CoWaiter> waiter;
    {
        std::lock_guard<std::mutex> lock(pending_mu);
        if (auto it = calls.find(item.key); it != calls.end() && it->second == item.pending) {
            it->second->outcome = result<Response>::err(error);
            it->second->done = true;
            waiter = it->second->waiter;
            calls.erase(it);
        } else if (auto si = sends.find(item.key); si != sends.end() && si->second == item.pending) {
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
            const auto status = out_signal->WaitWithCallback({RpcClock::now() + 25ms}, [this] {
                bool ready;
                {
                    std::lock_guard<std::mutex> lock(out_mu);
                    ready = dead.load() || std::any_of(std::begin(out_q), std::end(out_q), [](const auto& queue) {
                                return !queue.empty();
                            });
                }
                if (ready) {
                    out_signal->Notify();
                }
                return true;
            });
            if (dead.load()) {
                return;
            }
            if (status != WaitStatus::Completed && status != WaitStatus::TimedOut) {
                // 唯一 writer 无法继续等候，沿 link 终态路径归还队列和 pending。
                Kill({});
                return;
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
            const auto gate = detail::RunHookCheckpoint(
                *owner->hooks, HookPoint::BeforeWrite, HookGate{owner.get(), nullptr, this, channel.get(), deadline});
            if (item.pending) {
                bool withdrawn;
                {
                    std::lock_guard<std::mutex> lock(pending_mu);
                    withdrawn = item.pending->phase == PendingWrite::Phase::Withdrawn || item.pending->done;
                    if (!withdrawn && gate) {
                        item.pending->phase = PendingWrite::Phase::Committed;
                    }
                }
                if (withdrawn) {
                    NoteWriteSettled(item, ErrV(ErrorCode::Cancelled, "request withdrawn before write"));
                    continue;
                }
            }
            if (!gate) {
                failure = gate.error();
            } else {
                write_attempted = true;
                const auto total = item.bytes.size();
                const auto report = channel->WriteBytes(std::move(item.bytes), {deadline});
                const auto settled = detail::TcpSettleWrite(report, total, "frame write failed");
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

void SessionCore::FailPendingOnLinkLoss()
{
    std::vector<std::shared_ptr<CoWaiter>> wake;
    {
        std::lock_guard<std::mutex> lock(pending_mu);
        const auto fail_all = [&wake](auto& table) {
            for (auto it = table.begin(); it != table.end();) {
                auto& pending = it->second;
                const bool committed = pending->phase == PendingWrite::Phase::Committed;
                auto error =
                    committed
                        ? ErrPhase(
                              ErrorCode::OutcomeUnknown, "session lost after commit", RequestPhase::RequestCommitted)
                        : Err(ErrorCode::Closed, "session closed before commit");
                pending->outcome = decltype(pending->outcome)::err(std::move(error));
                pending->done = true;
                wake.push_back(pending->waiter);
                it = table.erase(it);
            }
        };
        fail_all(calls);
        fail_all(sends);
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
     * committed 但无可信终态 → OutcomeUnknown；未提交 → Closed。
     */
    FailPendingOnLinkLoss();

    if (auto server = this->server.lock()) {
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

bool SessionCore::ExitDone()
{
    if (responsibilities.fetch_sub(1) != 1) {
        return false;
    }
    // 回收可能晚于任务退出；接纳、观察和 Close 会重查租约。
    owner->close_signal->Notify();
    if (auto server = this->server.lock()) {
        server->close_signal->Notify();
    }
    return true;
}

void SessionCore::TakeHandshakeDecoder(SessionFrameDecoder&& handshake_decoder)
{
    decoder = std::move(handshake_decoder);
    decoder.SetHelloPhase(false);
}

void SessionCore::NoteOperationStarted(TrafficClass traffic)
{
    std::lock_guard<std::mutex> lock(out_mu);
    ++out_lanes[LaneIndex(traffic)].inflight;
}

void SessionCore::NoteEnqueueFailed(TrafficClass traffic)
{
    std::lock_guard<std::mutex> lock(out_mu);
    --out_lanes[LaneIndex(traffic)].inflight;
}

void SessionCore::NoteOperationFinished(TrafficClass traffic, bool published)
{
    std::lock_guard<std::mutex> lock(out_mu);
    auto& lane = out_lanes[LaneIndex(traffic)];
    --lane.inflight;
    if (published) {
        ++lane.finished;
    } else {
        ++lane.dropped;
    }
}

void SessionCore::AccumulateOutboundStats(ClientStats& stats) const
{
    std::lock_guard<std::mutex> lock(out_mu);
    for (std::size_t i = 0; i != kLaneCount; ++i) {
        auto& dst = i == 0 ? stats.system : i == 1 ? stats.service : stats.actor;
        const auto& src = out_lanes[i];
        dst.queued += src.queued;
        dst.dequeued += src.dequeued;
        dst.inflight += src.inflight;
        dst.finished += src.finished;
        dst.dropped += src.dropped;
        dst.bytes_queued += src.bytes_queued;
    }
}

WaitStatus SessionCore::WaitPending(const PendingWrite& pending, bbt::coroutine::Deadline deadline)
{
    // 登记后复查持久终态，补发早于 waiter 登记的通知。
    return pending.waiter->WaitWithCallback({deadline}, [&] {
        std::lock_guard<std::mutex> lock(pending_mu);
        if (pending.done) {
            pending.waiter->Notify();
        }
        return true;
    });
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

} // namespace bbt::infra::rpc
