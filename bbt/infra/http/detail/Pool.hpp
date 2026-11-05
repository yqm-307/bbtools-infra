#pragma once

// 内建连接池（内部部件，仅 http 模块实现 TU 包含）。契约要点：
//   - origin（scheme/host/port）匹配；HTTP/1.1 一条连接一次一个活动请求。
//   - 全池物理预算 entries.size()：Dialing/Active/Idle/Retiring 同列表；
//     逻辑摘除不释放名额；成功连接只按 Released() 出账，失败拨号只按
//     ReclaimReceipt::Released() 出账，二者都由 net 在真实物理回收后发布。
//   - Idle stale 检查只用公开观察：Closed/ReadEof/ReadError/ReadableBytes>0；
//     keepalive 过期惰性判定（sweep 内执行，无后台 reaper）。
//   - 排队：有界 max_queue（满即 Overloaded）；CoWaiter 登记→锁内复查→Notify，
//     锁永不跨挂起；handoff 在锁内完成，恢复后锁内领取，孤儿 handoff 回池。
//     分配行为：快路径先扫同 origin 空闲；归还把名额交给队首同 origin 等待者；
//     不承诺严格 FIFO 公平。
//   - 排队者唤醒源：同 origin 完整归还 handoff、封口、池超时，以及有界重试
//     切片（默认 20ms）——预算释放（退役连接观察到 Closed 出账）没有事件源，
//     用切片重查保证活性；只影响排队者，不在快路径引入轮询。
//   - 生命周期单向前：Open → Draining → Closed。Acquire 在同一把锁内检查
//     状态并 inflight++；非 Open 即拒绝（Close 原因或通用 Closed）。排空只
//     看 inflight（在途 Acquire + 未归还租借 + 排队等待者），不读条目
//     phase；Retiring 属延迟物理回收，不算未终结。Close 幂等、首个原因
//     固定、只执行一次「失败在途」，Shutdown 与 Close 不共用同一关闭位。
//   - 失败/超时/取消/协议错/EOF 的连接一律退役，不自动重放请求。

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/infra/NetError.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/Engine.hpp>

#include <bbt/infra/http/Client.hpp>
#include <bbt/infra/http/detail/Http11.hpp>

namespace bbt::infra::http {
namespace detail {

struct Origin {
    std::string scheme;
    std::string host;
    std::uint16_t port{0};

    bool operator==(const Origin& other) const noexcept
    {
        return port == other.port && scheme == other.scheme && host == other.host;
    }
    bool operator!=(const Origin& other) const noexcept
    {
        return !(*this == other);
    }
};

class ConnectionPool {
    struct State;

public:
    // 独占在途归还责任；共享状态使租借可安全活过池句柄，不保留裸 pool 引用。
    class Lease {
    public:
        Lease() = default;
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        Lease(Lease&&) noexcept = default;
        Lease& operator=(Lease&& other) noexcept
        {
            if (this != &other) {
                Release(false);
                connection = std::move(other.connection);
                m_state = std::move(other.m_state);
            }
            return *this;
        }
        ~Lease() noexcept
        {
            Release(false);
        }

        // 只有完整响应确认可复用后才传 healthy；默认析构保守退役，不挂起。
        void Release(bool healthy) noexcept;

        std::shared_ptr<Http11Connection> connection;

    private:
        friend class ConnectionPool;
        Lease(std::shared_ptr<State> state, std::shared_ptr<Http11Connection> http) noexcept
            : connection(std::move(http)), m_state(std::move(state))
        {
        }

        std::shared_ptr<State> m_state;
    };

    ConnectionPool(std::shared_ptr<net::Engine> engine, PoolLimits limits, std::uint64_t max_body_bytes,
                   std::optional<std::uint64_t> max_header_bytes)
        : m_state(std::make_shared<State>(std::move(engine), limits, max_body_bytes, max_header_bytes))
    {
    }

    // 借用一条 origin 匹配连接：快路径 / 预算内新建（Engine::CoDial）/ 有界排队。
    // 同一把锁内检查状态并 inflight++（非 Open 即拒绝）；本入口失败即
    // inflight--，成功则把这份在途责任随租借移交归还方（Release/Retire）。
    bbt_co_async(result<Lease>)
        Acquire(const Origin& origin, const net::Address& address, const PhaseDeadlines& deadlines)
    {
        const auto state = m_state;
        if (bbt::coroutine::GetLocalCoroutineId() == 0) {
            return result<Lease>::err(MakeError(ErrorCode::InvalidContext, "http pool: coroutine context required"));
        }
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            if (state->phase != Phase::Open) {
                return result<Lease>::err(state->ClosedErrorLocked());
            }
            ++state->inflight;
        }
        auto lease_result = AcquireBody(state, origin, address, deadlines);
        if (!lease_result) {
            // 唯一出口归还本操作的在途名额；成功路径的责任已随租借移交。
            std::lock_guard<std::mutex> lock(state->mutex);
            --state->inflight;
            state->cv.notify_all();
        }
        return lease_result;
    }

    // 立即逻辑封口：拒绝新请求、对全部条目（含在飞/拨号）请求关闭并退役、
    // 唤醒全部排队等待者与排空等待者、发布首个关闭原因；幂等不可重开且
    // 「失败在途」恰好执行一次（Draining 下同样执行，不与 Shutdown 共用位）。
    // 不等待在飞请求，不关共享 Engine，不假装物理排空。
    void Close(Error reason) noexcept
    {
        const auto state = m_state;
        std::deque<std::shared_ptr<Waiter>> pending;
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            if (!state->close_reason) {
                state->close_reason = std::move(reason);
            }
            if (state->phase == Phase::Closed) {
                // 幂等：物理退役已执行过；首个原因固定，唤醒兜底仍发。
                state->cv.notify_all();
                return;
            }
            state->phase = Phase::Closed;
            for (auto& entry : state->entries) {
                state->RetireLocked(*entry);
            }
            pending.swap(state->waiters);
            for (auto& waiter : pending) {
                waiter->in_queue = false;
            }
            state->cv.notify_all();
        }
        for (auto& waiter : pending) {
            waiter->gate->Notify();
        }
    }

    // 置 Draining（拒新）并等 inflight 归零：在途 Acquire、未归还租借与排队
    // 等待者都计入，不看条目 phase。有限期限超时返回 TimedOut 且保持拒新；
    // 等待中被 Close 且仍有在途则返回首个关闭原因，Close 前已排空则 ok；
    // 重复调用时仍 Draining 且有在途返回 Overloaded（Busy），已排空则 ok；
    // 已 Closed 返回首个关闭原因。不保存「到达时是否已排空」快照。
    result<void> Shutdown(bbt::coroutine::Deadline deadline)
    {
        const auto state = m_state;
        std::deque<std::shared_ptr<Waiter>> pending;
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            if (state->phase == Phase::Closed) {
                return result<void>::err(state->ClosedErrorLocked());
            }
            if (state->phase == Phase::Draining) {
                if (state->inflight == 0) {
                    return result<void>::ok();
                }
                return result<void>::err(MakeError(ErrorCode::Overloaded, "http pool: shutdown already in progress"));
            }
            state->phase = Phase::Draining;
            pending.swap(state->waiters);
            for (auto& waiter : pending) {
                waiter->in_queue = false;
            }
            state->cv.notify_all();
        }
        // 排队者已被拒绝：唤醒它们让出等待名额（其恢复路径只读 phase）。
        for (auto& waiter : pending) {
            waiter->gate->Notify();
        }

        std::unique_lock<std::mutex> lock(state->mutex);
        for (;;) {
            // 先排空先胜：当前已排空即 ok，不被随后到达的 Close 追改；
            // 等待中被 Close 且仍有在途 → 返回首个关闭原因。
            if (state->inflight == 0) {
                return result<void>::ok();
            }
            if (state->close_reason) {
                return result<void>::err(*state->close_reason);
            }
            if (deadline != bbt::coroutine::Deadline::max() && std::chrono::steady_clock::now() >= deadline) {
                return result<void>::err(MakeError(ErrorCode::TimedOut, "http pool: shutdown deadline expired"));
            }
            state->cv.wait_until(lock, deadline);
        }
    }

    bool IsClosed() const noexcept
    {
        const auto state = m_state;
        std::lock_guard<std::mutex> lock(state->mutex);
        return state->phase != Phase::Open;
    }

    std::optional<Error> CloseReason() const noexcept
    {
        const auto state = m_state;
        std::lock_guard<std::mutex> lock(state->mutex);
        return state->close_reason;
    }

    // 生命周期：单向前。Close 在任何状态下都执行且仅执行一次「失败在途」。
    enum class Phase { Open, Draining, Closed };

    PoolStats Stats() const noexcept
    {
        const auto state = m_state;
        std::lock_guard<std::mutex> lock(state->mutex);
        // 内省触发同一惰性维护（sweep 只做本地状态检查与 RequestClose，不挂起、
        // 不做 I/O），使退役出账在没有新请求时也可观察。
        state->SweepLocked();
        PoolStats stats;
        for (const auto& entry : state->entries) {
            switch (entry->phase) {
            case Entry::Phase::Active:
                ++stats.active;
                break;
            case Entry::Phase::Idle:
                ++stats.idle;
                break;
            case Entry::Phase::Dialing:
                ++stats.dialing;
                break;
            case Entry::Phase::Retiring:
                ++stats.retiring;
                break;
            }
        }
        stats.budget = state->entries.size();
        stats.waiting = state->waiters.size();
        return stats;
    }

private:
    struct Entry {
        enum class Phase { Dialing, Active, Idle, Retiring };
        Phase phase{Phase::Dialing};
        Origin origin;
        net::Address address;
        net::Connection connection;
        net::ReclaimReceipt reclaim;
        std::shared_ptr<Http11Connection> http;
        std::chrono::steady_clock::time_point expire_at{std::chrono::steady_clock::time_point::max()};
    };

    struct Waiter {
        bbt::coroutine::sync::CoWaiter::SPtr gate;
        Origin origin;
        // 待领取连接不拥有归还责任；Acquire 领取成功后才构造 Lease。
        std::shared_ptr<Http11Connection> handed;
        bool has_handed{false};
        bool in_queue{false};
    };

    struct State {
        State(std::shared_ptr<net::Engine> engine_arg, PoolLimits limits_arg, std::uint64_t body,
              std::optional<std::uint64_t> header)
            : engine(std::move(engine_arg)), limits(limits_arg), body_limit(body), header_limit(header)
        {
        }

        const std::shared_ptr<net::Engine> engine;
        const PoolLimits limits;
        const std::uint64_t body_limit;
        const std::optional<std::uint64_t> header_limit;
        mutable std::mutex mutex;
        std::condition_variable cv; // Shutdown 排空等待（native 宿主）
        Phase phase{Phase::Open};
        std::optional<Error> close_reason; // Close 发布；首个原因固定
        // 在途名额：Acquire 接纳时 +1，其失败出口或租借归还（Release/Retire）
        // 时 -1；排队中的 Acquire 也计入，等待者离队在同一出口结算。排空谓词
        // 只有 inflight==0，不读条目 phase/物理状态。
        std::uint64_t inflight{0};
        std::deque<std::unique_ptr<Entry>> entries;  // 稳定地址：跨挂起安全
        std::deque<std::shared_ptr<Waiter>> waiters; // 有界 FIFO

        // 非 Open 时的拒绝原因：Close 已发布原因时用它，Shutdown 只封口时回退
        // 到通用 Closed（不伪造一个并未发生的 Close）。
        Error ClosedErrorLocked() const
        {
            if (close_reason) {
                return *close_reason;
            }
            return MakeError(ErrorCode::Closed, "http pool: closed");
        }

        Entry* FindLocked(std::uint64_t connection_id)
        {
            for (auto& entry : entries) {
                if (entry->connection.Valid() && entry->connection.Id() == connection_id) {
                    return entry.get();
                }
            }
            return nullptr;
        }

        void DequeueLocked(const std::shared_ptr<Waiter>& waiter)
        {
            if (!waiter->in_queue) {
                return;
            }
            waiter->in_queue = false;
            for (auto it = waiters.begin(); it != waiters.end(); ++it) {
                if (*it == waiter) {
                    waiters.erase(it);
                    cv.notify_all();
                    return;
                }
            }
        }

        void RetireLocked(Entry& entry)
        {
            // 已 Reserving 的拨号条目在拨号成功后才拿到连接：此处仍要请求
            // 关闭一次（幂等），不能让封口后的新连接逃过退役。
            entry.phase = Entry::Phase::Retiring;
            if (entry.connection.Valid()) {
                entry.connection.RequestClose();
            }
            cv.notify_all();
        }

        // 快路径：sweep 后取一条同 origin 空闲连接（Idle→Active 原子预留）。
        bool TakeIdleLocked(const Origin& origin, std::shared_ptr<Http11Connection>& out)
        {
            for (auto& entry : entries) {
                if (entry->phase != Entry::Phase::Idle || entry->origin != origin) {
                    continue;
                }
                out = entry->http;
                entry->phase = Entry::Phase::Active;
                entry->expire_at = std::chrono::steady_clock::time_point::max();
                cv.notify_all();
                return true;
            }
            return false;
        }

        // 连接回空闲（封口/预算外则退役）。
        void AdmitToIdleLocked(Entry& entry)
        {
            if (phase != Phase::Open) {
                RetireLocked(entry);
                return;
            }
            std::size_t idle = 0;
            for (const auto& item : entries) {
                if (item->phase == Entry::Phase::Idle) {
                    ++idle;
                }
            }
            if (idle < limits.max_keepalive_connections) {
                entry.phase = Entry::Phase::Idle;
                entry.expire_at = limits.keepalive_expiry
                                      ? StageDeadline(PhaseTimeout::Milliseconds(*limits.keepalive_expiry),
                                                      bbt::coroutine::Deadline::max())
                                      : std::chrono::steady_clock::time_point::max();
                cv.notify_all();
            } else {
                RetireLocked(entry);
            }
        }

        // 孤儿 handoff 回池：封口即退役；否则交下一个同 origin 等待者或回空闲。
        void AdmitHandedLocked(const std::shared_ptr<Waiter>& waiter)
        {
            if (!waiter->has_handed) {
                return;
            }
            auto connection = std::move(waiter->handed);
            waiter->has_handed = false;
            auto* entry = FindLocked(connection->Net().Id());
            if (!entry) {
                return;
            }
            if (phase != Phase::Open) {
                RetireLocked(*entry);
                return;
            }
            for (auto next : waiters) {
                if (next->origin == entry->origin) {
                    // 责任转交下一个等待者，仍算在途。
                    entry->phase = Entry::Phase::Active;
                    next->handed = std::move(connection);
                    next->has_handed = true;
                    DequeueLocked(next);
                    next->gate->Notify();
                    return;
                }
            }
            AdmitToIdleLocked(*entry);
        }

        // 异 origin 空闲不能永久占据满池：先请求回收，真正释放后才重拨。
        void RetireOtherIdleLocked(const Origin& origin)
        {
            for (auto& entry : entries) {
                if (entry->phase == Entry::Phase::Idle && entry->origin != origin) {
                    RetireLocked(*entry);
                    return;
                }
            }
        }

        // 惰性维护：退役出账（观察到 Closed）、空闲 stale/过期退役、keepalive
        // 名额控制（保留到期时间最晚者）。只做公开观察，无副作用探测。
        void SweepLocked()
        {
            const auto now = std::chrono::steady_clock::now();
            for (auto it = entries.begin(); it != entries.end();) {
                auto& entry = *it;
                if (entry->phase == Entry::Phase::Retiring) {
                    const bool released =
                        entry->connection.Valid() ? entry->connection.Released() : entry->reclaim.Released();
                    // 只消费 net 真实物理回收凭据，逻辑 Closed 不释放名额；
                    // 条目出账后仍在途的租借只由 inflight 记账，归还时按
                    // conn.Id 找不到条目也只结算名额（见 Release/Retire）。
                    if (released) {
                        it = entries.erase(it);
                        continue;
                    }
                } else if (entry->phase == Entry::Phase::Idle) {
                    const bool stale = entry->connection.ReadEof() || entry->connection.ReadError()
                                       || entry->connection.ReadableBytes() > 0;
                    const bool expired = limits.keepalive_expiry && entry->expire_at <= now;
                    if (stale || expired || entry->connection.Closed()) {
                        RetireLocked(*entry);
                    }
                }
                ++it;
            }
        }
    };

    // Acquire 主体：已在本入口完成接纳（inflight++）。返回 ok 的在途责任随
    // 租借移交归还方；返回 err 由 Acquire 唯一出口结算名额。
    static result<Lease> AcquireBody(const std::shared_ptr<State>& state, const Origin& origin,
                                     const net::Address& address, const PhaseDeadlines& deadlines)
    {
        if (const auto gate = CheckCall(deadlines.total)) {
            return result<Lease>::err(
                MakeError(gate == std::errc::operation_canceled ? ErrorCode::Cancelled : ErrorCode::TimedOut,
                          "http pool: call ended before acquisition"));
        }
        // 快路径：sweep 后直接取同 origin 空闲。
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            if (state->phase != Phase::Open) {
                return result<Lease>::err(state->ClosedErrorLocked());
            }
            state->SweepLocked();
            std::shared_ptr<Http11Connection> connection;
            if (state->TakeIdleLocked(origin, connection)) {
                return result<Lease>::ok(Lease(state, std::move(connection)));
            }
        }
        // 预算内新建（Dialing 名额锁内预留、锁外 Engine::CoDial）。
        {
            std::unique_lock<std::mutex> lock(state->mutex);
            if (state->phase != Phase::Open) {
                return result<Lease>::err(state->ClosedErrorLocked());
            }
            if (state->entries.size() < state->limits.max_connections) {
                auto entry = std::make_unique<Entry>();
                entry->phase = Entry::Phase::Dialing;
                entry->origin = origin;
                entry->address = address;
                state->entries.push_back(std::move(entry));
                Entry* dial_entry = state->entries.back().get(); // deque 引用稳定
                lock.unlock();
                return DialReserved(state, dial_entry, deadlines);
            }
        }
        // 排队：有界队列 + 分片等待。片内事件：handoff / 封口 / 预算释放重试；
        // 片间重查（sweep 出账没有事件源，见文件头说明）。
        const auto pool_deadline = deadlines.pool;
        for (;;) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= pool_deadline) {
                return result<Lease>::err(MakeError(ErrorCode::TimedOut, "http pool: pool wait timeout"));
            }
            auto slice_end = now + std::chrono::milliseconds{20};
            if (slice_end > pool_deadline) {
                slice_end = pool_deadline;
            }
            auto waiter = std::make_shared<Waiter>();
            waiter->origin = origin;
            waiter->gate = bbt::coroutine::sync::CoWaiter::Create();
            if (!waiter->gate) {
                return result<Lease>::err(MakeError(ErrorCode::InternalError, "http pool: waiter alloc failed"));
            }
            {
                std::unique_lock<std::mutex> lock(state->mutex);
                if (state->phase != Phase::Open) {
                    return result<Lease>::err(state->ClosedErrorLocked());
                }
                state->SweepLocked();
                std::shared_ptr<Http11Connection> connection;
                if (state->TakeIdleLocked(origin, connection)) {
                    return result<Lease>::ok(Lease(state, std::move(connection)));
                }
                if (state->entries.size() < state->limits.max_connections) {
                    auto entry = std::make_unique<Entry>();
                    entry->phase = Entry::Phase::Dialing;
                    entry->origin = origin;
                    entry->address = address;
                    state->entries.push_back(std::move(entry));
                    Entry* dial_entry = state->entries.back().get();
                    lock.unlock();
                    return DialReserved(state, dial_entry, deadlines);
                }
                state->RetireOtherIdleLocked(origin);
                if (state->waiters.size() >= state->limits.max_queue) {
                    return result<Lease>::err(MakeError(ErrorCode::Overloaded, "http pool: queue full"));
                }
                state->waiters.push_back(waiter);
                waiter->in_queue = true;
            }
            // 登记→锁内复查→Notify；Notify 不等于获得，恢复后锁内领取。
            const auto status = waiter->gate->WaitWithCallback({slice_end}, [state, waiter] {
                std::lock_guard<std::mutex> lock(state->mutex);
                if (waiter->has_handed || state->phase != Phase::Open) {
                    waiter->gate->Notify();
                    return true;
                }
                state->SweepLocked();
                std::shared_ptr<Http11Connection> connection;
                if (state->TakeIdleLocked(waiter->origin, connection)) {
                    waiter->handed = std::move(connection);
                    waiter->has_handed = true;
                    state->DequeueLocked(waiter);
                    waiter->gate->Notify();
                    return true;
                }
                if (state->entries.size() < state->limits.max_connections) {
                    // 预算已释放：唤醒本等待者走重试（自建连接）。
                    waiter->gate->Notify();
                }
                return true;
            });
            {
                std::lock_guard<std::mutex> lock(state->mutex);
                state->DequeueLocked(waiter);
                if (waiter->has_handed && status == bbt::coroutine::WaitStatus::Completed) {
                    auto connection = std::move(waiter->handed);
                    waiter->has_handed = false;
                    return result<Lease>::ok(Lease(state, std::move(connection)));
                }
                if (waiter->has_handed) {
                    // 首胜不是本等待者（超时/取消/封口赢）：孤儿 handoff 回池。
                    state->AdmitHandedLocked(waiter);
                }
            }
            if (status == bbt::coroutine::WaitStatus::Cancelled) {
                return result<Lease>::err(MakeError(ErrorCode::Cancelled, "http pool: acquire cancelled"));
            }
            const auto now_after = std::chrono::steady_clock::now();
            if (now_after >= pool_deadline) {
                return result<Lease>::err(MakeError(ErrorCode::TimedOut, "http pool: pool wait timeout"));
            }
            if (status != bbt::coroutine::WaitStatus::Completed && status != bbt::coroutine::WaitStatus::TimedOut) {
                return result<Lease>::err(MakeError(ErrorCode::InternalError, "http pool: wait unavailable"));
            }
            // Completed（预算释放重试）或切片到点：重查。
        }
    }

    // 完成预留的 Dialing entry：锁外 Engine::CoDial，失败/封口在锁内出账。
    static result<Lease> DialReserved(const std::shared_ptr<State>& state, Entry* dial_entry,
                                      const PhaseDeadlines& deadlines)
    {
        net::ConnectionResult dialed;
        auto error = CheckCall(deadlines.total);
        if (!error) {
            dialed =
                state->engine->CoDial(dial_entry->address, {StageDeadline(deadlines.timeout.connect, deadlines.total)});
            error = dialed.error;
        }
        std::lock_guard<std::mutex> lock(state->mutex);
        if (error) {
            dial_entry->reclaim = std::move(dialed.reclaim);
            state->RetireLocked(*dial_entry);
            if (error == std::errc::operation_canceled) {
                return result<Lease>::err(MakeError(ErrorCode::Cancelled, "http pool: connect cancelled"));
            }
            if (error == std::errc::timed_out) {
                return result<Lease>::err(MakeError(ErrorCode::TimedOut, "http pool: connect timeout"));
            }
            // 拨号错误走公共桥 NetToError：容量满（no_buffer_space）归
            // Overloaded 而非 TransportError；backend_category/backend_code
            // 保留底层事实。pool 只覆盖 canceled/timed_out/closed 阶段语义。
            return result<Lease>::err(NetToError(error, "http pool: connect failed: " + error.message()));
        }
        dial_entry->connection = dialed.connection.Raw();
        dial_entry->reclaim = std::move(dialed.reclaim);
        if (state->phase != Phase::Open) {
            state->RetireLocked(*dial_entry);
            return result<Lease>::err(state->ClosedErrorLocked());
        }
        try {
            dial_entry->http =
                std::make_shared<Http11Connection>(dial_entry->connection, state->body_limit, state->header_limit);
            auto connection = dial_entry->http;
            dial_entry->phase = Entry::Phase::Active;
            state->cv.notify_all();
            return result<Lease>::ok(Lease(state, std::move(connection)));
        } catch (const std::exception& error) {
            state->RetireLocked(*dial_entry);
            return result<Lease>::err(MakeError(ErrorCode::InternalError, error.what()));
        }
    }

    static Error ClosedError()
    {
        return MakeError(ErrorCode::Closed, "http pool: closed");
    }

    std::shared_ptr<State> m_state;
};

inline void ConnectionPool::Lease::Release(bool healthy) noexcept
{
    // 先取走责任；重复调用、移动源与析构都不再结算。锁内只做本地状态变更。
    const auto state = std::move(m_state);
    auto http = std::move(connection);
    if (!state) {
        return;
    }
    std::lock_guard<std::mutex> lock(state->mutex);
    --state->inflight;
    auto* entry = state->FindLocked(http->Net().Id());
    if (!entry) {
        state->cv.notify_all();
        return;
    }
    if (!healthy || state->phase != Phase::Open) {
        state->RetireLocked(*entry);
        return;
    }
    for (auto waiter : state->waiters) {
        if (waiter->origin == entry->origin) {
            // 连接转交等待者；其在途名额仍属于自己的 Acquire，未领取前不造 Lease。
            entry->phase = Entry::Phase::Active;
            waiter->handed = std::move(http);
            waiter->has_handed = true;
            state->DequeueLocked(waiter);
            waiter->gate->Notify();
            return;
        }
    }
    state->AdmitToIdleLocked(*entry);
}

} // namespace detail
} // namespace bbt::infra::http
