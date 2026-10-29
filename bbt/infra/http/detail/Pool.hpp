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
//   - 失败/超时/取消/协议错/EOF 的连接一律退役，不自动重放请求。

#include <algorithm>
#include <chrono>
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

    bool operator==(const Origin& other) const noexcept {
        return port == other.port && scheme == other.scheme && host == other.host;
    }
    bool operator!=(const Origin& other) const noexcept { return !(*this == other); }
};

class ConnectionPool {
public:
    struct Lease {
        std::shared_ptr<Http11Connection> connection;
        Origin origin;
    };

    ConnectionPool(std::shared_ptr<net::Engine> engine, PoolLimits limits,
                   std::uint64_t max_body_bytes)
        : m_state(std::make_shared<State>(std::move(engine), limits, max_body_bytes)) {}

    // 借用一条 origin 匹配连接：快路径 / 预算内新建（Engine::CoDial）/ 有界排队。
    bbt_co_async(result<Lease>) Acquire(const Origin& origin, const net::Address& address,
                                        const PhaseDeadlines& deadlines) {
        const auto state = m_state;
        if (bbt::coroutine::GetLocalCoroutineId() == 0) {
            return result<Lease>::err(
                MakeError(ErrorCode::InvalidContext, "http pool: coroutine context required"));
        }
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            if (state->closed) { return result<Lease>::err(ClosedError()); }
        }
        if (const auto gate = CheckCall(deadlines.total)) {
            return result<Lease>::err(MakeError(
                gate == std::errc::operation_canceled ? ErrorCode::Cancelled : ErrorCode::TimedOut,
                "http pool: call ended before acquisition"));
        }
        // 快路径：sweep 后直接取同 origin 空闲。
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            if (state->closed) { return result<Lease>::err(ClosedError()); }
            state->SweepLocked();
            Lease lease;
            if (state->TakeIdleLocked(origin, lease)) {
                return result<Lease>::ok(std::move(lease));
            }
        }
        // 预算内新建（Dialing 名额锁内预留、锁外 Engine::CoDial）。
        {
            std::unique_lock<std::mutex> lock(state->mutex);
            if (state->closed) { return result<Lease>::err(ClosedError()); }
            if (state->entries.size() < state->limits.max_connections) {
                auto entry = std::make_unique<Entry>();
                entry->phase = Entry::Phase::Dialing;
                entry->origin = origin;
                entry->address = address;
                state->entries.push_back(std::move(entry));
                Entry* dial_entry = state->entries.back().get();  // deque 引用稳定
                lock.unlock();
                return DialReserved(state, dial_entry, origin, deadlines);
            }
        }
        // 排队：有界队列 + 分片等待。片内事件：handoff / 封口 / 预算释放重试；
        // 片间重查（sweep 出账没有事件源，见文件头说明）。
        const auto pool_deadline = deadlines.pool;
        for (;;) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= pool_deadline) {
                return result<Lease>::err(
                    MakeError(ErrorCode::TimedOut, "http pool: pool wait timeout"));
            }
            auto slice_end = now + std::chrono::milliseconds{20};
            if (slice_end > pool_deadline) { slice_end = pool_deadline; }
            auto waiter = std::make_shared<Waiter>();
            waiter->origin = origin;
            waiter->gate = bbt::coroutine::sync::CoWaiter::Create();
            if (!waiter->gate) {
                return result<Lease>::err(
                    MakeError(ErrorCode::InternalError, "http pool: waiter alloc failed"));
            }
            {
                std::unique_lock<std::mutex> lock(state->mutex);
                if (state->closed) { return result<Lease>::err(ClosedError()); }
                state->SweepLocked();
                Lease lease;
                if (state->TakeIdleLocked(origin, lease)) {
                    return result<Lease>::ok(std::move(lease));
                }
                if (state->entries.size() < state->limits.max_connections) {
                    auto entry = std::make_unique<Entry>();
                    entry->phase = Entry::Phase::Dialing;
                    entry->origin = origin;
                    entry->address = address;
                    state->entries.push_back(std::move(entry));
                    Entry* dial_entry = state->entries.back().get();
                    lock.unlock();
                    return DialReserved(state, dial_entry, origin, deadlines);
                }
                state->RetireOtherIdleLocked(origin);
                if (state->waiters.size() >= state->limits.max_queue) {
                    return result<Lease>::err(
                        MakeError(ErrorCode::Overloaded, "http pool: queue full"));
                }
                state->waiters.push_back(waiter);
                waiter->in_queue = true;
            }
            // 登记→锁内复查→Notify；Notify 不等于获得，恢复后锁内领取。
            const auto status = waiter->gate->WaitWithCallback(
                {slice_end}, [state, waiter] {
                    std::lock_guard<std::mutex> lock(state->mutex);
                    if (waiter->has_handed || state->closed) {
                        waiter->gate->Notify();
                        return true;
                    }
                    state->SweepLocked();
                    Lease lease;
                    if (state->TakeIdleLocked(waiter->origin, lease)) {
                        waiter->handed = std::move(lease);
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
                    Lease lease = std::move(waiter->handed);
                    waiter->has_handed = false;
                    return result<Lease>::ok(std::move(lease));
                }
                if (waiter->has_handed) {
                    // 首胜不是本等待者（超时/取消/封口赢）：孤儿 handoff 回池。
                    state->AdmitHandedLocked(waiter);
                }
            }
            if (status == bbt::coroutine::WaitStatus::Cancelled) {
                return result<Lease>::err(
                    MakeError(ErrorCode::Cancelled, "http pool: acquire cancelled"));
            }
            const auto now_after = std::chrono::steady_clock::now();
            if (now_after >= pool_deadline) {
                return result<Lease>::err(
                    MakeError(ErrorCode::TimedOut, "http pool: pool wait timeout"));
            }
            if (status != bbt::coroutine::WaitStatus::Completed &&
                status != bbt::coroutine::WaitStatus::TimedOut) {
                return result<Lease>::err(MakeError(ErrorCode::InternalError, "http pool: wait unavailable"));
            }
            // Completed（预算释放重试）或切片到点：重查。
        }
    }

    // 归还：healthy 且对端 keepalive → 回池或交队首同 origin 等待者；否则退役。
    void Release(const Lease& lease, bool healthy) {
        if (!lease.connection) { return; }
        if (!healthy) { Retire(lease); return; }
        const auto state = m_state;
        std::lock_guard<std::mutex> lock(state->mutex);
        auto* entry = state->FindLocked(lease.connection->Net().Id());
        if (!entry) { return; }
        if (state->closed) {
            state->RetireLocked(*entry);
            return;
        }
        for (auto waiter : state->waiters) {
            if (waiter->origin == lease.origin) {
                entry->phase = Entry::Phase::Active;
                waiter->handed = lease;
                waiter->has_handed = true;
                state->DequeueLocked(waiter);
                waiter->gate->Notify();
                return;
            }
        }
        state->AdmitToIdleLocked(*entry);
    }

    // 退役：请求封口并转入 Retiring；名额只在真实物理回收后出账。
    void Retire(const Lease& lease) {
        if (!lease.connection) { return; }
        const auto state = m_state;
        std::lock_guard<std::mutex> lock(state->mutex);
        if (auto* entry = state->FindLocked(lease.connection->Net().Id())) {
            state->RetireLocked(*entry);
        }
    }

    // 立即逻辑封口：拒绝新请求、唤醒全部排队等待者、退役空闲连接；幂等不可
    // 重开。在飞请求按已持有借用完成或失败，Release 时按封口语义退役。
    void Close() noexcept {
        const auto state = m_state;
        std::deque<std::shared_ptr<Waiter>> pending;
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            if (state->closed) { return; }
            state->closed = true;
            for (auto& entry : state->entries) {
                if (entry->phase == Entry::Phase::Idle) { state->RetireLocked(*entry); }
            }
            pending.swap(state->waiters);
            for (auto& waiter : pending) { waiter->in_queue = false; }
        }
        for (auto& waiter : pending) { waiter->gate->Notify(); }
    }

    bool IsClosed() const noexcept {
        const auto state = m_state;
        std::lock_guard<std::mutex> lock(state->mutex);
        return state->closed;
    }

    PoolStats Stats() const noexcept {
        const auto state = m_state;
        std::lock_guard<std::mutex> lock(state->mutex);
        // 内省触发同一惰性维护（sweep 只做本地状态检查与 RequestClose，不挂起、
        // 不做 I/O），使退役出账在没有新请求时也可观察。
        state->SweepLocked();
        PoolStats stats;
        for (const auto& entry : state->entries) {
            switch (entry->phase) {
                case Entry::Phase::Active: ++stats.active; break;
                case Entry::Phase::Idle: ++stats.idle; break;
                case Entry::Phase::Dialing: ++stats.dialing; break;
                case Entry::Phase::Retiring: ++stats.retiring; break;
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
        Lease handed;
        bool has_handed{false};
        bool in_queue{false};
    };

    struct State {
        State(std::shared_ptr<net::Engine> engine_arg, PoolLimits limits_arg,
              std::uint64_t body)
            : engine(std::move(engine_arg)), limits(limits_arg), body_limit(body) {}

        const std::shared_ptr<net::Engine> engine;
        const PoolLimits limits;
        const std::uint64_t body_limit;
        mutable std::mutex mutex;
        bool closed{false};
        std::deque<std::unique_ptr<Entry>> entries;   // 稳定地址：跨挂起安全
        std::deque<std::shared_ptr<Waiter>> waiters;  // 有界 FIFO

        Entry* FindLocked(std::uint64_t connection_id) {
            for (auto& entry : entries) {
                if (entry->connection.Valid() && entry->connection.Id() == connection_id) {
                    return entry.get();
                }
            }
            return nullptr;
        }

        void DequeueLocked(const std::shared_ptr<Waiter>& waiter) {
            if (!waiter->in_queue) { return; }
            waiter->in_queue = false;
            for (auto it = waiters.begin(); it != waiters.end(); ++it) {
                if (*it == waiter) { waiters.erase(it); return; }
            }
        }

        void RetireLocked(Entry& entry) {
            if (entry.phase == Entry::Phase::Retiring) { return; }
            entry.phase = Entry::Phase::Retiring;
            if (entry.connection.Valid()) { entry.connection.RequestClose(); }
        }

        // 快路径：sweep 后取一条同 origin 空闲连接（Idle→Active 原子预留）。
        bool TakeIdleLocked(const Origin& origin, Lease& out) {
            for (auto& entry : entries) {
                if (entry->phase != Entry::Phase::Idle || entry->origin != origin) { continue; }
                out.connection = entry->http;
                out.origin = origin;
                entry->phase = Entry::Phase::Active;
                entry->expire_at = std::chrono::steady_clock::time_point::max();
                return true;
            }
            return false;
        }

        // 连接回空闲（封口/预算外则退役）。
        void AdmitToIdleLocked(Entry& entry) {
            if (closed) { RetireLocked(entry); return; }
            std::size_t idle = 0;
            for (const auto& item : entries) {
                if (item->phase == Entry::Phase::Idle) { ++idle; }
            }
            if (idle < limits.max_keepalive_connections) {
                entry.phase = Entry::Phase::Idle;
                entry.expire_at = limits.keepalive_expiry
                    ? StageDeadline(PhaseTimeout::Milliseconds(*limits.keepalive_expiry),
                                    bbt::coroutine::Deadline::max())
                    : std::chrono::steady_clock::time_point::max();
            } else {
                RetireLocked(entry);
            }
        }

        // 孤儿 handoff 回池：封口即退役；否则交下一个同 origin 等待者或回空闲。
        void AdmitHandedLocked(const std::shared_ptr<Waiter>& waiter) {
            if (!waiter->has_handed) { return; }
            Lease lease = std::move(waiter->handed);
            waiter->has_handed = false;
            auto* entry = FindLocked(lease.connection->Net().Id());
            if (!entry) { return; }
            if (closed) { RetireLocked(*entry); return; }
            for (auto next : waiters) {
                if (next->origin == lease.origin) {
                    entry->phase = Entry::Phase::Active;
                    next->handed = std::move(lease);
                    next->has_handed = true;
                    DequeueLocked(next);
                    next->gate->Notify();
                    return;
                }
            }
            AdmitToIdleLocked(*entry);
        }

        // 异 origin 空闲不能永久占据满池：先请求回收，真正释放后才重拨。
        void RetireOtherIdleLocked(const Origin& origin) {
            for (auto& entry : entries) {
                if (entry->phase == Entry::Phase::Idle && entry->origin != origin) {
                    RetireLocked(*entry);
                    return;
                }
            }
        }

        // 惰性维护：退役出账（观察到 Closed）、空闲 stale/过期退役、keepalive
        // 名额控制（保留到期时间最晚者）。只做公开观察，无副作用探测。
        void SweepLocked() {
            const auto now = std::chrono::steady_clock::now();
            for (auto it = entries.begin(); it != entries.end();) {
                auto& entry = *it;
                if (entry->phase == Entry::Phase::Retiring) {
                    const bool released = entry->connection.Valid()
                        ? entry->connection.Released() : entry->reclaim.Released();
                    if (released) {
                        // 只消费 net 真实物理回收凭据，逻辑 Closed 不释放名额。
                        it = entries.erase(it);
                        continue;
                    }
                } else if (entry->phase == Entry::Phase::Idle) {
                    const bool stale = entry->connection.ReadEof() ||
                                       entry->connection.ReadError() ||
                                       entry->connection.ReadableBytes() > 0;
                    const bool expired =
                        limits.keepalive_expiry && entry->expire_at <= now;
                    if (stale || expired || entry->connection.Closed()) {
                        RetireLocked(*entry);
                    }
                }
                ++it;
            }
        }
    };

    // 完成预留的 Dialing entry：锁外 Engine::CoDial，失败/封口在锁内出账。
    static result<Lease> DialReserved(const std::shared_ptr<State>& state, Entry* dial_entry,
                                      const Origin& origin, const PhaseDeadlines& deadlines) {
        net::ConnectionResult dialed;
        auto error = CheckCall(deadlines.total);
        if (!error) {
            dialed = state->engine->CoDial(
                dial_entry->address,
                {StageDeadline(deadlines.timeout.connect, deadlines.total)});
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
                return result<Lease>::err(
                    MakeError(ErrorCode::TimedOut, "http pool: connect timeout"));
            }
            // 拨号错误走公共桥 NetToError：容量满（no_buffer_space）归
            // Overloaded 而非 TransportError；backend_category/backend_code
            // 保留底层事实。pool 只覆盖 canceled/timed_out/closed 阶段语义。
            return result<Lease>::err(
                NetToError(error, "http pool: connect failed: " + error.message()));
        }
        dial_entry->connection = dialed.connection.Raw();
        dial_entry->reclaim = std::move(dialed.reclaim);
        if (state->closed) {
            state->RetireLocked(*dial_entry);
            return result<Lease>::err(ClosedError());
        }
        try {
            dial_entry->http = std::make_shared<Http11Connection>(
                dial_entry->connection, state->body_limit);
            Lease lease{dial_entry->http, origin};
            dial_entry->phase = Entry::Phase::Active;
            return result<Lease>::ok(std::move(lease));
        } catch (const std::exception& error) {
            state->RetireLocked(*dial_entry);
            return result<Lease>::err(MakeError(ErrorCode::InternalError, error.what()));
        }
    }

    static Error ClosedError() { return MakeError(ErrorCode::Closed, "http pool: closed"); }

    std::shared_ptr<State> m_state;
};

} // namespace detail
} // namespace bbt::infra::http
