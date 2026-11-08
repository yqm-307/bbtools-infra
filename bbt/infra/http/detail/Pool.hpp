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
        Lease& operator=(Lease&& other) noexcept;
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
                   std::optional<std::uint64_t> max_header_bytes);

    // 借用一条 origin 匹配连接：快路径 / 预算内新建（Engine::CoDial）/ 有界排队。
    // 同一把锁内检查状态并 inflight++（非 Open 即拒绝）；本入口失败即
    // inflight--，成功则把这份在途责任随租借移交归还方（Release/Retire）。
    bbt_co_async(result<Lease>)
        Acquire(const Origin& origin, const net::Address& address, const PhaseDeadlines& deadlines);

    // 立即逻辑封口：拒绝新请求、对全部条目（含在飞/拨号）请求关闭并退役、
    // 唤醒全部排队等待者与排空等待者、发布首个关闭原因；幂等不可重开且
    // 「失败在途」恰好执行一次（Draining 下同样执行，不与 Shutdown 共用位）。
    // 不等待在飞请求，不关共享 Engine，不假装物理排空。
    void Close(Error reason) noexcept;

    // 置 Draining（拒新）并等 inflight 归零：在途 Acquire、未归还租借与排队
    // 等待者都计入，不看条目 phase。有限期限超时返回 TimedOut 且保持拒新；
    // 等待中被 Close 且仍有在途则返回首个关闭原因，Close 前已排空则 ok；
    // 重复调用时仍 Draining 且有在途返回 Overloaded（Busy），已排空则 ok；
    // 已 Closed 返回首个关闭原因。不保存「到达时是否已排空」快照。
    result<void> Shutdown(bbt::coroutine::Deadline deadline);

    bool IsClosed() const noexcept;

    std::optional<Error> CloseReason() const noexcept;

    // 生命周期：单向前。Close 在任何状态下都执行且仅执行一次「失败在途」。
    enum class Phase { Open, Draining, Closed };

    PoolStats Stats() const noexcept;

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
              std::optional<std::uint64_t> header);

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
        Error ClosedErrorLocked() const;

        Entry* FindLocked(std::uint64_t connection_id);

        void DequeueLocked(const std::shared_ptr<Waiter>& waiter);

        void RetireLocked(Entry& entry);

        // 快路径：sweep 后取一条同 origin 空闲连接（Idle→Active 原子预留）。
        bool TakeIdleLocked(const Origin& origin, std::shared_ptr<Http11Connection>& out);

        // 连接回空闲（封口/预算外则退役）。
        void AdmitToIdleLocked(Entry& entry);

        // 孤儿 handoff 回池：封口即退役；否则交下一个同 origin 等待者或回空闲。
        void AdmitHandedLocked(const std::shared_ptr<Waiter>& waiter);

        // 异 origin 空闲不能永久占据满池：先请求回收，真正释放后才重拨。
        void RetireOtherIdleLocked(const Origin& origin);

        // 惰性维护：退役出账（观察到 Closed）、空闲 stale/过期退役、keepalive
        // 名额控制（保留到期时间最晚者）。只做公开观察，无副作用探测。
        void SweepLocked();
    };

    // Acquire 主体：已在本入口完成接纳（inflight++）。返回 ok 的在途责任随
    // 租借移交归还方；返回 err 由 Acquire 唯一出口结算名额。
    static result<Lease> AcquireBody(const std::shared_ptr<State>& state, const Origin& origin,
                                     const net::Address& address, const PhaseDeadlines& deadlines);

    // 完成预留的 Dialing entry：锁外 Engine::CoDial，失败/封口在锁内出账。
    static result<Lease> DialReserved(const std::shared_ptr<State>& state, Entry* dial_entry,
                                      const PhaseDeadlines& deadlines);

    static Error ClosedError();

    std::shared_ptr<State> m_state;
};

} // namespace detail
} // namespace bbt::infra::http
