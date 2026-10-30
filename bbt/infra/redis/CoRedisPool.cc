#include <bbt/infra/redis/CoRedisPool.hpp>

#include <deque>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/infra/redis/CoRedisCli.hpp>
#include "detail/PoolTestAccess.hpp"

namespace bbt::infra::redis {
namespace {

Error ClosedError() { return MakeError(ErrorCode::Closed, "redis pool: closed"); }
Error OverloadedError() { return MakeError(ErrorCode::Overloaded, "redis pool: wait queue full"); }
Error ContextError() { return MakeError(ErrorCode::InvalidContext, "redis pool: coroutine context required"); }

} // namespace

// 命令包装共用样板：按现有借还语义借连接（失败原样透传），把借到的
// client 交给 invoke，返回其终态；离开作用域时 Borrow RAII 按实际流
// 健康状态归还或退役，不改变既有命令的可观察行为。
template <typename R, typename Invoke>
bbt_co_async(R) CoRedisPool::RunBorrowed(const CallOptions& options, Invoke&& invoke) {
    const auto state = state_;
    auto acquired = bbt_co_await Acquire(options);
    if (!acquired) { return R::err(std::move(acquired.error())); }
    auto borrow = std::move(acquired).value();
    return bbt_co_await invoke(*borrow.client); // borrow 析构归还。
}

// State：公开句柄与 State 各自持有 shared_ptr；命令入口先拷贝 state 再挂起，
// 恢复后不再访问公开句柄。每个等待者持独立 CoWaiter（单等待位语义），归还方
// 锁内 handoff 连接后 Notify，无 native 条件变量等待，不阻塞 worker。
// mutex 只保护接纳/借还/发布/封口，永不跨挂起。
struct CoRedisPool::State {
    struct Slot {
        std::shared_ptr<CoRedisCli> client;
        bool alive{false}; // 坏流退役后置 false；只能由显式 Connect 重建。
    };

    // 一个排队等待者：handoff 槽位 + 唤醒门。生命周期由 shared_ptr 管理，
    // 挂起恢复迁移 worker 安全。
    struct Waiter {
        bbt::coroutine::sync::CoWaiter::SPtr gate;
        std::shared_ptr<CoRedisCli> handed;   // 归还方在锁内放入后 Notify。
        std::size_t handed_index{0};
        bool in_queue{false};
    };

    const std::shared_ptr<net::Engine> engine;
    const net::Address address;
    const PoolOptions options;

    mutable std::mutex mutex;
    bool closed{false};
    bool ready{false};
    bool connecting{false};
    std::function<void(int)> wait_hook;
    std::vector<Slot> slots;
    std::deque<std::size_t> available;           // 空闲健康 slot 索引 FIFO
    std::deque<std::shared_ptr<Waiter>> waiters; // 有界 FIFO

    State(std::shared_ptr<net::Engine> e, net::Address a, PoolOptions opt)
        : engine(std::move(e)), address(std::move(a)), options(opt) {}

    ~State() {
        std::lock_guard<std::mutex> lock(mutex);
        closed = true;
        for (auto& waiter : waiters) { waiter->gate->Notify(); }
        waiters.clear();
    }

    // 快路径借取；调用方持锁。false 表示池空/未就绪/已封口。
    bool TryTakeLocked(std::shared_ptr<CoRedisCli>& out, std::size_t& index) {
        if (closed || !ready || available.empty()) { return false; }
        index = available.front();
        available.pop_front();
        out = slots[index].client;
        return true;
    }

    // 队列接纳检查（持锁调用）：max_waiters=0 或队列满即拒绝，不排队。
    bool AdmitLocked() const {
        return options.max_waiters != 0 && waiters.size() < options.max_waiters;
    }

    void DequeueLocked(const std::shared_ptr<Waiter>& waiter) {
        if (!waiter->in_queue) { return; }
        waiter->in_queue = false;
        for (auto it = waiters.begin(); it != waiters.end(); ++it) {
            if (*it == waiter) { waiters.erase(it); return; }
        }
    }

    // 连接交还（持锁调用）：优先 FIFO handoff 给队首等待者；无人等则回空闲
    // 队列。封口后丢弃——封口即池内不再持有连接；slot 已被修复替换时同样
    // 丢弃旧连接。
    void RequeueLocked(std::shared_ptr<CoRedisCli> client, std::size_t index) {
        if (closed || !client) { return; }
        if (index >= slots.size() || slots[index].client != client) { return; }
        if (!waiters.empty()) {
            auto waiter = waiters.front();
            waiters.pop_front();
            waiter->in_queue = false;
            waiter->handed = std::move(client);
            waiter->handed_index = index;
            waiter->gate->Notify();
            return;
        }
        available.push_back(index);
    }

    // 命令失败退役（持锁调用）：只标记仍归属本 slot 的连接，不误标修复后的新连接。
    void RetireLocked(const std::shared_ptr<CoRedisCli>& client, std::size_t index) {
        if (index < slots.size() && slots[index].client == client) { slots[index].alive = false; }
    }
};

bbt_co_async(result<CoRedisPool::Borrow>) PoolTestAccess::Take(CoRedisPool& pool, const CallOptions& options) {
    return bbt_co_await pool.Acquire(options);
}
std::size_t PoolTestAccess::Queued(const CoRedisPool& pool) {
    std::lock_guard<std::mutex> lock(pool.state_->mutex);
    return pool.state_->waiters.size();
}
void PoolTestAccess::SetWaitHook(CoRedisPool& pool, std::function<void(int)> hook) {
    pool.state_->wait_hook = std::move(hook);
}

CoRedisPool::Borrow::Borrow(std::shared_ptr<State> state, std::shared_ptr<CoRedisCli> client, std::size_t index)
    : client(std::move(client)), state_(std::move(state)), index_(index) {}

// RAII 归还：无论命令成败，离开作用域即交还 State 的锁内 handoff/回队逻辑；
// Retire 已置 finished_ 时本析构为空操作。
CoRedisPool::Borrow::~Borrow() { GiveBack(); }

// 归还：交给 State 的锁内 handoff/回队逻辑；封口或 slot 已替换时连接被丢弃。
void CoRedisPool::Borrow::GiveBack() {
    if (finished_ || !client) { return; }
    if (!client->StreamHealthy()) { Retire(); return; }
    finished_ = true;
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->RequeueLocked(std::move(client), index_);
}

// 命令失败：关闭连接并退役归属 slot，不归还；等待显式 Connect 修复。
void CoRedisPool::Borrow::Retire() {
    if (finished_ || !client) { return; }
    finished_ = true;
    client->Close();
    std::lock_guard<std::mutex> lock(state_->mutex);
    state_->RetireLocked(client, index_);
}

CoRedisPool::CoRedisPool(std::shared_ptr<State> state) noexcept : state_(std::move(state)) {}

CoRedisPool::~CoRedisPool() noexcept { Close(); }

result<std::shared_ptr<CoRedisPool>> CoRedisPool::Create(
    std::shared_ptr<net::Engine> engine,
    net::Address address,
    PoolOptions options) {
    if (!engine || !address.Valid() ||
        (address.Family() != net::AddressFamily::IPv4 && address.Family() != net::AddressFamily::IPv6)) {
        return result<std::shared_ptr<CoRedisPool>>::err(
            MakeError(ErrorCode::InvalidArgument, "redis pool: Engine and IP address required"));
    }
    if (options.connections == 0) {
        return result<std::shared_ptr<CoRedisPool>>::err(
            MakeError(ErrorCode::InvalidArgument, "redis pool: connections must be greater than zero"));
    }
    auto state = std::make_shared<State>(std::move(engine), std::move(address), options);
    return result<std::shared_ptr<CoRedisPool>>::ok(
        std::shared_ptr<CoRedisPool>(new CoRedisPool(std::move(state))));
}

bbt_co_async(result<void>) CoRedisPool::Connect(const CallOptions& options) {
    const auto state = state_; // 此后包括挂起恢复都不再访问公开对象。
    if (bbt::coroutine::GetLocalCoroutineId() == 0) { return result<void>::err(ContextError()); }
    bool need_build = false;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->closed) { return result<void>::err(ClosedError()); }
        if (state->connecting) { return result<void>::err(OverloadedError()); }
        if (state->ready) {
            // 幂等：全健康时零动作；有坏 slot 时进入修复，只重建坏 slot。
            for (const auto& slot : state->slots) {
                if (!slot.alive) { need_build = true; break; }
            }
            if (!need_build) { return result<void>::ok(); }
        } else {
            if (state->slots.empty()) { state->slots.resize(state->options.connections); }
            need_build = true;
        }
        state->connecting = true;
    }
    // 逐 slot 建连：新池建全部，修复只建坏 slot。部分失败回滚本次建立的
    // 连接并清 connecting，不发布半就绪状态。
    std::vector<std::shared_ptr<CoRedisCli>> established;
    struct BuildCleanup {
        std::shared_ptr<State> state;
        std::vector<std::shared_ptr<CoRedisCli>>& established;
        ~BuildCleanup() {
            for (auto& client : established) { if (client) { client->Close(); } }
            std::lock_guard<std::mutex> lock(state->mutex);
            state->connecting = false;
        }
    } cleanup{state, established};
    std::vector<std::size_t> targets;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        for (std::size_t slot_index = 0; slot_index < state->slots.size(); ++slot_index) {
            if (!state->slots[slot_index].alive) { targets.push_back(slot_index); }
        }
    }
    for (std::size_t remaining = targets.size(); remaining > 0; --remaining) {
        // 每条连接（含修复/重建）都用池配置的握手走三参 Create；
        // 握手配置非法或握手失败时下方 rollback 统一归还预算。
        auto created = CoRedisCli::Create(state->engine, state->address, state->options.handshake);
        if (!created) {
            for (auto& item : established) { item->Close(); }
            std::lock_guard<std::mutex> lock(state->mutex);
            return result<void>::err(std::move(created.error()));
        }
        auto client = std::move(created).value();
        auto connected = bbt_co_await client->Connect(options);
        if (!connected) {
            client->Close();
            for (auto& item : established) { item->Close(); }
            std::lock_guard<std::mutex> lock(state->mutex);
            return result<void>::err(std::move(connected.error()));
        }
        established.push_back(std::move(client));
    }
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->closed) {
            for (auto& item : established) { item->Close(); }
            return result<void>::err(ClosedError());
        }
        std::size_t cursor = 0;
        for (const auto target : targets) {
            state->slots[target].client = std::move(established[cursor++]);
            state->slots[target].alive = true;
            state->RequeueLocked(state->slots[target].client, target);
        }
        state->ready = true;
    }
    return result<void>::ok();
}

void CoRedisPool::Close() noexcept {
    const auto state = state_;
    std::deque<std::shared_ptr<State::Waiter>> pending;
    std::vector<State::Slot> slots;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->closed) { return; }
        state->closed = true; // 与接纳/借还/发布共用短锁的线性化点。
        state->ready = false;
        pending.swap(state->waiters);
        state->available.clear();
        slots.swap(state->slots);
    }
    // 锁外封口各 client 并唤醒全部等待者；等待者恢复后在锁内看到 closed。
    for (auto& slot : slots) { if (slot.client) { slot.client->Close(); } }
    for (auto& waiter : pending) { waiter->gate->Notify(); }
}

bool CoRedisPool::IsClosed() const noexcept {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->closed;
}

// 池内借出一条连接。快路径直接取；池空时有界 FIFO 排队，归还方 handoff。
// 超时/取消/封口可区分且等待者不遗失；离场时已收到的 handoff 连接转交回池。
bbt_co_async(result<CoRedisPool::Borrow>) CoRedisPool::Acquire(const CallOptions& options) {
    const auto state = state_;
    if (bbt::coroutine::GetLocalCoroutineId() == 0) {
        return result<Borrow>::err(ContextError());
    }
    std::shared_ptr<CoRedisCli> taken;
    std::size_t index = 0;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->closed) { return result<Borrow>::err(ClosedError()); }
        if (!state->ready) {
            return result<Borrow>::err(
                MakeError(ErrorCode::RuntimeUnavailable, "redis pool: not connected"));
        }
        if (state->TryTakeLocked(taken, index)) {
            return result<Borrow>::ok(Borrow(state, std::move(taken), index));
        }
        if (!state->AdmitLocked()) { return result<Borrow>::err(OverloadedError()); }
    }
    auto waiter = std::make_shared<State::Waiter>();
    waiter->gate = bbt::coroutine::sync::CoWaiter::Create();
    if (!waiter->gate) {
        return result<Borrow>::err(
            MakeError(ErrorCode::InternalError, "redis pool: waiter allocation failed"));
    }
    // 所有退出路径（包括登记失败、回调异常）统一出账并归还已交接的槽。
    struct Cleanup {
        std::shared_ptr<State> state;
        std::shared_ptr<State::Waiter> waiter;
        ~Cleanup() {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->DequeueLocked(waiter);
            if (waiter->handed) {
                state->RequeueLocked(std::move(waiter->handed), waiter->handed_index);
            }
        }
    } cleanup{state, waiter};
    bool overloaded = false;
    std::exception_ptr registration_error;
    if (state->wait_hook) { state->wait_hook(0); }
    // CoWaiter 已登记后才复查/入队；锁内发布状态后 Notify 不会丢失。
    const auto status = waiter->gate->WaitWithCallback({options.deadline}, [&] {
        try {
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            if (state->closed) { waiter->gate->Notify(); }
            else if (state->TryTakeLocked(waiter->handed, waiter->handed_index)) {
                waiter->gate->Notify();
            } else if (!state->AdmitLocked()) {
                overloaded = true;
                waiter->gate->Notify();
            } else {
                state->waiters.push_back(waiter);
                waiter->in_queue = true;
            }
        }
        if (state->wait_hook) { state->wait_hook(1); }
        } catch (...) {
            // on_registered 在调度器栈执行，异常不能穿过 runtime 回调边界。
            registration_error = std::current_exception();
            waiter->gate->Notify();
        }
        return true;
    });
    if (registration_error) { std::rethrow_exception(registration_error); }
    if (state->wait_hook) { state->wait_hook(2); }
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->DequeueLocked(waiter);
        if (state->closed) { return result<Borrow>::err(ClosedError()); }
        if (overloaded) { return result<Borrow>::err(OverloadedError()); }
        if (status == bbt::coroutine::WaitStatus::Completed && waiter->handed) {
            return result<Borrow>::ok(Borrow(state, std::move(waiter->handed), waiter->handed_index));
        }
        if (waiter->handed) {
            // 首胜不是本等待者（取消/超时赢），handoff 连接转交回池给下一
            // 等待者或空闲队列。
            auto orphan = std::move(waiter->handed);
            state->RequeueLocked(std::move(orphan), waiter->handed_index);
        }
    }
    if (status == bbt::coroutine::WaitStatus::Cancelled) {
        return result<Borrow>::err(MakeError(ErrorCode::Cancelled, "redis pool: acquire cancelled"));
    }
    if (status == bbt::coroutine::WaitStatus::TimedOut) {
        return result<Borrow>::err(MakeError(ErrorCode::TimedOut, "redis pool: acquire timeout"));
    }
    return result<Borrow>::err(MakeError(ErrorCode::RuntimeUnavailable, "redis pool: wait registration failed"));
}

bbt_co_async(result<void>) CoRedisPool::Ping(const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<void>>(options,
        [&](CoRedisCli& client) { return client.Ping(options); });
}

bbt_co_async(result<std::optional<std::string>>) CoRedisPool::Get(std::string_view key, const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<std::optional<std::string>>>(options,
        [&](CoRedisCli& client) { return client.Get(key, options); });
}

bbt_co_async(result<void>) CoRedisPool::Set(std::string_view key, std::string_view value, const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<void>>(options,
        [&](CoRedisCli& client) { return client.Set(key, value, options); });
}

bbt_co_async(result<bool>) CoRedisPool::Set(std::string_view key, std::string_view value, const SetOptions& set_options, const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<bool>>(options,
        [&](CoRedisCli& client) { return client.Set(key, value, set_options, options); });
}

bbt_co_async(result<bool>) CoRedisPool::Exists(std::string_view key, const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<bool>>(options,
        [&](CoRedisCli& client) { return client.Exists(key, options); });
}

bbt_co_async(result<std::uint64_t>) CoRedisPool::Delete(std::vector<std::string> keys, const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<std::uint64_t>>(options,
        [&](CoRedisCli& client) { return client.Delete(std::move(keys), options); });
}

bbt_co_async(result<bool>) CoRedisPool::Expire(std::string_view key, std::chrono::milliseconds ttl, const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<bool>>(options,
        [&](CoRedisCli& client) { return client.Expire(key, ttl, options); });
}

bbt_co_async(result<TtlResult>) CoRedisPool::Ttl(std::string_view key, const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<TtlResult>>(options,
        [&](CoRedisCli& client) { return client.Ttl(key, options); });
}

bbt_co_async(result<std::int64_t>) CoRedisPool::Incr(std::string_view key, const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<std::int64_t>>(options,
        [&](CoRedisCli& client) { return client.Incr(key, options); });
}

bbt_co_async(result<std::int64_t>) CoRedisPool::Decr(std::string_view key, const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<std::int64_t>>(options,
        [&](CoRedisCli& client) { return client.Decr(key, options); });
}

bbt_co_async(result<std::int64_t>) CoRedisPool::IncrBy(std::string_view key, std::int64_t delta, const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<std::int64_t>>(options,
        [&](CoRedisCli& client) { return client.IncrBy(key, delta, options); });
}

bbt_co_async(result<std::int64_t>) CoRedisPool::DecrBy(std::string_view key, std::int64_t delta, const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<std::int64_t>>(options,
        [&](CoRedisCli& client) { return client.DecrBy(key, delta, options); });
}

bbt_co_async(result<bool>) CoRedisPool::HSet(std::string_view key, std::string_view field, std::string_view value, const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<bool>>(options,
        [&](CoRedisCli& client) { return client.HSet(key, field, value, options); });
}

bbt_co_async(result<std::optional<std::string>>) CoRedisPool::HGet(std::string_view key, std::string_view field, const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<std::optional<std::string>>>(options,
        [&](CoRedisCli& client) { return client.HGet(key, field, options); });
}

bbt_co_async(result<std::uint64_t>) CoRedisPool::HDel(std::string_view key, std::vector<std::string> fields, const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<std::uint64_t>>(options,
        [&](CoRedisCli& client) { return client.HDel(key, std::move(fields), options); });
}

bbt_co_async(result<bool>) CoRedisPool::HExists(std::string_view key, std::string_view field, const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<bool>>(options,
        [&](CoRedisCli& client) { return client.HExists(key, field, options); });
}

bbt_co_async(result<HashEntries>) CoRedisPool::HGetAll(std::string_view key, const CallOptions& options) {
    return bbt_co_await RunBorrowed<result<HashEntries>>(options,
        [&](CoRedisCli& client) { return client.HGetAll(key, options); });
}

} // namespace bbt::infra::redis
