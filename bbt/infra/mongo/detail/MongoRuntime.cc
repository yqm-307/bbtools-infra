#include "bbt/infra/mongo/detail/MongoRuntime.hpp"
#include <bbt/infra/mongo/detail/CoMongoCliImpl.hpp>

#include <algorithm>
#include <chrono>
#include <limits>
#include <new>
#include <system_error>

#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/object/CoObject.hpp>

/**
 * 执行域判定 / 50ms 分片 / CloseOptions 校验
 */
#include <bbt/infra/detail/CloseWait.hpp>

#include <bsoncxx/v1/document/value.hpp>
#include <bsoncxx/v1/document/view.hpp>
#include <bsoncxx/v1/exception.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/v1/collection.hpp>
#include <mongocxx/v1/cursor.hpp>
#include <mongocxx/v1/database.hpp>
#include <mongocxx/v1/delete_many_result.hpp>
#include <mongocxx/v1/delete_one_result.hpp>
#include <mongocxx/v1/find_options.hpp>
#include <mongocxx/v1/insert_many_result.hpp>
#include <mongocxx/v1/insert_one_result.hpp>
#include <mongocxx/v1/server_error.hpp>
#include <mongocxx/v1/update_many_result.hpp>
#include <mongocxx/v1/update_one_result.hpp>

namespace bbt::infra::mongo_detail {

namespace {

thread_local MongoRuntime* g_worker_runtime = nullptr;
thread_local std::thread::id g_worker_thread;

/**
 * 测试接缝（detail-only，进程内唯一）：finalizer 交接阶段的停靠门 + 回收观察。
 * 放在静态存储而不是 owner 上，有两个硬理由：最后引用归零后测试已无法经 owner
 * 放行停靠；delete 之后连成员都不存在，观察值必须活得比对象久。生产路径只是
 * 多几次原子读/写，不停靠、不观察。
 */
struct FinalizerHandoffGateForTest {
    std::mutex mtx;
    std::condition_variable cv;
    std::atomic_bool paused{false};

    /**
     * mtx 保护：finalizer 已停在交接前一刻
     */
    bool at_stage{false};
    std::atomic<const MongoRuntime*> watched{nullptr};
    std::atomic<int> reclaim_count{0};
    std::atomic<std::thread::id> reclaim_thread{};
};

FinalizerHandoffGateForTest& HandoffGate()
{

    /**
     * 故意不析构：进程退出时可能有 detached finalizer 仍在跑。
     */
    static FinalizerHandoffGateForTest* gate = new FinalizerHandoffGateForTest();
    return *gate;
}

/**
 * finalizer 在「Finalize 完成、delete 责任尚未交接」处停靠，直到测试放行。
 */
void ParkFinalizerHandoffForTest()
{
    auto& gate = HandoffGate();
    std::unique_lock<std::mutex> lk(gate.mtx);
    gate.at_stage = true;
    gate.cv.notify_all();
    gate.cv.wait(lk, [&] { return !gate.paused.load(std::memory_order_acquire); });
    gate.at_stage = false;
}

/**
 * 回收观察：delete 之前记录执行线程，delete 之后累加计数——能读到计数就说明
 * 析构真的完成了。两条候选删除路径（Dispose / FinalizerLoop）都经此记账，
 * 故计数为 1 即「恰好删一次」。
 */
bool BeginReclaimObservationForTest(const MongoRuntime* owner) noexcept
{
    auto& gate = HandoffGate();
    if (gate.watched.load(std::memory_order_acquire) != owner)
        return false;
    gate.reclaim_thread.store(std::this_thread::get_id(), std::memory_order_release);
    return true;
}

void EndReclaimObservationForTest(bool observe) noexcept
{
    if (observe)
        HandoffGate().reclaim_count.fetch_add(1, std::memory_order_acq_rel);
}

/**
 * 关闭进度等待：受管协程按 `min(deadline, now+50ms)` 分片挂起，native 宿主用
 * 条件变量有界等待同一谓词。事件方（Publish / Teardown / finalizer）先落定状态
 * 再唤醒，故「登记后复查谓词」即可闭合丢唤醒窗口。谓词为真返回成功；到期返回
 * TimedOut 并保留封口与在途责任，不换模式、不续预算。
 */
template <class Pred>
result<void> WaitCloseProgress(
    std::mutex& mtx,
    std::condition_variable& cv,
    std::vector<bbt::coroutine::sync::CoWaiter::SPtr>& waiters,
    bbt::coroutine::Deadline deadline,
    const char* timeout_message,
    Pred&& pred)
{
    if (bbt::infra::detail::CurrentCloseWaitDomain() != bbt::infra::detail::CloseWaitDomain::Co) {
        std::unique_lock<std::mutex> lock(mtx);
        if (!cv.wait_until(lock, deadline, pred))
            return result<void>::err(MakeError(ErrorCode::TimedOut, timeout_message));
        return result<void>::ok();
    }
    for (;;) {
        if (pred())
            return result<void>::ok();
        if (std::chrono::steady_clock::now() >= deadline)
            return result<void>::err(MakeError(ErrorCode::TimedOut, timeout_message));
        auto waiter = bbt::coroutine::sync::CoWaiter::Create();
        if (!waiter)
            return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable, "mongo: close wait signal unavailable"));
        {
            std::lock_guard<std::mutex> lock(mtx);
            if (pred())
                return result<void>::ok();
            waiters.push_back(waiter);
        }
        auto slice = bbt::infra::detail::WaitCloseSlice(waiter, bbt::infra::detail::NextCloseSlice(deadline));
        {
            std::lock_guard<std::mutex> lock(mtx);
            auto& pending = waiters;
            pending.erase(std::remove(pending.begin(), pending.end(), waiter), pending.end());
        }
        if (!slice)
            return result<void>::err(std::move(slice).error());
    }
}

// clang-format off
}
// clang-format on

/**
 * MongoRuntime
 */
std::shared_ptr<MongoRuntime> MongoRuntime::Create(mongo::MongoRuntimeConfig config, bbt::coroutine::CoObjectInfo info)
{

    /**
     * 自定义删除器是「join 不落 Co worker」的一半：引用归零可能发生在协程 worker 上，
     * 那里既不能等 driver 也不能 pthread join。
     */
    return std::shared_ptr<MongoRuntime>(new MongoRuntime(std::move(config), std::move(info)), &MongoRuntime::Dispose);
}

void MongoRuntime::Dispose(MongoRuntime* self) noexcept
{

    /**
     * 成功启动的 owner 在 Start 时已前置创建 finalizer；引用归零只投递
     * 「收口 + delete」请求，绝不在调用线程（可能是 Co worker）执行 join。
     * 交接 delete 责任与最后一次成员访问同在一个临界区：finalizer 判定 delete_self
     * 也要取这把锁，必须先完成本线程的解锁才能拿到它，所以 notify 必然落在对象仍
     * 存活时，交出责任后除解锁外不再触碰 self。若把通知留到解锁之后，finalizer
     * 可能已经取走 delete 责任并释放对象，通知就会落在已释放的条件变量上。
     */
    {
        std::lock_guard<std::mutex> lk(self->m_finalize_mtx);
        if (self->m_finalizer_started.load(std::memory_order_acquire) && !self->m_finalize_done) {
            self->m_dispose_requested = true;
            self->m_finalize_cv.notify_one();
            return;
        }
    }

    /**
     * 无 finalizer 上下文 ⇒ 从未成功启动（worker 只在 finalizer 就绪后创建），
     * 或 finalizer 已完成：本线程直接释放，无 join。
     */
    const bool observe = BeginReclaimObservationForTest(self);
    delete self;
    EndReclaimObservationForTest(observe);
}

MongoRuntime::~MongoRuntime()
{

    /**
     * 删除器保证：finalizer 已完成，或从未成功启动（无 worker/在途 driver）。
     * 后一种情况在本线程补做幂等封口；此时没有可 join 的 worker。
     */
    if (!m_finalized.load(std::memory_order_acquire))
        Finalize();
}

result<void> MongoRuntime::Start()
{

    /**
     * Start 的前置是协程运行时已初始化（共享 executor 由此而来）；未初始化返回
     * RuntimeUnavailable，此时还不存在可回收的 worker 或 lease。
     */
    if (!g_scheduler->IsInitialized())
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable, "coroutine runtime not initialized"));

    /**
     * 先取得进程级 pool 与完成回投域；失败留在 kCreated，调用方可待
     * 条件就绪后重试 Start。
     */
    auto pool = AcquireMongoPool(EffectiveUri(m_config));
    if (!pool)
        return result<void>::err(std::move(pool).error());
    auto ready = m_engine.Start();
    if (!ready)
        return result<void>::err(std::move(ready).error());

    /**
     * 状态迁移与 lease 发布在同一临界区：与 Close 取关闭权、finalizer 归还
     * lease 互斥——消除同一 m_pool shared_ptr 对象的并发赋值/reset。
     */
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        if (m_state.load() != kCreated)
            return result<void>::err(MakeError(
                m_state.load() == kRunning ? ErrorCode::InvalidArgument : ErrorCode::Closed,
                m_state.load() == kRunning ? "mongo runtime already started" : "mongo runtime is closing or closed"));

        /**
         * 前置准备唯一 finalizer：成功启动的 owner 从此刻起就有确定回收责任，
         * 关闭/最后引用不再依赖临时建线程。失败发生在 worker 与队列创建之前，
         * 本 owner 不进入 Running，lease 随局部 pool 归还：返回有界、无 join。
         */
        if (!StartFinalizerThread())
            return result<void>::err(MakeError(ErrorCode::InternalError, "mongo: failed to prepare finalizer thread"));
        m_state.store(kRunning);
        m_pool = std::move(pool).value();
    }

    /**
     * 拉起固定上限的 worker 组；spawn 与 Close 的关闭权迁移同临界区：
     * 「关闭权已被 Close 取得（state 不再 kRunning）则不再 spawn」。
     * 线程创建失败按 stopping 收口已起线程——经同一 finalizer 完成。
     */
    bool spawn_failed = false;
    bool closed_during_start = false;
    {
        std::lock_guard<std::mutex> llock(m_lifecycle_mtx);
        std::lock_guard<std::mutex> wlock(m_workers_mtx);
        if (m_state.load() != kRunning) {
            closed_during_start = true;
        } else {
            try {

                /**
                 * 测试注入点：reserve 前失败，等价 reserve 抛
                 * bad_alloc；第 N 个 worker 创建前失败等价 emplace_back
                 * 抛 system_error（此时已起 N 个 worker）。生产路径默认
                 * kSpawnFailNone，不进入任一分支。
                 */
                if (m_spawn_fail_at.load(std::memory_order_relaxed) == kSpawnFailReserve)
                    throw std::bad_alloc();
                m_workers.reserve(m_config.worker_threads);
                for (std::size_t i = 0; i < m_config.worker_threads; ++i) {
                    if (m_spawn_fail_at.load(std::memory_order_relaxed) == i)
                        throw std::system_error(std::make_error_code(std::errc::resource_unavailable_try_again));
                    m_workers.emplace_back([this] { WorkerLoop(); });
                }
            } catch (...) {
                spawn_failed = true;
            }
        }
    }
    if (closed_during_start) {

        /**
         * 关闭权已被 Close 取得：本 owner lease 由该 Close 的 finalizer
         * 统一归还，Start 不再零散 reset。
         */
        return result<void>::err(MakeError(ErrorCode::Closed, "mongo runtime is closing or closed"));
    }
    if (spawn_failed) {

        /**
         * 与 Close/析构走同一 finalizer：封口清队列、join 已起线程、归还
         * lease、发布终态——不在 Start 内零散 join/reset。
         */
        RequestClose(MakeError(ErrorCode::Closed, "mongo runtime closed by owner"));

        /**
         * native 控制线程保持「Start 失败返回即已归还 lease」的既有同步承诺；
         * 受管协程不 join，物理收口交后台 finalizer。
         */
        if (bbt::infra::detail::CurrentCloseWaitDomain() == bbt::infra::detail::CloseWaitDomain::NativeHost) {
            auto stopped = WaitPhysicalClose(bbt::coroutine::Deadline::max());
            (void)stopped;
        }
        return result<void>::err(MakeError(ErrorCode::InternalError, "mongo: failed to spawn worker threads"));
    }

    /**
     * 起跑闩：等全部 worker 完成入口登记后再返回，消除「worker 尚未起跑、
     * live_workers 仍为 0 即被判定已收口」的窗口。m_started_workers 只增不减，
     * 且 Close 抢跑时 worker 仍先完成入口登记（不依赖 live_workers==配置数，
     * 不会自锁）；state 逃逸条件覆盖 Close 与 Start 并发。
     */
    {
        std::unique_lock<std::mutex> lk(m_workers_mtx);
        m_workers_cv.wait(lk, [this] {
            return m_started_workers == static_cast<int>(m_config.worker_threads) || m_state.load() != kRunning;
        });
    }

    /**
     * Close 抢跑并已收口：诚实返回 Closed，不把「线程已起但随即被关」
     * 冒充成功启动（IsRunning 也为假）。
     */
    if (m_state.load() != kRunning)
        return result<void>::err(MakeError(ErrorCode::Closed, "mongo runtime is closing or closed"));
    return result<void>::ok();
}

result<void> MongoRuntime::PreCheck() const
{
    if (g_bbt_tls_coroutine_co == nullptr)
        return result<void>::err(MakeError(ErrorCode::InvalidContext, "mongo command must run in coroutine context"));
    if (!m_close.IsOpen())
        return result<void>::err(MakeError(ErrorCode::Closed, "mongo runtime is closing or closed"));
    const int st = m_state.load();
    if (st != kRunning)
        return result<void>::err(MakeError(
            st == kCreated ? ErrorCode::RuntimeUnavailable : ErrorCode::Closed,
            st == kCreated ? "mongo runtime not started" : "mongo runtime is closing or closed"));
    return result<void>::ok();
}

result<MongoOpOutcome> MongoRuntime::Submit(
    std::shared_ptr<MongoCollImpl> handle,
    MongoOp::Kind kind,
    MongoDocument doc,
    MongoDocument update,
    const CallOptions& options,
    std::vector<MongoDocument> docs,
    std::size_t find_limit,
    std::size_t find_skip,
    MongoDocument find_sort,
    MongoDocument find_projection)
{
    auto pre = PreCheck();
    if (!pre)
        return result<MongoOpOutcome>::err(std::move(pre).error());

    auto sig = bbt::coroutine::sync::CoWaiter::Create();
    if (!sig)
        return result<MongoOpOutcome>::err(
            MakeError(ErrorCode::RuntimeUnavailable, "mongo: coroutine waiter unavailable"));

    auto op = std::make_shared<MongoOp>(shared_from_this(), std::move(handle), kind, std::move(doc), std::move(update));
    op->docs = std::move(docs);
    op->find_limit = find_limit;
    op->find_skip = find_skip;
    op->find_sort = std::move(find_sort);
    op->find_projection = std::move(find_projection);
    op->sig = sig;

    /**
     * 登记先于等待：与 Close 的 teardown 竞态的提交能被明确拒绝，且
     * 「已登记必被终态收口」——等待者不会因 close 竞态挂住。
     */
    if (!RegisterOp(op))
        return result<MongoOpOutcome>::err(MakeError(ErrorCode::Closed, "mongo runtime closed"));

    bbt::coroutine::WaitOptions wait;
    wait.deadline = options.deadline;

    /**
     * 范式（请求完成契约）：先登记等待事件 → 在 on_registered 内登记
     * CloseWaiters 并做唯一一次入队投递 → 随后挂起。on_registered 只在
     * 事件登记成功后执行，其内自唤醒走 CoPollEvent PENDING 不丢唤醒；
     * 登记失败（deadline 已过/取消/上下文非法）则不执行投递，op 也就
     * 从未进入队列。
     */
    bool dispatched = false;
    const auto status = sig->WaitWithCallback(wait, [&]() -> bool {
        // clang-format off

        /**
         * 1) 关闭期等待者登记：Close 已开始时自行唤醒一次并立即收口。
         */
        // clang-format on
        if (!m_waiters.Add(sig)) {
            op->phase.store(MongoOp::Phase::kDone);
            op->Finish(result<MongoOpOutcome>::err(MakeError(ErrorCode::Closed, "mongo runtime closed")));
            return true;
        }

        /**
         * 2) 唯一一次发起型投递。接纳判定与入队同临界区：满则确定性
         * Overloaded，stopping 则 Closed；接纳失败路径置 kDone 后
         * Finish，保证已登记 op 反登记。
         */
        std::optional<Error> fail;
        {
            std::lock_guard<std::mutex> lk(m_queue_mtx);
            if (!m_queue_stopping && m_queue.size() < m_config.max_queue) {
                m_queue.push_back(op);
                m_queue_cv.notify_one();
                dispatched = true;
            } else {
                fail = MakeError(
                    m_queue_stopping ? ErrorCode::Closed : ErrorCode::Overloaded,
                    m_queue_stopping ? "mongo runtime closed" : "mongo: pending queue is full");
            }
        }
        if (fail) {
            op->phase.store(MongoOp::Phase::kDone);

            /**
             * Finish 内部 MaybeUnregister→UnregisterOp 取 m_ops_mtx，
             * 与已释放的 m_queue_mtx 不构成回环。
             */
            op->Finish(result<MongoOpOutcome>::err(std::move(*fail)));
        }
        return true;
    });

    /**
     * 恢复即出账：等待位不再需要 Close 侧唤醒。
     */
    m_waiters.Remove(sig.get());

    if (status == bbt::coroutine::WaitStatus::Completed) {
        if (op->outcome)
            return std::move(*op->outcome);
        return result<MongoOpOutcome>::err(
            MakeError(ErrorCode::InternalError, "internal: mongo op completed without outcome"));
    }

    /**
     * deadline/取消首胜：未派发 op 直接摘除、不发送；已派发 op 先发布
     * 一次逻辑终态，driver 调用继续到自身超时——op/lease/payload 保活到
     * 物理收口，迟到 driver 结果只消费不交付（Finish CAS 保证唯一终态）。
     */
    if (!dispatched)
        op->phase.store(MongoOp::Phase::kDone);
    const auto wait_err = WaitStatusToError(status);
    op->Finish(result<MongoOpOutcome>::err(wait_err));
    return result<MongoOpOutcome>::err(wait_err);
}

/**
 * worker 线程域
 */
void MongoRuntime::WorkerLoop() noexcept
{
    g_worker_runtime = this;
    g_worker_thread = std::this_thread::get_id();

    /**
     * 起跑登记：在触碰任何 owner 状态之前完成，使 Start 的起跑闩可确定性
     * 判定「全部线程已起跑」。仅此一处取 m_workers_mtx，之后不再需要。
     */
    {
        std::lock_guard<std::mutex> lk(m_workers_mtx);
        ++m_started_workers;
    }
    m_workers_cv.notify_all();
    m_live_workers.fetch_add(1);
    for (;;) {
        std::shared_ptr<MongoOp> op;
        {
            std::unique_lock<std::mutex> lk(m_queue_mtx);
            m_queue_cv.wait(lk, [&] { return m_queue_stopping || !m_queue.empty(); });

            /**
             * stopping && 队列已空
             */
            if (m_queue.empty())
                break;
            op = std::move(m_queue.front());
            m_queue.pop_front();
        }

        /**
         * 已落定（deadline/取消/close）仅清除占位：phase 置 kDone 后反
         * 登记。二次检查覆盖「取出后恰被 close 落定」的竞态，不白跑 driver。
         */
        if (op->finished.load()) {
            op->phase.store(MongoOp::Phase::kDone);
            op->MaybeUnregister();
            continue;
        }
        op->phase.store(MongoOp::Phase::kRunning);
        if (op->finished.load()) {
            op->phase.store(MongoOp::Phase::kDone);
            op->MaybeUnregister();
            continue;
        }
        auto r = RunDriverCall(*op);
        op->phase.store(MongoOp::Phase::kDone);
        Publish(std::move(op), std::move(r));
    }
    m_live_workers.fetch_sub(1);
    DrainNotify();
    g_worker_runtime = nullptr;
    g_worker_thread = std::thread::id{};
}

result<MongoOpOutcome> MongoRuntime::RunDriverCall(const MongoOp& op) noexcept
{
    MongoOpOutcome out;
#ifdef BBT_INFRA_STRINGENT_DEBUG
    bbt::infra::debug::MongoThreadEvidence evidence;
    evidence.worker_thread = g_worker_runtime == this ? g_worker_thread : std::thread::id{};

    /**
     * 运行中 driver 调用记账（验收「峰值 ≤ worker_threads」）。
     */
    struct RunningGuard {
        MongoRuntime* self;

        ~RunningGuard() { self->m_obs.running_calls.fetch_sub(1); }
    };
#endif
    try {

        /**
         * 谓词/文档视图：InsertMany 用 docs 列表，Find 允许空 filter
         * （空 bytes 以空文档视图表达「匹配全部」），其余沿用既有校验。
         */
        bsoncxx::v1::document::view empty_doc_v{};
        bsoncxx::v1::document::view doc_v{};
        if (op.kind != MongoOp::Kind::InsertMany) {
            const bool empty_ok = op.kind == MongoOp::Kind::Find || op.kind == MongoOp::Kind::CountDocuments;
            if (empty_ok && op.doc.bytes.empty()) {
                doc_v = empty_doc_v;
            } else {
                auto dv = ViewOf(op.doc, op.kind == MongoOp::Kind::InsertOne ? "document" : "filter");
                if (!dv)
                    return result<MongoOpOutcome>::err(std::move(dv).error());
                doc_v = dv.value();
            }
        }
        bsoncxx::v1::document::view upd_v;
        if (op.kind == MongoOp::Kind::UpdateOne || op.kind == MongoOp::Kind::UpdateMany) {
            auto uv = ViewOf(op.update, "update");
            if (!uv)
                return result<MongoOpOutcome>::err(std::move(uv).error());
            upd_v = uv.value();
        }

        /**
         * InsertMany 的文档视图列表：接纳侧已挡空/越界，这里做最终良构校验。
         */
        std::vector<bsoncxx::v1::document::view> insert_docs;
        if (op.kind == MongoOp::Kind::InsertMany) {
            insert_docs.reserve(op.docs.size());
            for (const auto& d : op.docs) {
                auto dv = ViewOf(d, "document");
                if (!dv)
                    return result<MongoOpOutcome>::err(std::move(dv).error());
                insert_docs.push_back(dv.value());
            }
        }

#ifdef BBT_INFRA_STRINGENT_DEBUG
        const auto running = m_obs.running_calls.fetch_add(1) + 1;
        RunningGuard guard{this};
        std::size_t peak = m_obs.peak_calls.load();
        while (running > peak && !m_obs.peak_calls.compare_exchange_weak(peak, running)) {
        }
#endif

        /**
         * 同一 worker 上 acquire/use/release：满足 client 单线程亲和；
         * acquire 由 waitQueueTimeoutMS 保底，不会无限等待。目标集合
         * 由发起句柄携带——同 worker 可为不同集合服务，不新建线程。
         */
        const auto& target = op.handle->Target();
#ifdef BBT_INFRA_STRINGENT_DEBUG
        bool acquired = false;

        struct EvidenceGuard {
            MongoRuntime* runtime;
            bbt::infra::debug::MongoThreadEvidence& evidence;
            bool& acquired;

            ~EvidenceGuard()
            {
                if (!acquired)
                    return;
                evidence.release_thread = std::this_thread::get_id();
                runtime->m_obs.Record(std::move(evidence));
            }
        } evidence_guard{this, evidence, acquired};
#endif

        /**
         * acquire / use / release 的真实顺序与语义不受观测影响：
         * 观测包裹仅在宏内，Release 下不存在 evidence/guard 等对象。
         */
        auto entry = m_pool->pool.acquire();
#ifdef BBT_INFRA_STRINGENT_DEBUG
        acquired = true;
        evidence.acquire_thread = std::this_thread::get_id();
        evidence.database = target.database;
        evidence.collection = target.collection;
#endif
        auto coll = (*entry)[target.database].collection(target.collection);
#ifdef BBT_INFRA_STRINGENT_DEBUG
        evidence.use_thread = std::this_thread::get_id();
#endif
        switch (op.kind) {
        case MongoOp::Kind::InsertOne: {
            const auto r = coll.insert_one(doc_v);
            if (!r)
                return result<MongoOpOutcome>::err(
                    MakeError(ErrorCode::InternalError, "mongo: insert_one returned empty result"));
            return result<MongoOpOutcome>::ok(std::move(out));
        }
        case MongoOp::Kind::InsertMany: {

            /**
             * 有序批量插入：ordered=true（v1 默认），首个写错误即中止；
             * 此前已写入的文档保留在服务端——部分成功由 outcome.inserted
             * 表达（bulk_write_exception 的 write_errors() 给出失败下标）。
             */
            const auto r = coll.insert_many(insert_docs);
            if (!r)
                return result<MongoOpOutcome>::err(
                    MakeError(ErrorCode::InternalError, "mongo: insert_many returned empty result"));
            out.inserted = static_cast<std::int64_t>(r->inserted_count());
            return result<MongoOpOutcome>::ok(std::move(out));
        }
        case MongoOp::Kind::FindOne: {
            mongocxx::v1::find_options fo;
            fo.max_time(m_config.socket_timeout);
            auto r = coll.find_one(doc_v, fo);
            if (!r) {
                out.doc = std::nullopt;
                return result<MongoOpOutcome>::ok(std::move(out));
            }
            out.doc = DocOf(r->view());
            return result<MongoOpOutcome>::ok(std::move(out));
        }
        case MongoOp::Kind::Find: {
            mongocxx::v1::find_options fo;
            fo.limit(static_cast<std::int64_t>(op.find_limit));
            if (op.find_skip > 0)
                fo.skip(static_cast<std::int64_t>(op.find_skip));
            fo.max_time(m_config.socket_timeout);
            if (!op.find_sort.bytes.empty()) {
                auto sv = ViewOf(op.find_sort, "sort");
                if (!sv)
                    return result<MongoOpOutcome>::err(std::move(sv).error());
                fo.sort(bsoncxx::v1::document::value{sv.value()});
            }
            if (!op.find_projection.bytes.empty()) {
                auto pv = ViewOf(op.find_projection, "projection");
                if (!pv)
                    return result<MongoOpOutcome>::err(std::move(pv).error());
                fo.projection(bsoncxx::v1::document::value{pv.value()});
            }
            auto cursor = coll.find(doc_v, fo);
            std::size_t total = 0;
            for (auto&& doc : cursor) {
                const std::size_t n = doc.length();

                /**
                 * checked-add：减法预算在复制进 out.docs 之前判定，溢出/
                 * 超限不物化该文档，也不依赖 size_t 回绕后的比较。
                 */
                if (n > mongo::kMongoLimitsMaxFindResultBytes - total) {
                    Error e = MakeError(ErrorCode::RemoteError, "mongo: find result exceeds result-bytes cap");
                    e.domain_code = "result_too_large";
                    return result<MongoOpOutcome>::err(std::move(e));
                }
                total += n;
                out.docs.push_back(DocOf(doc));
            }
            return result<MongoOpOutcome>::ok(std::move(out));
        }
        case MongoOp::Kind::UpdateOne: {
            const auto r = coll.update_one(doc_v, upd_v);
            if (!r)
                return result<MongoOpOutcome>::err(
                    MakeError(ErrorCode::InternalError, "mongo: update_one returned empty result"));
            out.matched = r->matched_count();
            out.modified = r->modified_count();
            out.upserted = r->upserted_count();
            return result<MongoOpOutcome>::ok(std::move(out));
        }
        case MongoOp::Kind::UpdateMany: {
            const auto r = coll.update_many(doc_v, upd_v);
            if (!r)
                return result<MongoOpOutcome>::err(
                    MakeError(ErrorCode::InternalError, "mongo: update_many returned empty result"));
            out.matched = r->matched_count();
            out.modified = r->modified_count();
            out.upserted = r->upserted_count();
            return result<MongoOpOutcome>::ok(std::move(out));
        }
        case MongoOp::Kind::DeleteOne: {
            const auto r = coll.delete_one(doc_v);
            if (!r)
                return result<MongoOpOutcome>::err(
                    MakeError(ErrorCode::InternalError, "mongo: delete_one returned empty result"));
            out.deleted = r->deleted_count();
            return result<MongoOpOutcome>::ok(std::move(out));
        }
        case MongoOp::Kind::DeleteMany: {
            const auto r = coll.delete_many(doc_v);
            if (!r)
                return result<MongoOpOutcome>::err(
                    MakeError(ErrorCode::InternalError, "mongo: delete_many returned empty result"));
            out.deleted = r->deleted_count();
            return result<MongoOpOutcome>::ok(std::move(out));
        }
        case MongoOp::Kind::CountDocuments: {
            out.counted = static_cast<std::int64_t>(coll.count_documents(doc_v));
            return result<MongoOpOutcome>::ok(std::move(out));
        }
        }
    } catch (const mongocxx::v_noabi::operation_exception& e) {

        /**
         * CRUD 服务端失败实际抛 v_noabi::operation_exception，不是 v1::exception 子类。
         */
        return result<MongoOpOutcome>::err(ClassifyOperationError(e, "mongo: driver call failed"));
    } catch (const mongocxx::v1::exception& e) {
        return result<MongoOpOutcome>::err(ClassifyDriverError(e, "mongo: driver call failed"));
    } catch (const mongocxx::v_noabi::exception& e) {
        return result<MongoOpOutcome>::err(ClassifyVNoabiError(e, "mongo: driver call failed"));
    } catch (const bsoncxx::v1::exception& e) {
        return result<MongoOpOutcome>::err(ClassifyBsonError(e, "mongo: bson error"));
    } catch (const std::exception& e) {
        return result<MongoOpOutcome>::err(
            MakeError(ErrorCode::InternalError, std::string("mongo: driver call failed: ") + e.what()));
    } catch (...) {
        return result<MongoOpOutcome>::err(MakeError(ErrorCode::InternalError, "mongo: unknown driver exception"));
    }
    return result<MongoOpOutcome>::err(MakeError(ErrorCode::InternalError, "mongo: unreachable op kind"));
}

void MongoRuntime::Publish(const std::shared_ptr<MongoOp>& op, result<MongoOpOutcome> r)
{

    /**
     * 完成回投共享 executor：io 域落定逻辑终态并唤醒协程。
     */
    bool posted = m_engine.TryPost([op, rr = std::move(r)]() mutable { op->Finish(std::move(rr)); });
    if (!posted) {

        /**
         * 执行域不可达（引擎未启动/已封/post 分配失败）：worker 线程直接
         * 落定——CoWaiter::Notify 允许任意线程调用。
         */
        op->Finish(std::move(r));
    }

    /**
     * 已提交 op 的发布可能让 Graceful 判据成立：先落定再唤醒（不丢唤醒）。
     */
    WakePublishWaiters();
}

/**
 * 关闭链路
 */
void MongoRuntime::Close() noexcept
{
    RequestClose(MakeError(ErrorCode::Closed, "mongo runtime closed by owner"));

    /**
     * 物理收口只在 finalizer 线程执行（RequestClose 已向 Start 时前置创建的
     * finalizer 投递请求）。本线程只按执行域决定是否等它完成：native 宿主等真实
     * 物理落定（在途 driver 返回 + op 清空 + worker 全退 + join + lease 归还）后
     * 返回；受管协程与 Processer 上的非协程域立即返回，绝不把 pthread join 落在
     * Co worker 上。不用有界窗口冒充物理释放：driver 同步调用自身的超时才是其
     * 返回上界。
     */
    if (bbt::infra::detail::CurrentCloseWaitDomain() == bbt::infra::detail::CloseWaitDomain::NativeHost) {
        auto stopped = WaitPhysicalClose(bbt::coroutine::Deadline::max());
        (void)stopped;
    }
}

void MongoRuntime::RequestClose(Error reason) noexcept
{
    {
        std::lock_guard<std::mutex> lock(m_publish_mtx);
        if (!m_reason)
            m_reason = std::move(reason);
    }

    /**
     * 取关闭权与关闭态迁移在生命周期锁内：与 Start 的 pool 发布互斥。
     * 仅首个取得关闭权者执行封口/唤醒；其余合法并发/重复调用直接进入
     * 同一 finalizer 等待。
     */
    bool first = false;
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        first = m_close.BeginClose();
        if (first)
            m_state.store(kClosingOrClosed);
    }
    if (first) {
        Teardown();

        /**
         * 兜底唤醒全部挂起业务等待者：teardown 已让每个在册 op 落定
         * （结果先落地、再唤醒），此处覆盖在册但已不在 m_ops 的等待者。
         */
        m_waiters.CloseAndWakeAll();
        WakePublishWaiters();
    }

    /**
     * 前置 finalizer 由 Start 成功时创建：这里只投递收口请求，绝不在调用线程
     * 临时建线程。没有 finalizer 上下文 ⇒ 从未成功启动（无 worker/在途
     * driver），本线程同步完成同一幂等收口也是有界的、无 join 的。
     */
    if (!SignalFinalizer())
        Finalize();
}

std::optional<Error> MongoRuntime::CloseReason() const noexcept
{
    std::lock_guard<std::mutex> lock(m_publish_mtx);
    return m_reason;
}

result<void> MongoRuntime::Close(const CloseOptions& options)
{

    /**
     * 值校验先于任何副作用：非法组合不得封口、不得发布首因。
     */
    auto valid = bbt::infra::detail::ValidateCloseOptions(options);
    if (!valid)
        return valid;
    const bool waits = options.mode == CloseMode::Graceful || options.wait_release;
    const auto domain = bbt::infra::detail::CurrentCloseWaitDomain();
    if (waits && domain == bbt::infra::detail::CloseWaitDomain::Invalid)
        return result<void>::err(
            MakeError(ErrorCode::InvalidContext, "mongo: Close requires coroutine or native host"));

    RequestClose(bbt::infra::detail::SettleCloseError(options.reason, "mongo runtime closed by owner"));
    if (options.mode == CloseMode::Graceful) {

        /**
         * 已提交 op 全部拿到逻辑终态：封口路径（Teardown）已同步落定在册/排队 op，
         * 这里只覆盖可能的晚登记。已进入 driver 的调用属物理责任，不计入本等待。
         */
        auto published = WaitCloseProgress(
            m_publish_mtx,
            m_publish_cv,
            m_publish_waiters,
            options.deadline,
            "mongo: graceful close deadline expired",
            [this] { return AllOpsPublished(); });
        if (!published)
            return published;
    }
    if (!options.wait_release)
        return result<void>::ok();

    /**
     * 物理回收：同一绝对期限，等 finalizer 完成 worker 退出 + JoinWorkers + lease 归还。
     */
    return WaitPhysicalClose(options.deadline);
}

result<void> MongoRuntime::WaitPhysicalClose(bbt::coroutine::Deadline deadline)
{

    /**
     * finalizer 上下文由 Start 前置准备：本函数只等它的事实，不再按需建线程，
     * 因此 native 域也不会在超期后回落「本线程 Finalize」——等待严格受 deadline
     * 约束，不无限超过期限，也不假回收。
     */
    if (!m_finalizer_started.load(std::memory_order_acquire)) {

        /**
         * 从未成功启动（无 worker/在途 driver）：封口时已同步完成幂等收口，
         * 这里只确认终态；未封口属调用方前置条件错误。
         */
        if (!m_close.IsClosed())
            return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable, "mongo: finalizer not prepared"));
        return result<void>::ok();
    }
    return WaitCloseProgress(
        m_publish_mtx, m_publish_cv, m_publish_waiters, deadline, "mongo: physical stop deadline expired", [this] {
            return m_close.IsClosed();
        });
}

bool MongoRuntime::StartFinalizerThread() noexcept
{
    try {

        /**
         * 测试注入：等价线程资源耗尽。抛点在任何 worker 创建之前，故失败时
         * 没有可 join 的线程，Start 返回有界。
         */
        if (m_finalizer_spawn_fail_for_test.load(std::memory_order_relaxed))
            throw std::system_error(std::make_error_code(std::errc::resource_unavailable_try_again));
        std::thread([this] { FinalizerLoop(); }).detach();

        /**
         * 生命周期锁保证：调用方返回前不会有人投递封口请求，故本标志在
         * 线程开始等待之后发布即可。
         */
        m_finalizer_started.store(true, std::memory_order_release);
        return true;
    } catch (...) {
        return false;
    }
}

bool MongoRuntime::SignalFinalizer() noexcept
{
    if (!m_finalizer_started.load(std::memory_order_acquire))
        return false;
    {
        std::lock_guard<std::mutex> lk(m_finalize_mtx);
        m_finalize_requested = true;
    }
    m_finalize_cv.notify_one();
    return true;
}

void MongoRuntime::FinalizerLoop() noexcept
{
    {
        std::unique_lock<std::mutex> lk(m_finalize_mtx);
        m_finalize_cv.wait(lk, [this] { return m_finalize_requested || m_dispose_requested; });
    }
    Finalize();

    /**
     * 测试接缝：只对「被观察的 owner」在交接前一刻停靠，让最后引用归零与交接阶段
     * 确定重叠；其它 owner 与生产路径都不停靠（两次原子读）。
     */
    {
        auto& gate = HandoffGate();
        if (gate.paused.load(std::memory_order_acquire) && gate.watched.load(std::memory_order_acquire) == this)
            ParkFinalizerHandoffForTest();
    }

    /**
     * 完成事实与 delete 责任在同一临界区交接：Dispose 先到则本线程 delete；
     * 本线程先到则 Dispose 看到 done 后自行 delete。两方都不会重复释放；
     * 交权之后本线程不再触碰任何成员（回收记账用的是静态观察点）。
     */
    bool delete_self = false;
    {
        std::lock_guard<std::mutex> lk(m_finalize_mtx);
        m_finalize_done = true;
        delete_self = m_dispose_requested;
    }
    if (delete_self) {
        const bool observe = BeginReclaimObservationForTest(this);
        delete this;
        EndReclaimObservationForTest(observe);
    }
}

void MongoRuntime::SetFinalizerHandoffPauseForTest(bool pause) noexcept
{
    auto& gate = HandoffGate();
    {
        std::lock_guard<std::mutex> lk(gate.mtx);
        gate.paused.store(pause, std::memory_order_release);
        if (!pause)
            gate.at_stage = false;
    }
    gate.cv.notify_all();
}

bool MongoRuntime::WaitFinalizerHandoffStageForTest(int budget_ms) noexcept
{
    auto& gate = HandoffGate();
    std::unique_lock<std::mutex> lk(gate.mtx);
    return gate.cv.wait_for(lk, std::chrono::milliseconds{budget_ms}, [&] { return gate.at_stage; });
}

void MongoRuntime::WatchFinalizerReclaimForTest(const MongoRuntime* owner) noexcept
{
    auto& gate = HandoffGate();
    gate.reclaim_count.store(0, std::memory_order_release);
    gate.reclaim_thread.store(std::thread::id{}, std::memory_order_release);
    gate.watched.store(owner, std::memory_order_release);
}

MongoRuntime::FinalizerReclaimForTest MongoRuntime::ObservedFinalizerReclaimForTest() noexcept
{
    auto& gate = HandoffGate();
    FinalizerReclaimForTest out;
    out.count = gate.reclaim_count.load(std::memory_order_acquire);
    out.thread = gate.reclaim_thread.load(std::memory_order_acquire);
    return out;
}

result<void> MongoRuntime::WaitStopped(bbt::coroutine::Deadline deadline)
{
    if (m_close.IsOpen())
        return result<void>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo: Close or RequestClose before WaitStopped"));

    /**
     * join 只在 finalizer 线程执行：本调用只做有界 native 等待，不自己 join。
     * 协程域没有可用的等待面（会挡住 driver 完成回投），改用 Close(wait_release=true)。
     */
    if (bbt::infra::detail::CurrentCloseWaitDomain() != bbt::infra::detail::CloseWaitDomain::NativeHost)
        return result<void>::err(MakeError(ErrorCode::InvalidContext, "mongo: WaitStopped requires native host"));
    return WaitPhysicalClose(deadline);
}

void MongoRuntime::WakePublishWaiters() noexcept
{
    std::vector<bbt::coroutine::sync::CoWaiter::SPtr> woken;
    {
        std::lock_guard<std::mutex> lock(m_publish_mtx);
        woken.swap(m_publish_waiters);
    }
    m_publish_cv.notify_all();
    for (auto& waiter : woken)
        waiter->Notify();
}

bool MongoRuntime::AllOpsPublished() const noexcept
{
    std::lock_guard<std::mutex> lk(m_ops_mtx);
    for (const auto& op : m_ops) {
        if (!op->finished.load(std::memory_order_acquire))
            return false;
    }
    return true;
}

void MongoRuntime::JoinWorkers() noexcept
{
    std::lock_guard<std::mutex> jl(m_join_mtx);

    /**
     * 在 m_workers_mtx 下摘走线程组，避免与 Start 的 spawn 竞争同一向量；
     * m_join_mtx 使并发 Close 串行、后来者观察到同一 join 事实。
     */
    std::vector<std::thread> workers;
    {
        std::lock_guard<std::mutex> lk(m_workers_mtx);
        workers.swap(m_workers);
    }
    for (auto& t : workers) {
        if (!t.joinable())
            continue;

        /**
         * owner 线程非 worker；若异常地从 worker 自身调用则不能 join 自己。
         */
        if (t.get_id() == std::this_thread::get_id()) {
            t.detach();
            continue;
        }
        t.join();
    }
}

void MongoRuntime::Teardown() noexcept
{
    std::vector<std::shared_ptr<MongoOp>> snapshot;
    std::deque<std::shared_ptr<MongoOp>> queued;
    {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        if (m_io_dead)
            return;
        m_io_dead = true;
        snapshot.assign(m_ops.begin(), m_ops.end());
    }
    {
        std::lock_guard<std::mutex> lk(m_queue_mtx);
        m_queue_stopping = true;
        queued.swap(m_queue);
    }
    m_queue_cv.notify_all();
    m_engine.Seal();

    /**
     * 未派发 op：置 kDone 后以 Closed 落定（从未接触 driver）；
     * 派发中 op：先发布逻辑 Closed，物理收口由 worker 完成路径继续
     * ——op、client lease 与 payload 保活到 driver 调用返回。
     */
    for (auto& op : queued) {
        op->phase.store(MongoOp::Phase::kDone);
        op->Finish(result<MongoOpOutcome>::err(MakeError(ErrorCode::Closed, "mongo runtime closed")));
    }
    for (auto& op : snapshot)
        op->Finish(result<MongoOpOutcome>::err(MakeError(ErrorCode::Closed, "mongo runtime closed")));
    DrainNotify();
}

void MongoRuntime::DrainNotify() noexcept
{
    bool drained;
    {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        drained = m_io_dead && m_ops.empty() && m_live_workers.load() == 0;
    }
    if (drained)
        m_drain_cv.notify_all();
}

void MongoRuntime::Finalize() noexcept
{

    /**
     * 唯一配对 finalizer：call_once 使首个调用者执行，并发后来者阻塞等待
     * 同一事实——返回时 join、lease 归还、Closed 均已落定，不重复执行。
     */
    std::call_once(m_finalize_once, [this] { FinalizeOnce(); });
}

void MongoRuntime::FinalizeOnce() noexcept
{

    /**
     * 封口：保证在册/排队 op 被终态收口（幂等；Close 已封口则不重复动作，
     * Start 失败/析构路径也从这里补齐）。关闭态先于排空发布，避免新提交。
     */
    m_close.BeginClose();
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        m_state.store(kClosingOrClosed);
    }
    Teardown();

    /**
     * 真实物理落定：在途 driver 调用返回（自身超时上界）+ op 清空 +
     * worker 全退。条件满足只唤醒，不预先发布终态。
     */
    {
        std::unique_lock<std::mutex> lk(m_ops_mtx);
        m_drain_cv.wait(lk, [this] { return m_io_dead && m_ops.empty() && m_live_workers.load() == 0; });
    }

    /**
     * join：worker 线程真正退出后才返回；此后不再有触 this 的收尾。
     */
    JoinWorkers();

    /**
     * 归还本 owner 的 pool lease：只释放本 owner 的引用；进程级 instance
     * 强持于 ProcessState、同 URI 其他 owner 的 lease 引用不受影响。
     */
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        m_pool.reset();
    }

    /**
     * 终态发布：join 与 lease 归还之后才 Closed（一次性 closed hook）。
     */
    m_close.MarkClosed();
    m_finalized.store(true, std::memory_order_release);

    /**
     * 物理回收完成即关闭进度事件：唤醒 Close(wait_release=true)/WaitStopped 的等待者。
     */
    WakePublishWaiters();
}

// clang-format off
}
// clang-format on

namespace bbt::infra::mongo_detail {

MongoOp::MongoOp(
    std::shared_ptr<MongoRuntime> rt, std::shared_ptr<MongoCollImpl> h, Kind k, MongoDocument d, MongoDocument u)
    : runtime(std::move(rt))
    , handle(std::move(h))
    , kind(k)
    , doc(std::move(d))
    , update(std::move(u))
{
}

void MongoOp::Finish(result<MongoOpOutcome> r) noexcept
{
    if (finished.exchange(true)) {
        MaybeUnregister();
        return;
    }
    outcome = std::move(r);
    MaybeUnregister();

    /**
     * 结果先落地、再唤醒：等待者恢复后直接读 outcome，不依赖载荷。
     * Notify 跨线程安全；等待者尚未 park 时走 CoPollEvent PENDING 兑现。
     */
    sig->Notify();
}

MongoRuntime::MongoRuntime(mongo::MongoRuntimeConfig config, bbt::coroutine::CoObjectInfo info)
    : m_config(std::move(config))
    , m_info(std::move(info))
{
}

bbt::coroutine::CoObjectInfo MongoRuntime::GetObjectInfo() const
{
    return m_info;
}

bool MongoRuntime::RegisterOp(const std::shared_ptr<MongoOp>& op)
{
    std::lock_guard<std::mutex> lk(m_ops_mtx);
    if (m_io_dead)
        return false;
    m_ops.insert(op);
    return true;
}

void MongoRuntime::UnregisterOp(const std::shared_ptr<MongoOp>& op)
{
    bool drained = false;
    {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        m_ops.erase(op);
        drained = m_io_dead && m_ops.empty() && m_live_workers.load() == 0;
    }

    /**
     * 只唤醒排空等待者；终态发布归唯一 finalizer（join 之后），
     * 不在此处提前 MarkClosed。
     */
    if (drained)
        m_drain_cv.notify_all();
}

std::size_t MongoRuntime::QueuedOpsForTest() const noexcept
{
    std::lock_guard<std::mutex> lk(m_queue_mtx);
    return m_queue.size();
}

std::size_t MongoRuntime::DriverCallsInFlightForTest() const noexcept
{
    std::lock_guard<std::mutex> lk(m_ops_mtx);
    std::size_t n = 0;
    for (const auto& op : m_ops)
        if (op->phase.load() == MongoOp::Phase::kRunning)
            ++n;
    return n;
}

bool MongoRuntime::IoDeadForTest() const noexcept
{
    std::lock_guard<std::mutex> lk(m_ops_mtx);
    return m_io_dead;
}

// clang-format off
}
// clang-format on

namespace bbt::infra::mongo_detail {
#ifdef BBT_INFRA_STRINGENT_DEBUG
std::optional<bbt::infra::debug::MongoThreadEvidence> MongoRuntime::LastDriverThreadEvidenceForTest() const
{
    return m_obs.LastEvidence();
}
#endif
// clang-format off
}
// clang-format on
