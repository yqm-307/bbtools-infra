#include "mongo/MongoRuntime.hpp"

#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/object/CoObject.hpp>

#include <bsoncxx/v1/document/value.hpp>
#include <bsoncxx/v1/document/view.hpp>
#include <bsoncxx/v1/exception.hpp>
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/v1/collection.hpp>
#include <mongocxx/v1/database.hpp>
#include <mongocxx/v1/delete_one_result.hpp>
#include <mongocxx/v1/find_options.hpp>
#include <mongocxx/v1/insert_one_result.hpp>
#include <mongocxx/v1/server_error.hpp>
#include <mongocxx/v1/update_one_result.hpp>

namespace bbt::infra::mongo_detail {

namespace {

Error InvalidArg(std::string msg) {
    return MakeError(ErrorCode::InvalidArgument, std::move(msg));
}

} // namespace

// ============================ MongoRuntime ============================

MongoRuntime::~MongoRuntime() {
    // 外部引用归零才析构：worker 不经自身持 runtime（只经 op.runtime
    // 间接保活），这里先收口再 join——driver 调用有超时上界，join 收敛。
    Teardown();
    for (auto& t : m_workers)
        if (t.joinable())
            t.join();
}

result<void> MongoRuntime::Start() {
    const auto gen = bbt::coroutine::CurrentRuntimeGeneration();
    if (gen == 0 || gen != m_info.generation)
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "scheduler not running or runtime generation mismatch"));

    // 先取得进程级 pool 与完成回投域；失败留在 kCreated，调用方可待
    // 条件就绪后重试 Start。
    auto pool = AcquireMongoPool(EffectiveUri(m_config));
    if (!pool)
        return result<void>::err(std::move(pool).error());
    auto ready = m_engine.Start();
    if (!ready)
        return result<void>::err(std::move(ready).error());

    int expected = kCreated;
    if (!m_state.compare_exchange_strong(expected, kRunning))
        return result<void>::err(MakeError(
            m_state.load() == kRunning ? ErrorCode::InvalidArgument
                                       : ErrorCode::Closed,
            m_state.load() == kRunning ? "mongo runtime already started"
                                       : "mongo runtime is closing or closed"));
    m_pool = std::move(pool).value();

    // 拉起固定上限的 worker 组；线程创建失败时按 stopping 收口已起
    // 线程并 join（此时 op 队列尚空，无悬挂）。
    try {
        m_workers.reserve(m_config.worker_threads);
        for (std::size_t i = 0; i < m_config.worker_threads; ++i)
            m_workers.emplace_back([this] { WorkerLoop(); });
    } catch (...) {
        {
            std::lock_guard<std::mutex> lk(m_queue_mtx);
            m_queue_stopping = true;
        }
        m_queue_cv.notify_all();
        for (auto& t : m_workers)
            if (t.joinable())
                t.join();
        m_workers.clear();
        m_state.store(kClosingOrClosed);
        m_pool.reset();
        return result<void>::err(MakeError(ErrorCode::InternalError,
            "mongo: failed to spawn worker threads"));
    }
    return result<void>::ok();
}

result<void> MongoRuntime::PreCheck() const {
    if (g_bbt_tls_coroutine_co == nullptr)
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
            "mongo command must run in coroutine context"));
    if (!m_close.IsOpen())
        return result<void>::err(MakeError(ErrorCode::Closed,
            "mongo runtime is closing or closed"));
    const int st = m_state.load();
    if (st != kRunning)
        return result<void>::err(MakeError(
            st == kCreated ? ErrorCode::RuntimeUnavailable
                           : ErrorCode::Closed,
            st == kCreated ? "mongo runtime not started"
                           : "mongo runtime is closing or closed"));
    return result<void>::ok();
}

result<MongoOpOutcome> MongoRuntime::Submit(
    std::shared_ptr<MongoCollImpl> handle, MongoOp::Kind kind,
    MongoDocument doc, MongoDocument update, const CallOptions& options) {
    auto pre = PreCheck();
    if (!pre)
        return result<MongoOpOutcome>::err(std::move(pre).error());

    auto sig = NewCompletionSignal();
    if (!sig)
        return result<MongoOpOutcome>::err(std::move(sig).error());

    auto op = std::make_shared<MongoOp>(shared_from_this(),
                                        std::move(handle), kind,
                                        std::move(doc), std::move(update));
    op->sig = std::move(sig).value();

    // 登记先于入队：与 Teardown 竞态的提交也能被明确拒绝。
    if (!RegisterOp(op))
        return result<MongoOpOutcome>::err(
            MakeError(ErrorCode::Closed, "mongo runtime closed"));

    // 队列接纳判定与入队同临界区：满则确定性 Overloaded，stopping 则
    // Closed；接纳失败路径置 kDone 后 Finish，保证已登记 op 反登记。
    {
        std::lock_guard<std::mutex> lk(m_queue_mtx);
        if (!m_queue_stopping &&
            m_queue.size() < m_config.max_queue) {
            m_queue.push_back(op);
            m_queue_cv.notify_one();
        } else {
            const auto code = m_queue_stopping ? ErrorCode::Closed
                                               : ErrorCode::Overloaded;
            op->phase.store(MongoOp::Phase::kDone);
            // Finish 内部 MaybeUnregister→UnregisterOp 取 m_ops_mtx，
            // 与当前 m_queue_mtx 不构成回环（无路径持 ops_mtx 取
            // queue_mtx）。
            op->Finish(result<MongoOpOutcome>::err(MakeError(code,
                m_queue_stopping ? "mongo runtime closed"
                                 : "mongo: pending queue is full")));
            return result<MongoOpOutcome>::err(MakeError(code,
                m_queue_stopping ? "mongo runtime closed"
                                 : "mongo: pending queue is full"));
        }
    }

    bbt::coroutine::WaitOptions wait;
    wait.deadline = options.deadline;
    wait.cancel   = options.cancel;
    const auto status = op->sig->Wait(wait);
    if (status == bbt::coroutine::WaitStatus::Completed)
        return std::move(*op->outcome);

    // 逻辑终态先行发布一次：queued op 由 worker 跳过，running op 的
    // driver 调用继续到自身超时——op/lease/payload 保活到物理收口。
    op->Finish(result<MongoOpOutcome>::err(
        WaitStatusToError(status)));
    return result<MongoOpOutcome>::err(WaitStatusToError(status));
}

// ---------------- worker 线程域 ----------------

void MongoRuntime::WorkerLoop() noexcept {
    m_live_workers.fetch_add(1);
    for (;;) {
        std::shared_ptr<MongoOp> op;
        {
            std::unique_lock<std::mutex> lk(m_queue_mtx);
            m_queue_cv.wait(lk, [&] {
                return m_queue_stopping || !m_queue.empty();
            });
            if (m_queue.empty())
                break;   // stopping && 队列已空
            op = std::move(m_queue.front());
            m_queue.pop_front();
        }
        // 已落定（超时/取消/close）仅清除占位：phase 置 kDone 后反登记。
        // 二次检查覆盖「取出后恰被 close 落定」的竞态，不白跑 driver。
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
    DrainCheckClosed();
}

result<MongoOpOutcome> MongoRuntime::RunDriverCall(
    const MongoOp& op) noexcept {
    MongoOpOutcome out;
    // 运行中 driver 调用记账（验收「峰值 ≤ worker_threads」）。
    struct RunningGuard {
        MongoRuntime* self;
        ~RunningGuard() { self->m_running_calls.fetch_sub(1); }
    };
    try {
        auto doc_v = ViewOf(op.doc,
                            op.kind == MongoOp::Kind::InsertOne
                                ? "document"
                                : "filter");
        if (!doc_v)
            return result<MongoOpOutcome>::err(std::move(doc_v).error());
        bsoncxx::v1::document::view upd_v;
        if (op.kind == MongoOp::Kind::UpdateOne) {
            auto uv = ViewOf(op.update, "update");
            if (!uv)
                return result<MongoOpOutcome>::err(std::move(uv).error());
            upd_v = uv.value();
        }

        const auto running = m_running_calls.fetch_add(1) + 1;
        RunningGuard guard{this};
        std::size_t  peak = m_peak_calls.load();
        while (running > peak &&
               !m_peak_calls.compare_exchange_weak(peak, running)) {
        }

        // 同一 worker 上 acquire/use/release：满足 client 单线程亲和；
        // acquire 由 waitQueueTimeoutMS 保底，不会无限等待。目标集合
        // 由发起句柄携带——同 worker 可为不同集合服务，不新建线程。
        const auto& target = op.handle->Target();
        auto entry = m_pool->pool.acquire();
        auto coll  = (*entry)[target.database]
                        .collection(target.collection);
        switch (op.kind) {
        case MongoOp::Kind::InsertOne: {
            const auto r = coll.insert_one(doc_v.value());
            if (!r)
                return result<MongoOpOutcome>::err(MakeError(
                    ErrorCode::InternalError,
                    "mongo: insert_one returned empty result"));
            return result<MongoOpOutcome>::ok(std::move(out));
        }
        case MongoOp::Kind::FindOne: {
            mongocxx::v1::find_options fo;
            fo.max_time(m_config.socket_timeout);
            auto r = coll.find_one(doc_v.value(), fo);
            if (!r) {
                out.doc = std::nullopt;
                return result<MongoOpOutcome>::ok(std::move(out));
            }
            out.doc = DocOf(r->view());
            return result<MongoOpOutcome>::ok(std::move(out));
        }
        case MongoOp::Kind::UpdateOne: {
            const auto r = coll.update_one(doc_v.value(), upd_v);
            if (!r)
                return result<MongoOpOutcome>::err(MakeError(
                    ErrorCode::InternalError,
                    "mongo: update_one returned empty result"));
            out.matched  = r->matched_count();
            out.modified = r->modified_count();
            out.upserted = r->upserted_count();
            return result<MongoOpOutcome>::ok(std::move(out));
        }
        case MongoOp::Kind::DeleteOne: {
            const auto r = coll.delete_one(doc_v.value());
            if (!r)
                return result<MongoOpOutcome>::err(MakeError(
                    ErrorCode::InternalError,
                    "mongo: delete_one returned empty result"));
            out.deleted = r->deleted_count();
            return result<MongoOpOutcome>::ok(std::move(out));
        }
        }
    } catch (const mongocxx::v_noabi::operation_exception& e) {
        // CRUD 服务端失败在 r4.x 的实际抛出类型（非 v1::exception 子类）。
        return result<MongoOpOutcome>::err(
            ClassifyOperationError(e, "mongo: driver call failed"));
    } catch (const mongocxx::v1::exception& e) {
        return result<MongoOpOutcome>::err(
            ClassifyDriverError(e, "mongo: driver call failed"));
    } catch (const mongocxx::v_noabi::exception& e) {
        return result<MongoOpOutcome>::err(
            ClassifyVNoabiError(e, "mongo: driver call failed"));
    } catch (const bsoncxx::v1::exception& e) {
        return result<MongoOpOutcome>::err(
            ClassifyBsonError(e, "mongo: bson error"));
    } catch (const std::exception& e) {
        return result<MongoOpOutcome>::err(MakeError(
            ErrorCode::InternalError,
            std::string("mongo: driver call failed: ") + e.what()));
    } catch (...) {
        return result<MongoOpOutcome>::err(MakeError(
            ErrorCode::InternalError, "mongo: unknown driver exception"));
    }
    return result<MongoOpOutcome>::err(MakeError(
        ErrorCode::InternalError, "mongo: unreachable op kind"));
}

void MongoRuntime::Publish(const std::shared_ptr<MongoOp>& op,
                           result<MongoOpOutcome>          r) {
    // 完成回投共享 executor（Issue #7）：io 域落定逻辑终态并唤醒协程。
    if (m_engine.TryPost([op, rr = std::move(r)]() mutable {
            op->Finish(std::move(rr));
        }))
        return;
    // 执行域不可达（引擎未启动/已封/post 分配失败）：worker 线程直接
    // 落定——CompletionSignal::Complete 允许任意线程调用。
    op->Finish(std::move(r));
}

// ---------------- 关闭链路 ----------------

void MongoRuntime::RequestClose() noexcept {
    if (!m_close.BeginClose())
        return;
    m_state.store(kClosingOrClosed);
    Teardown();
}

void MongoRuntime::Teardown() noexcept {
    std::vector<std::shared_ptr<MongoOp>>    snapshot;
    std::deque<std::shared_ptr<MongoOp>>     queued;
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

    // 未派发 op：置 kDone 后以 Closed 落定（从未接触 driver）；
    // 运行中 op：先发布逻辑 Closed，物理收口由 worker 完成路径继续
    // ——op、client lease 与 payload 保活到 driver 调用返回。
    for (auto& op : queued) {
        op->phase.store(MongoOp::Phase::kDone);
        op->Finish(result<MongoOpOutcome>::err(
            MakeError(ErrorCode::Closed, "mongo runtime closed")));
    }
    for (auto& op : snapshot)
        op->Finish(result<MongoOpOutcome>::err(
            MakeError(ErrorCode::Closed, "mongo runtime closed")));
    DrainCheckClosed();
}

void MongoRuntime::DrainCheckClosed() noexcept {
    bool fin;
    {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        fin = m_io_dead && m_ops.empty() && m_live_workers.load() == 0;
    }
    if (fin)
        m_close.MarkClosed();
}

// ============================ MongoCollImpl ===========================

namespace {

// 句柄命令公共路径：句柄接纳态校验（Closed 不再收新命令）→ 目标集
// 合由句柄携带进 op → runtime 提交。句柄关闭不取消在途 op：op 保活
// handle 与 runtime，物理收口照常。
result<MongoOpOutcome> CollSubmit(
    const std::shared_ptr<MongoCollImpl>& self, MongoOp::Kind kind,
    MongoDocument doc, MongoDocument update, const CallOptions& options) {
    if (!self->Accepting())
        return result<MongoOpOutcome>::err(MakeError(
            ErrorCode::Closed, "mongo collection handle is closed"));
    return self->Runtime()->Submit(self, kind, std::move(doc),
                                   std::move(update), options);
}

} // namespace

result<void> MongoCollImpl::InsertOne(const MongoDocument& doc,
                                      const CallOptions&   options) {
    if (doc.bytes.empty())
        return result<void>::err(InvalidArg("mongo InsertOne: empty document"));
    auto r = CollSubmit(shared_from_this(), MongoOp::Kind::InsertOne,
                        doc, {}, options);
    if (!r)
        return result<void>::err(std::move(r).error());
    return result<void>::ok();
}

result<std::optional<MongoDocument>> MongoCollImpl::FindOne(
    const MongoDocument& filter, const CallOptions& options) {
    if (filter.bytes.empty())
        return result<std::optional<MongoDocument>>::err(
            InvalidArg("mongo FindOne: empty filter"));
    auto r = CollSubmit(shared_from_this(), MongoOp::Kind::FindOne,
                        filter, {}, options);
    if (!r)
        return result<std::optional<MongoDocument>>::err(
            std::move(r).error());
    return result<std::optional<MongoDocument>>::ok(
        std::move(r).value().doc);
}

result<MongoUpdateResult> MongoCollImpl::UpdateOne(
    const MongoDocument& filter, const MongoDocument& update,
    const CallOptions& options) {
    if (filter.bytes.empty())
        return result<MongoUpdateResult>::err(
            InvalidArg("mongo UpdateOne: empty filter"));
    if (update.bytes.empty())
        return result<MongoUpdateResult>::err(
            InvalidArg("mongo UpdateOne: empty update"));
    auto r = CollSubmit(shared_from_this(), MongoOp::Kind::UpdateOne,
                        filter, update, options);
    if (!r)
        return result<MongoUpdateResult>::err(std::move(r).error());
    MongoUpdateResult out;
    out.matched  = r.value().matched;
    out.modified = r.value().modified;
    out.upserted = r.value().upserted;
    return result<MongoUpdateResult>::ok(out);
}

result<std::uint64_t> MongoCollImpl::DeleteOne(
    const MongoDocument& filter, const CallOptions& options) {
    if (filter.bytes.empty())
        return result<std::uint64_t>::err(
            InvalidArg("mongo DeleteOne: empty filter"));
    auto r = CollSubmit(shared_from_this(), MongoOp::Kind::DeleteOne,
                        filter, {}, options);
    if (!r)
        return result<std::uint64_t>::err(std::move(r).error());
    return result<std::uint64_t>::ok(
        static_cast<std::uint64_t>(r.value().deleted < 0 ? 0
                                                        : r.value().deleted));
}

// ===================== CoMongoDb（owner 公共入口）=====================

namespace {

// CoMongoDbImpl：owner 的对外对象——持有 MongoRuntime 并把生命周期
// 语义透传给 ICoCloseable；Collection() 负责句柄装配。
class CoMongoDbImpl : public mongo::CoMongoDb {
public:
    CoMongoDbImpl(std::shared_ptr<MongoRuntime> runtime,
                  bbt::coroutine::CoObjectInfo  info,
                  std::shared_ptr<bbt::coroutine::CompletionSignal> close_sig)
        : m_runtime(std::move(runtime)),
          m_info(std::move(info)),
          m_close(std::move(close_sig)) {}
    ~CoMongoDbImpl() override { RequestClose(); }

    result<void> Start() override { return m_runtime->Start(); }

    result<std::shared_ptr<mongo::CoMongoColl>> Collection(
        mongo::MongoTarget target) override {
        auto valid = mongo::ValidateMongoTarget(target);
        if (!valid)
            return result<std::shared_ptr<mongo::CoMongoColl>>::err(
                std::move(valid).error());
        if (!m_close.IsOpen())
            return result<std::shared_ptr<mongo::CoMongoColl>>::err(
                MakeError(ErrorCode::Closed,
                          "mongo owner is closing or closed"));
        // 公共契约：owner 必须 Running 才接纳新句柄（Client.hpp）。
        if (!m_runtime->IsRunning())
            return result<std::shared_ptr<mongo::CoMongoColl>>::err(
                MakeError(ErrorCode::RuntimeUnavailable,
                          "mongo owner not started"));
        auto info = NewObjectInfo("infra.mongo_coll");
        if (!info)
            return result<std::shared_ptr<mongo::CoMongoColl>>::err(
                std::move(info).error());
        auto sig = NewCompletionSignal();
        if (!sig)
            return result<std::shared_ptr<mongo::CoMongoColl>>::err(
                std::move(sig).error());
        auto h = std::make_shared<MongoCollImpl>(
            m_runtime, std::move(target), std::move(info).value(),
            std::move(sig).value());
        return result<std::shared_ptr<mongo::CoMongoColl>>::ok(
            std::move(h));
    }

    void RequestClose() noexcept override {
        if (!m_close.BeginClose())
            return;
        // owner 关闭只触发 runtime 收口（队列排空 + worker drain 在
        // runtime 内进行）；不在此阻塞等 drain。
        m_runtime->RequestClose();
    }
    // IsClosed 诚实反映物理收口：runtime 的 ops 清空 + worker 全退
    // 才 Closed，不把「已发起关闭」冒充「物理完成」。
    bool IsClosed() const noexcept override {
        return m_runtime->IsClosed();
    }
    CloseStatus WaitClosed(
        bbt::coroutine::Deadline          deadline,
        bbt::coroutine::CancellationToken cancel) override {
        // 委托 runtime 的关闭态机：等的是同一个物理 drain；单等待位
        // 约束（AlreadyWaiting）与超时/取消语义保持一致。
        return m_runtime->WaitClosed(deadline, std::move(cancel));
    }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return m_info;
    }

    std::shared_ptr<MongoRuntime> Runtime() const { return m_runtime; }

private:
    std::shared_ptr<MongoRuntime>            m_runtime;
    bbt::coroutine::CoObjectInfo             m_info;
    ManagedCloseState                        m_close;
};

} // namespace

// ============================ CoMongoCliImpl ==========================
// 旧契约：一个 client = 独占 runtime + 一个集合句柄。资源归属不变式
// 全部由 runtime 承担；本层只做配置翻译与状态转发。

namespace {

// 未 Start（m_coll 为空）时的前置校验，与旧 CoMongoCliImpl::PreCheck
// 顺序一致：协程外 → InvalidContext；已关 → Closed；其余（协程内、
// close 仍 open、未 Start）→ RuntimeUnavailable。m_coll 存在时该路径
// 不触达，由 runtime Submit→PreCheck 走同一优先级。
result<void> CliPrecheckNoColl(const ManagedCloseState& close) {
    if (g_bbt_tls_coroutine_co == nullptr)
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
            "mongo command must run in coroutine context"));
    return result<void>::err(MakeError(
        !close.IsOpen() ? ErrorCode::Closed
                        : ErrorCode::RuntimeUnavailable,
        !close.IsOpen() ? "mongo client is closing or closed"
                        : "mongo client not started"));
}

} // namespace

result<void> CoMongoCliImpl::Start() {
    const auto gen = bbt::coroutine::CurrentRuntimeGeneration();
    if (gen == 0 || gen != m_info.generation)
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "scheduler not running or runtime generation mismatch"));
    if (!m_close.IsOpen())
        return result<void>::err(MakeError(ErrorCode::Closed,
            "mongo client is closing or closed"));
    if (m_runtime)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo client already started"));

    // 独占 runtime：资源预算照搬 MongoClientConfig 的 worker/queue/
    // 超时项；旧 client 语义保持「一个 client 一组 worker」，但资源
    // 实体收进显式 owner。
    mongo::MongoRuntimeConfig rcfg;
    rcfg.uri                      = m_config.uri;
    rcfg.worker_threads           = m_config.worker_threads;
    rcfg.max_queue                = m_config.max_queue;
    rcfg.server_selection_timeout = m_config.server_selection_timeout;
    rcfg.connect_timeout          = m_config.connect_timeout;
    rcfg.socket_timeout           = m_config.socket_timeout;
    rcfg.wait_queue_timeout       = m_config.wait_queue_timeout;

    auto rt_info = NewObjectInfo("infra.mongo_rt");
    if (!rt_info)
        return result<void>::err(std::move(rt_info).error());
    auto rt_sig = NewCompletionSignal();
    if (!rt_sig)
        return result<void>::err(std::move(rt_sig).error());
    auto rt = std::make_shared<MongoRuntime>(
        std::move(rcfg), std::move(rt_info).value(),
        std::move(rt_sig).value());
    auto started = rt->Start();
    if (!started)
        return result<void>::err(std::move(started).error());

    auto h_info = NewObjectInfo("infra.mongo_coll");
    if (!h_info) {
        rt->RequestClose();
        return result<void>::err(std::move(h_info).error());
    }
    auto h_sig = NewCompletionSignal();
    if (!h_sig) {
        rt->RequestClose();
        return result<void>::err(std::move(h_sig).error());
    }
    m_coll = std::make_shared<MongoCollImpl>(
        rt, mongo::MongoTarget{m_config.database, m_config.collection},
        std::move(h_info).value(), std::move(h_sig).value());
    m_runtime = std::move(rt);
    return result<void>::ok();
}

result<void> CoMongoCliImpl::InsertOne(const MongoDocument& doc,
                                       const CallOptions&   options) {
    if (doc.bytes.empty())
        return result<void>::err(InvalidArg("mongo InsertOne: empty document"));
    if (!m_coll) {
        auto pre = CliPrecheckNoColl(m_close);
        return result<void>::err(std::move(pre).error());
    }
    auto r = m_coll->Runtime()->Submit(m_coll, MongoOp::Kind::InsertOne,
                                       doc, {}, options);
    if (!r)
        return result<void>::err(std::move(r).error());
    return result<void>::ok();
}

result<std::optional<MongoDocument>> CoMongoCliImpl::FindOne(
    const MongoDocument& filter, const CallOptions& options) {
    if (filter.bytes.empty())
        return result<std::optional<MongoDocument>>::err(
            InvalidArg("mongo FindOne: empty filter"));
    if (!m_coll) {
        auto pre = CliPrecheckNoColl(m_close);
        return result<std::optional<MongoDocument>>::err(
            std::move(pre).error());
    }
    auto r = m_coll->Runtime()->Submit(m_coll, MongoOp::Kind::FindOne,
                                       filter, {}, options);
    if (!r)
        return result<std::optional<MongoDocument>>::err(
            std::move(r).error());
    return result<std::optional<MongoDocument>>::ok(
        std::move(r).value().doc);
}

result<MongoUpdateResult> CoMongoCliImpl::UpdateOne(
    const MongoDocument& filter, const MongoDocument& update,
    const CallOptions& options) {
    if (filter.bytes.empty())
        return result<MongoUpdateResult>::err(
            InvalidArg("mongo UpdateOne: empty filter"));
    if (update.bytes.empty())
        return result<MongoUpdateResult>::err(
            InvalidArg("mongo UpdateOne: empty update"));
    if (!m_coll) {
        auto pre = CliPrecheckNoColl(m_close);
        return result<MongoUpdateResult>::err(std::move(pre).error());
    }
    auto r = m_coll->Runtime()->Submit(m_coll, MongoOp::Kind::UpdateOne,
                                       filter, update, options);
    if (!r)
        return result<MongoUpdateResult>::err(std::move(r).error());
    MongoUpdateResult out;
    out.matched  = r.value().matched;
    out.modified = r.value().modified;
    out.upserted = r.value().upserted;
    return result<MongoUpdateResult>::ok(out);
}

result<std::uint64_t> CoMongoCliImpl::DeleteOne(
    const MongoDocument& filter, const CallOptions& options) {
    if (filter.bytes.empty())
        return result<std::uint64_t>::err(
            InvalidArg("mongo DeleteOne: empty filter"));
    if (!m_coll) {
        auto pre = CliPrecheckNoColl(m_close);
        return result<std::uint64_t>::err(std::move(pre).error());
    }
    auto r = m_coll->Runtime()->Submit(m_coll, MongoOp::Kind::DeleteOne,
                                       filter, {}, options);
    if (!r)
        return result<std::uint64_t>::err(std::move(r).error());
    return result<std::uint64_t>::ok(
        static_cast<std::uint64_t>(r.value().deleted < 0 ? 0
                                                        : r.value().deleted));
}

void CoMongoCliImpl::RequestClose() noexcept {
    if (!m_close.BeginClose())
        return;
    if (m_coll)
        m_coll->RequestClose();
    if (m_runtime)
        m_runtime->RequestClose();
    else
        // 未 Start：无 runtime 可 drain，本对象信号立即落定。
        m_close.MarkClosed();
}

} // namespace bbt::infra::mongo_detail

namespace bbt::infra {

// 旧契约装配入口：Create 只校验装配参数并取对象身份/完成信号，
// Start 才真正占用资源；二者均在控制线程使用，不挂起协程。
result<std::shared_ptr<CoMongoCli>>
CoMongoCli::Create(MongoClientConfig config) {
    auto valid = ValidateMongoClientConfig(config);
    if (!valid)
        return result<std::shared_ptr<CoMongoCli>>::err(
            std::move(valid).error());
    auto info = mongo_detail::NewObjectInfo("infra.mongo_cli");
    if (!info)
        return result<std::shared_ptr<CoMongoCli>>::err(
            std::move(info).error());
    auto sig = mongo_detail::NewCompletionSignal();
    if (!sig)
        return result<std::shared_ptr<CoMongoCli>>::err(
            std::move(sig).error());
    auto impl = std::make_shared<mongo_detail::CoMongoCliImpl>(
        std::move(config), std::move(info).value(), std::move(sig).value());
    return result<std::shared_ptr<CoMongoCli>>::ok(std::move(impl));
}

namespace mongo {

// owner 装配入口：与 CoMongoCli::Create 同纪律——Create 校验+身份，
// Start 占用资源。
result<std::shared_ptr<CoMongoDb>>
CoMongoDb::Create(MongoRuntimeConfig config) {
    auto valid = ValidateMongoRuntimeConfig(config);
    if (!valid)
        return result<std::shared_ptr<CoMongoDb>>::err(
            std::move(valid).error());
    auto info = mongo_detail::NewObjectInfo("infra.mongo_db");
    if (!info)
        return result<std::shared_ptr<CoMongoDb>>::err(
            std::move(info).error());
    auto sig = mongo_detail::NewCompletionSignal();
    if (!sig)
        return result<std::shared_ptr<CoMongoDb>>::err(
            std::move(sig).error());
    auto rt_info = mongo_detail::NewObjectInfo("infra.mongo_rt");
    if (!rt_info)
        return result<std::shared_ptr<CoMongoDb>>::err(
            std::move(rt_info).error());
    auto rt_sig = mongo_detail::NewCompletionSignal();
    if (!rt_sig)
        return result<std::shared_ptr<CoMongoDb>>::err(
            std::move(rt_sig).error());
    auto rt = std::make_shared<mongo_detail::MongoRuntime>(
        std::move(config), std::move(rt_info).value(),
        std::move(rt_sig).value());
    auto impl = std::make_shared<mongo_detail::CoMongoDbImpl>(
        std::move(rt), std::move(info).value(), std::move(sig).value());
    return result<std::shared_ptr<CoMongoDb>>::ok(std::move(impl));
}

} // namespace mongo
} // namespace bbt::infra
