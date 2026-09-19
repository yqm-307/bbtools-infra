#pragma once
// CoMongoCliImpl：协程原生 MongoDB 客户端实现（mongocxx 同步 driver
// + 有界 worker bridge，Issue #7 裁决形态）。
//
// 桥接方式：命令在协程内发起，堆上 MongoOp 自持有文档字节/sig/结果；
// 固定 worker_threads 个 std::thread 从有界队列取出 op，在同一 worker
// 上 acquire/use/release mongocxx::pool client 并执行阻塞 driver 调用；
// 完成经共享 executor 回投后 Complete CompletionSignal 唤醒等待协程
// （executor 不可达时 worker 线程直接 Complete——CompletionSignal 允许
// 任意线程调用）。不新建 io_context；worker 是本模块唯一新增线程来源
// （driver 自身 SDAM 监控线程属 pool 内部，不占 worker_threads 额度）。
//
// 容量语义：
//   - m_queue 等待队列 ≤ max_queue；满则新命令立即 Overloaded；
//   - 运行中 driver 调用峰值 ≤ worker_threads（m_running_calls/
//     m_peak_calls 记账，供验收核验）；
//   - 每项 operation 在同一 worker 上 acquire/use/release client lease；
//     driver 同步调用不可强杀：deadline/cancel/close 先发布一次逻辑
//     终态，已进入 driver 的调用继续到 driver timeout，op、lease 与
//     payload 保活到物理收口。
//
// 物理清理（契约 §逻辑结果与物理清理分离）：
//   - op 在 finished 且后端不再访问（phase==kDone）才离开 m_ops；
//   - m_io_dead && m_ops 空 && worker 全部退出 ⇒ MarkClosed；
//   - 析构对全部 worker join——driver 超时保上界，join 必然收敛。

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_set>
#include <vector>

#include <bbt/infra/CoMongoCli.hpp>

#include "mongo/MongoDetail.hpp"
#include "mongo/MongoProcess.hpp"
#include "mongo/MongoSupport.hpp"

namespace bbt::infra::mongo_detail {

class CoMongoCliImpl;

// 一次命令的堆上 operation state：晚到收口只访问它，不借用调用者栈。
struct MongoOp : std::enable_shared_from_this<MongoOp> {
    enum class Kind { InsertOne, FindOne, UpdateOne, DeleteOne };
    // kQueued 在 m_queue 等 worker；kRunning 已被 worker 取出、driver
    // 调用进行中；kDone 后端不再访问（未派发摘出 / driver 已返回）。
    enum class Phase : int { kQueued = 0, kRunning = 1, kDone = 2 };

    std::shared_ptr<CoMongoCliImpl>           owner;
    Kind                                      kind;
    MongoDocument                             doc;     // insert 文档 / 谓词
    MongoDocument                             update;  // 仅 UpdateOne
    std::shared_ptr<bbt::coroutine::CompletionSignal> sig;
    std::optional<result<MongoOpOutcome>>     outcome;
    // 首次发布即逻辑终态：Finish 经 CAS 保证只落定一次（worker 完成
    // 回投为主，调用方超时/取消与 close 收口路径可能在其它线程触发）。
    std::atomic_bool                          finished{false};
    std::atomic<Phase>                        phase{Phase::kQueued};

    MongoOp(std::shared_ptr<CoMongoCliImpl> owner_, Kind kind_,
            MongoDocument doc_, MongoDocument update_)
        : owner(std::move(owner_)), kind(kind_),
          doc(std::move(doc_)), update(std::move(update_)) {}

    // 可在任意线程调用：首次落定者独占 outcome 写入与 Complete；
    // 完成/取消/deadline/owner close 竞争时先到者的逻辑终态不被覆盖。
    // 早退分支也做 MaybeUnregister：phase=kDone 可能由另一收口路径
    // 在 finished 置位之后才落定，两条路径都要汇到反登记检查。
    void Finish(result<MongoOpOutcome> r) noexcept {
        if (finished.exchange(true)) {
            MaybeUnregister();
            return;
        }
        outcome = std::move(r);
        MaybeUnregister();
        sig->Complete();
    }
    // 定义在 CoMongoCliImpl 完整类型之后（owner->UnregisterOp 需要）。
    void MaybeUnregister();
};

class CoMongoCliImpl : public CoMongoCli,
                       public std::enable_shared_from_this<CoMongoCliImpl> {
public:
    CoMongoCliImpl(MongoClientConfig config,
                   bbt::coroutine::CoObjectInfo info,
                   std::shared_ptr<bbt::coroutine::CompletionSignal> close_sig)
        : m_config(std::move(config)),
          m_info(std::move(info)),
          m_close(std::move(close_sig)) {}
    // worker 只经 op.owner 保活 impl；析构前完成收口并 join 全部
    // worker——driver 调用有超时上界，join 必然收敛。
    ~CoMongoCliImpl() override;

    result<void> Start() override;

    result<void> InsertOne(const MongoDocument& doc,
                           const CallOptions&   options) override;
    result<std::optional<MongoDocument>> FindOne(
        const MongoDocument& filter, const CallOptions& options) override;
    result<MongoUpdateResult> UpdateOne(const MongoDocument& filter,
                                        const MongoDocument& update,
                                        const CallOptions&   options) override;
    result<std::uint64_t> DeleteOne(const MongoDocument& filter,
                                    const CallOptions&   options) override;

    void RequestClose() noexcept override;
    bool IsClosed() const noexcept override { return m_close.IsClosed(); }
    CloseStatus WaitClosed(
        bbt::coroutine::Deadline          deadline,
        bbt::coroutine::CancellationToken cancel) override {
        return m_close.WaitClosed(deadline, std::move(cancel),
                                  m_info.generation);
    }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return m_info;
    }

    // ---- op 生命周期（m_ops_mtx 保护，跨线程可触达）----
    // 返回 false 表示 teardown 已快照，调用方按 Closed 拒绝——保证
    // 「已登记必被终态收口」，等待者不会因 close 竞态挂住。
    bool RegisterOp(const std::shared_ptr<MongoOp>& op) {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        if (m_io_dead)
            return false;
        m_ops.insert(op);
        return true;
    }
    void UnregisterOp(const std::shared_ptr<MongoOp>& op) {
        bool fin = false;
        {
            std::lock_guard<std::mutex> lk(m_ops_mtx);
            m_ops.erase(op);
            fin = m_io_dead && m_ops.empty() && m_live_workers.load() == 0;
        }
        if (fin)
            m_close.MarkClosed();
    }

    // 验收观测钩子：运行中 driver 调用计数与峰值（worker 线程内维护）。
    std::size_t RunningDriverCallsForTest() const noexcept {
        return m_running_calls.load();
    }
    std::size_t PeakDriverCallsForTest() const noexcept {
        return m_peak_calls.load();
    }

private:
    // 命令提交（调用协程）：校验→登记→入队→Wait；返回结果载荷或
    // 提前失败错误（未接触 worker/driver 也可产生的失败）。
    result<MongoOpOutcome> Submit(MongoOp::Kind kind, MongoDocument doc,
                                  MongoDocument      update,
                                  const CallOptions& options);
    // 非协程上下文/已关闭/未启动的公共前置校验。
    result<void> PreCheck() const;

    // ---- worker 线程域 ----
    void                  WorkerLoop() noexcept;
    // 在同一 worker 上 acquire/use/release client 并执行阻塞调用；
    // 全部 driver/bson 异常分类为契约 Error，不向 worker 抛出。
    result<MongoOpOutcome> RunDriverCall(const MongoOp& op) noexcept;
    // 完成回投共享 executor 后落定；执行域不可达时 worker 直接落定。
    void Publish(const std::shared_ptr<MongoOp>& op,
                 result<MongoOpOutcome>          r);

    // ---- 关闭链路（RequestClose 调用线程直接执行）----
    void Teardown() noexcept;
    // 落定条件检查：m_io_dead && m_ops 空 && worker 全退 ⇒ MarkClosed。
    void DrainCheckClosed() noexcept;

    MongoClientConfig            m_config;
    bbt::coroutine::CoObjectInfo m_info;
    ManagedCloseState            m_close;
    MongoIoEngine                m_engine;   // 完成回投域
    std::atomic<int>             m_state{kCreated};
    std::shared_ptr<MongoPoolLease> m_pool;  // Start 建立

    std::mutex                                  m_ops_mtx;
    std::unordered_set<std::shared_ptr<MongoOp>> m_ops;
    bool m_io_dead{false};   // m_ops_mtx 保护：teardown 后置位

    std::mutex                            m_queue_mtx;
    std::condition_variable               m_queue_cv;
    std::deque<std::shared_ptr<MongoOp>>  m_queue;          // m_queue_mtx 保护
    bool                                  m_queue_stopping{false};

    std::vector<std::thread>   m_workers;
    // worker 在 loop 顶自增、退出前自减；DrainCheckClosed 据此判定
    // 「物理调用与队列清零」。
    std::atomic<int>           m_live_workers{0};
    std::atomic<std::size_t>   m_running_calls{0};
    std::atomic<std::size_t>   m_peak_calls{0};

    enum State : int { kCreated = 0, kRunning = 1, kClosingOrClosed = 2 };
};

inline void MongoOp::MaybeUnregister() {
    if (finished && phase.load() == Phase::kDone)
        owner->UnregisterOp(shared_from_this());
}

} // namespace bbt::infra::mongo_detail
