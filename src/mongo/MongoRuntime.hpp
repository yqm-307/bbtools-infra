#pragma once
// MongoRuntime：Issue #40 引入的 mongo 资源 owner（仍走 Issue #7 裁决的
// mongocxx 同步 driver + 有界 worker bridge）。
//
// 归属变化（与 #7 每 client 一组 worker 的差异）：
//   - 连接配置、pool lease、worker 组、接纳队列、ops 注册表与关闭排空
//     集中在 owner；集合句柄（MongoCollImpl）只携带 db/collection
//     目标值与 owner 共享指针，不新建线程。
//   - 同一 owner 的 N 个集合句柄共享同一 worker 组与同一 max_queue
//     接纳队列：worker 数不随句柄数增长，队列容量是跨集合共享背压；
//     不同 owner 拥有各自 worker 组/队列/ops 表，可验证隔离。
//   - 每项 operation 仍在同一 worker 上 acquire/use/release pooled
//     client，driver 同步调用不可强杀：close/deadline 只发布一次逻辑
//     终态，已进入 driver 的调用继续到 driver timeout，op、client lease
//     与 payload 保活到物理收口。
//
// 请求完成（契约 §2）：一次业务调用内部走 CoWaiter::WaitWithCallback
// 范式——登记等待事件 → 在 on_registered 内登记 CloseWaiters 并做一次
// 入队投递 → 挂起；响应/结果放 op state，每个 op 只向业务交付一次终态
// （CAS 落定）。pending（未派发）op 遇 Close 直接从队列摘出、不发送；
// 已派发 op 先交付逻辑终态，迟到 driver 结果只消费不交付。
//
// 物理清理（逻辑结果与物理清理分离）：
//   - op 在 finished 且后端不再访问（phase==kDone）才离开 m_ops；
//   - m_io_dead && m_ops 空 && worker 全退 ⇒ 只唤醒排空等待（DrainNotify）；
//   - Close() 同步：取得关闭权的调用者封口 → 唤醒挂起等待者 → 唯一配对
//     finalizer 无超时等待上述条件成立 → join 全部 worker（消除「计数归零后
//     仍触 this」的收尾窗口）→ 归还本 owner 的 pool lease → MarkClosed。
//     并发/重复 Close/析构阻塞等待同一 finalizer 事实；driver 超时保上界，
//     join 必然收敛。

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
#include <bbt/infra/mongo/Client.hpp>

#include "mongo/MongoDetail.hpp"
#include "mongo/MongoProcess.hpp"
#include "mongo/MongoSupport.hpp"

namespace bbt::infra::mongo_detail {

class MongoRuntime;
class MongoCollImpl;

// 一次命令的堆上 operation state：晚到收口只访问它，不借用调用者栈。
// runtime 保活 owner 资源（worker/队列/pool）；handle 保活发起句柄，
// 句柄关闭后在途 op 仍可物理收口并向其 sig 落定。
// sig 只承担「等待/唤醒」（CoWaiter，唯一等待位、跨线程 Notify 安全）；
// 协议结果与终态一律落在本对象，等待者恢复后自己读 outcome。
struct MongoOp : std::enable_shared_from_this<MongoOp> {
    enum class Kind { InsertOne, FindOne, UpdateOne, DeleteOne };
    // kQueued 在 runtime 队列等 worker（含已登记未派发）；kRunning 已被
    // 取出、driver 调用进行中；kDone 后端不再访问（未派发摘出 / driver 已返回）。
    enum class Phase : int { kQueued = 0, kRunning = 1, kDone = 2 };

    std::shared_ptr<MongoRuntime>        runtime;
    std::shared_ptr<MongoCollImpl>       handle;
    Kind                                 kind;
    MongoDocument                        doc;     // insert 文档 / 谓词
    MongoDocument                        update;  // 仅 UpdateOne
    bbt::coroutine::sync::CoWaiter::SPtr sig;
    std::optional<result<MongoOpOutcome>> outcome;
    // 首次发布即逻辑终态：Finish 经 CAS 保证只落定一次（worker 完成
    // 回投为主，调用方超时与 close 收口路径可能在其它线程触发）。
    std::atomic_bool                     finished{false};
    std::atomic<Phase>                   phase{Phase::kQueued};

    MongoOp(std::shared_ptr<MongoRuntime>  rt,
            std::shared_ptr<MongoCollImpl> h, Kind k, MongoDocument d,
            MongoDocument u)
        : runtime(std::move(rt)), handle(std::move(h)), kind(k),
          doc(std::move(d)), update(std::move(u)) {}

    // 可在任意线程调用：首次落定者独占 outcome 写入并唤醒等待者；
    // 完成/deadline/owner close 竞争时先到者的逻辑终态不被覆盖。
    // 早退分支也做 MaybeUnregister：phase=kDone 可能由另一收口路径在
    // finished 置位之后才落定，两条路径都要汇到反登记检查。
    void Finish(result<MongoOpOutcome> r) noexcept {
        if (finished.exchange(true)) {
            MaybeUnregister();
            return;
        }
        outcome = std::move(r);
        MaybeUnregister();
        // 结果先落地、再唤醒：等待者恢复后直接读 outcome，不依赖载荷。
        // Notify 跨线程安全；等待者尚未 park 时走 CoPollEvent PENDING 兑现。
        sig->Notify();
    }
    // 定义在 MongoRuntime 完整类型之后（UnregisterOp 需要）。
    void MaybeUnregister();
};

// MongoRuntime：一个 owner 的资源实体——连接配置、pool lease、固定
// worker 组、有界接纳队列、ops 注册表与关闭态机。句柄经 shared_ptr
// 持有；op 也保活 runtime，最后一个引用释放后才析构。
class MongoRuntime : public std::enable_shared_from_this<MongoRuntime> {
public:
    MongoRuntime(mongo::MongoRuntimeConfig    config,
                 bbt::coroutine::CoObjectInfo info)
        : m_config(std::move(config)), m_info(std::move(info)) {}
    // op 经 shared_ptr 保活 runtime；句柄不持 worker。析构前完成收口
    // 并 join 全部 worker——driver 调用有超时上界，join 必然收敛。
    ~MongoRuntime();

    result<void> Start();

    // 命令提交（调用协程）：校验→登记→（on_registered 内）入队→挂起；
    // 返回结果载荷或提前失败错误（未接触 worker/driver 也可产生的失败）。
    // handle 为目标集合来源；调用方须保证其处于接纳态（MongoCollImpl 已校验）。
    result<MongoOpOutcome> Submit(std::shared_ptr<MongoCollImpl> handle,
                                  MongoOp::Kind kind, MongoDocument doc,
                                  MongoDocument      update,
                                  const CallOptions& options);

    // 同步关闭（幂等、任意线程可调用）：取得关闭权者封口 → 唤醒全部挂起
    // 业务等待者 → 唯一配对 finalizer 等待真实物理落定（在途 driver 返回
    // + op 清空 + worker 全退）→ join → 归还本 owner lease → 发布终态；
    // 并发/重复 Close 阻塞等待同一 finalizer 事实后返回，不重复 join/reset、
    // 不提前 Closed。返回即 op/worker 已收口，且 runtime 不再接纳新命令；
    // driver 自身超时是在途调用返回上界。
    void Close() noexcept;
    bool IsClosed() const noexcept { return m_close.IsClosed(); }
    // owner 是否处于 Running（Start 成功且未进入关闭）。Collection()
    // 用它落实公共契约「owner 必须 Running 才能创建句柄」。
    bool IsRunning() const noexcept { return m_state.load() == kRunning; }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const { return m_info; }
    const mongo::MongoRuntimeConfig& Config() const { return m_config; }

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
        bool drained = false;
        {
            std::lock_guard<std::mutex> lk(m_ops_mtx);
            m_ops.erase(op);
            drained = m_io_dead && m_ops.empty() && m_live_workers.load() == 0;
        }
        // 只唤醒排空等待者；终态发布归唯一 finalizer（join 之后），
        // 不在此处提前 MarkClosed。
        if (drained)
            m_drain_cv.notify_all();
    }

    // 验收观测钩子：worker 数、运行中 driver 调用计数与峰值
    // （worker 线程内维护）。同一 owner 多句柄共享同一组计数。
    std::size_t LiveWorkersForTest() const noexcept {
        return static_cast<std::size_t>(m_live_workers.load());
    }
    std::size_t RunningDriverCallsForTest() const noexcept {
        return m_running_calls.load();
    }
    std::size_t PeakDriverCallsForTest() const noexcept {
        return m_peak_calls.load();
    }
    // 队列深度观测（m_queue_mtx 保护）：跨集合共享背压用例用它确定
    // 「前一个 op 已占住队列容量」，再提交下一个断言 Overloaded。
    std::size_t QueuedOpsForTest() const noexcept {
        std::lock_guard<std::mutex> lk(m_queue_mtx);
        return m_queue.size();
    }

private:
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

    // ---- 关闭链路（Close/finalizer 线程直接执行）----
    void Teardown() noexcept;
    // 一次性 join 既有 worker 组（finalizer 内单次调用）。
    void JoinWorkers() noexcept;
    // 排空条件（m_io_dead && m_ops 空 && worker 全退）满足时只唤醒 Close 的
    // 排空等待；终态发布归唯一 finalizer，不在此处提前 MarkClosed。
    void DrainNotify() noexcept;
    // 唯一配对 finalizer：等真实物理落定（在途 driver 返回 + op 清空 +
    // worker 全退）→ join → 归还本 owner lease → 发布终态 MarkClosed。
    // Finalize 单次执行；并发/重复 Close 与析构都阻塞等待同一事实。
    void Finalize() noexcept;
    void FinalizeOnce() noexcept;

    mongo::MongoRuntimeConfig    m_config;
    bbt::coroutine::CoObjectInfo m_info;
    ManagedCloseState            m_close;
    MongoIoEngine                m_engine;   // 完成回投域
    std::atomic<int>             m_state{kCreated};
    // 生命周期迁移（Start 发布 pool/取运行态、Close 取关闭权）与 finalizer
    // 归还 lease 互斥：同一 m_pool shared_ptr 对象不再被并发赋值/reset
    // （shared_ptr 引用计数的线程安全不覆盖同一对象的并发修改）。
    std::mutex                   m_lifecycle_mtx;
    std::shared_ptr<MongoPoolLease> m_pool;  // Start 建立

    std::mutex                                   m_ops_mtx;
    std::unordered_set<std::shared_ptr<MongoOp>> m_ops;
    bool m_io_dead{false};   // m_ops_mtx 保护：teardown 后置位
    // Close 有界排空等待（m_ops_mtx + 谓词「op 清空且 worker 全退」）。
    std::condition_variable                      m_drain_cv;

    // 挂起业务等待者登记：Close 在此唤醒全部在册等待者（跨线程安全）。
    CloseWaiters                                 m_waiters;

    mutable std::mutex                  m_queue_mtx;
    std::condition_variable               m_queue_cv;
    std::deque<std::shared_ptr<MongoOp>>  m_queue;          // m_queue_mtx 保护
    bool                                  m_queue_stopping{false};

    std::vector<std::thread>   m_workers;
    // worker 在 loop 顶自增、退出前自减；DrainNotify 据此唤醒 finalizer
    // 的排空等待（不在此发布终态）。
    std::atomic<int>           m_live_workers{0};
    // worker 起跑闩与 join 串行：m_started_workers（m_workers_mtx 保护）只增
    // 不减，Start 等其等于配置数（Close 抢跑时以 state 逃逸，不依赖
    // live_workers==配置数、不会自锁）；m_join_mtx 使并发 Close 的 join
    // 串行，后来者观察到同一 join 事实。
    std::mutex                 m_workers_mtx;
    std::condition_variable    m_workers_cv;
    int                        m_started_workers{0};
    std::mutex                 m_join_mtx;
    // 唯一配对 finalizer 的 once 闩：并发/重复 Close 与析构等待同一事实。
    std::once_flag             m_finalize_once;
    std::atomic<std::size_t>   m_running_calls{0};
    std::atomic<std::size_t>   m_peak_calls{0};

    enum State : int { kCreated = 0, kRunning = 1, kClosingOrClosed = 2 };
};

// MongoCollImpl：集合句柄——携带 db/collection 目标值与 owner 共享
// 指针，不持有 worker/队列/pool。句柄关闭只影响自身接纳与新命令
// 前置校验；owner 的物理收口与在途 op 保活由 runtime 负责。
class MongoCollImpl : public mongo::CoMongoColl,
                      public std::enable_shared_from_this<MongoCollImpl> {
public:
    MongoCollImpl(std::shared_ptr<MongoRuntime> runtime,
                  mongo::MongoTarget            target,
                  bbt::coroutine::CoObjectInfo  info)
        : m_runtime(std::move(runtime)),
          m_target(std::move(target)),
          m_info(std::move(info)) {}
    // 不拥有 worker/ops；析构前只做句柄级接纳封口（幂等）。
    ~MongoCollImpl() override { Close(); }

    result<void> InsertOne(const MongoDocument& doc,
                           const CallOptions&   options) override;
    result<std::optional<MongoDocument>> FindOne(
        const MongoDocument& filter, const CallOptions& options) override;
    result<MongoUpdateResult> UpdateOne(const MongoDocument& filter,
                                        const MongoDocument& update,
                                        const CallOptions&   options) override;
    result<std::uint64_t> DeleteOne(const MongoDocument& filter,
                                    const CallOptions&   options) override;

    // 句柄关闭只做接纳门禁：封口后新命令前置校验返回 Closed；已提交的
    // 在途 op 由 op->handle 保活，物理收口归 runtime，句柄不等 owner drain。
    void Close() noexcept override {
        m_close.BeginClose();
        m_close.MarkClosed();
    }
    bool IsClosed() const noexcept override { return m_close.IsClosed(); }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return m_info;
    }

    bool                          Accepting() const noexcept {
        return m_close.IsOpen();
    }
    const mongo::MongoTarget&     Target() const { return m_target; }
    std::shared_ptr<MongoRuntime> Runtime() const { return m_runtime; }

private:
    std::shared_ptr<MongoRuntime> m_runtime;
    mongo::MongoTarget            m_target;
    bbt::coroutine::CoObjectInfo  m_info;
    ManagedCloseState             m_close;
};

inline void MongoOp::MaybeUnregister() {
    if (finished && phase.load() == Phase::kDone)
        runtime->UnregisterOp(shared_from_this());
}

// CoMongoCliImpl：旧契约 client——内部持有一个独占 runtime + 一个
// 集合句柄；保留既有公开 API 行为，不新增公共能力。worker/队列/pool
// 资源归属已上移到 MongoRuntime，本类只剩状态转发。
class CoMongoCliImpl : public CoMongoCli,
                       public std::enable_shared_from_this<CoMongoCliImpl> {
public:
    CoMongoCliImpl(MongoClientConfig             config,
                   bbt::coroutine::CoObjectInfo  info)
        : m_config(std::move(config)), m_info(std::move(info)) {}
    ~CoMongoCliImpl() override { Close(); }

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

    // client 关闭 = 句柄接纳封口 + 独占 runtime 同步收口：返回即 worker
    // 全退、ops 清空（未 Start 时无 runtime，本对象信号即刻落定）。
    // Close 与在途 Start 并发时先封口、再等该次 Start 完成（发布则同步
    // 收口，被抢先封口则 Start 自清不发布），不会出现「Close 已返回、
    // Start 事后发布资源」。
    void Close() noexcept override;
    // 语义：runtime 物理收口（ops 清空 + worker 全退）才视为 Closed；
    // 未 Start（m_runtime 为空）时看本对象 close 信号。
    bool IsClosed() const noexcept override {
        const auto rt = RuntimeSnapshot();
        return rt ? rt->IsClosed() : m_close.IsClosed();
    }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return m_info;
    }

    // 兼容观测钩子（委托 runtime；Start 未成功建立 runtime 时为 0）。
    std::size_t RunningDriverCallsForTest() const noexcept {
        const auto rt = RuntimeSnapshot();
        return rt ? rt->RunningDriverCallsForTest() : 0;
    }
    std::size_t PeakDriverCallsForTest() const noexcept {
        const auto rt = RuntimeSnapshot();
        return rt ? rt->PeakDriverCallsForTest() : 0;
    }
    std::size_t LiveWorkersForTest() const noexcept {
        const auto rt = RuntimeSnapshot();
        return rt ? rt->LiveWorkersForTest() : 0;
    }

private:
    // 稳定快照：m_coll/m_runtime 由 Start 在 m_lifecycle_mtx 内发布、由
    // Close 在同一锁内取走并收口。所有直接消费者先取快照再使用，绝不
    // 持锁跨协程 Submit／回调，避免与 Close 配对制造死锁。
    std::shared_ptr<MongoCollImpl> CollSnapshot() const noexcept {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        return m_coll;
    }
    std::shared_ptr<MongoRuntime> RuntimeSnapshot() const noexcept {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        return m_runtime;
    }
    // 「Start 在途」标志的收尾：必须在资源自清完成后才清标志并唤醒
    // Close，保证 Close 返回当刻资源已收口。
    void FinishStarting() noexcept {
        {
            std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
            m_starting = false;
        }
        m_lifecycle_cv.notify_all();
    }
    // Start 全部出口（成功/失败/被 Close 抢先封口）的作用域收尾。
    struct StartingScope {
        CoMongoCliImpl* cli;
        explicit StartingScope(CoMongoCliImpl* c) noexcept : cli(c) {}
        StartingScope(const StartingScope&)            = delete;
        StartingScope& operator=(const StartingScope&) = delete;
        ~StartingScope() noexcept { cli->FinishStarting(); }
    };

    MongoClientConfig                 m_config;
    bbt::coroutine::CoObjectInfo      m_info;
    ManagedCloseState                 m_close;
    mutable std::mutex                m_lifecycle_mtx;  // 发布/封口配对
    std::condition_variable           m_lifecycle_cv;   // Start 完成唤醒 Close
    bool                              m_starting = false;  // Start 在途
    std::shared_ptr<MongoRuntime>     m_runtime;  // Start 成功才建立
    std::shared_ptr<MongoCollImpl>    m_coll;     // 与 runtime 同建
};

} // namespace bbt::infra::mongo_detail
