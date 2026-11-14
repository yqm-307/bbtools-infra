/**
 * MongoRuntime：mongo 资源 owner（契约见 docs/decisions/0007-mongo-owner-handles.md；
 * 底层为 mongocxx 同步 driver 加有界 worker bridge）。
 *
 * 资源归属：每个 owner 拥有一组 worker，不再是每个 client 各自一组：
 * - 连接配置、pool lease、worker 组、接纳队列、ops 注册表与关闭排空
 * 集中在 owner；集合句柄（MongoCollImpl）只携带 db/collection
 * 目标值与 owner 共享指针，不新建线程。
 * - 同一 owner 的 N 个集合句柄共享同一 worker 组与同一 max_queue
 * 接纳队列：worker 数不随句柄数增长，队列容量是跨集合共享背压；
 * 不同 owner 拥有各自 worker 组/队列/ops 表，可验证隔离。
 * - 每项 operation 仍在同一 worker 上 acquire/use/release pooled
 * client，driver 同步调用不可强杀：close/deadline 只发布一次逻辑
 * 终态，已进入 driver 的调用继续到 driver timeout，op、client lease
 * 与 payload 保活到物理收口。
 *
 * 请求完成（请求完成契约）：一次业务调用内部走 CoWaiter::WaitWithCallback
 * 范式——登记等待事件 → 在 on_registered 内登记 CloseWaiters 并做一次
 * 入队投递 → 挂起；响应/结果放 op state，每个 op 只向业务交付一次终态
 * （CAS 落定）。pending（未派发）op 遇 Close 直接从队列摘出、不发送；
 * 已派发 op 先交付逻辑终态，迟到 driver 结果只消费不交付。
 *
 * 物理清理（逻辑结果与物理清理分离）：
 * - op 在 finished 且后端不再访问（phase==kDone）才离开 m_ops；
 * - m_io_dead && m_ops 空 && worker 全退 ⇒ 只唤醒排空等待（DrainNotify）；
 * - 唯一配对 finalizer 在 Start 成功发布前前置创建：成功启动的 owner 从启动
 * 当刻起就有确定回收责任，关闭与最后引用都不再依赖临时创建线程。封口/引用
 * 归零只投递请求；finalizer 在 native 线程无超时等待上述条件成立 → join 全部
 * worker（消除「计数归零后仍触 this」的收尾窗口）→ 归还本 owner 的 pool lease
 * → MarkClosed。失败（Start 未成功、无 worker/在途 driver）由封口线程同步完成
 * 同一幂等收口：不 join、不受临时线程创建影响、不泄漏或假 Closed。
 * - delete 责任的交接与最后一次成员访问同在一个 `m_finalize_mtx` 临界区内：
 * 「谁 delete」由该锁下的 done/dispose_requested 唯一判定，交出责任的一方在
 * 解锁后不再触碰任何成员——否则对方可能已释放对象，通知/读取都会落空。
 */
#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/infra/CloseOptions.hpp>
#include <bbt/infra/CoMongoCli.hpp>
#include <bbt/infra/mongo/CoMongoDb.hpp>

#include "bbt/infra/debug/InfraDebug.hpp"
#include "bbt/infra/mongo/detail/MongoDetail.hpp"
#include "bbt/infra/mongo/detail/MongoProcess.hpp"
#include "bbt/infra/mongo/detail/MongoSupport.hpp"

namespace bbt::infra::mongo_detail {

class MongoRuntime;
class MongoCollImpl;

/**
 * 一次命令的堆上 operation state：晚到收口只访问它，不借用调用者栈。
 * runtime 保活 owner 资源（worker/队列/pool）；handle 保活发起句柄，
 * 句柄关闭后在途 op 仍可物理收口并向其 sig 落定。
 * sig 只承担「等待/唤醒」（CoWaiter，唯一等待位、跨线程 Notify 安全）；
 * 协议结果与终态一律落在本对象，等待者恢复后自己读 outcome。
 */
struct MongoOp : std::enable_shared_from_this<MongoOp> {
    enum class Kind {
        InsertOne,
        FindOne,
        UpdateOne,
        DeleteOne,
        InsertMany,
        Find,
        UpdateMany,
        DeleteMany,
        CountDocuments,
    };

    /**
     * kQueued 在 runtime 队列等 worker（含已登记未派发）；kRunning 已被
     * 取出、driver 调用进行中；kDone 后端不再访问（未派发摘出 / driver 已返回）。
     */
    enum class Phase : int { kQueued = 0, kRunning = 1, kDone = 2 };

    std::shared_ptr<MongoRuntime> runtime;
    std::shared_ptr<MongoCollImpl> handle;
    Kind kind;

    /**
     * insert 文档 / 谓词
     */
    MongoDocument doc;

    /**
     * UpdateOne/UpdateMany
     */
    MongoDocument update;

    /**
     * 仅 InsertMany
     */
    std::vector<MongoDocument> docs;

    /**
     * 仅 Find（必填）
     */
    std::size_t find_limit = 0;

    /**
     * 仅 Find
     */
    std::size_t find_skip = 0;

    /**
     * 仅 Find
     */
    MongoDocument find_sort;

    /**
     * 仅 Find
     */
    MongoDocument find_projection;
    bbt::coroutine::sync::CoWaiter::SPtr sig;
    std::optional<result<MongoOpOutcome>> outcome;

    /**
     * 首次发布即逻辑终态：Finish 经 CAS 保证只落定一次（worker 完成
     * 回投为主，调用方超时与 close 收口路径可能在其它线程触发）。
     */
    std::atomic_bool finished{false};
    std::atomic<Phase> phase{Phase::kQueued};

    MongoOp(
        std::shared_ptr<MongoRuntime> rt, std::shared_ptr<MongoCollImpl> h, Kind k, MongoDocument d, MongoDocument u);

    /**
     * 可在任意线程调用：首次落定者独占 outcome 写入并唤醒等待者；
     * 完成/deadline/owner close 竞争时先到者的逻辑终态不被覆盖。
     * 早退分支也做 MaybeUnregister：phase=kDone 可能由另一收口路径在
     * finished 置位之后才落定，两条路径都要汇到反登记检查。
     */
    void Finish(result<MongoOpOutcome> r) noexcept;

    /**
     * 定义在 MongoRuntime 完整类型之后（UnregisterOp 需要）。
     */
    void MaybeUnregister();
};

/**
 * MongoRuntime：一个 owner 的资源实体——连接配置、pool lease、固定
 * worker 组、有界接纳队列、ops 注册表与关闭态机。句柄经 shared_ptr
 * 持有；op 也保活 runtime，最后一个引用释放后才析构。
 */
class MongoRuntime : public std::enable_shared_from_this<MongoRuntime> {
public:
    /**
     * 唯一创建入口：finalizer 在 Start 成功前前置创建；删除器只投递收口请求，
     * 绝不在引用归零线程（可能是 Co worker）执行 join。
     */
    static std::shared_ptr<MongoRuntime> Create(mongo::MongoRuntimeConfig config, bbt::coroutine::CoObjectInfo info);

    /**
     * op 经 shared_ptr 保活 runtime；句柄不持 worker。~MongoRuntime 只会在
     * finalizer 已完成、或从未成功启动（无 worker/在途 driver）时执行。
     */
    ~MongoRuntime();

    result<void> Start();

    /**
     * 命令提交（调用协程）：校验→登记→（on_registered 内）入队→挂起；
     * 返回结果载荷或提前失败错误（未接触 worker/driver 也可产生的失败）。
     * handle 为目标集合来源；调用方须保证其处于接纳态（MongoCollImpl 已校验）。
     * docs/find_limit/find_skip/find_sort/find_projection 为可选扩展载荷：
     * docs 仅 InsertMany 携带，find_* 仅 Find 携带；其余 kind 保持默认。
     */
    result<MongoOpOutcome> Submit(
        std::shared_ptr<MongoCollImpl> handle,
        MongoOp::Kind kind,
        MongoDocument doc,
        MongoDocument update,
        const CallOptions& options,
        std::vector<MongoDocument> docs = {},
        std::size_t find_limit = 0,
        std::size_t find_skip = 0,
        MongoDocument find_sort = {},
        MongoDocument find_projection = {});

    /**
     * 统一逻辑关闭入口：校验 → 封口 + 首因 + 唤醒全部挂起业务等待者（未派发 op
     * 立即落定 Closed，已进入 driver 的调用先拿到逻辑终态，迟到结果只消费不交付）。
     * - `Immediate`：封口即返回（`wait_release=false` 时不等任何物理事实）；
     * - `Graceful`：等已提交 op 全部发布逻辑终态（派发后 driver 返回属物理责任）；
     * - `wait_release=true`：在模式工作完成后等同一 absolute deadline 内的真实物理回收
     * （worker 退出 + `JoinWorkers()` + lease 归还，由唯一 finalizer 在 native 线程完成）。
     * 受管协程按 50ms 分片挂起等待，native 宿主域有界等待；join 永不在调用线程执行。
     * Processer 线程上不在协程内返回 InvalidContext；非法选项在副作用前返回 InvalidArgument。
     */
    result<void> Close(const CloseOptions& options);

    /**
     * 非阻塞封口：任意线程、幂等、发布首因；不等待在途 op，不做物理收口。
     */
    void RequestClose(Error reason) noexcept;

    /**
     * 首个关闭原因；封口前为空，封口后固定。
     */
    std::optional<Error> CloseReason() const noexcept;

    /**
     * 同步关闭（ICoCloseable 面，等价「Immediate 封口 + 物理收口」）：取得关闭权
     * 者封口 → 唤醒全部挂起业务等待者 → 唯一配对 finalizer 等待真实物理落定
     * （在途 driver 返回 + op 清空 + worker 全退）→ join → 归还本 owner lease →
     * 发布终态；并发/重复 Close 阻塞等待同一 finalizer 事实后返回，不重复
     * join/reset、不提前 Closed。返回即 op/worker 已收口，且 runtime 不再接纳新
     * 命令；driver 自身超时是在途调用返回上界。
     * 执行域：native 宿主等 finalizer 完成；受管协程（及 Processer 上的非协程域）
     * 只封口并交后台 finalizer——join 不在 Co worker 上执行，需要物理凭据的协程
     * 调用方改用 Close(Immediate + wait_release=true)。
     */
    void Close() noexcept;

    /**
     * 物理收口凭据：op 清空 + worker 全退 + join 完成 + lease 归还才返回成功。
     * 前置是先封口（Close/RequestClose），否则 InvalidArgument；**native-only**：
     * 本调用只等 finalizer 的事实（有界 native 等待），不自己 join；协程域改用
     * `Close(wait_release=true)`，否则返回 InvalidContext。到期返回 TimedOut，
     * 保留 worker/op 责任，不用 drain 超时冒充物理释放。
     */
    result<void> WaitStopped(bbt::coroutine::Deadline deadline);

    bool IsClosed() const noexcept { return m_close.IsClosed(); }

    /**
     * owner 是否处于 Running（Start 成功且未进入关闭）。Collection()
     * 用它落实公共契约「owner 必须 Running 才能创建句柄」。
     */
    bool IsRunning() const noexcept { return m_state.load() == kRunning; }

    bbt::coroutine::CoObjectInfo GetObjectInfo() const;

    const mongo::MongoRuntimeConfig& Config() const { return m_config; }

    /**
     * op 生命周期（m_ops_mtx 保护，跨线程可触达）
     * 返回 false 表示 teardown 已快照，调用方按 Closed 拒绝——保证
     * 「已登记必被终态收口」，等待者不会因 close 竞态挂住。
     */
    bool RegisterOp(const std::shared_ptr<MongoOp>& op);
    void UnregisterOp(const std::shared_ptr<MongoOp>& op);

    /**
     * 真实生产状态观测：worker 数（m_live_workers 参与 drain 谓词）。
     * 保留、不门控——Release 下同样可用。同一 owner 多句柄共享同一组 worker。
     */
    std::size_t LiveWorkersForTest() const noexcept { return static_cast<std::size_t>(m_live_workers.load()); }
#ifdef BBT_INFRA_STRINGENT_DEBUG

    /**
     * debug 观测（仅观测数据，整体在宏内）：运行中 driver 调用计数与
     * 峰值由 debug/InfraDebug.hpp 的 MongoRuntimeObservation 维护，
     * 取证存储/锁也随其移出生产对象。Release 下这些访问器不存在。
     */
    std::size_t RunningDriverCallsForTest() const noexcept { return m_obs.running_calls.load(); }

    std::size_t PeakDriverCallsForTest() const noexcept { return m_obs.peak_calls.load(); }

    std::optional<bbt::infra::debug::MongoThreadEvidence> LastDriverThreadEvidenceForTest() const;
#endif

    /**
     * 队列深度观测（m_queue_mtx 保护）：跨集合共享背压用例用它确定
     * 「前一个 op 已占住队列容量」，再提交下一个断言 Overloaded。
     * 读的是真实生产队列 m_queue，Release 下同样可用。
     */
    std::size_t QueuedOpsForTest() const noexcept;

    /**
     * 真实状态观测（m_ops_mtx 保护）：计数已从接纳队列取出、被 worker 接管、
     * 正处 RunDriverCall 入口的 op（读既有 m_ops + MongoOp::phase，不新增运行时
     * 计数或采样）。Release 下同样可用。
     * 边界诚实说明：phase==kRunning 由 worker 在出队后、RunDriverCall 入口前落位，
     * 故它证明「op 已离开队列并被 worker 占用执行槽」，不等同于「已进入 native
     * driver 调用」——native 入口之后的证据仍由既有 debug 观测（RunningGuard）
     * 提供，本访问器不冒充后者。
     */
    std::size_t DriverCallsInFlightForTest() const noexcept;

    /**
     * 关闭链路观测（m_ops_mtx 保护）：启动失败或关闭收口后 io 域已置死，
     * 后续提交一律按 Closed 拒绝。
     */
    bool IoDeadForTest() const noexcept;

    /**
     * 测试注入点：让下一次 Start 的 worker 组在指定位置失败。生产路径不安装，
     * 默认 kSpawnFailNone 不改变行为。kSpawnFailReserve 在 reserve 前失败，
     * 等价 bad_alloc；其余值在第 N 个 worker 创建前失败，等价 std::thread 构造抛
     * system_error，此时已启动 N 个 worker。
     */
    static constexpr std::size_t kSpawnFailNone = static_cast<std::size_t>(-1);
    static constexpr std::size_t kSpawnFailReserve = static_cast<std::size_t>(-2);

    void SetWorkerSpawnFailForTest(std::size_t at) noexcept { m_spawn_fail_at.store(at, std::memory_order_relaxed); }

    /**
     * 测试注入点：让下一次 Start 的前置 finalizer 创建失败（等价线程资源耗尽）。
     * 失败发生在任何 worker 创建之前，生产路径默认 false。
     */
    void SetFinalizerSpawnFailForTest(bool fail) noexcept
    {
        m_finalizer_spawn_fail_for_test.store(fail, std::memory_order_relaxed);
    }

    /**
     * 测试接缝（detail-only）：finalizer 在「Finalize 完成、delete 责任尚未交接」
     * 处停靠，让最后引用归零与交接阶段确定重叠，而不是靠概率撞窗口。停靠门与回收
     * 观察都放在静态存储：引用归零之后已无法经 owner 成员放行，delete 之后连成员
     * 都不存在。生产路径默认不停靠、不观察。
     */
    struct FinalizerReclaimForTest {

        /**
         * 被观察 owner 完成 delete 的次数
         */
        int count{0};

        /**
         * 执行 delete 的线程
         */
        std::thread::id thread{};
    };

    static void SetFinalizerHandoffPauseForTest(bool pause) noexcept;

    /**
     * 等 finalizer 进入停靠点（交接前一刻）；超时返回 false。
     */
    static bool WaitFinalizerHandoffStageForTest(int budget_ms) noexcept;

    /**
     * 观察指定 owner 的真实回收（并重置计数）；传 nullptr 停止观察。
     */
    static void WatchFinalizerReclaimForTest(const MongoRuntime* owner) noexcept;
    static FinalizerReclaimForTest ObservedFinalizerReclaimForTest() noexcept;

private:
    MongoRuntime(mongo::MongoRuntimeConfig config, bbt::coroutine::CoObjectInfo info);

    /**
     * 非协程上下文/已关闭/未启动的公共前置校验。
     */
    result<void> PreCheck() const;

    /**
     * worker 线程域
     */
    void WorkerLoop() noexcept;

    /**
     * 在同一 worker 上 acquire/use/release client 并执行阻塞调用；
     * 全部 driver/bson 异常分类为契约 Error，不向 worker 抛出。
     */
    result<MongoOpOutcome> RunDriverCall(const MongoOp& op) noexcept;

    /**
     * 完成回投共享 executor 后落定；执行域不可达时 worker 直接落定。
     */
    void Publish(const std::shared_ptr<MongoOp>& op, result<MongoOpOutcome> r);

    /**
     * 关闭链路（Close/finalizer 线程直接执行）
     */
    void Teardown() noexcept;

    /**
     * 一次性 join 既有 worker 组（finalizer 内单次调用）。
     */
    void JoinWorkers() noexcept;

    /**
     * 排空条件（m_io_dead && m_ops 空 && worker 全退）满足时只唤醒 Close 的
     * 排空等待；终态发布归唯一 finalizer，不在此处提前 MarkClosed。
     */
    void DrainNotify() noexcept;

    /**
     * 已登记 op 是否全部发布逻辑终态（派发中 op 的 driver 返回属物理责任）。
     */
    bool AllOpsPublished() const noexcept;

    /**
     * Graceful 等待：已提交 op 发布结果后唤醒（Teardown 末尾与 DrainNotify 两处）。
     */
    void WakePublishWaiters() noexcept;

    /**
     * 唯一配对 finalizer：等真实物理落定（在途 driver 返回 + op 清空 +
     * worker 全退）→ join → 归还本 owner lease → 发布终态 MarkClosed。
     * Finalize 单次执行；并发/重复 Close 与析构都阻塞等待同一事实。
     */
    void Finalize() noexcept;
    void FinalizeOnce() noexcept;

    /**
     * 前置准备唯一配对 finalizer 的后台执行者（只在 Start 成功发布前调用）：
     * 线程等待封口/最后引用请求后执行一次 Finalize。成功启动的 owner 因此从启动
     * 当刻起就有确定回收责任，关闭与引用归零都不再依赖临时创建线程。
     */
    bool StartFinalizerThread() noexcept;

    /**
     * 投递封口请求；没有 finalizer 上下文（从未成功启动）返回 false。
     */
    bool SignalFinalizer() noexcept;

    /**
     * finalizer 线程主体：等到封口或最后引用归零 → Finalize → 交还 delete 责任。
     */
    void FinalizerLoop() noexcept;

    /**
     * 等真实物理回收完成（m_close 已 MarkClosed，即 join + lease 归还之后）。
     * 只等前置 finalizer 的事实；没有 finalizer 上下文时封口已同步完成，
     * 不在调用线程执行 join，等待严格受 deadline 约束。
     */
    result<void> WaitPhysicalClose(bbt::coroutine::Deadline deadline);

    /**
     * 自定义删除器：最后一个引用归零时只投递「收口 + delete」请求给前置
     * finalizer；只有从未成功启动（无 worker/在途 driver）或 finalizer 已完成
     * 时才直接释放，任何路径都不在引用归零线程 join。
     */
    static void Dispose(MongoRuntime* self) noexcept;

private:
    mongo::MongoRuntimeConfig m_config;
    bbt::coroutine::CoObjectInfo m_info;
    ManagedCloseState m_close;

    /**
     * 完成回投域
     */
    MongoIoEngine m_engine;
    std::atomic<int> m_state{kCreated};

    /**
     * 生命周期迁移（Start 发布 pool/取运行态、Close 取关闭权）与 finalizer
     * 归还 lease 互斥：同一 m_pool shared_ptr 对象不再被并发赋值/reset
     * （shared_ptr 引用计数的线程安全不覆盖同一对象的并发修改）。
     */
    std::mutex m_lifecycle_mtx;

    /**
     * Start 建立
     */
    std::shared_ptr<MongoPoolLease> m_pool;

    mutable std::mutex m_ops_mtx;
    std::unordered_set<std::shared_ptr<MongoOp>> m_ops;

    /**
     * m_ops_mtx 保护：teardown 后置位
     */
    bool m_io_dead{false};

    /**
     * 测试注入点的当前状态；生产默认 kSpawnFailNone。
     */
    std::atomic<std::size_t> m_spawn_fail_at{kSpawnFailNone};

    /**
     * Close 有界排空等待（m_ops_mtx + 谓词「op 清空且 worker 全退」）。
     */
    std::condition_variable m_drain_cv;

    /**
     * 挂起业务等待者登记：Close 在此唤醒全部在册等待者（跨线程安全）。
     */
    CloseWaiters m_waiters;

    /**
     * 关闭进度等待（已提交 op 发布逻辑终态 / 物理回收完成）：Co 域分片等待位 +
     * 宿主线程条件变量。谓词读 m_ops（m_ops_mtx）与 m_close（原子），登记用
     * m_publish_mtx；事件方（Publish、Teardown、finalizer）先落定状态再唤醒，
     * 故「登记后复查谓词」即可闭合丢唤醒窗口（锁序：publish → ops，反向不存在）。
     */
    mutable std::mutex m_publish_mtx;
    std::condition_variable m_publish_cv;
    std::vector<bbt::coroutine::sync::CoWaiter::SPtr> m_publish_waiters;

    /**
     * m_publish_mtx 保护：首个关闭原因
     */
    std::optional<Error> m_reason;

    mutable std::mutex m_queue_mtx;
    std::condition_variable m_queue_cv;

    /**
     * m_queue_mtx 保护
     */
    std::deque<std::shared_ptr<MongoOp>> m_queue;
    bool m_queue_stopping{false};

    std::vector<std::thread> m_workers;

    /**
     * worker 在 loop 顶自增、退出前自减；DrainNotify 据此唤醒 finalizer
     * 的排空等待（不在此发布终态）。
     */
    std::atomic<int> m_live_workers{0};

    /**
     * worker 起跑闩与 join 串行：m_started_workers（m_workers_mtx 保护）只增
     * 不减，Start 等其等于配置数（Close 抢跑时以 state 逃逸，不依赖
     * live_workers==配置数、不会自锁）；m_join_mtx 使并发 Close 的 join
     * 串行，后来者观察到同一 join 事实。
     */
    std::mutex m_workers_mtx;
    std::condition_variable m_workers_cv;
    int m_started_workers{0};
    std::mutex m_join_mtx;

    /**
     * 唯一配对 finalizer 的 once 闩：并发/重复 Close 与析构等待同一事实。
     */
    std::once_flag m_finalize_once;

    /**
     * 前置 finalizer 的握手（m_finalize_mtx 保护请求/完成标志）：
     * - Start 成功前创建线程并置 started；
     * - RequestClose 置 requested；
     * - 最后引用归零置 dispose_requested 并把 delete 责任交给 finalizer；
     * - finalizer 完成 Finalize 后置 done，若已请求 dispose 则自行 delete。
     */
    std::mutex m_finalize_mtx;
    std::condition_variable m_finalize_cv;
    std::atomic_bool m_finalizer_started{false};

    /**
     * m_finalize_mtx 保护
     */
    bool m_finalize_requested{false};

    /**
     * m_finalize_mtx 保护
     */
    bool m_dispose_requested{false};

    /**
     * m_finalize_mtx 保护
     */
    bool m_finalize_done{false};
    std::atomic_bool m_finalizer_spawn_fail_for_test{false};
    std::atomic_bool m_finalized{false};
#ifdef BBT_INFRA_STRINGENT_DEBUG

    /**
     * 仅观测：运行中 driver 调用计数/峰值 + 执行域取证存储与锁，整体收进
     * debug 层组件（Release 下生产对象无这些原子/锁/存储，也无相关符号）。
     */
    bbt::infra::debug::MongoRuntimeObservation m_obs;
#endif

    enum State : int { kCreated = 0, kRunning = 1, kClosingOrClosed = 2 };
};

// clang-format off
}
// clang-format on
