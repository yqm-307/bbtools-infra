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
//     client，driver 同步调用不可强杀：deadline/cancel/close 先发布
//     一次逻辑终态，已进入 driver 的调用继续到 driver timeout，op、
//     client lease 与 payload 保活到物理收口。
//
// 物理清理（逻辑结果与物理清理分离）：
//   - op 在 finished 且后端不再访问（phase==kDone）才离开 m_ops；
//   - m_io_dead && m_ops 空 && worker 全退 ⇒ MarkClosed；
//   - 句柄析构只做句柄级关闭标记；owner 析构前完成收口并 join 全部
//     worker——driver 超时保上界，join 必然收敛。

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
struct MongoOp : std::enable_shared_from_this<MongoOp> {
    enum class Kind { InsertOne, FindOne, UpdateOne, DeleteOne };
    // kQueued 在 runtime 队列等 worker；kRunning 已被取出、driver 调用
    // 进行中；kDone 后端不再访问（未派发摘出 / driver 已返回）。
    enum class Phase : int { kQueued = 0, kRunning = 1, kDone = 2 };

    std::shared_ptr<MongoRuntime>   runtime;
    std::shared_ptr<MongoCollImpl>  handle;
    Kind                            kind;
    MongoDocument                   doc;     // insert 文档 / 谓词
    MongoDocument                   update;  // 仅 UpdateOne
    std::shared_ptr<bbt::coroutine::CompletionSignal> sig;
    std::optional<result<MongoOpOutcome>>     outcome;
    // 首次发布即逻辑终态：Finish 经 CAS 保证只落定一次（worker 完成
    // 回投为主，调用方超时/取消与 close 收口路径可能在其它线程触发）。
    std::atomic_bool                finished{false};
    std::atomic<Phase>              phase{Phase::kQueued};

    MongoOp(std::shared_ptr<MongoRuntime>  rt,
            std::shared_ptr<MongoCollImpl> h, Kind k, MongoDocument d,
            MongoDocument u)
        : runtime(std::move(rt)), handle(std::move(h)), kind(k),
          doc(std::move(d)), update(std::move(u)) {}

    // 可在任意线程调用：首次落定者独占 outcome 写入与 Complete；
    // 完成/取消/deadline/owner close 竞争时先到者的逻辑终态不被覆盖。
    // 早退分支也做 MaybeUnregister：phase=kDone 可能由另一收口路径在
    // finished 置位之后才落定，两条路径都要汇到反登记检查。
    void Finish(result<MongoOpOutcome> r) noexcept {
        if (finished.exchange(true)) {
            MaybeUnregister();
            return;
        }
        outcome = std::move(r);
        MaybeUnregister();
        sig->Complete();
    }
    // 定义在 MongoRuntime 完整类型之后（UnregisterOp 需要）。
    void MaybeUnregister();
};

// MongoRuntime：一个 owner 的资源实体——连接配置、pool lease、固定
// worker 组、有界接纳队列、ops 注册表与关闭态机。句柄经 shared_ptr
// 持有；op 也保活 runtime，最后一个引用释放后才析构。
class MongoRuntime : public std::enable_shared_from_this<MongoRuntime> {
public:
    MongoRuntime(mongo::MongoRuntimeConfig config,
                 bbt::coroutine::CoObjectInfo info,
                 std::shared_ptr<bbt::coroutine::CompletionSignal> close_sig)
        : m_config(std::move(config)),
          m_info(std::move(info)),
          m_close(std::move(close_sig)) {}
    // op 经 shared_ptr 保活 runtime；句柄不持 worker。析构前完成收口
    // 并 join 全部 worker——driver 调用有超时上界，join 必然收敛。
    ~MongoRuntime();

    result<void> Start();

    // 命令提交（调用协程）：校验→登记→入队→Wait；返回结果载荷或提前
    // 失败错误（未接触 worker/driver 也可产生的失败）。handle 为目标
    // 集合来源；调用方须保证其处于接纳态（MongoCollImpl 已校验）。
    result<MongoOpOutcome> Submit(std::shared_ptr<MongoCollImpl> handle,
                                  MongoOp::Kind kind, MongoDocument doc,
                                  MongoDocument      update,
                                  const CallOptions& options);

    void RequestClose() noexcept;
    bool IsClosed() const noexcept { return m_close.IsClosed(); }
    // owner 是否处于 Running（Start 成功且未进入关闭）。Collection()
    // 用它落实公共契约「owner 必须 Running 才能创建句柄」。
    bool IsRunning() const noexcept { return m_state.load() == kRunning; }
    CloseStatus WaitClosed(bbt::coroutine::Deadline          deadline,
                           bbt::coroutine::CancellationToken cancel) {
        return m_close.WaitClosed(deadline, std::move(cancel),
                                  m_info.generation);
    }
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
        bool fin = false;
        {
            std::lock_guard<std::mutex> lk(m_ops_mtx);
            m_ops.erase(op);
            fin = m_io_dead && m_ops.empty() && m_live_workers.load() == 0;
        }
        if (fin)
            m_close.MarkClosed();
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

    // ---- 关闭链路（RequestClose 调用线程直接执行）----
    void Teardown() noexcept;
    // 落定条件检查：m_io_dead && m_ops 空 && worker 全退 ⇒ MarkClosed。
    void DrainCheckClosed() noexcept;

    mongo::MongoRuntimeConfig    m_config;
    bbt::coroutine::CoObjectInfo m_info;
    ManagedCloseState            m_close;
    MongoIoEngine                m_engine;   // 完成回投域
    std::atomic<int>             m_state{kCreated};
    std::shared_ptr<MongoPoolLease> m_pool;  // Start 建立

    std::mutex                                   m_ops_mtx;
    std::unordered_set<std::shared_ptr<MongoOp>> m_ops;
    bool m_io_dead{false};   // m_ops_mtx 保护：teardown 后置位

    mutable std::mutex                  m_queue_mtx;
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

// MongoCollImpl：集合句柄——携带 db/collection 目标值与 owner 共享
// 指针，不持有 worker/队列/pool。句柄关闭只影响自身接纳与新命令
// 前置校验；owner 的物理收口与在途 op 保活由 runtime 负责。
class MongoCollImpl : public mongo::CoMongoColl,
                      public std::enable_shared_from_this<MongoCollImpl> {
public:
    MongoCollImpl(std::shared_ptr<MongoRuntime> runtime,
                  mongo::MongoTarget target,
                  bbt::coroutine::CoObjectInfo info,
                  std::shared_ptr<bbt::coroutine::CompletionSignal> close_sig)
        : m_runtime(std::move(runtime)),
          m_target(std::move(target)),
          m_info(std::move(info)),
          m_close(std::move(close_sig)) {}
    // 不拥有 worker/ops；析构前只做句柄级关闭标记（幂等）。
    ~MongoCollImpl() override { RequestClose(); }

    result<void> InsertOne(const MongoDocument& doc,
                           const CallOptions&   options) override;
    result<std::optional<MongoDocument>> FindOne(
        const MongoDocument& filter, const CallOptions& options) override;
    result<MongoUpdateResult> UpdateOne(const MongoDocument& filter,
                                        const MongoDocument& update,
                                        const CallOptions&   options) override;
    result<std::uint64_t> DeleteOne(const MongoDocument& filter,
                                    const CallOptions&   options) override;

    // 句柄关闭只做接纳门禁：BeginClose+MarkClosed 完成句柄 sig，
    // 等待者可见 Closed；在途 op 由 op->handle 保活，物理收口归
    // runtime，句柄不等待 owner drain。
    void RequestClose() noexcept override {
        m_close.BeginClose();
        m_close.MarkClosed();
    }
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
    CoMongoCliImpl(MongoClientConfig config,
                   bbt::coroutine::CoObjectInfo info,
                   std::shared_ptr<bbt::coroutine::CompletionSignal> close_sig)
        : m_config(std::move(config)),
          m_info(std::move(info)),
          m_close(std::move(close_sig)) {}
    ~CoMongoCliImpl() override { RequestClose(); }

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
    // 语义与旧实现一致：runtime drain 完成（ops 清空 + worker 全退）
    // 才视为 Closed；未 Start（m_runtime 为空）时本对象 close 信号
    // 由 RequestClose 立即 MarkClosed。
    bool IsClosed() const noexcept override {
        return m_runtime ? m_runtime->IsClosed() : m_close.IsClosed();
    }
    CloseStatus WaitClosed(
        bbt::coroutine::Deadline          deadline,
        bbt::coroutine::CancellationToken cancel) override {
        if (m_runtime)
            return m_runtime->WaitClosed(deadline, std::move(cancel));
        return m_close.WaitClosed(deadline, std::move(cancel),
                                  m_info.generation);
    }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return m_info;
    }

    // 兼容观测钩子（委托 runtime；Start 未成功建立 runtime 时为 0）。
    std::size_t RunningDriverCallsForTest() const noexcept {
        return m_runtime ? m_runtime->RunningDriverCallsForTest() : 0;
    }
    std::size_t PeakDriverCallsForTest() const noexcept {
        return m_runtime ? m_runtime->PeakDriverCallsForTest() : 0;
    }
    std::size_t LiveWorkersForTest() const noexcept {
        return m_runtime ? m_runtime->LiveWorkersForTest() : 0;
    }

private:
    MongoClientConfig                 m_config;
    bbt::coroutine::CoObjectInfo      m_info;
    ManagedCloseState                 m_close;
    std::shared_ptr<MongoRuntime>     m_runtime;  // Start 成功才建立
    std::shared_ptr<MongoCollImpl>    m_coll;     // 与 runtime 同建
};

} // namespace bbt::infra::mongo_detail
