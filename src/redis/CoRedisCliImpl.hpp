#pragma once
// CoRedisCliImpl：协程原生 Redis 客户端实现（hiredis async）。
//
// 桥接方式：命令在协程内发起，堆上 RedisOp 自持有 argv/waiter/结果；发送、
// 回包、连接管理全部运行在 engine 的 io 域（共享 executor 上的 strand，由
// Scheduler 现有事件循环线程推进）。请求完成走契约 §2 口径：一次业务调用
// 先用 CoWaiter::WaitWithCallback 登记等待事件，在 on_registered 里把命令
// 投递到 io 域一次，随后挂起；hiredis 完成回调把 RawReply 写入 op 终态后
// Notify 唤醒，业务协程恢复后读 op 自己的结果。回调不触碰业务协程栈，不
// 依赖 Linux Hook，不新建 io_context/线程，不把粘滞完成语义加进 CoWaiter。
//
// 容量语义：
//   - m_pending 等待队列 ≤ max_queue；满则新命令立即 Overloaded；
//   - m_inflight（已发出未回包）≤ max_inflight，流控而非接纳拒绝；
//   - 命令不跨连接迁移：连接断开时 pending 队列与在途命令统一以
//     连接错误落定，新命令触发重连后在队列内等待。
//
// 物理清理与 Close（契约 §1，Close() 同步返回）：
//   - op 在终态已发布且后端不再访问（phase==kDone：未发送移出队列，已发送
//     等到回包/NULL 回调）才离开 m_ops；
//   - owner 域 = m_engine.IoGate()：全部 hiredis context / ev bridge / fd
//     触碰都持这把门，io 域上的每个执行入口都以持门形式运行——经
//     PostOnIoDomain 投递的 handler，以及 RedisConnection 的 fd 等待完成
//     handler（ev bridge 持同一把门）。因此 owner 线程可以在自己的调用线程
//     内同步完成 teardown：持门即与在途 io handler 配对。
//   - Close() 幂等、任意线程：封口拒绝新请求 → CloseWaiters.CloseAndWakeAll
//     唤醒全部挂起等待者 → 域门内同步 teardown（交付 Closed 终态、回收
//     hiredis context/fd、丢弃未发送队列）→ 直接返回；返回当刻后端不再访问
//     本对象拥有的操作资源。不投递、不等窗口：手动 Tick 模式下没有执行域
//     驱动者，投递的 teardown 不会被执行，等待窗口只会让 Close 在 fd 仍存活
//     时返回（父级真实 TCP 探针）。

#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include <bbt/infra/CoRedisCli.hpp>

#include "detail/IoSupport.hpp"
#include "redis/RedisConnection.hpp"
#include "redis/RedisDetail.hpp"

namespace bbt::infra::redis_detail {

class CoRedisCliImpl;

// 一次命令的堆上 operation state：晚到回调只访问它，不借用调用者栈。
struct RedisOp : std::enable_shared_from_this<RedisOp> {
    enum class Kind  { Ping, Get, Set, Exists, Delete };
    // kQueued 在 m_pending 中等待；kSent 已交 hiredis 等待回包；
    // kDone 后端不再访问（未发送移出队列 / 回包已消费）。
    enum class Phase : int { kQueued = 0, kSent = 1, kDone = 2 };

    std::shared_ptr<CoRedisCliImpl>           owner;
    Kind                                      kind;
    std::vector<std::string>                  argv_store;
    std::vector<const char*>                  argv;
    std::vector<size_t>                       argvlen;
    // 唯一等待位：命令投递前先登记，io 域回包/连接/Close 路径唤醒它。
    bbt::coroutine::sync::CoWaiter::SPtr      waiter;
    // 首次发布即逻辑终态：Finish 经 m_mtx + m_done 保证只落定一次（io 域
    // 为主，调用方超时/取消与 teardown 的收口路径可能在其它线程触发）。
    // finished 是 m_done 的无锁镜像，仅供 io 域快路径查询。
    std::atomic_bool                          finished{false};
    std::atomic<Phase>                        phase{Phase::kQueued};
    // 仅 io 域访问：phase==kQueued 时在 owner->m_pending 中的位置。
    std::list<std::shared_ptr<RedisOp>>::iterator queue_it;

    RedisOp(std::shared_ptr<CoRedisCliImpl> owner_, Kind kind_,
            std::vector<std::string> args)
        : owner(std::move(owner_)), kind(kind_),
          argv_store(std::move(args)) {
        argv.reserve(argv_store.size());
        argvlen.reserve(argv_store.size());
        for (const auto& a : argv_store) {
            argv.push_back(a.data());
            argvlen.push_back(a.size());
        }
    }

    // 可在任意线程调用：首次落定者独占 outcome 写入与唤醒；完成/超时/
    // Close 竞争时先到者的逻辑终态不被覆盖。终态在 m_mtx 内先于唤醒发布，
    // 读侧经 TakeOutcome 取用，不存在「读到未写完结果」的窗口。
    void Finish(result<RawReply> r) noexcept {
        bool first = false;
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (!m_done) {
                m_done  = true;
                outcome = std::move(r);
                finished.store(true, std::memory_order_release);
                first = true;
            }
        }
        MaybeUnregister();
        // 唤醒挂在本次等待上的业务协程；未登记/已恢复时返回 -1，无副作用。
        if (first && waiter)
            waiter->Notify();
    }

    // 取本次请求的终态；未发布返回 nullopt——Close 的唤醒（CloseWaiters）
    // 可能先于 teardown 的终态发布，此时业务按 Closed 交付，不再等待。
    std::optional<result<RawReply>> TakeOutcome() {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (!m_done)
            return std::nullopt;
        return std::optional<result<RawReply>>(std::move(*outcome));
    }

    // 定义在 CoRedisCliImpl 完整类型之后（owner->UnregisterOp 需要）。
    void MaybeUnregister();

private:
    std::mutex                        m_mtx;
    bool                              m_done{false};
    std::optional<result<RawReply>>   outcome;
};

class CoRedisCliImpl : public CoRedisCli,
                       public std::enable_shared_from_this<CoRedisCliImpl> {
public:
    CoRedisCliImpl(RedisClientConfig config, bbt::coroutine::CoObjectInfo info)
        : m_config(std::move(config)),
          m_info(std::move(info)) {}

    result<void> Start() override;

    result<void> Ping(const CallOptions& options) override;
    result<std::optional<std::string>> Get(std::string_view key,
                                         const CallOptions& options) override;
    result<void> Set(std::string_view key, std::string_view value,
                     const CallOptions& options) override;
    result<bool> Exists(std::string_view key,
                        const CallOptions& options) override;
    result<std::uint64_t> Delete(std::vector<std::string> keys,
                                 const CallOptions& options) override;

    // owner 主动同步关闭：幂等、任意线程；返回即连接/hiredis context/fd 已
    // 物理回收、未发送命令已丢弃、后端不再访问本对象拥有的操作资源。
    void Close() noexcept override;
    bool IsClosed() const noexcept override { return m_close.IsClosed(); }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return m_info;
    }

    // ---- io 域接口（RedisConnection 通知）----
    void OnConnReadyOnIoDomain();
    void OnConnDownOnIoDomain(Error conn_err);

    // ---- op 生命周期（m_ops_mtx 保护，跨线程可触达）----
    // 返回 false 表示 teardown 已快照，调用方按 Closed 拒绝——保证
    // 「已登记必被终态收口」，等待者不会因投递丢失而挂住。
    bool RegisterOp(const std::shared_ptr<RedisOp>& op) {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        if (m_io_dead)
            return false;
        m_ops.insert(op);
        return true;
    }
    void UnregisterOp(const std::shared_ptr<RedisOp>& op) {
        {
            std::lock_guard<std::mutex> lk(m_ops_mtx);
            m_ops.erase(op);
        }
    }
    // op 落定后从关闭期等待者登记中摘除，避免长寿命客户端无界增长。
    void ForgetCloseWaiter(const bbt::coroutine::sync::CoWaiter* w) noexcept {
        m_close_waiters.Remove(w);
    }

private:
    // 命令提交（调用协程）：校验→登记 op→WaitWithCallback（on_registered
    // 内投递一次）；Completed 后读 op 自己的终态。
    result<RawReply> Submit(RedisOp::Kind kind,
                            std::vector<std::string> args,
                            const CallOptions& options);
    // 非协程上下文/已关闭/未启动的公共前置校验。
    result<void> PreCheck() const;
    // WaitWithCallback 的 on_registered：等待事件与唯一等待位已就绪，
    // 在此登记关闭等待者并投递一次命令；期间任何 Notify 走 PENDING。
    void OnWaitRegisteredOnCoroutine(std::shared_ptr<RedisOp> op);

    // ---- io 域（engine strand）内部流程 ----
    void AdmitOnIoDomain(std::shared_ptr<RedisOp> op);
    void EnsureConnOnIoDomain();
    void PumpOnIoDomain();
    void AbortOnIoDomain(std::shared_ptr<RedisOp> op);
    void OnReplyOnIoDomain(std::shared_ptr<RedisOp> op,
                           const redisReply* reply);
    void TeardownOnIoDomain() noexcept;
    void DrainCheckClosed() noexcept;
    void DrainPendingOnIoDomain(const Error& err);

    // io 域唯一投递入口：投递的 handler 一律持 owner 域门运行，使「在途 io
    // handler」与「Close 在调用线程内同步执行的 teardown」配对。TryPost 返回
    // false 表示 io 域未启动或已封（调用方按「后端已回收」处理）。
    template <class F>
    bool PostOnIoDomain(F&& f) {
        auto self = std::static_pointer_cast<CoRedisCliImpl>(shared_from_this());
        return m_engine.TryPost([self, fn = std::forward<F>(f)]() mutable {
            std::lock_guard<std::recursive_mutex> gate(self->m_engine.IoGate());
            fn();
        });
    }

    static void OnReplyThunk(redisAsyncContext* ac, void* reply,
                             void* privdata);

    RedisClientConfig                m_config;
    bbt::coroutine::CoObjectInfo     m_info;
    detail::ManagedCloseState        m_close;
    detail::CloseWaiters             m_close_waiters;
    detail::IoEngine                 m_engine;
    std::atomic<int>                 m_state{kCreated};

    std::mutex                                    m_ops_mtx;
    std::unordered_set<std::shared_ptr<RedisOp>>  m_ops;
    bool                          m_io_dead{false};     // m_ops_mtx 保护
    bool                          m_conn_done{false};   // m_ops_mtx 保护

    // ---- 仅 io 域访问 ----
    std::shared_ptr<RedisConnection>              m_conn;
    // 已断开连接的退役暂存：通知发生在 hiredis 回调栈内，栈内销毁会让
    // bridge 先于 hiredis cleanup 析构（desc 误关 fd / cleanup 访问死
    // 对象）。下一次 EnsureConnOnIoDomain 清理——该路径只由 admission
    // 触发，必然脱离回调栈，届时 cleanup 已完成、desc 已 release。
    std::vector<std::shared_ptr<RedisConnection>> m_retired_conns;
    std::list<std::shared_ptr<RedisOp>>  m_pending;
    std::size_t                          m_inflight{0};

    enum State : int { kCreated = 0, kRunning = 1, kClosingOrClosed = 2 };
};

inline void RedisOp::MaybeUnregister() {
    if (finished && phase.load() == Phase::kDone) {
        owner->ForgetCloseWaiter(waiter.get());
        owner->UnregisterOp(shared_from_this());
    }
}

} // namespace bbt::infra::redis_detail
