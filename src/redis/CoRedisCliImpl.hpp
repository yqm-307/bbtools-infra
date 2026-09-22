#pragma once
// CoRedisCliImpl：协程原生 Redis 客户端实现（hiredis async）。
//
// 桥接方式：命令在协程内发起，堆上 RedisOp 自持有 argv/sig/结果；
// 发送、回包、连接管理全部运行在 engine 的 io 域（共享 executor 上的
// strand，由 Scheduler 现有事件循环线程推进）。hiredis 完成回调把
// RawReply 写入 op 后 Complete CompletionSignal；调用协程经
// CompletionSignal::Wait 挂起/恢复。回调不触碰业务协程栈，不新造
// 事件状态机，不依赖 Linux Hook，不新建 io_context/线程。
//
// 容量语义：
//   - m_pending 等待队列 ≤ max_queue；满则新命令立即 Overloaded；
//   - m_inflight（已发出未回包）≤ max_inflight，流控而非接纳拒绝；
//   - 命令不跨连接迁移：连接断开时 pending 队列与在途命令统一以
//     连接错误落定，新命令触发重连后在队列内等待。
//
// 物理清理（契约 §逻辑结果与物理清理分离）：
//   - op 在 finished 且后端不再访问（phase==kDone：未发送移出队列，
//     已发送等到回包/NULL 回调）才离开 m_ops；
//   - m_io_dead && m_ops 空 && 连接已回收 ⇒ MarkClosed。

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
    std::shared_ptr<bbt::coroutine::CompletionSignal> sig;
    std::optional<result<RawReply>>           outcome;
    // 首次发布即逻辑终态：Finish 经 CAS 保证只落定一次（io 域为主，
    // 调用方超时/取消与 teardown 的收口路径可能在其它线程触发）。
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

    // 可在任意线程调用：首次落定者独占 outcome 写入与 Complete；
    // 完成/取消/deadline/owner close 竞争时先到者的逻辑终态不被覆盖。
    // 早退分支也做 MaybeUnregister：phase=kDone 可能由另一收口路径
    // 在 finished 置位之后才落定，两条路径都要汇到反登记检查。
    void Finish(result<RawReply> r) noexcept {
        if (finished.exchange(true)) {
            MaybeUnregister();
            return;
        }
        outcome = std::move(r);
        MaybeUnregister();
        sig->Complete();
    }
    // 定义在 CoRedisCliImpl 完整类型之后（owner->UnregisterOp 需要）。
    void MaybeUnregister();
};

class CoRedisCliImpl : public CoRedisCli,
                       public std::enable_shared_from_this<CoRedisCliImpl> {
public:
    CoRedisCliImpl(RedisClientConfig config,
                   bbt::coroutine::CoObjectInfo info,
                   std::shared_ptr<bbt::coroutine::CompletionSignal> close_sig)
        : m_config(std::move(config)),
          m_info(std::move(info)),
          m_close(std::move(close_sig)) {}

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

    void RequestClose() noexcept override;
    bool IsClosed() const noexcept override { return m_close.IsClosed(); }
    CloseStatus WaitClosed(bbt::coroutine::Deadline          deadline,
                           bbt::coroutine::CancellationToken cancel) override {
        return m_close.WaitClosed(deadline, std::move(cancel),
                                  m_info.generation);
    }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return m_info;
    }

    // ---- io 域接口（RedisConnection 通知）----
    void OnConnReadyOnIoDomain();
    void OnConnDownOnIoDomain(Error conn_err);

    // ---- op 生命周期（m_ops_mtx 保护，跨线程可触达）----
    // 返回 false 表示 teardown 已快照，调用方按 Closed 拒绝——保证
    // 「已登记必被终态收口」，等待者不会因 post 丢失而挂住。
    bool RegisterOp(const std::shared_ptr<RedisOp>& op) {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        if (m_io_dead)
            return false;
        m_ops.insert(op);
        return true;
    }
    void UnregisterOp(const std::shared_ptr<RedisOp>& op) {
        bool fin = false;
        {
            std::lock_guard<std::mutex> lk(m_ops_mtx);
            m_ops.erase(op);
            fin = m_io_dead && m_ops.empty() && m_conn_done;
        }
        if (fin)
            m_close.MarkClosed();
    }

private:
    // 命令提交（调用协程）：校验→登记→投递→Wait；返回原始 RawReply 或
    // 提前失败错误（不进 io 域也可产生的失败）。
    result<RawReply> Submit(RedisOp::Kind kind,
                            std::vector<std::string> args,
                            const CallOptions& options);
    // 非协程上下文/已关闭/未启动的公共前置校验。
    result<void> PreCheck() const;

    // ---- io 域（engine strand）内部流程 ----
    void AdmitOnIoDomain(std::shared_ptr<RedisOp> op);
    void EnsureConnOnIoDomain();
    void PumpOnIoDomain();
    void AbortOnIoDomain(std::shared_ptr<RedisOp> op);
    void OnReplyOnIoDomain(std::shared_ptr<RedisOp> op,
                           const redisReply* reply);
    void TeardownOnIoDomain() noexcept;
    void TeardownOffDomain() noexcept;
    void DrainCheckClosed() noexcept;
    void DrainPendingOnIoDomain(const Error& err);

    static void OnReplyThunk(redisAsyncContext* ac, void* reply,
                             void* privdata);

    RedisClientConfig                m_config;
    bbt::coroutine::CoObjectInfo     m_info;
    detail::ManagedCloseState        m_close;
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
    if (finished && phase.load() == Phase::kDone)
        owner->UnregisterOp(shared_from_this());
}

} // namespace bbt::infra::redis_detail
