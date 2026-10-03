#pragma once
// 候选 B 级 owner binding Redis client（隔离实现，公开 CoRedisCli 语义不变）。
//
// 与生产路径的关系：本类是**独立实现**（新文件、新符号、新内部装配入口），
// 生产 CoRedisCli::Create 仍返回 hiredis async + strand 实现（CoRedisCliImpl）；
// 候选只经内部工厂 redis_detail::CreateCotcpRedisCli 装配，供隔离验证使用，
// 不切换生产默认路径、不进公共安装头。
//
// 执行模型（契约 §8「一个 coroutine 独占完整请求—响应」）：
//   - 命令由调用协程登记后挂起在 CoWaiter 上；
//   - 独占 owner coroutine 按 FIFO 串行执行「DialTCP → 编码 → WriteSome →
//     ReadSome → 解码」，hiredis 调用全部同步返回，可挂起的等待只发生在 CoTCP 内；
//   - 串行语义：同一连接同一时刻只有一条命令在途（无 async 回调并发推进），
//     max_inflight 不参与流水线；并发命令按 max_queue 排队，满即 Overloaded；
//   - 单连接、不重连：连接出错即不可复用，后续命令直接以连接错误落定（不重发）。
//
// 关闭（契约 §1，Close() 同步返回）：
//   - Close() 幂等、任意线程：封口拒绝新请求 → 收口全部已登记 op（发布 Closed 终态
//     并唤醒其等待者）→ 封口 owner 级 dial 等待登记（打断在途 DialTCP）→ 在
//     调用线程内同步物理关闭当前 CoTCP（fd 关闭、reader 释放）；
//   - 已建立连接的物理资源在 Close() 返回前释放；在途 DialTCP 尚未发布 fd 时，
//     由 owner 恢复后释放其候选 fd，Close() 不等待该执行域；
//   - owner coroutine 不在 Close 内等待（避免调用线程等待自身执行域推进）：封口后
//     它在下一个恢复点观察到封口即退出批次，届时 MarkClosed 落定。IsClosed() 反映
//     完整批次收口，可能晚于 Close() 返回。
//
// 本头仅 src/ 内部使用（不安装、不进 INSTALL_INTERFACE）。

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include <bbt/infra/CoRedisCli.hpp>
#include <bbt/infra/CoTCP.hpp>

#include "detail/IoSupport.hpp"
#include "redis/RedisDetail.hpp"   // RawReply / DecodeReply

namespace bbt::infra::redis_detail {

class RedisCotcpConn;

// owner coroutine 与落定竞争的可观测计数（隔离验证探针；不参与语义）。
struct CotcpProbe {
    std::atomic<std::uint64_t> owner_runs{0};            // owner coroutine 批次数
    std::atomic<std::uint64_t> finish_conflicts{0};      // 晚到终态被拒次数
    std::atomic<std::uint64_t> skipped_after_finish{0};  // 出队时已落定（未接触后端）
    std::atomic<std::uint64_t> dial_attempts{0};
    std::atomic<std::uint64_t> dial_inflight{0};         // 正在 DialTCP 内（挂起/未返回）的批次数
    std::atomic<std::uint64_t> conns_broken{0};          // 出错后被废弃的连接数
};

struct CotcpRedisOp : std::enable_shared_from_this<CotcpRedisOp> {
    enum class Kind { Ping, Get, Set, Exists, Delete };
    // kQueued 在队列中；kWorking owner coroutine 正在处理；kDone 后端不再访问。
    enum class Phase : int { kQueued = 0, kWorking = 1, kDone = 2 };

    CotcpRedisOp(std::shared_ptr<CotcpProbe> probe, Kind kind_,
                 std::vector<std::string> args, CallOptions opt)
        : probe(std::move(probe)), kind(kind_), options(std::move(opt)),
          argv_store(std::move(args)) {
        argv.reserve(argv_store.size());
        argvlen.reserve(argv_store.size());
        for (const auto& a : argv_store) {
            argv.push_back(a.data());
            argvlen.push_back(a.size());
        }
    }

    // 首次发布即逻辑终态：完成/超时/取消/owner close 竞争时先到者胜；晚到者只被
    // 计数，不覆盖首个终态。终态在 m_mtx 内先于唤醒发布，读侧经 TakeOutcome 取用。
    void Finish(result<RawReply> r) noexcept {
        bool first = false;
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            if (!m_done) {
                m_done  = true;
                outcome = std::move(r);
                first   = true;
            }
        }
        finished.store(true, std::memory_order_release);
        if (!first) {
            probe->finish_conflicts.fetch_add(1);
            return;
        }
        if (waiter)
            waiter->Notify();
    }

    // 取本次请求终态；未发布返回 nullopt（业务按 Closed 交付，不再等待）。
    std::optional<result<RawReply>> TakeOutcome() {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (!m_done)
            return std::nullopt;
        return std::optional<result<RawReply>>(std::move(*outcome));
    }

    std::shared_ptr<CotcpProbe>                       probe;
    Kind                                              kind;
    CallOptions                                       options;
    std::vector<std::string>                          argv_store;
    std::vector<const char*>                          argv;
    std::vector<size_t>                               argvlen;
    // 唯一等待位：命令提交前登记，io 域回包/关闭路径唤醒它。
    bbt::coroutine::sync::CoWaiter::SPtr              waiter;
    std::atomic_bool                                  finished{false};
    std::atomic<Phase>                                phase{Phase::kQueued};

private:
    std::mutex                        m_mtx;
    bool                              m_done{false};
    std::optional<result<RawReply>>   outcome;
};

// 隔离验证探针快照（纯整数，无 hiredis/第三方类型）。
struct CotcpBindingSnapshot {
    std::uint64_t owner_runs = 0;
    std::uint64_t finish_conflicts = 0;
    std::uint64_t skipped_after_finish = 0;
    std::uint64_t dial_attempts = 0;
    std::uint64_t dial_inflight = 0;  // >0：owner 批次正挂在 DialTCP 内
    std::uint64_t conns_broken = 0;
    std::uint64_t queued = 0;
    std::uint64_t registered = 0;
    bool          has_conn = false;
    // 物理收口判定：连接对象已释放，或其 Close 请求已发布且 CoTCP 已物理关闭。
    bool          conn_closed = false;
};

// 进程级 hiredis 调用/收口计数（定义在 .cc，跨连接累计）。
struct CotcpBindingTotals {
    std::uint64_t readers_created = 0;
    std::uint64_t readers_freed = 0;
    std::uint64_t conns_created = 0;
    std::uint64_t conns_destroyed = 0;
    std::uint64_t hiredis_calls = 0;
    std::uint64_t hiredis_max_call_ns = 0;
    std::uint64_t commands_encoded = 0;
    std::uint64_t write_rounds = 0;
    std::uint64_t read_rounds = 0;
    std::uint64_t partial_write_ops = 0;
    std::uint64_t partial_read_ops = 0;
};
CotcpBindingTotals CotcpBindingTotalsForTest() noexcept;
void ResetCotcpBindingTotalsForTest() noexcept;

class CoRedisCotcpCliImpl final : public CoRedisCli,
                                  public std::enable_shared_from_this<CoRedisCotcpCliImpl> {
public:
    CoRedisCotcpCliImpl(RedisClientConfig config,
                        bbt::coroutine::CoObjectInfo info)
        : m_config(std::move(config)), m_info(std::move(info)),
          m_probe(std::make_shared<CotcpProbe>()),
          m_dial_waiters(std::make_shared<detail::CloseWaiters>()) {}
    ~CoRedisCotcpCliImpl() override;

    result<void> Start() override;

    result<void> Ping(const CallOptions& options) override;
    result<std::optional<std::string>> Get(std::string_view     key,
                                          const CallOptions& options) override;
    result<void> Set(std::string_view key, std::string_view value,
                     const CallOptions& options) override;
    result<bool> Exists(std::string_view        key,
                        const CallOptions& options) override;
    result<std::uint64_t> Delete(std::vector<std::string> keys,
                                 const CallOptions& options) override;

    // owner 主动同步关闭：幂等、任意线程；返回即连接 CoTCP/fd、hiredis reader
    // 已物理回收、已登记 op 已收口、后端不再访问本对象拥有的操作资源。
    void Close() noexcept override;
    bool IsClosed() const noexcept override { return m_close.IsClosed(); }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override { return m_info; }

    // 测试接缝：等待事件已登记、op 尚未入队时暂停，用于验证 Close 竞态。
    void SetPreAdmitGateForTest(std::function<void()> gate);

    // ---- 隔离验证探针（src 内部头；不安装、不进公开面）----
    CotcpBindingSnapshot ProbeSnapshot() const;
    // 当前物理连接的 CoTCP 句柄（未建连时为 nullptr）。
    std::shared_ptr<tcp::CoTCP> TransportForTest() const;
    // 测试接缝：DialTCP 成功返回、进入编码/发送之前的落点（默认为空 ⇒ 零开销）。
    void SetPostDialGateForTest(std::function<void()> gate);

private:
    result<RawReply> Submit(CotcpRedisOp::Kind kind, std::vector<std::string> args,
                            const CallOptions& options);
    result<void>     PreCheck() const;

    // WaitWithCallback 的 on_registered：等待事件已登记后才把 op 入队；
    // 这样 owner/Close 的 Notify 不会早于等待位就绪而丢失。
    void AdmitOnWaitRegistered(const std::shared_ptr<CotcpRedisOp>& op);

    // ---- owner coroutine（独占执行域：编码→写→读→解码，串行消费队列）----
    void RunOwnerCoroutine();
    void ExecuteOne(const std::shared_ptr<CotcpRedisOp>& op);
    std::shared_ptr<CotcpRedisOp> TakeNextQueued();
    void RetireOp(const std::shared_ptr<CotcpRedisOp>& op);
    // 物理收口并释放连接对象（Close 的同步路径与 owner 批次退出共用）。
    void CloseAndDropConnOnCoroutine() noexcept;
    // 物理收口谓词：连接对象已释放，或其 Close 请求已发布且 CoTCP 已物理关闭。
    bool ConnPhysicallyClosed() const noexcept;
    void AbandonConnOnCoroutine() noexcept;
    void FinishAllRegistered(const Error& err) noexcept;
    void DrainCheckClosed() noexcept;
    bool OwnerRunning() const;
    bool IsIoDead() const;

    RedisClientConfig             m_config;
    bbt::coroutine::CoObjectInfo  m_info;
    detail::ManagedCloseState     m_close;
    std::shared_ptr<CotcpProbe>   m_probe;
    std::atomic<int>              m_state{kCreated};
    // owner 级 dial 关闭唤醒登记：Close 封口它即可打断在途 DialTCP（不再有取消令牌）。
    std::shared_ptr<detail::CloseWaiters> m_dial_waiters;

    // 队列与 owner 批次判定：同一把锁保证「清退出标志」与「入队+启动判定」互斥，
    // 不会丢唤醒（提交的命令必然被某批次消费或已被收口）。
    mutable std::mutex                        m_q_mtx;
    std::deque<std::shared_ptr<CotcpRedisOp>> m_queue;
    bool                                      m_owner_running{false}; // m_q_mtx
    bool                                      m_io_dead{false};       // m_q_mtx

    // 已登记 op（收口快照 + 关闭落定门控）。
    mutable std::mutex                               m_reg_mtx;
    std::unordered_set<std::shared_ptr<CotcpRedisOp>> m_registered;

    mutable std::mutex              m_conn_mtx;
    std::shared_ptr<RedisCotcpConn> m_conn; // 单连接，仅 owner coroutine 写入

    // 测试接缝（仅测试安装；不参与运行期语义）。
    std::function<void()> m_pre_admit_gate_for_test;
    std::function<void()> m_post_dial_gate_for_test;

    enum State : int { kCreated = 0, kRunning = 1, kClosingOrClosed = 2 };
};

// 内部装配入口（隔离验证专用）：与 CoRedisCli::Create 同一套配置校验与对象身份
// 来源，但装配 CoTCP owner binding 候选实现（不改变生产默认路径）。
result<std::shared_ptr<CoRedisCotcpCliImpl>> CreateCotcpRedisCli(
    RedisClientConfig config);

} // namespace bbt::infra::redis_detail
