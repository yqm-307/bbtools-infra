#pragma once
// 候选 B 级 owner binding Redis client（隔离实现，公开 CoRedisCli 语义不变）。
//
// 与生产路径的关系：本类是**独立实现**（新文件、新符号、新装配入口），
// 生产 CoRedisCli::Create 仍返回 hiredis async + strand 实现（CoRedisCliImpl）；
// 候选只经内部工厂 redis_detail::CreateCotcpRedisCli 装配，供隔离验证使用，
// 不切换生产默认路径。
//
// 执行模型（契约 §8「一个 coroutine 独占完整请求—响应」）：
//   - 命令由调用协程登记后挂起在 CompletionSignal 上（逻辑等待与旧路径一致）；
//   - 独占 owner coroutine 按 FIFO 串行执行「DialTCP → 编码 → WriteSome →
//     ReadSome → 解码」，hiredis 调用全部同步返回，可挂起的等待只发生在 CoTCP 内；
//   - 串行语义：同一连接同一时刻只有一条命令在途（不再有 async 回调并发推进），
//     max_inflight 不参与流水线；并发命令按 max_queue 排队，满即 Overloaded；
//   - 任一步失败即令该连接不可复用（丢连接、关闭 fd），下一条命令重新 DialTCP
//     ——与旧路径「不重发已断命令、新命令触发重连」一致；不同点是候选不做空闲
//     断连探测（不挂常驻可读等待），断连在下次命令时被发现。
//
// 本头仅 src/ 内部使用（不安装、不进 INSTALL_INTERFACE）；不含 hiredis 类型，
// 便于隔离测试在不需要 hiredis 头的情况下断言候选行为。
//
// 范围（候选边界，不得外推）：**单连接 / 单 owner**——一个 client 同一时刻至多一条
// 真实连接、一条在途命令；不覆盖多连接池、流水线、空闲断连探测、TLS/RESP3/
// Cluster/Sentinel 与跨平台。本候选不构成「完整 §10 B 级验收」，也不切换生产默认路径。

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
    std::atomic<std::uint64_t> owner_runs{0};           // owner coroutine 批次数
    std::atomic<std::uint64_t> finish_conflicts{0};      // 晚到终态被 CAS 拒绝次数
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

    // 首次发布即逻辑终态：完成/取消/deadline/owner close 竞争时先到者胜；
    // 晚到者只被计数，不覆盖首个终态（与旧路径 RedisOp::Finish 同一纪律）。
    void Finish(result<RawReply> r) noexcept {
        if (finished.exchange(true)) {
            probe->finish_conflicts.fetch_add(1);
            return;
        }
        outcome = std::move(r);
        sig->Complete();
    }

    std::shared_ptr<CotcpProbe>                       probe;
    Kind                                              kind;
    CallOptions                                       options;
    std::vector<std::string>                          argv_store;
    std::vector<const char*>                          argv;
    std::vector<size_t>                               argvlen;
    std::shared_ptr<bbt::coroutine::CompletionSignal> sig;
    std::optional<result<RawReply>>                   outcome;
    std::atomic_bool                                  finished{false};
    std::atomic<Phase>                                phase{Phase::kQueued};
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
    // 物理收口判定（与 DrainCheckClosed 同一谓词）：连接对象已释放，或已发布关闭
    // 请求且 CoTCP 已物理关闭（无在途拨号）。
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
    CoRedisCotcpCliImpl(RedisClientConfig                  config,
                        bbt::coroutine::CoObjectInfo        info,
                        std::shared_ptr<bbt::coroutine::CompletionSignal> close_sig)
        : m_config(std::move(config)), m_info(std::move(info)),
          m_close(std::move(close_sig)), m_probe(std::make_shared<CotcpProbe>()) {}
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

    void RequestClose() noexcept override;
    bool IsClosed() const noexcept override { return m_close.IsClosed(); }
    CloseStatus WaitClosed(bbt::coroutine::Deadline          deadline,
                           bbt::coroutine::CancellationToken cancel) override {
        return m_close.WaitClosed(deadline, std::move(cancel), m_info.generation);
    }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override { return m_info; }

    // ---- 隔离验证探针（src 内部头；不安装、不进公开面）----
    CotcpBindingSnapshot ProbeSnapshot() const;
    // 当前物理连接的 CoTCP 句柄（未建连时为 nullptr）：测试经装配面读 fd、
    // 装 closed_hook 或执行硬停后的强制物理关闭。
    std::shared_ptr<tcp::CoTCP> TransportForTest() const;
    // 测试接缝：DialTCP 成功返回、进入编码/发送之前的落点（默认为空 ⇒ 零开销）。
    // 用于确定性复现「拨号成功瞬间关闭已封口」窗口：此后不得编码/发送。
    void SetPostDialGateForTest(std::function<void()> gate);

private:
    result<RawReply> Submit(CotcpRedisOp::Kind kind, std::vector<std::string> args,
                            const CallOptions& options);
    result<void>     PreCheck() const;

    // ---- owner coroutine（独占执行域：编码→写→读→解码，串行消费队列）----
    void RunOwnerCoroutine();
    void ExecuteOne(const std::shared_ptr<CotcpRedisOp>& op);
    std::shared_ptr<CotcpRedisOp> TakeNextQueued();
    void RetireOp(const std::shared_ptr<CotcpRedisOp>& op);
    void DropConnOnCoroutine() noexcept;
    // B1：物理收口并释放连接对象（RequestClose + reset），供关闭路径与批次退出使用。
    void CloseAndDropConnOnCoroutine() noexcept;
    // B1：物理收口谓词（DrainCheckClosed 的 Closed 前置）。
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

    // 队列与 owner 批次判定：同一把锁保证「清退出标志」与「入队+启动判定」
    // 互斥，不会丢唤醒（提交的命令必然被某批次消费或已被收口）。
    mutable std::mutex                        m_q_mtx;
    std::deque<std::shared_ptr<CotcpRedisOp>> m_queue;
    bool                                      m_owner_running{false}; // m_q_mtx
    bool                                      m_io_dead{false};       // m_q_mtx

    // 已登记 op（跨线程收口快照 + Closed 落定门控）。
    mutable std::mutex                               m_reg_mtx;
    std::unordered_set<std::shared_ptr<CotcpRedisOp>> m_registered;

    mutable std::mutex              m_conn_mtx;
    std::shared_ptr<RedisCotcpConn> m_conn; // 仅 owner coroutine 改写

    // 测试接缝（仅测试安装；不参与运行期语义）。
    std::function<void()> m_post_dial_gate_for_test;

    enum State : int { kCreated = 0, kRunning = 1, kClosingOrClosed = 2 };
};

// 内部装配入口（隔离验证专用）：与 CoRedisCli::Create 同一套配置校验与
// 对象身份/完成信号来源，但装配 CoTCP owner binding 候选实现。
result<std::shared_ptr<CoRedisCotcpCliImpl>> CreateCotcpRedisCli(
    RedisClientConfig config);

} // namespace bbt::infra::redis_detail
