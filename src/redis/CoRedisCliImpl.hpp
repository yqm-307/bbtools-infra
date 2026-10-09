#pragma once
// CoRedisCliImpl：Redis 公共接口的唯一具体实现。
//
// CoTCP 仅作为本实现内部的窄连接组件使用：它独占 socket、Dial、读写、等待
// 和关闭；hiredis 只负责 RESP2 编码与 reader/reply 解码。公共头不暴露任何
// CoTCP、hiredis、fd 或 owner coroutine 类型。

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

#include "debug/InfraDebug.hpp"   // 纯观测数据集中层（仅 Debug 展开）
#include "detail/IoSupport.hpp"
#include "redis/RedisDetail.hpp"   // RawReply / DecodeReply

namespace bbt::infra::redis_detail {

class RedisCotcpConn;

// owner coroutine 与落定竞争的纯观测计数已集中到 src/debug/InfraDebug.hpp
// （debug::RedisProbe / debug::RedisProbeSnapshot）；仅内部 debug 层开启时存在，
// 不参与语义，Release 无该成员、无相关原子写。

struct RedisOp : std::enable_shared_from_this<RedisOp> {
    enum class Kind { Ping, Get, Set, Exists, Delete };
    // kQueued 在队列中；kWorking owner coroutine 正在处理；kDone 后端不再访问。
    enum class Phase : int { kQueued = 0, kWorking = 1, kDone = 2 };

    RedisOp(
#ifdef BBT_INFRA_STRINGENT_DEBUG
        std::shared_ptr<debug::RedisProbe> probe_,
#endif
        Kind kind_, std::vector<std::string> args, CallOptions opt)
        :
#ifdef BBT_INFRA_STRINGENT_DEBUG
          probe(std::move(probe_)),
#endif
          kind(kind_), options(std::move(opt)), argv_store(std::move(args)) {
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
#ifdef BBT_INFRA_STRINGENT_DEBUG
            probe->finish_conflicts.fetch_add(1);
#endif
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

#ifdef BBT_INFRA_STRINGENT_DEBUG
    std::shared_ptr<debug::RedisProbe>                probe;
#endif
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

// 真实运行状态快照（纯整数，无 hiredis/第三方类型）：始终存在，不受 debug 开关
// 影响——只含由生产逻辑消费/可生产观测的真实状态（队列长度、已登记请求数、
// 当前槽是否有连接、连接是否物理收口）。纯观测计数见 debug::RedisProbeSnapshot。
struct RedisBindingSnapshot {
    std::uint64_t queued = 0;
    std::uint64_t registered = 0;
    bool          has_conn = false;
    // 物理收口判定：连接对象已释放，或其 Close 请求已发布且 CoTCP 已物理关闭。
    bool          conn_closed = false;
};

#ifdef BBT_INFRA_STRINGENT_DEBUG
// 进程级 hiredis 调用/收口计数快照（定义在 .cc，跨连接累计；仅 Debug 面存在）。
using debug::RedisBindingTotals;
RedisBindingTotals RedisBindingTotalsForTest() noexcept;
void ResetRedisBindingTotalsForTest() noexcept;
#endif

class CoRedisCliImpl final : public CoRedisCli,
                                  public std::enable_shared_from_this<CoRedisCliImpl> {
public:
    CoRedisCliImpl(RedisClientConfig config,
                        bbt::coroutine::CoObjectInfo info)
        : m_config(std::move(config)), m_info(std::move(info))
#ifdef BBT_INFRA_STRINGENT_DEBUG
        , m_probe(std::make_shared<debug::RedisProbe>())
#endif
    {}
    ~CoRedisCliImpl() override;

    result<void> Connect(const CallOptions& options) override;
    ConnectState ConnectStatus() const noexcept override;
    void         Disconnect() noexcept override;

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
    // 真实状态快照（始终可用，Release 面同样有真实断言依据）。
    RedisBindingSnapshot BindingStateSnapshot() const;
#ifdef BBT_INFRA_STRINGENT_DEBUG
    // 纯观测快照（仅内部 debug 层开启时存在）。
    debug::RedisProbeSnapshot ProbeSnapshot() const;
#endif
    // 当前物理连接的 CoTCP 句柄（未建连时为 nullptr）。
    std::shared_ptr<tcp::CoTCP> TransportForTest() const;
    // 测试接缝：DialTCP 成功返回、进入编码/发送之前的落点（默认为空 ⇒ 零开销）。
    void SetPostDialGateForTest(std::function<void()> gate);
    // 测试接缝：受管 dial 的 connect 等待段在协程真正挂起（已登记 fd 可写等待）
    // 后回调一次。这是「dial 已进入 native 等待」的真实落点证据（DialWaitOptions::
    // connect_on_registered），Debug/Release 一致，不用观测计数冒充、不用 sleep。
    void SetDialWaitEntryGateForTest(std::function<void()> gate);

private:
    result<RawReply> Submit(RedisOp::Kind kind, std::vector<std::string> args,
                            const CallOptions& options);
    result<void>     PreCheck() const;

    // WaitWithCallback 的 on_registered：等待事件已登记后才把 op 入队；
    // 这样 owner/Close 的 Notify 不会早于等待位就绪而丢失。
    void AdmitOnWaitRegistered(const std::shared_ptr<RedisOp>& op);

    // ---- 显式连接生命周期（状态机 + 连接代际隔离）----
    // 真实建连：在调用协程内等待 Dial 完成；成功置 Connected。要求调用方已完成
    // Connecting 状态迁移。
    result<void> EstablishConn(const CallOptions& options);
    // 原子发布新连接槽（代际 +1），输出旧槽以供调用方封口/收口；已开始 Close 时
    // 不发布并返回 0。
    std::uint64_t PublishSlot(const std::shared_ptr<RedisCotcpConn>& conn,
                              const std::shared_ptr<detail::CloseWaiters>& waiters,
                              std::shared_ptr<RedisCotcpConn>* out_old_conn,
                              std::shared_ptr<detail::CloseWaiters>* out_old_waiters);
    // 若当前槽仍是 gen 则摘除（返回 true）；否则不动（返回 false）。
    bool TakeSlotIfGen(std::uint64_t gen,
                       std::shared_ptr<RedisCotcpConn>* out_conn,
                       std::shared_ptr<detail::CloseWaiters>* out_waiters);
    // 摘除当前槽（不限代际）并推进代际；Disconnect/Close/owner 收尾用。
    void TakeSlot(std::shared_ptr<RedisCotcpConn>* out_conn,
                  std::shared_ptr<detail::CloseWaiters>* out_waiters);
    // 当前槽连接快照（未连接为 nullptr）。
    std::shared_ptr<RedisCotcpConn> SlotConn() const;
    // 状态迁移：Closed 是终态不可被覆盖；only_if_connecting 只允许
    // Connecting→want（用于建连完成/失败路径，防止旧代际改写新状态）。
    void SetState(ConnectState want, bool only_if_connecting) noexcept;
    // 连接故障线性化专用：仅当仍处于 Connected（确有活连接被判不可复用）时迁移到
    // Failed，避免迟到 owner 清理覆盖 Disconnect/Close 已落定状态。
    void MarkFailedFromConnected() noexcept;
    // 显式 Disconnect / Close 的槽收口：封口 dial 唤醒登记 + 物理关闭连接。
    void SealAndCloseSlot(std::shared_ptr<RedisCotcpConn>       conn,
                          std::shared_ptr<detail::CloseWaiters> waiters) noexcept;

    // ---- owner coroutine（独占执行域：编码→写→读→解码，串行消费队列）----
    void RunOwnerCoroutine();
    void ExecuteOne(const std::shared_ptr<RedisOp>& op);
    std::shared_ptr<RedisOp> TakeNextQueued();
    void RetireOp(const std::shared_ptr<RedisOp>& op);
    // 物理收口并释放连接对象（Close 的同步路径与 owner 批次退出共用）。
    void CloseAndDropConnOnCoroutine() noexcept;
    // 物理收口谓词：连接对象已释放，或其 Close 请求已发布且 CoTCP 已物理关闭。
    bool ConnPhysicallyClosed() const noexcept;
    // 连接故障线性化：摘除并物理收口当前连接（推进代际，使迟到 Dial 完成不得
    // 改写新状态）、置 Failed，并排空「故障前已排队」的命令（它们不得跨连接迁移，
    // 按 TransportError 落定）。排在 q_mtx 内，故障后新提交的命令不会被误排空。
    void FailConnAndDrainQueue() noexcept;
    void FinishAllRegistered(const Error& err) noexcept;
    void DrainCheckClosed() noexcept;
    bool IsIoDead() const;

    RedisClientConfig             m_config;
    bbt::coroutine::CoObjectInfo  m_info;
    detail::ManagedCloseState     m_close;
#ifdef BBT_INFRA_STRINGENT_DEBUG
    std::shared_ptr<debug::RedisProbe> m_probe;
#endif
    std::atomic<int>              m_state{
        static_cast<int>(ConnectState::Disconnected)};

    // 当前连接槽（m_conn_mtx 保护）：连接对象、它的 dial 主动关闭唤醒登记与
    // 代际令牌同槽原子发布/摘除。代际令牌隔离在途 Dial 与迟到 owner 清理——每次
    // 可重建连接使用新的 dial CloseWaiters 与代际，旧代际的完成/失败路径不得改写
    // 新连接状态或排空新连接队列。
    struct ConnSlot {
        std::shared_ptr<RedisCotcpConn>       conn;
        std::shared_ptr<detail::CloseWaiters> dial_waiters;
        std::uint64_t                         gen{0};
    };
    mutable std::mutex m_conn_mtx;
    ConnSlot           m_slot;        // m_conn_mtx
    std::uint64_t      m_next_gen{0}; // m_conn_mtx

    // 队列与 owner 批次判定：同一把锁保证「清退出标志」与「入队+启动判定」互斥，
    // 不会丢唤醒（提交的命令必然被某批次消费或已被收口）。
    mutable std::mutex                        m_q_mtx;
    std::deque<std::shared_ptr<RedisOp>> m_queue;
    bool                                      m_owner_running{false}; // m_q_mtx
    bool                                      m_io_dead{false};       // m_q_mtx

    // 已登记 op（收口快照 + 关闭落定门控）。
    mutable std::mutex                               m_reg_mtx;
    std::unordered_set<std::shared_ptr<RedisOp>> m_registered;

    // 测试接缝（仅测试安装；不参与运行期语义）。
    std::function<void()> m_pre_admit_gate_for_test;
    std::function<void()> m_post_dial_gate_for_test;
    std::function<void()> m_dial_wait_gate_for_test;
};


} // namespace bbt::infra::redis_detail
