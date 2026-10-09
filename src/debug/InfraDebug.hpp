#pragma once
// bbtools-infra 内部统一 debug 观测层（PR #70 review 整改）。
//
// 定位（对齐 bbtools-coroutine 的 BBT_COROUTINE_STRINGENT_DEBUG + detail/debug/）：
//   - 本文件整体只在 BBT_INFRA_STRINGENT_DEBUG 打开时展开（该宏由顶层 CMake 的
//     配置边界产生：仅在 Debug 构建、且 option BBT_INFRA_STRINGENT_DEBUG=ON 时经
//     目标级 INTERFACE compile definition 传播）。默认/Release 构建下本文件展开
//     为空：没有原子计数、没有取值符号，冷路径也不发生任何计数写入，对生产对象
//     布局零影响。
//   - 只放「仅观测」数据。真值由生产逻辑消费的运行状态（连接/在途配额、closed、
//     drain 计数、存活 worker/会话数、Redis 队列/槽/registered/finished 等）继续留
//     在各自模块内，不搬到这里——否则会把真实状态误绑到 debug 开关上，Release
//     语义随之改变。
//   - header-only + C++17 inline 变量/函数：库与消费者共享唯一定义，无 ODR/ABI 混配。
//     宏经构建面对消费者传播，全部 TU 取值一致；库与消费者不得分别开关该宏。
#ifdef BBT_INFRA_STRINGENT_DEBUG

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

namespace bbt::infra::debug {

// ================= HTTP：会话生命周期观测（issue #62 验收第 5 项）=================
// m_sessions 只保存裸指针：任何「析构时仍未反登记」都会给 Teardown 的裸指针
// 遍历留下悬垂项（UAF）。正常路径下 MaybeRelease 已在 Close（io 域门内、inflight
// 归零时）反登记，故 destroyed_while_registered 必须恒为 0。
struct HttpSessionLifetime {
    inline static std::atomic<int> destroyed{0};
    inline static std::atomic<int> destroyed_while_registered{0};
};

// 写入点：仅 ~HttpSession（冷路径）。生产路径（accept/read/write/Close）不触碰。
inline void OnHttpSessionDestroyed(bool registered) noexcept {
    HttpSessionLifetime::destroyed.fetch_add(1, std::memory_order_relaxed);
    if (registered)
        HttpSessionLifetime::destroyed_while_registered.fetch_add(
            1, std::memory_order_relaxed);
}

inline int HttpSessionDestroyed() noexcept {
    return HttpSessionLifetime::destroyed.load(std::memory_order_relaxed);
}

inline int HttpSessionDestroyedWhileRegistered() noexcept {
    return HttpSessionLifetime::destroyed_while_registered.load(
        std::memory_order_relaxed);
}

// ================= Redis：hiredis 同步调用统计（每连接值类型）=====================
// 证明 hiredis 调用全部在协程栈内同步返回（无库内等待）与 I/O 轮数。
struct HiredisSyncStats {
    std::uint64_t calls{0};
    std::uint64_t max_call_ns{0};
    std::uint64_t write_rounds{0};
    std::uint64_t read_rounds{0};
};

inline void HiredisStatsNoteCall(HiredisSyncStats& s,
                                 std::uint64_t     ns) noexcept {
    ++s.calls;
    if (ns > s.max_call_ns)
        s.max_call_ns = ns;
}

// ================= Redis：cotcp binding 进程级计数（跨连接累计）==================
struct RedisCotcpCounters {
    std::atomic<std::uint64_t> readers_created{0};
    std::atomic<std::uint64_t> readers_freed{0};
    std::atomic<std::uint64_t> conns_created{0};
    std::atomic<std::uint64_t> conns_destroyed{0};
    std::atomic<std::uint64_t> hiredis_calls{0};
    std::atomic<std::uint64_t> hiredis_max_call_ns{0};
    std::atomic<std::uint64_t> commands_encoded{0};
    std::atomic<std::uint64_t> write_rounds{0};
    std::atomic<std::uint64_t> read_rounds{0};
    std::atomic<std::uint64_t> partial_write_ops{0}; // 出现 >1 轮的写操作数
    std::atomic<std::uint64_t> partial_read_ops{0};  // 出现 >1 轮的读操作数
};

inline RedisCotcpCounters& RedisCotcpBindingCounters() noexcept {
    static RedisCotcpCounters counters;
    return counters;
}

inline void ResetRedisCotcpBindingCounters() noexcept {
    auto& c = RedisCotcpBindingCounters();
    c.readers_created.store(0);
    c.readers_freed.store(0);
    c.conns_created.store(0);
    c.conns_destroyed.store(0);
    c.hiredis_calls.store(0);
    c.hiredis_max_call_ns.store(0);
    c.commands_encoded.store(0);
    c.write_rounds.store(0);
    c.read_rounds.store(0);
    c.partial_write_ops.store(0);
    c.partial_read_ops.store(0);
}

inline void RedisCotcpBumpMax(std::atomic<std::uint64_t>& m,
                              std::uint64_t               v) noexcept {
    std::uint64_t cur = m.load(std::memory_order_relaxed);
    while (v > cur &&
           !m.compare_exchange_weak(cur, v, std::memory_order_relaxed)) {
    }
}

// ================= Redis：owner/cli 竞争观测（每 client 值类型）==================
// 仅观测：隔离验证用。真值状态（槽/代际/registered/finished 等）留在
// CoRedisCliImpl 内，不在此。
struct RedisProbe {
    std::atomic<std::uint64_t> owner_runs{0};           // owner coroutine 批次数
    std::atomic<std::uint64_t> finish_conflicts{0};     // 晚到终态被拒次数
    std::atomic<std::uint64_t> skipped_after_finish{0}; // 出队时已落定
    std::atomic<std::uint64_t> dial_attempts{0};
    std::atomic<std::uint64_t> dial_inflight{0}; // 正挂在 DialTCP 内的批次数
    std::atomic<std::uint64_t> conns_broken{0};  // 出错后废弃的连接数
};

// 纯观测快照（纯整数，无第三方类型）。真实状态快照见各模块的 BindingState。
struct RedisProbeSnapshot {
    std::uint64_t owner_runs{0};
    std::uint64_t finish_conflicts{0};
    std::uint64_t skipped_after_finish{0};
    std::uint64_t dial_attempts{0};
    std::uint64_t dial_inflight{0};
    std::uint64_t conns_broken{0};
};

// 进程级 hiredis 调用/收口计数快照（测试用，跨连接累计）。
struct RedisBindingTotals {
    std::uint64_t readers_created{0};
    std::uint64_t readers_freed{0};
    std::uint64_t conns_created{0};
    std::uint64_t conns_destroyed{0};
    std::uint64_t hiredis_calls{0};
    std::uint64_t hiredis_max_call_ns{0};
    std::uint64_t commands_encoded{0};
    std::uint64_t write_rounds{0};
    std::uint64_t read_rounds{0};
    std::uint64_t partial_write_ops{0};
    std::uint64_t partial_read_ops{0};
};

// ================= Mongo：per-runtime 并发观测 + 取证 =================
// 仅供 Mongo unit 通过内部头读取的执行域证据；不进入公共 API。
// 一次 driver 调用的 pool acquire/use/release 必须都发生在同一 worker。
struct MongoThreadEvidence {
    std::thread::id worker_thread;
    std::thread::id acquire_thread;
    std::thread::id use_thread;
    std::thread::id release_thread;
    std::string     database;
    std::string     collection;
};

// 每个 MongoRuntime 一个实例：运行中 driver 调用计数与峰值 + 取证存储/锁。
// 纯观测；不参与 drain 谓词（drain 用生产状态 m_live_workers/m_ops）。
struct MongoRuntimeObservation {
    std::atomic<std::size_t> running_calls{0};
    std::atomic<std::size_t> peak_calls{0};
    mutable std::mutex       evidence_mtx;
    std::optional<MongoThreadEvidence> last_evidence;

    void Record(MongoThreadEvidence evidence) {
        std::lock_guard<std::mutex> lk(evidence_mtx);
        last_evidence = std::move(evidence);
    }
    std::optional<MongoThreadEvidence> LastEvidence() const {
        std::lock_guard<std::mutex> lk(evidence_mtx);
        return last_evidence;
    }
};

} // namespace bbt::infra::debug

#endif // BBT_INFRA_STRINGENT_DEBUG
