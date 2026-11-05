#pragma once
// bbtools-infra 内部统一 debug 观测层。
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
