#pragma once
// mongo owner/v1（bbtools-infra#40）：协程原生 MongoDB 资源 owner 公共面。
//
// 归属模型：MongoRuntime 显式拥有连接配置、进程级 pool lease、有界
// worker 组、接纳队列与关闭排空；集合句柄（CoMongoColl）只是
// db.collection 目标值 + owner 引用，不新建线程。
//   - 同一 owner 的 N 个集合句柄共享同一组 worker 与同一 max_queue
//     接纳队列：worker 数不随句柄数增长，跨集合共享背压；
//   - 不同 owner 各自持有 worker 组/队列/ops 表，资源相互隔离；
//   - 每项 operation 在同一 worker 上 acquire/use/release pooled
//     client；driver 同步调用不可强杀，deadline/cancel/close 只发布
//     一次逻辑终态，物理收口由 worker 完成路径继续；
//   - mongocxx/bsoncxx/pool/thread 细节全部隐藏于 src/mongo/ 实现，
//     本头只出现 infra/标准库类型。

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>

#include <bbt/infra/CoMongoCli.hpp>
#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra::mongo {

// CoMongoDb 装配参数（owner 级资源预算）。本切片一个 owner 绑定一组
// worker + 一条接纳队列 + 一个生效 URI 的 pool lease；database/
// collection 不再是 owner 字段，由集合句柄携带。
//   - worker_threads：执行阻塞 driver 调用的 worker 线程数上限
//     （<=8；测试默认 <=2）；每项 operation 在同一 worker 上
//     acquire/use/release pooled client；
//   - max_queue：等待 worker 的 operation 队列容量，耗尽时新命令立即
//     返回 Overloaded，不无限排队；这是同一 owner 全部集合句柄共享的
//     背压上限；
//   - server_selection_timeout/connect_timeout/socket_timeout/
//     wait_queue_timeout：驱动层阻塞上界，语义同 MongoClientConfig。
struct MongoRuntimeConfig {
    std::string               uri;
    std::size_t               worker_threads;
    std::size_t               max_queue;
    std::chrono::milliseconds server_selection_timeout;
    std::chrono::milliseconds connect_timeout;
    std::chrono::milliseconds socket_timeout;
    std::chrono::milliseconds wait_queue_timeout;
};

inline result<void> ValidateMongoRuntimeConfig(
    const MongoRuntimeConfig& cfg) {
    if (cfg.uri.empty() ||
        (cfg.uri.rfind("mongodb://", 0) != 0 &&
         cfg.uri.rfind("mongodb+srv://", 0) != 0))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo runtime config: uri must start with mongodb:// or mongodb+srv://"));
    if (cfg.worker_threads == 0 ||
        cfg.worker_threads > kMongoLimitsMaxWorkerThreads)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo runtime config: worker_threads out of range"));
    if (cfg.max_queue == 0 || cfg.max_queue > kMongoLimitsMaxQueue)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo runtime config: max_queue out of range"));
    const auto valid_timeout = [](std::chrono::milliseconds t) {
        return t > std::chrono::milliseconds{0} &&
               t <= kMongoLimitsMaxTimeout;
    };
    if (!valid_timeout(cfg.server_selection_timeout))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo runtime config: server_selection_timeout out of range"));
    if (!valid_timeout(cfg.connect_timeout))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo runtime config: connect_timeout out of range"));
    if (!valid_timeout(cfg.socket_timeout))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo runtime config: socket_timeout out of range"));
    if (!valid_timeout(cfg.wait_queue_timeout))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo runtime config: wait_queue_timeout out of range"));
    return result<void>::ok();
}

// MongoTarget：一次命令的目标集合。owner 不关心 database/collection；
// 句柄只携带目标值。database/collection 语法校验仍在提交时进行，
// 非法输入返回 InvalidArgument。
struct MongoTarget {
    std::string database;
    std::string collection;
};

inline result<void> ValidateMongoTarget(const MongoTarget& t) {
    if (t.database.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo target: empty database"));
    if (t.collection.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo target: empty collection"));
    return result<void>::ok();
}

// CoMongoColl：集合句柄（轻量目标值 + owner 引用）。句柄不拥有
// worker/队列/pool；关闭句柄只停止该句柄的接纳与新命令前置校验，
// 不影响同 owner 的兄弟句柄，也不等待 owner drain。
class CoMongoColl : public ICoNetwork, public ICoCloseable {
public:
    // 句柄级命令：语义与 CoMongoCli 同名方法一致，差别只在目标集合
    // 由句柄携带而非 owner 绑定。
    virtual result<void> InsertOne(const MongoDocument& doc,
                                   const CallOptions&   options) = 0;
    virtual result<std::optional<MongoDocument>> FindOne(
        const MongoDocument& filter, const CallOptions& options) = 0;
    virtual result<MongoUpdateResult> UpdateOne(
        const MongoDocument& filter, const MongoDocument& update,
        const CallOptions&   options) = 0;
    virtual result<std::uint64_t> DeleteOne(const MongoDocument& filter,
                                            const CallOptions&   options) = 0;
};

// CoMongoDb：mongo 资源 owner（连接配置 + pool lease + worker 组 +
// 接纳队列 + 关闭排空）。Create/Start 在启动控制线程使用，不挂起
// 协程；Create 要求 Scheduler 已启动。
//
// 关闭语义：RequestClose 停止接纳并触发 drain（未派发 op 落定
// Closed，已进入 driver 的调用继续到自身超时上界后收口）；
// IsClosed/WaitClosed 反映物理收口——ops 清空且 worker 全退才
// Closed，不提前宣告。句柄关闭（CoMongoColl::RequestClose）是接纳
// 门禁，只停该句柄的新命令，不动兄弟句柄与 owner。
//
// 命令语义边界（与 CoMongoCli 相同）：
//   - 全部命令只能在协程上下文调用，否则返回 Error(InvalidContext)；
//   - 服务端错误回复映射为 Error(RemoteError)；driver 侧失败映射为
//     Error(Unavailable)；无效 BSON/参数为 InvalidArgument；pool wait
//     queue 超时为 Overloaded；
//   - deadline/cancel/owner close 竞争只发布一次逻辑终态；已进入
//     driver 的同步调用不可强杀，op/lease/payload 保活到物理收口，
//     WaitClosed 等待在途 driver 调用与队列真正归零。
class CoMongoDb : public ICoNetwork, public ICoCloseable {
public:
    static result<std::shared_ptr<CoMongoDb>> Create(
        MongoRuntimeConfig config);

    // 同一 owner 只成功启动一次；要求与 Create 同一运行时代际。
    // Start 建立 worker 线程组并接入进程级 pool；关闭后不重开。
    virtual result<void> Start() = 0;

    // 创建集合句柄：owner 必须 Running；target.database/.collection
    // 空则 InvalidArgument。句柄共享 owner 的 worker/队列/pool，不新建
    // 线程。owner 关闭后不再接纳新句柄。
    virtual result<std::shared_ptr<CoMongoColl>> Collection(
        MongoTarget target) = 0;
};

} // namespace bbt::infra::mongo
