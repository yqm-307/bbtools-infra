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
//     client；driver 同步调用不可强杀，deadline/close 只发布
//     一次逻辑终态，物理收口由 worker 完成路径继续；
//   - mongocxx/bsoncxx/pool/thread 细节全部隐藏于 src/mongo/ 实现，
//     本头只出现 infra/标准库类型。

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/mongo/Bson.hpp>

namespace bbt::infra::mongo {

// mongo 装配参数的公共上限（owner 与旧 CoMongoCli 契约共用同一组边界，
// 逐项大于零且不超上限，违规返回 InvalidArgument）。
inline constexpr std::size_t kMongoLimitsMaxWorkerThreads = 8;
inline constexpr std::size_t kMongoLimitsMaxQueue         = 1000000;
inline constexpr std::chrono::milliseconds kMongoLimitsMaxTimeout{
    std::chrono::minutes{10}};

// ---- 多文档数据面的公共上限（一次性内存物化边界；违规在接纳前返回 ----
// ---- InvalidArgument，零接纳、什么都不发）----
// InsertMany 批量上限：文档数 ≤ 500 且提交的总 BSON 字节 ≤ 8 MiB
// （依据：MongoDB 单条命令/文档的 16 MiB BSON 上限，预留约一半余量给
// 命令封套与 insert 参数，防止单次调用物化出接近协议上限的命令）。
inline constexpr std::size_t kMongoLimitsMaxInsertDocs      = 500;
inline constexpr std::size_t kMongoLimitsMaxInsertTotalBytes =
    8u * 1024u * 1024u;
// 单份 BSON 输入文档的上限：filter/update/sort/projection 以及 InsertMany
// 列表中的单文档均不得超 8 MiB（同一命令输入预算，依据同上）。超限在
// CollSubmit 之前拒绝，越界零接纳。
inline constexpr std::size_t kMongoLimitsMaxInputDocBytes   =
    kMongoLimitsMaxInsertTotalBytes;
// Find 必填 limit 的上界：单次物化 ≤ 1000 行且返回文档总字节 ≤ 16 MiB
// （依据：一页数据的内存物化上限，杜绝无界游标物化 OOM；更大结果集
// 走分页或流式，属已知 ceiling，需要时再扩）。
inline constexpr std::size_t kMongoLimitsMaxFindLimit       = 1000;
inline constexpr std::size_t kMongoLimitsMaxFindResultBytes =
    16u * 1024u * 1024u;

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

// FindOptions：多文档 Find 的谓词选项。limit **必填**（调用方必须显式给出），
// 杜绝无界游标物化——limit==0 或 > kMongoLimitsMaxFindLimit 都在接纳前
// 返回 InvalidArgument；skip 为分页偏移（0 起），> INT64_MAX 无法交给
// driver，同样在接纳前 InvalidArgument（不另设分页上限——driver 的
// int64 表示范围即边界）。sort/projection 为可选 BSON 文档（MongoDocument），
// bytes 为空视为未设置；filter/sort/projection 任一字节数超
// kMongoLimitsMaxInputDocBytes 也在接纳前 InvalidArgument。
// 返回侧另有一道硬上限：命中文档累计 BSON 字节超
// kMongoLimitsMaxFindResultBytes 时返回明确错误（RemoteError，
// domain_code="result_too_large"）而不是 OOM。
struct FindOptions {
    std::size_t      limit = 0;                  // 必填：每页行数上限
    std::size_t      skip = 0;                   // 分页偏移
    MongoDocument    sort{};                     // 可选：BSON sort 文档
    MongoDocument    projection{};               // 可选：BSON projection 文档
};

// InsertMany 的回执：有序批量插入的服务端结果。
// 部分成功契约（调用方据此观察）：
//   - 全部成功 → ok(result)，inserted == docs.size()；
//   - 有序插入中途失败（如重复 _id）→ err(RemoteError)，此前已成功的
//     文档**已保留**在服务端；调用方通过错误通道观察部分成功：
//     Error.details["inserted_count"] 携带服务端回执的已写入数（来自
//     raw reply 的 nInserted/n，或 ordered 写入时首个 writeErrors.index，
//     三键任一可用时给出；服务端均未给出时该键缺省），
//     Error.details["requested_count"] 携带请求总数，
//     Error.details["failed_index"] 携带首个失败下标（服务端给出时）；
//     domain_code/backend_code 仍承载服务端 codeName/code。
struct MongoInsertManyResult {
    std::uint64_t inserted = 0;
};

// CoMongoColl：集合句柄（轻量目标值 + owner 引用）。句柄不拥有
// worker/队列/pool；Close() 只封该句柄的接纳与新命令前置校验（幂等、
// 任意线程），不影响同 owner 的兄弟句柄，也不等待 owner 在途归零。
//
// 等待入口：CRUD 用上游 bbt_co_async 标记返回类型，调用方必须经
// bbt_co_await 消费（`auto r = bbt_co_await coll->FindOne(...)`）。标记只
// 附加返回类型、不改变实现与语义；只有写了 await 的表达式才会在非协程域先撞
// AwaitHelper 的 std::logic_error（早于本组件的 InvalidContext 校验）。漏写
// await 直接持有返回值的调用不被静态拦住、函数体照常执行，故这不是协程外
// 调用的完整静态强制。
// Create/Start/Collection/Close 不挂起，保持裸 result<void>/void。
//
// 空 filter 的语义边界（本任务的一条设计决定，可被推翻）：
//   - UpdateMany / DeleteMany 一律拒绝空 filter（bytes 为空或 `{}` 空
//     文档）→ InvalidArgument：防止一句空谓词误改/误删全集合。要全量
//     更新/删除须写显式匹配条件（如 `{"_id":{"$exists":true}}`）；
//   - Find / CountDocuments 的空 filter 是合法的「全集合」谓词；
//   - InsertOne/FindOne/UpdateOne/DeleteOne 沿用既有校验（bytes 空拒绝）。
class CoMongoColl : public ICoNetwork, public ICoCloseable {
public:
    // 句柄级命令：语义与 CoMongoCli 同名方法一致，差别只在目标集合
    // 由句柄携带而非 owner 绑定。
    virtual bbt_co_async(result<void>) InsertOne(const MongoDocument& doc,
                                                 const CallOptions&   options) = 0;
    virtual bbt_co_async(result<std::optional<MongoDocument>>) FindOne(
        const MongoDocument& filter, const CallOptions& options) = 0;
    virtual bbt_co_async(result<MongoUpdateResult>) UpdateOne(
        const MongoDocument& filter, const MongoDocument& update,
        const CallOptions&   options) = 0;
    virtual bbt_co_async(result<std::uint64_t>) DeleteOne(
        const MongoDocument& filter, const CallOptions& options) = 0;

    // ---- 多文档数据面（data-plane-1030）----
    // InsertMany：有序批量插入。空列表、文档数 > kMongoLimitsMaxInsertDocs、
    // 总 BSON 字节 > kMongoLimitsMaxInsertTotalBytes、列表内含空/非法文档
    // → InvalidArgument（零接纳）。全部成功 → inserted == docs.size()；
    // 中途失败 → RemoteError + details 计数（见 MongoInsertManyResult 注释）。
    virtual bbt_co_async(result<MongoInsertManyResult>) InsertMany(
        const std::vector<MongoDocument>& docs,
        const CallOptions&                options) = 0;

    // Find：多文档读取。filter 可为空（匹配全部）；options.limit 必填且
    // ≤ kMongoLimitsMaxFindLimit，options.skip ≤ INT64_MAX，filter/sort/
    // projection 各自 BSON ≤ kMongoLimitsMaxInputDocBytes，否则
    // InvalidArgument（零接纳）。返回按游标顺序物化的命中页；无匹配返回
    // 空 vector（不是 NotFound）。返回总字节超
    // kMongoLimitsMaxFindResultBytes → RemoteError(domain_code=
    // "result_too_large")。
    virtual bbt_co_async(result<std::vector<MongoDocument>>) Find(
        const MongoDocument& filter, const FindOptions& find_options,
        const CallOptions&   options) = 0;

    // UpdateMany：复用 UpdateOne 的 update 文档校验；空 filter 一律
    // InvalidArgument（语义边界见类注释）；filter/update 任一 BSON 超
    // kMongoLimitsMaxInputDocBytes 同样 InvalidArgument（零接纳）。
    // 返回 matched/modified/upserted。
    virtual bbt_co_async(result<MongoUpdateResult>) UpdateMany(
        const MongoDocument& filter, const MongoDocument& update,
        const CallOptions&   options) = 0;

    // DeleteMany：空 filter 一律 InvalidArgument；filter BSON 超
    // kMongoLimitsMaxInputDocBytes 同样 InvalidArgument。返回删除数量。
    virtual bbt_co_async(result<std::uint64_t>) DeleteMany(
        const MongoDocument& filter, const CallOptions& options) = 0;

    // CountDocuments：精确计数。filter 可为空（统计全集合）；filter BSON
    // 超 kMongoLimitsMaxInputDocBytes → InvalidArgument。
    virtual bbt_co_async(result<std::uint64_t>) CountDocuments(
        const MongoDocument& filter, const CallOptions& options) = 0;
};

// CoMongoDb：mongo 资源 owner（连接配置 + pool lease + worker 组 +
// 接纳队列 + 关闭排空）。Create/Start 在启动控制线程使用，不挂起
// 协程；Start 要求协程运行时已初始化。
//
// 关闭语义：Close() 由 owner 主动同步发起（幂等、任意线程可调用）——
// 封口停止接纳、唤醒全部挂起业务等待者（未派发 op 立即落定 Closed，
// 已进入 driver 的调用继续到自身超时上界，迟到结果只消费不交付），
// 然后在有界窗口内等 in-flight 归零；ops 清空且 worker 全退才
// IsClosed()==true，不提前宣告。句柄关闭（CoMongoColl::Close）是接纳
// 门禁，只停该句柄的新命令，不动兄弟句柄与 owner。
//
// 命令语义边界（与 CoMongoCli 相同）：
//   - 全部命令只能在协程上下文调用，否则返回 Error(InvalidContext)；
//   - 服务端错误回复映射为 Error(RemoteError)；driver 侧失败映射为
//     Error(Unavailable)；无效 BSON/参数为 InvalidArgument；pool wait
//     queue 超时为 Overloaded；
//   - deadline/owner close 竞争只发布一次逻辑终态；已进入 driver 的
//     同步调用不可强杀，op/lease/payload 保活到物理收口。
class CoMongoDb : public ICoNetwork, public ICoCloseable {
public:
    static result<std::shared_ptr<CoMongoDb>> Create(
        MongoRuntimeConfig config);

    // 同一 owner 只成功启动一次；Start 要求协程运行时已初始化，
    // 建立 worker 线程组并接入进程级 pool；关闭后不重开。
    virtual result<void> Start() = 0;

    // 创建集合句柄：owner 必须 Running；target.database/.collection
    // 空则 InvalidArgument。句柄共享 owner 的 worker/队列/pool，不新建
    // 线程。owner 关闭后不再接纳新句柄。
    virtual result<std::shared_ptr<CoMongoColl>> Collection(
        MongoTarget target) = 0;
};

} // namespace bbt::infra::mongo
