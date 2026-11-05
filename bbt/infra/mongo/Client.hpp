/** @file Client.hpp
 * @brief 协程原生 MongoDB 资源 owner 公共面（契约见 docs/decisions/0007-mongo-owner-handles.md）。
 *
 * 归属模型：MongoRuntime 显式拥有连接配置、进程级 pool lease、有界
 * worker 组、接纳队列与关闭排空；集合句柄（CoMongoColl）只是
 * db.collection 目标值 + owner 引用，不新建线程。
 *   - 同一 owner 的 N 个集合句柄共享同一组 worker 与同一 max_queue
 *     接纳队列：worker 数不随句柄数增长，跨集合共享背压；
 *   - 不同 owner 各自持有 worker 组/队列/ops 表，资源相互隔离；
 *   - 每项 operation 在同一 worker 上 acquire/use/release pooled
 *     client；driver 同步调用不可强杀，deadline/close 只发布
 *     一次逻辑终态，物理收口由 worker 完成路径继续；
 *   - mongocxx/bsoncxx/pool/thread 细节全部隐藏于 bbt/infra/mongo/detail/ 实现，
 *     本头只出现 infra/标准库类型。
 */
#pragma once

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

/**
 * @brief mongo 装配参数的公共上限（owner 与旧 CoMongoCli 契约共用同一组边界，
 * 逐项大于零且不超上限，违规返回 InvalidArgument）。
 */
inline constexpr std::size_t kMongoLimitsMaxWorkerThreads = 8; ///< worker 线程数上限。
inline constexpr std::size_t kMongoLimitsMaxQueue = 1000000;   ///< 接纳队列容量上限。
inline constexpr std::chrono::milliseconds kMongoLimitsMaxTimeout{
    std::chrono::minutes{10}}; ///< 单个驱动层超时的上限（10 分钟）。

/**
 * @brief 多文档数据面的公共上限（一次性内存物化边界）。
 *
 * 违规在接纳前返回 InvalidArgument，零接纳、什么都不发。
 *   - InsertMany 批量上限：文档数 ≤ 500 且提交的总 BSON 字节 ≤ 8 MiB（依据：MongoDB
 *     单条命令/文档的 16 MiB BSON 上限，预留约一半余量给命令封套与 insert 参数，
 *     防止单次调用物化出接近协议上限的命令）。
 *   - 单份 BSON 输入文档上限 kMongoLimitsMaxInputDocBytes：filter/update/sort/projection
 *     以及 InsertMany 列表中的单文档均不得超 8 MiB（同一命令输入预算，依据同上）。
 *     超限在 CollSubmit 之前拒绝，越界零接纳。
 *   - Find 上界：单次物化 ≤ 1000 行且返回文档总字节 ≤ 16 MiB（依据：一页数据的内存
 *     物化上限，杜绝无界游标物化 OOM；更大结果集走分页或流式，属已知 ceiling）。
 */
inline constexpr std::size_t kMongoLimitsMaxInsertDocs = 500; ///< InsertMany 单次文档数上限。
inline constexpr std::size_t kMongoLimitsMaxInsertTotalBytes =
    8u * 1024u * 1024u; ///< InsertMany 单次提交的总 BSON 字节上限。
inline constexpr std::size_t kMongoLimitsMaxInputDocBytes =
    kMongoLimitsMaxInsertTotalBytes; ///< 单份输入文档（filter/update/sort/projection）的 BSON 字节上限。
inline constexpr std::size_t kMongoLimitsMaxFindLimit = 1000; ///< Find 单页行数上限。
inline constexpr std::size_t kMongoLimitsMaxFindResultBytes =
    16u * 1024u * 1024u; ///< Find 单次物化返回文档的总字节上限。

/**
 * @brief CoMongoDb 装配参数（owner 级资源预算）。
 *
 * 一个 owner 绑定一组 worker、一条接纳队列与一个生效 URI 的 pool lease；
 * database/collection 不属于 owner，由集合句柄携带。
 */
struct MongoRuntimeConfig {
    std::string uri; ///< mongodb:// 或 mongodb+srv:// 连接串，可能含凭据。
    std::size_t
        worker_threads; ///< 执行阻塞 driver 调用的 worker 线程数上限（<=8；测试默认 <=2）；每项 operation 在同一 worker 上 acquire/use/release pooled client。
    std::size_t
        max_queue; ///< 等待 worker 的 operation 队列容量，耗尽时新命令立即返回 Overloaded，不无限排队；这是同一 owner 全部集合句柄共享的背压上限。
    std::chrono::milliseconds server_selection_timeout; ///< 驱动层服务器选择阻塞上界，语义同 MongoClientConfig。
    std::chrono::milliseconds connect_timeout;          ///< 驱动层连接阻塞上界。
    std::chrono::milliseconds socket_timeout;           ///< 驱动层 socket 阻塞上界。
    std::chrono::milliseconds wait_queue_timeout;       ///< pool 等待队列上界。
};

/**
 * @brief 校验 owner 装配参数：URI 前缀、worker_threads/max_queue 范围与各超时上限。
 * @param cfg 待校验的装配参数。
 * @return 通过返回 ok；违规返回 InvalidArgument。
 */
inline result<void> ValidateMongoRuntimeConfig(const MongoRuntimeConfig& cfg)
{
    if (cfg.uri.empty() || (cfg.uri.rfind("mongodb://", 0) != 0 && cfg.uri.rfind("mongodb+srv://", 0) != 0))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "mongo runtime config: uri must start with mongodb:// or mongodb+srv://"));
    if (cfg.worker_threads == 0 || cfg.worker_threads > kMongoLimitsMaxWorkerThreads)
        return result<void>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo runtime config: worker_threads out of range"));
    if (cfg.max_queue == 0 || cfg.max_queue > kMongoLimitsMaxQueue)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "mongo runtime config: max_queue out of range"));
    const auto valid_timeout = [](std::chrono::milliseconds t) {
        return t > std::chrono::milliseconds{0} && t <= kMongoLimitsMaxTimeout;
    };
    if (!valid_timeout(cfg.server_selection_timeout))
        return result<void>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo runtime config: server_selection_timeout out of range"));
    if (!valid_timeout(cfg.connect_timeout))
        return result<void>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo runtime config: connect_timeout out of range"));
    if (!valid_timeout(cfg.socket_timeout))
        return result<void>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo runtime config: socket_timeout out of range"));
    if (!valid_timeout(cfg.wait_queue_timeout))
        return result<void>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo runtime config: wait_queue_timeout out of range"));
    return result<void>::ok();
}

/**
 * @brief 一次命令的目标集合：owner 不关心 database/collection，句柄只携带目标值。
 *
 * database/collection 语法校验仍在提交时进行，非法输入返回 InvalidArgument。
 */
struct MongoTarget {
    std::string database;   ///< 数据库名，不能为空。
    std::string collection; ///< 集合名，不能为空。
};

/**
 * @brief 校验目标集合：database 与 collection 均不得为空。
 * @param t 待校验的目标。
 * @return 通过返回 ok；空数据库名或空集合名返回 InvalidArgument。
 */
inline result<void> ValidateMongoTarget(const MongoTarget& t)
{
    if (t.database.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "mongo target: empty database"));
    if (t.collection.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "mongo target: empty collection"));
    return result<void>::ok();
}

/**
 * @brief 多文档 Find 的谓词选项。
 *
 * limit **必填**（调用方必须显式给出），杜绝无界游标物化——limit==0 或
 * > kMongoLimitsMaxFindLimit 都在接纳前返回 InvalidArgument；skip 为分页偏移（0 起），
 * > INT64_MAX 无法交给 driver，同样在接纳前 InvalidArgument（不另设分页上限——driver 的
 * int64 表示范围即边界）。sort/projection 为可选 BSON 文档（MongoDocument），bytes 为空
 * 视为未设置；filter/sort/projection 任一字节数超 kMongoLimitsMaxInputDocBytes 也在
 * 接纳前 InvalidArgument。
 *
 * 返回侧另有一道硬上限：命中文档累计 BSON 字节超 kMongoLimitsMaxFindResultBytes 时返回
 * 明确错误（RemoteError，domain_code="result_too_large"）而不是 OOM。
 */
struct FindOptions {
    std::size_t limit = 0;      ///< 必填：每页行数上限。
    std::size_t skip = 0;       ///< 分页偏移。
    MongoDocument sort{};       ///< 可选：BSON sort 文档，bytes 为空视为未设置。
    MongoDocument projection{}; ///< 可选：BSON projection 文档，bytes 为空视为未设置。
};

/**
 * @brief InsertMany 的回执：有序批量插入的服务端结果。
 *
 * 部分成功契约（调用方据此观察）：
 *   - 全部成功 → ok(result)，inserted == docs.size()；
 *   - 有序插入中途失败（如重复 _id）→ err(RemoteError)，此前已成功的文档
 *     **已保留**在服务端；调用方通过错误通道观察部分成功：
 *     Error.details["inserted_count"] 携带服务端回执的已写入数（来自 raw reply 的
 *     nInserted/n，或 ordered 写入时首个 writeErrors.index，三键任一可用时给出；服务端
 *     均未给出时该键缺省），Error.details["requested_count"] 携带请求总数，
 *     Error.details["failed_index"] 携带首个失败下标（服务端给出时）；
 *     domain_code/backend_code 仍承载服务端 codeName/code。
 */
struct MongoInsertManyResult {
    std::uint64_t inserted = 0; ///< 服务端回执的已写入文档数。
};

/**
 * @brief 集合句柄（轻量目标值 + owner 引用）：句柄不拥有 worker/队列/pool。
 *
 * Close() 只封该句柄的接纳与新命令前置校验（幂等、任意线程），不影响同 owner 的兄弟
 * 句柄，也不等待 owner 在途归零。
 *
 * 等待入口：CRUD 用上游 bbt_co_async 标记返回类型，调用方必须经 bbt_co_await 消费
 * （`auto r = bbt_co_await coll->FindOne(...)`）。标记只附加返回类型、不改变实现与语义；
 * 只有写了 await 的表达式才会在非协程域先撞 AwaitHelper 的 std::logic_error（早于本组件的
 * InvalidContext 校验）。漏写 await 直接持有返回值的调用不被静态拦住、函数体照常执行，
 * 故这不是协程外调用的完整静态强制。Create/Start/Collection/Close 不挂起，保持裸
 * result<void>/void。
 *
 * 空 filter 的语义：
 *   - 单条操作仅拒绝 bytes 为空的未初始化 filter；良构的空 BSON `{}` 可匹配首条文档。
 *     UpdateOne / DeleteOne 不会拒绝 `{}`，调用时须自行确保谓词安全；
 *   - UpdateMany / DeleteMany 拒绝 bytes 为空或良构 `{}` 的空 filter，返回 InvalidArgument，
 *     防止一个空谓词误改或误删整个集合。全量更新或删除须写显式匹配条件，
 *     例如 `{"_id":{"$exists":true}}`；
 *   - Find / CountDocuments 的空 filter 表示「全集合」谓词，合法。
 */
class CoMongoColl : public ICoNetwork, public ICoCloseable {
public:
    /**
     * @brief 句柄级命令：语义与 CoMongoCli 同名方法一致，差别只在目标集合由句柄携带
     * 而非 owner 绑定。
     */
    virtual bbt_co_async(result<void>) InsertOne(const MongoDocument& doc, const CallOptions& options) = 0;

    /// @brief 查询首个命中 filter 的文档；仅拒绝 bytes 为空的 filter，合法空 BSON `{}` 可匹配首条文档。
    virtual bbt_co_async(result<std::optional<MongoDocument>>)
        FindOne(const MongoDocument& filter, const CallOptions& options) = 0;

    /// @brief 更新首个命中 filter 的文档，返回 matched/modified/upserted；仅拒绝 bytes 为空的 filter，合法 `{}` 可匹配首条文档。
    virtual bbt_co_async(result<MongoUpdateResult>)
        UpdateOne(const MongoDocument& filter, const MongoDocument& update, const CallOptions& options) = 0;

    /// @brief 删除首个命中 filter 的文档，返回 0 或 1；仅拒绝 bytes 为空的 filter，合法 `{}` 可匹配首条文档。
    virtual bbt_co_async(result<std::uint64_t>) DeleteOne(const MongoDocument& filter, const CallOptions& options) = 0;

    /**
     * @brief 有序批量插入。
     *
     * 空列表、文档数 > kMongoLimitsMaxInsertDocs、总 BSON 字节 >
     * kMongoLimitsMaxInsertTotalBytes、列表内含空/非法文档 → InvalidArgument（零接纳）。
     * 全部成功 → inserted == docs.size()；中途失败 → RemoteError + details 计数
     * （见 MongoInsertManyResult 注释）。
     * @param docs 待插入文档列表。
     * @param options 绝对期限选项。
     */
    virtual bbt_co_async(result<MongoInsertManyResult>)
        InsertMany(const std::vector<MongoDocument>& docs, const CallOptions& options) = 0;

    /**
     * @brief 多文档读取。
     *
     * filter 可为空（匹配全部）；options.limit 必填且 ≤ kMongoLimitsMaxFindLimit，
     * options.skip ≤ INT64_MAX，filter/sort/projection 各自 BSON ≤ kMongoLimitsMaxInputDocBytes，
     * 否则 InvalidArgument（零接纳）。返回按游标顺序物化的命中页；无匹配返回空 vector
     * （不是 NotFound）。返回总字节超 kMongoLimitsMaxFindResultBytes →
     * RemoteError(domain_code="result_too_large")。
     * @param filter 查询谓词文档，可为空。
     * @param find_options 多文档选项，见 FindOptions。
     * @param options 绝对期限选项。
     */
    virtual bbt_co_async(result<std::vector<MongoDocument>>)
        Find(const MongoDocument& filter, const FindOptions& find_options, const CallOptions& options) = 0;

    /**
     * @brief 批量更新：复用 UpdateOne 的 update 文档校验。
     *
     * 空 filter 一律 InvalidArgument（语义边界见类注释）；filter/update 任一 BSON 超
     * kMongoLimitsMaxInputDocBytes 同样 InvalidArgument（零接纳）。返回 matched/modified/upserted。
     */
    virtual bbt_co_async(result<MongoUpdateResult>)
        UpdateMany(const MongoDocument& filter, const MongoDocument& update, const CallOptions& options) = 0;

    /**
     * @brief 批量删除：空 filter 一律 InvalidArgument；filter BSON 超
     * kMongoLimitsMaxInputDocBytes 同样 InvalidArgument。返回删除数量。
     */
    virtual bbt_co_async(result<std::uint64_t>) DeleteMany(const MongoDocument& filter, const CallOptions& options) = 0;

    /// @brief 精确计数：filter 可为空（统计全集合）；filter BSON 超上限为 InvalidArgument。
    virtual bbt_co_async(result<std::uint64_t>)
        CountDocuments(const MongoDocument& filter, const CallOptions& options) = 0;
};

/**
 * @brief mongo 资源 owner（连接配置 + pool lease + worker 组 + 接纳队列 + 关闭排空）。
 *
 * Create/Start 在启动控制线程使用，不挂起协程；Start 要求协程运行时已初始化。
 *
 * 关闭语义：Close() 由 owner 主动同步发起（幂等、任意线程可调用）——封口停止接纳、
 * 唤醒全部挂起业务等待者（未派发 op 立即落定 Closed，已进入 driver 的调用继续到自身
 * 超时上界，迟到结果只消费不交付），然后在有界窗口内等 in-flight 归零；ops 清空且
 * worker 全退才 IsClosed()==true，不提前宣告。句柄关闭（CoMongoColl::Close）是接纳门禁，
 * 只停该句柄的新命令，不动兄弟句柄与 owner。
 *
 * 命令语义边界（与 CoMongoCli 相同）：
 *   - 全部命令只能在协程上下文调用，否则返回 Error(InvalidContext)；
 *   - 服务端错误回复映射为 Error(RemoteError)；driver 侧失败映射为
 *     Error(Unavailable)；无效 BSON/参数为 InvalidArgument；pool wait
 *     queue 超时为 Overloaded；
 *   - deadline/owner close 竞争只发布一次逻辑终态；已进入 driver 的
 *     同步调用不可强杀，op/lease/payload 保活到物理收口。
 */
class CoMongoDb : public ICoNetwork, public ICoCloseable {
public:
    /**
     * @brief 校验 owner 装配参数并取对象身份，不建立资源；资源由 Start 建立。
     * @param config owner 装配参数，校验规则见 ValidateMongoRuntimeConfig。
     * @return 成功返回 owner 句柄；参数非法返回 InvalidArgument。
     */
    static result<std::shared_ptr<CoMongoDb>> Create(MongoRuntimeConfig config);

    /**
     * @brief 同一 owner 只成功启动一次；Start 要求协程运行时已初始化，
     * 建立 worker 线程组并接入进程级 pool；关闭后不重开。
     * @return 协程运行时未初始化或 pool 获取失败返回 RuntimeUnavailable；已启动返回
     *         InvalidArgument；关闭中或已关闭返回 Closed。
     */
    virtual result<void> Start() = 0;

    /**
     * @brief 创建集合句柄：owner 必须 Running；target.database/.collection
     * 空则 InvalidArgument。
     *
     * 句柄共享 owner 的 worker/队列/pool，不新建线程。owner 关闭后不再接纳新句柄。
     * @param target 目标 database 与 collection。
     */
    virtual result<std::shared_ptr<CoMongoColl>> Collection(MongoTarget target) = 0;
};

} // namespace bbt::infra::mongo
