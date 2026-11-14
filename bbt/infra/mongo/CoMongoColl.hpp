/**
 * @brief Mongo 集合句柄接口；共享 owner，不拥有 worker。
 */
#pragma once

#include <bbt/infra/mongo/MongoTypes.hpp>

namespace bbt::infra::mongo {

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

    /**
     * @brief 查询首个命中 filter 的文档；仅拒绝 bytes 为空的 filter，合法空 BSON `{}` 可匹配首条文档。
     */
    virtual bbt_co_async(result<std::optional<MongoDocument>>)
        FindOne(const MongoDocument& filter, const CallOptions& options) = 0;

    /**
     * @brief 更新首个命中 filter 的文档，返回 matched/modified/upserted；仅拒绝 bytes 为空的 filter，合法 `{}` 可匹配首条文档。
     */
    virtual bbt_co_async(result<MongoUpdateResult>)
        UpdateOne(const MongoDocument& filter, const MongoDocument& update, const CallOptions& options) = 0;

    /**
     * @brief 删除首个命中 filter 的文档，返回 0 或 1；仅拒绝 bytes 为空的 filter，合法 `{}` 可匹配首条文档。
     */
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

    /**
     * @brief 精确计数：filter 可为空（统计全集合）；filter BSON 超上限为 InvalidArgument。
     */
    virtual bbt_co_async(result<std::uint64_t>)
        CountDocuments(const MongoDocument& filter, const CallOptions& options) = 0;
};

// clang-format off
}
// clang-format on
