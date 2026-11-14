#include <bbt/infra/mongo/detail/CoMongoCliImpl.hpp>
#include <bbt/infra/mongo/detail/MongoDetail.hpp>
#include <bbt/infra/mongo/detail/MongoSupport.hpp>
#include <bbt/infra/detail/CloseWait.hpp>

namespace bbt::infra::mongo_detail {

/**
 * MongoCollImpl
 */
namespace {

/**
 * 句柄命令公共路径：句柄接纳态校验（封口后不再收新命令）→ 目标集合由
 * 句柄携带进 op → runtime 提交。句柄关闭不取消在途 op：op 保活 handle 与
 * runtime，物理收口照常。
 */
result<MongoOpOutcome> CollSubmit(
    const std::shared_ptr<MongoCollImpl>& self,
    MongoOp::Kind kind,
    MongoDocument doc,
    MongoDocument update,
    const CallOptions& options,
    std::vector<MongoDocument> docs = {},
    std::size_t find_limit = 0,
    std::size_t find_skip = 0,
    MongoDocument find_sort = {},
    MongoDocument find_projection = {})
{
    if (!self->Accepting())
        return result<MongoOpOutcome>::err(MakeError(ErrorCode::Closed, "mongo collection handle is closed"));
    return self->Runtime()->Submit(
        self,
        kind,
        std::move(doc),
        std::move(update),
        options,
        std::move(docs),
        find_limit,
        find_skip,
        std::move(find_sort),
        std::move(find_projection));
}

// clang-format off
}
// clang-format on

bbt_co_async(result<void>) MongoCollImpl::InsertOne(const MongoDocument& doc, const CallOptions& options)
{
    if (doc.bytes.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "mongo InsertOne: empty document"));
    auto r = CollSubmit(shared_from_this(), MongoOp::Kind::InsertOne, doc, {}, options);
    if (!r)
        return result<void>::err(std::move(r).error());
    return result<void>::ok();
}

bbt_co_async(result<std::optional<MongoDocument>>)
    MongoCollImpl::FindOne(const MongoDocument& filter, const CallOptions& options)
{
    if (filter.bytes.empty())
        return result<std::optional<MongoDocument>>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo FindOne: empty filter"));
    auto r = CollSubmit(shared_from_this(), MongoOp::Kind::FindOne, filter, {}, options);
    if (!r)
        return result<std::optional<MongoDocument>>::err(std::move(r).error());
    return result<std::optional<MongoDocument>>::ok(std::move(r).value().doc);
}

bbt_co_async(result<MongoUpdateResult>)
    MongoCollImpl::UpdateOne(const MongoDocument& filter, const MongoDocument& update, const CallOptions& options)
{
    if (filter.bytes.empty())
        return result<MongoUpdateResult>::err(MakeError(ErrorCode::InvalidArgument, "mongo UpdateOne: empty filter"));
    if (update.bytes.empty())
        return result<MongoUpdateResult>::err(MakeError(ErrorCode::InvalidArgument, "mongo UpdateOne: empty update"));
    auto r = CollSubmit(shared_from_this(), MongoOp::Kind::UpdateOne, filter, update, options);
    if (!r)
        return result<MongoUpdateResult>::err(std::move(r).error());
    MongoUpdateResult out;
    out.matched = r.value().matched;
    out.modified = r.value().modified;
    out.upserted = r.value().upserted;
    return result<MongoUpdateResult>::ok(out);
}

bbt_co_async(result<std::uint64_t>) MongoCollImpl::DeleteOne(const MongoDocument& filter, const CallOptions& options)
{
    if (filter.bytes.empty())
        return result<std::uint64_t>::err(MakeError(ErrorCode::InvalidArgument, "mongo DeleteOne: empty filter"));
    auto r = CollSubmit(shared_from_this(), MongoOp::Kind::DeleteOne, filter, {}, options);
    if (!r)
        return result<std::uint64_t>::err(std::move(r).error());
    return result<std::uint64_t>::ok(static_cast<std::uint64_t>(r.value().deleted < 0 ? 0 : r.value().deleted));
}

/**
 * 多文档数据面
 */
bbt_co_async(result<mongo::MongoInsertManyResult>)
    MongoCollImpl::InsertMany(const std::vector<MongoDocument>& docs, const CallOptions& options)
{

    /**
     * 接纳前边界（零接纳）：空列表 / 文档数 / 总字节 / 单文档空值。
     */
    if (docs.empty())
        return result<mongo::MongoInsertManyResult>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo InsertMany: empty docs"));
    if (docs.size() > mongo::kMongoLimitsMaxInsertDocs)
        return result<mongo::MongoInsertManyResult>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo InsertMany: docs count exceeds limit"));
    std::size_t total = 0;
    for (const auto& d : docs) {
        if (d.bytes.empty())
            return result<mongo::MongoInsertManyResult>::err(
                MakeError(ErrorCode::InvalidArgument, "mongo InsertMany: empty document in docs"));

        /**
         * checked-add：减法预算同时覆盖「单文档越界」与「累计溢出回绕」。
         */
        if (d.bytes.size() > mongo::kMongoLimitsMaxInsertTotalBytes - total)
            return result<mongo::MongoInsertManyResult>::err(
                MakeError(ErrorCode::InvalidArgument, "mongo InsertMany: total bytes exceeds limit"));
        total += d.bytes.size();
    }
    auto r = CollSubmit(shared_from_this(), MongoOp::Kind::InsertMany, {}, {}, options, docs);
    if (!r) {
        Error e = std::move(r).error();

        /**
         * 部分成功的请求侧事实：requested_count 服务端回执里没有，
         * 由本层补齐；inserted_count/failed_index 来自服务端 raw reply。
         */
        e.details.emplace_back("requested_count", std::to_string(docs.size()));
        return result<mongo::MongoInsertManyResult>::err(std::move(e));
    }
    mongo::MongoInsertManyResult out;
    out.inserted = static_cast<std::uint64_t>(r.value().inserted < 0 ? 0 : r.value().inserted);
    return result<mongo::MongoInsertManyResult>::ok(out);
}

bbt_co_async(result<std::vector<MongoDocument>>)
    MongoCollImpl::Find(const MongoDocument& filter, const mongo::FindOptions& find_options, const CallOptions& options)
{

    /**
     * limit 必填且有上界：无界游标物化在本层拒绝（零接纳）。
     */
    if (find_options.limit == 0)
        return result<std::vector<MongoDocument>>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo Find: limit is required"));
    if (find_options.limit > mongo::kMongoLimitsMaxFindLimit)
        return result<std::vector<MongoDocument>>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo Find: limit exceeds max find limit"));

    /**
     * skip 的 driver 表示是 int64：>INT64_MAX 的值无法表达，须在接纳前
     * 拒绝，不做实现相关的有符号收缩转换。
     */
    if (find_options.skip > static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max()))
        return result<std::vector<MongoDocument>>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo Find: skip exceeds int64 range"));

    /**
     * BSON 输入预算（CollSubmit 之前，零接纳）：filter/sort/projection 各
     * 自不超 kMongoLimitsMaxInputDocBytes。
     */
    if (filter.bytes.size() > mongo::kMongoLimitsMaxInputDocBytes
        || find_options.sort.bytes.size() > mongo::kMongoLimitsMaxInputDocBytes
        || find_options.projection.bytes.size() > mongo::kMongoLimitsMaxInputDocBytes)
        return result<std::vector<MongoDocument>>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo Find: input BSON exceeds limit"));

    /**
     * sort/projection 的良构校验在 worker 侧 ViewOf 统一归类
     * InvalidArgument；filter 空（未初始化或 {}）= 匹配全部。
     */
    auto r = CollSubmit(
        shared_from_this(),
        MongoOp::Kind::Find,
        filter,
        {},
        options,
        {},
        find_options.limit,
        find_options.skip,
        find_options.sort,
        find_options.projection);
    if (!r)
        return result<std::vector<MongoDocument>>::err(std::move(r).error());
    return result<std::vector<MongoDocument>>::ok(std::move(r).value().docs);
}

bbt_co_async(result<MongoUpdateResult>)
    MongoCollImpl::UpdateMany(const MongoDocument& filter, const MongoDocument& update, const CallOptions& options)
{

    /**
     * 空 filter（未初始化或 {}）一律拒绝：防止空谓词误改全集合；
     * 要全量更新须写显式匹配条件。
     */
    if (IsEmptyBson(filter))
        return result<MongoUpdateResult>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo UpdateMany: empty filter is not allowed"));
    if (update.bytes.empty())
        return result<MongoUpdateResult>::err(MakeError(ErrorCode::InvalidArgument, "mongo UpdateMany: empty update"));
    if (filter.bytes.size() > mongo::kMongoLimitsMaxInputDocBytes
        || update.bytes.size() > mongo::kMongoLimitsMaxInputDocBytes)
        return result<MongoUpdateResult>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo UpdateMany: input BSON exceeds limit"));
    auto r = CollSubmit(shared_from_this(), MongoOp::Kind::UpdateMany, filter, update, options);
    if (!r)
        return result<MongoUpdateResult>::err(std::move(r).error());
    MongoUpdateResult out;
    out.matched = r.value().matched;
    out.modified = r.value().modified;
    out.upserted = r.value().upserted;
    return result<MongoUpdateResult>::ok(out);
}

bbt_co_async(result<std::uint64_t>) MongoCollImpl::DeleteMany(const MongoDocument& filter, const CallOptions& options)
{
    if (IsEmptyBson(filter))
        return result<std::uint64_t>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo DeleteMany: empty filter is not allowed"));
    if (filter.bytes.size() > mongo::kMongoLimitsMaxInputDocBytes)
        return result<std::uint64_t>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo DeleteMany: input BSON exceeds limit"));
    auto r = CollSubmit(shared_from_this(), MongoOp::Kind::DeleteMany, filter, {}, options);
    if (!r)
        return result<std::uint64_t>::err(std::move(r).error());
    return result<std::uint64_t>::ok(static_cast<std::uint64_t>(r.value().deleted < 0 ? 0 : r.value().deleted));
}

bbt_co_async(result<std::uint64_t>)
    MongoCollImpl::CountDocuments(const MongoDocument& filter, const CallOptions& options)
{

    /**
     * filter 空（未初始化或 {}）= 统计全集合，合法谓词。
     */
    if (filter.bytes.size() > mongo::kMongoLimitsMaxInputDocBytes)
        return result<std::uint64_t>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo CountDocuments: input BSON exceeds limit"));
    auto r = CollSubmit(shared_from_this(), MongoOp::Kind::CountDocuments, filter, {}, options);
    if (!r)
        return result<std::uint64_t>::err(std::move(r).error());
    return result<std::uint64_t>::ok(static_cast<std::uint64_t>(r.value().counted < 0 ? 0 : r.value().counted));
}

MongoCollImpl::MongoCollImpl(
    std::shared_ptr<MongoRuntime> runtime, mongo::MongoTarget target, bbt::coroutine::CoObjectInfo info)
    : m_runtime(std::move(runtime))
    , m_target(std::move(target))
    , m_info(std::move(info))
{
}

MongoCollImpl::~MongoCollImpl()
{
    (void)Close();
}

void MongoCollImpl::Close() noexcept
{
    m_close.BeginClose();
    m_close.MarkClosed();
}

bbt::coroutine::CoObjectInfo MongoCollImpl::GetObjectInfo() const
{
    return m_info;
}

// clang-format off
}
// clang-format on
