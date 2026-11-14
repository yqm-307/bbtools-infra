/**
 * Mongo 集合命令提交与句柄接纳。
 */
#pragma once

#include <bbt/infra/mongo/detail/MongoRuntime.hpp>

namespace bbt::infra::mongo_detail {

/**
 * MongoCollImpl：集合句柄——携带 db/collection 目标值与 owner 共享
 * 指针，不持有 worker/队列/pool。句柄关闭只影响自身接纳与新命令
 * 前置校验；owner 的物理收口与在途 op 保活由 runtime 负责。
 */
class MongoCollImpl : public mongo::CoMongoColl, public std::enable_shared_from_this<MongoCollImpl> {
public:
    MongoCollImpl(std::shared_ptr<MongoRuntime> runtime, mongo::MongoTarget target, bbt::coroutine::CoObjectInfo info);

    /**
     * 不拥有 worker/ops；析构前只做句柄级接纳封口（幂等）。
     */
    ~MongoCollImpl() override;

    bbt_co_async(result<void>) InsertOne(const MongoDocument& doc, const CallOptions& options) override;
    bbt_co_async(result<std::optional<MongoDocument>>)
        FindOne(const MongoDocument& filter, const CallOptions& options) override;
    bbt_co_async(result<MongoUpdateResult>)
        UpdateOne(const MongoDocument& filter, const MongoDocument& update, const CallOptions& options) override;
    bbt_co_async(result<std::uint64_t>) DeleteOne(const MongoDocument& filter, const CallOptions& options) override;

    bbt_co_async(result<mongo::MongoInsertManyResult>)
        InsertMany(const std::vector<MongoDocument>& docs, const CallOptions& options) override;
    bbt_co_async(result<std::vector<MongoDocument>>)
        Find(const MongoDocument& filter, const mongo::FindOptions& find_options, const CallOptions& options) override;
    bbt_co_async(result<MongoUpdateResult>)
        UpdateMany(const MongoDocument& filter, const MongoDocument& update, const CallOptions& options) override;
    bbt_co_async(result<std::uint64_t>) DeleteMany(const MongoDocument& filter, const CallOptions& options) override;
    bbt_co_async(result<std::uint64_t>)
        CountDocuments(const MongoDocument& filter, const CallOptions& options) override;

    /**
     * 句柄关闭只做接纳门禁：封口后新命令前置校验返回 Closed；已提交的
     * 在途 op 由 op->handle 保活，物理收口归 runtime，句柄不等 owner drain。
     */
    void Close() noexcept override;

    bool IsClosed() const noexcept override { return m_close.IsClosed(); }

    bbt::coroutine::CoObjectInfo GetObjectInfo() const override;

    bool Accepting() const noexcept { return m_close.IsOpen(); }

    const mongo::MongoTarget& Target() const { return m_target; }

    std::shared_ptr<MongoRuntime> Runtime() const { return m_runtime; }

private:
    std::shared_ptr<MongoRuntime> m_runtime;
    mongo::MongoTarget m_target;
    bbt::coroutine::CoObjectInfo m_info;
    ManagedCloseState m_close;
};

// clang-format off
}
// clang-format on
