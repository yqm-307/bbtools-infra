/**
 * Mongo owner 的集合接纳门禁与 runtime 委托。
 */
#pragma once

#include <bbt/infra/mongo/detail/MongoRuntime.hpp>

namespace bbt::infra::mongo_detail {
class CoMongoDbImpl final : public mongo::CoMongoDb {
public:
    CoMongoDbImpl(std::shared_ptr<MongoRuntime> runtime, bbt::coroutine::CoObjectInfo info);
    ~CoMongoDbImpl() override;
    result<void> Start() override;
    result<std::shared_ptr<mongo::CoMongoColl>> Collection(mongo::MongoTarget target) override;
    result<void> Close(const CloseOptions& options) override;
    std::optional<Error> CloseReason() const noexcept override;
    bool IsClosed() const noexcept override;
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override;
    std::shared_ptr<MongoRuntime> Runtime() const;

private:
    std::shared_ptr<MongoRuntime> m_runtime;
    bbt::coroutine::CoObjectInfo m_info;
    ManagedCloseState m_close;
};

// clang-format off
}
// clang-format on
