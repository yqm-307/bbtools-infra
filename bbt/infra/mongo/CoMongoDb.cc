#include <bbt/infra/mongo/CoMongoDb.hpp>
#include <bbt/infra/mongo/detail/CoMongoDbImpl.hpp>

namespace bbt::infra {
namespace mongo {

/**
 * owner 装配入口：与 CoMongoCli::Create 同纪律——Create 校验+身份，
 * Start 占用资源。
 */
result<std::shared_ptr<CoMongoDb>> CoMongoDb::Create(MongoRuntimeConfig config)
{
    auto valid = ValidateMongoRuntimeConfig(config);
    if (!valid)
        return result<std::shared_ptr<CoMongoDb>>::err(std::move(valid).error());
    auto info = mongo_detail::NewObjectInfo("infra.mongo_db");
    if (!info)
        return result<std::shared_ptr<CoMongoDb>>::err(std::move(info).error());
    auto rt_info = mongo_detail::NewObjectInfo("infra.mongo_rt");
    if (!rt_info)
        return result<std::shared_ptr<CoMongoDb>>::err(std::move(rt_info).error());
    auto rt = mongo_detail::MongoRuntime::Create(std::move(config), std::move(rt_info).value());
    auto impl = std::make_shared<mongo_detail::CoMongoDbImpl>(std::move(rt), std::move(info).value());
    return result<std::shared_ptr<CoMongoDb>>::ok(std::move(impl));
}

// clang-format off
}
// clang-format on
// clang-format off
}
// clang-format on
