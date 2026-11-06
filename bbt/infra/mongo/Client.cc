#include <bbt/infra/mongo/Client.hpp>

namespace bbt::infra::mongo {

result<void> ValidateMongoRuntimeConfig(const MongoRuntimeConfig& cfg)
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

result<void> ValidateMongoTarget(const MongoTarget& t)
{
    if (t.database.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "mongo target: empty database"));
    if (t.collection.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "mongo target: empty collection"));
    return result<void>::ok();
}

} // namespace bbt::infra::mongo
