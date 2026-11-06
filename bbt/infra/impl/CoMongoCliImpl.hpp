#pragma once
// 父头末尾包含的实现；依赖父头已完成类型与声明。

namespace bbt::infra {

inline result<void> ValidateMongoClientConfig(const MongoClientConfig& cfg)
{
    if (cfg.uri.empty() || (cfg.uri.rfind("mongodb://", 0) != 0 && cfg.uri.rfind("mongodb+srv://", 0) != 0))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
                                           "mongo client config: uri must start with mongodb:// or mongodb+srv://"));
    if (cfg.database.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "mongo client config: empty database"));
    if (cfg.collection.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "mongo client config: empty collection"));
    if (cfg.worker_threads == 0 || cfg.worker_threads > kMongoLimitsMaxWorkerThreads)
        return result<void>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo client config: worker_threads out of range"));
    if (cfg.max_queue == 0 || cfg.max_queue > kMongoLimitsMaxQueue)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "mongo client config: max_queue out of range"));
    const auto valid_timeout = [](std::chrono::milliseconds t) {
        return t > std::chrono::milliseconds{0} && t <= kMongoLimitsMaxTimeout;
    };
    if (!valid_timeout(cfg.server_selection_timeout))
        return result<void>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo client config: server_selection_timeout out of range"));
    if (!valid_timeout(cfg.connect_timeout))
        return result<void>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo client config: connect_timeout out of range"));
    if (!valid_timeout(cfg.socket_timeout))
        return result<void>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo client config: socket_timeout out of range"));
    if (!valid_timeout(cfg.wait_queue_timeout))
        return result<void>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo client config: wait_queue_timeout out of range"));
    return result<void>::ok();
}

} // namespace bbt::infra
