#include "mongo/MongoProcess.hpp"

#include "mongo/MongoDetail.hpp"

namespace bbt::infra::mongo_detail {

namespace {

// 进程级注册表：instance 强引用 + URI → pool lease 弱引用表。
// 静态生存期问题经 lease 内 instance 强引用化解——即便注册表先析构，
// 存活 lease 也保证 instance 晚于 pool 销毁。
struct MongoProcessState {
    std::mutex mtx;
    std::shared_ptr<mongocxx::v1::instance> instance;
    std::unordered_map<std::string, std::weak_ptr<MongoPoolLease>> pools;
};

MongoProcessState& ProcessState() {
    static MongoProcessState state;
    return state;
}

} // namespace

result<std::shared_ptr<MongoPoolLease>> AcquireMongoPool(
    const std::string& effective_uri) {
    auto& state = ProcessState();
    std::lock_guard<std::mutex> lk(state.mtx);

    // 顺手回收已失效槽位，distinct URI 计数只算存活 lease。
    for (auto it = state.pools.begin(); it != state.pools.end();) {
        if (it->second.expired())
            it = state.pools.erase(it);
        else
            ++it;
    }
    const auto found = state.pools.find(effective_uri);
    if (found != state.pools.end()) {
        if (auto lease = found->second.lock())
            return result<std::shared_ptr<MongoPoolLease>>::ok(
                std::move(lease));
        state.pools.erase(found);
    }
    if (state.pools.size() >= kMongoMaxPools)
        return result<std::shared_ptr<MongoPoolLease>>::err(MakeError(
            ErrorCode::InvalidArgument,
            "mongo: distinct effective uri count exceeds process pool limit"));

    try {
        if (!state.instance)
            state.instance = std::make_shared<mongocxx::v1::instance>();
        mongocxx::v1::uri uri{bsoncxx::v1::stdx::string_view{effective_uri}};
        auto lease = std::make_shared<MongoPoolLease>(state.instance,
                                                      std::move(uri));
        state.pools.emplace(effective_uri, lease);
        return result<std::shared_ptr<MongoPoolLease>>::ok(std::move(lease));
    } catch (const mongocxx::v1::exception& e) {
        return result<std::shared_ptr<MongoPoolLease>>::err(
            ClassifyDriverError(e, "mongo: init driver pool"));
    } catch (const std::exception& e) {
        return result<std::shared_ptr<MongoPoolLease>>::err(MakeError(
            ErrorCode::InternalError,
            std::string("mongo: init driver pool: ") + e.what()));
    }
}

} // namespace bbt::infra::mongo_detail
