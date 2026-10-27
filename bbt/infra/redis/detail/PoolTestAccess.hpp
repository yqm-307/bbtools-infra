#pragma once
#include <cstddef>
#include <functional>
#include <bbt/infra/redis/CoRedisPool.hpp>

namespace bbt::infra::redis {
// 内部阶段探针；不属于公开消费接口。hook 不持池锁，设置须早于任务提交。
struct PoolTestAccess {
    static bbt_co_async(result<CoRedisPool::Borrow>) Take(CoRedisPool& pool, const CallOptions& options);
    static std::size_t Queued(const CoRedisPool& pool);
    // 0: 登记前；1: 登记与入队后；2: 唤醒后、出账前。
    static void SetWaitHook(CoRedisPool& pool, std::function<void(int)> hook);
};
} // namespace bbt::infra::redis
