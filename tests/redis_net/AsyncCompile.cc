#include <bbt/infra/redis/CoRedisCli.hpp>
#include <bbt/infra/redis/CoRedisPool.hpp>

using namespace bbt::infra;
void Consume(redis::CoRedisCli& client, redis::CoRedisPool& pool, const CallOptions& options) {
#ifdef BBT_NEGATIVE_ASYNC
    auto pending = pool.Ping(options);
    pending.value(); // async 包装不能直接当 result 消费。
#else
    auto one = bbt_co_await client.Ping(options);
    auto many = bbt_co_await pool.Ping(options);
    (void)one; (void)many;
#endif
}
