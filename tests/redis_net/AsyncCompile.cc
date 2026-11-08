#include <bbt/infra/redis/CoRedisCli.hpp>
#include <bbt/infra/redis/CoRedisPool.hpp>
#include <bbt/infra/redis/interface/IRedisCli.hpp>

#include <type_traits>

using namespace bbt::infra;
static_assert(std::is_abstract_v<redis::IRedisCli>);
static_assert(std::has_virtual_destructor_v<redis::IRedisCli>);
static_assert(std::is_convertible_v<redis::CoRedisCli*, redis::IRedisCli*>);
static_assert(std::is_convertible_v<redis::CoRedisPool*, redis::IRedisCli*>);
static_assert(!std::is_abstract_v<redis::CoRedisCli>);
static_assert(!std::is_abstract_v<redis::CoRedisPool>);
void Consume(redis::CoRedisCli& client, redis::CoRedisPool& pool, const CallOptions& options)
{
#ifdef BBT_NEGATIVE_ASYNC
    redis::IRedisCli& common = pool;
    auto pending = common.Ping(options);
    pending.value(); // async 包装不能直接当 result 消费。
#else
    auto one = bbt_co_await client.Ping(options);
    auto many = bbt_co_await pool.Ping(options);
    redis::IRedisCli& one_interface = client;
    redis::IRedisCli& pool_interface = pool;
    auto abstract_one = bbt_co_await one_interface.Ping(options);
    auto abstract_many = bbt_co_await pool_interface.Ping(options);
    (void)one;
    (void)many;
    (void)abstract_one;
    (void)abstract_many;
#endif
}
