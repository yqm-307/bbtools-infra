/**
 * @file IRedisCli.hpp
 * @brief 单连接与连接池共享的 Redis 消费接口；不暴露具体连接或池的装配方式。
 */
#pragma once
#include <bbt/infra/CloseOptions.hpp>

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/redis/RedisTypes.hpp>

namespace bbt::infra::redis {

/**
 * @brief Redis 命令与显式建连、逻辑关闭的公共契约。
 *
 * 等待入口要求协程域并经 bbt_co_await 消费，保留 result、deadline、
 * RemoteError 与 OutcomeUnknown 语义，不隐式重放命令。
 * 并发额度由实现决定：单连接竞争立即拒绝，池可按配置有界等待。
 * Close 不关闭共享 Engine，也不证明在途任务或物理连接已排空。
 * 装配、握手参数、池容量和未来集群拓扑不属于接口。
 */
class IRedisCli {
public:
    /**
     * @brief 支持多态销毁；具体实现仍负责逻辑 Close，不增加物理排空等待。
     */
    virtual ~IRedisCli() noexcept = default;

    /**
     * @brief 显式建立或修复实现拥有的连接；已就绪时幂等，关闭后不可重开。
     */
    virtual bbt_co_async(result<void>) Connect(const CallOptions& options) = 0;

    /**
     * @brief PING，期望 +PONG；其余回复形态按 ProtocolError 处理。
     */
    virtual bbt_co_async(result<void>) Ping(const CallOptions& options) = 0;

    /**
     * @brief GET：键缺失返回 ok(nullopt)。
     */
    virtual bbt_co_async(result<std::optional<std::string>>) Get(std::string_view key, const CallOptions& options) = 0;

    /**
     * @brief 无条件 SET：写入成功返回 ok。
     */
    virtual bbt_co_async(result<void>)
        Set(std::string_view key, std::string_view value, const CallOptions& options) = 0;

    /**
     * @brief 带选项的 SET：写入成功 ok(true)；NX/XX 条件不满足（nil 回复）
     * ok(false)，不是错误。
     *
     * options.expire 越界在接纳前返回 InvalidArgument（零接纳，什么都不发）。空 key 同样 InvalidArgument。
     */
    virtual bbt_co_async(result<bool>) Set(
        std::string_view key, std::string_view value, const SetOptions& set_options, const CallOptions& options) = 0;

    /**
     * @brief EXISTS：键存在为 true。
     */
    virtual bbt_co_async(result<bool>) Exists(std::string_view key, const CallOptions& options) = 0;

    /**
     * @brief DEL：返回服务端报告的实际删除数量。
     */
    virtual bbt_co_async(result<std::uint64_t>) Delete(std::vector<std::string> keys, const CallOptions& options) = 0;

    /**
     * @brief PEXPIRE：ok(true) 已设置过期；key 不存在 ok(false)。
     *
     * ttl 必须 >0，否则 InvalidArgument 零接纳。
     */
    virtual bbt_co_async(result<bool>)
        Expire(std::string_view key, std::chrono::milliseconds ttl, const CallOptions& options) = 0;

    /**
     * @brief PTTL：KeyMissing / NoExpiry / Expires 三态；不存在 -1/-2 魔数。
     */
    virtual bbt_co_async(result<TtlResult>) Ttl(std::string_view key, const CallOptions& options) = 0;

    /**
     * @brief INCR/DECR/INCRBY/DECRBY：返回服务端整数值。
     *
     * 非整数、溢出由服务端报错映射为 RemoteError（唯一保留连接的错误），连接保持同步。
     */
    virtual bbt_co_async(result<std::int64_t>) Incr(std::string_view key, const CallOptions& options) = 0;

    /**
     * @brief DECR 一步；错误语义同 Incr。
     */
    virtual bbt_co_async(result<std::int64_t>) Decr(std::string_view key, const CallOptions& options) = 0;

    /**
     * @brief INCRBY 指定增量；错误语义同 Incr。
     */
    virtual bbt_co_async(result<std::int64_t>)
        IncrBy(std::string_view key, std::int64_t delta, const CallOptions& options) = 0;

    /**
     * @brief DECRBY 指定减量；错误语义同 Incr。
     */
    virtual bbt_co_async(result<std::int64_t>)
        DecrBy(std::string_view key, std::int64_t delta, const CallOptions& options) = 0;

    /**
     * @brief Hash 命令族：key/field/value 二进制安全；空 key、空 field、HDel 空 fields
     * 一律 InvalidArgument 零接纳。
     *
     * HSet 的 ok(true) 是新建字段，ok(false) 是覆盖已有字段；HGet 在字段缺失或 key 不存在时
     * 返回 ok(nullopt)；HDel 返回删除数量；HExists 字段存在为 true。
     */
    virtual bbt_co_async(result<bool>)
        HSet(std::string_view key, std::string_view field, std::string_view value, const CallOptions& options) = 0;

    /**
     * @brief HGET：字段缺失或 key 不存在为 ok(nullopt)。
     */
    virtual bbt_co_async(result<std::optional<std::string>>)
        HGet(std::string_view key, std::string_view field, const CallOptions& options) = 0;

    /**
     * @brief HDEL：返回删除的字段数量。
     */
    virtual bbt_co_async(result<std::uint64_t>)
        HDel(std::string_view key, std::vector<std::string> fields, const CallOptions& options) = 0;

    /**
     * @brief HEXISTS：字段存在为 true。
     */
    virtual bbt_co_async(result<bool>)
        HExists(std::string_view key, std::string_view field, const CallOptions& options) = 0;

    /**
     * @brief HGETALL：field/value 对按服务端顺序。
     *
     * 嵌套数组、奇数元素、nil 元素、元素或字节超 detail::kMaxArrayElements/kMaxArrayBytes
     * 时退役连接，命令为 OutcomeUnknown（cause=ProtocolError）。
     */
    virtual bbt_co_async(result<HashEntries>) HGetAll(std::string_view key, const CallOptions& options) = 0;

    /**
     * @brief 立即逻辑封口并请求关闭已发布连接；不等 fd、在途拨号或 API 任务退出。
     *
     * 宿主最终须 Close 全部 client、Close(Immediate) Engine、join Run 且核 Stopped、
     * 等登记的全部 API 任务完成并释放捕获。不可当作 ICoCloseable 的同步资源使用。
     */
    virtual result<void> Close(const CloseOptions& options = {}) = 0;

    /**
     * @brief 只读查询逻辑封口：Close() 已生效为 true；不代表连接已物理回收。
     */
    virtual bool IsClosed() const noexcept = 0;
};

// clang-format off
}
// clang-format on
