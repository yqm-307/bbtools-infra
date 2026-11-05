/** @file CoRedisCli.hpp
 * @brief 单连接 Redis 适配：协程域内的命令等待、连接级握手配置与关闭语义。
 *
 * 只经 net::Engine 出网，不引入 Redis SDK、不自建 Engine 线程。服务端回复的边界形态
 * （nil 回复、PTTL 的 -1/-2、条件写失败）在公开面上各有明确表达，不透出协议魔数。
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

namespace bbt::infra::redis {

/**
 * @brief 连接级握手配置（AUTH/SELECT）：每次 Connect（含故障后显式重连）在
 * 已接纳操作内按 AUTH 再 SELECT 顺序各等 +OK，不流水线、不缓存终态。
 *
 * 默认构造为不握手，行为与未配置时完全一致。密码只在编码命令内短暂存在，
 * 不写入任何错误消息、日志或 details。
 */
struct RedisHandshake {
    std::string username;       ///< 可选；为空时 AUTH 走单参数 legacy 形式。
    std::string password;       ///< 可选；为空表示不发送 AUTH。
    std::int64_t database = -1; ///< <0 表示不 SELECT；合法值 0..kMaxDatabase。
};

/// @brief Redis SELECT 的库序号实际上限（int64 正数，与服务端 dbid_t 一致）。
inline constexpr std::int64_t kMaxDatabase = 1000000;

/**
 * @brief SET 选项：可选过期与条件写。
 *
 * 以毫秒表达 EX 语义（线上以 PX 发送），expire 设值时须 >0 且不超 kMaxSetExpire；
 * condition 为 NX/XX 时服务端条件不满足返回 nil，公开面表达为 ok(false) 而非错误。
 */
struct SetOptions {
    std::optional<std::chrono::milliseconds> expire; ///< 过期时长；未设置表示不设置过期。
    /// @brief 条件写模式：None 无条件写，NX 仅键不存在时写，XX 仅键存在时写。
    enum class Condition { None, NX, XX } condition = Condition::None; ///< 生效的条件写模式。
};

/// @brief SET 过期时长上限（365 天）；超过上限在接纳前返回 InvalidArgument。
inline constexpr std::chrono::milliseconds kMaxSetExpire{std::chrono::hours{24} * 365};

/**
 * @brief PTTL 三态：区分「key 不存在 / 存在但无过期 / 剩余毫秒」，不把服务端
 * -1/-2 魔数泄漏到公共 API。
 */
struct TtlResult {
    /// @brief PTTL 的三态：key 不存在、存在但无过期、剩余毫秒。
    enum class State { KeyMissing, NoExpiry, Expires } state; ///< 三态之一。
    std::chrono::milliseconds remaining{0};                   ///< 仅 State==Expires 有意义。
};

/// @brief HGETALL 的 field/value 对序列：保持服务端顺序与二进制安全。
using HashEntries = std::vector<std::pair<std::string, std::string>>;

/**
 * @brief 单连接 Redis 客户端：至多一个活动操作，无后台队列或隐式重连。
 *
 * 所有等待入口要求协程域；宿主拥有并推进无 RawHandler 的共享 Engine，地址只接受数值 IPv4/IPv6。
 * 故障连接（broken）不会自动重建，须显式 Connect；关闭后不重开。
 */
class CoRedisCli final {
public:
    /**
     * @brief 创建 client，只装配不联网。
     *
     * 与带 handshake 的重载等价于传入默认（不握手）配置。
     * @param engine 宿主拥有并推进的 Engine，不得为空。
     * @param address 服务端地址，须为数值 IPv4/IPv6。
     * @return 成功返回 client 句柄；参数非法返回 InvalidArgument，reader 分配失败返回 InternalError。
     */
    static result<std::shared_ptr<CoRedisCli>> Create(std::shared_ptr<net::Engine> engine, net::Address address);
    /**
     * @brief 携带连接级握手配置的装配。
     *
     * handshake 非法（database 越界）返回 InvalidArgument；配置随 State 固定，每次 Connect 内
     * 按 AUTH 再 SELECT 执行。任一步失败：Connect 返回映射后的错误，连接退役（broken），
     * client 仍可 Close；错误消息不含密码。
     * @param engine 宿主拥有并推进的 Engine，不得为空。
     * @param address 服务端地址，须为数值 IPv4/IPv6。
     * @param handshake 连接级握手配置。
     * @return 成功返回 client 句柄；参数非法返回 InvalidArgument，reader 分配失败返回 InternalError。
     */
    static result<std::shared_ptr<CoRedisCli>> Create(std::shared_ptr<net::Engine> engine, net::Address address,
                                                      RedisHandshake handshake);
    /// @brief 只调用 Close()，不等在途操作退出。
    ~CoRedisCli() noexcept;
    /// @brief 不可拷贝：句柄独占连接状态与 reader。
    CoRedisCli(const CoRedisCli&) = delete;
    /// @brief 不可拷贝赋值。
    CoRedisCli& operator=(const CoRedisCli&) = delete;

    /**
     * @brief 显式建连（含握手）：已连接时幂等。
     *
     * 故障后须显式 Connect；竞争中的活动操作返回 Overloaded。
     * 可能挂起的等待入口用 async/await 语法糖标记；调用方经 bbt_co_await 消费。
     * @param options 绝对期限选项。
     * @return 成功表示连接可用；失败见映射后的错误分类。
     */
    bbt_co_async(result<void>) Connect(const CallOptions& options);
    /// @brief PING，期望 +PONG；其余回复形态按 ProtocolError 处理。
    bbt_co_async(result<void>) Ping(const CallOptions& options);
    /// @brief GET：键缺失返回 ok(nullopt)。
    bbt_co_async(result<std::optional<std::string>>) Get(std::string_view key, const CallOptions& options);
    /// @brief 无条件 SET：写入成功返回 ok。
    bbt_co_async(result<void>) Set(std::string_view key, std::string_view value, const CallOptions& options);
    /**
     * @brief 带选项的 SET：写入成功 ok(true)；NX/XX 条件不满足（nil 回复）
     * ok(false)，不是错误。
     *
     * options.expire 越界在接纳前返回 InvalidArgument（零接纳，什么都不发）。空 key 同样 InvalidArgument。
     */
    bbt_co_async(result<bool>)
        Set(std::string_view key, std::string_view value, const SetOptions& set_options, const CallOptions& options);
    /// @brief EXISTS：键存在为 true。
    bbt_co_async(result<bool>) Exists(std::string_view key, const CallOptions& options);
    /// @brief DEL：返回服务端报告的实际删除数量。
    bbt_co_async(result<std::uint64_t>) Delete(std::vector<std::string> keys, const CallOptions& options);

    /**
     * @brief PEXPIRE：ok(true) 已设置过期；key 不存在 ok(false)。
     *
     * ttl 必须 >0，否则 InvalidArgument 零接纳。
     */
    bbt_co_async(result<bool>) Expire(std::string_view key, std::chrono::milliseconds ttl, const CallOptions& options);
    /// @brief PTTL：KeyMissing / NoExpiry / Expires 三态；不存在 -1/-2 魔数。
    bbt_co_async(result<TtlResult>) Ttl(std::string_view key, const CallOptions& options);

    /**
     * @brief INCR/DECR/INCRBY/DECRBY：返回服务端整数值。
     *
     * 非整数、溢出由服务端报错映射为 RemoteError（唯一保留连接的错误），连接保持同步。
     */
    bbt_co_async(result<std::int64_t>) Incr(std::string_view key, const CallOptions& options);
    /// @brief DECR 一步；错误语义同 Incr。
    bbt_co_async(result<std::int64_t>) Decr(std::string_view key, const CallOptions& options);
    /// @brief INCRBY 指定增量；错误语义同 Incr。
    bbt_co_async(result<std::int64_t>) IncrBy(std::string_view key, std::int64_t delta, const CallOptions& options);
    /// @brief DECRBY 指定减量；错误语义同 Incr。
    bbt_co_async(result<std::int64_t>) DecrBy(std::string_view key, std::int64_t delta, const CallOptions& options);

    /**
     * @brief Hash 命令族：key/field/value 二进制安全；空 key、空 field、HDel 空 fields
     * 一律 InvalidArgument 零接纳。
     *
     * HSet 的 ok(true) 是新建字段，ok(false) 是覆盖已有字段；HGet 在字段缺失或 key 不存在时
     * 返回 ok(nullopt)；HDel 返回删除数量；HExists 字段存在为 true。
     */
    bbt_co_async(result<bool>)
        HSet(std::string_view key, std::string_view field, std::string_view value, const CallOptions& options);
    /// @brief HGET：字段缺失或 key 不存在为 ok(nullopt)。
    bbt_co_async(result<std::optional<std::string>>)
        HGet(std::string_view key, std::string_view field, const CallOptions& options);
    /// @brief HDEL：返回删除的字段数量。
    bbt_co_async(result<std::uint64_t>)
        HDel(std::string_view key, std::vector<std::string> fields, const CallOptions& options);
    /// @brief HEXISTS：字段存在为 true。
    bbt_co_async(result<bool>) HExists(std::string_view key, std::string_view field, const CallOptions& options);
    /**
     * @brief HGETALL：field/value 对按服务端顺序。
     *
     * 嵌套数组、奇数元素、nil 元素、元素或字节超 detail::kMaxArrayElements/kMaxArrayBytes
     * 时退役连接，命令为 OutcomeUnknown（cause=ProtocolError）。
     */
    bbt_co_async(result<HashEntries>) HGetAll(std::string_view key, const CallOptions& options);

    /**
     * @brief 立即逻辑封口并请求关闭已发布连接；不等 fd、在途拨号或 API 任务退出。
     *
     * 宿主最终须 Close 全部 client、RequestClose Engine、join Run 且核 Stopped、
     * 等登记的全部 API 任务完成并释放捕获。不可直接充当旧 ICoCloseable 同步资源。
     */
    void Close() noexcept;
    /// @brief 只读查询逻辑封口：Close() 已生效为 true；不代表连接已物理回收。
    bool IsClosed() const noexcept;

private:
    friend class CoRedisPool;
    bool StreamHealthy() const noexcept;
    struct State;
    explicit CoRedisCli(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> state_;
};

} // namespace bbt::infra::redis
