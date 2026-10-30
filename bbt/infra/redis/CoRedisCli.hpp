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

// 连接级握手配置（AUTH/SELECT）：每次 Connect（含故障后显式重连）在
// 已接纳操作内按 AUTH 再 SELECT 顺序各等 +OK，不流水线、不缓存终态。
// 默认构造为不握手，行为与未配置时完全一致。密码只在编码命令内短暂
// 存在，不写入任何错误消息、日志或 details。
struct RedisHandshake {
    std::string   username;  // 可选；为空时 AUTH 走单参数 legacy 形式。
    std::string   password;  // 可选；为空表示不发送 AUTH。
    std::int64_t  database = -1; // <0 表示不 SELECT；合法值 0..kMaxDatabase。
};

// Redis SELECT 的库序号实际上限（int64 正数，与服务端 dbid_t 一致）。
inline constexpr std::int64_t kMaxDatabase = 1000000;

// SET 选项：可选过期与条件写。以毫秒表达 EX 语义（线上以 PX 发送），
// expire 设值时须 >0 且不超 kMaxSetExpire；condition 为 NX/XX 时服务端
// 条件不满足返回 nil，公开面表达为 ok(false) 而非错误。
struct SetOptions {
    std::optional<std::chrono::milliseconds> expire;
    enum class Condition { None, NX, XX } condition = Condition::None;
};
inline constexpr std::chrono::milliseconds kMaxSetExpire{
    std::chrono::hours{24} * 365};

// PTTL 三态：区分「key 不存在 / 存在但无过期 / 剩余毫秒」，不把服务端
// -1/-2 魔数泄漏到公共 API。
struct TtlResult {
    enum class State { KeyMissing, NoExpiry, Expires } state;
    std::chrono::milliseconds remaining{0}; // 仅 State==Expires 有意义。
};

// HGETALL 的 field/value 对序列：保持服务端顺序与二进制安全。
using HashEntries = std::vector<std::pair<std::string, std::string>>;

// 单连接、至多一个活动操作，无后台队列或隐式重连。所有等待入口要求协程域。
// 宿主拥有并推进无 RawHandler 的共享 Engine；地址只接受数值 IPv4/IPv6。
class CoRedisCli final {
public:
    static result<std::shared_ptr<CoRedisCli>> Create(std::shared_ptr<net::Engine> engine, net::Address address);
    // 携带连接级握手配置的装配：handshake 非法（database 越界）返回
    // InvalidArgument；配置随 State 固定，每次 Connect 内按 AUTH 再 SELECT
    // 执行。任一步失败：Connect 返回映射后的错误，连接退役（broken），
    // client 仍可 Close；错误消息不含密码。
    static result<std::shared_ptr<CoRedisCli>> Create(std::shared_ptr<net::Engine> engine, net::Address address, RedisHandshake handshake);
    ~CoRedisCli() noexcept;
    CoRedisCli(const CoRedisCli&) = delete;
    CoRedisCli& operator=(const CoRedisCli&) = delete;

    // 已连接时幂等；故障后须显式 Connect，竞争活动操作返回 Overloaded。
    // 可能挂起的等待入口用 async/await 语法糖标记；调用方经 bbt_co_await 消费。
    bbt_co_async(result<void>)    Connect(const CallOptions& options);
    bbt_co_async(result<void>)    Ping(const CallOptions& options);
    bbt_co_async(result<std::optional<std::string>>) Get(std::string_view key, const CallOptions& options);
    bbt_co_async(result<void>)    Set(std::string_view key, std::string_view value, const CallOptions& options);
    // 带选项的 SET：写入成功 ok(true)；NX/XX 条件不满足（nil 回复）
    // ok(false)，不是错误。options.expire 越界在接纳前返回
    // InvalidArgument（零接纳，什么都不发）。空 key 同样 InvalidArgument。
    bbt_co_async(result<bool>)    Set(std::string_view key, std::string_view value, const SetOptions& set_options, const CallOptions& options);
    bbt_co_async(result<bool>)    Exists(std::string_view key, const CallOptions& options);
    bbt_co_async(result<std::uint64_t>) Delete(std::vector<std::string> keys, const CallOptions& options);

    // PEXPIRE：ok(true) 已设置过期；key 不存在 ok(false)。ttl 必须 >0，
    // 否则 InvalidArgument 零接纳。
    bbt_co_async(result<bool>)    Expire(std::string_view key, std::chrono::milliseconds ttl, const CallOptions& options);
    // PTTL：KeyMissing / NoExpiry / Expires 三态；不存在 -1/-2 魔数。
    bbt_co_async(result<TtlResult>) Ttl(std::string_view key, const CallOptions& options);

    // INCR/DECR/INCRBY/DECRBY：返回服务端整数值；非整数、溢出由服务端
    // 报错映射为 RemoteError（唯一保留连接的错误），连接保持同步。
    bbt_co_async(result<std::int64_t>) Incr(std::string_view key, const CallOptions& options);
    bbt_co_async(result<std::int64_t>) Decr(std::string_view key, const CallOptions& options);
    bbt_co_async(result<std::int64_t>) IncrBy(std::string_view key, std::int64_t delta, const CallOptions& options);
    bbt_co_async(result<std::int64_t>) DecrBy(std::string_view key, std::int64_t delta, const CallOptions& options);

    // Hash：key/field/value 二进制安全；空 key、空 field、HDel 空 fields
    // 一律 InvalidArgument 零接纳。
    //   HSet：ok(true) 新建字段，ok(false) 覆盖已有字段。
    //   HGet：字段缺失或 key 不存在为 ok(nullopt)。
    //   HDel：返回删除数量。
    //   HExists：字段存在为 true。
    //   HGetAll：field/value 对按服务端顺序；嵌套数组、奇数元素、nil 元素、
    //     元素或字节超 detail::kMaxArrayElements/kMaxArrayBytes 时退役连接，
    //     命令为 OutcomeUnknown（cause=ProtocolError）。
    bbt_co_async(result<bool>)    HSet(std::string_view key, std::string_view field, std::string_view value, const CallOptions& options);
    bbt_co_async(result<std::optional<std::string>>) HGet(std::string_view key, std::string_view field, const CallOptions& options);
    bbt_co_async(result<std::uint64_t>) HDel(std::string_view key, std::vector<std::string> fields, const CallOptions& options);
    bbt_co_async(result<bool>)    HExists(std::string_view key, std::string_view field, const CallOptions& options);
    bbt_co_async(result<HashEntries>) HGetAll(std::string_view key, const CallOptions& options);

    // 立即逻辑封口并请求关闭已发布连接；不等 fd、在途拨号或 API 任务退出。
    // 宿主最终须 Close 全部 client、RequestClose Engine、join Run 且核 Stopped、
    // 等登记的全部 API 任务完成并释放捕获。不可直接充当旧 ICoCloseable 同步资源。
    void            Close() noexcept;
    bool            IsClosed() const noexcept;

private:
    friend class CoRedisPool;
    bool StreamHealthy() const noexcept;
    struct State;
    explicit        CoRedisCli(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> state_;
};

} // namespace bbt::infra::redis
