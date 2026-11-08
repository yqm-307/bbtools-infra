/** @file RedisTypes.hpp
 * @brief Redis 命令选项、返回值与装配值；不依赖具体客户端或网络实现。
 */
#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

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

} // namespace bbt::infra::redis
