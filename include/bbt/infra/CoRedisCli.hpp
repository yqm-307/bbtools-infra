#pragma once
// co-redis/v1（bbtools-infra#6）：协程原生 Redis 客户端公共面。
// hiredis 类型、线程、io_context、fd 全部隐藏于 src/redis/ 实现，
// 本头只出现 infra/标准库类型。

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra {

// RedisClientConfig 各项显式、大于零且不超下列上限；违规返回 InvalidArgument。
inline constexpr std::size_t kRedisLimitsMaxInflight = 1000000;
inline constexpr std::size_t kRedisLimitsMaxQueue    = 1000000;

// CoRedisCli 装配参数。本切片单连接语义：一个 client 实例持有一条到
// host:port 的 TCP 连接（连接数显式有界为 1；更多并发连接由调用方建
// 更多 client 实例聚合）。
//   - max_inflight：已发出未回包命令的上限（流控），不拒绝接纳；
//   - max_queue：等待发送/等待连接的命令队列容量，耗尽时新命令立即
//     返回 Overloaded，不无限排队。
struct RedisClientConfig {
    std::string   host;         // 数值 IP 或主机名；v1 由 hiredis 在 io 域内解析
    std::uint16_t port;
    std::size_t   max_inflight;
    std::size_t   max_queue;
};

inline result<void> ValidateRedisClientConfig(const RedisClientConfig& cfg) {
    if (cfg.host.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "redis client config: empty host"));
    if (cfg.port == 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "redis client config: port out of range"));
    if (cfg.max_inflight == 0 || cfg.max_inflight > kRedisLimitsMaxInflight)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "redis client config: max_inflight out of range"));
    if (cfg.max_queue == 0 || cfg.max_queue > kRedisLimitsMaxQueue)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "redis client config: max_queue out of range"));
    return result<void>::ok();
}

// CoRedisCli：出站 Redis 组件（hiredis async + 自定义 event adapter，
// fd 事件接入 coroutine 共享 executor 派生的 strand，不新建线程/context）。
//
// 命令语义边界：
//   - 全部命令只能在协程上下文调用，否则返回 Error(InvalidContext)；
//   - Get 命中缺失键返回 ok 的 std::nullopt，与错误分支区分；
//   - 服务端错误回复（如 WRONGTYPE）映射为 Error(RemoteError)，
//     domain_code 为 Redis 错误串首个词；
//   - 连接断开期间已在途命令不回滚也不迁移：报 Error(TransportError)；
//     断开后新命令触发重连，连接建立前在队列内等待（受 max_queue 与
//     各自 deadline 约束），不重发已断命令；
//   - deadline/cancel/owner close 竞争只发布一次逻辑终态；逻辑返回
//     与物理清理分离，WaitClosed 等在途 operation/connection 归零。
class CoRedisCli : public ICoNetwork, public ICoCloseable {
public:
    // Create/Start 在启动控制线程使用，不挂起协程。
    // Create 要求 Scheduler 已启动（对象身份与完成信号需要运行时代际），
    // 否则返回 Error(RuntimeUnavailable)。
    static result<std::shared_ptr<CoRedisCli>> Create(RedisClientConfig config);

    // 同一 client 只成功启动一次；要求与 Create 同一运行时代际。
    // Start 后立即在 io 域发起首次连接；关闭后不重开。
    virtual result<void> Start() = 0;

    // PING → 服务端回 PONG 为 ok；其余回复为 ProtocolError/RemoteError。
    virtual result<void> Ping(const CallOptions& options) = 0;

    // GET key → 命中返回 bulk 值（二进制安全），未命中返回 std::nullopt。
    virtual result<std::optional<std::string>> Get(
        std::string_view key, const CallOptions& options) = 0;

    // SET key value → 服务端回 OK 为 ok；key/value 均二进制安全。
    virtual result<void> Set(std::string_view key, std::string_view value,
                             const CallOptions& options) = 0;

    // EXISTS key → 存在返回 true。
    virtual result<bool> Exists(std::string_view key,
                                const CallOptions& options) = 0;

    // DEL keys... → 返回实际删除数量；空列表或含空键为 InvalidArgument。
    virtual result<std::uint64_t> Delete(std::vector<std::string> keys,
                                         const CallOptions& options) = 0;
};

} // namespace bbt::infra
