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
//   - max_inflight：已发出未回包命令的上限。本切片单 owner 串行，任一时刻至多
//     一条命令在途（实际并发恒为 1），故恒有 1 ≤ max_inflight——对任何通过校验的
//     合法配置成立。它当前只做装配期容量校验，不构成 pipeline 流控，也不保证多命令
//     并发；并发能力由「单 owner 串行」界定为 1（相对旧实现的并发预期下调，须下游
//     知悉）。真实多命令并发/流水线不在本切片范围。
//   - max_queue：等待发送/等待连接的命令队列容量，耗尽时新命令立即
//     返回 Overloaded，不无限排队。
//   - reconnect_on_new_command：连接进入 Failed（显式 Connect 建连失败，或已建立
//     连接被判定不可复用）后，是否为**故障之后提交的新命令**尝试一次新建连接。
//     默认 false：库只提供机制、不隐式恢复——同实例后续命令一律返回 TransportError，
//     由调用方决定 Disconnect/Connect 或 Close。true：故障之后提交的新命令**至多各
//     尝试一次**新连接；若该次重建再次失败，同一批次内其后已入队的命令可统一以
//     TransportError 落定（不各自再次重试）。已失败命令绝不重发/迁移，不做后台
//     循环重连，不隐藏已发生的失败或不确定执行结果。该选项不绕过显式生命周期：首次
//     未 Connect（Disconnected）、主动 Disconnect 之后、以及 Close 终态都不会被它
//     隐式建连，只有自 Failed 状态起提交的新命令才适用。
struct RedisClientConfig {
    std::string   host;         // 数值 IP 或主机名；v1 由 hiredis 在 io 域内解析
    std::uint16_t port;
    std::size_t   max_inflight; // 校验用；单 owner 串行天然 ≤1 在途
    std::size_t   max_queue;    // 排队容量上限
    bool          reconnect_on_new_command{false};
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

// CoRedisCli 连接状态。ConnectStatus() 只读本地状态，绝不发起网络探测。
enum class ConnectState {
    Disconnected,   // 从未连接，或已显式 Disconnect（配置保留，可再次 Connect）
    Connecting,     // Connect 进行中：真实 TCP 建连尚未完成
    Connected,      // TCP 已建立，命令可执行
    Disconnecting,  // Disconnect 收口进行中
    Failed,         // 建连失败，或已建立连接被判定不可复用
    Closed,         // Close() 终态：不可再 Connect
};

// 状态名的稳定英文串（日志/测试可读性；不参与语义）。
inline const char* ConnectStateName(ConnectState state) noexcept {
    switch (state) {
    case ConnectState::Disconnected:  return "Disconnected";
    case ConnectState::Connecting:    return "Connecting";
    case ConnectState::Connected:     return "Connected";
    case ConnectState::Disconnecting: return "Disconnecting";
    case ConnectState::Failed:        return "Failed";
    case ConnectState::Closed:        return "Closed";
    }
    return "Unknown";
}

// CoRedisCli：出站 Redis 组件。具体实现内部组合 CoTCP 与 hiredis：CoTCP
// 负责 socket/等待/关闭，hiredis 负责 RESP2 编解码；不新建专属线程。
//
// 显式生命周期（不做 Lazy：命令永不隐式建连）：
//   Connect(options) → Connected；Disconnect() → Disconnected（可再次 Connect）；
//   Close() → Closed（终态，不可再 Connect）。ConnectStatus() 只读本地状态。
//
// 命令语义边界：
//   - 全部命令只能在协程上下文调用，否则返回 Error(InvalidContext)；
//   - 命令要求 ConnectStatus()==Connected。Disconnected（含首次未 Connect 与主动
//     Disconnect 之后）与 Connecting/Disconnecting 期间调用命令返回
//     Error(RuntimeUnavailable) 且不建立连接；Close 之后返回 Error(Closed)；
//     Failed 状态下默认返回 Error(TransportError)（sticky）；
//   - Get 命中缺失键返回 ok 的 std::nullopt，与错误分支区分；
//   - 服务端错误回复（如 WRONGTYPE）映射为 Error(RemoteError)，
//     domain_code 为 Redis 错误串首个词；
//   - 连接断开期间已在途命令不回滚也不迁移：报 Error(TransportError)；连接
//     出错后即被判不可复用（不自动重发、不复用坏连接）。后续新命令的行为由
//     RedisClientConfig::reconnect_on_new_command 决定：默认 false 时一律返回
//     Error(TransportError)（sticky，调用方自行 Disconnect/Connect 或 Close）；
//     true 时允许故障之后提交的新命令至多各尝试一次新建连接，该次重建失败即报错
//     （同一批次内其后已入队命令可统一以 TransportError 落定）。两种取值都不重发
//     已失败命令、不做后台重连循环；
//   - 上述 TransportError 可能与并发生命周期操作竞争：若返回当刻连接已被并发的
//     Disconnect/Close 取代，则按取代方状态收口——状态可能是 Disconnected（可再次
//     Connect）而非 Failed，并非所有 TransportError 都伴随 Failed 终态；
//   - 命令 deadline/owner close 竞争只发布一次逻辑终态；Close() 是同步物理
//     收口：返回当刻 fd/reader 已回收（含在途 Dial TCP 的候选 fd——由 owner 关闭
//     原语在封口后同步收口，无例外）、已登记 operation 已落定，owner 不再访问本
//     对象拥有的后端资源。
//
// 本切片边界（ceiling，不得读作已支持）：单连接 / 单 owner 严格串行，任一时刻
// 至多一条命令在途，不提供 pipeline 或跨命令流水；连接只在显式 Connect 时建立；
// 不支持 TLS、RESP3/PUSH、Cluster/Sentinel、多连接池、空闲断连探测、后台自动重连
// 循环与失败命令自动重试。Disconnect/Close 在调用线程内**同步等待**在途 Dial 收口
// ——该等待无超时（不称“有界”）；等待段本身不含挂起点，但被唤醒的 dial 协程需能被
// 其它 Scheduler 线程推进才会退出，故若进程内全部 Scheduler 线程都停在该等待上则
// Close/Disconnect 会一直等待：单线程 Scheduler 下不得从 dial 所在线程调用
// Disconnect/Close。此为与既有 Close 相同的边界，不新增线程。
class CoRedisCli : public ICoNetwork, public ICoCloseable {
public:
    // Create 在启动控制线程使用，不挂起协程；要求 Scheduler 已启动（对象身份需要
    // 运行时已初始化），否则返回 Error(RuntimeUnavailable)。
    static result<std::shared_ptr<CoRedisCli>> Create(RedisClientConfig config);

    // 显式建连：必须在协程上下文调用，否则 Error(InvalidContext)。立即发起并
    // **等待真实 TCP 建连完成**——成功返回即连接已建立（不是仅投递/入队），状态
    // 进入 Connected。建连失败/超时返回 TransportError/TimedOut 并进入 Failed；
    // Failed 状态下重复 Connect 可显式重建。
    //   - 已 Connected 时重复 Connect 幂等返回 ok（不重新拨号）；
    //   - Connecting 或 Disconnecting 中重复 Connect 返回 Error(Overloaded)（不引入
    //     排队/等待架构；Disconnecting 期间连接正在收口，不接受并发 Connect）；
    //   - Close 之后返回 Error(Closed)。
    virtual result<void> Connect(const CallOptions& options) = 0;

    // 只读本地连接状态；任意线程可调用，不发起任何网络请求。
    virtual ConnectState ConnectStatus() const noexcept = 0;

    // 同步释放当前连接的 fd/reader 与已登记请求；保留配置，之后同一实例可再次
    // 显式 Connect。它不是 Close 的别名：不进入终态、不阻止重连。任意线程可调用、
    // 幂等；未连接时为空操作。正在执行的命令以 TransportError 落定。
    virtual void Disconnect() noexcept = 0;

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
