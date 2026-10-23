#include <bbt/infra/net/detail/OptionsNormalize.hpp>

// 实现放 .cc 的理由：字段验证/推导体量会显著膨胀头并拖慢每个消费者编译；
// 归一化已内部化（detail 头），公共面只剩 Options 值类型本身。

#include <cstdint>
#include <limits>
#include <utility>

namespace bbt::infra::net {
namespace {

constexpr std::int64_t kMaxNativeInt = std::numeric_limits<std::int32_t>::max();

bool IsValidLoadBalancing(LoadBalancing mode) noexcept
{
    switch (mode) {
    case LoadBalancing::RoundRobin:
    case LoadBalancing::LeastConnections:
    case LoadBalancing::SourceAddrHash:
        return true;
    }
    return false;
}

std::error_code InvalidArgument()
{
    return std::make_error_code(std::errc::invalid_argument);
}

std::error_code OutOfRange()
{
    return std::make_error_code(std::errc::result_out_of_range);
}

} // namespace

std::error_code NormalizeOptions(const Options& options, NormalizedOptions& out)
{
    if (!IsValidLoadBalancing(options.lb)) {
        return InvalidArgument();
    }
    if (options.num_event_loop < 0) {
        return InvalidArgument();
    }
    if (options.tcp_keep_alive < std::chrono::seconds::zero()
        || options.tcp_keep_interval < std::chrono::seconds::zero()
        || options.tcp_keep_count < 0) {
        return InvalidArgument();
    }
    if (options.tcp_keep_alive > std::chrono::seconds{kMaxNativeInt}
        || options.tcp_keep_interval > std::chrono::seconds{kMaxNativeInt}) {
        // 超原生 int 秒范围不默默截断（内核接口按 int 秒）。
        return OutOfRange();
    }
    if (options.tcp_keep_count > kMaxNativeInt) {
        // TCP_KEEPCNT 按原生 int 传入，超界接线时必截断；启用与禁用都做
        // 值范围验证，失败时 out 保持原值（提交发生在全部通过之后）。
        return OutOfRange();
    }
    if (options.socket_recv_buffer < 0 || options.socket_recv_buffer > kMaxNativeInt
        || options.socket_send_buffer < 0 || options.socket_send_buffer > kMaxNativeInt) {
        return OutOfRange();
    }
    if (options.max_inbound_bytes <= 0 || options.max_outbound_bytes <= 0
        || options.max_pending_events <= 0) {
        return InvalidArgument();
    }
    // 每连接总量界是 size_t 资源界：超过 size_t 表示范围同样拒绝（不静默截断）。
    // num_event_loop 一并验证：正值须能用 size_t 表示，且要给 server 的
    // main 槽留出 +1 空间（否则 slot_count 会在窄平台溢出）。
    constexpr auto kMaxSizeT = std::numeric_limits<std::size_t>::max();
    if (static_cast<std::uint64_t>(options.max_inbound_bytes) > kMaxSizeT
        || static_cast<std::uint64_t>(options.max_outbound_bytes) > kMaxSizeT
        || static_cast<std::uint64_t>(options.max_pending_events) > kMaxSizeT) {
        return OutOfRange();
    }
    if (options.num_event_loop > 0
        && static_cast<std::uint64_t>(options.num_event_loop) >= kMaxSizeT) {
        return OutOfRange();
    }

    NormalizedOptions normalized;

    normalized.lb = options.lb;
    normalized.reuse_addr = options.reuse_addr;
    normalized.reuse_port = options.reuse_port;
    normalized.tcp_no_delay = options.tcp_no_delay;
    // 未指定 loops → 1；不按 CPU 数推导，也不偷偷增线程。
    normalized.num_event_loop = options.num_event_loop > 0
        ? static_cast<std::size_t>(options.num_event_loop)
        : std::size_t{1};

    normalized.tcp_keep_alive = options.tcp_keep_alive;
    normalized.tcp_keep_interval = options.tcp_keep_interval;
    if (options.tcp_keep_alive > std::chrono::seconds::zero()) {
        if (options.tcp_keep_interval > std::chrono::seconds::zero()) {
            normalized.tcp_keep_interval = options.tcp_keep_interval;
        } else {
            normalized.tcp_keep_interval =
                std::chrono::seconds{options.tcp_keep_alive.count() / 5};
            if (normalized.tcp_keep_interval <= std::chrono::seconds::zero()) {
                // C++ 差异：最小 1 秒（参考按 Go int 相除可为 0）。
                normalized.tcp_keep_interval = std::chrono::seconds{1};
            }
        }
        normalized.tcp_keep_count =
            options.tcp_keep_count > 0 ? static_cast<std::size_t>(options.tcp_keep_count) : 5u;
    } else {
        // 禁用时不静默修正：保留用户 interval/count 原值，接线层才应用。
        normalized.tcp_keep_count =
            options.tcp_keep_count > 0 ? static_cast<std::size_t>(options.tcp_keep_count) : 0u;
    }

    normalized.socket_recv_buffer = static_cast<std::size_t>(options.socket_recv_buffer);
    normalized.socket_send_buffer = static_cast<std::size_t>(options.socket_send_buffer);
    normalized.max_inbound_bytes = static_cast<std::size_t>(options.max_inbound_bytes);
    normalized.max_outbound_bytes = static_cast<std::size_t>(options.max_outbound_bytes);
    normalized.max_pending_events = static_cast<std::size_t>(options.max_pending_events);

    out = std::move(normalized);    // 全部通过才提交；失败路径 out 不变。
    return {};
}

} // namespace bbt::infra::net
