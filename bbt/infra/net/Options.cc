#include <bbt/infra/net/Options.hpp>

// 实现放 .cc 的理由：20+ 字段的验证/推导体量会显著膨胀公共头并拖慢
// 每个消费者的编译；值契约本身仍在头文件（Options/NormalizedOptions）。

#include <cstdint>
#include <limits>
#include <utility>

namespace bbt::infra::net {
namespace {

constexpr std::size_t kDefaultStreamBufferCap = 64 * 1024;  // gnet MaxStreamBufferCap
constexpr std::size_t kMinBufferCap = 1024;                 // gnet ring.DefaultBufferSize
constexpr std::size_t kDefaultEtChunk = 1024 * 1024;        // gnet 1 << 20
constexpr std::int64_t kMaxNativeInt = std::numeric_limits<std::int32_t>::max();
constexpr std::int64_t kMaxLoopCount = 10000;               // gnet LockOSThread 上限

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

bool IsValidLogLevel(LogLevel level) noexcept
{
    switch (level) {
    case LogLevel::Debug:
    case LogLevel::Info:
    case LogLevel::Warn:
    case LogLevel::Error:
        return true;
    }
    return false;
}

// 正值向上取 2 的幂；v <= 0 或取整会超 size_t 表示时返回 0（拒绝）。
std::size_t CeilToPowerOfTwo(std::int64_t value) noexcept
{
    if (value <= 0 || value > (std::int64_t{1} << 62)) {
        return 0;
    }
    std::uint64_t result = 1;
    while (result < static_cast<std::uint64_t>(value)) {
        result <<= 1;
    }
    return static_cast<std::size_t>(result);
}

bool HasEmbeddedNul(const std::string& text) noexcept
{
    return text.find('\0') != std::string::npos;
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

std::error_code NormalizeOptions(const Options& options, int cpu_count, NormalizedOptions& out)
{
    if (cpu_count <= 0) {
        return InvalidArgument();
    }
    if (!IsValidLoadBalancing(options.lb) || !IsValidLogLevel(options.log_level)) {
        return InvalidArgument();
    }
    if (options.num_event_loop < 0) {
        return InvalidArgument();
    }
    if (options.read_buffer_cap < 0 || options.write_buffer_cap < 0) {
        return InvalidArgument();
    }
    if (options.edge_triggered_io_chunk < 0) {
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
    if (options.multicast_interface_index < 0
        || options.multicast_interface_index > std::numeric_limits<std::uint32_t>::max()) {
        return OutOfRange();
    }
    if (HasEmbeddedNul(options.bind_to_device) || HasEmbeddedNul(options.log_path)) {
        return InvalidArgument();
    }
    if (options.max_inbound_bytes <= 0 || options.max_outbound_bytes <= 0
        || options.max_pending_events <= 0) {
        return InvalidArgument();
    }

    NormalizedOptions normalized;

    normalized.lb = options.lb;
    normalized.reuse_addr = options.reuse_addr;
    normalized.reuse_port = options.reuse_port;
    normalized.multicore = options.multicore;
    normalized.lock_os_thread = options.lock_os_thread;
    normalized.ticker = options.ticker;
    normalized.tcp_no_delay = options.tcp_no_delay;

    if (options.num_event_loop > 0) {
        normalized.num_event_loop = static_cast<std::size_t>(options.num_event_loop);
    } else {
        // 未指定 loops：multicore 用传入 CPU 数，否则 1；不偷偷增线程。
        normalized.num_event_loop = options.multicore
            ? static_cast<std::size_t>(cpu_count)
            : std::size_t{1};
    }
    if (options.lock_os_thread && normalized.num_event_loop > static_cast<std::size_t>(kMaxLoopCount)) {
        // gnet.go:575-579 ErrTooManyEventLoopThreads 的值层等价检查。
        return InvalidArgument();
    }

    if (options.read_buffer_cap == 0) {
        normalized.read_buffer_cap = kDefaultStreamBufferCap;
    } else if (options.read_buffer_cap < static_cast<std::int64_t>(kMinBufferCap)) {
        normalized.read_buffer_cap = kMinBufferCap;
    } else {
        const std::size_t capped = CeilToPowerOfTwo(options.read_buffer_cap);
        if (capped == 0) {
            return OutOfRange();
        }
        normalized.read_buffer_cap = capped;
    }
    if (options.write_buffer_cap == 0) {
        normalized.write_buffer_cap = kDefaultStreamBufferCap;
    } else if (options.write_buffer_cap < static_cast<std::int64_t>(kMinBufferCap)) {
        normalized.write_buffer_cap = kMinBufferCap;
    } else {
        const std::size_t capped = CeilToPowerOfTwo(options.write_buffer_cap);
        if (capped == 0) {
            return OutOfRange();
        }
        normalized.write_buffer_cap = capped;
    }
    // 基础 buffer cap 不能超对应方向总量（两者语义不同）。
    if (normalized.read_buffer_cap > static_cast<std::size_t>(options.max_inbound_bytes)
        || normalized.write_buffer_cap > static_cast<std::size_t>(options.max_outbound_bytes)) {
        return InvalidArgument();
    }

    if (options.edge_triggered_io_chunk > 0) {
        const std::size_t chunk = CeilToPowerOfTwo(options.edge_triggered_io_chunk);
        if (chunk == 0) {
            return OutOfRange();
        }
        normalized.edge_triggered_io = true;
        normalized.edge_triggered_io_chunk = chunk;
    } else if (options.edge_triggered_io) {
        normalized.edge_triggered_io = true;
        normalized.edge_triggered_io_chunk = kDefaultEtChunk;
    }

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
    normalized.multicast_interface_index =
        static_cast<std::size_t>(options.multicast_interface_index);
    normalized.log_path = options.log_path;
    normalized.bind_to_device = options.bind_to_device;
    normalized.log_level = options.log_level;
    normalized.logger = options.logger;
    normalized.max_inbound_bytes = static_cast<std::size_t>(options.max_inbound_bytes);
    normalized.max_outbound_bytes = static_cast<std::size_t>(options.max_outbound_bytes);
    normalized.max_pending_events = static_cast<std::size_t>(options.max_pending_events);

    out = std::move(normalized);    // 全部通过才提交；失败路径 out 不变。
    return {};
}

} // namespace bbt::infra::net
