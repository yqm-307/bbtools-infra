#pragma once

// gnet options.go 全字段映射的值层契约（v2.10.0，Apache-2.0，见
// docs/dependencies/gnet-2.10.0.provenance.md）。归一化只做值验证与推导，
// 不调用 logger、不读环境/CPU（CPU 数显式传入）、不创建文件/socket、
// 不启动 runtime/线程；未接线的网络行为（reactor/listener/loop 启动）
// 保持 D3 待后续包实现，这里没有任何返回成功的 setter。
//
// LoggerCallback 是用户回调：不因存放或包装获得任何非挂起保证
// （0010 标签规则）。

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <system_error>

#include <bbt/infra/net/LoadBalancing.hpp>

namespace bbt::infra::net {

// 自有窄日志等级值；非法枚举值（任意 cast）在归一化时拒绝。
enum class LogLevel : int {
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
};

// 唯一的可存储自有日志出口：窄值 + 回调，不引入 zap、不实现日志框架。
using LoggerCallback = std::function<void(LogLevel, std::string_view)>;

// 用户可设置的原始值（gnet options.go 逐字段对应；秒字段用
// std::chrono::seconds 表达"负时长/超原生 int 范围拒绝"）。
struct Options {
    LoadBalancing lb{LoadBalancing::RoundRobin};
    bool reuse_addr{false};
    bool reuse_port{false};
    std::int64_t multicast_interface_index{0};      // >= 0
    std::string bind_to_device;                     // 拒绝嵌 NUL
    bool multicore{false};
    std::int64_t num_event_loop{0};                 // 0 未指定；>0 覆盖；<0 拒绝
    std::int64_t read_buffer_cap{0};                // 0 默认 64KiB；<0 拒绝
    std::int64_t write_buffer_cap{0};               // 0 默认 64KiB；<0 拒绝
    bool lock_os_thread{false};
    bool ticker{false};
    std::chrono::seconds tcp_keep_alive{0};         // 0 禁用；<0 拒绝
    std::chrono::seconds tcp_keep_interval{0};      // <0 拒绝
    std::int64_t tcp_keep_count{0};                 // <0 或超原生 int 拒绝
    bool tcp_no_delay{true};                        // engine 默认 true（0010）
    std::int64_t socket_recv_buffer{0};             // 0 不设置；<0 拒绝
    std::int64_t socket_send_buffer{0};             // 0 不设置；<0 拒绝
    std::string log_path;                           // 拒绝嵌 NUL
    LogLevel log_level{LogLevel::Info};
    LoggerCallback logger;
    bool edge_triggered_io{false};
    std::int64_t edge_triggered_io_chunk{0};        // >0 启用 ET；<0 拒绝

    // 本迁移有界资源扩展（0010 候选：每方向 1MiB / 队列 1024）。
    // 语义与 Read/WriteBufferCap 不同：这是每连接总量界，不是静态段容量。
    std::int64_t max_inbound_bytes{1024 * 1024};    // 必须正值
    std::int64_t max_outbound_bytes{1024 * 1024};   // 必须正值
    std::int64_t max_pending_events{1024};          // 必须正值
};

// 归一化后的值：供后续 Engine 消费；loop 数等在此已解析。
struct NormalizedOptions {
    LoadBalancing lb{LoadBalancing::RoundRobin};
    bool reuse_addr{false};
    bool reuse_port{false};
    bool multicore{false};
    bool lock_os_thread{false};
    bool ticker{false};
    bool tcp_no_delay{true};
    bool edge_triggered_io{false};
    std::size_t num_event_loop{1};
    std::size_t read_buffer_cap{64 * 1024};
    std::size_t write_buffer_cap{64 * 1024};
    std::size_t edge_triggered_io_chunk{0};          // 0 = ET 未启用
    std::chrono::seconds tcp_keep_alive{0};
    std::chrono::seconds tcp_keep_interval{0};
    std::size_t tcp_keep_count{0};
    std::size_t socket_recv_buffer{0};               // 0 = 不设置
    std::size_t socket_send_buffer{0};               // 0 = 不设置
    std::size_t multicast_interface_index{0};
    std::string log_path;
    std::string bind_to_device;
    LogLevel log_level{LogLevel::Info};
    LoggerCallback logger;
    std::size_t max_inbound_bytes{0};
    std::size_t max_outbound_bytes{0};
    std::size_t max_pending_events{0};
};

// 纯值归一化：验证失败时返回错误且 out 保持原值（内部先写临时再提交），
// 原输入 const 传入不可能被半改。
//   - loop：未指定且 multicore=false → 1；multicore=true → cpu_count；
//     正 NumEventLoop 覆盖；负值拒绝；不偷偷增线程。
//   - read/write cap：0 默认 64KiB（gnet MaxStreamBufferCap）；正值最小
//     1024（ring.DefaultBufferSize）再向上 2 次幂（差异：参考对 <=0 一律
//     默认，本迁移负值严格拒绝）；取整先核 size_t 溢出；且不得超对应
//     方向总量上限（max_inbound/outbound_bytes，两者语义不同）。
//   - ET chunk：>0 启用 ET 并向上 2 次幂；ET=true 且 chunk==0 → 默认 1MiB；
//     负值/溢出拒绝。
//   - keepalive：启用（idle>0）时 interval 未设取 idle/5，除得 0 时最小
//     1 秒（C++ 差异，测试覆盖边界），count 未设取 5，正 count 不得超
//     原生 int（TCP_KEEPCNT 截断防护，超界拒绝）；禁用时不静默修正
//     interval/count（仅保留原值，引擎接线时才生效）。
//   - socket buffer 0 表示不设置，负值/超原生 int 范围拒绝；
//     bind_to_device/log_path 拒绝嵌 NUL；接口索引/日志等级/枚举越界拒绝。
std::error_code NormalizeOptions(const Options& options, int cpu_count, NormalizedOptions& out);

} // namespace bbt::infra::net
