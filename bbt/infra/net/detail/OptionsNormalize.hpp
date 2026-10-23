#pragma once

// Engine 内部配置归一化（D3）：不属公共配置 API，不列入公共头自检。
//
// 为什么内部化：归一化结果（loop 数、缩放后的界）是实现细节，冻结它会迫使
// 后续内部调整（例如段容量算法）变成源码不兼容；纯值测试经内部头进行。
//
// 纯值验证性质不变：只做值检查与推导，不读 CPU/环境、不调 logger、不创建
// 线程/socket；失败时 out 保持原值（先写临时再提交）。

#include <chrono>
#include <cstddef>
#include <system_error>

#include <bbt/infra/net/LoadBalancing.hpp>
#include <bbt/infra/net/Options.hpp>

namespace bbt::infra::net {

// 归一化后的值：只保留真正被接线消费的字段。
struct NormalizedOptions {
    LoadBalancing lb{LoadBalancing::RoundRobin};
    bool reuse_addr{false};
    bool reuse_port{false};
    bool tcp_no_delay{true};
    std::size_t num_event_loop{1};
    std::chrono::seconds tcp_keep_alive{0};
    std::chrono::seconds tcp_keep_interval{0};
    std::size_t tcp_keep_count{0};
    std::size_t socket_recv_buffer{0};               // 0 = 不设置
    std::size_t socket_send_buffer{0};               // 0 = 不设置
    std::size_t max_inbound_bytes{0};
    std::size_t max_outbound_bytes{0};
    std::size_t max_pending_events{0};
};

//   - loop：0 → 1；正值即 sub 数；负值拒绝；不按 CPU 数推导。
//   - keepalive：启用（idle>0）时 interval 未设取 idle/5，除得 0 时最小 1 秒；
//     count 未设取 5；正 count 不得超原生 int（TCP_KEEPCNT 截断防护）。
//     禁用时不静默修正 interval/count，仅保留原值。
//   - socket buffer 0 表示不设置，负值/超原生 int 拒绝。
//   - 每连接总量界必须正值；不再有段容量字段与之交叉校验——段容量由内部
//     算法决定，因此极小总量界（例如 1 字节）也能得到可解释结果。
std::error_code NormalizeOptions(const Options& options, NormalizedOptions& out);

} // namespace bbt::infra::net
