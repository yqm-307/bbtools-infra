#pragma once

// 用户可设置的 Engine 配置（D3）：每个留下的字段都能映射到实际接线点。
//
// 冻结政策：只冻结这里的字段名与含义；新增字段会评估位置初始化/structured
// binding 等源码兼容影响。推荐具名赋值写法。
//
// 语义边界：
//   - num_event_loop 是 Reactor sub owner 数（server 另占 1 个 main 槽），
//     0 表示默认 1；负值拒绝；不做 CPU 数推导、不自动扩容。
//   - max_inbound_bytes / max_outbound_bytes 是每连接方向的资源总量界；
//     max_pending_events 只约束该连接的操作责任上限（不是所有 Engine 队列的
//     统一上限，其他内部硬界仍是实现限制）。
//   - reuse_port 只对应 socket SO_REUSEPORT，不表示跨 loop 接纳拓扑已验收；
//     其余 socket 字段直接映射到同名 socket 选项。
//
// 本头不做任何 I/O、日志、线程或 runtime 操作：只描述值。

#include <chrono>
#include <cstdint>
#include <system_error>

#include <bbt/infra/net/LoadBalancing.hpp>

namespace bbt::infra::net {

// 用户可设置的原始值。负值/超范围在 Engine::Run 的归一化阶段拒绝，
// 不会部分提交（先全量验证，再一次性生效）。
struct Options {
    LoadBalancing lb{LoadBalancing::RoundRobin};
    bool reuse_addr{false};                         // socket SO_REUSEADDR
    bool reuse_port{false};                         // socket SO_REUSEPORT
    std::int64_t num_event_loop{0};                 // 0 = 默认 1；>0 sub 数；<0 拒绝
    std::chrono::seconds tcp_keep_alive{0};         // 0 禁用；<0 拒绝
    std::chrono::seconds tcp_keep_interval{0};      // <0 拒绝；启用且为 0 时取 idle/5
    std::int64_t tcp_keep_count{0};                 // <0 或超原生 int 拒绝
    bool tcp_no_delay{true};                        // TCP_NODELAY，默认 true
    std::int64_t socket_recv_buffer{0};             // 0 不设置；<0 或超原生 int 拒绝
    std::int64_t socket_send_buffer{0};             // 0 不设置；<0 或超原生 int 拒绝
    std::int64_t max_inbound_bytes{1024 * 1024};    // 每连接读方向总量界，必须正值
    std::int64_t max_outbound_bytes{1024 * 1024};   // 每连接写方向总量界，必须正值
    std::int64_t max_pending_events{1024};          // 每连接操作责任上限，必须正值
};

} // namespace bbt::infra::net
