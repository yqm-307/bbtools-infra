#pragma once

/**
 * @file Options.hpp
 * @brief Engine 的用户配置值。
 *
 * 每个字段都映射到实际的接线点：socket 选项或 Engine 的资源界。字段名与含义属于稳定的公共契约；
 * 新增字段需要评估位置初始化与 structured binding 的源码兼容性，因此推荐使用具名赋值。
 * 本头不做 I/O、日志、线程或 runtime 操作，只描述值。
 */
#include <chrono>
#include <cstdint>
#include <system_error>

#include <bbt/infra/net/LoadBalancing.hpp>

namespace bbt::infra::net {

/**
 * @brief 用户可设置的 Engine 配置原始值。
 *
 * 字段级的非法值（负数、明显越界）以及超出原生 int 或 size_t 表示范围的值，都在 Engine::Run
 * 的归一化阶段拒绝。归一化先全量校验，全部通过后才整体生效，因此任一字段非法都不会部分提交。
 * worker 槽不足属于运行期资源事实，不在归一化阶段判定：绑定时以 no_buffer_space 返回。
 * 本类型不设固定的 loop 数上界。
 *
 * 语义边界：
 * - num_event_loop 是 Reactor sub owner 的数量，server 布局另占 1 个 main 槽。不按 CPU 数推导，
 *   也不自动扩容。
 * - max_inbound_bytes 与 max_outbound_bytes 是每连接、每方向的资源总量界。max_pending_events
 *   只约束单个连接上的操作责任数，不是 Engine 所有队列的统一上限，其它内部硬界仍是实现限制。
 * - reuse_port 只映射为 SO_REUSEPORT，本身不决定连接如何在 loop 之间分布。其余 socket 字段直接
 *   映射为同名 socket 选项。
 */
struct Options {

    /**
     * 连接分配到 sub owner 时使用的策略
     */
    LoadBalancing lb{LoadBalancing::RoundRobin};

    /**
     * 映射为 SO_REUSEADDR
     */
    bool reuse_addr{false};

    /**
     * 映射为 SO_REUSEPORT
     */
    bool reuse_port{false};

    /**
     * sub owner 数量；0 视为 1，负值拒绝
     */
    std::int64_t num_event_loop{0};

    /**
     * keepalive 空闲时长；0 禁用；负值或超出原生 int 秒范围时拒绝
     */
    std::chrono::seconds tcp_keep_alive{0};

    /**
     * 探测间隔；负值拒绝；keepalive 启用且为 0 时取 idle/5，最小 1 秒
     */
    std::chrono::seconds tcp_keep_interval{0};

    /**
     * 探测次数；负值或超出原生 int 拒绝；keepalive 启用且为 0 时取 5
     */
    std::int64_t tcp_keep_count{0};

    /**
     * TCP_NODELAY，默认开启
     */
    bool tcp_no_delay{true};

    /**
     * SO_RCVBUF；0 表示不设置；负值或超出原生 int 拒绝
     */
    std::int64_t socket_recv_buffer{0};

    /**
     * SO_SNDBUF；0 表示不设置；负值或超出原生 int 拒绝
     */
    std::int64_t socket_send_buffer{0};

    /**
     * 每连接读方向的总量界，必须为正
     */
    std::int64_t max_inbound_bytes{1024 * 1024};

    /**
     * 每连接写方向的总量界，必须为正
     */
    std::int64_t max_outbound_bytes{1024 * 1024};

    /**
     * 每连接的操作责任上限，必须为正
     */
    std::int64_t max_pending_events{1024};
};

// clang-format off
}
// clang-format on
