#pragma once
// bbtools-infra v2：RPC lane/owner 观测值类型。
// 契约来源：docs/decisions/0014-app-session.md §2.1/§2.3。
//
// 快照语义：Client::Stats()/Server::Stats() 按值返回；累计字段单调增加，
// 当前字段是取样时 gauge。MonitorService 只能消费这些 infra 事实。
// queued/dequeued/inflight/finished 职责（ADR §2.3）：
//   queued    — 有限队列尚未交付 dispatch/mailbox；
//   dequeued  — 已交给 framework dispatch/mailbox；
//   inflight  — 已交 admission、尚未到责任终态（Call 含业务处理与回复写终态；
//               Send 到 mailbox 接纳与所需 SendAccepted 写终态）；
//   finished  — 对应责任有终态后累计；
//   dropped   — 过载、旧 epoch 或显式丢弃；
//   bytes_queued — 当前排队字节数。

#include <cstdint>

namespace bbt::infra::rpc {

struct TrafficStats {
    std::uint64_t queued       = 0;
    std::uint64_t dequeued     = 0;
    std::uint64_t inflight     = 0;
    std::uint64_t finished     = 0;
    std::uint64_t dropped      = 0;
    std::uint64_t bytes_queued = 0;
};

struct ClientStats {
    std::uint64_t sessions                   = 0;
    std::uint64_t links                      = 0;
    std::uint64_t remote_acceptance_pending  = 0;
    TrafficStats  system;
    TrafficStats  service;
    TrafficStats  actor;
};

struct ServerStats {
    std::uint64_t accepted_connections = 0;
    std::uint64_t active_connections   = 0;
    std::uint64_t retiring_connections = 0;
    std::uint64_t callbacks            = 0;
    TrafficStats  system;
    TrafficStats  service;
    TrafficStats  actor;
};

} // namespace bbt::infra::rpc
