#pragma once

/**
 * RPC lane/owner 观测值类型。
 * 契约来源：docs/decisions/0014-app-session.md §2.1/§2.3。
 *
 * 取样语义（重要，不要按原子快照用）：
 *   CoRpcClient::Stats()/CoRpcServer::Stats() 按值返回一次「汇总取样」，不是同一时刻
 *   的一致快照。实现分多把锁/多个 atomic 分步读取：
 *     * owner 级 gauge（sessions/links/acceptance_pending）各自独立 atomic；
 *     * Client lane 计数逐个 session 取 out_mu 后累加；
 *     * Server 持会话台账锁取台账，但 queue 的 queued/bytes_queued 逐个经
 *       AdmissionQueue 自己的锁读（Queued(i) 与 BytesQueued(i) 是两次独立
 *       取锁）。
 *   因此字段之间可能相差若干次调度：queued>0 与 bytes_queued==0 同时出现
 *   是合法取样结果（worker 在两次读取之间出队）。按当前 session 台账汇总的
 *   lane 字段会在 session 退役后下降；当前字段是各自取样点的 gauge。
 *
 * 使用边界：只把这些数当趋势与容量观察。不要用单次取样的跨字段等式/不等式
 * 判定泄漏、收口或「队列为空」；需要确定性的收口判断时用 Close 各模式的返回值，或用 detail::ServerV2Ledger 的台账快照。
 *
 * queued/dequeued/inflight/finished 职责（ADR 0014 §2.3）：
 *   queued    — 有限队列尚未交付 dispatch/mailbox；
 *   dequeued  — 已交给 framework dispatch/mailbox；
 *   inflight  — 已交 admission、尚未到责任终态（Call 含业务处理与回复写终态；
 *               Send 到 mailbox 接纳与所需 SendAccepted 写终态）；
 *   finished  — 对应责任有终态后累计；
 *   dropped   — 过载、旧 epoch 或显式丢弃；
 *   bytes_queued — 当前排队字节数。
 */
#include <cstdint>

namespace bbt::infra::rpc {

/**
 * @brief 单通道（lane）流量计数。各字段语义见文件头注释。
 *
 * 各字段由不同的取样点汇总而来，不承诺彼此属于同一时刻。
 */
struct TrafficStats {

    /**
     * 有限队列尚未交付 dispatch/mailbox 的条目数（gauge）
     */
    std::uint64_t queued = 0;

    /**
     * 已交给 dispatch/mailbox 的累计条目数
     */
    std::uint64_t dequeued = 0;

    /**
     * 已交 admission、尚未到达责任终态的条目数（gauge）
     */
    std::uint64_t inflight = 0;

    /**
     * 已有终态的累计条目数
     */
    std::uint64_t finished = 0;

    /**
     * 过载、旧 epoch 或显式丢弃的累计条目数
     */
    std::uint64_t dropped = 0;

    /**
     * 当前排队字节数（gauge）
     */
    std::uint64_t bytes_queued = 0;
};

/**
 * @brief Client owner 观测值（`CoRpcClient::Stats()` 按值返回）。
 *
 * 一次汇总取样，不是同一时刻的一致快照：sessions/links/
 * remote_acceptance_pending 是三个独立 atomic，lane 计数是逐 session 取锁
 * 累加的结果。lane 字段是当前 session 台账的汇总，session 退役后可能下降；
 * 当前字段是各自取样点的 gauge；跨字段等式不成立（见文件头「取样语义」）。
 * ADR 0014 §2.3。
 */
struct ClientStats {

    /**
     * 当前在台账中的会话数（gauge）
     */
    std::uint64_t sessions = 0;

    /**
     * 当前活跃双向 link 数（gauge）
     */
    std::uint64_t links = 0;

    /**
     * 等待远端 SendAccepted 的条目数（gauge）
     */
    std::uint64_t remote_acceptance_pending = 0;

    /**
     * 系统 lane（握手/控制帧）
     */
    TrafficStats system;

    /**
     * Service lane
     */
    TrafficStats service;

    /**
     * Actor lane
     */
    TrafficStats actor;
};

/**
 * @brief Server owner 观测值（`CoRpcServer::Stats()` 按值返回）。
 *
 * 一次汇总取样，不是同一时刻的一致快照：连接与 lane 累计在会话台账锁内取，
 * queue 的 queued/bytes_queued 逐个经 AdmissionQueue 自己的锁读，两者之间
 * 可能有 worker 出队。当前字段是各自取样点的 gauge，跨字段等式不成立。
 * 逐 peer/epoch 明细不在本结构，见 ADR 0014 §2.3。
 */
struct ServerStats {

    /**
     * 累计已接受的连接数
     */
    std::uint64_t accepted_connections = 0;

    /**
     * 当前活跃连接数（gauge）
     */
    std::uint64_t active_connections = 0;

    /**
     * 当前正在退役的连接数（gauge）
     */
    std::uint64_t retiring_connections = 0;

    /**
     * 累计 handler 回调次数
     */
    std::uint64_t callbacks = 0;

    /**
     * 系统 lane
     */
    TrafficStats system;

    /**
     * Service lane
     */
    TrafficStats service;

    /**
     * Actor lane
     */
    TrafficStats actor;
};

// clang-format off
}
// clang-format on
