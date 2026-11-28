/**
 * @file ClientState.hpp
 * @brief Consul Client 的共享状态：并发收口、关闭发布与单次 HTTP 交换。
 *
 * 该状态由 shared_ptr 持有，操作可以比公开句柄活得更久：Close 等待期间 Client
 * 析构也不会悬空。并发/关闭不变量与公开契约一致（见 consul/Client.hpp 的
 * Client 契约注释），此处只写实现级约束：
 *   - inflight/http_closed/immediate/terminal/reason 都由 mutex 保护；closed 是
 *     原子封口位，IsClosed 无锁可读；
 *   - 三阶段发布顺序固定：在途归零（MarkSettledLocked）→ 专属 HTTP Client 封口
 *     （BeginHttpCloseLocked/CompleteHttpClose）→ 终态（MarkTerminalLocked，
 *     封口 && http_closed && inflight==0，单调不可逆）；
 *   - 锁内只登记与转移等待者，Notify/notify_all 一律锁外执行；
 *   - Immediate 后与结果交付共用 mutex：迟到结果只能消费，不能交付成功。
 *
 * 成员一律私有：跨线程不变量不允许按字段组合改动，入口只有 Client 及其嵌套
 * OpScope 的固定路径。本头是 consul 模块内部 detail，不属于公共 API，也不进
 * 公开 target 的接口面。
 */
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/infra/consul/Client.hpp>
#include <bbt/infra/http/Client.hpp>

namespace bbt::infra::consul {

struct HealthDeliveryPause;

/**
 * @brief 在飞台账与关闭状态机；操作经 OpScope 进入/离开，Close 等待同一谓词。
 */
class Client::State {
public:
    State(std::shared_ptr<bbt::infra::http::Client> client, Options opts);

    State(const State&) = delete;
    State& operator=(const State&) = delete;

private:
    friend class Client;
    friend struct ClientTestAccess;

    /**
     * 单次 HTTP 交换：token 头、分阶段超时、总期限在此统一收口，业务路径只给
     * 方法与 target（target 以 '/' 开头的 origin-form）。
     * 调用方持有 OpScope 并立即等待，保证 HTTP Client 活到交换结束。
     */
    bbt_co_async(result<http::Response>) Send(
        std::string_view method,
        const std::string& target,
        std::string body,
        std::chrono::milliseconds read_budget) const;

    /**
     * 阻塞查询的读档必须容纳 Consul 允许的 wait/16 附加抖动，否则「阻塞返回」
     * 会先被读档超时打断；非阻塞快照使用整个 request_timeout。
     */
    std::chrono::milliseconds ReadBudgetForQuery(bool blocking) const;

    std::shared_ptr<HealthDeliveryPause> TakeHealthDeliveryPause();

    /**
     * 终态发布（锁内调用）：仅当三条件同时成立才置位，并摘下全部协程等待者。
     * 返回 true 表示本次发布；woken 供调用方在锁外 Notify（不持锁唤醒）。
     */
    bool MarkTerminalLocked(std::vector<bbt::coroutine::sync::CoWaiter::SPtr>* woken);

    /**
     * 在途归零发布（锁内调用）：Graceful 只等这个谓词，不等专属 HTTP Client 的
     * 物理退役；终态未成立时被唤醒者复查后自行重新登记。
     */
    bool MarkSettledLocked(std::vector<bbt::coroutine::sync::CoWaiter::SPtr>* woken);

    /**
     * Graceful 等已接纳操作离开后才关闭专属 HTTP Client；Immediate 立即中断交换，
     * 但操作的状态和 owner 仍保留到各自安全退出。
     */
    bool BeginHttpCloseLocked();

    /**
     * 只关闭专属 HTTP Client，不关共享 Engine；Immediate 可在操作仍在途时进入，
     * 物理凭据仍须等操作退出和 HTTP 连接真实退役。
     */
    void CompleteHttpClose() noexcept;

    /**
     * 锁外唤醒：宿主线程 cv + 协程等待者。Notify 对已决议/已解绑的 waiter 返回
     * -1，无副作用，可安全重复。
     */
    void WakeAll(const std::vector<bbt::coroutine::sync::CoWaiter::SPtr>& woken) noexcept;

    const std::shared_ptr<bbt::infra::http::Client> http;
    const Options options;
    std::string base;

    mutable std::mutex mutex;

    std::weak_ptr<HealthDeliveryPause> health_delivery_pause;

    std::condition_variable idle;

    std::vector<bbt::coroutine::sync::CoWaiter::SPtr> co_waiters;
    std::size_t inflight{0};

    std::atomic<bool> closed{false};

    bool http_closed{false};

    bool immediate{false};

    bool http_close_started{false};

    bool terminal{false};

    std::optional<Error> reason;
};

// clang-format off
}
// clang-format on
