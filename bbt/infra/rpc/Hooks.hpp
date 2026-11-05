#pragma once
// RPC：owner 实例级的受控检查点。
//
// 职责：给 writer 与握手路径各一个固定的诊断/故障位置，生产与测试共用同
// 一条代码路径。它不是观察者回调——库不在关键路径上执行任何用户代码，
// 每个点只有两种由库实现的动作：Pause 与 ThrowOnce。
//
// 为什么公开：这些阶段（帧已出队但未提交写、conn 已建立但 link 未发布）
// 是使用者做时序诊断、验证故障结算时唯一无法从外部精确命中的窗口。藏成
// ForTest 私有接缝只会逼出第二套实现，测试也就无法证明生产路径。
//
// 作用域与寿命：配置挂在 owner 上，同一 owner 下的所有 link 共用一份，
// 不同 owner 完全隔离。只经 Client::Hooks() 取得；handle 只持有有界的
// 控制状态，不延长 owner、Engine、连接或业务对象的寿命——owner 收口后
// handle 仍可安全查询，只是永久 closed。
//
// 注册与注销：
//   * Arm 返回该点的单调 generation；Clear 只解除匹配的那次注册，旧 token
//     不会清掉后来的配置，重复/过期 token 是成功的 no-op；
//   * 替换产生新 generation，旧在途命中只退出旧 generation，不追随新配置；
//   * Clear 只更新状态，不等在途退出、不做排空；需要排空证据的调用方在
//     自己的 deadline 内轮询 Snapshot 的 in_flight，而不是在 strict 协程
//     里等自己；
//   * 注册窗口最长 10s，过期未命中的注册（含 ThrowOnce）自动失效。
//
// 执行域与等待：四个点都在现有受管协程内、相关锁已释放之后进入。唯一允许
// 的挂起是库自己的 CoWaiter：每个在途 Pause 独占一个等待者，按 10ms 分片
// 检查注册状态、owner/server/link/conn 关闭、协程取消与原操作期限，并在
// 每片重新计算 min(操作期限, 注册到期, now + 10ms)。这里只承诺库控制的
// 等待，不声称 timeout 能中止任意 C++ 代码；解除后仍走原封口检查与安装/
// 提交事务，不重置业务期限、不伪造写成功。
//
// 异常边界（由各调用点结算，不是本类）：
//   * BeforeWrite 抛出 → 本条已出队写责任结算为 InternalError，link 退役；
//   * OutboundBeforeHandshake 抛出 → 本次建立返回 InternalError，建立中
//     记录/额度/conn 由建立路径统一归还；
//   * InboundBeforeHandshake / InboundBeforePublish 抛出 → 该入站连接退役，
//     不发布 link。
// ThrowOnce 抛的是库内部固定标签，不接受用户异常对象或消息；调用点把它
// 转成可见错误并完成资源结算。

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <mutex>

#include <bbt/coroutine/sync/WaitTypes.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra::rpc {

struct SessionOwnerState;

namespace detail {
class SessionHookAccess;
} // namespace detail

/**
 * @brief 检查点位置：每个值对应生产路径上一处固定调用点。
 *
 * 不对使用者暴露回调次数或帧内容；入站握手前与发布前是两个独立点，不需要
 * 靠调用计数猜阶段。
 */
enum class HookPoint {
    BeforeWrite,             ///< writer 已出队、尚未标记提交/写 conn 之前。
    OutboundBeforeHandshake, ///< 出站 conn 已建立、握手 I/O 尚未开始。
    InboundBeforeHandshake,  ///< 入站 conn 已 accept、握手 I/O 尚未开始。
    InboundBeforePublish,    ///< 入站握手完成、link 尚未安装/发布。
};

/**
 * @brief 检查点动作：由库实现，不接收用户代码。
 */
enum class HookAction {
    Pause,     ///< 在库的受管协程内分片等待，直到解除或操作终态。
    ThrowOnce, ///< 由库抛出固定内部标签一次；调用点转成可见错误并结算。
};

/**
 * @brief 单个检查点的控制状态快照。
 *
 * 是控制状态的小值快照，不是 RPC 全局一致快照，也不代表业务成功或物理
 * 释放。`entered` 统计当前 generation 命中的次数，`in_flight` 统计跨
 * generation 尚未退出的命中次数（Clear 返回不等于 in_flight 为零）。
 */
struct HookSnapshot {
    std::uint64_t generation{0};          ///< 生成代号；Clear 递增
    HookAction action{HookAction::Pause}; ///< 当前故障动作
    bool armed{false};                    ///< 是否已布防
    bool closed{false};                   ///< 控制器是否已封口
    std::uint64_t entered{0};             ///< 当前 generation 命中的次数
    std::uint64_t in_flight{0};           ///< 跨 generation 尚未退出的命中次数
};

/**
 * @brief 单个会话 owner 上的受控检查点控制器。
 *
 * 由 Client::Hooks() 取得；同一 owner 下的所有 link 共享，不同 owner 各自
 * 独立。构造与封口都是库内部能力，使用方只做 Arm/Clear/Snapshot。
 */
class SessionHooks final {
public:
    SessionHooks(const SessionHooks&) = delete;
    SessionHooks& operator=(const SessionHooks&) = delete;

    /**
     * @brief 注册一个固定动作，返回该点的 generation token。
     *
     * 窗口必须是 (now, now + 10s]：拒绝过去时间、time_point::max、超过上限
     * 或未知的 point/action。同一 owner 封口后返回 Closed；generation 耗尽
     * 时拒绝注册（不回绕）。新注册会解除旧 generation 的 Pause，旧在途命中
     * 只退出旧 generation。
     */
    result<std::uint64_t> Arm(HookPoint point, HookAction action, bbt::coroutine::Deadline expires_at)
    {
        const auto index = IndexOf(point);
        if (index == kHookPointCount) {
            return result<std::uint64_t>::err(MakeError(ErrorCode::InvalidArgument, "rpc hook: unknown hook point"));
        }
        if (action != HookAction::Pause && action != HookAction::ThrowOnce) {
            return result<std::uint64_t>::err(MakeError(ErrorCode::InvalidArgument, "rpc hook: unknown hook action"));
        }
        const auto now = std::chrono::steady_clock::now();
        if (expires_at == bbt::coroutine::Deadline::max() || expires_at <= now || expires_at > now + kMaxArmLifetime) {
            return result<std::uint64_t>::err(
                MakeError(ErrorCode::InvalidArgument, "rpc hook: arm window must be (now, now + 10s]"));
        }
        std::lock_guard<std::mutex> lock(m_mu);
        if (m_closed) {
            return result<std::uint64_t>::err(MakeError(ErrorCode::Closed, "rpc hook: owner closed"));
        }
        auto& state = m_points[index];
        if (state.generation == std::numeric_limits<std::uint64_t>::max()) {
            return result<std::uint64_t>::err(
                MakeError(ErrorCode::RuntimeUnavailable, "rpc hook: generation exhausted"));
        }
        ++state.generation;
        state.action = action;
        state.expires_at = expires_at;
        state.armed = true;
        state.entered = 0;
        return result<std::uint64_t>::ok(state.generation);
    }

    /**
     * @brief 解除匹配 generation 的注册；不等待在途命中退出。
     *
     * 重复、已过期或不属于当前配置的 token 是成功的 no-op，不会清掉后来的
     * 注册。未知 point 返回 InvalidArgument。
     */
    result<void> Clear(HookPoint point, std::uint64_t generation)
    {
        const auto index = IndexOf(point);
        if (index == kHookPointCount) {
            return result<void>::err(MakeError(ErrorCode::InvalidArgument, "rpc hook: unknown hook point"));
        }
        std::lock_guard<std::mutex> lock(m_mu);
        auto& state = m_points[index];
        if (!m_closed && state.armed && state.generation == generation) {
            state.armed = false;
        }
        return result<void>::ok();
    }

    /**
     * @brief 读取该点当前的控制状态。
     *
     * 只返回固定值：generation/action/armed/closed/entered/in_flight，没有
     * 连接、帧或业务对象引用。未知 point 返回 InvalidArgument。
     */
    result<HookSnapshot> Snapshot(HookPoint point) const
    {
        const auto index = IndexOf(point);
        if (index == kHookPointCount) {
            return result<HookSnapshot>::err(MakeError(ErrorCode::InvalidArgument, "rpc hook: unknown hook point"));
        }
        std::lock_guard<std::mutex> lock(m_mu);
        const auto& state = m_points[index];
        HookSnapshot out;
        out.generation = state.generation;
        out.action = state.action;
        out.armed = state.armed;
        out.closed = m_closed;
        out.entered = state.entered;
        out.in_flight = state.in_flight;
        return result<HookSnapshot>::ok(out);
    }

private:
    friend class detail::SessionHookAccess;
    friend struct SessionOwnerState;

    static constexpr std::size_t kHookPointCount = 4;
    /// 诊断注册的绝对上限：不是性能保证，也不提供配置开关。
    static constexpr std::chrono::milliseconds kMaxArmLifetime{10000};
    /// 在途 Pause 的检查片长；每片都重算 min(操作期限, 注册到期, 片长)。
    static constexpr std::chrono::milliseconds kPauseSlice{10};

    struct PointState {
        std::uint64_t generation{0};
        HookAction action{HookAction::Pause};
        bbt::coroutine::Deadline expires_at{bbt::coroutine::Deadline::max()};
        bool armed{false};
        std::uint64_t entered{0};
        std::uint64_t in_flight{0};
    };

    SessionHooks() = default;

    static constexpr std::size_t IndexOf(HookPoint point) noexcept
    {
        switch (point) {
        case HookPoint::BeforeWrite:
            return 0;
        case HookPoint::OutboundBeforeHandshake:
            return 1;
        case HookPoint::InboundBeforeHandshake:
            return 2;
        case HookPoint::InboundBeforePublish:
            return 3;
        }
        return kHookPointCount;
    }

    mutable std::mutex m_mu;
    PointState m_points[kHookPointCount];
    bool m_closed{false};
};

} // namespace bbt::infra::rpc
