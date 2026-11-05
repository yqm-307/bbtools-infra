#pragma once

/**
 * @file Engine.hpp
 * @brief 公共 Engine：宿主控制线程上的网络执行域（listener、连接、Co 消费面）。
 *
 * `Engine::Run` 占调用它的宿主 native 控制线程，自动绑定已登记 Processer 槽，
 * 每个 Reactor 由对应 P 的 driver 推进自有 Asio context；不新建库内控制线程或
 * 网络 I/O 线程。生命周期与取舍见 docs/architecture/gnet-network-execution.md。
 *
 * 生命周期与判定顺序：
 *   - `Run` 先校验宿主控制域：在协程上下文或已登记 Processer 上调用返回
 *     operation_not_permitted；runtime 未初始化返回 operation_not_supported；
 *     未声明 `RuntimeProgress::Independent` 返回 operation_not_permitted。
 *     这些判断都在 driver 绑定与 socket 副作用之前。
 *   - server 布局（`listen_addresses` 非空）要求 handler 与 handoff 至少一个
 *     存在；两者都无时 `Run` 返回 operation_not_permitted，同样在副作用之前。
 *   - 成功后自动绑槽（server 1 main + N sub；client-only 无 main），安装
 *     listener，发布 Running（此时 `LocalAddresses` 才可能有完整语义），进入
 *     控制循环。
 *   - `RequestClose` 并发安全、幂等，只封口并唤醒控制域；`Run` 返回才表示本
 *     Engine 的网络资源、回调与 driver 已收口。
 *   - 控制域按持有 token 有限复查 `QueryProcesserDriver`；Failed 时封口、同代
 *     解绑，以冻结状态、`LastError`/`LastFailure` 与内部完成订阅作为替代出口，
 *     不伪调 owner 的用户回调（包括不伪报建连成功/失败）。
 *
 * 原始回调在 owner 的非业务协程边界执行：短、非阻塞、不可挂起。运行时对象
 * （Scheduler/driver/Processer）由宿主负责，本类不取消它们的寿命。
 */

#include <cstddef>
#include <exception>
#include <cstdint>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/object/CoObject.hpp>

#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/ConnectionResult.hpp>
#include <bbt/infra/net/Options.hpp>
#include <bbt/infra/net/RawHandler.hpp>

namespace bbt::infra::net {

/**
 * @brief Engine 的阶段。
 *
 * Starting 与 Closing 是过渡态，不承诺持续时间。
 */
enum class EnginePhase { Idle, Starting, Running, Closing, Stopped };

/**
 * @brief 宿主对 runtime 推进方式的必填声明。
 *
 * `Independent` 表示 runtime 内部 EventLoop 由独立于 `Run` 的执行流持续推进
 * （THREAD 的 S 线程 / SCHE_LOOP 另一线程 / NO_LOOP 另一执行流持续 LoopOnce）。
 * 未声明时在绑定前拒绝，避免「Run 阻塞了唯一推进者」这类静默死锁。
 */
enum class RuntimeProgress { Unspecified, Independent };

/**
 * @brief `Run` 的输入：监听地址、网络选项与推进方式声明。
 */
struct EngineConfig {
    std::vector<Address> listen_addresses;                          ///< 空表示 client-only，不创建 main accept owner
    Options options;                                                ///< 网络选项，见 Options
    RuntimeProgress runtime_progress{RuntimeProgress::Unspecified}; ///< 必须为 Independent，否则 Run 拒绝
};

struct EngineImpl;    ///< 定义在 Engine.cc，布局不出公共面
struct ReclaimRecord; ///< detail：物理回收共享释放记录，带句柄 CoDial 内部传递

class EngineTestAccess;
struct CoAccess;

/**
 * @brief 网络执行域：listener、连接台账与 Co 消费面。
 *
 * 一个 Engine 对应一代网络资源。「代」是唯一的重启单位：`Run` 成功进入
 * Running，`Run` 返回即本代全部资源收口；同一实例不能重新开启。
 *
 * 线程与域约束见各成员的注释：`RequestClose`、`RequestStopAccepting` 等可从任意线程
 * 调用，`Run` 只能在宿主的 native 控制线程上调用。
 */
class Engine : public bbt::coroutine::CoObject {
public:
    /**
     * @brief 推荐的创建入口。
     *
     * 经 coroutine `MakeCoObject` 的唯一 creator 构造，身份初始化与 created/destroyed
     * 统计在工厂提交后才记账。Engine 是 native 域对象，原生线程可直接调用，不要求协程上下文。
     * 构造或登记失败沿工厂的异常路径传播，且不提交计数。
     */
    template <class... Args> static std::shared_ptr<Engine> Create(Args&&... args)
    {
        return bbt::coroutine::MakeCoObject<Engine>(
            "infra.net.Engine", "", [&] { return std::shared_ptr<Engine>(new Engine(std::forward<Args>(args)...)); });
    }

    /**
     * @brief 直接构造。生产代码应使用 `Create()`，以便身份初始化与统计进入统一观测。
     *
     * @param handler raw 接收者，可为空。client-only 布局允许没有 raw 接收者（纯 Co 消费）；
     *        server 布局必须另有 handoff 接收者，见 `Run`。
     */
    explicit Engine(std::shared_ptr<RawHandler> handler);
    ~Engine();
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    /**
     * @brief 阻塞的宿主控制线程入口；本代成功收口后返回。
     *
     * 前置校验失败时在绑定与 socket 副作用之前返回。
     *
     * @param config 监听地址、网络选项与推进方式声明。
     * @return 空表示正常收口；否则为前置校验或启动失败的原因。
     */
    std::error_code Run(const EngineConfig& config);

    /**
     * @brief 请求关闭。任意线程可调用，非阻塞、幂等。
     *
     * 只封口并唤醒控制域；收口凭据是 `Run` 返回。
     */
    void RequestClose() noexcept;

    /** @brief 当前阶段快照。 */
    EnginePhase Phase() const noexcept;

    /**
     * @brief 启动失败或 `Run` 收口的错误。
     *
     * 无错误时为空。可在 `Run` 返回后查询。
     */
    std::error_code LastError() const;

    /**
     * @brief driver 进入 Failed 时保留的异常。
     *
     * 无异常时为空。不伪调 owner 用户回调。
     */
    std::exception_ptr LastFailure() const;

    /**
     * @brief 实际绑定地址快照。端口为 0 时返回内核分配的端口。
     *
     * - Running/Closing：返回本代快照。
     * - Starting：可能返回当前快照，也可能尚未安装 listener 而为空。空结果不代表
     *   「无监听」，完整语义只在 Running 之后成立。
     * - Idle/Stopped：返回 operation_not_permitted，且不改写 `out`。
     */
    std::error_code LocalAddresses(std::vector<Address>& out) const;

    /**
     * @brief 当前在册的连接数。
     *
     * 计数包含建立中、尚未收口的连接；注册时加一，移除时减一。
     */
    std::size_t CountConnections() const;

    /**
     * @brief 用户原始回调抛出异常的累计次数。
     *
     * 这些异常在调用点被隔离计数，属于用户回调故障，不是 Engine 故障，不影响 `Run` 的返回值。
     */
    std::size_t HandlerErrorCount() const;

    /**
     * @brief 观测用：本代各 owner 绑定的 ProcesserId。
     *
     * server 的 main 是 `main_owners` 的第一项；client-only 的 `main_owners` 为空。
     * 未运行时返回空。
     */
    std::error_code ReactorOwnerIds(std::vector<std::uint64_t>& main_owners,
                                    std::vector<std::uint64_t>& sub_owners) const;

    /**
     * @brief 非阻塞建连，只用于配置了 RawHandler 的 Engine。建连结果只通过 raw 回调交付。
     *
     * - 无 handler 时，提交前返回 operation_not_supported，且不修改 `out_connect_id`。
     *   协程建连走 `CoDial`，不公开中间 ID。
     * - 返回成功只表示本次提交已被接纳。`out_connect_id` 为本次非零操作 ID，
     *   且只在本 Engine 内唯一，不是取消或等待句柄。
     * - 正常推进下，成功或失败由 `OnConnect` 恰好交付一次，回调中的 operation 等于
     *   返回的 `out_connect_id`。回调可能先于本函数返回，调用方应以回调自带的 operation 关联结果。
     * - 任何拒绝路径（无 handler、非 Running 或已封口、容量满、投递失败）都保持
     *   `out_connect_id` 原值，且不产生本次 `OnConnect`。
     * - driver 进入 Failed 时不伪调 owner 回调，只能通过 `LastError`/`LastFailure` 与收口状态观测。
     */
    std::error_code AsyncConnect(const Address& address, std::uint64_t& out_connect_id);

    /**
     * @brief 在 `Run` 之前开启 CoAccept handoff，并预留 `capacity` 个有界槽位。
     *
     * capacity 为 0 时返回 invalid_argument；非 Idle 阶段调用返回 device_or_resource_busy。
     * 与 raw handler 可以共存：同一连接既会触发 `OnAccept`，也会进入 handoff 队列。两个读取者同时存在时，
     * 只保证字节前缀的线性化，不做自动路由。
     */
    std::error_code EnableAcceptHandoff(std::size_t capacity);

    /**
     * @brief 只停止监听与 handoff 的接受，可从任意线程调用，非阻塞、幂等，且不可重新开启。
     *
     * 只封本代的 listener 与 handoff。它不封已交付连接的 Read 与 Write，不封 `CoDial`，
     * 也不停止 driver，不调用 `RequestClose`，不改动已交付的连接。`Run` 之后，已交付连接的
     * 双向读写继续可用，宿主需另行调用 `RequestClose` 并 join。
     *
     * 请求本身不是物理关闭的凭据。只有 `AcceptingStopped` 为 true 时，本代全部 listener
     * 才已真正关闭（或在 Failed 时由控制域同代解绑收口），handoff 已封口，且等待者已唤醒。
     * `Run` 之前发出的请求不允许之后再开启监听。重复或并发的请求都不会重新开启。
     */
    void RequestStopAccepting() noexcept;

    /**
     * @brief 本代全部 listener 已物理关闭、handoff 已封口且等待者已唤醒时为 true；默认 false。
     *
     * 它只证明 listener 已停止，不证明已交付或未交付的 fd 与业务任务已释放。后者仍需
     * 依赖 `Connection::Released()` 及 Engine 的最终收口。
     */
    bool AcceptingStopped() const noexcept;

    // ---- Co 成员入口 ---------------------------------------------------------
    // 协程消费面以成员函数的形式提供，判定优先级见 CoConnection.hpp：执行域 gate 先于
    // 一切状态判定；终态（sealed/Closing/Stopped → operation_canceled）先于 Idle/Starting
    // 与 client-only；失败不覆盖 result.connection，reclaim 按提交阶段描述本次 socket 责任。
    // 实现落在 Engine.cc，经私有步骤 CoSubmitConnect/CoWaitConnect/CoWaitAccepted 完成。

    /** @brief 取一条已接纳的连接。必须在受管协程内调用，可挂起。 */
    ConnectionResult CoAccept(const bbt::coroutine::WaitOptions& options = {});

    /** @brief 拨号并等待结果。必须在受管协程内调用，可挂起。 */
    ConnectionResult CoDial(const Address& address, const bbt::coroutine::WaitOptions& options = {});

private:
    friend class EngineTestAccess;
    friend struct CoAccess;

    // ---- Co 内部步骤（仅由 detail/CoInternal.hpp 调用）-----------------------
    // 两段式建连是 CoDial 的内部细节：提交（登记接收意图）与领取（等待结果）。
    // 不公开，避免把一个无法放弃的中间 ID 变成公共契约。

    std::error_code CoSubmitConnect(const Address& address, std::uint64_t& out_connect_id);

    /// 带责任记录的提交，仅供带句柄的 CoDial 使用。记录随提交经过 owner 的各个真实
    /// close/转交点；未交付的失败由 owner 的回收路径收口该记录。
    std::error_code CoSubmitConnect(const Address& address, std::uint64_t& out_connect_id,
                                    const std::shared_ptr<ReclaimRecord>& reclaim);

    /// 领取结果。仅成功时把本次结果交付到 `out`；失败不修改 `out`。调用方必须在本函数内
    /// 归还 claim 与登记责任，并让 owner 回收未交付的 fd。
    std::error_code CoWaitConnect(std::uint64_t connect_id, Connection& out,
                                  const bbt::coroutine::WaitOptions& options);

    /// 从有界 handoff 队列取已接纳的连接。配置或生命周期错误立即返回，不等待。
    /// 判定优先级见 CoConnection.hpp 中 CoAccept 的注释：终态（sealed/Closing/Stopped →
    /// operation_canceled）先于 Idle/Starting 与 client-only 的判定。
    std::error_code CoWaitAccepted(Connection& out, const bbt::coroutine::WaitOptions& options);

    std::shared_ptr<EngineImpl> m_impl;
};

} // namespace bbt::infra::net
