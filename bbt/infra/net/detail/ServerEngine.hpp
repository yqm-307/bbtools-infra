#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <memory>
#include <mutex>
#include <system_error>
#include <vector>

#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/steady_timer.hpp>

#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/EventHandler.hpp>
#include <bbt/infra/net/LoadBalancing.hpp>
#include <bbt/infra/net/Options.hpp>
#include <bbt/infra/net/detail/ConnectionBridge.hpp>
#include <bbt/infra/net/detail/EventHandlerAdapter.hpp>
#include <bbt/infra/net/detail/EventLoop.hpp>

namespace bbt::infra::net {

// 当前执行环境是否受管协程（每次现查；runtime 未启动时为 0，不创建 Processer）。
bool InManagedCoroutine();

// 首包 Engine 私有实现：main owner（listener + LB）+ N 个 sub owner
// （各自 EventLoop + 私有 ConnectionBridge）。控制面（Run/Stop/析构/观测）
// 由 control_mutex_ 串行化；网络面只在各自 owner 线程通过命令推进。
//
// 相位（都在 control_mutex_ 下写；同一短锁同时承担状态与网络资源职责）：
//   Idle     未启动；
//   Starting 已起 owner、已绑定 listener、bound 快照已发布，正在等 OnBoot 完成
//            （Run 已释放 control_mutex_，不持锁等待）；
//   Running  已挂 accept 就绪等待，业务可接纳；
//   Stopped  终态（网络已真实收口；此后 Run → operation_canceled）。
// Starting/Running 都允许观测；Starting 期间 Stop 可真实收口，不等 OnBoot 结束。
class ServerEngine final : public std::enable_shared_from_this<ServerEngine> {
public:
    static std::shared_ptr<ServerEngine> Create(std::shared_ptr<EventHandler> handler,
        std::error_code& error);
    ~ServerEngine();
    ServerEngine(const ServerEngine&) = delete;
    ServerEngine& operator=(const ServerEngine&) = delete;

    // 非阻塞启动；失败回落（已起 owner/listener 全部回收），Idle 可重试。
    // Boot 等待在锁外进行：并发 Stop 可首胜并真实收口，Run 随后见终态返回取消。
    std::error_code Run(const Address& address, const Options& options);
    // native 强停；未启动/已停止幂等成功（未启动同样进入终态）。返回真实错误，不报假成功。
    std::error_code Stop();
    // 析构域专用：跳过域检查（调用方保证合法控制域），未运行则只回收资源。
    std::error_code ForceStopForDestruction();
    // Starting（已发布绑定快照）与 Running → 实际地址；未运行/已停止 →
    // operation_not_permitted 且不改写 out。
    std::error_code LocalAddresses(std::vector<Address>& out) const;
    std::size_t CountConnections() const;

private:
    enum class Phase { Idle, Starting, Running, Stopped };

    // 每 Run 的窄 Boot 结果：只承载「未开始 / 已开始 / 已完成 / 已放弃」判定与首胜唤醒，
    // 不捕 self/网络资源，不构成通用取消框架。读写一律在 mutex 下进行。
    struct BootResult {
        std::mutex mutex;
        std::condition_variable cv;
        bool abandoned{false};   // Stop/超时已放弃：未开始的任务不得进入 OnBoot
        bool started{false};     // 已完成 Pending→Running 的窄 CAS
        bool done{false};
        bool ok{false};
    };

    // 每 Run 一代的窄 ticker 状态：只承载「开始授权 / 停投原因」与一个稳定的 owner
    // 投递句柄，不构成通用取消框架，也不承载网络资源。由控制线程（Run/Stop/rollback）
    // 与在途 tick 任务按值共同持有；tick 任务与 timer 完成项只按值持本状态与 handler，
    // 不持 ServerEngine/EventLoop/steady_timer ⇒ 挂起中的 tick 不延长 Engine 寿命、
    // 不需要被取消。读写一律在 mutex 下进行（只覆盖判定+置标志，不跨用户回调）。
    //
    // 投递句柄的生命周期由本窄门发布/撤回，不依赖 Stop 的 control_mutex_：重挂路径只在
    // 本锁内读 loop/owner 并做非阻塞 Submit ⇒ Stop 全程持 control_mutex_（含 15s owner
    // 命令等待与 Join）时，唯一 Processer 也不会被重挂阻塞。撤回落空与 closed 同锁完成，
    // 因此不会对已回收的 owner/context 投递，也不需要裸 loop 循环重试或 reaper。
    struct TickState {
        // 内部停投原因（可核状态，不扩公共 API）：首个原因胜出，供测试/诊断核对。
        enum class StopReason {
            None,
            Closed,            // Stop / 启动回落 / 析构收口
            ZeroOrNegative,    // OnTick 返回 <= 0
            UserThrow,         // OnTick 抛出
            PostRejected,      // owner 队列拒绝（Full/Sealed/NotStarted/Invalid/Failed）
            PostFailed,        // Submit 抛出（投递分配/登记失败）
            ArmFailed,         // owner 内 timer 装配/登记失败
            RegistrationFailed // tick 协程登记失败（runtime 不可用）
        };
        std::mutex mutex;
        bool closed{false};                  // 唯一开始授权：置位后不再挂 timer、不再投递
        StopReason reason{StopReason::None};
        // 本代 main owner 投递句柄：StartOwners 发布，CloseTickGate 在同一短门内撤空。
        EventLoop* loop{nullptr};
        ServerEngine* owner{nullptr};
        std::shared_ptr<EventHandler> handler;   // 按值持 handler：重挂不读 Engine 成员
        // 仅用于让「非阻塞重挂调用」有一个存活对象（提升只覆盖一次非阻塞 Submit，
        // 不跨用户代码/挂起/owner 等待，也不延长 tick 寿命）。
        std::weak_ptr<ServerEngine> engine;
    };

    struct SubOwner {
        std::unique_ptr<EventLoop> loop;             // 先声明 ⇒ 后析构（bridge 先于 loop）
        std::shared_ptr<ConnectionBridge> bridge;
    };

    explicit ServerEngine(std::shared_ptr<EventHandler> handler);

    std::error_code StartOwners(const NormalizedOptions& options);
    std::error_code BindListener(const Address& address, const NormalizedOptions& options);
    // 只登记 Boot 任务并返回窄结果句柄；不等待（等待由 Run 在锁外做）。
    std::error_code RegisterBoot(std::shared_ptr<BootResult>& out);
    // 锁外等待 OnBoot（原 10s 截止）；到期置放弃位（未开始的任务不再进入 OnBoot）。
    bool WaitBoot(const std::shared_ptr<BootResult>& boot);
    // 置放弃位并唤醒可能正在等 Boot 的 Run（Stop 与超时唯一入口）。
    static void RequestAbandon(const std::shared_ptr<BootResult>& boot);
    // 启动回落：撤 accept 等待/关 listener + 回收 owner/context/FD（持 control_mutex_ 调用）。
    // 返回清理首错；有界推进一次仍失败 ⇒ 不回收 owner/context、不发布 Idle（未收口
    // 不假 Idle），相位保持 Starting（重试 Run 得 busy），启动原错由调用方另行返回。
    std::error_code RollbackStart();
    std::error_code ArmAccept();
    std::error_code ArmAcceptOwner();
    void OnAcceptReady(const boost::system::error_code& error);
    void AcceptPendingOwner();
    void HandoffOwner(NativeSocket&& socket, const Address& peer);
    // owner 域收口（Stop / Rollback 共用）：先取消并析构本 Engine 在 main owner 上登记的
    // Asio 资源（ticker timer → accept 就绪等待 → listener），再保留首错，不抛、不以单点
    // 失败中断后续尝试。必须在 RequestStop/Join 之前完成，否则 Join 会等待已登记的等待。
    std::error_code CloseOwnerNetwork();
    // 控制线程：登记一次 tick 投递（阻塞等 owner 命令完成，与 ArmAccept 同域顺序；失败
    // 透传 owner 内真实错误）。tick_state_/tick_timer_ 见下方成员注释。
    std::error_code ArmTick(std::chrono::nanoseconds delay);
    // Processer：经 TickState 内的稳定句柄非阻塞重挂。只持窄门（判定 + Submit），
    // 不取 control_mutex_（Stop 全程持该锁）、不阻塞等 owner。
    // 投递被拒（Full/Sealed/NotStarted/Invalid/Failed）或抛出 ⇒ 停 ticker 并记内部原因，
    // Engine 服务保留，不重试。
    void PostArmTick(const std::shared_ptr<TickState>& state, std::chrono::nanoseconds delay);
    // owner 域（只在 main owner 命令内调用；调用方须持 state->mutex）：创建/复用 timer
    // 并登记下一次投递。失败 ⇒ 停 ticker 并记内部原因（不向 owner 抛出）。
    std::error_code ArmTickOwner(const std::shared_ptr<TickState>& state,
        const std::shared_ptr<EventHandler>& handler, std::chrono::nanoseconds delay);
    // 受管协程任务体：OnTick → 短锁判定 → 非阻塞重挂。
    static void TickTask(const std::shared_ptr<TickState>& state,
        const std::shared_ptr<EventHandler>& handler);
    // OnTick 返回 delay 的溢出饱和（按 steady_clock 半程与 nanoseconds 可表示上限的较小者，
    // 不是业务时限上限；换算先比较后转换，不先整数溢出）。
    static std::chrono::nanoseconds SaturateTickDelay(std::chrono::nanoseconds delay);
    // 停投（调用方须持 state->mutex）：closed + 首个原因 + 撤空投递句柄，一次完成。
    static void StopTickerLocked(const std::shared_ptr<TickState>& state,
        TickState::StopReason reason);
    // 控制线程：置开始门 closed 并撤空投递句柄（Stop/rollback/ResetOwners 唯一入口；
    // 不触碰 timer，timer 在 owner 域收口）。
    void CloseTickGate();
    std::error_code StopLocked();
    // 内部只读收口判据（owner readiness，不扩公共面）：全部桥每会话物理已关闭
    // 且 owned 等待记录已结算。非 Drained：不等 Handler/OnClose/快照/业务。
    bool NetworkCloseReady() const;
    bool IsOwnerThreadLocked() const;
    void ResetOwners();

    // 与 Engine 共享 handler 所有权（按值移动接收）。挂起中的 OnBoot 任务只按值持
    // handler 与窄结果句柄，不持 ServerEngine ⇒ Engine 强停/销毁与 10s 超时都不需要取消
    // 已经开始的任务，也不会被 Boot 延寿。
    std::shared_ptr<EventHandler> handler_;
    mutable std::mutex control_mutex_;
    Phase phase_{Phase::Idle};
    std::shared_ptr<BootResult> boot_;               // 当前代 Boot 任务句柄（清理由 Reset/Stop）
    // 跨代栅栏（R52-1 裁定）：超时回落时若旧代 Boot 已开始未完成，保留其窄结果句柄；
    // 下一次 Run 见 started && !done 即拒绝 busy（同一 handler 不跨代重入 OnBoot）。
    // 只持窄结果，不保活任何 owner/context/网络资源；旧任务自然完成后清柄放行重试。
    std::shared_ptr<BootResult> pending_boot_;
    std::atomic<bool> stopping_{false};
    std::atomic<std::size_t> accept_errors_{0};
    std::atomic<std::size_t> handoff_rejected_{0};
    // 每 Run 一代的 ticker 窄门（与 live_connections_ 同代）：上一代在途 tick 的迟到重挂
    // 被其自身代的门拒绝，不污染新一代。
    std::shared_ptr<TickState> tick_state_;
    // 当前 Run 代的活跃连接计数（接纳成功 +1 / OnClose -1）；与适配器共享所有权，
    // 不持 ServerEngine/Engine；停止时解引用而非清零（避免迟到 OnClose underflow）。
    std::shared_ptr<std::atomic<std::size_t>> live_connections_;

    std::shared_ptr<EventHandlerAdapter> adapter_;
    std::unique_ptr<EventLoop> main_loop_;
    std::unique_ptr<NativeSocket> listener_;
    std::unique_ptr<boost::asio::posix::stream_descriptor> watcher_;
    // ticker 载体：只在 main owner 域创建/操作（main_loop_->Context() 只许 owner 线程）；
    // 声明在 main_loop_ 之后 ⇒ 逆序析构早于其 context，且 Stop/rollback 已在 Join 前取消。
    std::unique_ptr<boost::asio::steady_timer> tick_timer_;
    std::unique_ptr<LoadBalancer> balancer_;
    std::vector<SubOwner> subs_;
    std::vector<Address> bound_;
    NormalizedOptions options_{};
    bool tcp_no_delay_{true};
};

} // namespace bbt::infra::net
