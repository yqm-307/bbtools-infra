#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>
#include <boost/asio/steady_timer.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/LoadBalancing.hpp>
#include <bbt/infra/net/detail/DriverBinder.hpp>
#include <bbt/infra/net/detail/EngineSupport.hpp>
#include <bbt/infra/net/detail/OptionsNormalize.hpp>
#include <bbt/infra/net/detail/WakeHub.hpp>
#include <bbt/infra/net/detail/TestHooks.hpp>

namespace bbt::infra::net {

class Reactor;
class ConnCore;
struct Listener;
struct ConnectPending;
struct EngineImpl;
struct HandoffReservation;

void DetailNoteHandlerError(EngineImpl* impl);

struct Listener {
    explicit Listener(asio::io_context& context)
        : desc(context)
    {
    }

    TcpWait desc;
    Address local;
    std::atomic<bool> armed{false};
};

struct ConnectPending {
    std::shared_ptr<TcpWait> desc;
    std::shared_ptr<asio::steady_timer> start_timer;
    std::uint64_t connect_id{0};
    bool canceled{false};
};

/**
 * handoff 预留：先预留容量再 accept；析构未提交则释放预留。
 */
struct HandoffReservation {
    EngineImpl* impl{nullptr};
    bool counted{false};
    bool committed{false};
    ~HandoffReservation();

    void Commit() { committed = true; }
};

struct EngineImpl : public std::enable_shared_from_this<EngineImpl> {
    explicit EngineImpl(std::shared_ptr<RawHandler> raw_handler)
        : handler(std::move(raw_handler))
        , service_sealed(std::make_shared<std::atomic<bool>>(false))
    {
    }

    std::shared_ptr<RawHandler> handler;
    mutable std::mutex mutex;
    std::condition_variable control_cv;
    EnginePhase phase{EnginePhase::Idle};
    bool close_requested{false};

    /**
     * 唯一控制门：false=未占用、true=Run 已占用（成功后不释放，禁止重开）。
     */
    std::atomic<bool> control_claimed{false};

    /**
     * Run 已返回（含仍 Closing 的失败收口）
     */
    bool run_finished{false};

    /**
     * Running/Stopped 发布点的 Co 唤醒位；等待者以 Phase 为持久谓词，
     * 先通知后等待的丢唤醒窗口由登记后的谓词复查关闭。
     */
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> phase_signal = bbt::coroutine::sync::CoWaiter::Create();
    std::shared_ptr<std::atomic<bool>> service_sealed;

    void NotifyPhaseWaiters() noexcept
    {
        try {
            if (phase_signal) {
                phase_signal->Notify();
            }
        } catch (...) {
        }
    }

    mutable std::mutex error_mutex;
    std::error_code last_error;
    std::exception_ptr control_failure;

    std::vector<DriverSlot> slots;

    /**
     * 解绑失败保留的责任
     */
    std::vector<DriverSlot> residual_slots;
    std::vector<std::shared_ptr<Reactor>> reactors;
    std::shared_ptr<Reactor> main_reactor;
    std::vector<std::shared_ptr<Reactor>> subs;
    std::vector<Address> local_addresses;

    std::atomic<std::uint64_t> next_conn_id{1};
    std::atomic<std::uint64_t> next_connect_id{1};
    std::atomic<std::size_t> conn_count{0};

    /**
     * 仅封监听/交接授权的停止：由 RequestStopAccepting 置位的不可逆 latch；
     * accept_stop_done 仅在 owner 物理关闭全部本代 listener（或控制域同代
     * 解绑后确认无 listener）后置位，才作为 AcceptingStopped 的凭据。
     */
    std::atomic<bool> accept_stop_requested{false};
    std::atomic<bool> accept_stop_posted{false};
    std::atomic<bool> accept_stop_done{false};

    std::size_t max_inbound_bytes{1024 * 1024};
    std::size_t max_outbound_bytes{1024 * 1024};
    std::size_t max_ops{1024};
    NormalizedOptions options;
    std::unique_ptr<LoadBalancer> balancer;
    std::mutex balancer_mutex;
    std::size_t connect_limit{1024};
    std::atomic<bool> cancel_retry_needed{false};

    /**
     * seal handler 捕获到异常
     */
    std::atomic<bool> seal_error{false};

    std::mutex handoff_mutex;
    std::deque<Connection> handoff;
    std::size_t handoff_capacity{0};
    std::size_t handoff_reserved{0};
    bool handoff_enabled{false};
    bool handoff_closed{false};
    std::atomic<bool> resume_pending{false};
    std::shared_ptr<TestHooks> test_hooks;
    WakeHub accept_hub;

    void InvokeHook(HookPoint point) const
    {
        if (test_hooks)
            test_hooks->Invoke(point);
    }

    bool TakeFault(FaultPoint point) const { return test_hooks && test_hooks->TakeFault(point); }

    std::int64_t DelayCount(DelayPoint point) const { return test_hooks ? test_hooks->Delay(point).count() : 0; }

    struct ConnectOp {
        ConnectTestSnapshot InspectForTest()
        {
            std::lock_guard<std::mutex> lock(mutex);
            return {true, done, receiver_claimed};
        }

        std::size_t OccupyWaitersForTest(std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>>& keep)
        {
            std::size_t added = 0;
            for (std::size_t i = 0; i < WakeHub::kMaxWaiters + 1; ++i) {
                auto waiter = hub.Create();
                if (!hub.Add(waiter))
                    break;
                keep.push_back(std::move(waiter));
                ++added;
            }
            return added;
        }

        std::mutex mutex;
        bool done{false};
        bool abandoned{false};
        bool waiter_attached{false};

        /**
         * raw handler 已真实交付（不得再由 WaitConnect 领取）
         */
        bool raw_delivered{false};

        /**
         * 每个 op 只允许一个 WaitConnect 接收者
         */
        bool receiver_claimed{false};
        std::atomic<bool> cancel_requested{false};

        /**
         * owner 已进入授权交付段（seal 不得抢先终态）
         */
        bool owner_delivering{false};
        std::shared_ptr<Reactor> reactor;

        /**
         * 控制域兜底句柄：主标记沿 owner 捕获链在各真实 close 点置位；此处
         * 副本仅供 Run 收口/析构兜底在 owner 链路未执行时补标记（幂等）。
         */
        std::shared_ptr<ReclaimRecord> reclaim;
        Connection connection;
        std::error_code error;
        WakeHub hub;
    };

    std::mutex connect_mutex;
    std::unordered_map<std::uint64_t, std::shared_ptr<ConnectOp>> connects;

    std::atomic<std::size_t> handler_errors{0};

    /**
     * ---- service seal ----
     */
    void SealService()
    {
        if (service_sealed) {
            service_sealed->store(true, std::memory_order_release);
        }
        std::deque<Connection> undelivered;
        {
            std::lock_guard<std::mutex> lock(handoff_mutex);
            handoff_closed = true;

            /**
             * 关闭对 queue 与 inflight 共同封口
             */
            handoff_reserved = 0;
            undelivered.swap(handoff);
        }
        for (auto& connection : undelivered) {
            (void)connection.Close({}, std::make_error_code(std::errc::operation_canceled));
        }
        {

            /**
             * 不建临时 vector：同一 connect_mutex 下逐项结算并 Notify（不重入
             * connect_mutex），避免 seal 前置路径的分配抛异常。
             */
            std::lock_guard<std::mutex> lock(connect_mutex);
            for (auto& entry : connects) {
                auto& op = entry.second;
                {
                    std::lock_guard<std::mutex> op_lock(op->mutex);
                    if (op->done || op->owner_delivering) {

                        /**
                         * 已完成/已进入 owner 授权交付段：由原 owner 兑现终态。
                         */
                        continue;
                    }
                    op->done = true;
                    op->error = std::make_error_code(std::errc::operation_canceled);
                }
                op->hub.NotifyAll();
            }
        }
        accept_hub.NotifyAll();
    }

    /**
     * ---- handoff reservation ----
     */
    std::shared_ptr<HandoffReservation> TryReserveHandoff()
    {
        std::lock_guard<std::mutex> lock(handoff_mutex);
        if (handoff_closed || IsSealed()) {

            /**
             * RequestClose/Seal 与 accept 共门
             */
            return nullptr;
        }
        if (!handoff_enabled) {
            auto reservation = std::make_shared<HandoffReservation>();
            reservation->impl = this;
            reservation->counted = false;
            return reservation;
        }
        if (handoff_closed || handoff.size() + handoff_reserved >= handoff_capacity) {
            return nullptr;
        }
        ++handoff_reserved;
        auto reservation = std::make_shared<HandoffReservation>();
        reservation->impl = this;
        reservation->counted = true;
        return reservation;
    }

    void ReleaseHandoffReservation()
    {
        {
            std::lock_guard<std::mutex> lock(handoff_mutex);
            if (handoff_closed) {
                return;
            }
            if (handoff_reserved > 0) {
                --handoff_reserved;
            }
        }
        ResumeAcceptIfFree();
    }

    bool CommitHandoff(Connection connection, const std::shared_ptr<HandoffReservation>& reservation)
    {
        {
            std::lock_guard<std::mutex> lock(handoff_mutex);
            if (!handoff_enabled || handoff_closed) {
                return false;
            }
            if (reservation) {

                /**
                 * 只扣减一次
                 */
                reservation->committed = true;
            }
            if (handoff_reserved > 0) {
                --handoff_reserved;
            }
            handoff.push_back(std::move(connection));
        }
        accept_hub.NotifyAll();
        return true;
    }

    void ResumeAcceptIfFree();

    std::shared_ptr<ConnectOp> FindConnect(std::uint64_t id)
    {
        std::lock_guard<std::mutex> lock(connect_mutex);
        const auto it = connects.find(id);
        return it == connects.end() ? nullptr : it->second;
    }

    /**
     * 同一 op 锁内仲裁「交付 / 回收」。返回是否已交付（raw 或等待者）。
     */
    bool ResolveConnect(std::uint64_t id, Connection connection, std::error_code error)
    {
        auto op = FindConnect(id);
        if (!op) {
            if (connection.Valid()) {
                (void)connection.Close({}, std::make_error_code(std::errc::operation_canceled));
            }
            return false;
        }
        bool recycle = false;
        bool delivered = false;
        {
            std::lock_guard<std::mutex> lock(op->mutex);
            if (op->done) {

                /**
                 * 等待者已超时或已取消，本次完成的连接无人接收 → 回收已交付的 fd。
                 */
                recycle = connection.Valid();
            } else if (op->abandoned && !op->waiter_attached && handler == nullptr) {
                op->done = true;
                op->error = std::make_error_code(std::errc::operation_canceled);
                recycle = connection.Valid();
            } else {
                op->done = true;
                op->connection = std::move(connection);
                op->error = error;
                if (handler != nullptr) {

                    /**
                     * raw 已真实交付：清可等待意图，raw-only 清理不被 Co 等待阻塞；
                     * 也不得再由 WaitConnect 二次领取。
                     */
                    op->raw_delivered = true;
                    op->waiter_attached = false;
                }
                delivered = true;
            }
        }
        if (recycle) {
            (void)connection.Close({}, std::make_error_code(std::errc::operation_canceled));
        }
        op->hub.NotifyAll();
        MaybeEraseConnect(id, op);
        return delivered;
    }

    void MaybeEraseConnect(std::uint64_t id, const std::shared_ptr<ConnectOp>& op)
    {
        bool erase = false;
        {
            std::lock_guard<std::mutex> lock(op->mutex);

            /**
             * 未转移且未释放的记录必须留在表内，等控制域收尾兜底补标记；
             * 否则「lambda 未执行的提交」会随擦表丢失唯一可达的记录句柄。
             */
            erase = op->done && !op->waiter_attached && (!op->reclaim || op->reclaim->Released());
        }
        if (!erase) {
            return;
        }
        std::lock_guard<std::mutex> lock(connect_mutex);
        const auto it = connects.find(id);
        if (it != connects.end() && it->second == op) {

            /**
             * 完成且无人等待：释放活动登记
             */
            connects.erase(it);
        }
    }

    /**
     * 控制域收尾兜底（幂等）：仅在 unbind/FinalizeControl 之后调用——此刻
     * owner 链路不再执行；FinalizeControl 已显式关闭 pending descriptor、
     * delivery holder 和连接 backend，不依赖异步捕获或 context 的最终析构。
     * 因此未转移的建连记录此刻已无真实 fd，才可补标记为释放；
     * 已完结且无人等待的表项随后正常擦除，保持活动登记语义不变。
     */
    void SettleAbandonedConnectReclaim()
    {
        std::lock_guard<std::mutex> lock(connect_mutex);
        for (auto it = connects.begin(); it != connects.end();) {
            auto& op = it->second;
            {
                std::lock_guard<std::mutex> op_lock(op->mutex);
                MarkReclaimReleased(op->reclaim);
            }
            bool removable = false;
            {
                std::lock_guard<std::mutex> op_lock(op->mutex);
                removable = op->done && !op->waiter_attached;
            }
            it = removable ? connects.erase(it) : std::next(it);
        }
    }

    void RecordError(std::error_code error)
    {
        if (!error) {
            return;
        }
        std::lock_guard<std::mutex> lock(error_mutex);
        if (!last_error) {
            last_error = error;
        }
    }

    void NoteHandlerError()
    {

        /**
         * 用户原始回调异常：隔离并计数，不升级为 engine/ driver 故障，
         * 不影响其它终态/关闭回调兑现。
         */
        handler_errors.fetch_add(1, std::memory_order_relaxed);
    }

    std::size_t HandlerErrorCount() const { return handler_errors.load(std::memory_order_relaxed); }

    std::error_code LastError() const
    {
        std::lock_guard<std::mutex> lock(error_mutex);
        return last_error;
    }

    void RecordControlFailure(std::exception_ptr failure)
    {
        std::lock_guard<std::mutex> lock(error_mutex);
        if (!control_failure) {
            control_failure = std::move(failure);
        }
    }

    std::exception_ptr LastFailure() const
    {
        std::lock_guard<std::mutex> lock(error_mutex);
        return control_failure;
    }

    std::shared_ptr<Reactor> PickSub(const Address& peer);

    bool IsSealed() const { return service_sealed && service_sealed->load(std::memory_order_acquire); }

    /**
     * 仅在 owner 物理关闭全部本代 listener，或控制域同代同步解绑后确认无
     * listener 时置位；幂等、可重入，并唤醒 AcceptingStopped 观测者。
     */
    void TryPostAcceptStop(const std::shared_ptr<Reactor>& main);

    void MarkAcceptStopDone()
    {
        accept_stop_done.store(true, std::memory_order_release);
        std::lock_guard<std::mutex> lock(mutex);
        control_cv.notify_all();
    }

private:
    friend class Engine;
    std::error_code InjectDriverFailureForTest();
    void RestrictConnectCapacityForTest(std::size_t limit);
    struct EngineTestSnapshot InspectForTest();
    struct ConnectTestSnapshot InspectConnectForTest(std::uint64_t id);
    void DelayForTest(std::size_t point, std::chrono::milliseconds delay);
    void ArmHookForTest(std::size_t point, std::function<void()> hook, bool once);
    void ArmFaultForTest(std::size_t point);
    std::size_t
    OccupyConnectWaitersForTest(std::uint64_t id, std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>>& keep);
    void RequestOwnerPumpForTest();
    void OverrideWaitOnceForTest(int status);
    void ClearConnectInjectionForTest();
    void PrepareHooksForTest();
};

std::error_code StartConnectImpl(
    const std::shared_ptr<EngineImpl>& impl,
    const Address& address,
    std::uint64_t& out_connect_id,
    bool awaitable,
    const std::shared_ptr<ReclaimRecord>& reclaim = {});

Error WaitRunningError(const std::error_code& error);

// clang-format off
}
// clang-format on
