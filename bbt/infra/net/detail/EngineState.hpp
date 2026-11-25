#pragma once

#include <atomic>
#include <condition_variable>
#include <deque>
#include <exception>
#include <functional>
#include <future>
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
#include <bbt/infra/net/detail/WaitSet.hpp>
#include <bbt/infra/net/detail/TestHooks.hpp>

namespace bbt::infra::net {

class Reactor;
class ConnCore;
struct Listener;
struct ConnectPending;
struct EngineImpl;
class HandoffReservation;

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

class AcceptQueue;
class ConnectRegistry;

class HandoffReservation {
public:
    ~HandoffReservation();
    HandoffReservation(const HandoffReservation&) = delete;
    HandoffReservation& operator=(const HandoffReservation&) = delete;
    void Commit();

private:
    friend class AcceptQueue;
    HandoffReservation(AcceptQueue& queue, bool counted);
    bool ConsumeCredit();
    AcceptQueue* m_queue;
    std::atomic<bool> m_credit;
};

class EngineControl {
public:
    class Guard {
    public:
        explicit Guard(const EngineControl& control)
            : m_lock(control.Lock())
        {
        }

        Guard(const Guard&) = delete;
        Guard& operator=(const Guard&) = delete;
        Guard(Guard&&) = delete;
        Guard& operator=(Guard&&) = delete;

        void Wait(std::condition_variable& cv, const std::atomic<bool>& sealed)
        {
            cv.wait_for(m_lock, std::chrono::milliseconds(50), [&] { return sealed.load(std::memory_order_acquire); });
        }

    private:
        std::unique_lock<std::mutex> m_lock;
    };

    Guard Acquire() const;
    EnginePhase PhaseLocked(const Guard&) const;
    void PublishPhaseLocked(const Guard&, EnginePhase value);
    void RequestSealLocked(const Guard&);
    bool SealRequestedLocked(const Guard&) const;
    void FinishRunLocked(const Guard&);
    bool RunFinishedLocked(const Guard&) const;
    bool RunActiveLocked(const Guard&) const;
    bool TryClaimRun();
    void NotifyNative();
    void NotifyNativeLocked(const Guard&);
    void WaitCloseLocked(Guard& guard);
    void PublishAddressesLocked(const Guard&, std::vector<Address> local);
    std::vector<Address> AddressesLocked(const Guard&) const;
    void PublishSeal();
    std::shared_ptr<std::atomic<bool>> SealToken() const;
    void NoteOwnerFailure();
    bool OwnerFailed() const;
    result<bool> WaitPhaseSlice(const bbt::coroutine::WaitOptions& options);
    void NotifyPhaseWaiters() noexcept;
    void RecordError(std::error_code error);
    void RecordErrorLocked(const Guard&, std::error_code error);
    void NoteHandlerError();
    std::size_t HandlerErrorCount() const;
    std::error_code LastError() const;
    std::error_code LastErrorLocked(const Guard&) const;
    void RecordControlFailure(std::exception_ptr failure);
    std::exception_ptr LastFailure() const;
    bool IsSealed() const;

private:
    std::unique_lock<std::mutex> Lock() const;

    mutable std::mutex m_mutex;
    std::condition_variable control_cv;
    EnginePhase phase{EnginePhase::Idle};
    std::atomic<bool> control_claimed{false};
    bool run_finished{false};
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> phase_signal = bbt::coroutine::sync::CoWaiter::Create();
    std::shared_ptr<std::atomic<bool>> service_sealed = std::make_shared<std::atomic<bool>>(false);
    std::error_code last_error;
    std::exception_ptr control_failure;
    std::vector<Address> local_addresses;
    std::atomic<bool> seal_error{false};
    std::atomic<std::size_t> handler_errors{0};
};

class DriverBindings {
public:
    std::error_code Bind(std::size_t count, std::vector<DriverSlot>& out);
    bool HasResidual() const;
    std::vector<DriverSlot> TakeOwnedLocked(const EngineControl::Guard&);
    void PublishLocked(const EngineControl::Guard&, std::vector<DriverSlot> value);
    std::vector<DriverSlot> SnapshotLocked(const EngineControl::Guard&) const;
    std::size_t CountLocked(const EngineControl::Guard&) const;
    DriverSlot SlotLocked(const EngineControl::Guard&, std::size_t index) const;
    std::error_code InjectFailureLocked(const EngineControl::Guard&);
    void OwnerIdsLocked(
        const EngineControl::Guard&, std::vector<std::uint64_t>& main, std::vector<std::uint64_t>& subs) const;

private:
    std::vector<DriverSlot> slots;
    std::vector<DriverSlot> residual_slots;
};

class ReactorSet {
public:
    void PublishBalancerLocked(const EngineControl::Guard&, std::unique_ptr<LoadBalancer> value);
    void AddLocked(const EngineControl::Guard&, std::shared_ptr<Reactor> reactor);
    void PublishLayoutLocked(const EngineControl::Guard&, bool server);
    std::vector<std::shared_ptr<Reactor>> SnapshotLocked(const EngineControl::Guard&) const;
    std::shared_ptr<Reactor> MainLocked(const EngineControl::Guard&) const;
    std::shared_ptr<Reactor> AtLocked(const EngineControl::Guard&, std::size_t index) const;
    bool HasSubsLocked(const EngineControl::Guard&) const;
    std::vector<std::shared_ptr<Reactor>> SubsLocked(const EngineControl::Guard&) const;
    void ClearLocked(const EngineControl::Guard&);
    std::shared_ptr<Reactor> Select(
        const Address& peer,
        const std::vector<std::shared_ptr<Reactor>>& snapshot,
        const std::atomic<std::uint64_t>& next_conn_id);

private:
    std::vector<std::shared_ptr<Reactor>> reactors;
    std::size_t main_index{static_cast<std::size_t>(-1)};
    std::size_t sub_begin{0};
    std::size_t sub_count{0};
    std::unique_ptr<LoadBalancer> balancer;
    std::mutex balancer_mutex;
};

class ConnectOp : public std::enable_shared_from_this<ConnectOp> {
public:
    ConnectOp(
        std::shared_ptr<Reactor> owner,
        std::shared_ptr<ReclaimRecord> record,
        bool awaitable,
        const std::shared_ptr<TestHooks>& hooks);
    bool Removable();
    void DetachAfterPostFailure();
    bool Seal();
    void Notify();
    void AuthorizeDelivery();
    bool CancelRequested() const;
    bool SettleReclaim();
    ConnectTestSnapshot InspectForTest();
    std::size_t OccupyWaitersForTest(std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>>& keep);
    bool Complete(Connection connection, std::error_code result_error, bool raw);
    std::error_code Wait(
        ConnectRegistry& registry,
        EngineImpl& engine,
        std::uint64_t connect_id,
        Connection& out,
        const bbt::coroutine::WaitOptions& options);

private:
    std::mutex m_mutex;
    bool m_done{false};
    bool m_abandoned{false};
    bool m_waiter_attached{false};

    /**
     * raw handler 已真实交付（不得再由 WaitConnect 领取）
     */
    bool m_raw_delivered{false};

    /**
     * 每个 op 只允许一个 WaitConnect 接收者
     */
    bool m_receiver_claimed{false};
    std::atomic<bool> m_cancel_requested{false};

    /**
     * owner 已进入授权交付段（seal 不得抢先终态）
     */
    bool m_owner_delivering{false};
    std::shared_ptr<Reactor> m_reactor;

    /**
     * 控制域兜底句柄：主标记沿 owner 捕获链在各真实 close 点置位；此处
     * 副本仅供 Run 收口/析构兜底在 owner 链路未执行时补标记（幂等）。
     */
    std::shared_ptr<ReclaimRecord> m_reclaim;
    Connection m_connection;
    std::error_code m_error;
    WaitSet m_hub;
    std::shared_ptr<TestHooks> m_testHooks;
};

class ConnectRegistry {
public:
    std::uint64_t NextIdLocked(const EngineControl::Guard&);
    void RestrictCapacityLocked(const EngineControl::Guard&, std::size_t limit);
    void RequestCancelRetry();
    bool TakeCancelRetry();
    void MaybeErase(std::uint64_t id, const std::shared_ptr<ConnectOp>& op);
    void Seal();
    void SettleReclaim();
    std::error_code Insert(
        std::uint64_t id,
        const std::shared_ptr<Reactor>& reactor,
        const std::shared_ptr<ReclaimRecord>& reclaim,
        bool awaitable,
        const std::shared_ptr<TestHooks>& hooks);
    void RollbackPost(std::uint64_t id);
    bool Complete(std::uint64_t id, Connection connection, std::error_code error, bool raw);
    void AuthorizeDelivery(std::uint64_t id);
    bool CancelRequested(std::uint64_t id);
    std::error_code
    Wait(EngineImpl& engine, std::uint64_t id, Connection& out, const bbt::coroutine::WaitOptions& options);
    EngineTestSnapshot InspectForTest();
    ConnectTestSnapshot InspectConnectForTest(std::uint64_t id);
    std::size_t
    OccupyWaitersForTest(std::uint64_t id, std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>>& keep);

private:
    std::shared_ptr<ConnectOp> Find(std::uint64_t id);
    std::atomic<std::uint64_t> next_connect_id{1};
    std::size_t connect_limit{1024};
    std::atomic<bool> cancel_retry_needed{false};
    std::mutex connect_mutex;
    std::unordered_map<std::uint64_t, std::shared_ptr<ConnectOp>> connects;
};

class AcceptQueue {
public:
    AcceptQueue(EngineImpl& engine);
    void EnableLocked(const EngineControl::Guard&, std::size_t capacity);
    bool EnabledLocked(const EngineControl::Guard&) const;
    bool Enabled() const;
    bool RequestStop();
    bool StopRequested() const;
    bool Stopped() const;
    bool TryMarkPosted();
    void RestoreRequestedAfterPostFailure();
    void MarkPhysicallyStopped();
    void SetHooksLocked(const EngineControl::Guard&, const std::shared_ptr<TestHooks>& hooks);
    void Seal();
    void Notify();
    void SealForStop();
    bool AuthorizeDelivery();
    bool TryBeginResume();
    void FinishResume();
    std::shared_ptr<HandoffReservation> TryReserveHandoff();
    void ReleaseHandoffReservation();
    bool CommitHandoff(Connection connection, const std::shared_ptr<HandoffReservation>& reservation);
    std::error_code WaitAccepted(Connection& out, const bbt::coroutine::WaitOptions& options);

private:
    EngineImpl* m_engine;
    enum class StopStage { Open, Requested, Posted, Done };
    std::atomic<StopStage> m_stopStage{StopStage::Open};
    std::mutex handoff_mutex;
    std::deque<Connection> handoff;
    std::size_t handoff_capacity{0};
    std::size_t handoff_reserved{0};
    bool handoff_enabled{false};
    bool handoff_closed{false};
    std::atomic<bool> resume_pending{false};
    WaitSet accept_hub;
    std::shared_ptr<TestHooks> m_testHooks;
};

struct EngineImpl : public std::enable_shared_from_this<EngineImpl> {
public:
    explicit EngineImpl(std::shared_ptr<RawHandler> raw_handler);
    std::error_code Close(const CloseOptions& options) noexcept;
    void RequestStopAccepting() noexcept;
    bool AcceptingStopped() const noexcept;
    EnginePhase Phase() const noexcept;
    std::error_code EnableAcceptHandoff(std::size_t capacity);
    std::error_code Run(const EngineConfig& config);
    std::error_code FinalizeNative() noexcept;
    std::error_code RunImpl(const EngineConfig& config);
    std::error_code LocalAddresses(std::vector<Address>& out) const;
    std::size_t CountConnections() const;
    std::error_code
    ReactorOwnerIds(std::vector<std::uint64_t>& main_owners, std::vector<std::uint64_t>& sub_owners) const;
    result<void> CoWaitRunning(const bbt::coroutine::WaitOptions& options);
    std::error_code CoWaitAccepted(Connection& out, const bbt::coroutine::WaitOptions& options);
    std::error_code
    CoWaitConnect(std::uint64_t connect_id, Connection& out, const bbt::coroutine::WaitOptions& options);
    std::error_code StartConnect(
        const Address& address,
        std::uint64_t& out_connect_id,
        bool awaitable,
        const std::shared_ptr<ReclaimRecord>& reclaim = {});

    std::shared_ptr<RawHandler> Handler() const { return handler; }

    NormalizedOptions Options() const { return options; }

    std::shared_ptr<TestHooks> Hooks() const { return test_hooks; }

    std::shared_ptr<std::atomic<bool>> SealToken() const { return control.SealToken(); }

    std::uint64_t NextConnectionId() { return next_conn_id.fetch_add(1, std::memory_order_relaxed); }

    void AddConnection() { conn_count.fetch_add(1, std::memory_order_relaxed); }

    void RemoveConnection();

    void PublishConnectionCount(std::size_t count) { conn_count.store(count); }

    bool AcceptEnabled() const { return accepts.Enabled(); }

    bool AcceptStopRequested() const { return accepts.StopRequested(); }

    bool AuthorizeAccept() { return accepts.AuthorizeDelivery(); }

    void AuthorizeConnect(std::uint64_t id) { connects.AuthorizeDelivery(id); }

    bool ConnectCancelRequested(std::uint64_t id) { return connects.CancelRequested(id); }

    std::shared_ptr<Reactor> MainReactor();
    void ResumeAcceptIfFree();
    std::shared_ptr<Reactor> PickSub(const Address& peer);
    void TryPostAcceptStop(const std::shared_ptr<Reactor>& main);
    void MarkAcceptStopDone();
    void SealService();

    std::shared_ptr<HandoffReservation> TryReserveHandoff() { return accepts.TryReserveHandoff(); }

    bool CommitHandoff(Connection connection, const std::shared_ptr<HandoffReservation>& reservation)
    {
        return accepts.CommitHandoff(std::move(connection), reservation);
    }

    bool ResolveConnect(std::uint64_t id, Connection connection, std::error_code error)
    {
        return connects.Complete(id, std::move(connection), error, handler != nullptr);
    }

    void SettleAbandonedConnectReclaim() { connects.SettleReclaim(); }

    void RecordError(std::error_code error) { control.RecordError(error); }

    void NoteHandlerError() { control.NoteHandlerError(); }

    std::size_t HandlerErrorCount() const { return control.HandlerErrorCount(); }

    std::error_code LastError() const { return control.LastError(); }

    void RecordControlFailure(std::exception_ptr error) { control.RecordControlFailure(std::move(error)); }

    std::exception_ptr LastFailure() const { return control.LastFailure(); }

    bool IsSealed() const { return control.IsSealed(); }

    void NotifyPhaseWaiters() { control.NotifyPhaseWaiters(); }

    int TakeConnectWaitStatus() { return test_hooks ? test_hooks->TakeConnectWaitStatus() : -1; }

    void InvokeHook(HookPoint point) const;

    bool TakeFault(FaultPoint point) const { return test_hooks && test_hooks->TakeFault(point); }

    std::int64_t DelayCount(DelayPoint point) const { return test_hooks ? test_hooks->Delay(point).count() : 0; }

    // 仅测试装配：在 Idle 且尚未构造 Reactor 时串行调用，供 Reactor 快照钩子槽。
    void PrepareHooksForTest();

private:
    std::error_code SealOwnersNative();
    std::error_code ReclaimNative();
    std::shared_ptr<RawHandler> handler;
    EngineControl control;
    bool finalize_active{false}; // 由 control 保护。
    // Run 和 FinalizeNative 串行消费同一批回执。解绑后不再投递 owner 工作。
    std::vector<std::shared_future<std::error_code>> owner_seals;
    bool owner_seals_settled{false};
    DriverBindings bindings;
    ReactorSet owners;
    AcceptQueue accepts;
    ConnectRegistry connects;
    std::atomic<std::uint64_t> next_conn_id{1};
    std::atomic<std::size_t> conn_count{0};
    NormalizedOptions options;
    std::shared_ptr<TestHooks> test_hooks;
    friend class Engine;
    friend class EngineTestAccess;
    std::error_code InjectDriverFailureForTest();
    void RestrictConnectCapacityForTest(std::size_t limit);
    struct EngineTestSnapshot InspectForTest();
    bool RemoveConnForTest(std::uint64_t id);
    struct ConnectTestSnapshot InspectConnectForTest(std::uint64_t id);
    void DelayForTest(std::size_t point, std::chrono::milliseconds delay);
    void ArmHookForTest(std::size_t point, std::function<void()> hook, bool once);
    void ArmFaultForTest(std::size_t point);
    std::size_t
    OccupyConnectWaitersForTest(std::uint64_t id, std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>>& keep);
    void RequestOwnerPumpForTest();
    void OverrideWaitOnceForTest(int status);
    void ClearConnectInjectionForTest();
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
