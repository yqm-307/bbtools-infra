#include <bbt/infra/net/detail/EngineState.hpp>
#include <bbt/infra/net/detail/Reactor.hpp>
#include <bbt/infra/net/Engine.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <exception>
#include <functional>
#include <stdexcept>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/uio.h>
#include <unistd.h>

#include <boost/asio/post.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/steady_timer.hpp>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/detail/CloseWait.hpp>
#include <bbt/infra/net/LoadBalancing.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/detail/CoInternal.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/DriverBinder.hpp>
#include <bbt/infra/net/detail/OptionsNormalize.hpp>
#include <bbt/infra/net/detail/ReactorDriver.hpp>
#include <bbt/infra/net/detail/ReclaimRecord.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>
#include <bbt/infra/net/detail/WaitSet.hpp>

namespace bbt::infra::net {

std::error_code Engine::InjectDriverFailureForTest()
{
    return m_impl->InjectDriverFailureForTest();
}

void Engine::RestrictConnectCapacityForTest(std::size_t limit)
{
    m_impl->RestrictConnectCapacityForTest(limit);
}

EngineTestSnapshot Engine::InspectForTest()
{
    return m_impl->InspectForTest();
}

ConnectTestSnapshot Engine::InspectConnectForTest(std::uint64_t id)
{
    return m_impl->InspectConnectForTest(id);
}

void Engine::DelayForTest(std::size_t point, std::chrono::milliseconds delay)
{
    m_impl->DelayForTest(point, delay);
}

void Engine::ArmHookForTest(std::size_t point, std::function<void()> hook, bool once)
{
    m_impl->ArmHookForTest(point, std::move(hook), once);
}

void Engine::ArmFaultForTest(std::size_t point)
{
    m_impl->ArmFaultForTest(point);
}

std::size_t Engine::OccupyConnectWaitersForTest(
    std::uint64_t id, std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>>& keep)
{
    return m_impl->OccupyConnectWaitersForTest(id, keep);
}

void Engine::RequestOwnerPumpForTest()
{
    m_impl->RequestOwnerPumpForTest();
}

void Engine::OverrideWaitOnceForTest(int status)
{
    m_impl->OverrideWaitOnceForTest(status);
}

void Engine::ClearConnectInjectionForTest()
{
    m_impl->ClearConnectInjectionForTest();
}

void EngineImpl::PrepareHooksForTest()
{
    // Run 夹具通过 ClearConnectInjection 装配；直构夹具须在构造 Reactor 前显式装配。
    // 后续 setter 只修改 TestHooks 内部槽，连接持有的 shared_ptr 始终不变。
    if (!test_hooks) {
        auto guard = control.Acquire();
        if (control.PhaseLocked(guard) != EnginePhase::Idle)
            throw std::logic_error("test hooks must be installed before Run");
        test_hooks = std::make_shared<TestHooks>();
        accepts.SetHooksLocked(guard, test_hooks);
    }
}

std::error_code EngineImpl::InjectDriverFailureForTest()
{
    auto guard = control.Acquire();
    if (control.PhaseLocked(guard) != EnginePhase::Running)
        return std::make_error_code(std::errc::operation_not_permitted);
    return bindings.InjectFailureLocked(guard);
}

void EngineImpl::RestrictConnectCapacityForTest(std::size_t limit)
{
    auto guard = control.Acquire();
    connects.RestrictCapacityLocked(guard, limit);
}

EngineTestSnapshot EngineImpl::InspectForTest()
{
    EngineTestSnapshot snapshot = connects.InspectForTest();
    std::vector<std::shared_ptr<Reactor>> owners;
    {
        auto guard = control.Acquire();
        owners = this->owners.SnapshotLocked(guard);
    }
    for (auto& owner : owners) {
        snapshot.m_connections += owner->ConnCount();
        snapshot.m_dirty += owner->DirtyCount();
        snapshot.m_recvHold += owner->RecvHoldBytes();
        if (snapshot.m_firstFd < 0)
            snapshot.m_firstFd = owner->FirstConnFd();
    }
    return snapshot;
}

bool EngineImpl::RemoveConnForTest(std::uint64_t id)
{
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        auto guard = control.Acquire();
        reactors = owners.SnapshotLocked(guard);
    }
    for (auto& reactor : reactors) {
        if (reactor->RemoveConnForTest(id)) {
            return true;
        }
    }
    return false;
}

ConnectTestSnapshot EngineImpl::InspectConnectForTest(std::uint64_t id)
{
    return connects.InspectConnectForTest(id);
}

void EngineImpl::DelayForTest(std::size_t point, std::chrono::milliseconds delay)
{
    PrepareHooksForTest();
    test_hooks->DelayAt(static_cast<DelayPoint>(point), delay);
}

void EngineImpl::ArmHookForTest(std::size_t point, std::function<void()> hook, bool once)
{
    PrepareHooksForTest();
    const auto hook_point = static_cast<HookPoint>(point);
    switch (hook_point) {
    case HookPoint::BeforeCallbackAuthorization:
    case HookPoint::AfterCallbackAuthorization:
    case HookPoint::AfterReceiveBeforeAppend:
    case HookPoint::AfterAppendBusy:
    case HookPoint::AfterDirtyWeakLock:
    case HookPoint::AfterDirtyUnlinkBeforeErase:
    case HookPoint::BeforeShutdownStage:
    case HookPoint::BeforeWriteReadyPump:
    case HookPoint::BeforeDriverPost:
    case HookPoint::BeforeConnectStartAuthorization: {
        std::vector<std::shared_ptr<Reactor>> owners;
        {
            auto guard = control.Acquire();
            owners = this->owners.SnapshotLocked(guard);
        }
        for (auto& owner : owners)
            owner->ArmHookForTest(hook_point, hook, once);
        break;
    }
    default:
        test_hooks->Arm(hook_point, std::move(hook), once);
        break;
    }
}

void EngineImpl::ArmFaultForTest(std::size_t point)
{
    PrepareHooksForTest();
    test_hooks->FailOnce(static_cast<FaultPoint>(point));
}

std::size_t EngineImpl::OccupyConnectWaitersForTest(
    std::uint64_t id, std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>>& keep)
{
    return connects.OccupyWaitersForTest(id, keep);
}

void EngineImpl::RequestOwnerPumpForTest()
{
    std::vector<std::shared_ptr<Reactor>> owners;
    {
        auto guard = control.Acquire();
        owners = this->owners.SnapshotLocked(guard);
    }
    for (auto& owner : owners)
        owner->KickPumpOwner();
}

void EngineImpl::OverrideWaitOnceForTest(int status)
{
    PrepareHooksForTest();
    test_hooks->OverrideConnectWaitOnce(status);
}

void EngineImpl::ClearConnectInjectionForTest()
{
    PrepareHooksForTest();
    test_hooks->ClearConnectInjection();
}

constexpr std::size_t kMaxConnectOps = 1024;

/**
 * 建连责任收口：仅在各真实 close 点（或确认无 fd 创建）之后调用；幂等。
 */
void MarkReclaimReleased(const std::shared_ptr<ReclaimRecord>& record)
{
    if (record) {
        record->ConfirmReleased();
    }
}

void DetailNoteHandlerError(EngineImpl* impl)
{
    impl->NoteHandlerError();
}

Error WaitRunningError(const std::error_code& error)
{
    if (!error) {
        return MakeError(ErrorCode::Closed, "engine: stopped before running");
    }
    if (error == std::errc::operation_not_permitted) {
        return MakeError(ErrorCode::InvalidContext, "engine: run rejected by execution domain");
    }
    if (error == std::errc::operation_not_supported) {
        return MakeError(ErrorCode::RuntimeUnavailable, "engine: runtime unavailable");
    }
    if (error == std::errc::device_or_resource_busy) {
        return MakeError(ErrorCode::Overloaded, "engine: resource busy");
    }
    if (error == std::errc::operation_canceled) {
        return MakeError(ErrorCode::Cancelled, "engine: run canceled");
    }
    if (error == std::errc::timed_out) {
        return MakeError(ErrorCode::TimedOut, "engine: run timed out");
    }
    if (error == std::errc::invalid_argument) {
        return MakeError(ErrorCode::InvalidArgument, "engine: invalid run configuration");
    }
    return MakeError(ErrorCode::RuntimeUnavailable, "engine: " + error.message());
}

EngineControl::Guard EngineControl::Acquire() const
{
    return Guard(*this);
}

EnginePhase EngineControl::PhaseLocked(const Guard&) const
{
    return phase;
}

void EngineControl::PublishPhaseLocked(const Guard&, EnginePhase value)
{
    phase = value;
}

void EngineControl::RequestSealLocked(const Guard&)
{
    PublishSeal();
}

bool EngineControl::SealRequestedLocked(const Guard&) const
{
    return IsSealed();
}

void EngineControl::FinishRunLocked(const Guard&)
{
    run_finished = true;
}

bool EngineControl::RunFinishedLocked(const Guard&) const
{
    return run_finished;
}

bool EngineControl::TryClaimRun()
{
    bool expected = false;
    return control_claimed.compare_exchange_strong(expected, true);
}

void EngineControl::NotifyNative()
{
    control_cv.notify_all();
}

void EngineControl::NotifyNativeLocked(const Guard&)
{
    control_cv.notify_all();
}

void EngineControl::WaitCloseLocked(Guard& guard)
{
    guard.Wait(control_cv, *service_sealed);
}

void EngineControl::PublishAddressesLocked(const Guard&, std::vector<Address> local)
{
    local_addresses = std::move(local);
}

std::vector<Address> EngineControl::AddressesLocked(const Guard&) const
{
    return local_addresses;
}

void EngineControl::PublishSeal()
{
    if (service_sealed)
        service_sealed->store(true, std::memory_order_release);
}

std::shared_ptr<std::atomic<bool>> EngineControl::SealToken() const
{
    return service_sealed;
}

void EngineControl::NoteOwnerFailure()
{
    seal_error.store(true);
}

bool EngineControl::OwnerFailed() const
{
    return seal_error.load();
}

result<bool> EngineControl::WaitPhaseSlice(const bbt::coroutine::WaitOptions& options)
{
    return bbt::infra::detail::WaitCloseSlice(phase_signal, bbt::infra::detail::NextCloseSlice(options.deadline));
}

void EngineControl::NotifyPhaseWaiters() noexcept
{
    try {
        if (phase_signal) {
            phase_signal->Notify();
        }
    } catch (...) {
    }
}

void EngineControl::RecordError(std::error_code error)
{
    if (!error)
        return;
    auto guard = Acquire();
    RecordErrorLocked(guard, error);
}

void EngineControl::RecordErrorLocked(const Guard&, std::error_code error)
{
    if (error && !last_error)
        last_error = error;
}

void EngineControl::NoteHandlerError()
{

    /**
         * 用户原始回调异常：隔离并计数，不升级为 engine/ driver 故障，
         * 不影响其它终态/关闭回调兑现。
         */
    handler_errors.fetch_add(1, std::memory_order_relaxed);
}

std::size_t EngineControl::HandlerErrorCount() const
{
    return handler_errors.load(std::memory_order_relaxed);
}

std::error_code EngineControl::LastError() const
{
    auto guard = Acquire();
    return LastErrorLocked(guard);
}

std::error_code EngineControl::LastErrorLocked(const Guard&) const
{
    return last_error;
}

void EngineControl::RecordControlFailure(std::exception_ptr failure)
{
    auto guard = Acquire();
    if (!control_failure) {
        control_failure = std::move(failure);
    }
}

std::exception_ptr EngineControl::LastFailure() const
{
    auto guard = Acquire();
    return control_failure;
}

bool EngineControl::IsSealed() const
{
    return service_sealed && service_sealed->load(std::memory_order_acquire);
}

std::error_code DriverBindings::Bind(std::size_t count, std::vector<DriverSlot>& out)
{
    return DriverBinder::BindSlots(count, out, &residual_slots);
}

bool DriverBindings::HasResidual() const
{
    return !residual_slots.empty();
}

std::vector<DriverSlot> DriverBindings::TakeOwnedLocked(const EngineControl::Guard&)
{
    auto result = std::move(slots);
    slots.clear();
    for (auto& slot : residual_slots)
        result.push_back(std::move(slot));
    residual_slots.clear();
    return result;
}

void DriverBindings::PublishLocked(const EngineControl::Guard&, std::vector<DriverSlot> value)
{
    slots = std::move(value);
}

std::vector<DriverSlot> DriverBindings::SnapshotLocked(const EngineControl::Guard&) const
{
    return slots;
}

std::error_code DriverBindings::InjectFailureLocked(const EngineControl::Guard&)
{
    if (slots.empty())
        return std::make_error_code(std::errc::operation_not_permitted);
    slots.back().driver->InjectTickFailureOnce();
    return {};
}

void DriverBindings::OwnerIdsLocked(
    const EngineControl::Guard&,
    bool has_main,
    std::vector<std::uint64_t>& main,
    std::vector<std::uint64_t>& subs) const
{
    std::size_t index = 0;
    if (has_main && !slots.empty()) {
        main.push_back(slots[0].processer_id);
        index = 1;
    }
    for (; index < slots.size(); ++index)
        subs.push_back(slots[index].processer_id);
}

void ReactorSet::PublishBalancerLocked(const EngineControl::Guard&, std::unique_ptr<LoadBalancer> value)
{
    balancer = std::move(value);
}

void ReactorSet::AddLocked(const EngineControl::Guard&, std::shared_ptr<Reactor> reactor)
{
    reactors.push_back(std::move(reactor));
}

void ReactorSet::PublishLayoutLocked(const EngineControl::Guard&, bool server)
{
    main_index = server ? 0 : static_cast<std::size_t>(-1);
    sub_begin = server ? 1 : 0;
    sub_count = reactors.size() - sub_begin;
}

std::size_t DriverBindings::CountLocked(const EngineControl::Guard&) const
{
    return slots.size();
}

DriverSlot DriverBindings::SlotLocked(const EngineControl::Guard&, std::size_t index) const
{
    return slots[index];
}

std::vector<std::shared_ptr<Reactor>> ReactorSet::SnapshotLocked(const EngineControl::Guard&) const
{
    return reactors;
}

std::shared_ptr<Reactor> ReactorSet::MainLocked(const EngineControl::Guard&) const
{
    return main_index < reactors.size() ? reactors[main_index] : nullptr;
}

bool ReactorSet::HasSubsLocked(const EngineControl::Guard&) const
{
    return sub_count != 0;
}

std::vector<std::shared_ptr<Reactor>> ReactorSet::SubsLocked(const EngineControl::Guard&) const
{
    return {reactors.begin() + sub_begin, reactors.begin() + sub_begin + sub_count};
}

void ReactorSet::ClearLocked(const EngineControl::Guard&)
{
    reactors.clear();
    main_index = static_cast<std::size_t>(-1);
    sub_begin = 0;
    sub_count = 0;
}

std::shared_ptr<Reactor> ReactorSet::Select(
    const Address& peer,
    const std::vector<std::shared_ptr<Reactor>>& snapshot,
    const std::atomic<std::uint64_t>& next_conn_id)
{
    if (snapshot.empty()) {
        return nullptr;
    }
    std::vector<std::uint64_t> counts;
    counts.reserve(snapshot.size());
    for (auto& reactor : snapshot) {
        counts.push_back(reactor->ConnCount());
    }
    std::lock_guard<std::mutex> balancer_lock(balancer_mutex);
    if (balancer) {
        std::error_code error;
        const std::size_t index = balancer->Next(peer, counts, error);
        if (!error && index < snapshot.size()) {
            return snapshot[index];
        }
    }
    return snapshot[next_conn_id.load() % snapshot.size()];
}

ConnectOp::ConnectOp(
    std::shared_ptr<Reactor> owner,
    std::shared_ptr<ReclaimRecord> record,
    bool awaitable,
    const std::shared_ptr<TestHooks>& hooks)
{
    m_reactor = std::move(owner);
    m_reclaim = std::move(record);
    m_waiter_attached = awaitable;
    m_testHooks = hooks;
}

bool ConnectOp::Removable()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_done && !m_waiter_attached && (!m_reclaim || m_reclaim->Released());
}

void ConnectOp::DetachAfterPostFailure()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_waiter_attached = false;
}

bool ConnectOp::Seal()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_done || m_owner_delivering)
        return false;
    m_done = true;
    m_error = std::make_error_code(std::errc::operation_canceled);
    return true;
}

void ConnectOp::Notify()
{
    m_hub.Notify(WaitLane::Connect);
}

void ConnectOp::AuthorizeDelivery()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    m_owner_delivering = true;
    m_reclaim = nullptr;
}

bool ConnectOp::CancelRequested() const
{
    return m_cancel_requested.load();
}

bool ConnectOp::SettleReclaim()
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        MarkReclaimReleased(m_reclaim);
    }
    bool removable = false;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        removable = m_done && !m_waiter_attached;
    }
    return removable;
}

ConnectTestSnapshot ConnectOp::InspectForTest()
{
    std::lock_guard<std::mutex> lock(m_mutex);
    return {true, m_done, m_receiver_claimed};
}

std::size_t ConnectOp::OccupyWaitersForTest(std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>>& keep)
{
    std::size_t added = 0;
    for (std::size_t i = 0; i < WaitSet::kMaxWaiters + 1; ++i) {
        auto waiter = CreateWaiter(m_testHooks);
        if (!m_hub.Add(WaitLane::Connect, waiter))
            break;
        keep.push_back(std::move(waiter));
        ++added;
    }
    return added;
}

bool ConnectOp::Complete(Connection connection, std::error_code result_error, bool raw)
{
    bool recycle = false;
    bool delivered = false;
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_done) {

            /**
                 * 等待者已超时或已取消，本次完成的连接无人接收 → 回收已交付的 fd。
                 */
            recycle = connection.Valid();
        } else if (m_abandoned && !m_waiter_attached && !raw) {
            m_done = true;
            m_error = std::make_error_code(std::errc::operation_canceled);
            recycle = connection.Valid();
        } else {
            m_done = true;
            m_connection = std::move(connection);
            m_error = result_error;
            if (raw) {

                /**
                     * raw 已真实交付：清可等待意图，raw-only 清理不被 Co 等待阻塞；
                     * 也不得再由 WaitConnect 二次领取。
                     */
                m_raw_delivered = true;
                m_waiter_attached = false;
            }
            delivered = true;
        }
    }
    if (recycle) {
        (void)connection.Close({}, std::make_error_code(std::errc::operation_canceled));
    }
    m_hub.Notify(WaitLane::Connect);

    return delivered;
}

std::error_code ConnectOp::Wait(
    ConnectRegistry& registry,
    EngineImpl& engine,
    std::uint64_t connect_id,
    Connection& out,
    const bbt::coroutine::WaitOptions& options)
{
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_receiver_claimed) {
            return std::make_error_code(std::errc::device_or_resource_busy);
        }

        /**
         * 单接收者；超时/取消后不自动释放
         */
        m_receiver_claimed = true;
    }

    /**
     * 离开 CoDial 且未交付的唯一归还点：清 claim/attach、登记放弃意图；需要
     * 中断真实连接尝试时同时持久化取消（post 失败由控制循环重试）。owner 在
     * 真实完成或取消时看到「已放弃且无接收者」即回收未交付 fd，不调 owner 回调。
     */
    auto abandon = [&](bool cancel_owner) {
        std::shared_ptr<Reactor> reactor;
        Connection orphan;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_receiver_claimed = false;
            m_waiter_attached = false;
            if (m_done) {

                /**
                 * 已完成但本次领取已终结：连接此刻无主，直接回收。
                 */
                if (!m_raw_delivered) {
                    orphan = m_connection;
                }
                m_abandoned = false;
            } else {

                /**
                 * 未完成：放弃接收意图，owner 稍后交付或取消时回收 fd。
                 */
                m_abandoned = true;
            }
            reactor = m_reactor;
            if (cancel_owner) {
                m_cancel_requested.store(true);
            }
        }
        if (orphan.Valid()) {
            (void)orphan.Close({}, std::make_error_code(std::errc::operation_canceled));
            registry.MaybeErase(connect_id, shared_from_this());
        }
        if (!cancel_owner) {
            return;
        }
        if (reactor) {
            if (engine.TakeFault(FaultPoint::CancelPost)
                || !reactor->TryPost([reactor, connect_id] { reactor->CancelConnectOnOwner(connect_id); })) {
                registry.RequestCancelRetry();
            }
        } else {
            registry.RequestCancelRetry();
        }
    };

    /**
     * 终态 op 的收尾：只有无错误且确有连接的结果才算本次交付；否则不覆盖 out。
     */
    auto settle_done = [&](Connection delivered, std::error_code done_error) {
        registry.MaybeErase(connect_id, shared_from_this());
        if (done_error) {
            if (!engine.Handler() && delivered.Valid()) {

                /**
                 * 无接收者的已交付连接：owner 交付时已登记，这里回收。
                 */
                (void)delivered.Close({}, std::make_error_code(std::errc::operation_canceled));
            }
            return done_error;
        }
        if (!delivered.Valid()) {
            return std::make_error_code(std::errc::operation_canceled);
        }
        out = std::move(delivered);
        return std::error_code{};
    };

    for (;;) {
        bool already_done = false;
        bool raw_conflict = false;
        Connection delivered;
        std::error_code done_error;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_done) {
                if (m_raw_delivered) {

                    /**
                     * 已由 raw handler 唯一交付，不重复领取
                     */
                    raw_conflict = true;
                } else {
                    delivered = m_connection;
                    done_error = m_error;
                }
                m_waiter_attached = false;
                m_abandoned = false;
                already_done = true;
            } else {
                m_waiter_attached = true;
                m_abandoned = false;
            }
        }
        if (already_done) {
            if (raw_conflict) {
                registry.MaybeErase(connect_id, shared_from_this());
                return std::make_error_code(std::errc::operation_not_supported);
            }
            return settle_done(std::move(delivered), done_error);
        }

        /**
         * 登记 → 复查 → 挂起。登记失败与建立失败都在下方向外归还 claim。
         */
        bbt::coroutine::WaitStatus status = bbt::coroutine::WaitStatus::InvalidContext;
        bool add_failed = false;

        /**
         * 私有 test：一次性把"本次等待的返回值"固定成指定 WaitStatus，用于确定性
         * 覆盖非 Completed 的退出路径（InvalidContext / RuntimeUnavailable /
         * AlreadyWaiting）。注入点就在等待返回边界：不建立等待者、不等真实事件，
         * 其余（claim 已取得、op 仍 pending、下方 abandon(true) 的取消投递与 owner
         * 回收）全部走产品既有代码，不构造假网络响应。未武装时为纯产品路径。
         * Completed 不可注入：它表示"被通知后复查"，注入会在 op 仍 pending 时
         * 原地重试；夹带 Completed 的调用退化为真实等待，避免测试写出死循环。
         */
        const int injected_wait_status = (engine.TakeConnectWaitStatus());
        const bool inject_wait = injected_wait_status >= 0
                                 && injected_wait_status != static_cast<int>(bbt::coroutine::WaitStatus::Completed);
        if (inject_wait) {
            status = static_cast<bbt::coroutine::WaitStatus>(injected_wait_status);
        } else {
            try {
                const auto wait = WaitChange(
                    m_hub,
                    WaitLane::Connect,
                    options,
                    [&] {
                        std::lock_guard<std::mutex> lock(m_mutex);
                        return m_done;
                    },
                    m_testHooks);
                status = wait.status;
                add_failed = wait.problem != WaitProblem::None;
            } catch (...) {

                /**
                 * 等待对象建立失败：本条领取没有等待者，必须放弃而不是留着 claim。
                 */
                abandon(false);
                return std::make_error_code(std::errc::not_enough_memory);
            }
        }
        if (add_failed) {

            /**
             * 满额时结果可能已就绪：那就直接交付；否则放弃并让 owner 回收。
             */
            bool done = false;
            Connection ready;
            std::error_code ready_error;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                done = m_done && !m_raw_delivered;
                if (done) {
                    ready = m_connection;
                    ready_error = m_error;
                }
                m_waiter_attached = false;
            }
            if (done) {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_receiver_claimed = false;
                m_abandoned = false;
            }
            if (done && !ready_error && ready.Valid()) {
                registry.MaybeErase(connect_id, shared_from_this());
                out = std::move(ready);
                return {};
            }
            if (done) {
                registry.MaybeErase(connect_id, shared_from_this());
                return std::make_error_code(std::errc::no_buffer_space);
            }
            abandon(false);
            return std::make_error_code(std::errc::no_buffer_space);
        }
        if (status == bbt::coroutine::WaitStatus::Completed) {

            /**
             * 被通知：回到顶部按真实结果复查
             */
            continue;
        }

        /**
         * 其余 WaitStatus 全部终结本次领取，且一律归还责任后再返回。
         */
        bool done = false;
        Connection finished;
        std::error_code finished_error;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            done = m_done;
            if (done && !m_raw_delivered) {
                finished = m_connection;
                finished_error = m_error;
            }
            m_waiter_attached = false;
        }
        const auto mapped = WaitStatusError(status);
        if (done) {
            if (finished.Valid()) {

                /**
                 * 等待已按本次仲裁终结：结果无主 → 回收（raw 已交付时不动）。
                 */
                (void)finished.Close({}, std::make_error_code(std::errc::operation_canceled));
            }
            registry.MaybeErase(connect_id, shared_from_this());
            return mapped;
        }

        /**
         * 尚未完成：交还责任；超时/取消/上下文失效都要求中断真实尝试。
         */
        abandon(true);
        return mapped;
    }
}

std::uint64_t ConnectRegistry::NextIdLocked(const EngineControl::Guard&)
{
    return next_connect_id.fetch_add(1, std::memory_order_relaxed);
}

void ConnectRegistry::RestrictCapacityLocked(const EngineControl::Guard&, std::size_t limit)
{
    connect_limit = limit == 0 ? 1 : limit;
}

void ConnectRegistry::RequestCancelRetry()
{
    cancel_retry_needed.store(true);
}

bool ConnectRegistry::TakeCancelRetry()
{
    return cancel_retry_needed.exchange(false);
}

std::shared_ptr<ConnectOp> ConnectRegistry::Find(std::uint64_t id)
{
    std::lock_guard<std::mutex> lock(connect_mutex);
    const auto it = connects.find(id);
    return it == connects.end() ? nullptr : it->second;
}

void ConnectRegistry::MaybeErase(std::uint64_t id, const std::shared_ptr<ConnectOp>& op)
{
    if (!op->Removable())
        return;
    std::lock_guard<std::mutex> lock(connect_mutex);
    const auto it = connects.find(id);
    if (it != connects.end() && it->second == op)
        connects.erase(it);
}

void ConnectRegistry::Seal()
{
    std::lock_guard<std::mutex> lock(connect_mutex);
    for (auto& entry : connects) {
        if (entry.second->Seal())
            entry.second->Notify();
    }
}

void ConnectRegistry::SettleReclaim()
{
    std::lock_guard<std::mutex> lock(connect_mutex);
    for (auto it = connects.begin(); it != connects.end();) {
        const bool removable = it->second->SettleReclaim();
        it = removable ? connects.erase(it) : std::next(it);
    }
}

std::error_code ConnectRegistry::Insert(
    std::uint64_t id,
    const std::shared_ptr<Reactor>& reactor,
    const std::shared_ptr<ReclaimRecord>& reclaim,
    bool awaitable,
    const std::shared_ptr<TestHooks>& hooks)
{
    std::lock_guard<std::mutex> lock(connect_mutex);
    for (auto it = connects.begin(); it != connects.end();) {
        if (it->second->Removable())
            it = connects.erase(it);
        else
            ++it;
    }
    if (connects.size() >= connect_limit)
        return std::make_error_code(std::errc::no_buffer_space);
    connects[id] = std::make_shared<ConnectOp>(reactor, reclaim, awaitable, hooks);
    return {};
}

void ConnectRegistry::RollbackPost(std::uint64_t id)
{
    std::lock_guard<std::mutex> lock(connect_mutex);
    const auto it = connects.find(id);
    if (it != connects.end())
        it->second->DetachAfterPostFailure();
}

bool ConnectRegistry::Complete(std::uint64_t id, Connection connection, std::error_code error, bool raw)
{
    auto op = Find(id);
    if (!op) {
        if (connection.Valid())
            (void)connection.Close({}, std::make_error_code(std::errc::operation_canceled));
        return false;
    }
    const bool delivered = op->Complete(std::move(connection), error, raw);
    MaybeErase(id, op);
    return delivered;
}

void ConnectRegistry::AuthorizeDelivery(std::uint64_t id)
{
    if (auto op = Find(id))
        op->AuthorizeDelivery();
}

bool ConnectRegistry::CancelRequested(std::uint64_t id)
{
    auto op = Find(id);
    return op && op->CancelRequested();
}

std::error_code
ConnectRegistry::Wait(EngineImpl& engine, std::uint64_t id, Connection& out, const bbt::coroutine::WaitOptions& options)
{
    auto op = Find(id);
    if (!op)
        return std::make_error_code(std::errc::invalid_argument);
    return op->Wait(*this, engine, id, out, options);
}

EngineTestSnapshot ConnectRegistry::InspectForTest()
{
    EngineTestSnapshot snapshot;
    std::lock_guard<std::mutex> lock(connect_mutex);
    snapshot.m_connects = connects.size();
    for (auto& entry : connects)
        if (!entry.second->InspectForTest().m_done)
            ++snapshot.m_pendingConnects;
    return snapshot;
}

ConnectTestSnapshot ConnectRegistry::InspectConnectForTest(std::uint64_t id)
{
    auto op = Find(id);
    return op ? op->InspectForTest() : ConnectTestSnapshot{};
}

std::size_t ConnectRegistry::OccupyWaitersForTest(
    std::uint64_t id, std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>>& keep)
{
    auto op = Find(id);
    return op ? op->OccupyWaitersForTest(keep) : 0;
}

AcceptQueue::AcceptQueue(EngineImpl& engine)
{
    m_engine = &engine;
}

void AcceptQueue::EnableLocked(const EngineControl::Guard&, std::size_t capacity)
{
    handoff_capacity = capacity;
    handoff_enabled = true;
}

bool AcceptQueue::EnabledLocked(const EngineControl::Guard&) const
{
    return handoff_enabled;
}

bool AcceptQueue::Enabled() const
{
    return handoff_enabled;
}

bool AcceptQueue::RequestStop()
{
    auto expected = StopStage::Open;
    return m_stopStage.compare_exchange_strong(
        expected, StopStage::Requested, std::memory_order_acq_rel, std::memory_order_acquire);
}

bool AcceptQueue::StopRequested() const
{
    return m_stopStage.load(std::memory_order_acquire) != StopStage::Open;
}

bool AcceptQueue::Stopped() const
{
    return m_stopStage.load(std::memory_order_acquire) == StopStage::Done;
}

bool AcceptQueue::TryMarkPosted()
{
    auto expected = StopStage::Requested;
    return m_stopStage.compare_exchange_strong(
        expected, StopStage::Posted, std::memory_order_acq_rel, std::memory_order_acquire);
}

void AcceptQueue::RestoreRequestedAfterPostFailure()
{
    // owner 可能已物理停服；失败回退不能覆盖并发发布的 Done。
    auto expected = StopStage::Posted;
    (void)m_stopStage.compare_exchange_strong(
        expected, StopStage::Requested, std::memory_order_acq_rel, std::memory_order_acquire);
}

void AcceptQueue::MarkPhysicallyStopped()
{
    m_stopStage.store(StopStage::Done, std::memory_order_release);
}

void AcceptQueue::SetHooksLocked(const EngineControl::Guard&, const std::shared_ptr<TestHooks>& hooks)
{
    m_testHooks = hooks;
}

void AcceptQueue::Seal()
{
    std::deque<Connection> undelivered;
    {
        std::lock_guard<std::mutex> lock(handoff_mutex);
        handoff_closed = true;
        handoff_reserved = 0;
        undelivered.swap(handoff);
    }
    for (auto& connection : undelivered)
        (void)connection.Close({}, std::make_error_code(std::errc::operation_canceled));
}

void AcceptQueue::Notify()
{
    accept_hub.Notify(WaitLane::Accept);
}

void AcceptQueue::SealForStop()
{
    {
        std::lock_guard<std::mutex> lock(handoff_mutex);
        handoff_closed = true;
        handoff_reserved = 0;
    }
    accept_hub.Notify(WaitLane::Accept);
    for (;;) {
        Connection connection;
        {
            std::lock_guard<std::mutex> lock(handoff_mutex);
            if (handoff.empty())
                break;
            connection = std::move(handoff.front());
            handoff.pop_front();
        }
        (void)connection.Close({}, std::make_error_code(std::errc::operation_canceled));
    }
}

bool AcceptQueue::AuthorizeDelivery()
{
    std::lock_guard<std::mutex> lock(handoff_mutex);
    return !handoff_closed && !m_engine->IsSealed();
}

bool AcceptQueue::TryBeginResume()
{
    return !resume_pending.exchange(true);
}

void AcceptQueue::FinishResume()
{
    resume_pending.store(false);
}

std::shared_ptr<HandoffReservation> AcceptQueue::TryReserveHandoff()
{
    std::lock_guard<std::mutex> lock(handoff_mutex);
    if (handoff_closed || m_engine->IsSealed()) {

        /**
             * RequestClose/Seal 与 accept 共门
             */
        return nullptr;
    }
    if (!handoff_enabled) {
        auto reservation = std::shared_ptr<HandoffReservation>(new HandoffReservation(*this, false));
        return reservation;
    }
    if (handoff_closed || handoff.size() + handoff_reserved >= handoff_capacity) {
        return nullptr;
    }
    ++handoff_reserved;
    auto reservation = std::shared_ptr<HandoffReservation>(new HandoffReservation(*this, true));
    return reservation;
}

void AcceptQueue::ReleaseHandoffReservation()
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
    m_engine->ResumeAcceptIfFree();
}

bool AcceptQueue::CommitHandoff(Connection connection, const std::shared_ptr<HandoffReservation>& reservation)
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
            if (!reservation->ConsumeCredit())
                return false;
        }
        if (handoff_reserved > 0) {
            --handoff_reserved;
        }
        handoff.push_back(std::move(connection));
    }
    accept_hub.Notify(WaitLane::Accept);
    return true;
}

std::error_code AcceptQueue::WaitAccepted(Connection& out, const bbt::coroutine::WaitOptions& options)
{
    for (;;) {
        Connection popped;
        bool sealed = false;
        {
            std::lock_guard<std::mutex> lock(handoff_mutex);
            if (!handoff_closed && !handoff.empty()) {
                popped = handoff.front();
                handoff.pop_front();
            } else {
                sealed = m_engine->IsSealed() || StopRequested();
            }
        }

        /**
         * 以本次实际弹出的连接判断成功；调用者原有的 out 有效不算。
         */
        if (popped.Valid()) {
            out = popped;
            m_engine->ResumeAcceptIfFree();
            return {};
        }
        if (sealed) {
            return std::make_error_code(std::errc::operation_canceled);
        }
        const auto wait = WaitChange(
            accept_hub,
            WaitLane::Accept,
            options,
            [&] {
                std::lock_guard<std::mutex> lock(handoff_mutex);
                return !handoff.empty() || m_engine->IsSealed() || StopRequested();
            },
            m_testHooks,
            [&] { m_engine->InvokeHook(HookPoint::AfterAcceptRegistered); });
        const bool add_failed = wait.problem != WaitProblem::None;
        const auto status = wait.status;
        if (add_failed) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        if (status != bbt::coroutine::WaitStatus::Completed) {
            return WaitStatusError(status);
        }
    }
}

HandoffReservation::HandoffReservation(AcceptQueue& queue, bool counted)
    : m_queue(&queue)
    , m_credit(counted)
{
}

bool HandoffReservation::ConsumeCredit()
{
    return m_credit.exchange(false);
}

void HandoffReservation::Commit()
{
    (void)ConsumeCredit();
}

HandoffReservation::~HandoffReservation()
{
    if (ConsumeCredit())
        m_queue->ReleaseHandoffReservation();
}

EngineImpl::EngineImpl(std::shared_ptr<RawHandler> raw_handler)
    : handler(std::move(raw_handler))
    , accepts(*this)
{
}

void EngineImpl::SealService()
{
    control.PublishSeal();
    accepts.Seal();
    connects.Seal();
    accepts.Notify();
}

void EngineImpl::MarkAcceptStopDone()
{
    accepts.MarkPhysicallyStopped();
    auto guard = control.Acquire();
    control.NotifyNativeLocked(guard);
}

std::shared_ptr<Reactor> EngineImpl::MainReactor()
{
    auto guard = control.Acquire();
    return owners.MainLocked(guard);
}

std::error_code EngineImpl::Close(const CloseOptions& options) noexcept
{
    if (options.mode != CloseMode::Immediate || options.wait_release
        || (options.reason && !options.reason->message.empty()))
        return std::make_error_code(std::errc::invalid_argument);
    auto impl = shared_from_this();
    if (!impl) {
        return {};
    }
    try {
        {
            auto guard = control.Acquire();
            control.RequestSealLocked(guard);
        }
        SealService();
        control.NotifyNative();
    } catch (...) {
    }
    return {};
}

void EngineImpl::RequestStopAccepting() noexcept
{
    auto impl = shared_from_this();
    if (!impl) {
        return;
    }
    try {

        /**
         * 不可逆 latch：仅第一次真实执行封口，并发/重复请求幂等、不重开。
         */
        if (!accepts.RequestStop()) {
            return;
        }

        /**
         * 同一接纳门，封口路径不创建可能分配失败的临时 deque。
         */
        accepts.SealForStop();

        /**
         * 物理关闭 listener 交由主接受者 owner 执行（真实 close 后才 stopped）。
         * client-only / 尚未 Run / owner 已解绑：本代无 listener，停止即生效。
         */
        std::shared_ptr<Reactor> main;
        EnginePhase phase;
        {
            auto guard = control.Acquire();
            main = owners.MainLocked(guard);
            phase = control.PhaseLocked(guard);
        }
        if (main) {
            TryPostAcceptStop(main);
        } else if (phase == EnginePhase::Idle || phase == EnginePhase::Running || phase == EnginePhase::Stopped) {
            MarkAcceptStopDone();
        }

        /**
         * Starting/Closing 尚未发布或已移走 owner 时，必须等安装回滚/解绑收口。
         */
        control.NotifyNative();
    } catch (...) {
    }
}

bool EngineImpl::AcceptingStopped() const noexcept
{
    auto impl = shared_from_this();
    if (!impl) {
        return false;
    }
    return accepts.StopRequested() && accepts.Stopped();
}

EnginePhase EngineImpl::Phase() const noexcept
{
    auto guard = control.Acquire();
    return control.PhaseLocked(guard);
}

std::error_code EngineImpl::EnableAcceptHandoff(std::size_t capacity)
{
    if (capacity == 0) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    auto guard = control.Acquire();
    if (control.PhaseLocked(guard) != EnginePhase::Idle) {
        return std::make_error_code(std::errc::device_or_resource_busy);
    }
    accepts.EnableLocked(guard, capacity);
    return {};
}

std::error_code EngineImpl::Run(const EngineConfig& config)
{

    /**
     * 执行域 gate 先于控制门：非法域的失败不得占用本代控制权。
     */
    if (bbt::coroutine::GetLocalCoroutineId() != 0
        || bbt::coroutine::detail::Processer::TryGetLocalProcesser() != nullptr) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }

    /**
     * 唯一控制门：并发/重复 Run 在绑定或 socket 副作用之前拒绝；成功占用后不释放，
     * 同一实例不可重开。
     */
    if (!control.TryClaimRun()) {
        return std::make_error_code(std::errc::device_or_resource_busy);
    }
    const auto error = RunImpl(config);
    {
        auto guard = control.Acquire();
        control.FinishRunLocked(guard);
    }
    NotifyPhaseWaiters();
    return error;
}

std::error_code EngineImpl::FinalizeNative()
{
    auto impl = shared_from_this();
    if (!impl) {
        return {};
    }
    {
        auto guard = control.Acquire();
        if (control.PhaseLocked(guard) != EnginePhase::Closing) {
            return {};
        }
    }
    std::vector<DriverSlot> to_unbind;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        auto guard = control.Acquire();
        to_unbind = bindings.TakeOwnedLocked(guard);
        reactors = owners.SnapshotLocked(guard);
    }
    if (!to_unbind.empty()) {
        const auto error = DriverBinder::UnbindSlots(to_unbind);
        if (error || !to_unbind.empty()) {
            auto guard = control.Acquire();
            bindings.PublishLocked(guard, std::move(to_unbind));
            return error ? error : std::make_error_code(std::errc::io_error);
        }
    }
    for (auto& reactor : reactors) {
        reactor->FinalizeControl();
        reactor->ReleaseDriver();
    }
    SettleAbandonedConnectReclaim();
    if (accepts.StopRequested()) {
        MarkAcceptStopDone();
    }
    {
        auto guard = control.Acquire();
        owners.ClearLocked(guard);
        control.PublishPhaseLocked(guard, EnginePhase::Stopped);
        control.NotifyNative();
    }
    NotifyPhaseWaiters();
    return {};
}

std::error_code EngineImpl::RunImpl(const EngineConfig& config)
{
    auto impl = shared_from_this();

    /**
     * 前置域检查在 Starting/绑定/socket 任何副作用之前。
     */
    if (bbt::coroutine::GetLocalCoroutineId() != 0
        || bbt::coroutine::detail::Processer::TryGetLocalProcesser() != nullptr) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    bool early_close = false;
    {
        auto guard = control.Acquire();
        if (control.PhaseLocked(guard) != EnginePhase::Idle) {
            return control.PhaseLocked(guard) == EnginePhase::Stopped
                       ? std::make_error_code(std::errc::operation_canceled)
                       : std::make_error_code(std::errc::device_or_resource_busy);
        }
        if (control.SealRequestedLocked(guard)) {

            /**
             * Run 之前已收到关闭请求：不伪造 Running，按真实 Stopped + 首因收口。
             */
            control.RecordErrorLocked(guard, std::make_error_code(std::errc::operation_canceled));
            control.PublishPhaseLocked(guard, EnginePhase::Stopped);
            early_close = true;
        } else {
            control.PublishPhaseLocked(guard, EnginePhase::Starting);
        }
    }
    if (early_close) {
        NotifyPhaseWaiters();
        return LastError();
    }

    auto fail = [&](std::error_code error) {
        {
            auto guard = control.Acquire();
            control.RecordErrorLocked(guard, error);
            control.PublishPhaseLocked(guard, EnginePhase::Stopped);
            control.NotifyNative();
        }
        NotifyPhaseWaiters();
        return error;
    };
    std::vector<std::shared_ptr<Reactor>> reactors;

    /**
     * 安装/绑定失败，或监听停止与安装竞争：解绑已分配槽、控制域独占清理
     * reactor、不发布 Running、保留不可伪报的 Stopped/Closing 责任态。
     */
    const bool server = !config.listen_addresses.empty();

    /**
     * Run 前已封监听（且 server 布局）：不创建任何 socket/绑定，本代不再新开监听；
     * 置于运行时/绑定一切副作用之前，故无运行时时亦报 operation_canceled。
     */
    if (server && accepts.StopRequested()) {
        return fail(std::make_error_code(std::errc::operation_canceled));
    }

    auto abort_install = [&](std::error_code error) -> std::error_code {
        std::vector<DriverSlot> to_unbind;
        {
            auto guard = control.Acquire();
            to_unbind = bindings.TakeOwnedLocked(guard);
        }
        const auto unbind_error = DriverBinder::UnbindSlots(to_unbind);
        if (unbind_error || !to_unbind.empty()) {
            auto guard = control.Acquire();
            bindings.PublishLocked(guard, std::move(to_unbind));
            control.RecordErrorLocked(guard, unbind_error ? unbind_error : error);
            control.PublishPhaseLocked(guard, EnginePhase::Closing);
            return unbind_error ? unbind_error : error;
        }
        for (auto& reactor : reactors) {
            reactor->FinalizeControl();
            reactor->ReleaseDriver();
        }
        SettleAbandonedConnectReclaim();
        if (accepts.StopRequested()) {
            MarkAcceptStopDone();
        }
        {
            auto guard = control.Acquire();
            owners.ClearLocked(guard);
        }
        return fail(error);
    };

    auto* scheduler = bbt::coroutine::detail::Scheduler::GetInstance().get();
    if (scheduler == nullptr || !scheduler->IsInitialized()) {
        return fail(std::make_error_code(std::errc::operation_not_supported));
    }
    if (config.runtime_progress != RuntimeProgress::Independent) {
        return fail(std::make_error_code(std::errc::operation_not_permitted));
    }

    NormalizedOptions normalized;

    /**
     * 归一化只做值验证，不再依赖 CPU 数，也没有历史段容量字段可交叉校验。
     */
    if (const auto error = NormalizeOptions(config.options, normalized)) {
        return fail(error);
    }
    if (server && handler == nullptr && !accepts.Enabled()) {

        /**
         * 保持 handler+handoff 共存，但拒绝"两者都无"的无人接收布局：
         * 仍在 driver 绑定/socket 副作用之前。
         */
        return fail(std::make_error_code(std::errc::operation_not_permitted));
    }
    options = normalized;

    const std::size_t sub_count = std::max<std::size_t>(1, normalized.num_event_loop);
    const std::size_t slot_count = server ? sub_count + 1 : sub_count;

    std::error_code balancer_error;
    std::unique_ptr<LoadBalancer> balancer;
    try {
        balancer = std::make_unique<LoadBalancer>(normalized.lb, sub_count, balancer_error);
    } catch (...) {
        return fail(std::make_error_code(std::errc::not_enough_memory));
    }
    if (balancer_error) {
        return fail(balancer_error);
    }

    std::vector<DriverSlot> slots;
    if (const auto error = bindings.Bind(slot_count, slots)) {
        if (bindings.HasResidual()) {

            /**
             * 回滚失败：责任仍可达，保持 Closing，不伪报 Stopped。
             */
            auto guard = control.Acquire();
            control.RecordErrorLocked(guard, error);
            control.PublishPhaseLocked(guard, EnginePhase::Closing);
            return error;
        }
        return fail(error);
    }
    std::shared_ptr<Reactor> main_reactor;
    {
        auto guard = control.Acquire();
        bindings.PublishLocked(guard, std::move(slots));
        owners.PublishBalancerLocked(guard, std::move(balancer));
        for (std::size_t i = 0; i < bindings.CountLocked(guard); ++i) {
            owners.AddLocked(guard, std::make_shared<Reactor>(this, i, bindings.SlotLocked(guard, i).driver));
        }
        owners.PublishLayoutLocked(guard, server);
        reactors = owners.SnapshotLocked(guard);
        main_reactor = owners.MainLocked(guard);
    }

    /**
     * owner 安装：post 到各自 context 并等 owner 返回结果，然后才发布 Running。
     */
    std::vector<std::shared_future<std::error_code>> installs;
    for (auto& reactor : reactors) {
        auto promise = std::make_shared<std::promise<std::error_code>>();
        installs.push_back(promise->get_future().share());
        std::vector<Address> addresses;
        if (server && reactor == main_reactor) {
            addresses = config.listen_addresses;
        }
        if (!reactor->TryPost(
                [reactor, addresses, promise] { promise->set_value(reactor->InstallOnOwner(addresses)); })) {
            promise->set_value(std::make_error_code(std::errc::operation_canceled));
        }
    }

    /**
     * 无隐藏 deadline：等 owner 安装完成；期间若 driver 真实 Failed 由 Query
     * 控制域识别并走既定错误出口（不把超时冒 Failed）。
     */
    auto driver_failed = [&]() -> bool {
        if (TakeFault(FaultPoint::WaitDriverFailed)) {
            RecordError(std::make_error_code(std::errc::io_error));
            return true;
        }
        std::error_code control_error;
        std::exception_ptr control_failure;
        {
            auto guard = control.Acquire();
            for (std::size_t i = 0; i < bindings.CountLocked(guard); ++i) {
                const auto slot = bindings.SlotLocked(guard, i);
                bbt::coroutine::detail::ProcesserDriverSnapshot snapshot;
                const auto status = scheduler->QueryProcesserDriver(slot.processer_id, snapshot);
                if (status != bbt::coroutine::detail::ProcesserDriverStatus::Ok) {
                    control_error = std::make_error_code(std::errc::io_error);
                    break;
                }
                if (snapshot.m_token != slot.token) {
                    control_error = std::make_error_code(std::errc::operation_canceled);
                    break;
                }
                if (snapshot.m_state == bbt::coroutine::detail::ProcesserDriverState::Failed) {
                    control_failure = snapshot.m_exception;
                    control_error = std::make_error_code(std::errc::io_error);
                    break;
                }
            }
        }
        if (control_error) {
            if (control_failure) {
                RecordControlFailure(control_failure);
            }
            RecordError(control_error);
            return true;
        }
        return false;
    };
    std::error_code install_error;
    for (auto& future : installs) {
        while (future.wait_for(std::chrono::milliseconds(5)) != std::future_status::ready) {
            if (driver_failed()) {
                break;
            }
        }
        if (future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
            install_error = std::make_error_code(std::errc::operation_canceled);
            break;
        }
        install_error = future.get();
        if (install_error) {
            break;
        }
    }
    if (install_error) {
        if (install_error == std::make_error_code(std::errc::timed_out)) {

            /**
             * owner 安装结果未在界内返回：可能仍有 queued install；不得解绑/
             * 销毁 context 或伪报 Stopped，保留 Closing 由合法控制域析构兜底。
             */
            auto guard = control.Acquire();
            control.RecordErrorLocked(guard, install_error);
            control.PublishPhaseLocked(guard, EnginePhase::Closing);
            return install_error;
        }

        /**
         * 同步安装失败：走统一安装失败收口，不伪报 Stopped。
         */
        return abort_install(install_error);
    }

    if (server && accepts.StopRequested()) {

        /**
         * 停止与安装竞争：不得在接受已停止的布局上发布 Running；
         * listener 已由 owner 关闭或从未创建，走安装失败收口。
         */
        return abort_install(std::make_error_code(std::errc::operation_canceled));
    }

    if (!server && accepts.StopRequested()) {
        MarkAcceptStopDone();
    }
    {
        std::vector<Address> local;
        if (server && main_reactor) {
            local = main_reactor->LocalAddresses();
        }
        auto guard = control.Acquire();
        control.PublishAddressesLocked(guard, std::move(local));
        control.PublishPhaseLocked(guard, EnginePhase::Running);
    }

    /**
     * Running 发布后锁外通知：等待者以 Phase 为持久谓词，不漏本次发布。
     */
    NotifyPhaseWaiters();

    bool failed = false;
    for (;;) {
        bool close = false;
        {
            auto guard = control.Acquire();
            control.WaitCloseLocked(guard);
            close = control.SealRequestedLocked(guard);
        }
        if (close) {
            break;
        }
        if (main_reactor && accepts.StopRequested()) {
            TryPostAcceptStop(main_reactor);
        }
        if (connects.TakeCancelRetry()) {

            /**
             * 一次性取消投递失败后的持久重试：控制循环补交，不等网络无限终态。
             */
            for (auto& reactor : reactors) {
                if (!reactor->TryPostInternal([reactor] { reactor->CancelAllRequestedOnOwner(); })) {
                    connects.RequestCancelRetry();
                }
            }
        }
        std::vector<DriverSlot> current;
        {
            auto guard = control.Acquire();
            current = bindings.SnapshotLocked(guard);
        }
        bool control_error = false;
        for (const auto& slot : current) {
            bbt::coroutine::detail::ProcesserDriverSnapshot snapshot;
            const auto status = scheduler->QueryProcesserDriver(slot.processer_id, snapshot);
            if (status != bbt::coroutine::detail::ProcesserDriverStatus::Ok) {
                RecordError(std::make_error_code(std::errc::io_error));
                control_error = true;
                break;
            }
            if (snapshot.m_token != slot.token) {

                /**
                 * 代次不符：诚实报控制错误，绝不用新 token 解绑他人。
                 */
                RecordError(std::make_error_code(std::errc::operation_canceled));
                control_error = true;
                break;
            }
            if (snapshot.m_state == bbt::coroutine::detail::ProcesserDriverState::Failed) {
                failed = true;
                RecordControlFailure(snapshot.m_exception);
                RecordError(std::make_error_code(std::errc::io_error));
                break;
            }
        }
        if (failed || control_error) {

            /**
             * 诚实收口，不无限只记录后继续等
             */
            failed = true;
            break;
        }
    }

    {
        auto guard = control.Acquire();
        control.PublishPhaseLocked(guard, EnginePhase::Closing);
    }
    SealService();

    /**
     * 正常路径：owner 撤 listener/取消 connect/关闭连接并兑现用户终态。
     */
    bool owner_seal_complete = !failed;
    std::error_code final_error;
    if (!failed) {
        std::vector<std::shared_future<std::error_code>> seals;
        for (auto& reactor : reactors) {
            auto promise = std::make_shared<std::promise<std::error_code>>();
            seals.push_back(promise->get_future().share());

            /**
             * 单次可恢复 post 失败：补交同一条 seal 责任，不设隐藏关闭 deadline，
             * 直到原 owner 兑现全部已接纳回调（正常完成条件是 owner 结算，而非 2s）。
             */
            for (;;) {
                if (TakeFault(FaultPoint::SealPost)) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                    continue;
                }
                if (reactor->TryPostInternal([reactor, promise, impl] {
                        try {
                            if (impl->TakeFault(FaultPoint::SealThrow)) {
                                throw std::runtime_error("seal-throw-test");
                            }
                            reactor->SealServiceOnOwner([promise] { promise->set_value(std::error_code{}); });
                        } catch (...) {

                            /**
                             * 本地可恢复异常：保留首错，但先重试完成正常 owner
                             * 收口（OnClose/OnWrite/OnConnect），不得用冻结替代。
                             */
                            impl->control.NoteOwnerFailure();
                            try {
                                reactor->SealServiceOnOwner([promise] { promise->set_value(std::error_code{}); });
                            } catch (...) {
                                promise->set_value(std::make_error_code(std::errc::not_enough_memory));
                            }
                        }
                    })) {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
        for (auto& future : seals) {
            while (future.wait_for(std::chrono::milliseconds(5)) != std::future_status::ready) {
                if (driver_failed()) {

                    /**
                     * driver 真实 Failed：不再等一个不会执行的 handler
                     */
                    break;
                }
            }
            if (future.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready) {
                owner_seal_complete = false;
                final_error = std::make_error_code(std::errc::operation_canceled);
            } else if (const auto seal_result = future.get()) {
                owner_seal_complete = false;
                final_error = seal_result;
            }
        }
        if (control.OwnerFailed()) {
            RecordError(std::make_error_code(std::errc::not_enough_memory));
            if (!final_error) {
                final_error = std::make_error_code(std::errc::not_enough_memory);
            }
        }
        if (!owner_seal_complete) {
            auto guard = control.Acquire();
            control.RecordErrorLocked(guard, final_error);
        }
    }

    /**
     * 同代同步解绑；失败/残留则保留责任对象、保持 Closing、可观测错误，不发布 Stopped。
     */
    std::vector<DriverSlot> to_unbind;
    {
        auto guard = control.Acquire();
        to_unbind = bindings.TakeOwnedLocked(guard);
    }
    const auto unbind_error = DriverBinder::UnbindSlots(to_unbind);
    if (unbind_error || !to_unbind.empty()) {
        auto guard = control.Acquire();
        bindings.PublishLocked(guard, std::move(to_unbind));
        control.RecordErrorLocked(guard, unbind_error ? unbind_error : std::make_error_code(std::errc::io_error));
        control.PublishPhaseLocked(guard, EnginePhase::Closing);
        return control.LastErrorLocked(guard);
    }

    /**
     * 解绑成功：控制域独占清理旧 context（不再触发用户回调），再释放 driver 环。
     */
    for (auto& reactor : reactors) {
        reactor->FinalizeControl();
        reactor->ReleaseDriver();
    }
    SettleAbandonedConnectReclaim();
    if (accepts.StopRequested()) {
        MarkAcceptStopDone();
    }
    {
        auto guard = control.Acquire();
        owners.ClearLocked(guard);
        control.PublishPhaseLocked(guard, EnginePhase::Stopped);
        control.NotifyNative();
    }
    NotifyPhaseWaiters();
    return final_error ? final_error : LastError();
}

std::error_code EngineImpl::LocalAddresses(std::vector<Address>& out) const
{
    auto guard = control.Acquire();
    if (control.PhaseLocked(guard) == EnginePhase::Idle || control.PhaseLocked(guard) == EnginePhase::Stopped) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    out = control.AddressesLocked(guard);
    return {};
}

std::size_t EngineImpl::CountConnections() const
{
    return conn_count.load();
}

std::error_code
EngineImpl::ReactorOwnerIds(std::vector<std::uint64_t>& main_owners, std::vector<std::uint64_t>& sub_owners) const
{
    auto guard = control.Acquire();
    main_owners.clear();
    sub_owners.clear();
    if (control.PhaseLocked(guard) == EnginePhase::Idle || control.PhaseLocked(guard) == EnginePhase::Stopped) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    bindings.OwnerIdsLocked(guard, owners.MainLocked(guard) != nullptr, main_owners, sub_owners);
    return {};
}

result<void> EngineImpl::CoWaitRunning(const bbt::coroutine::WaitOptions& options)
{
    using R = result<void>;
    if (const auto gate = RequireCoroutineContext()) {
        return R::err(MakeError(ErrorCode::InvalidContext, "engine: CoWaitRunning requires coroutine context"));
    }
    if (options.deadline == bbt::coroutine::Deadline::max()) {
        return R::err(MakeError(ErrorCode::InvalidArgument, "engine: CoWaitRunning requires a finite deadline"));
    }
    auto impl = shared_from_this();
    for (;;) {
        EnginePhase phase;
        bool run_finished = false;
        {
            auto guard = control.Acquire();
            phase = control.PhaseLocked(guard);
            run_finished = control.RunFinishedLocked(guard);
        }
        if (phase == EnginePhase::Running) {
            return R::ok();
        }
        if (phase == EnginePhase::Stopped) {

            /**
             * 正常收口（无错误）→ Closed；启动失败/提前关闭按 LastError 映射。
             */
            return R::err(WaitRunningError(LastError()));
        }
        if (phase == EnginePhase::Closing && run_finished) {
            const auto error = LastError();
            return R::err(WaitRunningError(error ? error : std::make_error_code(std::errc::io_error)));
        }
        if (std::chrono::steady_clock::now() >= options.deadline) {
            return R::err(MakeError(ErrorCode::TimedOut, "engine: running wait deadline expired"));
        }

        /**
         * 谓词已在短锁内复查；等待在锁外分片挂起，不阻塞 worker。
         */
        auto waited = control.WaitPhaseSlice(options);
        if (!waited) {
            return R::err(waited.error());
        }
    }
}

std::error_code EngineImpl::CoWaitAccepted(Connection& out, const bbt::coroutine::WaitOptions& options)
{
    auto impl = shared_from_this();

    /**
     * 配置与生命周期错误立即返回，不等一个永远不会来的 listener。
     * 判定优先级是契约（见 Engine.hpp 的 Co 成员入口）：终态先于配置/布局判定。Stopped 时
     * main accept owner 已解绑，但那是收口带来的结果，不能据此把 Engine 判成
     * client-only（否则把"已结束"误报成"这类 Engine 不支持 accept"）。
     */
    {
        auto guard = control.Acquire();
        const bool service_sealed = IsSealed();
        if (service_sealed || control.PhaseLocked(guard) == EnginePhase::Closing
            || control.PhaseLocked(guard) == EnginePhase::Stopped) {

            /**
             * 已封口或已开始/完成收口：队列已清空，不会有新连接交付。
             */
            return std::make_error_code(std::errc::operation_canceled);
        }

        /**
         * 接受已停止（RequestStopAccepting 且本代 listener 已物理关）：仅在
         * Running 时作为接收域终态报 canceled；Idle/Starting 保持
         * operation_not_permitted（尚未建立 accept 生产者）。
         */
        if (accepts.StopRequested() && control.PhaseLocked(guard) == EnginePhase::Running) {
            return std::make_error_code(std::errc::operation_canceled);
        }
        if (control.PhaseLocked(guard) == EnginePhase::Idle || control.PhaseLocked(guard) == EnginePhase::Starting) {

            /**
             * 尚未发布 Running：还没有 accept 生产者，也不是配置错误。
             */
            return std::make_error_code(std::errc::operation_not_permitted);
        }
        if (!accepts.EnabledLocked(guard)) {
            return std::make_error_code(std::errc::operation_not_permitted);
        }
        if (!owners.MainLocked(guard)) {

            /**
             * Running + handoff 但没有 main accept owner：真正的 client-only 布局，
             * handoff 永远不会有生产者。
             */
            return std::make_error_code(std::errc::operation_not_supported);
        }
    }
    return accepts.WaitAccepted(out, options);
}

std::error_code
EngineImpl::CoWaitConnect(std::uint64_t connect_id, Connection& out, const bbt::coroutine::WaitOptions& options)
{
    if (handler)
        return std::make_error_code(std::errc::operation_not_supported);
    return connects.Wait(*this, connect_id, out, options);
}

void EngineImpl::ResumeAcceptIfFree()
{
    std::shared_ptr<Reactor> main;
    {
        auto guard = control.Acquire();
        main = owners.MainLocked(guard);
    }
    if (!main) {
        return;
    }
    if (!accepts.TryBeginResume()) {
        return;
    }
    if (!main->TryPost([main, this] {
            accepts.FinishResume();
            main->ArmAcceptAll();
        })) {
        accepts.FinishResume();
    }
}

std::shared_ptr<Reactor> EngineImpl::PickSub(const Address& peer)
{
    std::vector<std::shared_ptr<Reactor>> snapshot;
    {
        auto guard = control.Acquire();
        snapshot = owners.SubsLocked(guard);
    }
    return owners.Select(peer, snapshot, next_conn_id);
}

void EngineImpl::TryPostAcceptStop(const std::shared_ptr<Reactor>& main)
{
    if (!accepts.TryMarkPosted()) {
        return;
    }
    const bool inject_failure = TakeFault(FaultPoint::AcceptStopPost);
    if (inject_failure || !main->TryPostInternal([main] { main->StopAcceptingOnOwner(); })) {
        if (inject_failure) {
            InvokeHook(HookPoint::AfterAcceptStopPostFailure);
        }

        /**
         * post 拒绝不等于 fd 已关闭；保留持久请求，由控制循环再交。
         */
        accepts.RestoreRequestedAfterPostFailure();
    }
}

std::error_code EngineImpl::StartConnect(
    const Address& address,
    std::uint64_t& out_connect_id,
    bool awaitable,
    const std::shared_ptr<ReclaimRecord>& reclaim)
{
    auto impl = shared_from_this();

    /**
     * awaitable=true 是 CoDial 的提交步骤：要求无 raw handler（raw 配置下
     * OnConnect 是唯一交付，Co 不得提交一个无法领取的 op）。
     * awaitable=false 是公共 raw 提交：要求有 handler。
     */
    if (awaitable == (handler != nullptr)) {
        return std::make_error_code(std::errc::operation_not_supported);
    }
    std::uint64_t id = 0;
    {
        auto guard = control.Acquire();
        if (control.PhaseLocked(guard) != EnginePhase::Running || IsSealed()) {
            return std::make_error_code(std::errc::operation_canceled);
        }
        if (!owners.HasSubsLocked(guard)) {
            return std::make_error_code(std::errc::operation_not_supported);
        }
        id = connects.NextIdLocked(guard);
    }
    auto reactor = PickSub(address);
    if (!reactor) {
        return std::make_error_code(std::errc::operation_not_supported);
    }
    if (const auto error = connects.Insert(id, reactor, reclaim, awaitable, test_hooks))
        return error;

    /**
     * 私有 test：一次性"零接纳"投递失败注入。与真实失败走同一段回滚（清
     * receiver 意图、ResolveConnect 取消、清表项、out_id 不变、零回调），
     * 不构造假网络响应；未武装时行为与产品完全一致。
     */
    const bool force_post_failure = TakeFault(FaultPoint::ConnectPost);
    if (force_post_failure || !reactor->TryPost([impl, reactor, address, id, reclaim] {
            reactor->BeginConnectOnOwner(address, id, reclaim);
        })) {

        /**
         * 投递失败回滚：owner 从未执行，本次从未创建 fd，责任即时收口。
         */
        MarkReclaimReleased(reclaim);
        connects.RollbackPost(id);
        ResolveConnect(id, Connection(), std::make_error_code(std::errc::operation_canceled));
        return std::make_error_code(std::errc::operation_canceled);
    }
    {

        /**
         * 私有 test 屏障：已成功 TryPost（owner 可能立即完成并回调）、尚未写
         * out_connect_id。不持任何 impl 锁，owner 线程可自由推进。
         */
        InvokeHook(HookPoint::AfterConnectPostBeforeOutput);
    }
    out_connect_id = id;
    return {};
}

std::error_code StartConnectImpl(
    const std::shared_ptr<EngineImpl>& impl,
    const Address& address,
    std::uint64_t& out_connect_id,
    bool awaitable,
    const std::shared_ptr<ReclaimRecord>& reclaim)
{
    return impl->StartConnect(address, out_connect_id, awaitable, reclaim);
}

// clang-format off
}
// clang-format on
