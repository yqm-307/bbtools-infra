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
#include <bbt/infra/net/detail/WakeHub.hpp>

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
    // 所有 fixture 在 Run 发布 Reactor 前调用 ClearConnectInjection 完成装配。
    // 后续 setter 只修改 TestHooks 内部槽，连接持有的 shared_ptr 始终不变。
    if (!test_hooks) {
        std::lock_guard<std::mutex> lock(mutex);
        if (phase != EnginePhase::Idle)
            throw std::logic_error("test hooks must be installed before Run");
        test_hooks = std::make_shared<TestHooks>();
        accept_hub.SetHooksForTest(test_hooks);
    }
}

std::error_code EngineImpl::InjectDriverFailureForTest()
{
    std::lock_guard<std::mutex> lock(mutex);
    if (phase != EnginePhase::Running || slots.empty())
        return std::make_error_code(std::errc::operation_not_permitted);
    slots.back().driver->InjectTickFailureOnce();
    return {};
}

void EngineImpl::RestrictConnectCapacityForTest(std::size_t limit)
{
    std::lock_guard<std::mutex> lock(mutex);
    connect_limit = limit == 0 ? 1 : limit;
}

EngineTestSnapshot EngineImpl::InspectForTest()
{
    EngineTestSnapshot snapshot;
    {
        std::lock_guard<std::mutex> lock(connect_mutex);
        snapshot.m_connects = connects.size();
        for (auto& entry : connects)
            if (!entry.second->InspectForTest().m_done)
                ++snapshot.m_pendingConnects;
    }
    std::vector<std::shared_ptr<Reactor>> owners;
    {
        std::lock_guard<std::mutex> lock(mutex);
        owners = reactors;
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

ConnectTestSnapshot EngineImpl::InspectConnectForTest(std::uint64_t id)
{
    auto op = FindConnect(id);
    return op ? op->InspectForTest() : ConnectTestSnapshot{};
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
            std::lock_guard<std::mutex> lock(mutex);
            owners = reactors;
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
    auto op = FindConnect(id);
    return op ? op->OccupyWaitersForTest(keep) : 0;
}

void EngineImpl::RequestOwnerPumpForTest()
{
    std::vector<std::shared_ptr<Reactor>> owners;
    {
        std::lock_guard<std::mutex> lock(mutex);
        owners = reactors;
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

void EngineImpl::ResumeAcceptIfFree()
{
    std::shared_ptr<Reactor> main;
    {
        std::lock_guard<std::mutex> lock(mutex);
        main = main_reactor;
    }
    if (!main) {
        return;
    }
    if (resume_pending.exchange(true)) {
        return;
    }
    if (!main->TryPost([main, this] {
            resume_pending.store(false);
            main->ArmAcceptAll();
        })) {
        resume_pending.store(false);
    }
}

std::shared_ptr<Reactor> EngineImpl::PickSub(const Address& peer)
{
    std::vector<std::shared_ptr<Reactor>> snapshot;
    {
        std::lock_guard<std::mutex> lock(mutex);
        snapshot = subs;
    }
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

void EngineImpl::TryPostAcceptStop(const std::shared_ptr<Reactor>& main)
{
    if (accept_stop_done.load(std::memory_order_acquire)
        || accept_stop_posted.exchange(true, std::memory_order_acq_rel)) {
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
        accept_stop_posted.store(false, std::memory_order_release);
    }
}

void DetailNoteHandlerError(EngineImpl* impl)
{
    impl->NoteHandlerError();
}

HandoffReservation::~HandoffReservation()
{
    if (impl != nullptr && counted && !committed) {
        impl->ReleaseHandoffReservation();
    }
}

std::error_code StartConnectImpl(
    const std::shared_ptr<EngineImpl>& impl,
    const Address& address,
    std::uint64_t& out_connect_id,
    bool awaitable,
    const std::shared_ptr<ReclaimRecord>& reclaim)
{

    /**
     * awaitable=true 是 CoDial 的提交步骤：要求无 raw handler（raw 配置下
     * OnConnect 是唯一交付，Co 不得提交一个无法领取的 op）。
     * awaitable=false 是公共 raw 提交：要求有 handler。
     */
    if (awaitable == (impl->handler != nullptr)) {
        return std::make_error_code(std::errc::operation_not_supported);
    }
    std::uint64_t id = 0;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        if (impl->phase != EnginePhase::Running || impl->IsSealed()) {
            return std::make_error_code(std::errc::operation_canceled);
        }
        if (impl->subs.empty()) {
            return std::make_error_code(std::errc::operation_not_supported);
        }
        id = impl->next_connect_id.fetch_add(1, std::memory_order_relaxed);
    }
    auto reactor = impl->PickSub(address);
    if (!reactor) {
        return std::make_error_code(std::errc::operation_not_supported);
    }
    {
        std::lock_guard<std::mutex> lock(impl->connect_mutex);

        /**
         * 每次先清理已完结且无人等待项；若仍达硬上限（全 pending）则零接纳。
         */
        for (auto it = impl->connects.begin(); it != impl->connects.end();) {
            bool removable = false;
            {
                std::lock_guard<std::mutex> op_lock(it->second->mutex);

                /**
                 * 与 MaybeEraseConnect 同条件：未转移且未释放的记录留在表内兜底。
                 */
                removable = it->second->done && !it->second->waiter_attached
                            && (!it->second->reclaim || it->second->reclaim->Released());
            }
            if (removable) {
                it = impl->connects.erase(it);
            } else {
                ++it;
            }
        }
        if (impl->connects.size() >= impl->connect_limit) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        auto op = std::make_shared<EngineImpl::ConnectOp>();
        op->hub.SetHooksForTest(impl->test_hooks);
        op->reactor = reactor;
        op->reclaim = reclaim;

        /**
         * Co 提交即登记接收意图，完成早到不丢
         */
        op->waiter_attached = awaitable;
        impl->connects[id] = std::move(op);
    }

    /**
     * 私有 test：一次性"零接纳"投递失败注入。与真实失败走同一段回滚（清
     * receiver 意图、ResolveConnect 取消、清表项、out_id 不变、零回调），
     * 不构造假网络响应；未武装时行为与产品完全一致。
     */
    const bool force_post_failure = impl->TakeFault(FaultPoint::ConnectPost);
    if (force_post_failure || !reactor->TryPost([impl, reactor, address, id, reclaim] {
            reactor->BeginConnectOnOwner(address, id, reclaim);
        })) {

        /**
         * 投递失败回滚：owner 从未执行，本次从未创建 fd，责任即时收口。
         */
        MarkReclaimReleased(reclaim);
        {
            std::lock_guard<std::mutex> lock(impl->connect_mutex);
            const auto it = impl->connects.find(id);
            if (it != impl->connects.end()) {
                std::lock_guard<std::mutex> op_lock(it->second->mutex);
                it->second->waiter_attached = false;
            }
        }
        impl->ResolveConnect(id, Connection(), std::make_error_code(std::errc::operation_canceled));
        return std::make_error_code(std::errc::operation_canceled);
    }
    {

        /**
         * 私有 test 屏障：已成功 TryPost（owner 可能立即完成并回调）、尚未写
         * out_connect_id。不持任何 impl 锁，owner 线程可自由推进。
         */
        impl->InvokeHook(HookPoint::AfterConnectPostBeforeOutput);
    }
    out_connect_id = id;
    return {};
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

// clang-format off
}
// clang-format on
