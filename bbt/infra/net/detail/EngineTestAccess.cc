#include <bbt/infra/net/detail/EngineTestAccess.hpp>

#include <utility>

#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/detail/EngineState.hpp>
#include <bbt/infra/net/detail/TestHooks.hpp>

namespace bbt::infra::net {

bool EngineTestAccess::RemoveConn(Engine& engine, std::uint64_t id)
{
    return engine.m_impl->RemoveConnForTest(id);
}

void EngineTestAccess::ArmFault(Engine& engine, FaultPoint point)
{
    engine.ArmFaultForTest(static_cast<std::size_t>(point));
}

void EngineTestAccess::ArmHook(Engine& engine, HookPoint point, std::function<void()> hook, bool once)
{
    engine.ArmHookForTest(static_cast<std::size_t>(point), std::move(hook), once);
}

std::error_code EngineTestAccess::InjectDriverFailure(Engine& engine)
{
    return engine.InjectDriverFailureForTest();
}

void EngineTestAccess::SetConnectLimit(Engine& engine, std::size_t limit)
{
    engine.RestrictConnectCapacityForTest(limit);
}

std::size_t EngineTestAccess::ConnectTableSize(Engine& engine)
{
    return engine.InspectForTest().m_connects;
}

void EngineTestAccess::SetConnectStartBarrierOnce(Engine& engine, std::function<void()> barrier)
{
    engine.ArmHookForTest(
        static_cast<std::size_t>(HookPoint::BeforeConnectStartAuthorization), std::move(barrier), true);
}

std::size_t EngineTestAccess::PendingConnectCount(Engine& engine)
{
    return engine.InspectForTest().m_pendingConnects;
}

void EngineTestAccess::SetConnectDeliveryDelay(Engine& engine, std::chrono::milliseconds delay)
{
    engine.DelayForTest(static_cast<std::size_t>(DelayPoint::ConnectDelivery), delay);
}

std::size_t EngineTestAccess::DirtyCount(Engine& engine)
{
    return engine.InspectForTest().m_dirty;
}

std::size_t EngineTestAccess::ConnectionCount(Engine& engine)
{
    return engine.InspectForTest().m_connections;
}

void EngineTestAccess::SetSubmitBarrierOnce(Engine& engine, std::function<void()> barrier)
{
    engine.ArmHookForTest(static_cast<std::size_t>(HookPoint::BeforeDriverPost), std::move(barrier), true);
}

void EngineTestAccess::SetConnectSubmitBarrierOnce(Engine& engine, std::function<void()> barrier)
{
    engine.ArmHookForTest(static_cast<std::size_t>(HookPoint::AfterConnectPostBeforeOutput), std::move(barrier), true);
}

void EngineTestAccess::SetConnectPostFailureOnce(Engine& engine)
{
    engine.ArmFaultForTest(static_cast<std::size_t>(FaultPoint::ConnectPost));
}

void EngineTestAccess::SetConnectWaitStatusOnce(Engine& engine, int status)
{
    engine.OverrideWaitOnceForTest(status);
}

void EngineTestAccess::ClearConnectInjection(Engine& engine)
{
    engine.ClearConnectInjectionForTest();
}

void EngineTestAccess::KickOwner(Engine& engine)
{
    engine.RequestOwnerPumpForTest();
}

void EngineTestAccess::SetSealDelay(Engine& engine, std::chrono::milliseconds delay)
{
    engine.DelayForTest(static_cast<std::size_t>(DelayPoint::Seal), delay);
}

void EngineTestAccess::SetAuthBarriers(Engine& engine, std::function<void()> pre_auth, std::function<void()> post_auth)
{
    engine.ArmHookForTest(static_cast<std::size_t>(HookPoint::BeforeCallbackAuthorization), std::move(pre_auth), true);
    engine.ArmHookForTest(static_cast<std::size_t>(HookPoint::AfterCallbackAuthorization), std::move(post_auth), true);
}

bool EngineTestAccess::ConnectReceiverClaimed(Engine& engine, std::uint64_t id)
{
    return engine.InspectConnectForTest(id).m_receiverClaimed;
}

std::size_t EngineTestAccess::RecvHoldBytes(Engine& engine)
{
    return engine.InspectForTest().m_recvHold;
}

std::size_t EngineTestAccess::FillConnectHub(
    Engine& engine, std::uint64_t id, std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>>& keep)
{
    return engine.OccupyConnectWaitersForTest(id, keep);
}

void EngineTestAccess::SetWriteReadyBarrierOnce(Engine& engine, std::function<void()> barrier)
{
    engine.ArmHookForTest(static_cast<std::size_t>(HookPoint::BeforeWriteReadyPump), std::move(barrier), true);
}

void EngineTestAccess::SetConnectStartDelay(Engine& engine, std::chrono::milliseconds delay)
{
    engine.DelayForTest(static_cast<std::size_t>(DelayPoint::ConnectStart), delay);
}

void EngineTestAccess::SetCancelPostFailureOnce(Engine& engine)
{
    engine.ArmFaultForTest(static_cast<std::size_t>(FaultPoint::CancelPost));
}

void EngineTestAccess::SetShutdownBarrierOnce(Engine& engine, std::function<void()> barrier)
{
    engine.ArmHookForTest(static_cast<std::size_t>(HookPoint::BeforeShutdownStage), std::move(barrier), true);
}

void EngineTestAccess::SetRemoveBarrierOnce(Engine& engine, std::function<void()> barrier)
{
    engine.ArmHookForTest(static_cast<std::size_t>(HookPoint::AfterDirtyUnlinkBeforeErase), std::move(barrier), true);
}

void EngineTestAccess::SetDirtyBarrierOnce(Engine& engine, std::function<void()> barrier)
{
    engine.ArmHookForTest(static_cast<std::size_t>(HookPoint::AfterDirtyWeakLock), std::move(barrier), true);
}

void EngineTestAccess::FailNextConnectWaiterCreateOnce(Engine& engine)
{
    engine.ArmFaultForTest(static_cast<std::size_t>(FaultPoint::CreateWaiter));
}

void EngineTestAccess::SetWaitDriverFailedOnce(Engine& engine)
{
    engine.ArmFaultForTest(static_cast<std::size_t>(FaultPoint::WaitDriverFailed));
}

void EngineTestAccess::SetSealThrowOnce(Engine& engine)
{
    engine.ArmFaultForTest(static_cast<std::size_t>(FaultPoint::SealThrow));
}

void EngineTestAccess::SetAcceptWaiterHook(Engine& engine, std::function<void()> registered)
{
    engine.ArmHookForTest(static_cast<std::size_t>(HookPoint::AfterAcceptRegistered), std::move(registered), false);
}

void EngineTestAccess::SetAcceptStopPostFailureOnce(Engine& engine, std::function<void()> after_failure)
{
    engine.ArmHookForTest(
        static_cast<std::size_t>(HookPoint::AfterAcceptStopPostFailure), std::move(after_failure), false);
    engine.ArmFaultForTest(static_cast<std::size_t>(FaultPoint::AcceptStopPost));
}

void EngineTestAccess::SetInstallDelay(Engine& engine, std::chrono::milliseconds delay)
{
    engine.DelayForTest(static_cast<std::size_t>(DelayPoint::Install), delay);
}

void EngineTestAccess::SetSealPostFailureOnce(Engine& engine)
{
    engine.ArmFaultForTest(static_cast<std::size_t>(FaultPoint::SealPost));
}

void EngineTestAccess::SetReadAppendHook(Engine& engine, std::function<void()> hook)
{
    engine.ArmHookForTest(static_cast<std::size_t>(HookPoint::AfterReceiveBeforeAppend), std::move(hook), false);
}

void EngineTestAccess::SetAppendBusyHook(Engine& engine, std::function<void()> hook)
{
    engine.ArmHookForTest(static_cast<std::size_t>(HookPoint::AfterAppendBusy), std::move(hook), false);
}

int EngineTestAccess::FirstConnectionFd(Engine& engine)
{
    return engine.InspectForTest().m_firstFd;
}

} // namespace bbt::infra::net
