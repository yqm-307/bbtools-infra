#pragma once

// 测试槽跟随 Engine 或独立 fixture 的寿命；回调在槽锁之外执行。
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <mutex>
#include <unordered_map>

namespace bbt::infra::net {

enum class HookPoint : std::size_t {
    BeforeCallbackAuthorization,
    AfterCallbackAuthorization,
    AfterReceiveBeforeAppend,
    AfterAppendBusy,
    AfterDirtyWeakLock,
    AfterDirtyUnlinkBeforeErase,
    BeforeShutdownStage,
    BeforeWriteReadyPump,
    BeforeDriverPost,
    AfterConnectPostBeforeOutput,
    BeforeConnectStartAuthorization,
    AfterDialSubmitBeforeAttach,
    AfterAcceptRegistered,
    AfterWriteRegistered,
    AfterCloseRegistered,
    BeforeBusyWaiter,
    BeforeDataWaiter,
    AfterReadRegistered,
    BeforeOutputAppend,
    AfterAcceptStopPostFailure,
    DuringReadProtection,
    BeforeOwnerWake,
    Count
};
enum class FaultPoint : std::size_t {
    CreateWaiter,
    NotifyData,
    ConnectPost,
    CancelPost,
    SealPost,
    SealThrow,
    AcceptStopPost,
    WaitDriverFailed,
    Count
};
enum class DelayPoint : std::size_t { ConnectDelivery, ConnectStart, Seal, Install, Count };

struct EngineTestSnapshot {
    std::size_t m_connections{0};
    std::size_t m_dirty{0};
    std::size_t m_recvHold{0};
    std::size_t m_connects{0};
    std::size_t m_pendingConnects{0};
    int m_firstFd{-1};
};

struct ConnectTestSnapshot {
    bool m_exists{false};
    bool m_done{false};
    bool m_receiverClaimed{false};
};

class TestHooks {
public:
    TestHooks();
    void Arm(HookPoint point, std::function<void()> hook, bool once);
    void Invoke(HookPoint point);
    // Reactor 原一次性槽各自消费，不能由另一 owner 提前取走。
    void ArmOwner(HookPoint point, std::size_t owner, std::function<void()> hook, bool once);
    void InvokeOwner(HookPoint point, std::size_t owner);
    void FailOnce(FaultPoint point);
    bool TakeFault(FaultPoint point);
    void DelayAt(DelayPoint point, std::chrono::milliseconds delay);
    std::chrono::milliseconds Delay(DelayPoint point) const;
    void OverrideConnectWaitOnce(int status);
    int TakeConnectWaitStatus();
    void ClearConnectInjection();

private:
    struct Slot {
        std::function<void()> m_hook;
        bool m_once{false};
    };

    std::function<void()> TakeLocked(Slot& slot);
    std::mutex m_mutex;
    std::array<Slot, static_cast<std::size_t>(HookPoint::Count)> m_hooks;
    std::unordered_map<std::size_t, Slot> m_ownerHooks;
    std::array<std::atomic<bool>, static_cast<std::size_t>(FaultPoint::Count)> m_faults;
    std::array<std::atomic<std::int64_t>, static_cast<std::size_t>(DelayPoint::Count)> m_delays;
    std::atomic<int> m_connectWaitStatus{-1};
};

} // namespace bbt::infra::net
