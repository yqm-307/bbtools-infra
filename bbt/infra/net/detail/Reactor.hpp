#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <bbt/infra/net/detail/EngineState.hpp>
#include <bbt/infra/net/detail/ReactorDriver.hpp>

namespace bbt::infra::net {

/**
 * 单个 Reactor owner（main 或 sub）。
 */
class Reactor : public std::enable_shared_from_this<Reactor> {
public:
    Reactor(EngineImpl* impl, std::size_t index, std::shared_ptr<ReactorDriver> driver);
    ~Reactor();

    /**
     * 端点租约：外部提交只经 TryPost 安全获取当前 driver；解绑后返回 false。
     * 定义在 EngineImpl 完整之后（需要 IsSealed 的门禁）。
     */
    template <class Handler> bool TryPost(Handler&& handler);

    /**
     * 内部收口命令：允许在 service_sealed 之后由控制域投递（仅 Run 使用）。
     */
    template <class Handler> bool TryPostInternal(Handler&& handler);

    /**
     * owner 安装：设置 tick 钩子 + 创建/挂 listener。
     */
    std::error_code InstallOnOwner(const std::vector<Address>& addresses);
    std::vector<Address> LocalAddresses() const;

    bool HasListener() const { return !m_listeners.empty(); }

    /**
     * owner 封服务 / 控制域冻结 / 解绑后释放 driver。
     */
    bool SealServiceOnOwner(std::function<void()> on_done);

    /**
     * 接收域独立封口：撤该 owner 的全部 listener 并物理关闭 fd；不触已交付
     * 连接，也不停 driver。未绑 owner 时直接视为已停止。
     */
    void StopAcceptingOnOwner();
    void FinishConnectCallback();
    std::error_code FinalizeControl();
    void ReleaseDriver();

    /**
     * owner：从 native fd 接管（handoff / connect 完成）。reclaim 是带句柄
     * CoDial 的本次责任记录（accept/raw 路径为空，采用时另建活跃记录）。
     */
    void AdoptFdOnOwner(
        FdHandle fd,
        bool is_connect,
        std::uint64_t connect_id,
        std::shared_ptr<HandoffReservation> reservation,
        bool delayed_delivery = false,
        const std::shared_ptr<ReclaimRecord>& reclaim = {});
    void BeginConnectOnOwner(
        const Address& address,
        std::uint64_t connect_id,
        const std::shared_ptr<ReclaimRecord>& reclaim = {});
    void StartConnectSocketOnOwner(
        const Address& address,
        std::uint64_t connect_id,
        const std::shared_ptr<ReclaimRecord>& reclaim = {});
    void CancelAllRequestedOnOwner();

    void MarkDirty(const std::shared_ptr<ConnCore>& core);
    void MarkDirty(const std::weak_ptr<ConnCore>& core);

    void ArmHookForTest(HookPoint point, std::function<void()> hook, bool once);

    void InvokeHook(HookPoint point);

    void InvokeWriteReadyBarrier() { InvokeHook(HookPoint::BeforeWriteReadyPump); }

    void InvokeShutdownBarrier() { InvokeHook(HookPoint::BeforeShutdownStage); }

    void InvokeRemoveBarrier() { InvokeHook(HookPoint::AfterDirtyUnlinkBeforeErase); }

    void InvokePreAuthBarrier() { InvokeHook(HookPoint::BeforeCallbackAuthorization); }

    void InvokePostAuthBarrier() { InvokeHook(HookPoint::AfterCallbackAuthorization); }

    void InvokeConnectStartBarrier() { InvokeHook(HookPoint::BeforeConnectStartAuthorization); }

    void KickPumpOwner();

    void CancelConnectOnOwner(std::uint64_t connect_id);
    void ResumeMainAccept();
    std::size_t DirtyCount();
    std::size_t RecvHoldBytes();
    int FirstConnFd();
    bool RemoveConnForTest(std::uint64_t id);

    void InvokeReadAppendHook() { InvokeHook(HookPoint::AfterReceiveBeforeAppend); }

    void InvokeAppendBusyHook() { InvokeHook(HookPoint::AfterAppendBusy); }

    /**
     * 不构造 std::function（避免回调派发前的分配/抛出）；用户异常仍隔离。
     */
    template <class Callback> void SafeHandler(Callback&& callback);

    void ArmAcceptAll();
    void ArmAccept(const std::shared_ptr<Listener>& listener);
    void RecordAcceptError(std::error_code error);
    bool ImplSealed() const;
    std::size_t ReadLowWater() const;
    std::size_t ConnCount();

    EngineImpl* Impl() { return m_impl; }

private:
    /**
     * owner 内部使用
     */
    asio::io_context& Ctx() { return m_driver->Context(); }

    void DoAccept(const std::shared_ptr<Listener>& listener, std::shared_ptr<HandoffReservation> reservation);
    void RemoveConn(const std::shared_ptr<ConnCore>& core);
    void PumpOwner();
    void PumpAllConnsOwner();
    void _SealMembers();

    EngineImpl* m_impl;
    std::size_t m_index;
    std::mutex m_driver_mutex;
    std::shared_ptr<ReactorDriver> m_driver;
    const std::weak_ptr<ReactorDriver> m_contextLease;
    const std::shared_ptr<std::atomic<bool>> m_serviceSeal;
    const std::shared_ptr<TestHooks> m_wakeHooks;
    std::atomic<bool> m_installed{false};
    std::atomic<bool> m_sealed{false};
    std::vector<std::shared_ptr<Listener>> m_listeners;

    /**
     * owner-only
     */
    std::vector<std::shared_ptr<ConnectPending>> m_pending_connects;

    /**
     * 已建连、交付定时器仍捕获的 fd；正常由 owner 消费，解绑后由控制域显式关。
     * 不能靠 driver/context 最后一个强引用析构来证明物理关闭。
     */
    std::vector<std::shared_ptr<FdHandle>> m_delivery_fds;

    struct DispatchEntry {
        std::shared_ptr<ConnCore> core;
        std::uint64_t next{0};
        bool queued{false};
    };

    std::mutex m_membersMutex;
    std::unordered_map<std::uint64_t, DispatchEntry> m_conns;
    std::uint64_t m_dirtyHead{0};
    std::uint64_t m_dirtyTail{0};

    /**
     * owner-only
     */
    std::size_t m_pending_connect_callbacks{0};
    std::function<void()> m_on_pending_zero;
};

// clang-format off
}
// clang-format on

#include <bbt/infra/net/detail/__TReactor.hpp>
