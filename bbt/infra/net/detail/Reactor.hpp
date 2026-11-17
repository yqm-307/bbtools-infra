#pragma once

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>
#include <vector>
#include <boost/asio/post.hpp>
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
    void SealServiceOnOwner(std::function<void()> on_done = {});

    /**
     * 接收域独立封口：撤该 owner 的全部 listener 并物理关闭 fd；不触已交付
     * 连接，也不停 driver。未绑 owner 时直接视为已停止。
     */
    void StopAcceptingOnOwner();
    void FinishConnectCallback();
    void FinalizeControl();
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
        const Address& address, std::uint64_t connect_id, const std::shared_ptr<ReclaimRecord>& reclaim = {});
    void StartConnectSocketOnOwner(
        const Address& address, std::uint64_t connect_id, const std::shared_ptr<ReclaimRecord>& reclaim = {});
    void CancelAllRequestedOnOwner();

    void MarkDirty(const std::shared_ptr<ConnCore>& core);

    void SetDirtyBarrier(std::function<void()> barrier)
    {
        std::lock_guard<std::mutex> lock(m_barrier_mutex);
        m_dirty_barrier = std::move(barrier);
    }

    void SetShutdownBarrier(std::function<void()> barrier)
    {
        std::lock_guard<std::mutex> lock(m_shutdown_barrier_mutex);
        m_shutdown_barrier = std::move(barrier);
    }

    void SetWriteReadyBarrier(std::function<void()> barrier)
    {
        std::lock_guard<std::mutex> lock(m_write_ready_barrier_mutex);
        m_write_ready_barrier = std::move(barrier);
    }

    void InvokeWriteReadyBarrier()
    {
        std::function<void()> barrier;
        {
            std::lock_guard<std::mutex> lock(m_write_ready_barrier_mutex);
            barrier = std::move(m_write_ready_barrier);
            m_write_ready_barrier = {};
        }
        if (barrier) {
            barrier();
        }
    }

    void InvokeShutdownBarrier()
    {
        std::function<void()> barrier;
        {
            std::lock_guard<std::mutex> lock(m_shutdown_barrier_mutex);
            barrier = std::move(m_shutdown_barrier);
            m_shutdown_barrier = {};
        }
        if (barrier) {
            barrier();
        }
    }

    void SetRemoveBarrier(std::function<void()> barrier)
    {
        std::lock_guard<std::mutex> lock(m_remove_barrier_mutex);
        m_remove_barrier = std::move(barrier);
    }

    void InvokeRemoveBarrier()
    {
        std::function<void()> barrier;
        {
            std::lock_guard<std::mutex> lock(m_remove_barrier_mutex);
            barrier = std::move(m_remove_barrier);
            m_remove_barrier = {};
        }
        if (barrier) {
            barrier();
        }
    }

    void SetPreAuthBarrier(std::function<void()> barrier)
    {
        std::lock_guard<std::mutex> lock(m_auth_barrier_mutex);
        m_pre_auth_barrier = std::move(barrier);
    }

    void SetConnectStartBarrier(std::function<void()> barrier)
    {
        std::lock_guard<std::mutex> lock(m_connect_start_barrier_mutex);
        m_connect_start_barrier = std::move(barrier);
    }

    /**
     * owner 已占用本次 pending connect 回调预算、尚未做起点封口检查时调用。
     */
    void InvokeConnectStartBarrier()
    {
        std::function<void()> barrier;
        {
            std::lock_guard<std::mutex> lock(m_connect_start_barrier_mutex);
            barrier = std::move(m_connect_start_barrier);
            m_connect_start_barrier = {};
        }
        if (barrier) {
            barrier();
        }
    }

    void SetPostAuthBarrier(std::function<void()> barrier)
    {
        std::lock_guard<std::mutex> lock(m_auth_barrier_mutex);
        m_post_auth_barrier = std::move(barrier);
    }

    void InvokePreAuthBarrier()
    {
        std::function<void()> barrier;
        {
            std::lock_guard<std::mutex> lock(m_auth_barrier_mutex);
            barrier = std::move(m_pre_auth_barrier);
            m_pre_auth_barrier = {};
        }
        if (barrier) {
            barrier();
        }
    }

    void InvokePostAuthBarrier()
    {
        std::function<void()> barrier;
        {
            std::lock_guard<std::mutex> lock(m_auth_barrier_mutex);
            barrier = std::move(m_post_auth_barrier);
            m_post_auth_barrier = {};
        }
        if (barrier) {
            barrier();
        }
    }

    void SetSubmitBarrier(std::function<void()> barrier)
    {
        std::lock_guard<std::mutex> lock(m_submit_barrier_mutex);
        m_submit_barrier = std::move(barrier);
    }

    void KickPumpOwner()
    {
        auto self = shared_from_this();
        (void)TryPostInternal([self] { self->PumpAllConnsOwner(); });
    }

    void CancelConnectOnOwner(std::uint64_t connect_id);
    void ResumeMainAccept();
    std::size_t DirtyCount();
    std::size_t RecvHoldBytes();
    int FirstConnFd();

    /**
     * 私有 test 屏障：owner recv 后、ring append 前调用（正常为空）。
     */
    void SetReadAppendHook(std::function<void()> hook) { m_read_append_hook = std::move(hook); }

    void InvokeReadAppendHook()
    {
        if (m_read_append_hook) {
            m_read_append_hook();
        }
    }

    /**
     * 私有 test 接缝：TryAppend 观测 Busy 之后、AppendDeferred 之前调用（正常为空）。
     * 用于确定性制造「Busy 已发生 → 读者释放 → deferred 成功」的交错；产品路径为空时无差异。
     */
    void SetAppendBusyHook(std::function<void()> hook) { m_append_busy_hook = std::move(hook); }

    void InvokeAppendBusyHook()
    {
        if (m_append_busy_hook) {
            m_append_busy_hook();
        }
    }

    /**
     * 不构造 std::function（避免回调派发前的分配/抛出）；用户异常仍隔离。
     */
    template <class Callback> void SafeHandler(Callback&& callback)
    {
        try {
            callback();
        } catch (...) {
            DetailNoteHandlerError(m_impl);
        }
    }

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

    EngineImpl* m_impl;
    std::size_t m_index;
    std::mutex m_driver_mutex;
    std::shared_ptr<ReactorDriver> m_driver;
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
    std::mutex m_mutex;
    std::unordered_map<std::uint64_t, std::shared_ptr<ConnCore>> m_conns;
    std::mutex m_dirty_mutex;
    ConnCore* m_dirty_head{nullptr};
    ConnCore* m_dirty_tail{nullptr};

    /**
     * owner-only
     */
    std::size_t m_pending_connect_callbacks{0};
    std::function<void()> m_on_pending_zero;
    std::mutex m_barrier_mutex;

    /**
     * 私有 test 屏障（一次性）
     */
    std::function<void()> m_dirty_barrier;
    std::mutex m_remove_barrier_mutex;

    /**
     * 撤链后/擦表前（一次性）
     */
    std::function<void()> m_remove_barrier;
    std::mutex m_shutdown_barrier_mutex;

    /**
     * Shutdown stage 前（一次性）
     */
    std::function<void()> m_shutdown_barrier;
    std::mutex m_write_ready_barrier_mutex;

    /**
     * 写泵持锁前（一次性）
     */
    std::function<void()> m_write_ready_barrier;
    std::mutex m_submit_barrier_mutex;

    /**
     * 私有 test 屏障（一次性）
     */
    std::function<void()> m_submit_barrier;
    std::mutex m_connect_start_barrier_mutex;

    /**
     * 建连起点/封口检查前（一次性）
     */
    std::function<void()> m_connect_start_barrier;
    std::mutex m_auth_barrier_mutex;
    std::function<void()> m_pre_auth_barrier;
    std::function<void()> m_post_auth_barrier;
    std::function<void()> m_read_append_hook;

    /**
     * 私有 test：TryAppend Busy 后/deferred 前（正常为空）
     */
    std::function<void()> m_append_busy_hook;
};

template <class Handler> bool Reactor::TryPostInternal(Handler&& handler)
{

    /**
     * 提交资格与 driver 租约同一窄门：持锁跨 post，ReleaseDriver 只能在
     * post 完成后 reset，已接纳命令必进入封口前的 context 队列。
     */
    std::lock_guard<std::mutex> lock(m_driver_mutex);
    if (!m_driver) {
        return false;
    }
    try {
        asio::post(m_driver->Context(), std::forward<Handler>(handler));
        return true;
    } catch (...) {
        return false;
    }
}

template <class Handler> bool Reactor::TryPost(Handler&& handler)
{

    /**
     * 提交门：持 driver 租约跨 sealed 检查与 post，使「资格→入队」与
     * ReleaseDriver 的 reset 线性化。hold 只覆盖一次短 post（Asio do_run_one
     * 在 complete 前解锁，不构成 scheduler 反序死锁），
     * 不会跨同步解绑：ReleaseDriver 等该锁，post 返回即释放。
     */
    std::lock_guard<std::mutex> lock(m_driver_mutex);
    if (m_sealed.load() || m_impl->IsSealed() || !m_driver) {

        /**
         * 封口后/已解绑：零接纳
         */
        return false;
    }
    {
        std::function<void()> barrier;
        {
            std::lock_guard<std::mutex> barrier_lock(m_submit_barrier_mutex);
            barrier = std::move(m_submit_barrier);
            m_submit_barrier = {};
        }
        if (barrier) {

            /**
             * 测试屏障：已取得提交资格、尚未 post（仍持 driver 锁）
             */
            barrier();
        }
    }
    try {
        asio::post(m_driver->Context(), std::forward<Handler>(handler));
        return true;
    } catch (...) {
        return false;
    }
}

// clang-format off
}
// clang-format on
