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

/**
 * ---- Engine ----
 */
Engine::Engine(std::shared_ptr<RawHandler> handler)
    : m_impl(std::make_shared<EngineImpl>(std::move(handler)))
{
}

Engine::~Engine()
{

    /**
     * 合法控制域上的最后兜底：Run 因解绑失败保留 Closing 时，析构前再尝试
     * 同代同步解绑 + 冻结收口；成功才释放责任，失败则无 reaper 可再保证。
     */
    (void)FinalizeNative();
}

std::error_code Engine::Close(const CloseOptions& options) noexcept
{
    if (options.mode != CloseMode::Immediate || options.wait_release
        || (options.reason && !options.reason->message.empty()))
        return std::make_error_code(std::errc::invalid_argument);
    auto impl = m_impl;
    if (!impl) {
        return {};
    }
    try {
        {
            std::lock_guard<std::mutex> lock(impl->mutex);
            impl->close_requested = true;
        }
        impl->SealService();
        impl->control_cv.notify_all();
    } catch (...) {
    }
    return {};
}

void Engine::RequestStopAccepting() noexcept
{
    auto impl = m_impl;
    if (!impl) {
        return;
    }
    try {

        /**
         * 不可逆 latch：仅第一次真实执行封口，并发/重复请求幂等、不重开。
         */
        if (impl->accept_stop_requested.exchange(true, std::memory_order_acq_rel)) {
            return;
        }

        /**
         * 同一接纳门，封口路径不创建可能分配失败的临时 deque。
         */
        {
            std::lock_guard<std::mutex> lock(impl->handoff_mutex);
            impl->handoff_closed = true;
            impl->handoff_reserved = 0;
        }
        impl->accept_hub.NotifyAll();
        for (;;) {
            Connection connection;
            {
                std::lock_guard<std::mutex> lock(impl->handoff_mutex);
                if (impl->handoff.empty()) {
                    break;
                }
                connection = std::move(impl->handoff.front());
                impl->handoff.pop_front();
            }
            (void)connection.Close({}, std::make_error_code(std::errc::operation_canceled));
        }

        /**
         * 物理关闭 listener 交由主接受者 owner 执行（真实 close 后才 stopped）。
         * client-only / 尚未 Run / owner 已解绑：本代无 listener，停止即生效。
         */
        std::shared_ptr<Reactor> main;
        EnginePhase phase;
        {
            std::lock_guard<std::mutex> lock(impl->mutex);
            main = impl->main_reactor;
            phase = impl->phase;
        }
        if (main) {
            impl->TryPostAcceptStop(main);
        } else if (phase == EnginePhase::Idle || phase == EnginePhase::Running || phase == EnginePhase::Stopped) {
            impl->MarkAcceptStopDone();
        }

        /**
         * Starting/Closing 尚未发布或已移走 owner 时，必须等安装回滚/解绑收口。
         */
        impl->control_cv.notify_all();
    } catch (...) {
    }
}

bool Engine::AcceptingStopped() const noexcept
{
    auto impl = m_impl;
    if (!impl) {
        return false;
    }
    return impl->accept_stop_requested.load(std::memory_order_acquire)
           && impl->accept_stop_done.load(std::memory_order_acquire);
}

EnginePhase Engine::Phase() const noexcept
{
    std::lock_guard<std::mutex> lock(m_impl->mutex);
    return m_impl->phase;
}

std::error_code Engine::LastError() const
{
    return m_impl->LastError();
}

std::exception_ptr Engine::LastFailure() const
{
    return m_impl->LastFailure();
}

std::error_code Engine::EnableAcceptHandoff(std::size_t capacity)
{
    if (capacity == 0) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    std::lock_guard<std::mutex> lock(m_impl->mutex);
    if (m_impl->phase != EnginePhase::Idle) {
        return std::make_error_code(std::errc::device_or_resource_busy);
    }
    m_impl->handoff_capacity = capacity;
    m_impl->handoff_enabled = true;
    return {};
}

std::error_code Engine::Run(const EngineConfig& config)
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
    bool expected = false;
    if (!m_impl->control_claimed.compare_exchange_strong(expected, true)) {
        return std::make_error_code(std::errc::device_or_resource_busy);
    }
    const auto error = RunImpl(config);
    {
        std::lock_guard<std::mutex> lock(m_impl->mutex);
        m_impl->run_finished = true;
    }
    m_impl->NotifyPhaseWaiters();
    return error;
}

std::error_code Engine::FinalizeNative()
{
    auto impl = m_impl;
    if (!impl) {
        return {};
    }
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        if (impl->phase != EnginePhase::Closing) {
            return {};
        }
    }
    std::vector<DriverSlot> to_unbind;
    std::vector<std::shared_ptr<Reactor>> reactors;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        to_unbind = std::move(impl->slots);
        impl->slots.clear();
        for (auto& slot : impl->residual_slots) {
            to_unbind.push_back(std::move(slot));
        }
        impl->residual_slots.clear();
        reactors = impl->reactors;
    }
    if (!to_unbind.empty()) {
        const auto error = DriverBinder::UnbindSlots(to_unbind);
        if (error || !to_unbind.empty()) {
            std::lock_guard<std::mutex> lock(impl->mutex);
            impl->slots = std::move(to_unbind);
            return error ? error : std::make_error_code(std::errc::io_error);
        }
    }
    for (auto& reactor : reactors) {
        reactor->FinalizeControl();
        reactor->ReleaseDriver();
    }
    impl->SettleAbandonedConnectReclaim();
    if (impl->accept_stop_requested.load(std::memory_order_acquire)) {
        impl->MarkAcceptStopDone();
    }
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        impl->reactors.clear();
        impl->main_reactor.reset();
        impl->subs.clear();
        impl->phase = EnginePhase::Stopped;
        impl->control_cv.notify_all();
    }
    impl->NotifyPhaseWaiters();
    return {};
}

std::error_code Engine::RunImpl(const EngineConfig& config)
{
    auto impl = m_impl;

    /**
     * 前置域检查在 Starting/绑定/socket 任何副作用之前。
     */
    if (bbt::coroutine::GetLocalCoroutineId() != 0
        || bbt::coroutine::detail::Processer::TryGetLocalProcesser() != nullptr) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    bool early_close = false;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        if (impl->phase != EnginePhase::Idle) {
            return impl->phase == EnginePhase::Stopped ? std::make_error_code(std::errc::operation_canceled)
                                                       : std::make_error_code(std::errc::device_or_resource_busy);
        }
        if (impl->close_requested) {

            /**
             * Run 之前已收到关闭请求：不伪造 Running，按真实 Stopped + 首因收口。
             */
            impl->RecordError(std::make_error_code(std::errc::operation_canceled));
            impl->phase = EnginePhase::Stopped;
            early_close = true;
        } else {
            impl->phase = EnginePhase::Starting;
        }
    }
    if (early_close) {
        impl->NotifyPhaseWaiters();
        return LastError();
    }

    auto fail = [&](std::error_code error) {
        {
            std::lock_guard<std::mutex> lock(impl->mutex);
            impl->RecordError(error);
            impl->phase = EnginePhase::Stopped;
            impl->control_cv.notify_all();
        }
        impl->NotifyPhaseWaiters();
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
    if (server && impl->accept_stop_requested.load(std::memory_order_acquire)) {
        return fail(std::make_error_code(std::errc::operation_canceled));
    }

    auto abort_install = [&](std::error_code error) -> std::error_code {
        std::vector<DriverSlot> to_unbind;
        {
            std::lock_guard<std::mutex> lock(impl->mutex);
            to_unbind = std::move(impl->slots);
            impl->slots.clear();
            for (auto& slot : impl->residual_slots) {
                to_unbind.push_back(std::move(slot));
            }
            impl->residual_slots.clear();
        }
        const auto unbind_error = DriverBinder::UnbindSlots(to_unbind);
        if (unbind_error || !to_unbind.empty()) {
            std::lock_guard<std::mutex> lock(impl->mutex);
            impl->slots = std::move(to_unbind);
            impl->RecordError(unbind_error ? unbind_error : error);
            impl->phase = EnginePhase::Closing;
            return unbind_error ? unbind_error : error;
        }
        for (auto& reactor : reactors) {
            reactor->FinalizeControl();
            reactor->ReleaseDriver();
        }
        impl->SettleAbandonedConnectReclaim();
        if (impl->accept_stop_requested.load(std::memory_order_acquire)) {
            impl->MarkAcceptStopDone();
        }
        {
            std::lock_guard<std::mutex> lock(impl->mutex);
            impl->reactors.clear();
            impl->main_reactor.reset();
            impl->subs.clear();
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
    if (server && impl->handler == nullptr && !impl->handoff_enabled) {

        /**
         * 保持 handler+handoff 共存，但拒绝"两者都无"的无人接收布局：
         * 仍在 driver 绑定/socket 副作用之前。
         */
        return fail(std::make_error_code(std::errc::operation_not_permitted));
    }
    impl->options = normalized;

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
    if (const auto error = DriverBinder::BindSlots(slot_count, slots, &impl->residual_slots)) {
        if (!impl->residual_slots.empty()) {

            /**
             * 回滚失败：责任仍可达，保持 Closing，不伪报 Stopped。
             */
            std::lock_guard<std::mutex> lock(impl->mutex);
            impl->RecordError(error);
            impl->phase = EnginePhase::Closing;
            return error;
        }
        return fail(error);
    }
    std::shared_ptr<Reactor> main_reactor;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        impl->slots = std::move(slots);
        impl->balancer = std::move(balancer);
        for (std::size_t i = 0; i < impl->slots.size(); ++i) {
            impl->reactors.push_back(std::make_shared<Reactor>(impl.get(), i, impl->slots[i].driver));
        }
        if (server) {
            impl->main_reactor = impl->reactors.front();
            impl->subs.assign(impl->reactors.begin() + 1, impl->reactors.end());
        } else {
            impl->main_reactor = nullptr;
            impl->subs = impl->reactors;
        }
        reactors = impl->reactors;
        main_reactor = impl->main_reactor;
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
        if (impl->TakeFault(FaultPoint::WaitDriverFailed)) {
            impl->RecordError(std::make_error_code(std::errc::io_error));
            return true;
        }
        std::error_code control_error;
        std::exception_ptr control_failure;
        {
            std::lock_guard<std::mutex> lock(impl->mutex);
            for (auto& slot : impl->slots) {
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
                impl->RecordControlFailure(control_failure);
            }
            impl->RecordError(control_error);
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
            std::lock_guard<std::mutex> lock(impl->mutex);
            impl->RecordError(install_error);
            impl->phase = EnginePhase::Closing;
            return install_error;
        }

        /**
         * 同步安装失败：走统一安装失败收口，不伪报 Stopped。
         */
        return abort_install(install_error);
    }

    if (server && impl->accept_stop_requested.load(std::memory_order_acquire)) {

        /**
         * 停止与安装竞争：不得在接受已停止的布局上发布 Running；
         * listener 已由 owner 关闭或从未创建，走安装失败收口。
         */
        return abort_install(std::make_error_code(std::errc::operation_canceled));
    }

    if (!server && impl->accept_stop_requested.load(std::memory_order_acquire)) {
        impl->MarkAcceptStopDone();
    }
    {
        std::vector<Address> local;
        if (server && main_reactor) {
            local = main_reactor->LocalAddresses();
        }
        std::lock_guard<std::mutex> lock(impl->mutex);
        impl->local_addresses = std::move(local);
        impl->phase = EnginePhase::Running;
    }

    /**
     * Running 发布后锁外通知：等待者以 Phase 为持久谓词，不漏本次发布。
     */
    impl->NotifyPhaseWaiters();

    bool failed = false;
    for (;;) {
        bool close = false;
        {
            std::unique_lock<std::mutex> lock(impl->mutex);
            impl->control_cv.wait_for(lock, std::chrono::milliseconds(50), [&] { return impl->close_requested; });
            close = impl->close_requested;
        }
        if (close) {
            break;
        }
        if (main_reactor && impl->accept_stop_requested.load(std::memory_order_acquire)) {
            impl->TryPostAcceptStop(main_reactor);
        }
        if (impl->cancel_retry_needed.exchange(false)) {

            /**
             * 一次性取消投递失败后的持久重试：控制循环补交，不等网络无限终态。
             */
            for (auto& reactor : reactors) {
                if (!reactor->TryPostInternal([reactor] { reactor->CancelAllRequestedOnOwner(); })) {
                    impl->cancel_retry_needed.store(true);
                }
            }
        }
        std::vector<DriverSlot> current;
        {
            std::lock_guard<std::mutex> lock(impl->mutex);
            current = impl->slots;
        }
        bool control_error = false;
        for (const auto& slot : current) {
            bbt::coroutine::detail::ProcesserDriverSnapshot snapshot;
            const auto status = scheduler->QueryProcesserDriver(slot.processer_id, snapshot);
            if (status != bbt::coroutine::detail::ProcesserDriverStatus::Ok) {
                impl->RecordError(std::make_error_code(std::errc::io_error));
                control_error = true;
                break;
            }
            if (snapshot.m_token != slot.token) {

                /**
                 * 代次不符：诚实报控制错误，绝不用新 token 解绑他人。
                 */
                impl->RecordError(std::make_error_code(std::errc::operation_canceled));
                control_error = true;
                break;
            }
            if (snapshot.m_state == bbt::coroutine::detail::ProcesserDriverState::Failed) {
                failed = true;
                impl->RecordControlFailure(snapshot.m_exception);
                impl->RecordError(std::make_error_code(std::errc::io_error));
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
        std::lock_guard<std::mutex> lock(impl->mutex);
        impl->phase = EnginePhase::Closing;
    }
    impl->SealService();

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
                if (impl->TakeFault(FaultPoint::SealPost)) {
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
                            impl->seal_error.store(true);
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
        if (impl->seal_error.load()) {
            impl->RecordError(std::make_error_code(std::errc::not_enough_memory));
            if (!final_error) {
                final_error = std::make_error_code(std::errc::not_enough_memory);
            }
        }
        if (!owner_seal_complete) {
            std::lock_guard<std::mutex> lock(impl->mutex);
            impl->RecordError(final_error);
        }
    }

    /**
     * 同代同步解绑；失败/残留则保留责任对象、保持 Closing、可观测错误，不发布 Stopped。
     */
    std::vector<DriverSlot> to_unbind;
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        to_unbind = std::move(impl->slots);
        impl->slots.clear();
        for (auto& slot : impl->residual_slots) {
            to_unbind.push_back(std::move(slot));
        }
        impl->residual_slots.clear();
    }
    const auto unbind_error = DriverBinder::UnbindSlots(to_unbind);
    if (unbind_error || !to_unbind.empty()) {
        std::lock_guard<std::mutex> lock(impl->mutex);
        impl->slots = std::move(to_unbind);
        impl->RecordError(unbind_error ? unbind_error : std::make_error_code(std::errc::io_error));
        impl->phase = EnginePhase::Closing;
        return LastError();
    }

    /**
     * 解绑成功：控制域独占清理旧 context（不再触发用户回调），再释放 driver 环。
     */
    for (auto& reactor : reactors) {
        reactor->FinalizeControl();
        reactor->ReleaseDriver();
    }
    impl->SettleAbandonedConnectReclaim();
    if (impl->accept_stop_requested.load(std::memory_order_acquire)) {
        impl->MarkAcceptStopDone();
    }
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        impl->reactors.clear();
        impl->main_reactor.reset();
        impl->subs.clear();
        impl->phase = EnginePhase::Stopped;
        impl->control_cv.notify_all();
    }
    impl->NotifyPhaseWaiters();
    return final_error ? final_error : LastError();
}

std::error_code Engine::LocalAddresses(std::vector<Address>& out) const
{
    std::lock_guard<std::mutex> lock(m_impl->mutex);
    if (m_impl->phase == EnginePhase::Idle || m_impl->phase == EnginePhase::Stopped) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    out = m_impl->local_addresses;
    return {};
}

std::size_t Engine::HandlerErrorCount() const
{
    return m_impl->HandlerErrorCount();
}

std::size_t Engine::CountConnections() const
{
    return m_impl->conn_count.load();
}

std::error_code
Engine::ReactorOwnerIds(std::vector<std::uint64_t>& main_owners, std::vector<std::uint64_t>& sub_owners) const
{
    std::lock_guard<std::mutex> lock(m_impl->mutex);
    main_owners.clear();
    sub_owners.clear();
    if (m_impl->phase == EnginePhase::Idle || m_impl->phase == EnginePhase::Stopped) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    std::size_t index = 0;
    if (m_impl->main_reactor != nullptr && !m_impl->slots.empty()) {
        main_owners.push_back(m_impl->slots[0].processer_id);
        index = 1;
    }
    for (; index < m_impl->slots.size(); ++index) {
        sub_owners.push_back(m_impl->slots[index].processer_id);
    }
    return {};
}

std::error_code Engine::AsyncConnect(const Address& address, std::uint64_t& out_connect_id)
{

    /**
     * OnConnect 是唯一交付：成功或失败恰一次，operation 等于本次
     * out_connect_id；拒绝路径不改 out_connect_id，也不产生本次回调。
     */
    return StartConnectImpl(m_impl, address, out_connect_id, false);
}

std::error_code Engine::CoSubmitConnect(const Address& address, std::uint64_t& out_connect_id)
{

    /**
     * CoDial 的提交步骤：登记接收意图，完成早到也保留结果供领取。
     */
    return StartConnectImpl(m_impl, address, out_connect_id, true);
}

std::error_code Engine::CoSubmitConnect(
    const Address& address, std::uint64_t& out_connect_id, const std::shared_ptr<ReclaimRecord>& reclaim)
{

    /**
     * 带责任记录的提交：记录随 post 捕获流经 owner 各真实 close/转交点。
     */
    return StartConnectImpl(m_impl, address, out_connect_id, true, reclaim);
}

/**
 * ---- Co 成员入口（docs/decisions/0012-icoconn-member-interface.md）：薄转发到既有私有步骤，不新增第二路径。----
 * 判定优先级依次为门禁、终态、配置。
 * 成员形态返回 ConnectionResult{error, conn, reclaim}：失败不覆盖 conn，
 * reclaim 按提交阶段描述本次 socket 责任（提交前拒绝=无效，提交接纳后=
 * 有效且 Released() 与连接同终态）。
 */
ConnectionResult Engine::CoAccept(const bbt::coroutine::WaitOptions& options)
{
    ConnectionResult result;

    /**
     * 门禁先于一切状态判定：非法域即使
     * handoff 已有连接也返回 operation_not_permitted。
     */
    if (const auto gate = RequireCoroutineContext()) {
        result.error = gate;
        return result;
    }
    Connection conn;
    result.error = CoWaitAccepted(conn, options);
    if (!result.error) {

        /**
         * reclaim 与交付连接同一真源：accept 连接在 AdoptFdOnOwner 建立的
         * owner record 就是 Connection::Released() 的事实——reclaim 取同一
         * 记录才满足「receipt 与连接 IsReleased 同一事实源」。
         */
        if (const auto record = conn.ReclaimForOwner()) {
            result.reclaim = CoAccess::MakeReceipt(record);
        }
        result.connection = CoTcpConnection(std::move(conn));
    }
    return result;
}

ConnectionResult Engine::CoDial(const Address& address, const bbt::coroutine::WaitOptions& options)
{
    ConnectionResult result;

    /**
     * gate 先于提交：非法域不创建责任记录、
     * 不产生任何提交副作用，reclaim 保持无效（Released()=true，无责任）。
     */
    if (const auto gate = RequireCoroutineContext()) {
        result.error = gate;
        return result;
    }

    /**
     * 提交前预分配共享释放记录：一旦接纳，无论超时/取消/未交付/Close 竞态，
     * 记录都随提交流经 owner 各真实 close 点，描述本次物理 socket 责任。
     */
    auto record = std::make_shared<ReclaimRecord>(false);
    std::uint64_t id = 0;
    if (const auto error = CoSubmitConnect(address, id, record)) {
        record->ConfirmReleased();
        result.error = error;

        /**
         * 提交前拒绝：无已提交责任
         */
        result.reclaim = ReclaimReceipt{};
        return result;
    }
    m_impl->InvokeHook(HookPoint::AfterDialSubmitBeforeAttach);
    Connection conn;
    result.error = CoWaitConnect(id, conn, options);
    result.reclaim = CoAccess::MakeReceipt(std::move(record));
    if (!result.error) {
        result.connection = CoTcpConnection(std::move(conn));
    }
    return result;
}

result<void> Engine::CoWaitRunning(const bbt::coroutine::WaitOptions& options)
{
    using R = result<void>;
    if (const auto gate = RequireCoroutineContext()) {
        return R::err(MakeError(ErrorCode::InvalidContext, "engine: CoWaitRunning requires coroutine context"));
    }
    if (options.deadline == bbt::coroutine::Deadline::max()) {
        return R::err(MakeError(ErrorCode::InvalidArgument, "engine: CoWaitRunning requires a finite deadline"));
    }
    auto impl = m_impl;
    for (;;) {
        EnginePhase phase;
        bool run_finished = false;
        {
            std::lock_guard<std::mutex> lock(impl->mutex);
            phase = impl->phase;
            run_finished = impl->run_finished;
        }
        if (phase == EnginePhase::Running) {
            return R::ok();
        }
        if (phase == EnginePhase::Stopped) {

            /**
             * 正常收口（无错误）→ Closed；启动失败/提前关闭按 LastError 映射。
             */
            return R::err(WaitRunningError(impl->LastError()));
        }
        if (phase == EnginePhase::Closing && run_finished) {
            const auto error = impl->LastError();
            return R::err(WaitRunningError(error ? error : std::make_error_code(std::errc::io_error)));
        }
        if (std::chrono::steady_clock::now() >= options.deadline) {
            return R::err(MakeError(ErrorCode::TimedOut, "engine: running wait deadline expired"));
        }

        /**
         * 谓词已在短锁内复查；等待在锁外分片挂起，不阻塞 worker。
         */
        auto waited = bbt::infra::detail::WaitCloseSlice(
            impl->phase_signal, bbt::infra::detail::NextCloseSlice(options.deadline));
        if (!waited) {
            return R::err(waited.error());
        }
    }
}

/**
 * CoDial 的领取步骤（内部）：成功才写 out；任何未交付失败都归还 claim 并让
 * owner 回收 fd。有 handler 的 Engine 在写任何等待/取消状态之前拒绝。
 */
std::error_code
Engine::CoWaitConnect(std::uint64_t connect_id, Connection& out, const bbt::coroutine::WaitOptions& options)
{
    auto impl = m_impl;
    if (impl->handler) {

        /**
         * raw-only：不得在任何等待/取消状态写入之前改变 raw 建连责任。
         */
        return std::make_error_code(std::errc::operation_not_supported);
    }
    auto op = impl->FindConnect(connect_id);
    if (!op) {
        return std::make_error_code(std::errc::invalid_argument);
    }
    {
        std::lock_guard<std::mutex> lock(op->mutex);
        if (op->receiver_claimed) {
            return std::make_error_code(std::errc::device_or_resource_busy);
        }

        /**
         * 单接收者；超时/取消后不自动释放
         */
        op->receiver_claimed = true;
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
            std::lock_guard<std::mutex> lock(op->mutex);
            op->receiver_claimed = false;
            op->waiter_attached = false;
            if (op->done) {

                /**
                 * 已完成但本次领取已终结：连接此刻无主，直接回收。
                 */
                if (!op->raw_delivered) {
                    orphan = op->connection;
                }
                op->abandoned = false;
            } else {

                /**
                 * 未完成：放弃接收意图，owner 稍后交付或取消时回收 fd。
                 */
                op->abandoned = true;
            }
            reactor = op->reactor;
            if (cancel_owner) {
                op->cancel_requested.store(true);
            }
        }
        if (orphan.Valid()) {
            (void)orphan.Close({}, std::make_error_code(std::errc::operation_canceled));
            impl->MaybeEraseConnect(connect_id, op);
        }
        if (!cancel_owner) {
            return;
        }
        if (reactor) {
            if (impl->TakeFault(FaultPoint::CancelPost)
                || !reactor->TryPost([reactor, connect_id] { reactor->CancelConnectOnOwner(connect_id); })) {
                impl->cancel_retry_needed.store(true);
            }
        } else {
            impl->cancel_retry_needed.store(true);
        }
    };

    /**
     * 终态 op 的收尾：只有无错误且确有连接的结果才算本次交付；否则不覆盖 out。
     */
    auto settle_done = [&](Connection delivered, std::error_code done_error) {
        impl->MaybeEraseConnect(connect_id, op);
        if (done_error) {
            if (!impl->handler && delivered.Valid()) {

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
            std::lock_guard<std::mutex> lock(op->mutex);
            if (op->done) {
                if (op->raw_delivered) {

                    /**
                     * 已由 raw handler 唯一交付，不重复领取
                     */
                    raw_conflict = true;
                } else {
                    delivered = op->connection;
                    done_error = op->error;
                }
                op->waiter_attached = false;
                op->abandoned = false;
                already_done = true;
            } else {
                op->waiter_attached = true;
                op->abandoned = false;
            }
        }
        if (already_done) {
            if (raw_conflict) {
                impl->MaybeEraseConnect(connect_id, op);
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
        const int injected_wait_status = (impl->test_hooks ? impl->test_hooks->TakeConnectWaitStatus() : -1);
        const bool inject_wait = injected_wait_status >= 0
                                 && injected_wait_status != static_cast<int>(bbt::coroutine::WaitStatus::Completed);
        if (inject_wait) {
            status = static_cast<bbt::coroutine::WaitStatus>(injected_wait_status);
        } else {
            try {
                auto waiter = op->hub.Create();
                status = waiter->WaitWithCallback(options, [&] {
                    if (!op->hub.Add(waiter)) {

                        /**
                         * 满额：有界拒绝，不自激重试
                         */
                        add_failed = true;
                        waiter->Notify();
                        return true;
                    }
                    bool done = false;
                    {
                        std::lock_guard<std::mutex> lock(op->mutex);
                        done = op->done;
                    }
                    if (done) {

                        /**
                         * 锁外 Notify
                         */
                        waiter->Notify();
                    }
                    return true;
                });
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
                std::lock_guard<std::mutex> lock(op->mutex);
                done = op->done && !op->raw_delivered;
                if (done) {
                    ready = op->connection;
                    ready_error = op->error;
                }
                op->waiter_attached = false;
            }
            if (done) {
                std::lock_guard<std::mutex> lock(op->mutex);
                op->receiver_claimed = false;
                op->abandoned = false;
            }
            if (done && !ready_error && ready.Valid()) {
                impl->MaybeEraseConnect(connect_id, op);
                out = std::move(ready);
                return {};
            }
            if (done) {
                impl->MaybeEraseConnect(connect_id, op);
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
            std::lock_guard<std::mutex> lock(op->mutex);
            done = op->done;
            if (done && !op->raw_delivered) {
                finished = op->connection;
                finished_error = op->error;
            }
            op->waiter_attached = false;
        }
        const auto mapped = WaitStatusError(status);
        if (done) {
            if (finished.Valid()) {

                /**
                 * 等待已按本次仲裁终结：结果无主 → 回收（raw 已交付时不动）。
                 */
                (void)finished.Close({}, std::make_error_code(std::errc::operation_canceled));
            }
            impl->MaybeEraseConnect(connect_id, op);
            return mapped;
        }

        /**
         * 尚未完成：交还责任；超时/取消/上下文失效都要求中断真实尝试。
         */
        abandon(true);
        return mapped;
    }
}

std::error_code Engine::CoWaitAccepted(Connection& out, const bbt::coroutine::WaitOptions& options)
{
    auto impl = m_impl;

    /**
     * 配置与生命周期错误立即返回，不等一个永远不会来的 listener。
     * 判定优先级是契约（见 Engine.hpp 的 Co 成员入口）：终态先于配置/布局判定。Stopped 时
     * main accept owner 已解绑，但那是收口带来的结果，不能据此把 Engine 判成
     * client-only（否则把"已结束"误报成"这类 Engine 不支持 accept"）。
     */
    {
        std::lock_guard<std::mutex> lock(impl->mutex);
        const bool service_sealed = impl->IsSealed();
        const auto phase = impl->phase;
        if (service_sealed || phase == EnginePhase::Closing || phase == EnginePhase::Stopped) {

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
        if (impl->accept_stop_requested.load(std::memory_order_acquire) && phase == EnginePhase::Running) {
            return std::make_error_code(std::errc::operation_canceled);
        }
        if (phase == EnginePhase::Idle || phase == EnginePhase::Starting) {

            /**
             * 尚未发布 Running：还没有 accept 生产者，也不是配置错误。
             */
            return std::make_error_code(std::errc::operation_not_permitted);
        }
        if (!impl->handoff_enabled) {
            return std::make_error_code(std::errc::operation_not_permitted);
        }
        if (!impl->main_reactor) {

            /**
             * Running + handoff 但没有 main accept owner：真正的 client-only 布局，
             * handoff 永远不会有生产者。
             */
            return std::make_error_code(std::errc::operation_not_supported);
        }
    }
    for (;;) {
        Connection popped;
        bool sealed = false;
        {
            std::lock_guard<std::mutex> lock(impl->handoff_mutex);
            if (!impl->handoff_closed && !impl->handoff.empty()) {
                popped = impl->handoff.front();
                impl->handoff.pop_front();
            } else {
                sealed = impl->IsSealed() || impl->accept_stop_requested.load(std::memory_order_acquire);
            }
        }

        /**
         * 以本次实际弹出的连接判断成功；调用者原有的 out 有效不算。
         */
        if (popped.Valid()) {
            out = popped;
            impl->ResumeAcceptIfFree();
            return {};
        }
        if (sealed) {
            return std::make_error_code(std::errc::operation_canceled);
        }
        auto waiter = impl->accept_hub.Create();
        if (!waiter) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        bool add_failed = false;
        const auto status = waiter->WaitWithCallback(options, [&] {
            if (!impl->accept_hub.Add(waiter)) {
                add_failed = true;
                waiter->Notify();
                return true;
            }
            impl->InvokeHook(HookPoint::AfterAcceptRegistered);
            bool ready = false;
            {
                std::lock_guard<std::mutex> lock(impl->handoff_mutex);
                ready = !impl->handoff.empty() || impl->IsSealed()
                        || impl->accept_stop_requested.load(std::memory_order_acquire);
            }
            if (ready) {

                /**
                 * 锁外 Notify
                 */
                waiter->Notify();
            }
            return true;
        });
        if (add_failed) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        if (status != bbt::coroutine::WaitStatus::Completed) {
            return WaitStatusError(status);
        }
    }
}

// clang-format off
}
// clang-format on
