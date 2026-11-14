#include <bbt/infra/config/Watch.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "bbt/infra/config/detail/CloseSupport.hpp"

/**
 * 按实际使用的符号引入 coroutine 头：执行域判定（本地协程 id / Processer）、
 * 协程注册与退出（g_scheduler / bbtco_noexcept）、睡眠与唤醒（CoWaiter / Hook_Sleep）、
 * 对象信息（CreateObjectInfo）。
 */
#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/Hook.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/detail/CloseWait.hpp>

namespace bbt::infra::config {

namespace {

/**
 * 轮询协程退出记号：寿命独立于 Impl——协程帧与 Impl 各持一份 shared_ptr，
 * 协程退出时无条件 Signal（即使 weak 已失效、Impl 已销毁），不靠
 * weak_ptr 消失或提前 MarkClosed 伪造已退出。
 */
struct LoopExitSignal {

    std::mutex mtx;
    std::condition_variable cv;

    /**
     * 由 mtx 保护。
     */
    bool stopped{false};

    void Signal() noexcept
    {
        {
            std::lock_guard<std::mutex> lk(mtx);
            stopped = true;
        }
        cv.notify_all();
    }
};

/**
 * 与 bbt/infra/http/Server.cc 同一口径：受管协程与已登记 Processer 域都不是
 * 合法宿主——在这些域内 WaitStopped 会挂起调度线程（回调内调用即等待自身）。
 */
bool IsNativeHost()
{
    return bbt::coroutine::GetLocalCoroutineId() == 0
           && bbt::coroutine::detail::Processer::TryGetLocalProcesser() == nullptr;
}

// clang-format off
}
// clang-format on

struct Watcher::Impl {
    SourcePtr source;
    WatchCallback callback;
    std::chrono::milliseconds poll_interval{100};
    bool deliver_initial{true};

    bbt::coroutine::CoObjectInfo info;

    /**
     * 逻辑封口态：Open→Closing→Closed，不重开。IsOpen() 为真才读源/投递——
     * _BeginClose() 封口后不再产生消费者回调。Closed 仅由轮询协程退出时
     * 落定；RequestClose 自身（含回调内自关闭）不提前 MarkClosed。
     */
    config_detail::ManagedCloseState close;

    /**
     * 回调起点与封口的线性化门：Deliver 的「open 检查 + 置在途标记」与
     * RequestClose 的「发布首因 + BeginClose」在同一把门下，使 RequestClose 返回时
     * 不可能再有新回调起点。门不跨回调持有——回调执行期间 RequestClose 仍能立即返回，
     * 不会被用户回调阻塞（回调不得阻塞仍是契约前置）。
     */
    std::mutex gate;

    /**
     * 在途回调（本 watcher 同一时刻最多一个，轮询协程内联执行）。Close(Graceful)
     * 等它归零：Co 域用分片等待位（inflight_waiters），native 宿主用 callback_done。
     */
    bool callback_inflight{false};

    /**
     * 轮询协程 id，自等待判定用；协程启动时落定。
     */
    std::uint64_t poll_co_id{0};

    /**
     * gate 保护：首个关闭原因。
     */
    std::optional<Error> reason;

    /**
     * gate 保护：Co 域的分片等待位。
     */
    std::vector<bbt::coroutine::sync::CoWaiter::SPtr> inflight_waiters;

    /**
     * gate 保护：native 宿主的在途回调归零通知。
     */
    std::condition_variable callback_done;

    /**
     * 轮询协程的睡眠等待位（单等待者）+ 关闭期等待者登记。_BeginClose() 经
     * close_waiters 唤醒正在睡眠的轮询协程，不必等满 poll_interval。
     * 等待位与等待集合在 park 期间的保活见 RunWatchLoop：由协程帧上的
     * lambda 捕获持强引用，恢复时协程会回到 CoWaiter::WaitWithCallback
     * 内部继续执行，等待位在 park 期间不得析构。
     */
    bbt::coroutine::sync::CoWaiter::SPtr sleeper;
    std::shared_ptr<config_detail::CloseWaiters> close_waiters;

    /**
     * 轮询协程退出记号（与协程帧共享，独立寿命）。
     */
    std::shared_ptr<LoopExitSignal> exit_signal;

    void PollTick()
    {
        auto read = source->Read();
        if (!read) {
            WatchEvent event;
            event.kind = WatchEventKind::Failed;
            event.error = std::move(read).error();
            if (!failed) {

                /**
                 * 失败去重：持续失败期间不重复通知。
                 */
                failed = true;
                Deliver(event);
            }
            return;
        }

        Snapshot snapshot = std::move(read).value();
        WatchEvent event;
        event.snapshot = snapshot;

        if (failed) {
            event.kind = WatchEventKind::Recovered;
            failed = false;
            has_last = true;
            last_version = snapshot.version;
            last_revision = snapshot.revision;
            Deliver(event);
            return;
        }
        if (!has_last) {
            has_last = true;
            last_version = snapshot.version;
            last_revision = snapshot.revision;
            if (deliver_initial) {
                event.kind = WatchEventKind::Initial;
                Deliver(event);
            }
            return;
        }

        /**
         * version 与 revision 都未变即重复版本：去重，不通知。
         */
        if (snapshot.version != last_version || snapshot.revision != last_revision) {
            last_version = snapshot.version;
            last_revision = snapshot.revision;
            event.kind = WatchEventKind::Updated;
            Deliver(event);
        }
    }

    /**
     * 在途回调是否已归零（gate 保护）。供 Close(Graceful) 复查谓词。
     */
    bool CallbackIdle()
    {
        std::lock_guard<std::mutex> lk(gate);
        return !callback_inflight;
    }

private:
    /**
     * 投递与封口的线性化：open 检查 + 置在途标记在 gate 内完成，与
     * RequestClose 的 BeginClose 互斥——RequestClose 返回后不可能再有
     * 新回调起点。回调本身在 gate 外执行：封口不等用户代码，等待归 Close(Graceful)。
     */
    void Deliver(const WatchEvent& event)
    {
        {
            std::lock_guard<std::mutex> lk(gate);
            if (!close.IsOpen())
                return;
            callback_inflight = true;
        }
        callback(event);
        std::vector<bbt::coroutine::sync::CoWaiter::SPtr> woken;
        {
            std::lock_guard<std::mutex> lk(gate);
            callback_inflight = false;
            woken.swap(inflight_waiters);
        }
        callback_done.notify_all();
        for (auto& waiter : woken)
            waiter->Notify();
    }

    /**
     * 轮询循环专属状态（仅协程 worker 访问，单线程）：
     */
    bool has_last{false};
    std::uint64_t last_version{0};
    std::string last_revision;
    bool failed{false};
};

namespace {

/**
 * 轮询循环。
 * sleeper/exit_signal 由协程帧强持有（bbtco_noexcept 的 lambda 捕获）：挂起
 * 期间协程栈上必须保有等待位与等待集合，恢复路径会回到
 * CoWaiter::WaitWithCallback 内部。捕获不涉及 Impl，因此挂起期间不钉住
 * source/callback；exit_signal 由帧持有，句柄全部释放后退出记号仍落定。
 */
void RunWatchLoop(
    std::weak_ptr<Watcher::Impl> weak,
    const bbt::coroutine::sync::CoWaiter::SPtr& sleeper,
    const std::shared_ptr<LoopExitSignal>& exit_signal)
{

    /**
     * 自等待判定依据：Close(Graceful) 在本协程内（即回调内）调用时等的是自己。
     */
    if (auto self = weak.lock())
        self->poll_co_id = bbt::coroutine::GetLocalCoroutineId();

    for (;;) {
        std::chrono::milliseconds interval{0};
        std::shared_ptr<config_detail::CloseWaiters> waiters;
        {
            auto self = weak.lock();

            /**
             * Impl 已释放：直接退出。
             */
            if (!self)
                break;

            /**
             * 已封口：不再读源/投递。
             */
            if (!self->close.IsOpen())
                break;
            self->PollTick();
            interval = self->poll_interval;
            waiters = self->close_waiters;

            /**
             * 强引用在此释放：挂起期间不持有 Impl。
             */
        }

        /**
         * 有界睡眠：poll_interval 到点、或 RequestClose 封口时唤醒。
         * on_registered 在等待事件登记成功后、真正 park 前执行——在此登记进
         * 关闭等待集合；Close 已开始时（Add 返回 false）自行 Notify 一次，
         * 走 PENDING 早到路径，不丢唤醒。
         */
        const auto status = sleeper->WaitWithCallback(
            bbt::coroutine::WaitOptions{std::chrono::steady_clock::now() + interval}, [waiters, sleeper]() {
                if (!waiters->Add(sleeper))
                    sleeper->Notify();

                /**
                 * 登记成功后照常 park；唤醒由 Add/Notify 两步决定。
                 */
                return true;
            });
        waiters->Remove(sleeper.get());

        if (status == bbt::coroutine::WaitStatus::Cancelled) {

            /**
             * 轮询协程被运行时取消（本模块从不发起）：停止轮询，不空转重试。
             */
            break;
        }
        if (status == bbt::coroutine::WaitStatus::InvalidContext || status == bbt::coroutine::WaitStatus::AlreadyWaiting
            || status == bbt::coroutine::WaitStatus::RuntimeUnavailable) {

            /**
             * 未真正挂起（事件登记失败/等待位残留/上下文异常）：退化为一次
             * Hook_Sleep，避免立即重试空转；下一拍照常重查封口态。
             */
            bbtco_sleep(static_cast<int>(interval.count()));
        }
    }

    /**
     * 退出落定：物理退出只在此处发生——置 Closed（幂等）并无条件落定退出
     * 记号（帧持强引用，与 Impl 存活无关）。RequestClose 不代劳任何一步。
     */
    if (auto self = weak.lock())
        self->close.MarkClosed();
    exit_signal->Signal();
}

// clang-format off
}
// clang-format on

Watcher::Watcher(std::shared_ptr<Impl> impl)
    : m_impl(std::move(impl))
{
}

Watcher::~Watcher()
{

    /**
     * 句柄析构只请求封口、不等待：需要物理排空的宿主先 WaitStopped 再释放
     * 句柄。Impl 随本句柄销毁后，轮询协程下一拍见 weak 失效即退出，退出记号
     * 由协程帧上的 LoopExitSignal 落定，与本对象寿命无关。
     */
    if (m_impl)
        _BeginClose();
}

bbt::coroutine::CoObjectInfo Watcher::GetObjectInfo() const
{
    return m_impl->info;
}

result<Watcher::SPtr> Watcher::Create(SourcePtr source, WatchCallback callback, WatchOptions options)
{
    if (!source)
        return result<SPtr>::err(MakeError(ErrorCode::InvalidArgument, "config watcher: null source"));
    if (!callback)
        return result<SPtr>::err(MakeError(ErrorCode::InvalidArgument, "config watcher: null callback"));
    if (options.poll_interval.count() <= 0)
        return result<SPtr>::err(
            MakeError(ErrorCode::InvalidArgument, "config watcher: poll_interval must be positive"));

    /**
     * 睡眠走协程定时器（int 毫秒口径）：超过上限不挂定时器、会永久挂起，
     * 因此按非法参数拒绝，不静默退化。
     */
    if (options.poll_interval > std::chrono::milliseconds{std::numeric_limits<int>::max()})
        return result<SPtr>::err(
            MakeError(ErrorCode::InvalidArgument, "config watcher: poll_interval exceeds coroutine timer range"));
    if (!g_scheduler->IsInitialized())
        return result<SPtr>::err(
            MakeError(ErrorCode::RuntimeUnavailable, "config watcher: coroutine runtime not initialized"));

    auto impl = std::make_shared<Impl>();
    try {
        impl->info = bbt::coroutine::CreateObjectInfo("config", "Watcher");
    } catch (const std::logic_error&) {

        /**
         * 运行时在检查与创建之间停止初始化：映射为 RuntimeUnavailable，
         * 不泄漏异常（错误通过 result 返回）。
         */
        return result<SPtr>::err(
            MakeError(ErrorCode::RuntimeUnavailable, "config watcher: coroutine runtime unavailable"));
    }
    impl->sleeper = bbt::coroutine::sync::CoWaiter::Create();
    impl->close_waiters = std::make_shared<config_detail::CloseWaiters>();
    impl->exit_signal = std::make_shared<LoopExitSignal>();
    impl->source = std::move(source);
    impl->callback = std::move(callback);
    impl->poll_interval = options.poll_interval;
    impl->deliver_initial = options.deliver_initial;

    const std::weak_ptr<Impl> weak = impl;
    const auto sleeper = impl->sleeper;
    const auto exit_signal = impl->exit_signal;
    bool registered = false;
    bbtco_noexcept(&registered)[weak, sleeper, exit_signal]()
    {
        RunWatchLoop(weak, sleeper, exit_signal);
    };
    if (!registered)
        return result<SPtr>::err(
            MakeError(ErrorCode::RuntimeUnavailable, "config watcher: failed to register poll coroutine"));

    return result<SPtr>::ok(SPtr(new Watcher(std::move(impl))));
}

void Watcher::_BeginClose(Error reason) noexcept
{
    auto* impl = m_impl.get();
    if (impl == nullptr)
        return;

    /**
     * 与 Deliver 的 open 检查在同一把门下：首因发布 + BeginClose 构成「回调
     * 起点线性化点」——本函数返回即不可能再有新回调起点。门不跨回调持有，故
     * 回调执行期间本调用不会被用户代码拖住（回调不得阻塞是契约前置）。
     */
    bool first = false;
    {
        std::lock_guard<std::mutex> lk(impl->gate);
        if (!impl->reason)
            impl->reason = std::move(reason);
        first = impl->close.BeginClose();
    }

    /**
     * 原子封口并唤醒在册等待者。
     */
    if (first)
        impl->close_waiters->CloseAndWakeAll();
}

std::optional<Error> Watcher::CloseReason() const noexcept
{
    auto* impl = m_impl.get();
    if (impl == nullptr)
        return std::nullopt;
    std::lock_guard<std::mutex> lk(impl->gate);
    return impl->reason;
}

result<void> Watcher::Close(const CloseOptions& options)
{
    auto valid = detail::ValidateCloseOptions(options);
    if (!valid)
        return valid;

    const bool waits = options.mode == CloseMode::Graceful || options.wait_release;
    const auto domain = detail::CurrentCloseWaitDomain();
    if (waits && domain == detail::CloseWaitDomain::Invalid)
        return result<void>::err(
            MakeError(ErrorCode::InvalidContext, "config watcher: Close requires coroutine or native host"));

    /**
     * 物理凭据只由 native-only 的 WaitStopped 提供：Co 域请求 wait_release 属于
     * 模块不支持的组合，在封口/首因之前按 InvalidArgument 拒绝。
     */
    if (options.wait_release && domain != detail::CloseWaitDomain::NativeHost)
        return result<void>::err(
            MakeError(ErrorCode::InvalidArgument, "config watcher: wait_release requires native host"));

    /**
     * 自等待：回调在轮询协程内联执行，回调内 Close(Graceful) 等的是自己；
     * 在任何副作用（封口/首因发布）之前拒绝。Immediate 不等待，回调内合法。
     */
    if (waits) {
        std::lock_guard<std::mutex> lk(m_impl->gate);
        if (m_impl->callback_inflight && bbt::coroutine::GetLocalCoroutineId() == m_impl->poll_co_id)
            return result<void>::err(
                MakeError(ErrorCode::InvalidContext, "config watcher: Close from callback waits on itself"));
    }

    _BeginClose(detail::SettleCloseError(options.reason, "config watcher: closed by owner"));
    if (options.mode == CloseMode::Immediate)
        return detail::FinishCloseWithRelease(
            options, [this](bbt::coroutine::Deadline deadline) { return _WaitReleased(deadline); });

    /**
     * Graceful：等已获准的这一次回调返回。到期只报 TimedOut 并保留封口与在途责任，
     * 不自动升级为 Immediate。
     */
    if (domain == detail::CloseWaitDomain::NativeHost) {
        std::unique_lock<std::mutex> lk(m_impl->gate);
        if (!m_impl->callback_done.wait_until(lk, options.deadline, [&] { return !m_impl->callback_inflight; }))
            return result<void>::err(MakeError(ErrorCode::TimedOut, "config watcher: graceful close deadline expired"));
        lk.unlock();
        return detail::FinishCloseWithRelease(
            options, [this](bbt::coroutine::Deadline deadline) { return _WaitReleased(deadline); });
    }

    for (;;) {
        if (m_impl->CallbackIdle())
            break;
        const auto now = std::chrono::steady_clock::now();
        if (now >= options.deadline)
            return result<void>::err(MakeError(ErrorCode::TimedOut, "config watcher: graceful close deadline expired"));

        auto waiter = bbt::coroutine::sync::CoWaiter::Create();
        if (!waiter)
            return result<void>::err(
                MakeError(ErrorCode::RuntimeUnavailable, "config watcher: close wait unavailable"));
        {
            std::lock_guard<std::mutex> lk(m_impl->gate);
            if (!m_impl->callback_inflight)
                return result<void>::ok();
            m_impl->inflight_waiters.push_back(waiter);
        }
        auto slice = detail::WaitCloseSlice(waiter, detail::NextCloseSlice(options.deadline));
        {
            std::lock_guard<std::mutex> lk(m_impl->gate);
            auto& waiters = m_impl->inflight_waiters;
            waiters.erase(std::remove(waiters.begin(), waiters.end(), waiter), waiters.end());
        }
        if (!slice)
            return result<void>::err(std::move(slice).error());
    }
    return detail::FinishCloseWithRelease(
        options, [this](bbt::coroutine::Deadline deadline) { return _WaitReleased(deadline); });
}

result<void> Watcher::_WaitReleased(bbt::coroutine::Deadline deadline)
{

    /**
     * 仅 native 宿主控制域：受管协程/Processer 域内等待会挂起调度线程；
     * 本 watcher 回调内调用是等待自身（回调在轮询协程内联执行，协程上下文
     * 检查已覆盖该情形）。
     */
    if (!IsNativeHost())
        return result<void>::err(
            MakeError(ErrorCode::InvalidContext, "config watcher: WaitStopped requires native host"));

    auto impl = m_impl;
    auto exit_signal = impl->exit_signal;

    /**
     * 前置：先 RequestClose——未封口的等待没有意义（轮询协程不会自行退出；
     * 运行时取消/Impl 失效的退出路径也由 stopped 记号兑现，同样要求显式
     * 封口，保持「关闭决定归 owner」的单一入口）。
     */
    if (impl->close.IsOpen())
        return result<void>::err(
            MakeError(ErrorCode::InvalidArgument, "config watcher: RequestClose before WaitStopped"));

    std::unique_lock<std::mutex> lk(exit_signal->mtx);
    if (!exit_signal->cv.wait_until(lk, deadline, [&] { return exit_signal->stopped; }))
        return result<void>::err(
            MakeError(ErrorCode::TimedOut, "config watcher: poll coroutine stop deadline expired"));
    return result<void>::ok();
}

bool Watcher::IsClosed() const noexcept
{
    auto* impl = m_impl.get();

    /**
     * 逻辑封口投影：Closing（已 RequestClose）与 Closed（协程已退出）都算
     * 「不再打开」，不区分物理退出与否。
     */
    return impl == nullptr || !impl->close.IsOpen();
}

// clang-format off
}
// clang-format on
