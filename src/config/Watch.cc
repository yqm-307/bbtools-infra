#include <bbt/infra/config/Watch.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include "detail/IoSupport.hpp" // ManagedCloseState / CloseWaiters

#include <bbt/coroutine/coroutine.hpp>           // GetLocalCoroutineId
#include <bbt/coroutine/detail/Define.hpp>       // g_scheduler / g_bbt_tls_coroutine_co
#include <bbt/coroutine/detail/Hook.hpp>         // Hook_Sleep
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>    // TryGetLocalProcesser
#include <bbt/coroutine/detail/Scheduler.hpp>    // IsInitialized
#include <bbt/coroutine/object/CoObject.hpp>     // CreateObjectInfo
#include <bbt/coroutine/sync/CoWaiter.hpp>       // 睡眠等待位 + 关闭唤醒
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>  // bbtco_noexcept / bbtco_sleep

namespace bbt::infra::config {

namespace {

// 轮询协程退出记号：寿命独立于 Impl——协程帧与 Impl 各持一份 shared_ptr，
// 协程退出时无条件 Signal（即使 weak 已失效、Impl 已销毁），不靠
// weak_ptr 消失或提前 MarkClosed 伪造已退出。
struct LoopExitSignal {
    std::mutex              mtx;
    std::condition_variable cv;
    bool                    stopped{false}; // mtx 保护

    void Signal() noexcept {
        {
            std::lock_guard<std::mutex> lk(mtx);
            stopped = true;
        }
        cv.notify_all();
    }
};

// 与 bbt/infra/http/Server.cc 同一口径：受管协程与已登记 Processer 域都不是
// 合法宿主——在这些域内 WaitStopped 会挂起调度线程（回调内调用即等待自身）。
bool IsNativeHost() {
    return bbt::coroutine::GetLocalCoroutineId() == 0 &&
           bbt::coroutine::detail::Processer::TryGetLocalProcesser() == nullptr;
}

} // namespace

struct Watcher::Impl {
    SourcePtr source;
    WatchCallback callback;
    std::chrono::milliseconds poll_interval{100};
    bool deliver_initial{true};

    bbt::coroutine::CoObjectInfo info;

    // 逻辑封口态：Open→Closing→Closed，不重开。IsOpen() 为真才读源/投递——
    // RequestClose() 封口后不再产生消费者回调。Closed 仅由轮询协程退出时
    // 落定；RequestClose 自身（含回调内自关闭）不提前 MarkClosed。
    detail::ManagedCloseState close;

    // 回调起点与 close 的线性化门：Deliver 的「open 检查 + 进入
    // callback」与 RequestClose 的「BeginClose + 唤醒」在同一把门下，使
    // RequestClose 返回时不可能再有新回调起点；已通过 open 检查获准的
    // 当前回调可以执行完成（回调不得阻塞是本契约前置，等待有界）。
    // recursive：回调内自关闭/句柄析构在同一线程内重入门而不自锁。
    std::recursive_mutex deliver_gate;

    // 轮询协程的睡眠等待位（单等待者）+ 关闭期等待者登记。RequestClose() 经
    // close_waiters 唤醒正在睡眠的轮询协程，不必等满 poll_interval。
    // 等待位与等待集合在 park 期间的保活见 RunWatchLoop：由协程帧上的
    // lambda 捕获持强引用，恢复时协程会回到 CoWaiter::WaitWithCallback
    // 内部继续执行，等待位在 park 期间不得析构。
    bbt::coroutine::sync::CoWaiter::SPtr sleeper;
    std::shared_ptr<detail::CloseWaiters> close_waiters;

    // 轮询协程退出记号（与协程帧共享，独立寿命）。
    std::shared_ptr<LoopExitSignal> exit_signal;

    void PollTick() {
        auto read = source->Read();
        if (!read) {
            WatchEvent event;
            event.kind = WatchEventKind::Failed;
            event.error = std::move(read).error();
            if (!failed) {
                failed = true; // 失败去重：持续失败期间不重复通知
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
        if (snapshot.version != last_version ||
            snapshot.revision != last_revision) {
            last_version = snapshot.version;
            last_revision = snapshot.revision;
            event.kind = WatchEventKind::Updated;
            Deliver(event);
        }
        // (version, revision) 均未变 → 重复版本，去重不通知。
    }

private:
    // 投递与封口的线性化：open 检查 + 进入 callback 在 deliver_gate 内完成，
    // 与 RequestClose 的 BeginClose 互斥——RequestClose 返回后不可能再有
    // 新回调起点（已获准的当前回调可以执行完成，回调不得阻塞是契约前置）。
    // deliver_gate 是 recursive：回调内自关闭在同一线程内重入安全。
    void Deliver(const WatchEvent& event) {
        std::lock_guard<std::recursive_mutex> lk(deliver_gate);
        if (!close.IsOpen()) return;
        callback(event);
    }

    // 轮询循环专属状态（仅协程 worker 访问，单线程）：
    bool has_last{false};
    std::uint64_t last_version{0};
    std::string last_revision;
    bool failed{false};
};

namespace {

// 轮询循环。
// sleeper/exit_signal 由协程帧强持有（bbtco_noexcept 的 lambda 捕获）：挂起
// 期间协程栈上必须保有等待位与等待集合，恢复路径会回到
// CoWaiter::WaitWithCallback 内部。捕获不涉及 Impl，因此挂起期间不钉住
// source/callback；exit_signal 由帧持有，句柄全部释放后退出记号仍落定。
void RunWatchLoop(std::weak_ptr<Watcher::Impl> weak,
                  const bbt::coroutine::sync::CoWaiter::SPtr& sleeper,
                  const std::shared_ptr<LoopExitSignal>& exit_signal) {
    for (;;) {
        std::chrono::milliseconds interval{0};
        std::shared_ptr<detail::CloseWaiters> waiters;
        {
            auto self = weak.lock();
            if (!self) break;                  // Impl 已释放：直接退出
            if (!self->close.IsOpen()) break;  // 已封口：不再读源/投递
            self->PollTick();
            interval = self->poll_interval;
            waiters = self->close_waiters;
        } // 强引用在此释放：挂起期间不持有 Impl

        // 有界睡眠：poll_interval 到点、或 RequestClose 封口时唤醒。
        // on_registered 在等待事件登记成功后、真正 park 前执行——在此登记进
        // 关闭等待集合；Close 已开始时（Add 返回 false）自行 Notify 一次，
        // 走 PENDING 早到路径，不丢唤醒。
        const auto status = sleeper->WaitWithCallback(
            bbt::coroutine::WaitOptions{std::chrono::steady_clock::now() + interval},
            [waiters, sleeper]() {
                if (!waiters->Add(sleeper)) sleeper->Notify();
                return true; // 登记成功后照常 park；唤醒由 Add/Notify 两步决定
            });
        waiters->Remove(sleeper.get());

        if (status == bbt::coroutine::WaitStatus::Cancelled) {
            // 轮询协程被运行时取消（本模块从不发起）：停止轮询，不空转重试。
            break;
        }
        if (status == bbt::coroutine::WaitStatus::InvalidContext ||
            status == bbt::coroutine::WaitStatus::AlreadyWaiting ||
            status == bbt::coroutine::WaitStatus::RuntimeUnavailable) {
            // 未真正挂起（事件登记失败/等待位残留/上下文异常）：退化为一次
            // Hook_Sleep，避免立即重试空转；下一拍照常重查封口态。
            bbtco_sleep(static_cast<int>(interval.count()));
        }
    }

    // 退出落定：物理退出只在此处发生——置 Closed（幂等）并无条件落定退出
    // 记号（帧持强引用，与 Impl 存活无关）。RequestClose 不代劳任何一步。
    if (auto self = weak.lock())
        self->close.MarkClosed();
    exit_signal->Signal();
}

} // namespace

Watcher::Watcher(std::shared_ptr<Impl> impl) : m_impl(std::move(impl)) {}

Watcher::~Watcher() {
    // 句柄析构只请求封口、不等待：需要物理排空的宿主先 WaitStopped 再释放
    // 句柄。Impl 随本句柄销毁后，轮询协程下一拍见 weak 失效即退出，退出记号
    // 由协程帧上的 LoopExitSignal 落定，与本对象寿命无关。
    if (m_impl) RequestClose();
}

bbt::coroutine::CoObjectInfo Watcher::GetObjectInfo() const {
    return m_impl->info;
}

result<Watcher::SPtr> Watcher::Create(SourcePtr source, WatchCallback callback,
                                      WatchOptions options) {
    if (!source)
        return result<SPtr>::err(MakeError(ErrorCode::InvalidArgument,
            "config watcher: null source"));
    if (!callback)
        return result<SPtr>::err(MakeError(ErrorCode::InvalidArgument,
            "config watcher: null callback"));
    if (options.poll_interval.count() <= 0)
        return result<SPtr>::err(MakeError(ErrorCode::InvalidArgument,
            "config watcher: poll_interval must be positive"));
    // 睡眠走协程定时器（int 毫秒口径）：超过上限不挂定时器、会永久挂起，
    // 因此按非法参数拒绝，不静默退化。
    if (options.poll_interval > std::chrono::milliseconds{
            std::numeric_limits<int>::max()})
        return result<SPtr>::err(MakeError(ErrorCode::InvalidArgument,
            "config watcher: poll_interval exceeds coroutine timer range"));
    if (!g_scheduler->IsInitialized())
        return result<SPtr>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "config watcher: coroutine runtime not initialized"));

    auto impl = std::make_shared<Impl>();
    try {
        impl->info = bbt::coroutine::CreateObjectInfo("config", "Watcher");
    } catch (const std::logic_error&) {
        // 运行时在检查与创建之间停止初始化：映射为 RuntimeUnavailable，
        // 不泄漏异常（同 ManagedCloseState/NewObjectInfo 先例）。
        return result<SPtr>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "config watcher: coroutine runtime unavailable"));
    }
    impl->sleeper = bbt::coroutine::sync::CoWaiter::Create();
    impl->close_waiters = std::make_shared<detail::CloseWaiters>();
    impl->exit_signal = std::make_shared<LoopExitSignal>();
    impl->source = std::move(source);
    impl->callback = std::move(callback);
    impl->poll_interval = options.poll_interval;
    impl->deliver_initial = options.deliver_initial;

    const std::weak_ptr<Impl> weak = impl;
    const auto sleeper = impl->sleeper;
    const auto exit_signal = impl->exit_signal;
    bool registered = false;
    bbtco_noexcept(&registered) [weak, sleeper, exit_signal]() {
        RunWatchLoop(weak, sleeper, exit_signal);
    };
    if (!registered)
        return result<SPtr>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "config watcher: failed to register poll coroutine"));

    return result<SPtr>::ok(SPtr(new Watcher(std::move(impl))));
}

void Watcher::RequestClose() noexcept {
    auto* impl = m_impl.get();
    if (impl == nullptr) return;

    // 与 Deliver 的 open 检查在同一把门下：BeginClose + 唤醒构成「回调
    // 起点线性化点」——本函数返回即不可能再有新回调起点，已通过 open
    // 检查获准的在途回调最多再完成一次 callback 执行（回调不得阻塞是
    // 契约前置，等待有界）。recursive：回调内自关闭重入安全。
    std::lock_guard<std::recursive_mutex> lk(impl->deliver_gate);
    if (impl->close.BeginClose())
        impl->close_waiters->CloseAndWakeAll(); // 原子封口 + 唤醒在册等待者
}

result<void> Watcher::WaitStopped(bbt::coroutine::Deadline deadline) {
    // 仅 native 宿主控制域：受管协程/Processer 域内等待会挂起调度线程；
    // 本 watcher 回调内调用是等待自身（回调在轮询协程内联执行，协程上下文
    // 检查已覆盖该情形）。
    if (!IsNativeHost())
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
            "config watcher: WaitStopped requires native host"));

    auto impl = m_impl;
    auto exit_signal = impl->exit_signal;

    // 前置：先 RequestClose——未封口的等待没有意义（轮询协程不会自行退出；
    // 运行时取消/Impl 失效的退出路径也由 stopped 记号兑现，同样要求显式
    // 封口，保持「关闭决定归 owner」的单一入口）。
    if (impl->close.IsOpen())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "config watcher: RequestClose before WaitStopped"));

    std::unique_lock<std::mutex> lk(exit_signal->mtx);
    if (!exit_signal->cv.wait_until(lk, deadline,
                                    [&] { return exit_signal->stopped; }))
        return result<void>::err(MakeError(ErrorCode::TimedOut,
            "config watcher: poll coroutine stop deadline expired"));
    return result<void>::ok();
}

bool Watcher::IsClosed() const noexcept {
    auto* impl = m_impl.get();
    // 逻辑封口投影：Closing（已 RequestClose）与 Closed（协程已退出）都算
    // 「不再打开」，不区分物理退出与否。
    return impl == nullptr || !impl->close.IsOpen();
}

} // namespace bbt::infra::config
