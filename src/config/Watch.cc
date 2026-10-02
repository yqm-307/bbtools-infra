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

#include "detail/IoSupport.hpp" // ManagedCloseState / CloseWaiters / kCloseDrainTimeout

#include <bbt/coroutine/detail/Define.hpp>      // g_scheduler / g_bbt_tls_coroutine_co
#include <bbt/coroutine/detail/Hook.hpp>        // Hook_Sleep
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>   // IsInitialized
#include <bbt/coroutine/object/CoObject.hpp>    // CreateObjectInfo
#include <bbt/coroutine/sync/CoWaiter.hpp>      // 睡眠等待位 + 关闭唤醒
#include <bbt/coroutine/syntax/SyntaxMacro.hpp> // bbtco_noexcept / bbtco_sleep

namespace bbt::infra::config {

struct Watcher::Impl {
    SourcePtr source;
    WatchCallback callback;
    std::chrono::milliseconds poll_interval{100};
    bool deliver_initial{true};

    bbt::coroutine::CoObjectInfo info;

    // 关闭态机由栈外持有者（Watcher/Impl）一侧拥有：Open→Closing→Closed，
    // 不重开。IsOpen() 为真才读源/投递——Close() 封口后不再产生消费者回调。
    detail::ManagedCloseState close;

    // 轮询协程的睡眠等待位（单等待者）+ 关闭期等待者登记。Close() 经
    // close_waiters 唤醒正在睡眠的轮询协程，不必等满 poll_interval。
    // 等待位与等待集合在 park 期间的保活见 RunWatchLoop：由协程帧上的
    // lambda 捕获持强引用，恢复时协程会回到 CoWaiter::WaitWithCallback
    // 内部继续执行，等待位在 park 期间不得析构。
    bbt::coroutine::sync::CoWaiter::SPtr sleeper;
    std::shared_ptr<detail::CloseWaiters> close_waiters;

    // 轮询协程退出落定：Close 有界等待它。
    std::mutex              exit_mtx;
    std::condition_variable exit_cv;
    bool                    loop_exit{false}; // exit_mtx 保护

    // 轮询协程身份：首次进入 RunWatchLoop 时写入一次，此后只读比较、不解引用
    // （协程可迁移线程，指针身份仍稳定）。Close 据此区分「回调内自关闭」与
    // 「从其它线程/协程关闭」。
    std::atomic<bbt::coroutine::detail::Coroutine*> loop_co{nullptr};

    bool LoopStarted() const noexcept {
        return loop_co.load(std::memory_order_acquire) != nullptr;
    }

    bool IsLoopCoroutine() const noexcept {
        auto* loop = loop_co.load(std::memory_order_acquire);
        return loop != nullptr && loop == g_bbt_tls_coroutine_co;
    }

    void SignalLoopExit() noexcept {
        {
            std::lock_guard<std::mutex> lk(exit_mtx);
            loop_exit = true;
        }
        exit_cv.notify_all();
    }

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
    // 投递前检查封口态：Close()（含回调内自关闭）之后不再发起消费者调用，本次
    // 拍已读出的结果随之丢弃。不在回调内持锁，回调自身调用 Close()/IsClosed()
    // 不会自锁。
    void Deliver(const WatchEvent& event) {
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
// sleeper 由协程帧强持有（bbtco_noexcept 的 lambda 捕获）：挂起期间协程栈上
// 必须保有等待位与等待集合，恢复路径会回到 CoWaiter::WaitWithCallback 内部。
// 该捕获不涉及 Impl，因此挂起期间不钉住 source/callback。
void RunWatchLoop(std::weak_ptr<Watcher::Impl> weak,
                  const bbt::coroutine::sync::CoWaiter::SPtr& sleeper) {
    if (auto self = weak.lock())
        self->loop_co.store(g_bbt_tls_coroutine_co, std::memory_order_release);

    for (;;) {
        std::chrono::milliseconds interval{0};
        std::shared_ptr<detail::CloseWaiters> waiters;
        {
            auto self = weak.lock();
            if (!self) return;                 // Impl 已释放：直接退出
            if (!self->close.IsOpen()) break;  // 已封口：不再读源/投递
            self->PollTick();
            interval = self->poll_interval;
            waiters = self->close_waiters;
        } // 强引用在此释放：挂起期间不持有 Impl

        // 有界睡眠：poll_interval 到点、或 Close 封口时唤醒。on_registered 在
        // 等待事件登记成功后、真正 park 前执行——在此登记进关闭等待集合；
        // Close 已开始时（Add 返回 false）自行 Notify 一次，走 PENDING 早到
        // 路径，不丢唤醒。
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

    // 退出落定：置 Closed（幂等）并唤醒 Close 的有界等待。
    if (auto self = weak.lock()) {
        self->close.MarkClosed();
        self->SignalLoopExit();
    }
}

} // namespace

Watcher::Watcher(std::shared_ptr<Impl> impl) : m_impl(std::move(impl)) {}

Watcher::~Watcher() {
    // 句柄析构即收口：先停轮询协程，再释放 Impl（source/callback 随之释放）。
    if (m_impl) Close();
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
    impl->source = std::move(source);
    impl->callback = std::move(callback);
    impl->poll_interval = options.poll_interval;
    impl->deliver_initial = options.deliver_initial;

    const std::weak_ptr<Impl> weak = impl;
    const auto sleeper = impl->sleeper;
    bool registered = false;
    bbtco_noexcept(&registered) [weak, sleeper]() { RunWatchLoop(weak, sleeper); };
    if (!registered)
        return result<SPtr>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "config watcher: failed to register poll coroutine"));

    return result<SPtr>::ok(SPtr(new Watcher(std::move(impl))));
}

void Watcher::Close() noexcept {
    auto* impl = m_impl.get();
    if (impl == nullptr) return;

    // 幂等：仅首次调用执行封口与唤醒（CloseAndWakeAll 自带幂等，此处不再重复）。
    if (impl->close.BeginClose())
        impl->close_waiters->CloseAndWakeAll(); // 原子封口 + 唤醒在册等待者

    if (impl->close.IsClosed()) return; // 已落定（含回调内自关闭）

    // 回调内自关闭：本线程就是轮询协程，物理停止只能发生在本次回调返回之后，
    // 不能在此等待自身退出。封口已完成（Deliver 不再投递），先落定 Closed；
    // 循环在回调返回后的下一拍退出并补一次 SignalLoopExit。
    if (impl->IsLoopCoroutine()) {
        impl->close.MarkClosed();
        return;
    }

    // 已注册但从未被调度过的轮询协程：无从等待，也不需要等——它首次运行的
    // 第一拍就是「已封口 → 退出」，不读源、不回调；此刻封口已完成，落定
    // Closed 不掩盖任何可观察的协程退出。
    if (!impl->LoopStarted()) {
        impl->close.MarkClosed();
        return;
    }

    // 有界等待轮询协程退出并落定；超时即返回（IsClosed 保持 false）。
    std::unique_lock<std::mutex> lk(impl->exit_mtx);
    impl->exit_cv.wait_for(lk, detail::kCloseDrainTimeout,
                           [impl] { return impl->loop_exit; });
}

bool Watcher::IsClosed() const noexcept {
    auto* impl = m_impl.get();
    return impl == nullptr || impl->close.IsClosed();
}

} // namespace bbt::infra::config
