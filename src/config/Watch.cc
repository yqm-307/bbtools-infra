#include <bbt/infra/config/Watch.hpp>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <utility>

#include <bbt/coroutine/detail/Define.hpp>   // g_bbt_tls_coroutine_co
#include <bbt/coroutine/detail/Hook.hpp>     // Hook_Sleep
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/object/CoObject.hpp> // CurrentRuntimeGeneration / CreateObjectInfo
#include <bbt/coroutine/sync/CompletionSignal.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp> // bbtco_noexcept / bbtco_sleep

namespace bbt::infra::config {
namespace {

// 关闭态机：Open → Closing → Closed，不重开（同 ICoCloseable 契约）。
// Closing 只表示「已请求关闭」；Closed 表示轮询循环已 drain 完毕。
constexpr int kPhaseOpen = 0;
constexpr int kPhaseClosing = 1;
constexpr int kPhaseClosed = 2;

// WaitStatus → CloseStatus 固定映射（co-network/v1 §关闭规则）。
bbt::infra::CloseStatus MapWaitStatus(bbt::coroutine::WaitStatus status) noexcept {
    using bbt::coroutine::WaitStatus;
    switch (status) {
    case WaitStatus::Completed:          return bbt::infra::CloseStatus::Closed;
    case WaitStatus::TimedOut:           return bbt::infra::CloseStatus::TimedOut;
    case WaitStatus::Cancelled:          return bbt::infra::CloseStatus::Cancelled;
    case WaitStatus::InvalidContext:     return bbt::infra::CloseStatus::InvalidContext;
    case WaitStatus::AlreadyWaiting:     return bbt::infra::CloseStatus::AlreadyWaiting;
    case WaitStatus::RuntimeUnavailable: return bbt::infra::CloseStatus::RuntimeUnavailable;
    }
    return bbt::infra::CloseStatus::RuntimeUnavailable;
}

} // namespace

struct Watcher::Impl {
    SourcePtr source;
    WatchCallback callback;
    std::chrono::milliseconds poll_interval{100};
    bool deliver_initial{true};

    bbt::coroutine::CoObjectInfo info;

    // 关闭态与完成信号都由栈外持有者（Watcher/Impl）一侧拥有；等待路径
    // 不把它们复制到协程栈上。协程契约 §6 的强制 Stop 直接销毁挂起协程、
    // 不做栈展开，栈上的强引用会把配置源永久钉住（#35 已证实反例）。
    std::atomic<int> phase{kPhaseOpen};
    std::shared_ptr<bbt::coroutine::CompletionSignal> signal;

    bool Open() const noexcept {
        return phase.load(std::memory_order_acquire) == kPhaseOpen;
    }
    bool Closed() const noexcept {
        return phase.load(std::memory_order_acquire) == kPhaseClosed;
    }

    // drain 完成（仅轮询循环协程调用）：置 Closed 并唤醒 WaitClosed 等待者。
    // 强制 Stop 销毁循环协程时本函数不执行：态停留 Closing，IsClosed 保持
    // false，不伪装已关闭。
    void MarkClosed() noexcept {
        phase.store(kPhaseClosed, std::memory_order_release);
        if (signal) signal->Complete(); // one-shot：晚到/重复安全
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
    // 投递前检查关闭态：RequestClose 被观察到后不再发起新的消费者调用，
    // 本次拍的读结果随之丢弃（「关闭与正在读取/待投递通知交错」的确定性
    // 边界）。旧实现只在拍与拍之间检查，关闭请求落在 Read 与投递之间时
    // 会多投递一个晚到事件。
    // 不在回调内持锁：回调自身调用 RequestClose/IsClosed 不得自锁，因此
    // 关闭请求与「刚通过检查、尚未进入回调」的单个事件允许并发，drain
    // 完成（WaitClosed → Closed）后不再有任何消费者调用。
    void Deliver(const WatchEvent& event) {
        if (!Open()) return;
        callback(event);
    }

    // 轮询循环专属状态（仅协程 worker 访问，单线程）：
    bool has_last{false};
    std::uint64_t last_version{0};
    std::string last_revision;
    bool failed{false};
};

namespace {

// 轮询循环：持有 weak_ptr，不依赖协程栈析构持有 Impl 强引用。
// 关键：强引用只在本拍计算范围内持有，必须在 bbtco_sleep 挂起前释放——
// 强制 Stop 销毁挂起协程时不做栈展开，若挂起期间栈上仍持有
// shared_ptr<Impl>，其析构不执行，Impl 的强计数永不归零、耐久资源
// （源/回调）泄漏。挂起前释放后，挂起期间只剩 lambda 捕获的 weak_ptr，
// 强制 Stop 只残留一个控制块 weak 计数，源随外部句柄释放而析构。
void RunWatchLoop(std::weak_ptr<Watcher::Impl> weak) {
    for (;;) {
        bool done = false;
        int interval_ms = 0;
        {
            auto self = weak.lock();
            if (!self) return; // Impl 已被外部释放，安全退出
            if (self->Open()) {
                self->PollTick();
                interval_ms = static_cast<int>(self->poll_interval.count());
            } else {
                done = true;
            }
        } // self 在此释放：挂起期间不持有 Impl 强引用
        if (done) break;
        bbtco_sleep(interval_ms);
    }
    // drain 完成：置 closed 并唤醒 WaitClosed。
    if (auto self = weak.lock()) self->MarkClosed();
}

} // namespace

Watcher::Watcher(std::shared_ptr<Impl> impl) : m_impl(std::move(impl)) {}

Watcher::~Watcher() {
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
    if (bbt::coroutine::CurrentRuntimeGeneration() == 0)
        return result<SPtr>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "config watcher: scheduler not running"));

    auto impl = std::make_shared<Impl>();
    try {
        impl->info = bbt::coroutine::CreateObjectInfo("config", "Watcher");
        // 完成信号与对象身份同受运行时代际约束：代际为 0（并发 Stop）时
        // 构造函数抛 logic_error，按本仓先例（ManagedCloseState/NewObjectInfo）
        // 映射为 RuntimeUnavailable，不泄漏异常。
        impl->signal = std::make_shared<bbt::coroutine::CompletionSignal>();
    } catch (const std::logic_error&) {
        return result<SPtr>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "config watcher: runtime generation unavailable"));
    }
    impl->source = std::move(source);
    impl->callback = std::move(callback);
    impl->poll_interval = options.poll_interval;
    impl->deliver_initial = options.deliver_initial;

    const std::weak_ptr<Impl> weak = impl;
    bool registered = false;
    bbtco_noexcept(&registered) [weak]() { RunWatchLoop(weak); };
    if (!registered)
        return result<SPtr>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "config watcher: failed to register poll coroutine"));

    return result<SPtr>::ok(SPtr(new Watcher(std::move(impl))));
}

void Watcher::RequestClose() noexcept {
    auto* impl = m_impl.get();
    if (impl == nullptr) return;
    int expected = kPhaseOpen;
    impl->phase.compare_exchange_strong(expected, kPhaseClosing); // 幂等
    // 轮询循环在下一拍（≤ poll_interval）观察到非 Open 并退出；drain 完成后
    // 由循环协程置 Closed 并唤醒 WaitClosed，无需在此显式唤醒。
}

bool Watcher::IsClosed() const noexcept {
    auto* impl = m_impl.get();
    return impl == nullptr || impl->Closed();
}

bbt::infra::CloseStatus Watcher::WaitClosed(bbt::coroutine::Deadline deadline,
                                            bbt::coroutine::CancellationToken cancel) {
    // 关闭态与完成信号由 Impl（栈外持有者）拥有：本函数只经 m_impl 的
    // 裸解引用访问它们，不把任何 shared_ptr 复制到协程栈上，恢复后也不
    // 回访 Impl 以外的所有者。协程契约 §6 的强制 Stop 不展开挂起栈，
    // 栈上的 shared_ptr<Impl> 会永久钉住配置源（#35 父探针反例）。
    // 调用方须在等待期间保持 Watcher 存活（co-network/v1 §关闭规则）。
    if (!m_impl) return bbt::infra::CloseStatus::Closed;
    if (g_bbt_tls_coroutine_co == nullptr)
        return bbt::infra::CloseStatus::InvalidContext;
    const auto generation = bbt::coroutine::CurrentRuntimeGeneration();
    if (generation == 0 || generation != m_impl->info.generation)
        return bbt::infra::CloseStatus::RuntimeUnavailable;
    if (m_impl->Closed()) return bbt::infra::CloseStatus::Closed;

    bbt::coroutine::WaitOptions wait;
    wait.deadline = deadline;
    wait.cancel = std::move(cancel);
    // 结果由等待竞争点一次定死（契约 §C1/#347③）：不在恢复后做第二遍
    // 优先级重判。每个对象至多一个并发等待者，第二个返回 AlreadyWaiting
    // （CompletionSignal 唯一等待位）。
    return MapWaitStatus(m_impl->signal->Wait(wait));
}

} // namespace bbt::infra::config
