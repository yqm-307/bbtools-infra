#pragma once
// infra 内部共享 I/O 支撑（Issue #6 抽取）：strand 执行域封装、
// 受管对象关闭态机、WaitStatus 映射与对象身份工厂。
// HTTP（http_detail）与 Redis（redis_detail）模块共用一份实现，
// 不拥有 io_context 或线程；本头仅供 src/ 内部使用，不安装、不进公开面。
//
// 统一执行域：bbt::coroutine::io::GetExecutor() 返回的共享 executor
// 派生 strand，实际驱动线程是 Scheduler 现有 PollOnce 事件循环线程。
// 物理清理约定（契约 §逻辑结果与物理清理分离）：
//  - SealOnIoDomain（只能在 io 域内调用）在 m_post_mtx 内置 m_stopped
//    封死 TryPost，此后不再接纳新的发起型投递。它只是封口，不是排空
//    原语：fd/定时器事件催出的 reactor 完成项不经 TryPost，不能拿
//    「最后一个 handler」充当物理清理落定信号。
//  - 因此各受管对象按 in-flight async 计数自行判定落定——计数归零
//    才 MarkClosed；owner 聚合子对象 MarkClosed 之后才封口引擎。
//  - 共享 context 由 coroutine 全局 EventLoop 拥有并持续推进；关闭
//    链路不得 stop/restart 共享 context。

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>

#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/sync/CompletionSignal.hpp>
#include <bbt/coroutine/io/IoExecutor.hpp>

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra::detail {

using IoStrand = boost::asio::strand<boost::asio::any_io_executor>;

// IoEngine：后端 I/O 的串行执行域封装（不携带业务 limits，由拥有者自持）。
// 所有发起型投递必须经 TryPost 入口，m_post_mtx 内先查停止标志再入队，
// 保证 SealOnIoDomain 之后不会再有新 handler 落进已封队列。
class IoEngine {
public:
    // 控制线程调用，幂等且至多生效一次：经 bbt::coroutine::io::GetExecutor()
    // 取得共享 executor 并建立 strand。executor 不可得/无效时返回
    // RuntimeUnavailable——infra 不接触原始 io_context，无法自治兜底。
    result<void> Start() {
        boost::asio::any_io_executor ex;
        try {
            ex = bbt::coroutine::io::GetExecutor();
        } catch (...) {
            return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
                "coroutine shared io executor unavailable"));
        }
        if (!ex)
            return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
                "coroutine shared io executor is empty"));

        std::lock_guard<std::mutex> lk(m_post_mtx);
        if (m_io)
            return result<void>::ok();   // 幂等：并发 Start 只建立一次
        m_io.emplace(ex);
        return result<void>::ok();
    }

    // 仅 Start 成功后有效；之前调用是编程错误（工厂已按 running 态门控）。
    const IoStrand& Io() const noexcept { return *m_io; }

    // 所有「发起型」投递（请求/中止/关闭派发）必须经此入口。
    // 返回 false 表示 io 域未启动、已封，或 post 自身分配失败——调用方按
    // 「后端已回收」处理。noexcept 调用方依赖本方法不向调用方抛：
    // post 可能抛 bad_alloc，统一吞为 false。
    template <class F>
    bool TryPost(F&& f) {
        std::lock_guard<std::mutex> lk(m_post_mtx);
        if (m_stopped || !m_io)
            return false;
        try {
            boost::asio::post(*m_io, std::forward<F>(f));
        } catch (...) {
            return false;
        }
        return true;
    }

    // 只能在 io 域内调用：封死 TryPost，此后新投递一律拒绝。
    // 不承诺任何已受理 handler 已落定。
    void SealOnIoDomain() noexcept {
        std::lock_guard<std::mutex> lk(m_post_mtx);
        m_stopped = true;
    }

    bool Started() const noexcept {
        std::lock_guard<std::mutex> lk(m_post_mtx);
        return m_io.has_value();
    }
    bool Stopped() const noexcept {
        std::lock_guard<std::mutex> lk(m_post_mtx);
        return m_stopped;
    }

private:
    std::optional<IoStrand> m_io;
    mutable std::mutex      m_post_mtx;
    bool                    m_stopped{false};   // m_post_mtx 保护
};

// WaitStatus → 请求侧 Error 的固定映射（Completed 不出现）。
inline Error WaitStatusToError(bbt::coroutine::WaitStatus status) {
    using bbt::coroutine::WaitStatus;
    switch (status) {
    case WaitStatus::TimedOut:
        return MakeError(ErrorCode::TimedOut, "request deadline exceeded");
    case WaitStatus::Cancelled:
        return MakeError(ErrorCode::Cancelled, "request cancelled");
    case WaitStatus::InvalidContext:
        return MakeError(ErrorCode::InvalidContext,
            "request must run in coroutine context");
    case WaitStatus::AlreadyWaiting:
        return MakeError(ErrorCode::InternalError,
            "internal: duplicate waiter on request signal");
    case WaitStatus::RuntimeUnavailable:
        return MakeError(ErrorCode::RuntimeUnavailable,
            "coroutine runtime unavailable for request wait");
    case WaitStatus::Completed:
        break;
    }
    return MakeError(ErrorCode::InternalError, "internal: unexpected wait status");
}

// WaitStatus → CloseStatus 固定映射（契约 §关闭规则）。
inline CloseStatus WaitStatusToCloseStatus(
    bbt::coroutine::WaitStatus status) noexcept {
    using bbt::coroutine::WaitStatus;
    switch (status) {
    case WaitStatus::Completed:          return CloseStatus::Closed;
    case WaitStatus::TimedOut:           return CloseStatus::TimedOut;
    case WaitStatus::Cancelled:          return CloseStatus::Cancelled;
    case WaitStatus::InvalidContext:     return CloseStatus::InvalidContext;
    case WaitStatus::AlreadyWaiting:     return CloseStatus::AlreadyWaiting;
    case WaitStatus::RuntimeUnavailable: return CloseStatus::RuntimeUnavailable;
    }
    return CloseStatus::RuntimeUnavailable;
}

// 每个受管对象一份的关闭态机：Open→Closing→Closed，不重开。
// RequestClose 幂等可由任意线程发起；teardown 完成（io 域）后
// MarkClosed 使 WaitClosed 观察者返回 Closed。
class ManagedCloseState {
public:
    enum Phase : int { kOpen = 0, kClosing = 1, kClosed = 2 };

    explicit ManagedCloseState(
        std::shared_ptr<bbt::coroutine::CompletionSignal> sig)
        : m_sig(std::move(sig)) {}

    // Open→Closing；仅首次返回 true，调用方据此执行一次性 teardown。
    bool BeginClose() noexcept {
        int expected = kOpen;
        return m_phase.compare_exchange_strong(expected, kClosing);
    }
    // 物理清理落定：幂等（仅首次落实者发信号与回调）。
    // 「Closed = 后端不会再访问本组件拥有的操作资源」，而非仅收到关闭意图。
    void MarkClosed() noexcept {
        if (m_phase.exchange(kClosed) == kClosed)
            return;
        m_sig->Complete(); // one-shot：晚到/重复安全
        if (m_hook)
            m_hook();
    }
    // 发布前一次性注册：对象逃逸到其他线程之前由工厂设置。
    void SetClosedHook(std::function<void()> hook) noexcept {
        m_hook = std::move(hook);
    }
    bool IsClosed() const noexcept { return m_phase.load() == kClosed; }
    bool IsOpen()    const noexcept { return m_phase.load() == kOpen; }

    // 契约顺序：先校验协程上下文与运行时代际，已关闭对象在合法上下文
    // 立即返回 Closed；否则经 CompletionSignal 挂起等待并按固定映射返回。
    CloseStatus WaitClosed(bbt::coroutine::Deadline          deadline,
                           bbt::coroutine::CancellationToken cancel,
                           bbt::coroutine::RuntimeGeneration generation) {
        if (g_bbt_tls_coroutine_co == nullptr)
            return CloseStatus::InvalidContext;
        const auto gen = bbt::coroutine::CurrentRuntimeGeneration();
        if (gen == 0 || gen != generation)
            return CloseStatus::RuntimeUnavailable;
        if (IsClosed())
            return CloseStatus::Closed;
        bbt::coroutine::WaitOptions opt;
        opt.deadline = deadline;
        opt.cancel   = std::move(cancel);
        return WaitStatusToCloseStatus(m_sig->Wait(opt));
    }

private:
    std::shared_ptr<bbt::coroutine::CompletionSignal> m_sig;
    std::atomic<int> m_phase{kOpen};
    std::function<void()> m_hook;   // 仅发布前写一次，此后只读
};

// CreateObjectInfo / CompletionSignal 构造需要有效运行时代际；
// 抛出的 logic_error 统一映射为 RuntimeUnavailable。
inline result<bbt::coroutine::CoObjectInfo> NewObjectInfo(std::string kind) {
    try {
        return result<bbt::coroutine::CoObjectInfo>::ok(
            bbt::coroutine::CreateObjectInfo(std::move(kind), ""));
    } catch (const std::logic_error&) {
        return result<bbt::coroutine::CoObjectInfo>::err(MakeError(
            ErrorCode::RuntimeUnavailable,
            "coroutine runtime generation unavailable"));
    }
}

inline result<std::shared_ptr<bbt::coroutine::CompletionSignal>>
NewCompletionSignal() {
    try {
        return result<std::shared_ptr<bbt::coroutine::CompletionSignal>>::ok(
            std::make_shared<bbt::coroutine::CompletionSignal>());
    } catch (const std::logic_error&) {
        return result<std::shared_ptr<bbt::coroutine::CompletionSignal>>::err(
            MakeError(ErrorCode::RuntimeUnavailable,
                "coroutine runtime generation unavailable"));
    }
}

} // namespace bbt::infra::detail
