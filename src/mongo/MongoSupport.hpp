#pragma once
// mongo 模块内部支撑（Issue #7）：完成回投用的共享 executor strand 封装、
// 同步关闭的挂起等待者登记、受管对象关闭态机、WaitStatus 映射与对象身份工厂。
// 与 http_detail / redis_detail 的同名机制同构但独立副本——各模块实现互不
// 引用，本头仅供 src/mongo/ 使用，不安装、不进公开面。
//
// 统一执行域：bbt::coroutine::io::GetExecutor() 返回的共享 executor
// 派生 strand，实际驱动线程是 Scheduler 现有 PollOnce 事件循环线程。
// 本模块的阻塞 driver 调用不落在 io 域（worker 线程承担），io 域只用
// 于完成回投——TryPost 是「发起型投递」的唯一入口，Seal 后一律拒绝。
//
// 关闭约定（进程寿命运行时修订，与契约 §1 一致）：
//  - Close() 由 owner 主动同步发起：封口 → 唤醒全部挂起业务等待者
//    （CloseWaiters）→ 在自身锁 + condition_variable 上有界等待在途归零
//    → 一次性跑 closed hook。跨线程调用安全，幂等。
//  - CompletionSignal / CancellationToken / RuntimeGeneration 已从上游删除，
//    本模块不保留任何兼容壳。

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>

#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/io/IoExecutor.hpp>

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra::mongo_detail {

using IoStrand = boost::asio::strand<boost::asio::any_io_executor>;

// MongoIoEngine：完成回投的串行执行域封装（不拥有 io_context 或线程）。
// 所有发起型投递经 TryPost 入口，m_post_mtx 内先查停止标志再入队，
// 保证 Seal 之后不会再有新 handler 落进已封队列。
class MongoIoEngine {
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

    // 所有「发起型」投递必须经此入口。返回 false 表示 io 域未启动、
    // 已封，或 post 自身分配失败——调用方按「执行域不可达」处理。
    // noexcept 调用方依赖本方法不向调用方抛：post 可能抛 bad_alloc，
    // 统一吞为 false。
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

    // 关闭链路调用：封死 TryPost，此后新投递一律拒绝。
    // 不承诺任何已受理 handler 已落定——落定判据是 owner 的在途计数。
    void Seal() noexcept {
        std::lock_guard<std::mutex> lk(m_post_mtx);
        m_stopped = true;
    }

    bool Started() const noexcept {
        std::lock_guard<std::mutex> lk(m_post_mtx);
        return m_io.has_value();
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

// 同步 Close() 的在途排空上限：Close 在其调用线程有界等待在途计数归零，
// 超时即放弃等待并继续物理释放（调用方可据此记录告警，不无限阻塞）。
inline constexpr std::chrono::milliseconds kCloseDrainTimeout{5000};

// 关闭期挂起等待者登记（Close() 的唤醒侧，跨线程安全）。
// 语义：
//  - 挂起 op 在等待事件登记成功后（on_registered 内）经 Add 登记；Close 已
//    开始时返回 false，调用方必须在此刻自行 Notify 一次——事件与等待位均已
//    就绪，Notify 走 CoPollEvent 的 PENDING 路径，不会丢唤醒。
//  - CloseAndWakeAll 原子封口并唤醒全部在册等待者（幂等）；此后 Add 一律
//    false，新 op 按「已封口」立即返回 Closed，不再挂起。
//  - 唤醒用 shared_ptr 快照：Close 侧复制后即使 op 先恢复并解绑也不会悬垂；
//    晚到的 Notify 对已决议等待者返回 -1，无副作用。
//  - 本类型只负责「登记 + 唤醒」；物理排空判据仍是各对象自己的在途计数。
class CloseWaiters {
public:
    bool Add(const bbt::coroutine::sync::CoWaiter::SPtr& waiter) {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (m_closed)
            return false;
        m_waiters.push_back(waiter);
        return true;
    }

    void Remove(const bbt::coroutine::sync::CoWaiter* waiter) noexcept {
        std::lock_guard<std::mutex> lk(m_mtx);
        for (auto it = m_waiters.begin(); it != m_waiters.end(); ++it) {
            if (it->get() == waiter) {
                m_waiters.erase(it);
                return;
            }
        }
    }

    void CloseAndWakeAll() noexcept {
        std::vector<bbt::coroutine::sync::CoWaiter::SPtr> wake;
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_closed = true;
            wake.swap(m_waiters);
        }
        for (auto& waiter : wake)
            waiter->Notify();
    }

    bool Closed() const noexcept {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_closed;
    }

private:
    mutable std::mutex                                m_mtx;
    bool                                              m_closed{false};
    std::vector<bbt::coroutine::sync::CoWaiter::SPtr> m_waiters;
};

// 每个受管对象一份的关闭态机：Open→Closing→Closed，不重开。
// 关闭由 owner 主动同步发起（Close）；物理清理落定（在途归零）后
// MarkClosed 跑一次 closed hook。没有「等待关闭完成」的公共入口。
class ManagedCloseState {
public:
    enum Phase : int { kOpen = 0, kClosing = 1, kClosed = 2 };

    ManagedCloseState() = default;

    // Open→Closing；仅首次返回 true，调用方据此执行一次性 teardown。
    bool BeginClose() noexcept {
        int expected = kOpen;
        return m_phase.compare_exchange_strong(expected, kClosing);
    }
    // 物理清理落定：幂等（仅首次落实者跑回调）。
    // 「Closed = 后端不会再访问本组件拥有的操作资源」，而非仅收到关闭意图。
    void MarkClosed() noexcept {
        if (m_phase.exchange(kClosed) == kClosed)
            return;
        if (m_hook)
            m_hook();
    }
    // 发布前一次性注册：对象逃逸到其他线程之前由工厂设置。
    void SetClosedHook(std::function<void()> hook) noexcept {
        m_hook = std::move(hook);
    }
    bool IsClosed() const noexcept { return m_phase.load() == kClosed; }
    bool IsOpen()    const noexcept { return m_phase.load() == kOpen; }

private:
    std::atomic<int>      m_phase{kOpen};
    std::function<void()> m_hook;   // 仅发布前写一次，此后只读
};

// CreateObjectInfo 的前置条件是「运行时已初始化」（不再有运行时代际）；
// 抛出的 logic_error 统一映射为 RuntimeUnavailable。
inline result<bbt::coroutine::CoObjectInfo> NewObjectInfo(std::string kind) {
    try {
        return result<bbt::coroutine::CoObjectInfo>::ok(
            bbt::coroutine::CreateObjectInfo(std::move(kind), ""));
    } catch (const std::logic_error&) {
        return result<bbt::coroutine::CoObjectInfo>::err(MakeError(
            ErrorCode::RuntimeUnavailable,
            "coroutine runtime not initialized"));
    }
}

} // namespace bbt::infra::mongo_detail
