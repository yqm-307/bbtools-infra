#pragma once
// infra 内部共享 I/O 支撑（Issue #6 抽取）：strand 执行域封装、
// 受管对象关闭态机、同步关闭的挂起等待者登记、WaitStatus 映射与对象身份工厂。
// HTTP（http_detail）与 Redis（redis_detail）模块共用一份实现，
// 不拥有 io_context 或线程；本头仅供 src/ 内部使用，不安装、不进公开面。
//
// 统一执行域：bbt::coroutine::io::GetExecutor() 返回的共享 executor
// 派生 strand，实际驱动线程是 Scheduler 现有 PollOnce 事件循环线程。
// 物理清理约定（进程寿命运行时修订）：
//  - SealOnIoDomain（只能在 io 域内调用）在 m_post_mtx 内置 m_stopped
//    封死 TryPost，此后不再接纳新的发起型投递。它只是封口，不是排空
//    原语：fd/定时器事件催出的 reactor 完成项不经 TryPost，不能拿
//    「最后一个 handler」充当物理清理落定信号。
//  - 因此各受管对象按 in-flight async 计数自行判定落定——计数归零
//    才 MarkClosed；owner 聚合子对象 MarkClosed 之后才封口引擎。
//  - owner 主动调用的 Close() 是同步的：封口 → 唤醒挂起等待者 →
//    有界等待在途归零 → 物理释放。跨线程调用经 CloseWaiters（Notify
//    跨线程安全）+ std::condition_variable 状态等待实现，不引入通用
//    完成信号或取消令牌。
//  - 共享 context 由 coroutine 全局 EventLoop 拥有并持续推进；关闭
//    链路不得 stop/restart 共享 context。

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unistd.h>
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

    // 只能在 owner 域门内调用（io 域 handler 入口，或 Close 的同步
    // teardown）：封死 TryPost，此后新投递一律拒绝。
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

    // owner 域门：本引擎下属全部后端资源触碰（Asio 对象与第三方 context 的
    // 发起、完成回调、同步 teardown）持同一把门串行。strand 只保证「同域
    // handler 不并发」；本门额外覆盖「owner 从其它线程同步执行物理释放」这
    // 一跨域情形，使各模块 Close() 能在调用线程内完成资源释放，而不与在途
    // io handler 竞争同一后端对象。HTTP 与 Redis 共用本支撑层，因此「门覆盖
    // 了什么」只有一份口径。可重入：同线程内的嵌套（teardown → 对象 Close）
    // 由递归门承接，不会等待自身。
    // 持有纪律：门内只做同步、非阻塞的发起/关闭，绝不跨挂起点持有。
    // 覆盖边界（不夸大）：只覆盖「本模块自己的执行入口」——投递到 strand 的
    // handler 与由本模块提供的 Asio 完成回调。Asio/Beast composed operation
    // 的内部中间步骤不经过任何模块入口，其与跨线程 teardown 的条件性并发是
    // 已知残留，各模块须按实际调用链另有声明，不算作本门承诺。
    std::recursive_mutex& IoGate() const noexcept { return *m_gate; }

    // 门的共享持有形式：需要把门带进 Asio 完成 handler 的长寿对象（如 Redis
    // 的 ev bridge）以 shared_ptr 持有，避免引用在对象析构后悬挂。
    const std::shared_ptr<std::recursive_mutex>& IoGatePtr() const noexcept {
        return m_gate;
    }

private:
    std::optional<IoStrand> m_io;
    mutable std::mutex      m_post_mtx;
    bool                    m_stopped{false};   // m_post_mtx 保护
    const std::shared_ptr<std::recursive_mutex> m_gate{
        std::make_shared<std::recursive_mutex>()};
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
//
// 等待兴趣登记屏障（关闭线性化，修复「已过最后封口检查、尚未完成 fd 兴趣
// 登记」的窗口）：
//  - op 在**建立等待事件前**经 BeginRegister 声明「本次 fd 兴趣登记进行中」：
//    未封口则计数 +1 返回 true，已封口返回 false（调用方按 Closed 立即返回，
//    绝不在已关/将关 fd 上登记旧兴趣）；
//  - EndRegister 在 fd 兴趣登记完成后（co-io-adapter 的 on_registered，即事件
//    已挂到 fd、等待者已在册之后）释放计数；
//  - SealWakeAndDrainRegistrations 供对象级 Close：原子封口、唤醒在册等待者，
//    再等待「登记进行中」计数归零后才返回。BeginRegister/EndRegister 之间只含
//    非阻塞的 fd 事件建立（dup + epoll ADD），不含等待点，故该等待必然有界；
//    它把「最后一次封口检查」与「fd 兴趣登记」拉成同一线性化协议，物理 close
//    不会早于登记完成，晚到 op 也不会在已关 fd 上登记。
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

    // 对象级 Close 的收口原语：封口 + 唤醒在册等待者 + 等待在途 fd 兴趣登记与
    // 「候选 fd 移交」排空 + 恰好 close 一次在册候选 fd（见下方候选 fd 语义）。
    // 幂等；已封口后新 BeginRegister / CreateCandidate 一律失败，故等待必然收敛。
    void SealWakeAndDrainRegistrations() noexcept {
        int  candidate     = -1;
        bool has_candidate = false;
        std::vector<bbt::coroutine::sync::CoWaiter::SPtr> wake;
        {
            std::lock_guard<std::mutex> lk(m_mtx);
            m_closed = true;
            wake.swap(m_waiters);
            // 原子接管在册候选 fd：此后 dial 侧 CreateCandidate/Commit 见 m_closed
            // 一律失败，不会与本次 close 竞争同一 fd。
            has_candidate   = m_has_candidate;
            candidate       = m_candidate_fd;
            m_has_candidate = false;
            m_candidate_fd  = -1;
        }
        for (auto& waiter : wake)
            waiter->Notify();
        {
            std::unique_lock<std::mutex> lk(m_mtx);
            m_reg_drained.wait(lk, [this] {
                return m_registrations == 0 && m_handoffs == 0;
            });
        }
        // 登记与移交均排空后恰好 close 一次：dial 侧已见我接管（返回 false），
        // 不会重复 close，也不会触碰可能被复用的 fd 号。
        if (has_candidate && candidate >= 0)
            ::close(candidate);
    }

    // 见类头：未封口则登记一个「进行中的 fd 兴趣登记」，返回 true；已封口返回
    // false（调用方按 Closed 立即返回，不登记旧 fd）。
    bool BeginRegister() noexcept {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (m_closed)
            return false;
        ++m_registrations;
        return true;
    }

    // 与 BeginRegister 配对，幂等安全（多余调用不会把计数压到负）。
    void EndRegister() noexcept {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (m_registrations > 0)
            --m_registrations;
        if (m_registrations == 0)
            m_reg_drained.notify_all();
    }

    // ---- 在途「候选 fd」的同步收口（仅受管 dial 装配移交令牌时启用）----
    // 背景：非阻塞 connect 期间 socket fd 尚未落入连接对象；若只由 dial 协程
    // 在恢复时释放，Close 返回当刻 fd 可能仍活，违背 ICoCloseable「返回即物理
    // 释放」。以下原语把候选 fd 的所有权在 connect 等待期交入本关闭登记，owner
    // 封口即同步收口它，dial 恢复后不得双关或触碰可能复用的 fd 号。
    //
    // CreateCandidate：在封口门内建立候选 fd（factory 只做非阻塞 socket/
    //   SetNonBlocking/connect，锁内执行，与物理 close 同门）。已封口返回 false
    //   （调用方不得创建 socket/connect）；未封口则执行 factory，成功（fd>=0）
    //   即记名，owner 封口会在登记与移交排空后恰好 close 它一次。
    bool CreateCandidate(const std::function<int()>& factory) noexcept {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (m_closed)
            return false;
        const int fd = factory();
        if (fd >= 0) {
            m_candidate_fd  = fd;
            m_has_candidate = true;
        }
        return true;
    }

    // TakeCandidate：失败/清理路径取回候选 fd 所有权。返回 true 时 *out_fd 为
    // fd（调用方负责 close/重试）；返回 false 表示 owner 已接管（fd 已/将
    // close），调用方不得再触碰该 fd。
    bool TakeCandidate(int* out_fd) noexcept {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (m_closed || !m_has_candidate)
            return false;
        *out_fd         = m_candidate_fd;
        m_has_candidate = false;
        m_candidate_fd  = -1;
        return true;
    }

    // BeginHandoff：成功路径原子「登记移交在途 + 取回候选 fd」。返回 true 时
    // 调用方取得 *out_fd，且必须在 fd 所有权落位（连接对象发布）后调用
    // EndHandoff；封口等待（SealWakeAndDrainRegistrations）会等到该移交归零，
    // 使 Close 返回当刻 fd 必然已收口或已由连接对象拥有。返回 false 表示 owner
    // 已接管，调用方不得触碰。
    bool BeginHandoff(int* out_fd) noexcept {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (m_closed || !m_has_candidate)
            return false;
        *out_fd         = m_candidate_fd;
        m_has_candidate = false;
        m_candidate_fd  = -1;
        ++m_handoffs;
        return true;
    }

    // 与 BeginHandoff 配对，幂等安全。
    void EndHandoff() noexcept {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (m_handoffs > 0)
            --m_handoffs;
        if (m_handoffs == 0)
            m_reg_drained.notify_all();
    }

    // ---- 失败/清理路径的关闭门内收口（F-1a）----
    // 立即 connect 失败、等待失败、SO_ERROR 失败三类路径若「先取出候选 fd / 释放
    // 移交在途，再下一语句 close」，会让封口排空谓词（registrations==0 &&
    // handoffs==0）先于物理 close 归零，使并发 Close/Disconnect 返回当刻 fd 可能
    // 仍活。以下两原语在关闭门（本 m_mtx）内先 close 再归零计数，保证
    // SealWakeAndDrainRegistrations 的等待条件只在 fd 已 close 后成立。

    // 关闭门内收口「仍在候选槽登记」的 fd（立即 connect 失败 / 等待失败路径）。
    // 与 owner 封口在同一把 m_mtx 下互斥：owner 观测到候选已不在冊时该 fd 必已
    // close。返回 true 表示本次由调用方关闭并清除登记（可继续重试下一地址）；
    // 返回 false 表示 owner 已封口并接管候选 fd（fd 已/将由 owner close），调用方
    // 不得再触碰，按 Closed 收口。
    bool CloseCandidateInGate() noexcept {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (m_closed)
            return false;
        if (m_has_candidate && m_candidate_fd >= 0)
            ::close(m_candidate_fd);
        m_has_candidate = false;
        m_candidate_fd  = -1;
        return true;
    }

    // 关闭门内收口「已移交在途」的候选 fd（成功 connect 后 SO_ERROR 失败、重试
    // 下一地址前的路径）：该 fd 已由 BeginHandoff 移出候选槽并登记为一次在途移交。
    // close 先于归还移交计数与 notify，保证排空谓词（handoffs==0）只在 fd 已 close
    // 后成立。owner 不会在此刻接管（fd 已出候选槽），故调用方总是持有该 fd 并负责
    // 本次 close，不双关。
    void CloseHandoffFdAndEndHandoff(int fd) noexcept {
        std::lock_guard<std::mutex> lk(m_mtx);
        if (fd >= 0)
            ::close(fd);
        if (m_handoffs > 0)
            --m_handoffs;
        if (m_handoffs == 0)
            m_reg_drained.notify_all();
    }

    bool Closed() const noexcept {
        std::lock_guard<std::mutex> lk(m_mtx);
        return m_closed;
    }

private:
    mutable std::mutex                                m_mtx;
    std::condition_variable                           m_reg_drained;
    bool                                              m_closed{false};
    std::size_t                                       m_registrations{0};  // m_mtx 保护
    std::size_t                                       m_handoffs{0};       // m_mtx 保护（候选 fd 移交在途）
    int                                               m_candidate_fd{-1};  // m_mtx 保护
    bool                                              m_has_candidate{false}; // m_mtx 保护
    std::vector<bbt::coroutine::sync::CoWaiter::SPtr> m_waiters;
};

// BeginRegister/EndRegister 的 RAII 配对（异常安全）：析构时若仍持有登记则释放，
// 避免 fd 事件建立途中抛出把登记计数泄漏、令对象级 Close 的排空等待永挂。
class CloseRegisterClaim {
public:
    CloseRegisterClaim() = default;
    explicit CloseRegisterClaim(CloseWaiters* waiters) noexcept : m_waiters(waiters) {
        if (m_waiters != nullptr && !m_waiters->BeginRegister())
            m_waiters = nullptr;   // 已封口：未取得登记
    }
    ~CloseRegisterClaim() { release(); }
    CloseRegisterClaim(const CloseRegisterClaim&) = delete;
    CloseRegisterClaim& operator=(const CloseRegisterClaim&) = delete;

    bool acquired() const noexcept { return m_waiters != nullptr; }
    void release() noexcept {
        if (m_waiters != nullptr) {
            m_waiters->EndRegister();
            m_waiters = nullptr;
        }
    }

private:
    CloseWaiters* m_waiters{nullptr};
};

// 每个受管对象一份的关闭态机：Open→Closing→Closed，不重开。
// 关闭由 owner 主动同步发起（Close）；物理清理落定（in-flight 归零）后
// MarkClosed 跑一次 closed hook。并发/重复 Close 的非首次调用者经 WaitClosed
// 等待首个调用者完成物理收口，使每个合法调用者返回当刻都观察到 Closed 终态
// （ICoCloseable：返回即物理资源已释放）。
class ManagedCloseState {
public:
    enum Phase : int { kOpen = 0, kClosing = 1, kClosed = 2 };

    ManagedCloseState() = default;

    // Open→Closing；仅首次返回 true，调用方据此执行一次性 teardown。
    bool BeginClose() noexcept {
        int expected = kOpen;
        return m_phase.compare_exchange_strong(expected, kClosing);
    }
    // 物理清理落定：幂等（仅首次落实者跑回调并唤醒等待者）。
    // 「Closed = 后端不会再访问本组件拥有的操作资源」，而非仅收到关闭意图。
    void MarkClosed() noexcept {
        if (m_phase.exchange(kClosed) == kClosed)
            return;
        if (m_hook)
            m_hook();
        std::lock_guard<std::mutex> lk(m_mtx);
        m_cv.notify_all();
    }
    // 非首次 Close 调用者：等待首个调用者把物理收口落定为 Closed。
    // 首个调用者同步完成收口，故该等待有界；对已闭合对象调用立即返回。
    void WaitClosed() const noexcept {
        std::unique_lock<std::mutex> lk(m_mtx);
        m_cv.wait(lk, [this] { return m_phase.load() == kClosed; });
    }
    // 发布前一次性注册：对象逃逸到其他线程之前由工厂设置。
    void SetClosedHook(std::function<void()> hook) noexcept {
        m_hook = std::move(hook);
    }
    bool IsClosed() const noexcept { return m_phase.load() == kClosed; }
    bool IsOpen()    const noexcept { return m_phase.load() == kOpen; }

private:
    std::atomic<int> m_phase{kOpen};
    std::function<void()> m_hook;   // 仅发布前写一次，此后只读
    mutable std::mutex              m_mtx;
    mutable std::condition_variable m_cv;
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

// §4.0.1.7（Issue #32）：Runtime 级传输在途账本。覆盖受管 DialTCP
// 等待段、CoTCPListener::Accept 与 TCP/UDP 可等待数据操作；Try* 不经
// 账本。容量满立即拒绝（不排队），名额由对象在 op 结束时归还。
//
// Stop 安全：账本由 Runtime 与各受管对象经 shared_ptr 共享持有
// （quota_scope）。Scheduler::Stop 对挂起协程直接销毁、不展开栈
// （coroutine 契约 §6），因此归还不能依赖协程栈 RAII——对象侧用
// m_quota_held 记录未归还名额，op 正常结束归还 1，Close/析构
// 经 DrainQuota 一次性归还剩余，两种路径互补且无重复归还。
struct InflightLedger {
    explicit InflightLedger(std::size_t cap_) : cap(cap_) {}
    bool TryAdmit() noexcept {
        std::lock_guard<std::mutex> lk(mtx);
        if (count >= cap)
            return false;
        ++count;
        return true;
    }
    void Release(std::size_t n = 1) noexcept {
        std::lock_guard<std::mutex> lk(mtx);
        count = n >= count ? 0 : count - n;
    }
    std::size_t Held() const noexcept {
        std::lock_guard<std::mutex> lk(mtx);
        return count;
    }
    const std::size_t   cap;
    mutable std::mutex  mtx;
    std::size_t         count{0};
};

} // namespace bbt::infra::detail
