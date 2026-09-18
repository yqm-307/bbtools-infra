#pragma once
// HttpIoEngine：HTTP 后端 I/O 的串行执行域封装，不拥有 io_context 或线程。
// 所有 Asio/Beast I/O 对象（socket/resolver/acceptor/timer）与发起型
// 投递统一落在 coroutine 共享 executor 派生的 strand（io 域）上；实际驱动
// 线程是 Scheduler 现有 PollOnce 事件循环线程。runtime 与其派生的
// client/server 经 shared_ptr 共享持有本对象；业务 handler 永不进入此域。
//
// 物理清理约定（契约 §逻辑结果与物理清理分离）：
//  - SealOnIoDomain（只能在 io 域内调用）在 m_post_mtx 内置 m_stopped
//    封死 TryPost，此后不再接纳新的发起型投递。它只是封口，不是排空
//    原语：socket close/cancel 催出的 reactor 完成项与 Boost 1.90
//    resolver_thread_pool 回投的完成项都不经 TryPost，与 strand 内
//    既有 handler 没有 FIFO 保证，不能拿「最后一个 handler」充当
//    物理清理落定信号。
//  - 因此各受管对象（ClientOp/HttpSession）按 in-flight async 计数
//    自行判定落定——计数归零才 MarkClosed；runtime 聚合子对象
//    MarkClosed 之后才调本方法封口。
//  - 共享 context 由 coroutine 全局 EventLoop 拥有并持续推进：不再
//    需要旧实现的 stop/restart 两段排空——积压的中止 handler 会自然
//    执行并释放引用环；关闭链路不得 stop/restart 共享 context。

#include <memory>
#include <mutex>
#include <optional>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>

#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra::http_detail {

class HttpIoEngine : public std::enable_shared_from_this<HttpIoEngine> {
public:
    // 统一执行域：共享 executor 上的 strand。I/O 对象以其构造后，
    // 其全部完成 handler（含 composed op 中间 handler）都经 strand
    // 串行执行，等价于旧实现的单 io 线程语义。
    using IoStrand = boost::asio::strand<boost::asio::any_io_executor>;

    explicit HttpIoEngine(const NetworkLimits& limits) : m_limits(limits) {}

    const NetworkLimits& Limits() const noexcept { return m_limits; }

    // 控制线程调用，幂等且至多生效一次：经 bbt::coroutine::io::GetExecutor()
    // 取得共享 executor 并建立 strand。executor 不可得/无效时返回
    // RuntimeUnavailable——infra 不接触原始 io_context，无法自治兜底。
    result<void> Start();

    // 仅 Start 成功后有效；之前调用是编程错误（工厂已按 running 态门控）。
    const IoStrand& Io() const noexcept { return *m_io; }

    // 所有「发起型」投递（请求/中止/关闭派发）必须经此入口：
    // m_post_mtx 内先查停止标志再入队，保证 SealOnIoDomain 之后
    // 不会再有新 handler 落进已封队列。返回 false 表示 io 域未启动、
    // 已封，或 post 自身分配失败——调用方按「后端已回收」处理
    // （RequestClose 链路只在同时确认 Stopped() 后才直接落定 Closed）。
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

    // 只能在 io 域内调用（runtime teardown 链路）：封死 TryPost，
    // 此后新投递一律拒绝。本方法不承诺任何已受理 handler 已落定——
    // 物理清理完成由各子对象的 in-flight 计数归零上报，不在此判断。
    void SealOnIoDomain() noexcept;

    bool Started() const noexcept {
        std::lock_guard<std::mutex> lk(m_post_mtx);
        return m_io.has_value();
    }
    bool Stopped() const noexcept {
        std::lock_guard<std::mutex> lk(m_post_mtx);
        return m_stopped;
    }

private:
    NetworkLimits              m_limits;
    std::optional<IoStrand>    m_io;                 // Start 建立
    mutable std::mutex         m_post_mtx;
    bool                       m_stopped{false};     // m_post_mtx 保护
};

} // namespace bbt::infra::http_detail
