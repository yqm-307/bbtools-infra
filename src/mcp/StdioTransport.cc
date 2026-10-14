// MCP stdio transport（3-1 切片）：共享运行时就绪等待的非阻塞 pipe 读写、
// 有界输出、受管协程 dispatch、stdio 取消/EOF/诊断 stderr；把 2-5 的会话通知
// sink seam 接到真实 stdio 出帧（订阅 ack/事件/流结束帧）。
//
// 形态契约：design「核心与transport」「执行域和关闭」+ specs R8/S16-S17。
//   - 不新增 I/O 线程、无独立 io_context：读/写各为一个受管协程任务
//     （Scheduler::RegistCoroutineTask）。协程运行时对本进程 hook 了
//     read/write：协程内的 ::read/::write 在 fd 未就绪时经共享 executor
//     （CoPoller/共享 io 域）挂起协程而非阻塞线程——这就是「仅等待就绪、
//     payload 不借给 Asio」在本运行时的实现形态；载荷缓冲全部自有。
//   - 协议帧只写 stdout（逐行 '\n' 分隔），诊断只写 stderr（R8 分离）。
//   - 有界输出：挂起队列字节上限；背压下分派侧出队等待有 deadline，超限丢弃
//     并计数 + stderr 诊断；非协程上下文的 sink 调用方（业务线程 Publish）
//     从不阻塞。
//   - EOF：stdin 读到 0 → 走与显式 Close 同一收口序列（停读→订阅流结束→有界
//     排空→封队→协程汇合→当刻快照）；协程侧不做阻塞等待，收尾由最后退出的
//     协程完成，显式 Close() 兜底汇合（S16/S21 stdio 形态）。
//   - 取消：核心 notifications/cancelled 命中订阅（含流结束帧出队）；Close
//     竞争时封队后不再发送任何被取消请求的帧；晚到 cancel 在表内无条目、
//     静默无害。
//   - core.Closed：分派观察到核心封口（业务 Close server）→ transport 自行
//     收口，不反噬 core（design：transport.Close 只取消自身 requests，不关
//     core）。
//   - Close 对挂起在 Hook_Read 上的读协程用「自唤醒字节」收口（写端可能已
//     关闭，写字节前按线程屏蔽 SIGPIPE，EPIPE 容忍）。

#include <bbt/infra/mcp/McpStdioTransport.hpp>

#include "mcp/Server.hpp"

#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <condition_variable>
#include <cstdio>
#include <deque>
#include <mutex>
#include <pthread.h>
#include <string>
#include <utility>
#include <vector>

#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

namespace bbt::infra::mcp {
namespace {

using bbt::coroutine::sync::CoWaiter;
using bbt::coroutine::sync::CombinedWaitOptions;
using bbt::coroutine::sync::CombinedWaitStatus;
using bbt::coroutine::Deadline;  // WaitTypes.hpp：Deadline 在 bbt::coroutine（sync 之外）

constexpr std::size_t kReadChunkBytes = 64U << 10;  // 读缓冲（堆上自有，不借 Asio）
constexpr int kStderrDropDiagLimit = 10;            // 满队丢弃诊断条数上限

Deadline DeadlineAfter(std::chrono::milliseconds delta) noexcept {
    return std::chrono::steady_clock::now() + delta;
}

} // namespace

struct McpStdioTransport::Impl {
    Impl(std::shared_ptr<McpServer> server, StdioOptions options)
        : m_server(std::move(server)), m_options(options) {}

    // ---- 状态 -------------------------------------------------------------------
    std::shared_ptr<McpServer> m_server;      // transport 持 core 强引用
    std::shared_ptr<McpSession> m_session;    // 单通道会话（stdio = 单 channel）
    StdioOptions m_options;
    int m_in_fd = 0;
    int m_out_fd = 1;
    int m_saved_in_flags = -1;                // Start 置 O_NONBLOCK 前的标志（Close 还原）
    int m_saved_out_flags = -1;

    mutable std::mutex m_mtx;                 // 队列/标志域（锁序：m_mtx → m_life_mtx）
    std::deque<std::string> m_queue;          // 已含 '\n' 结尾的完整帧
    std::size_t m_queue_bytes = 0;
    std::size_t m_peak_queue_bytes = 0;
    std::size_t m_dropped_frames = 0;
    std::size_t m_abandoned_frames = 0;
    int m_drop_diag_sent = 0;
    bool m_input_sealed = false;              // 停读 + 写协程排空后退出
    bool m_output_sealed = false;             // 拒绝一切入队
    bool m_eof = false;
    bool m_read_failed = false;
    bool m_write_failed = false;
    bool m_core_closed = false;
    bool m_reader_exited = false;
    bool m_writer_exited = false;

    // 协程内空队/满队/fd 就绪的信号等待（Deadline 兜底 + Notify 唤醒）。
    std::vector<CoWaiter::SPtr> m_fd_waiters;    // fd 就绪等待（封口唤醒）
    std::vector<CoWaiter::SPtr> m_space_waiters; // 满队出队等待（写进度/封口唤醒）
    std::vector<CoWaiter::SPtr> m_data_waiters;  // 空队写等待（入队/封口唤醒）

    std::condition_variable m_host_cv;        // host 线程等待协程退出（Close 汇合）
    std::condition_variable m_life_cv;        // host 线程等待 closed_done（首次收口落定）
    std::mutex m_life_mtx;                    // 收口生命周期域
    bool m_teardown_started = false;
    bool m_closed_done = false;
    StdioCloseEvidence m_evidence;            // Close 落定当刻快照

    // ---- 队列信号等待（纯信号 + Deadline 兜底）--------------------------------------
    // Completed=被唤醒 / TimedOut=预算到 / Cancelled=封口 /
    // InvalidContext=非协程上下文（调用方据此立即丢弃）。
    CombinedWaitStatus WaitSpace(std::chrono::milliseconds budget) {
        auto waiter = CoWaiter::Create();
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_output_sealed) return CombinedWaitStatus::Cancelled;
            m_space_waiters.push_back(waiter);
        }
        CombinedWaitOptions wait;
        wait.deadline = DeadlineAfter(budget);
        const auto status = waiter->Wait(wait);
        ForgetWaiter(m_space_waiters, waiter);
        return status;
    }

    CombinedWaitStatus WaitData() {
        auto waiter = CoWaiter::Create();
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_output_sealed || m_input_sealed) return CombinedWaitStatus::Cancelled;
            m_data_waiters.push_back(waiter);
        }
        CombinedWaitOptions wait;
        wait.deadline = DeadlineAfter(m_options.wait_granularity);
        const auto status = waiter->Wait(wait);
        ForgetWaiter(m_data_waiters, waiter);
        return status;
    }

    void ForgetWaiter(std::vector<CoWaiter::SPtr>& registry,
                      const CoWaiter::SPtr& waiter) {
        std::lock_guard<std::mutex> lock(m_mtx);
        for (auto it = registry.begin(); it != registry.end(); ++it) {
            if (*it == waiter) {
                registry.erase(it);
                return;
            }
        }
    }

    // 锁内 swap、锁外 Notify（PENDING 路径不丢唤醒；晚到 Notify 无副作用）。
    void Wake(std::vector<CoWaiter::SPtr>& registry) {
        std::vector<CoWaiter::SPtr> woken;
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            woken.swap(registry);
        }
        for (auto& waiter : woken) waiter->Notify();
    }
    void WakeSpaceWaiters() { Wake(m_space_waiters); }
    void WakeDataWaiters() { Wake(m_data_waiters); }

    void NoteDropLocked(const char* reason) {
        ++m_dropped_frames;
        if (m_drop_diag_sent++ < kStderrDropDiagLimit)
            std::fprintf(stderr, "bbt.mcp.stdio: output frame dropped (%s)\n", reason);
    }

    // fd 就绪等待（登记先于 syscall 的 EAGAIN：数据边沿不会丢失）；deadline 为
    // 封口兜底轮询粒度，封口唤醒经 m_fd_waiters 的 PENDING Notify。
    CombinedWaitStatus WaitFd(int fd, bool writeable) {
        auto waiter = CoWaiter::Create();
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_input_sealed || m_output_sealed) return CombinedWaitStatus::Cancelled;
            m_fd_waiters.push_back(waiter);
        }
        CombinedWaitOptions wait;
        wait.fd = fd;
        wait.want_readable = !writeable;
        wait.want_writeable = writeable;
        wait.deadline = DeadlineAfter(m_options.wait_granularity);
        const auto status = waiter->Wait(wait);
        ForgetWaiter(m_fd_waiters, waiter);
        return status;
    }
    void WakeFdWaiters() { Wake(m_fd_waiters); }

    // ---- 有界入队 -----------------------------------------------------------------
    // 满队时：协程上下文内以 write_deadline 为预算等待排空，超限丢弃；
    // 非协程上下文（业务线程经 sink 等）：Wait 拒绝挂起（InvalidContext）→
    // 立即丢弃 + 计数 + stderr 诊断，绝不阻塞调用方。
    bool Enqueue(std::string frame) {
        const auto deadline = DeadlineAfter(m_options.write_deadline);
        for (;;) {
            bool pushed = false;
            bool need_wake_data = false;
            {
                std::lock_guard<std::mutex> lock(m_mtx);
                if (m_output_sealed) {
                    ++m_abandoned_frames;
                    return false;
                }
                if (m_queue_bytes + frame.size() <= m_options.max_pending_bytes) {
                    m_queue_bytes += frame.size();
                    if (m_queue_bytes > m_peak_queue_bytes)
                        m_peak_queue_bytes = m_queue_bytes;
                    m_queue.push_back(std::move(frame));
                    pushed = true;
                    need_wake_data = !m_data_waiters.empty();
                }
            }
            // Notify 一律在锁外（Wake 内部取 m_mtx，锁内调用即自死锁）。
            if (pushed) {
                if (need_wake_data) WakeDataWaiters();
                return true;
            }
            auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                deadline - std::chrono::steady_clock::now());
            if (remaining <= std::chrono::milliseconds{0}) {
                std::lock_guard<std::mutex> lock(m_mtx);
                if (m_output_sealed) {
                    ++m_abandoned_frames;
                    return false;
                }
                NoteDropLocked("write deadline exceeded");
                return false;
            }
            const auto status = WaitSpace(remaining);
            if (status == CombinedWaitStatus::InvalidContext) {
                std::lock_guard<std::mutex> lock(m_mtx);
                NoteDropLocked("no coroutine context");
                return false;
            }
            // Completed/Cancelled/TimedOut 都回到循环复查。
        }
    }

    // ---- 分派（受管协程内串行）------------------------------------------------------
    // 返回 false = 分派面结束（EOF/读错/核心 Closed），读循环应退出并收口。
    bool DispatchLine(const std::string& raw, bool oversized) {
        std::size_t begin = 0, end = raw.size();
        while (begin < end &&
               (raw[begin] == ' ' || raw[begin] == '\t' || raw[begin] == '\r'))
            ++begin;
        while (end > begin &&
               (raw[end - 1] == ' ' || raw[end - 1] == '\t' || raw[end - 1] == '\r'))
            --end;
        if (begin == end && !oversized) return true; // 空白行不是帧（含自唤醒字节）

        auto outcome = m_session->HandleFrame(
            oversized ? std::string_view(raw).substr(0, m_options.max_line_bytes)
                      : std::string_view(raw).substr(begin, end - begin));
        if (outcome) {
            if (outcome.value().has_response) Enqueue(outcome.value().response + "\n");
            return true;
        }
        // 核心拒绝（未 Start/超在途等）：wire 映射为库一致的错误帧（id 未知 → null；
        // 不新增自定义码）。核心 Closed → 通道结束，transport 自行收口。
        const auto& error = outcome.error();
        if (error.code == ErrorCode::Closed) {
            std::lock_guard<std::mutex> lock(m_mtx);
            m_core_closed = true;
            return false;
        }
        const auto failure = ToProtocolFailure(error);
        Enqueue(MakeErrorResponse(std::nullopt, failure).Serialize() + "\n");
        return true;
    }

    // ---- 读/分派循环 ------------------------------------------------------------------
    // ::read 被运行时 hook 全局替换（Hook_Read 会在协程内自行挂起、无视本层的
    // 封口协作），故这里用 syscall(SYS_read) 直连内核：O_NONBLOCK + EAGAIN →
    // CoWaiter fd 兴趣等待（就绪等待复用共享运行时，挂起协程而非线程）。
    void ReaderLoop() {
        std::string line;
        line.reserve(4096);
        bool oversized = false;
        bool dispatch_closed = false;
        // 读缓冲放堆上：协程栈由宿主调度器配置（pin 默认仅 12KiB），库不在协程栈
        // 上放大块缓冲。
        std::vector<char> buffer(kReadChunkBytes);
        while (!dispatch_closed) {
            {
                std::lock_guard<std::mutex> lock(m_mtx);
                if (m_input_sealed) break;
            }
            const ssize_t n = ::syscall(SYS_read, m_in_fd, buffer.data(), buffer.size());
            if (n > 0) {
                for (ssize_t i = 0; i < n; ++i) {
                    const char c = buffer.data()[i];
                    if (c == '\n') {
                        if (!DispatchLine(line, oversized)) {
                            dispatch_closed = true;
                            break;
                        }
                        line.clear();
                        oversized = false;
                    } else if (line.size() < m_options.max_line_bytes) {
                        line.push_back(c);
                    } else {
                        oversized = true; // 超限截断：保留前缀喂 core → 标准 -32700
                    }
                }
                continue;
            }
            if (n == 0) {
                std::lock_guard<std::mutex> lock(m_mtx);
                m_eof = true;
                break;
            }
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                WaitFd(m_in_fd, /*writeable=*/false);
                continue; // 复查 sealed 后再试 read（登记先于 EAGAIN，无边丢失）
            }
            {
                std::lock_guard<std::mutex> lock(m_mtx);
                m_read_failed = true;
            }
            std::fprintf(stderr, "bbt.mcp.stdio: stdin read error (errno=%d)\n", errno);
            break;
        }
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            m_reader_exited = true;
        }
        m_host_cv.notify_all();
        // EOF/读错/核心 Closed 自收口：与显式 Close 同序列；协程侧不做阻塞等待，
        // 排空与收尾由写协程退出完成（TryFinishTeardown）。
        BeginTeardown();
        TryFinishTeardown(/*blocking=*/false);
    }

    // ---- 写循环（Hook_Write 挂起等待可写；有界排空）------------------------------------
    void WriterLoop() {
        for (;;) {
            std::string frame;
            bool exit_now = false;
            {
                std::lock_guard<std::mutex> lock(m_mtx);
                if (!m_queue.empty()) {
                    frame = std::move(m_queue.front());
                    m_queue.pop_front();
                    m_queue_bytes -= frame.size();
                } else if (m_output_sealed || m_input_sealed) {
                    exit_now = true; // 无新帧可来（EOF/封队）且队列已排空
                }
            }
            if (exit_now) break;
            if (frame.empty()) {
                WaitData();
                continue;
            }
            std::size_t offset = 0;
            bool failed = false;
            // SIGPIPE 按线程屏蔽：写端读侧已关时 syscall 得到 EPIPE 而非进程终止。
            sigset_t block, old;
            ::sigemptyset(&block);
            ::sigaddset(&block, SIGPIPE);
            const bool mask_ok = ::pthread_sigmask(SIG_BLOCK, &block, &old) == 0;
            while (offset < frame.size()) {
                const ssize_t n = ::syscall(SYS_write, m_out_fd, frame.data() + offset,
                                            frame.size() - offset);
                if (n > 0) {
                    offset += static_cast<std::size_t>(n);
                    if (offset >= frame.size()) WakeSpaceWaiters(); // 有空位（锁外）
                    continue;
                }
                if (n < 0 && errno == EINTR) continue;
                if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                    if (WaitFd(m_out_fd, /*writeable=*/true) == CombinedWaitStatus::Cancelled) {
                        // 封口放弃在途帧计入 abandoned（与写错误路径、封队清队
                        // 同口径：封口时被放弃的未发输出帧都算，不只在队列内的残帧）。
                        std::lock_guard<std::mutex> lock(m_mtx);
                        ++m_abandoned_frames;
                        break;
                    }
                    continue;
                }
                {
                    std::lock_guard<std::mutex> lock(m_mtx);
                    m_write_failed = true;
                    ++m_abandoned_frames; // 当前帧无法完成
                }
                std::fprintf(stderr, "bbt.mcp.stdio: stdout write error (errno=%d)\n",
                             errno);
                failed = true;
                break;
            }
            if (mask_ok) ::pthread_sigmask(SIG_SETMASK, &old, nullptr);
            if (failed) break;
        }
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            m_writer_exited = true;
        }
        m_host_cv.notify_all();
        TryFinishTeardown(/*blocking=*/false);
    }

    // ---- 收口（显式 Close 与 EOF 自收口共用）------------------------------------------------
    // 第一步：停读 + 订阅流结束帧 + 唤醒读协程。返回 false 表示已有收口在进行。
    bool BeginTeardown() {
        {
            std::lock_guard<std::mutex> life_lock(m_life_mtx);
            if (m_teardown_started) return false;
            m_teardown_started = true;
        }
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            m_input_sealed = true;
        }
        WakeDataWaiters();
        WakeSpaceWaiters();
        WakeFdWaiters();
        // 订阅流结束帧经 sink 入队；满队行为按调用上下文分口径：host Close 路径
        // （非协程上下文）不等待、满队即丢并计数；EOF 自收口在读协程上下文内，
        // 满队按 write_deadline 预算等待排空（封口唤醒即提前返回，不会悬置——
        // 此刻客户端已不是可依赖的读方，等待只为顺序投递）。条目移除后核心订阅
        // 表不再引用本会话（Server.hpp C1 契约：transport 须先结束订阅再析构会话）。
        if (m_session) m_session->EndAllSubscriptions();
        return true;
    }

    // 第二步：封队 + 卸载 sink + 当刻快照。blocking=true（host 线程）先有界等待
    // 协程汇合；协程侧不做阻塞等待，只由「双协程均已退出」时收尾。
    void TryFinishTeardown(bool blocking) {
        {
            std::lock_guard<std::mutex> life_lock(m_life_mtx);
            if (m_closed_done) return; // 首次收口已落定（并发/重复调用幂等）
        }
        if (blocking) {
            const auto deadline = DeadlineAfter(m_options.close_drain_timeout);
            std::unique_lock<std::mutex> lock(m_mtx);
            m_host_cv.wait_until(lock, deadline,
                                 [this] { return m_reader_exited && m_writer_exited; });
        } else {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (!(m_reader_exited && m_writer_exited))
                return; // 收尾留给最后退出的协程 / Close 的阻塞路径
        }

        {
            std::lock_guard<std::mutex> lock(m_mtx);
            m_output_sealed = true;
            m_abandoned_frames += m_queue.size();
            m_queue.clear();
            m_queue_bytes = 0;
        }
        WakeDataWaiters();
        WakeSpaceWaiters();
        if (m_session) m_session->SetNotificationSink(McpSession::NotificationSink{});

        // O_NONBLOCK 还原（协程已退出/确认退出，fd 已无在途使用者）。
        bool nonblock_restored = false;
        if (m_saved_in_flags >= 0 && m_saved_out_flags >= 0) {
            const int r = ::fcntl(m_in_fd, F_SETFL, m_saved_in_flags);
            const int w = ::fcntl(m_out_fd, F_SETFL, m_saved_out_flags);
            nonblock_restored = (r == 0 && w == 0);
        }

        // 当刻快照（S21 stdio 形态：收口末尾一次性拷贝，不是事后间接描述）。
        McpSession::NotificationSink sink_probe;
        const bool sink_mounted =
            m_session ? m_session->CopyNotificationSink(sink_probe) : false;
        StdioCloseEvidence snapshot;
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            snapshot.reader_joined = m_reader_exited;
            snapshot.writer_joined = m_writer_exited;
            snapshot.input_sealed = m_input_sealed;
            snapshot.output_sealed = m_output_sealed;
            snapshot.sink_mounted = sink_mounted;
            snapshot.waiters_empty = m_fd_waiters.empty() && m_space_waiters.empty() &&
                                     m_data_waiters.empty();
            snapshot.eof = m_eof;
            snapshot.write_failed = m_write_failed;
            snapshot.core_closed = m_core_closed;
            snapshot.nonblock_restored = nonblock_restored;
            snapshot.pending_frames = m_queue.size();
            snapshot.pending_bytes = m_queue_bytes;
            snapshot.dropped_frames = m_dropped_frames;
            snapshot.abandoned_frames = m_abandoned_frames;
            snapshot.peak_pending_bytes = m_peak_queue_bytes;
        }
        {
            std::lock_guard<std::mutex> life_lock(m_life_mtx);
            if (!m_closed_done) {
                m_evidence = snapshot;
                m_closed_done = true;
                m_life_cv.notify_all();
            }
        }
    }

    // Close（host 线程路径）：并发/重复 Close 均等首次真实落定。
    void CloseAndWait() noexcept {
        if (BeginTeardown()) TryFinishTeardown(/*blocking=*/true);
        std::unique_lock<std::mutex> lock(m_life_mtx);
        m_life_cv.wait(lock, [this] { return m_closed_done; });
    }
};

// ---- 公共 API ---------------------------------------------------------------------------

result<std::shared_ptr<McpStdioTransport>> McpStdioTransport::Start(
    std::shared_ptr<McpServer> server, StdioOptions options) {
    if (!server)
        return result<std::shared_ptr<McpStdioTransport>>::err(
            MakeError(ErrorCode::InvalidArgument, "mcp.stdio.null_server"));
    if (!server->IsStarted())
        return result<std::shared_ptr<McpStdioTransport>>::err(
            MakeError(ErrorCode::InvalidContext, "mcp.stdio.server_not_started"));
    if (options.stdin_fd < 0 || options.stdout_fd < 0)
        return result<std::shared_ptr<McpStdioTransport>>::err(
            MakeError(ErrorCode::InvalidArgument, "mcp.stdio.bad_fd"));
    if (options.max_pending_bytes == 0 || options.max_line_bytes == 0 ||
        options.write_deadline <= std::chrono::milliseconds{0} ||
        options.wait_granularity <= std::chrono::milliseconds{0} ||
        options.close_drain_timeout <= std::chrono::milliseconds{0})
        return result<std::shared_ptr<McpStdioTransport>>::err(
            MakeError(ErrorCode::InvalidArgument, "mcp.stdio.bad_options"));
    if (!bbt::coroutine::detail::Scheduler::GetInstance()->IsInitialized())
        return result<std::shared_ptr<McpStdioTransport>>::err(
            MakeError(ErrorCode::RuntimeUnavailable, "mcp.stdio.runtime_not_running"));

    // 独占非 socket pipe 前提校验（R8；Start 拒绝非 FIFO）。
    struct stat in_st {};
    struct stat out_st {};
    if (::fstat(options.stdin_fd, &in_st) != 0 || ::fstat(options.stdout_fd, &out_st) != 0 ||
        !S_ISFIFO(in_st.st_mode) || !S_ISFIFO(out_st.st_mode))
        return result<std::shared_ptr<McpStdioTransport>>::err(
            MakeError(ErrorCode::InvalidArgument, "mcp.stdio.not_exclusive_pipe"));

    auto impl = std::make_shared<Impl>(std::move(server), options);
    impl->m_in_fd = options.stdin_fd;
    impl->m_out_fd = options.stdout_fd;

    // O_NONBLOCK 置位（保存原值供 Close 还原；dup 共享 OFD 的进程级影响由「独占
    // pipe」前提吸收，design「执行域和关闭」）。syscall(SYS_read/write) 直连内核，
    // 不经运行时 hook，故非阻塞语义在本层成立。
    impl->m_saved_in_flags = ::fcntl(impl->m_in_fd, F_GETFL);
    impl->m_saved_out_flags = ::fcntl(impl->m_out_fd, F_GETFL);
    if (impl->m_saved_in_flags < 0 || impl->m_saved_out_flags < 0)
        return result<std::shared_ptr<McpStdioTransport>>::err(
            MakeError(ErrorCode::InternalError, "mcp.stdio.fcntl_failed"));
    if (::fcntl(impl->m_in_fd, F_SETFL, impl->m_saved_in_flags | O_NONBLOCK) != 0 ||
        ::fcntl(impl->m_out_fd, F_SETFL, impl->m_saved_out_flags | O_NONBLOCK) != 0)
        return result<std::shared_ptr<McpStdioTransport>>::err(
            MakeError(ErrorCode::InternalError, "mcp.stdio.nonblock_failed"));

    // 会话 + 通知 sink（弱引用：core→transport 无环；transport 亡 → sink 变无操作）。
    impl->m_session = CreateSession(impl->m_server);
    std::weak_ptr<McpStdioTransport::Impl> weak = impl;
    impl->m_session->SetNotificationSink([weak](const std::string& frame) {
        auto self = weak.lock();
        if (!self) return;
        self->Enqueue(frame + "\n");
    });

    // 受管协程 dispatch：读/写各一协程任务（共享 scheduler 线程，无新增 I/O 线程）。
    bool reader_started = false;
    bool writer_started = false;
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [impl] { impl->ReaderLoop(); }, reader_started);
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [impl] { impl->WriterLoop(); }, writer_started);
    if (!reader_started || !writer_started) {
        // 半启动清理：封两向（未启动协程按已退出记账），收尾路径与正常 Close 一致。
        {
            std::lock_guard<std::mutex> lock(impl->m_mtx);
            impl->m_input_sealed = true;
            if (!reader_started) impl->m_reader_exited = true;
            if (!writer_started) impl->m_writer_exited = true;
        }
        impl->WakeDataWaiters();
        impl->TryFinishTeardown(/*blocking=*/true);
        return result<std::shared_ptr<McpStdioTransport>>::err(
            MakeError(ErrorCode::RuntimeUnavailable, "mcp.stdio.coroutine_start_failed"));
    }

    return result<std::shared_ptr<McpStdioTransport>>::ok(
        std::shared_ptr<McpStdioTransport>(new McpStdioTransport(std::move(impl))));
}

McpStdioTransport::McpStdioTransport(std::shared_ptr<Impl> impl) noexcept
    : m_impl(std::move(impl)) {}

McpStdioTransport::~McpStdioTransport() {
    if (m_impl) m_impl->CloseAndWait();
}

void McpStdioTransport::Close() noexcept {
    if (m_impl) m_impl->CloseAndWait();
}

bool McpStdioTransport::IsClosed() const noexcept {
    std::lock_guard<std::mutex> lock(m_impl->m_life_mtx);
    return m_impl->m_closed_done;
}

std::size_t McpStdioTransport::PendingOutputBytes() const noexcept {
    std::lock_guard<std::mutex> lock(m_impl->m_mtx);
    return m_impl->m_queue_bytes;
}

std::size_t McpStdioTransport::DroppedOutputFrames() const noexcept {
    std::lock_guard<std::mutex> lock(m_impl->m_mtx);
    return m_impl->m_dropped_frames;
}

StdioCloseEvidence McpStdioTransport::LastCloseEvidence() const noexcept {
    // 与 IsClosed() 同一锁域取值快照：m_evidence 的写入持 m_life_mtx
    // （TryFinishTeardown），无锁返回引用会与并发 Close 构成 data race。
    std::lock_guard<std::mutex> lock(m_impl->m_life_mtx);
    return m_impl->m_evidence;
}

} // namespace bbt::infra::mcp
