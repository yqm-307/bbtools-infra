#pragma once
// MCP stdio transport 公共接入面（3-1 切片）。
//
// 契约来源：WFER mcp-server-foundation（specs R8/S16-S17、design「核心与transport」
// 「执行域和关闭」）。帧协议为逐行 UTF-8 完整 JSON（'\n' 分隔）；协议帧只写 stdout，
// 诊断只写 stderr（R8 分离）。I/O 复用共享协程运行时：posix descriptor 仅做就绪等待，
// 读写 syscall 与载荷缓冲全部自有，不借给 Asio；不新增任何 I/O 线程。
//
// 生命周期（design「核心与transport」）：Start 要求 server 已 Start 且共享协程运行时
// 已启动；transport 持 server 强引用，core 对 transport 只经会话 sink 的弱引用（无环）。
// Close() 同步物理收口自有资源（挂起等待者、有界输出队列、协程派发、O_NONBLOCK 置位），
// 任意线程幂等/并发 Close 均等首次真实落定；不归属也不关闭 fd（默认 0/1 属进程），
// 不触碰 server core 生命周期（core.Close 由业务决定）。stdin EOF 视为通道结束，
// transport 自行走同一收口序列（S16）。
//
// 有界输出（R8）：挂起队列按字节上限；写协程排空受 stdout 背压约束，分派侧出队等待
// 有 deadline（超限丢弃并计数 + stderr 诊断），非协程上下文调用方（sink）从不阻塞。
//
// 本切片未实现、故不在此宣告：每请求 deadline/取消栈外状态（3-3）、HTTP/SSE（3-2）、
// 官方 SDK 互通（4-2/S23）。

#include <chrono>
#include <cstddef>
#include <memory>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/McpServer.hpp>

namespace bbt::infra::mcp {

struct StdioOptions {
    // stdin/stdout fd（默认标准输入/输出）。必须是本 server 独占的**非 socket pipe**
    // （FIFO；Start 校验 S_ISFIFO）；transport 不归属 fd，Close 也不关闭它。
    // Start 置 O_NONBLOCK（保存原值）、Close 还原——dup 共享 OFD 的进程级影响由
    // 「独占 pipe」前提吸收（design「执行域和关闭」）。读写 syscall 直连内核
    // （syscall(SYS_read/write)，不经运行时 hook），就绪等待复用共享协程运行时。
    int stdin_fd = 0;
    int stdout_fd = 1;
    // 有界输出：挂起队列字节上限（>0）。
    std::size_t max_pending_bytes = 4U << 20;
    // 单行装配上限；超限保留前缀截断喂 core → 标准 -32700（内存有界，不伪造协议帧）。
    std::size_t max_line_bytes = (1U << 20) + 1;
    // 背压下出队等待上限（有界挂起，超限丢弃 + 计数 + stderr 诊断）。
    std::chrono::milliseconds write_deadline{5000};
    // fd/队列就绪等待的兜底轮询粒度（封口唤醒之外的兜底，保证 Close 有界落定）。
    std::chrono::milliseconds wait_granularity{250};
    // Close 的有界排空/汇合上限（超时放弃等待并继续物理释放，与仓内 Close 排空口径一致）。
    std::chrono::milliseconds close_drain_timeout{2000};
};

// Close 真实落定当刻的资源状态快照（S21「返回当刻原生资源」在本切片的 stdio 形态）。
// 快照在收口末尾（fd 标志还原、队列封口、协程已汇合/确认退出之后）一次性拷贝，
// LastCloseEvidence() 返回的即该当刻值，不是事后间接描述。
struct StdioCloseEvidence {
    bool reader_joined = false;      // 读/分派协程已退出
    bool writer_joined = false;      // 写协程已退出
    bool input_sealed = false;       // 入队/读路径已封
    bool output_sealed = false;      // 出队/写路径已封
    bool sink_mounted = false;       // 会话通知 sink 卸载后应为 false（丢帧行为已封）
    bool waiters_empty = false;      // 在途挂起等待者登记为空（无悬挂 epoll 兴趣）
    bool eof = false;                // stdin 读到 EOF
    bool write_failed = false;       // stdout 写错误
    bool core_closed = false;        // 分派观察到 core Closed（core 由业务 Close）
    bool nonblock_restored = false;  // O_NONBLOCK 已还原为进入前标志
    std::size_t pending_frames = 0;  // 当刻残留挂起帧（封队清除前快照为排空后残量）
    std::size_t pending_bytes = 0;
    std::size_t dropped_frames = 0;    // 满队/封队拒绝的出队帧（stderr 有诊断）
    std::size_t abandoned_frames = 0;  // 封口放弃的未发帧：封口时在途被弃帧 +
                                       // 封队清除的队列残帧（响应/通知各计入此处）
    std::size_t peak_pending_bytes = 0; // 挂起队列峰值（S16 背压有界证据）
};

class McpStdioTransport {
public:
    // 要求 server 已 Start、共享协程运行时已启动、fd 为独占 FIFO 且选项合法；
    // 失败不留半启动 transport。会话通知 sink（订阅 ack/事件/流结束帧）在本调用挂载，
    // 无 sink 时的丢帧行为不再是本包 stdio 路径的默认形态。
    static result<std::shared_ptr<McpStdioTransport>> Start(
        std::shared_ptr<McpServer> server, StdioOptions options = {});
    ~McpStdioTransport();

    // 同步物理收口：停读 → 会话订阅流结束帧 → 有界排空 → 封队 → 协程汇合 →
    // O_NONBLOCK 还原 → 快照。并发/重复 Close 均等首次真实落定；EOF 自收口与
    // 显式 Close 走同一序列（EOF 侧不做阻塞等待，由写协程退出完成收尾）。
    void Close() noexcept;
    bool IsClosed() const noexcept;

    // 有界输出观测（S16 背压证据面）。
    std::size_t PendingOutputBytes() const noexcept;
    std::size_t DroppedOutputFrames() const noexcept;
    // 线程契约：任意线程可调用，与 IsClosed() 同一锁域；返回收口当刻快照的值
    // 拷贝（非内部引用），与并发 Close 落定无数据竞争。Close 尚未落定时返回
    // 默认构造（全 false/0）的证据。
    StdioCloseEvidence LastCloseEvidence() const noexcept;

private:
    struct Impl;
    std::shared_ptr<Impl> m_impl;
    explicit McpStdioTransport(std::shared_ptr<Impl> impl) noexcept;
};

} // namespace bbt::infra::mcp
