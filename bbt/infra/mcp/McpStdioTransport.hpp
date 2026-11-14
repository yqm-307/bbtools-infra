#pragma once

/**
 * @file McpStdioTransport.hpp
 * @brief MCP stdio transport 公共接入面。
 *
 * 帧协议为逐行 UTF-8 完整 JSON（'\n' 分隔）；协议帧只写 stdout，
 * 诊断只写 stderr。I/O 复用共享协程运行时：posix descriptor 仅做就绪等待，
 * 读写 syscall 与载荷缓冲全部自有，不借给 Asio；不新增任何 I/O 线程。
 *
 * 生命周期：Start 要求 server 已 Start 且共享协程运行时已启动；transport 持
 * server 强引用，core 对 transport 只经会话 sink 的弱引用（无环）。
 * Close 是唯一关闭操作。默认 Immediate 任意线程非等待封口，明确放弃未写帧，
 * 不等待 reader/writer；Graceful 等已接纳请求写终态与输出队列排空。
 * wait_release=true 在同一有限 deadline 内继续等 reader/writer 退出、sink 卸载和
 * O_NONBLOCK 还原。超时保留 Impl、fd 标志及在途责任，不作降级假回收。
 * 析构只发起非等待 Immediate；读写协程持 Impl 到真实退出并完成收尾。
 * 不归属也不关闭 fd（默认 0/1 属进程），不触碰 server core 生命周期（core.Close
 * 由业务决定）。stdin EOF 视为通道结束，transport 自行走同一收口序列。
 *
 * 有界输出：挂起队列按字节上限；写协程排空受 stdout 背压约束，分派侧出队等待
 * 有 deadline（超限丢弃并计数 + stderr 诊断），非协程上下文调用方（sink）从不
 * 阻塞。
 *
 * 当前不提供：每请求 deadline/取消栈外状态、HTTP/SSE、官方 SDK 互通。
 */
#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>

#include <bbt/infra/CloseOptions.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/McpServer.hpp>

namespace bbt::infra::mcp {

/**
 * @brief stdio transport 的 I/O 与有界队列配置。
 */
struct StdioOptions {

    /**
     * 独占读端 fd（须为 FIFO；默认标准输入）。
     *
     * stdin/stdout 都必须是本 server 独占的非 socket pipe（Start 校验 S_ISFIFO）；transport
     * 不归属 fd，Close 也不关闭它。Start 置 O_NONBLOCK（保存原值）、物理收尾还原——dup 共享
     * OFD 的进程级影响由「独占 pipe」前提吸收。读写 syscall 直连内核
     * （syscall(SYS_read/write)，不经运行时 hook），就绪等待复用共享协程运行时。
     */
    int stdin_fd = 0;

    /**
     * 独占写端 fd（须为 FIFO；默认标准输出）。
     */
    int stdout_fd = 1;

    /**
     * 有界输出：挂起队列字节上限（>0）。
     */
    std::size_t max_pending_bytes = 4U << 20;

    /**
     * 单行装配上限；超限保留前缀截断喂 core → 标准 -32700（内存有界，不伪造协议帧）。
     */
    std::size_t max_line_bytes = (1U << 20) + 1;

    /**
     * 背压下出队等待上限（有界挂起，超限丢弃 + 计数 + stderr 诊断）。
     */
    std::chrono::milliseconds write_deadline{5000};

    /**
     * fd/队列就绪等待的兜底轮询粒度（封口唤醒之外的兜底，保证 Close 有界落定）。
     */
    std::chrono::milliseconds wait_granularity{250};

    /**
     * Close 的有界排空/汇合上限（超时放弃等待并继续物理释放，与仓内 Close 排空口径一致）。
     */
    std::chrono::milliseconds close_drain_timeout{2000};
};

/**
 * @brief Close 真实落定当刻的资源状态快照。
 *
 * 快照在收口末尾（fd 标志还原、队列封口、协程已汇合/确认退出之后）一次性拷贝，
 * LastCloseEvidence() 返回的即该当刻值，不是事后间接描述。
 */
struct StdioCloseEvidence {

    /**
     * 读/分派协程已退出
     */
    bool reader_joined = false;

    /**
     * 写协程已退出
     */
    bool writer_joined = false;

    /**
     * 入队/读路径已封
     */
    bool input_sealed = false;

    /**
     * 出队/写路径已封
     */
    bool output_sealed = false;

    /**
     * 会话通知 sink 卸载后应为 false（丢帧行为已封）
     */
    bool sink_mounted = false;

    /**
     * 在途挂起等待者登记为空（无悬挂 epoll 兴趣）
     */
    bool waiters_empty = false;

    /**
     * stdin 读到 EOF
     */
    bool eof = false;

    /**
     * stdout 写错误
     */
    bool write_failed = false;

    /**
     * 分派观察到 core Closed（core 由业务 Close）
     */
    bool core_closed = false;

    /**
     * O_NONBLOCK 已还原为进入前标志
     */
    bool nonblock_restored = false;

    /**
     * 当刻残留挂起帧（封队清除前快照为排空后残量）
     */
    std::size_t pending_frames = 0;

    /**
     * 当刻残留挂起字节
     */
    std::size_t pending_bytes = 0;

    /**
     * 满队/封队拒绝的出队帧（stderr 有诊断）
     */
    std::size_t dropped_frames = 0;

    /**
     * 封口放弃的未发帧：封口时在途被弃帧 +
     * 封队清除的队列残帧（响应/通知各计入此处）
     */
    std::size_t abandoned_frames = 0;

    /**
     * 挂起队列峰值（背压有界证据）
     */
    std::size_t peak_pending_bytes = 0;
};

/**
 * @brief MCP stdio transport：逐行 JSON 帧的 stdin/stdout 通道。
 *
 * 持 server 强引用，core 对 transport 只经会话 sink 弱引用（无环）；不归属 fd，
 * 也不触碰 server core 生命周期。
 */
class McpStdioTransport {
public:
    /**
     * @brief 启动 transport。
     *
     * 要求 server 已 Start、共享协程运行时已启动、fd 为独占 FIFO 且选项合法；失败不留
     * 半启动 transport。会话通知 sink（订阅 ack/事件/流结束帧）在本调用挂载。
     * @param server 已 Start 的服务实例
     * @param options I/O fd 与有界队列配置
     * @return transport 或启动失败原因
     */
    static result<std::shared_ptr<McpStdioTransport>>
    Start(std::shared_ptr<McpServer> server, StdioOptions options = {});
    ~McpStdioTransport();

    /**
     * @brief 统一逻辑关闭入口：封输入并发布首因。
     *
     * Immediate 另封输出、放弃未发帧；Graceful 等到已接纳请求、队列和 writer 当前帧均到
     * 写终态或绝对期限，封读不取消写等待，到期保留责任。Co 域 50ms 分片（native 宿主域同一谓词的有界等待），不阻塞读/写协程推进。
     * @param options 模式、首因与优雅关闭的有限绝对期限
     */
    result<void> Close(const CloseOptions& options = {});

    /**
     * @brief 首个关闭原因；封口前为空，封口后固定。
     */
    std::optional<Error> CloseReason() const noexcept;

    /**
     * @brief 是否已逻辑封口（Close 生效）；不代表协程已退出或物理回收。
     */
    bool IsClosed() const noexcept;

    /**
     * @brief 有界输出观测：当前挂起字节数。
     */
    std::size_t PendingOutputBytes() const noexcept;

    /**
     * @brief 有界输出观测：满队/封队丢弃的帧数。
     */
    std::size_t DroppedOutputFrames() const noexcept;

    /**
     * @brief 收口当刻的资源状态快照拷贝；Close 尚未落定时为默认构造（全 false/0）。
     *
     * 线程契约：任意线程可调用，与 IsClosed() 同一锁域；返回值拷贝（非内部引用），与
     * 并发 Close 落定无数据竞争。
     */
    StdioCloseEvidence LastCloseEvidence() const noexcept;

private:
    friend struct StdioTestAccess;
    void _BeginClose(Error reason = MakeError(ErrorCode::Closed, "mcp.stdio: closed by owner")) noexcept;
    result<void> _WaitReleased(bbt::coroutine::Deadline deadline);

    struct Impl;
    std::shared_ptr<Impl> m_impl;
    explicit McpStdioTransport(std::shared_ptr<Impl> impl) noexcept;
};

// clang-format off
}
// clang-format on
