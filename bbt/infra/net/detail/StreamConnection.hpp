#pragma once

// 私有 stream 连接 backend（P3-b 局部交付，detail 路径，不是公开 API）。
//
// 范围与依据：docs/architecture/gnet-network-execution.md §3–6 的私有连接
// 局部；0010 决策「同一张执行权/回调/视图表」「Close(opt)」中本包兑现的
// 部分：唯一 fd/owner、按预算读写、独立事件快照、有界背压、
// Open→Closing→Closed 物理收口。公开 Engine/Handler/ICoConnection/
// CoTcpConnection 是后续增量，不在本头文件。
//
// 执行域约定：
// - 所有 mutable fd/buffer/状态只在创建它的 owner（已 Start 的 EventLoop）
//   线程访问；公开方法（含 const 查询）先 RequireOwner，非 owner 误用在
//   任何副作用之前以 std::logic_error 拒绝（与 EventLoop::Context 同型）。
// - 全程无 OS 锁：单 owner 线程天然串行，不存在跨挂起持锁。
// - 回调是短 owner 基础设施通知：在 owner 线程同步调用、不运行用户业务
//   协程、不调用 detail Resume；它不是公开 EventHandler。
//
// fd 所有权：输入是已连接 NativeSocket 的唯一所有权。接纳前校验失败时
// fd 仍归 NativeSocket（RAII 原样退回，不静默丢弃）；assign 成功并
// Release 后唯一 fd 归本对象内部的 Asio socket，无第二个 fd、无双 close。
// Read/write 用成熟 Asio reactor 语义（async_wait + 非阻塞 read_some/
// write_some），不为 primitive 外形重复造 poller。

#include <algorithm>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include <boost/asio/ip/tcp.hpp>

#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/Options.hpp>
#include <bbt/infra/net/detail/EventLoop.hpp>
#include <bbt/infra/net/detail/EventRingBuffer.hpp>
#include <bbt/infra/net/detail/OutboundQueue.hpp>

namespace bbt::infra::net {

class StreamConnection : public std::enable_shared_from_this<StreamConnection> {
public:
    enum class State { Open, Closing, Closed };

    // 观测计数（owner-only 查询）：给契约测试与后续审查提供预算/出口证据。
    struct Stats {
        std::size_t read_arms{0};               // 读就绪等待登记次数
        std::size_t write_arms{0};              // 写就绪等待登记次数
        std::size_t wait_callbacks{0};          // 就绪回调进入次数
        std::size_t guard_exits{0};             // 关闭/撤销后的晚回调安全出口
        std::size_t zero_write_progress{0};     // 写就绪但 0 字节进展（readiness 门控重挂）
        std::size_t traffic_notifications{0};   // 新数据/EOF/IO错误通知
        std::size_t drained_notifications{0};   // 待发队列清空通知
        std::size_t writes_accepted{0};
        std::size_t snapshots_taken{0};
        std::size_t snapshots_returned{0};
        std::size_t snapshot_consumed_bytes{0};
        std::size_t read_bytes{0};
        std::size_t write_bytes_queued{0};
        std::size_t write_bytes_sent{0};
        std::size_t peak_pending_bytes{0};
        std::size_t physical_closes{0};         // 必须 ==1（单次物理收口）
    };

    // 短 owner 基础设施通知（非公开 EventHandler）：
    // - on_traffic：读泵新读入字节，或读到 EOF/IO 错误停止读泵；业务据此
    //   取快照。不消费且无新数据时不会反复派发。
    // - on_drained：待发队列清空——只代表已全部交给内核缓冲，
    //   不代表对端已收到。
    // - on_closed：物理关闭完成后发布（撤销 read/write、最后内核访问与
    //   唯一 fd 关闭之后）；此后晚回调不再触碰 fd。
    struct Callbacks {
        std::function<void(StreamConnection&)> on_traffic;
        std::function<void(StreamConnection&)> on_drained;
        std::function<void(StreamConnection&, const std::error_code&)> on_closed;
    };

    // owner-only 工厂。输入为已连接 NativeSocket 的唯一所有权：
    // - 接纳前校验失败（非 owner、fd 非法、assign 失败）：错误返回，
    //   fd 所有权原样留在 NativeSocket，由调用者 RAII 回收，不静默丢 fd。
    // - assign 成功后所有权转移给本对象；其后的初始化失败走本对象
    //   物理收口（Close），fd 不外泄也不再归 NativeSocket。
    // 返回 nullptr 时 error 说明原因。应用 socket 缓冲/TCP_NODELAY 等
    // Options 字段发生在转移前的 NativeSocket 上（advisory，失败不阻断）。
    static std::shared_ptr<StreamConnection> Create(EventLoop& owner,
        NativeSocket&& socket, const NormalizedOptions& options,
        Callbacks callbacks, std::error_code& error);

    ~StreamConnection();
    StreamConnection(const StreamConnection&) = delete;
    StreamConnection& operator=(const StreamConnection&) = delete;

    // 创建后不变，可任意线程读取（诊断用）。
    std::size_t Id() const noexcept { return id_; }
    EventLoop& Owner() const noexcept { return owner_; }

    // ---- 以下全部 owner-only（非 owner 先抛 std::logic_error）----

    // 有界待发：复制进 OutboundQueue 取得不可变所有权，不保存调用栈 view。
    // 超出 max_outbound_bytes 或段数预算时在任何修改前拒绝
    // （no_buffer_space），已入队内容不变。Closing/Closed 拒绝新写。
    // 写入接纳（本返回值）≠ 内核全发 ≠ 对端收到。
    std::error_code Write(std::string data);

    // 独立事件快照：把当前积压字节整段转出（独立 backing storage）。
    // 同连接最多一个未归还快照；借出期间读泵暂停。空积压/已有借出/
    // 非 Open 拒绝（resource_unavailable_try_again / operation_not_supported）。
    EventSnapshot TakeSnapshot(std::error_code& error);

    // 快照归还：consumed 为业务已消费字节数，须满足
    // consumed + snapshot.Remaining() == 借出量，否则拒绝且状态不变
    // （快照仍归业务）。未消费尾部在任何新字节之前回到积压并保序合并，
    // 半包/粘包不丢弃。consumed==0 不派发事件；归还后读泵按预算恢复。
    // Closing/Closed 仍可归还：只做清理，不恢复读泵（物理关闭不等快照）。
    std::error_code ReturnSnapshot(EventSnapshot&& snapshot, std::size_t consumed);

    // 物理关闭：单次 Open→Closing→Closed；重复/竞争调用幂等返回。
    // 本包待发策略为 discard（无 FlushPolicy 产品语义）。撤销 read/write
    // 登记、完成最后内核访问并关闭唯一 fd 之后才发布 on_closed。
    // Closing 后拒绝新写/新事件，晚回调不再触碰 fd、不能重新打开。
    std::error_code Close(std::error_code reason);

    State ConnectionState() const;              // owner-only
    const Stats& GetStats() const;              // owner-only
    std::size_t BufferedBytes() const;          // owner-only 积压读量
    std::size_t PendingBytes() const;           // owner-only 待发字节
    std::size_t PendingSegments() const;        // owner-only 待发段数
    bool SnapshotLent() const;                  // owner-only
    bool ReadWaiting() const;                   // owner-only 读就绪等待在途
    bool WriteWaiting() const;                  // owner-only 写就绪等待在途
    bool ReadEof() const;                       // owner-only 对端已关写侧
    std::error_code LastIoError() const;        // owner-only 最近致命 IO 错误

private:
    StreamConnection(EventLoop& owner, const NormalizedOptions& options,
        Callbacks callbacks);

    void        RequireOwner() const;
    std::size_t ReadRoom() const noexcept;      // 距 max_inbound_bytes 余量
    // 等待登记唯一共同入口：async_wait 登记分配失败（bad_alloc）在此统一
    // 捕获并返回 std 错误（ENOMEM）；armed 与登记计数只在真实登记成功后
    // 置位。调用者统一按返回错误走断裂出口（RecordBroken+terminal+一次
    // 通知），不再各自 try/catch 复制出口。
    [[nodiscard]] std::error_code ArmRead();
    [[nodiscard]] std::error_code ArmWrite();
    void        OnReadWait(const boost::system::error_code& error);
    void        OnWriteWait(const boost::system::error_code& error);
    void        NotifyTraffic();
    void        NotifyDrained();
    void        RecordBroken(std::error_code error);

    EventLoop&          owner_;
    const NormalizedOptions options_;
    Callbacks           callbacks_;
    boost::asio::ip::tcp::socket socket_;
    State               state_{State::Open};
    EventRingBuffer     inbound_;               // 上限 max_inbound_bytes
    OutboundQueue       outbound_;              // 上限 max_outbound_bytes/段数
    std::vector<char>   read_block_;            // 单次读预算块（≤ read cap）
    bool                read_armed_{false};
    bool                write_armed_{false};
    bool                lent_{false};           // 最多一个未归还快照
    std::size_t         lent_bytes_{0};
    bool                read_terminal_{false};  // EOF/错误后停读
    bool                write_terminal_{false}; // 致命写错误后停写泵
    bool                eof_{false};
    std::error_code     io_error_;
    std::error_code     close_reason_;
    std::size_t         id_{0};
    Stats               stats_;
};

} // namespace bbt::infra::net
