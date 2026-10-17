#include <bbt/infra/net/detail/StreamConnection.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <new>
#include <stdexcept>
#include <string_view>
#include <utility>

#include <boost/asio/buffer.hpp>
#include <boost/asio/error.hpp>

#include <sys/socket.h>

namespace bbt::infra::net {
namespace {

std::size_t NextConnectionId() {
    static std::atomic<std::size_t> counter{0}; // 进程内诊断序号，不对外承诺。
    return counter.fetch_add(1) + 1;
}

// 内核 errno 语义错误按值映射到 std::system_category（Linux 同为 errno 值）；
// asio 专有状态（eof/would_block/aborted）在各回调先行比较 boost 值处理。
std::error_code MapError(const boost::system::error_code& error) {
    if (!error) { return {}; }
    return std::error_code(error.value(), std::system_category());
}

// 单次 writev 的段视图上限：ring 至多 2 段 + 溢出段；一次发不完由 FIFO
// 续投覆盖，不追求单次全发。
constexpr std::size_t kWriteViews = 16;

} // namespace

StreamConnection::StreamConnection(EventLoop& owner, const NormalizedOptions& options,
    Callbacks callbacks)
    : owner_(owner),
      options_(options),
      callbacks_(std::move(callbacks)),
      socket_(owner.Context()),
      inbound_(options_.max_inbound_bytes),
      outbound_(options_.write_buffer_cap, options_.max_outbound_bytes,
          options_.max_pending_events),
      read_block_(std::min<std::size_t>(options_.read_buffer_cap,
          options_.max_inbound_bytes)) {
    id_ = NextConnectionId();
}

StreamConnection::~StreamConnection() {
    // 正常路径必须先在 owner 上 Close()；fd 兜底回收归 socket_ RAII。
    // 不以析构冒充关闭完成：state_ 不是 Closed 的析构属于契约违例。
}

std::shared_ptr<StreamConnection> StreamConnection::Create(EventLoop& owner,
    NativeSocket&& socket, const NormalizedOptions& options,
    Callbacks callbacks, std::error_code& error) {
    if (!owner.IsOwner()) {
        throw std::logic_error("StreamConnection::Create is owner-only");
    }
    // 接纳前校验：fd 非法直接失败，唯一所有权原样留在 NativeSocket。
    if (socket.Fd() < 0) {
        error = std::make_error_code(std::errc::invalid_argument);
        return nullptr;
    }
    // 接纳门（副作用/转移前只读核验）：实际 SO_TYPE 必须是 SOCK_STREAM，必须已
    // 连接（getpeername 成功）且 family 在当前 backend 支持集内（本包仅 IPv4）。
    // 拒绝时不转移、不 close 输入，调用者可继续使用或 RAII 回收。
    int actual_type = 0;
    socklen_t type_size = sizeof(actual_type);
    if (::getsockopt(socket.Fd(), SOL_SOCKET, SO_TYPE, &actual_type, &type_size) < 0
        || actual_type != SOCK_STREAM) {
        error = std::make_error_code(std::errc::invalid_argument); // 非流式（如 UDP）
        return nullptr;
    }
    std::error_code peer_error;
    const Address peer = socket.RemoteAddress(peer_error);
    if (peer_error) {
        error = std::make_error_code(std::errc::not_connected); // 未连接 TCP
        return nullptr;
    }
    if (peer.Family() != AddressFamily::IPv4) {
        error = std::make_error_code(std::errc::operation_not_supported); // 不扩家族
        return nullptr;
    }
    // advisory 选项发生在转移前（fd 仍归 NativeSocket，失败不阻断接纳）。
    if (options.tcp_no_delay) { (void)socket.SetNoDelay(true); }
    if (options.socket_send_buffer != 0) {
        (void)socket.SetWriteBuffer(static_cast<int>(options.socket_send_buffer));
    }
    if (options.socket_recv_buffer != 0) {
        (void)socket.SetReadBuffer(static_cast<int>(options.socket_recv_buffer));
    }

    auto connection = std::shared_ptr<StreamConnection>(
        new StreamConnection(owner, options, std::move(callbacks)));
    boost::system::error_code assign_error;
    connection->socket_.assign(boost::asio::ip::tcp::v4(), socket.Fd(), assign_error);
    if (assign_error) {
        // 环境/类型非法：未转移，fd 仍归 NativeSocket RAII，明确退回。
        error = MapError(assign_error);
        return nullptr;
    }
    // 转移成功：此后唯一 fd 只归本对象，不再触碰 NativeSocket（无 dup/双 close）。
    (void)socket.Release();
    connection->socket_.non_blocking(true, assign_error);
    if (assign_error) {
        // 已接管后的初始化失败：走本对象物理收口，fd 不外泄。
        connection->Close(MapError(assign_error));
        error = MapError(assign_error);
        return nullptr;
    }
    if (const auto arm_error = connection->ArmRead()) {
        // 首次读登记分配失败：物理收口回收已接管 fd，并向调用者明确失败，
        // 不留半初始化对象（armed 只在真实登记成功后置位）。
        connection->Close(arm_error);
        error = arm_error;
        return nullptr;
    }
    error = {};
    return connection;
}

void StreamConnection::RequireOwner() const {
    if (!owner_.IsOwner()) {
        throw std::logic_error("StreamConnection state is owner-only");
    }
}

std::size_t StreamConnection::ReadRoom() const noexcept {
    return options_.max_inbound_bytes > inbound_.Size()
        ? options_.max_inbound_bytes - inbound_.Size() : 0;
}

std::error_code StreamConnection::Write(std::string data) {
    RequireOwner();
    if (state_ != State::Open) {
        return std::make_error_code(std::errc::operation_not_supported);
    }
    if (data.empty()) { return {}; }
    if (write_terminal_) {
        return io_error_ ? io_error_ : std::make_error_code(std::errc::io_error);
    }
    // 预算预检：任何修改之前拒绝，已入队内容不变。
    if (data.size() > options_.max_outbound_bytes - outbound_.TotalBytes()) {
        return std::make_error_code(std::errc::no_buffer_space);
    }
    if (outbound_.SegmentCount() >= options_.max_pending_events) {
        return std::make_error_code(std::errc::no_buffer_space);
    }
    if (const auto error = outbound_.Append(data)) { return error; }
    ++stats_.writes_accepted;
    stats_.write_bytes_queued += data.size();
    stats_.peak_pending_bytes = std::max(stats_.peak_pending_bytes, outbound_.TotalBytes());
    if (const auto arm_error = ArmWrite()) {
        // 接纳后的写登记失败必须明确失败：已入队数据保留（pending 可验证、
        // 不静默续发），写侧终态+错误一次可见；此后 Write 在 write_terminal_
        // 处拒绝。不回滚队列、不加通用重试。
        RecordBroken(arm_error);
        write_terminal_ = true;
        NotifyTraffic();
        return arm_error;
    }
    return {};
}

EventSnapshot StreamConnection::TakeSnapshot(std::error_code& error) {
    RequireOwner();
    if (state_ != State::Open) {
        error = std::make_error_code(std::errc::operation_not_supported);
        return {};
    }
    if (lent_) {
        error = std::make_error_code(std::errc::resource_unavailable_try_again);
        return {};
    }
    if (inbound_.Empty()) {
        error = std::make_error_code(std::errc::resource_unavailable_try_again);
        return {};
    }
    auto snapshot = inbound_.TakeSnapshot(); // 独立 backing，借出后不受泵影响。
    lent_bytes_ = inbound_.Size();
    inbound_.Clear();                        // 最小版：借出期间读泵暂停。
    lent_ = true;
    ++stats_.snapshots_taken;
    error = {};
    return snapshot;
}

std::error_code StreamConnection::ReturnSnapshot(EventSnapshot&& snapshot,
    std::size_t consumed) {
    RequireOwner();
    if (!lent_) {
        return std::make_error_code(std::errc::operation_not_supported);
    }
    // 强校验：consumed + 剩余必须对上借出量；拒绝时状态与快照都不变。
    if (consumed > lent_bytes_ || snapshot.Remaining() != lent_bytes_ - consumed) {
        return std::make_error_code(std::errc::result_out_of_range);
    }
    // 未消费尾部先回到积压（此时无新字节：借出期间读泵暂停），保序合并。
    if (!snapshot.Empty()) {
        if (const auto error = inbound_.Append(snapshot.Peek())) { return error; }
    }
    lent_ = false;
    stats_.snapshot_consumed_bytes += consumed;
    ++stats_.snapshots_returned;
    if (state_ == State::Open) {
        if (const auto arm_error = ArmRead()) {
            // 物理关闭不等快照归还；Closed/Closing 只清理不恢复。归还本身
            // 已完成（尾部不丢），恢复读泵的登记失败走统一断裂出口。
            RecordBroken(arm_error);
            read_terminal_ = true;
            NotifyTraffic();
        }
    }
    return {};
}

std::error_code StreamConnection::Close(std::error_code reason) {
    RequireOwner();
    if (state_ != State::Open) { return {}; } // 重复/竞争仅物理收口一次。
    state_ = State::Closing;
    close_reason_ = std::move(reason);
    // 撤销 read/write 登记：close 同步 abort 全部在途等待，晚回调由
    // state 守卫安全退出，不再触碰 fd。
    read_armed_ = false;
    write_armed_ = false;
    boost::system::error_code close_error;
    socket_.close(close_error); // 最后内核访问 + 唯一 fd 关闭（同步完成）。
    ++stats_.physical_closes;
    const auto result = MapError(close_error);
    if (result) {
        RecordBroken(result);
        close_reason_ = result; // 关闭 syscall 错误不可被原始 reason 或成功掩盖。
    }
    // 本地 discard 不再重建 OutboundQueue（重建分配曾令 Closing 永久丢
    // 完成）：ConsumeSent(TotalBytes()) 只清空已拥有数据，不依赖分配，
    // 已借快照不阻挡物理关闭，晚 callback 仅清理。
    (void)outbound_.ConsumeSent(outbound_.TotalBytes());
    state_ = State::Closed;
    if (callbacks_.on_closed) { callbacks_.on_closed(*this, close_reason_); }
    return result;
}

StreamConnection::State StreamConnection::ConnectionState() const {
    RequireOwner();
    return state_;
}
const StreamConnection::Stats& StreamConnection::GetStats() const {
    RequireOwner();
    return stats_;
}
std::size_t StreamConnection::BufferedBytes() const {
    RequireOwner();
    return inbound_.Size();
}
std::size_t StreamConnection::PendingBytes() const {
    RequireOwner();
    return outbound_.TotalBytes();
}
std::size_t StreamConnection::PendingSegments() const {
    RequireOwner();
    return outbound_.SegmentCount();
}
bool StreamConnection::SnapshotLent() const {
    RequireOwner();
    return lent_;
}
bool StreamConnection::ReadWaiting() const {
    RequireOwner();
    return read_armed_;
}
bool StreamConnection::WriteWaiting() const {
    RequireOwner();
    return write_armed_;
}
bool StreamConnection::ReadEof() const {
    RequireOwner();
    return eof_;
}
std::error_code StreamConnection::LastIoError() const {
    RequireOwner();
    return io_error_;
}

std::error_code StreamConnection::ArmRead() {
    if (state_ != State::Open || read_armed_ || read_terminal_) { return {}; }
    if (lent_) { return {}; }             // 快照借出期间暂停（最小版）。
    if (ReadRoom() == 0) { return {}; }   // 高水位：有空间才读，否则等归还。
    try {
        auto self = shared_from_this();    // 晚回调保活至末次 handler 退出。
        socket_.async_wait(boost::asio::ip::tcp::socket::wait_read,
            [self](const boost::system::error_code& error) { self->OnReadWait(error); });
    } catch (const std::bad_alloc&) {
        // 唯一共同登记出口：async_wait 登记分配失败不置 armed、不计登记
        // 次数（无在途 wait），错误交调用者按所在阶段统一收口。
        return std::make_error_code(std::errc::not_enough_memory);
    }
    read_armed_ = true;                // armed 只在真实登记成功后置位。
    ++stats_.read_arms;
    return {};
}

std::error_code StreamConnection::ArmWrite() {
    if (state_ != State::Open || write_armed_ || write_terminal_) { return {}; }
    if (outbound_.Empty()) { return {}; } // 空队列不空转。
    try {
        auto self = shared_from_this();
        socket_.async_wait(boost::asio::ip::tcp::socket::wait_write,
            [self](const boost::system::error_code& error) { self->OnWriteWait(error); });
    } catch (const std::bad_alloc&) {
        // 唯一共同登记出口：同 ArmRead，只捕获登记分配失败。
        return std::make_error_code(std::errc::not_enough_memory);
    }
    write_armed_ = true;               // armed 只在真实登记成功后置位。
    ++stats_.write_arms;
    return {};
}

void StreamConnection::OnReadWait(const boost::system::error_code& error) {
    read_armed_ = false;
    ++stats_.wait_callbacks;
    // 晚回调守卫：Closing/Closed（含撤销 abort）一律安全出口，不触碰 fd。
    if (state_ != State::Open || error == boost::asio::error::operation_aborted) {
        ++stats_.guard_exits;
        return;
    }
    if (error) {
        RecordBroken(MapError(error));
        read_terminal_ = true;
        NotifyTraffic();
        return;
    }
    if (lent_ || state_ != State::Open) { return; } // 借出/暂停：等待归还恢复。
    const std::size_t room =
        std::min<std::size_t>(options_.read_buffer_cap, ReadRoom());
    if (room == 0) { return; }                      // 高水位暂停，不空转。
    // 读前预留：recv 之前确保能全量接纳本窗口字节，消除「成功 recv 后
    // Append 首次扩容失败丢字节」。预留失败时内核字节未消费，错误/终态
    // 一次可见，不留 Open+无error+无arm+无通知的静默停滞。
    std::error_code reserve_error;
    try {
        reserve_error = inbound_.Reserve(inbound_.Size() + room);
    } catch (const std::bad_alloc&) {
        reserve_error = std::make_error_code(std::errc::not_enough_memory);
    }
    if (reserve_error) {
        RecordBroken(reserve_error);
        read_terminal_ = true;
        NotifyTraffic(); // 状态先行，本连接一次性终止通知
        return;
    }
    boost::system::error_code io_error;
    const auto bytes = socket_.read_some(boost::asio::buffer(read_block_.data(), room),
        io_error);
    if (io_error == boost::asio::error::would_block
        || io_error == boost::asio::error::try_again
        || io_error == boost::asio::error::interrupted) {
        if (const auto arm_error = ArmRead()) {
            // 就绪窗口竞争/中断的重挂登记失败：统一断裂出口，无数据批次
            // 只此一次错误通知。
            RecordBroken(arm_error);
            read_terminal_ = true;
            NotifyTraffic();
        }
        return;
    }
    if (io_error == boost::asio::error::eof) {
        eof_ = true; // 干净 FIN：ReadEof=true，LastIoError 保持为空。
        read_terminal_ = true;
        NotifyTraffic(); // 一次性 EOF 通知，业务可取最终快照。
        return;
    }
    if (io_error) {
        // 真实非EOF致命错误（如 RST/ECONNRESET）：保留 std 错误码，不伪装 EOF。
        RecordBroken(MapError(io_error));
        read_terminal_ = true;
        NotifyTraffic(); // 一次性终止通知（read_terminal_ 保证只此一次）。
        return;
    }
    if (bytes == 0) {
        eof_ = true; // 无错误的 0 字节读按干净 EOF 收口（防御路径）。
        read_terminal_ = true;
        NotifyTraffic();
        return;
    }
    if (const auto append_error =
            inbound_.Append(std::string_view(read_block_.data(), bytes))) {
        // 预算内拒绝：视作本连接 IO 故障停读，不丢已收字节（仍在内核）。
        RecordBroken(append_error);
        read_terminal_ = true;
        NotifyTraffic();
        return;
    }
    stats_.read_bytes += bytes;
    // 已备容量内 Append 只提交不分配；相邻登记的分配出口同样可见：
    // ArmRead 分配失败不静默停泵，状态先行、随后一次通知带数据+错误。
    if (const auto arm_error = ArmRead()) {
        // 预算内继续（LT 风格），满则自然停在高水位；登记失败只记录，
        // 与本批数据共用同一次通知。
        RecordBroken(arm_error);
        read_terminal_ = true;
    }
    NotifyTraffic();  // 每批新字节一次通知；暂停/无新数据不派发。
}

void StreamConnection::OnWriteWait(const boost::system::error_code& error) {
    write_armed_ = false;
    ++stats_.wait_callbacks;
    if (state_ != State::Open || error == boost::asio::error::operation_aborted) {
        ++stats_.guard_exits;
        return;
    }
    if (error) {
        RecordBroken(MapError(error));
        write_terminal_ = true;
        NotifyTraffic();
        return;
    }
    // 在途期间队列只追加不改首部；ConsumeSent 只在本 handler 内执行，
    // GatherViews 的存储到本次同步 send 返回都有效。
    std::string_view views[kWriteViews];
    const auto count = outbound_.GatherViews(views, kWriteViews);
    // 固定最多 16 段：栈上定长数组直接转 Asio buffer 视图，去掉每次
    // send 的 vector 热分配；尾部空 buffer 不参与传输。
    std::array<boost::asio::const_buffer, kWriteViews> buffers{};
    for (std::size_t i = 0; i < count; ++i) {
        buffers[i] = boost::asio::buffer(views[i].data(), views[i].size());
    }
    boost::system::error_code io_error;
    const auto sent = socket_.write_some(buffers, io_error);
    if (io_error == boost::asio::error::would_block
        || io_error == boost::asio::error::try_again
        || io_error == boost::asio::error::interrupted) {
        ++stats_.zero_write_progress; // 写就绪但零进度：重挂由 readiness 门控。
        if (const auto arm_error = ArmWrite()) {
            RecordBroken(arm_error);
            write_terminal_ = true;
            NotifyTraffic();
        }
        return;
    }
    if (io_error) {
        RecordBroken(MapError(io_error));
        write_terminal_ = true;
        NotifyTraffic();
        return;
    }
    if (sent == 0) {
        ++stats_.zero_write_progress; // 写就绪但零进度：重挂由 readiness 门控。
        if (const auto arm_error = ArmWrite()) {
            RecordBroken(arm_error);
            write_terminal_ = true;
            NotifyTraffic();
        }
        return;
    }
    // partial 写：FIFO 消费已发前缀，剩余保序续投（在同一次就绪内推进）。
    if (const auto consume_error = outbound_.ConsumeSent(sent)) {
        RecordBroken(consume_error);
        write_terminal_ = true;
        NotifyTraffic();
        return;
    }
    stats_.write_bytes_sent += sent;
    if (outbound_.Empty()) {
        NotifyDrained(); // 内核已全收（不代表对端收到）；此后不再 re-arm。
        return;
    }
    if (const auto arm_error = ArmWrite()) {
        // 相邻登记分配失败：已发进度保留在队列，错误/终止一次可见，
        // 不回到静默停滞。
        RecordBroken(arm_error);
        write_terminal_ = true;
        NotifyTraffic();
    }
}

void StreamConnection::NotifyTraffic() {
    ++stats_.traffic_notifications;
    if (callbacks_.on_traffic) { callbacks_.on_traffic(*this); }
}

void StreamConnection::NotifyDrained() {
    ++stats_.drained_notifications;
    if (callbacks_.on_drained) { callbacks_.on_drained(*this); }
}

void StreamConnection::RecordBroken(std::error_code error) {
    if (!io_error_) { io_error_ = std::move(error); }
}

} // namespace bbt::infra::net
