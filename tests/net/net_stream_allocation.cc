// 真实 TCP + 分配失败注入的 stream 连接回归：仅本可执行文件内做线程局部
// 一次分配注入与 link-time recv 记录（--wrap=recv，同 deadline/foundation
// 范式，不用 LD_PRELOAD、不改产品/DSO）。复用父级已证探针的驱动方式。
#include <bbt/infra/net/detail/StreamConnection.hpp>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <future>
#include <new>
#include <poll.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <utility>

#include <chrono>

using namespace bbt::infra::net;
using namespace std::chrono_literals;

namespace {

thread_local bool fail_next_allocation = false;
thread_local std::size_t fail_allocation_size = 0;
thread_local ssize_t kernel_bytes_seen = 0;
thread_local int close_error_fd = -1;
thread_local int close_error_injections = 0;
// 精确回收记录：TLS 指定 fd 的 close 尝试/成功次数（只数本测试的真实调用）。
thread_local int close_watch_fd = -1;
thread_local int close_watch_attempts = 0;
thread_local int close_watch_success = 0;

void ResetInjection() {
    fail_next_allocation = false;
    fail_allocation_size = 0;
}

} // namespace

void* operator new(std::size_t size) {
    if (fail_next_allocation && (fail_allocation_size == 0 || size == fail_allocation_size)) {
        fail_next_allocation = false;
        throw std::bad_alloc();
    }
    if (void* p = std::malloc(size == 0 ? 1 : size)) { return p; }
    throw std::bad_alloc();
}
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }
void operator delete[](void* p) noexcept { std::free(p); }
void operator delete[](void* p, std::size_t) noexcept { std::free(p); }

extern "C" ssize_t __real_recv(int fd, void* data, size_t size, int flags);
extern "C" int __real_close(int fd);
extern "C" int __wrap_close(int fd) {
    const bool watched = fd == close_watch_fd;
    if (watched) { ++close_watch_attempts; }
    const int result = __real_close(fd); // 原Hook/epoch路径，真实关闭fd。
    if (watched && result == 0) { ++close_watch_success; }
    if (fd == close_error_fd && result == 0) {
        close_error_fd = -1;
        ++close_error_injections;
        errno = EIO;
        return -1; // Linux关闭资源已释放但返回错误，adapter仍必须传出错误。
    }
    return result;
}
extern "C" ssize_t __wrap_recv(int fd, void* data, size_t size, int flags) {
    const auto bytes = __real_recv(fd, data, size, flags);
    if (bytes > 0 && (flags & MSG_PEEK) == 0) {
        kernel_bytes_seen += bytes;
    }
    return bytes;
}

// aligned_alloc 一次注入：Boost 1.90 Asio 的 async_wait 等待登记分配实际
// 走 std::aligned_alloc（asio/detail/memory.hpp aligned_new），全局
// operator new 注入命中不到。--wrap=aligned_alloc 仅本测试可执行文件；
// 命中一次即恢复透传，不关 recycling、不改产品宏/DSO/LD_*。
thread_local bool fail_aligned_once = false;
thread_local std::size_t aligned_failures = 0;
extern "C" void* __real_aligned_alloc(std::size_t alignment, std::size_t size);
extern "C" void* __wrap_aligned_alloc(std::size_t alignment, std::size_t size) {
    if (fail_aligned_once) {
        fail_aligned_once = false;
        ++aligned_failures;
        errno = ENOMEM;
        return nullptr;
    }
    return __real_aligned_alloc(alignment, size);
}

namespace {

void Check(bool value, const char* message) {
    if (!value) { throw std::runtime_error(message); }
}

void Owner(EventLoop& loop, std::function<void()> action) {
    auto promise = std::make_shared<std::promise<void>>();
    auto ready = promise->get_future();
    Check(loop.Submit([promise, action] {
        try { action(); promise->set_value(); }
        catch (...) { promise->set_exception(std::current_exception()); }
    }) == SubmitStatus::Accepted, "owner submit");
    Check(ready.wait_for(3s) == std::future_status::ready, "owner timeout");
    ready.get();
}

NormalizedOptions Config() {
    Options raw;
    raw.read_buffer_cap = 1024;
    raw.max_inbound_bytes = 8192;
    raw.write_buffer_cap = 4096;
    raw.max_outbound_bytes = 1024 * 1024;
    raw.max_pending_events = 64;
    NormalizedOptions out;
    Check(!NormalizeOptions(raw, 1, out), "normalize");
    return out;
}

struct Pair {
    NativeSocket client, server;
};

Pair TcpPair() {
    std::error_code error;
    auto listener = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    Check(!error, "open listener");
    auto address = Address::Ip("127.0.0.1", 0, error);
    Check(!error && !listener.Bind(address) && !listener.Listen(8), "listen");
    address = listener.LocalAddress(error);
    Check(!error, "address");
    Pair pair;
    pair.client = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    Check(!error, "open client");
    error = pair.client.Connect(address);
    Check(!error || error.value() == EINPROGRESS, "connect");
    pollfd item{listener.Fd(), POLLIN, 0};
    Check(::poll(&item, 1, 1000) == 1, "accept poll");
    Address peer;
    pair.server = listener.Accept(peer, error);
    Check(!error && pair.server.Fd() >= 0, "accept");
    return pair;
}

// 关闭 discard：真实连接 + 待发积压 + 已借快照；按 writecap 精确注入旧路径
// 重建 OutboundQueue 的 4096 分配。旧候选：Close 抛 bad_alloc、Closing 悬挂、
// on_closed 丢失；新候选：不抛、Closed、完成恰好一次、积压清空。
void RunCloseAllocation(EventLoop& loop) {
    auto pair = TcpPair();
    std::shared_ptr<StreamConnection> connection;
    std::size_t closed = 0;
    StreamConnection::Callbacks callbacks;
    callbacks.on_closed = [&](StreamConnection&, const std::error_code&) { ++closed; };
    Owner(loop, [&] {
        std::error_code error;
        connection = StreamConnection::Create(loop, std::move(pair.server), Config(), callbacks, error);
        Check(!error && connection, "adopt tcp");
    });
    Check(!pair.client.Send("SNAP", 4).error, "send snap");
    bool ready = false;
    for (int i = 0; i < 200 && !ready; ++i) {
        Owner(loop, [&] { ready = connection->BufferedBytes() == 4; });
        if (!ready) { std::this_thread::sleep_for(1ms); }
    }
    Check(ready, "snap data received");
    Owner(loop, [&] {
        // 同一 owner 动作内完成积压+注入+关闭：OnWriteWait 不会插入排空，
        // 关闭窗口内队列必有实物待发数据。
        for (int i = 0; i < 40; ++i) {
            const auto error = connection->Write(
                std::string(1024, static_cast<char>('a' + i % 26)));
            Check(!error, "write backlog");
        }
        Check(connection->PendingBytes() == 40960, "backlog queued");
        std::error_code snap_error;
        auto snapshot = connection->TakeSnapshot(snap_error);
        Check(!snap_error && snapshot.Peek() == "SNAP", "snapshot before close");
        bool threw = false;
        fail_allocation_size = Config().write_buffer_cap;
        fail_next_allocation = true;
        try { connection->Close({}); } catch (const std::bad_alloc&) { threw = true; }
        ResetInjection();
        std::printf("close_allocation threw=%d state=%d closed=%zu physical=%zu pending=%zu\n",
            threw ? 1 : 0, static_cast<int>(connection->ConnectionState()), closed,
            connection->GetStats().physical_closes, connection->PendingBytes());
        Check(!threw, "close discard must not throw on injected allocation");
        Check(connection->ConnectionState() == StreamConnection::State::Closed, "closed state");
        Check(closed == 1, "on_closed exactly once");
        Check(connection->GetStats().physical_closes == 1, "physical close once");
        Check(connection->PendingBytes() == 0, "backlog discarded locally");
        connection->Close({}); // 重复 Close 不得补发完成
        Check(closed == 1, "no duplicate on_closed");
        Check(snapshot.Peek() == "SNAP", "lent snapshot still readable after close");
        Check(!connection->ReturnSnapshot(std::move(snapshot), 0), "return lent snapshot");
        Check(!connection->SnapshotLent(), "snapshot returned");
    });
    Owner(loop, [&] { connection.reset(); });
}

void RunCloseError(EventLoop& loop) {
    auto pair = TcpPair();
    const int fd = pair.server.Fd();
    std::shared_ptr<StreamConnection> connection;
    std::size_t closed = 0;
    std::error_code notified;
    StreamConnection::Callbacks callbacks;
    callbacks.on_closed = [&](StreamConnection&, const std::error_code& error) {
        ++closed;
        notified = error;
    };
    Owner(loop, [&] {
        std::error_code error;
        connection = StreamConnection::Create(loop, std::move(pair.server), Config(), callbacks, error);
        Check(connection && !error, "adopt close error");
        close_error_fd = fd;
        close_error_injections = 0;
        const auto result = connection->Close(std::make_error_code(std::errc::operation_canceled));
        Check(close_error_injections == 1 && result.value() == EIO && notified == result,
            "close error must reach return value and completion before original reason");
        Check(connection->LastIoError() == result, "close error recorded");
        Check(connection->ConnectionState() == StreamConnection::State::Closed
            && connection->GetStats().physical_closes == 1 && closed == 1,
            "failed close result still has one physical completion on Linux");
        connection->Close({});
        Check(closed == 1 && close_error_injections == 1, "close error completion not repeated");
        std::printf("close_error returned=%d notified=%d physical=1 callbacks=1\n",
            result.value(), notified.value());
    });
    Owner(loop, [&] { connection.reset(); });
}

// 读路径分配：阶段一在读前 Reserve 扩容处注入（recv 之前），内核字节必须
// 未消费且资源错误/终态一次可见；阶段二在成功 recv 后的下一个分配处注入，
// 已收数据必须仍可取、错误出口可见，不得静默停泵。
void RunReadAllocation(EventLoop& loop) {
    auto pair_a = TcpPair();
    const int fd_a = pair_a.server.Fd();
    std::shared_ptr<StreamConnection> a;
    std::size_t traffic_a = 0;
    StreamConnection::Callbacks cb_a;
    cb_a.on_traffic = [&](StreamConnection&) { ++traffic_a; };
    Owner(loop, [&] {
        std::error_code error;
        a = StreamConnection::Create(loop, std::move(pair_a.server), Config(), cb_a, error);
        Check(!error && a, "adopt a");
    });
    Owner(loop, [&] {
        fail_allocation_size = Config().read_buffer_cap; // 读前 Reserve 扩容量
        fail_next_allocation = true;
    });
    Check(!pair_a.client.Send("DATA", 4).error, "send a");
    bool seen_a = false;
    for (int i = 0; i < 200 && !seen_a; ++i) {
        Owner(loop, [&] { seen_a = traffic_a > 0 || loop.FailureCount() > 0; });
        if (!seen_a) { std::this_thread::sleep_for(1ms); }
    }
    Check(seen_a, "reserve failure surfaced");
    Owner(loop, [&] {
        ResetInjection();
        std::printf(
            "read_reserve kernel=%zd failures=%zu state=%d buffered=%zu armed=%d error=%d eof=%d traffic=%zu\n",
            kernel_bytes_seen, loop.FailureCount(), static_cast<int>(a->ConnectionState()),
            a->BufferedBytes(), a->ReadWaiting() ? 1 : 0, a->LastIoError().value(),
            a->ReadEof() ? 1 : 0, traffic_a);
        Check(kernel_bytes_seen == 0, "reserve failure must not consume kernel bytes");
        Check(a->LastIoError() == std::make_error_code(std::errc::not_enough_memory),
            "reserve failure resource error visible");
        Check(a->ConnectionState() == StreamConnection::State::Open, "no fake closed");
        Check(!a->ReadEof(), "no fake eof");
        Check(!a->ReadWaiting(), "read not re-armed after reserve failure");
        Check(a->BufferedBytes() == 0, "no bytes swallowed into buffer");
        Check(traffic_a == 1, "one terminal notification");
        char unread[4] = {};
        Check(::recv(fd_a, unread, sizeof unread, MSG_PEEK | MSG_DONTWAIT) == 4
            && std::string_view(unread, sizeof unread) == "DATA",
            "reserve failure leaves original data in kernel");
        a->Close({});
    });

    Owner(loop, [] { kernel_bytes_seen = 0; });
    auto pair_b = TcpPair();
    const int fd_b = pair_b.server.Fd();
    std::shared_ptr<StreamConnection> b;
    std::size_t traffic_b = 0;
    std::size_t terminal_b = 0;
    std::size_t traffic_before_failure = 0;
    StreamConnection::Callbacks cb_b;
    cb_b.on_traffic = [&](StreamConnection& connection) {
        ++traffic_b;
        if (connection.LastIoError()) { ++terminal_b; }
    };
    Owner(loop, [&] {
        std::error_code error;
        b = StreamConnection::Create(loop, std::move(pair_b.server), Config(), cb_b, error);
        Check(!error && b, "adopt b");
    });
    // 批1正常接收后，注入批2 Reserve 增量扩容失败（修复后读路径中首个
    // 真实全局分配点；批1数据必须仍可取，批2内核字节未消费，错误一次可见）。
    Check(!pair_b.client.Send("FIRST", 5).error, "send b first");
    bool got_first = false;
    for (int i = 0; i < 200 && !got_first; ++i) {
        Owner(loop, [&] { got_first = b->BufferedBytes() == 5; });
        if (!got_first) { std::this_thread::sleep_for(1ms); }
    }
    Check(got_first, "first batch received");
    Owner(loop, [&] {
        traffic_before_failure = traffic_b;
        Check(traffic_before_failure == 1 && terminal_b == 0,
            "first batch publishes one data notification before failure");
        fail_allocation_size = 2048; // 批2 Reserve 增量扩容目标
        fail_next_allocation = true;
    });
    Check(!pair_b.client.Send("SECOND", 6).error, "send b second");
    bool seen_b = false;
    for (int i = 0; i < 200 && !seen_b; ++i) {
        Owner(loop, [&] { seen_b = b->LastIoError().value() != 0 || loop.FailureCount() > 0; });
        if (!seen_b) { std::this_thread::sleep_for(1ms); }
    }
    Check(seen_b, "batch2 failure surfaced");
    Owner(loop, [&] {
        ResetInjection();
        std::printf(
            "read_grow kernel=%zd failures=%zu state=%d buffered=%zu armed=%d error=%d traffic=%zu\n",
            kernel_bytes_seen, loop.FailureCount(), static_cast<int>(b->ConnectionState()),
            b->BufferedBytes(), b->ReadWaiting() ? 1 : 0, b->LastIoError().value(), traffic_b);
        Check(kernel_bytes_seen == 5, "batch2 recv must not consume kernel bytes");
        Check(b->BufferedBytes() == 5, "batch1 bytes retained");
        std::error_code snap_error;
        auto snapshot = b->TakeSnapshot(snap_error);
        Check(!snap_error && snapshot.Peek() == "FIRST",
            "batch1 data retrievable after growth allocation failure");
        Check(b->LastIoError() == std::make_error_code(std::errc::not_enough_memory),
            "growth failure error visible");
        Check(!b->ReadWaiting(), "no silent re-arm");
        Check(b->ConnectionState() == StreamConnection::State::Open, "state stays open");
        Check(traffic_b == traffic_before_failure + 1 && terminal_b == 1,
            "growth failure adds exactly one terminal notification after data");
        char unread[6] = {};
        Check(::recv(fd_b, unread, sizeof unread, MSG_PEEK | MSG_DONTWAIT) == 6
            && std::string_view(unread, sizeof unread) == "SECOND",
            "growth failure retains second batch in kernel");
        Check(!b->ReturnSnapshot(std::move(snapshot), 0),
            "retained first batch snapshot returned after failure");
        b->Close({});
    });
    Owner(loop, [&] { a.reset(); b.reset(); });
}

// 统一等待登记出口回归：真实 async_wait 登记分配命中（aligned_alloc 一次
// 注入，只测登记本身，不关 recycling）。后测：接纳后首次 Write 的
// ArmWrite 登记失败——Write 错误返回（不抛出/不假成功）、armed=false、
// 终态可观察、通知恰一次、后续 Write 拒绝且不再通知、数据保留可验证。
// 先测：冷 Create 的首次 ArmRead 登记失败——Create 失败返回、已接管
// fd 物理收口恰一次、on_closed 恰一次、原 fd 不再有效。
void RunWaitRegistration(EventLoop& loop) {

    const auto options = Config();
    auto pair_c = TcpPair();
    const int fd_c = pair_c.server.Fd();
    std::size_t closed_c = 0;
    std::weak_ptr<StreamConnection> closed_weak; // on_closed 无分配留弱引用。
    std::size_t closed_physical = 0;
    StreamConnection::Callbacks cb_c;
    cb_c.on_closed = [&](StreamConnection& connection, const std::error_code&) {
        ++closed_c;
        closed_weak = connection.weak_from_this(); // C++17 enable_shared_from_this。
        closed_physical = connection.GetStats().physical_closes;
    };
    std::shared_ptr<StreamConnection> c;
    Owner(loop, [&] {
        fail_aligned_once = true;
        aligned_failures = 0;
        close_watch_fd = fd_c;
        close_watch_attempts = 0;
        close_watch_success = 0;
        std::error_code error;
        bool threw = false;
        try {
            c = StreamConnection::Create(loop, std::move(pair_c.server), options, cb_c, error);
        } catch (const std::bad_alloc&) { threw = true; }
        std::printf("create_arm_aligned hit=%zu threw=%d object=%d closed=%zu error=%d"
            " close_attempts=%d close_success=%d callback_physical=%zu\n",
            aligned_failures, threw ? 1 : 0, c ? 1 : 0, closed_c, error.value(),
            close_watch_attempts, close_watch_success, closed_physical);
        Check(!threw, "create reports registration failure via error");
        Check(aligned_failures == 1, "cold create arm hit == 1");
        Check(!c, "failed create returns no object");
        Check(error == std::make_error_code(std::errc::not_enough_memory),
            "create reports ENOMEM");
        Check(closed_c == 1, "on_closed exactly once for adopted fd reclaim");
        Check(closed_physical == 1, "on_closed observed the single physical close");
        Check(close_watch_attempts == 1 && close_watch_success == 1,
            "cold create reclaim closes the adopted fd exactly once and successfully");
        close_watch_fd = -1; // 清理 TLS watch：不污染同进程后续 close 记录。
        close_watch_attempts = 0;
        close_watch_success = 0;
        Check(closed_weak.expired(), "no half-initialized object survives failed create");
    });
    errno = 0;
    struct stat after_c {};
    Check(::fstat(fd_c, &after_c) != 0 && errno == EBADF,
        "adopted fd physically closed exactly once");
    Check(pair_c.server.Fd() == -1, "fd ownership fully moved out of caller");
    Owner(loop, [&] { c.reset(); });
    auto pair_w = TcpPair();
    std::shared_ptr<StreamConnection> w;
    std::size_t traffic_w = 0;
    StreamConnection::Callbacks cb_w;
    cb_w.on_traffic = [&](StreamConnection&) { ++traffic_w; };
    Owner(loop, [&] {
        std::error_code error;
        w = StreamConnection::Create(loop, std::move(pair_w.server), Config(), cb_w, error);
        Check(!error && w, "adopt wait registration");
        aligned_failures = 0;
        fail_aligned_once = true;
        std::error_code write_error;
        bool threw = false;
        try { write_error = w->Write("DATA"); }
        catch (const std::bad_alloc&) { threw = true; }
        std::printf(
            "write_arm_aligned hit=%zu threw=%d returned=%d pending=%zu armed=%d "
            "io_error=%d traffic=%zu\n",
            aligned_failures, threw ? 1 : 0, write_error.value(), w->PendingBytes(),
            w->WriteWaiting() ? 1 : 0, w->LastIoError().value(), traffic_w);
        Check(!threw, "write must return registration error, not throw");
        Check(aligned_failures == 1, "real async_wait registration hit == 1");
        Check(write_error == std::make_error_code(std::errc::not_enough_memory),
            "write returns ENOMEM on registration failure");
        Check(!w->WriteWaiting(), "armed only after real registration");
        Check(w->LastIoError() == std::make_error_code(std::errc::not_enough_memory),
            "registration failure visible as LastIoError");
        Check(w->PendingBytes() == 4, "accepted data stays observable");
        Check(traffic_w == 1, "exactly one failure notification");
        const auto rejected = w->Write("MORE");
        std::printf("write_rearm rejected=%d pending=%zu traffic=%zu\n",
            rejected.value(), w->PendingBytes(), traffic_w);
        Check(rejected == std::make_error_code(std::errc::not_enough_memory),
            "write rejected after write terminal");
        Check(traffic_w == 1, "rejection must not notify again");
        Check(w->PendingBytes() == 4, "rejected write must not queue");
        w->Close({});
    });
    Owner(loop, [&] { w.reset(); });

}
} // namespace

int main(int argc, char** argv) {
    try {
        Check(argc == 2, "scenario");
        const std::string mode = argv[1];
        EventLoop loop(32);
        Check(loop.Start(), "start");
        if (mode == "stream_close_allocation") {
            RunCloseAllocation(loop);
            RunCloseError(loop);
        } else if (mode == "stream_read_allocation") {
            RunReadAllocation(loop);
        } else if (mode == "stream_wait_registration") {
            RunWaitRegistration(loop);
        } else {
            throw std::runtime_error("unknown scenario");
        }
        Check(loop.FailureCount() == 0, "event loop clean finish");
        loop.RequestStop();
        Check(loop.Join(), "join");
        std::printf("%s ok\n", mode.c_str());
        return 0;
    } catch (const std::exception& error) {
        std::fprintf(stderr, "stream allocation regression failed: %s\n", error.what());
        return 1;
    }
}
