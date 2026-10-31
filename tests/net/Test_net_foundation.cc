#include <bbt/infra/net/CoObject.hpp>
#include <bbt/infra/net/NativeSocket.hpp>

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <future>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
// 只为最小支持缺口探针使用 detail；产品 net API 没有该依赖。
#include <bbt/coroutine/detail/LocalThread.hpp>

using namespace bbt::infra::net;
using namespace std::chrono_literals;

static std::atomic_bool inject_send_eintr{false};
extern "C" ssize_t __real_send(int, const void*, size_t, int);
extern "C" ssize_t __wrap_send(int fd, const void* data, size_t size, int flags) {
    if (inject_send_eintr.exchange(false)) { errno = EINTR; return -1; }
    return __real_send(fd, data, size, flags);
}

static void Check(bool value, const char* expression, int line) {
    if (!value) {
        throw std::runtime_error("line " + std::to_string(line) + ": " + expression);
    }
}
#define CHECK(expression) Check(static_cast<bool>(expression), #expression, __LINE__)

static bool WouldBlock(const std::error_code& error) {
    return error.value() == EAGAIN || error.value() == EWOULDBLOCK;
}

static void Ready(int fd, short events) {
    const auto until = std::chrono::steady_clock::now() + 2s;
    pollfd ready{fd, events, 0};
    int result = 0;
    do {
        result = ::poll(&ready, 1, 100);
        CHECK(result >= 0 || errno == EINTR);
    } while (result <= 0 && std::chrono::steady_clock::now() < until);
    CHECK(result == 1);
    CHECK((ready.revents & (events | POLLERR | POLLHUP)) != 0);
}

static std::size_t FdCount() {
    return static_cast<std::size_t>(std::distance(std::filesystem::directory_iterator("/proc/self/fd"),
        std::filesystem::directory_iterator{}));
}

static NativeSocket Open(AddressFamily family, SocketType type) {
    std::error_code error;
    auto result = NativeSocket::Open(family, type, error);
    CHECK(!error && result.Fd() >= 0);
    CHECK((::fcntl(result.Fd(), F_GETFL) & O_NONBLOCK) != 0);
    CHECK((::fcntl(result.Fd(), F_GETFD) & FD_CLOEXEC) != 0);
    return result;
}

static Address Ip(const char* host, int port = 0) {
    std::error_code error;
    auto result = Address::Ip(host, port, error);
    CHECK(!error && result.Valid());
    return result;
}

static Address Unix(const std::string& path, bool abstract = false) {
    std::error_code error;
    auto result = Address::Unix(path, error, abstract);
    CHECK(!error && result.Valid());
    return result;
}

struct OwnedPath {
    std::string path;
    bool bound{false};
    ~OwnedPath() { if (bound) { (void)::unlink(path.c_str()); } }
};

static int GetIntOption(const NativeSocket& socket, int level, int option) {
    int result = 0;
    socklen_t size = sizeof(result);
    CHECK(::getsockopt(socket.Fd(), level, option, &result, &size) == 0);
    CHECK(size == sizeof(result));
    return result;
}

static void AddressAndOwnership() {
    const auto before = FdCount();
    std::error_code error;
    CHECK(!Address::Ip("not-an-address", 0, error).Valid() && error);
    CHECK(!Address::Ip("127.0.0.1", -1, error).Valid() && error);
    CHECK(!Address::Ip("127.0.0.1", 65536, error).Valid() && error);
    CHECK(!Address::Ip(std::string("127.0.0.1\0bad", 13), 0, error).Valid() && error);
    CHECK(!Address::Unix("", error).Valid() && error);
    CHECK(!Address::Unix(std::string(108, 'a'), error).Valid() && error);
    CHECK(!Address::Unix(std::string("a\0b", 3), error).Valid() && error);
    CHECK(Ip("127.0.0.1", 65535).ToString() == "127.0.0.1:65535");
    CHECK(Ip("::1", 0).ToString() == "[::1]:0");
    CHECK(Ip("fe80::1%3", 1).ToString() == "[fe80::1%3]:1");
    //scope 严格校验——合法边界与 Boost atoi 回退曾误收的反例。
    CHECK(Ip("fe80::1%4294967295", 1).ToString() == "[fe80::1%4294967295]:1"); // u32 上界
    CHECK(Ip("fe80::1%0", 1).ToString() == "[fe80::1]:1");                     // 0 合法，ToString 不输出
    const auto reject_ip = [&error](const std::string& host) {
        CHECK(!Address::Ip(host, 1, error).Valid() && error == std::errc::invalid_argument);
    };
    reject_ip("fe80::1%4294967296"); // 溢出
    reject_ip("fe80::1%-1");         // 负号
    reject_ip("fe80::1%+3");         // 显式符号
    reject_ip("fe80::1%3garbage");   // 尾随垃圾
    reject_ip("fe80::1%");           // 空 suffix
    reject_ip("fe80::1% 3");         // 空白
    reject_ip("fe80::1%3%4");        // 重复 %
    reject_ip("fe80::1%eth0");       // 接口名不支持，无 if_nametoindex/atoi 回退
    reject_ip("127.0.0.1%3");        // IPv4 带 scope
    CHECK(Unix("sample").ToString() == "sample");
    CHECK(Unix("sample", true).ToString() == "@sample");
    CHECK(Address{}.Family() == AddressFamily::Unspecified);
    CHECK(NativeSocket::Open(AddressFamily::Unspecified, SocketType::Stream, error).Fd() == -1 && error);
    CHECK(NativeSocket::Open(AddressFamily::IPv4, static_cast<SocketType>(123), error).Fd() == -1 && error);
    {
        auto socket = Open(AddressFamily::IPv4, SocketType::Stream);
        CHECK(socket.SetReadBuffer(0) == std::errc::invalid_argument);
        CHECK(socket.SetWriteBuffer(-1) == std::errc::invalid_argument);
        CHECK(!socket.SetReadBuffer(8192));
        CHECK(!socket.SetWriteBuffer(8192));
        CHECK(GetIntOption(socket, SOL_SOCKET, SO_RCVBUF) >= 8192);
        CHECK(GetIntOption(socket, SOL_SOCKET, SO_SNDBUF) >= 8192);
        CHECK(!socket.SetReuseAddr(true));
        CHECK(!socket.SetReusePort(true));
        CHECK(GetIntOption(socket, SOL_SOCKET, SO_REUSEADDR) == 1);
        CHECK(GetIntOption(socket, SOL_SOCKET, SO_REUSEPORT) == 1);
        CHECK(!socket.SetNoDelay(true));
        CHECK(GetIntOption(socket, IPPROTO_TCP, TCP_NODELAY) == 1);
        CHECK(socket.SetIpv6Only(true) == std::errc::operation_not_supported);
        CHECK(socket.SetKeepAlive(true, 0s, 1s, 1) == std::errc::invalid_argument);
        CHECK(GetIntOption(socket, SOL_SOCKET, SO_KEEPALIVE) == 0);
        CHECK(!socket.SetKeepAlive(true, 30s, 5s, 3));
        CHECK(GetIntOption(socket, IPPROTO_TCP, TCP_KEEPIDLE) == 30);
        CHECK(GetIntOption(socket, IPPROTO_TCP, TCP_KEEPINTVL) == 5);
        CHECK(GetIntOption(socket, IPPROTO_TCP, TCP_KEEPCNT) == 3);
        CHECK(!socket.SetKeepAlive(false, 0s, 0s, 0));
        CHECK(!socket.SetLinger(-1));
        CHECK(socket.SetLinger(1) == std::errc::operation_not_supported);
        linger linger_value{};
        socklen_t linger_size = sizeof(linger_value);
        CHECK(::getsockopt(socket.Fd(), SOL_SOCKET, SO_LINGER, &linger_value, &linger_size) == 0);
        CHECK(linger_value.l_onoff == 0);
        CHECK(!socket.SetLinger(0));
        CHECK(!socket.SetLinger(-1));
        CHECK(socket.Bind(Ip("::1")) == std::errc::invalid_argument);
        CHECK(socket.Listen(0) == std::errc::invalid_argument);
        CHECK(socket.Send(nullptr, 1).error == std::errc::invalid_argument);
        CHECK(socket.Send("x", std::numeric_limits<std::size_t>::max()).error == std::errc::invalid_argument);
        CHECK(socket.Sendv(nullptr, 1).error == std::errc::invalid_argument);
        CHECK(socket.Sendv(nullptr, 1025).error == std::errc::invalid_argument);
        auto duplicate = socket.Dup(error);
        CHECK(!error && duplicate.Fd() != socket.Fd());
        CHECK((::fcntl(duplicate.Fd(), F_GETFD) & FD_CLOEXEC) != 0);
        CHECK((::fcntl(duplicate.Fd(), F_GETFL) & O_NONBLOCK) != 0);
        const int original_fd = socket.Fd();
        CHECK(!socket.Close() && !socket.Close());
        CHECK(::fcntl(original_fd, F_GETFD) == -1 && errno == EBADF);
        CHECK(::fcntl(duplicate.Fd(), F_GETFD) >= 0);
        CHECK(socket.SetNoDelay(true).value() == EBADF);
        auto destination = Open(AddressFamily::IPv4, SocketType::Datagram);
        const int replaced_fd = destination.Fd();
        const int kept_fd = duplicate.Fd();
        destination = std::move(duplicate);
        CHECK(duplicate.Fd() == -1 && destination.Fd() == kept_fd);
        CHECK(::fcntl(replaced_fd, F_GETFD) == -1 && errno == EBADF);
        const int released = destination.Release();
        CHECK(released == kept_fd && destination.Fd() == -1);
        CHECK(::close(released) == 0);
        CHECK(socket.Send("x", 1).error.value() == EBADF);
        CHECK(socket.Dup(error).Fd() == -1 && error.value() == EBADF);
    }
    {
        auto original = Open(AddressFamily::IPv4, SocketType::Datagram);
        CHECK(!original.Bind(Ip("127.0.0.1")));
        const auto bound = original.LocalAddress(error);
        CHECK(!error);
        auto duplicate = original.Dup(error);
        CHECK(!error && !original.Close());
        auto sender = Open(AddressFamily::IPv4, SocketType::Datagram);
        CHECK(!sender.SendTo("d", 1, bound).error);
        Ready(duplicate.Fd(), POLLIN);
        char data = 0;
        CHECK(!duplicate.Receive(&data, 1).error && data == 'd');
        CHECK(!duplicate.Close() && !sender.Close());
    }
    for (int i = 0; i < 32; ++i) {
        auto failing = Open(AddressFamily::IPv4, SocketType::Datagram);
        CHECK(failing.SetNoDelay(true) == std::errc::operation_not_supported);
        CHECK(failing.Listen(1) == std::errc::operation_not_supported);
        CHECK(failing.Bind(Address{}));
    }
    CHECK(FdCount() == before);
    std::cout << "address/flags/options/dup/move/failure cleanup fd baseline=" << before << " restored\n";
}

struct StreamPair { NativeSocket client; NativeSocket server; };

static StreamPair ConnectPair(const Address& address, OwnedPath* path = nullptr) {
    auto listener = Open(address.Family(), SocketType::Stream);
    if (address.Family() == AddressFamily::IPv6) { CHECK(!listener.SetIpv6Only(true)); }
    CHECK(!listener.Bind(address));
    if (path) { path->bound = true; }
    CHECK(!listener.Listen(8));
    Address peer = Ip("127.0.0.1", 42);
    const auto old_peer = peer.ToString();
    std::error_code error;
    CHECK(listener.Accept(peer, error).Fd() == -1 && WouldBlock(error));
    CHECK(peer.ToString() == old_peer);
    const auto bound = listener.LocalAddress(error);
    CHECK(!error && bound.Valid());
    if (address.Family() != AddressFamily::Unix) { CHECK(bound.Port() != 0); }
    auto client = Open(address.Family(), SocketType::Stream);
    CHECK(!client.RemoteAddress(error).Valid() && error);
    error = client.Connect(bound);
    CHECK(!error || error.value() == EINPROGRESS);
    std::cout << "connect " << bound.ToString() << " result=" << error.value() << '\n';
    if (address.Family() != AddressFamily::Unix) { CHECK(error.value() == EINPROGRESS); }
    Ready(client.Fd(), POLLOUT);
    CHECK(!client.PendingError());
    Ready(listener.Fd(), POLLIN);
    auto server = listener.Accept(peer, error);
    CHECK(!error && server.Fd() >= 0);
    CHECK((::fcntl(server.Fd(), F_GETFL) & O_NONBLOCK) != 0);
    CHECK((::fcntl(server.Fd(), F_GETFD) & FD_CLOEXEC) != 0);
    CHECK(client.RemoteAddress(error).Valid() && !error);
    CHECK(!listener.Close());
    return {std::move(client), std::move(server)};
}

static std::vector<char> ReceiveExact(NativeSocket& socket, std::size_t size) {
    std::vector<char> received(size);
    std::size_t position = 0;
    const auto until = std::chrono::steady_clock::now() + 10s;
    while (position < size) {
        CHECK(std::chrono::steady_clock::now() < until);
        auto part = socket.Receive(received.data() + position, size - position);
        if (WouldBlock(part.error)) { Ready(socket.Fd(), POLLIN); continue; }
        CHECK(!part.error && !part.eof && part.bytes > 0);
        position += part.bytes;
    }
    return received;
}

static void StreamRoundtrip(const Address& address, OwnedPath* path = nullptr) {
    auto pair = ConnectPair(address, path);
    char byte = 0;
    CHECK(WouldBlock(pair.server.Receive(&byte, 1).error));
    const ConstBuffer buffers[]{{"hello", 5}, {"-net", 4}};
    const auto sent = pair.client.Sendv(buffers, 2);
    CHECK(!sent.error && sent.bytes == 9);
    const auto received = ReceiveExact(pair.server, 9);
    CHECK(std::string(received.data(), received.size()) == "hello-net");
    const auto reply = pair.server.Send(received.data(), received.size());
    CHECK(!reply.error && reply.bytes == received.size());
    CHECK(ReceiveExact(pair.client, 9) == received);
    CHECK(!pair.client.Shutdown(SHUT_WR));
    Ready(pair.server.Fd(), POLLIN);
    const auto eof = pair.server.Receive(&byte, 1);
    CHECK(!eof.error && eof.eof && eof.bytes == 0);
    CHECK(pair.client.Send("x", 1).error.value() == EPIPE); // NOSIGNAL：不能终止测试进程。
    CHECK(!pair.client.Close() && !pair.server.Close());
    std::cout << "stream roundtrip + EOF + NOSIGNAL passed\n";
}

static void DatagramRoundtrip(const Address& first, const Address& second) {
    auto a = Open(first.Family(), SocketType::Datagram);
    auto b = Open(second.Family(), SocketType::Datagram);
    if (first.Family() == AddressFamily::IPv6) {
        CHECK(!a.SetIpv6Only(true) && !b.SetIpv6Only(true));
        CHECK(GetIntOption(a, IPPROTO_IPV6, IPV6_V6ONLY) == 1);
    }
    CHECK(!a.Bind(first) && !b.Bind(second));
    std::error_code error;
    const auto a_bound = a.LocalAddress(error);
    CHECK(!error);
    const auto b_bound = b.LocalAddress(error);
    CHECK(!error);
    char data[16]{};
    Address source;
    CHECK(WouldBlock(b.ReceiveFrom(data, sizeof(data), source).error));
    const auto sent = a.SendTo("data", 4, b_bound);
    CHECK(!sent.error && sent.bytes == 4);
    Ready(b.Fd(), POLLIN);
    const auto received = b.ReceiveFrom(data, sizeof(data), source);
    CHECK(!received.error && !received.truncated && !received.eof && received.bytes == 4);
    CHECK(source.ToString() == a_bound.ToString());
    CHECK(std::memcmp(data, "data", 4) == 0);
    const auto replied = b.SendTo(data, 4, source);
    CHECK(!replied.error && replied.bytes == 4);
    Ready(a.Fd(), POLLIN);
    const auto echoed = a.Receive(data, sizeof(data));
    CHECK(!echoed.error && echoed.bytes == 4 && std::memcmp(data, "data", 4) == 0);
    CHECK(!a.Connect(b_bound));
    CHECK(!a.Send("truncate", 8).error);
    Ready(b.Fd(), POLLIN);
    const auto truncated = b.ReceiveFrom(data, 2, source);
    CHECK(!truncated.error && truncated.truncated && truncated.bytes == 2);
    CHECK(!a.Send(nullptr, 0).error);
    Ready(b.Fd(), POLLIN);
    const auto empty = b.ReceiveFrom(data, sizeof(data), source);
    CHECK(!empty.error && !empty.eof && !empty.truncated && empty.bytes == 0);
    CHECK(!a.Close() && !b.Close());
    std::cout << "datagram " << b_bound.ToString() << " roundtrip/truncation/zero passed\n";
}

static void PartialAndErrors() {
    auto pair = ConnectPair(Ip("127.0.0.1"));
    inject_send_eintr = true;
    CHECK(pair.client.Send("x", 1).error.value() == EINTR);
    CHECK(!inject_send_eintr);
    char unused = 0;
    CHECK(WouldBlock(pair.server.Receive(&unused, 1).error));
    CHECK(!pair.client.SetWriteBuffer(4096));
    std::vector<char> payload(2 * 1024 * 1024, 'x');
    const auto first = pair.client.Send(payload.data(), payload.size());
    CHECK(!first.error && first.bytes > 0 && first.bytes < payload.size());
    std::size_t total = first.bytes;
    bool would_block = false;
    for (int i = 0; i < 64; ++i) {
        auto sent = pair.client.Send(payload.data(), payload.size());
        if (WouldBlock(sent.error)) { would_block = true; break; }
        CHECK(!sent.error && sent.bytes > 0);
        total += sent.bytes;
        CHECK(total <= 8 * 1024 * 1024);
    }
    CHECK(would_block);
    const auto received = ReceiveExact(pair.server, total);
    CHECK(std::all_of(received.begin(), received.end(), [](char value) { return value == 'x'; }));
    CHECK(!pair.client.Close() && !pair.server.Close());
    std::cout << "partial=" << first.bytes << " queued before EAGAIN=" << total << "; injected EINTR preserved\n";
    // 自有动态端口：绑定但不 listen，绝不依赖外部不可达服务或固定端口。
    auto not_listening = Open(AddressFamily::IPv4, SocketType::Stream);
    CHECK(!not_listening.Bind(Ip("127.0.0.1")));
    std::error_code error;
    const auto bound = not_listening.LocalAddress(error);
    CHECK(!error);
    auto failing = Open(AddressFamily::IPv4, SocketType::Stream);
    error = failing.Connect(bound);
    CHECK(error.value() == EINPROGRESS || error.value() == ECONNREFUSED);
    if (error.value() == EINPROGRESS) {
        Ready(failing.Fd(), POLLOUT);
        CHECK(failing.PendingError().value() == ECONNREFUSED);
    }
    auto duplicate_bind = Open(AddressFamily::IPv4, SocketType::Stream);
    CHECK(duplicate_bind.Bind(bound).value() == EADDRINUSE);
    CHECK(!failing.Close() && !not_listening.Close() && !duplicate_bind.Close());
}

static void Sockets() {
    const auto before = FdCount();
    StreamRoundtrip(Ip("127.0.0.1"));
    StreamRoundtrip(Ip("::1"));
    {
        OwnedPath path{"foundation-" + std::to_string(::getpid()) + ".sock"};
        StreamRoundtrip(Unix(path.path), &path);
    }
    StreamRoundtrip(Unix("foundation-abstract-" + std::to_string(::getpid()), true));
    DatagramRoundtrip(Ip("127.0.0.1"), Ip("127.0.0.1"));
    DatagramRoundtrip(Ip("::1"), Ip("::1"));
    DatagramRoundtrip(Unix("foundation-dgram-a-" + std::to_string(::getpid()), true),
        Unix("foundation-dgram-b-" + std::to_string(::getpid()), true));
    PartialAndErrors();
    CHECK(FdCount() == before);
    CHECK(std::filesystem::is_empty(std::filesystem::current_path()));
    std::cout << "socket fd baseline=" << before << " restored; Unix paths removed\n";
}

class ProbeObject : public ICoObject {
public:
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override { return {}; }
};

static void Context() {
    static_assert(std::is_base_of_v<bbt::coroutine::ICoObject, ICoObject>);
    static_assert(std::is_base_of_v<ICoObject, CoObject>);
    static_assert(!std::is_copy_constructible_v<NativeSocket>);
    CHECK(!g_scheduler->IsInitialized());
    ProbeObject probe;
    CHECK(!probe.GetEnvironment().CanSuspend());
    int effects = 0;
    if (!probe.RequireCoroutine()) { ++effects; }
    CHECK(effects == 0);
    CHECK(!g_scheduler->IsInitialized());
    bool rejected_identity = false;
    try { CoObject too_early("test"); } catch (const std::logic_error&) { rejected_identity = true; }
    CHECK(rejected_identity);
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start();
    auto object = std::make_shared<CoObject>("test", "created-on-main");
    auto second = std::make_shared<CoObject>("test", "second");
    CHECK(object->GetObjectInfo().id != second->GetObjectInfo().id);
    CHECK(!object->GetEnvironment().CanSuspend());
    CHECK(object->RequireCoroutine() == std::errc::operation_not_permitted);
    auto promise = std::make_shared<std::promise<std::uint64_t>>();
    auto future = promise->get_future();
    g_scheduler->RegistCoroutineTask(65536, [object, promise] {
        try {
            const auto environment = object->GetEnvironment();
            CHECK(environment.CanSuspend());
            CHECK(!object->RequireCoroutine());
            for (int i = 0; i < 8; ++i) {
                bbtco_yield;
                CHECK(object->GetEnvironment().coroutine_id == environment.coroutine_id);
                CHECK(!object->RequireCoroutine());
            }
            promise->set_value(environment.coroutine_id);
        } catch (...) { promise->set_exception(std::current_exception()); }
    }, "net.context");
    CHECK(future.wait_for(5s) == std::future_status::ready);
    CHECK(future.get() != 0);
    CHECK(!object->GetEnvironment().CanSuspend());
    CHECK(object->GetObjectInfo().name == "created-on-main");
    std::cout << "context: ordinary rejected before effects, real managed accepted across yield; main-created identity\n";
}

static bool ContextNoCreate(bool upstream_only = false) {
    // 没启动 worker，本进程没有其他 Processer 创建者。id 是上游单调原子计数。
    CHECK(!g_scheduler->IsInitialized());
    const auto before_ordinary = bbt::coroutine::detail::Processer::Create()->GetId();
    ProbeObject object;
    CHECK((upstream_only ? bbt::coroutine::GetLocalCoroutineId() : object.GetEnvironment().coroutine_id) == 0);
    const auto after_ordinary = bbt::coroutine::detail::Processer::Create()->GetId();
    CHECK(after_ordinary == before_ordinary + 1);
    const auto before_enabled = bbt::coroutine::detail::Processer::Create()->GetId();
    std::exception_ptr thread_error;
    std::thread enabled_idle([&object, upstream_only, &thread_error] {
        try {
            bbt::coroutine::detail::LocalThread::GetTLSInst()->SetEnableUseCo(true);
            CHECK((upstream_only ? bbt::coroutine::GetLocalCoroutineId() : object.GetEnvironment().coroutine_id) == 0);
            if (!upstream_only) { CHECK(object.RequireCoroutine() == std::errc::operation_not_permitted); }
        } catch (...) { thread_error = std::current_exception(); }
    });
    enabled_idle.join();
    if (thread_error) { std::rethrow_exception(thread_error); }
    const auto after_enabled = bbt::coroutine::detail::Processer::Create()->GetId();
    CHECK(!g_scheduler->IsInitialized());
    std::cout << "ordinary delta=" << after_ordinary - before_ordinary
        << "; TLS-enabled idle delta=" << after_enabled - before_enabled
        << "; expected both 1 (only explicit post-query probe creates)\n";
    return after_enabled == before_enabled + 1;
}

static void NativeAcceptConnect() {
    //accept4/connect 在 Linux 原生 primitive 边界走 SYS_accept4/SYS_connect，
    // 不经 coroutine Hook。受管协程内空 listener 必须立即 EAGAIN 且不让出执行权；
    // Connect 保留真实 EINPROGRESS pending（受管返回 0 即旧 Hook 缺陷回归）。
    const auto before = FdCount();
    auto listener = Open(AddressFamily::IPv4, SocketType::Stream);
    CHECK(!listener.Bind(Ip("127.0.0.1")));
    CHECK(!listener.Listen(8));
    std::error_code error;
    const auto bound = listener.LocalAddress(error);
    CHECK(!error && bound.Valid());
    Address peer = Ip("127.0.0.1", 42);
    const auto ordinary_started = std::chrono::steady_clock::now();
    CHECK(listener.Accept(peer, error).Fd() == -1 && error.value() == EAGAIN);
    CHECK(std::chrono::steady_clock::now() - ordinary_started < 200ms);
    static std::atomic_bool probe_ran{false};
    g_bbt_coroutine_config->m_cfg_static_thread_num = 1; // 单 Processer：Accept 让位才会轮到后注册任务
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start();
    auto client_go = std::make_shared<std::promise<void>>();
    auto accepted_promise = std::make_shared<std::promise<int>>();
    auto probe_promise = std::make_shared<std::promise<bool>>();
    g_scheduler->RegistCoroutineTask(65536, [&, client_go, accepted_promise] {
        try {
            std::error_code inner;
            Address inner_peer = Ip("127.0.0.1", 42);
            const auto empty_started = std::chrono::steady_clock::now();
            // TLS 受管许可下空 listener：不设 SO_RCVTIMEO 也必须立即 EAGAIN（旧 Hook 会无限等）。
            CHECK(listener.Accept(inner_peer, inner).Fd() == -1 && inner.value() == EAGAIN);
            CHECK(std::chrono::steady_clock::now() - empty_started < 200ms);
            CHECK(!probe_ran); // 任务顺序证明：Accept 未让位（否则后注册 probe 已先运行）
            client_go->set_value();
            NativeSocket accepted;
            for (int i = 0; i < 200 && accepted.Fd() < 0; ++i) {
                accepted = listener.Accept(inner_peer, inner);
                if (accepted.Fd() < 0) {
                    CHECK(inner.value() == EAGAIN);
                    bbtco_yield;
                }
            }
            CHECK(accepted.Fd() >= 0);
            CHECK((::fcntl(accepted.Fd(), F_GETFL) & O_NONBLOCK) != 0);
            CHECK((::fcntl(accepted.Fd(), F_GETFD) & FD_CLOEXEC) != 0);
            // 受管 Connect → 真实 listener：pending 可见，readiness 后真正完成。
            auto connector = Open(AddressFamily::IPv4, SocketType::Stream);
            auto completed = Open(AddressFamily::IPv4, SocketType::Stream);
            CHECK(!completed.Bind(Ip("127.0.0.1")));
            CHECK(!completed.Listen(8));
            const auto completed_bound = completed.LocalAddress(inner);
            CHECK(!inner);
            const auto connected = connector.Connect(completed_bound);
            CHECK(!connected || connected.value() == EINPROGRESS);
            for (int i = 0; i < 200; ++i) {
                pollfd ready{connector.Fd(), POLLOUT, 0};
                if (::poll(&ready, 1, 0) == 1) { break; }
                bbtco_yield;
            }
            CHECK(!connector.PendingError());
            // 受管 Connect → 未监听端口：保持真实 pending，readiness 后 ECONNREFUSED；
            // 旧 Hook 缺陷在此返回 0。
            auto silent = Open(AddressFamily::IPv4, SocketType::Stream);
            CHECK(!silent.Bind(Ip("127.0.0.1")));
            const auto silent_bound = silent.LocalAddress(inner);
            CHECK(!inner);
            auto failing = Open(AddressFamily::IPv4, SocketType::Stream);
            const auto refused_connect = failing.Connect(silent_bound);
            CHECK(refused_connect.value() == EINPROGRESS || refused_connect.value() == ECONNREFUSED);
            if (refused_connect.value() == EINPROGRESS) {
                bool refused = false;
                for (int i = 0; i < 200 && !refused; ++i) {
                    pollfd ready{failing.Fd(), POLLOUT, 0};
                    if (::poll(&ready, 1, 0) == 1) {
                        CHECK(failing.PendingError().value() == ECONNREFUSED);
                        refused = true;
                    } else {
                        bbtco_yield;
                    }
                }
                CHECK(refused);
            }
            CHECK(!accepted.Close() && !connector.Close() && !completed.Close());
            CHECK(!silent.Close() && !failing.Close());
            accepted_promise->set_value(0);
        } catch (...) { accepted_promise->set_exception(std::current_exception()); }
    }, "net.native_accept_connect");
    g_scheduler->RegistCoroutineTask(65536, [probe_promise] {
        probe_ran = true;
        probe_promise->set_value(true);
    }, "net.native_accept_probe");
    auto accepted_future = accepted_promise->get_future();
    auto probe_future = probe_promise->get_future();
    std::exception_ptr client_error;
    std::thread client([&client_error, client_go, bound] {
        try {
            CHECK(client_go->get_future().wait_for(5s) == std::future_status::ready);
            std::error_code inner;
            auto pair_client = Open(AddressFamily::IPv4, SocketType::Stream);
            const auto connect_error = pair_client.Connect(bound);
            CHECK(!connect_error || connect_error.value() == EINPROGRESS);
            if (connect_error.value() == EINPROGRESS) {
                pollfd ready{pair_client.Fd(), POLLOUT, 0};
                CHECK(::poll(&ready, 1, 2000) == 1);
                CHECK(!pair_client.PendingError());
            }
            CHECK(!pair_client.Close());
        } catch (...) { client_error = std::current_exception(); }
    });
    // 任何等待/传播路径先 join：joinable thread 析构 terminate 会掩盖真实断言。
    try {
        CHECK(accepted_future.wait_for(10s) == std::future_status::ready);
        CHECK(accepted_future.get() == 0);
        CHECK(probe_future.wait_for(10s) == std::future_status::ready);
        CHECK(probe_future.get());
    } catch (...) {
        client.join();
        throw;
    }
    client.join();
    if (client_error) { std::rethrow_exception(client_error); }
    CHECK(!listener.Close());
    CHECK(FdCount() == before);
    std::cout << "native accept/connect: managed empty-listener EAGAIN no-yield; real EINPROGRESS/refused preserved\n";
}

static void FlagsContract() {
    //flags 只承诺保守 allowlist；未列出位在任何系统调用前拒绝（bytes=0，
    // 不发出/不消费报文），数据报 TRUNC 返回值只计拷贝字节。
    auto pair = ConnectPair(Ip("127.0.0.1"));
    CHECK(pair.client.Send("x", 1, 0x80000000).error == std::errc::operation_not_supported);
    CHECK(pair.client.Send("x", 1, MSG_OOB).error == std::errc::operation_not_supported);
    CHECK(pair.client.Send("x", 1, MSG_MORE).error == std::errc::operation_not_supported);
    const ConstBuffer buffers[]{{"v", 1}};
    CHECK(pair.client.Sendv(buffers, 1, MSG_ZEROCOPY).error == std::errc::operation_not_supported);
    char byte = 0;
    CHECK(WouldBlock(pair.server.Receive(&byte, 1).error)); // 被拒 send 未发出任何数据
    CHECK(!pair.client.Send("abc", 3).error);
    Ready(pair.server.Fd(), POLLIN);
    CHECK(pair.server.Receive(&byte, 1, MSG_TRUNC).error == std::errc::operation_not_supported);
    CHECK(pair.server.Receive(&byte, 1, MSG_WAITALL).error == std::errc::operation_not_supported);
    CHECK(pair.server.Receive(&byte, 1, 0x80000000).error == std::errc::operation_not_supported);
    const auto peeked = pair.server.Receive(&byte, 1, MSG_PEEK);
    CHECK(!peeked.error && peeked.bytes == 1 && byte == 'a');
    CHECK(!pair.server.Receive(&byte, 1).error && byte == 'a');
    CHECK(!pair.client.Close() && !pair.server.Close());

    auto a = Open(AddressFamily::IPv4, SocketType::Datagram);
    auto b = Open(AddressFamily::IPv4, SocketType::Datagram);
    CHECK(!a.Bind(Ip("127.0.0.1")) && !b.Bind(Ip("127.0.0.1")));
    std::error_code error;
    const auto a_bound = a.LocalAddress(error);
    CHECK(!error);
    const auto b_bound = b.LocalAddress(error);
    CHECK(!error);
    Address source;
    char data[8]{};
    CHECK(a.SendTo("truncate-me", 11, b_bound, MSG_ZEROCOPY).error == std::errc::operation_not_supported);
    CHECK(a.SendTo("truncate-me", 11, b_bound, MSG_TRUNC).error == std::errc::operation_not_supported);
    CHECK(WouldBlock(b.ReceiveFrom(data, sizeof(data), source).error)); // 被拒 SendTo 未发出
    CHECK(!a.SendTo("truncate-me-long", 16, b_bound, MSG_NOSIGNAL).error); // NOSIGNAL 用户可传
    Ready(b.Fd(), POLLIN);
    const auto truncated = b.ReceiveFrom(data, 4, source, MSG_TRUNC);
    CHECK(!truncated.error && truncated.truncated && truncated.bytes == 4); // bytes 只计拷贝
    CHECK(std::memcmp(data, "trun", 4) == 0);
    CHECK(!a.SendTo("keep", 4, b_bound).error);
    Ready(b.Fd(), POLLIN);
    CHECK(b.ReceiveFrom(data, 4, source, 0x80000000).error == std::errc::operation_not_supported);
    const auto kept = b.ReceiveFrom(data, 4, source);
    CHECK(!kept.error && kept.bytes == 4 && std::memcmp(data, "keep", 4) == 0); // 拒绝未消费报文
    CHECK(!a.SendTo("peek", 4, b_bound).error);
    Ready(b.Fd(), POLLIN);
    const auto peek_one = b.ReceiveFrom(data, 4, source, MSG_PEEK);
    CHECK(!peek_one.error && peek_one.bytes == 4);
    const auto peek_twice = b.ReceiveFrom(data, 4, source, MSG_PEEK);
    CHECK(!peek_twice.error && peek_twice.bytes == 4 && std::memcmp(data, "peek", 4) == 0);
    CHECK(!a.Close() && !b.Close());
    std::cout << "flags contract: allowlist rejected before syscall, PEEK keeps, datagram TRUNC counts copied bytes\n";
}

#include "ProcessRuntimeTestMain.hpp"

static int RunFoundationCases(int argc, char** argv) {
    try {
        CHECK(argc == 2);
        const std::string selected = argv[1];
        if (selected == "address_ownership") { AddressAndOwnership(); }
        else if (selected == "socket") { Sockets(); }
        else if (selected == "context") { Context(); }
        else if (selected == "native_accept_connect") { NativeAcceptConnect(); }
        else if (selected == "flags_contract") { FlagsContract(); }
        else if (selected == "context_no_create" || selected == "upstream_query_no_create") {
            if (!ContextNoCreate(selected == "upstream_query_no_create")) {
                std::cerr << "BLOCKED: fixed public GetLocalCoroutineId creates Processer in TLS-enabled idle context\n";
                return 1;
            }
        } else { throw std::invalid_argument("unknown test selection"); }
        std::cout << "PASS " << selected << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL " << error.what() << '\n';
        return 1;
    }
}

int main(int argc, char** argv) {
    // 测试进程退出边界：局部 RAII 收口 → 刷新输出 → _Exit，见
    // ProcessRuntimeTestMain.hpp 头注（静态析构竞态为已知限制，未修复）。
    return bbt_net_test::RunTestProcessExit([&] { return RunFoundationCases(argc, argv); });
}
