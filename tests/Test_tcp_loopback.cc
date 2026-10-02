#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>
#include <bbt/infra/CoTCP.hpp>

// 新契约（进程寿命运行时）：没有 Scheduler::Stop/restart，测试不再做
// Start→Stop→Start 隔离；每个测试可执行文件只初始化一次 runtime，用例之间
// 靠进程边界隔离。每个用例结束前显式 Close() 并断言物理收口（IsClosed）。
using bbt::infra::CallOptions;
using bbt::infra::ErrorCode;
using bbt::infra::tcp::CoTCP;
using bbt::infra::tcp::CoTCPListener;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

// 运行时只初始化一次（函数局部静态保证线程安全且恰好一次）。
void EnsureRuntime() {
    static const bool initialized = [] {
        auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
        if (!scheduler->IsInitialized())
            scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
        return scheduler->IsInitialized();
    }();
    BOOST_REQUIRE(initialized);
}

CallOptions Options(int timeout_ms = 5000) {
    CallOptions options;
    options.deadline = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(timeout_ms);
    return options;
}

// 协程与断言共享的观测点：堆分配，断言失败展开用例栈后协程写入仍合法。
struct LoopProbe {
    bbt::core::thread::CountDownLatch done{2};
    std::atomic_bool server_ok{false};
    std::atomic_bool client_ok{false};
    CoTCP::SPtr accepted;
    CoTCP::SPtr client;
};

int ConnectRawPeer(std::uint16_t port) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = ::htons(port);
    if (::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) != 1 ||
        ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return -1;
    }
    return fd;
}

// 协程与断言共享的连接句柄：堆持有，用例栈展开后协程写入仍合法。
struct ConnHolder {
    CoTCP::SPtr conn;
};

} // namespace

BOOST_AUTO_TEST_CASE(t_tcp_real_loopback) {
    EnsureRuntime();

    auto listener = CoTCPListener::ListenTCP("127.0.0.1", 0);
    BOOST_REQUIRE(listener);
    const auto port = listener.value()->LocalAddress().port;
    BOOST_REQUIRE(port != 0);

    auto probe = std::make_shared<LoopProbe>();

    bbtco [listener, probe]() {
        auto accepted = listener.value()->Accept(Options());
        if (!accepted) {
            probe->done.Down();
            return;
        }
        char request[5]{};
        auto read = accepted.value()->ReadSome(
            bbt::infra::MutableBytes{request, sizeof(request)}, Options());
        if (!read || read.value().bytes != 5 ||
            std::string(request, 5) != "hello") {
            probe->done.Down();
            return;
        }
        const char response[] = "world";
        auto write = accepted.value()->WriteAll(
            bbt::infra::ConstBytes{response, 5}, Options());
        probe->server_ok.store(write && write.value().bytes == 5);
        probe->accepted = accepted.value();
        probe->accepted->Close();
        probe->done.Down();
    };

    bbtco [port, probe]() {
        auto client = CoTCP::DialTCP("127.0.0.1", port, Options());
        if (!client) {
            probe->done.Down();
            return;
        }
        const char request[] = "hello";
        auto write = client.value()->WriteAll(
            bbt::infra::ConstBytes{request, 5}, Options());
        char response[5]{};
        auto read = client.value()->ReadSome(
            bbt::infra::MutableBytes{response, sizeof(response)}, Options());
        probe->client_ok.store(write && write.value().bytes == 5 && read &&
                               read.value().bytes == 5 &&
                               std::string(response, 5) == "world");
        probe->client = client.value();
        probe->client->Close();
        probe->done.Down();
    };

    BOOST_REQUIRE_EQUAL(probe->done.WaitTimeout(10000), 0);
    BOOST_CHECK(probe->server_ok.load());
    BOOST_CHECK(probe->client_ok.load());
    BOOST_CHECK_EQUAL(listener.value()->GetObjectInfo().kind, "tcp");

    // 显式收口并断言物理收口（Close 返回即资源已释放）。
    BOOST_REQUIRE(probe->accepted);
    BOOST_REQUIRE(probe->client);
    BOOST_CHECK(probe->accepted->IsClosed());
    BOOST_CHECK(probe->client->IsClosed());
    listener.value()->Close();
    BOOST_CHECK(listener.value()->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_tcp_accept_deadline) {
    EnsureRuntime();

    auto listener = CoTCPListener::ListenTCP("127.0.0.1", 0);
    BOOST_REQUIRE(listener);
    bbt::core::thread::CountDownLatch done{1};
    std::atomic_int code{-1};
    bbtco [listener, &done, &code]() {
        auto accepted = listener.value()->Accept(Options(80));
        code.store(accepted ? -2 : static_cast<int>(accepted.error().code));
        done.Down();
    };

    const int wait = done.WaitTimeout(1500);
    listener.value()->Close();
    BOOST_CHECK_EQUAL(wait, 0);
    BOOST_CHECK_EQUAL(code.load(), static_cast<int>(ErrorCode::TimedOut));
    BOOST_CHECK(listener.value()->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_tcp_read_deadline) {
    EnsureRuntime();

    auto listener = CoTCPListener::ListenTCP("127.0.0.1", 0);
    BOOST_REQUIRE(listener);
    const auto port = listener.value()->LocalAddress().port;
    BOOST_REQUIRE(port != 0);

    const int peer = ConnectRawPeer(port);
    BOOST_REQUIRE(peer >= 0);

    bbt::core::thread::CountDownLatch done{1};
    std::atomic_int code{-1};
    auto holder = std::make_shared<ConnHolder>();
    bbtco [listener, &done, &code, holder]() {
        auto accepted = listener.value()->Accept(Options(500));
        if (accepted) {
            holder->conn = accepted.value();
            char byte{};
            auto read = accepted.value()->ReadSome(
                bbt::infra::MutableBytes{&byte, 1}, Options(80));
            code.store(read ? -2 : static_cast<int>(read.error().code));
        }
        done.Down();
    };

    const int wait = done.WaitTimeout(1500);
    ::close(peer);
    listener.value()->Close();
    BOOST_CHECK_EQUAL(wait, 0);
    BOOST_CHECK_EQUAL(code.load(), static_cast<int>(ErrorCode::TimedOut));
    BOOST_REQUIRE(holder->conn);
    holder->conn->Close();
    BOOST_CHECK(holder->conn->IsClosed());
    BOOST_CHECK(listener.value()->IsClosed());
}

// Close 唤醒在途 Accept：跨线程 Close 先封口并唤醒等待者，Close 返回即物理收口。
BOOST_AUTO_TEST_CASE(t_tcp_close_wakes_accept) {
    EnsureRuntime();

    auto listener = CoTCPListener::ListenTCP("127.0.0.1", 0);
    BOOST_REQUIRE(listener);
    bbt::core::thread::CountDownLatch done{1};
    std::atomic_int code{-1};
    bbtco [listener, &done, &code]() {
        auto accepted = listener.value()->Accept(Options(5000));
        code.store(accepted ? -2 : static_cast<int>(accepted.error().code));
        done.Down();
    };
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    listener.value()->Close();
    const int wait = done.WaitTimeout(1500);
    BOOST_CHECK_EQUAL(wait, 0);
    BOOST_CHECK_EQUAL(code.load(), static_cast<int>(ErrorCode::Closed));
    BOOST_CHECK(listener.value()->IsClosed());
}

// Close 唤醒在途 ReadSome：挂起的数据路径 op 走关闭终态，且 Close 的有界
// 排空在 op 退出后才物理释放。
BOOST_AUTO_TEST_CASE(t_tcp_close_wakes_read) {
    EnsureRuntime();

    auto listener = CoTCPListener::ListenTCP("127.0.0.1", 0);
    BOOST_REQUIRE(listener);
    const auto port = listener.value()->LocalAddress().port;
    BOOST_REQUIRE(port != 0);
    const int peer = ConnectRawPeer(port);
    BOOST_REQUIRE(peer >= 0);

    bbt::core::thread::CountDownLatch accepted_done{1};
    bbt::core::thread::CountDownLatch read_done{1};
    std::atomic_int code{-1};
    auto holder = std::make_shared<ConnHolder>();
    bbtco [listener, holder, &accepted_done, &read_done, &code]() {
        auto accepted = listener.value()->Accept(Options());
        if (!accepted) { accepted_done.Down(); read_done.Down(); return; }
        holder->conn = accepted.value();
        accepted_done.Down();
        char byte{};
        auto read = holder->conn->ReadSome(
            bbt::infra::MutableBytes{&byte, 1}, Options(5000));
        code.store(read ? -2 : static_cast<int>(read.error().code));
        read_done.Down();
    };
    BOOST_REQUIRE_EQUAL(accepted_done.WaitTimeout(1500), 0);
    BOOST_REQUIRE(holder->conn);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    holder->conn->Close();
    BOOST_CHECK(holder->conn->IsClosed());
    const int wait = read_done.WaitTimeout(1500);
    ::close(peer);
    listener.value()->Close();
    BOOST_CHECK_EQUAL(wait, 0);
    BOOST_CHECK_EQUAL(code.load(), static_cast<int>(ErrorCode::Closed));
    BOOST_CHECK(listener.value()->IsClosed());
}

BOOST_AUTO_TEST_CASE(t_tcp_try_and_eof) {
    EnsureRuntime();

    auto listener = CoTCPListener::ListenTCP("127.0.0.1", 0);
    BOOST_REQUIRE(listener);
    const auto port = listener.value()->LocalAddress().port;
    BOOST_REQUIRE(port != 0);
    const int peer = ConnectRawPeer(port);
    BOOST_REQUIRE(peer >= 0);

    bbt::core::thread::CountDownLatch done{1};
    bbt::core::thread::CountDownLatch ready{1};
    std::atomic_bool ok{false};
    auto holder = std::make_shared<ConnHolder>();
    bbtco [listener, holder, &ready, &done, &ok]() {
        auto accepted = listener.value()->Accept(Options());
        if (accepted) {
            holder->conn = accepted.value();
            char byte{};
            auto empty = accepted.value()->TryReadSome(
                bbt::infra::MutableBytes{&byte, 1});
            const bool would_block =
                empty && empty.value().state == bbt::infra::IoState::WouldBlock;
            ready.Down();
            auto eof = accepted.value()->ReadSome(
                bbt::infra::MutableBytes{&byte, 1}, Options());
            auto zero = accepted.value()->TryReadSome(
                bbt::infra::MutableBytes{nullptr, 0});
            ok.store(would_block &&
                     eof && eof.value().state == bbt::infra::IoState::Eof &&
                     eof.value().bytes == 0 && zero &&
                     zero.value().state == bbt::infra::IoState::Ok);
        } else ready.Down();
        done.Down();
    };
    BOOST_REQUIRE_EQUAL(ready.WaitTimeout(1500), 0);
    ::shutdown(peer, SHUT_WR);
    const int wait = done.WaitTimeout(1500);
    ::close(peer);
    listener.value()->Close();
    BOOST_CHECK_EQUAL(wait, 0);
    BOOST_CHECK(ok.load());
    BOOST_REQUIRE(holder->conn);
    holder->conn->Close();
    BOOST_CHECK(holder->conn->IsClosed());
    BOOST_CHECK(listener.value()->IsClosed());
}
