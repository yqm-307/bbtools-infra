#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
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

using bbt::infra::CallOptions;
using bbt::infra::ErrorCode;
using bbt::infra::tcp::CoTCP;
using bbt::infra::tcp::CoTCPListener;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

CallOptions Options(int timeout_ms = 5000) {
    CallOptions options;
    options.deadline = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(timeout_ms);
    return options;
}

BOOST_AUTO_TEST_CASE(t_tcp_real_loopback) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

    auto listener = CoTCPListener::ListenTCP("127.0.0.1", 0);
    BOOST_REQUIRE(listener);
    const auto port = listener.value()->LocalAddress().port;
    BOOST_REQUIRE(port != 0);

    bbt::core::thread::CountDownLatch done{2};
    std::atomic_bool server_ok{false};
    std::atomic_bool client_ok{false};

    bbtco [listener, &server_ok, &done]() {
        auto accepted = listener.value()->Accept(Options());
        if (!accepted) {
            done.Down();
            return;
        }
        char request[5]{};
        auto read = accepted.value()->ReadSome(request, sizeof(request), Options());
        if (!read || read.value() != 5 || std::string(request, 5) != "hello") {
            done.Down();
            return;
        }
        const char response[] = "world";
        auto write = accepted.value()->WriteAll(response, 5, Options());
        server_ok.store(write && write.value() == 5);
        accepted.value()->RequestClose();
        done.Down();
    };

    bbtco [port, &client_ok, &done]() {
        auto client = CoTCP::DialTCP("127.0.0.1", port, Options());
        if (!client) {
            done.Down();
            return;
        }
        const char request[] = "hello";
        auto write = client.value()->WriteAll(request, 5, Options());
        char response[5]{};
        auto read = client.value()->ReadSome(response, sizeof(response), Options());
        client_ok.store(write && write.value() == 5 && read && read.value() == 5 &&
                        std::string(response, 5) == "world");
        client.value()->RequestClose();
        done.Down();
    };

    BOOST_REQUIRE_EQUAL(done.WaitTimeout(10000), 0);
    BOOST_CHECK(server_ok.load());
    BOOST_CHECK(client_ok.load());
    BOOST_CHECK_EQUAL(listener.value()->GetObjectInfo().kind, "tcp");
    listener.value()->RequestClose();
    BOOST_CHECK(listener.value()->IsClosed());
    BOOST_CHECK(listener.value()->WaitClosed(Options().deadline, {}) ==
                bbt::infra::CloseStatus::InvalidContext);
    scheduler->Stop();
}

BOOST_AUTO_TEST_CASE(t_tcp_accept_deadline) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());

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
    listener.value()->RequestClose();
    scheduler->Stop();
    BOOST_CHECK_EQUAL(wait, 0);
    BOOST_CHECK_EQUAL(code.load(), static_cast<int>(ErrorCode::TimedOut));
}

BOOST_AUTO_TEST_CASE(t_tcp_read_deadline) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());
    auto listener = CoTCPListener::ListenTCP("127.0.0.1", 0);
    BOOST_REQUIRE(listener);
    const auto port = listener.value()->LocalAddress().port;
    BOOST_REQUIRE(port != 0);

    const int peer = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(peer >= 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    BOOST_REQUIRE_EQUAL(::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr), 1);
    BOOST_REQUIRE_EQUAL(::connect(peer, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);

    bbt::core::thread::CountDownLatch done{1};
    std::atomic_int code{-1};
    bbtco [listener, &done, &code]() {
        auto accepted = listener.value()->Accept(Options(500));
        if (accepted) {
            char byte{};
            auto read = accepted.value()->ReadSome(&byte, 1, Options(80));
            code.store(read ? -2 : static_cast<int>(read.error().code));
            accepted.value()->RequestClose();
        }
        done.Down();
    };

    const int wait = done.WaitTimeout(1500);
    ::close(peer);
    listener.value()->RequestClose();
    scheduler->Stop();
    BOOST_CHECK_EQUAL(wait, 0);
    BOOST_CHECK_EQUAL(code.load(), static_cast<int>(ErrorCode::TimedOut));
}

BOOST_AUTO_TEST_CASE(t_tcp_close_wakes_accept) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());
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
    listener.value()->RequestClose();
    const int wait = done.WaitTimeout(1500);
    scheduler->Stop();
    BOOST_CHECK_EQUAL(wait, 0);
    BOOST_CHECK_EQUAL(code.load(), static_cast<int>(ErrorCode::Closed));
}

BOOST_AUTO_TEST_CASE(t_tcp_close_wakes_read) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());
    auto listener = CoTCPListener::ListenTCP("127.0.0.1", 0);
    BOOST_REQUIRE(listener);
    const auto port = listener.value()->LocalAddress().port;
    BOOST_REQUIRE(port != 0);
    const int peer = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(peer >= 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    BOOST_REQUIRE_EQUAL(::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr), 1);
    BOOST_REQUIRE_EQUAL(::connect(peer, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    bbt::core::thread::CountDownLatch accepted_done{1};
    bbt::core::thread::CountDownLatch read_done{1};
    std::atomic_int code{-1};
    CoTCP::SPtr connection;
    bbtco [listener, &connection, &accepted_done, &read_done, &code]() {
        auto accepted = listener.value()->Accept(Options());
        if (!accepted) { accepted_done.Down(); read_done.Down(); return; }
        connection = accepted.value();
        accepted_done.Down();
        char byte{};
        auto read = connection->ReadSome(&byte, 1, Options(5000));
        code.store(read ? -2 : static_cast<int>(read.error().code));
        read_done.Down();
    };
    BOOST_REQUIRE_EQUAL(accepted_done.WaitTimeout(1500), 0);
    BOOST_REQUIRE(connection);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    connection->RequestClose();
    const int wait = read_done.WaitTimeout(1500);
    ::close(peer);
    listener.value()->RequestClose();
    scheduler->Stop();
    BOOST_CHECK_EQUAL(wait, 0);
    BOOST_CHECK_EQUAL(code.load(), static_cast<int>(ErrorCode::Closed));
}

BOOST_AUTO_TEST_CASE(t_tcp_try_and_eof) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(scheduler->IsRunning());
    auto listener = CoTCPListener::ListenTCP("127.0.0.1", 0);
    BOOST_REQUIRE(listener);
    const auto port = listener.value()->LocalAddress().port;
    BOOST_REQUIRE(port != 0);
    const int peer = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(peer >= 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    BOOST_REQUIRE_EQUAL(::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr), 1);
    BOOST_REQUIRE_EQUAL(::connect(peer, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    bbt::core::thread::CountDownLatch done{1};
    bbt::core::thread::CountDownLatch ready{1};
    std::atomic_bool ok{false};
    bbtco [listener, &ready, &done, &ok]() {
        auto accepted = listener.value()->Accept(Options());
        if (accepted) {
            char byte{};
            auto empty = accepted.value()->TryReadSome(bbt::infra::MutableBytes{&byte, 1});
            const bool would_block = empty && empty.value().state == bbt::infra::IoState::WouldBlock;
            ready.Down();
            auto eof = accepted.value()->ReadSome(bbt::infra::MutableBytes{&byte, 1}, Options());
            auto zero = accepted.value()->TryReadSome(bbt::infra::MutableBytes{nullptr, 0});
            ok.store(would_block &&
                     eof && eof.value().state == bbt::infra::IoState::Eof &&
                     eof.value().bytes == 0 && zero &&
                     zero.value().state == bbt::infra::IoState::Ok);
            accepted.value()->RequestClose();
        } else ready.Down();
        done.Down();
    };
    BOOST_REQUIRE_EQUAL(ready.WaitTimeout(1500), 0);
    ::shutdown(peer, SHUT_WR);
    const int wait = done.WaitTimeout(1500);
    ::close(peer);
    listener.value()->RequestClose();
    scheduler->Stop();
    BOOST_CHECK_EQUAL(wait, 0);
    BOOST_CHECK(ok.load());
}

BOOST_AUTO_TEST_CASE(t_tcp_rejects_old_runtime_generation) {
    auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    auto listener = CoTCPListener::ListenTCP("127.0.0.1", 0);
    BOOST_REQUIRE(listener);
    const int peer = ::socket(AF_INET, SOCK_STREAM, 0);
    BOOST_REQUIRE(peer >= 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = ::htons(listener.value()->LocalAddress().port);
    BOOST_REQUIRE_EQUAL(::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr), 1);
    BOOST_REQUIRE_EQUAL(::connect(peer, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    CoTCP::SPtr old_connection;
    bbt::core::thread::CountDownLatch accepted_done{1};
    bbtco [listener, &old_connection, &accepted_done]() {
        auto accepted = listener.value()->Accept(Options(1000));
        if (accepted) old_connection = std::move(accepted).value();
        accepted_done.Down();
    };
    BOOST_REQUIRE_EQUAL(accepted_done.WaitTimeout(2000), 0);
    BOOST_REQUIRE(old_connection);
    scheduler->Stop();
    scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
    bbt::core::thread::CountDownLatch done{1};
    std::atomic_int read_code{-1}, write_code{-1}, all_code{-1},
                    zero_all_code{-1}, accept_code{-1};
    bbtco [listener, old_connection, &done, &read_code, &write_code,
           &all_code, &zero_all_code, &accept_code]() {
        char byte{};
        const char payload[] = "x";
        auto read = old_connection->ReadSome(&byte, 1, Options(80));
        read_code.store(read ? -2 : static_cast<int>(read.error().code));
        auto write = old_connection->WriteSome(payload, 1, Options(80));
        write_code.store(write ? -2 : static_cast<int>(write.error().code));
        auto all = old_connection->WriteAll(payload, 1, Options(80));
        all_code.store(all ? -2 : static_cast<int>(all.error().code));
        auto zero_all = old_connection->WriteAll(nullptr, 0, Options(80));
        zero_all_code.store(zero_all ? -2 : static_cast<int>(zero_all.error().code));
        auto accepted = listener.value()->Accept(Options(80));
        accept_code.store(accepted ? -2 : static_cast<int>(accepted.error().code));
        done.Down();
    };
    const int finished = done.WaitTimeout(2000);
    old_connection->RequestClose();
    listener.value()->RequestClose();
    ::close(peer);
    scheduler->Stop();
    BOOST_REQUIRE_EQUAL(finished, 0);
    const auto unavailable = static_cast<int>(ErrorCode::RuntimeUnavailable);
    BOOST_CHECK_EQUAL(read_code.load(), unavailable);
    BOOST_CHECK_EQUAL(write_code.load(), unavailable);
    BOOST_CHECK_EQUAL(all_code.load(), unavailable);
    BOOST_CHECK_EQUAL(zero_all_code.load(), unavailable);
    BOOST_CHECK_EQUAL(accept_code.load(), unavailable);
}

} // namespace
