#pragma once

// rpc_net 契约测试支持（V2 单一会话面）：自有进程、动态端口的真实 loopback
// 对端（raw POSIX 拨号/监听，脚本化 SessionFrame wire 字节），受控 CoWaiter
// 屏障与有界 watchdog。fixture 只补充时序控制与观察，不 mock RPC 协议本身。

#include <bbt/infra/rpc/Admission.hpp>
#include <bbt/infra/rpc/CoRpcClient.hpp>
#include <bbt/infra/rpc/Options.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>
#include <bbt/infra/rpc/CoRpcServer.hpp>
#include <bbt/infra/rpc/SessionCodec.hpp>
#include <bbt/infra/rpc/Stats.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

namespace rpc_net_test {
using namespace bbt::infra;
namespace rpc = bbt::infra::rpc;
namespace net = bbt::infra::net;
using namespace std::chrono_literals;

inline void Check(bool value, const char* expression, int line)
{
    if (!value) {
        throw std::runtime_error("line " + std::to_string(line) + ": " + expression);
    }
}
#define CHECK(expression) ::rpc_net_test::Check(static_cast<bool>(expression), #expression, __LINE__)

inline CallOptions Until(std::chrono::milliseconds duration = 3s)
{
    return {std::chrono::steady_clock::now() + duration};
}
inline bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout = 3s)
{
    const auto end = std::chrono::steady_clock::now() + timeout;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= end) {
            return false;
        }
        std::this_thread::sleep_for(1ms);
    }
    return true;
}
template <class T> T Get(std::future<T>& future)
{
    CHECK(future.wait_for(5s) == std::future_status::ready);
    return future.get();
}
template <class T> void ExpectCode(const result<T>& value, ErrorCode code)
{
    CHECK(!value);
    CHECK(value.error().code == code);
}

// 测试进程 watchdog：超时仍为失败；不把强制退出当资源清理成功。
class Watchdog {
public:
    Watchdog()
        : m_thread([this] {
              std::unique_lock<std::mutex> lock(m_mutex);
              if (!m_cv.wait_for(lock, 30s, [this] { return m_done; })) {
                  std::cerr << "FAIL watchdog: bounded test did not finish\n" << std::flush;
                  std::_Exit(3);
              }
          })
    {
    }
    ~Watchdog()
    {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_done = true;
        }
        m_cv.notify_one();
        m_thread.join();
    }

private:
    mutable std::mutex m_mutex;
    std::condition_variable m_cv;
    bool m_done{false};
    std::thread m_thread;
};

// 业务协程通过 CoWaiter 等待的真实屏障：handler 进入即挂起，测试线程显式放行。
struct Gate {
    std::atomic<bool> entered{false};
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> waiter;
    Gate() : waiter(bbt::coroutine::sync::CoWaiter::Create())
    {
        CHECK(waiter);
    }
    void Pause()
    {
        const auto status = waiter->WaitWithCallback({std::chrono::steady_clock::now() + 10s}, [this] {
            entered.store(true);
            return true;
        });
        CHECK(status == bbt::coroutine::WaitStatus::Completed);
    }
    void Release()
    {
        waiter->Notify();
    }
    ~Gate()
    {
        Release();
    }
};

// 自有 raw loopback TCP 拨号对端：任意分片注入、坏前缀、截断、canned 回复。
// 协议字节由测试脚本化（不 mock session/adapter）。
class RawTcpPeer {
public:
    explicit RawTcpPeer(const net::Address& address)
    {
        fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        CHECK(fd >= 0);
        timeval timeout{8, 0};
        CHECK(::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
        CHECK(::setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0);
        int nodelay = 1;
        (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
        sockaddr_in target{};
        target.sin_family = AF_INET;
        target.sin_port = htons(address.Port());
        target.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        CHECK(::connect(fd, reinterpret_cast<sockaddr*>(&target), sizeof(target)) == 0);
    }
    ~RawTcpPeer()
    {
        if (fd >= 0) {
            ::close(fd);
        }
    }

    void Send(const std::string& bytes)
    {
        std::size_t offset = 0;
        while (offset < bytes.size()) {
            const auto n = ::send(fd, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
            CHECK(n > 0);
            offset += static_cast<std::size_t>(n);
        }
    }
    // 逐片发送：prefix/body 任意分片（含 1 字节片）。
    void SendFragmented(const std::string& bytes, std::size_t chunk)
    {
        for (std::size_t offset = 0; offset < bytes.size(); offset += chunk) {
            Send(bytes.substr(offset, std::min(chunk, bytes.size() - offset)));
        }
    }
    void HalfClose()
    {
        ::shutdown(fd, SHUT_WR);
    }
    bool Eof()
    {
        char bytes[64];
        const auto n = ::recv(fd, bytes, sizeof(bytes), 0);
        return n == 0 || (n < 0 && errno == ECONNRESET);
    }
    // 读恰好 n 字节（测试响应帧前缀/body）。
    std::string ReadExact(std::size_t n)
    {
        std::string out;
        while (out.size() < n) {
            char bytes[32768];
            const auto got = ::recv(fd, bytes, std::min(sizeof(bytes), n - out.size()), 0);
            CHECK(got > 0);
            out.append(bytes, static_cast<std::size_t>(got));
        }
        return out;
    }

private:
    int fd{-1};
};

// 自有 raw loopback TCP 监听对端：以给定 identity 完成 inbound 侧握手，之后
// 由测试线程逐帧注入任意 Response/垃圾字节——验证客户端对畸形回复的收场。
// 只用于「对端行为」构造，不替代真实 server 实现。
class RawTcpListener {
public:
    explicit RawTcpListener(std::string identity) : m_identity(std::move(identity))
    {
        m_listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        CHECK(m_listener >= 0);
        int one = 1;
        CHECK(::setsockopt(m_listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) == 0);
        sockaddr_in local{};
        local.sin_family = AF_INET;
        local.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        CHECK(::bind(m_listener, reinterpret_cast<sockaddr*>(&local), sizeof(local)) == 0);
        CHECK(::listen(m_listener, 8) == 0);
        socklen_t size = sizeof(local);
        CHECK(::getsockname(m_listener, reinterpret_cast<sockaddr*>(&local), &size) == 0);
        m_port = ntohs(local.sin_port);
    }
    ~RawTcpListener()
    {
        if (m_fd >= 0) {
            ::close(m_fd);
        }
        if (m_listener >= 0) {
            ::close(m_listener);
        }
    }

    std::string Target() const
    {
        return "tcp://127.0.0.1:" + std::to_string(m_port);
    }

    // 有界接受一个连接（测试线程调用，不在协程内）：10s 内未连上按失败收场，
    // 不让 raw 对端把用例挂死到 watchdog。
    void Accept()
    {
        fd_set readable;
        FD_ZERO(&readable);
        FD_SET(m_listener, &readable);
        timeval wait{10, 0};
        CHECK(::select(m_listener + 1, &readable, nullptr, nullptr, &wait) == 1);
        m_fd = ::accept(m_listener, nullptr, nullptr);
        CHECK(m_fd >= 0);
        timeval timeout{8, 0};
        CHECK(::setsockopt(m_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
        CHECK(::setsockopt(m_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0);
        int nodelay = 1;
        (void)::setsockopt(m_fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
    }

    // inbound 侧握手：读对端 Hello → 写本端 Hello → 等对端 HelloAck（本端
    // identity 字典序更大，故对端是 epoch owner）→ echo HelloAck。
    std::uint64_t Handshake()
    {
        const auto hello = ReadFrame();
        CHECK(hello.kind == rpc::FrameKind::Hello);
        CHECK(m_identity > hello.peer_identity);
        rpc::SessionFrame own;
        own.kind = rpc::FrameKind::Hello;
        own.traffic = rpc::TrafficClass::System;
        own.peer_identity = m_identity;
        own.session_epoch = 1;
        own.connection_nonce = 0x5EED;
        SendFrame(own);
        const auto ack = ReadFrame();
        CHECK(ack.kind == rpc::FrameKind::HelloAck);
        CHECK(ack.connection_nonce == own.connection_nonce);
        CHECK(ack.peer_identity == hello.peer_identity);
        CHECK(ack.session_epoch != 0);
        rpc::SessionFrame echo;
        echo.kind = rpc::FrameKind::HelloAck;
        echo.traffic = rpc::TrafficClass::System;
        echo.peer_identity = m_identity;
        echo.session_epoch = ack.session_epoch;
        echo.connection_nonce = hello.connection_nonce;
        SendFrame(echo);
        return ack.session_epoch;
    }

    rpc::SessionFrame ReadFrame()
    {
        const auto prefix = ReadExact(4);
        const auto length = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(prefix.data()));
        CHECK(length > 0 && length <= 8 * 1024 * 1024);
        const auto body = ReadExact(length);
        auto decoded = rpc::DecodeSessionFrame(reinterpret_cast<const std::uint8_t*>(body.data()), body.size());
        CHECK(decoded);
        return std::move(decoded).value();
    }
    void SendFrame(const rpc::SessionFrame& frame)
    {
        auto bytes = rpc::EncodeFramedSessionFrame(frame, 8 * 1024 * 1024);
        CHECK(bytes);
        SendRaw(std::string(reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size()));
    }
    void SendRaw(const std::string& bytes)
    {
        std::size_t offset = 0;
        while (offset < bytes.size()) {
            const auto n = ::send(m_fd, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
            CHECK(n > 0);
            offset += static_cast<std::size_t>(n);
        }
    }
    bool Eof()
    {
        char bytes[64];
        const auto n = ::recv(m_fd, bytes, sizeof(bytes), 0);
        return n == 0 || (n < 0 && errno == ECONNRESET);
    }
    std::string ReadExact(std::size_t n)
    {
        std::string out;
        while (out.size() < n) {
            char bytes[32768];
            const auto got = ::recv(m_fd, bytes, std::min(sizeof(bytes), n - out.size()), 0);
            CHECK(got > 0);
            out.append(bytes, static_cast<std::size_t>(got));
        }
        return out;
    }

private:
    int m_listener{-1};
    int m_fd{-1};
    std::uint16_t m_port{0};
    std::string m_identity;
};

} // namespace rpc_net_test
