#pragma once

// rpc_net 契约测试支持：自有进程、动态端口的真实 loopback 双端（新
// http::Client/Server + RPC core 各自独立 Engine 与 Run 线程），raw POSIX
// 对端（脚本化 HTTP 应答/坏流注入），受控 CoWaiter 屏障与有界 watchdog。
// fixture 只补充时序控制与观察，不 mock RPC/HTTP 协议本身。

#include <bbt/infra/rpc/Client.hpp>
#include <bbt/infra/rpc/RpcPayload.hpp>
#include <bbt/infra/rpc/RpcWire.hpp>
#include <bbt/infra/rpc/Server.hpp>
#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
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

#include <boost/asio/buffer.hpp>
#include <boost/beast/http.hpp>

namespace rpc_net_test {
using namespace bbt::infra;
namespace http = bbt::infra::http;
namespace rpc = bbt::infra::rpc;
namespace net = bbt::infra::net;
namespace wire = boost::beast::http;
using namespace std::chrono_literals;

inline void Check(bool value, const char* expression, int line) {
    if (!value) {
        throw std::runtime_error("line " + std::to_string(line) + ": " + expression);
    }
}
#define CHECK(expression) ::rpc_net_test::Check(static_cast<bool>(expression), #expression, __LINE__)

inline CallOptions Until(std::chrono::milliseconds duration = 3s) {
    return {std::chrono::steady_clock::now() + duration};
}
inline bool WaitUntil(const std::function<bool()>& predicate,
                      std::chrono::milliseconds timeout = 3s) {
    const auto end = std::chrono::steady_clock::now() + timeout;
    while (!predicate()) {
        if (std::chrono::steady_clock::now() >= end) { return false; }
        std::this_thread::sleep_for(1ms);
    }
    return true;
}
template<class T> T Get(std::future<T>& future) {
    CHECK(future.wait_for(5s) == std::future_status::ready);
    return future.get();
}
template<class T> void ExpectCode(const result<T>& value, ErrorCode code) {
    CHECK(!value);
    CHECK(value.error().code == code);
}

// 测试进程 watchdog：超时仍为失败；不把强制退出当资源清理成功。
class Watchdog {
public:
    Watchdog() : m_thread([this] {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (!m_cv.wait_for(lock, 30s, [this] { return m_done; })) {
            std::cerr << "FAIL watchdog: bounded test did not finish\n" << std::flush;
            std::_Exit(3);
        }
    }) {}
    ~Watchdog() {
        { std::lock_guard<std::mutex> lock(m_mutex); m_done = true; }
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
    Gate() : waiter(bbt::coroutine::sync::CoWaiter::Create()) { CHECK(waiter); }
    void Pause() {
        const auto status = waiter->WaitWithCallback(
            {std::chrono::steady_clock::now() + 10s}, [this] {
                entered.store(true);
                return true;
            });
        CHECK(status == bbt::coroutine::WaitStatus::Completed);
    }
    void Release() { waiter->Notify(); }
    ~Gate() { Release(); }
};

// client-only Engine 宿主：Run 线程 + 协程任务提交 + 完整收口凭据。
class Host {
public:
    std::shared_ptr<net::Engine> engine;
    std::error_code run_error;
    Host() : engine(std::make_shared<net::Engine>(std::shared_ptr<net::RawHandler>{})) {
        net::EngineConfig config;
        config.runtime_progress = net::RuntimeProgress::Independent;
        config.options.num_event_loop = 1;
        m_thread = std::thread([this, config] { run_error = engine->Run(config); });
        CHECK(WaitUntil([&] { return engine->Phase() == net::EnginePhase::Running; }));
    }
    ~Host() { Stop(); }

    template<class F> auto Submit(F fn) -> std::future<decltype(fn())> {
        using T = decltype(fn());
        auto promise = std::make_shared<std::promise<T>>();
        auto future = promise->get_future();
        std::function<T()> callback = std::move(fn);
        bool registered = false;
        g_scheduler->RegistCoroutineTask(256 * 1024,
            [promise, callback = std::move(callback)]() mutable {
                try {
                    if constexpr (std::is_void_v<T>) {
                        callback();
                        promise->set_value();
                    } else {
                        promise->set_value(callback());
                    }
                } catch (...) { promise->set_exception(std::current_exception()); }
            }, registered);
        if (!registered) {
            promise->set_exception(
                std::make_exception_ptr(std::runtime_error("task registration failed")));
        }
        return future;
    }
    template<class F> auto Call(F fn) -> decltype(fn()) {
        auto future = Submit(std::move(fn));
        return Get(future);
    }
    void Stop() {
        if (!m_thread.joinable()) { return; }
        engine->RequestClose();
        m_thread.join();
    }
private:
    std::thread m_thread;
};

// server-side Engine + rpc::Server 宿主：真实监听动态端口，收口走 RPC Server
// 的 StopAccepting → Drain → Close → WaitStopped 序列。
struct ServerHost {
    std::shared_ptr<net::Engine> engine{
        std::make_shared<net::Engine>(std::shared_ptr<net::RawHandler>{})};
    std::shared_ptr<rpc::Server> server;
    std::thread runner;
    std::error_code run_error;
    net::Address address;
    std::atomic<unsigned> handled{0};

    explicit ServerHost(bbt::infra::RpcHandler handler, http::ServerOptions options = {},
                        bool start = true) {
        auto made = rpc::Server::Create(engine, std::move(handler), options);
        CHECK(made);
        server = std::move(made).value();
        CHECK(engine->Phase() == net::EnginePhase::Idle);
        net::EngineConfig config;
        config.runtime_progress = net::RuntimeProgress::Independent;
        config.options.num_event_loop = 1;
        std::error_code error;
        config.listen_addresses = {net::Address::Ip("127.0.0.1", 0, error)};
        CHECK(!error);
        runner = std::thread([this, config] { run_error = engine->Run(config); });
        CHECK(WaitUntil([&] { return engine->Phase() == net::EnginePhase::Running; }));
        std::vector<net::Address> addresses;
        CHECK(!engine->LocalAddresses(addresses) && addresses.size() == 1);
        address = addresses[0];
        if (start) { CHECK(server->Start()); }
    }
    ~ServerHost() {
        server->Close();
        engine->RequestClose();
        if (runner.joinable()) { runner.join(); }
    }
    std::string Base() const {
        return "http://127.0.0.1:" + std::to_string(address.Port());
    }
    RpcAddress Endpoint() const { return {"http", "127.0.0.1:" + std::to_string(address.Port())}; }

    // 正常收口凭据：listener 停止、任务/连接清账、Run join、Engine Stopped。
    void Finish() {
        server->StopAccepting();
        CHECK(server->Drain(std::chrono::steady_clock::now() + 3s));
        server->Close();
        CHECK(server->WaitStopped(std::chrono::steady_clock::now() + 3s));
        CHECK(engine->AcceptingStopped());
        CHECK(engine->Phase() == net::EnginePhase::Running);
        engine->RequestClose();
        runner.join();
        CHECK(!run_error && engine->Phase() == net::EnginePhase::Stopped);
        CHECK(engine->CountConnections() == 0);
    }
};

// server-side Engine + http::Server 宿主（脚本化 HTTP 层应答）：给错误映射与
// 畸形回复用例注入任意 ServerHandler；收口责任与 ServerHost 相同。
struct HttpServerHost {
    std::shared_ptr<net::Engine> engine{
        std::make_shared<net::Engine>(std::shared_ptr<net::RawHandler>{})};
    std::shared_ptr<http::Server> server;
    std::thread runner;
    std::error_code run_error;
    net::Address address;

    explicit HttpServerHost(http::ServerHandler handler) {
        auto made = http::Server::Create(engine, std::move(handler));
        CHECK(made);
        server = std::move(made).value();
        net::EngineConfig config;
        config.runtime_progress = net::RuntimeProgress::Independent;
        config.options.num_event_loop = 1;
        std::error_code error;
        config.listen_addresses = {net::Address::Ip("127.0.0.1", 0, error)};
        CHECK(!error);
        runner = std::thread([this, config] { run_error = engine->Run(config); });
        CHECK(WaitUntil([&] { return engine->Phase() == net::EnginePhase::Running; }));
        std::vector<net::Address> addresses;
        CHECK(!engine->LocalAddresses(addresses) && addresses.size() == 1);
        address = addresses[0];
        CHECK(server->Start());
    }
    ~HttpServerHost() {
        server->Close();
        engine->RequestClose();
        if (runner.joinable()) { runner.join(); }
    }
    std::string Base() const {
        return "http://127.0.0.1:" + std::to_string(address.Port());
    }
    RpcAddress Endpoint() const { return {"http", "127.0.0.1:" + std::to_string(address.Port())}; }
    void Finish() {
        server->StopAccepting();
        CHECK(server->Drain(std::chrono::steady_clock::now() + 3s));
        server->Close();
        CHECK(server->WaitStopped(std::chrono::steady_clock::now() + 3s));
        CHECK(engine->AcceptingStopped());
        engine->RequestClose();
        runner.join();
        CHECK(!run_error && engine->Phase() == net::EnginePhase::Stopped);
        CHECK(engine->CountConnections() == 0);
    }
};

// 组合好的 client 端：应用装配 http::Client（保留共享句柄供池观测），
// rpc::Client 只包装。tune 允许注入共享 transport 默认配置（headers/params）。
struct ClientFixture {
    Host host;
    std::shared_ptr<http::Client> http_client;
    std::shared_ptr<rpc::Client> client;
    explicit ClientFixture(
        const std::string& base_url,
        const std::function<void(http::ClientOptions&)>& tune = {}) {
        http::ClientOptions options;
        options.base_url = base_url;
        if (tune) { tune(options); }
        auto made = http::Client::Create(host.engine, options);
        CHECK(made);
        http_client = std::move(made).value();
        auto wrapped = rpc::Client::Create(http_client);
        CHECK(wrapped);
        client = std::move(wrapped).value();
    }
};

// 标准测试 envelope：位置参数 payload（复用 rpc::EncodeTuple）+ 关联字段。
inline RpcEnvelope MakeEnvelope(const std::string& service, const std::string& method,
                                const std::string& request_id,
                                std::vector<std::uint8_t> payload = {}) {
    RpcEnvelope env;
    env.service = service;
    env.method = method;
    env.request_id = request_id;
    env.request_schema = std::string(rpc::kRpcPositionalSchema);
    env.response_schema = std::string(rpc::kRpcPositionalSchema);
    env.payload = std::move(payload);
    return env;
}
inline std::vector<std::uint8_t> Pack(const std::string& text, std::int64_t number) {
    auto encoded = rpc::EncodeTuple(std::make_tuple(text, number),
                                    std::make_index_sequence<2>{});
    CHECK(encoded);
    return std::move(encoded).value();
}
inline std::tuple<std::string, std::int64_t> Unpack(
    const std::vector<std::uint8_t>& bytes) {
    auto decoded = rpc::DecodeTuple<std::string, std::int64_t>(
        bytes, std::make_index_sequence<2>{});
    CHECK(decoded);
    return std::move(decoded).value();
}

// 标准回声 handler：payload (string,int64) 原样回传，metadata 透传。
inline bbt::infra::RpcHandler EchoHandler(std::atomic<unsigned>* counter = nullptr) {
    return [counter](IncomingCallContext context, RpcEnvelope request)
               -> result<RpcEnvelope> {
        if (counter) { counter->fetch_add(1); }
        if (context.deadline <= std::chrono::steady_clock::now()) {
            return result<RpcEnvelope>::err(
                MakeError(ErrorCode::TimedOut, "handler entered past deadline"));
        }
        return result<RpcEnvelope>::ok(std::move(request));  // 原样回传（含 metadata/关联字段）
    };
}

// 自有 raw loopback HTTP/1.1 对端（脚本化服务端）：坏流注入、无响应断开、
// canned 应答；用 Beast 解析响应供断言。
class RawPeer {
public:
    explicit RawPeer(const net::Address& address) {
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
    ~RawPeer() { if (fd >= 0) { ::close(fd); } }

    void Send(const std::string& bytes) {
        std::size_t offset = 0;
        while (offset < bytes.size()) {
            const auto n = ::send(fd, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
            CHECK(n > 0);
            offset += static_cast<std::size_t>(n);
        }
    }
    // 半开关闭：发完 FIN 后保留读端等待响应（shutdown(SHUT_WR)）。
    void HalfClose() { ::shutdown(fd, SHUT_WR); }
    bool Eof() {
        char bytes[64];
        const auto n = ::recv(fd, bytes, sizeof(bytes), 0);
        return n == 0 || (n < 0 && errno == ECONNRESET);
    }
    http::Response ReadResponse() {
        wire::response_parser<wire::string_body> parser;
        parser.body_limit(16 * 1024 * 1024);
        parser.eager(true);
        while (!parser.is_done()) {
            if (!pending.empty()) {
                boost::system::error_code error;
                const auto consumed = parser.put(boost::asio::buffer(pending), error);
                pending.erase(0, consumed);
                CHECK(!error || error == wire::error::need_more);
                if (parser.is_done()) { break; }
                if (consumed) { continue; }
            }
            char bytes[32768];
            const auto size = ::recv(fd, bytes, sizeof(bytes), 0);
            CHECK(size > 0);
            pending.append(bytes, static_cast<std::size_t>(size));
        }
        const auto& message = parser.get();
        http::Response out{static_cast<int>(message.result_int()), {}, message.body()};
        for (auto& field : message) {
            out.headers.emplace_back(std::string(field.name_string()),
                                     std::string(field.value()));
        }
        return out;
    }
private:
    int fd{-1};
    std::string pending;
};

// 现成 wire 字节工具：请求/响应信封 → HTTP body 字节（测试脚本化用）。
inline std::string RequestBody(const rpc::RpcWireEnvelope& env) {
    auto bytes = rpc::EncodeRpcWireEnvelope(env, /*is_response=*/false);
    CHECK(bytes);
    return {bytes.value().begin(), bytes.value().end()};
}
inline std::string ResponseBody(const rpc::RpcWireEnvelope& env) {
    auto bytes = rpc::EncodeRpcWireEnvelope(env, /*is_response=*/true);
    CHECK(bytes);
    return {bytes.value().begin(), bytes.value().end()};
}
inline rpc::RpcWireEnvelope ParseReply(const http::Response& response) {
    auto decoded = rpc::ParseRpcWireHttpResponse(response);
    CHECK(decoded);
    return std::move(decoded).value();
}

} // namespace rpc_net_test
