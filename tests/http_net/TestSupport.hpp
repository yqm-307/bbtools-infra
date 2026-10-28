#pragma once

// http_net 契约测试支持：自有进程、动态端口的真实 loopback HTTP/1.1 对端
// （POSIX listener + 独立线程，记录 accept 总数与单连接请求数作为连接复用
// 证据），共享 client-only Engine 宿主装配（Run 线程 + 任务清账 + 收口凭据），
// 有界 watchdog 与真实 CoWaiter 屏障。fixture 只补充时序控制与观察，不 mock
// HTTP 协议本身。

#include <bbt/infra/http/Client.hpp>
#include <bbt/infra/net/detail/EngineTestAccess.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>
#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

namespace http_net_test {
using namespace bbt::infra;
using namespace std::chrono_literals;
using Access = net::EngineTestAccess;

inline void Check(bool value, const char* expression, int line) {
    if (!value) {
        throw std::runtime_error("line " + std::to_string(line) + ": " + expression);
    }
}
#define CHECK(expression) ::http_net_test::Check(static_cast<bool>(expression), #expression, __LINE__)

inline CallOptions Until(std::chrono::milliseconds duration = 2s) {
    return {std::chrono::steady_clock::now() + duration};
}
inline bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout = 3s) {
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

inline std::size_t SocketCount() {
    std::size_t count = 0;
    for (const auto& item : std::filesystem::directory_iterator("/proc/self/fd")) {
        std::error_code ec;
        const auto target = std::filesystem::read_symlink(item.path(), ec).string();
        if (!ec && target.rfind("socket:", 0) == 0) { ++count; }
    }
    return count;
}

// 测试进程 watchdog：超时仍为失败；不把强制退出当资源清理成功。
class Watchdog {
public:
    Watchdog() : m_thread([this] {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (!m_cv.wait_for(lock, 20s, [this] { return m_done; })) {
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

// 业务协程通过 CoWaiter 等待的真实屏障：目标阶段由 Release 之前的状态断言证明。
struct Gate {
    std::atomic<bool> entered{false};
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> waiter;
    Gate() : waiter(bbt::coroutine::sync::CoWaiter::Create()) { CHECK(waiter); }
    void Pause() {
        const auto status = waiter->WaitWithCallback({std::chrono::steady_clock::now() + 5s}, [this] {
            entered.store(true);
            return true;
        });
        CHECK(status == bbt::coroutine::WaitStatus::Completed);
    }
    void Release() { waiter->Notify(); }
    ~Gate() { Release(); }
};

struct Ledger {
    std::atomic<unsigned> submitted{0};
    std::atomic<unsigned> completed{0};
};
inline bool Drained(bool run_returned, net::EnginePhase phase, unsigned submitted, unsigned completed) {
    return run_returned && phase == net::EnginePhase::Stopped && submitted == completed;
}

// client-only Engine 宿主：Run 线程 + 协程任务提交 + 完整收口凭据。
class Host {
public:
    std::shared_ptr<net::Engine> engine;
    std::shared_ptr<Ledger> ledger{std::make_shared<Ledger>()};
    std::error_code run_error;
    std::atomic<bool> run_returned{false};

    Host() : engine(std::make_shared<net::Engine>(std::shared_ptr<net::RawHandler>{})) {
        net::EngineConfig config;
        config.runtime_progress = net::RuntimeProgress::Independent;
        config.options.num_event_loop = 1;
        m_thread = std::thread([this, config] {
            run_error = engine->Run(config);
            run_returned.store(true);
        });
        CHECK(WaitUntil([this] {
            return engine->Phase() == net::EnginePhase::Running || run_returned.load();
        }));
        CHECK(engine->Phase() == net::EnginePhase::Running);
    }
    ~Host() { Stop(); }

    template<class F> auto Submit(F fn) -> std::future<decltype(fn())> {
        using T = decltype(fn());
        auto promise = std::make_shared<std::promise<T>>();
        auto future = promise->get_future();
        std::function<T()> callback = std::move(fn);
        auto counts = ledger;
        counts->submitted.fetch_add(1);
        bool registered = false;
        g_scheduler->RegistCoroutineTask(128 * 1024, [promise, callback = std::move(callback), counts]() mutable {
            try {
                if constexpr (std::is_void_v<T>) {
                    callback();
                    callback = {};
                    counts->completed.fetch_add(1);
                    promise->set_value();
                } else {
                    auto value = callback();
                    callback = {};
                    counts->completed.fetch_add(1);
                    promise->set_value(std::move(value));
                }
            } catch (...) {
                callback = {};
                counts->completed.fetch_add(1);
                promise->set_exception(std::current_exception());
            }
        }, registered);
        if (!registered) {
            counts->completed.fetch_add(1);
            promise->set_exception(std::make_exception_ptr(std::runtime_error("task registration failed")));
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
    void Finish() {
        Stop();
        CHECK(WaitUntil([this] { return ledger->submitted.load() == ledger->completed.load(); }));
        CHECK(Drained(run_returned.load(), engine->Phase(), ledger->submitted.load(),
                      ledger->completed.load()));
        CHECK(!run_error);
        CHECK(!engine->LastError());
        CHECK(!engine->LastFailure());
        CHECK(engine->CountConnections() == 0);
        CHECK(Access::ConnectTableSize(*engine) == 0);
        CHECK(Access::PendingConnectCount(*engine) == 0);
        CHECK(Access::DirtyCount(*engine) == 0);
        std::cout << "receipt run_returned=1 joined=1 phase=Stopped tasks="
                  << ledger->completed.load() << "/" << ledger->submitted.load()
                  << " connections=0 connect_table=0 pending_dials=0 dirty=0\n";
    }
private:
    std::thread m_thread;
};

// 自有真实 loopback HTTP/1.1 对端。默认行为：keep-alive 循环读完整请求
// （header + Content-Length body），记录后按 canned 响应回写；hook 可完全接管
// 一次连接（坏流注入、无响应断开、屏障暂停等）。
class HttpPeer {
public:
    struct Recorded {
        std::string method;
        std::string target;
        bbt::infra::http::Headers headers;
        std::string body;
    };

    // canned 响应构造。
    static std::string HttpResponse(int status, const std::string& reason,
                                    const std::string& body,
                                    const bbt::infra::http::Headers& extra = {},
                                    bool include_content_length = true) {
        std::string out = "HTTP/1.1 " + std::to_string(status) + " " + reason + "\r\n";
        if (include_content_length) {
            out += "Content-Length: " + std::to_string(body.size()) + "\r\n";
        }
        for (const auto& header : extra) {
            out += header.first + ": " + header.second + "\r\n";
        }
        out += "\r\n";
        out += body;
        return out;
    }

    HttpPeer() {
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
        m_address = net::Address::Ip("127.0.0.1", m_port, m_address_error);
        CHECK(!m_address_error);
        m_thread = std::thread([this] { AcceptLoop(); });
    }
    ~HttpPeer() {
        Stop();
        ::close(m_listener);
    }

    // hook 复用入口：读完整 HTTP 请求（header+\r\n\r\n+Content-Length body）与发送。
    static std::optional<Recorded> ReadRequestForHook(int fd) { return ReadRequest(fd); }
    static bool SendForHook(int fd, const std::string& bytes) { return Send(fd, bytes); }

    int port() const noexcept { return m_port; }
    const net::Address& address() const noexcept { return m_address; }
    std::string base_url() const { return "http://127.0.0.1:" + std::to_string(m_port); }

    unsigned accept_total() const noexcept { return m_accept_total.load(); }
    unsigned requests_total() const noexcept { return m_requests_total.load(); }
    unsigned max_requests_on_one_connection() const noexcept {
        return m_max_requests_single.load();
    }
    std::vector<Recorded> requests() const {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_requests;
    }

    // 一次性连接 hook（按 accept 顺序消费）：完全接管一条连接（可阻塞、可发
    // 坏流）；没有 hook 的连接走默认 keep-alive 服务循环。
    void PushConnectionHook(std::function<void(int)> hook) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_hooks.push_back(std::move(hook));
    }
    void set_canned(std::function<std::string(const Recorded&)> canned) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_canned = std::move(canned);
    }
    void Stop() {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_stopped) { return; }
            m_stopped = true;
            for (const int fd : m_conns) { ::shutdown(fd, SHUT_RDWR); }
        }
        ::shutdown(m_listener, SHUT_RDWR);
        if (m_thread.joinable()) { m_thread.join(); }
        // 活跃连接线程的 fd 由 shutdown 触发收尾；等待全部线程退出与 fd 关闭。
        CHECK(WaitUntil([this] {
            std::lock_guard<std::mutex> lock(m_mutex);
            return m_conns.empty();
        }));
        for (auto& thread : m_threads) { thread.join(); }
        m_threads.clear();
    }

private:
    void AcceptLoop() {
        while (true) {
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_stopped) { return; }
            }
            pollfd pfd{m_listener, POLLIN, 0};
            if (::poll(&pfd, 1, 100) != 1) { continue; }
            const int fd = ::accept4(m_listener, nullptr, nullptr, SOCK_CLOEXEC);
            if (fd < 0) { continue; }
            m_accept_total.fetch_add(1);
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_conns.push_back(fd);
            }
            m_threads.emplace_back([this, fd] { ServeConnection(fd); });
        }
    }

    void ServeConnection(int fd) {
        timeval timeout{8, 0};
        (void)::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        int nodelay = 1;
        (void)::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
        std::function<void(int)> hook;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (!m_hooks.empty()) {
                hook = std::move(m_hooks.front());
                m_hooks.pop_front();
            }
        }
        if (hook) {
            hook(fd);
            { std::lock_guard<std::mutex> lock(m_mutex); ::close(fd); }
            RetireFd(fd);
            return;
        }
        unsigned served = 0;
        for (;;) {
            auto request = ReadRequest(fd);
            if (!request) { break; }
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                m_requests.push_back(*request);
            }
            m_requests_total.fetch_add(1);
            served++;
            unsigned observed = m_max_requests_single.load();
            while (observed < served &&
                   !m_max_requests_single.compare_exchange_weak(observed, served)) {
            }
            std::string response;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                response = m_canned ? m_canned(*request) : HttpResponse(200, "OK", "peer-default");
            }
            if (!Send(fd, response)) { break; }
        }
        { std::lock_guard<std::mutex> lock(m_mutex); ::close(fd); }
        RetireFd(fd);
    }

    void RetireFd(int fd) {
        std::lock_guard<std::mutex> lock(m_mutex);
        for (auto it = m_conns.begin(); it != m_conns.end(); ++it) {
            if (*it == fd) { m_conns.erase(it); return; }
        }
    }

    static bool Send(int fd, const std::string& bytes) {
        std::size_t offset = 0;
        while (offset < bytes.size()) {
            const auto n = ::send(fd, bytes.data() + offset, bytes.size() - offset, MSG_NOSIGNAL);
            if (n <= 0) { return false; }
            offset += static_cast<std::size_t>(n);
        }
        return true;
    }

    // 读完整 HTTP 请求（hook 复用）：header 直到 \r\n\r\n，再按 Content-Length 读 body。
    static std::optional<Recorded> ReadRequest(int fd) {
        std::string buffer;
        char chunk[4096];
        const auto head_end = [&] {
            return buffer.find("\r\n\r\n");
        };
        while (head_end() == std::string::npos) {
            const auto n = ::recv(fd, chunk, sizeof(chunk), 0);
            if (n <= 0) { return std::nullopt; }
            buffer.append(chunk, static_cast<std::size_t>(n));
            if (buffer.size() > 64 * 1024) { return std::nullopt; }
        }
        Recorded out;
        const std::string head = buffer.substr(0, head_end());
        std::string rest = buffer.substr(head_end() + 4);
        std::size_t line_end = head.find("\r\n");
        const std::string request_line = head.substr(0, line_end == std::string::npos ? head.size() : line_end);
        {
            const auto sp1 = request_line.find(' ');
            const auto sp2 = request_line.find(' ', sp1 + 1);
            CHECK(sp1 != std::string::npos && sp2 != std::string::npos);
            out.method = request_line.substr(0, sp1);
            out.target = request_line.substr(sp1 + 1, sp2 - sp1 - 1);
        }
        std::size_t content_length = 0;
        std::size_t pos = line_end == std::string::npos ? head.size() : line_end + 2;
        while (pos < head.size()) {
            const auto eol = head.find("\r\n", pos);
            const std::string line = head.substr(pos, eol == std::string::npos ? head.size() - pos : eol - pos);
            const auto colon = line.find(':');
            if (colon != std::string::npos) {
                std::string name = line.substr(0, colon);
                std::size_t value_start = colon + 1;
                while (value_start < line.size() && line[value_start] == ' ') { ++value_start; }
                const std::string value = line.substr(value_start);
                if (EqualsNoCase(name, "Content-Length")) {
                    content_length = static_cast<std::size_t>(std::atoll(value.c_str()));
                }
                out.headers.emplace_back(name, value);
            }
            if (eol == std::string::npos) { break; }
            pos = eol + 2;
        }
        while (rest.size() < content_length) {
            const auto n = ::recv(fd, chunk, sizeof(chunk), 0);
            if (n <= 0) { return std::nullopt; }
            rest.append(chunk, static_cast<std::size_t>(n));
        }
        out.body = rest.substr(0, content_length);
        return out;
    }

    static bool EqualsNoCase(std::string_view left, std::string_view right) {
        if (left.size() != right.size()) { return false; }
        for (std::size_t i = 0; i < left.size(); ++i) {
            if (std::tolower(static_cast<unsigned char>(left[i])) !=
                std::tolower(static_cast<unsigned char>(right[i]))) {
                return false;
            }
        }
        return true;
    }

    int m_listener{-1};
    int m_port{0};
    net::Address m_address;
    std::error_code m_address_error;
    std::thread m_thread;
    std::vector<std::thread> m_threads;
    mutable std::mutex m_mutex;
    bool m_stopped{false};
    std::vector<Recorded> m_requests;
    std::deque<std::function<void(int)>> m_hooks;
    std::function<std::string(const Recorded&)> m_canned;
    std::vector<int> m_conns;
    std::atomic<unsigned> m_accept_total{0};
    std::atomic<unsigned> m_requests_total{0};
    std::atomic<unsigned> m_max_requests_single{0};
};

} // namespace http_net_test
