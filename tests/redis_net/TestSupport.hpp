#pragma once

#include <bbt/infra/redis/CoRedisCli.hpp>
#include <bbt/infra/net/detail/EngineTestAccess.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>
#include <bbt/coroutine/coroutine.hpp>
#include <hiredis/hiredis.h>

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace redis_net_test {
using namespace bbt::infra;
using namespace std::chrono_literals;
using Client = redis::CoRedisCli;
using Access = net::EngineTestAccess;

inline void Check(bool value, const char* expression, int line) {
    if (!value) {
        throw std::runtime_error("line " + std::to_string(line) + ": " + expression);
    }
}
#define CHECK(expression) ::redis_net_test::Check(static_cast<bool>(expression), #expression, __LINE__)

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
inline net::Address Loopback(int port = 6379) {
    std::error_code error;
    auto address = net::Address::Ip("127.0.0.1", port, error);
    CHECK(!error);
    return address;
}
inline void PrintFds(const char* stage) {
    for (const auto& item : std::filesystem::directory_iterator("/proc/self/fd")) {
        std::error_code error;
        const auto target = std::filesystem::read_symlink(item.path(), error);
        if (!error) { std::cout << "fd_snapshot stage=" << stage << " fd=" << item.path().filename().string()
                                << " target=" << target.string() << '\n'; }
    }
}
inline std::size_t FdCount() {
    return static_cast<std::size_t>(std::distance(std::filesystem::directory_iterator("/proc/self/fd"),
                                                  std::filesystem::directory_iterator{}));
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
    std::mutex m_mutex;
    std::condition_variable m_cv;
    bool m_done{false};
    std::thread m_thread;
};

// native 端等待；业务协程通过 CoWaiter 等待，保证单 worker 仍推进 I/O 和其他任务。
struct Gate {
    std::atomic<bool> entered{false};
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> waiter;
    Gate() : waiter(bbt::coroutine::sync::CoWaiter::Create()) { CHECK(waiter); }
    void Pause() {
        auto status = waiter->WaitWithCallback({std::chrono::steady_clock::now() + 3s}, [this] {
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

inline bool DrainComplete(bool returned, net::EnginePhase phase, unsigned submitted, unsigned completed) {
    return returned && phase == net::EnginePhase::Stopped && submitted == completed;
}

class Host {
public:
    std::shared_ptr<net::Engine> engine;
    std::shared_ptr<Ledger> ledger{std::make_shared<Ledger>()};
    std::error_code run_error;
    std::atomic<bool> run_returned{false};

    explicit Host(std::shared_ptr<net::RawHandler> handler = {}, net::Options options = {})
        : engine(std::make_shared<net::Engine>(std::move(handler))) {
        net::EngineConfig config;
        config.runtime_progress = net::RuntimeProgress::Independent;
        config.options = options;
        m_thread = std::thread([this, config] {
            run_error = engine->Run(config);
            run_returned.store(true);
        });
        CHECK(WaitUntil([this] { return engine->Phase() == net::EnginePhase::Running || run_returned.load(); }));
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
    std::shared_ptr<Client> Make(net::Address address = Loopback()) {
        auto value = Client::Create(engine, address);
        CHECK(value);
        auto client = std::move(value).value();
        m_clients.push_back(client);
        return client;
    }
    void Connect(const std::shared_ptr<Client>& client) {
        auto value = Call([client] { return client->Connect(Until()); });
        if (!value) { throw std::runtime_error("Connect: " + value.error().message); }
    }
    void Stop() {
        if (!m_thread.joinable()) { return; }
        for (auto& weak : m_clients) {
            if (auto client = weak.lock()) { client->Close(); }
        }
        engine->RequestClose();
        m_thread.join();
    }
    bool Drained() const {
        return DrainComplete(run_returned.load(), engine->Phase(), ledger->submitted.load(), ledger->completed.load());
    }
    void Finish() {
        Stop();
        CHECK(WaitUntil([this] { return ledger->submitted.load() == ledger->completed.load(); }));
        CHECK(Drained());
        CHECK(!run_error);
        CHECK(!engine->LastError());
        CHECK(!engine->LastFailure());
        CHECK(engine->CountConnections() == 0);
        CHECK(Access::ConnectTableSize(*engine) == 0);
        CHECK(Access::PendingConnectCount(*engine) == 0);
        CHECK(Access::DirtyCount(*engine) == 0);
        CHECK(Access::RecvHoldBytes(*engine) == 0);
        std::vector<std::uint64_t> main, subs;
        CHECK(engine->ReactorOwnerIds(main, subs) == std::errc::operation_not_permitted);
        CHECK(main.empty() && subs.empty());
        std::cout << "receipt run_returned=1 joined=1 phase=Stopped tasks=" << ledger->completed.load()
                  << "/" << ledger->submitted.load()
                  << " connections=0 connect_table=0 pending_dials=0 dirty=0 recv_hold=0 driver_owners=0\n";
    }
private:
    std::thread m_thread;
    std::vector<std::weak_ptr<Client>> m_clients;
};

using Reply = std::unique_ptr<redisReply, decltype(&freeReplyObject)>;
class RealRedis {
public:
    RealRedis() : m_context(redisConnectWithTimeout("127.0.0.1", 6379, timeval{2, 0}), &redisFree) {
        CHECK(m_context && m_context->err == 0);
        CHECK(redisSetTimeout(m_context.get(), timeval{2, 0}) == REDIS_OK);
        std::random_device random;
        m_prefix = "bbt:redis-net:" + std::to_string(getpid()) + ":" + std::to_string(random()) + ":" +
                   std::to_string(random()) + ":";
    }
    ~RealRedis() {
        try { Cleanup(); }
        catch (const std::exception& e) { std::cerr << "cleanup_failed " << e.what() << "\n"; m_cleanup_failed = true; }
    }
    std::string Key(std::string suffix) {
        auto key = m_prefix + suffix;
        m_keys.push_back(key);
        return key;
    }
    Reply Command(std::vector<std::string> args) {
        std::vector<const char*> argv;
        std::vector<std::size_t> lengths;
        for (const auto& arg : args) { argv.push_back(arg.data()); lengths.push_back(arg.size()); }
        Reply reply(static_cast<redisReply*>(redisCommandArgv(m_context.get(), static_cast<int>(argv.size()),
                                                             argv.data(), lengths.data())), &freeReplyObject);
        CHECK(reply);
        return reply;
    }
    void Cleanup() {
        for (const auto& key : m_keys) {
            auto removed = Command({"DEL", key});
            CHECK(removed->type == REDIS_REPLY_INTEGER);
            auto remaining = Command({"EXISTS", key});
            CHECK(remaining->type == REDIS_REPLY_INTEGER && remaining->integer == 0);
            // 二进制 key 仅打印本次唯一前缀和长度，避免混淆日志。
            std::cout << "cleanup prefix=" << m_prefix << " key_bytes=" << key.size()
                      << " deleted=" << removed->integer << " exists_after=0\n";
        }
        m_keys.clear();
    }
    static bool CleanupFailed() { return m_cleanup_failed; }
private:
    std::unique_ptr<redisContext, decltype(&redisFree)> m_context;
    std::string m_prefix;
    std::vector<std::string> m_keys;
    inline static bool m_cleanup_failed{false};
};

// 自有真实 TCP fixture：只用于分帧和受控失效，不替代真实 Redis 命令验收。
class Peer {
public:
    net::Address address;
    explicit Peer(std::function<void(int)> script, unsigned sessions = 1) {
        m_listener = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        CHECK(m_listener >= 0);
        sockaddr_in local{};
        local.sin_family = AF_INET;
        local.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        CHECK(::bind(m_listener, reinterpret_cast<sockaddr*>(&local), sizeof(local)) == 0);
        CHECK(::listen(m_listener, 4) == 0);
        socklen_t size = sizeof(local);
        CHECK(::getsockname(m_listener, reinterpret_cast<sockaddr*>(&local), &size) == 0);
        address = Loopback(ntohs(local.sin_port));
        m_thread = std::thread([this, script = std::move(script), sessions] {
            try {
                for (unsigned session = 0; session < sessions; ++session) {
                    pollfd pfd{m_listener, POLLIN, 0};
                    CHECK(::poll(&pfd, 1, 3000) == 1);
                    const int fd = ::accept4(m_listener, nullptr, nullptr, SOCK_CLOEXEC);
                    CHECK(fd >= 0);
                    { std::lock_guard<std::mutex> lock(m_mutex); m_peer = fd; }
                    timeval timeout{3, 0};
                    CHECK(::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
                    script(fd);
                    { std::lock_guard<std::mutex> lock(m_mutex); ::close(m_peer); m_peer = -1; }
                }
            } catch (...) { m_failure = std::current_exception(); }
        });
    }
    ~Peer() {
        { std::lock_guard<std::mutex> lock(m_mutex);
          if (m_peer >= 0) { ::shutdown(m_peer, SHUT_RDWR); } }
        ::shutdown(m_listener, SHUT_RDWR);
        if (m_thread.joinable()) { m_thread.join(); }
        if (m_peer >= 0) { ::close(m_peer); }
        ::close(m_listener);
    }
    void Join() {
        m_thread.join();
        if (m_failure) { std::rethrow_exception(m_failure); }
    }
    static std::vector<std::string> ReadCommand(int fd) {
        std::unique_ptr<redisReader, decltype(&redisReaderFree)> reader(redisReaderCreate(), &redisReaderFree);
        CHECK(reader);
        for (;;) {
            char bytes[4096];
            auto n = ::recv(fd, bytes, sizeof(bytes), 0);
            CHECK(n > 0);
            CHECK(redisReaderFeed(reader.get(), bytes, static_cast<std::size_t>(n)) == REDIS_OK);
            void* raw = nullptr;
            CHECK(redisReaderGetReply(reader.get(), &raw) == REDIS_OK);
            Reply reply(static_cast<redisReply*>(raw), &freeReplyObject);
            if (!reply) { continue; }
            CHECK(reply->type == REDIS_REPLY_ARRAY);
            std::vector<std::string> command;
            for (std::size_t i = 0; i < reply->elements; ++i) {
                CHECK(reply->element[i]->type == REDIS_REPLY_STRING);
                command.emplace_back(reply->element[i]->str, reply->element[i]->len);
            }
            return command;
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
private:
    int m_listener{-1};
    int m_peer{-1};
    std::mutex m_mutex;
    std::thread m_thread;
    std::exception_ptr m_failure;
};
} // namespace redis_net_test
