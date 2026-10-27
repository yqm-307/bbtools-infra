#include <bbt/infra/redis/CoRedisCli.hpp>
#include <bbt/infra/redis/CoRedisPool.hpp>
#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

#include <atomic>
#include <future>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <random>
#include <stdexcept>
#include <thread>

using namespace std::chrono_literals;
using namespace bbt::infra;

namespace {
CallOptions Options() { return {std::chrono::steady_clock::now() + 2s}; }
void Require(const result<void>& value) {
    if (!value) { throw std::runtime_error(value.error().message); }
}

void RecordFailure(std::exception_ptr& failure, const char* stage) {
    const auto current = std::current_exception();
    std::string message(stage);
    try { std::rethrow_exception(current); }
    catch (const std::exception& error) { message += std::string(": ") + error.what(); }
    catch (...) { message += ": non-standard exception"; }
    if (failure) {
        try { std::rethrow_exception(failure); }
        catch (const std::exception& error) { message = std::string(error.what()) + "; " + message; }
        catch (...) { message = "non-standard exception; " + message; }
    }
    failure = std::make_exception_ptr(std::runtime_error(message));
}

unsigned short RedisPort() {
    const char* raw = std::getenv("BBT_REDIS_PORT");
    if (!raw) { return 6379; }
    const std::string text(raw);
    if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos) {
        throw std::runtime_error("invalid BBT_REDIS_PORT");
    }
    const auto port = std::stoul(text);
    if (port == 0 || port > 65535) { throw std::runtime_error("invalid BBT_REDIS_PORT"); }
    return static_cast<unsigned short>(port);
}

// 宿主收尾由 Run() 统一执行：RequestClose、join Run、核对 Phase=Stopped。

// 单连接阶段：显式 Connect 后 PING/SET/GET 读回，唯一 key 清理并 EXISTS 读回。
void RunSingleConnection(std::shared_ptr<net::Engine> engine, const net::Address& address) {
    auto created = redis::CoRedisCli::Create(engine, address);
    if (!created) { throw std::runtime_error(created.error().message); }
    auto client = std::move(created).value();
    std::random_device random;
    const std::string key = "bbt:redis-net:example:" + std::to_string(random()) + ":" + std::to_string(random());
    bool registered = false;
    auto completion = std::make_shared<std::promise<void>>();
    auto done = completion->get_future().share();
    // 宿主登记唯一 API 任务；完成信号前释放捕获，不再访问 adapter。
    g_scheduler->RegistCoroutineTask(128 * 1024, [client, key, completion]() mutable {
        std::exception_ptr failure;
        try {
            Require(bbt_co_await client->Connect(Options()));
            Require(bbt_co_await client->Ping(Options()));
            Require(bbt_co_await client->Set(key, "hello-net", Options()));
            auto value = bbt_co_await client->Get(key, Options());
            if (!value || !value.value() || *value.value() != "hello-net") {
                throw std::runtime_error("GET mismatch");
            }
            std::cout << "[single] PING=PONG SET=OK GET=hello-net\n";
        } catch (...) {
            RecordFailure(failure, "single business");
        }
        // 无论业务成败，只清理这个示例拥有的 key；故障后显式重连再尝试。
        try {
            auto cleanup = bbt_co_await client->Delete({key}, Options());
            if (!cleanup) {
                auto connected = bbt_co_await client->Connect(Options());
                if (connected) { cleanup = bbt_co_await client->Delete({key}, Options()); }
            }
            auto remaining = bbt_co_await client->Exists(key, Options());
            if (!cleanup || !remaining || remaining.value()) {
                throw std::runtime_error("unique key cleanup failed");
            }
            std::cout << "[single] cleanup unique_key=1 exists_after=0\n";
        } catch (...) {
            RecordFailure(failure, "single cleanup");
        }
        client->Close();
        client.reset();
        if (failure) { completion->set_exception(failure); }
        else { completion->set_value(); }
    }, registered);
    if (!registered) { throw std::runtime_error("API task registration failed"); }
    // 普通 native 宿主等待；业务 worker 继续推进，不在协程内同步 wait。
    if (done.wait_for(10s) != std::future_status::ready) { throw std::runtime_error("single stage timeout"); }
    done.get();
}

// 池阶段：Connect 全池就绪后经池执行 PING/SET/GET，唯一 key 清理读回。
void RunPool(std::shared_ptr<net::Engine> engine, const net::Address& address) {
    redis::CoRedisPool::PoolOptions pool_options{3, 10}; // 3 连接，最多 10 排队。
    auto created = redis::CoRedisPool::Create(engine, address, pool_options);
    if (!created) { throw std::runtime_error(created.error().message); }
    auto pool = std::move(created).value();
    std::random_device random;
    const std::string key = "bbt:redis-pool:example:" + std::to_string(random()) + ":" + std::to_string(random());
    bool registered = false;
    auto completion = std::make_shared<std::promise<void>>();
    auto done = completion->get_future().share();
    g_scheduler->RegistCoroutineTask(128 * 1024, [pool, key, completion]() mutable {
        std::exception_ptr failure;
        try {
            Require(bbt_co_await pool->Connect(Options()));
            Require(bbt_co_await pool->Ping(Options()));
            Require(bbt_co_await pool->Set(key, "hello-pool", Options()));
            auto value = bbt_co_await pool->Get(key, Options());
            if (!value || !value.value() || *value.value() != "hello-pool") {
                throw std::runtime_error("GET mismatch");
            }
            std::cout << "[pool] PING=PONG SET=OK GET=hello-pool pool_size=3\n";
        } catch (...) {
            RecordFailure(failure, "pool business");
        }
        try {
            auto cleanup = bbt_co_await pool->Delete({key}, Options());
            if (!cleanup) {
                auto connected = bbt_co_await pool->Connect(Options());
                if (connected) { cleanup = bbt_co_await pool->Delete({key}, Options()); }
            }
            auto remaining = bbt_co_await pool->Exists(key, Options());
            if (!cleanup || !remaining || remaining.value()) {
                throw std::runtime_error("unique key cleanup failed");
            }
            std::cout << "[pool] cleanup unique_key=1 exists_after=0\n";
        } catch (...) {
            RecordFailure(failure, "pool cleanup");
        }
        pool->Close();
        pool.reset();
        if (failure) { completion->set_exception(failure); }
        else { completion->set_value(); }
    }, registered);
    if (!registered) { throw std::runtime_error("API task registration failed"); }
    if (done.wait_for(10s) != std::future_status::ready) { throw std::runtime_error("pool stage timeout"); }
    done.get();
}

int Run() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 1;
    g_bbt_coroutine_config->m_cfg_stack_size = 128 * 1024;
    g_scheduler->Start(); // runtime 进程寿命：整个示例只 Start 一次。
    auto engine = std::make_shared<net::Engine>(nullptr);
    net::EngineConfig config;
    config.runtime_progress = net::RuntimeProgress::Independent;
    std::error_code run_error;
    std::atomic<bool> run_returned{false};
    std::thread control([&] { run_error = engine->Run(config); run_returned.store(true); });
    const auto startup_deadline = std::chrono::steady_clock::now() + 3s;
    while (engine->Phase() != net::EnginePhase::Running && !run_returned.load() &&
           std::chrono::steady_clock::now() < startup_deadline) {
        std::this_thread::sleep_for(1ms);
    }
    int code = 0;
    try {
        if (engine->Phase() != net::EnginePhase::Running) { throw std::runtime_error("Engine not running"); }
        std::error_code error;
        auto address = net::Address::Ip("127.0.0.1", RedisPort(), error);
        if (error) { throw std::runtime_error(error.message()); }
        RunSingleConnection(engine, address);
        RunPool(engine, address);
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        code = 1;
    }
    engine->RequestClose();
    control.join();
    if (!run_returned.load() || engine->Phase() != net::EnginePhase::Stopped || run_error ||
        engine->LastFailure()) {
        std::cerr << "host drain failed: " << run_error.message() << '\n';
        code = 1;
    } else {
        std::cout << "host run_returned=1 joined=1 phase=Stopped\n";
    }
    engine.reset();
    return code;
}
} // namespace

int main() {
    const int code = Run(); // 全部业务对象在 Run 内销毁后，结束进程寿命的 runtime。
    std::cout.flush();
    std::cerr.flush();
    std::_Exit(code);
}
