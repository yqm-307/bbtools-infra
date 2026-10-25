#include <bbt/infra/redis/CoRedisCli.hpp>
#include <bbt/coroutine/coroutine.hpp>

#include <chrono>
#include <cstdlib>
#include <future>
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

int Run() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 1;
    g_bbt_coroutine_config->m_cfg_stack_size = 128 * 1024;
    g_scheduler->Start();
    auto engine = std::make_shared<net::Engine>(nullptr);
    net::EngineConfig config;
    config.runtime_progress = net::RuntimeProgress::Independent;
    std::error_code run_error;
    std::atomic<bool> returned{false};
    std::thread control([&] { run_error = engine->Run(config); returned.store(true); });
    const auto startup_deadline = std::chrono::steady_clock::now() + 3s;
    while (engine->Phase() != net::EnginePhase::Running && !returned.load() &&
           std::chrono::steady_clock::now() < startup_deadline) {
        std::this_thread::sleep_for(1ms);
    }
    int code = 0;
    std::shared_ptr<redis::CoRedisCli> client;
    bool api_registered = false;
    std::atomic<bool> api_done{false};
    try {
        if (engine->Phase() != net::EnginePhase::Running) { throw std::runtime_error("Engine not running"); }
        std::error_code error;
        auto address = net::Address::Ip("127.0.0.1", 6379, error);
        if (error) { throw std::runtime_error(error.message()); }
        auto created = redis::CoRedisCli::Create(engine, address);
        if (!created) { throw std::runtime_error(created.error().message); }
        client = std::move(created).value();
        std::random_device random;
        const std::string key = "bbt:redis-net:example:" + std::to_string(random()) + ":" + std::to_string(random());
        auto completion = std::make_shared<std::promise<void>>();
        auto future = completion->get_future();
        bool registered = false;
        // 宿主登记唯一 API 任务。完成信号之前释放任务捕获，且不再访问 adapter。
        g_scheduler->RegistCoroutineTask(128 * 1024, [client, key, completion, &api_done]() mutable {
            std::exception_ptr failure;
            try {
                Require(client->Connect(Options()));
                Require(client->Ping(Options()));
                Require(client->Set(key, "hello-net", Options()));
                auto value = client->Get(key, Options());
                if (!value || !value.value() || *value.value() != "hello-net") {
                    throw std::runtime_error("GET mismatch");
                }
                std::cout << "PING=PONG SET=OK GET=hello-net workers=1\n";
            } catch (...) { failure = std::current_exception(); }
            // 无论业务成败，只清理这个示例拥有的 key；故障后显式重连再尝试。
            try {
                auto cleanup = client->Delete({key}, Options());
                if (!cleanup) {
                    auto connected = client->Connect(Options());
                    if (connected) { cleanup = client->Delete({key}, Options()); }
                }
                auto remaining = client->Exists(key, Options());
                if (!cleanup || !remaining || remaining.value()) {
                    throw std::runtime_error("unique key cleanup failed");
                }
                std::cout << "cleanup unique_key=1 exists_after=0\n";
            } catch (...) { failure = std::current_exception(); }
            client.reset();
            api_done.store(true);
            if (failure) { completion->set_exception(failure); }
            else { completion->set_value(); }
        }, registered);
        api_registered = registered;
        if (!registered) { throw std::runtime_error("API task registration failed"); }
        // 普通 native 宿主等待；业务 worker 继续推进，不在协程内同步 wait。
        future.get();
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        code = 1;
    }
    if (client) { client->Close(); }
    engine->RequestClose();
    control.join();
    if (!returned.load() || engine->Phase() != net::EnginePhase::Stopped || run_error || engine->LastFailure() ||
        (api_registered && !api_done.load())) {
        std::cerr << "host drain failed: " << run_error.message() << '\n';
        code = 1;
    } else {
        std::cout << "host run_returned=1 joined=1 phase=Stopped api_tasks=" << api_done.load()
                  << "/" << api_registered << '\n';
    }
    client.reset();
    engine.reset();
    return code;
}
} // namespace

int main() {
    const int code = Run(); // 全部业务对象在 Run 内销毁后，结束进程寿命的 runtime。
    std::cout.flush();
    std::cerr.flush();
    std::_Exit((std::cout && std::cerr) ? code : 1);
}
