/**
 * 回归测试：任务登记窗口与 Stop/WaitStopped 的竞争。Start 在私有登记屏障处
 * 被扣住（尚未计数、尚未检查封口），此时 RequestClose/WaitStopped 先完成；屏障
 * 释放后迟到登记必须看到封口并失败，不再预约任务，WaitStopped 后不复活。
 * 覆盖已进入登记的迟到 Start 被拒绝，以及停止后的重复公开 Start。
 */
#include <bbt/infra/http/Server.hpp>
#include <bbt/infra/http/detail/ServerTestAccess.hpp>
#include <bbt/coroutine/coroutine.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <thread>

using namespace std::chrono_literals;
namespace http = bbt::infra::http;
namespace net = bbt::infra::net;

static std::atomic<int> stage{0}, permit{0};

template <class P> static bool Until(P predicate)
{
    const auto end = std::chrono::steady_clock::now() + 3s;
    while (std::chrono::steady_clock::now() < end) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

int main()
{
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
    g_scheduler->Start();

    auto engine = net::Engine::Create(std::shared_ptr<net::RawHandler>{});
    auto made = http::Server::Create(engine, [](bbt::infra::IncomingCallContext, http::IncomingRequest) {
        return bbt::infra::result<http::Response>::ok({200, {}, "ok"});
    });
    if (!made) {
        std::_Exit(2);
    }
    auto server = std::move(made).value();

    net::EngineConfig config;
    config.runtime_progress = net::RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    std::error_code address_error, run_error;
    config.listen_addresses = {net::Address::Ip("127.0.0.1", 0, address_error)};
    if (address_error) {
        std::_Exit(2);
    }

    std::thread runner([&] { run_error = engine->Run(config); });
    if (!Until([&] { return engine->Phase() == net::EnginePhase::Running; })) {
        (void)engine->Close();
        runner.join();
        std::_Exit(2);
    }

    int result = 2;

    /**
     * 阶段 A：登记屏障内 Close/WaitStopped，迟到 Start 必须失败
     */
    {
        bool start_ok = false;
        http::ServerTestAccess::SetRegistrationBarrierOnce(*server, [] {
            stage = 1;
            const auto end = std::chrono::steady_clock::now() + 3s;
            while (permit.load() < 1 && std::chrono::steady_clock::now() < end) {
                std::this_thread::sleep_for(1ms);
            }
        });
        std::thread starter([&] { start_ok = bool(server->Start()); });
        if (Until([] { return stage.load() == 1; })) {
            const std::size_t tasks_at_submit = server->Stats().tasks;
            std::printf("PHASE-A before_stop tasks=%zu\n", tasks_at_submit);
            (void)server->Close(bbt::infra::CloseOptions::Immediate());
            const bool first_wait = bool(server->Close(
                bbt::infra::CloseOptions{
                    bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + 2s}));
            const std::size_t tasks_after_wait = server->Stats().tasks;
            const bool listener_stopped = engine->AcceptingStopped();
            std::printf(
                "PHASE-A first_wait_stopped=%d tasks_after_wait=%zu listener_stopped=%d\n",
                first_wait ? 1 : 0,
                tasks_after_wait,
                listener_stopped ? 1 : 0);
            permit = 1;
            starter.join();
            const auto tasks_after_start = server->Stats().tasks;
            std::printf("PHASE-A start_ok=%d tasks_after_start=%zu\n", start_ok ? 1 : 0, tasks_after_start);

            /**
             * 即使错误实现已自行回收任务，迟到 Start 成功仍必须使回归失败。
             */
            if (tasks_at_submit == 0 && first_wait && tasks_after_wait == 0 && listener_stopped && !start_ok
                && tasks_after_start == 0) {
                result = 0;
            } else {
                result = 2;
            }
        } else {
            permit = 1;
            starter.join();
            std::_Exit(2);
        }
    }

    /**
     * 阶段 B：停止后的重复公开 Start 必须被外层检查拒绝，不计数
     */
    {
        bool second_start = bool(server->Start());
        std::size_t tasks_b = server->Stats().tasks;
        std::printf("PHASE-B second_start=%d tasks=%zu\n", second_start ? 1 : 0, tasks_b);
        if (second_start || tasks_b != 0) {
            std::_Exit(2);
        }
    }

    /**
     * 阶段 C：最终 WaitStopped 与资源收口
     */
    {
        bool final_wait = bool(server->Close(
            bbt::infra::CloseOptions{
                bbt::infra::CloseMode::Immediate, true, {}, std::chrono::steady_clock::now() + 2s}));
        (void)engine->Close();
        runner.join();
        auto stats = server->Stats();
        const bool stopped = engine->Phase() == net::EnginePhase::Stopped;
        const auto connections = engine->CountConnections();
        std::printf(
            "RESULT-C final_wait=%d final_tasks=%zu run_error=%d stopped=%d connections=%zu\n",
            final_wait ? 1 : 0,
            stats.tasks,
            run_error.value(),
            stopped ? 1 : 0,
            connections);
        if (!final_wait || stats.tasks != 0 || run_error || !stopped || connections != 0) {
            result = 2;
        }
    }

    std::printf("F1 regression: stop-before-registration rejected and resources drained=%d\n", result == 0);
    std::fflush(nullptr);
    std::_Exit(result);
}
