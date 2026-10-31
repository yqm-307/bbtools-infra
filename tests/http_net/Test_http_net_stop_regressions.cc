// 回归测试：Server::Submit 在与 Stop 同一 mutex 的任务登记预约处校验 stopping，
// 封口后不再预约/登记新任务，WaitStopped 后不再复活；TaskLease 未计数回退不下溢。
// 覆盖已进入 Submit 的迟到 Start 被拒绝，以及停止后的重复公开 Start；不模拟 session Submit。
//
// 通过 --wrap=_Znwm 在 TaskLease（48 字节）分配处暂停，制造 Start 与 Close/WaitStopped
// 的确定性竞态窗口。与原 start-stop-task-probe 同一路径，但以正式用例形式断言并 exit 0。
//
// 编译链接需加 -Wl,--wrap=_Znwm；本文件为唯一使用 wrapped operator new 的 TU，
// 不与其它测试共享进程，避免污染。
#include <bbt/infra/http/Server.hpp>
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

static thread_local int allocation_phase = 0;
static std::atomic<int> stage{0}, permit{0};

extern "C" void* __real__Znwm(std::size_t);
extern "C" void* __wrap__Znwm(std::size_t size) {
    if (allocation_phase != 0) {
        if ((allocation_phase == 1 && size == 48) || allocation_phase == 2) {
            const int current = allocation_phase++;
            if (current == 2) { allocation_phase = 0; }
            stage = current;
            const auto end = std::chrono::steady_clock::now() + 3s;
            while (permit.load() < current && std::chrono::steady_clock::now() < end) {
                std::this_thread::sleep_for(1ms);
            }
        }
    }
    return __real__Znwm(size);
}

template<class P> static bool Until(P predicate) {
    const auto end = std::chrono::steady_clock::now() + 3s;
    while (std::chrono::steady_clock::now() < end) {
        if (predicate()) { return true; }
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

int main() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
    g_scheduler->Start();

    auto engine = std::make_shared<net::Engine>(std::shared_ptr<net::RawHandler>{});
    auto made = http::Server::Create(engine, [](bbt::infra::IncomingCallContext, http::IncomingRequest) {
        return bbt::infra::result<http::Response>::ok({200, {}, "ok"});
    });
    if (!made) { std::_Exit(2); }
    auto server = std::move(made).value();

    net::EngineConfig config;
    config.runtime_progress = net::RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    std::error_code address_error, run_error;
    config.listen_addresses = {net::Address::Ip("127.0.0.1", 0, address_error)};
    if (address_error) { std::_Exit(2); }

    std::thread runner([&] { run_error = engine->Run(config); });
    if (!Until([&] { return engine->Phase() == net::EnginePhase::Running; })) {
        engine->RequestClose(); runner.join(); std::_Exit(2);
    }

    int result = 2;

    // ---- 阶段 A：Submit 分配屏障内 Close/WaitStopped，迟到 Start 必须失败 ----
    {
        bool start_ok = false;
        std::thread starter([&] {
            allocation_phase = 1;
            start_ok = bool(server->Start());
            allocation_phase = 0;
        });
        if (Until([] { return stage.load() == 1; })) {
            std::size_t tasks_at_submit = server->Stats().tasks;
            std::printf("PHASE-A before_submit_allocation=1 tasks=%zu\n", tasks_at_submit);
            server->Close();
            bool first_wait = bool(server->WaitStopped(std::chrono::steady_clock::now() + 2s));
            std::size_t tasks_after_wait = server->Stats().tasks;
            std::printf("PHASE-A first_wait_stopped=%d tasks_after_wait=%zu listener_stopped=%d\n",
                        first_wait ? 1 : 0, tasks_after_wait,
                        engine->AcceptingStopped() ? 1 : 0);
            const bool listener_stopped = engine->AcceptingStopped();
            permit = 2;
            starter.join();
            const auto tasks_after_start = server->Stats().tasks;
            std::printf("PHASE-A start_ok=%d tasks_after_start=%zu\n",
                        start_ok ? 1 : 0, tasks_after_start);
            // 即使错误实现已自行回收任务，迟到 Start 成功仍必须使回归失败。
            if (tasks_at_submit == 0 && first_wait && tasks_after_wait == 0 &&
                listener_stopped && !start_ok && tasks_after_start == 0) {
                result = 0;
            } else {
                result = 2;
            }
        } else {
            starter.join();
            std::_Exit(2);
        }
    }

    // ---- 阶段 B：停止后的重复公开 Start 必须被外层检查拒绝，不计数 ----
    {
        // 再次 Start 应失败（已 stopping/closed）
        bool second_start = bool(server->Start());
        std::size_t tasks_b = server->Stats().tasks;
        std::printf("PHASE-B second_start=%d tasks=%zu\n", second_start ? 1 : 0, tasks_b);
        if (second_start || tasks_b != 0) { std::_Exit(2); }
    }

    // ---- 阶段 C：最终 WaitStopped 与资源收口 ----
    {
        bool final_wait = bool(server->WaitStopped(std::chrono::steady_clock::now() + 2s));
        engine->RequestClose();
        runner.join();
        auto stats = server->Stats();
        const bool stopped = engine->Phase() == net::EnginePhase::Stopped;
        const auto connections = engine->CountConnections();
        std::printf("RESULT-C final_wait=%d final_tasks=%zu run_error=%d stopped=%d connections=%zu\n",
                    final_wait ? 1 : 0, stats.tasks, run_error.value(), stopped ? 1 : 0, connections);
        if (!final_wait || stats.tasks != 0 || run_error || !stopped || connections != 0) {
            result = 2;
        }
    }

    std::printf("F1 regression: stop-before-submit rejected and resources drained=%d\n", result == 0);
    std::fflush(nullptr);
    std::_Exit(result);
}
