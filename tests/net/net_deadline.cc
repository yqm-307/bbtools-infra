// 有限 deadline 零/负剩余时间窗口回归（P3-a F1 定向包）。
//
// 仅本可执行文件在进程内 interpose clock_gettime（编译期符号优先于 DSO，
// 不用 LD_PRELOAD，不影响其他测试与产品）：两次受控单调采样分别落在等待
// 入口的“先判过期、再采样剩余期限”之间，使 deadline 刚过（ceil 后 0ms）
// 或已过 2ms（负）。断言采样次数恰为 2，证明命中的是两次采样间窗口而非
// 入口已过期（入口已过期只采样 1 次即返回）。契约：两窗口都必须返回
// WaitStatus::TimedOut、SubmitStatus::NotSubmitted、start 零副作用。
// 每进程 runtime 初始化一次，无 Stop；watchdog 超时即诊断后 _Exit 收口，
// 不依赖 ctest 超时伪装红灯。
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <memory>
#include <string>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/infra/net/detail/CoIoOperation.hpp>

using namespace std::chrono_literals;
using bbt::coroutine::WaitStatus;
using bbt::infra::net::CoIoOperation;
using bbt::infra::net::EventLoop;
using bbt::infra::net::SubmitStatus;

struct ControlledSamples {
    int phase{0};  // 0=放行；1=下一次采样取 deadline 前 1ns；2=下一次取 deadline 后
    int hits{0};
    long long deadline_ns{0};
    long long late_ns{1};
};
static thread_local ControlledSamples samples;

extern "C" int clock_gettime(clockid_t clock, timespec* out) noexcept {
    if (clock == CLOCK_MONOTONIC && samples.phase != 0) {
        const long long ns =
            samples.deadline_ns + (samples.phase == 1 ? -1 : samples.late_ns);
        out->tv_sec = ns / 1000000000LL;
        out->tv_nsec = ns % 1000000000LL;
        ++samples.hits;
        samples.phase = samples.phase == 1 ? 2 : 0;
        return 0;
    }
    return static_cast<int>(::syscall(SYS_clock_gettime, clock, out));
}

struct Outcome {
    WaitStatus wait_status{WaitStatus::InvalidContext};
    SubmitStatus submit_status{SubmitStatus::NotSubmitted};
    int start_effects{0};
    int hits{0};
};

static int RunCase(const std::string& mode) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 1;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start();  // 每进程一次；无 Stop，watchdog 路径以 _Exit 收口。
    EventLoop loop(2, 1);  // 容量 2、每批 1 条；构造本身不启动 owner。
    if (!loop.Start()) {
        std::cerr << "failed to start network owner\n";
        return 2;
    }
    auto promise = std::make_shared<std::promise<Outcome>>();
    auto done = promise->get_future();
    std::atomic<int> start_effects{0};
    g_scheduler->RegistCoroutineTask(
        65536,
        [promise, &loop, &start_effects, mode] {
            try {
                const auto deadline = std::chrono::steady_clock::now() + 1ms;
                samples.deadline_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    deadline.time_since_epoch()).count();
                samples.late_ns = mode == "negative_window" ? 2000000LL : 1LL;
                samples.hits = 0;
                samples.phase = 1;
                const auto result =
                    CoIoOperation::Run(loop, {deadline}, [&start_effects](auto) { ++start_effects; });
                const int hits = samples.hits;
                samples.phase = 0;
                promise->set_value({result.wait_status, result.submit_status, start_effects.load(), hits});
            } catch (...) {
                samples.phase = 0;
                promise->set_exception(std::current_exception());
            }
        },
        "net.reactor_deadline");

    // 有界 watchdog：旧实现 0ms 窗口会悬挂，收口为非 0 诊断（不能靠测试超时）。
    if (done.wait_for(2s) != std::future_status::ready) {
        std::cerr << "mode=" << mode
                  << " RUN_HUNG: finite-deadline wait did not return within 2s"
                  << " (0ms window lost the timeout), start_effects=" << start_effects.load() << '\n';
        std::_Exit(1);
    }
    const auto outcome = done.get();
    std::cout << "mode=" << mode << " wait_status=" << static_cast<int>(outcome.wait_status)
              << " submit_status=" << static_cast<int>(outcome.submit_status)
              << " start_effects=" << outcome.start_effects
              << " controlled_samples=" << outcome.hits << std::endl;
    const bool ok = outcome.wait_status == WaitStatus::TimedOut
        && outcome.submit_status == SubmitStatus::NotSubmitted && outcome.start_effects == 0
        && outcome.hits == 2;
    if (!ok) { std::cerr << "mode=" << mode << " CONTRACT_VIOLATION (see line above)\n"; }
    return ok ? 0 : 1;
}

#include "ProcessRuntimeTestMain.hpp"

static int RunDeadlineCases(int argc, char** argv) {
    const std::string mode = argc > 1 ? argv[1] : "";
    if (mode != "zero_window" && mode != "negative_window") {
        std::cerr << "usage: Test_net_deadline zero_window|negative_window\n";
        return 2;
    }
    return RunCase(mode);
}

int main(int argc, char** argv) {
    // 测试进程退出边界：局部 RAII 收口 → 刷新输出 → _Exit，见
    // ProcessRuntimeTestMain.hpp 头注（静态析构竞态为已知限制，未修复）。
    return bbt_net_test::RunTestProcessExit([&] { return RunDeadlineCases(argc, argv); });
}
