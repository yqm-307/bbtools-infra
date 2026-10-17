// Engine 全局单 ticker / OnTick（Options::ticker）的公共表面验证。
//
// 覆盖面（沿 ticker-contract-131 §5，删除无据的 1h 归一边界）：
//   - 周期投递：动态回环 + ticker=true，计数有界增长、相邻间隔以 steady_clock 观测的
//     下界（>= 返回的 delay）、峰值恰 1；同期真实客户端回显成功（业务与 tick 并存）。
//   - 不重入 + 挂起不堵别人：OnTick 挂起远超返回间隔时计数冻结，其它连接真实回显进展。
//   - Stop 门与在途寿命：在途 tick 挂起时 Stop 有界返回且不等 handler；Engine 作用域
//     结束（owner/context/timer 已销毁）后在途 tick 仍安全收尾；测试不持 handler 引用，
//     以释放收据证明（不是测试自持引用造成的假证明）。
//   - 返回 0 / 负值：不再投递但 Engine 仍 Running，端口仍接纳与回显。
//   - 抛出：ticker 停止、Engine 继续服务、Run/Stop 语义不变；极大返回值（nanoseconds::max）
//     不溢出/不崩溃（白盒核对 timer 到期时刻落在 steady_clock 可表示范围内）、仍可取消
//     （长等待下 Stop/Join 不挂）；其它 unsupported（reuse_port）仍显式拒绝。
//
// 客户端一律用公共 NativeSocket；除极大返回值 case 的白盒到期时刻核对（直接读
// detail::ServerEngine 的私有 ticker timer，仅用于核对「饱和而非业务截断」）外只用公共面。
// 不使用 sleep 猜确定性窗口：所有等待都基于测试自建的收据（原子标志）与有界轮询，
// 「不再投递」的负向断言一律先取得「在途已返回」收据，再有界窗口内核对计数不增长。

#include <dirent.h>
#include <poll.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/net/Action.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/EventHandler.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/Options.hpp>

// 白盒：直接读 detail::ServerEngine 的私有 ticker timer（仅 case throw_and_extreme_delay
// 的到期时刻核对）。先加载依赖头，避免修改标准库/Boost 的访问控制。
#include <bbt/infra/net/detail/EventLoop.hpp>
#define private public
#include <bbt/infra/net/detail/ServerEngine.hpp>
#undef private

#include "ProcessRuntimeTestMain.hpp"

using namespace std::chrono_literals;

// ---- 测试私有 linkwrap（只在本可执行文件内改调用目标，不新增生产注入接口）----
// 1) EventLoop::Submit(Command)：对非 owner 线程的调用注入一次「队列满」（Full），用于
//    真实覆盖重挂被拒路径；owner 线程的 handoff 不受影响。
//    同一入口还被 case 8 用作**确定性窗口闸门**：把非 owner 线程（Processer 上的 tick 重挂）
//    精确停在 Submit 入口，即 135 P1 的「已读到 loop/owner、尚未 Submit」窗口。
// 2) EventLoop::Submit(Command, error_code&)：在 owner 控制命令投递前阻塞，制造「Stop 持
//    control_mutex_ 且命令未完成的真实长窗口」（等价慢命令）；同一入口在 case 8 用作
//    「Stop 是否越过撤空门（CloseTickGate）到达 owner 命令」的收据。
// 注意：ServerEngine::PostArmTick 与 TickTask 同 TU，--wrap 只能改名**未定义**引用，故不能
// 用 linkwrap 拦截对 TickTask/PostArmTick 的**同 TU 内部调用**（两者都是未定义引用时才可改）；
// Stop 长锁窗口与重挂窗口都用白盒/真实路径白盒直调同一生产函数覆盖（见 case 6、case 8）。
struct TickInterleaveGates {
    std::mutex mutex;
    std::condition_variable cv;
    bool stop_submit_armed{false};
    bool stop_submit_entered{false};
    bool stop_submit_release{false};
    std::atomic<bool> reject_next_tick_post{false};
    std::atomic<bool> reject_done{false};
    // case 8：重挂 Submit 入口窗口闸门 + 完成收据。
    bool post_window_armed{false};
    bool post_window_entered{false};
    bool post_window_release{false};
    std::atomic<bool> post_submitted{false};
    std::atomic<int> post_status{-1};
};
TickInterleaveGates g_gates;

void GateSet(bool TickInterleaveGates::*field) {
    {
        std::lock_guard<std::mutex> lock(g_gates.mutex);
        g_gates.*field = true;
    }
    g_gates.cv.notify_all();
}

bool GateConsume(bool TickInterleaveGates::*field) {
    std::lock_guard<std::mutex> lock(g_gates.mutex);
    const bool armed = g_gates.*field;
    g_gates.*field = false;
    return armed;
}

bool GateGet(bool TickInterleaveGates::*field) {
    std::lock_guard<std::mutex> lock(g_gates.mutex);
    return g_gates.*field;
}

bool GateWait(bool TickInterleaveGates::*field, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(g_gates.mutex);
    return g_gates.cv.wait_for(lock, timeout, [field] { return g_gates.*field; });
}

extern "C" {
bbt::infra::net::SubmitStatus WrapSubmitOne(bbt::infra::net::EventLoop* loop,
    bbt::infra::net::EventLoop::Command command)
    __asm__("__wrap__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEE");
bbt::infra::net::SubmitStatus RealSubmitOne(bbt::infra::net::EventLoop* loop,
    bbt::infra::net::EventLoop::Command command)
    __asm__("__real__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEE");
bbt::infra::net::SubmitStatus WrapSubmitTwo(bbt::infra::net::EventLoop* loop,
    bbt::infra::net::EventLoop::Command command, std::error_code* error)
    __asm__("__wrap__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEERSt10error_code");
bbt::infra::net::SubmitStatus RealSubmitTwo(bbt::infra::net::EventLoop* loop,
    bbt::infra::net::EventLoop::Command command, std::error_code* error)
    __asm__("__real__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEERSt10error_code");
}

bbt::infra::net::SubmitStatus WrapSubmitOne(bbt::infra::net::EventLoop* loop,
    bbt::infra::net::EventLoop::Command command)
{
    const bool non_owner = loop->OwnerId() != std::this_thread::get_id();
    if (non_owner) {
        // 只拒非 owner 线程的调用（tick 重挂来自 Processer）；owner 线程的 handoff 不受影响。
        if (g_gates.reject_next_tick_post.exchange(false)) {
            g_gates.reject_done.store(true);
            return bbt::infra::net::SubmitStatus::Full;
        }
        // case 8：把非 owner 线程的重挂精确停在 Submit 入口（窗口打开），由测试线程放行。
        if (GateConsume(&TickInterleaveGates::post_window_armed)) {
            GateSet(&TickInterleaveGates::post_window_entered);
            (void)GateWait(&TickInterleaveGates::post_window_release, 20s);
        }
    }
    const auto status = RealSubmitOne(loop, std::move(command));
    if (non_owner) {
        // 收据：真实 Submit 已在（仍存活的）loop 上返回，返回码可核。
        g_gates.post_status.store(static_cast<int>(status));
        g_gates.post_submitted.store(true);
    }
    return status;
}

bbt::infra::net::SubmitStatus WrapSubmitTwo(bbt::infra::net::EventLoop* loop,
    bbt::infra::net::EventLoop::Command command, std::error_code* error)
{
    if (GateConsume(&TickInterleaveGates::stop_submit_armed)) {
        GateSet(&TickInterleaveGates::stop_submit_entered);
        (void)GateWait(&TickInterleaveGates::stop_submit_release, 20s);
    }
    return RealSubmitTwo(loop, std::move(command), error);
}

namespace {

using Clock = std::chrono::steady_clock;
using bbt::infra::net::Action;
using bbt::infra::net::Address;
using bbt::infra::net::AddressFamily;
using bbt::infra::net::Engine;
using bbt::infra::net::EventHandler;
using bbt::infra::net::ICoConnection;
using bbt::infra::net::NativeSocket;
using bbt::infra::net::Options;
using bbt::infra::net::SocketType;

#define CHECK(condition)                                                                  \
    do {                                                                                  \
        if (!(condition)) {                                                               \
            std::cerr << "CHECK failed: " #condition " @ " << __FILE__ << ":" << __LINE__  \
                      << std::endl;                                                       \
            std::abort();                                                                 \
        }                                                                                 \
    } while (0)

// 红跑对照：本文件对旧实现（TickState 尚无内部停投原因字段）也能编译并复现主因，
// 仅跳过新增内部原因断言（行为断言一字不减）。正常构建不定义该宏。
#if defined(BBT_NET_TICK_RED_BASELINE)
#define TICK_REASON_ASSERT(condition) do { (void)0; } while (0)
#else
#define TICK_REASON_ASSERT(condition) CHECK(condition)
#endif

// ---- 进程级观测 ----
int FdCount() {
    DIR* dir = ::opendir("/proc/self/fd");
    CHECK(dir != nullptr);
    int count = 0;
    while (::readdir(dir) != nullptr) { ++count; }
    ::closedir(dir);
    return count;
}

int ThreadCount() {
    std::ifstream status("/proc/self/status");
    CHECK(status.good());
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("Threads:", 0) == 0) {
            return std::atoi(line.c_str() + sizeof("Threads:") - 1);
        }
    }
    CHECK(false);
    return -1;
}

bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = Clock::now() + timeout;
    for (;;) {
        if (predicate()) { return true; }
        if (Clock::now() >= deadline) { return false; }
        std::this_thread::sleep_for(2ms);
    }
}

bool IsError(const std::error_code& error, std::errc expected) { return error == expected; }

// 测试私有 runtime 预热（单 Processer、真实挂起让出），与 engine_server_accept.cc 同范式。
void WarmRuntime() {
    static const bool started = [] {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 1;
        g_bbt_coroutine_config->m_cfg_stack_size = 65536;
        g_scheduler->Start();
        return true;
    }();
    CHECK(started);
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    const std::size_t stack = g_bbt_coroutine_config->m_cfg_stack_size;
    g_scheduler->RegistCoroutineTask(stack, [promise] {
        bbt::coroutine::sync::CoWaiter waiter;
        CHECK(waiter.Wait(bbt::coroutine::WaitOptions{Clock::now() + 5ms})
            == bbt::coroutine::WaitStatus::TimedOut);
        promise->set_value();
    }, "net.engine.tick.test.warm");
    CHECK(future.wait_for(10s) == std::future_status::ready);
    future.get();
}

// ---- TCP 客户端（公共 NativeSocket）----
NativeSocket OpenStream() {
    std::error_code error;
    auto socket = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    CHECK(!error && socket.Fd() >= 0);
    return socket;
}

NativeSocket ConnectTo(const Address& bound) {
    auto client = OpenStream();
    const auto connected = client.Connect(bound);
    CHECK(!connected || connected.value() == EINPROGRESS);
    const auto deadline = Clock::now() + 5s;
    while (client.PendingError()) {
        CHECK(Clock::now() < deadline);
        std::this_thread::sleep_for(1ms);
    }
    return client;
}

void SendAll(NativeSocket& socket, const std::string& data) {
    std::size_t sent = 0;
    const auto deadline = Clock::now() + 5s;
    while (sent < data.size()) {
        const auto result = socket.Send(data.data() + sent, data.size() - sent);
        if (result.bytes > 0) { sent += result.bytes; continue; }
        if (result.error) { CHECK(result.error.value() == EAGAIN || result.error.value() == EWOULDBLOCK); }
        CHECK(Clock::now() < deadline);
        std::this_thread::sleep_for(1ms);
    }
}

std::string ReceiveAtMost(NativeSocket& socket, std::size_t expected, std::chrono::milliseconds timeout) {
    std::string out;
    const auto deadline = Clock::now() + timeout;
    while (out.size() < expected && Clock::now() < deadline) {
        pollfd descriptor{socket.Fd(), POLLIN, 0};
        const int ready = ::poll(&descriptor, 1, 20);
        if (ready <= 0) { continue; }
        char buffer[512];
        const auto room = std::min(sizeof(buffer), expected - out.size());
        const auto result = socket.Receive(buffer, room);
        if (result.bytes > 0) { out.append(buffer, result.bytes); }
        else if (result.eof) { break; }
    }
    return out;
}

Address Loopback(std::uint16_t port) {
    std::error_code error;
    auto address = Address::Ip("127.0.0.1", port, error);
    CHECK(!error);
    return address;
}

// ---- ticker 观测面 ----
// 全部收据都是测试自建原子量：没有一项依赖 sleep 猜阶段。
struct TickProbe {
    std::atomic<int> count{0};           // OnTick 进入次数
    std::atomic<int> active{0};
    std::atomic<int> peak{0};            // 并发 OnTick 峰值（不重入/不并行）
    std::atomic<int> returned_index{0};  // 最近一次已返回的序号（收据）
    std::atomic<bool> entered{false};    // 至少一次进入（挂起闸的前置收据）
    std::atomic<bool> released{false};   // 挂起闸放行
    std::atomic<int> handler_destroyed{0};
    std::atomic<int> echo_count{0};      // 服务侧真实回显次数（业务进展证据）
    std::atomic<int> open_count{0};
    std::atomic<int> close_count{0};
    std::atomic<int> write_errors{0};
    std::atomic<int> discard_errors{0};
    std::atomic<long long> delay_ns{0};       // 正常返回值
    std::atomic<long long> stop_value_ns{0};  // 达到 stop_at 后的返回值（含 0/负）
    std::atomic<int> stop_at{0};              // 0 = 不启用
    std::atomic<int> hold_at{0};              // 在哪一次进入时挂起
    std::atomic<long long> hold_ns{0};
    std::atomic<int> throw_at{0};             // 在哪一次进入后抛出
    std::mutex mu;
    std::vector<Clock::time_point> entries;   // 进入时刻
    std::vector<Clock::time_point> exits;     // 返回时刻
};

class TickHandler final : public EventHandler {
public:
    explicit TickHandler(std::shared_ptr<TickProbe> probe) : probe_(std::move(probe)) {}
    ~TickHandler() override { probe_->handler_destroyed.fetch_add(1); }

    bool OnBoot() override { return true; }

    std::chrono::nanoseconds OnTick() override {
        const int index = probe_->count.fetch_add(1) + 1;
        const int active = probe_->active.fetch_add(1) + 1;
        int peak = probe_->peak.load();
        while (active > peak && !probe_->peak.compare_exchange_weak(peak, active)) {
        }
        {
            std::lock_guard<std::mutex> guard(probe_->mu);
            probe_->entries.push_back(Clock::now());
        }
        probe_->entered.store(true);
        if (probe_->hold_at.load() == index && probe_->hold_ns.load() > 0) {
            const auto deadline = Clock::now() + std::chrono::nanoseconds(probe_->hold_ns.load());
            while (!probe_->released.load() && Clock::now() < deadline) {
                bbt::coroutine::sync::CoWaiter waiter;
                (void)waiter.Wait(bbt::coroutine::WaitOptions{Clock::now() + 5ms});
            }
        }
        {
            std::lock_guard<std::mutex> guard(probe_->mu);
            probe_->exits.push_back(Clock::now());
        }
        probe_->active.fetch_sub(1);
        probe_->returned_index.store(index);
        if (probe_->throw_at.load() == index) { throw std::runtime_error("ontick-throw"); }
        const int stop_at = probe_->stop_at.load();
        if (stop_at != 0 && index >= stop_at) {
            return std::chrono::nanoseconds(probe_->stop_value_ns.load());
        }
        return std::chrono::nanoseconds(probe_->delay_ns.load());
    }

    Action OnOpen(ICoConnection& connection) override {
        (void)connection;
        probe_->open_count.fetch_add(1);
        return Action::None;
    }

    Action OnTraffic(ICoConnection& connection, bool wake) override {
        if (wake) { return Action::None; }
        const std::string data{connection.Peek()};
        if (data.empty()) { return Action::None; }
        probe_->echo_count.fetch_add(1);
        if (connection.Write(data, Clock::now() + 5s)) { probe_->write_errors.fetch_add(1); }
        if (connection.Discard(data.size())) { probe_->discard_errors.fetch_add(1); }
        return Action::None;
    }

    void OnClose(ICoConnection& connection, const std::error_code& reason) override {
        (void)connection;
        (void)reason;
        probe_->close_count.fetch_add(1);
    }

private:
    std::shared_ptr<TickProbe> probe_;
};

// 真实回显：连接 + 发送 + 收回（tick 与业务并存 / 挂起期间其它连接进展 / 服务仍可用）。
void ExpectEcho(const Address& bound, const std::string& payload) {
    auto client = ConnectTo(bound);
    SendAll(client, payload);
    CHECK(ReceiveAtMost(client, payload.size(), 5s) == payload);
    client.Close();
}

Options TickerOptions() {
    Options options;
    options.ticker = true;
    return options;
}

// ---- case 1：周期投递 + 间隔下界 + 业务并存 ----
int CasePeriodic() {
    WarmRuntime();
    auto probe = std::make_shared<TickProbe>();
    probe->delay_ns.store(20'000'000);   // 20ms
    auto handler = std::make_shared<TickHandler>(probe);
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), TickerOptions()));
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1 && addresses[0].Port() != 0);
        ExpectEcho(addresses[0], "tick-echo-1");
        CHECK(WaitUntil([&] { return probe->count.load() >= 3; }, 5s));
        CHECK(probe->peak.load() == 1);                       // 单 ticker 串行
        CHECK(probe->echo_count.load() == 1);
        {
            std::lock_guard<std::mutex> guard(probe->mu);
            CHECK(probe->entries.size() >= 3);
            CHECK(probe->exits.size() >= 2);
            // 相邻投递间隔（上一次返回 → 本次进入）不小于返回的 delay：重挂发生在 OnTick
            // 返回之后，因此下界只可能更宽松，不早于返回时刻。
            for (std::size_t i = 1; i < probe->exits.size() && i < probe->entries.size(); ++i) {
                const auto gap = probe->entries[i] - probe->exits[i - 1];
                CHECK(gap >= std::chrono::nanoseconds(probe->delay_ns.load()));
            }
        }
        // tick 仍在跑时业务继续可用。
        ExpectEcho(addresses[0], "tick-echo-2");
        CHECK(probe->count.load() >= 3);
        // 强停不承诺送达未开始的 OnClose（既有强停语义），因此这里只断言收口后无活跃连接。
        CHECK(!engine.Stop());
        CHECK(engine.CountConnections() == 0);
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
    CHECK(probe->write_errors.load() == 0 && probe->discard_errors.load() == 0);
    std::cout << "periodic: ok (ticks=" << probe->count.load() << ")" << std::endl;
    return 0;
}

// ---- case 2：挂起不重入 + 挂起期间其它连接真实进展 ----
int CaseNoReentryAndOthersProgress() {
    WarmRuntime();
    auto probe = std::make_shared<TickProbe>();
    probe->delay_ns.store(10'000'000);       // 10ms
    probe->hold_at.store(1);                 // 首次进入即挂起
    probe->hold_ns.store(3'000'000'000LL);   // 远超返回间隔（3s，由闸放行提前结束）
    auto handler = std::make_shared<TickHandler>(probe);
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), TickerOptions()));
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(WaitUntil([&] { return probe->entered.load(); }, 5s));   // 首 tick 已进入并挂起
        CHECK(probe->count.load() == 1);          // 挂起期间不得并行/重入第二个 tick
        CHECK(probe->peak.load() == 1);
        CHECK(probe->active.load() == 1);
        ExpectEcho(addresses[0], "progress-while-tick-holds");        // 其它连接真实进展
        CHECK(probe->echo_count.load() == 1);
        CHECK(probe->count.load() == 1);          // 仍在挂起：链上没有第二个 tick
        probe->released.store(true);
        CHECK(WaitUntil([&] { return probe->count.load() >= 3; }, 5s)); // 返回后链继续
        CHECK(probe->peak.load() == 1);           // 全程无重入
        CHECK(probe->active.load() <= 1);
        CHECK(!engine.Stop());
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
    std::cout << "no_reentry_and_others_progress: ok (ticks=" << probe->count.load() << ")" << std::endl;
    return 0;
}

// ---- case 3：Stop 门 + 在途 tick 跨 Engine 销毁的寿命/释放 ----
int CaseStopGateAndRelease() {
    WarmRuntime();
    auto probe = std::make_shared<TickProbe>();
    probe->delay_ns.store(10'000'000);
    probe->hold_at.store(1);
    probe->hold_ns.store(30'000'000'000LL);   // 只由闸放行结束
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    int count_at_stop = 0;
    {
        auto handler = std::make_shared<TickHandler>(probe);
        Engine engine(handler);
        handler.reset();   // 测试不持 handler 引用：handler 仅由 Engine/在途 tick 持有
        CHECK(probe->handler_destroyed.load() == 0);
        CHECK(!engine.Run(Loopback(0), TickerOptions()));
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(WaitUntil([&] { return probe->entered.load(); }, 5s));   // tick 在途（挂起）
        const auto start = Clock::now();
        CHECK(!engine.Stop());
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);
        CHECK(elapsed < 5s);                                  // 有界：不等挂起 handler/drain
        CHECK(!probe->released.load());
        count_at_stop = probe->count.load();
        CHECK(count_at_stop == 1);
        CHECK(probe->returned_index.load() == 0);             // 在途确实还没返回
        CHECK(engine.CountConnections() == 0);
        CHECK(!engine.Stop());                                // 幂等
        CHECK(IsError(engine.Run(Loopback(0), TickerOptions()), std::errc::operation_canceled));
        addresses.clear();
        CHECK(IsError(engine.LocalAddresses(addresses), std::errc::operation_not_permitted));
        // 作用域结束：owner/context/timer 真销毁，而在途 tick 仍挂起 ⇒ 跨销毁安全的前置条件。
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));   // timer 已取消 ⇒ Join 不挂
    CHECK(probe->returned_index.load() == 0);   // 仍未返回：跨销毁的确实是在途 tick
    probe->released.store(true);                // context 已销毁后放行在途 tick
    CHECK(WaitUntil([&] { return probe->returned_index.load() == 1; }, 10s));   // 自然收尾
    const int count_after_return = probe->count.load();
    CHECK(count_after_return == count_at_stop);
    // Stop 后不再开始：已有「在途已返回」收据，仍有界窗口内核对计数不增长（重挂被门拒绝）。
    CHECK(!WaitUntil([&] { return probe->count.load() != count_after_return; }, 500ms));
    CHECK(probe->peak.load() == 1);
    CHECK(WaitUntil([&] { return probe->handler_destroyed.load() == 1; }, 10s));  // 释放收据
    std::cout << "stop_gate_and_release: ok (ticks=" << count_after_return << ")" << std::endl;
    return 0;
}

// ---- case 4：返回 0 / 负值 → 停止投递但服务照常 ----
int RunZeroOrNegative(long long stop_value_ns, const char* label) {
    auto probe = std::make_shared<TickProbe>();
    probe->delay_ns.store(10'000'000);
    probe->stop_at.store(2);                 // 第一次立即投递 + 第二次（delay 后）后停止
    probe->stop_value_ns.store(stop_value_ns);
    auto handler = std::make_shared<TickHandler>(probe);
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), TickerOptions()));
        CHECK(WaitUntil([&] { return probe->returned_index.load() == 2; }, 5s));   // 已返回收据
        const int settled = probe->count.load();
        CHECK(settled == 2);
        // 停止 ticker ≠ 停止 Engine：仍 Running、地址可读、可接纳与回显。
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1);
        ExpectEcho(addresses[0], std::string("after-stop-tick-") + label);
        CHECK(probe->echo_count.load() == 1);
        CHECK(!WaitUntil([&] { return probe->count.load() != settled; }, 500ms));  // 不再投递
        CHECK(probe->peak.load() == 1);
        CHECK(!engine.Stop());
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
    std::cout << "stop_on_" << label << ": ok" << std::endl;
    return 0;
}

int CaseStopOnZeroOrNegative() {
    WarmRuntime();
    const int zero = RunZeroOrNegative(0, "zero");
    if (zero != 0) { return zero; }
    return RunZeroOrNegative(-5'000'000, "negative");
}

// ---- case 5：抛出 / 极大返回值（饱和 + 可取消）/ 其它 unsupported 仍拒绝 ----
int CaseThrowExtremeAndUnsupported() {
    WarmRuntime();
    // (a) OnTick 抛出：ticker 停止、Engine 继续服务、Run/Stop 语义不变。
    {
        auto probe = std::make_shared<TickProbe>();
        probe->delay_ns.store(10'000'000);
        probe->throw_at.store(2);
        auto handler = std::make_shared<TickHandler>(probe);
        const int threads_before = ThreadCount();
        const int fds_before = FdCount();
        std::vector<Address> addresses;
        {
            Engine engine(handler);
            CHECK(!engine.Run(Loopback(0), TickerOptions()));
            CHECK(WaitUntil([&] { return probe->returned_index.load() == 2; }, 5s));
            const int settled = probe->count.load();
            CHECK(settled == 2);
            CHECK(!engine.LocalAddresses(addresses));
            ExpectEcho(addresses[0], "after-throw");      // 连接仍可用（异常不传播、不封口）
            CHECK(!WaitUntil([&] { return probe->count.load() != settled; }, 500ms));
            CHECK(!engine.Stop());
            CHECK(IsError(engine.Run(Loopback(0), TickerOptions()), std::errc::operation_canceled));
        }
        CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
        CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
        std::cout << "throw_stops_ticker: ok" << std::endl;
    }
    // (b) 极大返回值：按 steady_clock 可表示范围饱和（不是 1h 之类业务截断），不溢出、
    //     不崩溃；服务继续可用；长等待下 Stop 取消 ⇒ Join 不挂。
    {
        auto probe = std::make_shared<TickProbe>();
        probe->delay_ns.store(static_cast<long long>(std::chrono::nanoseconds::max().count()));
        auto handler = std::make_shared<TickHandler>(probe);
        std::error_code error;
        auto server = bbt::infra::net::ServerEngine::Create(handler, error);
        CHECK(server && !error);
        CHECK(!server->Run(Loopback(0), TickerOptions()));
        CHECK(WaitUntil([&] { return probe->count.load() >= 1; }, 5s));   // 首次立即投递
        // 等重挂完成（受管协程返回 → 非阻塞 owner 命令 → 挂 timer）：到期时刻远大于 1 年，
        // 说明巨大 delay 被饱和到时钟可表示上限附近而非截断成业务时限。
        CHECK(WaitUntil([&] {
            const auto* timer = server->tick_timer_.get();
            return timer != nullptr && timer->expiry() > Clock::now() + std::chrono::hours(24 * 365);
        }, 5s));
        const auto* timer = server->tick_timer_.get();
        CHECK(timer != nullptr);
        CHECK(timer->expiry() <= Clock::time_point::max());   // 未溢出到可表示范围之外
        CHECK(timer->expiry() > Clock::now() + std::chrono::hours(24 * 365));
        CHECK(probe->count.load() == 1);                      // 长等待期间不再投递
        std::vector<Address> addresses;
        CHECK(!server->LocalAddresses(addresses));
        ExpectEcho(addresses[0], "extreme-delay");            // 服务照常
        const auto start = Clock::now();
        CHECK(!server->Stop());
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);
        CHECK(elapsed < 5s);                                  // 取消在途长等待：Join 不挂
        CHECK(WaitUntil([&] { return probe->count.load() == 1; }, 500ms));
        std::cout << "extreme_delay_saturates_and_cancels: ok" << std::endl;
    }
    // (c) ticker 已接线但其它 unsupported 仍显式拒绝（不静默忽略）。
    {
        auto probe = std::make_shared<TickProbe>();
        auto handler = std::make_shared<TickHandler>(probe);
        const int threads_before = ThreadCount();
        const int fds_before = FdCount();
        Engine engine(handler);
        Options options = TickerOptions();
        options.reuse_port = true;
        CHECK(IsError(engine.Run(Loopback(0), options), std::errc::operation_not_supported));
        CHECK(probe->count.load() == 0);
        CHECK(ThreadCount() == threads_before);
        CHECK(FdCount() == fds_before);
        std::cout << "other_unsupported_still_rejected: ok" << std::endl;
    }
    return 0;
}

// ---- case 6：Stop 长锁窗口内重挂不得阻塞唯一 Processer（132 审查 P2 主因）----
// 确定性编排（全部是显式收据 + 有界等待，不用 sleep 猜阶段）：
//   1) 唯一 Processer 上跑独立哨兵协程（1ms 让出一次），先证明它在跑；
//   2) 真实 Stop 线程持 control_mutex_，并阻塞在 owner 控制命令投递前（真实长窗口，
//      等价于 15s 命令等待中的慢命令）；
//   3) 在同一（唯一）Processer 上执行 TickTask 返回后**同一行**的重挂调用
//      PostArmTick(state, delay)（白盒直调同一生产函数；TickTask→PostArmTick 只差
//      这一行，线程/锁状态与生产一致）；
//   4) 在**不释放 Stop 闸门**的前提下要求哨兵有进展；失败也先放行再断言。
// 旧实现在 PostArmTick 入口取 control_mutex_ ⇒ 与 Stop 长锁重叠时 Processer 整段停摆。
int CaseStopLockInterleave() {
    WarmRuntime();
    auto probe = std::make_shared<TickProbe>();
    probe->delay_ns.store(10'000'000);
    auto handler = std::make_shared<TickHandler>(probe);
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    std::atomic<bool> sentinel_stop{false};
    std::atomic<bool> sentinel_exited{false};
    std::atomic<long long> sentinel{0};
    std::atomic<bool> rearm_started{false};
    std::atomic<bool> rearm_returned{false};
    std::error_code stop_error;
    {
        std::error_code error;
        auto server = bbt::infra::net::ServerEngine::Create(handler, error);
        CHECK(server && !error);
        CHECK(!server->Run(Loopback(0), TickerOptions()));
        CHECK(!server->LocalAddresses(addresses));
        CHECK(WaitUntil([&] { return probe->count.load() >= 1; }, 5s));
        // 独立哨兵协程：与 ticker 同一个（唯一）Processer；Processer 一旦被阻塞它就冻结。
        const std::size_t stack = g_bbt_coroutine_config->m_cfg_stack_size;
        g_scheduler->RegistCoroutineTask(stack, [&sentinel, &sentinel_stop, &sentinel_exited] {
            while (!sentinel_stop.load()) {
                sentinel.fetch_add(1);
                bbt::coroutine::sync::CoWaiter waiter;
                (void)waiter.Wait(bbt::coroutine::WaitOptions{Clock::now() + 1ms});
            }
            sentinel_exited.store(true);
        }, "net.engine.tick.sentinel");
        CHECK(WaitUntil([&] { return sentinel.load() >= 5; }, 5s));   // 哨兵在跑
        // 真实 Stop：持 control_mutex_ 且阻塞在 owner 控制命令投递前（长窗口）。
        GateSet(&TickInterleaveGates::stop_submit_armed);
        std::thread stopper([&server, &stop_error] { stop_error = server->Stop(); });
        CHECK(GateWait(&TickInterleaveGates::stop_submit_entered, 5s));
        // 唯一 Processer 上的重挂调用（TickTask 返回后的同一行）。
        const auto state = server->tick_state_;
        g_scheduler->RegistCoroutineTask(stack,
            [state, server, &rearm_started, &rearm_returned] {
                rearm_started.store(true);
                server->PostArmTick(state, std::chrono::nanoseconds(10'000'000));
                rearm_returned.store(true);
            }, "net.engine.tick.rearm");
        CHECK(WaitUntil([&] { return rearm_started.load(); }, 5s));
        const long long baseline = sentinel.load();
        const bool progressed = WaitUntil([&] { return sentinel.load() >= baseline + 20; }, 3s);
        // 失败也先放行 Stop 闸门（不让断言把 Stop 线程留在锁内），再断言。
        GateSet(&TickInterleaveGates::stop_submit_release);
        stopper.join();
        sentinel_stop.store(true);
        CHECK(WaitUntil([&] { return sentinel_exited.load(); }, 10s));
        CHECK(progressed);                    // 唯一 Processer 在 Stop 长锁窗口内仍有进展
        CHECK(rearm_returned.load());         // 重挂调用在停机窗口内已返回（不等 Stop 锁）
        CHECK(!stop_error);                   // Stop 真实成功
        // 门已关：Stop 后不再投递（先取收据，再有界窗口核对不增长）。
        const int settled = probe->count.load();
        CHECK(!WaitUntil([&] { return probe->count.load() != settled; }, 300ms));
        CHECK(server->CountConnections() == 0);
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
    std::cout << "stop_lock_interleave: ok (sentinel=" << sentinel.load() << ")" << std::endl;
    return 0;
}

// ---- case 7：重挂被 owner 队列真实拒绝（Full）⇒ ticker 明确停止、Engine 服务保留 ----
// 注入只作用于非 owner 线程的 Submit（即 tick 重挂）；owner 线程的 handoff 照常。
int CaseSubmitRejectStopsTicker() {
    WarmRuntime();
    auto probe = std::make_shared<TickProbe>();
    probe->delay_ns.store(10'000'000);
    auto handler = std::make_shared<TickHandler>(probe);
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        std::error_code error;
        auto server = bbt::infra::net::ServerEngine::Create(handler, error);
        CHECK(server && !error);
        // 在首次投递之前装好注入：首 tick 返回后的重挂会被 owner 队列拒绝一次（Full）。
        g_gates.reject_next_tick_post.store(true);
        CHECK(!server->Run(Loopback(0), TickerOptions()));
        CHECK(!server->LocalAddresses(addresses));
        CHECK(WaitUntil([&] { return g_gates.reject_done.load(); }, 5s));   // 真实注入已发生
        CHECK(WaitUntil([&] { return probe->returned_index.load() == 1; }, 5s));
        const int settled = probe->count.load();
        CHECK(settled == 1);
        // 可核内部状态：停投原因是「投递被拒」，不是无声消失（不新增公共 API）。
        TICK_REASON_ASSERT(server->tick_state_ != nullptr
            && server->tick_state_->reason
                == bbt::infra::net::ServerEngine::TickState::StopReason::PostRejected);
        CHECK(server->tick_state_->closed);   // ticker 已停投（Engine 仍 Running）
        // ticker 停 ≠ 服务停：端口仍接纳与回显，正常连接不受影响。
        ExpectEcho(addresses[0], "after-reject");
        CHECK(probe->echo_count.load() == 1);
        CHECK(!WaitUntil([&] { return probe->count.load() != settled; }, 500ms));
        CHECK(!server->Stop());
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
    std::cout << "submit_reject_stops_ticker: ok" << std::endl;
    return 0;
}

// ---- case 8：撤空门与重挂投递必须在同一临界区（135 P1：锁外裸 loop/owner）----
// 判据（全部显式收据 + 有界等待，不用 sleep 猜阶段；收据是「谁先越过哪道门」而不是时刻）：
//   1) 首 tick 正常投递后，用测试私有 --wrap=EventLoop::Submit(Command) 把**非 owner 线程**
//      （Processer 上的 tick 重挂）精确停在 Submit 入口 —— 即 135 P1 的「已读到句柄、尚未
//      Submit」窗口。本 case 不建立任何客户端连接 ⇒ 该入口的非 owner 调用只有 tick 重挂，
//      不存在 handoff 混入（handoff 只在有连接时发生）。
//   2) 此刻同一台 Engine 上跑真实 Stop()：新实现下 StopLocked 的第一步 CloseTickGate 必须
//      阻塞在同一把 state->mutex（撤空门）上；旧实现下 CloseTickGate 不阻塞，Stop 会继续
//      走到 owner 控制命令投递（第二个闸门 WSSubmitTwo 记录「已越过撤空门」）。
//   3) 收据 = 「Stop 已越过撤空门（进入 owner 命令投递）而重挂 Submit 尚未执行」。该收据成立
//      即 135 P1 时序 1→2 成立（撤空/销毁与投递无同一同步点）⇒ 直接判红；此时不释放重挂闸门、
//      不访问任何已被/将被回收的 loop，是安全的生命周期收据（旧实现红在此，不是靠跑 UAF）。
//   4) 放行顺序：先解除 Stop 侧（防自等），再放行重挂；随后要求重挂的 Submit 在**仍存活**的
//      loop 上真实返回（收据 post_submitted + 返回码）、Stop 成功完成、ticker 停投、
//      无 fd/线程泄漏。旧实现若不被判红，其 Submit 也会落在已析构的 EventLoop 上（本 case
//      不主动制造该状态）。
int CaseRearmWindowBlocksStop() {
    WarmRuntime();
    auto probe = std::make_shared<TickProbe>();
    probe->delay_ns.store(10'000'000);
    auto handler = std::make_shared<TickHandler>(probe);
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    std::error_code stop_error;
    {
        std::error_code error;
        auto server = bbt::infra::net::ServerEngine::Create(handler, error);
        CHECK(server && !error);
        // 窗口闸门在首 tick 之前装好：首 tick 返回后的重挂会停在 Submit 入口。
        GateSet(&TickInterleaveGates::post_window_armed);
        CHECK(!server->Run(Loopback(0), TickerOptions()));
        CHECK(!server->LocalAddresses(addresses));
        CHECK(WaitUntil([&] { return probe->count.load() >= 1; }, 5s));       // 首 tick 已投递
        CHECK(GateWait(&TickInterleaveGates::post_window_entered, 5s));       // 重挂在窗口内（已取句柄、未投递）
        CHECK(probe->count.load() == 1);
        // 此刻装 Stop 侧闸门并起真实 Stop：新实现必须在撤空门（state->mutex）上被同步阻止。
        GateSet(&TickInterleaveGates::stop_submit_armed);
        std::thread stopper([&server, &stop_error] { stop_error = server->Stop(); });
        const bool stop_passed_gate = GateWait(&TickInterleaveGates::stop_submit_entered, 2s);
        if (stop_passed_gate) {
            // 红：Stop 已越过撤空门而重挂 Submit 尚未执行（撤空门不在投递临界区内）。
            // 安全收据：不放行重挂（不把 Submit 打到已/将回收的 loop 上），直接判红。
            CHECK(false);
        }
        // 放行顺序：先解除 Stop 侧（即使它稍后才到达该入口也不自等），再放行重挂投递。
        GateSet(&TickInterleaveGates::stop_submit_release);
        GateSet(&TickInterleaveGates::post_window_release);
        stopper.join();
        CHECK(g_gates.post_submitted.load());       // 放行后 Submit 在存活 loop 上真实返回
        CHECK(g_gates.post_status.load()
            == static_cast<int>(bbt::infra::net::SubmitStatus::Accepted));
        CHECK(GateGet(&TickInterleaveGates::stop_submit_entered));  // Stop 在 Submit 之后确实到达 owner 命令
        CHECK(!stop_error);
        // 门已关：Stop 后不再投递（先取收据，再有界窗口核对不增长）。
        const int settled = probe->count.load();
        CHECK(!WaitUntil([&] { return probe->count.load() != settled; }, 300ms));
        CHECK(server->CountConnections() == 0);
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
    std::cout << "rearm_window_blocks_stop: ok" << std::endl;
    return 0;
}

int RunCase(const std::string& name) {
    if (name == "periodic") { return CasePeriodic(); }
    if (name == "no_reentry_and_others_progress") { return CaseNoReentryAndOthersProgress(); }
    if (name == "stop_gate_and_release") { return CaseStopGateAndRelease(); }
    if (name == "stop_on_zero_or_negative") { return CaseStopOnZeroOrNegative(); }
    if (name == "throw_and_extreme_delay") { return CaseThrowExtremeAndUnsupported(); }
    if (name == "stop_lock_interleave") { return CaseStopLockInterleave(); }
    if (name == "submit_reject_stops_ticker") { return CaseSubmitRejectStopsTicker(); }
    if (name == "rearm_window_blocks_stop") { return CaseRearmWindowBlocksStop(); }
    std::cerr << "unknown case: " << name << std::endl;
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: Test_net_engine_tick <case>" << std::endl;
        return 2;
    }
    const std::string name = argv[1];
    return bbt_net_test::RunTestProcessExit([&name] { return RunCase(name); });
}
