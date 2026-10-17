// Action::Shutdown 向上请求上报（用户裁定：向上层传递、自由度给使用者）的公共表面验证。
//
// 契约（见 bbt/infra/net/EventHandler.hpp 的 OnShutdownRequest 注释与 Action.hpp）：
//   OnOpen/OnTraffic 真实返回 Action::Shutdown ⇒ 在**同一个受管协程任务**内上报一次
//   EventHandler::OnShutdownRequest()；不封口、不关闭、不停 ticker、不改 phase_、
//   不调 Stop、不拒绝后续业务；可重复（无「关闭首胜/不可复活」）；上报回调不返回 Action
//   （无自激）；可挂起（只让出 Processer）；抛出沿用既有 handler 异常策略。
//
// 覆盖面（每个场景一个 case，全部走公共 Engine 真实 TCP 回环 / 动态端口 / 自有进程）：
//   1. reports_and_keeps_serving      —— 请求收到、连接仍能收发、Engine 仍接纳、无连接被封口
//   2. keeps_ticker_and_accept        —— ticker 仍按 delay 投递、端口仍接纳（不是「停服」）
//   3. repeatable_no_first_win        —— 逐次上报、无一次性上限（不依赖 TCP 包边界）
//   4. suspended_yields_and_progress  —— 挂起只让出唯一 Processer：同连接不重入、其它连接进展、
//                                        不同连接请求可真并发（非 engine 全局串行）
//   5. stop_domain_and_upper_signal   —— 回调内 Stop 被域门禁拒绝且无副作用；控制线程收到
//                                        线程安全信号后显式 Stop 真实关闭（示例口径：不新建线程）
//   6. suspended_across_destroy       —— 请求挂起跨 Engine 销毁的寿命安全（适配器自持引用）
//                                        与请求抛出按既有 handler 异常策略可观察
//   7. open_request_keeps_serving     —— OnOpen 真实返回 Action::Shutdown 同样在首个事件任务内
//                                        上报一次、不封口该连接（随后 Traffic 正常）、引擎仍接纳
//
// 不使用 sleep 猜确定性窗口：所有等待都基于测试自建收据（原子标志/线程安全信号）与有界轮询。
// 客户端一律用公共 NativeSocket；不直接调用 detail 适配器（不做假集成）。

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

#include "ProcessRuntimeTestMain.hpp"

using namespace std::chrono_literals;

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

// ---- 进程级观测（真实回落证据；不靠 sleep 推断）----
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
        if (line.rfind("Threads:", 0) == 0) { return std::atoi(line.c_str() + sizeof("Threads:") - 1); }
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

// 测试私有 runtime 预热（单 Processer ⇒ 挂起必须真让出；先例见 engine_server_accept.cc）。
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
    }, "net.engine.shutdown.warm");
    CHECK(future.wait_for(10s) == std::future_status::ready);
    future.get();
}

// 「向上层传递」的控制线程通知：线程安全信号（示例口径，不新建线程、不建事件总线）。
struct RequestSignal {
    void Notify() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            ++count_;
        }
        cv_.notify_all();
    }
    bool WaitForAtLeast(std::size_t target, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [&] { return count_ >= target; });
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::size_t count_{0};
};

// 挂起闸门（OnShutdownRequest 内挂起只让出 Processer）。
struct Gate {
    std::atomic<bool> entered{false};
    std::atomic<bool> released{false};
    std::atomic<bool> done{false};
};

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
        if (::poll(&descriptor, 1, 20) <= 0) { continue; }
        char buffer[512];
        const auto room = std::min(sizeof(buffer), expected - out.size());
        const auto result = socket.Receive(buffer, room);
        if (result.bytes > 0) { out.append(buffer, result.bytes); }
        else if (result.eof) { break; }
    }
    return out;
}

bool PeerClosed(NativeSocket& socket, std::chrono::milliseconds timeout) {
    const auto deadline = Clock::now() + timeout;
    for (;;) {
        pollfd descriptor{socket.Fd(), POLLIN | POLLHUP, 0};
        if (::poll(&descriptor, 1, 20) > 0) {
            char buffer[64];
            const auto result = socket.Receive(buffer, sizeof(buffer));
            if (result.eof || result.bytes == 0 || result.error) { return true; }
        }
        if (Clock::now() >= deadline) { return false; }
    }
}

Address Loopback(std::uint16_t port) {
    std::error_code error;
    auto address = Address::Ip("127.0.0.1", port, error);
    CHECK(!error);
    return address;
}

Options TwoLoopOptions() {
    Options options;
    options.num_event_loop = 2;
    return options;
}

// ---- handler 观测面（只走公共 EventHandler 契约）----
class ShutdownHandler final : public EventHandler {
public:
    void SetTrigger(std::string payload) { trigger_ = std::move(payload); }
    void SetSignal(std::shared_ptr<RequestSignal> signal) { signal_ = std::move(signal); }
    void SetGate(std::shared_ptr<Gate> gate) { gate_ = std::move(gate); }
    void SetTickDelay(std::chrono::nanoseconds delay) { tick_delay_.store(delay.count()); }
    void SetStopCallback(std::function<std::error_code()> callback) { stop_cb_ = std::move(callback); }
    void SetInCallbackStop(bool value) { in_callback_stop_.store(value); }
    void SetThrowNext(bool value) { throw_next_.store(value); }
    // 前 count 个连接的 OnOpen 返回 Action::Shutdown（默认 0 ⇒ 与既有 6 例行为一致）。
    void SetOpenReportCount(std::size_t count) { open_report_count_.store(count); }

    bool OnBoot() override { return true; }

    Action OnOpen(ICoConnection& connection) override {
        (void)connection;
        const std::size_t index = opens_.fetch_add(1);
        return index < open_report_count_.load() ? Action::Shutdown : Action::None;
    }

    Action OnTraffic(ICoConnection& connection, bool wake) override {
        Action action = Action::None;
        if (!wake) {
            const std::string data{connection.Peek()};
            if (!data.empty()) {
                traffic_.fetch_add(1);
                if (data == trigger_) { action = Action::Shutdown; }
                // 请求不封口：载荷仍被回显（「连接仍能收发」的观测）。
                const auto write_error = connection.Write(data, Clock::now() + 5s);
                if (write_error) { write_errors_.fetch_add(1); }
                const auto discard_error = connection.Discard(data.size());
                if (discard_error) { discard_errors_.fetch_add(1); }
            }
        }
        return action;
    }

    void OnClose(ICoConnection& connection, const std::error_code& reason) override {
        (void)connection;
        closes_.fetch_add(1);
        if (reason) { close_with_error_.fetch_add(1); }
    }

    std::chrono::nanoseconds OnTick() override {
        ticks_.fetch_add(1);
        return std::chrono::nanoseconds{tick_delay_.load()};
    }

    void OnShutdownRequest() override {
        requests_.fetch_add(1);
        const int in_flight = in_flight_.fetch_add(1) + 1;
        int peak = peak_in_flight_.load();
        while (in_flight > peak && !peak_in_flight_.compare_exchange_weak(peak, in_flight)) {}
        if (signal_) { signal_->Notify(); }              // 进入即通知（挂起前的观测点）
        if (gate_) {
            gate_->entered.store(true);
            const auto deadline = Clock::now() + 20s;    // 挂起：只让出 Processer
            while (!gate_->released.load() && Clock::now() < deadline) {
                bbt::coroutine::sync::CoWaiter waiter;
                (void)waiter.Wait(bbt::coroutine::WaitOptions{Clock::now() + 5ms});
            }
            gate_->done.store(true);
        }
        if (in_callback_stop_.exchange(false) && stop_cb_) {
            stop_error_.store(stop_cb_().value());       // 受管协程内调 Stop ⇒ 域门禁拒绝
            stop_attempts_.fetch_add(1);
        }
        in_flight_.fetch_sub(1);
        if (throw_next_.exchange(false)) {
            // 按既有 handler 异常策略（bridge 捕获）：不吞成成功、不自动停止引擎。
            throw std::runtime_error("on-shutdown-request-throw");
        }
    }

    std::size_t requests() const { return requests_.load(); }
    std::size_t opens() const { return opens_.load(); }
    std::size_t closes() const { return closes_.load(); }
    std::size_t close_with_error() const { return close_with_error_.load(); }
    std::size_t ticks() const { return ticks_.load(); }
    std::size_t traffic() const { return traffic_.load(); }
    int peak_in_flight() const { return peak_in_flight_.load(); }
    int stop_attempts() const { return stop_attempts_.load(); }
    int stop_error() const { return stop_error_.load(); }
    int write_errors() const { return write_errors_.load(); }
    int discard_errors() const { return discard_errors_.load(); }

private:
    std::string trigger_{"__never__"};
    std::shared_ptr<RequestSignal> signal_;
    std::shared_ptr<Gate> gate_;
    std::atomic<std::chrono::nanoseconds::rep> tick_delay_{0};
    std::function<std::error_code()> stop_cb_;
    std::atomic<bool> in_callback_stop_{false};
    std::atomic<bool> throw_next_{false};
    std::atomic<std::size_t> open_report_count_{0};
    std::atomic<std::size_t> requests_{0};
    std::atomic<std::size_t> opens_{0};
    std::atomic<std::size_t> closes_{0};
    std::atomic<std::size_t> close_with_error_{0};
    std::atomic<std::size_t> ticks_{0};
    std::atomic<std::size_t> traffic_{0};
    std::atomic<int> in_flight_{0};
    std::atomic<int> peak_in_flight_{0};
    std::atomic<int> stop_attempts_{0};
    std::atomic<int> stop_error_{0};
    std::atomic<int> write_errors_{0};
    std::atomic<int> discard_errors_{0};
};

// ---- case 1：请求收到 + 连接仍能收发 + Engine 仍接纳 + 无连接被封口 ----
int CaseReportsAndKeepsServing() {
    WarmRuntime();
    auto signal = std::make_shared<RequestSignal>();
    auto handler = std::make_shared<ShutdownHandler>();
    handler->SetTrigger("shutdown-now");
    handler->SetSignal(signal);
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), TwoLoopOptions()));
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1);
        const auto bound = addresses[0];

        auto client = ConnectTo(bound);
        const std::string trigger = "shutdown-now";
        SendAll(client, trigger);
        CHECK(signal->WaitForAtLeast(1, 10s));           // 请求回调已被调用
        CHECK(handler->requests() == 1);
        // 未封口：同一连接仍能收发（载荷先被回显，请求只上报）。
        CHECK(ReceiveAtMost(client, trigger.size(), 8s) == trigger);
        const std::string after = "after-request";
        SendAll(client, after);
        CHECK(ReceiveAtMost(client, after.size(), 8s) == after);
        CHECK(handler->requests() == 1);                 // 非请求载荷不产生上报
        CHECK(handler->closes() == 0);                   // 请求路径没有任何封口
        // Engine 状态不变：仍 Running、快照仍可读、活跃连接未被请求归零。
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1 && addresses[0].Port() == bound.Port());
        CHECK(WaitUntil([&] { return engine.CountConnections() == 1; }, 8s));
        // 端口仍接纳新连接并回显。
        auto second = ConnectTo(bound);
        SendAll(second, "second");
        CHECK(ReceiveAtMost(second, 6, 8s) == "second");
        CHECK(WaitUntil([&] { return engine.CountConnections() == 2; }, 8s));
        CHECK(handler->requests() == 1 && handler->closes() == 0);
        CHECK(handler->write_errors() == 0 && handler->discard_errors() == 0);
        CHECK(!engine.Stop());
        CHECK(engine.CountConnections() == 0);
        client.Close();
        second.Close();
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);
    std::cout << "reports_and_keeps_serving: ok" << std::endl;
    return 0;
}

// ---- case 2：请求不停 ticker、不停止接纳 ----
int CaseKeepsTickerAndAccept() {
    WarmRuntime();
    auto signal = std::make_shared<RequestSignal>();
    auto handler = std::make_shared<ShutdownHandler>();
    handler->SetTrigger("shutdown-now");
    handler->SetSignal(signal);
    handler->SetTickDelay(20ms);
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        Options options = TwoLoopOptions();
        options.ticker = true;
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), options));
        CHECK(!engine.LocalAddresses(addresses));
        const auto bound = addresses[0];
        CHECK(WaitUntil([&] { return handler->ticks() >= 1; }, 8s));   // ticker 已投递

        auto client = ConnectTo(bound);
        const std::string trigger = "shutdown-now";
        SendAll(client, trigger);
        CHECK(signal->WaitForAtLeast(1, 10s));
        CHECK(ReceiveAtMost(client, trigger.size(), 8s) == trigger);
        // 请求之后 ticker 仍按返回 delay 继续投递（不是「请求=停服」）。
        const std::size_t ticks_after_request = handler->ticks();
        CHECK(WaitUntil([&] { return handler->ticks() >= ticks_after_request + 3; }, 8s));
        // 端口仍接纳新连接并回显。
        auto second = ConnectTo(bound);
        SendAll(second, "ticker-alive");
        CHECK(ReceiveAtMost(second, 12, 8s) == "ticker-alive");
        CHECK(WaitUntil([&] { return engine.CountConnections() == 2; }, 8s));
        CHECK(!engine.Stop());
        CHECK(engine.CountConnections() == 0);
        client.Close();
        second.Close();
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);
    std::cout << "keeps_ticker_and_accept: ok" << std::endl;
    return 0;
}

// ---- case 3：可重复上报、无「关闭首胜/不可复活」（不依赖 TCP 包边界）----
int CaseRepeatableNoFirstWin() {
    WarmRuntime();
    auto signal = std::make_shared<RequestSignal>();
    auto handler = std::make_shared<ShutdownHandler>();
    handler->SetTrigger("shutdown-now");
    handler->SetSignal(signal);
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), TwoLoopOptions()));
        CHECK(!engine.LocalAddresses(addresses));
        const auto bound = addresses[0];
        auto client = ConnectTo(bound);
        const std::string trigger = "shutdown-now";

        // 每轮只等「出现一次新的上报」（严格增长），不假设一次 send 恰等于一次读事件。
        std::string echoed;
        for (int round = 1; round <= 3; ++round) {
            const std::size_t before = handler->requests();
            SendAll(client, trigger);
            CHECK(WaitUntil([&] { return handler->requests() > before; }, 10s));
            CHECK(handler->requests() >= static_cast<std::size_t>(round));
            // 回显按**累计字节数**读取：即使一次 send 被拆成多次读，总回显字节数不变。
            const std::size_t want = static_cast<std::size_t>(round) * trigger.size();
            echoed += ReceiveAtMost(client, want - echoed.size(), 8s);
        }
        CHECK(echoed == trigger + trigger + trigger);    // 反复请求后连接仍能正常收发
        CHECK(handler->requests() >= 3);                 // 没有被一次性语义吞掉
        // 反复请求后连接仍活、引擎仍接纳。
        CHECK(handler->closes() == 0);
        CHECK(WaitUntil([&] { return engine.CountConnections() == 1; }, 8s));
        CHECK(!engine.LocalAddresses(addresses));
        const std::size_t reports = handler->requests();
        const std::string after = "still-serving";
        SendAll(client, after);
        CHECK(ReceiveAtMost(client, after.size(), 8s) == after);
        CHECK(handler->requests() == reports);           // 「去重」是使用者策略，引擎不代做也不吞
        CHECK(!engine.Stop());
        client.Close();
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);
    std::cout << "repeatable_no_first_win: ok" << std::endl;
    return 0;
}

// ---- case 4：挂起只让出（唯一 Processer 真让出）----
// 同连接不重入 + 其它连接进展 + 不同连接的请求可真并发（本回调不是 engine 全局串行点）。
int CaseSuspendedYieldsAndProgress() {
    WarmRuntime();
    auto gate = std::make_shared<Gate>();
    auto signal = std::make_shared<RequestSignal>();
    auto handler = std::make_shared<ShutdownHandler>();
    handler->SetTrigger("shutdown-now");
    handler->SetSignal(signal);
    handler->SetGate(gate);
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), TwoLoopOptions()));
        CHECK(!engine.LocalAddresses(addresses));
        const auto bound = addresses[0];
        auto a = ConnectTo(bound);
        auto b = ConnectTo(bound);
        CHECK(WaitUntil([&] { return engine.CountConnections() == 2; }, 8s));
        const std::string trigger = "shutdown-now";

        SendAll(a, trigger);
        CHECK(signal->WaitForAtLeast(1, 10s));
        CHECK(gate->entered.load());                     // A 的请求正在挂起中
        SendAll(a, trigger);                             // 同一连接的新载荷：事件位串行 ⇒ 不重入
        CHECK(handler->requests() == 1);
        SendAll(b, "from-b");                            // 其它连接在挂起期间继续进展
        CHECK(ReceiveAtMost(b, 6, 8s) == "from-b");
        SendAll(b, trigger);                             // 不同连接的请求可真并发
        CHECK(signal->WaitForAtLeast(2, 10s));
        CHECK(WaitUntil([&] { return handler->peak_in_flight() >= 2; }, 8s));
        CHECK(handler->requests() == 2);                 // A 的连接仍无重入

        gate->released.store(true);
        CHECK(WaitUntil([&] { return gate->done.load(); }, 10s));
        // 放行后同连接的排队事件自然继续，最终也上报（第 3 次）。
        CHECK(WaitUntil([&] { return handler->requests() >= 3; }, 10s));
        CHECK(WaitUntil([&] { return engine.CountConnections() == 2; }, 8s));
        CHECK(handler->closes() == 0);
        CHECK(!engine.Stop());
        a.Close();
        b.Close();
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);
    std::cout << "suspended_yields_and_progress: ok" << std::endl;
    return 0;
}

// ---- case 5：回调内 Stop 域拒绝无副作用；控制线程收信号后显式 Stop 真实关闭 ----
int CaseStopDomainAndUpperSignal() {
    WarmRuntime();
    auto signal = std::make_shared<RequestSignal>();
    auto handler = std::make_shared<ShutdownHandler>();
    std::atomic<Engine*> control{nullptr};
    handler->SetTrigger("shutdown-now");
    handler->SetSignal(signal);
    handler->SetInCallbackStop(true);
    handler->SetStopCallback([&control] { return control.load()->Stop(); });
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        Engine engine(handler);
        control.store(&engine);
        CHECK(!engine.Run(Loopback(0), TwoLoopOptions()));
        CHECK(!engine.LocalAddresses(addresses));
        const auto bound = addresses[0];
        auto client = ConnectTo(bound);
        const std::string trigger = "shutdown-now";
        SendAll(client, trigger);
        CHECK(signal->WaitForAtLeast(1, 10s));
        CHECK(WaitUntil([&] { return handler->stop_attempts() >= 1; }, 10s));
        // 回调内 Stop：受管协程被域门禁拒绝，且在任何副作用之前 ⇒ 引擎状态不变。
        CHECK(IsError(std::error_code(handler->stop_error(), std::generic_category()),
            std::errc::operation_not_permitted));
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1 && addresses[0].Port() == bound.Port());
        CHECK(WaitUntil([&] { return engine.CountConnections() == 1; }, 8s));
        CHECK(ReceiveAtMost(client, trigger.size(), 8s) == trigger);   // 请求不封口：载荷仍被回显
        const std::string after = "still-serving";
        SendAll(client, after);
        CHECK(ReceiveAtMost(client, after.size(), 8s) == after);

        // 控制线程（当初 Run 的线程；非受管协程、非 owner 线程）显式 Stop 才真实关闭。
        CHECK(!engine.Stop());
        CHECK(PeerClosed(client, 8s));
        CHECK(WaitUntil([&] { return engine.CountConnections() == 0; }, 10s));
        addresses.clear();
        CHECK(IsError(engine.LocalAddresses(addresses), std::errc::operation_not_permitted));
        CHECK(IsError(engine.Run(Loopback(0), TwoLoopOptions()), std::errc::operation_canceled));
        CHECK(!engine.Stop());                           // 幂等
        client.Close();
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);
    std::cout << "stop_domain_and_upper_signal: ok" << std::endl;
    return 0;
}

// ---- case 6：请求挂起跨 Engine 销毁的寿命安全 + 请求抛出按既有异常策略可观察 ----
int CaseSuspendedAcrossDestroy() {
    WarmRuntime();
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();

    // 6a 挂起跨 Engine 销毁：适配器按值持有 handler ⇒ 挂起回调寿命安全。
    {
        auto gate = std::make_shared<Gate>();
        auto signal = std::make_shared<RequestSignal>();
        auto handler = std::make_shared<ShutdownHandler>();
        handler->SetTrigger("shutdown-now");
        handler->SetSignal(signal);
        handler->SetGate(gate);
        std::weak_ptr<ShutdownHandler> weak = handler;
        {
            auto engine = std::make_unique<Engine>(handler);
            std::vector<Address> addresses;
            CHECK(!engine->Run(Loopback(0), TwoLoopOptions()));
            CHECK(!engine->LocalAddresses(addresses));
            auto client = ConnectTo(addresses[0]);
            SendAll(client, "shutdown-now");
            CHECK(signal->WaitForAtLeast(1, 10s));
            CHECK(WaitUntil([&] { return gate->entered.load(); }, 10s));
            handler.reset();                             // 测试释放自持引用
            engine.reset();                              // 请求挂起中销毁 Engine（强停收口）
            CHECK(!weak.expired());                      // 适配器/在途事件仍持有 ⇒ 不悬垂
            client.Close();
        }
        CHECK(!weak.expired());
        gate->released.store(true);                      // 放行挂起中的请求回调
        CHECK(WaitUntil([&] { return gate->done.load(); }, 15s));
        CHECK(WaitUntil([&] { return weak.expired(); }, 15s));   // 最后一个引用释放后回收
    }

    // 6b 请求抛出：沿用既有 handler 异常策略（该连接封口 + 物理关闭），引擎不被自动停止。
    {
        auto handler = std::make_shared<ShutdownHandler>();
        handler->SetTrigger("shutdown-now");
        handler->SetThrowNext(true);
        std::vector<Address> addresses;
        {
            Engine engine(handler);
            CHECK(!engine.Run(Loopback(0), TwoLoopOptions()));
            CHECK(!engine.LocalAddresses(addresses));
            const auto bound = addresses[0];
            auto client = ConnectTo(bound);
            SendAll(client, "shutdown-now");
            CHECK(WaitUntil([&] { return handler->requests() >= 1; }, 10s));
            CHECK(WaitUntil([&] { return handler->closes() >= 1; }, 10s));   // 封口+物理关闭收尾
            CHECK(PeerClosed(client, 8s));
            CHECK(handler->close_with_error() >= 1);                         // 收口原因非空
            // 引擎未被异常自动停止：仍 Running、端口仍接纳新连接并回显。
            std::vector<Address> still;
            CHECK(!engine.LocalAddresses(still));
            CHECK(still.size() == 1 && still[0].Port() == bound.Port());
            auto fresh = ConnectTo(bound);
            SendAll(fresh, "after-throw");
            CHECK(ReceiveAtMost(fresh, 11, 8s) == "after-throw");
            CHECK(!engine.Stop());
            fresh.Close();
            client.Close();
        }
    }

    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 15s));
    CHECK(FdCount() == fds_before);
    std::cout << "suspended_across_destroy: ok" << std::endl;
    return 0;
}

// ---- case 7：OnOpen 真实返回 Action::Shutdown 的上报路径（契约面另一格）----
// 请求由连接建立后的首个事件（OnOpen）返回产生：不发送任何载荷即等到一次上报；
// 该请求不封口该连接（随后 Traffic 仍正常收发），引擎仍接纳后续连接，且第 2 个连接
// （OnOpen 返回 None）不再上报 ⇒ 上报与「某次真实返回」一一对应，而非按连接数计。
int CaseOpenRequestKeepsServing() {
    WarmRuntime();
    auto signal = std::make_shared<RequestSignal>();
    auto handler = std::make_shared<ShutdownHandler>();
    handler->SetSignal(signal);
    handler->SetOpenReportCount(1);                  // 仅第 1 个连接的 OnOpen 返回 Action::Shutdown
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), TwoLoopOptions()));
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1);
        const auto bound = addresses[0];

        auto client = ConnectTo(bound);
        // OnOpen 的返回即在首个事件任务内上报：不发载荷即可等到，且恰一次。
        CHECK(signal->WaitForAtLeast(1, 10s));
        CHECK(handler->opens() == 1);
        CHECK(handler->requests() == 1);
        CHECK(handler->traffic() == 0);                   // 上报来自 OnOpen 的真实返回，而非 Traffic 事件
        CHECK(handler->closes() == 0);                   // OnOpen 的请求不封口该连接
        CHECK(WaitUntil([&] { return engine.CountConnections() == 1; }, 8s));
        // 请求本身不改变引擎状态：快照仍可读、端口仍接纳。
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1 && addresses[0].Port() == bound.Port());
        // 同一连接仍能收发（OnOpen 请求之后 Traffic 正常，无封口）。
        const std::string payload = "after-open-request";
        SendAll(client, payload);
        CHECK(ReceiveAtMost(client, payload.size(), 8s) == payload);
        CHECK(handler->traffic() >= 1);
        CHECK(handler->requests() == 1);                  // 普通流量不产生额外上报
        CHECK(handler->closes() == 0);
        CHECK(handler->write_errors() == 0 && handler->discard_errors() == 0);
        // 端口仍接纳：第 2 个连接的 OnOpen 返回 None ⇒ 上报次数不变。
        auto second = ConnectTo(bound);
        SendAll(second, "second");
        CHECK(ReceiveAtMost(second, 6, 8s) == "second");
        CHECK(WaitUntil([&] { return engine.CountConnections() == 2; }, 8s));
        CHECK(handler->opens() == 2);
        CHECK(handler->requests() == 1);
        CHECK(handler->closes() == 0);
        CHECK(!engine.Stop());
        CHECK(engine.CountConnections() == 0);
        client.Close();
        second.Close();
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);
    std::cout << "open_request_keeps_serving: ok" << std::endl;
    return 0;
}

int RunCase(const std::string& name) {
    if (name == "reports_and_keeps_serving") { return CaseReportsAndKeepsServing(); }
    if (name == "keeps_ticker_and_accept") { return CaseKeepsTickerAndAccept(); }
    if (name == "repeatable_no_first_win") { return CaseRepeatableNoFirstWin(); }
    if (name == "suspended_yields_and_progress") { return CaseSuspendedYieldsAndProgress(); }
    if (name == "stop_domain_and_upper_signal") { return CaseStopDomainAndUpperSignal(); }
    if (name == "suspended_across_destroy") { return CaseSuspendedAcrossDestroy(); }
    if (name == "open_request_keeps_serving") { return CaseOpenRequestKeepsServing(); }
    std::cerr << "unknown case: " << name << std::endl;
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: Test_net_engine_shutdown_request <case>" << std::endl;
        return 2;
    }
    const std::string name = argv[1];
    return bbt_net_test::RunTestProcessExit([&name] { return RunCase(name); });
}
