// 首包公共 Engine（单 main + N sub）真实 TCP 闭环的**公共表面**验证。
//
// 覆盖面（详见 D/engine-implementation-41/plan.md）：
//   - 真实动态端口 listener → main accept → LoadBalancer → sub 唯一 socket 移交 →
//     既有 bridge → 受管 OnBoot/OnOpen/OnTraffic/OnClose；Action::{None,Close}。
//   - native 控制线程 Run/Stop 生命周期；Stop 强停不等挂起 handler；owner/context
//     真销毁后残留 shell/快照 view 仍安全（晚写/晚 Wake 明确拒绝、Close 幂等）。
//   - 生命周期负例：未接线选项、bind 失败回落、重复 Run、Stop 后 Run、
//     受管协程内 Run/Stop 在副作用前 operation_not_permitted。
//
// 本文件消费公共契约（Engine/EventHandler/Action/Address/NativeSocket/
// CoTcpConnection）与测试私有进程退出边界，客户端一律用公共 NativeSocket。
// 唯一例外：case onclose_suspended_stop_destroy 的**白盒补充段**直接构造私有
// EventHandlerAdapter（仅该 case，明确注释），用于共享计数跨挂起寿命/
// 最终值证据；公共 Engine 真实 TCP case 仍是主证据。

#include <dirent.h>
#include <poll.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
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
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/EventHandler.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/Options.hpp>
#include <bbt/infra/net/detail/EventHandlerAdapter.hpp>
// 测试私有：SourceAddrHash 的接线用例复用产品同一 CRC32-IEEE 入口（函数本身由
// net.load_balancing 的固定向量负责，本文件只验真实 accept 路径的接线）。
#include <bbt/infra/net/detail/SourceAddrHash.hpp>

// 仅测试白盒读取 sub bridge；先加载依赖，避免修改标准库/Boost 的访问控制。
#include <boost/asio/posix/stream_descriptor.hpp>
#include <bbt/infra/net/detail/ConnectionBridge.hpp>
#define private public
#include <bbt/infra/net/detail/ServerEngine.hpp>
#undef private

#include "ProcessRuntimeTestMain.hpp"

using namespace std::chrono_literals;

namespace {

using Clock = std::chrono::steady_clock;
using bbt::infra::net::Action;
using bbt::infra::net::Address;
using bbt::infra::net::AddressFamily;
using bbt::infra::net::CloseMode;
using bbt::infra::net::CoTcpConnection;
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

bool IsError(const std::error_code& error, std::errc expected) {
    return error == expected;
}

// 绕过协程 Hook 的原生睡眠（先例：runtime #284 RawSleepMs）。本进程 nanosleep 已被
// Hook 拦截：协程内 std::this_thread::sleep_for 会挂起协程并让出 Processer（Hook.cc
// Hook_Nanosleep → YieldUntilTimeout），因此「真实非协作占用执行线程」必须直达内核。
void RawNanosleep(int ms) noexcept {
    struct timespec req{ms / 1000, (ms % 1000) * 1000000L};
    while (::syscall(SYS_nanosleep, &req, &req) != 0 && errno == EINTR) {
    }
}

// ---- 测试私有 runtime 预热（单 Processer，真实挂起让出）----
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
    }, "net.engine.test.warm");
    CHECK(future.wait_for(10s) == std::future_status::ready);
    future.get();
}

// 在受管协程内执行并取回结果（域拒绝用例）。
std::error_code RunInCoroutine(const std::function<std::error_code()>& callback) {
    auto holder = std::make_shared<std::error_code>();
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    const std::size_t stack = g_bbt_coroutine_config->m_cfg_stack_size;
    g_scheduler->RegistCoroutineTask(stack, [holder, promise, callback] {
        *holder = callback();
        promise->set_value();
    }, "net.engine.test.task");
    CHECK(future.wait_for(10s) == std::future_status::ready);
    future.get();
    return *holder;
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

// 读侧被对端关闭（EOF/RST）：Action::Close 真实生效的客户端侧观察。
bool PeerClosed(NativeSocket& socket, std::chrono::milliseconds timeout) {
    const auto deadline = Clock::now() + timeout;
    for (;;) {
        pollfd descriptor{socket.Fd(), POLLIN | POLLHUP, 0};
        const int ready = ::poll(&descriptor, 1, 20);
        if (ready > 0) {
            char buffer[64];
            const auto result = socket.Receive(buffer, sizeof(buffer));
            if (result.eof || result.bytes == 0 || result.error) { return true; }
        }
        if (Clock::now() >= deadline) { return false; }
    }
}

// ---- handler 观测面 ----
struct ConnRecord {
    std::size_t id{0};
    std::size_t peer_port{0};
    int open{0};
    int traffic{0};
    int close{0};
    int active{0};
    int peak{0};
    bool open_before_traffic{false};
    bool traffic_before_close{false};
    std::string echoed;
};

// OnClose 内挂起闸门：Engine 被强停/销毁后才放行（F1 复现挂起跨销毁）。
struct CloseGate {
    std::atomic<bool> entered{false};
    std::atomic<bool> released{false};
    std::atomic<bool> done{false};
};

struct StopProbe {
    std::atomic<bool> entered{false};
    std::atomic<bool> released{false};
    std::atomic<bool> done{false};
    std::atomic<bool> view_stable{false};
    std::atomic<std::size_t> view_size{0};
    std::atomic<std::size_t> buffered{0};
    std::atomic<int> write_error{0};
    std::atomic<int> wake_error{0};
    std::atomic<int> close_first{-1};
    std::atomic<int> close_second{-1};
};

class TestHandler final : public EventHandler {
public:
    // 命中该载荷的首次 OnTraffic 会在事件协程内挂起并执行 StopProbe。
    void HoldOn(std::string payload, std::shared_ptr<StopProbe> probe, bool probe_ops) {
        hold_payload_ = std::move(payload);
        probe_ = std::move(probe);
        probe_ops_ = probe_ops;
    }

    bool OnBoot() override {
        boot_calls_.fetch_add(1);
        if (boot_gate_) {
            // OnBoot 内挂起：到测试放行才返回（按时钟超出 Engine 内部 10s deadline）。
            boot_gate_->entered.store(true);
            const auto deadline = Clock::now() + 30s;
            while (!boot_gate_->released.load() && Clock::now() < deadline) {
                bbt::coroutine::sync::CoWaiter waiter;
                (void)waiter.Wait(bbt::coroutine::WaitOptions{Clock::now() + 10ms});
            }
            boot_gate_->done.store(true);
        }
        return boot_result_.load();
    }

    Action OnOpen(ICoConnection& connection) override {
        Enter(connection);
        {
            std::lock_guard<std::mutex> guard(mu_);
            auto& record = records_[&connection];
            ++record.open;
            record.id = ConnectionId(connection);
            captured_.store(&connection);   // 引擎存活期真实连接引用（白盒补充用）
        }
        Leave(connection);
        return Action::None;
    }

    Action OnTraffic(ICoConnection& connection, bool wake) override {
        Enter(connection);
        Action action = Action::None;
        if (!wake) {
            const std::string data{connection.Peek()};
            if (!data.empty()) {
                {
                    std::lock_guard<std::mutex> guard(mu_);
                    auto& record = records_[&connection];
                    ++record.traffic;
                    record.open_before_traffic = record.open >= 1;
                    if (data != close_payload_) { record.echoed += data; }
                    record.peer_port = PeerPort(connection);
                }
                if (!hold_payload_.empty() && data == hold_payload_ && probe_) {
                    Hold(connection, data);
                }
                const auto write_error = connection.Write(data, Clock::now() + 5s);
                if (write_error) { write_errors_.fetch_add(1); }
                const auto discard_error = connection.Discard(data.size());
                if (discard_error) { discard_errors_.fetch_add(1); }
                if (data == close_payload_) { action = Action::Close; }
            }
        }
        Leave(connection);
        return action;
    }

    void OnClose(ICoConnection& connection, const std::error_code& reason) override {
        Enter(connection);
        {
            std::lock_guard<std::mutex> guard(mu_);
            auto& record = records_[&connection];
            ++record.close;
            record.traffic_before_close = record.traffic >= 1;
        }
        if (reason) { close_with_error_.fetch_add(1); }
        if (close_gate_) {
            // OnClose 内挂起：直到测试放行（期间 Engine 可能已强停/销毁）。
            close_gate_->entered.store(true);
            const auto deadline = Clock::now() + 20s;
            while (!close_gate_->released.load() && Clock::now() < deadline) {
                bbt::coroutine::sync::CoWaiter waiter;
                (void)waiter.Wait(bbt::coroutine::WaitOptions{Clock::now() + 10ms});
            }
            close_gate_->done.store(true);   // 用户回调完成；适配器 RAII 递减紧随其后
        }
        Leave(connection);
        // 用户 OnClose 抛出（EventHandler 契约允许，bridge 捕获并计数）：F2 复现异常漏减。
        if (throw_in_close_.load()) { throw std::runtime_error("onclose-throw"); }
    }

    void SetClosePayload(std::string payload) { close_payload_ = std::move(payload); }
    void SetBootResult(bool value) { boot_result_.store(value); }
    void SetThrowInClose(bool value) { throw_in_close_.store(value); }

    std::vector<ConnRecord> Records() const {
        std::lock_guard<std::mutex> guard(mu_);
        std::vector<ConnRecord> output;
        output.reserve(records_.size());
        for (const auto& entry : records_) { output.push_back(entry.second); }
        return output;
    }

    int open_count() const { std::lock_guard<std::mutex> guard(mu_); int n = 0; for (const auto& e : records_) { n += e.second.open; } return n; }
    int close_count() const { std::lock_guard<std::mutex> guard(mu_); int n = 0; for (const auto& e : records_) { n += e.second.close; } return n; }
    int boot_calls() const { return boot_calls_.load(); }
    ICoConnection* captured_connection() const { return captured_.load(); }
    void HoldOnClose(std::shared_ptr<CloseGate> gate) { close_gate_ = std::move(gate); }
    void HoldOnBoot(std::shared_ptr<CloseGate> gate) { boot_gate_ = std::move(gate); }
    int global_peak() const { return global_peak_.load(); }
    int write_errors() const { return write_errors_.load(); }
    int discard_errors() const { return discard_errors_.load(); }

private:
    static std::size_t ConnectionId(ICoConnection& connection) {
        const auto* tcp = dynamic_cast<const CoTcpConnection*>(&connection);
        CHECK(tcp != nullptr);
        return tcp->Id();
    }

    static std::size_t PeerPort(ICoConnection& connection) {
        const auto* tcp = dynamic_cast<const CoTcpConnection*>(&connection);
        CHECK(tcp != nullptr);
        const auto peer = tcp->PeerAddress();
        return peer.Port();
    }

    void Enter(ICoConnection& connection) {
        const int now = global_active_.fetch_add(1) + 1;
        const int peak = global_peak_.load();
        if (now > peak) { global_peak_.store(now); }
        std::lock_guard<std::mutex> guard(mu_);
        auto& record = records_[&connection];
        ++record.active;
        if (record.active > record.peak) { record.peak = record.active; }
    }

    void Leave(ICoConnection& connection) {
        {
            std::lock_guard<std::mutex> guard(mu_);
            auto& record = records_[&connection];
            --record.active;
        }
        (void)global_active_.fetch_sub(1);
    }

    void Hold(ICoConnection& connection, const std::string& data) {
        const std::string_view view = connection.Peek();
        probe_->view_size.store(view.size());
        probe_->entered.store(true);
        const auto deadline = Clock::now() + 20s;
        while (!probe_->released.load() && Clock::now() < deadline) {
            bbt::coroutine::sync::CoWaiter waiter;
            (void)waiter.Wait(bbt::coroutine::WaitOptions{Clock::now() + 10ms});
        }
        if (!probe_->released.load()) { probe_->done.store(true); return; }
        if (!probe_ops_) { probe_->done.store(true); return; }   // 只验证挂起让出
        // 此刻 engine 已被强停、owner/context 已销毁：view/shell 必须仍安全。
        probe_->view_stable.store(std::string(view) == data);
        probe_->buffered.store(connection.InboundBuffered());
        const auto op_deadline = Clock::now() + 3s;
        probe_->write_error.store(connection.Write("late-write", op_deadline).value());
        probe_->wake_error.store(connection.Wake().value());
        probe_->close_first.store(connection.Close(CloseMode::Unblock, {}, op_deadline).value());
        probe_->close_second.store(connection.Close(CloseMode::Unblock, {}, op_deadline).value());
        probe_->done.store(true);
    }

    mutable std::mutex mu_;
    std::map<ICoConnection*, ConnRecord> records_;
    std::atomic<int> global_active_{0};
    std::atomic<int> global_peak_{0};
    std::atomic<int> boot_calls_{0};
    std::atomic<bool> boot_result_{true};
    std::atomic<int> write_errors_{0};
    std::atomic<int> discard_errors_{0};
    std::atomic<int> close_with_error_{0};
    std::string hold_payload_;
    std::string close_payload_{"__never__"};
    std::shared_ptr<StopProbe> probe_;
    bool probe_ops_{false};
    std::atomic<bool> throw_in_close_{false};
    std::shared_ptr<CloseGate> close_gate_;
    std::shared_ptr<CloseGate> boot_gate_;
    std::atomic<ICoConnection*> captured_{nullptr};
};

// 白盒补充用：EventHandler 已是全默认空实现的非抽象类，空派生即可忽略连接/事件。
class NullHandler final : public EventHandler {};

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

// 家族拒绝用例自有 Unix 路径（相对 ctest WORKING_DIRECTORY = <build>/tests/net/runtime，
// 不写源码树）：仅这两个用例使用，跑前 unlink 残留、跑后断言从未被创建。
const char* const kUnsupportedFamilyPath = "unsupported-family-150.sock";

bool PathExists(const char* path) {
    struct stat info{};
    return ::stat(path, &info) == 0;
}

// ---- case 1：真实 roundtrip / 顺序 / 双 sub 身份 / CountConnections 回落 ----
int CaseRoundtrip() {
    WarmRuntime();
    auto handler = std::make_shared<TestHandler>();
    handler->SetClosePayload("close");
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        Engine engine(handler);
        // Run 前：LocalAddresses 明确拒绝且不改写 out。
        addresses.push_back(Loopback(1));
        CHECK(IsError(engine.LocalAddresses(addresses), std::errc::operation_not_permitted));
        CHECK(addresses.size() == 1 && addresses[0].Port() == 1);
        addresses.clear();
        CHECK(handler->boot_calls() == 0);
        CHECK(!engine.Run(Loopback(0), TwoLoopOptions()));
        CHECK(ThreadCount() > threads_before);          // 真实起了 owner 线程
        CHECK(handler->boot_calls() == 1);              // OnBoot 受管执行且已完成
        CHECK(engine.CountConnections() == 0);
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1);
        const auto bound = addresses[0];
        CHECK(bound.Family() == AddressFamily::IPv4);
        CHECK(bound.Port() != 0);                       // 实际绑定端口，不是请求里的 0
        CHECK(bound.ToString().find("127.0.0.1") != std::string::npos);
        std::vector<Address> second;
        CHECK(!engine.LocalAddresses(second));
        CHECK(second.size() == 1 && second[0].Port() == bound.Port());  // 稳定快照

        std::vector<std::string> payloads;
        std::vector<NativeSocket> clients;
        for (int index = 0; index < 3; ++index) {
            auto client = ConnectTo(bound);
            const std::string payload = "engine-roundtrip-" + std::to_string(index);
            SendAll(client, payload);
            CHECK(ReceiveAtMost(client, payload.size(), 5s) == payload);
            payloads.push_back(payload);
            clients.push_back(std::move(client));
        }
        CHECK(WaitUntil([&] { return handler->open_count() == 3; }, 8s));
        CHECK(handler->write_errors() == 0);
        CHECK(handler->discard_errors() == 0);
        // 三连接都经 handler 的 Action::Close 真实封口 + 物理关闭（客户端侧观察到对端关闭），
        // 收尾后每连接 OnClose 恰一次、活跃连接数回落。
        for (auto& client : clients) {
            SendAll(client, "close");
            CHECK(PeerClosed(client, 8s));
        }
        CHECK(WaitUntil([&] { return handler->close_count() == 3; }, 10s));
        CHECK(WaitUntil([&] { return engine.CountConnections() == 0; }, 8s));

        const auto records = handler->Records();
        CHECK(records.size() == 3);
        std::vector<std::size_t> ids;
        std::vector<std::string> echoed;
        std::vector<std::size_t> peers;
        for (const auto& record : records) {
            CHECK(record.open == 1 && record.traffic >= 1 && record.close == 1);
            CHECK(record.open_before_traffic);
            CHECK(record.traffic_before_close);
            CHECK(record.peak == 1);
            ids.push_back(record.id);
            echoed.push_back(record.echoed);
            peers.push_back(record.peer_port);
        }
        std::sort(ids.begin(), ids.end());
        std::sort(echoed.begin(), echoed.end());
        std::sort(payloads.begin(), payloads.end());
        std::sort(peers.begin(), peers.end());
        // 两个独立 sub bridge 各自从 0 编号 + RoundRobin：id 多重集必须 == {0,0,1}。
        CHECK((ids == std::vector<std::size_t>{0, 0, 1}));
        CHECK(echoed == payloads);                                       // 每连接回显内容正确
        CHECK(std::unique(peers.begin(), peers.end()) == peers.end());   // 三个真实独立对端
        CHECK(!engine.Stop());
        CHECK(engine.CountConnections() == 0);
        CHECK(!engine.Stop());                                           // 幂等
        addresses.clear();
        CHECK(IsError(engine.LocalAddresses(addresses), std::errc::operation_not_permitted));
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);
    std::cout << "roundtrip: ok" << std::endl;
    return 0;
}

// ---- case 2：A 业务挂起时 B/accept 继续；单连接峰值 1、全局真并发 ----
int CaseConcurrentHold() {
    WarmRuntime();
    auto probe = std::make_shared<StopProbe>();
    auto handler = std::make_shared<TestHandler>();
    handler->HoldOn("hold-payload", probe, false);
    handler->SetClosePayload("close");
    std::vector<Address> addresses;
    {
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), TwoLoopOptions()));
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1);
        const auto bound = addresses[0];
        auto held = ConnectTo(bound);
        SendAll(held, "hold-payload");
        CHECK(WaitUntil([&] { return probe->entered.load(); }, 8s));
        CHECK(!probe->released.load());
        // A 挂起期间：accept 继续，B 完成真实往返。
        auto other = ConnectTo(bound);
        SendAll(other, "concurrent-b");
        CHECK(ReceiveAtMost(other, std::string("concurrent-b").size(), 8s) == "concurrent-b");
        CHECK(handler->global_peak() >= 2);       // A 的事件协程挂起中，B 事件真实并行
        probe->released.store(true);
        CHECK(ReceiveAtMost(held, std::string("hold-payload").size(), 10s) == "hold-payload");
        CHECK(WaitUntil([&] { return probe->done.load(); }, 8s));
        SendAll(held, "close");
        SendAll(other, "close");
        CHECK(PeerClosed(held, 8s) && PeerClosed(other, 8s));
        CHECK(WaitUntil([&] { return handler->close_count() == 2; }, 10s));
        CHECK(WaitUntil([&] { return engine.CountConnections() == 0; }, 8s));
        for (const auto& record : handler->Records()) { CHECK(record.peak == 1); }
        CHECK(!engine.Stop());
    }
    std::cout << "concurrent_hold: ok" << std::endl;
    return 0;
}

// ---- case 3：handler 挂起持 snapshot 时 native 强停有界 + 残留 shell 安全 ----
int CaseStopWithSuspendedHandler() {
    WarmRuntime();
    auto probe = std::make_shared<StopProbe>();
    auto handler = std::make_shared<TestHandler>();
    handler->HoldOn("stop-payload", probe, true);
    handler->SetClosePayload("close");
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    NativeSocket client;
    {
        Engine engine(handler);
        Options options;
        CHECK(!engine.Run(Loopback(0), options));
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1);
        client = ConnectTo(addresses[0]);
        SendAll(client, "stop-payload");
        CHECK(WaitUntil([&] { return probe->entered.load(); }, 8s));
        const auto start = Clock::now();
        CHECK(!engine.Stop());
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);
        CHECK(elapsed < 5s);                       // 有界，不等挂起 handler/drain
        CHECK(!probe->released.load());            // 确实没有等 handler 返回
        CHECK(probe->done.load() == false);
        CHECK(engine.CountConnections() == 0);
        CHECK(!engine.Stop());                     // 幂等
        // engine 作用域结束：owner/context 真销毁（threads 在下方核对）
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    // context 已销毁后放行挂起的 handler：view/shell 安全 + 晚到操作明确拒绝/幂等。
    probe->released.store(true);
    CHECK(WaitUntil([&] { return probe->done.load(); }, 10s));
    CHECK(probe->view_stable.load());
    CHECK(probe->view_size.load() == std::string("stop-payload").size());
    CHECK(probe->buffered.load() == probe->view_size.load());
    CHECK(probe->write_error.load() == static_cast<int>(std::errc::operation_canceled));
    CHECK(probe->wake_error.load() == static_cast<int>(std::errc::operation_canceled));
    CHECK(probe->close_first.load() == 0 && probe->close_second.load() == 0);
    // 强停语义：未开始的 OnClose 被压掉（ConnectionBridge::StopServiceOwner 明确不构成
    // drain），此处如实断言“没有收到”，不伪称已收尾。
    CHECK(handler->close_count() == 0);
    client.Close();
    CHECK(FdCount() == fds_before);
    std::cout << "stop_with_suspended_handler: ok" << std::endl;
    return 0;
}

// ---- case 4：生命周期负例 ----
int CaseNegativeLifecycle() {
    WarmRuntime();
    auto handler = std::make_shared<TestHandler>();
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        Engine engine(handler);
        // 未接线选项：显式拒绝，不静默忽略、不起线程。
        // ticker 已接线（见 net.engine_tick_* / EventHandler::OnTick），不再是拒绝项，
        // 因此不在本负例里以「拒绝」断言它；reuse_port 仍代表显式拒绝集合。
        Options reuse_port;
        reuse_port.reuse_port = true;
        CHECK(IsError(engine.Run(Loopback(0), reuse_port), std::errc::operation_not_supported));
        CHECK(ThreadCount() == threads_before);
        CHECK(FdCount() == fds_before);
        // bind 失败：占用端口 → 明确错误 + 已起线程/listener fd 全回落。
        {
            auto occupied = OpenStream();
            std::error_code bind_error;
            CHECK(!occupied.Bind(Loopback(0)));
            CHECK(!occupied.Listen(8));
            const auto taken = occupied.LocalAddress(bind_error);
            CHECK(!bind_error);
            const int threads_steady = ThreadCount();
            const int fds_steady = FdCount();
            const auto failed = engine.Run(Loopback(taken.Port()), Options{});
            CHECK(failed);
            CHECK(failed.value() == EADDRINUSE || IsError(failed, std::errc::address_in_use));
            CHECK(ThreadCount() == threads_steady);
            CHECK(FdCount() == fds_steady);
            occupied.Close();
        }
        // 回落干净：同一 Engine 可再次成功启动。
        CHECK(!engine.Run(Loopback(0), Options{}));
        CHECK(IsError(engine.Run(Loopback(0), Options{}), std::errc::device_or_resource_busy));
        CHECK(!engine.Stop());
        CHECK(!engine.Stop());                                     // 幂等
        addresses.push_back(Loopback(2));
        CHECK(IsError(engine.Run(Loopback(0), Options{}), std::errc::operation_canceled));  // 终态
        CHECK(IsError(engine.LocalAddresses(addresses), std::errc::operation_not_permitted));
        CHECK(addresses.size() == 1 && addresses[0].Port() == 2);  // out 未被改写
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);
    std::cout << "negative_lifecycle: ok" << std::endl;
    return 0;
}

// ---- case 5：受管协程内 Run/Stop 在副作用前拒绝，服务仍可用 ----
int CaseManagedReject() {
    WarmRuntime();
    auto handler = std::make_shared<TestHandler>();
    handler->SetClosePayload("close");
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        Engine engine(handler);
        const auto managed_run = RunInCoroutine([&] { return engine.Run(Loopback(0), Options{}); });
        CHECK(IsError(managed_run, std::errc::operation_not_permitted));
        CHECK(handler->boot_calls() == 0);                 // 未起 owner、未跑 OnBoot
        CHECK(ThreadCount() == threads_before);
        CHECK(FdCount() == fds_before);
        CHECK(!engine.Run(Loopback(0), Options{}));
        CHECK(handler->boot_calls() == 1);
        const auto managed_stop = RunInCoroutine([&] { return engine.Stop(); });
        CHECK(IsError(managed_stop, std::errc::operation_not_permitted));
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1);
        auto client = ConnectTo(addresses[0]);
        SendAll(client, "managed-reject");
        CHECK(ReceiveAtMost(client, std::string("managed-reject").size(), 8s) == "managed-reject");
        CHECK(WaitUntil([&] { return handler->open_count() == 1; }, 8s));   // 服务仍可用
        SendAll(client, "close");
        CHECK(PeerClosed(client, 8s));
        CHECK(WaitUntil([&] { return engine.CountConnections() == 0; }, 8s));
        client.Close();
        CHECK(!engine.Stop());                             // 控制线程强停成功
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);
    std::cout << "managed_reject: ok" << std::endl;
    return 0;
}

// ---- case 6：OnClose 抛出后计数仍须回落（F2；原实现漏减 ⇒ CountConnections 恒 1）----
int CaseOnCloseThrowCount() {
    WarmRuntime();
    auto handler = std::make_shared<TestHandler>();
    handler->SetThrowInClose(true);
    handler->SetClosePayload("close");
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), TwoLoopOptions()));
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1);
        auto client = ConnectTo(addresses[0]);
        SendAll(client, "throw-close");
        CHECK(ReceiveAtMost(client, std::string("throw-close").size(), 5s) == "throw-close");
        CHECK(WaitUntil([&] { return engine.CountConnections() == 1; }, 8s));   // 接纳成功 +1
        SendAll(client, "close");                                               // Action::Close → OnClose（抛出）
        CHECK(PeerClosed(client, 8s));
        CHECK(WaitUntil([&] { return handler->close_count() == 1; }, 10s));     // OnClose 确实进入
        // F2 核心：OnClose 抛出也不能漏减；计数最终必须回落 0。
        CHECK(WaitUntil([&] { return engine.CountConnections() == 0; }, 8s));
        CHECK(!engine.Stop());
        client.Close();
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);
    std::cout << "onclose_throw_count: ok" << std::endl;
    return 0;
}

// ---- case 7：OnClose 挂起跨 Engine 强停/销毁（F1）----
int CaseOnCloseSuspendedStopDestroy() {
    WarmRuntime();
    auto gate = std::make_shared<CloseGate>();
    auto handler = std::make_shared<TestHandler>();
    handler->HoldOnClose(gate);
    handler->SetClosePayload("close");
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    NativeSocket client;
    ICoConnection* live_conn = nullptr;
    std::shared_ptr<std::atomic<std::size_t>> wb_counter;              // 白盒补充：共享计数
    std::shared_ptr<bbt::infra::net::EventHandlerAdapter> wb_adapter;
    std::weak_ptr<std::atomic<std::size_t>> wb_weak;
    {
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), Options{}));
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1);
        client = ConnectTo(addresses[0]);
        SendAll(client, "pre-close");
        CHECK(ReceiveAtMost(client, std::string("pre-close").size(), 5s) == "pre-close");
        CHECK(WaitUntil([&] { return engine.CountConnections() == 1; }, 8s));
        live_conn = handler->captured_connection();
        CHECK(live_conn != nullptr);                         // 引擎存活期抓取的真实连接引用

        // 白盒补充（仅本 case 直接引用 detail 适配器）：计数由适配器共享持有，不指向
        // ServerEngine；weak 证明计数在 Engine 之外仍被适配器保活，减计数恰一次为 0。
        {
            static std::shared_ptr<NullHandler> null_handler = std::make_shared<NullHandler>();
            wb_counter = std::make_shared<std::atomic<std::size_t>>(1);   // 模拟 Adopt 后 +1
            wb_weak = wb_counter;
            wb_adapter = std::make_shared<bbt::infra::net::EventHandlerAdapter>(
                null_handler, wb_counter);
            wb_adapter->OnClose(*live_conn, std::error_code{});
            CHECK(wb_counter->load() == 0);                 // 减计数恰一次
            wb_counter.reset();                             // 外部强引用释放
            CHECK(!wb_weak.expired());                      // 计数仍被适配器共享持有
        }

        SendAll(client, "close");                            // Action::Close → OnClose 进入并挂起
        CHECK(WaitUntil([&] { return gate->entered.load(); }, 8s));
        CHECK(!gate->released.load());
        const auto start = Clock::now();
        CHECK(!engine.Stop());                               // 有界，不等挂起 OnClose
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);
        CHECK(elapsed < 5s);
        CHECK(!gate->released.load());
        CHECK(!gate->done.load());
        CHECK(engine.CountConnections() == 0);               // 强停后对外 0
        CHECK(!engine.Stop());                               // 幂等
    }
    // engine 作用域结束：owner/context 真销毁（threads 回基线）。
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(!wb_weak.expired());                               // 适配器在 Engine 之外仍持共享计数
    wb_adapter.reset();
    CHECK(wb_weak.expired());                                // 适配器销毁后计数才释放
    // Engine 已销毁 → 放行挂起的 OnClose：适配器必须仍安全完成恰一次
    //（旧裸指针实现在此对已析构 ServerEngine 成员做 fetch_sub = 静态可达 UAF，无 ASan 无确定红）。
    gate->released.store(true);
    CHECK(WaitUntil([&] { return gate->done.load(); }, 10s));
    CHECK(handler->close_count() == 1);
    client.Close();
    CHECK(FdCount() == fds_before);

    // 新一代 Engine：上一代迟到 OnClose 不污染新代计数（代际隔离，无跨代别名）。
    {
        Engine engine2(handler);
        CHECK(!engine2.Run(Loopback(0), Options{}));
        CHECK(engine2.CountConnections() == 0);
        std::vector<Address> second;
        CHECK(!engine2.LocalAddresses(second));
        CHECK(second.size() == 1);
        auto c2 = ConnectTo(second[0]);
        SendAll(c2, "gen2");
        CHECK(ReceiveAtMost(c2, std::string("gen2").size(), 5s) == "gen2");
        CHECK(WaitUntil([&] { return engine2.CountConnections() == 1; }, 8s));
        SendAll(c2, "close");
        CHECK(PeerClosed(c2, 8s));
        CHECK(WaitUntil([&] { return engine2.CountConnections() == 0; }, 10s));
        c2.Close();
        CHECK(!engine2.Stop());
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);
    std::cout << "onclose_suspended_stop_destroy: ok" << std::endl;
    return 0;
}


// ---- case 8：空拥有关系：Run 在任何副作用前 invalid_argument，无线程/FD 副作用 ----
int CaseNullHandler() {
    WarmRuntime();
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    {
        std::shared_ptr<EventHandler> empty;      // 允许空构造（文档写明）
        Engine engine(empty);
        std::vector<Address> addresses;
        CHECK(engine.Run(Loopback(0), TwoLoopOptions()) == std::errc::invalid_argument);
        CHECK(engine.CountConnections() == 0);
        CHECK(engine.LocalAddresses(addresses) == std::errc::operation_not_permitted);
        CHECK(!engine.Stop());                    // 未运行：幂等成功，不解引用空拥有关系
        CHECK(engine.Run(Loopback(0), TwoLoopOptions()) == std::errc::invalid_argument);
    }
    CHECK(ThreadCount() == threads_before);       // 未起 owner/listener 线程
    CHECK(FdCount() == fds_before);               // 未建 socket
    std::cout << "null_handler: ok" << std::endl;
    return 0;
}

// ---- case 9：真实 TCP 挂起 OnClose 下 handler 的拥有型寿命（F9）----
// 外部释放调用方引用后，handler 仍被适配器/会话/挂起回调共同持有；Engine 真销毁后
// 回调安全继续；放行后才最终释放。不把用户 done 当 RAII 已完成。
int CaseHandlerOwnedAcrossDestroy() {
    WarmRuntime();
    auto gate = std::make_shared<CloseGate>();
    auto handler = std::make_shared<TestHandler>();
    handler->HoldOnClose(gate);
    handler->SetClosePayload("close");
    std::weak_ptr<TestHandler> weak = handler;    // 同一控制块的真实 weak
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    NativeSocket client;
    {
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), Options{}));
        CHECK(!engine.LocalAddresses(addresses));
        client = ConnectTo(addresses[0]);
        SendAll(client, "pre-close");
        CHECK(ReceiveAtMost(client, std::string("pre-close").size(), 5s) == "pre-close");
        CHECK(WaitUntil([&] { return engine.CountConnections() == 1; }, 8s));
        SendAll(client, "close");                 // Action::Close → OnClose 进入并挂起
        CHECK(WaitUntil([&] { return gate->entered.load(); }, 8s));
        handler.reset();                          // 调用方释放：仅剩引擎侧拥有者
        CHECK(!weak.expired());                   // 适配器/会话/挂起回调仍持 handler
        const auto start = Clock::now();
        CHECK(!engine.Stop());                    // 有界，不等挂起 OnClose
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);
        CHECK(elapsed < 5s);
        CHECK(engine.CountConnections() == 0);
    }
    // Engine 作用域结束：owner/context 真销毁；挂起中的回调仍安全持有 handler。
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(!weak.expired());
    gate->released.store(true);                   // 放行挂起回调
    CHECK(WaitUntil([&] { return gate->done.load(); }, 10s));
    CHECK(WaitUntil([&] { return weak.expired(); }, 10s));   // 回调真正结束才最终释放
    client.Close();
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
    std::cout << "handler_owned_across_destroy: ok" << std::endl;
    return 0;
}

// ---- case 10：Boot 已进入挂起 → 外部释放 → 真实 10s timed_out → Engine 销毁，
// 网络线程回落（ServerEngine 未被 Boot 延寿），恢复 Boot 后 handler 最终释放 ----
int CaseBootSuspendedTimeoutDestroy() {
    WarmRuntime();
    auto gate = std::make_shared<CloseGate>();
    auto handler = std::make_shared<TestHandler>();
    handler->HoldOnBoot(gate);
    std::weak_ptr<TestHandler> weak = handler;
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    {
        Engine engine(handler);
        handler.reset();                          // 外部拥有者先释放
        CHECK(!weak.expired());                   // Engine 持真实 shared_ptr
        const auto start = Clock::now();
        const auto error = engine.Run(Loopback(0), Options{});
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);
        CHECK(error == std::errc::timed_out);     // 按真实内部 deadline 回落，未缩短产品 deadline
        CHECK(elapsed >= 9s);
        CHECK(gate->entered.load());
        CHECK(!gate->done.load());                // Boot 任务仍在挂起，未被强杀
        CHECK(!weak.expired());                   // 挂起中的 Boot 闭包持 handler
        CHECK(engine.CountConnections() == 0);
        std::vector<Address> addresses;
        CHECK(engine.LocalAddresses(addresses) == std::errc::operation_not_permitted);   // 未进入 Running
    }
    // Engine 销毁：ServerEngine 未被挂起 Boot 延寿（线程真实回落）。
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(FdCount() == fds_before);
    CHECK(!weak.expired());                       // 挂起中的 Boot 仍安全持有 handler
    gate->released.store(true);                   // 恢复 Boot
    CHECK(WaitUntil([&] { return gate->done.load(); }, 10s));
    CHECK(WaitUntil([&] { return weak.expired(); }, 10s));   // 任务结束才最终释放
    std::cout << "boot_suspended_timeout_destroy: ok" << std::endl;
    return 0;
}

// ---- 51 新增：Boot 阶段观测（F6）/ Starting 强停（F3）/ Stop 终态与并发争用（F7）----
//
// BootGateProbe：OnBoot 内真实调用两个公共观测面并按闸门挂起（T1 不挂起、T2 挂起）。
struct BootGateProbe {
    std::atomic<bool> entered{false};
    std::atomic<bool> released{false};
    std::atomic<bool> done{false};
    std::atomic<int> boot_calls{0};
    std::atomic<int> local_error{-1};
    std::atomic<std::size_t> address_count{0};
    std::atomic<std::size_t> port{0};
    std::atomic<std::size_t> connections{0};
};

// T1/T2 专用 handler：OnBoot 内调用 Engine 的公共观测面（LocalAddresses/CountConnections），
// 因此需要真实 Engine 句柄。Engine 先于本 handler 释放；观测只发生在 OnBoot 内。
class BootObservingHandler final : public EventHandler {
public:
    void Attach(const Engine* engine, std::shared_ptr<BootGateProbe> probe)
    {
        engine_ = engine;
        probe_ = std::move(probe);
    }

    bool OnBoot() override
    {
        const auto probe = probe_;
        if (!probe || engine_ == nullptr) { return true; }
        probe->boot_calls.fetch_add(1);
        std::vector<Address> addresses;
        auto* engine = const_cast<Engine*>(engine_);
        probe->local_error.store(engine->LocalAddresses(addresses).value());
        probe->address_count.store(addresses.size());
        if (!addresses.empty()) { probe->port.store(addresses[0].Port()); }
        probe->connections.store(engine->CountConnections());
        probe->entered.store(true);
        const auto deadline = Clock::now() + 30s;
        while (!probe->released.load() && Clock::now() < deadline) {
            bbt::coroutine::sync::CoWaiter waiter;
            (void)waiter.Wait(bbt::coroutine::WaitOptions{Clock::now() + 10ms});
        }
        probe->done.store(true);
        return true;
    }

    Action OnOpen(ICoConnection&) override { return Action::None; }

    Action OnTraffic(ICoConnection& connection, bool wake) override
    {
        if (wake) { return Action::None; }
        const std::string data{connection.Peek()};
        if (data.empty()) { return Action::None; }
        (void)connection.Write(data, Clock::now() + 5s);
        (void)connection.Discard(data.size());
        return Action::None;
    }

    void OnClose(ICoConnection&, const std::error_code&) override {}

private:
    const Engine* engine_{nullptr};
    std::shared_ptr<BootGateProbe> probe_;
};

// ---- T1：OnBoot 内公共观测可用（Starting 相位观测）+ Run 成功后真实动态 TCP 回显 ----
// 判别力：旧实现 facade 句柄尚未发布（拒绝）或 Run 持 control_mutex_ 等 Boot（死等到 10s），
// 本 case 在其上必红。
int CaseBootObservation() {
    WarmRuntime();
    auto probe = std::make_shared<BootGateProbe>();
    auto handler = std::make_shared<BootObservingHandler>();
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    NativeSocket client;
    {
        Engine engine(handler);
        handler->Attach(&engine, probe);
        probe->released.store(true);                  // T1 不挂起：只验证 Boot 内观测
        CHECK(!engine.Run(Loopback(0), TwoLoopOptions()));
        CHECK(probe->boot_calls.load() == 1);
        CHECK(probe->entered.load());
        CHECK(probe->local_error.load() == 0);        // OnBoot 内 LocalAddresses 成功
        CHECK(probe->address_count.load() == 1);
        CHECK(probe->port.load() != 0);               // 实际绑定端口（port 0 已解析）
        CHECK(probe->connections.load() == 0);        // 尚未接纳
        CHECK(engine.CountConnections() == 0);
        CHECK(!engine.LocalAddresses(addresses));     // Run 后读回同一真实地址
        CHECK(addresses.size() == 1 && addresses[0].Port() == probe->port.load());
        CHECK(addresses[0].Family() == AddressFamily::IPv4);
        CHECK(addresses[0].ToString().find("127.0.0.1") != std::string::npos);
        client = ConnectTo(addresses[0]);             // 真实 dynamic TCP 回显
        const std::string payload = "boot-observation-echo";
        SendAll(client, payload);
        CHECK(ReceiveAtMost(client, payload.size(), 5s) == payload);
        CHECK(!engine.Stop());
        client.Close();
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
    std::cout << "boot_observation: ok" << std::endl;
    return 0;
}

// ---- T2：OnBoot 已进闸门 → 另一 native 线程 Stop 不等闸门真实收口 Starting 网络并返回；
// inflight Run 及时 operation_canceled（不是等 10s 或等 Boot 完成）；随后放行，Boot 自然
// 完成、handler 最终释放、无重新 accept、Engine 销毁安全 ----
int CaseStopDuringBoot() {
    WarmRuntime();
    auto probe = std::make_shared<BootGateProbe>();
    auto handler = std::make_shared<BootObservingHandler>();
    std::weak_ptr<BootObservingHandler> weak = handler;
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    {
        Engine engine(handler);
        handler->Attach(&engine, probe);
        std::error_code run_error = std::make_error_code(std::errc::state_not_recoverable);
        std::atomic<bool> run_done{false};
        std::chrono::milliseconds run_elapsed{0};
        // Run 在 native 线程上启动：Run 自身要等 Boot 或 Stop 的裁决。
        std::thread runner([&] {
            const auto start = Clock::now();
            run_error = engine.Run(Loopback(0), TwoLoopOptions());
            run_elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);
            run_done.store(true);
        });
        CHECK(WaitUntil([&] { return probe->entered.load(); }, 10s));   // OnBoot 已进闸门
        CHECK(!probe->done.load());
        CHECK(!probe->released.load());
        const auto start = Clock::now();
        CHECK(!engine.Stop());                                          // 不等闸门/业务
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);
        CHECK(elapsed < 5s);                                            // 真实收口且有界
        CHECK(!probe->released.load());
        CHECK(!probe->done.load());                                     // 未强杀任务
        CHECK(WaitUntil([&] { return run_done.load(); }, 5s));          // inflight Run 及时收场
        CHECK(IsError(run_error, std::errc::operation_canceled));
        CHECK(run_elapsed < 5s);                                        // 不是等满 10s/等 Boot
        std::vector<Address> addresses;
        CHECK(IsError(engine.LocalAddresses(addresses), std::errc::operation_not_permitted));
        CHECK(engine.CountConnections() == 0);
        CHECK(!engine.Stop());                                          // 终态幂等
        runner.join();
    }
    // Engine 销毁：owner/context 真销毁（Boot 不保活网络域），挂起 Boot 仍在闸门上。
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
    CHECK(!probe->done.load());
    handler.reset();
    CHECK(!weak.expired());                                             // 已开始的 Boot 闭包持 handler
    probe->released.store(true);                                        // 放行已开始的 Boot
    CHECK(WaitUntil([&] { return probe->done.load(); }, 10s));
    CHECK(WaitUntil([&] { return weak.expired(); }, 10s));              // 任务结束才最终释放
    CHECK(probe->boot_calls.load() == 1);                               // 不重复、不重新 accept
    std::cout << "stop_during_boot: ok" << std::endl;
    return 0;
}

// ---- T3：Stop-before-Run 终态、并发 Run 争用、Run/Stop 控制竞争（明确 handshake）----
int CaseStopBeforeRunAndContention() {
    WarmRuntime();
    auto handler = std::make_shared<TestHandler>();
    handler->SetClosePayload("close");
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    {
        // 1) Stop-before-Run：未启动即终态，随后 Run 明确 operation_canceled。
        Engine engine(handler);
        std::vector<Address> addresses;
        CHECK(!engine.Stop());
        CHECK(IsError(engine.LocalAddresses(addresses), std::errc::operation_not_permitted));
        CHECK(IsError(engine.Run(Loopback(0), Options{}), std::errc::operation_canceled));
        CHECK(!engine.Stop());                                     // 终态幂等
        CHECK(handler->boot_calls() == 0);                         // 未起 owner、未跑 OnBoot
        CHECK(ThreadCount() == threads_before);
    }
    {
        // 2) 并发 Run 争用：barrier 后同时进入，恰一个进入 Starting/Running，另一个 busy。
        Engine engine(handler);
        std::atomic<int> arrived{0};
        std::atomic<bool> go{false};
        std::error_code results[2];
        std::thread threads[2];
        for (int index = 0; index < 2; ++index) {
            threads[index] = std::thread([&, index] {
                arrived.fetch_add(1);
                while (!go.load()) { std::this_thread::yield(); }
                results[index] = engine.Run(Loopback(0), Options{});
            });
        }
        CHECK(WaitUntil([&] { return arrived.load() == 2; }, 5s));
        go.store(true);
        for (auto& thread : threads) { thread.join(); }
        const int wins = (results[0] ? 0 : 1) + (results[1] ? 0 : 1);
        const int busy = (IsError(results[0], std::errc::device_or_resource_busy) ? 1 : 0)
            + (IsError(results[1], std::errc::device_or_resource_busy) ? 1 : 0);
        CHECK(wins == 1 && busy == 1);                             // 只有一个可进入
        CHECK(handler->boot_calls() == 1);                         // Boot 恰一次
        CHECK(!engine.Stop());
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    {
        // 3) Run/Stop 控制竞争：barrier 后 Run 与 Stop 同时进入；二次 Run 只能 busy 或终态
        //    canceled，且 Stop 首胜后终态不得被 Run 覆盖回 Running。
        Engine engine(handler);
        CHECK(!engine.Run(Loopback(0), Options{}));
        const int boot_before = handler->boot_calls();
        std::atomic<bool> go{false};
        std::error_code run_error = std::make_error_code(std::errc::state_not_recoverable);
        std::error_code stop_error = std::make_error_code(std::errc::state_not_recoverable);
        std::thread runner([&] {
            while (!go.load()) { std::this_thread::yield(); }
            run_error = engine.Run(Loopback(0), Options{});
        });
        std::thread stopper([&] {
            while (!go.load()) { std::this_thread::yield(); }
            stop_error = engine.Stop();
        });
        go.store(true);
        runner.join();
        stopper.join();
        CHECK(IsError(run_error, std::errc::device_or_resource_busy)
            || IsError(run_error, std::errc::operation_canceled));
        CHECK(!stop_error);
        CHECK(handler->boot_calls() == boot_before);               // 竞争不重跑 Boot
        CHECK(IsError(engine.Run(Loopback(0), Options{}), std::errc::operation_canceled));
        CHECK(engine.CountConnections() == 0);
        std::vector<Address> addresses;
        CHECK(IsError(engine.LocalAddresses(addresses), std::errc::operation_not_permitted));
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));   // 无资源泄漏
    std::cout << "stop_before_run_and_contention: ok" << std::endl;
    return 0;
}

// ---- T4：尚未执行 Boot 任务的超时/Stop ⇒ 不得进入 OnBoot（原 10s deadline 保持）----
// 真实非协作占用（53 修正）：WarmRuntime 已是单 Processer（m_cfg_static_thread_num=1），
// 一个占用任务即可占满执行线程。51 版用 std::this_thread::sleep_for 占用——该调用经
// libc nanosleep 被 Hook 拦截（Hook_Nanosleep → YieldUntilTimeout），协程挂起让出、
// 从未真正占住线程，8 个「占用」全部立即开始且 Boot 照常执行（51 红根因，已由
// runtime 源码证实）。本版改用绕过 Hook 的原生 nanosleep（RawNanosleep）占住唯一
// Processer，配合 started 握手 ⇒ Boot 任务必然排队形成 Pending 窗口（确定性时序，
// 不盲增占用数、不 sleep 编时序、不改 runtime、15s 有界自释放）。
int CaseBootPendingAbandon() {
    WarmRuntime();
    auto probe = std::make_shared<BootGateProbe>();
    auto handler = std::make_shared<BootObservingHandler>();
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        Engine engine(handler);
        handler->Attach(&engine, probe);
        const std::size_t stack = g_bbt_coroutine_config->m_cfg_stack_size;
        auto release = std::make_shared<std::atomic<bool>>(false);
        auto drained = std::make_shared<std::atomic<bool>>(false);
        auto flag = std::make_shared<std::atomic<bool>>(false);
        g_scheduler->RegistCoroutineTask(stack, [flag, release] {
            flag->store(true);
            const auto deadline = Clock::now() + 15s;   // 有界：无论如何 15s 内自释放
            while (!release->load() && Clock::now() < deadline) {
                RawNanosleep(1);                        // 真实占住执行线程（不协作让出）
            }
        }, "net.engine.test.occupy");
        CHECK(WaitUntil([&] { return flag->load(); }, 3s));   // 握手：唯一 Processer 已被占住
        std::cout << "boot_pending_abandon diagnose: occupied_exec_threads=1" << std::endl;
        // 1) Boot 任务只能排队：Run 按原 10s deadline 超时，OnBoot 从未进入。
        const auto start = Clock::now();
        const auto error = engine.Run(Loopback(0), Options{});
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() - start);
        CHECK(error == std::errc::timed_out);
        CHECK(elapsed >= 9s);                              // 原 deadline 保持（不缩短）
        CHECK(probe->boot_calls.load() == 0);              // 未执行
        CHECK(!probe->entered.load());
        // 2) 回落干净 + 观测终态语义
        CHECK(engine.CountConnections() == 0);
        CHECK(IsError(engine.LocalAddresses(addresses), std::errc::operation_not_permitted));
        CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
        CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
        // 3) 放行 Processer：被放弃的 Boot 任务不得进入 OnBoot。
        probe->released.store(true);                       // 若误进 OnBoot 也立即返回（不挂死）
        release->store(true);
        g_scheduler->RegistCoroutineTask(stack, [drained] { drained->store(true); },
            "net.engine.test.marker");
        CHECK(WaitUntil([&] { return drained->load(); }, 10s));
        const auto grace_deadline = Clock::now() + 300ms;
        while (Clock::now() < grace_deadline) { std::this_thread::sleep_for(5ms); }
        CHECK(probe->boot_calls.load() == 0);              // 未开始的任务仍不得进 OnBoot
        CHECK(!probe->entered.load());
        CHECK(!probe->done.load());
        // 4) 终态不被后续 Run 覆盖
        CHECK(!engine.Stop());
        CHECK(IsError(engine.Run(Loopback(0), Options{}), std::errc::operation_canceled));
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
    std::cout << "boot_pending_abandon: ok" << std::endl;
    return 0;
}

// ---- 53 新增：跨代 OnBoot 重入栅栏（R52-1 裁定）----
// 时序握手：旧代 Boot 已进闸门（started && !done）⇒ Run 按原 10s 超时回落；
// 重试 Run 必须 busy 且 boot_calls 不增（同一 handler 不跨代重入 OnBoot）；
// 放行旧 Boot 自然完成后（窄结果句柄随代保留，不保活网络 context），再 Run
// 真实成功 + TCP 回显。判别力：无栅栏实现会登记新 Boot 并再次进入 OnBoot
//（boot_calls 增至 2）且重试返回 timed_out 而非 busy —— 本 case 在其上必红。
int CaseBootCrossGenReentryBusy() {
    WarmRuntime();
    auto probe = std::make_shared<BootGateProbe>();
    auto handler = std::make_shared<BootObservingHandler>();
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    NativeSocket client;
    {
        Engine engine(handler);
        handler->Attach(&engine, probe);
        // 1) 旧代 Boot 已进闸门：Run 在另一线程等待至原 10s deadline 超时。
        std::error_code run_error;
        std::thread runner([&] { run_error = engine.Run(Loopback(0), Options{}); });
        CHECK(WaitUntil([&] { return probe->entered.load(); }, 10s));   // Boot 已开始（握手）
        CHECK(probe->boot_calls.load() == 1);
        runner.join();
        CHECK(IsError(run_error, std::errc::timed_out));   // 原 10s 语义保持
        // 2) 旧 Boot 仍 started && !done：重试拒绝 busy，boot_calls 不增。
        CHECK(IsError(engine.Run(Loopback(0), Options{}), std::errc::device_or_resource_busy));
        CHECK(probe->boot_calls.load() == 1);              // 跨代 OnBoot 重入被阻止
        // 3) 放行旧 Boot：已开始的任务按值持 handler 自然完成（Engine 不等业务）。
        probe->released.store(true);
        CHECK(WaitUntil([&] { return probe->done.load(); }, 10s));
        // 4) 完成后可重试：再 Run 真实成功 + 真实 dynamic TCP 回显（boot_calls==2，不并发）。
        CHECK(!engine.Run(Loopback(0), Options{}));
        CHECK(probe->boot_calls.load() == 2);
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1 && addresses[0].Port() != 0);
        client = ConnectTo(addresses[0]);
        const std::string payload = "crossgen-retry-echo";
        SendAll(client, payload);
        CHECK(ReceiveAtMost(client, payload.size(), 5s) == payload);
        CHECK(!engine.Stop());
        client.Close();
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));   // 无资源泄漏
    std::cout << "boot_crossgen_reentry_busy: ok" << std::endl;
    return 0;
}

// LC: 同值优先 sub0，顺序握手形成 registry=(2,1)，关闭 sub0 两条后
// 物理存量=(0,1)。旧实现选 sub1；修复后选 sub0。OnClose 全程挂起。
int CaseLeastConnectionsClosedRegistry() {
    WarmRuntime();
    auto gate = std::make_shared<CloseGate>();
    auto handler = std::make_shared<TestHandler>();
    handler->HoldOnClose(gate);
    std::error_code error;
    auto server = bbt::infra::net::ServerEngine::Create(handler, error);
    CHECK(server && !error);
    auto options = TwoLoopOptions();
    options.lb = bbt::infra::net::LoadBalancing::LeastConnections;
    CHECK(!server->Run(Loopback(0), options));
    std::vector<Address> addresses;
    CHECK(!server->LocalAddresses(addresses));
    auto first = server->subs_[0].bridge;
    auto second = server->subs_[1].bridge;
    std::vector<NativeSocket> clients;
    const auto connect = [&] {
        auto client = ConnectTo(addresses[0]);
        SendAll(client, "sample");
        CHECK(ReceiveAtMost(client, 6, 5s) == "sample"); // Adopt/OnOpen 已发生
        clients.push_back(std::move(client));
    };
    connect();
    CHECK(first->SessionCount() == 1 && second->SessionCount() == 0);
    connect();
    CHECK(first->SessionCount() == 1 && second->SessionCount() == 1);
    connect();
    CHECK(first->SessionCount() == 2 && second->SessionCount() == 1);
    CHECK(WaitUntil([&] { return server->CountConnections() == 3; }, 5s));
    auto closed0 = first->SessionOf(0);
    auto closed1 = first->SessionOf(1);
    CHECK(closed0 && closed1);
    auto done = std::make_shared<std::promise<void>>();
    auto future = done->get_future();
    CHECK(first->Owner().Submit([first, done] {
        CHECK(!first->CloseNative(0, {}));
        CHECK(!first->CloseNative(1, {}));
        CHECK(!first->CloseNative(0, {})); // 幂等，不重复减计
        CHECK(!first->CloseNative(1, {}));
        done->set_value();
    }) == bbt::infra::net::SubmitStatus::Accepted);
    CHECK(future.wait_for(5s) == std::future_status::ready);
    future.get();
    CHECK(closed0->SnapshotState().physical_closed && closed1->SnapshotState().physical_closed);
    CHECK(WaitUntil([&] { return gate->entered.load(); }, 5s));
    CHECK(!closed0->SnapshotState().close_finished && !closed1->SnapshotState().close_finished);
    CHECK(first->SessionCount() == 2 && second->SessionCount() == 1); // registry 不删
    CHECK(server->CountConnections() == 3); // 业务 OnClose 尚未归还，不冒充物理计数
    connect();
    CHECK(first->SessionCount() == 3 && second->SessionCount() == 1); // 关键红绿断言
    CHECK(!server->Stop());
    CHECK(first->SessionCount() == 0 && second->SessionCount() == 0); // Detach 清表
    CHECK(server->CountConnections() == 0);
    gate->released.store(true);
    CHECK(WaitUntil([&] { return closed0->SnapshotState().close_finished
        && closed1->SnapshotState().close_finished; }, 5s));
    CHECK(closed0->SnapshotState().physical_closed && closed1->SnapshotState().physical_closed);
    std::cout << "least_connections_closed_registry: ok" << std::endl;
    return 0;
}

// ---- case 16：未支持地址族在副作用前诚实拒绝（Unix/IPv6），同一 Engine 可用 IPv4 重试 ----
// 缺陷：旧实现 Run(Unix) 假成功（bind 成功、发布 bound 快照、返回 0），但每个已接纳 fd 都被
// StreamConnection 家族门静默丢弃 ⇒ 零业务事件（146 探针 2.1 实证）。判别力：旧实现在本 case
// **首个家族断言处即 CHECK abort**（error 为 0 而非 operation_not_supported），并留下 bind 生成
// 的自有 socket 路径（由父级运行器在 red 后清理，日志原样保留）。
int CaseUnsupportedFamilyReject() {
    WarmRuntime();
    auto handler = std::make_shared<TestHandler>();
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    NativeSocket client;
    {
        (void)::unlink(kUnsupportedFamilyPath);            // 自有路径：先清残留
        CHECK(!PathExists(kUnsupportedFamilyPath));
        Engine engine(handler);
        std::error_code unix_error;
        const auto unix_address = Address::Unix(kUnsupportedFamilyPath, unix_error);
        CHECK(!unix_error);
        CHECK(unix_address.Valid() && unix_address.Family() == AddressFamily::Unix);
        std::error_code v6_error;
        const auto v6_address = Address::Ip("::1", 0, v6_error);
        CHECK(!v6_error);                                  // numeric 解析成功（与内核 IPv6 可用性无关）
        CHECK(v6_address.Valid() && v6_address.Family() == AddressFamily::IPv6);

        // 1) Unix stream 合法地址：明确拒绝，且从不 bind（自有路径从未创建）。
        CHECK(IsError(engine.Run(unix_address, Options{}), std::errc::operation_not_supported));
        CHECK(!PathExists(kUnsupportedFamilyPath));
        CHECK(handler->boot_calls() == 0);                 // 未登记 Boot、未进入 OnBoot
        CHECK(engine.CountConnections() == 0);
        addresses.push_back(Loopback(7));                  // 观测面：拒绝且不发布 bound 快照
        CHECK(IsError(engine.LocalAddresses(addresses), std::errc::operation_not_permitted));
        CHECK(addresses.size() == 1 && addresses[0].Port() == 7);   // out 未被改写
        CHECK(ThreadCount() == threads_before);            // 未起 owner 线程
        CHECK(FdCount() == fds_before);                    // 未建 socket/未 dup

        // 2) IPv6 合法地址：同样早拒绝；门在 socket() 之前 ⇒ 与内核 IPv6 可用性无关。
        CHECK(IsError(engine.Run(v6_address, Options{}), std::errc::operation_not_supported));
        CHECK(handler->boot_calls() == 0);
        CHECK(ThreadCount() == threads_before);
        CHECK(FdCount() == fds_before);

        // 3) 拒绝不终结为 Stopped：同一 Engine 换 IPv4 地址可真实服务 + 真实回显。
        CHECK(!engine.Run(Loopback(0), Options{}));
        CHECK(handler->boot_calls() == 1);
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1);
        CHECK(addresses[0].Family() == AddressFamily::IPv4);
        CHECK(addresses[0].Port() != 0);                   // port 0 → 内核分配的真实端口
        client = ConnectTo(addresses[0]);
        const std::string payload = "unsupported-family-echo";
        SendAll(client, payload);
        CHECK(ReceiveAtMost(client, payload.size(), 5s) == payload);
        CHECK(WaitUntil([&] { return engine.CountConnections() == 1; }, 5s));
        CHECK(!engine.Stop());
        client.Close();
    }
    CHECK(!PathExists(kUnsupportedFamilyPath));            // 全程未创建自有路径
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
    std::cout << "unsupported_family_reject: ok" << std::endl;
    return 0;
}

// ---- case 17：家族拒绝的错误优先级不变式（域门禁/相位/地址合法性优先于家族门）----
// 诚实标注：本 case 在**旧实现即绿**，不复现缺陷；它是防止「家族门被插到域门/相位检查之前」
// 这一错误修复的回归钉（那样的实现在断言 2/3/4 上必红）。
int CaseUnsupportedFamilyErrorPrecedence() {
    WarmRuntime();
    auto handler = std::make_shared<TestHandler>();
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        (void)::unlink(kUnsupportedFamilyPath);
        CHECK(!PathExists(kUnsupportedFamilyPath));
        Engine engine(handler);
        std::error_code unix_error;
        const auto unix_address = Address::Unix(kUnsupportedFamilyPath, unix_error);
        CHECK(!unix_error && unix_address.Family() == AddressFamily::Unix);

        // 1) 非法地址仍是 invalid_argument（不被家族门折叠成 unsupported）。
        CHECK(engine.Run(Address{}, Options{}) == std::errc::invalid_argument);
        // 2) 受管协程域门禁优先：协程内 Run(Unix) → operation_not_permitted。
        const auto managed = RunInCoroutine([&] { return engine.Run(unix_address, Options{}); });
        CHECK(IsError(managed, std::errc::operation_not_permitted));
        CHECK(handler->boot_calls() == 0);
        CHECK(ThreadCount() == threads_before);
        CHECK(FdCount() == fds_before);
        // 3) 相位优先：Running 中 Run(Unix) → busy（不是 unsupported）。
        CHECK(!engine.Run(Loopback(0), Options{}));
        CHECK(IsError(engine.Run(unix_address, Options{}), std::errc::device_or_resource_busy));
        // 4) 终态优先：Stop 后 Run(Unix) → operation_canceled，服务不被复活。
        CHECK(!engine.Stop());
        CHECK(IsError(engine.Run(unix_address, Options{}), std::errc::operation_canceled));
        CHECK(IsError(engine.LocalAddresses(addresses), std::errc::operation_not_permitted));
        CHECK(engine.CountConnections() == 0);
    }
    CHECK(!PathExists(kUnsupportedFamilyPath));            // 该路径全程不该被创建
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
    std::cout << "unsupported_family_error_precedence: ok" << std::endl;
    return 0;
}

// ---- case 17：多 loop owner/context 身份唯一（C4）----
// 契约依据：Engine.hpp「起 main owner 与 N 个 sub owner（各自 EventLoop 线程 + 私有 bridge）」、
// 执行域设计「网络 reactor 线程不是 runtime worker / 不重复驱动共享 context」。
// 判别力：任一 loop 复用另一 loop 的 context/线程身份，或身份未在 owner 线程内捕获（默认 id）
// ⇒ 身份相等/为默认值时即红。
// 白盒说明：owner 线程与 context 身份本身是内部不变量，公共面不暴露；本用例经私有
// ServerEngine 直连同一生产实现（真实线程、真实 TCP、动态端口），公共 Engine 的双 sub
// 路径由 net.engine_roundtrip 负责。不新增公共 API、不改产品。
int CaseMultiLoopOwnerIdentity() {
    WarmRuntime();
    auto handler = std::make_shared<TestHandler>();
    handler->SetClosePayload("close");
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        std::error_code error;
        auto server = bbt::infra::net::ServerEngine::Create(handler, error);
        CHECK(server && !error);
        CHECK(!server->Run(Loopback(0), TwoLoopOptions()));
        CHECK(server->subs_.size() == 2);
        CHECK(!server->LocalAddresses(addresses));
        CHECK(addresses.size() == 1 && addresses[0].Port() != 0);
        const auto main_id = server->main_loop_->OwnerId();
        const auto sub0_id = server->subs_[0].loop->OwnerId();
        const auto sub1_id = server->subs_[1].loop->OwnerId();
        CHECK(main_id != std::thread::id{} && sub0_id != std::thread::id{} && sub1_id != std::thread::id{});
        CHECK(main_id != sub0_id && main_id != sub1_id && sub0_id != sub1_id);   // 三个独立 OS 线程
        CHECK(main_id != std::this_thread::get_id() && sub0_id != std::this_thread::get_id()
            && sub1_id != std::this_thread::get_id());                          // 都不是调用线程
        CHECK(server->main_loop_->ContextIdentity() != server->subs_[0].loop->ContextIdentity());
        CHECK(server->main_loop_->ContextIdentity() != server->subs_[1].loop->ContextIdentity());
        CHECK(server->subs_[0].loop->ContextIdentity() != server->subs_[1].loop->ContextIdentity());
        CHECK(server->subs_[0].bridge != nullptr && server->subs_[1].bridge != nullptr);
        CHECK(server->subs_[0].bridge != server->subs_[1].bridge);               // 私有 bridge 不共享
        CHECK(server->subs_[0].bridge->Owner().OwnerId() == sub0_id);           // bridge 挂在自己的 owner 上
        CHECK(server->subs_[1].bridge->Owner().OwnerId() == sub1_id);
        // 真实服务：RoundRobin 第一条连接落 sub0，per-sub id 从 0 独立编号。
        auto client = ConnectTo(addresses[0]);
        const std::string payload = "identity-echo";
        SendAll(client, payload);
        CHECK(ReceiveAtMost(client, payload.size(), 5s) == payload);
        CHECK(WaitUntil([&] { return server->subs_[0].bridge->SessionCount() == 1; }, 5s));
        CHECK(server->subs_[1].bridge->SessionCount() == 0);
        CHECK(server->CountConnections() == 1);
        CHECK(!server->Stop());
        CHECK(server->CountConnections() == 0);
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
    std::cout << "multiloop_owner_identity: ok" << std::endl;
    return 0;
}

// ---- case 18：SourceAddrHash 在真实 accept 路径的接线（C4）----
// 契约依据：LoadBalancing.hpp / 0010「SourceAddrHash = IEEE CRC32 对 remote Address::ToString
// 取模」+ ServerEngine::HandoffOwner 把 accept 得到的 peer 交给 LoadBalancer。
// 判别力：接线若退化为 RoundRobin（或换用别的地址形态/别的哈希），下式在首个「同 sub 连续」
// 处即红 —— 期望 sub 序列由 peer 的 ToString 独立算出，per-sub id 又在每个 sub 内从 0 独立
// 编号，故 `id == #{j<i : s_j == s_i}` 当且仅当放置等于期望 hash 时成立。
// CRC32 函数本身由 net.load_balancing 固定向量负责（此处复用同一产品入口，不重复造实现）。
int CaseLoadBalancingSourceAddrHash() {
    WarmRuntime();
    auto handler = std::make_shared<TestHandler>();
    handler->SetClosePayload("close");
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    constexpr std::size_t kConnections = 6;
    std::vector<Address> addresses;
    std::vector<NativeSocket> clients;
    {
        Engine engine(handler);
        Options options = TwoLoopOptions();
        options.lb = bbt::infra::net::LoadBalancing::SourceAddrHash;
        CHECK(!engine.Run(Loopback(0), options));
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1 && addresses[0].Port() != 0);
        std::vector<std::size_t> expected_sub;
        std::vector<std::size_t> peer_ports;
        for (std::size_t index = 0; index < kConnections; ++index) {
            auto client = ConnectTo(addresses[0]);
            std::error_code local_error;
            const auto local = client.LocalAddress(local_error);
            CHECK(!local_error && local.Port() != 0);
            // 服务端 accept 得到的 peer 就是客户端本地地址（同一 ToString 字节）。
            expected_sub.push_back(static_cast<std::size_t>(
                bbt::infra::net::Crc32Checksum(local.ToString())) % 2);
            const std::string payload = "hash-" + std::to_string(index);
            SendAll(client, payload);
            CHECK(ReceiveAtMost(client, payload.size(), 5s) == payload);   // OnOpen/OnTraffic 已完成
            std::size_t expected_id = 0;                                   // 该 sub 上此前接纳数
            for (std::size_t previous = 0; previous < index; ++previous) {
                if (expected_sub[previous] == expected_sub[index]) { ++expected_id; }
            }
            std::size_t observed_id = 0;
            std::size_t found = 0;
            for (const auto& record : handler->Records()) {
                if (record.peer_port == local.Port()) { observed_id = record.id; ++found; }
            }
            CHECK(found == 1);                                             // 该对端端口唯一对应一条记录
            CHECK(observed_id == expected_id);                             // 关键接线断言
            peer_ports.push_back(local.Port());
            clients.push_back(std::move(client));
        }
        auto sorted_ports = peer_ports;
        std::sort(sorted_ports.begin(), sorted_ports.end());
        CHECK(std::unique(sorted_ports.begin(), sorted_ports.end()) == sorted_ports.end());
        std::cout << "hash_placement: expected_sub=";
        for (const auto sub : expected_sub) { std::cout << sub; }
        std::cout << " peer_ports=";
        for (const auto port : peer_ports) { std::cout << port << ' '; }
        std::cout << std::endl;
        CHECK(engine.CountConnections() == kConnections);
        CHECK(handler->write_errors() == 0 && handler->discard_errors() == 0);
        for (auto& client : clients) { SendAll(client, "close"); }
        CHECK(WaitUntil([&] { return handler->close_count() == static_cast<int>(kConnections); }, 10s));
        CHECK(!engine.Stop());
        CHECK(engine.CountConnections() == 0);
        clients.clear();   // 本用例自有客户端先销毁，FD 基线只反映产品回落
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
    std::cout << "lb_source_addr_hash: ok" << std::endl;
    return 0;
}

// ---- case 19：accept→LB 选中的 sub owner 命令队列满（有界重试后明确拒绝）----
// 契约依据：ServerEngine::HandoffOwner「仅对 SubmitStatus::Full 做 kHandoffAttempts 有界重试；
// 未接纳 ⇒ handoff_rejected_++，fd 随 socket 析构物理关闭，不发假接纳」+ EventLoop 真实容量。
// 判别力：拒绝路径若泄漏 fd、卡住 accept 循环、把未接纳的连接计入活跃数，或健康 sub 被饱和
// sub 拖死，第 3–5 步任一断言即红。
// 白盒说明：填满 sub owner 命令队列需要测试私有探针（既有 `#define private public` 范式）；
// 被验证的 accept→LB→移交逻辑与公共 Engine::Run 路径同一实现，连接全部真实 TCP 动态端口。
int CaseAcceptHandoffRejectedBounded() {
    WarmRuntime();
    auto handler = std::make_shared<TestHandler>();
    handler->SetClosePayload("close");
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        std::error_code error;
        auto server = bbt::infra::net::ServerEngine::Create(handler, error);
        CHECK(server && !error);
        CHECK(!server->Run(Loopback(0), TwoLoopOptions()));
        CHECK(server->subs_.size() == 2);
        CHECK(!server->LocalAddresses(addresses));
        auto& victim = *server->subs_[0].loop;
        // 1) 占住 sub0 owner：绕过 runtime Hook 的原生睡眠循环，握手 entered 后才继续。
        auto gate = std::make_shared<CloseGate>();
        CHECK(victim.Submit([gate] {
            gate->entered.store(true);
            const auto deadline = Clock::now() + 20s;
            while (!gate->released.load() && Clock::now() < deadline) { RawNanosleep(5); }
            gate->done.store(true);
        }) == bbt::infra::net::SubmitStatus::Accepted);
        CHECK(WaitUntil([&] { return gate->entered.load(); }, 5s));
        // 2) 填满 sub0 命令队列直到 Full（容量由 EventLoop 自身决定，不假设具体数值）。
        std::size_t filled = 0;
        while (victim.Submit([] {}) == bbt::infra::net::SubmitStatus::Accepted) {
            ++filled;
            CHECK(filled <= 4096);
        }
        CHECK(filled > 0);
        CHECK(!gate->released.load());
        std::cout << "handoff_fill: filled=" << filled << " (queue full reached)" << std::endl;
        const auto rejected_before = server->handoff_rejected_.load();
        // 3) 连接 1（RoundRobin → sub0）：8 次有界重试仍 Full ⇒ 明确拒绝，
        //    对端观察到物理关闭，未发生接纳（无 OnOpen、无活跃计数、registry 不增）。
        auto rejected_client = ConnectTo(addresses[0]);
        CHECK(WaitUntil([&] { return server->handoff_rejected_.load() == rejected_before + 1; }, 8s));
        CHECK(PeerClosed(rejected_client, 5s));
        CHECK(handler->open_count() == 0);
        CHECK(server->CountConnections() == 0);
        CHECK(server->subs_[0].bridge->SessionCount() == 0 && server->subs_[1].bridge->SessionCount() == 0);
        // 4) sub0 仍被占住：连接 2（RoundRobin → sub1）真实回显 ⇒ accept 循环未停、
        //    健康 sub 未被饱和 sub 拖死。
        auto healthy = ConnectTo(addresses[0]);
        const std::string healthy_payload = "healthy-echo";
        SendAll(healthy, healthy_payload);
        CHECK(ReceiveAtMost(healthy, healthy_payload.size(), 8s) == healthy_payload);
        CHECK(!gate->released.load());
        CHECK(server->subs_[1].bridge->SessionCount() == 1);
        CHECK(server->subs_[0].bridge->SessionCount() == 0);
        CHECK(server->CountConnections() == 1);
        // 5) 放行 sub0；其队列排空后连接 3（RoundRobin → sub0）恢复真实服务。
        gate->released.store(true);
        CHECK(WaitUntil([&] { return victim.Submit([] {}) == bbt::infra::net::SubmitStatus::Accepted; }, 10s));
        auto recovered = ConnectTo(addresses[0]);
        const std::string recovered_payload = "recovered-echo";
        SendAll(recovered, recovered_payload);
        CHECK(ReceiveAtMost(recovered, recovered_payload.size(), 8s) == recovered_payload);
        CHECK(server->subs_[0].bridge->SessionCount() == 1);
        CHECK(server->CountConnections() == 2);
        CHECK(server->handoff_rejected_.load() == rejected_before + 1);   // 无额外拒绝
        // 6) 收口：关闭客户端 → 计数回落；Stop；线程/FD 回落。
        healthy.Close();
        recovered.Close();
        rejected_client.Close();
        CHECK(WaitUntil([&] { return server->CountConnections() == 0; }, 10s));
        CHECK(!server->Stop());
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
    std::cout << "accept_handoff_rejected_bounded: ok" << std::endl;
    return 0;
}

// ---- case 20：公共 Co 业务面的有界待发真拒绝（超总量预算）----
// 契约依据：ICoConnection::Write「owner 队列满 ⇒ no_buffer_space / resource_unavailable_try_again；
// 成功 = 命令被接纳 ≠ 对端收到」+ Options 每方向总量预算（max_outbound_bytes）。
// 判别力：若超预算被部分接纳或静默截断，客户端字节数/内容断言即红；若拒绝后连接不可用，
// 预算内第二次写与回显即红。
class BoundedWriteHandler final : public EventHandler {
public:
    static constexpr std::size_t kOversizeBytes = 8192;   // > max_outbound_bytes(2048)
    static constexpr std::size_t kSmallBytes = 256;       // <= 预算

    Action OnTraffic(ICoConnection& connection, bool wake) override
    {
        if (wake) { return Action::None; }
        const std::string data{connection.Peek()};
        if (data.empty()) { return Action::None; }
        if (!attempted_.exchange(true)) {
            oversize_error_.store(connection.Write(std::string(kOversizeBytes, 'O'), Clock::now() + 5s).value());
            small_error_.store(connection.Write(std::string(kSmallBytes, 's'), Clock::now() + 5s).value());
            done_.store(true);
        }
        (void)connection.Discard(data.size());
        return Action::None;
    }

    void OnClose(ICoConnection&, const std::error_code&) override { close_calls_.fetch_add(1); }

    bool done() const { return done_.load(); }
    int oversize_error() const { return oversize_error_.load(); }   // error_code::value()，-1 未执行
    int small_error() const { return small_error_.load(); }
    int close_calls() const { return close_calls_.load(); }

private:
    std::atomic<bool> attempted_{false};
    std::atomic<bool> done_{false};
    std::atomic<int> oversize_error_{-1};
    std::atomic<int> small_error_{-1};
    std::atomic<int> close_calls_{0};
};

int CaseBoundedWriteReject() {
    WarmRuntime();
    auto handler = std::make_shared<BoundedWriteHandler>();
    const int threads_before = ThreadCount();
    const int fds_before = FdCount();
    std::vector<Address> addresses;
    {
        Engine engine(handler);
        Options options;
        options.num_event_loop = 2;
        options.max_outbound_bytes = 2048;
        options.write_buffer_cap = 1024;          // <= max_outbound_bytes（归一化要求）
        CHECK(!engine.Run(Loopback(0), options));
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1 && addresses[0].Port() != 0);
        auto client = ConnectTo(addresses[0]);
        SendAll(client, "go");
        CHECK(WaitUntil([&] { return handler->done(); }, 8s));
        CHECK(handler->oversize_error() == static_cast<int>(std::errc::no_buffer_space));  // 真拒绝，非部分接纳
        CHECK(handler->small_error() == 0);                                                // 预算内写被接纳
        // 超预算写的字节一个都不得到达对端：有界窗口内可读总量恰为 256 且全为 's'。
        const std::string received = ReceiveAtMost(client, BoundedWriteHandler::kSmallBytes, 5s);
        CHECK(received.size() == BoundedWriteHandler::kSmallBytes);
        CHECK(received == std::string(BoundedWriteHandler::kSmallBytes, 's'));
        std::string extra;
        for (int attempt = 0; attempt < 25; ++attempt) {
            extra += ReceiveAtMost(client, 1, 20ms);
        }
        CHECK(extra.empty());                                             // 无迟到/残余字节
        CHECK(engine.CountConnections() == 1);
        client.Close();
        CHECK(WaitUntil([&] { return handler->close_calls() == 1; }, 10s));  // OnClose 恰一次
        CHECK(WaitUntil([&] { return engine.CountConnections() == 0; }, 10s));
        CHECK(!engine.Stop());
    }
    CHECK(WaitUntil([&] { return ThreadCount() == threads_before; }, 10s));
    CHECK(WaitUntil([&] { return FdCount() == fds_before; }, 10s));
    std::cout << "bounded_write_reject: ok" << std::endl;
    return 0;
}

int RunCase(const std::string& name) {
    if (name == "multiloop_owner_identity") { return CaseMultiLoopOwnerIdentity(); }
    if (name == "lb_source_addr_hash") { return CaseLoadBalancingSourceAddrHash(); }
    if (name == "accept_handoff_rejected_bounded") { return CaseAcceptHandoffRejectedBounded(); }
    if (name == "bounded_write_reject") { return CaseBoundedWriteReject(); }
    if (name == "unsupported_family_reject") { return CaseUnsupportedFamilyReject(); }
    if (name == "unsupported_family_error_precedence") { return CaseUnsupportedFamilyErrorPrecedence(); }
    if (name == "least_connections_closed_registry") { return CaseLeastConnectionsClosedRegistry(); }
    if (name == "boot_observation") { return CaseBootObservation(); }
    if (name == "stop_during_boot") { return CaseStopDuringBoot(); }
    if (name == "stop_before_run_and_contention") { return CaseStopBeforeRunAndContention(); }
    if (name == "boot_pending_abandon") { return CaseBootPendingAbandon(); }
    if (name == "boot_crossgen_reentry_busy") { return CaseBootCrossGenReentryBusy(); }
    if (name == "roundtrip") { return CaseRoundtrip(); }
    if (name == "concurrent_hold") { return CaseConcurrentHold(); }
    if (name == "stop_with_suspended_handler") { return CaseStopWithSuspendedHandler(); }
    if (name == "negative_lifecycle") { return CaseNegativeLifecycle(); }
    if (name == "managed_reject") { return CaseManagedReject(); }
    if (name == "onclose_throw_count") { return CaseOnCloseThrowCount(); }
    if (name == "onclose_suspended_stop_destroy") { return CaseOnCloseSuspendedStopDestroy(); }
    if (name == "null_handler") { return CaseNullHandler(); }
    if (name == "handler_owned_across_destroy") { return CaseHandlerOwnedAcrossDestroy(); }
    if (name == "boot_suspended_timeout_destroy") { return CaseBootSuspendedTimeoutDestroy(); }
    std::cerr << "unknown case: " << name << std::endl;
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "usage: Test_net_engine_server <case>" << std::endl;
        return 2;
    }
    const std::string name = argv[1];
    return bbt_net_test::RunTestProcessExit([&name] { return RunCase(name); });
}
