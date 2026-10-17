// P5 推荐协程网络用法示例（只消费公开 API）：真实 TCP server(Engine) + client(Client)。
//
// 适用：TCP/Unix stream（本例只演示 TCP 回环）。不适用 UDP：数据报保留报文边界，
// 不套 stream 分帧/半包语义。
//
// 用法（一个示例多个模式，模式即已接受设计里的使用路径）：
//   Example_co_net_service lifecycle   启动/注册/顺序请求响应/分帧/view/Wake/Close 双模式/收口
//   Example_co_net_service concurrent  一个 Handler 挂起时另一连接仍进展 + 同连接串行
//   Example_co_net_service limits      有界配置下的可观察返回（背压/错误/deadline），不虚报命中
//   Example_co_net_service errors      受控失败模式：预期失败语义命中时以退出码 3 结束（非零）
//
// 库负责：owner/线程/上下文、事件串行、跨域命令的域门禁、命令接纳与物理关闭的分界、
//         挂起不占执行线程（受管协程）。业务负责：帧格式与请求配对、幂等、
//         等待登记（先登记后投递）、把 Peek 视图在挂起点之前复制为自有数据。
//
// 退出约定：本示例进程内不提供 runtime 停机（既有模型无优雅停机）；业务对象按序收口后
// 显式 flush 并以 std::_Exit 结束进程，不展开栈、不代表优雅停机，也不作为库资源回收证据。
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <dirent.h>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Client.hpp>
#include <bbt/infra/net/CoConnectionHandler.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/EventHandler.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/Options.hpp>

namespace net = bbt::infra::net;
using bbt::coroutine::WaitOptions;
using bbt::coroutine::WaitStatus;
using bbt::coroutine::sync::CoWaiter;
using namespace std::chrono_literals;

static int g_fail = 0;
#define CHECK(cond) do { if (!(cond)) { \
    std::cerr << "CHECK failed: " #cond " @" << __LINE__ << '\n'; ++g_fail; } } while (0)

static std::error_code MakeErr(std::errc value) { return std::make_error_code(value); }
static bool IsError(const std::error_code& error, std::errc expected) { return error == MakeErr(expected); }
static net::CoDeadline After(std::chrono::milliseconds ms) {
    return std::chrono::steady_clock::now() + ms;
}

// ---- 外层（harness）超时的独立错误值：绝不等价于库内 std::errc::timed_out， ----
// ---- 否则「业务协程根本没返回」会被当成 deadline 命中而假绿。 ----
class HarnessTimeoutCategory final : public std::error_category {
public:
    const char* name() const noexcept override { return "bbt.infra.net.example.harness_timeout"; }
    std::string message(int) const override { return "example harness outer timeout"; }
};
static const std::error_category& HarnessTimeoutCategoryRef() {
    static const HarnessTimeoutCategory category;
    return category;
}
static std::error_code HarnessTimedOut() { return {7, HarnessTimeoutCategoryRef()}; }

// 受管协程执行器：把业务闭包登记到 runtime，外层只做有界等待。
template <typename Fn>
static std::error_code RunInCoroutine(Fn&& fn, const char* name, std::chrono::seconds timeout) {
    auto promise = std::make_shared<std::promise<std::error_code>>();
    auto done = promise->get_future();
    g_scheduler->RegistCoroutineTask(131072,
        [promise, fn = std::forward<Fn>(fn)]() mutable {
            try { promise->set_value(fn()); }
            catch (...) { promise->set_exception(std::current_exception()); }
        }, name);
    if (done.wait_for(timeout) != std::future_status::ready) { return HarnessTimedOut(); }
    return done.get();
}

// ---- 地址/端口/资源探针（公开 NativeSocket，与已接受公共 consumer 同口径） ----
static net::Address Loopback(int port) {
    std::error_code error;
    const auto address = net::Address::Ip("127.0.0.1", port, error);
    if (error || !address.Valid()) { std::cerr << "bad loopback address\n"; std::abort(); }
    return address;
}

// 申请一个当前无监听者的回环端口（bind :0 后立即关闭）：用于「连接被拒绝」正例。
static int ClosedPort() {
    std::error_code error;
    auto probe = net::NativeSocket::Open(net::AddressFamily::IPv4, net::SocketType::Stream, error);
    if (error) { std::abort(); }
    if (probe.Bind(Loopback(0))) { std::abort(); }
    const auto local = probe.LocalAddress(error);
    (void)probe.Close();
    return local.Port();
}

// backlog=1 且从不 accept：Linux 语义下最多容纳 1 条已完成握手的连接，
// 后续 connect 停在真实未决窗口（不靠时间猜测阶段）。
static net::NativeSocket ListenBacklog1(int& port) {
    std::error_code error;
    auto socket = net::NativeSocket::Open(net::AddressFamily::IPv4, net::SocketType::Stream, error);
    if (error) { std::abort(); }
    if (socket.Bind(Loopback(0))) { std::abort(); }
    if (socket.Listen(1)) { std::abort(); }
    const auto bound = socket.LocalAddress(error);
    if (error) { std::abort(); }
    port = bound.Port();
    return socket;
}

static std::size_t CountFds() {
    DIR* dir = ::opendir("/proc/self/fd");
    if (dir == nullptr) { return 0; }
    std::size_t count = 0;
    while (auto* entry = ::readdir(dir)) {
        const std::string name{entry->d_name};
        if (name != "." && name != "..") { ++count; }
    }
    ::closedir(dir);
    return count;
}

static bool WaitFdReturn(std::size_t baseline, int tries = 1000) {
    for (int i = 0; i < tries; ++i) {
        if (CountFds() <= baseline) { return true; }
        std::this_thread::sleep_for(10ms);
    }
    return false;
}

// ---- 应用层帧：4 字节大端长度前缀 + 载荷（分帧属于应用约定，库只保证字节流与事件） ----
static std::string Frame(std::string_view payload) {
    std::string out(4, '\0');
    const auto size = static_cast<std::uint32_t>(payload.size());
    out[0] = static_cast<char>((size >> 24) & 0xFFu);
    out[1] = static_cast<char>((size >> 16) & 0xFFu);
    out[2] = static_cast<char>((size >> 8) & 0xFFu);
    out[3] = static_cast<char>(size & 0xFFu);
    out.append(payload.data(), payload.size());
    return out;
}

// 增量分帧解析：未消费尾部保序保留；只有整帧到达才产出。
struct FrameParser {
    std::string tail;
    std::vector<std::string> frames;

    void Feed(std::string_view bytes) {
        tail.append(bytes.data(), bytes.size());
        while (tail.size() >= 4u) {
            const auto size =
                (static_cast<std::uint32_t>(static_cast<unsigned char>(tail[0])) << 24) |
                (static_cast<std::uint32_t>(static_cast<unsigned char>(tail[1])) << 16) |
                (static_cast<std::uint32_t>(static_cast<unsigned char>(tail[2])) << 8) |
                 static_cast<std::uint32_t>(static_cast<unsigned char>(tail[3]));
            if (tail.size() < 4u + size) { break; }
            frames.emplace_back(tail.substr(4, size));
            tail.erase(0, 4 + size);
        }
    }
};

// ---- 客户端业务 handler：一个实例服务本 Client 的所有连接，按连接 id 分桶 ----
//
// 规范点（都在这里落地）：
//  * OnTraffic 的 Peek 视图只在本次事件内有效——进入任何挂起点之前复制为自有数据；
//  * 未消费尾部由库保留，因此这里只在确实消费时 Discard；
//  * Wake-only 事件的 Reader 契约是「空」：不借出快照；
//  * 唤醒业务协程用公开 CoWaiter（handler 从快照复制后 Notify）。
// 逐连接事件序号/峰值用于观测「同连接串行、峰值 1」。
class ClientObserver final : public net::ICoConnectionHandler {
public:
    void OnOpen(net::ICoConnection& connection) override {
        std::lock_guard<std::mutex> lock(m_mutex);
        ++m_opens;
        m_events[connection.GetObjectInfo().id];   // 公开身份：进程内唯一、永不复用
    }

    void OnTraffic(net::ICoConnection& connection, bool wake) override {
        const std::size_t id = connection.GetObjectInfo().id;
        std::shared_ptr<CoWaiter> waiter;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            ++m_events[id];
            ++m_active[id];
            if (m_active[id] > m_peak[id]) { m_peak[id] = m_active[id]; }
            if (wake) {
                ++m_wake_events;
                if (connection.InboundBuffered() != 0 || !connection.Peek().empty()) { ++m_wake_reader_dirty; }
            } else {
                const auto view = connection.Peek();
                m_rx[id].append(view.data(), view.size());   // 复制边界：挂起前搬走字节
                (void)connection.Discard(view.size());
            }
            --m_active[id];
            const auto it = m_waiters.find(id);
            if (it != m_waiters.end()) { waiter = it->second; }
        }
        // 通知放在解锁之后；等待方的「登记 + 复查已到字节」与这里的「追加字节 + 取本连接等待者」
        // 都在同一把锁内，二者必有先后可见性，因此不会出现丢唤醒窗口（见 ArmAndCheck 注释）。
        if (waiter) { (void)waiter->Notify(); }   // 无等待者返回 -1，不丢数据（数据已在 m_rx）
    }

    void OnClose(net::ICoConnection& connection, const std::error_code&) override {
        std::lock_guard<std::mutex> lock(m_mutex);
        ++m_closes;
        m_closed.insert(connection.GetObjectInfo().id);
    }

    // ---- 业务协程侧访问器：短临界区，持锁不挂起 ----
    // 等待责任按 connection id 明确归属：只唤醒「正在等这条连接新字节」的等待者，
    // 其它连接的数据事件不会误唤醒（不是全局单一等待位）。
    // ArmAndCheck 在同一把锁内完成「登记等待者 + 复查已到字节」；OnTraffic 的
    // 「追加字节 + 读取本连接等待者」用同一把锁，二者必有一个先看到对方，
    // 因此不存在「检查-订阅」窗口。返回 true 表示登记时字节已到，调用方应立即 Notify。
    // 上限（示例范围内，不新增抽象）：m_waiters 每条连接只有**一个**槽位，ConditionLatch 的
    // m_waiter 同为单槽；本例每条连接同一时刻只有一条等待链路，故未触发。若同一 handler 内
    // 多个协程同时等同一条连接的新字节，后到者会覆盖先到者，先到者只能等到自己的 deadline
    // （记超时、不假绿，但会停等）。
    bool ArmAndCheck(const std::shared_ptr<CoWaiter>& waiter, std::size_t id,
                     std::size_t base, std::size_t want) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_waiters[id] = waiter;
        return m_rx[id].size() - base >= want;
    }
    void Disarm(const std::shared_ptr<CoWaiter>& waiter, std::size_t id) {
        std::lock_guard<std::mutex> lock(m_mutex);
        const auto it = m_waiters.find(id);
        if (it != m_waiters.end() && it->second == waiter) { m_waiters.erase(it); }
    }
    std::size_t RxSize(std::size_t id) {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_rx[id].size();
    }
    std::string RxRange(std::size_t id, std::size_t from) {
        std::lock_guard<std::mutex> lock(m_mutex);
        const auto& buffer = m_rx[id];
        return from >= buffer.size() ? std::string{} : buffer.substr(from);
    }
    int Opens() { std::lock_guard<std::mutex> lock(m_mutex); return m_opens; }
    int Closes() { std::lock_guard<std::mutex> lock(m_mutex); return m_closes; }
    int WakeEvents() { std::lock_guard<std::mutex> lock(m_mutex); return m_wake_events; }
    int WakeReaderDirty() { std::lock_guard<std::mutex> lock(m_mutex); return m_wake_reader_dirty; }
    int ObservedConns() { std::lock_guard<std::mutex> lock(m_mutex); return static_cast<int>(m_events.size()); }
    long long Events(std::size_t id) { std::lock_guard<std::mutex> lock(m_mutex); return m_events[id]; }
    int Peak(std::size_t id) { std::lock_guard<std::mutex> lock(m_mutex); return m_peak[id]; }
    int PeakMax() {
        std::lock_guard<std::mutex> lock(m_mutex);
        int peak = 0;
        for (const auto& entry : m_peak) { if (entry.second > peak) { peak = entry.second; } }
        return peak;
    }

private:
    std::mutex m_mutex;
    /* 等待责任按连接 id 归属：只有「正在等这条连接新字节」的等待者会被本连接事件唤醒。 */
    std::map<std::size_t, std::shared_ptr<CoWaiter>> m_waiters;
    std::map<std::size_t, std::string> m_rx;
    std::map<std::size_t, long long> m_events;
    std::map<std::size_t, int> m_active;
    std::map<std::size_t, int> m_peak;
    std::set<std::size_t> m_closed;
    int m_opens{0};
    int m_closes{0};
    int m_wake_events{0};
    int m_wake_reader_dirty{0};
};

// ---- 受管协程内：等 id 连接累计新字节到 want ----
// 等待口径（examples-review-08 实施前置 1；examples-review-110 P1/P2-4 收口）：
//  * 用公开 CoWaiter::WaitWithCallback：只有登记成功（协程已挂上等待事件）之后才执行
//    on_registered；回调里做两件**不挂起**的事：
//      (1) 在同一把 observer 锁内把本连接的等待者登记进去；
//      (2) 同锁复查已到字节——若响应在本协程登记之前就已到达，立即 Notify()
//          走 PENDING 早到路径唤醒自己，不丢唤醒。
//    OnTraffic 的「追加字节 + 取本连接等待者」用同一把锁，二者必有先后可见性，
//    因此不存在「检查-订阅」窗口，也就不需要短期限切片轮询兜底。
//  * 完整等待调用方给定 deadline，不用 200ms 切片；取消/错误不吞成重试。
//  * on_registered 内不做任何可挂起调用（不投递 Write/Wake）；请求由调用方先行投递，
//    Write 只等 owner 接纳，响应早到由 (2) 的复查兜住。
static std::error_code AwaitBytes(net::ICoConnection& connection, ClientObserver& observer,
    std::size_t base, std::size_t want, std::string& out, net::CoDeadline deadline) {
    const std::size_t id = connection.GetObjectInfo().id;
    while (observer.RxSize(id) - base < want) {
        if (std::chrono::steady_clock::now() >= deadline) { return MakeErr(std::errc::timed_out); }
        auto waiter = CoWaiter::Create();
        const auto status = waiter->WaitWithCallback(WaitOptions{deadline},
            [&observer, &waiter, id, base, want]() {
                // 登记成功回调（CoroutineOnYieldCallback 要求返回 bool）：只做短锁下的
                // 登记与复查，不挂起、不投递可挂起命令。
                if (observer.ArmAndCheck(waiter, id, base, want)) { (void)waiter->Notify(); }
                return true;
            }, nullptr);
        observer.Disarm(waiter, id);
        if (status == WaitStatus::Completed) { continue; }   // 回到循环顶按字节数复查
        if (status == WaitStatus::TimedOut) {
            if (observer.RxSize(id) - base >= want) { continue; }   // 超时与到达竞争：以字节为准
            return MakeErr(std::errc::timed_out);
        }
        if (status == WaitStatus::Cancelled) { return MakeErr(std::errc::operation_canceled); }
        if (status == WaitStatus::AlreadyWaiting) { return MakeErr(std::errc::device_or_resource_busy); }
        if (status == WaitStatus::InvalidContext) { return MakeErr(std::errc::operation_not_permitted); }
        // RuntimeUnavailable：事件登记失败，未真正挂起，按错误返回而不是停等到 deadline
        return MakeErr(std::errc::resource_unavailable_try_again);
    }
    out = observer.RxRange(id, base);
    return {};
}

// 一轮请求/响应：先投递 req（只等 owner 接纳），再按字节数有界等待对端回显。
static std::error_code Exchange(net::ICoConnection& connection, ClientObserver& observer,
    const std::string& req, std::string& out, net::CoDeadline deadline) {
    const std::size_t base = observer.RxSize(connection.GetObjectInfo().id);
    if (const auto error = connection.Write(req, deadline)) { return error; }
    return AwaitBytes(connection, observer, base, req.size(), out, deadline);
}

// ---- server 端 handler ----
// 回显：Peek 快照 → 复制 → Discard 消费 → Write(副本)。挂起（Write）之前不再触碰视图。
class EchoHandler final : public net::EventHandler {
public:
    net::Action OnOpen(net::ICoConnection&) override { opens.fetch_add(1); return net::Action::None; }

    net::Action OnTraffic(net::ICoConnection& connection, bool wake) override {
        if (wake) { return net::Action::None; }
        const auto view = connection.Peek();
        std::string copy(view);                 // 复制边界：字节搬进自有缓冲
        (void)connection.Discard(view.size());  // 消费当前快照（未消费尾部本可保留）
        if (!copy.empty()) {
            const auto error = connection.Write(copy, After(2000ms));
            if (error) { ++write_errors; } else { echoed.fetch_add(copy.size()); }
        }
        return net::Action::None;               // 视图在此之后不再被使用
    }

    void OnClose(net::ICoConnection&, const std::error_code&) override { closes.fetch_add(1); }

    std::atomic<int> opens{0};
    std::atomic<int> closes{0};
    std::atomic<int> write_errors{0};
    std::atomic<long long> echoed{0};
};

// ---- handler 内的「条件事件」等待位（最小结构，不造泛化框架）----
// 与 ClientObserver::ArmAndCheck 同一口径：Wait() 的「登记 + 复查条件」与 Signal()
// 在同 一把锁内，登记成功之后才返回来等；若条件在登记之前已成立，on_registered 里
// 复查到就立即 Notify（PENDING 早到路径），通知方不会丢唤醒。handler 回调本身是
// 受管协程，可直接在此挂起等待，不占用执行线程，也不用 yield 自旋。
class ConditionLatch {
public:
    // 在受管协程内挂起等条件成立或 deadline 到期；返回条件是否（曾）成立。
    bool Wait(net::CoDeadline deadline) {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_signaled) { return true; }
        auto waiter = CoWaiter::Create();
        m_waiter = waiter;
        lock.unlock();                      // 挂起期间绝不持锁
        const auto status = waiter->WaitWithCallback(WaitOptions{deadline}, [this, waiter]() {
            std::lock_guard<std::mutex> inner(m_mutex);
            if (m_signaled) { (void)waiter->Notify(); }
            return true;
        }, nullptr);
        lock.lock();
        if (m_waiter == waiter) { m_waiter.reset(); }
        return m_signaled || status == WaitStatus::Completed;
    }
    // 通知方：条件成立并唤醒等待者（等待者尚未登记时由它登记的复查兜住）。
    void Signal() {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_signaled = true;
        if (m_waiter) { (void)m_waiter->Notify(); }
    }
private:
    std::mutex m_mutex;
    std::shared_ptr<CoWaiter> m_waiter;
    bool m_signaled{false};
};

// 慢对端：首个数据事件里先不读，在受管协程内挂起等业务侧 Release()（有界 5s deadline），
// 不是 yield 自旋；通知方 Release() 与登记复查同锁，不丢唤醒。
class SlowPeerHandler final : public net::EventHandler {
public:
    net::Action OnTraffic(net::ICoConnection& connection, bool wake) override {
        if (wake) { return net::Action::None; }
        if (!m_latch.Wait(After(5000ms))) { ++wait_timeouts; }
        ++holds;
        const auto view = connection.Peek();
        std::string copy(view);
        (void)connection.Discard(view.size());
        if (!copy.empty()) {
            const auto error = connection.Write(copy, After(2000ms));
            if (error) { ++write_errors; } else { echoed.fetch_add(copy.size()); }
        }
        return net::Action::None;
    }

    void Release() { m_latch.Signal(); }   // 通知方：条件成立并唤醒对端 handler 的挂起等待

    std::atomic<long long> echoed{0};
    std::atomic<int> holds{0};
    std::atomic<int> wait_timeouts{0};
    std::atomic<int> write_errors{0};

private:
    ConditionLatch m_latch;
};

// 并发 gate：A 的第一个事件在受管协程内挂起，直到 B 的回显字节完成（由内容区分 A/B，
// 不依赖到达顺序）；B 完成回显后 Signal（同锁通知，A 不丢唤醒）。不用 yield 自旋。
class GateHandler final : public net::EventHandler {
public:
    net::Action OnTraffic(net::ICoConnection& connection, bool wake) override {
        if (wake) { return net::Action::None; }
        const auto view = connection.Peek();
        std::string copy(view);
        (void)connection.Discard(view.size());
        if (copy.empty()) { return net::Action::None; }
        const bool is_a = copy.compare(0, 6, "A-hold") == 0;
        if (!is_a) {
            b_echoed.fetch_add(copy.size());
            m_gate.Signal();                       // 通知方：B 回显完成
        } else if (m_gate.Wait(After(5000ms))) {
            if (b_echoed.load() > 0) { gate_released_after_b.fetch_add(1); }
        } else {
            ++a_wait_timeouts;
        }
        const auto error = connection.Write(copy, After(2000ms));
        if (!error) {
            if (is_a) { a_echoed.fetch_add(copy.size()); }
        }
        return net::Action::None;
    }

    std::atomic<long long> b_echoed{0};
    std::atomic<long long> a_echoed{0};
    std::atomic<int> gate_released_after_b{0};
    std::atomic<int> a_wait_timeouts{0};
    std::atomic<int> opens{0};

private:
    ConditionLatch m_gate;
};

// =============================== lifecycle ===============================
static int ModeLifecycle() {
    const std::size_t fd_baseline = CountFds();

    // 1) Options 归一化自检（纯值校验，无副作用）：正例 + 负值拒绝。
    net::Options options;
    options.num_event_loop = 2;
    options.tcp_no_delay = true;
    options.read_buffer_cap = 16384;
    options.write_buffer_cap = 16384;
    options.max_outbound_bytes = 1 << 20;
    options.max_pending_events = 1024;
    net::NormalizedOptions normalized;
    CHECK(!net::NormalizeOptions(options, static_cast<int>(std::thread::hardware_concurrency()), normalized));
    CHECK(normalized.num_event_loop == 2);
    net::Options rejected = options;
    rejected.num_event_loop = -1;
    CHECK(static_cast<bool>(net::NormalizeOptions(rejected, 4, normalized)));   // 负值拒绝（返回错误）

    // 2) server Engine（port 0 由内核分配，替换旧设计的端口探测法）+ 客户端 Client
    auto echo = std::make_shared<EchoHandler>();
    net::Engine server(echo);
    if (const auto error = server.Run(Loopback(0), options)) {
        std::cerr << "engine run failed: " << error.message() << '\n';
        return 1;
    }
    std::vector<net::Address> bound;
    if (const auto error = server.LocalAddresses(bound); error || bound.empty()) {
        std::cerr << "no bound address\n";
        (void)server.Stop();
        return 1;
    }
    const int port = bound.front().Port();

    // Client 是最小单 owner 形态：归一化 loop 数必须为 1（显式 1 或未指定），
    // 未接线选项同样显式拒绝（不静默忽略），故用独立的 client options。
    net::Options client_options = options;
    client_options.num_event_loop = 1;
    auto observer = std::make_shared<ClientObserver>();
    net::Client client(observer);
    if (const auto error = client.Start(client_options)) {
        std::cerr << "client start failed: " << error.message() << '\n';
        (void)server.Stop();
        return 1;
    }

    std::atomic<int> roundtrips{0};
    std::atomic<int> frames{0};
    std::atomic<int> closes_ok{0};
    std::atomic<int> unblock_closes{0};
    std::atomic<int> idempotent_closes{0};
    std::atomic<int> late_write_value{0};
    std::atomic<int> late_write_unsupported{0};

    const auto rc = RunInCoroutine([&]() -> std::error_code {
        std::shared_ptr<net::CoTcpConnection> first;
        std::shared_ptr<net::CoTcpConnection> second;
        if (const auto error = client.Dial(Loopback(port), first, After(5000ms))) { return error; }
        if (first == nullptr) { return MakeErr(std::errc::bad_file_descriptor); }
        CHECK(first->PeerAddress().Port() == port);

        // 顺序请求/响应：直线代码，库内挂起等 owner 接纳 + 业务等待对端回显
        for (int i = 0; i < 8; ++i) {
            const std::string request = Frame("round-" + std::to_string(i));
            std::string response;
            const auto error = Exchange(*first, *observer, request, response, After(3000ms));
            CHECK(!error);
            if (error) { std::cerr << "roundtrip[" << i << "] failed: " << error.message() << '\n'; return error; }
            CHECK(response == request);
            if (response == request) { roundtrips.fetch_add(1); }
        }

        // 分帧：粘包（一次 Write 两帧）+ 半包（一帧拆两次 Write）+ 尾帧保留
        FrameParser parser;
        std::string out;
        if (const auto error = Exchange(*first, *observer,
                Frame("alpha") + Frame("beta"), out, After(3000ms))) { return error; }
        parser.Feed(out);
        CHECK(parser.frames.size() == 2 && parser.tail.empty());

        const std::string gamma = Frame("gamma");
        if (const auto error = Exchange(*first, *observer,
                gamma.substr(0, gamma.size() / 2), out, After(3000ms))) { return error; }
        parser.Feed(out);
        CHECK(parser.frames.size() == 2);                       // 半帧不产出
        CHECK(!parser.tail.empty());                            // 未消费尾部保序保留

        if (const auto error = Exchange(*first, *observer,
                gamma.substr(gamma.size() / 2), out, After(3000ms))) { return error; }
        parser.Feed(out);
        CHECK(parser.frames.size() == 3 && parser.tail.empty());

        const std::string delta = Frame("delta");
        if (const auto error = Exchange(*first, *observer,
                delta.substr(0, delta.size() - 2), out, After(3000ms))) { return error; }
        parser.Feed(out);
        CHECK(parser.frames.size() == 3 && parser.tail.size() == delta.size() - 2);
        if (const auto error = Exchange(*first, *observer,
                delta.substr(delta.size() - 2), out, After(3000ms))) { return error; }
        parser.Feed(out);
        CHECK(parser.frames.size() == 4 && parser.tail.empty());
        CHECK(parser.frames[0] == "alpha" && parser.frames[1] == "beta"
            && parser.frames[2] == "gamma" && parser.frames[3] == "delta");
        frames.store(static_cast<int>(parser.frames.size()));

        // Wake：受管协程内的跨域命令，不挂起；Wake-only 事件的 Reader 观察为空
        const int wakes_before = observer->WakeEvents();
        CHECK(!first->Wake());
        for (int i = 0; i < 500 && observer->WakeEvents() == wakes_before; ++i) { bbtco_yield; }
        CHECK(observer->WakeEvents() > wakes_before);
        CHECK(observer->WakeReaderDirty() == 0);

        // 第二个连接：多连接 + Close 双模式
        if (const auto error = client.Dial(Loopback(port), second, After(5000ms))) { return error; }
        if (second == nullptr) { return MakeErr(std::errc::bad_file_descriptor); }
        std::string second_echo;
        if (const auto error = Exchange(*second, *observer, Frame("second"), second_echo, After(3000ms))) { return error; }
        CHECK(second_echo == Frame("second"));

        // Unblock：返回时已封口、已可靠投递，不等物理完成
        CHECK(!second->Close(net::CloseMode::Unblock, {}, After(3000ms)));
        unblock_closes.fetch_add(1);
        // 封口后的业务命令不得静默成功，也不能拿「任意非零」充数。公开契约
        // bbt/infra/net/ICoConnection.hpp:47-50 写明：非 Open 状态的 Write 由物理连接
        // 拒绝，映射为 operation_not_supported 等；封口/关闭路径的 Wake 才是
        // operation_canceled。这里据此把断言收紧到契约给出的「非 Open 拒绝族」，
        // 并原样记录实际错误码（本轮 errno 95 = operation_not_supported）。
        const auto late_write = second->Write(std::string("late"), After(1000ms));
        CHECK(static_cast<bool>(late_write));
        CHECK(!IsError(late_write, std::errc::operation_not_permitted));   // 不是域拒绝
        late_write_value.store(late_write.value());
        const bool late_write_documented = IsError(late_write, std::errc::operation_not_supported)
            || IsError(late_write, std::errc::operation_canceled);
        CHECK(late_write_documented);                                     // 落在契约的拒绝族内
        CHECK(!IsError(late_write, std::errc::timed_out));                // 不是「结果未知」路径
        late_write_unsupported.store(late_write_documented ? 1 : 0);
        // 已关闭后的 Close 是幂等观察
        CHECK(!second->Close(net::CloseMode::Block, {}, After(3000ms)));
        closes_ok.fetch_add(1);
        // Block：挂起等到物理关闭完成（不等自身 OnClose）
        CHECK(!first->Close(net::CloseMode::Block, {}, After(5000ms)));
        closes_ok.fetch_add(1);
        // 幂等实测：物理关闭完成之后再次 Close(Block) 必须是成功的幂等观察
        // （不是「Unblock 后再 Block」这种升级，而是真正的已完成连接上的重复 Close）。
        CHECK(!first->Close(net::CloseMode::Block, {}, After(3000ms)));
        idempotent_closes.fetch_add(1);
        return {};
    }, "example_lifecycle", 25s);

    if (rc) { std::cerr << "lifecycle coroutine failed: " << rc.message()
                        << " (category=" << rc.category().name() << ")\n"; }

    // 收口顺序：连接 Close(Block) 已在协程内完成 → 对端 FIN 驱动的服务端自动收口
    // （服务端 OnClose 送达 + 链路级计数归零）在强停前观测 → Client::Stop → Engine::Stop → fd 回落。
    // 这里等的正是「对端 EOF 自动收口」而非强停：用**有界期限**等该可达状态出现，
    // 并把实际是否在期限内达成作为断言（不靠睡眠估计阶段，也不用常量改写输出）。
    // 边界：强停不等 handler/OnClose，本观测只覆盖不与 Stop 竞争的正常收口路径。
    const auto observe_deadline = std::chrono::steady_clock::now() + 5s;
    int opens = 0;
    int closes = 0;
    int client_closes = 0;
    int connections = -1;
    int wait_iters = 0;
    bool closed_before_stop = false;
    for (int i = 0;; ++i) {
        opens = echo->opens.load();
        closes = echo->closes.load();
        client_closes = observer->Closes();
        connections = static_cast<int>(server.CountConnections());
        wait_iters = i;
        if (opens >= 2 && client_closes >= 2 && closes >= 2 && connections == 0) {
            closed_before_stop = true;
            break;
        }
        if (std::chrono::steady_clock::now() >= observe_deadline) { break; }
        std::this_thread::sleep_for(10ms);
    }
    const int conns_before_stop = connections;
    const auto client_stop = client.Stop();
    const auto server_stop = server.Stop();
    CHECK(!client_stop);
    if (client_stop) { std::cerr << "client stop failed: " << client_stop.message() << '\n'; }
    CHECK(!server_stop);
    if (server_stop) { std::cerr << "server stop failed: " << server_stop.message() << '\n'; }
    const bool fd_return = WaitFdReturn(fd_baseline);

    CHECK(roundtrips.load() == 8);
    CHECK(frames.load() == 4);
    CHECK(closes_ok.load() == 2);
    CHECK(unblock_closes.load() == 1);                  // Unblock 关闭恰一次
    CHECK(idempotent_closes.load() == 1);               // 物理完成后再次 Close 幂等成功（实测）
    CHECK(opens == 2);                                  // 两个连接都派发了 OnOpen
    CHECK(observer->Opens() == 2);
    CHECK(client_closes == 2);                          // 客户端侧两连接的 OnClose 已串行送达
    CHECK(closed_before_stop);                          // 服务端对端 EOF 收口在 Engine::Stop 之前达成（有界期限内）
    CHECK(closes == 2);                                 // 两条连接的服务端 OnClose 均在强停前送达
    CHECK(conns_before_stop == 0);                      // 强停前链路级连接计数已自然归零
    CHECK(server.CountConnections() == 0);              // Engine 实例强停后链路级连接数归零
    CHECK(observer->PeakMax() == 1);
    CHECK(fd_return);

    std::cout << "lifecycle pass roundtrips=" << roundtrips.load()
              << " frames=" << frames.load()
              << " wake_events=" << observer->WakeEvents()
              << " wake_reader_dirty=" << observer->WakeReaderDirty()
              << " server_opens=" << opens << " server_closes=" << closes
              << " client_opens=" << observer->Opens() << " client_closes=" << client_closes
              << " observed_conns=" << observer->ObservedConns()
              << " conns_before_stop=" << conns_before_stop << " wait_iters=" << wait_iters
              << " closed_before_stop=" << (closed_before_stop ? 1 : 0)
              << " per_connection_handler_peak=" << observer->PeakMax()
              << " late_write_after_unblock_errno=" << late_write_value.load()
              << " late_write_is_operation_not_supported=" << late_write_unsupported.load()
              << " server_echoed=" << echo->echoed.load()
              << " fd_return=" << (fd_return ? 1 : 0) << '\n';
    std::cout << "lifecycle note: unblock_close=" << unblock_closes.load()
              << " block_close=" << closes_ok.load()
              << " idempotent_reclose_ok=" << idempotent_closes.load()
              << " (Close(Unblock) 只保证已封口并投递；Close(Block) 等物理完成；"
              << " 幂等为物理完成后再次 Close 的实测计数，不是常量)\n";
    if (late_write_unsupported.load()) {
        std::cout << "lifecycle note: 封口后 Write 实测 errno=" << late_write_value.load()
                  << "，落在公开契约 ICoConnection.hpp:47-50「非 Open 状态由物理连接拒绝"
                  << "（operation_not_supported 等）」的拒绝族内；示例只断言该归属，"
                  << "不把该码冒充 closed/canceled 语义。\n";
    }
    if (closed_before_stop) {
        std::cout << "lifecycle note: server_close_observed=1 — 对端 FIN 驱动的服务端 OnClose 与链路级计数归零\n"
                  << "  在 Engine::Stop 之前达成（有界观测期限 5s 未耗尽，wait_iters=" << wait_iters
                  << "）；此前候选上 server_closes=0 的观测线索在本候选上已消失，\n"
                  << "  与 docs/decisions/0010「对端 FIN/RST 收口」契约一致（不承诺残余响应送达对端，\n"
                  << "  因此推荐先读完响应再主动 Close）。该确认只覆盖与 Stop 不竞争的正常收口路径。\n";
    }
    return g_fail == 0 ? 0 : 1;
}

// =============================== concurrent ===============================
static int ModeConcurrent() {
    const std::size_t fd_baseline = CountFds();
    net::Options options;
    options.num_event_loop = 2;
    options.tcp_no_delay = true;
    options.max_pending_events = 1024;

    auto gate = std::make_shared<GateHandler>();
    net::Engine server(gate);
    if (const auto error = server.Run(Loopback(0), options)) {
        std::cerr << "engine run failed: " << error.message() << '\n';
        return 1;
    }
    std::vector<net::Address> bound;
    if (const auto error = server.LocalAddresses(bound); error || bound.empty()) {
        std::cerr << "no bound address\n"; (void)server.Stop(); return 1;
    }
    const int port = bound.front().Port();

    net::Options client_options = options;   // Client 单 owner：loop 数须为 1
    client_options.num_event_loop = 1;
    auto observer = std::make_shared<ClientObserver>();
    net::Client client(observer);
    const auto start_error = client.Start(client_options);
    if (start_error) { std::cerr << "client start failed\n"; (void)server.Stop(); return 1; }

    std::atomic<int> a_ok{0};
    std::atomic<int> b_ok{0};
    const auto rc = RunInCoroutine([&]() -> std::error_code {
        std::shared_ptr<net::CoTcpConnection> a;
        std::shared_ptr<net::CoTcpConnection> b;
        if (const auto error = client.Dial(Loopback(port), a, After(5000ms))) { return error; }
        if (const auto error = client.Dial(Loopback(port), b, After(5000ms))) { return error; }
        if (a == nullptr || b == nullptr) { return MakeErr(std::errc::bad_file_descriptor); }

        const std::string a_req = "A-hold";
        const std::size_t a_id = a->GetObjectInfo().id;
        const std::size_t a_base = observer->RxSize(a_id);
        // 仅等 owner 接纳（≠对端收到）；对端会持有 A 直到 B 的回显完成
        CHECK(!a->Write(a_req, After(3000ms)));
        const int wakes_before = observer->WakeEvents();
        CHECK(!a->Wake());
        for (int i = 0; i < 500 && observer->WakeEvents() == wakes_before; ++i) { bbtco_yield; }
        CHECK(observer->WakeEvents() > wakes_before);    // A 挂起期间 Wake 事件仍被派发（串行排队）

        // B 整轮完成（A 的响应仍被对端持有）
        std::string b_resp;
        const auto b_error = Exchange(*b, *observer, Frame("B-echo"), b_resp, After(5000ms));
        CHECK(!b_error);
        if (b_error) { return b_error; }
        if (b_resp == Frame("B-echo")) { b_ok.store(1); }

        // 再回头取 A
        std::string a_resp;
        const auto a_error = AwaitBytes(*a, *observer, a_base, a_req.size(), a_resp, After(5000ms));
        CHECK(!a_error);
        if (a_error) { return a_error; }
        CHECK(a_resp == a_req);
        if (a_resp == a_req) { a_ok.store(1); }

        CHECK(!a->Close(net::CloseMode::Block, {}, After(5000ms)));
        CHECK(!b->Close(net::CloseMode::Block, {}, After(5000ms)));
        return {};
    }, "example_concurrent", 25s);
    if (rc) { std::cerr << "concurrent coroutine failed: " << rc.message() << '\n'; }

    for (int i = 0; i < 300 && (gate->b_echoed.load() == 0 || observer->Closes() < 2); ++i) {
        std::this_thread::sleep_for(10ms);
    }
    const auto client_stop = client.Stop();
    const auto server_stop = server.Stop();
    CHECK(!client_stop);
    CHECK(!server_stop);
    const bool fd_return = WaitFdReturn(fd_baseline);

    CHECK(b_ok.load() == 1 && a_ok.load() == 1);
    // 并发证据只取事件 gate：A 的 handler 挂起期间 B 整轮回显完成，gate 只有在 B 的
    // 回显字节到达后才放行 A。不用「B 完成时刻早于 A 完成时刻」这种程序顺序冒充并发。
    CHECK(gate->gate_released_after_b.load() == 1);
    CHECK(gate->a_wait_timeouts.load() == 0);
    CHECK(observer->PeakMax() == 1);                  // 同连接串行、每连接活跃事件峰值 1
    CHECK(fd_return);
    std::cout << "concurrent pass a_ok=" << a_ok.load() << " b_ok=" << b_ok.load()
              << " gate_released_after_b=" << gate->gate_released_after_b.load()
              << " a_gate_wait_timeouts=" << gate->a_wait_timeouts.load()
              << " observed_conns=" << observer->ObservedConns()
              << " per_connection_handler_peak=" << observer->PeakMax()
              << " b_echoed=" << gate->b_echoed.load()
              << " fd_return=" << (fd_return ? 1 : 0) << '\n';
    return g_fail == 0 ? 0 : 1;
}

// =============================== limits ===============================
static int ModeLimits() {
    const std::size_t fd_baseline = CountFds();
    // 有界配置经归一化后交给库；小 outbound/写缓冲用于观察背压返回（静态配置不等于命中）。
    net::Options server_options;
    server_options.num_event_loop = 2;
    server_options.read_buffer_cap = 4096;
    server_options.write_buffer_cap = 4096;
    server_options.max_inbound_bytes = 1 << 20;
    server_options.max_outbound_bytes = 1 << 20;
    server_options.max_pending_events = 256;
    net::NormalizedOptions normalized;
    CHECK(!net::NormalizeOptions(server_options, static_cast<int>(std::thread::hardware_concurrency()), normalized));

    net::Options client_options = server_options;
    client_options.num_event_loop = 1;        // Client 单 owner：loop 数须为 1
    client_options.max_outbound_bytes = 4096;
    client_options.write_buffer_cap = 4096;
    CHECK(!net::NormalizeOptions(client_options, static_cast<int>(std::thread::hardware_concurrency()), normalized));
    CHECK(normalized.max_outbound_bytes == 4096 && normalized.write_buffer_cap == 4096);

    auto slow = std::make_shared<SlowPeerHandler>();
    net::Engine server(slow);
    if (const auto error = server.Run(Loopback(0), server_options)) {
        std::cerr << "engine run failed: " << error.message() << '\n';
        return 1;
    }
    std::vector<net::Address> bound;
    if (const auto error = server.LocalAddresses(bound); error || bound.empty()) {
        std::cerr << "no bound address\n"; (void)server.Stop(); return 1;
    }
    const int port = bound.front().Port();

    auto observer = std::make_shared<ClientObserver>();
    net::Client client(observer);
    if (const auto error = client.Start(client_options)) {
        std::cerr << "client start failed: " << error.message() << '\n'; (void)server.Stop(); return 1;
    }

    constexpr int kAttempts = 32;
    constexpr std::size_t kChunk = 1024;
    std::atomic<long long> accepted{0};
    std::atomic<long long> rejected{0};
    std::atomic<long long> timed_out{0};
    std::atomic<int> resource_errors{0};
    std::atomic<int> deadline_hits{0};
    std::atomic<long long> echoed{0};

    const auto rc = RunInCoroutine([&]() -> std::error_code {
        std::shared_ptr<net::CoTcpConnection> conn;
        if (const auto error = client.Dial(Loopback(port), conn, After(5000ms))) { return error; }
        if (conn == nullptr) { return MakeErr(std::errc::bad_file_descriptor); }
        const std::string chunk(kChunk, 'y');
        const std::size_t base = observer->RxSize(conn->GetObjectInfo().id);
        for (int i = 0; i < kAttempts; ++i) {
            // Write 成功 = 命令被 owner 接纳（≠ 内核全发，≠ 对端收到）；
            // deadline 到期只结束等待，已投递命令结果未知 —— 不自动重发。
            const auto error = conn->Write(chunk, After(300ms));
            if (!error) { accepted.fetch_add(kChunk); continue; }
            if (IsError(error, std::errc::timed_out)) { ++deadline_hits; timed_out.fetch_add(kChunk); continue; }
            ++resource_errors; rejected.fetch_add(kChunk);
            std::cerr << "limits write[" << i << "] rejected: "
                      << error.category().name() << '/' << error.value() << ' ' << error.message() << '\n';
        }
        // 放行慢对端；已接纳字节必须全部回显（不静默丢数据）
        slow->Release();   // 通知慢对端 handler（条件事件挂起）放行
        const auto want = static_cast<std::size_t>(accepted.load());
        if (want == 0) { return {}; }
        std::string out;
        return AwaitBytes(*conn, *observer, base, want, out, After(15000ms));
    }, "example_limits", 30s);
    if (rc) { std::cerr << "limits coroutine failed: " << rc.message()
                        << " (category=" << rc.category().name() << ")\n"; }

    for (int i = 0; i < 500 && static_cast<long long>(slow->echoed.load()) < accepted.load(); ++i) {
        std::this_thread::sleep_for(10ms);
    }
    echoed.store(slow->echoed.load());
    const auto client_stop = client.Stop();
    const auto server_stop = server.Stop();
    CHECK(!client_stop);
    CHECK(!server_stop);
    const bool fd_return = WaitFdReturn(fd_baseline);

    const long long accepted_bytes = accepted.load();
    const long long rejected_bytes = rejected.load();
    const long long unknown_bytes = timed_out.load();
    const bool accounted = (accepted_bytes + rejected_bytes + unknown_bytes)
        == static_cast<long long>(kAttempts) * static_cast<long long>(kChunk);
    const bool no_loss = echoed.load() >= accepted_bytes;
    const bool dynamic_hit = (resource_errors.load() > 0) || (deadline_hits.load() > 0);
    CHECK(accounted);            // 每次尝试都有明确归属（成功/拒绝/结果未知）
    CHECK(no_loss);              // 已接纳字节全部回显
    CHECK(fd_return);
    std::cout << "limits accounting-only attempts=" << kAttempts
              << " accepted_bytes=" << accepted_bytes
              << " rejected_bytes=" << rejected_bytes
              << " timed_out_unknown_bytes=" << unknown_bytes
              << " resource_errors=" << resource_errors.load()
              << " deadline_hits=" << deadline_hits.load()
              << " echoed_bytes=" << echoed.load()
              << " accounted=" << (accounted ? 1 : 0)
              << " accepted_not_lost=" << (no_loss ? 1 : 0)
              << " backpressure_dynamic_hit=" << (dynamic_hit ? "1/OBSERVED" : "0/NOT_COVERED")
              << " fd_return=" << (fd_return ? 1 : 0) << '\n';
    std::cout << "limits note: 只报记账与不丢数据；静态上限配置不等于观察到队列满/deadline 返回。"
              << " 原设计 E4 的有界背压断言已被 examples-review-08 撤销，本模式不以 limits 通过"
              << " 宣传背压已覆盖（backpressure_dynamic_hit=0/NOT_COVERED 即未覆盖）。\n";
    return g_fail == 0 ? 0 : 1;
}

// =============================== errors ===============================
// 受控失败模式：预期失败语义命中 → 退出码 3（按设计非零退出）；断言不符 → 1；用法错 → 2。
static int ModeErrors() {
    const std::size_t fd_baseline = CountFds();
    net::Options options;
    options.num_event_loop = 1;
    options.tcp_no_delay = true;

    // (a) 域门禁：受管协程内调用 Engine::Run 在任何副作用之前拒绝
    auto handler = std::make_shared<EchoHandler>();
    auto engine = std::make_shared<net::Engine>(handler);
    const auto engine_run_rc = RunInCoroutine([&]() -> std::error_code {
        return engine->Run(Loopback(0), options);
    }, "example_errors_engine_run", 5s);
    CHECK(IsError(engine_run_rc, std::errc::operation_not_permitted));
    int domain_reject_ops = IsError(engine_run_rc, std::errc::operation_not_permitted) ? 1 : 0;

    // (b) 真实未决窗口的 Dial deadline：由库内返回 timed_out（不是外层 harness 超时）
    int pending_port = 0;
    auto listener = ListenBacklog1(pending_port);
    auto observer = std::make_shared<ClientObserver>();
    net::Client client(observer);
    if (const auto error = client.Start(options)) {
        std::cerr << "client start failed: " << error.message() << '\n';
        (void)listener.Close();
        return 1;
    }
    std::vector<std::shared_ptr<net::CoTcpConnection>> fillers;
    for (int i = 0; i < 2; ++i) {
        std::shared_ptr<net::CoTcpConnection> filler;
        const auto error = RunInCoroutine([&]() -> std::error_code {
            return client.Dial(Loopback(pending_port), filler, After(2000ms));
        }, "example_errors_fill", 6s);
        if (!error && filler != nullptr) { fillers.push_back(filler); }
    }
    CHECK(!fillers.empty());                 // accept 队列已被真实成功连接占满

    std::shared_ptr<net::CoTcpConnection> pending;
    int dial_attempts = 1;                   // 记录本请求只尝试一次：结果未知不盲目重发
    const auto pending_error = RunInCoroutine([&]() -> std::error_code {
        return client.Dial(Loopback(pending_port), pending, After(300ms));
    }, "example_errors_pending", 6s);
    const bool deadline_hit = IsError(pending_error, std::errc::timed_out);
    CHECK(deadline_hit);
    CHECK(pending == nullptr);               // 失败时 out 不变
    CHECK(pending_error.category().name() != std::string("bbt.infra.net.example.harness_timeout"));
    std::cout << "errors deadline: code=" << pending_error.category().name() << '/'
              << pending_error.value() << " (" << pending_error.message() << ')'
              << " dial_attempts=" << dial_attempts << " retransmit=0\n";

    // (c) 域门禁：非受管线程上的连接业务命令一律拒绝且无副作用（用已建立的连接）
    net::CoTcpConnection& live = *fillers.front();
    const auto deadline = After(1000ms);
    domain_reject_ops += IsError(live.Write(std::string("x"), deadline), std::errc::operation_not_permitted) ? 1 : 0;
    domain_reject_ops += IsError(live.Wake(), std::errc::operation_not_permitted) ? 1 : 0;
    domain_reject_ops += IsError(live.Close(net::CloseMode::Unblock, {}, deadline), std::errc::operation_not_permitted) ? 1 : 0;
    domain_reject_ops += IsError(live.Discard(1), std::errc::operation_not_permitted) ? 1 : 0;
    int reader_throws = 0;
    int reader_codes = 0;
    try { (void)live.InboundBuffered(); } catch (const std::system_error& error) {
        if (error.code() == MakeErr(std::errc::operation_not_permitted)) { ++reader_throws; }
    }
    try { (void)live.Peek(); } catch (const std::system_error& error) {
        if (error.code() == MakeErr(std::errc::operation_not_permitted)) { ++reader_codes; }
    }
    CHECK(reader_throws == 1 && reader_codes == 1);
    domain_reject_ops += reader_throws + reader_codes;
    CHECK(domain_reject_ops == 7);   // 1 Engine::Run + 4 连接命令 + 2 Reader，全部实测拒绝

    // (d) 连接被拒绝：无监听端口
    std::shared_ptr<net::CoTcpConnection> refused;
    const int closed_port = ClosedPort();
    const auto refuse_error = RunInCoroutine([&]() -> std::error_code {
        return client.Dial(Loopback(closed_port), refused, After(2000ms));
    }, "example_errors_refuse", 6s);
    const bool refused_hit = IsError(refuse_error, std::errc::connection_refused);
    CHECK(refused_hit);
    CHECK(refused == nullptr);
    std::cout << "errors refuse: code=" << refuse_error.category().name() << '/'
              << refuse_error.value() << " (" << refuse_error.message() << ")\n";

    // 收口：已交付 shell 先按序 Close(Block)，再 Client::Stop，再关 listener
    for (auto& filler : fillers) {
        const auto close_error = RunInCoroutine([&]() -> std::error_code {
            return filler->Close(net::CloseMode::Block, {}, After(3000ms));
        }, "example_errors_close", 6s);
        CHECK(!close_error || IsError(close_error, std::errc::timed_out));
    }
    const auto client_stop = client.Stop();
    CHECK(!client_stop);
    (void)listener.Close();
    const bool fd_return = WaitFdReturn(fd_baseline);
    CHECK(fd_return);

    const bool expected = (g_fail == 0);
    std::cout << "errors mode: expected_failures_observed=" << (expected ? 1 : 0)
              << " domain_reject_ops=" << domain_reject_ops
              << " refused=" << (refused_hit ? 1 : 0)
              << " deadline_timed_out=" << (deadline_hit ? 1 : 0)
              << " fd_return=" << (fd_return ? 1 : 0) << '\n';
    if (!expected) { return 1; }
    return 3;   // 受控失败模式：预期失败语义命中，按设计以非零码退出
}

// ---- 进程退出边界（既有约定：runtime 无停机入口） ----
// 先做一次最简受管挂起等待：让 runtime 自身的事件/定时资源在 fd 基线之前就位，
// 否则第一次业务挂起新建的 runtime 内部 fd 会被误当成网络资源残留。
static void WarmRuntime() {
    auto waiter = CoWaiter::Create();
    const auto rc = RunInCoroutine([&]() -> std::error_code {
        (void)waiter->Wait(WaitOptions{std::chrono::steady_clock::now() + 5ms});
        bbtco_yield;
        return {};
    }, "example_warmup", 5s);
    if (rc) { std::cerr << "runtime warmup failed: " << rc.message() << '\n'; }
}

static bool FlushOutput() {
    std::cout.flush();
    std::cerr.flush();
    std::clog.flush();
    if (std::fflush(nullptr) != 0) { return false; }
    if (std::ferror(stdout) != 0 || std::ferror(stderr) != 0) { return false; }
    return !std::cout.fail() && !std::cerr.fail() && !std::clog.fail();
}

static int ExitProcess(int code) {
    // 成败两条路径都必须先把输出刷出去：std::_Exit 不刷新 stdio，
    // 失败路径若只在成功时 flush，会把观测输出丢掉。
    const bool flushed = FlushOutput();
    if (code == 0 && !flushed) { code = 1; }
    std::_Exit(code);   // 跳过静态析构/atexit，不展开栈；不代表优雅停机
    return code;
}

int main(int argc, char** argv) {
    const std::string mode = argc > 1 ? argv[1] : "";
    if (mode != "lifecycle" && mode != "concurrent" && mode != "limits" && mode != "errors") {
        std::cerr << "usage: Example_co_net_service lifecycle|concurrent|limits|errors\n";
        return 2;
    }
    // runtime 引导：进程内一次；挂起只让出 Processer。无 runtime 停机（既有模型）。
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start();
    WarmRuntime();   // 先让 runtime 自身的挂起基础设施就位，fd 基线才有意义

    int code = 1;
    if (mode == "lifecycle") { code = ModeLifecycle(); }
    else if (mode == "concurrent") { code = ModeConcurrent(); }
    else if (mode == "limits") { code = ModeLimits(); }
    else { code = ModeErrors(); }
    return ExitProcess(code);
}
