// net 最小 API 冻结契约测试（计划 T4 / §7 D1–D6）。
//
// 与 b4_contract / r2_tcp 的分工：这里只针对"冻结后的公共面"与取消的内部接缝
// 做可判别回归，不重复业务往返。每个用例名对应计划 §7 的一行场景：
//   guard_lifetime           D2 守卫保活/唯一释放/移动/空守卫（Engine 收口后尾部）
//   co_context_gate          D4 四个 Co 入口在非法执行域零消费/零提交/零建连
//   options_contract         D3 保留字段边界 + 默认值（编译期删除由负向 TU 证明）
//   write_output_reuse       D4 写输出只描述本次调用（含接纳后等待失败）
//   output_preservation      D4 失败不覆盖调用者 out（CoRead/CoAccept/CoDial）
//   connect_delivery         D5 raw 单一交付：out_id/回调 once+相等/拒绝不变；
//                            早到交付用提交后屏障确定性制造（回调确在返回前）
//   connect_failure_cleanup  D5 未交付退出全回收（建立失败/满额/超时/取消）
//   accept_configuration     D6 无 handler 无 handoff 拒绝 + handler/handoff 共存
//
// 编译期"公共面不含 State/内部构造/测试屏障/旧等待 API"由
// net.freeze_public_surface（tests/net/consumers/expect_public_surface.cmake +
// consumers/public_surface_negative/neg_*.cc）以正向 TU 与逐项负向 TU 证明。
// 本文件除公共头外只经 detail/TestAccess、detail/CoInternal、detail/EngineTestAccess
// 注入交错与内部状态；这些接缝不回到公共面，也不被公共 consumer 引用。

#include <bbt/infra/net/CoConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/RawHandler.hpp>
#include <bbt/infra/net/ReadAccess.hpp>
#include <bbt/infra/net/Types.hpp>
#include <bbt/infra/net/detail/CoInternal.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/EngineTestAccess.hpp>
#include <bbt/infra/net/detail/OptionsNormalize.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <csignal>
#include <cstdlib>
#include <new>
#include <execinfo.h>
#include <filesystem>
#include <functional>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <poll.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>

using namespace bbt::infra::net;
using namespace std::chrono_literals;

// ---- 分配失败注入（仅本可执行文件；thread_local 只影响设置它的线程）----
namespace {
thread_local bool g_fail_alloc = false;
}

void* operator new(std::size_t size) {
    if (g_fail_alloc) {
        throw std::bad_alloc();
    }
    void* memory = std::malloc(size == 0 ? 1 : size);
    if (memory == nullptr) {
        throw std::bad_alloc();
    }
    return memory;
}
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete[](void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void* memory, std::size_t) noexcept { std::free(memory); }

static void Check(bool value, const char* expression, int line) {
    if (!value) {
        throw std::runtime_error("line " + std::to_string(line) + ": " + expression);
    }
}
#define CHECK(expression) Check(static_cast<bool>(expression), #expression, __LINE__)

static bbt::coroutine::WaitOptions Until(std::chrono::milliseconds duration) {
    return {std::chrono::steady_clock::now() + duration};
}

static bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

static void StartRuntime(std::size_t workers) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = workers;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start();
}

static std::future<void> Task(std::function<void()> callback) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    g_scheduler->RegistCoroutineTask(65536, [promise, callback] {
        try {
            callback();
            promise->set_value();
        } catch (...) {
            promise->set_exception(std::current_exception());
        }
    }, "net.freeze");
    return future;
}

static void Get(std::future<void>& future) {
    CHECK(future.wait_for(5s) == std::future_status::ready);
    future.get();
}

static Address Loopback(int port) {
    std::error_code error;
    Address address = Address::Ip("127.0.0.1", port, error);
    CHECK(!error);
    return address;
}

static std::size_t CountFds() {
    return static_cast<std::size_t>(std::distance(
        std::filesystem::directory_iterator("/proc/self/fd"),
        std::filesystem::directory_iterator{}));
}

struct EngineRunner {
    Engine& engine;
    std::error_code error;
    std::thread thread;

    EngineRunner(Engine& target, EngineConfig config) : engine(target) {
        thread = std::thread([this, config]() mutable { error = engine.Run(config); });
    }
    bool WaitForReturn(std::chrono::milliseconds timeout) {
        return WaitUntil([&] { return engine.Phase() == EnginePhase::Stopped; }, timeout);
    }
    void CloseAndJoin() {
        engine.RequestClose();
        if (thread.joinable()) {
            thread.join();
        }
    }
    ~EngineRunner() { CloseAndJoin(); }
};

static EngineConfig ServerConfig(const Address& listen, std::size_t subs = 1) {
    EngineConfig config;
    config.listen_addresses = {listen};
    config.runtime_progress = RuntimeProgress::Independent;
    config.options.num_event_loop = static_cast<std::int64_t>(subs);
    return config;
}

static EngineConfig ClientConfig() {
    EngineConfig config;
    config.runtime_progress = RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    return config;
}

static NativeSocket MakeListener(Address& address) {
    std::error_code error;
    NativeSocket listener = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    CHECK(!error);
    CHECK(!listener.Bind(Loopback(0)));
    CHECK(!listener.Listen(8));
    address = listener.LocalAddress(error);
    CHECK(!error);
    return listener;
}

static NativeSocket ConnectClient(const Address& address) {
    std::error_code error;
    NativeSocket client = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    CHECK(!error);
    const auto connect_error = client.Connect(address);
    CHECK(!connect_error || connect_error.value() == EINPROGRESS
          || connect_error.value() == EWOULDBLOCK);
    pollfd descriptor{client.Fd(), POLLOUT, 0};
    CHECK(::poll(&descriptor, 1, 3000) > 0);
    CHECK(!client.PendingError());
    return client;
}

struct NullHandler : RawHandler {};

// ===========================================================================
// D2 guard_lifetime
// ===========================================================================
static void GuardLifetime() {
    // 1) 保活：只在内部 state 上建立守卫，随后销毁唯一 Connection，守卫仍可读。
    auto state = std::make_shared<ConnectionState>(1, 4096, 1024, 4, nullptr);
    CHECK(state->ring.TryAppend("abcdef").status == ReadStatus::Ok);
    Connection connection = ConnectionTestAccess::Make(state);
    auto guard = connection.TryAccess();
    CHECK(bool(guard));
    CHECK(guard.Size() == 6);
    connection = Connection();          // 释放唯一句柄：守卫必须保活数据
    CHECK(state.use_count() == 2);      // 局部 state + 守卫（唯一业务引用已释放）
    CHECK(guard.Peek() == "abcdef");
    std::string copied;
    CHECK(!guard.CopyAll(copied));      // 不消费
    CHECK(copied == "abcdef");
    CHECK(guard.Size() == 6);
    CHECK(!guard.Consume(6));
    CHECK(guard.Size() == 0);

    // 2) 关闭后尾部仍可读：先入队尾部（需先释放上段保护），再发布 MarkClosed，
    //    然后由新守卫消费；空守卫在已关闭连接上应报 Eof 而不是 WouldBlock。
    ConnectionTestAccess::Reset(guard);   // 释放上段保护，否则 TryAppend 会 Busy
    CHECK(state->ring.TryAppend("tail").status == ReadStatus::Ok);
    state->MarkClosed(std::make_error_code(std::errc::operation_canceled));
    {
        // 原句柄已清空：空句柄的 TryAccess 只能是 WouldBlock，不得伪装 Eof。
        auto empty_handle_guard = connection.TryAccess();
        CHECK(empty_handle_guard.WouldBlock() && !empty_handle_guard.Eof());
        CHECK(empty_handle_guard.Size() == 0);
        // 关闭后的尾部属于 state 真源：新句柄经守卫仍能取走，之后才是 Eof。
        Connection rebuilt = ConnectionTestAccess::Make(state);
        auto closed_guard = rebuilt.TryAccess();
        CHECK(bool(closed_guard));
        copied.clear();
        CHECK(!closed_guard.CopyAll(copied));
        CHECK(copied == "tail");
        CHECK(!closed_guard.ConsumeInto(copied));
        CHECK(copied == "tailtail");
        ConnectionTestAccess::Reset(closed_guard);
        auto after_tail = rebuilt.TryAccess();
        CHECK(!after_tail && after_tail.Eof());
        CHECK(after_tail.Size() == 0);
    }

    // 3) 析构安全：已释放的守卫再访问必须安全（不重复解锁、不触碰 state）。
    CHECK(!guard);
    CHECK(guard.Size() == 0);
    CHECK(guard.Peek().empty());
    CHECK(guard.Consume(1) == std::make_error_code(std::errc::operation_not_permitted));
    CHECK(state.use_count() == 1);

    // 4) 移动构造：锁与通知责任转移，源变为空守卫；释放只推进一次代次。
    {
        auto move_state = std::make_shared<ConnectionState>(2, 4096, 1024, 4, nullptr);
        CHECK(move_state->ring.TryAppend("mv").status == ReadStatus::Ok);
        Connection owner = ConnectionTestAccess::Make(move_state);
        auto source = owner.TryAccess();
        CHECK(bool(source));
        const auto before = move_state->ring.AccessGeneration();
        ReadAccess moved(std::move(source));
        CHECK(!source);                       // 源不再持锁/不再有通知责任
        CHECK(source.Size() == 0);
        CHECK(bool(moved) && moved.Size() == 2);
        CHECK(move_state->ring.AccessGeneration() == before);  // 移动不释放保护
        ReadAccess sink;
        sink = std::move(moved);              // 移动赋值：先释放旧（空）守卫
        CHECK(!moved);
        CHECK(bool(sink) && sink.Size() == 2);
        CHECK(move_state->ring.AccessGeneration() == before);
        ConnectionTestAccess::Reset(sink);    // 唯一一次真实释放
        CHECK(move_state->ring.AccessGeneration() == before + 1);
    }

    // 5) 移动赋值释放旧守卫：旧守卫的 state 引用必须归还，且只推进一次代次。
    {
        auto first_state = std::make_shared<ConnectionState>(3, 4096, 1024, 4, nullptr);
        auto second_state = std::make_shared<ConnectionState>(4, 4096, 1024, 4, nullptr);
        CHECK(first_state->ring.TryAppend("aa").status == ReadStatus::Ok);
        CHECK(second_state->ring.TryAppend("bb").status == ReadStatus::Ok);
        auto held = ConnectionTestAccess::HoldForTest(first_state);
        CHECK(bool(held));
        const auto first_before = first_state->ring.AccessGeneration();
        held = ConnectionTestAccess::HoldForTest(second_state);  // 释放旧守卫 → 接管新
        CHECK(first_state->ring.AccessGeneration() == first_before + 1);
        CHECK(first_state.use_count() == 1);   // 旧 state 引用已归还
        CHECK(bool(held) && held.Size() == 2 && held.Peek() == "bb");
        ConnectionTestAccess::Reset(held);
    }

    // 6) 空守卫：析构/移动都不触碰任何 state，也不推进代次。
    {
        auto empty_state = std::make_shared<ConnectionState>(5, 4096, 1024, 4, nullptr);
        CHECK(empty_state->ring.TryAppend("z").status == ReadStatus::Ok);
        const auto before = empty_state->ring.AccessGeneration();
        ReadAccess none;
        ReadAccess also_none(std::move(none));
        CHECK(!also_none);
        none = std::move(also_none);
        CHECK(!none);
        CHECK(empty_state->ring.AccessGeneration() == before);
        CHECK(empty_state.use_count() == 1);
    }

    std::cout << "guard_lifetime keepalive/move/empty/closed_tail=ok\n";
}

// ===========================================================================
// D4 co_context_gate
// ===========================================================================
namespace {

struct GateProbe {
    std::error_code read_error;
    std::error_code write_error;
    std::error_code accept_error;
    std::error_code dial_error;
    WriteOutcome outcome;
    bool operation_valid{true};
    std::string read_out;
};

// 在给定调用域内跑四个 Co 入口；调用者预先放好"快路径"条件（有数据/零长写/
// 已就绪 handoff 队列），从而证明拒绝发生在消费/提交/出队之前。
GateProbe RunAllCoEntries(Connection& connection, Engine& accept_engine, Engine& dial_engine,
                          const Address& dial_address) {
    GateProbe probe;
    probe.read_error = CoRead(connection, probe.read_out, Until(50ms));
    WriteOperation operation;
    probe.write_error = CoWrite(connection, std::string(4, 'w'), Until(50ms), probe.outcome, operation);
    probe.operation_valid = operation.Valid();
    Connection accepted;
    probe.accept_error = CoAccept(accept_engine, accepted, Until(50ms));
    Connection dialed;
    probe.dial_error = CoDial(dial_engine, dial_address, dialed, Until(50ms));
    return probe;
}

void CheckGateProbe(const GateProbe& probe, const char* domain) {
    const auto not_permitted = std::make_error_code(std::errc::operation_not_permitted);
    CHECK(probe.read_error == not_permitted);
    CHECK(probe.write_error == not_permitted);
    CHECK(probe.accept_error == not_permitted);
    CHECK(probe.dial_error == not_permitted);
    CHECK(probe.outcome.phase == WritePhase::Rejected);
    CHECK(probe.outcome.sent == 0);
    CHECK(probe.outcome.error == not_permitted);
    CHECK(!probe.operation_valid);
    CHECK(probe.read_out.empty());      // CoRead 失败不改 out
    std::cout << "co_context_gate domain=" << domain << " all=operation_not_permitted\n";
}

}  // namespace

static void CoContextGate() {
    StartRuntime(4);   // server 占 main+1 sub，host 另占 1 sub
    // 服务端：handler + handoff，用来观察"已就绪 accept 是否被出队"。
    auto handler = std::make_shared<NullHandler>();
    Engine server(handler);
    CHECK(!server.EnableAcceptHandoff(4));
    EngineRunner server_runner(server, ServerConfig(Loopback(0), 1));
    CHECK(WaitUntil([&] { return server.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> server_addresses;
    CHECK(!server.LocalAddresses(server_addresses));

    // client-only + handoff：用于非法域 CoAccept（无 main owner → not_supported 是
    // 合法域下的结果，这里只关心 gate 先行，所以另建一个已配置 handoff 的 server）。
    Engine host{std::shared_ptr<RawHandler>()};
    EngineRunner host_runner(host, ClientConfig());
    CHECK(WaitUntil([&] { return host.Phase() == EnginePhase::Running; }, 3000ms));

    // 连接真源：ring 里已有数据 + 一个活动写操作（任何提交都会推进 ActiveOpCount）。
    auto state = std::make_shared<ConnectionState>(7, 4096, 1024, 4, nullptr);
    CHECK(state->ring.TryAppend("pending-bytes").status == ReadStatus::Ok);
    Connection connection = ConnectionTestAccess::Make(state);
    std::shared_ptr<WriteOpRecord> warm_record;
    CHECK(!state->AcceptWrite("queued", warm_record));
    const std::size_t ring_before = state->ring.Size();
    const std::size_t ops_before = state->ActiveOpCount();
    const auto queue_before = state->ring.AccessGeneration();

    // 让 handoff 队列里真实有一个已接纳连接（否则"不出队"无法判别）。
    auto client = ConnectClient(server_addresses[0]);
    CHECK(WaitUntil([&] { return server.CountConnections() >= 1; }, 3000ms));

    const auto connect_table_before = EngineTestAccess::ConnectTableSize(host);

    // 域 1：普通线程（未启动 TLS 的调用线程）。
    CheckGateProbe(RunAllCoEntries(connection, server, host, server_addresses[0]), "plain_thread");

    // 域 2：runtime 已启动、本线程显式开启 TLS 协程使用，但仍未持有协程上下文
    // （idle：LocalThread 允许用协程，GetLocalCoroutineId() 仍为 0）。与域 1 的
    // 区别是可判别事实：TLS 开关为 true，gate 仍须按 coroutine_id 拒绝。
    std::exception_ptr idle_thread_error;
    std::thread idle_thread([&] {
        try {
            bbt::coroutine::detail::LocalThread::GetTLSInst()->SetEnableUseCo(true);
            CHECK(bbt::coroutine::GetLocalCoroutineId() == 0);
            CheckGateProbe(RunAllCoEntries(connection, server, host, server_addresses[0]),
                           "runtime_idle_thread_tls_enabled");
        } catch (...) {
            idle_thread_error = std::current_exception();
        }
    });
    idle_thread.join();
    if (idle_thread_error) { std::rethrow_exception(idle_thread_error); }

    // 域 3：raw owner 回调同域（非业务协程）。**这是模拟**：raw 回调在 owner 的
    // 非协程边界执行，这里用一个不持协程上下文的普通线程复现同一判定，避免向
    // 产品面加测试回调钩子；它不证明 owner 线程的其它属性（TLS 开关、线程名）。
    std::exception_ptr owner_like_error;
    std::thread owner_like([&] {
        try {
            CHECK(bbt::coroutine::GetLocalCoroutineId() == 0);
            CheckGateProbe(RunAllCoEntries(connection, server, host, server_addresses[0]),
                           "owner_callback_domain_simulated");
        } catch (...) {
            owner_like_error = std::current_exception();
        }
    });
    owner_like.join();
    if (owner_like_error) { std::rethrow_exception(owner_like_error); }

    // 零副作用：数据未消费、无新活动写、handoff 队列未被弹走、零新建连。
    CHECK(state->ring.Size() == ring_before);
    CHECK(state->ActiveOpCount() == ops_before);
    CHECK(state->ring.AccessGeneration() == queue_before);
    CHECK(EngineTestAccess::ConnectTableSize(host) == connect_table_before);
    CHECK(host.CountConnections() == 0);
    CHECK(server.CountConnections() >= 1);  // 队列里的连接仍在

    // 已就绪 handoff 仍可被合法域（协程）取走：证明拒绝路径零出队。
    std::atomic<int> accepted{-1};
    auto task = Task([&] {
        Connection got;
        if (!CoAccept(server, got, Until(3000ms)) && got.Valid()) {
            accepted.store(1);
            got.RequestClose();
        }
    });
    Get(task);
    CHECK(accepted.load() == 1);

    // 受管协程正向对照：同样四个入口在合法域成功（CoDial 到 server 地址）。
    std::atomic<int> positive{-1};
    auto positive_task = Task([&] {
        auto pos_state = std::make_shared<ConnectionState>(8, 4096, 1024, 4, nullptr);
        CHECK(pos_state->ring.TryAppend("in-domain").status == ReadStatus::Ok);
        Connection pos = ConnectionTestAccess::Make(pos_state);
        std::string out;
        if (CoRead(pos, out, Until(2000ms)) || out != "in-domain") {
            return;
        }
        // 已接纳后无 owner 推进：允许 timed_out + InFlight（本次 op 有效可查）。
        WriteOutcome outcome;
        WriteOperation operation;
        const auto write_error = CoWrite(pos, "x", Until(300ms), outcome, operation);
        const bool accepted_in_flight =
            write_error == std::make_error_code(std::errc::timed_out)
            && outcome.phase == WritePhase::InFlight && operation.Valid();
        const bool done_locally =
            !write_error && outcome.phase == WritePhase::Done && operation.Valid();
        if (!accepted_in_flight && !done_locally) {
            return;
        }
        Connection dialed;
        if (CoDial(host, server_addresses[0], dialed, Until(3000ms)) || !dialed.Valid()) {
            return;
        }
        dialed.RequestClose();
        positive.store(1);
    });
    Get(positive_task);
    CHECK(positive.load() == 1);

    (void)client.Close();
    server_runner.CloseAndJoin();
    host_runner.CloseAndJoin();
    std::cout << "co_context_gate positive_control=ok queue_preserved=ok\n";
}

// ===========================================================================
// D3 options_contract（编译期删除项在 net.freeze_public_surface 负向 TU）
// ===========================================================================
static void OptionsContract() {
    // 默认：loop=1（不做 CPU 推导）、总量界 1MiB、pending 1024、keepalive 关闭。
    NormalizedOptions defaults;
    CHECK(!NormalizeOptions(Options{}, defaults));
    CHECK(defaults.num_event_loop == 1);
    CHECK(defaults.max_inbound_bytes == 1024 * 1024);
    CHECK(defaults.max_outbound_bytes == 1024 * 1024);
    CHECK(defaults.max_pending_events == 1024);
    CHECK(defaults.tcp_keep_alive == std::chrono::seconds{0});
    CHECK(defaults.tcp_keep_interval == std::chrono::seconds{0});
    CHECK(defaults.tcp_keep_count == 0);
    CHECK(defaults.socket_recv_buffer == 0);
    CHECK(defaults.socket_send_buffer == 0);
    CHECK(defaults.tcp_no_delay);
    CHECK(!defaults.reuse_addr);
    CHECK(!defaults.reuse_port);

    // 显式 loop 与负值（失败不改 out）。
    {
        Options options;
        options.num_event_loop = 3;
        NormalizedOptions out;
        CHECK(!NormalizeOptions(options, out));
        CHECK(out.num_event_loop == 3);
    }
    {
        Options options;
        options.num_event_loop = -1;
        NormalizedOptions out;
        out.num_event_loop = 777;
        out.max_pending_events = 4321;
        CHECK(NormalizeOptions(options, out) == std::make_error_code(std::errc::invalid_argument));
        CHECK(out.num_event_loop == 777);
        CHECK(out.max_pending_events == 4321);
    }

    // num_event_loop 的可表示范围：只断言本平台真能表达的结论，不机械复制
    // "32 位也拒绝"。64 位下 int64 正值都能用 size_t 表示，归一化不设固定上界。
#if SIZE_MAX < UINT64_MAX
    // 32 位目标：int64 正值不能全部用 size_t 表示，server 还要额外 +1 main 槽，
    // 所以归一化必须拒绝，且失败路径不改 out。
    {
        const std::int64_t unusable[] = {
            static_cast<std::int64_t>(std::numeric_limits<std::size_t>::max()),
            std::int64_t{1} << 62,
            std::numeric_limits<std::int64_t>::max()};
        for (const auto loops_value : unusable) {
            Options options;
            options.num_event_loop = loops_value;
            NormalizedOptions out;
            out.num_event_loop = 777;
            out.max_pending_events = 4321;
            CHECK(NormalizeOptions(options, out)
                  == std::make_error_code(std::errc::result_out_of_range));
            CHECK(out.num_event_loop == 777);
            CHECK(out.max_pending_events == 4321);
        }
    }
#else
    // 64 位目标：归一化接受，worker 槽不足属运行期资源事实，由 Engine::Run 拒绝。
    {
        Options options;
        options.num_event_loop = std::numeric_limits<std::int64_t>::max();
        NormalizedOptions out;
        CHECK(!NormalizeOptions(options, out));
        CHECK(out.num_event_loop
              == static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max()));
    }
#endif

    // 资源界 0 / 1 / 4096 / 超范围：小值必须可用（旧 cap 交叉校验已删除）。
    const auto bounds = [](std::int64_t inbound, std::int64_t outbound, std::int64_t events) {
        Options options;
        options.max_inbound_bytes = inbound;
        options.max_outbound_bytes = outbound;
        options.max_pending_events = events;
        NormalizedOptions out;
        return std::make_pair(NormalizeOptions(options, out), out);
    };
    CHECK(bounds(0, 1024, 1024).first
          == std::make_error_code(std::errc::invalid_argument));
    CHECK(bounds(1, 1, 1).first == std::error_code{});
    CHECK(bounds(1, 1, 1).second.max_inbound_bytes == 1);
    CHECK(bounds(1, 1, 1).second.max_outbound_bytes == 1);
    CHECK(bounds(1, 1, 1).second.max_pending_events == 1);
    CHECK(bounds(4096, 4096, 1).second.max_inbound_bytes == 4096);
    CHECK(bounds(-1, 1024, 1024).first
          == std::make_error_code(std::errc::invalid_argument));
    {
        Options options;
        options.max_pending_events = std::numeric_limits<std::int64_t>::max();
        NormalizedOptions out;
        // 64 位平台上 int64 max 恰好落在 size_t 范围内：合法且不截断。
        const auto error = NormalizeOptions(options, out);
        CHECK(!error);
        CHECK(out.max_pending_events
              == static_cast<std::size_t>(std::numeric_limits<std::int64_t>::max()));
    }

    // keepalive：默认 interval=idle/5、count=5、最小 1s；非法值逐项拒绝。
    {
        Options options;
        options.tcp_keep_alive = std::chrono::seconds{11};
        NormalizedOptions out;
        CHECK(!NormalizeOptions(options, out));
        CHECK(out.tcp_keep_interval == std::chrono::seconds{2});
        CHECK(out.tcp_keep_count == 5);
    }
    {
        Options options;
        options.tcp_keep_alive = std::chrono::seconds{1};
        NormalizedOptions out;
        CHECK(!NormalizeOptions(options, out));
        CHECK(out.tcp_keep_interval == std::chrono::seconds{1});
    }
    {
        Options options;
        options.tcp_keep_interval = std::chrono::seconds{-1};
        NormalizedOptions out;
        CHECK(NormalizeOptions(options, out) == std::make_error_code(std::errc::invalid_argument));
    }
    {
        Options options;
        options.tcp_keep_count = std::int64_t{std::numeric_limits<std::int32_t>::max()} + 1;
        NormalizedOptions out;
        CHECK(NormalizeOptions(options, out) == std::make_error_code(std::errc::result_out_of_range));
    }
    {
        Options options;
        options.socket_send_buffer = -1;
        NormalizedOptions out;
        CHECK(NormalizeOptions(options, out) == std::make_error_code(std::errc::result_out_of_range));
    }
    {
        // 合法枚举原样保留；非法枚举拒绝（不再有 LogLevel 这类占位字段）。
        Options options;
        options.lb = static_cast<LoadBalancing>(42);
        NormalizedOptions out;
        CHECK(NormalizeOptions(options, out) == std::make_error_code(std::errc::invalid_argument));
    }

    // 极端 num_event_loop 经真实 Engine::Run：必须返回错误而不是抛异常，且 phase
    // 落在 Stopped（旧行为抛 std::length_error 并停在 Starting，Engine 永久作废）。
    // 客户端（无 main 槽）与服务端（合法 handler + handoff，多一个 main 槽）各跑一遍。
    StartRuntime(2);
    const auto extreme_config = [](bool as_server, std::int64_t loops) {
        EngineConfig config;
        config.runtime_progress = RuntimeProgress::Independent;
        config.options.num_event_loop = loops;
        if (as_server) {
            config.listen_addresses = {Loopback(0)};
        }
        return config;
    };
    const auto loop_space_exhausted = std::make_error_code(std::errc::no_buffer_space);
    const std::int64_t extreme_loops[] = {
        std::int64_t{1} << 62, std::numeric_limits<std::int64_t>::max()};
    for (const auto loops_value : extreme_loops) {
        {
            Engine client{std::shared_ptr<RawHandler>()};
            std::error_code error;
            bool threw = false;
            try {
                error = client.Run(extreme_config(false, loops_value));
            } catch (...) {
                threw = true;
            }
            CHECK(!threw);
            CHECK(error == loop_space_exhausted);
            CHECK(client.Phase() == EnginePhase::Stopped);
        }
        {
            auto handler = std::make_shared<NullHandler>();
            Engine server(handler);
            CHECK(!server.EnableAcceptHandoff(4));
            std::error_code error;
            bool threw = false;
            try {
                error = server.Run(extreme_config(true, loops_value));
            } catch (...) {
                threw = true;
            }
            CHECK(!threw);
            CHECK(error == loop_space_exhausted);
            CHECK(server.Phase() == EnginePhase::Stopped);
        }
    }

    // 正对照：同一 runtime 上普通客户端仍能启动 —— 极端值失败没有留下 driver 占用
    // （拒绝发生在 reserve/登记之前，走既有 fail() 回滚）。
    {
        Engine normal{std::shared_ptr<RawHandler>()};
        EngineRunner normal_runner(normal, ClientConfig());
        CHECK(WaitUntil([&] { return normal.Phase() == EnginePhase::Running; }, 3000ms));
    }

    std::cout << "options_contract defaults/bounds/keepalive/extreme_loop=ok\n";
}

// ===========================================================================
// D4 write_output_reuse
// ===========================================================================
static void WriteOutputReuse() {
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(1, 4096, 256, 4, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);
    auto sink = [](const std::string_view* views, std::size_t, std::size_t budget) -> ssize_t {
        return static_cast<ssize_t>(std::min(views[0].size(), budget));
    };
    // 1) 先取得一次真实成功的结果，把它留在 outcome/operation 里（复用变量的起点）。
    std::shared_ptr<WriteOpRecord> done_record;
    CHECK(!state->AcceptWrite("hello", done_record));   // 真实接纳路径
    state->PumpWrites(8, 4096, sink);
    CHECK(ConnectionTestAccess::MakeWrite(done_record).Query().phase == WritePhase::Done);

    std::atomic<int> result{-1};
    std::atomic<int> progress{0};   // 失败时定位到具体步骤
    auto task = Task([&] {
        WriteOutcome outcome = ConnectionTestAccess::MakeWrite(done_record).Query();
        WriteOperation operation = ConnectionTestAccess::MakeWrite(done_record);
        if (outcome.phase != WritePhase::Done || outcome.sent != 5 || !operation.Valid()
            || outcome.error) {
            return;   // 起点不成立，后面的"覆盖"就没有判别力
        }
        const auto first_id = operation.Id();
        progress.store(2);

        // 2) 无效连接：必须覆盖为本次 Rejected + op 无效，不泄露上次 Done。
        Connection invalid;
        const auto invalid_error = CoWrite(invalid, "x", Until(50ms), outcome, operation);
        if (invalid_error != make_error_code(NetErrc::Closed)
            || outcome.phase != WritePhase::Rejected || operation.Valid()) {
            return;
        }

        // 3) 超大写（> max_outbound_bytes）：Rejected/no_buffer_space，op 无效。
        progress.store(4);
        const std::string oversized(257, 'o');
        const auto oversized_error = CoWrite(connection, oversized, Until(500ms), outcome, operation);
        if (oversized_error != std::make_error_code(std::errc::no_buffer_space)
            || outcome.phase != WritePhase::Rejected || operation.Valid()) {
            return;
        }

        // 4) 已封口连接：Rejected/Closed，op 无效。
        progress.store(5);
        Connection closed_conn = ConnectionTestAccess::Make(state);
        state->MarkClosed(std::make_error_code(std::errc::operation_canceled));
        const auto closed_error = CoWrite(closed_conn, "y", Until(50ms), outcome, operation);
        if (closed_error != make_error_code(NetErrc::Closed)
            || outcome.phase != WritePhase::Rejected || operation.Valid()) {
            return;
        }
        (void)first_id;
        progress.store(6);
        result.store(0);
    });
    Get(task);
    if (result.load() != 0) {
        throw std::runtime_error("write_output_reuse failed at step "
                                 + std::to_string(progress.load()));
    }
    CHECK(result.load() == 0);

    // 5) 接纳后等待失败保留本次 op：无 owner 推进的 state 上必然超时且在途。
    {
        auto in_flight_state = std::make_shared<ConnectionState>(2, 4096, 4096, 16, nullptr);
        Connection in_flight = ConnectionTestAccess::Make(in_flight_state);
        std::atomic<int> inflight_result{-1};
        auto inflight_task = Task([&] {
            WriteOutcome outcome;
            WriteOperation operation;
            const auto error = CoWrite(in_flight, std::string(8, 'd'), Until(30ms), outcome, operation);
            if (error == std::make_error_code(std::errc::timed_out)
                && outcome.phase == WritePhase::InFlight && operation.Valid()) {
                inflight_result.store(0);
            }
        });
        Get(inflight_task);
        CHECK(inflight_result.load() == 0);
    }

    // 6) 写接纳分配失败：AsyncWrite 返回 not_enough_memory → 本次 Rejected + op 无效。
    {
        auto alloc_state = std::make_shared<ConnectionState>(3, 4096, 4096, 4, nullptr);
        Connection alloc_conn = ConnectionTestAccess::Make(alloc_state);
        std::atomic<int> alloc_result{-1};
        auto alloc_task = Task([&] {
            WriteOutcome outcome;
            WriteOperation operation;
            g_fail_alloc = true;
            const auto error = CoWrite(alloc_conn, "zz", Until(500ms), outcome, operation);
            g_fail_alloc = false;
            if (error == std::make_error_code(std::errc::not_enough_memory)
                && outcome.phase == WritePhase::Rejected && outcome.sent == 0
                && outcome.error == std::make_error_code(std::errc::not_enough_memory)
                && !operation.Valid() && alloc_state->ActiveOpCount() == 0) {
                alloc_result.store(0);
            }
        });
        Get(alloc_task);
        CHECK(alloc_result.load() == 0);
    }

    // 7) 容量等待名额满（hub 256）：Rejected/no_buffer_space，零接纳、op 无效。
    {
        auto full_state = std::make_shared<ConnectionState>(4, 4096, 4, 4, nullptr);
        Connection full_conn = ConnectionTestAccess::Make(full_state);
        std::shared_ptr<WriteOpRecord> warm_record;
        CHECK(!full_state->AcceptWrite("warm", warm_record));   // 占满 outbound=8
        std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>> keep;
        for (std::size_t i = 0; i < WakeHub::kMaxWaiters; ++i) {
            auto waiter = full_state->CreateWriteWaiter();
            CHECK(full_state->AddWriteWaiter(waiter));
            keep.push_back(std::move(waiter));
        }
        std::atomic<int> full_result{-1};
        auto full_task = Task([&] {
            WriteOutcome outcome;
            WriteOperation operation;
            const auto error = CoWrite(full_conn, "yy", Until(500ms), outcome, operation);
            if (error == std::make_error_code(std::errc::no_buffer_space)
                && outcome.phase == WritePhase::Rejected && !operation.Valid()) {
                full_result.store(0);
            }
        });
        Get(full_task);
        CHECK(full_result.load() == 0);
        CHECK(full_state->ActiveOpCount() == 1);   // 只有 warm 被接纳
    }

    // 8) Done + error（失败结算）与等待错误分开：等待返回空表示本地终态，
    //    outcome.error 才是发送结果。用 EPIPE sink 制造真实写失败。
    {
        auto failed_state = std::make_shared<ConnectionState>(4, 4096, 4096, 4, nullptr);
        Connection failed_conn = ConnectionTestAccess::Make(failed_state);
        std::atomic<int> failed_result{-1};
        auto failed_task = Task([&] {
            WriteOutcome outcome;
            WriteOperation operation;
            std::thread pump([&] {
                std::this_thread::sleep_for(20ms);
                failed_state->PumpWrites(4, 4096,
                    [](const std::string_view*, std::size_t, std::size_t) -> ssize_t {
                        errno = EPIPE;
                        return -1;
                    });
            });
            const auto error = CoWrite(failed_conn, "boom", Until(2000ms), outcome, operation);
            pump.join();
            // 本地完成路径：返回的就是该次发送错误（EPIPE），outcome 同为 Done+EPIPE；
            // 这与"等待超时但仍在途"（timed_out + InFlight）是不同的两个结果。
            if (error.value() == EPIPE && outcome.phase == WritePhase::Done
                && outcome.error.value() == EPIPE && outcome.sent == 0
                && operation.Valid()) {
                failed_result.store(0);
            }
        });
        Get(failed_task);
        CHECK(failed_result.load() == 0);
    }

    std::cout << "write_output_reuse invalid/oversize/closed/wait_fail/done_error=ok\n";
}

// ===========================================================================
// D4 output_preservation
// ===========================================================================
static void OutputPreservation() {
    StartRuntime(2);
    // 1) CoRead 超时不消费、不改非空 out。
    {
        auto state = std::make_shared<ConnectionState>(1, 4096, 1024, 4, nullptr);
        Connection connection = ConnectionTestAccess::Make(state);
        std::string out = "keep";
        std::atomic<int> result{-1};
        auto task = Task([&] {
            const auto error = CoRead(connection, out, Until(30ms));
            if (error == std::make_error_code(std::errc::timed_out) && out == "keep") {
                result.store(0);
            }
        });
        Get(task);
        CHECK(result.load() == 0);
        CHECK(state->ring.Size() == 0);
    }
    // 2) CoRead 分配失败：out 与 ring 双不变（强保证）。
    {
        auto state = std::make_shared<ConnectionState>(2, 4096, 1024, 4, nullptr);
        CHECK(state->ring.TryAppend("payload").status == ReadStatus::Ok);
        Connection connection = ConnectionTestAccess::Make(state);
        auto out = std::make_shared<std::string>("existing");
        ReceiveRing::SetAppendFaultForTest([] { throw std::bad_alloc(); });
        auto task = Task([&] {
            const auto error = CoRead(connection, *out, Until(500ms));
            CHECK(static_cast<bool>(error));
        });
        Get(task);
        ReceiveRing::SetAppendFaultForTest({});
        CHECK(*out == "existing");
        CHECK(state->ring.Size() == 7);
    }
    // 3) CoAccept 失败不覆盖调用者已有 Connection（未启用 handoff → 立即拒绝）。
    {
        auto stack_handler = std::make_shared<NullHandler>();
        Engine engine(stack_handler);
        EngineRunner runner(engine, ServerConfig(Loopback(0), 1));
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));

        auto original_state = std::make_shared<ConnectionState>(9, 1024, 1024, 4, nullptr);
        CHECK(original_state->ring.TryAppend("original").status == ReadStatus::Ok);
        Connection original = ConnectionTestAccess::Make(original_state);
        std::atomic<int> result{-1};
        auto task = Task([&] {
            Connection out = original;
            const auto error = CoAccept(engine, out, Until(100ms));
            std::string tail;
            if (error == std::make_error_code(std::errc::operation_not_permitted)
                && out.Valid() && out.Id() == original.Id()
                && out.TryRead(tail).status == ReadStatus::Ok && tail == "original") {
                result.store(0);
            }
        });
        Get(task);
        CHECK(result.load() == 0);
        runner.CloseAndJoin();
    }
    // 4) CoDial 失败（无监听对端）不覆盖调用者已有 Connection 且连接仍可用。
    {
        Engine host{std::shared_ptr<RawHandler>()};
        EngineRunner runner(host, ClientConfig());
        CHECK(WaitUntil([&] { return host.Phase() == EnginePhase::Running; }, 3000ms));
        Address dead;
        auto dead_listener = MakeListener(dead);
        CHECK(!dead_listener.Close());      // 已关闭的监听地址：连接必然失败

        auto original_state = std::make_shared<ConnectionState>(10, 1024, 1024, 4, nullptr);
        CHECK(original_state->ring.TryAppend("kept").status == ReadStatus::Ok);
        Connection original = ConnectionTestAccess::Make(original_state);
        std::atomic<int> result{-1};
        auto task = Task([&] {
            Connection out = original;
            const auto error = CoDial(host, dead, out, Until(500ms));
            std::string tail;
            if (error && out.Valid() && out.Id() == original.Id()
                && out.TryRead(tail).status == ReadStatus::Ok && tail == "kept") {
                result.store(0);
            }
        });
        Get(task);
        CHECK(result.load() == 0);
        runner.CloseAndJoin();
        CHECK(EngineTestAccess::ConnectTableSize(host) == 0);
    }
    std::cout << "output_preservation read/accept/dial=ok\n";
}

// ===========================================================================
// D5 connect_delivery
// ===========================================================================
static void ConnectDelivery() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);

    struct Recorder : RawHandler {
        std::atomic<int> calls{0};
        std::atomic<int> success{0};
        std::atomic<int> failures{0};
        std::atomic<int> last_error_value{0};
        std::atomic<std::uint64_t> last_operation{0};
        std::atomic<bool> callback_before_return{false};
        std::atomic<bool> submit_returned{false};
        std::mutex mutex;
        std::set<std::uint64_t> operations;

        void OnConnect(Connection connection, std::error_code error,
                       std::uint64_t operation) override {
            if (!submit_returned.load()) {
                callback_before_return.store(true);
            }
            {
                std::lock_guard<std::mutex> lock(mutex);
                operations.insert(operation);
            }
            last_operation.store(operation);
            calls.fetch_add(1);
            if (error) {
                failures.fetch_add(1);
                last_error_value.store(error.value());
                return;
            }
            if (connection.Valid()) {
                success.fetch_add(1);
                connection.RequestClose();
            }
        }
    };

    // 1) raw 成功：非零 id、回调恰一次、operation == out_id。
    {
        auto handler = std::make_shared<Recorder>();
        Engine client(handler);
        EngineRunner runner(client, ClientConfig());
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
        std::uint64_t id = 0;
        CHECK(!client.AsyncConnect(address, id));
        CHECK(id != 0);
        handler->submit_returned.store(true);
        CHECK(WaitUntil([&] { return handler->calls.load() == 1; }, 3000ms));
        CHECK(handler->last_operation.load() == id);   // 以回调自带 operation 关联
        CHECK(handler->success.load() == 1);
        CHECK(handler->operations.size() == 1);
        CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 2000ms));
        CHECK(WaitUntil([&] { return EngineTestAccess::ConnectionCount(client) == 0; }, 2000ms));
        runner.CloseAndJoin();
        CHECK(!runner.error);
        std::cout << "connect_delivery raw id=" << id
                  << " callback_before_return=" << (handler->callback_before_return.load() ? 1 : 0)
                  << '\n';
    }

    // 1b) 早到交付交错（确定性）：提交已成功 TryPost 但尚未写 out_id 时，用内部
    //     屏障在提交线程内等 owner 完成 OnConnect 回调；返回后断言回调恰一次、
    //     回调 operation == 最终 out_id、且确实发生在返回之前（不再靠竞速观察）。
    {
        auto handler = std::make_shared<Recorder>();
        Engine client(handler);
        EngineRunner runner(client, ClientConfig());
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
        std::atomic<bool> callback_inside_barrier{false};
        std::atomic<bool> barrier_entered{false};
        EngineTestAccess::SetConnectSubmitBarrierOnce(client, [&] {
            barrier_entered.store(true);
            // 屏障在 TryPost 之后、写 out_id 之前：owner 可自由推进并回调。
            const bool fired = WaitUntil([&] { return handler->calls.load() >= 1; }, 3000ms);
            callback_inside_barrier.store(fired);
        });
        const std::uint64_t sentinel = 0x5a5a;
        std::uint64_t id = sentinel;
        CHECK(!client.AsyncConnect(address, id));
        CHECK(barrier_entered.load());                 // 屏障真的被调用（否则本用例无判别力）
        CHECK(callback_inside_barrier.load());         // 回调确实先于提交返回
        CHECK(handler->callback_before_return.load());
        CHECK(id != 0);                                // 返回时 out_id 已写入
        CHECK(id != sentinel);
        CHECK(WaitUntil([&] { return handler->calls.load() == 1; }, 3000ms));
        CHECK(handler->calls.load() == 1);             // 恰一次（屏障内没有重复派发）
        CHECK(handler->last_operation.load() == id);   // operation == 最终 out_id
        CHECK(handler->success.load() == 1);
        std::lock_guard<std::mutex> lock(handler->mutex);
        CHECK(handler->operations.size() == 1);
        CHECK(*handler->operations.begin() == id);
        std::cout << "connect_delivery early_delivery barrier_entered=1 callback_before_return=1"
                  << " calls=1 operation_matches_out_id=1" << std::endl;
    }
    {
        // 同一屏障的负对照：不武装时回调不在返回前（保证 1b 的"先于返回"不是
        // 由本用例环境天然成立而无法判别）。
        auto handler = std::make_shared<Recorder>();
        Engine client(handler);
        EngineRunner runner(client, ClientConfig());
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
        // 300ms 只为让"回调晚于返回"稳定成立（负对照比对的是先后关系，不是耗时上限）。
        EngineTestAccess::SetConnectDeliveryDelay(client, std::chrono::milliseconds(300));
        std::uint64_t id = 0;
        CHECK(!client.AsyncConnect(address, id));
        CHECK(id != 0);
        handler->submit_returned.store(true);
        CHECK(!handler->callback_before_return.load());   // 负对照：未武装时不会早到
        CHECK(WaitUntil([&] { return handler->calls.load() == 1; }, 3000ms));
        CHECK(handler->last_operation.load() == id);
    }

    // 1c) raw 投递失败（零接纳）：与真实 post 失败同一段回滚——out_id 不变、
    //     零表项、零回调；下一次提交走真实路径并正常交付。
    {
        auto handler = std::make_shared<Recorder>();
        Engine client(handler);
        EngineRunner runner(client, ClientConfig());
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
        EngineTestAccess::SetConnectPostFailureOnce(client);
        const std::uint64_t sentinel = 0x2b2b;
        std::uint64_t id = sentinel;
        CHECK(client.AsyncConnect(address, id)
              == std::make_error_code(std::errc::operation_canceled));
        CHECK(id == sentinel);                                  // 拒绝不改 out_id
        CHECK(EngineTestAccess::ConnectTableSize(client) == 0);
        CHECK(handler->calls.load() == 0);                      // 零回调
        CHECK(client.CountConnections() == 0);
        EngineTestAccess::ClearConnectInjection(client);
        std::uint64_t recovered = 0;
        CHECK(!client.AsyncConnect(address, recovered));
        CHECK(recovered != 0);                                  // 名额/op 槽未被消耗
        CHECK(WaitUntil([&] { return handler->calls.load() == 1; }, 3000ms));
        CHECK(handler->last_operation.load() == recovered);
        CHECK(handler->success.load() == 1);
        CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 2000ms));
        runner.CloseAndJoin();
        CHECK(!runner.error);
    }

    // 1d) raw 在 Idle（从未 Run）阶段提交：明确拒绝、out_id 不变、零回调；
    //     正对照：同一 Engine 正常 Run 后提交成功交付。
    {
        auto handler = std::make_shared<Recorder>();
        Engine client(handler);
        CHECK(client.Phase() == EnginePhase::Idle);
        const std::uint64_t sentinel = 0x3c3c;
        std::uint64_t id = sentinel;
        CHECK(client.AsyncConnect(address, id)
              == std::make_error_code(std::errc::operation_canceled));
        CHECK(id == sentinel);
        CHECK(EngineTestAccess::ConnectTableSize(client) == 0);
        CHECK(handler->calls.load() == 0);
        EngineRunner runner(client, ClientConfig());
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
        std::uint64_t recovered = 0;
        CHECK(!client.AsyncConnect(address, recovered));
        CHECK(recovered != 0);
        CHECK(WaitUntil([&] { return handler->calls.load() == 1; }, 3000ms));
        CHECK(handler->last_operation.load() == recovered);
        CHECK(handler->success.load() == 1);
        std::lock_guard<std::mutex> lock(handler->mutex);
        CHECK(handler->operations.size() == 1);
    }

    // 1e) raw 真实建连失败（对端监听已关闭）：恰一次失败回调，operation 与
    //     out_id 相等，无连接进入 Engine，表项回收。
    {
        auto handler = std::make_shared<Recorder>();
        Engine client(handler);
        EngineRunner runner(client, ClientConfig());
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
        Address dead;
        auto dead_listener = MakeListener(dead);
        CHECK(!dead_listener.Close());
        std::uint64_t id = 0;
        CHECK(!client.AsyncConnect(dead, id));
        CHECK(id != 0);
        handler->submit_returned.store(true);
        CHECK(WaitUntil([&] { return handler->calls.load() >= 1; }, 3000ms));
        // owner 先派发回调、再 ResolveConnect 擦表项：等表项回收即可确定回调动
        // 作已完成，无需"睡一段再看"这种不受控等待。
        CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 3000ms));
        CHECK(handler->calls.load() == 1);          // 恰一次（无重复派发）
        CHECK(handler->failures.load() == 1);
        CHECK(handler->success.load() == 0);
        CHECK(handler->last_operation.load() == id);
        CHECK(handler->last_error_value.load() != 0);
        CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 3000ms));
        CHECK(WaitUntil([&] { return client.CountConnections() == 0; }, 3000ms));
        runner.CloseAndJoin();
        CHECK(!runner.error);
    }

    // 2) 无 handler：零提交拒绝，out_id 原值，无本次回调（无回调通道）。
    {
        auto handler = std::make_shared<Recorder>();
        Engine client(handler);
        EngineRunner runner(client, ClientConfig());
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
        const std::uint64_t sentinel = 0x1234;
        std::uint64_t id = sentinel;
        // 有 handler 时是合法 raw 提交；此处换到无 handler 的 Engine 验证拒绝。
        Engine co_only{std::shared_ptr<RawHandler>()};
        EngineRunner co_runner(co_only, ClientConfig());
        CHECK(WaitUntil([&] { return co_only.Phase() == EnginePhase::Running; }, 3000ms));
        CHECK(co_only.AsyncConnect(address, id)
              == std::make_error_code(std::errc::operation_not_supported));
        CHECK(id == sentinel);
        CHECK(EngineTestAccess::ConnectTableSize(co_only) == 0);
        CHECK(handler->calls.load() == 0);
        // 零提交是同步结论：直接断言，不用 sleep 当观察窗口。
        CHECK(co_only.CountConnections() == 0);
        CHECK(EngineTestAccess::ConnectTableSize(co_only) == 0);
        runner.CloseAndJoin();
        co_runner.CloseAndJoin();
    }

    // 3) 已封口 Engine：拒绝、out_id 不变、无回调。
    {
        auto handler = std::make_shared<Recorder>();
        Engine client(handler);
        EngineRunner runner(client, ClientConfig());
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
        client.RequestClose();
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Closing
                                   || client.Phase() == EnginePhase::Stopped; }, 3000ms));
        const std::uint64_t sentinel = 0x77;
        std::uint64_t id = sentinel;
        CHECK(client.AsyncConnect(address, id)
              == std::make_error_code(std::errc::operation_canceled));
        CHECK(id == sentinel);
        CHECK(handler->calls.load() == 0);
        runner.CloseAndJoin();
    }

    // 4) 容量满：零接纳、out_id 不变、不产生该次回调。
    {
        auto handler = std::make_shared<Recorder>();
        Engine client(handler);
        EngineRunner runner(client, ClientConfig());
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
        EngineTestAccess::SetConnectLimit(client, 1);
        EngineTestAccess::SetConnectDeliveryDelay(client, std::chrono::milliseconds(300));
        std::uint64_t first = 0;
        CHECK(!client.AsyncConnect(address, first));
        CHECK(first != 0);
        const std::uint64_t sentinel = 0x99;
        std::uint64_t second = sentinel;
        CHECK(client.AsyncConnect(address, second)
              == std::make_error_code(std::errc::no_buffer_space));
        CHECK(second == sentinel);
        CHECK(WaitUntil([&] { return handler->calls.load() == 1; }, 3000ms));
        CHECK(handler->last_operation.load() == first);
        CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 2000ms));
        runner.CloseAndJoin();
    }

    // 5) CoDial 矩阵：无 handler 的 client-only 与 server 都支持；有 handler 拒绝。
    {
        // client-only（无 listen）Engine。
        Engine co_client{std::shared_ptr<RawHandler>()};
        EngineRunner client_runner(co_client, ClientConfig());
        CHECK(WaitUntil([&] { return co_client.Phase() == EnginePhase::Running; }, 3000ms));
        std::atomic<int> client_result{-1};
        auto task = Task([&] {
            Connection out;
            if (!CoDial(co_client, address, out, Until(3000ms)) && out.Valid()) {
                client_result.store(0);
                out.RequestClose();
            }
        });
        Get(task);
        CHECK(client_result.load() == 0);
        client_runner.CloseAndJoin();

        // server 布局（有 listener）+ handoff，无 handler：CoDial 也支持。
        Engine co_server{std::shared_ptr<RawHandler>()};
        CHECK(!co_server.EnableAcceptHandoff(4));
        EngineRunner server_runner(co_server, ServerConfig(Loopback(0), 1));
        CHECK(WaitUntil([&] { return co_server.Phase() == EnginePhase::Running; }, 3000ms));
        std::atomic<int> server_result{-1};
        auto server_task = Task([&] {
            Connection out;
            if (!CoDial(co_server, address, out, Until(3000ms)) && out.Valid()) {
                server_result.store(0);
                out.RequestClose();
            }
        });
        Get(server_task);
        CHECK(server_result.load() == 0);
        server_runner.CloseAndJoin();

        // 有 handler 的 Engine：CoDial 在任何提交前拒绝。
        auto raw_handler = std::make_shared<Recorder>();
        Engine raw_client(raw_handler);
        EngineRunner raw_runner(raw_client, ClientConfig());
        CHECK(WaitUntil([&] { return raw_client.Phase() == EnginePhase::Running; }, 3000ms));
        std::atomic<int> rejected{-1};
        auto reject_task = Task([&] {
            Connection out;
            const auto error = CoDial(raw_client, address, out, Until(1000ms));
            if (error == std::make_error_code(std::errc::operation_not_supported) && !out.Valid()) {
                rejected.store(0);
            }
        });
        Get(reject_task);
        CHECK(rejected.load() == 0);
        CHECK(EngineTestAccess::ConnectTableSize(raw_client) == 0);  // 零提交
        CHECK(raw_handler->calls.load() == 0);
        raw_runner.CloseAndJoin();
    }

    Address peer;
    std::error_code ignored;
    while (listener.Accept(peer, ignored).Fd() >= 0) {}
    std::cout << "connect_delivery raw/nohandler/sealed/cap/codial_matrix=ok\n";
}

// ===========================================================================
// D5 connect_failure_cleanup
// ===========================================================================
static void ConnectFailureCleanup() {
    StartRuntime(2);
    Address address;
    auto listener = MakeListener(address);

    // 1) 等待对象建立失败 → not_enough_memory；同 Engine 仍可正常建连，且失败
    //    退出路径不遗留 fd（首轮含 Asio 上下文惰性建立，故按"轮次间不增长"判定）。
    {
        Engine client{std::shared_ptr<RawHandler>()};
        EngineRunner runner(client, ClientConfig());
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
        EngineTestAccess::SetConnectDeliveryDelay(client, std::chrono::milliseconds(200));
        auto fail_cycle = [&] {
            EngineTestAccess::FailNextConnectWaiterCreateOnce(client);
            std::atomic<int> value{-2};
            auto task = Task([&] {
                Connection out;
                value.store(CoDial(client, address, out, Until(1000ms)).value());
            });
            Get(task);
            CHECK(value.load() == std::make_error_code(std::errc::not_enough_memory).value());
            // 未交付路径必须归还 claim/连接责任：表项回收、连接回收。
            CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 3000ms));
            CHECK(WaitUntil([&] { return EngineTestAccess::ConnectionCount(client) == 0; }, 3000ms));
        };
        fail_cycle();                                   // 首轮：含上下文惰性 fd
        const auto fds_after_first = CountFds();
        fail_cycle();                                   // 次轮：fd 不得增长
        CHECK(WaitUntil([&] { return CountFds() <= fds_after_first; }, 3000ms));
        std::atomic<int> recovered{-2};
        auto recovered_task = Task([&] {
            Connection out;
            if (!CoDial(client, address, out, Until(3000ms)) && out.Valid()) {
                recovered.store(0);
                out.RequestClose();
            }
        });
        Get(recovered_task);
        CHECK(recovered.load() == 0);
        runner.CloseAndJoin();
        CHECK(!runner.error);
    }

    // 2) 等待登记满额（hub 256）→ no_buffer_space；owner 稍后交付必须被回收。
    {
        Engine client{std::shared_ptr<RawHandler>()};
        EngineRunner runner(client, ClientConfig());
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
        EngineTestAccess::SetConnectDeliveryDelay(client, std::chrono::milliseconds(2000));
        std::uint64_t id = 0;
        CHECK(!CoAccess::SubmitConnect(client, address, id));
        std::vector<std::shared_ptr<bbt::coroutine::sync::CoWaiter>> keep;
        CHECK(EngineTestAccess::FillConnectHub(client, id, keep) == WakeHub::kMaxWaiters);
        std::atomic<int> full{-2};
        auto task = Task([&] {
            Connection out;
            full.store(CoAccess::WaitConnect(client, id, out, Until(2000ms)).value());
            if (out.Valid()) {
                out.RequestClose();
            }
        });
        Get(task);
        CHECK(full.load() == std::make_error_code(std::errc::no_buffer_space).value());
        CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 4000ms));
        CHECK(WaitUntil([&] { return EngineTestAccess::ConnectionCount(client) == 0; }, 4000ms));
        runner.CloseAndJoin();
        CHECK(!runner.error);
    }

    // 3) 超时：迟到交付被回收，fd 不增长，第二轮正常。
    {
        Engine client{std::shared_ptr<RawHandler>()};
        EngineRunner runner(client, ClientConfig());
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
        EngineTestAccess::SetConnectDeliveryDelay(client, std::chrono::milliseconds(200));
        std::atomic<int> timed_out{-2};
        auto timeout_task = Task([&] {
            Connection out;
            timed_out.store(CoDial(client, address, out, Until(30ms)).value());
        });
        Get(timeout_task);
        CHECK(timed_out.load() == std::make_error_code(std::errc::timed_out).value());
        CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 3000ms));
        CHECK(WaitUntil([&] { return EngineTestAccess::ConnectionCount(client) == 0; }, 3000ms));
        const auto fds_after = CountFds();
        std::atomic<int> recovered{-2};
        auto recovered_task = Task([&] {
            Connection out;
            if (!CoDial(client, address, out, Until(3000ms)) && out.Valid()) {
                recovered.store(0);
                out.RequestClose();
            }
        });
        Get(recovered_task);
        CHECK(recovered.load() == 0);
        CHECK(WaitUntil([&] { return CountFds() <= fds_after; }, 3000ms));
        runner.CloseAndJoin();
        CHECK(!runner.error);
    }

    // 4) 取消投递失败（单次）：责任持久化到控制循环，pending 早于 5s 撤销。
    {
        Engine client{std::shared_ptr<RawHandler>()};
        EngineRunner runner(client, ClientConfig());
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
        EngineTestAccess::SetConnectStartDelay(client, std::chrono::milliseconds(5000));
        EngineTestAccess::SetCancelPostFailureOnce(client);
        std::atomic<int> result{-2};
        auto task = Task([&] {
            Connection out;
            result.store(CoDial(client, address, out, Until(30ms)).value());
        });
        Get(task);
        CHECK(result.load() == std::make_error_code(std::errc::timed_out).value());
        CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 3000ms));
        CHECK(EngineTestAccess::PendingConnectCount(client) == 0);
        runner.CloseAndJoin();
        CHECK(!runner.error);
    }

    // 5) 已有 raw 已交付连接不受 Co 失败影响：失败后该连接仍可收发/可读。
    {
        struct RawDelivery : RawHandler {
            std::atomic<int> connects{0};
            std::atomic<int> closes{0};
            std::mutex mutex;
            std::shared_ptr<Connection> stored;
            void OnConnect(Connection connection, std::error_code error,
                           std::uint64_t) override {
                if (error || !connection.Valid()) {
                    return;
                }
                connects.fetch_add(1);
                std::lock_guard<std::mutex> lock(mutex);
                stored = std::make_shared<Connection>(connection);
            }
            void OnClose(Connection, std::error_code) override { closes.fetch_add(1); }
        };
        auto handler = std::make_shared<RawDelivery>();
        Engine client(handler);
        EngineRunner runner(client, ClientConfig());
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
        std::uint64_t id = 0;
        CHECK(!client.AsyncConnect(address, id));
        CHECK(WaitUntil([&] { return handler->connects.load() == 1; }, 3000ms));
        std::shared_ptr<Connection> delivered;
        {
            std::lock_guard<std::mutex> lock(handler->mutex);
            delivered = handler->stored;
        }
        CHECK(delivered && delivered->Valid());
        // Co 侧同 Engine 一律拒绝（raw 唯一交付），且不影响已交付连接。
        std::atomic<int> rejected{-1};
        auto task = Task([&] {
            Connection out;
            const auto error = CoDial(client, address, out, Until(1000ms));
            if (error == std::make_error_code(std::errc::operation_not_supported)) {
                rejected.store(0);
            }
        });
        Get(task);
        CHECK(rejected.load() == 0);
        WriteOperation write;
        CHECK(!delivered->AsyncWrite("alive", write));
        CHECK(write.Valid());
        CHECK(delivered->WriteAcceptance() != AcceptanceState::Closed);
        CHECK(handler->closes.load() == 0);
        delivered->RequestClose();
        CHECK(WaitUntil([&] { return handler->closes.load() == 1; }, 3000ms));
        runner.CloseAndJoin();
        CHECK(!runner.error);
    }

    // 6) 等待返回非 Completed 的三个真实退出路径（InvalidContext / RuntimeUnavailable
    //    / AlreadyWaiting）：在等待返回边界注入状态，op 仍 pending，走产品既有
    //    abandon(true) 归还 claim + 取消投递 + owner 回收；断言实际 CoDial/领取
    //    返回 errc、out 不变、无 raw 误关、表项/名额/fd/连接回热基线，之后正常
    //    CoDial 可用。每条都有正对照（注入前 pending、注入后同上 Engine 成功）。
    {
        using bbt::coroutine::WaitStatus;
        struct Injected {
            WaitStatus status;
            std::error_code expected;
            const char* label;
        };
        const Injected cases[] = {
            {WaitStatus::InvalidContext,
             std::make_error_code(std::errc::operation_not_permitted), "invalid_context"},
            {WaitStatus::RuntimeUnavailable,
             std::make_error_code(std::errc::operation_not_supported), "runtime_unavailable"},
            {WaitStatus::AlreadyWaiting,
             std::make_error_code(std::errc::device_or_resource_busy), "already_waiting"},
        };
        for (const auto& item : cases) {
            // 本段的 listener 只是"有对端可连"：每轮先取走 backlog，避免多轮内核
            // 拒绝新连接而让本条因环境而非契约失败。
            auto drain_listener = [&listener] {
                Address peer;
                std::error_code ignored;
                while (listener.Accept(peer, ignored).Fd() >= 0) {}
                (void)peer;
            };
            drain_listener();
            // 无 handler（Co-only）Engine：raw 唯一交付通道不存在，任何"误关/误回调"
            // 都只能表现为连接数/表项/fd 异常。
            Engine client{std::shared_ptr<RawHandler>()};
            EngineRunner runner(client, ClientConfig());
            CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
            // 正对照：无注入时同一路径真实成功。
            {
                std::atomic<int> baseline{-1};
                auto warm = Task([&] {
                    Connection out;
                    if (!CoDial(client, address, out, Until(3000ms)) && out.Valid()) {
                        baseline.store(0);
                        out.RequestClose();
                    }
                });
                Get(warm);
                CHECK(baseline.load() == 0);
                CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; },
                                3000ms));
            }
            const auto fds_baseline = CountFds();

            // 让 op 在 owner 侧保持 pending（尚未建 socket），注入点才落在"未完成"分支。
            EngineTestAccess::SetConnectStartDelay(client, std::chrono::milliseconds(5000));
            std::uint64_t id = 0;
            CHECK(!CoAccess::SubmitConnect(client, address, id));
            CHECK(id != 0);
            CHECK(WaitUntil([&] { return EngineTestAccess::PendingConnectCount(client) == 1; },
                            3000ms));
            EngineTestAccess::SetConnectWaitStatusOnce(client, static_cast<int>(item.status));

            auto original_state = std::make_shared<ConnectionState>(97, 1024, 1024, 4, nullptr);
            CHECK(original_state->ring.TryAppend("held").status == ReadStatus::Ok);
            Connection original = ConnectionTestAccess::Make(original_state);
            std::atomic<int> observed{-99};
            std::atomic<int> out_kept{-1};
            auto task = Task([&] {
                Connection out = original;
                const auto error = CoAccess::WaitConnect(client, id, out, Until(3000ms));
                observed.store(error.value());
                std::string tail;
                if (out.Valid() && out.Id() == original.Id()
                    && out.TryRead(tail).status == ReadStatus::Ok && tail == "held") {
                    out_kept.store(0);
                }
            });
            Get(task);
            CHECK(observed.load() == item.expected.value());   // 实际返回的 errc
            CHECK(out_kept.load() == 0);                       // 未交付失败不覆盖 out
            // 责任回收：claim 已归还、op 表项清空、无遗留 pending、无连接进入 Engine。
            CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 4000ms));
            CHECK(EngineTestAccess::PendingConnectCount(client) == 0);
            CHECK(WaitUntil([&] { return client.CountConnections() == 0; }, 3000ms));
            CHECK(WaitUntil([&] { return CountFds() <= fds_baseline; }, 3000ms));  // 无 fd 泄漏/误关
            CHECK(client.Phase() == EnginePhase::Running);

            // 同一状态再经公开 CoDial 走一遍：这是"实际 CoDial 返回 errc"的证据，
            // 与上面"先 pending 再失败"的内部两段式互为补充，不互相替代。
            EngineTestAccess::SetConnectWaitStatusOnce(client, static_cast<int>(item.status));
            // 上一段已消费过 original 的字节，这里换一条新的同款连接做 out 保留证据。
            auto dial_state = std::make_shared<ConnectionState>(197, 1024, 1024, 4, nullptr);
            CHECK(dial_state->ring.TryAppend("held-dial").status == ReadStatus::Ok);
            Connection dial_original = ConnectionTestAccess::Make(dial_state);
            std::atomic<int> dial_errc{-99};
            std::atomic<int> dial_out_kept{-1};
            auto dial_task = Task([&] {
                Connection out = dial_original;
                const auto error = CoDial(client, address, out, Until(3000ms));
                dial_errc.store(error.value());
                std::string tail;
                if (out.Valid() && out.Id() == dial_original.Id()
                    && out.TryRead(tail).status == ReadStatus::Ok && tail == "held-dial") {
                    dial_out_kept.store(0);
                }
            });
            Get(dial_task);
            CHECK(dial_errc.load() == item.expected.value());   // CoDial 实际 errc
            CHECK(dial_out_kept.load() == 0);                   // CoDial 也保留原 out
            CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 4000ms));
            CHECK(EngineTestAccess::PendingConnectCount(client) == 0);
            CHECK(client.CountConnections() == 0);              // 未交付 fd 被回收，无误留
            CHECK(WaitUntil([&] { return CountFds() <= fds_baseline; }, 3000ms));

            // 同一 Engine 解除注入后正常 CoDial：名额/fd/连接都回到可用基线。
            // 起始延迟是持久设置（非 once），恢复前必须归零，否则下一次仍是 pending。
            EngineTestAccess::ClearConnectInjection(client);
            EngineTestAccess::SetConnectStartDelay(client, std::chrono::milliseconds(0));
            std::atomic<int> recovered{-1};
            auto recovered_task = Task([&] {
                Connection out;
                if (!CoDial(client, address, out, Until(3000ms)) && out.Valid()) {
                    recovered.store(0);
                    out.RequestClose();
                }
            });
            Get(recovered_task);
            CHECK(recovered.load() == 0);
            CHECK(WaitUntil([&] { return EngineTestAccess::ConnectTableSize(client) == 0; }, 3000ms));
            CHECK(WaitUntil([&] { return CountFds() <= fds_baseline + 1; }, 3000ms));
            runner.CloseAndJoin();
            CHECK(!runner.error);
            std::cout << "connect_failure_cleanup wait_status_injected=" << item.label
                      << " errc=" << observed.load() << " codial_errc=" << dial_errc.load()
                      << " out_kept=1 recovered=1" << std::endl;
        }
    }

    // 7) WaitStatus 逐项映射：上游该 SHA 的 6 个成员各自映射到明确 errc，
    //    不允许任何成员落到 io_error（旧实现的默认项）。上一段已用内部接缝在
    //    运行期真实制造 InvalidContext/RuntimeUnavailable/AlreadyWaiting；本段
    //    只锁纯映射表（含 TimedOut/Cancelled/Completed），两者不可互相替代。
    {
        using bbt::coroutine::WaitStatus;
        struct Mapping {
            WaitStatus status;
            std::error_code expected;
        };
        const Mapping mappings[] = {
            {WaitStatus::Completed, std::error_code{}},
            {WaitStatus::TimedOut, std::make_error_code(std::errc::timed_out)},
            {WaitStatus::Cancelled, std::make_error_code(std::errc::operation_canceled)},
            {WaitStatus::InvalidContext, std::make_error_code(std::errc::operation_not_permitted)},
            {WaitStatus::AlreadyWaiting, std::make_error_code(std::errc::device_or_resource_busy)},
            {WaitStatus::RuntimeUnavailable,
             std::make_error_code(std::errc::operation_not_supported)},
        };
        for (const auto& item : mappings) {
            const auto mapped = WaitStatusError(item.status);
            CHECK(mapped == item.expected);
            CHECK(mapped != std::make_error_code(std::errc::io_error));
        }
        std::cout << "connect_failure_cleanup wait_status_mapping entries="
                  << (sizeof(mappings) / sizeof(mappings[0])) << "\n";
    }

    Address peer;
    std::error_code ignored;
    while (listener.Accept(peer, ignored).Fd() >= 0) {}
    std::cout << "connect_failure_cleanup create/hub/timeout/cancel/raw_intact=ok\n";
}

// ===========================================================================
// D6 accept_configuration
// ===========================================================================
static void AcceptConfiguration() {
    StartRuntime(3);

    // 1) server 布局 + 无 handler + 未启用 handoff：Run 在绑定前拒绝。
    {
        const auto fds_before = CountFds();
        Engine engine{std::shared_ptr<RawHandler>()};
        EngineConfig config = ServerConfig(Loopback(0), 1);
        const auto error = engine.Run(config);
        CHECK(error == std::make_error_code(std::errc::operation_not_permitted));
        CHECK(engine.Phase() == EnginePhase::Stopped);
        std::vector<Address> addresses;
        CHECK(engine.LocalAddresses(addresses)
              == std::make_error_code(std::errc::operation_not_permitted));
        CHECK(CountFds() == fds_before);      // 零 socket 副作用
    }

    // 2) CoAccept 阶段/配置精确 errc 矩阵。每条都断言：精确 errc、快速拒绝（不占满
    //    deadline）、失败不覆盖调用者原有 out（原连接仍可读自己的字节）。
    //    3) 与 4/4b) 的具体阶段见各段注释。
    auto expect_accept_rejection = [](Engine& engine, std::error_code expected,
                                      const char* label, std::uint64_t state_id) {
        auto original_state =
            std::make_shared<ConnectionState>(state_id, 1024, 1024, 4, nullptr);
        CHECK(original_state->ring.TryAppend("accept-kept").status == ReadStatus::Ok);
        Connection original = ConnectionTestAccess::Make(original_state);
        const auto begin = std::chrono::steady_clock::now();
        std::atomic<int> errc{0};
        std::atomic<int> kept{-1};
        auto task = Task([&] {
            Connection out = original;
            const auto error = CoAccept(engine, out, Until(3000ms));
            errc.store(error.value());
            std::string tail;
            if (out.Valid() && out.Id() == original.Id()
                && out.TryRead(tail).status == ReadStatus::Ok && tail == "accept-kept") {
                kept.store(0);
            }
        });
        Get(task);
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - begin).count();
        CHECK(errc.load() == expected.value());
        CHECK(kept.load() == 0);          // 原 out 保留且原连接未受影响
        CHECK(elapsed < 1000);            // 快速拒绝，不占满 deadline
        std::cout << "accept_configuration reject=" << label
                  << " errc=" << errc.load() << " fast=1 out_kept=1" << std::endl;
    };

    // 2a) Idle（从未 Run）：尚未建立 accept 生产者 → not_permitted。
    {
        Engine engine{std::shared_ptr<RawHandler>()};
        CHECK(!engine.EnableAcceptHandoff(2));
        CHECK(engine.Phase() == EnginePhase::Idle);
        expect_accept_rejection(engine, std::make_error_code(std::errc::operation_not_permitted),
                                "idle", 101);
    }

    // 2b) Starting（Run 已发布 Starting、owner 尚未安装完）：not_permitted；
    //     用 install 延迟把 Starting 窗口固定住，之后转 Running 并正常收口。
    {
        auto handler = std::make_shared<NullHandler>();
        Engine engine(handler);
        CHECK(!engine.EnableAcceptHandoff(2));
        // 800ms：把 Starting 窗口拉宽到足够覆盖一次协程调度，避免落到安装完成之后。
        EngineTestAccess::SetInstallDelay(engine, std::chrono::milliseconds(800));
        EngineRunner runner(engine, ServerConfig(Loopback(0), 1));
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Starting; }, 3000ms));
        expect_accept_rejection(engine, std::make_error_code(std::errc::operation_not_permitted),
                                "starting", 102);
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
        runner.CloseAndJoin();
    }

    // 2c) Running server + handler，未开启 handoff：not_permitted（配置缺失）。
    {
        auto handler = std::make_shared<NullHandler>();
        Engine engine(handler);
        EngineRunner runner(engine, ServerConfig(Loopback(0), 1));
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
        expect_accept_rejection(engine, std::make_error_code(std::errc::operation_not_permitted),
                                "running_without_handoff", 103);
        runner.CloseAndJoin();
    }

    // 2d) Running client-only（有 handoff 无 main owner）：not_supported。
    {
        Engine engine{std::shared_ptr<RawHandler>()};
        CHECK(!engine.EnableAcceptHandoff(2));
        EngineRunner runner(engine, ClientConfig());
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
        expect_accept_rejection(engine, std::make_error_code(std::errc::operation_not_supported),
                                "client_only", 104);
        runner.CloseAndJoin();
    }

    // 4) 已封口（Closing，main owner 仍在）：operation_canceled。
    //    用 seal 延迟把 Closing 窗口固定住，避免与收口竞争。
    {
        auto handler = std::make_shared<NullHandler>();
        Engine engine(handler);
        CHECK(!engine.EnableAcceptHandoff(2));
        EngineRunner runner(engine, ServerConfig(Loopback(0), 1));
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
        EngineTestAccess::SetSealDelay(engine, std::chrono::milliseconds(400));
        engine.RequestClose();
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Closing; }, 3000ms));
        expect_accept_rejection(engine, std::make_error_code(std::errc::operation_canceled),
                                "closing", 105);
        runner.CloseAndJoin();
    }

    // 4b) 完全 Stopped（main owner 已解绑）：终态优先 → operation_canceled，
    //     不得因"没有 main owner"被误判成 client-only 的 not_supported。
    {
        auto handler = std::make_shared<NullHandler>();
        Engine engine(handler);
        CHECK(!engine.EnableAcceptHandoff(2));
        EngineRunner runner(engine, ServerConfig(Loopback(0), 1));
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
        engine.RequestClose();
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Stopped; }, 3000ms));
        expect_accept_rejection(engine, std::make_error_code(std::errc::operation_canceled),
                                "stopped", 106);
        runner.CloseAndJoin();
    }

    // 5) handler + handoff 共存：同一连接既进 OnAccept 也进 handoff，Id 相同。
    {
        struct CountingHandler : RawHandler {
            std::atomic<int> accepted{0};
            std::mutex mutex;
            std::vector<std::uint64_t> ids;
            void OnAccept(Connection connection) override {
                accepted.fetch_add(1);
                std::lock_guard<std::mutex> lock(mutex);
                ids.push_back(connection.Id());
            }
        };
        auto handler = std::make_shared<CountingHandler>();
        Engine engine(handler);
        CHECK(!engine.EnableAcceptHandoff(4));
        EngineRunner runner(engine, ServerConfig(Loopback(0), 1));
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
        std::vector<Address> addresses;
        CHECK(!engine.LocalAddresses(addresses));
        auto client = ConnectClient(addresses[0]);
        CHECK(WaitUntil([&] { return handler->accepted.load() == 1; }, 3000ms));
        std::atomic<std::uint64_t> handoff_id{0};
        std::atomic<int> result{-1};
        auto task = Task([&] {
            Connection out;
            if (!CoAccept(engine, out, Until(3000ms)) && out.Valid()) {
                handoff_id.store(out.Id());
                result.store(0);
                out.RequestClose();
            }
        });
        Get(task);
        CHECK(result.load() == 0);
        std::uint64_t raw_id = 0;
        {
            std::lock_guard<std::mutex> lock(handler->mutex);
            CHECK(handler->ids.size() == 1);
            raw_id = handler->ids.front();
        }
        CHECK(raw_id != 0);
        CHECK(handoff_id.load() == raw_id);    // 同一连接、两个交付者
        (void)client.Close();
        runner.CloseAndJoin();
        CHECK(!runner.error);
    }

    // 6) 队列满后恢复：容量 1，先 reserve 再取，依次交付 3 条。
    {
        auto handler = std::make_shared<NullHandler>();
        Engine engine(handler);
        CHECK(!engine.EnableAcceptHandoff(1));
        EngineRunner runner(engine, ServerConfig(Loopback(0), 1));
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
        std::vector<Address> addresses;
        CHECK(!engine.LocalAddresses(addresses));
        std::vector<NativeSocket> clients;
        for (int i = 0; i < 3; ++i) {
            clients.push_back(ConnectClient(addresses[0]));
        }
        std::atomic<int> accepted{0};
        auto task = Task([&] {
            for (int i = 0; i < 3; ++i) {
                Connection connection;
                if (CoAccept(engine, connection, Until(3000ms)) || !connection.Valid()) {
                    return;
                }
                accepted.fetch_add(1);
                connection.RequestClose();
            }
        });
        Get(task);
        CHECK(accepted.load() == 3);
        runner.CloseAndJoin();
        CHECK(!runner.error);
        for (auto& item : clients) {
            (void)item.Close();
        }
    }

    // 7) 已有 out 有效但本轮无 handoff：内部等待必须按本轮结果判定（超时），
    //    不能因 out 已有效而误报成功。
    {
        auto handler = std::make_shared<NullHandler>();
        Engine engine(handler);
        CHECK(!engine.EnableAcceptHandoff(4));
        EngineRunner runner(engine, ServerConfig(Loopback(0), 1));
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
        std::vector<Address> addresses;
        CHECK(!engine.LocalAddresses(addresses));
        auto client = ConnectClient(addresses[0]);
        std::atomic<int> result{-1};
        auto task = Task([&] {
            Connection out;
            if (CoAccept(engine, out, Until(3000ms)) || !out.Valid()) {
                return;
            }
            const auto second = CoAccess::WaitAccepted(engine, out, Until(40ms));
            if (second == std::make_error_code(std::errc::timed_out)) {
                result.store(0);
            }
        });
        Get(task);
        CHECK(result.load() == 0);
        (void)client.Close();
        runner.CloseAndJoin();
    }

    std::cout << "accept_configuration run_gate/phase_matrix/coexist/full/resume=ok\n";
}

#include "ProcessRuntimeTestMain.hpp"

static void DumpBacktrace(int signal_number) {
    void* frames[64];
    const int count = backtrace(frames, 64);
    std::cerr << "caught signal " << signal_number << '\n';
    backtrace_symbols_fd(frames, count, 2);
    std::_Exit(139);
}

static int RunCases(int argc, char** argv) {
    try {
        CHECK(argc == 2);
        const std::string test = argv[1];
        if (test == "guard_lifetime") { GuardLifetime(); }
        else if (test == "co_context_gate") { CoContextGate(); }
        else if (test == "options_contract") { OptionsContract(); }
        else if (test == "write_output_reuse") { WriteOutputReuse(); }
        else if (test == "output_preservation") { OutputPreservation(); }
        else if (test == "connect_delivery") { ConnectDelivery(); }
        else if (test == "connect_failure_cleanup") { ConnectFailureCleanup(); }
        else if (test == "accept_configuration") { AcceptConfiguration(); }
        else { throw std::runtime_error("unknown test: " + test); }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}

int main(int argc, char** argv) {
    signal(SIGSEGV, DumpBacktrace);
    signal(SIGABRT, DumpBacktrace);
    return bbt_net_test::RunTestProcessExit([&] { return RunCases(argc, argv); });
}
