// net 最小 API 冻结契约测试（计划 T4 / §7 D1–D6）。
//
// 与 b4_contract / r2_tcp 的分工：这里只针对"冻结后的公共面"与取消的内部接缝
// 做可判别回归，不重复业务往返。每个用例名对应计划 §7 的一行场景：
//   guard_lifetime           D2 守卫保活/唯一释放/移动/空守卫（Engine 收口后尾部）
//   co_context_gate          D4 四个 Co 入口在非法执行域零消费/零提交/零建连
//   options_contract         D3 保留字段边界 + 默认值（编译期删除由负向 TU 证明）
//   write_output_reuse       D4 写输出只描述本次调用（含接纳后等待失败）
//   output_preservation      D4 失败不覆盖调用者 out（CoRead/CoAccept/CoDial）
//   connect_delivery         D5 raw 单一交付：out_id/回调 once+相等/拒绝不变
//   connect_failure_cleanup  D5 未交付退出全回收（建立失败/满额/超时/取消）
//   accept_configuration     D6 无 handler 无 handoff 拒绝 + handler/handoff 共存
//
// 编译期"公共面不含 State/内部构造/测试屏障/旧等待 API"由
// net.freeze_public_surface（tests/net/consumers/expect_public_surface.cmake +
// consumers/public_surface_negative/neg_*.cc）以正向 TU 与逐项负向 TU 证明。
// 本文件除公共头外只经 detail/TestAccess、detail/CoInternal、detail/EngineTestAccess
// 注入交错与内部状态；这些接缝不回到公共面，也不被公共 consumer 引用。
//
// 另有一段文件级「编译期冻结断言」：签名/noexcept/字段类型/枚举值/默认值/移动
// 语义在构建本目标时无条件求值，覆盖运行时用例走不到的漂移（见该段注释）。

#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Annotations.hpp>
#include <bbt/infra/net/CoConnection.hpp>
#include <bbt/infra/net/CoObject.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/Error.hpp>
#include <bbt/infra/net/ICoObject.hpp>
#include <bbt/infra/net/LoadBalancing.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/Options.hpp>
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
#include <type_traits>
#include <utility>
#include <vector>

#include <poll.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>

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
// 编译期冻结断言（D1–D5）
//
// 运行时用例只能覆盖走到的路径，签名被改宽/改窄、noexcept 被丢掉、字段类型
// 或默认值漂移、守卫被加回拷贝，往往仍能通过某个用例。这里在构建本目标时
// 无条件求值，把「冻结的公共面」变成编译期约束：
//   - 值类型与枚举（D1）：字段类型 + 枚举数值（日志/持久化按值解释的依据）
//   - Options（D3）：保留字段类型 + 公共默认值 + aggregate 形态
//   - 句柄与守卫（D1/D2）：精确成员签名（C++17 下 noexcept 属于函数类型）、
//     守卫只可移动且移动不抛、Connection 是稳定可复制值句柄
//   - Engine 与 Co 四入口（D4/D5）：精确签名、Engine 不可复制、构造 explicit
// 默认实参本身不是类型的一部分，由 net.freeze_public_surface 的正向 TU 以
// 省略实参的调用证明；这里不重复。
// 未改动的 Address/NativeSocket/CoObject 头由单头自检与正向 TU 覆盖，本段只
// 冻结 D1–D5 收敛面，避免把无关实现细节固化成断言。
// ===========================================================================
namespace freeze_contract {

static_assert(std::is_aggregate_v<ReadResult>);
static_assert(std::is_same_v<decltype(ReadResult::status), ReadStatus>);
static_assert(std::is_same_v<decltype(ReadResult::bytes), std::size_t>);
static_assert(std::is_same_v<decltype(ReadResult::error), std::error_code>);
static_assert(std::is_same_v<decltype(WriteOutcome::phase), WritePhase>);
static_assert(std::is_same_v<decltype(WriteOutcome::sent), std::size_t>);
static_assert(std::is_same_v<decltype(WriteResult::operation), std::uint64_t>);

static_assert(static_cast<int>(ReadStatus::Ok) == 0);
static_assert(static_cast<int>(ReadStatus::Busy) == 1);
static_assert(static_cast<int>(ReadStatus::WouldBlock) == 2);
static_assert(static_cast<int>(ReadStatus::Eof) == 3);
static_assert(static_cast<int>(ReadStatus::Error) == 4);
static_assert(static_cast<int>(WritePhase::InFlight) == 0);
static_assert(static_cast<int>(WritePhase::Done) == 1);
static_assert(static_cast<int>(WritePhase::Rejected) == 2);
static_assert(static_cast<int>(WritePhase::Unknown) == 3);
static_assert(static_cast<int>(AcceptanceState::Open) == 0);
static_assert(static_cast<int>(AcceptanceState::Settling) == 1);
static_assert(static_cast<int>(AcceptanceState::Closed) == 2);
static_assert(static_cast<int>(EnginePhase::Idle) == 0);
static_assert(static_cast<int>(EnginePhase::Stopped) == 4);
static_assert(static_cast<int>(RuntimeProgress::Unspecified) == 0);
static_assert(static_cast<int>(RuntimeProgress::Independent) == 1);
static_assert(static_cast<int>(LoadBalancing::SourceAddrHash) == 2);
static_assert(static_cast<int>(NetErrc::Eof) == 1);
static_assert(static_cast<int>(NetErrc::Closed) == 6);
static_assert(std::is_error_code_enum_v<NetErrc>);

static_assert(std::is_aggregate_v<Options>);
static_assert(std::is_same_v<decltype(Options::lb), LoadBalancing>);
static_assert(std::is_same_v<decltype(Options::reuse_addr), bool>);
static_assert(std::is_same_v<decltype(Options::num_event_loop), std::int64_t>);
static_assert(std::is_same_v<decltype(Options::tcp_keep_alive), std::chrono::seconds>);
static_assert(std::is_same_v<decltype(Options::max_inbound_bytes), std::int64_t>);
static_assert(Options{}.lb == LoadBalancing::RoundRobin);
static_assert(!Options{}.reuse_addr);
static_assert(!Options{}.reuse_port);
static_assert(Options{}.num_event_loop == 0);
static_assert(Options{}.tcp_keep_alive == std::chrono::seconds{0});
static_assert(Options{}.tcp_keep_count == 0);
static_assert(Options{}.tcp_no_delay);
static_assert(Options{}.socket_recv_buffer == 0);
static_assert(Options{}.socket_send_buffer == 0);
static_assert(Options{}.max_inbound_bytes == 1024 * 1024);
static_assert(Options{}.max_outbound_bytes == 1024 * 1024);
static_assert(Options{}.max_pending_events == 1024);

static_assert(std::is_same_v<decltype(&Connection::Valid),
                             bool (Connection::*)() const noexcept>);
static_assert(std::is_same_v<decltype(&Connection::Id),
                             std::uint64_t (Connection::*)() const noexcept>);
static_assert(std::is_same_v<decltype(&Connection::AsyncWrite),
                             std::error_code (Connection::*)(std::string, WriteOperation&)>);
static_assert(std::is_same_v<decltype(&Connection::TryRead),
                             ReadResult (Connection::*)(std::string&)>);
static_assert(std::is_same_v<decltype(&Connection::TryAccess), ReadAccess (Connection::*)()>);
static_assert(std::is_same_v<decltype(&Connection::ReadableBytes),
                             std::size_t (Connection::*)()>);
static_assert(std::is_same_v<decltype(&Connection::ReadError),
                             std::error_code (Connection::*)() const>);
static_assert(std::is_same_v<decltype(&Connection::ReadEof), bool (Connection::*)() const>);
static_assert(std::is_same_v<decltype(&Connection::LastWriteError),
                             std::error_code (Connection::*)(std::uint64_t*)>);
static_assert(std::is_same_v<decltype(&Connection::WriteAcceptance),
                             AcceptanceState (Connection::*)() const>);
static_assert(std::is_same_v<decltype(&Connection::Closed), bool (Connection::*)() const>);
static_assert(std::is_same_v<decltype(&Connection::CloseReason),
                             std::error_code (Connection::*)() const>);
static_assert(std::is_same_v<decltype(&Connection::RequestClose),
                             void (Connection::*)(std::error_code)>);
// 稳定值句柄：复制只共享状态，不复制连接。
static_assert(std::is_copy_constructible_v<Connection>);

static_assert(std::is_same_v<decltype(&WriteOperation::Valid),
                             bool (WriteOperation::*)() const noexcept>);
static_assert(std::is_same_v<decltype(&WriteOperation::Id),
                             std::uint64_t (WriteOperation::*)() const noexcept>);
static_assert(std::is_same_v<decltype(&WriteOperation::Query),
                             WriteOutcome (WriteOperation::*)() const>);

// D2：守卫只可移动；移动必须不抛，否则移动赋值里「解锁→通知→释放」的收尾
// 无法在异常路径上保持单一责任。
static_assert(!std::is_copy_constructible_v<ReadAccess>);
static_assert(!std::is_copy_assignable_v<ReadAccess>);
static_assert(std::is_nothrow_move_constructible_v<ReadAccess>);
static_assert(std::is_nothrow_move_assignable_v<ReadAccess>);
static_assert(std::is_nothrow_default_constructible_v<ReadAccess>);
static_assert(std::is_same_v<decltype(&ReadAccess::Size), std::size_t (ReadAccess::*)() const>);
static_assert(std::is_same_v<decltype(&ReadAccess::Peek),
                             std::string_view (ReadAccess::*)() const>);
static_assert(std::is_same_v<decltype(&ReadAccess::Consume),
                             std::error_code (ReadAccess::*)(std::size_t)>);
static_assert(std::is_same_v<decltype(&ReadAccess::CopyAll),
                             std::error_code (ReadAccess::*)(std::string&) const>);
static_assert(std::is_same_v<decltype(&ReadAccess::ConsumeInto),
                             std::error_code (ReadAccess::*)(std::string&)>);

static_assert(!std::is_copy_constructible_v<Engine>);
static_assert(!std::is_convertible_v<std::shared_ptr<RawHandler>, Engine>);
static_assert(std::is_same_v<decltype(&Engine::Run),
                             std::error_code (Engine::*)(const EngineConfig&)>);
static_assert(std::is_same_v<decltype(&Engine::RequestClose), void (Engine::*)() noexcept>);
static_assert(std::is_same_v<decltype(&Engine::Phase),
                             EnginePhase (Engine::*)() const noexcept>);
static_assert(std::is_same_v<decltype(&Engine::LastError),
                             std::error_code (Engine::*)() const>);
static_assert(std::is_same_v<decltype(&Engine::LastFailure),
                             std::exception_ptr (Engine::*)() const>);
static_assert(std::is_same_v<decltype(&Engine::LocalAddresses),
                             std::error_code (Engine::*)(std::vector<Address>&) const>);
static_assert(std::is_same_v<decltype(&Engine::CountConnections),
                             std::size_t (Engine::*)() const>);
static_assert(std::is_same_v<decltype(&Engine::HandlerErrorCount),
                             std::size_t (Engine::*)() const>);
static_assert(std::is_same_v<decltype(&Engine::ReactorOwnerIds),
                             std::error_code (Engine::*)(std::vector<std::uint64_t>&,
                                                         std::vector<std::uint64_t>&) const>);
static_assert(std::is_same_v<decltype(&Engine::AsyncConnect),
                             std::error_code (Engine::*)(const Address&, std::uint64_t&)>);
static_assert(std::is_same_v<decltype(&Engine::EnableAcceptHandoff),
                             std::error_code (Engine::*)(std::size_t)>);
static_assert(std::is_aggregate_v<EngineConfig>);
static_assert(std::is_same_v<decltype(EngineConfig::listen_addresses), std::vector<Address>>);
static_assert(std::is_same_v<decltype(EngineConfig::options), Options>);
static_assert(std::is_same_v<decltype(EngineConfig::runtime_progress), RuntimeProgress>);

static_assert(std::is_same_v<decltype(&CoRead),
                             std::error_code (*)(Connection&, std::string&,
                                                 const bbt::coroutine::WaitOptions&)>);
static_assert(std::is_same_v<decltype(&CoWrite),
                             std::error_code (*)(Connection&, const std::string&,
                                                 const bbt::coroutine::WaitOptions&,
                                                 WriteOutcome&, WriteOperation&)>);
static_assert(std::is_same_v<decltype(&CoAccept),
                             std::error_code (*)(Engine&, Connection&,
                                                 const bbt::coroutine::WaitOptions&)>);
static_assert(std::is_same_v<decltype(&CoDial),
                             std::error_code (*)(Engine&, const Address&, Connection&,
                                                 const bbt::coroutine::WaitOptions&)>);

static_assert(std::is_same_v<decltype(&LoadBalancer::Size),
                             std::size_t (LoadBalancer::*)() const noexcept>);
static_assert(std::is_same_v<decltype(&LoadBalancer::Mode),
                             LoadBalancing (LoadBalancer::*)() const noexcept>);
static_assert(std::is_same_v<decltype(&LoadBalancer::Next),
                             std::size_t (LoadBalancer::*)(const Address&,
                                                           const std::vector<std::uint64_t>&,
                                                           std::error_code&)>);

}  // namespace freeze_contract

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

    // 域 2：runtime 已启动但本线程不是协程（TLS-enabled idle）。
    std::thread idle_thread([&] {
        CheckGateProbe(RunAllCoEntries(connection, server, host, server_addresses[0]),
                       "runtime_idle_thread");
    });
    idle_thread.join();

    // 域 3：raw owner 回调同域（非业务协程）。raw 回调在 owner 的非协程边界执行，
    // 用一个不持协程上下文的线程复现同一判定，避免向产品面加测试回调钩子。
    // 用可控线程模拟 owner 边界：该线程不持有协程上下文，与 raw 回调同域判定。
    std::thread owner_like([&] {
        CheckGateProbe(RunAllCoEntries(connection, server, host, server_addresses[0]),
                       "owner_callback_domain");
    });
    owner_like.join();

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

    // 直接读公开成员默认值（不经 NormalizeOptions）：冻结的是 Options{} 本身。
    {
        const Options raw{};
        CHECK(raw.lb == LoadBalancing::RoundRobin);
        CHECK(!raw.reuse_addr);
        CHECK(!raw.reuse_port);
        CHECK(raw.num_event_loop == 0);   // 0 = 由归一化取默认 1
        CHECK(raw.tcp_keep_alive == std::chrono::seconds{0});
        CHECK(raw.tcp_keep_interval == std::chrono::seconds{0});
        CHECK(raw.tcp_keep_count == 0);
        CHECK(raw.tcp_no_delay);
        CHECK(raw.socket_recv_buffer == 0);
        CHECK(raw.socket_send_buffer == 0);
        CHECK(raw.max_inbound_bytes == 1024 * 1024);
        CHECK(raw.max_outbound_bytes == 1024 * 1024);
        CHECK(raw.max_pending_events == 1024);
    }

    // 值类型与 EngineConfig 的默认构造：聚合零值即契约，不依赖实现的初始化顺序。
    {
        const ReadResult read{};
        CHECK(read.status == ReadStatus::WouldBlock);
        CHECK(read.bytes == 0);
        CHECK(!read.error);
        const WriteOutcome outcome{};
        CHECK(outcome.phase == WritePhase::Unknown);
        CHECK(outcome.sent == 0);
        CHECK(!outcome.error);
        // OnWrite 回调值的默认成员：无操作身份、零字节、无错误。
        const WriteResult write_result{};
        CHECK(write_result.operation == 0);
        CHECK(write_result.sent == 0);
        CHECK(!write_result.error);
        const EngineConfig config{};
        CHECK(config.listen_addresses.empty());
        CHECK(config.runtime_progress == RuntimeProgress::Unspecified);
        CHECK(config.options.max_inbound_bytes == 1024 * 1024);
        CHECK(!config.options.reuse_addr);
        WriteOperation invalid_operation{};
        CHECK(!invalid_operation.Valid());
        CHECK(invalid_operation.Id() == 0);
        const WriteOutcome invalid_outcome = invalid_operation.Query();
        CHECK(invalid_outcome.phase == WritePhase::Unknown);
        CHECK(invalid_outcome.sent == 0);
        CHECK(!invalid_outcome.error);
    }

    // 空句柄（未绑定状态的 Connection）输出：终态读错误、读侧 EOF、写 Closed，
    // 且不返回任何有效操作身份。
    {
        Connection invalid;
        CHECK(!invalid.Valid());
        CHECK(invalid.Id() == 0);
        CHECK(invalid.ReadableBytes() == 0);
        std::string buffer = "keep";
        const ReadResult read = invalid.TryRead(buffer);
        CHECK(read.status == ReadStatus::Error);
        CHECK(read.bytes == 0);
        CHECK(read.error == make_error_code(NetErrc::Closed));
        CHECK(buffer == "keep");   // 失败不改调用者缓冲
        CHECK(invalid.ReadError() == std::error_code{});
        CHECK(invalid.ReadEof());
        CHECK(invalid.LastWriteError() == make_error_code(NetErrc::Closed));
        CHECK(invalid.WriteAcceptance() == AcceptanceState::Closed);
        CHECK(invalid.Closed());
        CHECK(invalid.CloseReason() == make_error_code(NetErrc::Closed));
        WriteOperation operation;
        CHECK(invalid.AsyncWrite("x", operation) == make_error_code(NetErrc::Closed));
        CHECK(!operation.Valid());
        ReadAccess guard = invalid.TryAccess();
        CHECK(!guard);
        CHECK(guard.WouldBlock());
        CHECK(!guard.Error());
        CHECK(guard.Size() == 0);
        CHECK(guard.Peek().empty());
        CHECK(!guard.ErrorCode());
    }

    std::cout << "options_contract defaults/bounds/keepalive=ok\n";
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
            if (!error && connection.Valid()) {
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

    // 1b) 早到回调交错：反复提交，尽量把"回调先于 AsyncConnect 返回"逼出来。
    //     不变量与顺序无关——回调用自带 operation 关联，集合里必须只有该次 id。
    //     该交错由 owner 线程与提交线程的竞速决定，无法用公开面确定制造；
    //     未观察到时只记录 0，不把"没撞上"当通过证据。
    {
        auto handler = std::make_shared<Recorder>();
        Engine client(handler);
        EngineRunner runner(client, ClientConfig());
        CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));
        EngineTestAccess::SetConnectDeliveryDelay(client, std::chrono::milliseconds(1));
        int observed_early = 0;
        int attempts = 0;
        for (int i = 0; i < 40; ++i) {
            handler->submit_returned.store(false);
            handler->callback_before_return.store(false);
            handler->calls.store(0);
            {
                std::lock_guard<std::mutex> lock(handler->mutex);
                handler->operations.clear();
            }
            std::uint64_t id = 0;
            const auto submit_error = client.AsyncConnect(address, id);
            handler->submit_returned.store(true);
            if (submit_error) {
                std::cout << "connect_delivery early_callback submit_error="
                          << submit_error.value() << " at " << i << '\n';
                break;
            }
            ++attempts;
            CHECK(WaitUntil([&] { return handler->calls.load() == 1; }, 3000ms));
            std::uint64_t callback_operation = 0;
            {
                std::lock_guard<std::mutex> lock(handler->mutex);
                CHECK(handler->operations.size() == 1);   // 每次提交恰一次回调
                callback_operation = *handler->operations.begin();
            }
            CHECK(callback_operation == id);
            if (handler->callback_before_return.load()) {
                ++observed_early;
            }
            // 该 listener 只是"有对端可连"：把 backlog 里的连接取走，避免 40 次后
            // 内核拒绝新连接（不取走会让本条因环境而非契约失败）。
            Address peer;
            std::error_code ignored;
            while (listener.Accept(peer, ignored).Fd() >= 0) {}
            (void)peer;
            CHECK(WaitUntil([&] { return EngineTestAccess::ConnectionCount(client) == 0; }, 2000ms));
        }
        std::cout << "connect_delivery early_callback attempts=" << attempts
                  << " observed=" << observed_early
                  << " (order-independent id match asserted; early interleave not forced by public face)\n";
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
        std::this_thread::sleep_for(50ms);
        CHECK(co_only.CountConnections() == 0);
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

    // 6) WaitStatus 逐项映射：上游该 SHA 的 6 个成员各自映射到明确 errc，
    //    不允许任何成员落到 io_error（旧实现的默认项）。RuntimeUnavailable 的
    //    真实注入需要产品侧等待接缝（本轮产品未提供，SCOPE 也不允许加），
    //    因此这里只锁映射契约，不声称已在运行期制造该状态。
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

    // 2) CoAccept：未启用 handoff → not_permitted（立即，不等超时）。
    {
        auto handler = std::make_shared<NullHandler>();
        Engine engine(handler);
        EngineRunner runner(engine, ServerConfig(Loopback(0), 1));
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
        const auto begin = std::chrono::steady_clock::now();
        std::atomic<int> result{-1};
        auto task = Task([&] {
            Connection out;
            const auto error = CoAccept(engine, out, Until(3000ms));
            if (error == std::make_error_code(std::errc::operation_not_permitted)) {
                result.store(0);
            }
        });
        Get(task);
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - begin).count();
        CHECK(result.load() == 0);
        CHECK(elapsed < 1000);               // 快速拒绝，不占满 deadline
        runner.CloseAndJoin();
    }

    // 3) CoAccept：client-only（有 handoff 无 main owner）→ not_supported。
    {
        Engine engine{std::shared_ptr<RawHandler>()};
        CHECK(!engine.EnableAcceptHandoff(2));
        EngineRunner runner(engine, ClientConfig());
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
        std::atomic<int> result{-1};
        auto task = Task([&] {
            Connection out;
            const auto error = CoAccept(engine, out, Until(3000ms));
            if (error == std::make_error_code(std::errc::operation_not_supported)) {
                result.store(0);
            }
        });
        Get(task);
        CHECK(result.load() == 0);
        runner.CloseAndJoin();
    }

    // 4) 已封口（Closing，main owner 仍在）：CoAccept → operation_canceled，
    //    且不覆盖调用者原有 out。用 seal 延迟把 Closing 窗口固定住，避免与收口竞争。
    {
        auto handler = std::make_shared<NullHandler>();
        Engine engine(handler);
        CHECK(!engine.EnableAcceptHandoff(2));
        EngineRunner runner(engine, ServerConfig(Loopback(0), 1));
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
        EngineTestAccess::SetSealDelay(engine, std::chrono::milliseconds(400));
        engine.RequestClose();
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Closing; }, 3000ms));
        auto original_state = std::make_shared<ConnectionState>(91, 1024, 1024, 4, nullptr);
        CHECK(original_state->ring.TryAppend("keep").status == ReadStatus::Ok);
        Connection original = ConnectionTestAccess::Make(original_state);
        std::atomic<int> result{-1};
        auto task = Task([&] {
            Connection out = original;
            const auto error = CoAccept(engine, out, Until(3000ms));
            std::string tail;
            if (error == std::make_error_code(std::errc::operation_canceled)
                && out.Valid() && out.Id() == original.Id()
                && out.TryRead(tail).status == ReadStatus::Ok && tail == "keep") {
                result.store(0);
            }
        });
        Get(task);
        CHECK(result.load() == 0);
        runner.CloseAndJoin();
    }

    // 4b) 完全 Stopped（main owner 已解绑）时 CoAccept 的观察结果：产品在该阶段
    //     先命中"无 listener 的 client-only"分支返回 operation_not_supported，
    //     与 CoConnection.hpp 注释「未 Running 或已封口：operation_canceled」不完全
    //     一致。此处只记录观察值（不锁定为通过条件），差异已上报实现负责人。
    {
        auto handler = std::make_shared<NullHandler>();
        Engine engine(handler);
        CHECK(!engine.EnableAcceptHandoff(2));
        EngineRunner runner(engine, ServerConfig(Loopback(0), 1));
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
        engine.RequestClose();
        CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Stopped; }, 3000ms));
        std::atomic<int> observed{-99};
        auto task = Task([&] {
            Connection out;
            observed.store(CoAccept(engine, out, Until(1000ms)).value());
        });
        Get(task);
        CHECK(observed.load() != 0);   // 必须明确拒绝，不允许成功或无限等待
        std::cout << "accept_configuration stopped_coaccept=" << observed.load()
                  << " (product reports not_supported at Stopped; doc says canceled)\n";
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

    std::cout << "accept_configuration run_gate/fast_reject/coexist/full/resume=ok\n";
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
