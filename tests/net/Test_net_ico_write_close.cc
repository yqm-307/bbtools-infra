// /：ICoConnection 成员写/关闭观察聚焦验证（契约行为，不是“跑过”）。
//
// 覆盖（每条用例以真实屏障证明「等待已挂起」后再触发事件，不靠 sleep）：
// member_write_done 成员 Write 等首胜后采到 Done+空错
// async_write_reject_ticket AsyncWrite 拒绝票 Query=Rejected/0/具体原因
// write_unsubmitted_timeout 容量等待超时未提交（Rejected+无效 op）
// write_accepted_timeout 已接纳超时仍在途，op 终态后可查
// write_accepted_cancel 已接纳等待被协程取消，发送不撤销
// op_wait_partial_error 部分发送后错误结算 Done+error+真实前缀
// write_zero_len 零长写立即可采 Done
// close_default_reason RequestClose({}) 未发完写 Done+非空错误
// facade_released_op_queryable facade 释放后 op 终态仍可查/可 Wait
// waitclosed_driver_failed driver Failed 唤醒 WaitClosed，不伪调 OnClose
// waitclosed_late_no_miss 已关闭后 WaitClosed 立即成功（晚到不丢）
// status_projection valid/read_eof/write/closed/released 采样

#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/RawHandler.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/EngineTestAccess.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <execinfo.h>
#include <filesystem>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <poll.h>
#include <unistd.h>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/Define.hpp>

using namespace bbt::infra::net;
using namespace std::chrono_literals;

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
    }, "net.ico");
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

struct StoreHandler : RawHandler {
    std::atomic<int> accepted{0};
    std::atomic<int> closed{0};
    std::mutex mutex;
    std::shared_ptr<Connection> stored;
    void OnAccept(Connection connection) override {
        accepted.fetch_add(1);
        std::lock_guard<std::mutex> lock(mutex);
        stored = std::make_shared<Connection>(connection);
    }
    void OnClose(Connection, std::error_code) override { closed.fetch_add(1); }
};

static EngineConfig ServerConfig(const Address& listen, std::size_t subs = 1) {
    EngineConfig config;
    config.listen_addresses = {listen};
    config.runtime_progress = RuntimeProgress::Independent;
    config.options.num_event_loop = static_cast<std::int64_t>(subs);
    return config;
}

// 独立 ConnectionState + facade：无 owner，终态由测试线程手动 PumpWrites/
// MarkClosed 推进；等待「已挂起」由 SetWrite/CloseAddHookForTest 在
// on_registered 内给出屏障信号（Add 成功 = runtime 已登记事件）。
static CoTcpConnection FacadeOf(const std::shared_ptr<ConnectionState>& state) {
    return CoTcpConnection(ConnectionTestAccess::Make(state));
}

struct ArmedFlag {
    std::atomic<int> armed{0};
    ~ArmedFlag() { Clear(); }
    void ArmWriteAdd() {
        ConnectionState::SetWriteAddHookForTest([this] { armed.fetch_add(1); });
    }
    void ArmCloseAdd() {
        ConnectionState::SetCloseAddHookForTest([this] { armed.fetch_add(1); });
    }
    void Clear() {
        ConnectionState::SetWriteAddHookForTest({});
        ConnectionState::SetCloseAddHookForTest({});
    }
};

// ---------------- cases ----------------

static void MemberWriteDone() {
    StartRuntime(2);
    auto state = std::make_shared<ConnectionState>(1, 4096, 4096, 8, nullptr);
    auto conn = FacadeOf(state);
    ArmedFlag flag;
    flag.ArmWriteAdd();
    std::atomic<int> result{-1};
    auto task = Task([&] {
        auto report = conn.Write("hello", bbt::coroutine::WaitOptions{});
        if (!report.wait_error && report.outcome.phase == WritePhase::Done
            && !report.outcome.error && report.outcome.sent == 5
            && report.operation.Valid() && report.operation.Id() != 0) {
            result.store(0);
        } else {
            result.store(1);
        }
    });
    // 真实屏障：等待者已在 write hub 登记后再完成该 op（不用 sleep）。
    CHECK(WaitUntil([&] { return flag.armed.load() >= 1; }, 3000ms));
    auto sink = [](const std::string_view* views, std::size_t, std::size_t budget) -> ssize_t {
        return static_cast<ssize_t>(std::min(views[0].size(), budget));
    };
    state->PumpWrites(8, 4096, sink);
    Get(task);
    CHECK(result.load() == 0);
    // 等待返回时已采到 Done（首胜后采样，不是“InFlight 快照”）。
    flag.Clear();
    std::cout << "member_write_done done=ok sampled_after_win=ok\n";
}

static void AsyncWriteRejectTicket() {
    auto state = std::make_shared<ConnectionState>(2, 4096, 128, 4, nullptr);
    auto conn = FacadeOf(state);
    auto warm = conn.AsyncWrite(std::string(64, 'a'));
    CHECK(warm.Valid());
    warm = conn.AsyncWrite(std::string(64, 'b'));
    CHECK(warm.Valid());
    // 字节额度已满（128/128）：第三笔拒绝，票自带 Rejected 原因。
    const auto rejected = conn.AsyncWrite(std::string(1, 'c'));
    CHECK(!rejected.Valid());
    const auto outcome = rejected.Query();
    CHECK(outcome.phase == WritePhase::Rejected);
    CHECK(outcome.sent == 0);
    CHECK(outcome.error == std::make_error_code(std::errc::no_buffer_space));
    // 关闭后拒绝票原因是 Closed（非裸 Unknown）。
    conn.RequestClose(make_error_code(NetErrc::Closed));
    const auto closed_ticket = conn.AsyncWrite("x");
    CHECK(closed_ticket.Query().phase == WritePhase::Rejected);
    CHECK(closed_ticket.Query().error == make_error_code(NetErrc::Closed));
    // 默认票（无身份无原因）才是 Unknown。
    WriteOperation empty;
    CHECK(empty.Query().phase == WritePhase::Unknown);
    std::cout << "async_write_reject_ticket rejected=reason closed=reason unknown=default\n";
}

static void WriteUnsubmittedTimeout() {
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(3, 4096, 128, 4, nullptr);
    auto conn = FacadeOf(state);
    CHECK(conn.AsyncWrite(std::string(64, 'a')).Valid());
    CHECK(conn.AsyncWrite(std::string(64, 'b')).Valid());
    std::atomic<int> result{-1};
    auto task = Task([&] {
        auto report = conn.Write(std::string(1, 'c'), Until(30ms));
        if (report.wait_error == std::make_error_code(std::errc::timed_out)
            && report.outcome.phase == WritePhase::Rejected
            && report.outcome.error == report.wait_error
            && !report.operation.Valid()) {
            result.store(0);
        } else {
            result.store(1);
        }
    });
    Get(task);
    CHECK(result.load() == 0);
    std::cout << "write_unsubmitted_timeout rejected=no_op\n";
}

static void WriteAcceptedTimeout() {
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(4, 4096, 4096, 8, nullptr);
    auto conn = FacadeOf(state);
    auto holder = std::make_shared<WriteOperation>();
    std::atomic<int> result{-1};
    auto task = Task([&] {
        auto report = conn.Write(std::string(8, 'd'), Until(30ms));
        *holder = report.operation;
        if (report.wait_error == std::make_error_code(std::errc::timed_out)
            && report.outcome.phase == WritePhase::InFlight
            && report.operation.Valid()) {
            result.store(0);
        } else {
            result.store(1);
        }
    });
    Get(task);
    CHECK(result.load() == 0);
    // 超时不撤销：op 仍在途可查；推进后终态落到同一 record。
    CHECK(holder->Query().phase == WritePhase::InFlight);
    auto sink = [](const std::string_view* views, std::size_t, std::size_t budget) -> ssize_t {
        return static_cast<ssize_t>(std::min(views[0].size(), budget));
    };
    state->PumpWrites(8, 4096, sink);
    const auto done = holder->Query();
    CHECK(done.phase == WritePhase::Done && done.sent == 8 && !done.error);
    std::cout << "write_accepted_timeout inflight_then_done=ok\n";
}

static void WriteAcceptedCancel() {
    StartRuntime(2);
    auto state = std::make_shared<ConnectionState>(5, 4096, 4096, 8, nullptr);
    auto conn = FacadeOf(state);
    ArmedFlag flag;
    flag.ArmWriteAdd();
    auto self = std::make_shared<std::promise<bbt::coroutine::detail::Coroutine*>>();
    auto self_future = self->get_future();
    auto holder = std::make_shared<WriteOperation>();
    std::atomic<int> result{-1};
    auto task = Task([&] {
        self->set_value(g_bbt_tls_coroutine_co);
        auto report = conn.Write(std::string(8, 'e'), bbt::coroutine::WaitOptions{});
        *holder = report.operation;
        if (report.wait_error == std::make_error_code(std::errc::operation_canceled)
            && report.operation.Valid()) {
            result.store(0);
        } else {
            result.store(1);
        }
    });
    // 屏障：等待已真实挂起（runtime 已登记事件 + Add 成功）后取消协程。
    CHECK(WaitUntil([&] { return flag.armed.load() >= 1; }, 3000ms));
    flag.Clear();
    CHECK(self_future.wait_for(3s) == std::future_status::ready);
    self_future.get()->RequestCancel();
    Get(task);
    CHECK(result.load() == 0);
    // 取消不撤销发送：record 仍可推进到 Done。
    auto sink = [](const std::string_view* views, std::size_t, std::size_t budget) -> ssize_t {
        return static_cast<ssize_t>(std::min(views[0].size(), budget));
    };
    state->PumpWrites(8, 4096, sink);
    CHECK(holder->Query().phase == WritePhase::Done && holder->Query().sent == 8);
    std::cout << "write_accepted_cancel wait=cancelled op=still_done\n";
}

static void OpWaitPartialError() {
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(6, 4096, 4096, 8, nullptr);
    auto conn = FacadeOf(state);
    auto ticket = conn.AsyncWrite(std::string(64, 'p'));
    CHECK(ticket.Valid());
    ArmedFlag flag;
    flag.ArmWriteAdd();
    std::atomic<int> result{-1};
    auto task = Task([&] {
        WriteOutcome outcome;
        const auto error = ticket.Wait(bbt::coroutine::WaitOptions{}, &outcome);
        if (!error && outcome.phase == WritePhase::Done
            && outcome.error.value() == EPIPE && outcome.sent == 8) {
            result.store(0);
        } else {
            result.store(1);
        }
    });
    // 屏障：op.Wait 已挂起后注入「发 8 字节后 EPIPE」的部分发送错误。
    CHECK(WaitUntil([&] { return flag.armed.load() >= 1; }, 3000ms));
    flag.Clear();
    int calls = 0;
    auto sink = [&calls](const std::string_view*, std::size_t, std::size_t) -> ssize_t {
        ++calls;
        if (calls == 1) {
            return 8;
        }
        errno = EPIPE;
        return -1;
    };
    state->PumpWrites(8, 4096, sink);
    Get(task);
    CHECK(result.load() == 0);
    const auto terminal = ticket.Query();
    CHECK(terminal.phase == WritePhase::Done && terminal.sent == 8
          && terminal.error.value() == EPIPE);
    std::cout << "op_wait_partial_error sent=8 epipe=ok\n";
}

static void WriteZeroLen() {
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(7, 4096, 4096, 8, nullptr);
    auto conn = FacadeOf(state);
    const auto ticket = conn.AsyncWrite("");
    CHECK(ticket.Valid());
    // 零长写接纳即终态：Done/0/空错误，不占发送字节。
    const auto outcome = ticket.Query();
    CHECK(outcome.phase == WritePhase::Done && outcome.sent == 0 && !outcome.error);
    std::atomic<int> result{-1};
    auto task = Task([&] {
        auto report = conn.Write("", Until(500ms));
        if (!report.wait_error && report.outcome.phase == WritePhase::Done
            && !report.outcome.error && report.operation.Valid()) {
            result.store(0);
        } else {
            result.store(1);
        }
    });
    Get(task);
    CHECK(result.load() == 0);
    std::cout << "write_zero_len done_immediate=ok\n";
}

static void CloseDefaultReason() {
    auto state = std::make_shared<ConnectionState>(8, 4096, 4096, 8, nullptr);
    auto conn = FacadeOf(state);
    const auto ticket = conn.AsyncWrite(std::string(64, 'u'));
    CHECK(ticket.Valid());
    // 默认空 reason：CloseReason 保持空值（首请求固定），但未发完写
    // 必须以 Done+非空错误结算——空 reason 不能冒充本地成功。
    conn.RequestClose({});
    const auto outcome = ticket.Query();
    CHECK(outcome.phase == WritePhase::Done);
    CHECK(outcome.error);  // 非空：不能是假成功
    CHECK(outcome.error == std::make_error_code(std::errc::operation_canceled));
    CHECK(outcome.sent == 0);
    CHECK(conn.Status().close_reason == std::error_code{});  // reason 仍为空
    // 显式 reason 保留原值结算。
    auto state2 = std::make_shared<ConnectionState>(9, 4096, 4096, 8, nullptr);
    auto conn2 = FacadeOf(state2);
    const auto ticket2 = conn2.AsyncWrite(std::string(8, 'v'));
    conn2.RequestClose(std::error_code(ECONNRESET, std::system_category()));
    CHECK(ticket2.Query().phase == WritePhase::Done
          && ticket2.Query().error == std::error_code(ECONNRESET, std::system_category()));
    CHECK(conn2.Status().close_reason == std::error_code(ECONNRESET, std::system_category()));
    std::cout << "close_default_reason unfinished=canceled reason_kept=ok\n";
}

static void FacadeReleasedOpQueryable() {
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(10, 4096, 4096, 8, nullptr);
    WriteOperation op;
    {
        auto conn = FacadeOf(state);
        op = conn.AsyncWrite(std::string(16, 'f'));
        CHECK(op.Valid());
    }  // facade 释放：句柄销毁不关闭连接，op 终态仍可查
    CHECK(op.Query().phase == WritePhase::InFlight);
    ArmedFlag flag;
    flag.ArmWriteAdd();
    std::atomic<int> result{-1};
    auto task = Task([&] {
        WriteOutcome outcome;
        const auto error = op.Wait(Until(2000ms), &outcome);
        if (!error && outcome.phase == WritePhase::Done && outcome.sent == 16) {
            result.store(0);
        } else {
            result.store(1);
        }
    });
    CHECK(WaitUntil([&] { return flag.armed.load() >= 1; }, 3000ms));
    flag.Clear();
    auto sink = [](const std::string_view* views, std::size_t, std::size_t budget) -> ssize_t {
        return static_cast<ssize_t>(std::min(views[0].size(), budget));
    };
    state->PumpWrites(8, 4096, sink);
    Get(task);
    CHECK(result.load() == 0);
    std::cout << "facade_released_op_queryable wait_after_release=done\n";
}

static void WaitClosedDriverFailed() {
    StartRuntime(2);
    auto handler = std::make_shared<StoreHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    auto client = ConnectClient(addresses[0]);
    CHECK(WaitUntil([&] {
        std::lock_guard<std::mutex> lock(handler->mutex);
        return handler->stored != nullptr;
    }, 3000ms));
    std::shared_ptr<Connection> raw;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        raw = handler->stored;
    }
    auto state = ConnectionTestAccess::StateOf(*raw);
    CoTcpConnection conn(*raw);
    ArmedFlag flag;
    flag.ArmCloseAdd();
    std::atomic<int> result{-1};
    auto task = Task([&] {
        const auto error = conn.WaitClosed(Until(4000ms));
        result.store(error ? 1 : 0);
    });
    // 真实屏障：WaitClosed 已在 close hub 登记后才注入 driver Failed；
    // Failed 走控制域 CancelControl→MarkClosed，唤醒等待但不伪调 OnClose。
    CHECK(WaitUntil([&] { return flag.armed.load() >= 1; }, 3000ms));
    flag.Clear();
    CHECK(!EngineTestAccess::InjectDriverFailure(engine));
    Get(task);
    CHECK(result.load() == 0);
    CHECK(conn.Status().closed);
    CHECK(handler->closed.load() == 0);  // Failed 路径不伪调 owner 回调
    CHECK(runner.WaitForReturn(5000ms));
    CHECK(engine.LastError());
    CHECK(!client.Close());
    runner.CloseAndJoin();
    std::cout << "waitclosed_driver_failed woke=closed no_pseudo_onclose=ok\n";
}

static void WaitClosedLateNoMiss() {
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(11, 4096, 4096, 8, nullptr);
    auto conn = FacadeOf(state);
    conn.RequestClose(std::error_code(ECONNRESET, std::system_category()));
    CHECK(conn.Status().closed);
    // 已发布后晚到的 WaitClosed 直接成功（谓词复查，不依赖 Notify 留存）。
    std::atomic<int> result{-1};
    auto task = Task([&] {
        const auto error = conn.WaitClosed(Until(500ms));
        result.store(error ? 1 : 0);
    });
    Get(task);
    CHECK(result.load() == 0);
    CHECK(conn.Status().close_reason == std::error_code(ECONNRESET, std::system_category()));
    std::cout << "waitclosed_late_no_miss immediate=ok\n";
}

static void StatusProjection() {
    auto state = std::make_shared<ConnectionState>(12, 4096, 4096, 8, nullptr);
    {
        CoTcpConnection invalid;
        const auto status = invalid.Status();
        CHECK(!status.valid && status.closed && status.released && status.read_eof);
        CHECK(status.write == AcceptanceState::Closed);
        CHECK(!invalid.Valid() && invalid.IsClosed() && invalid.IsReleased());
    }
    auto conn = FacadeOf(state);
    auto status = conn.Status();
    CHECK(status.valid && !status.closed && !status.read_eof);
    CHECK(status.write == AcceptanceState::Open);
    CHECK(status.readable_bytes == 0 && !status.read_error);
    // 独立 state 无 backend fd：ReclaimRecord 缺省即「无责任」→
    // released=true 从创建起为真（与 closed=false 可合法共存）。
    CHECK(status.released);
    CHECK(conn.Valid() && !conn.IsClosed() && conn.IsReleased());
    conn.RequestClose({});
    status = conn.Status();
    CHECK(status.closed && status.released);
    CHECK(status.write == AcceptanceState::Closed);
    std::cout << "status_projection invalid=terminal open=sampled closed+released=ok\n";
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
        if (test == "member_write_done") { MemberWriteDone(); }
        else if (test == "async_write_reject_ticket") { AsyncWriteRejectTicket(); }
        else if (test == "write_unsubmitted_timeout") { WriteUnsubmittedTimeout(); }
        else if (test == "write_accepted_timeout") { WriteAcceptedTimeout(); }
        else if (test == "write_accepted_cancel") { WriteAcceptedCancel(); }
        else if (test == "op_wait_partial_error") { OpWaitPartialError(); }
        else if (test == "write_zero_len") { WriteZeroLen(); }
        else if (test == "close_default_reason") { CloseDefaultReason(); }
        else if (test == "facade_released_op_queryable") { FacadeReleasedOpQueryable(); }
        else if (test == "waitclosed_driver_failed") { WaitClosedDriverFailed(); }
        else if (test == "waitclosed_late_no_miss") { WaitClosedLateNoMiss(); }
        else if (test == "status_projection") { StatusProjection(); }
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
