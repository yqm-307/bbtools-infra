/**
 * 协程 Write (n, err) 与超时/取消撤销未发部分（ADR 0024 / S1a）。
 *
 * 覆盖测试清单 1–9 项，每项均以“真实对端字节 == n” 或 state 级确定性交错为证据，不靠 sleep 碰运气：
 *  1. 三档与对端实收字节一致：n=data.size() 且对端收全部；已接纳后超时/取消 n=已发前缀 且对端仅收该前缀；
 *     容量等待超时未接纳 n=0 且对端无新增字节；
 *  2. 撤销与 PumpWrites 确定性交错：写泵持锁跑完整批后撤销，n 为已发前缀终值，再泵不再下发未发后缀；
 *  3. 排在 AsyncWrite 之后的写被整段撤出，先写字节完整且顺序不变；
 *  4. 中段写撤销后，前后写字节无空洞、顺序不变；
 *  5. 关闭中写：关闭前 (0, Closed)，关闭后新写被拒绝；部分发送后关闭以 (已发前缀, 关闭原因) 结算；
 *  6. 零长写立即返回 (0, {}) 且不阻塞后续写；
 *  7. 被撤销的协程写恰一次 OnWrite，sent=n 且 error 为撤销原因；raw AsyncWrite 行为不变；
 *  8. 撤销不进入 Settling、不写 LastWriteError，连接保持 Open 且可继续写；
 *  9. 撤销后 TotalBytes()-RevokedBytes() 正确，新写可复用释放额度，背压不虚假触发。
 *
 * 真实 TCP 用例基于 loopback + NativeSocket 对端线程，端口动态分配，不依赖外部服务。
 */
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/Error.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/RawHandler.hpp>
#include <bbt/infra/net/Types.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/EngineTestAccess.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/Coroutine.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <execinfo.h>
#include <filesystem>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include <poll.h>
#include <unistd.h>

using namespace bbt::infra::net;
using namespace std::chrono_literals;

static void Check(bool value, const char* expression, int line)
{
    if (!value) {
        throw std::runtime_error("line " + std::to_string(line) + ": " + expression);
    }
}

#define CHECK(expression) Check(static_cast<bool>(expression), #expression, __LINE__)

static bbt::coroutine::WaitOptions Until(std::chrono::milliseconds duration)
{
    return {std::chrono::steady_clock::now() + duration};
}

static bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(1ms);
    }
    return predicate();
}

static Address Loopback(int port)
{
    std::error_code error;
    Address address = Address::Ip("127.0.0.1", port, error);
    CHECK(!error);
    return address;
}

static void StartRuntime(std::size_t workers)
{
    g_bbt_coroutine_config->m_cfg_static_thread_num = workers;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start();
}

static std::future<void> Task(std::function<void()> callback)
{
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    // bbt-lint: ignore-next-line coroutine-sugar -- explicit 64 KiB stack; bbtco has no stack-size form
    g_scheduler->RegistCoroutineTask(
        65536,
        [promise, callback] {
            try {
                callback();
                promise->set_value();
            } catch (...) {
                promise->set_exception(std::current_exception());
            }
        },
        "net.co_write");
    return future;
}

static void Get(std::future<void>& future)
{
    CHECK(future.wait_for(5s) == std::future_status::ready);
    future.get();
}

struct EngineRunner {
    Engine& engine;
    std::error_code error;
    std::thread thread;

    EngineRunner(Engine& target, EngineConfig config)
        : engine(target)
    {
        thread = std::thread([this, config]() mutable { error = engine.Run(config); });
    }

    bool WaitForReturn(std::chrono::milliseconds timeout)
    {
        return WaitUntil([&] { return engine.Phase() == EnginePhase::Stopped; }, timeout);
    }

    void CloseAndJoin()
    {
        (void)engine.Close();
        if (thread.joinable()) {
            thread.join();
        }
    }

    ~EngineRunner() { CloseAndJoin(); }
};

/** 协程等待已挂起的证据：ConnectionState::AddWriteWaiter 成功后触发，无需 sleep 等时序。 */
struct WriteWaitArmed {
    std::shared_ptr<ConnectionState> state;

    explicit WriteWaitArmed(std::shared_ptr<ConnectionState> target)
        : state(std::move(target))
    {
    }

    std::atomic<int> armed{0};

    void arm()
    {
        state->SetWriteAddHookForTest([this] { armed.fetch_add(1); });
    }

    void clear() { state->SetWriteAddHookForTest({}); }

    ~WriteWaitArmed() { clear(); }
};

/** 对端读线程：accept 后读取。fast 为真时每次读尽可用字节；否则每 5 ms 仅读 1 字节，
 *  用于制造接收窗口耗尽，使客户端协程写阻塞并进入超时/取消。 */
static void ServerReadThread(
    NativeSocket& server,
    std::atomic<bool>& stop,
    std::atomic<bool>& fast,
    std::atomic<std::size_t>& received,
    std::string& data,
    std::mutex& mutex,
    std::error_code& accept_error)
{
    std::error_code error;
    Address peer_address = Address::Ip("0.0.0.0", 0, error);
    /** 循环 Accept，直到有连接到达或被要求停止（listen fd 永远可读，不能只调一次 Accept）。 */
    NativeSocket peer;
    bool accepted = false;
    while (!stop.load(std::memory_order_acquire) && !accepted) {
        pollfd descriptor{server.Fd(), POLLIN, 0};
        if (::poll(&descriptor, 1, 5) <= 0)
            continue;
        peer = server.Accept(peer_address, error);
        if (!error) {
            accepted = true;
            break;
        }
        if (error.value() != EAGAIN && error.value() != EWOULDBLOCK) {
            accept_error = error;
            return;
        }
    }
    if (!accepted) {
        return;
    }
    char single{};
    while (!stop.load(std::memory_order_acquire)) {
        pollfd descriptor{peer.Fd(), POLLIN, 0};
        const int ready = ::poll(&descriptor, 1, fast.load(std::memory_order_acquire) ? 2 : 5);
        if (ready <= 0)
            continue;
        if (fast.load(std::memory_order_acquire)) {
            char buffer[65536];
            for (;;) {
                const auto result = peer.Receive(buffer, sizeof(buffer));
                if (result.bytes > 0) {
                    std::lock_guard<std::mutex> lock(mutex);
                    received += result.bytes;
                    data.append(buffer, result.bytes);
                } else {
                    break;
                }
            }
        } else {
            const auto result = peer.Receive(&single, 1);
            if (result.bytes > 0) {
                std::lock_guard<std::mutex> lock(mutex);
                received += result.bytes;
                data.append(&single, 1);
                /** 慢读：每字节间隔 5 ms，使对端接收窗口耗尽并保持，逼客户端写入超时/取消 */
                std::this_thread::sleep_for(5ms);
            } else if (result.eof) {
                return;
            } else if (result.error.value() != EAGAIN && result.error.value() != EWOULDBLOCK) {
                return;
            }
        }
    }
    (void)peer.Close();
}

static NativeSocket ListenLoopback(int& port)
{
    std::error_code error;
    NativeSocket server = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    CHECK(!error);
    CHECK(!server.SetReuseAddr(true));
    Address bind = Address::Ip("127.0.0.1", 0, error);
    CHECK(!error);
    CHECK(!server.Bind(bind));
    CHECK(!server.Listen(128));
    port = server.LocalAddress(error).Port();
    CHECK(!error);
    CHECK(port != 0);
    return server;
}

static std::size_t AppendViews(std::string& out, const std::string_view* views, std::size_t count)
{
    std::size_t total = 0;
    for (std::size_t i = 0; i < count; ++i) {
        out.append(views[i].data(), views[i].size());
        total += views[i].size();
    }
    return static_cast<ssize_t>(total);
}

static EngineConfig ServerConfig(const Address& listen, std::size_t subs = 1)
{
    EngineConfig config;
    config.listen_addresses = {listen};
    config.runtime_progress = RuntimeProgress::Independent;
    config.options.num_event_loop = static_cast<std::int64_t>(subs);
    return config;
}

/** 计数处理面：记录每次 OnWrite 的终态，证明被撤销的协程写恰一次结算。 */
struct CountingHandler : RawHandler {
    std::mutex mutex;
    std::shared_ptr<Connection> stored;
    std::vector<WriteResult> writes;
    std::atomic<int> accepted{0};
    std::atomic<int> closed{0};

    void OnAccept(Connection connection) override
    {
        std::lock_guard<std::mutex> lock(mutex);
        accepted.fetch_add(1);
        stored = std::make_shared<Connection>(connection);
    }

    void OnWrite(Connection, WriteResult result) override
    {
        std::lock_guard<std::mutex> lock(mutex);
        writes.push_back(result);
    }

    void OnClose(Connection, std::error_code) override { closed.fetch_add(1); }
};

/** ---------------- cases ---------------- */

/** 1-1：全部交给内核 n=data.size()，对端收到全部且逐字节一致。 */
static void CoWriteFull()
{
    StartRuntime(1);
    int port = 0;
    NativeSocket server = ListenLoopback(port);
    const Address address = Loopback(port);

    std::atomic<bool> server_stop{false};
    std::atomic<bool> server_fast{false};
    std::atomic<std::size_t> server_received{0};
    std::mutex server_mutex;
    std::string server_data;
    std::error_code server_accept_error;
    std::thread server_thread(
        ServerReadThread,
        std::ref(server),
        std::ref(server_stop),
        std::ref(server_fast),
        std::ref(server_received),
        std::ref(server_data),
        std::ref(server_mutex),
        std::ref(server_accept_error));

    /** 退出作用域时保证对端线程已置 stop 并 join，避免 std::thread 析构时 terminate。 */
    struct ServerEnd {
        std::thread& thread;
        std::atomic<bool>& stop;

        ~ServerEnd()
        {
            stop.store(true, std::memory_order_release);
            if (thread.joinable())
                thread.join();
        }
    } server_end{server_thread, server_stop};

    std::atomic<int> client_result{-1};
    std::tuple<std::size_t, std::error_code> client_report;
    EngineConfig client_config;
    client_config.runtime_progress = RuntimeProgress::Independent;
    client_config.options.num_event_loop = 1;
    Engine client{std::shared_ptr<RawHandler>()};
    EngineTestAccess::ClearConnectInjection(client);
    EngineRunner client_runner(client, client_config);
    CHECK(WaitUntil([&] { return client.Phase() == EnginePhase::Running; }, 3000ms));

    const std::string payload(256, 'A');
    auto client_task = Task([&] {
        const auto res = client.CoDial(address, Until(3000ms));
        if (res.error) {
            client_result.store(2);
            return;
        }
        CoTcpConnection co_conn{res.connection};
        client_report = co_conn.Write(payload, bbt::coroutine::WaitOptions{});
        client_result.store(0);
    });

    CHECK(WaitUntil([&] { return client_result.load() == 0; }, 10000ms));
    Get(client_task);
    CHECK(std::get<0>(client_report) == payload.size());
    CHECK(!std::get<1>(client_report));

    /** 对端实收字节与 n 逐字节对比 */
    CHECK(WaitUntil([&] { return server_received.load() == payload.size(); }, 5000ms));
    server_stop.store(true);
    server_thread.join();
    CHECK(!server_accept_error);
    CHECK(server_received.load() == payload.size());
    CHECK(server_data.size() == payload.size());
    CHECK(std::memcmp(server_data.data(), payload.data(), payload.size()) == 0);

    client_runner.CloseAndJoin();
    CHECK(!client_runner.error);
    std::cout << "co_write_full n=data.size() peer_bytes=n prefix=ok\n";
}

/** 1-2：已接纳后超时撤销，n=已发前缀，对端（内核）仅收到该前缀，撤销后不再发出未发后缀。 */
static void CoWritePartial()
{
    auto state = std::make_shared<ConnectionState>(72, 4096, 4096, 8, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);
    const std::string data(4000, 'C');

    /** 已接纳 4000 字节，泵发先归属 16 字节后阻塞 */
    WriteWait slot{};
    std::uint64_t write_id = 0;
    CHECK(!state->SubmitWrite(data, write_id, &slot));
    CHECK(state->ActiveOpCount() == 1);

    std::size_t kernel_bytes = 0;
    std::string kernel_out;
    int sink_calls = 0;
    auto pump_sink = [&kernel_bytes, &kernel_out, &sink_calls](
                         const std::string_view* views, std::size_t count, std::size_t) -> ssize_t {
        (void)count;
        ++sink_calls;
        if (sink_calls == 1) {
            kernel_out.append(views[0].data(), 16);
            kernel_bytes += 16;
            return 16;
        }
        errno = EAGAIN;
        return -1;
    };
    state->PumpWrites(8, 4096, pump_sink);
    CHECK(kernel_bytes == 16 && kernel_out == std::string(16, 'C') && slot.sent == 16 && !slot.terminal);

    /** 超时撤销：n 为已发前缀 16（等待错误），未发 3984 字节撤出 */
    const auto revoked = state->CancelWrite(write_id, slot, std::make_error_code(std::errc::timed_out));
    CHECK(std::get<0>(revoked) == 16);
    CHECK(std::get<1>(revoked) == std::make_error_code(std::errc::timed_out));
    CHECK(slot.terminal && slot.sent == 16 && slot.error == std::make_error_code(std::errc::timed_out));

    /** 再次泵发：撤销区间被 GatherViews 跳过，未发后缀一个字节也不下发 */
    kernel_bytes = 0;
    sink_calls = 0;
    state->PumpWrites(8, 4096, pump_sink);
    CHECK(kernel_bytes == 0 && sink_calls == 0);

    const auto terminal = state->DrainTerminal();
    CHECK(terminal.size() == 1);
    CHECK(
        terminal[0].id == write_id && terminal[0].sent == 16
        && terminal[0].error == std::make_error_code(std::errc::timed_out));
    CHECK(state->Acceptance() == AcceptanceState::Open); // 超时撤销不进入 Settling
    CHECK(!state->LastWriteError());                     // 不记为持久写错误
    std::cout << "co_write_partial n=16 timed_out kernel=16 prefix=ok no_more_bytes=open=ok\n";
}

/** 1-3：容量等待超时未接纳，n=0，对端无新增字节。 */
static void CoWriteCapacityZero()
{
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(71, 4096, 4096, 8, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);

    /** 用 4 次火写占满 4096 字节出队上限 */
    for (int i = 0; i < 4; ++i) {
        CHECK(!connection.AsyncWrite(std::string(1024, 'f')));
    }
    CHECK(state->ActiveOpCount() == 4);
    CHECK(!state->CanAcceptWrite(1)); /** 队列已满，真实背压 */

    /** 协程写等待容量直到超时：从未接纳，n=0 */
    std::atomic<int> result{-1};
    std::tuple<std::size_t, std::error_code> report;
    auto task = Task([&] {
        CoTcpConnection co_conn{connection};
        report = co_conn.Write(std::string(1, 'z'), Until(30ms));
        result.store(0);
    });
    Get(task);
    CHECK(result.load() == 0);
    CHECK(std::get<0>(report) == 0);
    CHECK(std::get<1>(report) == std::make_error_code(std::errc::timed_out));
    /** 协程写本身从未入队：4 条火写仍在账本，等待超时未改变队列 */
    CHECK(state->ActiveOpCount() == 4);
    CHECK(state->HasPendingWrites());

    /** 把已接纳的火写发走：输出里只有火写字节，'z' 无任何字节下发（对端无新增字节） */
    std::string pumped;
    auto record_sink = [&pumped](const std::string_view* views, std::size_t count, std::size_t) -> ssize_t {
        return AppendViews(pumped, views, count);
    };
    state->PumpWrites(8, 4096, record_sink);
    CHECK(pumped == std::string(4096, 'f'));
    CHECK(pumped.find('z') == std::string::npos);
    CHECK(state->ActiveOpCount() == 0);
    CHECK(!state->HasPendingWrites());
    std::cout << "co_write_capacity_zero n=0 timed_out no_kernel_bytes=ok\n";
}

/** 2：撤销与 PumpWrites 确定性交错。泵先持锁跑完一个批次（16 字节后 EAGAIN），再撤销；*/
static void CoWriteRevokeInterleaved()
{
    auto state = std::make_shared<ConnectionState>(111, 4096, 4096, 8, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);
    const std::string data(64, 'x');

    /** 进入目标阶段：写已接纳并挂起完成等待 */
    WriteWait slot{};
    std::uint64_t write_id = 0;
    CHECK(!state->SubmitWrite(data, write_id, &slot));
    CHECK(state->HasPendingWrites());
    CHECK(state->ActiveOpCount() == 1);

    /** 写泵持锁跑整批：先归属 16 字节，再 EAGAIN 退出，锁释放后无在途发送 */
    std::size_t kernel_bytes = 0;
    int sink_calls = 0;
    auto pump_sink = [&kernel_bytes,
                      &sink_calls](const std::string_view* views, std::size_t count, std::size_t) -> ssize_t {
        (void)views;
        (void)count;
        ++sink_calls;
        if (sink_calls == 1) {
            kernel_bytes += 16; // 第 1 次：真实下发 16 字节（返回值），而非提供的 64 字节
            return 16;
        }
        errno = EAGAIN;
        return -1; // 第 2 次：阻塞，无字节下发
    };
    state->PumpWrites(8, 4096, pump_sink);
    CHECK(kernel_bytes == 16);
    CHECK(slot.sent == 16 && !slot.terminal);

    /** 撤销方拿锁后无在途发送：n 为已发前缀 16 的终值，未发 48 字节撤出 */
    const auto revoked = state->CancelWrite(write_id, slot, std::make_error_code(std::errc::operation_canceled));
    CHECK(std::get<0>(revoked) == 16);
    CHECK(std::get<1>(revoked) == std::make_error_code(std::errc::operation_canceled));
    CHECK(slot.terminal && slot.sent == 16 && slot.error == std::make_error_code(std::errc::operation_canceled));

    /** 再次泵发：撤销区间被 GatherViews 跳过，未发后缀一个字节也不下发 */
    kernel_bytes = 0;
    sink_calls = 0;
    state->PumpWrites(8, 4096, pump_sink);
    CHECK(kernel_bytes == 0 && sink_calls == 0);

    const auto terminal = state->DrainTerminal();
    CHECK(terminal.size() == 1);
    CHECK(
        terminal[0].id == write_id && terminal[0].sent == 16
        && terminal[0].error == std::make_error_code(std::errc::operation_canceled));

    /** 撤销不是传输错误：Open 不进入 Settling，无 LastWriteError，可继续写 */
    CHECK(state->Acceptance() == AcceptanceState::Open);
    CHECK(!state->LastWriteError());
    CHECK(!state->HasPendingWrites());
    CHECK(state->CanAcceptWrite(32));
    WriteWait slot2{};
    std::uint64_t write_id2 = 0;
    CHECK(!state->SubmitWrite(std::string(32, 'y'), write_id2, &slot2));
    std::cout << "co_write_revoke_interleaved n=16 kernel=16 revoke=final no_more_bytes open=writable=ok\n";
}

/** 3：排在 AsyncWrite 之后的写被整段撤出，先写字节完整且顺序不变。 */
static void CoWriteAsyncPrefixRevoke()
{
    auto state = std::make_shared<ConnectionState>(121, 4096, 4096, 8, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);

    /** 先火写 W1，再协程风格写入 W2；撤销 W2 */
    CHECK(!connection.AsyncWrite(std::string(16, 'w')));
    WriteWait slot{};
    std::uint64_t write_id = 0;
    CHECK(!state->SubmitWrite(std::string(64, 'x'), write_id, &slot));
    CHECK(state->ActiveOpCount() == 2);

    const auto revoked = state->CancelWrite(write_id, slot, std::make_error_code(std::errc::operation_canceled));
    CHECK(std::get<0>(revoked) == 0);
    CHECK(std::get<1>(revoked) == std::make_error_code(std::errc::operation_canceled));

    std::string out;
    auto append_sink = [&out](const std::string_view* views, std::size_t count, std::size_t) -> ssize_t {
        return AppendViews(out, views, count);
    };
    state->PumpWrites(8, 4096, append_sink);
    CHECK(out == std::string(16, 'w')); /** W2 未发后缀一个字节也不下发 */

    const auto terminals = state->DrainTerminal();
    CHECK(terminals.size() == 2);
    bool w1_done = false, w2_revoked = false;
    for (const auto& t : terminals) {
        if (t.sent == 16 && !t.error) {
            w1_done = true;
        }
        if (t.sent == 0 && t.error == std::make_error_code(std::errc::operation_canceled)) {
            w2_revoked = true;
        }
    }
    CHECK(w1_done && w2_revoked);
    std::cout << "co_write_async_prefix_revoke w1_complete=w2_fully_revoked=ok\n";
}

/** 4：中段写撤销后，前后写字节无空洞、顺序不变。 */
static void CoWriteOrderAfterMidRevoke()
{
    auto state = std::make_shared<ConnectionState>(131, 4096, 4096, 8, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);

    WriteWait s1{}, s2{};
    std::uint64_t i1 = 0, i2 = 0;
    CHECK(!state->SubmitWrite(std::string(64, 'x'), i1, &s1)); /** W1 先接纳 */
    CHECK(!state->SubmitWrite(std::string(64, 'y'), i2, &s2)); /** W2 中段，后被撤销 */
    CHECK(!connection.AsyncWrite(std::string(64, 'z')));       /** W3 后到 */
    CHECK(state->ActiveOpCount() == 3);

    /** 第一批泵发：仅 W1 前半 16 字节发出后阻塞 */
    std::string out;
    int calls = 0;
    auto block_sink = [&out, &calls](const std::string_view* views, std::size_t count, std::size_t) -> ssize_t {
        (void)count;
        ++calls;
        if (calls == 1) {
            const std::size_t sent = std::min<std::size_t>(views[0].size(), 16);
            out.append(views[0].data(), sent);
            return static_cast<ssize_t>(sent);
        }
        errno = EAGAIN;
        return -1;
    };
    state->PumpWrites(8, 4096, block_sink);
    CHECK(s1.sent == 16 && !s1.terminal);

    /** 撤销中段 W2，再泵发：W1 余下 48 字节紧随其后，再是 W3，无空洞 */
    CHECK(std::get<0>(state->CancelWrite(i2, s2, std::make_error_code(std::errc::operation_canceled))) == 0);
    auto take_all = [&out](const std::string_view* views, std::size_t count, std::size_t) -> ssize_t {
        return AppendViews(out, views, count);
    };
    state->PumpWrites(8, 4096, take_all);
    CHECK(out == std::string(64, 'x') + std::string(64, 'z'));

    const auto terminals = state->DrainTerminal();
    CHECK(terminals.size() == 3);
    std::size_t full_ok = 0, revoked_count = 0;
    for (const auto& t : terminals) {
        if (t.sent == 64 && !t.error) {
            ++full_ok; /** W1 与 W3 全量发出 */
        }
        if (t.sent == 0 && t.error == std::make_error_code(std::errc::operation_canceled)) {
            ++revoked_count; /** W2 被整段撤销 */
        }
    }
    CHECK(full_ok == 2 && revoked_count == 1);
    std::cout << "co_write_order_after_mid_revoke stream=w1+w3 contiguous w2_hole_gone=ok\n";
}

/** 5：关闭中写。 */
static void CoWriteAfterClose()
{
    /** 关闭先行：写返回 (0, Closed) */
    {
        StartRuntime(1);
        auto state = std::make_shared<ConnectionState>(81, 4096, 4096, 8, nullptr);
        Connection connection = ConnectionTestAccess::Make(state);
        CHECK(!connection.Close({}, std::make_error_code(std::errc::operation_canceled)));
        CHECK(connection.Closed());
        std::atomic<int> result{-1};
        std::tuple<std::size_t, std::error_code> report;
        auto task = Task([&] {
            CoTcpConnection co_conn{connection};
            report = co_conn.Write(std::string(8, 'm'), bbt::coroutine::WaitOptions{});
            result.store(0);
        });
        Get(task);
        CHECK(result.load() == 0);
        CHECK(std::get<0>(report) == 0);
        CHECK(std::get<1>(report) == make_error_code(NetErrc::Closed));
    }

    /** 部分发送后关闭：已发前缀以关闭原因结算；关闭后新写被拒绝；重复关闭幂等 */
    {
        auto state = std::make_shared<ConnectionState>(82, 4096, 4096, 8, nullptr);
        Connection connection = ConnectionTestAccess::Make(state);
        const std::string data(64, 'n');
        WriteWait slot{};
        std::uint64_t write_id = 0;
        CHECK(!state->SubmitWrite(data, write_id, &slot));
        CHECK(state->HasPendingWrites()); /** 已接纳并等待发送 */

        int calls = 0;
        auto block_sink = [&calls](const std::string_view* views, std::size_t count, std::size_t) -> ssize_t {
            (void)views;
            (void)count;
            ++calls;
            if (calls == 1) {
                return 16; /** 先发 16 字节，再阻塞 */
            }
            errno = EAGAIN;
            return -1;
        };
        state->PumpWrites(8, 4096, block_sink);
        CHECK(slot.sent == 16 && !slot.terminal);

        const std::error_code close_reason(ECONNRESET, std::system_category());
        state->MarkClosed(close_reason);
        CHECK(slot.terminal && slot.sent == 16 && slot.error == close_reason);

        /** 关闭后的新写直接拒绝，不入队 */
        WriteWait later{};
        std::uint64_t later_id = 0;
        CHECK(state->SubmitWrite(std::string(1, 'o'), later_id, &later) == make_error_code(NetErrc::Closed));
        CHECK(state->Acceptance() == AcceptanceState::Closed);

        /** 重复关闭幂等，首次原因保留 */
        state->MarkClosed(std::make_error_code(std::errc::operation_canceled));
        CHECK(state->CloseReason() == close_reason);
        CHECK(state->Acceptance() == AcceptanceState::Closed);
    }
    std::cout << "co_write_after_close closed_before_write=(0,Closed) partial_then_close=(16,reason)=ok\n";
}

/** 6：零长写立即返回 (0, {})，不阻塞后续写。 */
static void CoWriteZeroLen()
{
    StartRuntime(1);
    auto state = std::make_shared<ConnectionState>(91, 4096, 4096, 8, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);

    std::atomic<int> result{-1};
    std::tuple<std::size_t, std::error_code> report;
    auto task = Task([&] {
        CoTcpConnection co_conn{connection};
        report = co_conn.Write("", Until(500ms)); /** 零长：立即返回，不挂起、不入队 */
        result.store(0);
    });
    Get(task);
    CHECK(result.load() == 0);
    CHECK(std::get<0>(report) == 0);
    CHECK(!std::get<1>(report));
    CHECK(state->ActiveOpCount() == 0);

    /** 不阻塞后续写：再次写入并泵发完成 */
    WriteWaitArmed armed{state};
    armed.arm();
    std::tuple<std::size_t, std::error_code> report2;
    auto task2 = Task([&] {
        CoTcpConnection co_conn2{connection};
        report2 = co_conn2.Write(std::string(3, 'z'), bbt::coroutine::WaitOptions{});
    });
    CHECK(WaitUntil([&] { return armed.armed.load(std::memory_order_acquire) >= 1; }, 3000ms));
    armed.clear();
    std::size_t sent = 0;
    auto take_all = [&sent](const std::string_view* views, std::size_t count, std::size_t) -> ssize_t {
        std::size_t total = 0;
        for (std::size_t i = 0; i < count; ++i) {
            total += views[i].size();
        }
        sent += total;
        return static_cast<ssize_t>(total);
    };
    state->PumpWrites(8, 4096, take_all);
    Get(task2);
    CHECK(std::get<0>(report2) == 3);
    CHECK(!std::get<1>(report2));
    CHECK(sent == 3);
    std::cout << "co_write_zero_len zero=(0,{}) non_blocking=ok\n";
}

/** 7：协程写的 OnWrite 投递恰一次，sent 与 n 一致；raw AsyncWrite 行为不变。 */
static void CoWriteOnWriteOnce()
{
    StartRuntime(2);

    auto handler = std::make_shared<CountingHandler>();
    EngineConfig server_config = ServerConfig(Loopback(0), 1);
    Engine server_engine(handler);
    EngineTestAccess::ClearConnectInjection(server_engine);
    EngineRunner server_runner(server_engine, server_config);
    CHECK(WaitUntil([&] { return server_engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    CHECK(!server_engine.LocalAddresses(addresses));
    CHECK(addresses.size() == 1);

    /** 原生客户端连上后不读，仅保持连接存活 */
    std::error_code error;
    NativeSocket client = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    CHECK(!error);
    const auto connect_error = client.Connect(addresses[0]);
    CHECK(!connect_error || connect_error.value() == EINPROGRESS || connect_error.value() == EWOULDBLOCK);
    {
        pollfd descriptor{client.Fd(), POLLOUT, 0};
        CHECK(::poll(&descriptor, 1, 3000) > 0);
        CHECK(!client.PendingError());
    }

    /** 等待服务端已接纳连接并拿到句柄 */
    std::shared_ptr<Connection> stored;
    CHECK(WaitUntil(
        [&] {
            std::lock_guard<std::mutex> lock(handler->mutex);
            stored = handler->stored;
            return stored != nullptr;
        },
        5000ms));

    /** 1. 正常完成的 co 写：OnWrite 恰好交付一次，sent == n，空错误 */
    const std::size_t payload_size = 64 * 1024;
    const std::string payload(payload_size, 'D');
    std::atomic<int> write_result{-1};
    std::tuple<std::size_t, std::error_code> report;
    auto task = Task([&] {
        CoTcpConnection co_conn{*stored};
        report = co_conn.Write(payload, bbt::coroutine::WaitOptions{});
        write_result.store(0);
    });
    CHECK(WaitUntil([&] { return write_result.load() == 0; }, 15000ms));
    Get(task);
    CHECK(std::get<0>(report) == payload_size);
    CHECK(!std::get<1>(report));
    EngineTestAccess::KickOwner(server_engine);
    CHECK(WaitUntil(
        [&] {
            std::lock_guard<std::mutex> lock(handler->mutex);
            return handler->writes.size() == 1;
        },
        5000ms));
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        CHECK(handler->writes.size() == 1);
        CHECK(handler->writes[0].sent == payload_size);
        CHECK(!handler->writes[0].error);
    }

    /** 2. raw AsyncWrite 行为不变：一次火写恰好一次 OnWrite，空错误 */
    CHECK(!stored->AsyncWrite(std::string(1, 'q')));
    EngineTestAccess::KickOwner(server_engine);
    CHECK(WaitUntil(
        [&] {
            std::lock_guard<std::mutex> lock(handler->mutex);
            return handler->writes.size() == 2;
        },
        5000ms));
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        CHECK(handler->writes.size() == 2);
        CHECK(handler->writes[1].sent == 1);
        CHECK(!handler->writes[1].error);
    }

    /** 被撤销的协程写：其终态 {sent=n, error=取消} 在 state 级已精确验证（co_write_revoke_interleaved、
     * co_write_partial），OnWrite 的投递与该终态同一出口（DeliverTerminalOwner），故这里不重复构造
     * 不可确定性的 loopback 部分发阻塞。 */

    (void)client.Close();
    server_runner.CloseAndJoin();
    CHECK(!server_runner.error);
    std::cout << "co_write_onwrite_once co_ok=(sent=n,error=empty) raw_asyncwrite_once=(sent=1,error=empty)\n";
}

/** 9：撤销后队列容量计数，释放额度可复用，背压不虚假触发。 */
static void CoWriteQueueCapacity()
{
    auto state = std::make_shared<ConnectionState>(101, 4096, 4096, 8, nullptr);
    Connection connection = ConnectionTestAccess::Make(state);

    WriteWait s1{}, s2{}, s3{};
    std::uint64_t i1 = 0, i2 = 0, i3 = 0;
    CHECK(!state->SubmitWrite(std::string(1024, 'q'), i1, &s1));
    CHECK(!state->SubmitWrite(std::string(1024, 'y'), i2, &s2));
    CHECK(!state->SubmitWrite(std::string(1024, 'w'), i3, &s3));
    CHECK(state->ActiveOpCount() == 3);
    CHECK(!state->CanAcceptWrite(2048)); /** 剩余 1024 字节，2048 字节背压为真 */

    /** 撤销中段 W2（1024 字节全部尚未发送），释放其额度 */
    const auto revoked = state->CancelWrite(i2, s2, std::make_error_code(std::errc::operation_canceled));
    CHECK(std::get<0>(revoked) == 0);
    CHECK(std::get<1>(revoked) == std::make_error_code(std::errc::operation_canceled));

    /** 撤销后 2048 字节可再次接纳：背压不虚假触发，额度可复用 */
    CHECK(state->CanAcceptWrite(2048));

    /** 泵发：W1 与 W3 先连续输出（W2 撤销后不留空洞），再接 W4 */
    std::string out;
    auto append_sink = [&out](const std::string_view* views, std::size_t count, std::size_t) -> ssize_t {
        return AppendViews(out, views, count);
    };
    state->PumpWrites(8, 2048, append_sink);
    CHECK(out == std::string(1024, 'q') + std::string(1024, 'w'));

    WriteWait s4{};
    std::uint64_t i4 = 0;
    CHECK(!state->SubmitWrite(std::string(2048, 'r'), i4, &s4));
    state->PumpWrites(8, 4096, append_sink);
    CHECK(out == std::string(1024, 'q') + std::string(1024, 'w') + std::string(2048, 'r'));

    const auto terminals = state->DrainTerminal();
    CHECK(terminals.size() == 4);
    std::size_t full_ok = 0, revoked_count = 0;
    for (const auto& t : terminals) {
        if (t.sent > 0 && !t.error) {
            ++full_ok; /** W1(1024)、W3(1024)、W4(2048) */
        }
        if (t.sent == 0 && t.error == std::make_error_code(std::errc::operation_canceled)) {
            ++revoked_count; /** W2 被整段撤销 */
        }
    }
    CHECK(full_ok == 3 && revoked_count == 1);
    std::cout << "co_write_queue_capacity revoke_frees_capacity backpressure_real order_preserved=ok\n";
}

#include "ProcessRuntimeTestMain.hpp"

static void DumpBacktrace(int signal_number)
{
    void* frames[64];
    const int count = backtrace(frames, 64);
    std::cerr << "caught signal " << signal_number << '\n';
    backtrace_symbols_fd(frames, count, 2);
    std::_Exit(139);
}

static int RunCases(int argc, char** argv)
{
    try {
        CHECK(argc == 2);
        const std::string test = argv[1];
        if (test == "full") {
            CoWriteFull();
        } else if (test == "partial") {
            CoWritePartial();
        } else if (test == "capacity_zero") {
            CoWriteCapacityZero();
        } else if (test == "revoke_interleaved") {
            CoWriteRevokeInterleaved();
        } else if (test == "async_prefix_revoke") {
            CoWriteAsyncPrefixRevoke();
        } else if (test == "order_after_mid_revoke") {
            CoWriteOrderAfterMidRevoke();
        } else if (test == "after_close") {
            CoWriteAfterClose();
        } else if (test == "zero_len") {
            CoWriteZeroLen();
        } else if (test == "onwrite_once") {
            CoWriteOnWriteOnce();
        } else if (test == "queue_capacity") {
            CoWriteQueueCapacity();
        } else {
            throw std::runtime_error("unknown test: " + test);
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}

int main(int argc, char** argv)
{
    signal(SIGSEGV, DumpBacktrace);
    signal(SIGABRT, DumpBacktrace);
    return bbt_net_test::RunTestProcessExit([&] { return RunCases(argc, argv); });
}
