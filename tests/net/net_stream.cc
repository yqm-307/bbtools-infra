#include <bbt/infra/net/detail/CoIoOperation.hpp>
#include <bbt/infra/net/detail/StreamConnection.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <cerrno>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <boost/asio/ip/tcp.hpp>
#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/io/IoExecutor.hpp>

#include <poll.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

// P3-b 私有 stream 连接 backend 契约测试：真实 TCP 动态端口/对端，无 mock I/O。
// 每进程 runtime 初始化一次（无 Stop）；七个短场景按 argv 分派，各 TIMEOUT 45。

using namespace bbt::infra::net;
using namespace std::chrono_literals;
using bbt::coroutine::WaitOptions;
using bbt::coroutine::WaitStatus;

static void Check(bool value, const char* expression, int line) {
    if (!value) { throw std::runtime_error("line " + std::to_string(line) + ": " + expression); }
}
#define CHECK(expression) Check(static_cast<bool>(expression), #expression, __LINE__)

template<class T> static T Get(std::future<T>& future) {
    CHECK(future.wait_for(5s) == std::future_status::ready);
    return future.get();
}
static void Get(std::future<void>& future) {
    CHECK(future.wait_for(5s) == std::future_status::ready);
    future.get();
}
static std::future<void> Task(std::function<void()> callback) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    g_scheduler->RegistCoroutineTask(65536, [promise, callback] {
        try { callback(); promise->set_value(); }
        catch (...) { promise->set_exception(std::current_exception()); }
    }, "net.stream");
    return future;
}
static void OnOwner(EventLoop& loop, std::function<void()> callback) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    CHECK(loop.Submit([promise, callback] {
        try { callback(); promise->set_value(); }
        catch (...) { promise->set_exception(std::current_exception()); }
    }) == SubmitStatus::Accepted);
    Get(future);
}
static WaitOptions Until(std::chrono::milliseconds duration) {
    return {std::chrono::steady_clock::now() + duration};
}
static std::size_t Count(const char* directory) {
    return static_cast<std::size_t>(std::distance(std::filesystem::directory_iterator(directory),
        std::filesystem::directory_iterator{}));
}
struct RuntimeIdentity { std::thread::id scheduler; std::thread::id processer; const void* context; };
static RuntimeIdentity WarmRuntime() {
    static const bool started = [] {
        g_bbt_coroutine_config->m_cfg_static_thread_num = 1;
        g_bbt_coroutine_config->m_cfg_stack_size = 65536;
        g_scheduler->Start(); // 每进程一次；没有 Stop 或第二个 driver。
        return true;
    }();
    CHECK(started);
    RuntimeIdentity identity;
    auto warm = Task([&identity] {
        identity.processer = std::this_thread::get_id(); // 单 worker fixture。
        bbt::coroutine::sync::CoWaiter waiter;
        CHECK(waiter.Wait(Until(5ms)) == WaitStatus::TimedOut); // 证明业务在 Processer 挂起。
    });
    Get(warm);
    std::promise<void> posted;
    auto ready = posted.get_future();
    auto executor = bbt::coroutine::io::GetExecutor();
    identity.context = &executor.context();
    boost::asio::post(executor, [&identity, &posted] {
        identity.scheduler = std::this_thread::get_id();
        posted.set_value();
    });
    Get(ready);
    CHECK(identity.scheduler != identity.processer);
    std::this_thread::sleep_for(30ms);
    return identity;
}

// ---- owner 侧观测门（产品回调与 Run 的 start 均在 owner 线程执行，无锁）。----
struct Gate {
    std::shared_ptr<CoIoOperation> traffic_wait;
    std::size_t traffic_seed{0};
    std::shared_ptr<CoIoOperation> drained_wait;
    std::shared_ptr<CoIoOperation> closed_wait;
    std::size_t traffic_total{0};
    std::size_t drained_total{0};
    bool closed{false};
    std::error_code closed_error;
    // 关闭原因经 promise/future 同步出域：构造即装配，业务侧不裸读 closed。
    std::shared_ptr<std::promise<std::error_code>> closed_promise{
        std::make_shared<std::promise<std::error_code>>()};
    std::shared_future<std::error_code> closed_future{closed_promise->get_future()};
    bool closed_signaled{false}; // owner-only：set_value 恰一次。
};
static void GateTraffic(Gate& gate) {
    ++gate.traffic_total;
    if (gate.traffic_wait && gate.traffic_total > gate.traffic_seed) {
        auto operation = gate.traffic_wait;
        gate.traffic_wait.reset();
        CHECK(operation->Complete({gate.traffic_total, {}})); // bytes=total 随 operation 同步出域。
    }
}
static void GateDrained(Gate& gate) {
    ++gate.drained_total;
    if (gate.drained_wait) {
        auto operation = gate.drained_wait;
        gate.drained_wait.reset();
        CHECK(operation->Complete({1, {}}));
    }
}
static void GateClosed(Gate& gate, const std::error_code& error) {
    gate.closed = true;
    gate.closed_error = error;
    if (!gate.closed_signaled) { // 先同步关闭原因再完成等待：业务 get() 必已就绪。
        gate.closed_signaled = true;
        if (gate.closed_promise) { gate.closed_promise->set_value(error); }
    }
    if (gate.closed_wait) {
        auto operation = gate.closed_wait;
        gate.closed_wait.reset();
        CHECK(operation->Complete({1, {}}));
    }
}
static void WaitTraffic(EventLoop& loop, Gate& gate, std::size_t& seen) {
    const auto result = CoIoOperation::Run(loop, Until(2s), [&gate, seen](auto operation) {
        if (gate.traffic_total > seen) { CHECK(operation->Complete({gate.traffic_total, {}})); }
        else { gate.traffic_seed = seen; gate.traffic_wait = operation; }
    });
    CHECK(result.wait_status == WaitStatus::Completed && result.io.bytes >= seen + 1);
    seen = result.io.bytes; // payload 同步出域：业务侧不再跨域读 traffic_total。
}
static void WaitDrained(EventLoop& loop, Gate& gate) {
    const auto result = CoIoOperation::Run(loop, Until(5s), [&gate](auto operation) {
        if (gate.drained_total > 0) { CHECK(operation->Complete({1, {}})); }
        else { gate.drained_wait = operation; }
    });
    CHECK(result.wait_status == WaitStatus::Completed && result.io.bytes == 1);
}
static std::error_code WaitClosed(EventLoop& loop, Gate& gate) {
    const auto result = CoIoOperation::Run(loop, Until(2s), [&gate](auto operation) {
        if (gate.closed) { CHECK(operation->Complete({1, {}})); }
        else { gate.closed_wait = operation; }
    });
    CHECK(result.wait_status == WaitStatus::Completed && result.io.bytes == 1);
    CHECK(gate.closed_future.wait_for(0s) == std::future_status::ready);
    return gate.closed_future.get(); // 关闭原因经 promise/future 同步出域。
}

// ---- owner 侧操作/采样 ----
static EventSnapshot OwnerTake(EventLoop& loop,
    const std::shared_ptr<StreamConnection>& connection, std::error_code& error) {
    auto holder = std::make_shared<EventSnapshot>();
    OnOwner(loop, [&] { *holder = connection->TakeSnapshot(error); });
    return std::move(*holder);
}
static std::error_code OwnerReturn(EventLoop& loop,
    const std::shared_ptr<StreamConnection>& connection, EventSnapshot&& snapshot,
    std::size_t consumed) {
    auto holder = std::make_shared<EventSnapshot>(std::move(snapshot));
    auto error_holder = std::make_shared<std::error_code>();
    OnOwner(loop, [&] {
        *error_holder = connection->ReturnSnapshot(std::move(*holder), consumed);
    });
    return *error_holder;
}
static std::error_code OwnerWrite(EventLoop& loop,
    const std::shared_ptr<StreamConnection>& connection, std::string data) {
    auto error_holder = std::make_shared<std::error_code>();
    OnOwner(loop, [&] { *error_holder = connection->Write(std::move(data)); });
    return *error_holder;
}
static std::error_code OwnerClose(EventLoop& loop,
    const std::shared_ptr<StreamConnection>& connection, std::error_code reason) {
    auto error_holder = std::make_shared<std::error_code>();
    OnOwner(loop, [&] { *error_holder = connection->Close(std::move(reason)); });
    return *error_holder;
}
static StreamConnection::Stats OwnerStats(EventLoop& loop,
    const std::shared_ptr<StreamConnection>& connection) {
    auto holder = std::make_shared<StreamConnection::Stats>();
    OnOwner(loop, [&] { *holder = connection->GetStats(); });
    return *holder;
}
static std::size_t OwnerPending(EventLoop& loop,
    const std::shared_ptr<StreamConnection>& connection) {
    auto holder = std::make_shared<std::size_t>();
    OnOwner(loop, [&] { *holder = connection->PendingBytes(); });
    return *holder;
}
static std::size_t OwnerBuffered(EventLoop& loop,
    const std::shared_ptr<StreamConnection>& connection) {
    auto holder = std::make_shared<std::size_t>();
    OnOwner(loop, [&] { *holder = connection->BufferedBytes(); });
    return *holder;
}

// ---- TCP 对端 fixture（真实动态端口；不是完整 server）----
static char PatternAt(std::size_t offset) {
    return static_cast<char>((offset * 31 + 7) % 251);
}
static std::string PatternChunk(std::size_t offset, std::size_t size) {
    std::string chunk(size, '\0');
    for (std::size_t i = 0; i < size; ++i) { chunk[i] = PatternAt(offset + i); }
    return chunk;
}
static NativeSocket OpenStream() {
    std::error_code error;
    auto socket = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    CHECK(!error && socket.Fd() >= 0);
    return socket;
}
static Address ListenOn(NativeSocket& listener) {
    std::error_code error;
    CHECK(!listener.Bind(Address::Ip("127.0.0.1", 0, error)) && !error);
    CHECK(!listener.Listen(8));
    const auto bound = listener.LocalAddress(error);
    CHECK(!error);
    return bound;
}
static NativeSocket ConnectTo(const Address& bound) {
    auto client = OpenStream();
    const auto connected = client.Connect(bound);
    CHECK(!connected || connected.value() == EINPROGRESS);
    for (int i = 0; i < 1000; ++i) { // 非阻塞：有界等待连接建立再接纳。
        if (!client.PendingError()) { return client; }
        std::this_thread::sleep_for(1ms);
    }
    CHECK(!client.PendingError());
    return client;
}
// 经既有 CoIoOperation::Run 在 owner 上等待接纳并创建连接（等待「接纳」）。
static std::shared_ptr<StreamConnection> AcceptStream(EventLoop& loop,
    NativeSocket& listener, const NormalizedOptions& options,
    const std::shared_ptr<Gate>& gate) {
    std::shared_ptr<StreamConnection> connection;
    std::error_code failure = std::make_error_code(std::errc::io_error);
    auto socket = std::make_shared<NativeSocket>();
    auto* loop_pointer = &loop;
    const auto result = CoIoOperation::Run(loop, Until(2s),
        [&, socket, loop_pointer](auto operation) {
            CHECK(loop_pointer->IsOwner());
            Address peer;
            std::error_code accept_error;
            *socket = listener.Accept(peer, accept_error); // 已就绪：单次接纳。
            if (accept_error || socket->Fd() < 0) {
                failure = accept_error ? accept_error
                    : std::make_error_code(std::errc::io_error);
                std::cerr << "accept failed: " << failure.message()
                    << " fd=" << socket->Fd() << '\n';
                operation->Complete({0, failure});
                return;
            }
            StreamConnection::Callbacks callbacks;
            callbacks.on_traffic = [gate, loop_pointer](StreamConnection&) {
                CHECK(loop_pointer->IsOwner() && bbt::coroutine::GetLocalCoroutineId() == 0);
                GateTraffic(*gate);
            };
            callbacks.on_drained = [gate, loop_pointer](StreamConnection&) {
                CHECK(loop_pointer->IsOwner() && bbt::coroutine::GetLocalCoroutineId() == 0);
                GateDrained(*gate);
            };
            callbacks.on_closed = [gate, loop_pointer](StreamConnection&,
                const std::error_code& error) {
                CHECK(loop_pointer->IsOwner() && bbt::coroutine::GetLocalCoroutineId() == 0);
                GateClosed(*gate, error);
            };
            connection = StreamConnection::Create(loop, std::move(*socket), options,
                std::move(callbacks), failure);
            operation->Complete({connection ? std::size_t{1} : std::size_t{0},
                connection ? std::error_code{} : failure});
        });
    if (result.wait_status != WaitStatus::Completed || result.io.error) {
        std::cerr << "accept run failed: status=" << static_cast<int>(result.wait_status)
            << " submit=" << static_cast<int>(result.submit_status)
            << " error=" << (result.io.error ? result.io.error.message() : std::string("none"))
            << '\n';
    }
    CHECK(result.wait_status == WaitStatus::Completed && !result.io.error);
    CHECK(connection);
    return connection;
}
// 接纳/创建经业务协程进入（CoIoOperation::Run 需要 Processer 上的协程上下文）。
static std::shared_ptr<StreamConnection> AcceptStreamTask(EventLoop& loop,
    NativeSocket& listener, const NormalizedOptions& options,
    const std::shared_ptr<Gate>& gate) {
    // 控制线程先等就绪：不让业务 Processer 或 I/O owner 执行阻塞 poll。
    pollfd pfd{listener.Fd(), POLLIN, 0};
    CHECK(::poll(&pfd, 1, 2000) == 1 && (pfd.revents & POLLIN) != 0);
    auto holder = std::make_shared<std::shared_ptr<StreamConnection>>();
    auto future = Task([&] { *holder = AcceptStream(loop, listener, options, gate); });
    Get(future);
    return std::move(*holder);
}
static NormalizedOptions StreamOptions(std::size_t read_cap, std::size_t max_inbound,
    std::size_t write_cap, std::size_t max_outbound, std::size_t max_pending) {
    Options raw;
    raw.read_buffer_cap = static_cast<std::int64_t>(read_cap);
    raw.write_buffer_cap = static_cast<std::int64_t>(write_cap);
    raw.max_inbound_bytes = static_cast<std::int64_t>(max_inbound);
    raw.max_outbound_bytes = static_cast<std::int64_t>(max_outbound);
    raw.max_pending_events = static_cast<std::int64_t>(max_pending);
    NormalizedOptions options;
    CHECK(!NormalizeOptions(raw, 1, options));
    return options;
}
static void ShutdownConnection(EventLoop& loop,
    const std::shared_ptr<StreamConnection>& connection) {
    OwnerClose(loop, connection, {});
    OnOwner(loop, [] {}); // 让 abort 晚回调全部退出后再释放引用。
}
static void ShutdownLoop(EventLoop& loop) {
    loop.RequestStop();
    CHECK(loop.Join());
    CHECK(loop.FailureCount() == 0);
}

// 场景一 net.stream_traffic：同 owner 双连接、独立快照、尾部保序、高水位。
static void ScenarioTraffic() {
    const auto runtime = WarmRuntime();
    EventLoop loop(64);
    CHECK(loop.Start());
    auto options = StreamOptions(1024, 8192, 4096, 1024 * 1024, 64);
    auto listener = std::make_shared<NativeSocket>(OpenStream());
    const auto bound = ListenOn(*listener);
    auto gate_a = std::make_shared<Gate>();
    auto gate_b = std::make_shared<Gate>();
    auto client_a = std::make_shared<NativeSocket>(ConnectTo(bound));
    auto conn_a = AcceptStreamTask(loop, *listener, options, gate_a);
    auto client_b = std::make_shared<NativeSocket>(ConnectTo(bound));
    auto conn_b = AcceptStreamTask(loop, *listener, options, gate_b);
    CHECK(conn_a->Id() != conn_b->Id());

    auto traffic = Task([&] {
        const auto business_thread = std::this_thread::get_id();
        CHECK(business_thread == runtime.processer); // 业务运行在 Processer。
        std::size_t seen_a = 0;
        std::size_t seen_b = 0;
        std::error_code error;

        // 阶段一：A 持独立快照并挂起；B 的真实流量与进展协程继续推进。
        CHECK(client_a->Send("hello,", 6).bytes == 6);
        WaitTraffic(loop, *gate_a, seen_a);
        auto snapshot = OwnerTake(loop, conn_a, error);
        CHECK(!error && snapshot.Remaining() == 6 && snapshot.Peek() == "hello,");
        CHECK(std::this_thread::get_id() == business_thread
            && business_thread != loop.OwnerId()); // 业务运行在 Processer。
        CHECK(client_b->Send("B1", 2).bytes == 2);
        WaitTraffic(loop, *gate_b, seen_b);        // A 的业务挂起期间推进 B。
        auto snapshot_b = OwnerTake(loop, conn_b, error);
        CHECK(!error && snapshot_b.Peek() == "B1");
        std::string sink_b; // 业务先消费再带已消费位置归还。
        const auto used_b = snapshot_b.Remaining();
        CHECK(!snapshot_b.Consume(used_b, sink_b) && sink_b == "B1");
        CHECK(!OwnerReturn(loop, conn_b, std::move(snapshot_b), used_b));
        CHECK(std::this_thread::get_id() == business_thread
            && business_thread != loop.OwnerId()); // B 推进仍在原业务协程/线程。
        CHECK(snapshot.Peek() == "hello,");        // 挂起期间独立 backing 不变。

        // 阶段二：新字节不改已持有快照；尾部先于新字节保序合并（半包保序）。
        CHECK(client_a->Send("world-tail", 10).bytes == 10);
        std::this_thread::sleep_for(50ms);         // 新字节已在内核/借出期被暂停。
        CHECK(snapshot.Peek() == "hello,");
        std::string sink_head; // 先消费 "hel"，再带已消费位置归还。
        CHECK(!snapshot.Consume(3, sink_head) && sink_head == "hel");
        CHECK(!OwnerReturn(loop, conn_a, std::move(snapshot), 3)); // 消费 "hel"。
        WaitTraffic(loop, *gate_a, seen_a);
        auto merged = OwnerTake(loop, conn_a, error);
        CHECK(!error && merged.Peek() == "lo,world-tail");         // 尾部在前。
        std::string sink_merged; // 全量消费后归还：consumed == 借出量。
        const auto merged_bytes = merged.Remaining();
        CHECK(!merged.Consume(merged_bytes, sink_merged));
        CHECK(!OwnerReturn(loop, conn_a, std::move(merged), merged_bytes));

        // 阶段三：不消费且无新数据 → 事件与读臂稳定有界；借出期读泵暂停。
        CHECK(client_a->Send("S2DATA", 6).bytes == 6);
        WaitTraffic(loop, *gate_a, seen_a);
        auto held = OwnerTake(loop, conn_a, error);
        CHECK(!error && held.Peek() == "S2DATA");
        const auto events_before = OwnerStats(loop, conn_a).traffic_notifications;
        const auto arms_before = OwnerStats(loop, conn_a).read_arms;
        std::this_thread::sleep_for(150ms);
        const auto stats_stable = OwnerStats(loop, conn_a);
        CHECK(stats_stable.traffic_notifications == events_before);
        CHECK(stats_stable.read_arms == arms_before);
        CHECK(!OwnerReturn(loop, conn_a, std::move(held), 0));     // 0 消费归还。
        const auto events_after_return = OwnerStats(loop, conn_a).traffic_notifications;
        CHECK(events_after_return == events_before);               // 归还不派发。
        std::this_thread::sleep_for(100ms);
        CHECK(OwnerStats(loop, conn_a).traffic_notifications == events_before);
    });
    Get(traffic);

    // 阶段四：连接 C 高水位暂停与消费后恢复，全程不丢字节。
    auto gate_c = std::make_shared<Gate>();
    auto client_c = std::make_shared<NativeSocket>(ConnectTo(bound));
    auto conn_c = AcceptStreamTask(loop, *listener, options, gate_c);
    constexpr std::size_t kTotal = 65536;
    std::thread flood([&] { // 非阻塞对端：poll 可写 + 半包续发，不丢不乱。
        for (std::size_t offset = 0; offset < kTotal;) {
            pollfd pfd{client_c->Fd(), POLLOUT, 0};
            CHECK(::poll(&pfd, 1, 1000) == 1);
            const auto chunk = PatternChunk(offset, 4096);
            auto sent = client_c->Send(chunk.data(), chunk.size());
            if (sent.error.value() == EAGAIN) { sent.bytes = 0; sent.error.clear(); }
            CHECK(!sent.error);
            CHECK(sent.bytes <= chunk.size());
            offset += sent.bytes; // 未发完的余量下一轮续发（保持字节序）。
        }
    });
    auto consumer = Task([&] {
        std::size_t seen_c = 0;
        for (int i = 0; i < 200; ++i) { // 等高水位：积压打满即暂停。
            if (OwnerBuffered(loop, conn_c) == 8192) { break; }
            std::this_thread::sleep_for(10ms);
        }
        CHECK(OwnerBuffered(loop, conn_c) == 8192);
        const auto arms_watermark = OwnerStats(loop, conn_c).read_arms;
        std::this_thread::sleep_for(100ms); // 高水位暂停期读臂不再增长。
        CHECK(OwnerStats(loop, conn_c).read_arms == arms_watermark);
        std::size_t consumed_total = 0;
        while (consumed_total < kTotal) {
            std::error_code error;
            WaitTraffic(loop, *gate_c, seen_c);
            auto snapshot = OwnerTake(loop, conn_c, error);
            CHECK(!error);
            const auto view = snapshot.Peek();
            const auto used = view.size();
            std::string sink;
            CHECK(!snapshot.Consume(used, sink));
            for (std::size_t i = 0; i < sink.size(); ++i) {
                CHECK(sink[i] == PatternAt(consumed_total + i)); // 保序不丢。
            }
            CHECK(!OwnerReturn(loop, conn_c, std::move(snapshot), used));
            consumed_total += used;
        }
        CHECK(consumed_total == kTotal);
    });
    Get(consumer);
    flood.join();

    std::cout << "stream_traffic conns=" << conn_a->Id() << '/' << conn_b->Id() << '/'
        << conn_c->Id() << " a_stats=" << OwnerStats(loop, conn_a).traffic_notifications
        << " c_read_bytes=" << OwnerStats(loop, conn_c).read_bytes
        << " owner=" << loop.OwnerId() << " processer=" << runtime.processer << '\n';
    for (auto* connection : {&conn_a, &conn_b, &conn_c}) { ShutdownConnection(loop, *connection); }
    for (auto* socket : {&client_a, &client_b, &client_c}) { (void)(*socket)->Close(); }
    listener->Close();
    ShutdownLoop(loop);
}

// 场景二 net.stream_write：真实 pending/partial/背压、预算拒绝、FIFO、空队列不空转。
static void ScenarioWrite() {
    const auto runtime = WarmRuntime();
    EventLoop loop(64);
    CHECK(loop.Start());
    auto options = StreamOptions(1024, 8192, 4096, 1024 * 1024, 64);
    options.socket_send_buffer = 4096; // 小内核 sendbuf：真实背压。
    auto listener = std::make_shared<NativeSocket>(OpenStream());
    const auto bound = ListenOn(*listener);
    auto gate = std::make_shared<Gate>();
    auto client = std::make_shared<NativeSocket>(ConnectTo(bound)); // 连接后暂停读取。
    auto conn = AcceptStreamTask(loop, *listener, options, gate);
    // 实际段语义核算：Write 预检在 SegmentCount()>=max_pending_events(64) 即拒绝，
    // 同一 owner 动作内最多接纳 63×8192=516096 字节（ring 1 段 + spill 63 段）。
    constexpr std::size_t kTotal = 63 * 8192;
    constexpr std::size_t kChunk = 8192;

    {   // 预算拒绝：临时大对象释放后队列不变、旧内容不损。
        std::string huge(2 * 1024 * 1024, 'x');
        const auto error = OwnerWrite(loop, conn, std::move(huge));
        CHECK(error == std::make_error_code(std::errc::no_buffer_space));
    }
    const auto pending_before = OwnerPending(loop, conn);
    CHECK(pending_before == 0); // 拒绝后队列仍为空：旧内容为空集。

    std::size_t fill_pending = 0;
    std::size_t fill_segments = 0;
    std::size_t fill_accepted = 0;
    OnOwner(loop, [&] { // 同一 owner 动作：写 handler 不可能中途排空，核算确定。
        CHECK(!conn->Write(PatternChunk(0, kChunk)));
        const auto byte_pending = conn->PendingBytes();
        const auto byte_segments = conn->PendingSegments();
        const auto byte_stats = conn->GetStats();
        CHECK(byte_pending == kChunk && byte_segments < options.max_pending_events);
        // 字节界单独违反：队列非空，但段数远未到界，不能靠另一条件兜底。
        const auto byte_rejected = conn->Write(std::string(2 * 1024 * 1024, 'x'));
        CHECK(byte_rejected == std::make_error_code(std::errc::no_buffer_space));
        CHECK(conn->PendingBytes() == byte_pending);
        CHECK(conn->PendingSegments() == byte_segments);
        CHECK(conn->GetStats().writes_accepted == byte_stats.writes_accepted);
        CHECK(conn->GetStats().write_bytes_queued == byte_stats.write_bytes_queued);
        CHECK(conn->GetStats().write_bytes_sent == byte_stats.write_bytes_sent);
        for (std::size_t i = 1; i < kTotal / kChunk; ++i) {
            // 输入是临时对象：队列复制取得不可变所有权。
            const auto error = conn->Write(PatternChunk(i * kChunk, kChunk));
            CHECK(!error);
        }
        fill_pending = conn->PendingBytes();
        fill_segments = conn->PendingSegments();
        fill_accepted = conn->GetStats().writes_accepted;
        CHECK(fill_pending == kTotal);
        CHECK(fill_segments == 64); // ring 1 段 + spill 63 段 = max_pending_events。
        CHECK(fill_accepted == kTotal / kChunk);
        // 段界单独违反：总字节仍有空间，拒绝不能靠字节界兜底。
        CHECK(kChunk <= options.max_outbound_bytes - fill_pending);
        const auto segment_rejected = conn->Write(PatternChunk(kTotal, kChunk));
        CHECK(segment_rejected == std::make_error_code(std::errc::no_buffer_space));
        // 两次拒绝前后 PendingBytes/PendingSegments/接纳与发送统计逐项相等。
        CHECK(conn->PendingBytes() == fill_pending);
        CHECK(conn->PendingSegments() == fill_segments);
        CHECK(conn->GetStats().writes_accepted == fill_accepted);
        CHECK(conn->GetStats().write_bytes_queued == fill_pending);
        CHECK(conn->GetStats().write_bytes_sent == 0);
    });

    std::size_t observed_peak = 0; // 客户端暂停读取：等待真实 pending 稳定。
    {
        std::size_t stable = 0;
        std::size_t last = 0;
        for (int i = 0; i < 400 && stable < 5; ++i) {
            const auto now = OwnerPending(loop, conn);
            if (now == last) { ++stable; } else { stable = 0; last = now; }
            observed_peak = std::max(observed_peak, now);
            std::this_thread::sleep_for(5ms);
        }
    }
    CHECK(observed_peak > 32768); // 远超 ~8KiB 内核缓冲：真实待发/背压证据。
    CHECK(observed_peak <= 1024 * 1024);
    const auto stalled = OwnerStats(loop, conn);
    CHECK(stalled.write_bytes_queued == kTotal);
    CHECK(stalled.write_bytes_sent < kTotal); // 内核未全收：存在未发送积压。
    CHECK(OwnerPending(loop, conn) > 0); // 客户端未读完：仍有未发送积压。

    std::thread reader([&] { // 恢复对端读取：poll 可读 + 校验完整 FIFO 字节。
        std::size_t received = 0;
        char buffer[8192];
        while (received < kTotal) {
            pollfd pfd{client->Fd(), POLLIN, 0};
            CHECK(::poll(&pfd, 1, 1000) == 1);
            const auto n = ::recv(client->Fd(), buffer, sizeof buffer, 0);
            if (n == 0) { throw std::runtime_error("reader EOF at " + std::to_string(received)); }
            if (n < 0) {
                CHECK(errno == EAGAIN);
                continue;
            }
            for (ssize_t i = 0; i < n; ++i) {
                CHECK(buffer[i] == PatternAt(received + static_cast<std::size_t>(i)));
            }
            received += static_cast<std::size_t>(n);
        }
        CHECK(received == kTotal);
        pollfd residue{client->Fd(), POLLIN, 0};
        CHECK(::poll(&residue, 1, 0) == 0); // 被拒内容未混入：满量读取后无余字节。
    });
    auto drained = Task([&] { WaitDrained(loop, *gate); }); // 队列清空=内核全收。
    Get(drained);
    reader.join();
    const auto stats = OwnerStats(loop, conn);
    CHECK(stats.write_bytes_queued == kTotal);
    CHECK(stats.write_bytes_sent == kTotal);
    CHECK(stats.drained_notifications >= 1); // 内核先吃满前队列可能瞬时清空：按真实清空次数计。
    CHECK(stats.peak_pending_bytes >= observed_peak);
    CHECK(stats.peak_pending_bytes <= 1024 * 1024);
    CHECK(stats.traffic_notifications == 0); // 本场景无入站流量。

    {   // 空队列不空转：drained 后写臂与 drained 计数稳定。
        const auto arms_after = OwnerStats(loop, conn).write_arms;
        std::this_thread::sleep_for(150ms);
        const auto idle = OwnerStats(loop, conn);
        CHECK(idle.write_arms == arms_after); // 空队列不空转：不再重新挂写臂。
        CHECK(idle.drained_notifications == stats.drained_notifications); // 无新写入不再派发。
        CHECK(idle.peak_pending_bytes == stats.peak_pending_bytes);
    }

    std::cout << "stream_write queued=" << stats.write_bytes_queued << " sent="
        << stats.write_bytes_sent << " peak_pending=" << stats.peak_pending_bytes
        << " observed_peak=" << observed_peak
        << " fill_segments=" << fill_segments
        << " byte_reject=no_buffer_space segment_reject=no_buffer_space"
        << " independent_limits=true"
        << " write_arms=" << stats.write_arms << " drained=1"
        << " owner=" << loop.OwnerId() << " processer=" << runtime.processer << '\n';
    ShutdownConnection(loop, conn);
    (void)client->Close();
    listener->Close();
    ShutdownLoop(loop);
}

// 场景三 net.stream_close：真实 inflight read/write + 持有快照时物理收口。
static void ScenarioClose() {
    WarmRuntime();
    const auto fds = Count("/proc/self/fd");
    const auto threads = Count("/proc/self/task");
    {   // loop 及全部连接在计数前出作用域析构（asio epoll/eventfd 一并收口）。
    EventLoop loop(64);
    CHECK(loop.Start());
    auto options = StreamOptions(1024, 8192, 4096, 1024 * 1024, 64);
    options.socket_send_buffer = 4096;
    auto listener = std::make_shared<NativeSocket>(OpenStream());
    const auto bound = ListenOn(*listener);
    auto gate = std::make_shared<Gate>();
    auto client = std::make_shared<NativeSocket>(ConnectTo(bound));
    auto conn = AcceptStreamTask(loop, *listener, options, gate);
    std::weak_ptr<StreamConnection> weak = conn;
    const auto close_reason = std::make_error_code(std::errc::operation_canceled);

    CHECK(client->Send(PatternChunk(0, 4096).data(), 4096).bytes == 4096);
    auto held = std::make_shared<EventSnapshot>();
    auto taker = Task([&] { // 业务协程：等 traffic 并持有独立快照（读泵借出暂停）。
        std::size_t seen = 0;
        std::error_code error;
        WaitTraffic(loop, *gate, seen);
        *held = OwnerTake(loop, conn, error);
        CHECK(!error && held->Remaining() == 4096);
    });
    Get(taker);

    CHECK(!OwnerWrite(loop, conn, std::string(512 * 1024, 'w'))); // 真实待发积压（远超内核可吞量）。
    bool inflight = false; // read/write 都有在途就绪等待。
    for (int i = 0; i < 200 && !inflight; ++i) {
        OnOwner(loop, [&] { inflight = conn->ReadWaiting() && conn->WriteWaiting(); });
        if (!inflight) { std::this_thread::sleep_for(5ms); }
    }
    CHECK(inflight);

    std::atomic<bool> resumed{false};
    std::atomic<int> progress_effects{0};
    auto closer = Task([&] { // 关闭等待经既有 CoIoOperation 挂起、让出 worker。
        CHECK(WaitClosed(loop, *gate) == close_reason); // 关闭原因经 future 同步校验。
        resumed = true;
    });
    auto progress = Task([&] { // closer 挂起期间另一协程推进。
        CHECK(!resumed.load());
        ++progress_effects;
    });
    Get(progress);
    CHECK(!resumed.load());

    CHECK(!OwnerClose(loop, conn, close_reason)); // 撤销+内核收口后才发布完成。
    Get(closer);
    CHECK(resumed.load() && progress_effects == 1);

    // 物理关闭不等快照归还：关闭完成后快照仍可安全读取/归还（只清理）。
    const auto view = held->Peek();
    CHECK(view.size() == 4096);
    for (std::size_t i = 0; i < view.size(); ++i) { CHECK(view[i] == PatternAt(i)); }
    std::string sink_held; // 部分消费 1024 后归还：尾部只清理不恢复读泵。
    CHECK(!held->Consume(1024, sink_held) && sink_held.size() == 1024);
    CHECK(!OwnerReturn(loop, conn, std::move(*held), 1024));
    // Closing/Closed 后拒绝新写/新事件；重复归还同样拒绝。
    CHECK(OwnerWrite(loop, conn, "late") ==
        std::make_error_code(std::errc::operation_not_supported));
    std::error_code take_error;
    OwnerTake(loop, conn, take_error);
    CHECK(take_error == std::make_error_code(std::errc::operation_not_supported));
    CHECK(OwnerReturn(loop, conn, EventSnapshot{}, 0) ==
        std::make_error_code(std::errc::operation_not_supported));

    std::size_t guard_exits_observed = 0; // abort 晚回调经 reactor 异步派发：有界等待出口。
    for (int i = 0; i < 200 && guard_exits_observed < 2; ++i) {
        guard_exits_observed = OwnerStats(loop, conn).guard_exits;
        if (guard_exits_observed < 2) { std::this_thread::sleep_for(5ms); }
    }
    const auto stats = OwnerStats(loop, conn);
    CHECK(stats.physical_closes == 1);        // 单次物理收口。
    CHECK(guard_exits_observed == 2);         // read/write 晚回调安全出口。
    CHECK(stats.snapshots_taken == 1 && stats.snapshots_returned == 1);
    CHECK(stats.snapshot_consumed_bytes == 1024);
    bool state_closed = false; // 终态查询也经 owner（非 owner 拒绝是产品契约）。
    OnOwner(loop, [&] { state_closed = conn->ConnectionState() == StreamConnection::State::Closed; });
    CHECK(state_closed);
    CHECK(!OwnerClose(loop, conn, {}));       // 重复关闭幂等。
    CHECK(OwnerStats(loop, conn).physical_closes == 1);
    std::cout << "stream_close guard_exits=2 physical_closes=1 late_write_rejected"
        << " snapshot_cleanup=ok reason=operation_canceled\n";

    conn.reset(); // 最后引用释放：晚回调全部退出后 weak 可释放（有界等待）。
    bool released = false;
    for (int i = 0; i < 200 && !released; ++i) {
        OnOwner(loop, [&] { released = weak.expired(); });
        if (!released) { std::this_thread::sleep_for(5ms); }
    }
    CHECK(released);
    ShutdownLoop(loop);
    } // loop/io_context 析构：唯一 fd 与 owner 线程资源收口。
    std::this_thread::sleep_for(50ms);
    const auto final_fds = Count("/proc/self/fd");
    const auto final_threads = Count("/proc/self/task");
    CHECK(final_fds == fds && final_threads == threads); // 唯一 fd/owner 线程回基线。
    std::cout << "stream_close fd=" << fds << "->" << final_fds << " threads=" << threads
        << "->" << final_threads << " weak_released=true\n";
}


// ---- 场景四/五公共：带 owner/协程守卫的回调组（复用既有 Gate）。----
static StreamConnection::Callbacks GateCallbacks(const std::shared_ptr<Gate>& gate,
    EventLoop* loop) {
    StreamConnection::Callbacks callbacks;
    callbacks.on_traffic = [gate, loop](StreamConnection&) {
        CHECK(loop->IsOwner() && bbt::coroutine::GetLocalCoroutineId() == 0);
        GateTraffic(*gate);
    };
    callbacks.on_drained = [gate, loop](StreamConnection&) {
        CHECK(loop->IsOwner() && bbt::coroutine::GetLocalCoroutineId() == 0);
        GateDrained(*gate);
    };
    callbacks.on_closed = [gate, loop](StreamConnection&,
        const std::error_code& error) {
        CHECK(loop->IsOwner() && bbt::coroutine::GetLocalCoroutineId() == 0);
        GateClosed(*gate, error);
    };
    return callbacks;
}

// 场景四 net.stream_peer_rst：真实对端 RST 保留 ECONNRESET 非EOF；
// 同场景独立连接干净 shutdown(SHUT_WR)/FIN 保持 ReadEof=true、无错误。
static void ScenarioPeerRst() {
    WarmRuntime();
    EventLoop loop(64);
    CHECK(loop.Start());
    auto options = StreamOptions(1024, 8192, 4096, 1024 * 1024, 64);
    auto listener = std::make_shared<NativeSocket>(OpenStream());
    const auto bound = ListenOn(*listener);
    {   // abortive RST：对端 SO_LINGER{1,0} 后 close → 本端读得 ECONNRESET。
        auto gate = std::make_shared<Gate>();
        auto client = std::make_shared<NativeSocket>(ConnectTo(bound));
        auto conn = AcceptStreamTask(loop, *listener, options, gate);
        auto probe = Task([&] {
            std::size_t seen = 0;
            CHECK(client->Send("ping", 4).bytes == 4);
            WaitTraffic(loop, *gate, seen); // 数据先被连接读走，RST 再到。
            CHECK(!client->SetLinger(0));   // abortive：close 发 RST 不发 FIN。
            (void)client->Close();
            WaitTraffic(loop, *gate, seen); // RST 终止通知（现有通知恰好一次）。
            bool eof = true;
            std::error_code io = std::make_error_code(std::errc::io_error);
            OnOwner(loop, [&] {             // 采样只在 owner 上。
                eof = conn->ReadEof();
                io = conn->LastIoError();
            });
            CHECK(!eof); // RST 不得伪装成 EOF。
            std::cout << "rst_sample eof=" << eof << " io_value=" << io.value()
                << " io_msg=" << io.message() << '\n';
            CHECK(io.value() == ECONNRESET); // LastIoError 保留 ECONNRESET。
            CHECK(io.category() == std::system_category());
        });
        Get(probe);
        ShutdownConnection(loop, conn);
    }
    {   // 干净 FIN：shutdown(SHUT_WR) → ReadEof=true 且 LastIoError 保持为空。
        auto gate = std::make_shared<Gate>();
        auto client = std::make_shared<NativeSocket>(ConnectTo(bound));
        auto conn = AcceptStreamTask(loop, *listener, options, gate);
        auto probe = Task([&] {
            std::size_t seen = 0;
            CHECK(client->Send("bye", 3).bytes == 3);
            WaitTraffic(loop, *gate, seen);
            CHECK(!client->Shutdown(SHUT_WR));
            WaitTraffic(loop, *gate, seen); // 干净 EOF 一次性通知。
            bool eof = false;
            std::error_code io = std::make_error_code(std::errc::io_error);
            OnOwner(loop, [&] {
                eof = conn->ReadEof();
                io = conn->LastIoError();
            });
            CHECK(eof);
            CHECK(!io); // 干净 FIN 无错误码。
        });
        Get(probe);
        ShutdownConnection(loop, conn);
        (void)client->Close();
    }
    listener->Close();
    ShutdownLoop(loop);
    std::cout << "stream_peer_rst rst=not_eof/ECONNRESET fin=eof/no_error\n";
}

// 场景五 net.stream_adoption：接纳门——未连接 TCP/已连接 UDP 拒绝且 fd 原样
// 保留（不转移不 close），合法已连接 IPv4 TCP 单次转移、数据可达、Close 仅一次。
static void ScenarioAdoption() {
    WarmRuntime();
    EventLoop loop(64);
    CHECK(loop.Start());
    auto options = StreamOptions(1024, 8192, 4096, 1024 * 1024, 64);
    {   // 未连接 TCP：拒绝且 fd 保留（可 RAII 回收）。旧实现在此成功接管（red 断言点）。
        auto lone = OpenStream();
        const int lone_fd = lone.Fd();
        auto gate = std::make_shared<Gate>();
        std::shared_ptr<StreamConnection> conn;
        std::error_code adopt_error = std::make_error_code(std::errc::io_error);
        OnOwner(loop, [&] {
            conn = StreamConnection::Create(loop, std::move(lone), options,
                GateCallbacks(gate, &loop), adopt_error);
        });
        CHECK(!conn);
        CHECK(adopt_error == std::make_error_code(std::errc::not_connected));
        CHECK(lone.Fd() == lone_fd);
        (void)lone.Close();
    }
    {   // 已连接 UDP（实际 SO_TYPE=SOCK_DGRAM）：拒绝、fd 原样、随后真实收发可用。
        std::error_code error;
        auto peer_udp = NativeSocket::Open(AddressFamily::IPv4, SocketType::Datagram, error);
        CHECK(!error && peer_udp.Fd() >= 0);
        CHECK(!peer_udp.Bind(Address::Ip("127.0.0.1", 0, error)) && !error);
        const auto peer_bound = peer_udp.LocalAddress(error);
        CHECK(!error);
        auto udp = NativeSocket::Open(AddressFamily::IPv4, SocketType::Datagram, error);
        CHECK(!error && udp.Fd() >= 0);
        CHECK(!udp.Connect(peer_bound)); // 真实已连接 UDP。
        const int raw_fd = udp.Fd();
        auto gate = std::make_shared<Gate>();
        std::shared_ptr<StreamConnection> conn;
        std::error_code adopt_error = std::make_error_code(std::errc::io_error);
        OnOwner(loop, [&] {
            conn = StreamConnection::Create(loop, std::move(udp), options,
                GateCallbacks(gate, &loop), adopt_error);
        });
        CHECK(!conn); // 非法类型不得被 stream 接纳（旧实现接管的另一形态）。
        CHECK(adopt_error == std::make_error_code(std::errc::invalid_argument));
        CHECK(udp.Fd() == raw_fd); // 拒绝不转移、不 close，调用者可继续用。
        CHECK(udp.SendTo("adopt-probe", 11, peer_bound, 0).bytes == 11);
        char buffer[16] = {};
        Address source;
        bool echoed = false; // 有界等待回包：非阻塞 UDP 可能瞬时 EAGAIN。
        for (int i = 0; i < 1000 && !echoed; ++i) {
            const auto received = peer_udp.ReceiveFrom(buffer, sizeof buffer, source, 0);
            if (!received.error && received.bytes == 11) { echoed = true; }
            else { std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
        }
        CHECK(echoed);
        CHECK(!peer_udp.SendTo("ok", 2, source, 0).error);
        bool replied = false;
        for (int i = 0; i < 1000 && !replied; ++i) {
            const auto answer = udp.Receive(buffer, sizeof buffer, 0);
            if (!answer.error && answer.bytes == 2) { replied = true; } // fd 仍真实可用。
            else { std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
        }
        CHECK(replied && std::string_view(buffer, 2) == "ok");
        (void)udp.Close();
        (void)peer_udp.Close();
    }
    {   // 合法已连接 IPv4 TCP：成功转移、原 Fd=-1、对端数据可达、物理 Close 恰一次。
        auto listener = std::make_shared<NativeSocket>(OpenStream());
        const auto bound = ListenOn(*listener);
        auto gate = std::make_shared<Gate>();
        auto client = std::make_shared<NativeSocket>(ConnectTo(bound));
        NativeSocket accepted; // listener 接纳的服务端 fd（对端是 client）。
        {
            Address peer;
            std::error_code accept_error;
            for (int i = 0; i < 1000; ++i) { // listener 非阻塞：EAGAIN 有界重试。
                accepted = listener->Accept(peer, accept_error);
                if (!accept_error && accepted.Fd() >= 0) { break; }
                CHECK(accept_error.value() == EAGAIN);
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            CHECK(accepted.Fd() >= 0);
        }
        std::shared_ptr<StreamConnection> conn;
        std::error_code adopt_error = std::make_error_code(std::errc::io_error);
        OnOwner(loop, [&] {
            conn = StreamConnection::Create(loop, std::move(accepted), options,
                GateCallbacks(gate, &loop), adopt_error);
        });
        CHECK(conn && !adopt_error);
        CHECK(accepted.Fd() == -1); // 唯一所有权转移，无 dup/双 close。
        CHECK(client->Send("adopt", 5).bytes == 5); // 对端仍是独立存活 fd。
        auto traffic = Task([&] {
            std::size_t seen = 0;
            WaitTraffic(loop, *gate, seen);
            std::error_code error;
            auto snapshot = OwnerTake(loop, conn, error);
            CHECK(!error && snapshot.Remaining() == 5 && snapshot.Peek() == "adopt");
        });
        Get(traffic);
        ShutdownConnection(loop, conn);
        CHECK(OwnerStats(loop, conn).physical_closes == 1);
        (void)client->Close();
        listener->Close();
    }
    ShutdownLoop(loop);
    std::cout << "stream_adoption unconnected=rejected udp=rejected/fd_alive"
        " tcp=adopted/fd_-1/data/close_once\n";
}

// 测试自有 RAII：只收口测试自造/独占的 raw fd，不 close 产品借用的 fd。
struct FdGuard {
    int fd{-1};
    ~FdGuard() { if (fd >= 0) { (void)::close(fd); } }
};

// 控制侧 poll 就绪后单次接纳（无 owner 内睡眠；与 AcceptStream 同形但手动持有 fd）。
static bool AcceptOnce(NativeSocket& listener, NativeSocket& peer_socket) {
    for (int i = 0; i < 1000 && peer_socket.Fd() < 0; ++i) {
        pollfd pfd{listener.Fd(), POLLIN, 0};
        if (::poll(&pfd, 1, 2) != 1) { continue; }
        Address peer;
        std::error_code accept_error;
        peer_socket = listener.Accept(peer, accept_error);
        CHECK(!accept_error || accept_error.value() == EAGAIN);
    }
    return peer_socket.Fd() >= 0;
}

// 短 TCP 标记也允许拆包；有界地读满，不能用单次 recv 的长度冒充完整性。
static void CheckMarker(int fd, std::string_view expected) {
    std::string received(expected.size(), '\0');
    std::size_t offset = 0;
    for (int i = 0; i < 1000 && offset < received.size(); ++i) {
        pollfd pfd{fd, POLLIN, 0};
        const auto ready = ::poll(&pfd, 1, 2);
        if (ready < 0 && errno == EINTR) { continue; }
        CHECK(ready >= 0);
        if (ready == 0) { continue; }
        const auto bytes = ::recv(fd, received.data() + offset,
            received.size() - offset, MSG_DONTWAIT);
        if (bytes < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            continue;
        }
        CHECK(bytes > 0);
        offset += static_cast<std::size_t>(bytes);
    }
    CHECK(offset == received.size() && received == expected);
}

// 场景六 net.stream_owner_rejection：控制线程（非 owner）调用逐项在
// fd 转移/队列变更/快照移动/关闭之前以 std::logic_error 拒绝；
// Create 输入 fd 原样保留且仍真实可用；最后 owner 正常归还/收口。
static void ScenarioOwnerRejection() {
    WarmRuntime();
    EventLoop loop(64);
    CHECK(loop.Start());
    auto options = StreamOptions(1024, 8192, 4096, 1024 * 1024, 64);
    options.socket_send_buffer = 4096; // 小内核 sendbuf + 客户端暂停读取：积压真实滞留。
    auto listener = std::make_shared<NativeSocket>(OpenStream());
    const auto bound = ListenOn(*listener);
    auto gate = std::make_shared<Gate>();
    auto client = std::make_shared<NativeSocket>(ConnectTo(bound));
    auto conn = AcceptStreamTask(loop, *listener, options, gate);
    CHECK(!loop.IsOwner()); // 控制线程执行全部拒绝探测。

    // owner 预置真实状态：入站数据 + 借出快照 + 非空待发队列 + 基线计数。
    CHECK(client->Send("owner-data", 10).bytes == 10);
    bool ready = false;
    for (int i = 0; i < 200 && !ready; ++i) {
        OnOwner(loop, [&] { ready = conn->BufferedBytes() == 10; });
        if (!ready) { std::this_thread::sleep_for(5ms); }
    }
    CHECK(ready);
    CHECK(!OwnerWrite(loop, conn, std::string(516096, 'q'))); // 非空待发：远超小内核 sendbuf。
    std::size_t queue_bytes = 0;
    std::size_t queue_segments = 0;
    {   // 等写 handler 排到内核满载后采样：基线稳定，复查 before==after 才确定。
        std::size_t stable = 0;
        std::size_t last_bytes = 0;
        std::size_t last_segments = 0;
        for (int i = 0; i < 200 && stable < 20; ++i) {
            OnOwner(loop, [&] {
                queue_bytes = conn->PendingBytes();
                queue_segments = conn->PendingSegments();
            });
            if (queue_bytes == last_bytes && queue_segments == last_segments
                && queue_bytes > 0) { ++stable; } else { stable = 0; }
            last_bytes = queue_bytes;
            last_segments = queue_segments;
            if (stable < 20) { std::this_thread::sleep_for(5ms); }
        }
        CHECK(stable == 20 && queue_bytes > 0 && queue_segments >= 1);
    }
    auto lent = std::make_shared<EventSnapshot>();
    OnOwner(loop, [&] {
        std::error_code error;
        *lent = conn->TakeSnapshot(error);
        CHECK(!error && lent->Remaining() == 10 && lent->Peek() == "owner-data");
    });
    const auto baseline = OwnerStats(loop, conn);
    CHECK(baseline.writes_accepted == 1 && baseline.physical_closes == 0);
    OnOwner(loop, [&] {
        CHECK(conn->PendingBytes() == queue_bytes && conn->PendingSegments() == queue_segments);
        CHECK(conn->SnapshotLent() && conn->BufferedBytes() == 0);
        CHECK(conn->ConnectionState() == StreamConnection::State::Open);
    });

    int rejections = 0;
    const auto probe = [&](const char* what, auto&& call) {
        bool threw = false;
        try { call(); } catch (const std::logic_error&) { threw = true; }
        CHECK(threw); // 每项都在任何副作用之前以 std::logic_error 拒绝。
        std::cout << "owner_rejection " << what << "=logic_error\n";
        ++rejections;
    };

    {   // Create：非 owner 抛出，输入 fd 原样保留且仍真实可用（拒绝在转移前）。
        auto lone_listener = std::make_shared<NativeSocket>(OpenStream());
        const auto lone_bound = ListenOn(*lone_listener);
        auto lone_client = std::make_shared<NativeSocket>(ConnectTo(lone_bound));
        auto lone_server = std::make_shared<NativeSocket>();
        CHECK(AcceptOnce(*lone_listener, *lone_server));
        const int lone_fd = lone_server->Fd();
        std::error_code adopt_error = std::make_error_code(std::errc::io_error);
        probe("create", [&] {
            (void)StreamConnection::Create(loop, std::move(*lone_server), options,
                GateCallbacks(gate, &loop), adopt_error);
        });
        CHECK(lone_server->Fd() == lone_fd); // fd 未被转移。
        CHECK(lone_client->Send("alive", 5).bytes == 5); // 原 fd 仍真实存活。
        CheckMarker(lone_server->Fd(), "alive");
        (void)lone_client->Close();
        (void)lone_server->Close();
        lone_listener->Close();
    }
    probe("write", [&] { (void)conn->Write("nope"); });
    probe("take_snapshot", [&] {
        std::error_code error;
        (void)conn->TakeSnapshot(error);
    });
    probe("return_snapshot", [&] { // 直接传原快照：拒绝必须发生在任何移动之前。
        (void)conn->ReturnSnapshot(std::move(*lent), 0);
    });
    CHECK(lent->Remaining() == 10 && lent->Peek() == "owner-data");
    probe("close", [&] { (void)conn->Close({}); });
    probe("pending_bytes", [&] { (void)conn->PendingBytes(); }); // mutable 查询。
    CHECK(rejections == 6);

    // owner 复查：state/buffer/pending/counters 逐项未变（拒绝无副作用）。
    const auto after = OwnerStats(loop, conn);
    CHECK(after.read_arms == baseline.read_arms);
    CHECK(after.write_arms == baseline.write_arms);
    CHECK(after.wait_callbacks == baseline.wait_callbacks);
    CHECK(after.guard_exits == baseline.guard_exits);
    CHECK(after.zero_write_progress == baseline.zero_write_progress);
    CHECK(after.traffic_notifications == baseline.traffic_notifications);
    CHECK(after.drained_notifications == baseline.drained_notifications);
    CHECK(after.writes_accepted == baseline.writes_accepted);
    CHECK(after.write_bytes_queued == baseline.write_bytes_queued);
    CHECK(after.write_bytes_sent == baseline.write_bytes_sent);
    CHECK(after.read_bytes == baseline.read_bytes);
    CHECK(after.peak_pending_bytes == baseline.peak_pending_bytes);
    CHECK(after.snapshots_taken == baseline.snapshots_taken);
    CHECK(after.snapshots_returned == baseline.snapshots_returned);
    CHECK(after.snapshot_consumed_bytes == baseline.snapshot_consumed_bytes);
    CHECK(after.physical_closes == baseline.physical_closes);
    OnOwner(loop, [&] {
        CHECK(conn->PendingBytes() == queue_bytes && conn->PendingSegments() == queue_segments);
        CHECK(conn->SnapshotLent() && conn->BufferedBytes() == 0);
        CHECK(conn->ConnectionState() == StreamConnection::State::Open);
    });

    // 最后正常 owner 归还：非 owner 拒绝后原借出内容完整（未消费尾部保序回来）。
    CHECK(!OwnerReturn(loop, conn, std::move(*lent), 0));
    CHECK(OwnerBuffered(loop, conn) == 10);
    std::error_code verify_error;
    auto still = OwnerTake(loop, conn, verify_error); // 归还后可再次借出=内容仍在。
    CHECK(!verify_error && still.Remaining() == 10 && still.Peek() == "owner-data");
    CHECK(!OwnerReturn(loop, conn, std::move(still), 0));

    ShutdownConnection(loop, conn); // 正常 owner 收口。
    const auto final_stats = OwnerStats(loop, conn);
    CHECK(final_stats.physical_closes == 1); // 唯一物理关闭来自最后 owner 收口。
    CHECK(final_stats.snapshots_taken == baseline.snapshots_taken + 1);
    CHECK(final_stats.snapshots_returned == baseline.snapshots_returned + 2);
    (void)client->Close();
    listener->Close();
    ShutdownLoop(loop);
    std::cout << "stream_owner_rejection rejections=" << rejections
        << " fd_alive=true queue_snapshot_counters_unchanged=true"
        << " original_snapshot_preserved=true all_stats_unchanged=true physical_closes=1\n";
}

// 场景七 net.stream_fd_reuse：真实旧连接 read/write wait 均在途时 Close 一次，
// 在任何 abort 晚回调运行之前用 dup3 独占把同一 numeric fd 指向另一条真实
// 存活 TCP；晚回调确实退出后复用 fd 仍 fstat 有效、双向 marker 完整无旧
// pending payload 污染；旧 close 不再触碰复用 fd；资源回 warm 基线。
static void ScenarioFdReuse() {
    WarmRuntime();
    const auto fds = Count("/proc/self/fd");
    const auto threads = Count("/proc/self/task");
    {   // loop 与全部连接在计数前出作用域析构：epoll/eventfd 一并收口（同场景三）。
    EventLoop loop(64);
    CHECK(loop.Start());
    auto options = StreamOptions(1024, 8192, 4096, 1024 * 1024, 64);
    options.socket_send_buffer = 4096;
    auto listener = std::make_shared<NativeSocket>(OpenStream());
    const auto bound = ListenOn(*listener);
    auto gate = std::make_shared<Gate>();
    auto client = std::make_shared<NativeSocket>(ConnectTo(bound));
    auto old_socket = std::make_shared<NativeSocket>();
    CHECK(AcceptOnce(*listener, *old_socket)); // 控制侧接纳，转移前捕获 numeric fd。
    const int old_fd = old_socket->Fd();
    std::shared_ptr<StreamConnection> conn;
    std::error_code adopt_error = std::make_error_code(std::errc::io_error);
    OnOwner(loop, [&] {
        conn = StreamConnection::Create(loop, std::move(*old_socket), options,
            GateCallbacks(gate, &loop), adopt_error);
    });
    CHECK(conn && !adopt_error);
    CHECK(old_socket->Fd() == -1); // 唯一所有权转移。

    // 同 owner 准备：入站在途 + 借出快照 + 出站积压，read/write wait 均在途。
    CHECK(client->Send(PatternChunk(0, 4096).data(), 4096).bytes == 4096);
    bool got = false;
    for (int i = 0; i < 200 && !got; ++i) {
        OnOwner(loop, [&] { got = conn->BufferedBytes() == 4096; });
        if (!got) { std::this_thread::sleep_for(5ms); }
    }
    CHECK(got);
    auto held = std::make_shared<EventSnapshot>(); // 旧快照：关闭后走「只清理」归还。
    OnOwner(loop, [&] {
        std::error_code error;
        *held = conn->TakeSnapshot(error);
        CHECK(!error && held->Remaining() == 4096);
    });
    bool armed_both = false;
    for (int i = 0; i < 200 && !armed_both; ++i) {
        OnOwner(loop, [&] { // 写入后 handler 未插入：写 wait 挂在不可吞的积压上。
            if (!conn->WriteWaiting()) { (void)conn->Write(std::string(512 * 1024, 'w')); }
            armed_both = conn->ReadWaiting() && conn->WriteWaiting();
        });
        if (!armed_both) { std::this_thread::sleep_for(5ms); }
    }
    CHECK(armed_both);

    // 复用 fixture：预建独立真实 TCP 对（与旧连接无关），仅测试使用的 source。
    auto reuse_listener = std::make_shared<NativeSocket>(OpenStream());
    const auto reuse_bound = ListenOn(*reuse_listener);
    auto reuse_client = std::make_shared<NativeSocket>(ConnectTo(reuse_bound));
    auto reuse_server = std::make_shared<NativeSocket>();
    CHECK(AcceptOnce(*reuse_listener, *reuse_server));

    const auto close_reason = std::make_error_code(std::errc::operation_canceled);
    FdGuard reused; // dup 目标一经取得即由测试 RAII 持有，直到旧对象析构后验证完毕。
    OnOwner(loop, [&] { // 同一 owner 动作：Close → 晚回调运行前 dup3 精确复用。
        CHECK(conn->ReadWaiting() && conn->WriteWaiting()); // 撤销前双 wait 在途。
        CHECK(!conn->Close(close_reason)); // 同步物理收口 + on_closed；晚回调此后异步派发。
        CHECK(conn->GetStats().physical_closes == 1);
        CHECK(!conn->ReadWaiting() && !conn->WriteWaiting()); // armed 同步清除。
        CHECK(conn->GetStats().guard_exits == 0); // 同线程同步块：晚回调尚未运行。
        errno = 0;
        CHECK(::dup3(reuse_server->Fd(), old_fd, O_CLOEXEC) == old_fd); // numeric 相等。
        reused.fd = old_fd;
        CHECK(reuse_client->Send("reuse-ok", 8).bytes == 8);
        char peek[8] = {};
        CHECK(::recv(old_fd, peek, sizeof peek, MSG_PEEK | MSG_DONTWAIT) == 8);
        CHECK(std::string_view(peek, sizeof peek) == "reuse-ok");
        CHECK(conn->GetStats().guard_exits == 0); // marker 已入新 TCP，晚回调仍未运行。
    });
    CHECK(reuse_server->Fd() != old_fd); // source 是 fixture 自己的 fd，未被偷换。

    std::size_t guard_exits = 0; // 有界等待旧 abort 晚回调确实退出（不用 sleep 冒充）。
    for (int i = 0; i < 200 && guard_exits < 2; ++i) {
        guard_exits = OwnerStats(loop, conn).guard_exits;
        if (guard_exits < 2) { std::this_thread::sleep_for(5ms); }
    }
    CHECK(guard_exits == 2);
    const auto after_close = OwnerStats(loop, conn);
    CHECK(after_close.physical_closes == 1);
    auto closed_check = Task([&] { // CoIoOperation::Run 需业务协程上下文（同场景三）。
        CHECK(WaitClosed(loop, *gate) == close_reason); // 关闭原因经 promise/future 同步。
    });
    Get(closed_check);

    OnOwner(loop, [&] {
        CHECK(!conn->ReadWaiting() && !conn->WriteWaiting()); // 晚回调退出后仍无重挂。
        CHECK(conn->ConnectionState() == StreamConnection::State::Closed);
    });
    CHECK(!OwnerClose(loop, conn, {}));       // 重复 Close 幂等。
    CHECK(OwnerStats(loop, conn).physical_closes == 1);
    CHECK(!OwnerReturn(loop, conn, std::move(*held), 0)); // 旧快照只清理不重开。
    OnOwner(loop, [&] {
        CHECK(conn->GetStats().snapshots_returned == 1);
        CHECK(!conn->ReadWaiting()); // 归还不恢复读泵。
    });

    std::weak_ptr<StreamConnection> weak = conn; // 最后引用释放：weak 可释放。
    conn.reset();
    bool released = false;
    for (int i = 0; i < 200 && !released; ++i) {
        OnOwner(loop, [&] { released = weak.expired(); });
        if (!released) { std::this_thread::sleep_for(5ms); }
    }
    CHECK(released);

    // 晚回调、重复 Close、归还与最后旧对象析构都已结束，复用 fd 必须仍存活。
    struct ::stat reused_stat {};
    CHECK(::fstat(old_fd, &reused_stat) == 0);
    CheckMarker(old_fd, "reuse-ok"); // 预放 marker 未被旧读回调偷走。
    pollfd incoming_residue{old_fd, POLLIN, 0};
    CHECK(::poll(&incoming_residue, 1, 0) == 0);
    CHECK(::send(old_fd, "ack-01", 6, MSG_NOSIGNAL | MSG_DONTWAIT) == 6);
    CheckMarker(reuse_client->Fd(), "ack-01"); // 旧 pending 没有被错误地发进新 TCP。
    pollfd outgoing_residue{reuse_client->Fd(), POLLIN, 0};
    CHECK(::poll(&outgoing_residue, 1, 0) == 0);
    CHECK(::close(old_fd) == 0); // 只收口测试自获的 dup fd，证据窗口内一直保持打开。
    reused.fd = -1;
    (void)client->Close();
    (void)reuse_client->Close();
    (void)reuse_server->Close(); // 只关自身 fd 引用；reused 独立持有同一描述。
    reuse_listener->Close();
    listener->Close();
    ShutdownLoop(loop);
    std::cout << "stream_fd_reuse reuse_fd=" << old_fd
        << " guard_exits=2 physical_closes=1 markers_bidirectional=true"
        << " marker_before_callbacks=true alive_after_old_destructor=true"
        << " old_pending_unpolluted=true weak_released=true\n";
    } // loop/io_context 析构：唯一 fd 与 owner 线程资源收口。
    std::this_thread::sleep_for(50ms);
    const auto final_fds = Count("/proc/self/fd");
    const auto final_threads = Count("/proc/self/task");
    CHECK(final_fds == fds && final_threads == threads); // ownFD/owner 线程回 warm 基线。
    std::cout << "stream_fd_reuse fd=" << fds << "->" << final_fds
        << " threads=" << threads << "->" << final_threads << '\n';
}

#include "ProcessRuntimeTestMain.hpp"

static int RunStreamCases(int argc, char** argv) {
    try {
        CHECK(argc == 2);
        const std::string test = argv[1];
        if (test == "stream_traffic") { ScenarioTraffic(); }
        else if (test == "stream_write") { ScenarioWrite(); }
        else if (test == "stream_close") { ScenarioClose(); }
        else if (test == "stream_peer_rst") { ScenarioPeerRst(); }
        else if (test == "stream_adoption") { ScenarioAdoption(); }
        else if (test == "stream_owner_rejection") { ScenarioOwnerRejection(); }
        else if (test == "stream_fd_reuse") { ScenarioFdReuse(); }
        else { throw std::runtime_error("unknown test"); }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}

int main(int argc, char** argv) {
    // 测试进程退出边界：局部 RAII 收口 → 刷新输出 → _Exit，见
    // ProcessRuntimeTestMain.hpp 头注（静态析构竞态为已知限制，未修复）。
    return bbt_net_test::RunTestProcessExit([&] { return RunStreamCases(argc, argv); });
}
