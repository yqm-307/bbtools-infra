#include "TestSupport.hpp"
#include "../net/ProcessRuntimeTestMain.hpp"

#include <bbt/coroutine/detail/Coroutine.hpp>

using namespace redis_net_test;

namespace {
const std::thread::id kControlThread = std::this_thread::get_id();
std::atomic<int> g_readers{0};
std::atomic<int> g_reader_created{0};
std::atomic<int> g_reader_freed{0};
std::atomic<redisReader*> g_last_reader{nullptr};
std::atomic<redisReader*> g_watch_reader{nullptr};
std::atomic<int> g_partial{0};
std::atomic<int> g_feed_error{0};
}
extern "C" redisReader* __real_redisReaderCreate();
extern "C" void __real_redisReaderFree(redisReader*);
extern "C" int __real_redisReaderGetReply(redisReader*, void**);
extern "C" int __real_redisReaderFeed(redisReader*, const char*, std::size_t);
extern "C" redisReader* __wrap_redisReaderCreate() {
    auto* reader = __real_redisReaderCreate();
    if (reader) {
        ++g_readers;
        ++g_reader_created;
        if (std::this_thread::get_id() == kControlThread || bbt::coroutine::GetLocalCoroutineId() != 0) {
            g_last_reader.store(reader);
        }
    }
    return reader;
}
extern "C" void __wrap_redisReaderFree(redisReader* reader) {
    if (reader) { --g_readers; ++g_reader_freed; }
    __real_redisReaderFree(reader);
}
extern "C" int __wrap_redisReaderGetReply(redisReader* reader, void** reply) {
    const auto code = __real_redisReaderGetReply(reader, reply);
    if (reader == g_watch_reader.load() && code == REDIS_OK && !*reply) { ++g_partial; }
    return code;
}
extern "C" int __wrap_redisReaderFeed(redisReader* reader, const char* bytes, std::size_t size) {
    if (reader == g_watch_reader.load()) {
        if (const auto error = g_feed_error.exchange(0)) {
            reader->err = error;
            std::snprintf(reader->errstr, sizeof(reader->errstr), "injected Feed failure");
            return REDIS_ERR;
        }
    }
    return __real_redisReaderFeed(reader, bytes, size);
}

namespace {
struct NativeBarrier {
    std::atomic<bool> entered{false};
    std::mutex mutex;
    std::condition_variable cv;
    bool released{false};
    void Pause() {
        std::unique_lock<std::mutex> lock(mutex);
        entered.store(true);
        CHECK(cv.wait_for(lock, 3s, [this] { return released; }));
    }
    void Release() {
        { std::lock_guard<std::mutex> lock(mutex); released = true; }
        cv.notify_all();
    }
    ~NativeBarrier() { Release(); }
};

struct Endpoint {
    int fd{-1};
    net::Address address;
    explicit Endpoint(bool listening) {
        fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        CHECK(fd >= 0);
        sockaddr_in local{};
        local.sin_family = AF_INET;
        local.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        CHECK(::bind(fd, reinterpret_cast<sockaddr*>(&local), sizeof(local)) == 0);
        socklen_t size = sizeof(local);
        CHECK(::getsockname(fd, reinterpret_cast<sockaddr*>(&local), &size) == 0);
        address = Loopback(ntohs(local.sin_port));
        if (listening) { CHECK(::listen(fd, 4) == 0); }
    }
    ~Endpoint() { ::close(fd); }
};

void CheckUnknown(const Error& error, const std::string& cause) {
    CHECK(error.code == ErrorCode::OutcomeUnknown);
    CHECK(!error.request_phase);
    CHECK(ValidateErrorDetails(error));
    CHECK(std::find(error.details.begin(), error.details.end(),
                    std::make_pair(std::string("cause"), cause)) != error.details.end());
}

void RealCommands() {
    RealRedis backend;
    Host host;
    auto cli = host.Make();
    host.Connect(cli);
    CHECK(host.Call([&] { return cli->Ping(Until()); }));
    const auto key = backend.Key(std::string("binary\0key", 10));
    const auto list = backend.Key("list");
    const auto missing = backend.Key("missing");
    const std::string value("value\0\r\n\xff", 9);
    CHECK(host.Call([&] { return cli->Set(key, value, Until()); }));
    auto got = host.Call([&] { return cli->Get(key, Until()); });
    CHECK(got && got.value() && *got.value() == value);
    auto exists = host.Call([&] { return cli->Exists(key, Until()); });
    CHECK(exists && exists.value());
    auto absent = host.Call([&] { return cli->Get(missing, Until()); });
    CHECK(absent && !absent.value());
    auto pushed = backend.Command({"LPUSH", list, "one"});
    CHECK(pushed->type == REDIS_REPLY_INTEGER && pushed->integer == 1);
    auto wrong = host.Call([&] { return cli->Get(list, Until()); });
    ExpectCode(wrong, ErrorCode::RemoteError);
    CHECK(wrong.error().domain_code == "WRONGTYPE");
    CHECK(wrong.error().backend_category == "redis");
    CHECK(host.Call([&] { return cli->Ping(Until()); }));
    got = host.Call([&] { return cli->Get(key, Until()); });
    CHECK(got && got.value() && *got.value() == value);

    // 读回复超出一次 native recv/CoRead 的容量；只经公开 GET 消费。
    const std::string large(256 * 1024, 'x');
    auto set_large = backend.Command({"SET", key, large});
    CHECK(set_large->type == REDIS_REPLY_STATUS);
    got = host.Call([&] { return cli->Get(key, Until()); });
    CHECK(got && got.value() && *got.value() == large);
    ExpectCode(host.Call([&] { return cli->Get({}, Until()); }), ErrorCode::InvalidArgument);
    ExpectCode(host.Call([&] { return cli->Delete({}, Until()); }), ErrorCode::InvalidArgument);
    ExpectCode(host.Call([&] { return cli->Delete({key, ""}, Until()); }), ErrorCode::InvalidArgument);
    auto deleted = host.Call([&] { return cli->Delete({key, list}, Until()); });
    CHECK(deleted && deleted.value() == 2);
    exists = host.Call([&] { return cli->Exists(key, Until()); });
    CHECK(exists && !exists.value());
    cli->Close();
    cli.reset();
    host.Finish();
    backend.Cleanup();
    std::cout << "real_redis ping=1 binary_key_value=1 large_reply_bytes=" << large.size()
              << " wrongtype=RemoteError same_stream_ping_get=1 empty_args_rejected=1\n";
}

void FragmentedReply() {
    std::atomic<bool> first_sent{false}, release{false};
    Peer peer([&](int fd) {
        CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"GET", "fragment"}));
        CHECK(Peer::Send(fd, "$9\r\nab"));
        first_sent.store(true);
        CHECK(WaitUntil([&] { return release.load(); }));
        CHECK(Peer::Send(fd, std::string("c\0defgh\r\n", 9)));
    });
    Host host;
    auto cli = host.Make(peer.address);
    host.Connect(cli);
    g_watch_reader.store(g_last_reader.load());
    auto call = host.Submit([&] { return cli->Get("fragment", Until()); });
    CHECK(WaitUntil([&] { return first_sent.load() && g_partial.load() != 0; }));
    const bool stayed_pending = call.wait_for(20ms) == std::future_status::timeout;
    release.store(true);
    auto value = Get(call);
    peer.Join();
    CHECK(stayed_pending);
    CHECK(value && value.value() && *value.value() == std::string("abc\0defgh", 9));
    cli.reset();
    host.Finish();
    g_watch_reader.store(nullptr);
    std::cout << "fragmented_reply partial_observed=1 waited_for_tail=1 binary_bulk=1\n";
}

void UnknownDisconnect() {
    unsigned sessions = 0;
    Peer peer([&](int fd) {
        CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"PING"}));
        if (++sessions == 2) { CHECK(Peer::Send(fd, "+PONG\r\n")); }
        // 第一次读到完整命令后断连；不声称服务端没执行。
    }, 2);
    Host host;
    auto cli = host.Make(peer.address);
    host.Connect(cli);
    auto value = host.Call([&] { return cli->Ping(Until()); });
    CHECK(!value);
    CheckUnknown(value.error(), "TransportError");
    CHECK(value.error().backend_category == "net" && value.error().backend_code != 0);
    ExpectCode(host.Call([&] { return cli->Ping(Until()); }), ErrorCode::TransportError);
    host.Connect(cli);  // 显式重连必须换连接与 reader，不能凭 Valid() 返回伪成功。
    CHECK(host.Call([&] { return cli->Ping(Until()); }));
    peer.Join();
    CHECK(sessions == 2);
    cli.reset();
    host.Finish();
    std::cout << "disconnect accepted=1 cause=TransportError sticky=1 explicit_reconnect=1 requests=2\n";
}

void ConnectFailureOwned() {
    Host host;
    Endpoint refused(false);
    auto cli = host.Make(refused.address);
    auto failed = host.Call([&] { return cli->Connect(Until()); });
    ExpectCode(failed, ErrorCode::TransportError);
    CHECK(failed.error().backend_category == "net");
    CHECK(failed.error().backend_code == std::make_error_code(std::errc::connection_refused).value());
    ExpectCode(host.Call([&] { return cli->Ping(Until()); }), ErrorCode::RuntimeUnavailable);
    cli.reset();

    Endpoint listener(true);
    Access::SetConnectStartDelay(*host.engine, 500ms);
    cli = host.Make(listener.address);
    auto expired = host.Call([&] { return cli->Connect(Until(-1ms)); });
    ExpectCode(expired, ErrorCode::TimedOut);
    cli.reset();
    CHECK(WaitUntil([&] { return Access::ConnectTableSize(*host.engine) == 0; }));
    std::promise<bbt::coroutine::detail::Coroutine*> coroutine;
    auto current = coroutine.get_future();
    std::atomic<bool> submitted{false};
    net::CoTestAccess::SetDialBarrier([&] { submitted.store(true); });
    cli = host.Make(listener.address);
    auto pending = host.Submit([&] {
        coroutine.set_value(g_bbt_tls_coroutine_co);
        return cli->Connect(Until());
    });
    auto task = Get(current);
    CHECK(WaitUntil([&] { return submitted.load() && Access::PendingConnectCount(*host.engine) == 1; }));
    task->RequestCancel();
    auto cancelled = Get(pending);
    ExpectCode(cancelled, ErrorCode::Cancelled);
    task = nullptr;
    cli.reset();
    net::CoTestAccess::SetDialBarrier({});
    Access::SetConnectStartDelay(*host.engine, 0ms);
    host.Finish();

    // handler Engine 只拒绝 Co 建连，不接管已有 handler。
    auto handler = std::make_shared<net::RawHandler>();
    Host raw_host(handler);
    auto raw_cli = raw_host.Make(listener.address);
    ExpectCode(raw_host.Call([&] { return raw_cli->Connect(Until()); }), ErrorCode::RuntimeUnavailable);
    CHECK(Access::ConnectTableSize(*raw_host.engine) == 0);
    raw_cli.reset();
    raw_host.Finish();
    auto idle = std::make_shared<net::Engine>(nullptr);
    auto idle_cli = Client::Create(idle, Loopback());
    CHECK(idle_cli);
    ExpectCode(raw_host.Call([&] { return idle_cli.value()->Connect(Until()); }), ErrorCode::RuntimeUnavailable);
    CHECK(!Client::Create(nullptr, Loopback()));
    std::error_code error;
    auto unix_address = net::Address::Unix("/not-used-redis-net", error);
    CHECK(!error);
    auto invalid = Client::Create(idle, unix_address);
    ExpectCode(invalid, ErrorCode::InvalidArgument);
    std::cout << "connect_failure refused=TransportError expired=TimedOut controlled_cancel=Cancelled raw_handler=RuntimeUnavailable idle=RuntimeUnavailable\n";
}

void CloseInvalidates() {
    Host host;
    auto cli = host.Make();
    ExpectCode(cli->Connect(Until()), ErrorCode::InvalidContext);
    ExpectCode(cli->Ping(Until()), ErrorCode::InvalidContext);
    CHECK(Access::ConnectTableSize(*host.engine) == 0);
    host.Connect(cli);
    host.Call([&] { cli->Close(); });
    CHECK(cli->IsClosed());
    ExpectCode(host.Call([&] { return cli->Set("closed-key-not-written", "x", Until()); }), ErrorCode::Closed);
    ExpectCode(host.Call([&] { return cli->Connect(Until()); }), ErrorCode::Closed);
    ExpectCode(host.Call([&] { return cli->Get("x", Until()); }), ErrorCode::Closed);
    ExpectCode(host.Call([&] { return cli->Get({}, Until()); }), ErrorCode::Closed);
    ExpectCode(cli->Ping(Until()), ErrorCode::InvalidContext);
    CHECK(host.Call([] { return bbt::coroutine::GetLocalCoroutineId() != 0; }));
    cli.reset();
    host.Finish();
    std::cout << "close_invalidates closed_codes=4 invalid_context=3 other_task_progress=1 workers=1\n";
}

void CloseIdempotent() {
    Host host;
    auto cli = host.Make();
    host.Connect(cli);
    NativeBarrier barrier;
    std::atomic<int> removals{0};
    Access::SetRemoveBarrierOnce(*host.engine, [&] { ++removals; barrier.Pause(); });
    cli->Close();
    CHECK(WaitUntil([&] { return barrier.entered.load(); }));
    std::vector<std::future<void>> closers;
    for (int i = 0; i < 4; ++i) {
        closers.push_back(std::async(std::launch::async, [cli] { cli->Close(); }));
    }
    bool nonblocking = true;
    for (auto& closer : closers) { nonblocking &= closer.wait_for(100ms) == std::future_status::ready; }
    barrier.Release();
    for (auto& closer : closers) { closer.get(); }
    CHECK(nonblocking && cli->IsClosed());
    CHECK(WaitUntil([&] { return host.engine->CountConnections() == 0; }));
    CHECK(removals.load() == 1);
    cli.reset();
    host.Finish();
    std::cout << "close_idempotent concurrent_closes=4 returned_before_owner_release=1 physical_removal=1\n";
}

void CloseDuringConnect() {
    Endpoint listener(true);
    Host host;
    auto cli = host.Make(listener.address);
    Gate barrier;
    net::CoTestAccess::SetDialBarrier([&] { barrier.Pause(); });
    auto pending = host.Submit([&] { return cli->Connect(Until()); });
    CHECK(WaitUntil([&] { return barrier.entered.load(); }));
    CHECK(WaitUntil([&] { return Access::PendingConnectCount(*host.engine) == 0 && host.engine->CountConnections() == 1; }));
    const auto start = std::chrono::steady_clock::now();
    cli->Close();
    CHECK(std::chrono::steady_clock::now() - start < 100ms);
    barrier.Release();
    ExpectCode(Get(pending), ErrorCode::Closed);
    net::CoTestAccess::SetDialBarrier({});
    ExpectCode(host.Call([&] { return cli->Ping(Until()); }), ErrorCode::Closed);
    CHECK(WaitUntil([&] { return host.engine->CountConnections() == 0; }));
    cli.reset();
    auto published = host.Make();
    host.Connect(published);
    CHECK(host.Call([&] { return published->Connect(Until()); }));
    published->Close();
    ExpectCode(host.Call([&] { return published->Ping(Until()); }), ErrorCode::Closed);
    published.reset();
    host.Finish();
    std::cout << "close_during_connect submitted_barrier=1 real_success_before_claim=1 late_publish_refused=1 publish_first_control=1\n";
}

void ClosePendingOnce() {
    std::atomic<bool> received{false}, send_late{false};
    Peer peer([&](int fd) {
        CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"SET", "fixture-only", "value"}));
        received.store(true);
        CHECK(WaitUntil([&] { return send_late.load(); }));
        (void)Peer::Send(fd, "+OK\r\n");
    });
    Host host;
    auto cli = host.Make(peer.address);
    host.Connect(cli);
    std::atomic<int> terminals{0};
    auto pending = host.Submit([&] {
        auto value = cli->Set("fixture-only", "value", Until());
        ++terminals;
        return value;
    });
    CHECK(WaitUntil([&] { return received.load(); }));
    CHECK(pending.wait_for(0ms) == std::future_status::timeout);
    ExpectCode(host.Call([&] { return cli->Ping(Until()); }), ErrorCode::Overloaded);
    CHECK(host.Call([] { return true; })); // 单 worker：在途等待不阻塞其他业务任务。
    cli->Close();
    auto value = Get(pending);
    CHECK(!value);
    CheckUnknown(value.error(), "Closed");
    send_late.store(true);
    peer.Join();
    CHECK(terminals.load() == 1);
    ExpectCode(host.Call([&] { return cli->Ping(Until()); }), ErrorCode::Closed);
    cli.reset();
    host.Finish();
    CHECK(terminals.load() == 1);
    std::cout << "pending full_command_received=1 reply_withheld=1 second=Overloaded workers=1 progress=1 terminal_count=1 late_reply_no_republish=1\n";
}

void DestroyLateCallback() {
    std::atomic<bool> received{false}, send_late{false};
    Peer peer([&](int fd) {
        CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"PING"}));
        received.store(true);
        CHECK(WaitUntil([&] { return send_late.load(); }));
        (void)Peer::Send(fd, "+PONG\r\n");
    });
    Host host;
    NativeBarrier barrier;
    auto cli = host.Make(peer.address);
    host.Connect(cli);
    std::weak_ptr<Client> weak = cli;
    Access::SetRemoveBarrierOnce(*host.engine, [&] { barrier.Pause(); });
    Client* raw = cli.get();
    std::atomic<int> terminals{0};
    auto pending = host.Submit([raw, &terminals] {
        auto value = raw->Ping(Until(80ms));
        ++terminals;
        return value;
    });
    CHECK(WaitUntil([&] { return received.load(); }));
    auto value = Get(pending);
    CHECK(!value);
    CheckUnknown(value.error(), "TimedOut");
    CHECK(WaitUntil([&] { return barrier.entered.load(); }));
    cli.reset();
    CHECK(weak.expired());
    CHECK(g_readers.load() == 0);
    send_late.store(true);
    peer.Join();
    CHECK(terminals.load() == 1);
    barrier.Release();
    host.Finish();
    CHECK(terminals.load() == 1);

    // Connect 已经进入 CoDial 后销毁公开句柄；只允许入口持有的 State 保活。
    Endpoint listener(true);
    Host connecting;
    auto dial = connecting.Make(listener.address);
    std::weak_ptr<Client> weak_dial = dial;
    Gate dial_barrier;
    net::CoTestAccess::SetDialBarrier([&] { dial_barrier.Pause(); });
    Client* raw_dial = dial.get();
    auto dialing = connecting.Submit([raw_dial] { return raw_dial->Connect(Until()); });
    CHECK(WaitUntil([&] { return dial_barrier.entered.load(); }));
    dial.reset();
    CHECK(weak_dial.expired());
    dial_barrier.Release();
    ExpectCode(Get(dialing), ErrorCode::Closed);
    net::CoTestAccess::SetDialBarrier({});
    connecting.Finish();
    std::cout << "destroy_late public_weak_expired=1 readers=0 owner_removal_pending_at_destroy=1 late_completion_terminal_count=1 suspended_connect_state_owned=1\n";
}

void SharedEngine() {
    RealRedis backend;
    Host host;
    auto a = host.Make(), b = host.Make();
    host.Connect(a);
    host.Connect(b);
    a->Close();
    ExpectCode(host.Call([&] { return a->Ping(Until()); }), ErrorCode::Closed);
    const auto key = backend.Key("isolation");
    CHECK(host.Call([&] { return b->Ping(Until()); }));
    CHECK(host.Call([&] { return b->Set(key, "shared", Until()); }));
    auto value = host.Call([&] { return b->Get(key, Until()); });
    CHECK(value && value.value() && *value.value() == "shared");
    CHECK(host.engine->Phase() == net::EnginePhase::Running);
    a.reset(); b.reset();
    host.Finish();
    backend.Cleanup();
    std::cout << "shared_engine A=Closed B_ping_set_get=1 engine_running_after_A=1\n";
}

void HostDrain() {
    Host host;
    auto a = host.Make(), b = host.Make();
    host.Connect(a);
    host.Connect(b);
    std::atomic<bool> received{false};
    std::atomic<bool> release{false};
    Peer peer([&](int fd) {
        CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"PING"}));
        received.store(true);
        CHECK(WaitUntil([&] { return release.load(); }));
    });
    auto pending_cli = host.Make(peer.address);
    host.Connect(pending_cli);
    auto pending = host.Submit([&] { return pending_cli->Ping(Until()); });
    CHECK(WaitUntil([&] { return received.load(); }));
    CHECK(!host.Drained());
    a->Close(); b->Close(); pending_cli->Close();
    release.store(true);
    host.Finish();
    auto value = Get(pending);
    CHECK(!value);
    CHECK(value.error().code == ErrorCode::OutcomeUnknown);
    a.reset(); b.reset(); pending_cli.reset();
    peer.Join();
    CHECK(g_readers.load() == 0);
    // 收尾合取的负对照：Run 返回但 Closing 仍须拒绝。
    CHECK(!DrainComplete(true, net::EnginePhase::Closing, 3, 3));
    CHECK(!DrainComplete(true, net::EnginePhase::Stopped, 3, 2));
    CHECK(DrainComplete(true, net::EnginePhase::Stopped, 3, 3));
    std::cout << "host_drain pending_api_settled=1 capture_release=1 reader_release=1 returned_closing_rejected=1\n";
}

void FramingAndFaults() {
    for (const std::string response : {"?bad\r\n", ":7\r\n", "+NOTPONG\r\n", "+PONG\r\n+EXTRA\r\n"}) {
        Peer peer([&](int fd) {
            CHECK(Peer::ReadCommand(fd)[0] == "PING");
            CHECK(Peer::Send(fd, response));
        });
        Host host;
        auto cli = host.Make(peer.address);
        host.Connect(cli);
        auto value = host.Call([&] { return cli->Ping(Until()); });
        CHECK(!value);
        CheckUnknown(value.error(), "ProtocolError");
        ExpectCode(host.Call([&] { return cli->Ping(Until()); }), ErrorCode::TransportError);
        cli.reset();
        peer.Join();
        host.Finish();
    }
    for (const int failure : {REDIS_ERR_PROTOCOL, REDIS_ERR_OOM}) {
        Peer peer([](int fd) {
            CHECK(Peer::ReadCommand(fd)[0] == "PING");
            CHECK(Peer::Send(fd, "+PONG\r\n"));
        });
        Host host;
        auto cli = host.Make(peer.address);
        host.Connect(cli);
        g_watch_reader.store(g_last_reader.load());
        g_feed_error.store(failure);
        auto value = host.Call([&] { return cli->Ping(Until()); });
        CHECK(!value);
        CheckUnknown(value.error(), failure == REDIS_ERR_OOM ? "InternalError" : "ProtocolError");
        CHECK(value.error().backend_category == "hiredis" && value.error().backend_code == failure);
        cli.reset();
        peer.Join();
        host.Finish();
        g_watch_reader.store(nullptr);
    }
    {
        std::atomic<bool> send_extra{false}, extra_sent{false}, extra_received{false};
        Peer peer([&](int fd) {
            CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"PING"}));
            CHECK(Peer::Send(fd, "+PONG\r\n"));
            CHECK(WaitUntil([&] { return send_extra.load(); }));
            CHECK(Peer::Send(fd, "+PONG\r\n"));
            extra_sent.store(true);
            char byte;
            CHECK(::recv(fd, &byte, 1, 0) == 0); // 拒绝下一命令前不得把它提交到后端。
        });
        Host host;
        auto cli = host.Make(peer.address);
        host.Connect(cli);
        CHECK(host.Call([&] { return cli->Ping(Until()); }));
        Access::SetReadAppendHook(*host.engine, [&] { extra_received.store(true); });
        send_extra.store(true);
        CHECK(WaitUntil([&] { return extra_sent.load() && extra_received.load(); }));
        // 单 worker 的标记任务只能在 owner append 返回后运行，保证字节已进 ring。
        CHECK(host.Call([] { return true; }));
        ExpectCode(host.Call([&] { return cli->Ping(Until()); }), ErrorCode::ProtocolError);
        Access::SetReadAppendHook(*host.engine, {});
        peer.Join();
        cli.reset();
        host.Finish();
        std::cout << "unsolicited_idle_frame rejected_before_write=1\n";
    }
    {
        std::atomic<bool> received{false}, release{false};
        Peer peer([&](int fd) {
            CHECK(Peer::ReadCommand(fd) == std::vector<std::string>({"PING"}));
            received.store(true);
            CHECK(WaitUntil([&] { return release.load(); }));
            (void)Peer::Send(fd, "+PONG\r\n");
        });
        Host host;
        auto cli = host.Make(peer.address);
        host.Connect(cli);
        std::promise<bbt::coroutine::detail::Coroutine*> coroutine;
        auto current = coroutine.get_future();
        auto pending = host.Submit([&] {
            coroutine.set_value(g_bbt_tls_coroutine_co);
            return cli->Ping(Until());
        });
        auto* task = Get(current);
        CHECK(WaitUntil([&] { return received.load(); }));
        task->RequestCancel();
        auto value = Get(pending);
        CHECK(!value);
        CheckUnknown(value.error(), "Cancelled");
        CHECK(value.error().backend_category == "net");
        CHECK(value.error().backend_code == std::make_error_code(std::errc::operation_canceled).value());
        release.store(true);
        peer.Join();
        ExpectCode(host.Call([&] { return cli->Ping(Until()); }), ErrorCode::TransportError);
        cli.reset();
        host.Finish();
        std::cout << "command_cancel full_request_received=1 cause=Cancelled retired_stream=1 no_replay=1\n";
    }
    // 全量编码命令超出写容量时零接纳，真实 Redis 读回 missing。
    RealRedis backend;
    net::Options options;
    options.max_outbound_bytes = 128;
    Host host({}, options);
    auto cli = host.Make();
    host.Connect(cli);
    const auto key = backend.Key("oversize");
    ExpectCode(host.Call([&] { return cli->Set(key, std::string(1024, 'z'), Until()); }), ErrorCode::Overloaded);
    auto missing = backend.Command({"EXISTS", key});
    CHECK(missing->type == REDIS_REPLY_INTEGER && missing->integer == 0);
    CHECK(host.Call([&] { return cli->Ping(Until()); }));
    cli.reset();
    host.Finish();
    backend.Cleanup();
    std::cout << "faults parse_bad=1 wrong_type=1 wrong_status=1 extra_frame=1 Feed_protocol=1 Feed_oom=1 oversize_zero_acceptance=1\n";
}

int Run(int argc, char** argv) {
    Watchdog watchdog;
    try {
        CHECK(argc == 2);
        g_bbt_coroutine_config->m_cfg_static_thread_num = 1;
        g_bbt_coroutine_config->m_cfg_stack_size = 128 * 1024;
        g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
        g_scheduler->Start();
        {
            Host warm;
            CHECK(warm.Call([] {
                auto waiter = bbt::coroutine::sync::CoWaiter::Create();
                CHECK(waiter);
                const auto status = waiter->WaitWithCallback({std::chrono::steady_clock::now() + 5ms}, [] { return true; });
                CHECK(status == bbt::coroutine::WaitStatus::TimedOut);
                return true;
            }));
            warm.Finish();
        }
        const auto baseline = SocketCount();
        const auto fd_baseline = FdCount();
        PrintFds("baseline");
        CHECK(g_readers.load() == 0);
        const std::string name = argv[1];
        if (name == "set_get_remote_error") { RealCommands(); }
        else if (name == "fragmented_reply") { FragmentedReply(); }
        else if (name == "unknown_disconnect") { UnknownDisconnect(); }
        else if (name == "connect_failure_owned") { ConnectFailureOwned(); }
        else if (name == "close_invalidates_immediately") { CloseInvalidates(); }
        else if (name == "close_idempotent") { CloseIdempotent(); }
        else if (name == "close_during_connect") { CloseDuringConnect(); }
        else if (name == "close_pending_command_once") { ClosePendingOnce(); }
        else if (name == "handle_destroy_late_callback") { DestroyLateCallback(); }
        else if (name == "shared_engine_isolation") { SharedEngine(); }
        else if (name == "host_drain_no_residue") { HostDrain(); }
        else if (name == "framing_and_faults") { FramingAndFaults(); }
        else { throw std::runtime_error("unknown case: " + name); }
        CHECK(!RealRedis::CleanupFailed());
        CHECK(g_readers.load() == 0 && g_reader_created.load() == g_reader_freed.load());
        PrintFds("after_scope");
        CHECK(WaitUntil([&] { return SocketCount() == baseline && FdCount() == fd_baseline; }));
        std::cout << "receipt after_scope fds=" << FdCount() << " fd_baseline=" << fd_baseline
                  << " sockets=" << SocketCount() << " socket_baseline=" << baseline
                  << " readers_created=" << g_reader_created.load() << " readers_freed=" << g_reader_freed.load()
                  << " readers_live=0 cleanup_failed=0\nPASS " << name << "\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "FAIL " << error.what() << "\n";
        return 1;
    }
}
} // namespace
int main(int argc, char** argv) {
    return bbt_net_test::RunTestProcessExit([&] { return Run(argc, argv); });
}
