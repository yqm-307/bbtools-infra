/**
 * rpc v2 AppSession 契约测试：真实 loopback TCP、Hello/HelloAck 握手与
 * epoch、Client-owned 双向会话、单 FIFO admission、lane 有界隔离、
 * Call/Send/SendAccepted、Close(Immediate/Timeout/Graceful)/Shutdown、Stats。真实双端：
 * 每进程一 Engine + Client(owner) + Server(同 owner)（ADR §3.1）。
 */
#include "TestSupport.hpp"

#include <bbt/infra/rpc/Admission.hpp>
#include <bbt/infra/rpc/SessionCodec.hpp>
#include <bbt/infra/rpc/Options.hpp>
#include <bbt/infra/rpc/Stats.hpp>
#include <bbt/infra/rpc/detail/Session.hpp>
#include <bbt/infra/rpc/detail/RpcChannel.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>
#include <bbt/infra/net/Error.hpp>

#include <deque>
#include <map>
#include <vector>

namespace rpc_v2_session_test {
namespace {

using namespace rpc_net_test;
using namespace std::chrono_literals;
namespace rpc = bbt::infra::rpc;
namespace net = bbt::infra::net;
using Clock = std::chrono::steady_clock;

/**
 * 全局协程提交（同 Host::Submit/Call，免 Host 自带 Engine——v2 AppHost
 * 需要 listener+handoff，不能复用 client-only Host::engine）。
 */
template <class F> auto SubmitCo(F fn) -> std::future<decltype(fn())>
{
    using T = decltype(fn());
    auto promise = std::make_shared<std::promise<T>>();
    auto future = promise->get_future();
    std::function<T()> callback = std::move(fn);
    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        256 * 1024,
        [promise, callback = std::move(callback)]() mutable {
            try {
                if constexpr (std::is_void_v<T>) {
                    callback();
                    promise->set_value();
                } else {
                    promise->set_value(callback());
                }
            } catch (...) {
                promise->set_exception(std::current_exception());
            }
        },
        registered);
    if (!registered) {
        promise->set_exception(std::make_exception_ptr(std::runtime_error("task registration failed")));
    }
    return future;
}

template <class F> auto CallCo(F fn) -> decltype(fn())
{
    auto future = SubmitCo(std::move(fn));
    return Get(future);
}

/**
 * Close 是唯一关闭入口且为 bbt_co_async：原生测试线程经受管协程消费。
 */
result<void> CloseServer(const std::shared_ptr<rpc::CoRpcServer>& server, bbt::infra::CloseOptions options)
{
    return CallCo([&] { return bbt_co_await server->Close(options); });
}

result<void> CloseClient(const std::shared_ptr<rpc::CoRpcClient>& client, bbt::infra::CloseOptions options)
{
    return CallCo([&] { return bbt_co_await client->Close(options); });
}

/**
 * Timeout 语义已拆成 Immediate + WaitStopped：先强制封口，再有界等物理收口。
 */
template <class Handle>
result<void> CloseTimeoutFor(const Handle& handle, std::int64_t timeout_ms, bbt::infra::Error reason = {})
{
    return CallCo([&] {
        auto closed = bbt_co_await handle->Close(bbt::infra::CloseOptions::Immediate(std::move(reason)));
        if (!closed) {
            return closed;
        }
        return bbt_co_await handle->Close(
            bbt::infra::CloseOptions{
                bbt::infra::CloseMode::Immediate,
                true,
                {},
                std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms)});
    });
}

/**
 * v2 进程装配（ADR §3.1）：自有 Engine(listener+handoff) + Client(owner)
 * + Server(同 owner)。admission handler 默认记录并转移 owning item。
 * Start 域由用例选择：Native 覆盖既有宿主入口，Co 覆盖受管协程直接起服，
 * Deferred 留给启动/关闭竞争用例自行触发。
 */
enum class StartMode {
    Native,
    Co,
    Deferred,
};

struct AppHost {
    std::shared_ptr<net::Engine> engine{net::Engine::Create(std::shared_ptr<net::RawHandler>{})};
    std::shared_ptr<rpc::CoRpcClient> client;
    std::shared_ptr<rpc::CoRpcServer> server;
    net::Address address;
    std::thread runner;
    std::error_code run_error;

    /**
     * framework 侧：默认 admission handler（记录+转移）；on_item 可替换以
     * 注入定制处理（Disconnect 用例）。
     */
    std::function<bbt::infra::result<void>(rpc::AdmissionItem&&)> on_item;
    std::mutex mu;
    std::deque<rpc::AdmissionItem> admitted;
    std::map<std::string, std::vector<std::string>> by_key;
    std::atomic<unsigned> calls{0}, sends{0};

    AppHost(
        const std::string& identity,
        const rpc::ClientOptions& client_options = {},
        const rpc::ServerOptions& server_options = {},
        StartMode start_mode = StartMode::Native)
    {
        rpc::ClientOptions co = client_options;
        co.SetLocalIdentity(identity).SetTrustedNetworkPlaintext(true);
        auto made_client = rpc::CoRpcClient::Create(engine, co);
        CHECK(made_client);
        client = std::move(made_client).value();

        on_item = [this](rpc::AdmissionItem&& item) -> bbt::infra::result<void> {
            std::lock_guard<std::mutex> lock(mu);
            auto& key = item.request.OrderingKey();
            by_key[key].push_back(
                item.request.RequestId().empty() ? item.request.MessageId() : item.request.RequestId());
            if (item.request.Reply() == rpc::ReplyMode::Required) {
                ++calls;
            } else {
                ++sends;
            }
            admitted.push_back(std::move(item));
            return bbt::infra::result<void>::ok();
        };
        rpc::AdmissionHandler handler = [this](rpc::AdmissionItem&& item) {
            return on_item(std::move(item));
        };

        rpc::ServerOptions so = server_options;
        std::error_code error;
        so.SetListenAddress(net::Address::Ip("127.0.0.1", 0, error));
        CHECK(!error);
        auto made = rpc::CoRpcServer::Create(engine, client, handler, so);
        CHECK(made);
        server = std::move(made).value();

        net::EngineConfig config;
        config.runtime_progress = net::RuntimeProgress::Independent;
        config.options.num_event_loop = 1;
        config.listen_addresses = {so.ListenAddress()};
        runner = std::thread([this, config] { run_error = engine->Run(config); });
        try {
            CHECK(WaitUntil(
                [&] { return engine->Phase() == net::EnginePhase::Running || run_error != std::error_code{}; }));
            if (run_error) {
                throw std::runtime_error("engine run failed: " + run_error.message());
            }
            if (start_mode != StartMode::Deferred) {

                /**
                 * Co 模式的 Start 不建任何 native 桥：受管协程直接调用产品
                 * 入口；Native 模式覆盖既有宿主兼容路径。
                 */
                auto started = start_mode == StartMode::Co ? CallCo([&] { return server->Start(); }) : server->Start();
                if (!started) {
                    throw std::runtime_error("server start failed: " + started.error().message);
                }
                auto local = server->LocalAddress();
                CHECK(local);
                address = local.value();
            }
        } catch (...) {

            /**
             * ctor 失败必须收口 runner——否则 std::thread 成员析构
             * joinable → terminate 掩盖真实错误。
             */
            (void)engine->Close();
            if (runner.joinable()) {
                runner.join();
            }
            throw;
        }
    }

    ~AppHost()
    {
        if (server) {

            /**
             * Shutdown 是宿主侧同步强制关闭（不挂起、幂等），析构不再等
             * 物理回收——Engine 收口是物理释放的唯一责任方。
             */
            (void)server->Close(bbt::infra::CloseOptions::Immediate());
        }
        if (client) {
            (void)client->Close(bbt::infra::CloseOptions::Immediate());
        }
        (void)engine->Close();
        if (runner.joinable()) {
            runner.join();
        }
    }

    std::string Target() const { return "tcp://127.0.0.1:" + std::to_string(address.Port()); }

    /**
     * framework 逐个处理 admitted item（echo / 自定义 make_response）。
     */
    void
    ProcessAdmitted(const std::function<rpc::Response(const rpc::Request&)>& make_response, std::size_t max_items = 64)
    {
        for (std::size_t i = 0; i != max_items; ++i) {
            std::shared_ptr<rpc::ReplyCompletion> reply;
            std::shared_ptr<rpc::AdmissionLease> lease;
            rpc::Request request;
            {
                std::lock_guard<std::mutex> lock(mu);
                if (admitted.empty()) {
                    return;
                }
                auto& front = admitted.front();
                reply = front.reply;
                lease = front.lease;
                request = front.request;
                admitted.pop_front();
            }
            if (reply) {
                auto response = make_response(request);

                /**
                 * Complete 挂起等 socket 真实写终态——需要受管协程上下文；
                 * 提交到 scheduler（本函数可能跑在原生线程），回复到达与否
                 * 由调用方 Call 结果证明。
                 */
                SubmitCo([reply = std::move(reply), response = std::move(response)]() mutable {
                    return bbt_co_await reply->Complete(std::move(response));
                });
            }
            if (lease) {

                /**
                 * Send 的 lease 已由 infra worker 在接纳边界收口——重复
                 * Release 返回 false 属正常，不作断言。
                 */
                (void)lease->Release();
            }
        }
    }
};

rpc::Request MakeCall(
    const std::string& id,
    std::uint64_t sequence,
    const std::string& ordering_key,
    rpc::TrafficClass lane = rpc::TrafficClass::Service)
{
    rpc::Request request("svc", "M");
    request.SetRequestId(id)
        .SetReply(rpc::ReplyMode::Required)
        .SetTraffic(lane)
        .SetOrderingKey(ordering_key)
        .SetSequence(sequence)
        .SetRequestSchema("req")
        .SetResponseSchema("resp");
    std::string payload = "p-" + id;
    request.SetPayload({payload.begin(), payload.end()});
    return request;
}

rpc::Request MakeSend(
    const std::string& mid,
    std::uint64_t sequence,
    const std::string& ordering_key,
    rpc::TrafficClass lane = rpc::TrafficClass::Actor)
{
    rpc::Request message("svc", "Tell");
    message.SetMessageId(mid)
        .SetReply(rpc::ReplyMode::None)
        .SetTraffic(lane)
        .SetOrderingKey(ordering_key)
        .SetSequence(sequence)
        .SetRequestSchema("req")
        .SetResponseSchema("resp");
    std::string payload = "s-" + mid;
    message.SetPayload({payload.begin(), payload.end()});
    return message;
}

rpc::SendOptions SendUntil(std::chrono::milliseconds duration = 3s, bool remote_acceptance = false)
{
    rpc::SendOptions options;
    options.SetDeadline(Clock::now() + duration);
    options.RequireRemoteAcceptance(remote_acceptance);
    return options;
}

CallOptions UntilMs(std::chrono::milliseconds duration = 3s)
{
    return {Clock::now() + duration};
}

rpc::Response Echo(const rpc::Request& request)
{
    auto response = rpc::Response::FromRequest(request);
    response.SetPayload(request.Payload());
    return response;
}

void RegisteredRoute()
{
    AppHost client("route-client");
    AppHost server("route-server");
    std::atomic<bool> entered{false};
    rpc::CoRpcServer::RpcHandler handler = [&entered](rpc::Request request) -> bbt_co_async(result<rpc::Response>) {
        entered.store(true);
        return result<rpc::Response>::ok(Echo(request));
    };
    rpc::CoRpcServer::RouteOptions limit;
    limit.burst_tokens = 1;
    limit.refill_per_second = 0.1;
    CHECK(server.server->RegisterRpc("svc/M", std::move(handler), limit));

    const auto first = CallCo(
        [&] { return bbt_co_await client.client->Call(server.Target(), MakeCall("route-1", 1, "route"), UntilMs()); });
    CHECK(first);
    CHECK(first.value().Payload() == MakeCall("route-1", 1, "route").Payload());
    CHECK(entered.load());

    const auto limited = CallCo(
        [&] { return bbt_co_await client.client->Call(server.Target(), MakeCall("route-2", 2, "route"), UntilMs()); });
    ExpectCode(limited, ErrorCode::Overloaded);

    CHECK(server.server->UnregisterRpc("svc/M"));
    CHECK(server.server->UnregisterRpc("svc/M").error().code == ErrorCode::NotFound);
    std::cout << "v2 registered route: handler=ok rate_limit=overloaded unregister=ok\n";
}

/**
 * ---- 1. 基本 Call（握手 + RequestContext 注入 + Stats） ----------------------
 */
void Roundtrip()
{
    AppHost a("app-a"), b("app-b");
    auto boot = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("boot", 1, "k1"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    {
        std::lock_guard<std::mutex> lock(b.mu);
        CHECK(!b.admitted.empty());
        const auto& ctx = b.admitted.front().request.Context();
        CHECK(ctx.peer_identity == "app-a");
        CHECK(ctx.peer_principal.empty());
    }
    b.ProcessAdmitted(Echo);
    CHECK(Get(boot));
    auto second = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("c2", 2, "k1"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 2; }));
    b.ProcessAdmitted(Echo);
    const auto result = Get(second);
    CHECK(result && result.value().RequestId() == "c2");
    const auto stats = a.client->Stats();
    CHECK(stats.sessions == 1 && stats.links == 1);
    const auto server_stats = b.server->Stats();
    CHECK(server_stats.accepted_connections >= 1);
    CHECK(server_stats.callbacks >= 1);
    std::cout << "v2 roundtrip call ok sessions=1 links=1\n";
}

/**
 * ---- 2. 双向会话：B 反向 Call 复用同一 link ----------------------------------
 */
void Bidirectional()
{
    AppHost a("app-a"), b("app-b");
    auto first = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("a2b", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(first));

    /**
     * B → A：同 PeerKey 已有 session（入站 link）；B 新开拨号会走 duplicate
     * fencing——ADR §3.3：identity 较小者留本地发起 link，较大者留入站 link。
     * "app-a" < "app-b" → 保留 A→B 这条 wire；B 的新拨号被 fencing。
     */
    auto second = SubmitCo([&] { return bbt_co_await b.client->Call(a.Target(), MakeCall("b2a", 2, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return a.calls.load() == 1; }));
    a.ProcessAdmitted(Echo);
    CHECK(Get(second));
    CHECK(WaitUntil([&] { return a.client->Stats().links == 1 && b.client->Stats().links == 1; }, 3s));
    CHECK(a.client->Stats().links == 1);
    CHECK(b.client->Stats().links == 1);
    std::cout << "v2 bidirectional one shared link a=1 b=1\n";
}

/**
 * ---- 3. Send 远端确认 ---------------------------------------------------------
 */
void SendAcceptance()
{
    AppHost a("app-a"), b("app-b");
    auto boot = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(boot));

    auto send_ok =
        CallCo([&] { return bbt_co_await a.client->Send(b.Target(), MakeSend("m1", 2, "k"), SendUntil(3s, true)); });
    CHECK(send_ok);
    CHECK(WaitUntil([&] { return b.sends.load() == 1; }));

    /**
     * AR=true 的 Send 已返回 → 远端 mailbox 接纳已发生（worker 已转移）。
     */
    auto send_local =
        CallCo([&] { return bbt_co_await a.client->Send(b.Target(), MakeSend("m2", 3, "k"), SendUntil(3s, false)); });
    CHECK(send_local);
    CHECK(WaitUntil([&] { return b.sends.load() == 2; }));
    std::cout << "v2 send remote-acceptance + local-acceptance ok\n";
}

/**
 * ---- 4. 同 ordering_key 按序 admission -----------------------------------------
 */
void Ordering()
{
    AppHost a("app-a"), b("app-b");
    auto boot = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(boot));

    /**
     * Call1(seq=1) → Send2(seq=2) → Call3(seq=3)：同协程内顺序入队同一
     * ordering_key；Call 挂起等 reply 时由 pump 协程驱动 B 端 ProcessAdmitted
     * 完成 Response，wire 顺序由同 session FIFO 保持。
     */
    auto pump = SubmitCo([&] {
        // clang-format off

        /**
         * 处理到三条 same-key item 全部完成回复为止（上限 ~4s 兜底）。
         */
        // clang-format on
        for (unsigned i = 0; i != 2000; ++i) {
            bool drained = false;
            unsigned done = 0;
            {
                std::lock_guard<std::mutex> lock(b.mu);
                done = static_cast<unsigned>(b.by_key["same-key"].size());
                drained = b.admitted.empty();
            }
            if (done >= 3 && drained) {
                return;
            }
            b.ProcessAdmitted(Echo);
            bbtco_sleep(2);
        }
    });
    auto c1 =
        SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("call1", 1, "same-key"), UntilMs()); });
    CHECK(WaitUntil([&] {
        std::lock_guard<std::mutex> lock(b.mu);
        return b.by_key["same-key"].size() >= 1;
    }));
    auto s2 = SubmitCo([&] {
        return bbt_co_await a.client->Send(b.Target(), MakeSend("send2", 2, "same-key"), SendUntil(3s, false));
    });
    (void)Get(s2);
    auto c3 =
        SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("call3", 3, "same-key"), UntilMs()); });
    const auto r1 = Get(c1);
    const auto r3 = Get(c3);
    CHECK(r1 && r1.value().RequestId() == "call1");
    CHECK(r3 && r3.value().RequestId() == "call3");
    CHECK(WaitUntil([&] { return b.calls.load() + b.sends.load() >= 3; }));
    {
        std::lock_guard<std::mutex> lock(b.mu);
        const auto& seq = b.by_key["same-key"];
        CHECK(seq.size() == 3);
        CHECK(seq[0] == "call1" && seq[1] == "send2" && seq[2] == "call3");
    }
    (void)Get(pump);
    std::cout << "v2 ordering call1/send2/call3 preserved\n";
}

/**
 * ---- 4b. 入站顺序台账：重复/逆序/缺口按协议违规退役 link ---------------------
 * 首个有序帧即 per-key 台账基值，此后必须连续递增。缺口与重复帧不进入
 * admission，违规按 RejectProtocol 退役当前 link；新拨号开新 session 台账。
 */
void OrderingLedger()
{
    AppHost a("app-a"), b("app-b");
    auto g1 = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("g1", 1, "gap"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(g1));

    /**
     * 缺口：同 key seq=3 跳过 seq=2——按协议违规退役 link，不进入 admission。
     */
    const auto accepted_before = b.server->Stats().accepted_connections;
    const auto gap =
        CallCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("g3", 3, "gap"), UntilMs()); });
    CHECK(!gap);
    CHECK(gap.error().code == ErrorCode::OutcomeUnknown);

    /**
     * link 退役后新拨号开新 session（台账重置），空 key/seq=0 不受顺序约束。
     */
    auto probe = SubmitCo([&] {
        rpc::Request plain("svc", "M");
        plain.SetRequestId("p1")
            .SetReply(rpc::ReplyMode::Required)
            .SetTraffic(rpc::TrafficClass::Service)
            .SetRequestSchema("req")
            .SetResponseSchema("resp");
        return bbt_co_await a.client->Call(b.Target(), std::move(plain), UntilMs());
    });
    CHECK(WaitUntil([&] { return b.server->Stats().accepted_connections > accepted_before; }));
    CHECK(WaitUntil([&] { return b.calls.load() == 2; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(probe));
    auto unordered = SubmitCo([&] {
        rpc::Request plain("svc", "M");
        plain.SetRequestId("u1")
            .SetReply(rpc::ReplyMode::Required)
            .SetTraffic(rpc::TrafficClass::Service)
            .SetRequestSchema("req")
            .SetResponseSchema("resp");
        return bbt_co_await a.client->Call(b.Target(), std::move(plain), UntilMs());
    });
    CHECK(WaitUntil([&] { return b.calls.load() == 3; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(unordered));
    {
        std::lock_guard<std::mutex> lock(b.mu);
        CHECK(b.by_key["gap"].size() == 1 && b.by_key["gap"][0] == "g1");
        CHECK(b.by_key[""].size() == 2);
    }

    /**
     * 重复：同 key 同一 sequence 第二帧按协议违规退役 link。
     */
    auto d1 = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("d1", 1, "dup"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 4; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(d1));
    const auto dup =
        CallCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("d2", 1, "dup"), UntilMs()); });
    CHECK(!dup);
    CHECK(dup.error().code == ErrorCode::OutcomeUnknown);

    /**
     * 逆序：同 key seq=2 后到达的 seq=1 按违规退役（新 session 先收进 2）。
     */
    auto r2 = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("r2", 2, "rev"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 5; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(r2));
    const auto rev =
        CallCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("r1", 1, "rev"), UntilMs()); });
    CHECK(!rev);
    {
        std::lock_guard<std::mutex> lock(b.mu);
        CHECK(b.by_key["dup"].size() == 1);
        CHECK(b.by_key["rev"].size() == 1);
    }
    std::cout << "v2 ordering ledger: gap/duplicate/reversed reject protocol ok\n";
}

/**
 * ---- 4c. 统一关闭输入校验与 Server 跨 Engine 装配边界 ---------------------
 */
void CloseNegativeTimeout()
{
    AppHost a("app-a");

    /**
     * 默认控制面：裸 CloseOptions 是 Immediate + 不等物理回收。
     */
    const bbt::infra::CloseOptions defaults{};
    CHECK(defaults.mode == bbt::infra::CloseMode::Immediate);
    CHECK(!defaults.wait_release);

    /**
     * 需要等待却没有有限绝对期限的两种组合在发布原因/封口之前返回 InvalidArgument。
     */
    const auto unbounded = bbt::infra::CloseOptions::Graceful(bbt::coroutine::Deadline::max());
    auto r = CloseClient(a.client, unbounded);
    CHECK(!r && r.error().code == ErrorCode::InvalidArgument);
    r = CloseServer(a.server, unbounded);
    CHECK(!r && r.error().code == ErrorCode::InvalidArgument);
    bbt::infra::CloseOptions unbounded_wait = bbt::infra::CloseOptions::Immediate();
    unbounded_wait.wait_release = true;
    r = CloseClient(a.client, unbounded_wait);
    CHECK(!r && r.error().code == ErrorCode::InvalidArgument);
    r = CloseServer(a.server, unbounded_wait);
    CHECK(!r && r.error().code == ErrorCode::InvalidArgument);
    CHECK(!a.client->IsClosed());
    CHECK(!a.client->CloseReason().has_value());
    CHECK(!a.server->IsClosed());
    CHECK(!a.server->CloseReason().has_value());

    /**
     * Immediate + wait_release=true：同一 Close 内封口并完成物理收口（不另调
     * WaitStopped），封口立即可观察。
     */
    bbt::infra::CloseOptions immediate_release = bbt::infra::CloseOptions::Immediate();
    immediate_release.wait_release = true;
    immediate_release.deadline = std::chrono::steady_clock::now() + std::chrono::seconds{3};
    CHECK(CloseClient(a.client, immediate_release));
    CHECK(a.client->IsClosed());
    CHECK(CloseServer(a.server, immediate_release));
    CHECK(a.server->IsClosed());
    std::cout << "v2 close options validation + wait_release merge ok\n";
}

/**
 * CoRpcServer::Create 要求注入的 session_owner 与 Server 使用同一 net::Engine。
 */
void ServerCrossEngine()
{
    auto engine_a = net::Engine::Create(std::shared_ptr<net::RawHandler>{});
    auto engine_b = net::Engine::Create(std::shared_ptr<net::RawHandler>{});
    rpc::ClientOptions co;
    co.SetLocalIdentity("cross-a").SetTrustedNetworkPlaintext(true);
    auto made_client = rpc::CoRpcClient::Create(engine_a, co);
    CHECK(made_client);
    auto client = std::move(made_client).value();
    rpc::AdmissionHandler handler = [](rpc::AdmissionItem&&) {
        return bbt::infra::result<void>::ok();
    };

    std::error_code error;
    rpc::ServerOptions so;
    so.SetListenAddress(net::Address::Ip("127.0.0.1", 0, error));
    CHECK(!error);
    auto wrong = rpc::CoRpcServer::Create(engine_b, client, handler, so);
    CHECK(!wrong);
    CHECK(wrong.error().code == ErrorCode::InvalidArgument);

    auto same = rpc::CoRpcServer::Create(engine_a, client, handler, so);
    CHECK(same);

    /**
     * Engine 未 Run：显式收口对象，engine 由本用例归应用侧 RequestClose。
     */
    (void)same.value()->Close(bbt::infra::CloseOptions::Immediate());
    (void)client->Close(bbt::infra::CloseOptions::Immediate());
    (void)engine_a->Close();
    (void)engine_b->Close();
    std::cout << "v2 server cross-engine create rejected ok\n";
}

/**
 * ---- 5. lane 满队列隔离 ---------------------------------------------------------
 */
void Overload()
{
    rpc::ClientOptions tiny;
    tiny.SetActorQueueBytes(1024).SetServiceQueueBytes(64 * 1024).SetSystemQueueBytes(64 * 1024);
    AppHost a("app-a", tiny), b("app-b");
    auto boot = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(boot));

    /**
     * Actor lane cap(1024B) 小于单帧 → 每条 actor Send 立即 Overloaded
     * （确定性满队列隔离），Service lane 仍独立可用。
     */
    auto flooded = CallCo([&]() -> std::vector<bbt::infra::result<void>> {
        std::vector<bbt::infra::result<void>> out;
        for (unsigned i = 0; i != 4; ++i) {
            rpc::Request message("svc", "Tell");
            message.SetMessageId("flood" + std::to_string(i))
                .SetReply(rpc::ReplyMode::None)
                .SetTraffic(rpc::TrafficClass::Actor)
                .SetOrderingKey("k")
                .SetSequence(i + 2);
            std::string payload(4096, 'x');
            message.SetPayload({payload.begin(), payload.end()});
            out.push_back(bbt_co_await a.client->Send(b.Target(), std::move(message), SendUntil(3s, false)));
        }
        return out;
    });
    unsigned ok = 0, overloaded = 0;
    for (const auto& r : flooded) {
        if (r) {
            ++ok;
        } else if (r.error().code == ErrorCode::Overloaded) {
            ++overloaded;
        }
    }
    CHECK(ok == 0 && overloaded == 4);

    /**
     * Service lane 独立可用（满 actor 不阻塞）。同 key 连续 seq——入站
     * 顺序台账对缺口按协议违规退役 link，测试须遵守递增契约。
     */
    auto svc = SubmitCo([&] {
        return bbt_co_await a.client->Call(
            b.Target(), MakeCall("svc-ok", 2, "k", rpc::TrafficClass::Service), UntilMs());
    });
    CHECK(WaitUntil([&] { return b.calls.load() == 2; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(svc));
    const auto stats = a.client->Stats();
    CHECK(stats.actor.dropped >= 4);
    std::cout << "v2 overload actor=4-overloaded service-isolated ok\n";
}

/**
 * ---- 5b. lane 调度公平：Service 持续有流量时 Actor 不饿死 ---------
 * 单协程内把 Service lane 连续压满（>ServiceWeight 条），期间交叉投 Actor；
 * 权重轮询必须让 Actor 在 Service 未排空的情况下获得调度——两端 dequeued
 * 计数各自推进，actor.finished 不归零卡死。
 */
void LaneFairness()
{
    rpc::ClientOptions weights;
    weights.SetServiceWeight(4).SetActorWeight(1);
    AppHost a("app-a", weights), b("app-b");
    auto boot = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(boot));

    /**
     * 同协程顺序发出：6 条 Service Send + 2 条 Actor Send（Send 本地接纳即
     * 返回，不等写终态——队列在单协程内已排好出场序）。若 Actor 被 Service
     * 饿死，stats.actor.dequeued 停在 0；公平轮询下 Actor 与 Service 都推进。
     * 两 lane 用不同 ordering_key：同一 key 的 wire 到达序受入站顺序台账
     * 约束，跨 lane 交叉同一 key 反而是顺序违规场景，不属于本用例的公平性。
     */
    CallCo([&] {
        for (unsigned i = 0; i != 6; ++i) {
            rpc::Request svc_msg("svc", "Tell");
            svc_msg.SetMessageId("svc" + std::to_string(i))
                .SetReply(rpc::ReplyMode::None)
                .SetTraffic(rpc::TrafficClass::Service)
                .SetOrderingKey("svc-k")
                .SetSequence(i + 1)
                .SetRequestSchema("req")
                .SetResponseSchema("resp");
            auto sent = bbt_co_await a.client->Send(b.Target(), std::move(svc_msg), SendUntil(3s, false));
            (void)sent;
        }
        for (unsigned i = 0; i != 2; ++i) {
            rpc::Request act_msg("svc", "Tell");
            act_msg.SetMessageId("act" + std::to_string(i))
                .SetReply(rpc::ReplyMode::None)
                .SetTraffic(rpc::TrafficClass::Actor)
                .SetOrderingKey("act-k")
                .SetSequence(i + 1)
                .SetRequestSchema("req")
                .SetResponseSchema("resp");
            auto sent = bbt_co_await a.client->Send(b.Target(), std::move(act_msg), SendUntil(3s, false));
            (void)sent;
        }
    });

    /**
     * 两端都应真实交付：actor dequeued>0 且 server 端收到 actor 项。
     */
    CHECK(WaitUntil(
        [&] {
            const auto s = a.client->Stats();
            return s.actor.dequeued >= 2;
        },
        3s));
    const auto stats = a.client->Stats();
    CHECK(stats.service.dequeued >= 6);
    CHECK(stats.actor.dequeued >= 2);
    CHECK(WaitUntil([&] { return b.sends.load() >= 8; }, 3s));
    std::cout << "v2 lane fairness actor-not-starved ok svc=" << stats.service.dequeued
              << " actor=" << stats.actor.dequeued << "\n";
}

/**
 * ---- 6. committed 后超时 → OutcomeUnknown ----------------------------------------
 */
void Disconnect()
{
    AppHost a("app-a"), b("app-b");
    auto boot = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(boot));

    /**
     * framework 拿走后不完成（真实 Tell 处理延迟超过调用期限）。
     */
    b.on_item = [&b](rpc::AdmissionItem&& item) -> bbt::infra::result<void> {
        SubmitCo([reply = item.reply, lease = item.lease] {
            bbtco_sleep(600);
            if (reply) {
                auto fail_result = bbt_co_await reply->Fail(MakeError(ErrorCode::InternalError, "late"));
                (void)fail_result;
            }
            if (lease) {
                lease->Release();
            }
        });
        return bbt::infra::result<void>::ok();
    };
    auto late =
        CallCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("hang", 2, "k"), UntilMs(300ms)); });
    CHECK(!late);
    CHECK(late.error().code == ErrorCode::OutcomeUnknown || late.error().code == ErrorCode::TimedOut);

    /**
     * 等待迟到的 Fail 到达（不阻塞用例）。
     */
    std::this_thread::sleep_for(400ms);
    std::cout << "v2 disconnect committed outcome="
              << (late.error().code == ErrorCode::OutcomeUnknown ? "OutcomeUnknown" : "TimedOut") << "\n";
}

/**
 * ---- 7. deadline/参数校验 ---------------------------------------------------------
 */
void Deadline()
{
    AppHost a("app-a"), b("app-b");
    auto expired = CallCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("x", 1, "k"), UntilMs(-1s)); });
    CHECK(!expired && expired.error().code == ErrorCode::TimedOut);
    rpc::SendOptions no_deadline;
    auto rejected = CallCo([&] { return bbt_co_await a.client->Send(b.Target(), MakeSend("x", 1, "k"), no_deadline); });
    CHECK(!rejected && rejected.error().code == ErrorCode::InvalidArgument);

    /**
     * 入站预算按本地时钟换算（V1 budget_local_min 的通用断言）：80ms 剩余
     * 预算被接纳时，Request::Context().deadline 不晚于进入时刻 +100ms，
     * 不是远端绝对时间也不是无限期。
     */
    auto short_budget =
        SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("budget", 2, "k"), UntilMs(80ms)); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    const auto entered = Clock::now();
    {
        std::lock_guard<std::mutex> lock(b.mu);
        const auto inbound_deadline = b.admitted.front().request.Context().deadline;
        CHECK(inbound_deadline <= entered + 100ms);
        CHECK(inbound_deadline > entered - 1s);
    }
    b.ProcessAdmitted(Echo);

    /**
     * 预算已尽：成功或 TimedOut 都是真实终态
     */
    const auto settled = Get(short_budget);
    if (!settled) {
        CHECK(
            settled.error().code == ErrorCode::TimedOut || settled.error().code == ErrorCode::OutcomeUnknown
            || settled.error().code == ErrorCode::Closed);
    }
    std::cout << "v2 deadline expired + invalid-args rejected + inbound budget local-min\n";
}

/**
 * ---- 8. raw peer 握手 + epoch 注入 ------------------------------------------------
 */
void HandshakeRaw()
{
    AppHost a("app-a"), b("app-b");
    RawTcpPeer peer(b.address);

    rpc::SessionFrame hello;
    hello.kind = rpc::FrameKind::Hello;
    hello.traffic = rpc::TrafficClass::System;
    hello.peer_identity = "rogue-peer";
    hello.session_epoch = 7;
    hello.connection_nonce = 0xABCDEF;
    auto hello_bytes = rpc::EncodeFramedSessionFrame(hello, 8 * 1024 * 1024);
    CHECK(hello_bytes);
    peer.Send({reinterpret_cast<const char*>(hello_bytes.value().data()), hello_bytes.value().size()});

    const auto hdr = peer.ReadExact(4);
    const auto body_len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(hdr.data()));
    CHECK(body_len > 0 && body_len <= 64 * 1024);
    const auto body = peer.ReadExact(body_len);
    auto server_hello = rpc::DecodeSessionFrame(reinterpret_cast<const std::uint8_t*>(body.data()), body.size());
    CHECK(server_hello && server_hello.value().kind == rpc::FrameKind::Hello);
    CHECK(server_hello.value().peer_identity == "app-b");

    /**
     * "app-b" < "rogue-peer" → B 是 epoch owner → 先 HelloAck 带 selected。
     */
    const auto ack = peer.ReadExact(4);
    const auto ack_len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(ack.data()));
    CHECK(ack_len > 0 && ack_len <= 64 * 1024);
    const auto ack_body = peer.ReadExact(ack_len);
    auto hello_ack = rpc::DecodeSessionFrame(reinterpret_cast<const std::uint8_t*>(ack_body.data()), ack_body.size());
    CHECK(hello_ack && hello_ack.value().kind == rpc::FrameKind::HelloAck);
    const auto selected_epoch = hello_ack.value().session_epoch;
    CHECK(selected_epoch != 0);

    rpc::SessionFrame echo;
    echo.kind = rpc::FrameKind::HelloAck;
    echo.traffic = rpc::TrafficClass::System;
    echo.peer_identity = "rogue-peer";
    echo.session_epoch = selected_epoch;
    echo.connection_nonce = server_hello.value().connection_nonce;
    auto echo_bytes = rpc::EncodeFramedSessionFrame(echo, 8 * 1024 * 1024);
    CHECK(echo_bytes);
    peer.Send({reinterpret_cast<const char*>(echo_bytes.value().data()), echo_bytes.value().size()});

    /**
     * 错 epoch 的 Call → 丢弃（不入 admission；link 保持）。
     */
    rpc::SessionFrame call;
    call.kind = rpc::FrameKind::Call;
    call.traffic = rpc::TrafficClass::Service;
    call.session_epoch = selected_epoch + 1;
    call.request_id = "bad-epoch";
    call.service = "svc";
    call.method = "m";
    call.request_schema = "req";
    call.response_schema = "resp";
    call.reply = rpc::ReplyMode::Required;
    call.remaining_budget_ms = 1000;
    auto call_bytes = rpc::EncodeFramedSessionFrame(call, 8 * 1024 * 1024);
    CHECK(call_bytes);
    peer.Send({reinterpret_cast<const char*>(call_bytes.value().data()), call_bytes.value().size()});
    std::this_thread::sleep_for(300ms);
    CHECK(b.calls.load() == 0);
    std::cout << "v2 handshake raw + wrong-epoch dropped\n";
}

/**
 * ---- 8b. 握手合包/分片不丢帧 + 握手前缀限额即时断开 ----------------
 */
void HandshakeCoalesced()
{
    AppHost a("app-a"), b("app-b");
    RawTcpPeer peer(b.address);

    rpc::SessionFrame hello;
    hello.kind = rpc::FrameKind::Hello;
    hello.traffic = rpc::TrafficClass::System;
    hello.peer_identity = "zz-peer";
    hello.session_epoch = 7;
    hello.connection_nonce = 0xABCDEF;
    auto hello_bytes = rpc::EncodeFramedSessionFrame(hello, 8 * 1024 * 1024);
    CHECK(hello_bytes);

    /**
     * 分片注入 Hello（任意分片到达必须照常握手，ADR §5.2）。
     */
    peer.SendFragmented({reinterpret_cast<const char*>(hello_bytes.value().data()), hello_bytes.value().size()}, 7);

    const auto hdr = peer.ReadExact(4);
    const auto body_len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(hdr.data()));
    CHECK(body_len > 0 && body_len <= 64 * 1024);
    const auto body = peer.ReadExact(body_len);
    auto server_hello = rpc::DecodeSessionFrame(reinterpret_cast<const std::uint8_t*>(body.data()), body.size());
    CHECK(server_hello && server_hello.value().kind == rpc::FrameKind::Hello);
    CHECK(server_hello.value().peer_identity == "app-b");

    /**
     * "app-b" < "zz-peer" → B 是 epoch owner → HelloAck 带 selected。
     */
    const auto ack_hdr = peer.ReadExact(4);
    const auto ack_len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(ack_hdr.data()));
    const auto ack_body = peer.ReadExact(ack_len);
    auto hello_ack = rpc::DecodeSessionFrame(reinterpret_cast<const std::uint8_t*>(ack_body.data()), ack_body.size());
    CHECK(hello_ack && hello_ack.value().kind == rpc::FrameKind::HelloAck);
    const auto selected_epoch = hello_ack.value().session_epoch;
    CHECK(selected_epoch != 0);

    rpc::SessionFrame echo;
    echo.kind = rpc::FrameKind::HelloAck;
    echo.traffic = rpc::TrafficClass::System;
    echo.peer_identity = "zz-peer";
    echo.session_epoch = selected_epoch;
    echo.connection_nonce = server_hello.value().connection_nonce;
    auto echo_bytes = rpc::EncodeFramedSessionFrame(echo, 8 * 1024 * 1024);
    CHECK(echo_bytes);

    rpc::SessionFrame call;
    call.kind = rpc::FrameKind::Call;
    call.traffic = rpc::TrafficClass::Service;
    call.session_epoch = selected_epoch;
    call.request_id = "pipelined";
    call.service = "svc";
    call.method = "m";
    call.request_schema = "req";
    call.response_schema = "resp";
    call.reply = rpc::ReplyMode::Required;
    call.remaining_budget_ms = 3000;
    auto call_bytes = rpc::EncodeFramedSessionFrame(call, 8 * 1024 * 1024);
    CHECK(call_bytes);

    /**
     * echo HelloAck + 首 Call 同一 send 合包。握手 decoder 必须把余帧
     * 移交 live reader——Call 立即准入；随后 control Call 走同一 link。
     */
    std::string joined(reinterpret_cast<const char*>(echo_bytes.value().data()), echo_bytes.value().size());
    joined.append(reinterpret_cast<const char*>(call_bytes.value().data()), call_bytes.value().size());
    peer.Send(joined);
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }, 3s));
    b.ProcessAdmitted(Echo);
    const auto rsp_hdr = peer.ReadExact(4);
    const auto rsp_len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(rsp_hdr.data()));
    const auto rsp_body = peer.ReadExact(rsp_len);
    auto rsp = rpc::DecodeSessionFrame(reinterpret_cast<const std::uint8_t*>(rsp_body.data()), rsp_body.size());
    CHECK(rsp && rsp.value().kind == rpc::FrameKind::Response);
    CHECK(rsp.value().request_id == "pipelined");
    std::cout << "v2 handshake coalesced ack+call admitted\\n";
}

/**
 * ---- 8c. 合法最终 HelloAck + 192KiB 大 Call 同批合包切数据态 -----------
 * 192KiB > 128KiB（4+2*64KiB 握手 pending 上限）且 >64KiB（握手单帧限额）：
 * echo-Ack 与 Call 同一 send 合包时，若仍按握手 pending 界判定必被拒。
 * 修复要求：Ack 逐项验证通过 → 切数据态 → 同批余字节按数据限额补喂，
 * Call 正常准入并回 Response——不依赖 TCP 偶然分段（单次 ::send）。
 */
void HandshakeCoalescedBig()
{
    AppHost a("app-a"), b("app-b");
    (void)a;
    RawTcpPeer peer(b.address);

    rpc::SessionFrame hello;
    hello.kind = rpc::FrameKind::Hello;
    hello.traffic = rpc::TrafficClass::System;
    hello.peer_identity = "zz-peer";
    hello.session_epoch = 7;
    hello.connection_nonce = 0xABCDEF;
    auto hello_bytes = rpc::EncodeFramedSessionFrame(hello, 8 * 1024 * 1024);
    CHECK(hello_bytes);
    peer.Send({reinterpret_cast<const char*>(hello_bytes.value().data()), hello_bytes.value().size()});

    const auto hdr = peer.ReadExact(4);
    const auto body_len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(hdr.data()));
    CHECK(body_len > 0 && body_len <= 64 * 1024);
    const auto body = peer.ReadExact(body_len);
    auto server_hello = rpc::DecodeSessionFrame(reinterpret_cast<const std::uint8_t*>(body.data()), body.size());
    CHECK(server_hello && server_hello.value().kind == rpc::FrameKind::Hello);
    CHECK(server_hello.value().peer_identity == "app-b");

    /**
     * "app-b" < "zz-peer" → B 是 epoch owner → HelloAck 带 selected。
     */
    const auto ack_hdr = peer.ReadExact(4);
    const auto ack_len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(ack_hdr.data()));
    const auto ack_body = peer.ReadExact(ack_len);
    auto hello_ack = rpc::DecodeSessionFrame(reinterpret_cast<const std::uint8_t*>(ack_body.data()), ack_body.size());
    CHECK(hello_ack && hello_ack.value().kind == rpc::FrameKind::HelloAck);
    const auto selected_epoch = hello_ack.value().session_epoch;
    CHECK(selected_epoch != 0);

    rpc::SessionFrame echo;
    echo.kind = rpc::FrameKind::HelloAck;
    echo.traffic = rpc::TrafficClass::System;
    echo.peer_identity = "zz-peer";
    echo.session_epoch = selected_epoch;
    echo.connection_nonce = server_hello.value().connection_nonce;
    auto echo_bytes = rpc::EncodeFramedSessionFrame(echo, 8 * 1024 * 1024);
    CHECK(echo_bytes);

    rpc::SessionFrame call;
    call.kind = rpc::FrameKind::Call;
    call.traffic = rpc::TrafficClass::Service;
    call.session_epoch = selected_epoch;
    call.request_id = "big-pipelined";
    call.service = "svc";
    call.method = "m";
    call.request_schema = "req";
    call.response_schema = "resp";
    call.reply = rpc::ReplyMode::Required;
    call.remaining_budget_ms = 3000;

    /**
     * >128KiB：握手 pending 界外
     */
    call.payload.assign(192 * 1024, 0x5A);
    auto call_bytes = rpc::EncodeFramedSessionFrame(call, 8 * 1024 * 1024);
    CHECK(call_bytes);

    /**
     * 单次 send：最终 Ack 与同批 192KiB Call 不依赖 TCP 分段时机。
     */
    std::string joined(reinterpret_cast<const char*>(echo_bytes.value().data()), echo_bytes.value().size());
    joined.append(reinterpret_cast<const char*>(call_bytes.value().data()), call_bytes.value().size());
    peer.Send(joined);
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }, 5s));
    b.ProcessAdmitted(Echo);
    const auto rsp_hdr = peer.ReadExact(4);
    const auto rsp_len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(rsp_hdr.data()));
    const auto rsp_body = peer.ReadExact(rsp_len);
    auto rsp = rpc::DecodeSessionFrame(reinterpret_cast<const std::uint8_t*>(rsp_body.data()), rsp_body.size());
    CHECK(rsp && rsp.value().kind == rpc::FrameKind::Response);
    CHECK(rsp.value().request_id == "big-pipelined");
    std::cout << "v2 handshake coalesced ack+192KiB-call admitted\\n";
}

/**
 * ---- 8d. 确定性钉批：脚本化 ICoConnection 单次 Buffer 交付 -------
 * 「echo HelloAck + 192KiB Call」整批 >131076（4+2*64KiB 握手 pending
 * 界）。不经 TCP：批大小由脚本字节序固定钉住，不靠 TCP 分段/排程偶合。
 * 修复前整批先按 hello_phase Feed → pending 硬界拒绝；修复后 Ack 在
 * 增量补喂中先解出、验证后切数据态，同批余字节按数据限额准入。
 */
class ScriptedCoConnection final : public net::ICoConnection {
public:
    /**
     * 队列化剧本：每次 Read 弹出一条 outcome（Data=整批 Buffer）。
     * 握手协程与测试线程并发访问，全部状态经 mu 同步。
     */
    std::mutex mu;

    /**
     * 有新剧本/关闭时通知
     */
    std::condition_variable cv;
    std::deque<net::Buffer> script;

    /**
     * 按调用序拼接全部 Write 载荷
     */
    std::string written;
    std::atomic<int> reads{0};
    std::atomic<int> writes{0};
    bool closed{false};

    void Enqueue(net::Buffer outcome)
    {
        {
            std::lock_guard<std::mutex> lock(mu);
            script.push_back(std::move(outcome));
        }
        cv.notify_all();
    }

    void CloseScript()
    {
        {
            std::lock_guard<std::mutex> lock(mu);
            closed = true;
        }
        cv.notify_all();
    }

    std::string Written()
    {
        std::lock_guard<std::mutex> lock(mu);
        return written;
    }

    using net::ICoConnection::Read;
    using net::ICoConnection::TryRead;

    std::error_code TryRead(net::Buffer& out) override
    {
        std::lock_guard<std::mutex> lock(mu);
        if (script.empty()) {
            return net::make_error_code(closed ? net::NetErrc::Eof : net::NetErrc::WouldBlock);
        }
        out = std::move(script.front());
        script.pop_front();
        return {};
    }

    std::error_code Read(net::Buffer& buffer, const bbt::coroutine::WaitOptions&) override
    {
        ++reads;
        std::unique_lock<std::mutex> lock(mu);

        /**
         * 脚本未备好时等测试线程投递（有界 8s；不裸等——避免协程线程
         * 永久占住 scheduler 槽位）。超时返回 Eof 让握手按期限结算。
         */
        cv.wait_for(lock, 8s, [&] { return !script.empty() || closed; });
        if (script.empty()) {
            return net::make_error_code(net::NetErrc::Eof);
        }
        auto out = std::move(script.front());
        script.pop_front();
        buffer = std::move(out);
        return {};
    }

    std::error_code AsyncWrite(std::string) override { return {}; }

    net::WriteReport Write(std::string data, const bbt::coroutine::WaitOptions&) override
    {
        {
            std::lock_guard<std::mutex> lock(mu);
            written.append(data);
        }
        ++writes;
        net::WriteReport report;
        report.outcome = net::WriteOutcome{net::WritePhase::Done, data.size(), {}};
        return report;
    }

    std::error_code Close(const bbt::infra::CloseOptions& = {}, std::error_code = {}) override { return {}; }

    net::ReadView View() override { return {}; }

    std::error_code WaitClosed(const bbt::coroutine::WaitOptions&) override { return {}; }

    net::ConnectionStatus Status() const override { return {}; }
};

/**
 * 让脚本化连接经新的无状态 Channel 接缝进入握手：只转发字节与物理关闭，
 * 不改变 fake 的脚本/读写计数语义，也不在测试里复制协议状态。
 */
class ScriptedChannel final : public rpc::detail::ICoChannel {
public:
    explicit ScriptedChannel(ScriptedCoConnection& conn) noexcept
        : m_conn(conn)
    {
    }

    ~ScriptedChannel() override = default;
    ScriptedChannel(const ScriptedChannel&) = delete;
    ScriptedChannel& operator=(const ScriptedChannel&) = delete;

    using rpc::detail::ICoChannel::ReadBytes;

    std::error_code ReadBytes(net::Buffer& out, const bbt::coroutine::WaitOptions& options = {}) override
    {
        return m_conn.Read(out, options);
    }

    std::error_code TryReadBytes(net::Buffer& out) override { return m_conn.TryRead(out); }

    std::error_code WriteBytes(std::string bytes) override { return m_conn.AsyncWrite(std::move(bytes)); }

    net::WriteReport WriteBytes(std::string bytes, const bbt::coroutine::WaitOptions& options) override
    {
        return m_conn.Write(std::move(bytes), options);
    }

    void RequestClose(std::error_code reason = {}) override { (void)m_conn.Close({}, reason); }

    bool IsClosed() const override { return false; }

    bool IsReleased() const override { return false; }

    bool Valid() const override { return true; }

private:
    ScriptedCoConnection& m_conn;
};

/**
 * 单次移交形态的 owning Buffer：一段装整批字节（BufferTestAccess 接缝）。
 */
net::Buffer OneShotBuffer(const std::string& bytes)
{
    auto backing = std::make_shared<std::string>(bytes);
    std::vector<net::BufferSegment> segments{{backing->data(), backing->size()}};
    return net::BufferTestAccess::Make(std::move(backing), std::move(segments));
}

std::string Framed(const rpc::SessionFrame& f)
{
    auto bytes = rpc::EncodeFramedSessionFrame(f, 8 * 1024 * 1024);
    CHECK(bytes);
    return {reinterpret_cast<const char*>(bytes.value().data()), bytes.value().size()};
}

void HandshakeCoalescedBigPinned()
{

    /**
     * server 端身份 "app-b" < peer "zz-peer" → server 是 epoch owner：
     * InboundHandshake 剧本 = 读 Hello → 写 Hello+HelloAck → 读 echo。
     */
    ScriptedCoConnection conn;
    rpc::ClientOptions options;
    options.SetLocalIdentity("app-b").SetTrustedNetworkPlaintext(true);

    rpc::SessionFrame peer_hello;
    peer_hello.kind = rpc::FrameKind::Hello;
    peer_hello.traffic = rpc::TrafficClass::System;
    peer_hello.peer_identity = "zz-peer";
    peer_hello.session_epoch = 7;
    peer_hello.connection_nonce = 0xABCDEF;
    conn.Enqueue(OneShotBuffer(Framed(peer_hello)));

    /**
     * 握手协程跑出第一帧后阻塞在第二次 conn.Read（script 空→cv 等测试
     * 线程投递）；server Hello+HelloAck 已写进 fake 的 written，从中取出
     * selected epoch 与 server nonce 才能构造合法 echo——握手互锁决定
     * 第二帧输入不能预编，只能「先读回写再投递」，而投递天然单批。
     */
    rpc::SessionFrameDecoder decoder(8 * 1024 * 1024);
    ScriptedChannel channel(conn);
    auto future = SubmitCo([&]() -> bbt::infra::result<std::uint64_t> {
        std::string peer_identity;
        return rpc::detail::InboundHandshakeOnChannel(
            channel, options, 11, 0x1234, peer_identity, decoder, Clock::now() + 10s);
    });

    /**
     * 等「server 已写 Hello+HelloAck 且握手协程已进入第二次 Read」。
     */
    CHECK(WaitUntil([&] { return conn.writes.load() >= 2 && conn.reads.load() >= 2; }, 3s));
    const auto wire = conn.Written();
    const auto srv_hello_len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(wire.data()));
    auto srv_hello = rpc::DecodeSessionFrame(reinterpret_cast<const std::uint8_t*>(wire.data()) + 4, srv_hello_len);
    CHECK(srv_hello && srv_hello.value().kind == rpc::FrameKind::Hello);
    const auto* ack_hdr = reinterpret_cast<const std::uint8_t*>(wire.data()) + 4 + srv_hello_len;
    const auto srv_ack_len = rpc::DecodeFramePrefix(ack_hdr);
    auto srv_ack = rpc::DecodeSessionFrame(ack_hdr + 4, srv_ack_len);
    CHECK(srv_ack && srv_ack.value().kind == rpc::FrameKind::HelloAck);
    const auto selected = srv_ack.value().session_epoch;
    CHECK(selected != 0);

    rpc::SessionFrame echo;
    echo.kind = rpc::FrameKind::HelloAck;
    echo.traffic = rpc::TrafficClass::System;
    echo.peer_identity = "zz-peer";
    echo.session_epoch = selected;
    echo.connection_nonce = srv_hello.value().connection_nonce;

    rpc::SessionFrame call;
    call.kind = rpc::FrameKind::Call;
    call.traffic = rpc::TrafficClass::Service;
    call.session_epoch = selected;
    call.request_id = "pinned-big";
    call.service = "svc";
    call.method = "m";
    call.request_schema = "req";
    call.response_schema = "resp";
    call.reply = rpc::ReplyMode::Required;
    call.remaining_budget_ms = 3000;
    call.payload.assign(192 * 1024, 0x5A);

    /**
     * 钉住边界：echo+192KiB Call 为同一 Buffer 单段交付，批大小
     * 确定性 >4+2*64KiB 握手 pending 界（不靠 TCP 分段时机，也不拆两次
     * Read/另一次 Feed 冒充合包）。
     */
    const std::string batch = Framed(echo) + Framed(call);
    CHECK(batch.size() > 4 + 2 * 64 * 1024);
    conn.Enqueue(OneShotBuffer(batch));

    const auto result = Get(future);
    CHECK(result && result.value() == selected);

    /**
     * 握手 decoder 移交：同批余字节经数据限额补喂后，192KiB Call 完整
     * 可解（声明>64KiB 若在握手限额下早被拒）。
     */
    auto pipelined = decoder.Next();
    CHECK(pipelined && pipelined.value().has_value());
    CHECK(pipelined.value().value().kind == rpc::FrameKind::Call);
    CHECK(pipelined.value().value().request_id == "pinned-big");
    CHECK(pipelined.value().value().payload.size() == 192 * 1024);

    /**
     * 整批在第二次 Read 一次交付
     */
    CHECK(conn.reads.load() == 2);
    conn.CloseScript();
    std::cout << "v2 handshake pinned single-batch ack+192KiB admitted\\n";
}

/**
 * ---- 8e. stash 分支确定性用例：Read#1 = Hello+echoAck+大 Call 同批 ---------
 * Hello 在 Data 分支解出后余量入 stash；第二帧（echo Ack）走 stash 增量补喂，
 * 32KiB 块内 Ack 解出时 decoder 余量（Call 头部）与 stash 未喂尾部（Call 剩余
 * ~160KiB）并存——修复前 accept() 的 stash.bytes=TakePending() 赋值覆盖未喂
 * 尾部 → Call 静默截断；修复后前置拼接，尾部完整保留并按数据限额补喂。
 * server "app-b" < "zz-peer" 是 epoch owner：echo Ack 内容（epoch=proposal
 * 11、nonce=本端 0x1234、identity 回显）全部前置确定，可编入同一 Read 批。
 */
void HandshakeStashTailPreserved()
{
    ScriptedCoConnection conn;
    rpc::ClientOptions options;
    options.SetLocalIdentity("app-b").SetTrustedNetworkPlaintext(true);

    rpc::SessionFrame peer_hello;
    peer_hello.kind = rpc::FrameKind::Hello;
    peer_hello.traffic = rpc::TrafficClass::System;
    peer_hello.peer_identity = "zz-peer";
    peer_hello.session_epoch = 7;
    peer_hello.connection_nonce = 0xABCDEF;

    /**
     * i_own_epoch 分支：echo 必须 identity="zz-peer"、epoch=11、nonce=0x1234。
     */
    rpc::SessionFrame echo;
    echo.kind = rpc::FrameKind::HelloAck;
    echo.traffic = rpc::TrafficClass::System;
    echo.peer_identity = "zz-peer";
    echo.session_epoch = 11;
    echo.connection_nonce = 0x1234;

    rpc::SessionFrame call;
    call.kind = rpc::FrameKind::Call;
    call.traffic = rpc::TrafficClass::Service;
    call.session_epoch = 11;
    call.request_id = "stash-tail";
    call.service = "svc";
    call.method = "m";
    call.request_schema = "req";
    call.response_schema = "resp";
    call.reply = rpc::ReplyMode::Required;
    call.remaining_budget_ms = 3000;
    call.payload.assign(192 * 1024, 0x5A);

    /**
     * 单个 Buffer 单段：Hello+echoAck+192KiB Call。Call >32KiB 保证
     * stash 补喂解出 Ack 时仍存在未喂尾部（触发旧覆盖缺陷的必要条件）。
     */
    const std::string batch = Framed(peer_hello) + Framed(echo) + Framed(call);
    conn.Enqueue(OneShotBuffer(batch));

    rpc::SessionFrameDecoder decoder(8 * 1024 * 1024);
    ScriptedChannel channel(conn);
    auto future = SubmitCo([&]() -> bbt::infra::result<std::uint64_t> {
        std::string peer_identity;

        /**
         * proposal=11 / nonce=0x1234
         */
        return rpc::detail::InboundHandshakeOnChannel(
            channel, options, 11, 0x1234, peer_identity, decoder, Clock::now() + 10s);
    });
    const auto result = Get(future);
    CHECK(result && result.value() == 11);

    /**
     * 尾部完整保留：decoder（数据态）Next() 直接解出 192KiB Call——旧赋值
     * 覆盖下 stash 尾部丢失，该帧永远解不出。
     */
    auto pipelined = decoder.Next();
    CHECK(pipelined && pipelined.value().has_value());
    CHECK(pipelined.value().value().kind == rpc::FrameKind::Call);
    CHECK(pipelined.value().value().request_id == "stash-tail");
    CHECK(pipelined.value().value().payload.size() == 192 * 1024);

    /**
     * 整批在第一次 Read 一次交付
     */
    CHECK(conn.reads.load() == 1);
    conn.CloseScript();
    std::cout << "v2 handshake stash tail preserved\\n";
}

void HandshakePrefixReject()
{
    AppHost a("app-a"), b("app-b");
    (void)a;
    RawTcpPeer peer(b.address);

    /**
     * 合法 Hello 完成第一步握手（inbound 先读后写）。
     */
    rpc::SessionFrame hello;
    hello.kind = rpc::FrameKind::Hello;
    hello.traffic = rpc::TrafficClass::System;
    hello.peer_identity = "zz-peer";
    hello.session_epoch = 1;
    hello.connection_nonce = 0xABCDEF;
    auto hello_bytes = rpc::EncodeFramedSessionFrame(hello, 8 * 1024 * 1024);
    CHECK(hello_bytes);
    peer.Send({reinterpret_cast<const char*>(hello_bytes.value().data()), hello_bytes.value().size()});

    /**
     * server Hello
     */
    const auto hdr = peer.ReadExact(4);
    const auto len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(hdr.data()));
    CHECK(len > 0 && len <= 64 * 1024);
    (void)peer.ReadExact(len);

    /**
     * server HelloAck（B 是 epoch owner）
     */
    const auto ack_hdr = peer.ReadExact(4);
    const auto ack_len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(ack_hdr.data()));
    (void)peer.ReadExact(ack_len);

    /**
     * 握手中途（等 echo HelloAck）送来声明 128KiB 的前缀——超
     * min(max,64KiB) 握手限额，server 在不收 body 的前提下立即退役 link。
     */
    std::uint8_t pre[4];
    rpc::EncodeFramePrefix(128 * 1024, pre);
    peer.Send({reinterpret_cast<const char*>(pre), 4});
    CHECK(WaitUntil([&] { return peer.Eof(); }, 3s));

    /**
     * 该 link 拒绝不计 handshake 成功；server 本体仍存活可观。
     */
    CHECK(b.server->Stats().accepted_connections == 0);
    std::cout << "v2 handshake oversized prefix link retired\n";
}

void CloseDrain()
{
    auto a = std::make_unique<AppHost>("app-a");
    auto b = std::make_unique<AppHost>("app-b");
    auto boot =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b->calls.load() == 1; }));
    b->ProcessAdmitted(Echo);
    CHECK(Get(boot));

    /**
     * 留一条已接纳未归还的调用：Graceful 必须拒新并等它真实归还后才封口。
     */
    auto held =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("held", 2, "k"), UntilMs(5s)); });
    CHECK(WaitUntil([&] { return b->calls.load() == 2; }));
    auto graceful = SubmitCo([&] {
        return bbt_co_await b->server->Close(
            bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::milliseconds(5000)));
    });

    /**
     * 已接纳责任未结：Graceful 不得早成功。
     */
    CHECK(graceful.wait_for(200ms) != std::future_status::ready);

    /**
     * Draining 期间新调用立即收终态，而非等本地 deadline。
     */
    const auto rejected =
        CallCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("late", 3, "k"), UntilMs(800ms)); });
    CHECK(!rejected);
    CHECK(rejected.error().code != ErrorCode::TimedOut);
    b->ProcessAdmitted(Echo);
    CHECK(Get(held));
    CHECK(Get(graceful));
    CHECK(b->server->IsClosed());
    CHECK(b->server->CloseReason().has_value());

    /**
     * Timeout：封口后有界等待 listener/握手/session/写责任物理收口。
     */
    CHECK(CloseTimeoutFor(b->server, 3000));
    CHECK(WaitUntil([&] { return b->engine->CountConnections() == 0; }, 3s));

    auto after =
        CallCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("after", 4, "k"), UntilMs(800ms)); });
    CHECK(!after);
    std::cout << "v2 close graceful drain + timeout physical release ok\n";
}

/**
 * Close(Immediate)：立即逻辑封口并发布首个关闭原因；重复 Close/Shutdown
 * 不覆盖首个原因，新调用按该原因拒绝，已提交在途调用保持 OutcomeUnknown。
 */
void CloseReason()
{
    auto a = std::make_unique<AppHost>("app-a");
    auto b = std::make_unique<AppHost>("app-b");
    auto held =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("hold", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b->calls.load() == 1; }));

    const auto reason = MakeError(ErrorCode::Unavailable, "owner close");
    CHECK(CloseServer(b->server, bbt::infra::CloseOptions::Immediate(reason)));
    CHECK(b->server->IsClosed());
    auto observed = b->server->CloseReason();
    CHECK(observed.has_value());
    if (observed) {
        CHECK(observed->code == ErrorCode::Unavailable);
        CHECK(observed->message == "owner close");
    }

    /**
     * 重复 Close/Shutdown 不覆盖首个原因。
     */
    CHECK(CloseServer(b->server, bbt::infra::CloseOptions::Immediate(MakeError(ErrorCode::TimedOut, "later"))));
    (void)b->server->Close(bbt::infra::CloseOptions::Immediate(MakeError(ErrorCode::Unavailable, "shutdown later")));
    auto again = b->server->CloseReason();
    CHECK(again.has_value() && again->message == "owner close");

    /**
     * 已提交在途调用按 OutcomeUnknown 收场（不追改成成功）。
     */
    const auto held_result = Get(held);
    CHECK(!held_result);
    CHECK(held_result.error().code == ErrorCode::OutcomeUnknown);

    /**
     * 封口后新调用立即终态。
     */
    const auto late =
        CallCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("late", 2, "k"), UntilMs(800ms)); });
    CHECK(!late);
    CHECK(late.error().code != ErrorCode::TimedOut);

    /**
     * 归还已接纳项（写已不可能成功）：物理收口需要 inflight/写责任归零。
     */
    b->ProcessAdmitted(Echo);
    CHECK(CloseTimeoutFor(b->server, 3000));
    std::cout << "v2 close immediate publishes first reason + idempotent ok\n";
}

/**
 * Close(custom)：v2 已提交在途 Call 保留 OutcomeUnknown/request_phase，并在
 * 唯一终态上附带首个关闭原因的 code/message，不追改成已胜出的成功。
 */
void CloseReasonPending()
{
    auto a = std::make_unique<AppHost>("app-a");
    auto b = std::make_unique<AppHost>("app-b");
    auto held =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("hold", 1, "k"), UntilMs(5s)); });

    /**
     * 请求已被对端接纳 = 请求字节已完整写出，属于已提交。
     */
    CHECK(WaitUntil([&] { return b->calls.load() == 1; }));

    CHECK(
        CloseClient(a->client, bbt::infra::CloseOptions::Immediate(MakeError(ErrorCode::Unavailable, "owner close"))));
    const auto result = Get(held);
    CHECK(!result);
    CHECK(result.error().code == ErrorCode::OutcomeUnknown);
    CHECK(result.error().request_phase == bbt::infra::RequestPhase::RequestCommitted);
    const auto expected_code = std::to_string(static_cast<int>(ErrorCode::Unavailable));
    bool has_reason = false;
    bool has_code = false;
    for (const auto& detail : result.error().details) {
        if (detail.first == "close_reason" && detail.second == "owner close") {
            has_reason = true;
        }
        if (detail.first == "close_reason_code" && detail.second == expected_code) {
            has_code = true;
        }
    }
    CHECK(has_reason);
    CHECK(has_code);

    /**
     * 封口后新调用按同一原因拒绝，已有终态不被追改。
     */
    const auto late =
        CallCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("late", 2, "k"), UntilMs(3s)); });
    CHECK(!late);
    CHECK(late.error().code == ErrorCode::Unavailable);
    CHECK(late.error().message == "owner close");

    /**
     * 归还 b 侧已接纳项，避免测试自身留下未终结 handler。
     */
    b->ProcessAdmitted(Echo);
    CHECK(CloseServer(
        b->server,
        bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::milliseconds(3000))));
    std::cout << "v2 close reason attached to committed pending call\n";
}

/**
 * ---- 9. inflight：持有 lease 超额 → 立即 Overloaded 终态，连接可进展 ---
 * MaxInflight=1：Call1 被 framework 持有（未 Release）→ 第二条 Call 不等
 * deadline 即收 Overloaded Response；持有项完成后第三条正常——证明
 * inflight 覆盖已移交未释放 lease，且拒绝保留唯一 completion。
 */
void AdmissionInflight()
{
    rpc::ServerOptions one;
    one.SetMaxInflight(1);
    AppHost a("app-a"), b("app-b", {}, one);

    auto first = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("held", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));

    /**
     * Call1 已移交 framework、lease 未 Release——此时 server inflight=1 满。
     */
    auto second =
        CallCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("reject", 2, "k"), UntilMs(3s)); });
    CHECK(!second);
    CHECK(second.error().code == ErrorCode::Overloaded);

    /**
     * 释放持有项（写终态+Release）后额度恢复——第三条 Call 成功，证明
     * 拒绝是单次结算且 link 未死。
     */
    b.ProcessAdmitted(Echo);
    CHECK(Get(first));
    auto third =
        SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("after", 3, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 2; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(third));
    std::cout << "v2 admission inflight cap immediate-overload + recover ok\n";
}

/**
 * ---- 10. handler 拒绝 → 唯一错误终态 + inflight 不欠账、Graceful 收口 ----
 * handler 对 "bad" 返回错误：调用方立即收非 timeout 错误终态；server
 * inflight 回零且 finished/dropped 各记一次——Lease 单次结算。
 */
void AdmissionReject()
{
    AppHost a("app-a"), b("app-b");
    b.on_item = [&b](rpc::AdmissionItem&& item) -> bbt::infra::result<void> {
        if (item.request.RequestId() == "bad") {
            return bbt::infra::result<void>::err(MakeError(ErrorCode::Unavailable, "handler rejects"));
        }
        std::lock_guard<std::mutex> lock(b.mu);
        ++b.calls;
        b.admitted.push_back(std::move(item));
        return bbt::infra::result<void>::ok();
    };
    auto boot = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(boot));

    auto rejected =
        CallCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("bad", 2, "k"), UntilMs(3s)); });
    CHECK(!rejected);

    /**
     * 远端立即结算 handler 错误——不是本地 deadline 超时。
     */
    CHECK(rejected.error().code == ErrorCode::Unavailable);
    CHECK(WaitUntil([&] {
        const auto stats = b.server->Stats();
        return stats.service.inflight == 0 && stats.service.dropped >= 1 && stats.service.finished >= 2;
    }));

    /**
     * inflight 干净回零：Graceful 立即成功（若双扣下溢则假超时）。
     */
    CHECK(CloseServer(
        b.server,
        bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::milliseconds(1500))));
    std::cout << "v2 admission handler-reject unique-terminal graceful close ok\n";
}

/**
 * ---- 11. bytes：owning metadata/request 字段计入 admission 字节 --------
 * MaxHandlerBytes=512：payload 400B 单独可过（payload+64=464<512），但真实
 * owning 字节含 service/method/schema/id/metadata → >512 立即 Overloaded。
 */
void AdmissionBytes()
{
    rpc::ServerOptions bytes;
    bytes.SetMaxHandlerBytes(512);
    AppHost a("app-a"), b("app-b", {}, bytes);

    rpc::Request heavy("svc", "M");
    heavy.SetRequestId("fat")
        .SetReply(rpc::ReplyMode::Required)
        .SetTraffic(rpc::TrafficClass::Service)
        .SetOrderingKey("k")
        .SetSequence(1)
        .SetRequestSchema("req")
        .SetResponseSchema("resp");
    std::string payload(400, 'x');
    heavy.SetPayload({payload.begin(), payload.end()});
    heavy.SetMetadata({{"trace.id", std::string(200, 't')}});
    auto rejected = CallCo([&] { return bbt_co_await a.client->Call(b.Target(), heavy, UntilMs(3s)); });
    CHECK(!rejected);
    CHECK(rejected.error().code == ErrorCode::Overloaded);

    /**
     * 小请求（无 metadata、小 payload）仍被接纳——字节计费精确而非粗拒。
     */
    auto ok = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("thin", 2, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(ok));
    std::cout << "v2 admission owning-bytes charged + small admitted\n";
}

/**
 * ---- 12. 封口：Shutdown 后 Call 立即收终态而非等 deadline -----------------
 */
void AdmissionSealed()
{
    AppHost a("app-a"), b("app-b");
    auto boot = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(boot));

    /**
     * Shutdown 是宿主侧同步强制关闭：立即停止接纳并封口 admission。
     */
    (void)b.server->Close(bbt::infra::CloseOptions::Immediate());
    CHECK(WaitUntil([&] { return b.server->Stats().accepted_connections >= 1; }));
    auto sealed =
        CallCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("sealed", 2, "k"), UntilMs(3s)); });
    CHECK(!sealed);

    /**
     * 封口的唯一终态：Overloaded/Closed/Unavailable 之一——关键是立即而非
     * 本地 deadline TimedOut。
     */
    CHECK(sealed.error().code != ErrorCode::TimedOut);
    std::cout << "v2 admission sealed immediate terminal ok\n";
}

/**
 * ---- 13-16. 回复写终态：确定性钉住「已排队/写入中」交错 --------------------
 * 只经公开控制器暂停真实 writer/握手，不向严格路径安装测试回调。
 */
using rpc::HookAction;
using rpc::HookPoint;

rpc::HookSnapshot HookState(const std::shared_ptr<rpc::SessionHooks>& hooks, HookPoint point)
{
    const auto state = hooks->Snapshot(point);
    CHECK(state);
    return state.value();
}

struct HookPause {
    std::shared_ptr<rpc::SessionHooks> hooks;
    HookPoint point;
    std::uint64_t generation;
};

struct HookGate {
    std::vector<HookPause> pauses;

    void Arm(AppHost& host, HookPoint point)
    {
        auto hooks = host.client->Hooks();
        CHECK(hooks);
        const auto armed = hooks->Arm(point, HookAction::Pause, Clock::now() + 10s);
        CHECK(armed);
        pauses.push_back({std::move(hooks), point, armed.value()});
    }

    std::uint64_t Entered(HookPoint point) const
    {
        std::uint64_t count = 0;
        for (const auto& pause : pauses) {
            if (pause.point != point) {
                continue;
            }
            const auto state = HookState(pause.hooks, point);
            CHECK(state.generation == pause.generation);
            count += state.entered;
        }
        return count;
    }

    std::uint64_t InFlight() const
    {
        std::uint64_t count = 0;
        for (const auto& pause : pauses) {
            count += HookState(pause.hooks, pause.point).in_flight;
        }
        return count;
    }

    void Release(HookPoint point)
    {
        for (const auto& pause : pauses) {
            if (pause.point == point) {
                CHECK(pause.hooks->Clear(point, pause.generation));
            }
        }
    }

    void Release()
    {
        for (const auto& pause : pauses) {
            CHECK(pause.hooks->Clear(pause.point, pause.generation));
        }
    }
};

std::shared_ptr<HookGate> MakeWriterGate(AppHost& first, AppHost& second)
{
    auto gate = std::make_shared<HookGate>();
    gate->Arm(first, HookPoint::BeforeWrite);
    gate->Arm(second, HookPoint::BeforeWrite);
    return gate;
}

/**
 * 13a. 回复已入队/在写未终态：Complete 不早成功、Graceful Close 不早成功，
 *      写终态后唯一结算（计数只扣一次）。
 */
void ReplyWriteDeferred()
{
    AppHost a("app-a"), b("app-b");
    auto boot = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(boot));

    /**
     * 阶段一：真实写终态后 Complete 才成功。
     */
    auto held1 =
        SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("held1", 2, "k"), UntilMs(5s)); });
    CHECK(WaitUntil([&] { return b.calls.load() == 2; }));
    auto gate1 = MakeWriterGate(a, b);
    rpc::Request request;
    std::shared_ptr<rpc::ReplyCompletion> reply;
    {
        std::lock_guard<std::mutex> lock(b.mu);
        CHECK(!b.admitted.empty());
        request = b.admitted.front().request;
        reply = b.admitted.front().reply;
        b.admitted.front().lease->Release();
        b.admitted.pop_front();
    }
    auto done1 = SubmitCo([&] { return bbt_co_await reply->Complete(Echo(request)); });

    /**
     * 已排队/写入中的直接证据：writer 取走条目停在闸门，计数 >0。
     */
    CHECK(WaitUntil([&] { return gate1->Entered(HookPoint::BeforeWrite); }, 3s));
    CHECK(rpc::detail::ServerV2LedgerForTest(*b.server).writes_outstanding >= 1);

    /**
     * Complete 不得入队即返成功。
     */
    CHECK(done1.wait_for(200ms) != std::future_status::ready);
    gate1->Release();

    /**
     * 真实写终态后 Complete 成功
     */
    CHECK(Get(done1));

    /**
     * 调用方收到 Response
     */
    CHECK(Get(held1));
    CHECK(WaitUntil([&] { return rpc::detail::ServerV2LedgerForTest(*b.server).writes_outstanding == 0; }, 2s));

    /**
     * 阶段二：Graceful Close 不早成功——已接纳写责任未结算时关闭必须挂起，
     * 在写项的真实终态随后唯一结算。
     */
    auto held2 =
        SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("held2", 3, "k"), UntilMs(5s)); });
    CHECK(WaitUntil([&] { return b.calls.load() == 3; }));
    auto gate2 = MakeWriterGate(a, b);
    {
        std::lock_guard<std::mutex> lock(b.mu);
        CHECK(!b.admitted.empty());
        request = b.admitted.front().request;
        reply = b.admitted.front().reply;
        b.admitted.front().lease->Release();
        b.admitted.pop_front();
    }
    auto done2 = SubmitCo([&] { return bbt_co_await reply->Complete(Echo(request)); });
    CHECK(WaitUntil([&] { return gate2->Entered(HookPoint::BeforeWrite); }, 3s));
    CHECK(rpc::detail::ServerV2LedgerForTest(*b.server).writes_outstanding >= 1);
    CHECK(done2.wait_for(0ms) != std::future_status::ready);

    /**
     * Graceful 不早成功：诊断 Pause 中的在写项仍是未结算写责任——关闭挂起
     * 在已接纳责任上，不靠杀 link 提前收口。
     */
    auto graceful = SubmitCo([&] {
        return bbt_co_await b.server->Close(
            bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::milliseconds(5000)));
    });
    CHECK(graceful.wait_for(200ms) != std::future_status::ready);

    /**
     * 放行后写真实完成：Complete/调用方按真实终态返回，写责任唯一结算。
     */
    gate2->Release();
    CHECK(Get(done2));
    CHECK(Get(held2));
    CHECK(WaitUntil([&] { return rpc::detail::ServerV2LedgerForTest(*b.server).writes_outstanding == 0; }, 2s));
    CHECK(Get(graceful));
    CHECK(b.server->IsClosed());
    std::cout << "v2 reply write deferred: no early complete/graceful close ok\n";
}

/**
 * 13b. 写前关闭 link：Complete 返回真实写失败而非 ok；写责任唯一结算。
 */
void ReplyWriteFailClose()
{
    auto b = std::make_unique<AppHost>("app-b");
    auto a = std::make_unique<AppHost>("app-a");
    auto boot =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b->calls.load() == 1; }));
    b->ProcessAdmitted(Echo);
    CHECK(Get(boot));

    auto held =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("held", 2, "k"), UntilMs(5s)); });
    CHECK(WaitUntil([&] { return b->calls.load() == 2; }));
    auto gate = MakeWriterGate(*a, *b);

    rpc::Request request;
    std::shared_ptr<rpc::ReplyCompletion> reply;
    {
        std::lock_guard<std::mutex> lock(b->mu);
        CHECK(!b->admitted.empty());
        request = b->admitted.front().request;
        reply = b->admitted.front().reply;
        b->admitted.front().lease->Release();
        b->admitted.pop_front();
    }
    auto done = SubmitCo([&] { return bbt_co_await reply->Complete(Echo(request)); });
    CHECK(WaitUntil([&] { return gate->Entered(HookPoint::BeforeWrite); }, 3s));
    CHECK(rpc::detail::ServerV2LedgerForTest(*b->server).writes_outstanding >= 1);

    /**
     * 在写未终态时 v2 立即封口：Kill 对在写项保留责任（不属排队丢弃），
     * 真实终态由 WriterLoop 的失败路径唯一结算。
     */
    (void)b->server->Close(bbt::infra::CloseOptions::Immediate());
    CHECK(WaitUntil([&] { return gate->InFlight() == 0; }));
    const auto result = Get(done);

    /**
     * 不得伪报写成功
     */
    CHECK(!result);
    CHECK(result.error().code != ErrorCode::TimedOut);

    /**
     * 写终态唯一结算：凭据发布后计数恰好归零（发布先于计数递减，须等待）。
     */
    CHECK(WaitUntil([&] { return rpc::detail::ServerV2LedgerForTest(*b->server).writes_outstanding == 0; }, 2s));
    const auto held_result = Get(held);

    /**
     * link 退役：提交后失联
     */
    CHECK(!held_result);
    CHECK(CloseTimeoutFor(b->server, 3000));
    std::cout << "v2 reply write fail-close: real failure returned\n";
}

/**
 * 同一真实 link：先证明成功写终态，再同时保留一条 writer 持有项和两条
 * 排队项。Close 必须唤醒全部 Complete，不能漏结算或重复扣写责任。
 * 13b. 不要求远端确认的 Send 不产生回复写责任：接纳完成后 Graceful Close
 *      必须在有限预算内成功；旧缺陷会给它记一条无法结算的 open_replies，
 *      关闭只能等到 deadline 超时。
 */
void SendWithoutAcceptanceDrains()
{
    AppHost a("send-a"), b("send-b");
    auto sent = SubmitCo([&] { return bbt_co_await a.client->Send(b.Target(), MakeSend("m-1", 1, "k"), SendUntil()); });
    CHECK(WaitUntil([&] { return b.sends.load() == 1; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(sent));

    const auto closed = CloseServer(
        b.server, bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::seconds(2)));
    CHECK(closed);
    CHECK(b.server->IsClosed());
    CHECK(CloseTimeoutFor(b.server, 3000));
    CHECK(WaitUntil([&] { return b.engine->CountConnections() == 0; }, 3s));
    std::cout << "send without remote acceptance drains without leaked open reply\n";
}

void WriteTerminalOnce()
{
    AppHost a("app-a"), b("app-b");
    auto boot = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(boot));
    CHECK(WaitUntil([&] { return rpc::detail::ServerV2LedgerForTest(*b.server).writes_outstanding == 0; }));

    std::vector<std::future<bbt::infra::result<rpc::Response>>> calls;
    for (unsigned i = 0; i != 3; ++i) {
        calls.push_back(SubmitCo([&, i] {
            // clang-format off

            /**
             * 并发提交不能共用 ordering_key：契约要求出站方保证同 key 按序入队，
             * 这里改用每个请求独立的 key，避免调度顺序触发 inbound 顺序台账。
             */
            // clang-format on
            return bbt_co_await a.client->Call(
                b.Target(), MakeCall("held" + std::to_string(i), i + 2, "k-held-" + std::to_string(i)), UntilMs(10s));
        }));
    }
    CHECK(WaitUntil([&] { return b.calls.load() == 4; }));
    auto gate = MakeWriterGate(a, b);
    std::vector<rpc::Request> requests;
    std::vector<std::shared_ptr<rpc::ReplyCompletion>> replies;
    {
        std::lock_guard<std::mutex> lock(b.mu);
        CHECK(b.admitted.size() == 3);
        while (!b.admitted.empty()) {
            auto& item = b.admitted.front();
            requests.push_back(item.request);
            replies.push_back(item.reply);
            CHECK(item.lease->Release());
            b.admitted.pop_front();
        }
    }
    std::atomic<unsigned> completed{0};
    std::vector<std::future<bbt::infra::result<void>>> writes;
    for (unsigned i = 0; i != 3; ++i) {
        writes.push_back(SubmitCo([&, i] {
            auto result = bbt_co_await replies[i]->Complete(Echo(requests[i]));
            ++completed;
            return result;
        }));
        if (i == 0) {
            CHECK(WaitUntil([&] { return gate->Entered(HookPoint::BeforeWrite) == 1; }));
        }
    }

    /**
     * writer 被检查点扣留期间队列静止：三份责任中恰有两条仍排队。
     */
    CHECK(WaitUntil([&] {
        const auto stats = b.client->Stats().service;
        return stats.queued == 2 && stats.bytes_queued > 0
               && rpc::detail::ServerV2LedgerForTest(*b.server).writes_outstanding == 3;
    }));
    CHECK(completed.load() == 0);
    for (auto& write : writes) {
        CHECK(write.wait_for(0ms) != std::future_status::ready);
    }
    (void)b.server->Close(bbt::infra::CloseOptions::Immediate());

    /**
     * 重复封口不再拥有任何队列/写结算责任。
     */
    (void)b.server->Close(bbt::infra::CloseOptions::Immediate());
    for (auto& write : writes) {
        const auto result = Get(write);
        CHECK(!result);
        CHECK(result.error().code == ErrorCode::Closed);
    }
    CHECK(completed.load() == 3);
    CHECK(WaitUntil([&] { return gate->InFlight() == 0; }));
    for (auto& call : calls) {
        const auto result = Get(call);
        CHECK(!result && result.error().code == ErrorCode::OutcomeUnknown);
        CHECK(result.error().request_phase == bbt::infra::RequestPhase::RequestCommitted);
    }
    for (unsigned i = 0; i != 3; ++i) {
        CHECK(replies[i]->IsTerminal());
        const auto duplicate = CallCo([&, i] { return bbt_co_await replies[i]->Complete(Echo(requests[i])); });
        CHECK(!duplicate && duplicate.error().code == ErrorCode::InvalidArgument);
    }
    CHECK(CloseTimeoutFor(b.server, 3000));
    const auto ledger = rpc::detail::ServerV2LedgerForTest(*b.server);
    CHECK(ledger.writes_outstanding == 0);
    CHECK(ledger.links == 0 && ledger.handshakes == 0);
    CHECK(b.client->Stats().service.queued == 0);
    CHECK(b.client->Stats().service.bytes_queued == 0);
    CHECK(b.server->Stats().service.inflight == 0);
    (void)a.server->Close(bbt::infra::CloseOptions::Immediate());
    CHECK(CloseTimeoutFor(a.server, 3000));
    CHECK(WaitUntil([&] { return a.engine->CountConnections() == 0 && b.engine->CountConnections() == 0; }));
    CHECK(completed.load() == 3);
    std::cout << "write terminal once: writer + queued close, all waiters and resources settled\n";
}

/**
 * 13c. 回复等待预算 = 入站请求有限预算（min(remaining, incoming_timeout)），
 *      不是任意重置 30s。
 */
void ReplyWriteBudget()
{
    rpc::ServerOptions tight;
    tight.SetIncomingTimeout(400ms);
    auto b = std::make_unique<AppHost>("app-b", rpc::ClientOptions{}, tight);
    auto a = std::make_unique<AppHost>("app-a");
    auto boot =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b->calls.load() == 1; }));
    b->ProcessAdmitted(Echo);
    CHECK(Get(boot));

    auto held =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("held", 2, "k"), UntilMs(5s)); });
    CHECK(WaitUntil([&] { return b->calls.load() == 2; }));
    auto gate = MakeWriterGate(*a, *b);

    rpc::Request request;
    std::shared_ptr<rpc::ReplyCompletion> reply;
    {
        std::lock_guard<std::mutex> lock(b->mu);
        CHECK(!b->admitted.empty());
        request = b->admitted.front().request;
        reply = b->admitted.front().reply;
        b->admitted.front().lease->Release();
        b->admitted.pop_front();
    }
    auto done = SubmitCo([&] { return bbt_co_await reply->Complete(Echo(request)); });
    CHECK(WaitUntil([&] { return gate->Entered(HookPoint::BeforeWrite); }, 3s));

    /**
     * 请求预算 400ms（incoming_timeout 比 wire remaining 3s 更紧）：
     * Complete 应在预算内 TimedOut 而非等满 30s/5s。
     */
    const auto result = Get(done);
    CHECK(!result);
    CHECK(result.error().code == ErrorCode::TimedOut);
    CHECK(WaitUntil([&] { return gate->InFlight() == 0; }));

    /**
     * 写责任在 writer 终态路径唯一结算（在写项 deadline 已过 → 写失败）。
     */
    CHECK(WaitUntil([&] { return rpc::detail::ServerV2LedgerForTest(*b->server).writes_outstanding == 0; }, 3s));
    const auto held_result = Get(held);
    CHECK(!held_result);
    CHECK(held_result.error().code == ErrorCode::OutcomeUnknown);
    std::cout << "v2 reply write budget: request budget not reset\n";
}

/**
 * 13d. 关联字段不匹配：保留 ProtocolError 语义，但回复帧仍是真实写责任——
 *      Complete 等写终态、wire 上确实交付错误 Response。
 */
void ReplyCorrelationMismatch()
{
    AppHost a("app-a"), b("app-b");
    auto boot = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(boot));

    auto held =
        SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("held", 2, "k"), UntilMs(5s)); });
    CHECK(WaitUntil([&] { return b.calls.load() == 2; }));
    auto gate = MakeWriterGate(a, b);

    rpc::Request request;
    std::shared_ptr<rpc::ReplyCompletion> reply;
    {
        std::lock_guard<std::mutex> lock(b.mu);
        CHECK(!b.admitted.empty());
        request = b.admitted.front().request;
        reply = b.admitted.front().reply;
        b.admitted.front().lease->Release();
        b.admitted.pop_front();
    }

    /**
     * FromRequest 初始化后再误改关联字段仍须拒绝；错误终态仍要写出。
     */
    auto bad = Echo(request);
    bad.SetService("other-service");
    auto done = SubmitCo([&] { return bbt_co_await reply->Complete(std::move(bad)); });
    CHECK(WaitUntil([&] { return gate->Entered(HookPoint::BeforeWrite); }, 3s));
    CHECK(rpc::detail::ServerV2LedgerForTest(*b.server).writes_outstanding >= 1);

    /**
     * 等写终态
     */
    CHECK(done.wait_for(200ms) != std::future_status::ready);
    gate->Release();
    const auto result = Get(done);
    CHECK(!result);
    CHECK(result.error().code == ErrorCode::ProtocolError);

    /**
     * 调用方收到 wire 上真实交付的 ProtocolError Response。
     */
    const auto held_result = Get(held);
    CHECK(!held_result);
    CHECK(held_result.error().code == ErrorCode::ProtocolError);
    CHECK(WaitUntil([&] { return rpc::detail::ServerV2LedgerForTest(*b.server).writes_outstanding == 0; }, 2s));
    CHECK(CloseServer(
        b.server,
        bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::milliseconds(2000))));
    std::cout << "v2 reply correlation mismatch: ProtocolError + write accounted\n";
}

/**
 * ---- 14-19. 物理关闭责任与建立中额度 -----------------------------------------
 * 三个握手点分别注册；放行握手起点不会顺带放行尚未命中的发布点。
 */
void ArmDialGate(AppHost& host, const std::shared_ptr<HookGate>& gate)
{
    gate->Arm(host, HookPoint::OutboundBeforeHandshake);
    gate->Arm(host, HookPoint::InboundBeforeHandshake);
    gate->Arm(host, HookPoint::InboundBeforePublish);
}

std::shared_ptr<HookGate> MakeDialGate(AppHost& host)
{
    auto gate = std::make_shared<HookGate>();
    ArmDialGate(host, gate);
    return gate;
}

void ReleaseHandshakeStart(const std::shared_ptr<HookGate>& gate)
{
    gate->Release(HookPoint::OutboundBeforeHandshake);
    gate->Release(HookPoint::InboundBeforeHandshake);
}

/**
 * 14. RPC Pause 可取消；完成一次真实回复后 Close 冒烟收口。
 */
void CloseReleased()
{
    auto b = std::make_unique<AppHost>("app-b");
    auto a = std::make_unique<AppHost>("app-a");
    auto boot =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b->calls.load() == 1; }));
    b->ProcessAdmitted(Echo);
    CHECK(Get(boot));

    auto held =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("held", 2, "k"), UntilMs(5s)); });
    CHECK(WaitUntil([&] { return b->calls.load() == 2; }));
    auto gate = MakeWriterGate(*a, *b);
    rpc::Request request;
    std::shared_ptr<rpc::ReplyCompletion> reply;
    {
        std::lock_guard<std::mutex> lock(b->mu);
        request = b->admitted.front().request;
        reply = b->admitted.front().reply;
        b->admitted.front().lease->Release();
        b->admitted.pop_front();
    }
    auto done = SubmitCo([&] { return bbt_co_await reply->Complete(Echo(request)); });
    CHECK(WaitUntil([&] { return gate->Entered(HookPoint::BeforeWrite); }, 3s));
    gate->Release();
    CHECK(WaitUntil([&] { return gate->InFlight() == 0; }));
    CHECK(done.wait_for(5s) == std::future_status::ready);
    (void)Get(done);
    (void)Get(held);

    /**
     * Timeout：封口 + 有界等待物理收口（listener/握手/session/写责任）。
     */
    CHECK(CloseTimeoutFor(b->server, 3000));
    CHECK(b->server->IsClosed());
    CHECK(WaitUntil([&] { return b->engine->CountConnections() == 0; }, 3s));
    CHECK(WaitUntil([&] { return rpc::detail::ServerV2LedgerForTest(*b->server).writes_outstanding == 0; }, 2s));
    std::cout << "v2 close reply + responsibility smoke ok\n";
}

/**
 * 15. Server Close 自动取消入站 Pause，握手/conn 收口无需用户 Clear。
 */
void CloseHandshake()
{
    auto b = std::make_unique<AppHost>("app-b");
    auto gate = MakeDialGate(*b);
    RawTcpPeer peer(b->address);
    CHECK(WaitUntil([&] { return gate->Entered(HookPoint::InboundBeforeHandshake) == 1; }, 3s));
    CHECK(rpc::detail::ServerV2LedgerForTest(*b->server).handshakes >= 1);
    CHECK(gate->InFlight() == 1);

    /**
     * 立即封口：取消握手 Pause，不等待物理收口
     */
    (void)b->server->Close(bbt::infra::CloseOptions::Immediate());
    CHECK(b->server->IsClosed());
    CHECK(CloseTimeoutFor(b->server, 3000));
    CHECK(WaitUntil([&] { return gate->InFlight() == 0; }));
    CHECK(!HookState(b->client->Hooks(), HookPoint::InboundBeforeHandshake).closed);
    CHECK(WaitUntil(
        [&] {
            const auto ledger = rpc::detail::ServerV2LedgerForTest(*b->server);
            return ledger.links == 0 && ledger.handshakes == 0;
        },
        3s));
    CHECK(WaitUntil([&] { return b->engine->CountConnections() == 0; }, 3s));
    CHECK(WaitUntil([&] { return peer.Eof(); }));
    std::cout << "v2 close handshake-conn responsibility + sealed kill ok\n";
}

/**
 * 16. 封口后不发布 link：握手完成、安装前的建立中连接在 Close 封口后
 *     继续推进时必须退役——links 台账不新增、conn 物理释放。
 */
void CloseSealedNoPublish()
{
    auto b = std::make_unique<AppHost>("app-b");
    auto gate = std::make_shared<HookGate>();
    gate->Arm(*b, HookPoint::InboundBeforePublish);
    RawTcpPeer peer(b->address);

    /**
     * 完整握手（B 是 epoch owner："app-b" < "zz-peer"）。
     */
    rpc::SessionFrame hello;
    hello.kind = rpc::FrameKind::Hello;
    hello.traffic = rpc::TrafficClass::System;
    hello.peer_identity = "zz-peer";
    hello.session_epoch = 1;
    hello.connection_nonce = 0xABCDEF;
    auto hello_bytes = rpc::EncodeFramedSessionFrame(hello, 8 * 1024 * 1024);
    CHECK(hello_bytes);
    peer.Send({reinterpret_cast<const char*>(hello_bytes.value().data()), hello_bytes.value().size()});
    const auto hdr = peer.ReadExact(4);
    const auto len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(hdr.data()));
    const auto body = peer.ReadExact(len);
    auto srv_hello = rpc::DecodeSessionFrame(reinterpret_cast<const std::uint8_t*>(body.data()), body.size());
    CHECK(srv_hello && srv_hello.value().kind == rpc::FrameKind::Hello);
    const auto ack_hdr = peer.ReadExact(4);
    const auto ack_len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(ack_hdr.data()));
    const auto ack_body = peer.ReadExact(ack_len);
    auto ack = rpc::DecodeSessionFrame(reinterpret_cast<const std::uint8_t*>(ack_body.data()), ack_body.size());
    CHECK(ack && ack.value().kind == rpc::FrameKind::HelloAck);
    rpc::SessionFrame echo;
    echo.kind = rpc::FrameKind::HelloAck;
    echo.traffic = rpc::TrafficClass::System;
    echo.peer_identity = "zz-peer";
    echo.session_epoch = ack.value().session_epoch;
    echo.connection_nonce = srv_hello.value().connection_nonce;
    auto echo_bytes = rpc::EncodeFramedSessionFrame(echo, 8 * 1024 * 1024);
    CHECK(echo_bytes);
    peer.Send({reinterpret_cast<const char*>(echo_bytes.value().data()), echo_bytes.value().size()});

    /**
     * 第二挂点：握手完成、安装前——此时 Close 封口。
     */
    CHECK(WaitUntil([&] { return gate->Entered(HookPoint::InboundBeforePublish) == 1; }, 3s));

    /**
     * 立即封口（不等待）
     */
    (void)b->server->Close(bbt::infra::CloseOptions::Immediate());
    CHECK(WaitUntil([&] { return gate->InFlight() == 0; }));
    CHECK(WaitUntil(
        [&] {
            const auto ledger = rpc::detail::ServerV2LedgerForTest(*b->server);
            return ledger.links == 0 && ledger.handshakes == 0;
        },
        3s));
    CHECK(CloseTimeoutFor(b->server, 3000));
    CHECK(b->server->Stats().accepted_connections == 0);
    CHECK(WaitUntil([&] { return b->engine->CountConnections() == 0; }));
    CHECK(WaitUntil([&] { return peer.Eof(); }));
    std::cout << "v2 sealed post-close handshake not published ok\n";
}

/**
 * 17. MaxSessions=1 跨 peer 真实占用——第二个不同 peer 入站被拒（不分配
 *     第二份长期 session），建立中 outbound 占位也使超额。进程内 Engine
 *     驱动槽受 scheduler worker 数限制（每带监听 Engine 占 2 槽），超量
 *     peer 用 raw TCP socket 代替第三 Engine。
 */
void QuotaPeers()
{
    rpc::ClientOptions one;
    one.SetMaxSessions(1);
    rpc::ClientOptions two;
    two.SetMaxSessions(2);
    auto a = std::make_unique<AppHost>("app-a", one);
    auto b = std::make_unique<AppHost>("app-b", two);

    /**
     * b→a 建立唯一 link（占用 a 的全部 1 额度）。
     */
    auto boot =
        SubmitCo([&] { return bbt_co_await b->client->Call(a->Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return a->calls.load() == 1; }));
    a->ProcessAdmitted(Echo);
    CHECK(Get(boot));
    CHECK(a->client->Stats().sessions == 1);

    /**
     * 不同 peer（raw socket）完整握手入站：a 的 InstallSession 超额
     * Overloaded → conn 退役、不产生第二份长期 session。拒绝的物理表现
     * 是 conn 关闭——ack/echo 字节与 RST 竞速时 ReadExact 可能失败，两个
     * 都算真实拒绝证据。
     */
    const auto raw_handshake = [&](const char* identity, const net::Address& target) {
        auto peer = std::make_unique<RawTcpPeer>(target);
        rpc::SessionFrame hello;
        hello.kind = rpc::FrameKind::Hello;
        hello.traffic = rpc::TrafficClass::System;
        hello.peer_identity = identity;
        hello.session_epoch = 1;
        hello.connection_nonce = 0xBEEF;
        auto hello_bytes = rpc::EncodeFramedSessionFrame(hello, 8 * 1024 * 1024);
        CHECK(hello_bytes);
        peer->Send({reinterpret_cast<const char*>(hello_bytes.value().data()), hello_bytes.value().size()});
        try {
            const auto hdr = peer->ReadExact(4);
            const auto len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(hdr.data()));
            const auto body = peer->ReadExact(len);
            auto srv_hello = rpc::DecodeSessionFrame(reinterpret_cast<const std::uint8_t*>(body.data()), body.size());
            if (!srv_hello || srv_hello.value().kind != rpc::FrameKind::Hello) {

                /**
                 * 已关即拒绝证据
                 */
                return peer;
            }
            const auto ack_hdr = peer->ReadExact(4);
            const auto ack_len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(ack_hdr.data()));
            const auto ack_body = peer->ReadExact(ack_len);
            auto ack = rpc::DecodeSessionFrame(reinterpret_cast<const std::uint8_t*>(ack_body.data()), ack_body.size());
            if (!ack || ack.value().kind != rpc::FrameKind::HelloAck) {
                return peer;
            }
            rpc::SessionFrame echo;
            echo.kind = rpc::FrameKind::HelloAck;
            echo.traffic = rpc::TrafficClass::System;
            echo.peer_identity = identity;
            echo.session_epoch = ack.value().session_epoch;
            echo.connection_nonce = srv_hello.value().connection_nonce;
            auto echo_bytes = rpc::EncodeFramedSessionFrame(echo, 8 * 1024 * 1024);
            CHECK(echo_bytes);
            peer->Send({reinterpret_cast<const char*>(echo_bytes.value().data()), echo_bytes.value().size()});
        } catch (const std::exception&) {

            /**
             * ReadExact 在 conn 已关时抛错——也是拒绝证据。
             */
        }
        return peer;
    };
    {
        auto rejected = raw_handshake("zz-quota", a->address);
        CHECK(WaitUntil(
            [&] {
                try {
                    return rejected->Eof();
                } catch (...) {
                    return true;
                }
            },
            5s));

        /**
         * 无第二份长期 session
         */
        CHECK(a->client->Stats().sessions == 1);
    }

    /**
     * 建立中额度真实占用（合法路径）：b 的 owner 已装 1 条 a→b link——
     * MaxSessions=2 下 b→裸 listen 的拨号前检查通过（1<2）并占住
     * reserved=1；OutboundBeforeHandshake Pause 钉在握手起点时，再来的 raw peer 入站
     * install 按「已装 1 + 建立中 1」= 2 ≥ 2 超额拒绝。第三阶段移到
     * b 的 owner 是因为 a 的 MaxSessions=1 在新前置检查下连合法的
     * 建立责任都创建不了——a 的「建立中占位」语义已由
     * v2s_quota_establish_dial 覆盖。
     */
    int lfd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    CHECK(lfd >= 0);
    sockaddr_in bind_addr{};
    bind_addr.sin_family = AF_INET;
    bind_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    bind_addr.sin_port = 0;
    CHECK(::bind(lfd, reinterpret_cast<sockaddr*>(&bind_addr), sizeof(bind_addr)) == 0);
    CHECK(::listen(lfd, 4) == 0);
    socklen_t addr_len = sizeof(bind_addr);
    CHECK(::getsockname(lfd, reinterpret_cast<sockaddr*>(&bind_addr), &addr_len) == 0);
    const auto lport = ntohs(bind_addr.sin_port);
    auto gate = MakeDialGate(*b);
    auto outbound = SubmitCo([&] {
        return bbt_co_await b->client->Call(
            "tcp://127.0.0.1:" + std::to_string(lport), MakeCall("blocked", 1, "k"), UntilMs(5s));
    });

    /**
     * 拨号完成、握手起点被钉住：b 的 establishing reserved 已占。
     */
    CHECK(WaitUntil([&] { return gate->Entered(HookPoint::OutboundBeforeHandshake) == 1; }, 3s));

    /**
     * 第二个 raw peer 的入站握手（打到 b）也被同一闸门钉在起点
     * （entered=2）——放行后它完整握手并停在「安装前」第三挂点
     * （entered=3）。此时 b→lfd 的建立中握手仍在 conn.Read 等待
     * （裸 listener 不回应），reserved 持续计数。
     */
    std::unique_ptr<RawTcpPeer> rejected2;
    std::exception_ptr driver_error;
    std::thread driver([&] {
        try {
            rejected2 = raw_handshake("zz-quota2", b->address);
        } catch (...) {
            driver_error = std::current_exception();
        }
    });
    CHECK(WaitUntil([&] { return gate->Entered(HookPoint::InboundBeforeHandshake) == 1; }, 3s));
    ReleaseHandshakeStart(gate);
    CHECK(WaitUntil([&] { return gate->Entered(HookPoint::InboundBeforePublish) == 1; }, 3s));
    gate->Release();

    /**
     * install 判定「已装 1 + 建立中 1」超额：conn 退役、无第二份 session。
     */
    driver.join();
    if (driver_error) {
        std::rethrow_exception(driver_error);
    }
    CHECK(rejected2);
    CHECK(WaitUntil(
        [&] {
            try {
                return rejected2->Eof();
            } catch (...) {
                return true;
            }
        },
        5s));
    CHECK(b->client->Stats().sessions == 1);

    /**
     * 建立中握手最终也失败（裸 listener 不回应）——真实终态即可，
     * 不指定具体错误（可能是 TimedOut/Unavailable/Closed）。
     */
    const auto outbound_result = Get(outbound);
    CHECK(!outbound_result);
    ::close(lfd);
    gate->Release();
    std::cout << "v2 quota max-sessions cross-peer + establishing ok\n";
}

/**
 * 18. ADR §3.3 实际同时拨号：握手起点 Pause 钉住双向拨号，双侧并发到 4
 *     个建立中连接后才放行——identity 较小侧保留本地发起 link，最终
 *     links=1 是两个物理 TCP 经 fencing 收敛的结果而非串行假象。
 */
void SimultaneousDial()
{
    auto a = std::make_unique<AppHost>("app-a");
    auto b = std::make_unique<AppHost>("app-b");
    auto gate = MakeDialGate(*a);
    ArmDialGate(*b, gate);
    auto call_ab =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("a2b", 1, "k"), UntilMs(10s)); });

    /**
     * 等 a 的出站握手被闸门钉住，再发 b→a：两侧同刻都有「拨号已发起、
     * 握手未推进」的连接——真实同时拨号。
     */
    CHECK(WaitUntil([&] { return gate->Entered(HookPoint::OutboundBeforeHandshake) == 1; }, 3s));
    auto call_ba =
        SubmitCo([&] { return bbt_co_await b->client->Call(a->Target(), MakeCall("b2a", 2, "k"), UntilMs(10s)); });

    /**
     * 4 路到达：a-outbound + a-inbound + b-outbound + b-inbound。
     */
    CHECK(WaitUntil(
        [&] {
            return gate->Entered(HookPoint::OutboundBeforeHandshake) == 2
                   && gate->Entered(HookPoint::InboundBeforeHandshake) == 2;
        },
        5s));

    /**
     * 全过程额度证据：此刻两端各有 ≥2 条物理连接在册（建立中也占）。
     */
    CHECK(a->engine->CountConnections() >= 2);
    CHECK(b->engine->CountConnections() >= 2);
    ReleaseHandshakeStart(gate);

    /**
     * 两条入站连接还会停在「握手完成/安装前」第二挂点（entered 5/6），
     * 放行后才有 link 安装与 fencing。
     */
    CHECK(WaitUntil([&] { return gate->Entered(HookPoint::InboundBeforePublish) == 2; }, 5s));
    gate->Release();

    /**
     * fencing 后唯一幸存 link 服务两个方向——两个方向的 Call 都需要各自
     * 对端 ProcessAdmitted 驱动回复；pump 不触碰调用 future（Get 消费
     * 后 wait_for 会抛 future_error），只驱动 admitted 处理若干轮。
     * 预算按时间给足：link 退役/回复处理都要经 pump 推进，Debug 下 800 轮
     * （~2.4s）会在收敛前耗尽，令 links 记录永远停在退役前。
     */
    auto pump_stop = std::make_shared<std::atomic<bool>>(false);
    auto pump = SubmitCo([&, pump_stop] {
        for (unsigned i = 0; i != 8000 && !pump_stop->load(); ++i) {
            a->ProcessAdmitted(Echo);
            b->ProcessAdmitted(Echo);
            bbtco_sleep(3);
        }
    });
    auto r_ab = Get(call_ab);
    auto r_ba = Get(call_ba);

    /**
     * fencing 竞速语义：先到安装后被选举退役的已提交调用是合法的
     * OutcomeUnknown/Closed（ADR 不自动重放）——应用层重试一次落在
     * 收敛后的幸存 link；重试成功同样证明双向共享，失败则真挂。
     */
    if (!r_ba) {
        std::fprintf(stderr, "[dial] r_ba first err=%d retry\n", static_cast<int>(r_ba.error().code));
        auto retry =
            SubmitCo([&] { return bbt_co_await b->client->Call(a->Target(), MakeCall("b2a", 2, "k"), UntilMs(10s)); });
        r_ba = Get(retry);
    }
    if (!r_ab) {
        std::fprintf(stderr, "[dial] r_ab first err=%d retry\n", static_cast<int>(r_ab.error().code));
        auto retry =
            SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("a2b", 1, "k"), UntilMs(10s)); });
        r_ab = Get(retry);
    }
    CHECK(r_ab && r_ab.value().RequestId() == "a2b");
    CHECK(r_ba && r_ba.value().RequestId() == "b2a");

    /**
     * 最终 links=1 是 4 条建立中物理连接 fencing 收敛的结果。
     */
    CHECK(WaitUntil([&] { return a->client->Stats().links == 1 && b->client->Stats().links == 1; }, 5s));
    CHECK(a->client->Stats().links == 1);
    CHECK(b->client->Stats().links == 1);
    gate->Release();

    /**
     * 入站 link 曾被接受再 fencing 的证据：至少一端 accepted≥1。
     */
    CHECK(b->server->Stats().accepted_connections + a->server->Stats().accepted_connections >= 1);

    /**
     * pump 按引用持有 a/b：用例结束前必须等它真正退出，否则它在 AppHost
     * 析构后再次唤醒会访问已释放内存（ASan heap-use-after-free）。
     */
    pump_stop->store(true);
    (void)Get(pump);
    std::cout << "v2 simultaneous dial fenced to one link ok\n";
}

/**
 * 19. epoch 单调重连：同 identity 的 raw peer 两次握手，第二次 selected
 *     epoch 严格大于第一次（旧 link 物理退役后额度归还，新 epoch 发布）。
 */
void EpochReconnect()
{
    auto b = std::make_unique<AppHost>("app-b");
    auto a = std::make_unique<AppHost>("app-a");
    (void)a;
    const auto raw_handshake = [&]() -> std::uint64_t {
        RawTcpPeer peer(b->address);
        rpc::SessionFrame hello;
        hello.kind = rpc::FrameKind::Hello;
        hello.traffic = rpc::TrafficClass::System;
        hello.peer_identity = "zz-peer";
        hello.session_epoch = 1;
        hello.connection_nonce = 0xABCDEF;
        auto hello_bytes = rpc::EncodeFramedSessionFrame(hello, 8 * 1024 * 1024);
        CHECK(hello_bytes);
        peer.Send({reinterpret_cast<const char*>(hello_bytes.value().data()), hello_bytes.value().size()});
        const auto hdr = peer.ReadExact(4);
        const auto len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(hdr.data()));
        (void)peer.ReadExact(len);
        const auto ack_hdr = peer.ReadExact(4);
        const auto ack_len = rpc::DecodeFramePrefix(reinterpret_cast<const std::uint8_t*>(ack_hdr.data()));
        const auto ack_body = peer.ReadExact(ack_len);
        auto ack = rpc::DecodeSessionFrame(reinterpret_cast<const std::uint8_t*>(ack_body.data()), ack_body.size());
        CHECK(ack && ack.value().kind == rpc::FrameKind::HelloAck);
        return ack.value().session_epoch;
    };
    const auto first = raw_handshake();
    const auto second = raw_handshake();

    /**
     * owner 寿命内单调递增
     */
    CHECK(first != 0 && second > first);
    std::cout << "v2 epoch reconnect monotonic " << first << "->" << second << " ok\n";
}

/**
 * ---- 20-21. 拨号前额度预留（拨号前额度预留缺口）---------------------------
 * 裸 listen 夹具：内核 backlog 完成 TCP connect 而应用层永不 accept/回应——
 * 对端观察「真实 TCP 连接是否越过额度」用 accept() 计数，而不是只看
 * 最终 links=1 或统计计数（本包区分性要求）。
 */
struct BareListen {
    int fd{-1};
    std::uint16_t port{0};

    BareListen()
    {
        fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
        CHECK(fd >= 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        CHECK(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
        CHECK(::listen(fd, 8) == 0);
        socklen_t len = sizeof(addr);
        CHECK(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0);
        port = ntohs(addr.sin_port);
    }

    ~BareListen()
    {
        if (fd >= 0) {
            ::close(fd);
        }
    }

    std::string Target() const { return "tcp://127.0.0.1:" + std::to_string(port); }

    /**
     * 非阻塞 accept 一次：>0 表示真实 TCP 连接已越过额度到达本端；
     * 监听 fd 自身置 O_NONBLOCK（accept4 的 SOCK_NONBLOCK 只作用于
     * 返回的新 fd，不能让 accept 本身在空队列时不阻塞）。
     */
    int AcceptOnce()
    {
        const int flags = ::fcntl(fd, F_GETFL, 0);
        (void)::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        sockaddr_in peer{};
        socklen_t len = sizeof(peer);
        const int got = ::accept(fd, reinterpret_cast<sockaddr*>(&peer), &len);
        const int saved = errno;
        (void)::fcntl(fd, F_SETFL, flags);
        errno = saved;
        return got;
    }
};

/**
 * 20. 拨号前额度检查：MaxSessions=1 + 第一条建立中被握手起点 Pause 钉住
 *     （其 reserved 占位在册、握手未推进）时，对「不同 endpoint」的第二
 *     GetSession 必须在 CoDial/新握手之前 Overloaded——对端裸 listen 的
 *     accept 观察证明没有第二条物理连接越过上限。
 */
void QuotaEstablishDial()
{
    rpc::ClientOptions one;
    one.SetMaxSessions(1);
    auto a = std::make_unique<AppHost>("app-a", one);

    /**
     * a 的第一条拨号目标（永不回应，握手悬挂）
     */
    BareListen first;

    /**
     * 第二条拨号目标：accept 计数即越限证据
     */
    BareListen second;
    auto gate = MakeDialGate(*a);

    /**
     * 第一条拨号：connect 完成、挂点在握手 I/O 前——reserved 已占。
     * 拨号本身也走 Call 的 deadline：3s 足够盖住区分窗口且放行后
     * 快速收场。
     */
    auto pending =
        SubmitCo([&] { return bbt_co_await a->client->Call(first.Target(), MakeCall("p1", 1, "k"), UntilMs(3s)); });
    CHECK(WaitUntil([&] { return gate->Entered(HookPoint::OutboundBeforeHandshake) == 1; }, 3s));

    /**
     * 此刻第一条建立中责任真实存在（conn 在手、占位在册）。
     */
    CHECK(WaitUntil([&] { return a->engine->CountConnections() >= 1; }, 3s));

    /**
     * 第二条不同 endpoint 的 GetSession：必须在拨号/握手前 Overloaded；
     * 若仍走旧逻辑（先拨号后检查），裸 listen 的 accept 会真实收到
     * 第二条连接——区分点。
     */
    auto rejected =
        SubmitCo([&] { return bbt_co_await a->client->Call(second.Target(), MakeCall("p2", 2, "k"), UntilMs(3s)); });
    const auto rejected_result = Get(rejected);
    CHECK(!rejected_result);
    CHECK(rejected_result.error().code == ErrorCode::Overloaded);

    /**
     * 物理证据：第二条目标 listen 上不得出现任何已 accept 的连接。
     * 给可能漏网的 connect 留出窗口
     */
    std::this_thread::sleep_for(200ms);
    CHECK(second.AcceptOnce() < 0);
    CHECK(errno == EAGAIN || errno == EWOULDBLOCK);

    /**
     * 无安装、无第二占位
     */
    CHECK(a->client->Stats().sessions == 0);

    /**
     * 放行第一条（对端不回应 → 握手随 deadline/close 失败）；不依赖
     * 其结果，只证明闸门解除、资源回收路径不挂。
     */
    gate->Release();
    (void)Get(pending);
    std::cout << "v2 quota pre-dial reject: overloaded before second conn\n";
}

/**
 * 21. 建立责任回收后可再占：同 endpoint 并发 GetSession 共用一条建立
 *     记录（不双占）；第一条建立中 conn 失败后责任保留至真实释放再
 *     归还——下一个请求能拿到额度并建成唯一 link。
 */
void QuotaReleaseRetry()
{
    rpc::ClientOptions one;
    one.SetMaxSessions(1);
    auto a = std::make_unique<AppHost>("app-a", one);
    auto b = std::make_unique<AppHost>("app-b");

    /**
     * 拨得通但永不回应：第一条责任将在此失败
     */
    BareListen dead;

    /**
     * 同一 endpoint 两个并发 GetSession：共用一条建立记录——若各自
     * 占位则第二条必然撞额（区分「不双占」）。dead 对端永不回应，
     * 两个调用最终同以 TimedOut/Unavailable 收场；不指定唯一错误码，
     * 只要求同一条建立责任下都真实失败而非 Overloaded/伪成功。
     */
    auto gate = MakeDialGate(*a);
    auto c1 =
        SubmitCo([&] { return bbt_co_await a->client->Call(dead.Target(), MakeCall("d1", 1, "k"), UntilMs(1500ms)); });
    auto c2 =
        SubmitCo([&] { return bbt_co_await a->client->Call(dead.Target(), MakeCall("d2", 2, "k"), UntilMs(1500ms)); });
    CHECK(WaitUntil([&] { return gate->Entered(HookPoint::OutboundBeforeHandshake) == 1; }, 3s));

    /**
     * 两条调用共享一条建立中 conn：对端物理上只有一条连接。
     */
    CHECK(a->engine->CountConnections() == 1);
    gate->Release();
    const auto r1 = Get(c1);
    const auto r2 = Get(c2);
    CHECK(!r1 && !r2);
    CHECK(r1.error().code != ErrorCode::Overloaded);
    CHECK(r1.error().code == r2.error().code);

    /**
     * 责任归还后（conn 随建立记录消亡而物理关闭）额度空出：新目标可
     * 建立唯一 link——「真实释放后可接纳」而非永久占用。
     */
    CHECK(WaitUntil([&] { return a->engine->CountConnections() == 0; }, 3s));
    auto boot =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("boot", 3, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b->calls.load() == 1; }));
    b->ProcessAdmitted(Echo);
    CHECK(Get(boot));
    CHECK(a->client->Stats().sessions == 1);
    std::cout << "v2 quota release then admit: shared record + retry ok\n";
}

/**
 * ---- 未 Start 直接 Close/Shutdown 幂等不虚假等待；queue-full + close +
 * stats 并发不卡死、计数一致；IsClosed/LocalAddress 并发读与 Start/Close
 * 同锁 --------------------------------------------------------------
 */
void NeverStartedDrainClose()
{

    /**
     * 未 Start 的 v2 Server：Create 后 queue 为空、accept 任务未登记——
     * Close 不得解引用空 queue，也不得在 accept_loop_exited 上虚假等待
     * （无启动任务即视为已退出）。
     */
    auto engine = net::Engine::Create(std::shared_ptr<net::RawHandler>{});
    rpc::ClientOptions co;
    co.SetLocalIdentity("never-started").SetTrustedNetworkPlaintext(true);
    auto made_client = rpc::CoRpcClient::Create(engine, co);
    CHECK(made_client);
    auto client = std::move(made_client).value();
    std::error_code addr_error;
    auto made_server = rpc::CoRpcServer::Create(
        engine,
        client,
        [](rpc::AdmissionItem&&) { return bbt::infra::result<void>::ok(); },
        rpc::ServerOptions{}.SetListenAddress(net::Address::Ip("127.0.0.1", 0, addr_error)));
    CHECK(!addr_error);
    CHECK(made_server);
    auto server = std::move(made_server).value();

    /**
     * 未 Start 的 Graceful 立即完成（DrainedLocked 无 queue 也为真）并封口。
     */
    CHECK(CloseServer(
        server,
        bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::milliseconds(1500))));
    CHECK(server->IsClosed());

    /**
     * Timeout 对已封口对象幂等：不得等 accept_loop_exited（无 accept
     * 任务），物理收口谓词同样空即真。
     */
    CHECK(CloseTimeoutFor(server, 1500));

    /**
     * 已封口：重复 Close 幂等 ok（已排空即成功，不被追改），首因不覆盖。
     */
    CHECK(CloseServer(server, bbt::infra::CloseOptions::Immediate()));
    CHECK(CloseServer(
        server, bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::milliseconds(500))));
    const auto first_reason = server->CloseReason();
    CHECK(first_reason.has_value() && first_reason->code == ErrorCode::Closed);
    (void)server->Close(bbt::infra::CloseOptions::Immediate(MakeError(ErrorCode::TimedOut, "later")));

    /**
     * 并发 IsClosed/LocalAddress 与 Shutdown（Start 未发生）同锁读取——
     * 读侧与写侧同一把 v2->mu，不构成数据竞争。在 Shutdown 前
     * 启动 reader 覆盖并发窗口；reads 计数在 stop 前推进即证明循环
     * 真实执行（Close 已幂等完成，reader 立即退出）。
     */
    std::atomic<bool> stop{false};
    std::atomic<unsigned> reads{0};
    std::thread reader([&] {
        while (!stop.load(std::memory_order_acquire)) {
            (void)server->IsClosed();
            (void)server->LocalAddress();
            ++reads;
        }
    });
    (void)server->Close(bbt::infra::CloseOptions::Immediate());
    CHECK(server->IsClosed());
    CHECK(server->CloseReason().has_value());
    if (const auto observed = server->CloseReason(); observed) {

        /**
         * 首个原因不被覆盖
         */
        CHECK(observed->code == ErrorCode::Closed);
    }
    CHECK(WaitUntil([&] { return reads.load() > 0; }, 1s));
    stop.store(true, std::memory_order_release);
    reader.join();
    std::cout << "v2 never-started close/shutdown idempotent + concurrent reads ok\n";
}

void QueueFullDrainStatsConcurrent()
{

    /**
     * queue-full 下 Admit 的 dropped 记账不再在 impl->mu 内取 server->mu：
     * 与 Close（server->mu → queue->Queued(impl->mu)）不构成反向锁序闭环。
     * 并发 Stats 同样只走 queue 观察面，不直读 impl 字段。
     */
    rpc::ServerOptions opts;

    /**
     * MaxInflight 放大避免先触 cap；MaxHandlerBytes 收窄让字节计数在
     * queue 内快速打满——queue-full 拒绝经 impl->mu 满判定路径。
     */
    opts.SetMaxInflight(64).SetMaxHandlerBytes(256);
    AppHost a("app-a"), b("app-b", {}, opts);

    /**
     * handler 门：worker 取出第 1 条后在 handler 内阻塞——后续 Admit
     * 真实积压在 admission queue 内，字节计数打满后拒绝。
     */
    auto gate = std::make_shared<Gate>();
    std::atomic<bool> armed{false};
    b.on_item = [&b, gate, &armed](rpc::AdmissionItem&& item) -> bbt::infra::result<void> {
        if (armed.load(std::memory_order_acquire)) {
            gate->Pause();
        }
        std::lock_guard<std::mutex> lock(b.mu);
        ++b.calls;
        b.admitted.push_back(std::move(item));
        return bbt::infra::result<void>::ok();
    };

    /**
     * 先用一条 Call 建立 link（boot 不经 gate）；建立后再并发灌水。
     */
    auto boot = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(boot));
    armed.store(true, std::memory_order_release);

    /**
     * 8 条并发 Call（各自独立协程）：第 1 条在 handler 阻塞，中间条目
     * 积压在 queue（每条 ~110B），字节预算 256B 打满后触发 queue-full
     * 拒绝；各条 Overloaded/成功分布随 worker 时序，断言总量与类型。
     * 并发同 key 的顺序号会乱序到达——本用例度量 queue-full，不度量顺序，
     * 洪水请求不带 ordering_key/sequence（seq=0 不启用顺序台账）。
     */
    std::vector<std::future<bbt::infra::result<rpc::Response>>> flood;
    for (unsigned i = 0; i != 8; ++i) {
        flood.push_back(SubmitCo([&a, &b, i] {
            return bbt_co_await a.client->Call(b.Target(), MakeCall("f" + std::to_string(i), 0, ""), UntilMs(6s));
        }));
    }

    /**
     * 并发：Stats 轮询与 queue 积压/拒绝路径交错；Graceful Close 在拒绝
     * 记账后启动，与积压在途项的收口观察交错——关闭必须在时限内返回
     * （无死锁；queue 积压与 inflight 未释放期间合法 TimedOut 也接受）。
     * 先汇合全部线程再断言，避免 CHECK 失败时 joinable thread 析构。
     */
    std::atomic<bool> stop{false};
    std::thread stats_reader([&] {
        while (!stop.load(std::memory_order_acquire)) {
            const auto s = b.server->Stats();
            (void)s;
        }
    });

    /**
     * queue 积压与并发拒绝真实发生（宽松等待，失败也不跳过早汇合）。
     */
    const bool dropped_seen = WaitUntil(
        [&] {
            const auto s = b.server->Stats();
            return s.service.dropped >= 1;
        },
        3s);

    /**
     * Close 持 server->mu 等 DrainedLocked，经 queue 观察面读 impl->mu——
     * 反向序 Admit（impl->mu → server->mu）已消除，此处不得死锁。
     */
    auto close_result = SubmitCo([&] {
        return bbt_co_await b.server->Close(
            bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::milliseconds(1200)));
    });
    const bool close_done = close_result.wait_for(std::chrono::seconds(4)) == std::future_status::ready;

    /**
     * 先撤闸再放行：放行后 worker 立刻取下一条，若此时 armed 仍为真会再次
     * 进入无人释放的 Pause（直到其 10s 等待到期），责任无法在窗口内归零。
     */
    armed.store(false, std::memory_order_release);
    gate->Release();
    stop.store(true, std::memory_order_release);
    stats_reader.join();
    CHECK(dropped_seen);
    CHECK(close_done);

    /**
     * 并发 Close 的结果必须真实分类：在途 handler 责任未结时允许 TimedOut，
     * 其他错误码是缺陷；不得以 (void) 丢弃返回值。
     */
    const auto close_first = close_result.get();
    CHECK(close_first || close_first.error().code == ErrorCode::TimedOut);

    /**
     * 责任真实归还：把 handler 已接纳的 item 逐个 Complete+Release，直到
     * admission 队列、inflight 与写责任全部归零——不以 boot 已满足的
     * calls>=1 当作「已消费」。
     */
    const auto responsibility_clear = [&] {
        std::size_t pending = 0;
        {
            std::lock_guard<std::mutex> lock(b.mu);
            pending = b.admitted.size();
        }
        if (pending != 0) {
            b.ProcessAdmitted(Echo, pending);
            return false;
        }
        const auto s = b.server->Stats();
        const auto writes = rpc::detail::ServerV2LedgerForTest(*b.server).writes_outstanding;
        return s.service.inflight == 0 && s.service.queued == 0 && s.service.bytes_queued == 0 && writes == 0;
    };
    CHECK(WaitUntil(responsibility_clear, 10s));
    unsigned ok = 0, overloaded = 0, other = 0;
    for (auto& f : flood) {
        const auto r = Get(f);
        if (r) {
            ++ok;
        } else if (r.error().code == ErrorCode::Overloaded) {
            ++overloaded;
        } else if (r.error().code == ErrorCode::OutcomeUnknown || r.error().code == ErrorCode::Closed) {

            /**
             * owner 封口（Close/Shutdown）令 CoAccept 返回 canceled 后
             * Seal——既有 link 收口是并发关闭的合法终态。
             */
            ++other;
        } else {
            ++other;
            std::fprintf(
                stderr, "dbg flood err code=%d msg=%s\n", static_cast<int>(r.error().code), r.error().message.c_str());
        }
    }
    CHECK(overloaded >= 1);
    CHECK(ok + overloaded + other == 8);

    /**
     * 责任归零后 Timeout Close 必须收口到真实终态：物理释放全部完成，
     * 不以 TimedOut 作为本用例可接受的终点。
     */
    CHECK(CloseTimeoutFor(b.server, 10000));
    CHECK(b.server->IsClosed());
    CHECK(WaitUntil([&] { return b.engine->CountConnections() == 0; }, 5s));

    /**
     * 最终责任统计：inflight/queue/写责任/link 台账全部归零。
     */
    CHECK(WaitUntil(
        [&] {
            const auto s = b.server->Stats();
            const auto ledger = rpc::detail::ServerV2LedgerForTest(*b.server);
            return s.service.inflight == 0 && s.service.queued == 0 && s.service.bytes_queued == 0
                   && s.active_connections == 0 && ledger.links == 0 && ledger.handshakes == 0
                   && rpc::detail::ServerV2LedgerForTest(*b.server).writes_outstanding == 0;
        },
        5s));
    const auto final_stats = b.server->Stats();
    CHECK(final_stats.service.finished >= final_stats.service.dropped);
    std::cout << "v2 queue-full close/stats concurrent no deadlock"
              << " ok=" << ok << " overloaded=" << overloaded << " finished=" << final_stats.service.finished << "\n";
}

/**
 * 20. SessionHooks 公共状态契约：owner 隔离、generation 代际、旧 token
 *     幂等清理，以及 owner 封口后的 handle 安全性。
 */
void HookStateContract()
{
    auto a = std::make_unique<AppHost>("app-a");
    auto b = std::make_unique<AppHost>("app-b");
    auto hooks_a = a->client->Hooks();
    auto hooks_b = b->client->Hooks();
    CHECK(hooks_a && hooks_b);
    CHECK(hooks_a.get() != hooks_b.get());
    CHECK(!HookState(hooks_a, HookPoint::BeforeWrite).armed);
    CHECK(!HookState(hooks_b, HookPoint::BeforeWrite).armed);

    const auto past = hooks_a->Arm(HookPoint::BeforeWrite, HookAction::Pause, Clock::now() - 1ms);
    CHECK(!past && past.error().code == ErrorCode::InvalidArgument);
    const auto infinite = hooks_a->Arm(HookPoint::BeforeWrite, HookAction::Pause, bbt::coroutine::Deadline::max());
    CHECK(!infinite && infinite.error().code == ErrorCode::InvalidArgument);
    const auto too_far = hooks_a->Arm(HookPoint::BeforeWrite, HookAction::Pause, Clock::now() + 11s);
    CHECK(!too_far && too_far.error().code == ErrorCode::InvalidArgument);

    const auto first = hooks_a->Arm(HookPoint::BeforeWrite, HookAction::Pause, Clock::now() + 5s);
    CHECK(first);
    const auto g1 = first.value();
    auto state = HookState(hooks_a, HookPoint::BeforeWrite);
    CHECK(state.generation == g1 && state.armed && state.action == HookAction::Pause);
    const auto second = hooks_a->Arm(HookPoint::BeforeWrite, HookAction::ThrowOnce, Clock::now() + 5s);
    CHECK(second && second.value() > g1);
    CHECK(hooks_a->Clear(HookPoint::BeforeWrite, g1));
    state = HookState(hooks_a, HookPoint::BeforeWrite);
    CHECK(state.generation == second.value() && state.armed && state.action == HookAction::ThrowOnce);
    CHECK(hooks_a->Clear(HookPoint::BeforeWrite, second.value()));
    CHECK(hooks_a->Clear(HookPoint::BeforeWrite, second.value()));
    CHECK(!HookState(hooks_a, HookPoint::BeforeWrite).armed);
    CHECK(!HookState(hooks_b, HookPoint::BeforeWrite).armed);

    /**
     * 每个 server 型 Engine 占 2 个 driver 槽位，测试进程只有 4 个：建第三台
     * Engine（closed-owner）前必须释放只用于隔离断言的 b，否则其 Run 以
     * no_buffer_space 失败。hooks_b 的断言至此已全部完成。
     */
    hooks_b.reset();
    b.reset();

    std::shared_ptr<rpc::SessionHooks> closed;
    {
        auto owner = std::make_unique<AppHost>("closed-owner");
        closed = owner->client->Hooks();
        owner.reset();
    }
    CHECK(WaitUntil([&] { return HookState(closed, HookPoint::BeforeWrite).closed; }));
    const auto after_close = closed->Arm(HookPoint::BeforeWrite, HookAction::Pause, Clock::now() + 1s);
    CHECK(!after_close && after_close.error().code == ErrorCode::Closed);
    std::cout << "v2 hook state: owner isolation + generation + close contract ok\n";
}

/**
 * 21. OutboundBeforeHandshake 的 Pause 受真实 Call deadline 解除；业务期限
 *     不被注册窗口续租，in_flight 退出后再清理注册。
 */
void HookPauseDeadline()
{
    AppHost a("app-a"), b("app-b");
    auto hooks = a.client->Hooks();
    const auto armed = hooks->Arm(HookPoint::OutboundBeforeHandshake, HookAction::Pause, Clock::now() + 5s);
    CHECK(armed);
    auto pending = SubmitCo(
        [&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("pause-deadline", 1, "k"), UntilMs(250ms)); });
    CHECK(WaitUntil([&] {
        return HookState(hooks, HookPoint::OutboundBeforeHandshake).entered == 1
               && HookState(hooks, HookPoint::OutboundBeforeHandshake).in_flight == 1;
    }));
    const auto result = Get(pending);
    CHECK(!result && result.error().code == ErrorCode::TimedOut);
    CHECK(WaitUntil([&] { return HookState(hooks, HookPoint::OutboundBeforeHandshake).in_flight == 0; }));
    CHECK(hooks->Clear(HookPoint::OutboundBeforeHandshake, armed.value()));
    std::cout << "v2 hook pause deadline: real outbound checkpoint timed out and settled\n";
}

/**
 * 22. 同一真实出站握手中分别覆盖协程取消与 owner Close，二者都取消 Pause
 *     并回收 in_flight；没有通过 sleep 或假函数推进阶段。
 */
void HookPauseCancelClose()
{
    {
        AppHost a("cancel-a"), b("cancel-b");
        auto hooks = a.client->Hooks();
        const auto armed = hooks->Arm(HookPoint::OutboundBeforeHandshake, HookAction::Pause, Clock::now() + 5s);
        CHECK(armed);
        std::promise<bbt::coroutine::detail::Coroutine*> promise;
        auto current = promise.get_future();
        auto pending = SubmitCo([&] {
            promise.set_value(g_bbt_tls_coroutine_co);
            return bbt_co_await a.client->Call(b.Target(), MakeCall("pause-cancel", 1, "k"), UntilMs(5s));
        });
        auto* coroutine = Get(current);
        CHECK(coroutine != nullptr);
        CHECK(WaitUntil([&] {
            return HookState(hooks, HookPoint::OutboundBeforeHandshake).entered == 1
                   && HookState(hooks, HookPoint::OutboundBeforeHandshake).in_flight == 1;
        }));
        coroutine->RequestCancel();
        const auto result = Get(pending);
        CHECK(!result && result.error().code == ErrorCode::Cancelled);
        CHECK(WaitUntil([&] { return HookState(hooks, HookPoint::OutboundBeforeHandshake).in_flight == 0; }));
        CHECK(hooks->Clear(HookPoint::OutboundBeforeHandshake, armed.value()));
    }
    {
        auto a = std::make_unique<AppHost>("close-a");
        auto b = std::make_unique<AppHost>("close-b");
        auto hooks = a->client->Hooks();
        const auto armed = hooks->Arm(HookPoint::OutboundBeforeHandshake, HookAction::Pause, Clock::now() + 5s);
        CHECK(armed);
        auto pending = SubmitCo(
            [&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("pause-close", 1, "k"), UntilMs(5s)); });
        CHECK(WaitUntil([&] {
            return HookState(hooks, HookPoint::OutboundBeforeHandshake).entered == 1
                   && HookState(hooks, HookPoint::OutboundBeforeHandshake).in_flight == 1;
        }));

        /**
         * 宿主侧同步强制关闭：不等、不挂起，封口即取消出站握手 Pause。
         */
        (void)a->client->Close(bbt::infra::CloseOptions::Immediate());
        const auto result = Get(pending);
        CHECK(!result && result.error().code == ErrorCode::Closed);
        CHECK(WaitUntil([&] {
            return HookState(hooks, HookPoint::OutboundBeforeHandshake).in_flight == 0
                   && HookState(hooks, HookPoint::OutboundBeforeHandshake).closed;
        }));
    }
    std::cout << "v2 hook pause cancel/close: real outbound checkpoint settled\n";
}

/**
 * 23. 出站固定 ThrowOnce：establishing 记录、额度、done 与 conn 责任全归还，
 *     错误可见且与「等 deadline」可区分；随后同 target 真实重试成功。
 */
void HookOutboundThrow()
{
    rpc::ClientOptions one;
    one.SetMaxSessions(1);
    AppHost a("app-a", one), b("app-b");
    auto hooks = a.client->Hooks();
    const auto armed = hooks->Arm(HookPoint::OutboundBeforeHandshake, HookAction::ThrowOnce, Clock::now() + 5s);
    CHECK(armed);
    const auto failed =
        CallCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("boom", 1, "k"), UntilMs(5s)); });
    CHECK(!failed && failed.error().code == ErrorCode::InternalError);
    CHECK(failed.error().message.find("outbound_handshake") != std::string::npos);
    const auto state = HookState(hooks, HookPoint::OutboundBeforeHandshake);
    CHECK(state.entered == 1 && state.in_flight == 0 && !state.armed);
    CHECK(a.client->Stats().sessions == 0);
    CHECK(WaitUntil([&] { return a.engine->CountConnections() == 0; }, 3s));

    auto retried =
        SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("retry", 2, "k"), UntilMs(5s)); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }, 3s));
    b.ProcessAdmitted(Echo);
    CHECK(Get(retried));
    CHECK(a.client->Stats().sessions == 1);
    std::cout << "v2 outbound ThrowOnce: visible failure + quota retry ok\n";
}

/**
 * 24. BeforeWrite 固定 ThrowOnce：真实 ReplyCompletion 进入已出队 writer，
 *     提交前失败不得伪报成功，writes_outstanding 恰好归零且调用方被唤醒。
 */
void HookWriteThrow()
{
    AppHost a("app-a"), b("app-b");
    auto boot = SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    b.ProcessAdmitted(Echo);
    CHECK(Get(boot));

    auto held =
        SubmitCo([&] { return bbt_co_await a.client->Call(b.Target(), MakeCall("held", 2, "k"), UntilMs(5s)); });
    CHECK(WaitUntil([&] { return b.calls.load() == 2; }));
    rpc::Request request;
    std::shared_ptr<rpc::ReplyCompletion> reply;
    {
        std::lock_guard<std::mutex> lock(b.mu);
        CHECK(!b.admitted.empty());
        request = b.admitted.front().request;
        reply = b.admitted.front().reply;
        b.admitted.front().lease->Release();
        b.admitted.pop_front();
    }
    auto hooks = b.client->Hooks();
    const auto armed = hooks->Arm(HookPoint::BeforeWrite, HookAction::ThrowOnce, Clock::now() + 5s);
    CHECK(armed);
    auto done = SubmitCo([&] { return bbt_co_await reply->Complete(Echo(request)); });
    const auto result = Get(done);
    CHECK(!result && result.error().code == ErrorCode::InternalError);
    CHECK(HookState(hooks, HookPoint::BeforeWrite).entered == 1);
    CHECK(HookState(hooks, HookPoint::BeforeWrite).in_flight == 0);
    CHECK(!HookState(hooks, HookPoint::BeforeWrite).armed);
    CHECK(WaitUntil([&] { return rpc::detail::ServerV2LedgerForTest(*b.server).writes_outstanding == 0; }, 2s));
    const auto held_result = Get(held);

    /**
     * held 的请求帧已在注入前真实送达并被接纳（calls==2）：提交后 link 退役，
     * 调用方按 OutcomeUnknown 收场，不得伪报提交前错误。
     */
    CHECK(!held_result);
    CHECK(held_result.error().code == ErrorCode::OutcomeUnknown);
    CHECK(CloseServer(
        b.server,
        bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::milliseconds(3000))));
    std::cout << "v2 BeforeWrite ThrowOnce: dequeued write settled once\n";
}

/**
 * close-simplify 契约用例 a：Shutdown 是宿主侧同步强制关闭——不等待、不
 * 挂起，立即封口并发布默认原因；新调用按该原因拒绝，已提交在途调用立即
 * OutcomeUnknown。屏障：对端已接纳（b.calls==1）证明确实在途再触发。
 */
void ShutdownTimeoutStaysDraining()
{
    auto a = std::make_unique<AppHost>("app-a");
    auto b = std::make_unique<AppHost>("app-b");
    auto held =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("held", 1, "k"), UntilMs(5s)); });
    CHECK(WaitUntil([&] { return b->calls.load() == 1; }));

    (void)a->client->Close(bbt::infra::CloseOptions::Immediate());
    CHECK(a->client->IsClosed());

    /**
     * Shutdown 发布默认关闭原因（不是「无原因」）。
     */
    auto published = a->client->CloseReason();
    CHECK(published.has_value() && published->code == ErrorCode::Closed);

    /**
     * 在途调用立即收终态，不等对端回复。
     */
    const auto held_result = Get(held);
    CHECK(!held_result);
    CHECK(held_result.error().code == ErrorCode::OutcomeUnknown);

    const auto rejected =
        CallCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("late", 2, "k"), UntilMs()); });
    CHECK(!rejected);
    CHECK(rejected.error().code == ErrorCode::Closed);

    /**
     * 已接纳项归还走真实写失败路径，不拖死关闭状态。
     */
    b->ProcessAdmitted(Echo);

    /**
     * 幂等且不覆盖首个原因。
     */
    (void)a->client->Close(bbt::infra::CloseOptions::Immediate(MakeError(ErrorCode::Unavailable, "later")));
    CHECK(CloseClient(a->client, bbt::infra::CloseOptions::Immediate()));
    auto again = a->client->CloseReason();
    CHECK(again.has_value() && again->code == ErrorCode::Closed);
    std::cout << "v2 shutdown sync seal + rejects new\n";
}

/**
 * close-simplify 契约用例 b：Graceful Close 等待中被 Shutdown 打断，返回
 * 首个关闭原因；已提交的在途调用保持 OutcomeUnknown 并带原因详情。屏障：
 * 对端已接纳（b.calls==1）而 handler 未归还，Graceful 挂起未发布原因。
 */
void ShutdownInterruptedByClose()
{
    auto a = std::make_unique<AppHost>("app-a");
    auto b = std::make_unique<AppHost>("app-b");
    auto held =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("held", 1, "k"), UntilMs(10s)); });
    CHECK(WaitUntil([&] { return b->calls.load() == 1; }, 3s));

    auto graceful = SubmitCo([&] {
        return bbt_co_await b->server->Close(
            bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::seconds(10)));
    });

    /**
     * 屏障：Graceful 已封口并发布首因，但仍在等已接纳责任归还。
     */
    CHECK(graceful.wait_for(200ms) != std::future_status::ready);
    auto first = b->server->CloseReason();
    CHECK(first.has_value() && first->code == ErrorCode::Closed);

    (void)b->server->Close(bbt::infra::CloseOptions::Immediate(MakeError(ErrorCode::Unavailable, "owner abort")));
    const auto closed = Get(graceful);
    CHECK(!closed);

    /**
     * 首因固定，升级不覆盖
     */
    CHECK(closed.error().code == ErrorCode::Closed);

    /**
     * 首个原因固定：二次 Close/Shutdown 不覆盖。
     */
    CHECK(CloseServer(b->server, bbt::infra::CloseOptions::Immediate(MakeError(ErrorCode::TimedOut, "later"))));
    (void)b->server->Close(bbt::infra::CloseOptions::Immediate(MakeError(ErrorCode::Unavailable, "shutdown later")));
    auto observed = b->server->CloseReason();
    CHECK(observed.has_value() && observed->code == ErrorCode::Closed);

    const auto held_result = Get(held);
    CHECK(!held_result);

    /**
     * 已提交且无可信终态：不得追改成已胜出的成功，也不伪造服务端关闭原因。
     */
    CHECK(held_result.error().code == ErrorCode::OutcomeUnknown);
    CHECK(held_result.error().request_phase == bbt::infra::RequestPhase::RequestCommitted);
    b->ProcessAdmitted(Echo);
    std::cout << "v2 graceful close interrupted by shutdown returns first reason\n";
}

/**
 * close-simplify 契约用例 c：Close 时在途调用按分类如实收场并统一附加
 * 关闭原因——已提交保持 OutcomeUnknown + request_phase，未提交保持
 * Closed；两种失败都带 close_reason/close_reason_code 详情。屏障：
 * held1 已由对端接纳（请求字节已写出=已提交），held2 停在 BeforeWrite
 * Pause（在途未提交）后才触发 Close。
 */
void CloseInflightReason()
{
    auto a = std::make_unique<AppHost>("app-a");
    auto b = std::make_unique<AppHost>("app-b");
    auto held1 =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("held1", 1, "k"), UntilMs(10s)); });

    /**
     * committed
     */
    CHECK(WaitUntil([&] { return b->calls.load() == 1; }));

    auto gate = std::make_shared<HookGate>();
    gate->Arm(*a, HookPoint::BeforeWrite);
    auto held2 =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("held2", 2, "k"), UntilMs(10s)); });
    CHECK(WaitUntil([&] { return gate->Entered(HookPoint::BeforeWrite) >= 1; }, 3s));

    CHECK(
        CloseClient(a->client, bbt::infra::CloseOptions::Immediate(MakeError(ErrorCode::Unavailable, "owner close"))));

    const auto r1 = Get(held1);
    CHECK(!r1);
    CHECK(r1.error().code == ErrorCode::OutcomeUnknown);
    CHECK(r1.error().request_phase == bbt::infra::RequestPhase::RequestCommitted);
    const auto r2 = Get(held2);
    CHECK(!r2);
    CHECK(r2.error().code == ErrorCode::Closed);
    const auto expected_code = std::to_string(static_cast<int>(ErrorCode::Unavailable));
    for (const auto* err : {&r1.error(), &r2.error()}) {
        bool has_reason = false, has_code = false;
        for (const auto& detail : err->details) {
            if (detail.first == "close_reason" && detail.second == "owner close") {
                has_reason = true;
            }
            if (detail.first == "close_reason_code" && detail.second == expected_code) {
                has_code = true;
            }
        }
        CHECK(has_reason);
        CHECK(has_code);
    }

    /**
     * 归还 b 侧已接纳项，避免测试自身留下未终结 handler。
     */
    b->ProcessAdmitted(Echo);
    std::cout << "v2 close inflight: committed keeps OutcomeUnknown, "
                 "uncommitted gets Closed, both carry reason\n";
}

/**
 * ---- 24. 客户端输入边界（V1 client_input 的通用断言迁移到 V2 入口） ---------
 * target 只接受 tcp://numeric-ip:port；Call 固定 Required、Send 固定 None；
 * 未设置/已过期 deadline 不发请求。全部拒绝发生在拨号前。
 */
void ClientInput()
{
    AppHost a("app-a");
    for (const char* target :
         {"127.0.0.1:9",
          "http://127.0.0.1:9",
          "tcp://127.0.0.1:9/rpc",
          "tcp://user@127.0.0.1:9",
          "tcp://127.0.0.1:0",
          "tcp://127.0.0.1:9 ",
          "tcp://"}) {
        const auto bad =
            CallCo([&] { return bbt_co_await a.client->Call(target, MakeCall("in-1", 1, "k"), UntilMs()); });
        ExpectCode(bad, ErrorCode::InvalidArgument);
    }

    /**
     * Call 收到 ReplyNone 请求。
     */
    {
        const auto bad = CallCo(
            [&] { return bbt_co_await a.client->Call("tcp://127.0.0.1:9", MakeSend("in-2", 2, "k"), UntilMs()); });
        ExpectCode(bad, ErrorCode::InvalidArgument);
    }

    /**
     * Call 不允许 System lane。
     */
    {
        const auto bad = CallCo([&] {
            return bbt_co_await a.client->Call(
                "tcp://127.0.0.1:9", MakeCall("in-3", 3, "k", rpc::TrafficClass::System), UntilMs());
        });
        ExpectCode(bad, ErrorCode::InvalidArgument);
    }

    /**
     * 未设置 deadline（默认已过期）→ 确定 TimedOut。
     */
    {
        const auto bad = CallCo(
            [&] { return bbt_co_await a.client->Call("tcp://127.0.0.1:9", MakeCall("in-4", 4, "k"), CallOptions{}); });
        ExpectCode(bad, ErrorCode::TimedOut);
    }

    /**
     * Send 收到 ReplyRequired 请求。
     */
    {
        const auto bad = CallCo(
            [&] { return bbt_co_await a.client->Send("tcp://127.0.0.1:9", MakeCall("in-5", 5, "k"), SendUntil()); });
        ExpectCode(bad, ErrorCode::InvalidArgument);
    }

    /**
     * Send 无 deadline。
     */
    {
        rpc::SendOptions no_deadline;
        const auto bad = CallCo(
            [&] { return bbt_co_await a.client->Send("tcp://127.0.0.1:9", MakeSend("in-6", 6, "k"), no_deadline); });
        ExpectCode(bad, ErrorCode::InvalidArgument);
    }

    /**
     * 拒绝全部发生在拨号前：没有 session/link/连接产生。
     */
    const auto stats = a.client->Stats();
    CHECK(stats.sessions == 0 && stats.links == 0);
    CHECK(a.engine->CountConnections() == 0);
    std::cout << "v2 client input: target/request/send boundaries rejected before dial\n";
}

/**
 * ---- 25. 已提交后协程取消（V1 cancel_post_commit 迁移） ---------------------
 * 对端已接纳（请求字节完整写出=已提交）后取消：唯一终态 Cancelled +
 * RequestCommitted；晚到回复不得翻转终态，link 不被取消破坏。
 */
void CancelPostCommit()
{
    AppHost a("app-a"), b("app-b");
    std::promise<bbt::coroutine::detail::Coroutine*> promise;
    auto current = promise.get_future();
    auto inflight = SubmitCo([&] {
        promise.set_value(g_bbt_tls_coroutine_co);
        return bbt_co_await a.client->Call(b.Target(), MakeCall("pc-1", 1, "k"), UntilMs(10s));
    });
    auto* coroutine = Get(current);
    CHECK(coroutine != nullptr);

    /**
     * committed 已证明
     */
    CHECK(WaitUntil([&] { return b.calls.load() == 1; }));
    coroutine->RequestCancel();
    const auto outcome = Get(inflight);
    CHECK(!outcome);
    CHECK(outcome.error().code == ErrorCode::Cancelled);
    CHECK(outcome.error().request_phase == bbt::infra::RequestPhase::RequestCommitted);

    /**
     * 晚到回复：终态已发布且唯一，放行后不得翻转。
     */
    b.ProcessAdmitted(Echo);
    CHECK(outcome.error().code == ErrorCode::Cancelled);
    CHECK(a.client->Stats().links == 1);
    std::cout << "v2 cancel post-commit: Cancelled + RequestCommitted, late reply dropped\n";
}

/**
 * ---- 26. 畸形回复（V1 malformed_reply 的通用断言迁移） ---------------------
 * (a) 可解码但关联不符 → ProtocolError + ReplyTerminal，link 保留；
 * (b) 帧层不可解码的回复 → 会话逻辑死亡，已提交调用 OutcomeUnknown +
 *     RequestCommitted，link 物理退役。对端行为由 raw 监听对端脚本化。
 */
void MalformedReply()
{
    {
        RawTcpListener listener("zz-raw");
        AppHost a("app-a");
        auto pending = SubmitCo(
            [&] { return bbt_co_await a.client->Call(listener.Target(), MakeCall("mr-1", 1, "k"), UntilMs(5s)); });
        listener.Accept();
        const auto epoch = listener.Handshake();
        const auto call = listener.ReadFrame();
        CHECK(call.kind == rpc::FrameKind::Call);
        CHECK(call.request_id == "mr-1");
        rpc::SessionFrame response;
        response.kind = rpc::FrameKind::Response;
        response.traffic = rpc::TrafficClass::Service;
        response.reply = rpc::ReplyMode::Required;
        response.session_epoch = epoch;
        response.request_id = call.request_id;
        response.service = call.service;

        /**
         * 关联字段被换
         */
        response.method = "spoofed";
        response.request_schema = call.request_schema;
        response.response_schema = call.response_schema;
        response.has_outcome = true;
        response.success = true;
        listener.SendFrame(response);
        const auto outcome = Get(pending);
        CHECK(!outcome);
        CHECK(outcome.error().code == ErrorCode::ProtocolError);
        CHECK(outcome.error().message == "rpc session: response correlation mismatch");
        CHECK(outcome.error().request_phase == bbt::infra::RequestPhase::ReplyTerminal);
        CHECK(a.client->Stats().links == 1);
    }
    {
        RawTcpListener listener("zz-raw");
        AppHost a("app-a");
        auto pending = SubmitCo(
            [&] { return bbt_co_await a.client->Call(listener.Target(), MakeCall("mr-2", 2, "k"), UntilMs(5s)); });
        listener.Accept();
        (void)listener.Handshake();
        const auto call = listener.ReadFrame();
        CHECK(call.request_id == "mr-2");

        /**
         * 非法 wire type（7）的 protobuf 字节：帧层必拒。
         */
        const std::string garbage("\x0f\x0f\x0f\x0f", 4);
        std::uint8_t prefix[4];
        rpc::EncodeFramePrefix(static_cast<std::uint32_t>(garbage.size()), prefix);
        listener.SendRaw(std::string(reinterpret_cast<const char*>(prefix), sizeof(prefix)) + garbage);
        const auto outcome = Get(pending);
        CHECK(!outcome);
        CHECK(outcome.error().code == ErrorCode::OutcomeUnknown);
        CHECK(outcome.error().request_phase == bbt::infra::RequestPhase::RequestCommitted);

        /**
         * link 的额度归还由下一次取会话或 Close(Timeout) 重驱；这里断言物理
         * 连接已退役（fd/engine 责任归零），不以懒驱动计数冒充资源状态。
         */
        CHECK(WaitUntil([&] { return a.engine->CountConnections() == 0; }, 3s));
    }
    std::cout << "v2 malformed reply: correlation mismatch=ProtocolError/ReplyTerminal, "
                 "undecodable frame=OutcomeUnknown/RequestCommitted\n";
}

/**
 * Graceful 到期只返回 TimedOut 并保留已接纳责任；到期后再 Close(Graceful)
 * 仍可完成同一份排空，不自动升级。
 */
void CloseGracefulTimeout()
{
    auto a = std::make_unique<AppHost>("app-a");
    auto b = std::make_unique<AppHost>("app-b");
    auto boot =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("boot", 1, "k"), UntilMs()); });
    CHECK(WaitUntil([&] { return b->calls.load() == 1; }));
    b->ProcessAdmitted(Echo);
    CHECK(Get(boot));

    auto held =
        SubmitCo([&] { return bbt_co_await a->client->Call(b->Target(), MakeCall("held", 2, "k"), UntilMs(5s)); });
    CHECK(WaitUntil([&] { return b->calls.load() == 2; }));

    auto graceful = SubmitCo([&] {
        return bbt_co_await b->server->Close(
            bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::milliseconds(50)));
    });
    auto timed = Get(graceful);
    CHECK(!timed && timed.error().code == ErrorCode::TimedOut);

    /**
     * 优雅排空期间已逻辑封口
     */
    CHECK(b->server->IsClosed());
    CHECK(b->server->CloseReason().has_value());

    b->ProcessAdmitted(Echo);
    CHECK(Get(held));
    CHECK(CloseServer(
        b->server, bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::seconds(2))));
    CHECK(CloseTimeoutFor(b->server, 3000));
    CHECK(WaitUntil([&] { return b->engine->CountConnections() == 0; }, 3s));
    std::cout << "v2 close graceful timeout retains responsibility then drains\n";
}

/**
 * 已授权 handler 任务内的 Close/WaitStopped 是自等待：副作用前 InvalidContext。
 */
void CloseSelfWait()
{
    AppHost client("self-client"), server("self-server");
    auto* server_ptr = server.server.get();
    std::atomic<bool> entered{false};
    std::atomic<bool> close_rejected{false};
    std::atomic<bool> wait_rejected{false};
    rpc::CoRpcServer::RpcHandler handler = [&](rpc::Request request) -> bbt_co_async(result<rpc::Response>) {
        entered.store(true);
        auto closed = bbt_co_await server_ptr->Close(
            bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::milliseconds(200)));
        close_rejected.store(!closed && closed.error().code == ErrorCode::InvalidContext);
        auto waited = bbt_co_await server_ptr->Close(
            bbt::infra::CloseOptions{
                bbt::infra::CloseMode::Immediate,
                true,
                {},
                std::chrono::steady_clock::now() + std::chrono::milliseconds(200)});
        wait_rejected.store(!waited && waited.error().code == ErrorCode::InvalidContext);
        return result<rpc::Response>::ok(Echo(request));
    };
    CHECK(server.server->RegisterRpc("svc/M", std::move(handler)));

    auto reply =
        CallCo([&] { return bbt_co_await client.client->Call(server.Target(), MakeCall("self", 1, "k"), UntilMs()); });
    CHECK(reply);
    CHECK(entered.load());
    CHECK(close_rejected.load());
    CHECK(wait_rejected.load());
    CHECK(!server.server->IsClosed());
    std::cout << "v2 close self-wait rejected from admitted handler\n";
}

/**
 * Co 域起服：受管协程内直接调用 Server::Start（无 native 桥），随后用真实
 * loopback Call 证明 accept/admission/路由已起服，再统一关闭；重复 Start
 * 幂等，封口后 Start 返回 Closed。
 */
void CoStartRoundtrip()
{
    AppHost server("co-start-server", {}, {}, StartMode::Co);
    AppHost client("co-start-client");
    rpc::CoRpcServer::RpcHandler handler = [](rpc::Request request) -> bbt_co_async(result<rpc::Response>) {
        return result<rpc::Response>::ok(Echo(request));
    };
    CHECK(server.server->RegisterRpc("svc/M", std::move(handler)));

    const auto reply = CallCo(
        [&] { return bbt_co_await client.client->Call(server.Target(), MakeCall("co-start", 1, "k"), UntilMs()); });
    CHECK(reply);
    CHECK(reply.value().RequestId() == "co-start");

    /**
     * 顺序重复 Start 幂等：不重复登记 accept/admission 任务。
     */
    CHECK(CallCo([&] { return server.server->Start(); }));

    CHECK(CloseServer(server.server, bbt::infra::CloseOptions::Immediate()));
    CHECK(CloseTimeoutFor(server.server, 3000));
    CHECK(WaitUntil([&] { return server.engine->CountConnections() == 0; }, 3s));

    const auto late = CallCo([&] { return server.server->Start(); });
    CHECK(!late && late.error().code == ErrorCode::Closed);
    std::cout << "v2 co-domain start: loopback ok; repeat start idempotent; start after close closed\n";
}

/**
 * Start/Close 竞争：受管协程 Start 停在「已认领启动权、尚未登记任务」的
 * 无锁窗口时，native RequestClose 必须立即完成；WaitStopped 在启动责任
 * 未落账期间不得虚假完成；注册窗口内被 Close 的 Start 返回 Closed。
 */
void CoStartCloseRace()
{
    AppHost server("co-race-server", {}, {}, StartMode::Deferred);
    std::promise<void> entered;
    std::promise<void> release;
    auto entered_future = entered.get_future();
    auto release_future = release.get_future();
    rpc::detail::ServerV2TestAccess::SetStartRegistrationBarrierOnce([&] {
        entered.set_value();
        release_future.wait();
    });

    auto start = SubmitCo([&] { return server.server->Start(); });
    CHECK(entered_future.wait_for(3s) == std::future_status::ready);

    /**
     * 若 Start 持有 v2->mu，下面的 RequestClose 只能等 Start 走完。
     */
    auto closer =
        std::async(std::launch::async, [&] { (void)server.server->Close(bbt::infra::CloseOptions::Immediate()); });
    const bool closed_while_registering = closer.wait_for(2s) == std::future_status::ready;

    /**
     * 启动责任未落账（starting）时不得按「无 accept 任务」判定已收口。
     */
    const auto early = CallCo([&] {
        return bbt_co_await server.server->Close(
            bbt::infra::CloseOptions{
                bbt::infra::CloseMode::Immediate, true, {}, Clock::now() + std::chrono::milliseconds(150)});
    });
    CHECK(!early && early.error().code == ErrorCode::TimedOut);

    release.set_value();
    closer.get();
    CHECK(closed_while_registering);

    const auto started = Get(start);
    CHECK(!started && started.error().code == ErrorCode::Closed);
    CHECK(CloseServer(server.server, bbt::infra::CloseOptions::Immediate()));
    CHECK(CloseTimeoutFor(server.server, 3000));
    CHECK(WaitUntil([&] { return server.engine->CountConnections() == 0; }, 1s));
    std::cout << "v2 co start/close race: close not blocked; start closed; physical stop gated\n";
}

/**
 * 部分启动失败：accept 任务登记失败后启动权与 admission queue 全部回滚，
 * 不发布封口/started；重试成功且真实 loopback 可用（只注册一个 accept 任务）。
 */
void CoStartRegistrationFailureRollback()
{
    AppHost server("co-rollback-server", {}, {}, StartMode::Deferred);
    rpc::detail::ServerV2TestAccess::FailNextStartRegistration();
    const auto failed = CallCo([&] { return server.server->Start(); });
    CHECK(!failed && failed.error().code == ErrorCode::RuntimeUnavailable);
    CHECK(!server.server->IsClosed());

    AppHost client("co-rollback-client");
    rpc::CoRpcServer::RpcHandler handler = [](rpc::Request request) -> bbt_co_async(result<rpc::Response>) {
        return result<rpc::Response>::ok(Echo(request));
    };
    CHECK(server.server->RegisterRpc("svc/M", std::move(handler)));
    const auto retried = CallCo([&] { return server.server->Start(); });
    CHECK(retried);
    auto local = server.server->LocalAddress();
    CHECK(local);
    server.address = local.value();

    const auto reply = CallCo(
        [&] { return bbt_co_await client.client->Call(server.Target(), MakeCall("rollback", 1, "k"), UntilMs()); });
    CHECK(reply);
    CHECK(reply.value().RequestId() == "rollback");

    CHECK(CloseServer(server.server, bbt::infra::CloseOptions::Immediate()));
    CHECK(CloseTimeoutFor(server.server, 3000));
    CHECK(WaitUntil([&] { return server.engine->CountConnections() == 0; }, 3s));
    std::cout << "v2 co start registration failure: rolled back; retry serves loopback\n";
}

// clang-format off
}
// clang-format on

int RunCase(const std::string& mode)
{
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 256 * 1024;
    g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
    g_scheduler->Start();
    if (mode == "v2s_roundtrip") {
        Roundtrip();
    } else if (mode == "v2s_registered_route") {
        RegisteredRoute();
    } else if (mode == "v2s_bidirectional") {
        Bidirectional();
    } else if (mode == "v2s_send_acceptance") {
        SendAcceptance();
    } else if (mode == "v2s_ordering") {
        Ordering();
    } else if (mode == "v2s_ordering_ledger") {
        OrderingLedger();
    } else if (mode == "v2s_close_negative_timeout") {
        CloseNegativeTimeout();
    } else if (mode == "v2s_server_cross_engine") {
        ServerCrossEngine();
    } else if (mode == "v2s_overload") {
        Overload();
    } else if (mode == "v2s_lane_fairness") {
        LaneFairness();
    } else if (mode == "v2s_disconnect") {
        Disconnect();
    } else if (mode == "v2s_deadline") {
        Deadline();
    } else if (mode == "v2s_handshake_raw") {
        HandshakeRaw();
    } else if (mode == "v2s_handshake_coalesced") {
        HandshakeCoalesced();
    } else if (mode == "v2s_handshake_coalesced_big") {
        HandshakeCoalescedBig();
    } else if (mode == "v2s_handshake_coalesced_big_pinned") {
        HandshakeCoalescedBigPinned();
    } else if (mode == "v2s_handshake_stash_tail") {
        HandshakeStashTailPreserved();
    } else if (mode == "v2s_handshake_prefix_reject") {
        HandshakePrefixReject();
    } else if (mode == "v2s_close_drain") {
        CloseDrain();
    } else if (mode == "v2s_close_reason") {
        CloseReason();
    } else if (mode == "v2s_close_reason_pending") {
        CloseReasonPending();
    } else if (mode == "v2s_close_graceful_timeout") {
        CloseGracefulTimeout();
    } else if (mode == "v2s_close_self_wait") {
        CloseSelfWait();
    } else if (mode == "v2s_co_start_roundtrip") {
        CoStartRoundtrip();
    } else if (mode == "v2s_co_start_close_race") {
        CoStartCloseRace();
    } else if (mode == "v2s_co_start_registration_rollback") {
        CoStartRegistrationFailureRollback();
    } else if (mode == "v2s_admission_inflight") {
        AdmissionInflight();
    } else if (mode == "v2s_admission_reject") {
        AdmissionReject();
    } else if (mode == "v2s_admission_bytes") {
        AdmissionBytes();
    } else if (mode == "v2s_admission_sealed") {
        AdmissionSealed();
    } else if (mode == "v2s_reply_write_deferred") {
        ReplyWriteDeferred();
    } else if (mode == "v2s_reply_write_fail_close") {
        ReplyWriteFailClose();
    } else if (mode == "write_terminal_once") {
        WriteTerminalOnce();
    } else if (mode == "v2s_send_no_acceptance_drain") {
        SendWithoutAcceptanceDrains();
    } else if (mode == "v2s_reply_write_budget") {
        ReplyWriteBudget();
    } else if (mode == "v2s_reply_corr_mismatch") {
        ReplyCorrelationMismatch();
    } else if (mode == "v2s_close_released") {
        CloseReleased();
    } else if (mode == "v2s_close_handshake") {
        CloseHandshake();
    } else if (mode == "v2s_close_no_publish") {
        CloseSealedNoPublish();
    } else if (mode == "v2s_quota_peers") {
        QuotaPeers();
    } else if (mode == "v2s_simultaneous_dial") {
        SimultaneousDial();
    } else if (mode == "v2s_epoch_reconnect") {
        EpochReconnect();
    } else if (mode == "v2s_quota_establish_dial") {
        QuotaEstablishDial();
    } else if (mode == "v2s_quota_release_retry") {
        QuotaReleaseRetry();
    } else if (mode == "v2s_never_started_drain_close") {
        NeverStartedDrainClose();
    } else if (mode == "v2s_queue_full_drain_stats") {
        QueueFullDrainStatsConcurrent();
    } else if (mode == "v2s_hook_state") {
        HookStateContract();
    } else if (mode == "v2s_hook_pause_deadline") {
        HookPauseDeadline();
    } else if (mode == "v2s_hook_pause_cancel_close") {
        HookPauseCancelClose();
    } else if (mode == "v2s_hook_outbound_throw") {
        HookOutboundThrow();
    } else if (mode == "v2s_hook_write_throw") {
        HookWriteThrow();
    } else if (mode == "v2s_shutdown_timeout_draining") {
        ShutdownTimeoutStaysDraining();
    } else if (mode == "v2s_shutdown_interrupted_by_close") {
        ShutdownInterruptedByClose();
    } else if (mode == "v2s_close_inflight_reason") {
        CloseInflightReason();
    } else if (mode == "v2s_client_input") {
        ClientInput();
    } else if (mode == "v2s_cancel_post_commit") {
        CancelPostCommit();
    } else if (mode == "v2s_malformed_reply") {
        MalformedReply();
    } else {
        CHECK(false);
    }
    std::cout << "rpc v2 session " << mode << " PASS\n";
    return 0;
}

// clang-format off
}
// clang-format on

#include "../net/ProcessRuntimeTestMain.hpp"

int main(int argc, char** argv)
{
    return bbt_net_test::RunTestProcessExit([&] {
        rpc_net_test::Watchdog watchdog;
        try {
            CHECK(argc == 2);
            return rpc_v2_session_test::RunCase(argv[1]);
        } catch (const std::exception& error) {
            std::cerr << "FAIL " << error.what() << '\n';
            return 1;
        }
    });
}
