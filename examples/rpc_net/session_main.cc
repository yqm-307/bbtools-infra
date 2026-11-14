/**
 * RPC v2 AppSession 双进程 consumer 示例（真实两 OS 进程 + 动态 loopback
 * 端口）：server 进程打印实际监听端口到 stdout；client 进程读端口并发起
 * Call/Send；两侧各一 Engine + Client(owner) + Server(同 owner)。
 * 用法：
 *   rpc_v2_session_example server        → stdout 第一行 "PORT <n>"，之后
 *                                          处理 admission 直到 stdin EOF/60s。
 *   rpc_v2_session_example client <port> → 拨号 + Call/Send，打印 PASS 行。
 * 本示例只绑定 127.0.0.1 动态端口，不连接生产/共享服务。
 */
#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/rpc/CoRpcClient.hpp>
#include <bbt/infra/rpc/CoRpcServer.hpp>
#include <bbt/infra/rpc/Options.hpp>
#include <bbt/infra/net/Engine.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <future>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

using namespace std::chrono_literals;
namespace rpc = bbt::infra::rpc;
namespace net = bbt::infra::net;
using bbt::infra::CallOptions;
using Clock = std::chrono::steady_clock;

namespace {

int Fail(const char* message)
{
    std::cerr << "FAIL " << message << '\n';
    return 1;
}

template <class F> auto SubmitCo(F fn) -> std::future<decltype(fn())>
{
    using T = decltype(fn());
    auto promise = std::make_shared<std::promise<T>>();
    auto future = promise->get_future();
    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        256 * 1024,
        [promise, callback = std::move(fn)]() mutable {
            try {
                promise->set_value(callback());
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

struct Assembly {
    std::shared_ptr<net::Engine> engine{net::Engine::Create(std::shared_ptr<net::RawHandler>{})};
    std::shared_ptr<rpc::CoRpcClient> client;
    std::shared_ptr<rpc::CoRpcServer> server;
    std::thread runner;
    std::error_code run_error;
    net::Address address;
    std::mutex mu;
    std::deque<rpc::AdmissionItem> admitted;
    std::uint64_t admitted_total = 0;

    bool Start(const std::string& identity, bool listen)
    {
        rpc::ClientOptions co;
        co.SetLocalIdentity(identity).SetTrustedNetworkPlaintext(true);
        auto made_client = rpc::CoRpcClient::Create(engine, co);
        if (!made_client) {
            return false;
        }
        client = std::move(made_client).value();

        rpc::ServerOptions so;
        std::error_code error;
        so.SetListenAddress(net::Address::Ip("127.0.0.1", 0, error));
        if (error) {
            return false;
        }
        auto made = rpc::CoRpcServer::Create(
            engine,
            client,
            [this](rpc::AdmissionItem&& item) -> bbt::infra::result<void> {
                std::lock_guard<std::mutex> lock(mu);
                admitted.push_back(std::move(item));
                return bbt::infra::result<void>::ok();
            },
            so);
        if (!made) {
            return false;
        }
        server = std::move(made).value();

        net::EngineConfig config;
        config.runtime_progress = net::RuntimeProgress::Independent;
        config.options.num_event_loop = 1;
        if (listen) {
            config.listen_addresses = {so.ListenAddress()};
        }
        runner = std::thread([this, config] { run_error = engine->Run(config); });
        const auto deadline = Clock::now() + 5s;
        while (engine->Phase() != net::EnginePhase::Running) {
            if (Clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(1ms);
        }
        if (!server->Start()) {
            return false;
        }
        if (listen) {
            auto local = server->LocalAddress();
            if (!local) {
                return false;
            }
            address = local.value();
        }
        return true;
    }

    ~Assembly()
    {
        if (server) {
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

    bool HasPendingAdmissions()
    {
        std::lock_guard<std::mutex> lock(mu);
        return !admitted.empty();
    }

    void PumpOnce()
    {
        std::deque<rpc::AdmissionItem> items;
        {
            std::lock_guard<std::mutex> lock(mu);
            items.swap(admitted);
        }
        admitted_total += items.size();
        for (auto& item : items) {
            if (item.reply && item.request.Reply() == rpc::ReplyMode::Required) {
                auto response = rpc::Response::FromRequest(item.request);
                response.SetPayload(item.request.Payload());

                /**
                 * Complete 是 bbt_co_async（仅受管协程可挂起等写终态）——
                 * native pump 线程不能直接调用（返回 InvalidContext 且不回
                 * 复）。把 Complete 提交到 scheduler 的受管协程执行，回复
                 * 是否到达由对端 Call 结果证明；不静默丢弃 InvalidContext。
                 */
                SubmitCo([reply = item.reply, response = std::move(response)]() mutable {
                    return bbt_co_await reply->Complete(std::move(response));
                });
            }
            if (item.lease) {
                item.lease->Release();
            }
        }
    }
};

int RunServer()
{
    Assembly app;
    if (!app.Start("session-server", true)) {
        return Fail("server start failed");
    }
    std::cout << "PORT " << app.address.Port() << '\n' << std::flush;

    /**
     * 协作退出：stdin watcher 线程阻塞读 stdin；driver 在 client 通过后
     * 关闭 stdin（EOF，或写一行 shutdown）→ 结束 pump 循环。60s 硬上限
     * 只是 driver 被强杀时的兜底，不属于正常路径，触发即判失败。
     */
    std::atomic<bool> stop{false};
    std::thread stdin_watcher([&stop] {
        std::string line;
        while (std::getline(std::cin, line)) {
            if (line.rfind("shutdown", 0) == 0) {
                break;
            }
        }
        stop.store(true);
    });
    const auto hard_end = Clock::now() + 60s;
    while (!stop.load() && Clock::now() < hard_end) {
        app.PumpOnce();
        std::this_thread::sleep_for(1ms);
    }
    stdin_watcher.join();
    const bool normal_stop = stop.load();

    /**
     * 正常资源收口：排空已准入项（完成在途回复）后走唯一关闭入口——
     * Close(Graceful) 等已接纳责任归还后封口；WaitStopped 有界确认物理收口；
     * 析构内 RequestClose 幂等。
     */
    const auto drain_deadline = Clock::now() + 3s;
    while (app.HasPendingAdmissions() && Clock::now() < drain_deadline) {
        app.PumpOnce();
        std::this_thread::sleep_for(1ms);
    }
    auto close_future = SubmitCo([&] {
        return bbt_co_await app.server->Close(
            bbt::infra::CloseOptions::Graceful(std::chrono::steady_clock::now() + std::chrono::milliseconds(3000)));
    });
    if (close_future.wait_for(5s) != std::future_status::ready) {
        return Fail("server graceful close hung");
    }
    if (!close_future.get()) {
        return Fail("server graceful close failed");
    }
    auto settle_future = SubmitCo([&] {
        auto closed = bbt_co_await app.server->Close(bbt::infra::CloseOptions::Immediate());
        if (!closed) {
            return closed;
        }
        return bbt_co_await app.server->Close(
            bbt::infra::CloseOptions{
                bbt::infra::CloseMode::Immediate,
                true,
                {},
                std::chrono::steady_clock::now() + std::chrono::milliseconds(3000)});
    });
    if (settle_future.wait_for(5s) != std::future_status::ready || !settle_future.get()) {
        return Fail("server close did not settle");
    }
    if (!app.server->IsClosed()) {
        return Fail("server not closed after close");
    }
    const auto server_stats = app.server->Stats();
    if (!normal_stop) {
        std::cerr << "FAIL server hit 60s backstop (driver lost?)\n";
        return 1;
    }
    std::cout << "SERVER CLOSED ok admitted=" << app.admitted_total << " accepted=" << server_stats.accepted_connections
              << " active=" << server_stats.active_connections << '\n'
              << std::flush;
    return 0;
}

int RunClient(std::uint16_t port)
{
    Assembly app;
    if (!app.Start("session-client", false)) {
        return Fail("client start failed");
    }
    const std::string target = "tcp://127.0.0.1:" + std::to_string(port);

    /**
     * Call：echo 回复。
     */
    auto call_future = SubmitCo([&] {
        rpc::Request request("svc", "Echo");
        request.SetRequestId("consumer-call")
            .SetReply(rpc::ReplyMode::Required)
            .SetTraffic(rpc::TrafficClass::Service)
            .SetOrderingKey("k")
            .SetSequence(1)
            .SetRequestSchema("req")
            .SetResponseSchema("resp");
        std::string payload = "consumer-payload";
        request.SetPayload({payload.begin(), payload.end()});
        CallOptions options;
        options.deadline = Clock::now() + 5s;
        return bbt_co_await app.client->Call(target, std::move(request), options);
    });

    /**
     * pump server admission from client side thread (server 进程已 pump)。
     */
    const auto reply = [&] {
        const auto status = call_future.wait_for(5s);
        if (status != std::future_status::ready) {
            return bbt::infra::result<rpc::Response>::err(
                bbt::infra::MakeError(bbt::infra::ErrorCode::TimedOut, "call wait timed out"));
        }
        return call_future.get();
    }();
    if (!reply) {
        return Fail("call failed");
    }
    if (reply.value().RequestId() != "consumer-call") {
        return Fail("call correlation mismatch");
    }

    /**
     * Send + RequireRemoteAcceptance。
     */
    auto send_future = SubmitCo([&] {
        rpc::Request message("svc", "Tell");
        message.SetMessageId("consumer-send")
            .SetReply(rpc::ReplyMode::None)
            .SetTraffic(rpc::TrafficClass::Service)
            .SetOrderingKey("k")
            .SetSequence(2)
            .SetRequestSchema("req")
            .SetResponseSchema("resp");
        rpc::SendOptions options;
        options.SetDeadline(Clock::now() + 5s);
        options.RequireRemoteAcceptance(true);
        return bbt_co_await app.client->Send(target, std::move(message), options);
    });
    const auto sent = [&] {
        const auto status = send_future.wait_for(5s);
        if (status != std::future_status::ready) {
            return bbt::infra::result<void>::err(
                bbt::infra::MakeError(bbt::infra::ErrorCode::TimedOut, "send wait timed out"));
        }
        return send_future.get();
    }();
    if (!sent) {
        return Fail("send acceptance failed");
    }

    const auto stats = app.client->Stats();
    if (stats.sessions != 1 || stats.links != 1) {
        return Fail("expected exactly one session and one link");
    }
    std::cout << "CONSUMER PASS sessions=" << stats.sessions << " links=" << stats.links << '\n' << std::flush;
    return 0;
}

// clang-format off
}
// clang-format on

int main(int argc, char** argv)
{
    if (argc < 2) {
        return Fail("usage: server|client <port>");
    }
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 256 * 1024;
    g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
    g_scheduler->Start();
    int code = 0;
    if (std::strcmp(argv[1], "server") == 0) {
        code = RunServer();
    } else if (std::strcmp(argv[1], "client") == 0 && argc == 3) {
        code = RunClient(static_cast<std::uint16_t>(std::stoi(argv[2])));
    } else {
        code = Fail("usage: server|client <port>");
    }

    /**
     * 与 tests 同一收口理由：跳过 runtime 静态析构竞态。
     */
    std::cout.flush();
    std::cerr.flush();
    std::_Exit(code);
}
