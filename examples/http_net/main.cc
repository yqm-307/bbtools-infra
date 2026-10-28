// 新 Client → 新 Server：应用装配两个 Engine，普通 handler/Get/Post 不接触 Run。
#include <bbt/infra/http/Server.hpp>
#include <bbt/coroutine/coroutine.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <iostream>
#include <stdexcept>
#include <thread>

namespace http = bbt::infra::http;
namespace net = bbt::infra::net;
using namespace std::chrono_literals;
namespace {
void Check(bool condition, const char* message) {
    if (!condition) { throw std::runtime_error(message); }
}
struct Runner {
    std::shared_ptr<net::Engine> engine;
    std::error_code error;
    std::thread thread;
    Runner(std::shared_ptr<net::Engine> value, net::EngineConfig config) : engine(std::move(value)) {
        thread = std::thread([this, config] { error = engine->Run(config); });
    }
    void Join() {
        engine->RequestClose();
        if (thread.joinable()) { thread.join(); }
    }
    ~Runner() { Join(); }
};
void Running(const std::shared_ptr<net::Engine>& engine) {
    const auto end = std::chrono::steady_clock::now() + 3s;
    while (engine->Phase() != net::EnginePhase::Running && std::chrono::steady_clock::now() < end) {
        std::this_thread::sleep_for(1ms);
    }
    Check(engine->Phase() == net::EnginePhase::Running, "Engine did not start");
}
int Example() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4; // server 2 driver 槽 + client 1 + 业务
    g_bbt_coroutine_config->m_cfg_stack_size = 128 * 1024;
    g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
    g_scheduler->Start();
    auto server_engine = std::make_shared<net::Engine>(std::shared_ptr<net::RawHandler>{});
    std::atomic<unsigned> peer_requests{0};
    auto made = http::Server::Create(server_engine,
        [&peer_requests](bbt::infra::IncomingCallContext ctx, http::IncomingRequest request) {
            Check(ctx.peer_principal.empty(), "unauthenticated peer is not a principal");
            Check(ctx.deadline > std::chrono::steady_clock::now(), "finite incoming deadline");
            ++peer_requests;
            http::Response response;
            response.status_code = request.target == "/missing" ? 404 : 200;
            response.body = request.method == "POST" ? request.body : "example-get";
            if (request.method == "POST") {
                Check(request.target == "/submit", "POST target");
                Check(request.body == "post-payload- bytes", "server observed real POST bytes");
            }
            return bbt::infra::result<http::Response>::ok(std::move(response));
        });
    Check(static_cast<bool>(made), "Server Create");
    auto server = std::move(made).value();
    Check(server_engine->Phase() == net::EnginePhase::Idle, "Create does not Run");
    net::EngineConfig config;
    config.runtime_progress = net::RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    std::error_code address_error;
    config.listen_addresses = {net::Address::Ip("127.0.0.1", 0, address_error)};
    Check(!address_error, "listen address");
    Runner server_run(server_engine, config);
    Running(server_engine);
    Check(static_cast<bool>(server->Start()), "Server Start");
    std::vector<net::Address> addresses;
    Check(!server_engine->LocalAddresses(addresses) && addresses.size() == 1, "bound address");
    auto client_engine = std::make_shared<net::Engine>(std::shared_ptr<net::RawHandler>{});
    config.listen_addresses.clear();
    Runner client_run(client_engine, config);
    Running(client_engine);
    http::ClientOptions options;
    options.base_url = "http://127.0.0.1:" + std::to_string(addresses.front().Port());
    auto created = http::Client::Create(client_engine, options);
    Check(static_cast<bool>(created), "Client Create");
    auto client = std::move(created).value();
    std::promise<void> done;
    auto future = done.get_future();
    std::atomic<unsigned> completed{0};
    bool registered = false;
    g_scheduler->RegistCoroutineTask(128 * 1024, [&] {
        try {
            const auto until = [] { return bbt::infra::CallOptions{std::chrono::steady_clock::now() + 3s}; };
            auto first = bbt_co_await client->Get("/items", {}, until());
            Check(first && first.value().status_code == 200 && first.value().body == "example-get", "first GET");
            auto second = bbt_co_await client->Get("/missing", {}, until());
            Check(second && second.value().status_code == 404 && second.value().body == "example-get", "second GET status");
            auto post = bbt_co_await client->Post("/submit", {"post-payload- bytes", "text/plain"}, {}, until());
            Check(post && post.value().body == "post-payload- bytes", "POST reply bytes");
            ++completed;
            done.set_value();
        } catch (...) { ++completed; done.set_exception(std::current_exception()); }
    }, registered);
    Check(registered, "consumer task registration");
    Check(future.wait_for(5s) == std::future_status::ready, "consumer completed");
    future.get();
    auto stats = server->Stats();
    Check(stats.accepted == 1 && stats.requests == 3 && peer_requests == 3, "same-connection reuse evidence");
    Check(client->Stats().idle == 1 && client->Stats().budget == 1, "client pool reuse");
    std::cout << "new Client -> new Server accepted=" << stats.accepted << " requests=" << stats.requests
              << " peer_requests=" << peer_requests << " GET=200,404 POST=echo reuse=1\n";
    server->StopAccepting();
    Check(static_cast<bool>(server->Drain(std::chrono::steady_clock::now() + 3s)), "Drain includes replies");
    server->Close();
    Check(static_cast<bool>(server->WaitStopped(std::chrono::steady_clock::now() + 3s)), "HTTP physical stop");
    stats = server->Stats();
    Check(stats.tasks == 0 && stats.sessions == 0 && stats.inflight == 0 && server_engine->AcceptingStopped(), "HTTP receipt");
    std::cout << "HTTP stopped tasks=0 sessions=0 inflight=0 listener_stopped=1 engines_still_running="
              << (server_engine->Phase() == net::EnginePhase::Running) << '\n';
    client->Close();
    client_run.Join();
    server_run.Join();
    Check(!client_run.error && !server_run.error, "Run errors");
    Check(server_engine->Phase() == net::EnginePhase::Stopped && client_engine->Phase() == net::EnginePhase::Stopped,
          "Engine phases");
    Check(server_engine->CountConnections() == 0 && client_engine->CountConnections() == 0 && completed == 1,
          "network and consumer tasks drained");
    std::cout << "network stopped server_run=returned client_run=returned joined=2 connections=0 consumer_tasks=1/1\n";
    return 0;
}
} // namespace
int main() {
    int result = 1;
    try { result = Example(); }
    catch (const std::exception& error) { std::cerr << "FAIL " << error.what() << '\n'; }
    std::cout.flush();
    std::cerr.flush();
    std::fflush(nullptr);
    std::_Exit(result); // runtime 进程寿命；不是 HTTP/network 回收证据
}
