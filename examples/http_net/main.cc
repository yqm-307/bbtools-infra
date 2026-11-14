/**
 * Co-only 消费示例：bbtco_main 在 Engine Idle 阶段装配 Server 与 Client，
 * 发布共享 Engine 后由入口 native host hook 在主线程调用一次 Engine::Run；
 * 主任务用 Engine::CoWaitRunning 等 Running，Run 返回即本代网络资源收口的
 * 唯一凭据。业务侧不建线程、不 join、不等待 future。
 */
#include <bbt/infra/http/Client.hpp>
#include <bbt/infra/http/Server.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/coroutine/coroutine.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <iostream>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

namespace http = bbt::infra::http;
namespace net = bbt::infra::net;
using bbt::infra::IncomingCallContext;
using namespace std::chrono_literals;

namespace {
void Check(bool condition, const char* message)
{
    if (!condition) {
        throw std::runtime_error(message);
    }
}

/**
 * 入口宿主闸门：主任务在 Idle 装配完成后发布共享 Engine 与 Run 配置；host hook
 * 在 native 主线程等待发布后调用一次 Run。runtime 的 OnMainTaskDone 回调在主任务
 * 结束时 RequestClose，Run 的控制循环因此封口返回。
 */
struct HostGate {
    std::mutex mutex;
    std::condition_variable cv;
    bool published{false};
    bool main_done{false};
    std::shared_ptr<net::Engine> engine;
    net::EngineConfig config;
};

HostGate g_gate;

/**
 * Start 前冻结 runtime 配置；bbtco_main 与所有业务任务都运行在受管协程域。
 */
void setup_runtime()
{

    /**
     * server 2 driver 槽 + 业务与回调
     */
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 128 * 1024;
    g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
}

bbt::infra::result<http::Response>
Handle(IncomingCallContext ctx, http::IncomingRequest request, std::atomic<unsigned>& peer_requests)
{
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
}

bbt::coroutine::Deadline Soon()
{
    return std::chrono::steady_clock::now() + 3s;
}

// clang-format off
}
// clang-format on

int bbtco_main()
{

    /**
     * 已准入的 handler 可能晚于主任务退栈：计数器的所有权放在堆上，由 handler
     * 闭包按值持有，Server 保存的 handler 就是最后一个 owner；不借用主协程栈。
     */
    auto peer_requests = std::make_shared<std::atomic<unsigned>>(0u);
    auto engine = net::Engine::Create(std::shared_ptr<net::RawHandler>{});
    auto made = http::Server::Create(engine, [peer_requests](IncomingCallContext ctx, http::IncomingRequest request) {
        return Handle(std::move(ctx), std::move(request), *peer_requests);
    });
    Check(static_cast<bool>(made), "Server Create");
    auto server = std::move(made).value();
    Check(engine->Phase() == net::EnginePhase::Idle, "Create does not Run");

    net::EngineConfig config;
    config.runtime_progress = net::RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    std::error_code address_error;
    config.listen_addresses = {net::Address::Ip("127.0.0.1", 0, address_error)};
    Check(!address_error, "listen address");

    /**
     * 发布即表示 Idle 阶段装配完成；host hook 从此刻起可以 Run。
     */
    {
        std::lock_guard<std::mutex> lock(g_gate.mutex);
        g_gate.engine = engine;
        g_gate.config = config;
        g_gate.published = true;
        g_gate.cv.notify_all();
    }

    auto server_ready = engine->CoWaitRunning({Soon()});
    Check(static_cast<bool>(server_ready), "server Engine ready");
    Check(static_cast<bool>(server->Start()), "Server Start");
    std::vector<net::Address> addresses;
    Check(!engine->LocalAddresses(addresses) && addresses.size() == 1, "bound address");

    http::ClientOptions options;
    options.base_url = "http://127.0.0.1:" + std::to_string(addresses.front().Port());
    auto created = http::Client::Create(engine, options);
    Check(static_cast<bool>(created), "Client Create");
    auto client = std::move(created).value();

    auto first = bbt_co_await client->Get("/items", {}, {Soon()});
    Check(first && first.value().status_code == 200 && first.value().body == "example-get", "first GET");
    auto second = bbt_co_await client->Get("/missing", {}, {Soon()});
    Check(second && second.value().status_code == 404 && second.value().body == "example-get", "second GET status");
    auto post = bbt_co_await client->Post("/submit", {"post-payload- bytes", "text/plain"}, {}, {Soon()});
    Check(post && post.value().body == "post-payload- bytes", "POST reply bytes");

    auto stats = server->Stats();
    Check(stats.accepted == 1 && stats.requests == 3 && peer_requests->load() == 3, "same-connection reuse evidence");
    Check(client->Stats().idle == 1 && client->Stats().budget == 1, "client pool reuse");
    std::cout << "new Client -> new Server accepted=" << stats.accepted << " requests=" << stats.requests
              << " peer_requests=" << peer_requests->load() << " GET=200,404 POST=echo reuse=1\n";

    Check(static_cast<bool>(client->Close(bbt::infra::CloseOptions::Graceful(Soon()))), "client close");
    server->StopAccepting();
    Check(static_cast<bool>(server->Close(bbt::infra::CloseOptions::Graceful(Soon()))), "Drain includes replies");
    (void)server->Close(bbt::infra::CloseOptions::Immediate());
    Check(
        static_cast<bool>(server->Close(bbt::infra::CloseOptions{bbt::infra::CloseMode::Immediate, true, {}, Soon()})),
        "HTTP physical stop");
    stats = server->Stats();
    Check(stats.tasks == 0 && stats.sessions == 0 && stats.inflight == 0 && engine->AcceptingStopped(), "HTTP receipt");
    std::cout << "HTTP stopped tasks=0 sessions=0 inflight=0 listener_stopped=1\n";

    /**
     * 业务责任已收口；主任务返回后由 host hook 的 RequestClose 让 Run 返回，
     * Engine 本代网络资源的物理收口由该返回证明。
     */
    return 0;
}

int bbtco_main_host_hook(bbt::coroutine::HostHookContext& ctx)
{

    /**
     * 主任务结束（正常/异常）恰好一次：RequestClose 只封口并唤醒控制域，
     * Run 返回才是收口凭据。
     */
    ctx.OnMainTaskDone([] {
        std::shared_ptr<net::Engine> engine;
        {
            std::lock_guard<std::mutex> lock(g_gate.mutex);
            g_gate.main_done = true;
            engine = g_gate.engine;
            g_gate.cv.notify_all();
        }
        if (engine) {
            (void)engine->Close();
        }
    });

    std::shared_ptr<net::Engine> engine;
    net::EngineConfig config;
    {
        std::unique_lock<std::mutex> lock(g_gate.mutex);
        if (!g_gate.cv.wait_for(lock, 10s, [] { return g_gate.published || g_gate.main_done; })) {
            std::cerr << "host hook: main task did not publish the shared Engine\n";
            return 1;
        }

        /**
         * 主任务在 Run 之前已失败/返回：没有需要托管的 Engine，等其退出码生效。
         */
        if (!g_gate.published || g_gate.main_done) {
            return 0;
        }
        engine = g_gate.engine;
        config = g_gate.config;
    }

    const auto run_error = engine->Run(config);
    if (run_error) {
        std::cerr << "Engine::Run failed: " << run_error.message() << '\n';
        return 1;
    }
    if (engine->Phase() != net::EnginePhase::Stopped || engine->CountConnections() != 0) {
        std::cerr << "host hook: Engine did not reach a clean Stopped phase\n";
        return 1;
    }
    std::cout << "network stopped engine_run_returned=1 phase=Stopped connections=0\n";
    return 0;
}

BBTCO_MAIN_WITH_SETUP_AND_HOST_HOOK()
