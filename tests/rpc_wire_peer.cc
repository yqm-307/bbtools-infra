// Issue #8：RPC wire 双进程测试的独立 peer 二进制。
// 不经 Boost.Test main；argv 分流：
//   <peer> server <port_file>
//   <peer> client <port>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <thread>

#include <unistd.h>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/detail/Hook.hpp>
#include <bbt/coroutine/io/IoExecutor.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/RpcWire.hpp>

using namespace bbt::infra;
using namespace bbt::infra::rpc;
using bbt::coroutine::Deadline;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

constexpr std::size_t kTestMaxBody   = 64 * 1024;
constexpr int         kBudgetMs      = 15000;

std::shared_ptr<NetworkRuntime> g_runtime;
std::shared_ptr<HttpClient>     g_client;
std::shared_ptr<HttpServer>     g_server;

template <class F>
bool RunInCoroutine(F&& f, int budget_ms = kBudgetMs) {
    bbt::core::thread::CountDownLatch done{1};
    bool succ = false;
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [fn = std::forward<F>(f), &done]() mutable {
            fn();
            done.Down();
        },
        succ);
    if (!succ)
        return false;
    return done.WaitTimeout(budget_ms) == 0;
}

result<HttpResponse> CallApi(HttpRequest req, CallOptions options) {
    std::optional<result<HttpResponse>> out;
    bool ran = RunInCoroutine(
        [&] { out.emplace(g_client->Request(std::move(req), options)); });
    if (!ran)
        return result<HttpResponse>::err(
            MakeError(ErrorCode::InternalError, "peer: coroutine did not run"));
    return std::move(*out);
}

CallOptions DefaultOptions(int budget_ms = 10000) {
    CallOptions opt;
    opt.deadline = std::chrono::steady_clock::now() +
                   std::chrono::milliseconds(budget_ms);
    return opt;
}

result<HttpResponse> RpcWireHandler(IncomingCallContext ctx, HttpRequest req) {
    (void)ctx;
    auto parsed = ParseRpcWireHttpRequest(req);
    if (!parsed) {
        RpcWireEnvelope err_env;
        err_env.request_id = "unknown";
        err_env.service    = "unknown";
        err_env.method     = "unknown";
        err_env.success    = false;
        err_env.error      = parsed.error();
        auto http = MakeRpcWireHttpResponse(err_env);
        if (!http)
            return result<HttpResponse>::err(parsed.error());
        return result<HttpResponse>::ok(std::move(http.value()));
    }
    const auto& in = parsed.value();

    RpcWireEnvelope out;
    out.profile_version     = kRpcWireProfileVersion;
    out.request_id          = in.request_id;
    out.service             = in.service;
    out.method              = in.method;
    out.request_schema      = in.request_schema;
    out.response_schema     = in.response_schema;
    out.payload             = in.payload;
    out.metadata            = in.metadata;
    // 响应方向的 remaining_budget_ms：wire 契约要求非零（接收端 local-min
    // clamp 由上层 listener 决定），peer 作为后端应答取 server 本地预算。
    out.remaining_budget_ms = 1000;
    out.success             = true;
    auto http = MakeRpcWireHttpResponse(out);
    if (!http)
        return result<HttpResponse>::err(http.error());
    return result<HttpResponse>::ok(std::move(http.value()));
}

void CommonRuntimeSetup() {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 2;
    g_scheduler->Start(SCHE_START_OPT_SCHE_THREAD);

    NetworkLimits limits{};
    limits.max_connections = 16;
    limits.max_inflight    = 16;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = kTestMaxBody;
    limits.incoming_timeout = std::chrono::milliseconds{5000};

    auto rt = NetworkRuntime::Create(limits);
    if (rt) {
        g_runtime = std::move(rt).value();
        g_runtime->Start();
    }
}

void CommonTeardown() {
    if (g_server) { g_server->RequestClose(); g_server.reset(); }
    if (g_client) { g_client.reset(); }
    if (g_runtime) { g_runtime->RequestClose(); g_runtime.reset(); }
    g_scheduler->Stop();
}

} // namespace

int main(int argc, char* argv[]) {
    if (argc >= 2 && std::strcmp(argv[1], "server") == 0) {
        if (argc < 3) return 90;
        CommonRuntimeSetup();
        if (!g_runtime) return 3;
        auto srv = g_runtime->ListenHttp({"127.0.0.1", 0}, RpcWireHandler);
        if (!srv) { CommonTeardown(); return 5; }
        g_server = std::move(srv).value();
        {
            std::ofstream f(argv[2], std::ios::trunc);
            f << g_server->LocalAddress().port;
        }
        char buf[8];
        while (::read(STDIN_FILENO, buf, sizeof(buf)) > 0) {}
        CommonTeardown();
        return 0;
    }
    if (argc >= 2 && std::strcmp(argv[1], "client") == 0) {
        if (argc < 3) return 91;
        const auto port = static_cast<std::uint16_t>(std::atoi(argv[2]));
        CommonRuntimeSetup();
        if (!g_runtime) return 3;
        auto cl = g_runtime->CreateHttpClient();
        if (!cl) { CommonTeardown(); return 5; }
        g_client = std::move(cl).value();

        RpcWireEnvelope env;
        env.request_id          = "dual-proc-req-1";
        env.service             = "svc.echo";
        env.method              = "Echo";
        env.remaining_budget_ms = 3000;
        env.request_schema      = "bbt.echo.EchoReq/v1";
        env.response_schema     = "bbt.echo.EchoResp/v1";
        env.payload             = {'p', 'i', 'n', 'g'};
        env.metadata            = {{"route.zone", "z1"}, {"trace.id", "t-9"}};
        env.success             = true;

        auto req = MakeRpcWireHttpRequest(
            env, "http://127.0.0.1:" + std::to_string(port));
        if (!req) { CommonTeardown(); return 6; }

        auto resp = CallApi(std::move(req.value()), DefaultOptions());
        if (!resp) { CommonTeardown(); return 7; }
        auto parsed = ParseRpcWireHttpResponse(resp.value());
        if (!parsed) { CommonTeardown(); return 8; }
        const auto& out = parsed.value();
        if (!out.success ||
            out.request_id != env.request_id ||
            out.service    != env.service ||
            out.method     != env.method ||
            out.payload    != env.payload ||
            out.metadata.size() != env.metadata.size()) {
            CommonTeardown();
            return 9;
        }
        CommonTeardown();
        return 0;
    }
    return 1;
}
