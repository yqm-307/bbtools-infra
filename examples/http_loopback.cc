// Issue #27：最小真实消费示例（无外部 Redis/Mongo 依赖，可独立编译运行）。
//
// 演示 bbtools-infra 当前公开面（co-network/v1 N1b-1）的完整生命周期：
//
//   Scheduler::Start → NetworkRuntime::Create/Start → ListenHttp/CreateHttpClient
//   → 协程内 HttpClient::Request（deadline 预算）→ server.StopAccepting
//   → runtime.RequestClose/WaitClosed → 释放网络对象 → Scheduler::Stop
//
// 全部调用基于 bbtools-coroutine 自有模型（C++17，无 co_await/Task<T>）：
// 挂起/恢复由 Scheduler 驱动，等待型 API（Request/WaitClosed）只在协程
// 上下文调用，返回 bbt::infra::result<T>，不用异常传错误。
//
// 运行：./Example_http_loopback，期望打印一次 loopback 响应并以 0 退出。

#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <utility>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

using namespace bbt::infra;

namespace {

// handler 在 infra 提供的受管协程中被调用；ctx 携带 listener 本地预算与
// 取消令牌，本示例只回显，不消费 ctx。
result<HttpResponse> EchoHandler(IncomingCallContext /*ctx*/, HttpRequest req) {
    HttpResponse resp;
    resp.status = 200;
    resp.body   = "echo:" + req.body;
    return result<HttpResponse>::ok(std::move(resp));
}

// 在协程内执行 fn 并限时等待完成；任何一步失败返回 false（示例不得挂死）。
template <class F>
bool RunInCoroutine(F&& fn, int budget_ms = 10000) {
    bbt::core::thread::CountDownLatch done{1};
    bool succ = false;
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [fn = std::forward<F>(fn), &done]() mutable {
            fn();
            done.Down();
        },
        succ);
    if (!succ)
        return false;
    return done.WaitTimeout(budget_ms) == 0;
}

} // namespace

int main() {
    // 纤程栈给到 256KB，避免深调用链撞默认 12KB 栈的保护页（与单测一致）。
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 2;
    cfg->m_cfg_stack_size        = 1024 * 256;

    auto* scheduler = bbt::coroutine::detail::Scheduler::GetInstance().get();
    scheduler->Start(bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);

    NetworkLimits limits{};
    limits.max_connections  = 64;
    limits.max_inflight     = 64;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = 64 * 1024;
    limits.incoming_timeout = std::chrono::milliseconds{5000};

    int exit_code = 0;

    auto rt = NetworkRuntime::Create(limits);
    if (!rt) {
        std::cerr << "NetworkRuntime::Create failed\n";
        return 1;
    }
    auto runtime = std::move(rt).value();
    if (!runtime->Start()) {
        std::cerr << "NetworkRuntime::Start failed\n";
        return 1;
    }

    auto srv = runtime->ListenHttp({"127.0.0.1", 0}, EchoHandler);
    if (!srv) {
        std::cerr << "ListenHttp failed\n";
        return 1;
    }
    auto server = std::move(srv).value();
    const auto port = server->LocalAddress().port;

    // 出站调用只能在协程上下文执行：等待期间挂起当前协程，不占线程。
    const bool ran = RunInCoroutine([&] {
        auto cl = runtime->CreateHttpClient();
        if (!cl) {
            std::cerr << "CreateHttpClient failed\n";
            exit_code = 1;
            return;
        }
        auto client = std::move(cl).value();

        CallOptions options;
        // deadline 是本次调用的预算；到期返回 Error(TimedOut)，底层清理继续。
        options.deadline = std::chrono::steady_clock::now() +
                           std::chrono::seconds{5};

        HttpRequest request;
        request.method = "GET";
        request.url    = "http://127.0.0.1:" + std::to_string(port) + "/echo";
        request.body   = "hello";
        auto reply     = client->Request(std::move(request), options);
        if (!reply) {
            const auto& err = reply.error();
            std::cerr << "Request failed: code=" << static_cast<int>(err.code)
                      << " message=" << err.message << "\n";
            exit_code = 1;
        } else {
            std::cout << "status=" << reply.value().status
                      << " body=" << reply.value().body << "\n";
        }

        // 关闭边界：RequestClose 幂等，可在协程或普通线程调用；
        // WaitClosed 只能在协程内等待，等在途操作真正归零。
        client->RequestClose();
        client->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds{5}, {});
    });
    if (!ran) {
        std::cerr << "coroutine did not finish in budget\n";
        exit_code = 1;
    }

    // 正常关闭顺序：先停止接纳新请求 → 关闭 runtime 并等待在途清理。
    server->StopAccepting();
    runtime->RequestClose();
    RunInCoroutine([&] {
        runtime->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::seconds{5}, {});
    });

    server.reset();
    runtime.reset();
    scheduler->Stop();
    return exit_code;
}
