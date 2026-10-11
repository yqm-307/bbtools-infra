// 独立于 bbt-framework 的 RpcClient/RpcServer 双进程 consumer。
//
// 模式：
//   <peer> server <port_file>
//       启动 RpcServer（handler 覆盖 echo / missing / mismatch），读 stdin 行；
//       收到 "stop" 调用 StopAccepting()（只停新接纳），EOF 后收口退出。
//   <peer> client <port>
//       进程 A 侧消费者：成功、结构化错误 NotFound、关联字段不匹配、
//       坏消息（非 POST/rpc、非 protobuf Content-Type、空 body、非法字节）。
//   <peer> client_stop_check <port>
//       StopAccepting 之后的新调用：必须是有界失败，且不是成功回复。
//       （由测试侧在「服务端 acceptor 物理拒绝」之后才发起，见 server 模式
//        的 refused ack。）
//   <peer> server_hold <port_file> <marker_file>
//       handler 被调用后写 marker 文件（服务端已完整收到请求的因果事件），
//       然后有界阻塞；由测试侧 kill 制造「请求已完整写出后对端失联」。
//   <peer> client_hold <port>
//       请求已完整写出后对端失联 ⇒ OutcomeUnknown + RequestCommitted。
//
// 全部经公开面（NetworkRuntime/RpcClient/RpcServer/HttpClient + wire 编解码），
// 不经 framework，也不 include src/ 内部头。

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/RpcClient.hpp>
#include <bbt/infra/RpcServer.hpp>
#include <bbt/infra/rpc/RpcWire.hpp>

using namespace bbt::infra;

namespace {

std::shared_ptr<NetworkRuntime> g_runtime;
std::shared_ptr<RpcClient>      g_client;
std::shared_ptr<RpcServer>      g_server;
std::shared_ptr<HttpClient>     g_http;

NetworkLimits Limits() {
    NetworkLimits limits{};
    limits.max_connections  = 16;
    limits.max_inflight     = 16;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = 64 * 1024;
    limits.incoming_timeout = std::chrono::milliseconds{5000};
    return limits;
}

// 协程任务的完成态与等待量都放堆上：外层预算必须 >= 内层 deadline + 裕量，
// 否则内层调用仍在运行时外层等待先超时，用例离开作用域会留下悬垂栈引用
// （peer 侧所有调用点都按「内层 deadline + 5s 裕量」传外层预算）。
bool RunInCoroutine(std::function<void()> task, int budget_ms) {
    auto done    = std::make_shared<bbt::core::thread::CountDownLatch>(1);
    bool started = false;
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [task = std::move(task), done]() mutable {
            task();
            done->Down();
        },
        started);
    return started && done->WaitTimeout(budget_ms) == 0;
}

void Setup() {
    auto* config = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    config->m_cfg_static_thread_num = 2;
    bbt::coroutine::detail::Scheduler::GetInstance()->Start(
        bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);
    auto runtime = NetworkRuntime::Create(Limits());
    if (runtime) {
        g_runtime = std::move(runtime).value();
        g_runtime->Start();
    }
}

void Teardown() {
    if (g_http) {
        g_http->Close();
        g_http.reset();
    }
    if (g_client) {
        g_client->Close();
        g_client.reset();
    }
    if (g_server) {
        g_server->Close();
        g_server.reset();
    }
    if (g_runtime) {
        g_runtime->Close();
        g_runtime.reset();
    }
}

CallOptions DeadlineAfter(int ms) {
    CallOptions options;
    options.deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds{ms};
    return options;
}

RpcEnvelope Request(std::string id, std::string method) {
    RpcEnvelope request;
    request.request_id      = std::move(id);
    request.service         = "svc.echo";
    request.method          = std::move(method);
    request.request_schema  = "bbt.echo.EchoReq/v1";
    request.response_schema = "bbt.echo.EchoResp/v1";
    request.payload         = {'p', 'i', 'n', 'g'};
    request.metadata        = {{"trace.id", "runtime-peer"}};
    return request;
}

result<RpcEnvelope> CallOnce(const RpcAddress& address, RpcEnvelope request,
                            int budget_ms = 10000) {
    // request 由协程任务拥有（move 进 lambda）；结果放堆上共享对象。
    auto reply = std::make_shared<std::optional<result<RpcEnvelope>>>();
    const bool returned = RunInCoroutine(
        [address, request = std::move(request), budget_ms, reply]() mutable {
            reply->emplace(
                g_client->Call(address, std::move(request), DeadlineAfter(budget_ms)));
        },
        budget_ms + 5000);   // 外预算 >= 内层 deadline + 裕量
    if (!returned) {
        return result<RpcEnvelope>::err(
            MakeError(ErrorCode::InternalError, "peer: call did not return"));
    }
    if (!reply->has_value())
        return result<RpcEnvelope>::err(
            MakeError(ErrorCode::InternalError, "peer: no reply slot"));
    return std::move(**reply);
}

// 有界阻塞（进程会被 kill；这里只作为「不无限等待」的上限）。
void BlockBoundedMs(int ms) {
    const int steps = ms / 10;
    for (int i = 0; i < steps; ++i)
        ::usleep(10 * 1000);
}

// 有界 raw connect 直到连接被物理拒绝：返回 true 表示本进程确认服务端的
// acceptor 已真实关闭（不是靠 sleep 猜时序）。用于 StopAccepting 的 ack：
// 只有物理拒绝已发生，后续 client 调用才是确定失败（连接未建立）。
bool WaitAcceptorRefused(std::uint16_t port, int budget_ms) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds{budget_ms};
    while (std::chrono::steady_clock::now() < deadline) {
        const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0)
            return false;
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port        = htons(port);
        const int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr),
                                 sizeof(addr));
        ::close(fd);
        if (rc != 0)
            return true;   // 物理拒绝 ⇒ acceptor 已关闭
        ::usleep(5 * 1000);
    }
    return false;
}

// 用公开 HttpClient 发一条「原始」HTTP 请求；成功取到 200 响应时返回解析出的
// wire envelope（坏消息应得到 success=false 的错误 envelope）。任何前置失败
// （无响应/非 200/解析失败）返回 nullopt，由调用方判定为失败。
std::optional<rpc::RpcWireEnvelope> RawWireProbe(
    const std::string& url,
    std::vector<std::pair<std::string, std::string>> headers,
    const std::string& body) {
    auto state = std::make_shared<std::optional<result<HttpResponse>>>();
    HttpRequest request;
    request.method  = "POST";
    request.url     = url;
    request.headers = std::move(headers);
    request.body    = body;
    CallOptions options = DeadlineAfter(10000);
    if (!RunInCoroutine(
            [request = std::move(request), options, state]() mutable {
                state->emplace(g_http->Request(std::move(request), options));
            },
            15000))   // 外预算 = 内层 deadline + 5s 裕量
        return std::nullopt;
    if (!state->has_value())
        return std::nullopt;
    if (!state->value() || state->value().value().status != 200)
        return std::nullopt;
    auto parsed = rpc::ParseRpcWireHttpResponse(state->value().value());
    if (!parsed)
        return std::nullopt;
    return parsed.value();
}

} // namespace

int main(int argc, char* argv[]) {
    // --- server：普通服务端（stdin "stop" → StopAccepting）------------------
    if (argc >= 2 && std::strcmp(argv[1], "server") == 0) {
        if (argc < 3)
            return 90;
        Setup();
        if (!g_runtime)
            return 3;
        auto server = g_runtime->ListenRpc(
            {"127.0.0.1", 0},
            [](IncomingCallContext, RpcEnvelope request) -> result<RpcEnvelope> {
                if (request.method == "missing")
                    return result<RpcEnvelope>::err(
                        MakeError(ErrorCode::NotFound, "method not found"));
                if (request.method == "mismatch") {
                    request.response_schema = "bbt.echo.OtherResp/v1";
                    return result<RpcEnvelope>::ok(std::move(request));
                }
                request.payload.push_back('!');
                return result<RpcEnvelope>::ok(std::move(request));
            });
        if (!server) {
            Teardown();
            return 5;
        }
        g_server = std::move(server).value();
        {
            std::ofstream port_file(argv[2], std::ios::trunc);
            port_file << g_server->LocalAddress().port;
        }
        std::string line;
        char        buffer[64];
        for (;;) {
            const ssize_t n = ::read(STDIN_FILENO, buffer, sizeof(buffer));
            if (n <= 0)
                break;
            line.append(buffer, static_cast<std::size_t>(n));
            if (line.find("stop") != std::string::npos) {
                g_server->StopAccepting();   // 只停新接纳，不动已接纳 handler
                if (argc >= 4) {
                    // ack 只在「acceptor 已物理关闭、新连接被拒绝」之后写：
                    // 测试侧据此确定后续调用是「连接未建立」的确定失败，不靠
                    // sleep 猜 StopAccepting 的异步收口时序（M1）。
                    const bool refused = WaitAcceptorRefused(
                        g_server->LocalAddress().port, 6000);
                    std::ofstream acknowledged(argv[3], std::ios::trunc);
                    acknowledged << (refused ? "refused" : "not-refused");
                }
                line.clear();
            }
            if (line.size() > 4096)
                line.clear();
        }
        Teardown();
        return 0;
    }

    // --- server_hold：handler 写 marker 后有界阻塞（由测试侧 kill）---------
    if (argc >= 2 && std::strcmp(argv[1], "server_hold") == 0) {
        if (argc < 4)
            return 92;
        Setup();
        if (!g_runtime)
            return 3;
        const std::string marker = argv[3];
        auto server = g_runtime->ListenRpc(
            {"127.0.0.1", 0},
            [marker](IncomingCallContext, RpcEnvelope request)
                -> result<RpcEnvelope> {
                {
                    std::ofstream out(marker, std::ios::trunc);
                    out << "handling";
                }
                BlockBoundedMs(60000);   // 进程被 kill，这里只设上限
                return result<RpcEnvelope>::ok(std::move(request));
            });
        if (!server) {
            Teardown();
            return 5;
        }
        g_server = std::move(server).value();
        {
            std::ofstream port_file(argv[2], std::ios::trunc);
            port_file << g_server->LocalAddress().port;
        }
        BlockBoundedMs(60000);
        Teardown();
        return 0;
    }

    // --- client：成功 / 结构化错误 / 关联不匹配 / 坏消息 ---------------------
    if (argc >= 2 && std::strcmp(argv[1], "client") == 0) {
        if (argc < 3)
            return 91;
        Setup();
        if (!g_runtime)
            return 3;
        auto client = g_runtime->CreateRpcClient();
        if (!client) {
            Teardown();
            return 5;
        }
        g_client = std::move(client).value();
        auto http = g_runtime->CreateHttpClient();
        if (!http) {
            Teardown();
            return 5;
        }
        g_http = std::move(http).value();

        const std::string   port = argv[2];
        const RpcAddress    address{"http", "127.0.0.1:" + port};
        const std::string   url = "http://127.0.0.1:" + port + "/rpc";

        // 1) 成功：回复关联与 payload 一致。
        auto ok = CallOnce(address, Request("runtime-peer-1", "Echo"));
        if (!ok || ok.value().request_id != "runtime-peer-1" ||
            ok.value().payload != std::vector<std::uint8_t>{'p', 'i', 'n', 'g', '!'}) {
            Teardown();
            return 7;
        }

        // 2) 结构化错误：错误回复不进成功 payload。
        auto missing = CallOnce(address, Request("runtime-peer-2", "missing"));
        if (missing || missing.error().code != ErrorCode::NotFound) {
            Teardown();
            return 8;
        }

        // 3) 关联不匹配：客户端关联校验拒绝为 ProtocolError。
        auto mismatch = CallOnce(address, Request("runtime-peer-3", "mismatch"));
        if (mismatch || mismatch.error().code != ErrorCode::ProtocolError) {
            Teardown();
            return 9;
        }

        // 4) 坏消息（原始 HTTP，public HttpClient）：非 protobuf Content-Type
        //    ⇒ 服务端返回 wire 错误 envelope，不进入业务 handler。
        //    G8：未解析请求的错误 envelope 只带固定占位（request_id "unparsed"、
        //    service/method "unknown"）；不宣称 runtime client 收到这些错误的
        //    原码（关联字段会对应不上，client 侧会拒绝）。
        {
            auto env = RawWireProbe(url, {{"Content-Type", "text/plain"}},
                                    "not-a-wire-envelope");
            if (!env || env->success ||
                env->error.code != ErrorCode::InvalidArgument) {
                Teardown();
                return 11;
            }
            if (env->request_id != "unparsed" || env->service != "unknown" ||
                env->method != "unknown") {
                Teardown();
                return 17;
            }
        }

        // 5) 坏消息：空 body（Content-Type 正确）⇒ ProtocolError envelope。
        {
            auto env = RawWireProbe(
                url, {{"Content-Type", "application/x-protobuf"}}, "");
            if (!env || env->success ||
                env->error.code != ErrorCode::ProtocolError) {
                Teardown();
                return 13;
            }
            if (env->request_id != "unparsed" || env->service != "unknown") {
                Teardown();
                return 18;
            }
        }

        // 6) 坏消息：非法 protobuf 字节 ⇒ ProtocolError envelope（不崩、不成功），
        //    同样以固定占位标注「未解析」。
        {
            auto env = RawWireProbe(
                url, {{"Content-Type", "application/x-protobuf"}},
                std::string("\xff\xff\xff\xff\x01\x02", 6));
            if (!env || env->success ||
                env->error.code != ErrorCode::ProtocolError) {
                Teardown();
                return 15;
            }
            if (env->request_id != "unparsed" || env->service != "unknown") {
                Teardown();
                return 19;
            }
        }

        Teardown();
        return 0;
    }

    // --- client_stop_check：StopAccepting 后的新调用必须有界失败 ------------
    if (argc >= 2 && std::strcmp(argv[1], "client_stop_check") == 0) {
        if (argc < 3)
            return 93;
        Setup();
        if (!g_runtime)
            return 3;
        auto client = g_runtime->CreateRpcClient();
        if (!client) {
            Teardown();
            return 5;
        }
        g_client = std::move(client).value();
        const RpcAddress address{"http", "127.0.0.1:" + std::string(argv[2])};

        // 只用合法小请求，确保走到 socket 连接而非本地 body 限额。
        auto res = CallOnce(address, Request("runtime-peer-4", "Echo"), 1500);
        if (res || res.error().code != ErrorCode::TransportError) {
            Teardown();
            return 16;
        }
        if (!res.error().request_phase.has_value() ||
            IsRequestCommitted(*res.error().request_phase)) {
            Teardown();
            return 17;
        }
        Teardown();
        return 0;
    }

    // --- client_hold：完整写出后对端失联 ⇒ OutcomeUnknown -------------------
    if (argc >= 2 && std::strcmp(argv[1], "client_hold") == 0) {
        if (argc < 3)
            return 94;
        Setup();
        if (!g_runtime)
            return 3;
        auto client = g_runtime->CreateRpcClient();
        if (!client) {
            Teardown();
            return 5;
        }
        g_client = std::move(client).value();
        const RpcAddress address{"http", "127.0.0.1:" + std::string(argv[2])};

        auto res = CallOnce(address, Request("runtime-peer-5", "hold"), 20000);
        if (res || res.error().code != ErrorCode::OutcomeUnknown) {
            Teardown();
            return 18;
        }
        if (!res.error().request_phase.has_value() ||
            *res.error().request_phase != RequestPhase::RequestCommitted) {
            Teardown();
            return 19;
        }
        Teardown();
        return 0;
    }

    return 1;
}
