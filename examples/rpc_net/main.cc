// RPC core 最小真实消费示例：应用装配双 Engine（server 监听 + client 池），
// rpc::Client ↔ rpc::Server 经真实 loopback TCP 完成 (string,int64) 位置参数
// 回声与一次业务错误往返；收口走 StopAccepting → Drain → Close → WaitStopped。
// 本示例是公开调用方式/错误/关闭责任的消费确认入口，不是 mock 冒烟。

#include <bbt/infra/rpc/Client.hpp>
#include <bbt/infra/rpc/HttpTransport.hpp>
#include <bbt/infra/rpc/RpcPayload.hpp>
#include <bbt/infra/rpc/RpcWire.hpp>
#include <bbt/infra/rpc/Server.hpp>
#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <exception>
#include <future>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <vector>

namespace rpc = bbt::infra::rpc;
namespace http = bbt::infra::http;
namespace net = bbt::infra::net;
using bbt::infra::IncomingCallContext;
using bbt::infra::RpcEnvelope;
using bbt::infra::result;
using namespace std::chrono_literals;

namespace {

void Check(bool condition, const char* message) {
    if (!condition) { throw std::runtime_error(message); }
}

struct Runner {
    std::shared_ptr<net::Engine> engine;
    std::error_code error;
    std::thread thread;
    Runner(std::shared_ptr<net::Engine> value, net::EngineConfig config)
        : engine(std::move(value)) {
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
    while (engine->Phase() != net::EnginePhase::Running &&
           std::chrono::steady_clock::now() < end) {
        std::this_thread::sleep_for(1ms);
    }
    Check(engine->Phase() == net::EnginePhase::Running, "Engine did not start");
}

std::vector<std::uint8_t> Pack(const std::string& text, std::int64_t number) {
    auto encoded = rpc::EncodeTuple(std::make_tuple(text, number),
                                    std::make_index_sequence<2>{});
    Check(static_cast<bool>(encoded), "EncodeTuple");
    return std::move(encoded).value();
}

std::tuple<std::string, std::int64_t> Unpack(const std::vector<std::uint8_t>& bytes) {
    auto decoded = rpc::DecodeTuple<std::string, std::int64_t>(
        bytes, std::make_index_sequence<2>{});
    Check(static_cast<bool>(decoded), "DecodeTuple");
    return std::move(decoded).value();
}

// detached 消费任务与宿主之间的共享完成状态：堆对象 + 一次性终态。
// 任务按值持有 shared_ptr，宿主先收口/退出时状态仍存活，不会悬空；一次
// 尝试只交付一个终态，避免第二次 set_value/set_exception 抛 future_error。
class ConsumerTicket {
public:
    std::future<void> Ready() { return m_done.get_future(); }

    void Succeed() noexcept {
        if (!m_settled.exchange(true)) { m_done.set_value(); }
    }

    void Fail(std::exception_ptr error) noexcept {
        if (!m_settled.exchange(true)) { m_done.set_exception(std::move(error)); }
    }

private:
    std::promise<void> m_done;
    std::atomic<bool> m_settled{false};
};

int Example() {
    // 1) 执行底座由应用一次性装配：scheduler worker ≥ server 1 driver 槽 +
    //    client 1 槽 + 业务协程。
    g_bbt_coroutine_config->m_cfg_static_thread_num = 4;
    g_bbt_coroutine_config->m_cfg_stack_size = 128 * 1024;
    g_bbt_coroutine_config->m_cfg_processer_auto_scale = false;
    g_scheduler->Start();

    // 2) server：HttpRpcInboundTransport::Create（Engine Idle，只配置 handoff）
    //    → rpc::Server::Create(inbound, handler) → Run → Start。
    auto server_engine =
        std::make_shared<net::Engine>(std::shared_ptr<net::RawHandler>{});
    auto made_inbound = rpc::HttpRpcInboundTransport::Create(server_engine);
    Check(static_cast<bool>(made_inbound), "rpc inbound transport Create");
    auto made_server = rpc::Server::Create(
        std::shared_ptr<rpc::IRpcInboundTransport>(
            std::move(made_inbound).value()),
        [](IncomingCallContext, RpcEnvelope request) -> result<RpcEnvelope> {
            if (request.method == "Fail") {
                return result<RpcEnvelope>::err(bbt::infra::MakeError(
                    bbt::infra::ErrorCode::NotFound, "method Fail always fails"));
            }
            return result<RpcEnvelope>::ok(std::move(request));
        });
    Check(static_cast<bool>(made_server), "rpc Server Create");
    auto server = std::move(made_server).value();
    Check(server_engine->Phase() == net::EnginePhase::Idle, "Create does not Run");
    net::EngineConfig config;
    config.runtime_progress = net::RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    std::error_code address_error;
    config.listen_addresses = {net::Address::Ip("127.0.0.1", 0, address_error)};
    Check(!address_error, "listen address");
    Runner server_run(server_engine, config);
    Running(server_engine);
    Check(static_cast<bool>(server->Start()), "rpc Server Start");
    std::vector<net::Address> addresses;
    Check(!server_engine->LocalAddresses(addresses) && addresses.size() == 1,
          "bound address");
    const std::string endpoint =
        "127.0.0.1:" + std::to_string(addresses.front().Port());

    // 3) client：应用装配 http::Client（Engine + base_url），HttpRpcTransport
    //    包装其池，rpc::Client 只持有 IRpcTransport seam。
    auto client_engine =
        std::make_shared<net::Engine>(std::shared_ptr<net::RawHandler>{});
    config.listen_addresses.clear();
    Runner client_run(client_engine, config);
    Running(client_engine);
    http::ClientOptions client_options;
    client_options.base_url = "http://" + endpoint;
    auto made_http = http::Client::Create(client_engine, client_options);
    Check(static_cast<bool>(made_http), "http Client Create");
    auto made_transport =
        rpc::HttpRpcTransport::Create(std::move(made_http).value());
    Check(static_cast<bool>(made_transport), "rpc transport Create");
    auto made_client = rpc::Client::Create(
        std::shared_ptr<rpc::IRpcTransport>(std::move(made_transport).value()));
    Check(static_cast<bool>(made_client), "rpc Client Create");
    auto client = std::move(made_client).value();

    // 4) 业务协程内：Call 以 bbt_co_await 消费；错误经 result 返回，不抛异常。
    //    detached 任务只按值持有堆上完成状态、rpc::Client 的共享所有权与
    //    endpoint 副本，不捕获本函数的栈引用；完成与失败都只交付一次终态。
    auto ticket = std::make_shared<ConsumerTicket>();
    auto consumer_done = ticket->Ready();
    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        128 * 1024,
        [ticket, client, endpoint] {
            try {
                const bbt::infra::CallOptions until{
                    std::chrono::steady_clock::now() + 3s};
                bbt::infra::RpcEnvelope request;
                request.service = "echo";
                request.method = "Say";
                request.request_id = "example-1";
                request.request_schema = std::string(rpc::kRpcPositionalSchema);
                request.response_schema = std::string(rpc::kRpcPositionalSchema);
                request.payload = Pack("hello", 42);
                request.metadata = {{"trace.id", "example"}};

                auto reply =
                    bbt_co_await client->Call({"http", endpoint}, request, until);
                Check(reply && reply.value().request_id == "example-1", "echo reply");
                const auto [text, number] = Unpack(reply.value().payload);
                Check(text == "hello" && number == 42, "echo payload roundtrip");

                request.request_id = "example-2";
                request.method = "Fail";
                auto failure =
                    bbt_co_await client->Call({"http", endpoint}, request, until);
                Check(!failure && failure.error().code ==
                                        bbt::infra::ErrorCode::NotFound,
                      "handler error propagates");
                ticket->Succeed();
            } catch (...) {
                ticket->Fail(std::current_exception());
            }
        },
        registered);

    // 宿主等待有界：超时不能在这里抛出——栈展开会先销毁示例的局部状态，而
    // detached 任务可能仍在运行。先记录结果，按示例顺序收口后再上报失败。
    const bool consumer_ready =
        registered && consumer_done.wait_for(5s) == std::future_status::ready;

    // 5) 收口：server StopAccepting → Drain → Close → WaitStopped（每个 result
    //    均检查）；client 逻辑封口；最后 Engine RequestClose + join。
    server->StopAccepting();
    Check(static_cast<bool>(server->Drain(std::chrono::steady_clock::now() + 3s)),
          "Drain includes replies");
    server->Close();
    Check(server->IsClosed(), "IsClosed after Close");
    Check(static_cast<bool>(server->WaitStopped(std::chrono::steady_clock::now() + 3s)),
          "HTTP physical stop");
    client->Close();
    Check(client->IsClosed(), "client closed");
    client_run.Join();
    server_run.Join();
    Check(!client_run.error && !server_run.error, "Run errors");
    Check(server_engine->Phase() == net::EnginePhase::Stopped &&
              client_engine->Phase() == net::EnginePhase::Stopped,
          "Engine phases");
    Check(server_engine->CountConnections() == 0 &&
              client_engine->CountConnections() == 0,
          "network drained");

    // 6) 消费终态：收口已完成，任务此时只能观察到关闭后的终态。超时（含注册
    //    失败）无论任务随后是否落定，都作为宿主失败上报，且不打印成功行。
    if (!registered) { throw std::runtime_error("consumer task registration"); }
    if (!consumer_ready) {
        const bool settled_after_close =
            consumer_done.wait_for(3s) == std::future_status::ready;
        throw std::runtime_error(
            settled_after_close
                ? "consumer exceeded 5s and settled only after shutdown"
                : "consumer exceeded 5s and never settled");
    }
    std::exception_ptr consumer_failure;
    try {
        consumer_done.get();
    } catch (...) {
        consumer_failure = std::current_exception();
    }
    // 收口完成后才传播消费失败：main 返回非零，且不打印成功行。
    if (consumer_failure) { std::rethrow_exception(consumer_failure); }
    std::cout << "rpc example echo=(string,int64) errors=NotFound stopped=2 engines "
                 "connections=0\n";
    return 0;
}

} // namespace

int main() {
    int result = 1;
    try {
        result = Example();
    } catch (const std::exception& error) {
        std::cerr << "FAIL " << error.what() << '\n';
    }
    std::cout.flush();
    std::cerr.flush();
    std::fflush(nullptr);
    std::_Exit(result); // runtime 进程寿命；不是 RPC/network 回收证据
}
