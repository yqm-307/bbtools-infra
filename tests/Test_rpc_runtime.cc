// N2：公开 RpcClient/RpcServer 真实 loopback 合同。
// 覆盖正式 body wire、请求关联、handler 错误传播与同步 Close；
// 不经 framework，不把 wire-only round-trip 当作 runtime 完成。
//
// 异步栈借用（本次修复）：本文件里所有跨协程/跨线程延后执行的对象都在堆上共享，
// 闭包一律按值持有 shared 引用，绝不按引用捕获测试栈帧——协程没有 join，
// 调用方在超时路径提前返回并析构栈帧时，仍在运行的协程任务不得再触碰其中任何
// 对象。具体：
//   - 完成信号 CoDone（mutex+condition_variable）与结果值 CoRunState<T> 均在堆上，
//     由任务闭包按值持有的 shared_ptr 拥有；测试线程按有界预算等待完成屏障后再
//     读取结果（跨线程有 happens-before，不读未排序状态）。
//   - 每次 Call 的 client/address/request/options 全部按值拷入闭包，不借用测试局
//     部变量；reply 在完成屏障之后从堆状态取回。
//   - 服务端 handler 观测到的期限放在堆上 mutex 保护的 DeadlineObservation，handler
//     按值持有 shared_ptr（不捕获测试栈上的 observed_deadline）。
//   - runtime 由 RAII guard 在任何失败/早退路径 Close；任务闭包按值持有 callee
//     （RpcClient shared_ptr），晚到任务不会看到已析构对象。
// 原 RunInCoroutine 的 [&fn, &done] 风格按引用借用调用方栈，已删除。
//
// 新增回归 t_timeout_return_then_late_completion：受控堆 gate（按值持有的
// shared_ptr，拒绝捕获栈）+ 一次有界短等待真实超时返回（任务仍在 gate 上未完成）
// + 释放 gate 后观察晚到任务真实完成；注册失败即失败（不以「没注册成功」空过）。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>

#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/RpcClient.hpp>
#include <bbt/infra/RpcServer.hpp>

using namespace bbt::infra;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

constexpr auto kDeadline = std::chrono::milliseconds{5000};
// 受控 gate 的进入/退出有界等待上限（非完成依据，仅防挂死）。
constexpr auto kGateWait = std::chrono::seconds{20};

void EnsureScheduler() {
    auto& scheduler = *bbt::coroutine::detail::Scheduler::GetInstance();
    if (!scheduler.IsInitialized()) {
        auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
        // 显式协程栈大小（与既有 RPC 套件 Test_rpc_runtime_lifetime /
        // Test_rpc_runtime_faults 同为 256 KiB，HTTP 侧套件同量级）。
        // 理由：本用例的协程内调用链（Call → ToWireEnvelope →
        // ValidateRpcWireEnvelope → protobuf 生成构造 + UBSan dynamic_cast）
        // 在 ASan/UBSan 下的有效栈耗用约 25–32 KiB，超出 coroutine 默认
        // 12 KiB 栈；实测 ASan/UBSan 构建下 12/16/24 KiB 确定性
        // stack-overflow、≥32 KiB 通过，而同一代码在 Debug/Release 默认栈下
        // 通过——是 sanitizer 栈开销而非被测行为/断言问题。256 KiB 留约 8 倍
        // 余量，且不改动任何断言或被测语义。
        cfg->m_cfg_stack_size = 256 * 1024;
        // 4 调度线程：受控 gate 上用阻塞等待挂起一个调度线程时，其余线程继续
        // 推进 io 域（与 Test_rpc_runtime_lifetime 同理由）。
        cfg->m_cfg_static_thread_num = 4;
        scheduler.Start(SCHE_START_OPT_SCHE_THREAD);
    }
    BOOST_REQUIRE(scheduler.IsInitialized());
}

NetworkLimits Limits() {
    NetworkLimits limits{};
    limits.max_connections = 32;
    limits.max_inflight = 32;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes = 64 * 1024;
    limits.incoming_timeout = std::chrono::milliseconds{3000};
    return limits;
}

// 堆上完成信号：由任务闭包按值持有的 shared 状态拥有，不捕获调用方栈对象。
// 超时返回不会让仍在运行的任务触碰已析构的调用方栈帧（消除旧 RunInCoroutine
// [&fn, &done] 的悬垂窗口）。
struct CoDone {
    std::mutex              mtx;
    std::condition_variable cv;
    bool                    done{false};

    void Complete() {
        {
            std::lock_guard<std::mutex> lk(mtx);
            done = true;
        }
        cv.notify_all();
    }
    // 返回 true = 预算内有界完成；false = 超时（任务可能仍在运行，堆状态仍有效）。
    bool WaitFor(std::chrono::milliseconds budget) {
        std::unique_lock<std::mutex> lk(mtx);
        return cv.wait_for(lk, budget, [this] { return done; });
    }
    bool Ready() {
        std::lock_guard<std::mutex> lk(mtx);
        return done;
    }
};

// 协程任务堆上完成态：任务只写自己 shared 持有的对象。
template <class T>
struct CoRunState {
    CoDone           barrier;
    std::optional<T> value;   // 写入发生在 barrier.Complete() 之前（见 SpawnTask）

    bool Ready() { return barrier.Ready(); }
    bool WaitFor(std::chrono::milliseconds budget) {
        return barrier.WaitFor(budget);
    }
    // 只在完成屏障之后有意义：加锁并仅在 done 时返回，读取与任务写入经 barrier
    // 的 mutex 建立 happens-before；未完成返回 nullopt（不读未排序状态）。
    std::optional<T> Snapshot() {
        std::lock_guard<std::mutex> lk(barrier.mtx);
        if (!barrier.done)
            return std::nullopt;
        return value;
    }
};

// 注册协程任务：闭包按值持有 fn（自带按值捕获）与共享完成态，绝不借用调用方栈。
// 注册失败返回 nullptr（调用方必须显式失败，不许空过）。
template <class F, class T = std::invoke_result_t<F>>
std::shared_ptr<CoRunState<T>> SpawnTask(F&& f) {
    auto state = std::make_shared<CoRunState<T>>();
    bool ok    = false;
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [fn = std::forward<F>(f), state]() mutable {
            state->value.emplace(fn());
            state->barrier.Complete();
        },
        ok);
    return ok ? state : nullptr;
}

// 单次 Call 的堆上共享状态：client/address/request/options 全部按值拷入闭包。
struct PendingCall {
    std::shared_ptr<CoRunState<result<RpcEnvelope>>> state;

    bool Spawn(const std::shared_ptr<RpcClient>& client, RpcAddress address,
               RpcEnvelope request, CallOptions options) {
        state = SpawnTask(
            [client, address, request = std::move(request), options]() mutable {
                return client->Call(address, std::move(request), options);
            });
        return state != nullptr;
    }
    bool Ready() { return state && state->Ready(); }
    bool WaitReady(std::chrono::milliseconds budget) {
        return state && state->WaitFor(budget);
    }
    // 完成屏障之后从堆状态取回结果；未完成返回 nullopt。
    std::optional<result<RpcEnvelope>> Reply() {
        if (!state)
            return std::nullopt;
        return state->Snapshot();
    }
};

// 服务端 handler 观测到的期限：堆上 mutex 保护，handler 按值持有 shared_ptr。
struct DeadlineObservation {
    mutable std::mutex mtx;
    std::optional<std::chrono::steady_clock::time_point> last;

    void Record(std::chrono::steady_clock::time_point tp) {
        std::lock_guard<std::mutex> lk(mtx);
        last = tp;
    }
    std::optional<std::chrono::steady_clock::time_point> Get() const {
        std::lock_guard<std::mutex> lk(mtx);
        return last;
    }
};

// 受控 gate（堆上共享对象，按值持有的 shared_ptr，绝不捕获栈）：handler 进入后
// 挂起，测试线程显式 Release（有界）。
struct Gate {
    std::mutex              mtx;
    std::condition_variable cv;
    bool                    entered{false};
    bool                    released{false};
    bool                    exited{false};

    void Enter() {
        {
            std::lock_guard<std::mutex> lk(mtx);
            entered = true;
        }
        cv.notify_all();
    }
    bool WaitEntered(std::chrono::milliseconds budget) {
        std::unique_lock<std::mutex> lk(mtx);
        return cv.wait_for(lk, budget, [this] { return entered; });
    }
    // 挂起直至显式 Release（含内部 20s 上界，防测试挂死；正常路径由 Release 放行）。
    void Hold() {
        std::unique_lock<std::mutex> lk(mtx);
        cv.wait_for(lk, std::chrono::seconds{20}, [this] { return released; });
    }
    void MarkExited() {
        {
            std::lock_guard<std::mutex> lk(mtx);
            exited = true;
        }
        cv.notify_all();
    }
    bool WaitExited(std::chrono::milliseconds budget) {
        std::unique_lock<std::mutex> lk(mtx);
        return cv.wait_for(lk, budget, [this] { return exited; });
    }
    void Release() {
        {
            std::lock_guard<std::mutex> lk(mtx);
            released = true;
        }
        cv.notify_all();
    }
};

// RAII：任何退出路径都释放 gate（handler 不悬空等待）。
struct GateReleaseGuard {
    std::shared_ptr<Gate> gate;
    ~GateReleaseGuard() {
        if (gate)
            gate->Release();
    }
};

// RAII：失败/早退路径也 Close runtime（Close 幂等）。声明早于 gate guard →
// 析构晚于它 → 失败清理顺序为「先 Release gate，再 Close runtime」。
struct RuntimeCloseGuard {
    std::shared_ptr<NetworkRuntime> runtime;
    ~RuntimeCloseGuard() {
        if (runtime)
            runtime->Close();
    }
};

// 受控 gate handler：进入 gate 并挂起，放行后 echo 请求。只按值持有 gate。
RpcHandler MakeGateHandler(const std::shared_ptr<Gate>& gate) {
    return [gate](IncomingCallContext, RpcEnvelope request) -> result<RpcEnvelope> {
        gate->Enter();
        gate->Hold();
        gate->MarkExited();
        return result<RpcEnvelope>::ok(std::move(request));
    };
}

} // namespace

BOOST_AUTO_TEST_SUITE(rpc_runtime)

BOOST_AUTO_TEST_CASE(t_public_runtime_roundtrip_and_error) {
    EnsureScheduler();

    auto runtime_result = NetworkRuntime::Create(Limits());
    BOOST_REQUIRE(runtime_result);
    auto runtime = std::move(runtime_result).value();
    // 任何失败/早退路径也 Close runtime（RAII），不留未收口 runtime。
    RuntimeCloseGuard runtime_guard{runtime};
    BOOST_REQUIRE(runtime->Start());

    // handler 观测到的期限放堆上（不捕获测试栈 observed_deadline）。
    auto observation = std::make_shared<DeadlineObservation>();
    auto server_result = runtime->ListenRpc({"127.0.0.1", 0},
        [observation](IncomingCallContext context, RpcEnvelope request)
            -> result<RpcEnvelope> {
            // handler 可能被并发派发/晚到；在 mutex 保护下记录，不触碰测试栈。
            observation->Record(context.deadline);
            if (request.method == "missing")
                return result<RpcEnvelope>::err(
                    MakeError(ErrorCode::NotFound, "method not found"));
            if (request.method == "mismatch") {
                request.response_schema = "test.OtherResponse/v1";
                return result<RpcEnvelope>::ok(std::move(request));
            }
            request.payload.push_back('!');
            return result<RpcEnvelope>::ok(std::move(request));
        });
    BOOST_REQUIRE(server_result);
    auto server = std::move(server_result).value();

    auto client_result = runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);
    auto client = std::move(client_result).value();

    RpcEnvelope invalid_request;
    CallOptions invalid_options;
    invalid_options.deadline = std::chrono::steady_clock::now() + kDeadline;
    auto invalid_endpoint = client->Call(
        {"http", "http://127.0.0.1:1"}, invalid_request, invalid_options);
    BOOST_REQUIRE(!invalid_endpoint);
    BOOST_CHECK(invalid_endpoint.error().code == ErrorCode::InvalidArgument);

    RpcAddress address{"http", "127.0.0.1:" +
        std::to_string(server->LocalAddress().port)};
    RpcEnvelope request;
    request.service = "test.Service";
    request.method = "Echo";
    request.request_id = "rpc-test-1";
    request.request_schema = "test.Request/v1";
    request.response_schema = "test.Response/v1";
    request.payload = {'o', 'k'};

    std::optional<result<RpcEnvelope>> reply;
    const auto outbound_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds{5};
    PendingCall echo;
    {
        CallOptions options;
        options.deadline = outbound_deadline;
        // 每次调用按值持有 client/address/request/options，不借用测试栈对象。
        BOOST_REQUIRE(echo.Spawn(client, address, request, options));
    }
    BOOST_REQUIRE(echo.WaitReady(kDeadline));   // 完成屏障（有界）
    reply = echo.Reply();                       // 屏障之后从堆状态读取
    BOOST_REQUIRE(reply.has_value());
    BOOST_REQUIRE(*reply);
    BOOST_CHECK_EQUAL(reply->value().request_id, request.request_id);
    BOOST_CHECK_EQUAL(reply->value().service, request.service);
    BOOST_CHECK_EQUAL(reply->value().method, request.method);
    BOOST_CHECK(reply->value().payload ==
                (std::vector<std::uint8_t>{'o', 'k', '!'}));
    const auto observed_deadline = observation->Get();
    BOOST_REQUIRE(observed_deadline.has_value());
    BOOST_CHECK(*observed_deadline <= outbound_deadline);
    BOOST_CHECK(*observed_deadline <=
                std::chrono::steady_clock::now() + std::chrono::seconds{4});

    request.method = "missing";
    request.request_id = "rpc-test-2";
    reply.reset();
    const auto short_deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds{200};
    PendingCall missing;
    {
        CallOptions options;
        options.deadline = short_deadline;
        BOOST_REQUIRE(missing.Spawn(client, address, request, options));
    }
    BOOST_REQUIRE(missing.WaitReady(kDeadline));
    reply = missing.Reply();
    BOOST_REQUIRE(reply.has_value());
    BOOST_REQUIRE(!*reply);
    BOOST_CHECK(reply->error().code == ErrorCode::NotFound);
    const auto observed_deadline2 = observation->Get();
    BOOST_REQUIRE(observed_deadline2.has_value());
    const auto server_now = std::chrono::steady_clock::now();
    BOOST_CHECK(*observed_deadline2 > server_now);
    BOOST_CHECK(*observed_deadline2 <=
                server_now + std::chrono::seconds{1});

    request.method = "mismatch";
    request.request_id = "rpc-test-3";
    reply.reset();
    PendingCall mismatch;
    {
        CallOptions options;
        options.deadline = std::chrono::steady_clock::now() + kDeadline;
        BOOST_REQUIRE(mismatch.Spawn(client, address, request, options));
    }
    BOOST_REQUIRE(mismatch.WaitReady(kDeadline));
    reply = mismatch.Reply();
    BOOST_REQUIRE(reply.has_value());
    BOOST_REQUIRE(!*reply);
    BOOST_CHECK(reply->error().code == ErrorCode::ProtocolError);

    reply.reset();
    PendingCall past_deadline;
    {
        CallOptions options;
        options.deadline = std::chrono::steady_clock::now() -
            std::chrono::milliseconds{1};
        BOOST_REQUIRE(past_deadline.Spawn(client, address, request, options));
    }
    BOOST_REQUIRE(past_deadline.WaitReady(kDeadline));
    reply = past_deadline.Reply();
    BOOST_REQUIRE(reply.has_value());
    BOOST_REQUIRE(!*reply);
    BOOST_CHECK(reply->error().code == ErrorCode::TimedOut);

    server->StopAccepting();
    server->Close();
    runtime->Close();
    BOOST_CHECK(server->IsClosed());
    BOOST_CHECK(client->IsClosed());
    BOOST_CHECK(runtime->IsClosed());
}

// 有界真实超时返回 + 晚到任务完成的回归：
// 受控堆 gate 上的 handler 让调用在短等待预算内真实超时返回（任务仍在 gate 上未
// 完成），随后释放 gate 并观察晚到任务真实完成。所有延后执行对象都在堆上，按值
// 持有。本用例正向验证迟到完成安全，不复现旧代码在超时失败后栈帧销毁的 UAF。
// 注册失败即失败，不以「没注册成功」空过。断言不依赖睡眠驱动时序。
BOOST_AUTO_TEST_CASE(t_timeout_return_then_late_completion) {
    EnsureScheduler();

    auto runtime_result = NetworkRuntime::Create(Limits());
    BOOST_REQUIRE(runtime_result);
    auto runtime = std::move(runtime_result).value();
    RuntimeCloseGuard runtime_guard{runtime};   // 失败先 Close runtime
    BOOST_REQUIRE(runtime->Start());

    auto gate = std::make_shared<Gate>();
    GateReleaseGuard gate_guard{gate};          // 声明晚于 runtime guard → 先 Release

    auto server_result = runtime->ListenRpc({"127.0.0.1", 0},
                                            MakeGateHandler(gate));
    BOOST_REQUIRE(server_result);
    auto server = std::move(server_result).value();

    auto client_result = runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);
    auto client = std::move(client_result).value();

    const RpcAddress address{"http", "127.0.0.1:" +
        std::to_string(server->LocalAddress().port)};
    RpcEnvelope request;
    request.service = "test.Service";
    request.method = "Gate";
    request.request_id = "rpc-late-1";
    request.request_schema = "test.Request/v1";
    request.response_schema = "test.Response/v1";
    request.payload = {'g'};

    PendingCall call;
    {
        CallOptions options;
        options.deadline = std::chrono::steady_clock::now() + kDeadline;
        BOOST_REQUIRE(call.Spawn(client, address, request, options));  // 注册成功
    }
    // 真实会话已建立：服务端 handler 已进入并停在 gate（请求完整到达）。
    BOOST_REQUIRE(gate->WaitEntered(kGateWait));

    // 一次有界短等待：handler 仍在 gate 上，调用未完成 → 真实超时返回。
    BOOST_CHECK(!call.WaitReady(std::chrono::milliseconds{50}));
    BOOST_CHECK(!call.Ready());

    // 释放 gate，观察晚到任务真实完成（有界），并从堆状态取回结果。
    gate->Release();
    BOOST_CHECK(gate->WaitExited(kGateWait));
    BOOST_REQUIRE(call.WaitReady(kDeadline));
    auto reply = call.Reply();
    BOOST_REQUIRE(reply.has_value());
    BOOST_REQUIRE(*reply);
    BOOST_CHECK_EQUAL(reply->value().request_id, request.request_id);
    BOOST_CHECK_EQUAL(reply->value().method, request.method);
    BOOST_CHECK(reply->value().payload ==
                (std::vector<std::uint8_t>{'g'}));

    server->StopAccepting();
    server->Close();
    client->Close();
    runtime->Close();
    BOOST_CHECK(server->IsClosed());
    BOOST_CHECK(client->IsClosed());
    BOOST_CHECK(runtime->IsClosed());
}

BOOST_AUTO_TEST_SUITE_END()
