// co-network/v1 N2 · waypoint rpc-infra-foundation 2-5：RPC 真路径的
// owner 物理收口 + 在途 handler 与 client/server Close 的生命周期定向测试。
//
// 范围与证据边界（不照抄调查假说，只断言本用例真实观察到的事实）：
//
//   1) 真工厂 owner 绑定 0→1→2→1→0（Debug/Release 两构建均成立，生产状态，非计数）：
//      NetworkRuntime::CreateRpcClient()/ListenRpc() 经 HTTP owner 登记为 runtime
//      子对象；对应 RpcClient/RpcServer 同步 Close() 触发 HTTP owner 物理关闭并经
//      ClosedHook 反登记。观测点 = http_detail::NetworkRuntimeImpl 的生产探针
//      UnclosedChildrenForTest()（在册未物理关闭的 HTTP 子对象数）。
//      每一前置（Create/Start/工厂成功）都显式 CHECK，不只打印。
//
//   2) 真会话建立 + 在途 handler 与 client/server Close 的真实收口：
//      经公开 RpcClient::Call → ListenRpc(handler) 完成一次成功 echo（服务端 handler
//      真被派发，证明真实 HTTP 会话已建立），随后受控 gated handler 在途时同步
//      Close server 再 Close client。Close 返回当刻核：
//        - UnclosedChildrenForTest()：server Close 后 ==1（client 仍登记）、client
//          Close 后 ==0；
//        - IsClosed()；
//        - server 端口新 connect() 被拒（连接阶段失败且 errno==ECONNREFUSED，
//          listener fd 已物理释放；socket() 创建失败等其它阶段/errno 不算拒绝）；
//        - /proc/self/fd 中自有进程 socket 数回落至 runtime 建立前基线（零容差；
//          本用例无测试自持客户端 fd，计数项全部为 runtime 持有）。
//      放行真正晚到 handler 并等其退出（有界），再等会话析构/收口（有界）。
//      Close 之后在途 Call 的终态必须是 OutcomeUnknown + RequestCommitted（请求已
//      完整写出、已提交）= 依时序正交的确定口径，不放宽成「任意错误」。
//
//   Debug 面（BBT_INFRA_STRINGENT_DEBUG，仅 Debug 配置）：追加进程级观测组件
//      src/debug/InfraDebug.hpp 的计数断言——HttpSessionDestroyed 增量 !=0（真发生
//      会话销毁，避免 0==0 恒真）且 DestroyedWhileRegistered 增量 ==0（无「仍登记即
//      析构」结构缺陷）。归因分两段证明，不允许 echo 会话单独满足最终条件：
//        (i) 显式 Release gated handler 之前，gated 会话被 handler 协程强持有、
//            必然未析构，故此刻「先等到」的 destroyed 增量只可能来自 echo 会话；
//            取该点为第二基线（destroyed_base2/tripwire_base2）；
//        (ii) Release 后「再等到」的 destroyed 增量（相对第二基线）只能归因于晚到
//            gated 会话真实退出并析构，且其 tripwire 增量也必须为 0。
//      计数器是进程级 inline 变量，故用本用例起始快照做增量比对。本 binary 含
//      owner 用例与在途用例两例，同进程顺序执行；owner 用例不建立任何 HTTP 会话
//      （无 Call），故计数的增量全部归于本用例（不存在「单用例进程隔离」）。
//   Release 面（宏 OFF，同一 .cc 编译）：计数与访问器在本构建里不存在（不是返回
//      常数 0 的假接口），本用例退回全部生产可观测断言（owner 0→1→0、端口拒绝、
//      socket 回落至基线、Call 终态），关宏不会让用例恒过。
//
//   sanitizer：本目标在 ASan/UBSan 构建下运行；迟到完成项触碰已释放 fd/对象会直接
//      报出（扫描 stderr，不只看退出码）。detect_leaks=0 时只覆盖实际执行路径、
//      不检出泄漏，不得外推为「全生命周期无 UAF/已完成收口覆盖」。
//
// 未覆盖（如实，不冒充验收）：无法用当前公开/内部 API 从 RPC 路径取回
//   RpcServer 背后的 http_detail::HttpServerImpl（RpcServerImpl 位于
//   src/rpc/RpcRuntimeImpl.cc 匿名 namespace，无 backend 访问器；加该访问器属新增
//   生产 seam，本次授权不含）。故「RPC server->Close() 返回当刻其 HTTP owner 的
//   已登记会话数 ==0 且 io 域封口」这一更深排序**未在本用例直接断言**；本用例只
//   断言 owner 子对象在 Close 返回当刻已反登记（这是可观测的等价上限）。该排序由
//   HTTP 侧 tests/Test_http_session_lifetime_probe.cc 覆盖，这里不重复也不外推。
//
// 无第三方测试框架依赖（Boost.Test header-only，同既有 RPC 套件）；所有等待都有界
//   （有条件预算的谓词轮询，不用 sleep 驱动时序）；协程任务完成态放堆上共享对象，
//   失败路径 RAII 先 Release gate 再 Close runtime，不留悬垂引用（协程没有 join，
//   任务态由堆上共享对象持有，任务退出即释放）。

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <arpa/inet.h>
#include <cerrno>
#include <dirent.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

#include <bbt/infra/NetworkRuntime.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/RpcClient.hpp>
#include <bbt/infra/RpcServer.hpp>

// 生产始终存在的 runtime 内部只读探针（UnclosedChildrenForTest）；src 内部头
// 仅本目标可见（tests/CMakeLists.txt 里 PRIVATE ${CMAKE_SOURCE_DIR}/src）。
#include "http/NetworkRuntimeImpl.hpp"

#ifdef BBT_INFRA_STRINGENT_DEBUG
// 独立 debug 观测组件（仅 Debug+STRINGENT 构建存在）。
#include "debug/InfraDebug.hpp"
namespace dbg = bbt::infra::debug;
#endif

using namespace bbt::infra;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

namespace {

// 单次 Call 期限：必须覆盖「server drain(≤5s) + client Close + 少量检查」，
// 让在途 Call 到 client Close 之后才由连接断开决定终态（否则期限先到会改语义）。
constexpr int kCallDeadlineMs = 30000;
// gated handler 显式放行前的挂起上界：严格大于「server drain(≤5s) + client
// Close(≤5s) + 在途等待(≤kGateWaitMs=20s) + echo 析构等待(≤kPollMs=8s) + 余量」
// 的整段合法耗时，确保正常路径下由测试显式 Release 放行而非内部超时自放行
// （Hold 记录并暴露自放行，见 Gate::hold_timed_out）。
constexpr int kGateHoldMs     = 60000;
constexpr int kGateWaitMs     = 20000;  // WaitEntered / WaitReady 等待上限（非完成依据）
constexpr int kPollMs         = 8000;   // 收口 / 析构 / socket 回落有界等待

void EnsureScheduler() {
    auto& scheduler = *bbt::coroutine::detail::Scheduler::GetInstance();
    if (!scheduler.IsInitialized()) {
        auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
        // 4 调度线程：gated handler 阻塞一个调度线程等待释放，其余继续推进
        // io 域（完成项/回投），避免自持死锁（与既有 HTTP/RPC 套件一致）。
        cfg->m_cfg_static_thread_num = 4;
        cfg->m_cfg_stack_size        = 256 * 1024;
        scheduler.Start(SCHE_START_OPT_SCHE_THREAD);
    }
    BOOST_REQUIRE(scheduler.IsInitialized());
}

NetworkLimits MakeLimits(std::size_t max_conn, std::size_t max_inflight) {
    NetworkLimits limits{};
    limits.max_connections  = max_conn;
    limits.max_inflight     = max_inflight;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = 64 * 1024;
    limits.incoming_timeout = std::chrono::milliseconds{30000};
    return limits;
}

std::shared_ptr<NetworkRuntime> MakeRuntime() {
    auto created = NetworkRuntime::Create(MakeLimits(8, 8));
    BOOST_REQUIRE(created);
    auto runtime = std::move(created).value();
    BOOST_REQUIRE(runtime->Start());
    return runtime;
}

// runtime 的 HTTP 切片实现（探针入口）。dynamic cast 失败即用例失败。
std::shared_ptr<http_detail::NetworkRuntimeImpl> RuntimeImpl(
    const std::shared_ptr<NetworkRuntime>& runtime) {
    return std::dynamic_pointer_cast<http_detail::NetworkRuntimeImpl>(runtime);
}

template <class Pred>
bool WaitUntil(Pred pred, int budget_ms) {
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(budget_ms);
    for (;;) {
        if (pred())
            return true;
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::yield();
    }
}

// 物理 fd 观测：/proc/self/fd 中目标为 socket: 的项（eventfd/epoll/timerfd 是
// runtime 自身基建，不计入）。本用例无测试自持客户端 fd，命中项均为 runtime 持有。
// 返回 -1 = 读取失败（须显式检查，不得当 0）。
int CountSockets() {
    DIR* d = ::opendir("/proc/self/fd");
    if (!d)
        return -1;
    int     n = 0;
    dirent* e = nullptr;
    while ((e = ::readdir(d)) != nullptr) {
        char path[320];
        char buf[320];
        std::snprintf(path, sizeof(path), "/proc/self/fd/%s", e->d_name);
        const ssize_t len = ::readlink(path, buf, sizeof(buf) - 1);
        if (len <= 0)
            continue;
        buf[len] = '\0';
        if (std::strncmp(buf, "socket:", 7) == 0)
            ++n;
    }
    ::closedir(d);
    return n;
}

// 端口探针：区分 socket() 失败与 connect() 失败，connect errno 在 close 之前保存；
// 正常建立的探针 fd 由 RAII 析构关闭（失败/早退不泄漏）。这样「端口被拒」必须
// 落在 connect 阶段且 errno==ECONNREFUSED，EMFILE/ENFILE 等 socket 阶段失败不会
// 被冒充成拒绝。
struct PortProbe {
    enum class Phase { SocketFailed, ConnectFailed, Connected };
    Phase phase{Phase::SocketFailed};
    int   err{0};   // 失败阶段的 errno（ConnectFailed 时为 connect 的 errno）
    int   fd{-1};   // 仅 Connected 时持有（RAII close）

    PortProbe() = default;
    PortProbe(const PortProbe&)            = delete;
    PortProbe& operator=(const PortProbe&) = delete;
    PortProbe(PortProbe&& o) noexcept : phase(o.phase), err(o.err), fd(o.fd) {
        o.fd = -1;
    }
    ~PortProbe() {
        if (fd >= 0)
            ::close(fd);
    }
};

PortProbe ProbeConnect(std::uint16_t port) {
    PortProbe p;
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        p.phase = PortProbe::Phase::SocketFailed;
        p.err   = errno;
        return p;
    }
    timeval tv{2, 0};
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    errno = 0;
    if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        p.phase = PortProbe::Phase::ConnectFailed;
        p.err   = errno;   // 在 close 之前保存
        ::close(fd);
        return p;
    }
    p.phase = PortProbe::Phase::Connected;
    p.fd    = fd;
    return p;
}

// 协程任务的堆上完成态：任务只写自己 shared_ptr 持有的对象（闭包按值持有 shared
// 延续引用，不捕获借用）。
template <class T>
struct CoRunState {
    bbt::core::thread::CountDownLatch done{1};
    std::optional<T>                 value;
    std::atomic_bool                 ok{false};
};

template <class F, class T = std::invoke_result_t<F>>
std::shared_ptr<CoRunState<T>> SpawnCoTask(F&& f) {
    auto state = std::make_shared<CoRunState<T>>();
    bool succ  = false;
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [fn = std::forward<F>(f), state]() mutable {
            state->value.emplace(fn());
            state->ok.store(true, std::memory_order_release);
            state->done.Down();
        },
        succ);
    if (!succ)
        return nullptr;
    return state;
}

struct PendingCall {
    std::shared_ptr<CoRunState<result<RpcEnvelope>>> state;

    void Spawn(const std::shared_ptr<RpcClient>& client, RpcAddress address,
               RpcEnvelope request, CallOptions options) {
        state = SpawnCoTask(
            [client, address, request = std::move(request), options]() mutable {
                return client->Call(address, std::move(request), options);
            });
        BOOST_REQUIRE(state != nullptr);
    }
    bool Ready() const {
        return state != nullptr && state->ok.load(std::memory_order_acquire);
    }
    bool WaitReady(int budget_ms = kGateWaitMs) {
        return WaitUntil([this] { return Ready(); }, budget_ms);
    }
    const std::optional<result<RpcEnvelope>>& out() const { return state->value; }
};

CallOptions DeadlineAfter(int budget_ms) {
    CallOptions opt;
    opt.deadline = std::chrono::steady_clock::now() +
                   std::chrono::milliseconds(budget_ms);
    return opt;
}

RpcEnvelope MakeRequest(std::string id, std::string method) {
    RpcEnvelope request;
    request.request_id      = std::move(id);
    request.service         = "svc.life";
    request.method          = std::move(method);
    request.request_schema  = "bbt.life.Req/v1";
    request.response_schema = "bbt.life.Resp/v1";
    request.payload         = {'p', 'i', 'n', 'g'};
    return request;
}

// 受控 gate：gated handler 进入后挂起，测试线程显式 Release（有界）。
// Hold() 返回「是否由显式 Release 放行」并记录内部超时自放行次数——让「handler
// 自己超时返回」这一错误归因可被断言捕获，不再静默。
struct Gate {
    std::mutex              mtx;
    std::condition_variable cv;
    bool                    entered{false};
    bool                    released{false};
    std::atomic_int         calls{0};
    std::atomic_int         exits{0};
    std::atomic_int         released_by_test{0};  // Hold 被显式 Release 放行的次数
    std::atomic_int         hold_timed_out{0};    // Hold 内部超时自放行的次数

    void Enter() {
        {
            std::lock_guard<std::mutex> lk(mtx);
            entered = true;
        }
        cv.notify_all();
    }
    bool WaitEntered(int budget_ms) {
        std::unique_lock<std::mutex> lk(mtx);
        return cv.wait_for(lk, std::chrono::milliseconds(budget_ms),
                           [this] { return entered; });
    }
    bool Released() {
        std::lock_guard<std::mutex> lk(mtx);
        return released;
    }
    // 返回 true = 被测试显式 Release 放行；false = 内部超时自放行（失败归因信号）。
    bool Hold() {
        std::unique_lock<std::mutex> lk(mtx);
        const bool by_release = cv.wait_for(
            lk, std::chrono::milliseconds(kGateHoldMs),
            [this] { return released; });
        if (!by_release)
            hold_timed_out.fetch_add(1);
        return by_release;
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

// RAII：任何失败/早退路径也 Close runtime（不依赖后续显式 Close；Close 幂等）。
// 声明位置在实际 gate guard 之前 → 析构晚于 gate guard → 失败清理顺序为
// 「先 Release gate，再 Close runtime」（逆序析构自证）。
struct RuntimeCloseGuard {
    std::shared_ptr<NetworkRuntime> runtime;
    ~RuntimeCloseGuard() {
        if (runtime)
            runtime->Close();
    }
};

// RPC handler：method=="Gate" 时挂起在 gate 上（受控在途）；其余立即 echo。
RpcHandler MakeHandler(const std::shared_ptr<Gate>& gate) {
    return [gate](IncomingCallContext, RpcEnvelope request)
               -> result<RpcEnvelope> {
        gate->calls.fetch_add(1);
        if (request.method == "Gate") {
            gate->Enter();
            if (gate->Hold())
                gate->released_by_test.fetch_add(1);
        }
        gate->exits.fetch_add(1);
        return result<RpcEnvelope>::ok(std::move(request));
    };
}

} // namespace

BOOST_AUTO_TEST_SUITE(rpc_runtime_lifetime)

// 2-5（任务项 1）：真 RPC 工厂 → HTTP owner 子对象绑定 0→1→0。
BOOST_AUTO_TEST_CASE(t_rpc_owner_binding_zero_one_zero) {
    EnsureScheduler();
    auto runtime = MakeRuntime();
    RuntimeCloseGuard runtime_guard{runtime};
    auto impl    = RuntimeImpl(runtime);
    BOOST_REQUIRE(impl != nullptr);

    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 0u);

    auto client_result = runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);                 // 前置成功
    auto client = std::move(client_result).value();
    BOOST_CHECK(client != nullptr);
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 1u);  // 0→1（client owner）

    auto server_result = runtime->ListenRpc({"127.0.0.1", 0}, MakeHandler(
        std::make_shared<Gate>()));
    BOOST_REQUIRE(server_result);                 // 前置成功
    auto server = std::move(server_result).value();
    BOOST_CHECK(server != nullptr);
    BOOST_CHECK_NE(server->LocalAddress().port, 0);
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 2u);  // 1→2（+server owner）

    client->Close();
    BOOST_CHECK(client->IsClosed());
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 1u);  // 2→1
    client->Close();                              // 幂等
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 1u);

    server->Close();
    BOOST_CHECK(server->IsClosed());
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 0u);  // 1→0
    server->Close();                              // 幂等
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 0u);

    runtime->Close();
    BOOST_CHECK(runtime->IsClosed());
}

// 2-5（任务项 2 + debug 面）：真会话 + 在途 gated handler 与 client/server Close。
BOOST_AUTO_TEST_CASE(t_rpc_inflight_close_lifetime) {
    EnsureScheduler();

    // M5：runtime 建立前的 socket 基线（零容差落定判据）。读取失败必须显式失败，
    // 不得当 0 继续。
    const int socks_baseline = CountSockets();
    BOOST_REQUIRE_MESSAGE(socks_baseline >= 0,
                          "CountSockets 基线读取失败（/proc/self/fd 不可读）");

#ifdef BBT_INFRA_STRINGENT_DEBUG
    const int destroyed_base = dbg::HttpSessionDestroyed();
    const int tripwire_base  = dbg::HttpSessionDestroyedWhileRegistered();
    std::fprintf(stderr, "rpc-lifetime: debug surface (counters enabled)\n");
#else
    std::fprintf(stderr, "rpc-lifetime: release surface (no debug counters)\n");
#endif

    auto runtime = MakeRuntime();
    // runtime Close RAII（声明早于 gate guard → 析构晚于它 → 失败先 Release 再 Close）。
    RuntimeCloseGuard runtime_guard{runtime};
    auto impl    = RuntimeImpl(runtime);
    BOOST_REQUIRE(impl != nullptr);
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 0u);

    auto gate = std::make_shared<Gate>();
    GateReleaseGuard gate_guard{gate};   // 失败/早退也释放 gate

    auto client_result = runtime->CreateRpcClient();
    BOOST_REQUIRE(client_result);
    auto client = std::move(client_result).value();
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 1u);

    auto server_result = runtime->ListenRpc({"127.0.0.1", 0}, MakeHandler(gate));
    BOOST_REQUIRE(server_result);
    auto server = std::move(server_result).value();
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 2u);
    const std::uint16_t port = server->LocalAddress().port;
    BOOST_CHECK_NE(port, 0);
    const RpcAddress address{"http", "127.0.0.1:" + std::to_string(port)};

    // (A) 真会话建立：一次成功的 echo Call（服务端 handler 真被派发）。
    PendingCall echo;
    echo.Spawn(client, address, MakeRequest("life-1", "Echo"),
               DeadlineAfter(kCallDeadlineMs));
    BOOST_REQUIRE(echo.WaitReady());
    BOOST_REQUIRE(echo.out().has_value());
    BOOST_REQUIRE(echo.out().value());           // 成功 = 真实会话闭环
    BOOST_CHECK_EQUAL(echo.out()->value().request_id, "life-1");
    BOOST_CHECK_EQUAL(gate->calls.load(), 1);

    // (B) 受控在途：末次请求停在 gate 上（handler 已进入 = 请求已完整到达）。
    PendingCall in_flight;
    in_flight.Spawn(client, address, MakeRequest("life-2", "Gate"),
                    DeadlineAfter(kCallDeadlineMs));
    BOOST_REQUIRE(gate->WaitEntered(kGateWaitMs));
    BOOST_CHECK_EQUAL(gate->calls.load(), 2);

    const int socks_before = CountSockets();

    // (C) 在途 handler 期间同步 Close server，再 Close client。
    //     与既有 fixture 一致：Close 不抢占已接纳 handler，有界返回。
    long long server_close_ms = -1;
    bool      server_closed   = false;
    int       unclosed_at_server_close = -1;
    int       socks_after_server       = -1;
    // 端口探针结果（区分 socket/connect 阶段与 errno）。
    PortProbe::Phase probe_phase{PortProbe::Phase::SocketFailed};
    int              probe_errno = 0;
    {
        const auto t0 = std::chrono::steady_clock::now();
        server->Close();
        server_close_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - t0)
                              .count();
        server_closed = server->IsClosed();
        unclosed_at_server_close =
            static_cast<int>(impl->UnclosedChildrenForTest());
        socks_after_server = CountSockets();
        // listener fd 已物理释放：端口新连接在 connect 阶段失败且 errno==ECONNREFUSED。
        PortProbe probe = ProbeConnect(port);
        probe_phase = probe.phase;
        probe_errno = probe.err;
        // probe 析构在此作用域出口：Connected 时其 fd 由 RAII 关闭。
    }
    // Close 返回当刻：server 已反登记（client 仍登记），端口拒新连接（connect 阶段
    // + ECONNREFUSED），物理 socket 回收 >= 在途会话 + listener。
    BOOST_CHECK(server_closed);
    BOOST_CHECK_EQUAL(unclosed_at_server_close, 1);   // server owner 反登记
    BOOST_CHECK_MESSAGE(probe_phase != PortProbe::Phase::SocketFailed,
                        "端口探针 socket() 失败（如 EMFILE/ENFILE），非端口拒绝");
    BOOST_CHECK(probe_phase == PortProbe::Phase::ConnectFailed);  // connect 调用失败
    BOOST_CHECK_EQUAL(probe_errno, ECONNREFUSED);                 // 且为 ECONNREFUSED
    BOOST_CHECK(server_close_ms >= 0 && server_close_ms < 10000);  // 有界
    BOOST_CHECK(socks_before > 0);
    BOOST_CHECK(socks_after_server >= 0);
    if (socks_before > 0 && socks_after_server >= 0)
        BOOST_CHECK(socks_before - socks_after_server >= 2);

    client->Close();
    BOOST_CHECK(client->IsClosed());
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 0u);  // 全部反登记

    // (D) 在途 Call 的终态：请求已完整写出/提交，故只能是未知事实 +
    //     RequestCommitted（依时序正交的确定口径，不放宽成任意错误）。
    BOOST_REQUIRE(in_flight.WaitReady());
    BOOST_REQUIRE(in_flight.out().has_value());
    BOOST_REQUIRE(!in_flight.out().value());
    BOOST_CHECK(in_flight.out()->error().code == ErrorCode::OutcomeUnknown);
    BOOST_REQUIRE(in_flight.out()->error().request_phase.has_value());
    BOOST_CHECK(*in_flight.out()->error().request_phase ==
                RequestPhase::RequestCommitted);

    // (E) 放行真正晚到 handler（须为显式 Release，且返回前 gate 未自放行）。
#ifdef BBT_INFRA_STRINGENT_DEBUG
    // M1 归因前提：此刻 gated 会话仍被 handler 协程强持有（RunHandler 未返回），
    // 故必然未析构。先有界等 echo 会话析构——该增量只可能来自 echo。同时断言 gate
    // 未显式放行且 gated handler 未退出（退出数仍为 1），即 hold 未超时自放行。
    BOOST_CHECK_MESSAGE(!gate->Released(), "gate 在显式 Release 前已被放行");
    BOOST_CHECK_EQUAL(gate->exits.load(), 1);       // 仅 echo handler 退出
    BOOST_CHECK(WaitUntil(
        [&] { return dbg::HttpSessionDestroyed() - destroyed_base >= 1; },
        kPollMs));
    const int destroyed_base2 = dbg::HttpSessionDestroyed();
    const int tripwire_base2  = dbg::HttpSessionDestroyedWhileRegistered();
#endif
    gate->Release();
    BOOST_CHECK(WaitUntil([&] { return gate->exits.load() >= 2; }, kPollMs));
    // handler 必须由显式 Release 放行，不能是内部超时自放行。
    BOOST_CHECK_EQUAL(gate->released_by_test.load(), 1);
    BOOST_CHECK_EQUAL(gate->hold_timed_out.load(), 0);

    // (F) runtime 收口 + socket 回落至基线（自有进程 socket，无测试自持 fd）。
    runtime->Close();
    BOOST_CHECK(runtime->IsClosed());
    BOOST_CHECK_EQUAL(impl->UnclosedChildrenForTest(), 0u);
    BOOST_CHECK(WaitUntil([&] { return CountSockets() == socks_baseline; },
                          kPollMs));
    BOOST_CHECK_EQUAL(CountSockets(), socks_baseline);   // 零容差，无 1fd 掩盖

    std::fprintf(stderr,
                 "rpc-lifetime: sockets baseline=%d before=%d after_server=%d "
                 "close_ms=%lld probe_errno=%d probe_phase=%d "
                 "hold_released_by_test=%d hold_timeout=%d\n",
                 socks_baseline, socks_before, socks_after_server,
                 server_close_ms, probe_errno, static_cast<int>(probe_phase),
                 gate->released_by_test.load(), gate->hold_timed_out.load());

#ifdef BBT_INFRA_STRINGENT_DEBUG
    // M1 最终条件：Release 后「再等到」的 destroyed 增量（相对第二基线，echo 已在
    // 其之前析构）只能归因于晚到 gated 会话真实退出并析构；其 tripwire 增量必须为 0。
    BOOST_CHECK(WaitUntil(
        [&] { return dbg::HttpSessionDestroyed() - destroyed_base2 >= 1; },
        kPollMs));
    const int echo_destroyed_delta   = destroyed_base2 - destroyed_base;
    const int gated_destroyed_delta  = dbg::HttpSessionDestroyed() - destroyed_base2;
    const int tripwire_late_delta =
        dbg::HttpSessionDestroyedWhileRegistered() - tripwire_base2;
    const int tripwire_delta =
        dbg::HttpSessionDestroyedWhileRegistered() - tripwire_base;
    std::fprintf(stderr,
                 "rpc-lifetime[debug]: echo_destroyed_delta=%d "
                 "gated_destroyed_delta=%d tripwire_late_delta=%d "
                 "tripwire_base_delta=%d\n",
                 echo_destroyed_delta, gated_destroyed_delta,
                 tripwire_late_delta, tripwire_delta);
    BOOST_CHECK(echo_destroyed_delta >= 1);   // echo 会话确实析构（第一段增量）
    BOOST_CHECK(gated_destroyed_delta >= 1);  // 晚到 gated 会话真实退出并析构
    BOOST_CHECK(tripwire_late_delta == 0);    // 晚到会话窗口无「仍登记即析构」
    BOOST_CHECK(tripwire_delta == 0);         // 整体基线：无「仍登记即析构」
#endif
}

BOOST_AUTO_TEST_SUITE_END()
