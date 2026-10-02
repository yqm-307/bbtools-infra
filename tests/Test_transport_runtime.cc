#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

// P2 transport-only 消费验收：本文件只包含 transport 公共面与 coroutine/core
// 基础头，不包含任何 HTTP 头（HttpClient.hpp/HttpServer.hpp/NetworkRuntime.hpp），
// 目标只链接 bbt::infra_transport。因此它同时是「受管 TCP/UDP 消费者无需
// HTTP」的编译期证据，而不是只靠目录命名或注释声明。
//
// 覆盖（P2 验收要求：不只测静态容量超限）：
//   1. owner 门禁与参数校验（Start 前拒绝、Start 后参数非法）
//   2. 容量名额（max_connections）超限与「物理关闭后才归还」
//   3. 真实 loopback 收发 + 完整关闭生命周期（Close 同步物理收口）
//   4. 多 owner 配额隔离（同一进程两个 TransportRuntime 各自 max_inflight）
//   5. 关闭期已接纳但未落定的 op（挂起 Accept）——名额归还 + 物理收口
//
// 新契约（进程寿命运行时）：没有 Scheduler::Stop/restart、没有 WaitClosed。
// 每个测试可执行文件只初始化一次 runtime，用例之间靠进程边界隔离；用例结束
// 前显式 Close() 并断言物理收口（IsClosed / 名额归零），不再以 Stop 做隔离。
//
// 失败路径约定（独立复审 🔴-1 / 🟡-1 / 🟡-2 修复）：断言失败不得被放大成
// SegFault，等待不得依赖脆弱的固定短预算。为此本文件遵守两条约束：
//   a) 协程/回调触碰的一切共享状态都堆分配并按 shared_ptr 持有。Boost.Test 的
//      BOOST_REQUIRE* 失败会抛异常展开用例栈；若协程仍按引用写这些栈对象，
//      就是一个 use-after-free（记录中的 SegFault 即此机制）。
//   b) 每个用例持有 CaseGuard：正常结束与断言失败走同一条收口路径（关闭本用例
//      创建的受管对象）。
// 等待预算：wait-entry gate 是确定性事件接缝（名额登记后由协程同步触发），
// 实测重负载（load 14 / load 58，12 核）下延迟 ≤5ms，故 kSeamBudgetMs=5000
// 作为兜底上限（约 1000x 观测上界）；仍超时则由断言信息区分两种原因。

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>
#include <bbt/infra/CoTCP.hpp>
#include <bbt/infra/CoUDP.hpp>
#include <bbt/infra/TransportRuntime.hpp>

// 内部装配面（src 内部头，不安装）：owner 测试探针与等待入口 gate。
#include "detail/TransportWiring.hpp"

using bbt::infra::CallOptions;
using bbt::infra::ConstBytes;
using bbt::infra::ErrorCode;
using bbt::infra::MutableBytes;
using bbt::infra::NetworkLimits;
using bbt::infra::SocketAddress;
using bbt::infra::TcpEndpoint;
using bbt::infra::TransportRuntime;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

using bbt::infra::detail::TransportWiring;

namespace {

// 运行时只初始化一次（函数局部静态保证线程安全且恰好一次）。
void EnsureRuntime() {
    static const bool initialized = [] {
        auto& scheduler = bbt::coroutine::detail::Scheduler::GetInstance();
        if (!scheduler->IsInitialized())
            scheduler->Start(SCHE_START_OPT_SCHE_THREAD);
        return scheduler->IsInitialized();
    }();
    BOOST_REQUIRE(initialized);
}

NetworkLimits Limits(std::size_t max_connections, std::size_t max_inflight) {
    NetworkLimits limits{};
    limits.max_connections = max_connections;
    limits.max_inflight    = max_inflight;
    limits.max_header_bytes = 16 * 1024;
    limits.max_body_bytes   = 64 * 1024;
    limits.incoming_timeout = std::chrono::milliseconds{3000};
    return limits;
}

CallOptions Options(int timeout_ms = 5000) {
    CallOptions options;
    options.deadline = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds(timeout_ms);
    return options;
}

// 等待预算（见文件头「等待预算」）。
constexpr int kSeamBudgetMs = 5000;

// 用例级观测点：整体堆分配，协程与 gate 回调按 shared_ptr 持有。断言失败
// 展开用例栈后，挂起协程的写入仍落在存活对象上（不引用已析构栈对象）。
struct Waiter {
    bbt::core::thread::CountDownLatch entered{1}; // wait-entry gate 已落定
    bbt::core::thread::CountDownLatch settled{1}; // op 已落定
    std::atomic_int code{-1};                     // -2=正常返回，-1=未落定
};
using WaiterPtr = std::shared_ptr<Waiter>;

// 用例级收口：正常返回与断言失败（Boost fatal error 抛出）走同一条路径。
// 新契约没有 runtime 停机入口，只做本用例受管对象的 Close。
class CaseGuard {
public:
    CaseGuard() = default;
    CaseGuard(const CaseGuard&) = delete;
    CaseGuard& operator=(const CaseGuard&) = delete;

    ~CaseGuard() { Unwind(); }

    // 登记收口动作（按值持有 shared_ptr，被登记对象保活到收口执行完）。
    void OnUnwind(std::function<void()> fn) { m_on_unwind.push_back(std::move(fn)); }

    void Unwind() {
        if (m_unwound)
            return;
        m_unwound = true;
        for (auto it = m_on_unwind.rbegin(); it != m_on_unwind.rend(); ++it)
            (*it)();
    }

private:
    std::vector<std::function<void()>> m_on_unwind;
    bool m_unwound{false};
};

// 协程内执行任务并限时等待；任何一步失败返回 false（测试不得无限挂住）。
// latch 堆分配并随协程闭包保活：超时返回后协程再 Down() 不会写已析构栈对象。
template <class F>
bool RunInCoroutine(F&& f, int budget_ms = kSeamBudgetMs) {
    auto done = std::make_shared<bbt::core::thread::CountDownLatch>(1);
    bool succ = false;
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [fn = std::forward<F>(f), done]() mutable {
            try {
                fn();
            } catch (...) {
                // 任务内异常也必须落定等待方，否则调用点只会看到「超时」。
            }
            done->Down();
        },
        succ);
    if (!succ)
        return false;
    return done->WaitTimeout(budget_ms) == 0;
}

// wait-entry gate 回调只持 shared_ptr<Waiter>：用例栈展开后回调仍安全。
template <class Listener>
void InstallWaitEntryGate(const std::shared_ptr<Listener>& listener,
                          const WaiterPtr& waiter) {
    TransportWiring::SetWaitEntryGateForTest(*listener, [waiter] { waiter->entered.Down(); });
}

// 可等待 op 的结果码语义：-2 = 正常返回（未预期）。
constexpr int kOpSucceeded = -2;

} // namespace

BOOST_AUTO_TEST_SUITE(transport_runtime)

// ---------------------------------------------------------------------------
// 1. owner 门禁：Create 只校验装配参数，Start 才放开工厂；参数非法在入口
//    快返 InvalidArgument，且发生在容量预留之前。
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(t_owner_gate_and_param_validation) {
    EnsureRuntime();
    CaseGuard guard;

    auto created = TransportRuntime::Create(Limits(1, 4));
    BOOST_REQUIRE(created);
    auto owner = std::move(created).value();
    BOOST_REQUIRE(owner);
    guard.OnUnwind([owner] { owner->Close(); });

    // 未 Start：不产出半成品对象。
    auto before = owner->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(!before);
    BOOST_CHECK_EQUAL(static_cast<int>(before.error().code),
                      static_cast<int>(ErrorCode::RuntimeUnavailable));

    BOOST_REQUIRE(owner->Start());
    // 二次 Start：同一次装配只成功启动一次。
    auto again = owner->Start();
    BOOST_REQUIRE(!again);
    BOOST_CHECK_EQUAL(static_cast<int>(again.error().code),
                      static_cast<int>(ErrorCode::InvalidArgument));

    auto bad_backlog = owner->ListenTCP(SocketAddress{"127.0.0.1", 0}, 0);
    BOOST_REQUIRE(!bad_backlog);
    BOOST_CHECK_EQUAL(static_cast<int>(bad_backlog.error().code),
                      static_cast<int>(ErrorCode::InvalidArgument));
    auto bad_ip = owner->ListenTCP(SocketAddress{"", 0}, 16);
    BOOST_REQUIRE(!bad_ip);
    BOOST_CHECK_EQUAL(static_cast<int>(bad_ip.error().code),
                      static_cast<int>(ErrorCode::InvalidArgument));
    // 参数拒绝不消耗容量：max_connections=1 下随后仍可成功绑定。
    auto numeric = owner->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(numeric);
    BOOST_REQUIRE_NE(numeric.value()->LocalAddress().port, 0);

    // DialTCP 参数校验（在协程内调用，与真实调用位置一致）。
    bool host_checked = false;
    bool port_checked = false;
    const bool ran = RunInCoroutine([&] {
        auto no_host = owner->DialTCP(TcpEndpoint{"", 80}, Options());
        host_checked = !no_host &&
            no_host.error().code == ErrorCode::InvalidArgument;
        auto no_port = owner->DialTCP(TcpEndpoint{"127.0.0.1", 0}, Options());
        port_checked = !no_port &&
            no_port.error().code == ErrorCode::InvalidArgument;
    });
    BOOST_REQUIRE(ran);
    BOOST_CHECK(host_checked);
    BOOST_CHECK(port_checked);

    // 显式收口 + 物理收口断言（Close 返回即资源已释放）。
    numeric.value()->Close();
    BOOST_CHECK(numeric.value()->IsClosed());
    guard.Unwind();
    BOOST_CHECK(owner->IsClosed());
}

// ---------------------------------------------------------------------------
// 2. 容量名额（max_connections）：listener/accepted/dialed/UDP 各算一个名额；
//    超限 Overloaded、不排队；**物理关闭落定后才归还**，逻辑超时不提前归还。
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(t_capacity_gate_and_physical_release) {
    EnsureRuntime();
    CaseGuard guard;

    auto created = TransportRuntime::Create(Limits(2, 8));
    BOOST_REQUIRE(created);
    auto owner = std::move(created).value();
    guard.OnUnwind([owner] { owner->Close(); });
    BOOST_REQUIRE(owner->Start());

    auto listen = owner->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen);
    auto listen2 = owner->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen2);
    const auto port = listen.value()->LocalAddress().port;
    BOOST_REQUIRE_NE(port, 0);

    // 容量 2 已满：第三个资源（TCP/UDP 任一路径）都立即 Overloaded。
    auto overflow_listen = owner->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(!overflow_listen);
    BOOST_CHECK_EQUAL(static_cast<int>(overflow_listen.error().code),
                      static_cast<int>(ErrorCode::Overloaded));
    auto overflow_udp = owner->BindUDP(SocketAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(!overflow_udp);
    BOOST_CHECK_EQUAL(static_cast<int>(overflow_udp.error().code),
                      static_cast<int>(ErrorCode::Overloaded));
    bool dial_overloaded = false;
    const bool ran = RunInCoroutine([&] {
        auto dialed = owner->DialTCP(TcpEndpoint{"127.0.0.1", port}, Options());
        dial_overloaded = !dialed &&
            dialed.error().code == ErrorCode::Overloaded;
    });
    BOOST_REQUIRE(ran);
    BOOST_CHECK(dial_overloaded);
    BOOST_CHECK_EQUAL(TransportWiring::InflightQuotaHeldForTest(*owner), 0u);

    // 物理关闭落定 → 名额归还 → 同一条路径再次成功（不是只读计数）。
    listen.value()->Close();
    BOOST_REQUIRE(listen.value()->IsClosed());
    auto udp = owner->BindUDP(SocketAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(udp);
    BOOST_REQUIRE_NE(udp.value()->LocalAddress().port, 0);
    udp.value()->Close();
    BOOST_REQUIRE(udp.value()->IsClosed());

    listen2.value()->Close();
    BOOST_CHECK(listen2.value()->IsClosed());
    guard.Unwind();
    BOOST_CHECK(owner->IsClosed());
}

// ---------------------------------------------------------------------------
// 3. 真实 loopback 收发 + 完整关闭生命周期：受管连接可读写，owner Close 后
//    交付对象物理关闭（IsClosed）、托管引用随物理关闭释放、工厂随后拒绝。
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(t_loopback_data_path_and_close_lifecycle) {
    EnsureRuntime();
    CaseGuard guard;

    auto created = TransportRuntime::Create(Limits(4, 8));
    BOOST_REQUIRE(created);
    auto owner = std::move(created).value();
    guard.OnUnwind([owner] { owner->Close(); });
    BOOST_REQUIRE(owner->Start());

    auto listen = owner->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen);
    const auto port = listen.value()->LocalAddress().port;
    BOOST_REQUIRE_NE(port, 0);

    // 数据面观测点堆分配：两个协程与断言共享，栈展开后写入仍合法。
    struct DataProbe {
        bbt::core::thread::CountDownLatch done{2};
        std::atomic_bool server_ok{false};
        std::atomic_bool client_ok{false};
        std::shared_ptr<bbt::infra::CoTCP> accepted;
    };
    auto probe = std::make_shared<DataProbe>();

    bbtco [listen, probe]() {
        auto acc = listen.value()->Accept(Options());
        if (!acc) { probe->done.Down(); return; }
        auto accepted = std::move(acc).value();
        char request[5]{};
        auto read = accepted->ReadSome(bbt::infra::MutableBytes{request, sizeof(request)}, Options());
        if (!read || read.value().bytes != 5) { probe->done.Down(); return; }
        const char response[] = "world";
        auto write = accepted->WriteAll(bbt::infra::ConstBytes{response, 5}, Options());
        probe->server_ok.store(write && write.value().bytes == 5);
        probe->accepted = std::move(accepted);
        probe->done.Down();
    };

    bbtco [owner, port, probe]() {
        auto dialed = owner->DialTCP(TcpEndpoint{"127.0.0.1", port}, Options());
        if (!dialed) { probe->done.Down(); return; }
        auto client = std::move(dialed).value();
        const char request[] = "hello";
        auto write = client->WriteAll(bbt::infra::ConstBytes{request, 5}, Options());
        char response[5]{};
        auto read = client->ReadSome(bbt::infra::MutableBytes{response, sizeof(response)}, Options());
        probe->client_ok.store(write && write.value().bytes == 5 && read &&
                               read.value().bytes == 5 &&
                               std::string(response, 5) == "world");
        // 受管引用在关闭后仍可用于查询（§4.0.1.2）。
        client->Close();
        probe->done.Down();
    };

    BOOST_REQUIRE_MESSAGE(probe->done.WaitTimeout(10000) == 0,
                          "case3: loopback 收发未在 10000ms 内落定"
                          "（server_ok=" << probe->server_ok.load()
                          << " client_ok=" << probe->client_ok.load() << "）");
    BOOST_CHECK(probe->server_ok.load());
    BOOST_CHECK(probe->client_ok.load());
    BOOST_REQUIRE(probe->accepted);
    // 数据面在途 op 已结束：账本归零，没有残留名额。
    BOOST_CHECK_EQUAL(TransportWiring::InflightQuotaHeldForTest(*owner), 0u);

    // owner 关闭：已接纳连接被收口（不仅停 listener），Close 返回即物理收口。
    std::weak_ptr<bbt::infra::CoTCP> accepted_weak = probe->accepted;
    owner->Close();
    BOOST_CHECK(probe->accepted->IsClosed());
    BOOST_CHECK(owner->IsClosed());
    // 托管引用随物理关闭释放（不是靠外部 reset 才消失）。
    probe->accepted.reset();
    BOOST_CHECK(accepted_weak.expired());

    // 关闭后工厂拒绝：已封口 → Closed（不是 RuntimeUnavailable/Overloaded）。
    auto after = owner->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(!after);
    BOOST_CHECK_EQUAL(static_cast<int>(after.error().code),
                      static_cast<int>(ErrorCode::Closed));
    guard.Unwind();
}

// ---------------------------------------------------------------------------
// 4. 配额隔离：同一进程两个 TransportRuntime 各自持有 max_inflight 预算，
//    一个 owner 的挂起 op 占满自己的账本时不会影响另一个 owner。
//    （Issue #37「多 Runtime 配额隔离」在 transport 侧的直接证据。）
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(t_quota_isolated_between_owners) {
    EnsureRuntime();
    CaseGuard guard;

    auto a_created = TransportRuntime::Create(Limits(4, 1));
    auto b_created = TransportRuntime::Create(Limits(4, 1));
    BOOST_REQUIRE(a_created);
    BOOST_REQUIRE(b_created);
    auto a = std::move(a_created).value();
    auto b = std::move(b_created).value();
    guard.OnUnwind([a] { a->Close(); });
    guard.OnUnwind([b] { b->Close(); });
    BOOST_REQUIRE(a->Start());
    BOOST_REQUIRE(b->Start());

    auto listen_a = a->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    auto listen_b = b->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen_a);
    BOOST_REQUIRE(listen_b);

    // A 的第一笔 Accept 挂起后占满 A 的唯一名额（接缝在名额入账后才放行，
    // 不靠时间推断）。
    auto wait_a = std::make_shared<Waiter>();
    auto wait_b = std::make_shared<Waiter>();
    InstallWaitEntryGate(listen_a.value(), wait_a);
    InstallWaitEntryGate(listen_b.value(), wait_b);

    bbtco [listen_a, wait_a]() {
        auto r = listen_a.value()->Accept(Options(30000));
        wait_a->code.store(r ? kOpSucceeded
                             : static_cast<int>(r.error().code));
        wait_a->settled.Down();
    };
    BOOST_REQUIRE_MESSAGE(
        wait_a->entered.WaitTimeout(kSeamBudgetMs) == 0,
        "case4/a: Accept 未在 " << kSeamBudgetMs
        << "ms 内进入等待（wait-entry gate 未落定）；"
           "A 账本 inflight=" << TransportWiring::InflightQuotaHeldForTest(*a)
        << " b 账本 inflight=" << TransportWiring::InflightQuotaHeldForTest(*b)
        << "（未到达 = 未进入等待，或被 Overloaded 拒绝/参数或上下文校验失败）");
    BOOST_CHECK_EQUAL(TransportWiring::InflightQuotaHeldForTest(*a), 1u);
    BOOST_CHECK_EQUAL(TransportWiring::InflightQuotaHeldForTest(*b), 0u);

    // A 账本已满：A 上的第二个可等待 op 立即 Overloaded。
    auto a2 = std::make_shared<Waiter>();
    bbtco [listen_a, a2]() {
        auto r = listen_a.value()->Accept(Options(30000));
        a2->code.store(r ? kOpSucceeded
                         : static_cast<int>(r.error().code));
        a2->settled.Down();
    };
    BOOST_REQUIRE_MESSAGE(
        a2->settled.WaitTimeout(kSeamBudgetMs) == 0,
        "case4/a2: 第二笔 Accept 未在 " << kSeamBudgetMs
        << "ms 内落定（应立即 Overloaded，不排队）; code=" << a2->code.load());
    BOOST_CHECK_EQUAL(a2->code.load(), static_cast<int>(ErrorCode::Overloaded));

    // B 的账本独立：同样 max_inflight=1，B 的 Accept 仍然可以正常挂起
    // （若配额被隐式合并，这里会 Overloaded）。
    bbtco [listen_b, wait_b]() {
        auto r = listen_b.value()->Accept(Options(30000));
        wait_b->code.store(r ? kOpSucceeded
                             : static_cast<int>(r.error().code));
        wait_b->settled.Down();
    };
    BOOST_REQUIRE_MESSAGE(
        wait_b->entered.WaitTimeout(kSeamBudgetMs) == 0,
        "case4/b: Accept 未在 " << kSeamBudgetMs
        << "ms 内进入等待（wait-entry gate 未落定；配额被误合并时 B 会 Overloaded）;"
           " code=" << wait_b->code.load());
    BOOST_CHECK_EQUAL(TransportWiring::InflightQuotaHeldForTest(*b), 1u);
    BOOST_CHECK_EQUAL(TransportWiring::InflightQuotaHeldForTest(*a), 1u);

    // 关闭 A 只归还 A 的名额，B 的挂起 op 不受影响（不误杀兄弟 owner）。
    listen_a.value()->Close();
    BOOST_CHECK(listen_a.value()->IsClosed());
    BOOST_REQUIRE_MESSAGE(wait_a->settled.WaitTimeout(kSeamBudgetMs) == 0,
                          "case4/a: listener 关闭后挂起 Accept 未在 "
                          << kSeamBudgetMs
                          << "ms 内落定（关闭未唤醒）; code="
                          << wait_a->code.load());
    BOOST_CHECK_EQUAL(wait_a->code.load(),
                      static_cast<int>(ErrorCode::Closed));
    BOOST_CHECK_EQUAL(TransportWiring::InflightQuotaHeldForTest(*a), 0u);
    BOOST_CHECK_EQUAL(TransportWiring::InflightQuotaHeldForTest(*b), 1u);

    listen_b.value()->Close();
    BOOST_CHECK(listen_b.value()->IsClosed());
    BOOST_REQUIRE_MESSAGE(wait_b->settled.WaitTimeout(kSeamBudgetMs) == 0,
                          "case4/b: listener 关闭后挂起 Accept 未在 "
                          << kSeamBudgetMs
                          << "ms 内落定（关闭未唤醒）; code="
                          << wait_b->code.load());
    BOOST_CHECK_EQUAL(wait_b->code.load(),
                      static_cast<int>(ErrorCode::Closed));
    BOOST_CHECK_EQUAL(TransportWiring::InflightQuotaHeldForTest(*b), 0u);

    guard.Unwind();
    BOOST_CHECK(a->IsClosed());
    BOOST_CHECK(b->IsClosed());
}

// ---------------------------------------------------------------------------
// 5. 关闭期已接纳但未落定的 op：挂起的 Accept 在 owner Close 时被唤醒走正常
//    归还路径（名额归零），且对象在 Close 返回时已物理收口（IsClosed）。
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(t_close_wakes_adopted_suspended_op) {
    EnsureRuntime();
    CaseGuard guard;

    // max_connections=4：listener + UDP 各占一个名额，Accept 仍有容量
    // （Accept 的容量判定也走同一 max_connections 门禁）。
    auto created = TransportRuntime::Create(Limits(4, 1));
    BOOST_REQUIRE(created);
    auto owner = std::move(created).value();
    guard.OnUnwind([owner] { owner->Close(); });
    BOOST_REQUIRE(owner->Start());

    auto listen = owner->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen);
    auto udp = owner->BindUDP(SocketAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(udp);

    auto waiter = std::make_shared<Waiter>();
    InstallWaitEntryGate(listen.value(), waiter);
    bbtco [listen, waiter]() {
        auto r = listen.value()->Accept(Options(30000));
        waiter->code.store(r ? kOpSucceeded
                             : static_cast<int>(r.error().code));
        waiter->settled.Down();
    };

    // 未进入等待：预算内 gate 未落定。断言信息区分「未进入等待/被拒」与
    // 「关闭未唤醒」，并带上 op 已观察到的结果码，避免 [-1 != 0] 无法归因。
    BOOST_REQUIRE_MESSAGE(
        waiter->entered.WaitTimeout(kSeamBudgetMs) == 0,
        "case5: Accept 未在 " << kSeamBudgetMs
        << "ms 内进入等待（wait-entry gate 未落定）; op code="
        << waiter->code.load()
        << "（-1 = op 未落定且未进入等待；非 -1 = op 已被拒绝/提前返回）;"
           " 名额 inflight=" << TransportWiring::InflightQuotaHeldForTest(*owner));
    BOOST_CHECK_EQUAL(TransportWiring::InflightQuotaHeldForTest(*owner), 1u);

    owner->Close();
    BOOST_REQUIRE_MESSAGE(waiter->settled.WaitTimeout(kSeamBudgetMs) == 0,
                          "case5: owner Close 后挂起 Accept 未在 "
                          << kSeamBudgetMs
                          << "ms 内落定（关闭未唤醒）; code="
                          << waiter->code.load()
                          << " 名额 inflight="
                          << TransportWiring::InflightQuotaHeldForTest(*owner));
    BOOST_CHECK_EQUAL(waiter->code.load(),
                      static_cast<int>(ErrorCode::Closed));
    BOOST_CHECK_EQUAL(TransportWiring::InflightQuotaHeldForTest(*owner), 0u);
    // Close 同步物理收口：owner 自身与全部受管对象都已关闭。
    BOOST_CHECK(listen.value()->IsClosed());
    BOOST_CHECK(udp.value()->IsClosed());
    BOOST_CHECK(owner->IsClosed());

    guard.Unwind();
    BOOST_CHECK(owner->IsClosed());
}

// ---------------------------------------------------------------------------
// 6. 并发 Close：两个线程同时关闭同一 owner，两个调用者返回当刻都必须观察到
//    同一真实终态（IsClosed），不得让后来者以独立 kCloseDrainTimeout 提前返回
//    而聚合尚未物理收口。
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(t_concurrent_close_both_observe_closed) {
    EnsureRuntime();
    CaseGuard guard;

    auto created = TransportRuntime::Create(Limits(4, 2));
    BOOST_REQUIRE(created);
    auto owner = std::move(created).value();
    guard.OnUnwind([owner] { owner->Close(); });
    BOOST_REQUIRE(owner->Start());

    auto listen = owner->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
    BOOST_REQUIRE(listen);
    auto udp = owner->BindUDP(SocketAddress{"127.0.0.1", 0});
    BOOST_REQUIRE(udp);

    std::atomic_int closed_at_return_a{-1};
    std::atomic_int closed_at_return_b{-1};
    std::thread a([&] {
        owner->Close();
        closed_at_return_a.store(owner->IsClosed() ? 1 : 0);
    });
    std::thread b([&] {
        owner->Close();
        closed_at_return_b.store(owner->IsClosed() ? 1 : 0);
    });
    a.join();
    b.join();

    // 两个调用者返回当刻都观察到同一 Closed 终态。
    BOOST_CHECK_EQUAL(closed_at_return_a.load(), 1);
    BOOST_CHECK_EQUAL(closed_at_return_b.load(), 1);
    BOOST_CHECK(owner->IsClosed());
    BOOST_CHECK(listen.value()->IsClosed());
    BOOST_CHECK(udp.value()->IsClosed());

    guard.Unwind();
}

// ---------------------------------------------------------------------------
// 7. 并发 Close + 在途工厂（R1 聚合根因）：工厂（ListenTCP）与两个 Close 调用
//    者竞争时，Close 封口后必须等「在途工厂」归零才收口——工厂要么把 child 交接
//    进托管集合（随即被 Close 物理关闭），要么在 sealed 路径自行物理关闭刚创建的
//    fd 并归还容量。断言：两个 Close 调用者返回当刻都观察同一 Closed 终态、无在途
//    工厂残留、任何被产出的 listener 都已物理关闭。多轮覆盖两种交错。
// ---------------------------------------------------------------------------
BOOST_AUTO_TEST_CASE(t_concurrent_close_tracks_inflight_factory) {
    EnsureRuntime();
    int failures = 0;
    for (int iter = 0; iter < 30; ++iter) {
        auto created = TransportRuntime::Create(Limits(4, 4));
        if (!created) { ++failures; continue; }
        auto owner = std::move(created).value();
        if (!owner->Start()) { ++failures; continue; }

        auto produced =
            std::make_shared<std::shared_ptr<bbt::infra::CoTCPListener>>();
        std::thread factory([&owner, produced] {
            auto listener = owner->ListenTCP(SocketAddress{"127.0.0.1", 0}, 16);
            if (listener) *produced = std::move(listener).value();
        });
        std::atomic_int closed_a{-1};
        std::atomic_int closed_b{-1};
        std::thread a([&owner, &closed_a] {
            owner->Close();
            closed_a.store(owner->IsClosed() ? 1 : 0);
        });
        std::thread b([&owner, &closed_b] {
            owner->Close();
            closed_b.store(owner->IsClosed() ? 1 : 0);
        });
        factory.join();
        a.join();
        b.join();

        if (!owner->IsClosed()) ++failures;
        if (TransportWiring::PendingFactoriesForTest(*owner) != 0) ++failures;
        if (closed_a.load() != 1 || closed_b.load() != 1) ++failures;
        if (*produced && !(*produced)->IsClosed()) ++failures;
    }
    BOOST_CHECK_MESSAGE(failures == 0,
                        "在途工厂聚合收口失败轮数=" << failures
                        << "（应为 0：Close 已封口并等在途工厂归零，无快照遗漏）");
}

BOOST_AUTO_TEST_SUITE_END()
