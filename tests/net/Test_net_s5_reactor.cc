/** @file Test_net_s5_reactor.cc
 * S5 Reactor 确定性交错测试：成员锁合一、OwnerWake 弱绑定、CloseStage 收口。
 *
 * 不依赖 sleep 证明竞态：端到端用例的全部交错由 EngineTestAccess 的一次性
 * 屏障在目标阶段内同步制造；直构用例同步验证寿命边界。不改产品代码。
 *
 * 用例清单（ctest 名 net.s5_<行为>）：
 *   s5_wake_registered_once    同一连接重复 Wake 只入链一次；泵后 OnClose 恰 1
 *   s5_wake_late_window        OwnerWake 取得 shared 后、登记前连接被驱逐，晚
 *                              Wake 不重复入链、不访问已拆对象
 *   s5_wake_reactor_destroyed  Reactor 先销毁：残留 state 调 Wake 安静返回
 *   s5_wake_driver_unbound     driver 解绑：跨域 Wake 的临时弱租约提升失败，
 *                              不登记、不唤醒、不崩溃
 *   s5_shutdownowner_stages    ShutdownOwner 分段推进到 Finished、OnClose 恰
 *                              一、幂等；OnClose 抛出仍收口；CancelControl 不
 *                              回调且 owner 路径短路
 *   s5_shutdownowner_stage_faults 每个 CloseStage 阶段注入故障后停在该阶段，
 *                                 续推至 Finished，OnClose 恰一次；控制域不
 *                                 伪造回调
 *   s5_removeconn_dirty_window AfterDirtyWeakLock 窗口内真实 RemoveConn，晚
 *                                Wake 不入链、DirtyCount 归 0、不访问已拆对象
 *
 * 接缝已全部由 seam2 产品入口覆盖，不改产品代码、不放宽断言：
 *   - PrepareHooksForTest() 在 Reactor 构造前串行装配，直构夹具可装配
 *     AfterDirtyWeakLock 等钩子，wake_driver_unbound 的（b）动态驻守分支
 *     可直接断言；
 *   - SeedRecvHoldForTest() 向 ConnCore 注入已知尾字节，shutdownowner_stage_faults
 *     以 RecvHold()==N / 0 观测持与移交边界，并从 state 环缓冲读回原串再 EOF。
 */
#define BOOST_TEST_NO_MAIN
#define BOOST_TEST_ALTERNATIVE_INIT_API
#include <boost/test/included/unit_test.hpp>

#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/RawHandler.hpp>

#include <bbt/infra/net/detail/ConnCore.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/EngineState.hpp>
#include <bbt/infra/net/detail/EngineTestAccess.hpp>
#include <bbt/infra/net/detail/TestHooks.hpp>
#include <bbt/infra/net/detail/Reactor.hpp>
#include <bbt/infra/net/detail/ReactorDriver.hpp>
#include <bbt/infra/net/detail/ReclaimRecord.hpp>

#include <bbt/coroutine/coroutine.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <utility>

#include <boost/asio/io_context.hpp>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace bbt::infra::net;
using namespace std::chrono_literals;

namespace {

static void Check(bool value, const char* expression, int line)
{
    if (!value) {
        throw std::runtime_error("line " + std::to_string(line) + ": " + expression);
    }
}

#define CHECK(expression) Check(static_cast<bool>(expression), #expression, __LINE__)

static bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::yield();
    }
    return predicate();
}

/** 屏障守卫：析构时先放行（release 并 notify），再由 runner 析构执行 join。
 * 声明在屏障状态之后，故析构在 runner 之前；CHECK 抛异常时，正在
 * cv.wait 的 owner 线程能被放行，避免与已析构的同步对象相遇或
 * join 永久阻塞。 */
struct BarrierGuard {
    std::atomic<bool>& release;
    std::condition_variable& cv;

    ~BarrierGuard() noexcept
    {
        release.store(true, std::memory_order_release);
        cv.notify_all();
    }
};

Address Loopback(int port)
{
    std::error_code error;
    Address address = Address::Ip("127.0.0.1", port, error);
    return address;
}

EngineConfig ServerConfig(const Address& listen)
{
    EngineConfig config;
    config.listen_addresses = {listen};
    config.runtime_progress = RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    return config;
}

void StartRuntime(std::size_t workers)
{
    g_bbt_coroutine_config->m_cfg_static_thread_num = workers;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start();
}

struct EngineRunner {
    Engine& engine;
    std::error_code error;
    std::thread thread;

    EngineRunner(Engine& target, EngineConfig config)
        : engine(target)
    {
        thread = std::thread([this, config]() mutable { error = engine.Run(config); });
    }

    void CloseAndJoin()
    {
        (void)engine.Close();
        if (thread.joinable()) {
            thread.join();
        }
    }

    ~EngineRunner() { CloseAndJoin(); }
};

NativeSocket ConnectClient(const Address& address, bool& ok)
{
    ok = false;
    std::error_code error;
    NativeSocket client = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    if (error) {
        return client;
    }
    const auto connect_error = client.Connect(address);
    if (connect_error && connect_error.value() != EINPROGRESS && connect_error.value() != EWOULDBLOCK) {
        return client;
    }
    pollfd descriptor{client.Fd(), POLLOUT, 0};
    if (::poll(&descriptor, 1, 3000) <= 0) {
        return client;
    }
    if (client.PendingError()) {
        return client;
    }
    (void)client.SetNoDelay(true);
    ok = true;
    return client;
}

bool SendAll(NativeSocket& client, const std::string& payload, std::chrono::milliseconds timeout)
{
    std::size_t sent = 0;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (sent < payload.size()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return false;
        }
        const auto result = client.Send(payload.data() + sent, payload.size() - sent);
        if (result.bytes > 0) {
            sent += result.bytes;
            continue;
        }
        if (result.error.value() == EAGAIN || result.error.value() == EWOULDBLOCK) {
            pollfd descriptor{client.Fd(), POLLOUT, 0};
            (void)::poll(&descriptor, 1, 10);
            continue;
        }
        return false;
    }
    return true;
}

/** 原始回调计数：accepted 与存下首条连接句柄，onClose 计数用于断言恰一次。 */
struct CountingHandler : RawHandler {
    std::atomic<int> accepted{0};
    std::atomic<int> onClose{0};
    std::mutex mutex;
    std::shared_ptr<Connection> stored;

    void OnAccept(Connection connection) override
    {
        std::shared_ptr<Connection> stored_connection = std::make_shared<Connection>(connection);
        std::lock_guard<std::mutex> lock(mutex);
        // 先在锁内发布句柄，再置就绪：accepted>=1 时必可读到非空 stored，
        // 避免等待线程在句柄发布前拿到空句柄。
        stored = std::move(stored_connection);
        accepted.fetch_add(1, std::memory_order_release);
    }

    void OnRead(Connection) override {}

    void OnConnect(Connection, std::error_code, std::uint64_t) override {}

    void OnWrite(Connection, WriteResult) override {}

    void OnClose(Connection, std::error_code) override { onClose.fetch_add(1); }
};

/** OnClose 抛出版本：验证 owner 回调异常不逃逸、收口仍完成且恰一次。 */
struct ThrowingHandler : RawHandler {
    std::atomic<int> onClose{0};

    void OnClose(Connection, std::error_code) override
    {
        onClose.fetch_add(1);
        throw std::runtime_error("s5 close callback");
    }

    void OnRead(Connection) override {}

    void OnConnect(Connection, std::error_code, std::uint64_t) override {}

    void OnWrite(Connection, WriteResult) override {}
};

const std::error_code kCloseReason = std::make_error_code(std::errc::operation_canceled);

/* ----------------------------------------------------------------
 * 直构 Reactor + ConnCore：不启动 runtime，不做真实 I/O，m_desc 未分配 fd。
 * core 不经 AdoptFdOnOwner 登记进 Reactor 目录，故 MarkDirty 的 registry
 * 查找会失败；用来验证「取得 shared、未登记」的早退路径与寿命边界。
 * 夹具在 Idle 串行调用 PrepareHooksForTest（早于 Reactor 构造，供
 * Reactor 忙拓钩子槽），descriptor 用 driver->Context()（与生产 Ctx()
 * 一致的 io_context）。
 * ---------------------------------------------------------------- */
struct ReactorDirect {
    /** io_context 先声明、后析构：须活到 core 及其 descriptor 析构之后，
     * 避免 descriptor 析构时 service 已失效（与生产一致的 context 寿命）。 */
    boost::asio::io_context ctx_;
    std::shared_ptr<EngineImpl> impl;
    std::shared_ptr<ReactorDriver> driver;
    std::shared_ptr<Reactor> reactor;
    std::shared_ptr<ReclaimRecord> reclaim;
    std::shared_ptr<ConnectionState> state;
    std::shared_ptr<ConnCore> core;
    std::shared_ptr<CountingHandler> handler;

    explicit ReactorDirect(std::shared_ptr<RawHandler> h)
        : handler(std::dynamic_pointer_cast<CountingHandler>(h))
    {
        impl = std::make_shared<EngineImpl>(h);
        // Idle 串行装配，必须早于 Reactor 构造（钩子槽在构造时快照）。
        impl->PrepareHooksForTest();
        driver = std::make_shared<ReactorDriver>();
        reactor = std::make_shared<Reactor>(impl.get(), 0, driver);
        reclaim = std::make_shared<ReclaimRecord>(false);
        state = std::make_shared<ConnectionState>(1, 4096, 1024, 4, impl->SealToken(), reclaim);
        TcpWait desc(driver->Context()); // 未分配 fd，供寿命边界测试使用
        core = std::make_shared<ConnCore>(state, std::move(desc), reactor.get(), h);
        state->AttachOwner(reactor, core);
    }
};

/* ----------------------------------------------------------------
 * 直构 CloseStage 故障注入夹具：自备 TestHooks，FaultPoint 经 FailOnce
 * 装入后，ConnCore::ShutdownOwner 在对应阶段一次性停住；再次调用从故障
 * 阶段续做。直构夹具不启动 runtime，ShutdownOwner 及回调同步执行，
 * 阶段边界可直接断言，不靠睡眠。
 * ---------------------------------------------------------------- */
struct StageFaultFixture {
    boost::asio::io_context ctx_; // 同上：先声明、后析构
    std::shared_ptr<EngineImpl> impl;
    std::shared_ptr<ReactorDriver> driver;
    std::shared_ptr<Reactor> reactor;
    std::shared_ptr<ReclaimRecord> reclaim;
    std::shared_ptr<TestHooks> hooks;
    std::shared_ptr<ConnectionState> state;
    std::shared_ptr<ConnCore> core;
    std::shared_ptr<CountingHandler> handler;
    int peerFd{-1}; // socketpair 对端 fd，由本夹具独占 RAII，析构时关闭；
                    // 本端（fds[0]）assign 成功后归 descriptor/core 托管，夹具不再按缓存 fd 数值 close

    StageFaultFixture(std::shared_ptr<CountingHandler> h, std::shared_ptr<TestHooks> testHooks)
        : hooks(std::move(testHooks))
        , handler(std::move(h))
    {
        impl = std::make_shared<EngineImpl>(handler);
        driver = std::make_shared<ReactorDriver>();
        reactor = std::make_shared<Reactor>(impl.get(), 0, driver);
        reclaim = std::make_shared<ReclaimRecord>(false);
        state = std::make_shared<ConnectionState>(1, 4096, 1024, 4, impl->SealToken(), reclaim, hooks);
        int fds[2];
        if (::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
            throw std::runtime_error("socketpair failed");
        }
        peerFd = fds[1]; // 对端由本夹具独占
        boost::system::error_code ec;
        TcpWait desc(ctx_);
        desc.assign(fds[0], ec); // 成功后本端归 descriptor/core 托管
        if (ec) {
            ::close(fds[0]);
            ::close(fds[1]);
            peerFd = -1;
            throw std::runtime_error("TcpWait::assign failed: " + ec.message());
        }
        core = std::make_shared<ConnCore>(state, std::move(desc), reactor.get(), handler);
        state->AttachOwner(reactor, core);
    }

    ~StageFaultFixture()
    {
        if (peerFd >= 0) {
            ::close(peerFd);
            peerFd = -1;
        }
        // fds[0] 由 descriptor/core 托管，析构随 ConnCore 一并关闭；
        // 夹具不再按缓存 fd 数值 close，避免复用后误关无关 fd
    }
};

/* ----------------------------------------------------------------
 * 直构用例：各断言同步成立
 * ---------------------------------------------------------------- */

/** Reactor 先销毁 / core 先移除：残留 state 调 Wake 必须安静返回，
 * 不崩溃、不入链、不访问已拆对象。
 */
static void S5WakeReactorDestroyed()
{
    auto h = std::make_shared<CountingHandler>();
    ReactorDirect fx(h);

    fx.state->WakeOwner(); // core 未登记：弱锁成功，登记查找失败，不入链
    CHECK(fx.reactor->DirtyCount() == 0);

    fx.reactor.reset();     // Reactor 先销毁：m_ownerWake 的 reactor 弱引用失效
    fx.state->WakeOwner();  // 必须安静返回
    fx.core.reset();        // 再让 core 先于 Reactor 失效
    fx.state->WakeOwner();  // 同理，不得访问已拆对象

    ReactorDirect fx2(h);   // Reactor 尚在，core 已被移除
    fx2.core.reset();
    fx2.state->WakeOwner(); // core 弱引用失效，MarkDirty 早退
    CHECK(fx2.reactor->DirtyCount() == 0);
}

/** driver 解绑与弱租约过期。
 *
 * (a) 租约已过期：夹具与 Reactor 均不再强持 driver，m_contextLease.lock()
 *     失败，Wake 在 contextLease 检查（登记前钩子之前）即早退，不登记、
 *     不唤醒；不保留 driver 强引用做观测，仅以 weak_ptr 观测租约。
 * (b) 动态子例：descriptor 用 driver->Context()，AfterDirtyWeakLock 扣住
 *     Wake，释放 core/driver 强引用并解绑，仅 weak 观察；core deleter
 *     记录删除 core 时 driver 未过期；放行、join 后断言两者都过期。
 * (c) 旧对照：解绑后外部仍持 driver，Wake 后旧 driver WakeCount 仍为 0。
 *
 * 直构，同步断言，不睡眠。
 */
static void S5WakeDriverUnbound()
{
    auto h = std::make_shared<CountingHandler>();

    // 在线 Wake 证明同一钩子槽可命中：装 AfterDirtyWeakLock，Wake 命中。
    {
        ReactorDirect fx(h);
        std::atomic<bool> hookEntered{false};
        fx.reactor->ArmHookForTest(HookPoint::AfterDirtyWeakLock, [&] { hookEntered = true; }, true);
        fx.state->WakeOwner();                // driver 在线，core 未登记：命中钩子，查找失败，不入链
        CHECK(hookEntered);                   // 同一钩子槽可命中
        CHECK(fx.reactor->DirtyCount() == 0); // core 未登记，不入链
    }

    // (a) 租约已过期：释放夹具与 Reactor 的全部 driver 强引用，Wake 早退。
    {
        ReactorDirect fx(h);
        std::weak_ptr<ReactorDriver> lease = fx.driver;
        fx.driver.reset();              // 释放夹具最后持有的强引用
        fx.reactor->ReleaseDriver();    // 释放 Reactor 副本
        CHECK(lease.lock() == nullptr); // 租约已过期；不保留 driver 强引用做观测

        fx.state->WakeOwner();          // contextLease.lock() 失败，早退，不登记、不唤醒
        CHECK(fx.reactor->DirtyCount() == 0);
    }

    // (b) 动态子例：AfterDirtyWeakLock 扣住 Wake，释放 core/driver 引用并解绑，
    //     仅 weak 观察；core deleter 记录删除 core 时 driver 未过期；
    //     放行、join 后断言两者都过期。
    {
        auto h2 = std::make_shared<CountingHandler>();
        auto impl2 = std::make_shared<EngineImpl>(h2);
        impl2->PrepareHooksForTest(); // 早于 Reactor 构造
        auto driver2 = std::make_shared<ReactorDriver>();
        std::weak_ptr<ReactorDriver> weakDriver2 = driver2;
        auto reactor2 = std::make_shared<Reactor>(impl2.get(), 0, driver2);
        auto reclaim2 = std::make_shared<ReclaimRecord>(false);
        auto state2 = std::make_shared<ConnectionState>(1, 4096, 1024, 4, impl2->SealToken(), reclaim2);
        TcpWait desc2(driver2->Context()); // descriptor 用 driver->Context()

        std::atomic<bool> driverAliveAtCoreDelete{false};
        std::weak_ptr<ConnCore> weakCore2;
        {
            auto core2 =
                std::shared_ptr<ConnCore>(new ConnCore(state2, std::move(desc2), reactor2.get(), h2), [&](ConnCore* c) {
                    driverAliveAtCoreDelete = (weakDriver2.lock() != nullptr); // 删除 core 时 driver 未过期
                    delete c;
                });
            weakCore2 = core2;
            state2->AttachOwner(reactor2, core2);

            // 装阻塞式 AfterDirtyWeakLock 钩子：Wake 取得 shared 后在此扣住
            std::mutex mtx;
            std::condition_variable cv;
            std::atomic<bool> hookEntered2{false};
            std::atomic<bool> release2{false};
            reactor2->ArmHookForTest(
                HookPoint::AfterDirtyWeakLock,
                [&] {
                    hookEntered2 = true;
                    std::unique_lock<std::mutex> lock(mtx);
                    cv.wait(lock, [&] { return release2.load(); });
                },
                true);

            // Wake 在单独线程执行，阻塞于钩子
            std::atomic<bool> wakeDone2{false};
            std::thread wakeThread2([&] {
                state2->WakeOwner();
                wakeDone2 = true;
            });
            CHECK(WaitUntil([&] { return hookEntered2.load(); }, 10000ms));

            // 扣住期间：先释放核心引用（MarkDirty 的 shared 参数仍持核心，
            // 真正析构在钩子返回后）；放行、join，此时 deleter 运行且
            // driver2 尚未 reset，记录 driver 未过期。
            core2.reset();
            {
                std::lock_guard<std::mutex> lock(mtx);
                release2 = true;
            }
            cv.notify_all();
            wakeThread2.join();

            // 钩子返回后核心已析构，driver2 尚活，deleter 应记录 driver 未过期。
            CHECK(driverAliveAtCoreDelete.load());

            // 现在释放 driver 并解绑，再断言两者都过期。
            driver2.reset();
            reactor2->ReleaseDriver();
            CHECK(weakDriver2.lock() == nullptr); // driver 已过期
            CHECK(weakCore2.lock() == nullptr);   // core 已析构
        }
    }

    // (c) 旧对照：解绑后外部仍持 driver，Wake 后旧 driver WakeCount 仍为 0。
    {
        auto h3 = std::make_shared<CountingHandler>();
        auto impl3 = std::make_shared<EngineImpl>(h3);
        impl3->PrepareHooksForTest();
        auto driver3 = std::make_shared<ReactorDriver>();
        auto reactor3 = std::make_shared<Reactor>(impl3.get(), 0, driver3);
        auto reclaim3 = std::make_shared<ReclaimRecord>(false);
        auto state3 = std::make_shared<ConnectionState>(1, 4096, 1024, 4, impl3->SealToken(), reclaim3);
        TcpWait desc3(driver3->Context());
        auto core3 = std::make_shared<ConnCore>(state3, std::move(desc3), reactor3.get(), h3);
        state3->AttachOwner(reactor3, core3);

        reactor3->ReleaseDriver();        // 解绑 reactor，外部仍持 driver3
        std::weak_ptr<ReactorDriver> lease3 = driver3;
        CHECK(lease3.lock() != nullptr);  // 外部仍持 driver

        state3->WakeOwner();              // reactor 租约已释放，不登记
        CHECK(driver3->WakeCount() == 0); // 旧 driver WakeCount 仍为 0
        CHECK(reactor3->DirtyCount() == 0);
    }
}

/** ShutdownOwner：分段推进到 Finished，OnClose 恰一次，重复推进幂等；
 * OnClose 抛出仍收口；控制域 CancelControl 不回调且 owner 路径短路。
 */
static void S5ShutdownOwnerStages()
{
    // 正常推进：Open → … → Finished，OnClose 恰一次，重复推进短路。
    auto h = std::make_shared<CountingHandler>();
    ReactorDirect fx(h);
    fx.core->ShutdownOwner(kCloseReason);
    CHECK(fx.core->Closed());
    CHECK(fx.state->OwnerReleased());
    CHECK(fx.handler->onClose == 1);
    fx.core->ShutdownOwner(kCloseReason); // Closed() 短路，不再派发
    CHECK(fx.handler->onClose == 1);

    // OnClose 抛出：owner 回异常在调用点隔离，收口仍完成且恰一次。
    auto th = std::make_shared<ThrowingHandler>();
    ReactorDirect fx2(th);
    fx2.core->ShutdownOwner(kCloseReason);
    CHECK(fx2.core->Closed());
    CHECK(fx2.state->OwnerReleased());
    CHECK(th->onClose == 1);

    // 控制域 CancelControl：冻结状态并收口，不调用用户回调；
    // 已收口后 owner 路径短路，不伪造 OnClose。
    auto h3 = std::make_shared<CountingHandler>();
    ReactorDirect fx3(h3);
    fx3.core->CancelControl();
    CHECK(fx3.core->Closed());
    CHECK(h3->onClose == 0);
    CHECK(fx3.state->OwnerReleased());
    fx3.core->ShutdownOwner(kCloseReason);
    CHECK(h3->onClose == 0);
}

/** CloseStage 各阶段注入一次故障：在 stage 入口停住；下次 ShutdownOwner 从
 * 故障阶段续做至 Finished，OnClose 恰好一次，不重复派发；控制域
 * CancelControl 不回调，且已收口后 owner 推进短路，不伪造 OnClose。
 * 直构，同步断言，不睡眠。
 *
 * 用 SeedRecvHoldForTest 注入已知串（长度 N），观测各阶段边界：
 *   CloseStageHold：hold == N，fd 仍活，未收口；
 *   CloseBackend：hold 已移交（RecvHold 归零），fd 仍活；
 *   PublishCloseState：fd 已物理关闭（Fd()==-1），state 终态尚未发布。
 * 续推后从 state 环缓冲读回原串再 EOF，重复推进不重复字节或回调。
 */
static void S5ShutdownOwnerStageFaults()
{
    const std::string kTail = "s5-stage-fault-tail-data"; // 已知串，长度 N
    const std::size_t kTailN = kTail.size();

    auto finish = [](StageFaultFixture& fx) {
        fx.core->ShutdownOwner(kCloseReason);
        CHECK(fx.core->Closed());
        CHECK(fx.state->OwnerReleased());
        CHECK(fx.handler->onClose == 1);
        fx.core->ShutdownOwner(kCloseReason);
        CHECK(fx.handler->onClose == 1); // 短路，不伪造重复回调
    };

    auto readTailThenEof = [&](StageFaultFixture& fx) {
        std::string out;
        auto res = fx.state->TryRead(out);
        CHECK(res.status == ReadStatus::Ok);
        CHECK(out == kTail); // 原串恰转入 state 一次
        out.clear();
        res = fx.state->TryRead(out);
        CHECK(res.status == ReadStatus::Eof); // 再读 EOF，无重复字节
        CHECK(res.bytes == 0);
        CHECK(out.empty());
    };

    // CloseStageHold：在 hold 转入 state 前停住，hold == N，fd 尚未关闭。
    {
        auto hooks = std::make_shared<TestHooks>();
        hooks->FailOnce(FaultPoint::CloseStageHold);
        StageFaultFixture fx(std::make_shared<CountingHandler>(), hooks);
        fx.core->SeedRecvHoldForTest(kTail); // 注入已知串，长度 N
        fx.core->ShutdownOwner(kCloseReason);
        // 第一次推进在注入阶段停住：未收口、无回调、无回收、state 终态未发布
        CHECK(!fx.core->Closed());
        CHECK(!fx.state->Closed());
        CHECK(fx.handler->onClose == 0);
        CHECK(!fx.state->OwnerReleased());
        CHECK(fx.core->Fd() >= 0);            // fd 仍活
        CHECK(fx.core->RecvHold() == kTailN); // hold == N，未转入 state
        finish(fx);                           // 续推至 Finished
        CHECK(fx.core->Fd() == -1);           // 续推中 backend 已物理关闭
        readTailThenEof(fx);
        CHECK(fx.handler->onClose == 1);      // 无重复回调
    }

    // CloseBackend：hold 已移交（RecvHold 归零），fd 仍活；续推中关闭 backend。
    {
        auto hooks = std::make_shared<TestHooks>();
        hooks->FailOnce(FaultPoint::CloseBackend);
        StageFaultFixture fx(std::make_shared<CountingHandler>(), hooks);
        fx.core->SeedRecvHoldForTest(kTail);
        fx.core->ShutdownOwner(kCloseReason);
        CHECK(!fx.core->Closed());
        CHECK(!fx.state->Closed());
        CHECK(fx.handler->onClose == 0);
        CHECK(!fx.state->OwnerReleased());
        CHECK(fx.core->Fd() >= 0);       // fd 仍活（_CloseBackend 未执行）
        CHECK(fx.core->RecvHold() == 0); // hold 已转入 state
        finish(fx);
        CHECK(fx.core->Fd() == -1);
        readTailThenEof(fx);
        CHECK(fx.handler->onClose == 1);
    }

    // PublishCloseState：backend 已物理关闭，state 终态尚未发布。
    {
        auto hooks = std::make_shared<TestHooks>();
        hooks->FailOnce(FaultPoint::PublishCloseState);
        StageFaultFixture fx(std::make_shared<CountingHandler>(), hooks);
        fx.core->SeedRecvHoldForTest(kTail);
        fx.core->ShutdownOwner(kCloseReason);
        CHECK(!fx.core->Closed());
        CHECK(!fx.state->Closed()); // state 终态尚未发布
        CHECK(fx.handler->onClose == 0);
        CHECK(!fx.state->OwnerReleased());
        CHECK(fx.core->Fd() == -1); // fd 已关
        CHECK(fx.core->RecvHold() == 0);
        finish(fx);
        CHECK(fx.state->Closed());
        readTailThenEof(fx);
        CHECK(fx.handler->onClose == 1);
    }

    // DeliverCloseCallbacks：state 终态已发布，OnClose 尚未派发。
    {
        auto hooks = std::make_shared<TestHooks>();
        hooks->FailOnce(FaultPoint::DeliverCloseCallbacks);
        StageFaultFixture fx(std::make_shared<CountingHandler>(), hooks);
        fx.core->SeedRecvHoldForTest(kTail);
        fx.core->ShutdownOwner(kCloseReason);
        CHECK(!fx.core->Closed());
        CHECK(fx.state->Closed());
        CHECK(fx.handler->onClose == 0);
        CHECK(!fx.state->OwnerReleased());
        fx.core->ShutdownOwner(kCloseReason);
        CHECK(fx.core->Closed());
        CHECK(fx.state->OwnerReleased());
        CHECK(fx.handler->onClose == 1);
        readTailThenEof(fx);
        CHECK(fx.handler->onClose == 1); // 无重复回调
    }

    // PublishOwnerReleased：OnClose 已派发，回收尚未发布。
    {
        auto hooks = std::make_shared<TestHooks>();
        hooks->FailOnce(FaultPoint::PublishOwnerReleased);
        StageFaultFixture fx(std::make_shared<CountingHandler>(), hooks);
        fx.core->SeedRecvHoldForTest(kTail);
        fx.core->ShutdownOwner(kCloseReason);
        CHECK(!fx.core->Closed());
        CHECK(fx.state->Closed());
        CHECK(fx.handler->onClose == 1);
        CHECK(!fx.state->OwnerReleased());
        fx.core->ShutdownOwner(kCloseReason);
        CHECK(fx.core->Closed());
        CHECK(fx.state->OwnerReleased());
        CHECK(fx.handler->onClose == 1);
        readTailThenEof(fx);
        CHECK(fx.handler->onClose == 1); // 无重复回调
    }

    // 控制域 CancelControl：终结连接但不调用用户回调；后续 owner 推进
    // 短路，不伪造 OnClose。
    {
        auto hooks = std::make_shared<TestHooks>();
        StageFaultFixture fx(std::make_shared<CountingHandler>(), hooks);
        fx.core->SeedRecvHoldForTest(kTail);
        fx.core->CancelControl();
        CHECK(fx.core->Closed());
        CHECK(fx.state->OwnerReleased());
        CHECK(fx.handler->onClose == 0);
        fx.core->ShutdownOwner(kCloseReason);
        CHECK(fx.handler->onClose == 0);
        readTailThenEof(fx);
        CHECK(fx.handler->onClose == 0);
    }
}

/** AfterDirtyWeakLock 窗口内真实 RemoveConn：屏障命中后同步撤销登记再放行，
 * 晚 Wake 不重复入链、DirtyCount 归 0、不访问已拆对象、进程不崩溃。
 * 先由屏障钩子证明进入目标阶段；客户端断开使服务端读 EOF、核心析构
 * 并回收，再提交晚 Wake 证明对已撤销核心的唤醒不入链；不睡眠。
 */
static void S5RemoveConnDirtyWindow()
{
    StartRuntime(2);

    auto handler = std::make_shared<CountingHandler>();
    Engine engine(handler);
    // Idle 阶段预装 test_hooks：Running 后 owner 才创建，owner-hook 才分发到 reactor。
    EngineTestAccess::SetDirtyBarrierOnce(engine, [] {});
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 10000ms));

    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    CHECK(!addresses.empty());

    bool ok = false;
    NativeSocket client = ConnectClient(addresses[0], ok);
    CHECK(ok);

    CHECK(WaitUntil([&] { return handler->accepted.load() >= 1; }, 10000ms));

    std::shared_ptr<Connection> conn;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        conn = handler->stored;
    }
    CHECK(conn);

    std::mutex mtx;
    std::condition_variable cv;
    std::atomic<bool> hook_entered{false};
    std::atomic<bool> release{false};
    std::atomic<bool> removed{false};
    BarrierGuard guard{release, cv}; // 先放行再 join：异常时不挂 owner 于已析构的 CV

    EngineTestAccess::SetDirtyBarrierOnce(engine, [&] {
        removed = EngineTestAccess::RemoveConn(engine, conn->Id()); // 窗口内真实同步撤销登记
        hook_entered = true; // 已取得 core shared，尚未登记入链（在 RemoveConn 之后置位，避免竞态）
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [&] { return release.load(); }); // 放行前扣住窗口
    });

    // Wake #1 阻塞于 AfterDirtyWeakLock 窗口；先证明进入目标阶段
    CHECK(SendAll(client, "x", 5000ms));

    CHECK(WaitUntil([&] { return hook_entered.load(); }, 10000ms));
    CHECK(removed); // 登记在窗口内被同步撤销

    // 放行：被扣住的 MarkDirty 续行发现登记已不在，不入链。
    {
        std::lock_guard<std::mutex> lock(mtx);
        release = true;
    }
    cv.notify_all();

    // 客户端断开：服务端读 EOF 后不再重发读注册，挂起的 weak 推进完成、
    // 核心析构并回收，晚 Wake 不再访问已拆对象。
    client.Close();
    CHECK(WaitUntil([&] { return conn->Released(); }, 10000ms));

    // 晚 Wake：核心已撤销/已析构，RequestClose -> WakeOwner -> MarkDirty
    // 不得重复入链、不崩溃。
    CHECK(!conn->Close());
    CHECK(EngineTestAccess::DirtyCount(engine) == 0); // 归零，未重复入链
    CHECK(handler->onClose == 0);                     // 未走 graceful 关闭回调

    runner.CloseAndJoin();
    CHECK(!runner.error);
}

/* ----------------------------------------------------------------
 * 端到端用例：真实 Engine + loopback TCP，竞态由屏障在目标阶段内制造
 * ---------------------------------------------------------------- */

/** 同一连接重复 Wake 只入链一次。
 *
 * 时序：服务端读入字段后，OwnerWake 唤醒进入 AfterDirtyWeakLock
 * 窗口并被屏障阻塞（已取得 shared，尚未登记）；测试线程再发起 Close()
 * 导致第二次 Wake。阻塞期脏链不被泵走，DirtyCount 恒等于 1，释放屏障
 * 后第一次 Wake 重入发现已入链，不重复登记；泵后脏链归 0、OnClose 恰
 * 1。两次 Wake 只登记一条，绝不出现第二条链节点。
 */
static void S5WakeRegisteredOnce()
{
    StartRuntime(2);
    auto handler = std::make_shared<CountingHandler>();
    Engine engine(handler);
    // Idle 阶段预装 test_hooks：Running 后 owner 才创建，owner-hook 才能分发到 reactor。
    EngineTestAccess::SetDirtyBarrierOnce(engine, [] {});
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 10000ms));

    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    CHECK(!addresses.empty());

    bool ok = false;
    NativeSocket client = ConnectClient(addresses[0], ok);
    CHECK(ok);

    CHECK(WaitUntil([&] { return handler->accepted.load() >= 1; }, 10000ms));

    std::shared_ptr<Connection> conn;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        conn = handler->stored;
    }
    CHECK(conn);

    std::mutex mtx;
    std::condition_variable cv;
    std::atomic<bool> hook_entered{false};
    std::atomic<bool> release{false};
    BarrierGuard guard{release, cv}; // 先放行再 join：异常时不挂 owner 于已析构的 CV

    EngineTestAccess::SetDirtyBarrierOnce(engine, [&] {
        hook_entered = true; // 已取得 core shared，尚未登记
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [&] { return release.load(); });
    });

    // Wake #1：服务端读入 'x' → NotifyDataChanged → WakeOwner → MarkDirty
    // → 阻塞于登记前。
    CHECK(SendAll(client, "x", 5000ms));

    CHECK(WaitUntil([&] { return hook_entered.load(); }, 10000ms));

    // Wake #2：同一连接 Close() → RequestClose → WakeOwner，在 Wake #1 登记前完成。
    CHECK(!conn->Close());

    // 脏链必须恒等于 1：两次 Wake 只登记一条，绝不出现第二条链节点。
    // Wake #1 阻塞于登记前钩子，Wake #2 完成唯一登记；屏障释放后，重入的
    // Wake #1 发现已入链，不重复登记；泵后脏链归 0。
    CHECK(WaitUntil([&] { return EngineTestAccess::DirtyCount(engine) == 1; }, 10000ms));
    for (int i = 0; i < 2000; ++i) {
        CHECK(EngineTestAccess::DirtyCount(engine) <= 1);
        std::this_thread::yield();
    }

    {
        std::lock_guard<std::mutex> lock(mtx);
        release = true;
    }
    cv.notify_all();

    // 泵入链：CloseRequested → ShutdownOwner → OnClose ×1 → Finished → RemoveConn。
    EngineTestAccess::KickOwner(engine);
    CHECK(WaitUntil([&] { return EngineTestAccess::DirtyCount(engine) == 0 && conn->Released(); }, 10000ms));
    CHECK(handler->onClose == 1);

    runner.CloseAndJoin();
    CHECK(!runner.error);
}

/** OwnerWake 取得 shared 后、登记前，连接被驱逐，晚 Wake 不重复入链。
 *
 * 时序：服务端读入字段后进入 AfterDirtyWeakLock 窗口；钩子内部对同一连接
 * 发起 Close()（唤醒并登记本次）并强制泵入链，使连接已进入收口；
 * 屏障释放后，晚 Wake 重入发现核心已入链/被撤销，不再重复登记；连接
 * 最终被物理收口，OnClose 恰一次，无 UAF 崩溃。
 */
static void S5WakeLateWindow()
{
    StartRuntime(2);
    auto handler = std::make_shared<CountingHandler>();
    Engine engine(handler);
    // Idle 阶段预装 test_hooks：Running 后 owner 才创建，owner-hook 才能分发到 reactor。
    EngineTestAccess::SetDirtyBarrierOnce(engine, [] {});
    EngineRunner runner(engine, ServerConfig(Loopback(0)));
    CHECK(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 10000ms));

    std::vector<Address> addresses;
    CHECK(!engine.LocalAddresses(addresses));
    CHECK(!addresses.empty());

    bool ok = false;
    NativeSocket client = ConnectClient(addresses[0], ok);
    CHECK(ok);

    CHECK(WaitUntil([&] { return handler->accepted.load() >= 1; }, 10000ms));

    std::shared_ptr<Connection> conn;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        conn = handler->stored;
    }
    CHECK(conn);

    std::atomic<bool> hook_entered{false};
    EngineTestAccess::SetDirtyBarrierOnce(engine, [&] {
        hook_entered = true;                 // 已取得 core shared，尚未登记
        (void)!conn->Close();                // 窗口内驱逐连接：RequestClose 并登记本次
        EngineTestAccess::KickOwner(engine); // 同步驱动收口
    });

    CHECK(SendAll(client, "x", 5000ms));

    CHECK(WaitUntil([&] { return hook_entered.load(); }, 10000ms)); // 进入目标阶段

    // 晚 Wake 不重复入链：脏链最终被收口清零，绝不出现第二条链节点。
    CHECK(WaitUntil([&] { return EngineTestAccess::DirtyCount(engine) == 0; }, 10000ms));
    CHECK(WaitUntil([&] { return conn->Released(); }, 10000ms)); // 对象已收口，晚 Wake 不访问已拆对象
    CHECK(handler->onClose == 1);                                // OnClose 恰一次

    runner.CloseAndJoin();
    CHECK(!runner.error);
}

static int RunCases(int argc, char** argv)
{
    try {
        CHECK(argc == 2);
        const std::string test = argv[1];
        if (test == "wake_registered_once") {
            S5WakeRegisteredOnce();
        } else if (test == "wake_late_window") {
            S5WakeLateWindow();
        } else if (test == "wake_reactor_destroyed") {
            S5WakeReactorDestroyed();
        } else if (test == "wake_driver_unbound") {
            S5WakeDriverUnbound();
        } else if (test == "shutdownowner_stages") {
            S5ShutdownOwnerStages();
        } else if (test == "shutdownowner_stage_faults") {
            S5ShutdownOwnerStageFaults();
        } else if (test == "removeconn_dirty_window") {
            S5RemoveConnDirtyWindow();
        } else {
            throw std::runtime_error("unknown test: " + test);
        }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}

} // namespace

#include "ProcessRuntimeTestMain.hpp"

int main(int argc, char** argv)
{
    return bbt_net_test::RunTestProcessExit([&] { return RunCases(argc, argv); });
}
