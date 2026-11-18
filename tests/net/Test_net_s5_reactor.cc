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
        accepted.fetch_add(1);
        std::lock_guard<std::mutex> lock(mutex);
        stored = std::make_shared<Connection>(connection);
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
 * ---------------------------------------------------------------- */
struct ReactorDirect {
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
        driver = std::make_shared<ReactorDriver>();
        reactor = std::make_shared<Reactor>(impl.get(), 0, driver);
        reclaim = std::make_shared<ReclaimRecord>(false);
        state = std::make_shared<ConnectionState>(1, 4096, 1024, 4, impl->SealToken(), reclaim);
        boost::asio::io_context ctx;
        TcpWait desc(ctx);
        core = std::make_shared<ConnCore>(state, std::move(desc), reactor.get(), h);
        state->AttachOwner(reactor, core);
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

/** driver 解绑：跨域 Wake 的临时弱租约提升失败后不登记、不唤醒、不崩溃。 */
static void S5WakeDriverUnbound()
{
    auto h = std::make_shared<CountingHandler>();
    ReactorDirect fx(h);

    // 驾驶员在线，核心未登记：Wake 取得 shared，登记查找失败，不入链，
    // 也不唤醒驾驶员。
    fx.state->WakeOwner();
    CHECK(fx.reactor->DirtyCount() == 0);
    CHECK(fx.driver->WakeCount() == 0);

    // 解绑驾驶员：临时弱租约提升失败，Wake 更早早退，不动脏链也不唤醒。
    fx.reactor->ReleaseDriver();
    fx.state->WakeOwner();
    CHECK(fx.reactor->DirtyCount() == 0);
    CHECK(fx.driver->WakeCount() == 0);

    // 另一组实例：driver 先解绑后再 Wake，结果相同。
    ReactorDirect fx2(h);
    fx2.reactor->ReleaseDriver();
    fx2.state->WakeOwner();
    CHECK(fx2.reactor->DirtyCount() == 0);
    CHECK(fx2.driver->WakeCount() == 0);
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

/* ----------------------------------------------------------------
 * 端到端用例：真实 Engine + loopback TCP，竞态由屏障在目标阶段内制造
 * ---------------------------------------------------------------- */

/** 同一连接重复 Wake 只入链一次。
 *
 * 时序：服务端读入字段后，OwnerWake 唤醒进入 AfterDirtyWeakLock
 * 窗口并被屏障阻塞（已取得 shared，尚未登记）；测试线程再发起 Close()
 * 导致第二次 Wake 登记该核心；释放屏障后，第一次 Wake 重入发现已入链，
 * 不重复登记。阻塞期 driver 线程被占据，脏链不被泵走，DirtyCount 恒
 * 等于 1，确定性证明「重复 Wake 只入链一次」。 */
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
