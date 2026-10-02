#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <string>
#include <sys/syscall.h>
#include <thread>
#include <time.h>
#include <unistd.h>
#include <utility>
#include <vector>

#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/infra/config/FileSource.hpp>
#include <bbt/infra/config/MemorySource.hpp>
#include <bbt/infra/config/Watch.hpp>

using namespace bbt::infra::config;
using bbt::infra::result;
using bbt::infra::ErrorCode;

namespace {

using Scheduler = bbt::coroutine::detail::Scheduler;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

// 进程寿命运行时：本可执行文件只在首次需要时 Start 一次；没有 Stop/
// 重启（Start 一次性，重复 Start 抛 std::logic_error）。旧用例在每个 case
// 里 Start→Stop 的写法在进程寿命契约下不可用，统一经本函数取运行时。
// 资源收口由各对象自己的 Close() 承担，不再依赖运行时代际。
bool EnsureRuntime() {
    auto& sched = Scheduler::GetInstance();
    if (!sched->IsInitialized())
        sched->Start(SCHE_START_OPT_SCHE_THREAD);
    return sched->IsInitialized();
}

Value Map(std::initializer_list<std::pair<std::string, std::string>> kv) {
    Value::Map m;
    for (const auto& p : kv) m.emplace(p.first, p.second);
    return Value::FromMap(std::move(m));
}

struct EventLog {
    std::mutex mtx;
    std::vector<WatchEvent> events;
    void Add(WatchEvent e) {
        std::lock_guard<std::mutex> lk(mtx);
        events.push_back(std::move(e));
    }
    std::size_t Size() {
        std::lock_guard<std::mutex> lk(mtx);
        return events.size();
    }
    WatchEvent At(std::size_t i) {
        std::lock_guard<std::mutex> lk(mtx);
        return events.at(i);
    }
};

bool WaitUntil(const std::function<bool()>& cond, int timeout_ms) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (cond()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return cond();
}

// 非协程测试线程上的定长睡眠：直达内核 syscall，避开协程 Hook 对
// libc nanosleep 的拦截（本文件链接 coroutine，不假设 Hook 未生效）。
void SleepRawMs(int ms) {
    timespec req{ms / 1000, static_cast<long>(ms % 1000) * 1000000L};
    timespec rem{};
    while (::syscall(SYS_nanosleep, &req, &rem) != 0 && errno == EINTR)
        req = rem;
}

std::string MakeTempPath(const char* name) {
    const char* t = std::getenv("TMPDIR");
    std::string dir = (t && *t) ? t : ".";
    std::string tmpl = dir + "/" + name + "_XXXXXX";
    std::vector<char> buf(tmpl.begin(), tmpl.end());
    buf.push_back('\0');
    const int fd = ::mkstemp(buf.data());
    BOOST_REQUIRE(fd >= 0);
    ::close(fd);
    return std::string(buf.data());
}

void WriteFile(const std::string& path, const std::string& content) {
    std::ofstream f(path, std::ios::trunc);
    f << content;
    BOOST_REQUIRE(f.good());
}

std::string MakeTempFile(const char* name, const std::string& content) {
    const std::string path = MakeTempPath(name);
    WriteFile(path, content);
    return path;
}

// 「关闭与正在读取/待投递通知交错」的确定性测试源：Read() 在本次拍内
// 执行测试指定的关闭（不阻塞线程、不 sleep），命中次序由原子计数握手
// 指定。关闭在轮询协程内联调用 Close() —— 契约规定回调/源内可调用
// Close()，封口后本拍已读出的结果被丢弃、循环在下一拍退出。
struct GateSource : ISource {
    std::atomic<int> reads{0};
    std::atomic<Watcher*> closer{nullptr}; // 非 owning：Watcher 生命周期由测试持有
    std::atomic<int> close_on_read{0};     // 命中该读次序时于同一拍内关闭
    std::atomic<bool> close_requested_from_tick{false};
    Snapshot current;

    GateSource() {
        current.ns = "app";
        current.key = "server";
        current.source = "test:gate";
        current.version = 1;
        current.revision = "rev-1";
        current.value = Map({{"a", "1"}});
    }

    result<Snapshot> Read() override {
        const int n = ++reads;
        if (close_on_read.load(std::memory_order_acquire) == n) {
            // 本拍读到的是新版本（其余拍在本窗口内仍返回旧版本，不产生事件），
            // 随后在同一拍内 Close()：该更新事件必须被丢弃。
            current.version = 2;
            current.revision = "rev-2";
            current.value = Map({{"a", "2"}});
            if (auto* w = closer.load(std::memory_order_acquire)) {
                close_requested_from_tick.store(true, std::memory_order_release);
                w->Close();
            }
        }
        return result<Snapshot>::ok(current);
    }

    std::string SourceId() const noexcept override { return "test:gate"; }
};

// 带析构标记的测试源：验证 Close() 之后耐久资源（源）随句柄释放而析构，
// 不依赖被销毁协程的栈展开。
struct TrackedSource : ISource {
    std::shared_ptr<std::atomic_bool> destroyed;
    Snapshot current;
    explicit TrackedSource(std::shared_ptr<std::atomic_bool> d)
        : destroyed(std::move(d)) {
        current.ns = "app";
        current.key = "server";
        current.source = "test:tracked";
        current.value = Map({{"a", "1"}});
    }
    ~TrackedSource() override {
        if (destroyed) destroyed->store(true);
    }
    result<Snapshot> Read() override { return result<Snapshot>::ok(current); }
    std::string SourceId() const noexcept override { return "test:tracked"; }
};

} // namespace

// 未初始化协程运行时（进程寿命下即「从未 Start」）时 Create 返回
// RuntimeUnavailable，不注册协程也不抛异常。必须是本文件第一个用例：
// 任何一次 Start 都会让运行时进入初始化后的常驻状态。
BOOST_AUTO_TEST_CASE(watch_create_without_running_scheduler) {
    auto& sched = Scheduler::GetInstance();
    if (sched->IsInitialized()) {
        BOOST_TEST_MESSAGE("skipped: coroutine runtime already initialized (test order changed)");
        return;
    }
    auto source = std::make_shared<MemorySource>("app", "server");
    auto r = Watcher::Create(source, [](const WatchEvent&) {}, WatchOptions{});
    BOOST_REQUIRE(!r);
    BOOST_CHECK(static_cast<int>(r.error().code) ==
                static_cast<int>(ErrorCode::RuntimeUnavailable));
}

BOOST_AUTO_TEST_CASE(watch_initial_dedup_update_memory) {
    BOOST_REQUIRE(EnsureRuntime());

    auto source = std::make_shared<MemorySource>("app", "server");
    source->Set(Map({{"server.port", "8080"}}));

    auto log = std::make_shared<EventLog>();
    WatchOptions opt;
    opt.poll_interval = std::chrono::milliseconds(10);
    auto wres = Watcher::Create(
        source, [log](const WatchEvent& e) { log->Add(e); }, opt);
    BOOST_REQUIRE(wres);
    auto watcher = std::move(wres).value();

    // 初始快照
    BOOST_REQUIRE(WaitUntil([&] { return log->Size() >= 1; }, 2000));
    auto e0 = log->At(0);
    BOOST_CHECK(e0.kind == WatchEventKind::Initial);
    BOOST_CHECK(e0.snapshot.version == 1u);

    // 重复版本去重：多拍轮询后仍只有 Initial
    SleepRawMs(60);
    BOOST_CHECK(log->Size() == 1u);

    // 版本更新
    source->Set(Map({{"server.port", "9090"}}));
    BOOST_REQUIRE(WaitUntil([&] { return log->Size() >= 2; }, 2000));
    auto e1 = log->At(1);
    BOOST_CHECK(e1.kind == WatchEventKind::Updated);
    BOOST_CHECK(e1.snapshot.version == 2u);
    auto port = e1.snapshot.value.GetInt64("server.port");
    BOOST_REQUIRE(port);
    BOOST_CHECK_EQUAL(port.value(), 9090);

    // 显式 Close：返回即轮询协程已退出、等待与定时器已释放，物理收口落定。
    watcher->Close();
    BOOST_CHECK(watcher->IsClosed());
}

BOOST_AUTO_TEST_CASE(watch_no_callback_after_close) {
    BOOST_REQUIRE(EnsureRuntime());

    auto source = std::make_shared<MemorySource>("app", "server");
    source->Set(Map({{"a", "1"}}));

    auto log = std::make_shared<EventLog>();
    WatchOptions opt;
    opt.poll_interval = std::chrono::milliseconds(10);
    auto wres = Watcher::Create(
        source, [log](const WatchEvent& e) { log->Add(e); }, opt);
    BOOST_REQUIRE(wres);
    auto watcher = std::move(wres).value();

    BOOST_REQUIRE(WaitUntil([&] { return log->Size() >= 1; }, 2000));

    watcher->Close();
    BOOST_CHECK(watcher->IsClosed());

    // 关闭后更新源 → 晚到变更不得再回调消费者；轮询协程已退出。
    source->Set(Map({{"a", "2"}}));
    SleepRawMs(60);
    BOOST_CHECK(log->Size() == 1u);
    BOOST_CHECK(watcher->IsClosed());
}

// 关闭落在「本次拍 Read 已完成、事件尚未投递」之间：该事件必须被丢弃，
// 关闭后不再调用消费者；循环退出后源不再被读取（有界窗口内的负向检查）。
BOOST_AUTO_TEST_CASE(watch_close_during_inflight_read_drops_pending_event) {
    BOOST_REQUIRE(EnsureRuntime());

    auto source = std::make_shared<GateSource>();
    auto log = std::make_shared<EventLog>();
    WatchOptions opt;
    opt.poll_interval = std::chrono::milliseconds(5);
    auto wres = Watcher::Create(
        source, [log](const WatchEvent& e) { log->Add(e); }, opt);
    BOOST_REQUIRE(wres);
    auto watcher = std::move(wres).value();

    BOOST_REQUIRE(WaitUntil([&] { return log->Size() >= 1; }, 2000));
    BOOST_CHECK(log->At(0).kind == WatchEventKind::Initial);

    // 武装：下一拍在 Read 内（投递之前）Close()。
    source->closer.store(watcher.get(), std::memory_order_release);
    source->close_on_read.store(source->reads.load() + 1, std::memory_order_release);

    // 关闭在轮询协程内发生：封口后 IsClosed 立即为真。
    BOOST_REQUIRE(WaitUntil([&] { return watcher->IsClosed(); }, 3000));

    // 交错确实发生在 tick 内（读→投递之间的窗口）
    BOOST_CHECK(source->close_requested_from_tick.load());
    // 待投递的更新事件被丢弃：消费者只收到 Initial
    BOOST_CHECK_EQUAL(log->Size(), 1u);

    // 循环协程已退出：有界窗口内源不再被读取
    const int reads_after_close = source->reads.load();
    const bool ticked_again =
        WaitUntil([&] { return source->reads.load() > reads_after_close; }, 100);
    BOOST_CHECK(!ticked_again);
    BOOST_CHECK_EQUAL(log->Size(), 1u);
}

BOOST_AUTO_TEST_CASE(watch_failure_recovery_file) {
    BOOST_REQUIRE(EnsureRuntime());

    const std::string path =
        MakeTempFile("cfg_watch", "version = 1\nserver.port = 8080\n");
    auto fsrc = FileSource::Create(path, "app", "server");
    BOOST_REQUIRE(fsrc);
    auto source = std::static_pointer_cast<ISource>(fsrc.value());

    auto log = std::make_shared<EventLog>();
    WatchOptions opt;
    opt.poll_interval = std::chrono::milliseconds(10);
    auto wres = Watcher::Create(
        source, [log](const WatchEvent& e) { log->Add(e); }, opt);
    BOOST_REQUIRE(wres);
    auto watcher = std::move(wres).value();

    // 初始
    BOOST_REQUIRE(WaitUntil([&] { return log->Size() >= 1; }, 2000));
    BOOST_CHECK(log->At(0).kind == WatchEventKind::Initial);

    // 删除文件 → Failed（NotFound），不发布伪成功快照
    ::unlink(path.c_str());
    BOOST_REQUIRE(WaitUntil([&] { return log->Size() >= 2; }, 2000));
    auto e_fail = log->At(1);
    BOOST_CHECK(e_fail.kind == WatchEventKind::Failed);
    BOOST_CHECK(static_cast<int>(e_fail.error.code) ==
                static_cast<int>(ErrorCode::NotFound));

    // 失败去重：持续失败不重复通知
    SleepRawMs(60);
    BOOST_CHECK(log->Size() == 2u);

    // 恢复 → Recovered
    WriteFile(path, "version = 2\nserver.port = 9090\n");
    BOOST_REQUIRE(WaitUntil([&] { return log->Size() >= 3; }, 2000));
    auto e_rec = log->At(2);
    BOOST_CHECK(e_rec.kind == WatchEventKind::Recovered);
    BOOST_CHECK(e_rec.snapshot.version == 2u);

    // 格式错误 → Failed（ProtocolError），不发布伪成功
    WriteFile(path, "version = abc\n");
    BOOST_REQUIRE(WaitUntil([&] { return log->Size() >= 4; }, 2000));
    auto e_mal = log->At(3);
    BOOST_CHECK(e_mal.kind == WatchEventKind::Failed);
    BOOST_CHECK(static_cast<int>(e_mal.error.code) ==
                static_cast<int>(ErrorCode::ProtocolError));

    watcher->Close();
    BOOST_CHECK(watcher->IsClosed());
    ::unlink(path.c_str());
}

// 资源边界（进程寿命修订版）：轮询协程确定性地挂起在 poll_interval 睡眠中
// （初始投递后循环进入 200ms 挂起），此时 Close() 必须封口并唤醒它、在其
// 调用线程上有界等它退出，返回即 IsClosed；释放句柄后源（耐久资源）由
// 句柄析构释放——不再用 Scheduler::Stop 的硬停/栈不展开口径。
BOOST_AUTO_TEST_CASE(watch_close_stops_loop_and_releases_source) {
    BOOST_REQUIRE(EnsureRuntime());

    auto destroyed = std::make_shared<std::atomic_bool>(false);
    auto source = std::make_shared<TrackedSource>(destroyed);
    auto log = std::make_shared<EventLog>();
    WatchOptions opt;
    // 较大间隔：初始投递立即发生，随后循环确定性地挂起在睡眠里。
    opt.poll_interval = std::chrono::milliseconds(200);
    auto wres = Watcher::Create(
        source, [log](const WatchEvent& e) { log->Add(e); }, opt);
    BOOST_REQUIRE(wres);
    auto watcher = std::move(wres).value();
    source.reset(); // 测试侧句柄释放：源只由 Watcher 持有，便于观察析构

    BOOST_REQUIRE(WaitUntil([&] { return log->Size() >= 1; }, 2000));

    // 初始投递后循环已进入 200ms 挂起；再等 100ms，确保 Close 命中挂起点。
    SleepRawMs(100);
    BOOST_CHECK(!destroyed->load());   // Close 前：源仍由句柄持有

    // 显式 Close：封口 + 唤醒挂起的轮询协程 + 有界等待其退出。
    watcher->Close();
    BOOST_CHECK(watcher->IsClosed());

    // 关闭后源不再被读取（循环已退出，有界负向检查）。
    const int reads_after_close = log->Size();
    SleepRawMs(50);
    BOOST_CHECK_EQUAL(log->Size(), reads_after_close);

    // 释放句柄 → 源的强引用（Impl 持有）随之释放，由调用方线程析构。
    watcher.reset();
    BOOST_REQUIRE(WaitUntil([&] { return destroyed->load(); }, 1000));
    BOOST_CHECK(destroyed->load());

    BOOST_CHECK(EnsureRuntime()); // 收尾后运行时仍常驻（进程寿命）
}
