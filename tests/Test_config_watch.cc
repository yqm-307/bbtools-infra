#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MAIN
#include <boost/test/included/unit_test.hpp>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unistd.h>
#include <utility>
#include <vector>

#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/sync/Cancellation.hpp>
#include <bbt/coroutine/syntax/SyntaxMacro.hpp>
#include <bbt/infra/config/FileSource.hpp>
#include <bbt/infra/config/MemorySource.hpp>
#include <bbt/infra/config/Watch.hpp>

using namespace bbt::infra::config;
using bbt::infra::result;
using bbt::infra::ErrorCode;
using bbt::infra::CloseStatus;

namespace {

using Scheduler = bbt::coroutine::detail::Scheduler;
using bbt::coroutine::SCHE_START_OPT_SCHE_THREAD;

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

CloseStatus WaitClosedInCoroutine(Watcher::SPtr watcher) {
    std::atomic<int> status{-1};
    bbtco [watcher, &status]() {
        watcher->RequestClose();
        status.store(static_cast<int>(watcher->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::milliseconds(2000), {})));
    };
    WaitUntil([&] { return status.load() != -1; }, 3000);
    return static_cast<CloseStatus>(status.load());
}

// 在协程内等待关闭（不在此处发起 RequestClose）：关闭时机由调用方控制，
// 用于确定性验证「关闭请求 → drain → Closed」而不用 sleep 近似时序。
CloseStatus AwaitClosedInCoroutine(Watcher::SPtr watcher,
                                   std::chrono::milliseconds budget) {
    std::atomic<int> status{-1};
    bbtco [watcher, budget, &status]() {
        status.store(static_cast<int>(watcher->WaitClosed(
            std::chrono::steady_clock::now() + budget, {})));
    };
    WaitUntil([&] { return status.load() != -1; },
              static_cast<int>(budget.count()) + 1000);
    return static_cast<CloseStatus>(status.load());
}

// 「关闭与正在读取/待投递通知交错」的确定性测试源：Read() 在本次拍内
// 执行测试指定的关闭请求（不阻塞线程、不 sleep），命中次序由原子计数握手
// 指定，因此不需要近似挂起点的等待。
struct GateSource : ISource {
    std::atomic<int> reads{0};
    std::atomic<Watcher*> closer{nullptr}; // 非 owning：Watcher 生命周期由测试持有
    std::atomic<int> close_on_read{0};     // 命中该读次序时于同一拍内请求关闭
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
            // 随后在同一拍内请求关闭：该更新事件必须被丢弃。
            current.version = 2;
            current.revision = "rev-2";
            current.value = Map({{"a", "2"}});
            if (auto* w = closer.load(std::memory_order_acquire)) {
                close_requested_from_tick.store(true, std::memory_order_release);
                w->RequestClose();
            }
        }
        return result<Snapshot>::ok(current);
    }

    std::string SourceId() const noexcept override { return "test:gate"; }
};

// ③ 回归：未启动调度器（代际 0）时 Create 返回 RuntimeUnavailable，不注册协程
//   也不抛异常。必须是本文件第一个用例：任何 Start 都会改变运行时代际。
BOOST_AUTO_TEST_CASE(watch_create_without_running_scheduler) {
    auto& sched = Scheduler::GetInstance();
    if (sched->IsRunning()) {
        BOOST_TEST_MESSAGE("skipped: scheduler already running (test order changed)");
        return;
    }
    auto source = std::make_shared<MemorySource>("app", "server");
    auto r = Watcher::Create(source, [](const WatchEvent&) {}, WatchOptions{});
    BOOST_REQUIRE(!r);
    BOOST_CHECK(static_cast<int>(r.error().code) ==
                static_cast<int>(ErrorCode::RuntimeUnavailable));
}

// 正常 drain + 单等待位握手：第二个 WaitClosed 返回 AlreadyWaiting 即证明
// 首个等待已建立（不用 sleep 近似）；RequestClose 后首个等待必须返回 Closed。
BOOST_AUTO_TEST_CASE(watch_waitclosed_single_waiter_and_drain) {
    auto& sched = Scheduler::GetInstance();
    sched->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(sched->IsRunning());

    auto source = std::make_shared<MemorySource>("app", "server");
    source->Set(Map({{"a", "1"}}));
    auto log = std::make_shared<EventLog>();
    WatchOptions opt;
    opt.poll_interval = std::chrono::milliseconds(5);
    auto wres = Watcher::Create(
        source, [log](const WatchEvent& e) { log->Add(e); }, opt);
    BOOST_REQUIRE(wres);
    auto watcher = std::move(wres).value();
    BOOST_REQUIRE(WaitUntil([&] { return log->Size() >= 1; }, 2000));

    std::atomic<int> first{-1};
    std::atomic<int> second{-1};
    bbtco [watcher, &first]() {
        // 等待位只有一个：先注册不等于先占位，若被第二个协程抢到则重试，
        // 直到本协程占住等待位（拿到 Closed 才返回）。原实现假定投递顺序
        // 必为「先注册者先占位」，顺序反转时断言偶发失败。
        const auto limit = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        while (std::chrono::steady_clock::now() < limit) {
            const auto status = watcher->WaitClosed(
                std::chrono::steady_clock::now() + std::chrono::seconds(30), {});
            if (status == CloseStatus::Closed) {
                first.store(static_cast<int>(status));
                return;
            }
        }
    };
    bbtco [watcher, &second]() {
        // 观察到一次 AlreadyWaiting 即证明等待位已被首个 waiter 占住；
        // 有限重试只用于吸收两个协程的投递顺序竞争。
        for (int i = 0; i < 200; ++i) {
            const auto status = watcher->WaitClosed(
                std::chrono::steady_clock::now() + std::chrono::milliseconds(50), {});
            second.store(static_cast<int>(status), std::memory_order_release);
            if (status == CloseStatus::AlreadyWaiting)
                return;
        }
    };
    BOOST_REQUIRE(WaitUntil(
        [&] { return second.load(std::memory_order_acquire) ==
                     static_cast<int>(CloseStatus::AlreadyWaiting); },
        3000));
    BOOST_CHECK(first.load() == -1); // 首个等待仍在途，未被 AlreadyWaiting 取消

    watcher->RequestClose();
    BOOST_REQUIRE(WaitUntil([&] { return first.load() != -1; }, 3000));
    BOOST_CHECK(first.load() == static_cast<int>(CloseStatus::Closed));
    BOOST_CHECK(watcher->IsClosed());
    sched->Stop();
}

// ② 回归：调用方令牌取消映射为 Cancelled（不是 RuntimeUnavailable），
//   且超时/取消不撤销关闭——随后仍能正常 drain 到 Closed。
BOOST_AUTO_TEST_CASE(watch_waitclosed_cancelled_mapping) {
    auto& sched = Scheduler::GetInstance();
    sched->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(sched->IsRunning());

    auto source = std::make_shared<MemorySource>("app", "server");
    source->Set(Map({{"a", "1"}}));
    auto log = std::make_shared<EventLog>();
    WatchOptions opt;
    opt.poll_interval = std::chrono::milliseconds(5);
    auto wres = Watcher::Create(
        source, [log](const WatchEvent& e) { log->Add(e); }, opt);
    BOOST_REQUIRE(wres);
    auto watcher = std::move(wres).value();
    BOOST_REQUIRE(WaitUntil([&] { return log->Size() >= 1; }, 2000));

    bbt::coroutine::CancellationSource cancel;
    cancel.RequestCancel();
    const bbt::coroutine::CancellationToken token = cancel.Token();
    std::atomic<int> status{-1};
    bbtco [watcher, token, &status]() {
        status.store(static_cast<int>(watcher->WaitClosed(
            std::chrono::steady_clock::now() + std::chrono::milliseconds(500),
            token)));
    };
    BOOST_REQUIRE(WaitUntil([&] { return status.load() != -1; }, 3000));
    BOOST_CHECK(status.load() == static_cast<int>(CloseStatus::Cancelled));
    BOOST_CHECK(!watcher->IsClosed()); // 取消不撤销关闭、不代表资源已释放

    // 取消过的关闭请求仍可正常 drain（取消不撤销关闭）
    watcher->RequestClose();
    BOOST_CHECK(AwaitClosedInCoroutine(watcher, std::chrono::milliseconds(3000)) ==
                CloseStatus::Closed);
    BOOST_CHECK(watcher->IsClosed());
    sched->Stop();
}

// 关闭请求落在「本次拍 Read 已完成、事件尚未投递」之间：该事件必须被丢弃，
// 关闭后不再调用消费者；drain 后循环不再读取源（有界窗口内的负向检查）。
BOOST_AUTO_TEST_CASE(watch_close_during_inflight_read_drops_pending_event) {
    auto& sched = Scheduler::GetInstance();
    sched->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(sched->IsRunning());

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

    // 武装：下一拍在 Read 内（投递之前）请求关闭
    source->closer.store(watcher.get(), std::memory_order_release);
    source->close_on_read.store(source->reads.load() + 1, std::memory_order_release);

    BOOST_CHECK(AwaitClosedInCoroutine(watcher, std::chrono::milliseconds(3000)) ==
                CloseStatus::Closed);

    // 交错确实发生在 tick 内（读→投递之间的窗口）
    BOOST_CHECK(source->close_requested_from_tick.load());
    // 待投递的更新事件被丢弃：消费者只收到 Initial
    BOOST_CHECK_EQUAL(log->Size(), 1u);

    // drain 后循环协程已退出：有界窗口内源不再被读取
    const int reads_after_close = source->reads.load();
    const bool ticked_again =
        WaitUntil([&] { return source->reads.load() > reads_after_close; }, 100);
    BOOST_CHECK(!ticked_again);
    BOOST_CHECK_EQUAL(log->Size(), 1u);
    sched->Stop();
}

BOOST_AUTO_TEST_CASE(watch_initial_dedup_update_memory) {
    auto& sched = Scheduler::GetInstance();
    sched->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(sched->IsRunning());

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
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
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

    // 正常 drain
    const auto cs = WaitClosedInCoroutine(watcher);
    BOOST_CHECK(cs == CloseStatus::Closed);
    BOOST_CHECK(watcher->IsClosed());
    sched->Stop();
}

BOOST_AUTO_TEST_CASE(watch_no_callback_after_close) {
    auto& sched = Scheduler::GetInstance();
    sched->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(sched->IsRunning());

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

    const auto cs = WaitClosedInCoroutine(watcher);
    BOOST_CHECK(cs == CloseStatus::Closed);

    // 关闭后更新源 → 晚到变更不得再回调消费者
    source->Set(Map({{"a", "2"}}));
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
    BOOST_CHECK(log->Size() == 1u);
    sched->Stop();
}

BOOST_AUTO_TEST_CASE(watch_failure_recovery_file) {
    auto& sched = Scheduler::GetInstance();
    sched->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(sched->IsRunning());

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
    std::this_thread::sleep_for(std::chrono::milliseconds(60));
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

    const auto cs = WaitClosedInCoroutine(watcher);
    BOOST_CHECK(cs == CloseStatus::Closed);
    ::unlink(path.c_str());
    sched->Stop();
}

// 带析构标记的测试源：验证强制 Stop 后耐久资源由调用方句柄释放，
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

BOOST_AUTO_TEST_CASE(watch_force_stop_resource_boundary) {
    auto& sched = Scheduler::GetInstance();
    sched->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(sched->IsRunning());

    auto destroyed = std::make_shared<std::atomic_bool>(false);
    auto source = std::make_shared<TrackedSource>(destroyed);
    auto log = std::make_shared<EventLog>();
    WatchOptions opt;
    // 较大间隔：初始投递立即发生，随后循环确定性地挂起在 bbtco_sleep。
    opt.poll_interval = std::chrono::milliseconds(200);
    auto wres = Watcher::Create(
        source, [log](const WatchEvent& e) { log->Add(e); }, opt);
    BOOST_REQUIRE(wres);
    auto watcher = std::move(wres).value();
    source.reset(); // 测试侧句柄释放：源只由 Watcher 持有，便于观察析构

    BOOST_REQUIRE(WaitUntil([&] { return log->Size() >= 1; }, 2000));

    // 初始投递后循环已进入 200ms 挂起；再等 100ms，确保 Stop 命中挂起点
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // 强制 Stop：直接销毁挂起协程，不做栈展开（契约 §6 显式例外）
    sched->Stop();
    BOOST_CHECK(!sched->IsRunning());
    BOOST_CHECK(!watcher->IsClosed()); // 循环从未 drain，不伪装 Closed
    BOOST_CHECK(!destroyed->load());   // 源仍由句柄持有

    // 释放句柄 → 耐久资源（源）由调用方线程析构，不依赖被销毁协程的栈展开
    watcher.reset();
    BOOST_REQUIRE(WaitUntil([&] { return destroyed->load(); }, 1000));
    BOOST_CHECK(destroyed->load());
}

// 父验收红色探针的仓内确定性回归：WaitClosed 在途时强制 Stop，随后释放
// 外部句柄，配置源必须随之析构（修复前实测 source_destroyed_after_release=0）。
// 第二个 WaitClosed 返回 AlreadyWaiting 用作「首个等待已建立」的握手，
// 不以 sleep 近似挂起点；协程只捕获裸指针，测试自身不成为源的持有者。
BOOST_AUTO_TEST_CASE(watch_force_stop_inflight_waitclosed_releases_source) {
    auto& sched = Scheduler::GetInstance();
    sched->Start(SCHE_START_OPT_SCHE_THREAD);
    BOOST_REQUIRE(sched->IsRunning());

    auto destroyed = std::make_shared<std::atomic_bool>(false);
    auto source = std::make_shared<TrackedSource>(destroyed);
    auto log = std::make_shared<EventLog>();
    WatchOptions opt;
    opt.poll_interval = std::chrono::milliseconds(200);
    auto wres = Watcher::Create(
        source, [log](const WatchEvent& e) { log->Add(e); }, opt);
    BOOST_REQUIRE(wres);
    auto watcher = std::move(wres).value();
    source.reset(); // 测试侧释放源：此后只可能由库/句柄持有

    BOOST_REQUIRE(WaitUntil([&] { return log->Size() >= 1; }, 2000));

    Watcher* raw = watcher.get();
    std::atomic<int> first{-1};
    std::atomic<int> second{-1};
    bbtco [raw, &first]() {
        // 等待位只有一个：先注册不等于先占位，若被第二个协程抢到则重试，
        // 直到本协程占住等待位（拿到 Closed 才返回）。原实现假定投递顺序
        // 必为「先注册者先占位」，顺序反转时断言偶发失败。
        const auto limit = std::chrono::steady_clock::now() + std::chrono::seconds(20);
        while (std::chrono::steady_clock::now() < limit) {
            const auto status = raw->WaitClosed(
                std::chrono::steady_clock::now() + std::chrono::seconds(60), {});
            if (status == CloseStatus::Closed) {
                first.store(static_cast<int>(status));
                return;
            }
        }
    };
    bbtco [raw, &second]() {
        // 观察到一次 AlreadyWaiting 即证明等待位已被首个 waiter 占住；
        // 有限重试只用于吸收两个协程的投递顺序竞争。
        for (int i = 0; i < 200; ++i) {
            const auto status = raw->WaitClosed(
                std::chrono::steady_clock::now() + std::chrono::milliseconds(50), {});
            second.store(static_cast<int>(status), std::memory_order_release);
            if (status == CloseStatus::AlreadyWaiting)
                return;
        }
    };
    BOOST_REQUIRE(WaitUntil(
        [&] { return second.load(std::memory_order_acquire) ==
                     static_cast<int>(CloseStatus::AlreadyWaiting); },
        3000));
    BOOST_CHECK(first.load() == -1); // 首个等待仍在途：挂起点已建立

    // 强制 Stop：直接销毁挂起协程，不做栈展开（契约 §6 显式例外）
    sched->Stop();
    BOOST_CHECK(!sched->IsRunning());
    BOOST_CHECK(!watcher->IsClosed()); // 循环从未 drain，不伪装 Closed
    BOOST_CHECK(!destroyed->load());   // 源仍由句柄强持有

    watcher.reset(); // 释放外部句柄
    if (!destroyed->load())
        BOOST_TEST_MESSAGE("source released after coroutine reclaim (not synchronous)");
    BOOST_REQUIRE(WaitUntil([&] { return destroyed->load(); }, 1000));
    BOOST_CHECK(destroyed->load());
}

} // namespace
