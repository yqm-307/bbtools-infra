#include <bbt/infra/net/detail/CoIoOperation.hpp>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp> // 仅非法 TLS-idle 测试使用。
#include <bbt/coroutine/io/IoExecutor.hpp>

using namespace bbt::infra::net;
using namespace std::chrono_literals;
using bbt::coroutine::WaitOptions;
using bbt::coroutine::WaitStatus;

static void Check(bool value, const char* expression, int line) {
    if (!value) { throw std::runtime_error("line " + std::to_string(line) + ": " + expression); }
}
#define CHECK(expression) Check(static_cast<bool>(expression), #expression, __LINE__)

template<class T> static T Get(std::future<T>& future) {
    CHECK(future.wait_for(5s) == std::future_status::ready);
    return future.get();
}
static void Get(std::future<void>& future) {
    CHECK(future.wait_for(5s) == std::future_status::ready);
    future.get();
}
static std::future<void> Task(std::function<void()> callback) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    g_scheduler->RegistCoroutineTask(65536, [promise, callback] {
        try { callback(); promise->set_value(); }
        catch (...) { promise->set_exception(std::current_exception()); }
    }, "net.reactor");
    return future;
}
static void OnOwner(EventLoop& loop, std::function<void()> callback) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    CHECK(loop.Submit([promise, callback] {
        try { callback(); promise->set_value(); }
        catch (...) { promise->set_exception(std::current_exception()); }
    }) == SubmitStatus::Accepted);
    Get(future);
}
static WaitOptions Until(std::chrono::milliseconds duration) {
    return {std::chrono::steady_clock::now() + duration};
}
static std::size_t Count(const char* directory) {
    return static_cast<std::size_t>(std::distance(std::filesystem::directory_iterator(directory),
        std::filesystem::directory_iterator{}));
}
struct RuntimeIdentity { std::thread::id scheduler; std::thread::id processer; const void* context; };
static RuntimeIdentity WarmRuntime() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 1;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start(); // 每进程一次；没有 Stop 或第二个 driver。
    RuntimeIdentity identity;
    auto warm = Task([&identity] {
        identity.processer = std::this_thread::get_id();
        bbt::coroutine::sync::CoWaiter waiter;
        CHECK(waiter.WaitWithCallback(Until(1000ms), [&waiter] {
            CHECK(waiter.Notify(bbt::coroutine::CoEventValue::From(7)) == 0);
            return true;
        }) == WaitStatus::Completed);
        CHECK(waiter.Wait(Until(5ms)) == WaitStatus::TimedOut);
    });
    Get(warm);
    std::promise<void> posted;
    auto ready = posted.get_future();
    auto executor = bbt::coroutine::io::GetExecutor();
    identity.context = &executor.context();
    boost::asio::post(executor, [&identity, &posted] {
        identity.scheduler = std::this_thread::get_id();
        posted.set_value();
    });
    Get(ready);
    CHECK(identity.scheduler != identity.processer);
    std::this_thread::sleep_for(30ms);
    return identity;
}

static void Owner() {
    const auto runtime = WarmRuntime();
    EventLoop first(2, 1), second(8, 1);
    CHECK(first.Submit([] {}) == SubmitStatus::NotStarted);
    CHECK(first.Submit({}) == SubmitStatus::Invalid);
    bool invalid = false;
    try { EventLoop zero(0); } catch (const std::invalid_argument&) { invalid = true; }
    CHECK(invalid);
    CHECK(first.Start() && second.Start());
    CHECK(!first.Start());
    bool context_rejected = false;
    try { first.Context(); } catch (const std::logic_error&) { context_rejected = true; }
    CHECK(context_rejected);
    OnOwner(first, [&first] { CHECK(first.IsOwner()); CHECK(&first.Context() == first.ContextIdentity()); });
    OnOwner(second, [&second] { CHECK(second.IsOwner()); });
    CHECK(first.OwnerId() != second.OwnerId());
    CHECK(first.ContextIdentity() != second.ContextIdentity());
    for (auto* loop : {&first, &second}) {
        CHECK(loop->OwnerId() != runtime.scheduler && loop->OwnerId() != runtime.processer);
        CHECK(loop->ContextIdentity() != runtime.context);
    }
    std::promise<void> entered, release;
    auto entered_future = entered.get_future();
    auto release_future = release.get_future().share();
    CHECK(first.Submit([&entered, release_future] {
        entered.set_value();
        CHECK(release_future.wait_for(2s) == std::future_status::ready); // fixture 有界 barrier。
    }) == SubmitStatus::Accepted);
    Get(entered_future);
    std::vector<int> order;
    CHECK(first.Submit([&order] { order.push_back(1); }) == SubmitStatus::Accepted);
    CHECK(first.Submit([&order] { order.push_back(2); }) == SubmitStatus::Accepted);
    std::atomic<int> rejected_effects{0};
    CHECK(first.Submit([&rejected_effects] { ++rejected_effects; }) == SubmitStatus::Full);
    first.RequestStop();
    CHECK(first.Submit([&rejected_effects] { ++rejected_effects; }) == SubmitStatus::Sealed);
    release.set_value();
    CHECK(first.Join());
    CHECK((order == std::vector<int>{1, 2}) && rejected_effects == 0);
    CHECK(first.FailureCount() == 0);

    // 短批 drain 必须让已经排队的 Asio handler 在后续命令前运行。
    std::atomic<int> commands{0};
    std::promise<int> between;
    auto between_future = between.get_future();
    OnOwner(second, [&second, &commands, &between] {
        boost::asio::post(second.Context(), [&commands, &between] { between.set_value(commands.load()); });
        for (int i = 0; i < 4; ++i) {
            CHECK(second.Submit([&commands] { ++commands; }) == SubmitStatus::Accepted);
        }
    });
    CHECK(Get(between_future) == 0);
    OnOwner(second, [&second] {
        boost::asio::post(second.Context(), [] { throw std::runtime_error("asio failure observed"); });
    });
    CHECK(second.Submit([] { throw std::runtime_error("command failure observed"); }) == SubmitStatus::Accepted);
    OnOwner(second, [] {});
    second.RequestStop();
    CHECK(second.Join());
    CHECK(commands == 4 && second.FailureCount() == 2 && second.LastFailure());
    std::cout << "owner main=" << first.OwnerId() << " sub=" << second.OwnerId()
        << " scheduler=" << runtime.scheduler << " processer=" << runtime.processer
        << " contexts=" << first.ContextIdentity() << ',' << second.ContextIdentity()
        << ',' << runtime.context << " full/sealed/order/exactly-once/fairness/exceptions=ok\n";
}

static NativeSocket Open() {
    std::error_code error;
    auto socket = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    CHECK(!error && socket.Fd() >= 0);
    return socket;
}

static void Bridge() {
    const auto runtime = WarmRuntime();
    EventLoop accepting(8), receiving(8);
    CHECK(accepting.Start() && receiving.Start());
    std::atomic<int> effects{0};
    CHECK(CoIoOperation::Run(receiving, Until(1s), [&effects](auto) { ++effects; }).wait_status
        == WaitStatus::InvalidContext);
    std::exception_ptr idle_error;
    std::thread idle([&] {
        try {
            bbt::coroutine::detail::LocalThread::GetTLSInst()->SetEnableUseCo(true);
            CHECK(CoIoOperation::Run(receiving, Until(1s), [&effects](auto) { ++effects; }).wait_status
                == WaitStatus::InvalidContext);
        } catch (...) { idle_error = std::current_exception(); }
    });
    idle.join();
    if (idle_error) { std::rethrow_exception(idle_error); }
    CHECK(effects == 0);

    auto listener = std::make_shared<NativeSocket>();
    Address bound;
    OnOwner(accepting, [&] {
        *listener = Open();
        std::error_code error;
        CHECK(!listener->Bind(Address::Ip("127.0.0.1", 0, error)) && !error);
        CHECK(!listener->Listen(8));
        bound = listener->LocalAddress(error);
        CHECK(!error);
    });
    auto client = Open();
    const auto connected = client.Connect(bound);
    CHECK(!connected || connected.value() == EINPROGRESS);
    auto accepted = std::make_shared<NativeSocket>();
    OnOwner(accepting, [&] {
        Address peer;
        std::error_code error;
        *accepted = listener->Accept(peer, error);
        CHECK(!error && accepted->Fd() >= 0);
        CHECK(!listener->Close());
    });
    CHECK(!client.PendingError());
    std::shared_ptr<boost::asio::ip::tcp::socket> socket;
    OnOwner(receiving, [&] {
        socket = std::make_shared<boost::asio::ip::tcp::socket>(receiving.Context());
        boost::system::error_code error;
        socket->assign(boost::asio::ip::tcp::v4(), accepted->Fd(), error);
        CHECK(!error);
        (void)accepted->Release(); // assign 成功后唯一 fd 所有权移给 Asio。
        socket->non_blocking(true, error);
        CHECK(!error && accepted->Fd() == -1);
    });
    std::promise<void> registered;
    auto registered_future = registered.get_future();
    std::atomic_bool resumed{false};
    std::thread::id before, after, callback_thread;
    auto read = Task([&] {
        before = std::this_thread::get_id();
        const auto id = bbt::coroutine::GetLocalCoroutineId();
        auto result = CoIoOperation::Run(receiving, Until(2s), [&, socket](auto operation) {
            CHECK(receiving.IsOwner());
            auto data = std::make_shared<std::string>(4, '\0');
            socket->async_wait(boost::asio::ip::tcp::socket::wait_read, [&, socket, operation, data](auto error) {
                callback_thread = std::this_thread::get_id();
                CHECK(operation->CompleteFrom([&]() -> IoResult {
                    CHECK(receiving.IsOwner() && bbt::coroutine::GetLocalCoroutineId() == 0);
                    if (error) { return {0, std::error_code(error.value(), std::system_category())}; }
                    boost::system::error_code read_error;
                    const auto bytes = socket->read_some(boost::asio::buffer(*data), read_error);
                    CHECK(!read_error && *data == "ping");
                    return {bytes, {}};
                }));
                CHECK(!operation->Complete({999, {}}));
            });
            registered.set_value();
        });
        after = std::this_thread::get_id();
        CHECK(bbt::coroutine::GetLocalCoroutineId() == id);
        CHECK(result.wait_status == WaitStatus::Completed && result.submit_status == SubmitStatus::Accepted);
        CHECK(!result.io.error && result.io.bytes == 4);
        CHECK(after != receiving.OwnerId() && after != runtime.scheduler);
        resumed = true;
    });
    Get(registered_future);
    auto progress = Task([&] { CHECK(!resumed); ++effects; });
    Get(progress); // 单 Processer：另一业务协程完成，证明 read 真让出 worker。
    CHECK(!resumed);
    CHECK(client.Send("ping", 4).bytes == 4);
    Get(read);
    CHECK(effects == 1 && callback_thread == receiving.OwnerId());
    CHECK(before == runtime.processer && after == runtime.processer); // fixture 仅一个 worker，产品不固定归属。

    // 外部线程完成、重复首胜、owner 即时完成以及 start 抛出都经同一登记路径。
    std::promise<std::shared_ptr<CoIoOperation>> external;
    auto external_future = external.get_future();
    auto external_task = Task([&] {
        auto result = CoIoOperation::Run(receiving, Until(1s), [&external](auto operation) {
            external.set_value(operation);
        });
        CHECK(result.wait_status == WaitStatus::Completed && result.io.bytes == 17);
    });
    auto external_op = Get(external_future);
    CHECK(external_op->Complete({17, {}}));
    CHECK(!external_op->Complete({18, {}}));
    external_op.reset();
    Get(external_task);
    auto immediate = Task([&] {
        for (int i = 0; i < 100; ++i) {
            auto result = CoIoOperation::Run(receiving, Until(1s), [i](auto operation) {
                CHECK(operation->Complete({static_cast<std::size_t>(i), {}}));
            });
            CHECK(result.wait_status == WaitStatus::Completed && result.io.bytes == static_cast<std::size_t>(i));
        }
        auto expired = CoIoOperation::Run(receiving, Until(-1ms), [&effects](auto) { ++effects; });
        CHECK(expired.wait_status == WaitStatus::TimedOut && expired.submit_status == SubmitStatus::NotSubmitted);
        auto failed = CoIoOperation::Run(receiving, Until(1s), [](auto) { throw std::runtime_error("start failure"); });
        CHECK(failed.wait_status == WaitStatus::Completed && failed.exception && failed.io.error);
        auto async_failed = CoIoOperation::Run(receiving, WaitOptions{}, [&receiving](auto operation) {
            auto timer = std::make_shared<boost::asio::steady_timer>(receiving.Context(), 1ms);
            timer->async_wait([timer, operation](auto) {
                operation->CompleteFrom([]() -> IoResult { throw std::runtime_error("async result failure"); });
            });
        });
        CHECK(async_failed.wait_status == WaitStatus::Completed && async_failed.exception && async_failed.io.error);
    });
    Get(immediate);
    CHECK(effects == 1);
    OnOwner(receiving, [&] { boost::system::error_code error; socket->close(error); CHECK(!error); });
    socket.reset();
    CHECK(!client.Close());
    accepting.RequestStop();
    receiving.RequestStop();
    CHECK(accepting.Join() && receiving.Join());
    CHECK(accepting.FailureCount() == 0 && receiving.FailureCount() == 0);
    std::cout << "bridge TCP accept-owner=" << accepting.OwnerId() << " readiness-owner=" << callback_thread
        << " before/after Processer=" << before << '/' << after
        << " wait-progress=1 bytes=4 immediate=100 external/duplicate/expired/invalid/start-exception=ok\n";
}

struct LifetimeData {
    explicit LifetimeData(std::atomic<int>& destroyed) : counter(destroyed) {}
    ~LifetimeData() { ++counter; }
    std::atomic<int>& counter;
    std::string buffer{"late-buffer"};
};
static void Lifecycle() {
    WarmRuntime();
    const auto fds = Count("/proc/self/fd"), threads = Count("/proc/self/task");
    {
        EventLoop loop(2, 1);
        CHECK(loop.Start());
        std::atomic<int> destroyed{0};
        std::weak_ptr<CoIoOperation> old_operation;
        std::weak_ptr<LifetimeData> old_buffer;
        std::atomic_bool late_completed{false};
        std::atomic_bool late_registered{false};
        auto lifetime = Task([&] {
            auto result = CoIoOperation::Run(loop, Until(20ms), [&](auto operation) {
                auto data = std::make_shared<LifetimeData>(destroyed);
                old_operation = operation;
                old_buffer = data;
                auto timer = std::make_shared<boost::asio::steady_timer>(loop.Context(), 120ms);
                timer->async_wait([&, timer, operation, data](auto error) {
                    CHECK(operation->CompleteFrom([&]() -> IoResult {
                        CHECK(!error && data->buffer == "late-buffer");
                        return {1, {}};
                    }));
                    CHECK(!operation->Complete({2, {}}));
                    late_completed = true;
                });
                late_registered.store(true, std::memory_order_release);
            });
            CHECK(result.wait_status == WaitStatus::TimedOut && result.submit_status == SubmitStatus::Accepted);
            CHECK(late_registered.load(std::memory_order_acquire));
            CHECK(!old_operation.expired() && !old_buffer.expired() && destroyed == 0);
            const auto started = std::chrono::steady_clock::now();
            auto fresh = CoIoOperation::Run(loop, Until(1s), [&](auto operation) {
                auto timer = std::make_shared<boost::asio::steady_timer>(loop.Context(), 200ms);
                timer->async_wait([timer, operation](auto error) {
                    operation->CompleteFrom([&]() -> IoResult { CHECK(!error); return {9, {}}; });
                });
            });
            CHECK(fresh.wait_status == WaitStatus::Completed && fresh.io.bytes == 9);
            CHECK(std::chrono::steady_clock::now() - started >= 150ms);
            CHECK(late_completed && old_operation.expired() && old_buffer.expired() && destroyed == 1);
        });
        Get(lifetime);

        std::promise<void> entered, release;
        auto entered_future = entered.get_future();
        auto release_future = release.get_future().share();
        CHECK(loop.Submit([&entered, release_future] {
            entered.set_value();
            CHECK(release_future.wait_for(2s) == std::future_status::ready);
        }) == SubmitStatus::Accepted);
        Get(entered_future);
        CHECK(loop.Submit([] {}) == SubmitStatus::Accepted);
        CHECK(loop.Submit([] {}) == SubmitStatus::Accepted);
        std::atomic<int> effects{0};
        auto full = Task([&] {
            auto result = CoIoOperation::Run(loop, Until(1s), [&effects](auto) { ++effects; });
            CHECK(result.wait_status == WaitStatus::Completed && result.submit_status == SubmitStatus::Full);
            CHECK(result.io.error == std::errc::no_buffer_space);
        });
        Get(full);
        loop.RequestStop();
        auto sealed = Task([&] {
            auto result = CoIoOperation::Run(loop, Until(1s), [&effects](auto) { ++effects; });
            CHECK(result.wait_status == WaitStatus::Completed && result.submit_status == SubmitStatus::Sealed);
        });
        Get(sealed);
        release.set_value();
        CHECK(loop.Join() && loop.FailureCount() == 0 && effects == 0);
    }
    {
        EventLoop loop(64, 2);
        CHECK(loop.Start());
        std::atomic<int> accepted{0}, executed{0}, rejected{0};
        std::atomic_bool go{false};
        std::promise<void> first_accepted;
        auto first_future = first_accepted.get_future();
        std::atomic_bool signalled{false};
        std::vector<std::thread> producers;
        for (int p = 0; p < 4; ++p) {
            producers.emplace_back([&] {
                while (!go.load()) { std::this_thread::yield(); } // 仅 fixture gate。
                for (int i = 0; i < 100; ++i) {
                    const auto status = loop.Submit([&executed] { ++executed; });
                    if (status == SubmitStatus::Accepted) {
                        ++accepted;
                        if (!signalled.exchange(true)) { first_accepted.set_value(); }
                    } else {
                        CHECK(status == SubmitStatus::Full || status == SubmitStatus::Sealed);
                        ++rejected;
                    }
                }
            });
        }
        go = true;
        Get(first_future);
        loop.RequestStop();
        for (auto& producer : producers) { producer.join(); }
        CHECK(loop.Join());
        CHECK(accepted > 0 && accepted == executed && accepted + rejected == 400 && loop.FailureCount() == 0);
        CHECK(loop.Submit([] {}) == SubmitStatus::Sealed);
        std::cout << "concurrent submit/seal accepted=" << accepted << " executed=" << executed
            << " rejected=" << rejected << '\n';
    }
    {
        EventLoop loop(2);
        CHECK(loop.Start());
        std::promise<bool> self_join;
        auto future = self_join.get_future();
        CHECK(loop.Submit([&] { loop.RequestStop(); self_join.set_value(loop.Join()); }) == SubmitStatus::Accepted);
        CHECK(!Get(future));
        CHECK(loop.Join());
        CHECK(loop.Submit([] {}) == SubmitStatus::Sealed);
    }
    std::this_thread::sleep_for(50ms);
    const auto final_fds = Count("/proc/self/fd"), final_threads = Count("/proc/self/task");
    CHECK(final_fds == fds && final_threads == threads);
    std::cout << "lifecycle fd=" << fds << "->" << final_fds << " threads=" << threads << "->" << final_threads
        << " timeout/late-buffer/old-notify-isolation/release/self-stop/control-join=ok; cancellation=not-covered\n";
}

#include "ProcessRuntimeTestMain.hpp"

static int RunReactorCases(int argc, char** argv) {
    try {
        CHECK(argc == 2);
        const std::string test = argv[1];
        if (test == "owner") { Owner(); }
        else if (test == "bridge") { Bridge(); }
        else if (test == "lifecycle") { Lifecycle(); }
        else { throw std::runtime_error("unknown test"); }
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}

int main(int argc, char** argv) {
    // 测试进程退出边界：局部 RAII 收口 → 刷新输出 → _Exit，见
    // ProcessRuntimeTestMain.hpp 头注（静态析构竞态为已知限制，未修复）。
    return bbt_net_test::RunTestProcessExit([&] { return RunReactorCases(argc, argv); });
}
