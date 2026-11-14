/**
 * 分配失败契约：复制读取不丢字节，追加失败不提交，预留容量可复用。
 */
#define BOOST_TEST_NO_MAIN
#define BOOST_TEST_ALTERNATIVE_INIT_API
#include <boost/test/included/unit_test.hpp>

#include <bbt/infra/net/Buffer.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/ConnectionResult.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/Error.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/RawHandler.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>

#include <bbt/coroutine/coroutine.hpp>

#include <poll.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <new>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace {

/**
 * ---- 分配失败/计数注入（本可执行文件私有；thread_local 不污染 runtime 线程）----
 * fail_at=0 关闭；>0 时第 fail_at 次 operator new 抛 bad_alloc；calls 记录序号。
 */
thread_local int g_fail_at = 0;
thread_local int g_calls = 0;
thread_local std::size_t g_alloc_sizes[16]{};

void Arm(int index)
{
    g_calls = 0;
    for (auto& size : g_alloc_sizes) {
        size = 0;
    }
    g_fail_at = index;
}

void Disarm()
{
    g_fail_at = 0;
}

std::string AllocTrace()
{
    std::string out = "calls=" + std::to_string(g_calls) + " sizes=";
    for (int i = 0; i < g_calls && i < 16; ++i) {
        out += std::to_string(g_alloc_sizes[i]);
        out += ',';
    }
    return out;
}

// clang-format off
}
// clang-format on

void* operator new(std::size_t size)
{
    if (g_fail_at != 0) {
        const int index = ++g_calls;
        if (index <= 16) {
            g_alloc_sizes[index - 1] = size;
        }
        if (index == g_fail_at) {
            throw std::bad_alloc();
        }
    }
    void* memory = std::malloc(size == 0 ? 1 : size);
    if (memory == nullptr) {
        throw std::bad_alloc();
    }
    return memory;
}

void* operator new[](std::size_t size)
{
    return ::operator new(size);
}

void operator delete(void* memory) noexcept
{
    std::free(memory);
}

void operator delete[](void* memory) noexcept
{
    std::free(memory);
}

void operator delete(void* memory, std::size_t) noexcept
{
    std::free(memory);
}

void operator delete[](void* memory, std::size_t) noexcept
{
    std::free(memory);
}

namespace {

using namespace bbt::infra::net;
using namespace std::chrono_literals;

bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout)
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

void StartRuntime(std::size_t workers)
{
    g_bbt_coroutine_config->m_cfg_static_thread_num = workers;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start();
}

std::future<void> Task(std::function<void()> callback)
{
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    g_scheduler->RegistCoroutineTask(
        65536,
        [promise, callback] {
            try {
                callback();
                promise->set_value();
            } catch (...) {
                promise->set_exception(std::current_exception());
            }
        },
        "net.buffer_oom");
    return future;
}

bbt::coroutine::WaitOptions Until(std::chrono::milliseconds duration)
{
    return {std::chrono::steady_clock::now() + duration};
}

// clang-format off
}
// clang-format on

BOOST_AUTO_TEST_SUITE(net_buffer_oom)

/**
 * 逐序号注入覆盖快照与输出分配；失败保留已有输出、全部接收字节和重试能力。
 */
BOOST_AUTO_TEST_CASE(read_copy_oom_no_loss)
{
    StartRuntime(1);
    bool saw_error = false;
    bool saw_data = false;
    const std::string prefix(512, 'P');
    const std::string payload(64, 'X');
    for (int index = 1; index <= 6; ++index) {
        auto state = std::make_shared<ConnectionState>(1, 256, 1024, 4, nullptr);
        BOOST_REQUIRE(state->ring.TryAppend(payload).status == ReadStatus::Ok);
        state->ring.MarkEof();
        CoTcpConnection conn(ConnectionTestAccess::Make(state));
        std::error_code first_error;
        std::error_code retry_error;
        std::string first_data;
        std::string retry_data;
        std::size_t used_after = 0;
        bool threw = false;
        auto task = Task([&, index] {
            Buffer out;
            out.WriteBytes(prefix);
            Arm(index);
            try {
                first_error = conn.Read(out, Until(2000ms));
            } catch (const std::bad_alloc&) {
                threw = true;
            }
            Disarm();
            first_data = out.CopyTo();
            used_after = state->ring.Size();
            auto retry = conn.Read(Until(2000ms));
            retry_error = std::get<1>(retry);
            retry_data = std::get<0>(retry).CopyTo();
        });
        BOOST_REQUIRE(task.wait_for(10s) == std::future_status::ready);
        task.get();
        BOOST_TEST_MESSAGE(
            "read_fail_at=" << index << " threw=" << threw << " error=" << first_error.value()
                            << " used_after=" << used_after);
        BOOST_CHECK(!threw);
        if (!first_error) {
            saw_data = true;
            BOOST_CHECK_EQUAL(first_data, prefix + payload);
            BOOST_CHECK_EQUAL(used_after, 0);
            BOOST_CHECK(retry_error == make_error_code(NetErrc::Eof));
            BOOST_CHECK(retry_data.empty());
        } else {
            saw_error = true;
            BOOST_CHECK(first_error == std::make_error_code(std::errc::not_enough_memory));
            BOOST_CHECK_EQUAL(first_data, prefix);
            BOOST_CHECK_EQUAL(used_after, payload.size());
            BOOST_CHECK(!retry_error);
            BOOST_CHECK_EQUAL(retry_data, payload);
        }
    }
    BOOST_CHECK(saw_data);
    BOOST_CHECK(saw_error);
}

/**
 * 复制消费不再移走 ring backing；预留仍可复用，但真正扩容的 OOM 仍须返回错误。
 */
BOOST_AUTO_TEST_CASE(grant_copy_append_oom_no_throw)
{
    ReceiveRing ring(4096);
    BOOST_REQUIRE(ring.TryAppend("old").status == ReadStatus::Ok);
    BOOST_REQUIRE(ring.TryReserveForAppend(64).status == ReadStatus::Ok);
    Buffer out;
    BOOST_REQUIRE(ring.TryCopyConsume(out).status == ReadStatus::Ok);
    const std::string payload(2048, 'R');
    Arm(1);
    const auto appended = ring.TryAppend(payload);
    Disarm();
    BOOST_TEST_MESSAGE("append OOM " << AllocTrace());
    BOOST_CHECK(appended.status == ReadStatus::WouldBlock);
    BOOST_CHECK(appended.error == std::make_error_code(std::errc::not_enough_memory));
    BOOST_CHECK_EQUAL(appended.bytes, 0);
    BOOST_CHECK_EQUAL(ring.Size(), 0);
    BOOST_CHECK_EQUAL(out.CopyTo(), "old");
    BOOST_REQUIRE(ring.TryAppend(payload).status == ReadStatus::Ok);
    Buffer retry;
    BOOST_REQUIRE(ring.TryCopyConsume(retry).status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(retry.CopyTo(), payload);
}

/**
 * 不强迫短串转堆；真正追加扩容失败时，原 pending/detached 数据和计数不变。
 */
BOOST_AUTO_TEST_CASE(staging_append_failure_keeps_bytes)
{
    const std::string payload(2048, 'Q');
    for (bool detached : {false, true}) {
        ReceiveRing ring(4096);
        const auto append = [&](std::string_view bytes) {
            return detached ? ring.StageDetached(bytes) : ring.AppendDeferred(bytes);
        };
        BOOST_REQUIRE(append("old"));
        const auto pending_before = ring.PendingSize();
        Arm(1);
        const bool accepted = append(payload);
        Disarm();
        BOOST_CHECK(!accepted);
        BOOST_CHECK_EQUAL(ring.Size(), 3);
        BOOST_CHECK_EQUAL(ring.PendingSize(), pending_before);
        BOOST_REQUIRE(append(payload));
        Buffer out;
        BOOST_REQUIRE(ring.TryCopyConsume(out).status == ReadStatus::Ok);
        BOOST_CHECK_EQUAL(out.CopyTo(), "old" + payload);
        BOOST_CHECK_EQUAL(ring.Size(), 0);
    }
}

BOOST_AUTO_TEST_CASE(staging_reserved_capacity_reused)
{
    const std::string payload(64, 'S');
    for (bool detached : {false, true}) {
        ReceiveRing ring(256);
        if (detached) {
            ring.ReserveDetached(payload.size());
        } else {
            ring.ReserveDeferred(payload.size());
        }
        for (int round = 0; round < 2; ++round) {
            Arm(1);
            const bool accepted = detached ? ring.StageDetached(payload) : ring.AppendDeferred(payload);
            Disarm();
            BOOST_REQUIRE(accepted);
            BOOST_CHECK_EQUAL(g_calls, 0);
            Buffer out;
            BOOST_REQUIRE(ring.TryCopyConsume(out).status == ReadStatus::Ok);
            BOOST_CHECK_EQUAL(out.CopyTo(), payload);
        }
    }
    ReceiveRing ring(256);
    BOOST_REQUIRE(ring.TryReserveForAppend(64).status == ReadStatus::Ok);
    for (int round = 0; round < 2; ++round) {
        Arm(1);
        const auto appended = ring.TryAppend(payload);
        Disarm();
        BOOST_REQUIRE(appended.status == ReadStatus::Ok);
        BOOST_CHECK_EQUAL(g_calls, 0);
        Buffer out;
        BOOST_REQUIRE(ring.TryCopyConsume(out).status == ReadStatus::Ok);
        BOOST_CHECK_EQUAL(out.CopyTo(), payload);
    }
}

/**
 * ---- （修复后契约）：Buffer 拷贝已删除，OOM 悬空段形态不可达 ----
 */
BOOST_AUTO_TEST_CASE(copy_assign_deleted)
{
    static_assert(!std::is_copy_constructible_v<Buffer>, "修复后契约：Buffer 不可拷贝构造");
    static_assert(!std::is_copy_assignable_v<Buffer>, "修复后契约：Buffer 不可拷贝赋值");
    static_assert(std::is_move_constructible_v<Buffer>, "修复后契约：Buffer 可移动构造");
    static_assert(std::is_move_assignable_v<Buffer>, "修复后契约：Buffer 可移动赋值");

    /**
     * 移动语义仍可用：move 后来源为空、目标持有全部段。
     */
    ReceiveRing one(256);
    BOOST_REQUIRE(one.TryAppend(std::string(64, 'A')).status == ReadStatus::Ok);
    Buffer dest;
    BOOST_REQUIRE(one.TryCopyConsume(dest).status == ReadStatus::Ok);
    Buffer moved = std::move(dest);
    BOOST_CHECK_EQUAL(moved.Size(), 64);
    BOOST_CHECK(dest.Segments().empty());

    /**
     * move-assign：旧 backing 移交，不留旧段悬空形态。
     */
    Buffer assigned;
    assigned = std::move(moved);
    BOOST_CHECK_EQUAL(assigned.Size(), 64);
    BOOST_CHECK_EQUAL(assigned.CopyTo().front(), 'A');
    BOOST_TEST_MESSAGE("d4 copy_deleted move_ok=1");
}

/**
 * ---- （修复后契约）：backing+segments 构造已收回 private ----
 */
BOOST_AUTO_TEST_CASE(public_ctor_forge_denied)
{
    static_assert(
        !std::is_constructible_v<Buffer, std::shared_ptr<void>, std::vector<BufferSegment>>,
        "修复后契约：公共构造已收回，owning 不变量不可伪造");

    /**
     * 内部接缝仍可构造任意分段输入；不代表 Read 的复制路径。
     */
    char backing_byte = 'B';
    auto backing = std::make_shared<char>(backing_byte);
    const char* seg_ptr = backing.get();
    Buffer made =
        BufferTestAccess::Make(std::static_pointer_cast<void>(backing), std::vector<BufferSegment>{{seg_ptr, 1}});
    BOOST_CHECK(BufferTestAccess::Backing(made) == backing.get());
    BOOST_CHECK_EQUAL(made.Size(), 1);
    BOOST_CHECK_EQUAL(made.CopyTo(), "B");
    BOOST_TEST_MESSAGE("d5 forge_denied=1 test_access_make_ok=1");
}

/**
 * ---- （修复后契约）：CoAccept reclaim 与连接同一事实源 ----
 */
BOOST_AUTO_TEST_CASE(coaccept_reclaim_valid_ticket)
{
    struct NullHandler : RawHandler {};

    auto handler = std::make_shared<NullHandler>();
    Engine engine(handler);
    BOOST_REQUIRE(!engine.EnableAcceptHandoff(4));
    EngineConfig config;
    std::error_code addr_ec;
    const Address loopback = Address::Ip("127.0.0.1", 0, addr_ec);
    BOOST_REQUIRE(!addr_ec);
    config.listen_addresses = {loopback};
    config.runtime_progress = RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    StartRuntime(2);
    std::error_code run_error;
    std::thread runner([&] { run_error = engine.Run(config); });

    struct RunnerGuard {
        Engine& engine;
        std::thread& thread;

        ~RunnerGuard()
        {
            (void)engine.Close();
            if (thread.joinable()) {
                thread.join();
            }
        }
    } guard{engine, runner};

    BOOST_REQUIRE(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    BOOST_REQUIRE(!engine.LocalAddresses(addresses));
    BOOST_REQUIRE(!addresses.empty());

    std::atomic<int> accept_error{-1};
    std::atomic<int> reclaim_valid{-1};
    std::atomic<int> reclaim_released{-1};
    std::atomic<int> conn_released{-1};
    std::atomic<int> conn_valid{-1};
    auto task = Task([&] {
        auto res = engine.CoAccept(Until(3000ms));
        if (res.error) {
            accept_error.store(res.error.value());
            return;
        }
        accept_error.store(0);
        conn_valid.store(res.connection.Valid() ? 1 : 0);
        reclaim_valid.store(res.reclaim.Valid() ? 1 : 0);
        reclaim_released.store(res.reclaim.Released() ? 1 : 0);
        Connection raw = res.connection.Raw();
        conn_released.store(raw.Released() ? 1 : 0);
        (void)res.connection.Close();
    });
    std::error_code ec;
    NativeSocket client = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, ec);
    BOOST_REQUIRE(!ec);
    const auto conn_err = client.Connect(addresses[0]);
    BOOST_CHECK(!conn_err || conn_err.value() == EINPROGRESS || conn_err.value() == EWOULDBLOCK);
    pollfd pfd{client.Fd(), POLLOUT, 0};
    BOOST_REQUIRE(::poll(&pfd, 1, 3000) > 0);
    BOOST_REQUIRE(!client.PendingError());
    BOOST_REQUIRE(task.wait_for(10s) == std::future_status::ready);
    task.get();
    BOOST_TEST_MESSAGE(
        "d6 accept_error=" << accept_error.load() << " conn_valid=" << conn_valid.load() << " reclaim_valid="
                           << reclaim_valid.load() << " reclaim_released=" << reclaim_released.load()
                           << " conn_released=" << conn_released.load());
    BOOST_REQUIRE_EQUAL(accept_error.load(), 0);

    /**
     * 修复后契约：reclaim 有效且与连接同一事实源——fd 存活时 ticket 报未释放。
     */
    BOOST_CHECK_EQUAL(conn_valid.load(), 1);
    BOOST_CHECK_EQUAL(conn_released.load(), 0);

    /**
     * 有效票
     */
    BOOST_CHECK_EQUAL(reclaim_valid.load(), 1);

    /**
     * 与 conn 同一事实：未释放
     */
    BOOST_CHECK_EQUAL(reclaim_released.load(), 0);
    (void)client.Close();
    (void)engine.Close();
    runner.join();
    BOOST_CHECK(!run_error);
}

BOOST_AUTO_TEST_SUITE_END()

bool init_unit_test()
{
    return true;
}

int main(int argc, char* argv[])
{
    const int code = boost::unit_test::unit_test_main(&init_unit_test, argc, argv);
    std::cout.flush();
    std::cerr.flush();
    int final_code = code;
    if (std::fflush(nullptr) != 0 || std::ferror(stdout) != 0 || std::ferror(stderr) != 0) {
        if (final_code == 0) {
            final_code = 1;
        }
    }
    std::_Exit(final_code);
}
