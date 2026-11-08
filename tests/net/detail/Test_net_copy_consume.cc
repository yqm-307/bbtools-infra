// 复制读取契约：跨段 FIFO、尾部、失败零消费、容量释放与输出独立寿命。
#define BOOST_TEST_NO_MAIN
#define BOOST_TEST_ALTERNATIVE_INIT_API
#include <boost/test/included/unit_test.hpp>

#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/Error.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/net/detail/CoInternal.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>

#include <bbt/coroutine/coroutine.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <new>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace {

// ---- 分配失败注入（本可执行文件私有；thread_local 不污染 runtime 线程）----
thread_local bool g_fail_alloc = false;
} // namespace

void* operator new(std::size_t size)
{
    if (g_fail_alloc) {
        throw std::bad_alloc();
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

std::shared_ptr<ConnectionState> MakeState(std::size_t inbound = 4096)
{
    return std::make_shared<ConnectionState>(1, inbound, 1024, 4, nullptr);
}

std::string JoinBuffer(const Buffer& buffer)
{
    std::string out;
    for (const auto& seg : buffer.Segments()) {
        out.append(seg.data, seg.size);
    }
    return out;
}

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
        "net.copy_consume");
    return future;
}

bbt::coroutine::WaitOptions Until(std::chrono::milliseconds duration)
{
    return {std::chrono::steady_clock::now() + duration};
}

// 连续追加填满每个增长块，构造超过旧段表固定容量的分段输入。
// ReceiveRing 不可移动（mutex+atomic），用 unique_ptr 返回。
std::unique_ptr<ReceiveRing> MakeSegmentedRing(std::string& expected)
{
    auto ring = std::make_unique<ReceiveRing>(64 * 1024);
    for (int i = 0; i < 5; ++i) {
        const std::string chunk((std::max<std::size_t>)(512, ring->Size() * 2), static_cast<char>('A' + i));
        BOOST_REQUIRE(ring->TryAppend(chunk).status == ReadStatus::Ok);
        expected += chunk;
    }
    return ring;
}

} // namespace

BOOST_AUTO_TEST_SUITE(net_copy_consume)

BOOST_AUTO_TEST_CASE(multiple_segments_fifo)
{
    std::string expected;
    auto ring = MakeSegmentedRing(expected);
    Buffer out;
    BOOST_REQUIRE(out.WriteBytes("prefix"));
    const auto result = ring->TryCopyConsume(out);
    BOOST_REQUIRE(result.status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(result.bytes, expected.size());
    BOOST_CHECK_EQUAL(JoinBuffer(out), "prefix" + expected);
    BOOST_CHECK_EQUAL(ring->Size(), 0);
    BOOST_REQUIRE(ring->TryAppend("after").status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(JoinBuffer(out), "prefix" + expected);
    Buffer next;
    BOOST_REQUIRE(ring->TryCopyConsume(next).status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(JoinBuffer(next), "after");
}

BOOST_AUTO_TEST_CASE(pending_fifo)
{
    auto state = MakeState(1024);
    BOOST_REQUIRE(state->ring.TryAppend("ring-body").status == ReadStatus::Ok);
    BOOST_REQUIRE(state->ring.AppendDeferred("pend"));
    BOOST_REQUIRE_EQUAL(state->ring.PendingSize(), 4);
    Buffer out;
    BOOST_REQUIRE(state->ring.TryCopyConsume(out).status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(JoinBuffer(out), "ring-bodypend");
    BOOST_CHECK_EQUAL(state->ring.Size(), 0);
    BOOST_CHECK_EQUAL(state->ring.PendingSize(), 0);
}

BOOST_AUTO_TEST_CASE(detached_tail_after_pending)
{
    auto state = MakeState(1024);
    BOOST_REQUIRE(state->ring.TryAppend("R").status == ReadStatus::Ok);
    BOOST_REQUIRE(state->ring.AppendDeferred("P"));
    BOOST_REQUIRE(state->ring.StageDetached("D"));
    state->ring.MarkClosed();
    Buffer out;
    BOOST_REQUIRE(state->ring.TryCopyConsume(out).status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(JoinBuffer(out), "RPD");
    BOOST_CHECK_EQUAL(state->ring.Size(), 0);
    BOOST_CHECK(state->ring.TryCopyConsume(out).status == ReadStatus::Eof);
    BOOST_CHECK_EQUAL(JoinBuffer(out), "RPD");
}

BOOST_AUTO_TEST_CASE(busy_contention_zero_consume)
{
    auto state = MakeState(1024);
    BOOST_REQUIRE(state->ring.TryAppend("xyz").status == ReadStatus::Ok);
    std::size_t wakes = 0;
    state->ring.SetOwnerWake([&] { ++wakes; });
    const auto generation = state->ring.AccessGeneration();
    Buffer out;
    BOOST_REQUIRE(out.WriteBytes("keep"));
    auto hold = ConnectionTestAccess::HoldForTest(state);
    BOOST_REQUIRE(bool(hold));
    const auto result = state->ring.TryCopyConsume(out);
    BOOST_CHECK(result.status == ReadStatus::Busy);
    BOOST_CHECK_EQUAL(result.bytes, 0);
    BOOST_CHECK_EQUAL(JoinBuffer(out), "keep");
    BOOST_CHECK_EQUAL(state->ring.Size(), 3);
    ConnectionTestAccess::Reset(state, hold);
    BOOST_CHECK_EQUAL(state->ring.AccessGeneration(), generation + 1);
    BOOST_CHECK_EQUAL(wakes, 1);
    ConnectionTestAccess::Reset(state, hold);
    BOOST_CHECK_EQUAL(state->ring.AccessGeneration(), generation + 1);
    BOOST_CHECK_EQUAL(wakes, 1);
    BOOST_REQUIRE(state->ring.TryCopyConsume(out).status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(JoinBuffer(out), "keepxyz");
}

BOOST_AUTO_TEST_CASE(quota_releases_immediately)
{
    ReceiveRing ring(64);
    BOOST_REQUIRE(ring.TryAppend(std::string(64, 'q')).status == ReadStatus::Ok);
    BOOST_CHECK(ring.TryReserveForAppend(1).status == ReadStatus::WouldBlock);
    Buffer out;
    BOOST_REQUIRE(ring.TryCopyConsume(out).status == ReadStatus::Ok);
    // 用户仍持有输出时，库内额度已经释放。
    const auto grant = ring.TryReserveForAppend(60);
    BOOST_CHECK(grant.status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(grant.granted, 60);
    BOOST_CHECK_EQUAL(JoinBuffer(out), std::string(64, 'q'));
}

BOOST_AUTO_TEST_CASE(alloc_failure_keeps_ring_and_quota)
{
    auto state = MakeState(256);
    BOOST_REQUIRE(state->ring.TryAppend(std::string(64, 'f')).status == ReadStatus::Ok);
    BOOST_REQUIRE(state->ring.AppendDeferred("d"));
    const auto before_size = state->ring.Size();
    const auto gen_before = state->ring.AccessGeneration();
    Buffer out;
    BOOST_REQUIRE(out.WriteBytes("keep"));
    g_fail_alloc = true;
    const auto result = state->ring.TryCopyConsume(out);
    g_fail_alloc = false;
    BOOST_CHECK(result.status == ReadStatus::Error);
    BOOST_CHECK(result.error == std::make_error_code(std::errc::not_enough_memory));
    BOOST_CHECK_EQUAL(result.bytes, 0);
    BOOST_CHECK_EQUAL(JoinBuffer(out), "keep");
    BOOST_CHECK_EQUAL(state->ring.Size(), before_size);
    // pending 可以合入 ring，但逻辑字节和额度不能被消费。
    const auto grant = state->ring.TryReserveForAppend(4096);
    BOOST_CHECK(grant.status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(grant.granted, 256 - 65);
    BOOST_CHECK(state->ring.AccessGeneration() > gen_before);
    BOOST_REQUIRE(state->ring.TryCopyConsume(out).status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(JoinBuffer(out), "keep" + std::string(64, 'f') + "d");
}

BOOST_AUTO_TEST_CASE(tail_delivered_before_eof)
{
    auto state = MakeState(1024);
    BOOST_REQUIRE(state->ring.TryAppend("tail").status == ReadStatus::Ok);
    state->RequestClose(std::make_error_code(std::errc::operation_canceled));
    BOOST_REQUIRE(state->ring.ReadClosed());
    Buffer out;
    BOOST_REQUIRE(state->ring.TryCopyConsume(out).status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(JoinBuffer(out), "tail");
    BOOST_CHECK(state->ring.TryCopyConsume(out).status == ReadStatus::Eof);
    BOOST_CHECK_EQUAL(JoinBuffer(out), "tail");
}

BOOST_AUTO_TEST_CASE(output_survives_ring_reuse_and_destruction)
{
    auto state = MakeState(1024);
    BOOST_REQUIRE(state->ring.TryAppend("base").status == ReadStatus::Ok);
    Buffer out;
    BOOST_REQUIRE(state->ring.TryCopyConsume(out).status == ReadStatus::Ok);
    BOOST_REQUIRE(state->ring.AppendDeferred("NEW"));
    BOOST_REQUIRE(state->ring.TryAppend("MORE").status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(JoinBuffer(out), "base");
    state.reset();
    BOOST_CHECK_EQUAL(JoinBuffer(out), "base");
}

BOOST_AUTO_TEST_CASE(read_gate_rejects_non_coroutine)
{
    auto state = MakeState(1024);
    auto conn = ConnectionTestAccess::Make(state);
    CoTcpConnection ico(conn);
    BOOST_REQUIRE(state->ring.TryAppend("data").status == ReadStatus::Ok);
    auto [outcome_data, outcome_error] = ico.Read(Until(100ms));
    BOOST_CHECK(bool(outcome_error));
    BOOST_CHECK(outcome_error == std::make_error_code(std::errc::operation_not_permitted));
    BOOST_CHECK_EQUAL(state->ring.Size(), 4); // 未消费
}

BOOST_AUTO_TEST_CASE(read_waits_and_delivers_across_wakeup)
{
    auto state = MakeState(1024);
    auto conn = ConnectionTestAccess::Make(state);
    CoTcpConnection ico(conn);
    StartRuntime(2);
    std::atomic<bool> registered{false};
    ReceiveRing::SetAfterAddHookForTest([&] { registered.store(true); });
    struct HookGuard {
        ~HookGuard()
        {
            ReceiveRing::SetAfterAddHookForTest({});
        }
    } hook_guard;
    std::atomic<int> result{-1};
    auto task = Task([&] {
        auto [outcome_data, outcome_error] = ico.Read(Until(3000ms));
        if (!outcome_error && JoinBuffer(outcome_data) == "wakeup") {
            result.store(0);
        } else if (!outcome_error) {
            result.store(2);
        } else {
            result.store(1);
        }
    });
    BOOST_REQUIRE(WaitUntil([&] { return registered.load(); }, 2000ms));
    BOOST_REQUIRE(state->ring.TryAppend("wakeup").status == ReadStatus::Ok);
    BOOST_REQUIRE(task.wait_for(5s) == std::future_status::ready);
    task.get();
    BOOST_CHECK_EQUAL(result.load(), 0);
}

BOOST_AUTO_TEST_CASE(read_timeout_does_not_consume)
{
    auto state = MakeState(1024);
    auto conn = ConnectionTestAccess::Make(state);
    CoTcpConnection ico(conn);
    StartRuntime(1);
    std::atomic<int> result{-1};
    auto task = Task([&] {
        auto [outcome_data, outcome_error] = ico.Read(Until(30ms));
        if (bool(outcome_error) && outcome_error == std::make_error_code(std::errc::timed_out)
            && outcome_data.Empty()) {
            result.store(0);
        }
    });
    BOOST_REQUIRE(task.wait_for(5s) == std::future_status::ready);
    task.get();
    BOOST_CHECK_EQUAL(result.load(), 0);
}

BOOST_AUTO_TEST_CASE(read_buffer_and_eof_drain)
{
    // Read 复制交付尾部，下一次才 Eof。
    auto state = MakeState(1024);
    auto conn = ConnectionTestAccess::Make(state);
    CoTcpConnection ico(conn);
    BOOST_REQUIRE(state->ring.TryAppend("rest").status == ReadStatus::Ok);
    state->RequestClose(std::make_error_code(std::errc::operation_canceled));
    StartRuntime(1);
    std::atomic<int> result{-1};
    auto task = Task([&] {
        auto [first_data, first_error] = ico.Read(Until(1000ms));
        if (bool(first_error)) {
            result.store(1);
            return;
        }
        if (JoinBuffer(first_data) != "rest") {
            result.store(2);
            return;
        }
        auto [second_data, second_error] = ico.Read(Until(1000ms));
        if (second_error == bbt::infra::net::make_error_code(bbt::infra::net::NetErrc::Eof) && second_data.Empty()) {
            result.store(0);
        } else {
            result.store(4);
        }
    });
    BOOST_REQUIRE(task.wait_for(5s) == std::future_status::ready);
    task.get();
    BOOST_CHECK_EQUAL(result.load(), 0);
    // 无效句柄。
    CoTcpConnection bad;
    BOOST_CHECK(bool(std::get<1>(bad.Read(Until(1ms)))));
}

BOOST_AUTO_TEST_CASE(readers_consume_only_once)
{
    auto state = MakeState(1024);
    BOOST_REQUIRE(state->ring.TryAppend("only-once").status == ReadStatus::Ok);
    Buffer first;
    BOOST_REQUIRE(state->ring.TryCopyConsume(first).status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(JoinBuffer(first), "only-once");
    Buffer second;
    BOOST_REQUIRE(second.WriteBytes("keep"));
    BOOST_CHECK(state->ring.TryCopyConsume(second).status == ReadStatus::WouldBlock);
    BOOST_CHECK_EQUAL(JoinBuffer(second), "keep");
    std::string sink;
    BOOST_CHECK(state->ring.TryRead(sink).status == ReadStatus::WouldBlock);
    BOOST_CHECK(sink.empty());
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
