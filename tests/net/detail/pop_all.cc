// A2 pop-all 移交契约：ReceiveRing::TryPopAll + CoTcpConnection::Read 的
// 零拷贝 backing 移交与故障/交错边界。
//
// 覆盖：
//   wrap              绕环两段以同一 backing 分段移交，FIFO 与逐段指针一致
//   pending           deferred 尾部以自有段移交（FIFO：ring → pending）
//   detached          关闭前 staged 尾部尾随 pending（FIFO：ring → pending → detached）
//   busy_zero         他方持保护时 TryPopAll 返回 Busy 且 ring 零变化
//   quota             移交成功瞬间 Size/额度归零，reserve 立即恢复
//   alloc_fail        准备分配失败：Error 且 ring/pending/额度全部不变
//   tail_before_eof   尾部先交尽，下一次才 Eof（MarkClosed 后残留仍可读）
//   no_pollute        pop 后新 deferred/append 字节不写入已交付段
//   identity          Buffer 段指针即移交存储；Read 交付与 TryPopAll 同 backing
//   gate              非协程域 Read 返回 operation_not_permitted、零消费
//   wait              协程内 Read 挂起-唤醒收到真实数据
//   timeout           Read 超时不消费
//   eof_invalid       尾部先交付再 Eof；无效句柄得 Error
//   multi_reader      两个读者线性化：先取者全拿，后者 WouldBlock
//
// 线程约定：不用 std::this_thread::sleep_for（走 coroutine hook 路径）；等待
// 一律 steady_clock + yield 有界轮询。退出边界同 append_busy_hook.cc。
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
// 分配字节计数探针：TryPopAll 只允许元数据级分配（takeout 对象 + segments
// 数组预留），载荷字节一律走移交——计数 < payload 即零载荷复制证据。
thread_local std::size_t g_alloc_bytes = 0;
thread_local bool g_count_alloc = false;

}  // namespace

void* operator new(std::size_t size) {
    if (g_fail_alloc) {
        throw std::bad_alloc();
    }
    if (g_count_alloc) {
        g_alloc_bytes += size;
    }
    void* memory = std::malloc(size == 0 ? 1 : size);
    if (memory == nullptr) {
        throw std::bad_alloc();
    }
    return memory;
}
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete[](void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void* memory, std::size_t) noexcept { std::free(memory); }

namespace {

using namespace bbt::infra::net;
using namespace std::chrono_literals;

std::shared_ptr<ConnectionState> MakeState(std::size_t inbound = 4096) {
    return std::make_shared<ConnectionState>(1, inbound, 1024, 4, nullptr);
}

std::string JoinTakeout(const ReceiveRing::PopAllTakeout& takeout) {
    std::string out;
    for (const auto& seg : takeout.segments) {
        out.append(seg.data, seg.size);
    }
    return out;
}

std::string JoinBuffer(const Buffer& buffer) {
    std::string out;
    for (const auto& seg : buffer.Segments()) {
        out.append(seg.data, seg.size);
    }
    return out;
}

bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::yield();
    }
    return predicate();
}

void StartRuntime(std::size_t workers) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = workers;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start();
}

std::future<void> Task(std::function<void()> callback) {
    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();
    g_scheduler->RegistCoroutineTask(65536, [promise, callback] {
        try {
            callback();
            promise->set_value();
        } catch (...) {
            promise->set_exception(std::current_exception());
        }
    }, "net.popall");
    return future;
}

bbt::coroutine::WaitOptions Until(std::chrono::milliseconds duration) {
    return {std::chrono::steady_clock::now() + duration};
}

// 确定性构造绕环：capacity 64；写 60 读尽（游标到 60），再写 44 —
// 4 字节落在尾部 60-63，40 字节回绕到头部 0-39，必然两段。
// ReceiveRing 不可移动（mutex+atomic），用 unique_ptr 返回。
std::unique_ptr<ReceiveRing> MakeWrappedRing() {
    auto ring = std::make_unique<ReceiveRing>(64);
    const std::string first(60, 'W');
    const std::string wrapped(44, 'Z');
    BOOST_REQUIRE(ring->TryAppend(first).status == ReadStatus::Ok);
    {
        std::string sink;
        BOOST_REQUIRE(ring->TryRead(sink).status == ReadStatus::Ok);
        BOOST_REQUIRE(sink == first);
    }
    BOOST_REQUIRE(ring->TryAppend(wrapped).status == ReadStatus::Ok);
    return ring;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(net_popall)

BOOST_AUTO_TEST_CASE(wrap_two_segments_one_backing) {
    auto ring = MakeWrappedRing();
    const std::string expected(44, 'Z');
    auto result = ring->TryPopAll();
    BOOST_REQUIRE(result.status == ReadStatus::Ok);
    BOOST_REQUIRE(result.takeout != nullptr);
    BOOST_REQUIRE_EQUAL(result.takeout->segments.size(), 2);
    BOOST_CHECK_EQUAL(JoinTakeout(*result.takeout), expected);
    BOOST_CHECK_EQUAL(ring->Size(), 0);
    // 同一 backing：两段指针来自同一份移交存储（ring 字段持有它）。
    BOOST_REQUIRE(result.takeout->ring != nullptr);
    const auto one = result.takeout->ring->array_one();
    const auto two = result.takeout->ring->array_two();
    BOOST_REQUIRE(one.second != 0 && two.second != 0);
    BOOST_CHECK(result.takeout->segments[0].data == one.first);
    BOOST_CHECK(result.takeout->segments[1].data == two.first);
    // backing 复用：pop 后 ring 立即可用，新数据不碰已交付段。
    BOOST_REQUIRE(ring->TryAppend("after").status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(JoinTakeout(*result.takeout), expected);
    std::string sink;
    BOOST_REQUIRE(ring->TryRead(sink).status == ReadStatus::Ok);
    BOOST_CHECK(sink == "after");
}

BOOST_AUTO_TEST_CASE(pending_moves_as_own_segment) {
    auto state = MakeState(1024);
    BOOST_REQUIRE(state->ring.TryAppend("ring-body").status == ReadStatus::Ok);
    // AppendDeferred 在无竞争时直接落 pending（Busy 时返回 false，由 owner
    // 保留 m_recv_hold 重试）；本例直接走 staging 路径取得 pending 段。
    BOOST_REQUIRE(state->ring.AppendDeferred("pend"));
    BOOST_REQUIRE_EQUAL(state->ring.PendingSize(), 4);
    auto result = state->ring.TryPopAll();
    BOOST_REQUIRE(result.status == ReadStatus::Ok);
    BOOST_REQUIRE_EQUAL(result.takeout->segments.size(), 2);  // ring seg + pending seg
    BOOST_CHECK_EQUAL(JoinTakeout(*result.takeout), "ring-bodypend");
    BOOST_CHECK_EQUAL(result.takeout->pending, "pend");
    BOOST_CHECK_EQUAL(state->ring.Size(), 0);
    BOOST_CHECK_EQUAL(state->ring.PendingSize(), 0);
}

BOOST_AUTO_TEST_CASE(detached_tail_after_pending) {
    auto state = MakeState(1024);
    BOOST_REQUIRE(state->ring.TryAppend("R").status == ReadStatus::Ok);
    BOOST_REQUIRE(state->ring.AppendDeferred("P"));
    BOOST_REQUIRE(state->ring.StageDetached("D"));  // 关闭前 hold 尾部 staging
    auto result = state->ring.TryPopAll();
    BOOST_REQUIRE(result.status == ReadStatus::Ok);
    BOOST_REQUIRE_EQUAL(result.takeout->segments.size(), 3);
    BOOST_CHECK_EQUAL(JoinTakeout(*result.takeout), "RPD");  // ring → pending → detached
    BOOST_CHECK_EQUAL(result.takeout->pending, "P");
    BOOST_CHECK_EQUAL(result.takeout->detached, "D");
    BOOST_CHECK_EQUAL(state->ring.Size(), 0);
}

BOOST_AUTO_TEST_CASE(busy_contention_zero_consume) {
    auto state = MakeState(1024);
    BOOST_REQUIRE(state->ring.TryAppend("xyz").status == ReadStatus::Ok);
    auto hold = ConnectionTestAccess::HoldForTest(state);
    BOOST_REQUIRE(bool(hold));
    const auto result = state->ring.TryPopAll();
    BOOST_CHECK(result.status == ReadStatus::Busy);
    BOOST_CHECK(result.takeout == nullptr);
    BOOST_CHECK_EQUAL(state->ring.Size(), 3);
    ConnectionTestAccess::Reset(hold);
    const auto after = state->ring.TryPopAll();
    BOOST_REQUIRE(after.status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(JoinTakeout(*after.takeout), "xyz");
}

BOOST_AUTO_TEST_CASE(quota_releases_immediately) {
    ReceiveRing ring(64);
    BOOST_REQUIRE(ring.TryAppend(std::string(64, 'q')).status == ReadStatus::Ok);
    // 满额：任何 reserve 都 granted=0 → WouldBlock。
    BOOST_CHECK(ring.TryReserveForAppend(1).status == ReadStatus::WouldBlock);
    auto result = ring.TryPopAll();
    BOOST_REQUIRE(result.status == ReadStatus::Ok);
    // 移交成功瞬间额度释放：接近上限的 reserve 立即成功。
    const auto grant = ring.TryReserveForAppend(60);
    BOOST_CHECK(grant.status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(grant.granted, 60);
    BOOST_CHECK_EQUAL(JoinTakeout(*result.takeout), std::string(64, 'q'));
}

BOOST_AUTO_TEST_CASE(alloc_failure_keeps_ring_and_quota) {
    auto state = MakeState(256);
    BOOST_REQUIRE(state->ring.TryAppend(std::string(64, 'f')).status == ReadStatus::Ok);
    BOOST_REQUIRE(state->ring.AppendDeferred("d"));
    const auto before_size = state->ring.Size();
    const auto before_pending = state->ring.PendingSize();
    const auto gen_before = state->ring.AccessGeneration();

    g_fail_alloc = true;
    const auto result = state->ring.TryPopAll();
    g_fail_alloc = false;

    BOOST_CHECK(result.status == ReadStatus::Error);
    BOOST_CHECK(result.error == std::make_error_code(std::errc::not_enough_memory));
    BOOST_CHECK(result.takeout == nullptr);
    BOOST_CHECK_EQUAL(state->ring.Size(), before_size);
    BOOST_CHECK_EQUAL(state->ring.PendingSize(), before_pending);
    // 额度未释放：可用额仍是 max - 65（64 ring + 1 pending）= 191，不是 256。
    const auto grant = state->ring.TryReserveForAppend(4096);
    BOOST_CHECK(grant.status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(grant.granted, 256 - 65);
    BOOST_CHECK(state->ring.AccessGeneration() > gen_before);  // 保护释放通知不丢
    const auto retry = state->ring.TryPopAll();
    BOOST_REQUIRE(retry.status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(JoinTakeout(*retry.takeout), std::string(64, 'f') + "d");
}

BOOST_AUTO_TEST_CASE(tail_delivered_before_eof) {
    auto state = MakeState(1024);
    BOOST_REQUIRE(state->ring.TryAppend("tail").status == ReadStatus::Ok);
    // 无 owner 的独立 state：RequestClose 直接 MarkClosed → ring.MarkClosed。
    state->RequestClose(std::make_error_code(std::errc::operation_canceled));
    BOOST_REQUIRE(state->ring.ReadClosed());

    const auto first = state->ring.TryPopAll();
    BOOST_REQUIRE(first.status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(JoinTakeout(*first.takeout), "tail");
    const auto second = state->ring.TryPopAll();
    BOOST_CHECK(second.status == ReadStatus::Eof);
    BOOST_CHECK(second.takeout == nullptr);
}

BOOST_AUTO_TEST_CASE(no_pollute_after_pop) {
    auto state = MakeState(1024);
    BOOST_REQUIRE(state->ring.TryAppend("base").status == ReadStatus::Ok);
    const auto popped = state->ring.TryPopAll();
    BOOST_REQUIRE(popped.status == ReadStatus::Ok);
    // pop 后新 deferred/append 字节不得写进已交付存储。
    BOOST_REQUIRE(state->ring.AppendDeferred("NEW"));
    BOOST_REQUIRE(state->ring.TryAppend("MORE").status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(JoinTakeout(*popped.takeout), "base");
}

BOOST_AUTO_TEST_CASE(read_gate_rejects_non_coroutine) {
    auto state = MakeState(1024);
    auto conn = ConnectionTestAccess::Make(state);
    CoTcpConnection ico(conn);
    BOOST_REQUIRE(state->ring.TryAppend("data").status == ReadStatus::Ok);
    const auto outcome = ico.Read(Until(100ms));
    BOOST_CHECK(outcome.state == ReadState::Error);
    BOOST_CHECK(outcome.error == std::make_error_code(std::errc::operation_not_permitted));
    BOOST_CHECK_EQUAL(state->ring.Size(), 4);  // 未消费
}

BOOST_AUTO_TEST_CASE(read_waits_and_delivers_across_wakeup) {
    auto state = MakeState(1024);
    auto conn = ConnectionTestAccess::Make(state);
    CoTcpConnection ico(conn);
    StartRuntime(2);
    std::atomic<int> result{-1};
    auto task = Task([&] {
        ReadOutcome outcome = ico.Read(Until(3000ms));
        if (outcome.state == ReadState::Data && JoinBuffer(outcome.data) == "wakeup") {
            result.store(0);
        } else if (outcome.state == ReadState::Data) {
            result.store(2);
        } else {
            result.store(1);
        }
    });
    // 给协程进入等待的时间窗，再投递数据（TryAppend 唤醒 data waiter）。
    BOOST_REQUIRE(WaitUntil([&] { return task.wait_for(0ms) == std::future_status::timeout; }, 200ms));
    std::this_thread::yield();
    BOOST_REQUIRE(state->ring.TryAppend("wakeup").status == ReadStatus::Ok);
    BOOST_REQUIRE(task.wait_for(5s) == std::future_status::ready);
    task.get();
    BOOST_CHECK_EQUAL(result.load(), 0);
}

BOOST_AUTO_TEST_CASE(read_timeout_does_not_consume) {
    auto state = MakeState(1024);
    auto conn = ConnectionTestAccess::Make(state);
    CoTcpConnection ico(conn);
    StartRuntime(1);
    std::atomic<int> result{-1};
    auto task = Task([&] {
        ReadOutcome outcome = ico.Read(Until(30ms));
        if (outcome.state == ReadState::Error
            && outcome.error == std::make_error_code(std::errc::timed_out)
            && outcome.data.Empty()) {
            result.store(0);
        }
    });
    BOOST_REQUIRE(task.wait_for(5s) == std::future_status::ready);
    task.get();
    BOOST_CHECK_EQUAL(result.load(), 0);
}

BOOST_AUTO_TEST_CASE(read_buffer_identity_and_eof_drain) {
    // Read 交付 Buffer 的段指针就是移交存储；尾部先交付再 Eof。
    auto state = MakeState(1024);
    auto conn = ConnectionTestAccess::Make(state);
    CoTcpConnection ico(conn);
    BOOST_REQUIRE(state->ring.TryAppend("rest").status == ReadStatus::Ok);
    state->RequestClose(std::make_error_code(std::errc::operation_canceled));
    StartRuntime(1);
    std::atomic<int> result{-1};
    auto task = Task([&] {
        ReadOutcome first = ico.Read(Until(1000ms));
        if (first.state != ReadState::Data) { result.store(1); return; }
        // backing 非空且段指针可用——BufferTestAccess 核 backing 身份。
        if (BufferTestAccess::Backing(first.data) == nullptr) { result.store(3); return; }
        if (JoinBuffer(first.data) != "rest") { result.store(2); return; }
        ReadOutcome second = ico.Read(Until(1000ms));
        if (second.state == ReadState::Eof && second.data.Empty()) {
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
    BOOST_CHECK(bad.Read(Until(1ms)).state == ReadState::Error);
}

BOOST_AUTO_TEST_CASE(multi_reader_linearizes) {
    auto state = MakeState(1024);
    BOOST_REQUIRE(state->ring.TryAppend("only-once").status == ReadStatus::Ok);
    const auto first = state->ring.TryPopAll();
    BOOST_REQUIRE(first.status == ReadStatus::Ok);
    const auto second = state->ring.TryPopAll();          // 线性化点之后：无数据
    BOOST_CHECK(second.status == ReadStatus::WouldBlock);
    BOOST_CHECK(second.takeout == nullptr);
    std::string sink;
    BOOST_CHECK(state->ring.TryRead(sink).status == ReadStatus::WouldBlock);
    BOOST_CHECK(sink.empty());                           // raw 路径同样读不到
}

// 零载荷复制探针：payload 足够大（8K + 1K pending + 1K detached）使
// 元数据分配（takeout 对象 ≈200B + segments 预留 + string move  bookkeeping）
// 必然 ≪ payload；若有任何路径按载荷规模复制，计数必 ≥ payload 并被证伪。
// 断言：g_alloc_bytes ≪ payload（< payload/8），同时段内容与 FIFO 完整。
BOOST_AUTO_TEST_CASE(zero_payload_copy_probe) {
    auto state = MakeState(16 * 1024);
    BOOST_REQUIRE(state->ring.TryAppend(std::string(8192, 'p')).status == ReadStatus::Ok);
    BOOST_REQUIRE(state->ring.AppendDeferred(std::string(1024, 'q')));
    BOOST_REQUIRE(state->ring.StageDetached(std::string(1024, 'r')));
    const std::size_t payload = 8192 + 1024 + 1024;

    g_alloc_bytes = 0;
    g_count_alloc = true;
    const auto result = state->ring.TryPopAll();
    g_count_alloc = false;

    BOOST_REQUIRE(result.status == ReadStatus::Ok);
    BOOST_TEST_MESSAGE("popall alloc_bytes=" << g_alloc_bytes << " payload=" << payload);
    BOOST_CHECK_LT(g_alloc_bytes, payload / 8);  // 元数据 ≪ 载荷：无载荷级复制
    BOOST_CHECK_EQUAL(JoinTakeout(*result.takeout),
        std::string(8192, 'p') + std::string(1024, 'q') + std::string(1024, 'r'));
    BOOST_REQUIRE_EQUAL(result.takeout->segments.size(), 3);
    // pending/detached 的 string 移动保留原分配：段指针与字段同址。
    BOOST_CHECK(result.takeout->segments[1].data == result.takeout->pending.data());
    BOOST_CHECK(result.takeout->segments[2].data == result.takeout->detached.data());
}

BOOST_AUTO_TEST_SUITE_END()

bool init_unit_test() { return true; }

int main(int argc, char* argv[]) {
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
