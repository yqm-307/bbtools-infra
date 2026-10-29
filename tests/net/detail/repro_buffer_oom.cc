// infra-ico-conn 独立审查 6 个 Buffer/ring/Engine confirmed defect 的修复后
// 契约断言（原「缺陷存在」断言已在修复后翻转）。
//
// 每个 case 对应审查报告（final-review-report.md）一条缺陷的修复方向，
// 断言以「修复后契约成立」为真：
//   d1_read_post_pop_oom_no_loss         P1-1 段元数据在 pop 事务内完成，
//        Read 不再做 post-pop 分配；注入任何序号都不会丢已提交字节。
//   d2_grant_pop_append_oom_no_throw    P1-2 TryAppend 的 OOM 折成
//        Error+not_enough_memory 返回而非抛出——SubmitReceivedOwner 的
//        m_recv_hold 登记分支可达，owner recv 字节不丢。
//   d3_sso_payload_not_copied           P2-1 pending/detached 保持可整体
//        转移的堆 backing（SSO 内联缓冲在移交前已转出），段指针不落在
//        目标 string 对象内联区间——移交不复制载荷。
//   d4_copy_assign_deleted              P2-2a Buffer 拷贝构造/赋值已删除，
//        OOM 悬空段形态不可达。
//   d5_public_ctor_forge_denied         P2-2b backing+segments 构造已收回
//        private——外部不可构造伪造 owning 不变量的 Buffer。
//   d6_coaccept_reclaim_valid_ticket    P1-5 CoAccept 交付的 reclaim 与连接
//        IsReleased 同一真源：成功时 reclaim 有效且未释放。
//
// 线程约定同 pop_all.cc：不用 sleep_for 依赖时序；等待一律有界轮询/yield。
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

// ---- 分配失败/计数注入（本可执行文件私有；thread_local 不污染 runtime 线程）----
// fail_at=0 关闭；>0 时第 fail_at 次 operator new 抛 bad_alloc；calls 记录序号。
thread_local int g_fail_at = 0;
thread_local int g_calls = 0;
thread_local std::size_t g_alloc_sizes[16]{};

void Arm(int index) {
    g_calls = 0;
    for (auto& size : g_alloc_sizes) {
        size = 0;
    }
    g_fail_at = index;
}
void Disarm() { g_fail_at = 0; }

std::string AllocTrace() {
    std::string out = "calls=" + std::to_string(g_calls) + " sizes=";
    for (int i = 0; i < g_calls && i < 16; ++i) {
        out += std::to_string(g_alloc_sizes[i]);
        out += ',';
    }
    return out;
}

}  // namespace
void* operator new(std::size_t size) {
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
void* operator new[](std::size_t size) { return ::operator new(size); }
void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete[](void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void* memory, std::size_t) noexcept { std::free(memory); }

namespace {

using namespace bbt::infra::net;
using namespace std::chrono_literals;

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
    }, "net.repro_oom");
    return future;
}

bbt::coroutine::WaitOptions Until(std::chrono::milliseconds duration) {
    return {std::chrono::steady_clock::now() + duration};
}

// 由 TryPopAll 结果按 CoTcpConnection::Read 相同方式构造 Buffer（复刻 Read
// 的移交步骤，用于 d4/d5 的 Buffer 形态断言）。
Buffer Convert(ReceiveRing::PopAllResult& popped) {
    std::vector<BufferSegment> segments =
        std::move(popped.takeout->segments);
    return BufferTestAccess::Make(std::move(popped.takeout), std::move(segments));
}

}  // namespace

BOOST_AUTO_TEST_SUITE(net_repro_buffer_oom)

// ---- D1（修复后契约）：Read 不做 post-pop 分配 ----
// 注入逐序号扫描：TryPopAll 内部的可失败步骤仍允许出现（它们先于首个变更
// 步骤，失败零消费可重试），但 Read 侧不再有第 4 次分配——任何序号下
// bad_alloc 都不会逃出 Read，已提交字节不会随异常丢失。
BOOST_AUTO_TEST_CASE(d1_read_post_pop_oom_no_loss) {
    StartRuntime(1);
    // 修复后断言：任何注入序号下 Read 要么成功交付、要么 Error+not_enough_memory
    // 且零消费可重试；绝不抛出且绝不丢已提交字节。
    bool saw_error = false;
    bool saw_data = false;
    for (int index = 1; index <= 6; ++index) {
        auto state = std::make_shared<ConnectionState>(1, 256, 1024, 4, nullptr);
        BOOST_REQUIRE(state->ring.TryAppend(std::string(64, 'X')).status == ReadStatus::Ok);
        state->ring.MarkEof();  // 交尽后重试应得 Eof——丢字节可观测
        CoTcpConnection conn(ConnectionTestAccess::Make(state));
        std::atomic<int> threw{-1};
        std::atomic<int> first_state{-1};
        std::atomic<std::size_t> first_bytes{0};
        std::atomic<int> retry_state{-1};
        std::atomic<std::size_t> retry_bytes{0};
        std::atomic<int> used_after{-1};
        auto task = Task([&, index] {
            ReadOutcome outcome;
            bool threw_local = false;
            Arm(index);
            try {
                outcome = conn.Read(Until(2000ms));
            } catch (const std::bad_alloc&) {
                threw_local = true;
            }
            Disarm();
            threw.store(threw_local ? 1 : 0);
            first_state.store(static_cast<int>(outcome.state));
            first_bytes.store(outcome.data.Size());
            used_after.store(static_cast<int>(state->ring.Size()));
            const auto retry = conn.Read(Until(2000ms));
            retry_state.store(static_cast<int>(retry.state));
            retry_bytes.store(retry.data.Size());
        });
        BOOST_REQUIRE(task.wait_for(10s) == std::future_status::ready);
        task.get();
        BOOST_TEST_MESSAGE("read_fail_at=" << index
            << " threw=" << threw.load()
            << " state=" << first_state.load()
            << " bytes=" << first_bytes.load()
            << " used_after=" << used_after.load()
            << " retry_state=" << retry_state.load()
            << " retry_bytes=" << retry_bytes.load());
        // 修复后契约：bad_alloc 不逃出 Read（post-pop 分配已消失，pop 内部
        // 分配失败被 TryPopAll 折成 Error+ENOMEM）。
        BOOST_CHECK_EQUAL(threw.load(), 0);
        if (first_state.load() == static_cast<int>(ReadState::Data)) {
            // 成功交付：字节完整取回。
            saw_data = true;
            BOOST_CHECK_EQUAL(first_bytes.load(), 64);
            BOOST_CHECK_EQUAL(used_after.load(), 0);
        } else {
            // 失败路径：状态是 Error+ENOMEM 且零消费（ring 字节仍在），
            // 重试取回全部 64 字节——失败零消费不变量成立。
            saw_error = true;
            BOOST_CHECK(first_state.load() == static_cast<int>(ReadState::Error));
            BOOST_CHECK_EQUAL(used_after.load(), 64);
            BOOST_CHECK(retry_state.load() == static_cast<int>(ReadState::Data));
            BOOST_CHECK_EQUAL(retry_bytes.load(), 64);
        }
    }
    // 扫描覆盖必须同时见到两类结果：成功交付与零消费失败——证明注入窗口
    // 仍然存在（pop 内部分配）但不再能吞掉已提交字节。
    BOOST_TEST_MESSAGE("d1 saw_data=" << saw_data << " saw_error=" << saw_error);
    BOOST_CHECK(saw_data);
    BOOST_CHECK(saw_error);
}

// ---- D2（修复后契约）：grant→pop→append OOM 返回错误而非抛出 ----
// TryAppend 在「grant 后 pop 移走 backing」交错下需要重新分配；分配失败
// 折成 Error+not_enough_memory 返回值，SubmitReceivedOwner 的 m_recv_hold
// 登记分支可达，owner 已 recv 字节不再随异常丢失。
BOOST_AUTO_TEST_CASE(d2_grant_pop_append_oom_no_throw) {
    ReceiveRing ring(256);
    BOOST_REQUIRE(ring.TryAppend("old").status == ReadStatus::Ok);
    const auto grant = ring.TryReserveForAppend(64);
    BOOST_REQUIRE(grant.status == ReadStatus::Ok);
    BOOST_REQUIRE(grant.granted == 64);
    // 并发读侧在 owner recv 完成、SubmitReceivedOwner 之前 pop 走全部存储。
    auto popped = ring.TryPopAll();
    BOOST_REQUIRE(popped.status == ReadStatus::Ok);
    // ring 存储已移交：下次 append 必须重新分配。注入分配失败。
    bool threw = false;
    ReadResult appended;
    Arm(1);
    try {
        appended = ring.TryAppend("recv-bytes");
    } catch (const std::bad_alloc&) {
        threw = true;
    }
    Disarm();
    BOOST_TEST_MESSAGE("d2 grant_then_pop capacity_after_pop=0 append_threw=" << threw
        << " appended_status=" << static_cast<int>(appended.status)
        << " appended_error=" << appended.error.value()
        << " used_after=" << ring.Size() << " " << AllocTrace());
    // 修复后契约：OOM 不抛出，以 Error+not_enough_memory 返回；
    // SubmitReceivedOwner 据此登记 m_recv_hold，字节不丢（ring 未接纳）。
    BOOST_CHECK(!threw);
    BOOST_CHECK(appended.status == ReadStatus::WouldBlock ||
                appended.status == ReadStatus::Error);
    BOOST_CHECK(appended.error);
    BOOST_CHECK_EQUAL(ring.Size(), 0);
    // 正对照：同一交错下不注入失败时 append 成功。
    ReceiveRing ring2(256);
    BOOST_REQUIRE(ring2.TryAppend("old").status == ReadStatus::Ok);
    BOOST_REQUIRE(ring2.TryReserveForAppend(64).status == ReadStatus::Ok);
    BOOST_REQUIRE(ring2.TryPopAll().status == ReadStatus::Ok);
    const auto ok_append = ring2.TryAppend("recv-bytes");
    BOOST_CHECK(ok_append.status == ReadStatus::Ok);
    BOOST_CHECK_EQUAL(ok_append.error.value(), 0);
}

// ---- D3（修复后契约）：pending/detached 移交不复制载荷 ----
// 短载荷仍可在移交后保持「段指针不落在 takeout string 对象内联区间」——
// pending/detached 在移交前已确保持有堆 backing，move 即指针移交。
BOOST_AUTO_TEST_CASE(d3_sso_payload_not_copied) {
    auto ring = std::make_unique<ReceiveRing>(256);
    BOOST_REQUIRE(ring->AppendDeferred("P"));
    auto popped = ring->TryPopAll();
    BOOST_REQUIRE(popped.status == ReadStatus::Ok);
    BOOST_REQUIRE_EQUAL(popped.takeout->pending, "P");
    // 移交后段指针不再落在 takeout->pending 对象的 SSO 内联区间：
    // 短载荷也拥有独立堆 backing，移交不复制。
    const auto* seg_ptr =
        reinterpret_cast<const unsigned char*>(popped.takeout->segments.front().data);
    const auto* pending_obj =
        reinterpret_cast<const unsigned char*>(&popped.takeout->pending);
    const bool seg_inside_object =
        seg_ptr >= pending_obj && seg_ptr < pending_obj + sizeof(popped.takeout->pending);
    BOOST_TEST_MESSAGE("d3 sso seg_inside_takeout_object=" << seg_inside_object
        << " pending_size=" << popped.takeout->pending.size()
        << " pending_data_is_heap=" << (seg_ptr != nullptr));
    BOOST_CHECK(!seg_inside_object);
    BOOST_CHECK_EQUAL(popped.takeout->segments.front().size, 1);
    BOOST_CHECK_EQUAL(*popped.takeout->segments.front().data, 'P');

    // detached 同样：短尾部也走堆 backing。
    auto ring_d = std::make_unique<ReceiveRing>(256);
    BOOST_REQUIRE(ring_d->StageDetached("D"));
    auto popped_d = ring_d->TryPopAll();
    BOOST_REQUIRE(popped_d.status == ReadStatus::Ok);
    BOOST_REQUIRE_EQUAL(popped_d.takeout->detached, "D");
    const auto* dseg =
        reinterpret_cast<const unsigned char*>(popped_d.takeout->segments.front().data);
    const auto* dobj =
        reinterpret_cast<const unsigned char*>(&popped_d.takeout->detached);
    const bool dseg_inline =
        dseg >= dobj && dseg < dobj + sizeof(popped_d.takeout->detached);
    BOOST_CHECK(!dseg_inline);

    // 对照：长载荷（>SSO）依然真移动，指针同址。
    auto big = std::make_unique<ReceiveRing>(4096);
    const std::string payload(2048, 'Q');
    BOOST_REQUIRE(big->AppendDeferred(payload));
    auto big_pop = big->TryPopAll();
    BOOST_REQUIRE(big_pop.status == ReadStatus::Ok);
    const auto* big_seg = reinterpret_cast<const unsigned char*>(
        big_pop.takeout->segments.front().data);
    const auto* big_obj = reinterpret_cast<const unsigned char*>(
        &big_pop.takeout->pending);
    const bool big_inline =
        big_seg >= big_obj && big_seg < big_obj + sizeof(big_pop.takeout->pending);
    BOOST_CHECK(!big_inline);
}

// ---- D4（修复后契约）：Buffer 拷贝已删除，OOM 悬空段形态不可达 ----
BOOST_AUTO_TEST_CASE(d4_copy_assign_deleted) {
    static_assert(!std::is_copy_constructible_v<Buffer>,
                  "修复后契约：Buffer 不可拷贝构造");
    static_assert(!std::is_copy_assignable_v<Buffer>,
                  "修复后契约：Buffer 不可拷贝赋值");
    static_assert(std::is_move_constructible_v<Buffer>,
                  "修复后契约：Buffer 可移动构造");
    static_assert(std::is_move_assignable_v<Buffer>,
                  "修复后契约：Buffer 可移动赋值");
    // 移动语义仍可用：move 后来源为空、目标持有全部段。
    ReceiveRing one(256);
    BOOST_REQUIRE(one.TryAppend(std::string(64, 'A')).status == ReadStatus::Ok);
    auto p1 = one.TryPopAll();
    BOOST_REQUIRE(p1.status == ReadStatus::Ok);
    Buffer dest = Convert(p1);
    Buffer moved = std::move(dest);
    BOOST_CHECK_EQUAL(moved.Size(), 64);
    BOOST_CHECK(dest.Segments().empty());
    // move-assign：旧 backing 移交，不留旧段悬空形态。
    Buffer assigned;
    assigned = std::move(moved);
    BOOST_CHECK_EQUAL(assigned.Size(), 64);
    BOOST_CHECK_EQUAL(assigned.CopyTo().front(), 'A');
    BOOST_TEST_MESSAGE("d4 copy_deleted move_ok=1");
}

// ---- D5（修复后契约）：backing+segments 构造已收回 private ----
BOOST_AUTO_TEST_CASE(d5_public_ctor_forge_denied) {
    static_assert(!std::is_constructible_v<Buffer, std::shared_ptr<void>,
                                         std::vector<BufferSegment>>,
                  "修复后契约：公共构造已收回，owning 不变量不可伪造");
    // 内部接缝仍可构造（等价于 Read 移交路径）：BufferTestAccess::Make。
    char backing_byte = 'B';
    auto backing = std::make_shared<char>(backing_byte);
    const char* seg_ptr = backing.get();
    Buffer made = BufferTestAccess::Make(
        std::static_pointer_cast<void>(backing),
        std::vector<BufferSegment>{{seg_ptr, 1}});
    BOOST_CHECK(BufferTestAccess::Backing(made) == backing.get());
    BOOST_CHECK_EQUAL(made.Size(), 1);
    BOOST_CHECK_EQUAL(made.CopyTo(), "B");
    BOOST_TEST_MESSAGE("d5 forge_denied=1 test_access_make_ok=1");
}

// ---- D6（修复后契约）：CoAccept reclaim 与连接同一事实源 ----
BOOST_AUTO_TEST_CASE(d6_coaccept_reclaim_valid_ticket) {
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
        ~RunnerGuard() {
            engine.RequestClose();
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
        res.connection.RequestClose();
    });
    std::error_code ec;
    NativeSocket client = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, ec);
    BOOST_REQUIRE(!ec);
    const auto conn_err = client.Connect(addresses[0]);
    BOOST_CHECK(!conn_err || conn_err.value() == EINPROGRESS
                || conn_err.value() == EWOULDBLOCK);
    pollfd pfd{client.Fd(), POLLOUT, 0};
    BOOST_REQUIRE(::poll(&pfd, 1, 3000) > 0);
    BOOST_REQUIRE(!client.PendingError());
    BOOST_REQUIRE(task.wait_for(10s) == std::future_status::ready);
    task.get();
    BOOST_TEST_MESSAGE("d6 accept_error=" << accept_error.load()
        << " conn_valid=" << conn_valid.load()
        << " reclaim_valid=" << reclaim_valid.load()
        << " reclaim_released=" << reclaim_released.load()
        << " conn_released=" << conn_released.load());
    BOOST_REQUIRE_EQUAL(accept_error.load(), 0);
    // 修复后契约：reclaim 有效且与连接同一事实源——fd 存活时 ticket 报未释放。
    BOOST_CHECK_EQUAL(conn_valid.load(), 1);
    BOOST_CHECK_EQUAL(conn_released.load(), 0);
    BOOST_CHECK_EQUAL(reclaim_valid.load(), 1);   // 有效票
    BOOST_CHECK_EQUAL(reclaim_released.load(), 0); // 与 conn 同一事实：未释放
    (void)client.Close();
    engine.RequestClose();
    runner.join();
    BOOST_CHECK(!run_error);
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
