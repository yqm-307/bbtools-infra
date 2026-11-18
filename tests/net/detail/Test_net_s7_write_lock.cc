/**
 * S7 写泵按轮持锁 + owner try_lock 重试：确定性交错测试。
 *
 * 直构 ConnectionState，自备 TestHooks；以 std::thread + 屏障
 *（mutex/cv/atomic、有界 yield 轮询，无 sleep_for）驱动各方交错，
 *不依赖真实网络，不改产品代码。
 *
 * 覆盖项（对应 S7 简报与实现交接）：
 *  1. owner try_lock 失败后，持锁者解锁接手 retry：两种先后均不丢 retry，
 *     最终数据全部送达，每条写恰一次终态，不重复泵出错；
 *  2. 每个持锁出口（提交/撤销/查询/关闭等）的解锁均走 retry-claim 点
 *     （_UnlockWrite），且唤醒后 owner 仍能继续推进；
 *  3. 轮间 CancelWrite/超时撤销返回的 n 为终值，且与 sink 实收字节一致；
 *  4. 按轮持锁下轮数/字节预算语义不变，contention 后 owner 能继续推进（fairness）；
 *  5. 选择器 counts 复用：见 Test_net_primitives load_balancing 组（结果与改前一致）。
 *
 * 判据全部为状态断言（送达字节、终态 id/sent/error、lock_busy 计数、
 * 钩子触发次数），不靠时序或 sleep 判定。
 *
 * 常量取舍：写 op 3000 字节、byte_budget 4000 字节。OutboundQueue 的 GatherViews
 * 会把 ring 内连续未发数据合并成一个视图，因此单轮 sendmsg 最多投递 byte_budget
 * 字节；按轮预算小于一个 op 且大于零时，可在 op 边界处制造“轮间”窗口：
 * 轮 0 发完 op1、轮 1 屏障处 op2 仍完全未发（或 op1 仅部分发送）。
 */

#define BOOST_TEST_NO_MAIN
#define BOOST_TEST_ALTERNATIVE_INIT_API
#include <boost/test/included/unit_test.hpp>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/RawHandler.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/EngineTestAccess.hpp>
#include <bbt/infra/net/detail/TestHooks.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

using namespace bbt::infra::net;
using namespace std::chrono_literals;

namespace {

constexpr std::size_t kOpBytes = 3000;
constexpr std::size_t kBigOpBytes = 5000;
constexpr std::size_t kOutboundMax = 1 << 20;
constexpr std::size_t kMaxOps = 8;
constexpr std::size_t kByteBudget = 4000; // 小于一个 op，大于零，用于在 op 边界拆分轮
constexpr std::size_t kMaxRounds = 4;
constexpr auto kWait = 5000ms;

bool WaitUntil(const std::function<bool()>& pred, std::chrono::milliseconds timeout)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) {
            return true;
        }
        std::this_thread::yield();
    }
    return pred();
}

/** 模拟发送 sink（运行在写锁内）。per_call_cap 为 0 时一次投递全部视图；
 * 非零时每段最多投递 per_call_cap 字节，用于制造部分发送。
 * block_call 为 1-based 调用索引时，该调用投递完毕后阻塞直到 release()，
 * 用于让 holder 线程持写锁停留，制造确定性的锁竞争窗口。 */
class MockSink {
public:
    MockSink(std::size_t block_call = 0, bool block_enabled = false, std::size_t per_call_cap = 0)
        : m_block_call(block_call)
        , m_block_enabled(block_enabled)
        , m_per_call_cap(per_call_cap)
    {
    }

    ssize_t operator()(const std::string_view* views, std::size_t count, std::size_t budget)
    {
        std::size_t sent = 0;
        {
            std::lock_guard<std::mutex> lk(m_mx);
            ++m_call_count;
            if (count != 0 && budget != 0) {
                std::size_t take = views[0].size();
                if (m_per_call_cap != 0) {
                    take = std::min(take, m_per_call_cap);
                }
                take = std::min(take, budget);
                m_chunks.emplace_back(views[0].data(), take);
                m_total_bytes += take;
                sent = take;
            }
        }
        if (m_block_enabled && m_call_count == m_block_call) {
            m_in_block.store(true);
            std::unique_lock<std::mutex> lk(m_block_mx);
            m_block_cv.wait(lk, [this] { return m_block_release.load(); });
            m_in_block.store(false);
        }
        return static_cast<ssize_t>(sent);
    }

    void release()
    {
        m_block_release.store(true);
        m_block_cv.notify_all();
    }

    bool in_block() const { return m_in_block.load(); }

    std::size_t call_count() const
    {
        std::lock_guard<std::mutex> lk(m_mx);
        return m_call_count;
    }

    std::size_t total_bytes() const
    {
        std::lock_guard<std::mutex> lk(m_mx);
        return m_total_bytes;
    }

private:
    mutable std::mutex m_mx;
    std::size_t m_call_count{0};
    std::size_t m_total_bytes{0};
    std::vector<std::string> m_chunks;

    std::mutex m_block_mx;
    std::condition_variable m_block_cv;
    std::atomic<bool> m_block_release{false};
    std::atomic<bool> m_in_block{false};
    const std::size_t m_block_call;
    const bool m_block_enabled;
    const std::size_t m_per_call_cap;
};

struct HookCounters {
    std::atomic<int> try_lock_busy{0};
    std::atomic<int> unlock_before_retry{0};
    std::atomic<std::size_t> last_unlock_tag{0};
};

enum ExitTag : std::size_t {
    TagNone = 0,
    TagSubmitWrite = 1,
    TagCancelWrite = 2,
    TagQueryWrite = 3,
    TagTryPopTerminal = 4,
    TagDrainTerminal = 5,
    TagHasPendingWrites = 6,
    TagActiveOpCount = 7,
    TagCanAcceptWrite = 8,
    TagRequestClose = 9,
    TagMarkClosed = 10,
    TagClosed = 11,
    TagCloseReason = 12,
    TagAcceptance = 13,
    TagLastWriteError = 14,
};

struct ExitExpectation {
    std::vector<CompletedWrite> popped; // 出口期内弹出的终态
    std::vector<std::tuple<std::uint64_t, std::size_t, std::error_code>>
        expected;                       // 期望的全部终态（popped + 最终 drain）
};

struct Fixture {
    std::shared_ptr<std::atomic<bool>> sealed;
    std::shared_ptr<TestHooks> hooks;
    std::shared_ptr<ConnectionState> state;
    std::uint64_t op1{0};
    std::uint64_t op2{0};

    Fixture()
        : sealed(std::make_shared<std::atomic<bool>>(false))
        , hooks(std::make_shared<TestHooks>())
        , state(
              std::make_shared<ConnectionState>(
                  1, 65536, kOutboundMax, kMaxOps, sealed, std::shared_ptr<ReclaimRecord>{}, hooks))
    {
        std::uint64_t id = 0;
        BOOST_REQUIRE_MESSAGE(!state->SubmitWrite(std::string(kOpBytes, 'A'), id), "op1 接纳失败");
        op1 = id;
        BOOST_REQUIRE_MESSAGE(!state->SubmitWrite(std::string(kOpBytes, 'B'), id), "op2 接纳失败");
        op2 = id;
    }

    std::vector<CompletedWrite> drain()
    {
        std::vector<CompletedWrite> out;
        CompletedWrite item;
        while (state->TryPopTerminal(item)) {
            out.push_back(item);
        }
        return out;
    }
};

/** 确定性编排（所有线程在返回前均已 join，exit_fn 抛异常时也会放行 owner
 * 并 join，不挂起测试进程）：
 * 1. holder 线程跑 PumpWrites(1 轮)，sink 在第 1 次调用内阻塞（此时 holder 持写锁）；
 * 2. owner 线程跑 TryPumpWrites，观测 lock_busy 并登记 retry；
 * 3. 释放 holder，holder 解锁并接手 retry（唤醒 owner），写完 op1 后结束；
 * 4. 执行 exit_fn（测试线程，此时写锁已空闲；标签由 exit_fn 自行设置）；
 * 5. 放行 owner 重试，owner 必取得空闲锁并继续推进。
 *
 * exit_fn 返回出口期内弹出的终态。返回 owner 的首次与重试结果。 */
struct DanceResult {
    ConnectionState::SendResult first;
    ConnectionState::SendResult second;
    bool first_busy{false};
    ExitExpectation exit_outcome;
};

DanceResult
RunDance(std::shared_ptr<ConnectionState> state, MockSink& sink, const std::function<ExitExpectation()>& exit_fn)
{
    std::atomic<bool> busy_seen{false};
    ConnectionState::SendResult first{};
    ConnectionState::SendResult second{};
    ExitExpectation exit_outcome;

    std::mutex go_mx;
    std::condition_variable go_cv;
    bool go = false;
    auto go_wait = [&]() {
        std::unique_lock<std::mutex> lk(go_mx);
        go_cv.wait(lk, [&] { return go; });
    };

    struct GoReleaser {
        std::mutex* mx;
        std::condition_variable* cv;
        bool* go;

        ~GoReleaser()
        {
            std::lock_guard<std::mutex> lk(*mx);
            *go = true;
            cv->notify_all();
        }
    } go_releaser{&go_mx, &go_cv, &go};

    std::thread holder_thread([&] { state->PumpWrites(1, kByteBudget, sink); });
    WaitUntil([&] { return sink.in_block(); }, kWait); // holder 持写锁阻塞在 sink 内

    std::thread owner_thread([&] {
        first = state->TryPumpWrites(kMaxRounds, kByteBudget, sink);
        busy_seen.store(first.lock_busy);
        go_wait();
        second = state->TryPumpWrites(kMaxRounds, kByteBudget, sink);
    });
    WaitUntil([&] { return busy_seen.load(); }, kWait); // owner 观测到忙并登记 retry

    sink.release();                                     // holder 解锁，接手 retry，写完 op1
    if (holder_thread.joinable()) {
        holder_thread.join();
    }
    exit_outcome = exit_fn(); // 测试线程执行持锁出口（写锁已空闲）
    // 必须在 join owner 之前放行，否则与 owner_thread.join() 互锁死锁。
    {
        std::lock_guard<std::mutex> lk(go_mx);
        go = true;
    }
    go_cv.notify_all();
    if (owner_thread.joinable()) {
        owner_thread.join();
    }
    // GoReleaser 析构时 go 已为 true，无副作用。

    return {first, second, busy_seen.load(), std::move(exit_outcome)};
}

bool IsOk(const std::error_code& error)
{
    return !error;
}

const std::error_code kOpCanceled = std::make_error_code(std::errc::operation_canceled);

bool IsCanceled(const std::error_code& error)
{
    return error == kOpCanceled;
}

std::vector<std::tuple<std::uint64_t, std::size_t, std::error_code>> Project(const std::vector<CompletedWrite>& terms)
{
    std::vector<std::tuple<std::uint64_t, std::size_t, std::error_code>> out;
    out.reserve(terms.size());
    for (const auto& t : terms) {
        out.emplace_back(t.id, t.sent, t.error);
    }
    return out;
}

void ArmTryLockBusy(HookCounters& c, std::shared_ptr<TestHooks> hooks)
{
    hooks->Arm(HookPoint::AfterWriteTryLockBusy, [&c] { c.try_lock_busy.fetch_add(1); }, false);
}

void ArmUnlockBeforeRetry(HookCounters& c, std::shared_ptr<TestHooks> hooks, std::atomic<std::size_t>& current_tag)
{
    hooks->Arm(
        HookPoint::AfterWriteUnlockBeforeRetry,
        [&c, &current_tag] {
            c.unlock_before_retry.fetch_add(1);
            c.last_unlock_tag.store(current_tag.load());
        },
        false);
}

} // namespace

BOOST_AUTO_TEST_SUITE(net_s7_write_lock)

/** (1) 先序：owner try_lock 先见忙。holder 持锁阻塞，owner 首次 try_lock 失败
 * 并登记 retry；holder 解锁接手 retry 后，owner 重试取得空闲锁并继续推进。
 * 期望：全部 6000 字节送达，op1/op2 各恰一次终态且全成功，try_lock_busy 仅一次，
 * 不重复泵出错。 */
BOOST_AUTO_TEST_CASE(try_busy_retry_delivers_all)
{
    Fixture f;
    MockSink sink(1, true); // holder 第 1 次 sendmsg 阻塞，持写锁
    HookCounters counters;
    std::atomic<std::size_t> current_tag{TagNone};
    ArmTryLockBusy(counters, f.hooks);
    ArmUnlockBeforeRetry(counters, f.hooks, current_tag);

    auto dance = RunDance(f.state, sink, []() -> ExitExpectation { return {}; });

    BOOST_REQUIRE_MESSAGE(dance.first_busy, "首次 try_lock 应失败并登记 retry");
    BOOST_REQUIRE_MESSAGE(!dance.second.lock_busy, "holder 解锁接手 retry 后，owner 重试应取得空闲锁");
    BOOST_REQUIRE_EQUAL(counters.try_lock_busy.load(), 1);
    BOOST_REQUIRE_EQUAL(sink.total_bytes(), 2 * kOpBytes);

    auto terms = f.drain();
    BOOST_REQUIRE_EQUAL(terms.size(), 2);
    std::map<std::uint64_t, CompletedWrite> by_id;
    for (const auto& t : terms) {
        by_id.emplace(t.id, t);
    }
    BOOST_REQUIRE_EQUAL(by_id.size(), 2);
    BOOST_REQUIRE(by_id.count(f.op1) && by_id[f.op1].sent == kOpBytes && IsOk(by_id[f.op1].error));
    BOOST_REQUIRE(by_id.count(f.op2) && by_id[f.op2].sent == kOpBytes && IsOk(by_id[f.op2].error));
    BOOST_TEST_MESSAGE(
        "SENT_BYTES=" << sink.total_bytes() << " TERMINALS=" << terms.size()
                      << " TRY_LOCK_BUSY=" << counters.try_lock_busy.load()
                      << " UNLOCK_BEFORE_RETRY=" << counters.unlock_before_retry.load());
}

/** (1) 后序：owner 注册 retry 时已晚于某次解锁（该次解锁看到 retry=false）。
 * 先由 holder 不带争用地完成一轮，再进入争用窗口；owner 首次 try_lock 仍失败，
 * 后续由下一持锁者解锁接手，重试必成功。 */
BOOST_AUTO_TEST_CASE(uncontended_try_succeeds_after_unlock)
{
    Fixture f;
    MockSink immediate(0, false);

    // holder0 不带争用地完成一轮：此时 retry 未被登记，其解锁不接手任何 retry。
    {
        std::thread h([&] { f.state->PumpWrites(1, kByteBudget, immediate); });
        h.join();
    }
    BOOST_REQUIRE_EQUAL(immediate.total_bytes(), kByteBudget); // holder0 按 byte_budget 发送（op1 全部 + op2 部分）

    HookCounters counters;
    std::atomic<std::size_t> current_tag{TagNone};
    ArmTryLockBusy(counters, f.hooks);
    ArmUnlockBeforeRetry(counters, f.hooks, current_tag);

    MockSink sink(1, true);
    auto dance = RunDance(f.state, sink, []() -> ExitExpectation { return {}; });

    BOOST_REQUIRE_MESSAGE(dance.first_busy, "争用窗口内首次 try_lock 应失败");
    BOOST_REQUIRE_MESSAGE(!dance.second.lock_busy, "置位晚于解锁后，owner 随后的 try_lock 必成功");
    BOOST_REQUIRE_EQUAL(immediate.total_bytes() + sink.total_bytes(), 2 * kOpBytes);

    auto terms = f.drain();
    BOOST_REQUIRE_EQUAL(terms.size(), 2);
    bool ok1 = false, ok2 = false;
    for (const auto& t : terms) {
        if (t.id == f.op1 && t.sent == kOpBytes && IsOk(t.error)) {
            ok1 = true;
        }
        if (t.id == f.op2 && t.sent == kOpBytes && IsOk(t.error)) {
            ok2 = true;
        }
    }
    BOOST_REQUIRE_MESSAGE(ok1 && ok2, "两条写均须恰一次成功终态");
}

/** (1) 三方：两个先后持锁者，每次 owner try_lock 失败后，均由随后解锁的
 * 持锁者接手 retry（retry 不丢）。期望观测 2 次 lock_busy，最终全部送达。 */
BOOST_AUTO_TEST_CASE(two_contentions_next_holder_claims)
{
    Fixture f;
    MockSink sink1(1, true);
    MockSink sink2(1, true);
    HookCounters counters;
    std::atomic<std::size_t> current_tag{TagNone};
    ArmTryLockBusy(counters, f.hooks);

    // H1 阻塞；owner try #1 失败（sink1 被 owner 复用，但 try 在发送前即失败）。
    std::thread h1([&] { f.state->PumpWrites(1, kByteBudget, sink1); });
    BOOST_REQUIRE_MESSAGE(WaitUntil([&] { return sink1.in_block(); }, kWait), "H1 未进入阻塞窗口");
    ConnectionState::SendResult r1{};
    std::atomic<bool> o1done{false};
    std::thread o1([&] {
        r1 = f.state->TryPumpWrites(kMaxRounds, kByteBudget, sink1);
        o1done.store(true);
    });
    BOOST_REQUIRE_MESSAGE(WaitUntil([&] { return o1done.load(); }, kWait), "owner try #1 未返回");
    BOOST_REQUIRE_MESSAGE(r1.lock_busy, "owner try #1 应失败并登记 retry");

    // H1 解锁接手 retry。
    sink1.release();
    h1.join();

    // H2 又持锁阻塞；owner try #2 又失败（retry 再次被登记）。
    std::thread h2([&] { f.state->PumpWrites(1, kByteBudget, sink2); });
    BOOST_REQUIRE_MESSAGE(WaitUntil([&] { return sink2.in_block(); }, kWait), "H2 未进入阻塞窗口");
    ConnectionState::SendResult r2{};
    std::atomic<bool> o2done{false};
    std::thread o2([&] {
        r2 = f.state->TryPumpWrites(kMaxRounds, kByteBudget, sink2);
        o2done.store(true);
    });
    BOOST_REQUIRE_MESSAGE(WaitUntil([&] { return o2done.load(); }, kWait), "owner try #2 未返回");
    BOOST_REQUIRE_MESSAGE(r2.lock_busy, "owner try #2 应因 H2 占锁而失败");

    // H2 解锁接手 retry；owner try #3 取得空闲锁。
    sink2.release();
    h2.join();
    if (o1.joinable()) {
        o1.join();
    }
    if (o2.joinable()) {
        o2.join();
    }
    ConnectionState::SendResult r3 = f.state->TryPumpWrites(kMaxRounds, kByteBudget, sink1);
    BOOST_REQUIRE_MESSAGE(!r3.lock_busy, "owner try #3 应取得空闲锁");

    BOOST_REQUIRE_EQUAL(counters.try_lock_busy.load(), 2);
    BOOST_REQUIRE_EQUAL(sink1.total_bytes() + sink2.total_bytes(), 2 * kOpBytes);

    auto terms = f.drain();
    BOOST_REQUIRE_EQUAL(terms.size(), 2);
    bool ok1 = false, ok2 = false;
    for (const auto& t : terms) {
        if (t.id == f.op1 && t.sent == kOpBytes && IsOk(t.error)) {
            ok1 = true;
        }
        if (t.id == f.op2 && t.sent == kOpBytes && IsOk(t.error)) {
            ok2 = true;
        }
    }
    BOOST_REQUIRE_MESSAGE(ok1, "op1 未恰一次成功终态");
    BOOST_REQUIRE_MESSAGE(ok2, "op2 未恰一次成功终态");
    BOOST_TEST_MESSAGE(
        "TRY_LOCK_BUSY=" << counters.try_lock_busy.load()
                         << " SENT_BYTES=" << sink1.total_bytes() + sink2.total_bytes());
}

/** (2) 每个持锁出口的解锁均经过 retry-claim 点（_UnlockWrite），且出口执行后
 * owner 重试仍能继续推进（唤醒不丢失）。对每个出口：fresh state，holder 阻塞
 * 写 op1、owner 登记 retry、holder 解锁接手、执行出口、owner 重试推进。 */
struct ExitSpec {
    const char* name;
    std::size_t tag;
    std::size_t expect_exit_unlocks; // 该出口自身的解锁次数（1 或 2，作文档）
    std::size_t expect_total_bytes;  // 所有泵结束后 sink 累计送达字节
    std::function<ExitExpectation(Fixture&)> fn;
};

BOOST_AUTO_TEST_CASE(every_locked_exit_passes_retry_point)
{
    std::vector<ExitSpec> specs = {
        {"SubmitWrite",
         TagSubmitWrite,
         1,
         8000,
         [](Fixture& fx) -> ExitExpectation {
             std::uint64_t id = 0;
             BOOST_REQUIRE_MESSAGE(!fx.state->SubmitWrite(std::string(2000, 'C'), id), "op3 接纳失败");
             return ExitExpectation{{}, {{fx.op1, kOpBytes, {}}, {fx.op2, kOpBytes, {}}, {id, 2000, {}}}};
         }},
        {"CancelWrite",
         TagCancelWrite,
         1,
         4000,
         [](Fixture& fx) -> ExitExpectation {
             WriteWait waiter{};
             auto result = fx.state->CancelWrite(fx.op2, waiter, kOpCanceled);
             BOOST_REQUIRE_EQUAL(std::get<0>(result), 0);
             // waiter 为空时返回值不承载错误，终态队列才是权威（下方 expected 已断言）。
             return ExitExpectation{{}, {{fx.op1, kOpBytes, {}}, {fx.op2, 1000, kOpCanceled}}};
         }},
        {"QueryWrite",
         TagQueryWrite,
         1,
         6000,
         [](Fixture& fx) -> ExitExpectation {
             (void)fx.state->QueryWrite(WriteWait{});
             return ExitExpectation{{}, {{fx.op1, kOpBytes, {}}, {fx.op2, kOpBytes, {}}}};
         }},
        {"TryPopTerminal",
         TagTryPopTerminal,
         1,
         6000,
         [](Fixture& fx) -> ExitExpectation {
             CompletedWrite item;
             bool popped = fx.state->TryPopTerminal(item);
             BOOST_REQUIRE_MESSAGE(popped, "应弹出 op1 终态");
             return ExitExpectation{{item}, {{fx.op1, kOpBytes, {}}, {fx.op2, kOpBytes, {}}}};
         }},
        {"DrainTerminal",
         TagDrainTerminal,
         1,
         6000,
         [](Fixture& fx) -> ExitExpectation {
             return ExitExpectation{fx.state->DrainTerminal(), {{fx.op1, kOpBytes, {}}, {fx.op2, kOpBytes, {}}}};
         }},
        {"HasPendingWrites",
         TagHasPendingWrites,
         1,
         6000,
         [](Fixture& fx) -> ExitExpectation {
             (void)fx.state->HasPendingWrites();
             return ExitExpectation{{}, {{fx.op1, kOpBytes, {}}, {fx.op2, kOpBytes, {}}}};
         }},
        {"ActiveOpCount",
         TagActiveOpCount,
         1,
         6000,
         [](Fixture& fx) -> ExitExpectation {
             (void)fx.state->ActiveOpCount();
             return ExitExpectation{{}, {{fx.op1, kOpBytes, {}}, {fx.op2, kOpBytes, {}}}};
         }},
        {"CanAcceptWrite",
         TagCanAcceptWrite,
         1,
         6000,
         [](Fixture& fx) -> ExitExpectation {
             (void)fx.state->CanAcceptWrite(1);
             return ExitExpectation{{}, {{fx.op1, kOpBytes, {}}, {fx.op2, kOpBytes, {}}}};
         }},
        {"RequestClose",
         TagRequestClose,
         1,
         4000,
         [](Fixture& fx) -> ExitExpectation {
             fx.state->RequestClose({});
             return ExitExpectation{{}, {{fx.op1, kOpBytes, {}}, {fx.op2, 1000, kOpCanceled}}};
         }},
        {"MarkClosed",
         TagMarkClosed,
         2,
         4000,
         [](Fixture& fx) -> ExitExpectation {
             fx.state->MarkClosed(kOpCanceled);
             return ExitExpectation{{}, {{fx.op1, kOpBytes, {}}, {fx.op2, 1000, kOpCanceled}}};
         }},
        {"Closed",
         TagClosed,
         1,
         6000,
         [](Fixture& fx) -> ExitExpectation {
             (void)fx.state->Closed();
             return ExitExpectation{{}, {{fx.op1, kOpBytes, {}}, {fx.op2, kOpBytes, {}}}};
         }},
        {"CloseReason",
         TagCloseReason,
         1,
         6000,
         [](Fixture& fx) -> ExitExpectation {
             (void)fx.state->CloseReason();
             return ExitExpectation{{}, {{fx.op1, kOpBytes, {}}, {fx.op2, kOpBytes, {}}}};
         }},
        {"Acceptance",
         TagAcceptance,
         1,
         6000,
         [](Fixture& fx) -> ExitExpectation {
             (void)fx.state->Acceptance();
             return ExitExpectation{{}, {{fx.op1, kOpBytes, {}}, {fx.op2, kOpBytes, {}}}};
         }},
        {"LastWriteError",
         TagLastWriteError,
         1,
         6000,
         [](Fixture& fx) -> ExitExpectation {
             (void)fx.state->LastWriteError(nullptr);
             return ExitExpectation{{}, {{fx.op1, kOpBytes, {}}, {fx.op2, kOpBytes, {}}}};
         }},
    };

    for (const auto& spec : specs) {
        Fixture f;
        MockSink sink(1, true);
        HookCounters counters;
        std::atomic<std::size_t> current_tag{TagNone};
        ArmTryLockBusy(counters, f.hooks);
        ArmUnlockBeforeRetry(counters, f.hooks, current_tag);

        auto tagged_fn = [&spec, &current_tag, &f]() -> ExitExpectation {
            current_tag.store(spec.tag);
            return spec.fn(f);
        };

        auto dance = RunDance(f.state, sink, tagged_fn);

        BOOST_REQUIRE_MESSAGE(dance.first_busy, std::string(spec.name) + ": 首次 try_lock 应失败");
        BOOST_REQUIRE_MESSAGE(!dance.second.lock_busy, std::string(spec.name) + ": 出口后 owner 重试应取得空闲锁");
        BOOST_REQUIRE_MESSAGE(
            static_cast<int>(counters.last_unlock_tag.load()) == static_cast<int>(spec.tag),
            std::string(spec.name) + ": 出口解锁的标签应与被测出口一致");
        BOOST_REQUIRE_EQUAL(counters.try_lock_busy.load(), 1);
        BOOST_REQUIRE_EQUAL(sink.total_bytes(), spec.expect_total_bytes);
        BOOST_REQUIRE_MESSAGE(
            counters.unlock_before_retry.load() >= 2,
            std::string(spec.name) + ": holder 与出口均应解锁并经过 retry 点");

        auto terms = f.drain();
        auto all = Project(dance.exit_outcome.popped);
        auto rest = Project(terms);
        all.insert(all.end(), rest.begin(), rest.end());
        BOOST_REQUIRE_MESSAGE(
            all.size() == dance.exit_outcome.expected.size(), std::string("终态数量不符：") + spec.name);
        std::sort(all.begin(), all.end());
        std::vector<decltype(dance.exit_outcome.expected)::value_type> expected = dance.exit_outcome.expected;
        std::sort(expected.begin(), expected.end());
        BOOST_REQUIRE_MESSAGE(
            std::equal(all.begin(), all.end(), expected.begin()), std::string("终态内容不符：") + spec.name);

        BOOST_TEST_MESSAGE("EXIT=" << spec.name << " SENT_BYTES=" << sink.total_bytes() << " TERMINALS=" << all.size());
    }
}

/** (3) 轮间 CancelWrite/超时撤销：撤销尚未发送的 op，返回 n=0 作为终值，
 * 且与对端实收（sink 实收）一致。 */
BOOST_AUTO_TEST_CASE(cancel_between_rounds_terminal_zero)
{
    auto sealed = std::make_shared<std::atomic<bool>>(false);
    auto hooks = std::make_shared<TestHooks>();
    auto state = std::make_shared<ConnectionState>(
        1, 65536, kOutboundMax, kMaxOps, sealed, std::shared_ptr<ReclaimRecord>{}, hooks);
    std::uint64_t op1 = 0, op2 = 0;
    BOOST_REQUIRE_MESSAGE(!state->SubmitWrite(std::string(kOpBytes, 'A'), op1), "op1 接纳失败");
    WriteWait op2_waiter{};
    BOOST_REQUIRE_MESSAGE(!state->SubmitWrite(std::string(kOpBytes, 'B'), op2, &op2_waiter), "op2 接纳失败");
    MockSink sink(0, false, kOpBytes); // 每次仅投递一个 op（3000 字节），制造部分发送

    std::atomic<bool> at_round1{false};
    std::mutex r1_mx;
    std::condition_variable r1_cv;
    bool r1_go = false;
    hooks->Arm(
        HookPoint::BetweenWriteRounds,
        [&] {
            at_round1.store(true);
            std::unique_lock<std::mutex> lk(r1_mx);
            r1_cv.wait(lk, [&] { return r1_go; });
        },
        false);

    std::thread holder([&] { state->PumpWrites(10, kByteBudget, sink); });
    BOOST_REQUIRE_MESSAGE(WaitUntil([&] { return at_round1.load(); }, kWait), "holder 未到达第 1 轮屏障");

    // 轮间撤销 op2（尚未发送）：n 必为 0，与对端实收一致。
    WriteWait waiter{};
    auto result = state->CancelWrite(op2, waiter, kOpCanceled);
    BOOST_REQUIRE_EQUAL(std::get<0>(result), 0);
    // waiter 为空时返回值不承载错误，终态队列才是权威（下方 expected 已断言）。
    // waiter 为空时返回值不承载错误，终态队列已断言 n=0、canceled。

    {
        std::lock_guard<std::mutex> lk(r1_mx);
        r1_go = true;
    }
    r1_cv.notify_all();
    holder.join();

    // 对端仅实收 op1（3000 字节），op2 一字节未收。
    BOOST_REQUIRE_EQUAL(sink.total_bytes(), kOpBytes);
    std::vector<CompletedWrite> terms;
    CompletedWrite item;
    while (state->TryPopTerminal(item)) {
        terms.push_back(item);
    }
    BOOST_REQUIRE_EQUAL(terms.size(), 2);
    bool ok1 = false, revoked = false;
    for (const auto& t : terms) {
        if (t.id == op1 && t.sent == kOpBytes && IsOk(t.error)) {
            ok1 = true;
        }
        if (t.id == op2 && t.sent == 0 && IsCanceled(t.error)) {
            revoked = true;
        }
    }
    BOOST_REQUIRE_MESSAGE(ok1 && revoked, "op1 全部送达，op2 撤销为终值 n=0");
    BOOST_TEST_MESSAGE("SENT_BYTES=" << sink.total_bytes() << " TERMINALS=" << terms.size());
}

/** (3) 部分发送后的轮间撤销：n 必为已发前缀（1000 字节），等于对端实际收到的
 * 字节；剩余后缀被撤出，不再下发。 */
BOOST_AUTO_TEST_CASE(cancel_partial_sent_terminal_prefix)
{
    auto sealed = std::make_shared<std::atomic<bool>>(false);
    auto hooks = std::make_shared<TestHooks>();
    auto state = std::make_shared<ConnectionState>(
        1, 65536, kOutboundMax, kMaxOps, sealed, std::shared_ptr<ReclaimRecord>{}, hooks);
    std::uint64_t op1 = 0, op2 = 0;
    WriteWait op1_waiter{};
    BOOST_REQUIRE_MESSAGE(!state->SubmitWrite(std::string(kOpBytes, 'A'), op1, &op1_waiter), "op1 接纳失败");
    BOOST_REQUIRE_MESSAGE(!state->SubmitWrite(std::string(kOpBytes, 'B'), op2), "op2 接纳失败");
    MockSink sink(0, false, 1000); // 每次仅投递 1000 字节，制造部分发送

    std::atomic<bool> at_round1{false};
    std::mutex r1_mx;
    std::condition_variable r1_cv;
    bool r1_go = false;
    hooks->Arm(
        HookPoint::BetweenWriteRounds,
        [&] {
            at_round1.store(true);
            std::unique_lock<std::mutex> lk(r1_mx);
            r1_cv.wait(lk, [&] { return r1_go; });
        },
        false);

    std::thread holder([&] { state->PumpWrites(10, kByteBudget, sink); });
    BOOST_REQUIRE_MESSAGE(WaitUntil([&] { return at_round1.load(); }, kWait), "holder 未到达第 1 轮屏障");

    // op1 已发 1000 字节：撤销 op1 返回 n=1000（已发前缀，终值）。
    auto result = state->CancelWrite(op1, op1_waiter, kOpCanceled);
    BOOST_REQUIRE_EQUAL(std::get<0>(result), 1000);
    // waiter 为空时返回值不承载错误，终态队列才是权威（下方 expected 已断言）。
    BOOST_REQUIRE_MESSAGE(op1_waiter.sent == 1000 && op1_waiter.error == kOpCanceled, "等待者可见的 n 与终值应一致");

    {
        std::lock_guard<std::mutex> lk(r1_mx);
        r1_go = true;
    }
    r1_cv.notify_all();
    holder.join();

    // 1000（op1 已发前缀，对端已收）+ 3000（op2 全量）= 4000，op1 后续字节不再下发。
    BOOST_REQUIRE_EQUAL(sink.total_bytes(), 1000 + kOpBytes);
    std::vector<CompletedWrite> terms;
    CompletedWrite item;
    while (state->TryPopTerminal(item)) {
        terms.push_back(item);
    }
    BOOST_REQUIRE_EQUAL(terms.size(), 2);
    bool revoked_prefix = false, op2_ok = false;
    for (const auto& t : terms) {
        if (t.id == op1 && t.sent == 1000 && IsCanceled(t.error)) {
            revoked_prefix = true;
        }
        if (t.id == op2 && t.sent == kOpBytes && IsOk(t.error)) {
            op2_ok = true;
        }
    }
    BOOST_REQUIRE_MESSAGE(revoked_prefix && op2_ok, "op1 终值为已发前缀，op2 全部送达");
    BOOST_TEST_MESSAGE("SENT_BYTES=" << sink.total_bytes() << " TERMINALS=" << terms.size());
}

/** (4) 轮数/字节预算与 fairness：单次泵受 max_rounds 与 byte_budget 双重约束，
 * 余额留待下一轮；contention 后 owner 重试仍推进完全部未发数据。 */
BOOST_AUTO_TEST_CASE(rounds_byte_budget_and_fairness)
{
    // (a) 字节预算：单次泵不得超预算，余额留待下一轮。
    {
        Fixture f;
        MockSink sink(0, false);
        auto r = f.state->PumpWrites(10, kOpBytes, sink);
        BOOST_REQUIRE_EQUAL(r.bytes, kOpBytes);            // 仅 op1
        BOOST_REQUIRE_EQUAL(sink.call_count(), 1);         // 1 轮
        BOOST_REQUIRE_EQUAL(f.state->ActiveOpCount(), 1u); // op2 仍在途
        r = f.state->PumpWrites(10, kOpBytes, sink);
        BOOST_REQUIRE_EQUAL(r.bytes, kOpBytes);            // 仅 op2
        BOOST_REQUIRE_EQUAL(f.state->ActiveOpCount(), 0u);
        auto terms = f.drain();
        BOOST_REQUIRE_EQUAL(terms.size(), 2);
        for (const auto& t : terms) {
            BOOST_REQUIRE_MESSAGE(IsOk(t.error) && t.sent == kOpBytes, "每条写须恰一次成功终态");
        }
    }
    // (b) 轮数上限：三个 3000 字节大写连续在环中，两轮各发一个，第三个留至下一轮。
    {
        auto sealed2 = std::make_shared<std::atomic<bool>>(false);
        auto hooks2 = std::make_shared<TestHooks>();
        auto bs = std::make_shared<ConnectionState>(
            1, 65536, kOutboundMax, kMaxOps, sealed2, std::shared_ptr<ReclaimRecord>{}, hooks2);
        std::uint64_t b1 = 0, b2 = 0, b3 = 0;
        BOOST_REQUIRE_MESSAGE(!bs->SubmitWrite(std::string(kOpBytes, 'X'), b1), "b1 接纳失败");
        BOOST_REQUIRE_MESSAGE(!bs->SubmitWrite(std::string(kOpBytes, 'Y'), b2), "b2 接纳失败");
        BOOST_REQUIRE_MESSAGE(!bs->SubmitWrite(std::string(kOpBytes, 'Z'), b3), "b3 接纳失败");
        MockSink sink(0, false);
        auto r = bs->PumpWrites(2, kOpBytes, sink);
        BOOST_REQUIRE_EQUAL(sink.call_count(), 1);    // 第 1 轮发 op1（预算耗尽）
        BOOST_REQUIRE_EQUAL(r.bytes, kOpBytes);       // op1
        BOOST_REQUIRE_EQUAL(bs->ActiveOpCount(), 2u); // op2、op3 仍在途
        r = bs->PumpWrites(2, kOpBytes, sink);
        BOOST_REQUIRE_EQUAL(sink.call_count(), 2);    // 第 2 轮发 op2
        BOOST_REQUIRE_EQUAL(r.bytes, kOpBytes);       // op2
        BOOST_REQUIRE_EQUAL(bs->ActiveOpCount(), 1u); // op3 仍在途
        r = bs->PumpWrites(2, kOpBytes, sink);
        BOOST_REQUIRE_EQUAL(sink.call_count(), 3);    // 第 3 轮发 op3
        BOOST_REQUIRE_EQUAL(r.bytes, kOpBytes);       // op3
        std::vector<CompletedWrite> bterms;
        CompletedWrite bi;
        while (bs->TryPopTerminal(bi)) {
            bterms.push_back(bi);
        }
        BOOST_REQUIRE_EQUAL(bterms.size(), 3);
        for (const auto& t : bterms) {
            BOOST_REQUIRE_MESSAGE(IsOk(t.error) && t.sent == kOpBytes, "每条大写须恰一次成功终态");
        }
    }
    // (c) 竞争后 fairness：holder 阻塞后 owner 重试仍推进完全部未发数据。
    {
        auto sealed2 = std::make_shared<std::atomic<bool>>(false);
        auto hooks2 = std::make_shared<TestHooks>();
        auto bs = std::make_shared<ConnectionState>(
            1, 5000, kOutboundMax, kMaxOps, sealed2, std::shared_ptr<ReclaimRecord>{}, hooks2);
        std::uint64_t b1 = 0, b2 = 0, b3 = 0;
        BOOST_REQUIRE_MESSAGE(!bs->SubmitWrite(std::string(kBigOpBytes, 'X'), b1), "b1 接纳失败");
        BOOST_REQUIRE_MESSAGE(!bs->SubmitWrite(std::string(kBigOpBytes, 'Y'), b2), "b2 接纳失败");
        BOOST_REQUIRE_MESSAGE(!bs->SubmitWrite(std::string(kBigOpBytes, 'Z'), b3), "b3 接纳失败");
        MockSink sink(1, true);
        HookCounters counters;
        std::atomic<std::size_t> current_tag{TagNone};
        ArmTryLockBusy(counters, hooks2);
        ArmUnlockBeforeRetry(counters, hooks2, current_tag);

        std::thread h([&] { bs->PumpWrites(1, kByteBudget, sink); });
        BOOST_REQUIRE_MESSAGE(WaitUntil([&] { return sink.in_block(); }, kWait), "holder 未进入阻塞窗口");
        ConnectionState::SendResult r1{};
        std::atomic<bool> odone{false};
        std::thread o([&] {
            r1 = bs->TryPumpWrites(kMaxRounds, kByteBudget, sink);
            odone.store(true);
        });
        BOOST_REQUIRE_MESSAGE(WaitUntil([&] { return odone.load(); }, kWait), "owner 试 #1 未返回");
        BOOST_REQUIRE_MESSAGE(r1.lock_busy, "owner 试 #1 应失败并登记 retry");

        sink.release();
        h.join();
        if (o.joinable()) {
            o.join();
        }

        ConnectionState::SendResult r2 = bs->TryPumpWrites(kMaxRounds, kOutboundMax, sink);
        BOOST_REQUIRE_MESSAGE(!r2.lock_busy, "owner 重试应取得空闲锁并继续推进");
        BOOST_REQUIRE_EQUAL(counters.try_lock_busy.load(), 1);
        BOOST_REQUIRE_EQUAL(sink.total_bytes(), 3 * kBigOpBytes);

        std::vector<CompletedWrite> bterms;
        CompletedWrite bi;
        while (bs->TryPopTerminal(bi)) {
            bterms.push_back(bi);
        }
        BOOST_REQUIRE_EQUAL(bterms.size(), 3);
        for (const auto& t : bterms) {
            BOOST_REQUIRE_MESSAGE(IsOk(t.error) && t.sent == kBigOpBytes, "每条大写须恰一次成功终态");
        }
        BOOST_TEST_MESSAGE("TRY_LOCK_BUSY=" << counters.try_lock_busy.load() << " SENT_BYTES=" << sink.total_bytes());
    }
}

BOOST_AUTO_TEST_SUITE_END()

/* ======================================================================
 * (5) 选择器 counts 复用：并发 connect 安全（Engine 层，真实 loopback）。
 * 多客户端并发建连，每条 connect 必被分配到有效 owner 并被完整计入，
 * 不崩溃、不丢连接。算法层面的“复用后结果与改前一致”由
 * Test_net_primitives load_balancing 组的纯单元测试覆盖。
 * ====================================================================== */

namespace {

Address Loopback(int port)
{
    std::error_code error;
    Address address = Address::Ip("127.0.0.1", port, error);
    BOOST_REQUIRE_MESSAGE(!error, "loopback 地址构造失败");
    return address;
}

void StartRuntime(std::size_t workers)
{
    g_bbt_coroutine_config->m_cfg_static_thread_num = workers;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start();
}

EngineConfig ServerConfig(const Address& listen)
{
    EngineConfig config;
    config.listen_addresses = {listen};
    config.runtime_progress = RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    return config;
}

struct StoreHandler : RawHandler {
    std::atomic<int> accepted{0};
    std::mutex mutex;
    std::shared_ptr<Connection> stored;

    void OnAccept(Connection connection) override
    {
        accepted.fetch_add(1);
        std::lock_guard<std::mutex> lock(mutex);
        stored = std::make_shared<Connection>(connection);
    }

    void OnRead(Connection) override {}

    void OnClose(Connection, std::error_code) override {}
};

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

} // namespace

BOOST_AUTO_TEST_SUITE(net_s7_concurrent_connect)

BOOST_AUTO_TEST_CASE(concurrent_connects_all_accounted)
{
    StartRuntime(2);
    auto handler = std::make_shared<StoreHandler>();
    Engine engine(handler);
    EngineTestAccess::ClearConnectInjection(engine);
    EngineConfig config = ServerConfig(Loopback(0));
    config.options.num_event_loop = 1; // 单 owner 即可；Select 被真实调用，并发 connect 安全亦得到验证
    EngineRunner runner(engine, config);
    BOOST_REQUIRE_MESSAGE(
        WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms), "engine 未进入 Running");
    std::vector<Address> addresses;
    BOOST_REQUIRE_MESSAGE(!engine.LocalAddresses(addresses) && !addresses.empty(), "无监听地址");

    const int n = 8;
    std::atomic<int> connected{0};
    std::vector<std::thread> threads;
    threads.reserve(n);
    for (int i = 0; i < n; ++i) {
        threads.emplace_back([&] {
            bool ok = false;
            NativeSocket client = ConnectClient(addresses[0], ok);
            if (ok) {
                connected.fetch_add(1);
            }
            (void)client.Close();
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    BOOST_REQUIRE_MESSAGE(
        WaitUntil([&] { return handler->accepted.load() == n; }, 5000ms), "并发 connect 未全部被 accept");
    BOOST_CHECK_EQUAL(EngineTestAccess::ConnectionCount(engine), n);
    BOOST_CHECK_EQUAL(connected.load(), n);
    runner.CloseAndJoin();
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