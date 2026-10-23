// 接收路径确定性交错用例：验证「pending 未合入前，下一次 recv 的数据会插到它前面」。
//
// 与既有用例的关系：
//   net.b4_raw_busy_deferred_onread 只验证 Busy→保留→补投 OnRead 的可见性，
//   不涉及「pending 存在期间继续 recv」。本文件补的正是那条缺口。
//
// 判据（块序与全字节分别断言，不只看块首字节）：
//   out == A+B+C（逐字节 + 长度）→ 通过（修复后应如此）
//   out == A+C+B（逐字节 + 长度）→ 违反 FIFO，必须失败并打印证据
//   其他                          → 其他不一致，单独报错（不得归入上面两类）
//
// 线程约定：本文件不使用 std::this_thread::sleep_for —— 它在非协程上下文会
// 进入 coroutine hook 的 UseSleep/Nanosleep 路径并触发断言。等待一律用
// steady_clock + yield 的有界轮询（与 b4_contract.cc:1746 的既有写法一致）。

// net 测试的退出边界（见 tests/net/ProcessRuntimeTestMain.hpp）：coroutine runtime
// 无 Stop，正常 return 会进入静态析构窗口并可能 SIGSEGV。故本文件抑制 Boost.Test
// 自动生成的 main，自己调用 unit_test_main 后在所有局部对象离开作用域再 _Exit。
#define BOOST_TEST_NO_MAIN
#define BOOST_TEST_ALTERNATIVE_INIT_API
#include <boost/test/included/unit_test.hpp>

#include <cstdio>
#include <cstdlib>
#include <iostream>

#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/NativeSocket.hpp>
#include <bbt/infra/net/RawHandler.hpp>
#include <bbt/infra/net/ReadAccess.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/EngineTestAccess.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>
#include <bbt/coroutine/coroutine.hpp>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <poll.h>

namespace {

using namespace bbt::infra::net;
using namespace std::chrono_literals;

constexpr std::size_t kSeg = 16384;   // = Engine.cc 的 kReadChunk：一段对应一次完整 recv

bool WaitUntil(const std::function<bool()>& predicate, std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::yield();   // 不用 sleep_for：见文件头线程约定
    }
    return predicate();
}

void StartRuntime(std::size_t workers) {
    g_bbt_coroutine_config->m_cfg_static_thread_num = workers;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    g_scheduler->Start();
}

Address Loopback(int port) {
    std::error_code error;
    Address address = Address::Ip("127.0.0.1", port, error);
    return address;
}

EngineConfig ServerConfig(const Address& listen) {
    EngineConfig config;
    config.listen_addresses = {listen};
    config.runtime_progress = RuntimeProgress::Independent;
    config.options.num_event_loop = 1;
    return config;
}

// 低压水线场景：把读方向总量界压到刚好容纳本例字节数。
// ReadLowWater() = max_inbound_bytes / 2。选 49152*2 使低水位 = 49152，
// 与 A+B+C 总量相等 —— 这是「消费者读完仍不低于低水位」的最不利边界，
// 用来验证 pending 导致的暂停能否真正解除（而不是永久卡住读泵）。
EngineConfig TightServerConfig(const Address& listen) {
    EngineConfig config = ServerConfig(listen);
    config.options.max_inbound_bytes = 49152 * 2;
    return config;
}

struct EngineRunner {
    Engine& engine;
    std::error_code error;
    std::thread thread;

    EngineRunner(Engine& target, EngineConfig config) : engine(target) {
        thread = std::thread([this, config]() mutable { error = engine.Run(config); });
    }
    void CloseAndJoin() {
        engine.RequestClose();
        if (thread.joinable()) {
            thread.join();
        }
    }
    ~EngineRunner() { CloseAndJoin(); }
};

NativeSocket ConnectClient(const Address& address, bool& ok) {
    ok = false;
    std::error_code error;
    NativeSocket client = NativeSocket::Open(AddressFamily::IPv4, SocketType::Stream, error);
    if (error) {
        return client;
    }
    const auto connect_error = client.Connect(address);
    if (connect_error && connect_error.value() != EINPROGRESS
        && connect_error.value() != EWOULDBLOCK) {
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

bool SendAll(NativeSocket& client, const std::string& payload,
             std::chrono::milliseconds timeout) {
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

struct StoreHandler : RawHandler {
    std::atomic<int> accepted{0};
    std::mutex mutex;
    std::shared_ptr<Connection> stored;

    void OnAccept(Connection connection) override {
        accepted.fetch_add(1);
        std::lock_guard<std::mutex> lock(mutex);
        stored = std::make_shared<Connection>(connection);
    }
    void OnRead(Connection) override {}
    void OnClose(Connection, std::error_code) override {}
};

// 段内容：整段填同一标记，段首/段尾带可识别标签。
std::string MakeSeg(char tag) {
    std::string segment(kSeg, tag);
    const std::string head = std::string("SEG=") + tag + ";BEGIN;";
    const std::string tail = std::string(";END=") + tag + ";";
    std::memcpy(&segment[1], head.data(), head.size());
    std::memcpy(&segment[kSeg - tail.size()], tail.data(), tail.size());
    return segment;
}

// 交错门：接缝回调（owner 线程）与辅助线程（真实数据锁）之间的握手状态。
struct Gate {
    std::shared_ptr<ConnectionState> state;
    bool expect_c{true};
    std::atomic<int> read_hook_calls{0};    // B 的 recv、C 的 recv 各一次
    std::atomic<int> busy_hook_calls{0};    // 每次 TryAppend 观测到 Busy 记一次
    std::atomic<bool> take_lock{false};
    std::atomic<bool> lock_held{false};
    std::atomic<bool> c_sent{false};
    std::atomic<bool> c_recv_seen{false};   // C 的 recv 已发生（append 紧随其后）
    std::atomic<bool> release{false};
    std::atomic<bool> released{false};
    std::atomic<bool> seam_observed{false};     // Busy 接缝首次在「仍持锁」时触发
    std::atomic<bool> aux_error{false};
    std::atomic<std::size_t> pending_observed{0};
};

// 接缝回调在 owner 线程执行：绝不抛异常（抛出会终止进程，破坏可判定性）。
//   #1 = B 的 recv 之后（既有 read-append 接缝）：让 aux 取得真实数据锁并确认持有，
//        这同时把 owner 挡在 append 之前，构成「此时才能发 C」的门。
//   #2 = C 的 recv 之后：置位 c_recv_seen，让主线程知道 append 即将发生。
std::function<void()> MakeReadAppendHook(const std::shared_ptr<Gate>& gate) {
    return [gate] {
        const int calls = gate->read_hook_calls.fetch_add(1);
        if (calls == 0) {
            gate->take_lock.store(true);
            if (!WaitUntil([&] { return gate->lock_held.load(); }, 6000ms)) {
                gate->aux_error.store(true);
            }
            return;
        }
        if (calls == 1) {
            gate->c_recv_seen.store(true);
        }
    };
}

// Busy 接缝：只在「TryAppend 观测 Busy、deferred 尚未提交」时被调用。
// 首次 = B；若 C 也 Busy，则第二次出现，说明本次没构造出「C 直接入 ring」的窗口。
std::function<void()> MakeAppendBusyHook(const std::shared_ptr<Gate>& gate) {
    return [gate] {
        const int calls = gate->busy_hook_calls.fetch_add(1);
        if (calls == 0) {
            if (gate->lock_held.load() && !gate->released.load()) {
                gate->seam_observed.store(true);
            } else {
                gate->aux_error.store(true);
            }
            if (gate->expect_c
                && !WaitUntil([&] { return gate->c_sent.load(); }, 6000ms)) {
                gate->aux_error.store(true);
            }
            gate->release.store(true);
            if (!WaitUntil([&] { return gate->released.load(); }, 6000ms)) {
                gate->aux_error.store(true);
            }
        }
    };
}

void AuxThread(std::shared_ptr<Gate> gate) {
    if (!WaitUntil([&] { return gate->take_lock.load(); }, 6000ms)) {
        gate->aux_error.store(true);
        return;
    }
    ReadAccess hold;
    const auto hold_deadline = std::chrono::steady_clock::now() + 6000ms;
    while (std::chrono::steady_clock::now() < hold_deadline) {
        hold = ConnectionTestAccess::HoldForTest(gate->state);
        if (hold) {
            break;
        }
        std::this_thread::yield();
    }
    if (!hold) {
        gate->aux_error.store(true);
        return;
    }
    gate->lock_held.store(true);
    (void)WaitUntil([&] { return gate->release.load(); }, 8000ms);
    ConnectionTestAccess::Reset(hold);
    gate->released.store(true);
    const auto pending_deadline = std::chrono::steady_clock::now() + 6000ms;
    while (std::chrono::steady_clock::now() < pending_deadline) {
        const std::size_t pending = gate->state->ring.PendingSize();
        if (pending != 0) {
            gate->pending_observed.store(pending);
            break;
        }
        std::this_thread::yield();
    }
}

void DrainOnce(Connection& connection, Engine& engine, std::string& out) {
    ReadAccess access = connection.TryAccess();
    if (access && access.Size() != 0) {
        (void)access.ConsumeInto(out);
        return;
    }
    EngineTestAccess::KickOwner(engine);
}

std::string ReadBounded(Connection& connection, Engine& engine, std::size_t expected,
                        std::chrono::milliseconds budget) {
    std::string out;
    const auto deadline = std::chrono::steady_clock::now() + budget;
    while (out.size() < expected && std::chrono::steady_clock::now() < deadline) {
        DrainOnce(connection, engine, out);
        if (out.size() < expected) {
            std::this_thread::yield();
        }
    }
    return out;
}

std::string BlockOrder(const std::string& out) {
    std::string order;
    for (std::size_t offset = 0; offset + kSeg <= out.size(); offset += kSeg) {
        if (!order.empty()) {
            order.push_back(',');
        }
        order.push_back(out[offset]);
    }
    return order;
}

std::size_t FirstDiff(const std::string& actual, const std::string& expected) {
    const std::size_t common = std::min(actual.size(), expected.size());
    std::size_t index = 0;
    while (index < common && actual[index] == expected[index]) {
        ++index;
    }
    return index;
}

// 一次完整交错：A 入 ring → B 走 deferred → C 单独 recv → 读回判定。
// 返回 true 表示 FIFO 成立；false 表示观测到 A+C+B 或其他不一致。
struct Outcome {
    bool fifo_ok{false};
    bool reordered{false};
    bool precondition_ok{true};
    bool window_observed{false};
    std::string out;
};

Outcome RunInterleave(bool send_c, bool tight = false) {
    Outcome result;
    StartRuntime(2);

    const std::string a = MakeSeg('A');
    const std::string b = MakeSeg('B');
    const std::string c = MakeSeg('C');
    const std::string abc = a + b + c;
    const std::string acb = a + c + b;

    auto handler = std::make_shared<StoreHandler>();
    Engine engine(handler);
    EngineRunner runner(engine, tight ? TightServerConfig(Loopback(0))
                                      : ServerConfig(Loopback(0)));
    BOOST_REQUIRE(WaitUntil([&] { return engine.Phase() == EnginePhase::Running; }, 3000ms));
    std::vector<Address> addresses;
    BOOST_REQUIRE(!engine.LocalAddresses(addresses));
    BOOST_REQUIRE(!addresses.empty());

    bool ok = false;
    NativeSocket client = ConnectClient(addresses[0], ok);
    BOOST_REQUIRE(ok);
    BOOST_REQUIRE(WaitUntil([&] { return handler->accepted.load() == 1; }, 3000ms));

    std::shared_ptr<Connection> conn;
    std::shared_ptr<ConnectionState> state;
    {
        std::lock_guard<std::mutex> lock(handler->mutex);
        BOOST_REQUIRE(handler->stored && handler->stored->Valid());
        conn = handler->stored;
    }
    state = ConnectionTestAccess::StateOf(*conn);
    BOOST_REQUIRE(state != nullptr);

    // 步骤 1：A 先入 ring（owner 已从内核取走 = ring.Size() 含 pending 达到 |A|）。
    BOOST_REQUIRE(SendAll(client, a, 3000ms));
    BOOST_REQUIRE(WaitUntil([&] { return state->ring.Size() == a.size(); }, 5000ms));

    // 步骤 2：武装两个接缝 + 辅助线程。
    auto gate = std::make_shared<Gate>();
    gate->state = state;
    gate->expect_c = send_c;
    EngineTestAccess::SetReadAppendHook(engine, MakeReadAppendHook(gate));
    EngineTestAccess::SetAppendBusyHook(engine, MakeAppendBusyHook(gate));
    std::thread aux(AuxThread, gate);

    // 步骤 3：此刻发 B → TryAppend 观测 Busy（aux 持真实数据锁）→ 接缝 #2 等待。
    BOOST_REQUIRE(SendAll(client, b, 3000ms));
    BOOST_REQUIRE(WaitUntil([&] { return gate->lock_held.load() || gate->aux_error.load(); }, 6000ms));
    BOOST_TEST(!gate->aux_error.load());

    // 步骤 4：B 已 recv、append 尚未发生；此刻发 C，使其落在后续独立 recv。
    if (send_c) {
        BOOST_REQUIRE(SendAll(client, c, 3000ms));
        gate->c_sent.store(true);
    }

    // 步骤 5：等 defer 落位。aux 释放后 AppendDeferred 成功，未修路径继续 recv。
    BOOST_REQUIRE(WaitUntil([&] { return gate->released.load(); }, 8000ms));
    (void)WaitUntil([&] { return gate->pending_observed.load() != 0; }, 6000ms);

    // 步骤 5b：观测「缺陷窗口是否出现」，用于解释结果，不作为执行前提。
    //   window_ok=1：C 在 pending 仍在时被 recv（旧行为下即 A+C+B 的成因）
    //   window_ok=0：owner 在 pending 存在时停止了 recv（修复后的正确行为）
    // 把 window_ok 当前置条件会把「已修好」误判成「前提未成立」——这是判据错误。
    const bool window_ok = WaitUntil([&] {
        return gate->read_hook_calls.load() >= 2
            && gate->busy_hook_calls.load() == 1
            && state->ring.PendingSize() == b.size();
    }, 6000ms);

    // 步骤 6：读回全部字节。
    const std::size_t expected_size = send_c ? abc.size() : (a.size() + b.size());
    result.out = ReadBounded(*conn, engine, expected_size, 8000ms);

    BOOST_TEST_MESSAGE("READ_HOOK_CALLS=" << gate->read_hook_calls.load()
                       << " BUSY_HOOK_CALLS=" << gate->busy_hook_calls.load()
                       << " SEAM_OBSERVED=" << (gate->seam_observed.load() ? 1 : 0)
                       << " WINDOW_OK=" << (window_ok ? 1 : 0)
                       << " PENDING_OBSERVED=" << gate->pending_observed.load());
    BOOST_TEST_MESSAGE("BLOCK_ORDER=" << BlockOrder(result.out)
                       << " ACTUAL_SIZE=" << result.out.size()
                       << " EXPECTED_SIZE=" << expected_size);

    EngineTestAccess::SetReadAppendHook(engine, std::function<void()>());
    EngineTestAccess::SetAppendBusyHook(engine, std::function<void()>());
    aux.join();

    // 前提只要求「真的造出了 Busy→deferred 交错」；是否出现窗口属于被测行为，
    // 由 fifo_ok/reordered 判定，不能拿来当前置。
    result.precondition_ok = gate->seam_observed.load() && !gate->aux_error.load();
    result.window_observed = window_ok;
    result.fifo_ok = (result.out == abc) || (!send_c && result.out == a + b);
    result.reordered = send_c && (result.out == acb);
    if (!result.fifo_ok && !result.reordered) {
        BOOST_TEST_MESSAGE("FIRST_BAD_OFFSET=" << FirstDiff(result.out, abc));
    }

    (void)client.Close();
    runner.CloseAndJoin();
    return result;
}

}  // namespace

BOOST_AUTO_TEST_SUITE(net_append_busy_hook)

// 核心用例：pending 存在期间继续 recv 的数据不得越过 pending。
// 未修产品上应观测到 A+C+B（本用例失败）；修复后必须 A+B+C。
BOOST_AUTO_TEST_CASE(pending_not_flushed_before_next_recv) {
    const Outcome outcome = RunInterleave(true);
    BOOST_REQUIRE_MESSAGE(outcome.precondition_ok, "交错前提未成立（接缝/持锁/释放/deferred 落位）");
    BOOST_TEST_MESSAGE("out == A+C+B ? " << (outcome.reordered ? "yes" : "no"));
    BOOST_CHECK_MESSAGE(!outcome.reordered,
                        "观测到 A+C+B：pending 未合入前的 recv 越过了它（FIFO 被破坏）");
    BOOST_CHECK(outcome.fifo_ok);
}

// 对照：B 走 deferred 但不继续 recv，读回必须是 A+B。
// 用来把变量隔离到「pending 未合入前的下一次 recv」，而不是「存在 pending」。
BOOST_AUTO_TEST_CASE(pending_without_next_recv_is_ordered) {
    const Outcome outcome = RunInterleave(false);
    BOOST_REQUIRE_MESSAGE(outcome.precondition_ok, "交错前提未成立");
    BOOST_CHECK(outcome.fifo_ok);
}

// 低水位边界：读方向总量界压到 49152（= A+B+C 总量），使 ReadLowWater() 恰为
// 49152。消费者读完全部数据后 ring.Size() 才降到低水位，是「暂停能否解除」的
// 最不利情况。若该场景下仍能读全 A+B+C，说明 m_read_paused 恢复路径有效，
// 不会因 pending 暂停而永久卡住读泵。
BOOST_AUTO_TEST_CASE(pending_pause_recovers_at_low_water) {
    const Outcome outcome = RunInterleave(true, /*tight=*/true);
    BOOST_REQUIRE_MESSAGE(outcome.precondition_ok, "交错前提未成立");
    BOOST_TEST_MESSAGE("BLOCK_ORDER=" << BlockOrder(outcome.out)
                       << " ACTUAL_SIZE=" << outcome.out.size());
    BOOST_CHECK_MESSAGE(outcome.fifo_ok,
                        "低水位边界下未读全全部字节：暂停恢复路径可能卡死读泵");
}

BOOST_AUTO_TEST_SUITE_END()

// 替代 Boost.Test 自动 main：保留 net 测试的 _Exit 退出边界。
bool init_unit_test() {
    return true;
}

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
    std::_Exit(final_code);   // 跳过静态析构/atexit 窗口
}
