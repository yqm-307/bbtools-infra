// EOF 自动收口 与 Engine::Stop 竞争 —— 两个确定性窗口（只加测试，不改产品）。
//
// 缺口来源：peer-eof-parent-119 的 limitations「EOF versus Engine Stop race not
// separately injected」。本文件只补这一条，不扩持续队列满/持续 OOM/UDP/Tick/CI。
//
// 窗口 A（eof_rejected_then_stop）：对端 FIN 真实到达，自动收口在该事件内的两次有界提交
//   都被本文件 wrap **注入为 Full**（注入，不是产品在队列非满时自行产生），故收口回到
//   Idle、责任留在 registry/session；此后测试不再发任何可达事件（否则 dispatcher 会按
//   契约重新进入收口入口，窗口就不再是 A），直接由控制线程 Engine::Stop() 收口
//   （0010:138「有后续可达事件即可收敛」的反面：此处无后续事件）。
// 窗口 B（eof_accepted_then_stop）：EOF 收口提交**已被真实接纳**，owner 域在唯一物理
//   关闭边界 StreamConnection::Close 执行中进入测试闸门；控制线程先确认「物理收口在
//   途」，再并发发起 Engine::Stop()，并等到 Stop 侧**真实封门收据**（
//   ConnectionBridge::SealService 的 __real 调用返回）之后才放行 owner。故本窗口验证的
//   是：胜者（EOF 收口）已在途时，Stop 的封门确实完成于放行之前，且不产生第二次物理
//   收口、不形成自等环。不主张二者同时抢首胜——首胜在 StreamConnection.cc:222-223 入口
//   Open→Closing 时即已判定，窗口 B 里 EOF 收口已先胜。
//
// 既有覆盖不重复：handler 挂起时的强停有界/残留 shell 安全见
// net.engine_stop_with_suspended_handler（engine_server_accept.cc）；停止后新操作
// 拒绝（Wake/晚写）见 net.co_bridge_close 与 net.peer_eof_idempotent_after_auto_close
// （co_bridge_peer_eof.cc 已在契约里断言）。本文件不断言 OnClose 恰一次
// （0010:140 只承诺 at-most-once，Stop 可压掉未开始的 OnClose），不等 handler/drain。
//
// 注入与计数（复用 tests/net 既有 linkwrap 范式，不新增生产 hook、不 sleep 猜阶段）：
//   - EventLoop::Submit(std::function<void()>)：按调用栈筛选只拦真实 EOF 收口提交
//     （同 co_bridge_peer_eof_full.cc:16-33 的 dladdr/strstr 筛选）；
//   - StreamConnection::Close(std::error_code)：唯一 fd 关闭边界（StreamConnection.cc
//     内该函数 state != Open 时早退、不关 fd）。因此分别记录「Close 入口调用次数」
//     与「真正执行了唯一 fd 关闭的次数」（进入时 state == Open）。幂等/重复 Close
//     调用不等于重复物理关闭，后者才用于判定重复收口缺陷。
//     **范围限定**：本计数只覆盖经 --wrap 的跨 TU 调用点（唯一真实调用点
//     ConnectionBridge.cc:186）。同 TU 的 StreamConnection.cc:110/117 与 socket_ RAII
//     兜底不经包装，故它是「被演练路径下恰好一次物理关闭」的证据，不是全进程无重复
//     fd close 的证明；内核侧真实收口另有客户端 poll + Receive.eof 独立支撑。
//   - ConnectionBridge::SealService()：Stop 侧封门边界（定义在 ConnectionBridge.cc，
//     被 ServerEngine.cc:565 跨 TU 调用 ⇒ 可 wrap）。**只在窗口 B 开启收据**：__real
//     调用返回后才 signal，控制线程等到该收据才放行 owner。故窗口 B 的放行前收据是
//     真实 Stop 侧封门完成，而非测试自设标志（stop_issued 仅是测试侧标记，不作为证据）。
//
// case：eof_rejected_then_stop / eof_accepted_then_stop

#define main CoBridgeCloseRaceFixtureMain
#include "co_bridge_contract.cc"
#undef main

#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/detail/StreamConnection.hpp>

#include <cstring>
#include <dlfcn.h>
#include <execinfo.h>
#include <fstream>

namespace {

// ---- 注入阶段（只作用于真实 EOF 收口 Submit 调用栈）-----------------------
std::atomic<int> full_remaining{0};
std::atomic<int> eof_submit_rejected{0};
std::atomic<int> eof_submit_accepted{0};
std::atomic<int> stack_match_seen{0};
std::promise<void> rejected_stage;

// ---- 物理收口边界计数与窗口 B 闸门 ---------------------------------------
std::atomic<int> close_entered{0};
std::atomic<int> physical_close_actual{0};
std::atomic<bool> close_in_flight{false};
std::atomic<bool> gate_armed{false};
std::atomic<bool> gate_released{false};
std::atomic<bool> stop_issued{false};   // 测试侧标记（不作为 Stop 侧证据）
std::promise<void> close_entered_signal;
std::promise<void> gate_release_signal;

// ---- 窗口 B 的 Stop 侧真实收据（ConnectionBridge::SealService）------------
std::atomic<int> seal_calls{0};
std::atomic<int> real_stop_seal_completed{0};
std::atomic<bool> seal_observe_armed{false};
std::promise<void> seal_completed_signal;

bool IsPeerCloseSubmit() {
    void* frames[24];
    const int count = ::backtrace(frames, 24);
    for (int i = 0; i < count; ++i) {
        Dl_info info{};
        if (::dladdr(frames[i], &info) && info.dli_sname
            && std::strstr(info.dli_sname, "ConnectionSession19CloseOnPeerTerminalEv")) {
            return true;
        }
    }
    return false;
}

// 控制线程只读观测进程资源（与 engine_server_accept.cc 同口径，不靠 sleep 推断）。
int ThreadCount() {
    std::ifstream status("/proc/self/status");
    CHECK(status.good());
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("Threads:", 0) == 0) { return std::atoi(line.c_str() + sizeof("Threads:") - 1); }
    }
    CHECK(false);
    return -1;
}

struct RaceHandler final : EventHandler {
    ICoConnection* connection{nullptr}; // 仅 Engine 存活期内使用（Stop 后 registry 释放）
    std::promise<void> opened;
    std::promise<void> closed;
    std::string received;
    std::error_code reason;
    std::atomic<int> closes{0};

    Action OnOpen(ICoConnection& value) override {
        connection = &value;
        opened.set_value();
        return Action::None;
    }
    Action OnTraffic(ICoConnection& value, bool wake) override {
        if (wake) { return Action::None; }
        const auto view = value.Peek();
        if (!view.empty()) {
            received.append(view.data(), view.size());
            (void)value.Discard(view.size());
        }
        return Action::None;
    }
    void OnClose(ICoConnection&, const std::error_code& value) override {
        reason = value;
        if (++closes == 1) { closed.set_value(); } // at-most-once：不假设恰一次
    }
};
} // namespace

extern "C" SubmitStatus RealRaceSubmit(EventLoop*, std::function<void()>)
    asm("__real__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEE");
extern "C" SubmitStatus WrapRaceSubmit(EventLoop*, std::function<void()>)
    asm("__wrap__ZN3bbt5infra3net9EventLoop6SubmitESt8functionIFvvEE");
extern "C" SubmitStatus WrapRaceSubmit(EventLoop* loop, std::function<void()> command) {
    const bool peer = IsPeerCloseSubmit();
    if (peer) { ++stack_match_seen; }
    if (peer && full_remaining.load() > 0) {
        const int left = full_remaining.fetch_sub(1) - 1;
        ++eof_submit_rejected;
        if (left == 0) { rejected_stage.set_value(); }
        return SubmitStatus::Full; // 未接纳：不执行闭包，责任留在 registry/session
    }
    const auto status = RealRaceSubmit(loop, std::move(command));
    if (peer && status == SubmitStatus::Accepted) { ++eof_submit_accepted; }
    return status;
}

extern "C" std::error_code RealRaceClose(StreamConnection*, std::error_code)
    asm("__real__ZN3bbt5infra3net16StreamConnection5CloseESt10error_code");
extern "C" std::error_code WrapRaceClose(StreamConnection*, std::error_code)
    asm("__wrap__ZN3bbt5infra3net16StreamConnection5CloseESt10error_code");
extern "C" std::error_code WrapRaceClose(StreamConnection* connection, std::error_code reason) {
    ++close_entered;
    const bool will_close_fd = (connection->ConnectionState() == StreamConnection::State::Open);
    if (will_close_fd) {
        ++physical_close_actual; // 真正拥有 fd 的唯一物理关闭（owner 域，非幂等入口调用）
        if (gate_armed.exchange(false)) {
            close_in_flight.store(true);
            close_entered_signal.set_value();
            // 有界等待测试放行；绝不等 Stop future（否则自造死锁）。
            auto release = gate_release_signal.get_future();
            if (release.wait_for(std::chrono::seconds(20)) != std::future_status::ready) {
                std::cerr << "close gate not released within 20s" << std::endl;
                std::abort();
            }
            close_in_flight.store(false);
        }
    }
    return RealRaceClose(connection, std::move(reason));
}

// Stop 侧封门收据：ServerEngine::StopLocked（ServerEngine.cc:565，控制线程）调用
// ConnectionBridge::SealService，定义在另一个 TU ⇒ 跨 TU 引用可被 --wrap 捕获。
// 先执行真实封门，返回后才记录收据，故收据等价于「封门已完成」。
extern "C" void RealRaceSealService(ConnectionBridge*) asm("__real__ZN3bbt5infra3net16ConnectionBridge11SealServiceEv");
extern "C" void WrapRaceSealService(ConnectionBridge*) asm("__wrap__ZN3bbt5infra3net16ConnectionBridge11SealServiceEv");
extern "C" void WrapRaceSealService(ConnectionBridge* bridge) {
    RealRaceSealService(bridge);
    ++seal_calls;
    if (seal_observe_armed.exchange(false)) { // 只在窗口 B 记录，且只记首次
        ++real_stop_seal_completed;
        seal_completed_signal.set_value();
    }
}

// ===========================================================================
// 两个窗口共用的场景装配 + 各自的控制/收据，收口断言共用。
// ===========================================================================
static void RunRaceBody(bool accepted_window) {
    WarmRuntime();
    const auto fd_baseline = OpenFdCount();
    const int fds_before = static_cast<int>(fd_baseline);
    const int threads_before = ThreadCount();

    full_remaining = accepted_window ? 0 : 2;
    gate_armed = accepted_window;
    seal_observe_armed = accepted_window; // 收据只在窗口 B 启用（窗口 A 的 Stop 封门不计收据）
    auto handler = std::make_shared<RaceHandler>();
    std::error_code error;
    const auto address = Address::Ip("127.0.0.1", 0, error);
    CHECK(!error);
    Options options;
    options.num_event_loop = 1;

    {
        Engine engine(handler);
        CHECK(!engine.Run(address, options));
        std::vector<Address> addresses;
        CHECK(!engine.LocalAddresses(addresses));
        CHECK(addresses.size() == 1);
        auto client = ConnectTo(addresses.front());
        Get(handler->opened.get_future());
        CHECK(engine.CountConnections() == 1);

        CHECK(ClientSend(client, "tail"));
        CHECK(!client.Shutdown(SHUT_WR)); // 尾帧 + FIN：触发真实 EOF 自动收口入口

        if (!accepted_window) {
            // ---- 窗口 A 控制：两次真实 EOF 收口 Submit 均 Full（未接纳）----
            Get(rejected_stage.get_future());
            CHECK(eof_submit_rejected == 2);
            CHECK(eof_submit_accepted == 0);
            CHECK(stack_match_seen > 0); // 筛选命中，防 mangled 漂移导致注入空转
            CHECK(physical_close_actual == 0); // Stop 前没有物理收口
            const auto start = Clock::now();
            CHECK(!engine.Stop()); // 直接收口，不等 handler/drain
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                Clock::now() - start).count();
            CHECK(elapsed < 5000);
            CHECK(eof_submit_accepted == 0); // Stop 路径未重新进入 EOF 收口提交
            CHECK(seal_calls > 0); // Stop 链确实经过封门边界（注入点非空转，封门收据可被观测）
            Log("close_race_A", 0, "rejected=2 accepted=0 stop_ms=" + std::to_string(elapsed)
                + " seal_calls=" + std::to_string(seal_calls.load()));
        } else {
            // ---- 窗口 B 收据：EOF 收口已接纳 + owner 物理收口在途 + Stop 侧封门完成 ----
            auto entered = close_entered_signal.get_future();
            CHECK(entered.wait_for(std::chrono::seconds(10)) == std::future_status::ready);
            CHECK(eof_submit_accepted == 1);
            CHECK(physical_close_actual == 1);
            CHECK(close_in_flight.load());
            CHECK(real_stop_seal_completed == 0); // 封门收据只可能来自本轮 Stop 链
            stop_issued.store(true);
            auto stop_future = std::async(std::launch::async, [&engine] { return engine.Stop(); });
            // 等 Stop 侧真实封门收据（ConnectionBridge::SealService 的 __real 已返回）。
            // 有界等待；无论就绪或超时，都在任何断言之前无条件放行 owner，故断言失败/超时
            // 也不会把 owner 困在闸门里（否则 std::async future 析构会自等）。
            auto seal_done = seal_completed_signal.get_future();
            const bool seal_ready = (seal_done.wait_for(std::chrono::seconds(15))
                == std::future_status::ready);
            const bool stop_started = stop_issued.load();
            const bool close_still_in_flight = close_in_flight.load();
            const bool seal_ran = real_stop_seal_completed.load() > 0;
            const int seal_count_at_release = seal_calls.load();
            const auto release_at = Clock::now();
            gate_released.store(true);
            gate_release_signal.set_value(); // 无条件放行
            CHECK(stop_started);             // 测试侧标记：Stop 已发起
            CHECK(close_still_in_flight);    // 放行当刻 owner 仍在物理收口中（收口未先于 Stop 结束）
            CHECK(seal_ready);               // Stop 侧封门收据在放行前到达
            CHECK(seal_ran);                 // 且真实封门 __real 已调用返回
            CHECK(seal_count_at_release > 0);
            CHECK(stop_future.wait_for(std::chrono::seconds(15)) == std::future_status::ready);
            CHECK(!stop_future.get());
            const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                Clock::now() - release_at).count();
            CHECK(elapsed < 8000); // 放行后仍在有界量级：Stop 与 EOF 收口无自等环
            Log("close_race_B", 0, "accepted=1 stop_ms=" + std::to_string(elapsed)
                + " seal_before_release=1 seal_calls=" + std::to_string(seal_count_at_release));
        }

        // ---- 两窗口共用的真实收口断言 ----
        CHECK(eof_submit_accepted == (accepted_window ? 1 : 0));
        CHECK(stack_match_seen > 0);
        CHECK(physical_close_actual == 1); // 唯一物理关闭；>=2 即重复收口缺陷签名
        CHECK(handler->closes <= 1);       // at-most-once（0010:140），禁止 ==1
        if (accepted_window && handler->closes == 1) {
            CHECK(!handler->reason); // 竞争前已授权的干净 FIN 首胜，不被 Stop 改写成 canceled
        }
        // CountConnections()==0 只是 Engine::Stop 的公共停止语义（ServerEngine.cc:277-281
        // 由 subs_.empty() 表达），**不代表 OnClose 已执行完毕**；本窗口不对 OnClose 计数
        // 作完成断言（0010:140 只承诺 at-most-once）。
        CHECK(engine.CountConnections() == 0);

        // 客户端侧真实 TCP 收口（内核侧 fd 已关），不只回调计数。
        pollfd pfd{client.Fd(), POLLIN | POLLHUP, 0};
        CHECK(::poll(&pfd, 1, 5000) > 0);
        char byte{0};
        CHECK(client.Receive(&byte, 1).eof);

        // 关闭后引擎级操作拒绝（不触碰 Stop 后已释放的 shell 引用；连接级 Wake/晚写
        // 拒绝已由 net.co_bridge_close / engine_server_accept 覆盖）。
        CHECK(!engine.Stop()); // 已停止幂等成功
        std::vector<Address> after;
        CHECK(engine.LocalAddresses(after) == std::make_error_code(std::errc::operation_not_permitted));
        CHECK(engine.Run(address, options) == std::make_error_code(std::errc::operation_canceled));

        client.Close();
    }

    // 最后引用释放后 fd / thread 回落（有界等待，不 sleep 推断阶段）。
    CHECK(OpenFdCount() == fd_baseline);
    const auto deadline = Until(10s);
    while (ThreadCount() != threads_before && Clock::now() < deadline) {
        std::this_thread::sleep_for(5ms);
    }
    CHECK(ThreadCount() == threads_before);
    Log("close_race_fd", 0, "fd_baseline=" + std::to_string(fds_before)
        + " restored=1 threads_restored=1");
}

static int RunCloseRaceCases(int argc, char** argv) {
    CHECK(argc == 2);
    const std::string case_name = argv[1];
    if (case_name == "eof_rejected_then_stop") { RunRaceBody(false); }
    else if (case_name == "eof_accepted_then_stop") { RunRaceBody(true); }
    else { std::cerr << "unknown case: " << case_name << '\n'; return 2; }
    std::cout << "CASE OK co_bridge_close_race " << case_name
              << " asserts=" << AssertCounter().load() << '\n';
    return 0;
}

int main(int argc, char** argv) {
    return bbt_net_test::RunTestProcessExit([&] { return RunCloseRaceCases(argc, argv); });
}
