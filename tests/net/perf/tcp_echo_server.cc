// infra-perf 闭环：bbt::infra_net TCP echo 基准服务端（raw / co 双模式，进程级）。
//
// 进程契约（与 scripts/bench/driver.py 对齐）：
//   argv  --mode=raw|co [--port=PORT|0] [--runtime-workers=N] [--subs=N]
//         [--accept-cap=N] [--read-timeout-ms=N]
//   就绪  stdout 输出单行 "PORT=<实际端口>" 后进入服务；就绪行之外的诊断全部走
//         stderr（不产生 per-request stdout）。
//   stdin "stat"      → 回一行 {"role":"server_stat",...}（当前累计指标 +
//         active_conns；测量窗边界快照用，无重置语义）
//   stdin "fin_overhead" → 回一行 {"role":"fin_overhead","accepted":N,
//         "echoed_fin":N}（fin 帧对账；driver 在 conns 稳态后查询，须
//         echoed_fin==accepted）
//   结束  stdin EOF / 首行 "quit" → g_stop、engine.RequestClose、join engine
//         线程、收 accept/连接协程（各自带界等待）、输出单行 JSON 指标到 stdout
//         后退出。退出码非零当 errors/frames_bad>0、engine 收口报错、或收口后
//         仍有未完成 accept/conn 任务/未释放连接（teardown 不全 = 真实失败）。
//   协议  共享 perf_common.hpp 流式帧（BBT0 magic + kind + conn + seq + stamp_ns
//         + len + payload）。'F' fin：echo 回后对该连接 RequestClose（单向 transfer
//         负载的收尾信号）。每 data 帧 echo 前做确定性内容 + 每连接 seq 单调校验
//         （seq 必须从 0 起逐帧 +1；重复/缺失/乱序一律 frames_bad + RequestClose）。
//
// 关闭契约（产品）：对端 FIN 只是 MarkEof + OnRead——EOF 不关闭写侧，连接的
// 最终释放必须由 handler 端 RequestClose 发起。因此 raw OnRead 见到 Eof/Error
// 读终态时，先把 ring 中剩余数据全部拆帧并入 pending，再按以下规则收尾：
//   pending 仍有待发 echo → 登记进 eof_pending 等 OnWrite 把队列排空后
//   RequestClose（不丢未发 echo）；pending 已空 → 立即 RequestClose。
// co 模式 Read 返回 Eof/Closed 前其顺序 Write 已终态，直接 RequestClose。
//
// 进程退出：coroutine runtime 无 Stop/静态析构收口（已知限制，见
// tests/net/ProcessRuntimeTestMain.hpp）；本进程在 engine join + 自有任务全部
// 归零验证后走 RunTestProcessExit(std::_Exit)，不依赖 runtime 静态析构排水。
// 收口后 active_conns!=0 / 任务超时退出都是非零——不允许在 callback 仍引用
// 栈对象时退出，也不把连接泄漏当 teardown 成功。
//
// 资源/口径：port=0 → 内核分配动态端口（本进程独有）；runtime workers 与 engine
// subs 显式参数化；进程级 CPU/RSS 经 /proc/self 采样（proc_ok=false 时测量无效
// 如实上报，不拿 0 冒充）；指标含 accepted/completed/bytes_in/bytes_out/errors/
// frames_bad/seq_bad、服务端 cpu_per_op、RSS max/end 采样（显式标注 max）。
// 样本与内存有界：不存 per-request 数据，无逐帧 stdout；conn 任务用活动计数器
// （condvar）记账，不积累历史 future 容器。

#include <bbt/infra/net/ConnectionResult.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/net/RawHandler.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <deque>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <bbt/coroutine/coroutine.hpp>

#include "../ProcessRuntimeTestMain.hpp"
#include "perf_common.hpp"

using namespace bbt::infra::net;
using namespace bbt::infra::net_perf;
using namespace std::chrono_literals;

namespace {

std::atomic<bool> g_stop{false};
int g_read_timeout_ms = 200;

struct Metrics {
    std::atomic<std::uint64_t> accepted{0};
    std::atomic<std::uint64_t> completed{0};     // 成功 echo 的 data 帧数
    std::atomic<std::uint64_t> echoed_fin{0};    // 已回显的 fin 帧数（fin 对账）
    std::atomic<std::uint64_t> bytes_in{0};      // 已消费协议字节（含头）
    std::atomic<std::uint64_t> bytes_out{0};
    std::atomic<std::uint64_t> errors{0};        // 写失败/连接级错误
    std::atomic<std::uint64_t> frames_bad{0};    // magic/长度/内容校验失败
    std::atomic<std::uint64_t> seq_bad{0};       // 帧序重复/缺失/乱序
};
Metrics g_metrics;
std::atomic<std::uint64_t> g_write_closed{0};  // 对端先关导致的良性 echo 写失败

// 已接受未释放连接的活动记账（teardown 判据；容器有界=活动数，不积累历史）。
// co 模式同一计数器同时覆盖 accept 任务与 conn 协程：收口后 pending!=0 即失败。
std::mutex g_active_mu;
std::condition_variable g_active_cv;
std::int64_t g_active_conns = 0;
std::int64_t g_pending_tasks = 0;  // 已提交未完成的协程任务（accept+conn）

void TrackTaskBegin() {
    std::lock_guard<std::mutex> lk(g_active_mu);
    ++g_pending_tasks;
}
void TrackTaskEnd() {
    std::lock_guard<std::mutex> lk(g_active_mu);
    --g_pending_tasks;
    g_active_cv.notify_all();
}

// 等待所有协程任务归零（deadline 有界）；false=超时仍有未完成任务。
bool WaitTasksDone(std::chrono::steady_clock::time_point deadline) {
    std::unique_lock<std::mutex> lk(g_active_mu);
    return g_active_cv.wait_until(lk, deadline,
                                  [] { return g_pending_tasks == 0; });
}
std::int64_t PendingTasks() {
    std::lock_guard<std::mutex> lk(g_active_mu);
    return g_pending_tasks;
}
std::int64_t ActiveConns() {
    std::lock_guard<std::mutex> lk(g_active_mu);
    return g_active_conns;
}

void EmitStatsLine() {
    const auto proc = ReadProcSelf();
    Json j;
    j.K("role").V("server_stat")
     .K("accepted").V(static_cast<std::uint64_t>(g_metrics.accepted.load()))
     .K("completed").V(static_cast<std::uint64_t>(g_metrics.completed.load()))
     .K("echoed_fin").V(static_cast<std::uint64_t>(g_metrics.echoed_fin.load()))
     .K("bytes_in").V(static_cast<std::uint64_t>(g_metrics.bytes_in.load()))
     .K("bytes_out").V(static_cast<std::uint64_t>(g_metrics.bytes_out.load()))
     .K("errors").V(static_cast<std::uint64_t>(g_metrics.errors.load()))
     .K("frames_bad").V(static_cast<std::uint64_t>(g_metrics.frames_bad.load()))
     .K("seq_bad").V(static_cast<std::uint64_t>(g_metrics.seq_bad.load()))
     .K("write_closed").V(static_cast<std::uint64_t>(g_write_closed.load()))
     .K("active_conns").V(ActiveConns())
     .K("proc_ok").V(proc.ok)
     .K("cpu_ticks").V(proc.utime_ticks + proc.stime_ticks)
     .K("ticks_per_second").V(static_cast<std::uint64_t>(TicksPerSecond()))
     .K("rss_bytes").V(proc.rss_pages * static_cast<std::uint64_t>(PageBytes()));
    std::cout << j.Done() << '\n';
    std::cout.flush();
}

void EmitFinOverheadLine() {
    Json j;
    j.K("role").V("fin_overhead")
     .K("accepted").V(static_cast<std::uint64_t>(g_metrics.accepted.load()))
     .K("echoed_fin").V(static_cast<std::uint64_t>(g_metrics.echoed_fin.load()))
     .K("active_conns").V(ActiveConns());
    std::cout << j.Done() << '\n';
    std::cout.flush();
}

// 把 ring 中剩余数据全部拆帧进 slot 的工作由 DrainToPending 承担
// （EOF 路径必须先排空可读字节再谈关闭，否则已入 ring 的尾帧连同其 echo
// 一起被丢）。

// ---------------- raw 模式：RawHandler 短回调 + 每连接发送队列 ----------------
// echo 语义要求不丢帧：帧统一先入本连接 pending（FIFO），再尽力 AsyncWrite 补发；
// 发送侧容量满（no_buffer_space）留队，待 OnWrite/OnRead 推进补投。对端先关
// （Closed/Eof/WriteSettling）丢弃队列记 write_closed，不算 errors。
struct RawEchoHandler : RawHandler {
    struct Slot {
        std::mutex mu;
        FrameParser parser;
        std::deque<std::string> pending;          // 待 echo 的完整帧（含头），FIFO
        std::shared_ptr<ConnectionState> state;   // 身份：防 Id 复用后误删后继 slot
        std::uint64_t expected_seq{0};            // 下一帧期望 seq（帧序完整性）
        bool eof{false};                          // 对端 FIN（读终态）已见；
                                                // pending 排空后须 RequestClose
        bool close_requested{false};              // 已发起 RequestClose（幂等去重）
    };
    std::mutex mu;
    std::map<std::uint64_t, std::shared_ptr<Slot>> parsers;
    // EOF 但 pending 未排空的连接：OnWrite 推进补投，排空即 RequestClose。
    // key=conn Id，值=slot；OnClose 时按 state 匹配删（同 OnClose 的 Id 复用防御）。
    std::map<std::uint64_t, std::shared_ptr<Slot>> eof_pending;

    void OnAccept(Connection connection) override {
        g_metrics.accepted.fetch_add(1);
        {
            std::lock_guard<std::mutex> lk(g_active_mu);
            ++g_active_conns;
        }
        auto slot = std::make_shared<Slot>();
        // 身份令牌：公共面已删除 Connection::State()，经测试接缝取同一
        // shared_ptr 身份防 Id 复用后误删后继 slot（语义不变）。
        slot->state = ConnectionTestAccess::StateOf(connection);
        std::lock_guard<std::mutex> lk(mu);
        // Id 可复用：同 id 旧连接的 slot 可能仍在表（其 OnClose 未到达）——
        // 直接覆盖是对的，旧连接的数据面已终态；旧 OnClose 到达时按 state 识别。
        parsers[connection.Id()] = std::move(slot);
    }

    // 尽力发 pending 头部（FIFO，遇容量满停）。false=对端终态/真实写错（调用方
    // 收尾 RequestClose）。调用方持 slot->mu；用 front() 引用发送避免移动丢失。
    bool FlushPending(Connection& connection, Slot& slot) {
        while (!slot.pending.empty()) {
            WriteOperation op;
            const auto err = connection.AsyncWrite(slot.pending.front(), op);
            if (!err) {
                g_metrics.bytes_out.fetch_add(slot.pending.front().size());
                slot.pending.pop_front();
                continue;
            }
            if (err == std::make_error_code(std::errc::no_buffer_space)) {
                return true;  // 仍满：留给下一次 OnWrite/OnRead 补投
            }
            if (err == make_error_code(NetErrc::Closed) ||
                err == make_error_code(NetErrc::Eof) ||
                err == make_error_code(NetErrc::WriteSettling)) {
                g_write_closed.fetch_add(
                    static_cast<std::uint64_t>(slot.pending.size()));
                slot.pending.clear();
                return false;
            }
            g_metrics.errors.fetch_add(1);
            slot.pending.clear();
            return false;
        }
        return true;
    }

    // EOF 收尾：读终态后 pending 已空（或 FlushPending 排空）→ RequestClose；
    // 仍有待发 → 登记 eof_pending 由 OnWrite 排空后关。调用方持 slot->mu。
    void CloseOrDefer(Connection& connection, std::shared_ptr<Slot>& slot) {
        if (!slot->pending.empty()) {
            std::lock_guard<std::mutex> lk(mu);
            eof_pending[connection.Id()] = slot;
            return;
        }
        if (!slot->close_requested) {
            slot->close_requested = true;
            connection.RequestClose();
        }
    }

    // 喂一段字节流并把所有完整帧入 pending；false=协议错（frames_bad/seq_bad
    // 已计数），调用方 RequestClose。调用方持 slot->mu。
    bool DrainToPending(Slot& slot,
                        const char* data, std::size_t size) {
        if (!slot.parser.Feed(data, size)) {
            g_metrics.frames_bad.fetch_add(1);
            g_metrics.errors.fetch_add(1);
            return false;
        }
        while (slot.parser.HasFrame()) {
            const std::string frame = slot.parser.TakeFrame();
            const auto kind = slot.parser.kind();
            // 帧序完整性：seq 必须逐帧 +1（重复/缺失/乱序即拒——transfer/conns
            // 与 drain 路径同口径，不只 pingpong 校验）。
            if (slot.parser.seq() != slot.expected_seq) {
                g_metrics.seq_bad.fetch_add(1);
                return false;
            }
            ++slot.expected_seq;
            if (kind == kKindData && !slot.parser.VerifyCurrent(frame)) {
                g_metrics.frames_bad.fetch_add(1);
                g_metrics.errors.fetch_add(1);
                return false;
            }
            slot.pending.push_back(std::move(frame));
            if (kind == kKindData) g_metrics.completed.fetch_add(1);
            else if (kind == kKindFin) g_metrics.echoed_fin.fetch_add(1);
        }
        return true;
    }

    void OnRead(Connection connection) override {
        std::shared_ptr<Slot> slot;
        {
            std::lock_guard<std::mutex> lk(mu);
            const auto it = parsers.find(connection.Id());
            if (it == parsers.end()) return;
            slot = it->second;
        }
        std::lock_guard<std::mutex> data_lk(slot->mu);
        bool terminal = false;
        for (;;) {
            std::string data;
            const auto result = connection.TryRead(data);
            if (result.status == ReadStatus::Ok) {
                if (data.empty()) break;  // Ok 但无数据：本批已尽
                g_metrics.bytes_in.fetch_add(data.size());
                if (!DrainToPending(*slot, data.data(), data.size())) {
                    slot->close_requested = true;
                    connection.RequestClose();
                    return;
                }
                continue;
            }
            if (result.status == ReadStatus::Busy ||
                result.status == ReadStatus::WouldBlock) {
                // Busy：他线程正在读，留待其释放触发的重投 OnRead；
                // WouldBlock：本批可读数据已尽且无终态——正常中途，不关。
                break;
            }
            if (result.status == ReadStatus::Error) {
                g_metrics.errors.fetch_add(1);
            }
            terminal = true;  // Eof / Error 终态
            break;
        }
        if (terminal) slot->eof = true;
        // 尽力补投；剩余容量不足时留队，由 OnWrite/下次 OnRead 继续。
        if (!FlushPending(connection, *slot)) {
            slot->close_requested = true;
            connection.RequestClose();
            return;
        }
        // 产品契约：EOF 不关闭写侧——应用层见对端 FIN 必须自己 RequestClose，
        // 否则连接状态永不释放。pending 未空先登记，OnWrite 排空后关。
        if (slot->eof && !slot->close_requested) CloseOrDefer(connection, slot);
    }

    void OnWrite(Connection connection, WriteResult) override {
        // 写完成/容量释放：补投 pending echo；EOF 连接排空后即 RequestClose。
        std::shared_ptr<Slot> slot;
        {
            std::lock_guard<std::mutex> lk(mu);
            const auto it = parsers.find(connection.Id());
            if (it == parsers.end()) return;
            slot = it->second;
        }
        std::lock_guard<std::mutex> data_lk(slot->mu);
        if (!FlushPending(connection, *slot)) {
            slot->close_requested = true;
            connection.RequestClose();
            return;
        }
        if (slot->eof && slot->pending.empty() && !slot->close_requested) {
            slot->close_requested = true;
            {
                std::lock_guard<std::mutex> lk(mu);
                eof_pending.erase(connection.Id());
            }
            connection.RequestClose();
        }
    }

    void OnClose(Connection connection, std::error_code) override {
        std::lock_guard<std::mutex> lk(mu);
        const auto it = parsers.find(connection.Id());
        // Id 复用：迟到的 OnClose 只删属于自己 state 的 slot——同 id 已有新连接
        // 登记时，表中 slot 属于新连接，不得擦除（否则新连接 OnRead 提前返回，
        // echo 永远不到 → 对端 3s 超时，conns 负载尾部稳定丢 clients 个连接）。
        if (it != parsers.end() &&
            it->second->state == ConnectionTestAccess::StateOf(connection)) {
            // 丢弃未发出的 echo（对端已走）；如实计数不静默。
            const auto dropped = it->second->pending.size();
            if (dropped > 0) {
                g_write_closed.fetch_add(static_cast<std::uint64_t>(dropped));
            }
            parsers.erase(it);
        }
        const auto eit = eof_pending.find(connection.Id());
        if (eit != eof_pending.end() &&
            eit->second->state == ConnectionTestAccess::StateOf(connection)) {
            eof_pending.erase(eit);
        }
        {
            std::lock_guard<std::mutex> alk(g_active_mu);
            --g_active_conns;
        }
        g_active_cv.notify_all();
    }
};

// ---------------- co 模式：协程 echo 循环（read-timeout 周期复查 g_stop） ----------------
void CoServeConnection(Connection connection) {
    {
        std::lock_guard<std::mutex> lk(g_active_mu);
        ++g_active_conns;
    }
    struct ConnGuard {
        ~ConnGuard() {
            std::lock_guard<std::mutex> lk(g_active_mu);
            --g_active_conns;
            g_active_cv.notify_all();
        }
    } guard;
    FrameParser parser;
    std::uint64_t expected_seq = 0;
    CoTcpConnection co_conn{connection};
    for (;;) {
        if (g_stop.load()) { connection.RequestClose(); return; }
        std::string data;
        bbt::coroutine::WaitOptions opts;
        opts.deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(g_read_timeout_ms);
        const auto read = co_conn.Read(opts);
        if (read.state == ReadState::Error &&
            read.error == std::make_error_code(std::errc::timed_out)) {
            continue;
        }
        if (read.state == ReadState::Eof ||
            (read.state == ReadState::Error &&
             read.error == make_error_code(NetErrc::Closed))) {
            // 产品契约：EOF 不关闭写侧——本协程的顺序 Write 在读到 EOF 前
            // 已全部终态，此处 RequestClose 是连接释放的唯一发起方。
            connection.RequestClose();
            return;
        }
        if (read.state == ReadState::Error) {
            g_metrics.errors.fetch_add(1);
            connection.RequestClose();
            return;
        }
        read.data.AppendTo(data);
        g_metrics.bytes_in.fetch_add(data.size());
        if (!parser.Feed(data.data(), data.size())) {
            g_metrics.frames_bad.fetch_add(1);
            g_metrics.errors.fetch_add(1);
            connection.RequestClose();
            return;
        }
        bool close_after = false;
        while (parser.HasFrame()) {
            const std::string frame = parser.TakeFrame();
            // 帧序完整性：seq 逐帧 +1（重复/缺失/乱序即拒）。
            if (parser.seq() != expected_seq) {
                g_metrics.seq_bad.fetch_add(1);
                connection.RequestClose();
                return;
            }
            ++expected_seq;
            if (parser.kind() == kKindData && !parser.VerifyCurrent(frame)) {
                g_metrics.frames_bad.fetch_add(1);
                g_metrics.errors.fetch_add(1);
                connection.RequestClose();
                return;
            }
            bbt::coroutine::WaitOptions wopts;
            wopts.deadline = std::chrono::steady_clock::now() + 5000ms;
            const auto report = co_conn.Write(frame, wopts);
            // 有效写错误：等待失败取 wait_error；等待完成但发送未确认取
            // outcome.error（与旧 CoWrite err+outcome 终态同口径）。
            const auto werr = report.wait_error
                ? report.wait_error : report.outcome.error;
            if (werr) {
                if (werr == make_error_code(NetErrc::Closed) ||
                    werr == make_error_code(NetErrc::Eof)) {
                    ++g_write_closed;
                    connection.RequestClose();
                    return;  // 对端先关：良性拆除，不计 errors
                }
                g_metrics.errors.fetch_add(1);
                connection.RequestClose();
                return;
            }
            g_metrics.bytes_out.fetch_add(frame.size());
            if (parser.kind() == kKindFin) {
                g_metrics.echoed_fin.fetch_add(1);
                close_after = true;
                break;
            }
            g_metrics.completed.fetch_add(1);
        }
        // fin 的 echo 已被 Write 确认后 RequestClose：单向 transfer 的收尾。
        // （上一轮 Read 的对端 EOF 路径同样显式关——EOF 不触发写侧关闭。）
        if (close_after) {
            connection.RequestClose();
            return;
        }
    }
}

int RunServer(int argc, char** argv) {
    const Args args = ParseArgs(argc, argv,
        /*required*/{"mode"},
        /*optional*/{"port", "runtime-workers", "subs", "accept-cap", "read-timeout-ms"});
    if (!args.ok) {
        std::cerr << "args error: " << args.error << '\n';
        return 2;
    }
    const std::string mode = ArgStr(args, "mode");
    const int port = static_cast<int>(ArgU64(args, "port", 0));
    std::uint64_t workers = ArgU64(args, "runtime-workers", 0);
    const std::uint64_t subs = ArgU64(args, "subs", 0);
    const std::uint64_t accept_cap = ArgU64(args, "accept-cap", 0);
    g_read_timeout_ms = static_cast<int>(ArgU64(args, "read-timeout-ms", 200));
    if (mode != "raw" && mode != "co") {
        std::cerr << "args error: --mode 必须是 raw|co\n";
        return 2;
    }
    // server 布局需要 1 个 main accept owner + subs 个 sub owner 各自占一个
    // Processer driver 槽：runtime workers 必须 ≥ subs+1（client-only 无 main）。
    // 未显式给 workers 时按 subs+1 取最小可用值；显式给小了即拒绝（不静默升）。
    const std::uint64_t eff_subs = subs > 0 ? subs : (workers > 0 ? workers : 2);
    const std::uint64_t min_workers = eff_subs + 1;
    if (workers == 0) {
        workers = min_workers;
    } else if (workers < min_workers) {
        std::cerr << "args error: --runtime-workers=" << workers
                  << " < subs+1=" << min_workers
                  << "（server 需 1 main + " << eff_subs << " sub owner 槽）\n";
        return 2;
    }

    std::signal(SIGPIPE, SIG_IGN);

    g_bbt_coroutine_config->m_cfg_static_thread_num = workers;
    g_bbt_coroutine_config->m_cfg_stack_size = 65536;
    try {
        g_scheduler->Start();
    } catch (const std::exception& e) {
        std::cerr << "scheduler start failed: " << e.what() << '\n';
        return 2;
    }

    ProcObserver observer;
    observer.Start(50ms);
    const auto wall_start = NowNs();

    std::shared_ptr<RawEchoHandler> raw_handler;
    std::shared_ptr<RawHandler> handler;
    if (mode == "raw") {
        raw_handler = std::make_shared<RawEchoHandler>();
        handler = raw_handler;
    }
    Engine engine(handler);
    if (mode == "co") {
        const std::size_t cap = accept_cap > 0
            ? static_cast<std::size_t>(accept_cap)
            : static_cast<std::size_t>(eff_subs * 256);
        if (engine.EnableAcceptHandoff(cap)) {
            std::cerr << "EnableAcceptHandoff failed\n";
            return 2;
        }
    }

    EngineConfig config;
    std::error_code addr_err;
    const Address listen = Address::Ip("127.0.0.1", port, addr_err);
    if (addr_err) { std::cerr << "bad listen addr\n"; return 2; }
    config.listen_addresses = {listen};
    config.runtime_progress = RuntimeProgress::Independent;
    config.options.num_event_loop = static_cast<std::int64_t>(eff_subs);

    std::error_code run_err;
    std::thread engine_thread([&] { run_err = engine.Run(config); });
    const auto deadline = std::chrono::steady_clock::now() + 5000ms;
    bool ready = false;
    while (std::chrono::steady_clock::now() < deadline) {
        if (engine.Phase() == EnginePhase::Running) { ready = true; break; }
        std::this_thread::sleep_for(5ms);
    }
    if (!ready) {
        std::cerr << "engine 未在 5s 内进入 Running\n";
        engine.RequestClose();
        if (engine_thread.joinable()) engine_thread.join();
        return 2;
    }
    std::vector<Address> bound;
    if (engine.LocalAddresses(bound) || bound.empty()) {
        std::cerr << "LocalAddresses failed\n";
        engine.RequestClose();
        engine_thread.join();
        return 2;
    }
    const auto bound_port = bound[0].Port();

    // co 模式：接受协程 + 每连接 echo 协程由 scheduler worker 推进。任务用
    // g_pending_tasks 计数（有界=活动数，不积累历史 future）；收口带界等待，
    // 超时仍有未完成任务是真实失败（非零退出），不允许在协程仍引用栈对象时
    // 退出进程。
    bool teardown_incomplete = false;
    if (mode == "co") {
        TrackTaskBegin();
        g_scheduler->RegistCoroutineTask(65536, [&engine] {
            while (!g_stop.load()) {
                bbt::coroutine::WaitOptions opts;
                opts.deadline = std::chrono::steady_clock::now() + 200ms;
                const auto res = engine.CoAccept(opts);
                if (res.error == std::make_error_code(std::errc::timed_out)) continue;
                if (res.error) break;
                const Connection c = res.connection.Raw();
                g_metrics.accepted.fetch_add(1);
                TrackTaskBegin();
                g_scheduler->RegistCoroutineTask(65536, [c] {
                    CoServeConnection(c);
                    TrackTaskEnd();
                }, "bench.conn");
            }
            TrackTaskEnd();
        }, "bench.accept");
    }

    // 就绪协议：唯一一行 PORT= 到 stdout（driver 按行读，非 JSON）。
    std::cout << "PORT=" << bound_port << '\n';
    std::cout.flush();

    // stdin 命令循环：stat/fin_overhead 回一行 JSON；quit/EOF 收口。
    std::string line;
    while (std::getline(std::cin, line)) {
        if (line == "quit") break;
        if (line == "stat") { EmitStatsLine(); continue; }
        if (line == "fin_overhead") { EmitFinOverheadLine(); continue; }
    }

    g_stop.store(true);
    engine.RequestClose();
    if (engine_thread.joinable()) engine_thread.join();
    if (mode == "co") {
        const auto conn_deadline = std::chrono::steady_clock::now() + 5000ms;
        if (!WaitTasksDone(conn_deadline)) {
            std::cerr << "FAIL: conn/accept coroutine 未在 5s 内退出（pending="
                      << PendingTasks() << "）\n";
            teardown_incomplete = true;
        }
    }
    // 收口验证：engine Run 已返回 = 网络资源/callback/同代解绑完成；此时仍
    // 有未释放连接或未完成协程即 teardown 不全——非零退出而非 WARN 放行。
    const auto remaining_conns = ActiveConns();
    if (remaining_conns != 0) {
        std::cerr << "FAIL: teardown 后仍有 " << remaining_conns
                  << " 个未释放连接\n";
        teardown_incomplete = true;
    }

    observer.Stop();
    const auto wall_ns = NowNs() - wall_start;
    const double wall_s = wall_ns > 0 ? wall_ns / 1e9 : 0.0;
    const double cpu_s = observer.CpuSeconds();
    const std::uint64_t completed = g_metrics.completed.load();

    Json j;
    j.K("role").V("server")
     .K("mode").V(mode)
     .K("port").V(static_cast<std::uint64_t>(bound_port))
     .K("runtime_workers").V(workers)
     .K("subs").V(static_cast<std::uint64_t>(config.options.num_event_loop))
     .K("accepted").V(static_cast<std::uint64_t>(g_metrics.accepted.load()))
     .K("completed").V(completed)
     .K("echoed_fin").V(static_cast<std::uint64_t>(g_metrics.echoed_fin.load()))
     .K("bytes_in").V(static_cast<std::uint64_t>(g_metrics.bytes_in.load()))
     .K("bytes_out").V(static_cast<std::uint64_t>(g_metrics.bytes_out.load()))
     .K("errors").V(static_cast<std::uint64_t>(g_metrics.errors.load()))
     .K("frames_bad").V(static_cast<std::uint64_t>(g_metrics.frames_bad.load()))
     .K("seq_bad").V(static_cast<std::uint64_t>(g_metrics.seq_bad.load()))
     .K("write_closed").V(static_cast<std::uint64_t>(g_write_closed.load()))
     .K("active_conns_end").V(remaining_conns)
     .K("wall_seconds").V(wall_s)
     .K("cpu_seconds").V(cpu_s)
     .K("cpu_per_op").V(completed > 0 ? cpu_s / completed : 0.0)
     .K("proc_ok").V(observer.ProcOk())
     .K("rss_max_bytes").V(observer.RssMaxBytes())
     .K("rss_end_bytes").V(observer.RssEndBytes());
    std::cout << j.Done() << '\n';
    std::cout.flush();

    return (run_err || g_metrics.errors.load() != 0 ||
            g_metrics.frames_bad.load() != 0 || g_metrics.seq_bad.load() != 0 ||
            teardown_incomplete) ? 1 : 0;
}

}  // namespace

int main(int argc, char** argv) {
    // 进程寿命 runtime 无静态析构收口：engine join + 自有任务归零验证后走
    // _Exit 边界（见 ProcessRuntimeTestMain.hpp 头注的已知限制说明）。
    return bbt_net_test::RunTestProcessExit([&] { return RunServer(argc, argv); });
}
