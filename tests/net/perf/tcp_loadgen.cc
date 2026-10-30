// infra-perf 闭环：纯 POSIX TCP 负载发生器（独立进程，与 server 分离）。
//
// 进程契约（与 scripts/bench/driver.py 对齐）：
// argv --mode=pingpong|transfer|conns --port=PORT [--clients=N]
// [--payload-bytes=N] [--duration-ms=N] [--warmup-ms=N] [--sample-cap=N]
// 工作 连接 127.0.0.1:port，按模式发确定性帧，校验 echo 内容/序号，记录每
// 次 completed 往返时延（有界样本）。非阻塞 socket + poll 驱动；每客户
// 端一个连接，clients 个 worker 线程。
// 结束 达到 duration（或 conns 模式完成全部迭代）→ 输出单行 JSON 到 stdout，
// 关闭全部 socket。退出码非零当 errors/校验失败/未完成超时>0。
// 协议 与服务端同一 perf_common.hpp 帧；pingpong=一问一答，transfer=单向大
// 块发送并以 echo 字节数吞吐计，conns=多连接建连+单帧往返。
//
// 统一测量窗（三负载一致，冻结口径）：
// wall_start → [warmup_ms 预热窗：建立连接并跑真实流量，但不计任何业务指标]
// → warm_end → [duration_ms 测量窗：completed/bytes/时延/conns/CPU
// 全部从零起算] → 结束（drain/teardown 在测量窗外，drain 收到的
// 合法 echo 仍计 bytes_in/completed——是测量窗内发出工作的完成）。
// warm_end 由各 worker 本地时钟判定（连接建立前起算，预热连接在测量窗内
// 复用）；正式运行时 driver 优先用「独立预热进程 + 测量进程 warmup=0」的
// 服务端同进程双调用形态（见 driver.py --warmup-ms 文档），本内建 warmup
// 是等价的单机降级路径。/proc 自身采样只在 ProcObserver 窗口内有效：
// proc_ok=false 时本进程测量整体无效，如实上报而非拿 0 冒充。
//
// 帧序完整性：每连接 seq 校验覆盖全部三负载（含 transfer drain 段）——服务端
// 逐帧回显即逐帧对账；seq 乱序/重复/缺失与内容错一样计 seq_bad 并终止该连接。
//
// 口径：客户端 CPU/RSS 由本进程 /proc/self 采样（与服务端分离）；latencies 用
// 有界环形样本（sample-cap，默认 8192），超限后 reservoir 抽样保持分布代表；
// 不存逐帧日志、不写 per-request stdout。

// 注意：客户端刻意只用裸 POSIX socket（不链接 bbt_infra_net）——负载发生器必须
// 是独立于被测产品的参照实现，客户端 CPU/RSS 与服务端分进程计量。

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "perf_common.hpp"

using namespace bbt::infra::net_perf;
using namespace std::chrono_literals;

namespace {

std::atomic<bool> g_stop{false};

struct ClientMetrics {
    std::atomic<std::uint64_t> completed{0};
    std::atomic<std::uint64_t> bytes_out{0};
    std::atomic<std::uint64_t> bytes_in{0};
    std::atomic<std::uint64_t> errors{0};
    std::atomic<std::uint64_t> seq_bad{0};
    std::atomic<std::uint64_t> timeouts{0};
    std::atomic<std::uint64_t> conns_ok{0};
};

// 有界时延样本：reservoir 抽样，超过 cap 后仍保持均匀代表（不存无限流）。
class LatencySink {
public:
    explicit LatencySink(std::size_t cap) : m_cap(cap) {
        m_samples.reserve(cap);
        m_rng.seed(0x9e3779b9);
    }
    void Add(double v) {
        std::lock_guard<std::mutex> lk(m_mu);
        ++m_seen;
        if (m_samples.size() < m_cap) {
            m_samples.push_back(v);
        } else {
            std::uniform_int_distribution<std::uint64_t> pick(0, m_seen - 1);
            const auto slot = pick(m_rng);
            if (slot < m_cap) m_samples[slot] = v;
        }
    }
    std::vector<double> Sorted() {
        std::lock_guard<std::mutex> lk(m_mu);
        auto v = m_samples;
        std::sort(v.begin(), v.end());
        return v;
    }
    std::uint64_t Seen() { std::lock_guard<std::mutex> lk(m_mu); return m_seen; }
private:
    std::size_t m_cap;
    std::vector<double> m_samples;
    std::uint64_t m_seen{0};
    std::mt19937_64 m_rng;
    std::mutex m_mu;
};

// 非阻塞 socket 包装：poll 驱动的 send/recv 全量循环（带 deadline）。
struct Pipe {
    int fd{-1};
    std::string inbuf;          // 已读未消费字节
    std::string outbuf;         // 待发字节
    std::size_t out_off{0};
    bool eof{false};
    bool failed{false};

    bool ConnectLoopback(std::uint16_t port, int timeout_ms) {
        fd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) return false;
        int flags = ::fcntl(fd, F_GETFL, 0);
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        ::inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
        if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
            if (errno != EINPROGRESS) { ::close(fd); fd = -1; return false; }
        }
        pollfd p{fd, POLLOUT, 0};
        const int r = ::poll(&p, 1, timeout_ms);
        if (r <= 0 || !(p.revents & POLLOUT)) { ::close(fd); fd = -1; return false; }
        int so_err = 0; socklen_t sl = sizeof(so_err);
        ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &so_err, &sl);
        if (so_err != 0) { ::close(fd); fd = -1; return false; }
        return true;
    }
    void Close() { if (fd >= 0) { ::close(fd); fd = -1; } }
    ~Pipe() { Close(); }

    // 把 outbuf 推给内核直到空或超时；返回 false=写错/超时。
    bool Flush(int timeout_ms) {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        while (out_off < outbuf.size()) {
            const ssize_t n = ::send(fd, outbuf.data() + out_off,
                                     outbuf.size() - out_off, MSG_NOSIGNAL);
            if (n > 0) { out_off += static_cast<std::size_t>(n); continue; }
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                const auto remain = std::chrono::duration_cast<std::chrono::milliseconds>(
                    deadline - std::chrono::steady_clock::now());
                if (remain.count() <= 0) return false;
                pollfd p{fd, POLLOUT, 0};
                if (::poll(&p, 1, static_cast<int>(remain.count())) <= 0) return false;
                continue;
            }
            return false;  // EPIPE/ECONNRESET 等真实错误
        }
        outbuf.clear();
        out_off = 0;
        return true;
    }
    // 同 Flush，但遇到 EAGAIN 时把 POLLOUT|POLLIN 一起等；可读就调
    // on_readable() 让调用方先消费对端数据再重试写。
    // transfer 负载是双向等量流：服务端回显把本端接收缓冲填满后，服务端
    // 自己的发送也会阻塞；此时只等 POLLOUT 会双方互堵到 deadline。
    // on_readable 返回 false 表示消费失败（IO/协议/对账错误），立即失败。
    template <typename OnReadable>
    bool FlushConsuming(int timeout_ms, OnReadable&& on_readable) {
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        while (out_off < outbuf.size()) {
            const ssize_t n = ::send(fd, outbuf.data() + out_off,
                                     outbuf.size() - out_off, MSG_NOSIGNAL);
            if (n > 0) { out_off += static_cast<std::size_t>(n); continue; }
            if (n < 0 && errno == EINTR) continue;
            if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                const auto remain = std::chrono::duration_cast<std::chrono::milliseconds>(
                    deadline - std::chrono::steady_clock::now());
                if (remain.count() <= 0) return false;
                pollfd p{fd, static_cast<short>(POLLOUT | POLLIN), 0};
                if (::poll(&p, 1, static_cast<int>(remain.count())) <= 0) return false;
                if (p.revents & (POLLIN | POLLRDHUP | POLLHUP | POLLERR)) {
                    if (!on_readable()) return false;
                }
                continue;
            }
            return false;  // EPIPE/ECONNRESET 等真实错误
        }
        outbuf.clear();
        out_off = 0;
        return true;
    }
    // 读一次（非阻塞）；返回 >0 字节，0=暂无可读/对端EOF，<0=错误。
    int ReadSome(char* buf, std::size_t cap) {
        const ssize_t n = ::recv(fd, buf, cap, 0);
        if (n > 0) return static_cast<int>(n);
        if (n == 0) { eof = true; return 0; }
        if (errno == EINTR) return 0;
        if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
        failed = true;
        return -1;
    }
    // 等可读（deadline 内）；返回 false=超时/错误。
    bool WaitReadable(int timeout_ms) {
        pollfd p{fd, static_cast<short>(POLLIN | POLLRDHUP), 0};
        const int r = ::poll(&p, 1, timeout_ms);
        return r > 0 && (p.revents & (POLLIN | POLLRDHUP | POLLHUP | POLLERR));
    }
};

// 从 inbuf 拆一个完整帧（需要时再读）；false=协议错/IO错/超时。
bool RecvFrame(Pipe& p, FrameParser& parser, std::string& frame, int timeout_ms) {
    for (;;) {
        if (parser.bad()) return false;
        if (parser.HasFrame()) { frame = parser.TakeFrame(); return true; }
        // 还需要更多字节：先读一次尝试，不够再 wait。
        char buf[65536];
        const int n = p.ReadSome(buf, sizeof(buf));
        if (n < 0) return false;
        if (n > 0) {
            if (!parser.Feed(buf, n)) return false;
            continue;
        }
        if (p.eof) return false;
        if (!p.WaitReadable(timeout_ms)) return false;
    }
}

// ---------------- 工作负载 ----------------
// measuring(now) = now >= warm_end：统一测量窗起点；预热窗内所有业务指标
// （completed/bytes/latency/conns_ok）一律不计——窗口口径对三负载一致。

// pingpong：单连接顺序请求-响应；每 completed 计一次时延。
void PingPongWorker(std::uint64_t conn_id, std::uint16_t port,
                    std::uint32_t payload, std::uint64_t duration_ms,
                    std::uint64_t warmup_ms, ClientMetrics& m,
                    LatencySink& latency) {
    const auto start = std::chrono::steady_clock::now();
    const auto warm_end = start + std::chrono::milliseconds(warmup_ms);
    const auto end = warm_end + std::chrono::milliseconds(duration_ms);
    Pipe p;
    if (!p.ConnectLoopback(port, 3000)) { m.errors.fetch_add(1); return; }
    FrameParser parser;
    std::uint64_t seq = 0;
    bool outstanding = false;  // 已发未收的在途请求（顺序一问一答至多 1 个）
    std::int64_t outstanding_stamp = 0;
    while (!g_stop.load() && std::chrono::steady_clock::now() < end) {
        const bool measuring = std::chrono::steady_clock::now() >= warm_end;
        const auto stamp = NowNs();
        p.outbuf += MakeFrame(conn_id, seq, stamp, payload);
        if (!p.Flush(3000)) { m.errors.fetch_add(1); break; }
        outstanding = true;
        outstanding_stamp = stamp;
        if (measuring) m.bytes_out.fetch_add(kHeaderBytes + payload);
        std::string frame;
        if (!RecvFrame(p, parser, frame, 3000)) {
            m.timeouts.fetch_add(1);
            break;
        }
        outstanding = false;
        if (!parser.VerifyCurrent(frame) || parser.seq() != seq ||
            parser.conn() != conn_id) {
            m.seq_bad.fetch_add(1);
            break;
        }
        if (measuring) {
            m.bytes_in.fetch_add(frame.size());
            m.completed.fetch_add(1);
            latency.Add(static_cast<double>(NowNs() - stamp));
        }
        ++seq;
    }
    // 窗口到点时可能恰有一发在途请求（循环条件截断在 Flush 后、RecvFrame 前）：
    // 有界 drain 把它收干净再关——server 端「窗内收到并回显的帧」才能与本端
    // 「窗内验证的回声」逐帧对账一致。drain 收不到（对端丢了）按 timeouts 计。
    if (outstanding) {
        std::string extra;
        if (!RecvFrame(p, parser, extra, 1500)) {
            m.timeouts.fetch_add(1);
        } else if (!parser.VerifyCurrent(extra) || parser.seq() != seq ||
                   parser.conn() != conn_id) {
            m.seq_bad.fetch_add(1);
        } else {
            m.bytes_in.fetch_add(extra.size());
            m.completed.fetch_add(1);
            latency.Add(static_cast<double>(NowNs() - outstanding_stamp));
        }
    }
    if (std::chrono::steady_clock::now() >= warm_end) m.conns_ok.fetch_add(1);
}

// transfer：单向持续发送 data 帧，以 echo 字节吞吐计（服务端回显即确认）。
// 统一测量窗：预热窗内发送/回收照常进行（让 TCP/服务端进入稳态），但
// bytes/completed/latency 一律不计；测量窗结束后 fin + drain，drain 回收的
// 是测量窗内已发出工作的合法 echo，仍如实计数（completed/bytes_in），其
// stamp 时延也计入样本。seq 对账覆盖主循环与 drain 全路径。
void TransferWorker(std::uint64_t conn_id, std::uint16_t port,
                    std::uint32_t payload, std::uint64_t duration_ms,
                    std::uint64_t warmup_ms,
                    ClientMetrics& m, LatencySink& latency) {
    const auto start = std::chrono::steady_clock::now();
    const auto warm_end = start + std::chrono::milliseconds(warmup_ms);
    const auto end = warm_end + std::chrono::milliseconds(duration_ms);
    Pipe p;
    if (!p.ConnectLoopback(port, 3000)) { m.errors.fetch_add(1); return; }
    FrameParser parser;
    std::uint64_t seq = 0;
    std::uint64_t echo_seq = 0;     // 服务端逐帧回显：echo 的 seq 也必须单调
    std::uint64_t outstanding = 0;  // 在途上限：防客户端快于服务端堆积内存
    constexpr std::uint64_t kMaxOutstanding = 256;
    bool counted_conn = false;
    // 收 echo：把已读字节喂进 parser，逐帧校验并计数（主循环与写阻塞等待共用）。
    // 返回 false 表示 IO 或对账错误，调用方应中止本连接。
    auto consume_echo = [&](char* buf, int n, bool measuring) -> bool {
        if (n < 0) { m.errors.fetch_add(1); return false; }
        if (n > 0 && !parser.Feed(buf, n)) { m.errors.fetch_add(1); return false; }
        while (parser.HasFrame()) {
            const std::string frame = parser.TakeFrame();
            if (parser.kind() != kKindData || !parser.VerifyCurrent(frame) ||
                parser.conn() != conn_id || parser.seq() != echo_seq) {
                m.seq_bad.fetch_add(1);
                return false;
            }
            ++echo_seq;
            --outstanding;
            if (measuring) {
                m.bytes_in.fetch_add(frame.size());
                m.completed.fetch_add(1);
                latency.Add(static_cast<double>(NowNs() - parser.stamp_ns()));
            }
        }
        return true;
    };
    while (!g_stop.load() && std::chrono::steady_clock::now() < end) {
        const bool measuring = std::chrono::steady_clock::now() >= warm_end;
        if (measuring && !counted_conn) { m.conns_ok.fetch_add(1); counted_conn = true; }
        if (outstanding < kMaxOutstanding) {
            p.outbuf += MakeFrame(conn_id, seq, NowNs(), payload);
            ++seq;
            if (measuring) m.bytes_out.fetch_add(kHeaderBytes + payload);
            ++outstanding;
        }
        // 推写；推不动（EAGAIN）时在同一次等待里收 echo 再重试，避免两侧
        // 内核发送缓冲互堵。半帧由 parser 保持。
        char buf[65536];
        const bool flushed = p.FlushConsuming(3000, [&]() -> bool {
            const int rn = p.ReadSome(buf, sizeof(buf));
            return consume_echo(buf, rn, measuring);
        });
        if (!flushed) {
            if (m.errors.load() == 0 && m.seq_bad.load() == 0) m.errors.fetch_add(1);
            break;
        }
        // 非阻塞顺手收 echo（服务端逐帧回显）。
        const int n = p.ReadSome(buf, sizeof(buf));
        if (!consume_echo(buf, n, measuring)) break;
        if (n == 0 && outstanding > 0) {
            if (!p.WaitReadable(20)) { /* 继续发 */ }
        }
    }
    // fin：告知服务端本连接发送完毕；继续把残余 echo 收干净（drain 在测量窗
    // 外但收的是窗内已发出的工作，合法 echo 仍计 bytes_in/completed）。
    p.outbuf += MakeFrame(conn_id, seq, NowNs(), 0, kKindFin);
    if (!p.Flush(3000)) { m.errors.fetch_add(1); }
    const auto drain_deadline = std::chrono::steady_clock::now() + 3000ms;
    bool drain_bad = false;
    while (outstanding > 0 && std::chrono::steady_clock::now() < drain_deadline) {
        if (!p.WaitReadable(500)) break;
        char buf[65536];
        const int n = p.ReadSome(buf, sizeof(buf));
        if (n < 0) break;
        if (n > 0 && !parser.Feed(buf, n)) break;
        while (parser.HasFrame()) {
            const std::string frame = parser.TakeFrame();
            // fin echo（kind='F'）是服务端对本端 fin 的回显：不参与 seq 序列，
            // 收到即本连接回声收尾信号（outstanding 数据回声必须已经全部先到）。
            if (parser.kind() == kKindFin) {
                if (outstanding != 0) { m.seq_bad.fetch_add(1); drain_bad = true; }
                break;
            }
            // drain 段同样校验 seq/内容：漏帧、重复、乱序都不是良性收尾。
            if (parser.kind() != kKindData || !parser.VerifyCurrent(frame) ||
                parser.conn() != conn_id || parser.seq() != echo_seq) {
                m.seq_bad.fetch_add(1);
                drain_bad = true;
                break;
            }
            ++echo_seq;
            --outstanding;
            m.bytes_in.fetch_add(frame.size());
            m.completed.fetch_add(1);
            latency.Add(static_cast<double>(NowNs() - parser.stamp_ns()));
        }
        if (drain_bad) break;
        if (parser.kind() == kKindFin) break;  // fin echo 已收：收尾完成
    }
    if (drain_bad) { p.Close(); return; }
    if (outstanding > 0) m.timeouts.fetch_add(1);
    if (std::chrono::steady_clock::now() >= warm_end && !counted_conn) {
        m.conns_ok.fetch_add(1);
    }
}

// conns：clients 个连接各自完成一次建连 + 单帧往返；度量建连/首往返成本。
// 统一测量窗：预热窗内建连/往返照常（连接数 ramp-up），conns_ok/completed/
// bytes/latency 一律不计。conns 连接短生命周期，服务端侧见 fin/EOF 关闭。
// 尾部裁切：deadline 前不足单轮预算（建连+echo 上限 kIterBudgetMs）则不再开新
// 连接——避免把「窗口到点」误报成超时；accept 滞后仍属真实失败由 timeouts 计。
void ConnsWorker(std::uint64_t conn_id, std::uint16_t port,
                 std::uint32_t payload, std::uint64_t duration_ms,
                 std::uint64_t warmup_ms,
                 ClientMetrics& m, LatencySink& latency) {
    constexpr std::int64_t kIterBudgetMs = 500;  // loopback 建连+echo 实际 <ms；
                                                 // 500ms 是给 accept 滞后的窗口
    const auto start = std::chrono::steady_clock::now();
    const auto warm_end = start + std::chrono::milliseconds(warmup_ms);
    const auto end = warm_end + std::chrono::milliseconds(duration_ms);
    std::uint64_t iter = 0;
    while (!g_stop.load() &&
           std::chrono::duration_cast<std::chrono::milliseconds>(
               end - std::chrono::steady_clock::now()).count() > kIterBudgetMs) {
        const bool measuring = std::chrono::steady_clock::now() >= warm_end;
        const auto stamp = NowNs();
        Pipe p;
        if (!p.ConnectLoopback(port, 3000)) { m.errors.fetch_add(1); break; }
        if (measuring) m.conns_ok.fetch_add(1);
        FrameParser parser;
        // conns 每连接只有一个 data 帧（seq=0）+ fin 收尾（seq=1）。
        p.outbuf += MakeFrame(conn_id, 0, NowNs(), payload);
        if (!p.Flush(3000)) { m.errors.fetch_add(1); break; }
        if (measuring) m.bytes_out.fetch_add(kHeaderBytes + payload);
        std::string frame;
        if (!RecvFrame(p, parser, frame, 3000)) { m.timeouts.fetch_add(1); break; }
        if (!parser.VerifyCurrent(frame) || parser.seq() != 0 ||
            parser.conn() != conn_id) { m.seq_bad.fetch_add(1); break; }
        if (measuring) {
            m.bytes_in.fetch_add(frame.size());
            m.completed.fetch_add(1);
            latency.Add(static_cast<double>(NowNs() - stamp));
        }
        // 半关收尾：发 fin 后 shutdown(SHUT_WR)（对端见 EOF 但本端仍能收
        // fin echo），读回 echo 再全关——避免 close() 带着未读 echo 触发
        // RST 把服务端的回声/收尾打成真实错误。
        p.outbuf += MakeFrame(conn_id, 1, NowNs(), 0, kKindFin);
        if (p.Flush(1000)) {
            ::shutdown(p.fd, SHUT_WR);
            std::string fin_frame;
            // 读 fin echo（或 EOF）但不强要求：服务端在 fin 回声已发后
            // RequestClose，本端读到任一终态都算正常收尾。
            RecvFrame(p, parser, fin_frame, 1000);
        }
        p.Close();
        ++iter;
    }
}

}  // namespace

int main(int argc, char** argv) {
    const Args args = ParseArgs(argc, argv,
        /*required*/{"mode", "port"},
        /*optional*/{"clients", "payload-bytes", "duration-ms", "warmup-ms",
                     "sample-cap", "io-timeout-ms"});
    if (!args.ok) {
        std::cerr << "args error: " << args.error << '\n';
        return 2;
    }
    const std::string mode = ArgStr(args, "mode");
    const std::uint64_t port = ArgU64(args, "port", 0);
    const std::uint64_t clients = ArgU64(args, "clients", 1);
    const std::uint64_t payload = ArgU64(args, "payload-bytes", 64);
    const std::uint64_t duration_ms = ArgU64(args, "duration-ms", 1000);
    const std::uint64_t warmup_ms = ArgU64(args, "warmup-ms", 0);
    const std::uint64_t sample_cap = ArgU64(args, "sample-cap", 8192);
    if (port == 0 || port > 65535) {
        std::cerr << "args error: --port 必须是 1..65535\n";
        return 2;
    }
    if (mode != "pingpong" && mode != "transfer" && mode != "conns") {
        std::cerr << "args error: --mode 必须是 pingpong|transfer|conns\n";
        return 2;
    }
    if (clients == 0 || clients > 4096) {
        std::cerr << "args error: --clients 必须在 1..4096\n";
        return 2;
    }

    std::signal(SIGPIPE, SIG_IGN);
    std::signal(SIGINT, [](int) { g_stop.store(true); });
    std::signal(SIGTERM, [](int) { g_stop.store(true); });

    ProcObserver observer;
    observer.Start(50ms);
    // 进程墙钟含预热窗：ops_per_sec 的分母只用测量窗 wall（见下 measured_wall_s
    // 的口径——本进程全部 worker 共享同一 warm_end，墙钟直接用 duration_ms
    // 加实测 drain 开销的近似；精确吞吐分母以 measuring 窗口的 steady 差为准）。
    const auto wall_start = NowNs();

    ClientMetrics m;
    LatencySink latency(static_cast<std::size_t>(sample_cap));
    // 测量窗真实墙钟：所有 worker 用同一 [start+ warmup, start+warmup+duration]
    // 窗口；进程级吞吐分母 = duration_ms（窗口名义长）——drain/尾巴在窗外不
    // 计入分母也不计入完成数（drain 计的是窗内已发工作的完成，口径一致）。
    const double measured_wall_s = duration_ms > 0 ? duration_ms / 1000.0 : 0.0;
    std::vector<std::thread> workers;
    workers.reserve(clients);
    for (std::uint64_t i = 0; i < clients; ++i) {
        if (mode == "pingpong") {
            workers.emplace_back(PingPongWorker, i + 1, static_cast<std::uint16_t>(port),
                static_cast<std::uint32_t>(payload), duration_ms, warmup_ms,
                std::ref(m), std::ref(latency));
        } else if (mode == "transfer") {
            workers.emplace_back(TransferWorker, i + 1, static_cast<std::uint16_t>(port),
                static_cast<std::uint32_t>(payload), duration_ms, warmup_ms,
                std::ref(m), std::ref(latency));
        } else {
            workers.emplace_back(ConnsWorker, i + 1, static_cast<std::uint16_t>(port),
                static_cast<std::uint32_t>(payload), duration_ms, warmup_ms,
                std::ref(m), std::ref(latency));
        }
    }
    for (auto& t : workers) t.join();
    observer.Stop();

    const auto wall_ns = NowNs() - wall_start;
    const double wall_s = wall_ns > 0 ? wall_ns / 1e9 : 0.0;
    const auto sorted = latency.Sorted();
    const std::uint64_t completed = m.completed.load();
    const double cpu_s = observer.CpuSeconds();

    Json j;
    j.K("role").V("loadgen")
     .K("mode").V(mode)
     .K("clients").V(clients)
     .K("conns_ok").V(static_cast<std::uint64_t>(m.conns_ok.load()))
     .K("payload_bytes").V(payload)
     .K("duration_ms").V(duration_ms)
     .K("warmup_ms").V(warmup_ms)
     .K("measured_wall_seconds").V(measured_wall_s)
     .K("completed").V(completed)
     .K("bytes_out").V(static_cast<std::uint64_t>(m.bytes_out.load()))
     .K("bytes_in").V(static_cast<std::uint64_t>(m.bytes_in.load()))
     .K("errors").V(static_cast<std::uint64_t>(m.errors.load()))
     .K("seq_bad").V(static_cast<std::uint64_t>(m.seq_bad.load()))
     .K("timeouts").V(static_cast<std::uint64_t>(m.timeouts.load()))
     .K("wall_seconds").V(wall_s)
     .K("ops_per_sec").V(measured_wall_s > 0 ? completed / measured_wall_s : 0.0)
     .K("throughput_bytes_per_sec").V(measured_wall_s > 0 ? m.bytes_in.load() / measured_wall_s : 0.0)
     .K("latency_p50_ns").V(Quantile(sorted, 0.50))
     .K("latency_p95_ns").V(Quantile(sorted, 0.95))
     .K("latency_p99_ns").V(Quantile(sorted, 0.99))
     .K("latency_samples").V(latency.Seen())
     .K("latency_sample_cap").V(sample_cap)
     .K("cpu_seconds").V(cpu_s)
     .K("cpu_per_op").V(completed > 0 ? cpu_s / completed : 0.0)
     .K("proc_ok").V(observer.ProcOk())
     .K("rss_max_bytes").V(observer.RssMaxBytes())
     .K("rss_end_bytes").V(observer.RssEndBytes());
    std::cout << j.Done() << '\n';
    std::cout.flush();

    const bool bad = m.errors.load() != 0 || m.seq_bad.load() != 0 ||
                     m.timeouts.load() != 0;
    return bad ? 1 : 0;
}
