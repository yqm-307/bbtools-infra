#pragma once

// infra-perf 闭环的 bench 共享头部（测试私有，不发布、不进入公共接口面）。
// 统一：确定性小端 wire 帧、序列校验、argv 解析、/proc 进程统计采样（有界）、
// 紧凑 JSON 发射。所有 benchmark 进程只写结构化行到 stdout（协议帧与最终 JSON），
// 诊断/噪声一律走 stderr 由 driver 捕获并落盘，不产生 per-request stdout。

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <unistd.h>

namespace bbt::infra::net_perf {

// ---------------- wire 帧（确定性，两进程同协议） ----------------
// 'B','B','T','0' magic + kind(1) + conn(u64 LE) + seq(u64 LE) + stamp_ns(u64 LE)
// + payload_len(u32 LE) + payload
// kind: 'D' data / 'F' fin（单向传输侧告知对端“写完即等 EOF/关”）
// conn: 连接内局部序号；seq: 连接内单调帧序号；stamp_ns: 发送端 steady ns（时延）。
inline constexpr std::uint32_t kMagic = 0x30544242u;  // "BBT0" LE
inline constexpr std::uint8_t kKindData = 'D';
inline constexpr std::uint8_t kKindFin  = 'F';
inline constexpr std::size_t kHeaderBytes = 4 + 1 + 8 + 8 + 8 + 4;

inline void AppendHeader(std::string& out, std::uint8_t kind, std::uint64_t conn,
                         std::uint64_t seq, std::int64_t stamp_ns,
                         std::uint32_t payload_len) {
    const std::size_t base = out.size();
    out.resize(base + kHeaderBytes);
    char* p = out.data() + base;
    std::memcpy(p, &kMagic, 4); p += 4;
    *p++ = static_cast<char>(kind);
    std::uint64_t v = conn;
    for (int i = 0; i < 8; ++i) { *p++ = static_cast<char>(v & 0xff); v >>= 8; }
    v = seq;
    for (int i = 0; i < 8; ++i) { *p++ = static_cast<char>(v & 0xff); v >>= 8; }
    v = static_cast<std::uint64_t>(stamp_ns);
    for (int i = 0; i < 8; ++i) { *p++ = static_cast<char>(v & 0xff); v >>= 8; }
    std::uint32_t w = payload_len;
    for (int i = 0; i < 4; ++i) { *p++ = static_cast<char>(w & 0xff); w >>= 8; }
}

// 确定性伪随机 payload：(conn,seq,word_index) 的 xorshift——两端可独立重算校验
// 内容，不依赖对端任何状态；内容错误/截断即 fail（不静默吞错）。按 8 字节 word
// 生成以支撑大 payload 的生成/校验吞吐。
inline std::uint64_t PayloadWord(std::uint64_t conn, std::uint64_t seq,
                                 std::uint64_t word_index) {
    std::uint64_t x = conn * 0x9E3779B97F4A7C15ull
                    + seq * 0xBF58476D1CE4E5B9ull
                    + word_index * 0x94D049BB133111EBull;
    x ^= x >> 33; x *= 0xFF51AFD7ED558CCDull; x ^= x >> 33;
    return x;
}

inline void FillPayload(char* dst, std::uint64_t conn, std::uint64_t seq,
                        std::uint32_t len) {
    std::uint32_t off = 0;
    std::uint64_t wi = 0;
    while (off < len) {
        const std::uint64_t w = PayloadWord(conn, seq, wi++);
        const std::uint32_t take = std::min<std::uint32_t>(8, len - off);
        std::memcpy(dst + off, &w, take);
        off += take;
    }
}

inline bool VerifyPayload(const char* src, std::uint64_t conn, std::uint64_t seq,
                          std::uint32_t len) {
    std::uint32_t off = 0;
    std::uint64_t wi = 0;
    while (off < len) {
        const std::uint64_t w = PayloadWord(conn, seq, wi++);
        const std::uint32_t take = std::min<std::uint32_t>(8, len - off);
        if (std::memcmp(src + off, &w, take) != 0) return false;
        off += take;
    }
    return true;
}

inline std::string MakeFrame(std::uint64_t conn, std::uint64_t seq,
                             std::int64_t stamp_ns, std::uint32_t payload_len,
                             std::uint8_t kind = kKindData) {
    std::string frame;
    frame.reserve(kHeaderBytes + payload_len);
    AppendHeader(frame, kind, conn, seq, stamp_ns, payload_len);
    frame.resize(kHeaderBytes + payload_len);
    FillPayload(frame.data() + kHeaderBytes, conn, seq, payload_len);
    return frame;
}

// 解析器：流式拆帧，跨 TryRead 边界保持半帧状态（不用 substr 拷贝整帧）。
// Feed 返回消费字节数；frame_ready()/TakeFrame 取出完整帧。magic/长度错 → bad()。
class FrameParser {
public:
    // 追加数据；返回 false 表示协议级错误（调用方应关闭连接并计 error）。
    bool Feed(const char* data, std::size_t size) {
        if (m_bad) return false;
        m_buf.append(data, size);
        return true;
    }
    bool bad() const { return m_bad; }
    // 有完整帧可读
    bool HasFrame() {
        if (m_bad || m_buf.size() < kHeaderBytes) return false;
        if (m_frame_total == 0) {
            std::uint32_t magic = 0;
            std::memcpy(&magic, m_buf.data(), 4);
            if (magic != kMagic) { m_bad = true; return false; }
            m_kind = static_cast<std::uint8_t>(m_buf[4]);
            std::uint64_t v = 0;
            for (int i = 0; i < 8; ++i)
                v |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(m_buf[5 + i])) << (8 * i);
            m_conn = v;
            v = 0;
            for (int i = 0; i < 8; ++i)
                v |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(m_buf[13 + i])) << (8 * i);
            m_seq = v;
            v = 0;
            for (int i = 0; i < 8; ++i)
                v |= static_cast<std::uint64_t>(static_cast<std::uint8_t>(m_buf[21 + i])) << (8 * i);
            m_stamp = static_cast<std::int64_t>(v);
            std::uint32_t len = 0;
            for (int i = 0; i < 4; ++i)
                len |= static_cast<std::uint32_t>(static_cast<std::uint8_t>(m_buf[29 + i])) << (8 * i);
            if (len > 64u * 1024u * 1024u) { m_bad = true; return false; }
            m_len = len;
            m_frame_total = kHeaderBytes + static_cast<std::size_t>(len);
        }
        return m_buf.size() >= m_frame_total;
    }
    // 取出已就绪帧；payload 字段为 frame[kHeaderBytes..)。HasFrame 为假调用未定义。
    std::string TakeFrame() {
        std::string frame = m_buf.substr(0, m_frame_total);
        m_buf.erase(0, m_frame_total);
        m_frame_total = 0;
        return frame;
    }
    std::uint8_t kind() const { return m_kind; }
    std::uint64_t conn() const { return m_conn; }
    std::uint64_t seq() const { return m_seq; }
    std::int64_t stamp_ns() const { return m_stamp; }
    std::uint32_t payload_len() const { return m_len; }
    // 校验当前帧 payload（帧未 Take 前）——用 frame 数据直接核验，避免二次拷贝。
    bool VerifyCurrent(const std::string& frame) const {
        return VerifyPayload(frame.data() + kHeaderBytes, m_conn, m_seq, m_len);
    }
private:
    std::string m_buf;
    std::size_t m_frame_total{0};
    std::uint8_t m_kind{0};
    std::uint64_t m_conn{0};
    std::uint64_t m_seq{0};
    std::int64_t m_stamp{0};
    std::uint32_t m_len{0};
    bool m_bad{false};
};

// ---------------- argv 解析（fail-closed，未知参数即拒绝） ----------------
struct Args {
    std::map<std::string, std::string> kv;
    bool ok{false};
    std::string error;
};

inline Args ParseArgs(int argc, char** argv,
                      const std::vector<std::string>& required,
                      const std::vector<std::string>& optional) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        const std::string item = argv[i];
        if (item.rfind("--", 0) != 0) {
            args.error = "非 --key=value 参数: " + item;
            return args;
        }
        const auto eq = item.find('=');
        if (eq == std::string::npos || eq <= 2) {
            args.error = "缺少 = 的参数: " + item;
            return args;
        }
        args.kv[item.substr(2, eq - 2)] = item.substr(eq + 1);
    }
    for (const auto& key : required) {
        if (args.kv.find(key) == args.kv.end()) {
            args.error = "缺必填参数 --" + key;
            return args;
        }
    }
    for (const auto& entry : args.kv) {
        const bool known =
            std::find(required.begin(), required.end(), entry.first) != required.end()
            || std::find(optional.begin(), optional.end(), entry.first) != optional.end();
        if (!known) {
            args.error = "未知参数 --" + entry.first;
            return args;
        }
    }
    args.ok = true;
    return args;
}

inline std::uint64_t ArgU64(const Args& args, const std::string& key, std::uint64_t dflt) {
    const auto it = args.kv.find(key);
    if (it == args.kv.end()) return dflt;
    try {
        std::size_t used = 0;
        const auto value = std::stoull(it->second, &used);
        if (used != it->second.size()) return dflt;
        return value;
    } catch (...) { return dflt; }
}

inline std::string ArgStr(const Args& args, const std::string& key,
                          const std::string& dflt = "") {
    const auto it = args.kv.find(key);
    return it == args.kv.end() ? dflt : it->second;
}

inline std::int64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count();
}

// ---------------- /proc 自身统计（采样有界，driver 归并） ----------------
struct ProcSample {
    std::int64_t at_ns{0};
    std::uint64_t utime_ticks{0};
    std::uint64_t stime_ticks{0};
    std::uint64_t rss_pages{0};
    bool ok{false};
};

inline ProcSample ReadProcSelf() {
    ProcSample s;
    s.at_ns = NowNs();
    std::ifstream stat("/proc/self/stat");
    std::string line;
    if (!std::getline(stat, line)) return s;
    const auto rparen = line.rfind(')');
    if (rparen == std::string::npos) return s;
    std::istringstream rest(line.substr(rparen + 1));
    std::string token;
    std::vector<std::string> fields;
    while (rest >> token) fields.push_back(token);
    if (fields.size() <= 21) return s;
    try {
        s.utime_ticks = std::stoull(fields[11]);
        s.stime_ticks = std::stoull(fields[12]);
        const long long rss = std::stoll(fields[21]);
        s.rss_pages = rss < 0 ? 0 : static_cast<std::uint64_t>(rss);
        s.ok = true;
    } catch (...) { s.ok = false; }
    return s;
}

inline long TicksPerSecond() {
    static const long v = ::sysconf(_SC_CLK_TCK);
    return v > 0 ? v : 100;
}
inline long PageBytes() {
    static const long v = ::sysconf(_SC_PAGESIZE);
    return v > 0 ? v : 4096;
}

// 进程 CPU/内存观测器：线程内以固定间隔采样自身（对被观测进程零侵入——
// 只读 /proc/self，不引入第三方采样器）。Start() 时 /proc 不可用即记录
// proc_ok=false：进程必须在最终 JSON 如实上报 proc_ok=false（driver 视该
// 运行为无效测量），不得静默退化成 0 值冒充成功。
// cpu_per_op 用窗口 utime+stime 增量 / 完成数；rss_max_bytes 是观测器寿命内
// （Start 到 Stop：含预热与收口，Stop 在 Engine join 之后）按固定间隔采样
// /proc/self 的最大 RSS，显式标注 max 并与 end/瞬时读数分开、不混计。它是
// 采样峰值，可能漏掉瞬时峰——不是内核 VmHWM，也不是正式测量窗内的峰值。
class ProcObserver {
public:
    void Start(std::chrono::milliseconds interval) {
        m_interval = interval;
        m_start = ReadProcSelf();
        m_proc_ok.store(m_start.ok);
        m_rss_max.store(m_start.rss_pages);
        m_running.store(true);
        m_thread = std::thread([this] {
            while (m_running.load()) {
                const auto s = ReadProcSelf();
                if (s.ok) {
                    m_last = s;
                    auto cur = m_rss_max.load();
                    while (s.rss_pages > cur &&
                           !m_rss_max.compare_exchange_weak(cur, s.rss_pages)) {}
                }
                std::this_thread::sleep_for(m_interval);
            }
        });
    }
    void Stop() {
        if (m_running.exchange(false) && m_thread.joinable()) m_thread.join();
        const auto end = ReadProcSelf();
        m_end = end;
        if (!end.ok || !m_start.ok) m_proc_ok.store(false);
    }
    ~ProcObserver() { Stop(); }

    // /proc 在 start/stop 两个端点都可用才算测量有效（中间采样缺失只影响
    // max 精度，仍如实上报 proc_ok=true）。
    bool ProcOk() const { return m_proc_ok.load(); }

    double CpuSeconds() const {
        if (!m_start.ok) return 0.0;
        const ProcSample end = m_end.ok ? m_end : ReadProcSelf();
        if (!end.ok) return 0.0;
        const auto ticks = (end.utime_ticks + end.stime_ticks)
                         - (m_start.utime_ticks + m_start.stime_ticks);
        return static_cast<double>(ticks) / static_cast<double>(TicksPerSecond());
    }
    std::uint64_t RssMaxBytes() const {
        const auto pages = std::max({m_rss_max.load(),
                                     m_end.ok ? m_end.rss_pages : 0});
        return pages * static_cast<std::uint64_t>(PageBytes());
    }
    std::uint64_t RssEndBytes() const {
        const ProcSample end = m_end.ok ? m_end : ReadProcSelf();
        return end.ok ? end.rss_pages * static_cast<std::uint64_t>(PageBytes()) : 0;
    }
    std::uint64_t RssStartBytes() const {
        return m_start.ok ? m_start.rss_pages * static_cast<std::uint64_t>(PageBytes()) : 0;
    }
private:
    std::chrono::milliseconds m_interval{50};
    ProcSample m_start, m_end, m_last;
    std::atomic<bool> m_running{false};
    std::atomic<bool> m_proc_ok{false};
    std::atomic<std::uint64_t> m_rss_max{0};
    std::thread m_thread;
};

// ---------------- 紧凑 JSON 发射（无第三方，仅 std::string 拼接） ----------------
struct Json {
    std::string out{"{"};
    bool first{true};
    void Key() { if (!first) out += ','; first = false; }
    Json& K(const char* k) { Key(); out += '"'; out += k; out += "\":"; return *this; }
    Json& V(const std::string& s) {
        out += '"';
        for (char c : s) {
            if (c == '"' || c == '\\') out += '\\';
            if (c == '\n') { out += "\\n"; continue; }
            if (c == '\r') { out += "\\r"; continue; }
            if (c == '\t') { out += "\\t"; continue; }
            out += c;
        }
        out += '"';
        return *this;
    }
    Json& V(const char* s) { return V(std::string(s ? s : "")); }
    Json& V(std::uint64_t v) { out += std::to_string(v); return *this; }
    Json& V(std::int64_t v) { out += std::to_string(v); return *this; }
    Json& V(int v) { out += std::to_string(v); return *this; }
    Json& V(bool v) { out += v ? "true" : "false"; return *this; }
    Json& V(double v) {
        if (v != v) { out += "null"; return *this; }  // NaN → null（不伪造 0）
        std::ostringstream os; os << std::setprecision(9) << v;
        out += os.str(); return *this;
    }
    std::string Done() { out += '}'; return out; }
};

// 分位数：要求传入已 sort 的有界 vector（样本有界，不存无限流）。
inline double Quantile(const std::vector<double>& sorted, double q) {
    if (sorted.empty()) return 0.0;
    const std::size_t idx =
        static_cast<std::size_t>((sorted.size() - 1) * q + 0.5);
    return sorted[std::min(idx, sorted.size() - 1)];
}

}  // namespace bbt::infra::net_perf
