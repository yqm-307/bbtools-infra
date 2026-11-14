#pragma once

/**
 * perf_common.hpp 的实现件：测试私有头内可见实现，由 perf_common.hpp 末尾
 * include；保留 inline，与声明同处一文件可见域（header-only，不进公共 target）。
 */
namespace bbt::infra::net_perf {

inline void AppendHeader(
    std::string& out,
    std::uint8_t kind,
    std::uint64_t conn,
    std::uint64_t seq,
    std::int64_t stamp_ns,
    std::uint32_t payload_len)
{
    const std::size_t base = out.size();
    out.resize(base + kHeaderBytes);
    char* p = out.data() + base;
    std::memcpy(p, &kMagic, 4);
    p += 4;
    *p++ = static_cast<char>(kind);
    std::uint64_t v = conn;
    for (int i = 0; i < 8; ++i) {
        *p++ = static_cast<char>(v & 0xff);
        v >>= 8;
    }
    v = seq;
    for (int i = 0; i < 8; ++i) {
        *p++ = static_cast<char>(v & 0xff);
        v >>= 8;
    }
    v = static_cast<std::uint64_t>(stamp_ns);
    for (int i = 0; i < 8; ++i) {
        *p++ = static_cast<char>(v & 0xff);
        v >>= 8;
    }
    std::uint32_t w = payload_len;
    for (int i = 0; i < 4; ++i) {
        *p++ = static_cast<char>(w & 0xff);
        w >>= 8;
    }
}

inline std::uint64_t PayloadWord(std::uint64_t conn, std::uint64_t seq, std::uint64_t word_index)
{
    std::uint64_t x = conn * 0x9E3779B97F4A7C15ull + seq * 0xBF58476D1CE4E5B9ull + word_index * 0x94D049BB133111EBull;
    x ^= x >> 33;
    x *= 0xFF51AFD7ED558CCDull;
    x ^= x >> 33;
    return x;
}

inline void FillPayload(char* dst, std::uint64_t conn, std::uint64_t seq, std::uint32_t len)
{
    std::uint32_t off = 0;
    std::uint64_t wi = 0;
    while (off < len) {
        const std::uint64_t w = PayloadWord(conn, seq, wi++);
        const std::uint32_t take = std::min<std::uint32_t>(8, len - off);
        std::memcpy(dst + off, &w, take);
        off += take;
    }
}

inline bool VerifyPayload(const char* src, std::uint64_t conn, std::uint64_t seq, std::uint32_t len)
{
    std::uint32_t off = 0;
    std::uint64_t wi = 0;
    while (off < len) {
        const std::uint64_t w = PayloadWord(conn, seq, wi++);
        const std::uint32_t take = std::min<std::uint32_t>(8, len - off);
        if (std::memcmp(src + off, &w, take) != 0)
            return false;
        off += take;
    }
    return true;
}

inline std::string
MakeFrame(std::uint64_t conn, std::uint64_t seq, std::int64_t stamp_ns, std::uint32_t payload_len, std::uint8_t kind)
{
    std::string frame;
    frame.reserve(kHeaderBytes + payload_len);
    AppendHeader(frame, kind, conn, seq, stamp_ns, payload_len);
    frame.resize(kHeaderBytes + payload_len);
    FillPayload(frame.data() + kHeaderBytes, conn, seq, payload_len);
    return frame;
}

inline bool FrameParser::Feed(const char* data, std::size_t size)
{
    if (m_bad)
        return false;
    m_buf.append(data, size);
    return true;
}

inline bool FrameParser::HasFrame()
{
    if (m_bad || m_buf.size() < kHeaderBytes)
        return false;
    if (m_frame_total == 0) {
        std::uint32_t magic = 0;
        std::memcpy(&magic, m_buf.data(), 4);
        if (magic != kMagic) {
            m_bad = true;
            return false;
        }
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
        if (len > 64u * 1024u * 1024u) {
            m_bad = true;
            return false;
        }
        m_len = len;
        m_frame_total = kHeaderBytes + static_cast<std::size_t>(len);
    }
    return m_buf.size() >= m_frame_total;
}

inline std::string FrameParser::TakeFrame()
{
    std::string frame = m_buf.substr(0, m_frame_total);
    m_buf.erase(0, m_frame_total);
    m_frame_total = 0;
    return frame;
}

inline bool FrameParser::VerifyCurrent(const std::string& frame) const
{
    return VerifyPayload(frame.data() + kHeaderBytes, m_conn, m_seq, m_len);
}

inline Args
ParseArgs(int argc, char** argv, const std::vector<std::string>& required, const std::vector<std::string>& optional)
{
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
        const bool known = std::find(required.begin(), required.end(), entry.first) != required.end()
                           || std::find(optional.begin(), optional.end(), entry.first) != optional.end();
        if (!known) {
            args.error = "未知参数 --" + entry.first;
            return args;
        }
    }
    args.ok = true;
    return args;
}

inline std::uint64_t ArgU64(const Args& args, const std::string& key, std::uint64_t dflt)
{
    const auto it = args.kv.find(key);
    if (it == args.kv.end())
        return dflt;
    try {
        std::size_t used = 0;
        const auto value = std::stoull(it->second, &used);
        if (used != it->second.size())
            return dflt;
        return value;
    } catch (...) {
        return dflt;
    }
}

inline std::string ArgStr(const Args& args, const std::string& key, const std::string& dflt)
{
    const auto it = args.kv.find(key);
    return it == args.kv.end() ? dflt : it->second;
}

inline std::int64_t NowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

inline ProcSample ReadProcSelf()
{
    ProcSample s;
    s.at_ns = NowNs();
    std::ifstream stat("/proc/self/stat");
    std::string line;
    if (!std::getline(stat, line))
        return s;
    const auto rparen = line.rfind(')');
    if (rparen == std::string::npos)
        return s;
    std::istringstream rest(line.substr(rparen + 1));
    std::string token;
    std::vector<std::string> fields;
    while (rest >> token)
        fields.push_back(token);
    if (fields.size() <= 21)
        return s;
    try {
        s.utime_ticks = std::stoull(fields[11]);
        s.stime_ticks = std::stoull(fields[12]);
        const long long rss = std::stoll(fields[21]);
        s.rss_pages = rss < 0 ? 0 : static_cast<std::uint64_t>(rss);
        s.ok = true;
    } catch (...) {
        s.ok = false;
    }
    return s;
}

inline long TicksPerSecond()
{
    static const long v = ::sysconf(_SC_CLK_TCK);
    return v > 0 ? v : 100;
}

inline long PageBytes()
{
    static const long v = ::sysconf(_SC_PAGESIZE);
    return v > 0 ? v : 4096;
}

inline void ProcObserver::Start(std::chrono::milliseconds interval)
{
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
                while (s.rss_pages > cur && !m_rss_max.compare_exchange_weak(cur, s.rss_pages)) {
                }
            }
            std::this_thread::sleep_for(m_interval);
        }
    });
}

inline void ProcObserver::Stop()
{
    if (m_running.exchange(false) && m_thread.joinable())
        m_thread.join();
    const auto end = ReadProcSelf();
    m_end = end;
    if (!end.ok || !m_start.ok)
        m_proc_ok.store(false);
}

inline double ProcObserver::CpuSeconds() const
{
    if (!m_start.ok)
        return 0.0;
    const ProcSample end = m_end.ok ? m_end : ReadProcSelf();
    if (!end.ok)
        return 0.0;
    const auto ticks = (end.utime_ticks + end.stime_ticks) - (m_start.utime_ticks + m_start.stime_ticks);
    return static_cast<double>(ticks) / static_cast<double>(TicksPerSecond());
}

inline std::uint64_t ProcObserver::RssMaxBytes() const
{
    const auto pages = std::max({m_rss_max.load(), m_end.ok ? m_end.rss_pages : 0});
    return pages * static_cast<std::uint64_t>(PageBytes());
}

inline std::uint64_t ProcObserver::RssEndBytes() const
{
    const ProcSample end = m_end.ok ? m_end : ReadProcSelf();
    return end.ok ? end.rss_pages * static_cast<std::uint64_t>(PageBytes()) : 0;
}

inline std::uint64_t ProcObserver::RssStartBytes() const
{
    return m_start.ok ? m_start.rss_pages * static_cast<std::uint64_t>(PageBytes()) : 0;
}

inline void Json::Key()
{
    if (!first)
        out += ',';
    first = false;
}

inline Json& Json::K(const char* k)
{
    Key();
    out += '"';
    out += k;
    out += "\":";
    return *this;
}

inline Json& Json::V(const std::string& s)
{
    out += '"';
    for (char c : s) {
        if (c == '"' || c == '\\')
            out += '\\';
        if (c == '\n') {
            out += "\\n";
            continue;
        }
        if (c == '\r') {
            out += "\\r";
            continue;
        }
        if (c == '\t') {
            out += "\\t";
            continue;
        }
        out += c;
    }
    out += '"';
    return *this;
}

inline Json& Json::V(double v)
{

    /**
     * NaN → null（不伪造 0）。
     */
    if (v != v) {
        out += "null";
        return *this;
    }
    std::ostringstream os;
    os << std::setprecision(9) << v;
    out += os.str();
    return *this;
}

inline std::string Json::Done()
{
    out += '}';
    return out;
}

inline double Quantile(const std::vector<double>& sorted, double q)
{
    if (sorted.empty())
        return 0.0;
    const std::size_t idx = static_cast<std::size_t>((sorted.size() - 1) * q + 0.5);
    return sorted[std::min(idx, sorted.size() - 1)];
}

// clang-format off
}
// clang-format on
