#pragma once

/**
 * infra-perf 闭环的 bench 共享头部（测试私有，不发布、不进入公共接口面）。
 * 统一：确定性小端 wire 帧、序列校验、argv 解析、/proc 进程统计采样（有界）、
 * 紧凑 JSON 发射。所有 benchmark 进程只写结构化行到 stdout（协议帧与最终 JSON），
 * 诊断/噪声一律走 stderr 由 driver 捕获并落盘，不产生 per-request stdout。
 * 实现（含 inline 成员定义）见末尾 include 的 __Tperf_common.hpp。
 */
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

/**
 * @name wire 帧（确定性，两进程同协议）
 * @{
 * 'B','B','T','0' magic + kind(1) + conn(u64 LE) + seq(u64 LE) + stamp_ns(u64 LE)
 * + payload_len(u32 LE) + payload
 * kind: 'D' data / 'F' fin（单向传输侧告知对端“写完即等 EOF/关”）
 * conn: 连接内局部序号；seq: 连接内单调帧序号；stamp_ns: 发送端 steady ns（时延）。
 * "BBT0" LE
 */

inline constexpr std::uint32_t kMagic = 0x30544242u;
inline constexpr std::uint8_t kKindData = 'D';
inline constexpr std::uint8_t kKindFin = 'F';
inline constexpr std::size_t kHeaderBytes = 4 + 1 + 8 + 8 + 8 + 4;

inline void AppendHeader(
    std::string& out,
    std::uint8_t kind,
    std::uint64_t conn,
    std::uint64_t seq,
    std::int64_t stamp_ns,
    std::uint32_t payload_len);

/**
 * 确定性伪随机 payload：(conn,seq,word_index) 的 xorshift——两端可独立重算校验
 * 内容，不依赖对端任何状态；内容错误/截断即 fail（不静默吞错）。按 8 字节 word
 * 生成以支撑大 payload 的生成/校验吞吐。
 */
inline std::uint64_t PayloadWord(std::uint64_t conn, std::uint64_t seq, std::uint64_t word_index);

inline void FillPayload(char* dst, std::uint64_t conn, std::uint64_t seq, std::uint32_t len);

inline bool VerifyPayload(const char* src, std::uint64_t conn, std::uint64_t seq, std::uint32_t len);

inline std::string MakeFrame(
    std::uint64_t conn,
    std::uint64_t seq,
    std::int64_t stamp_ns,
    std::uint32_t payload_len,
    std::uint8_t kind = kKindData);

/**
 * 解析器：流式拆帧，跨 TryRead 边界保持半帧状态（不用 substr 拷贝整帧）。
 * Feed 返回消费字节数；frame_ready()/TakeFrame 取出完整帧。magic/长度错 → bad()。
 */
class FrameParser {
public:
    /**
     * 追加数据；返回 false 表示协议级错误（调用方应关闭连接并计 error）。
     */
    bool Feed(const char* data, std::size_t size);

    bool bad() const { return m_bad; }

    /**
     * 有完整帧可读
     */
    bool HasFrame();

    /**
     * 取出已就绪帧；payload 字段为 frame[kHeaderBytes..)。HasFrame 为假调用未定义。
     */
    std::string TakeFrame();

    std::uint8_t kind() const { return m_kind; }

    std::uint64_t conn() const { return m_conn; }

    std::uint64_t seq() const { return m_seq; }

    std::int64_t stamp_ns() const { return m_stamp; }

    std::uint32_t payload_len() const { return m_len; }

    /**
     * 校验当前帧 payload（帧未 Take 前）——用 frame 数据直接核验，避免二次拷贝。
     */
    bool VerifyCurrent(const std::string& frame) const;

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

/**
 * @}
 * @name argv 解析（fail-closed，未知参数即拒绝）
 * @{
 */

struct Args {
    std::map<std::string, std::string> kv;
    bool ok{false};
    std::string error;
};

inline Args
ParseArgs(int argc, char** argv, const std::vector<std::string>& required, const std::vector<std::string>& optional);

inline std::uint64_t ArgU64(const Args& args, const std::string& key, std::uint64_t dflt);

inline std::string ArgStr(const Args& args, const std::string& key, const std::string& dflt = "");

inline std::int64_t NowNs();

/**
 * @}
 * @name /proc 自身统计（采样有界，driver 归并）
 * @{
 */

struct ProcSample {
    std::int64_t at_ns{0};
    std::uint64_t utime_ticks{0};
    std::uint64_t stime_ticks{0};
    std::uint64_t rss_pages{0};
    bool ok{false};
};

inline ProcSample ReadProcSelf();

inline long TicksPerSecond();

inline long PageBytes();

/**
 * 进程 CPU/内存观测器：线程内以固定间隔采样自身（对被观测进程零侵入——
 * 只读 /proc/self，不引入第三方采样器）。Start() 时 /proc 不可用即记录
 * proc_ok=false：进程必须在最终 JSON 如实上报 proc_ok=false（driver 视该
 * 运行为无效测量），不得静默退化成 0 值冒充成功。
 * cpu_per_op 用窗口 utime+stime 增量 / 完成数；rss_max_bytes 是窗口内 max RSS
 * （显式标注 max，与 end/steady 分开，不混计）。
 */
class ProcObserver {
public:
    void Start(std::chrono::milliseconds interval);

    void Stop();

    ~ProcObserver() { Stop(); }

    /**
     * /proc 在 start/stop 两个端点都可用才算测量有效（中间采样缺失只影响
     * max 精度，仍如实上报 proc_ok=true）。
     */
    bool ProcOk() const { return m_proc_ok.load(); }

    double CpuSeconds() const;

    std::uint64_t RssMaxBytes() const;

    std::uint64_t RssEndBytes() const;

    std::uint64_t RssStartBytes() const;

private:
    std::chrono::milliseconds m_interval{50};
    ProcSample m_start, m_end, m_last;
    std::atomic<bool> m_running{false};
    std::atomic<bool> m_proc_ok{false};
    std::atomic<std::uint64_t> m_rss_max{0};
    std::thread m_thread;
};

/**
 * @}
 * @name 紧凑 JSON 发射（无第三方，仅 std::string 拼接）
 * @{
 */

struct Json {
    std::string out{"{"};
    bool first{true};

    void Key();

    Json& K(const char* k);

    Json& V(const std::string& s);

    Json& V(const char* s) { return V(std::string(s ? s : "")); }

    Json& V(std::uint64_t v)
    {
        out += std::to_string(v);
        return *this;
    }

    Json& V(std::int64_t v)
    {
        out += std::to_string(v);
        return *this;
    }

    Json& V(int v)
    {
        out += std::to_string(v);
        return *this;
    }

    Json& V(bool v)
    {
        out += v ? "true" : "false";
        return *this;
    }

    /**
     * NaN → null（不伪造 0）。
     */
    Json& V(double v);

    std::string Done();
};

/**
 * @}
 * 分位数：要求传入已 sort 的有界 vector（样本有界，不存无限流）。
 */
inline double Quantile(const std::vector<double>& sorted, double q);

// clang-format off
}
// clang-format on

#include "__Tperf_common.hpp"