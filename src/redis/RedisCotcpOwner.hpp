#pragma once
// B 级 owner binding 候选（co-io-adapter/v1 §8/§10 的「B：非阻塞 owner binding」形态）：
// hiredis 只负责 RESP2 命令编码与 reply 解码，CoTCP 独占 socket 的建立、字节
// 读写、等待、取消与关闭。
//
// 不变量（实现与测试逐条核对）：
//   1. 不出现 hiredis 的 context/async 形态：不创建 redisContext /
//      redisAsyncContext，不注册 ev 钩子，不接触裸 fd；只用
//      redisFormatCommandArgv 与 redisReader{Create,Feed,GetReply,Free}。
//   2. 每次 hiredis 调用都在调用协程栈内同步返回（纯内存变换，不阻塞、不等待）；
//      任何可能挂起的等待都发生在 CoTCP::DialTCP/ReadSome/WriteSome 内部，
//      即 hiredis 调用之外。调用次数与单次最长耗时经 SyncStats 上报。
//   3. fd 由 CoTCP 唯一拥有：本类从不 close/shutdown/read/write 裸 fd。
//   4. redisReader 由本类唯一拥有：构造时创建，析构时恰好释放一次。
//   5. RequestClose 覆盖在途 DialTCP：取消 dial 等待，且 dial 成功后若关闭请求
//      已发布则立即物理收口；关闭请求发布后本类不再编码、不再发送任何字节。
//
// 范围：单连接 / 单 owner 候选 —— 一个 RedisCotcpConn 对应一条真实连接，不覆盖
//   多连接池、流水线（max_inflight 不参与）或空闲断连探测。

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include <hiredis/hiredis.h>

#include <bbt/coroutine/sync/Cancellation.hpp>

#include <bbt/infra/CoTCP.hpp>
#include <bbt/infra/Result.hpp>

#include "redis/RedisDetail.hpp"

namespace bbt::infra::redis_detail {

// 单连接上的 hiredis 调用观测：同步性（单次最长耗时）与 I/O 轮数。
struct HiredisSyncStats {
    std::uint64_t calls{0};        // hiredis 同步调用次数
    std::uint64_t max_call_ns{0};  // 单次最长耗时（证明库内无等待）
    std::uint64_t write_rounds{0}; // WriteAllBytes 实际写次数（>1 即部分写/WouldBlock）
    std::uint64_t read_rounds{0};  // ReadReply 实际读次数
};

// 进程级计数（跨连接）：测试用于断言 reader 创建/释放恰好配对、无库内等待。
struct CotcpBindingCounters {
    std::atomic<std::uint64_t> readers_created{0};
    std::atomic<std::uint64_t> readers_freed{0};
    std::atomic<std::uint64_t> conns_created{0};
    std::atomic<std::uint64_t> conns_destroyed{0};
    std::atomic<std::uint64_t> hiredis_calls{0};
    std::atomic<std::uint64_t> hiredis_max_call_ns{0};
    std::atomic<std::uint64_t> commands_encoded{0};
    std::atomic<std::uint64_t> write_rounds{0};
    std::atomic<std::uint64_t> read_rounds{0};
    std::atomic<std::uint64_t> partial_write_ops{0}; // 出现 >1 轮的写操作数
    std::atomic<std::uint64_t> partial_read_ops{0};  // 出现 >1 轮的读操作数
};
CotcpBindingCounters& BindingCounters() noexcept;
void ResetBindingCounters() noexcept;

class RedisCotcpConn : public std::enable_shared_from_this<RedisCotcpConn> {
public:
    RedisCotcpConn(std::string host, std::uint16_t port);
    ~RedisCotcpConn();

    RedisCotcpConn(const RedisCotcpConn&) = delete;
    RedisCotcpConn& operator=(const RedisCotcpConn&) = delete;

    // ---- 以下全部必须在协程上下文调用（CoTCP 的入口门禁）----

    // 建连：CoTCP::DialTCP（DNS/connect/等待/取消全部由 CoTCP 承担）。
    result<void> Dial(const CallOptions& options);
    // RESP2 命令编码：hiredis 同步调用，无任何 I/O。
    static result<std::string> EncodeCommand(int argc, const char** argv,
                                             const size_t* argvlen);
    // 写满一条命令：部分写/WouldBlock 由 CoTCP 调和；等待只发生在 CoTCP 内部。
    result<void> WriteAllBytes(const std::string& bytes,
                               const CallOptions& options);
    // 读一条完整 RESP2 reply：CoTCP 读到字节 ↔ hiredis 同步喂入/解析交替进行。
    result<RawReply> ReadReply(const CallOptions& options);

    // 关闭（任意线程可调，幂等）：封口本连接 ——
    //   1) 发布关闭请求：此后 Dial/WriteAllBytes/ReadReply 一律拒绝，不再编码/发送；
    //   2) 取消在途 DialTCP 等待（dial 未发布 CoTCP 对象，取消源是唯一可打断点）；
    //   3) 转发 CoTCP::RequestClose（fd 唯一收口在 CoTCP）。
    void        RequestClose() noexcept;
    // 关闭请求已发布（尚不保证物理收口）。
    bool        CloseRequested() const noexcept {
        return m_close_requested.load(std::memory_order_acquire);
    }
    // 物理收口：无在途拨号，且没有未关闭的 CoTCP（fd 已关闭）。
    bool        IsClosed() const noexcept;
    CloseStatus WaitClosed(bbt::coroutine::Deadline          deadline,
                           bbt::coroutine::CancellationToken cancel);

    bool                    Dialed() const noexcept { return m_tcp != nullptr; }
    const tcp::CoTCP::SPtr& Transport() const noexcept { return m_tcp; }
    const HiredisSyncStats& SyncStats() const noexcept { return m_stats; }
    bool                    Broken() const noexcept { return m_broken; }
    void                    MarkBroken() noexcept { m_broken = true; }

private:
    void NoteCall(std::uint64_t ns) noexcept;

    std::string      m_host;
    std::uint16_t    m_port;
    // B1：m_tcp 由 owner 协程写入（Dial），RequestClose 可在任意线程读，故加锁。
    mutable std::mutex m_tcp_mtx;
    tcp::CoTCP::SPtr   m_tcp; // m_tcp_mtx
    // B1：关闭请求（任意线程发布）与在途拨号标志（IsClosed 的物理收口判定用）。
    std::atomic_bool m_close_requested{false};
    std::atomic_bool m_dialing{false};
    // B1：dial 等待可被 RequestClose 打断的取消源。
    std::shared_ptr<bbt::coroutine::CancellationSource> m_dial_cancel{
        std::make_shared<bbt::coroutine::CancellationSource>()};
    redisReader*     m_reader{nullptr};
    bool             m_broken{false};
    HiredisSyncStats m_stats;
    char             m_read_buf[8192];
};

} // namespace bbt::infra::redis_detail
