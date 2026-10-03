#pragma once
// B 级 owner binding 候选（co-io-adapter/v1 §8/§10「B：非阻塞 owner binding」形态）：
// hiredis 只负责 RESP2 命令编码与 reply 解码，CoTCP 独占 socket 的 Dial / 读写 /
// 等待 / 关闭。
//
// 与当前 main 契约对齐（同步 Close、无取消令牌、无 CompletionSignal）：
//   1. 不创建 redisContext/redisAsyncContext，不注册 ev 钩子、不接触裸 fd；只用
//      redisFormatCommandArgv 与 redisReader{Create,Feed,GetReply,Free}。
//   2. 每次 hiredis 调用都在调用协程栈内同步返回（纯内存变换，不阻塞、不等待）；
//      任何可能挂起的等待都发生在 CoTCP::DialTCP/ReadSome/WriteSome 内部，即
//      hiredis 调用之外。调用次数与单次最长耗时经 SyncStats 上报。
//   3. fd 由 CoTCP 唯一拥有：本类从不 close/shutdown/read/write 裸 fd。
//   4. redisReader 由本类唯一拥有：构造时创建，Close()/析构恰好释放一次。
//   5. Close() 同步、幂等、任意线程：封口（此后不再编码/发送/读）→ 物理关闭
//      CoTCP（fd 关闭，CoTCP 内唤醒挂起的 ReadSome/WriteSome）→ 释放 redisReader；
//      返回当刻本连接拥有的物理资源（fd、reader）已释放、后端不再访问。
//      reader 访问与 Close 的释放用同一把锁配对：owner 只在非挂起的短临界区内触碰
//      reader（解析/喂入/取 reply），Close 在该锁内摘除并释放，故不与在途 hiredis
//      调用并发；挂起只发生在锁外的 CoTCP 调用里。
//   6. close 请求覆盖在途 Dial：DialTCP 的 connect 等待段挂在本连接所属 owner 的
//      关闭唤醒登记上（owner 级 CloseWaiters），Close 封口即唤醒该等待段，Dial
//      随即走正常错误返回并自行释放候选 fd（若 dial 已成功则本类立即物理关闭）。
//
// 范围：单连接 / 单 owner —— 一个 RedisCotcpConn 对应一条真实连接，不覆盖多连接
//   池、流水线（max_inflight 不参与）或空闲断连探测；不重连（连接出错即不可复用）。

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include <hiredis/hiredis.h>

#include <bbt/infra/CoTCP.hpp>
#include <bbt/infra/Result.hpp>

#include "detail/IoSupport.hpp"
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

    // ---- 以下入口必须在协程上下文调用（CoTCP 的入口门禁）----

    // 建连：经 owner 装配面 CoTCP::DialTCP（DNS/connect/等待/关闭唤醒由 CoTCP 承担）。
    // dial_waiters 为本连接所属 owner 的关闭唤醒登记：Close() 封口它即可打断在途
    // connect 等待，使 Dial 尽快走错误返回（不再有取消令牌）。
    result<void> Dial(const CallOptions& options,
                      const std::shared_ptr<detail::CloseWaiters>& dial_waiters);
    // RESP2 命令编码：hiredis 同步调用，无任何 I/O。
    static result<std::string> EncodeCommand(int argc, const char** argv,
                                             const size_t* argvlen);
    // 写满一条命令：部分写/WouldBlock 由 CoTCP 调和；等待只发生在 CoTCP 内部。
    result<void> WriteAllBytes(const std::string& bytes,
                               const CallOptions& options);
    // 读一条完整 RESP2 reply：CoTCP 读到字节 ↔ hiredis 同步喂入/解析交替进行。
    result<RawReply> ReadReply(const CallOptions& options);

    // 同步物理收口（任意线程、幂等）：
    //   1) 发布关闭请求：此后 Dial/WriteAllBytes/ReadReply 一律拒绝，不再编码/发送；
    //   2) 物理关闭 CoTCP（fd 关闭，唤醒挂起的读写等待）；
    //   3) 释放 redisReader。
    // 返回当刻 fd 与 reader 均已释放；本类拥有者不再有独立收口入口。
    void        Close() noexcept;
    // 关闭请求已发布（尚不保证物理收口）。
    bool        CloseRequested() const noexcept {
        return m_close_requested.load(std::memory_order_acquire);
    }
    // 物理收口：关闭请求已发布，且（无 CoTCP 或 CoTCP 已物理关闭）。
    bool        IsClosed() const noexcept;
    // 构造期 reader 分配失败时，避免先发送命令再在 ReadReply 中暴露内部故障。
    bool        ReaderAvailable() const noexcept;

    bool                    Dialed() const noexcept;
    tcp::CoTCP::SPtr        Transport() const noexcept;
    const HiredisSyncStats& SyncStats() const noexcept { return m_stats; }
    bool                    Broken() const noexcept { return m_broken; }
    void                    MarkBroken() noexcept { m_broken = true; }

private:
    void NoteCall(std::uint64_t ns) noexcept;
    void FreeReader() noexcept;

    std::string      m_host;
    std::uint16_t    m_port;
    // m_tcp 由 owner 协程写入（Dial），Close 可在任意线程读，故加锁。
    mutable std::mutex m_tcp_mtx;
    tcp::CoTCP::SPtr   m_tcp; // m_tcp_mtx
    // 关闭请求（任意线程发布）。
    std::atomic_bool m_close_requested{false};
    // reader 所有权：m_reader_mtx 保护 m_reader；owner 仅在非挂起的短临界区持锁触碰。
    mutable std::mutex m_reader_mtx;
    redisReader*       m_reader{nullptr};   // m_reader_mtx
    bool               m_broken{false};
    HiredisSyncStats   m_stats;
    char               m_read_buf[8192];
};

} // namespace bbt::infra::redis_detail
