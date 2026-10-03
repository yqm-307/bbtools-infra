#include "redis/RedisCotcpOwner.hpp"

#include <hiredis/alloc.h>

#include <chrono>

#include "detail/TransportWiring.hpp"   // owner 装配面：受管 Dial 的关闭唤醒登记

namespace bbt::infra::redis_detail {

namespace {

CotcpBindingCounters g_counters;

std::uint64_t NowNs() noexcept {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

void BumpMax(std::atomic<std::uint64_t>& m, std::uint64_t v) noexcept {
    std::uint64_t cur = m.load(std::memory_order_relaxed);
    while (v > cur &&
           !m.compare_exchange_weak(cur, v, std::memory_order_relaxed)) {
    }
}

} // namespace

CotcpBindingCounters& BindingCounters() noexcept { return g_counters; }

void ResetBindingCounters() noexcept {
    g_counters.readers_created.store(0);
    g_counters.readers_freed.store(0);
    g_counters.conns_created.store(0);
    g_counters.conns_destroyed.store(0);
    g_counters.hiredis_calls.store(0);
    g_counters.hiredis_max_call_ns.store(0);
    g_counters.commands_encoded.store(0);
    g_counters.write_rounds.store(0);
    g_counters.read_rounds.store(0);
    g_counters.partial_write_ops.store(0);
    g_counters.partial_read_ops.store(0);
}

RedisCotcpConn::RedisCotcpConn(std::string host, std::uint16_t port)
    : m_host(std::move(host)), m_port(port) {
    // hiredis 只提供一个解析器实例；不创建 context、不接触 fd。
    m_reader = redisReaderCreate();
    if (m_reader != nullptr)
        g_counters.readers_created.fetch_add(1);
    g_counters.conns_created.fetch_add(1);
}

RedisCotcpConn::~RedisCotcpConn() {
    // redisReader 唯一收口：恰好释放一次（Close 已释放则为空操作）。
    FreeReader();
    g_counters.conns_destroyed.fetch_add(1);
    // 本类不 close 裸 fd：m_tcp 析构走 CoTCP 的物理关闭（恰好一次）。
}

void RedisCotcpConn::FreeReader() noexcept {
    redisReader* reader = nullptr;
    {
        std::lock_guard<std::mutex> lk(m_reader_mtx);
        reader   = m_reader;
        m_reader = nullptr;
    }
    if (reader != nullptr) {
        redisReaderFree(reader);
        g_counters.readers_freed.fetch_add(1);
    }
}

void RedisCotcpConn::NoteCall(std::uint64_t ns) noexcept {
    ++m_stats.calls;
    if (ns > m_stats.max_call_ns)
        m_stats.max_call_ns = ns;
    g_counters.hiredis_calls.fetch_add(1);
    BumpMax(g_counters.hiredis_max_call_ns, ns);
}

result<void> RedisCotcpConn::Dial(
    const CallOptions& options,
    const std::shared_ptr<detail::CloseWaiters>& dial_waiters) {
    if (m_close_requested.load(std::memory_order_acquire))
        return result<void>::err(
            MakeError(ErrorCode::Closed, "redis: connection close requested"));
    // 经 owner 装配面建立受管 dial：connect 等待段登记到本连接所属 owner 的关闭
    // 唤醒登记上，owner Close 封口即打断在途 connect（无取消令牌）。数值地址不
    // 走 DNS 等待段；DNS 等待段不可外部 Notify，只能由调用方 deadline 划定。
    detail::DialWaitOptions dial_wait;
    dial_wait.close_waiters = dial_waiters;
    auto tcp = detail::TransportWiring::DialTCP(m_host, m_port, options,
                                                dial_wait);
    if (!tcp) {
        // 失败路径由 CoTCP 自行 close 候选 fd：本连接不保留任何 socket。
        return result<void>::err(std::move(tcp).error());
    }
    {
        std::lock_guard<std::mutex> lk(m_tcp_mtx);
        m_tcp = std::move(tcp).value();
    }
    // 成功后复查封口：关闭请求先到则立即物理收口刚建立的 fd，调用方不得继续编码。
    if (m_close_requested.load(std::memory_order_acquire)) {
        Close();
        return result<void>::err(
            MakeError(ErrorCode::Closed, "redis: connection closed during dial"));
    }
    return result<void>::ok();
}

result<std::string> RedisCotcpConn::EncodeCommand(int argc, const char** argv,
                                                 const size_t* argvlen) {
    char*           target = nullptr;
    const long long len    = redisFormatCommandArgv(&target, argc, argv, argvlen);
    if (len < 0 || target == nullptr) {
        if (target != nullptr)
            hi_free(target);
        return result<std::string>::err(MakeError(
            ErrorCode::InternalError, "redis: RESP2 command encode failed"));
    }
    std::string bytes(target, static_cast<std::size_t>(len));
    hi_free(target); // 使用 hiredis 当前分配器释放编码缓冲
    g_counters.commands_encoded.fetch_add(1);
    return result<std::string>::ok(std::move(bytes));
}

result<void> RedisCotcpConn::WriteAllBytes(const std::string& bytes,
                                          const CallOptions& options) {
    if (m_close_requested.load(std::memory_order_acquire))
        return result<void>::err(
            MakeError(ErrorCode::Closed, "redis: connection close requested"));
    tcp::CoTCP::SPtr tcp;
    {
        std::lock_guard<std::mutex> lk(m_tcp_mtx);
        tcp = m_tcp;
    }
    if (!tcp)
        return result<void>::err(
            MakeError(ErrorCode::Closed, "redis: transport not dialed"));
    std::size_t sent  = 0;
    std::size_t round = 0;
    while (sent < bytes.size()) {
        if (std::chrono::steady_clock::now() >= options.deadline) {
            Error e = MakeError(ErrorCode::TimedOut,
                                "redis: request deadline expired");
            if (e.transferred_bytes < sent)
                e.transferred_bytes = sent;
            return result<void>::err(std::move(e));
        }
        // 每一轮发送前复查封口：关闭后不得再写字节。
        if (m_close_requested.load(std::memory_order_acquire)) {
            Error e = MakeError(ErrorCode::Closed,
                                "redis: connection close requested");
            if (e.transferred_bytes < sent)
                e.transferred_bytes = sent;
            return result<void>::err(std::move(e));
        }
        // WriteSome 内部消化 WouldBlock 并等待（等待发生在 hiredis 调用之外）。
        auto w = tcp->WriteSome(
            ConstBytes{bytes.data() + sent, bytes.size() - sent}, options);
        if (!w) {
            Error e             = std::move(w).error();
            // 已写字节数随错误回报（契约 §8.1：累计已发送量交回 owner，
            // 出错即令连接不可复用，不自动重发）。当前路径通过 WriteSome，
            // 其错误不会额外回报已写字节；保留 max 写法以防底层接口日后补充
            // 跨层计数时被本地累计量覆盖，避免复用半条 RESP2 帧。
            if (e.transferred_bytes < sent)
                e.transferred_bytes = sent;
            return result<void>::err(std::move(e));
        }
        if (w.value().bytes == 0)
            continue; // 阻塞版不返回 0 字节；防御性续等
        sent += w.value().bytes;
        ++round;
        m_stats.write_rounds = round;
        g_counters.write_rounds.fetch_add(1);
    }
    if (round > 1)
        g_counters.partial_write_ops.fetch_add(1);
    return result<void>::ok();
}

result<RawReply> RedisCotcpConn::ReadReply(const CallOptions& options) {
    if (m_close_requested.load(std::memory_order_acquire))
        return result<RawReply>::err(
            MakeError(ErrorCode::Closed, "redis: connection close requested"));
    tcp::CoTCP::SPtr tcp;
    {
        std::lock_guard<std::mutex> lk(m_tcp_mtx);
        tcp = m_tcp;
    }
    if (!tcp)
        return result<RawReply>::err(
            MakeError(ErrorCode::Closed, "redis: transport not dialed"));

    std::size_t round = 0;
    for (;;) {
        // 解析 hiredis reader：与本类 Close 的 reader 释放同锁配对。持锁区间不含
        // 挂起点（ReadSome 在锁外），故 Close 不会与在途 hiredis 调用并发。
        {
            std::lock_guard<std::mutex> lk(m_reader_mtx);
            if (m_reader == nullptr) {
                const bool closing =
                    m_close_requested.load(std::memory_order_acquire);
                return result<RawReply>::err(MakeError(
                    closing ? ErrorCode::Closed : ErrorCode::InternalError,
                    closing ? "redis: connection close requested"
                            : "redis: reply reader unavailable"));
            }
            void*      obj = nullptr;
            const auto t0  = NowNs();
            const int  rc  = redisReaderGetReply(m_reader, &obj);
            NoteCall(NowNs() - t0);
            if (rc == REDIS_ERR) {
                const char* why = m_reader->errstr[0] != '\0' ? m_reader->errstr
                                                              : "reader error";
                return result<RawReply>::err(MakeError(
                    ErrorCode::ProtocolError,
                    std::string("redis: RESP2 parse error: ") + why));
            }
            if (obj != nullptr) {
                auto* reply   = static_cast<redisReply*>(obj);
                auto  decoded = DecodeReply(reply); // 先取值语义快照
                freeReplyObject(reply);             // 再释放 hiredis 对象
                if (round > 1)
                    g_counters.partial_read_ops.fetch_add(1);
                return decoded;
            }
        }
        // 不完整：等 CoTCP 读到更多字节。等待点不在 hiredis 调用栈内。
        auto r = tcp->ReadSome(
            MutableBytes{m_read_buf, sizeof(m_read_buf)}, options);
        if (!r)
            return result<RawReply>::err(std::move(r).error());
        if (r.value().state == IoState::Eof)
            return result<RawReply>::err(MakeError(
                ErrorCode::TransportError, "redis: peer closed connection (EOF)"));
        if (r.value().bytes == 0)
            continue; // WouldBlock 防御：阻塞版不返回
        ++round;
        m_stats.read_rounds = round;
        g_counters.read_rounds.fetch_add(1);
        {
            std::lock_guard<std::mutex> lk(m_reader_mtx);
            if (m_reader == nullptr) {
                const bool closing =
                    m_close_requested.load(std::memory_order_acquire);
                return result<RawReply>::err(MakeError(
                    closing ? ErrorCode::Closed : ErrorCode::InternalError,
                    closing ? "redis: connection close requested"
                            : "redis: reply reader unavailable"));
            }
            const auto t1  = NowNs();
            const int  frc = redisReaderFeed(m_reader, m_read_buf, r.value().bytes);
            NoteCall(NowNs() - t1);
            if (frc != REDIS_OK) {
                const bool oom = m_reader->err == REDIS_ERR_OOM;
                return result<RawReply>::err(MakeError(
                    oom ? ErrorCode::InternalError : ErrorCode::ProtocolError,
                    oom ? "redis: RESP2 reader allocation failed"
                        : "redis: RESP2 reader feed failed"));
            }
        }
    }
}

void RedisCotcpConn::Close() noexcept {
    // 1) 封口：此后不再编码/发送/读（含 dial 返回后的复查）。
    m_close_requested.store(true, std::memory_order_release);
    // 2) 物理关闭 CoTCP：fd 关闭（恰好一次），并唤醒挂起在 CoTCP 等待上的读写。
    tcp::CoTCP::SPtr tcp;
    {
        std::lock_guard<std::mutex> lk(m_tcp_mtx);
        tcp = m_tcp;
    }
    if (tcp)
        tcp->Close();
    // 3) 释放 redisReader（与 owner 的 reader 访问同锁配对）。
    FreeReader();
}

bool RedisCotcpConn::IsClosed() const noexcept {
    if (!m_close_requested.load(std::memory_order_acquire))
        return false;
    std::lock_guard<std::mutex> lk(m_tcp_mtx);
    return !m_tcp || m_tcp->IsClosed();
}

bool RedisCotcpConn::ReaderAvailable() const noexcept {
    std::lock_guard<std::mutex> lk(m_reader_mtx);
    return m_reader != nullptr;
}

bool RedisCotcpConn::Dialed() const noexcept {
    std::lock_guard<std::mutex> lk(m_tcp_mtx);
    return static_cast<bool>(m_tcp);
}

tcp::CoTCP::SPtr RedisCotcpConn::Transport() const noexcept {
    std::lock_guard<std::mutex> lk(m_tcp_mtx);
    return m_tcp;
}

} // namespace bbt::infra::redis_detail
