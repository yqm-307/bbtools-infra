#include "redis/RedisCotcpOwner.hpp"

#include <chrono>
#include <cstdlib>

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
    // redisReader 唯一收口：恰好释放一次。
    if (m_reader != nullptr) {
        redisReaderFree(m_reader);
        m_reader = nullptr;
        g_counters.readers_freed.fetch_add(1);
    }
    g_counters.conns_destroyed.fetch_add(1);
    // 本类不 close 裸 fd：m_tcp 析构走 CoTCP::_CloseFd（物理关闭恰好一次）。
}

void RedisCotcpConn::NoteCall(std::uint64_t ns) noexcept {
    ++m_stats.calls;
    if (ns > m_stats.max_call_ns)
        m_stats.max_call_ns = ns;
    g_counters.hiredis_calls.fetch_add(1);
    BumpMax(g_counters.hiredis_max_call_ns, ns);
}

result<void> RedisCotcpConn::Dial(const CallOptions& options) {
    if (m_close_requested.load(std::memory_order_acquire))
        return result<void>::err(
            MakeError(ErrorCode::Closed, "redis: connection close requested"));
    // B1：RequestClose 覆盖在途 DialTCP。
    //   1) dial 等待同时监听本连接的取消源：RequestClose 可在任意线程打断
    //      connect/DNS 等待（否则 fd 会一直挂到调用方 deadline，WaitClosed 不收敛）；
    //   2) 成功后先发布 m_tcp 再复查封口：若关闭请求先到，立即物理收口并报
    //      Closed —— 调用方不得继续编码/发送。
    CallOptions dial_opt = options;
    dial_opt.cancel      = bbt::coroutine::CancellationToken::Combine(
        m_dial_cancel->Token(), options.cancel);
    m_dialing.store(true, std::memory_order_release);
    auto tcp = tcp::CoTCP::DialTCP(m_host, m_port, dial_opt);
    m_dialing.store(false, std::memory_order_release);
    if (!tcp) {
        // 失败路径由 _DialTcp 自行 close 候选 fd：本连接不保留任何 socket。
        return result<void>::err(std::move(tcp).error());
    }
    {
        std::lock_guard<std::mutex> lk(m_tcp_mtx);
        m_tcp = std::move(tcp).value();
    }
    if (m_close_requested.load(std::memory_order_acquire)) {
        RequestClose(); // 立即物理收口刚建立的 fd（m_inflight==0 ⇒ 同步 close）
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
            free(target);
        return result<std::string>::err(MakeError(
            ErrorCode::InternalError, "redis: RESP2 command encode failed"));
    }
    std::string bytes(target, static_cast<std::size_t>(len));
    free(target); // hiredis 用 hi_malloc 分配，默认分配器即 free
    g_counters.commands_encoded.fetch_add(1);
    return result<std::string>::ok(std::move(bytes));
}

result<void> RedisCotcpConn::WriteAllBytes(const std::string& bytes,
                                          const CallOptions& options) {
    // B1：关闭请求发布后不得再发送（dial 期间到达的关闭同样在此生效）。
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
        // B1：每一轮发送前复查封口：关闭后不得再写字节。
        if (m_close_requested.load(std::memory_order_acquire)) {
            Error e = MakeError(ErrorCode::Closed,
                                "redis: connection close requested");
            e.transferred_bytes = sent;
            return result<void>::err(std::move(e));
        }
        // WriteSome 内部消化 WouldBlock 并等待（等待发生在 hiredis 调用之外）。
        auto w = tcp->WriteSome(
            ConstBytes{bytes.data() + sent, bytes.size() - sent}, options);
        if (!w) {
            Error e             = std::move(w).error();
            // 已写字节数随错误回报（契约 §8.1：累计已发送量交回 owner，
            // 出错即令连接不可复用，不自动重发）。
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
    if (m_reader == nullptr)
        return result<RawReply>::err(MakeError(ErrorCode::InternalError,
                                              "redis: RESP2 reader unavailable"));

    std::size_t round = 0;
    for (;;) {
        void*      obj = nullptr;
        const auto t0  = NowNs();
        // hiredis 同步解析：只消费已喂入的字节，无 I/O、不阻塞。
        const int rc = redisReaderGetReply(m_reader, &obj);
        NoteCall(NowNs() - t0);
        if (rc == REDIS_ERR) {
            const char* why =
                m_reader->errstr[0] != '\0' ? m_reader->errstr : "reader error";
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
        const auto t1  = NowNs();
        const int  frc = redisReaderFeed(m_reader, m_read_buf, r.value().bytes);
        NoteCall(NowNs() - t1);
        if (frc != REDIS_OK)
            return result<RawReply>::err(MakeError(
                ErrorCode::ProtocolError, "redis: RESP2 reader feed failed"));
    }
}

void RedisCotcpConn::RequestClose() noexcept {
    // B1：先封口、再打断在途拨号、最后转发 CoTCP（fd 唯一收口）。全路径幂等。
    m_close_requested.store(true, std::memory_order_release);
    m_dial_cancel->RequestCancel();
    tcp::CoTCP::SPtr tcp;
    {
        std::lock_guard<std::mutex> lk(m_tcp_mtx);
        tcp = m_tcp;
    }
    if (tcp)
        tcp->RequestClose();
}

bool RedisCotcpConn::IsClosed() const noexcept {
    // 在途拨号：候选 fd 尚未纳入 CoTCP 管理，不能算作物理收口。
    if (m_dialing.load(std::memory_order_acquire))
        return false;
    std::lock_guard<std::mutex> lk(m_tcp_mtx);
    return m_tcp == nullptr || m_tcp->IsClosed();
}

CloseStatus RedisCotcpConn::WaitClosed(bbt::coroutine::Deadline          deadline,
                                      bbt::coroutine::CancellationToken cancel) {
    if (m_tcp == nullptr)
        return CloseStatus::Closed;
    return m_tcp->WaitClosed(deadline, std::move(cancel));
}

} // namespace bbt::infra::redis_detail
