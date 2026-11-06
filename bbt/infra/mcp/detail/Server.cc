#include <bbt/infra/mcp/detail/Server.hpp>

namespace bbt::infra::mcp {

RequestOwner::RequestOwner(RequestId id) : request_id(std::move(id))
{
}

bool RequestOwner::TryFinish(Terminal kind) noexcept
{
    int expected = static_cast<int>(Terminal::kNone);
    if (!terminal.compare_exchange_strong(expected, static_cast<int>(kind), std::memory_order_acq_rel,
                                          std::memory_order_acquire))
        return false; // 已有终态：本触发源竞争失败，只消费空壳
    abort->store(true, std::memory_order_release);
    // 仅内测计数（非公共面）：恰好一次终态的直接观测（S21）；生产开销为每请求
    // 一次原子自增，聚合的是「终态总数 = 落定请求数」这一进程级不变项。
    switch (kind) {
    case Terminal::kCompleted:
        s_test_completed.fetch_add(1, std::memory_order_relaxed);
        break;
    case Terminal::kCancelled:
        s_test_cancelled.fetch_add(1, std::memory_order_relaxed);
        break;
    case Terminal::kTimedOut:
        s_test_timed_out.fetch_add(1, std::memory_order_relaxed);
        break;
    case Terminal::kClosed:
        s_test_closed.fetch_add(1, std::memory_order_relaxed);
        break;
    case Terminal::kNone:
        break;
    }
    return true;
}

bool RequestOwner::DeadlinePassed() const noexcept
{
    return deadline.time_since_epoch().count() != 0 && std::chrono::steady_clock::now() >= deadline;
}

long RequestOwner::TestCount(Terminal kind) noexcept
{
    switch (kind) {
    case Terminal::kCompleted:
        return s_test_completed.load(std::memory_order_relaxed);
    case Terminal::kCancelled:
        return s_test_cancelled.load(std::memory_order_relaxed);
    case Terminal::kTimedOut:
        return s_test_timed_out.load(std::memory_order_relaxed);
    case Terminal::kClosed:
        return s_test_closed.load(std::memory_order_relaxed);
    case Terminal::kNone:
        return 0;
    }
    return 0;
}

void RequestOwner::TestResetCounters() noexcept
{
    s_test_completed.store(0, std::memory_order_relaxed);
    s_test_cancelled.store(0, std::memory_order_relaxed);
    s_test_timed_out.store(0, std::memory_order_relaxed);
    s_test_closed.store(0, std::memory_order_relaxed);
}

McpServer::Impl::Admission::Admission(Admission&& other) noexcept : m_server(other.m_server)
{
    other.m_server = nullptr;
}

McpServer::Impl::Admission::~Admission()
{
    if (m_server)
        m_server->Release();
}

void McpSession::SetNotificationSink(NotificationSink sink)
{
    std::lock_guard<std::mutex> lock(m_sink_mutex);
    m_sink = std::move(sink);
}

bool McpSession::CopyNotificationSink(NotificationSink& out) const
{
    std::lock_guard<std::mutex> lock(m_sink_mutex);
    out = m_sink;
    return static_cast<bool>(out);
}

void McpSession::EmitNotificationFrame(const std::string& frame)
{
    NotificationSink sink;
    {
        std::lock_guard<std::mutex> lock(m_sink_mutex);
        sink = m_sink;
    }
    if (sink)
        sink(frame);
}

bool McpSession::InitializeAccepted() const noexcept
{
    std::lock_guard<std::mutex> lock(m_state_mutex);
    return m_initialize_accepted;
}

bool McpSession::Initialized() const noexcept
{
    std::lock_guard<std::mutex> lock(m_state_mutex);
    return m_initialized;
}

std::optional<std::string> McpSession::NegotiatedVersion() const
{
    std::lock_guard<std::mutex> lock(m_state_mutex);
    return m_negotiated_version;
}

JsonValue McpSession::ClientCapabilities() const
{
    std::lock_guard<std::mutex> lock(m_state_mutex);
    return m_client_capabilities;
}

void McpSession::AdoptLegacyHeaderGate(std::string negotiated_version)
{
    std::lock_guard<std::mutex> lock(m_state_mutex);
    if (m_initialize_accepted)
        return;
    m_initialize_accepted = true;
    m_negotiated_version = std::move(negotiated_version);
}

} // namespace bbt::infra::mcp
