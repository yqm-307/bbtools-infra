#include <bbt/infra/net/detail/EventLoop.hpp>

#include <stdexcept>
#include <new>
#include <utility>

#include <boost/asio/post.hpp>

namespace bbt::infra::net {

EventLoop::EventLoop(std::size_t capacity, std::size_t batch_size)
    : m_capacity(capacity), m_batch_size(batch_size) {
    if (capacity == 0 || batch_size == 0) {
        throw std::invalid_argument("EventLoop capacity/batch must be positive");
    }
}

EventLoop::~EventLoop() {
    if (IsOwner()) { std::terminate(); }
    RequestStop();
    Join();
}

bool EventLoop::Start() {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_started || m_sealed) { return false; }
    m_guard.emplace(m_context.get_executor());
    try {
        m_thread = std::thread([this] {
            {
                std::lock_guard<std::mutex> owner_lock(m_mutex);
                m_owner_id = std::this_thread::get_id();
            }
            // Asio handler 抛出只退出该次 run；记录后继续推进其余接纳工作。
            for (;;) {
                try { m_context.run(); break; }
                catch (...) { RecordFailure(std::current_exception()); }
            }
        });
    } catch (...) {
        m_guard.reset();
        throw;
    }
    m_started = true;
    return true;
}

SubmitStatus EventLoop::Submit(Command command) {
    std::error_code ignored;
    return Submit(std::move(command), ignored);
}

SubmitStatus EventLoop::Submit(Command command, std::error_code& submit_error) {
    submit_error.clear();
    if (!command) { return SubmitStatus::Invalid; }
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_sealed) { return SubmitStatus::Sealed; }
    if (!m_started) { return SubmitStatus::NotStarted; }
    if (m_commands.size() >= m_capacity) { return SubmitStatus::Full; }
    try {
        m_commands.push_back(std::move(command));
        if (!m_scheduled) {
            try { boost::asio::post(m_context, [this] { Drain(); }); }
            catch (...) { m_commands.pop_back(); throw; }
            m_scheduled = true;
        }
    } catch (const std::bad_alloc&) {
        submit_error = std::make_error_code(std::errc::not_enough_memory);
        m_last_failure = std::current_exception();
    } catch (...) {
        submit_error = std::make_error_code(std::errc::io_error);
        m_last_failure = std::current_exception();
    }
    if (submit_error) {
        ++m_failures;
        return SubmitStatus::Failed;
    }
    return SubmitStatus::Accepted;
}

void EventLoop::RequestStop() {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_sealed = true;
    // 已接纳队列有唯一 drain 链，不 stop context、不丢弃命令或在途回调。
    if (!m_scheduled) { m_guard.reset(); }
}

bool EventLoop::Join() {
    if (IsOwner()) { return false; }
    if (m_thread.joinable()) { m_thread.join(); }
    return true;
}

void EventLoop::Drain() {
    for (;;) {
        for (std::size_t count = 0; count < m_batch_size; ++count) {
            Command command;
            {
                std::lock_guard<std::mutex> lock(m_mutex);
                if (m_commands.empty()) { break; }
                command = std::move(m_commands.front());
                m_commands.pop_front();
            }
            try { command(); }
            catch (...) { RecordFailure(std::current_exception()); }
        }
        std::lock_guard<std::mutex> lock(m_mutex);
        if (m_commands.empty()) {
            m_scheduled = false;
            if (m_sealed) { m_guard.reset(); }
            return;
        }
        try {
            boost::asio::post(m_context, [this] { Drain(); });
            return;
        } catch (...) {
            // 分配失败仍兑现已接纳命令；仅异常降级为连续 drain，不遗失工作。
            ++m_failures;
            m_last_failure = std::current_exception();
        }
    }
}

bool EventLoop::IsOwner() const { return OwnerId() == std::this_thread::get_id(); }
std::thread::id EventLoop::OwnerId() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_owner_id;
}
boost::asio::io_context& EventLoop::Context() {
    if (!IsOwner()) { throw std::logic_error("EventLoop context is owner-only"); }
    return m_context;
}
void EventLoop::RecordFailure(std::exception_ptr error) {
    std::lock_guard<std::mutex> lock(m_mutex);
    ++m_failures;
    m_last_failure = std::move(error);
}
std::size_t EventLoop::FailureCount() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_failures;
}
std::exception_ptr EventLoop::LastFailure() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_last_failure;
}

} // namespace bbt::infra::net
