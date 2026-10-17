#pragma once

#include <cstddef>
#include <deque>
#include <exception>
#include <functional>
#include <mutex>
#include <optional>
#include <system_error>
#include <thread>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

namespace bbt::infra::net {

enum class SubmitStatus { Accepted, Full, Sealed, NotStarted, Invalid, Failed, NotSubmitted };

// 私有单 owner driver。命令/Asio callback 必须短且非阻塞；不负责业务任务调度。
// Stop 仅封口并 drain：调用者须先在 owner 上取消/收口自己登记的 Asio 资源，
// 否则 Join 会等待这些资源完成。控制线程独占 Start/Join/析构；不可在 owner 析构。
class EventLoop {
public:
    using Command = std::function<void()>;
    explicit            EventLoop(std::size_t capacity, std::size_t batch_size = 16);
                        ~EventLoop();
    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;
    bool                Start();
    SubmitStatus        Submit(Command command);
    // 仅报告本次 push/post 失败；非 Failed 结果清空 submit_error。
    SubmitStatus        Submit(Command command, std::error_code& submit_error);
    void                RequestStop();
    bool                Join(); // owner 返回 false，不 join 自身；其余控制线程同步回收。
    bool                IsOwner() const;
    boost::asio::io_context& Context(); // 只许 owner 登记/取消；不得跨线程保存使用。
    const void*         ContextIdentity() const { return &m_context; }
    std::thread::id      OwnerId() const;
    std::size_t         FailureCount() const;
    std::exception_ptr  LastFailure() const;

private:
    void                Drain();
    void                RecordFailure(std::exception_ptr error);
    const std::size_t   m_capacity;
    const std::size_t   m_batch_size;
    boost::asio::io_context m_context;
    using Guard = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;
    std::optional<Guard> m_guard;
    mutable std::mutex  m_mutex;
    std::deque<Command> m_commands;
    bool                m_started{false};
    bool                m_sealed{false};
    bool                m_scheduled{false};
    std::thread::id     m_owner_id;
    std::size_t         m_failures{0};
    std::exception_ptr  m_last_failure;
    std::thread         m_thread;
};

} // namespace bbt::infra::net
