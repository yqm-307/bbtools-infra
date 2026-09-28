#include <bbt/infra/CoTCP.hpp>
#include <arpa/inet.h>

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <limits>

#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/sync/Cancellation.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/DnsResolver.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>

#include <functional>
#include <unordered_set>

#include "detail/IoSupport.hpp"
#include "detail/TransportWiring.hpp"

namespace bbt::infra::tcp {
namespace {

using bbt::coroutine::sync::CombinedWaitOptions;
using bbt::coroutine::sync::CombinedWaitStatus;

std::atomic<std::uint64_t> g_next_object_id{1};

int _SetNonBlocking(int fd) {
    const int flags = ::fcntl(fd, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0)
        return -1;
    return 0;
}

Error _Errno(ErrorCode code, const char* message) {
    Error error = MakeError(code, message);
    error.backend_category = "errno";
    error.backend_code = errno;
    return error;
}

Error _GaiError(const char* message, int code) {
    Error error = MakeError(ErrorCode::TransportError, message);
    error.backend_category = "dns";
    error.backend_code = code;
    return error;
}

struct _DnsResolution {
    std::string host;
    std::string service;
    addrinfo hints{};
    addrinfo* results{nullptr};
    int code{EAI_FAIL};
    bool abandoned{false};
    std::mutex mutex;
};

result<addrinfo*> _ResolveHost(const std::string& host, std::uint16_t port,
                               const CallOptions& options,
                               std::function<void()> on_registered = {},
                               bbt::coroutine::CancellationToken extra_cancel = {}) {
    auto state = std::make_shared<_DnsResolution>();
    state->host = host;
    state->service = std::to_string(port);
    state->hints.ai_socktype = SOCK_STREAM;
    state->hints.ai_family = AF_UNSPEC;
    bbt::coroutine::sync::CombinedWaitOptions wait;
    wait.deadline = options.deadline;
    // Issue #32 F2：runtime 关闭源与调用方 cancel 合成 OR 视图——受管
    // DialTCP 等待段期间 runtime->RequestClose() 能唤醒本等待返回
    // Closed/Cancelled 终态，而不是让协程留在 parked 表里被硬销毁。
    wait.cancel = bbt::coroutine::CancellationToken::Combine(
        extra_cancel, options.cancel);
    // 阻塞 libc 调用只在上游 DNS worker 上运行；state 延寿至 worker 完成。
    // 确定性等待入口证据：AwaitBounded 把 job 入队发生在协程挂起落定后
    // （on_registered 回调内），worker 开始执行 work 即证明协程已在等待。
    // 因此在 work lambda 顶部触发可选接缝即可观察「dial 处于 DNS 等待」。
    const auto status =
        bbt::coroutine::detail::DnsResolver::GetInstance()->AwaitBounded(
            [state, on_registered = std::move(on_registered)]() {
        if (on_registered)
            on_registered();   // worker 已起跑 ⇒ 调用方协程已挂起在等待
        addrinfo* resolved = nullptr;
        const int code = ::getaddrinfo(state->host.c_str(), state->service.c_str(),
                                       &state->hints, &resolved);
        addrinfo* discard = nullptr;
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->code = code;
            state->results = resolved;
            if (state->abandoned) {
                discard = state->results;
                state->results = nullptr;
            }
        }
        if (discard != nullptr)
            ::freeaddrinfo(discard);
        }, wait);
    if (status != bbt::coroutine::sync::CombinedWaitStatus::Completed) {
        addrinfo* discard = nullptr;
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            state->abandoned = true;
            if (state->results != nullptr) {
                discard = state->results;
                state->results = nullptr;
            }
        }
        if (discard != nullptr)
            ::freeaddrinfo(discard);

        Error error = MakeError(ErrorCode::RuntimeUnavailable,
                                "DNS resolution wait unavailable");
        if (status == bbt::coroutine::sync::CombinedWaitStatus::TimedOut)
            error = MakeError(ErrorCode::TimedOut, "DNS resolution timed out");
        else if (status == bbt::coroutine::sync::CombinedWaitStatus::Cancelled)
            error = MakeError(ErrorCode::Cancelled, "DNS resolution cancelled");
        error.backend_category = "dns";
        return result<addrinfo*>::err(std::move(error));
    }

    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->code != 0)
        return result<addrinfo*>::err(_GaiError(
            "TCP address resolution failed", state->code));
    return result<addrinfo*>::ok(state->results);
}

// co-io-adapter/v1 §4.0.1.3：数值地址直接进入非阻塞 connect，
// 不允许落到 getaddrinfo（DNS 属于 transport 接管的阻塞路径）。
bool _TryNumericAddress(const std::string& host, sockaddr_storage& out,
                        socklen_t& out_len) {
    sockaddr_in v4{};
    if (::inet_pton(AF_INET, host.c_str(), &v4.sin_addr) == 1) {
        v4.sin_family = AF_INET;
        v4.sin_port = 0; // port 由调用方填
        std::memcpy(&out, &v4, sizeof(v4));
        out_len = sizeof(v4);
        return true;
    }
    sockaddr_in6 v6{};
    if (::inet_pton(AF_INET6, host.c_str(), &v6.sin6_addr) == 1) {
        v6.sin6_family = AF_INET6;
        v6.sin6_port = 0;
        std::memcpy(&out, &v6, sizeof(v6));
        out_len = sizeof(v6);
        return true;
    }
    return false;
}

result<void> _WaitFd(int fd, bool readable, const CallOptions& options,
                     bbt::coroutine::CancellationToken close = {},
                     std::function<void()> on_registered = {}) {
    if (g_bbt_tls_coroutine_co == nullptr)
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
            "CoTCP operation must run in coroutine context"));

    auto waiter = bbt::coroutine::sync::CoWaiter::Create();
    CombinedWaitOptions wait;
    wait.fd = fd;
    wait.want_readable = readable;
    wait.want_writeable = !readable;
    wait.deadline = options.deadline;
    wait.cancel = bbt::coroutine::CancellationToken::Combine(close, options.cancel);
    const auto status = waiter->Wait(wait,
        [on_registered = std::move(on_registered)]() -> bool {
            if (on_registered) on_registered();
            return true;
        });
    switch (status) {
    case CombinedWaitStatus::FdReadable:
    case CombinedWaitStatus::FdWriteable:
        return result<void>::ok();
    case CombinedWaitStatus::Cancelled:
        if (close.IsCancellationRequested())
            return result<void>::err(MakeError(ErrorCode::Closed, "TCP socket closed"));
        return result<void>::err(MakeError(ErrorCode::Cancelled, "operation cancelled"));
    case CombinedWaitStatus::TimedOut:
        return result<void>::err(MakeError(ErrorCode::TimedOut, "operation timed out"));
    case CombinedWaitStatus::InvalidContext:
        return result<void>::err(MakeError(ErrorCode::InvalidContext, "invalid coroutine context"));
    default:
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "coroutine wait unavailable"));
    }
}

} // namespace

CoTCP::CoTCP(int fd) noexcept
    : m_fd(fd), m_info{g_next_object_id.fetch_add(1),
        bbt::coroutine::CurrentRuntimeGeneration(), "tcp", "CoTCP"} {}
CoTCP::~CoTCP() {
    std::lock_guard<std::mutex> lock(m_mtx);
    _DrainQuotaLocked();   // 未经 RequestClose 就析构时兜底归还（Stop 路径）
    _CloseFd();
}

bbt::coroutine::CoObjectInfo CoTCP::GetObjectInfo() const { return m_info; }

result<void> CoTCP::_CheckEntry(bool in_coroutine_only) const {
    if (in_coroutine_only && g_bbt_tls_coroutine_co == nullptr)
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
            "CoTCP operation must run in coroutine context"));
    const auto generation = bbt::coroutine::CurrentRuntimeGeneration();
    if (generation == 0 || generation != m_info.generation)
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "CoTCP operation belongs to another runtime generation"));
    return result<void>::ok();
}

IoResult CoTCP::TryReadSome(MutableBytes dst) {
    if (dst.size && !dst.data)
        return IoResult::err(MakeError(ErrorCode::InvalidArgument, "null TCP read buffer"));
    if (g_bbt_tls_coroutine_co == nullptr)
        return IoResult::err(MakeError(ErrorCode::InvalidContext, "TCP read requires coroutine"));
    if (bbt::coroutine::CurrentRuntimeGeneration() != m_info.generation)
        return IoResult::err(MakeError(ErrorCode::RuntimeUnavailable, "TCP runtime generation changed"));
    std::lock_guard<std::mutex> lock(m_mtx);
    if (m_closing)
        return IoResult::err(MakeError(ErrorCode::Closed, "TCP socket closed"));
    if (!dst.size)
        return IoResult::ok(IoProgress{IoState::Ok, 0});
    const ssize_t n = ::recv(m_fd, dst.data, dst.size, MSG_DONTWAIT);
    if (n > 0) return IoResult::ok(IoProgress{IoState::Ok, static_cast<std::size_t>(n)});
    if (n == 0) return IoResult::ok(IoProgress{IoState::Eof, 0});
    if (errno == EAGAIN || errno == EWOULDBLOCK)
        return IoResult::ok(IoProgress{IoState::WouldBlock, 0});
    return IoResult::err(_Errno(ErrorCode::TransportError, "TCP read failed"));
}

IoResult CoTCP::TryWriteSome(ConstBytes src) {
    if (src.size && !src.data)
        return IoResult::err(MakeError(ErrorCode::InvalidArgument, "null TCP write buffer"));
    if (g_bbt_tls_coroutine_co == nullptr)
        return IoResult::err(MakeError(ErrorCode::InvalidContext, "TCP write requires coroutine"));
    if (bbt::coroutine::CurrentRuntimeGeneration() != m_info.generation)
        return IoResult::err(MakeError(ErrorCode::RuntimeUnavailable, "TCP runtime generation changed"));
    std::lock_guard<std::mutex> lock(m_mtx);
    if (m_closing)
        return IoResult::err(MakeError(ErrorCode::Closed, "TCP socket closed"));
    if (!src.size)
        return IoResult::ok(IoProgress{IoState::Ok, 0});
    const ssize_t n = ::send(m_fd, src.data, src.size, MSG_DONTWAIT | MSG_NOSIGNAL);
    if (n >= 0) return IoResult::ok(IoProgress{IoState::Ok, static_cast<std::size_t>(n)});
    if (errno == EAGAIN || errno == EWOULDBLOCK)
        return IoResult::ok(IoProgress{IoState::WouldBlock, 0});
    return IoResult::err(_Errno(ErrorCode::TransportError, "TCP write failed"));
}

IoResult CoTCP::ReadSome(MutableBytes dst, const CallOptions& options) {
    auto read = _ReadSome(dst.data, dst.size, options);
    if (!read) return IoResult::err(std::move(read).error());
    return IoResult::ok(IoProgress{
        dst.size && read.value() == 0 ? IoState::Eof : IoState::Ok, read.value()});
}

IoResult CoTCP::WriteSome(ConstBytes src, const CallOptions& options) {
    auto written = _WriteSome(src.data, src.size, options);
    if (!written) return IoResult::err(std::move(written).error());
    return IoResult::ok(IoProgress{IoState::Ok, written.value()});
}

IoResult CoTCP::WriteAll(ConstBytes src, const CallOptions& options) {
    auto written = _WriteAll(src.data, src.size, options);
    if (!written) return IoResult::err(std::move(written).error());
    return IoResult::ok(IoProgress{IoState::Ok, written.value()});
}

result<CoTCP::SPtr> CoTCP::DialTCP(std::string host, std::uint16_t port,
                                   const CallOptions& options) {
    // 未托管入口：不装配取消源与挂起落定接缝，语义等同于默认构造的
    // DialWaitOptions（见 src/detail/TransportWiring.hpp）。
    return _DialTcp(std::move(host), port, options,
                    bbt::infra::detail::DialWaitOptions{});
}

result<CoTCP::SPtr> CoTCP::_DialTcp(
    std::string host, std::uint16_t port, const CallOptions& options,
    const bbt::infra::detail::DialWaitOptions& dial_wait) {
    sockaddr_storage numeric{};
    socklen_t numeric_len = 0;
    addrinfo* results = nullptr;
    if (_TryNumericAddress(host, numeric, numeric_len)) {
        if (numeric.ss_family == AF_INET)
            reinterpret_cast<sockaddr_in*>(&numeric)->sin_port = ::htons(port);
        else
            reinterpret_cast<sockaddr_in6*>(&numeric)->sin6_port = ::htons(port);
    } else {
        auto resolved = _ResolveHost(host, port, options,
                                     dial_wait.dns_on_registered,
                                     dial_wait.extra_cancel);
        if (!resolved)
            return result<SPtr>::err(std::move(resolved).error());
        results = std::move(resolved).value();
    }

    result<SPtr> output = result<SPtr>::err(
        MakeError(ErrorCode::Unavailable, "TCP connection failed"));
    int last_connect_error = 0;
    bool wait_failed = false;
    addrinfo* item = results;
    bool numeric_attempt = results == nullptr && numeric_len != 0;
    while (item != nullptr || numeric_attempt) {
        const sockaddr* address = item ? item->ai_addr
                                       : reinterpret_cast<const sockaddr*>(&numeric);
        const socklen_t address_len = item ? item->ai_addrlen : numeric_len;
        const int fd = ::socket(item ? item->ai_family : numeric.ss_family,
                                SOCK_STREAM, 0);
        if (fd < 0 || _SetNonBlocking(fd) != 0) {
            last_connect_error = errno;
            if (fd >= 0) ::close(fd);
            if (item != nullptr)
                item = item->ai_next;
            else
                numeric_attempt = false;
            continue;
        }
        // The coroutine Hook waits without our CallOptions deadline. This
        // transport owns the nonblocking syscall and CoWaiter retry loop.
        const int rc = ::syscall(SYS_connect, fd, address, address_len);
        if (rc == 0) {
            output = result<SPtr>::ok(std::shared_ptr<CoTCP>(new CoTCP(fd)));
            break;
        }
        if (errno != EINPROGRESS) {
            last_connect_error = errno;
            ::close(fd);
            if (item != nullptr)
                item = item->ai_next;
            else
                numeric_attempt = false;
            continue;
        }
        auto wait = _WaitFd(fd, false, options,
                            dial_wait.extra_cancel,
                            dial_wait.connect_on_registered);
        if (!wait) {
            ::close(fd);
            output = result<SPtr>::err(std::move(wait).error());
            wait_failed = true;
            break;
        }
        int socket_error = 0;
        socklen_t length = sizeof(socket_error);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &length) != 0 ||
            socket_error != 0) {
            last_connect_error = socket_error != 0 ? socket_error : errno;
            ::close(fd);
            if (item != nullptr)
                item = item->ai_next;
            else
                numeric_attempt = false;
            continue;
        }
        output = result<SPtr>::ok(std::shared_ptr<CoTCP>(new CoTCP(fd)));
        break;
    }
    if (results) ::freeaddrinfo(results);
    if (!output && !wait_failed && last_connect_error != 0) {
        Error error = MakeError(ErrorCode::TransportError, "TCP connection failed");
        error.backend_category = "errno";
        error.backend_code = last_connect_error;
        return result<SPtr>::err(std::move(error));
    }
    return output;
}

result<std::size_t> CoTCP::_ReadSome(void* buffer, std::size_t size,
                                     const CallOptions& options) {
    if (size && !buffer)
        return result<std::size_t>::err(MakeError(ErrorCode::InvalidArgument, "null TCP read buffer"));
    auto entry = _CheckEntry(/*in_coroutine_only=*/true);
    if (!entry)
        return result<std::size_t>::err(std::move(entry).error());
    // §4.0.1.7：Runtime 在途门禁——admit 先于 m_inflight 计数，拒绝即
    // Overloaded 立即返回；名额登记在 m_quota_held（对象侧账本），
    // op 结束归还 1，RequestClose/析构 DrainQuota 兜底（Stop 安全）。
    if (m_inflight_admit && !m_inflight_admit())
        return result<std::size_t>::err(MakeError(ErrorCode::Overloaded,
            "TCP read: runtime max_inflight exceeded"));
    int fd;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        if (m_closing) {
            if (m_inflight_release) m_inflight_release();
            return result<std::size_t>::err(MakeError(ErrorCode::Closed, "TCP socket closed"));
        }
        fd = m_fd;
        ++m_inflight;
        if (m_inflight_admit)
            ++m_quota_held;
    }
    // §4.0.1.7 测试接缝：名额已登记（m_quota_held++），等待循环尚未进入。
    // 测试据此断言「op 已占名额」而无需时间推断。
    if (m_wait_entry_gate_for_test)
        m_wait_entry_gate_for_test();
    auto run = [&]() -> result<std::size_t> {
      if (size == 0) return result<std::size_t>::ok(0);
      for (;;) {
        if (m_close_source->Token().IsCancellationRequested())
            return result<std::size_t>::err(MakeError(ErrorCode::Closed, "TCP socket closed"));
        const ssize_t n = ::recv(fd, buffer, size, MSG_DONTWAIT);
        if (n >= 0)
            return result<std::size_t>::ok(static_cast<std::size_t>(n));
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
            return result<std::size_t>::err(_Errno(ErrorCode::TransportError, "TCP read failed"));
        if (errno == EINTR) continue;
        // F3：数据路径挂起落定回调——证明协程真实挂起在 fd 可读等待。
        auto wait = _WaitFd(fd, true, options, m_close_source->Token(),
                            m_io_wait_registered_gate_for_test);
        if (!wait) return result<std::size_t>::err(std::move(wait).error());
      }
    };
    auto output = run();
    _EndIo();
    return output;
}

result<std::size_t> CoTCP::_WriteSome(const void* buffer, std::size_t size,
                                      const CallOptions& options) {
    if (size && !buffer)
        return result<std::size_t>::err(MakeError(ErrorCode::InvalidArgument, "null TCP write buffer"));
    auto entry = _CheckEntry(/*in_coroutine_only=*/true);
    if (!entry)
        return result<std::size_t>::err(std::move(entry).error());
    if (m_inflight_admit && !m_inflight_admit())
        return result<std::size_t>::err(MakeError(ErrorCode::Overloaded,
            "TCP write: runtime max_inflight exceeded"));
    int fd;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        if (m_closing) {
            if (m_inflight_release) m_inflight_release();
            return result<std::size_t>::err(MakeError(ErrorCode::Closed, "TCP socket closed"));
        }
        fd = m_fd;
        ++m_inflight;
        if (m_inflight_admit)
            ++m_quota_held;
    }
    if (m_wait_entry_gate_for_test)
        m_wait_entry_gate_for_test();
    auto run = [&]() -> result<std::size_t> {
      if (size == 0) return result<std::size_t>::ok(0);
      for (;;) {
        if (m_close_source->Token().IsCancellationRequested())
            return result<std::size_t>::err(MakeError(ErrorCode::Closed, "TCP socket closed"));
        const ssize_t n = ::send(fd, buffer, size, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (n >= 0)
            return result<std::size_t>::ok(static_cast<std::size_t>(n));
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
            return result<std::size_t>::err(_Errno(ErrorCode::TransportError, "TCP write failed"));
        if (errno == EINTR) continue;
        // F3：数据路径挂起落定回调——证明协程真实挂起在 fd 可写等待。
        auto wait = _WaitFd(fd, false, options, m_close_source->Token(),
                            m_io_wait_registered_gate_for_test);
        if (!wait) return result<std::size_t>::err(std::move(wait).error());
      }
    };
    auto output = run();
    _EndIo();
    return output;
}

result<std::size_t> CoTCP::_WriteAll(const void* buffer, std::size_t size,
                                     const CallOptions& options) {
    if (size && !buffer)
        return result<std::size_t>::err(MakeError(ErrorCode::InvalidArgument,
            "null TCP write buffer"));
    // §4.0.1.7：与 ReadSome/WriteSome 共用同一入口门禁（参数之外的协程上下文
    // 与代际检查），不再自带一份等价副本。
    auto entry = _CheckEntry(/*in_coroutine_only=*/true);
    if (!entry)
        return result<std::size_t>::err(std::move(entry).error());
    // §4.0.1.7 在途粒度：WriteAll 是 _WriteSome 的重试循环，**不**在自身入口
    // 占名额——在途粒度是每一轮内部单次写尝试（每轮 admit/归还）。轮间名额可
    // 被其他 op 取走，故多轮 WriteAll 可能中途 Overloaded；已写字节数始终经
    // error.transferred_bytes 回报（见下方 !part 分支）。
    const auto* bytes = static_cast<const std::uint8_t*>(buffer);
    std::size_t written = 0;
    while (written < size) {
        auto part = _WriteSome(bytes + written, size - written, options);
        if (!part) {
            Error error = std::move(part).error();
            error.transferred_bytes = written;
            return result<std::size_t>::err(std::move(error));
        }
        if (part.value() == 0)
            break;
        written += part.value();
    }
    return result<std::size_t>::ok(written);
}

void CoTCP::_CloseFd() noexcept {
    if (m_fd >= 0) {
        ::shutdown(m_fd, SHUT_RDWR);
        ::close(m_fd);
        m_fd = -1;
    }
    m_closed = true;
}

void CoTCP::_EndIo() noexcept {
    bool closed_now = false;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        --m_inflight;
        // §4.0.1.7：op 落定归还本对象计入的一个在途名额；若对象已
        // closing（RequestClose 先行 DrainQuota），m_quota_held 已清零，
        // 此归还不会发生——两条归还路径互斥不重复。
        if (m_quota_held > 0) {
            --m_quota_held;
            if (m_inflight_release) m_inflight_release();
        }
        if (m_closing && m_inflight == 0 && !m_closed) {
            _CloseFd();
            closed_now = true;
        }
    }
    if (closed_now) {
        m_closed_source->RequestCancel();
        if (m_closed_hook) m_closed_hook();
    }
}

void CoTCP::RequestClose() noexcept {
    bool closed_now = false;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        if (m_closing) return;
        m_closing = true;
        // §4.0.1.7：挂起 op 可能不再返回（强制 Stop 不展开栈），此处
        // 归还全部未归还名额；op 若仍返回，其 _EndIo 见 quota_held==0
        // 不再重复归还。
        _DrainQuotaLocked();
        if (m_inflight == 0) {
            _CloseFd();
            closed_now = true;
        }
    }
    m_close_source->RequestCancel();
    if (closed_now) {
        m_closed_source->RequestCancel();
        if (m_closed_hook) m_closed_hook();
    }
}
bool CoTCP::ForceCloseAfterQuiescence() noexcept {
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        if (m_closed)
            return false;      // 幂等：物理关闭恰好一次
        m_closing = true;
        _DrainQuotaLocked();
        // 硬停闸门：此处故意绕过 m_inflight 门控。前置条件由调用方保证
        // （仅 Scheduler::Stop() 返回后才允许），见头文件注释。
        _CloseFd();
    }
    m_close_source->RequestCancel();
    m_closed_source->RequestCancel();
    if (m_closed_hook)
        m_closed_hook();
    return true;
}

bool CoTCP::IsClosed() const noexcept {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_closed;
}
CloseStatus CoTCP::WaitClosed(bbt::coroutine::Deadline deadline,
                              bbt::coroutine::CancellationToken cancel) {
    if (g_bbt_tls_coroutine_co == nullptr)
        return CloseStatus::InvalidContext;
    if (bbt::coroutine::CurrentRuntimeGeneration() != m_info.generation)
        return CloseStatus::RuntimeUnavailable;
    if (IsClosed()) return CloseStatus::Closed;
    if (m_close_waiting.exchange(true)) return CloseStatus::AlreadyWaiting;
    struct Guard { std::atomic_bool& value; ~Guard() { value.store(false); } } guard{m_close_waiting};
    auto waiter = bbt::coroutine::sync::CoWaiter::Create();
    CombinedWaitOptions wait;
    wait.deadline = deadline;
    wait.cancel = bbt::coroutine::CancellationToken::Combine(
        m_closed_source->Token(), cancel);
    const auto status = waiter->Wait(wait);
    if (IsClosed()) return CloseStatus::Closed;
    if (cancel.IsCancellationRequested()) return CloseStatus::Cancelled;
    if (status == CombinedWaitStatus::TimedOut) return CloseStatus::TimedOut;
    if (status == CombinedWaitStatus::InvalidContext) return CloseStatus::InvalidContext;
    return CloseStatus::RuntimeUnavailable;
}

bool CoTCPListener::ForceCloseAfterQuiescence() noexcept {
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        if (m_closed)
            return false;      // 幂等：物理关闭恰好一次
        m_closing = true;
        _DrainQuotaLocked();
        // §4.0.1.7：挂起的 Accept 永不返回 ⇒ 其连接容量预留必须在此归还，
        // 否则 owner 的连接名额永不归零、托管链在硬停后无法落定。
        _DrainAcceptReservedLocked();
        // 硬停闸门：此处故意绕过 m_inflight 门控。前置条件由调用方保证
        // （仅 Scheduler::Stop() 返回后才允许），见头文件注释。
        _CloseFd();
    }
    m_close_source->RequestCancel();
    m_closed_source->RequestCancel();
    if (m_closed_hook)
        m_closed_hook();
    return true;
}

CoTCPListener::CoTCPListener(int fd) noexcept
    : m_fd(fd), m_info{g_next_object_id.fetch_add(1),
        bbt::coroutine::CurrentRuntimeGeneration(), "tcp", "CoTCPListener"} {}
CoTCPListener::~CoTCPListener() { RequestClose(); }

bbt::coroutine::CoObjectInfo CoTCPListener::GetObjectInfo() const { return m_info; }

result<void> CoTCPListener::_CheckEntry() const {
    if (g_bbt_tls_coroutine_co == nullptr)
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
            "TCP accept requires coroutine"));
    const auto generation = bbt::coroutine::CurrentRuntimeGeneration();
    if (generation == 0 || generation != m_info.generation)
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "TCP accept belongs to another runtime generation"));
    return result<void>::ok();
}

SocketAddress CoTCPListener::LocalAddress() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    SocketAddress local;
    if (m_fd < 0) return local;
    sockaddr_storage addr{};
    socklen_t len = sizeof(addr);
    if (::getsockname(m_fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0)
        return local;
    char ip[INET6_ADDRSTRLEN]{};
    if (addr.ss_family == AF_INET) {
        const auto* v4 = reinterpret_cast<const sockaddr_in*>(&addr);
        if (::inet_ntop(AF_INET, &v4->sin_addr, ip, sizeof(ip))) {
            local.ip = ip;
            local.port = ntohs(v4->sin_port);
        }
    } else if (addr.ss_family == AF_INET6) {
        const auto* v6 = reinterpret_cast<const sockaddr_in6*>(&addr);
        if (::inet_ntop(AF_INET6, &v6->sin6_addr, ip, sizeof(ip))) {
            local.ip = ip;
            local.port = ntohs(v6->sin6_port);
        }
    }
    return local;
}

result<CoTCPListener::SPtr> CoTCPListener::ListenTCP(std::string host,
                                                      std::uint16_t port,
                                                      int backlog) {
    if (backlog <= 0)
        return result<SPtr>::err(MakeError(ErrorCode::InvalidArgument, "invalid TCP backlog"));
    if (!host.empty()) {
        sockaddr_storage numeric{};
        socklen_t numeric_len = 0;
        if (!_TryNumericAddress(host, numeric, numeric_len))
            return result<SPtr>::err(MakeError(
                ErrorCode::InvalidArgument,
                "TCP listen address must be a numeric IP"));
    }
    addrinfo hints{};
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = AF_UNSPEC;
    hints.ai_flags = AI_PASSIVE;
    addrinfo* results = nullptr;
    const std::string service = std::to_string(port);
    const char* node = host.empty() ? nullptr : host.c_str();
    const int gai = ::getaddrinfo(node, service.c_str(), &hints, &results);
    if (gai != 0)
        return result<SPtr>::err(_GaiError("TCP listen address resolution failed", gai));

    result<SPtr> output = result<SPtr>::err(
        MakeError(ErrorCode::Unavailable, "TCP listen failed"));
    for (addrinfo* item = results; item != nullptr; item = item->ai_next) {
        const int fd = ::socket(item->ai_family, item->ai_socktype, item->ai_protocol);
        if (fd < 0 || _SetNonBlocking(fd) != 0) {
            if (fd >= 0) ::close(fd);
            continue;
        }
        int reuse = 1;
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
        if (::bind(fd, item->ai_addr, item->ai_addrlen) == 0 &&
            ::listen(fd, backlog) == 0) {
            output = result<SPtr>::ok(std::shared_ptr<CoTCPListener>(new CoTCPListener(fd)));
            break;
        }
        ::close(fd);
    }
    ::freeaddrinfo(results);
    return output;
}

result<std::shared_ptr<CoTCP>> CoTCPListener::Accept(const CallOptions& options) {
    auto entry = _CheckEntry();
    if (!entry)
        return result<std::shared_ptr<CoTCP>>::err(std::move(entry).error());
    // §4.0.1.7：Runtime 在途门禁先于连接容量（m_accept_admit）。容量满
    // 立即 Overloaded、不排队；名额计入 m_quota_held，op 结束经 _EndIo
    // 归还，RequestClose/析构经 DrainQuota 兜底（Stop 安全）。
    bool quota_admitted = false;
    if (m_inflight_admit) {
        if (!m_inflight_admit())
            return result<std::shared_ptr<CoTCP>>::err(MakeError(
                ErrorCode::Overloaded, "TCP accept: runtime max_inflight exceeded"));
        quota_admitted = true;
    }
    bool capacity_reserved = false;
    // §4.0.1.7：接纳容量预留的出账凭据。正常终态（adopt 转移 / release 归还）
    // 由本 RAII 出账；硬停不展开协程栈、本 op 永不返回 ⇒ 凭据不执行，改由
    // ForceCloseAfterQuiescence 经 _DrainAcceptReservedLocked 兜底归还。
    struct ReservedLedger {
        CoTCPListener* self{nullptr};
        ~ReservedLedger() {
            if (self) {
                std::lock_guard<std::mutex> lock(self->m_mtx);
                self->_ConsumeAcceptReservedLocked();
            }
        }
    } reserved_ledger;
    if (m_accept_admit) {
        if (!m_accept_admit()) {
            if (quota_admitted && m_inflight_release) m_inflight_release();
            return result<std::shared_ptr<CoTCP>>::err(
                MakeError(ErrorCode::Overloaded, "TCP accept capacity exceeded"));
        }
        capacity_reserved = true;
    }
    int fd_to_wait;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        if (m_closing || m_fd < 0) {
            if (m_accept_release) m_accept_release();
            if (quota_admitted && m_inflight_release) m_inflight_release();
            return result<std::shared_ptr<CoTCP>>::err(
                MakeError(ErrorCode::Closed, "TCP listener closed"));
        }
        fd_to_wait = m_fd;
        ++m_inflight;
        // 本次 Accept 占用的连接容量预留登记在对象上，出账凭据随即生效。
        if (capacity_reserved) {
            ++m_accept_reserved;
            reserved_ledger.self = this;
        }
        if (quota_admitted)
            ++m_quota_held;
    }
    // §4.0.1.7 测试接缝：名额登记完成、首次 accept4/等待前触发。
    if (m_wait_entry_gate_for_test)
        m_wait_entry_gate_for_test();
    auto run = [&]() -> result<std::shared_ptr<CoTCP>> {
      for (;;) {
        if (m_close_source->Token().IsCancellationRequested())
            return result<std::shared_ptr<CoTCP>>::err(
                MakeError(ErrorCode::Closed, "TCP listener closed"));
        // accept() is transparently hooked and parks indefinitely on EAGAIN.
        // Use the native nonblocking syscall, then let CoWaiter own the wait.
        const int fd = ::syscall(SYS_accept4, fd_to_wait, nullptr, nullptr,
                                 SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd >= 0) {
            if (m_close_source->Token().IsCancellationRequested()) {
                ::close(fd);
                return result<std::shared_ptr<CoTCP>>::err(
                    MakeError(ErrorCode::Closed, "TCP listener closed"));
            }
            if (_SetNonBlocking(fd) != 0) {
                ::close(fd);
                return result<std::shared_ptr<CoTCP>>::err(
                    _Errno(ErrorCode::TransportError, "TCP accepted socket setup failed"));
            }
            return result<std::shared_ptr<CoTCP>>::ok(std::shared_ptr<CoTCP>(new CoTCP(fd)));
        }
        if (errno == EINTR)
            continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK)
            return result<std::shared_ptr<CoTCP>>::err(_Errno(ErrorCode::TransportError, "TCP accept failed"));
        auto wait = _WaitFd(fd_to_wait, true, options, m_close_source->Token());
        if (!wait)
            return result<std::shared_ptr<CoTCP>>::err(std::move(wait).error());
      }
    };
    auto output = run();
    _EndIo();
    if (output && m_accept_adopt && !m_accept_adopt(output.value())) {
        output.value()->RequestClose();
        if (m_accept_release) m_accept_release();
        return result<std::shared_ptr<CoTCP>>::err(MakeError(
            ErrorCode::Closed, "runtime closed during TCP accept"));
    }
    if (!output && m_accept_release)
        m_accept_release();
    return output;
}

void CoTCPListener::_CloseFd() noexcept {
    if (m_fd >= 0) {
        ::close(m_fd);
        m_fd = -1;
    }
    m_closed = true;
}

void CoTCPListener::_EndIo() noexcept {
    bool closed_now = false;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        --m_inflight;
        if (m_quota_held > 0) {
            --m_quota_held;
            if (m_inflight_release) m_inflight_release();
        }
        if (m_closing && m_inflight == 0 && !m_closed) {
            _CloseFd();
            closed_now = true;
        }
    }
    if (closed_now) {
        m_closed_source->RequestCancel();
        if (m_closed_hook) m_closed_hook();
    }
}

void CoTCPListener::RequestClose() noexcept {
    bool closed_now = false;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        if (m_closing) return;
        m_closing = true;
        _DrainQuotaLocked();   // §4.0.1.7：挂起 Accept 被 Stop 销毁时兜底归还
        if (m_inflight == 0) {
            _CloseFd();
            closed_now = true;
        }
    }
    m_close_source->RequestCancel();
    if (closed_now) {
        m_closed_source->RequestCancel();
        if (m_closed_hook) m_closed_hook();
    }
}
bool CoTCPListener::IsClosed() const noexcept {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_closed;
}
CloseStatus CoTCPListener::WaitClosed(bbt::coroutine::Deadline deadline,
                                      bbt::coroutine::CancellationToken cancel) {
    if (g_bbt_tls_coroutine_co == nullptr)
        return CloseStatus::InvalidContext;
    if (bbt::coroutine::CurrentRuntimeGeneration() != m_info.generation)
        return CloseStatus::RuntimeUnavailable;
    if (IsClosed())
        return CloseStatus::Closed;
    if (m_close_waiting.exchange(true))
        return CloseStatus::AlreadyWaiting;
    struct Guard { std::atomic_bool& value; ~Guard() { value.store(false); } } guard{m_close_waiting};
    auto waiter = bbt::coroutine::sync::CoWaiter::Create();
    CombinedWaitOptions wait;
    wait.deadline = deadline;
    wait.cancel = bbt::coroutine::CancellationToken::Combine(
        m_closed_source->Token(), cancel);
    const auto status = waiter->Wait(wait);
    if (IsClosed()) return CloseStatus::Closed;
    if (cancel.IsCancellationRequested()) return CloseStatus::Cancelled;
    if (status == CombinedWaitStatus::TimedOut) return CloseStatus::TimedOut;
    if (status == CombinedWaitStatus::InvalidContext) return CloseStatus::InvalidContext;
    return CloseStatus::RuntimeUnavailable;
}

} // namespace bbt::infra::tcp
