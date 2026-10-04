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
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/DnsResolver.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>   // g_scheduler->IsInitialized（运行时是否在跑）

#include <functional>
#include <unordered_set>

#include "detail/IoSupport.hpp"
#include "detail/TransportWiring.hpp"

namespace bbt::infra::tcp {
namespace {

using bbt::coroutine::sync::CoWaiter;
using bbt::coroutine::sync::CombinedWaitOptions;
using bbt::coroutine::sync::CombinedWaitStatus;

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

// coroutine 运行时是否在跑：进程寿命运行时只有「已初始化」一种活态，
// 不再有运行时代际（对象身份也不携带代际）。
bool _RuntimeRunning() noexcept {
    return g_scheduler != nullptr && g_scheduler->IsInitialized();
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
                               std::function<void()> on_registered = {}) {
    auto state = std::make_shared<_DnsResolution>();
    state->host = host;
    state->service = std::to_string(port);
    state->hints.ai_socktype = SOCK_STREAM;
    state->hints.ai_family = AF_UNSPEC;
    bbt::coroutine::sync::CombinedWaitOptions wait;
    wait.deadline = options.deadline;
    // 阻塞 libc 调用只在上游 DNS worker 上运行；state 延寿至 worker 完成。
    // 确定性等待入口证据：AwaitBounded 把 job 入队发生在协程挂起落定后
    // （on_registered 回调内），worker 开始执行 work 即证明协程已在等待。
    // 因此在 work lambda 顶部触发可选接缝即可观察「dial 处于 DNS 等待」。
    //
    // 关闭口径：DNS 等待段不挂在对象/owner 的 CloseWaiters 上——上游
    // AwaitBounded 内部自建 waiter，外部无法 Notify（取消令牌已删除）。
    // 因此该段的封口响应由调用方 CallOptions::deadline 界定（见
    // src/detail/TransportWiring.hpp DialWaitOptions 注释）。
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

// 单轮 fd 等待（组合等待：fd interest + 绝对 deadline）。封口唤醒口径：
//   - 等待者先在事件登记成功后（on_registered 内）向 close_waiters 登记；
//     封口后 Add 失败时自行 Notify 一次——事件与等待位均已就绪，Notify 走
//     CoPollEvent 的 PENDING 路径，不丢唤醒；
//   - 封口唤醒经 CoWaiter::Notify 抵达，组合等待以 Completed 首胜返回，
//     本函数把它映射为 Closed（仅关闭路径会 Notify 这些等待者）；
//   - Cancelled 只来自协程级 RequestCancel（业务取消不再由 infra 令牌表达）。
result<void> _WaitFd(int fd, bool readable, const CallOptions& options,
                     bbt::infra::detail::CloseWaiters* close_waiters,
                     std::function<void()> on_registered = {}) {
    if (g_bbt_tls_coroutine_co == nullptr)
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
            "CoTCP operation must run in coroutine context"));

    // CoWaiter::Create 不触碰 fd；放在封口登记之前使「可能被暂停的边界」不落在
    // 登记屏障内（屏障内只允许非阻塞的 fd 事件建立）。
    auto waiter = CoWaiter::Create();
    // 关闭线性化：与对象级 Close 的封口原子配对。已封口即返回 Closed，绝不在
    // 已关/将关 fd 上登记旧兴趣；否则进入「进行中登记」计数，物理 close 会等到
    // 本登记释放后才发生。RAII 配对保证异常/提前返回时也不泄漏登记。
    bbt::infra::detail::CloseRegisterClaim claim(close_waiters);
    if (close_waiters != nullptr && !claim.acquired())
        return result<void>::err(MakeError(ErrorCode::Closed, "TCP socket closed"));
    CombinedWaitOptions wait;
    wait.fd = fd;
    wait.want_readable = readable;
    wait.want_writeable = !readable;
    wait.deadline = options.deadline;
    bool registered = false;
    const auto status = waiter->Wait(wait,
        [&waiter, &registered, &claim, close_waiters,
         on_registered = std::move(on_registered)]() -> bool {
            // on_registered 在事件已建立（InitFdEvent）、等待者已纳管
            // （_RegistAwaitEvent）之后运行：此刻释放登记屏障是安全的——旧 fd
            // 的登记已完成，后续唤醒经 close_waiters 的 Notify 抵达。
            claim.release();
            if (close_waiters != nullptr) {
                registered = close_waiters->Add(waiter);
                if (!registered)
                    waiter->Notify();   // 已封口：不挂起，自行唤醒
            }
            if (on_registered) on_registered();
            return true;
        });
    if (registered && close_waiters != nullptr)
        close_waiters->Remove(waiter.get());
    switch (status) {
    case CombinedWaitStatus::FdReadable:
    case CombinedWaitStatus::FdWriteable:
        return result<void>::ok();
    case CombinedWaitStatus::Completed:
        return result<void>::err(MakeError(ErrorCode::Closed, "TCP socket closed"));
    case CombinedWaitStatus::Cancelled:
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

CoTCP::CoTCP(int fd, bbt::coroutine::CoObjectInfo info) noexcept
    : m_fd(fd), m_info(std::move(info)),
      m_close_waiters(std::make_shared<bbt::infra::detail::CloseWaiters>()) {}

CoTCP::~CoTCP() {
    std::lock_guard<std::mutex> lock(m_mtx);
    _DrainQuotaLocked();   // 未经 Close 就析构时兜底归还
    _SettleClosedLocked();
}

bbt::coroutine::CoObjectInfo CoTCP::GetObjectInfo() const { return m_info; }

result<void> CoTCP::_CheckEntry(bool in_coroutine_only) const {
    if (in_coroutine_only && g_bbt_tls_coroutine_co == nullptr)
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
            "CoTCP operation must run in coroutine context"));
    if (!_RuntimeRunning())
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "coroutine runtime not initialized"));
    return result<void>::ok();
}

IoResult CoTCP::TryReadSome(MutableBytes dst) {
    if (dst.size && !dst.data)
        return IoResult::err(MakeError(ErrorCode::InvalidArgument, "null TCP read buffer"));
    if (g_bbt_tls_coroutine_co == nullptr)
        return IoResult::err(MakeError(ErrorCode::InvalidContext, "TCP read requires coroutine"));
    if (!_RuntimeRunning())
        return IoResult::err(MakeError(ErrorCode::RuntimeUnavailable, "coroutine runtime not initialized"));
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
    if (!_RuntimeRunning())
        return IoResult::err(MakeError(ErrorCode::RuntimeUnavailable, "coroutine runtime not initialized"));
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
    // 未托管入口：不装配 owner 级关闭唤醒登记与挂起落定接缝，语义等同于默认
    // 构造的 DialWaitOptions（见 src/detail/TransportWiring.hpp）。
    return _DialTcp(std::move(host), port, options,
                    bbt::infra::detail::DialWaitOptions{});
}

result<CoTCP::SPtr> CoTCP::_DialTcp(
    std::string host, std::uint16_t port, const CallOptions& options,
    const bbt::infra::detail::DialWaitOptions& dial_wait) {
    // 对象身份（进程内唯一 id，无运行时代际）在真实连接建立前一刻申请。
    auto make_connection = [](int fd) -> result<SPtr> {
        auto info = bbt::infra::detail::NewObjectInfo("tcp");
        if (!info) {
            ::close(fd);
            return result<SPtr>::err(std::move(info).error());
        }
        return result<SPtr>::ok(SPtr(new CoTCP(fd, std::move(info).value())));
    };

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
                                     dial_wait.dns_on_registered);
        if (!resolved)
            return result<SPtr>::err(std::move(resolved).error());
        results = std::move(resolved).value();
    }

    result<SPtr> output = result<SPtr>::err(
        MakeError(ErrorCode::Unavailable, "TCP connection failed"));
    int last_connect_error = 0;
    // output 已携带确定错误（等待段失败 / 对象身份申请失败）时不改写为
    // connect 错误。
    bool decisive_error = false;
    // 候选 fd 同步收口：仅当调用方装配移交令牌（DialWaitOptions::handoff，
    // 受管 Redis dial）时启用。未装配时（未托管入口 / TCP 传输 owner）保持原
    // 语义，由本协程在恢复时自行 close 候选 fd。
    bbt::infra::detail::CloseWaiters* const cw = dial_wait.close_waiters.get();
    bbt::infra::detail::DialHandoffToken* const handoff = dial_wait.handoff.get();
    const bool managed_candidate = handoff != nullptr && cw != nullptr;
    addrinfo* item = results;
    bool numeric_attempt = results == nullptr && numeric_len != 0;
    while (item != nullptr || numeric_attempt) {
        const sockaddr* address = item ? item->ai_addr
                                       : reinterpret_cast<const sockaddr*>(&numeric);
        const socklen_t address_len = item ? item->ai_addrlen : numeric_len;

        int  fd            = -1;
        int  connect_rc    = -1;
        int  connect_errno = 0;
        bool sealed        = false;
        // 建立候选 fd：受管路径在 owner 关闭门内建立（封口后不得再发起
        // socket/connect），成功即在关闭登记中记名，owner 封口会同步收口它；
        // 非受管路径沿用原语义直连。
        auto establish = [&]() -> int {
            fd = ::socket(item ? item->ai_family : numeric.ss_family,
                          SOCK_STREAM, 0);
            if (fd < 0) {
                connect_errno = errno;
                return -1;
            }
            if (_SetNonBlocking(fd) != 0) {
                connect_errno = errno;
                ::close(fd);
                fd = -1;
                return -1;
            }
            // The coroutine Hook waits without our CallOptions deadline. This
            // transport owns the nonblocking syscall and CoWaiter retry loop.
            connect_rc = static_cast<int>(
                ::syscall(SYS_connect, fd, address, address_len));
            connect_errno = connect_rc == 0 ? 0 : errno;
            return fd;
        };
        if (managed_candidate) {
            if (!cw->CreateCandidate(establish))
                sealed = true;
        } else {
            establish();
        }
        auto next_attempt = [&]() {
            if (item != nullptr)
                item = item->ai_next;
            else
                numeric_attempt = false;
        };
        if (sealed) {
            output = result<SPtr>::err(
                MakeError(ErrorCode::Closed, "TCP socket closed"));
            decisive_error = true;
            break;
        }
        if (fd < 0) {
            last_connect_error = connect_errno;
            next_attempt();
            continue;
        }
        if (connect_rc == 0) {
            if (managed_candidate) {
                int owned = -1;
                if (!handoff->Arm(&owned)) {
                    // owner 已封口并接管候选 fd：不建立连接对象（其 fd 已收口）。
                    output = result<SPtr>::err(
                        MakeError(ErrorCode::Closed, "TCP socket closed"));
                    decisive_error = true;
                    break;
                }
                fd = owned;
            }
            output = make_connection(fd);
            decisive_error = !output;
            break;
        }
        if (connect_errno != EINPROGRESS) {
            last_connect_error = connect_errno;
            if (managed_candidate) {
                // F-1a：关闭门内收口候选 fd（先 close 再清除登记），使并发
                // Close/Disconnect 的封口排空谓词只在 fd 已 close 后归零。owner 已
                // 接管（返回 false）则不得再触碰该 fd，按 Closed 收口。
                if (!cw->CloseCandidateInGate()) {
                    output = result<SPtr>::err(
                        MakeError(ErrorCode::Closed, "TCP socket closed"));
                    decisive_error = true;
                    break;
                }
            } else {
                ::close(fd);
            }
            next_attempt();
            continue;
        }
        auto wait = _WaitFd(fd, false, options, cw,
                            dial_wait.connect_on_registered);
        const bool wait_ok = static_cast<bool>(wait);
        if (managed_candidate) {
            // 等待段结束：成功路径经移交取得所有权并登记移交在途（fd 之后由连接
            // 对象/移交令牌收口）；失败路径在关闭门内收口候选 fd。两者都与 owner
            // 封口原子互斥——owner 已接管（返回 false）则不得再触碰该 fd（可能已被
            // 复用），按 Closed 收口。
            if (wait_ok) {
                int owned = -1;
                if (!handoff->Arm(&owned)) {
                    output = result<SPtr>::err(
                        MakeError(ErrorCode::Closed, "TCP socket closed"));
                    decisive_error = true;
                    break;
                }
                fd = owned;
            } else {
                // F-1a：关闭门内收口候选 fd；owner 已接管则返回 false，不再触碰。
                if (!cw->CloseCandidateInGate()) {
                    output = result<SPtr>::err(
                        MakeError(ErrorCode::Closed, "TCP socket closed"));
                    decisive_error = true;
                    break;
                }
                output = result<SPtr>::err(std::move(wait).error());
                decisive_error = true;
                break;
            }
        } else if (!wait_ok) {
            ::close(fd);
            output = result<SPtr>::err(std::move(wait).error());
            decisive_error = true;
            break;
        }
        int socket_error = 0;
        socklen_t length = sizeof(socket_error);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &length) != 0 ||
            socket_error != 0) {
            last_connect_error = socket_error != 0 ? socket_error : errno;
            if (managed_candidate) {
                // F-1a：fd 已由 Arm 移交在途——关闭门内先 close 再归还移交计数，
                // 保证并发封口的排空谓词只在 fd 已 close 后归零；本地址失败后重试
                // 下一地址。
                handoff->CloseAndDisarm(fd);
            } else {
                ::close(fd);
            }
            next_attempt();
            continue;
        }
        output = make_connection(fd);
        decisive_error = !output;
        break;
    }
    if (results) ::freeaddrinfo(results);
    if (!output && !decisive_error && last_connect_error != 0) {
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
    // op 结束归还 1，Close/析构 DrainQuota 兜底。
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
        ssize_t n;
        {
          // §1：最后一次封口检查 + 非阻塞 syscall 与物理 close 同锁配对。
          // _CloseFd 持同一把 m_mtx，封口置位后不可能再有 syscall 越过本锁。
          std::lock_guard<std::mutex> lock(m_mtx);
          if (m_closing || m_fd < 0)
            return result<std::size_t>::err(MakeError(ErrorCode::Closed, "TCP socket closed"));
          n = ::recv(m_fd, buffer, size, MSG_DONTWAIT);
        }
        if (n >= 0)
            return result<std::size_t>::ok(static_cast<std::size_t>(n));
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
            return result<std::size_t>::err(_Errno(ErrorCode::TransportError, "TCP read failed"));
        if (errno == EINTR) continue;
        {
          // 挂起前再确认未封口：已封口则不注册旧 fd 兴趣，直接落定 Closed。
          std::lock_guard<std::mutex> lock(m_mtx);
          if (m_closing || m_fd < 0)
            return result<std::size_t>::err(MakeError(ErrorCode::Closed, "TCP socket closed"));
        }
        // F3：数据路径挂起落定回调——证明协程真实挂起在 fd 可读等待。
        auto wait = _WaitFd(fd, true, options, m_close_waiters.get(),
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
        ssize_t n;
        {
          // §1：最后一次封口检查 + 非阻塞 syscall 与物理 close 同锁配对。
          std::lock_guard<std::mutex> lock(m_mtx);
          if (m_closing || m_fd < 0)
            return result<std::size_t>::err(MakeError(ErrorCode::Closed, "TCP socket closed"));
          n = ::send(m_fd, buffer, size, MSG_NOSIGNAL | MSG_DONTWAIT);
        }
        if (n >= 0)
            return result<std::size_t>::ok(static_cast<std::size_t>(n));
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
            return result<std::size_t>::err(_Errno(ErrorCode::TransportError, "TCP write failed"));
        if (errno == EINTR) continue;
        {
          std::lock_guard<std::mutex> lock(m_mtx);
          if (m_closing || m_fd < 0)
            return result<std::size_t>::err(MakeError(ErrorCode::Closed, "TCP socket closed"));
        }
        // F3：数据路径挂起落定回调——证明协程真实挂起在 fd 可写等待。
        auto wait = _WaitFd(fd, false, options, m_close_waiters.get(),
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
    // 与运行时可用性检查），不再自带一份等价副本。
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
}

bool CoTCP::_SettleClosedLocked() noexcept {
    if (m_closed)
        return false;
    _CloseFd();
    m_closed = true;
    m_cv.notify_all();   // 唤醒并发 Close 的收口等待
    return true;
}

void CoTCP::_EndIo() noexcept {
    bool closed_now = false;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        --m_inflight;
        // §4.0.1.7：op 落定归还本对象计入的一个在途名额；若对象已封口
        // （Close 先行 DrainQuota），m_quota_held 已清零，此归还不会发生
        // ——两条归还路径互斥不重复。
        if (m_quota_held > 0) {
            --m_quota_held;
            if (m_inflight_release) m_inflight_release();
        }
        // 在途归零即物理落定：Close 的有界等待超时（或同线程恢复）后由最后
        // 一个退出的 op 完成收口。
        if (m_closing && m_inflight == 0)
            closed_now = _SettleClosedLocked();
        m_cv.notify_all();   // 唤醒 Close 的在途排空等待
    }
    if (closed_now && m_closed_hook) m_closed_hook();
}

void CoTCP::Close() noexcept {
    bool settled_now = false;
    {
        std::unique_lock<std::mutex> lock(m_mtx);
        if (!m_closing.exchange(true)) {
            // 首次调用者执行完整收口：封口 → 唤醒挂起 op → 物理释放。
            // §4.0.1.7：挂起 op 可能仍在等待段（未被唤醒或唤醒后尚未退出），
            // 此处先归还全部未归还名额；op 若仍返回，其 _EndIo 见
            // m_quota_held==0 不再重复归还。
            _DrainQuotaLocked();
            lock.unlock();
            // 关闭线性化：封口并唤醒在册等待者，然后等在途「fd 兴趣登记」排空
            // 才物理释放。数据路径的最后封口检查与 fd 兴趣登记不再留窗口——已在
            // 登记中的 op 先完成登记（旧 fd 有效），物理 close 不会越过；已过最后
            // 检查但尚未 BeginRegister 的晚到 op 在此封口后得到明确 Closed，不在
            // 已关/复用 fd 上登记（修复 InitFdEvent EBADF/RuntimeUnavailable）。
            // syscall 与物理 close 仍持同一把 m_mtx（§1），封口后无 syscall 越过。
            m_close_waiters->SealWakeAndDrainRegistrations();
            lock.lock();
            settled_now = _SettleClosedLocked();
        } else {
            // 并发/重复 Close：等首次调用者的真实物理落定事实（无独立短
            // 上限），使每个合法调用者返回当刻都观察到同一 Closed 终态。
            m_cv.wait(lock, [this] { return m_closed; });
        }
    }
    if (settled_now && m_closed_hook) m_closed_hook();
}

bool CoTCP::IsClosed() const noexcept {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_closed;
}

CoTCPListener::CoTCPListener(int fd, bbt::coroutine::CoObjectInfo info) noexcept
    : m_fd(fd), m_info(std::move(info)),
      m_close_waiters(std::make_shared<bbt::infra::detail::CloseWaiters>()) {}

CoTCPListener::~CoTCPListener() {
    std::lock_guard<std::mutex> lock(m_mtx);
    _DrainQuotaLocked();
    _SettleClosedLocked();
}

bbt::coroutine::CoObjectInfo CoTCPListener::GetObjectInfo() const { return m_info; }

result<void> CoTCPListener::_CheckEntry() const {
    if (g_bbt_tls_coroutine_co == nullptr)
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
            "TCP accept requires coroutine"));
    if (!_RuntimeRunning())
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "coroutine runtime not initialized"));
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

    // 对象身份在监听 socket 建立前申请：运行时未初始化即不产出对象。
    auto info = bbt::infra::detail::NewObjectInfo("tcp");
    if (!info) {
        ::freeaddrinfo(results);
        return result<SPtr>::err(std::move(info).error());
    }

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
            output = result<SPtr>::ok(SPtr(new CoTCPListener(fd, info.value())));
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
    // 归还，Close/析构经 DrainQuota 兜底。
    bool quota_admitted = false;
    if (m_inflight_admit) {
        if (!m_inflight_admit())
            return result<std::shared_ptr<CoTCP>>::err(MakeError(
                ErrorCode::Overloaded, "TCP accept: runtime max_inflight exceeded"));
        quota_admitted = true;
    }
    bool capacity_reserved = false;
    // §4.0.1.7：接纳容量预留的出账凭据。正常终态（adopt 转移 / release 归还）
    // 由本 op 唯一返回路径出账（容量归还 + 本凭据消费计数）；封口唤醒使挂起
    // Accept 照常走该返回路径，故不再需要停机期兜底出账。
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
        int fd;
        {
          // §1：最后一次封口检查 + 非阻塞 syscall 与物理 close 同锁配对。
          std::lock_guard<std::mutex> lock(m_mtx);
          if (m_closing || m_fd < 0)
            return result<std::shared_ptr<CoTCP>>::err(
                MakeError(ErrorCode::Closed, "TCP listener closed"));
          // accept() is transparently hooked and parks indefinitely on EAGAIN.
          // Use the native nonblocking syscall, then let CoWaiter own the wait.
          fd = static_cast<int>(::syscall(SYS_accept4, m_fd, nullptr, nullptr,
                                          SOCK_NONBLOCK | SOCK_CLOEXEC));
        }
        if (fd >= 0) {
            if (m_closing.load()) {
                ::close(fd);
                return result<std::shared_ptr<CoTCP>>::err(
                    MakeError(ErrorCode::Closed, "TCP listener closed"));
            }
            if (_SetNonBlocking(fd) != 0) {
                ::close(fd);
                return result<std::shared_ptr<CoTCP>>::err(
                    _Errno(ErrorCode::TransportError, "TCP accepted socket setup failed"));
            }
            auto info = bbt::infra::detail::NewObjectInfo("tcp");
            if (!info) {
                ::close(fd);
                return result<std::shared_ptr<CoTCP>>::err(std::move(info).error());
            }
            return result<std::shared_ptr<CoTCP>>::ok(
                std::shared_ptr<CoTCP>(new CoTCP(fd, std::move(info).value())));
        }
        if (errno == EINTR)
            continue;
        if (errno != EAGAIN && errno != EWOULDBLOCK)
            return result<std::shared_ptr<CoTCP>>::err(_Errno(ErrorCode::TransportError, "TCP accept failed"));
        {
          // 挂起前再确认未封口，不注册旧 fd 兴趣。
          std::lock_guard<std::mutex> lock(m_mtx);
          if (m_closing || m_fd < 0)
            return result<std::shared_ptr<CoTCP>>::err(
                MakeError(ErrorCode::Closed, "TCP listener closed"));
        }
        auto wait = _WaitFd(fd_to_wait, true, options, m_close_waiters.get());
        if (!wait)
            return result<std::shared_ptr<CoTCP>>::err(std::move(wait).error());
      }
    };
    auto output = run();
    _EndIo();
    if (output && m_accept_adopt_gate_for_test)
        m_accept_adopt_gate_for_test();
    if (output && m_accept_adopt && !m_accept_adopt(output.value())) {
        output.value()->Close();
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
}

bool CoTCPListener::_SettleClosedLocked() noexcept {
    if (m_closed)
        return false;
    _CloseFd();
    m_closed = true;
    m_cv.notify_all();
    return true;
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
        if (m_closing && m_inflight == 0)
            closed_now = _SettleClosedLocked();
        m_cv.notify_all();
    }
    if (closed_now && m_closed_hook) m_closed_hook();
}

void CoTCPListener::Close() noexcept {
    bool settled_now = false;
    {
        std::unique_lock<std::mutex> lock(m_mtx);
        if (!m_closing.exchange(true)) {
            _DrainQuotaLocked();   // 封口头归还仍持有的在途名额
            lock.unlock();
            // §1 + 关闭线性化：封口后无 accept4 可越过本锁；先唤醒在册等待者，
            // 再等在途「fd 兴趣登记」排空才物理释放 fd——登记中的 op 先完成登记
            // （fd 有效），晚到 op 在 BeginRegister 处得到明确 Closed，不在已关/
            // 复用 fd 上登记。
            m_close_waiters->SealWakeAndDrainRegistrations();
            lock.lock();
            settled_now = _SettleClosedLocked();
        } else {
            m_cv.wait(lock, [this] { return m_closed; });
        }
    }
    if (settled_now && m_closed_hook) m_closed_hook();
}

bool CoTCPListener::IsClosed() const noexcept {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_closed;
}

} // namespace bbt::infra::tcp
