#include <bbt/infra/CoUDP.hpp>

#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <string>

#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>   // g_scheduler->IsInitialized（运行时是否在跑）

#include "detail/IoSupport.hpp"

namespace bbt::infra::udp {
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

// coroutine 运行时是否在跑：进程寿命运行时只有「已初始化」一种活态，
// 不再有运行时代际（对象身份也不携带代际）。
bool _RuntimeRunning() noexcept {
    return g_scheduler != nullptr && g_scheduler->IsInitialized();
}

// 值类型 ⇄ sockaddr_in（最小切片仅 IPv4）。
result<sockaddr_in> _ToSockaddr(const SocketAddress& address) {
    if (address.ip.empty())
        return result<sockaddr_in>::err(MakeError(ErrorCode::InvalidArgument,
            "SocketAddress.ip must not be empty"));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    if (::inet_pton(AF_INET, address.ip.c_str(), &addr.sin_addr) != 1)
        return result<sockaddr_in>::err(MakeError(ErrorCode::InvalidArgument,
            "SocketAddress.ip is not a numeric IPv4 address"));
    addr.sin_port = ::htons(address.port);
    return result<sockaddr_in>::ok(addr);
}

SocketAddress _FromSockaddr(const sockaddr_in& addr) {
    SocketAddress out;
    char ip[INET_ADDRSTRLEN]{};
    if (::inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip)) != nullptr)
        out.ip = ip;
    out.port = ::ntohs(addr.sin_port);
    return out;
}

// 一次 recvmsg（MSG_TRUNC 检测截断）：Ok/WouldBlock/错误三态。
// 零长度 datagram 合法：bytes=0、truncated=false、peer 有效。
// dst.size=0 且有报文：bytes=0、truncated=true（报头边界仍返回）。
enum class RecvOutcome { Ok, WouldBlock, Error };

RecvOutcome _RecvOnce(int fd, MutableBytes dst, SocketAddress& peer,
                      std::size_t& bytes, bool& truncated, Error& error) {
    iovec iov{};
    iov.iov_base = dst.data;
    iov.iov_len  = dst.size;
    sockaddr_in peer_addr{};
    msghdr msg{};
    msg.msg_name    = &peer_addr;
    msg.msg_namelen = sizeof(peer_addr);
    msg.msg_iov     = &iov;
    msg.msg_iovlen  = 1;
    // The adapter owns the wait: bypass the coroutine's transparent hook.
    const ssize_t n = ::recvmsg(fd, &msg, MSG_TRUNC | MSG_DONTWAIT);
    if (n >= 0) {
        peer  = _FromSockaddr(peer_addr);
        bytes = std::min(static_cast<std::size_t>(n), dst.size);
        // MSG_TRUNC 置位：报文超 dst.size，超出部分已被内核丢弃；
        // 报文恰好等于容量时 MSG_TRUNC 不置位（Linux 语义）。
        truncated = (msg.msg_flags & MSG_TRUNC) != 0;
        return RecvOutcome::Ok;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK)
        return RecvOutcome::WouldBlock;
    if (errno == EINTR)
        return RecvOutcome::WouldBlock; // 可等待方法由上层检查终止条件后重试
    error = _Errno(ErrorCode::TransportError, "UDP receive failed");
    return RecvOutcome::Error;
}

RecvOutcome _SendOnce(int fd, ConstBytes packet, const sockaddr* addr,
                      socklen_t addr_len, std::size_t& bytes, Error& error) {
    const ssize_t n = ::sendto(fd, packet.data, packet.size, MSG_DONTWAIT, addr, addr_len);
    if (n >= 0) {
        bytes = static_cast<std::size_t>(n);
        return RecvOutcome::Ok;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK)
        return RecvOutcome::WouldBlock;
    if (errno == EINTR)
        return RecvOutcome::WouldBlock; // 同上，由可等待方法重试
    error = _Errno(ErrorCode::TransportError, "UDP send failed");
    return RecvOutcome::Error;
}

} // namespace

CoUDP::CoUDP(int fd, bbt::coroutine::CoObjectInfo info) noexcept
    : m_fd(fd), m_info(std::move(info)),
      m_close_waiters(std::make_shared<bbt::infra::detail::CloseWaiters>()) {}

CoUDP::~CoUDP() {
    std::lock_guard<std::mutex> lock(m_mtx);
    // §4.0.1.7：未走 Close 的析构兜底归还剩余名额，并物理释放。
    while (m_quota_held > 0) {
        --m_quota_held;
        if (m_inflight_release) m_inflight_release();
    }
    _SettleClosedLocked();
}

bbt::coroutine::CoObjectInfo CoUDP::GetObjectInfo() const { return m_info; }

result<CoUDP::SPtr> CoUDP::BindUDP(SocketAddress local) {
    // §4.0.1：控制线程配置操作；要求 coroutine 运行时已初始化（对象身份
    // 的前置条件是运行时已初始化，不再有代际）。
    if (local.ip.empty())
        return result<SPtr>::err(MakeError(ErrorCode::InvalidArgument,
            "BindUDP: SocketAddress.ip must not be empty (use explicit wildcard)"));

    auto addr = _ToSockaddr(local);
    if (!addr)
        return result<SPtr>::err(std::move(addr).error());
    if (!_RuntimeRunning())
        return result<SPtr>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "BindUDP requires an initialized coroutine runtime"));

    const int fd = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (fd < 0)
        return result<SPtr>::err(_Errno(ErrorCode::TransportError,
            "UDP socket creation failed"));
    if (_SetNonBlocking(fd) != 0) {
        const Error error = _Errno(ErrorCode::TransportError,
            "UDP non-blocking setup failed");
        ::close(fd);
        return result<SPtr>::err(error);
    }
    int reuse = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    if (::bind(fd, reinterpret_cast<const sockaddr*>(&addr.value()),
               sizeof(addr.value())) != 0) {
        const Error error = _Errno(ErrorCode::TransportError, "UDP bind failed");
        ::close(fd); // §4.0.1.6：失败即无对象，内部负责 close 已创建的 FD
        return result<SPtr>::err(error);
    }

    auto info = bbt::infra::detail::NewObjectInfo("udp");
    if (!info) {
        ::close(fd);
        return result<SPtr>::err(std::move(info).error());
    }
    auto object = std::shared_ptr<CoUDP>(new CoUDP(fd, std::move(info).value()));
    // 读取实际绑定地址（端口 0 时由内核分配）。
    sockaddr_in actual{};
    socklen_t actual_len = sizeof(actual);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&actual), &actual_len) == 0)
        object->m_local = _FromSockaddr(actual);
    return result<SPtr>::ok(std::move(object));
}

// §4 入口检查：参数 → 受管协程上下文 → coroutine 运行时是否已初始化；成立
// 即返回，不挂起。§4 的 Try* 规则：Try* 只检查参数、上下文与关闭状态——只有
// 协程方法要求「当前处于受管协程内」，运行时可用性对两类入口都成立。
result<void> CoUDP::_CheckEntry(bool in_coroutine_only) const {
    if (in_coroutine_only && g_bbt_tls_coroutine_co == nullptr)
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
            "CoUDP operation must run in coroutine context"));
    if (!_RuntimeRunning())
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "coroutine runtime not initialized"));
    return result<void>::ok();
}

SocketAddress CoUDP::LocalAddress() const {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_local;
}

result<DatagramRead> CoUDP::TryReceive(MutableBytes dst) {
    if (dst.size > 0 && dst.data == nullptr)
        return result<DatagramRead>::err(MakeError(ErrorCode::InvalidArgument,
            "TryReceive: null buffer with non-zero size"));
    auto entry = _CheckEntry(/*in_coroutine_only=*/false);
    if (!entry)
        return result<DatagramRead>::err(std::move(entry).error());

    std::lock_guard<std::mutex> lock(m_mtx);
    if (m_close_requested || m_fd < 0)
        return result<DatagramRead>::err(MakeError(ErrorCode::Closed,
            "UDP socket closed"));

    SocketAddress peer;
    std::size_t bytes = 0;
    bool truncated = false;
    Error error;
    const auto outcome = _RecvOnce(m_fd, dst, peer, bytes, truncated, error);
    if (outcome == RecvOutcome::Ok) {
        DatagramRead read;
        read.state     = IoState::Ok;
        read.bytes     = bytes;
        read.peer      = peer;
        read.truncated = truncated;
        return result<DatagramRead>::ok(read);
    }
    if (outcome == RecvOutcome::WouldBlock) {
        DatagramRead read;
        read.state     = IoState::WouldBlock;
        read.bytes     = 0;
        read.truncated = false;
        return result<DatagramRead>::ok(read);
    }
    return result<DatagramRead>::err(std::move(error));
}

IoResult CoUDP::TrySend(ConstBytes packet, const SocketAddress& peer) {
    if (packet.size > 0 && packet.data == nullptr)
        return IoResult::err(MakeError(ErrorCode::InvalidArgument,
            "TrySend: null buffer with non-zero size"));
    auto addr = _ToSockaddr(peer);
    if (!addr)
        return IoResult::err(std::move(addr).error());
    auto entry = _CheckEntry(/*in_coroutine_only=*/false);
    if (!entry)
        return IoResult::err(std::move(entry).error());

    std::lock_guard<std::mutex> lock(m_mtx);
    if (m_close_requested || m_fd < 0)
        return IoResult::err(MakeError(ErrorCode::Closed, "UDP socket closed"));

    std::size_t bytes = 0;
    Error error;
    const auto outcome = _SendOnce(m_fd, packet,
        reinterpret_cast<const sockaddr*>(&addr.value()), sizeof(addr.value()),
        bytes, error);
    if (outcome == RecvOutcome::Ok)
        return IoResult::ok(IoProgress{IoState::Ok, bytes});
    if (outcome == RecvOutcome::WouldBlock)
        return IoResult::ok(IoProgress{IoState::WouldBlock, 0});
    return IoResult::err(std::move(error));
}

// §5.2 组合等待：readable/writable interest + 绝对 deadline + close（对象级
// CloseWaiters）单轮一次性挂起。等待注册由上游 CoWaiter 完成（先登记后挂起，
// 唤醒不丢失）；infra 不复制等待状态机。fd 就绪只表示「可以重试」，不是操作
// 成功。封口唤醒以 Completed 抵达，本函数解释为 Closed。
CoUDP::WaitOutcome CoUDP::_Wait(bool readable, const CallOptions& options) {
    if (g_bbt_tls_coroutine_co == nullptr)
        return WaitOutcome::InvalidContext;

    auto waiter = CoWaiter::Create();
    // 关闭线性化：与对象级 Close 的封口原子配对（见 detail::CloseWaiters
    // 「等待兴趣登记屏障」）。取得登记 ⇒ 尚未封口，物理 close 会等本登记释放后
    // 才发生，故此后读 m_fd 有效；已封口即返回 Closed，绝不在已关/复用 fd 上
    // 登记旧兴趣。RAII 配对保证异常/提前返回时不泄漏登记。
    bbt::infra::detail::CloseRegisterClaim claim(m_close_waiters.get());
    if (!claim.acquired())
        return WaitOutcome::Closed;
    const int fd = m_fd;
    CombinedWaitOptions wait;
    wait.fd             = fd;
    wait.want_readable  = readable;
    wait.want_writeable = !readable;
    wait.deadline       = options.deadline;
    // 读 m_fd 与物理 close 由登记屏障同步（见上）；登记在事件登记成功后、
    // 真正挂起前完成；已封口时 Add 失败，自行 Notify 走 PENDING 路径，不丢唤醒。
    bool registered = false;
    const auto status = waiter->Wait(wait,
        [&waiter, &registered, &claim, this]() -> bool {
            // 唤醒登记必须压在登记屏障内、先于 claim.release()（见 CoTCP.cc
            // _WaitFd 同序说明）：先放行屏障会让对象级封口在「屏障已放行、唤醒
            // 登记未落」窗口里封口空 waiters 并物理 close fd，上游 Hook_Close 以
            // POLL_EVENT_CLOSED 首胜，等待被兜底成 Cancelled 而非 Closed。Add
            // 计入屏障后，封口只在唤醒登记落定后物理 close，自行 Notify 先胜。
            registered = m_close_waiters->Add(waiter);
            if (!registered)
                waiter->Notify();
            claim.release();
            return true;
        });
    if (registered)
        m_close_waiters->Remove(waiter.get());
    switch (status) {
    case CombinedWaitStatus::FdReadable:
    case CombinedWaitStatus::FdWriteable:
        return WaitOutcome::Ready;
    case CombinedWaitStatus::Completed: // 封口唤醒（CloseAndWakeAll）
        return WaitOutcome::Closed;
    case CombinedWaitStatus::Cancelled: // 协程级 RequestCancel
        return WaitOutcome::Cancelled;
    case CombinedWaitStatus::TimedOut:
        return WaitOutcome::TimedOut;
    case CombinedWaitStatus::InvalidContext:
        return WaitOutcome::InvalidContext;
    default:
        return WaitOutcome::RuntimeUnavailable;
    }
}

result<DatagramRead> CoUDP::Receive(MutableBytes dst, const CallOptions& options) {
    auto entry = _CheckEntry(/*in_coroutine_only=*/true);
    if (!entry)
        return result<DatagramRead>::err(std::move(entry).error());
    if (dst.size > 0 && dst.data == nullptr)
        return result<DatagramRead>::err(MakeError(ErrorCode::InvalidArgument,
            "Receive: null buffer with non-zero size"));

    // §4.0.1.7（Issue #32）：Runtime 在途门禁先于在途计数——容量满即
    // Overloaded、立即返回不挂起；名额计入 m_quota_held，op 结束归还 1，
    // Close/析构 DrainQuota 兜底归还。
    bool quota_admitted = false;
    if (m_inflight_admit) {
        if (!m_inflight_admit())
            return result<DatagramRead>::err(MakeError(ErrorCode::Overloaded,
                "UDP receive: runtime max_inflight exceeded"));
        quota_admitted = true;
    }

    // §6.4.3：在途计数先于等待登记，物理 close 在计数归零后才发生；
    // 封口后入口立即拒绝（Closed），等待中封口则由 CloseWaiters 唤醒 Closed。
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        if (m_close_requested || m_fd < 0) {
            if (quota_admitted && m_inflight_release) m_inflight_release();
            return result<DatagramRead>::err(MakeError(ErrorCode::Closed,
                "UDP socket closed"));
        }
        ++m_inflight;
        if (quota_admitted)
            ++m_quota_held;
    }
    // §4.0.1.7 测试接缝：名额已登记，等待循环尚未进入。
    if (m_wait_entry_gate_for_test)
        m_wait_entry_gate_for_test();

    result<DatagramRead> output = result<DatagramRead>::err(
        MakeError(ErrorCode::Closed, "UDP socket closed"));
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_close_requested || m_fd < 0)
                break; // 封口：在途等待返回 Closed
        }
        const auto wait = _Wait(/*readable=*/true, options);
        if (wait != WaitOutcome::Ready) {
            Error error =
                wait == WaitOutcome::Closed
                    ? MakeError(ErrorCode::Closed, "UDP socket closed")
                    : wait == WaitOutcome::Cancelled
                        ? MakeError(ErrorCode::Cancelled, "operation cancelled")
                        : wait == WaitOutcome::TimedOut
                            ? MakeError(ErrorCode::TimedOut,
                                "operation timed out")
                            : wait == WaitOutcome::InvalidContext
                                ? MakeError(ErrorCode::InvalidContext,
                                    "invalid coroutine context")
                                : MakeError(ErrorCode::RuntimeUnavailable,
                                    "coroutine wait unavailable");
            output = result<DatagramRead>::err(std::move(error));
            break;
        }
        // Ready：重试一次 syscall。等待期间对象受在途计数保护，fd 不会
        // 被物理 close；锁内复查封口状态后再触碰 fd。
        SocketAddress peer;
        std::size_t bytes = 0;
        bool truncated = false;
        Error error;
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_close_requested || m_fd < 0)
                break;
            const auto outcome =
                _RecvOnce(m_fd, dst, peer, bytes, truncated, error);
            if (outcome == RecvOutcome::Ok) {
                DatagramRead read;
                read.state     = IoState::Ok;
                read.bytes     = bytes;
                read.peer      = peer;
                read.truncated = truncated;
                output = result<DatagramRead>::ok(read);
                break;
            }
            if (outcome == RecvOutcome::Error) {
                output = result<DatagramRead>::err(std::move(error));
                break;
            }
            // WouldBlock：虚假唤醒或 EINTR，回到等待（同一绝对 deadline，
            // 重试不重置超时）。
        }
    }

    bool closed_now = false;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        --m_inflight;
        if (m_quota_held > 0) {
            --m_quota_held;
            if (m_inflight_release) m_inflight_release();
        }
        // 在途归零即物理落定：Close 的有界等待超时（或同线程恢复）后由最后
        // 一个退出的 op 完成收口。
        if (m_close_requested && m_inflight == 0)
            closed_now = _SettleClosedLocked();
        m_cv.notify_all();
    }
    if (closed_now && m_closed_hook)
        m_closed_hook();
    return output;
}

IoResult CoUDP::Send(ConstBytes packet, const SocketAddress& peer,
                     const CallOptions& options) {
    if (packet.size > 0 && packet.data == nullptr)
        return IoResult::err(MakeError(ErrorCode::InvalidArgument,
            "Send: null buffer with non-zero size"));
    auto addr = _ToSockaddr(peer);
    if (!addr)
        return IoResult::err(std::move(addr).error());
    auto entry = _CheckEntry(/*in_coroutine_only=*/true);
    if (!entry)
        return IoResult::err(std::move(entry).error());

    bool quota_admitted = false;
    if (m_inflight_admit) {
        if (!m_inflight_admit())
            return IoResult::err(MakeError(ErrorCode::Overloaded,
                "UDP send: runtime max_inflight exceeded"));
        quota_admitted = true;
    }

    {
        std::lock_guard<std::mutex> lock(m_mtx);
        if (m_close_requested || m_fd < 0) {
            if (quota_admitted && m_inflight_release) m_inflight_release();
            return IoResult::err(MakeError(ErrorCode::Closed,
                "UDP socket closed"));
        }
        ++m_inflight;
        if (quota_admitted)
            ++m_quota_held;
    }
    if (m_wait_entry_gate_for_test)
        m_wait_entry_gate_for_test();

    IoResult output =
        IoResult::err(MakeError(ErrorCode::Closed, "UDP socket closed"));
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_close_requested || m_fd < 0)
                break;
        }
        const auto wait = _Wait(/*readable=*/false, options);
        if (wait != WaitOutcome::Ready) {
            Error error =
                wait == WaitOutcome::Closed
                    ? MakeError(ErrorCode::Closed, "UDP socket closed")
                    : wait == WaitOutcome::Cancelled
                        ? MakeError(ErrorCode::Cancelled, "operation cancelled")
                        : wait == WaitOutcome::TimedOut
                            ? MakeError(ErrorCode::TimedOut,
                                "operation timed out")
                            : wait == WaitOutcome::InvalidContext
                                ? MakeError(ErrorCode::InvalidContext,
                                    "invalid coroutine context")
                                : MakeError(ErrorCode::RuntimeUnavailable,
                                    "coroutine wait unavailable");
            output = IoResult::err(std::move(error));
            break;
        }
        std::size_t bytes = 0;
        Error error;
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_close_requested || m_fd < 0)
                break;
            const auto outcome = _SendOnce(m_fd, packet,
                reinterpret_cast<const sockaddr*>(&addr.value()),
                sizeof(addr.value()), bytes, error);
            if (outcome == RecvOutcome::Ok) {
                output = IoResult::ok(IoProgress{IoState::Ok, bytes});
                break;
            }
            if (outcome == RecvOutcome::Error) {
                output = IoResult::err(std::move(error));
                break;
            }
        }
    }

    bool closed_now = false;
    {
        std::lock_guard<std::mutex> lock(m_mtx);
        --m_inflight;
        if (m_quota_held > 0) {
            --m_quota_held;
            if (m_inflight_release) m_inflight_release();
        }
        if (m_close_requested && m_inflight == 0)
            closed_now = _SettleClosedLocked();
        m_cv.notify_all();
    }
    if (closed_now && m_closed_hook)
        m_closed_hook();
    return output;
}

void CoUDP::_CloseFd() noexcept {
    // 物理 close：只在收口路径调用（m_mtx held）。
    if (m_fd >= 0) {
        ::close(m_fd);
        m_fd = -1;
    }
}

bool CoUDP::_SettleClosedLocked() noexcept {
    if (m_physically_closed)
        return false;
    _CloseFd();
    m_physically_closed = true;
    m_cv.notify_all();
    return true;
}

// §1（进程寿命运行时修订）：Close 幂等、任意线程可调用。序列：封口（拒绝新
// 调用、归还已持有名额）→ 唤醒全部挂起等待者（CloseWaiters；封口后登记失败
// 者自行 Notify）→ 等在途「fd 兴趣登记」排空（登记屏障，见 detail::CloseWaiters）
// → 物理 close 并一次性跑 closed hook。不等待后端结果、不 flush。
// 与 CoTCP 同一关闭线性化：物理 close 不早于任何已在途的 fd 兴趣登记完成，
// 晚到 op 在 BeginRegister 处得到明确 Closed，不在已关/复用 fd 上登记；并发
// Close 调用者无独立短上限地等首次调用者的真实物理落定事实（同一终态）。
void CoUDP::Close() noexcept {
    bool settled_now = false;
    {
        std::unique_lock<std::mutex> lock(m_mtx);
        if (!m_close_requested) {
            m_close_requested = true;
            // §4.0.1.7：封口头归还本对象仍持有的全部名额；op 若仍返回，其
            // 收尾见 m_quota_held==0 不再重复归还。
            while (m_quota_held > 0) {
                --m_quota_held;
                if (m_inflight_release) m_inflight_release();
            }
            lock.unlock();
            m_close_waiters->SealWakeAndDrainRegistrations();
            lock.lock();
            settled_now = _SettleClosedLocked();
        } else {
            // 并发/重复 Close：无独立短上限地等首次调用者的真实物理落定事实。
            m_cv.wait(lock, [this] { return m_physically_closed; });
        }
    }
    if (settled_now && m_closed_hook)
        m_closed_hook();
}

bool CoUDP::IsClosed() const noexcept {
    std::lock_guard<std::mutex> lock(m_mtx);
    return m_physically_closed;
}

} // namespace bbt::infra::udp
