#include "transport/TransportRuntimeImpl.hpp"

#include <algorithm>
#include <limits>
#include <utility>

#include <bbt/coroutine/object/CoObject.hpp>

#include "detail/IoSupport.hpp"

namespace bbt::infra {

namespace transport_detail {

namespace {

// co-io-adapter/v1 §4.0.1.7：容量名额归还，只在真实物理关闭落定后调用一次。
void ReleaseTransportSlot(std::mutex& mtx, std::size_t& count) noexcept {
    std::lock_guard<std::mutex> lk(mtx);
    if (count > 0) --count;
}

} // namespace

result<void> TransportRuntimeImpl::Start() {
    // 对象身份、完成信号与工厂门禁都要绑运行时代际：Scheduler 未启动或
    // 属于其他代际时本 owner 不可用。
    const auto gen = bbt::coroutine::CurrentRuntimeGeneration();
    if (gen == 0 || gen != m_info.generation)
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "scheduler not running or runtime generation mismatch"));

    int expected = kCreated;
    if (!m_state.compare_exchange_strong(expected, kRunning))
        return result<void>::err(MakeError(
            m_state.load() == kRunning ? ErrorCode::InvalidArgument
                                       : ErrorCode::Closed,
            m_state.load() == kRunning ? "transport runtime already started"
                                       : "transport runtime is closing or closed"));
    return result<void>::ok();
}

result<void> TransportRuntimeImpl::CheckUsableForFactory() const {
    if (m_state.load() != kRunning)
        return result<void>::err(MakeError(
            m_state.load() == kCreated ? ErrorCode::RuntimeUnavailable
                                       : ErrorCode::Closed,
            m_state.load() == kCreated ? "transport runtime not started"
                                       : "transport runtime is closing or closed"));
    if (!m_close.IsOpen())
        return result<void>::err(
            MakeError(ErrorCode::Closed, "transport runtime is closing or closed"));
    // §4.0.1.7（Issue #32）：工厂同样受运行时代际门禁——Stop→Start 后旧代际
    // owner 不得再产出新对象（对象钉住当前代际，跨代交付只会产出入场即失败的
    // 对象）。
    const auto gen = bbt::coroutine::CurrentRuntimeGeneration();
    if (gen == 0 || gen != m_info.generation)
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "transport runtime belongs to another runtime generation"));
    return result<void>::ok();
}

// §4.0.1.7：容量原子预留。检查与递增在同一临界区内完成，超限立即拒绝、
// 不静默排队；调用方在失败或物理关闭落定时回退/归还一次。
result<void> TransportRuntimeImpl::ReserveConnectionSlot(
    std::string_view factory) noexcept {
    std::lock_guard<std::mutex> lk(m_transport_mtx);
    if (m_transport_sealed)
        return result<void>::err(
            MakeError(ErrorCode::Closed, "transport runtime is closing"));
    if (m_transport_count >= m_limits.max_connections)
        return result<void>::err(MakeError(ErrorCode::Overloaded,
            std::string(factory) + ": max_connections exceeded"));
    ++m_transport_count;
    return result<void>::ok();
}

result<std::shared_ptr<CoTCP>> TransportRuntimeImpl::DialTCP(
    TcpEndpoint endpoint, const CallOptions& options) {
    auto usable = CheckUsableForFactory();
    if (!usable)
        return result<std::shared_ptr<CoTCP>>::err(std::move(usable).error());
    if (endpoint.host.empty())
        return result<std::shared_ptr<CoTCP>>::err(MakeError(
            ErrorCode::InvalidArgument, "DialTCP: endpoint.host must not be empty"));
    if (endpoint.port == 0)
        return result<std::shared_ptr<CoTCP>>::err(MakeError(
            ErrorCode::InvalidArgument, "DialTCP: endpoint.port must not be 0"));

    // 容量在真实 connect 发起之前预留——拒绝路径不建活跃连接。
    auto reserved = ReserveConnectionSlot("DialTCP");
    if (!reserved)
        return result<std::shared_ptr<CoTCP>>::err(std::move(reserved).error());

    // §4.0.1.7（Issue #32）：受管 DialTCP 的等待段（DNS 解析 + 非阻塞
    // connect 等待）占一个在途名额；容量满立即 Overloaded，不排队不挂起。
    if (!m_inflight_quota->TryAdmit()) {
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        MaybeMarkClosed();
        return result<std::shared_ptr<CoTCP>>::err(MakeError(
            ErrorCode::Overloaded, "DialTCP: max_inflight exceeded"));
    }

    // Issue #32 F2：等待段名额不能依赖协程栈 RAII——Scheduler::Stop 对挂起
    // 协程直接销毁、不展开栈，栈上任何局部对象都不会析构。因此「未完成 dial」
    // 的归还职责放在本 owner 的堆对象上：permit 由 m_dial_permits 强持有
    // （不是栈上局部），正常终态（等待段落定、dial 返回）由协程返回后移除并
    // 归还；RequestClose() 对每个未完成 dial Fail() 归还并唤醒其等待段；
    // owner 析构时成员析构（~DialWaitPermit）归还剩余。栈上只保留弱引用观察
    // 终态。
    auto dial_cancel = std::make_shared<bbt::coroutine::CancellationSource>();
    auto permit = std::make_shared<tcp::DialWaitPermit>(
        [quota = m_inflight_quota] { quota->Release(); });
    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        if (!m_transport_sealed) {
            m_dial_cancels.push_back(dial_cancel);
            m_dial_permits.push_back(permit);
        }
    }

    tcp::DialWaitOptions dial_wait;
    dial_wait.extra_cancel = dial_cancel->Token();
    // 测试接缝：等待段挂起落定（协程已进入 parked 表）后回调一次，用于
    // 确定性区分「等待中取消/Stop」与「未进入等待」。
    if (m_dial_wait_entry_gate_for_test) {
        auto gate = m_dial_wait_entry_gate_for_test;
        dial_wait.dns_on_registered = gate;
        dial_wait.connect_on_registered = std::move(gate);
    }

    auto dialed = tcp::CoTCP::DialTCP(std::move(endpoint.host), endpoint.port,
                                      options, std::move(dial_wait));
    // 等待段已结束（成功/失败/取消/超时）：正常路径归还名额并从登记簿移除
    // ——归还与 permit->Fail 幂等互斥（released 原子位），不重复。
    permit->Fail();
    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        auto& cans = m_dial_cancels;
        cans.erase(std::remove(cans.begin(), cans.end(), dial_cancel),
                   cans.end());
        auto& perms = m_dial_permits;
        perms.erase(std::remove(perms.begin(), perms.end(), permit),
                    perms.end());
    }

    if (!dialed) {
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        MaybeMarkClosed();
        return result<std::shared_ptr<CoTCP>>::err(std::move(dialed).error());
    }
    auto connection = std::move(dialed).value();
    bool sealed = false;
    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        sealed = m_transport_sealed;
        if (!sealed) {
            std::weak_ptr<TransportRuntimeImpl> weak =
                std::static_pointer_cast<TransportRuntimeImpl>(
                    shared_from_this());
            std::weak_ptr<CoTCP> child = connection;
            connection->SetClosedHook([weak, child] {
                auto rt = weak.lock();
                auto object = child.lock();
                if (rt && object)
                    rt->OnTransportClosed(object.get());
            });
            connection->SetInflightHooks(
                [quota = m_inflight_quota] { return quota->TryAdmit(); },
                [quota = m_inflight_quota] { quota->Release(); },
                m_inflight_quota);
            m_tcp_children.push_back(connection);
        }
    }
    if (sealed) {
        // 交付即已封口：不再托管，物理关闭并归还容量。
        connection->RequestClose();
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        MaybeMarkClosed();
        return result<std::shared_ptr<CoTCP>>::err(MakeError(
            ErrorCode::Closed, "transport runtime closed during TCP dial"));
    }
    return result<std::shared_ptr<CoTCP>>::ok(std::move(connection));
}

result<std::shared_ptr<CoTCPListener>> TransportRuntimeImpl::ListenTCP(
    SocketAddress local, unsigned backlog) {
    auto usable = CheckUsableForFactory();
    if (!usable)
        return result<std::shared_ptr<CoTCPListener>>::err(
            std::move(usable).error());
    if (local.ip.empty())
        return result<std::shared_ptr<CoTCPListener>>::err(MakeError(
            ErrorCode::InvalidArgument,
            "ListenTCP: SocketAddress.ip must not be empty (use explicit wildcard)"));
    if (backlog == 0 || backlog > static_cast<unsigned>(std::numeric_limits<int>::max()))
        return result<std::shared_ptr<CoTCPListener>>::err(MakeError(
            ErrorCode::InvalidArgument, "ListenTCP: backlog must be 1..INT_MAX"));

    auto reserved = ReserveConnectionSlot("ListenTCP");
    if (!reserved)
        return result<std::shared_ptr<CoTCPListener>>::err(
            std::move(reserved).error());

    auto bound = tcp::CoTCPListener::ListenTCP(
        std::move(local.ip), local.port, static_cast<int>(backlog));
    if (!bound) {
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        MaybeMarkClosed();
        return result<std::shared_ptr<CoTCPListener>>::err(std::move(bound).error());
    }
    auto listener = std::move(bound).value();
    std::unique_lock<std::mutex> transport_lock(m_transport_mtx);
    if (m_transport_sealed) {
        transport_lock.unlock();
        listener->RequestClose();
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        MaybeMarkClosed();
        return result<std::shared_ptr<CoTCPListener>>::err(MakeError(
            ErrorCode::Closed, "transport runtime closed during TCP listen"));
    }
    std::weak_ptr<TransportRuntimeImpl> weak =
        std::static_pointer_cast<TransportRuntimeImpl>(shared_from_this());
    std::weak_ptr<CoTCPListener> child = listener;
    listener->SetClosedHook([weak, child] {
        auto rt = weak.lock();
        auto object = child.lock();
        if (rt && object)
            rt->OnTransportClosed(object.get());
    });
    listener->SetAcceptHooks(
        [weak] {
            auto rt = weak.lock();
            if (!rt) return false;
            std::lock_guard<std::mutex> lk(rt->m_transport_mtx);
            if (rt->m_transport_sealed ||
                rt->m_transport_count >= rt->m_limits.max_connections)
                return false;
            ++rt->m_transport_count;
            return true;
        },
        [weak] {
            if (auto rt = weak.lock()) {
                ReleaseTransportSlot(rt->m_transport_mtx, rt->m_transport_count);
                rt->MaybeMarkClosed();
            }
        },
        [weak](std::shared_ptr<CoTCP> accepted) {
            auto rt = weak.lock();
            if (!rt) return false;
            std::lock_guard<std::mutex> lk(rt->m_transport_mtx);
            if (rt->m_transport_sealed) return false;
            std::weak_ptr<CoTCP> child = accepted;
            accepted->SetClosedHook([weak, child] {
                auto rt_locked = weak.lock();
                auto child_locked = child.lock();
                if (rt_locked && child_locked)
                    rt_locked->OnTransportClosed(child_locked.get());
            });
            // §4.0.1.7：接纳的 socket 同样挂本 owner 在途账本——其
            // ReadSome/WriteSome/WriteAll 占用同一 max_inflight 预算。
            accepted->SetInflightHooks(
                [quota = rt->m_inflight_quota] { return quota->TryAdmit(); },
                [quota = rt->m_inflight_quota] { quota->Release(); },
                rt->m_inflight_quota);
            rt->m_tcp_children.push_back(std::move(accepted));
            return true;
        });
    // §4.0.1.7：listener 的 Accept 占用同一 max_inflight 账本。
    listener->SetInflightHooks(
        [quota = m_inflight_quota] { return quota->TryAdmit(); },
        [quota = m_inflight_quota] { quota->Release(); },
        m_inflight_quota);
    m_tcp_children.push_back(listener);
    transport_lock.unlock();
    return result<std::shared_ptr<CoTCPListener>>::ok(std::move(listener));
}

result<std::shared_ptr<CoUDP>> TransportRuntimeImpl::BindUDP(
    SocketAddress local) {
    auto usable = CheckUsableForFactory();
    if (!usable)
        return result<std::shared_ptr<CoUDP>>::err(std::move(usable).error());

    auto reserved = ReserveConnectionSlot("BindUDP");
    if (!reserved)
        return result<std::shared_ptr<CoUDP>>::err(std::move(reserved).error());

    auto bound = udp::CoUDP::BindUDP(std::move(local));
    if (!bound) {
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        MaybeMarkClosed();
        return result<std::shared_ptr<CoUDP>>::err(std::move(bound).error());
    }
    auto socket = std::move(bound).value();
    bool sealed = false;
    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        sealed = m_transport_sealed;
        if (!sealed) {
            std::weak_ptr<TransportRuntimeImpl> weak =
                std::static_pointer_cast<TransportRuntimeImpl>(
                    shared_from_this());
            std::weak_ptr<CoUDP> child = socket;
            socket->SetClosedHook([weak, child] {
                auto rt = weak.lock();
                auto object = child.lock();
                if (rt && object)
                    rt->OnTransportClosed(object.get());
            });
            // §4.0.1.7：UDP 的 Receive/Send 挂起段占同一 max_inflight 账本。
            socket->SetInflightHooks(
                [quota = m_inflight_quota] { return quota->TryAdmit(); },
                [quota = m_inflight_quota] { quota->Release(); },
                m_inflight_quota);
            m_tcp_children.push_back(socket);
        }
    }
    if (sealed) {
        socket->RequestClose();
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        MaybeMarkClosed();
        return result<std::shared_ptr<CoUDP>>::err(MakeError(
            ErrorCode::Closed, "transport runtime closed during UDP bind"));
    }
    return result<std::shared_ptr<CoUDP>>::ok(std::move(socket));
}

void TransportRuntimeImpl::RequestClose() noexcept {
    if (!m_close.BeginClose())
        return;
    m_state.store(kClosingOrClosed);
    std::vector<std::shared_ptr<ICoCloseable>> transports;
    std::vector<std::shared_ptr<bbt::coroutine::CancellationSource>> cancels;
    std::vector<std::shared_ptr<tcp::DialWaitPermit>> permits;
    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        m_transport_sealed = true;
        transports = m_tcp_children;
        cancels.swap(m_dial_cancels);
        permits.swap(m_dial_permits);
    }
    // Issue #32 F2：每个未完成的受管 DialTCP 等待段名额先归还（Fail 幂等，
    // 与协程正常返回路径的 Fail 互斥），再经取消源唤醒挂起协程——协程走正常
    // 返回路径到登记簿移除点（已清空，无重复），不依赖硬销毁兜底。
    for (auto& permit : permits)
        permit->Fail();
    for (auto& cancel : cancels)
        cancel->RequestCancel();
    // 一个 socket 只有一个物理关闭责任方：本 owner 只发起关闭，落定以对象侧
    // ClosedHook 为唯一信号（容量归还与该信号配对）。
    for (auto& transport : transports)
        transport->RequestClose();
    MaybeMarkClosed();
}

void TransportRuntimeImpl::OnTransportClosed(const ICoCloseable* child) noexcept {
    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        if (m_transport_count > 0)
            --m_transport_count;
        m_tcp_children.erase(
            std::remove_if(m_tcp_children.begin(), m_tcp_children.end(),
                           [child](const auto& item) {
                               return item.get() == child;
                           }),
            m_tcp_children.end());
    }
    MaybeMarkClosed();
}

void TransportRuntimeImpl::MaybeMarkClosed() noexcept {
    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        // 物理清理落定 = 已封口（不再接纳新资源）且受管 transport 名额归零。
        // 未封口时对象正常关闭不触发落定。
        if (!m_transport_sealed || m_transport_count != 0)
            return;
    }
    // 幂等：MarkClosed 只首次落实（重复调用不重复通知）。
    m_close.MarkClosed();
    NotifyClosed();
}

} // namespace transport_detail

// P2 公开装配入口：Create 只校验装配参数并建立身份；Start 才实际允许工厂
// 产出资源。二者均在控制线程使用，不挂起协程。
result<std::shared_ptr<TransportRuntime>>
TransportRuntime::Create(NetworkLimits limits) {
    auto valid = ValidateNetworkLimits(limits);
    if (!valid)
        return result<std::shared_ptr<TransportRuntime>>::err(
            std::move(valid).error());
    auto info = bbt::infra::detail::NewObjectInfo("infra.transport_runtime");
    if (!info)
        return result<std::shared_ptr<TransportRuntime>>::err(
            std::move(info).error());
    std::shared_ptr<bbt::coroutine::CompletionSignal> sig;
    try {
        sig = std::make_shared<bbt::coroutine::CompletionSignal>();
    } catch (const std::logic_error&) {
        return result<std::shared_ptr<TransportRuntime>>::err(MakeError(
            ErrorCode::RuntimeUnavailable,
            "coroutine runtime generation unavailable"));
    }
    auto impl = std::make_shared<transport_detail::TransportRuntimeImpl>(
        limits, std::move(info).value(), std::move(sig));
    return result<std::shared_ptr<TransportRuntime>>::ok(std::move(impl));
}

} // namespace bbt::infra
