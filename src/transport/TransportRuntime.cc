#include "transport/TransportRuntimeImpl.hpp"

#include <algorithm>
#include <limits>
#include <utility>

#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>   // g_scheduler->IsInitialized（运行时是否在跑）

#include "detail/IoSupport.hpp"

namespace bbt::infra {

namespace transport_detail {

namespace {

// co-io-adapter/v1 §4.0.1.7：容量名额归还，只在真实物理关闭落定后调用一次。
void ReleaseTransportSlot(std::mutex& mtx, std::size_t& count) noexcept {
    std::lock_guard<std::mutex> lk(mtx);
    if (count > 0) --count;
}

// coroutine 运行时是否在跑：进程寿命运行时只有「已初始化」一种活态，不再有
// 运行时代际。
bool RuntimeRunning() noexcept {
    return g_scheduler != nullptr && g_scheduler->IsInitialized();
}

} // namespace

result<void> TransportRuntimeImpl::Start() {
    // 对象身份与工厂门禁的前置条件是「coroutine 运行时已初始化」（不再有
    // 运行时代际）。
    if (!RuntimeRunning())
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "coroutine runtime not initialized"));

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
    // §4.0.1.7（Issue #32）：工厂同样受运行时可用性门禁——运行时未初始化时
    // 不再产出新对象（对象钉住运行时的等待基础设施）。
    if (!RuntimeRunning())
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "coroutine runtime not initialized"));
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
    // R1：与容量名额同临界区登记「在途工厂」。此后到 EndFactory 之间 fd 可能
    // 已创建但尚未交接 child，Close 必须等该计数归零才收口，不能快照遗漏。
    ++m_pending_factories;
    return result<void>::ok();
}

// R1：工厂每一条返回路径注销在途工厂（与 ReserveConnectionSlot 配对），随后
// 重判终态。仅注销，不释放容量名额——名额由 ReleaseTransportSlot 或 child
// 物理关闭落定（ClosedHook）负责。
void TransportRuntimeImpl::EndFactory() noexcept {
    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        if (m_pending_factories > 0)
            --m_pending_factories;
        m_pending_cv.notify_all();
    }
    MaybeMarkClosed();
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
        EndFactory();
        return result<std::shared_ptr<CoTCP>>::err(MakeError(
            ErrorCode::Overloaded, "DialTCP: max_inflight exceeded"));
    }

    // 等待段接缝：owner 级关闭唤醒登记——owner->Close() 唤醒挂起的 connect
    // 等待段，使 dial 尽快走正常返回路径归还容量/名额；等待段挂起落定回调
    // 仅供测试确定性观察。DNS 等待段的封口响应由调用方 deadline 界定（上游
    // AwaitBounded 内部 waiter 外部不可 Notify）。
    detail::DialWaitOptions dial_wait;
    dial_wait.close_waiters = m_close_waiters;
    if (m_dial_wait_entry_gate_for_test) {
        auto gate = m_dial_wait_entry_gate_for_test;
        dial_wait.dns_on_registered = gate;
        dial_wait.connect_on_registered = std::move(gate);
    }

    auto dialed = detail::TransportWiring::DialTCP(
        std::move(endpoint.host), endpoint.port, options, dial_wait);
    // 等待段已结束（成功/失败/超时/被 owner 关闭唤醒）：归还在途名额的唯一
    // 归还点——此后所有返回路径都已归还，不需要跨栈的兜底凭据（运行时不再硬
    // 销毁挂起协程）。
    m_inflight_quota->Release();

    if (!dialed) {
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        EndFactory();
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
            detail::TransportWiring::SetClosedHook(*connection, [weak, child] {
                auto rt = weak.lock();
                auto object = child.lock();
                if (rt && object)
                    rt->OnTransportClosed(object.get());
            });
            detail::TransportWiring::SetInflightHooks(*connection,
                [quota = m_inflight_quota] { return quota->TryAdmit(); },
                [quota = m_inflight_quota] { quota->Release(); },
                m_inflight_quota);
            m_tcp_children.push_back(connection);
        }
    }
    if (sealed) {
        // 交付即已封口：不再托管，物理关闭并归还容量。
        connection->Close();
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        EndFactory();
        return result<std::shared_ptr<CoTCP>>::err(MakeError(
            ErrorCode::Closed, "transport runtime closed during TCP dial"));
    }
    EndFactory();
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
        EndFactory();
        return result<std::shared_ptr<CoTCPListener>>::err(std::move(bound).error());
    }
    auto listener = std::move(bound).value();
    std::unique_lock<std::mutex> transport_lock(m_transport_mtx);
    if (m_transport_sealed) {
        transport_lock.unlock();
        // 交付即已封口：不再托管，物理关闭（含刚创建的 fd）并归还容量。
        listener->Close();
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        EndFactory();
        return result<std::shared_ptr<CoTCPListener>>::err(MakeError(
            ErrorCode::Closed, "transport runtime closed during TCP listen"));
    }
    std::weak_ptr<TransportRuntimeImpl> weak =
        std::static_pointer_cast<TransportRuntimeImpl>(shared_from_this());
    std::weak_ptr<CoTCPListener> child = listener;
    detail::TransportWiring::SetClosedHook(*listener, [weak, child] {
        auto rt = weak.lock();
        auto object = child.lock();
        if (rt && object)
            rt->OnTransportClosed(object.get());
    });
    detail::TransportWiring::SetAcceptHooks(*listener,
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
            detail::TransportWiring::SetClosedHook(*accepted, [weak, child] {
                auto rt_locked = weak.lock();
                auto child_locked = child.lock();
                if (rt_locked && child_locked)
                    rt_locked->OnTransportClosed(child_locked.get());
            });
            // §4.0.1.7：接纳的 socket 同样挂本 owner 在途账本——其
            // ReadSome/WriteSome/WriteAll 占用同一 max_inflight 预算。
            detail::TransportWiring::SetInflightHooks(*accepted,
                [quota = rt->m_inflight_quota] { return quota->TryAdmit(); },
                [quota = rt->m_inflight_quota] { quota->Release(); },
                rt->m_inflight_quota);
            rt->m_tcp_children.push_back(std::move(accepted));
            return true;
        });
    // §4.0.1.7：listener 的 Accept 占用同一 max_inflight 账本。
    detail::TransportWiring::SetInflightHooks(*listener,
        [quota = m_inflight_quota] { return quota->TryAdmit(); },
        [quota = m_inflight_quota] { quota->Release(); },
        m_inflight_quota);
    m_tcp_children.push_back(listener);
    transport_lock.unlock();
    EndFactory();
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
        EndFactory();
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
            detail::TransportWiring::SetClosedHook(*socket, [weak, child] {
                auto rt = weak.lock();
                auto object = child.lock();
                if (rt && object)
                    rt->OnTransportClosed(object.get());
            });
            // §4.0.1.7：UDP 的 Receive/Send 挂起段占同一 max_inflight 账本。
            detail::TransportWiring::SetInflightHooks(*socket,
                [quota = m_inflight_quota] { return quota->TryAdmit(); },
                [quota = m_inflight_quota] { quota->Release(); },
                m_inflight_quota);
            m_tcp_children.push_back(socket);
        }
    }
    if (sealed) {
        socket->Close();
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        EndFactory();
        return result<std::shared_ptr<CoUDP>>::err(MakeError(
            ErrorCode::Closed, "transport runtime closed during UDP bind"));
    }
    EndFactory();
    return result<std::shared_ptr<CoUDP>>::ok(std::move(socket));
}

void TransportRuntimeImpl::Close() noexcept {
    if (!m_close.BeginClose()) {
        // 并发/重复 Close：无独立短上限地等首次调用者的真实物理落定事实
        // （子对象 Close 均同步物理收口，落定事实由 MaybeMarkClosed 经
        // m_close_cv 通知），使每个合法调用者返回当刻都观察到同一 Closed 终态，
        // 不再各自以 kCloseDrainTimeout 冒充收口。
        std::unique_lock<std::mutex> lk(m_transport_mtx);
        m_close_cv.wait(lk, [this] { return m_close.IsClosed(); });
        return;
    }
    m_state.store(kClosingOrClosed);
    std::vector<std::shared_ptr<ICoCloseable>> transports;
    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        m_transport_sealed = true;
        transports = m_tcp_children;
    }
    // 先唤醒本 owner 级挂起等待者（受管 DialTCP 的 connect 等待段），再逐对象
    // 同步 Close：对象各自封口 → 唤醒自身挂起 op → 有界排空在途计数 → 物理
    // 释放。一个 socket 只有一个物理关闭责任方（对象自己），本 owner 只调用。
    m_close_waiters->CloseAndWakeAll();
    // R1：等待在途工厂排空后才收口——工厂要么已把 child 交接进 m_tcp_children
    // （此时也已被上面快照之外的「sealed 路径」自行物理关闭），要么在途 fd 尚未
    // 交接；两者都在 EndFactory 前完成物理关闭/交接，故 Close 返回当刻不可能有
    // 遗漏的 pending fd。谓词与通知同用 m_transport_mtx。
    {
        std::unique_lock<std::mutex> lk(m_transport_mtx);
        m_pending_cv.wait(lk, [this] { return m_pending_factories == 0; });
    }
    for (auto& transport : transports)
        transport->Close();
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
    bool publish = false;
    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        // 物理清理落定 = 已封口（不再接纳新资源）、受管 transport 名额归零、
        // 且无在途工厂。未封口时对象正常关闭不触发落定。
        if (m_transport_sealed && m_transport_count == 0 &&
            m_pending_factories == 0 && !m_closed_published) {
            m_closed_published = true;
            publish = true;
            // 谓词发布（m_close → Closed）与 notify 在同一临界区：并发 Close
            // 的后来者在同一把 m_transport_mtx 下检查 m_close.IsClosed() 并入队
            // condition_variable::wait，谓词更新与通知同锁配对 ⇒ 不丢唤醒（修复
            // 「谓词检查与入队之间通知丢失 → 无限 wait 永挂」）。
            m_close.MarkClosed();
            m_close_cv.notify_all();
        }
    }
    // closed hook 在锁外跑一次（装配约定：hook 不取本 owner 的锁）。
    if (publish)
        NotifyClosed();
}

} // namespace transport_detail

// 内部装配面（src/detail/TransportWiring.hpp）的 owner 侧实现：TransportRuntime
// 的抽象面不再声明这些测试接缝（公开面没有对应槽位/虚表条目），探针落在唯
// 一实现 transport_detail::TransportRuntimeImpl 上。
namespace detail {

std::size_t TransportWiring::InflightQuotaHeldForTest(
    TransportRuntime& owner) noexcept {
    return static_cast<transport_detail::TransportRuntimeImpl&>(owner)
        .InflightQuotaHeldForTest();
}

std::size_t TransportWiring::PendingFactoriesForTest(
    TransportRuntime& owner) noexcept {
    return static_cast<transport_detail::TransportRuntimeImpl&>(owner)
        .PendingFactoriesForTest();
}

bool TransportWiring::CloseSealedForTest(TransportRuntime& owner) noexcept {
    return static_cast<transport_detail::TransportRuntimeImpl&>(owner)
        .CloseSealedForTest();
}

std::size_t TransportWiring::TransportsHeldForTest(
    TransportRuntime& owner) noexcept {
    return static_cast<transport_detail::TransportRuntimeImpl&>(owner)
        .TransportsHeldForTest();
}

void TransportWiring::SetDialWaitEntryGateForTest(
    TransportRuntime& owner, std::function<void()> gate) noexcept {
    static_cast<transport_detail::TransportRuntimeImpl&>(owner)
        .SetDialWaitEntryGateForTest(std::move(gate));
}

} // namespace detail

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
    auto impl = std::make_shared<transport_detail::TransportRuntimeImpl>(
        limits, std::move(info).value());
    return result<std::shared_ptr<TransportRuntime>>::ok(std::move(impl));
}

} // namespace bbt::infra
