#include "http/NetworkRuntimeImpl.hpp"

#include <algorithm>
#include <limits>

#include <bbt/coroutine/object/CoObject.hpp>

#include <bbt/infra/CoTCP.hpp>
#include <bbt/infra/CoUDP.hpp>

#include "http/HttpClientImpl.hpp"
#include "http/HttpServerImpl.hpp"

namespace bbt::infra {

namespace http_detail {

namespace {

// CreateObjectInfo / CompletionSignal 构造需要有效运行时代际；
// 实现已收敛到 detail（Issue #6 起与 Redis 模块共用）。
result<bbt::coroutine::CoObjectInfo> NewObjectInfo(std::string kind) {
    return bbt::infra::detail::NewObjectInfo(std::move(kind));
}

} // namespace

result<void> NetworkRuntimeImpl::Start() {
    const auto gen = bbt::coroutine::CurrentRuntimeGeneration();
    if (gen == 0 || gen != m_info.generation)
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "scheduler not running or runtime generation mismatch"));

    // 先取得共享 executor 并建立 io 域（不创建线程/context）；失败
    // 留在 kCreated，调用方可待 scheduler 就绪后重试 Start。
    auto engine_ready = m_engine->Start();
    if (!engine_ready)
        return result<void>::err(std::move(engine_ready).error());

    int expected = kCreated;
    if (!m_state.compare_exchange_strong(expected, kRunning))
        return result<void>::err(MakeError(
            m_state.load() == kRunning ? ErrorCode::InvalidArgument
                                       : ErrorCode::Closed,
            m_state.load() == kRunning ? "runtime already started"
                                       : "runtime is closing or closed"));
    return result<void>::ok();
}

result<std::shared_ptr<bbt::coroutine::CompletionSignal>>
NetworkRuntimeImpl::NewCloseSignal() const {
    try {
        return result<std::shared_ptr<bbt::coroutine::CompletionSignal>>::ok(
            std::make_shared<bbt::coroutine::CompletionSignal>());
    } catch (const std::logic_error&) {
        return result<std::shared_ptr<bbt::coroutine::CompletionSignal>>::err(
            MakeError(ErrorCode::RuntimeUnavailable,
                "coroutine runtime generation unavailable"));
    }
}

result<void> NetworkRuntimeImpl::CheckUsableForFactory() const {
    if (m_state.load() != kRunning)
        return result<void>::err(MakeError(
            m_state.load() == kCreated ? ErrorCode::RuntimeUnavailable
                                       : ErrorCode::Closed,
            m_state.load() == kCreated ? "runtime not started"
                                       : "runtime is closing or closed"));
    if (!m_close.IsOpen())
        return result<void>::err(
            MakeError(ErrorCode::Closed, "runtime is closing or closed"));
    return result<void>::ok();
}

namespace {

// co-io-adapter/v1 §4.0.1.7：容量原子预留，失败回退、物理关闭后释放。
// 超限返回 Overloaded，不静默排队。
void ReleaseTransportSlot(std::mutex& mtx, std::size_t& count) noexcept {
    std::lock_guard<std::mutex> lk(mtx);
    if (count > 0) --count;
}

} // namespace

result<std::shared_ptr<CoTCP>> NetworkRuntimeImpl::DialTCP(
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

    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        if (m_transport_sealed)
            return result<std::shared_ptr<CoTCP>>::err(MakeError(
                ErrorCode::Closed, "runtime is closing"));
        if (m_transport_count >= m_limits.max_connections)
            return result<std::shared_ptr<CoTCP>>::err(MakeError(
                ErrorCode::Overloaded, "DialTCP: max_connections exceeded"));
        ++m_transport_count;
    }

    auto dialed = tcp::CoTCP::DialTCP(std::move(endpoint.host), endpoint.port, options);
    if (!dialed) {
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        MaybeFinalize();
        return result<std::shared_ptr<CoTCP>>::err(std::move(dialed).error());
    }
    auto connection = std::move(dialed).value();
    bool sealed = false;
    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        sealed = m_transport_sealed;
        if (!sealed) {
            std::weak_ptr<NetworkRuntimeImpl> weak =
                std::static_pointer_cast<NetworkRuntimeImpl>(shared_from_this());
            std::weak_ptr<CoTCP> child = connection;
            connection->SetClosedHook([weak, child] {
                auto rt = weak.lock();
                auto object = child.lock();
                if (rt && object)
                    rt->OnTransportClosed(object.get());
            });
            m_tcp_children.push_back(connection);
        }
    }
    if (sealed) {
        connection->RequestClose();
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        MaybeFinalize();
        return result<std::shared_ptr<CoTCP>>::err(MakeError(
            ErrorCode::Closed, "runtime closed during TCP dial"));
    }
    return result<std::shared_ptr<CoTCP>>::ok(std::move(connection));
}

result<std::shared_ptr<CoTCPListener>> NetworkRuntimeImpl::ListenTCP(
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

    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        if (m_transport_sealed)
            return result<std::shared_ptr<CoTCPListener>>::err(MakeError(
                ErrorCode::Closed, "runtime is closing"));
        if (m_transport_count >= m_limits.max_connections)
            return result<std::shared_ptr<CoTCPListener>>::err(MakeError(
                ErrorCode::Overloaded, "ListenTCP: max_connections exceeded"));
        ++m_transport_count;
    }

    auto bound = tcp::CoTCPListener::ListenTCP(
        std::move(local.ip), local.port, static_cast<int>(backlog));
    if (!bound) {
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        MaybeFinalize();
        return result<std::shared_ptr<CoTCPListener>>::err(std::move(bound).error());
    }
    auto listener = std::move(bound).value();
    std::unique_lock<std::mutex> transport_lock(m_transport_mtx);
    if (m_transport_sealed) {
        transport_lock.unlock();
        listener->RequestClose();
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        MaybeFinalize();
        return result<std::shared_ptr<CoTCPListener>>::err(MakeError(
            ErrorCode::Closed, "runtime closed during TCP listen"));
    }
    std::weak_ptr<NetworkRuntimeImpl> weak =
        std::static_pointer_cast<NetworkRuntimeImpl>(shared_from_this());
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
                rt->MaybeFinalize();
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
            rt->m_tcp_children.push_back(std::move(accepted));
            return true;
        });
    m_tcp_children.push_back(listener);
    transport_lock.unlock();
    return result<std::shared_ptr<CoTCPListener>>::ok(std::move(listener));
}

result<std::shared_ptr<CoUDP>> NetworkRuntimeImpl::BindUDP(SocketAddress local) {
    auto usable = CheckUsableForFactory();
    if (!usable)
        return result<std::shared_ptr<CoUDP>>::err(std::move(usable).error());
    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        if (m_transport_sealed)
            return result<std::shared_ptr<CoUDP>>::err(MakeError(
                ErrorCode::Closed, "runtime is closing"));
        if (m_transport_count >= m_limits.max_connections)
            return result<std::shared_ptr<CoUDP>>::err(MakeError(
                ErrorCode::Overloaded, "BindUDP: max_connections exceeded"));
        ++m_transport_count;
    }
    auto bound = udp::CoUDP::BindUDP(std::move(local));
    if (!bound) {
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        MaybeFinalize();
        return result<std::shared_ptr<CoUDP>>::err(std::move(bound).error());
    }
    auto socket = std::move(bound).value();
    bool sealed = false;
    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        sealed = m_transport_sealed;
        if (!sealed) {
            std::weak_ptr<NetworkRuntimeImpl> weak =
                std::static_pointer_cast<NetworkRuntimeImpl>(shared_from_this());
            std::weak_ptr<CoUDP> child = socket;
            socket->SetClosedHook([weak, child] {
                auto rt = weak.lock();
                auto object = child.lock();
                if (rt && object)
                    rt->OnTransportClosed(object.get());
            });
            m_tcp_children.push_back(socket);
        }
    }
    if (sealed) {
        socket->RequestClose();
        ReleaseTransportSlot(m_transport_mtx, m_transport_count);
        MaybeFinalize();
        return result<std::shared_ptr<CoUDP>>::err(MakeError(
            ErrorCode::Closed, "runtime closed during UDP bind"));
    }
    return result<std::shared_ptr<CoUDP>>::ok(std::move(socket));
}

result<void> NetworkRuntimeImpl::CheckAndAdoptLocked(
    const std::shared_ptr<IIoTeardown>& child) {
    // 调用方已持 m_lifecycle_mtx。sealed 之后不再接纳——登记了的子对象
    // 必在 teardown 快照里被收口，线性化点即此处。
    if (m_sealed || m_state.load() != kRunning || !m_close.IsOpen())
        return result<void>::err(
            MakeError(ErrorCode::Closed, "runtime is closing or closed"));
    // 测试接缝：复检已过、登记未提交的临界点（仍持 m_lifecycle_mtx）。
    // 用于证明「登记提交与 RequestClose 调用窗口重叠」的收口语义；
    // 生产路径不安装，空钩子零额外语义。钩子契约见头文件注释。
    if (m_adopt_commit_gate_for_test)
        m_adopt_commit_gate_for_test();
    m_children.push_back(child);
    ++m_unclosed;
    return result<void>::ok();
}

result<std::shared_ptr<HttpClient>> NetworkRuntimeImpl::CreateHttpClient() {
    auto usable = CheckUsableForFactory();
    if (!usable)
        return result<std::shared_ptr<HttpClient>>::err(
            std::move(usable).error());
    auto info = NewObjectInfo("infra.http_client");
    if (!info)
        return result<std::shared_ptr<HttpClient>>::err(
            std::move(info).error());
    auto sig = NewCloseSignal();
    if (!sig)
        return result<std::shared_ptr<HttpClient>>::err(
            std::move(sig).error());

    auto impl = std::make_shared<HttpClientImpl>(
        m_engine, std::move(info).value(), std::move(sig).value());
    std::weak_ptr<NetworkRuntimeImpl> weak =
        std::static_pointer_cast<NetworkRuntimeImpl>(shared_from_this());
    // 关闭 hook 携带子对象弱引用：物理关闭后按稳定身份反登记；
    // hook 内 lock 出的强引用兼作移除期间的保活，避免本回调成为
    // 最后一个强引用时在持锁路径上析构自身（Issue #38）。
    std::weak_ptr<IIoTeardown> child =
        std::static_pointer_cast<IIoTeardown>(impl);
    impl->SetClosedHook([weak, child] {
        auto rt = weak.lock();
        auto object = child.lock();
        if (rt && object)
            rt->OnChildClosed(object.get());
    });
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        auto adopted = CheckAndAdoptLocked(
            std::static_pointer_cast<IIoTeardown>(impl));
        if (!adopted)
            return result<std::shared_ptr<HttpClient>>::err(
                std::move(adopted).error());
    }
    return result<std::shared_ptr<HttpClient>>::ok(std::move(impl));
}

result<std::shared_ptr<HttpServer>> NetworkRuntimeImpl::ListenHttp(
    ListenAddress address, HttpHandler handler) {
    if (!handler)
        return result<std::shared_ptr<HttpServer>>::err(MakeError(
            ErrorCode::InvalidArgument, "ListenHttp: handler must not be null"));
    auto usable = CheckUsableForFactory();
    if (!usable)
        return result<std::shared_ptr<HttpServer>>::err(
            std::move(usable).error());
    auto info = NewObjectInfo("infra.http_server");
    if (!info)
        return result<std::shared_ptr<HttpServer>>::err(
            std::move(info).error());
    auto sig = NewCloseSignal();
    if (!sig)
        return result<std::shared_ptr<HttpServer>>::err(
            std::move(sig).error());

    auto impl = std::make_shared<HttpServerImpl>(
        m_engine, std::move(handler), std::move(info).value(),
        std::move(sig).value());
    auto bound = impl->Bind(std::move(address));
    if (!bound)
        return result<std::shared_ptr<HttpServer>>::err(
            std::move(bound).error());
    std::weak_ptr<NetworkRuntimeImpl> weak =
        std::static_pointer_cast<NetworkRuntimeImpl>(shared_from_this());
    // 与 CreateHttpClient 同一约定：hook 携带弱引用身份并兼任移除期保活。
    std::weak_ptr<IIoTeardown> child =
        std::static_pointer_cast<IIoTeardown>(impl);
    impl->SetClosedHook([weak, child] {
        auto rt = weak.lock();
        auto object = child.lock();
        if (rt && object)
            rt->OnChildClosed(object.get());
    });
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        auto adopted = CheckAndAdoptLocked(
            std::static_pointer_cast<IIoTeardown>(impl));
        if (!adopted)
            return result<std::shared_ptr<HttpServer>>::err(
                std::move(adopted).error());
    }
    impl->BeginAccept();
    return result<std::shared_ptr<HttpServer>>::ok(std::move(impl));
}

void NetworkRuntimeImpl::RequestClose() noexcept {
    if (!m_close.BeginClose())
        return;
    m_state.store(kClosingOrClosed);
    std::vector<std::shared_ptr<ICoCloseable>> transports;
    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        m_transport_sealed = true;
        transports = m_tcp_children;
    }
    for (auto& transport : transports)
        transport->RequestClose();
    if (!m_engine->Started()) {
        // 未 Start：无 io 资源，直接落定。
        m_close.MarkClosed();
        return;
    }
    auto self = shared_from_this();
    if (!m_engine->TryPost([self] { self->TeardownOnIoDomain(); }))
        TeardownOffDomain();
}

void NetworkRuntimeImpl::TeardownOnIoDomain() noexcept {
    std::vector<std::shared_ptr<IIoTeardown>> children;
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        m_sealed = true;
        children = m_children;
        m_teardown = true;
    }
    // 锁外逐个收口：子对象 teardown 完成时其 MarkClosed 会经
    // ClosedHook 回到 OnChildClosed 再拿本锁——持锁调用会死锁。
    for (auto& child : children)
        child->TeardownOnIoDomain();
    MaybeFinalize();
}

void NetworkRuntimeImpl::TeardownOffDomain() noexcept {
    std::vector<std::shared_ptr<IIoTeardown>> children;
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        m_sealed = true;
        children = m_children;
        m_teardown = true;
    }
    for (auto& child : children)
        child->TeardownOffDomain();
    MaybeFinalize();
}

void NetworkRuntimeImpl::OnChildClosed(const IIoTeardown* child) noexcept {
    // 物理关闭落定（ClosedHook 经 MarkClosed 触发）：按稳定身份把子对象
    // 移出托管集合并与计数一次性配对。调用方（ClosedHook lambda）在本
    // 回调期间持有 child 的强引用——若 m_children 已是最后一个强持有者，
    // 移除后的析构发生在锁外的 hook 返回路径上，不在持锁段内析构。
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        if (m_unclosed > 0)
            --m_unclosed;
        m_children.erase(
            std::remove_if(m_children.begin(), m_children.end(),
                           [child](const auto& item) {
                               return item.get() == child;
                           }),
            m_children.end());
    }
    MaybeFinalize();
}

void NetworkRuntimeImpl::OnTransportClosed(const ICoCloseable* child) noexcept {
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
    MaybeFinalize();
}

void NetworkRuntimeImpl::MaybeFinalize() noexcept {
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        if (!m_teardown || m_unclosed != 0 || m_finalize_started)
            return;
    }
    {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        if (m_transport_count != 0)
            return;
    }
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        if (!m_teardown || m_unclosed != 0 || m_finalize_started)
            return;
        m_finalize_started = true;
    }
    auto self = shared_from_this();
    if (!m_engine->TryPost([self] { self->FinalizeEngine(); }))
        FinalizeEngine();
}

void NetworkRuntimeImpl::FinalizeEngine() noexcept {
    m_engine->SealOnIoDomain();
    m_close.MarkClosed();
}

} // namespace http_detail

// 契约冻结的装配入口（见 0002 契约 N0 段）：Create 只校验装配参数，
// Start 才真正占用资源；二者均在控制线程使用，不挂起协程。
result<std::shared_ptr<NetworkRuntime>>
NetworkRuntime::Create(NetworkLimits limits) {
    auto valid = ValidateNetworkLimits(limits);
    if (!valid)
        return result<std::shared_ptr<NetworkRuntime>>::err(
            std::move(valid).error());
    auto info = http_detail::NewObjectInfo("infra.network_runtime");
    if (!info)
        return result<std::shared_ptr<NetworkRuntime>>::err(
            std::move(info).error());
    std::shared_ptr<bbt::coroutine::CompletionSignal> sig;
    try {
        sig = std::make_shared<bbt::coroutine::CompletionSignal>();
    } catch (const std::logic_error&) {
        return result<std::shared_ptr<NetworkRuntime>>::err(MakeError(
            ErrorCode::RuntimeUnavailable,
            "coroutine runtime generation unavailable"));
    }
    auto engine = std::make_shared<http_detail::HttpIoEngine>(limits);
    auto impl = std::make_shared<http_detail::NetworkRuntimeImpl>(
        limits, engine, std::move(info).value(), std::move(sig));
    return result<std::shared_ptr<NetworkRuntime>>::ok(std::move(impl));
}

} // namespace bbt::infra
