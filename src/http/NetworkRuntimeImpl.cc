#include "http/NetworkRuntimeImpl.hpp"

#include <bbt/coroutine/object/CoObject.hpp>

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

result<void> NetworkRuntimeImpl::CheckAndAdoptLocked(
    const std::shared_ptr<IIoTeardown>& child) {
    // 调用方已持 m_lifecycle_mtx。sealed 之后不再接纳——登记了的子对象
    // 必在 teardown 快照里被收口，线性化点即此处。
    if (m_sealed || m_state.load() != kRunning || !m_close.IsOpen())
        return result<void>::err(
            MakeError(ErrorCode::Closed, "runtime is closing or closed"));
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
    impl->SetClosedHook([weak] {
        if (auto rt = weak.lock())
            rt->OnChildClosed();
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
    impl->SetClosedHook([weak] {
        if (auto rt = weak.lock())
            rt->OnChildClosed();
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
    }
    // 锁外逐个收口：子对象 teardown 完成时其 MarkClosed 会经
    // ClosedHook 回到 OnChildClosed 再拿本锁——持锁调用会死锁。
    for (auto& child : children)
        child->TeardownOnIoDomain();
    bool fin = false;
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        m_teardown = true;
        if (m_unclosed == 0 && !m_finalize_started) {
            m_finalize_started = true;
            fin = true;
        }
    }
    if (fin)
        FinalizeEngine();
}

void NetworkRuntimeImpl::TeardownOffDomain() noexcept {
    std::vector<std::shared_ptr<IIoTeardown>> children;
    bool fin = false;
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        m_sealed = true;
        children = m_children;
        m_teardown = true;
        if (m_unclosed == 0 && !m_finalize_started) {
            m_finalize_started = true;
            fin = true;
        }
    }
    for (auto& child : children)
        child->TeardownOffDomain();
    if (fin)
        FinalizeEngine();
}

void NetworkRuntimeImpl::OnChildClosed() noexcept {
    bool fin = false;
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        if (m_unclosed > 0)
            --m_unclosed;
        if (m_teardown && m_unclosed == 0 && !m_finalize_started) {
            m_finalize_started = true;
            fin = true;
        }
    }
    if (fin) {
        auto self = shared_from_this();
        // 引擎封口必须在 io 域；投递失败说明引擎已封（finalize 已发生）。
        if (!m_engine->TryPost([self] { self->FinalizeEngine(); }))
            FinalizeEngine();
    }
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
