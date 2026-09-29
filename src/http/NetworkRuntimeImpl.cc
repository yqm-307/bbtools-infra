#include "http/NetworkRuntimeImpl.hpp"

#include <algorithm>

#include <bbt/coroutine/object/CoObject.hpp>

#include <bbt/infra/CoTCP.hpp>
#include <bbt/infra/CoUDP.hpp>

#include "detail/IoSupport.hpp"
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
    // P2：transport owner 在同一次 Start 中启动（同一代际门禁）。失败回滚
    // 状态，保留「待 scheduler 就绪后重试 Start」的既有语义。
    auto transport_ready = m_transport->Start();
    if (!transport_ready) {
        m_state.store(kCreated);
        return result<void>::err(std::move(transport_ready).error());
    }
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
    // §4.0.1.7（Issue #32）：工厂同样受运行时代际门禁——Stop→Start 后
    // 旧代际 runtime 不得再产出新对象（对象钉住当前代际，跨代交付只会
    // 产出入场即失败的对象）。
    const auto gen = bbt::coroutine::CurrentRuntimeGeneration();
    if (gen == 0 || gen != m_info.generation)
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "runtime belongs to another runtime generation"));
    return result<void>::ok();
}

// P2 装配接缝实现：见头文件注释。transport 侧是唯一通知来源。
void NetworkRuntimeImpl::AdoptTransportOwner() {
    std::weak_ptr<NetworkRuntimeImpl> weak =
        std::static_pointer_cast<NetworkRuntimeImpl>(shared_from_this());
    detail::TransportWiring::SetClosedHook(*m_transport, [weak] {
        if (auto rt = weak.lock())
            rt->MaybeFinalize();
    });
}

// ---------------------------------------------------------------------------
// P2：受管 TCP/UDP 工厂整体转发给 transport owner（bbt_infra_transport）。
// 容量名额（max_connections）、在途账本（max_inflight）、受管对象强持有与
// 物理关闭收口、受管 DialTCP 等待段的取消/归还都由该 owner 独占实现；
// 本切片不保留第二份账本，也不重复 transport 的关闭实现。参数校验、
// Overloaded/Closed 映射与「拒绝路径不建活跃连接」的语义随之移动到
// src/transport/TransportRuntime.cc，行为不变（由既有
// Test_tcp_runtime_factory 全套 + 新增 Test_transport_runtime 验收）。
// ---------------------------------------------------------------------------

result<std::shared_ptr<CoTCP>> NetworkRuntimeImpl::DialTCP(
    TcpEndpoint endpoint, const CallOptions& options) {
    return m_transport->DialTCP(std::move(endpoint), options);
}

result<std::shared_ptr<CoTCPListener>> NetworkRuntimeImpl::ListenTCP(
    SocketAddress local, unsigned backlog) {
    return m_transport->ListenTCP(std::move(local), backlog);
}

result<std::shared_ptr<CoUDP>> NetworkRuntimeImpl::BindUDP(SocketAddress local) {
    return m_transport->BindUDP(std::move(local));
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

// Issue #37：HTTP 出站原子接纳。检查与递增在同一 m_lifecycle_mtx
// 临界区内，与 CheckAndAdoptLocked 共用一把锁——接纳判定与「runtime
// 正在关闭」互斥，保证超额判定不被并发请求或 teardown 穿透。
// 不引入排队：名额不足立即 Overloaded，此时还没有创建 socket/resolver，
// 也没有向 io 域投递任何 async_*，满足「拒绝路径不建活跃连接」。
result<void> NetworkRuntimeImpl::TryAdmitHttpRequest() noexcept {
    std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
    if (m_sealed || m_state.load() != kRunning || !m_close.IsOpen())
        return result<void>::err(MakeError(
            ErrorCode::Closed, "runtime is closing or closed"));
    if (m_http_inflight_count >= m_limits.max_inflight)
        return result<void>::err(MakeError(
            ErrorCode::Overloaded, "http outbound: max_inflight exceeded"));
    if (m_http_conn_count >= m_limits.max_connections)
        return result<void>::err(MakeError(
            ErrorCode::Overloaded, "http outbound: max_connections exceeded"));
    ++m_http_inflight_count;
    ++m_http_conn_count;
    return result<void>::ok();
}

// 归还只在 op 物理收口（finished && inflight==0 → UnregisterOp）后经
// 本钩子发生一次。TryAdmit/Release 严格配对：接纳成功才注册归还钩，
// 所以这里只需防下限，不会出现负数/重复归还。HTTP 出站名额不参与
// runtime finalize 门控（finalize 等的是子对象 Closed），无需在此
// 触发 MaybeFinalize。
void NetworkRuntimeImpl::ReleaseHttpRequest() noexcept {
    std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
    if (m_http_inflight_count > 0) --m_http_inflight_count;
    if (m_http_conn_count > 0)    --m_http_conn_count;
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
    // Issue #37：HTTP 出站配额挂到 Runtime owner——同一 runtime 下所有
    // HttpClient 共享同一预算，避免多 client 各自绕过总量。admit 在
    // 登记 op 之前原子预留；release 在 op 物理收口时被调一次。
    impl->SetQuotaHooks(
        [weak] {
            auto rt = weak.lock();
            if (!rt)
                return result<void>::err(MakeError(
                    ErrorCode::Closed, "runtime gone"));
            return rt->TryAdmitHttpRequest();
        },
        [weak] {
            if (auto rt = weak.lock())
                rt->ReleaseHttpRequest();
        });
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
    // P2：transport owner 自行封口工厂（拒绝新资源）、归还未完成受管 dial 的
    // 等待段名额并唤醒其挂起协程、再对每个受管 transport 发起唯一一次物理
    // 关闭。本切片不再持有 transport 计数、账本或 dial 登记簿。
    m_transport->RequestClose();
    if (!m_engine->Started()) {
        // 未 Start：无 io 资源，直接落定。
        m_close.MarkClosed();
        return;
    }
    auto self = shared_from_this();
    if (!m_engine->TryPost([self] { self->TeardownOnIoDomain(); }))
        TeardownOffDomain();
}

std::size_t NetworkRuntimeImpl::ForceCloseAfterQuiescence() noexcept {
    // 前置条件：Scheduler::Stop() 已返回。本入口不复用 RequestClose 的
    // BeginClose 早退——「已 RequestClose 但 teardown 落在已死 io 域上」的硬停
    // 是必须能收口的形态，故各步骤幂等且不依赖调用顺序。
    // 闸门先置位：随后任何 MaybeFinalize（含 transport 落定 hook 与本入口尾部
    // 的 teardown 路径）都就地落定，不投递已死的 io 域——投递成功也会丢失落定。
    m_hard_stop.store(true);
    m_close.BeginClose();          // 幂等：已封口时无副作用
    m_state.store(kClosingOrClosed);
    // transport owner 自证落定：批量强制物理关闭使其 IsClosed 观察点在硬停后
    // 可达（本切片不再持有第二份 transport 记账）。
    const std::size_t forced =
        detail::TransportWiring::ForceCloseAfterQuiescence(*m_transport);
    if (!m_engine->Started()) {
        // 未 Start：无 io 资源，直接落定（与 RequestClose 同一分支）。
        m_close.MarkClosed();
        return forced;
    }
    // 硬停后 io 域已无执行体：走 off-domain 收口（与 RequestClose 的 TryPost
    // 失败降级路径同一实现），不尝试投递。
    TeardownOffDomain();
    return forced;
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

void NetworkRuntimeImpl::MaybeFinalize() noexcept {
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        if (!m_teardown || m_unclosed != 0 || m_finalize_started)
            return;
    }
    // P2：transport 侧落定由 transport owner 自证（已封口且受管 transport 全部
    // 物理关闭）。这里只读它的观察点，不再有第二份 transport 计数；该观察点的
    // 事件来源是装配时安装的 SetClosedHook（见 AdoptTransportOwner）。
    if (!m_transport->IsClosed())
        return;
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        if (!m_teardown || m_unclosed != 0 || m_finalize_started)
            return;
        m_finalize_started = true;
    }
    auto self = shared_from_this();
    // 硬停闸门（I5）：Scheduler::Stop() 已返回后 io 域不再执行任何任务，投递成功
    // 也会丢失落定 ⇒ 就地落定。运行期路径一字未改（闸门只由硬停入口置位）。
    if (m_hard_stop.load() || !m_engine->TryPost([self] { self->FinalizeEngine(); }))
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
    // P2：transport owner 独立创建——它的实现目标（bbt_infra_transport）不
    // 链接 HTTP，也不引用 HTTP 的任何类型/符号；本入口只做组合。
    auto transport = TransportRuntime::Create(limits);
    if (!transport)
        return result<std::shared_ptr<NetworkRuntime>>::err(
            std::move(transport).error());
    auto impl = std::make_shared<http_detail::NetworkRuntimeImpl>(
        limits, engine, std::move(info).value(), std::move(sig),
        std::move(transport).value());
    // 装配完成后接通 transport 落定通知（需 shared_from_this 已可用）。
    impl->AdoptTransportOwner();
    return result<std::shared_ptr<NetworkRuntime>>::ok(std::move(impl));
}

} // namespace bbt::infra
