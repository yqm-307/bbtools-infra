#include <bbt/infra/mongo/detail/CoMongoCliImpl.hpp>
#include <bbt/infra/mongo/detail/MongoDetail.hpp>
#include <bbt/infra/mongo/detail/MongoSupport.hpp>
#include <bbt/infra/detail/CloseWait.hpp>

namespace bbt::infra::mongo_detail {

/**
 * CoMongoCliImpl：单集合 client，一个 client 对应一个独占 runtime 与一个集合句柄。
 * 资源归属不变式全部由 runtime 承担；本层只做配置翻译与状态转发。
 */
namespace {

/**
 * 未 Start（m_coll 为空）时的前置校验，顺序与 CoMongoCli 的前置校验一致：
 * 协程外 → InvalidContext；已关 → Closed；其余（协程内、close 仍 open、
 * 未 Start）→ RuntimeUnavailable。m_coll 存在时该路径不触达，由 runtime
 * Submit→PreCheck 走同一优先级。
 */
result<void> CliPrecheckNoColl(const ManagedCloseState& close)
{
    if (g_bbt_tls_coroutine_co == nullptr)
        return result<void>::err(MakeError(ErrorCode::InvalidContext, "mongo command must run in coroutine context"));
    return result<void>::err(MakeError(
        !close.IsOpen() ? ErrorCode::Closed : ErrorCode::RuntimeUnavailable,
        !close.IsOpen() ? "mongo client is closing or closed" : "mongo client not started"));
}

// clang-format off
}
// clang-format on

result<void> CoMongoCliImpl::Start()
{
    if (!g_scheduler->IsInitialized())
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable, "coroutine runtime not initialized"));

    /**
     * 与 Close 配对：关闭门禁、已启动检查与「启动在途」标志在同一临界区
     * 判定——Close 取到关闭权后 Start 不得再进入发布路径。
     */
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        if (!m_close.IsOpen())
            return result<void>::err(MakeError(ErrorCode::Closed, "mongo client is closing or closed"));
        if (m_runtime || m_starting)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument, "mongo client already started"));
        m_starting = true;
    }

    /**
     * 离开 Start 前必清在途标志并唤醒等待中的 Close（含全部失败/自清
     * 路径），否则与 Close 配对会自锁。
     */
    StartingScope starting_scope{this};

    /**
     * 独占 runtime：资源预算照搬 MongoClientConfig 的 worker/queue/
     * 超时项；CoMongoCli 的语义保持「一个 client 一组 worker」，但资源
     * 实体收进显式 owner。
     */
    mongo::MongoRuntimeConfig rcfg;
    rcfg.uri = m_config.uri;
    rcfg.worker_threads = m_config.worker_threads;
    rcfg.max_queue = m_config.max_queue;
    rcfg.server_selection_timeout = m_config.server_selection_timeout;
    rcfg.connect_timeout = m_config.connect_timeout;
    rcfg.socket_timeout = m_config.socket_timeout;
    rcfg.wait_queue_timeout = m_config.wait_queue_timeout;

    auto rt_info = NewObjectInfo("infra.mongo_rt");
    if (!rt_info)
        return result<void>::err(std::move(rt_info).error());
    auto rt = MongoRuntime::Create(std::move(rcfg), std::move(rt_info).value());
    auto started = rt->Start();
    if (!started)
        return result<void>::err(std::move(started).error());

    auto h_info = NewObjectInfo("infra.mongo_coll");
    if (!h_info) {
        (void)rt->Close();
        return result<void>::err(std::move(h_info).error());
    }
    auto coll = std::make_shared<MongoCollImpl>(
        rt, mongo::MongoTarget{m_config.database, m_config.collection}, std::move(h_info).value());

    /**
     * 发布与封口同临界区配对：Close 已封口则绝不发布 worker/lease，改由
     * Start 自清该 runtime（自清完成后 StartingScope 才唤醒 Close，保证
     * Close 返回当刻资源已收口、终态不回退）。
     */
    bool sealed = false;
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        sealed = !m_close.IsOpen();
        if (!sealed) {
            m_coll = std::move(coll);
            m_runtime = std::move(rt);
        }
    }
    if (sealed) {
        (void)rt->Close();
        return result<void>::err(MakeError(ErrorCode::Closed, "mongo client is closing or closed"));
    }
    return result<void>::ok();
}

result<void> CoMongoCliImpl::InsertOne(const MongoDocument& doc, const CallOptions& options)
{
    if (doc.bytes.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "mongo InsertOne: empty document"));
    auto coll = CollSnapshot();
    if (!coll) {
        auto pre = CliPrecheckNoColl(m_close);
        return result<void>::err(std::move(pre).error());
    }
    auto r = coll->Runtime()->Submit(coll, MongoOp::Kind::InsertOne, doc, {}, options);
    if (!r)
        return result<void>::err(std::move(r).error());
    return result<void>::ok();
}

result<std::optional<MongoDocument>> CoMongoCliImpl::FindOne(const MongoDocument& filter, const CallOptions& options)
{
    if (filter.bytes.empty())
        return result<std::optional<MongoDocument>>::err(
            MakeError(ErrorCode::InvalidArgument, "mongo FindOne: empty filter"));
    auto coll = CollSnapshot();
    if (!coll) {
        auto pre = CliPrecheckNoColl(m_close);
        return result<std::optional<MongoDocument>>::err(std::move(pre).error());
    }
    auto r = coll->Runtime()->Submit(coll, MongoOp::Kind::FindOne, filter, {}, options);
    if (!r)
        return result<std::optional<MongoDocument>>::err(std::move(r).error());
    return result<std::optional<MongoDocument>>::ok(std::move(r).value().doc);
}

result<MongoUpdateResult>
CoMongoCliImpl::UpdateOne(const MongoDocument& filter, const MongoDocument& update, const CallOptions& options)
{
    if (filter.bytes.empty())
        return result<MongoUpdateResult>::err(MakeError(ErrorCode::InvalidArgument, "mongo UpdateOne: empty filter"));
    if (update.bytes.empty())
        return result<MongoUpdateResult>::err(MakeError(ErrorCode::InvalidArgument, "mongo UpdateOne: empty update"));
    auto coll = CollSnapshot();
    if (!coll) {
        auto pre = CliPrecheckNoColl(m_close);
        return result<MongoUpdateResult>::err(std::move(pre).error());
    }
    auto r = coll->Runtime()->Submit(coll, MongoOp::Kind::UpdateOne, filter, update, options);
    if (!r)
        return result<MongoUpdateResult>::err(std::move(r).error());
    MongoUpdateResult out;
    out.matched = r.value().matched;
    out.modified = r.value().modified;
    out.upserted = r.value().upserted;
    return result<MongoUpdateResult>::ok(out);
}

result<std::uint64_t> CoMongoCliImpl::DeleteOne(const MongoDocument& filter, const CallOptions& options)
{
    if (filter.bytes.empty())
        return result<std::uint64_t>::err(MakeError(ErrorCode::InvalidArgument, "mongo DeleteOne: empty filter"));
    auto coll = CollSnapshot();
    if (!coll) {
        auto pre = CliPrecheckNoColl(m_close);
        return result<std::uint64_t>::err(std::move(pre).error());
    }
    auto r = coll->Runtime()->Submit(coll, MongoOp::Kind::DeleteOne, filter, {}, options);
    if (!r)
        return result<std::uint64_t>::err(std::move(r).error());
    return result<std::uint64_t>::ok(static_cast<std::uint64_t>(r.value().deleted < 0 ? 0 : r.value().deleted));
}

void CoMongoCliImpl::Close() noexcept
{
    std::shared_ptr<MongoCollImpl> coll;
    std::shared_ptr<MongoRuntime> rt;
    {
        std::unique_lock<std::mutex> lk(m_lifecycle_mtx);

        /**
         * 封口：此后 Start 不得再发布资源
         */
        m_close.BeginClose();

        /**
         * 与在途 Start 配对：先封口，再等它完成（发布或自清）后才取快照
         * 收口——不会出现「Close 已返回、Start 事后发布」；未 Start 时
         * 该等待立即返回（本对象信号即刻落定）。
         */
        m_lifecycle_cv.wait(lk, [this] { return !m_starting; });
        coll = m_coll;
        rt = m_runtime;
    }

    /**
     * 句柄接纳封口（不影响 runtime 在途 op）
     */
    if (coll)
        (void)coll->Close();

    /**
     * native 宿主等物理收口；Co 域只封口并交后台 finalizer
     */
    if (rt)
        (void)rt->Close();

    /**
     * 物理收口落定才发布本层 Closed；未收口时 IsClosed() 仍由 runtime 事实回答，
     * 不提前宣告。终态不回退（重复 Close 幂等）。
     */
    if (!rt || rt->IsClosed())
        m_close.MarkClosed();
}

CoMongoCliImpl::CoMongoCliImpl(MongoClientConfig config, bbt::coroutine::CoObjectInfo info)
    : m_config(std::move(config))
    , m_info(std::move(info))
{
}

CoMongoCliImpl::~CoMongoCliImpl()
{
    (void)Close();
}

bool CoMongoCliImpl::IsClosed() const noexcept
{
    const auto rt = RuntimeSnapshot();
    return rt ? rt->IsClosed() : m_close.IsClosed();
}

bbt::coroutine::CoObjectInfo CoMongoCliImpl::GetObjectInfo() const
{
    return m_info;
}

std::size_t CoMongoCliImpl::LiveWorkersForTest() const noexcept
{
    const auto rt = RuntimeSnapshot();
    return rt ? rt->LiveWorkersForTest() : 0;
}

std::size_t CoMongoCliImpl::QueuedOpsForTest() const noexcept
{
    const auto rt = RuntimeSnapshot();
    return rt ? rt->QueuedOpsForTest() : 0;
}

std::size_t CoMongoCliImpl::DriverCallsInFlightForTest() const noexcept
{
    const auto rt = RuntimeSnapshot();
    return rt ? rt->DriverCallsInFlightForTest() : 0;
}

std::shared_ptr<MongoCollImpl> CoMongoCliImpl::CollSnapshot() const noexcept
{
    std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
    return m_coll;
}

std::shared_ptr<MongoRuntime> CoMongoCliImpl::RuntimeSnapshot() const noexcept
{
    std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
    return m_runtime;
}

void CoMongoCliImpl::FinishStarting() noexcept
{
    {
        std::lock_guard<std::mutex> lk(m_lifecycle_mtx);
        m_starting = false;
    }
    m_lifecycle_cv.notify_all();
}

#ifdef BBT_INFRA_STRINGENT_DEBUG
std::size_t CoMongoCliImpl::RunningDriverCallsForTest() const noexcept
{
    const auto rt = RuntimeSnapshot();
    return rt ? rt->RunningDriverCallsForTest() : 0;
}
#endif

#ifdef BBT_INFRA_STRINGENT_DEBUG
std::size_t CoMongoCliImpl::PeakDriverCallsForTest() const noexcept
{
    const auto rt = RuntimeSnapshot();
    return rt ? rt->PeakDriverCallsForTest() : 0;
}
#endif

void MongoOp::MaybeUnregister()
{
    if (finished && phase.load() == Phase::kDone)
        runtime->UnregisterOp(shared_from_this());
}

// clang-format off
}
// clang-format on

namespace bbt::infra::mongo_detail {
CoMongoCliImpl::StartingScope::~StartingScope() noexcept
{
    cli->FinishStarting();
}

// clang-format off
}
// clang-format on

namespace bbt::infra {

CoMongoCli::CoMongoCli(MongoClientConfig config, bbt::coroutine::CoObjectInfo info)
    : m_impl(std::make_unique<mongo_detail::CoMongoCliImpl>(std::move(config), std::move(info)))
{
}

CoMongoCli::~CoMongoCli() = default;

result<void> CoMongoCli::Start()
{
    return m_impl->Start();
}

result<void> CoMongoCli::InsertOne(const MongoDocument& doc, const CallOptions& options)
{
    return m_impl->InsertOne(doc, options);
}

result<std::optional<MongoDocument>> CoMongoCli::FindOne(const MongoDocument& filter, const CallOptions& options)
{
    return m_impl->FindOne(filter, options);
}

result<MongoUpdateResult>
CoMongoCli::UpdateOne(const MongoDocument& filter, const MongoDocument& update, const CallOptions& options)
{
    return m_impl->UpdateOne(filter, update, options);
}

result<std::uint64_t> CoMongoCli::DeleteOne(const MongoDocument& filter, const CallOptions& options)
{
    return m_impl->DeleteOne(filter, options);
}

void CoMongoCli::Close() noexcept
{
    m_impl->Close();
}

bool CoMongoCli::IsClosed() const noexcept
{
    return m_impl->IsClosed();
}

bbt::coroutine::CoObjectInfo CoMongoCli::GetObjectInfo() const
{
    return m_impl->GetObjectInfo();
}

std::shared_ptr<mongo_detail::CoMongoCliImpl>
mongo_detail::CoMongoCliImpl::ForTest(const std::shared_ptr<CoMongoCli>& client)
{
    return std::shared_ptr<CoMongoCliImpl>(client, client->m_impl.get());
}

/**
 * 装配入口：Create 只校验装配参数并取对象身份，
 * Start 才真正占用资源；二者均在控制线程使用，不挂起协程。
 */
result<std::shared_ptr<CoMongoCli>> CoMongoCli::Create(MongoClientConfig config)
{
    auto valid = ValidateMongoClientConfig(config);
    if (!valid)
        return result<std::shared_ptr<CoMongoCli>>::err(std::move(valid).error());
    auto info = mongo_detail::NewObjectInfo("infra.mongo_cli");
    if (!info)
        return result<std::shared_ptr<CoMongoCli>>::err(std::move(info).error());
    auto impl = std::shared_ptr<CoMongoCli>(new CoMongoCli(std::move(config), std::move(info).value()));
    return result<std::shared_ptr<CoMongoCli>>::ok(std::move(impl));
}

// clang-format off
}
// clang-format on
