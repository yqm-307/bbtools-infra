#include "redis/CoRedisCliImpl.hpp"

#include <cerrno>

#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/coroutine/object/CoObject.hpp>

namespace bbt::infra {

namespace redis_detail {

namespace {

// 各命令合法回复类型外的统一拒绝：协议形态不符，不是传输失败。
Error UnexpectedReply(const char* what) {
    return MakeError(ErrorCode::ProtocolError,
        std::string("redis: unexpected reply type for ") + what);
}

Error InvalidArg(std::string msg) {
    return MakeError(ErrorCode::InvalidArgument, std::move(msg));
}

// teardown 投递的有界重试参数：post 被拒多为瞬时分配失败，几次
// 短间隔重试足以区分「瞬时」与「执行域不可达」，且不引入无限等待。
constexpr int kTeardownPostRetries = 8;
constexpr int kTeardownPostRetryMs = 4;

// 任意线程上可调用的定长睡眠：必须直达内核 syscall。coroutine Hook
// 全进程拦截 libc nanosleep 并在非协程线程断言——std::this_thread
// ::sleep_for 在调用线程不是协程时命中即崩。
void SleepUnhooked(int ms) noexcept {
    timespec req{ms / 1000, static_cast<long>(ms % 1000) * 1000000L};
    timespec rem{};
    while (::syscall(SYS_nanosleep, &req, &rem) != 0 && errno == EINTR)
        req = rem;
}

} // namespace

result<void> CoRedisCliImpl::Start() {
    const auto gen = bbt::coroutine::CurrentRuntimeGeneration();
    if (gen == 0 || gen != m_info.generation)
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "scheduler not running or runtime generation mismatch"));

    // 先取得共享 executor 并建立 io 域（不创建线程/context）；失败
    // 留在 kCreated，调用方可待 scheduler 就绪后重试 Start。
    auto ready = m_engine.Start();
    if (!ready)
        return result<void>::err(std::move(ready).error());

    int expected = kCreated;
    if (!m_state.compare_exchange_strong(expected, kRunning))
        return result<void>::err(MakeError(
            m_state.load() == kRunning ? ErrorCode::InvalidArgument
                                       : ErrorCode::Closed,
            m_state.load() == kRunning ? "redis client already started"
                                       : "redis client is closing or closed"));

    // 首次连接在 io 域发起；投递失败不致命，首个命令会再触发。
    auto self = shared_from_this();
    m_engine.TryPost([self] { self->EnsureConnOnIoDomain(); });
    return result<void>::ok();
}

result<void> CoRedisCliImpl::PreCheck() const {
    if (g_bbt_tls_coroutine_co == nullptr)
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
            "redis command must run in coroutine context"));
    if (!m_close.IsOpen())
        return result<void>::err(MakeError(ErrorCode::Closed,
            "redis client is closing or closed"));
    const int st = m_state.load();
    if (st != kRunning)
        return result<void>::err(MakeError(
            st == kCreated ? ErrorCode::RuntimeUnavailable
                           : ErrorCode::Closed,
            st == kCreated ? "redis client not started"
                           : "redis client is closing or closed"));
    return result<void>::ok();
}

result<RawReply> CoRedisCliImpl::Submit(RedisOp::Kind kind,
                                       std::vector<std::string> args,
                                       const CallOptions& options) {
    auto pre = PreCheck();
    if (!pre)
        return result<RawReply>::err(std::move(pre).error());

    auto sig = detail::NewCompletionSignal();
    if (!sig)
        return result<RawReply>::err(std::move(sig).error());

    auto self = std::static_pointer_cast<CoRedisCliImpl>(shared_from_this());
    auto op   = std::make_shared<RedisOp>(self, kind, std::move(args));
    op->sig   = std::move(sig).value();

    // 登记先于 post：与 RequestClose 竞态的提交也能被 teardown 明确拒绝。
    if (!RegisterOp(op))
        return result<RawReply>::err(
            MakeError(ErrorCode::Closed, "redis client closed"));

    if (!m_engine.TryPost([self, op] { self->AdmitOnIoDomain(op); })) {
        // io 域不可用：op 从未接触后端（kDone 允许即刻反登记），
        // 与 teardown 收口竞争由 Finish CAS 兜底。
        op->phase = RedisOp::Phase::kDone;
        op->Finish(result<RawReply>::err(
            MakeError(ErrorCode::Closed, "redis client closed")));
        return result<RawReply>::err(
            MakeError(ErrorCode::Closed, "redis client closed"));
    }

    bbt::coroutine::WaitOptions wait;
    wait.deadline = options.deadline;
    wait.cancel   = options.cancel;
    const auto status = op->sig->Wait(wait);
    if (status == bbt::coroutine::WaitStatus::Completed)
        return std::move(*op->outcome);

    // 逻辑结果先行返回；物理清理继续：io 域摘出未发送命令。
    m_engine.TryPost([self, op] { self->AbortOnIoDomain(op); });
    return result<RawReply>::err(detail::WaitStatusToError(status));
}

result<void> CoRedisCliImpl::Ping(const CallOptions& options) {
    auto raw = Submit(RedisOp::Kind::Ping, {"PING"}, options);
    if (!raw)
        return result<void>::err(std::move(raw).error());
    if (raw.value().type == RawReply::Type::Status &&
        raw.value().str == "PONG")
        return result<void>::ok();
    return result<void>::err(UnexpectedReply("PING"));
}

result<std::optional<std::string>> CoRedisCliImpl::Get(
    std::string_view key, const CallOptions& options) {
    if (key.empty())
        return result<std::optional<std::string>>::err(
            InvalidArg("redis GET: empty key"));
    auto raw = Submit(RedisOp::Kind::Get, {"GET", std::string(key)}, options);
    if (!raw)
        return result<std::optional<std::string>>::err(
            std::move(raw).error());
    auto& r = raw.value();
    if (r.type == RawReply::Type::Nil)
        return result<std::optional<std::string>>::ok(std::nullopt);
    if (r.type == RawReply::Type::Bulk)
        return result<std::optional<std::string>>::ok(
            std::optional<std::string>(std::move(r.str)));
    return result<std::optional<std::string>>::err(UnexpectedReply("GET"));
}

result<void> CoRedisCliImpl::Set(std::string_view key, std::string_view value,
                                 const CallOptions& options) {
    if (key.empty())
        return result<void>::err(InvalidArg("redis SET: empty key"));
    auto raw = Submit(RedisOp::Kind::Set,
                      {"SET", std::string(key), std::string(value)}, options);
    if (!raw)
        return result<void>::err(std::move(raw).error());
    if (raw.value().type == RawReply::Type::Status &&
        raw.value().str == "OK")
        return result<void>::ok();
    return result<void>::err(UnexpectedReply("SET"));
}

result<bool> CoRedisCliImpl::Exists(std::string_view key,
                                    const CallOptions& options) {
    if (key.empty())
        return result<bool>::err(InvalidArg("redis EXISTS: empty key"));
    auto raw =
        Submit(RedisOp::Kind::Exists, {"EXISTS", std::string(key)}, options);
    if (!raw)
        return result<bool>::err(std::move(raw).error());
    if (raw.value().type == RawReply::Type::Integer)
        return result<bool>::ok(raw.value().integer > 0);
    return result<bool>::err(UnexpectedReply("EXISTS"));
}

result<std::uint64_t> CoRedisCliImpl::Delete(std::vector<std::string> keys,
                                             const CallOptions& options) {
    if (keys.empty())
        return result<std::uint64_t>::err(
            InvalidArg("redis DEL: empty key list"));
    std::vector<std::string> args;
    args.reserve(keys.size() + 1);
    args.emplace_back("DEL");
    for (const auto& k : keys) {
        if (k.empty())
            return result<std::uint64_t>::err(
                InvalidArg("redis DEL: empty key"));
        args.push_back(k);
    }
    auto raw = Submit(RedisOp::Kind::Delete, std::move(args), options);
    if (!raw)
        return result<std::uint64_t>::err(std::move(raw).error());
    if (raw.value().type == RawReply::Type::Integer)
        return result<std::uint64_t>::ok(
            static_cast<std::uint64_t>(raw.value().integer < 0
                                           ? 0
                                           : raw.value().integer));
    return result<std::uint64_t>::err(UnexpectedReply("DEL"));
}

// ---------------- io 域内部流程 ----------------

void CoRedisCliImpl::AdmitOnIoDomain(std::shared_ptr<RedisOp> op) {
    if (m_pending.size() >= m_config.max_queue) {
        // 队列满：确定性 Overloaded，不进队列、不接触后端。
        op->phase = RedisOp::Phase::kDone;
        op->Finish(result<RawReply>::err(MakeError(ErrorCode::Overloaded,
            "redis: pending queue is full")));
        return;
    }
    m_pending.push_back(op);
    op->queue_it = std::prev(m_pending.end());
    EnsureConnOnIoDomain();
    PumpOnIoDomain();
}

void CoRedisCliImpl::EnsureConnOnIoDomain() {
    // 退役连接在此销毁：本路径只由 admission 触发、必然脱离 hiredis
    // 回调栈；届时旧 context 的 cleanup 已完成、desc 已 release。
    m_retired_conns.clear();
    if (!m_conn)
        m_conn = std::make_shared<RedisConnection>(
            m_engine.Io(), m_config.host, m_config.port,
            std::weak_ptr<CoRedisCliImpl>(shared_from_this()));
    m_conn->OpenOnIoDomain();   // 仅 kIdle 真正发起；其余状态幂等
}

void CoRedisCliImpl::PumpOnIoDomain() {
    while (!m_pending.empty() && m_conn && m_conn->CanSend() &&
           m_inflight < m_config.max_inflight) {
        auto op = m_pending.front();
        m_pending.pop_front();
        if (op->finished.load()) {
            // 已落定（超时/取消/off-domain 收口）仅清除占位。
            op->phase = RedisOp::Phase::kDone;
            op->MaybeUnregister();
            continue;
        }
        if (!m_conn->SendOnIoDomain(&OnReplyThunk, op.get(),
                                    static_cast<int>(op->argv.size()),
                                    op->argv.data(), op->argvlen.data())) {
            op->phase = RedisOp::Phase::kDone;
            op->Finish(result<RawReply>::err(MakeError(
                ErrorCode::TransportError, "redis: command dispatch failed")));
            continue;   // context dying 时 CanSend 下轮自然为假
        }
        op->phase = RedisOp::Phase::kSent;
        ++m_inflight;
    }
}

void CoRedisCliImpl::AbortOnIoDomain(std::shared_ptr<RedisOp> op) {
    if (op->phase.load() != RedisOp::Phase::kQueued)
        return;   // kSent 等回包落定；kDone 已收口
    m_pending.erase(op->queue_it);
    op->phase = RedisOp::Phase::kDone;
    // 调用方已按自身 wait 结果返回，此处仅发布簿记终态。
    op->Finish(result<RawReply>::err(
        MakeError(ErrorCode::Cancelled, "redis: request abandoned")));
}

void CoRedisCliImpl::OnReplyThunk(redisAsyncContext*, void* reply,
                                  void* privdata) {
    auto* raw = static_cast<RedisOp*>(privdata);
    auto  op  = raw->shared_from_this();
    op->owner->OnReplyOnIoDomain(op, static_cast<const redisReply*>(reply));
}

void CoRedisCliImpl::OnReplyOnIoDomain(std::shared_ptr<RedisOp> op,
                                       const redisReply* reply) {
    --m_inflight;
    op->phase = RedisOp::Phase::kDone;
    op->Finish(DecodeReply(reply));
    PumpOnIoDomain();
}

void CoRedisCliImpl::OnConnReadyOnIoDomain() {
    PumpOnIoDomain();
}

void CoRedisCliImpl::OnConnDownOnIoDomain(Error conn_err) {
    // 已发送命令由 hiredis NULL 回调逐个落定（connect 失败阶段无在途）；
    // pending 队列以连接错误统一落定，不跨连接迁移。
    DrainPendingOnIoDomain(conn_err);
    // m_conn 立即脱离：旧 conn 对象是单生命周期语义（断开后不可复用），
    // 新命令经 EnsureConnOnIoDomain 走新建连接；对象本体移入退役暂存，
    // 销毁延迟到脱离本回调栈之后（见 EnsureConnOnIoDomain 头注）。
    if (m_conn)
        m_retired_conns.push_back(std::move(m_conn));
}

void CoRedisCliImpl::DrainPendingOnIoDomain(const Error& err) {
    while (!m_pending.empty()) {
        auto op = m_pending.front();
        m_pending.pop_front();
        op->phase = RedisOp::Phase::kDone;
        op->Finish(result<RawReply>::err(Error(err)));
    }
    // hiredis 已对全部 sent 发过 NULL 回调（connect 失败则从未发送）。
    m_inflight = 0;
}

// ---------------- 关闭链路 ----------------

void CoRedisCliImpl::RequestClose() noexcept {
    if (!m_close.BeginClose())
        return;
    m_state.store(kClosingOrClosed);
    auto self = std::static_pointer_cast<CoRedisCliImpl>(shared_from_this());
    if (m_engine.TryPost([self] { self->TeardownOnIoDomain(); }))
        return;
    // TryPost 失败三种情形：引擎未启动（无 io 域、无连接资源）⇒ 直接
    // 逻辑收口；引擎已封 ⇒ 此前的 teardown 已执行（m_io_dead 门拦在
    // TeardownOffDomain 前）；已启动而 post 被拒 ⇒ 多为瞬时分配失败，
    // 有界重试争取正常 io 域物理清理，持续被拒才落到逻辑收口。
    if (m_engine.Started()) {
        for (int i = 0; i < kTeardownPostRetries; ++i) {
            SleepUnhooked(kTeardownPostRetryMs);
            if (m_engine.TryPost([self] { self->TeardownOnIoDomain(); }))
                return;
        }
    }
    TeardownOffDomain();
}

void CoRedisCliImpl::TeardownOnIoDomain() noexcept {
    if (m_close.IsClosed())
        return;
    std::vector<std::shared_ptr<RedisOp>> snapshot;
    {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        m_io_dead = true;
        snapshot.assign(m_ops.begin(), m_ops.end());
    }
    // 未发送命令移出队列并以 Closed 落定；io_dead 先于快照置位，
    // 此后 RegisterOp 一律失败，无漏网 op。
    DrainPendingOnIoDomain(
        MakeError(ErrorCode::Closed, "redis client closed"));
    for (auto& op : snapshot)
        op->Finish(result<RawReply>::err(
            MakeError(ErrorCode::Closed, "redis client closed")));
    // 回收连接：redisAsyncFree 同步催出在途命令的 NULL 回调——各 op
    // 经 OnReplyOnIoDomain 置 kDone 后离开 m_ops。
    if (m_conn)
        m_conn->TeardownOnIoDomain();
    {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        m_conn_done = true;
    }
    m_engine.SealOnIoDomain();
    DrainCheckClosed();
}

void CoRedisCliImpl::TeardownOffDomain() noexcept {
    // 投递失败的兜底：hiredis context/bridge 只许 io 域触碰，故不能
    // free；仅逻辑收口已登记 op（Finish 本就支持跨线程），在途回包
    // 若引擎仍活会自然落定并经计数门控汇合 MarkClosed。
    std::vector<std::shared_ptr<RedisOp>> snapshot;
    {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        if (m_io_dead)
            return;
        m_io_dead = true;
        snapshot.assign(m_ops.begin(), m_ops.end());
        // 连接簿记落定：能走到这里只有两种情形——
        //   a) 引擎从未启动 ⇒ io 域从未运行 ⇒ 不可能存在连接资源；
        //   b) 已启动引擎的投递有界重试后仍被拒 ⇒ 执行域不可达，teardown
        //      永远无法送达 ⇒ conn 对象不可再管理，按「放弃即脱离簿记」
        //      落定；其 fd 随 conn 对象在 impl 析构时由 desc 析构释放，
        //      未释放的 hiredis context 视为放弃。
        // 引擎已封（此前 teardown 已执行）的情形由上面 m_io_dead 早退
        // 门拦截，不会到达这里。不置位会让 DrainCheckClosed 永远无法
        // MarkClosed、WaitClosed 悬挂。
        m_conn_done = true;
    }
    for (auto& op : snapshot)
        op->Finish(result<RawReply>::err(
            MakeError(ErrorCode::Closed, "redis client closed")));
    DrainCheckClosed();
}

void CoRedisCliImpl::DrainCheckClosed() noexcept {
    bool fin;
    {
        std::lock_guard<std::mutex> lk(m_ops_mtx);
        fin = m_io_dead && m_ops.empty() && m_conn_done;
    }
    if (fin)
        m_close.MarkClosed();
}

} // namespace redis_detail

// 契约装配入口：Create 只校验装配参数并取对象身份/完成信号，
// Start 才真正占用资源；二者均在控制线程使用，不挂起协程。
result<std::shared_ptr<CoRedisCli>>
CoRedisCli::Create(RedisClientConfig config) {
    auto valid = ValidateRedisClientConfig(config);
    if (!valid)
        return result<std::shared_ptr<CoRedisCli>>::err(
            std::move(valid).error());
    auto info = detail::NewObjectInfo("infra.redis_cli");
    if (!info)
        return result<std::shared_ptr<CoRedisCli>>::err(
            std::move(info).error());
    auto sig = detail::NewCompletionSignal();
    if (!sig)
        return result<std::shared_ptr<CoRedisCli>>::err(
            std::move(sig).error());
    auto impl = std::make_shared<redis_detail::CoRedisCliImpl>(
        std::move(config), std::move(info).value(), std::move(sig).value());
    return result<std::shared_ptr<CoRedisCli>>::ok(std::move(impl));
}

} // namespace bbt::infra
