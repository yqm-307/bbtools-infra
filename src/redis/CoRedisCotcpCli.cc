#include "redis/CoRedisCotcpCli.hpp"

#include <bbt/coroutine/detail/Scheduler.hpp>

#include "redis/RedisCotcpOwner.hpp"

namespace bbt::infra {
namespace redis_detail {

namespace {

Error UnexpectedReply(const char* what) {
    return MakeError(ErrorCode::ProtocolError,
                     std::string("redis: unexpected reply type for ") + what);
}

Error InvalidArg(std::string msg) {
    return MakeError(ErrorCode::InvalidArgument, std::move(msg));
}

Error ClosedError() {
    return MakeError(ErrorCode::Closed, "redis client closed");
}

} // namespace

CotcpBindingTotals CotcpBindingTotalsForTest() noexcept {
    const auto& c = BindingCounters();
    CotcpBindingTotals t;
    t.readers_created    = c.readers_created.load();
    t.readers_freed      = c.readers_freed.load();
    t.conns_created      = c.conns_created.load();
    t.conns_destroyed    = c.conns_destroyed.load();
    t.hiredis_calls      = c.hiredis_calls.load();
    t.hiredis_max_call_ns = c.hiredis_max_call_ns.load();
    t.commands_encoded   = c.commands_encoded.load();
    t.write_rounds       = c.write_rounds.load();
    t.read_rounds        = c.read_rounds.load();
    t.partial_write_ops  = c.partial_write_ops.load();
    t.partial_read_ops   = c.partial_read_ops.load();
    return t;
}

void ResetCotcpBindingTotalsForTest() noexcept { ResetBindingCounters(); }

CoRedisCotcpCliImpl::~CoRedisCotcpCliImpl() {
    // 析构只做兜底：正常路径的连接已在 owner coroutine / RequestClose 中收口。
    std::lock_guard<std::mutex> lk(m_conn_mtx);
    m_conn.reset();
}

// ---------------- 公开语义（与旧路径同形）----------------

result<void> CoRedisCotcpCliImpl::Start() {
    const auto gen = bbt::coroutine::CurrentRuntimeGeneration();
    if (gen == 0 || gen != m_info.generation)
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "scheduler not running or runtime generation mismatch"));

    int expected = kCreated;
    if (!m_state.compare_exchange_strong(expected, kRunning))
        return result<void>::err(MakeError(
            m_state.load() == kRunning ? ErrorCode::InvalidArgument
                                       : ErrorCode::Closed,
            m_state.load() == kRunning ? "redis client already started"
                                       : "redis client is closing or closed"));
    // 候选不建立 strand/executor 执行域：owner coroutine 即执行域，连接在
    // 首条命令的 owner 批次数内按需 DialTCP。
    return result<void>::ok();
}

result<void> CoRedisCotcpCliImpl::PreCheck() const {
    if (g_bbt_tls_coroutine_co == nullptr)
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
            "redis command must run in coroutine context"));
    if (!m_close.IsOpen())
        return result<void>::err(ClosedError());
    const int st = m_state.load();
    if (st != kRunning)
        return result<void>::err(MakeError(
            st == kCreated ? ErrorCode::RuntimeUnavailable
                           : ErrorCode::Closed,
            st == kCreated ? "redis client not started"
                           : "redis client is closing or closed"));
    return result<void>::ok();
}

result<RawReply> CoRedisCotcpCliImpl::Submit(CotcpRedisOp::Kind   kind,
                                            std::vector<std::string> args,
                                            const CallOptions&   options) {
    auto pre = PreCheck();
    if (!pre)
        return result<RawReply>::err(std::move(pre).error());

    auto sig = detail::NewCompletionSignal();
    if (!sig)
        return result<RawReply>::err(std::move(sig).error());

    auto self = shared_from_this();
    auto op   = std::make_shared<CotcpRedisOp>(m_probe, kind, std::move(args),
                                              options);
    op->sig   = std::move(sig).value();

    bool spawn = false;
    {
        // 登记与入队在 m_q_mtx 下原子完成：与 RequestClose 的封口/快照互斥，
        // 保证「已登记必被终态收口」与「入队必被某批次消费」同时成立。
        std::lock_guard<std::mutex> lk(m_q_mtx);
        if (m_io_dead)
            return result<RawReply>::err(ClosedError());
        if (m_queue.size() >= m_config.max_queue)
            return result<RawReply>::err(MakeError(
                ErrorCode::Overloaded, "redis: pending queue is full"));
        {
            std::lock_guard<std::mutex> reg(m_reg_mtx);
            m_registered.insert(op);
        }
        m_queue.push_back(op);
        if (!m_owner_running) {
            m_owner_running = true;
            spawn           = true;
        }
    }

    if (spawn) {
        bool succ = false;
        g_scheduler->RegistCoroutineTask([self] { self->RunOwnerCoroutine(); },
                                         succ);
        if (!succ) {
            // 执行域不可达：不留下永不落定的等待者——队列内与已登记 op 统一收口。
            {
                std::lock_guard<std::mutex> lk(m_q_mtx);
                m_owner_running = false;
                m_io_dead       = true;
                m_queue.clear();
            }
            FinishAllRegistered(MakeError(ErrorCode::RuntimeUnavailable,
                "redis: owner coroutine unavailable"));
            return result<RawReply>::err(MakeError(ErrorCode::RuntimeUnavailable,
                "redis: owner coroutine unavailable"));
        }
    }

    bbt::coroutine::WaitOptions wait;
    wait.deadline = options.deadline;
    wait.cancel   = options.cancel;
    const auto status = op->sig->Wait(wait);
    if (status == bbt::coroutine::WaitStatus::Completed)
        return std::move(*op->outcome);
    // 逻辑结果先行返回；物理清理由 owner coroutine 继续（与旧路径同一分离原则）。
    return result<RawReply>::err(detail::WaitStatusToError(status));
}

result<void> CoRedisCotcpCliImpl::Ping(const CallOptions& options) {
    auto raw = Submit(CotcpRedisOp::Kind::Ping, {"PING"}, options);
    if (!raw)
        return result<void>::err(std::move(raw).error());
    if (raw.value().type == RawReply::Type::Status &&
        raw.value().str == "PONG")
        return result<void>::ok();
    return result<void>::err(UnexpectedReply("PING"));
}

result<std::optional<std::string>> CoRedisCotcpCliImpl::Get(
    std::string_view key, const CallOptions& options) {
    if (key.empty())
        return result<std::optional<std::string>>::err(
            InvalidArg("redis GET: empty key"));
    auto raw = Submit(CotcpRedisOp::Kind::Get, {"GET", std::string(key)},
                      options);
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

result<void> CoRedisCotcpCliImpl::Set(std::string_view key,
                                     std::string_view value,
                                     const CallOptions& options) {
    if (key.empty())
        return result<void>::err(InvalidArg("redis SET: empty key"));
    auto raw = Submit(CotcpRedisOp::Kind::Set,
                      {"SET", std::string(key), std::string(value)}, options);
    if (!raw)
        return result<void>::err(std::move(raw).error());
    if (raw.value().type == RawReply::Type::Status &&
        raw.value().str == "OK")
        return result<void>::ok();
    return result<void>::err(UnexpectedReply("SET"));
}

result<bool> CoRedisCotcpCliImpl::Exists(std::string_view        key,
                                        const CallOptions& options) {
    if (key.empty())
        return result<bool>::err(InvalidArg("redis EXISTS: empty key"));
    auto raw = Submit(CotcpRedisOp::Kind::Exists, {"EXISTS", std::string(key)},
                      options);
    if (!raw)
        return result<bool>::err(std::move(raw).error());
    if (raw.value().type == RawReply::Type::Integer)
        return result<bool>::ok(raw.value().integer > 0);
    return result<bool>::err(UnexpectedReply("EXISTS"));
}

result<std::uint64_t> CoRedisCotcpCliImpl::Delete(
    std::vector<std::string> keys, const CallOptions& options) {
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
    auto raw = Submit(CotcpRedisOp::Kind::Delete, std::move(args), options);
    if (!raw)
        return result<std::uint64_t>::err(std::move(raw).error());
    if (raw.value().type == RawReply::Type::Integer)
        return result<std::uint64_t>::ok(
            static_cast<std::uint64_t>(raw.value().integer < 0
                                           ? 0
                                           : raw.value().integer));
    return result<std::uint64_t>::err(UnexpectedReply("DEL"));
}

// ---------------- owner coroutine（独占执行域）----------------

void CoRedisCotcpCliImpl::RunOwnerCoroutine() {
    m_probe->owner_runs.fetch_add(1);
    for (;;) {
        auto op = TakeNextQueued();
        if (!op) {
            // 退出判定与 Submit 的启动判定在同一把锁下：要么这里看到非空队列
            // 继续消费，要么 Submit 之后会自建新批次——不会丢唤醒。
            std::lock_guard<std::mutex> lk(m_q_mtx);
            if (!m_queue.empty())
                continue;
            m_owner_running = false;
            break;
        }
        ExecuteOne(op);
    }
    // B1：批次退出前若已封口，则物理收口并释放连接对象 —— 使 DrainCheckClosed
    // 的「物理连接已收口」前置成立（WaitClosed 的 Closed 必须对应 fd 已关闭）。
    if (IsIoDead())
        CloseAndDropConnOnCoroutine();
    DrainCheckClosed();
}

void CoRedisCotcpCliImpl::CloseAndDropConnOnCoroutine() noexcept {
    std::lock_guard<std::mutex> lk(m_conn_mtx);
    if (!m_conn)
        return;
    m_conn->RequestClose(); // 幂等：fd 立即关闭（在途 op 归零），或待 op 返回后关闭
    m_conn.reset();         // 释放最后一个引用（CoTCP 析构 close 幂等）
}

bool CoRedisCotcpCliImpl::ConnPhysicallyClosed() const noexcept {
    std::lock_guard<std::mutex> lk(m_conn_mtx);
    if (!m_conn)
        return true; // 连接对象已释放 ⇒ 无 fd 可留
    return m_conn->CloseRequested() && m_conn->IsClosed();
}

std::shared_ptr<CotcpRedisOp> CoRedisCotcpCliImpl::TakeNextQueued() {
    for (;;) {
        std::shared_ptr<CotcpRedisOp> op;
        {
            std::lock_guard<std::mutex> lk(m_q_mtx);
            if (m_queue.empty())
                return nullptr;
            op = m_queue.front();
            m_queue.pop_front();
        }
        if (op->finished.load()) {
            // 已由 deadline/cancel/owner close 落定：只做簿记，不接触后端。
            m_probe->skipped_after_finish.fetch_add(1);
            op->phase.store(CotcpRedisOp::Phase::kDone);
            RetireOp(op);
            continue;
        }
        return op;
    }
}

void CoRedisCotcpCliImpl::RetireOp(const std::shared_ptr<CotcpRedisOp>& op) {
    {
        std::lock_guard<std::mutex> reg(m_reg_mtx);
        m_registered.erase(op);
    }
    DrainCheckClosed();
}

void CoRedisCotcpCliImpl::DropConnOnCoroutine() noexcept {
    std::lock_guard<std::mutex> lk(m_conn_mtx);
    m_conn.reset(); // RedisCotcpConn 析构：redisReader 恰好释放一次
}

void CoRedisCotcpCliImpl::AbandonConnOnCoroutine() noexcept {
    // 出错后连接不可复用（契约 §8.1：不自动重发、不伪造 EOF）：标记废弃并
    // 走 CoTCP::RequestClose 释放 fd（fd 收口只在 CoTCP 内发生，恰好一次）。
    m_probe->conns_broken.fetch_add(1);
    std::lock_guard<std::mutex> lk(m_conn_mtx);
    if (m_conn) {
        m_conn->MarkBroken();
        m_conn->RequestClose();
    }
}

void CoRedisCotcpCliImpl::ExecuteOne(const std::shared_ptr<CotcpRedisOp>& op) {
    op->phase.store(CotcpRedisOp::Phase::kWorking);

    auto finish = [&](result<RawReply> r) {
        op->phase.store(CotcpRedisOp::Phase::kDone);
        op->Finish(std::move(r)); // 晚到终态只被计数，不覆盖
        RetireOp(op);
    };

    if (!m_close.IsOpen()) {
        finish(result<RawReply>::err(ClosedError()));
        return;
    }

    // 1) 连接：DialTCP（DNS/connect/等待/取消全部由 CoTCP 承担）。
    //    上一条失败即被废弃，这里按需重建（新命令触发重连）。
    //    B1：新连接**先发布再拨号** —— RequestClose 必须能看到这条在途连接，
    //    否则关闭请求既打不断 dial，也无法在 dial 成功后物理收口。
    std::shared_ptr<RedisCotcpConn> conn;
    {
        std::lock_guard<std::mutex> lk(m_conn_mtx);
        const bool need_dial =
            !m_conn || m_conn->Broken() || m_conn->IsClosed();
        if (!need_dial)
            conn = m_conn;
    }
    if (!conn) {
        DropConnOnCoroutine();
        auto fresh = std::make_shared<RedisCotcpConn>(m_config.host,
                                                     m_config.port);
        {
            std::lock_guard<std::mutex> lk(m_conn_mtx);
            m_conn = fresh;
        }
        m_probe->dial_attempts.fetch_add(1);
        m_probe->dial_inflight.fetch_add(1);
        auto dial = fresh->Dial(op->options);
        m_probe->dial_inflight.fetch_sub(1);
        if (!dial) {
            Error e = std::move(dial).error();
            // 拨号失败或被关闭请求打断：不保留连接对象，也不留任何 socket。
            CloseAndDropConnOnCoroutine();
            finish(result<RawReply>::err(std::move(e)));
            return;
        }
        // 测试接缝：dial 成功、尚未编码的确定性落点（默认空）。
        if (m_post_dial_gate_for_test)
            m_post_dial_gate_for_test();
        conn = std::move(fresh);
    }

    // B1：统一关闭门（拨号返回后 / 复用连接前）：关闭已请求则绝不编码/发送。
    if (!m_close.IsOpen() || conn->CloseRequested()) {
        CloseAndDropConnOnCoroutine();
        finish(result<RawReply>::err(ClosedError()));
        return;
    }

    // 2) 编码（hiredis 同步，无 I/O）。
    auto bytes = RedisCotcpConn::EncodeCommand(
        static_cast<int>(op->argv.size()), op->argv.data(), op->argvlen.data());
    if (!bytes) {
        finish(result<RawReply>::err(std::move(bytes).error()));
        return;
    }

    // 3) 写满（部分写/WouldBlock 由 CoTCP 调和；等待在 hiredis 调用之外）。
    auto w = conn->WriteAllBytes(bytes.value(), op->options);
    if (!w) {
        finish(result<RawReply>::err(std::move(w).error()));
        AbandonConnOnCoroutine();
        return;
    }

    // 4) 读一条完整 reply（CoTCP 等待与 hiredis 同步解析交替）。
    auto r = conn->ReadReply(op->options);
    if (!r) {
        finish(result<RawReply>::err(std::move(r).error()));
        AbandonConnOnCoroutine();
        return;
    }
    finish(std::move(r));
}

// ---------------- 关闭链路 ----------------

void CoRedisCotcpCliImpl::RequestClose() noexcept {
    if (!m_close.BeginClose())
        return;
    m_state.store(kClosingOrClosed);

    // 1) 封口：此后新命令一律 Closed，且不再启动新 owner 批次。
    std::vector<std::shared_ptr<CotcpRedisOp>> snapshot;
    {
        std::lock_guard<std::mutex> lk(m_q_mtx);
        m_io_dead = true;
    }
    {
        std::lock_guard<std::mutex> reg(m_reg_mtx);
        snapshot.assign(m_registered.begin(), m_registered.end());
    }
    // 已登记 op 统一收口（CAS：与 owner coroutine 的落定竞争只发布一次终态）。
    // 未入队的 op 由 owner 批次出队时跳过（TakeNextQueued 的 skip 分支）。
    for (auto& op : snapshot)
        op->Finish(result<RawReply>::err(ClosedError()));

    // 2) 物理：唤醒挂起在 CoTCP 等待上的 owner coroutine；fd 由 CoTCP 唯一收口
    //    （在途 op 归还后 _EndIo 完成物理关闭，m_closed_source 落定）。
    {
        std::lock_guard<std::mutex> lk(m_conn_mtx);
        if (m_conn)
            m_conn->RequestClose();
    }
    DrainCheckClosed();
}

bool CoRedisCotcpCliImpl::OwnerRunning() const {
    std::lock_guard<std::mutex> lk(m_q_mtx);
    return m_owner_running;
}

bool CoRedisCotcpCliImpl::IsIoDead() const {
    std::lock_guard<std::mutex> lk(m_q_mtx);
    return m_io_dead;
}

void CoRedisCotcpCliImpl::FinishAllRegistered(const Error& err) noexcept {
    std::vector<std::shared_ptr<CotcpRedisOp>> snapshot;
    {
        std::lock_guard<std::mutex> reg(m_reg_mtx);
        snapshot.assign(m_registered.begin(), m_registered.end());
    }
    for (auto& op : snapshot)
        op->Finish(result<RawReply>::err(Error(err)));
    {
        std::lock_guard<std::mutex> reg(m_reg_mtx);
        m_registered.clear();
    }
    DrainCheckClosed();
}

void CoRedisCotcpCliImpl::DrainCheckClosed() noexcept {
    // B1：Closed 必须对应物理连接收口 —— 除「已封口 + owner 批次退出 + 无已登记 op」
    // 外，还要求连接对象已释放或 CoTCP 已物理关闭（否则 WaitClosed 会在 fd 仍开着
    // 时提前返回 Closed，关闭语义不成立）。
    bool fin = IsIoDead() && !OwnerRunning();
    if (fin)
        fin = ConnPhysicallyClosed();
    if (fin) {
        std::lock_guard<std::mutex> reg(m_reg_mtx);
        fin = m_registered.empty();
    }
    if (fin)
        m_close.MarkClosed();
}

// ---------------- 探针 ----------------

CotcpBindingSnapshot CoRedisCotcpCliImpl::ProbeSnapshot() const {
    CotcpBindingSnapshot s;
    s.owner_runs            = m_probe->owner_runs.load();
    s.finish_conflicts      = m_probe->finish_conflicts.load();
    s.skipped_after_finish  = m_probe->skipped_after_finish.load();
    s.dial_attempts         = m_probe->dial_attempts.load();
    s.dial_inflight         = m_probe->dial_inflight.load();
    s.conns_broken          = m_probe->conns_broken.load();
    {
        std::lock_guard<std::mutex> lk(m_q_mtx);
        s.queued = m_queue.size();
    }
    {
        std::lock_guard<std::mutex> reg(m_reg_mtx);
        s.registered = m_registered.size();
    }
    {
        std::lock_guard<std::mutex> lk(m_conn_mtx);
        s.has_conn    = static_cast<bool>(m_conn);
        s.conn_closed = !m_conn ||
                        (m_conn->CloseRequested() && m_conn->IsClosed());
    }
    return s;
}

void CoRedisCotcpCliImpl::SetPostDialGateForTest(std::function<void()> gate) {
    m_post_dial_gate_for_test = std::move(gate);
}

std::shared_ptr<tcp::CoTCP> CoRedisCotcpCliImpl::TransportForTest() const {
    std::lock_guard<std::mutex> lk(m_conn_mtx);
    if (!m_conn)
        return nullptr;
    return m_conn->Transport();
}

} // namespace redis_detail

// 内部装配入口：与 CoRedisCli::Create 同一套参数校验与身份/信号来源，
// 但装配 CoTCP owner binding 候选（不改变生产默认路径）。
result<std::shared_ptr<redis_detail::CoRedisCotcpCliImpl>>
redis_detail::CreateCotcpRedisCli(RedisClientConfig config) {
    auto valid = ValidateRedisClientConfig(config);
    if (!valid)
        return result<std::shared_ptr<CoRedisCotcpCliImpl>>::err(
            std::move(valid).error());
    auto info = detail::NewObjectInfo("infra.redis_cli.cotcp");
    if (!info)
        return result<std::shared_ptr<CoRedisCotcpCliImpl>>::err(
            std::move(info).error());
    auto sig = detail::NewCompletionSignal();
    if (!sig)
        return result<std::shared_ptr<CoRedisCotcpCliImpl>>::err(
            std::move(sig).error());
    auto impl = std::make_shared<CoRedisCotcpCliImpl>(
        std::move(config), std::move(info).value(), std::move(sig).value());
    return result<std::shared_ptr<CoRedisCotcpCliImpl>>::ok(std::move(impl));
}

} // namespace bbt::infra
