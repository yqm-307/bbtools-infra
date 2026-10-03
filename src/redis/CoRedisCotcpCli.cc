#include "redis/CoRedisCotcpCli.hpp"

#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/LocalThread.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>   // g_scheduler->IsInitialized / RegistCoroutineTask

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
    t.readers_created     = c.readers_created.load();
    t.readers_freed       = c.readers_freed.load();
    t.conns_created       = c.conns_created.load();
    t.conns_destroyed     = c.conns_destroyed.load();
    t.hiredis_calls       = c.hiredis_calls.load();
    t.hiredis_max_call_ns = c.hiredis_max_call_ns.load();
    t.commands_encoded    = c.commands_encoded.load();
    t.write_rounds        = c.write_rounds.load();
    t.read_rounds         = c.read_rounds.load();
    t.partial_write_ops   = c.partial_write_ops.load();
    t.partial_read_ops    = c.partial_read_ops.load();
    return t;
}

void ResetCotcpBindingTotalsForTest() noexcept { ResetBindingCounters(); }

CoRedisCotcpCliImpl::~CoRedisCotcpCliImpl() {
    // 析构只做兜底：正常路径的连接已在 owner coroutine / Close 中收口。
    Close();
    std::lock_guard<std::mutex> lk(m_conn_mtx);
    m_conn.reset();
}

// ---------------- 公开语义（与旧路径同形）----------------

result<void> CoRedisCotcpCliImpl::Start() {
    // 进程寿命运行时：不再有运行时代际，「运行时是否在跑」只看 Scheduler。
    if (!g_scheduler || !g_scheduler->IsInitialized())
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "scheduler not running"));

    int expected = kCreated;
    if (!m_state.compare_exchange_strong(expected, kRunning))
        return result<void>::err(MakeError(
            m_state.load() == kRunning ? ErrorCode::InvalidArgument
                                       : ErrorCode::Closed,
            m_state.load() == kRunning ? "redis client already started"
                                       : "redis client is closing or closed"));
    // 候选不建立 strand/executor 执行域：owner coroutine 即执行域，连接在首条
    // 命令的 owner 批次数内按需 DialTCP。
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

    auto op   = std::make_shared<CotcpRedisOp>(m_probe, kind, std::move(args),
                                              options);
    op->waiter = bbt::coroutine::sync::CoWaiter::Create();
    if (!op->waiter)
        return result<RawReply>::err(MakeError(ErrorCode::InternalError,
            "redis: failed to create request waiter"));

    bbt::coroutine::WaitOptions wait;
    wait.deadline = options.deadline;
    auto self = shared_from_this();
    // self 由等待回调按值持有，覆盖 Wait 挂起到 owner 入队的窗口；否则调用方
    // 释放最后一个 shared_ptr 时，回调中的 this 可能悬空。
    const auto status = op->waiter->WaitWithCallback(
        wait, [self, op] {
            self->AdmitOnWaitRegistered(op);
            return true;
        }, nullptr);
    if (status == bbt::coroutine::WaitStatus::Completed) {
        auto out = op->TakeOutcome();
        if (out)
            return std::move(*out);
        // 关闭唤醒先于终态发布：按 Closed 交付（终态仍由收口路径落定一次）。
        return result<RawReply>::err(ClosedError());
    }
    // 超时/取消也要先发布 op 终态：owner 出队时会跳过它，避免调用方已经
    // 返回后仍把排队命令发到 Redis。物理清理由 owner coroutine 继续。
    auto wait_error = detail::WaitStatusToError(status);
    auto expected_phase = CotcpRedisOp::Phase::kQueued;
    if (op->phase.compare_exchange_strong(
            expected_phase, CotcpRedisOp::Phase::kDone,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        op->Finish(result<RawReply>::err(Error(wait_error)));
        RetireOp(op);
    }
    return result<RawReply>::err(std::move(wait_error));
}

void CoRedisCotcpCliImpl::AdmitOnWaitRegistered(
    const std::shared_ptr<CotcpRedisOp>& op) {
    if (m_pre_admit_gate_for_test)
        m_pre_admit_gate_for_test();

    bool spawn = false;
    Error admission_error;
    {
        // 回调已在 WaitWithCallback 的等待事件登记之后执行；此处再入队，
        // owner/Close 的 Finish→Notify 只能命中已就绪的等待位。
        std::lock_guard<std::mutex> lk(m_q_mtx);
        if (m_io_dead) {
            admission_error = ClosedError();
        } else if (m_queue.size() >= m_config.max_queue) {
            admission_error = MakeError(ErrorCode::Overloaded,
                                        "redis: pending queue is full");
        } else {
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
    }

    if (!admission_error.message.empty()) {
        op->Finish(result<RawReply>::err(std::move(admission_error)));
        return;
    }
    if (!spawn)
        return;

    auto self = shared_from_this();
    bool succ = false;
    g_scheduler->RegistCoroutineTask([self] { self->RunOwnerCoroutine(); }, succ);
    if (!succ) {
        // 执行域不可达：不留下永不落定的等待者——清退并统一收口。
        {
            std::lock_guard<std::mutex> lk(m_q_mtx);
            m_owner_running = false;
            m_io_dead       = true;
            m_queue.clear();
        }
        FinishAllRegistered(MakeError(ErrorCode::RuntimeUnavailable,
            "redis: owner coroutine unavailable"));
    }
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
    // 批次退出前若已封口，物理收口并释放连接对象，使 DrainCheckClosed 的
    // 「物理连接已收口」前置成立。
    if (IsIoDead())
        CloseAndDropConnOnCoroutine();
    DrainCheckClosed();
}

void CoRedisCotcpCliImpl::CloseAndDropConnOnCoroutine() noexcept {
    std::shared_ptr<RedisCotcpConn> conn;
    {
        std::lock_guard<std::mutex> lk(m_conn_mtx);
        conn = std::move(m_conn);
    }
    if (conn)
        conn->Close();   // 幂等：fd 立即关闭、reader 释放；随后释放最后一个引用
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
            // 已由 deadline/关闭落定：只做簿记，不接触后端。
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

void CoRedisCotcpCliImpl::AbandonConnOnCoroutine() noexcept {
    // 出错后连接不可复用（不自动重发、不伪造 EOF）：标记废弃并物理收口 fd。
    m_probe->conns_broken.fetch_add(1);
    std::shared_ptr<RedisCotcpConn> conn;
    {
        std::lock_guard<std::mutex> lk(m_conn_mtx);
        conn = m_conn;
    }
    if (conn) {
        conn->MarkBroken();
        conn->Close();
    }
}

void CoRedisCotcpCliImpl::ExecuteOne(const std::shared_ptr<CotcpRedisOp>& op) {
    auto expected_phase = CotcpRedisOp::Phase::kQueued;
    if (!op->phase.compare_exchange_strong(
            expected_phase, CotcpRedisOp::Phase::kWorking,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        if (expected_phase == CotcpRedisOp::Phase::kDone)
            RetireOp(op);
        return;
    }

    auto finish = [&](result<RawReply> r) {
        op->phase.store(CotcpRedisOp::Phase::kDone);
        op->Finish(std::move(r)); // 晚到终态只被计数，不覆盖
        RetireOp(op);
    };

    if (!m_close.IsOpen()) {
        finish(result<RawReply>::err(ClosedError()));
        return;
    }

    // 调用方可能已因 deadline/取消返回，而 owner 尚未取得队首；即使该
    // race 没有先由 Submit 落定，也不能为已过期且尚未写入的请求发字节。
    if (std::chrono::steady_clock::now() >= op->options.deadline) {
        finish(result<RawReply>::err(MakeError(
            ErrorCode::TimedOut, "redis: request deadline expired")));
        return;
    }

    // 1) 连接：单连接、不重连。首次按需 DialTCP；连接一旦建立即复用。
    std::shared_ptr<RedisCotcpConn> conn;
    {
        std::lock_guard<std::mutex> lk(m_conn_mtx);
        conn = m_conn;
    }
    if (!conn) {
        auto fresh = std::make_shared<RedisCotcpConn>(m_config.host,
                                                     m_config.port);
        // 先发布再拨号：Close 必须能看到这条在途连接，才能封口其 dial 等待登记。
        {
            std::lock_guard<std::mutex> lk(m_conn_mtx);
            m_conn = fresh;
        }
        m_probe->dial_attempts.fetch_add(1);
        m_probe->dial_inflight.fetch_add(1);
        auto dial = fresh->Dial(op->options, m_dial_waiters);
        m_probe->dial_inflight.fetch_sub(1);
        if (!dial) {
            // 拨号失败/被关闭请求打断：连接不可用（不重连），保留对象供收口判定。
            fresh->MarkBroken();
            fresh->Close();
            finish(result<RawReply>::err(std::move(dial).error()));
            return;
        }
        // 测试接缝：dial 成功、尚未编码的确定性落点（默认空）。
        if (m_post_dial_gate_for_test)
            m_post_dial_gate_for_test();
        conn = std::move(fresh);
    }

    // 连接已被 owner 标记为不可复用时，必须优先返回 TransportError；
    // CloseRequested 是该错误的物理收口结果，不能遮蔽 Broken 状态并触发重连。
    if (conn->Broken()) {
        finish(result<RawReply>::err(MakeError(
            ErrorCode::TransportError, "redis: connection not reusable")));
        return;
    }
    // 统一关闭门（拨号返回后 / 复用连接前）：关闭已请求则绝不编码/发送。
    if (!m_close.IsOpen() || conn->CloseRequested()) {
        CloseAndDropConnOnCoroutine();
        finish(result<RawReply>::err(ClosedError()));
        return;
    }
    if (!conn->ReaderAvailable()) {
        finish(result<RawReply>::err(MakeError(
            ErrorCode::InternalError, "redis: reply reader unavailable")));
        return;
    }

    // Dial/队列等待可能已经耗尽 deadline；保持健康连接可复用，不把
    // 尚未开始写入的超时请求当成传输失败。
    if (std::chrono::steady_clock::now() >= op->options.deadline) {
        finish(result<RawReply>::err(MakeError(
            ErrorCode::TimedOut, "redis: request deadline expired")));
        return;
    }

    // 2) 编码（hiredis 同步，无 I/O）。
    auto bytes = RedisCotcpConn::EncodeCommand(
        static_cast<int>(op->argv.size()), op->argv.data(), op->argvlen.data());
    if (!bytes) {
        finish(result<RawReply>::err(std::move(bytes).error()));
        return;
    }

    if (std::chrono::steady_clock::now() >= op->options.deadline) {
        finish(result<RawReply>::err(MakeError(
            ErrorCode::TimedOut, "redis: request deadline expired")));
        return;
    }

    // 3) 写满（部分写/WouldBlock 由 CoTCP 调和；等待在 hiredis 调用之外）。
    auto w = conn->WriteAllBytes(bytes.value(), op->options);
    if (!w) {
        auto write_error = std::move(w).error();
        const bool preserve_conn =
            write_error.code == ErrorCode::TimedOut &&
            write_error.transferred_bytes == 0;
        finish(result<RawReply>::err(Error(write_error)));
        if (!preserve_conn)
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

void CoRedisCotcpCliImpl::Close() noexcept {
    if (!m_close.BeginClose())
        return;   // 幂等：仅首次进入执行收口
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
    // 已登记 op 统一收口（CAS：与 owner coroutine 的落定竞争只发布一次终态），
    // 唤醒各自挂起的业务协程；未入队的 op 由 owner 出队时跳过。
    for (auto& op : snapshot)
        op->Finish(result<RawReply>::err(ClosedError()));

    // 2) 打断在途 DialTCP：封口 owner 级 dial 等待登记，挂起的 connect 等待段
    //    随即以 Closed 返回并自行释放候选 fd（不再有取消令牌）。
    m_dial_waiters->SealWakeAndDrainRegistrations();

    // 3) 在本调用线程内同步物理关闭当前连接：CoTCP fd 关闭（唤醒挂起的读写等待）、
    //    hiredis reader 释放。返回当刻本对象拥有的物理资源已释放、后端不再访问。
    std::shared_ptr<RedisCotcpConn> conn;
    {
        std::lock_guard<std::mutex> lk(m_conn_mtx);
        conn = m_conn;
    }
    if (conn)
        conn->Close();

    // 4) 落定 Closed（无 owner 在跑即当刻；否则 owner 批次退出时收口）。
    DrainCheckClosed();
}

void CoRedisCotcpCliImpl::SetPreAdmitGateForTest(std::function<void()> gate) {
    m_pre_admit_gate_for_test = std::move(gate);
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
    // Closed 必须对应物理连接收口：封口 + owner 批次退出 + 无已登记 op + 连接
    // 物理已收口（fd 关闭），四者同时成立才 MarkClosed。
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
    s.owner_runs           = m_probe->owner_runs.load();
    s.finish_conflicts     = m_probe->finish_conflicts.load();
    s.skipped_after_finish = m_probe->skipped_after_finish.load();
    s.dial_attempts        = m_probe->dial_attempts.load();
    s.dial_inflight        = m_probe->dial_inflight.load();
    s.conns_broken         = m_probe->conns_broken.load();
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

// 内部装配入口：与 CoRedisCli::Create 同一套参数校验与身份来源，但装配
// CoTCP owner binding 候选（不改变生产默认路径）。
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
    auto impl = std::make_shared<CoRedisCotcpCliImpl>(
        std::move(config), std::move(info).value());
    return result<std::shared_ptr<CoRedisCotcpCliImpl>>::ok(std::move(impl));
}

} // namespace bbt::infra
