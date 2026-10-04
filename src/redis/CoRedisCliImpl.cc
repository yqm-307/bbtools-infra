#include "redis/CoRedisCliImpl.hpp"

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

RedisBindingTotals RedisBindingTotalsForTest() noexcept {
    const auto& c = BindingCounters();
    RedisBindingTotals t;
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

void ResetRedisBindingTotalsForTest() noexcept { ResetBindingCounters(); }

CoRedisCliImpl::~CoRedisCliImpl() {
    // 析构只做兜底：正常路径的连接已在 owner coroutine / Close 中收口。
    Close();
    std::shared_ptr<RedisCotcpConn>       conn;
    std::shared_ptr<detail::CloseWaiters> waiters;
    TakeSlot(&conn, &waiters);
}

// ---------------- 显式连接生命周期 ----------------

void CoRedisCliImpl::SetState(ConnectState want, bool only_if_connecting) noexcept {
    int cur = m_state.load(std::memory_order_acquire);
    for (;;) {
        // Closed 是终态：任何迟到路径都不得覆盖它。
        if (cur == static_cast<int>(ConnectState::Closed))
            return;
        // 建连完成/失败路径只允许从本代际的 Connecting 迁移，避免旧代际结果
        // 改写已被新 Connect/Disconnect 改写过的状态。
        if (only_if_connecting &&
            cur != static_cast<int>(ConnectState::Connecting))
            return;
        if (m_state.compare_exchange_weak(
                cur, static_cast<int>(want), std::memory_order_acq_rel,
                std::memory_order_acquire))
            return;
    }
}

ConnectState CoRedisCliImpl::ConnectStatus() const noexcept {
    // 只读本地状态，不发任何网络探测。
    return static_cast<ConnectState>(m_state.load(std::memory_order_acquire));
}

void CoRedisCliImpl::MarkFailedFromConnected() noexcept {
    int expected = static_cast<int>(ConnectState::Connected);
    m_state.compare_exchange_strong(
        expected, static_cast<int>(ConnectState::Failed),
        std::memory_order_acq_rel, std::memory_order_acquire);
}

std::uint64_t CoRedisCliImpl::PublishSlot(
    const std::shared_ptr<RedisCotcpConn>&       conn,
    const std::shared_ptr<detail::CloseWaiters>& waiters,
    std::shared_ptr<RedisCotcpConn>*             out_old_conn,
    std::shared_ptr<detail::CloseWaiters>*       out_old_waiters) {
    out_old_conn->reset();
    out_old_waiters->reset();
    std::lock_guard<std::mutex> lk(m_conn_mtx);
    // 发布与「Close 是否已开始」在同一临界区内判定：Close 已开始则绝不发布，
    // 避免 Close 返回之后仍有新连接落入槽中。
    if (!m_close.IsOpen())
        return 0;
    *out_old_conn    = std::move(m_slot.conn);
    *out_old_waiters = std::move(m_slot.dial_waiters);
    m_slot.conn        = conn;
    m_slot.dial_waiters = waiters;
    m_slot.gen         = ++m_next_gen;
    return m_slot.gen;
}

bool CoRedisCliImpl::TakeSlotIfGen(
    std::uint64_t gen, std::shared_ptr<RedisCotcpConn>* out_conn,
    std::shared_ptr<detail::CloseWaiters>* out_waiters) {
    std::lock_guard<std::mutex> lk(m_conn_mtx);
    if (m_slot.gen != gen || gen == 0)
        return false;
    *out_conn    = std::move(m_slot.conn);
    *out_waiters = std::move(m_slot.dial_waiters);
    m_slot        = ConnSlot{};
    ++m_next_gen;   // 推进代际：此后旧 dial 的完成路径一律失效
    return true;
}

void CoRedisCliImpl::TakeSlot(std::shared_ptr<RedisCotcpConn>* out_conn,
                              std::shared_ptr<detail::CloseWaiters>* out_waiters) {
    std::lock_guard<std::mutex> lk(m_conn_mtx);
    *out_conn    = std::move(m_slot.conn);
    *out_waiters = std::move(m_slot.dial_waiters);
    m_slot        = ConnSlot{};
    ++m_next_gen;
}

std::shared_ptr<RedisCotcpConn> CoRedisCliImpl::SlotConn() const {
    std::lock_guard<std::mutex> lk(m_conn_mtx);
    return m_slot.conn;
}

void CoRedisCliImpl::SealAndCloseSlot(
    std::shared_ptr<RedisCotcpConn>       conn,
    std::shared_ptr<detail::CloseWaiters> waiters) noexcept {
    // 先封口 dial 唤醒登记（打断在途 connect 并同步收口候选 fd），再物理关闭
    // 连接。两步都同步，返回当刻本代际对象资源已收口。
    if (waiters)
        waiters->SealWakeAndDrainRegistrations();
    if (conn)
        conn->Close();
}

result<void> CoRedisCliImpl::EstablishConn(const CallOptions& options) {
    auto fresh   = std::make_shared<RedisCotcpConn>(m_config.host, m_config.port);
    auto waiters = std::make_shared<detail::CloseWaiters>();
    std::shared_ptr<RedisCotcpConn>       old_conn;
    std::shared_ptr<detail::CloseWaiters> old_waiters;
    const std::uint64_t gen = PublishSlot(fresh, waiters, &old_conn, &old_waiters);
    if (gen == 0) {
        // Close 已开始：不发布、不拨号。
        SetState(ConnectState::Closed, /*only_if_connecting=*/true);
        fresh->Close();
        return result<void>::err(ClosedError());
    }
    // 上一代际收口（正常为空：Disconnect/Close 已摘槽）。
    SealAndCloseSlot(std::move(old_conn), std::move(old_waiters));

    m_probe->dial_attempts.fetch_add(1);
    m_probe->dial_inflight.fetch_add(1);
    auto dial = fresh->Dial(options, waiters);
    m_probe->dial_inflight.fetch_sub(1);

    if (!dial) {
        auto err = std::move(dial).error();
        // 失败清理按代际进行：只有仍是本代际的槽才可摘除/迁移状态。旧代际的
        // 失败不得触碰新 Connect/Disconnect 建立的连接或状态。
        std::shared_ptr<RedisCotcpConn>       stale;
        std::shared_ptr<detail::CloseWaiters> stale_waiters;
        if (TakeSlotIfGen(gen, &stale, &stale_waiters)) {
            SealAndCloseSlot(nullptr, std::move(stale_waiters));
            SetState(ConnectState::Failed, /*only_if_connecting=*/true);
        }
        fresh->Close();
        return result<void>::err(std::move(err));
    }

    // 测试接缝：dial 成功、尚未编码的确定性落点（默认空）。
    if (m_post_dial_gate_for_test)
        m_post_dial_gate_for_test();

    // 成功路径：只有本代际仍是当前槽、且 Close 未开始，才发布 Connected。
    if (!m_close.IsOpen()) {
        std::shared_ptr<RedisCotcpConn>       mine;
        std::shared_ptr<detail::CloseWaiters> my_waiters;
        if (TakeSlotIfGen(gen, &mine, &my_waiters))
            SealAndCloseSlot(std::move(mine), std::move(my_waiters));
        fresh->Close();
        return result<void>::err(ClosedError());
    }
    bool superseded = false;
    {
        std::lock_guard<std::mutex> lk(m_conn_mtx);
        superseded = (m_slot.gen != gen);
    }
    if (superseded) {
        fresh->Close();
        return result<void>::err(MakeError(ErrorCode::TransportError,
            "redis: connect superseded by a newer lifecycle operation"));
    }
    SetState(ConnectState::Connected, /*only_if_connecting=*/true);
    return result<void>::ok();
}

result<void> CoRedisCliImpl::Connect(const CallOptions& options) {
    if (g_bbt_tls_coroutine_co == nullptr)
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
            "redis Connect must run in coroutine context"));
    if (!m_close.IsOpen())
        return result<void>::err(ClosedError());

    // 状态门：已连接=幂等 ok；连接中=Overloaded；终态=Closed；Disconnected/Failed
    // 才允许新建连接。
    for (;;) {
        const int st = m_state.load(std::memory_order_acquire);
        if (st == static_cast<int>(ConnectState::Connected))
            return result<void>::ok();
        if (st == static_cast<int>(ConnectState::Connecting) ||
            st == static_cast<int>(ConnectState::Disconnecting))
            return result<void>::err(MakeError(ErrorCode::Overloaded,
                "redis: connect already in progress"));
        if (st == static_cast<int>(ConnectState::Closed))
            return result<void>::err(ClosedError());
        int expected = st;
        if (m_state.compare_exchange_weak(
                expected, static_cast<int>(ConnectState::Connecting),
                std::memory_order_acq_rel, std::memory_order_acquire))
            break;
    }
    return EstablishConn(options);
}

void CoRedisCliImpl::Disconnect() noexcept {
    if (!m_close.IsOpen())
        return; // Close 终态优先；Disconnect 不再是有效操作
    for (;;) {
        const int st = m_state.load(std::memory_order_acquire);
        if (st == static_cast<int>(ConnectState::Disconnected) ||
            st == static_cast<int>(ConnectState::Closed))
            return; // 未连接 / 已终态：空操作
        int expected = st;
        if (m_state.compare_exchange_weak(
                expected, static_cast<int>(ConnectState::Disconnecting),
                std::memory_order_acq_rel, std::memory_order_acquire))
            break;
    }
    // 1) 摘除当前槽并推进代际：在途 Dial 的完成/失败路径即刻失效。
    std::shared_ptr<RedisCotcpConn>       conn;
    std::shared_ptr<detail::CloseWaiters> waiters;
    TakeSlot(&conn, &waiters);
    // 2) 已登记请求（含排队与在途）同步以 TransportError 落定——先发布终态，使在途
    //    命令的错误确定性地是 TransportError（owner 的迟到终态只被计数、不覆盖）。
    FinishAllRegistered(MakeError(ErrorCode::TransportError,
        "redis: disconnected"));
    // 3) 封口 dial 唤醒登记（打断在途 connect、同步收口候选 fd）+ 物理关闭连接
    //    （fd/reader 同步释放）；返回当刻本代际对象资源已收口。
    SealAndCloseSlot(std::move(conn), std::move(waiters));
    // 4) 进入 Disconnected：配置保留，允许同实例再次显式 Connect。
    SetState(ConnectState::Disconnected, /*only_if_connecting=*/false);
}

result<void> CoRedisCliImpl::PreCheck() const {
    if (g_bbt_tls_coroutine_co == nullptr)
        return result<void>::err(MakeError(ErrorCode::InvalidContext,
            "redis command must run in coroutine context"));
    if (!m_close.IsOpen())
        return result<void>::err(ClosedError());
    const auto st = static_cast<ConnectState>(m_state.load(std::memory_order_acquire));
    switch (st) {
    case ConnectState::Connected:
        return result<void>::ok();
    case ConnectState::Failed:
        // 交 ExecuteOne 裁决：默认 TransportError（sticky），显式重连选项下允许
        // 本条新命令尝试一次新连接。
        return result<void>::ok();
    case ConnectState::Closed:
        return result<void>::err(ClosedError());
    case ConnectState::Disconnected:
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "redis client not connected"));
    case ConnectState::Connecting:
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "redis client is connecting"));
    case ConnectState::Disconnecting:
        return result<void>::err(MakeError(ErrorCode::RuntimeUnavailable,
            "redis client is disconnecting"));
    }
    return result<void>::err(MakeError(ErrorCode::InternalError,
        "redis: unexpected connect state"));
}

result<RawReply> CoRedisCliImpl::Submit(RedisOp::Kind   kind,
                                            std::vector<std::string> args,
                                            const CallOptions&   options) {
    auto pre = PreCheck();
    if (!pre)
        return result<RawReply>::err(std::move(pre).error());

    auto op   = std::make_shared<RedisOp>(m_probe, kind, std::move(args),
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
    auto expected_phase = RedisOp::Phase::kQueued;
    if (op->phase.compare_exchange_strong(
            expected_phase, RedisOp::Phase::kDone,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        op->Finish(result<RawReply>::err(Error(wait_error)));
        RetireOp(op);
    }
    return result<RawReply>::err(std::move(wait_error));
}

void CoRedisCliImpl::AdmitOnWaitRegistered(
    const std::shared_ptr<RedisOp>& op) {
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
    auto raw = Submit(RedisOp::Kind::Get, {"GET", std::string(key)},
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

result<void> CoRedisCliImpl::Set(std::string_view key,
                                     std::string_view value,
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

result<bool> CoRedisCliImpl::Exists(std::string_view        key,
                                        const CallOptions& options) {
    if (key.empty())
        return result<bool>::err(InvalidArg("redis EXISTS: empty key"));
    auto raw = Submit(RedisOp::Kind::Exists, {"EXISTS", std::string(key)},
                      options);
    if (!raw)
        return result<bool>::err(std::move(raw).error());
    if (raw.value().type == RawReply::Type::Integer)
        return result<bool>::ok(raw.value().integer > 0);
    return result<bool>::err(UnexpectedReply("EXISTS"));
}

result<std::uint64_t> CoRedisCliImpl::Delete(
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

// ---------------- owner coroutine（独占执行域）----------------

void CoRedisCliImpl::RunOwnerCoroutine() {
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

void CoRedisCliImpl::CloseAndDropConnOnCoroutine() noexcept {
    std::shared_ptr<RedisCotcpConn>       conn;
    std::shared_ptr<detail::CloseWaiters> waiters;
    TakeSlot(&conn, &waiters);
    SealAndCloseSlot(std::move(conn), std::move(waiters));
}

bool CoRedisCliImpl::ConnPhysicallyClosed() const noexcept {
    std::lock_guard<std::mutex> lk(m_conn_mtx);
    if (!m_slot.conn)
        return true; // 连接对象已释放 ⇒ 无 fd 可留
    return m_slot.conn->CloseRequested() && m_slot.conn->IsClosed();
}

std::shared_ptr<RedisOp> CoRedisCliImpl::TakeNextQueued() {
    for (;;) {
        std::shared_ptr<RedisOp> op;
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
            op->phase.store(RedisOp::Phase::kDone);
            RetireOp(op);
            continue;
        }
        return op;
    }
}

void CoRedisCliImpl::RetireOp(const std::shared_ptr<RedisOp>& op) {
    {
        std::lock_guard<std::mutex> reg(m_reg_mtx);
        m_registered.erase(op);
    }
    DrainCheckClosed();
}

void CoRedisCliImpl::FailConnAndDrainQueue() noexcept {
    // 1) 连接故障线性化：摘除并物理收口当前连接（推进代际，使迟到 Dial 完成不得
    //    改写新状态），标记不可复用。连接对象不再留在槽内。
    m_probe->conns_broken.fetch_add(1);
    std::shared_ptr<RedisCotcpConn>       conn;
    std::shared_ptr<detail::CloseWaiters> waiters;
    TakeSlot(&conn, &waiters);
    if (conn)
        conn->MarkBroken();
    SealAndCloseSlot(std::move(conn), std::move(waiters));
    // 2) 状态进入 Failed：后续新命令默认 TransportError（sticky）；显式开启
    //    reconnect_on_new_command 时，故障之后提交的新命令才可各尝试一次重建。
    //    仅从 Connected 迁移：避免迟到 owner 清理覆盖 Disconnect/Close 已落定状态。
    MarkFailedFromConnected();
    // 3) 排空「故障前已排队」的命令：它们不得跨连接迁移。在 q_mtx 内整体摘下
    //    当前队列，故故障之后新提交（在卡点之后入队）的命令不会被误排空，只有
    //    它们可在 reconnect_on_new_command 下重建连接。此步先于失败命令的
    //    Finish->Notify，避免被唤醒的调用方新提交命令时遭到误排空。
    std::deque<std::shared_ptr<RedisOp>> drained;
    {
        std::lock_guard<std::mutex> lk(m_q_mtx);
        drained.swap(m_queue);
    }
    if (drained.empty())
        return;
    for (auto& op : drained) {
        auto expected = RedisOp::Phase::kQueued;
        if (op->phase.compare_exchange_strong(
                expected, RedisOp::Phase::kDone,
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            op->Finish(result<RawReply>::err(MakeError(
                ErrorCode::TransportError, "redis: connection not reusable")));
        }
        RetireOp(op);   // 已由 Submit(超时/取消) 落定的命令仅做簿记
    }
}

void CoRedisCliImpl::ExecuteOne(const std::shared_ptr<RedisOp>& op) {
    auto expected_phase = RedisOp::Phase::kQueued;
    if (!op->phase.compare_exchange_strong(
            expected_phase, RedisOp::Phase::kWorking,
            std::memory_order_acq_rel, std::memory_order_acquire)) {
        if (expected_phase == RedisOp::Phase::kDone)
            RetireOp(op);
        return;
    }

    auto finish = [&](result<RawReply> r) {
        op->phase.store(RedisOp::Phase::kDone);
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

    // 1) 连接：显式生命周期（不做 Lazy）——命令不得隐式建连。只有「已建立连接被判定不可
    //    复用」（state==Failed）且显式开启 reconnect_on_new_command 时，本条新命令
    //    才允许尝试一次新连接；其余情形一律按状态拒绝/报错。
    std::shared_ptr<RedisCotcpConn> conn = SlotConn();
    if (conn && conn->Broken()) {
        // 坏连接若仍在槽内（正常已被 FailConnAndDrainQueue 摘除）：一律不复用。
        finish(result<RawReply>::err(MakeError(
            ErrorCode::TransportError, "redis: connection not reusable")));
        return;
    }
    if (!conn) {
        const auto st =
            static_cast<ConnectState>(m_state.load(std::memory_order_acquire));
        if (!m_config.reconnect_on_new_command || st != ConnectState::Failed) {
            // 首次未 Connect / 主动 Disconnect 之后 / 关闭：不隐式建连。
            finish(result<RawReply>::err(MakeError(
                st == ConnectState::Closed ? ErrorCode::Closed
                                           : ErrorCode::TransportError,
                st == ConnectState::Closed ? "redis client closed"
                                           : "redis: not connected")));
            return;
        }
        // 显式开启重连：仅为本条故障之后的新命令尝试一次新连接（状态 CAS
        // Failed→Connecting，保证与显式 Connect 互斥，不并发拨号）。
        int expected = static_cast<int>(ConnectState::Failed);
        if (!m_state.compare_exchange_strong(
                expected, static_cast<int>(ConnectState::Connecting),
                std::memory_order_acq_rel, std::memory_order_acquire)) {
            finish(result<RawReply>::err(MakeError(
                ErrorCode::TransportError, "redis: not connected")));
            return;
        }
        auto dial = EstablishConn(op->options);
        if (!dial) {
            auto dial_error = std::move(dial).error();
            FailConnAndDrainQueue();
            finish(result<RawReply>::err(std::move(dial_error)));
            return;
        }
        conn = SlotConn();
        if (!conn) {
            finish(result<RawReply>::err(MakeError(
                ErrorCode::TransportError, "redis: connection unavailable")));
            return;
        }
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
        // 连接故障（除「未写任何字节的超时」保留连接外）先线性化并排空故障前
        // 排队命令，再落定本条失败命令。
        if (!preserve_conn)
            FailConnAndDrainQueue();
        finish(result<RawReply>::err(Error(write_error)));
        return;
    }

    // 4) 读一条完整 reply（CoTCP 等待与 hiredis 同步解析交替）。
    auto r = conn->ReadReply(op->options);
    if (!r) {
        auto read_error = std::move(r).error();
        FailConnAndDrainQueue();
        finish(result<RawReply>::err(std::move(read_error)));
        return;
    }
    finish(std::move(r));
}

// ---------------- 关闭链路 ----------------

void CoRedisCliImpl::Close() noexcept {
    if (!m_close.BeginClose()) {
        // 并发/重复 Close：非首次调用者等待首个调用者把物理收口落定为 Closed，
        // 使每个合法调用者返回当刻都观察到同一终态（fd/reader 已释放、
        // registered 归零、IsClosed() 为真）。首个调用者同步完成收口，有界。
        m_close.WaitClosed();
        return;
    }
    m_state.store(static_cast<int>(ConnectState::Closed));

    // 1) 封口：此后新命令一律 Closed，且不再启动新 owner 批次。
    {
        std::lock_guard<std::mutex> lk(m_q_mtx);
        m_io_dead = true;
    }
    // 2) 已登记 op 统一收口（CAS：与 owner coroutine 的落定竞争只发布一次终态），
    //    唤醒各自挂起的业务协程；并就地摘除登记——Close 返回当刻 registered 即
    //    归零，不再依赖 owner 批次退出。owner 出队已落定 op 时只做簿记
    //    （skipped_after_finish），不会把已关闭请求发往后端。
    std::vector<std::shared_ptr<RedisOp>> snapshot;
    {
        std::lock_guard<std::mutex> reg(m_reg_mtx);
        snapshot.assign(m_registered.begin(), m_registered.end());
        m_registered.clear();
    }
    for (auto& op : snapshot)
        op->Finish(result<RawReply>::err(ClosedError()));

    // 3) 摘除当前槽（推进代际）：封口 dial 唤醒登记打断在途 connect（挂起的
    //    connect 等待段随即以 Closed 返回并自行释放内部 fd），并在本调用线程内
    //    同步物理关闭当前连接——CoTCP fd 关闭（唤醒挂起的读写等待）、hiredis
    //    reader 释放。返回当刻本对象拥有的物理资源已释放、后端不再访问。
    std::shared_ptr<RedisCotcpConn>       conn;
    std::shared_ptr<detail::CloseWaiters> waiters;
    TakeSlot(&conn, &waiters);
    SealAndCloseSlot(std::move(conn), std::move(waiters));

    // 4) 落定 Closed：物理资源已在当刻收口，不等待 owner 批次退出。
    DrainCheckClosed();
}

void CoRedisCliImpl::SetPreAdmitGateForTest(std::function<void()> gate) {
    m_pre_admit_gate_for_test = std::move(gate);
}

bool CoRedisCliImpl::IsIoDead() const {
    std::lock_guard<std::mutex> lk(m_q_mtx);
    return m_io_dead;
}

void CoRedisCliImpl::FinishAllRegistered(const Error& err) noexcept {
    std::vector<std::shared_ptr<RedisOp>> snapshot;
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

void CoRedisCliImpl::DrainCheckClosed() noexcept {
    // Closed = 后端不会再访问本组件拥有的操作资源：封口 + 无已登记 op + 连接
    // 物理已收口（fd 关闭 / 对象已释放）。owner 批次是否退出不是物理收口谓词：
    // 连接关闭、登记清空后，owner 只能看到 Closed/已落定 op，不再触碰后端。
    // 因此 Close() 返回当刻即可落定 Closed（同步）。
    bool fin = IsIoDead() && ConnPhysicallyClosed();
    if (fin) {
        std::lock_guard<std::mutex> reg(m_reg_mtx);
        fin = m_registered.empty();
    }
    if (fin)
        m_close.MarkClosed();
}

// ---------------- 探针 ----------------

RedisBindingSnapshot CoRedisCliImpl::ProbeSnapshot() const {
    RedisBindingSnapshot s;
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
        s.has_conn    = static_cast<bool>(m_slot.conn);
        s.conn_closed = !m_slot.conn ||
                        (m_slot.conn->CloseRequested() && m_slot.conn->IsClosed());
    }
    return s;
}

void CoRedisCliImpl::SetPostDialGateForTest(std::function<void()> gate) {
    m_post_dial_gate_for_test = std::move(gate);
}

std::shared_ptr<tcp::CoTCP> CoRedisCliImpl::TransportForTest() const {
    std::lock_guard<std::mutex> lk(m_conn_mtx);
    if (!m_slot.conn)
        return nullptr;
    return m_slot.conn->Transport();
}

} // namespace redis_detail

// 正式装配入口：公共 CoRedisCli 只装配这一种具体实现；CoTCP 保持在
// CoRedisCliImpl 内部，不通过第二个 client 工厂暴露。
result<std::shared_ptr<CoRedisCli>> CoRedisCli::Create(RedisClientConfig config) {
    auto valid = ValidateRedisClientConfig(config);
    if (!valid)
        return result<std::shared_ptr<CoRedisCli>>::err(
            std::move(valid).error());
    auto info = detail::NewObjectInfo("infra.redis_cli");
    if (!info)
        return result<std::shared_ptr<CoRedisCli>>::err(
            std::move(info).error());
    auto impl = std::make_shared<redis_detail::CoRedisCliImpl>(
        std::move(config), std::move(info).value());
    return result<std::shared_ptr<CoRedisCli>>::ok(std::move(impl));
}

} // namespace bbt::infra
