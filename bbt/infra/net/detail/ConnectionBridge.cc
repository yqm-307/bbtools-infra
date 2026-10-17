#include <bbt/infra/net/detail/ConnectionBridge.hpp>

#include <new>
#include <stdexcept>
#include <utility>

namespace bbt::infra::net {

// ============================================================================
// ConnectionSession
// ============================================================================

ConnectionSession::ConnectionSession(std::shared_ptr<ConnectionBridgeState> state,
    std::shared_ptr<ICoConnectionHandler> handler, Address peer, std::size_t id)
    : state_(std::move(state)),
      handler_(std::move(handler)),
      peer_(std::move(peer)),
      id_(id),
      identity_("net.co_tcp_connection", "connection-" + std::to_string(id)) {}

void ConnectionSession::BindShell(const std::shared_ptr<CoTcpConnection>& shell) { shell_ = shell; }

bbt::coroutine::CoObjectInfo ConnectionSession::ObjectInfo() const { return identity_.GetObjectInfo(); }

bbt::coroutine::WaitOptions ConnectionSession::WaitOpts(CoDeadline deadline)
{
    bbt::coroutine::WaitOptions options{};
    options.deadline = deadline;
    return options;
}

std::error_code ConnectionSession::MapSubmitError(SubmitStatus status)
{
    switch (status) {
    case SubmitStatus::Full:
        return std::make_error_code(std::errc::resource_unavailable_try_again);
    case SubmitStatus::Sealed:
    case SubmitStatus::NotStarted:
        return std::make_error_code(std::errc::operation_canceled);
    case SubmitStatus::Invalid:
        return std::make_error_code(std::errc::invalid_argument);
    case SubmitStatus::Failed:
    case SubmitStatus::NotSubmitted:
        break;
    }
    return std::make_error_code(std::errc::io_error);
}

// ---- 工厂接纳（owner 域）--------------------------------------------------

void ConnectionSession::AdoptOwner(std::shared_ptr<StreamConnection> connection)
{
    conn_ = std::move(connection); // owner 域绑定；回调此后才可能触发
    bool spawn = false;
    {
        std::lock_guard<std::mutex> guard(mx_);
        pending_open_ = true;
        if (!busy_) {
            busy_ = true;
            spawn = true;
        }
    }
    state_->NotifyChange();
    if (spawn) {
        SpawnDispatcher();
    }
}

StreamConnection::Callbacks ConnectionSession::MakeCallbacks()
{
    StreamConnection::Callbacks callbacks;
    std::weak_ptr<ConnectionSession> weak = shared_from_this(); // owner 通知侧 weak，过期可忽略
    callbacks.on_traffic = [weak](StreamConnection&) {
        if (auto session = weak.lock()) { session->OnTrafficOwner(); }
    };
    callbacks.on_drained = [weak](StreamConnection&) {
        if (auto session = weak.lock()) { session->OnDrainedOwner(); }
    };
    callbacks.on_closed = [weak](StreamConnection&, const std::error_code& reason) {
        if (auto session = weak.lock()) { session->OnClosedOwner(reason); }
    };
    return callbacks;
}

// ---- owner 域短通知 -------------------------------------------------------

void ConnectionSession::OnTrafficOwner()
{
    bool spawn = false;
    {
        std::lock_guard<std::mutex> guard(mx_);
        if (sealed_) {
            ++late_dropped_; // 封口后 readiness 忽略（计数）；字节留在物理积压，随物理收口清理
            return;
        }
        // owner 域只「记录」读终态：干净 FIN 或致命 IO（RST/errno）。绝不在此
        // 收口——业务事件尚未派发，物理关闭会丢尾帧/未发响应。
        if (!eof_terminal_ && conn_) {
            if (const auto io_error = conn_->LastIoError()) {
                eof_terminal_ = true;
                eof_reason_ = io_error;   // 真实 errno（system_category，如 ECONNRESET）
            } else if (conn_->ReadEof()) {
                eof_terminal_ = true;
                eof_reason_.clear();      // 干净 FIN → 空 reason
            }
            if (eof_terminal_) { ++peer_terminal_detected_; }
        }
        pending_traffic_ = true;
        if (!busy_) {
            busy_ = true;
            spawn = true;
        }
    }
    if (spawn) {
        SpawnDispatcher();
    }
}

void ConnectionSession::OnDrainedOwner()
{
    state_->NotifyChange();
}

void ConnectionSession::OnClosedOwner(const std::error_code& reason)
{
    // owner 域；conn_->Close 的同步路径重入安全（调用方未持有 mx_）。
    std::vector<std::shared_ptr<CoIoOperation>> waiters;
    bool spawn = false;
    {
        std::lock_guard<std::mutex> guard(mx_);
        physical_closed_ = true;
        if (!sealed_) {
            sealed_ = true;
            sealed_reason_ = reason;
        }
        waiters = std::move(physical_waiters_); // G6：owned state 持有到最后回调
        if (!service_stopped_ && !close_event_pending_) {
            close_event_pending_ = true;
            if (!busy_) {
                busy_ = true;
                spawn = true;
            }
        }
    }
    std::exception_ptr failure;
    for (auto& operation : waiters) {
        try {
            (void)operation->Complete({1, {}}); // Complete 首胜，重试不会重复完成。
        } catch (...) {
            if (!failure) { failure = std::current_exception(); }
        }
    }
    if (failure) {
        // 不因单个通知失败丢掉其余 waiter；保留 owned 记录供再次推进。
        std::lock_guard<std::mutex> guard(mx_);
        physical_waiters_.swap(waiters);
    }
    state_->NotifyChange();
    if (spawn) {
        SpawnDispatcher();
    }
    if (failure) { std::rethrow_exception(failure); }
}

std::error_code ConnectionSession::SealAndPhysicalCloseOwner(std::error_code reason)
{
    bool start = false;
    {
        std::lock_guard<std::mutex> guard(mx_);
        if (!sealed_) {
            sealed_ = true;
            sealed_reason_ = reason;
        }
        if (!close_started_ && !physical_closed_) {
            close_started_ = true;
            start = true;
        }
    }
    std::error_code close_error;
    try {
        if (start && conn_) {
            // Close 已完成而通知抛出时只重放完成发布，不再物理 close。
            if (conn_->ConnectionState() == StreamConnection::State::Closed) {
                OnClosedOwner(reason);
            } else {
                close_error = conn_->Close(reason);
            }
        } else if (physical_closed_ && !physical_waiters_.empty()) {
            OnClosedOwner(reason);
        }
    } catch (...) {
        std::lock_guard<std::mutex> guard(mx_);
        close_started_ = false; // 依赖在物理 close 前抛出时，下一轮仍能推进。
        throw;
    }
    state_->NotifyChange();
    return close_error;
}

// ---- 业务命令（受管协程）--------------------------------------------------

std::error_code ConnectionSession::StopServiceOwner()
{
    {
        std::lock_guard<std::mutex> guard(mx_);
        service_stopped_ = true;
        pending_open_ = false;
        pending_traffic_ = false;
        pending_wake_ = false;
        close_event_pending_ = false;
    }
    return SealAndPhysicalCloseOwner(std::make_error_code(std::errc::operation_canceled));
}

std::error_code ConnectionSession::ReleaseOwnerBackend()
{
    // 脱离专用（owner 已 join、非 owner 域）：把 owner 域 backend 的强引用
    // 移到局部，短锁外析构，使 socket/EventLoop 依赖在 context 仍活时真正
    // 释放；未物理关闭时拒绝（不得拆掉活 socket 或丢 waiter）。
    std::shared_ptr<StreamConnection> backend;
    {
        std::lock_guard<std::mutex> guard(mx_);
        if (!physical_closed_) { return std::make_error_code(std::errc::operation_in_progress); }
        backend = std::move(conn_);
    }
    backend.reset(); // ~StreamConnection（socket_ RAII）在短锁外执行
    return {};
}

std::weak_ptr<StreamConnection> ConnectionSession::BackendProbe() const
{
    std::lock_guard<std::mutex> guard(mx_);
    return conn_;
}

std::error_code ConnectionSession::Wake()
{
    {
        std::lock_guard<std::mutex> bridge_guard(state_->m_);
        std::lock_guard<std::mutex> guard(mx_);
        if (state_->service_stopped_ || sealed_) {
            ++wake_rejected_sealed_; // G3：封口后明确拒绝，不静默丢弃
            return std::make_error_code(std::errc::operation_canceled);
        }
    }
    ++wake_calls_;
    auto self = shared_from_this(); // 命令随强引用保活（G3）
    const auto status = SubmitOperation([self] { self->WakeArriveOwner(); });
    if (status != SubmitStatus::Accepted) {
        return MapSubmitError(status);
    }
    return {};
}

void ConnectionSession::WakeArriveOwner()
{
    bool spawn = false;
    {
        std::lock_guard<std::mutex> guard(mx_);
        if (sealed_) {
            ++late_dropped_; // 与并发 Close 竞争：命令已接纳但事件被合并丢弃（计数可观测）
        } else {
            pending_wake_ = true; // Wake 合并：已排队不重复建事件
            if (!busy_) {
                busy_ = true;
                spawn = true;
            }
        }
    }
    if (spawn) {
        SpawnDispatcher();
    }
}

ConnectionSession::OperationTicket::OperationTicket(std::shared_ptr<ConnectionSession> value)
    : session(std::move(value)) { session->state_->OpSubmitted(); }

ConnectionSession::OperationTicket::~OperationTicket() { session->state_->OpExecuted(); }

SubmitStatus ConnectionSession::SubmitOperation(std::function<void()> command, SubmitGate gate)
{
    auto ticket = std::make_shared<OperationTicket>(shared_from_this());
    return state_->Submit([ticket, command = std::move(command)] { command(); }, gate);
}

CoIoResult ConnectionSession::RunOperation(CoDeadline deadline, CoIoOperation::Start start,
    SubmitGate gate)
{
    // caller 与已接纳命令共享账票：登记/投递前抛出、start 抛出、迟到执行
    // 都在最后持有者退出后恰一次结算，不把 Complete 当作命令已退出。
    // 窄提交变体：门只覆盖投递（同一门内取有效 owner 并入队），不跨挂起等待；
    // 脱离后 Sealed → 命令以 operation_canceled 收口，不再触死 owner。
    auto ticket = std::make_shared<OperationTicket>(shared_from_this());
    auto state = state_;
    return CoIoOperation::Run(
        [state, gate](std::function<void()> command) { return state->Submit(std::move(command), gate); },
        WaitOpts(deadline),
        [ticket, start = std::move(start)](std::shared_ptr<CoIoOperation> operation) {
            start(std::move(operation));
        });
}

std::error_code ConnectionSession::Write(std::string data, CoDeadline deadline)
{
    {
        std::lock_guard<std::mutex> guard(state_->m_);
        if (state_->service_stopped_) {
            return std::make_error_code(std::errc::operation_canceled);
        }
    }
    // 取得不可变字符串所有权；异步执行必须转移，不保存调用栈 view。
    auto payload = std::make_shared<std::string>(std::move(data));
    auto error_holder = std::make_shared<std::error_code>();
    auto self = shared_from_this();
    const auto result = RunOperation(deadline,
        [self, payload, error_holder](std::shared_ptr<CoIoOperation> operation) {
            // 入队不等于开始；与 SealService 同短锁取得不可撤销授权。
            // 锁外调用 backend/Complete，禁止跨通知或用户回调持锁。
            bool authorized;
            {
                std::lock_guard<std::mutex> guard(self->state_->m_);
                authorized = !self->state_->service_stopped_;
            }
            if (!authorized) {
                (void)operation->Complete({0, std::make_error_code(std::errc::operation_canceled)});
                return;
            }
            const auto bytes = payload->size();
            std::error_code error;
            if (self->conn_) {
                error = self->conn_->Write(std::move(*payload)); // 成功=接纳，非内核全发
            } else {
                error = std::make_error_code(std::errc::operation_canceled);
            }
            *error_holder = error;
            (void)operation->Complete({bytes, error});
        });
    if (result.wait_status != bbt::coroutine::WaitStatus::Completed) {
        if (result.submit_status == SubmitStatus::Accepted) { ++op_timeouts_; }
        return std::make_error_code(std::errc::timed_out);
    }
    // 提交失败（Full/Sealed 等）时 Run 以 Complete({0, error}) 收口：明确返回
    // 该错误，不把未入队命令当成接纳成功。
    return result.io.error ? result.io.error : *error_holder;
}

std::error_code ConnectionSession::Close(CloseMode mode, std::error_code reason, CoDeadline deadline)
{
    return mode == CloseMode::Block ? CloseBlock(std::move(reason), deadline)
                                    : CloseUnblock(std::move(reason));
}

std::error_code ConnectionSession::CloseBlock(std::error_code reason, CoDeadline deadline)
{
    {
        std::lock_guard<std::mutex> guard(mx_);
        if (physical_closed_) {
            return {}; // 幂等物理观察：OnClose 内重复 Block 不自等
        }
    }
    auto self = shared_from_this();
    auto error_holder = std::make_shared<std::error_code>();
    const auto result = RunOperation(deadline,
        [self, error_holder, reason](std::shared_ptr<CoIoOperation> operation) {
            bool already = false;
            {
                std::lock_guard<std::mutex> guard(self->mx_);
                if (self->physical_closed_) {
                    already = true;
                } else {
                    self->physical_waiters_.push_back(operation); // G6：owned 持有到最后回调
                }
            }
            if (already) {
                (void)operation->Complete({1, {}});
            } else {
                // 封口 → 撤销/最后内核访问/关 fd → on_closed 完成等待者。
                self->SealAndPhysicalCloseOwner(reason);
            }
        });
    if (result.wait_status != bbt::coroutine::WaitStatus::Completed) {
        if (result.submit_status == SubmitStatus::Accepted) { ++op_timeouts_; }
        return std::make_error_code(std::errc::timed_out);
    }
    return result.io.error ? result.io.error : *error_holder;
}

std::error_code ConnectionSession::CloseUnblock(std::error_code reason)
{
    {
        std::lock_guard<std::mutex> guard(mx_);
        if (physical_closed_ || sealed_) {
            return {}; // 明确幂等：封口/关闭首胜已发生
        }
        sealed_ = true; // 先封口：返回时已封口（短锁，不跨等待）
        sealed_reason_ = reason;
    }
    auto self = shared_from_this();
    SubmitStatus status = SubmitStatus::Failed;
    try {
        status = SubmitOperation([self, reason] { self->SealAndPhysicalCloseOwner(reason); });
    } catch (...) {
        // 闭包/账票的构造发生在 Submit 内部保护之外，也必须撤销尚未接纳的封口。
        status = SubmitStatus::Failed;
    }
    if (status != SubmitStatus::Accepted) {
        bool rolled_back = false;
        {
            std::lock_guard<std::mutex> guard(self->mx_);
            if (!self->close_started_ && !self->physical_closed_) {
                self->sealed_ = false; // 提交失败保持原状态，无假 Closing
                self->sealed_reason_.clear();
                rolled_back = true;
            }
        }
        (void)rolled_back;
        return MapSubmitError(status);
    }
    return {}; // 已封口且命令可靠接纳
}

// ---- dispatcher：单连接串行事件位（active_peak==1）------------------------

void ConnectionSession::SpawnDispatcher()
{
    auto self = shared_from_this();
    auto shell = shell_.lock();
    if (!shell) {
        // registry 强持有连接对象；理论不可达。兜底复位，不留 busy 滞留。
        {
            std::lock_guard<std::mutex> guard(mx_);
            busy_ = false;
            ++dispatch_crashes_;
        }
        state_->NotifyChange();
        return;
    }
    // 所有调用者均在 owner 域，且已置 busy_。登记失败保证没有任务入队。
    // ponytail: 仅追加一次关闭事件登记；持续拒绝保留业务未完成 HOLD，
    // 不循环重试。若需跨运行时恢复，由后续生命周期策略负责。
    for (int attempt = 0; attempt < 2; ++attempt) {
        ++spawns_;
        try {
            const std::size_t stack = g_bbt_coroutine_config->m_cfg_stack_size;
            g_scheduler->RegistCoroutineTask(stack, [self, shell] {
                try {
                    self->DispatchLoop(shell);
                } catch (...) {
                    self->OnDispatchCrash(); // 已运行 dispatcher 异常，区别于登记失败。
                }
            }, "co.bridge.dispatch");
            return;
        } catch (...) {
            ++dispatch_crashes_;
            {
                std::lock_guard<std::mutex> guard(mx_);
                // 尚无执行者：封口后不再派发失效 Traffic/Wake，保留 Open→Close。
                pending_traffic_ = false;
                pending_wake_ = false;
            }
            // 保持 busy_ 跨同步 Close：OnClosedOwner 可重入，但不会递归登记。
            // 已在 owner，无需再 Submit（队列拒绝也不应阻断物理收口）。
            SealAndPhysicalCloseOwner(std::make_error_code(std::errc::operation_canceled));
        }
    }
    {
        std::lock_guard<std::mutex> guard(mx_);
        busy_ = false; // 两次均未入队；终态事件位和 close_finished_ 保持真实未完成。
    }
    state_->NotifyChange();
}

void ConnectionSession::DispatchLoop(const std::shared_ptr<CoTcpConnection>& shell)
{
    for (;;) {
        EventKind kind = EventKind::Open;
        bool wake = false;
        std::error_code reason;
        bool have = false;
        {
            std::lock_guard<std::mutex> guard(mx_);
            // 优先级：Open → Traffic/Wake → Close 终态；close 位 sticky，
            // 不能被普通事件挤掉。封口后无新事件位。
            if (service_stopped_) {
                busy_ = false;
                return;
            }
            if (pending_open_) {
                kind = EventKind::Open;
                pending_open_ = false;
                open_dispatched_ = true;
                have = true;
            } else if (pending_traffic_ || pending_wake_) {
                kind = EventKind::Traffic;
                wake = pending_wake_;
                pending_traffic_ = false;
                pending_wake_ = false;
                have = true;
            } else if (close_event_pending_) {
                kind = EventKind::Close;
                close_event_pending_ = false;
                reason = sealed_reason_;
                have = true;
            }
            if (!have) {
                busy_ = false; // 入队与判空在同一临界区，无丢唤醒窗口
                return;
            }
        }
        RunEvent(shell, kind, wake, reason);
        // 业务事件（含快照归还）已结束后才提交读终态收口：Open →
        // Traffic(尾帧先派发) → [peer terminal 收口] → Close。事件优先级
        // 保证 Close 恒在最后一个 Traffic 返回之后。
        CloseOnPeerTerminal();
    }
}

void ConnectionSession::RunEvent(const std::shared_ptr<CoTcpConnection>& shell,
    EventKind kind, bool wake, const std::error_code& reason)
{
    bool stopped;
    {
        // 与桥强停发布共用门；锁顺序与 DrainedLocked 一致。
        std::lock_guard<std::mutex> bridge_guard(state_->m_);
        std::lock_guard<std::mutex> guard(mx_);
        stopped = state_->service_stopped_;
        if (!stopped) {
            event_running_ = true;
            event_coro_ = bbt::coroutine::GetLocalCoroutineId();
        }
    }
    if (stopped) { return; }
    const int current = ++active_;
    int peak = active_peak_.load();
    while (current > peak && !active_peak_.compare_exchange_weak(peak, current)) {}

    const bool close_kind = (kind == EventKind::Close);

    try {
        if (kind == EventKind::Open) {
            ++open_entered_;
            handler_->OnOpen(*shell);
        } else if (kind == EventKind::Traffic) {
            PrepareEventSnapshot(wake);
            // Take 可能挂起；在业务调用前再次通过强停门。归还命令不封禁。
            {
                std::lock_guard<std::mutex> guard(state_->m_);
                stopped = state_->service_stopped_;
            }
            if (!stopped) {
                ++traffic_dispatched_;
                handler_->OnTraffic(*shell, wake);
            }
        } else {
            ++onclose_entered_;
            handler_->OnClose(*shell, reason);
        }
    } catch (...) {
        ++handler_exceptions_; // 可观测
        if (!close_kind) {
            // 业务异常不得无声留在 Open：封口 + 物理关闭；OnClose 仍串行执行。
            auto self = shared_from_this();
            if (state_->Submit([self] {
                    self->SealAndPhysicalCloseOwner(std::make_error_code(std::errc::operation_canceled));
                }, SubmitGate::Internal) != SubmitStatus::Accepted) {
                ++close_init_submit_failures_; // 极端背压下的可见缺口（测试不在此路径塞满队列）
            }
        }
    }
    // 资源归还失败不能跳过事件自身结算；资源槽位仍由 drain 单独把关。
    std::exception_ptr cleanup_error;
    try { AutoReturnSnapshot(); }
    catch (...) {
        ++snapshot_return_errors_;
        cleanup_error = std::current_exception();
    }
    if (close_kind) {
        ++onclose_returned_;
    }
    {
        std::lock_guard<std::mutex> guard(mx_);
        event_running_ = false;
        event_coro_ = 0;
        if (close_kind) {
            close_finished_ = true;
        }
    }
    --active_;
    state_->NotifyChange();
    if (cleanup_error) { std::rethrow_exception(cleanup_error); }
}

void ConnectionSession::ReturnAbandonedSnapshotOwner(const std::shared_ptr<SnapshotTransfer>& transfer)
{
    // transfer 已被业务放弃，不再有读者；只由 owner 操作其中的快照。
    {
        std::lock_guard<std::mutex> guard(mx_);
        if (pending_take_ != transfer) { return; }
    }
    std::error_code error;
    if (transfer->lent) {
        try { error = conn_->ReturnSnapshot(std::move(transfer->snapshot), 0); }
        catch (...) { ++snapshot_return_errors_; throw; }
    }
    if (error) {
        ++snapshot_return_errors_;
        state_->NotifyChange();
        return; // 保留 owned 槽位和未归还状态；绝不能以清标志伪装 drain。
    }
    {
        std::lock_guard<std::mutex> guard(mx_);
        if (pending_take_ == transfer) { pending_take_.reset(); }
    }
    // 未消费的字节还在物理积压，或根本未被借出；重新派发而不是等新数据。
    OnTrafficOwner();
    state_->NotifyChange();
}

void ConnectionSession::PrepareEventSnapshot(bool wake)
{
    if (wake) { return; }
    auto transfer = std::make_shared<SnapshotTransfer>();
    auto self = shared_from_this();
    {
        std::lock_guard<std::mutex> guard(mx_);
        if (pending_take_ || snap_return_pending_) { return; }
        pending_take_ = transfer;
    }
    CoIoResult result;
    try {
        result = RunOperation(std::chrono::steady_clock::now() + kInternalOpDeadline,
            [self, transfer](std::shared_ptr<CoIoOperation> operation) {
                bool abandoned;
                {
                    std::lock_guard<std::mutex> guard(self->mx_);
                    abandoned = transfer->abandoned;
                }
                std::error_code error;
                if (!abandoned && self->conn_) {
                    try {
                        transfer->snapshot = self->conn_->TakeSnapshot(error);
                        transfer->lent = !error;
                    } catch (...) {
                        std::lock_guard<std::mutex> guard(self->mx_);
                        transfer->ready = true;
                        throw;
                    }
                }
                if (error == std::errc::resource_unavailable_try_again
                    || error == std::errc::operation_not_supported) { error.clear(); }
                {
                    std::lock_guard<std::mutex> guard(self->mx_);
                    transfer->ready = true; // 同锁发布 snapshot/lent，业务只能在此后认领。
                    abandoned = transfer->abandoned;
                    if (abandoned) { transfer->cleanup_claimed = true; }
                }
                if (abandoned) { self->ReturnAbandonedSnapshotOwner(transfer); }
                (void)operation->Complete({1, error});
            }, SubmitGate::Internal);
    } catch (...) {
        // 登记/闭包构造抛出也走同一放弃协议，禁止留下无主交付槽位。
        bool ready;
        {
            std::lock_guard<std::mutex> guard(mx_);
            transfer->abandoned = true;
            ready = transfer->ready && !transfer->cleanup_claimed;
            if (ready) { transfer->cleanup_claimed = true; }
            // 即便登记抛出，保留槽位直至 owner 确认：已接纳状态不能靠猜测清空。
            if (!ready && !transfer->ready) { ready = true; transfer->cleanup_claimed = true; }
        }
        if (ready) {
            if (SubmitOperation([self, transfer] {
                    self->ReturnAbandonedSnapshotOwner(transfer);
                }, SubmitGate::Internal) != SubmitStatus::Accepted) { ++snapshot_return_errors_; }
        }
        throw;
    }
    bool cleanup = false;
    {
        std::lock_guard<std::mutex> guard(mx_);
        if (result.wait_status == bbt::coroutine::WaitStatus::Completed
            && result.submit_status == SubmitStatus::Accepted && !result.io.error) {
            if (transfer->lent) {
                snap_ = std::move(transfer->snapshot);
                snap_taken_ = snap_.Remaining();
                snap_lent_ = true;
            }
            pending_take_.reset(); // 业务认领；不是用 Complete()==true 猜测认领。
        } else {
            transfer->abandoned = true;
            cleanup = transfer->ready && !transfer->cleanup_claimed;
            if (cleanup) { transfer->cleanup_claimed = true; }
            if (result.submit_status != SubmitStatus::Accepted) { pending_take_.reset(); }
            else if (result.wait_status != bbt::coroutine::WaitStatus::Completed) { ++op_timeouts_; }
        }
    }
    if (cleanup) {
        // owner 已取走数据、但业务 deadline 先胜：同一 owned 槽位保留到归还结束。
        if (SubmitOperation([self, transfer] {
                self->ReturnAbandonedSnapshotOwner(transfer);
            }, SubmitGate::Internal) != SubmitStatus::Accepted) { ++snapshot_return_errors_; }
    }
    if (result.exception) { std::rethrow_exception(result.exception); }
}

void ConnectionSession::AutoReturnSnapshot()
{
    // 无快照的事件收尾不分配；确需归还时分配仍早于资源转移。
    // owner 已脱离：无 owner 可归还，已物理关闭的借出快照与未确认的归还槽位
    // 在短锁内转移、锁外析构（不触已死 owner、不借调用栈、不在锁下跑析构）；
    // 正常未脱离路径语义完整保留。
    if (state_->detached_.load()) {
        EventSnapshot doomed_snapshot;
        std::shared_ptr<EventSnapshot> doomed_return;
        {
            std::lock_guard<std::mutex> guard(mx_);
            if (!snap_lent_ && !snap_return_pending_) { return; }
            doomed_snapshot = std::move(snap_);
            doomed_return = std::move(pending_return_);
            snap_lent_ = false;
            snap_return_pending_ = false;
        }
        state_->NotifyChange();
        return;
    }
    std::shared_ptr<EventSnapshot> holder;
    std::size_t consumed = 0;
    {
        std::lock_guard<std::mutex> guard(mx_);
        if (!snap_lent_ || snap_return_pending_) { return; }
        holder = std::make_shared<EventSnapshot>();
        *holder = std::move(snap_);
        consumed = snap_taken_ - holder->Remaining();
        pending_return_ = holder;
        snap_lent_ = false;
        snap_return_pending_ = true;
    }
    auto self = shared_from_this();
    const auto result = RunOperation(std::chrono::steady_clock::now() + kInternalOpDeadline,
        [self, holder, consumed](std::shared_ptr<CoIoOperation> operation) {
            std::error_code error;
            try {
                error = self->conn_
                    ? self->conn_->ReturnSnapshot(std::move(*holder), consumed)
                    : std::make_error_code(std::errc::operation_canceled);
            } catch (...) { ++self->snapshot_return_errors_; throw; }
            if (error) {
                ++self->snapshot_return_errors_;
            } else {
                std::lock_guard<std::mutex> guard(self->mx_);
                self->pending_return_.reset();
                self->snap_return_pending_ = false;
            }
            (void)operation->Complete({consumed, error});
        }, SubmitGate::Internal);
    if (result.submit_status != SubmitStatus::Accepted) {
        // Sealed 表示命令未被接纳：owner 已封口但尚未 Detach 也在此列。
        // 仅物理已关闭时可以本地丢弃；不再有读泵需要恢复，也没有迟到归还
        // 命令会使用 holder。Full/Failed 等真实失败仍保留资源与错误信号。
        std::shared_ptr<EventSnapshot> doomed_return;
        bool locally_released = false;
        {
            std::lock_guard<std::mutex> guard(mx_);
            if (result.submit_status == SubmitStatus::Sealed && physical_closed_) {
                if (pending_return_ == holder) {
                    doomed_return = std::move(pending_return_);
                    snap_return_pending_ = false;
                }
                locally_released = true;
            }
        }
        if (!locally_released) { ++snapshot_return_errors_; }
    } else if (result.wait_status != bbt::coroutine::WaitStatus::Completed) {
        ++op_timeouts_; // 已接纳命令仍持有 holder，迟到归还后才清槽位。
    }
}

void ConnectionSession::OnDispatchCrash()
{
    // dispatcher 本身异常：观测 + busy 复位（不允许 busy=true 永久无人运行）
    // + 物理收口提交。保留未归还快照及 pending 槽位；物理关闭不等于归还，
    // 禁止重置资源标志制造假 drain。事件自身已在 RunEvent 中结算。
    ++dispatch_crashes_;
    {
        std::lock_guard<std::mutex> guard(mx_);
        busy_ = false;
        event_running_ = false;
        event_coro_ = 0;
    }
    auto self = shared_from_this();
    if (state_->Submit([self] {
            self->SealAndPhysicalCloseOwner(std::make_error_code(std::errc::operation_canceled));
        }, SubmitGate::Internal) != SubmitStatus::Accepted) {
        ++close_init_submit_failures_;
    }
    state_->NotifyChange();
}

void ConnectionSession::CloseOnPeerTerminal()
{
    // 读终态 sticky；尝试中的占位与 Accepted 的责任移交分开。
    // 单连接 dispatcher 串行推进，mx_ 只保护状态，不跨 Submit/同步回调持锁。
    // sealed/service_stopped 首胜；已有命令与 Stop 均经物理收口首胜门。
    std::error_code reason;
    {
        std::lock_guard<std::mutex> guard(mx_);
        if (!eof_terminal_ || sealed_ || service_stopped_
            || eof_close_submission_ != PeerCloseSubmission::Idle) { return; }
        eof_close_submission_ = PeerCloseSubmission::Submitting;
        reason = eof_reason_;
    }
    auto self = shared_from_this();
    // ponytail: 每事件 Full 只补偿一次；持续拒绝或没有后续事件不承诺自动恢复。
    // 未接纳时责任留在 registry/session，下一次可达事件可再试，Stop 仍直接收口，
    // Detach 仍拒绝活 fd。若需无事件恢复，另评审 owner 恢复通知，不能在此造重试循环。
    for (int attempt = 0; attempt < 2; ++attempt) {
        SubmitStatus status = SubmitStatus::Failed;
        try {
            status = SubmitOperation([self, reason] { self->SealAndPhysicalCloseOwner(reason); },
                SubmitGate::Internal);
        } catch (...) {
            status = SubmitStatus::Failed;
        }
        if (status == SubmitStatus::Accepted) {
            std::lock_guard<std::mutex> guard(mx_);
            eof_close_submission_ = PeerCloseSubmission::Accepted;
            return; // 只有接纳的命令拥有后续物理收口责任
        }
        ++close_init_submit_failures_;
        if (status != SubmitStatus::Full) { break; }
    }
    {
        std::lock_guard<std::mutex> guard(mx_);
        eof_close_submission_ = PeerCloseSubmission::Idle; // 全部未接纳，无命令可重复执行
    }
}

// ---- 事件内 Reader --------------------------------------------------------

std::error_code ConnectionSession::RequireEventContext() const
{
    const auto coro = bbt::coroutine::GetLocalCoroutineId();
    if (coro == 0) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    std::lock_guard<std::mutex> guard(mx_);
    if (!event_running_ || event_coro_ != coro) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    return {};
}

std::size_t ConnectionSession::InboundBuffered() const
{
    if (RequireEventContext()) {
        throw std::system_error(std::make_error_code(std::errc::operation_not_permitted),
            "bbt::infra::net: InboundBuffered 仅限当前 connection 的活跃事件协程");
    }
    std::lock_guard<std::mutex> guard(mx_);
    return snap_lent_ ? snap_.Remaining() : 0;
}

std::string_view ConnectionSession::Peek() const
{
    if (RequireEventContext()) {
        throw std::system_error(std::make_error_code(std::errc::operation_not_permitted),
            "bbt::infra::net: Peek 仅限当前 connection 的活跃事件协程");
    }
    std::lock_guard<std::mutex> guard(mx_);
    return snap_lent_ ? snap_.Peek() : std::string_view{};
}

std::error_code ConnectionSession::Discard(std::size_t bytes)
{
    const auto context_error = RequireEventContext();
    if (context_error) {
        return context_error;
    }
    std::lock_guard<std::mutex> guard(mx_);
    if (!snap_lent_) {
        // Wake 空快照：Discard(0) 合法；无快照时任何消费都被拒绝。
        return bytes == 0 ? std::error_code{} : std::make_error_code(std::errc::result_out_of_range);
    }
    return snap_.Discard(bytes);
}

// ---- 观测 -----------------------------------------------------------------

ConnectionSession::Counters ConnectionSession::GetCounters() const
{
    Counters counters;
    counters.spawns = spawns_.load();
    counters.open_entered = open_entered_.load();
    counters.traffic_dispatched = traffic_dispatched_.load();
    counters.peer_terminal_detected = peer_terminal_detected_.load();
    counters.wake_calls = wake_calls_.load();
    counters.wake_rejected_sealed = wake_rejected_sealed_.load();
    counters.late_dropped = late_dropped_.load();
    counters.handler_exceptions = handler_exceptions_.load();
    counters.dispatch_crashes = dispatch_crashes_.load();
    counters.onclose_entered = onclose_entered_.load();
    counters.onclose_returned = onclose_returned_.load();
    counters.op_timeouts = op_timeouts_.load();
    counters.snapshot_return_errors = snapshot_return_errors_.load();
    counters.close_init_submit_failures = close_init_submit_failures_.load();
    counters.active_peak = static_cast<std::size_t>(active_peak_.load());
    return counters;
}

ConnectionSession::StateSnapshot ConnectionSession::SnapshotState() const
{
    std::lock_guard<std::mutex> guard(mx_);
    StateSnapshot snapshot;
    snapshot.id = id_;
    snapshot.sealed = sealed_;
    snapshot.physical_closed = physical_closed_;
    snapshot.busy = busy_;
    snapshot.snap_lent = snap_lent_;
    snapshot.close_finished = close_finished_;
    snapshot.open_dispatched = open_dispatched_;
    snapshot.snap_return_pending = snap_return_pending_;
    return snapshot;
}

StreamConnection::Stats ConnectionSession::ConnStatsOwner() const
{
    if (!state_->Owner().IsOwner()) {
        throw std::logic_error("bbt::infra::net: ConnStatsOwner 仅限 owner 域");
    }
    return conn_ ? conn_->GetStats() : StreamConnection::Stats{};
}

bool ConnectionSession::DrainedSnapshot() const
{
    std::lock_guard<std::mutex> guard(mx_);
    // 「handler 事件/快照/桥接命令均已结束」的会话侧谓词；全桥命令结算
    // 由 ConnectionBridge::DrainedLocked 以 submitted==executed 把关。
    return physical_closed_ && close_finished_ && !busy_
        && !pending_open_ && !pending_traffic_ && !pending_wake_ && !close_event_pending_
        && !pending_take_ && !snap_lent_ && !snap_return_pending_ && !pending_return_;
}

// ============================================================================
// ConnectionBridge
// ============================================================================

ConnectionBridge::ConnectionBridge(EventLoop& owner, const NormalizedOptions& options)
    : state_(std::make_shared<ConnectionBridgeState>(owner, options)) {}

std::shared_ptr<ConnectionBridge> ConnectionBridge::Create(EventLoop& owner,
    const NormalizedOptions& options, std::error_code& error)
{
    error.clear();
    return std::shared_ptr<ConnectionBridge>(new ConnectionBridge(owner, options));
}

std::shared_ptr<CoTcpConnection> ConnectionBridge::Adopt(NativeSocket&& socket, Address peer,
    std::shared_ptr<ICoConnectionHandler> handler, std::error_code& error)
{
    if (!state_->Attached() || !state_->Owner().IsOwner()) {
        error = std::make_error_code(std::errc::operation_not_permitted);
        return nullptr;
    }
    {
        std::lock_guard<std::mutex> guard(state_->m_);
        if (state_->service_stopped_) {
            error = std::make_error_code(std::errc::operation_canceled);
            return nullptr; // fd 尚未转移；授权后与控制线程封门交错也由本 owner 完成或回滚。
        }
    }
    if (!handler) {
        error = std::make_error_code(std::errc::invalid_argument);
        return nullptr;
    }
    const auto id = next_id_++;
    auto shell = std::shared_ptr<CoTcpConnection>(new CoTcpConnection());
    auto session = std::make_shared<ConnectionSession>(state_, std::move(handler), std::move(peer), id);
    shell->m_session = session;
    session->BindShell(shell);
    {
        std::lock_guard<std::mutex> guard(state_->m_);
        registry_.emplace(id, shell);
        ++state_->stats_.sessions_registered;
    }
    state_->cv_.notify_all();
    std::error_code create_error;
    const auto rollback_unbound = [&] {
        {
            std::lock_guard<std::mutex> guard(state_->m_);
            registry_.erase(id);
            --state_->stats_.sessions_registered;
        }
        state_->cv_.notify_all();
    };
    // 仅回滚尚未绑定的登记；Create 转移前异常仍由输入 NativeSocket 拥有 fd，
    // 转移后的初始化清理由 StreamConnection 自身负责。
    std::shared_ptr<StreamConnection> connection;
    try {
        connection = StreamConnection::Create(state_->Owner(), std::move(socket), state_->Options(),
            session->MakeCallbacks(), create_error);
    } catch (...) {
        rollback_unbound();
        throw;
    }
    if (!connection) {
        rollback_unbound();
        error = create_error;
        return nullptr;
    }
    try {
        session->AdoptOwner(std::move(connection)); // 先绑定，再登记首个 OnOpen 事件位
    } catch (...) {
        // 已绑定时不能 erase 活 backend。合法 owner 内撤业务并推进物理关闭；
        // 若关闭也失败，异常继续报告，registry 保留责任供 Stop 重试（非析构保证）。
        if (const auto close_error = session->StopServiceOwner()) {
            throw std::system_error(close_error);
        }
        throw;
    }
    error.clear();
    return shell;
}

std::error_code ConnectionBridge::CloseNative(std::size_t connection_id, std::error_code reason)
{
    if (!state_->Attached() || !state_->Owner().IsOwner()) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    std::shared_ptr<CoTcpConnection> shell;
    {
        std::lock_guard<std::mutex> guard(state_->m_);
        const auto it = registry_.find(connection_id);
        if (it == registry_.end()) {
            return std::make_error_code(std::errc::invalid_argument);
        }
        shell = it->second;
    }
    return shell->m_session->SealAndPhysicalCloseOwner(std::move(reason));
}

void ConnectionBridge::SealService()
{
    std::lock_guard<std::mutex> guard(state_->m_);
    state_->service_stopped_ = true;
}

std::error_code ConnectionBridge::StopServiceOwner()
{
    if (!state_->Attached() || !state_->Owner().IsOwner()) {
        return std::make_error_code(std::errc::operation_not_permitted);
    }
    SealService(); // 已 sealed 也必须继续物理关闭，不能提前返回。
    // registry 的插入/接纳回滚都在同 owner；Detach 仅允许 Join 后执行。
    // 关闭短通知只改 session/state，不移除登记；不持 state 锁跨关闭/回调。
    // 因而可直接遍历，无需分配 shells 快照；重复调用仍推进所有未完成项。
    std::error_code result;
    for (const auto& entry : registry_) {
        try {
            const auto error = entry.second->m_session->StopServiceOwner();
            if (error && !result) { result = error; }
        } catch (const std::bad_alloc&) {
            if (!result) { result = std::make_error_code(std::errc::not_enough_memory); }
        } catch (...) {
            if (!result) { result = std::make_error_code(std::errc::io_error); }
        }
    }
    return result;
}

bool ConnectionBridge::NetworkCloseReady() const
{
    // 内部只读：state 锁内嵌 session 短锁，与 DetachOwner 的 SnapshotState 前置
    // 核查同一嵌套顺序。physical_waiters 记录结算 = OnClosedOwner 已完成移交/补发，
    // 不要求 Handler/OnClose 返回（非 Drained，不等业务）。
    std::lock_guard<std::mutex> guard(state_->m_);
    for (const auto& entry : registry_) {
        if (!entry.second->m_session->PhysicalCloseSettled()) { return false; }
    }
    return true;
}

std::error_code ConnectionBridge::DetachOwner()
{
    // 网络完成点（显式顺序见头文件契约）：owner 循环已停并 join 后由控制
    // 线程调用。前置核查每会话已物理关闭且 owned waiter 已结算——不满足时
    // 不发布 detached、不清 registry、不释放 backend，返回拒绝错误，由调用方
    // 推进收口后重试，避免半脱离（强拆活 socket / 丢 waiter / 丢物理收口责任人）。
    std::map<std::size_t, std::shared_ptr<CoTcpConnection>> doomed;
    {
        std::lock_guard<std::mutex> guard(state_->m_);
        if (state_->detached_.load()) { return {}; } // 幂等：已脱离
        for (const auto& entry : registry_) {
            if (!entry.second->m_session->PhysicalCloseSettled()) {
                return std::make_error_code(std::errc::operation_in_progress);
            }
        }
        // 脱离前冻结真实 drain 结果：registry 清空后无任何会话可评估，只能
        // 沿用已证 drained 或保守 false，绝不把强停压掉 OnClose 的会话报成 drained。
        const bool drained = DrainedLocked();
        state_->owner_attached_ = false;
        state_->service_stopped_ = true;
        state_->owner_ = nullptr;
        state_->detached_.store(true);
        state_->drained_at_detach_ = drained;
        doomed.swap(registry_); // 锁外析构：最后强持有引发的用户析构可合法观测桥
    }
    state_->cv_.notify_all();
    // 锁外、context 仍存活：断开每会话 owner 域 backend（socket/EventLoop 依赖）。
    // 直接遍历已转移的登记表，发布后不再分配临时副本，避免 bad_alloc
    // 造成 detached=true 而 backend 尚未释放、重试又虚报成功的半完成状态。
    for (const auto& entry : doomed) { (void)entry.second->m_session->ReleaseOwnerBackend(); }
    // doomed 在本作用域结束时析构（短锁外，不在 state_->m_ 下）。
    return {};
}

bool ConnectionBridge::Drained() const noexcept
{
    std::lock_guard<std::mutex> guard(state_->m_);
    return DrainedLocked();
}

bool ConnectionBridge::WaitDrained(CoDeadline deadline) const
{
    std::unique_lock<std::mutex> guard(state_->m_);
    return state_->cv_.wait_until(guard, deadline, [this] { return DrainedLocked(); });
}

bool ConnectionBridge::DrainedLocked() const
{
    // 脱离后不再重估（registry 已空、无会话可评估）：沿用脱离前冻结的真实结果。
    if (state_->detached_.load()) {
        return state_->drained_at_detach_;
    }
    if (state_->stats_.ops_submitted.load() != state_->stats_.ops_executed.load()) {
        return false; // 桥接命令未全部结束
    }
    for (const auto& entry : registry_) {
        if (!entry.second->m_session->DrainedSnapshot()) {
            return false; // 无成功 OnClose 执行时不报成功 drain
        }
    }
    return true;
}

std::size_t ConnectionBridge::SessionCount() const
{
    std::lock_guard<std::mutex> guard(state_->m_);
    return registry_.size();
}

std::size_t ConnectionBridge::LiveSessionCount() const
{
    // ponytail: O(保留会话数)，仅 LC 使用；实测扫描成为瓶颈后再升级恰一次计数。
    // 与 NetworkCloseReady/DetachOwner 相同的 state → session 短锁序；不等业务。
    std::lock_guard<std::mutex> guard(state_->m_);
    std::size_t count = 0;
    for (const auto& entry : registry_) {
        if (!entry.second->m_session->SnapshotState().physical_closed) { ++count; }
    }
    return count;
}

std::shared_ptr<ConnectionSession> ConnectionBridge::SessionOf(std::size_t connection_id) const
{
    std::lock_guard<std::mutex> guard(state_->m_);
    const auto it = registry_.find(connection_id);
    if (it == registry_.end()) {
        return nullptr;
    }
    return it->second->m_session;
}

// ============================================================================
// ConnectionBridgeState（轻量共享控制状态）
// ============================================================================

SubmitStatus ConnectionBridgeState::Submit(std::function<void()> command, SubmitGate gate)
{
    // 同一门内取有效 owner 并完成入队：只覆盖投递，不跨挂起等待/用户回调；
    // 绝不解锁取裸指针再 Submit。owner 不可达 → Sealed（不触 owner）。
    // Internal 门只看 owner 可达性：强停封服务不等于已脱离，handler 返回时
    // 的内部归还仍必须可靠入队；业务事件/命令门保留 service_stopped_ 拦截。
    std::lock_guard<std::mutex> guard(m_);
    if (!owner_attached_ || owner_ == nullptr) {
        return SubmitStatus::Sealed;
    }
    if (gate == SubmitGate::Business && service_stopped_) {
        return SubmitStatus::Sealed;
    }
    return owner_->Submit(std::move(command));
}

void ConnectionBridgeState::NotifyChange() const
{
    std::lock_guard<std::mutex> guard(m_);
    cv_.notify_all();
}

} // namespace bbt::infra::net
