/**
 * 客户端 owner 和服务端的启动、accept 与关闭流程。
 */
#include <bbt/infra/rpc/CoRpcServer.hpp>
#include <bbt/infra/rpc/detail/Session.hpp>
#include <bbt/infra/rpc/detail/SessionTestAccess.hpp>
#include <bbt/infra/rpc/detail/RpcSessionState.hpp>

#include <algorithm>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <bbt/coroutine/syntax/SyntaxMacro.hpp>
#include <bbt/infra/detail/CloseWait.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>

namespace bbt::infra::rpc {

using namespace std::chrono_literals;

namespace {

void FinishInbound(ServerState& server, std::uint64_t id)
{
    net::Connection abandoned;
    {
        std::lock_guard<std::mutex> lock(server.mu);
        abandoned = server.handshakes.at(id).conn;
    }
    if (abandoned.Valid()) {
        (void)abandoned.Close();
    }
    std::lock_guard<std::mutex> lock(server.mu);
    server.handshakes.at(id).finished = true;
    server.ReapReleasedLocked();
    server.close_signal->Notify();
}

void AcceptLoop(std::shared_ptr<ServerState> server)
{
    const auto exit_loop = [&] {
        std::lock_guard<std::mutex> lock(server->mu);
        server->accept_loop_exited = true;
        server->close_signal->Notify();
    };
    for (;;) {
        bool reject_pending;
        {
            std::lock_guard<std::mutex> lock(server->mu);
            if (server->stopping) {
                server->accept_loop_exited = true;
                server->close_signal->Notify();
                return;
            }
            server->ReapReleasedLocked();
            reject_pending = server->rejected_accept.Valid();
        }
        if (reject_pending) {
            bbtco_sleep(10);
            continue;
        }
        auto accepted = server->engine->CoAccept({RpcClock::now() + 50ms});
        if (accepted.error == std::errc::timed_out) {
            continue;
        }
        if (accepted.error) {

            /*
             * RequestStopAccepting/Seal 会取消在途 CoAccept：这是正常封口路径，
             * 保留调用方已发布的关闭原因。
             */
            bool sealed = false;
            {
                std::lock_guard<std::mutex> lock(server->mu);
                sealed = server->stopping;
            }
            if (sealed) {
                exit_loop();
                return;
            }
            detail::ServerSeal(server, Err(ErrorCode::RuntimeUnavailable, "rpc server: accept loop failed"));
            exit_loop();
            return;
        }
        const auto conn_id = accepted.connection.Raw().Id();
        bool reject;
        {
            std::lock_guard<std::mutex> lock(server->mu);
            server->ReapReleasedLocked();
            reject = server->stopping
                     || server->links.size() + server->handshakes.size() >= server->options.MaxConnections();
            if (!reject) {
                try {
                    server->handshakes.emplace(conn_id, ServerState::Handshake{accepted.connection.Raw()});
                } catch (...) {
                    reject = true;
                }
            }
            if (reject) {
                server->rejected_accept = accepted.connection.Raw();
            }
        }
        if (reject) {
            (void)accepted.connection.Close();
            continue;
        }
        bool registered = false;
        std::weak_ptr<ServerState> weak = server;
        try {
            // NOLINTNEXTLINE(coroutine-sugar) -- inbound task needs the explicit 128 KiB stack
            g_scheduler->RegistCoroutineTask(
                128 * 1024,
                [weak, conn = accepted.connection, conn_id]() mutable {
                    if (auto server = weak.lock()) {
                        try {
                            detail::ServeInbound(server, detail::TcpChannel{conn});
                        } catch (...) {

                            /**
                             * 与握手拒绝共用同一退役路径，不越过 scheduler 边界。
                             */
                        }
                        FinishInbound(*server, conn_id);
                    } else {
                        (void)conn.Close();
                    }
                },
                registered);
        } catch (...) {

            /**
             * 构造任务失败，仍由 accept 循环归还握手责任。
             */
        }
        if (!registered) {
            FinishInbound(*server, conn_id);
        }
        bbtco_yield;
    }
}

// clang-format off
}
// clang-format on

void detail::SessionOwnerSeal(const std::shared_ptr<SessionOwnerState>& owner, Error reason) noexcept
{
    std::vector<std::shared_ptr<SessionCore>> sessions;
    std::vector<net::Connection> handshakes;
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        if (!owner->close_reason) {
            owner->close_reason = std::move(reason);
        }
        if (!owner->closed) {

            /**
             * 「失败在途」恰好一次：closed 位唯一表达，Draining 下同样执行。
             */
            owner->closed = true;
            for (const auto& lease : owner->link_leases) {
                if (auto session = lease.session.lock()) {
                    sessions.push_back(std::move(session));
                }
            }

            /**
             * 建立中出站握手 conn 同受关闭责任：RequestClose 让握手尽快
             * 失败退出，不等调用 deadline（物理关闭责任）。
             */
            for (auto& [key, est] : owner->establishing) {
                (void)key;
                if (est->conn.Valid()) {
                    handshakes.push_back(est->conn);
                }
            }
        }
        owner->close_signal->Notify();
    }
    for (auto& session : sessions) {
        session->Kill({});
    }
    for (auto& conn : handshakes) {
        (void)conn.Close({}, std::make_error_code(std::errc::connection_aborted));
    }

    /**
     * 控制器永久 closed：无论本次是否首次封口都置位，保证 Close 返回后 Arm
     * 一定看到 Closed；在途 Pause 由 runtime 的 owner.closed 检查退出。锁外
     * 单独取 hook 锁，不与 owner.mu 形成嵌套锁序。
     */
    if (owner->hooks) {
        detail::SessionHookAccess::Close(*owner->hooks);
    }
}

SessionOwnerState::~SessionOwnerState()
{

    /**
     * 兜底：owner 未经 Seal 直接销毁时同样封口控制器。handle 可能比 owner
     * 活得久，但只保留有界控制状态，不再接受注册。
     */
    if (hooks) {
        detail::SessionHookAccess::Close(*hooks);
    }
}

bool detail::SessionOwnerClosed(const SessionOwnerState& owner) noexcept
{
    std::lock_guard<std::mutex> lock(owner.mu);
    return owner.closed || owner.draining;
}

bool detail::SessionOwnerSealed(const SessionOwnerState& owner) noexcept
{
    std::lock_guard<std::mutex> lock(owner.mu);

    /**
     * 逻辑封口：优雅排空期间已拒新
     */
    return owner.closed || owner.draining;
}

std::optional<Error> detail::SessionOwnerCloseReason(const SessionOwnerState& owner)
{
    std::lock_guard<std::mutex> lock(owner.mu);
    return owner.close_reason;
}

bool detail::SessionOwnerBeginOperation(const std::shared_ptr<SessionOwnerState>& owner, Error& closed_error)
{
    std::lock_guard<std::mutex> lock(owner->mu);

    /**
     * 非 Open 即拒绝：Close 原因优先，否则通用 Closed。
     */
    if (owner->closed || owner->draining) {
        closed_error = owner->close_reason ? *owner->close_reason : Err(ErrorCode::Closed, "rpc client: closed");
        return false;
    }
    ++owner->active_ops;
    if (const auto id = bbt::coroutine::GetLocalCoroutineId(); id != 0) {
        ++owner->operation_tasks[id];
    }
    return true;
}

void detail::SessionOwnerEndOperation(const std::shared_ptr<SessionOwnerState>& owner) noexcept
{
    std::lock_guard<std::mutex> lock(owner->mu);
    if (owner->active_ops != 0) {
        --owner->active_ops;
    }
    if (const auto id = bbt::coroutine::GetLocalCoroutineId(); id != 0) {
        if (const auto it = owner->operation_tasks.find(id); it != owner->operation_tasks.end()) {
            if (--it->second == 0) {
                owner->operation_tasks.erase(it);
            }
        }
    }
    owner->close_signal->Notify();
}

/**
 * 每个 close_signal 只有一个等待位。关闭方在状态锁内检查责任归还情况，
 * 在锁外分片等待，直到责任归还或达到绝对期限。
 */
namespace {

/**
 * 关闭等待唯一分片实现见 bbt/infra/detail/CloseWait.hpp：每轮取
 * min(deadline, now+50ms)，AlreadyWaiting 让出 5ms 后复查谓词。
 */
using bbt::infra::detail::WaitCloseSlice;

/**
 * owner 逻辑排空等待（Graceful）：DrainedLocked 胜返回 ok；等待期间被
 * Immediate 强制封口（closed）时返回首个关闭原因；到 deadline
 * 返回 TimedOut。以时间片复查而不是完全依赖唤醒——close_signal 是单等待位。
 */
result<void> OwnerWaitDrained(const std::shared_ptr<SessionOwnerState>& owner, bbt::coroutine::Deadline deadline)
{
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(owner->mu);
            if (owner->closed) {
                return result<void>::err(
                    owner->close_reason ? *owner->close_reason : Err(ErrorCode::Closed, "rpc client: closed"));
            }
            if (owner->DrainedLocked()) {
                return result<void>::ok();
            }
        }
        if (deadline != bbt::coroutine::Deadline::max() && RpcClock::now() >= deadline) {
            return ErrV(ErrorCode::TimedOut, "graceful close deadline exceeded");
        }
        const auto slice = bbt::infra::detail::NextCloseSlice(deadline);
        if (const auto waited = WaitCloseSlice(owner->close_signal, slice); !waited) {
            return result<void>::err(waited.error());
        }
    }
}

result<void> OwnerWaitSettled(const std::shared_ptr<SessionOwnerState>& owner, bbt::coroutine::Deadline deadline)
{
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(owner->mu);
            owner->ReapReleasedLocked();
            if (owner->TeardownLocked()) {
                return result<void>::ok();
            }
        }
        if (deadline != bbt::coroutine::Deadline::max() && RpcClock::now() >= deadline) {
            return ErrV(ErrorCode::TimedOut, "timeout close: resource release incomplete");
        }
        const auto slice = bbt::infra::detail::NextCloseSlice(deadline);
        if (const auto waited = WaitCloseSlice(owner->close_signal, slice); !waited) {
            return result<void>::err(waited.error());
        }
    }
}

/**
 * server 逻辑排空等待（Graceful）：DrainedLocked 胜返回 ok；被打断返回已
 * 发布原因；到 deadline 返回 TimedOut。
 */
result<void> ServerWaitDrained(const std::shared_ptr<ServerState>& state, bbt::coroutine::Deadline deadline)
{
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(state->mu);
            if (state->closed) {
                return result<void>::err(
                    state->close_reason ? *state->close_reason : Err(ErrorCode::Closed, "rpc server: closed"));
            }
            if (state->DrainedLocked()) {
                return result<void>::ok();
            }
        }
        if (deadline != bbt::coroutine::Deadline::max() && RpcClock::now() >= deadline) {
            return ErrV(ErrorCode::TimedOut, "graceful close deadline exceeded");
        }
        const auto slice = bbt::infra::detail::NextCloseSlice(deadline);
        if (const auto waited = WaitCloseSlice(state->close_signal, slice); !waited) {
            return result<void>::err(waited.error());
        }
    }
}

result<void> WaitServerReleased(const std::shared_ptr<ServerState>& state, bbt::coroutine::Deadline deadline)
{
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(state->mu);
            state->ReapReleasedLocked();
            if (state->StoppedLocked()) {
                return result<void>::ok();
            }
        }
        if (deadline != bbt::coroutine::Deadline::max() && RpcClock::now() >= deadline) {
            return ErrV(ErrorCode::TimedOut, "timeout close: resource release incomplete");
        }
        const auto slice = bbt::infra::detail::NextCloseSlice(deadline);
        if (const auto waited = WaitCloseSlice(state->close_signal, slice); !waited) {
            return result<void>::err(waited.error());
        }
    }
}

// clang-format off
}
// clang-format on

result<std::shared_ptr<ServerState>> detail::ServerCreate(
    std::shared_ptr<net::Engine> engine,
    const std::shared_ptr<SessionOwnerState>& owner,
    AdmissionHandler admission_handler,
    ServerOptions options)
{
    using R = result<std::shared_ptr<ServerState>>;
    if (!engine || !owner || !admission_handler) {
        return R::err(Err(ErrorCode::InvalidArgument, "engine/session_owner/handler required"));
    }
    if (auto valid = options.Validate(); !valid) {
        return R::err(valid.error());
    }

    /**
     * shared-Engine 装配边界：accept/收口与 owner 出站拨号、台账、关闭必须
     * 由同一宿主推进；跨 Engine 注入的 owner 拒绝在 accept-handoff 之前。
     */
    if (engine.get() != owner->engine.get()) {
        return R::err(Err(ErrorCode::InvalidArgument, "server and session_owner must share the same net::Engine"));
    }
    if (engine->Phase() != net::EnginePhase::Idle) {
        return R::err(Err(ErrorCode::InvalidContext, "engine must be Idle at Create"));
    }
    if (auto handoff = engine->EnableAcceptHandoff(options.MaxConnections() < 64 ? options.MaxConnections() : 64);
        handoff) {
        return R::err(Err(ErrorCode::RuntimeUnavailable, "accept handoff setup failed"));
    }
    auto state = std::make_shared<ServerState>();
    state->engine = std::move(engine);
    state->owner = owner;
    state->framework_handler = std::move(admission_handler);
    state->options = options;
    const std::weak_ptr<ServerState> weak_server = state;
    state->handler = [weak_server](AdmissionItem item) -> result<void> {
        const auto server = weak_server.lock();
        if (!server) {
            return ErrV(ErrorCode::Closed, "rpc server is gone");
        }
        return detail::DispatchAdmission(server, std::move(item));
    };
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        owner->inbound_server = state;
    }
    return R::ok(std::move(state));
}

result<void> detail::ServerStart(const std::shared_ptr<ServerState>& state)
{

    /*
     * 测试注入在入口取得：任何早退都会丢弃它，不会留下引用已销毁测试
     * 对象的待执行 barrier。
     */
    auto test_barrier = detail::TakeStartRegistrationBarrierForTest();
    {
        std::lock_guard<std::mutex> lock(state->mu);
        if (state->closed || state->draining) {
            return ErrV(ErrorCode::Closed, "server closed");
        }
        if (state->started) {
            return result<void>::ok();
        }
        if (state->starting) {

            /**
             * 同一 Server 只允许一个 Start 在途；并发重复启动不静默成功。
             */
            return ErrV(ErrorCode::InvalidContext, "server start already in progress");
        }
        if (state->engine->Phase() != net::EnginePhase::Running) {
            return ErrV(ErrorCode::InvalidContext, "engine must be Running with listener");
        }

        /**
         * 只在本临界区认领启动权：AdmissionQueue::Create 与两次协程任务
         * 注册都可能因栈池未命中在 Co 域挂起，不能持 state->mu 完成（Close/
         * Seal、Stats 与收口观察都用这把锁）。
         */
        state->starting = true;
    }

    if (test_barrier) {
        test_barrier();
    }

    /**
     * 单 FIFO admission worker。
     */
    auto created =
        AdmissionQueue::Create(state->options.MaxInflight(), state->options.MaxHandlerBytes(), state->handler);
    if (!created) {
        std::lock_guard<std::mutex> lock(state->mu);
        state->starting = false;
        state->close_signal->Notify();
        return result<void>::err(created.error());
    }
    std::shared_ptr<AdmissionQueue> queue = std::move(created).value();
    detail::AdmissionAccess::AttachQueue(queue, state);

    bool registered = false;
    if (!detail::ConsumeStartRegistrationFailureForTest()) {
        // NOLINTNEXTLINE(coroutine-sugar) -- accept task needs the explicit 128 KiB stack
        g_scheduler->RegistCoroutineTask(
            128 * 1024,
            [state] {
                // clang-format off

                /**
                 * 裸协程任务：异常逃逸即 terminate——兜底封 server。
                 */
                // clang-format on
                try {
                    AcceptLoop(state);
                } catch (...) {
                    detail::ServerSeal(state, Err(ErrorCode::InternalError, "rpc server: accept loop threw"));
                    std::lock_guard<std::mutex> lock(state->mu);
                    state->accept_loop_exited = true;
                    state->close_signal->Notify();
                }
            },
            registered);
    }

    if (registered) {
        bool sealed = false;
        {
            std::lock_guard<std::mutex> lock(state->mu);
            state->starting = false;
            state->queue = queue;

            /**
             * accept 任务已登记：即使同时被 Close/Seal 封口，物理收口也要
             * 等该任务退出，不能按「从未 Start」放行。
             */
            state->started = true;

            /**
             * Graceful Close 先置 draining 再 Seal；两者都与「已封口」同义，
             * 不能让 Start 在排空窗口里仍报成功。
             */
            sealed = state->closed || state->draining;
            state->close_signal->Notify();
        }
        if (sealed) {

            /**
             * Close/Seal 在注册窗口内封口：Seal 看不到尚未公布的 queue，
             * 由这里补封；accept 任务进入循环前先看 stopping，会立即退出。
             */
            (void)queue->Close();
            return ErrV(ErrorCode::Closed, "server closed during start");
        }
        return result<void>::ok();
    }

    /**
     * 部分启动失败：启动权与队列责任全部回滚，不发布 started/queue；
     * 调用方（或下一次 Start）看到的是可重试的空闲 Server。
     */
    {
        std::lock_guard<std::mutex> lock(state->mu);
        state->starting = false;
        state->close_signal->Notify();
    }
    (void)queue->Close();
    return ErrV(ErrorCode::RuntimeUnavailable, "accept task registration failed");
}

void detail::ServerStopAccepting(const std::shared_ptr<ServerState>& state) noexcept
{
    std::shared_ptr<AdmissionQueue> queue;
    {
        std::lock_guard<std::mutex> lock(state->mu);
        state->stopping = true;
        queue = state->queue;
        state->close_signal->Notify();
    }
    state->engine->RequestStopAccepting();
    if (queue) {
        (void)queue->Close();
    }
}

void detail::ServerSeal(const std::shared_ptr<ServerState>& state, Error reason) noexcept
{
    std::vector<std::shared_ptr<SessionCore>> links;
    std::vector<net::Connection> handshakes;
    std::shared_ptr<AdmissionQueue> queue;
    {
        std::lock_guard<std::mutex> lock(state->mu);
        if (!state->close_reason) {
            state->close_reason = std::move(reason);
        }
        if (state->closed) {
            return;
        }
        state->closed = true;
        state->stopping = true;
        queue = state->queue;
        for (const auto& lease : state->links) {
            if (auto session = lease.session.lock()) {
                links.push_back(session);
            }
        }

        for (const auto& [id, handshake] : state->handshakes) {
            (void)id;
            if (handshake.conn.Valid()) {
                handshakes.push_back(handshake.conn);
            }
        }
        if (state->rejected_accept.Valid()) {
            handshakes.push_back(state->rejected_accept);
        }
        state->close_signal->Notify();
    }
    state->engine->RequestStopAccepting();
    if (queue) {
        (void)queue->Close();
    }
    for (auto& session : links) {
        session->Kill({});
    }
    for (auto& conn : handshakes) {
        (void)conn.Close({}, std::make_error_code(std::errc::connection_aborted));
    }
}

result<void> detail::SessionOwnerClose(const std::shared_ptr<SessionOwnerState>& owner, const CloseOptions& options)
{
    if (const auto valid = bbt::infra::detail::ValidateCloseOptions(options); !valid) {
        return result<void>::err(valid.error());
    }
    if ((options.mode == CloseMode::Graceful || options.wait_release) && !bbt::coroutine::IsInCoroutineContext()) {
        return ErrV(ErrorCode::InvalidContext, "rpc client: Close requires coroutine context");
    }
    const auto self = bbt::coroutine::GetLocalCoroutineId();
    {
        std::lock_guard<std::mutex> lock(owner->mu);

        /**
         * 自等待拒绝先于任何封口副作用：已接纳调用的协程不能等自己归还。
         */
        if ((options.mode == CloseMode::Graceful || options.wait_release) && self != 0
            && owner->operation_tasks.find(self) != owner->operation_tasks.end()) {
            return ErrV(ErrorCode::InvalidContext, "rpc client: Close rejected from an admitted call");
        }
    }
    const auto reason = SettleCloseReason(options.reason.value_or(Error{}), "rpc client: closed by owner");
    if (options.mode == CloseMode::Immediate) {
        SessionOwnerSeal(owner, reason);
        return bbt::infra::detail::FinishCloseWithRelease(
            options, [owner](bbt::coroutine::Deadline deadline) { return SessionOwnerWaitStopped(owner, deadline); });
    }
    bool already_closed = false;
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        if (owner->closed) {

            /**
             * 已被立即关闭：排空则继续可能需要的物理等待，否则如实返回首个关闭原因。
             */
            if (!owner->DrainedLocked()) {
                return result<void>::err(
                    owner->close_reason ? *owner->close_reason : Err(ErrorCode::Closed, "rpc client: closed"));
            }
            already_closed = true;
        }
        if (!already_closed) {

            /**
             * 发布首因与拒新共用这把锁，后续 Immediate 沿用首因。
             */
            if (!owner->close_reason) {
                owner->close_reason = reason;
            }
            if (!owner->draining) {
                owner->draining = true;
                owner->close_signal->Notify();
            }
        }
    }
    if (already_closed) {
        return bbt::infra::detail::FinishCloseWithRelease(
            options, [owner](bbt::coroutine::Deadline deadline) { return SessionOwnerWaitStopped(owner, deadline); });
    }
    if (const auto drained = OwnerWaitDrained(owner, options.deadline); !drained) {
        return drained;
    }
    SessionOwnerSeal(owner, reason);
    return bbt::infra::detail::FinishCloseWithRelease(
        options, [owner](bbt::coroutine::Deadline deadline) { return SessionOwnerWaitStopped(owner, deadline); });
}

result<void> detail::SessionOwnerWaitStopped(
    const std::shared_ptr<SessionOwnerState>& owner,
    bbt::coroutine::Deadline deadline)
{
    if (!bbt::coroutine::IsInCoroutineContext()) {
        return ErrV(ErrorCode::InvalidContext, "rpc client: WaitStopped requires coroutine context");
    }
    const auto self = bbt::coroutine::GetLocalCoroutineId();
    {
        std::lock_guard<std::mutex> lock(owner->mu);

        /**
         * 自等待拒绝先于封口状态判断：已接纳调用的协程不能等自己归还。
         */
        if (self != 0 && owner->operation_tasks.find(self) != owner->operation_tasks.end()) {
            return ErrV(ErrorCode::InvalidContext, "rpc client: WaitStopped rejected from an admitted call");
        }
        if (!owner->closed) {
            return ErrV(ErrorCode::InvalidArgument, "rpc client: close before WaitStopped");
        }
    }
    return OwnerWaitSettled(owner, deadline);
}

result<void> detail::ServerClose(const std::shared_ptr<ServerState>& state, const CloseOptions& options)
{
    if (const auto valid = bbt::infra::detail::ValidateCloseOptions(options); !valid) {
        return result<void>::err(valid.error());
    }
    if ((options.mode == CloseMode::Graceful || options.wait_release) && !bbt::coroutine::IsInCoroutineContext()) {
        return ErrV(ErrorCode::InvalidContext, "rpc server: Close requires coroutine context");
    }
    const auto self = bbt::coroutine::GetLocalCoroutineId();
    {
        std::lock_guard<std::mutex> lock(state->mu);
        if ((options.mode == CloseMode::Graceful || options.wait_release) && self != 0
            && state->responsibility_tasks.find(self) != state->responsibility_tasks.end()) {
            return ErrV(ErrorCode::InvalidContext, "rpc server: Close rejected from an admitted handler task");
        }
    }
    const auto reason = SettleCloseReason(options.reason.value_or(Error{}), "rpc server: closed by owner");
    if (options.mode == CloseMode::Immediate) {
        ServerSeal(state, reason);
        return bbt::infra::detail::FinishCloseWithRelease(
            options, [state](bbt::coroutine::Deadline deadline) { return WaitServerReleased(state, deadline); });
    }
    bool already_closed = false;
    {
        std::lock_guard<std::mutex> lock(state->mu);
        if (state->closed) {
            if (!state->DrainedLocked()) {
                return result<void>::err(
                    state->close_reason ? *state->close_reason : Err(ErrorCode::Closed, "rpc server: closed"));
            }
            already_closed = true;
        }
        if (!already_closed) {
            if (!state->close_reason) {
                state->close_reason = reason;
            }
            if (!state->draining) {
                state->draining = true;

                /**
                 * 与停止接纳同一封口点：新握手在 Seal 前到达也必须退役。
                 */
                state->stopping = true;
                state->close_signal->Notify();
            }
        }
    }
    if (already_closed) {
        return bbt::infra::detail::FinishCloseWithRelease(
            options, [state](bbt::coroutine::Deadline deadline) { return WaitServerReleased(state, deadline); });
    }

    /**
     * 拒新：stopping + admission 封口；原因已在同门发布。
     */
    ServerStopAccepting(state);
    if (const auto drained = ServerWaitDrained(state, options.deadline); !drained) {
        return drained;
    }
    ServerSeal(state, reason);
    return bbt::infra::detail::FinishCloseWithRelease(
        options, [state](bbt::coroutine::Deadline deadline) { return WaitServerReleased(state, deadline); });
}

result<void> detail::ServerWaitStopped(const std::shared_ptr<ServerState>& state, bbt::coroutine::Deadline deadline)
{
    if (!bbt::coroutine::IsInCoroutineContext()) {
        return ErrV(ErrorCode::InvalidContext, "rpc server: WaitStopped requires coroutine context");
    }
    const auto self = bbt::coroutine::GetLocalCoroutineId();
    {
        std::lock_guard<std::mutex> lock(state->mu);

        /**
         * 自等待拒绝先于封口状态判断：已授权 handler 任务不能等自己回收。
         */
        if (self != 0 && state->responsibility_tasks.find(self) != state->responsibility_tasks.end()) {
            return ErrV(ErrorCode::InvalidContext, "rpc server: WaitStopped rejected from an admitted handler task");
        }
        if (!state->closed) {
            return ErrV(ErrorCode::InvalidArgument, "rpc server: close before WaitStopped");
        }
    }
    return WaitServerReleased(state, deadline);
}

bool detail::ServerClosed(const ServerState& state) noexcept
{
    std::lock_guard<std::mutex> lock(state.mu);

    /**
     * 逻辑封口：优雅排空期间已拒新
     */
    return state.closed || state.draining;
}

std::optional<Error> detail::ServerCloseReason(const ServerState& state)
{
    std::lock_guard<std::mutex> lock(state.mu);
    return state.close_reason;
}

// clang-format off
}
// clang-format on
