// RPC v2 会话层生命周期实现：SessionOwnerState 台账、ServerV2State 装配、
// accept/路由/admission 分发与统一 Close（Immediate/Timeout/Graceful）。
// 单 link 的收发缓存、拆包、pending、握手进度、顺序与关闭状态收敛在
// detail/RpcSession.cc 的 SessionCore；Channel/帧协议接缝无状态，见
// detail/RpcChannel.hpp 与 detail/RpcFrameProtocol.hpp。
//
// 公共 API（CoRpcClient/CoRpcServer）本体在 CoRpcClient.cc/CoRpcServer.cc；
// admission 公共类型的 impl 在 detail/AdmissionImpl.cc；
// 检查点在 detail/SessionHooks.cc。

#include <bbt/infra/rpc/CoRpcClient.hpp>
#include <bbt/infra/rpc/CoRpcServer.hpp>
#include <bbt/infra/rpc/detail/Session.hpp>
#include <bbt/infra/rpc/detail/RpcSessionState.hpp>

#include <algorithm>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <bbt/coroutine/syntax/SyntaxMacro.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>

namespace bbt::infra::rpc {

using namespace std::chrono_literals;
using bbt::coroutine::WaitStatus;

namespace {

std::string RouteKey(const Request& request)
{
    std::string key = request.Service();
    key.push_back('/');
    key.append(request.Method());
    return key;
}

bool ConsumeRouteToken(ServerV2State::Route& route)
{
    if (route.options.burst_tokens == 0) {
        return true;
    }
    const auto now = RpcClock::now();
    const auto elapsed = std::chrono::duration<double>(now - route.last_refill).count();
    route.tokens = std::min(static_cast<double>(route.options.burst_tokens),
                            route.tokens + elapsed * route.options.refill_per_second);
    route.last_refill = now;
    if (route.tokens < 1.0) {
        return false;
    }
    route.tokens -= 1.0;
    return true;
}

void RunRoute(CoRpcServer::RpcHandler handler, AdmissionItem item)
{
    try {
        auto response = bbt_co_await handler(std::move(item.request));
        if (item.reply) {
            if (response) {
                const auto completion = bbt_co_await item.reply->Complete(std::move(response).value());
                (void)completion;
            } else {
                const auto completion = bbt_co_await item.reply->Fail(response.error());
                (void)completion;
            }
        }
    } catch (...) {
        if (item.reply) {
            const auto completion =
                bbt_co_await item.reply->Fail(Err(ErrorCode::InternalError, "rpc route handler threw"));
            (void)completion;
        }
    }
    if (item.lease) {
        item.lease->Release();
    }
}

} // namespace

result<void> detail::DispatchAdmission(const std::shared_ptr<ServerV2State>& server, AdmissionItem item)
{
    CoRpcServer::RpcHandler route_handler;
    AdmissionHandler fallback;
    {
        std::lock_guard<std::mutex> lock(server->mu);
        const auto it = server->routes.find(RouteKey(item.request));
        if (it == server->routes.end()) {
            fallback = server->framework_handler;
        } else {
            if (item.request.Reply() != ReplyMode::Required) {
                return ErrV(ErrorCode::InvalidArgument, "registered rpc route requires a Call request");
            }
            if (!ConsumeRouteToken(it->second)) {
                return ErrV(ErrorCode::Overloaded, "rpc route rate limit exceeded");
            }
            route_handler = it->second.handler;
        }
    }
    if (!route_handler) {
        if (!fallback) {
            return ErrV(ErrorCode::Unavailable, "rpc server has no admission handler");
        }
        return fallback(std::move(item));
    }

    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        128 * 1024,
        [handler = std::move(route_handler), item = std::move(item)]() mutable {
            RunRoute(std::move(handler), std::move(item));
        },
        registered);
    if (!registered) {
        return ErrV(ErrorCode::RuntimeUnavailable, "rpc route task registration failed");
    }
    return result<void>::ok();
}

namespace {

void AcceptLoop(std::shared_ptr<ServerV2State> server)
{
    const auto exit_loop = [&] {
        std::lock_guard<std::mutex> lock(server->mu);
        server->accept_loop_exited = true;
        server->close_signal->Notify();
    };
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(server->mu);
            if (server->stopping) {
                // exit_loop 自身取 server->mu——持锁内联调用会自死锁；
                // 标记与唤醒直接在本临界区内联完成。
                server->accept_loop_exited = true;
                server->close_signal->Notify();
                return;
            }
        }
        auto accepted = server->engine->CoAccept({RpcClock::now() + 50ms});
        if (accepted.error == std::errc::timed_out) {
            continue;
        }
        if (accepted.error) {
            // RequestStopAccepting/Seal 会取消在途 CoAccept：这是正常封口路径，
            // 不是 accept 故障，不得发布失败原因覆盖调用方的 Close/Shutdown。
            bool sealed = false;
            {
                std::lock_guard<std::mutex> lock(server->mu);
                sealed = server->stopping;
            }
            if (sealed) {
                exit_loop();
                return;
            }
            detail::ServerV2Seal(server, Err(ErrorCode::RuntimeUnavailable, "rpc server: accept loop failed"));
            exit_loop();
            return;
        }
        {
            std::lock_guard<std::mutex> lock(server->mu);
            // 握手 conn 与已发布 link 同占 MaxConnections——
            // 建立中不算免费额度。
            if (server->stopping
                || server->links.size() + server->handshakes.size() >= server->options.MaxConnections()) {
                accepted.connection.RequestClose();
                continue;
            }
            // 握手责任早登记：conn.Id() 台账先于任务接管——Close/Seal
            // 能立即看到并 RequestClose 这条建立中连接（物理关闭责任）。
            server->handshakes.emplace(accepted.connection.Raw().Id(), accepted.connection.Raw());
        }
        bool registered = false;
        std::weak_ptr<ServerV2State> weak = server;
        const auto conn_id = accepted.connection.Raw().Id();
        g_scheduler->RegistCoroutineTask(
            128 * 1024,
            [weak, conn = accepted.connection]() mutable {
                if (auto server = weak.lock()) {
                    // 裸协程任务：异常逃逸即 terminate——隔离到本 conn
                    // 退役，不穿透 scheduler。
                    try {
                        detail::ServeInbound(server, detail::TcpChannel{conn});
                    } catch (...) {
                        conn.RequestClose();
                        std::lock_guard<std::mutex> lock(server->mu);
                        server->handshakes.erase(conn.Raw().Id());
                        server->close_signal->Notify();
                    }
                } else {
                    conn.RequestClose();
                }
            },
            registered);
        if (!registered) {
            accepted.connection.RequestClose();
            std::lock_guard<std::mutex> lock(server->mu);
            server->handshakes.erase(conn_id);
        }
        bbtco_yield;
    }
}

} // namespace

// v2 台账快照：links/handshakes/writes 三项在同一把 server 锁内取样。
detail::ServerV2Ledger detail::ServerV2LedgerOf(const std::shared_ptr<ServerV2State>& v2) noexcept
{
    detail::ServerV2Ledger out;
    if (!v2) {
        return out;
    }
    std::lock_guard<std::mutex> lock(v2->mu);
    for (const auto& weak : v2->links) {
        auto session = weak.lock();
        if (!session) {
            continue;
        }
        ++out.links;
        out.writes_outstanding += session->writes_outstanding.load();
    }
    out.handshakes = v2->handshakes.size();
    return out;
}

// ============================================================================
// detail 挂钩实现
// ============================================================================

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
            // 「失败在途」恰好一次：closed 位唯一表达，Draining 下同样执行。
            owner->closed = true;
            for (auto& [id, session] : owner->by_identity) {
                (void)id;
                sessions.push_back(session);
            }
            // 建立中出站握手 conn 同受关闭责任：RequestClose 让握手尽快
            // 失败退出，不等调用 deadline（物理关闭责任）。
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
        conn.RequestClose(std::make_error_code(std::errc::connection_aborted));
    }
    // 控制器永久 closed：无论本次是否首次封口都置位，保证 Close 返回后 Arm
    // 一定看到 Closed；在途 Pause 由 runtime 的 owner.closed 检查退出。锁外
    // 单独取 hook 锁，不与 owner.mu 形成嵌套锁序。
    if (owner->hooks) {
        detail::SessionHookAccess::Close(*owner->hooks);
    }
}

SessionOwnerState::~SessionOwnerState()
{
    // 兜底：owner 未经 Seal 直接销毁时同样封口控制器。handle 可能比 owner
    // 活得久，但只保留有界控制状态，不再接受注册。
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
    return owner.closed;
}

std::optional<Error> detail::SessionOwnerCloseReason(const SessionOwnerState& owner)
{
    std::lock_guard<std::mutex> lock(owner.mu);
    return owner.close_reason;
}

bool detail::SessionOwnerBeginOperation(const std::shared_ptr<SessionOwnerState>& owner, Error& closed_error)
{
    std::lock_guard<std::mutex> lock(owner->mu);
    // 非 Open 即拒绝：Close 原因优先，否则通用 Closed。
    if (owner->closed || owner->draining) {
        closed_error = owner->close_reason ? *owner->close_reason : Err(ErrorCode::Closed, "rpc client: closed");
        return false;
    }
    ++owner->active_ops;
    return true;
}

void detail::SessionOwnerEndOperation(const std::shared_ptr<SessionOwnerState>& owner) noexcept
{
    std::lock_guard<std::mutex> lock(owner->mu);
    if (owner->active_ops != 0) {
        --owner->active_ops;
    }
    owner->close_signal->Notify();
}

// ============================================================================
// 统一 Close：Immediate 立即 Seal；Timeout 先 Seal 再有界等待物理收口；
// Graceful 先拒新、等已接纳责任归还后 Seal。等待经各自 close_signal 分片
// （单等待位 + 周期复查），谓词评估在状态锁内，等待在锁外。
// ============================================================================

namespace {

// close_signal 一次分片等待：谓词满足返回 true；其它状态如实映射。
// AlreadyWaiting 表示另有协程在等待同一收口——本片按未命中处理（下一片
// 复查谓词），不报 AlreadyWaiting 类错误。
result<bool> WaitCloseSlice(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& signal,
                            bbt::coroutine::Deadline deadline)
{
    using bbt::coroutine::WaitStatus;
    const auto status = signal->Wait({deadline});
    switch (status) {
    case WaitStatus::Completed:
        return result<bool>::ok(true);
    case WaitStatus::TimedOut:
        return result<bool>::ok(false);
    case WaitStatus::AlreadyWaiting:
        bbtco_sleep(5);
        return result<bool>::ok(false);
    case WaitStatus::Cancelled:
        return result<bool>::err(Err(ErrorCode::Cancelled, "close wait cancelled"));
    case WaitStatus::InvalidContext:
        return result<bool>::err(Err(ErrorCode::InvalidContext, "close wait requires coroutine context"));
    case WaitStatus::RuntimeUnavailable:
        return result<bool>::err(Err(ErrorCode::RuntimeUnavailable, "close wait unavailable"));
    }
    return result<bool>::err(Err(ErrorCode::RuntimeUnavailable, "close wait unavailable"));
}

// owner 逻辑排空等待（Graceful）：DrainedLocked 胜返回 ok；等待期间被
// 其它 Close/Shutdown 打断（已发布原因）返回该原因；到 deadline 返回
// TimedOut。以时间片复查而不是完全依赖唤醒——close_signal 是单等待位。
result<void> OwnerWaitDrained(const std::shared_ptr<SessionOwnerState>& owner, bbt::coroutine::Deadline deadline)
{
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(owner->mu);
            if (owner->close_reason) {
                return result<void>::err(*owner->close_reason);
            }
            if (owner->DrainedLocked()) {
                return result<void>::ok();
            }
        }
        if (deadline != bbt::coroutine::Deadline::max() && RpcClock::now() >= deadline) {
            return ErrV(ErrorCode::TimedOut, "graceful close deadline exceeded");
        }
        const auto slice = deadline == bbt::coroutine::Deadline::max() ? RpcClock::now() + 50ms
                                                                       : std::min(deadline, RpcClock::now() + 50ms);
        if (const auto waited = WaitCloseSlice(owner->close_signal, slice); !waited) {
            return result<void>::err(waited.error());
        }
    }
}

// owner 物理收口等待（Timeout）：Seal 之后等 by_identity/establishing 全部
// 归还。conn 物理 Released 可能晚于退出责任归零，每次唤醒都在 owner->mu
// 下重驱各 link 的 ReleasedLocked（幂等）——物理关闭凭据按次轮询推进。
result<void> OwnerWaitSettled(const std::shared_ptr<SessionOwnerState>& owner, bbt::coroutine::Deadline deadline)
{
    for (;;) {
        std::vector<std::shared_ptr<SessionCore>> live;
        {
            std::lock_guard<std::mutex> lock(owner->mu);
            if (owner->TeardownLocked()) {
                return result<void>::ok();
            }
            for (auto& [id, session] : owner->by_identity) {
                (void)id;
                live.push_back(session);
            }
        }
        for (auto& session : live) {
            std::lock_guard<std::mutex> lock(owner->mu);
            session->DriveReleaseLocked();
        }
        if (deadline != bbt::coroutine::Deadline::max() && RpcClock::now() >= deadline) {
            return ErrV(ErrorCode::TimedOut, "timeout close: resource release incomplete");
        }
        const auto slice = deadline == bbt::coroutine::Deadline::max() ? RpcClock::now() + 50ms
                                                                       : std::min(deadline, RpcClock::now() + 50ms);
        if (const auto waited = WaitCloseSlice(owner->close_signal, slice); !waited) {
            return result<void>::err(waited.error());
        }
    }
}

// server 逻辑排空等待（Graceful）：DrainedLocked 胜返回 ok；被打断返回已
// 发布原因；到 deadline 返回 TimedOut。
result<void> ServerWaitDrained(const std::shared_ptr<ServerV2State>& v2, bbt::coroutine::Deadline deadline)
{
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(v2->mu);
            if (v2->close_reason) {
                return result<void>::err(*v2->close_reason);
            }
            if (v2->DrainedLocked()) {
                return result<void>::ok();
            }
        }
        if (deadline != bbt::coroutine::Deadline::max() && RpcClock::now() >= deadline) {
            return ErrV(ErrorCode::TimedOut, "graceful close deadline exceeded");
        }
        const auto slice = deadline == bbt::coroutine::Deadline::max() ? RpcClock::now() + 50ms
                                                                       : std::min(deadline, RpcClock::now() + 50ms);
        if (const auto waited = WaitCloseSlice(v2->close_signal, slice); !waited) {
            return result<void>::err(waited.error());
        }
    }
}

// server 物理收口等待（Timeout）：Seal 之后等 StoppedLocked——listener/
// accept task、握手台账、session 退出责任与 conn Released 全部完成。
// 同样每片重驱各 link 的 ReleasedLocked（幂等）。
result<void> ServerWaitStopped(const std::shared_ptr<ServerV2State>& v2, bbt::coroutine::Deadline deadline)
{
    for (;;) {
        std::vector<std::shared_ptr<SessionCore>> live;
        {
            std::lock_guard<std::mutex> lock(v2->mu);
            if (v2->StoppedLocked()) {
                return result<void>::ok();
            }
            for (auto& weak : v2->links) {
                if (auto session = weak.lock()) {
                    live.push_back(std::move(session));
                }
            }
        }
        if (!live.empty()) {
            std::lock_guard<std::mutex> lock(v2->owner->mu);
            for (auto& session : live) {
                session->DriveReleaseLocked();
            }
        }
        if (deadline != bbt::coroutine::Deadline::max() && RpcClock::now() >= deadline) {
            return ErrV(ErrorCode::TimedOut, "timeout close: resource release incomplete");
        }
        const auto slice = deadline == bbt::coroutine::Deadline::max() ? RpcClock::now() + 50ms
                                                                       : std::min(deadline, RpcClock::now() + 50ms);
        if (const auto waited = WaitCloseSlice(v2->close_signal, slice); !waited) {
            return result<void>::err(waited.error());
        }
    }
}

} // namespace

result<std::shared_ptr<ServerV2State>> detail::ServerV2Create(std::shared_ptr<net::Engine> engine,
                                                              const std::shared_ptr<SessionOwnerState>& owner,
                                                              AdmissionHandler admission_handler, ServerOptions options)
{
    using R = result<std::shared_ptr<ServerV2State>>;
    if (!engine || !owner || !admission_handler) {
        return R::err(Err(ErrorCode::InvalidArgument, "engine/session_owner/handler required"));
    }
    if (auto valid = options.Validate(); !valid) {
        return R::err(valid.error());
    }
    // shared-Engine 装配边界：accept/收口与 owner 出站拨号、台账、关闭必须
    // 由同一宿主推进；跨 Engine 注入的 owner 拒绝在 accept-handoff 之前。
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
    auto v2 = std::make_shared<ServerV2State>();
    v2->engine = std::move(engine);
    v2->owner = owner;
    v2->framework_handler = std::move(admission_handler);
    v2->options = options;
    const std::weak_ptr<ServerV2State> weak_v2 = v2;
    v2->handler = [weak_v2](AdmissionItem item) -> result<void> {
        const auto server = weak_v2.lock();
        if (!server) {
            return ErrV(ErrorCode::Closed, "rpc server is gone");
        }
        return detail::DispatchAdmission(server, std::move(item));
    };
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        owner->inbound_server = v2;
    }
    return R::ok(std::move(v2));
}
result<void> detail::ServerV2Start(const std::shared_ptr<ServerV2State>& v2)
{
    if (!NativeHost()) {
        return ErrV(ErrorCode::InvalidContext, "Start requires native host context");
    }
    std::lock_guard<std::mutex> lock(v2->mu);
    if (v2->closed) {
        return ErrV(ErrorCode::Closed, "server closed");
    }
    if (v2->started) {
        return result<void>::ok();
    }
    if (v2->engine->Phase() != net::EnginePhase::Running) {
        return ErrV(ErrorCode::InvalidContext, "engine must be Running with listener");
    }
    // 单 FIFO admission worker。
    auto queue = AdmissionQueue::Create(v2->options.MaxInflight(), v2->options.MaxHandlerBytes(), v2->handler);
    if (!queue) {
        return result<void>::err(queue.error());
    }
    detail::AdmissionAccess::AttachQueue(queue.value(), v2);
    v2->queue = std::move(queue.value());
    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        128 * 1024,
        [v2] {
            // 裸协程任务：异常逃逸即 terminate——兜底封 server。
            try {
                AcceptLoop(v2);
            } catch (...) {
                detail::ServerV2Seal(v2, Err(ErrorCode::InternalError, "rpc server: accept loop threw"));
                std::lock_guard<std::mutex> lock(v2->mu);
                v2->accept_loop_exited = true;
                v2->close_signal->Notify();
            }
        },
        registered);
    if (!registered) {
        v2->queue->Close();
        return ErrV(ErrorCode::RuntimeUnavailable, "accept task registration failed");
    }
    v2->started = true;
    return result<void>::ok();
}

void detail::ServerV2StopAccepting(const std::shared_ptr<ServerV2State>& v2) noexcept
{
    std::shared_ptr<AdmissionQueue> queue;
    {
        std::lock_guard<std::mutex> lock(v2->mu);
        v2->stopping = true;
        queue = v2->queue;
        v2->close_signal->Notify();
    }
    v2->engine->RequestStopAccepting();
    if (queue) {
        queue->Close();
    }
}

void detail::ServerV2Seal(const std::shared_ptr<ServerV2State>& v2, Error reason) noexcept
{
    std::vector<std::shared_ptr<SessionCore>> links;
    std::vector<net::Connection> handshakes;
    std::shared_ptr<AdmissionQueue> queue;
    {
        std::lock_guard<std::mutex> lock(v2->mu);
        if (!v2->close_reason) {
            v2->close_reason = std::move(reason);
        }
        if (v2->closed) {
            return;
        }
        v2->closed = true;
        v2->stopping = true;
        queue = v2->queue;
        for (auto& weak : v2->links) {
            if (auto session = weak.lock()) {
                links.push_back(session);
            }
        }
        // 建立中握手 conn 同样是关闭责任：RequestClose 让握手 Read 尽快
        // 失败退出；台账条目由 ServeInbound 结束时移除（物理关闭责任）。
        for (auto& [id, conn] : v2->handshakes) {
            (void)id;
            handshakes.push_back(conn);
        }
        v2->close_signal->Notify();
    }
    v2->engine->RequestStopAccepting();
    if (queue) {
        queue->Close();
    }
    for (auto& session : links) {
        session->Kill({});
    }
    for (auto& conn : handshakes) {
        conn.RequestClose(std::make_error_code(std::errc::connection_aborted));
    }
}

result<void> detail::SessionOwnerClose(const std::shared_ptr<SessionOwnerState>& owner, CloseFlag flag,
                                       std::int64_t timeout_ms, Error reason)
{
    // 负 timeout 入口契约：仅 Close(Graceful, -1) 表示无限等待；其余负值
    // 一律拒绝，先于任何封口/等待动作——Immediate/Timeout 的负值不得
    // 退化成无界等待。
    if (timeout_ms < 0 && !(flag == CloseFlag::Graceful && timeout_ms == -1)) {
        return ErrV(ErrorCode::InvalidArgument, "negative timeout_ms is only allowed as Close(Graceful, -1)");
    }
    reason = SettleCloseReason(std::move(reason), "rpc client: closed by owner");
    if (flag == CloseFlag::Immediate) {
        SessionOwnerSeal(owner, std::move(reason));
        return result<void>::ok();
    }
    const auto deadline =
        timeout_ms < 0 ? bbt::coroutine::Deadline::max() : RpcClock::now() + std::chrono::milliseconds(timeout_ms);
    if (flag == CloseFlag::Timeout) {
        // 先强制收口，再有界等待物理释放；不等待业务 handler 优雅结束。
        SessionOwnerSeal(owner, std::move(reason));
        return OwnerWaitSettled(owner, deadline);
    }
    if (flag != CloseFlag::Graceful) {
        return ErrV(ErrorCode::InvalidArgument, "unknown close flag");
    }
    {
        std::lock_guard<std::mutex> lock(owner->mu);
        if (owner->close_reason) {
            return result<void>::err(*owner->close_reason);
        }
        if (owner->draining) {
            if (!owner->DrainedLocked()) {
                return ErrV(ErrorCode::Overloaded, "graceful close already in progress");
            }
        } else {
            // 拒新与 Call/Send 接纳同门（BeginOperation 读 draining），不占用
            // closed 位、不发布原因——被打断时首个原因才能如实传出。
            owner->draining = true;
            owner->close_signal->Notify();
        }
    }
    if (const auto drained = OwnerWaitDrained(owner, deadline); !drained) {
        // 有限期限到点或被中断：强制收口并把真实终态如实返回。
        if (drained.error().code == ErrorCode::TimedOut) {
            SessionOwnerSeal(owner, std::move(reason));
        }
        return drained;
    }
    SessionOwnerSeal(owner, std::move(reason));
    return result<void>::ok();
}

result<void> detail::ServerV2Close(const std::shared_ptr<ServerV2State>& v2, CloseFlag flag, std::int64_t timeout_ms,
                                   Error reason)
{
    // 与 SessionOwnerClose 同一入口契约：仅 Close(Graceful, -1) 无限等待；
    // 其他负值先返回 InvalidArgument，不启动无界等待。
    if (timeout_ms < 0 && !(flag == CloseFlag::Graceful && timeout_ms == -1)) {
        return ErrV(ErrorCode::InvalidArgument, "negative timeout_ms is only allowed as Close(Graceful, -1)");
    }
    reason = SettleCloseReason(std::move(reason), "rpc server: closed by owner");
    if (flag == CloseFlag::Immediate) {
        ServerV2Seal(v2, std::move(reason));
        return result<void>::ok();
    }
    const auto deadline =
        timeout_ms < 0 ? bbt::coroutine::Deadline::max() : RpcClock::now() + std::chrono::milliseconds(timeout_ms);
    if (flag == CloseFlag::Timeout) {
        ServerV2Seal(v2, std::move(reason));
        return ServerWaitStopped(v2, deadline);
    }
    if (flag != CloseFlag::Graceful) {
        return ErrV(ErrorCode::InvalidArgument, "unknown close flag");
    }
    {
        std::lock_guard<std::mutex> lock(v2->mu);
        if (v2->close_reason) {
            return result<void>::err(*v2->close_reason);
        }
        if (v2->draining) {
            if (!v2->DrainedLocked()) {
                return ErrV(ErrorCode::Overloaded, "graceful close already in progress");
            }
        } else {
            v2->draining = true;
            v2->close_signal->Notify();
        }
    }
    // 拒新：stopping + admission 封口（不发布原因——被打断时首个原因如实传出）。
    ServerV2StopAccepting(v2);
    if (const auto drained = ServerWaitDrained(v2, deadline); !drained) {
        if (drained.error().code == ErrorCode::TimedOut) {
            ServerV2Seal(v2, std::move(reason));
        }
        return drained;
    }
    ServerV2Seal(v2, std::move(reason));
    return result<void>::ok();
}

bool detail::ServerV2Closed(const ServerV2State& v2) noexcept
{
    std::lock_guard<std::mutex> lock(v2.mu);
    return v2.closed;
}

std::optional<Error> detail::ServerV2CloseReason(const ServerV2State& v2)
{
    std::lock_guard<std::mutex> lock(v2.mu);
    return v2.close_reason;
}

} // namespace bbt::infra::rpc
