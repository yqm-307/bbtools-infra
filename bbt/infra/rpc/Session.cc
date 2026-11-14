/**
 * RPC v2 会话层生命周期实现：SessionOwnerState 台账、ServerV2State 装配、
 * accept/路由/admission 分发与统一 Close（Immediate/Timeout/Graceful）。
 * 单 link 的收发缓存、拆包、pending、握手进度、顺序与关闭状态收敛在
 * detail/RpcSession.cc 的 SessionCore；Channel/帧协议接缝无状态，见
 * detail/RpcChannel.hpp 与 detail/RpcFrameProtocol.hpp。
 *
 * 公共 API（CoRpcClient/CoRpcServer）本体在 CoRpcClient.cc/CoRpcServer.cc；
 * admission 公共类型的 impl 在 detail/AdmissionImpl.cc；
 * 检查点在 detail/SessionHooks.cc。
 */
#include <bbt/infra/rpc/CoRpcClient.hpp>
#include <bbt/infra/rpc/CoRpcServer.hpp>
#include <bbt/infra/rpc/detail/Session.hpp>
#include <bbt/infra/rpc/detail/RpcSessionState.hpp>

#include <algorithm>
#include <atomic>
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
using bbt::coroutine::WaitStatus;

namespace {

/**
 * 测试注入（detail::ServerV2TestAccess）：只影响下一次 ServerV2Start，产品
 * 路径默认不触发。注册失败注入在 accept 任务登记处消费；barrier 在
 * 「已认领启动权、尚未创建队列/登记任务」的无锁窗口执行一次。
 */
std::atomic<bool> g_fail_next_start_registration{false};
std::mutex g_start_barrier_mu;
std::function<void()> g_start_barrier;

bool ConsumeStartRegistrationFailureForTest()
{
    return g_fail_next_start_registration.exchange(false, std::memory_order_acq_rel);
}

std::function<void()> TakeStartRegistrationBarrierForTest()
{
    std::lock_guard<std::mutex> lock(g_start_barrier_mu);
    return std::move(g_start_barrier);
}

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
    route.tokens = std::min(
        static_cast<double>(route.options.burst_tokens), route.tokens + elapsed * route.options.refill_per_second);
    route.last_refill = now;
    if (route.tokens < 1.0) {
        return false;
    }
    route.tokens -= 1.0;
    return true;
}

/**
 * 已授权路由 handler 的责任标记：以协程 id 登记，Close/WaitStopped 据此
 * 在副作用前拒绝自等待；作用域结束（含异常）移除。
 */
class RouteResponsibility {
public:
    explicit RouteResponsibility(std::shared_ptr<ServerV2State> server)
        : m_server(std::move(server))
    {
        m_id = bbt::coroutine::GetLocalCoroutineId();
        if (m_id != 0) {
            std::lock_guard<std::mutex> lock(m_server->mu);
            m_server->responsibility_tasks.insert(m_id);
        }
    }

    ~RouteResponsibility()
    {
        if (m_id != 0) {
            std::lock_guard<std::mutex> lock(m_server->mu);
            m_server->responsibility_tasks.erase(m_id);
        }
    }

    RouteResponsibility(const RouteResponsibility&) = delete;
    RouteResponsibility& operator=(const RouteResponsibility&) = delete;

private:
    std::shared_ptr<ServerV2State> m_server;
    bbt::coroutine::detail::CoroutineId m_id{0};
};

void RunRoute(std::shared_ptr<ServerV2State> server, CoRpcServer::RpcHandler handler, AdmissionItem item)
{
    RouteResponsibility responsibility(std::move(server));
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

// clang-format off
}
// clang-format on

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
        RouteResponsibility responsibility(server);
        return fallback(std::move(item));
    }

    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        128 * 1024,
        [server, handler = std::move(route_handler), item = std::move(item)]() mutable {
            RunRoute(server, std::move(handler), std::move(item));
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

                /**
                 * exit_loop 自身取 server->mu——持锁内联调用会自死锁；
                 * 标记与唤醒直接在本临界区内联完成。
                 */
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

            /**
             * RequestStopAccepting/Seal 会取消在途 CoAccept：这是正常封口路径，
             * 不是 accept 故障，不得发布失败原因覆盖调用方的 Close/Shutdown。
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
            detail::ServerV2Seal(server, Err(ErrorCode::RuntimeUnavailable, "rpc server: accept loop failed"));
            exit_loop();
            return;
        }
        {
            std::lock_guard<std::mutex> lock(server->mu);

            /**
             * 握手 conn 与已发布 link 同占 MaxConnections——
             * 建立中不算免费额度。
             */
            if (server->stopping
                || server->links.size() + server->handshakes.size() >= server->options.MaxConnections()) {
                (void)accepted.connection.Close();
                continue;
            }

            /**
             * 握手责任早登记：conn.Id() 台账先于任务接管——Close/Seal
             * 能立即看到并 RequestClose 这条建立中连接（物理关闭责任）。
             */
            server->handshakes.emplace(accepted.connection.Raw().Id(), accepted.connection.Raw());
        }
        bool registered = false;
        std::weak_ptr<ServerV2State> weak = server;
        const auto conn_id = accepted.connection.Raw().Id();
        g_scheduler->RegistCoroutineTask(
            128 * 1024,
            [weak, conn = accepted.connection]() mutable {
                if (auto server = weak.lock()) {

                    /**
                     * 裸协程任务：异常逃逸即 terminate——隔离到本 conn
                     * 退役，不穿透 scheduler。
                     */
                    try {
                        detail::ServeInbound(server, detail::TcpChannel{conn});
                    } catch (...) {
                        (void)conn.Close();
                        std::lock_guard<std::mutex> lock(server->mu);
                        server->handshakes.erase(conn.Raw().Id());
                        server->close_signal->Notify();
                    }
                } else {
                    (void)conn.Close();
                }
            },
            registered);
        if (!registered) {
            (void)accepted.connection.Close();
            std::lock_guard<std::mutex> lock(server->mu);
            server->handshakes.erase(conn_id);
        }
        bbtco_yield;
    }
}

// clang-format off
}
// clang-format on

/**
 * v2 台账快照：links/handshakes/writes 三项在同一把 server 锁内取样。
 */
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

/**
 * detail 挂钩实现
 */
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
            for (auto& [id, session] : owner->by_identity) {
                (void)id;
                sessions.push_back(session);
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
 * 统一 Close：Immediate 立即 Seal；Timeout 先 Seal 再有界等待物理收口； Graceful 先拒新、等已接纳责任归还后 Seal。等待经各自 close_signal 分片 （单等待位 + 周期复查），谓词评估在状态锁内，等待在锁外。
 */
namespace {

/**
 * 关闭等待唯一分片实现见 bbt/infra/detail/CloseWait.hpp：每轮取
 * min(deadline, now+50ms)，AlreadyWaiting 让出 5ms 后复查谓词。
 */
using bbt::infra::detail::WaitCloseSlice;

/**
 * owner 逻辑排空等待（Graceful）：DrainedLocked 胜返回 ok；等待期间被
 * Immediate/RequestClose 强制封口（closed）时返回首个关闭原因；到 deadline
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

/**
 * owner 物理收口等待（Timeout）：Seal 之后等 by_identity/establishing 全部
 * 归还。conn 物理 Released 可能晚于退出责任归零，每次唤醒都在 owner->mu
 * 下重驱各 link 的 ReleasedLocked（幂等）——物理关闭凭据按次轮询推进。
 */
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
result<void> ServerWaitDrained(const std::shared_ptr<ServerV2State>& v2, bbt::coroutine::Deadline deadline)
{
    for (;;) {
        {
            std::lock_guard<std::mutex> lock(v2->mu);
            if (v2->closed) {
                return result<void>::err(
                    v2->close_reason ? *v2->close_reason : Err(ErrorCode::Closed, "rpc server: closed"));
            }
            if (v2->DrainedLocked()) {
                return result<void>::ok();
            }
        }
        if (deadline != bbt::coroutine::Deadline::max() && RpcClock::now() >= deadline) {
            return ErrV(ErrorCode::TimedOut, "graceful close deadline exceeded");
        }
        const auto slice = bbt::infra::detail::NextCloseSlice(deadline);
        if (const auto waited = WaitCloseSlice(v2->close_signal, slice); !waited) {
            return result<void>::err(waited.error());
        }
    }
}

/**
 * server 物理收口等待（Timeout）：Seal 之后等 StoppedLocked——listener/
 * accept task、握手台账、session 退出责任与 conn Released 全部完成。
 * 同样每片重驱各 link 的 ReleasedLocked（幂等）。
 */
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
        const auto slice = bbt::infra::detail::NextCloseSlice(deadline);
        if (const auto waited = WaitCloseSlice(v2->close_signal, slice); !waited) {
            return result<void>::err(waited.error());
        }
    }
}

// clang-format off
}
// clang-format on

result<std::shared_ptr<ServerV2State>> detail::ServerV2Create(
    std::shared_ptr<net::Engine> engine,
    const std::shared_ptr<SessionOwnerState>& owner,
    AdmissionHandler admission_handler,
    ServerOptions options)
{
    using R = result<std::shared_ptr<ServerV2State>>;
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

    /**
     * 测试注入在入口取得：任何早退都会丢弃它，不会留下引用已销毁测试
     * 对象的待执行 barrier。
     */
    auto test_barrier = TakeStartRegistrationBarrierForTest();
    {
        std::lock_guard<std::mutex> lock(v2->mu);
        if (v2->closed || v2->draining) {
            return ErrV(ErrorCode::Closed, "server closed");
        }
        if (v2->started) {
            return result<void>::ok();
        }
        if (v2->starting) {

            /**
             * 同一 Server 只允许一个 Start 在途；并发重复启动不静默成功。
             */
            return ErrV(ErrorCode::InvalidContext, "server start already in progress");
        }
        if (v2->engine->Phase() != net::EnginePhase::Running) {
            return ErrV(ErrorCode::InvalidContext, "engine must be Running with listener");
        }

        /**
         * 只在本临界区认领启动权：AdmissionQueue::Create 与两次协程任务
         * 注册都可能因栈池未命中在 Co 域挂起，不能持 v2->mu 完成（Close/
         * Seal、Stats 与收口观察都用这把锁）。
         */
        v2->starting = true;
    }

    if (test_barrier) {
        test_barrier();
    }

    /**
     * 单 FIFO admission worker。
     */
    auto created = AdmissionQueue::Create(v2->options.MaxInflight(), v2->options.MaxHandlerBytes(), v2->handler);
    if (!created) {
        std::lock_guard<std::mutex> lock(v2->mu);
        v2->starting = false;
        v2->close_signal->Notify();
        return result<void>::err(created.error());
    }
    std::shared_ptr<AdmissionQueue> queue = std::move(created).value();
    detail::AdmissionAccess::AttachQueue(queue, v2);

    bool registered = false;
    if (!ConsumeStartRegistrationFailureForTest()) {
        g_scheduler->RegistCoroutineTask(
            128 * 1024,
            [v2] {
                // clang-format off

                /**
                 * 裸协程任务：异常逃逸即 terminate——兜底封 server。
                 */
                // clang-format on
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
    }

    if (registered) {
        bool sealed = false;
        {
            std::lock_guard<std::mutex> lock(v2->mu);
            v2->starting = false;
            v2->queue = queue;

            /**
             * accept 任务已登记：即使同时被 Close/Seal 封口，物理收口也要
             * 等该任务退出，不能按「从未 Start」放行。
             */
            v2->started = true;

            /**
             * Graceful Close 先置 draining 再 Seal；两者都与「已封口」同义，
             * 不能让 Start 在排空窗口里仍报成功。
             */
            sealed = v2->closed || v2->draining;
            v2->close_signal->Notify();
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
        std::lock_guard<std::mutex> lock(v2->mu);
        v2->starting = false;
        v2->close_signal->Notify();
    }
    (void)queue->Close();
    return ErrV(ErrorCode::RuntimeUnavailable, "accept task registration failed");
}

void detail::ServerV2TestAccess::FailNextStartRegistration() noexcept
{
    g_fail_next_start_registration.store(true, std::memory_order_release);
}

void detail::ServerV2TestAccess::SetStartRegistrationBarrierOnce(std::function<void()> barrier)
{
    std::lock_guard<std::mutex> lock(g_start_barrier_mu);
    g_start_barrier = std::move(barrier);
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
        (void)queue->Close();
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

        /**
         * 建立中握手 conn 同样是关闭责任：RequestClose 让握手 Read 尽快
         * 失败退出；台账条目由 ServeInbound 结束时移除（物理关闭责任）。
         */
        for (auto& [id, conn] : v2->handshakes) {
            (void)id;
            handshakes.push_back(conn);
        }
        v2->close_signal->Notify();
    }
    v2->engine->RequestStopAccepting();
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
             * 首因在封口同门发布；后续 Immediate/RequestClose 只升级强制收口。
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

result<void>
detail::SessionOwnerWaitStopped(const std::shared_ptr<SessionOwnerState>& owner, bbt::coroutine::Deadline deadline)
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

result<void> detail::ServerV2Close(const std::shared_ptr<ServerV2State>& v2, const CloseOptions& options)
{
    if (const auto valid = bbt::infra::detail::ValidateCloseOptions(options); !valid) {
        return result<void>::err(valid.error());
    }
    if ((options.mode == CloseMode::Graceful || options.wait_release) && !bbt::coroutine::IsInCoroutineContext()) {
        return ErrV(ErrorCode::InvalidContext, "rpc server: Close requires coroutine context");
    }
    const auto self = bbt::coroutine::GetLocalCoroutineId();
    {
        std::lock_guard<std::mutex> lock(v2->mu);
        if ((options.mode == CloseMode::Graceful || options.wait_release) && self != 0
            && v2->responsibility_tasks.find(self) != v2->responsibility_tasks.end()) {
            return ErrV(ErrorCode::InvalidContext, "rpc server: Close rejected from an admitted handler task");
        }
    }
    const auto reason = SettleCloseReason(options.reason.value_or(Error{}), "rpc server: closed by owner");
    if (options.mode == CloseMode::Immediate) {
        ServerV2Seal(v2, reason);
        return bbt::infra::detail::FinishCloseWithRelease(
            options, [v2](bbt::coroutine::Deadline deadline) { return ServerV2WaitStopped(v2, deadline); });
    }
    bool already_closed = false;
    {
        std::lock_guard<std::mutex> lock(v2->mu);
        if (v2->closed) {
            if (!v2->DrainedLocked()) {
                return result<void>::err(
                    v2->close_reason ? *v2->close_reason : Err(ErrorCode::Closed, "rpc server: closed"));
            }
            already_closed = true;
        }
        if (!already_closed) {
            if (!v2->close_reason) {
                v2->close_reason = reason;
            }
            if (!v2->draining) {
                v2->draining = true;

                /**
                 * 与停止接纳同一封口点：新握手在 Seal 前到达也必须退役。
                 */
                v2->stopping = true;
                v2->close_signal->Notify();
            }
        }
    }
    if (already_closed) {
        return bbt::infra::detail::FinishCloseWithRelease(
            options, [v2](bbt::coroutine::Deadline deadline) { return ServerV2WaitStopped(v2, deadline); });
    }

    /**
     * 拒新：stopping + admission 封口；原因已在同门发布。
     */
    ServerV2StopAccepting(v2);
    if (const auto drained = ServerWaitDrained(v2, options.deadline); !drained) {
        return drained;
    }
    ServerV2Seal(v2, reason);
    return bbt::infra::detail::FinishCloseWithRelease(
        options, [v2](bbt::coroutine::Deadline deadline) { return ServerV2WaitStopped(v2, deadline); });
}

result<void> detail::ServerV2WaitStopped(const std::shared_ptr<ServerV2State>& v2, bbt::coroutine::Deadline deadline)
{
    if (!bbt::coroutine::IsInCoroutineContext()) {
        return ErrV(ErrorCode::InvalidContext, "rpc server: WaitStopped requires coroutine context");
    }
    const auto self = bbt::coroutine::GetLocalCoroutineId();
    {
        std::lock_guard<std::mutex> lock(v2->mu);

        /**
         * 自等待拒绝先于封口状态判断：已授权 handler 任务不能等自己回收。
         */
        if (self != 0 && v2->responsibility_tasks.find(self) != v2->responsibility_tasks.end()) {
            return ErrV(ErrorCode::InvalidContext, "rpc server: WaitStopped rejected from an admitted handler task");
        }
        if (!v2->closed) {
            return ErrV(ErrorCode::InvalidArgument, "rpc server: close before WaitStopped");
        }
    }
    return ServerWaitStopped(v2, deadline);
}

bool detail::ServerV2Closed(const ServerV2State& v2) noexcept
{
    std::lock_guard<std::mutex> lock(v2.mu);

    /**
     * 逻辑封口：优雅排空期间已拒新
     */
    return v2.closed || v2.draining;
}

std::optional<Error> detail::ServerV2CloseReason(const ServerV2State& v2)
{
    std::lock_guard<std::mutex> lock(v2.mu);
    return v2.close_reason;
}

// clang-format off
}
// clang-format on
