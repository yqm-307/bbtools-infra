/**
 * @file CoRpcServer.hpp
 * @brief 双向 RPC 会话入站服务端，与 CoRpcClient 共享会话 owner。
 *
 * 接纳、路由、回复与写责任由会话台账管理；上层提供同步 admission
 * handler 或可挂起的注册路由。Close 是唯一关闭入口（Immediate/Timeout/
 * Graceful 三态），Shutdown 是宿主侧同步强制关闭。Engine 的运行与收口
 * 归应用，本类不另建 session registry。
 */
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/rpc/Admission.hpp>
#include <bbt/infra/rpc/Options.hpp>
#include <bbt/infra/rpc/Stats.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>

namespace bbt::infra::net {
class Engine;
} // 只前向声明；net::Address 值类型
//（sockaddr 快照，无物理栈泄漏，leak-check 白名单）经 LocalAddress 返回需要
// 完整类型（上方 include）。

namespace bbt::infra::rpc {

class CoRpcClient;
class CoRpcServer;
struct ServerV2State; // detail：会话入站状态，定义见 detail/RpcSessionState.hpp

namespace detail {
// 完整定义见 detail/RpcSessionState.hpp。
struct ServerV2Ledger;
/// 测试接缝：读取会话入站台账快照（仅测试使用）。
ServerV2Ledger ServerV2LedgerForTest(const CoRpcServer& server) noexcept;
} // namespace detail

/**
 * @brief 双向 RPC 会话入站服务端。
 *
 * 装配时校验共享 Engine、CoRpcClient owner、admission handler 与 options，
 * 不监听、不建线程；由应用启动并最终收口。
 */
class CoRpcServer : public bbt::coroutine::CoObject {
public:
    /**
     * @brief 路由处理器：接收一个 owning Request，返回可挂起的 Response。
     *
     * Server 只负责路由、限流和回复提交；Mailbox、业务调度和 handler 的内部
     * 并发策略由上层负责。回调捕获对象的寿命由注册方保证。
     */
    using RpcHandler = std::function<bbt_co_async(result<Response>)(Request)>;

    /**
     * @brief 单路由令牌桶配置；两个字段都为零表示不启用限流。
     *
     * 启用时 burst_tokens 必须大于零，refill_per_second 必须为有限正数。
     */
    struct RouteOptions {
        std::size_t burst_tokens{0};
        double refill_per_second{0.0};
    };

    /**
     * @brief 双向会话 Server 装配。
     *
     * Engine 必须 Idle（Create 内预留 accept-handoff 容量），且必须与
     * session_owner 装配的 net::Engine 是同一实例——accept/收口与 owner 的
     * 出站拨号、session 台账、关闭由同一宿主推进；跨 Engine 装配返回
     * InvalidArgument。session_owner 必须非空且启用 trusted-network 明文
     * 模式，handler 非空，options.Validate() 通过。只装配，不监听、不建线程；
     * Engine 由应用 Run/收口。
     * Server 不另建 session registry——完成握手的入站 link 交给同一
     * session_owner（CoRpcClient owner），实现一条双向 link 的 Call/Send 复用。
     */
    static result<std::shared_ptr<CoRpcServer>> Create(std::shared_ptr<net::Engine> engine,
                                                       std::shared_ptr<CoRpcClient> session_owner,
                                                       AdmissionHandler admission_handler, ServerOptions options);

    /**
     * @brief 注册一条 service/method 路由。
     *
     * rpc_name 使用 `service/method` 形式。重复注册返回 InvalidArgument；
     * 运行中的 Server 允许更新其它路由，但不会影响已经取出 handler 的调用。
     * 已注册路由接收 `ReplyMode::Required` 的 Call；Send 继续交给 framework
     * admission handler。
     */
    result<void> RegisterRpc(std::string rpc_name, RpcHandler handler);
    result<void> RegisterRpc(std::string rpc_name, RpcHandler handler, RouteOptions options);
    /// @brief 删除一条路由；不存在时返回 NotFound。
    result<void> UnregisterRpc(std::string_view rpc_name);

    /// @brief 启动监听（native 宿主上下文；Engine 须 Running 且 scheduler 已启动）。
    result<void> Start();

    /**
     * @brief 统一关闭入口（受管协程内 bbt_co_await 消费）。
     *
     * 三种形态共用同一入口，不再暴露 StopAccepting/Drain/WaitStopped：
     *   - Close(Immediate, 0)：立即逻辑封口并发布首个关闭原因（幂等、不可
     *     重开）；停止接纳、admission 封口、在途 link 强制关闭，不等待。
     *   - Close(Timeout, timeout_ms)：先同上强制收口，再有界等待 listener/
     *     握手/session/写责任物理收口；超时返回 TimedOut。
     *   - Close(Graceful, timeout_ms)：先拒新，等 admission 队列、已授权
     *     reply/lease 与写责任真实归还后封口；timeout_ms==-1 表示无限等待，
     *     有限期限超时返回 TimedOut 并强制收口。
     * @param flag 关闭模式。
     * @param timeout_ms 毫秒；仅 Graceful 允许 -1（无限），其余负值返回
     *        InvalidArgument 且不启动等待。
     * @param reason 关闭原因；message 为空时使用默认 Closed 原因。
     */
    bbt_co_async(result<void>)
        Close(CloseFlag flag = CloseFlag::Immediate, std::int64_t timeout_ms = 0, Error reason = {});

    /**
     * @brief 宿主侧同步强制关闭（任意域、不挂起、幂等）。
     *
     * 立即拒绝新操作、停止接纳、强制关闭所有 session/link、未终结调用立即
     * 失败；不等待 handler、reply、队列或物理释放。与 Close(Immediate) 共用
     * 同一套关闭状态。
     * @param reason 关闭原因；message 为空时使用默认 Closed 原因。
     */
    void Shutdown(Error reason = {}) noexcept;

    /// @brief 查询本 Server 是否已逻辑封口。
    bool IsClosed() const noexcept;
    /// @brief 已发布的首个关闭原因；未发布为空。
    std::optional<Error> CloseReason() const noexcept;

    /**
     * @brief 实际绑定地址（Start() 成功后有效）。
     *
     * 动态端口返回内核分配的 net::Address；未启动或已不可观察时返回错误。
     */
    result<bbt::infra::net::Address> LocalAddress() const;

    /**
     * @brief 入站观测快照（会话 owner 汇总）。
     *
     * 逐 peer/epoch 观测缺口见 docs/decisions/0014-app-session.md。
     */
    ServerStats Stats() const noexcept;
    ~CoRpcServer() noexcept;
    CoRpcServer(const CoRpcServer&) = delete;
    CoRpcServer& operator=(const CoRpcServer&) = delete;

private:
    // 内部接缝读 m_v2：台账快照与 owner 访问都在 detail 下，公共 API 面
    // 不含它们。
    friend detail::ServerV2Ledger detail::ServerV2LedgerForTest(const CoRpcServer&) noexcept;

    explicit CoRpcServer(std::shared_ptr<ServerV2State> v2) noexcept;
    std::shared_ptr<ServerV2State> m_v2;
};

} // namespace bbt::infra::rpc
