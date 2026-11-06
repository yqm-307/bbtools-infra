/**
 * @file Server.hpp
 * @brief RPC core 入站服务端（只依赖 IRpcInboundTransport seam，不依赖物理栈）。
 *
 * 职责边界：Server 只做 wire 解析、deadline local-min（min(本请求期限,
 * now + remaining_budget_ms)）、handler 五字段关联校验与错误信封化；
 * 监听、会话/连接管理、协议安全与资源配额全部归 IRpcInboundTransport
 * 实现。RpcHandler 由上层提供，在受管协程中调用，
 * 生命周期由本 Server 强持有。HTTP adapter 见 bbt/infra/rpc/HttpTransport.hpp。
 *
 * 生命周期：Close(reason) 立即逻辑封口（幂等、不可
 * 重开）并发布关闭原因；Shutdown(deadline) 封口并等已接纳调用真实归还
 * 责任，超时/等待中被 Close 且有在途如实返回，重复调用仍 Draining 且有
 * 在途返回 Overloaded（Busy）。IsClosed() 观测本 Server 是否已
 * Close（会话 owner 装配下仅反映 Close）。StopAccepting/Drain/WaitStopped 是可选的
 * 排序工具，由调用方按自己的顺序与预算调用，不是 infra 强制的多阶段协议；业务任务的停止/等待
 * 由用户控制，infra 不新增 drain/receipt/多阶段关闭协议。
 */
#pragma once

#include <cstdint>
#include <memory>
#include <optional>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/rpc/Admission.hpp>
#include <bbt/infra/rpc/Stats.hpp>
#include <bbt/infra/rpc/Transport.hpp>

// 源兼容工厂（HTTP adapter 实现，见 HttpTransport.hpp）：完整签名需要
// bbt::infra::http/net 类型，这里仅前向声明。
namespace bbt::infra::http {
struct ServerOptions;
}
namespace bbt::infra::net {
class Engine;
} // 只前向声明；net::Address 值类型
//（sockaddr 快照，无物理栈泄漏，leak-check 白名单）经 LocalAddress 返回需要
// 完整类型（上方 include）。

namespace bbt::infra::rpc {

class Client;
class Server;
class ServerOptions;
struct ServerV2State; // detail：会话入站状态，仅 Session TU 有定义

namespace detail {
// 完整定义见 detail/Session.hpp。
struct ServerV2Ledger;
struct SessionOwnerAccess;
/// 测试接缝：读取会话入站台账快照（仅测试使用）。
ServerV2Ledger ServerV2LedgerForTest(const Server& server) noexcept;
}

/**
 * @brief RPC core 入站服务端。
 *
 * 只装配：校验并持有 inbound transport 与 handler，不监听、不联网。
 * transport 自身的装配约束（Engine Idle、只配置 handoff）归 adapter；
 * Run/Start/收口责任不变。
 */
class Server : public bbt::coroutine::CoObject {
public:
    /// @brief 装配 IRpcInboundTransport 的通用工厂：校验并持有 inbound 与 handler，不联网。
    static result<std::shared_ptr<Server>> Create(std::shared_ptr<IRpcInboundTransport> inbound, RpcHandler handler);

    /**
     * @brief 源兼容工厂：装配 HttpRpcInboundTransport 后委托新构造。
     *
     * 定义在 HTTP adapter（bbt/infra/rpc/HttpTransport.cc）。双参数与三参数
     * 两个重载等价于旧的 `options = {}` 默认实参（不完整类型上不能写默认实参）。
     */
    static result<std::shared_ptr<Server>> Create(std::shared_ptr<net::Engine> engine, RpcHandler handler);
    /// @brief 同上，附带 ServerOptions。
    static result<std::shared_ptr<Server>> Create(std::shared_ptr<net::Engine> engine, RpcHandler handler,
                                                  http::ServerOptions options);

    // ---- 双向会话 Server（ADR 0014 §2.3/§3.1）------------------------------
    /**
     * @brief 双向会话 Server 装配。
     *
     * Engine 必须 Idle（Create 内预留 accept-handoff 容量），
     * session_owner 必须非空且启用 trusted-network 明文模式，handler 非空，
     * options.Validate() 通过。只装配，不监听、不建线程；Engine 由应用 Run/收口。
     * Server 不另建 session registry——完成握手的入站 link 交给同一
     * session_owner（Client owner），实现一条双向 link 的 Call/Send 复用。
     */
    static result<std::shared_ptr<Server>> Create(std::shared_ptr<net::Engine> engine,
                                                  std::shared_ptr<Client> session_owner,
                                                  AdmissionHandler admission_handler, ServerOptions options);

    /**
     * @brief 立即逻辑封口并发布关闭原因（ADR 0014 §2.3）。
     *
     * 只封口：StopAccepting + admission 封口 + 入站 link RequestClose，
     * 不等待任何 handler/reply/连接收口，不强杀 handler，不关闭共享
     * Engine。首个原因固定，幂等不可重开。
     */
    void Close(Error reason = MakeError(ErrorCode::Closed, "rpc server: closed by owner")) noexcept;

    /**
     * @brief 封口并等待本 Server 承诺范围内的真实排空。
     *
     * 拒新后等 admission 队列、已授权 reply/lease 与写责任归零（经
     * inbound transport 排空；会话 owner 装配用同一排空谓词）。不等连接物理 Released。
     * 有限期限超时返回 TimedOut 且保持封口；等待期间被 Close 打断返回其
     * 关闭原因；先排空后 Close 返回 ok。
     *
     * @par 执行域约束
     * 由非协程宿主上下文调用；受管协程/Processer 域返回 InvalidContext。
     */
    result<void> Shutdown(bbt::coroutine::Deadline deadline);

    /// @brief Close 发布的原因；仅 Shutdown 封口时为空。
    std::optional<Error> CloseReason() const noexcept;

    /**
     * @brief 实际绑定地址（仅会话 owner 装配且 Start() 成功后有效）。
     *
     * 动态端口返回内核分配的 net::Address；未启动、非会话装配或已不可观察时返回错误。
     */
    result<bbt::infra::net::Address> LocalAddress() const;

    /**
     * @brief 入站观测快照（会话 owner 汇总）。
     *
     * 逐 peer/epoch 观测缺口见 docs/decisions/0014-app-session.md。
     */
    ServerStats Stats() const noexcept;
    ~Server() noexcept;
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    /// @brief 启动监听。
    result<void> Start();
    /// @brief 停止接受新连接：任意线程、非阻塞、幂等。
    void StopAccepting() noexcept;
    /// @brief 等待 in-flight 请求排空（可选排序工具）。
    result<void> Drain(bbt::coroutine::Deadline deadline);
    /// @brief 等待 Server 完全收口（可选排序工具）。
    result<void> WaitStopped(bbt::coroutine::Deadline deadline);
    /// @brief 查询本 Server 是否已封口。
    bool IsClosed() const noexcept;

private:
    // 内部接缝读 m_v2：台账快照与 owner 访问都在 detail 下，公共 API 面
    // 不含它们。
    friend detail::ServerV2Ledger detail::ServerV2LedgerForTest(const Server&) noexcept;
    friend struct detail::SessionOwnerAccess;

    struct State;
    explicit Server(std::shared_ptr<State> state) noexcept;
    explicit Server(std::shared_ptr<ServerV2State> v2) noexcept;
    std::shared_ptr<State> m_state;
    std::shared_ptr<ServerV2State> m_v2; // 会话装配（与 m_state 互斥）
};

} // namespace bbt::infra::rpc
