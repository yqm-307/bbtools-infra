#pragma once

// RPC core 入站服务端（bbt::infra::rpc，只依赖 IRpcInboundTransport seam，
// 不依赖任何物理栈；HTTP adapter 见 bbt/infra/rpc/HttpTransport.hpp）。
//
// 职责边界：Server 只做 wire 解析、deadline local-min（min(本请求期限,
// now + remaining_budget_ms)）、handler 五字段关联校验与错误信封化；
// 监听、会话/连接管理、协议安全与资源配额全部归 IRpcInboundTransport
// 实现（architecture §5.2/§5.3）。RpcHandler 由上层提供，在受管协程中调用，
// 生命周期由本 Server 强持有。
//
// 生命周期（v2 收敛为简单 Close）：Close(deadline) 立即逻辑封口（幂等、
// 不可重开）并在 deadline 内等物理收口；IsClosed() 观测本 Server 已封口。
// StopAccepting/Drain/WaitStopped 是可选的用户排序工具——由调用方按自己
// 的顺序与预算调用，不是 infra 强制的多阶段协议；Close(deadline) 单次
// 调用即完成 Seal+收口等待，业务任务的停止/等待由用户控制，infra 不新增
// drain/receipt/多阶段关闭协议。

#include <cstdint>
#include <memory>

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
namespace bbt::infra::http { struct ServerOptions; }
namespace bbt::infra::net { class Engine; }  // 只前向声明；net::Address 值类型
//（sockaddr 快照，无物理栈泄漏，leak-check 白名单）经 LocalAddress 返回需要
// 完整类型（上方 include）。

namespace bbt::infra::rpc {

class Client;
class Server;
class ServerOptions;
struct ServerV2State;  // detail：v2 入站状态，仅 Session TU 有定义

namespace detail {
// detail/Session.hpp 的测试接缝声明前置（friend 需要限定名先声明）。
std::uint64_t ServerV2WritesOutstandingForTest(const Server& server) noexcept;
struct ServerV2Ledger;   // 完整定义见 detail/Session.hpp
ServerV2Ledger ServerV2LinkLedgerForTest(const Server& server) noexcept;
}

class Server : public bbt::coroutine::CoObject {
public:
    // 只装配：校验并持有 inbound transport 与 handler，不监听、不联网。
    // transport 自身的装配约束（Engine Idle、只配置 handoff）归 adapter；
    // Run/Start/收口责任不变。
    static result<std::shared_ptr<Server>> Create(
        std::shared_ptr<IRpcInboundTransport> inbound, RpcHandler handler);
    // 源兼容工厂：装配 HttpRpcInboundTransport 后委托新构造；下一迁移窗口
    // 结束前保留（architecture §10）。定义在 HTTP adapter
    // （bbt/infra/rpc/HttpTransport.cc）。双参数与三参数两个重载等价于旧的
    // `options = {}` 默认实参（不完整类型上不能写默认实参）。
    static result<std::shared_ptr<Server>> Create(
        std::shared_ptr<net::Engine> engine, RpcHandler handler);
    static result<std::shared_ptr<Server>> Create(
        std::shared_ptr<net::Engine> engine, RpcHandler handler,
        http::ServerOptions options);

    // ---- v2 双向会话 Server（ADR §2.3/§3.1）------------------------------
    // 只装配：Engine 必须 Idle（Create 内预留 accept-handoff 容量），
    // session_owner 必须非空且启用 trusted-network 明文模式，handler 非空，
    // options.Validate() 通过。不监听、不建线程；Engine 由应用 Run/收口。
    // Server 不另建 session registry——完成握手的入站 link 交给同一
    // session_owner（Client owner），实现一条双向 link 的 Call/Send 复用。
    static result<std::shared_ptr<Server>> Create(
        std::shared_ptr<net::Engine> engine,
        std::shared_ptr<Client> session_owner,
        AdmissionHandler admission_handler,
        ServerOptions options);

    // v2 收敛关闭（ADR §2.3）：立即逻辑封口（StopAccepting + admission 封
    // 口 + 入站 link RequestClose），再在 deadline 内等待 listener、单 FIFO
    // admission worker、已授权 reply/lease 与物理连接收口。超时返回
    // TimedOut——IsClosed() 仍为 true，owner/Engine 责任保留，可用更晚
    // deadline 再次调用观察最终收口。不强杀 handler，不关闭共享 Engine。
    // 由非协程宿主上下文调用；受管协程/Processer 域返回 InvalidContext。
    result<void> Close(bbt::coroutine::Deadline deadline);

    // 仅在 v2 Start() 成功后有效：动态端口返回实际绑定的 net::Address；
    // 未启动/v1 装配/已不可观察返回明确错误。
    result<bbt::infra::net::Address> LocalAddress() const;

    // v2 入站观测快照（owner 汇总；逐 peer/epoch 观测缺口见 handoff）。
    ServerStats Stats() const noexcept;
    ~Server() noexcept;
    Server(const Server&) = delete;
    Server& operator=(const Server&) = delete;

    result<void> Start();
    void StopAccepting() noexcept;
    result<void> Drain(bbt::coroutine::Deadline deadline);
    void Close() noexcept;
    result<void> WaitStopped(bbt::coroutine::Deadline deadline);
    bool IsClosed() const noexcept;

private:
    // detail 测试接缝访问 m_v2（detail/Session.hpp 声明，实现在 Server.cc）；
    // 不改变公共 API 面。
    friend std::uint64_t detail::ServerV2WritesOutstandingForTest(
        const Server&) noexcept;
    friend detail::ServerV2Ledger detail::ServerV2LinkLedgerForTest(
        const Server&) noexcept;

    struct State;
    explicit Server(std::shared_ptr<State> state) noexcept;
    explicit Server(std::shared_ptr<ServerV2State> v2) noexcept;
    std::shared_ptr<State>         m_state;
    std::shared_ptr<ServerV2State> m_v2;  // v2 装配（与 m_state 互斥）
};

} // namespace bbt::infra::rpc
