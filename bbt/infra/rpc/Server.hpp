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
// 生命周期（Created → Running → StopAccepting → Draining → Closed →
// Stopped，无重启）：
//   Start()          执行底座 Running 且有监听后一次性调用；
//   StopAccepting()  只封新连接/新请求接纳，已授权 handler 及回复继续；
//   Drain(deadline)  等待全部已授权 handler 真实返回且回复写终态（仅 native 宿主）；
//   Close()          幂等强制逻辑封口自身连接（在途回复明确放弃；不关共享 Engine）；
//   WaitStopped()    listener 停止、callback 全部销毁、连接 Released（仅 native 宿主）。
// IsClosed() 观测本 Server 的 Close 已调用。不新增第二种关闭入口。

#include <memory>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/Transport.hpp>

// 源兼容工厂（HTTP adapter 实现，见 HttpTransport.hpp）：完整签名需要
// bbt::infra::http/net 类型，这里仅前向声明。
namespace bbt::infra::http { struct ServerOptions; }
namespace bbt::infra::net { class Engine; }

namespace bbt::infra::rpc {

class Server {
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
    struct State;
    explicit Server(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> m_state;
};

} // namespace bbt::infra::rpc
