#pragma once

// RPC core 入站服务端（bbt::infra::rpc，消费新 bbt::infra::http::Server）。
//
// 职责边界：Server 只在 http::Server 的 owning request/handler 之上做 wire
// 解析、deadline local-min（min(本请求期限, now + remaining_budget_ms)）、
// handler 关联字段校验与错误信封化；监听、会话/连接管理、协议安全与资源
// 配额全部归 http::Server owner。RpcHandler 由上层提供，在受管协程中调用。
//
// 生命周期完全沿用 http::Server 语义（不做第二套定义）：
//   Start()          Engine Running 且有监听后一次性调用；
//   StopAccepting()  只封新连接/新请求接纳，已授权 handler 及回复继续；
//   Drain(deadline)  等待全部已授权 handler 真实返回且回复写终态（仅 native 宿主）；
//   Close()          幂等强制逻辑封口自身连接（在途回复明确放弃）；
//   WaitStopped()    listener 停止、callback 全部销毁、连接 Released（仅 native 宿主）。
// 唯一补充的观测是 IsClosed()：Close() 已在 RPC Server 上调用过。不复制
// http::Server 的 Stats/LocalAddress 观测面（地址经共享 Engine 的
// LocalAddresses() 查询），也不新增第二种关闭入口。

#include <memory>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/http/Server.hpp>
#include <bbt/infra/net/Engine.hpp>

namespace bbt::infra::rpc {

class Server {
public:
    // 只装配：在给定 Engine 上创建底层 http::Server（Engine 必须 Idle，只配置
    // handoff，不 Run/监听/建线程）并挂 RPC handler。Run/Start/收口责任与
    // http::Server 相同：应用先 Run（listener 归 EngineConfig），Start 后接纳，
    // 收口走 StopAccepting → Drain → Close → WaitStopped。
    static result<std::shared_ptr<Server>> Create(
        std::shared_ptr<net::Engine> engine, RpcHandler handler,
        http::ServerOptions options = {});
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
