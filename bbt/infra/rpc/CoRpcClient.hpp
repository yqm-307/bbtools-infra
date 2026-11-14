/**
 * @file CoRpcClient.hpp
 * @brief 双向 RPC 会话客户端，持有共享 Engine 与单一会话 owner。
 *
 * 只装配，不启动 Engine、不拨号。Call/Send 在受管协程内建立或复用
 * 双向 TCP link；会话负责握手、关联、预算、取消与提交后的未知结果。
 * Close 是唯一关闭入口（Immediate/Timeout/Graceful 三态），Shutdown 是
 * 宿主侧同步强制关闭；连接物理回收仍由 Engine owner 收口，关闭后不可重开。
 */
#pragma once

#include <memory>
#include <optional>
#include <string_view>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/infra/CloseOptions.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/Options.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>
#include <bbt/infra/rpc/Stats.hpp>
#include <bbt/infra/rpc/Hooks.hpp>

/**
 * 执行底座由应用装配并最终收口。
 */
namespace bbt::infra::net {
class Engine;
}

namespace bbt::infra::rpc {

class CoRpcServer;

/**
 * detail：会话 owner，定义见 detail/RpcSessionState.hpp
 */
struct SessionOwnerState;

/**
 * @brief 双向 RPC 会话客户端。
 *
 * 校验 options 并持有共享 Engine，不在装配时联网。
 */
class CoRpcClient : public bbt::coroutine::CoObject {
public:
    /**
     * @brief 会话 owner 装配：校验 options 并持有共享 Engine。
     *
     * 只装配：不启动 Engine、不建线程、不监听、不拨号。未显式
     * SetTrustedNetworkPlaintext(true) 且当前没有 verifier API 时返回
     * UnsupportedRoute。
     */
    static result<std::shared_ptr<CoRpcClient>> Create(std::shared_ptr<net::Engine> engine, ClientOptions options);

    /**
     * @brief 会话出站 RPC 调用（仅受管协程内调用，bbt_co_await 消费）。
     *
     * target 为 "tcp://host:port"（numeric-ip:port，无 DNS）。首次使用经
     * Hello/HelloAck 握手与 epoch 协商建立 provisional session，同 PeerKey
     * 的并发 Call/Send 共用一条双向 link（含本进程 Server 入站接纳的 link）。
     * Call 要求 request.Reply()==Required、Traffic 为 Service/Actor；
     * request_id 为空时由 session 生成唯一值。
     *
     * @return 失败错误面：InvalidArgument/InvalidContext/TimedOut/Closed/
     *         Overloaded/UnsupportedRoute/OutcomeUnknown（committed 后无可信终态）。
     */
    bbt_co_async(result<Response>) Call(std::string_view target, Request request, const CallOptions& options);

    /**
     * @brief 会话出站单向发送（仅受管协程内调用）。
     *
     * Send 要求 request.Reply()==None、Traffic 为 Service/Actor；
     * message_id 为空时由 session 生成唯一值。RequireRemoteAcceptance
     * 编码为 acceptance_requested presence；true 时等到远端 admission
     * 成功（或拒绝）的 SendAccepted 才返回。false 在本地 lane 接纳后返回。
     */
    bbt_co_async(result<void>) Send(std::string_view target, Request message, const SendOptions& options);

    /**
     * @brief 本 owner 实例的受控检查点控制器。
     *
     * 返回同一 owner 的共享 handle；handle 只
     * 持有有界的控制状态，不延长 owner、Engine、连接或业务对象的寿命。
     * 注册、注销、快照与故障动作的语义见 bbt/infra/rpc/Hooks.hpp。
     */
    std::shared_ptr<SessionHooks> Hooks() const noexcept;

    /**
     * @brief 会话 owner 观测快照（owner 汇总）。
     *
     * 逐 peer/epoch/link 观测缺口见 docs/decisions/0014-app-session.md；
     * 本结构不声称完整 AppSession 监控。
     */
    ClientStats Stats() const noexcept;
    ~CoRpcClient() noexcept;
    CoRpcClient(const CoRpcClient&) = delete;
    CoRpcClient& operator=(const CoRpcClient&) = delete;

    /**
     * @brief 统一关闭入口（受管协程内 bbt_co_await 消费）。
     *
     * 模式、首因与绝对期限唯一由 CloseOptions 表达：
     *   - Immediate：立即逻辑封口并发布首个关闭原因（幂等、不可重开）；
     *     拒绝新 Call/Send，对在途 link 强制关闭并结算全部未终结调用，不等待。
     *   - Graceful：拒新并发布首个原因，等已接纳调用真实归还责任；到期只
     *     返回 TimedOut 并保留责任，不自动升级。等待只覆盖 RPC 自身责任，
     *     不承诺业务 handler 的优雅结束。
     *   - wait_release=true：模式工作完成后在同一 Close 内继续等 link 物理
     *     close 与在途结算出账到同一绝对期限（不续预算）。
     *
     * 本 owner 已接纳调用的协程内调用（自等待）在副作用前返回
     * InvalidContext。重复调用幂等、首因固定；Graceful → Immediate 升级由任一
     * 次 Immediate/Close(Immediate) 触发。
     * @param options 模式、首因与优雅关闭的有限绝对期限。
     */
    bbt_co_async(result<void>) Close(const CloseOptions& options = {});

    /**
     * @brief 查询当前是否已逻辑封口。
     */
    bool IsClosed() const noexcept;

    /**
     * @brief 已发布的首个关闭原因；未发布为空。
     */
    std::optional<Error> CloseReason() const noexcept;

private:
    explicit CoRpcClient(std::shared_ptr<SessionOwnerState> owner) noexcept;

    /**
     * CoRpcServer::Create 注入 session_owner 取 m_owner
     */
    friend class CoRpcServer;
    std::shared_ptr<SessionOwnerState> m_owner;
};

// clang-format off
}
// clang-format on
