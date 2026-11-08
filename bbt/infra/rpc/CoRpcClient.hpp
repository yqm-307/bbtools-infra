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
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/Options.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>
#include <bbt/infra/rpc/Stats.hpp>
#include <bbt/infra/rpc/Hooks.hpp>

// 执行底座由应用装配并最终收口。
namespace bbt::infra::net {
class Engine;
}

namespace bbt::infra::rpc {

class CoRpcServer;
struct SessionOwnerState; // detail：会话 owner，定义见 detail/RpcSessionState.hpp

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
     * 三种形态共用同一入口，不另设 Drain/WaitClosed：
     *   - Close(Immediate, 0)：立即逻辑封口并发布首个关闭原因（幂等、不可
     *     重开）；拒绝新 Call/Send，对在途 link 强制关闭并结算全部未终结
     *     调用，不等待任何在途完成。
     *   - Close(Timeout, timeout_ms)：先同上强制收口，再有界等待 RPC 自己
     *     持有的 session/握手责任物理收口；超时返回 TimedOut。
     *   - Close(Graceful, timeout_ms)：先拒新（不发布原因），等已接纳调用
     *     真实归还责任后封口并发布原因；timeout_ms==-1 表示无限等待，有限
     *     期限超时返回 TimedOut 并强制收口。等待只覆盖 RPC 自身责任，
     *     不承诺业务 handler 的优雅结束。
     * @param flag 关闭模式。
     * @param timeout_ms 毫秒；仅 Graceful 允许 -1（无限），其余负值返回
     *        InvalidArgument 且不启动等待。
     * @param reason 关闭原因；message 为空时使用默认 Closed 原因。
     * @return 等待模式的收口结果；不返回「排空失败」的额外分类。
     */
    bbt_co_async(result<void>)
        Close(CloseFlag flag = CloseFlag::Immediate, std::int64_t timeout_ms = 0, Error reason = {});

    /**
     * @brief 宿主侧同步强制关闭（任意域、不挂起、幂等）。
     *
     * 立即拒绝新操作、强制关闭所有 session/link、未终结调用立即失败；
     * 不等待 handler、reply、队列或物理释放。与 Close(Immediate) 共用同一
     * 套关闭状态，不产生另一套生命周期。
     * @param reason 关闭原因；message 为空时使用默认 Closed 原因。
     */
    void Shutdown(Error reason = {}) noexcept;

    /// @brief 查询当前是否已逻辑封口。
    bool IsClosed() const noexcept;
    /// @brief 已发布的首个关闭原因；未发布为空。
    std::optional<Error> CloseReason() const noexcept;

private:
    explicit CoRpcClient(std::shared_ptr<SessionOwnerState> owner) noexcept;
    friend class CoRpcServer; // CoRpcServer::Create 注入 session_owner 取 m_owner
    std::shared_ptr<SessionOwnerState> m_owner;
};

} // namespace bbt::infra::rpc
