/**
 * @file Client.hpp
 * @brief RPC core 出站客户端（只依赖 IRpcTransport seam，不依赖任何物理栈）。
 *
 * 职责边界：Client 只做 envelope wire 编解码、address 边界检查、请求/回复
 * 五字段关联校验与剩余预算换算；网络、连接池、阶段超时与错误分类全部归
 * IRpcTransport 实现。wire profile 语义沿用
 * bbt/infra/rpc/RpcWire.hpp。HTTP adapter 见 bbt/infra/rpc/HttpTransport.hpp。
 *
 * 生命周期（无重开）：Close() 立即逻辑封口（幂等，
 * 委托给 adapter；未提交调用 Closed，已提交无回复 OutcomeUnknown）；
 * IsClosed() 查询当前状态。析构=Close。不新增第二种关闭入口，也不承诺物理
 * 排空（退役连接由 adapter/Engine owner 收口）。
 *
 * 错误面（透传优先，RPC 不重新覆盖物理错误）：
 *   - Closed：本 Client/adapter 已 Close 后调用；
 *   - InvalidArgument：address transport/endpoint 非法、envelope 不可编码；
 *   - TimedOut：调用方 deadline 已过期（确定失败，不发请求）；
 *   - ProtocolError：回复五字段关联不匹配或 wire 解析失败；
 *   - 传输/取消/未知结果原样透传 adapter 错误（含 OutcomeUnknown 与
 *     request_phase 事实），RPC 不把提交后错误改写成其他终态；
 *   - HTTP adapter 另将非 200 映射为 Unavailable（backend_category=http）。
 */
#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <string_view>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/coroutine/object/CoObject.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/rpc/Request.hpp>
#include <bbt/infra/rpc/Response.hpp>
#include <bbt/infra/rpc/Stats.hpp>
#include <bbt/infra/rpc/Transport.hpp>
#include <bbt/infra/rpc/Hooks.hpp>

// 源兼容工厂（HTTP adapter 实现，见 HttpTransport.hpp）：完整声明需要
// bbt::infra::http 类型，这里仅前向声明；定义与实现在 HTTP adapter TU。
namespace bbt::infra::http {
class Client;
}
// 会话 owner：只前向声明 net::Engine（不产生头依赖），options/值类型
// 见 rpc/Options.hpp。
namespace bbt::infra::net {
class Engine;
}

namespace bbt::infra::rpc {

class ClientOptions;
class SendOptions;
struct SessionOwnerState; // detail：会话 owner，仅 Session TU 有定义

namespace detail {
struct SessionOwnerAccess;
} // namespace detail

/**
 * @brief RPC core 出站客户端。
 *
 * 只装配：校验并持有 transport，不联网。Engine/执行底座由应用装配并
 * 最终收口（与 adapter owner 同责）。
 */
class Client : public bbt::coroutine::CoObject {
public:
    /// @brief 装配 IRpcTransport 的通用工厂：校验并持有 transport，不联网。
    static result<std::shared_ptr<Client>> Create(std::shared_ptr<IRpcTransport> transport);

    /**
     * @brief 源兼容工厂：包装 HttpRpcTransport。
     *
     * 定义在 HTTP adapter（bbt/infra/rpc/HttpTransport.cc）。
     */
    static result<std::shared_ptr<Client>> Create(std::shared_ptr<http::Client> http_client);

    // ---- 会话 owner（ADR 0014 §2.2/§3.1）----------------------------------
    /**
     * @brief 会话 owner 装配：校验 options 并持有共享 Engine。
     *
     * 只装配：不启动 Engine、不建线程、不监听、不拨号。未显式
     * SetTrustedNetworkPlaintext(true) 且当前没有 verifier API 时返回
     * UnsupportedRoute。
     */
    static result<std::shared_ptr<Client>> Create(std::shared_ptr<net::Engine> engine, ClientOptions options);

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
     * 会话装配返回同一 owner 的共享 handle；非会话装配返回空 handle。handle 只
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
    ~Client() noexcept;
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    /**
     * @brief envelope 级 RPC 调用（仅受管协程内调用，bbt_co_await 消费）。
     *
     * 期限由 options.deadline 表达，必须是未来时刻（epoch/已过期确定
     * TimedOut）。地址 transport 只接受 "http"/"tcp"，endpoint 合法性归
     * adapter；请求经 wire profile 编码后交 adapter Exchange。回复按五字段
     * 关联校验（request_id/service/method/request_schema/response_schema），
     * 不一致报 ProtocolError。
     *
     * @param address 目标地址（"http" 或 "tcp"）与端点
     * @param request 待发送的 envelope
     * @param options 本次调用的期限与流量选项
     */
    bbt_co_async(result<RpcEnvelope>) Call(const RpcAddress& address, RpcEnvelope request, const CallOptions& options);

    /// @brief 立即逻辑封口：发布首个关闭原因并转发 adapter/会话 owner
    ///        （幂等、不可重开）。
    void Close(Error reason = MakeError(ErrorCode::Closed, "rpc client: closed by owner")) noexcept;
    /// @brief 查询当前是否已 Close。
    bool IsClosed() const noexcept;
    /// @brief Close 发布的原因；仅 Shutdown 封口时为空。
    std::optional<Error> CloseReason() const noexcept;
    /// @brief 封口并等待已接纳调用真实归还责任。
    ///
    /// 经 transport（HTTP adapter 排空底层池；未提供排空谓词的 adapter
    /// 如实返回 UnsupportedRoute）；会话 owner 用自身接纳/操作计数，不
    /// 依赖连接物理 Released。有限期限超时 TimedOut 且保持封口；等待期间
    /// 被 Close 打断返回其关闭原因。native 宿主专用。
    result<void> Shutdown(bbt::coroutine::Deadline deadline);

private:
    explicit Client(std::shared_ptr<IRpcTransport> transport) noexcept;
    explicit Client(std::shared_ptr<SessionOwnerState> owner) noexcept;
    friend class Server; // Server::Create 注入 session_owner 取 m_owner
    friend struct detail::SessionOwnerAccess;
    std::shared_ptr<IRpcTransport> m_transport;
    std::shared_ptr<SessionOwnerState> m_owner; // 会话 owner（与
                                                // m_transport 互斥装配）
    mutable std::mutex m_reason_mtx;
    std::optional<Error> m_close_reason;
};

} // namespace bbt::infra::rpc
