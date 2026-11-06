#pragma once

/**
 * @file Transport.hpp
 * @brief RPC core transport seam（bbt::infra::rpc）：一次完整请求/回复字节交换的
 * 抽象契约。HTTP（bbt/infra/rpc/HttpTransport.hpp）与 TCP（bbt/infra/rpc/TcpTransport.hpp）adapter 各自
 * 实现本接口；rpc::Client/Server façade 只依赖本头，不依赖任何物理栈。
 *
 * 职责/所有权（docs/architecture/gnet-network-execution.md §5）：
 *   * IRpcTransport 只承诺一条完整 RPC 请求/回复交换与 transport 错误阶段；
 *     WireRequest/WireReply 值所有权归调用方/返回值。
 *   * IRpcInboundTransport 接收完整 request bytes 并调用借入的 handler；
 *     handler 生命周期由 rpc::Server 强持有。
 *   * 二者不定义 envelope、retry、路由、认证或共享 Engine 关闭。
 *
 * 生命周期：IRpcTransport::Close() 幂等/不可重开，立即封口新调用；未提交
 * 调用落 Closed，已提交但尚无可信 reply 的调用落 OutcomeUnknown。不等待
 * Engine fd 回收，也不得关闭共享 Engine。IRpcInboundTransport::Start()
 * 一次性；StopAccepting() 幂等不重开；Drain() 只在停止接纳后等已授权
 * handler/reply；Close() 强制封口 adapter 自己的 listener/connection；
 * WaitStopped(deadline) 只在 native 宿主域等待 listener/callback/连接
 * 物理回收凭据，超时保留状态与宿主责任。
 *
 * 错误：实现返回 bbt::infra::result<T>；底层 ErrorCode、backend_* 与
 * request_phase 透传不变。adapter 只能新增其物理域信息，不得把
 * RequestCommitted 之后的失联降为确定失败。
 */

#include <cstdint>
#include <functional>
#include <vector>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra::rpc {

/**
 * @brief 一条完整请求的 wire bytes（已编码 RpcWireEnvelope）；transport 不改字节。
 */
struct WireRequest {
    std::vector<std::uint8_t> body; ///< 已编码的 envelope 字节
};
/**
 * @brief 一条完整回复的 wire bytes。
 */
struct WireReply {
    std::vector<std::uint8_t> body; ///< 已编码的 envelope 字节
};

/**
 * @brief 入站完整请求 → 一条完整回复。由 rpc::Server 提供，借入调用。
 *
 * handler 的生命周期由 rpc::Server 强持有；transport 只借用调用。
 */
using WireInboundHandler = std::function<result<WireReply>(IncomingCallContext, WireRequest)>;

/**
 * @brief 出站 RPC 字节交换 transport 接口：一次完整请求/回复。
 *
 * 由 HTTP / TCP adapter 实现；Close 与 Shutdown 的语义见成员注释。
 */
class IRpcTransport {
public:
    virtual ~IRpcTransport() = default;
    /// 交换一次完整请求/回复（address 目标地址、request 已编码字节、options
    /// 期限与流量选项）；错误阶段（request_phase）由实现透传。
    virtual bbt_co_async(result<WireReply>) Exchange(const RpcAddress&, WireRequest, ///< 目标地址与已编码请求字节
                                                     const CallOptions&) = 0;        ///< 本次调用的期限与流量选项
    /// 立即逻辑封口；幂等、不可重开。
    virtual void Close() noexcept = 0;
    /// 是否已封口。
    virtual bool IsClosed() const noexcept = 0;

    /// @brief 带关闭原因的 Close：默认丢弃原因并转发到 Close()，保持既有 adapter 源兼容。
    ///
    /// 需要把原因传到底座（HTTP 池）的 adapter 覆盖。
    virtual void Close(Error reason) noexcept;
    /// @brief 封口并等待已接纳调用真实归还责任。
    ///
    /// 默认实现诚实报告本 adapter 未提供排空谓词（能力边界，不伪装成功）；HTTP adapter 覆盖为底座排空。
    virtual result<void> Shutdown(bbt::coroutine::Deadline deadline);
};

/**
 * @brief 入站 RPC 字节接收 transport 接口：接收完整请求并交给借入的 handler 回复一次。
 *
 * Start 一次性；WaitStopped 只在 native 宿主域等待物理回收凭据；Close 强制封口。
 */
class IRpcInboundTransport {
public:
    virtual ~IRpcInboundTransport() = default;
    /// 登记入站处理器并开始接纳；一次性。handler 为借用调用的入站处理器
    /// （寿命由 rpc::Server 强持有），重复 Start 或登记失败时返回错误。
    virtual result<void> Start(WireInboundHandler) = 0;
    /// 停止接纳新请求；幂等、不重开。
    virtual void StopAccepting() noexcept = 0;
    /// 停止接纳后按 deadline 等已授权 handler/reply 结束；返回排空完成、超时
    /// 或已关闭。
    virtual result<void> Drain(bbt::coroutine::Deadline) = 0;
    /// 强制封口 adapter 自己的 listener/connection；幂等。
    virtual void Close() noexcept = 0;
    /// 只在 native 宿主域按 deadline 等待 listener/callback/连接物理回收凭据；
    /// 超时保留状态与宿主责任。
    virtual result<void> WaitStopped(bbt::coroutine::Deadline) = 0;
    /// 是否已封口。
    virtual bool IsClosed() const noexcept = 0;

    /// @brief 带关闭原因的 Close：默认丢弃原因并转发到 Close()，既有 adapter 源兼容。
    virtual void Close(Error reason) noexcept;
    /// @brief 单入口封口 + 排空：默认复用既有两段（StopAccepting → Drain）。
    ///
    /// 有更真实排空谓词的 adapter 覆盖。
    virtual result<void> Shutdown(bbt::coroutine::Deadline deadline);
};

} // namespace bbt::infra::rpc
