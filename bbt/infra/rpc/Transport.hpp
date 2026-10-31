#pragma once

// RPC core transport seam（bbt::infra::rpc）：一次完整请求/回复字节交换的
// 抽象契约。HTTP（bbt/infra/rpc/HttpTransport.hpp）与后续 TCP adapter 各自
// 实现本接口；rpc::Client/Server façade 只依赖本头，不依赖任何物理栈。
//
// 职责/所有权（architecture §5.1）：
//   * IRpcTransport 只承诺一条完整 RPC 请求/回复交换与 transport 错误阶段；
//     WireRequest/WireReply 值所有权归调用方/返回值。
//   * IRpcInboundTransport 接收完整 request bytes 并调用借入的 handler；
//     handler 生命周期由 rpc::Server 强持有。
//   * 二者不定义 envelope、retry、路由、认证或共享 Engine 关闭。
//
// 生命周期：IRpcTransport::Close() 幂等/不可重开，立即封口新调用；未提交
// 调用落 Closed，已提交但尚无可信 reply 的调用落 OutcomeUnknown。不等待
// Engine fd 回收，也不得关闭共享 Engine。IRpcInboundTransport::Start()
// 一次性；StopAccepting() 幂等不重开；Drain() 只在停止接纳后等已授权
// handler/reply；Close() 强制封口 adapter 自己的 listener/connection；
// WaitStopped(deadline) 只在 native 宿主域等待 listener/callback/连接
// 物理回收凭据，超时保留状态与宿主责任。
//
// 错误：实现返回 bbt::infra::result<T>；底层 ErrorCode、backend_* 与
// request_phase 原样保留。adapter 只能新增其物理域信息，不得把
// RequestCommitted 之后的失联降为确定失败。

#include <cstdint>
#include <functional>
#include <vector>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra::rpc {

// 一条完整请求的 wire bytes（已编码 RpcWireEnvelope）；transport 不改字节。
struct WireRequest {
    std::vector<std::uint8_t> body;
};
// 一条完整回复的 wire bytes。
struct WireReply {
    std::vector<std::uint8_t> body;
};

// 入站完整请求 → 一条完整回复。由 rpc::Server 提供，借入调用。
using WireInboundHandler = std::function<result<WireReply>(
    IncomingCallContext, WireRequest)>;

class IRpcTransport {
public:
    virtual ~IRpcTransport() = default;
    virtual bbt_co_async(result<WireReply>) Exchange(
        const RpcAddress&, WireRequest, const CallOptions&) = 0;
    virtual void Close() noexcept = 0;
    virtual bool IsClosed() const noexcept = 0;
};

class IRpcInboundTransport {
public:
    virtual ~IRpcInboundTransport() = default;
    virtual result<void> Start(WireInboundHandler) = 0;
    virtual void StopAccepting() noexcept = 0;
    virtual result<void> Drain(bbt::coroutine::Deadline) = 0;
    virtual void Close() noexcept = 0;
    virtual result<void> WaitStopped(bbt::coroutine::Deadline) = 0;
    virtual bool IsClosed() const noexcept = 0;
};

} // namespace bbt::infra::rpc
