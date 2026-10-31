#pragma once

// RPC 原生 TCP adapter（bbt::infra::rpc）：architecture §6 的 u32be
// 长度前缀帧在 infra_net 栈上的实现，一连接一请求一回复，无 multiplex、
// 无半关闭。
//
// wire frame（architecture §6，v1 钉死）：
//   u32be payload_length | payload[payload_length]
//   payload 是既有 protobuf RpcWireEnvelope 字节；transport 不改字节。
//   length==0 非法；max_frame_bytes 默认 8 MiB（对齐 HTTP server 默认
//   body 预算），可配置且不得超过 kNetworkLimitsMaxBodyBytes（1 GiB 硬
//   上界）；上限在读前缀后、分配/复制前强制执行。
//
// 字节流语义：前缀与 body 可能任意分片/合并到达；先读满 4 前缀字节，
// 校验长度，再读满 body。单条响应帧之后的多余字节是协议违规并关闭
// 连接；server 一连接只处理一帧。写侧经 ICoConnection::Write：只有全
// 部帧字节本地完成才把 RequestPhase 推进到 RequestCommitted；本地完
// 成不证明对端收妥，部分/未知写不重放。
//
// 生命周期（architecture §5.1/§5.2）：
//   * TcpRpcTransport 借用 client-only net::Engine（共享寿命，从不
//     Run/RequestClose）；max_inflight 上限接纳，超额立即 Overloaded，
//     无隐藏无界队列；一次 Exchange 一条连接，无池/跨请求复用。
//     Close 幂等不可重开：立即封新调用并对已发布连接 RequestClose；
//     未提交调用 Closed，已提交无回复 OutcomeUnknown。物理回收归
//     Engine owner。
//   * TcpRpcInboundTransport 借用宿主 Engine（监听地址由宿主提供，
//     accept-handoff 在 Run 前预留）；是本 Engine 唯一 CoAccept 消费者。
//     自有有界 session/handler 任务台账；一连接一请求帧一回复帧后关
//     闭。StopAccepting/Drain/Close/WaitStopped 语义与 http::Server
//     相同（Drain/WaitStopped 仅 native 宿主域）；只停自己的
//     listener/session，不关 Engine。
//
// 错误：透传保留底层 ErrorCode、backend_* 与 request_phase 事实
// （NetToError 基础分类 + adapter 阶段结算）；RequestCommitted 之后
// 失联升级为 OutcomeUnknown，不降成确定失败；协议违规为
// ProtocolError；adapter 已 Closed 为 Closed。

#include <cstddef>
#include <memory>
#include <string>

#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/rpc/Transport.hpp>

namespace bbt::infra::rpc {

// 默认帧上限：8 MiB，与既有 HTTP server 默认 body 预算对齐
// （architecture §6）。硬上界 kNetworkLimitsMaxBodyBytes（1 GiB）。
inline constexpr std::size_t kRpcDefaultMaxFrameBytes = 8 * 1024 * 1024;

struct TcpRpcOptions {
    // 同时在途 Exchange 上限：>0 且 <= kNetworkLimitsMaxInflight；
    // 第 max_inflight+1 次调用立即 Overloaded，不排队。
    std::size_t max_inflight{64};
    // 帧 payload 上限（不含 4 字节前缀）：(0, kNetworkLimitsMaxBodyBytes]。
    std::size_t max_frame_bytes{kRpcDefaultMaxFrameBytes};
};

struct TcpRpcServerOptions {
    // accept-handoff 预留容量（Run 前经 EnableAcceptHandoff 配置）：>0。
    std::size_t handoff_capacity{16};
    // 单连接帧 payload 上限（不含前缀），语义与 TcpRpcOptions 相同。
    std::size_t max_frame_bytes{kRpcDefaultMaxFrameBytes};
    // 连接/inflight/读期限约束；各项沿用 NetworkLimits 校验。
    //   - max_connections：活 session 物理额度（含未回收退役连接）；
    //   - max_inflight：已授权 handler+回复写责任上限；
    //   - incoming_timeout：单连接读/写总期限（超时明确放弃该 session）。
    // max_header_bytes 对本 adapter 无语义（前缀固定 4 字节）。
    NetworkLimits limits{64, 64, 64 * 1024, 8 * 1024 * 1024,
                       std::chrono::milliseconds{5000}};
};

class TcpRpcTransport final : public IRpcTransport {
public:
    // 只装配：校验并持有 client-only Engine 的共享引用（不 Run、不监听、
    // 不建线程）；Engine 由应用装配并最终收口。
    static result<std::shared_ptr<TcpRpcTransport>> Create(
        std::shared_ptr<net::Engine> engine, TcpRpcOptions options = {});
    ~TcpRpcTransport() noexcept override;
    TcpRpcTransport(const TcpRpcTransport&) = delete;
    TcpRpcTransport& operator=(const TcpRpcTransport&) = delete;

    // 一连接一请求一回复：endpoint 必须是 numeric-ip:port（IPv6 bracketed），
    // 无 DNS。写满 4+N 字节后等一条响应帧；committed 后失联为
    // OutcomeUnknown(request_phase=RequestCommitted)。
    bbt_co_async(result<WireReply>) Exchange(const RpcAddress& address,
                                             WireRequest request,
                                             const CallOptions& options) override;
    // 立即逻辑封口：封新调用并对在途连接 RequestClose（幂等、不重开）。
    void Close() noexcept override;
    bool IsClosed() const noexcept override;

private:
    struct State;
    explicit TcpRpcTransport(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> m_state;  // gate + max_inflight 名额 + 活动连接台账
};

class TcpRpcInboundTransport final : public IRpcInboundTransport {
public:
    // 只装配：Engine 必须 Idle；只配置 accept-handoff（capacity>0），不
    // Run/监听/建线程。Engine 由应用独占接纳消费（不得另设 raw reader 或
    // 第二个 CoAccept 消费者），最终由应用关闭。
    static result<std::shared_ptr<TcpRpcInboundTransport>> Create(
        std::shared_ptr<net::Engine> engine, TcpRpcServerOptions options = {});
    ~TcpRpcInboundTransport() noexcept override;
    TcpRpcInboundTransport(const TcpRpcInboundTransport&) = delete;
    TcpRpcInboundTransport& operator=(const TcpRpcInboundTransport&) = delete;

    // Start 一次性：登记 WireInboundHandler 并启动 accept 任务。Engine
    // 必须 Running 且有监听。入站连接读一条请求帧交 handler，写一条回复
    // 帧后关闭；不处理同一连接的第二帧。
    result<void> Start(WireInboundHandler handler) override;
    // 只封新连接/新会话接纳（幂等不重开）；已授权 handler 及回复继续；
    // 未授权 session 被关闭。
    void StopAccepting() noexcept override;
    // native 宿主专用：必须先 StopAccepting；等已授权 handler 返回且回复
    // 写终态。协程/Processer 域返回 InvalidContext。
    result<void> Drain(bbt::coroutine::Deadline deadline) override;
    // 幂等强制逻辑封口自身 listener/连接（在途回复明确放弃），不关
    // Engine，不强杀 handler。
    void Close() noexcept override;
    // native 宿主专用：listener 停止、全部本 adapter callback 已销毁、
    // 连接 Released。超时保留任务/共享状态责任。
    result<void> WaitStopped(bbt::coroutine::Deadline deadline) override;
    bool IsClosed() const noexcept override;

private:
    struct State;
    explicit TcpRpcInboundTransport(std::shared_ptr<State> state) noexcept;
    std::shared_ptr<State> m_state;  // bounded accept/session/handler ledger
};

} // namespace bbt::infra::rpc
