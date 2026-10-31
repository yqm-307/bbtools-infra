// RPC core 入站服务端实现：组合 IRpcInboundTransport seam，不引用任何物理
// 栈头。wire 解析/local-min/关联/错误信封语义沿用旧 RPC runtime 契约
// （0009 §3）；HTTP 兼容工厂在 bbt/infra/rpc/HttpTransport.cc。

#include <bbt/infra/rpc/Server.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <utility>

#include <bbt/infra/rpc/RpcWire.hpp>
#include <bbt/infra/rpc/detail/Core.hpp>

namespace bbt::infra::rpc {
namespace {

// 单请求处理：wire 解析 → deadline local-min → handler → 关联校验 → 回信封
// 字节。任何失败都转成 wire error envelope（不是 transport 错误）；解析失败
// 无法取关联字段时用确定占位值（unparsed/unknown）。与旧实现逐分支一致。
result<WireReply> EncodeErrorReply(const Error& error,
                                   const RpcWireEnvelope* wire) {
    RpcWireEnvelope out = wire
        ? detail::ErrorEnvelope(error, wire->request_id, wire->service,
                                wire->method, wire->request_schema,
                                wire->response_schema)
        : detail::ErrorEnvelope(error, "", "", "", "", "");
    auto body = EncodeRpcWireEnvelope(out, /*is_response=*/true);
    if (!body) {
        return result<WireReply>::err(body.error());
    }
    return result<WireReply>::ok(WireReply{std::move(body).value()});
}

result<WireReply> HandleWireRequest(const IncomingCallContext& context,
                                    WireRequest request,
                                    const RpcHandler& handler) {
    auto parsed = DecodeRpcWireEnvelope(request.body, /*is_response=*/false);
    if (!parsed) {
        return EncodeErrorReply(parsed.error(), nullptr);
    }
    const auto wire = std::move(parsed).value();
    auto payload = FromWireEnvelope(wire);
    if (!payload) {
        return EncodeErrorReply(payload.error(), &wire);
    }

    IncomingCallContext effective = context;
    const auto remote_deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds{wire.remaining_budget_ms};
    effective.deadline = std::min(context.deadline, remote_deadline);
    if (effective.deadline <= std::chrono::steady_clock::now()) {
        return EncodeErrorReply(
            MakeError(ErrorCode::TimedOut, "rpc request deadline expired"),
            &wire);
    }

    const auto handler_deadline = effective.deadline;
    auto handled = handler(std::move(effective), std::move(payload.value()));
    if (!handled) {
        return EncodeErrorReply(handled.error(), &wire);
    }
    auto reply = std::move(handled.value());
    if (reply.request_id != wire.request_id ||
        reply.service != wire.service ||
        reply.method != wire.method ||
        reply.request_schema != wire.request_schema ||
        reply.response_schema != wire.response_schema) {
        return EncodeErrorReply(
            MakeError(ErrorCode::ProtocolError,
                      "rpc handler changed association fields"),
            &wire);
    }

    result<void> budget_status = result<void>::ok();
    const auto budget = detail::RemainingBudgetMs(handler_deadline, budget_status);
    if (!budget_status) {
        return EncodeErrorReply(budget_status.error(), &wire);
    }
    auto out = ToWireEnvelope(reply, budget);
    if (!out) {
        return EncodeErrorReply(out.error(), &wire);
    }
    auto body = EncodeRpcWireEnvelope(out.value(), /*is_response=*/true);
    if (!body) {
        return result<WireReply>::err(body.error());
    }
    return result<WireReply>::ok(WireReply{std::move(body).value()});
}

} // namespace

struct Server::State {
    std::shared_ptr<IRpcInboundTransport> inbound;
    RpcHandler handler;
    // wire handler 与 adapter 共享同一 shared_ptr 计数（Start 时建立）：
    // State 强持有，adapter 只弱引用——在途回调期间引用稳定、adapter
    // 一侧不成环、Server 析构不等待在途 handler 时回调不会悬空。
    std::shared_ptr<WireInboundHandler> wire;
    std::atomic<bool> closed{false};
};

Server::Server(std::shared_ptr<State> state) noexcept
    : m_state(std::move(state)) {}

Server::~Server() noexcept { Close(); }

result<std::shared_ptr<Server>> Server::Create(
    std::shared_ptr<IRpcInboundTransport> inbound, RpcHandler handler) {
    if (!inbound) {
        return result<std::shared_ptr<Server>>::err(MakeError(
            ErrorCode::InvalidArgument,
            "rpc server: inbound transport required"));
    }
    if (!handler) {
        return result<std::shared_ptr<Server>>::err(MakeError(
            ErrorCode::InvalidArgument, "rpc server: handler must not be null"));
    }
    auto state = std::make_shared<State>();
    state->inbound = std::move(inbound);
    state->handler = std::move(handler);
    return result<std::shared_ptr<Server>>::ok(
        std::shared_ptr<Server>(new Server(std::move(state))));
}

result<void> Server::Start() {
    auto state = m_state;
    if (state->closed.load()) {
        return result<void>::err(
            MakeError(ErrorCode::Closed, "rpc server: closed"));
    }
    // RpcHandler 按值收进共享计数的 wire handler（std::function 内拷一份；
    // 不回指 State，避免 State↔wire 自持环）。wire 由 State 强持有、adapter
    // 只弱引用：在途调用 lock 期间引用稳定，Server 析构后 wire 释放、
    // adapter 弱引用失效，回调不会悬空。
    if (!state->wire) {
        state->wire = std::make_shared<WireInboundHandler>(
            [handler = state->handler](IncomingCallContext context,
                                       WireRequest request)
                -> result<WireReply> {
                return HandleWireRequest(context, std::move(request),
                                         handler);
            });
    }
    return state->inbound->Start(*state->wire);
}

void Server::StopAccepting() noexcept { m_state->inbound->StopAccepting(); }

result<void> Server::Drain(bbt::coroutine::Deadline deadline) {
    return m_state->inbound->Drain(deadline);
}

void Server::Close() noexcept {
    if (!m_state) { return; }
    m_state->closed.store(true);
    m_state->inbound->Close();
}

result<void> Server::WaitStopped(bbt::coroutine::Deadline deadline) {
    return m_state->inbound->WaitStopped(deadline);
}

bool Server::IsClosed() const noexcept {
    return !m_state || m_state->closed.load();
}

} // namespace bbt::infra::rpc
