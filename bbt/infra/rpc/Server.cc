// RPC core 入站服务端实现：组合公开 http::Server，不引用 HTTP 内部头。
// 解析/local-min/关联/错误信封语义沿用旧 RPC runtime 契约（docs/decisions/0009-rpc-runtime.md）。

#include <bbt/infra/rpc/Server.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <utility>

#include <bbt/infra/rpc/detail/Core.hpp>

namespace bbt::infra::rpc {
namespace {

// 单请求处理：wire 解析 → deadline local-min → handler → 关联校验 → 回信封。
// 任何失败都转成 wire error envelope（HTTP 200），不把错误当成功 payload；
// 解析失败无法取关联字段时用确定占位值（unparsed/unknown）。
result<http::Response> HandleRpcRequest(const IncomingCallContext& context,
                                        http::IncomingRequest request,
                                        RpcHandler& handler) {
    auto parsed = ParseRpcWireHttpRequest(request);
    if (!parsed) {
        return MakeRpcWireHttpResponse(
            detail::ErrorEnvelope(parsed.error(), "", "", "", "", ""));
    }
    const auto& wire = parsed.value();
    auto payload = FromWireEnvelope(wire);
    if (!payload) {
        return MakeRpcWireHttpResponse(detail::ErrorEnvelope(
            payload.error(), wire.request_id, wire.service, wire.method,
            wire.request_schema, wire.response_schema));
    }

    IncomingCallContext effective = context;
    const auto remote_deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds{wire.remaining_budget_ms};
    effective.deadline = std::min(context.deadline, remote_deadline);
    if (effective.deadline <= std::chrono::steady_clock::now()) {
        return MakeRpcWireHttpResponse(detail::ErrorEnvelope(
            MakeError(ErrorCode::TimedOut, "rpc request deadline expired"),
            wire.request_id, wire.service, wire.method,
            wire.request_schema, wire.response_schema));
    }

    const auto handler_deadline = effective.deadline;
    auto handled = handler(std::move(effective), std::move(payload.value()));
    if (!handled) {
        return MakeRpcWireHttpResponse(detail::ErrorEnvelope(
            handled.error(), wire.request_id, wire.service, wire.method,
            wire.request_schema, wire.response_schema));
    }
    auto reply = std::move(handled.value());
    if (reply.request_id != wire.request_id ||
        reply.service != wire.service ||
        reply.method != wire.method ||
        reply.request_schema != wire.request_schema ||
        reply.response_schema != wire.response_schema) {
        return MakeRpcWireHttpResponse(detail::ErrorEnvelope(
            MakeError(ErrorCode::ProtocolError,
                      "rpc handler changed association fields"),
            wire.request_id, wire.service, wire.method,
            wire.request_schema, wire.response_schema));
    }

    result<void> budget_status = result<void>::ok();
    const auto budget = detail::RemainingBudgetMs(handler_deadline, budget_status);
    if (!budget_status) {
        return MakeRpcWireHttpResponse(detail::ErrorEnvelope(
            budget_status.error(), wire.request_id, wire.service,
            wire.method, wire.request_schema, wire.response_schema));
    }
    auto out = ToWireEnvelope(reply, budget);
    if (!out) {
        return MakeRpcWireHttpResponse(detail::ErrorEnvelope(
            out.error(), wire.request_id, wire.service, wire.method,
            wire.request_schema, wire.response_schema));
    }
    return MakeRpcWireHttpResponse(out.value());
}

} // namespace

struct Server::State {
    std::shared_ptr<http::Server> http;
    std::atomic<bool> closed{false};
};

Server::Server(std::shared_ptr<State> state) noexcept
    : m_state(std::move(state)) {}

Server::~Server() noexcept { Close(); }

result<std::shared_ptr<Server>> Server::Create(
    std::shared_ptr<net::Engine> engine, RpcHandler handler,
    http::ServerOptions options) {
    if (!engine) {
        return result<std::shared_ptr<Server>>::err(MakeError(
            ErrorCode::InvalidArgument,
            "rpc server: engine required (server 不自建执行底座)"));
    }
    if (!handler) {
        return result<std::shared_ptr<Server>>::err(MakeError(
            ErrorCode::InvalidArgument, "rpc server: handler must not be null"));
    }
    // handler 适配：Capture RPC handler，交 http::Server 的 owning request。
    auto made = http::Server::Create(
        std::move(engine),
        [handler = std::move(handler)](IncomingCallContext context,
                                       http::IncomingRequest request)
            mutable -> result<http::Response> {
            return HandleRpcRequest(context, std::move(request), handler);
        },
        std::move(options));
    if (!made) {
        return result<std::shared_ptr<Server>>::err(made.error());
    }
    auto state = std::make_shared<State>();
    state->http = std::move(made).value();
    return result<std::shared_ptr<Server>>::ok(
        std::shared_ptr<Server>(new Server(std::move(state))));
}

result<void> Server::Start() { return m_state->http->Start(); }

void Server::StopAccepting() noexcept { m_state->http->StopAccepting(); }

result<void> Server::Drain(bbt::coroutine::Deadline deadline) {
    return m_state->http->Drain(deadline);
}

void Server::Close() noexcept {
    if (!m_state) { return; }
    m_state->closed.store(true);
    m_state->http->Close();
}

result<void> Server::WaitStopped(bbt::coroutine::Deadline deadline) {
    return m_state->http->WaitStopped(deadline);
}

bool Server::IsClosed() const noexcept {
    return !m_state || m_state->closed.load();
}

} // namespace bbt::infra::rpc
