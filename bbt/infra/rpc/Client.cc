// RPC core 出站客户端实现：组合 IRpcTransport seam，不引用任何物理栈头。
// wire/关联/预算语义见 bbt/infra/rpc/Client.hpp 与 RpcWire.hpp；
// HTTP 兼容工厂在 bbt/infra/rpc/HttpTransport.cc。

#include <bbt/infra/rpc/Client.hpp>

#include <utility>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/infra/rpc/detail/Core.hpp>
#include <bbt/infra/rpc/detail/Session.hpp>

namespace bbt::infra::rpc {
namespace {

Error Invalid(std::string message) {
    return MakeError(ErrorCode::InvalidArgument,
                     "rpc client: " + std::move(message));
}

} // namespace

Client::Client(std::shared_ptr<IRpcTransport> transport) noexcept
    : m_transport(std::move(transport)) {}

Client::Client(std::shared_ptr<SessionOwnerState> owner) noexcept
    : m_owner(std::move(owner)) {}

Client::~Client() noexcept { Close(); }

result<std::shared_ptr<Client>> Client::Create(
    std::shared_ptr<IRpcTransport> transport) {
    if (!transport) {
        return result<std::shared_ptr<Client>>::err(
            Invalid("transport required (client does not own an execution base)"));
    }
    return result<std::shared_ptr<Client>>::ok(
        bbt::coroutine::MakeCoObject<Client>("infra.rpc.Client", "", [&] {
            return std::shared_ptr<Client>(new Client(std::move(transport)));
        }));
}

bbt_co_async(result<RpcEnvelope>) Client::Call(const RpcAddress& address,
                                               RpcEnvelope request,
                                               const CallOptions& options) {
    if (!bbt::coroutine::IsInCoroutineContext()) {
        return result<RpcEnvelope>::err(
            MakeError(ErrorCode::InvalidContext,
                      "rpc client: coroutine context required"));
    }
    // 局部安全引用：调用期间外部 RPC/adapter 句柄可被释放，本协程仍持有
    // transport。
    const auto transport = m_transport;
    if (!transport || transport->IsClosed()) {
        return result<RpcEnvelope>::err(
            MakeError(ErrorCode::Closed, "rpc client: closed"));
    }
    // transport 字段只作 adapter 路由标识（"http"/"tcp"），endpoint 的完整
    // 合法性归 adapter（architecture §5.3）。
    if ((address.transport != "http" && address.transport != "tcp") ||
        address.endpoint.empty()) {
        return result<RpcEnvelope>::err(Invalid(
            "address requires http/tcp transport and non-empty endpoint"));
    }
    result<void> budget_status = result<void>::ok();
    const auto budget = detail::RemainingBudgetMs(options.deadline, budget_status);
    if (!budget_status) {
        return result<RpcEnvelope>::err(budget_status.error());
    }

    auto wire = ToWireEnvelope(request, budget);
    if (!wire) {
        return result<RpcEnvelope>::err(wire.error());
    }
    auto body = EncodeRpcWireEnvelope(wire.value(), /*is_response=*/false);
    if (!body) {
        return result<RpcEnvelope>::err(body.error());
    }

    // 传输/排队/取消/未知结果错误原样透传（含 request_phase 事实）。
    auto response = bbt_co_await transport->Exchange(
        address, WireRequest{std::move(body).value()}, options);
    if (!response) {
        return result<RpcEnvelope>::err(response.error());
    }
    auto decoded = DecodeRpcWireEnvelope(response.value().body,
                                         /*is_response=*/true);
    if (!decoded) {
        return result<RpcEnvelope>::err(decoded.error());
    }
    const auto& reply = decoded.value();
    if (reply.request_id != request.request_id ||
        reply.service != request.service ||
        reply.method != request.method ||
        reply.request_schema != request.request_schema ||
        reply.response_schema != request.response_schema) {
        return result<RpcEnvelope>::err(MakeError(
            ErrorCode::ProtocolError,
            "rpc reply envelope association mismatch"));
    }
    if (!reply.success) {
        return result<RpcEnvelope>::err(reply.error);
    }
    return FromWireEnvelope(reply);
}

void Client::Close(Error reason) noexcept {
    try {
        {
            std::lock_guard<std::mutex> lock(m_reason_mtx);
            if (!m_close_reason) { m_close_reason = reason; }
        }
        if (m_transport) { m_transport->Close(std::move(reason)); }
        if (m_owner) { detail::SessionOwnerSeal(m_owner, std::move(reason)); }
    } catch (...) {
        // noexcept 契约：封口状态已在上面写定，原因复制失败不放大。
    }
}

bool Client::IsClosed() const noexcept {
    if (m_owner) { return detail::SessionOwnerClosed(*m_owner); }
    return !m_transport || m_transport->IsClosed();
}

std::optional<Error> Client::CloseReason() const noexcept {
    {
        std::lock_guard<std::mutex> lock(m_reason_mtx);
        if (m_close_reason) { return m_close_reason; }
    }
    if (m_owner) { return detail::SessionOwnerCloseReason(*m_owner); }
    return std::nullopt;
}

result<void> Client::Shutdown(bbt::coroutine::Deadline deadline) {
    if (m_owner) { return detail::SessionOwnerShutdown(m_owner, deadline); }
    if (!m_transport) {
        return result<void>::err(
            MakeError(ErrorCode::Closed, "rpc client: closed"));
    }
    return m_transport->Shutdown(deadline);
}

} // namespace bbt::infra::rpc
