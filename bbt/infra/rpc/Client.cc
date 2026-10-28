// RPC core 出站客户端实现：组合公开 http::Client，不引用 HTTP 内部头。
// wire/关联/预算语义见 bbt/infra/rpc/Client.hpp 与 RpcWire.hpp。

#include <bbt/infra/rpc/Client.hpp>

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/infra/rpc/detail/Core.hpp>

namespace bbt::infra::rpc {
namespace {

Error Invalid(std::string message) {
    return MakeError(ErrorCode::InvalidArgument,
                     "rpc client: " + std::move(message));
}

bool EqualsNoCase(std::string_view left, std::string_view right) {
    if (left.size() != right.size()) { return false; }
    for (std::size_t i = 0; i < left.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(left[i])) !=
            std::tolower(static_cast<unsigned char>(right[i]))) {
            return false;
        }
    }
    return true;
}

bool IsContentType(std::string_view name) {
    return EqualsNoCase(name, kRpcWireContentTypeKey);
}

} // namespace

Client::Client(std::shared_ptr<http::Client> transport) noexcept
    : m_transport(std::move(transport)) {}

Client::~Client() noexcept { Close(); }

result<std::shared_ptr<Client>> Client::Create(
    std::shared_ptr<http::Client> transport) {
    if (!transport) {
        return result<std::shared_ptr<Client>>::err(
            Invalid("transport required (client 不自建执行底座)"));
    }
    return result<std::shared_ptr<Client>>::ok(
        std::shared_ptr<Client>(new Client(std::move(transport))));
}

bbt_co_async(result<RpcEnvelope>) Client::Call(const RpcAddress& address,
                                               RpcEnvelope request,
                                               const CallOptions& options) {
    // 局部安全引用：调用期间外部 RPC/HTTP 句柄可被释放，本协程仍持有 transport。
    const auto transport = m_transport;
    if (!transport || transport->IsClosed()) {
        return result<RpcEnvelope>::err(
            MakeError(ErrorCode::Closed, "rpc client: closed"));
    }
    if (bbt::coroutine::GetLocalCoroutineId() == 0) {
        return result<RpcEnvelope>::err(
            MakeError(ErrorCode::InvalidContext,
                      "rpc client: coroutine context required"));
    }
    if (address.transport != "http" || address.endpoint.empty()) {
        return result<RpcEnvelope>::err(Invalid(
            "address requires http transport and non-empty endpoint"));
    }
    if (!detail::IsRpcAuthority(address.endpoint)) {
        return result<RpcEnvelope>::err(Invalid(
            "endpoint must be an authority without scheme or path"));
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

    const std::string url =
        "http://" + address.endpoint + kRpcWireHttpPath;

    // 经公开 BuildRequest 完成 base_url/默认 headers/params/分档超时合并与解析，
    // 再收敛到固定 wire profile：target 必须是精确 /rpc、Content-Type 只有一个
    // application/x-protobuf。HTTP 的共享默认配置（授权头、查询参数、超时）仍
    // 参与合并，只有会破坏 profile 的这两项被覆盖；Send 内仍执行
    // ValidatePrepared，二者不一致会显式拒绝而不是发出坏请求。
    auto prepared = transport->BuildRequest("POST", url, {});
    if (!prepared) {
        return result<RpcEnvelope>::err(prepared.error());
    }
    auto& outbound = prepared.value();
    outbound.url = url;
    outbound.target_for_wire = kRpcWireHttpPath;
    outbound.headers.erase(
        std::remove_if(outbound.headers.begin(), outbound.headers.end(),
                       [](const http::Headers::value_type& header) {
                           return IsContentType(header.first);
                       }),
        outbound.headers.end());
    outbound.headers.emplace_back(kRpcWireContentTypeKey, kRpcWireContentType);
    outbound.body.assign(reinterpret_cast<const char*>(body.value().data()),
                         body.value().size());

    // 传输/排队/取消/未知结果错误原样透传（含 request_phase 事实）。
    auto response = bbt_co_await transport->Send(outbound, options);
    if (!response) {
        return result<RpcEnvelope>::err(response.error());
    }
    if (response.value().status_code != 200) {
        auto error = MakeError(
            ErrorCode::Unavailable,
            "rpc endpoint returned non-success HTTP status");
        error.backend_category = "http";
        error.backend_code = response.value().status_code;
        return result<RpcEnvelope>::err(std::move(error));
    }
    auto decoded = ParseRpcWireHttpResponse(response.value());
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

void Client::Close() noexcept {
    if (m_transport) { m_transport->Close(); }
}

bool Client::IsClosed() const noexcept {
    return !m_transport || m_transport->IsClosed();
}

} // namespace bbt::infra::rpc
