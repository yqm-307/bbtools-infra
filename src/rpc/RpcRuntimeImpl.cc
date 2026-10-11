// RPC runtime adapter：把公开 NetworkRuntime 装配面上的 RPC 工厂接到 HTTP
// owner。工厂是非虚普通成员（非 vtable 槽），只组合公开的
// CreateHttpClient/ListenHttp，不引用 HTTP 模块内部头。
//
// 本 TU 有互斥两分支：
//   - 启用锁定 Protobuf wire profile（BBT_INFRA_HAS_RPC_WIRE）：编入
//     bbt_infra_rpc（STATIC，wire + runtime），经 RpcWire 编解码。
//   - 未启用：编入 bbt_infra_http，只提供返回 RuntimeUnavailable 的 stub，
//     不产生假成功，也不让 HTTP-only 消费者链接 protobuf。
#include <bbt/infra/NetworkRuntime.hpp>

#include <algorithm>
#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/HttpServer.hpp>
#if defined(BBT_INFRA_HAS_RPC_WIRE)
#include <bbt/infra/rpc/RpcWire.hpp>
#endif

namespace bbt::infra {
#if defined(BBT_INFRA_HAS_RPC_WIRE)
namespace {

using rpc::RpcWireEnvelope;

// 只做 authority 的局部边界检查：RPC endpoint 必须是裸 authority（host[:port]），
// 不得夹带 scheme/path/query/fragment/userinfo 或空白/控制字符。host/port 的
// 完整合法性仍由公开 HttpClient 自己的 URL 校验负责，这里不复制 HTTP parser。
bool IsRpcAuthority(std::string_view endpoint) noexcept {
    if (endpoint.empty())
        return false;
    if (endpoint.find("://") != std::string_view::npos)
        return false; // scheme
    for (const char ch : endpoint) {
        const auto c = static_cast<unsigned char>(ch);
        if (c <= 0x20 || c == 0x7f)
            return false; // 空白/控制字符/DEL
        if (ch == '/' || ch == '?' || ch == '#')
            return false; // path/query/fragment
        if (ch == '@')
            return false; // userinfo
    }
    return true;
}

result<std::string> RpcBaseUrl(const RpcAddress& address) {
    if (address.transport != "http" || address.endpoint.empty())
        return result<std::string>::err(MakeError(
            ErrorCode::InvalidArgument,
            "rpc client requires http transport and non-empty endpoint"));
    if (!IsRpcAuthority(address.endpoint))
        return result<std::string>::err(MakeError(
            ErrorCode::InvalidArgument,
            "rpc endpoint must be an authority without scheme or path"));
    return result<std::string>::ok("http://" + address.endpoint);
}

std::uint32_t RemainingBudgetMs(const bbt::coroutine::Deadline& deadline,
                                result<void>& status) {
    const auto now = std::chrono::steady_clock::now();
    if (deadline <= now) {
        status = result<void>::err(
            MakeError(ErrorCode::TimedOut, "rpc deadline already expired"));
        return 0;
    }
    auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
        deadline - now).count();
    if (remaining < 1)
        remaining = 1;
    if (remaining > static_cast<long long>(rpc::kMaxRemainingBudgetMs))
        remaining = static_cast<long long>(rpc::kMaxRemainingBudgetMs);
    status = result<void>::ok();
    return static_cast<std::uint32_t>(remaining);
}


RpcWireEnvelope ErrorEnvelope(const Error& error,
                              std::string request_id,
                              std::string service,
                              std::string method,
                              std::string request_schema,
                              std::string response_schema) {
    RpcWireEnvelope out;
    out.profile_version = rpc::kRpcWireProfileVersion;
    out.request_id = request_id.empty() ? "unparsed" : std::move(request_id);
    out.service = service.empty() ? "unknown" : std::move(service);
    out.method = method.empty() ? "unknown" : std::move(method);
    out.request_schema = std::move(request_schema);
    out.response_schema = std::move(response_schema);
    out.remaining_budget_ms = 1;
    out.success = false;
    out.error = error;
    return out;
}

class RpcClientImpl final : public RpcClient {
public:
    explicit RpcClientImpl(std::shared_ptr<HttpClient> client)
        : m_client(std::move(client)) {}

    result<RpcEnvelope> Call(const RpcAddress& address,
                             RpcEnvelope request,
                             const CallOptions& options) override {
        auto base_url = RpcBaseUrl(address);
        if (!base_url)
            return result<RpcEnvelope>::err(base_url.error());
        result<void> budget_status = result<void>::ok();
        const auto budget = RemainingBudgetMs(options.deadline, budget_status);
        if (!budget_status)
            return result<RpcEnvelope>::err(budget_status.error());

        auto wire = rpc::ToWireEnvelope(request, budget);
        if (!wire)
            return result<RpcEnvelope>::err(wire.error());
        auto http_request = rpc::MakeRpcWireHttpRequest(
            wire.value(), base_url.value());
        if (!http_request)
            return result<RpcEnvelope>::err(http_request.error());

        auto response = m_client->Request(std::move(http_request.value()),
                                           options);
        if (!response)
            return result<RpcEnvelope>::err(response.error());
        if (response.value().status != 200) {
            auto error = MakeError(
                ErrorCode::Unavailable,
                "rpc endpoint returned non-success HTTP status");
            error.backend_category = "http";
            error.backend_code = static_cast<int>(response.value().status);
            return result<RpcEnvelope>::err(std::move(error));
        }
        auto decoded = rpc::ParseRpcWireHttpResponse(response.value());
        if (!decoded)
            return result<RpcEnvelope>::err(decoded.error());
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
        if (!reply.success)
            return result<RpcEnvelope>::err(reply.error);
        return rpc::FromWireEnvelope(reply);
    }

    void Close() noexcept override { m_client->Close(); }
    bool IsClosed() const noexcept override { return m_client->IsClosed(); }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return m_client->GetObjectInfo();
    }

private:
    std::shared_ptr<HttpClient> m_client;
};

class RpcServerImpl final : public RpcServer {
public:
    explicit RpcServerImpl(std::shared_ptr<HttpServer> server)
        : m_server(std::move(server)) {}

    ListenAddress LocalAddress() const override { return m_server->LocalAddress(); }
    void StopAccepting() noexcept override { m_server->StopAccepting(); }
    void Close() noexcept override { m_server->Close(); }
    bool IsClosed() const noexcept override { return m_server->IsClosed(); }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return m_server->GetObjectInfo();
    }

private:
    std::shared_ptr<HttpServer> m_server;
};

result<HttpResponse> HandleRpcRequest(const IncomingCallContext& context,
                                      HttpRequest request,
                                      RpcHandler& handler) {
    auto parsed = rpc::ParseRpcWireHttpRequest(request);
    if (!parsed) {
        return rpc::MakeRpcWireHttpResponse(
            ErrorEnvelope(parsed.error(), "", "", "", "", ""));
    }
    const auto& wire = parsed.value();
    auto payload = rpc::FromWireEnvelope(wire);
    if (!payload) {
        return rpc::MakeRpcWireHttpResponse(
            ErrorEnvelope(payload.error(), wire.request_id, wire.service,
                          wire.method, wire.request_schema,
                          wire.response_schema));
    }

    IncomingCallContext effective = context;
    const auto remote_deadline = std::chrono::steady_clock::now() +
        std::chrono::milliseconds{wire.remaining_budget_ms};
    effective.deadline = std::min(context.deadline, remote_deadline);
    if (effective.deadline <= std::chrono::steady_clock::now()) {
        return rpc::MakeRpcWireHttpResponse(
            ErrorEnvelope(MakeError(ErrorCode::TimedOut,
                                    "rpc request deadline expired"),
                          wire.request_id, wire.service, wire.method,
                          wire.request_schema, wire.response_schema));
    }

    const auto handler_deadline = effective.deadline;
    auto handled = handler(std::move(effective), std::move(payload.value()));
    if (!handled) {
        return rpc::MakeRpcWireHttpResponse(
            ErrorEnvelope(handled.error(), wire.request_id, wire.service,
                          wire.method, wire.request_schema,
                          wire.response_schema));
    }
    auto reply = std::move(handled.value());
    if (reply.request_id != wire.request_id ||
        reply.service != wire.service ||
        reply.method != wire.method ||
        reply.request_schema != wire.request_schema ||
        reply.response_schema != wire.response_schema) {
        return rpc::MakeRpcWireHttpResponse(ErrorEnvelope(
            MakeError(ErrorCode::ProtocolError,
                      "rpc handler changed association fields"),
            wire.request_id, wire.service, wire.method,
            wire.request_schema, wire.response_schema));
    }

    result<void> budget_status = result<void>::ok();
    const auto budget = RemainingBudgetMs(handler_deadline, budget_status);
    if (!budget_status)
        return rpc::MakeRpcWireHttpResponse(
            ErrorEnvelope(budget_status.error(), wire.request_id,
                          wire.service, wire.method, wire.request_schema,
                          wire.response_schema));
    auto out = rpc::ToWireEnvelope(reply, budget);
    if (!out)
        return rpc::MakeRpcWireHttpResponse(
            ErrorEnvelope(out.error(), wire.request_id, wire.service,
                          wire.method, wire.request_schema,
                          wire.response_schema));
    return rpc::MakeRpcWireHttpResponse(out.value());
}

} // namespace

result<std::shared_ptr<RpcClient>>
NetworkRuntime::CreateRpcClient() {
    auto client = CreateHttpClient();
    if (!client)
        return result<std::shared_ptr<RpcClient>>::err(client.error());
    return result<std::shared_ptr<RpcClient>>::ok(
        std::make_shared<RpcClientImpl>(std::move(client.value())));
}

result<std::shared_ptr<RpcServer>>
NetworkRuntime::ListenRpc(ListenAddress address,
                          RpcHandler handler) {
    if (!handler)
        return result<std::shared_ptr<RpcServer>>::err(MakeError(
            ErrorCode::InvalidArgument, "ListenRpc: handler must not be null"));
    auto http_server = ListenHttp(
        std::move(address),
        [handler = std::move(handler)](IncomingCallContext context,
                                       HttpRequest request)
            mutable -> result<HttpResponse> {
            return HandleRpcRequest(context, std::move(request), handler);
        });
    if (!http_server)
        return result<std::shared_ptr<RpcServer>>::err(http_server.error());
    return result<std::shared_ptr<RpcServer>>::ok(
        std::make_shared<RpcServerImpl>(std::move(http_server.value())));
}

#else

// 无 wire stub：同一 TU 的另一分支，编入 bbt_infra_http。让只链接
// bbt::infra_http 的消费者仍能链接并在调用时得到 RuntimeUnavailable。
result<std::shared_ptr<RpcClient>>
NetworkRuntime::CreateRpcClient() {
    return result<std::shared_ptr<RpcClient>>::err(MakeError(
        ErrorCode::RuntimeUnavailable,
        "bbt_infra_rpc requires a locked BBT_PROTOBUF_PREFIX"));
}

result<std::shared_ptr<RpcServer>>
NetworkRuntime::ListenRpc(ListenAddress,
                          RpcHandler) {
    return result<std::shared_ptr<RpcServer>>::err(MakeError(
        ErrorCode::RuntimeUnavailable,
        "bbt_infra_rpc requires a locked BBT_PROTOBUF_PREFIX"));
}

#endif

} // namespace bbt::infra
