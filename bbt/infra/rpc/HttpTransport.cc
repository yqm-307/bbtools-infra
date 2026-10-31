// RPC HTTP adapter 实现：HttpRpcTransport/HttpRpcInboundTransport 组合公开
// http::Client/http::Server，不引用 HTTP 内部头；HTTP body 绑定助手与
// rpc::Client/Server 源兼容工厂同 TU（adapter 物理域归 HTTP）。

#include <bbt/infra/rpc/HttpTransport.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <string>
#include <string_view>
#include <utility>

#include <bbt/infra/rpc/Client.hpp>
#include <bbt/infra/rpc/Server.hpp>
#include <bbt/infra/rpc/detail/Core.hpp>

namespace bbt::infra::rpc {
namespace {

Error Invalid(std::string message) {
    return MakeError(ErrorCode::InvalidArgument,
                     "rpc http transport: " + std::move(message));
}

// 只做 authority 的局部边界检查：HTTP RPC endpoint 必须是裸 authority
// （host[:port]），不得夹带 scheme/path/query/fragment/userinfo 或空白/
// 控制字符。host/port 的完整合法性仍由公开 http::Client 自己的 URL 校验
// 负责，这里不复制 HTTP parser（沿用旧 rpc::Client 检查，搬入 adapter）。
bool IsHttpRpcAuthority(std::string_view endpoint) noexcept {
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

// --- HTTP 字段辅助（bbt::infra::http 值类型） ---------------------------------

bool HasHeader(const std::vector<std::pair<std::string, std::string>>& hs,
               const std::string& name, const std::string& expect) {
    for (const auto& [k, v] : hs)
        if (EqualsNoCase(k, name) && v == expect)
            return true;
    return false;
}

// 新 http::Server 只交付 origin-form target（无 scheme/host）；RPC wire 固定
// path，不接受 query/fragment 变体。
result<void> CheckHttpRequestShape(const http::IncomingRequest& req) {
    if (req.method != "POST")
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "rpc wire requires POST"));
    if (req.target != kRpcWireHttpPath)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "rpc wire path mismatch"));
    if (!HasHeader(req.headers, kRpcWireContentTypeKey, kRpcWireContentType))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "rpc wire requires Content-Type: application/x-protobuf"));
    if (req.body.empty())
        return result<void>::err(MakeError(ErrorCode::ProtocolError,
            "rpc wire empty body"));
    return result<void>::ok();
}

result<void> CheckHttpResponseShape(const http::Response& resp) {
    if (resp.status_code != 200)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "rpc wire response requires HTTP 200"));
    if (!HasHeader(resp.headers, kRpcWireContentTypeKey, kRpcWireContentType))
        return result<void>::err(MakeError(ErrorCode::ProtocolError,
            "rpc wire response missing Content-Type"));
    if (resp.body.empty())
        return result<void>::err(MakeError(ErrorCode::ProtocolError,
            "rpc wire response empty body"));
    return result<void>::ok();
}

} // namespace

// --- HTTP body 绑定 -----------------------------------------------------------

result<http::Response> MakeRpcWireHttpResponse(
    const RpcWireEnvelope& env) {
    auto body = EncodeRpcWireEnvelope(env, /*is_response=*/true);
    if (!body)
        return result<http::Response>::err(body.error());
    http::Response resp;
    resp.status_code = 200;
    resp.headers.emplace_back(kRpcWireContentTypeKey, kRpcWireContentType);
    resp.body.assign(reinterpret_cast<const char*>(body.value().data()),
                     body.value().size());
    return result<http::Response>::ok(std::move(resp));
}

result<RpcWireEnvelope> ParseRpcWireHttpRequest(
    const http::IncomingRequest& req) {
    if (auto r = CheckHttpRequestShape(req); !r)
        return result<RpcWireEnvelope>::err(r.error());
    return DecodeRpcWireEnvelope(req.body, /*is_response=*/false);
}

result<RpcWireEnvelope> ParseRpcWireHttpResponse(const http::Response& resp) {
    if (auto r = CheckHttpResponseShape(resp); !r)
        return result<RpcWireEnvelope>::err(r.error());
    return DecodeRpcWireEnvelope(resp.body, /*is_response=*/true);
}

// --- HttpRpcTransport ----------------------------------------------------------

HttpRpcTransport::HttpRpcTransport(
    std::shared_ptr<http::Client> http_client) noexcept
    : m_http(std::move(http_client)) {}

HttpRpcTransport::~HttpRpcTransport() noexcept { Close(); }

result<std::shared_ptr<HttpRpcTransport>> HttpRpcTransport::Create(
    std::shared_ptr<http::Client> http_client) {
    if (!http_client) {
        return result<std::shared_ptr<HttpRpcTransport>>::err(
            Invalid("http client required (adapter 不自建执行底座)"));
    }
    return result<std::shared_ptr<HttpRpcTransport>>::ok(
        std::shared_ptr<HttpRpcTransport>(
            new HttpRpcTransport(std::move(http_client))));
}

bbt_co_async(result<WireReply>) HttpRpcTransport::Exchange(
    const RpcAddress& address, WireRequest request,
    const CallOptions& options) {
    const auto http = m_http;  // 局部安全引用：在飞期间外部句柄可释放
    if (!http || http->IsClosed()) {
        return result<WireReply>::err(
            MakeError(ErrorCode::Closed, "rpc http transport: closed"));
    }
    if (address.transport != "http") {
        return result<WireReply>::err(Invalid(
            "address requires http transport"));
    }
    if (!IsHttpRpcAuthority(address.endpoint)) {
        return result<WireReply>::err(Invalid(
            "endpoint must be an authority without scheme or path"));
    }

    const std::string url =
        "http://" + address.endpoint + kRpcWireHttpPath;

    // 经公开 BuildRequest 完成 base_url/默认 headers/params/分档超时合并与解析，
    // 再收敛到固定 wire profile：target 必须是精确 /rpc、Content-Type 只有一个
    // application/x-protobuf。HTTP 的共享默认配置（授权头、查询参数、超时）仍
    // 参与合并，只有会破坏 profile 的这两项被覆盖；Send 内仍执行
    // ValidatePrepared，二者不一致会显式拒绝而不是发出坏请求。
    auto prepared = http->BuildRequest("POST", url, {});
    if (!prepared) {
        return result<WireReply>::err(prepared.error());
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
    outbound.body.assign(
        reinterpret_cast<const char*>(request.body.data()),
        request.body.size());

    // 传输/排队/取消/未知结果错误原样透传（含 request_phase 事实）。
    auto response = bbt_co_await http->Send(outbound, options);
    if (!response) {
        return result<WireReply>::err(response.error());
    }
    if (response.value().status_code != 200) {
        auto error = MakeError(
            ErrorCode::Unavailable,
            "rpc endpoint returned non-success HTTP status");
        error.backend_category = "http";
        error.backend_code = response.value().status_code;
        return result<WireReply>::err(std::move(error));
    }
    if (auto r = CheckHttpResponseShape(response.value()); !r) {
        return result<WireReply>::err(r.error());
    }
    WireReply reply;
    const auto& body = response.value().body;
    reply.body.assign(body.begin(), body.end());
    return result<WireReply>::ok(std::move(reply));
}

void HttpRpcTransport::Close() noexcept {
    if (m_http) { m_http->Close(); }
}

bool HttpRpcTransport::IsClosed() const noexcept {
    return !m_http || m_http->IsClosed();
}

// --- HttpRpcInboundTransport -----------------------------------------------------

struct HttpRpcInboundTransport::State {
    std::shared_ptr<http::Server> http;
    // wire handler 由本 State 与 rpc::Server 共同强持有（http::Server 内的
    // bridge lambda 只弱捕获 State，不成环）：在途调用 lock 期间引用稳定，
    // Server 释放后本 State 仍持有一份，回调不会悬空。
    std::shared_ptr<WireInboundHandler> handler;
    std::atomic<bool> closed{false};
};

HttpRpcInboundTransport::HttpRpcInboundTransport(
    std::shared_ptr<State> state) noexcept
    : m_state(std::move(state)) {}

HttpRpcInboundTransport::~HttpRpcInboundTransport() noexcept { Close(); }

result<std::shared_ptr<HttpRpcInboundTransport>>
HttpRpcInboundTransport::Create(std::shared_ptr<net::Engine> engine,
                                http::ServerOptions options) {
    if (!engine) {
        return result<std::shared_ptr<HttpRpcInboundTransport>>::err(MakeError(
            ErrorCode::InvalidArgument,
            "rpc http inbound: engine required (adapter 不自建执行底座)"));
    }
    // handler 在 Start 时经 Start(WireInboundHandler) 注入；先挂固定桥接 lambda。
    // 只弱捕获 State（http::Server 持有本 lambda，强捕获会让 State↔http::Server
    // 成环、Close 后 Engine 无法过期）；handler 本体与 rpc::Server 共享同一
    // shared_ptr 计数，在途调用期间引用稳定。
    auto state = std::make_shared<State>();
    auto made = http::Server::Create(
        std::move(engine),
        [weak = std::weak_ptr<State>(state)](
            IncomingCallContext context, http::IncomingRequest request)
            -> result<http::Response> {
            // 形状违规仍走 wire 契约：错误信封 + HTTP 200（与旧 RPC server
            // 行为逐位一致），不降级为 HTTP 500。无法取关联字段时占位
            // unparsed/unknown。
            if (auto shape = CheckHttpRequestShape(request); !shape) {
                return MakeRpcWireHttpResponse(
                    detail::ErrorEnvelope(shape.error(), "", "", "", "", ""));
            }
            const auto handler = [&] {
                if (const auto state = weak.lock()) {
                    return state->handler;
                }
                return std::shared_ptr<WireInboundHandler>{};
            }();
            if (!handler) {
                // adapter 已 Close/释放：handler 责任已随 rpc::Server 释放，
                // 回复 wire 错误信封（仍 200，与形状违规同一出口）。
                return MakeRpcWireHttpResponse(detail::ErrorEnvelope(
                    MakeError(ErrorCode::Closed, "rpc http inbound: closed"),
                    "", "", "", "", ""));
            }
            WireRequest wire;
            wire.body.assign(request.body.begin(), request.body.end());
            auto reply = (*handler)(std::move(context), std::move(wire));
            if (!reply) {
                return result<http::Response>::err(reply.error());
            }
            http::Response resp;
            resp.status_code = 200;
            resp.headers.emplace_back(kRpcWireContentTypeKey,
                                      kRpcWireContentType);
            resp.body.assign(
                reinterpret_cast<const char*>(reply.value().body.data()),
                reply.value().body.size());
            return result<http::Response>::ok(std::move(resp));
        },
        std::move(options));
    if (!made) {
        return result<std::shared_ptr<HttpRpcInboundTransport>>::err(
            made.error());
    }
    state->http = std::move(made).value();
    return result<std::shared_ptr<HttpRpcInboundTransport>>::ok(
        std::shared_ptr<HttpRpcInboundTransport>(
            new HttpRpcInboundTransport(std::move(state))));
}

result<void> HttpRpcInboundTransport::Start(WireInboundHandler handler) {
    if (!handler) {
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "rpc http inbound: handler must not be null"));
    }
    auto state = m_state;
    if (state->closed.load()) {
        return result<void>::err(MakeError(ErrorCode::Closed,
            "rpc http inbound: closed"));
    }
    // wire handler 与 rpc::Server 共同强持有同一计数：本 State 存一份，
    // http::Server 的 bridge lambda 只弱捕获 State——adapter 释放后 lambda
    // 失效、在途 lock 期间引用稳定、不成环。
    state->handler = std::make_shared<WireInboundHandler>(std::move(handler));
    return state->http->Start();
}

void HttpRpcInboundTransport::StopAccepting() noexcept {
    m_state->http->StopAccepting();
}

result<void> HttpRpcInboundTransport::Drain(
    bbt::coroutine::Deadline deadline) {
    return m_state->http->Drain(deadline);
}

void HttpRpcInboundTransport::Close() noexcept {
    if (!m_state) { return; }
    m_state->closed.store(true);
    m_state->http->Close();
}

result<void> HttpRpcInboundTransport::WaitStopped(
    bbt::coroutine::Deadline deadline) {
    return m_state->http->WaitStopped(deadline);
}

bool HttpRpcInboundTransport::IsClosed() const noexcept {
    return !m_state || m_state->closed.load();
}

// --- 源兼容工厂（architecture §10：下一迁移窗口结束前保留） ---------------------

result<std::shared_ptr<Client>> Client::Create(
    std::shared_ptr<http::Client> http_client) {
    auto transport = HttpRpcTransport::Create(std::move(http_client));
    if (!transport) {
        return result<std::shared_ptr<Client>>::err(transport.error());
    }
    return Create(std::shared_ptr<IRpcTransport>(
        std::move(transport).value()));
}

result<std::shared_ptr<Server>> Server::Create(
    std::shared_ptr<net::Engine> engine, RpcHandler handler) {
    return Create(std::move(engine), std::move(handler), http::ServerOptions{});
}

result<std::shared_ptr<Server>> Server::Create(
    std::shared_ptr<net::Engine> engine, RpcHandler handler,
    http::ServerOptions options) {
    auto inbound = HttpRpcInboundTransport::Create(
        std::move(engine), std::move(options));
    if (!inbound) {
        return result<std::shared_ptr<Server>>::err(inbound.error());
    }
    return Create(
        std::shared_ptr<IRpcInboundTransport>(std::move(inbound).value()),
        std::move(handler));
}

} // namespace bbt::infra::rpc
