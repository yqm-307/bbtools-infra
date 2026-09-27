// bbtools-infra Issue #8：RpcEnvelope wire profile 的 protobuf adapter 实现。
// 公开面见 include/bbt/infra/rpc/RpcWire.hpp；protobuf 生成物只在本文件
// 与 protoc 生成目录内出现，不向公共头泄漏。

#include <bbt/infra/rpc/RpcWire.hpp>

#include <algorithm>
#include <cstring>
#include <string>
#include <utility>

#include "bbt/infra/rpc/v1/rpc_envelope.pb.h"

namespace bbt::infra::rpc {
namespace {

// proto 生成物保留 wire 版本 namespace（bbt::infra::rpc::v1，与 proto
// package bbt.infra.rpc.v1 对应）；公开 C++ API 收敛在 bbt::infra::rpc。
// 内部实现显式引用生成类型，不把 v1 透传到公开面。
namespace pbv1 = ::bbt::infra::rpc::v1;

// --- 错误域映射 -------------------------------------------------------------

pbv1::RpcErrorCode ToProtoErrorCode(ErrorCode c) {
    switch (c) {
        case ErrorCode::InvalidArgument:   return pbv1::RPC_ERROR_CODE_INVALID_ARGUMENT;
        case ErrorCode::InvalidContext:    return pbv1::RPC_ERROR_CODE_INVALID_CONTEXT;
        case ErrorCode::RuntimeUnavailable:return pbv1::RPC_ERROR_CODE_RUNTIME_UNAVAILABLE;
        case ErrorCode::Closed:            return pbv1::RPC_ERROR_CODE_CLOSED;
        case ErrorCode::Cancelled:         return pbv1::RPC_ERROR_CODE_CANCELLED;
        case ErrorCode::TimedOut:          return pbv1::RPC_ERROR_CODE_TIMED_OUT;
        case ErrorCode::Overloaded:        return pbv1::RPC_ERROR_CODE_OVERLOADED;
        case ErrorCode::Unavailable:       return pbv1::RPC_ERROR_CODE_UNAVAILABLE;
        case ErrorCode::TransportError:    return pbv1::RPC_ERROR_CODE_TRANSPORT_ERROR;
        case ErrorCode::ProtocolError:     return pbv1::RPC_ERROR_CODE_PROTOCOL_ERROR;
        case ErrorCode::NotFound:          return pbv1::RPC_ERROR_CODE_NOT_FOUND;
        case ErrorCode::TypeMismatch:      return pbv1::RPC_ERROR_CODE_TYPE_MISMATCH;
        case ErrorCode::UnsupportedRoute:  return pbv1::RPC_ERROR_CODE_UNSUPPORTED_ROUTE;
        case ErrorCode::OutcomeUnknown:    return pbv1::RPC_ERROR_CODE_OUTCOME_UNKNOWN;
        case ErrorCode::RemoteError:       return pbv1::RPC_ERROR_CODE_REMOTE_ERROR;
        case ErrorCode::InternalError:     return pbv1::RPC_ERROR_CODE_INTERNAL_ERROR;
    }
    return pbv1::RPC_ERROR_CODE_INTERNAL_ERROR;
}

ErrorCode FromProtoErrorCode(pbv1::RpcErrorCode c) {
    switch (c) {
        case pbv1::RPC_ERROR_CODE_INVALID_ARGUMENT:    return ErrorCode::InvalidArgument;
        case pbv1::RPC_ERROR_CODE_INVALID_CONTEXT:     return ErrorCode::InvalidContext;
        case pbv1::RPC_ERROR_CODE_RUNTIME_UNAVAILABLE: return ErrorCode::RuntimeUnavailable;
        case pbv1::RPC_ERROR_CODE_CLOSED:              return ErrorCode::Closed;
        case pbv1::RPC_ERROR_CODE_CANCELLED:           return ErrorCode::Cancelled;
        case pbv1::RPC_ERROR_CODE_TIMED_OUT:           return ErrorCode::TimedOut;
        case pbv1::RPC_ERROR_CODE_OVERLOADED:          return ErrorCode::Overloaded;
        case pbv1::RPC_ERROR_CODE_UNAVAILABLE:         return ErrorCode::Unavailable;
        case pbv1::RPC_ERROR_CODE_TRANSPORT_ERROR:     return ErrorCode::TransportError;
        case pbv1::RPC_ERROR_CODE_PROTOCOL_ERROR:      return ErrorCode::ProtocolError;
        case pbv1::RPC_ERROR_CODE_NOT_FOUND:           return ErrorCode::NotFound;
        case pbv1::RPC_ERROR_CODE_TYPE_MISMATCH:       return ErrorCode::TypeMismatch;
        case pbv1::RPC_ERROR_CODE_UNSUPPORTED_ROUTE:   return ErrorCode::UnsupportedRoute;
        case pbv1::RPC_ERROR_CODE_OUTCOME_UNKNOWN:     return ErrorCode::OutcomeUnknown;
        case pbv1::RPC_ERROR_CODE_REMOTE_ERROR:        return ErrorCode::RemoteError;
        case pbv1::RPC_ERROR_CODE_INTERNAL_ERROR:      return ErrorCode::InternalError;
        case pbv1::RPC_ERROR_CODE_UNSPECIFIED:         break;
        default: break;
    }
    // 未识别/未指定的 code：按 proto3 兼容规则不静默吞掉，归为 ProtocolError。
    return ErrorCode::ProtocolError;
}

// --- metadata 校验 -----------------------------------------------------------

bool HasMetaPrefix(const std::string& key) {
    return key.rfind(kMetaPrefixFw, 0) == 0 ||
           key.rfind(kMetaPrefixRoute, 0) == 0 ||
           key.rfind(kMetaPrefixTrace, 0) == 0;
}

result<void> ValidateMetadata(
    const std::vector<std::pair<std::string, std::string>>& md) {
    if (md.size() > kMaxMetadataItems)
        return result<void>::err(MakeError(ErrorCode::ProtocolError,
            "metadata exceeds max item count"));
    for (std::size_t i = 0; i < md.size(); ++i) {
        const auto& [k, v] = md[i];
        if (k.empty() || k.size() > kMaxMetadataKeyBytes)
            return result<void>::err(MakeError(ErrorCode::ProtocolError,
                "metadata key empty or exceeds max bytes"));
        if (v.size() > kMaxMetadataValueBytes)
            return result<void>::err(MakeError(ErrorCode::ProtocolError,
                "metadata value exceeds max bytes"));
        if (!HasMetaPrefix(k))
            return result<void>::err(MakeError(ErrorCode::ProtocolError,
                "metadata key uses unregistered prefix"));
        if (!detail::IsValidUtf8(k) || !detail::IsValidUtf8(v))
            return result<void>::err(MakeError(ErrorCode::ProtocolError,
                "metadata not valid UTF-8"));
        for (std::size_t j = 0; j < i; ++j)
            if (md[j].first == k)
                return result<void>::err(MakeError(ErrorCode::ProtocolError,
                    "metadata duplicate key"));
    }
    return result<void>::ok();
}

// --- Error 分支校验 ----------------------------------------------------------
// ErrorCode/domain/domain_code/message/backend_* 的 UTF-8 与长度边界在编码前
// 整体校验，与 details 共用同一上限集合；违规以 InvalidArgument 拒绝，
// 不产生部分编码。
result<void> ValidateErrorBranch(const Error& err) {
    if (!detail::IsValidUtf8(err.domain) ||
        !detail::IsValidUtf8(err.domain_code) ||
        !detail::IsValidUtf8(err.message) ||
        !detail::IsValidUtf8(err.backend_category))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "error string fields not valid UTF-8"));
    if (err.domain.size() > kMaxKeyBytes ||
        err.domain_code.size() > kMaxKeyBytes ||
        err.backend_category.size() > kMaxKeyBytes)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "error domain/domain_code/backend_category exceeds max bytes"));
    if (err.message.size() > kMaxValueBytes)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "error message exceeds max bytes"));
    // ValidateErrorDetails 用 ProtocolError 表达结构违规（decisions/0002 与
    // Result.hpp:157-177 的冻结语义）：重复键、非法 UTF-8、超限 details
    // 必须原样保留 ProtocolError，不得改写为 InvalidArgument（R1 Issue-2）。
    if (auto r = ValidateErrorDetails(err); !r)
        return r;
    return result<void>::ok();
}

// --- proto ⇄ 值类型 ----------------------------------------------------------

result<void> FillProto(const RpcWireEnvelope& env,
                       pbv1::RpcEnvelopeMsg* out,
                       bool is_response) {
    if (env.profile_version != kRpcWireProfileVersion)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "unsupported profile_version"));
    if (env.request_id.empty() || env.service.empty() || env.method.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "request_id/service/method must be non-empty"));
    if (!detail::IsValidUtf8(env.request_id) ||
        !detail::IsValidUtf8(env.service) ||
        !detail::IsValidUtf8(env.method) ||
        !detail::IsValidUtf8(env.request_schema) ||
        !detail::IsValidUtf8(env.response_schema))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "identifier fields not valid UTF-8"));
    // Issue #8：remaining_budget_ms 为 0 明确拒绝（不取本地预算兜底）；
    // 越界同样拒绝。接收端 local-min clamp 由上层 listener 完成，本层只钉死
    // wire 上的合法范围 1..kMaxRemainingBudgetMs。
    if (env.remaining_budget_ms == 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "remaining_budget_ms zero is rejected"));
    if (env.remaining_budget_ms > kMaxRemainingBudgetMs)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "remaining_budget_ms exceeds max"));
    if (auto r = ValidateMetadata(env.metadata); !r)
        return r;
    // 请求方向不允许携带 outcome：success=false 的 request 会被静默丢弃
    // error 分支（编码端不写 outcome，解码端按 request 无 outcome 解读），
    // 违反 wire 边界。编码前显式拒绝（R1 kanban_run_63 Issue-1）。
    if (!is_response && !env.success)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "request envelope must not carry failure outcome "
            "(success=false is response-only)"));
    if (is_response && !env.success) {
        if (auto r = ValidateErrorBranch(env.error); !r)
            return r;
    }

    out->set_profile_version(env.profile_version);
    out->set_request_id(env.request_id);
    out->set_service(env.service);
    out->set_method(env.method);
    out->set_remaining_budget_ms(env.remaining_budget_ms);
    out->set_request_schema(env.request_schema);
    out->set_response_schema(env.response_schema);
    if (!env.payload.empty())
        out->set_payload(env.payload.data(), env.payload.size());
    for (const auto& [k, v] : env.metadata) {
        auto* e = out->add_metadata();
        e->set_key(k);
        e->set_value(v);
    }

    if (is_response) {
        if (env.success) {
            out->set_success(true);
        } else {
            auto* pe = out->mutable_error();
            pe->set_code(ToProtoErrorCode(env.error.code));
            pe->set_domain(env.error.domain);
            pe->set_domain_code(env.error.domain_code);
            pe->set_message(env.error.message);
            pe->set_backend_category(env.error.backend_category);
            pe->set_backend_code(env.error.backend_code);
            pe->set_transferred_bytes(env.error.transferred_bytes);
            for (const auto& [k, v] : env.error.details) {
                auto* de = pe->add_details();
                de->set_key(k);
                de->set_value(v);
            }
        }
    }
    return result<void>::ok();
}

result<RpcWireEnvelope> FromProto(const pbv1::RpcEnvelopeMsg& in,
                                  bool is_response) {
    if (in.profile_version() != kRpcWireProfileVersion)
        return result<RpcWireEnvelope>::err(MakeError(
            ErrorCode::ProtocolError, "unsupported profile_version"));

    RpcWireEnvelope env;
    env.profile_version     = in.profile_version();
    env.request_id          = in.request_id();
    env.service             = in.service();
    env.method              = in.method();
    env.remaining_budget_ms = in.remaining_budget_ms();
    env.request_schema      = in.request_schema();
    env.response_schema     = in.response_schema();
    const auto& p = in.payload();
    env.payload.assign(p.begin(), p.end());
    env.metadata.reserve(in.metadata_size());
    for (const auto& e : in.metadata())
        env.metadata.emplace_back(e.key(), e.value());

    if (is_response) {
        if (in.has_error()) {
            env.success = false;
            const auto& pe = in.error();
            env.error.code              = FromProtoErrorCode(pe.code());
            env.error.domain            = pe.domain();
            env.error.domain_code       = pe.domain_code();
            env.error.message           = pe.message();
            env.error.backend_category  = pe.backend_category();
            env.error.backend_code      = pe.backend_code();
            env.error.transferred_bytes = pe.transferred_bytes();
            env.error.details.reserve(pe.details_size());
            for (const auto& de : pe.details())
                env.error.details.emplace_back(de.key(), de.value());
        } else if (in.has_success()) {
            env.success = true;
        } else {
            return result<RpcWireEnvelope>::err(MakeError(
                ErrorCode::ProtocolError,
                "response envelope missing outcome oneof"));
        }
    } else {
        // 请求方向不允许携带 outcome。
        if (in.has_success() || in.has_error())
            return result<RpcWireEnvelope>::err(MakeError(
                ErrorCode::ProtocolError,
                "request envelope must not set outcome"));
        env.success = true;
    }

    // 解码后复检：字段级约束（UTF-8 / metadata / budget 上限）在值类型上执行，
    // 不依赖 proto 的字段存在性。
    if (env.request_id.empty() || env.service.empty() || env.method.empty())
        return result<RpcWireEnvelope>::err(MakeError(
            ErrorCode::InvalidArgument,
            "decoded envelope missing required identifier"));
    if (!detail::IsValidUtf8(env.request_id) ||
        !detail::IsValidUtf8(env.service) ||
        !detail::IsValidUtf8(env.method) ||
        !detail::IsValidUtf8(env.request_schema) ||
        !detail::IsValidUtf8(env.response_schema))
        return result<RpcWireEnvelope>::err(MakeError(
            ErrorCode::InvalidArgument,
            "decoded identifier fields not valid UTF-8"));
    if (env.remaining_budget_ms == 0)
        return result<RpcWireEnvelope>::err(MakeError(
            ErrorCode::InvalidArgument,
            "decoded remaining_budget_ms zero is rejected"));
    if (env.remaining_budget_ms > kMaxRemainingBudgetMs)
        return result<RpcWireEnvelope>::err(MakeError(
            ErrorCode::InvalidArgument,
            "decoded remaining_budget_ms exceeds max"));
    if (auto r = ValidateMetadata(env.metadata); !r)
        return result<RpcWireEnvelope>::err(r.error());
    // Error 分支字段级复检（UTF-8/长度/details 上限、重复键），与 encode
    // 路径同一套规则；字段值违规按 InvalidArgument，details 结构违规按
    // ProtocolError（冻结契约），整体拒绝。
    if (!env.success) {
        if (auto r = ValidateErrorBranch(env.error); !r)
            return result<RpcWireEnvelope>::err(r.error());
    }
    return result<RpcWireEnvelope>::ok(std::move(env));
}

// --- HTTP 字段辅助 -----------------------------------------------------------

bool HasHeader(const std::vector<std::pair<std::string, std::string>>& hs,
               const std::string& name, const std::string& expect) {
    for (const auto& [k, v] : hs)
        if (k == name && v == expect)
            return true;
    return false;
}

result<void> CheckHttpRequestShape(const HttpRequest& req) {
    if (req.method != "POST")
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "rpc wire requires POST"));
    // url 允许是绝对 URL 或仅 path；这里只校验 path 后缀为 /rpc，
    // 完整 URL 解析属于 HTTP adapter 边界。
    const std::string& u = req.url;
    const std::string path_suffix = kRpcWireHttpPath;
    bool path_ok = false;
    if (u.size() >= path_suffix.size()) {
        // 提取 path 部分（去掉 scheme://host:port）
        std::string path = u;
        auto scheme = u.find("://");
        if (scheme != std::string::npos) {
            auto slash = u.find('/', scheme + 3);
            path = (slash == std::string::npos) ? "/" : u.substr(slash);
        }
        path_ok = (path == path_suffix);
    }
    if (!path_ok)
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

result<void> CheckHttpResponseShape(const HttpResponse& resp) {
    if (resp.status != 200)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "rpc wire response requires HTTP 200"));
    if (!HasHeader(resp.headers, kRpcWireContentTypeKey, kRpcWireContentType))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "rpc wire response missing Content-Type"));
    if (resp.body.empty())
        return result<void>::err(MakeError(ErrorCode::ProtocolError,
            "rpc wire response empty body"));
    return result<void>::ok();
}

} // namespace

// --- 公开实现 ---------------------------------------------------------------

result<void> ValidateRpcWireEnvelope(const RpcWireEnvelope& env,
                                     bool is_response) {
    pbv1::RpcEnvelopeMsg tmp;
    return FillProto(env, &tmp, is_response);
}

result<std::vector<std::uint8_t>> EncodeRpcWireEnvelope(
    const RpcWireEnvelope& env, bool is_response) {
    pbv1::RpcEnvelopeMsg p;
    if (auto r = FillProto(env, &p, is_response); !r)
        return result<std::vector<std::uint8_t>>::err(r.error());
    std::string s;
    if (!p.SerializeToString(&s))
        return result<std::vector<std::uint8_t>>::err(MakeError(
            ErrorCode::ProtocolError, "protobuf serialize failed"));
    return result<std::vector<std::uint8_t>>::ok(
        std::vector<std::uint8_t>(s.begin(), s.end()));
}

result<RpcWireEnvelope> DecodeRpcWireEnvelope(
    const std::vector<std::uint8_t>& bytes, bool is_response) {
    if (bytes.empty())
        return result<RpcWireEnvelope>::err(MakeError(
            ErrorCode::ProtocolError, "empty envelope bytes"));
    pbv1::RpcEnvelopeMsg p;
    if (!p.ParseFromArray(bytes.data(), static_cast<int>(bytes.size())))
        return result<RpcWireEnvelope>::err(MakeError(
            ErrorCode::ProtocolError, "envelope parse failed"));
    return FromProto(p, is_response);
}

result<RpcWireEnvelope> DecodeRpcWireEnvelope(
    const std::string& bytes, bool is_response) {
    if (bytes.empty())
        return result<RpcWireEnvelope>::err(MakeError(
            ErrorCode::ProtocolError, "empty envelope bytes"));
    pbv1::RpcEnvelopeMsg p;
    if (!p.ParseFromString(bytes))
        return result<RpcWireEnvelope>::err(MakeError(
            ErrorCode::ProtocolError, "envelope parse failed"));
    return FromProto(p, is_response);
}

result<HttpRequest> MakeRpcWireHttpRequest(
    const RpcWireEnvelope& env, const std::string& base_url) {
    auto body = EncodeRpcWireEnvelope(env, /*is_response=*/false);
    if (!body)
        return result<HttpRequest>::err(body.error());
    HttpRequest req;
    req.method = "POST";
    req.url = base_url + kRpcWireHttpPath;
    req.headers.emplace_back(kRpcWireContentTypeKey, kRpcWireContentType);
    req.body.assign(reinterpret_cast<const char*>(body.value().data()),
                    body.value().size());
    return result<HttpRequest>::ok(std::move(req));
}

result<HttpResponse> MakeRpcWireHttpResponse(
    const RpcWireEnvelope& env) {
    auto body = EncodeRpcWireEnvelope(env, /*is_response=*/true);
    if (!body)
        return result<HttpResponse>::err(body.error());
    HttpResponse resp;
    resp.status = 200;
    resp.headers.emplace_back(kRpcWireContentTypeKey, kRpcWireContentType);
    resp.body.assign(reinterpret_cast<const char*>(body.value().data()),
                     body.value().size());
    return result<HttpResponse>::ok(std::move(resp));
}

result<RpcWireEnvelope> ParseRpcWireHttpRequest(const HttpRequest& req) {
    if (auto r = CheckHttpRequestShape(req); !r)
        return result<RpcWireEnvelope>::err(r.error());
    return DecodeRpcWireEnvelope(req.body, /*is_response=*/false);
}

result<RpcWireEnvelope> ParseRpcWireHttpResponse(const HttpResponse& resp) {
    if (auto r = CheckHttpResponseShape(resp); !r)
        return result<RpcWireEnvelope>::err(r.error());
    return DecodeRpcWireEnvelope(resp.body, /*is_response=*/true);
}

result<RpcWireEnvelope> ToWireEnvelope(const RpcEnvelope& env,
                                       std::uint32_t remaining_budget_ms) {
    RpcWireEnvelope w;
    w.profile_version     = kRpcWireProfileVersion;
    w.request_id          = env.request_id;
    w.service             = env.service;
    w.method              = env.method;
    w.remaining_budget_ms = remaining_budget_ms;
    w.request_schema      = env.request_schema;
    w.response_schema     = env.response_schema;
    w.payload             = env.payload;
    w.metadata            = env.metadata;
    w.success             = true;
    if (auto r = ValidateRpcWireEnvelope(w, /*is_response=*/false); !r)
        return result<RpcWireEnvelope>::err(r.error());
    return result<RpcWireEnvelope>::ok(std::move(w));
}

result<RpcEnvelope> FromWireEnvelope(const RpcWireEnvelope& env) {
    RpcEnvelope out;
    out.service         = env.service;
    out.method          = env.method;
    out.request_id      = env.request_id;
    out.request_schema  = env.request_schema;
    out.response_schema = env.response_schema;
    out.payload         = env.payload;
    out.metadata        = env.metadata;
    return result<RpcEnvelope>::ok(std::move(out));
}

} // namespace bbt::infra::rpc
