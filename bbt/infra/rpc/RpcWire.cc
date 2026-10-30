// bbtools-infra：RpcEnvelope wire profile 的 protobuf adapter 实现。
// 公开面见 bbt/infra/rpc/RpcWire.hpp；protobuf 生成物只在本文件
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
// 内部实现显式引用生成类型，不把 wire 版本名透传到公开面。
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

// --- 出站请求阶段映射（docs/decisions/0008-request-phase-outcome-unknown.md 的 Error::request_phase 事实）------------
// wire 把 UNSPECIFIED 保留为 0（非法值 → ProtocolError），本地合法阶段从
// 1 起排——与 Result.hpp 的 RequestPhase 声明序一致，序号不经枚举值直接互转。

// 返回可检查结果：非法本地阶段值不得静默编码为 present-UNSPECIFIED——
// 接收端对 UNSPECIFIED 判 ProtocolError，发送端必须在边界先拒。
bool ToProtoRequestPhase(RequestPhase p, pbv1::RpcRequestPhase* out) {
    switch (p) {
        case RequestPhase::NotStarted:
            *out = pbv1::RPC_REQUEST_PHASE_NOT_STARTED;       return true;
        case RequestPhase::Connecting:
            *out = pbv1::RPC_REQUEST_PHASE_CONNECTING;        return true;
        case RequestPhase::Writing:
            *out = pbv1::RPC_REQUEST_PHASE_WRITING;           return true;
        case RequestPhase::RequestCommitted:
            *out = pbv1::RPC_REQUEST_PHASE_REQUEST_COMMITTED; return true;
        case RequestPhase::ReplyTerminal:
            *out = pbv1::RPC_REQUEST_PHASE_REPLY_TERMINAL;    return true;
    }
    return false;
}

// 返回值：nullopt 表示字段缺席（合法，解码为 nullopt）；带值的 optional
// 表示已识别阶段；UNSPECIFIED/未识别值经失败分支返回 ProtocolError。
result<std::optional<RequestPhase>> FromProtoRequestPhase(
    const pbv1::RpcError& pe) {
    if (!pe.has_request_phase())
        return result<std::optional<RequestPhase>>::ok(std::nullopt);
    switch (pe.request_phase()) {
        case pbv1::RPC_REQUEST_PHASE_NOT_STARTED:
            return result<std::optional<RequestPhase>>::ok(RequestPhase::NotStarted);
        case pbv1::RPC_REQUEST_PHASE_CONNECTING:
            return result<std::optional<RequestPhase>>::ok(RequestPhase::Connecting);
        case pbv1::RPC_REQUEST_PHASE_WRITING:
            return result<std::optional<RequestPhase>>::ok(RequestPhase::Writing);
        case pbv1::RPC_REQUEST_PHASE_REQUEST_COMMITTED:
            return result<std::optional<RequestPhase>>::ok(RequestPhase::RequestCommitted);
        case pbv1::RPC_REQUEST_PHASE_REPLY_TERMINAL:
            return result<std::optional<RequestPhase>>::ok(RequestPhase::ReplyTerminal);
        case pbv1::RPC_REQUEST_PHASE_UNSPECIFIED:
        default:
            break;
    }
    // 阶段是单调机器事实，未知值不得静默吞掉：与 error code 同规则，拒绝。
    return result<std::optional<RequestPhase>>::err(MakeError(
        ErrorCode::ProtocolError, "unrecognized request_phase value"));
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
    // present 的 request_phase 必须是本地合法阶段：非法值此前被静默编码成
    // present-UNSPECIFIED，接收端必拒（ProtocolError）——来源侧在边界先拒。
    if (err.request_phase.has_value()) {
        pbv1::RpcRequestPhase ignored;
        if (!ToProtoRequestPhase(*err.request_phase, &ignored))
            return result<void>::err(MakeError(ErrorCode::ProtocolError,
                "request_phase value is not a valid local phase"));
    }
    // ValidateErrorDetails 用 ProtocolError 表达结构违规（docs/decisions/0002-co-network-contract.md 与
    // Result.hpp:157-177 的冻结语义）：重复键、非法 UTF-8、超限 details
    // 必须原样保留 ProtocolError，不得改写为 InvalidArgument。
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
    // wire 契约（docs/decisions/0009-rpc-runtime.md）：remaining_budget_ms 为 0 明确拒绝（不取本地预算兜底）；
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
    // 违反 wire 边界。编码前显式拒绝。
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
            // request_phase 只在携带时写：nullopt 不落字段，wire 上保持缺席，
            // 旧 peer 按未知字段忽略、新 peer 解回 nullopt（不补 NotStarted）。
            // 合法性已由 ValidateErrorBranch 拒绝非法本地值；此处断言式复验，
            // 任何绕过校验的新调用路径也不得产出 present-UNSPECIFIED。
            if (env.error.request_phase.has_value()) {
                pbv1::RpcRequestPhase phase;
                if (!ToProtoRequestPhase(*env.error.request_phase, &phase))
                    return result<void>::err(MakeError(ErrorCode::ProtocolError,
                        "request_phase value is not a valid local phase"));
                pe->set_request_phase(phase);
            }
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
            auto phase = FromProtoRequestPhase(pe);
            if (!phase)
                return result<RpcWireEnvelope>::err(phase.error());
            env.error.request_phase = phase.value();
            env.error.details.reserve(pe.details_size());
            for (const auto& de : pe.details())
                env.error.details.emplace_back(de.key(), de.value());
        } else if (in.has_success()) {
            if (!in.success())
                return result<RpcWireEnvelope>::err(MakeError(
                    ErrorCode::ProtocolError,
                    "response success=false without error"));
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

// --- HTTP 字段辅助（bbt::infra::http 值类型） ---------------------------------

bool HasHeader(const std::vector<std::pair<std::string, std::string>>& hs,
               const std::string& name, const std::string& expect) {
    const auto equal_ascii_ci = [](const std::string& lhs,
                                   const std::string& rhs) {
        if (lhs.size() != rhs.size())
            return false;
        for (std::size_t i = 0; i < lhs.size(); ++i) {
            const auto lower = [](unsigned char c) {
                return static_cast<char>(c >= 'A' && c <= 'Z'
                                             ? c + ('a' - 'A')
                                             : c);
            };
            if (lower(static_cast<unsigned char>(lhs[i])) !=
                lower(static_cast<unsigned char>(rhs[i])))
                return false;
        }
        return true;
    };
    for (const auto& [k, v] : hs)
        if (equal_ascii_ci(k, name) && v == expect)
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
