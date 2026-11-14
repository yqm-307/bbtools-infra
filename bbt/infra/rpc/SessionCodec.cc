/**
 * bbtools-infra v2：SessionFrame wire codec 与 u32be 分帧实现。
 * 公开面见 bbt/infra/rpc/SessionCodec.hpp；protobuf 生成物只在本文件与 protoc
 * 生成目录内出现，不向公共头泄漏。
 *
 * 校验矩阵来源：docs/decisions/0014-app-session.md §5.1 字段约束表与
 * 分帧上限。错误映射：编码端 InvalidArgument
 * （值不可编码）/ProtocolError（本地非法枚举会产出接收端必拒 wire）；
 * 解码端字段级违规统一 ProtocolError，不产生部分解码。
 */
#include <bbt/infra/rpc/SessionCodec.hpp>

#include <algorithm>
#include <cstring>
#include <string>
#include <utility>

#include "bbt/infra/rpc/v2/session_frame.pb.h"

namespace bbt::infra::rpc {
namespace {

/**
 * proto 生成物保留 wire 版本 namespace（bbt::infra::rpc::v2）；公开 API 收敛
 * 在 bbt::infra::rpc，wire 版本名不透传到公开面。
 */
namespace pbv2 = ::bbt::infra::rpc::v2;

/**
 * @name 枚举映射（不裸转数值：未知 wire 值解码即拒，本地非法值编码即拒） @{
 */
result<pbv2::FrameKind> ToProtoFrameKind(FrameKind kind)
{
    switch (kind) {
    case FrameKind::Hello:
        return result<pbv2::FrameKind>::ok(pbv2::HELLO);
    case FrameKind::HelloAck:
        return result<pbv2::FrameKind>::ok(pbv2::HELLO_ACK);
    case FrameKind::Call:
        return result<pbv2::FrameKind>::ok(pbv2::CALL);
    case FrameKind::Response:
        return result<pbv2::FrameKind>::ok(pbv2::RESPONSE);
    case FrameKind::Send:
        return result<pbv2::FrameKind>::ok(pbv2::SEND);
    case FrameKind::SendAccepted:
        return result<pbv2::FrameKind>::ok(pbv2::SEND_ACCEPTED);
    case FrameKind::SessionError:
        return result<pbv2::FrameKind>::ok(pbv2::SESSION_ERROR);
    case FrameKind::Close:
        return result<pbv2::FrameKind>::ok(pbv2::CLOSE);
    case FrameKind::Unspecified:
        break;
    }
    return result<pbv2::FrameKind>::err(MakeError(ErrorCode::ProtocolError, "frame_kind is not a valid local kind"));
}

result<FrameKind> FromProtoFrameKind(pbv2::FrameKind kind)
{
    switch (kind) {
    case pbv2::HELLO:
        return result<FrameKind>::ok(FrameKind::Hello);
    case pbv2::HELLO_ACK:
        return result<FrameKind>::ok(FrameKind::HelloAck);
    case pbv2::CALL:
        return result<FrameKind>::ok(FrameKind::Call);
    case pbv2::RESPONSE:
        return result<FrameKind>::ok(FrameKind::Response);
    case pbv2::SEND:
        return result<FrameKind>::ok(FrameKind::Send);
    case pbv2::SEND_ACCEPTED:
        return result<FrameKind>::ok(FrameKind::SendAccepted);
    case pbv2::SESSION_ERROR:
        return result<FrameKind>::ok(FrameKind::SessionError);
    case pbv2::CLOSE:
        return result<FrameKind>::ok(FrameKind::Close);
    case pbv2::FRAME_KIND_UNSPECIFIED:
        break;
    default:
        break;
    }
    return result<FrameKind>::err(MakeError(ErrorCode::ProtocolError, "unrecognized frame_kind value"));
}

result<pbv2::TrafficClass> ToProtoTraffic(TrafficClass traffic)
{
    switch (traffic) {
    case TrafficClass::System:
        return result<pbv2::TrafficClass>::ok(pbv2::TRAFFIC_SYSTEM);
    case TrafficClass::Service:
        return result<pbv2::TrafficClass>::ok(pbv2::TRAFFIC_SERVICE);
    case TrafficClass::Actor:
        return result<pbv2::TrafficClass>::ok(pbv2::TRAFFIC_ACTOR);
    }
    return result<pbv2::TrafficClass>::err(
        MakeError(ErrorCode::ProtocolError, "traffic_class is not a valid local class"));
}

result<TrafficClass> FromProtoTraffic(pbv2::TrafficClass traffic)
{
    switch (traffic) {
    case pbv2::TRAFFIC_SYSTEM:
        return result<TrafficClass>::ok(TrafficClass::System);
    case pbv2::TRAFFIC_SERVICE:
        return result<TrafficClass>::ok(TrafficClass::Service);
    case pbv2::TRAFFIC_ACTOR:
        return result<TrafficClass>::ok(TrafficClass::Actor);
    default:
        break;
    }
    return result<TrafficClass>::err(MakeError(ErrorCode::ProtocolError, "unrecognized traffic_class value"));
}

result<pbv2::ReplyMode> ToProtoReply(ReplyMode reply)
{
    switch (reply) {
    case ReplyMode::None:
        return result<pbv2::ReplyMode>::ok(pbv2::REPLY_NONE);
    case ReplyMode::Required:
        return result<pbv2::ReplyMode>::ok(pbv2::REPLY_REQUIRED);
    }
    return result<pbv2::ReplyMode>::err(MakeError(ErrorCode::ProtocolError, "reply_mode is not a valid local mode"));
}

result<ReplyMode> FromProtoReply(pbv2::ReplyMode reply)
{
    switch (reply) {
    case pbv2::REPLY_NONE:
        return result<ReplyMode>::ok(ReplyMode::None);
    case pbv2::REPLY_REQUIRED:
        return result<ReplyMode>::ok(ReplyMode::Required);
    default:
        break;
    }
    return result<ReplyMode>::err(MakeError(ErrorCode::ProtocolError, "unrecognized reply_mode value"));
}

/**
 * @}
 * @name Error 分支映射 @{
 */
pbv2::RpcErrorCode ToProtoErrorCode(ErrorCode c)
{
    switch (c) {
    case ErrorCode::InvalidArgument:
        return pbv2::RPC_ERROR_CODE_INVALID_ARGUMENT;
    case ErrorCode::InvalidContext:
        return pbv2::RPC_ERROR_CODE_INVALID_CONTEXT;
    case ErrorCode::RuntimeUnavailable:
        return pbv2::RPC_ERROR_CODE_RUNTIME_UNAVAILABLE;
    case ErrorCode::Closed:
        return pbv2::RPC_ERROR_CODE_CLOSED;
    case ErrorCode::Cancelled:
        return pbv2::RPC_ERROR_CODE_CANCELLED;
    case ErrorCode::TimedOut:
        return pbv2::RPC_ERROR_CODE_TIMED_OUT;
    case ErrorCode::Overloaded:
        return pbv2::RPC_ERROR_CODE_OVERLOADED;
    case ErrorCode::Unavailable:
        return pbv2::RPC_ERROR_CODE_UNAVAILABLE;
    case ErrorCode::TransportError:
        return pbv2::RPC_ERROR_CODE_TRANSPORT_ERROR;
    case ErrorCode::ProtocolError:
        return pbv2::RPC_ERROR_CODE_PROTOCOL_ERROR;
    case ErrorCode::NotFound:
        return pbv2::RPC_ERROR_CODE_NOT_FOUND;
    case ErrorCode::TypeMismatch:
        return pbv2::RPC_ERROR_CODE_TYPE_MISMATCH;
    case ErrorCode::UnsupportedRoute:
        return pbv2::RPC_ERROR_CODE_UNSUPPORTED_ROUTE;
    case ErrorCode::OutcomeUnknown:
        return pbv2::RPC_ERROR_CODE_OUTCOME_UNKNOWN;
    case ErrorCode::RemoteError:
        return pbv2::RPC_ERROR_CODE_REMOTE_ERROR;
    case ErrorCode::InternalError:
        return pbv2::RPC_ERROR_CODE_INTERNAL_ERROR;
    }
    return pbv2::RPC_ERROR_CODE_INTERNAL_ERROR;
}

ErrorCode FromProtoErrorCode(pbv2::RpcErrorCode c)
{
    switch (c) {
    case pbv2::RPC_ERROR_CODE_INVALID_ARGUMENT:
        return ErrorCode::InvalidArgument;
    case pbv2::RPC_ERROR_CODE_INVALID_CONTEXT:
        return ErrorCode::InvalidContext;
    case pbv2::RPC_ERROR_CODE_RUNTIME_UNAVAILABLE:
        return ErrorCode::RuntimeUnavailable;
    case pbv2::RPC_ERROR_CODE_CLOSED:
        return ErrorCode::Closed;
    case pbv2::RPC_ERROR_CODE_CANCELLED:
        return ErrorCode::Cancelled;
    case pbv2::RPC_ERROR_CODE_TIMED_OUT:
        return ErrorCode::TimedOut;
    case pbv2::RPC_ERROR_CODE_OVERLOADED:
        return ErrorCode::Overloaded;
    case pbv2::RPC_ERROR_CODE_UNAVAILABLE:
        return ErrorCode::Unavailable;
    case pbv2::RPC_ERROR_CODE_TRANSPORT_ERROR:
        return ErrorCode::TransportError;
    case pbv2::RPC_ERROR_CODE_PROTOCOL_ERROR:
        return ErrorCode::ProtocolError;
    case pbv2::RPC_ERROR_CODE_NOT_FOUND:
        return ErrorCode::NotFound;
    case pbv2::RPC_ERROR_CODE_TYPE_MISMATCH:
        return ErrorCode::TypeMismatch;
    case pbv2::RPC_ERROR_CODE_UNSUPPORTED_ROUTE:
        return ErrorCode::UnsupportedRoute;
    case pbv2::RPC_ERROR_CODE_OUTCOME_UNKNOWN:
        return ErrorCode::OutcomeUnknown;
    case pbv2::RPC_ERROR_CODE_REMOTE_ERROR:
        return ErrorCode::RemoteError;
    case pbv2::RPC_ERROR_CODE_INTERNAL_ERROR:
        return ErrorCode::InternalError;
    case pbv2::RPC_ERROR_CODE_UNSPECIFIED:
        break;
    default:
        break;
    }

    /**
     * 未知/未指定 code：不静默吞掉
     */
    return ErrorCode::ProtocolError;
}

bool ToProtoRequestPhase(RequestPhase p, pbv2::RpcRequestPhase* out)
{
    switch (p) {
    case RequestPhase::NotStarted:
        *out = pbv2::RPC_REQUEST_PHASE_NOT_STARTED;
        return true;
    case RequestPhase::Connecting:
        *out = pbv2::RPC_REQUEST_PHASE_CONNECTING;
        return true;
    case RequestPhase::Writing:
        *out = pbv2::RPC_REQUEST_PHASE_WRITING;
        return true;
    case RequestPhase::RequestCommitted:
        *out = pbv2::RPC_REQUEST_PHASE_REQUEST_COMMITTED;
        return true;
    case RequestPhase::ReplyTerminal:
        *out = pbv2::RPC_REQUEST_PHASE_REPLY_TERMINAL;
        return true;
    }
    return false;
}

result<std::optional<RequestPhase>> FromProtoRequestPhase(const pbv2::RpcError& pe)
{
    if (!pe.has_request_phase())
        return result<std::optional<RequestPhase>>::ok(std::nullopt);
    switch (pe.request_phase()) {
    case pbv2::RPC_REQUEST_PHASE_NOT_STARTED:
        return result<std::optional<RequestPhase>>::ok(RequestPhase::NotStarted);
    case pbv2::RPC_REQUEST_PHASE_CONNECTING:
        return result<std::optional<RequestPhase>>::ok(RequestPhase::Connecting);
    case pbv2::RPC_REQUEST_PHASE_WRITING:
        return result<std::optional<RequestPhase>>::ok(RequestPhase::Writing);
    case pbv2::RPC_REQUEST_PHASE_REQUEST_COMMITTED:
        return result<std::optional<RequestPhase>>::ok(RequestPhase::RequestCommitted);
    case pbv2::RPC_REQUEST_PHASE_REPLY_TERMINAL:
        return result<std::optional<RequestPhase>>::ok(RequestPhase::ReplyTerminal);
    case pbv2::RPC_REQUEST_PHASE_UNSPECIFIED:
    default:
        break;
    }
    return result<std::optional<RequestPhase>>::err(
        MakeError(ErrorCode::ProtocolError, "unrecognized request_phase value"));
}

/**
 * @}
 * @name 共享字段校验（metadata / error 分支） @{
 */
bool HasMetaPrefix(const std::string& key)
{
    return key.rfind("fw.", 0) == 0 || key.rfind("route.", 0) == 0 || key.rfind("trace.", 0) == 0;
}

result<void> ValidateMetadata(const Metadata& md)
{
    if (md.size() > 32)
        return result<void>::err(MakeError(ErrorCode::ProtocolError, "metadata exceeds max item count"));
    for (std::size_t i = 0; i < md.size(); ++i) {
        const auto& [k, v] = md[i];
        if (k.empty() || k.size() > kMaxKeyBytes)
            return result<void>::err(MakeError(ErrorCode::ProtocolError, "metadata key empty or exceeds max bytes"));
        if (v.size() > kMaxValueBytes)
            return result<void>::err(MakeError(ErrorCode::ProtocolError, "metadata value exceeds max bytes"));
        if (!HasMetaPrefix(k))
            return result<void>::err(MakeError(ErrorCode::ProtocolError, "metadata key uses unregistered prefix"));
        if (!infra::detail::IsValidUtf8(k) || !infra::detail::IsValidUtf8(v))
            return result<void>::err(MakeError(ErrorCode::ProtocolError, "metadata not valid UTF-8"));
        for (std::size_t j = 0; j < i; ++j)
            if (md[j].first == k)
                return result<void>::err(MakeError(ErrorCode::ProtocolError, "metadata duplicate key"));
    }
    return result<void>::ok();
}

/**
 * Error 分支字段级校验：UTF-8/长度/details 结构。details 结构违规保持
 * ProtocolError，字符串字段违规 InvalidArgument。
 * phase_valid 只用于编码侧（本地非法阶段先拒，不产出 present-UNSPECIFIED）。
 */
result<void> ValidateErrorBranch(const Error& err, bool check_local_phase)
{
    if (!infra::detail::IsValidUtf8(err.domain) || !infra::detail::IsValidUtf8(err.domain_code)
        || !infra::detail::IsValidUtf8(err.message) || !infra::detail::IsValidUtf8(err.backend_category))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "error string fields not valid UTF-8"));
    if (err.domain.size() > kMaxKeyBytes || err.domain_code.size() > kMaxKeyBytes
        || err.backend_category.size() > kMaxKeyBytes)
        return result<void>::err(
            MakeError(ErrorCode::InvalidArgument, "error domain/domain_code/backend_category exceeds max bytes"));
    if (err.message.size() > kMaxValueBytes)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "error message exceeds max bytes"));
    if (check_local_phase && err.request_phase.has_value()) {
        pbv2::RpcRequestPhase ignored;
        if (!ToProtoRequestPhase(*err.request_phase, &ignored))
            return result<void>::err(
                MakeError(ErrorCode::ProtocolError, "request_phase value is not a valid local phase"));
    }
    if (auto r = ValidateErrorDetails(err); !r)
        return r;
    return result<void>::ok();
}

/**
 * UTF-8 + 长度检查助手：标识/字符串字段通用（InvalidArgument on encode）。
 */
result<void> CheckUtf8Len(const std::string& field, const char* name, std::size_t max, bool required)
{
    if (field.empty()) {
        if (required)
            return result<void>::err(MakeError(ErrorCode::InvalidArgument, std::string(name) + " must be non-empty"));
        return result<void>::ok();
    }
    if (field.size() > max)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, std::string(name) + " exceeds max bytes"));
    if (!infra::detail::IsValidUtf8(field))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, std::string(name) + " not valid UTF-8"));
    return result<void>::ok();
}

/**
 * @}
 * @name kind 相关字段存在性矩阵（ADR §5.1 表） @{
 */

/**
 * 编码路径（InvalidArgument）；解码路径把同一矩阵映射为 ProtocolError。
 */
result<void> ValidateKindRules(const SessionFrame& f, bool decoding)
{
    const auto fail = [&](const char* what) {
        return result<void>::err(MakeError(decoding ? ErrorCode::ProtocolError : ErrorCode::InvalidArgument, what));
    };

    /**
     * 枚举本地合法性先于字段矩阵：本地值超出 wire 枚举域会产出接收端必拒的
     * wire（present-UNSPECIFIED/未识别值），按
     * ProtocolError 拒绝，不落到字段存在性分支的 InvalidArgument。
     */
    switch (f.kind) {
    case FrameKind::Hello:
    case FrameKind::HelloAck:
    case FrameKind::Call:
    case FrameKind::Response:
    case FrameKind::Send:
    case FrameKind::SendAccepted:
    case FrameKind::SessionError:
    case FrameKind::Close:
        break;
    case FrameKind::Unspecified:
    default:
        return result<void>::err(MakeError(ErrorCode::ProtocolError, "frame_kind is not a valid local kind"));
    }
    switch (f.traffic) {
    case TrafficClass::System:
    case TrafficClass::Service:
    case TrafficClass::Actor:
        break;
    default:
        return result<void>::err(MakeError(ErrorCode::ProtocolError, "traffic_class is not a valid local class"));
    }
    switch (f.reply) {
    case ReplyMode::None:
    case ReplyMode::Required:
        break;
    default:
        return result<void>::err(MakeError(ErrorCode::ProtocolError, "reply_mode is not a valid local mode"));
    }
    const bool handshake = f.kind == FrameKind::Hello || f.kind == FrameKind::HelloAck;

    /**
     * 数据帧（Call/Response/Send）使用 Service/Actor；确认/错误/关闭属控制面
     * （ADR §5.1：Hello/HelloAck/错误/关闭/确认使用 System）。
     */
    const bool data = f.kind == FrameKind::Call || f.kind == FrameKind::Send || f.kind == FrameKind::Response;
    const bool outcome_kind =
        f.kind == FrameKind::Response || f.kind == FrameKind::SendAccepted || f.kind == FrameKind::SessionError;

    /**
     * traffic_class：数据帧 Service/Actor；控制帧 System。
     */
    if (data && f.traffic == TrafficClass::System)
        return fail("data frame must not use TRAFFIC_SYSTEM");
    if (!data && f.traffic != TrafficClass::System)
        return fail("control frame must use TRAFFIC_SYSTEM");

    /**
     * peer_identity：仅握手帧，非空、UTF-8、≤256。
     */
    if (handshake) {
        if (f.peer_identity.empty() || f.peer_identity.size() > kRpcV2MaxPeerIdentityBytes
            || !infra::detail::IsValidUtf8(f.peer_identity))
            return fail("peer_identity required ≤256 UTF-8 on handshake frame");
    } else if (!f.peer_identity.empty()) {
        return fail("peer_identity must be absent on non-handshake frame");
    }

    /**
     * connection_nonce：仅握手帧，非零。
     */
    if (handshake) {
        if (f.connection_nonce == 0)
            return fail("connection_nonce must be non-zero on handshake frame");
    } else if (f.connection_nonce != 0) {
        return fail("connection_nonce must be absent on non-handshake frame");
    }

    /**
     * session_epoch：握手帧、数据帧与已建立会话的确认帧非零；SESSION_ERROR/
     * CLOSE 允许 0（可能早于 epoch 协商完成），非零同样合法。
     */
    if ((handshake || data || f.kind == FrameKind::SendAccepted) && f.session_epoch == 0)
        return fail("session_epoch must be non-zero");

    /**
     * message_id：Send/SendAccepted 必填 ≤128；其他 kind 必须缺席。
     */
    if (f.kind == FrameKind::Send || f.kind == FrameKind::SendAccepted) {
        if (f.message_id.empty() || f.message_id.size() > kRpcV2MaxMessageIdBytes
            || !infra::detail::IsValidUtf8(f.message_id))
            return fail("message_id required ≤128 UTF-8 on Send/SendAccepted");
    } else if (!f.message_id.empty()) {
        return fail("message_id must be absent on non-Send frame");
    }

    /**
     * request_id：Call/Response 非空；其他 kind 允许为空（Send 无回复可为空）。
     */
    if ((f.kind == FrameKind::Call || f.kind == FrameKind::Response) && f.request_id.empty())
        return fail("request_id required on Call/Response");
    if (!f.request_id.empty()
        && (f.request_id.size() > kRpcV2MaxFieldBytes || !infra::detail::IsValidUtf8(f.request_id)))
        return fail("request_id exceeds max bytes / not UTF-8");

    /**
     * service/method：Call/Response 必填；Send 只做值和长度检查。
     */
    const bool require_route = f.kind == FrameKind::Call || f.kind == FrameKind::Response;
    auto svc = CheckUtf8Len(f.service, "service", kRpcV2MaxFieldBytes, require_route);
    if (!svc) {
        if (decoding)
            return fail("service invalid on wire");
        return svc;
    }
    auto mth = CheckUtf8Len(f.method, "method", kRpcV2MaxFieldBytes, require_route);
    if (!mth) {
        if (decoding)
            return fail("method invalid on wire");
        return mth;
    }

    /**
     * reply_mode：Call/Response 固定 Required；其他 kind 固定 None。
     */
    if ((f.kind == FrameKind::Call || f.kind == FrameKind::Response) && f.reply != ReplyMode::Required)
        return fail("reply_mode must be Required on Call/Response");
    if (f.kind != FrameKind::Call && f.kind != FrameKind::Response && f.reply != ReplyMode::None)
        return fail("reply_mode must be None on non-Call/Response frame");

    /**
     * sequence/ordering_key：seq==0 未启用；启用时 key 非空 ≤256。
     */
    if (f.sequence != 0) {
        if (f.ordering_key.empty() || f.ordering_key.size() > kRpcV2MaxOrderingKeyBytes)
            return fail("ordering_key required ≤256 when sequence enabled");
    } else if (!f.ordering_key.empty()) {
        return fail("ordering_key requires non-zero sequence");
    }

    /**
     * remaining_budget_ms：Call/Send 必填 1..max；Response/SendAccepted 允许
     * 0（响应不强制带剩余预算）但非零仍须 ≤max；控制帧必须为 0。
     */
    if (f.kind == FrameKind::Call || f.kind == FrameKind::Send) {
        if (f.remaining_budget_ms == 0 || f.remaining_budget_ms > kRpcV2MaxRemainingBudgetMs)
            return fail("remaining_budget_ms must be in 1..max on Call/Send");
    } else if (f.kind == FrameKind::Response || f.kind == FrameKind::SendAccepted) {
        if (f.remaining_budget_ms > kRpcV2MaxRemainingBudgetMs)
            return fail("remaining_budget_ms exceeds max");
    } else if (f.remaining_budget_ms != 0) {
        return fail("remaining_budget_ms must be absent on control frame");
    }

    /**
     * request_schema/response_schema：Call/Response 必填 UTF-8 ≤max。
     */
    auto rs = CheckUtf8Len(f.request_schema, "request_schema", kRpcV2MaxFieldBytes, require_route);
    if (!rs) {
        if (decoding)
            return fail("request_schema invalid on wire");
        return rs;
    }
    auto xs = CheckUtf8Len(f.response_schema, "response_schema", kRpcV2MaxFieldBytes, require_route);
    if (!xs) {
        if (decoding)
            return fail("response_schema invalid on wire");
        return xs;
    }

    /**
     * acceptance_requested presence：仅 Send；其他 kind 必须缺席。
     */
    if (f.acceptance_requested.has_value() && f.kind != FrameKind::Send)
        return fail("acceptance_requested allowed only on Send");

    /**
     * outcome：请求帧（Hello/HelloAck/Call/Send/Close）不携带；
     * Response/SendAccepted/SessionError 必须二选一。
     */
    if (f.has_outcome != outcome_kind)
        return fail(outcome_kind ? "outcome required on this frame kind" : "outcome must be absent on this frame kind");
    if (f.has_outcome && !f.success) {

        /**
         * check_local_phase = !decoding。
         */
        if (auto r = ValidateErrorBranch(f.error, !decoding); !r) {
            if (decoding && r.error().code == ErrorCode::InvalidArgument)
                return fail("error branch invalid on wire");
            return r;
        }
    }

    /**
     * metadata：出现的任何地方都按同一受限规则校验。
     */
    if (auto r = ValidateMetadata(f.metadata); !r) {
        if (decoding)
            return fail("metadata invalid on wire");
        return r;
    }
    return result<void>::ok();
}

/**
 * @}
 * @name proto ⇄ 值类型 @{
 */
result<void> FillProto(const SessionFrame& f, pbv2::SessionFrame* out)
{

    /**
     * decoding = false。
     */
    if (auto r = ValidateKindRules(f, false); !r)
        return r;
    auto kind = ToProtoFrameKind(f.kind);
    if (!kind)
        return result<void>::err(kind.error());
    auto traffic = ToProtoTraffic(f.traffic);
    if (!traffic)
        return result<void>::err(traffic.error());
    auto reply = ToProtoReply(f.reply);
    if (!reply)
        return result<void>::err(reply.error());

    out->set_profile_version(kRpcV2ProfileVersion);
    out->set_frame_kind(kind.value());
    out->set_traffic_class(traffic.value());
    if (!f.peer_identity.empty())
        out->set_peer_identity(f.peer_identity);
    out->set_session_epoch(f.session_epoch);
    if (!f.message_id.empty())
        out->set_message_id(f.message_id);
    if (!f.request_id.empty())
        out->set_request_id(f.request_id);
    if (!f.service.empty())
        out->set_service(f.service);
    if (!f.method.empty())
        out->set_method(f.method);
    out->set_reply_mode(reply.value());
    out->set_sequence(f.sequence);
    if (!f.ordering_key.empty())
        out->set_ordering_key(f.ordering_key);
    out->set_remaining_budget_ms(f.remaining_budget_ms);
    if (!f.request_schema.empty())
        out->set_request_schema(f.request_schema);
    if (!f.response_schema.empty())
        out->set_response_schema(f.response_schema);
    if (!f.payload.empty())
        out->set_payload(f.payload.data(), f.payload.size());
    for (const auto& [k, v] : f.metadata) {
        auto* e = out->add_metadata();
        e->set_key(k);
        e->set_value(v);
    }
    if (f.has_outcome) {
        if (f.success) {
            out->set_success(true);
        } else {
            auto* pe = out->mutable_error();
            pe->set_code(ToProtoErrorCode(f.error.code));
            pe->set_domain(f.error.domain);
            pe->set_domain_code(f.error.domain_code);
            pe->set_message(f.error.message);
            pe->set_backend_category(f.error.backend_category);
            pe->set_backend_code(f.error.backend_code);
            pe->set_transferred_bytes(f.error.transferred_bytes);
            if (f.error.request_phase.has_value()) {
                pbv2::RpcRequestPhase phase;
                if (!ToProtoRequestPhase(*f.error.request_phase, &phase))
                    return result<void>::err(
                        MakeError(ErrorCode::ProtocolError, "request_phase value is not a valid local phase"));
                pe->set_request_phase(phase);
            }
            for (const auto& [k, v] : f.error.details) {
                auto* de = pe->add_details();
                de->set_key(k);
                de->set_value(v);
            }
        }
    }
    if (f.connection_nonce != 0)
        out->set_connection_nonce(f.connection_nonce);
    if (f.acceptance_requested.has_value())
        out->set_acceptance_requested(*f.acceptance_requested);
    return result<void>::ok();
}

result<SessionFrame> FromProto(const pbv2::SessionFrame& in)
{
    if (in.profile_version() != kRpcV2ProfileVersion)
        return result<SessionFrame>::err(MakeError(ErrorCode::ProtocolError, "unsupported profile_version"));

    SessionFrame f;
    auto kind = FromProtoFrameKind(in.frame_kind());
    if (!kind)
        return result<SessionFrame>::err(kind.error());
    f.kind = kind.value();
    auto traffic = FromProtoTraffic(in.traffic_class());
    if (!traffic)
        return result<SessionFrame>::err(traffic.error());
    f.traffic = traffic.value();
    auto reply = FromProtoReply(in.reply_mode());
    if (!reply)
        return result<SessionFrame>::err(reply.error());
    f.reply = reply.value();

    f.peer_identity = in.peer_identity();
    f.session_epoch = in.session_epoch();
    f.message_id = in.message_id();
    f.request_id = in.request_id();
    f.service = in.service();
    f.method = in.method();
    f.sequence = in.sequence();
    f.ordering_key = in.ordering_key();
    f.remaining_budget_ms = in.remaining_budget_ms();
    f.request_schema = in.request_schema();
    f.response_schema = in.response_schema();
    const auto& p = in.payload();
    f.payload.assign(p.begin(), p.end());
    f.metadata.reserve(in.metadata_size());
    for (const auto& e : in.metadata())
        f.metadata.emplace_back(e.key(), e.value());

    /**
     * oneof 缺席时保留 has_outcome=false，由 kind 矩阵判定合法性。
     */
    if (in.has_error()) {
        f.has_outcome = true;
        f.success = false;
        const auto& pe = in.error();
        f.error.code = FromProtoErrorCode(pe.code());
        f.error.domain = pe.domain();
        f.error.domain_code = pe.domain_code();
        f.error.message = pe.message();
        f.error.backend_category = pe.backend_category();
        f.error.backend_code = pe.backend_code();
        f.error.transferred_bytes = pe.transferred_bytes();
        auto phase = FromProtoRequestPhase(pe);
        if (!phase)
            return result<SessionFrame>::err(phase.error());
        f.error.request_phase = phase.value();
        f.error.details.reserve(pe.details_size());
        for (const auto& de : pe.details())
            f.error.details.emplace_back(de.key(), de.value());
    } else if (in.has_success()) {
        if (!in.success())
            return result<SessionFrame>::err(MakeError(ErrorCode::ProtocolError, "success=false without error branch"));
        f.has_outcome = true;
        f.success = true;
    }

    f.connection_nonce = in.connection_nonce();
    if (in.has_acceptance_requested())
        f.acceptance_requested = in.acceptance_requested();

    /**
     * 解码后复检：kind 字段存在性矩阵 + UTF-8/metadata/长度上限。
     *
     * decoding = true。
     */
    if (auto r = ValidateKindRules(f, true); !r)
        return result<SessionFrame>::err(r.error());
    return result<SessionFrame>::ok(std::move(f));
}

// clang-format off
}
// clang-format on

/**
 * @}
 * @name 公开实现 @{
 */
result<void> ValidateSessionFrame(const SessionFrame& frame)
{

    /**
     * decoding = false。
     */
    return ValidateKindRules(frame, false);
}

result<std::vector<std::uint8_t>> EncodeSessionFrame(const SessionFrame& frame)
{
    pbv2::SessionFrame p;
    if (auto r = FillProto(frame, &p); !r)
        return result<std::vector<std::uint8_t>>::err(r.error());
    std::string s;
    if (!p.SerializeToString(&s))
        return result<std::vector<std::uint8_t>>::err(MakeError(ErrorCode::ProtocolError, "protobuf serialize failed"));
    return result<std::vector<std::uint8_t>>::ok(std::vector<std::uint8_t>(s.begin(), s.end()));
}

result<SessionFrame> DecodeSessionFrame(const std::uint8_t* data, std::size_t size)
{
    if (data == nullptr || size == 0)
        return result<SessionFrame>::err(MakeError(ErrorCode::ProtocolError, "empty frame bytes"));
    pbv2::SessionFrame p;
    if (!p.ParseFromArray(data, static_cast<int>(size)))
        return result<SessionFrame>::err(MakeError(ErrorCode::ProtocolError, "frame parse failed"));
    return FromProto(p);
}

result<SessionFrame> DecodeSessionFrame(const std::vector<std::uint8_t>& bytes)
{
    return DecodeSessionFrame(bytes.data(), bytes.size());
}

result<SessionFrame> DecodeSessionFrame(const std::string& bytes)
{
    return DecodeSessionFrame(reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size());
}

/**
 * @}
 * @name Request/Response 互转 @{
 */
result<SessionFrame> ToSessionFrame(
    const Request& request,
    FrameKind kind,
    std::uint64_t session_epoch,
    std::uint32_t remaining_budget_ms,
    std::optional<bool> acceptance_requested)
{
    if (kind != FrameKind::Call && kind != FrameKind::Send)
        return result<SessionFrame>::err(
            MakeError(ErrorCode::InvalidArgument, "ToSessionFrame only accepts Call/Send"));
    SessionFrame f;
    f.kind = kind;
    f.traffic = request.Traffic();
    f.session_epoch = session_epoch;
    f.message_id = request.MessageId();
    f.request_id = request.RequestId();
    f.service = request.Service();
    f.method = request.Method();
    f.reply = request.Reply();
    f.sequence = request.Sequence();
    f.ordering_key = request.OrderingKey();
    f.remaining_budget_ms = remaining_budget_ms;
    f.request_schema = request.RequestSchema();
    f.response_schema = request.ResponseSchema();
    f.payload = request.Payload();
    f.metadata = request.Metadata();
    f.acceptance_requested = acceptance_requested;
    if (auto r = ValidateSessionFrame(f); !r)
        return result<SessionFrame>::err(r.error());
    return result<SessionFrame>::ok(std::move(f));
}

result<Request> RequestFromSessionFrame(const SessionFrame& frame, RequestContext context)
{
    if (frame.kind != FrameKind::Call && frame.kind != FrameKind::Send)
        return result<Request>::err(
            MakeError(ErrorCode::InvalidArgument, "RequestFromSessionFrame only accepts Call/Send"));
    Request r;
    r.SetService(frame.service)
        .SetMethod(frame.method)
        .SetRequestId(frame.request_id)
        .SetMessageId(frame.message_id)
        .SetRequestSchema(frame.request_schema)
        .SetResponseSchema(frame.response_schema)
        .SetPayload(frame.payload)
        .SetMetadata(frame.metadata)
        .SetTraffic(frame.traffic)
        .SetReply(frame.reply)
        .SetOrderingKey(frame.ordering_key)
        .SetSequence(frame.sequence);
    detail::RequestContextAccess::Set(r, std::move(context));
    return result<Request>::ok(std::move(r));
}

result<SessionFrame> ResponseToSessionFrame(const Response& response, std::uint64_t session_epoch)
{
    SessionFrame f;
    f.kind = FrameKind::Response;

    /**
     * 调用方按 Request traffic 覆盖
     */
    f.traffic = TrafficClass::Service;
    f.session_epoch = session_epoch;
    f.request_id = response.RequestId();
    f.service = response.Service();
    f.method = response.Method();
    f.reply = ReplyMode::Required;
    f.request_schema = response.RequestSchema();
    f.response_schema = response.ResponseSchema();
    f.payload = response.Payload();
    f.metadata = response.Metadata();
    f.has_outcome = true;
    f.success = true;
    return result<SessionFrame>::ok(std::move(f));
}

result<Response> ResponseFromSessionFrame(const SessionFrame& frame)
{
    if (frame.kind != FrameKind::Response)
        return result<Response>::err(
            MakeError(ErrorCode::InvalidArgument, "ResponseFromSessionFrame only accepts Response"));
    Response r;
    r.SetRequestId(frame.request_id)
        .SetService(frame.service)
        .SetMethod(frame.method)
        .SetRequestSchema(frame.request_schema)
        .SetResponseSchema(frame.response_schema)
        .SetPayload(frame.payload)
        .SetMetadata(frame.metadata);
    return result<Response>::ok(std::move(r));
}

/**
 * @}
 * @name u32be 分帧 @{
 */
void EncodeFramePrefix(std::uint32_t payload_length, std::uint8_t out[4]) noexcept
{
    out[0] = static_cast<std::uint8_t>(payload_length >> 24);
    out[1] = static_cast<std::uint8_t>(payload_length >> 16);
    out[2] = static_cast<std::uint8_t>(payload_length >> 8);
    out[3] = static_cast<std::uint8_t>(payload_length);
}

std::uint32_t DecodeFramePrefix(const std::uint8_t in[4]) noexcept
{
    return (static_cast<std::uint32_t>(in[0]) << 24) | (static_cast<std::uint32_t>(in[1]) << 16)
           | (static_cast<std::uint32_t>(in[2]) << 8) | (static_cast<std::uint32_t>(in[3]));
}

/**
 * kind 相关帧上限：握手帧 min(max,64KiB)，数据帧 max。
 */
std::size_t FrameByteLimit(FrameKind kind, std::size_t max_frame_bytes)
{
    if (kind == FrameKind::Hello || kind == FrameKind::HelloAck)
        return std::min(max_frame_bytes, kRpcV2HelloMaxFrameBytes);
    return max_frame_bytes;
}

result<std::vector<std::uint8_t>> EncodeFramedSessionFrame(const SessionFrame& frame, std::size_t max_frame_bytes)
{
    if (max_frame_bytes == 0 || max_frame_bytes > kRpcV2MaxFrameBytes)
        return result<std::vector<std::uint8_t>>::err(
            MakeError(ErrorCode::InvalidArgument, "max_frame_bytes out of range"));
    auto body = EncodeSessionFrame(frame);
    if (!body)
        return result<std::vector<std::uint8_t>>::err(body.error());
    const std::size_t limit = FrameByteLimit(frame.kind, max_frame_bytes);
    if (body.value().size() > limit)
        return result<std::vector<std::uint8_t>>::err(
            MakeError(ErrorCode::InvalidArgument, "frame body exceeds kind byte limit"));
    std::vector<std::uint8_t> out(4 + body.value().size());
    EncodeFramePrefix(static_cast<std::uint32_t>(body.value().size()), out.data());
    std::memcpy(out.data() + 4, body.value().data(), body.value().size());
    return result<std::vector<std::uint8_t>>::ok(std::move(out));
}

/**
 * @}
 * @name SessionFrameDecoder @{
 */
struct SessionFrameDecoder::Impl {
    explicit Impl(std::size_t max_frame_bytes)
        : max_frame_bytes(max_frame_bytes)
    {
    }

    std::size_t max_frame_bytes;
    std::vector<std::uint8_t> buffer;
    std::optional<Error> terminal_error;

    /**
     * hello_phase=true（默认）：握手态，前缀限额收窄到 min(max,64KiB)。
     */
    bool hello_phase{true};

    /**
     * 当前阶段的单帧声明上限：握手态 min(max,64KiB)，数据态 max。
     */
    std::size_t DeclaredLimit() const noexcept
    {
        return hello_phase ? std::min(max_frame_bytes, kRpcV2HelloMaxFrameBytes) : max_frame_bytes;
    }

    /**
     * 未决字节硬上限：前缀 + 两帧当前阶段上限（容纳合法流水线：同一 recv 内
     * 至少两个完整帧 + 一帧前缀）。握手态整体缓冲天然 ≤4+2*64KiB，大声明
     * 前缀在复制 body 前已被 DeclaredLimit 拒绝。
     */
    std::size_t PendingLimit() const noexcept { return 4 + 2 * std::min(DeclaredLimit(), kRpcV2MaxFrameBytes); }
};

SessionFrameDecoder::SessionFrameDecoder(std::size_t max_frame_bytes)
    : m_impl(new Impl(max_frame_bytes))
{
}

SessionFrameDecoder::~SessionFrameDecoder() = default;
SessionFrameDecoder::SessionFrameDecoder(SessionFrameDecoder&&) noexcept = default;
SessionFrameDecoder& SessionFrameDecoder::operator=(SessionFrameDecoder&&) noexcept = default;

void SessionFrameDecoder::SetHelloPhase(bool hello_phase) noexcept
{
    m_impl->hello_phase = hello_phase;
}

std::size_t SessionFrameDecoder::PendingBytes() const noexcept
{
    return m_impl->buffer.size();
}

std::vector<std::uint8_t> SessionFrameDecoder::TakePending()
{
    return std::move(m_impl->buffer);
}

result<void> SessionFrameDecoder::Feed(const std::uint8_t* data, std::size_t size)
{
    if (m_impl->terminal_error)
        return result<void>::err(*m_impl->terminal_error);
    if (data == nullptr && size != 0)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument, "null feed data with non-zero size"));

    /**
     * 分配/复制前检查未决字节硬上限（随阶段收窄：握手态 4+2*min(max,64KiB)）。
     * 缓冲可合法超过首帧声明长度——流水线帧是正常形态，由 Next() 逐帧取走。
     */
    if (size > m_impl->PendingLimit() || m_impl->buffer.size() + size > m_impl->PendingLimit()) {
        m_impl->terminal_error = MakeError(ErrorCode::ProtocolError, "pending decode bytes exceed hard limit");
        return result<void>::err(*m_impl->terminal_error);
    }

    /**
     * 逐帧复制：每一帧先拼齐 4 字节前缀并按当前阶段 DeclaredLimit 校验，
     * 校验通过才复制其 body（ADR §5.2：检查长度后才允许分配 body）。覆盖
     * 三种落点：首帧前缀、完整帧后的流水线帧前缀（同批或跨 Feed 分批）、
     * 以及前缀已齐未验的防御路径——超限声明在任一位置都只保留前缀字节、
     * body 永不入缓冲。
     * 缓冲不变量（Session 层每解出一帧即 Next/TakePending）：完整帧只能
     * 出现在缓冲头部，其后至多是同一个未完成帧。
     *
     * 未消费的输入下标
     */
    std::size_t offset = 0;

    /**
     * 当前帧在 buffer 的起点
     */
    std::size_t frame_start = 0;
    const std::size_t limit = m_impl->DeclaredLimit();
    while (true) {
        const std::size_t have = m_impl->buffer.size() - frame_start;
        if (have < 4) {

            /**
             * 当前帧前缀未齐：尽量补到 4；输入耗尽即返回。
             */
            const std::size_t take = std::min(4 - have, size - offset);
            m_impl->buffer.insert(m_impl->buffer.end(), data + offset, data + offset + take);
            offset += take;
            if (m_impl->buffer.size() - frame_start < 4)
                return result<void>::ok();
        }
        if (frame_start != 0 && m_impl->hello_phase) {

            /**
             * 握手阶段完整帧之后的余字节是未验证 stash 候选（Session 在每帧
             * 解出后 TakePending 移交，握手验证完成后才按数据阶段限额补喂）：
             * 其前缀属未来阶段，不能按握手 DeclaredLimit 判定——原样收进
             * 缓冲（总量仍受 PendingLimit 约束），校验延后到阶段切换。
             */
            m_impl->buffer.insert(m_impl->buffer.end(), data + offset, data + size);
            return result<void>::ok();
        }
        const auto declared = DecodeFramePrefix(m_impl->buffer.data() + frame_start);
        if (declared > limit) {
            m_impl->terminal_error = MakeError(ErrorCode::ProtocolError, "declared frame length exceeds phase limit");
            return result<void>::err(*m_impl->terminal_error);
        }
        const std::size_t frame_end = frame_start + 4 + declared;
        if (m_impl->buffer.size() >= frame_end) {

            /**
             * 该帧已完整在缓冲（等 Next 取走）：跳到下一帧前缀继续校验。
             */
            frame_start = frame_end;
            continue;
        }
        if (offset >= size)

            /**
             * 前缀已验，无输入可吃
             */
            return result<void>::ok();

        /**
         * declared ≤ limit 已成立：帧全长 4+declared ≤ PendingLimit 成立
         * （PendingLimit ≥ 4+2*limit），按帧界复制不会越过已验前缀。
         */
        const std::size_t need = frame_end - m_impl->buffer.size();
        const std::size_t take = std::min(need, size - offset);
        m_impl->buffer.insert(m_impl->buffer.end(), data + offset, data + offset + take);
        offset += take;
        if (m_impl->buffer.size() < frame_end)

            /**
             * body 未齐，待下次 Feed
             */
            return result<void>::ok();
        if (offset >= size)

            /**
             * 完整帧留待 Next 取走
             */
            return result<void>::ok();

        /**
         * 流水线：校验下一帧前缀
         */
        frame_start = frame_end;
    }
}

result<void> SessionFrameDecoder::Feed(const std::vector<std::uint8_t>& bytes)
{
    return Feed(bytes.data(), bytes.size());
}

result<void> SessionFrameDecoder::Feed(const std::string& bytes)
{
    return Feed(reinterpret_cast<const std::uint8_t*>(bytes.data()), bytes.size());
}

result<std::optional<SessionFrame>> SessionFrameDecoder::Next()
{
    using R = result<std::optional<SessionFrame>>;
    if (m_impl->terminal_error)
        return R::err(*m_impl->terminal_error);
    if (m_impl->buffer.size() < 4)
        return R::ok(std::nullopt);
    const auto declared = DecodeFramePrefix(m_impl->buffer.data());
    if (declared == 0) {
        m_impl->terminal_error = MakeError(ErrorCode::ProtocolError, "frame payload_length zero is rejected");
        return R::err(*m_impl->terminal_error);
    }
    if (declared > m_impl->DeclaredLimit()) {
        m_impl->terminal_error = MakeError(ErrorCode::ProtocolError, "frame payload_length exceeds phase limit");
        return R::err(*m_impl->terminal_error);
    }
    if (m_impl->buffer.size() < 4 + declared)
        return R::ok(std::nullopt);

    /**
     * 完整帧就绪：先按 declared 上限做数据帧粗检（kind 细分上限在 body 解码
     * 后按 kind 复核——前缀阶段不分配 body 副本之外的大内存）。
     */
    auto frame = DecodeSessionFrame(m_impl->buffer.data() + 4, declared);
    if (!frame) {
        m_impl->terminal_error = frame.error();
        return R::err(*m_impl->terminal_error);
    }
    const std::size_t kind_limit = FrameByteLimit(frame.value().kind, m_impl->max_frame_bytes);
    if (declared > kind_limit) {
        m_impl->terminal_error = MakeError(ErrorCode::ProtocolError, "frame body exceeds kind byte limit");
        return R::err(*m_impl->terminal_error);
    }
    m_impl->buffer.erase(m_impl->buffer.begin(), m_impl->buffer.begin() + 4 + declared);
    return R::ok(std::move(frame).value());
}

bool SessionFrameDecoder::IsTerminal() const noexcept
{
    return m_impl->terminal_error.has_value();
}

/**
 * @}
 */
// clang-format off
}
// clang-format on
