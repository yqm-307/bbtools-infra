#include "redis/RedisDetail.hpp"

namespace bbt::infra::redis_detail {

result<RawReply> DecodeReply(const redisReply* reply) {
    if (reply == nullptr)
        return result<RawReply>::err(MakeError(ErrorCode::TransportError,
            "redis: connection lost before reply"));

    switch (reply->type) {
    case REDIS_REPLY_STATUS:
        return result<RawReply>::ok(RawReply{
            RawReply::Type::Status,
            std::string(reply->str, reply->len), 0});
    case REDIS_REPLY_STRING:
        return result<RawReply>::ok(RawReply{
            RawReply::Type::Bulk,
            std::string(reply->str, reply->len), 0});
    case REDIS_REPLY_NIL:
        return result<RawReply>::ok(RawReply{RawReply::Type::Nil, {}, 0});
    case REDIS_REPLY_INTEGER:
        return result<RawReply>::ok(
            RawReply{RawReply::Type::Integer, {}, reply->integer});
    case REDIS_REPLY_ERROR: {
        std::string text(reply->str, reply->len);
        Error e = MakeError(ErrorCode::RemoteError,
            "redis: server error reply: " + text);
        const auto sp = text.find(' ');
        e.domain_code = sp == std::string::npos ? text : text.substr(0, sp);
        e.backend_category = "redis";
        return result<RawReply>::err(std::move(e));
    }
    default:
        return result<RawReply>::ok(RawReply{RawReply::Type::Other, {}, 0});
    }
}

Error ClassifyHiredisError(int err, const char* errstr,
                         std::string fallback_message) {
    Error e;
    e.message = errstr != nullptr && errstr[0] != '\0'
                    ? fallback_message + ": " + errstr
                    : std::move(fallback_message);
    e.backend_category = "hiredis";
    e.backend_code     = err;
    switch (err) {
    case REDIS_ERR_IO:
    case REDIS_ERR_EOF:
        e.code        = ErrorCode::TransportError;
        e.domain_code = "transport";
        break;
    case REDIS_ERR_PROTOCOL:
        e.code        = ErrorCode::ProtocolError;
        e.domain_code = "protocol";
        break;
    case REDIS_ERR_TIMEOUT:
        e.code        = ErrorCode::TimedOut;
        e.domain_code = "timeout";
        break;
    case REDIS_ERR_OOM:
        e.code        = ErrorCode::InternalError;
        e.domain_code = "oom";
        break;
    case REDIS_ERR_OTHER:
        // getaddrinfo/参数类失败落在 OTHER：按对端不可达归类。
        e.code        = ErrorCode::Unavailable;
        e.domain_code = "resolve_or_other";
        break;
    default:
        e.code        = ErrorCode::InternalError;
        e.domain_code = "internal";
        break;
    }
    return e;
}

} // namespace bbt::infra::redis_detail
