#include "RedisDetail.hpp"

namespace bbt::infra::redis_detail {

result<RawReply> DecodeReply(const redisReply* reply) {
    if (reply == nullptr)
        return result<RawReply>::err(MakeError(ErrorCode::TransportError,
            "redis: connection lost before reply"));

    switch (reply->type) {
    case REDIS_REPLY_STATUS:
        return result<RawReply>::ok(RawReply{
            RawReply::Type::Status,
            std::string(reply->str, reply->len), 0, {}});
    case REDIS_REPLY_STRING:
        return result<RawReply>::ok(RawReply{
            RawReply::Type::Bulk,
            std::string(reply->str, reply->len), 0, {}});
    case REDIS_REPLY_NIL:
        return result<RawReply>::ok(RawReply{RawReply::Type::Nil, {}, 0, {}});
    case REDIS_REPLY_INTEGER:
        return result<RawReply>::ok(
            RawReply{RawReply::Type::Integer, {}, reply->integer, {}});
    case REDIS_REPLY_ERROR: {
        std::string text(reply->str, reply->len);
        Error e = MakeError(ErrorCode::RemoteError,
            "redis: server error reply: " + text);
        const auto sp = text.find(' ');
        e.domain_code = sp == std::string::npos ? text : text.substr(0, sp);
        e.backend_category = "redis";
        return result<RawReply>::err(std::move(e));
    }
    case REDIS_REPLY_ARRAY: {
        // 只支持一维扁平数组：嵌套数组与其他元素类型整体拒绝为
        // ProtocolError，不截断后继续；元素与负载字节总量有界。
        if (reply->elements > kMaxArrayElements) {
            return result<RawReply>::err(MakeError(ErrorCode::ProtocolError,
                "redis: array reply exceeds max elements"));
        }
        RawReply raw{RawReply::Type::Array, {}, 0, {}};
        raw.elements.reserve(reply->elements);
        std::size_t total_bytes = 0;
        for (std::size_t i = 0; i < reply->elements; ++i) {
            const redisReply* element = reply->element[i];
            if (element == nullptr) {
                return result<RawReply>::err(MakeError(ErrorCode::ProtocolError,
                    "redis: null array element"));
            }
            RawReply::Element flat;
            switch (element->type) {
            case REDIS_REPLY_STRING:
            case REDIS_REPLY_STATUS:
                // 复制前先按剩余预算校验本元素字节数，checked-add 累计：
                // 单个超限元素不产生分配，累计值不可能回绕越过上限。
                if (element->str == nullptr && element->len != 0) {
                    return result<RawReply>::err(MakeError(ErrorCode::ProtocolError,
                        "redis: malformed array element"));
                }
                if (element->len > kMaxArrayBytes - total_bytes) {
                    return result<RawReply>::err(MakeError(ErrorCode::ProtocolError,
                        "redis: array reply exceeds max bytes"));
                }
                flat.str.assign(element->str, element->len);
                total_bytes += flat.str.size();
                break;
            case REDIS_REPLY_NIL:
                flat.is_nil = true;
                break;
            case REDIS_REPLY_INTEGER:
                flat.str = std::to_string(element->integer);
                break;
            default:
                return result<RawReply>::err(MakeError(ErrorCode::ProtocolError,
                    "redis: unsupported array element type"));
            }
            raw.elements.push_back(std::move(flat));
        }
        return result<RawReply>::ok(std::move(raw));
    }
    default:
        return result<RawReply>::ok(RawReply{RawReply::Type::Other, {}, 0, {}});
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
