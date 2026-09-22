#pragma once
// infra redis 内部工具：redisReply 解码为拥有型 RawReply、hiredis 错误码
// 与 Redis 服务端错误到契约 Error 的分类。
// 仅供 src/redis/ 实现使用，不安装、不进公开面；允许出现 hiredis 类型。

#include <cstdint>
#include <string>

#include <hiredis/hiredis.h>

#include <bbt/infra/Result.hpp>

namespace bbt::infra::redis_detail {

// redisReply 的值语义快照：回调返回前完成拷贝，之后不再触碰 hiredis 内存。
struct RawReply {
    enum class Type {
        Status,   // REDIS_REPLY_STATUS（PONG/OK 等）
        Integer,  // REDIS_REPLY_INTEGER
        Bulk,     // REDIS_REPLY_STRING（二进制安全）
        Nil,      // REDIS_REPLY_NIL
        Other,    // 本切片命令不期望的类型（ARRAY/MAP/DOUBLE...）
    };
    Type        type;
    std::string str;        // Status/Bulk 负载（str+len 拷贝，二进制安全）
    long long   integer = 0;
};

// 解码一条 redisReply：
//   - nullptr（连接已断/释放前 pending 回调）→ TransportError；
//   - REDIS_REPLY_ERROR → RemoteError，domain_code 为错误串首个词
//     （如 WRONGTYPE/ERR），message 为完整错误串；
//   - 其余按类型映射为 RawReply。
result<RawReply> DecodeReply(const redisReply* reply);

// hiredis ac->err（REDIS_ERR_*）到契约 Error：
//   - IO/EOF → TransportError；PROTOCOL → ProtocolError；
//   - TIMEOUT → TimedOut；OOM → InternalError；
//   - OTHER（含 getaddrinfo 解析失败）→ Unavailable；未知 → InternalError。
// backend_category 固定 "hiredis"，backend_code 为原始 err。
Error ClassifyHiredisError(int err, const char* errstr,
                         std::string fallback_message);

} // namespace bbt::infra::redis_detail
