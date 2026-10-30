#pragma once
// infra redis 内部工具：redisReply 解码为拥有型 RawReply、hiredis 错误码
// 与 Redis 服务端错误到契约 Error 的分类。
// 仅供 src/redis/ 实现使用，不安装、不进公开面；允许出现 hiredis 类型。

#include <cstdint>
#include <string>
#include <vector>

#include <hiredis/hiredis.h>

#include <bbt/infra/Result.hpp>

namespace bbt::infra::redis_detail {

// 一维数组回复的有界解码上限（HGETALL 等命令使用）：
//   - kMaxArrayElements：单条数组回复最多元素个数（HGETALL 为 field/value
//     交替，故可表达至多 kMaxArrayElements/2 个字段）；
//   - kMaxArrayBytes：数组内全部 Bulk/Status 元素负载字节合计上限。
// 取值依据：本入口面向常规 KV/Hash 数据面，64K 元素 / 16 MiB 已远超
// 「单条命令适合 Engine 单次全量写容量」的请求侧约束；超限说明对端回复
// 超出本入口的数据面定位，按不支持回复退役连接。数组解码不做增量，
// 一次 DecodeReply 完整物化。
inline constexpr std::size_t kMaxArrayElements = 65536;
inline constexpr std::size_t kMaxArrayBytes    = 16 * 1024 * 1024;

// redisReply 的值语义快照：回调返回前完成拷贝，之后不再触碰 hiredis 内存。
struct RawReply {
    enum class Type {
        Status,   // REDIS_REPLY_STATUS（PONG/OK 等）
        Integer,  // REDIS_REPLY_INTEGER
        Bulk,     // REDIS_REPLY_STRING（二进制安全）
        Nil,      // REDIS_REPLY_NIL
        Array,    // REDIS_REPLY_ARRAY 的一维扁平快照（元素按服务端顺序）
        Other,    // 本切片命令不期望的类型（MAP/DOUBLE/嵌套 ARRAY...）
    };
    // 一维数组元素的扁平视图：只保留调用方需要的形态，不递归。
    struct Element {
        bool        is_nil = false; // true 时忽略 str（数组内 nil 元素）
        std::string str;            // Bulk/Status 负载或 Integer 的十进制文本
    };
    Type        type;
    std::string str;        // Status/Bulk 负载（str+len 拷贝，二进制安全）
    long long   integer = 0;
    // Type==Array 时的元素序列（服务端顺序，二进制安全）；其余类型恒空。
    std::vector<Element> elements;
};

// 解码一条 redisReply：
//   - nullptr（连接已断/释放前 pending 回调）→ TransportError；
//   - REDIS_REPLY_ERROR → RemoteError，domain_code 为错误串首个词
//     （如 WRONGTYPE/ERR），message 为完整错误串；
//   - REDIS_REPLY_ARRAY → RawReply::Type::Array，一维扁平快照：元素只接受
//     Bulk/Status/Nil/Integer（Integer 转十进制文本）；空元素指针、str 为空
//     但 len 非零、嵌套数组、元素数或负载字节超 kMaxArrayElements/
//     kMaxArrayBytes、或其他元素类型一律 ProtocolError（调用方据此退役
//     连接，已接纳命令为 OutcomeUnknown）。字节预算在复制元素负载前按
//     剩余额度校验（checked-add），不为单个超限元素先分配再检查。
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
