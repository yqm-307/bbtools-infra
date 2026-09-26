#pragma once
// infra mongo 内部工具：infra BSON 字节载体 ↔ bsoncxx 视图的校验转换、
// 生效 URI 的驱动超时注入、mongocxx/bsoncxx 异常 → 契约 Error 的分类。
// 仅供 src/mongo/ 实现使用，不安装、不进公开面；允许出现 mongocxx/
// bsoncxx 类型。

#include <optional>
#include <string>

#include <bsoncxx/v1/document/view.hpp>
#include <bsoncxx/v1/exception-fwd.hpp>
#include <mongocxx/exception/exception-fwd.hpp>
#include <mongocxx/exception/operation_exception-fwd.hpp>
#include <mongocxx/v1/exception.hpp>

#include <bbt/infra/CoMongoCli.hpp>
#include <bbt/infra/mongo/Client.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra::mongo_detail {

// 一次 CRUD 调用的结果载荷（worker 线程产出，协程消费）。
struct MongoOpOutcome {
    std::optional<MongoDocument> doc;   // FindOne 命中（未命中为 nullopt）
    std::int64_t matched  = 0;
    std::int64_t modified = 0;
    std::int64_t upserted = 0;
    std::int64_t deleted  = 0;
};

// MongoDocument.bytes → bsoncxx 只读视图：空字节或结构不良构 →
// InvalidArgument（完整校验经 libbson validate，不止长度一致性）。
// 视图仅借用入参字节不拷贝；调用方保证 view 使用期内 bytes 存活。
result<bsoncxx::v1::document::view> ViewOf(const MongoDocument& doc,
                                         const char*        what);

// document::view → 拥有型 MongoDocument（整段字节拷贝）。
MongoDocument DocOf(bsoncxx::v1::document::view v);

// mongocxx 异常 → 契约 Error：
//   - v_noabi::operation_exception（CRUD 服务端失败的实际抛出类型）与
//     v1::server_error → RemoteError；domain_code 取
//     raw_server_error()/raw() 的 ["codeName"]（缺省 "server"），
//     backend_code 取 ["code"]，缺省回退 e.code()；
//   - type_errc::invalid_argument 及 client/database/collection 的
//     invalid_*_name → InvalidArgument；
//   - pool::errc::wait_queue_timeout → Overloaded；
//   - mongoc 域（server selection/socket/stream/pool 等）→ Unavailable；
//   - 其余 → InternalError。
// backend_category 固定 "mongocxx"，backend_code 为 e.code().value()。
Error ClassifyDriverError(const mongocxx::v1::exception& e,
                          const char*                    what);

// v_noabi::operation_exception → 契约 Error：raw_server_error() 存在时
// → RemoteError（codeName→domain_code、code→backend_code）；无服务端
// 回复的 operation 失败 → Unavailable。独立于 ClassifyDriverError：
// v_noabi::exception 并非 v1::exception 子类，必须单独捕获。
Error ClassifyOperationError(
    const mongocxx::v_noabi::operation_exception& e, const char* what);

// 其余 v_noabi::exception（query_exception 等驱动层错误）→ InternalError，
// backend_category 固定 "mongocxx"。
Error ClassifyVNoabiError(const mongocxx::v_noabi::exception& e,
                          const char*                       what);

// bsoncxx 异常 → 契约 Error：invalid_argument → InvalidArgument，
// 其余 → InternalError；backend_category 固定 "bsoncxx"。
Error ClassifyBsonError(const bsoncxx::v1::exception& e, const char* what);

// 装配 URI → 生效 URI：缺省注入 serverSelectionTimeoutMS/
// connectTimeoutMS/socketTimeoutMS/waitQueueTimeoutMS/maxPoolSize；
// 已在 URI 中显式给出的同名项（大小写不敏感）不覆盖。
// 输入是 owner 级 MongoRuntimeConfig：pool 与 worker 预算归 owner
// （Issue #40），database/collection 不再参与生效 URI。
std::string EffectiveUri(const mongo::MongoRuntimeConfig& cfg);

} // namespace bbt::infra::mongo_detail
