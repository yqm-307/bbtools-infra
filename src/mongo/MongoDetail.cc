#include "mongo/MongoDetail.hpp"

#include <algorithm>
#include <cctype>
#include <cstring>

#include <bsoncxx/document/element.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/types.hpp>
#include <bsoncxx/validate.hpp>
#include <bsoncxx/v1/element/view.hpp>
#include <bsoncxx/v1/exception.hpp>
#include <bsoncxx/v1/types/id.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <mongocxx/v1/client.hpp>
#include <mongocxx/v1/collection.hpp>
#include <mongocxx/v1/database.hpp>
#include <mongocxx/v1/pool.hpp>
#include <mongocxx/v1/server_error.hpp>

namespace bbt::infra::mongo_detail {

namespace {

Error InvalidArg(std::string msg) {
    return MakeError(ErrorCode::InvalidArgument, std::move(msg));
}

} // namespace

result<bsoncxx::v1::document::view> ViewOf(const MongoDocument& doc,
                                         const char*        what) {
    if (doc.bytes.empty())
        return result<bsoncxx::v1::document::view>::err(InvalidArg(
            std::string("mongo: empty ") + what + " document"));
    // libbson 全结构校验（长度、元素边界、终止符），不是仅看首部长度。
    if (!bsoncxx::validate(doc.bytes.data(), doc.bytes.size()))
        return result<bsoncxx::v1::document::view>::err(InvalidArg(
            std::string("mongo: malformed BSON in ") + what));
    try {
        return result<bsoncxx::v1::document::view>::ok(
            bsoncxx::v1::document::view{doc.bytes.data(), doc.bytes.size()});
    } catch (const bsoncxx::v1::exception&) {
        return result<bsoncxx::v1::document::view>::err(InvalidArg(
            std::string("mongo: invalid BSON view in ") + what));
    }
}

MongoDocument DocOf(bsoncxx::v1::document::view v) {
    MongoDocument out;
    if (v.data() != nullptr && v.length() > 0)
        out.bytes.assign(v.data(), v.data() + v.length());
    return out;
}

Error ClassifyDriverError(const mongocxx::v1::exception& e,
                          const char*                    what) {
    Error err;
    err.message           = std::string(what) + ": " + e.what();
    err.backend_category  = "mongocxx";
    err.backend_code      = e.code().value();

    // v1::server_error：raw() 文档承载服务端 codeName/code。
    // 注意：CRUD 失败在 r4.x 实际抛 v_noabi::operation_exception（不是
    // v1::exception 子类），由 ClassifyOperationError 单独归类。
    if (const auto* se =
            dynamic_cast<const mongocxx::v1::server_error*>(&e)) {
        err.code        = ErrorCode::RemoteError;
        err.domain_code = "server";
        try {
            const auto raw = se->raw();
            const auto el  = raw["codeName"];
            if (el &&
                el.type_id() == bsoncxx::v1::types::id::k_string) {
                const auto sv = el.get_string().value;
                err.domain_code.assign(sv.data(), sv.size());
            }
            const auto ce = raw["code"];
            if (ce && ce.type_id() == bsoncxx::v1::types::id::k_int32)
                err.backend_code = ce.get_int32().value;
            else if (ce &&
                     ce.type_id() == bsoncxx::v1::types::id::k_int64)
                err.backend_code = ce.get_int64().value;
        } catch (...) {
        }
        if (err.domain_code == "server" &&
            (err.backend_code == 11000 || err.backend_code == 11001 ||
             err.backend_code == 12582))
            err.domain_code = "DuplicateKey";
        return err;
    }

    const std::error_code ec = e.code();
    if (ec == mongocxx::v1::type_errc::invalid_argument) {
        err.code        = ErrorCode::InvalidArgument;
        err.domain_code = "invalid_argument";
        return err;
    }
    const auto& cat = ec.category();
    if (cat == mongocxx::v1::pool::error_category()) {
        if (ec.value() ==
            static_cast<int>(mongocxx::v1::pool::errc::wait_queue_timeout)) {
            err.code        = ErrorCode::Overloaded;
            err.domain_code = "wait_queue_timeout";
        } else {
            err.code        = ErrorCode::Unavailable;
            err.domain_code = "pool";
        }
        return err;
    }
    if ((cat == mongocxx::v1::client::error_category() &&
         ec.value() == static_cast<int>(
             mongocxx::v1::client::errc::invalid_database_name)) ||
        (cat == mongocxx::v1::database::error_category() &&
         ec.value() == static_cast<int>(
             mongocxx::v1::database::errc::invalid_collection_name)) ||
        (cat == mongocxx::v1::collection::error_category() &&
         ec.value() == static_cast<int>(
             mongocxx::v1::collection::errc::invalid_collection_name))) {
        err.code        = ErrorCode::InvalidArgument;
        err.domain_code = "invalid_name";
        return err;
    }
    // 服务端错误的非 server_error 形态：驱动部分路径只以普通
    // v1::exception + server_error_category 抛出（其 equivalent() 使
    // ec == source_errc::server 成立）；codeName 不可得时按已知码归类。
    if (ec == mongocxx::v1::source_errc::server) {
        err.code        = ErrorCode::RemoteError;
        err.domain_code = "server";
        if (ec.value() == 11000 || ec.value() == 11001 ||
            ec.value() == 12582)
            err.domain_code = "DuplicateKey";
        return err;
    }
    // mongoc 域（server selection/socket/stream/pool 等）：driver 侧失败，
    // 对端不可达或传输失败统一 Unavailable。
    if (ec == mongocxx::v1::source_errc::mongoc ||
        ec == mongocxx::v1::source_errc::mongocrypt) {
        err.code        = ErrorCode::Unavailable;
        err.domain_code = "mongoc";
        return err;
    }
    err.code        = ErrorCode::InternalError;
    err.domain_code = "internal";
    return err;
}

Error ClassifyOperationError(
    const mongocxx::v_noabi::operation_exception& e, const char* what) {
    Error err;
    err.message           = std::string(what) + ": " + e.what();
    err.backend_category  = "mongocxx";
    err.backend_code      = e.code().value();
    // raw_server_error() 存在 → 服务端拒绝 → RemoteError；
    // 无服务端回复（客户端侧 operation 失败）→ Unavailable。
    if (!e.raw_server_error()) {
        err.code        = ErrorCode::Unavailable;
        err.domain_code = "driver";
        return err;
    }
    err.code        = ErrorCode::RemoteError;
    err.domain_code = "server";
    try {
        const auto raw = e.raw_server_error()->view();
        const auto el  = raw["codeName"];
        if (el && el.type() == bsoncxx::v_noabi::type::k_string) {
            const auto sv = el.get_string().value;
            err.domain_code.assign(sv.data(), sv.size());
        }
        const auto ce = raw["code"];
        if (ce && ce.type() == bsoncxx::v_noabi::type::k_int32)
            err.backend_code = ce.get_int32().value;
        else if (ce && ce.type() == bsoncxx::v_noabi::type::k_int64)
            err.backend_code = ce.get_int64().value;
    } catch (...) {
    }
    if (err.domain_code == "server" &&
        (err.backend_code == 11000 || err.backend_code == 11001 ||
         err.backend_code == 12582))
        err.domain_code = "DuplicateKey";
    return err;
}

Error ClassifyVNoabiError(const mongocxx::v_noabi::exception& e,
                          const char*                       what) {
    Error err;
    err.message           = std::string(what) + ": " + e.what();
    err.backend_category  = "mongocxx";
    err.backend_code      = e.code().value();
    err.code              = ErrorCode::InternalError;
    err.domain_code       = "internal";
    return err;
}

Error ClassifyBsonError(const bsoncxx::v1::exception& e, const char* what) {
    Error err;
    err.message          = std::string(what) + ": " + e.what();
    err.backend_category = "bsoncxx";
    err.backend_code     = e.code().value();
    if (e.code() == bsoncxx::v1::type_errc::invalid_argument) {
        err.code        = ErrorCode::InvalidArgument;
        err.domain_code = "invalid_argument";
    } else {
        err.code        = ErrorCode::InternalError;
        err.domain_code = "internal";
    }
    return err;
}

std::string EffectiveUri(const MongoClientConfig& cfg) {
    std::string uri = cfg.uri;
    std::string lowered = uri;
    std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                   [](unsigned char c) {
                       return static_cast<char>(std::tolower(c));
                   });
    auto append = [&](const char* key, long long value) {
        std::string needle = key;
        std::transform(needle.begin(), needle.end(), needle.begin(),
                       [](unsigned char c) {
                           return static_cast<char>(std::tolower(c));
                       });
        needle += '=';
        if (lowered.find(needle) != std::string::npos)
            return;   // URI 已显式给出：不覆盖调用方取值
        uri += (uri.find('?') == std::string::npos) ? '?' : '&';
        uri += needle + std::to_string(value);
    };
    append("serverSelectionTimeoutMS",
           static_cast<long long>(cfg.server_selection_timeout.count()));
    append("connectTimeoutMS",
           static_cast<long long>(cfg.connect_timeout.count()));
    append("socketTimeoutMS",
           static_cast<long long>(cfg.socket_timeout.count()));
    append("waitQueueTimeoutMS",
           static_cast<long long>(cfg.wait_queue_timeout.count()));
    // 每项 operation 在同一 worker 上 acquire/use/release：并发 lease
    // 上限即 worker 数，连接池按此显式有界。
    append("maxPoolSize", static_cast<long long>(cfg.worker_threads));
    return uri;
}

} // namespace bbt::infra::mongo_detail
