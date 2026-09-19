#pragma once
// 进程级驱动运行域（Issue #7 裁决：一个进程级 mongocxx::pool）：
// mongocxx::v1::instance 唯一实例 + 按生效 URI 划分的 pool 注册表
// （上限 kMongoMaxPools）。client 经 MongoPoolLease 持有共享 pool；
// lease 内 instance 成员先于 pool 声明，保证 pool 必然先于 instance
// 销毁，不受注册表/静态对象析构顺序影响。
//
// 仅供 src/mongo/ 实现使用，允许出现 mongocxx 类型。

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include <mongocxx/v1/instance.hpp>
#include <mongocxx/v1/pool.hpp>
#include <mongocxx/v1/uri.hpp>

#include <bbt/infra/Result.hpp>

namespace bbt::infra::mongo_detail {

// 进程内 distinct 生效 URI 数上限：防止注册表无界增长。
inline constexpr std::size_t kMongoMaxPools = 8;

// pool lease：共享所有权的进程级 pool。成员顺序即析构顺序——
// pool 先于 instance 释放，满足 mongoc 清理约束。
struct MongoPoolLease {
    std::shared_ptr<mongocxx::v1::instance> instance;
    mongocxx::v1::pool                    pool;

    MongoPoolLease(std::shared_ptr<mongocxx::v1::instance> inst,
                   mongocxx::v1::uri                     u)
        : instance(std::move(inst)), pool(std::move(u)) {}
};

// 取生效 URI 对应的进程级共享 pool：首个 client 建立 instance+pool，
// 同 URI 后续 client 复用同一 lease；distinct URI 超过 kMongoMaxPools
// 返回 InvalidArgument。URI 语法错误或 instance 冲突映射为相应 Error。
result<std::shared_ptr<MongoPoolLease>> AcquireMongoPool(
    const std::string& effective_uri);

} // namespace bbt::infra::mongo_detail
