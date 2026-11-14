/**
 * @file CoMongoCli.hpp
 * @brief 协程原生 MongoDB 客户端的公共面。
 *
 * mongocxx/bsoncxx 类型、驱动线程、连接池与阻塞调用全部隐藏在 bbt/infra/mongo/detail/ 实现中，
 * 本头只出现 infra 与标准库类型。客户端为具体类，独占私有实现，不提供 Mongo 操作的派生扩展点。
 */
#pragma once

#include <cstdint>

#include <memory>

#include <optional>

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/MongoClientConfig.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/mongo/CoMongoDb.hpp>

namespace bbt::infra {

namespace mongo_detail {
class CoMongoCliImpl;
}

/**
 * @brief 文档值类型；唯一定义在 bbt/infra/mongo/Bson.hpp（namespace bbt::infra::mongo），此处保持 bbt::infra:: 下的名字可用。
 */
using mongo::MongoDocument;

/**
 * @brief 更新回执类型；唯一定义在 bbt/infra/mongo/Bson.hpp。
 */
using mongo::MongoUpdateResult;

/**
 * @brief 接纳队列容量上限，取值定义见 bbt/infra/mongo/CoMongoDb.hpp。
 */
using mongo::kMongoLimitsMaxQueue;

/**
 * @brief 驱动层超时上限，取值定义见 bbt/infra/mongo/CoMongoDb.hpp。
 */
using mongo::kMongoLimitsMaxTimeout;

/**
 * @brief worker 线程数上限，取值定义见 bbt/infra/mongo/CoMongoDb.hpp。
 */
using mongo::kMongoLimitsMaxWorkerThreads;

/**
 * @brief 出站 MongoDB 组件：独占一个 mongo owner 与单个集合句柄。
 *
 * 实现为 mongocxx 同步 driver 加有界 worker bridge。mongocxx::client 的线程亲和由
 * 「同一 worker 上 acquire/use/release」满足；进程级 mongocxx::pool 按生效 URI 共享。
 *
 * 命令语义边界：
 *   - 全部命令只能在协程上下文调用，否则返回 Error(InvalidContext)；
 *   - FindOne 未命中返回 ok 的 std::nullopt，与错误分支区分；
 *   - 服务端错误回复（如 duplicate key）映射为对应的服务端错误分类：domain_code 为服务端 codeName
 *     （如 "DuplicateKey"），backend_code 为原始服务端错误码；driver 侧失败（server selection、
 *     socket、stream、pool）映射为 Unavailable，无效 BSON 或参数为 InvalidArgument，
 *     pool wait queue 超时为 Overloaded；
 *   - 已派发（进入 driver）的调用不可强杀，继续到 driver timeout 或完成：operation state、
 *     client lease 与 payload 保活到物理收口；每个请求只向业务交付一次终态，迟到的 driver 结果只消费不交付。
 *   - 关闭由 owner 主动发起：Close() 幂等、任意线程可调用，返回即接纳封口、挂起等待者已收到终态、
 *     在途计数已在有界窗口内归零（超过上限后由 worker 完成路径补跑物理落定）。Close 与在途 Start
 *     并发时先封口、再等该次 Start 完成：已封口的 Start 不发布资源并返回 Closed，否则其已建资源由
 *     本次 Close 同步收口。因此 Close 返回时 worker 与 lease 已收口、终态不回退，此后 Start 一律 Closed。
 */
class CoMongoCli final : public ICoNetwork, public ICoCloseable {
public:
    /**
     * @brief 校验装配参数并取对象身份，不建立任何资源。
     *
     * 在启动控制线程使用，不挂起协程。资源的建立由 Start 完成。
     * @param config 装配参数，校验规则见 ValidateMongoClientConfig。
     * @return 成功返回 client 句柄；参数非法返回 InvalidArgument。
     */
    static result<std::shared_ptr<CoMongoCli>> Create(MongoClientConfig config);

    /**
     * @brief 释放独占实现，由实现完成既有关闭流程。
     */
    ~CoMongoCli() override;

    CoMongoCli(const CoMongoCli&) = delete;
    CoMongoCli& operator=(const CoMongoCli&) = delete;

    /**
     * @brief 建立 worker 线程组并接入进程级 pool。同一 client 只成功启动一次，关闭后不重开。
     *
     * 要求协程运行时已初始化，未初始化返回 RuntimeUnavailable。与 Close 并发时封口优先：
     * Start 不发布资源并返回 Closed。
     */
    result<void> Start();

    /**
     * @brief 插入一份文档。
     *
     * 文档字节必须是完整良构的 BSON，非法返回 InvalidArgument。服务端错误（如 duplicate key）
     * 映射为服务端错误分类，domain_code 为 "DuplicateKey" 等 codeName。
     */
    result<void> InsertOne(const MongoDocument& doc, const CallOptions& options);

    /**
     * @brief 查询首个命中 filter 的文档。
     *
     * filter 为查询谓词文档。命中返回文档字节拷贝，未命中返回 std::nullopt（ok 分支）。
     */
    result<std::optional<MongoDocument>> FindOne(const MongoDocument& filter, const CallOptions& options);

    /**
     * @brief 更新首个命中 filter 的文档。
     *
     * update 为更新文档，须包含 $set 等更新算子。返回 matched、modified、upserted 计数。
     */
    result<MongoUpdateResult>
    UpdateOne(const MongoDocument& filter, const MongoDocument& update, const CallOptions& options);

    /**
     * @brief 删除首个命中 filter 的文档。
     * @return 实际删除的数量，0 或 1。
     */
    result<std::uint64_t> DeleteOne(const MongoDocument& filter, const CallOptions& options);

    /**
     * @brief 按既有 owner 关闭协议封口并收口资源。
     */
    void Close() noexcept override;

    /**
     * @brief 查询实现的物理收口状态。
     */
    bool IsClosed() const noexcept override;

    /**
     * @brief 返回创建时取得的对象身份。
     */
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override;

private:
    friend class mongo_detail::CoMongoCliImpl;

    explicit CoMongoCli(MongoClientConfig config, bbt::coroutine::CoObjectInfo info);
    std::unique_ptr<mongo_detail::CoMongoCliImpl> m_impl;
};

// clang-format off
}
// clang-format on
