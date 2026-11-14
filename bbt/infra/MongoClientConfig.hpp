/**
 * @file MongoClientConfig.hpp
 * @brief MongoDB 单集合客户端的装配参数与校验声明。
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <string>

#include <bbt/infra/Result.hpp>

namespace bbt::infra {

/**
 * @brief CoMongoCli 的装配参数：一个 client 绑定一个 db.collection。
 *
 * 内部由独占的 mongo owner（bbt/infra/mongo/detail/MongoRuntime）承载 worker、队列与 pool，
 * 资源归属与 bbt::infra::mongo::CoMongoDb 相同。需要多个集合共享一组 worker 的代码应直接使用
 * mongo::CoMongoDb 加集合句柄。
 *
 * socket_timeout 是单操作的物理上界（driver 阻塞 I/O 不会超过它），FindOne 另以 maxTimeMS 下发同一上界。
 * URI 中已显式给出的同名项不会被覆盖，调用方须自行保证这些值有限。
 */
struct MongoClientConfig {

    /**
     * @brief mongodb:// 或 mongodb+srv:// 连接串，可能含凭据。
     */
    std::string uri;

    /**
     * @brief 数据库名，不能为空。
     */
    std::string database;

    /**
     * @brief 集合名，不能为空。
     */
    std::string collection;

    /**
     * @brief 执行阻塞 driver 调用的 worker 线程数，范围 (0, kMongoLimitsMaxWorkerThreads]。
     *
     * 每项 operation 在同一 worker 上 acquire/use/release pooled client。
     */
    std::size_t worker_threads;

    /**
     * @brief 等待 worker 的 operation 队列容量，范围 (0, kMongoLimitsMaxQueue]。耗尽时新命令立即返回 Overloaded，不无限排队。
     */
    std::size_t max_queue;

    /**
     * @brief 驱动服务器选择超时，注入 serverSelectionTimeoutMS。
     */
    std::chrono::milliseconds server_selection_timeout;

    /**
     * @brief 连接超时，注入 connectTimeoutMS。
     */
    std::chrono::milliseconds connect_timeout;

    /**
     * @brief 单操作物理上界，注入 socketTimeoutMS。
     */
    std::chrono::milliseconds socket_timeout;

    /**
     * @brief pool 等待队列超时，注入 waitQueueTimeoutMS；超时映射为 Overloaded。
     */
    std::chrono::milliseconds wait_queue_timeout;
};

/**
 * @brief 校验 MongoClientConfig：URI 前缀、库名与集合名非空，各数值与超时位于对应上限之内。
 * @param cfg 待校验的装配参数。
 * @return 通过返回 ok；违规返回 InvalidArgument。
 */
result<void> ValidateMongoClientConfig(const MongoClientConfig& cfg);

// clang-format off
}
// clang-format on
