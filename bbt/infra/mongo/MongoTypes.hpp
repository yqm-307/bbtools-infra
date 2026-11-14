/**
 * @file MongoTypes.hpp
 * @brief Mongo 装配参数、集合目标与命令值类型。
 *
 * 归属模型：MongoRuntime 显式拥有连接配置、进程级 pool lease、有界
 * worker 组、接纳队列与关闭排空；集合句柄（CoMongoColl）只是
 * db.collection 目标值 + owner 引用，不新建线程。
 *   - 同一 owner 的 N 个集合句柄共享同一组 worker 与同一 max_queue
 *     接纳队列：worker 数不随句柄数增长，跨集合共享背压；
 *   - 不同 owner 各自持有 worker 组/队列/ops 表，资源相互隔离；
 *   - 每项 operation 在同一 worker 上 acquire/use/release pooled
 *     client；driver 同步调用不可强杀，deadline/close 只发布
 *     一次逻辑终态，物理收口由 worker 完成路径继续；
 *   - mongocxx/bsoncxx/pool/thread 细节全部隐藏于 bbt/infra/mongo/detail/ 实现，
 *     本头只出现 infra/标准库类型。
 */
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/infra/CloseOptions.hpp>
#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/mongo/Bson.hpp>

namespace bbt::infra::mongo {

/**
 * @brief worker 线程数上限。
 *
 * 与 kMongoLimitsMaxQueue、kMongoLimitsMaxTimeout 同属 mongo 装配参数的公共上限：
 * owner 与 CoMongoCli 共用同一组边界，逐项大于零且不超上限，违规返回 InvalidArgument。
 */
inline constexpr std::size_t kMongoLimitsMaxWorkerThreads = 8;

/**
 * @brief 接纳队列容量上限。
 */
inline constexpr std::size_t kMongoLimitsMaxQueue = 1000000;

/**
 * @brief 单个驱动层超时的上限（10 分钟）。
 */
inline constexpr std::chrono::milliseconds kMongoLimitsMaxTimeout{std::chrono::minutes{10}};

/**
 * @brief InsertMany 单次文档数上限。
 *
 * 多文档数据面的公共上限（一次性内存物化边界）：违规在接纳前返回 InvalidArgument，
 * 零接纳、什么都不发。
 *   - InsertMany 批量上限：文档数 ≤ 500 且提交的总 BSON 字节 ≤ 8 MiB（依据：MongoDB
 *     单条命令/文档的 16 MiB BSON 上限，预留约一半余量给命令封套与 insert 参数，
 *     防止单次调用物化出接近协议上限的命令）。
 *   - 单份 BSON 输入文档上限 kMongoLimitsMaxInputDocBytes：filter/update/sort/projection
 *     以及 InsertMany 列表中的单文档均不得超 8 MiB（同一命令输入预算，依据同上）。
 *     超限在 CollSubmit 之前拒绝，越界零接纳。
 *   - Find 上界：单次物化 ≤ 1000 行且返回文档总字节 ≤ 16 MiB（依据：一页数据的内存
 *     物化上限，杜绝无界游标物化 OOM；更大结果集走分页或流式，属已知 ceiling）。
 */
inline constexpr std::size_t kMongoLimitsMaxInsertDocs = 500;

/**
 * @brief InsertMany 单次提交的总 BSON 字节上限。
 */
inline constexpr std::size_t kMongoLimitsMaxInsertTotalBytes = 8u * 1024u * 1024u;

/**
 * @brief 单份输入文档（filter/update/sort/projection）的 BSON 字节上限。
 */
inline constexpr std::size_t kMongoLimitsMaxInputDocBytes = kMongoLimitsMaxInsertTotalBytes;

/**
 * @brief Find 单页行数上限。
 */
inline constexpr std::size_t kMongoLimitsMaxFindLimit = 1000;

/**
 * @brief Find 单次物化返回文档的总字节上限。
 */
inline constexpr std::size_t kMongoLimitsMaxFindResultBytes = 16u * 1024u * 1024u;

/**
 * @brief CoMongoDb 装配参数（owner 级资源预算）。
 *
 * 一个 owner 绑定一组 worker、一条接纳队列与一个生效 URI 的 pool lease；
 * database/collection 不属于 owner，由集合句柄携带。
 */
struct MongoRuntimeConfig {

    /**
     * @brief mongodb:// 或 mongodb+srv:// 连接串，可能含凭据。
     */
    std::string uri;

    /**
     * @brief 执行阻塞 driver 调用的 worker 线程数上限（<=8；测试默认 <=2）。
     *
     * 每项 operation 在同一 worker 上 acquire/use/release pooled client。
     */
    std::size_t worker_threads;

    /**
     * @brief 等待 worker 的 operation 队列容量，耗尽时新命令立即返回 Overloaded，不无限排队；这是同一 owner 全部集合句柄共享的背压上限。
     */
    std::size_t max_queue;

    /**
     * @brief 驱动层服务器选择阻塞上界，语义同 MongoClientConfig。
     */
    std::chrono::milliseconds server_selection_timeout;

    /**
     * @brief 驱动层连接阻塞上界。
     */
    std::chrono::milliseconds connect_timeout;

    /**
     * @brief 驱动层 socket 阻塞上界。
     */
    std::chrono::milliseconds socket_timeout;

    /**
     * @brief pool 等待队列上界。
     */
    std::chrono::milliseconds wait_queue_timeout;
};

/**
 * @brief 校验 owner 装配参数：URI 前缀、worker_threads/max_queue 范围与各超时上限。
 * @param cfg 待校验的装配参数。
 * @return 通过返回 ok；违规返回 InvalidArgument。
 */
result<void> ValidateMongoRuntimeConfig(const MongoRuntimeConfig& cfg);

/**
 * @brief 一次命令的目标集合：owner 不关心 database/collection，句柄只携带目标值。
 *
 * database/collection 语法校验仍在提交时进行，非法输入返回 InvalidArgument。
 */
struct MongoTarget {

    /**
     * @brief 数据库名，不能为空。
     */
    std::string database;

    /**
     * @brief 集合名，不能为空。
     */
    std::string collection;
};

/**
 * @brief 校验目标集合：database 与 collection 均不得为空。
 * @param t 待校验的目标。
 * @return 通过返回 ok；空数据库名或空集合名返回 InvalidArgument。
 */
result<void> ValidateMongoTarget(const MongoTarget& t);

/**
 * @brief 多文档 Find 的谓词选项。
 *
 * limit **必填**（调用方必须显式给出），杜绝无界游标物化——limit==0 或
 * > kMongoLimitsMaxFindLimit 都在接纳前返回 InvalidArgument；skip 为分页偏移（0 起），
 * > INT64_MAX 无法交给 driver，同样在接纳前 InvalidArgument（不另设分页上限——driver 的
 * int64 表示范围即边界）。sort/projection 为可选 BSON 文档（MongoDocument），bytes 为空
 * 视为未设置；filter/sort/projection 任一字节数超 kMongoLimitsMaxInputDocBytes 也在
 * 接纳前 InvalidArgument。
 *
 * 返回侧另有一道硬上限：命中文档累计 BSON 字节超 kMongoLimitsMaxFindResultBytes 时返回
 * 明确错误（RemoteError，domain_code="result_too_large"）而不是 OOM。
 */
struct FindOptions {

    /**
     * @brief 必填：每页行数上限。
     */
    std::size_t limit = 0;

    /**
     * @brief 分页偏移。
     */
    std::size_t skip = 0;

    /**
     * @brief 可选：BSON sort 文档，bytes 为空视为未设置。
     */
    MongoDocument sort{};

    /**
     * @brief 可选：BSON projection 文档，bytes 为空视为未设置。
     */
    MongoDocument projection{};
};

/**
 * @brief InsertMany 的回执：有序批量插入的服务端结果。
 *
 * 部分成功契约（调用方据此观察）：
 *   - 全部成功 → ok(result)，inserted == docs.size()；
 *   - 有序插入中途失败（如重复 _id）→ err(RemoteError)，此前已成功的文档
 *     **已保留**在服务端；调用方通过错误通道观察部分成功：
 *     Error.details["inserted_count"] 携带服务端回执的已写入数（来自 raw reply 的
 *     nInserted/n，或 ordered 写入时首个 writeErrors.index，三键任一可用时给出；服务端
 *     均未给出时该键缺省），Error.details["requested_count"] 携带请求总数，
 *     Error.details["failed_index"] 携带首个失败下标（服务端给出时）；
 *     domain_code/backend_code 仍承载服务端 codeName/code。
 */
struct MongoInsertManyResult {

    /**
     * @brief 服务端回执的已写入文档数。
     */
    std::uint64_t inserted = 0;
};

// clang-format off
}
// clang-format on
