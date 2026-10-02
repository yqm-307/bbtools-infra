#pragma once
// co-mongo/v1（bbtools-infra#7）：协程原生 MongoDB 客户端公共面。
// mongocxx/bsoncxx 类型、驱动线程、连接池与阻塞调用全部隐藏于
// src/mongo/ 实现，本头只出现 infra/标准库类型。

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra {

// MongoClientConfig 各项显式、大于零且不超下列上限；违规返回 InvalidArgument。
inline constexpr std::size_t kMongoLimitsMaxWorkerThreads = 8;
inline constexpr std::size_t kMongoLimitsMaxQueue         = 1000000;
inline constexpr std::chrono::milliseconds kMongoLimitsMaxTimeout{
    std::chrono::minutes{10}};

// BSON 文档的 owning 字节载体：bytes 为一份完整 BSON 文档（首部 int32
// 长度 + 元素序列 + 结尾 0x00），二进制安全。合法性（结构良构）在提交
// 时校验，非法输入返回 InvalidArgument，不会进入 driver。
struct MongoDocument {
    std::vector<std::uint8_t> bytes;
};

// UpdateOne 的写回执（acknowledged write concern 下由驱动给出）。
struct MongoUpdateResult {
    std::int64_t matched  = 0;
    std::int64_t modified = 0;
    std::int64_t upserted = 0;
};

// CoMongoCli 装配参数（Issue #7 旧契约，保留兼容）。本切片一个
// client 绑定一个 db.collection，内部由独占的 mongo owner
// （src/mongo/MongoRuntime）承载 worker/队列/pool——资源归属与
// Issue #40 的 bbt::infra::mongo::CoMongoDb 同构；需要多集合共享
// 一组 worker 的新代码应直接用 mongo::CoMongoDb + 集合句柄。
//   - worker_threads：执行阻塞 driver 调用的 worker 线程数上限
//     （<=8；测试默认 <=2）；每项 operation 在同一 worker 上
//     acquire/use/release pooled client；
//   - max_queue：等待 worker 的 operation 队列容量，耗尽时新命令立即
//     返回 Overloaded，不无限排队；
//   - server_selection_timeout/connect_timeout/socket_timeout/
//     wait_queue_timeout：驱动层阻塞上界，分别注入
//     serverSelectionTimeoutMS/connectTimeoutMS/socketTimeoutMS/
//     waitQueueTimeoutMS。socket_timeout 是单操作物理上界（driver 阻塞
//     I/O 不会超过它），FindOne 另以 maxTimeMS 形式下发同一上界。
//     URI 中已显式给出的同名项不被覆盖（调用方须自行保证有限值）。
struct MongoClientConfig {
    std::string               uri;
    std::string               database;
    std::string               collection;
    std::size_t               worker_threads;
    std::size_t               max_queue;
    std::chrono::milliseconds server_selection_timeout;
    std::chrono::milliseconds connect_timeout;
    std::chrono::milliseconds socket_timeout;
    std::chrono::milliseconds wait_queue_timeout;
};

inline result<void> ValidateMongoClientConfig(const MongoClientConfig& cfg) {
    if (cfg.uri.empty() ||
        (cfg.uri.rfind("mongodb://", 0) != 0 &&
         cfg.uri.rfind("mongodb+srv://", 0) != 0))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo client config: uri must start with mongodb:// or mongodb+srv://"));
    if (cfg.database.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo client config: empty database"));
    if (cfg.collection.empty())
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo client config: empty collection"));
    if (cfg.worker_threads == 0 ||
        cfg.worker_threads > kMongoLimitsMaxWorkerThreads)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo client config: worker_threads out of range"));
    if (cfg.max_queue == 0 || cfg.max_queue > kMongoLimitsMaxQueue)
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo client config: max_queue out of range"));
    const auto valid_timeout = [](std::chrono::milliseconds t) {
        return t > std::chrono::milliseconds{0} &&
               t <= kMongoLimitsMaxTimeout;
    };
    if (!valid_timeout(cfg.server_selection_timeout))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo client config: server_selection_timeout out of range"));
    if (!valid_timeout(cfg.connect_timeout))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo client config: connect_timeout out of range"));
    if (!valid_timeout(cfg.socket_timeout))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo client config: socket_timeout out of range"));
    if (!valid_timeout(cfg.wait_queue_timeout))
        return result<void>::err(MakeError(ErrorCode::InvalidArgument,
            "mongo client config: wait_queue_timeout out of range"));
    return result<void>::ok();
}

// CoMongoCli：出站 MongoDB 组件（Issue #7 旧契约，兼容保留；新代码
// 优先用 bbt::infra::mongo::CoMongoDb + CoMongoColl，见
// include/bbt/infra/mongo/Client.hpp）。实现为「独占 mongo owner +
// 单个集合句柄」：mongocxx 同步 driver + 有界 worker bridge，
// mongocxx::client 单线程亲和由「同一 worker 上 acquire/use/
// release」满足；进程级 mongocxx::pool 按生效 URI 共享。
//
// 命令语义边界：
//   - 全部命令只能在协程上下文调用，否则返回 Error(InvalidContext)；
//   - FindOne 未命中返回 ok 的 std::nullopt，与错误分支区分；
//   - 服务端错误回复（duplicate key 等）映射为 Error(RemoteError)，
//     domain_code 为服务端 codeName（如 "DuplicateKey"），backend_code
//     为原始服务端错误码；driver 侧失败（server selection/socket/stream/
//     pool）映射为 Error(Unavailable)，无效 BSON/参数为
//     InvalidArgument，pool wait queue 超时为 Overloaded；
//   - 已派发（进入 driver）的调用不可强杀，继续到 driver timeout 或完成
//     ——operation state、client lease 与 payload 保活到物理收口；
//     每个请求只向业务交付一次终态，迟到 driver 结果只消费不交付。
//   - 关闭由 owner 主动发起：Close() 幂等、任意线程可调用，返回即接纳
//     封口、挂起等待者已收到终态、在途计数已在有界窗口内归零（或超过
//     上限后由 worker 完成路径补跑物理落定）。Close 与在途 Start 并发时
//     先封口、再等该次 Start 完成：已被封口的 Start 不发布资源并返回
//     Closed，否则其已建资源由本次 Close 同步收口。因此 Close 返回当刻
//     本 client 的 worker/lease 已收口、终态不回退，此后 Start 一律 Closed。
class CoMongoCli : public ICoNetwork, public ICoCloseable {
public:
    // Create/Start 在启动控制线程使用，不挂起协程。
    // Create 只校验装配参数并取对象身份；Start 要求协程运行时已初始化
    // （未初始化返回 Error(RuntimeUnavailable)），否则返回对应错误。
    static result<std::shared_ptr<CoMongoCli>> Create(MongoClientConfig config);

    // 同一 client 只成功启动一次；关闭后不重开。
    // Start 建立 worker 线程组并接入进程级 pool；与 Close 并发时封口优先，
    // Start 不发布资源并返回 Error(Closed)。
    virtual result<void> Start() = 0;

    // 插入一份文档；bytes 必须是完整良构 BSON，非法 → InvalidArgument。
    // duplicate key 等服务端错误 → RemoteError(domain_code="DuplicateKey")。
    virtual result<void> InsertOne(const MongoDocument& doc,
                                   const CallOptions& options) = 0;

    // filter 为查询谓词文档；命中返回文档字节拷贝，未命中返回 std::nullopt。
    virtual result<std::optional<MongoDocument>> FindOne(
        const MongoDocument& filter, const CallOptions& options) = 0;

    // filter 为查询谓词文档，update 为更新文档（含 $set 等更新算子）；
    // 返回 matched/modified/upserted 计数。
    virtual result<MongoUpdateResult> UpdateOne(
        const MongoDocument& filter, const MongoDocument& update,
        const CallOptions& options) = 0;

    // 删除首个命中 filter 的文档；返回实际删除数量（0 或 1）。
    virtual result<std::uint64_t> DeleteOne(const MongoDocument& filter,
                                            const CallOptions& options) = 0;
};

} // namespace bbt::infra
