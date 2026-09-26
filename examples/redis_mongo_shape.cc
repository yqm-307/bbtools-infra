// Issue #27：Redis/Mongo 调用形态示例（静态形态演示）。
//
// 本文件演示 CoRedisCli / CoMongoCli 的真实公开 API 调用形态：装配校验 →
// Scheduler 内 Create/Start → 协程内命令（deadline/cancel）→ 关闭边界。
// 它只做参数校验与错误路径（不连接真实服务），因此无外部 Redis/Mongo
// 依赖即可编译运行；真实服务调用形态与语义见 README「示例与兼容说明」。
//
// 注意：这两个客户端的链接依赖 hiredis / mongocxx（经显式前缀接入）。
// 本示例 target 仅在对应模块 target 存在时注册（见 examples/CMakeLists.txt）。

#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <bbt/core/thread/Lock.hpp>
#include <bbt/coroutine/detail/Define.hpp>
#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>
#include <bbt/infra/CoMongoCli.hpp>
#include <bbt/infra/CoRedisCli.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/mongo/Client.hpp>

using namespace bbt::infra;

namespace {

// 协程内限时执行；等待型 API 只在协程上下文调用。
template <class F>
bool RunInCoroutine(F&& fn, int budget_ms = 15000) {
    bbt::core::thread::CountDownLatch done{1};
    bool succ = false;
    bbt::coroutine::detail::Scheduler::GetInstance()->RegistCoroutineTask(
        [fn = std::forward<F>(fn), &done]() mutable {
            fn();
            done.Down();
        },
        succ);
    if (!succ)
        return false;
    return done.WaitTimeout(budget_ms) == 0;
}

CallOptions Budget(int ms) {
    CallOptions options;
    options.deadline = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds{ms};
    return options;
}

// Redis 形态：PING → SET → GET → DEL → 关闭。未连接真实 Redis 时，
// 首个命令在连接建立前等待并按 deadline 返回 Error（本示例打印错误码）。
void RedisShape() {
    RedisClientConfig cfg;
    cfg.host         = "127.0.0.1";
    cfg.port         = 6379;
    cfg.max_inflight = 16;
    cfg.max_queue    = 64;
    if (!ValidateRedisClientConfig(cfg)) {
        std::cerr << "redis config invalid\n";
        return;
    }

    auto created = CoRedisCli::Create(cfg);
    if (!created) {
        std::cerr << "CoRedisCli::Create failed: "
                  << created.error().message << "\n";
        return;
    }
    auto redis = std::move(created).value();
    if (!redis->Start()) {
        std::cerr << "CoRedisCli::Start failed\n";
        return;
    }

    auto ping = redis->Ping(Budget(200));
    if (!ping)
        std::cerr << "redis Ping error (expected without live redis): code="
                  << static_cast<int>(ping.error().code) << "\n";

    auto set = redis->Set("bbt:example", "v1", Budget(200));
    if (!set)
        std::cerr << "redis Set error: code="
                  << static_cast<int>(set.error().code) << "\n";

    auto get = redis->Get("bbt:example", Budget(200));
    if (!get)
        std::cerr << "redis Get error: code="
                  << static_cast<int>(get.error().code) << "\n";
    else if (!get.value())
        std::cout << "redis Get miss (nullopt, distinct from error)\n";

    auto del = redis->Delete({"bbt:example"}, Budget(200));
    if (!del)
        std::cerr << "redis Delete error: code="
                  << static_cast<int>(del.error().code) << "\n";

    redis->RequestClose();
    redis->WaitClosed(std::chrono::steady_clock::now() +
                          std::chrono::seconds{5},
                      {});
}

// Mongo 形态（Issue #40 owner + 集合句柄）：一个 CoMongoDb owner 承载
// worker 组/接纳队列/pool lease，多个集合句柄共享同一组执行资源。
// InsertOne → FindOne → 关闭。MongoDocument.bytes 是一份完整良构
// BSON；这里用最小合法文档 {"x":1}（BSON：int32 len + 0x10 int32
// tag + "x\0" + int32 value + 0x00）。未连接真实 MongoDB 时按驱动
// 超时上界返回 Error(Unavailable)。
void MongoShape() {
    mongo::MongoRuntimeConfig cfg;
    cfg.uri                      = "mongodb://127.0.0.1:27017";
    cfg.worker_threads           = 1;
    cfg.max_queue                = 64;
    cfg.server_selection_timeout = std::chrono::milliseconds{200};
    cfg.connect_timeout          = std::chrono::milliseconds{200};
    cfg.socket_timeout           = std::chrono::milliseconds{200};
    cfg.wait_queue_timeout       = std::chrono::milliseconds{200};
    if (!mongo::ValidateMongoRuntimeConfig(cfg)) {
        std::cerr << "mongo runtime config invalid\n";
        return;
    }

    auto created = mongo::CoMongoDb::Create(cfg);
    if (!created) {
        std::cerr << "CoMongoDb::Create failed: "
                  << created.error().message << "\n";
        return;
    }
    auto db = std::move(created).value();
    if (!db->Start()) {
        std::cerr << "CoMongoDb::Start failed\n";
        return;
    }

    // 同一 owner 上的两个集合句柄：共享同一 worker 组与接纳队列，
    // 不各自建线程。
    auto items = db->Collection({"bbt_example", "items"});
    auto logs  = db->Collection({"bbt_example", "logs"});
    if (!items || !logs) {
        std::cerr << "Collection() failed\n";
        return;
    }

    MongoDocument doc;
    doc.bytes = {0x0c, 0x00, 0x00, 0x00,  // 总长 12
                 0x10, 'x',  0x00,        // int32 元素 "x"
                 0x01, 0x00, 0x00, 0x00,  // 值 = 1
                 0x00};                   // 结尾
    auto ins = items.value()->InsertOne(doc, Budget(1000));
    if (!ins)
        std::cerr << "mongo InsertOne error (expected without live mongo): code="
                  << static_cast<int>(ins.error().code) << "\n";

    auto find = logs.value()->FindOne(doc, Budget(1000));
    if (!find)
        std::cerr << "mongo FindOne error: code="
                  << static_cast<int>(find.error().code) << "\n";
    else if (!find.value())
        std::cout << "mongo FindOne miss (nullopt, distinct from error)\n";

    // 句柄关闭只影响自身接纳，不关闭兄弟/owner；owner 关闭负责物理
    // drain。
    items.value()->RequestClose();
    logs.value()->RequestClose();
    db->RequestClose();
    db->WaitClosed(std::chrono::steady_clock::now() +
                       std::chrono::seconds{5},
                   {});
}

} // namespace

int main() {
    auto* cfg = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    cfg->m_cfg_static_thread_num = 2;
    cfg->m_cfg_stack_size        = 1024 * 256;

    auto* scheduler = bbt::coroutine::detail::Scheduler::GetInstance().get();
    scheduler->Start(bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);

    // 命令只能在协程上下文调用，否则返回 Error(InvalidContext)。
    const bool ran = RunInCoroutine([&] {
        RedisShape();
        MongoShape();
    });
    scheduler->Stop();
    return ran ? 0 : 1;
}
