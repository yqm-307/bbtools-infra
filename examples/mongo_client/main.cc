// Mongo 公开入口的最小真实消费示例（Mongo 适配迁移）。
//
// 消费面：只 include 公开头 <bbt/infra/mongo/Bson.hpp> /
// <bbt/infra/mongo/Client.hpp> / <bbt/infra/Result.hpp>，只链接公开 target
// bbt::infra_mongo。不引用 src/mongo 内部头，不出现 mongocxx/bsoncxx 类型。
//
// 真实后端：读取 BBT_TEST_MONGO_URI（例如 tests/mongo-live/run.sh 注入的
// mongodb://127.0.0.1:<port>/）。未设置时打印 "skip:" 并返回 77（ctest 记为
// Skipped，绝不冒充已运行）。用例只写自己的唯一 _id，结束前删除并用 FindOne
// 读回未命中确认清理；插入后任何失败都先按同一 _id 补偿清理再抛出，失败路径
// 同样不留残留，且清理问题与原错误一并上报。
//
// 驱动方式：命令只能在协程上下文调用；写了 bbt_co_await 的表达式在协程外会先
// 撞 AwaitHelper 的 std::logic_error（域检查先于函数体，漏写 await 则不被静态
// 拦住），故全部调用都放在 Scheduler 的协程任务里，宿主线程等待其完成后再
// Close owner。

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/syntax/Async.hpp>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mongo/Bson.hpp>
#include <bbt/infra/mongo/Client.hpp>

#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <future>
#include <iostream>
#include <optional>
#include <stdexcept>
#include <string>
#include <unistd.h>

using namespace bbt::infra;
using namespace std::chrono_literals;

namespace {

CallOptions Budget(int ms) {
    CallOptions options;
    options.deadline = std::chrono::steady_clock::now() +
                       std::chrono::milliseconds{ms};
    return options;
}

const char* Uri() {
    const char* value = std::getenv("BBT_TEST_MONGO_URI");
    return value != nullptr && value[0] != '\0' ? value : nullptr;
}

void Require(bool condition, const std::string& message) {
    if (!condition)
        throw std::runtime_error(message);
}

template <class T>
void RequireOk(const result<T>& value, const char* stage) {
    if (!value)
        throw std::runtime_error(std::string(stage) + ": " +
                                 value.error().message);
}

// 插入成功后的补偿清理：按本次唯一 _id 删除并读回未命中。返回空串 = 删除与
// 读回都确认无残留；否则是清理问题描述（包括「读回仍能读到」）。删除计数为 0
// 不算问题（重复清理或已被删都正常），但删除本身报错要带给调用方。只在协程内
// 调用：不进析构、不在 owner 关闭之后 await，也不宽范围删集合。
bbt_co_async(std::string) CleanupInserted(mongo::CoMongoColl& coll,
                                          const mongo::MongoDocument& filter) {
    std::string problems;
    const auto removed = bbt_co_await coll.DeleteOne(filter, Budget(5000));
    const auto miss    = bbt_co_await coll.FindOne(filter, Budget(5000));

    if (!miss)
        problems = "FindOne(after cleanup) failed: " + miss.error().message;
    else if (miss.value().has_value())
        problems = "residual document for this _id is still readable";
    if (!removed) {
        if (!problems.empty())
            problems += "; ";
        problems += "DeleteOne failed: " + removed.error().message;
    }
    return problems;
}

// 一次完整 CRUD 闭环：构造 → 插入 → 读回（类型化 getter + canonical JSON）
// → 删除 → 读回未命中。
void Crud(mongo::CoMongoColl& coll, const std::string& id) {
    const std::string where = "{\"_id\":\"" + id + "\"}";

    // 构造：JSON → BSON，不再手写字节数组。
    auto doc = mongo::MongoDocument::FromJson(
        "{\"_id\":\"" + id +
        "\",\"n\":7,\"ratio\":1.5,\"ok\":true,\"tag\":\"hello\"}");
    Require(static_cast<bool>(doc), "FromJson(document) failed");
    auto filter = mongo::MongoDocument::FromJson(where);
    Require(static_cast<bool>(filter), "FromJson(filter) failed");

    // 写：新入口的等待方法经 bbt_co_await 消费。返回错误不代表未落库（如
    // writeConcern 错误时写入可能已完成），故同样纳入下方精确 _id 补偿路径。
    // 此后任何失败都必须先按精确 _id 补偿清理，再带原错误抛出——真实服务的
    // 失败路径同样不得遗留本轮测试数据。
    try {
        RequireOk(bbt_co_await coll.InsertOne(doc.value(), Budget(5000)),
                  "InsertOne");

        // 读：命中返回文档；未命中是 ok(nullopt)，与错误分支区分。
        auto found = bbt_co_await coll.FindOne(filter.value(), Budget(5000));
        RequireOk(found, "FindOne");
        Require(found.value().has_value(), "FindOne: 插入的文档未命中");
        const mongo::MongoDocument& got = *found.value();

        // 读取只经公开面：类型化 getter + Extended JSON（无 bsoncxx 类型）。
        const auto n     = got.GetInt64("n");
        const auto ratio = got.GetDouble("ratio");
        const auto ok    = got.GetBool("ok");
        const auto tag   = got.GetString("tag");
        const auto json  = got.ToJson();
        RequireOk(n, "GetInt64(n)");
        RequireOk(ratio, "GetDouble(ratio)");
        RequireOk(ok, "GetBool(ok)");
        RequireOk(tag, "GetString(tag)");
        RequireOk(json, "ToJson");
        Require(got.Has("_id"), "Has(_id)");

        // 删除并读回：清理属于本示例自己写入的唯一 _id。
        auto removed = bbt_co_await coll.DeleteOne(filter.value(), Budget(5000));
        RequireOk(removed, "DeleteOne");
        auto miss = bbt_co_await coll.FindOne(filter.value(), Budget(5000));
        RequireOk(miss, "FindOne(after delete)");

        // 值断言放在清理之后：失败不会留下测试文档。
        Require(n.value() == 7, "n != 7");
        Require(ratio.value() == 1.5, "ratio != 1.5");
        Require(ok.value(), "ok != true");
        Require(tag.value() == "hello", "tag != hello");
        Require(removed.value() == 1u, "DeleteOne 删除数量 != 1");
        Require(!miss.value().has_value(), "删除后仍能读到文档");
        // canonical Extended JSON 保留 int32/double 类型标记。
        Require(json.value().find("$numberInt") != std::string::npos,
                "canonical JSON 缺少 $numberInt");
        Require(json.value().find("$numberDouble") != std::string::npos,
                "canonical JSON 缺少 $numberDouble");

        std::cout << "mongo_client_example: n=" << n.value()
                  << " ratio=" << ratio.value()
                  << " tag=" << tag.value()
                  << " deleted=" << removed.value()
                  << " miss_after_delete=1\n"
                  << "canonical=" << json.value() << "\n";
    } catch (const std::exception& e) {
        // 补偿清理仍在同一协程内、owner 关闭之前完成；原错误在前，清理问题随后。
        const std::string cleanup =
            bbt_co_await CleanupInserted(coll, filter.value());
        throw std::runtime_error(
            std::string(e.what()) + " | cleanup: " +
            (cleanup.empty() ? "_id=" + id + " 已删除且读回未命中"
                             : cleanup));
    }
}

int Run() {
    const char* uri = Uri();
    if (uri == nullptr) {
        std::cout << "skip: 需要 BBT_TEST_MONGO_URI（真实 MongoDB）\n";
        return 77;
    }

    mongo::MongoRuntimeConfig cfg;
    cfg.uri                      = uri;
    cfg.worker_threads           = 1;
    cfg.max_queue                = 16;
    cfg.server_selection_timeout = std::chrono::milliseconds{5000};
    cfg.connect_timeout          = std::chrono::milliseconds{3000};
    cfg.socket_timeout           = std::chrono::milliseconds{3000};
    cfg.wait_queue_timeout       = std::chrono::milliseconds{3000};

    auto valid = mongo::ValidateMongoRuntimeConfig(cfg);
    RequireOk(valid, "ValidateMongoRuntimeConfig");

    auto created = mongo::CoMongoDb::Create(cfg);
    RequireOk(created, "CoMongoDb::Create");
    auto db = std::move(created).value();
    RequireOk(db->Start(), "CoMongoDb::Start");

    auto handle = db->Collection({"bbt_example", "items"});
    if (!handle) {
        db->Close();
        throw std::runtime_error("Collection: " + handle.error().message);
    }
    auto coll = std::move(handle).value();
    const std::string id =
        "bbt:example:mongo-client:" + std::to_string(::getpid());

    // 协程内执行；结果/异常经 promise 交回宿主线程后再收口 owner。
    auto completion = std::make_shared<std::promise<void>>();
    auto done       = completion->get_future();
    bool registered = false;
    g_scheduler->RegistCoroutineTask(
        [coll, completion, id] {
            try {
                Crud(*coll, id);
                completion->set_value();
            } catch (...) {
                completion->set_exception(std::current_exception());
            }
        },
        registered);
    int code = 0;
    if (!registered) {
        std::cerr << "coroutine task registration failed\n";
        code = 1;
    } else if (done.wait_for(30s) != std::future_status::ready) {
        std::cerr << "mongo example timeout\n";
        code = 1;
    } else {
        try {
            done.get();
            std::cout << "mongo_client_example: CRUD ok\n";
        } catch (const std::exception& e) {
            std::cerr << "mongo_client_example: " << e.what() << "\n";
            code = 1;
        }
    }

    // 句柄门禁只封本句柄；owner Close 同步收口（返回即 ops 清空 + worker 全退）。
    coll->Close();
    db->Close();
    return code;
}

} // namespace

int main() {
    g_bbt_coroutine_config->m_cfg_static_thread_num = 1;
    g_bbt_coroutine_config->m_cfg_stack_size        = 256 * 1024;
    g_scheduler->Start();   // runtime 进程寿命：整个示例只 Start 一次。
    const int code = Run();
    std::cout.flush();
    std::cerr.flush();
    std::_Exit(code);       // Scheduler 无停机入口，进程结束即退出
}
