// Mongo 新入口 async/await 正负编译门禁（与 tests/redis_net 同规约）：
//   - 正例：4 个 CRUD 等待入口经 bbt_co_await 消费，必须可编译；
//   - 负例（-DBBT_NEGATIVE_ASYNC）：漏写 bbt_co_await、把 async 包装直接当
//     result 访问，必须编译失败（否则「标记」形同虚设）。
// 只做 -fsyntax-only，不链接、不需要后端；包含面只有公开头，公开头出现的
// coroutine 依赖是消费方本来就需要的（Async.hpp）。
#include <bbt/infra/mongo/Bson.hpp>
#include <bbt/infra/mongo/Client.hpp>

using namespace bbt::infra;

void Consume(mongo::CoMongoColl& coll, const mongo::MongoDocument& doc,
             const CallOptions& options) {
#ifdef BBT_NEGATIVE_ASYNC
    // 漏写 bbt_co_await：AsyncReturn 没有 result 的 value()，必须编译失败。
    auto pending = coll.FindOne(doc, options);
    pending.value();
#else
    auto found    = bbt_co_await coll.FindOne(doc, options);
    auto inserted = bbt_co_await coll.InsertOne(doc, options);
    auto updated  = bbt_co_await coll.UpdateOne(doc, doc, options);
    auto removed  = bbt_co_await coll.DeleteOne(doc, options);
    (void)found;
    (void)inserted;
    (void)updated;
    (void)removed;
    // BSON 入口是同步值操作，不需要 await。
    auto built = mongo::MongoDocument::FromJson(R"({"a":1})");
    (void)built;
#endif
}
