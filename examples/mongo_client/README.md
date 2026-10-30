# Mongo 公开入口消费示例：只链接公开 target `bbt::infra_mongo`。

本示例演示 Mongo 公开入口的最小真实消费：只 include `<bbt/infra/mongo/Bson.hpp>`、`<bbt/infra/mongo/Client.hpp>`、`<bbt/infra/Result.hpp>`，只链接 `bbt::infra_mongo`，不出现 mongocxx/bsoncxx 类型。走 `FromJson → InsertOne → FindOne → 类型化 getter + canonical ToJson → DeleteOne → 读回未命中` 的 CRUD 闭环，只写自己的唯一 `_id` 并在结束前删除。

真实后端由 `BBT_TEST_MONGO_URI` 注入（例如 `tests/mongo-live/run.sh` 容器）。未设置时打印 `skip:` 并返回 77（ctest 记为 Skipped，绝不冒充已运行）；CI 全量构建只给「公开消费可编译」证据。

## 怎么跑

```bash
cmake -S <repo> -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DBBT_MONGOCXX_PREFIX=<mongo-cxx-driver 私有前缀> \
  -DBBT_MONGOC_PREFIX=<mongo-c-driver 私有前缀>
cmake --build build --target mongo_client_example --parallel 2
# 有真实后端时运行：
BBT_TEST_MONGO_URI=mongodb://127.0.0.1:<port>/ ./build/examples/mongo_client/mongo_client_example
ctest --test-dir build -j1 -R '^mongo.client.consumer$' --output-on-failure
```

## 期望输出

```
# 未设置 BBT_TEST_MONGO_URI:
skip: 需要 BBT_TEST_MONGO_URI（真实 MongoDB）
# 设置后:
mongo_client_example: n=7 ratio=1.5 tag=hello deleted=1 miss_after_delete=1
canonical=...
```
退出码 0 表示 CRUD 闭环与清理确认通过；77 表示未提供真实后端（Skipped）。
