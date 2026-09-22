# bbtools-infra
统一的现代 C++ 第三方能力接入层：MCP、RPC、HTTP 与基础设施适配

## 模块

- `bbt::infra_http` — 协程原生 HTTP（Issue #5）
- `bbt::infra_redis` — `CoRedisCli` 协程原生 Redis 客户端（Issue #6）：
  Ping / 二进制安全 Get·Set / Exists / Delete，hiredis asynchronous API +
  共享 coroutine executor，显式 `max_inflight`/`max_queue` 与确定性
  `Overloaded`。依赖与执行域决策见
  `docs/decisions/0003-redis-client-hiredis-dependency.md`。

- `bbt::infra_mongo` — `CoMongoCli` 协程原生 MongoDB 客户端（Issue #7）：
  InsertOne / FindOne / UpdateOne / DeleteOne，BSON 以 infra 自有字节载体
  （`MongoDocument`）进出，公共头不泄漏 mongocxx/bsoncxx；mongocxx 同步
  driver + 固定上限 worker bridge + 有界队列（确定性 `Overloaded`），
  完成回投共享 coroutine executor；`deadline/cancel/close` 只发布一次
  逻辑终态，`WaitClosed` 等待在途 driver 调用物理归零。依赖与执行域
  决策见 `docs/decisions/0004-mongo-client-mongocxx-dependency.md`。

### Redis 模块构建

```bash
cmake -S . -B build \
  -DBBT_HIREDIS_PREFIX=<hiredis 私有前缀>   # 见决策文档固定版本表
cmake --build build -j1
ctest --test-dir build --output-on-failure
# 真实容器验收（需 docker + redis:7-alpine）：
./tests/redis-live/run.sh build/tests/Test_redis_live
```

### Mongo 模块构建

```bash
cmake -S . -B build \
  -DBBT_MONGOCXX_PREFIX=<mongo-cxx-driver 私有前缀> \
  -DBBT_MONGOC_PREFIX=<mongo-c-driver 私有前缀>     # find_dependency(mongoc) 需要
cmake --build build -j1
ctest --test-dir build --output-on-failure
# 真实容器验收（需 docker + mongo:8.0）：
./tests/mongo-live/run.sh build/tests/Test_mongo_live
```
