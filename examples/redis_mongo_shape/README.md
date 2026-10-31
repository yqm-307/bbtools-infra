# Redis/Mongo 调用形态示例：静态形态演示，不连接真实服务。

本示例演示 CoRedisCli / CoMongoCli 的真实公开 API 调用形态：装配校验 → Scheduler 内 Create → 协程内命令（deadline） → 关闭边界。只做参数校验与错误路径打印，不连接真实 Redis/Mongo，因此无 hiredis/mongocxx 依赖即可编译运行。真实服务调用形态与语义见本目录上层 README「示例与兼容说明」。

当 `bbt::infra_redis` 与 `bbt::infra_mongo` 目标均存在时注册并同时链接两者；任一缺失时跳过（公共头仍可独立编译）。

## 怎么跑

```bash
cmake -S <repo> -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树>
cmake --build build --target Example_redis_mongo_shape --parallel 2
./build/examples/redis_mongo_shape/Example_redis_mongo_shape
ctest --test-dir build -j1 -R '^example_redis_mongo_shape$' --output-on-failure
```

## 期望输出

```
redis Connect error (expected without live redis): code=... status=...
redis Ping error (expected without live redis): code=...
redis Set error: code=...
redis Get error: code=...
redis Get miss (nullopt, distinct from error)
redis Delete error: code=...
mongo InsertOne error (expected without live mongo): code=...
mongo FindOne error: code=...
mongo FindOne miss (nullopt, distinct from error)
```
退出码 0 表示形态演示跑通；错误码/非命中均为预期（无真实后端时）。
