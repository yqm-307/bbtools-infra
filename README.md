# bbtools-infra
统一的现代 C++ 工具层与基础接入层：MCP、RPC、HTTP、Redis、Mongo 及分布式运行时基础能力。

本仓不承载 `bbt-framework` 的 App、Service 或业务配置编排。infra 提供可独立消费的第三方适配、执行域、错误映射、资源生命周期和基础动态配置机制；上层分布式服务框架负责配置治理、服务发现、灰度、业务 schema 与动态变更编排。

稳定架构边界见 [0006：infra 基础层与动态配置边界](docs/decisions/0006-infra-foundation-and-dynamic-config.md)。新增 Redis/Mongo 公共 API 使用 `bbt::infra::redis` 与 `bbt::infra::mongo`，不使用新的扁平模块命名空间；公共头不泄漏 hiredis、mongocxx、bsoncxx 或底层线程类型。

## I/O 执行边界

[CoTCP/CoUDP 与第三方 Binding 规格](docs/decisions/0005-co-io-adapter-contract-v1.md)
定义目标执行模型：proc 执行收发与协议，sche 只检测就绪并唤醒。
该规格尚未实现，但已于 2026-09-22 通过独立只读规格终审。当前 HTTP 为 Asio/Beast 异步推进，
Redis 为 hiredis async + strand，Mongo 为同步 driver + worker bridge。
旧实现继续按各自决策维护；目标迁移需另获实施授权和真实验收，不由规格落盘自动生效。

## 模块

- `bbt::infra_http` — 协程原生 HTTP（Issue #5）
- `bbt::infra::redis` — Redis 客户端基础接入（当前兼容入口仍记录于现有 Redis 决策）：后续公共 API 按 `include/bbt/infra/redis/` 拆分，覆盖最新 hiredis/Redis 的主流数据结构和控制能力；不把 hiredis 类型、连接线程或底层 context 暴露给消费者。
- `bbt::infra::mongo` — MongoDB 客户端基础接入：公共 API 按 `include/bbt/infra/mongo/` 拆分。`mongo::CoMongoDb` 是显式资源 owner（连接配置、pool lease、worker 组、接纳队列与关闭排空）；`mongo::CoMongoColl` 是集合句柄（db.collection 目标值 + owner 引用），同一 owner 的多句柄共享同一组 worker 与队列，不同 owner 资源隔离。旧契约 `bbt::infra::CoMongoCli` 保留兼容，内部由独占 owner + 单句柄实现；不暴露 mongocxx/bsoncxx、worker 线程或 pool。
- `bbt::infra::config` — 分布式 framework 使用的基础动态配置机制：版本化 snapshot、配置源适配、watch、重连、版本去重和关闭；不实现 framework 的配置中心治理、业务 schema 或动态生效策略。

当前 Redis/Mongo 已交付切片仍是最小能力，不代表上述后续主流 API 已全部实现。

## 示例

最小真实消费示例见 [examples/README.md](examples/README.md)：HTTP loopback 完整生命周期（无外部依赖）与 Redis/Mongo 调用形态演示，以及基于 bbtools-coroutine 现有等待/恢复模型的兼容说明（C++17，不使用 `co_await`/`Task<T>`）。Redis/Mongo live 容器验收未纳入 CI，仅按环境变量驱动、未提供时跳过。
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
