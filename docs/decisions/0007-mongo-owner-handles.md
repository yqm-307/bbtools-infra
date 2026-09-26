# mongo owner 与集合句柄归属（Issue #40）

## 状态与范围

状态：已实现候选（待独立审查）
日期：2026-09-25。基线：remote main `562a14bc3e03315d994b4834e0c0a01b53287dc9`。

本文固定 Issue #40 的资源归属取舍：Mongo 模块由「一个 client 一组
worker」改为「显式 owner + 轻量集合句柄」，保留 Issue #7 裁决的
mongocxx 同步 driver + 有界 worker bridge 形态。

## 决策

1. **资源 owner 是 `bbt::infra::mongo::CoMongoDb`**（实现实体
   `mongo_detail::MongoRuntime`）：连接配置（生效 URI）、进程级
   pool lease、固定 worker 组、有界接纳队列、ops 注册表与关闭排空
   全部集中在 owner。worker 数与队列容量是 owner 级预算，不随
   集合句柄数增长。
2. **集合句柄是 `bbt::infra::mongo::CoMongoColl`**（实现实体
   `mongo_detail::MongoCollImpl`）：只携带 `MongoTarget{database,
   collection}` 与 owner 共享指针，不新建线程。句柄关闭是接纳
   门禁（`RequestClose` 后新命令 → Closed），不影响兄弟句柄与
   owner 的物理收口。
3. **每项 operation 仍在同一 worker 上 acquire/use/release**
   pooled client；driver 同步调用不可强杀，deadline/cancel/close
   只发布一次逻辑终态，op、client lease 与 payload 保活到物理
   收口；`WaitClosed` 等 ops 清空且 worker 全退才 Closed，不提前
   宣告物理完成。
4. **旧契约 `bbt::infra::CoMongoCli` 保留兼容**：内部实现为
   「独占 owner + 单个集合句柄」，公开 API 与命令语义不变；不
   新增能力，需要多集合共享执行资源的新代码用 `mongo::CoMongoDb`
   + `Collection()`。
5. **公共面不泄漏** mongocxx/bsoncxx/pool/thread：`include/bbt/
   infra/mongo/Client.hpp` 只出现 infra/标准库类型；mongocxx 只
   在 `src/mongo/` 内部头出现。
6. **C++17**：不使用 `co_await`/`Task<T>`；等待经
   `bbt::coroutine::CompletionSignal` + `WaitOptions`。

## 非目标（本任务不做）

- 不迁移 amongoc/direct binding、不重做驱动选型；
- 不建进程全局万能任务池，不扩跨库事务等无需求能力；
- 不复制 Issue #12 的 worker 启动/Redis 清理缺陷核验；
- 不运行无界压测；live 验收沿用 #7 的有界容器编排。

## 验证锚点

- `tests/Test_mongo_unit.cc` `mongo_owner` suite：同一 owner 多
  句柄 worker 数恒定、不同 owner 隔离、`max_queue=1` 跨集合共享
  背压 Overloaded、句柄关闭范围、owner 关闭 drain。
- `tests/Test_mongo_live.cc` `t_owner_multi_collection_live`：真实
  容器上同一 owner 两个集合句柄并行 CRUD。
- 观测钩子：`MongoRuntime::LiveWorkersForTest /
  RunningDriverCallsForTest / PeakDriverCallsForTest`。

## 与已有决策的关系

- 0004（mongocxx 依赖）不变：仍走显式前缀接入的 mongocxx r4.x；
- 0006（`bbt::infra::mongo` 命名空间/目录方向）落地：新公共头在
  `include/bbt/infra/mongo/Client.hpp`，实现继续在 `src/mongo/`；
- 0005（co-io-adapter）不变：完成回投仍走共享 executor strand。
