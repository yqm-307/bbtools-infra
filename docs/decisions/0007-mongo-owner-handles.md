# mongo owner 与集合句柄归属（Issue #40）

## 状态与范围

状态：owner + 集合句柄形态已合入 main（PR #44 `feat(mongo): move client ownership into runtime`，`3bd558a`；Issue #40 已关闭）。本记录不代表完整生命周期/跨平台验收已完成。
日期：2026-09-25。基线：remote main `562a14bc3e03315d994b4834e0c0a01b53287dc9`。

### 修订记录（2026-10-01：进程寿命运行时 + 同步 Close）

本文原依赖 `RequestClose`/`WaitClosed`/`CompletionSignal` 的表述已被 infra 的**同步 `Close()`**
与 coroutine **进程寿命运行时**（候选 HEAD `03430a5`）取代：句柄关闭是接纳门禁（`Close()` 后
新命令 → `Closed`）；owner 同步 `Close()` 返回即 ops 清空且 worker 全退；等待经
`CoWaiter::WaitWithCallback` + `WaitOptions`（结果放 operation state），不再有 `CompletionSignal`。
本文资源归属结论（owner 集中预算、句柄轻量、公共面不泄漏 mongocxx/线程）**继续有效**。superseded by
[0002 修订记录](0002-co-network-contract-v1.md)。

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
   门禁（`Close()` 后新命令 → Closed），不影响兄弟句柄与
   owner 的物理收口。
3. **每项 operation 仍在同一 worker 上 acquire/use/release**
   pooled client；driver 同步调用不可强杀，deadline/cancel/close
   只发布一次逻辑终态，op、client lease 与 payload 保活到物理
   收口；owner 同步 `Close()` 返回即 ops 清空且 worker 全退，不提前
   宣告物理完成。
4. **旧契约 `bbt::infra::CoMongoCli` 保留兼容**：内部实现为
   「独占 owner + 单个集合句柄」，公开 API 与命令语义不变；不
   新增能力，需要多集合共享执行资源的新代码用 `mongo::CoMongoDb`
   + `Collection()`。
5. **公共面不泄漏** mongocxx/bsoncxx/pool/thread：`include/bbt/
   infra/mongo/Client.hpp` 只出现 infra/标准库类型；mongocxx 只
   在 `src/mongo/` 内部头出现。
6. **C++17**：不使用 `co_await`/`Task<T>`；等待经
   `CoWaiter::WaitWithCallback` + `WaitOptions`（结果放 operation state）。

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
  RunningDriverCallsForTest / PeakDriverCallsForTest`；unit 还通过
  `LastDriverThreadEvidenceForTest` 核对同一 owner 两个集合的
  `worker/acquire/use/release` 线程与目标集合。

### 增量：BSON 入口与 async 等待标记（2026-10-27，未合入，待独立审查）

本增量只做**适配迁移**，不扩 Mongo 能力（无 `insert_many`、聚合、事务、ChangeStream、
跨集合批量）；`MongoRuntime`/`MongoProcess`/`MongoSupport` 的 owner、pool lease、
worker 组、`max_queue` 背压、deadline/取消与 Close 收口语义**一行未改**。

1. **单一定义 + 旧名别名**。`MongoDocument` / `MongoUpdateResult` 的唯一实现在
   `include/bbt/infra/mongo/Bson.hpp`（`bbt::infra::mongo`）；`include/bbt/infra/CoMongoCli.hpp`
   改为 `#include <bbt/infra/mongo/Client.hpp>` + `using mongo::MongoDocument; using
   mongo::MongoUpdateResult;`，`kMongoLimits*` 边界也移到 `mongo/Client.hpp` 并以 `using`
   保留旧名。旧写法 `MongoDocument{{0x05, ...}}` 聚合直写继续合法（`bytes` 仍是唯一真源）。
   **ABI**：类型 canonical name 改变 → 二进制不兼容；本仓无 install/export 包、只做源码
   消费，故接受，消费方需重新编译。
2. **BSON 构造/读取只做薄封装**。构造 = `MongoDocument::FromJson(std::string_view)`
   （底层 `bsoncxx::from_json`，不自写编码器）；读取 = `ToJson(MongoJsonMode)`
   （`bsoncxx::to_json`，默认 canonical）+ `Has` 与 `GetString/GetInt64/GetDouble/GetBool`
   四个**顶层**字段 getter。公开头不出现任何 `bsoncxx::` 类型。明确 ceiling：无嵌套路径、
   无数组索引、无 `_id` 特殊化；后续按真实需求扩。`FromJson` 只接纳**一份完整对象**：
   libbson 的 JSON 前端只消费第一份文档、静默忽略其后内容，故解析成功后还要核对整段输入
   （结构范围定位，跳过字符串与转义），相邻/空白分隔的第二份对象与尾部垃圾都拒绝。
3. **错误语义**（沿用既有错误域，不新增 `ErrorCode`）：字段缺失 → `NotFound`；类型不符 →
   `TypeMismatch`；JSON 解析失败/空串/顶层非对象/多份对象或尾部内容/超 int64 →
   `InvalidArgument`；bytes 空或不良构 → `InvalidArgument`（`bsoncxx::validate` 全结构校验，
   在 driver 之前返回）。`backend_category="bsoncxx"` 与 `backend_code`（库返回码）只在**实际
   调用了库且库报错**时出现；本组件自己的前置检查（空串/非对象/单文档完整性）不带后端元数据。
   `Has` 是纯谓词（无错误通道），bytes 非法时为 false。
4. **数值保真（实测 libbson 2.5.4，非推测；断言固化在 `mongo_bson` suite）**：

   | 输入 JSON | 解析类型 | canonical 输出 | relaxed 输出 |
   |---|---|---|---|
   | `7` | int32 | `{"$numberInt":"7"}` | `7` |
   | `{"$numberLong":"7"}` | int64 | `{"$numberLong":"7"}` | `7`（类型信息丢失，回环变 int32） |
   | `7.0` / `{"$numberDouble":"7.0"}` | double | `{"$numberDouble":"7.0"}` | `7.0` |
   | `9223372036854775808` | — | — | `FromJson` → `InvalidArgument`（不静默转 double） |

   因此默认 canonical：需要 int64 必须写 `$numberLong`，且只有 canonical 往返能保住类型。
   另：libbson 会把顶层数组 `[1,2]` 解析成数字键文档，`FromJson` 显式拒绝（`InvalidArgument`），
   不把数组静默变成文档。
5. **async 标记只在 `mongo::CoMongoColl` 的 4 个 CRUD**：返回类型为
   `bbt_co_async(result<...>)`，调用方必须经 `bbt_co_await` 消费。旧
   `bbt::infra::CoMongoCli` 的 4 个方法**保持裸 `result<T>`**（framework 与仓内既有调用点
   不动）；`Create/Start/Collection/Close/IsClosed/Validate*` 与观测访问器不挂起。标记只
   附加返回类型，不改变实现、队列、错误映射或关闭路径。
6. **非协程调用口径变化（只限新入口）**：`bbt_co_await` 的 `AwaitHelper` 构造在非协程域抛
   `std::logic_error`，且该检查先于函数体，故新入口在协程外**写了 `bbt_co_await` 的调用**
   不再返回 `InvalidContext` 而是抛异常；漏写 `bbt_co_await`、直接持有返回值的调用不会被
   静态拦住（函数体照常执行），因此这不是协程外调用的完整静态强制。legacy `CoMongoCli` 仍是
   `InvalidContext`。`CoMongoColl::Close()` 的验收边界
   不变：仍为句柄接纳门禁，不等待 owner drain；owner `Close()` 仍阻塞到真实物理落定
   （在途 driver 返回 + ops 清空 + worker 全退 + lease 归还），不是固定 5 秒窗口。

验证锚点（本轮实测，构建目录与日志在仓外私有 scratch）：

- `mongo.unit`：新增 `mongo_bson` suite 6 例（`t_bson_from_json_canonical_roundtrip`、
  `t_bson_numeric_fidelity`、`t_bson_invalid_json`、`t_bson_missing_field_not_found`、
  `t_bson_type_mismatch`、`t_bson_empty_document`）；既有 `mongo_unit`/`mongo_owner`
  用例在句柄调用点补 `bbt_co_await` 后全绿。
- `mongo.live`：新增 `t_public_bson_api_live`（PHASE=A，FromJson → InsertOne → FindOne →
  getter/ToJson → DeleteOne → 读回未命中）；A/B/C 三相位经 `tests/mongo-live/run.sh` 全绿。
- `mongo.client.consumer`：`examples/mongo_client` 只链接 `bbt::infra_mongo`、只含公开头，
  真实后端 CRUD + 自清理读回；无 `BBT_TEST_MONGO_URI` 时为 Skipped。
- `mongo.async_compile`：async 正负编译门禁（正例 4 个 CRUD 经 `bbt_co_await` 可编译；
  漏写 await、直接按 `result` 取 `value()` 的负例被拒绝）。

**未覆盖（诚实边界）**：CI 的 Release job 只对新示例做**编译**覆盖，运行需要
`BBT_TEST_MONGO_URI`，CI 中为 Skipped；framework 仍是 legacy `CoMongoCli` 消费方，本轮
不切换；事务/聚合/`insert_many`/ChangeStream 与 TSan 仍属既有未覆盖项。

### Sanitizer 边界（2026-10-03）

ASan/UBSan 的 `mongo.unit` 已通过。TSan 在当前固定的
mongo-c-driver `2.5.4`（上游 commit `ad87ab88907a0105823469fb5d393ed717bed9ba`）
上稳定报告 `libmongoc2.so.2.5.4:mongoc_set_destroy` 与其 server-monitor 线程的竞争；
调用栈位于第三方驱动内部，不能作为本 owner identity 钩子已通过 TSan 的证据。
该项保留为上游/依赖构建边界，不能以跳过或 suppression 包装成通过。

## 与已有决策的关系

- 0004（mongocxx 依赖）不变：仍走显式前缀接入的 mongocxx r4.x；
- 0006（`bbt::infra::mongo` 命名空间/目录方向）落地：新公共头在
  `include/bbt/infra/mongo/Client.hpp`，实现继续在 `src/mongo/`；
- 0005（co-io-adapter）不变：完成回投仍走共享 executor strand。
