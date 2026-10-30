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

### 增量：多文档数据面（2026-10-30，未合入，待独立审查）

本增量为 `bbt::infra::mongo::CoMongoColl` 新增 5 个多文档入口：`InsertMany`、`Find`、
`UpdateMany`、`DeleteMany`、`CountDocuments`（声明在 `include/bbt/infra/mongo/Client.hpp`；
实现增量在 `src/mongo/MongoRuntime.{hpp,cc}` 与 `src/mongo/MongoDetail.{hpp,cc}`——后者
只扩错误分类对 `inserted_count`/`failed_index` 的提取）。owner、pool lease、worker 组、
有界接纳队列、终态发布与物理收口语义一行未改：5 个入口复用既有
`CollSubmit → Submit → 有界队列 → worker → RunDriverCall → Publish/Finish` 同一管线；
队列满 Overloaded、deadline/Close 竞争只发布一次逻辑终态、协程外 `bbt_co_await` 的
`std::logic_error` 口径（同 2026-10-27 增量第 6 条）、错误映射（服务端回复→RemoteError，
driver 侧失败→Unavailable，无效参数→InvalidArgument，pool wait 超时→Overloaded）全部沿用。
现有 `InsertOne/FindOne/UpdateOne/DeleteOne` 签名与语义不变。

1. **入口语义**：`InsertMany(docs, CallOptions) -> result<MongoInsertManyResult>`（有序
   插入，返回已插入数）；`Find(filter, FindOptions, CallOptions) ->
   result<std::vector<MongoDocument>>`（可选 sort/projection/skip，按游标顺序物化一页，
   无匹配返回空 vector 而非 NotFound）；`UpdateMany(filter, update, CallOptions) ->
   result<MongoUpdateResult>`（复用 UpdateOne 的 update 校验与结果结构，返回
   matched/modified/upserted）；`DeleteMany(filter, CallOptions) -> result<std::uint64_t>`
   （返回删除数）；`CountDocuments(filter, CallOptions) -> result<std::uint64_t>`（精确计数）。
2. **有界上限（公共常量，越界一律接纳前 `InvalidArgument` 零接纳，什么都不发）**：
   `kMongoLimitsMaxInsertDocs=500`、`kMongoLimitsMaxInsertTotalBytes=8 MiB`——依据单条
   命令 16 MiB 的 BSON 协议上限，预留约一半余量给命令封套与 insert 参数，不物化接近
   协议上限的命令；Find 的 `FindOptions::limit` **必填**且 `≤ kMongoLimitsMaxFindLimit=1000`
   （`limit==0` 或超上界均拒绝，杜绝无界游标物化），返回文档累计字节超
   `kMongoLimitsMaxFindResultBytes=16 MiB` → `RemoteError(domain_code="result_too_large")`
   而非 OOM。更大结果集属已知 ceiling（分页/流式，后续按真实需求扩）。
   独立审查后补：**输入侧同界**——`kMongoLimitsMaxInputDocBytes=8 MiB`（同值常量）限制
   filter/update/sort/projection 及 InsertMany 单文档，全部在 `CollSubmit` 之前校验
   （零接纳）；`FindOptions::skip` 上限即 driver int64 表示范围，`skip > INT64_MAX`
   → `InvalidArgument`，不另设分页上限；InsertMany 累计与 Find 返回字节累计均改
   `part > limit - total` 减法预算，杜绝 `size_t` 回绕绕过上限。
3. **空 filter 拒绝（本任务的一条设计决定，可被推翻）**：`UpdateMany`/`DeleteMany` 一律
   拒绝空 filter（bytes 为空或良构 `{}` 均算空，`IsEmptyBson` 判定）→ `InvalidArgument`：
   防止一句空谓词误改/误删全集合；要全量更新/删除须写显式匹配条件（如
   `{"_id":{"$exists":true}}`）。对照面：`Find`/`CountDocuments` 的空 filter 合法（匹配
   全部/统计全集合）；`InsertOne/FindOne/UpdateOne/DeleteOne` 沿用既有 bytes 空拒绝。
   该边界同时写在 `Client.hpp` 的 `CoMongoColl` 类注释，便于调用方发现与推翻。
4. **部分成功契约（InsertMany）**：有序 `insert_many`（v1 默认 ordered=true）首个写错误
   中止 → `RemoteError`，此前已写入的文档**保留**在服务端。调用方经错误通道观察：
   `Error.details["requested_count"]`（本层补齐的请求总数，必然存在）、
   `details["inserted_count"]`（服务端 raw reply 的 `nInserted`/`n`/ordered 时首个
   `writeErrors.index` 三键回退，服务端未给出时该键缺省）、`details["failed_index"]`
   （服务端给出时）；`domain_code`/`backend_code` 仍承载 codeName/code（如
   `DuplicateKey`/`11000`）。live 实测：当前驱动版本 E11000 单点失败只回传
   `requested_count`，`inserted_count`/`failed_index` 缺省——契约按「服务端给出时才有」
   写明（`MongoInsertManyResult` 注释），调用方可靠回退是 `requested_count` +
   复 `Find`/`CountDocuments` 读回核对。

验证锚点（本轮实测，本机 `mongo:8.0` 临时容器）：

- `mongo.unit` 新增 `mongo_data_plane` suite 5 例：`t_dp_param_validation`（InsertMany
  空列表/>500 文档/含空文档/8 MiB 越界→InvalidArgument 与 ~7 MiB 穿过→Unavailable 的
  字节边界对照、Find `limit==0`/>1000、UpdateMany/DeleteMany 空 filter 两种形态，全部
  零接纳——队列深度与执行槽计数为 0）、`t_dp_invalid_context_plain`、
  `t_dp_queue_overloaded`、`t_dp_handle_close_scoped`、`t_dp_owner_close_drains_inflight`
  （在途 op 进入执行槽后 owner Close：唯一终态 Closed、`inflight_at_return` 证明晚到
  driver 结果不覆盖、Close 返回即 worker/执行槽归零）。
- `mongo.live` 新增 7 例（PHASE=A）：`t_dp_insert_many`（全量成功 + 重复 _id 中止 +
  读回证明部分成功）、`t_dp_find_semantics`（sort 升降序、projection 字段集、skip/limit
  页边界、空 filter 匹配全部）、`t_dp_find_result_bytes`（~19 MiB 超 16 MiB 上限 →
  `result_too_large`）、`t_dp_update_many`、`t_dp_delete_many`、`t_dp_count_documents`、
  `t_dp_find_blocked_owner_close`（failCommand blockConnection + waitForFailPoint 握手
  确认新 Find 已进入 native driver 阻塞后再 owner Close，取代仅靠 phase==kRunning 的
  执行槽口径）。
  数据面用例每例独占临时库 `bbt_dp_<pid>_<seq>_<tag>`，`DpNsGuard` 经显式 `Finish()`
  drop 并 `list_collection_names` 读回，清理失败置共享原子并让用例失败（`CheckDpCleanup`），
  成功与异常路径都只清理自有命名空间，不触碰共享 `bbt_live`。
- `mongo.async_compile`：5 个新入口经 `bbt_co_await` 的正例编译门禁。

未覆盖（诚实边界）：本机验证用 `mongo:8.0`，CI compose 为 `mongo:7.0`
（`tests/mongo-live/compose.yml`），7.0 上未实测；`mongo.live` 两个既有不稳定签名
（`t_close_drains_inflight` 的 settled 0 != 16、`t_owner_multi_collection_live` 的
WaitUntil ok==2）未消除，非本增量引入，本轮单次全过不构成已修；PHASE B/C 与 run.sh
编排未在本增量中实跑。CI 路径：新 unit 用例落在 `mongo.unit` 全量 ctest，新 live 用例
落在 `tests/mongo-live/run.sh` PHASE A（全量跑该二进制）；`.github/` 未改。
