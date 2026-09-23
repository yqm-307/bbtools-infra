# MongoDB 客户端切片：mongo-cxx-driver 依赖选型与 worker bridge 决策

## 状态与范围

日期：2026-09-19。关联 Issue：#7（拆分 #3：CoMongoCli 协程客户端与有界 worker bridge）、父任务 #3、契约文档 `0002-co-network-contract-v1.md`。本决策记录当前 Mongo 最小切片，不限制后续主流 API 扩展。后续公共 API 应迁移到 `bbt::infra::mongo` 与 `include/bbt/infra/mongo/`；实现必须基于 bbtools-coroutine 现有等待/恢复模型，不使用 C++20 `co_await`/`Task<T>` 作为公共接口示例。配置读取、动态 watch 和分布式配置治理不属于本客户端决策，基础配置机制见 `0006-infra-foundation-and-dynamic-config.md`。

### 后续目标规格

[0005：CoTCP/CoUDP 与第三方 Binding](0005-co-io-adapter-contract-v1.md) 将 worker bridge 作为显式兼容例外保留。本文“只能在专用 worker”是当前切片选型，不是 mongocxx 永远不能协程化的证明。是否能采用直接绑定或非阻塞 owner 驱动，由独立兼容调查裁决；PR 合并不代替真实后端、关闭及下游消费验收。

## 固定依赖

| 项 | 值 |
|---|---|
| C++ 驱动 | mongo-cxx-driver `r4.6.0`（`mongo::mongocxx_shared` imported target） |
| C 驱动 | mongo-c-driver `2.5.4`（`mongoc::shared`，mongocxxConfig 经 `find_dependency(mongoc 2.5.3)` 解析到此前缀） |
| BSON 库 | bsoncxx `4.6.0`（随 C++ 驱动前缀，`mongo::bsoncxx_shared`） |
| 许可证 | Apache-2.0 |
| 消费方式 | 私有前缀安装 + CMake package config |

## 链接来源与接入

- 构建经 `BBT_MONGOCXX_PREFIX`/`BBT_MONGOC_PREFIX`（cache PATH，默认空）
  定位前缀；`find_package(mongocxx CONFIG REQUIRED PATHS ... NO_DEFAULT_PATH)`
  杜绝误链系统位置同名库。
- `mongocxxConfig.cmake` 内 `find_dependency(mongoc/bsoncxx)` 的嵌套
  `find_package` 不吃 `CMAKE_PREFIX_PATH`（外层 `NO_DEFAULT_PATH` 会
  传播），改为在顶层作用域把 `mongoc_DIR`/`bson_DIR`/`bsoncxx_DIR`
  钉到私有前缀下 glob 出的版本目录。
- imported target 只有目录级可见性：`find_package` 必须放在根
  CMakeLists 顶层，`src/` 与 `tests/` 才都能引用。
- 未给前缀时 `bbt_infra_mongo` 整体跳过，公共头仍经 selfcheck 验证自给。
- 实测链接解析（`ldd`）：`libmongocxx1.so.1`/`libbsoncxx1.so.1` →
  `mongo-cxx-driver` 前缀，`libmongoc2.so.2`/`libbson2.so.2` →
  `mongo-c-driver` 前缀。

## 执行域决策（同步 driver + worker bridge）

- mongocxx 无异步 API；`amongoc` 实验库不被采用。同步 driver 调用只能
  在专用 worker 线程上阻塞执行，**不落在共享 executor/io 域**。
- `worker_threads` 固定上限（验收默认 ≤2）；每项 operation 在同一
  worker 上 `pool.acquire()/use/release`，满足 `mongocxx::client`
  单线程亲和。
- `max_queue` 有界；队列满新命令立即 `Overloaded`，不无限排队。
- 完成回投：`boost::asio::post` 到共享 executor 派生 strand 后
  `CompletionSignal::Complete()` 唤醒等待协程；executor 不可达时
  worker 线程直接 Complete（CompletionSignal 允许任意线程）。
- 进程级 `mongocxx::v1::instance` + 按生效 URI 划分的共享 pool
  （上限 8 个 distinct URI）；lease 内 instance 成员先于 pool 声明，
  保证 instance 晚于全部 pool 析构。

## 超时与取消的真实语义

- 驱动层阻塞上界注入生效 URI：`serverSelectionTimeoutMS`/
  `connectTimeoutMS`/`socketTimeoutMS`/`waitQueueTimeoutMS`，外加
  `maxPoolSize = worker_threads`（并发 lease 与 worker 数一致）。
  URI 中已显式给出的同名项不覆盖。
- 同步 driver 调用**不可强杀**：`deadline/cancel/close` 只发布一次
  逻辑终态（TimedOut/Cancelled/Closed），已进入 driver 的调用继续到
  自身超时；op、client lease 与 payload 保活到物理收口。
- `WaitClosed` 只在「在途 driver 调用与队列真正归零且 worker 全退」
  后返回 `Closed`——CloseStatus::Closed 表示后端不再访问本组件资源，
  而非仅收到关闭意图。

## 错误映射

- `v1::server_error`（含 `raw()`）与 `v_noabi::operation_exception`
  （`raw_server_error()`）→ `RemoteError`；`domain_code` 取服务端
  `codeName`（如 `DuplicateKey`），`backend_code` 取服务端 `code`。
- 驱动另存在「普通 `v1::exception` + server_error_category」形态：
  `ec == v1::source_errc::server` 经 `equivalent()` 成立 → `RemoteError`，
  codeName 缺失时按已知码归类（11000/11001/12582 → `DuplicateKey`）。
  注意 `v_noabi::exception` **不是** `v1::exception` 子类，必须单独
  catch；只捕获 v1 层级会漏掉 CRUD 的实际抛出类型。
- `type_errc::invalid_argument`、`invalid_*_name` → `InvalidArgument`；
  `pool::errc::wait_queue_timeout` → `Overloaded`；mongoc/mongocrypt
  域（server selection、socket、stream）→ `Unavailable`；其余 →
  `InternalError`。`backend_category` 固定 `"mongocxx"`/`"bsoncxx"`。
- 不良构 BSON 经 `bsoncxx::validate` 全结构校验 → `InvalidArgument`，
  在 driver 调用之前返回，不接触服务端。

## 测试架构

- 单测不依赖 Docker：`mongodb://127.0.0.1:1` 不可达地址 + 有限
  `serverSelectionTimeoutMS` 驱动确定性失败；worker 并发/容量经 impl
  观测钩子（`RunningDriverCallsForTest`/`PeakDriverCallsForTest`）
  核验，不 sleep 猜时序。覆盖配置校验、InvalidContext、不良构 BSON、
  确定性 Overloaded、deadline/cancel 与「逻辑返回≠物理收口」、
  in-flight close drain、峰值 ≤ worker_threads、单等待位竞争。
- 真实验收 `tests/mongo-live/`：`mongo:8.0` 容器 `cpus<=0.75`、
  `mem_limit<=512m`、`--wiredTigerCacheSizeGB 0.25`、动态项目名、
  固定宿主机端口（compose stop/start 会重分随机映射端口）、
  `trap down -v` 清理。PHASE=A 全功能+16 并发 smoke（单请求 ≤3s、
  总时长 ≤30s）；PHASE=B 停止态命令必须 `Unavailable`；PHASE=C
  重启后新 client 恢复。
- coroutine Hook 对非协程线程的 `nanosleep` 断言：测试线程睡眠一律
  `SYS_nanosleep`；套件收尾 `g_scheduler.release()` 绕过
  `~Scheduler→Stop` 同类断言（上游基线竞态，测试侧不可修复）。

## 退出/替换路径

- 公共头 `CoMongoCli.hpp` 不暴露 mongocxx/bsoncxx 类型；替换后端只需
  重写 `src/mongo/` 内部（MongoDetail 转换/分类、MongoProcess 池域、
  CoMongoCliImpl 编排），API 与语义不变。
- 若未来接 `amongoc` 等异步后端，worker bridge 可由事件驱动实现替换，
  错误映射与关闭语义需在新后端等价域上重新复核。

## 未覆盖与已知差异

- `ChangeStream`/事务/`insert_many`/聚合不在本切片；多集合由多 client
  实例聚合。
- `find_one` 仅下发 `maxTimeMS=socket_timeout`；其余 CRUD 无等价服务端
  选项，超时由 socket/server-selection 上界承担。
- URI 显式给出的驱动超时项不被覆盖——调用方须自行保证有限值，否则
  阻塞上界不再由 config 保证。
- 每 client 绑定单一 `db.collection`；跨库/集合操作不在本切片。
