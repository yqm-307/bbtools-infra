# 示例与 bbtools-coroutine 兼容说明（Issue #27）

本目录（`examples/`）提供最小真实消费示例，全部基于 **C++17 + bbtools-coroutine
自有模型**：不使用 C++20 `co_await`/`Task<T>`，公共接口不泄漏 hiredis、
mongocxx、bsoncxx 或底层线程类型。

> **已合入 main（close/stop 迁移 `02567ed`）**：本文描述的是**新关闭/等待契约**口径；本目录
> `*.cc` 示例源码已迁到此契约（旧 `RequestClose`/`WaitClosed`/`Scheduler::Stop` 调用链已删除）。
> 本文不充当已发布契约的最终验收结论，覆盖边界见下文与各决策文档。

## 示例一览

| 示例 | 目标 | 外部依赖 | 说明 |
| --- | --- | --- | --- |
| `http_loopback.cc` | `Example_http_loopback` | 无（loopback） | NetworkRuntime 完整生命周期：装配 → HTTP loopback → 关闭边界 |
| `config_consume.cc` | `Example_config_consume` | 无 | 配置首切片：本地文件/内存源读取 → 上层校验 → 接受/拒绝；格式错误不发布伪成功 |
| `redis_mongo_shape.cc` | `Example_redis_mongo_shape` | 链接 hiredis + mongocxx（经 `bbt::infra_redis` + `bbt::infra_mongo`） | Redis/Mongo 命令调用形态与错误/缺失分支；不连真实服务 |

`examples/config_consumer/` 是一个**独立 CMake 工程**（不被本仓构建引用），只 include
`bbt/infra/config/` 公共头、只链接 `bbt::infra_config`，用于从仓外视角复核模块 target 可独立
消费；命令、实测结果与覆盖边界见该目录 `README.md` 与 [docs/config-watch-v1.md](../docs/config-watch-v1.md)。
watch 的关闭/去重/drain 与 owner 同步 `Close()` 资源边界不在示例中，由 `tests/Test_config_watch.cc`（`config.watch`）覆盖。

`Example_redis_mongo_shape` 只在 `bbt::infra_redis` 与 `bbt::infra_mongo`
两个 target 都存在（即同时配置了 `BBT_HIREDIS_PREFIX`、
`BBT_MONGOCXX_PREFIX` 与 `BBT_MONGOC_PREFIX`）时注册，并同时链接两个模块
target（`CoRedisCli::Create` 与 `mongo::CoMongoDb::Create` 分属两模块实现；
公共契约均为 header-only）。Mongo 侧演示 Issue #40 的 owner + 集合句柄形态：
一个 `mongo::CoMongoDb` 承载 worker/队列/pool，`Collection()` 取轻量句柄。

## 构建

```bash
# 最小构建（无 Redis/Mongo）：仅 HTTP 示例
cmake -S . -B build \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DBBT_CORE_SOURCE_DIR=<bbtools-core 源码树>
cmake --build build --target Example_http_loopback --parallel 2
./build/examples/Example_http_loopback

# 含 Redis/Mongo 形态示例
cmake -S . -B build \
  -DBBT_COROUTINE_SOURCE_DIR=<...> -DBBT_CORE_SOURCE_DIR=<...> \
  -DBBT_HIREDIS_PREFIX=<hiredis 私有前缀> \
  -DBBT_MONGOCXX_PREFIX=<mongo-cxx-driver 私有前缀> \
  -DBBT_MONGOC_PREFIX=<mongo-c-driver 私有前缀>
cmake --build build --target Example_redis_mongo_shape --parallel 2
```

## bbtools-coroutine 兼容模型（如何复用现有等待/恢复机制）

bbtools-infra 不引入新协程语法；等待/恢复完全复用 bbtools-coroutine 现有
机制，示例中体现为四个要点：

1. **运行时装配**：`bbt::coroutine::detail::Scheduler::GetInstance()->Start(
   SCHE_START_OPT_SCHE_THREAD)` 启动调度器；`NetworkRuntime/CoRedisCli/
   CoMongoCli::Create` 要求运行时已初始化（`Scheduler::IsInitialized()`；
   不再有运行时代际，未初始化返回 `Error(RuntimeUnavailable)`）。
2. **任务注册即协程入口**：`RegistCoroutineTask(callback, succ)` 把回调注册
   为协程任务，由 Scheduler 的 Processer 线程调度执行——这就是 `bbtco` 宏
   背后的稳定入口（示例直接使用稳定 C++ API，不依赖语法宏）。
3. **挂起等待**：所有等待型 API（`HttpClient::Request`、`CoRedisCli::Ping/
   Get/Set/...`、`CoMongoCli::InsertOne/FindOne/...`）只能在协程上下文调用：
   内部以 `CoWaiter::WaitWithCallback` 登记等待事件 → 执行一次投递回调 →
   挂起（底层经 coroutine 的等待/唤醒机制），响应到达由 adapter 路径 `Notify`
   唤醒；在非协程上下文调用返回 `Error(InvalidContext)`，不阻塞线程。**已删除**
   `ICoCloseable::WaitClosed`——关闭是资源 owner 主动调用的同步 `Close()`，
   没有「等待关闭完成」入口。
4. **时限与取消**：`CallOptions.deadline`（`bbt::coroutine::Deadline`，即
   `steady_clock::time_point`）是唯一的调用选项；`CancellationToken`
   （`sync/Cancellation.hpp`）与 `CompletionSignal` 已删除。等待中的取消来自
   协程级 `RequestCancel`（`WaitStatus::Cancelled`），业务级取消由上层带载荷
   `Notify`（`CoEventValue`）表达。逻辑结果与物理清理分离：每请求只交付一次
   终态，物理收口由 owner 主动同步 `Close()` 落定（返回即后端不再访问）。

主线程一侧的限时等待用 `bbt::core::thread::CountDownLatch::WaitTimeout` 做
屏障，不 sleep 假设时序。

## Redis/Mongo 调用形态要点（以当前源码为准）

- **Redis（CoTCP 字节 I/O + hiredis RESP2 编解码）**：`CoRedisCli` 单连接语义；
  CoTCP 承担 socket/等待/关闭，hiredis 只做同步编解码，不新建线程/io_context。
  显式生命周期：协程内 `Connect(options)` 真实等待 TCP 建连完成（命令不隐式建连）；
  `ConnectStatus()` 只读本地状态、不发网络探测；`Disconnect()` 同步收口连接但保留
  配置、可再次 `Connect`；`Close()` 是终态。`Get` 未命中返回 `ok(nullopt)`（与错误
  区分）；服务端错误回复（如 WRONGTYPE）映射为 `Error(RemoteError)`；连接故障后
  默认（`reconnect_on_new_command=false`）新命令返回 `Error(TransportError)`，显式
  打开时仅故障后的新命令至多各尝试一次新连接；若同一批次重建再次失败，后续已入队命令可统一以 `TransportError` 落定；已失败命令不重发/不迁移。
- **Mongo（同步 driver + 有界 worker bridge）**：阻塞 driver 调用在有界
  worker 线程（`worker_threads <= 8`）执行，协程在 worker bridge 上等待；
  driver 失败映射 `Error(Unavailable)`，服务端错误映射 `Error(RemoteError)`
  （`domain_code` 为 codeName），pool 等待超时映射 `Error(Overloaded)`。已
  进入 driver 的同步调用不可强杀，继续到 driver timeout 或完成。
- **关闭边界**：三者一致——先停接纳/不再发起新命令 → 资源 owner 主动同步
  `Close()`（返回即物理资源已释放、后端不再访问；未发送数据直接丢弃、不 flush、
  不重开）→ 继续后续收尾。已删除 `RequestClose`/`WaitClosed`/`CloseStatus` 与
  `Scheduler::Stop`：没有「等待关闭完成」入口，运行时按进程寿命存在、不参与
  资源收口。

## 与 CI 的边界（诚实声明）

- `Example_http_loopback` 与单测 `contract.basics`、`http.loopback` 等一样
  **不依赖外部服务**，可在 CI 构建/运行。
- Redis/Mongo **live 验收**（`redis.live`、`mongo.live`，经
  `tests/redis-live/run.sh`、`tests/mongo-live/run.sh` + docker 容器）**当前
  未纳入 CI**：按环境变量 `BBT_TEST_REDIS_ADDR` / `BBT_TEST_MONGO_URI` 驱动，
  未提供时整件跳过。`Example_redis_mongo_shape` 同样只演示静态调用形态与
  错误路径，不构成对真实服务的验证。
