# 示例与 bbtools-coroutine 兼容说明（Issue #27）

本目录（`examples/`）提供最小真实消费示例，全部基于 **C++17 + bbtools-coroutine
自有模型**：不使用 C++20 `co_await`/`Task<T>`，公共接口不泄漏 hiredis、
mongocxx、bsoncxx 或底层线程类型。

## 示例一览

| 示例 | 目标 | 外部依赖 | 说明 |
| --- | --- | --- | --- |
| `http_loopback.cc` | `Example_http_loopback` | 无（loopback） | NetworkRuntime 完整生命周期：装配 → HTTP loopback → 关闭边界 |
| `redis_mongo_shape.cc` | `Example_redis_mongo_shape` | 链接 hiredis + mongocxx（经 `bbt::infra_redis` + `bbt::infra_mongo`） | Redis/Mongo 命令调用形态与错误/缺失分支；不连真实服务 |

`Example_redis_mongo_shape` 只在 `bbt::infra_redis` 与 `bbt::infra_mongo`
两个 target 都存在（即同时配置了 `BBT_HIREDIS_PREFIX`、
`BBT_MONGOCXX_PREFIX` 与 `BBT_MONGOC_PREFIX`）时注册，并同时链接两个模块
target（`CoRedisCli::Create` 与 `CoMongoCli::Create` 分属两模块实现；
`CoRedisCli.hpp`/`CoMongoCli.hpp` 是 header-only 公共契约）。

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
   CoMongoCli::Create` 要求 Scheduler 已启动（对象身份与完成信号依赖运行时
   代际，未启动返回 `Error(RuntimeUnavailable)`）。
2. **任务注册即协程入口**：`RegistCoroutineTask(callback, succ)` 把回调注册
   为协程任务，由 Scheduler 的 Processer 线程调度执行——这就是 `bbtco` 宏
   背后的稳定入口（示例直接使用稳定 C++ API，不依赖语法宏）。
3. **挂起等待**：所有等待型 API（`HttpClient::Request`、`CoRedisCli::Ping/
   Get/Set/...`、`CoMongoCli::InsertOne/FindOne/...`、`ICoCloseable::
   WaitClosed`）只能在协程上下文调用：等待期间当前协程挂起（底层经 coroutine
   的等待/唤醒机制），到期或完成由 Scheduler 恢复；在非协程上下文调用返回
   `Error(InvalidContext)`，不阻塞线程。
4. **时限与取消**：`CallOptions.deadline`（`bbt::coroutine::Deadline`，即
   `steady_clock::time_point`）与 `CancellationToken` 来自
   `bbt::coroutine::sync/Cancellation.hpp`；deadline/cancel/owner close 竞争
   只发布一次逻辑终态，逻辑返回与物理清理分离（`WaitClosed` 等在途操作归零）。

主线程一侧的限时等待用 `bbt::core::thread::CountDownLatch::WaitTimeout` 做
屏障，不 sleep 假设时序。

## Redis/Mongo 调用形态要点（以当前源码为准）

- **Redis（hiredis async + strand）**：`CoRedisCli` 单连接语义；fd 事件接入
  coroutine 共享 executor 派生的 strand，不新建线程/io_context。`Get` 未命中
  返回 `ok(nullopt)`（与错误区分）；服务端错误回复（如 WRONGTYPE）映射为
  `Error(RemoteError)`；连接断开期间在途命令返回 `Error(TransportError)`，
  新命令触发重连并在队列内等待（受 `max_queue` 与各自 deadline 约束）。
- **Mongo（同步 driver + 有界 worker bridge）**：阻塞 driver 调用在有界
  worker 线程（`worker_threads <= 8`）执行，协程在 worker bridge 上等待；
  driver 失败映射 `Error(Unavailable)`，服务端错误映射 `Error(RemoteError)`
  （`domain_code` 为 codeName），pool 等待超时映射 `Error(Overloaded)`。已
  进入 driver 的同步调用不可强杀，继续到 driver timeout 或完成。
- **关闭边界**：三者关闭顺序一致——先停接纳/不再发起新命令 →
  `RequestClose()`（幂等）→ 协程内 `WaitClosed()`（单等待位，并发第二个
  返回 `CloseStatus::AlreadyWaiting`）→ 释放对象 → `Scheduler::Stop`。

## 与 CI 的边界（诚实声明）

- `Example_http_loopback` 与单测 `contract.basics`、`http.loopback` 等一样
  **不依赖外部服务**，可在 CI 构建/运行。
- Redis/Mongo **live 验收**（`redis.live`、`mongo.live`，经
  `tests/redis-live/run.sh`、`tests/mongo-live/run.sh` + docker 容器）**当前
  未纳入 CI**：按环境变量 `BBT_TEST_REDIS_ADDR` / `BBT_TEST_MONGO_URI` 驱动，
  未提供时整件跳过。`Example_redis_mongo_shape` 同样只演示静态调用形态与
  错误路径，不构成对真实服务的验证。
