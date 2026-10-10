# bbtools-infra

统一的现代 C++ 工具层与基础接入层：HTTP、CoTCP/CoUDP transport、Redis、Mongo 及分布式运行时基础能力。

本仓不承载 `bbt-framework` 的 App、Service 或业务配置编排。infra 提供可独立消费的第三方适配、执行域、错误映射、资源生命周期和基础动态配置机制；上层分布式服务框架负责配置治理、服务发现、灰度、业务 schema 与动态变更编排。

稳定架构边界见 [0006：infra 基础层与动态配置边界](docs/decisions/0006-infra-foundation-and-dynamic-config.md)。新增 Redis/Mongo 公共 API 使用 `bbt::infra::redis` 与 `bbt::infra::mongo`，不使用新的扁平模块命名空间；公共头不泄漏 hiredis、mongocxx、bsoncxx 或底层线程类型。

## 模块交付状态

| 模块 | CMake target | 状态 | 说明 |
|------|-------------|------|------|
| HTTP（client + server + NetworkRuntime） | `bbt::infra_http` | 已实现，单测覆盖中 | Beast/Asio 异步；配额、关闭排空、子对象反登记等可靠性边界仍在 Issue 中推进 |
| CoTCP / CoUDP transport | `bbt::infra_tcp`、`bbt::infra_udp` | 已实现 | 基础收发与受管工厂可用；Runtime 级 `max_inflight` 在途门禁已实现并合入（Issue #31/#32 已关闭）；跨平台/非 Linux、公网 DNS 与部分 lifecycle 直接证据仍未覆盖 |
| Redis client | `bbt::infra_redis` | 已实现最小切片 | CoTCP owner binding（`CoRedisCliImpl` + 窄 `RedisCotcpConn`）：CoTCP 承担 Dial/读写/等待/关闭，hiredis 只做同步 RESP2 编解码；单连接语义；`bbt::infra::redis` 公共 API 目录尚未拆分 |
| Mongo client | `bbt::infra_mongo` | 已实现 owner + 句柄形态 | 同步 driver + worker bridge；`mongo::CoMongoDb` 为资源 owner，`mongo::CoMongoColl` 为集合句柄；`include/bbt/infra/mongo/` 公共目录已建，剩余 API 按决策 0006 逐步迁移 |
| 基础动态配置 | `bbt::infra_config` | 最小切片已合入主线，独立消费已验证 | `bbt::infra::config`；版本化不可变快照 + 内存源 + 真实本地文件源 + watch（初始/更新/重复去重、失败恢复、关闭与晚到通知、正常 drain 与 owner 同步 `Close()` 的资源边界）。**未交付**：远程 provider 与真实重连、业务 schema 校验、客户端资源热切换。契约/命令/未覆盖矩阵见 [docs/config-watch-v1.md](docs/config-watch-v1.md) |
| RPC | `bbt::infra_rpc` | wire profile 最小切片已实现 | 版本化 Protobuf RpcEnvelope wire profile（`bbt::infra::rpc`，Issue #8）：值类型校验、编解码与 HTTP body 绑定；带网络运行的 RpcClient/Server 完整运行时**未交付**（设 `BBT_PROTOBUF_PREFIX` 时构建） |
| MCP | — | 未开始 | 归对应 Issue 推进，当前无实现 |

已实现能力的单测与示例均可独立构建运行，见下文「独立消费」与 [examples/README.md](examples/README.md)。

## I/O 执行边界

[CoTCP/CoUDP 与第三方 Binding 规格](docs/decisions/0005-co-io-adapter-contract-v1.md) 定义目标执行模型：proc 执行收发与协议，sche 只检测就绪并唤醒。

当前 HTTP 为 Asio/Beast 异步推进，Redis 为 CoTCP owner binding（CoTCP 承担字节 I/O/等待/关闭，hiredis 只做同步 RESP2 编解码），Mongo 为同步 driver + worker bridge。CoTCP/CoUDP 已交付基础实现，Runtime 级 `max_inflight` 在途门禁已实现并合入（Issue #32 已关闭，见 [0005 §4](docs/decisions/0005-co-io-adapter-contract-v1.md)）；跨平台/非 Linux、公网 DNS 与部分 lifecycle 的直接验收证据仍未覆盖。

## 资源关闭与请求完成契约（进程寿命运行时）

coroutine 运行时按**进程寿命**存在、不参与业务资源收口（不再有 `Scheduler::Stop()`/restart 与运行时代际）。infra 关闭契约见 [0002 co-network/v1 修订记录](docs/decisions/0002-co-network-contract-v1.md)，要点：

- `ICoCloseable` 只有 `void Close() noexcept` + `bool IsClosed() const noexcept`。`Close()` 由资源 owner/framework manager 主动调用、同步完成物理释放并返回：**返回即该对象拥有的物理资源已释放、后端不会再访问它们**；未发送数据直接丢弃、不做 flush、不重开；**没有「等待关闭完成」的入口**。
- 已删除且不保留兼容壳：`RequestClose`、`WaitClosed`、`ReleaseClosed`、`CloseStatus` 等待结果枚举、`CompletionSignal`、`CancellationToken`/`CancellationSource`（含 `CallOptions.cancel` 字段）、`RuntimeGeneration`/`CurrentRuntimeGeneration`、`Scheduler::Stop()`/restart。
- **请求完成**：业务一次调用内部以 `CoWaiter::WaitWithCallback`（登记等待 → 一次投递回调 → 挂起）+ adapter 路径 `Notify`（可带载荷 `CoEventValue`）唤醒；结果放各模块 operation state。早到响应走 `CoPollEvent::PENDING`；每个请求只在完成/超时/取消/关闭中交付**一次**终态；pending（未发送）请求遇关闭直接丢弃、不发送；已发送请求关闭后业务立即返回终态，迟到响应继续被消费以保持连接对齐但不再交付；不在 infra 内重试；事件回调不得阻塞。
- **分层**：Scheduler 只驱动 FD 就绪与通用唤醒；Connection handler 处理本连接的非阻塞读写、buffer 与关闭；协议（Redis/HTTP/Mongo）各自实现协议与错误映射，不共享万能请求接口。
- 「运行时是否在跑」用初始化状态 `Scheduler::IsInitialized()`，不再有代际语义。

> 状态：新的关闭/进程寿命契约**已合入 main**（close/stop 迁移 `02567ed`）：`ICoCloseable.hpp`、`NetworkTypes.hpp`、`src/detail/IoSupport.hpp` 与模块头/实现、tests、examples 的公开调用链均已迁到该契约（`RequestClose`/`WaitClosed`/`ReleaseClosed`/`CloseStatus` 等旧公开入口已删除）。跨线程同步 `Close()` 的实现口径见 [0005 §6.4](docs/decisions/0005-co-io-adapter-contract-v1.md)；本 README 只陈述契约与迁移状态，不据此宣称同步 `Close()` 实现已被完整验收，未覆盖项以各决策文档与 Issue 为准。

## 依赖方向与公共边界

- `bbt-framework → bbtools-infra → bbtools-coroutine` 为单向依赖；本仓不依赖 framework。
- `bbtools-core` 已冻结不再维护：coroutine 已脱离 `libbbt_core` 自闭环；本仓在 coroutine 拆分后同样不再引入 `bbt_core` 依赖（CMake 中 `BBT_CORE_SOURCE_DIR` 仅兼容 coroutine 未完全拆分前的过渡形态）。
- 公共头位于 `include/bbt/infra/`；各模块为独立 CMake target，不强制同时构建或链接。
- Redis/Mongo 旧公共头 `CoRedisCli.hpp`/`CoMongoCli.hpp` 仍保留兼容入口；新公共 API 目录（`include/bbt/infra/redis/`、`include/bbt/infra/mongo/`）按决策 0006 拆分后逐步迁移。

## 独立消费

本仓当前**不提供安装包或导出 CMake 包**。消费方式为**源码树内 add_subdirectory** 或**直接在本仓构建后链接静态库**。

### 源码接入（推荐）

```cmake
# 消费者 CMakeLists.txt 中
set(BBT_COROUTINE_SOURCE_DIR "<bbtools-coroutine 源码树>")
add_subdirectory(<bbtools-infra 源码树> bbtools-infra)

target_link_libraries(your_target PRIVATE bbt::infra_http)   # 按需选择
```

`bbt_add_source_dependency` 会遮蔽上游单测/示例开关、隔离 `CMAKE_POLICY_VERSION_MINIMUM`，并把上游头路径补成目标级接口。

### 本仓构建

```bash
cmake -S . -B build \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  [-DBBT_CORE_SOURCE_DIR=<bbtools-core 源码树>]          # coroutine 未完全拆分时需要
cmake --build build --parallel $JOBS
ctest --test-dir build --output-on-failure
```

### 可选模块依赖

| 模块 | 额外 CMake 变量 | 说明 |
|------|----------------|------|
| Redis | `BBT_HIREDIS_PREFIX` | hiredis 私有安装前缀；未设置时跳过 `bbt_infra_redis` |
| Mongo | `BBT_MONGOCXX_PREFIX` + `BBT_MONGOC_PREFIX` | mongo-cxx-driver 与 mongo-c-driver 私有前缀；须成对设置 |

### 示例

最小真实消费示例见 [examples/README.md](examples/README.md)：
- `Example_http_loopback`：无外部依赖的 HTTP loopback 完整生命周期
- `Example_config_consume`：配置读取 → 上层校验 → 接受/拒绝（含格式错误不发布伪成功）
- `Example_redis_mongo_shape`：Redis/Mongo 调用形态与错误路径演示（不连真实服务）

另有独立消费工程 `examples/config_consumer`（只链接 `bbt::infra_config`，不被本仓构建引用），
用于从仓外视角复核模块 target 可独立消费；命令见该目录 README 与 [docs/config-watch-v1.md](docs/config-watch-v1.md)。

## CI 覆盖边界

- PR/push CI 中**本仓定义的 job** 跑在 hosted `ubuntu-24.04`（`arc-s4-infra` 仅存历史含义）；跨仓 reusable callee（分类/计划、编译 & ctest 汇聚）按其固定 `ubuntu-latest` 运行。对本构建中已注册的全部测试执行 `ctest -j1`。
- CI 在 `prepare-deps` 阶段用 `scripts/prepare_resource_deps.sh` 以固定 tag+SHA 源码构建 hiredis/mongoc/mongocxx 私有前缀，并在 `release` job 显式断言 `redis.unit`/`mongo.unit` 已注册后纳入 `ctest -j1`；故 CI 覆盖 HTTP/transport/contract 面**以及 Redis/Mongo unit**。真实 run 证据见 [docs/ci/infra-ci-v1.md](docs/ci/infra-ci-v1.md)。
- Redis/Mongo **live 容器验收**（`redis.live`、`mongo.live`、`redis.cotcp_binding.live`）**未纳入 CI**：按环境变量 `BBT_TEST_REDIS_ADDR` / `BBT_TEST_MONGO_URI` 驱动，未提供时如实标 Skipped。
- CI 固定依赖到上游 `main`/`master` HEAD，不代表对任意历史 SHA 的回溯兼容。

## 构建约束

本地验证边界与并发限制按 AGENTS.md 执行：必走冒烟 + 本次开发功能单测 + 直接耦合功能单测；新 configure 加 `-DCMAKE_CXX_COMPILER_LAUNCHER=ccache`；本地并发取 `JOBS=$(( $(nproc) / 4 ))`；禁止 bare `--parallel` 或 `ninja -j$(nproc)`。
