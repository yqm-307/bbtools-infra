# bbtools-infra

统一的现代 C++ 工具层与基础接入层：HTTP、CoTCP/CoUDP transport、Redis、Mongo 及分布式运行时基础能力。

本仓不承载 `bbt-framework` 的 App、Service 或业务配置编排。infra 提供可独立消费的第三方适配、执行域、错误映射、资源生命周期和基础动态配置机制；上层分布式服务框架负责配置治理、服务发现、灰度、业务 schema 与动态变更编排。

稳定架构边界见 [0006：infra 基础层与动态配置边界](docs/decisions/0006-infra-foundation-and-dynamic-config.md)。新增 Redis/Mongo 公共 API 使用 `bbt::infra::redis` 与 `bbt::infra::mongo`，不使用新的扁平模块命名空间；公共头不泄漏 hiredis、mongocxx、bsoncxx 或底层线程类型。

## 模块交付状态

| 模块 | CMake target | 状态 | 说明 |
|------|-------------|------|------|
| HTTP（client + server + NetworkRuntime） | `bbt::infra_http` | 已实现，单测覆盖中 | Beast/Asio 异步；配额、关闭排空、子对象反登记等可靠性边界仍在 Issue 中推进 |
| CoTCP / CoUDP transport | `bbt::infra_tcp`、`bbt::infra_udp` | 已实现，行为验收未完成 | 基础收发与受管工厂可用；`max_inflight` 门禁、生命周期完整验收归 Issue #31/#32 |
| Redis client | `bbt::infra_redis` | 已实现最小切片 | hiredis async + strand；单连接语义；`bbt::infra::redis` 公共 API 目录尚未拆分 |
| Mongo client | `bbt::infra_mongo` | 已实现 owner + 句柄形态 | 同步 driver + worker bridge；`mongo::CoMongoDb` 为资源 owner，`mongo::CoMongoColl` 为集合句柄；`include/bbt/infra/mongo/` 公共目录已建，剩余 API 按决策 0006 逐步迁移 |
| 基础动态配置 | `bbt::infra::config` | 规划中 | 版本化 snapshot、配置源适配、watch、重连、去重与关闭；具体实现待 Issue #35 |
| RPC / MCP | — | 未开始 | 归对应 Issue 推进，当前无实现 |

已实现能力的单测与示例均可独立构建运行，见下文「独立消费」与 [examples/README.md](examples/README.md)。

## I/O 执行边界

[CoTCP/CoUDP 与第三方 Binding 规格](docs/decisions/0005-co-io-adapter-contract-v1.md) 定义目标执行模型：proc 执行收发与协议，sche 只检测就绪并唤醒。

当前 HTTP 为 Asio/Beast 异步推进，Redis 为 hiredis async + strand，Mongo 为同步 driver + worker bridge。CoTCP/CoUDP 已交付基础实现，但完整生命周期与 `max_inflight` 门禁验收仍在 Issue #31/#32 中推进，尚未完成全部规格目标。

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
- `Example_redis_mongo_shape`：Redis/Mongo 调用形态与错误路径演示（不连真实服务）

## CI 覆盖边界

- PR/push CI（`arc-s4-infra` runner）对本构建中已注册的全部测试执行 `ctest -j1`；当前注册的测试集随 `BBT_HIREDIS_PREFIX`/`BBT_MONGOCXX_PREFIX` 是否提供而变。
- 未提供 Redis/Mongo 前缀时，`redis.unit`/`redis.live`/`mongo.unit`/`mongo.live` 不注册、不运行；CI 镜像默认不带 hiredis/mongocxx 前缀，故当前 CI 实际覆盖 HTTP/transport/contract 面，不覆盖 Redis/Mongo。
- Redis/Mongo **live 容器验收**（`redis.live`、`mongo.live`）**未纳入 CI**：按环境变量 `BBT_TEST_REDIS_ADDR` / `BBT_TEST_MONGO_URI` 驱动，未提供时跳过。
- CI 固定依赖到上游 `main`/`master` HEAD，不代表对任意历史 SHA 的回溯兼容。

## 构建约束

本地验证边界与并发限制按 AGENTS.md 执行：必走冒烟 + 本次开发功能单测 + 直接耦合功能单测；新 configure 加 `-DCMAKE_CXX_COMPILER_LAUNCHER=ccache`；本地并发取 `JOBS=$(( $(nproc) / 4 ))`；禁止 bare `--parallel` 或 `ninja -j$(nproc)`。
