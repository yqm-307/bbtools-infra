# bbtools-infra

C++17 基础接入层：TCP 网络执行域、HTTP、Redis、RPC、Mongo 与基础动态配置。依赖方向为 `bbt-framework → bbtools-infra → bbtools-coroutine`，不承载 App/Service、业务 schema 或配置治理。

## 模块状态

| 模块 | 公开 target | 当前边界 |
|---|---|---|
| TCP 网络底座 | `bbt::infra_net` | Processer driver 推进；Engine + ICoConnection 的建连、收发、关闭与回收 |
| HTTP Client / Server | `bbt::infra_http_net` | 唯一现役 HTTP 实现；连接池、HTTP/1.1、请求级派发（每连接 reader/parser + 请求任务 + 单顺序 writer）与流式响应；见 [HTTP 指南](docs/guides/http-net.md) |
| Redis | `bbt::infra_redis_net` | 单连接与有界池，RESP2、KV/Hash、AUTH/SELECT；见 [Redis 指南](docs/guides/redis-net.md) |
| RPC | `bbt::infra_rpc_net` | 唯一 V2 会话面（`SessionFrame` + u32be + Hello/HelloAck），锁定 protobuf 3.21.12；不依赖 HTTP 开关；见 [RPC 指南](docs/guides/rpc-net.md) |
| Consul 注册发现 adapter | `bbt::infra_consul` | agent HTTP API 注册、注销与健康发现；专属 HTTP Client；见 [Consul 指南](docs/guides/consul-registry.md) |
| Mongo | `bbt::infra_mongo` | bounded native worker bridge、BSON、CoMongoDb owner 与 CoMongoColl 句柄；关闭等待由同一 Close 输入控制 |
| 动态配置 | `bbt::infra_config` | 内存源、文件源与 watch；Close 控制 callback 排空和轮询退出；见 [config 指南](docs/guides/config-watch.md) |
| RPC over TCP / MCP | 按模块开关导出 | 可选模块，不属于默认构建；接口和依赖以各模块 CMake 为准 |
| 高层 UDP | 无 | 显式 include 拒绝；NativeSocket datagram 原语与测试保留 |

旧 HTTP/TCP/UDP/transport/Redis 实现与旧扁平网络入口已移除，不保留转发兼容副本。现役 owner 的公开关闭操作统一为 `Close(CloseOptions)`；模式、支持域、物理凭据与 Engine 能力限制见 [关闭契约](docs/architecture/close-options-contract.md)。同步低层 `ICoCloseable` 与 socket 半关闭仍保留各自职责。Mongo 公共入口为 `mongo/CoMongoDb.hpp`、`mongo/CoMongoColl.hpp` 和 `mongo/MongoTypes.hpp`。

执行域与入口告警：受管协程内不得阻塞 OS 线程、等待 native CV/join/`future`；阻塞型第三方调用只能留在 adapter 的 native 执行域。示例与消费骨架只写 `bbtco_main`（`BBTCO_MAIN*` 生成 native main），不得自行 `g_scheduler->Start`、建 runner 线程或 `future.wait/get`；Engine 只由应用入口 native host hook 在主线程调用一次 `Run`，主任务用 `Engine::CoWaitRunning` 等 Running，`EngineLifecycle` 与其 runner/join 路径已删除。`examples/http_net` 仍待迁移到该入口，其余示例仍是旧入口，新增代码不得以其为先例。规则详见 [代码约定 §11](docs/conventions.md)。

## 构建与消费

源码和公共头统一位于 `bbt/infra/`，私有头放模块 `detail/`。消费者只链接公开 target，include root 由 target 传播。依赖默认使用所选本地源码的当前内容；指定版本与设计要求见[依赖版本选择](docs/guides/development.md#依赖版本选择)。

```bash
cmake -S . -B <build> -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DBBT_COROUTINE_SOURCE_DIR=<本地 coroutine Git 源码目录>
cmake --build <build> --target http_net_example --parallel 2
ctest --test-dir <build> -j1 -R '^example_http_net_consumer$' --output-on-failure
```

默认启用 net、HTTP、config；Mongo 仅在显式 driver 前缀可用时产生目标。Redis/RPC 默认关闭，开启后缺 hiredis/protobuf 前缀会配置失败，不会静默跳过。Redis 使用 `-DBBT_INFRA_REDIS_NET=ON -DBBT_HIREDIS_PREFIX=<hiredis 1.4.0 私有前缀>`；RPC 使用 `-DBBT_INFRA_RPC_NET=ON -DBBT_PROTOBUF_PREFIX=<锁定 3.21.12 私有前缀>`。Mongo 的 `BBT_MONGOCXX_PREFIX` / `BBT_MONGOC_PREFIX` 必须成对设置。`BBT_INFRA_NETWORK_ONLY=ON` 只接入 net 和选中的协议，HTTP 如不需要可显式关闭。

## 验证与文档

- [示例](examples/README.md)：公开 target 的独立消费。
- [测试策略](docs/guides/testing.md)与 [CI](docs/guides/ci.md)：PR smoke + 直接耦合 focused；真实 Redis/Mongo 由独占 live 环境执行，不以 skip 冒充通过。
- [开发规范](docs/guides/development.md)、[代码约定](docs/conventions.md)、[Agent 规范](AGENTS.md)。
- [bbtools-pub README](https://github.com/yqm-307/bbtools-pub/blob/main/README.md)：跨仓设计、编码、Review 与用例设计规范入口（`third-party/bbtools-pub` 固定 submodule，clone 后 `git submodule update --init --recursive`）。
- API 文档本地导出：`scripts/docs/build_docs.sh`（需 `doxygen`、`dot`；约定 Doxygen 1.15.0，版本不一致只告警；输出目录默认 `build-docs/`，也可用 `DOCS_OUT` 指定 `build/` 或 `build-*/` 下的路径，均在忽略范围；产物含 `site/` HTML 与 `ai/` 分层 Markdown）。CI 导出尚未接入。
- [网络执行域契约](docs/architecture/gnet-network-execution.md)与 [历史决策](docs/decisions/)；ADR 正文保留原决策，不代表旧 API 仍可消费。
- [Consul 指南](docs/guides/consul-registry.md)：agent HTTP adapter 的契约、错误映射、关闭语义与真后端验证。

本地使用 `flock /tmp/bbt-build.lock` 串行重任务、ccache、`--parallel 2`、`ctest -j1`；只跑冒烟、本次功能及直接耦合测试，完整回归由 CI 执行。跨仓消费者切换和独立审查须另有实测，不由本仓构建通过替代。
