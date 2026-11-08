# bbtools-infra

C++17 基础接入层：TCP 网络执行域、HTTP、Redis、RPC、Mongo 与基础动态配置。依赖方向为 `bbt-framework → bbtools-infra → bbtools-coroutine`，不承载 App/Service、业务 schema 或配置治理。

## 模块状态

| 模块 | 公开 target | 当前边界 |
|---|---|---|
| TCP 网络底座 | `bbt::infra_net` | Processer driver 推进；Engine + ICoConnection 的建连、收发、关闭与回收 |
| HTTP Client / Server | `bbt::infra_http_net` | 唯一现役 HTTP 实现；连接池、HTTP/1.1、流式响应及服务端停止/排空；见 [HTTP 指南](docs/guides/http-net.md) |
| Redis | `bbt::infra_redis_net` | 单连接与有界池，RESP2、KV/Hash、AUTH/SELECT；见 [Redis 指南](docs/guides/redis-net.md) |
| RPC | `bbt::infra_rpc_net` | 唯一 V2 会话面（`SessionFrame` + u32be + Hello/HelloAck），锁定 protobuf 3.21.12；不依赖 HTTP 开关；见 [RPC 指南](docs/guides/rpc-net.md) |
| Consul 注册发现 adapter | `bbt::infra_consul` | Consul agent HTTP API 的最小注册、注销与健康发现；复用 `bbt::infra_http_net`，无 Consul SDK；契约与真后端验证见 [Consul 指南](docs/guides/consul-registry.md) |
| Mongo | `bbt::infra_mongo` | 保留 bounded worker bridge、BSON 与 owner/句柄；不依赖 TCP/HTTP adapter |
| 动态配置 | `bbt::infra_config` | 内存源、本地文件源、watch；私有关闭支撑；见 [config 指南](docs/guides/config-watch.md) |
| RPC over TCP / MCP | 按模块开关导出 | 可选模块，不属于默认构建；接口和依赖以各模块 CMake 为准 |
| 高层 UDP | 无 | 显式 include 拒绝；NativeSocket datagram 原语与测试保留 |

旧 HTTP/TCP/UDP/transport/Redis 实现、旧扁平网络入口与目标均已移除，没有兼容 transport 副本。旧消费者必须迁移到模块内公开头；关闭语义不能机械替换：HTTP 服务端使用 StopAccepting/Drain/Close/WaitStopped，Redis Close 是逻辑封口，宿主负责最终排空。公共值契约 `Result`、`NetworkTypes`、`ICoObject`、`ICoNetwork`、`ICoCloseable`、`Codec`、`NetError` 保留；Mongo 的 `CoMongoCli.hpp` 仍有内部消费者。

## 构建与消费

源码和公共头统一位于 `bbt/infra/`，私有头放模块 `detail/`。消费者只链接公开 target，include root 由 target 传播。

```bash
cmake -S . -B <build> -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DBBT_COROUTINE_SOURCE_DIR=<cmake/coroutine.sha 对应干净检出>
cmake --build <build> --target http_net_example --parallel 2
ctest --test-dir <build> -j1 -R '^example_http_net_consumer$' --output-on-failure
```

默认启用 net、HTTP、config；Mongo 仅在显式 driver 前缀可用时产生目标。Redis/RPC 默认关闭，开启后缺 hiredis/protobuf 前缀会配置失败，不会静默跳过。Redis 使用 `-DBBT_INFRA_REDIS_NET=ON -DBBT_HIREDIS_PREFIX=<hiredis 1.4.0 私有前缀>`；RPC 使用 `-DBBT_INFRA_RPC_NET=ON -DBBT_PROTOBUF_PREFIX=<锁定 3.21.12 私有前缀>`。Mongo 的 `BBT_MONGOCXX_PREFIX` / `BBT_MONGOC_PREFIX` 必须成对设置。`BBT_INFRA_NETWORK_ONLY=ON` 只接入 net 和选中的协议，HTTP 如不需要可显式关闭。

## 验证与文档

- [示例](examples/README.md)：公开 target 的独立消费。
- [测试策略](docs/guides/testing.md)与 [CI](docs/guides/ci.md)：PR smoke + 直接耦合 focused；真实 Redis/Mongo 由独占 live 环境执行，不以 skip 冒充通过。
- [开发规范](docs/guides/development.md)、[代码约定](docs/conventions.md)、[Agent 规范](AGENTS.md)。
- API 文档本地导出：`scripts/docs/build_docs.sh`（需 `doxygen`、`dot`；约定 Doxygen 1.15.0，版本不一致只告警；输出目录默认 `build-docs/`，也可用 `DOCS_OUT` 指定 `build/` 或 `build-*/` 下的路径，均在忽略范围；产物含 `site/` HTML 与 `ai/` 分层 Markdown）。CI 导出尚未接入。
- [网络执行域契约](docs/architecture/gnet-network-execution.md)与 [历史决策](docs/decisions/)；ADR 正文保留原决策，不代表旧 API 仍可消费。
- [Consul 指南](docs/guides/consul-registry.md)：agent HTTP adapter 的契约、错误映射、关闭语义与真后端验证。

本地使用 `flock /tmp/bbt-build.lock` 串行重任务、ccache、`--parallel 2`、`ctest -j1`；只跑冒烟、本次功能及直接耦合测试，完整回归由 CI 执行。跨仓消费者切换和独立审查须另有实测，不由本仓构建通过替代。
