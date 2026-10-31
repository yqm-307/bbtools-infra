# bbtools-infra

统一的现代 C++ 工具层与基础接入层：HTTP、R2 TCP 网络底座、RPC、CoTCP/CoUDP transport、Redis、Mongo 及分布式运行时基础能力。

本仓不承载 `bbt-framework` 的 App、Service 或业务配置编排。infra 提供可独立消费的第三方适配、执行域、错误映射、资源生命周期和基础动态配置机制；上层分布式服务框架负责配置治理、服务发现、灰度、业务 schema 与动态变更编排。

稳定架构边界见 [0006：infra 基础层与动态配置边界](docs/decisions/0006-infra-foundation-and-dynamic-config.md)。新增 Redis/Mongo 公共 API 使用 `bbt::infra::redis` 与 `bbt::infra::mongo`，不使用新的扁平模块命名空间；公共头不泄漏 hiredis、mongocxx、bsoncxx 或底层线程类型。

## 模块交付状态

| 模块 | CMake target | 状态 | 说明 |
|------|-------------|------|------|
| HTTP（client + server + NetworkRuntime） | `bbt::infra_http` | 已实现，单测覆盖中 | Beast/Asio 异步；配额、关闭排空、子对象反登记等可靠性边界仍在 Issue 中推进 |
| CoTCP / CoUDP transport | `bbt::infra_tcp`、`bbt::infra_udp` | 已实现 | 基础收发与受管工厂可用；Runtime 级 `max_inflight` 在途门禁已实现并合入；跨平台/非 Linux、公网 DNS 与部分 lifecycle 直接证据仍未覆盖 |
| Redis client | `bbt::infra_redis` | 已实现最小切片 | 旧入口仍使用 CoTCP owner binding（`CoRedisCliImpl` + 窄 `RedisCotcpConn`）；原同步关闭与排队配置契约保留 |
| Redis net 窄入口 | `bbt::infra_redis_net` | 隔离候选，待独立审查 | `bbt::infra::redis::CoRedisCli`（共享 Engine、单连接/单活动操作、非阻塞 Close）与 `CoRedisPool`（固定大小、命令内借还、有界 FIFO 排队）；数据面含 SET 条件/过期写（NX/XX、PX）、PEXPIRE/PTTL 三态、INCR 族、Hash 与连接级 AUTH/SELECT 握手；契约与验收入口见 [docs/guides/redis-net.md](docs/guides/redis-net.md)。framework 消费者未切换 |
| HTTP net Client / Server | `bbt::infra_http_net` | 候选已实现，独立审查通过，待远端 CI 验证 | HTTPX 形态原生有栈协程 Client + 有界连接池；Server 提供 owning request/handler、顺序 keepalive session、StopAccepting/Drain/Close/WaitStopped 与物理回收额度。Beast 只编解码，网络只经 net 公开 Co 接口。https/DNS/json/form/流式/重定向/HTTP2 未实现，framework 消费者未切换。契约与构建见 [docs/guides/http-net.md](docs/guides/http-net.md) |
| Mongo client | `bbt::infra_mongo` | 已实现 owner + 句柄形态；BSON/async 与多文档数据面入口为待审查增量 | 同步 driver + worker bridge、owner/句柄归属与关闭语义不变；`mongo::CoMongoDb` 为资源 owner，`mongo::CoMongoColl` 为集合句柄；契约见 [0007](docs/decisions/0007-mongo-owner-handles.md) |
| 基础动态配置 | `bbt::infra_config` | 最小切片已合入主线，独立消费已验证 | `bbt::infra::config`；版本化不可变快照 + 内存源 + 真实本地文件源 + watch。**未交付**：远程 provider 与真实重连、业务 schema 校验、客户端资源热切换。契约/命令/未覆盖矩阵见 [docs/guides/config-watch.md](docs/guides/config-watch.md) |
| RPC core（新 HTTP 栈） | `bbt::infra_rpc_net` | 候选已实现，独立审查通过，待远端 CI 验证 | 消费新 `bbt::infra::http::Client/Server` 的 RPC core：版本化 Protobuf RpcEnvelope wire profile 与 RpcPayload 逐位复用；请求关联、deadline local-min、错误透传、畸形报文错误信封、`StopAccepting/Drain/Close/WaitStopped`+`IsClosed`。https/DNS/双进程/framework 消费切换未覆盖。契约、构建与验证命令见 [docs/guides/rpc-net.md](docs/guides/rpc-net.md)；旧 runtime 契约的历史记录见 [0009](docs/decisions/0009-rpc-runtime.md) |
| R2 TCP 网络底座（Processer driver） | `bbt::infra_net` | 已合入主线 | 自有 main/sub Asio context 由已登记 Processer driver 推进，不建网络 I/O 线程；协程消费面为 `ICoConnection` 成员接口（`Engine::CoAccept`/`Engine::CoDial` → `ConnectionResult{error, connection, reclaim}`，`conn.Read`/`Write`/`AsyncWrite`/`RequestClose`/`WaitClosed`/`Status`）；旧 `CoRead/CoWrite/CoAccept/CoDial` 自由函数与 `Client/EventHandler/Action` 已删除。设计与契约真源见 [网络执行域设计](docs/architecture/gnet-network-execution.md) 与 [0010](docs/decisions/0010-gnet-network-foundation.md)、[0012](docs/decisions/0012-icoconn-member-interface.md) |
| RPC over TCP | `bbt::infra_rpc_tcp` | 未交付（开关骨架已接线） | `BBT_INFRA_RPC_TCP`（默认 OFF）已接线；实现目录 `bbt/infra/rpc_tcp/` 尚未合入，开关 ON 时 configure fail-closed。长度前缀 TCP 不需要第三方前缀，不拉入 HTTP |
| MCP server | — | 未交付（草稿候选） | C++17 server-only 的协议核心、tools/resources/prompts 与 stdio/Streamable HTTP 候选保存在 [草稿 PR #80](https://github.com/yqm-307/bbtools-infra/pull/80)，未合入 main，当前主线无可消费 target；`BBT_INFRA_MCP` / `BBT_INFRA_MCP_SCHEMA`（默认 OFF）开关已接线，`bbt/infra/mcp/` 尚未合入，开关 ON 时 configure fail-closed。目标、阻断与 R1–R11 / S1–S24 验收由 [Issue #85](https://github.com/yqm-307/bbtools-infra/issues/85) 维护；完整 client SDK 与 OAuth 不在首版范围。实时冲突和检查状态以该 PR 为准 |

已实现能力的单测与示例均可独立构建运行，见下文「独立消费」与 [examples/README.md](examples/README.md)。

## 文档地图

| 位置 | 内容 |
|---|---|
| [docs/architecture/gnet-network-execution.md](docs/architecture/gnet-network-execution.md) | R2 TCP 网络底座执行域、所有权与关闭契约（真源） |
| [docs/decisions/](docs/decisions/) | ADR：0002 通信层契约、0003 Redis/hiredis、0004 Mongo/mongocxx、0005 CoTCP/CoUDP 与第三方 Binding、0006 infra 边界与动态配置、0007 Mongo owner/句柄、0008 出站请求阶段与 OutcomeUnknown、0009 RPC runtime（旧）、0010 gnet 网络基础、0011 HTTP client 失败收口、0012 协程连接成员接口 |
| [docs/guides/development.md](docs/guides/development.md) | 开发与验收指南（隔离、接口职责、测试口径、本地验证、审查交付） |
| [docs/guides/config-watch.md](docs/guides/config-watch.md) | `bbt::infra::config` 快照/文件源/watch 契约与未覆盖矩阵 |
| [docs/guides/redis-net.md](docs/guides/redis-net.md) | Redis net 窄入口契约、构建消费与验证边界 |
| [docs/guides/http-net.md](docs/guides/http-net.md) | HTTP net Client/Server 契约、构建与边界 |
| [docs/guides/rpc-net.md](docs/guides/rpc-net.md) | RPC core（新 HTTP 栈）契约、构建与测试覆盖 |
| [docs/dependencies/](docs/dependencies/) | 第三方依赖来源与 provenance（gnet、protobuf、coroutine pin、resource prefixes） |
| [docs/guides/ci.md](docs/guides/ci.md) | CI job 与 run 证据说明 |
| [examples/README.md](examples/README.md) | 可运行示例清单与消费方式 |

## I/O 执行边界

`bbt::infra_net` 的 R2 TCP 契约已合入主线（PR #84，合入提交 `99d0821a`）：infra 拥有自有 main/sub Asio context 与网络资源，每个 Reactor 自动绑定一个空闲已登记 Processer driver 槽；不新增网络 I/O 线程。原始 `OnAccept/OnConnect/OnRead/OnWrite/OnClose` 回调在 owner 非业务协程边界短、非阻塞、不可挂起地运行；协程同步 API 只用既有 CoWaiter/Chan 做薄封装。接收 ring 是并发安全的数据真源，`OnRead` 仅通知；提交接纳、本地发送完成和业务回包严格分开，普通 EOF/错误/等待超时不自动关连接或重发。

停止接纳（`RequestStopAccepting`/`AcceptingStopped`）与物理回收观察（`ReclaimReceipt`/`Connection::Released()`、`IsReleased()`）的契约并入 [网络执行域设计 §8.1](docs/architecture/gnet-network-execution.md#81-停止接纳与物理回收观察)。

[CoTCP/CoUDP 与第三方 Binding 规格](docs/decisions/0005-co-io-adapter-contract.md) 的“proc 执行收发与协议，sche 只检测就绪并唤醒”描述旧路径，不覆盖新 net。HTTP/Redis/Mongo/config/RPC 仍走各自既有路径，新 net 仅替换旧 TCP/协程桥接 API，不表示这些消费者已迁移。

当前 HTTP 为 Asio/Beast 异步推进，Redis 为 CoTCP owner binding，Mongo 为同步 driver + worker bridge。

## 资源关闭与请求完成契约（进程寿命运行时）

coroutine 运行时按**进程寿命**存在、不参与业务资源收口（不再有 `Scheduler::Stop()`/restart 与运行时代际）。新 net 的关闭语义见 [关闭契约](docs/architecture/gnet-network-execution.md#6-网络关闭不是-runtime-停机)：`RequestClose` 仅封口并请求控制域收口，`Run` 返回才确认网络资源、callback 与同代解绑完成；该约束取代旧 Close(opt)，但不改**旧模块** [0002](docs/decisions/0002-co-network-contract.md) 契约及状态：

- `ICoCloseable` 只有 `void Close() noexcept` + `bool IsClosed() const noexcept`；`Close()` 由资源 owner/framework manager 主动调用、同步完成物理释放并返回：返回即该对象拥有的物理资源已释放、后端不会再访问；未发送数据直接丢弃、不 flush、不重开；没有「等待关闭完成」的入口。
- **请求完成**：业务一次调用内部以 `CoWaiter::WaitWithCallback`（登记等待 → 一次投递回调 → 挂起）+ adapter 路径 `Notify` 唤醒；结果放各模块 operation state；每个请求只在完成/超时/取消/关闭中交付**一次**终态；不在 infra 内重试；事件回调不得阻塞。
- **分层**：Scheduler 只驱动 FD 就绪与通用唤醒；Connection handler 处理本连接的非阻塞读写、buffer 与关闭；协议（Redis/HTTP/Mongo）各自实现协议与错误映射。
- 「运行时是否在跑」用初始化状态 `Scheduler::IsInitialized()`，不再有代际语义。

> 状态：新的关闭/进程寿命契约已合入 main（close/stop 迁移 `02567ed`）。跨线程同步 `Close()` 的实现口径见 [0005 §6.4](docs/decisions/0005-co-io-adapter-contract.md)；本 README 只陈述契约与迁移状态，不据此宣称同步 `Close()` 实现已被完整验收，未覆盖项以各决策文档与 Issue 为准。

## 依赖方向与公共边界

- `bbt-framework → bbtools-infra → bbtools-coroutine` 为单向依赖；本仓不依赖 framework。
- `bbtools-core` 已冻结不再维护：coroutine 已脱离 `libbbt_core` 自闭环；本仓同样不再引入 `bbt_core` 依赖（`BBT_CORE_SOURCE_DIR` 仅兼容过渡形态）。
- 公共头位于 `include/bbt/infra/`；各模块为独立 CMake target，不强制同时构建或链接。新 net/Redis 模块例外把公共头与实现并置于 `bbt/infra/net/`、`bbt/infra/redis/`。
- Redis/Mongo 旧公共头 `CoRedisCli.hpp`/`CoMongoCli.hpp` 仍保留兼容入口；新公共 API 目录按决策 0006 拆分后逐步迁移。

## 独立消费

本仓当前**不提供安装包或导出 CMake 包**。消费方式为**源码树内 add_subdirectory** 或**直接在本仓构建后链接静态库**。

### 源码接入（推荐）

```cmake
set(BBT_COROUTINE_SOURCE_DIR "<bbtools-coroutine 源码树>")
add_subdirectory(<bbtools-infra 源码树> bbtools-infra)
target_link_libraries(your_target PRIVATE bbt::infra_http)   # 按需选择
```

`bbt_add_source_dependency` 会遮蔽上游单测/示例开关、隔离 `CMAKE_POLICY_VERSION_MINIMUM`，并把上游头路径补成目标级接口。CI 与本地 configure 都把 coroutine 消费固定到 `cmake/coroutine.sha` 的不可变 commit（见 [coroutine pin provenance](docs/dependencies/bbtools-coroutine-pin.provenance.md)）：`bbt_coroutine_pin_verify()` 要求该检出 HEAD==pin 且干净；非 git 树拒绝。

### 本仓构建

```bash
cmake -S . -B build \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  [-DBBT_CORE_SOURCE_DIR=<bbtools-core 源码树>]
cmake --build build --parallel 2
ctest --test-dir build --output-on-failure
```

network-only 闭包（`-DBBT_INFRA_NETWORK_ONLY=ON`）在旧目标、数据库/protobuf 包发现之前分流；新协议闭包开关 `BBT_INFRA_HTTP_NET` / `BBT_INFRA_REDIS_NET` / `BBT_INFRA_RPC_NET` 的具体构建命令见各 [guides](docs/guides/)。

### 可选模块依赖

| 模块 | 额外 CMake 变量 | 说明 |
|------|----------------|------|
| Redis | `BBT_HIREDIS_PREFIX` | hiredis 私有安装前缀；未设置时跳过 `bbt_infra_redis` |
| Mongo | `BBT_MONGOCXX_PREFIX` + `BBT_MONGOC_PREFIX` | mongo-cxx-driver 与 mongo-c-driver 私有前缀；须成对设置 |
| RPC over TCP | — | `BBT_INFRA_RPC_TCP=ON` 不需要前缀，依赖 net 基础；NETWORK_ONLY 下拒绝；实现目录未合入时 configure fail-closed |
| MCP | — | `BBT_INFRA_MCP=ON` 核心不需要前缀；NETWORK_ONLY 下拒绝；实现目录未合入时 configure fail-closed |
| MCP schema | `BBT_JSONCONS_PREFIX` + `BBT_ICU_PREFIX` | `BBT_INFRA_MCP_SCHEMA=ON` 须同时给两个前缀且依赖 `BBT_INFRA_MCP=ON`；缺前缀 fail-closed；validator 启用须先过 provenance 审批 |

### 示例

最小真实消费示例见 [examples/README.md](examples/README.md)：`Example_http_loopback`、`Example_config_consume`、`Example_redis_mongo_shape`、`mongo_client_example`、`Example_co_net_service`；另有独立消费工程 `examples/config_consumer` 与 `examples/net_foundation_consumer`。

## CI 覆盖边界

- PR/push CI 中**本仓定义的 job** 跑在 hosted `ubuntu-24.04`；对本构建中已注册的全部测试执行 `ctest -j1`。
- CI 在 `prepare-deps` 阶段用 `scripts/prepare_resource_deps.sh` 以固定 tag+SHA 源码构建 hiredis/mongoc/mongocxx 私有前缀，并在 `release` job 显式断言 `redis.unit`/`mongo.unit` 已注册后纳入 `ctest -j1`。
- Redis/Mongo **live 容器验收**纳入 Release job 后段：在独占临时容器中运行正常、停止态失败和重启恢复（A/B/C），另执行 `redis.cotcp_binding.live` 大值回归；不以 Skipped 代替通过，`docs-only` 变更允许跳过整个 Release job。编排与资源边界见 [CI 说明](docs/guides/ci.md) 与对应 PR/Issue 验收评论。
- **TCP network-only 独立 job**：以 `-DBBT_INFRA_NETWORK_ONLY=ON` 独立 Debug 构建显式目标集，`scripts/ci/check_network_tests.py` 拒绝缺项/重复/失败/跳过；不计入 Release/Debug 旧模块结果。
- CI 的 coroutine 消费固定到 `cmake/coroutine.sha`；其它上游按 `master` HEAD 消费。

## 构建约束

本地验证边界与并发限制按 AGENTS.md 执行：必走冒烟 + 本次开发功能单测 + 直接耦合功能单测；新 configure 加 `-DCMAKE_CXX_COMPILER_LAUNCHER=ccache`；本地编译固定 `--parallel 2`；禁止 bare `--parallel` 或 `ninja -j$(nproc)`。详见 [docs/guides/development.md](docs/guides/development.md)。
