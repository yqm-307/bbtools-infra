# 独立消费验证（MCP 核心与 tools×Schema）

本目录是一个**独立 CMake 工程**，不被本仓构建引用（本仓 `examples/CMakeLists.txt` 只注册
`Example_mcp_server` 等示例 target）。它用于从仓外视角证明：`bbt::infra_mcp_core`（MCP
协议核）的公共契约与模块 target 可被独立程序消费，公共头不泄漏 framework 或第三方类型。

本仓当前无 install/export 目标，因此 consumer 以源码方式接入整个 infra 树
（`add_subdirectory`），但只链接 `bbt::infra_mcp_core`（**不链接** transport 面
`bbt::infra_mcp_http`），并关闭 infra 自身单测（`BUILD_TESTING=OFF`）。

## 有界命令（实测见 `scratch/mcp-foundation-4-2a-*/logs/`）

```bash
# bbtools-infra 源码树 = 本仓 worktree；bbtools-coroutine 源码树需另行提供
#（infra 顶层 CMakeLists 校验其有效性；本仓 CI 用的固定路径形如 workspace/deps-coroutine-<sha>）
cmake -S examples/mcp_consumer -B <build>/mcp-consumer \
  -DBBT_INFRA_SOURCE_DIR="$PWD" \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
cmake --build <build>/mcp-consumer --target mcp_consumer --parallel 2
<build>/mcp-consumer/mcp_consumer
```

退出码 0 且输出 `consumer: ALL OK` 即全部检查通过；每项检查有一行 `ok NN <说明>` 输出。
任一检查失败输出 `FAIL NN` 行并以 1 退出——**坏路径拒绝失败视同契约破坏**，不产生伪成功。

## 覆盖边界（不启动 Scheduler 的离线面）

覆盖：
- `McpServer::Create` 严格校验（空 name、零 in-flight 配额、零分页在 Create 即拒）；
- `AdvertisedVersions`（3 个 revision，含 modern 2026-07-28）；
- 注册面合法/坏路径：重复 uri、空 uri、空 handler、重复 prompt name、重复参数名；
- 生命周期：Start 冻结快照（重复 Start / Start 后注册 → InvalidContext）、
  `PublishResourceChanged` 未注册拒绝与已注册 ok、Close 幂等封口（Close 后注册/Start →
  Closed，`IsClosed` 置位）；
- `JsonValue` 严格 Parse（语法/重复键/超深/超 max_bytes 均拒，错误码 ProtocolError）
  与 Serialize 往返；initialize 帧形状字段回读；
- 自有类型原语：`ResourceContents` 构造即校验（空 uri / 非 base64 拒）、`Content`、
  `InputRequired` 双空拒、`RequestId` owned 原值往返、`CancellationView` 置位可观察、
  `RequestContext` 只读回读（id/版本/capabilities/无期限/legacy 输入恒空）。

未覆盖（如实声明，非本 consumer 职责）：
- **wire 请求分派与 transport 装配**：`McpSession::HandleFrame` 是 transport seam
  （公共头注释明确「不属公共契约」，方法不在公共头暴露），仓外 consumer 无法也不应经它
  验证；该面由仓内 `mcp.protocol` / `mcp.stdio_transport` / `mcp.http_transport` /
  `mcp.teardown` 等测试与 4-2B（官方 Python SDK 互通）覆盖。
- 默认 `mcp_consumer` 不测试 tools；工具与真实 Schema 接线由显式变体 `mcp_consumer_tools` 覆盖（见下文）。
- stdio/HTTP 真实 I/O：需要协程运行时与 fd，属在线面，见 `Example_mcp_server`。

## tools×Schema 显式变体

`tools_main.cc` 只用公开头，显式链接 `bbt::infra_mcp_core` 与 `bbt_infra_mcp_schema`。
`MCP_CONSUMER_TOOLS` 在 `add_subdirectory` 前启用选件；缺依赖根 configure 失败，不回退系统安装。

```bash
JOBS=$(( $(nproc) / 4 )); [ "$JOBS" -ge 2 ] || JOBS=2
cmake -S examples/mcp_consumer -B build-consumer-tools \
  -DBBT_INFRA_SOURCE_DIR="$PWD" -DBBT_COROUTINE_SOURCE_DIR="$CO" \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release \
  -DMCP_CONSUMER_TOOLS=ON -DMCP_JSONCONS_ROOT="$JSONCONS" -DMCP_ICU_ROOT="$ICU"
cmake --build build-consumer-tools --target mcp_consumer_tools --parallel "$JOBS"
LD_LIBRARY_PATH="$ICU/usr/lib/x86_64-linux-gnu" build-consumer-tools/mcp_consumer_tools
```

依赖根必须满足 [主 README](../../README.md) 的固定 jsoncons + overlay / ICU 边界；公开依赖重放资产仍缺，
这些命令不代表全新环境已可复现。默认注册模式失败非零；`--stdio` 用 MCP 客户端的独占 pipe 启动，
验证输入拒绝不进 handler、输出 schema、异常脱敏与 MRTR 续调。它的 `boom`/`confirm` 等测试业务不属于公开 server 示例。

`Example_mcp_server` 的 Schema ON 模式才演示完整 echo 三原语；OFF 保持资源/提示词可构建，不宣告 tools。
两个消费者的离线注册检查不等于 SDK、HTTP/SSE、标准 Schema 全合规或 CI 通过。
