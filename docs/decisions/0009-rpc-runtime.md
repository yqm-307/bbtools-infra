# RPC Runtime v1：infra 通用能力与 framework 消费边界

> 状态：**已合入 main**（PR #79，合入提交 `3136d1de5031a2eec55475e770d5ebbadc3c32da`）。
> **2026-10-28 分支状态注记**：本文记录的 `bbt::infra::RpcClient/RpcServer` 运行时
> 壳（HTTP-backed 工厂、`NetworkRuntime::CreateRpcClient/ListenRpc`）已在
> RPC core 迁移分支删除并改建于新 `bbt::infra::http` 栈
> （`bbt::infra_rpc_net`，见 [docs/guides/rpc-net.md](../guides/rpc-net.md)）；本文其余内容
> （wire profile、payload codec、预算与关联语义）仍是现行契约的历史依据。
> framework 侧接线与预算验收已在 Issue #2 的交接评论公开（framework `deps.lock` 固定本仓该 SHA）。第 6 节的本地证据是候选期实测记录，不能替代合入后 PR CI 与 framework 集成的结论。

## 1. 目的与边界

bbtools-infra 提供不依赖 bbt-framework 的 RPC 基础能力：版本化 Protobuf envelope、定址 HTTP 出站、请求/回复关联、结构化错误、剩余预算和 owner 生命周期。bbt-framework 保留 Service/Actor、业务方法表、逻辑路由、RequestContext、治理策略、业务 proto 与上层扩展。

本切片不建设服务发现、负载均衡、自动重试、认证系统、MCP 或动态插件平台；不修改 bbtools-coroutine，不新增 io_context、io_thread 或线程池。

## 2. 公共面

`include/bbt/infra/RpcClient.hpp`：

```cpp
result<RpcEnvelope> Call(const RpcAddress& address,
                         RpcEnvelope request,
                         const CallOptions& options);
```

`include/bbt/infra/RpcServer.hpp`：

```cpp
ListenAddress LocalAddress() const;
void StopAccepting() noexcept;
```

`NetworkRuntime` 新增：

```cpp
result<std::shared_ptr<RpcClient>> CreateRpcClient();
result<std::shared_ptr<RpcServer>> ListenRpc(
    ListenAddress address, RpcHandler handler);
```

所有对象继续实现 `ICoNetwork` 与同步 `ICoCloseable::Close()`。`RpcHandler` 接收 `IncomingCallContext` 和 owning `RpcEnvelope`，由 handler/framework 决定业务分发。

## 3. 实现边界

- `RpcClient` 将 `RpcEnvelope` 映射为正式 `RpcWireEnvelope`，只接受 `http` transport，复用已有 `HttpClient` owner；回包验证 request_id、service、method、request_schema、response_schema。
- `RpcServer` 复用已有 `HttpServer` owner；只接受正式 `POST /rpc` + `application/x-protobuf` body，解析后计算 `min(listener deadline, now + remaining_budget_ms)`，再调用 `RpcHandler`。
- infra handler deadline 的 local-min 不包含 framework 的回复投递预留。framework 接线后，在 local-min 之后保留现有 20ms reserve。
- HTTP/RPC 后端共享 `bbt_infra_http` 的 owner 和执行域；启用 wire 时 `bbt::infra_rpc` 为 STATIC（wire + runtime adapter）并公开依赖 `bbt::infra_http`，未启用 wire 时不导出该 target，`NetworkRuntime` 工厂返回 `RuntimeUnavailable`，不产生假成功。
- 模块边界：RPC 工厂（`NetworkRuntime::CreateRpcClient/ListenRpc`）是 `NetworkRuntime` 的**非虚普通成员**，不是 vtable 槽——装配面固定在公共头，符号只在 RPC 模块（或 http 内 stub）定义。这样只链接 `bbt::infra_http` 的消费者既不拉入 protobuf/生成库，也不会因未定义虚函数留下未解析符号。RPC runtime adapter（`src/rpc/RpcRuntimeImpl.cc`）只组合公开的 `CreateHttpClient/ListenHttp`，不引用 HTTP 内部头；RPC endpoint 只做 authority 局部边界检查（拒绝 scheme/path/query/fragment/userinfo/控制字符），host/port 的完整性仍归公开 `HttpClient` 自己的 URL 校验，不复制 HTTP parser。未启用 wire 时同一 TU 的 stub 分支编入 HTTP target。
- 容量目前继承 HTTP owner：同一 runtime 的 RPC/HTTP 出站共享连接/in-flight 配额；入站同样消耗既有 HTTP 连接/handler 槽，不是 RPC 每个包装独享额度。包装无独立可控物理资源，底层 HTTP owner 执行 Close；如以后增加包装状态必须同步补 owner 收口。
- HTTP 层按冻结的 0008 决策从 `Error::request_phase` 区分真正回复丢失的 `OutcomeUnknown` 与确定的 `ProtocolError`。RPC 原样透传错误与物理原因，不在上层重新覆盖 `domain_code` 或将所有提交后错误升级为未知结果。
- 服务 handler 返回的关联字段必须与请求一致；错误由 wire error envelope 返回，不把错误当成功 payload。

## 4. 迁移表

| 现有位置 | 处理 | 原因 |
|---|---|---|
| `bbtools-infra/bbt/infra/rpc/RpcWire.hpp` / `bbt/infra/rpc/RpcWire.cc` | 保留为 wire 唯一真源，并由 runtime adapter 调用 | #8 已冻结 body profile；不复制字节规则 |
| `bbt-framework/framework/src/host/RpcHttpBridge.cc` 的正式 ProtoWireV1 发送 | 后续改为调用 infra `RpcClient` | 通用发送、wire、关联和错误属于 infra |
| `bbt-framework/framework/src/host/RpcWireBridge.cc` 的正式入站 | 后续改为 `RpcServer` handler 适配 | infra 提供 envelope/期限，framework 保留业务分发 |
| `bbt-framework/framework/src/host/RpcHttpBridge.cc` 的 LegacyHeaders | 暂留 framework 迁移期桥 | 不把旧 header 协议复制进 infra；退出/默认切换另行裁决 |
| `bbt-framework/framework/include/bbt/framework/CoRpc.hpp` 的业务包装 | 保留薄包装，映射 infra 唯一 wire/codec | 兼容 Parse/From/ProtoCodec 消费者，禁止第二套字节规则 |
| `CoApp` 路由、方法表、RequestScope、Actor/有序票据、framework.actor 错误域 | 保留 framework | 属于服务语义与治理，不下沉 infra |

## 4.1 codec 兼容映射

`include/bbt/infra/rpc/RpcPayload.hpp` 是 infra 唯一的**位置参数 payload codec** 下层算法，供 framework 消费侧迁移使用。它是 framework `CoRpc.hpp`（基线 `bd9c4905` 的 `rpc_detail` 通用区间 23-251 行）的逐位等价收敛，**不**改 framework、不新增第二套字节规则：

- **字节规则**：与旧源完全一致——`count(u32 LE)` + 逐字段 `[tag(u8)][len(u32 LE)][payload LE]`；tag 数值 `Bool=1/Int=2/Uint=3/Float=4/String=5/Bytes=6` 不重排；字符串按原始 UTF-8 字节直写，整数/浮点小端。**不是** protobuf wire，也不伪装成 protobuf。
- **导出面**（复用旧名，便于机械迁移，未新增 registry/codecbase/polymorphism）：`WireType`、`wire_type_v`/`scalar_supported_v`/`is_bytes_v`/`Decay`、`PutU32`/`TakeU32`、`EncodeOne`/`DecodeOne`/`DecodeAt`/`DecodeTuple`/`EncodeTuple`，以及 schema 常量 `kRpcPositionalSchema = "bbt.rpc.positional/v1"`（值等于旧 `kCoRpcPositionalSchema`）。
- **错误码保持**：结构/字节违规 `ProtocolError`（前缀 `rpc payload: `）、类型不匹配 `TypeMismatch`、序号越界与 count 不匹配 `InvalidArgument`。
- **ProtoCodec seam**：infra 只给无 protobuf include 的默认 trait `ProtoCodec<T>{ kDefined=false }` 与 `is_proto_codec_v`。`CoRpcReq/CoRpcResp` 业务包装与业务 ProtoCodec 特化仍留 framework；framework 侧可令 `rpc_detail::ProtoCodec` 继承本默认 trait 或薄兼容，从而保留既有用户特化、不因换 namespace 失效。infra 不 include 任何业务 proto，也不承载业务包装。
- **收敛的潜在 UB**：迁移时对旧实现隐含 UB 面加编译期约束——整数仅 ≤8 字节（旧代码按 `sizeof(D)` 写字节并在 `len<8` 时符号扩展，>8 字节会左移越界）、浮点仅 4/8 字节（旧代码按 4/8 字节 memcpy，`long double` 等会错宽）；超出即编译期拒绝，不扩张类型支持。
- **32 位长度显式检查**：写字段长度前经 `FitsU32` 判定，超 32 位上界返回 `InvalidArgument`，不静默截断。
- **命名/依赖**：`namespace bbt::infra::rpc`（不超三层），`Error`/`result` 取 infra，无 framework 依赖。

本小节只定义下层契约：framework 的 `CoRpcReq/CoRpcResp` 薄包装改造、接线与上层策略随 framework 侧任务推进。验证目标 `rpc.payload`（`tests/Test_rpc_payload.cc`，只链接 `bbt::infra_common`）：golden 期望字节取自旧源规则的 literal，decode 侧用同一 literal 独立解回；旧源/新源字节级等价由一次性交叉验证程序（不进仓）另行比对。旧源 pin：rev `bd9c4905…`、blob oid `08d1c954…`、sha256 `ae37d61f…`。

## 5. 依赖与构建

- 合入基线：`3136d1de5031a2eec55475e770d5ebbadc3c32da`（PR #79）。framework 侧 `deps.lock` 固定该 SHA。
- coroutine 验证输入：`5c10c68603260e3204c337638b1e3bf10658661a`，包含 `WaitTypes.hpp` 与进程寿命运行时公共头。该 SHA 是候选期验证输入，不等于 CI 现取值；CI 从 `coroutine@main` 拉取并在 run 中记录实际 SHA。
- Protobuf 前缀：Issue #8 已锁定的 3.21.12 provenance；候选期实测命中 `libprotobuf.a` SHA256 `3e3e4ae1f24b2dd52e8104e7af229918a2d504274a08f7d660aaff59bae7a2de`，CI 侧由 `scripts/prepare_protobuf.sh` 与 `BBT_PROTOBUF_PREFIX` 强制校验，不静默回退。
- framework 消费：framework PR #55 接线、PR #56 补预算验收，最终 `aeafaae7a146a9ec1bcb6f5d3e2e5b6a06993975`；其 `deps.lock` 固定本仓 `3136d1de…` 与 coroutine `7bcda3b078f975ff2978424be7f6ba38e04fb7f6`。更早的调查基线 `bd9c4905…` 及其固定的 `162bb5fd…` 只作历史参考，不再是当前接线依据。
- 有 wire 构建：`BBT_PROTOBUF_PREFIX` 必须通过版本、runtime、库 SHA 和 pkg-config provenance 检查，并实际定义 `BBT_INFRA_HAS_RPC_WIRE`、生成/链接 `bbt_infra_rpc_proto`；framework 接线还必须启用 `BBT_FRAMEWORK_HAS_RPC_WIRE` 正式分支。
- 无 wire 构建：`bbt_infra_http` 仍应独立编译；RPC 工厂 stub 编入 HTTP target，调用返回 `RuntimeUnavailable`；不存在 `bbt_infra_rpc` target，也不留下未解析符号。

## 6. 候选期验证记录

以下为合入前候选 worktree 的实测记录（全新 scratch build 目录、C++17、Boost 1.90），保留作实现细节证据：

- `cmake` 配置成功，Protobuf 3.21.12 同源与库 SHA 命中。
- `cmake --build ... --target Test_rpc_runtime bbt_rpc_runtime_peer Test_rpc_runtime_roundtrip bbt_rpc_wire_peer Test_rpc_wire_contract Test_rpc_wire_roundtrip -j2` 成功。
- `ctest --test-dir ... --output-on-failure -R '^rpc\\.(runtime|runtime_roundtrip|wire_contract|wire_roundtrip)$'`：4/4 Passed；wire contract 另覆盖显式 `success=false` 与 Content-Type 名大小写。
- `rpc.runtime` 实测 local-min 期限不放大、handler 结构化错误、关联字段不匹配、调用方已过期的确定 `TimedOut`；`rpc.runtime_roundtrip` 不经 framework 实测公开 RpcClient/RpcServer 双进程 success 与 `NotFound`。
- 不带 `BBT_PROTOBUF_PREFIX` 的独立配置，`cmake --build ... --target bbt_infra_http -j2` 成功；RPC 工厂不导出聚合 target，HTTP target 仍可链接。

模块边界复核（候选期，隔离 build，`-DBBT_INFRA_STRINGENT_DEBUG=OFF`、ccache、`-j2`）：

- 有 wire：`bbt_infra_rpc` 为 STATIC，`nm` 显示 `NetworkRuntime::CreateRpcClient/ListenRpc` 只在该库定义；`libbbt_infra_http.a` 无这两个符号（无第二份定义，无 ODR 冲突）。
- HTTP-only 消费者链接闭包不含 protobuf：`Test_http_loopback` 与 `Test_tcp_runtime_factory`（配置了 `BBT_PROTOBUF_PREFIX` 的构建）的 `link.txt` 只含 `libbbt_infra_http/http/tcp/transport/udp`，`grep -i protobuf|rpc_envelope|bbt_infra_rpc` 无命中；`http.loopback` 正常运行。
- RPC 真调用：`ctest -R '^rpc\.(runtime|runtime_roundtrip|runtime_faults|wire_contract|wire_roundtrip)$'` 与 `^http\.loopback$` 共 6/6 Passed（`rpc.runtime_faults` 2.75s 覆盖同类型故障矩阵）。
- 无 wire：`cmake` 配置不导出 `bbt_infra_rpc`（tests/CMakeLists 的 FATAL_ERROR 门禁未触发）；`nm libbbt_infra_http.a` 显示 stub 版 `NetworkRuntime::CreateRpcClient/ListenRpc` 在其中；`rpc.runtime_no_wire` Passed，工厂返回 `RuntimeUnavailable`。
- endpoint 边界改动未改协议/默认行为：RPC 调用签名、错误码、owner 收口、local-min 预算不变；仅把 `RpcBaseUrl` 从复用 `http_detail::ParseHttpUrl` 改为本地 authority 边界检查，host/port 完整性仍由公开 `HttpClient` 校验。

以上是候选期 worktree 的证据，合入后由 PR CI 与 framework 集成验收接续。尚未覆盖：framework Service/Actor、跨语言 GetValue、真实 Service→Service 跨机器链路、预算跨 hop 与 20ms reserve 的组合验收、可选 `dual_service` 示例、全套 sanitizer 门禁。
