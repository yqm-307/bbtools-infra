# RPC core：新 HTTP 栈上的 Client/Server（`bbt::infra_rpc_net`）

> 状态：**候选已完成本地实现、独立审查与现役 workflow 接线，待远端 CI 验证**。本切片在 HTTP PR 基线
> （`feat/httpx-style-client` 2e92660）上把 RPC 运行时迁移到
> `bbt::infra::http::Client/Server`，删除旧 `RpcClient`/`RpcServer` 兼容壳；远端 CI 尚未观察到本候选结果。
> 本文描述当前实现与边界，不是冻结契约；接口以
> `bbt/infra/rpc/Client.hpp`、`Server.hpp`、`RpcWire.hpp` 为准。

## 定位与复用

- RPC core 只做四件事：版本化 Protobuf envelope 编解码、endpoint 局部边界检查、
  请求/回复五字段关联校验、deadline local-min 预算换算。网络、连接池、阶段
  超时、协议安全与资源配额全部归公开 `http::Client`/`http::Server` owner。
- **wire profile 与 payload 复用不改**：`proto/bbt/infra/rpc/v1/rpc_envelope.proto`
  字节规则逐位不变（POST `/rpc`、`application/x-protobuf`、`remaining_budget_ms`
  1..24h、metadata 前缀白名单）；位置参数 payload codec 仍是
  `include/bbt/infra/rpc/RpcPayload.hpp`（header-only，framework 消费面不变）。
- 变化点只有一处：`RpcWire` 的 HTTP body 绑定从旧 `bbt::infra::HttpRequest/
  HttpResponse` 改绑新 `http::IncomingRequest`/`http::Response`
  （`ParseRpcWireHttpRequest` / `ParseRpcWireHttpResponse` /
  `MakeRpcWireHttpResponse`）。旧 `MakeRpcWireHttpRequest`（旧 HTTP 类型）随之删除；
  出站请求由 `rpc::Client` 经 `http::Client::BuildRequest` 合并共享 transport
  默认配置后 `Send` 发出。

## 公共面

```cpp
// bbt/infra/rpc/Client.hpp（namespace bbt::infra::rpc）
static result<std::shared_ptr<Client>> Create(std::shared_ptr<http::Client> transport);
bbt_co_async(result<RpcEnvelope>) Call(const RpcAddress& address,
                                       RpcEnvelope request,
                                       const CallOptions& options);
void Close() noexcept;          // 转发 http::Client，幂等、不可重开
bool IsClosed() const noexcept; // 转发 http::Client

// bbt/infra/rpc/Server.hpp
static result<std::shared_ptr<Server>> Create(std::shared_ptr<net::Engine> engine,
                                              RpcHandler handler,
                                              http::ServerOptions options = {});
result<void> Start();                       // Engine Running 且有监听后，一次性
void StopAccepting() noexcept;              // 只封新接纳，已授权 handler+回复继续
result<void> Drain(bbt::coroutine::Deadline);   // 等 handler 返回且回复写终态（native 宿主）
void Close() noexcept;                      // 幂等强制封口（在途回复明确放弃）
result<void> WaitStopped(bbt::coroutine::Deadline); // listener 停 + callback 销毁 + 连接 Released
bool IsClosed() const noexcept;             // 唯一补充的观测：RPC Server 上 Close() 已调用
```

生命周期语义完全沿用 HTTP：`StopAccepting → Drain → Close → WaitStopped`
（每个 result 都要检查）；Engine 由应用装配、Run 并最终 `RequestClose` 收口。
监听地址经共享 Engine 的 `LocalAddresses()` 查询，不在 RPC 面重复暴露
Stats/LocalAddress。

## 错误与关联契约

- **固定 profile 收敛**：共享 http transport 的默认配置（base_url/headers/
  params/超时）经 `BuildRequest` 正常合并，但线上 wire 目标始终收敛为精确
  `/rpc`、`Content-Type` 恰好一个 `application/x-protobuf`；其余默认头
  （如授权头）不清除。`Send` 仍执行 `ValidatePrepared`，不一致显式拒绝；
  `BuildRequest` 因非法配置失败则原样透传。
- **调用输入**：`transport != "http"`、空 endpoint、endpoint 带
  scheme/path/query/fragment/userinfo/控制字符 → `InvalidArgument`；deadline
  已过期/未设置 → 确定 `TimedOut`（不发请求）；envelope 校验失败
  （空 service/method/request_id、budget 0/越界、metadata 违规）→
  `InvalidArgument`/`ProtocolError`。
- **HTTP 状态**：非 200 → `Unavailable`，`backend_category="http"`、
  `backend_code=状态码`；4xx/5xx 是有效 Response，不是传输异常。
- **关联**：回复的 `request_id/service/method/request_schema/response_schema`
  必须与请求同值，不一致 → `ProtocolError`（服务端 handler 改关联字段同样以
  `ProtocolError` 错误信封返回，关联字段保留请求值）。
- **透传不改写**：传输/排队/取消/未知结果错误原样透传 http::Client 错误——
  建连失败 `TransportError`、排队满 `Overloaded`、封口 `Closed`、协程取消
  `Cancelled`；请求字节完整写出后对端失联/读超时按 `OutcomeUnknown`
  （`Error.request_phase=RequestCommitted`）交付，RPC 不重发、不升级、不降级。
- **畸形报文**：服务端对非 POST、错 path（仅接受精确 `/rpc`）、错 Content-Type、
  空 body、坏 protobuf 字节一律回 **200 + 错误信封**（解析失败时关联字段用
  `unparsed`/`unknown` 占位），不 crash、不断言成功；客户端对错误形状
  （非 200、错 Content-Type、空 body、坏字节）返回对应
  `InvalidArgument`/`ProtocolError`。
- **deadline local-min**：服务端 handler 期限 =
  `min(本请求期限, now + remaining_budget_ms)`；handler 返回时预算已尽 →
  `TimedOut` 错误信封（不把超预算当成功）。

## 构建与验证

```bash
# network-only 闭包：BBT_INFRA_RPC_NET 需要 BBT_INFRA_HTTP_NET 与锁定 protobuf
cmake -S <infra 工作树> -B <私有 build 目录> \
    -DBBT_INFRA_HTTP_NET=ON -DBBT_INFRA_RPC_NET=ON -DBUILD_TESTING=ON \
    -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 精确 pin 检出> \
    -DBBT_PROTOBUF_PREFIX=<锁定 protobuf 3.21.12 前缀> \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
cmake --build <build> --parallel 2 --target \
    bbt_infra_rpc_net_header_selfcheck Test_rpc_net_wire Test_rpc_net_roundtrip \
    Test_rpc_net_faults Test_rpc_net_lifetime rpc_net_example
ctest --test-dir <build> -j1 -R '^rpc\.net\.' --output-on-failure
```

- 目标：`bbt::infra_rpc_net`（alias，STATIC：`RpcWire.cc`/`Client.cc`/`Server.cc`）；
  公开头自检 `bbt_infra_rpc_net_header_selfcheck`；示例 `rpc_net_example`
  （CTest 名 `rpc.net.consumer`）。
- 依赖：`bbt::infra_http_net`（PUBLIC）+ `bbt_infra_rpc_proto`（PRIVATE，
  protoc/runtime 同源 3.21.12 锁定与 SHA256/pkg-config 核验集中在
  `cmake/RpcNet.cmake`，是 network-only 闭包内 protobuf 锁定的唯一真源）。
  只链接 `bbt::infra_http_net` 的消费者不拉入 protobuf。
- 开关：`BBT_INFRA_RPC_NET=ON` 且缺 `BBT_INFRA_HTTP_NET` 或缺
  `BBT_PROTOBUF_PREFIX` → configure 直接 `FATAL_ERROR`（不静默跳过、不假成功）。
- 旧入口已删除：`include/bbt/infra/RpcClient.hpp`、`RpcServer.hpp`、
  `src/rpc/RpcRuntimeImpl.cc`、`NetworkRuntime::CreateRpcClient/ListenRpc`
  与旧 `rpc.runtime*`/`rpc.wire_*` 测试全量移除；全量闭包不再接入 protobuf。

## 测试覆盖（`tests/rpc_net/`，CTest `rpc.net.*`）

- `Test_rpc_net_wire`：profile 编解码往返（含 Error 分支字段与
  `Error.request_phase` 全阶段往返）、值层拒绝（budget/metadata/outcome/
  关联字段/非法阶段值）、畸形字节与 HTTP 绑定形状拒绝；缺 `request_phase`
  字段解码回 `nullopt`（旧 peer 兼容），UNSPECIFIED/未识别值按
  `ProtocolError` 拒绝；远端阶段只透传为远端事实，本地提交程度仍由本地
  传输错误路径携带（不改写）。
- `Test_rpc_net_roundtrip`：真实双端（双 Engine、真实 loopback TCP）成功回程、
  位置参数 payload 编解码、metadata 透传、并发关联不串线、local-min 预算、
  同连接复用（handler 计数 + 池 idle/budget）。
- `Test_rpc_net_faults`：输入边界、连接拒绝 `TransportError`、HTTP 503 →
  `Unavailable(503)`、错 Content-Type/垃圾 body/关联劫持/错误信封透传、
  零字节断连 → `OutcomeUnknown`+`RequestCommitted`、协程取消 → `Cancelled`、
  服务端畸形请求错误信封（raw POSIX 对端）。
- `Test_rpc_net_lifetime`：client Close 幂等/拒绝/共享 owner、server
  `StopAccepting→Drain→Close→WaitStopped` 收口凭据、停止授权下在途 handler
  完成并真实送达、`Close` 丢弃在途回复（客户端非成功终态）、`IsClosed`。

## 边界与未覆盖

- RPC/HTTP 定向门禁已接入现役 workflow：Release/Debug 的独立新闭包构建并执行 `rpc.net.*` 与 `http.net.*`，注册集与 JUnit 实跑集必须一致且零跳过；HTTP-only 闭包继续验证不引入 protobuf，缺少锁定 protobuf 前缀时 RPC configure 必须拒绝。本地结构与集合检查已通过，远端 CI 尚未运行。
- https/DNS/主机名、json/form、流式、重定向、HTTP/2、自动重试——均继承
  `bbt::infra::http` 的未实现边界，本模块不暗示。
- 双进程（跨进程）round-trip、framework Service/Actor 耦合、跨机器与压力
  验证不在本切片；消费切换由 framework owner 另行授权。
- 本轮顺带修复 `bbt/infra/http/detail/Http11.cc` 一个可达缺陷：对端未发送任何
  响应字节即断开时，旧代码对空 parser 调 `put_eof` 触发 Beast 断言 abort；
  现按「请求已提交、回复丢失」直接交付 `OutcomeUnknown`。`http.net.*` 31 项
  回归全绿。
