# RPC core 指南（bbt::infra_rpc_net）

> 状态：**现行公共面（已落地）**。公共入口是 `bbt/infra/rpc/CoRpcClient.hpp` 与 `bbt/infra/rpc/CoRpcServer.hpp`，不保留旧路径转发头。分层、状态归属与线上格式见 [RPC 分层现状](../architecture/rpc-layered-protocol.md)。

## 职责

- 提供 `tcp://` 长连接上的 Call/Send、单 FIFO 入站接纳、请求/回复关联、deadline 预算与会话 owner 生命周期。
- 不负责业务路由、授权策略、业务 payload 语义、服务发现、DNS/TLS、重试与逐 peer 观测。
- 只装配不联网：Engine 由应用 `Run` / `RequestClose`；`Close` 与 `Shutdown` 都不关闭共享 Engine。

## 公共接口

```cpp
// bbt/infra/rpc/CoRpcClient.hpp
static result<std::shared_ptr<CoRpcClient>> Create(std::shared_ptr<net::Engine> engine, ClientOptions options);
bbt_co_async(result<Response>) Call(std::string_view target, Request request, const CallOptions& options);
bbt_co_async(result<void>)     Send(std::string_view target, Request message, const SendOptions& options);
bbt_co_async(result<void>)     Close(CloseFlag flag = CloseFlag::Immediate, std::int64_t timeout_ms = 0, Error reason = {});
void Shutdown(Error reason = {}) noexcept;

// bbt/infra/rpc/CoRpcServer.hpp
static result<std::shared_ptr<CoRpcServer>> Create(std::shared_ptr<net::Engine> engine,
                                                   std::shared_ptr<CoRpcClient> session_owner,
                                                   AdmissionHandler admission_handler, ServerOptions options);
result<void> RegisterRpc(std::string rpc_name, RpcHandler handler);                        // 可选
result<void> RegisterRpc(std::string rpc_name, RpcHandler handler, RouteOptions options);  // 只接 Required
result<void> Start();                                                                      // Engine Running 且 scheduler 已启动
bbt_co_async(result<void>) Close(CloseFlag flag = CloseFlag::Immediate, std::int64_t timeout_ms = 0, Error reason = {});
void Shutdown(Error reason = {}) noexcept;
```

其余查询接口（`IsClosed`、`CloseReason`、`Stats`、`LocalAddress`、`UnregisterRpc`、`Hooks` 等）见头文件。

## 调用约定

- `target` 只接受 `tcp://<numeric-ip>:<port>`（IPv6 用方括号）。无 DNS，不接受 scheme 之外的路径、查询或主机名。
- `Call` 要求 `Request::Reply() == Required`，且 `Traffic` 为 Service/Actor；`Send` 要求 `Reply() == None`。
- `request_id` 或 `message_id` 为空时由 session 生成。
- 同 session 同 `ordering_key` 且 `sequence != 0` 的入站帧必须连续递增（首帧即台账基值）；重复、逆序或缺口按 `ProtocolError` 退役该 link。出站方保证同 key 按序入队。
- 入站 `AdmissionHandler` 不得挂起，也不得运行业务。业务在协程中完成后回写回复，`Complete` 等到真实写终态：

```cpp
auto response = rpc::Response::FromRequest(item.request);  // 只复制关联字段
response.SetPayload(item.request.Payload());
auto completed = bbt_co_await item.reply->Complete(std::move(response));
```

改错关联字段仍会被判 `ProtocolError`，`FromRequest` 不能用来绕过校验（见 [ADR 0017](../decisions/0017-rpc-response-from-request.md)）。

- payload 是不透明字节。需要位置参数编码时，直接包含 `bbt/infra/rpc/codec/PositionalCodec.hpp`。
- 错误语义：
  - 输入非法 → `InvalidArgument`；
  - deadline 已过期 → `TimedOut`，不发请求；
  - 传输、排队、取消错误原样透传（`TransportError` / `Overloaded` / `Closed` / `Cancelled`）；
  - 请求已提交后失联或超时 → `OutcomeUnknown`，`Error.request_phase` 保留阶段事实。
- RPC 不重发、不升级、不降级。

## 关闭

- 协程域使用 `Close(CloseFlag, timeout_ms, reason)`，三态为 `Immediate`、`Timeout`、`Graceful`。
- 负 `timeout_ms` 只允许 `Close(Graceful, -1)`（无限等待）；其余负值返回 `InvalidArgument`。
- `CoRpcServer::Create` 的 `engine` 必须与 `session_owner` 的 CoRpcClient 装配同一 `net::Engine` 实例。
- 宿主 native 控制线程使用同步 `Shutdown(reason)`；协程 `Close` 不在该执行域内使用。
- 三态的等待边界与结果见 [RPC 分层现状 §5](../architecture/rpc-layered-protocol.md)。
- 等待只覆盖 RPC 自身责任，不承诺业务 handler 的优雅结束。物理回收以 Engine 收口为证。

## 构建与配置

```bash
flock /tmp/bbt-build.lock cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DBBT_COROUTINE_SOURCE_DIR=<coroutine 源码目录> -DBBT_PROTOBUF_PREFIX=<protobuf 3.21.12 前缀> \
  -DBBT_INFRA_RPC_NET=ON -DBUILD_TESTING=ON
flock /tmp/bbt-build.lock cmake --build build --parallel 2 \
  --target bbt_infra_rpc_net_header_selfcheck Test_rpc_v2_wire Test_rpc_v2_session Test_rpc_co_lifecycle rpc_v2_session_example
ctest --test-dir build -j1 --output-on-failure -R '^rpc\.net\.'
```

- `BBT_INFRA_RPC_NET=ON` 时，`cmake/RpcNet.cmake` 要求显式 `BBT_PROTOBUF_PREFIX`。protoc 与 runtime 必须是锁定的 3.21.12，来源与 SHA256 核验见 `docs/dependencies/protobuf-3.21.12.provenance.md`。
- RPC 不要求 `BBT_INFRA_HTTP_NET`，`rpc_net` 不链接 HTTP 栈。
- 自检目标：`bbt_infra_rpc_net_header_selfcheck` 验证公开头可独立编译；`bbt_infra_rpc_public_header_leak_check` 要求 core 头不拉入 HTTP/net 物理栈或 protobuf。

## 测试入口

- 测试位于 `tests/rpc_net/`，CTest 名为 `rpc.net.*`。
- 必选清单为 `scripts/ci/required_rpc_http_net.txt`（文件名沿用历史，内容是 `rpc.net.*` 注册名）。
- 覆盖范围与最近一次运行结果见 [RPC 分层现状 §6](../architecture/rpc-layered-protocol.md)。

## 边界

- 无 DNS、TLS、代理、重定向、自动重试；`target` 只接受数值 IP。
- 逐 peer/epoch 观测缺口见 [ADR 0014](../decisions/0014-app-session.md)；`Stats()` 只是 owner 汇总，不是完整 AppSession 监控。
- 跨机器、压力与故障注入不在本模块门禁内。framework 真实服务调用与跨语言闭环见 bbt-framework 验证集。
