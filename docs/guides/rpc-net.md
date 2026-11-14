# RPC core 指南（bbt::infra_rpc_net）

> 状态：**现行公共面（已落地）**。公共入口是 `bbt/infra/rpc/CoRpcClient.hpp` 与 `bbt/infra/rpc/CoRpcServer.hpp`，不保留旧路径转发头。分层、状态归属与线上格式见 [RPC 分层现状](../architecture/rpc-layered-protocol.md)。
>
> 关闭控制面已迁移为统一 `Close(CloseOptions{mode, reason, deadline, wait_release})`：`CloseFlag` 三模式与 `Shutdown` 已移除，逻辑关闭/用户责任/物理收口由同一 `Close(CloseOptions)` 的 `mode` 与 `wait_release` 组合表达；不保留 `Close(Immediate)`/`Close(wait_release=true)` 形式的重载，见 [关闭控制面契约](../architecture/close-options-contract.md)。

## 职责

- 提供 `tcp://` 长连接上的 Call/Send、单 FIFO 入站接纳、请求/回复关联、deadline 预算与会话 owner 生命周期。
- 不负责业务路由、授权策略、业务 payload 语义、服务发现、DNS/TLS、重试与逐 peer 观测。
- 只装配不联网：Engine 由应用 `Run` / `Close()`（Immediate）收口；任何 `Close(CloseOptions)` 组合都不关闭共享 Engine。

## 公共接口

```cpp
// bbt/infra/rpc/CoRpcClient.hpp
static result<std::shared_ptr<CoRpcClient>> Create(std::shared_ptr<net::Engine> engine, ClientOptions options);
bbt_co_async(result<Response>) Call(std::string_view target, Request request, const CallOptions& options);
bbt_co_async(result<void>)     Send(std::string_view target, Request message, const SendOptions& options);
bbt_co_async(result<void>)     Close(const CloseOptions& options = {}); // 同一入口；Immediate 可任意域直接调用
// Immediate/Graceful 与 wait_release 都由 CloseOptions 表达，不提供其他 Close 重载。

// bbt/infra/rpc/CoRpcServer.hpp
static result<std::shared_ptr<CoRpcServer>> Create(std::shared_ptr<net::Engine> engine,
                                                   std::shared_ptr<CoRpcClient> session_owner,
                                                   AdmissionHandler admission_handler, ServerOptions options);
result<void> RegisterRpc(std::string rpc_name, RpcHandler handler);                        // 可选
result<void> RegisterRpc(std::string rpc_name, RpcHandler handler, RouteOptions options);  // 只接 Required
result<void> Start();                                                                      // 任意域；Engine Running 且 scheduler 已启动
bbt_co_async(result<void>) Close(const CloseOptions& options = {});
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

## 启动

- `Start()` 不限定宿主域：native 线程与受管协程都可直接调用（framework 的 Co 生命周期入口即后者）。前提是 Engine 已 Running 且 scheduler 已启动。
- 启动先在状态锁内认领启动权并校验封口态，再在**锁外**创建 admission worker、登记 accept 任务。协程任务登记在 Co 域可能因栈池未命中而挂起，因此这段窗口不得持 server 状态锁；挂起期间 `Close`（Immediate/Graceful/wait_release 组合）与 `Stats` 都可进入。
- 启动窗口内 `Close` 胜出时 `Start` 返回 `Closed`，不再发布新接纳；accept 任务若已登记仍计为收口责任，带 `wait_release=true` 的 `Close` 等其退出后才完成。
- accept 任务登记失败返回 `RuntimeUnavailable`：新 admission 队列与启动权全部回滚，不发布 `started`/queue，可重试。成功后重复 `Start` 幂等返回 ok；已封口返回 `Closed`；并发重复启动返回 `InvalidContext`（不静默成功）。

## 关闭

- 受管协程使用 `Close(CloseOptions)`：`Immediate` 立即封口并发布首因，不等待；`Graceful` 拒新并发布首因，等 admission 队列、已授权 reply/lease 与回复写责任归零；到期只返回 `TimedOut` 并保留责任，不自动升级。
- `CloseOptions` 的 `Graceful` 必须给有限绝对期限；无期限（`Deadline::max()`）返回 `InvalidArgument`。
- `wait_release=true` 时同一 `Close` 在模式工作完成后继续等 session/link 的物理 close 与 retiring lease 出账到同一绝对期限；它同样要求有限期限，参数校验先于封口/首因。
- `CoRpcServer::Create` 的 `engine` 必须与 `session_owner` 的 CoRpcClient 装配同一 `net::Engine` 实例。
- 任意域使用非阻塞 `Close(CloseOptions::Immediate(reason))` 立即封口；`Graceful → Immediate` 升级由任一次 Immediate（同一 `Close(CloseOptions)` 入口）触发，反向不允许。
- 物理收口用 Co `Close(CloseOptions{..., wait_release=true})`（有限 deadline）；它不替模块排空，不关闭共享 Engine。
- 本对象已授权 handler 或已接纳调用协程内调用带等待的 `Close`（Graceful 或 wait_release=true）是自等待，在副作用前返回 `InvalidContext`。
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
