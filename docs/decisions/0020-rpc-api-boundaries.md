# RPC 公共 API 与 Channel 边界重构方向

## 状态与范围

状态：方向已确认，待按本记录迁移实现。

本文记录 RPC 公共 API 的重构目标，不把目标接口当作当前实现，也不授权立即删除现有接口。迁移时需要单独核对消费者、公共头、构建 target、契约测试和跨仓 framework 接入。

## 背景

当前 RPC 代码同时暴露了用户入口、wire 值层、传输适配和会话支撑，目录路径不足以说明哪些头是给用户使用的。`Envelope` 等名称也不能稳定表达调用者正在处理的是请求还是回复。Server、Client 的接口混入了传输 owner、连接生命周期和排空等待，调用者难以从入口看出自己需要负责什么。

RPC 只应承担 infra 的协议和通信能力。Mailbox、业务调度、CallContext、服务执行编排以及上层业务关闭策略属于 framework 或应用层。

## 目标边界

### 公共头与内部实现

`bbt/infra/rpc/` 的公共面只保留稳定的用户消费入口、请求/回复值类型和明确导出的抽象。以下内容默认放入 `bbt/infra/rpc/detail/`，不进入普通用户依赖：

- wire 编解码和 protobuf 绑定；
- HTTP/TCP 传输 adapter、连接池和长连接实现；
- session、admission、写入记账和关闭支撑；
- 测试 hook、构造接缝和实现专用访问器。

公共 target 必须显式导出真正的公共头，并用正向/负向头自检证明边界。目录位置本身不能作为公共 API 证明。

### 名称

面向调用者的语义对象优先使用角色名：

- `Request` 表示一次 RPC 请求；
- `Response` 表示一次 RPC 回复；
- `Envelope` 只用于确实表示 wire framing 或传输封装的对象，不作为含义不明的通用业务容器；
- `Mailbox` 保留在 framework 层，不在 infra RPC 中复制其调度语义。

旧名称的迁移需要检查 framework 消费面和 wire 类型边界。wire 层仍可保留独立的内部结构，但不能因为 wire 对象叫 `Envelope` 就把它直接当成业务公共对象。

## Server 目标接口

RPC server 的用户入口收敛到四类职责：

```cpp
Server::Create(Options{address, timeout, limits, ...})
Server::Start()
Server::RegisterRpc(RpcName, RpcHandler)
Server::Stop()
```

约束如下：

- `Create` 只构建和校验对象，不隐式监听；
- `Start` 开始监听并接收入站调用；
- `RegisterRpc` 注册服务处理入口，具体注册时机和重复注册错误必须在迁移契约中明确；
- `Stop` 停止监听，返回停止监听的结果，不等待业务 handler 执行完、不负责排空、不等待上层服务回收；
- handler 执行、Mailbox 投递、业务排空和应用关闭顺序由 framework 或应用负责。

底层实现可以有内部等待和资源收口步骤，但这些步骤不应重新暴露为普通用户必须理解的 `Drain`、`WaitStopped`、`Shutdown(deadline)` 等多入口。若某种资源确实需要观察物理回收，必须作为独立的内部/运维能力说明，不能改变 Server 的最小用户语义。

## Client 目标接口

RPC client 的用户入口收敛到：

```cpp
Client::Create(Options{...})
Client::Call(RpcName, Request)
Client::AsyncCall(RpcName, Request, Callback)
```

`Call` 和 `AsyncCall` 的返回、错误和取消契约需要明确；`AsyncCall` 必须明确 callback 的调用线程/协程域、取消、关闭以及调用方对象寿命。Channel 不为用户 callback 捕获的对象提供额外保活保证。

Client 应支持由底层实现选择连接复用、长连接、连接池等策略。是否复用连接不应迫使调用者直接操作 TCP/HTTP session；需要暴露的只是可观察的超时、错误和关闭语义。

## 路由与限流

`Server` 内部需要一个按 `service/method` 查找的路由表，提供 `RegisterRpc` 和
`UnregisterRpc`。路由处理器使用已有 `Request`/`Response` 值类型，并可以挂起；
Server 负责把处理结果提交回对应请求，Mailbox、业务调度和更高层并发策略仍由
framework 持有。

当前迁移切片只把 `ReplyMode::Required` 的 Call 交给注册路由；`Send` 仍由 framework
admission handler 接收，避免在 Server 内复制另一套无回复投递语义。

路由限流采用每路由令牌桶，配置只表达 burst 容量和每秒补充速率；未配置时不启用。
令牌桶只是路由项的状态，不单独抽成公共 `TokenBucket` 类。路由表同样是 Server
的内部状态，不新增只被 Server 使用的公共 `Router` facade。限流拒绝统一返回
`Overloaded`，不进入 handler，也不改变其它路由的配额。

## Channel 目标抽象

高层 RPC 依赖稳定的 Channel 通信抽象：

```text
RpcClient -> Channel -> Http / Tcp / TcpPool / TcpKeepAlive
RpcServer -> Channel
```

Channel 负责发送请求、接收对应返回，并提供同步/协程调用与异步 callback 形式。Channel 隐藏具体传输协议、连接池和长连接实现；HTTP、TCP 及其池化/保活实现作为 Channel adapter 存在。

Channel 不拥有 framework 的 Mailbox，也不替上层决定业务 handler 的排空策略。异步返回只承诺 Channel 自身的交付契约；调用方必须自行保证 callback 捕获对象在回调、取消和关闭路径上的寿命安全。

抽象只在存在稳定复用边界时建立：Channel 因为需要承载 HTTP、TCP、连接池和长连接
多种实现，可以是独立抽象；只服务一个 Server 的路由表、路由项和令牌状态保留为
Server 内部的 `struct`/值状态，不继续扩张成独立 manager、router、limiter 类。

该抽象不要求现在预建万能 transport 插件平台。先以一个真实 HTTP 或 TCP adapter 验证 Channel 的最小行为，再按第二种传输的真实重复提取稳定 seam。

## 迁移与审查门禁

迁移一个公共 RPC 类型前，必须同时回答：

1. 它是用户入口、稳定值类型，还是内部 wire/session 实现；
2. 它是否应位于公共目录或 `detail/`；
3. 调用者能否仅通过 `Server`、`Client`、`Channel` 完成最小任务；
4. 是否把 Mailbox、业务调度、排空或传输细节错误地下沉到 infra；
5. `Request`、`Response` 等名称是否准确表达角色；
6. 是否有正向/负向公共面编译检查、最小真实消费示例和直接耦合测试。

在迁移完成前，[RPC 网络指南](../guides/rpc-net.md)继续描述现行接口；每次代码迁移必须同步更新该指南和对应测试，不能用本方向文档代替实现证据。

## 取代关系

- 本文的迁移方向与 `Channel` 目标抽象已被 [0022](0022-rpc-single-protocol.md) 取代：公共入口收敛为 V2 `Client::Call/Send` 与 `Server::Create/Start/RegisterRpc/StopAccepting`，不建立 `Channel` 插件体系，也不保留旧 envelope/transport 面。本文的「Mailbox/业务调度不下沉 infra」判断继续有效。
