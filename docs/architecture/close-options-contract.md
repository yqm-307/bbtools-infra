# 关闭控制面（CloseOptions）与 HTTP 请求责任契约

统一公开关闭操作为 `Close(options)`；没有公开 `RequestClose` 或配套 `WaitStopped`。默认 `CloseOptions{}` 为 Immediate、不等待物理回收。稳定取舍见 [ADR 0023](../decisions/0023-single-close.md)。共享 Engine 的主线程宿主归 [宿主契约](native-lifecycle-seam-contract.md)。

## 1. 输入与执行域

```cpp
result<void> Close(const CloseOptions& options = {});
```

- `Immediate`：封口、拒新、发布首因，主动中断或明确放弃本对象的在途责任；不等 handler/worker，不宣称物理回收。任意线程可发起，包括 callback、native IO 域、析构收尾。该路径不得为了统一接口而进入 Co 等待或自等待。
- `Graceful`：停止接纳，在有限绝对 `deadline` 前排空本模块已接纳的用户责任；到期返回 `TimedOut`，保留真实 owner、连接、worker、lease，不自动升级为 Immediate。
- `wait_release=true`：在模式完成后追加本对象真实物理回收等待，仍使用同一绝对期限；不等待共享 Engine，不续预算。
- 所有需要等待的组合先校验有限期限，再校验模块能力、执行域与自等待；失败在封口、发布首因或关闭连接等任何副作用之前返回。无有限期限或不支持的组合为 `InvalidArgument`；合法组合处于不允许等待的执行域为 `InvalidContext`。
- 受管 Co 域每轮以 `min(deadline, now + 50ms)` 挂起等待，不退回 native CV/join，不创建等待线程。模块明确支持的 native 宿主域可做同一谓词的有界等待。IO callback、driver worker 与本对象已接纳责任内部不能等待自身。
- 重复关闭幂等、不可重开；首个原因固定。另一次显式调用允许 Graceful → Immediate，不允许逆转，也不由内部自动重试。

## 2. 三种事实

| 事实 | 含义 | 凭据 |
|---|---|---|
| 封口 | 不再接纳，首因已发布 | 后续操作返回 Closed；CloseReason 只读 |
| 用户责任结算 | 既有责任写终态、结果已发布或明确放弃 | Graceful 成功及模块责任谓词 |
| 物理回收 | 本对象连接、监听、worker 或 lease 已真实释放 | Close(wait_release=true) 成功、Released()/ReclaimReceipt、Engine Run 返回 |

三者不可互相替代。非等待 Immediate 成功只说明关闭动作已提交；Graceful 成功也不独立证明 fd/worker 已释放。超时不得清台账冒充回收。

## 3. 模块职责与能力

| 对象 | Graceful 等待责任 | 物理回收与支持域 |
|---|---|---|
| http::Server | 已授权请求的 handler 返回及回复写终态/明确放弃 | 连接 Released、listener AcceptingStopped、session/callback 退出；Co/native |
| http::Client / detail Pool | 已借出 Lease 与排队等待者 | 池条目真实退役、Connection Released；Co/native |
| rpc::CoRpcServer / CoRpcClient | admission、调用、回复写责任 | session/link/retiring lease 真实出账；等待限 Co |
| redis::CoRedisPool | 借出连接归还与排队获取者 | slots 退役且 borrowed 归零；等待限 Co |
| redis::CoRedisCli | 不支持 Graceful | 单连接只支持 Immediate、不等待；不支持 wait_release |
| config::Watcher | 已授权 callback 完成 | 轮询协程退出；wait_release 仅 native 宿主，Co 组合副作用前拒绝 |
| consul::Client | Graceful 等在途请求发布结果；Immediate 中断专属 HTTP，迟到成功不交付 | 专属 HTTP Client 连接退役；Co/native |
| MCP HTTP transport | 委托 http::Server | 同 HTTP Server，不关闭共享 Engine |
| MCP stdio transport | 已接纳请求写终态、输出队列及 writer 当前帧均结算 | reader/writer 退出、sink 卸载、O_NONBLOCK 还原；Co/native |
| mongo::CoMongoDb | 已提交 op 发布逻辑终态 | infra 原有 native finalizer 完成 worker 退出、join、lease 归还；Co/native 等信号，不在 Co worker join |
| net::Connection / ICoConnection | 无业务排空责任，不支持 Graceful | Immediate 可选 wait_release，等待限 Co；非等待任意线程 |
| net::Engine | 无业务 drain，不支持 Graceful | 只支持 Immediate、不等待；唯一回收凭据仍为原主线程 host hook 内 Run 返回且 Stopped |

net 返回 `std::error_code`，协议 adapter 返回 `result<void>`；不强行合并错误层。连接 `Close(options, native_reason)` 保留原生首因，`options.reason` 的非空协议错误不是 native error，副作用前返回 invalid_argument；Engine 同样拒绝非空协议 reason。Engine 无业务排空或独立 runner；`wait_release` 与现有「主任务完成才关闭宿主」存在自依赖，因此在副作用前明确拒绝该参数，不创建 reaper/runner 或隐式等待 HostGate。此能力限制须由集成方独立确认，不能把 Engine 非等待成功当作物理回收。

`ICoCloseable`、NativeSocket 与旧 CoMongoCli/集合句柄仍是同步低层接口，不是上述 owner 的第二条控制面。socket `Shutdown` 是读写半关闭，不释放 fd，不替换为 Close。

## 4. HTTP 请求与响应所有权

- 每连接唯一 reader/parser，拥有 pending 字节与解析状态；接纳前同时保留连接内槽位和全局额度，不足则背压。
- 每个已接纳请求一个执行任务，拥有 IncomingRequest、Response/ResponseBody 与序号。handler 可重叠执行，不直接写 socket。
- 每连接唯一 writer/sequencer，按读序写响应，是唯一拉取流式 ResponseBody 的角色；不承诺 handler 副作用执行顺序。
- inflight 从接纳开始，直到回复写终态或明确放弃才结算；handler 返回本身不减 inflight。放弃时仍存活的任务保留额度到退出。
- 写失败、断连或 Immediate 令所有未写序号明确失败/放弃并唤醒等待者，不重试、不重排；流源至少保活至写终态或取消，Cancel 恰好一次。
- 请求数、队列、字节与流时长均有界；完成响应发布入队前校验字节上限，writer 保留防御性校验。
- Graceful 到期保留连接、响应与任务责任；wait_release 在同一调用内继续等真实释放，不清计数冒充完成。

## 5. 验证与消费

消费者不得拼接「发起关闭 + 等待关闭」。需要物理回收时直接设置 `wait_release=true` 和有限 deadline。非法组合、副作用前域拒绝、自等待、首因、显式升级、超时保留责任与真实物理终态分别验证；并发用例先用屏障证明进入目标阶段。真实 Redis/Mongo/Consul 后端未运行时不可标为 Passed。
