# Redis 客户端公共接口

## 决策

业务只依赖 `bbt::infra::redis::IRedisCli`；装配方仍通过 `CoRedisCli::Create` 或 `CoRedisPool::Create` 选择单连接或连接池。两个现有类直接实现接口，不增加包装类、统一工厂、命令 DSL 或新的执行路径。

## 职责

| 入口 | 责任 | 不承担 |
|---|---|---|
| `IRedisCli` | 公共命令、显式 Connect、逻辑 Close/IsClosed 的消费契约 | Engine、拨号、池配置、借还、集群拓扑 |
| `CoRedisCli` | 单连接、握手、RESP2、故障后的显式重连 | 排队、自动重试、共享 Engine 的停止 |
| `CoRedisPool` | 复用单连接命令、有界借还和等待、显式修复 | 向业务转移连接所有权、自动重放命令 |

接口按本仓规范放在 `redis/interface/IRedisCli.hpp`，命名空间保持 `bbt::infra::redis`，不为目录标记增加第四层命名空间。公共值类型移到 `redis/RedisTypes.hpp`；旧的 `CoRedisCli.hpp` 仍经包含链提供这些类型，不改变既有源码用法。

## 调用与失败边界

消费函数接收 `IRedisCli&` 或 `std::shared_ptr<IRedisCli>`，仍用 `bbt_co_await client.Get(key, options)`。接口不改变当前的返回值、绝对 deadline、参数校验、二进制安全、RemoteError 和 OutcomeUnknown 契约；详细规则见 [Redis 指南](../guides/redis-net.md)。单连接竞争时立即 Overloaded，池可按其配置有界等待，统一接口不承诺相同并发容量或排队策略。

`Connect` 显式建立或修复实现拥有的连接；`Close` 幂等封口、不可重开，不关共享 Engine；`IsClosed` 不代表物理回收完成。公开句柄寿命与宿主任务排空责任不变，不继承语义不同的旧 `ICoCloseable`。接口虚析构只支持正确的多态销毁，不增加等待行为。

## 扩展与兼容

当前没有 `CoRedisCluster` 实现，不增加空壳或伪装支持。未来实现必须逐项明确多 key、路由、握手、失败和重试语义，尤其不能把一次多 key 命令静默拆成若干次执行而声称语义不变。Cluster 专属装配与拓扑能力不进入此公共接口；确有不兼容命令时先裁定契约，不通过默认成功或隐式降级实现。

现有方法签名和工厂返回类型不变；引入虚基接口改变两个类的对象布局，库及消费者必须重新编译，不承诺旧二进制 ABI 兼容。没有改变运行时推进、wire 编码、资源寿命或线程模型。

## 验证

公开头各自自给编译；现有 async 编译用例核对抽象性、虚析构、两个实现可转为公共接口及 await 约束；既有真实 Redis 命令用例经公共接口调用单连接与池，保持原数据、错误和资源收口断言。仅运行本模块与直接耦合的定向测试，不以注册或静态检查冒充真实后端验证。
