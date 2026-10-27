# Redis 的 net 窄入口（已合入，默认关闭）

公共头 `<bbt/infra/redis/CoRedisCli.hpp>` 与 `<bbt/infra/redis/CoRedisPool.hpp>`，类型 `bbt::infra::redis::CoRedisCli` / `CoRedisPool`，CMake 目标 `bbt::infra_redis_net`。公共头与实现同目录置于 `bbt/infra/redis/`（对齐 `bbt/infra/net` 布局；include 路径与 namespace 不变），hiredis 编解码共享层在 `bbt/infra/redis/detail/`（新旧 Redis 实现共用，不公开）。等待入口用上游 PR #414 的 `bbt_co_async(result<...>)` 标记，调用方经 `bbt_co_await` 消费；await helper 的协程域前置检查先于函数体，非协程直调抛 `std::logic_error`。固定大小连接池 `CoRedisPool` 的语义见[池](#池-coredispool)一节。该独立入口经审查后由 [PR #95](https://github.com/yqm-307/bbtools-infra/pull/95) 合入，合并提交 `7401099b16741d67f88a64807dd086a3ea0996ae`；旧 `bbt::infra::CoRedisCli` / `bbt::infra_redis` 保留原契约。默认构建不启用新入口，framework 消费者尚未切换。

## 构建和消费

只需既有 Boost 1.90、固定 coroutine 源码和决策 0003 对应的 hiredis 私有前缀。`BBT_INFRA_REDIS_NET=ON` 选择新 net 与 Redis 的依赖闭包，不构建旧 HTTP/RPC/CoTCP。未提供 hiredis 前缀时明确配置失败，默认不开启这个目标。

```bash
cmake -S . -B <private-build> \
  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DBBT_INFRA_REDIS_NET=ON \
  -DBBT_COROUTINE_SOURCE_DIR=<coroutine-source-at-cmake/coroutine.sha> \
  -DBBT_HIREDIS_PREFIX=<hiredis-private-prefix> -DBUILD_TESTING=ON
cmake --build <private-build> --target Test_redis_net_contract redis_net_example \
  bbt_infra_redis_net_header_selfcheck --parallel 2  # target 定义在 bbt/infra/redis/CMakeLists.txt
ctest --test-dir <private-build> -j1 -R '^redis\.net\.' --output-on-failure
```

父工程可显式设置上述选项并 `add_subdirectory(<infra-source> <private-binary-dir>)`，业务目标只需：

```cmake
target_link_libraries(my_app PRIVATE bbt::infra_redis_net)
```

hiredis 仅用于实现的 RESP2 编码/reader 和共享的 `bbt/infra/redis/detail/RedisDetail`；公共头不含 hiredis 类型，亦不链接旧 CoTCP。实现放在编译单元中以隔离连接、锁和解析状态。

可运行示例是 [examples/redis_net/main.cc](../examples/redis_net/main.cc)，执行 `ctest --test-dir <private-build> -R '^redis\.net\.consumer$' --output-on-failure`。运行前须提供允许写入测试 key 的自有 loopback Redis，通过 `BBT_REDIS_PORT` 注入端口（未设置默认 6379；设置值须为 1–65535 的十进制整数）；示例只写入强唯一 key，删除并用 EXISTS 读回确认。运行时只有一个 worker；Engine 的 Run 在自有 native 控制线程推进。

## 调用语义

`Create(engine, address)` 只装配，不拨号；空 Engine、无效地址或 Unix 地址返回 InvalidArgument。地址由 `net::Address::Ip` 注入，接受数值 IPv4/IPv6，不执行 DNS。Engine 必须由宿主以 `RuntimeProgress::Independent` 合法启动，且没有 RawHandler；不会替换或接管宿主 handler。

`Connect`、`Ping`、`Get`、`Set`、`Exists`、`Delete` 都返回本仓 `result<T>`，要求真实协程域。调用方式：

```cpp
auto reply = bbt_co_await client->Get(key, options);
if (reply) {
    if (reply.value()) { Consume(*reply.value()); } // 缺失 key 为 nullopt
} else {
    Handle(reply.error());
}
```

key/value 二进制安全；拒绝空 key 和空 DEL 列表。PING 只接受 PONG，SET 只接受 OK，EXISTS 只接受 0/1，DEL 返回非负数量。整条编码命令必须适合 Engine 的单次全量写容量，超限返回 Overloaded、零接纳；无分块发送、流水线、后台队列或隐式重试。

一个 client 只允许一个活动 Connect 或命令，竞争立即 Overloaded；已经健康连接时重复 Connect 幂等。每个等待入口先复制共享 State 和调用所需值，挂起恢复后不再访问公开句柄；短锁保护接纳/封口/连接发布，不跨挂起。reader 由活动操作独占，Close 不抢先释放活动 reader。

## 错误与流同步

| 事实 | 结果与后续动作 |
|---|---|
| 非协程 / 未连接 / 已封口 | InvalidContext / RuntimeUnavailable / Closed，零提交 |
| CoDial 拒连 / deadline / 取消 | TransportError / TimedOut / Cancelled，不发布连接；失败不代表 fd 已回收 |
| Engine 不可用或有 RawHandler | RuntimeUnavailable，保留 net 原始错误码 |
| 写未接纳 | 按原因 Closed、TimedOut、Cancelled、Overloaded 或 TransportError，不一概映射 Overloaded |
| 写已接纳，没有可信完整回复 | OutcomeUnknown，`details["cause"]` 保存物理错误类别，`backend_category/backend_code` 保留后端原因；不填 request_phase，不推断 Redis 未执行 |
| 完整 Redis ERROR | RemoteError，`domain_code` 为 WRONGTYPE 等首词，`backend_category="redis"`；连接继续同步 |
| 分帧回复 | 同一绝对 deadline 循环读和 Feed/解析，空 reply 继续等待 |
| 解析/Feed 错误、不支持的回复或意外帧 | 退役连接并丢弃 reader；已接纳命令仍为 OutcomeUnknown，cause 为 ProtocolError 或 InternalError（hiredis OOM） |

未知结果或后端故障后，命令持续返回 TransportError，只有显式 Connect 才恢复；不会消费迟到回复或重发旧命令。收到完整 ERROR 后同一流仍可 Ping/Get。

## 池（CoRedisPool）

`CoRedisPool::Create(engine, address, {connections, max_waiters})` 只装配不联网；`connections` 固定池大小且大于零，`max_waiters=0` 表示池满立即返回 Overloaded、拒绝排队。`Connect(CallOptions)` 显式把全池建好才 Ready；部分失败回滚本次建连，不发布半就绪状态；已就绪且无坏 slot 时重复 Connect 幂等零动作；并发 Connect 竞争返回 Overloaded。

每个命令（Ping/Get/Set/Exists/Delete，与单连接同签名）内部借一条连接执行后 RAII 归还，业务无需手动 Acquire；借与命令执行共用同一绝对 deadline。池满时有界 FIFO 排队：归还方锁内 handoff 连接并 Notify 队首等待者；超时、协程取消与 Close 分别可区分（TimedOut/Cancelled/Closed），等待者不遗失；离场时已收到的 handoff 连接转交回池。

命令退出后按单连接实际流状态判断：传输/协议等破坏流的失败关闭该连接并退役归属 slot，坏流不回池；完整 RemoteError、参数错误及零接纳且流仍健康的失败正常归还；slot 由显式 `Connect` 重建（只建坏 slot），无自动重发命令或后台重连。`Close` 并发安全幂等不可重开：立即逻辑封口、唤醒全部等待者并关闭持有 client，不关共享 Engine，不假装物理排空；in-flight 命令按单连接语义保留 OutcomeUnknown。

## 关闭与宿主收尾

Close/析构在短锁内封口，返回前对已发布连接 RequestClose，不等物理 fd、在途 CoDial 或任务结束。合法域中此后新操作返回 Closed；IsClosed 只表示逻辑封口。Close 不关闭共享 Engine，关闭 A 后 B 仍能正常操作。

在途 Connect 可能继续到原 deadline/owner 完成；迟到成功在同一锁内复查封口，Close 已胜就请求回收局部连接并返回 Closed。库没有后台 owner 协程或捕获公开句柄的 net 回调。宿主须从提交时就登记所有 API 任务，注册失败也必须明确记为终态。

最终收尾由普通 native 宿主执行：停止接纳业务、Close 所有 client、整体 Engine RequestClose、等待 Run 返回并 join，确认 Phase 是 Stopped 及检查错误；运行时继续推进直至全部登记任务完成、任务捕获与 client 释放，随后核对 fd/driver/未交付拨号等资源。Run 返回但仍 Closing 不构成排空；CountConnections==0 和 IsClosed 也不能单独证明完成。没有 coroutine Stop/restart。示例在所有局部业务资源收口后以 `_Exit` 结束进程寿命 runtime，不验证全局静态析构；测试统一复用 `tests/net/ProcessRuntimeTestMain.hpp` 的退出边界。

新类不继承旧 ICoCloseable。framework 原有同步资源关闭视图不能直接使用这个异步 Close；framework owner 仍须明确登记、最终 Engine/任务排空和消费者迁移，再验证其自身集成。

## 验证边界

`tests/redis_net` 注册计划的九个稳定契约名称，另有分帧、断连、解析/Feed/意外帧/取消/超限回归及公开示例。真实 Redis 用例通过同步 hiredis helper 只对本次唯一 key 做 LPUSH，再用公开 Get 观察 WRONGTYPE，同流 Ping/Get 验证继续同步；同时验证二进制数据和 256 KiB 回复。受控 TCP 只补充分帧、断连和生命周期时序。

测试使用真实 CoDial 提交屏障、真实完整请求接收、被扣留的回复、owner 回收屏障、weak 句柄、reader 分配/释放计数、完整任务集、Run/Stopped、总 fd/socket 基线。每例有有限 watchdog，成功失败都清理自有 keys。原始日志、机器结果与本机绝对路径留在私有交接目录，不进入仓库。

PR #95 的本地独立验收限于 Linux/WSL、IPv4 RESP2：新 Redis 测试、公开示例及直接耦合 net 选集合计 25/25，通过且零跳过；仓外只链接公开目标的消费程序完成真实命令、清键读回和宿主排空。验证按进入目标阶段、触发事件、检查结果与资源收口构造，方法见[开发指南](development.md#测试要证明什么)。

PR #95 的 CI run 没有启用 `BBT_INFRA_REDIS_NET`。当前 workflow 已接入独立 `redis-net` job：`BBT_INFRA_REDIS_NET=ON` Debug 构建公开头自检、`Test_redis_net_contract`（含池用例）与 `redis_net_example`，经 `tests/redis-live/compose.yml` 起临时 Redis 容器（动态项目名/端口，`BBT_REDIS_PORT` 注入），`redis.net.*` 注册集与 JUnit 实跑集合必须精确一致且零跳过，示例跑真实命令冒烟，结束 `down -v` 清理本 job 独占容器；该 job 与 release/debug/network 一起进入 result 汇聚。现有 Redis live 临时容器机制见[真实后端验收](redis-mongo-acceptance.md#ci-与复现)。

IPv6 端到端、AUTH/TLS、RESP3、Cluster/Sentinel、其他平台、sanitizer 及 framework 消费切换尚未验收；readerCreate OOM 与真实同代 Engine driver unbind failure 未动态注入。旧 `net.b4_request_close_write_readiness` 偶发失败根因仍未知，拆分断言只改善归因能力；一次通过不宣称已根治。
