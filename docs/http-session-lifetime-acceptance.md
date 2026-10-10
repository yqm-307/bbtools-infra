# HTTP session 生命周期：#62 收尾证据与边界

本记录整理 [Issue #62](https://github.com/yqm-307/bbtools-infra/issues/62) 的会话反登记、teardown 与真实消费证据，不修改公开 `Close()` 契约，不宣称所有异常分支或平台已验收。

## 版本与修复

- timer cancel/re-arm 记账修复：PR #63，合并提交 `65a0a407acee7ece7f25e1aa294cf2e83f608965`。
- 反登记/析构顺序回归及 Debug 观测：PR #70，合并提交 `b024b2cff6f7526593d718c477a9eb33b9381bf4`。
- 本轮验收 infra：`28d929bea87c1e0470a93f2919da2c73a758c451`。
- 下游 framework：`bd9c490541ccad10cf32e15b91bb40104aa86118`；其锁定 infra 为 `162bb5fd1340e8b3c043086e8ed753aa5ec533cb`。本轮分别测试锁定版本与通过源码消费参数指定的当前 infra；未改下游依赖锁。

## 反登记与末次释放顺序

`HttpServerImpl::m_sessions` 是非持有的 `HttpSession*` 集合，teardown 对它取裸指针快照。因此安全性不能靠集合本身延寿，必须维持以下顺序：**已登记会话先反登记，随后才能释放最后一个强引用并析构。**

现行实现与回归检查的依据：

1. `RegisterSession`、`UnregisterSession` 与 teardown 使用同一 `Engine()->IoGate()` 同步边界；集合修改与快照遍历不能无同步地交错。
2. `MaybeRelease()` 仅在 `closed && inflight == 0 && registered` 时反登记。timer cancel/re-arm 必须准确配对 outstanding completion 的记账；旧记账错误会使已关闭会话残留在集合中。
3. 已发起的 I/O、deadline、peer-watch completion，以及 handler/task 回投持有 `self`。协程侧也有不持门的引用释放点，不能把“所有释放都在门内”作为证明。
4. `Test_http_session_lifetime_probe` 使用受控持门交错、空闲 keep-alive（多次请求）并发关闭、在途 handler 并发关闭，检查 `destroyed_while_registered == 0` 及反登记/析构次序。完成 handler 的迟到引用可以在反登记后继续存活，不等同于仍持有未释放的 socket。

该推导和探针针对已审查路径，不是对所有抛异常路径的数学穷举证明。详细历史说明及负向对照见 [#62 验收记录](https://github.com/yqm-307/bbtools-infra/issues/62#issuecomment-6073037192)。该记录报告移除 timer 记账递减后出现顺序断言失败；本轮未重跑该历史变体。

## 已有自动门禁

[main CI run 37948171767](https://github.com/yqm-307/bbtools-infra/actions/runs/37948171767) 对应上述 infra 验收提交：Release CTest 22 Passed、0 Failed、3 live Skipped；Debug 定向门禁 8 Passed、0 Failed、0 Skipped。

- Release 继续验证普通功能与资源行为；`BBT_INFRA_STRINGENT_DEBUG` 观测计数不在 Release 产物中，不能把 Release 的零计数读数当有效寿命证据。
- 反登记/析构计数的实质验证来自 Debug 定向门禁；其 8 项集合不是全量 Debug 测试。
- Redis/Mongo live 的跳过不构成 HTTP #62 的失败，也不是该票要求新增的验收范围。

## 本轮真实 HTTP 双进程验证

使用 framework 的 `examples/getvalue/getvalue_s2s_run.py`，当前 infra 组合运行 3 轮，锁定 infra 组合运行 1 轮；另有独立审查者重跑当前组合 1 轮。每轮核心场景 `known / miss / empty / expired / noroute / blackhole-unknown / journal-terminal` 均通过，caller/callee 均正常退出。

编译启用 `-fsanitize=address,undefined -fno-sanitize=vptr -fno-omit-frame-pointer -g -O1`；运行关闭 leak 检测并设置 `halt_on_error=0`。子进程 stderr 被完整捕获；后置核验同时检查核心场景集合、失败列表、退出码与 sanitizer 日志模式，发现失败即非零退出。所有已检查运行日志未见已启用检查集的 finding。

边界必须保留：

- vptr 与 leak 检测未启用，未验证 TSan；不能写成完整 UBSan 或无泄漏证明。
- `halt_on_error=0` 下退出码为零不独自证明没有 finding，必须同时扫描日志。
- 当前组合通过是有限运行证据。原始旧版本组合（framework `e35894c…`、infra `0a40b702…`）本轮未重放，不能写成技术上无法检出或原始缺陷字节级前后对照。
- 公开头文本未变不独自证明 ABI 或所有行为兼容；证据仅覆盖实际运行场景。
- 历史轮次归属依赖运行脚本、独立工作目录及产物记录；相同结果 JSON 不独自证明轮次身份。

## 尚未收口的边界

PR #70 记录的 `close_after_write` 延迟分支与 handler 晚退状态，以及两个 arm 操作异常时的理论保活缺口，不能由上述正常运行证据自动排除。它们保持明确的未覆盖边界；本记录不以文档降低同步 `Close()` 的承诺，也不把不可达推断写成实测缺陷。

`m_sessions` 仍为非持有集合；若后续证据要求结构性加固，应单独评估不引入 server/session 强引用环的方案。本轮不引入新的集合模型、不修改 coroutine、不增加平台或协议承诺。#62 的最终关闭仍需对照原验收与这些边界决定，不由本记录自动关闭。
