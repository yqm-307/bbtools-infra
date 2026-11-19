# 0024：协程 Write 采用系统调用语义，删除 WriteOperation

**状态：已接受。** 取代 [0010](0010-gnet-network-foundation.md)「协程超时/取消」行与公共头表 `WriteOperation.hpp` 行，取代 [0012](0012-icoconn-member-interface.md) 中 `Write` 返回 `WriteReport`、`op.Wait` 的约定。[0018](0018-async-write-fire.md) 的 AsyncWrite 只提交约定不变。

## 决定

协程 `Write` 的形态和 `write(2)` 加发送超时一致：返回本次调用交给内核的字节数和错误，返回时结果就是最终结果。

```cpp
std::tuple<std::size_t, std::error_code> Write(std::string data, const bbt::coroutine::WaitOptions& options = {});
```

| 情况 | n | err |
|---|---|---|
| 全部交给内核 | `data.size()` | 空 |
| 容量等待超时/取消，未接纳 | 0 | 等待错误（原映射） |
| 已接纳，超时/取消时一个字节都没发 | 0 | 等待错误；这次的字节全部撤出发送队列 |
| 已接纳，超时/取消时发了一部分 | 已发前缀 | 等待错误；未发后缀撤出发送队列 |
| 发送出错 / 连接关闭 | 已发前缀 | 发送错误 / 关闭原因（原映射） |
| 零长 | 0 | 空 |

- **超时即取消**：等待结束时撤销本次写的未发部分。返回后这次写不会再发送任何字节，也不会再改变 n。
- **n 只表示交给内核**，不表示对端收到，这一点和原 `Done` 一致。
- **部分写不自动关连接**，和 `write(2)` 一样由调用方决定。字节流里留下半个帧时，上层协议应关闭连接。
- **调用方取消不是传输错误**：撤销不进入 `Settling`，不写 `LastWriteError`，连接照常可写。
- **raw 路径不变**：`AsyncWrite` 发了就不管，结果走 `RawHandler::OnWrite(connection, WriteResult)`。被撤销的协程写同样恰好一次触发 `OnWrite`，`sent=n`，`error` 为撤销原因。

## 线性化点

撤销在连接的写锁内完成。当前实现中 owner 的 `sendmsg(MSG_DONTWAIT)` 也在写锁内执行，所以撤销方拿到锁时没有在途发送，n 就是最终值。撤销方可能要等 owner 当前持锁的整个发送批次（`PumpWrites` 在一把写锁内跑完 `max_rounds`/`byte_budget` 的多轮 `sendmsg`），不需要等 owner 的下一个 tick。将来若缩小写锁范围、把 `sendmsg` 移出锁，必须保留“撤销等待当前发送轮次结束”的屏障，不能让撤销方读到中间态。

撤销操作：本次写在队首且已发一部分时截掉剩余；本次写还没开始时把它的字节整段移出队列，前后其他写不受影响。

现有 `OutboundQueue` 只支持尾部追加和消费前缀（`detail/OutboundQueue.hpp`），一次写可能被拆进 ring 与 spill，或合并进已有尾段（`detail/OutboundQueue.cc`）。因此本决定要求给内部队列补“按写记录字节区间、撤出指定区间”的能力，属于内部实现，不改变本决定。

## 删除

- `WriteOperation.hpp`（`WriteOperation`、`WriteReport`）及其导出、负向编译用例 `neg_write_operation_ctor.cc`。
- `Types.hpp` 中的 `WritePhase`、`WriteOutcome` 在迁移后若没有其他使用者，一并删除。
- 内部 `WriteOpRecord`：协程写的结果由等待中的协程直接拿走，不需要独立寿命的记录。

## 原因

原规则“等待不是取消”让 `Write` 返回时结果还没定，只能附一张回执留给以后查。上层四处调用（rpc `Tcp.cc`、http `Http11.cc`/`Server.cc`、redis `CoRedisCli.cc`）都在用 `operation.Valid()` 加 `WritePhase` 推断“发出去没有”。改成取消语义后，n 直接回答这个问题，接口与操作系统一致，回执类型和它的记录、锁、等待通道一起消失。

## 代价

- 公共接口破坏性变更：`ICoConnection::Write`、`CoTcpConnection::Write` 签名，删除公共头。仓外直接使用这些接口的程序会编译失败，不提供兼容壳（同 0012 惯例）。
- 撤销与 owner 发送存在竞争，属于独立审查类别，须用确定性交错测试证明三档结果（0 / 部分 / 全部）与对端实际收到的字节一致。

## 上层迁移

按 n 分三档，保持原有关闭/复用/错误码决定，不确定的分支保守处理（关连接、`OutcomeUnknown`）：

| 位置 | 原判断 | 新判断 |
|---|---|---|
| rpc `detail/Tcp.cc:99` | `operation.Valid() && InFlight` → `OutcomeUnknown` | `n == 0` → 未提交（原 committed=false 路径）；`0 < n < total` → `OutcomeUnknown`，连接作废 |
| http `detail/Http11.cc:147` | 同上 | 同上 |
| http `Server.cc:899` | `no_buffer_space && Rejected && !Valid()` → 减半重试 | `err == no_buffer_space && n == 0` → 减半重试；其余失败照旧返回 false |
| redis `CoRedisCli.cc:367` | `Valid() \|\| phase != Rejected` → 连接作废 | `n > 0` → 连接作废；`n == 0` 走原未提交分支 |

可见变化只有一处：已接纳但一个字节都没发就超时，原来报 `OutcomeUnknown`（字节可能稍后发出），现在报未提交，可以安全重试。这是本决定要达到的效果。

## 补充：撤销可在发送轮次之间取得写锁

本节取代「线性化点」中关于一把写锁覆盖整个发送批次的说明。发送泵每轮只在同一次持锁中收集借用视图、执行非阻塞 `sendmsg` 并归属已发字节，轮次之间释放写锁。撤销不再被整个批次的单次持锁挡住，但互斥锁不保证公平，也不承诺撤销的墙钟等待上限。撤销取得写锁时仍没有在途发送，未发后缀在锁内撤出，返回的已发前缀仍是终值。
