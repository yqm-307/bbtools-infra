# 0018：AsyncWrite 只提交，不交付等待票

**状态：本地实现，待审查。** 取代 [0012](0012-icoconn-member-interface.md) 中 AsyncWrite 返回拒绝票/操作票的约定，以及 [0010](0010-gnet-network-foundation.md) 中 raw AsyncWrite 的操作票出参；Write 的等待与结算约定不变。

## 原因

非挂起提交只需表达是否接纳，等待容量和本地发送完成已有 Write。让 AsyncWrite 同时交付可查询、可等待的票据，增加了拒绝票、有效性与等待状态的使用成本。

## 决定

- `Connection`、`ICoConnection`、`CoTcpConnection` 统一提供 `std::error_code AsyncWrite(std::string)`，不保留旧票据返回或出参重载。
- 返回空错误只代表全量接纳并承担后续发送责任，不保证已写入内核、对端收到或业务执行成功。
- 队列满、关闭、结算中与分配失败等接纳错误立即返回，失败零接纳，不自动等待或重试。调用仍可短暂取得内部锁，不是 wait-free 承诺。
- 接纳后数据归 owner，调用者不需要保留参数。按值传入后即使拒绝，也不承诺恢复调用者已 move 的字符串。
- 后续发送的进度与错误仍经 raw `OnWrite` 和 `Connection::LastWriteError` 暴露；不静默丢弃错误，不新增连接自动关闭策略。
- 内部逐笔 FIFO、字节/操作额度、部分发送归属与终态事件不变；这些记录服务于发送正确性，不是 AsyncWrite 的等待票。
- `Write` 保留同一 deadline 下等待容量、接纳、本地完成，以及 `WriteReport` 的等待事实、发送事实与操作观察。超时/取消只结束等待，不撤销已接纳发送；相关记录不能因本次简化删除。

## 迁移

原 `connection.AsyncWrite(data, outOperation)` 改为检查 `connection.AsyncWrite(data)` 的错误码。
原 `co.AsyncWrite(data).Query()/Wait()` 不再存在；要等待提交和本地完成，改用 `co.Write(data, options)`。
raw OnWrite 的 operation 仍是 owner 分配的诊断身份，不再与提交时返回的票据关联。

仓外实现 ICoConnection 的类需更新 override 返回类型；跨仓消费者迁移另行授权。
