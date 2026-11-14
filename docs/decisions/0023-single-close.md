# ADR 0023：单一公开关闭操作

状态：已决策，实施候选待独立审查。

## 决定

公开 owner 控制面只保留 `Close(CloseOptions)`。删除公开 `RequestClose` 和配套 `WaitStopped`；内部发起关闭、排空与真实回收 helper 为 private/detail。只读状态和回收凭据保留。既有 `CloseOptions` 的 Immediate/Graceful、wait_release、有限绝对 deadline 与首因不另造同义输入类型。

非等待 Immediate 必须保留 any-thread、callback/native/析构可用能力；等待才进行执行域与自等待检查。等待前先校验参数，全部阶段复用同一期限；超时保留真实责任，不升级、不创建等待线程。

net 保持 `std::error_code`，协议保持 `result<void>`。Engine 只统一发起入口，不承担业务排空或新增 runner/reaper；Run 返回仍是唯一宿主回收凭据。其不支持的参数在副作用前明确拒绝，详见关闭契约。连接的 native reason 单独传入同一 Close 操作，不伪造协议 Error 到 native category 的映射。

声明与实现按类、按职责并置；私有实现进入 detail。不以行数机械拆分，也不创建兼容转发头。

## 代价与取代

这是破坏性迁移：调用方不再拼接两次操作，直接以同一 Close 输入指定是否等待物理回收。framework 在独立候选集成，不由 infra 代改。低层同步 ICoCloseable 与 socket Shutdown 不是第二条 owner 控制面，保持各自职责。

本决定取代 ADR 0015、0007 与 0012 中的公开关闭入口约定，不改写其历史正文。当前唯一语义真源为 [关闭契约](../architecture/close-options-contract.md)。
