# 0019：读取面收敛为 Read 与只读 View

**状态：本地实现，待审查。** 取代 [0016](0016-owning-read-overloads.md) 中把接收 ring backing 移交给调用方的读取约定，以及 [0010](0010-gnet-network-foundation.md)/[0012](0012-icoconn-member-interface.md) 中 `TryAccess`/`ReadAccess` 的借用守卫约定。

## 决定

- 删除公共 `ReadAccess`、`Connection::TryAccess()` 及其内部锁守卫和消费接口。
- `Connection::View()` 与 `ICoConnection::View()` 返回只读 `ReadView`。
- `ReadView` 只暴露状态、错误、长度和 `std::string_view`；不持有连接、锁或底层存储，不消费数据，不提供修改入口。
- `View` 返回的视图只借用 ring 当前连续区域。调用方必须在本次同步使用中完成读取，不得跨越同一连接的下一次 I/O、`View` 调用或并发 owner 写入。
- `Connection::TryRead(Buffer&)` 与 `ICoConnection::Read(Buffer&, options)` 把接收数据复制到调用方自己的 `Buffer` 后消费 ring，不移交 ring backing。失败不写入输出、不消费连接数据。
- `TryRead(std::string&)` 保留为 raw 复制消费入口；它与 `Read` 共用接收真源，不提供第二套游标。
- 连接继续持有接收 ring 和其内部存储；用户需要跨调用保存数据时，显式使用 `Read` 复制到自己的 `Buffer`。

## 原因

`ReadAccess` 同时承担状态保活、锁寿命、视图有效期、消费游标和移动语义，接口能力超过“读取接收数据”的职责。把 backing 移交给通用可写 `Buffer` 又把数据生命周期与存储所有权绑定在一起。只读 `View` 保留协议解析所需的零拷贝观察能力，`Read` 负责明确的等待、复制和消费，连接内部存储的所有权保持单一。

## 代价与边界

`View` 不是可跨协程保存的消息对象，也不提供选择性消费。需要长期处理或跨连接生命周期保存的数据必须通过 `Read` 复制；当前 `View` 只覆盖 ring 的当前连续区域，协议层不能假设一次 view 覆盖完整消息。

仓外消费者若使用 `ReadAccess`、`TryAccess` 或依赖 `Read` 的 owning backing，需要按新公共面迁移；本仓不保留兼容壳。
