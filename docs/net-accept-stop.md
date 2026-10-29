# net 独立停止监听

`Engine::RequestStopAccepting()` 是 HTTP 服务端停止接纳新业务、保留已授权回复路径的基础能力；它与关闭整个 Engine 分开。当前是未提交候选，本地聚焦验收已执行；CI 尚未接线，不代表 HTTP Server 已交付。

## 消费方式

```cpp
engine.RequestStopAccepting();   // 任意线程、非阻塞、幂等，本 Engine 不重新开放接纳
// 此后已有业务连接仍可在协程中 conn.Read/conn.Write，CoDial 也不因本调用被封口。
if (engine.AcceptingStopped()) {
    // 本代 listener 已真实关闭，handoff 已封口，接纳等待者已被通知。
}
// 宿主在业务回复结束后另行关闭整个执行域：
engine.RequestClose();
runner.join();
```

`AcceptingStopped()` 初始为 false。停止请求本身不是物理停止凭据；只有 owner 关闭 listener 或 driver Failed 后同代同步解绑、控制域关闭 listener，才可确认真实停止。它不证明所有 accepted fd、外部连接引用或业务任务已经释放；连接回收仍依据 `Connection::Released()`，Engine 最终收口依据其公开契约。

## 阶段与错误

- Idle 停止后不允许后续 server 布局重新创建监听；server Run 返回 `operation_canceled`。client-only 布局不需要创建监听，不应因此禁止启动或拨号。
- Starting 期间停止时，若还不能确认 owner 和安装责任收口，观察面保持未完成；安装回滚后才能确认停止。不得在已确认停止后又创建本代监听。
- Running 停止不设置整个服务的 sealed 状态，不关闭已交付连接，不解绑 driver。关闭监听后 Engine 仍可为已交付连接推进读写。
- 停止封口与 `Engine::CoAccept`/handoff 授权共门；未交付队列和晚到交接请求回收。停止后的 `CoAccept` 返回 `operation_canceled`，`result.connection` 无效。执行域非法仍先按原 Co 入口规则拒绝；Idle/Starting 错误优先级见公开头。
- raw OnAccept 在停止门之前已经取得交付授权的调用允许完成；门后不授予新交付。原始回调仍短、非阻塞、不可挂起。
- 投递失败不等于 fd 已关闭：停止请求保持可达，由控制域有界节拍重新交给原 owner。driver Failed 的最终停止依据同步解绑后的显式关闭，不能依赖清容器或异步捕获最终析构。
- LocalAddresses 保留本代启动后的绑定地址快照，不是当前仍在监听的凭据。最终完整关闭的相位与错误语义不变。

## 本地验证入口

在已配置的 network-only 私有构建目录中，按仓库构建锁与资源规则执行：

```bash
cmake --build <build> --target Test_net_accept_stop Test_net_reclaim_receipt Test_net_r2 Test_net_freeze Test_net_b4_contract --parallel 2
ctest --test-dir <build> -j1 -R 'net\.(stop_|reclaim_|r2_co_accept_read_write|r2_close_waiters_settle|freeze_accept_configuration|freeze_guard_lifetime|freeze_co_context_gate|b4_accept_after_close|b4_engine_driver_failed|b4_run_preconditions)' --output-on-failure --no-tests=error
```

本批已实际执行该选集：26 项通过，真实退出码 0。新 `net.stop_*` 用例覆盖真实地址拒绝新连接、已交付连接在停止后双向收发、`CoAccept` 取消与 `connection` 保留、启动前停止、重复/并发停止、启动竞态、driver Failed 和未交付交接回收；另有 9 项物理回收回归。用例和通过数不能替代各自阶段与字节断言。

补充真实用例：`net.stop_stop_client_only_before_run` 在 Run 前停止后仍启动 client-only Engine，`Engine::CoDial`/`conn.Write`/`conn.Read` 与原生对端完成 ping/pong；`net.stop_stop_post_failure_retry` 控制一次零接纳投递失败，在 posted latch 未释放时观察 AcceptingStopped=false 且真实地址仍可连，随后控制域正常重交、观察物理停止及原地址拒连。`stop_wakes_coaccept_waiters` 先确认 waiter 已登记再停止；`stop_during_starting_race` 先观察 Starting，固定延迟只扩大窗口。

未验证：跨平台、sanitizer、持续资源耗尽、长时并发压力；HTTP Server 的回复 Drain 与任务/连接物理收口见 [HTTP net](http-net.md)，不从 net Run 返回推断 HTTP handler 已结束。
