# config watch v1：版本化快照、本地配置源与 watch 公开契约

范围：`bbtools-infra#35` 首切片（`include/bbt/infra/config/`、`src/config/`、`bbt::infra_config`）。
状态：**已并入 main**——`029dc2a4fe113499cfa43feab69e25d2d55d7345`（`feat(config): 实现版本化快照与本地配置源 watch (#35)`），
随后 `02567ed8017fd2afef21ffc32d18d9f85c2d0b93`（`refactor(infra): migrate close stop lifecycle semantics`）
按进程寿命运行时迁移为同步 `Close()`。**验收基线** `e0478cb541e55350da5259bcc18f0029852add11`。
已交付：版本化不可变快照、内存源、真实本地文件源、watch 去重/失败恢复/关闭、
正常 drain 与 owner 同步 `Close()` 的资源边界、最小消费示例与独立 consumer。
未交付：远程配置中心/provider 与真实重连、业务 schema 校验、客户端资源热切换（framework 归属）。

### 修订记录（2026-10-01：进程寿命运行时 + 同步 Close）

上游 coroutine 已按**进程寿命运行时**收敛（main HEAD `7bcda3b`；契约修订自 `03430a5`：删除 `Scheduler::Stop()`/restart、
运行时代际、`CompletionSignal`、`CancellationToken`/`CancellationSource`），infra 关闭契约改为**资源
owner 主动调用的同步 `Close()`**。本文原「§事件、错误与关闭语义」「§所有权与资源边界」中基于
`RequestClose`/`WaitClosed`/`CloseStatus`/`CompletionSignal`/强制 `Stop` 的结论**已被取代**（原文保留
于下方并以本记录 + 改写后的段落标注）。**当前契约**如下：

- `Watcher` 实现 `ICoCloseable`：只有 `void Close() noexcept` 与 `bool IsClosed() const noexcept`；
  `Close()` 幂等、owner 主动调用、**同步完成物理收口并返回**；没有 `RequestClose`/`WaitClosed`/
  `CloseStatus`、没有「等待关闭完成」入口、没有单等待位/`AlreadyWaiting` 结果。
- coroutine 运行时**不参与**资源收口；「运行时是否在跑」用 `Scheduler::IsInitialized()`（不再有代际）。
- 请求等待用 `CoWaiter::WaitWithCallback`（登记 → 一次投递回调 → 挂起），adapter/轮询路径 `Notify`
  唤醒；结果（事件、错误）放 operation state。

说明：`include/bbt/infra/config/Watch.hpp` 与 `src/config/` 的迁移（`RequestClose`/`WaitClosed` → `Close()`）
已随 `02567ed` 并入 main，本记录与下方改写描述的是**当前已实现并复验的契约**（复验命令与结果见
「§有界命令与实测结果」）。

## 模块与公共面

- 公共头：`include/bbt/infra/config/{Value,Snapshot,Source,MemorySource,FileSource,Watch}.hpp`，
  命名空间固定 `bbt::infra::config`（三层）；实现 `src/config/{Value 为 header-only,MemorySource,FileSource,Watch}`。
- 模块 target `bbt::infra_config`（STATIC），`PUBLIC` 只依赖 `bbt::infra_common`（即 bbtools-coroutine），
  无第三方新依赖、无隐藏 I/O 线程；公共头不出现 framework 类型或第三方类型。
- 固定依赖 SHA：`bbtools-coroutine` = `7bcda3b078f975ff2978424be7f6ba38e04fb7f6`（main HEAD，
  `Merge pull request #376`；构建前核对 `git -C <coroutine> status --porcelain` 为空）。等待/关闭契约依据其
  进程寿命运行时契约（`03430a5` 起的修订）与 `sync/CoWaiter.hpp`/`sync/WaitTypes.hpp`；旧版依据的
  `sync/CompletionSignal.hpp` §C1 已随进程寿命运行时删除。（本文早前记录为 `ddfa93c8edad9ce1e6c68b3006dce32df9130d1c`，
  它是进程寿命运行时合并前的历史 SHA，已过时。）

## 文件格式（`FileSource`）

行式解析，无第三方解析依赖：

- `#` 起始行为注释；空行忽略。
- `key = value` 每行一对，首个 `=` 分割，键值两端空白裁剪；键为空 → `ProtocolError`。
- 保留元数据键 `version` 为可选 uint64（缺省 0），不进入 `Value`；非 uint64 → `ProtocolError`。
- 重复键 → `ProtocolError`（不静默覆盖）。
- 读取失败（文件缺失 → `NotFound`；目录路径 → `InternalError`；读取中失败 → `InternalError`）
  一律返回错误结果，**不发布伪成功空快照**；空文件是合法空配置（`version = 0`、空 `Value`）。
- `version` 取自文件内容（显式字段），`revision` 为解析后 `Value` 的内容指纹；二者都不依赖墙钟/mtime。

## 版本与去重

- `MemorySource::Set` 版本单调 +1；显式版本必须严格大于当前版本，否则 `InvalidArgument`。
- `revision` = `Value::Fingerprint()`（FNV-1a 64 作用于排序后 `"k=v\n"` 规范序列化，十六进制）；
  `observed_at` 只是观测元数据，**不参与去重**。
- watch 去重键为 `(version, revision)`：二者均未变即重复版本，不通知；失败期间持续失败不重复通知；
  失败后首次成功发 `Recovered` 并重置基准；恢复后的新 `(version, revision)` 再发 `Updated`。

## 事件、错误与关闭语义（修订见「修订记录」）

- 事件：`Initial`（首拍）/`Updated`（版本变化）/`Failed`（携带 `Error`）/`Recovered`。
- `Close()` 幂等、owner 主动调用、**同步完成物理收口并返回**：返回即 watcher 已停止、不再投递消费者、
  物理资源已释放、后端不会再访问。`IsClosed()` 只读表示该物理收口状态。没有 `RequestClose`/`WaitClosed`/
  `CloseStatus`，也没有「等待关闭完成」的入口（不再有单等待位/`AlreadyWaiting`）。
- 轮询循环在下一拍（≤ `poll_interval`）观察到封口后 drain、不再投递消费者；`Close()` 在其调用线程上以
  `std::condition_variable` **有界**等待轮询循环与在途 `Read()` 退出（上限见 `co-io-adapter/v1` §6.4 的
  `detail::kCloseDrainTimeout`），超时即放弃等待并继续物理释放（调用方可据此记录告警，不无限阻塞）。
- 关闭落在「本次拍 `Read()` 已完成、事件尚未投递」之间时，**该事件被丢弃**；关闭后不再调用消费者。
  关闭与「刚通过检查、尚未进入回调」的单个事件允许并发（不在回调内持锁，回调不得阻塞）。

## 所有权与资源边界（#35 已证实缺陷的修复点；修订见「修订记录」）

coroutine 运行时按进程寿命存在、不再有 `Scheduler::Stop()`；infra **不得**依赖协程栈展开回收资源，
物理收口由资源 owner 主动同步 `Close()` 完成。因此**任何跨挂起点的强引用都会钉住其所有权图**。
本模块的所有权规则：

- `Watcher` 是唯一外部强持有者（`m_impl`）；关闭态由 `Impl`（栈外持有者）拥有；请求等待用 `CoWaiter`
  （`WaitWithCallback` 登记 → 一次投递 → 挂起，adapter 路径 `Notify` 唤醒），不把 `shared_ptr<Impl>`
  复制到协程栈上，恢复后也不回访 `Impl` 以外的所有者。
- 轮询循环只捕获 `weak_ptr<Impl>`，并在 `bbtco_sleep` 挂起前释放每拍的 `shared_ptr<Impl>`：
  挂起期间只剩一个控制块 weak 计数，源随外部句柄释放而析构。
- `Close()` 同步落定：封口 → 唤醒挂起等待者 → 有界等待轮询循环/在途 `Read()` 退出 → 物理释放并跑 closed hook。

前置条件（违反即超出本契约，见 `Watch.hpp` 注释）：`ISource::Read()` 与消费者回调在协程域内联执行，
必须同步返回、不得挂起（不得调用任何协程等待原语）——循环在本次拍内持有源与回调强引用，
若在用户代码的挂起点被 `Close()` 收口，栈上强引用不会释放，配置源可能在外部句柄释放后仍被保留；回调不得阻塞。

## 有界命令与实测结果

> **本轮复验（2026-10-08，新契约：owner 主动同步 `Close()` + 进程寿命运行时）**：以下命令与结果均在
> 验收基线 `e0478cb541e55350da5259bcc18f0029852add11`（worktree `test/infra35-acceptance-3cb0847e2024`）
> 上、对固定依赖 `bbtools-coroutine = 7bcda3b078f975ff2978424be7f6ba38e04fb7f6`（main HEAD；
> `git -C <coroutine> status --porcelain` 为空）实测；构建并发 `max(2, nproc/4)=3`、`ctest -j1`、
> `ccache` 加速，临时文件走 `TMPDIR`。旧 `RequestClose`/`WaitClosed`/强制 `Stop` 语义下的历史命令与结果
> 已随本修订删除（其结论已被上文取代，不再作为现行证据）。

```bash
# 1) 配置构建（ccache、并发 max(2, nproc/4)）
JOBS=$(( $(nproc) / 4 )); [ "$JOBS" -ge 2 ] || JOBS=2
cmake -S . -B <build> -G Ninja \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DCMAKE_BUILD_TYPE=Release
cmake --build <build> --target bbt_infra_config Test_config_unit Test_config_watch \
  Example_config_consume bbt_infra_header_selfcheck Test_contract_basics --parallel "$JOBS"
# 结果：0 error；公共头自给检查（config 6 头各生成独立 TU）链接通过

# 2) 定向测试（config 功能 + 冒烟；不跑全量）
ctest --test-dir <build> -j1 -R '^(config\.unit|config\.watch|contract\.basics)$' \
  --output-on-failure --no-tests=error
# 结果：3/3 Passed，0 skipped
# config.unit：7 用例 96 断言全过；config.watch：6 用例 58 断言全过
# （断言数经 Boost.Test --report_level=detailed 复核）

# 3) 示例
<build>/examples/Example_config_consume
# 结果：valid→ACCEPT、invalid→REJECT、malformed→读取失败（code=9 ProtocolError）、memory→ACCEPT，退出码 0

# 4) 独立 consumer（仓内可重现入口 examples/config_consumer）
cmake -S examples/config_consumer -B <build>/config-consumer -G Ninja \
  -DBBT_INFRA_SOURCE_DIR="$PWD" -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
cmake --build <build>/config-consumer --target config_consumer --parallel "$JOBS"
mkdir -p <run-dir>
TMPDIR=<run-dir> <build>/config-consumer/config_consumer
# 结果：16 项检查全 ok，输出 consumer: ALL OK，退出码 0
```

`config.watch` 的回归覆盖（**已按新契约（owner 主动同步 `Close()`、进程寿命 runtime）迁移**；
部分交错由握手建立状态；既有去重、关闭后静默与资源边界用例仍使用 `SleepRawMs` 观察窗口，
这些窗口不能单独证明轮询协程已到达挂起点。`Test_config_watch.cc` 共 6 用例 58 断言全过）：

- `watch_create_without_running_scheduler`：运行时未初始化 → 创建被拒为 `RuntimeUnavailable`。
- `watch_initial_dedup_update_memory`：首拍 `Initial`、同版本去重、更新版本投递，消费者只看到
  版本变化。
- `watch_no_callback_after_close`：`Close()` 返回后轮询协程已退出，回调不再发生，
  `IsClosed()` 为真。
- `watch_close_during_inflight_read_drops_pending_event`：关闭在 `Read()` 内、投递之前发出
  （同拍交错）→ 消费者只收到 `Initial`，待投递更新被丢弃。
- `watch_failure_recovery_file`：源读取失败不发布伪成功；恢复后继续投递新版本。
- `watch_close_stops_loop_and_releases_source`：`Close()` 唤醒并等待轮询协程退出 → `IsClosed()`
  → 释放外部句柄后**源随之析构**（测试侧 `TrackedSource` 析构标志为真）。

已删除的旧语义用例（不保留旧断言伪装兼容）：`watch_waitclosed_single_waiter_and_drain`、
`watch_waitclosed_cancelled_mapping`（`WaitClosed`/单等待位/`AlreadyWaiting`/取消令牌已随上游删除）、
`watch_force_stop_inflight_waitclosed_releases_source` 与旧「强制 `Stop` 在途 `WaitClosed`」红线反例
（`Scheduler::Stop` 已不存在）。其真正要守的不变量——「释放外部句柄后源随句柄析构」——由
`watch_close_stops_loop_and_releases_source` 以 `Close()` 路径继续覆盖。

## 未覆盖矩阵

| 项 | 状态 | 说明 |
| --- | --- | --- |
| 远程 provider 适配 / 真实重连 | 未实现（非目标） | 只定义 provider 隔离与错误/重连语义的边界，无实现 |
| 业务 schema 校验、生效/拒绝、回滚、资源热切换 | 未实现（framework 归属） | `examples/config_consume.cc` 的校验规则仅为演示 |
| 运行时代际相关的 logic_error 映射 | 已随语义删除，不再是覆盖项 | 上游已删 `Scheduler::Stop`/运行时代际；入口检查改以「运行时未初始化 → `RuntimeUnavailable`」回归覆盖 |
| 等待者被恢复前调用方释放 Watcher | 契约要求调用方保活，未验证 | 该路径属契约外误用；本轮不做承诺 |
| 回调/`Read()` 内挂起的后果 | 前置条件违反，不承诺 | 在用户代码的挂起点被 `Close()` 收口时可导致外部句柄释放后源仍不析构；已写入公开前置条件 |
| 文件读取期间被替换/权限变化（TOCTOU） | 未覆盖 | 单次读取语义，不保证原子替换 |
| 非 Linux 平台 | 未验证 | 以本仓 PR 的 CI 为准 |
| HTTP/RPC 等无关模块的关闭路径 | 不在本切片范围 | 未改动、未回归 |
