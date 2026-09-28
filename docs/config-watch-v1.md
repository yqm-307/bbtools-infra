# config watch v1：版本化快照、本地配置源与 watch 公开契约

范围：`bbtools-infra#35` 首切片（`include/bbt/infra/config/`、`src/config/`、`bbt::infra_config`）。
状态：**本候选已实现**（版本化不可变快照、内存源、真实本地文件源、watch 去重/失败恢复/关闭、
正常 drain 与强制 Stop 的资源边界、最小消费示例与独立 consumer）。
未交付：远程配置中心/provider 与真实重连、业务 schema 校验、客户端资源热切换（framework 归属）。

## 模块与公共面

- 公共头：`include/bbt/infra/config/{Value,Snapshot,Source,MemorySource,FileSource,Watch}.hpp`，
  命名空间固定 `bbt::infra::config`（三层）；实现 `src/config/{Value 为 header-only,MemorySource,FileSource,Watch}`。
- 模块 target `bbt::infra_config`（STATIC），`PUBLIC` 只依赖 `bbt::infra_common`（即 bbtools-coroutine），
  无第三方新依赖、无隐藏 I/O 线程；公共头不出现 framework 类型或第三方类型。
- 固定依赖 SHA：`bbtools-coroutine` = `ddfa93c8edad9ce1e6c68b3006dce32df9130d1c`
  （构建前核对 `git -C <coroutine> status --porcelain` 为空；契约依据该 SHA 的
  `agent-docs/2026-09-07-core-runtime-contract.md` §6 与 `sync/CompletionSignal.hpp` §C1）。

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

## 事件、错误与关闭语义

- 事件：`Initial`（首拍）/`Updated`（版本变化）/`Failed`（携带 `Error`）/`Recovered`。
- `RequestClose()` 幂等（Open → Closing）；轮询循环在下一拍（≤ `poll_interval`）观察到关闭请求后 drain，
  不再投递消费者，然后置 Closed（`IsClosed()` 只表示 drain 完成，不表示已请求关闭）。
- `WaitClosed()` 只在协程内等待；**每个对象至多一个并发等待者**，第二个返回 `AlreadyWaiting`，
  不影响首个。映射固定（`co-network/v1` §关闭规则）：`Completed → Closed`，`TimedOut/Cancelled/
  InvalidContext/AlreadyWaiting/RuntimeUnavailable` 一一同名；结果由等待竞争点一次定死，恢复后不重判。
  超时/取消不撤销关闭，也不代表资源已释放；调用方须在等待期间保持 Watcher 存活。
- 关闭请求落在「本次拍 `Read()` 已完成、事件尚未投递」之间时，**该事件被丢弃**；关闭后不再调用消费者。
  关闭请求与「刚通过检查、尚未进入回调」的单个事件允许并发（不在回调内持锁，回调可调用 `RequestClose`）。

## 所有权与资源边界（#35 已证实缺陷的修复点）

协程契约 §6：`Scheduler::Stop()` 对挂起协程直接销毁、不展开栈，栈上对象不执行析构。
因此**任何跨挂起点的强引用都会永久钉住其所有权图**。本模块的所有权规则：

- `Watcher` 是唯一外部强持有者（`m_impl`）；关闭态与 `CompletionSignal` 都由 `Impl`（栈外持有者）
  拥有，`WaitClosed` 期间不把 `shared_ptr<Impl>`/完成信号复制到协程栈上，恢复后也不回访 `Impl` 以外的所有者。
- 轮询循环只捕获 `weak_ptr<Impl>`，并在 `bbtco_sleep` 挂起前释放每拍的 `shared_ptr<Impl>`：
  挂起期间只剩一个控制块 weak 计数，源随外部句柄释放而析构。
- 强制 Stop 后循环不执行 drain：`IsClosed()` 保持 false，不伪装 Closed。

前置条件（违反即超出本契约，见 `Watch.hpp` 注释）：`ISource::Read()` 与消费者回调在协程域内联执行，
必须同步返回、不得挂起（不得调用任何协程等待原语）——循环在本次拍内持有源与回调强引用，
若强制 Stop 命中用户代码的挂起点，栈上强引用不会释放，配置源可能在外部句柄释放后仍被永久保留；回调不得阻塞。

## 有界命令与实测结果

以下命令在 `bbtools-infra` 工作树 `feat/issue-35-config-pilot`（base `0f468d9b7af409227f53920bdbf2ba5bd9882e8d`）
上实测；构建并发 2、`ctest -j1`、`ccache` 加速，临时文件走 `TMPDIR`。

```bash
# 1) 配置构建（ccache、并发 2）
cmake -S . -B <build> -G Ninja \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
cmake --build <build> --target bbt_infra_config Test_config_unit Test_config_watch \
  Example_config_consume bbt_infra_header_selfcheck Test_contract_basics \
  Test_tcp_loopback Test_udp_loopback Test_iowait_probe --parallel 2
# 结果：0 error，公共头自给检查（6 个 config 头各生成独立 TU）构建通过

# 2) 定向测试（功能 + 冒烟 + 直接耦合；不跑全量）
ctest --test-dir <build> -j1 -R '^(config\.unit|config\.watch|contract\.basics|tcp\.loopback|udp\.loopback|iowait\.probe)$' \
  --output-on-failure --no-tests=error
# 结果：6/6 Passed，0 skipped（config.unit/config.watch 与冒烟一致通过）

# 3) 示例
<build>/examples/Example_config_consume
# 结果：valid→ACCEPT、invalid→REJECT、malformed→读取失败（code=9 ProtocolError）、memory→ACCEPT，退出码 0

# 4) 独立 consumer（仓内可重现入口 examples/config_consumer）
cmake -S examples/config_consumer -B <build>/config-consumer -G Ninja \
  -DBBT_INFRA_SOURCE_DIR="$PWD" -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
cmake --build <build>/config-consumer --target config_consumer --parallel 2
mkdir -p <run-dir>
TMPDIR=<run-dir> <build>/config-consumer/config_consumer
# 结果：16 项检查全 ok，输出 consumer: ALL OK，退出码 0
```

`config.watch` 的确定性回归（握手建立状态，不用 sleep 近似时序；`Test_config_watch.cc` 共 9 用例 83 断言全过）：

- `watch_create_without_running_scheduler`：未启动调度器（代际 0）→ `RuntimeUnavailable`。
- `watch_waitclosed_single_waiter_and_drain`：第二个 `WaitClosed` 返回 `AlreadyWaiting`（证明首个等待已建立）
  → `RequestClose` → 首个等待返回 `Closed` 且 `IsClosed()`。
- `watch_waitclosed_cancelled_mapping`：已取消令牌 → `Cancelled`（不误报 `RuntimeUnavailable`），
  且取消不撤销关闭（随后仍 drain 到 `Closed`）。
- `watch_close_during_inflight_read_drops_pending_event`：关闭请求在 `Read()` 内、投递之前发出（同拍交错）
  → 消费者只收到 `Initial`，待投递更新被丢弃；drain 后有界窗口内源不再被读取。
- `watch_force_stop_inflight_waitclosed_releases_source`：`WaitClosed` 在途 → 强制 `Stop` → 释放外部句柄
  → **源随之析构**（修复前同一场景 `source_destroyed_after_release=0`，见下）。

**红线反例（父验收探针，修复后重跑）**：`Stop` 在途 `WaitClosed` 场景

- 修复前（父验收记录）：`with_waiter=1 first_waiter_observed=1 waiter_returned=0 marked_closed=0
  source_destroyed_after_release=0`，exit 1。
- 修复后（本候选重编重跑）：同样输入输出 `source_destroyed_after_release=1`，exit 0；
  无 waiter 对照组 `source_destroyed_after_release=1`，exit 0。
- 根因回归（把 `shared_ptr<Impl>` 重新复制到等待协程栈上）：同一仓内回归用例在
  「释放句柄后源随句柄析构」断言处失败（RED），恢复修复后 11/11 通过（GREEN）。

## 未覆盖矩阵

| 项 | 状态 | 说明 |
| --- | --- | --- |
| 远程 provider 适配 / 真实重连 | 未实现（非目标） | 只定义 provider 隔离与错误/重连语义的边界，无实现 |
| 业务 schema 校验、生效/拒绝、回滚、资源热切换 | 未实现（framework 归属） | `examples/config_consume.cc` 的校验规则仅为演示 |
| 与 `Scheduler::Stop` 竞争时代际 logic_error 映射 | 代码路径存在，未构造确定性用例 | 入口检查以「未启动调度器 → `RuntimeUnavailable`」回归覆盖；并发 Stop 竞态未测 |
| 等待者被恢复前调用方释放 Watcher | 契约要求调用方保活，未验证 | 该路径属契约外误用；本轮不做承诺 |
| 回调/`Read()` 内挂起的后果 | 前置条件违反，不承诺 | 强制 Stop 命中该挂起点时可导致外部句柄释放后源仍不析构；已写入公开前置条件 |
| 文件读取期间被替换/权限变化（TOCTOU） | 未覆盖 | 单次读取语义，不保证原子替换 |
| 非 Linux 平台 | 未验证 | 以本仓 PR 的 CI 为准 |
| HTTP/RPC 等无关模块的关闭路径 | 不在本切片范围 | 未改动、未回归 |
