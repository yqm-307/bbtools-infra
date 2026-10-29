# 纯 TCP echo 基准 harness（infra-perf 闭环）

本目录记录 `tests/net/perf` + `scripts/bench` 的可选纯 TCP echo 基准设施。
它是**编排与取证**工具，不是产品性能优化，也不进默认 CI 负载。

## 拓扑

```
driver.py (本机编排)
  ├─ spawn tcp_echo_server  (被测：bbt::infra_net，raw 或 co)
  ├─ spawn tcp_loadgen      (参照：裸 POSIX，独立进程) —— warmup 调用
  └─ spawn tcp_loadgen      (参照：裸 POSIX，独立进程) —— measured 调用
       └─ TCP loopback echo ──► server（同一进程跨两次调用）
```

- `tcp_echo_server`：raw（`RawHandler` 短回调 + 每连接 pending 发送队列）/
  co（`Engine::CoAccept` + `conn.Read`/`conn.Write` 协程）两种模式；`--port=0` 动态分配；
  stdout 先 `PORT=<n>`，期间应答 stdin `stat`/`fin_overhead` 查询各回一行
  JSON，最终 `quit`/EOF 收口后输出完整 JSON。CPU/RSS 由本进程 `/proc/self`
  采样（`proc_ok=false` 时 driver 判该运行无效，不拿 0 冒充）。
- `tcp_loadgen`：POSIX socket + poll，与 server 分进程（客户端 CPU 不混进
  服务端）；按 `pingpong`/`transfer`/`conns` 三种负载；JSON 含
  completed/bytes/errors/seq_bad/timeouts、ops_per_sec、throughput、
  p50/p95/p99 时延、cpu_per_op、RSS max/end（显式标 max）与 proc_ok。
- `driver.py`：拉起 server、等 `PORT=`、跑统一测量窗（见下）、收集两端
  JSON、fail-closed 校验后合成聚合结果（含 meta 身份/参数/flags）；
  watchdog + 进程组级 teardown。

## 统一测量窗（三负载冻结口径）

driver 在**同一 server 进程**上依次执行：

1. **warmup 调用**（`--warmup-ms>0` 时）：`tcp_loadgen --warmup-ms=0
   --duration-ms=<warmup>`——warmup 本身是一段真实负载，但其
   completed/bytes/时延不进入任何测量指标（独立进程、独立 JSON，丢弃）。
2. **窗边界**：等 warmup 进程退出且 `stat` 报告 `active_conns==0`（warmup
   连接全部释放，无残留）；取 server `stat` 快照为测量窗起点
   （`measurement_start` 字段留证）。server 侧错误计数跨两阶段累计——
   warmup 发生的错误如实计入 server 最终 JSON，**不重置**（warmup 清零
   会掩盖真实失败）。
3. **measured 调用**：`tcp_loadgen --warmup-ms=0 --duration-ms=<duration>`。
   throughput/ops_per_sec（分母=名义测量窗 `measured_wall_seconds=
   duration`）、pXX 时延、successful operations、cpu_per_op 全部只覆盖
   本调用窗口。聚合字段 `measurement.server_cpu_per_op` 用服务端测量前后 stat 的 CPU ticks 差除以 measured completed；窗口含 measured loadgen 启动和退出至末次 stat 的短控制开销，排除预热与 Engine teardown。`server.cpu_per_op` 是整个服务进程口径，只作诊断，不用于性能裁决。

loadgen 自带的 `--warmup-ms` 内建路径等价（每 worker 在 `[start, start+
warmup]` 窗口内跑真实流量但不计 completed/bytes/latency/conns_ok）；driver
正式运行走上述独立调用形态，三负载共用同一窗语义。

## 协议（`tests/net/perf/perf_common.hpp`）

`BBT0` magic + kind('D'/'F') + conn(u64) + seq(u64) + stamp_ns(u64) + len(u32)
+ payload。确定性 (conn,seq,word) xorshift payload，两端可独立校验。
**帧序完整性**：每连接 seq 必须从 0 起逐帧 +1——server 端
（raw/co，含 EOF drain 路径）与 loadgen 端（pingpong/transfer/conns 全负载）
都对重复/缺失/乱序计 `seq_bad` 并终止该连接。
`'F'` fin：发送端告知「写完即等 EOF」；server 回显 fin 后 `RequestClose`
（conns 短连接收尾），`echoed_fin` 对账每已接受连接恰收一个。

## 关闭契约与 teardown 证据

产品语义：对端 FIN 只是 `MarkEof`+`OnRead`，**EOF 不关闭写侧**——连接最终
释放必须由 handler `RequestClose` 发起。

- raw `OnRead` 见到读终态（Eof/Error）先把 ring 剩余数据全部拆帧入
  pending（不丢尾帧 echo），pending 空即 `RequestClose`；未空登记
  eof_pending 由 `OnWrite` 排空后关（不丢已收数据的 echo）。
- co `conn.Read` 返回 `Eof`/`Closed` 时其顺序 `conn.Write` 已全终态，直接
  `RequestClose`；`conn.Write` 良性先关同样 `RequestClose`。
- 收口后 `active_conns!=0`、conn/accept 协程超时未退都算 teardown 不全
  → server 退出码非零（不再仅 WARN 放行）；co 任务用活动计数器
  （condvar）记账，不积累历史 future。
- 进程退出在 engine join + 自有任务归零后走 `std::_Exit` 边界——
  coroutine runtime 无静态析构收口（已知限制），不靠进程静态析构排水。

## 背压与错误口径

- 发送容量满（`no_buffer_space`）：raw 模式将帧挂每连接 `pending` FIFO，
  `OnWrite`/`OnRead` 推进补投——echo 不丢帧，背压如实表现为时延/吞吐，
  不计 `errors`。
- 对端先关（`Closed`/`Eof`/`WriteSettling`）：未发出的 echo 丢弃，计
  `write_closed`（拆除期良性，非 `errors`）。
- `runtime_workers` 必须 ≥ `subs+1`（1 个 main accept owner + subs 个 sub
  owner 各占一个 Processer driver 槽）；driver 未显式给时自动取 `subs+1`。

## driver fail-closed 校验

任一不符即 `ok=false`（不拿部分数据冒充成功）：两端 JSON 缺/缺键
（schema）、`proc_ok=false`（/proc 不可用测量无效）、`completed==0`
（零工作）、server 端 errors/frames_bad/seq_bad 或 loadgen 端
errors/seq_bad/timeouts 非零、任一侧非零退出码、
`server.completed != loadgen.completed`（逐帧 echo 对账，丢帧/截断两侧
都不能藏）、conns 的 `echoed_fin < accepted`（fin 对账）或收口后仍有
未释放连接。

## 工作负载

| workload | 含义 | 度量重点 |
|---|---|---|
| pingpong | 一问一答（小包往返） | p50/p95/p99 时延、ops/sec |
| transfer | 单向持续 data 帧（server 回显计吞吐） | bytes/sec、cpu_per_op |
| conns | 多连接建连+单帧往返+fin 收尾 | 建连/首往返成本、连接 churn 资源 |

## 使用

```bash
# candidate（network-only）Release 构建 bench 目标（默认 OFF，显式开启）
cmake -S <candidate> -B <build> -G Ninja \
    -DBBT_INFRA_NETWORK_ONLY=ON -DCMAKE_BUILD_TYPE=Release \
    -DBBT_COROUTINE_SOURCE_DIR=<固定 coroutine 树> \
    -DBBT_INFRA_PERF_BENCH=ON -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
cmake --build <build> --parallel 2 \
    --target bbt_net_tcp_echo_server bbt_net_tcp_loadgen

# 冒烟（ctest 注册 net.perf_smoke_raw / net.perf_smoke_co）
ctest --test-dir <build> -R '^net\.perf_smoke_' --output-on-failure

# pin 测试（net.pin_*，cmake -P 级，轻量不编译）
ctest --test-dir <build> -R '^net\.pin_' --output-on-failure

# 正式驱动（verifier 用；一次配对运行产一行聚合 JSON）
python3 <candidate>/scripts/bench/driver.py \
    --mode raw --workload pingpong \
    --server-bin <build>/tests/net/bbt_net_tcp_echo_server \
    --loadgen-bin <build>/tests/net/bbt_net_tcp_loadgen \
    --clients 8 --payload-bytes 64 \
    --duration-ms 20000 --warmup-ms 5000 --timeout-ms 40000 \
    --log-dir <logs> --out run.json
```

## baseline（B0）配对

`tests/net/perf/baseline_consumer` 是独立外部 consumer：同一 harness 源对 B0
已构建的 `libbbt_infra_net.a` 编译出 `bbt_net_tcp_echo_server_b0` /
`bbt_net_tcp_loadgen_b0`。先按常规方式把 B0 build 出 `bbt_infra_net`，再：

```bash
cmake -S <candidate>/tests/net/perf/baseline_consumer -B <b0-build> -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DBBT_BASELINE_BUILD_DIR=<B0 build 目录> \
    -DBBT_BASELINE_SOURCE_DIR=<B0 源码树> \
    -DBBT_COROUTINE_SOURCE_DIR=<固定 coroutine 树> \
    -DBBT_PERF_HARNESS_SRC=<candidate>/tests/net/perf \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
cmake --build <b0-build> --parallel "$JOBS"
```

baseline 产品树**不修改、不安装**；只复用其构建产物与公共头。

## 诚实边界 / 不属于本包

- 本 harness 只**产生原始指标 JSON**；不宣称任何性能结论。baseline/candidate
  配对矩阵、A/A 噪声校验、AB/BA 交替与推广决策由 verifier/审阅者按
  `mission.md` §Loop rules 完成。
- p99 需要足够样本（本驱动有界样本，默认 cap 8192 reservoir；正式计时须保证
  completed ≥ 10000 才解释 p99——driver 如实上报 `completed`，样本量判据
  归 verifier）。
- 冒烟只证明「进程能起、协议通、出 JSON、退出码正确」，不是性能证据。
- 已有 `tests/net/r2_tcp.cc::perf_baseline`（raw-only、n=500、co 未测）是旧
  CTest 内的极小基线，**不动、不删、不替代**；co 路径若 hang 属于真实
  baseline 问题，由本 harness 的 watchdog 如实报告而非掩盖。
- RSS 口径：`server.rss_max_bytes` 是服务进程寿命内采样峰值，包含预热；`measurement.server_rss_end_bytes` 是 measured loadgen 结束、Engine teardown 之前的 RSS，作为当前矩阵内存护栏。它不等于测量窗峰值或泄漏证明。`proc_ok=false` 时测量整体无效。
- `conns` 每次调用需超过其 500ms 的建连尾部预算；正式矩阵使用预热 5000ms、测量 20000ms。更短运行可能产生零工作，driver 必须拒绝，不能当通过。
