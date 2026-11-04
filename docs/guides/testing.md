# bbtools-infra 测试分层与执行规范

本文是三仓测试标准化统一规范在 bbtools-infra 的落点（与 `bbt-framework`/
`bbtools-coroutine` 同规范同源；执行细节以本文件与 [CI 说明](ci.md) 为准，
`docs/conventions.md` §6 给 label 定义，本文给执行策略，不重复维护）。

## 1. 分层

| 层级 | label/profile | 范围 | 时机 |
|---|---|---|---|
| 冒烟 | `smoke` | 构建后快速确认基本可用性；秒级-亚秒级 | 每次代码 PR + main 集成 |
| 聚焦 | `focused`（测试名正则） | 本次改动行为 + 直接耦合行为 + 对应回归 | 本地开发；必要 focused 进 PR |
| 完整回归 | `full` | 全部 unit/integration + 本地可跑用例 | 手动调度（`test-depth=extended|full`）、定时、发布 |
| 真实服务 | `live` | Redis/Mongo 等真实外部服务 | `test-depth=full|live` / 发布 Gate |
| 性能 | `perf` | 基线、压力、疲劳（`BBT_INFRA_PERF_BENCH=ON`） | 性能相关改动、定时、发布 |
| 诊断 | 非普通 profile | `BBT_INFRA_STRINGENT_DEBUG` 等 | `test-depth=extended|full`、发布、手动 |

`unit`/`integration` 表达测试性质，`smoke/focused/full` 表达执行策略，两层不混用：
smoke 是各层性质的选集子集（本仓当前 smoke 项均同为 `unit`）。

## 2. smoke 选集口径

- 每模块/每覆盖轴至少 1 项秒级代表用例打 `smoke;unit`：当前 13 项
  （`contract.basics`、`config.unit`、`iowait.combined_wait`、`rpc.payload`、
  `tcp.loopback`、`udp.loopback`、`transport.runtime`、`transport.acceptance`、
  `http.loopback`、`http.session_lifetime`、`http.outbound_quota`、
  `http.child_unregister`、`redis.unit`），实测约 4s。
- 重项刻意不进 smoke：`mongo.unit`（基线实测约 129s，占非 live 耗时 79%）、
  `config.watch`、`http.lifecycle`/`failure_close`/`request_phase` 等归
  full/extended 路径。
- 必选清单钉死：`scripts/ci/required_smoke.txt`；label 改动必须同步清单，
  契约测试（`scripts/ci/tests/run_tests.py` 的 `SmokeSelectionTests`）
  比对 CMakeLists `LABELS` 与清单一致性，漂移即 FATAL。
- 新增测试必须在注册时决定 label；缺 smoke 的模块在 PR 层没有保护。

## 3. 执行契约

统一入口 `scripts/ci/run_test_profile.sh`（三仓逐字节相同）：

```bash
run_test_profile.sh --build-dir <dir> --profile <smoke|focused|full> [选项]
```

| profile | 选集 |
|---|---|
| `smoke` | `-L smoke` |
| `focused` | 必须给 `--regex` 或 `--label-expr`（二者交集） |
| `full` | 默认 `-LE 'live\|perf'`；`--include-labels live` 可从排除集放回 |

通用选项：`--timeout`（默认 120）、`--junit`（缺省 `<build-dir>/junit-<profile>.xml`；
脚本会把相对 `--junit` 归一化到当前工作目录下的绝对路径，workflow 恒传绝对路径）、
`--require-count`、`--require-file`、`--repeat`。

执行契约：查询注册集 → 选集非空 → `ctest -j1 --no-tests=error
--output-on-failure` → JUnit → 校验注册集=实跑集 →
skipped/failure/error 一律 FATAL（live 在选集阶段经 `-LE live` 排除，
不靠事后容忍）。退出码：0 全过；1 选集/一致性/用例判定失败；2 参数错误；
其余=ctest 退出码。

## 4. CI 触发矩阵（普通入口 `.github/workflows/ci.yml`）

| 触发 | test-depth | build-and-gates | live | result expected_skips |
|---|---|---|---|---|
| `pull_request`/`push` | 恒 `smoke` | deps + Release 构建 + smoke profile + 必要 focused（见下） | skipped | `["live"]` |
| `workflow_dispatch` | `smoke` | 同 PR 路径 | skipped | `["live"]` |
| `workflow_dispatch` | `live` | 同 PR 路径（构建供 bundle 锚定） | 实跑 | `[]` |
| `workflow_dispatch` | `extended` | + release_gate full（全量 ctest + RPC/HTTP + HTTP-only + 符号 + ldd）+ debug_gate + network_gate + redis_net_gate | skipped | `["live"]` |
| `workflow_dispatch` | `full` | 同 extended | 实跑 | `[]` |

分类为 `docs-only` 时 build-and-gates 与 live 均 skipped；`code`/`unknown`
禁止 build-and-gates 跳过（unknown 保守执行 smoke 深度以上即全部默认路径）。
`result` 是唯一 required check；`expected_skips` 在 `result.py` 显式声明
计划内跳过的 optional job——只放行显式 `skipped` 状态，结果键缺失一律判失败，
非计划 skipped 同样判失败。

**必要 focused（PR 入口）**：`changed_files.py` 把本次变更路径经仓内
`FOCUS_MAP`（路径前缀 → 测试名正则）归并为 focused 正则输出；非空时
`build-and-gates` 在同一构建树追加一次
`run_test_profile.sh --profile focused --regex <focused>`（junit 落
`<build>/junit-focused.xml`）；无映射命中（纯 docs/scripts 等）不追加。
映射表按模块轴维护：src/include 旧栈 → `<module>.*`；`bbt/infra/{net,http,
rpc,rpc_tcp,redis,mcp}` → `net.*`/`http.net.*`/`rpc.*`/`rpc.tcp.*`/
`redis.net.*|redis.*`/`mcp.*`；`proto/` → `rpc.*`；`tests/<group>/` 按组映射；
`cmake/`、`CMakeLists.txt`、其余 `src/`、`include/`、`tests/` 兜底 →
`contract.*`；`examples/` → `example_*` + 消费面 integration。新增模块或
测试组时同步扩表，`test_changed_files.py` 的 `FocusedRegexTests` 钉死映射。

深度门禁（debug STRINGENT_DEBUG 契约、network-only ≥111 项、redis-net 容器
实跑、Redis/Mongo live）没有删除，只移出普通 PR 默认路径，由 `test-depth`
手动入口完整保留。

## 5. 必要性判定与风险覆盖

新增测试必须回答：保护哪条公共行为/错误语义/状态转换/资源生命周期；输入或
事件序列；断言观察的结果/状态/副作用；能区分哪种具体错误。

必须新增或修改：修复真实 bug；新增公共 API/错误语义/状态转换；改动取消、
超时、关闭、回收、并发、重入；改协议/wire/跨模块边界；改测试选择逻辑。

不新增：只为凑数；重复已有输入断言；只断言 mock 被调用；只断言进程返回 0；
复制 fixture；无新风险的复杂代码。

风险覆盖矩阵（替代行覆盖率门禁）：正常 API→unit/focused；错误返回→
unit/focused；状态转换→unit/integration；取消超时→focused/integration；
并发重入→integration；跨模块协议→integration；外部服务→live；性能→perf。
代码覆盖率只作定时/发布诊断，不作 PR 门禁（本仓无 gcov/lcov/llvm-cov 配置）。

## 6. 本地等价

```bash
flock /tmp/bbt-build.lock bash scripts/ci/run_test_profile.sh \
    --build-dir <build> --profile smoke \
    --require-file scripts/ci/required_smoke.txt \
    --junit "$PWD/<build>/junit-smoke.xml"

# focused：本次改动相关用例
flock /tmp/bbt-build.lock bash scripts/ci/run_test_profile.sh \
    --build-dir <build> --profile focused --regex '<affected>'

# full（本地默认不含 live/perf）
flock /tmp/bbt-build.lock bash scripts/ci/run_test_profile.sh \
    --build-dir <build> --profile full
```

本地约束不变：`flock /tmp/bbt-build.lock` 串行；增量 `--target` 构建
`--parallel 2`；本地默认不跑全量。
