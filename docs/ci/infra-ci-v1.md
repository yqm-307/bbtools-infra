# infra-ci-v1：bbtools-infra 正式 hosted 普通 CI（Issue #50 正式切换）

> 状态：**已合入 main**（commit `28d929bea87c1e0470a93f2919da2c73a758c451`，PR #73），并已由真实
> hosted run 验证（run `37948171767`，全 job success，总时长 8m50s）。本文件由影子文档
> `docs/ci/infra-shadow-v1.md`（随影子入口一并退役）改写而来；产品 C++、CMake、依赖锁、AGENTS、
> release、mirrors 均未改。在线事实以 run `37948171767` 为准；仅个别项仍标 UNVERIFIED（见 §8）。

## 1. 定位与范围

本仓普通 CI 从 ARC self-hosted `arc-s4-infra` 迁到 hosted `ubuntu-24.04`，并复用已发布、
fixed-SHA 的 framework 分类/结果 reusable callee：

```
yqm-307/bbt-framework/.github/workflows/bbtools-classify-v1.yml@1c0b0fb0ebc8e7ca3888aa4f6a74d1baecf08cc7
```

正式入口保持 `.github/workflows/ci.yml` 路径与触发面（PR→main / push→main /
`workflow_dispatch` 的 `build-mode`、`cache-enabled`）。影子入口
`.github/workflows/ci-shadow-v1.yml` 已删除，普通 check 只有本 workflow 一个 producer
（不是新旧双跑，也没有第二套分类/结果公共契约）。历史保留在 Git。

## 2. 目标与触发

| 维度 | 取值 |
|---|---|
| workflow 文件 / 名 | `.github/workflows/ci.yml` / `CI` |
| 触发 | `pull_request`→main、`push`→main、`workflow_dispatch`（`build-mode`、`cache-enabled`） |
| runner | 全部本地 job 固定 hosted `ubuntu-24.04`（reusable callee 自身 `ubuntu-latest`） |
| 权限 | 顶层 `permissions: {}`；仅需 checkout 的 job 授予 `contents: read`；无 secret、无 OIDC、无写权限 |
| concurrency | PR：group 用 `github.ref`（只取消同一 PR 旧 run）；main/手动：group 含 `run_id`（逐提交独立、互不取消） |

## 3. 判据（保留现役门禁）

| job（check 名） | 判据 |
|---|---|
| `变更类型检测` | 真实 diff（`scripts/ci/changed_files.py`）归一为受限路径列表；未知/超预算保守回退 unknown |
| `分类/计划（framework classify callee）` | 只调用已发布 callee 分类（classification-only，不传 results） |
| `依赖准备（固定源前缀归档）` | 复现锁定 Boost 1.90（`scripts/ci/prepare_boost.sh`）+ 唯一 recipe `scripts/prepare_resource_deps.sh` 源码构建 hiredis/mongoc/mongocxx + coroutine/core 源码 → 单一 tar artifact（sha256 digest） |
| `Release 构建 & ctest` | 下载并校验 sha256 解包 → resource-prefix A/B → Release 全量 `cmake` + `ctest -j1` → Release 归档无 Debug 观测符号 → 依赖来源核查 → Redis/Mongo 真实容器 A/B/C 与 CoTCP binding live；job 上限 45 分钟，Redis/Mongo live step 上限分别 19/16 分钟（包含 kill-after 宽限） |
| `Debug 生命周期定向门禁（8 项，零跳过）` | 下载同一归档 → Debug 8 项定向门禁（符号 + 8 项零跳过） |
| `编译 & ctest` | 唯一权威结果汇聚（`always()`，调用已发布 callee result 契约） |

`prepare-deps` / `release` / `debug` 均为重型 job（callee `optional`），仅 `docs-only`
允许显式跳过；`code` / `unknown` 执行全部。`changes` / `plan` 为必跑（callee `required`）。

## 4. 分类与结果：真实复用已发布 callee

- **分类**：`changed_files` 由本仓真实 diff 计算后作为**受限数据**传入；`source`（`github.sha`）
  与 callee automation 身份由 callee 自身 `job.workflow_*` 分离解析，不解析 caller 变量。
- **结果**：`result` job（check 名保持旧 required 名「编译 & ctest」）恒 `always()`，
  把 5 个真实 `needs.*.result`（`changes`/`plan`/`prepare-deps`/`release`/`debug`，含
  `skipped`）交给 callee result 契约。任一必跑 job `failure`/`cancelled`/空/意外 `skipped`
  → `verdict=failure` → 调用非零失败。同名 check「编译 & ctest」只有这一个 producer。
- **保守回退**：`changes` 失败致其 outputs 为空时，`result` 显式回退为保守值
  （`changed_files=[]` + `classifier_status=unknown` + 有界 event），令 callee 判
  `unknown`→执行全部，从而仍能汇聚真实 job 结果，而不是被拒绝、绕过聚合。

## 5. 工具链复现与依赖前缀

现役 ARC 日志实证运行时为 `g++ 13.3.0 / cmake 3.28.3 / Boost 1.90`（`/opt/boost`，组件
`context`）。hosted `ubuntu-24.04` 唯一缺口是 Boost 1.90（hosted 预装更旧），故
`scripts/ci/prepare_boost.sh` 把锁定 Boost **源码编译**到工作区私有前缀：

- 版本/校验值沿用 framework 已发布 `docker/toolchain.lock`：`BOOST_VERSION=1.90.0`、
  `BOOST_SHA256=5e93d582…eea9`（下载后 sha256 fail-closed）；
- 只编实际消费组件 `context`；不写 `/usr/local`、不装系统包、不改镜像；
- `boost-prefix` 随 `deps_bundle.sh` 的 prepared deps 归档一并传给 release/debug
  （`unpack` 把 `boost-prefix/lib` 并入消费端 `LD_LIBRARY_PATH`）。

资源前缀经本仓唯一 recipe `scripts/prepare_resource_deps.sh` 以固定 tag+SHA 源码构建
（hiredis v1.4.1 / mongo-c-driver 2.5.4 / mongo-cxx-driver r4.6.0）。

## 6. 缓存边界（明确阻塞）

hosted 每次干净全量构建；**跨 run ccache/GitHub cache 明确阻塞**——本 workflow 不引入
未固定完整 SHA 的 cache action，也不 restore/save 跨 run 缓存。`workflow_dispatch` 的
`build-mode`（`clean` / `cache-bypass`）与 `cache-enabled` 输入保留，仅决定本次 run 内
ccache launcher 是否启用（runner 镜像自带 ccache 时），不改变任何通过标准。#29 冷暖收益
在 hosted 上单独验收。

## 7. 与现役/影子关系与回滚

- 本文件替代影子 `ci-shadow-v1.yml`（已删除）与影子配方 `scripts/ci/run_infra_gate.sh`
  （已删除，判据并入本 workflow 的 release/debug job）以及本地结果脚本
  `scripts/ci/required_result_gate.sh`（已删除，结果汇聚改由已发布 callee 承担）。
- 回滚 = 经 PR revert 正式切换提交 `28d929b`（恢复 `.github/workflows/ci.yml` 现役内容、恢复
  `ci-shadow-v1.yml` 与两个脚本、恢复影子文档），不触碰产品代码/依赖锁/release/runner。

## 8. 验证与未覆盖

切换前本地只跑 Python/YAML/真实 scripts 门禁测试（`scripts/ci/tests/run_tests.py`）与
`bash -n` / `git diff --check` / 敏感扫描，**未跑产品 C++ 全量构建**（由 hosted CI 承担）。
本仓正式 CI 已在 push→main 上运行验证：run `37948171767`（event `push`，`28d929b`，PR #73 合入后）全 job success，总时长 8m50s。

**已由真实 run 覆盖（run `37948171767`，event `push`→main，`28d929b`，全 job success，8m50s）**：

- 真实 hosted run（`push`→main）、跨仓 reusable callee 调用的在线身份/权限与 `job.workflow_*` 解析。
- 各 job 在 hosted 的实际时长：`依赖准备（固定源前缀归档）`≈2m18s、`Release 构建 & ctest`≈5m33s、
  `Debug 生命周期定向门禁（8 项，零跳过）`≈2m41s，均在现役 15 分钟预算内，未触发超时。

**仍未核实（UNVERIFIED）**：

- hosted 实际工具链版本（`cmake`/`g++`/`ninja`/Boost）与 Boost 下载校验的真实执行：本轮未读 run 日志，绿色 run 不代替版本记录。
- `result` job（reusable 调用）在平台上的 required check 名与 job `name`「编译 & ctest」的精确
  对应、required(context,app) 的精确切换属 F3，须在线读回（本轮未读回；run 已产出以该名为前缀的
  汇聚 check 并 success，但不足以断言 required context 已精确切换）。
- `coroutine@main` / `core@master` 仍沿用现役可变分支头，实际 SHA 由 run 记录。
- 历史 run `37948171767` 的 live 组（`redis.live`/`mongo.live`/`redis.cotcp_binding.live`）为 Skipped，不能作为新增 live 门禁已执行的证据。新增门禁与逐条遗留核销见 [Redis/Mongo 验收说明](../redis-mongo-acceptance.md)；在线验收以相应 PR 的精确 head 和合入后 main run 日志为准。
