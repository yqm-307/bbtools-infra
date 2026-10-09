# ci-shadow-v1：bbtools-infra hosted 影子门禁（Issue #50 / 方案 T4）

> 状态：**本地最小候选，未 commit / 未 push / 未建 PR / 未触发任何 CI**。本目录与新增文件
> 只存在于隔离 worktree；现行 `.github/workflows/ci.yml`、产品 C++、依赖锁、docker、
> release、required 规则与 runner 均未改。影子不是权威门禁，其失败/成功都不改变现役结果。

## 1. 定位与范围

影子门禁与现役 `ci.yml` **同判据**（构建 + `ctest -j1`），但使用**完全独立身份**，且
运行在 **hosted `ubuntu-24.04`**（现役为 ARC self-hosted `arc-s4-infra`）。用途：在不触碰
现役权威、不引入任何写副作用的前提下，验证「hosted 上等价复现现役判据 + 复用已发布
classifier/result 模板」这条迁移路径。

新增文件（仅这三类，未改任何现役文件）：

| 文件 | 职责 |
|---|---|
| `.github/workflows/ci-shadow-v1.yml` | 影子 caller：变更集 → 复用已发布 callee 分类 → 重型构建 → 复用 callee 汇聚结果 |
| `scripts/ci/changed_files.py` | 从真实 git diff 归一为受限路径列表（不实现分类契约） |
| `scripts/ci/prepare_boost.sh` | 源码复现锁定 Boost 1.90（hosted 工具链缺口） |
| `scripts/ci/run_infra_gate.sh` | 构建 + ctest 门禁配方（等价现役 build-and-test job） |
| `scripts/ci/tests/run_tests.py` | 配方守卫 + 分类路由冒烟 + workflow 静态契约 + 与 ci.yml 耦合 |
| `docs/ci/infra-shadow-v1.md` | 本迁移说明 |

**非目标**（明确不做）：不改现役 `ci.yml`/依赖锁/产品代码/镜像/docker/release/required
规则/runner；不新增自建准入；不采购/注册机器（self-hosted、light/heavy、perf 全部不动）；
不发布/不写基线；不执行也不下载任何 artifact；不复制第二套分类/结果公共契约。

## 2. 身份独立性（与现役 ci.yml 不共用）

| 维度 | 现役 ci.yml | 影子 ci-shadow-v1 |
|---|---|---|
| workflow 名 / check | `CI` / `编译 & ctest` | `ci-shadow-v1` / `shadow 构建 & ctest（hosted，无缓存）` |
| concurrency group | `CI-${{ github.ref }}` | `ci-shadow-v1-${{ github.ref }}` |
| runner | `arc-s4-infra`（self-hosted） | `ubuntu-24.04`（hosted） |
| artifact | 无 | **无**（不产出、不消费、不下载；与 canary payload 命名空间 `bbt-canary-payload-v1` 无交集） |
| cache | ccache + actions/cache（#29） | **无**（每次干净全量构建；等价 cache-bypass 路径） |

触发面：仅 `push.branches: [ci/issue-50-hosted-shadow]` 与 `pull_request.branches: [main]`；
无 `workflow_dispatch`/`schedule`/`tag`/`merge_group`。concurrency：
`cancel-in-progress: ${{ github.event_name == 'pull_request' }}` —— push 逐提交各一 run
（不取消，保留逐提交结果），PR 只取消**同一 PR**的旧 run。

## 3. 权限与身份

- 顶层 `permissions: {}`；仅需要 checkout 的 job 授予 `contents: read`（含两个 callee 调用 job，
  以允许 callee 的 classify 只读 checkout 自身 automation）。无写权限、无 secret、无 OIDC、
  不向 callee 透传 secret。
- 远程 `uses` 全部完整 40-hex SHA pin：`actions/checkout@11d5960a326750d5838078e36cf38b85af677262`
  （经只读 `git ls-remote` 核验为 actions/checkout v4 的真实 tag 指向）。
- **source 与 automation 身份分离**：caller 只传 `source_sha: ${{ github.sha }}`（被测提交，
  纯身份，不执行）；callee automation 由**已发布 callee** 经自身 `job.workflow_repository`/
  `job.workflow_sha` 解析，不解析 caller 变量。

## 4. 分类与结果：真实复用已发布 callee

影子**不实现**分类/结果逻辑，只调用已发布、已独立审查的 framework reusable callee：

```
yqm-307/bbt-framework/.github/workflows/bbtools-classify-v1.yml@1c0b0fb0ebc8e7ca3888aa4f6a74d1baecf08cc7
```

- **分类（`plan` job，classification-only）**：`changed_files` 由本仓真实 diff 计算
  （`scripts/ci/changed_files.py`；未知/超预算保守回退为空 → callee 判 `unknown` → 执行全部），
  作为受限数据传入；不接受调用方任意 shell/jobs 输入（本 workflow 无 `workflow_call` 输入）。
- **required/optional 设计**：`required=["changes","plan"]`（恒跑、防全跳过变绿）；
  `optional=["build"]`（重型构建，仅 `docs-only` 允许显式跳过）。
  - `code`/`unknown`：`build` 必跑，失败 → `failure`；
  - `docs-only`：`build` 允许跳过 → `success`；
  - **classifier 失败/未知 → `unknown` ≠ `docs-only` → 禁止跳过 `build`**，不会借 docs 变绿。
- **结果（`result` job，`if: always()`）**：把三个真实 `needs.*.result`
  （`changes`/`plan`/`build`，含 `skipped`）交给 callee result 契约；任一必跑
  `failure`/`cancelled`/空/意外 `skipped` → `verdict=failure` → 调用非零失败。
  复用与 `plan` 相同的受限输入；当 `changes` 失败致其 outputs 为空时，`result` 显式回退为
  保守值（`changed_files=[]` + `classifier_status=unknown` + 有界 event），使 callee 判
  `unknown`→执行全部，从而**仍能汇聚真实 job 结果**（必跑 skipped 即 `failure`），而不是因缺
  输入被拒绝、绕过聚合。

## 5. 工具链复现（hosted 缺口）

现役 ARC 日志实证运行时为 `g++ 13.3.0 / cmake 3.28.3 / Boost 1.90`（`/opt/boost`，组件
`context`）。hosted `ubuntu-24.04` 默认编译器/cMark 与现役同源（gcc/g++ 13.3），**唯一缺口是
Boost 1.90**（hosted 预装版本更旧）。按「不能用 hosted 预装版本假替代」，`prepare_boost.sh`
把锁定 Boost **源码编译**到工作区前缀：

- 版本/校验值沿用 framework 已发布 `docker/toolchain.lock`：
  `BOOST_VERSION=1.90.0`、`BOOST_SHA256=5e93d582…eea9`（下载后 sha256 fail-closed）；
- 只编实际消费组件 `context`（现役日志 `found components: context` 实证）；
- 不写 `/usr/local`、不装系统包、不改镜像；配置期以 `BOOST_ROOT`/`CMAKE_PREFIX_PATH` 传入。

## 6. 判据等价与影子强化

`run_infra_gate.sh` 逐项复现现役 `build-and-test` 判据：资源前缀 fail-closed 回归 **A/B**
（不含 C/D，与现役同范围并如实声明）、hiredis/mongoc/mongocxx **固定 tag+SHA** 前缀
（复用本仓唯一 recipe `scripts/prepare_resource_deps.sh`）、上游 HEAD 记录、干净性预检、
`cmake` Release + `BUILD_TESTING=ON` + 源码接入 + 前缀、Ninja 构建、`redis.unit`/`mongo.unit`
注册断言、`ctest -j1 --output-on-failure --timeout 180`、依赖来源断言（`libbbt_coroutine.so`
必检 / `libbbt_core.so` 告警 / 关键二进制 `ldd`）。

**影子强化点（不弱于现役，不借绿）**：live 组（`redis.live`/`mongo.live`/`redis.cotcp_binding.live`）
在无容器时如实标 `Skipped` 且**不冒充已通过**；若出现**非 live 测试被跳过**（意外 skipped）即失败。
这比现役 `ctest` 更严，用于兑现「意外 skipped 不得绿」。

## 7. 与现役 ci.yml 的耦合（防漂移）

因「不改现役 `ci.yml`」，其构建步骤与 `run_infra_gate.sh` 各存一份；`run_tests.py`
的 `CiYmlCouplingTests` 逐项对照两份：资源依赖固定 tag/commit、`ctest` 判据文本、
`redis.unit`/`mongo.unit` 注册集、资源前缀 A/B 入口、`cmake` 关键选项、依赖来源断言。
任一侧改动而另一侧未同步即测试失败。

## 8. 本地验证（本轮真实执行）

```bash
cd <infra-worktree>
# 配方守卫 + changed_files 路由 + 与 ci.yml 耦合（stdlib，无需 PyYAML）
PYTHONDONTWRITEBYTECODE=1 python3 scripts/ci/tests/run_tests.py
# 追加：workflow 静态契约（需 PyYAML）+ 分类路由冒烟（复用已发布 callee 逻辑）
PYTHONDONTWRITEBYTECODE=1 \
  BBT_CI_SHARED_DIR=<framework-checkout>/scripts/ci/shared \
  python3 scripts/ci/tests/run_tests.py
```

真实结果（本轮）：

- 默认 `python3`（无 PyYAML）：`Ran 39 tests ... OK (skipped=23)`（16 通过；23 显式 SKIP——静态
  workflow 契约与分类路由冒烟，**SKIP 不等于通过**）。
- `/usr/bin/python3`（PyYAML 6.0.3）+ 已发布 callee `scripts/ci/shared`：
  `Ran 39 tests ... OK`（全绿，含 14 静态契约 + 9 分类路由用例）。
- 新增前缀回归：按真实 workflow 的目录布局模拟 Boost 安装，执行真实资源 recipe 的前缀检查；
  在首个 clone 处由离线 git shim 阻断，不下载、不编译。回退旧布局会失败，独立前缀通过。

**未执行**：任何真实 hosted run、C++ 全量构建、远端调用；这些走父级授权的 PR CI。

## 9. 未覆盖 / 非等价 / 阻塞

- **分类口径**：docs-only 来自 callee，与现役 `ci.yml` 的 grep 口径存在已知差异，只影响
  哪些变更跳过 build；本文“同判据”仅指真正执行的构建/ctest 判据，不声明分类集合相同。
- **上游身份**：coroutine@main、core@master 仍沿用现役的可变分支头，实际 SHA 由 run 记录；
  callee 的 `source_sha` 40-hex 约束不等于这两个 checkout 已固定。后续迁移需单独固定验收版本。
- **缓存范围**：本影子“无缓存”指无跨 run 缓存复用；资源 recipe 可能自动启用 runner 内 ccache，
  不恢复/保存缓存，主 infra 构建不启用 launcher。
- **跳过守卫**：首个线上 run 必须核对 `ctest-shadow.log` 的实际 Skipped 行与守卫解析一致，
  离线配方测试不能替代这一实测。

- **callee runner**：已发布 callee 内部纯逻辑 job 固定 `ubuntu-latest`（caller 不可改），当前
  `ubuntu-latest==24.04`；影子仅负责自身 job 固定 `ubuntu-24.04`。
- **cmake 版本**：hosted 的 cmake 版本可能高于现役 ARC 的 `3.28.3`（本仓 `CMakeLists.txt` 只需
  `>= 3.16`）；真实 run 需读回实际 `cmake/g++/ninja` 版本作为证据。编译器期望与现役同为 g++ 13.3。
- **Boost 组件面**：只编 `context`（依现役日志实证的唯一编译组件）；若上游 coroutine 新增 Boost
  编译组件，需在此追加并同步验证（不预先扩编）。
- **双份构建配方**：见 §7 耦合测试兜底；根治需在现役 `ci.yml` 改为调用同一脚本（本轮未授权）。
- **未在线事实**：reusable 跨仓调用的在线身份/权限、hosted 工具链实际版本、Boost 下载校验的
  真实执行均属在线事实，本地不造值。

## 10. 回滚

影子为纯新增、无写副作用：回滚 = 删除新增的 `.github/workflows/ci-shadow-v1.yml`、`scripts/ci/`
与本文档；无可回滚的现役状态、无外部写入、无 artifact/cache 需清理。
