# AGENTS.md — bbtools-infra

## 工程介绍

现代 C++ 工具层与基础接入层：第三方适配、执行域、错误映射、连接/资源生命周期与基础动态配置。不承载 framework 的 App/Service、业务 schema、发现治理、灰度发布或配置生效策略，也不是 IaC 仓。

- 依赖方向固定 `bbt-framework → bbtools-infra → bbtools-coroutine`，禁止反向依赖、环依赖和引用兄弟模块内部实现。跨仓契约以契约所在仓文档为真源，变更由用户拍板；跨仓架构基调见 framework 仓 `docs/architecture/distributed-framework-baseline.md`（本地并列检出时为 `../bbt-framework/docs/architecture/distributed-framework-baseline.md`），本仓只实现 infra 机制。
- C++17 基线，优先标准库与 Boost 1.90，模板适度；命名空间 `bbt::{大模块}::{小模块}`，最多三层。
- 顶层目录：`bbt/`、`tests/`、`examples/`、`proto/`、`cmake/`、`docs/`、`scripts/`、`.github/`、`.config/`，另有 `third-party/`（只固定公共规范 submodule，不放本仓实现、依赖或运行产物）。不新增顶层目录。

## 规范与真源入口

先按任务选一条路径，不全量预载。

| 任务 | 公共规范（pub submodule） | 本仓真源 |
|---|---|---|
| 设计 API、模块边界、失败与关闭语义 | `third-party/bbtools-pub/docs/10-design/README.md` | `docs/architecture/`（执行域、关闭、生命周期接缝、RPC 分层等契约）；稳定取舍见 `docs/decisions/` |
| 写或改实现 | `third-party/bbtools-pub/docs/20-coding/README.md`（C++、注释、文档专项按入口展开） | `docs/conventions.md`（命名、目录、依赖、所有权、执行域与 Co-only 入口、示例）；格式以 `.clang-format`、`.editorconfig` 为真源 |
| 评审 | `third-party/bbtools-pub/docs/30-review/README.md` | 本文「审查门禁」；`docs/guides/development.md`「审查与交付」 |
| 设计测试、选择运行范围 | `third-party/bbtools-pub/docs/40-testcase/README.md` | `docs/guides/testing.md`、`docs/conventions.md` §6 |
| 使用或改某个模块 | — | `README.md` 模块表（交付状态真源，链到各模块 `docs/guides/*.md`）、`examples/` |
| 开发流程、验证、发布 | — | `docs/guides/development.md`（隔离、接口职责、依赖版本、本地验证、审查交付、旧仓迁移）、`docs/guides/ci.md` |

- 优先级：用户最新明确指令 > 本文与本仓 `docs/` > pub；用户最新明确指令同样优先于历史 Issue/PR。冲突时以本仓为准，并在交付中报告差异；pub 未覆盖的本仓特例写在本仓文档，不改 pub 来迁就本仓。
- pub 是 Git submodule，固定到已审阅提交；`git submodule update --init --recursive` 后读取。更新指针时先核对提交与变更范围；不读取仓外 pub 工作树的未提交改动。
- 新 worktree/clone 不带入未提交规范；开工先核对实际规范快照与依赖版本。

## 本仓特有规则

<!-- omh:agent-instructions:begin -->
- 执行域、Co-only 入口与存量迁移遵循 [docs/conventions.md](docs/conventions.md) 第十一节；旧示例不得作为新增代码的入口或原生等待先例。
<!-- omh:agent-instructions:end -->

- **adapter 边界**：第三方类型、异常、线程模型不进入公共 API；adapter 负责 I/O、取消/关闭、错误映射与资源寿命，coroutine 只负责运行时等待/唤醒。等待结束、逻辑关闭与物理回收按各模块契约区分。
- **源码布局**：源码统一在 `bbt/infra/<模块>/`，公共 API 命名空间 `bbt::infra::<模块>`；公共头与实现按模块并置，私有实现放模块 `detail/`，公开 target 导出仓根 include 路径。模块构建开关以 `CMakeLists.txt` 的 `option()` 为准。
- **依赖**：`bbt_core` 已冻结，不新增依赖（`BBT_CORE_SOURCE_DIR` 仅为过渡兼容开关，见 `docs/conventions.md` §3）。依赖版本按 `docs/guides/development.md`「依赖版本选择」：设计指定版本，开发/构建有指定按指定、未指定用本地最新源码，不要求先提交才可联调。
- **需求真源**：GitHub Issue/PR/Review 是需求、决策、进度与验收的消息真源；动手前读对应线程，无法访问时明确说明；消息、契约、实现、测试冲突时显式记录，不静默选择。
- **实施边界**：用户明确要求实施，或所涉 Issue 已有方案共识时直接实现；改公共 API 或契约、且无共识也未被明确要求直接实施时，先「现状分析 → 方案共识」。只在疑问会改变做法且自己查不到时才问。
- **跨仓协作**：用户可直接与本仓 Agent 确认任务；跨仓契约与相关拥有者协调，不能代改其他仓。framework 外程序可直接消费本仓；经 framework 使用不得绕过其生命周期、配置或安全约束。
- **旧仓迁移**：旧仓（bbtools-http/rpc/network/conet）只作历史素材；归档、只读、删除、重命名或消费者切换须先确认，细则见 `docs/guides/development.md`「旧仓迁移与支撑缺口」。
- **环境**：不假设链接库或系统工具可用（先查 `CMakeLists.txt` 与邻近文件）；不修改 `.env`、密钥或凭据；不终止他人进程。

## 门禁

### 授权

- 只改获授权范围，不覆盖他人已有改动；Issue/任务存在不等于实施授权。并行写任务使用独立工作区、索引、build 和依赖目录。
- 须先获用户授权：commit、push、开 PR、付费调用、部署、破坏性操作、新依赖、CI/发布变更、公共接口或数据布局破坏性变更、系统安装或运行基础设施操作。
- 任务会产生改动时，开工时一次问清是否授权 commit/push/开 PR（无法询问时按未授权处理）；授权覆盖整段任务、不逐步再问；未授权则停在 commit 前，交付改动与验证结果。

### 本地构建与验证

- 构建并发固定为 2：`flock /tmp/bbt-build.lock cmake --build <dir> --parallel 2`，configure 使用 ccache；测试 `ctest -j1`。禁止无参数并行构建。
- 本地验证覆盖冒烟、本次功能及直接耦合测试，三者全过；本地不跑全量。具体命令选自 `docs/guides/development.md`「本地验证」，不另起流程。
- CI 分层（矩阵见 `docs/guides/testing.md` §4）：PR/push 只跑 smoke 与按变更映射的 focused；完整回归与深度门禁走 `workflow_dispatch` 的 `test-depth=extended|full`；真实 Redis/Mongo 由 `live` job 在 `test-depth=full|live` 时运行，普通 PR 不跑。
- 并发/生命周期测试先证明进入目标阶段，再触发故障/关闭并检查结果与资源收口；fixture 不替代真实后端集成。
- 提交前 `bash scripts/ci/check_format.sh --staged`；文档改动跑 `git diff --check` 并核对引用路径与命令。
- 不把本地结果说成远端 CI 结果；不把 WARN 说成 PASS；缺环境、权限或依赖时标 `BLOCKED`，不以 stub、跳过断言或放宽检查伪装完成。

### 提交与合入

- 标题 `<type>(<scope>): <简洁中文说明>`；单次提交不混无关变更；格式化变更单独提交。
- 目标测试通过且无新增编译警告后才允许 commit；交付前检查无意外改动并报告未覆盖项。
- Author 固定为用户本人 `yqm-307`：用本机 git 配置（`user.name=yqm-307`）直接提交，不加 `--author`，不使用 agent 身份。
- 本地 push 门禁：`git config --local core.hooksPath .config/bbt-git-hooks` 启用（先查已有值，不覆盖其他 hook 系统）；本仓规则配置在 `.config/bbt-git-hooks.toml`，规则与 review ticket 格式见 `third-party/bbtools-pub/tools/git-hooks/README.md`。代码 commit 审查通过后，ticket 放在紧随其后的单独 commit（只改 `.config/review-tickets/`）。
- 不直推 `main`、不自行合并或发布；PR 关联 Issue，写明范围、验证命令、结果与未覆盖边界。远端写入后回读确认目标对象。
- PR 唯一 required check 为 `result`（语义见 `docs/guides/ci.md`）。CI 失败先读日志定位根因，不靠重跑掩盖；业务代码修复可自主做，CI/CD 配置修改须先获确认。

### 审查门禁

以下改动须独立审查（作者不自审放行），其余按风险自审并跑相关测试：

- 受保护文件（含本 `AGENTS.md`）、公共契约、权限；
- 等待/唤醒/取消竞争、调度状态机、对象寿命与资源收口路径；
- 本仓文档另列须独立审查的类别，从其规定。

审查按风险收敛：通过即停，不因非阻塞措辞反复整树复审。

### 仓库卫生

- 文档一事一处，随代码在同一 PR 更新；README 只保留状态与入口（≤200 行，`docs-check` 校验）；ADR 编号唯一递增，合入后只追加取代关系。细则见 `docs/conventions.md` §8。
- 注释说明 WHY，不重复 WHAT；必须保留时序与所有权约束，公开头文件开头说明职责；不留 Issue/PR、审查/阶段代号、契约行号或临时版本号；形态规则见 `third-party/bbtools-pub/docs/20-coding/documentation.md`。
- 构建产物只进已忽略的 `build/`、`build-*/`；运行证据、原始日志、私有依赖前缀（`.deps/`）不入仓。
