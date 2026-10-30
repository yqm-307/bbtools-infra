# AGENTS.md — bbtools-infra

<!-- bbtools:common:begin -->
依赖方向固定为 `bbt-framework → bbtools-infra → bbtools-coroutine`，禁止反向依赖、环依赖和引用兄弟模块内部实现。
新 worktree/clone 不会带入未提交规范；开工先核对实际规范快照与依赖版本。
共享公共契约由单一负责人协调；并行写任务使用独立工作区、索引、build 和依赖目录，保留他人已有改动。
统一以 C++17 为基线；优先标准库与 Boost 1.90，模板适度，namespace 不超过三层（`bbt::{大模块}::{小模块}`）。
本地构建并发固定为 2；使用共享队列 `flock /tmp/bbt-build.lock`，构建显式 `--parallel 2`，configure 使用 ccache；测试用 `ctest -j1`。
本地验证必须覆盖冒烟、本次功能及直接耦合测试；本地不跑全量，完整测试走 PR CI。
只改获授权范围，不覆盖他人已有改动；Issue/任务存在不等于实施授权；commit、push、PR、付费调用、部署与破坏性操作须先获用户授权。
提交标题使用 `<type>(<scope>): <简洁中文说明>`，单次提交不混无关变更；目标测试通过且无新增警告后才允许 commit。交付前检查无意外改动并报告未覆盖项。Agent author 固定写法：`git commit --author="agent <agent@users.noreply.github.com>"`。
不直推 `main`、不自行合并或发布；受保护文件（含 `AGENTS.md`）及公共契约/权限/并发关键改动须独立审查。远端写入后回读确认目标对象。
文档一事一处、代码与对应文档同一 PR 更新；README 只保留状态与入口；ADR 使用唯一递增编号，合入后只追加取代关系。
注释说明 WHY，不重复 WHAT；必须保留时序与所有权约束，公开头文件开头说明职责；不留 Issue/PR、审查/阶段代号、契约行号或临时版本号。
新依赖、CI/发布变更、公共接口或数据布局破坏性变更、系统安装或运行基础设施操作，先确认。
<!-- bbtools:common:end -->

## 定位

本仓是现代 C++ 工具层与基础接入层：第三方适配、执行域、错误映射、连接/资源生命周期与基础动态配置。不承载 framework 的 App/Service、业务 schema、发现治理、灰度发布或配置生效策略，也不是 IaC 仓。

## 真源入口（按任务读取，不全量预载）

- 状态与文档地图：[README.md](README.md)；模块交付状态以 README 表格为准。
- 操作规范：[docs/guides/development.md](docs/guides/development.md)（隔离、接口职责、测试口径、本地验证、审查交付、旧仓迁移）。
- 编码与维护细则：[docs/conventions.md](docs/conventions.md)；C++ 格式唯一真源 `.clang-format`。
- 网络执行域契约：[docs/architecture/gnet-network-execution.md](docs/architecture/gnet-network-execution.md)；稳定取舍见 [docs/decisions/](docs/decisions/)。
- 模块契约/构建/边界：[docs/guides/redis-net.md](docs/guides/redis-net.md)、[docs/guides/http-net.md](docs/guides/http-net.md)、[docs/guides/rpc-net.md](docs/guides/rpc-net.md)、[docs/guides/config-watch.md](docs/guides/config-watch.md)。
- CI 编排与覆盖：[docs/guides/ci.md](docs/guides/ci.md) 与 [.github/workflows/ci.yml](.github/workflows/ci.yml)。
- 跨仓基调：framework 的 `bbt-framework/docs/architecture/distributed-framework-baseline.md`；本仓只实现 infra 机制。主 Issue：https://github.com/yqm-307/bbtools-infra/issues/1 。

## 本仓特有门禁

- `bbt_core` 依赖已冻结不再新增（`BBT_CORE_SOURCE_DIR` 仅为过渡兼容开关）；细则见 `docs/conventions.md` 第三节。新增 Redis/Mongo 公共 API 固定 `bbt::infra::redis`、`bbt::infra::mongo`，配置为 `bbt::infra::config`。
- adapter 边界：第三方类型、异常、线程模型不进入公共 API；adapter 负责 I/O、取消/关闭、错误映射与资源寿命，coroutine 只负责运行时等待/唤醒。等待结束、逻辑关闭与物理回收按各模块契约区分。
- coroutine 消费 pin 在 `cmake/coroutine.sha`，由 `bbt_coroutine_pin_verify()` 核验；新协议闭包构建开关 `BBT_INFRA_HTTP_NET` / `BBT_INFRA_REDIS_NET` / `BBT_INFRA_RPC_NET`。
- 公开头一般在 `include/`；新 net/Redis 模块例外，公开头与实现并置于 `bbt/infra/net/`、`bbt/infra/redis/`，由公开 target 导出仓根 include 路径。
- 并发/生命周期测试先证明进入目标阶段，再触发故障/关闭并检查结果与资源收口；fixture 不替代真实后端集成。Redis/Mongo live 容器编排归 Release job，普通 PR 不跑。
- 用户可直接与本仓 Agent 确认任务；跨仓契约与相关拥有者协调，不能代改其他仓。framework 外程序可直接消费本仓；经 framework 使用不得绕过其生命周期、配置或安全约束。
- 旧仓（bbtools-http/rpc/network/conet）只作历史素材；归档、只读、删除、重命名或消费者切换须先确认，规则详见 development.md「旧仓迁移与支撑缺口」。
