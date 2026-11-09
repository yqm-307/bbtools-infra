# CI 说明

普通入口为 `.github/workflows/ci.yml`（hosted `ubuntu-24.04`）。[测试策略](testing.md)定义 label/profile，本文维护门禁与依赖入口；实际脚本是可执行真源。

## 触发与结果

`changes → docs-check → build-and-gates → result`；`result` 是唯一 required check。PR/push 默认 Release 构建、smoke 与必要 focused，`workflow_dispatch test-depth=extended|full` 增加深度门禁，`full|live` 增加独立 live job。docs-only 才能跳过构建；未知分类保守执行。`result.py` 只容许显式声明的计划跳过，结果键缺失、失败、取消或意外跳过一律失败。

`changed_files.py` 从真实 diff 归一受限变更集。CMake、Python、workflow、`scripts/ci/`（包括必选清单）一律是 code；空集、未知 revision 或超预算是 unknown。活动路径映射覆盖 `bbt/infra/`：net 改动带 HTTP/Redis/RPC，HTTP 带 RPC/MCP HTTP，config/Mongo helper 各自带本模块；构建入口带新栈闭包。PR focused 使用 `--label-expr "unit|integration"`，真实服务项由 live 门禁覆盖。

## 依赖

- `prepare_boost.sh`：固定 Boost 1.90 context，校验 sha256，私有前缀。
- `prepare_deps.sh`：固定 hiredis 1.4.0、mongoc/mongocxx、protobuf 3.21.12，与 coroutine pin。
- `deps_bundle.sh`：同 run pack/unpack，expected SHA 与归档 sha256 双校验，保留 symlink。
- `verify_coroutine_pin.sh`：`cmake/coroutine.sha` 的 40hex 格式与精确检出。
- 模块开启但缺 hiredis/protobuf 前缀时 configure 必须失败；HTTP-only 不拉 protobuf，也不注册 RPC 测试。

跨 run 只复用两类内容，均在 `build-and-gates` 内、装在 `runner.temp`：

- **ccache 对象**：上限 500M。key 由完整工具链/依赖指纹（ccache 实际版本、`g++ -dumpversion`、`infra/CMakeLists.txt` 与 `infra/cmake/**` 哈希）加 run_id/run_attempt 组成；restore 前缀保留完整指纹，不跨指纹复用。ccache 只安装在 hosted 临时 runner，本机不安装；安装不钉 apt 包版本，只要求可用且版本号可解析。`max_size/compression/compiler_check/base_dir` 在**恢复之后**重新固化（归档自带的旧 `ccache.conf` 会覆盖安装时的设置），同一处 `ccache --zero-stats`，统计只反映本次 run。
- **Boost 源码归档**：只缓存锁定 `boost_1_90_0.tar.gz`，恢复后 `prepare_boost.sh` 仍做完整 SHA256 校验；缺失、损坏或 symlink 一律回退下载，回写失败只告警。不复用已安装 prefix 或 build 树；该条目只由 main push 建立。

`cache-bypass` / `cache-enabled=false` 跳过 ccache 安装与全部 restore/save，并由初始化步骤导出 `CCACHE_DISABLE=1`——即使镜像预装 ccache，`prepare_resource_deps.sh` 见到它也不会真正使用对象缓存；fork PR 保持对象缓存禁用，只可读内容寻址且逐次校验的源码归档。缓存可见范围由 GitHub ref 作用域决定（PR 条目写在 merge ref，main 读不到），key 与 `if` 只是选择条目，不是安全边界；缓存只影响耗时，不改变任何门禁判据或测试选集。

四个 cache action 都是 `continue-on-error: true`：缓存服务或配额异常只失去复用，构建与门禁照跑；`ccache --show-stats` 只作证据。缓存目录在 job 级 `env` 里取不到 `runner.temp`（该位置无 `runner` context），由 `build-and-gates` 首个 run 步骤写进 `$GITHUB_ENV`。`scripts/ci/tests/run_tests.py`（缓存契约、fork/bypass、Boost SHA 等）在同一 job 内作为独立步骤执行，失败即阻断，不再只在本地跑。

## 执行门禁

| 入口 | 必须证明 |
|---|---|
| `release_gate.sh RELEASE_GATE_MODE=smoke` | 显式启用 HTTP/Redis/RPC、锁定依赖，完整构建；新 Redis、Mongo、payload 注册存在；按 `required_smoke.txt` 运行 smoke，注册集=实跑集且零跳过 |
| `run_test_profile.sh` | smoke/focused/full 选集非空；注册集、JUnit 精确一致；失败/error/skipped 不得降级通过 |
| `release_gate.sh RELEASE_GATE_MODE=full` | 非 live/perf 完整回归、新 Redis 独占容器全注册集、新 HTTP/RPC 契约、HTTP-only 与 RPC 缺前缀拒绝；Release 归档零 debug 观测符号及依赖来源核查 |
| `debug_gate.sh` | `required_debug.txt` 的 8 项（contract/payload、新 HTTP 请求与关闭、新 Redis 分帧与关闭、Mongo worker）；Mongo Debug 归档含统一观测符号；另跑新 HTTP/RPC 完整契约集 |
| `network_gate.sh` | net 严格闭包完整注册集与 JUnit 一致、零跳过；既有必选清单和 pingpong 复跑不放宽 |
| `redis_net_gate.sh` | 新 Redis 独立构建，复用 `tests/redis-live/run.sh` 的独占后端门禁 |
| `live_redis_gate.sh` | 校验新 Redis 测试和示例实际存在，复用同一 harness；不保留其它 Redis transport 链 |
| `live_mongo_gate.sh` | 保留 Mongo 在线、停止态错误、重启恢复的真实容器验收 |

HTTP/Redis 新栈不使用 Mongo 的统一 debug 计数；它们由生产状态、真实 loopback 与关闭断言验证，不要求虚构 debug 符号。Release 归档路径为 `bbt_infra_http_net/`、`bbt_infra_redis_net/` 与 `bbt/infra/`，不再使用旧目录。

## 真实 Redis 与 label

`tests/redis_net` 中需要 Redis 的命令、连接池、握手、宿主排空用例标 `live`；分帧、故障、关闭竞争等独占原始对端用例保持 `unit`。smoke 代表项使用后者，不依赖共享服务。`tests/redis-live/run.sh <build-dir>` 启动动态 project/端口的真实 Redis 容器，运行 **全部** `redis.net.*`（包含 unit/live/async 编译项）和公开消费示例，检查注册=执行、零跳过；trap 只清理本 project。缺 docker、compose、产物或健康超时直接失败。

live workflow 显式开启新 Redis，只构建 `Test_redis_net_contract`、`redis_net_example`、`Test_mongo_live`，不重跑本地全量。普通 PR 不运行外部服务，深度入口不删除真实后端覆盖。

## 维护检查

```bash
PYTHONDONTWRITEBYTECODE=1 python3 scripts/ci/tests/run_tests.py
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest scripts/ci/test_changed_files.py scripts/ci/test_check_docs.py
python3 -I scripts/ci/check_docs.py --root . --readme-max 200
```

`check_format_diff.sh <base>` 只核对改动 C/C++ 行。本地仍只跑 smoke、本次功能和直接耦合选集，重任务通过 `flock /tmp/bbt-build.lock` 串行、ccache、构建 `--parallel 2`、测试 `ctest -j1`。脚本已接线不代表远端 CI 或跨仓集成已通过。
