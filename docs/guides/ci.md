# CI 说明

普通入口为 `.github/workflows/ci.yml`（hosted `ubuntu-24.04`）。[测试策略](testing.md)定义 label/profile，本文维护门禁与依赖入口；实际脚本是可执行真源。

## 触发与结果

`changes` 判定变更分类；`docs-check` 恒执行，`build-and-gates` 与 `live` 按分类与 `test-depth` 执行或按计划跳过；`result` 汇总判定，是唯一 required check。触发矩阵见 [测试策略](testing.md) §4。docs-only 才能跳过构建；未知分类保守执行。`result.py` 只容许显式声明的计划跳过，结果键缺失、失败、取消或意外跳过一律失败。

`changed_files.py` 从真实 diff 归一受限变更集。CMake、Python、workflow、`scripts/ci/`（包括必选清单）一律是 code；空集、未知 revision 或超预算是 unknown。PR focused 的路径到测试名映射见 [测试策略](testing.md) §4；真实服务项由 live 门禁覆盖。

## 依赖

版本与来源锁定见 [resource-prefixes](../dependencies/resource-prefixes.provenance.md) 与 [protobuf 3.21.12](../dependencies/protobuf-3.21.12.provenance.md)，本节只说明脚本职责。

- `prepare_boost.sh`：按锁定版本构建 Boost `context`，校验 sha256，安装到私有前缀。
- `prepare_deps.sh`：核验 coroutine 检出 SHA 与 pin 相等；调用 `scripts/prepare_resource_deps.sh` 构建 hiredis/mongoc/mongocxx；protobuf 由 framework 发布的配方安装（按固定 SHA 取回，sha256 校验后执行）。
- `deps_bundle.sh`：同 run pack/unpack，expected SHA 与归档 sha256 双校验，保留 symlink。
- `verify_coroutine_pin.sh`：`cmake/coroutine.sha` 的 40hex 格式校验；精确检出由 workflow 完成。
- 模块开启但缺 hiredis/protobuf 前缀时 configure 必须失败；HTTP-only 不拉 protobuf，也不注册 RPC 测试。

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

`tests/redis_net/CMakeLists.txt` 把命令、连接池、握手、宿主排空等需要真实 Redis 的用例标为 `live`；分帧、连接失败与关闭竞争的代表项（`fragmented_reply`、`connect_failure_owned`、`close_pending_command_once`）为 `smoke;unit`，使用独占原始对端，不依赖共享服务。`tests/redis-live/run.sh <build-dir>` 启动动态 project/端口的真实 Redis 容器，运行 **全部** `redis.net.*`（包含 unit/live/async 编译项）和公开消费示例，检查注册=执行、零跳过；trap 只清理本 project。缺 docker、compose、产物或健康超时直接失败。

live workflow 显式开启新 Redis，只构建 `Test_redis_net_contract`、`redis_net_example`、`Test_mongo_live`，不重跑本地全量。普通 PR 不运行外部服务，深度入口不删除真实后端覆盖。

## 维护检查

```bash
PYTHONDONTWRITEBYTECODE=1 python3 scripts/ci/tests/run_tests.py
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest scripts/ci/test_changed_files.py scripts/ci/test_check_docs.py
python3 -I scripts/ci/check_docs.py --root . --readme-max 200
```

`check_format_diff.sh <base>` 只核对改动 C/C++ 行。本地命令与并发约束见 [测试策略](testing.md) §6。脚本已接线不代表远端 CI 或跨仓集成已通过。
