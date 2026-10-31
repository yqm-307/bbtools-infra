# bbtools-infra CI 说明

本文是 bbtools-infra 持续集成的唯一落点文档（原 `docs/ci/` 已收敛至此）。它描述
`.github/workflows/ci.yml` 的组成、门禁清单与固定输入；workflow 文件本身是权威定义，
本文不复制其步骤，只给门禁索引与语义。

## 1. 组成

| 文件 | 作用 |
|---|---|
| `.github/workflows/ci.yml` | 普通 CI 入口：`changes → docs-check → build-and-gates（deps + Release/debug/network/redis-net 串行）→ live → result`（hosted `ubuntu-24.04`） |
| `scripts/ci/changed_files.py` | 从真实 git diff 归一受限变更集，并完成路径分类（本仓内联，见 §2） |
| `scripts/ci/result.py` | result job 的本仓内联汇总：needs.*.result → verdict，fail-closed（见 §3） |
| `scripts/ci/prepare_boost.sh` | 按锁定版本/sha256 源码构建 Boost 1.90 `context` 到工作区私有前缀 |
| `scripts/ci/prepare_deps.sh` | Boost + hiredis/mongoc/mongocxx + protobuf 私有前缀 + coroutine pin 校验编排 |
| `scripts/ci/deps_bundle.sh` | prepared deps tar 打包/校验/解包（sha256 fail-closed，保留 symlink） |
| `scripts/ci/verify_coroutine_pin.sh` | 读取并校验 `cmake/coroutine.sha`（40hex 单行，格式不符即 fail-closed） |
| `scripts/ci/resolve_ccache.sh` | 解析本次 run 内 ccache launcher 启用条件（跨 run 缓存已阻塞） |
| `scripts/ci/release_gate.sh` | Release 构建 + 全量 ctest + 新 RPC/HTTP 契约 + HTTP-only + 符号断言 + ldd |
| `scripts/ci/debug_gate.sh` | Debug 构建 + 旧模块 9 项契约 + 新 RPC/HTTP Debug 契约 |
| `scripts/ci/network_gate.sh` | network-only 构建 + 全注册集实跑 + pingpong 复跑 |
| `scripts/ci/redis_net_gate.sh` | redis-net 构建 + 临时容器 + redis.net.* 全注册集实跑 |
| `scripts/ci/live_redis_gate.sh` | Redis live 门禁前置（docker/二进制 fail-closed） |
| `scripts/ci/live_mongo_gate.sh` | Mongo live 门禁前置（同上） |
| `scripts/ci/run_ctest_label.sh` | 按 label/regex 实跑并核对注册集与 JUnit 一致、零跳过 |
| `scripts/ci/check_network_tests.py` | network-only 注册集/JUnit 精确一致 + 缺项/重复/跳过失败 |
| `scripts/ci/check_docs.py` | docs-check：相对链接有效、docs 文件名无过程编号、README 行数上限 |
| `scripts/ci/check_format_diff.sh` | 只检查本 PR 改动行的 clang-format 违规 |
| `scripts/ci/test_changed_files.py` | 分类规则 + result 契约单测（仅标准库） |
| `scripts/ci/test_check_docs.py` | check_docs 正反例单测 |
| `scripts/ci/tests/run_tests.py` | CI 脚本离线冒烟与契约测试 |
| `required_*.txt` | 注册集必选清单（required_rpc_http_net / required_debug_legacy / required_network_tests） |

**分类/结果已本仓内联**：framework 共享的 `bbtools-classify-v1` reusable callee
链及其 `scripts/ci/shared/**` 逻辑已删除（方案 D3：各仓用自己的 changes + result
job）。分类规则由 `scripts/ci/changed_files.py` 承载，结果判定由
`scripts/ci/result.py` 承载。

## 2. 路径分类（changes job）

`changed_files.py` 把真实仓库 diff 归一为仓库相对路径列表，并按下列规则分类：

| 路径形状 | 判定 |
|---|---|
| `CMakeLists.txt`（任意目录） | code |
| `*.cmake`、`*.py` | code |
| `.github/workflows/**` | code |
| `docs/` 前缀、`*.md`/图片/`*.txt`、`LICENSE` 等根目录文书 | doc |
| 其余（源码、脚本、锁文件等） | code |

变更集判定：**全部路径为 doc → `docs-only`**；含任一 code 路径 → `code`；
空集或分类器非 ok（diff 失败/未知 revision/超预算）→ `unknown`，保守执行全部。
`docs-only` 才允许重型 job（build-and-gates、live）跳过；
`code`/`unknown` 禁止任何跳过。分类用例集见 `test_changed_files.py`（覆盖
`CMakeLists.txt`、`*.cmake`、`*.py`、workflows、纯 md、混合、空集、非 ok 状态）。

## 3. 结果汇聚（result job）

workflow 中 `result` job（`always()`，needs：`changes`、`docs-check`、
`build-and-gates`、`live`）是唯一 required check，调用 `result.py`，其中
`required = [changes, docs-check]`、`optional = [build-and-gates, live]`；
分类为 `docs-only` 时才允许两个 optional job 为 skipped。`result.py` 的判定规则：

- `required` 集合必须非空（空即拒绝，防「全跳过变绿」）；
- 任一 required 为 `failure`/`cancelled`/缺失/`skipped` → verdict=failure；
- results 出现 required∪optional 之外的 job 或非法状态值 → 拒绝；
- optional 非 success 时：`skipped`/缺失仅当 classification=docs-only 放行；
- 无任何真实 success 不得判绿；
- verdict=failure → 退出码 1；契约拒绝 → 退出码 3。

## 4. 门禁清单（不得放宽）

- **依赖闭包**：`deps_bundle.sh` pack/unpack 以 sha256 + expected-sha 双锚定；
  Boost 1.90 `context` 源码复现到 `boost-prefix`（sha256 fail-closed）；
  hiredis/mongoc/mongocxx 固定 tag+SHA 源码构建到 `deps-prefix`；
  protobuf 3.21.12 私有前缀经固定 40hex SHA 配方下载 + sha256 fail-closed 校验；
  coroutine HEAD 必须命中 `cmake/coroutine.sha` pin（双钉：bundle + pin）。
- **no-SKIP**：ctest 注册集与实跑集精确一致；JUnit 无 `skipped`/`failure`/`error`；
  `ctest -j1 --no-tests=error --output-on-failure`。
- **资源前缀拒绝路径**：`tests/resource-prefix/run.sh` A/B（已存在目录/悬浮
  符号链接/锁占用必须 exit 2；克隆失败不留半个前缀）。
- **Release 判据**：全量旧模块 + `rpc.payload`；新 RPC/HTTP Release 闭包
  （`BBT_INFRA_HTTP_NET=ON` + `BBT_INFRA_RPC_NET=ON` + 锁定 protobuf）注册集与
  JUnit 精确一致（RPC 21 项 + HTTP 31 项，必选清单见
  `scripts/ci/required_rpc_http_net.txt`）；HTTP-only 闭包（`RPC_NET=OFF` +
  `BBT_PROTOBUF_PREFIX=`）构建 `libbbt_infra_http_net.a` 不含 protobuf 符号，
  `Test_http_net_contract` 实跑 `http.net.get_post_reuse`；RPC 缺锁定前缀
  configure 必须拒绝且失败原因来自前置条件门禁；Release 三模块归档
  （http/redis/mongo）不得含 `bbt::infra::debug::` 符号；test 二进制 ldd 不得有
  `not found`、不得链接 `/usr/local` 下 bbt 产物。
- **Debug 判据**：`BBT_INFRA_STRINGENT_DEBUG=ON` 构建 9 项既有用例
  （`contract.basics`/`http.session_lifetime`/`http.timer_rearm`/
  `http.inflight_close`/`http.child_unregister`/`redis.unit`/
  `redis.cotcp_binding`/`mongo.unit`/`rpc.payload`）实跑零跳过；三模块 Debug
  归档必须含 `bbt::infra::debug::` 符号；新 RPC/HTTP Debug 闭包同一
  注册集/JUnit 精确一致。
- **network-only 判据**：`BBT_INFRA_NETWORK_ONLY=ON` 独立 Debug 构建，
  `check_network_tests.py` 核对注册集（≥111 项且含必选 `net.consumer`/
  `net.tcp_*`/`net.co_pingpong_strict`/`net.request_close_*`/
  `net.sealed_write_error_read_publication`）与 JUnit 实跑集精确一致、零跳过；
  `net.co_pingpong_strict` 额外 `--repeat until-fail:5` 复跑。
- **redis-net 判据**：`BBT_INFRA_REDIS_NET=ON` + `BBT_HIREDIS_PREFIX` 构建，
  临时容器（动态 project + 动态端口，`trap` 保证 `down -v` 清理本 project），
  `redis.net.*` 全注册集实跑零跳过，`redis_net_example` 冒烟。
- **live 判据**：独立 `live` job（`needs: build-and-gates`，与构建失败分离）。
  解包同 run 的 deps bundle，校验 bundle sha256 与 coroutine pin 双钉后，只构建
  `Test_redis_live`、`Test_redis_cotcp_binding`、`Test_mongo_live` 三个目标，
  再依次运行 `live_redis_gate.sh`、`live_mongo_gate.sh`（内部调
  `tests/redis-live/run.sh`、`tests/mongo-live/run.sh`，A/B/C 阶段：在线 →
  停止态必须失败 → 重启恢复）；缺 docker/compose/live 二进制一律非零失败，
  不得 skip 变绿。step 上限（redis 19min / mongo 16min）大于脚本内界之和，
  避免外框硬杀导致 trap 不执行残留；Mongo live 失败时上传诊断产物。
- **docs-check**：`check_docs.py --root <repo> --readme-max 200`；
  docs-only PR 只跑 docs-check。
- **format-diff**：`check_format_diff.sh <base>` 只检查本 PR 改动行的
  C/C++ 行，不强制格式化存量代码。

## 5. ctest label 与本地等价命令

测试 label 语义与分配见 `docs/conventions.md` §6：

| label | 含义 | 主要注册集 |
|---|---|---|
| `smoke` | 构建后秒级冒烟 | `contract.basics`、`config.unit`、`net.*` 部分 |
| `unit` | 模块内行为/边界，无外部服务 | 主配置 21 项 + `tests/net` 168 + `http_net`/`rpc_net`/`redis_net` |
| `integration` | 编译/配置/消费面门禁 | `mongo.async_compile`、`net.freeze_public_surface`、`net.consumer*`、`net.pin_*` |
| `live` | 真实外部服务 | `redis.live`、`redis.cotcp_binding.live`、`mongo.live`、`mongo.client.consumer` |
| `perf` | 基准/性能回归 | `net.perf_smoke_*`（`BBT_INFRA_PERF_BENCH=ON`） |

```bash
# 分类 + result 契约单测（仅标准库）
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest scripts/ci/test_changed_files.py

# check_docs 单测
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest scripts/ci/test_check_docs.py

# CI 脚本离线冒烟（含 bash -n、deps_bundle 往返、pin 校验、report 门禁）
PYTHONDONTWRITEBYTECODE=1 python3 scripts/ci/tests/run_tests.py

# docs-check 本仓自检
python3 -I scripts/ci/check_docs.py --root . --readme-max 200

# 本地 Release 构建 + 全量 ctest（等价 release_gate.sh 主路径）
cmake -S . -B build -G Ninja \
    -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON \
    -DBOOST_ROOT=<boost-prefix> -DCMAKE_PREFIX_PATH=<boost-prefix> \
    -DBBT_COROUTINE_SOURCE_DIR=<coroutine-src> \
    -DBBT_CORE_SOURCE_DIR=<core-src> \
    -DBBT_HIREDIS_PREFIX=<deps>/hiredis \
    -DBBT_MONGOCXX_PREFIX=<deps>/mongocxx \
    -DBBT_MONGOC_PREFIX=<deps>/mongoc
cmake --build build --parallel 3
ctest --test-dir build -j1 --output-on-failure --timeout 180

# 按 label 实跑（等价 run_ctest_label.sh）
ctest --test-dir build -N --show-only=json-v1 -L 'smoke|unit'
ctest --test-dir build -j1 -L 'smoke|unit' --no-tests=error \
    --output-on-failure --output-junit build/ctest.xml
```

## 6. 触发与身份

| 维度 | 取值 |
|---|---|
| workflow 文件 / 名 | `.github/workflows/ci.yml` / `CI` |
| 触发 | `pull_request`→main、`push`→main、`workflow_dispatch`（`build-mode`、`cache-enabled`） |
| runner | 全部本地 job 固定 hosted `ubuntu-24.04` |
| 权限 | 顶层 `permissions: {}`；仅需 checkout 的 job 授予 `contents: read`；无 secret、无 OIDC、无写权限 |
| concurrency | PR：group 用 `github.ref`（只取消同一 PR 旧 run）；main/手动：group 含 `run_id`（逐提交独立、互不取消） |
| 远程 action | 全部完整 40-hex pin（checkout/upload-artifact/download-artifact） |

## 7. 缓存边界（明确阻塞）

hosted 每次干净全量构建；**跨 run ccache/GitHub cache 明确阻塞**——本 workflow
不引入未固定完整 SHA 的 cache action，也不 restore/save 跨 run 缓存。
`workflow_dispatch` 的 `build-mode`（`clean`/`cache-bypass`）与 `cache-enabled`
输入保留，仅决定本次 run 内 ccache launcher 是否启用（runner 镜像自带
ccache 时），不改变任何通过标准。
