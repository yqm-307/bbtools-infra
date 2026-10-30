# tests/ 组织与约定

本目录是 infra 全部测试的注册与源码落点。约定见 `docs/conventions.md` §6：
文件 `tests/<module>/Test_<行为>.cc`、ctest 名 `<module>.<行为>`、label ∈
`smoke` / `unit` / `integration` / `live` / `perf`。

## 目录

| 目录 | 模块 | 构建开关 |
|---|---|---|
| `tests/` 平铺 `Test_*.cc` | 旧栈（http/tcp/udp/transport）+ config + redis/mongo 客户端 + rpc payload codec + 通用契约 | `BBT_INFRA_LEGACY_STACK` / `BBT_INFRA_CONFIG` / 模块 target 存在 |
| `tests/net/` | 新 gnet 网络面 | `BBT_INFRA_NET`（`BBT_INFRA_NETWORK_ONLY` 时唯一注册集） |
| `tests/http_net/` | 新 HTTP 协议面 | `BBT_INFRA_HTTP_NET` |
| `tests/rpc_net/` | 新 RPC 协议面 | `BBT_INFRA_RPC_NET` |
| `tests/redis_net/` | 新 Redis 协议面 | `BBT_INFRA_REDIS_NET` |
| `tests/mongo_async/`、`tests/redis_net/AsyncCompile.*`、`tests/net/consumers/` | 编译门禁/消费面 stub | 随对应模块 |
| `tests/redis-live/`、`tests/mongo-live/` | live 容器编排脚本（`run.sh` + `compose.yml`） | 手动驱动，注入 `BBT_TEST_REDIS_ADDR` / `BBT_TEST_MONGO_URI` |
| `tests/resource-prefix/` | 资源前缀拒绝门禁脚本 | 手动驱动 |
| `tests/net/perf/` | 纯 TCP 基准 harness（server/loadgen） | `-DBBT_INFRA_PERF_BENCH=ON` |

## 测试形态：两套并存，按可执行文件选择

仓库存在两种测试可执行形态，**选择按文件决定，不混用**：

1. **Boost.Test**（多数文件）：`BOOST_AUTO_TEST_SUITE` / `BOOST_AUTO_TEST_CASE`，
   一个可执行内多个 case，经 `--run_test=<suite>/<case>` 按 case 注册到 CTest。
   涉及 coroutine runtime 的文件用 `BOOST_TEST_NO_MAIN` +
   `unit_test_main` 自包 main，退出走 `bbt_net_test::RunTestProcessExit` 边界
   （`tests/net/ProcessRuntimeTestMain.hpp`）。

2. **自建 main + case-dispatch**（`tests/net/` 的 7 个文件与 `tests/` 平铺层的
   契约/iowait/rpc_payload 等非 Boost.Test 文件，以及 `tests/http_net/`、
   `tests/rpc_net/`、`tests/redis_net/` 的契约文件）：`main(argc, argv)`
   按 `argv[1]` 分发到具名 case 函数，CTest 每 case 注册一条
   `add_test(NAME <module>.<case> COMMAND <exe> <case>)`。

case-dispatch 的存在理由（非历史遗留，**为进程隔离与进程退出边界**）：

- **进程隔离**：每个 case 是独立 ctest 进程，崩溃/挂起不带走同套件其它
  case；coroutine runtime 进程寿命常驻，case 间共享状态不可控时按进程切开
  是最可靠的隔离粒度。
- **退出边界**：runtime 无 Stop/优雅停机，main 正常 return 会进入静态析构
  窗口并可能 SIGSEGV（调度线程在静态对象析构后仍 PollOnce）。故 main 必须
  在所有局部对象离开作用域后经 `std::_Exit` 结束，跳过静态析构——这由
  `RunTestProcessExit` 统一承担，与 Boost.Test 自包 main 同一约定。
- 已启动 runtime 的测试进程**不得**直接 `return`/`exit()`；新增同形态
  用例复用 `ProcessRuntimeTestMain.hpp`，不复制该边界。

迁移到 Boost.Test 的判定：若某文件只是「按 case 进程隔离 + `_Exit` 边界」，
改 Boost.Test + `BOOST_TEST_NO_MAIN` 自包 main 是等价形态（detail/ 三个
文件即先例）；若断言宏依赖「首失败即终止本 case」语义（自建 `CHECK` 抛
异常），迁移须逐条核对断言等价性，不为统一而统一。当前 `tests/net/` 的
7 个 RunCases 文件维持 case-dispatch（见
`workspace/plans/hygiene/out/in-p2b2/d8-decision.md`）。

## label 使用

- `smoke`：构建后秒级冒烟，与 `unit` 可双标。
- `unit`：模块内行为/边界，可起 runtime，不依赖外部服务。
- `integration`：编译/配置/消费面门禁（cmake -P、子构建、consumer 运行）。
- `live`：真实外部服务（Redis/Mongo 容器），环境未注入时必须以
  `SKIP_RETURN_CODE 77` 显式 Skipped，不得冒充绿。
- `perf`：基准 harness 冒烟，默认 OFF（`BBT_INFRA_PERF_BENCH`）。
