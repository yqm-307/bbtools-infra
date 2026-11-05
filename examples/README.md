# 示例

示例使用 C++17 与 bbtools-coroutine 有栈协程，只消费公开头与 target；断言与生命周期验收在 tests 中。

| 目录 | 目标 | 内容 |
|---|---|---|
| `co_net_service/` | `Example_co_net_service` | Engine/ICoConnection TCP loopback |
| `ico_connection/` | `Example_ico_connection` | 成员 Read/Write 与关闭观察 |
| `http_net/` | `http_net_example` | Client/Server GET、404、POST、连接复用与排空 |
| `redis_net/` | `redis_net_example` | 单连接/池命令、唯一 key 清理；需要真实 Redis |
| `rpc_net/` | `rpc_net_example` | HTTP 上的 RPC 回声与业务错误 |
| `config_consume/` | `Example_config_consume` | 内存/文件配置，校验成功才接受 |
| `mongo_client/` | `mongo_client_example` | BSON 与 async CRUD，真实 Mongo 由 `BBT_TEST_MONGO_URI` 注入 |

`config_consumer/` 与 `net_foundation_consumer/` 是独立 CMake 消费工程，不由默认示例构建接入。各目录 README 给出模块用法。

```bash
cmake -S . -B <build> -G Ninja \
  -DBBT_COROUTINE_SOURCE_DIR=<干净 pin 检出> \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
cmake --build <build> --target http_net_example --parallel 2
ctest --test-dir <build> -j1 -R '^example_http_net_consumer$' --no-tests=error --output-on-failure
```

Redis/RPC 需要各自开关及锁定依赖前缀。Mongo 示例仅在 driver 模块存在时编译，未提供 URI 返回 77，不能算作真实后端通过。Redis 示例由新 Redis live harness 与同一独占容器运行。

关闭按各模块契约执行，不把逻辑封口当成物理资源已回收。HTTP 服务端按 StopAccepting/Drain/Close/WaitStopped 收尾；Redis 由宿主最终排空；Mongo 保留 bounded worker 的同步 Close 语义。运行时按进程寿命存在，不以停止运行时替代资源收口。

模块契约见 [HTTP](../docs/guides/http-net.md)、[Redis](../docs/guides/redis-net.md)、[RPC](../docs/guides/rpc-net.md)、[config](../docs/guides/config-watch.md)。
