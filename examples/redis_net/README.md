# Redis 窄入口消费示例：只链接 `bbt::infra_redis_net`。

本示例演示基于新 net 的 Redis 窄适配：单连接与连接池两种形态，经真实 loopback TCP 完成 Connect/PING/SET/GET/Delete/Exists 及唯一 key 清理。只消费公开头与公开 target，不触碰内部装配。

依赖 `BBT_INFRA_REDIS_NET=ON`（需 hiredis 前缀），目标缺失时跳过。默认连 `127.0.0.1:6379`，可经 `BBT_REDIS_PORT` 覆盖；无真实 Redis 时连接/命令按 deadline 返回错误并打印，示例不冒充已运行。

## 怎么跑

```bash
cmake -S <repo> -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DBBT_HIREDIS_PREFIX=<hiredis 私有前缀> \
  -DBBT_INFRA_REDIS_NET=ON
cmake --build build --target redis_net_example --parallel 2
# 有真实 Redis 时运行：
BBT_REDIS_PORT=6379 ./build/examples/redis_net/redis_net_example
ctest --test-dir build -j1 -R '^example_redis_net_consumer$' --output-on-failure
```

## 期望输出

```
[single] PING=PONG SET=OK GET=hello-net
[single] cleanup unique_key=1 exists_after=0
[pool] PING=PONG SET=OK GET=hello-pool pool_size=3
[pool] cleanup unique_key=1 exists_after=0
host run_returned=1 joined=1 phase=Stopped
```
退出码 0 表示单连接与池两种形态均通过并完成唯一 key 清理。
