# config 首切片消费示例：只消费公开 target `bbt::infra_config`。

本示例演示配置首切片的最小消费用法：本地文件/内存源读取 → 上层 schema 校验 → 接受/拒绝；格式错误不发布伪成功快照。只 include `bbt/infra/config/` 公共头，只链接 `bbt::infra_config`，不依赖 Redis/Mongo，无外部服务。

## 怎么跑

```bash
cmake -S <repo> -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树>
cmake --build build --target Example_config_consume --parallel 2
./build/examples/config_consume/Example_config_consume
ctest --test-dir build -j1 -R '^example_config_consume$' --output-on-failure
```

## 期望输出

```
[valid] version=1 revision=... -> ACCEPT (port ok)
[invalid] version=2 revision=... -> REJECT (server.port 越界)
[malformed] read failed (code=...): ...
[memory] version=2 revision=... -> ACCEPT (port ok)
```
退出码 0 表示全部检查通过；远程 provider 适配、业务 schema 归属、资源热切换属后续切片，本示例不交付。
