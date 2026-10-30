# HTTP loopback 消费示例：只消费公开 target `bbt::infra_http` + bbtools-coroutine。

本示例演示 bbtools-infra 当前公开面的完整生命周期：Scheduler::Start → NetworkRuntime::Create/Start → ListenHttp/CreateHttpClient → 协程内 HttpClient::Request（deadline 预算）→ 显式同步 Close() → 进程退出。基于 bbtools-coroutine 自有模型（C++17，无 co_await/Task<T>），不依赖外部 Redis/Mongo，可独立编译运行。

## 怎么跑

```bash
cmake -S <repo> -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树>
cmake --build build --target Example_http_loopback --parallel 2
./build/examples/http_loopback/Example_http_loopback
ctest --test-dir build -j1 -R '^example_http_loopback$' --output-on-failure
```

## 期望输出

```
status=200 body=echo:hello
```
退出码 0 表示 loopback 请求/回显/关闭边界均正常；无外部服务依赖，CI 可直接构建运行。
