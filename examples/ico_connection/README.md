# ICoConnection 成员化收口消费示例：只链接公开 target `bbt::infra_net` + bbtools-coroutine。

本示例演示真实 TCP 回环，两端都是本仓 Engine：server `EnableAcceptHandoff` → 协程 `CoAccept` → `conn.Read()` → `conn.Write(回显)` → `RequestClose` → `WaitClosed`；client `CoDial` → `conn.Write("hello")` → `conn.Read()` 回显 → `RequestClose` → `WaitClosed` → `Status` 采样。覆盖全部成员形态：Read/Buffer、Write/WriteReport、WaitClosed、Status 投影。不触碰 net detail 或 raw ring 接口。

只链接 `bbt::infra_net`，无外部服务依赖，loopback 自给自足。

## 怎么跑

```bash
cmake -S <repo> -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树>
cmake --build build --target Example_ico_connection --parallel 2
./build/examples/ico_connection/Example_ico_connection
ctest --test-dir build -j1 -R '^example_ico_connection_roundtrip$' --output-on-failure
```

## 期望输出

```
ico_connection dial=ok write_report=ok read_buffer=ok wait_closed=ok status=ok server_read=ok server_write=ok server_waitclosed=ok failures=0
```
退出码 0 表示全部成员形态断言通过。
