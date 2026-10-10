# 独立消费验证（transport，Issue #31）

本目录是一个**独立 CMake 工程**，不被本仓构建引用（本仓 `examples/CMakeLists.txt`
只注册 `Example_http_loopback`、`Example_config_consume` 等示例 target）。它用于从仓外
视角证明：基础 TCP/UDP transport 的公共契约与模块 target `bbt::infra_transport` 可以被
独立程序消费，且**受管 TCP/UDP 消费者无需 HTTP**——本工程只链接
`bbt::infra_transport`，`LINK_LIBRARIES` 闭包中不含 `bbt_infra_http`（配置期即门禁）。

本仓当前无 install/export 目标，因此 consumer 以源码方式接入整个 infra 树
（`add_subdirectory`），但只链接 transport target，并关闭 infra 自身单测
（`BUILD_TESTING=OFF`）。`main.cc` 只 include `bbt/infra/` 公共头与 coroutine/core
基础头，不 include 任何 `src/` 内部装配头。

## 有界命令

```bash
# 依赖：可用的 bbtools-coroutine 源码树
cmake -S examples/transport_consumer -B <build>/transport-consumer \
  -DBBT_INFRA_SOURCE_DIR="$PWD" \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
cmake --build <build>/transport-consumer --target transport_consumer --parallel 2
<build>/transport-consumer/transport_consumer
```

退出码 0 且输出 `transport_consumer: ALL OK` 即全部检查通过；检查项覆盖
`TransportRuntime::Create/Start`、运行时未初始化时工厂拒绝 `RuntimeUnavailable`、
数值地址 `ListenTCP`/`BindUDP` 与 `LocalAddress()`、资源 owner 主动同步 `Close()`
（返回即物理收口）后 `IsClosed()` 为真，以及受管对象随 runtime 关闭而落定。
**已合入 main（close/stop 迁移 `02567ed`）**：`main.cc` 的关闭调用点已迁到 `Close()`（旧 `RequestClose`/
`WaitClosed`/`CloseStatus`/`Scheduler::Stop` 已删除），并已真实构建与运行通过
（`transport_consumer: ALL OK`）；本文不充当已发布契约的最终验收结论。

## 覆盖边界

本 consumer 只覆盖同步配置面 + 关闭落定面（控制线程 + 关闭调用），不覆盖
数据路径读写、容量/在途配额矩阵、DNS、取消竞态与 owner `Close()` 竞态；这些在
`tests/Test_tcp_loopback.cc`（`tcp.loopback`）、`tests/Test_udp_loopback.cc`
（`udp.loopback`）、`tests/Test_transport_runtime.cc`（`transport.runtime`）、
`tests/Test_tcp_runtime_factory.cc`（`tcp.runtime_factory`）、
`tests/Test_transport_acceptance.cc`（`transport.acceptance`，DNS 在途到期）中覆盖，契约见
`docs/decisions/0005-co-io-adapter-contract-v1.md`。旧硬停语义用例
（`tests/Test_hardstop_*.cc`）已随 `Scheduler::Stop` 删除，不再是覆盖项。
