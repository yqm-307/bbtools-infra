# 独立消费验证（config 首切片，Issue #35）

本目录是一个**独立 CMake 工程**，不被本仓构建引用（本仓 `examples/CMakeLists.txt`
只注册 `Example_http_loopback`、`Example_config_consume` 等示例 target）。它用于从仓外
视角证明：`bbt::infra::config` 的最小公共契约与模块 target 可被独立程序消费，公共头
不泄漏 framework 或第三方类型。

本仓当前无 install/export 目标，因此 consumer 以源码方式接入整个 infra 树
（`add_subdirectory`），但只链接 `bbt::infra_config`，并关闭 infra 自身单测
（`BUILD_TESTING=OFF`）。

## 有界命令（实测见 `docs/config-watch-v1.md`）

```bash
# 依赖：固定 SHA 7bcda3b078f975ff2978424be7f6ba38e04fb7f6（main HEAD，进程寿命运行时）的 bbtools-coroutine 源码树
cmake -S examples/config_consumer -B <build>/config-consumer \
  -DBBT_INFRA_SOURCE_DIR="$PWD" \
  -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 源码树> \
  -DCMAKE_CXX_COMPILER_LAUNCHER=ccache
cmake --build <build>/config-consumer --target config_consumer --parallel 2
mkdir -p <build>/config-consumer-run
TMPDIR=<build>/config-consumer-run <build>/config-consumer/config_consumer
```

退出码 0 且输出 `consumer: ALL OK` 即全部检查通过；检查项覆盖内存源版本/指纹/类型化
读取、真实本地文件源读取与 source 标识、格式错误不发布伪成功、目录路径不发布伪成功
空快照。

## 覆盖边界

本 consumer 只覆盖同步读取面（不启动 Scheduler）。watch 的关闭/去重/drain、强制 Stop
资源边界与在途关闭交错在 `tests/Test_config_watch.cc`（`config.watch`）中覆盖，
契约与未覆盖矩阵见 `docs/config-watch-v1.md`。
