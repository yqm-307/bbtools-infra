# 新 net 首包独立消费

本工程只链接 `bbt::infra_net`，只包含 `<bbt/infra/net/NativeSocket.hpp>`，验证 numeric IPv4 地址、真实非阻塞 UDP 发送/接收、内容比对及显式关闭。不是完整 Engine/Conn 示例，不声称旧 transport 或上层已适配。

```bash
cmake -S examples/net_foundation_consumer -B <独立 consumer build> -G Ninja \
  -DCMAKE_BUILD_TYPE=Debug -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
  -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
  -DBBT_INFRA_SOURCE_DIR=<infra 源码树> \
  -DBBT_COROUTINE_SOURCE_DIR=<固定 coroutine 源码树>
cmake --build <独立 consumer build> --target consume_net --parallel 3
<独立 consumer build>/consume_net
```

`BBT_INFRA_NETWORK_ONLY` 在工程中显式打开，`BUILD_TESTING=OFF` 只用于此独立消费者；库自身的正常首包测试不能用该配置代替。无安装/导出包，无旧 API 兼容壳。详见 [决策 0010](../../docs/decisions/0010-gnet-network-foundation.md) 的支持缺口及后续依赖。
