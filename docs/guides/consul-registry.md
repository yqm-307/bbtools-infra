# Consul agent HTTP adapter（`bbt::infra_consul`）

> 状态：提供协议 smoke 与可选 Consul live 验证入口；CI 是否启用该 live 后端由消费者工作流
> 决定。接口真源是 `bbt/infra/consul/Client.hpp`。

## 定位

`bbt::infra::consul` 把「注册本进程实例 / 注销 / 按服务名取 passing 实例快照」
映射到 Consul agent 的 HTTP API，只负责 I/O、协议解析、错误映射与关闭收口。
它**不是**注册发现平台：

- 不做路由、负载均衡、选主、重试决策；
- 不做阻塞查询循环与陈旧视图策略（只回报 `X-Consul-Index`，由上层决定下一轮 index）；
- 不做业务健康判定（`TCP` check 只证明端口可达，不等于 RPC readiness）；
- 不引入 Consul SDK、不自建 Engine/线程，只经 `bbt::infra::http::Client` 出网；
- 只支持 Consul `default` namespace；`datacenter` 为空表示 agent 默认。

## 构建与消费

```bash
cmake -S <infra 工作树> -B <私有 build 目录> -G Ninja \
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
    -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 精确 pin 检出> \
    -DBBT_INFRA_NETWORK_ONLY=ON -DBBT_INFRA_HTTP_NET=ON -DBBT_INFRA_CONSUL=ON \
    -DBUILD_TESTING=ON
cmake --build <私有 build 目录> --parallel 2 --target \
    bbt_infra_consul bbt_infra_consul_header_selfcheck Test_infra_consul
```

- target：`bbt::infra_consul`（alias）；公开头 `<bbt/infra/consul/Client.hpp>`。
- 依赖闭包：`bbt::infra_http_net` + `bbt::infra_common` + `Boost::headers`。Boost.JSON
  以 `boost/json/src.hpp` 源码内联进 `Client.cc`，不链接 `libboost_json`，不新增第三方依赖。
- 开关 fail-closed：`BBT_INFRA_CONSUL=ON` 必须同时 `BBT_INFRA_HTTP_NET=ON`，否则
  configure 失败；`BBT_INFRA_CONSUL` 与 `BBT_INFRA_MCP` 同开也会失败（两者各有一个
  `boost/json/src.hpp` TU，同一最终链接会重复定义）。
- 普通模式与 `BBT_INFRA_NETWORK_ONLY=ON` 共用同一入口（`cmake/Consul.cmake`）。

## 最小消费

```cpp
#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/consul/Client.hpp>

// HTTP Client 由应用装配（Engine 可共享），专属本对象：Consul Client 负责关它。
bbt::infra::http::ClientOptions http_options;   // base_url 可空：本模块用绝对 URL
auto http = bbt::infra::http::Client::Create(engine, http_options);

bbt::infra::consul::Options options;
options.agent_url = "http://127.0.0.1:8500";    // 仅 http + 数值 loopback
auto created = bbt::infra::consul::Client::Create(std::move(http).value(), options);
auto client = std::move(created).value();

// 以下调用只在协程上下文 await。
auto registered = bbt_co_await client->Register(registration);       // PUT /v1/agent/service/register
auto snapshot   = bbt_co_await client->HealthyInstances("catalog", 0); // GET /v1/health/service/...
auto removed    = bbt_co_await client->Deregister(registration.id);  // PUT .../deregister/<id>
```

注册载荷固定为 `{ID,Name,Address,Port,Tags,Check:{TCP:"<addr>:<port>",Interval:"1s",
Timeout:"500ms",DeregisterCriticalServiceAfter:"1m"}}`：实例 ID 必须包含每进程
incarnation（不能用可复用地址充当），崩溃进程的登记由 `DeregisterCriticalServiceAfter`
兜底清理。

## 语义与错误映射

| 情形 | 返回 |
|---|---|
| 注册/查询/注销收到 2xx | `ok` |
| 注销收到 404 且 body 为 `Unknown service ID`（agent 已确认不存在） | `ok`：目标已不存在即已注销（幂等） |
| 其它非 2xx | `TransportError`，`backend_category="consul.http"`，`backend_code=状态码` |
| 传输失败/连接拒绝 | `TransportError`（http 层原码透传） |
| 总期限到期 | `TimedOut`（不解释成「未执行」） |
| 缺失/非数字 `X-Consul-Index`、非法 JSON、非数组响应、空 ID、非法地址、0/越界端口 | `ProtocolError` |
| 参数非法（agent 地址、注册字段、空服务名/ID、非正期限） | `InvalidArgument` |
| `Close()` 之后的任何操作 | `Closed` |
| 协程外调用 | `InvalidContext`（http 层拒绝，不抛异常） |

查询附加 `passing=true`，并额外核对每条 `Checks[].Status`：agent 忽略该参数时也不会
把 critical 实例当成可用目标。地址取值优先 `Service.Address`，为空才回落
`Node.Address`，且必须是数值 IPv4（主机名不是本层支持的形态）。任一条目出现非法地址、
空 ID、非法端口或缺失检查状态时，整个快照 fail-closed 返回 `ProtocolError`；上层应把它
与 agent 传输故障区分处理。

`query_wait` 只影响阻塞查询：`index != 0` 且 `query_wait > 0` 时发送 `index`+`wait`，
读档预算为 `query_wait + query_wait/16 + 250ms`（Consul 允许附加 wait/16 抖动），
因此 `Create()` 会拒绝没有留下这段抖动和收尾余量的 `query_wait/request_timeout` 组合。
`query_wait=0` 表示只取一次权威快照。

`index` 是 agent 视图修订号：只在同一 agent 内可比；有效 header 为 0 表示 agent 未提供有效修订，
下一轮应以 index=1 重问，不能判等，也不能用重复 index 推断「数据没变」。缺失、非数字或越界
header 直接返回 `ProtocolError`。

## 关闭语义

`Close()` 同步、幂等、可在任意线程（含协程上下文）调用：先置封口，拒绝新操作；已接纳
HTTP 交换完成或失败后，再关闭专属 HTTP Client，最后等待已登记操作全部离开后返回。
排队者得 `Closed`，在飞交换不由 adapter 主动取消。首个与并发 `Close` 等同一终态：**返回即
已封口、专属 HTTP Client 已关闭、在飞操作已离开**，不设 `min(request_timeout, 1s)` 之类的提前返回上限
（`0005` §6.4：不以 drain timeout 放弃等待）。每个已登记操作受 `request_timeout`
绝对期限约束，因此终态可达；协程上下文内以 `CoWaiter` 协作挂起等待，不占用
Processer 线程（阻塞 Processer 会让在飞操作无法恢复，并发 `Close` 可占满 worker）。
当前协程已被 `RequestCancel` 时，`CoWaiter` 入口即返回 `Cancelled`、`Hook_Sleep`
也在取消态立即返回；该分支改用 `bbtco_yield` 让出当前调度片（不使用线程 sleep），
但循环会持续占用可运行 worker，不能当作低开销阻塞等待；因此 `Close` 必须在该 Client 的
调度器/Engine 仍能推进在途操作时调用（框架在 handlers drain 后、网络关闭前调用）；
已登记操作各自交付自己的终态，`Close` 不替它们取消，也不把「已返回」当成「操作被取消」。

## 真实后端与 pin

测试使用的 Consul 固定为官方 **1.20.2**（`consul version` → `Consul v1.20.2`,
`Revision 33e5727a`），二进制只落 `build/`，不做系统安装：

```bash
cd <infra 工作树>/build/consul
curl -sS -O https://releases.hashicorp.com/consul/1.20.2/consul_1.20.2_SHA256SUMS
curl -sS -O https://releases.hashicorp.com/consul/1.20.2/consul_1.20.2_linux_amd64.zip
grep " consul_1.20.2_linux_amd64.zip$" consul_1.20.2_SHA256SUMS \
  | awk '{print $1"  consul_1.20.2_linux_amd64.zip"}' | sha256sum -c -
unzip -qq -o consul_1.20.2_linux_amd64.zip -d bin
```

测试只依赖上面的官方 `SHA256SUMS` 校验包；构建不会自动下载或安装 Consul。运行时
通过 `BBT_TEST_CONSUL_BIN` 显式传入已校验的可执行文件，测试不会仅凭路径名宣称版本。
未设置 `BBT_TEST_CONSUL_BIN` 时，live CTest 标为 Skipped，不冒充通过；agent 以 `-dev` 模式在本进程租用的动态端口上
启动，仅 bind/advertise `127.0.0.1`，DNS/gRPC 关闭；测试结束或异常都经析构
`SIGTERM → 有界等待 → SIGKILL`，只作用于自己的子进程。

## 验证

```bash
export BBT_TEST_CONSUL_BIN=<infra 工作树>/build/consul/bin/consul
flock /tmp/bbt-build.lock ctest --test-dir <私有 build 目录> -j1 -R '^infra[.]consul[.](protocol|live)$' \
  --no-tests=error --output-on-failure
```

`Test_infra_consul --protocol` 覆盖确定性协议与关闭契约（不依赖 Consul 二进制）；
`Test_infra_consul --live` 覆盖真实后端注册、passing 视图、阻塞查询、注销、空视图和
agent 停止后的有限失败：

1. 参数与注册字段校验（不触网）；
2. 不可达 agent：`request_timeout=1s` 下返回 `TransportError`，不会无限重试；
3. 协议面（自有 loopback 固定应答对端，只产生协议字节）：注册 wire 形态（PUT 目标 +
   JSON 字段 + TCP check）、500 → `TransportError`+`backend_code=500`、缺 index/坏 JSON/
   端口 0/空 ID/非数组 → `ProtocolError`、critical 过滤、注销路径百分号编码、404 幂等、
   封口后 `Closed`；
4. 关闭契约（真实在飞交换：对端完整读出请求后 hold，触发 `Close` 之后才回包）：在途
   请求关闭、三个并发 `Close` 等同一终态、协程上下文两个并发 `Close` 不占 Processer
   （ticker 持续推进）、两个**预取消**协程 `Close` 仍不提前返回且 ticker/在飞操作继续
   推进；四例都断言 `Close` 返回不早于回包与操作离开，前三例还断言不早于 1.2s；
5. 真后端：注册三实例（两端口可达、一端口不可达）→ passing 视图为 2 →
   同 index 阻塞查询约 1s 返回同一视图 → 扩容到 3（index 前进）→ 注销一个降为 2 →
   全部注销得空视图（合法空列表）→ 幂等注销与未知服务查询 → `Close` 后 `Closed` →
   停 agent 后查询在 1s 内 `TransportError`。

## 未覆盖（不在本模块承诺）

- ACL token 只透传 `X-Consul-Token`，不验证权限模型、不做 token 轮换；
- https/TLS、远端 agent、主机名解析、多 datacenter 选路、Enterprise namespace；
- 阻塞查询循环、陈旧视图上限、健康视图缓存——归上层组件；
- 真实故障注入（分区、agent 重启、Consul 集群一致性）与压测；
- 普通模式和两个 fail-closed configure 分支需要在启用对应构建矩阵后分别验证，不能由
  `BBT_INFRA_NETWORK_ONLY=ON` 的一次构建代替。

协议/关闭 smoke 与真实 Consul live 已拆为两个 CTest；构建和运行命令见上面的“验证”章节。
`infra.consul.protocol` 不需要 Consul 二进制，始终执行确定性协议、错误映射和关闭契约；
`infra.consul.live` 只有在 `BBT_TEST_CONSUL_BIN` 已设置且指向可执行文件时才执行真后端。
变量未设置时 live 项以 Skipped 结束；变量已设置但路径不存在或不可执行时测试失败，
避免配置错误被隐藏为 skip。
