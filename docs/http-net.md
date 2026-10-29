# HTTP net 客户端与服务端（`bbt::infra_http_net`）

> 状态：实验性未提交候选，本地聚焦验证，待独立审查，未接 CI；不代表整个 HTTP 迁移已交付。本文描述当前实现，不是冻结契约；接口以 `bbt/infra/http/Client.hpp`、`Server.hpp` 为准。

## 定位

`bbt::infra::http` 是 HTTPX（0.28.1）AsyncClient 语义到本仓原生有栈协程 + `bbt::infra::net` 的映射：单一协程 `Client`、内建连接池，普通用户只面对 URL/params/headers/body/timeout 与 `result<Response>`，不接触 Engine Run/Ready/WaitStopped。Beast 只承担 HTTP 编解码；网络只经 `bbt::infra::net` 协程成员接口（`Engine::CoDial` + `ICoConnection::Read`/`Write`），不触碰 Asio socket/native fd/net detail。旧 `bbt::infra_http`（Asio 形态）不受影响，消费者不切换。

## 构建与消费

```bash
# network-only 闭包：新 HTTP + net + 精确 pin 的 bbtools-coroutine
cmake -S <infra 工作树> -B <私有 build 目录> \
    -DBBT_INFRA_HTTP_NET=ON \
    -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 精确 pin 检出> \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DBUILD_TESTING=ON
cmake --build <私有 build 目录> --target bbt_infra_http_net_header_selfcheck http_net_example Test_http_net_contract Test_http_net_regressions Test_http_net_server --parallel 2
ctest --test-dir <私有 build 目录> -j1 -R 'http\.net\.' --output-on-failure --no-tests=error
```

- 目标：`bbt::infra_http_net`（alias）；示例 `http_net_example`（CTest 名 `http.net.consumer`）；契约测试 `Test_http_net_contract` 与 `Test_http_net_regressions`（用例名分发，`http.net.*`，LABELS `http_net`）。
- 依赖：仅 `bbt::infra_net`（含 Boost::headers，Beast header-only），无新第三方依赖；coroutine 消费受 `cmake/coroutine.sha` pin 门禁约束。
- CI：未接入（`BBT_INFRA_HTTP_NET` 不在任何 workflow 中），CI 全绿不代表本模块已验证。

## 最小消费

```cpp
#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/http/Client.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>

namespace http = bbt::infra::http;

// 1) 执行底座由应用一次性装配（Client 不建线程、不建 Engine）：
//    coroutine scheduler + client-only Engine + 宿主线程 Run。
g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
g_scheduler->Start();
bbt::infra::net::Engine engine{std::shared_ptr<bbt::infra::net::RawHandler>{}};
std::thread runner([&] {
    bbt::infra::net::EngineConfig config;
    config.runtime_progress = bbt::infra::net::RuntimeProgress::Independent;  // 必填
    config.options.num_event_loop = 1;
    (void)engine.Run(config);   // 阻塞宿主控制线程，返回=本 Engine 网络收口
});

// 2) Client 只装配：Create 不联网。
auto created = http::Client::Create(
    std::shared_ptr<bbt::infra::net::Engine>(&engine, [](auto*) {}), [&] {
        http::ClientOptions options;
        options.base_url = "http://127.0.0.1:8080";   // 仅 http + 数值 IP（DNS 未交付）
        options.headers = {{"Accept", "application/json"}};   // client 级默认头
        return options;
    }());
auto client = std::move(created).value();

// 3) 业务协程内：Get/Post/Request 以 bbt_co_await 消费。
http::RequestOptions options;
options.params = {{"page", "1"}};
auto response = bbt_co_await client->Get("/items", options,
                                         bbt::infra::CallOptions{ /* 总绝对 deadline */ });
if (!response) {
    // 传输/排队/装配失败；HTTP 4xx/5xx 是有效 Response，不走这里
} else {
    Use(response.value().status_code, response.value().body);
}

// 4) 收口：client 逻辑封口（幂等）→ RequestClose → Run 返回。
client->Close();
engine.RequestClose();
runner.join();
```

完整可运行双端示例见 `examples/http_net/main.cc`：新 Client 向新 Server 发两次 GET（200/404）与一次 POST 回显；真实 accepted=1/requests=3 证明复用，不含自带 POSIX HTTP 服务端。

## Server 消费与责任

```cpp
// 应用先启动至少 4 个固定 scheduler worker（server 2 driver 槽 + client 1 槽）。
auto engine = std::make_shared<net::Engine>(std::shared_ptr<net::RawHandler>{});
auto made = http::Server::Create(engine,
    [](IncomingCallContext ctx, http::IncomingRequest request) -> result<http::Response> {
        // 在原生有栈协程中，可 bbt_co_await 其它库；ctx.deadline 为有限总绝对期限。
        return result<http::Response>::ok({200, {}, std::move(request.body)});
    });
auto server = std::move(made).value();
// Create 只配置 handoff；应用用自有 native runner 调用 Engine::Run，
// 配置 Independent、listen_addresses={127.0.0.1:0}，确认 Running 后：
auto started = server->Start(); // 必须检查；一次性，登记失败显式回滚并封口
// 原生控制线程中收口（每个 result 均须检查）：
server->StopAccepting();
auto drained = server->Drain(deadline);
server->Close();
auto stopped = server->WaitStopped(deadline);
// 最后由应用另行 engine->RequestClose()、Run 返回与 runner.join()。
```

- Engine 的接纳端专用于本 Server；不得并置第二个 `Engine::CoAccept` 消费者或 raw reader。Create 在 Idle 调用；不隐式 Run、监听或创建线程。应用 handler 是 `std::function<result<Response>(IncomingCallContext, IncomingRequest)>`，request 全部 owning；未认证 `peer_principal` 恒空，不用 IP 冒充身份。
- 一 accept 协程、一连接一顺序 session；不同连接并发。请求 parser 与回复 serializer 均使用 Beast，网络只用公开 `Engine::CoAccept` 与 `ICoConnection::Read`/`Write`。保留 pipeline 多读字节；支持分片、chunked、HEAD、204/304、对端 FIN 后完整请求回复。HTTP/1.1 origin-form；不提供路由框架。
- StopAccepting 与 handler 授权在同一短锁内仲裁；不持锁调用 handler 或挂起。门前授权继续 handler+回复，门后已有连接也不能授权新请求；关闭 idle/半包连接并请求 net 独立停止监听，不 RequestClose 整个 Engine。
- Drain 必须先停止接纳，从授权计到 handler 真实返回及回复写 Done 且无 error，或封写后明确放弃。不是 handler 返回即排空，也不代表 peer 应用已读到回复。Close 幂等强制封本 Server 连接、不强杀 handler。Drain/WaitStopped 仅 native 宿主调用；协程/已登记 Processer 域返回 InvalidContext，不惰性创建域。
- WaitStopped 同时检查 callback 最后捕获销毁、全部连接 `Released()` 与 listener `AcceptingStopped()`；共享 Engine 可以仍 Running。Stats 的 sessions 包含逻辑已关但未物理回收的 retiring，额度只在 task_done + Released 后复用；不是计数 0 假装 fd 已释放。超时保留共享状态和任务责任，宿主须继续驱动；永久不让出的 handler 无强杀保证。
- ServerOptions 复用 NetworkLimits；连接/inflight/header/body/target/handoff 有界，incoming_timeout/write_timeout 都有限且至多 24h。总期限从每个请求开始解析前起算，覆盖读取、handler 与回复授权；handler 晚返不再起写，写期限取总期限与 write_timeout 较早者。超限或坏流本阶段明确断开，不承诺生成 4xx。
- HTTP/1.1 要求唯一合法 Host；当前 Host 接受 ASCII DNS 标签/数值 host（IPv6 必须方括号，可选十进制 port），拒绝 userinfo、空白、路径和畸形端口。target 校验 origin-form URI 字符及百分号转义。CONNECT、Upgrade、Expect 明确拒绝并关连接，Expect 在读完 header、等待 body 之前拒绝。
- handler 异常或错误 result 隔离为 500；非法状态（仅接受 200–599）、CR/LF/控制字节头及用户 Content-Length/Transfer-Encoding/Connection/Upgrade/Trailer 明确断开。Beast 负责 framing；HEAD 保留表示长度但不发 body，204/304 不发 body。大响应以有界块 `conn.Write`，只有 Rejected + 无 operation 的零提交容量错误才减半重试，不重放已接纳写；每块复查期限与 Close 并让出。
- 本地 `Test_http_net_server` 覆盖 wire（分片/chunked/pipeline/HEAD/204/304/FIN）、停止授权及 idle/半包、回复写屏障 Drain、Close 超时责任、入站期限晚返、安全、Start/登记失败、inflight 门禁及 owner 扣留物理回收额度。每例在进程退出前验证 HTTP 任务/连接、listener、Run/join 及 socket 基线归还。私有注入仅用于确定性阶段，不替代真实 TCP 字节。
- CI 尚未接线；TLS、HTTP/2、流式、代理、DNS 解析、路由框架、永久阻塞 handler 的抢占以及跨平台/sanitizer/压力验证不在本批。

## 契约要点

- **超时**：`Timeout` 四档 connect/read/write/pool，单档三态 `PhaseTimeout::Inherit/Milliseconds/Disabled`；Client 默认四档均 5s（显式逐字段）。请求级 `RequestOptions::timeout` 分档覆盖，Inherit 档取 Client 档。总绝对 deadline 单一真源是 `CallOptions.deadline`；各阶段期限 = min(总 deadline, 实际阶段开始时刻 + 阶段档)。pool 等待不消耗后续 connect/write/read 档；read 档从每次 `conn.Read` 开始起算，总 deadline 不因读到字节重置。拨号/写入/读取及缓存快路径前预检总 deadline 和协程取消。
- **连接池**：origin（scheme/host/port）匹配；HTTP/1.1 一条连接一次一个活动请求。`PoolLimits{max_connections, max_keepalive_connections, keepalive_expiry, max_queue}` 为全局配额。完整响应且 keepalive 才回池；写超时未终态/读中断/取消/协议错一律退役；合法 EOF 定界响应正常返回但不复用，不自动重放请求。空闲 stale 检查只用公开观察（`Closed/ReadEof/ReadError/ReadableBytes>0`），keepalive 过期惰性判定（Acquire/Release/Stats 的 sweep 内执行，无后台 reaper）。
- **物理记账**：Dialing/Active/Idle/Retiring 同列表，`entries.size()` 是本池资源预算。成功连接只在底层 `Connection::Released()`（与 `conn.IsReleased()` 同一真源）后出账；失败/取消/未交付拨号保留 `ConnectionResult::reclaim`（`ReclaimReceipt`），只在其 `Released()` 后出账。逻辑 `Closed()` 不能代替物理释放，排队者在资源回收前不得重开连接。`PoolStats`（active/idle/dialing/retiring/waiting/budget）是公开观测面。
- **排队**：有界 `max_queue`，满即 `Overloaded`；CoWaiter 登记→锁内复查→Notify，锁永不跨挂起；Notify 不等于获得，恢复后锁内领取，孤儿 handoff 回池。分配行为：快路径先扫同 origin 空闲；归还把名额交队首同 origin 等待者；异 origin 被空闲连接占满预算时，退役一条异 origin 空闲连接，真实回收后再拨号。不承诺严格 FIFO 公平。排队者唤醒源：同 origin 完整归还、封口、池超时，以及 20ms 有界重试切片（预算释放没有事件源，切片保证活性）。
- **错误面**：沿用 `bbt::infra::ErrorCode`——未提交前的排队/连接/期限超时 `TimedOut`、排队满 `Overloaded`、建连失败 `TransportError`、坏流 `ProtocolError`、封口 `Closed`、协程取消 `Cancelled`（保留请求阶段事实）；请求字节完整写出后的读超时/连接中断按 `OutcomeUnknown`（`Error.request_phase=RequestCommitted`，请求可能已生效）。已接纳但写入尚未终态时也不能认定零发送，不自动重试。
- **关闭**：`Close()` 立即逻辑封口、拒绝新请求、唤醒全部排队者（`Closed`）、退役空闲连接；在飞请求按已持有借用完成或失败；不关共享 Engine，不假装物理排空（退役 fd 由 Engine owner 收口）。析构=Close。

## 输入边界与协议覆盖

- `BuildRequest` 和 `Send` 均校验输入；公开可变的 `PreparedRequest` 不是可信凭据。URL、target、数值地址与默认 Host 派生信息必须一致。
- 方法和头名须为 HTTP token；头值拒绝 CR/LF/NUL 及除 HTAB 外的控制字符。每个头名最多 8 KiB、头值 32 KiB、全部头合计 64 KiB。
- `Content-Length`/`Transfer-Encoding` 由客户端根据 raw body 生成；拒绝手工 framing，避免歧义。需恰好一个非空 Host；不支持 CONNECT 隧道。
- 字段序列化在借用连接前完成；序列化错误映射 `InvalidArgument`。借用有异常收口，错误不得遗留 active 名额。
- `Test_http_net_regressions` 覆盖分片头/体、64 个 chunk、HEAD/中间响应、EOF/截断、body limit、输入边界、冷/热连接预取消与过期、阶段期限、跨 origin、并发交接及延迟物理回收预算。
- 这些是回归注册范围，不是凭测试名称证明通过；实际执行使用上文 CTest 命令。

## 里程碑能力边界（未实现不得暗示）

实现范围（本地验收命令见上文，不代表全部 HTTPX 功能）：http scheme、数值 IPv4 host、GET/POST/扩展 token 方法 `Request`、raw body（含二进制）、base_url/默认 headers/params 合并（params client 在前、请求级追加、百分号编码）、分阶段超时、连接池复用/上限/排队/过载/退役/过期、关闭与取消竞争、坏流退役。

未实现（接口不暗示）：https/TLS、主机名 DNS（host 名显式报 `InvalidArgument`，DNS 方案需单独决策）、json/form 编码、流式响应（`StreamResponse`）、重定向、代理、HTTP/2、请求自动重试、事件 hook。DNS/新依赖/native worker 属未授权决策点，不在本模块内悄悄实施。

## 已知局限

- 回收由 net owner 异步推进；retiring 期间仍占名额，不能因等待结束就假定资源已释放。多个 Client 的池独立，各自预算不构成共享 Engine 的全局 fd 上限。
- 排队者不因「预算释放」被即时唤醒，20ms 重查切片会增加分配延迟；这是重查节拍，不是在任意调度压力下的严格延迟上界。
- 只实现完整 HTTP/1.1 消息：支持分片、chunked、HEAD、有限中间 1xx 与 EOF 定界；101 升级显式拒绝，最多接受 16 个中间响应。多余尾随字节不复用。CONNECT 隧道不支持。
- CI 未接线；跨平台未验证（net 层当前仅 Linux）。
