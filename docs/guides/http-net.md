# HTTP net 客户端与服务端（`bbt::infra_http_net`）

> 状态：已合入 main（PR #102，提交 `4b07517`）；远端 CI 结论以 PR 与 main 的 workflow run 为准。本文描述当前实现，不是冻结契约；接口以 `bbt/infra/http/Client.hpp`、`Server.hpp` 为准。
>
> **迁移中**：`net::EngineLifecycle` 与其 pthread runner/timer 已从实现删除；本文片段与示例尚未迁移，不代表当前可照抄的接缝。目标形态是应用根持有共享 Engine、入口 native host hook 在主线程调用一次 `Engine::Run`，主任务用 `Engine::CoWaitRunning` 等 Running、`Run` 返回即本代收口；Engine 的共享、关闭与两向退出见 [Engine 主线程宿主与共享所有权契约](../architecture/native-lifecycle-seam-contract.md) 与 coroutine `core-runtime-contract.md` §12.2。
>
> 关闭控制面（`http::Server/Client`、`rpc::CoRpcServer/CoRpcClient`、`redis::CoRedisPool` 的 `Close(CloseOptions)` + `Close(Immediate)` + `Close(wait_release=true)`）与 **HTTP 请求级派发**已在 infra 落地；请求级责任、响应顺序与 inflight 结算的冻结语义见 [关闭控制面契约](../architecture/close-options-contract.md) §4，本文只写实现口径与验证入口。

## 定位

`bbt::infra::http` 是 HTTPX（0.28.1）AsyncClient 语义到本仓原生有栈协程 + `bbt::infra::net` 的映射：单一协程 `Client`、内建连接池，普通用户只面对 URL/params/headers/body/timeout 与 `result<Response>`，不接触 Engine Run/Ready/Close(wait_release=true)。Beast 只承担 HTTP 编解码；网络只经 `bbt::infra::net` 协程成员接口（`Engine::CoDial` + `ICoConnection::Read`/`Write`），不触碰 Asio socket/native fd/net detail。旧网络栈已退役，本模块是唯一现役 HTTP 实现；旧消费者须迁移到 `Client/Server`，关闭按本模块契约执行。

## 构建与消费

```bash
# network-only 闭包：新 HTTP + net + 精确 pin 的 bbtools-coroutine
cmake -S <infra 工作树> -B <私有 build 目录> \
    -DBBT_INFRA_HTTP_NET=ON \
    -DBBT_COROUTINE_SOURCE_DIR=<bbtools-coroutine 精确 pin 检出> \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache -DBUILD_TESTING=ON
cmake --build <私有 build 目录> --target bbt_infra_http_net_header_selfcheck http_net_example Test_http_net_contract Test_http_net_regressions Test_http_net_server Test_http_net_streaming --parallel 2
ctest --test-dir <私有 build 目录> -j1 -R 'http\.net\.' --output-on-failure --no-tests=error
```

- 目标：`bbt::infra_http_net`（alias）；示例 `http_net_example`（CTest 名 `example_http_net_consumer`）；契约测试 `Test_http_net_contract`/`Test_http_net_regressions`/`Test_http_net_server`/`Test_http_net_stop_regressions`/`Test_http_net_streaming`（用例名分发，`http.net.*`，LABELS `unit`）。
- 依赖：仅 `bbt::infra_net`（含 Boost::headers，Beast header-only），无新第三方依赖；coroutine 消费受 `cmake/coroutine.sha` pin 门禁约束。
- CI：`BBT_INFRA_HTTP_NET` 已由 Release/Debug 门禁的新 RPC/HTTP 闭包覆盖（`scripts/ci/release_gate.sh`/`debug_gate.sh` → `run_ctest_label.sh --regex '^(rpc|http)\.net\.'`）；HTTP-only 闭包另验证不引入 protobuf。Server 端响应流（`http.net.stream_*`）已加入 release 门禁（`Test_http_net_streaming` 构建目标与 `required_rpc_http_net.txt` 清单）。

## 最小消费

```cpp
#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/syntax/Async.hpp>
#include <bbt/infra/http/Client.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>

namespace http = bbt::infra::http;

// 1) 执行底座由 runtime 的 bbtco_main 统一启动；Engine 由应用根在 Idle 阶段完成
//    装配后发布给入口 host gate（framework 侧，片段省略）。native host hook 在主线程
//    调用一次 Engine::Run(config)，主任务用 CoWaitRunning 等真实 Running；不存在
//    runner 线程/join/试收割。两向退出见 native-lifecycle-seam-contract.md。
void setup_runtime()
{
    g_bbt_coroutine_config->m_cfg_static_thread_num = 2;
}
int bbtco_main()
{
    bbt::infra::net::EngineConfig config;
    config.runtime_progress = bbt::infra::net::RuntimeProgress::Independent; // 必填
    config.options.num_event_loop = 1;
    auto engine = bbt::infra::net::Engine::Create(std::shared_ptr<bbt::infra::net::RawHandler>{});
    // ... 装配 Client/Server（均要求 Idle），然后由 host hook 发布并 Run ...
    auto ready = engine->CoWaitRunning({std::chrono::steady_clock::now() + std::chrono::seconds(3)});
    if (!ready) { return 1; } // 启动失败按 CoWaitRunning 的错误收口
    // ... 消费 ...
    return 0;
}
BBTCO_MAIN_WITH_SETUP()

// 2) Client 只装配：Create 不联网。
auto created = http::Client::Create(
    std::shared_ptr<bbt::infra::net::Engine>(&engine, [](auto*) {}), [&] {
        http::ClientOptions options;
        options.base_url = "http://127.0.0.1:8080";   // 仅 http + 数值 IP（DNS 未交付）
        options.headers = {{"Accept", "application/json"}};   // client 级默认头
        options.SetTimeout(std::chrono::seconds(5)).SetConnectTimeout(std::chrono::seconds(2));
        return options;
    }());
auto client = std::move(created).value();

// 3) 业务协程内：Get/Post/Request 以 bbt_co_await 消费。
http::RequestOptions options;
options.params = {{"page", "1"}};
options.SetReadTimeout(std::chrono::seconds(30)); // 只覆盖读取，其他阶段继承 Client
auto response = bbt_co_await client->Get("/items", options,
                                         bbt::infra::CallOptions{ /* 总绝对 deadline */ });
if (!response) {
    // 传输/排队/装配失败；HTTP 4xx/5xx 是有效 Response，不走这里
} else {
    Use(response.value().status_code, response.value().body);
}

// 4) 收口：关闭是统一控制面（幂等、不可重开）。
//    Graceful：封口拒新并在绝对期限前排空已借出/在途；到期只返回 TimedOut 并保留责任。
auto drained = client->Close(bbt::infra::CloseOptions::Graceful(
    std::chrono::steady_clock::now() + std::chrono::seconds(5)));
//    Immediate：只封口、发布首因、令在飞/排队请求失败；不等待业务完成。
client->Close();
//    Engine 的收口：主任务结束后由 host hook 的 OnMainTaskDone 调用一次
//    engine->Close()；Run 返回即本代收口，业务不 join 线程、不等待 future。
```

上面 2)–4) 的片段都位于 `bbtco_main()` 内；Engine 的发布、host hook 与两向退出
见 [Engine 主线程宿主与共享所有权契约](../architecture/native-lifecycle-seam-contract.md)。
`examples/http_net/main.cc` 仍使用已删除的 `EngineLifecycle`，迁移前不能编译或照抄。

示例场景：新 Client 向新 Server 发两次 GET（200/404）与一次 POST 回显；真实 accepted=1/requests=3 证明复用，不含自带 POSIX HTTP 服务端。

## Server 消费与责任

```cpp
// 应用先启动至少 4 个固定 scheduler worker（server 2 driver 槽 + client 1 槽）。
auto engine = net::Engine::Create(std::shared_ptr<net::RawHandler>{});
auto made = http::Server::Create(engine,
    [](IncomingCallContext ctx, http::IncomingRequest request) -> result<http::Response> {
        // 在原生有栈协程中，可 bbt_co_await 其它库；ctx.deadline 为有限总绝对期限。
        return result<http::Response>::ok({200, {}, std::move(request.body)});
    });
auto server = std::move(made).value();
// Create 只配置 handoff；引擎由应用入口的 native host hook 调用一次 Engine::Run，
// 配置 Independent、listen_addresses={127.0.0.1:0}，主任务用 CoWaitRunning 确认 Running 后：
auto started = server->Start(); // 必须检查；一次性，登记失败显式回滚并封口
// 原生控制域或受管协程中收口（每个 result 均须检查）：
// 优雅：封口 + 排空已授权请求；到期只报 TimedOut，不自动升级。
auto drained = server->Close(CloseOptions::Graceful(deadline));
// 立即：server->Close(CloseOptions::Immediate(reason)) 只封口、取消流源、令 session 失败，不等 handler。
auto stopped = server->Close(options)（wait_release=true，有限 deadline）; // 物理收口：callback/连接/listener
// 最后由 host hook 在 OnMainTaskDone 中调用 engine->Close()；
// Run 返回即 Engine 收口，不存在 runner/join。
```

- Engine 的接纳端专用于本 Server；不得并置第二个 `Engine::CoAccept` 消费者或 raw reader。Create 在 Idle 调用；不隐式 Run、监听或创建线程。应用 handler 是 `std::function<result<Response>(IncomingCallContext, IncomingRequest)>`，request 全部 owning；未认证 `peer_principal` 恒空，不用 IP 冒充身份。
- 一 accept 协程；每连接固定 **reader/parser + 请求执行任务 + 单 writer/sequencer**，不同连接并发。reader 唯一解析并按读顺序分配连接内序号；请求任务重叠执行 handler；writer 是唯一写 socket、唯一调用流式 `ResponseBody::Start/Next` 的角色，按序号升序写，前序未完成时后续已完成响应只能在有界队列等待。writer 是独立受管协程，不占用 handler 执行身份。网络只用公开 `Engine::CoAccept` 与 `ICoConnection::Read`/`Write`；支持分片、chunked、HEAD、204/304、对端 FIN 后完整请求回复。HTTP/1.1 origin-form；路由分派由独立的 `http::Router` 承担（见下节），Server 本身不做路径匹配。响应顺序=请求顺序，handler 执行顺序不保证（pipeline 中后序请求可能先产生副作用）。
- StopAccepting 与请求接纳在同一短锁内仲裁；不持锁调用 handler 或挂起。封口后 reader 停止接纳（封口后到达的 pipeline 请求不再授权），已接纳序号继续由 writer 排空；无已接纳责任的 idle/半包连接立即关闭，并请求 net 独立停止监听，不 Close(Immediate) 整个 Engine。
- `Close(CloseOptions)` 是唯一逻辑关闭控制面（Co 受管协程或 native 宿主调用）：`Graceful` 封口并等在途序号全部到写终态/明确放弃（所有已接纳 pipeline 响应仍按序写完，只有最后一条宣告 connection close），`Immediate` 强封连接、取消活动流源并把全部在途序号明确失败并**立即结算 inflight**；二者都发布首个关闭原因。到期只返回 `TimedOut` 并保留已接纳责任，不自动升级；`Graceful → Immediate` 升级由任一次 Immediate/`Close(Immediate)` 触发。`Close(CloseOptions::Immediate(reason))` 是任意域非阻塞的立即封口。本 Server 已接纳的 reader/writer/handler 任务内调用 `Close`/`Close(wait_release=true)` 是自等待，在副作用前返回 `InvalidContext`。首因固定；`CloseReason()` 在封口后即可观察。强杀任意 C++ handler 不在承诺内，关闭不撤销远端副作用。
- `wait_release=true` 时同一 `Close` 调用在模式工作完成（Immediate 封口 / Graceful 排空）后继续等本 Server 的连接 `Released()` 与 callback 销毁到同一绝对期限；`false` 时物理事实仍只由 `Close(wait_release=true)` 证明。响应字节/头上限在**发布入队前**校验：超限结果不进入队列，整条连接按协议放弃（writer 侧保留同一防御性检查）。
- `Close(options)（wait_release=true，有限 deadline）` 是物理收口凭据，同时检查 accept/reader/writer/请求任务全部销毁、全部连接 `Released()` 与 listener `AcceptingStopped()`；共享 Engine 可以仍 Running。Co 域按绝对期限以 50ms 分片等待，native 宿主域用同一谓词的有界等待。Stats 的 sessions 包含逻辑已关但未物理回收的 retiring，额度只在 task_done + Released 后复用；不是计数 0 假装 fd 已释放。超时保留共享状态和任务责任，宿主须继续驱动；永久不让出的 handler 无强杀保证。
- ServerOptions 复用 NetworkLimits；连接/header/body/target/handoff 有界。每个已接纳请求恰好占一个全局 `max_inflight` 名额（连接内待处理槽与全局额度共用同一预算），额度不足时 reader 暂停读 socket 与解析（TCP 背压），不丢已读字节、不越过额度继续解析。已完成未写响应数因此不超过 `max_inflight`，缓冲字节上界为 `max_inflight × max_body_bytes`（每个响应体另有单响应上限），流式每 chunk 受 `max_chunk_bytes` 限制。incoming_timeout/write_timeout 都有限且至多 24h；总期限从每个请求开始解析前起算，覆盖读取、handler 与回复写终态。超时或坏流明确断开，不承诺生成 4xx。
- handler 异常或错误 result 隔离为 500（仍算一个响应）；非法状态（仅接受 200–599）、CR/LF/控制字节头及用户 Content-Length/Transfer-Encoding/Connection/Upgrade/Trailer 明确断开。Beast 负责 framing；HEAD 保留表示长度但不发 body，204/304 不发 body。大响应以有界块 `conn.Write`，只有 Rejected + 无 operation 的零提交容量错误才减半重试，不重放已接纳写；每块复查期限与 Close 并让出。
- HTTP/1.1 要求唯一合法 Host；当前 Host 接受 ASCII DNS 标签/数值 host（IPv6 必须方括号，可选十进制 port），拒绝 userinfo、空白、路径和畸形端口。target 校验 origin-form URI 字符及百分号转义。CONNECT、Upgrade、Expect 明确拒绝并关连接，Expect 在读完 header、等待 body 之前拒绝。
- 本地 `Test_http_net_server` 覆盖 wire（分片/chunked/pipeline/HEAD/204/304/FIN）、停止授权及 idle/半包、回复写屏障 Drain、Close 超时责任、入站期限晚返、安全、Start/登记失败、inflight 背压及 owner 扣留物理回收额度；`server_pipeline_*` 用真实 TCP pipeline 覆盖「B 先完成仍按 A→B 回复」、慢首响应、Immediate/迟到结果、解析失败与断连。每例在进程退出前验证 HTTP 任务/连接、listener、Run/join 及 socket 基线归还。私有注入仅用于确定性阶段，不替代真实 TCP 字节。
- TLS、HTTP/2、客户端流式、代理、DNS 解析、DI/OpenAPI 等完整路由框架能力、永久阻塞 handler 的抢占以及跨平台/sanitizer/压力验证不在本批。

## Server 端响应流（streaming）

`Server::CreateStreaming(engine, StreamingServerHandler, ServerOptions, StreamingOptions)` 是独立于 `Create` 的工厂（不改动既有 `ServerHandler`/`Response` 签名与语义）。handler 返回 `ServerReply`（`std::variant<Response, std::shared_ptr<ResponseBody>>`）：`Response` 与 buffered 路径完全一致；`ResponseBody` 是**拉取源**——`Start(WaitOptions)` 在头提交前返回 `StreamStart` 选择 `Buffered`（等价完整回复）或 `Chunked`（可带 `first_chunk`），之后由该连接的 writer 逐个 `Next(WaitOptions)` 取 chunk（`nullopt`=EOF 写 `0\r\n\r\n` 终止块）。`Cancel()` 幂等，由记录/连接责任在 `Close`、commit 前后写头/写块失败、commit 后 `Next` 错误/超时/peer EOF 时恰好调用一次（Start 自身失败除外——Start 未成功即视为未获源）。

- **wire**：普通和流式响应的头名 token、头值控制字节、业务受控头及累计头大小由同一私有校验入口维护，编码仍各自独立；`http.net.response_header_parity` 使用同表合法/非法输入驱动普通工厂与流式工厂（直接 Response、Start Buffered、Start Chunked），检查真实 TCP 字节及提交前拒绝。chunked 回复头由 Beast `write_header` 序列化（`Transfer-Encoding: chunked`，禁止业务 Content-Length/TE/Connection/Upgrade/Trailer 头，与 buffered 同一校验）；数据块手工按 `HEX\r\ndata\r\n` 编码，上限 `StreamingOptions.max_chunk_bytes`（超限取消源并断连）。HEAD/204/304 拒绝 chunked（`InvalidArgument`，commit 前断开）。
- **状态机**：连接级 `ReadingRequest*（reader 并行解析）`；单条回复 `Dispatching → WaitingStreamStart → WritingBuffered | Streaming → Settled`。序列由 writer 按序号推进：前序流未结束时后序响应在 queue 等待；流 EOF 且 keep-alive 时继续写下一条。
- **commit 边界**：`Start` err 或异常在头提交前→普通 500 错误响应；commit 后 `Next` err/超时/取消/peer EOF→`Cancel()` 源一次 + 关闭连接，不重发、不补终止块。
- **双 deadline**：头提交前受请求总期限（`limits.incoming_timeout`）约束；commit 后切到 `min(stream_start + max_stream_lifetime, 剩余总期限)`，每次写再取 `stream_write_timeout` 较早者。两档都必须有限正值且 ≤24h（`CreateStreaming` 校验）。期限已尽时连 500 也无法写出，按超限明确断开。
- **背压**：拉取模型天然有界——writer 一次只挂起一个 `Next`，后序完成响应只在 queue 等待；写路径复用「Rejected + 无 operation = 零提交减半重试、已接纳不重放」规则；`Close`/`StopAccepting` 语义与 buffered 相同，Graceful 排空覆盖到最终 chunk 写终态或明确放弃，`Close(wait_release=true)` 仍要求 listener 停止 + 任务销毁 + 连接 `Released()`。
- 本地 `Test_http_net_streaming`（`http.net.stream_*`）覆盖：chunk wire 字节与复用、variant 三路、HEAD/204/304、commit 前后错误分界、peer RST、slow-reader 写超时（带 `elapsed ≥ stream_write_timeout` 阶段证据）、流总期限、Start 期限、Close-during-handler 源取消、StopAccepting/Drain/Close/Close(wait_release=true) 全收口；`stream_pipeline_order` 证明流未结束时后序已完成响应不抢写、终止块先于后序响应。

## Router 路由装配

`bbt::infra::http::Router`（`bbt/infra/http/Router.hpp`）是 Server 之上的轻量路由层：装配期注册路由与中间件，`Build()` 产出自拥有的 `ServerHandler` 快照直接交给 `Server::Create`，不产生第二套宿主。类型面：`Request{context, http, params}`、`Handler`、`Middleware`、`Text(status, body)`。

- 注册单线程装配；`Get/Post` 返回单路由 builder，可链式调用 `.Use(...).Depend(...).Handle(...)`，也可继续使用 `builder(handler)` 兼容直接注册。`AddRoute/Use/Include` 返回 void，配置错误（非法方法 token、非法路径、空 handler/中间件、同方法同形状重复）由 Router 保存并在 `Build` 统一返回。方法名按 HTTP token（RFC 9110 tchar）校验，大小写敏感不自动改写。
- 匹配先确定最具体路径（逐段静态优先于参数）再匹配方法：无路径 404，方法不命中 405 并输出 `Allow`，不回退到较宽泛的参数路由；`params` 取最终选中方法的参数名。尾斜杠严格匹配，重复斜杠按字面空段；`?` 后查询串不参与匹配；百分号编码、反斜杠、点段拒绝为 400。
- 中间件先注册先进入、逆序退出：全局链覆盖 Router 产生的 400/404/405；`Include` 的组中间件只包裹组内选中的 handler，不覆盖组前缀下的 404/405。`Next` 仅当前调用栈内有效、最多调用一次；不调用即提前返回。
- `Build` 复制路由表生成快照，原 Router 销毁或继续装配不改变已发出的 handler；`Include` 复制组当前路由与中间件链，自包含在副作用前拒绝。闭包捕获资源的并发与寿命由调用者负责。
- **单路由装饰器与依赖**：`.Use(middleware)` 只包裹该路由，先注册先进入、逆序退出；限流、鉴权、日志可在这里提前返回，未调用 `next` 时不会解析依赖或进入业务。`.Depend(dependency)` 声明业务函数所需的 typed dependency，`.Handle(function)` 按声明顺序注入 `const T&`；函数也可把 `Request&` 放在依赖参数之前。依赖 provider 返回 `result<T>`，错误沿 handler 返回，交给外层统一响应中间件映射。
- 依赖可用 `Dependencies<>().Depend(parent).Provide<T>(provider)` 声明子依赖。同一依赖声明在一个请求 Scope 内只解析一次，多个下游节点共享同一个值；每个请求重新解析，Scope 在 handler 返回或异常退出时逆序释放。依赖引用不得逃出当前 handler，也不承诺跨请求缓存或自动按类型查找。
- **统一响应**：建议最外层 `router.Use(...)` 处理 `Reply::err` 和下游异常，映射为统一 HTTP 错误体；已经构造的 401/404/405/429 等响应保留状态码和必要头。响应头提交后，流式响应不再通过这条同步 Router 链改写。
- 首版边界：ASCII 未编码路径、单段 `{name}` 参数、无 OpenAPI/自动 JSON/热更新；HEAD/OPTIONS 不自动派生，用 `AddRoute` 显式声明。本地 `Test_http_router`（`http.router.*`）覆盖匹配/405/参数归属/target 规则/全局与单路由中间件/依赖子图/请求内复用/短路/统一响应/快照/配置错误/Next 重入与一次真实 loopback 往返。

### framework 接入接缝

上层宿主组件（framework 的 `HttpServerComponent`）按以下时序使用本节接口，infra 不再为其改动：

- **组件按值持有 Router**：Router 默认可拷贝/可移动，装配完成的 Router 可直接移入组件 Options；OnInstall 校验 Options 后调用 `Build()` 取得 `ServerHandler`，随即 `Server::Create` 并保留 owner。Build 快照自拥有路由表，原 Router 之后销毁或继续装配均不影响在线 handler。
- **启动/关闭由宿主编排**：宿主负责 Engine 的配置与唯一一次 `Run`；组件在 Engine Running 且有监听后调用 `Server::Start`，成功后立即返回，不阻塞后续启动。`OnStop` 把同一个 `CloseOptions` 原样转发给 `Server::Close`；组件不 Run 也不关闭共享 Engine。
- **同一 Engine 上只存在一个 accept 消费者**：`Server::Create` 要求独占接纳消费（Server.hpp 的 Create 契约）；接入默认宿主时须把既有 HTTP Server 所有权迁入组件，不能并行创建第二个 Server 抢同一 accept 队列。
- **业务门走外层中间件 + Include**：未就绪/停止中的拒绝逻辑注册为外层 Router 的 `Use` 中间件（先于路由匹配运行，覆盖 Router 生成的 404），受管业务路由经 `Include` 挂入；Router 只认识 `Request/Reply`，不承载 App 状态或 Service 类型。后续 framework 可在组件边界组装自己的 dependency provider，不需要把 App/Service 反向引入 infra。

## 契约要点

- **超时**：直接在 `ClientOptions` / `RequestOptions` 上用 `SetTimeout(duration)` 设置四个阶段，或用 `SetConnectTimeout/SetReadTimeout/SetWriteTimeout/SetPoolTimeout(duration)` 只设置某一阶段；时长使用 `std::chrono::milliseconds`，须为正，分别由 `Create` / `BuildRequest` 校验。Client 默认四档均 5s；请求未设置的阶段自动继承 Client。`DisableTimeout()` 禁用全部阶段超时，`DisableConnectTimeout/DisableReadTimeout/DisableWriteTimeout/DisablePoolTimeout()` 只禁用相应阶段，但仍受总期限约束。底层字段为 `connect_timeout/read_timeout/write_timeout/pool_timeout`：Client 和 PreparedRequest 保存时长（`milliseconds::max()` 表示无阶段上限），RequestOptions 用 `optional<milliseconds>`，`reset()` 恢复继承。总绝对 deadline 单一真源是 `CallOptions.deadline`；各阶段期限 = min(总 deadline, 实际阶段开始时刻 + 阶段时长)。pool 等待不消耗后续 connect/write/read 预算；read 从每次 `conn.Read` 开始起算，总 deadline 不因读到字节重置。拨号/写入/读取及缓存快路径前预检总 deadline 和协程取消。
- **连接池**：origin（scheme/host/port）匹配；HTTP/1.1 一条连接一次一个活动请求。`PoolLimits{max_connections, max_keepalive_connections, keepalive_expiry, max_queue}` 为全局配额。完整响应且 keepalive 才回池；写超时未终态/读中断/取消/协议错一律退役；合法 EOF 定界响应正常返回但不复用，不自动重放请求。空闲 stale 检查只用公开观察（`Closed/ReadEof/ReadError/ReadableBytes>0`），keepalive 过期惰性判定（Acquire/Release/Stats 的 sweep 内执行，无后台 reaper）。
- **物理记账**：Dialing/Active/Idle/Retiring 同列表，`entries.size()` 是本池资源预算。成功连接只在底层 `Connection::Released()`（与 `conn.IsReleased()` 同一真源）后出账；失败/取消/未交付拨号保留 `ConnectionResult::reclaim`（`ReclaimReceipt`），只在其 `Released()` 后出账。逻辑 `Closed()` 不能代替物理释放，排队者在资源回收前不得重开连接。`PoolStats`（active/idle/dialing/retiring/waiting/budget）是公开观测面。
- **租借责任**：内部 Lease 不可复制、可移动，独占一次在途归还责任；移动源和重复归还不结算。完整响应后显式确认可复用才回池，早退、异常和未确认健康的析构默认退役；析构不挂起。Lease 持有共享池状态而非池句柄引用，等待者只持有待领取连接，领取成功才生成 Lease，避免复制归还责任和形成状态引用环。物理额度仍仅在 `Released()` 后出账。
- **排队**：有界 `max_queue`，满即 `Overloaded`；CoWaiter 登记→锁内复查→Notify，锁永不跨挂起；Notify 不等于获得，恢复后锁内领取，孤儿 handoff 回池。分配行为：快路径先扫同 origin 空闲；归还把名额交队首同 origin 等待者；异 origin 被空闲连接占满预算时，退役一条异 origin 空闲连接，真实回收后再拨号。不承诺严格 FIFO 公平。排队者唤醒源：同 origin 完整归还、封口、池超时，以及 20ms 有界重试切片（预算释放没有事件源，切片保证活性）。
- **错误面**：沿用 `bbt::infra::ErrorCode`——未提交前的排队/连接/期限超时 `TimedOut`、排队满 `Overloaded`、建连失败 `TransportError`、坏流 `ProtocolError`、封口 `Closed`、协程取消 `Cancelled`（保留请求阶段事实）；请求字节完整写出后的读超时/连接中断按 `OutcomeUnknown`（`Error.request_phase=RequestCommitted`，请求可能已生效）。已接纳但写入尚未终态时也不能认定零发送，不自动重试。
- **关闭**：状态单向前 `Open → Draining → Closed`。`Close(CloseOptions)` 是唯一逻辑控制面（Co 分片 50ms / native 有界）：`Graceful` 封口、拒绝新请求（以首个原因或 `Closed` 失败）、唤醒排队者并在期限前排空已借出 Lease+在途；`Immediate` 等价 `Close(Immediate)`，立即封口、发布首因并令在飞/排队请求失败，不等待在飞请求。`TimedOut` 只表示期限已到并保留责任，不自动升级；`Graceful → Immediate` 由 `Immediate`/`Close(Immediate)` 触发，首因固定。都不关共享 Engine、不假装物理排空；`Close(options)（wait_release=true，有限 deadline）` 才等池条目真实退役（`Connection::Released()`）。`CloseReason()` 在封口后可读；析构=Close(Immediate)。
- `wait_release=true` 时同一 `Close` 在模式工作完成后继续等池条目按真实回收出账到同一绝对期限；不关共享 Engine。

## 输入边界与协议覆盖

- `BuildRequest` 和 `Send` 均校验输入；公开可变的 `PreparedRequest` 不是可信凭据。URL、target、数值地址与默认 Host 派生信息必须一致。
- `ClientOptions.max_request_body_bytes` 限制最终 HTTP request body；`max_header_bytes` 限制响应 parser 的 header 字节数。两者默认 `nullopt`，分别保持原有无额外请求上限和 Beast 默认响应头行为；有限值必须大于零，响应头值还必须不超过 parser 可表示上限。
- 方法和头名须为 HTTP token；头值拒绝 CR/LF/NUL 及除 HTAB 外的控制字符。每个头名最多 8 KiB、头值 32 KiB、全部头合计 64 KiB。
- `Content-Length`/`Transfer-Encoding` 由客户端根据 raw body 生成；拒绝手工 framing，避免歧义。需恰好一个非空 Host；不支持 CONNECT 隧道。
- 字段序列化在借用连接前完成；序列化错误映射 `InvalidArgument`。借用有异常收口，错误不得遗留 active 名额。
- `Test_http_net_regressions` 覆盖分片头/体、64 个 chunk、HEAD/中间响应、EOF/截断、body limit、输入边界、冷/热连接预取消与过期、阶段期限、跨 origin、并发交接及延迟物理回收预算。
- 这些是回归注册范围，不是凭测试名称证明通过；实际执行使用上文 CTest 命令。

## 里程碑能力边界（未实现不得暗示）

实现范围（本地验收命令见上文，不代表全部 HTTPX 功能）：http scheme、数值 IPv4 host、GET/POST/扩展 token 方法 `Request`、raw body（含二进制）、base_url/默认 headers/params 合并（params client 在前、请求级追加、百分号编码）、分阶段超时、连接池复用/上限/排队/过载/退役/过期、关闭与取消竞争、坏流退役。

未实现（接口不暗示）：https/TLS、主机名 DNS（host 名显式报 `InvalidArgument`，DNS 方案需单独决策）、json/form 编码、客户端流式响应（`Client::Send` 仍是有界 buffered）、重定向、代理、HTTP/2、请求自动重试、事件 hook。DNS/新依赖/native worker 属未授权决策点，不在本模块内悄悄实施。

## 已知局限

- 回收由 net owner 异步推进；retiring 期间仍占名额，不能因等待结束就假定资源已释放。多个 Client 的池独立，各自预算不构成共享 Engine 的全局 fd 上限。
- 排队者不因「预算释放」被即时唤醒，20ms 重查切片会增加分配延迟；这是重查节拍，不是在任意调度压力下的严格延迟上界。
- 只实现完整 HTTP/1.1 消息：支持分片、chunked、HEAD、有限中间 1xx 与 EOF 定界；101 升级显式拒绝，最多接受 16 个中间响应。多余尾随字节不复用。CONNECT 隧道不支持。
- 跨平台未验证（net 层当前仅 Linux）。
