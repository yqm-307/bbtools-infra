# MCP 2026-07-28 Streamable HTTP 与 lifecycle 逐条 crosswalk

> 状态：草案（工作树，未合入）。逐条核对官方规范 2026-07-28 的
> Streamable HTTP / lifecycle(versioning) / subscriptions / discover 的
> MUST/SHOULD，满足 architecture §12.4「官方 MCP spec 2026-07-28 must/should
> crosswalk」与 §8.1 [A]「exact spec crosswalk」门禁。
> 每条记录「规范锚点 → 实现位置 → 测试名」；无对应测试的条目明确标未覆盖。

- 日期：2026-10-31 · 对象：`bbt/infra/mcp/`（core + stdio + Streamable HTTP）
- 规范来源（上游 `modelcontextprotocol/modelcontextprotocol`，revision `2026-07-28`；tag `2026-07-28` 于 2026-10-31 核验指向提交 `5f5440bb26a62e2cf3440b92da5a667efa03b267`）。以下路径不在本仓工作树中，链接固定到该提交，不使用可漂移的 main/latest；每个链接均经 HTTP 200 核验，且与 `git show <提交>:<路径>` 字节数一致：
  - [`docs/specification/2026-07-28/basic/transports/streamable-http.mdx`](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/5f5440bb26a62e2cf3440b92da5a667efa03b267/docs/specification/2026-07-28/basic/transports/streamable-http.mdx)（下称 **SHTTP**）
  - [`docs/specification/2026-07-28/basic/versioning.mdx`](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/5f5440bb26a62e2cf3440b92da5a667efa03b267/docs/specification/2026-07-28/basic/versioning.mdx)（下称 **VER**）
  - [`docs/specification/2026-07-28/basic/patterns/subscriptions.mdx`](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/5f5440bb26a62e2cf3440b92da5a667efa03b267/docs/specification/2026-07-28/basic/patterns/subscriptions.mdx)（下称 **SUB**）
  - [`docs/specification/2026-07-28/server/discover.mdx`](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/5f5440bb26a62e2cf3440b92da5a667efa03b267/docs/specification/2026-07-28/server/discover.mdx)（下称 **DISC**）
  - [`docs/specification/2026-07-28/basic/index.mdx`](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/5f5440bb26a62e2cf3440b92da5a667efa03b267/docs/specification/2026-07-28/basic/index.mdx)（下称 **BASE**，`_meta` 键命名规则与错误码分区）
  - [`schema/2026-07-28/schema.ts`](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/5f5440bb26a62e2cf3440b92da5a667efa03b267/schema/2026-07-28/schema.ts) 错误码分区（**SCHEMA**，proto-mcp-gates §1）
- 实现位置缩写：`HT`=`bbt/infra/mcp/HttpTransport.cc`，`HP`=`bbt/infra/mcp/McpHttpTransport.hpp`，`PC`=`bbt/infra/mcp/Protocol.cc`，`RG`=`bbt/infra/mcp/Registry.cc`，`MT`=`bbt/infra/mcp/McpTypes.hpp`。测试名均为 ctest `mcp.*` 用例名。

## 1. Endpoint 与安全（SHTTP「The server MUST provide…」「Security & Endpoint」）

| # | 规范条目（原文锚点） | 等级 | 实现位置 | 测试 |
|---|---|---|---|---|
| 1 | 单一 MCP endpoint path 支持 POST（`The server **MUST** provide a single HTTP endpoint path … supports POST`） | MUST | `HT` `kPath="/mcp"` + `Handle` 路径门禁 | `mcp.http.headers`（非 `/mcp` → 404） |
| 2 | Origin 校验防 DNS rebinding；`Origin` 存在且非法 → `403 Forbidden`，body MAY 为无 id 的 JSON-RPC error | MUST | `HT` `OriginOk`（白名单空表时任何给出的 Origin 即拒、无 Origin 放行）→ 403 + `JsonErrorBody(…, std::nullopt, …)`（无 id error body，命中 MAY 形态） | `mcp.http.headers`（Origin 命中/未命中/无） |
| 3 | 本地运行时 **SHOULD** 只绑 localhost 而非 0.0.0.0 | SHOULD | **已实现**（fix-mcp-policy-r1）：`McpHttpTransport::Start` 拒绝任何非 loopback 监听地址（`engine->LocalAddresses` 快照，127.0.0.0/8、::1、Unix 之外 → `UnsupportedRoute` `mcp.http.non_loopback`）；`McpHttpOptions::allow_non_loopback`（默认 false）为显式 opt-in。`allowed_hosts`/`allowed_origins` 是头白名单（防 Host/Origin 头伪造），不替代本绑定检查 | `mcp.http.bind`（0.0.0.0 默认拒绝 + opt-in 放行） |
| 4 | Servers **SHOULD** implement proper authentication | SHOULD | 本仓不提供认证，由上层 host/origin 白名单承载（HP §「未宣告」清单已列） | n/a（上层责任） |

## 2. Sending Messages（SHTTP「Sending Messages」，server 侧镜像条目）

| # | 规范条目 | 等级 | 实现位置 | 测试 |
|---|---|---|---|---|
| 5 | JSON-RPC 消息必须各自一个 HTTP POST（SHTTP「Sending Messages」1，client 义务）；server 侧 POST-only 为实现选择（GET/DELETE→405 的规范依据见 #42） | MUST（client） | `HT` `Handle`：`request.method != "POST"` → 405 | `mcp.http.headers`（GET/DELETE/PUT/HEAD → 405） |
| 6 | `Accept` 必须同时含 `application/json` 与 `text/event-stream`（client MUST）→ server 校验缺任一 → 拒 | MUST（client 侧）；**406 是实现选择**——规范未规定违规状态码（SHTTP mdx 中无 406），本实现选 406 Not Acceptable | `HT` `AcceptOk`（精确/type\*/\*/*、参数忽略、多 Accept 头聚合）→ 406 | `mcp.http.headers`（单媒体/文本/空 → 406；参数与 wildcard 放行） |
| 7 | 每 POST 必须带 request metadata headers（见 §4） | MUST | `HT` `HeaderGate` modern 分支 | `mcp.http.modern_headers` / `mcp.http.mcp_name` |
| 8 | body 必须是单个 JSON-RPC request/notification；client MUST NOT 发 response | MUST | `HT`→core `ParseRequestFrame`：混入 `result`/`error` 的帧 → -32600；batch/非标量 id 同拒 | `mcp.protocol`（帧完整性）+ `mcp.http.responses`（parse error → 200 -32700） |
| 9 | notification 被接受 → **202 Accepted** 空 body；不接受 → HTTP error 状态，body MAY 为无 id JSON-RPC error | MUST | `HT` `Handle`：`done.has_response == false` 且非 listen → `Empty(202)` | `mcp.http.responses`（legacy cancelled 与现代信封通知 → 202） |
| 10 | request 必须返回 `application/json` 单对象**或** `text/event-stream` 流；client MUST 支持两者 | MUST | `HT`：`subscriptions/listen` → `McpEventBody`（SSE chunked）；其余 → `JsonBody`/`JsonErrorBody` `application/json` | `mcp.stream.sse`（SSE 头形状）+ `mcp.http.responses`（JSON 200） |

## 3. Receiving Messages / SSE（SHTTP「Receiving Messages」「Cancellation」）

| # | 规范条目 | 等级 | 实现位置 | 测试 |
|---|---|---|---|---|
| 11 | SSE 流上 server MAY 发与原始请求相关的 notifications；**MUST NOT** 发独立 JSON-RPC requests | MUST / MUST NOT | `McpEventBody` 只承载会话订阅事件帧（`notifications/*`），本仓 v1 无 server→client request 产线（MRTR inputRequests 走 result 内嵌，不经流） | `mcp.stream.sse`（事件帧 = notification） |
| 12 | final response **SHOULD** 终止流 | SHOULD | `McpEventBody::IsFlowEndFrame`：success response（`resultType=complete` + id）排干后置 EOF | `mcp.stream.sse_queue`（Close 后 EOF）、`mcp.http.lifecycle` |
| 13 | 发起 SSE 时 server **SHOULD** 带 `X-Accel-Buffering: no` | SHOULD | `McpEventBody::MakeSseStart` 固定该头（另 `Cache-Control: no-cache`） | `mcp.stream.sse`（头断言） |
| 14 | 长流鼓励周期性 `:` 注释 keep-alive（encouraged，非编号条款） | 非规范等级（Note: encouraged） | `McpEventBody` `m_keepalive` tick 产 `": \n\n"` chunk，不重置流期限 | `mcp.stream.sse`（keepalive tick 到达） |
| 15 | `Last-Event-ID` resumable 流不支持（declarative） | — | `HT`/`HP` 无视 `Last-Event-ID`，无重放缓冲 | `mcp.stream.sse`（SSE 头形状断言：text/event-stream、chunked、`X-Accel-Buffering`、无 `Mcp-Session-Id`）；**未覆盖**：帧内无 `id:` 字段未单独断言（见「未覆盖」） |
| 16 | server **MUST** 将关闭 SSE 响应流视为该请求的 cancel；server SHOULD 尽快停止工作且 **MUST NOT** 再发消息 | MUST / MUST NOT | `McpEventBody::Cancel`（commit 后 http 层唯一取消点）：封队 + 卸 sink + `EndAllSubscriptions`（S20 断连驱动订阅结束） | `mcp.stream.sse`（peer close 后发布计 0）、`mcp.http.lifecycle`、`mcp.stream.sse_queue` |
| 17 | 周期发 SSE comment keep-alive 防中介断连：Note 中为 encouraged，非 SHOULD（同 #14 的 listen 语境） | 非规范等级 | 同 #14（listen 流同一 `McpEventBody`） | `mcp.stream.sse` |

## 4. Request Metadata / 标准头 / 版本（SHTTP「Request Metadata」「Standard Request Headers」「Case Sensitivity」「Server Validation」）

| # | 规范条目 | 等级 | 实现位置 | 测试 |
|---|---|---|---|---|
| 18 | 每 POST 必须含 `MCP-Protocol-Version`；值 MUST 等于 `_meta.io.modelcontextprotocol/protocolVersion`；失配 → `400` + `HeaderMismatch` | MUST | `HT` `HeaderGate` modern：缺头或头≠信封版本 → 400 + `-32020`（id 原值） | `mcp.http.modern_headers`（缺/失配/垃圾值 → 400 -32020） |
| 19 | server 不实现请求版本（未知或不支持）→ **MUST** `400 Bad Request` + `UnsupportedProtocolVersionError`（-32022，data 含 supported/requested） | MUST | `PC` `ValidateModernMeta`：非 `2026-07-28` → -32022 + `data.{requested,supported}`；`HT` `ResponseErrorCode` 结构化判定 → **HTTP 400**（本轮 B1 修复；原实现仅 -32601→404） | `mcp.http.responses`（`2099-01-01` → 400 + -32022 + requested/supported） |
| 20 | server 不实现请求 method → **MUST** `404` + `-32601`（JSON-RPC error body 区分 legacy 404） | MUST | `RG` 方法表未命中 → -32601 body；`HT` `ResponseErrorCode` → **404**（结构化 `error.code` 判定，非 body 子串——本轮 N3 修复） | `mcp.http.responses`（`bogus/method` → 404 + -32601） |
| 21 | 缺 `MCP-Protocol-Version` 头：不支持 <2025-06-18 client 的 server **MUST** 按 Server Validation 拒绝（modern 面）；支持更老 client 的 server MAY 按 `2025-03-26` 处理 | MUST | `HT` `HeaderGate`：modern 缺版本头 → 400 -32020；legacy（无 modern 信封）无版本头 → 进入 core 无会话态校验（`-32602`，见 `mcp.http.legacy`），不冒充 `2025-03-26` | `mcp.http.modern_headers` + `mcp.http.legacy` |
| 22 | `Mcp-Method`（all requests）与 `Mcp-Name`（`tools/call`/`resources/read`/`prompts/get`）**REQUIRED**；named 方法 `Mcp-Name` 源 = `params.name`/`params.uri` | MUST（REQUIRED） | `HT` `HeaderGate`：`Mcp-Method` 必现且=method；三 named 方法 `Mcp-Name` 必现且解码后=params.name/uri；非 named 方法给出即拒 | `mcp.http.modern_headers` + `mcp.http.mcp_name`（含 prompts/get） |
| 23 | `Mcp-Name` 源值不能安全表示为 ASCII header → Base64 sentinel `=?base64?…?=`（server MUST decode 后比对；形似 sentinel 的字面值也须按编码比对） | MUST | `HT` `DecodeHeaderValue`/`B64Decode`：sentinel 严格解码（字母表/4 倍长/≤2 pad/UTF-8 良构），畸形→不匹配；直值含非 ASCII/控制字节→不匹配（`=?base64?AAAA?=` 字面体值场景覆盖） | `mcp.http.mcp_name`（直值/sentinel/非 ASCII/畸形/字面 sentinel 全矩阵） |
| 24 | header 名大小写不敏感、值大小写敏感（RFC 9110） | MUST | `HT` `EqualsNoCase`（名）/精确比较（值） | `mcp.http.*`（全部头门禁经 `HeaderValues` 大小写不敏感命中） |
| 25 | Server Validation：处理 body 的 server **MUST** 在头/体不一致、必需头缺失、头值含非法字符时拒 `400` + `-32020` | MUST | `HT` `HeaderGate` 全分支（重复头即拒 → 第一/最后副本读者分歧消除；`mismatch` 统一 400+-32020） | `mcp.http.modern_headers`（重复头/缺头/失配）+ `mcp.http.mcp_name`（解码失败） |

## 5. Custom Headers from Tool Parameters（SHTTP「Custom Headers…」「Value Encoding」「Client/Server Behavior」）

| # | 规范条目 | 等级 | 实现位置 | 测试 |
|---|---|---|---|---|
| 26 | `x-mcp-header` 注解（server MAY；值非空、field-name token、大小写不重复、仅 primitive integer/string/boolean、仅 `properties` 静态可达路径） | MAY + schema 约束 | **未接线**：v1 `x-mcp-header` 注册期 fail-closed 拒绝（`RG` AddTool 顶层与 `properties.<name>` 浅层检查，错误 `mcp.registry.x_mcp_header`）；无 `Mcp-Param-*` 生成/校验 | **未覆盖**：`x-mcp-header` 拒绝路径断言在 `tests/mcp/Test_mcp_tools.cc`，仅 schema-ON 配置注册为 `mcp.tools.*`——本次构建（schema OFF）无可执行 ctest 证据 |
| 27 | client 有注解且值存在 → MUST 发 `Mcp-Param-{Name}`；值 null/不在 arguments → MUST 省略；server MUST 校验已识别头与 body 一致、非法字符 MUST 拒 400 -32020；不识别的中间 server MUST 转发忽略 | MUST | **不适用**：v1 无 `x-mcp-header` 注解可达（#26 fail-closed），故无「已识别 `Mcp-Param-*`」；`HT` 不把 `Mcp-Param-*` 当路由头，不校验也不拒 | 无（见「未覆盖」） |
| 28 | `Mcp-Param-*`/name 值编码：非 ASCII、控制字符、首尾空白或 sentinel 冲突值 → `=?base64?…?=`；server MUST 先解码再比对 | MUST | sentinel 解码器与 `Mcp-Name` 共用（`HT` `DecodeHeaderValue`/`B64Decode`，同 SDK `shared/inbound.py` 规则） | `mcp.http.mcp_name`（同一解码路径） |

## 6. Subscriptions（SUB）

| # | 规范条目 | 等级 | 实现位置 | 测试 |
|---|---|---|---|---|
| 29 | server **MUST NOT** 发送 client 未显式请求的 notification 类型 | MUST NOT | `RG` `ParseSubscriptionFilter` 只接纳过滤内类型；未注册 uri 不接纳（omitted） | `mcp.stream.sse`（`mem://nope` 不在 ack、不投递）+ `mcp.subscribe` |
| 30 | `notifications/subscriptions/acknowledged` **MUST** 为首帧（携带 `_meta.io.modelcontextprotocol/subscriptionId`），其前 MUST NOT 发该订阅的通知 | MUST / MUST NOT | `HT` `Handle`：sink 先于 `HandleFrame` 挂载捕获，ack 同步发出后预置 `McpEventBody` 队首（`captured->frames` 首帧即 ack）；subscriptionId = listen 请求 id（`MakeSubscriptionIdMeta`） | `mcp.stream.sse`（首 data 帧 = acknowledged + subscriptionId = `"L1"`） |
| 31 | 流上所有通知携带 `_meta.io.modelcontextprotocol/subscriptionId`（= listen 请求 JSON-RPC id） | 陈述（SUB:86-91 为声明句：通知「carry」subscriptionId）；client 在 stdio 上 MUST 用该字段关联 | `RG`/`PC` `MakeSubscriptionIdMeta` 注入每事件帧 | `mcp.stream.sse`（事件帧含 subscriptionId） |
| 32 | server 主动结束订阅 SHOULD 先回 listen 的空 result 再关流 | SHOULD | `McpEventBody::IsFlowEndFrame`：流结束响应（resultType=complete + id）排干后 EOF（Close 路径亦经 Cancel→排干已入队帧） | `mcp.stream.sse_queue`（Close 后 peer EOF）+ `mcp.http.lifecycle` |
| 33 | transport 关闭（HTTP timeout/TCP 断连）→ 订阅结束 | — | `McpEventBody::Cancel` → `EndAllSubscriptions` | `mcp.stream.sse`（peer 断连后发布计 0） |

## 7. Lifecycle / Versioning（VER + DISC）

| # | 规范条目 | 等级 | 实现位置 | 测试 |
|---|---|---|---|---|
| 34 | 每请求 `_meta` 携带 `io.modelcontextprotocol/protocolVersion`（必需）与 `clientCapabilities`（必需）；`clientInfo` 为可选字段（schema 中 `clientInfo?`） | MUST（前两者） | `PC` `ValidateModernMeta`：缺/非 object/类型错 → -32602 固定文本 | `mcp.protocol`（meta 形状矩阵）+ `mcp.http.modern_headers` |
| 35 | 不支持版本 → `UnsupportedProtocolVersionError` 列 supported（同 #19；VER 同名 MUST） | MUST | 同 #19 | `mcp.http.responses` + `mcp.protocol` |
| 36 | client SHOULD 从 supported 列表选共同版本重试 | SHOULD | client 行为，server 侧已给 supported 列表（`MT` `kSupportedProtocolVersions` → -32022 data.supported） | `mcp.http.responses`（body 含 `"supported":`） |
| 37 | Servers **MUST** implement `server/discover`（DiscoverResult 含 supportedVersions 与 capabilities；`_meta` 中 serverInfo SHOULD 包含；instructions 可选） | MUST | `PC` `MakeDiscoverResult` + `RG` 内建方法表（含 resultType=complete、ttlMs=0、cacheScope=private） | `mcp.http.responses` + `mcp.server`/`mcp.registry`（supportedVersions/capabilities） |
| 38 | extension 标识符 MUST 遵循 `_meta` 键命名规则（VER「Extension Negotiation」；键规则见 BASE「_meta」） | MUST | `PC` `IsValidExtensionKey`（prefix/label/name 规则，用于 clientCapabilities.extensions 键校验） | `mcp.protocol` |
| 39 | dual-era server MAY 同端点服务两 era；`initialize` 选 legacy 语义 | MAY | `HT` `HeaderGate` legacy 分支：`AdoptLegacyHeaderGate`（版本头采纳连接会话）；无 `_meta` 帧走 legacy 方法表（`RG` era 隔离） | `mcp.http.legacy`（initialize/业务直通/非服务 revision 拒） |
| 40 | HTTP 上 modern 请求返 `4xx` 且 body 为可识别 modern error → 不切 legacy fallback（client 判定；server 侧 400/404 的 body 由 #19/#20 的 MUST 约束） | SHOULD（client） | `HT` 所有 400/403/404/405/406/413/421/503 应答统一 `JsonErrorBody`/`Empty`：400/404 携带 JSON-RPC error body（-32020/-32601/-32022 均为现代可识别错误） | `mcp.http.responses` + `mcp.http.headers`（body 形状断言） |
| 41 | modern-only server SHOULD 在对 `initialize` 的错误中点名所支持版本 | SHOULD | `RG`：`initialize` 在 modern 方法表 → -32601（不含 supported 列表）。**偏差记录**：v1 未在 initialize 的 modern 应答中回显 supported；客户端可经 `server/discover` 获列表 | `mcp.protocol`/`mcp.server`（initialize era 行为） |
| 42 | earlier revisions 机制（`Mcp-Session-Id`/GET SSE/Last-Event-ID/流上 server request）不属于本 revision；仅本 revision 的 server SHOULD：GET/DELETE→405、忽略 Mcp-Session-Id 且不 mint/echo、忽略 Last-Event-ID | SHOULD | `HT`：非 POST → 405；`Mcp-Session-Id` 绝不 mint/echo（`HP` §「未宣告」+ 无该头产出路径）；`Last-Event-ID` 无视；无 GET 流、无流上 request（#11） | `mcp.http.headers`（GET/DELETE→405）+ `mcp.stream.sse`（无 `Mcp-Session-Id` 头断言） |

## 8. 错误码分区（SCHEMA，配合 proto-mcp-gates §1）

| # | 规范条目 | 实现位置 | 测试 |
|---|---|---|---|
| 43 | `-32020` HeaderMismatch / `-32021` MissingRequiredClientCapability / `-32022` UnsupportedProtocolVersion 为 MCP 保留区已分配值 | `MT` `JsonRpcErrorCode`；`PC` 产出 -32020（HT gate）/-32021（`CheckInputRequestCapabilities`）/-32022 | `mcp.http.*`（-32020/-32022）、`mcp.subscribe`（-32021） |
| 44 | `-32002`/`-32042` 为历史保留，不得复用新语义；resource-not-found 用 `-32602` | `MT` 枚举未定义 -32002/-32042；`RG` 资源未命中 → -32602 | `mcp.registry`/`mcp.protocol` |

## 未覆盖（明确列出，不假装修复）

- **#15 SSE 帧无 `id:` 字段**：`mcp.stream.sse` 断言头形状与事件帧内容，但没有单独断言事件帧不含 SSE `id:` 行（`McpEventBody` 本来就不产出 `id:`，未防守性断言）。补一条 `id:` 计数为 0 的断言即可。
- **#26 `x-mcp-header` 拒绝路径**：断言在 `Test_mcp_tools.cc`，仅 schema-ON 注册；schema OFF 构建下无对应用例。
- **#27 `Mcp-Param-*`**：v1 `x-mcp-header` 未接线（fail-closed 拒绝），无对应流量可校验。若后续启用，需补 client 生成 + server 校验 + 中间转发用例。
- **#41 `initialize` 的 modern 错误带 supported**：当前 -32601 无 supported 列表（规范 SHOULD）。改动面在 `RG` 方法表应答，未在本轮修复范围；记录为偏差。
- **413 语义**：`HT` 对超 `max_body_bytes` 返回 413 无 JSON-RPC body（规范对不可接受 notification/request 只要求 HTTP error 状态，413 属合法 HTTP error）。无专项断言之外的缺口。
- **Host 头**：2026-07-28 SHTTP 对 Host 无规范 MUST（`\bhost\b` 仅出现在散文中），只有 Origin 校验。本实现的 `allowed_hosts` 421 门禁是超出规范的本仓加固（防 Host 头伪造），不建行、不声明为规范要求。
- **legacy `Mcp-Method`/`Mcp-Name` 头**：本实现对 legacy 帧给出路由头即 400（HP：头不属于 legacy profile）；规范对 legacy POST 的头要求未定义（「header requirements for notification POSTs are not defined by this revision」）。行为偏严但不违规范。

## 与 B2 的关系（已于 fix-mcp-policy-r1 收口）

architecture §8.1 [A] 要求 legacy `2025-06-18`「only after exact spec crosswalk」。owner 决策：**不补 crosswalk、移除 2025-06-18**——服务它会冒充「已支持的旧版」，而不服务的版本按规范应反提议/拒绝。当前 `supportedVersions`/`-32022` 的 `data.supported` 只回显 `2026-07-28` 与 `2025-11-25`；legacy header gate 只采纳 `2025-11-25`；`initialize` 对 `2025-06-18` 提议反提议 `2025-11-25`。本表不再声称构成 2025-06-18 的任何支持面，#21/#39 的双 era 分流面保留（legacy era 现在只含 2025-11-25 一个 revision）。
