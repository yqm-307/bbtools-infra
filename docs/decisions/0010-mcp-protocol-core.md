# 0010 MCP server：协议、依赖与交付边界

状态：本地实现候选，未完成最终交付门禁。本文替换历史切片摘要；不把已实现的 tools、Schema、transport 再写成「未实现」，也不把实现存在写成全验收通过。
基线：`3136d1de5031a2eec55475e770d5ebbadc3c32da`。需求口径为已批准的 R1–R11 / S1–S24（revision 12）；文档限制说明不自动解除原 Schema/兼容验收。

## 1. 核心与公共契约

- 公开目录 `include/bbt/infra/mcp/`；namespace `bbt::infra::mcp`，C++17，自有 JsonValue/业务结果类型；无公共 Boost/Beast/jsoncons/ICU 类型。
- `bbt::infra_mcp_core` 保持 common/coroutine 与 Boost.JSON 闭包；stdio 在 core 内。
  `bbt::infra_mcp_http` 独立组合公开 TransportRuntime/CoTCP 与私有 Beast framing，不依赖 HTTP/RPC/protobuf/资源 adapter。
- Boost.JSON 是唯一协议 DOM。`JsonValue.cc` 单 TU 包含 `boost/json/src.hpp`；解析复用 Boost basic_parser/SAX，不手造词法器。
  重复对象键、非法 UTF-8、非有限数、超 int64/uint64 的纯整数字面量明确拒绝；只接受完整单文档。
  `RequestId` 为 owned string/int64/uint64，精确保留原值，不用 FIFO 或 double 关联。
- 有限浮点遵循 IEEE double 舍入；如 `1e-999` 下溢为 0。不承诺任意十进制浮点无损。
  `AsDouble` 不静默转换超 2^53 精度范围的整数；通知不产生 JSON-RPC 响应，合法通知的参数错误也静默丢弃。
- 应用 `Create` → 注册三原语 → `Start` 一次冻结快照 → transport；坏注册不发布半成品，Start 后拒注册，Close 幂等且不重开。
  资源/prompt 返回值由库校验并构造信封；URI 不自动访问文件或网络，动态 URI template 未注册，模板列表真实为空。

## 2. tools 与私有 Schema

- `BBT_MCP_SCHEMA_ADAPTER` 默认 OFF；ON 构建 `bbt::infra_mcp_schema`（实现 target `bbt_infra_mcp_schema`）。
  公开 `EnableToolsSchemaValidation(McpServer&)` 在该 adapter 实现；业务显式链接并在 Start 前调用，再 AddTool。
  core 本身不含该实现或 ICU 闭包；未启用时 AddTool fail-closed，不注册未校验工具。
- 输入 Schema 根须 `type:object`，注册期完整 draft2020-12 metaschema 校验后编译；不声明 `$schema` 默认 2020-12，不支持 dialect 拒绝。
  tools 列表稳定有界分页；输入校验失败 `-32602` 且 handler 不进入，业务 `is_error` 不是 schema/协议错误。
  若声明 outputSchema，则 structuredContent 必须通过真实校验；非法输出/异常固定 `-32603`，不回显敏感值。
- 禁自动网络 `$ref` 与隐式强转/填默认/删字段；离线 suite 专用 remotes resolver 不等于产品开放外部 ref。
  注解/未知扩展按标准处理，`format` 是 annotation，不宣称 format assertion。
- 固定 jsoncons v1.10.0 `9e57e421285422d7aeeb5aa7dc839f6a19121ee6`，BSL-1.0，仅私有校验；
  dynamic-scope 三头 overlay 修复跨资源 `$dynamicRef`。ICU 78.2（已测 Ubuntu amd64 `78.2-2ubuntu1`）从显式 `MCP_ICU_ROOT` 查找头/库，无系统安装。
  运行时仍须显式私有库环境保证三库均落隔离根；裸 RUNPATH 曾使传递依赖 libicudata 落宿主，不能将 CMake 查找根视为运行时无回落保证。
  `MCP_JSONCONS_ROOT` 必须是带经核验 overlay 的根，原版 jsoncons 不是已测候选。CMake 仅检查根/查找私有库，不等于自动核完整 SHA。
- **可复现交付仍有硬缺口**：隔离重放已成功，但仓内尚无完整 overlay/包锁/重放配方；不能把 scratch 依赖成功构建当作全新公开检出可复现。
  不以这次文档/示例改动扩展依赖或安装授权。

## 3. revision、能力与错误

- 对外实际服务版本仅 `2026-07-28`、`2025-11-25`、`2025-06-18`；modern 接受集仅第一个。
  modern 每请求独立校验 `_meta` 的 `io.modelcontextprotocol/protocolVersion`（string）和
  `io.modelcontextprotocol/clientCapabilities`（object），不靠 initialize 或前一请求建立现代 session。
- legacy stdio 保留 initialize/initialized 门禁与 ping；legacy HTTP 是规范允许的无 session profile。
  首次 initialize 可不带 `MCP-Protocol-Version`，按 body 在两个 legacy revision 中协商；存在头则仍须一致。
  后续 legacy HTTP 业务继续要求实际 legacy 版本头；不得借无 session 放宽 stdio 或支持 2025-03-26。
- modern discover 返回真实 supportedVersions/capabilities；成功由库加 resultType（complete 或 MRTR input_required）。
  CacheableResult 固定 `ttlMs=0` / `cacheScope=private`，legacy 无这些现代字段。
  capabilities 基于真实注册，不宣告 tasks/apps/OAuth/proxy/completion/logging；legacy 没有 subscribe/unsubscribe handler，不虚宣 resources.subscribe。
- modern subscriptions/listen 有独立 ack 与 accepted 过滤器，事件带 subscriptionId；资源更新由应用显式 PublishResourceChanged。
  空 accepted 子集仍可 ack；listChanged 不宣告。公开示例无测试发布工具，SDK 对它只验证 listen ack，不伪造事件证明。
- MRTR 仅 modern：inputRequests/inputResponses 为 id-keyed maps，request value 只有 method+params；requestState 是 opaque string。
  必需客户端能力缺失 `-32021` + requiredCapabilities；重试新 id 的数据 owned，不作为授权或可信平台状态，不自动业务重试。
- parse/envelope/params/method/internal：`-32700` / `-32600` / `-32602` / `-32601` / `-32603`。
  unsupported modern：`-32022`，data.requested 保持原 string、supported 只列真实版本；HTTP 400。
  header mismatch `-32020` / 400；HTTP unknown method 404，合法通知 202 空 body。
  请求期限实现使用 `-32000 RequestDeadlineExceeded`（实现自定义，不推断跨实现语义），取消是协作式，超时不证明业务未执行。

## 4. transport、头校验与关闭

- stdio 使用共享 coroutine executor 的 POSIX 非阻塞 descriptor 就绪等待；协议 stdout、诊断 stderr，独占非 socket pipe。
  dup 不隔离 open-file-description 的 O_NONBLOCK，不能声称不影响宿主原 fd flags；关闭走还原/物理 owned fd 回收路径。
- HTTP 默认且只允许 loopback（默认 127.0.0.1），POST `/mcp`；普通结果 JSON，listen 真 SSE，GET/DELETE 405，不 mint/echo session。
  无 auth/TLS、公网明文降级或 Last-Event-ID 重放；带 Origin 时精确白名单，重复/非法/不允许的 Origin 403。
- modern `MCP-Protocol-Version` / `Mcp-Method` 唯一且与 meta/body 一致。
  `Mcp-Name` **仅** tools/call、resources/read、prompts/get 必需唯一，解码后与 name/uri 一致；不适用的方法缺它不能拒绝。
  非 ASCII 原值的 UTF-8 字节用大小写敏感的 `=?base64?<标准 Base64>?=` 编码；仅同时匹配此前缀与 `?=` 后缀的完整 sentinel 才解码，并与 body 精确比较。
  原值自身具有完整 sentinel 形态时也须再编码；prefix-only（如 `=?base64?prefix`）和旧式 `=?B64?…?=` 是普通可打印 ASCII 字面值，不专门拒绝。
  完整 sentinel 的非法编码、control、重复及解码后不一致仍拒绝，不支持的 Mcp-* 注解不假宣支持。
- ponytail: HTTP 一连接一请求、Connection:close，吞吐不等于 keep-alive 后端；只有出现真实吞吐需求才升级连接复用，不另造 transport 平台。
- 完整 POST 后 SHUT_WR 仍可读 JSON/SSE，不因读 EOF 误取消；不完整 body EOF 拒截断。
  确认写失败/reset、Close/deadline 取消；无法立即确认的全关用有限 heartbeat/写期限探测，不承诺 TCP 断连瞬时可知。
- 同会话同 id 在途请求去重并以 `-32600` 拒绝重复准入；跨会话同 id 可共存。`notifications/cancelled` 只命中发起取消的会话自己的订阅与 request owner，未命中或已终态静默丢弃。
  HTTP 每连接独立会话，跨连接帧取消为静默 no-op，不支持 HTTP 帧取消；订阅由确认断连、Close 或期限终止，不增加 session-id 路由平台。
- request deadline 主要在结果提交时检查，handler 可通过 `RequestContext::DeadlineExceeded()` 协作观察；没有期限定时器线程或任意 handler 的定时抢占。过期结果丢弃并返回 `-32000`，不证明 handler 未执行。
- 每请求 owner 持有栈外期限/取消/唯一终态。调用方显式 transport.Close 再 server.Close；物理资源收口与任意业务栈退出分开。
  Close 不 flush、不重开、不强杀 handler，也不依赖 Scheduler Stop；捕获对象须活到回调真实结束。
  LastCloseEvidence/IsClosed 本身不能代替原生 FD/listener/载荷取证。

## 5. 实测与仍未闭合的验收

- 固定官方 Python SDK `4b5fe5afeb5a6391a167d124d8ee0c21a5f2abec`：公开 ON 示例 stdio/HTTP × 三 revision，
  discover/list、真实 tool/resource/prompt 调用、坏输入拒绝且计数不增长，6/6 本地通过。modern listen 仅 ack；实际事件仍由既有业务 fixture 独立证明。
- 固定 JSON-Schema-Test-Suite `8568bcc477cc3aaeed5bc397c9c8648e631317fc`：Boost 产品 46 文件/384 组/1301 例 pass，
  zero skip/error/mismatch；dynamicRef 44/44。运行限制/缺 remote 负例非零退出且 pass=0，未吞失败。
- native std::regex 对照是 1296 pass + 5 编译阻断，不是 native+ICU；Python jsonschema oracle 为 1295 一致、
  5 个 Unicode pattern 不支持、1 个 vocabulary 行为差异。不得称「双模式 1301 全绿」。
- **regex 仍非完整 ECMA-262**：可选 ECMAScript 64/74、non-BMP 9/12；std::regex 对照也有同类 10+3 mismatch。
  同步 ICU regex 仍无已验证抢占/时间预算，真实 UErrorCode 运行故障注入缺失；字节/深度/节点预算不能冒充 regex 时间上限。
  文档边界不解除原 R5/S9–S11 与 2-3 的验收责任，不能宣传完整 draft2020-12 合规。
- 未覆盖所有 raw golden/header/SSE 分支；SDK model_dump 不是 server→client 原始 wire。
  双订阅/取消/关闭局部证据不等于全场景证明；低频退出崩溃根因未明，有限成功退出不排除它。
  重构基线的 `mcp.teardown` 实跑出现 T6 busy HTTP 503/released 两处失败断言，随后在 T8 超时（120 秒）；
  失败来源未核清，保留原期望与未完成验收状态，不能凭基线提交宣称生命周期已闭环。
- 无 Windows/iOS、多平台、压力或全量 TSan 承诺。整包独立审查不补齐未覆盖的验收项，PR/exact-head CI 尚未获得执行/通过证据。
  先解决已列硬缺口再谈完整 ready；本文件、局部独审 PASS 和本地绿测试都不能自审放行。

构建/公开 C++17 用法见 [主 README](../../README.md) 与 [公开示例](../../examples/mcp_server.cc)，
独立 source-build 接入见 [mcp_consumer](../../examples/mcp_consumer/README.md)。不提供 install/export 包或业务 MCP client SDK。
