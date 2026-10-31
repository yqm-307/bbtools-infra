// MCP transport-independent 核心实现：生命周期 / 有界准入 / 注册快照 / 业务方法分派。
//
// 组织：本 TU 是唯一把「已验收的纯 Protocol 原语」连成可运行 core 行为的地方。
//   - McpServer::Impl：生命周期、注册表（resources/prompts）、modern/legacy 分派；
//   - 公共 McpServer：只做薄转发（不重复 core 逻辑），公共面类型都在 include/。
// 2-5 增量：subscriptions/listen（ack/filters/subscriptionId、长请求名额）、资源发布
// PublishResourceChanged、MRTR 重试输入（id-keyed inputResponses/requestState）与
// 客户端能力门禁原语。不做 Schema 校验（2-3）、不做 transport（3-x）、不接线业务
// handler 返回 InputRequired、不造第二套 JSON DOM、不新增 I/O 线程/io_context。
//
// 分派契约（与固定规范 + 官方 SDK 安装版 mcp 2.0.0 对齐）：
//   - era 路由：params._meta 是 object 且含规范保留键
//     io.modelcontextprotocol/protocolVersion → modern（SDK `_has_modern_envelope`）；
//     否则 legacy。该键只在 2026-07-28+ 信封出现，legacy 流量不产生它。
//   - modern：逐请求独立校验 `_meta`，方法表 = 库内建 server/discover + 业务方法
//     （resources/*、prompts/*）；无握手、无会话状态。legacy 的 initialize/ping 不在
//     现代表 → -32601（方法表隔离 S7）。
//   - legacy：方法表 = initialize/ping（+ notifications/initialized 通知）+ 业务方法；
//     业务方法有初始化门禁（SDK runner.py「handler 存在性优先，未握手 → -32602
//     Invalid request parameters」）；modern 的 server/discover 不在 legacy 表 → -32601。
//   - 合法通知（无 id）永不产生响应；只有无法构成合法请求的帧才回错误（id 仅在
//     已确定时原值，否则 null）。

#include "detail/Server.hpp"

#include <algorithm>
#include <charconv>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <bbt/infra/Result.hpp>

namespace bbt::infra::mcp {

namespace {

// 规范命名空间键：serverInfo（SHOULD）。
constexpr std::string_view kMetaKeyServerInfo = "io.modelcontextprotocol/serverInfo";

// 固定诊断文本：不回显请求输入（脱敏）。SDK runner.py 未握手业务方法用同一 shape。
constexpr const char* kInvalidParamsGeneric = "Invalid request parameters";
constexpr const char* kInternalErrorGeneric = "Internal error";

Error MakeCoreError(ErrorCode code, const char* domain_code, const char* message) {
    Error e;
    e.code = code;
    e.domain_code = domain_code;
    e.message = message;
    return e;
}

ProtocolFailure Failure(JsonRpcErrorCode code, const char* message) {
    ProtocolFailure f;
    f.code = code;
    f.message = message;
    return f;
}

// 核心拒绝（未 Start / 已 Close / 超在途上限）的固定错误：transport 映射 503 一类。
Error ClosedCoreError() {
    return MakeCoreError(ErrorCode::Closed, "mcp.server.closed", "server is closed");
}

// modern `_meta` 信封判定（SDK `_has_modern_envelope` 语义）：仅凭规范保留键
// protocolVersion 的存在即路由 modern；半构造信封仍走 modern 并由校验阶梯给出
// 具名缺键错误，而不是被误判为 legacy。
bool HasModernEnvelope(const std::optional<JsonValue>& params) {
    if (!params || !params->IsObject()) return false;
    auto meta = params->Find("_meta");
    if (!meta || !meta.value().IsObject()) return false;
    return static_cast<bool>(meta.value().Find(kMetaKeyProtocolVersion));
}

DispatchOutcome ErrorOutcome(const std::optional<RequestId>& id, const ProtocolFailure& failure) {
    DispatchOutcome outcome;
    outcome.has_response = true;
    outcome.response = MakeErrorResponse(id, failure).Serialize();
    return outcome;
}

DispatchOutcome SuccessOutcome(const RequestId& id, JsonValue result_value) {
    DispatchOutcome outcome;
    outcome.has_response = true;
    outcome.response = MakeSuccessResponse(id, std::move(result_value)).Serialize();
    return outcome;
}

// modern 成功结果补 `_meta.io.modelcontextprotocol/serverInfo`（规范 SHOULD；SDK 对
// modern 结果 stamp）。已存在则不覆盖（未来业务/调用方写入优先）。legacy 结果不调用。
result<void> StampServerInfo(JsonValue& target, const ServerOptions& options) {
    JsonValue meta = JsonValue::Object();
    auto existing = target.Find("_meta");
    if (existing && existing.value().IsObject()) meta = std::move(existing).value();
    if (meta.Find(kMetaKeyServerInfo)) return result<void>::ok();

    JsonValue info = JsonValue::Object();
    result<void> r = info.Set("name", JsonValue(options.server_name));
    if (!r) return r;
    r = info.Set("version", JsonValue(options.server_version));
    if (!r) return r;
    r = meta.Set(std::string(kMetaKeyServerInfo), std::move(info));
    if (!r) return r;
    return target.Set("_meta", std::move(meta));
}

// 单个 Resource（规范 Resource extends BaseMetadata）：uri/name 必需，其余可选。
// 新建 object + ASCII 常量键 → Set 不可失败；仍显式传播以保持单一错误出口。
result<void> BuildResourceJson(const ResourceDefinition& definition, JsonValue& out) {
    out = JsonValue::Object();
    result<void> r = out.Set("uri", JsonValue(definition.uri));
    if (!r) return r;
    r = out.Set("name", JsonValue(definition.name));
    if (!r) return r;
    if (!definition.description.empty()) {
        r = out.Set("description", JsonValue(definition.description));
        if (!r) return r;
    }
    if (!definition.mime_type.empty()) {
        r = out.Set("mimeType", JsonValue(definition.mime_type));
        if (!r) return r;
    }
    return result<void>::ok();
}

result<void> BuildPromptJson(const PromptDefinition& definition, JsonValue& out) {
    out = JsonValue::Object();
    result<void> r = out.Set("name", JsonValue(definition.name));
    if (!r) return r;
    if (!definition.description.empty()) {
        r = out.Set("description", JsonValue(definition.description));
        if (!r) return r;
    }
    if (!definition.arguments.empty()) {
        JsonValue args = JsonValue::Array();
        for (const PromptArgument& argument : definition.arguments) {
            JsonValue item = JsonValue::Object();
            r = item.Set("name", JsonValue(argument.name));
            if (!r) return r;
            if (!argument.description.empty()) {
                r = item.Set("description", JsonValue(argument.description));
                if (!r) return r;
            }
            // 规范 required 为可选 boolean：仅在 true 时写出，避免无意义噪声字段。
            if (argument.required) {
                r = item.Set("required", JsonValue(true));
                if (!r) return r;
            }
            r = args.Push(std::move(item));
            if (!r) return r;
        }
        r = out.Set("arguments", std::move(args));
        if (!r) return r;
    }
    return result<void>::ok();
}

// 工具定义 → wire Tool（name/title/description/inputSchema[/outputSchema]）。
// 新建 object + ASCII 键 → Set 不可失败；仍显式传播以保持单一错误出口。
result<void> BuildToolsJson(const ToolDefinition& definition, JsonValue& out) {
    out = JsonValue::Object();
    result<void> r = out.Set("name", JsonValue(definition.name));
    if (!r) return r;
    if (!definition.title.empty()) {
        r = out.Set("title", JsonValue(definition.title));
        if (!r) return r;
    }
    if (!definition.description.empty()) {
        r = out.Set("description", JsonValue(definition.description));
        if (!r) return r;
    }
    r = out.Set("inputSchema", definition.input_schema);
    if (!r) return r;
    if (definition.output_schema) {
        r = out.Set("outputSchema", *definition.output_schema);
        if (!r) return r;
    }
    return result<void>::ok();
}

// wire id → owned RequestId（string/integer 原值；其他类型 → nullopt）。
std::optional<RequestId> RequestIdFromJsonValue(const JsonValue& value) {
    if (value.IsString()) {
        auto text = value.AsString();
        if (!text) return std::nullopt;
        return RequestId(std::string(text.value()));
    }
    if (auto signed_value = value.AsInt64()) return RequestId(signed_value.value());
    if (auto unsigned_value = value.AsUInt64()) return RequestId(unsigned_value.value());
    return std::nullopt;
}

// 解析列表分页 cursor。缺省 → 0；非 string / 非纯十进制 / 超 total → InvalidParams。
// 注册表在 Start 后冻结且按 key 有序，故下标 cursor 稳定可复现。
result<std::size_t> ParseCursor(const std::optional<JsonValue>& params, std::size_t total) {
    if (!params) return result<std::size_t>::ok(0);
    if (!params->IsObject())
        return result<std::size_t>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.params",
                                                     "params must be an object"));
    auto cursor = params->Find("cursor");
    if (!cursor) return result<std::size_t>::ok(0);
    auto text = cursor.value().AsString();
    if (!text)
        return result<std::size_t>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.params",
                                                     "cursor must be a string"));
    const std::string& digits = text.value();
    if (digits.empty() || digits.size() > 20)
        return result<std::size_t>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.params",
                                                     "cursor is not a valid position"));
    std::size_t value = 0;
    auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), value);
    if (parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size() ||
        (digits.size() > 1 && digits[0] == '0') || value > total)
        return result<std::size_t>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.params",
                                                     "cursor is not a valid position"));
    return result<std::size_t>::ok(value);
}

} // namespace

// ---- 核心实现 -----------------------------------------------------------------

McpServer::Impl::Impl(ServerOptions options)
    : m_options(std::move(options)),
      m_advertised({std::string(kProtocolVersion2026_07_28),
                    std::string(kProtocolVersion2025_11_25)}) {}

result<void> McpServer::Impl::Start() {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_closed)
        return result<void>::err(
            MakeCoreError(ErrorCode::Closed, "mcp.server.closed", "server is closed"));
    if (m_started)
        return result<void>::err(MakeCoreError(ErrorCode::InvalidContext, "mcp.server.started",
                                               "server already started"));
    {
        // 冻结注册快照：Start 之后注册被拒（快照不可变）。锁序固定为
        // m_mutex → m_registry_mutex（AddResource 同序），不存在反向获取。
        std::lock_guard<std::mutex> registry_lock(m_registry_mutex);
        m_frozen = true;
    }
    m_started = true;
    return result<void>::ok();
}

void McpServer::Impl::Close() noexcept {
    // 先声明后锁：函数返回时先解锁 m_mutex、再析构 drained 里的 Admission
    //（析构只归还名额，不发送任何帧——per-subscription 流结束响应的 wire 发送属
    // transport 面， abrupt close 不携带响应）。
    std::map<std::size_t, std::unique_ptr<SubscriptionEntry>> drained;
    std::lock_guard<std::mutex> lock(m_mutex);
    // 封口：拒绝后续注册与准入。幂等（重复 Close 等价首次）；不重开。
    // 同一 m_mutex 门内同时置位关闭域只读视图（M3）：已进入回调的请求可协作式退出。
    // 本切片无 transport/队列/业务栈资源，故不声称已物理 teardown（属 3-3）。
    m_closed = true;
    m_close_signal->store(true, std::memory_order_release);
    {
        // 固定锁序 m_mutex → m_subscriptions_mutex；swap 无抛出，把订阅条目整体
        // 移出后在锁外析构（Admission 需再取 m_mutex，故不能在锁内析构）。
        std::lock_guard<std::mutex> subscriptions_lock(m_subscriptions_mutex);
        drained.swap(m_subscriptions);
    }
    {
        // 3-3（R10）：翻转全部在途 request owner 为 Closed 终态并清表。abort 位对
        // handler 立即可见（协作式退出）；提交门已封口，晚到完成项只消费空壳。
        // TryFinish 恰一次：并发/重复 Close 下先到者落定，后到者看到空表/已终态。
        // 锁序 m_mutex → m_owners_mutex（与订阅表同序；本表操作不回取 m_mutex）。
        std::lock_guard<std::mutex> owners_lock(m_owners_mutex);
        for (auto& entry : m_request_owners) {
            entry.second->abort->store(true, std::memory_order_release);
            entry.second->TryFinish(RequestOwner::Terminal::kClosed);
        }
        m_request_owners.clear();
    }
}

// M3 callback-start 门：与 Close 在同一 m_mutex 门内定序（本函数即线性化点）。
// 不持锁跨回调：本函数返回后立刻释放 m_mutex，用户回调在锁外执行。
bool McpServer::Impl::EnterCallback() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return !m_closed;
}

// M3 结果提交门：与 Close 同域（同一 m_mutex/m_closed）。封口后不产生任何响应。
bool McpServer::Impl::Sealed() const {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_closed;
}

bool McpServer::Impl::IsStarted() const noexcept {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_started;
}

bool McpServer::Impl::IsClosed() const noexcept {
    std::lock_guard<std::mutex> lock(m_mutex);
    return m_closed;
}

result<void> McpServer::Impl::AddResource(ResourceDefinition definition, ResourceHandler handler) {
    // 全量校验先于任何写入：失败不留半条记录。
    if (!handler)
        return result<void>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.handler",
                                               "resource handler is required"));
    if (definition.uri.empty() || definition.name.empty())
        return result<void>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.definition",
                                               "resource uri and name are required"));
    if (!detail::IsValidUtf8(definition.uri) || !detail::IsValidUtf8(definition.name) ||
        !detail::IsValidUtf8(definition.description) || !detail::IsValidUtf8(definition.mime_type))
        return result<void>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.definition",
                                               "resource text must be valid UTF-8"));

    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_closed)
        return result<void>::err(
            MakeCoreError(ErrorCode::Closed, "mcp.server.closed", "server is closed"));
    if (m_started)
        return result<void>::err(MakeCoreError(ErrorCode::InvalidContext, "mcp.registry.frozen",
                                               "registration snapshot is frozen"));
    std::lock_guard<std::mutex> registry_lock(m_registry_mutex);
    if (m_resources.count(definition.uri) != 0)
        return result<void>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.duplicate",
                                               "resource uri already registered"));
    if (m_resources.size() + m_prompts.size() >= m_options.max_registrations)
        return result<void>::err(MakeCoreError(ErrorCode::Overloaded, "mcp.registry.capacity",
                                               "max registrations reached"));
    ResourceEntry entry;
    entry.definition = std::move(definition);
    entry.handler = std::move(handler);
    m_resources.emplace(entry.definition.uri, std::move(entry));
    return result<void>::ok();
}

result<void> McpServer::Impl::AddPrompt(PromptDefinition definition, PromptHandler handler) {
    if (!handler)
        return result<void>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.handler",
                                               "prompt handler is required"));
    if (definition.name.empty())
        return result<void>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.definition",
                                               "prompt name is required"));
    if (!detail::IsValidUtf8(definition.name) || !detail::IsValidUtf8(definition.description))
        return result<void>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.definition",
                                               "prompt text must be valid UTF-8"));
    for (const PromptArgument& argument : definition.arguments) {
        if (argument.name.empty() || !detail::IsValidUtf8(argument.name) ||
            !detail::IsValidUtf8(argument.description))
            return result<void>::err(
                MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.definition",
                              "prompt argument name must be non-empty valid UTF-8"));
    }
    // 参数名重复：整体拒绝（不做「后者胜」的半份覆盖）。
    for (std::size_t i = 0; i < definition.arguments.size(); ++i)
        for (std::size_t j = i + 1; j < definition.arguments.size(); ++j)
            if (definition.arguments[i].name == definition.arguments[j].name)
                return result<void>::err(
                    MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.definition",
                                  "duplicate prompt argument name"));

    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_closed)
        return result<void>::err(
            MakeCoreError(ErrorCode::Closed, "mcp.server.closed", "server is closed"));
    if (m_started)
        return result<void>::err(MakeCoreError(ErrorCode::InvalidContext, "mcp.registry.frozen",
                                               "registration snapshot is frozen"));
    std::lock_guard<std::mutex> registry_lock(m_registry_mutex);
    if (m_prompts.count(definition.name) != 0)
        return result<void>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.duplicate",
                                               "prompt name already registered"));
    if (m_resources.size() + m_prompts.size() >= m_options.max_registrations)
        return result<void>::err(MakeCoreError(ErrorCode::Overloaded, "mcp.registry.capacity",
                                               "max registrations reached"));
    PromptEntry entry;
    entry.definition = std::move(definition);
    entry.handler = std::move(handler);
    m_prompts.emplace(entry.definition.name, std::move(entry));
    return result<void>::ok();
}

// 2-6：tools 注册。Schema 语义校验（R5/S10）经编译接缝在锁内一次成型：编译失败
// 整体拒绝、不留半条记录；接缝未接通 → InvalidContext（fail-closed，不注册
// 未校验工具）。其余校验与 resources/prompts 同口径（有界池、重复名、UTF-8）。
result<void> McpServer::Impl::AddTool(ToolDefinition definition, ToolHandler handler) {
    if (!handler)
        return result<void>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.handler",
                                               "tool handler is required"));
    if (definition.name.empty())
        return result<void>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.definition",
                                               "tool name is required"));
    if (!detail::IsValidUtf8(definition.name) || !detail::IsValidUtf8(definition.title) ||
        !detail::IsValidUtf8(definition.description))
        return result<void>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.definition",
                                               "tool text must be valid UTF-8"));
    // R5：input_schema 根 type 必须是 string "object"（禁止 {} / 缺 type / 标量或
    // 数组 type）；检查先于任何 mutation，无效注册不消耗名字/容量（事务性）。
    auto input_type = definition.input_schema.Find("type");
    if (!input_type)
        return result<void>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.definition",
                                               "tool input_schema root type must be object"));
    auto input_type_text = input_type.value().AsString();
    if (!input_type_text || input_type_text.value() != "object")
        return result<void>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.definition",
                                               "tool input_schema root type must be object"));
    if (definition.output_schema && !definition.output_schema->IsObject())
        return result<void>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.definition",
                                               "tool output_schema must be an object"));
    // architecture §8.4 [G]：v1 拒绝声明 x-mcp-header 的工具注册（Mcp-Param-*
    // 头↔参数映射未实现）；检查是浅层（顶层 + properties 一级），覆盖标准
    // x-mcp-header 注解位置。深层/nested properties 的 x-mcp-header 不随
    // v1 支持面声称——编译期 schema 已 fail-closed 拒绝未知扩展前的形状。
    {
        auto ext = definition.input_schema.Find("x-mcp-header");
        if (ext)
            return result<void>::err(MakeCoreError(ErrorCode::InvalidArgument,
                                                   "mcp.registry.x_mcp_header",
                                                   "x-mcp-header annotation is not supported in v1"));
        auto props = definition.input_schema.Find("properties");
        if (props && props.value().IsObject()) {
            auto keys = props.value().Keys();
            if (keys) {
                for (const auto& key : keys.value()) {
                    auto prop = props.value().Find(key);
                    if (prop && prop.value().IsObject() &&
                        prop.value().Find("x-mcp-header"))
                        return result<void>::err(MakeCoreError(
                            ErrorCode::InvalidArgument, "mcp.registry.x_mcp_header",
                            "x-mcp-header annotation is not supported in v1"));
                }
            }
        }
    }

    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_closed)
        return result<void>::err(
            MakeCoreError(ErrorCode::Closed, "mcp.server.closed", "server is closed"));
    if (m_started)
        return result<void>::err(MakeCoreError(ErrorCode::InvalidContext, "mcp.registry.frozen",
                                               "registration snapshot is frozen"));
    std::lock_guard<std::mutex> registry_lock(m_registry_mutex);
    if (!m_tools_hooks.set)
        return result<void>::err(MakeCoreError(ErrorCode::InvalidContext, "mcp.tools.schema_disabled",
                                               "tools schema validation is not enabled"));
    if (m_tools.count(definition.name) != 0)
        return result<void>::err(MakeCoreError(ErrorCode::InvalidArgument, "mcp.registry.duplicate",
                                               "tool name already registered"));
    if (m_resources.size() + m_prompts.size() + m_tools.size() >= m_options.max_registrations)
        return result<void>::err(MakeCoreError(ErrorCode::Overloaded, "mcp.registry.capacity",
                                               "max registrations reached"));
    // S10 fail-closed：编译（含官方 metaschema 全文校验 / ref 策略 / 上限）先于任何
    // 写入；hooks 自持 per-tool validator（owned opaque，core 不见 Schema 类型）。
    result<void> compiled = m_tools_hooks.compile(definition);
    if (!compiled) return compiled;
    ToolEntry entry;
    entry.definition = std::move(definition);
    entry.handler = std::move(handler);
    m_tools.emplace(entry.definition.name, std::move(entry));
    return result<void>::ok();
}

JsonValue McpServer::Impl::BuildCapabilities() const {
    // 只宣告真实注册的原语与真实机制：resources/list/read 有显式发布入口 → 宣告
    // resources 原语；legacy resources/subscribe/unsubscribe 无 handler（FamilyOf
    // 不含，SDK 实测 -32601）→ 不宣告 subscribe；listChanged 无发布机制（注册
    // 快照不可变），也不宣告。modern subscriptions/listen 不经本能力位。
    JsonValue capabilities = JsonValue::Object();
    std::lock_guard<std::mutex> registry_lock(m_registry_mutex);
    if (!m_resources.empty()) capabilities.Set("resources", JsonValue::Object());
    if (!m_prompts.empty()) capabilities.Set("prompts", JsonValue::Object());
    if (!m_tools.empty()) capabilities.Set("tools", JsonValue::Object());
    return capabilities;
}

// P4：方法 → 原语族。与 BuildCapabilities 用同一注册事实判定可用性。
McpServer::Impl::MethodFamily McpServer::Impl::FamilyOf(std::string_view method) {
    if (method == "resources/list" || method == "resources/read" ||
        method == "resources/templates/list")
        return MethodFamily::Resources;
    if (method == "prompts/list" || method == "prompts/get") return MethodFamily::Prompts;
    if (method == "tools/list" || method == "tools/call") return MethodFamily::Tools;
    return MethodFamily::None;
}

bool McpServer::Impl::MethodAvailable(std::string_view method) const {
    const MethodFamily family = FamilyOf(method);
    if (family == MethodFamily::None) return false;
    std::lock_guard<std::mutex> registry_lock(m_registry_mutex);
    if (family == MethodFamily::Resources) return !m_resources.empty();
    if (family == MethodFamily::Prompts) return !m_prompts.empty();
    return !m_tools.empty();
}

result<void> McpServer::Impl::Admit() {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_started)
        return result<void>::err(MakeCoreError(ErrorCode::InvalidContext, "mcp.server.not_started",
                                               "server is not started"));
    if (m_closed)
        return result<void>::err(
            MakeCoreError(ErrorCode::Closed, "mcp.server.closed", "server is closed"));
    if (m_inflight >= m_options.max_inflight_requests)
        return result<void>::err(MakeCoreError(ErrorCode::Overloaded, "mcp.server.overloaded",
                                               "max in-flight requests reached"));
    ++m_inflight;
    return result<void>::ok();
}

void McpServer::Impl::Release() noexcept {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_inflight > 0) --m_inflight;
}

namespace {
// I1：帧分类与 Dispatch 同源（ParseRequestFrame 的 notification_recognized：完整
// 基本信封合法且无 id）。DOM 不跨函数复用，Dispatch 内重新解析（解析为纯函数，
// 结果一致）；解析失败的帧按普通 request 准入（与既有 -32700/-32600 路径一致）。
// 3-3：同时带回 detected_id 供 owner 登记（期限自登记起算）。
struct FramePeek {
    bool notification_recognized = false;
    std::optional<RequestId> detected_id;
};
FramePeek PeekFrameClassification(const ServerOptions& options, std::string_view raw) {
    ProtocolLimits limits;
    limits.max_message_bytes = options.max_message_bytes;
    limits.max_depth = options.max_depth;
    FramePeek peek;
    ParseRequestFrame(raw, limits, &peek.detected_id, &peek.notification_recognized);
    return peek;
}
}  // namespace

result<DispatchOutcome> McpSession::HandleFrame(std::string_view raw) {
    // I1：通知类帧不占在途名额——订阅是长请求，max_inflight=1 时订阅占满通道后，其
    // 自身的 notifications/cancelled 若也要过准入将永远被 Overloaded 拒绝（订阅
    // 无法被客户端取消，名额只能靠 Close 释放）。普通 request 的准入点与超限
    // 语义（Overloaded）不变。
    FramePeek peek = PeekFrameClassification(m_core->m_options, raw);
    std::optional<McpServer::Impl::Admission> guard;
    std::shared_ptr<RequestOwner> owner;
    if (!peek.notification_recognized) {
        result<void> admitted = m_core->Admit();
        if (!admitted) return result<DispatchOutcome>::err(admitted.error());
        guard.emplace(*m_core);  // 任何返回路径都归还名额
        // 3-3（R10）：request 栈外 owner 在分派前登记——期限此刻起算，cancel（任意
        // 线程）从登记起即可按会话域命中（S4：不跨会话）。id 不可定的帧 cancel 路由
        // 不了，但期限/封口门仍然适用。通知（含 cancel）自身无 owner、无响应。
        // 3-3 F1（S4 前半句）：同会话同 id 已有在途 owner → 重复帧拒绝（-32600）、
        // handler 不双调用；去重与登记在同一 m_owners_mutex 临界区，无 TOCTOU 窗口。
        owner = m_core->RegisterRequestOwner(
            *this, peek.detected_id ? *peek.detected_id : RequestId(),
            std::chrono::steady_clock::now() + m_core->m_options.request_timeout,
            /*dedup_id=*/static_cast<bool>(peek.detected_id));
        if (!owner) {
            return result<DispatchOutcome>::ok(ErrorOutcome(
                peek.detected_id ? *peek.detected_id : RequestId(),
                Failure(JsonRpcErrorCode::InvalidRequest,
                        "Invalid Request: request id already in flight")));
        }
    }
    result<DispatchOutcome> outcome = m_core->Dispatch(*this, raw, owner);
    // M3 提交门（与 Close 同域，唯一出口）：封口后晚到结果——成功、业务失败、异常、
    // 甚至解析/参数错误——一律不再产生响应。此处覆盖全部返回分支，不靠 finish 成功分支。
    if (outcome && outcome.value().has_response && m_core->Sealed()) {
        owner->TryFinish(RequestOwner::Terminal::kClosed);
        m_core->RetireOwner(owner.get());
        return result<DispatchOutcome>::err(ClosedCoreError());
    }
    // 3-3 终态门（唯一提交出口）：按 owner 终态落定 wire 去留。恰好一次终态；
    // 竞争失败方只消费空壳（S17 晚到 cancel 无害、R10 晚发消息禁止）。
    if (owner) {
        using TerminalKind = RequestOwner::Terminal;
        if (outcome && outcome.value().has_response) {
            // S22：期限已过 → 可观察拒绝（-32603 + data.kind="deadline_exceeded"，
            // architecture §3.3：不向 -32000..-32099 分配新码）。结果本体丢弃
            //（handler 可能已产生副作用，timeout ≠ 未执行）；期限落定与
            // cancel/Close 先到先得。
            if (owner->DeadlinePassed() && owner->TryFinish(TerminalKind::kTimedOut)) {
                m_core->RetireOwner(owner.get());
                JsonValue data = JsonValue::Object();
                data.Set("kind", JsonValue("deadline_exceeded"));
                return result<DispatchOutcome>::ok(ErrorOutcome(
                    owner->request_id,
                    ProtocolFailure{JsonRpcErrorCode::InternalError,
                                    "Request deadline exceeded: result discarded",
                                    std::move(data)}));
            }
            if (owner->TryFinish(TerminalKind::kCompleted)) {
                m_core->RetireOwner(owner.get());
                return outcome;  // 正常提交：恰好一次消息
            }
            m_core->RetireOwner(owner.get());
            return result<DispatchOutcome>::ok(DispatchOutcome{});  // 空壳：不再产生消息
        }
        owner->TryFinish(TerminalKind::kCompleted);  // 无响应分支（通知/核心拒绝）：生命周期落定
        m_core->RetireOwner(owner.get());
    }
    return outcome;
}

result<DispatchOutcome> McpServer::Impl::Dispatch(McpSession& session, std::string_view raw,
                                                  const std::shared_ptr<RequestOwner>& owner) {
    ProtocolLimits limits;
    limits.max_message_bytes = m_options.max_message_bytes;
    limits.max_depth = m_options.max_depth;

    std::optional<RequestId> detected_id;
    bool notification_recognized = false;
    auto parsed = ParseRequestFrame(raw, limits, &detected_id, &notification_recognized);
    if (!parsed) {
        // F2：无 id 且**基本信封合法**的帧（可识别通知）因 params 形状非法而失败：
        // 按 JSON-RPC 2.0 §4.1「通知不得回包」静默丢弃，且不推进任何会话状态。
        if (notification_recognized)
            return result<DispatchOutcome>::ok(DispatchOutcome{});  // has_response = false
        return result<DispatchOutcome>::ok(
            ErrorOutcome(detected_id, ToProtocolFailure(parsed.error())));
    }
    JsonRpcRequest request = std::move(parsed).value();

    if (request.notification) {
        ApplyNotification(session, request);
        return result<DispatchOutcome>::ok(DispatchOutcome{});  // has_response = false
    }
    if (HasModernEnvelope(request.params)) return DispatchModern(session, request, owner);
    return DispatchLegacy(session, request, owner);
}

void McpServer::Impl::ApplyNotification(McpSession& session, const JsonRpcRequest& request) {
    // 2-5 起接入：notifications/cancelled —— 客户端用它终止自己先前发出且仍在途的
    // 请求/订阅。在 era 判定之前处理：cancel 可能带 modern 信封。
    if (request.method == kNotificationCancelled) {
        // 3-3 F1（ADR 0010 决策 18，规格 S4 后半句）：cancel 路由**会话域**——只影响
        // 发起 cancel 的会话自己的在册订阅与在途 owner；跨会话/跨连接 cancel 零命中
        //（HTTP 帧取消不支持，订阅终止由断连驱动 S20）。未命中/已终态仍静默（S17）。
        if (request.params && request.params->IsObject()) {
            if (auto request_id = request.params->Find("requestId")) {
                if (auto id = RequestIdFromJsonValue(request_id.value())) {
                    EndSubscription(session, *id);
                    CancelInflightForSession(session, *id);
                }
            }
        }
        return;
    }
    // modern（2026-07-28）已移除 notifications/initialized；带 modern 信封的通知
    // 不推进 legacy 状态，保证两 era 隔离。
    if (HasModernEnvelope(request.params)) return;
    if (request.method == "notifications/initialized") {
        std::lock_guard<std::mutex> lock(session.m_state_mutex);
        session.m_initialized = true;
    }
    // 其余通知（含未知方法）静默丢弃：通知不得回包。
}

result<DispatchOutcome> McpServer::Impl::DispatchModern(McpSession& session,
                                                        const JsonRpcRequest& request,
                                                        const std::shared_ptr<RequestOwner>& owner) {
    // 逐请求独立校验：modern 路径不读写任何会话状态，故请求之间不可能互相污染。
    std::variant<ModernMeta, ProtocolFailure> meta =
        ValidateModernMeta(request.params, m_advertised);
    if (auto* failure = std::get_if<ProtocolFailure>(&meta))
        return result<DispatchOutcome>::ok(ErrorOutcome(request.id, *failure));
    const ModernMeta& accepted = std::get<ModernMeta>(meta);

    if (request.method == "server/discover") {
        auto discover = MakeDiscoverResult(m_advertised, BuildCapabilities(), m_options.instructions);
        if (!discover) return result<DispatchOutcome>::err(discover.error());
        JsonValue result_value = std::move(discover).value();
        result<void> stamped = StampServerInfo(result_value, m_options);
        if (!stamped) return result<DispatchOutcome>::err(stamped.error());
        return result<DispatchOutcome>::ok(SuccessOutcome(request.id, std::move(result_value)));
    }

    if (request.method == kMethodSubscriptionsListen)
        return HandleSubscriptionsListen(session, request);

    // S15：modern 重试请求的 inputResponses（id-keyed）与 requestState（opaque
    // string）形状校验，owned 交给 handler 上下文；两者皆缺 → 空输入（首次请求）。
    MrtrRetryInputs retry;
    if (request.params) {
        auto parsed_retry = ParseMrtrRetryInputs(*request.params);
        if (auto* failure = std::get_if<ProtocolFailure>(&parsed_retry))
            return result<DispatchOutcome>::ok(ErrorOutcome(request.id, *failure));
        retry = std::move(std::get<MrtrRetryInputs>(parsed_retry));
    }
    // 3-3：协作式退出观察位与期限由本请求 owner 持有（cancel/期限/Close 任一触发
    // 即置位）；owner 为空仅在防御性直调（正常路径 HandleFrame 必给）。
    RequestContext context(request.id, accepted.protocol_version, accepted.client_capabilities,
                           CancellationView(owner ? owner->abort : m_close_signal),
                           std::move(retry.input_responses), std::move(retry.request_state),
                           owner ? owner->deadline : std::chrono::steady_clock::time_point{});
    // P4：modern 与 BuildCapabilities 同一冻结注册事实——未 advertise 能力的业务
    // 方法 → -32601（无 handler 调用，不借空列表假成功）。discover/listen 已在上面
    // 分支处理。
    if (!MethodAvailable(request.method))
        return result<DispatchOutcome>::ok(
            ErrorOutcome(request.id, Failure(JsonRpcErrorCode::MethodNotFound, "Method not found")));
    return DispatchBusiness(context, request, /*modern=*/true);
}

result<DispatchOutcome> McpServer::Impl::DispatchLegacy(McpSession& session,
                                                        const JsonRpcRequest& request,
                                                        const std::shared_ptr<RequestOwner>& owner) const {
    if (request.method == "initialize") return HandleInitialize(session, request);
    if (request.method == "ping") {
        // legacy ping 结果为空 object（EmptyResult）；不写 modern resultType/缓存字段。
        return result<DispatchOutcome>::ok(SuccessOutcome(request.id, JsonValue::Object()));
    }
    // 业务方法：可用性（= 真实注册事实，P4）优先于生命周期门禁（SDK runner.py：
    // 未 advertise/未注册方法一律 -32601，与握手状态无关）；已服务方法未握手 →
    // -32602 固定文本（SDK 同 shape）。
    if (!MethodAvailable(request.method))
        return result<DispatchOutcome>::ok(
            ErrorOutcome(request.id, Failure(JsonRpcErrorCode::MethodNotFound, "Method not found")));
    if (!session.InitializeAccepted())
        return result<DispatchOutcome>::ok(
            ErrorOutcome(request.id, Failure(JsonRpcErrorCode::InvalidParams, kInvalidParamsGeneric)));

    const std::optional<std::string> negotiated = session.NegotiatedVersion();
    // 3-3：legacy 同样接 owner 的协作式退出位与期限（R10 不分 era）。
    RequestContext context(request.id, negotiated.value_or(std::string()), session.ClientCapabilities(),
                           CancellationView(owner ? owner->abort : m_close_signal),
                           {}, {}, owner ? owner->deadline : std::chrono::steady_clock::time_point{});
    return DispatchBusiness(context, request, /*modern=*/false);
}

result<DispatchOutcome> McpServer::Impl::HandleInitialize(McpSession& session,
                                                          const JsonRpcRequest& request) const {
    // legacy initialize params（2025-11-25 InitializeRequestParams）：
    // capabilities(object) / clientInfo(object) / protocolVersion(string) 三者必需。
    auto invalid = [&](const char* detail) {
        return result<DispatchOutcome>::ok(
            ErrorOutcome(request.id, Failure(JsonRpcErrorCode::InvalidParams, detail)));
    };
    if (!request.params || !request.params->IsObject())
        return invalid("Invalid params: initialize params must be an object");
    const JsonValue& params = *request.params;

    auto capabilities = params.Find("capabilities");
    if (!capabilities || !capabilities.value().IsObject())
        return invalid("Invalid params: initialize params.capabilities must be an object");
    auto client_info = params.Find("clientInfo");
    if (!client_info || !client_info.value().IsObject())
        return invalid("Invalid params: initialize params.clientInfo must be an object");
    // F1：固定 schema 的 Implementation 要求 name 与 version 均为必需 string。
    const JsonValue& info = client_info.value();
    auto info_name = info.Find("name");
    if (!info_name || !info_name.value().IsString())
        return invalid("Invalid params: initialize params.clientInfo.name must be a string");
    auto info_version = info.Find("version");
    if (!info_version || !info_version.value().IsString())
        return invalid("Invalid params: initialize params.clientInfo.version must be a string");
    auto version = params.Find("protocolVersion");
    if (!version) return invalid("Invalid params: initialize params.protocolVersion is required");
    auto version_text = version.value().AsString();
    if (!version_text)
        return invalid("Invalid params: initialize params.protocolVersion must be a string");

    const std::string requested = std::move(version_text).value();
    // 版本协商（SDK `_negotiate_initialize`）：客户端提议属于已服务的 legacy
    // revision（2025-11-25）则采纳，否则反提议该 revision。2025-06-18 不在
    // 服务矩阵内（owner 决策：其 spec crosswalk 未核对），按未服务版本反提议。
    std::string negotiated(kProtocolVersion2025_11_25);
    if (requested == kProtocolVersion2025_11_25)
        negotiated = requested;

    // F6：按本次 accepted/negotiated 的 legacy revision 校验客户端 capabilities 已知
    // shape（不支持版本按既定反提议 2025-11-25 后按该 profile 校验）。校验先于
    // 任何状态提交：非法 initialize 不推进/覆盖 accepted/version/caps；错误 id
    // 原值保真。
    auto caps_shape = ValidateClientCapabilities(
        capabilities.value(), CapabilitiesRevision::Legacy2025_11_25);
    if (auto* caps_failure = std::get_if<ProtocolFailure>(&caps_shape))
        return result<DispatchOutcome>::ok(ErrorOutcome(request.id, *caps_failure));

    // F3：状态字段由 m_state_mutex 保护；校验全部在锁外完成，提交在同一短临界区。
    // 客户端 capabilities 取 owned 副本（RequestContext 用），与状态同锁提交。
    {
        std::lock_guard<std::mutex> lock(session.m_state_mutex);
        session.m_initialize_accepted = true;
        session.m_negotiated_version = negotiated;
        session.m_client_capabilities = std::move(std::get<JsonValue>(caps_shape));
    }

    JsonValue initialize_result = JsonValue::Object();
    initialize_result.Set("protocolVersion", JsonValue(negotiated));
    initialize_result.Set("capabilities", BuildCapabilities());  // 只含真实注册
    JsonValue server_info = JsonValue::Object();
    server_info.Set("name", JsonValue(m_options.server_name));
    server_info.Set("version", JsonValue(m_options.server_version));
    initialize_result.Set("serverInfo", std::move(server_info));
    if (m_options.instructions)
        initialize_result.Set("instructions", JsonValue(*m_options.instructions));
    // legacy 结果不写 resultType/ttlMs/cacheScope（modern 专有字段，隔离）。
    return result<DispatchOutcome>::ok(
        SuccessOutcome(request.id, std::move(initialize_result)));
}

result<DispatchOutcome> McpServer::Impl::DispatchBusiness(const RequestContext& context,
                                                         const JsonRpcRequest& request,
                                                         bool modern) const {
    const std::string& method = request.method;
    auto invalid = [&](const char* detail) {
        return result<DispatchOutcome>::ok(
            ErrorOutcome(request.id, Failure(JsonRpcErrorCode::InvalidParams, detail)));
    };
    auto internal = [&]() {
        // 脱敏：不回显异常文本、handler 返回的 domain_code、请求内容或唯一注入值。
        return result<DispatchOutcome>::ok(
            ErrorOutcome(request.id, Failure(JsonRpcErrorCode::InternalError, kInternalErrorGeneric)));
    };
    auto finish = [&](JsonValue result_value, bool cacheable) {
        if (modern) {
            result<void> typed = ApplyModernResultType(result_value);
            if (!typed) return result<DispatchOutcome>::err(typed.error());
            if (cacheable) {
                result<void> cached = ApplyPrivateCacheHints(result_value);
                if (!cached) return result<DispatchOutcome>::err(cached.error());
            }
            result<void> stamped = StampServerInfo(result_value, m_options);
            if (!stamped) return result<DispatchOutcome>::err(stamped.error());
        }
        return result<DispatchOutcome>::ok(SuccessOutcome(request.id, std::move(result_value)));
    };
    // 2-6：MRTR 接线（tools/call 专用）。handler 返回 InputRequired → 既有原语做
    // 客户端能力门禁与 wire 构造；legacy 不产生 modern 结构（固定内部错误）。
    auto finish_deferred = [&](const InputRequired& required) {
        if (!modern) return internal();
        auto caps_failure = CheckInputRequestCapabilities(context.ClientCapabilities(),
                                                          required.InputRequests());
        if (caps_failure)
            return result<DispatchOutcome>::ok(ErrorOutcome(request.id, *caps_failure));
        auto wire = MakeInputRequiredResult(required);
        if (!wire) return result<DispatchOutcome>::err(wire.error());
        return result<DispatchOutcome>::ok(
            SuccessOutcome(request.id, std::move(wire).value()));
    };
    // params 若存在必须是 object（规范所有本切片方法都用 object params）。
    if (request.params && !request.params->IsObject())
        return invalid("Invalid params: params must be an object");

    if (method == "resources/list" || method == "resources/templates/list") {
        // 注册快照在 Start 后不可变 → 有序表给出稳定顺序；cursor 是稳定的下标。
        std::vector<ResourceDefinition> snapshot;
        {
            std::lock_guard<std::mutex> registry_lock(m_registry_mutex);
            if (method == "resources/list")
                for (const auto& item : m_resources) snapshot.push_back(item.second.definition);
        }
        auto start = ParseCursor(request.params, snapshot.size());
        if (!start) return invalid("Invalid params: cursor is not a valid position");
        const std::size_t begin = start.value();
        // 分页上界不溢出：begin ≤ size（ParseCursor 保证），故 size-begin 不回绕；
        // 直接 begin + page_size 在 page_size=SIZE_MAX 时会回绕使 end < begin（cursor 回退）。
        const std::size_t end = begin + std::min(m_options.page_size, snapshot.size() - begin);

        JsonValue items = JsonValue::Array();
        for (std::size_t i = begin; i < end; ++i) {
            JsonValue item;
            result<void> built = BuildResourceJson(snapshot[i], item);
            if (!built) return result<DispatchOutcome>::err(built.error());
            result<void> pushed = items.Push(std::move(item));
            if (!pushed) return result<DispatchOutcome>::err(pushed.error());
        }
        JsonValue result_value = JsonValue::Object();
        const char* key = method == "resources/list" ? "resources" : "resourceTemplates";
        result<void> set = result_value.Set(key, std::move(items));
        if (!set) return result<DispatchOutcome>::err(set.error());
        if (end < snapshot.size())
            result_value.Set("nextCursor", JsonValue(std::to_string(end)));
        return finish(std::move(result_value), /*cacheable=*/true);
    }

    if (method == "resources/read") {
        if (!request.params) return invalid("Invalid params: uri must be a string");
        auto uri = request.params->Find("uri");
        if (!uri || !uri.value().IsString())
            return invalid("Invalid params: uri must be a string");
        const std::string wanted = uri.value().AsString().value();
        ResourceHandler handler;
        {
            std::lock_guard<std::mutex> registry_lock(m_registry_mutex);
            auto found = m_resources.find(wanted);
            if (found == m_resources.end())
                return invalid("Invalid params: unknown resource uri");
            handler = found->second.handler;  // 复制 handler，锁外执行
        }
        // M3 callback-start 门（锁外调用内测同步点后，与 Close 同域定序）。
        if (m_test_pre_callback_hook) m_test_pre_callback_hook();
        if (!EnterCallback()) return result<DispatchOutcome>::err(ClosedCoreError());
        ResourceResult produced;
        try {
            auto called = handler(context, wanted);
            if (!called) return internal();
            produced = std::move(called).value();
        } catch (...) {
            return internal();  // 异常脱敏：固定内部错误，不泄漏异常文本
        }
        if (produced.contents.empty()) return internal();  // 空 contents 被拒（R6/S12）
        JsonValue contents = JsonValue::Array();
        for (const ResourceContents& content : produced.contents) {
            JsonValue item = JsonValue::Object();
            result<void> r = item.Set("uri", JsonValue(content.Uri()));
            if (!r) return result<DispatchOutcome>::err(r.error());
            if (!content.MimeType().empty()) {
                r = item.Set("mimeType", JsonValue(content.MimeType()));
                if (!r) return result<DispatchOutcome>::err(r.error());
            }
            r = item.Set(content.IsText() ? "text" : "blob",
                         JsonValue(content.IsText() ? content.Text() : content.Blob()));
            if (!r) return result<DispatchOutcome>::err(r.error());
            r = contents.Push(std::move(item));
            if (!r) return result<DispatchOutcome>::err(r.error());
        }
        JsonValue result_value = JsonValue::Object();
        result<void> set = result_value.Set("contents", std::move(contents));
        if (!set) return result<DispatchOutcome>::err(set.error());
        return finish(std::move(result_value), /*cacheable=*/true);
    }

    if (method == "prompts/list") {
        std::vector<PromptDefinition> snapshot;
        {
            std::lock_guard<std::mutex> registry_lock(m_registry_mutex);
            for (const auto& item : m_prompts) snapshot.push_back(item.second.definition);
        }
        auto start = ParseCursor(request.params, snapshot.size());
        if (!start) return invalid("Invalid params: cursor is not a valid position");
        const std::size_t begin = start.value();
        // 同 resources/list：先取 min 再相加，page_size=SIZE_MAX 也不回绕。
        const std::size_t end = begin + std::min(m_options.page_size, snapshot.size() - begin);
        JsonValue items = JsonValue::Array();
        for (std::size_t i = begin; i < end; ++i) {
            JsonValue item;
            result<void> built = BuildPromptJson(snapshot[i], item);
            if (!built) return result<DispatchOutcome>::err(built.error());
            result<void> pushed = items.Push(std::move(item));
            if (!pushed) return result<DispatchOutcome>::err(pushed.error());
        }
        JsonValue result_value = JsonValue::Object();
        result<void> set = result_value.Set("prompts", std::move(items));
        if (!set) return result<DispatchOutcome>::err(set.error());
        if (end < snapshot.size())
            result_value.Set("nextCursor", JsonValue(std::to_string(end)));
        return finish(std::move(result_value), /*cacheable=*/true);
    }

    // ---- 2-6：tools/list（cacheable；稳定 cursor 分页复用既有语义）--------------
    if (method == "tools/list") {
        std::vector<ToolDefinition> snapshot;
        {
            std::lock_guard<std::mutex> registry_lock(m_registry_mutex);
            for (const auto& item : m_tools) snapshot.push_back(item.second.definition);
        }
        auto start = ParseCursor(request.params, snapshot.size());
        if (!start) return invalid("Invalid params: cursor is not a valid position");
        const std::size_t begin = start.value();
        const std::size_t end = begin + std::min(m_options.page_size, snapshot.size() - begin);
        JsonValue items = JsonValue::Array();
        for (std::size_t i = begin; i < end; ++i) {
            JsonValue item;
            result<void> built = BuildToolsJson(snapshot[i], item);
            if (!built) return result<DispatchOutcome>::err(built.error());
            result<void> pushed = items.Push(std::move(item));
            if (!pushed) return result<DispatchOutcome>::err(pushed.error());
        }
        JsonValue result_value = JsonValue::Object();
        result<void> set = result_value.Set("tools", std::move(items));
        if (!set) return result<DispatchOutcome>::err(set.error());
        if (end < snapshot.size())
            result_value.Set("nextCursor", JsonValue(std::to_string(end)));
        return finish(std::move(result_value), /*cacheable=*/true);
    }

    // ---- 2-6：tools/call（R5/S9/S10/S11：真实 input/output Schema 校验 + 业务 MRTR）
    if (method == "tools/call") {
        if (!request.params) return invalid("Invalid params: name must be a string");
        auto name = request.params->Find("name");
        if (!name || !name.value().IsString())
            return invalid("Invalid params: name must be a string");
        const std::string wanted = name.value().AsString().value();

        ToolHandler handler;
        bool has_output_schema = false;
        {
            std::lock_guard<std::mutex> registry_lock(m_registry_mutex);
            auto found = m_tools.find(wanted);
            if (found == m_tools.end())
                return invalid("Invalid params: unknown tool name");
            handler = found->second.handler;              // 复制 handler，锁外执行
            has_output_schema = found->second.definition.output_schema.has_value();
        }
        // arguments：缺省为空 object；存在则必须为 object（无隐式转换/删除）。
        JsonValue arguments = JsonValue::Object();
        if (request.params) {
            auto provided = request.params->Find("arguments");
            if (provided) {
                if (!provided.value().IsObject())
                    return invalid("Invalid params: arguments must be an object");
                arguments = provided.value();
            }
        }
        // 输入 Schema 校验（真实编译 validator，Start 前已注册；快照冻结后只读）。
        // 失败 → -32602 固定文本（不回显 issue 细节），handler 计数 0。
        if (m_tools_hooks.set && !m_tools_hooks.validate_input(wanted, arguments))
            return invalid("Invalid params: arguments do not match input schema");

        // M3 callback-start 门（与 Close 同域定序；封口后不进入回调）。
        if (m_test_pre_callback_hook) m_test_pre_callback_hook();
        if (!EnterCallback()) return result<DispatchOutcome>::err(ClosedCoreError());
        try {
            auto called = handler(context, arguments);
            if (!called) return internal();
            HandlerResult<ToolResult> outcome = std::move(called).value();
            if (outcome.IsInputRequired()) {
                // legacy 不产生 modern input_required 结构。
                return finish_deferred(outcome.Required());
            }
            ToolResult produced = outcome.Value();
            // structured_content 只在声明了 output_schema 时允许，且必须通过真实
            // 输出 Schema 校验；违反 → 固定内部错误（S11，不回显细节）。
            if (produced.structured_content) {
                if (!has_output_schema) return internal();
                if (m_tools_hooks.set &&
                    !m_tools_hooks.validate_output(wanted, *produced.structured_content))
                    return internal();
            }
            JsonValue content = JsonValue::Array();
            for (const Content& block : produced.content) {
                JsonValue item = JsonValue::Object();
                result<void> r = item.Set("type", JsonValue(std::string("text")));
                if (!r) return result<DispatchOutcome>::err(r.error());
                r = item.Set("text", JsonValue(block.TextValue()));
                if (!r) return result<DispatchOutcome>::err(r.error());
                r = content.Push(std::move(item));
                if (!r) return result<DispatchOutcome>::err(r.error());
            }
            JsonValue result_value = JsonValue::Object();
            result<void> set = result_value.Set("content", std::move(content));
            if (!set) return result<DispatchOutcome>::err(set.error());
            if (produced.is_error) {
                set = result_value.Set("isError", JsonValue(true));
                if (!set) return result<DispatchOutcome>::err(set.error());
            }
            if (produced.structured_content) {
                set = result_value.Set("structuredContent", *produced.structured_content);
                if (!set) return result<DispatchOutcome>::err(set.error());
            }
            return finish(std::move(result_value), /*cacheable=*/false);
        } catch (...) {
            return internal();  // 异常脱敏：固定内部错误，不泄漏异常文本
        }
    }

    // prompts/get
    if (method == "prompts/get") {
        if (!request.params) return invalid("Invalid params: name must be a string");
        auto name = request.params->Find("name");
        if (!name || !name.value().IsString())
            return invalid("Invalid params: name must be a string");
        const std::string wanted = name.value().AsString().value();

        PromptHandler handler;
        std::vector<PromptArgument> declared;
        {
            std::lock_guard<std::mutex> registry_lock(m_registry_mutex);
            auto found = m_prompts.find(wanted);
            if (found == m_prompts.end())
                return invalid("Invalid params: unknown prompt name");
            handler = found->second.handler;
            declared = found->second.definition.arguments;
        }

        // arguments：可选 object。固定 schema `GetPromptRequestParams.arguments` 为
        // `{ [key: string]: string }` —— **所有**键的值都必须是 string，与是否已声明无关；
        // 未知键的合法 string 值可原样透传给 handler。非 string → -32602（handler 0 次）。
        JsonValue arguments = JsonValue::Object();
        if (request.params) {
            auto provided = request.params->Find("arguments");
            if (provided) {
                if (!provided.value().IsObject())
                    return invalid("Invalid params: arguments must be an object");
                arguments = provided.value();
                auto keys = arguments.Keys();
                if (!keys) return internal();  // 刚校验为 object，正常不可达
                for (const std::string& key : keys.value()) {
                    auto value = arguments.Find(key);
                    if (!value || !value.value().IsString())
                        return invalid("Invalid params: argument must be a string");
                }
            }
        }
        for (const PromptArgument& argument : declared) {
            auto value = arguments.Find(argument.name);
            if (!value) {
                if (argument.required) return invalid("Invalid params: missing required argument");
                continue;
            }
            if (!value.value().IsString())
                return invalid("Invalid params: argument must be a string");
        }

        // M3 callback-start 门（与 Close 同域定序；封口后不进入回调）。
        if (m_test_pre_callback_hook) m_test_pre_callback_hook();
        if (!EnterCallback()) return result<DispatchOutcome>::err(ClosedCoreError());

        PromptResult produced;
        try {
            auto called = handler(context, arguments);
            if (!called) return internal();
            produced = std::move(called).value();
        } catch (...) {
            return internal();
        }
        if (produced.messages.empty()) return internal();

        JsonValue messages = JsonValue::Array();
        for (const PromptMessage& message : produced.messages) {
            JsonValue item = JsonValue::Object();
            result<void> r = item.Set(
                "role", JsonValue(std::string(message.role == PromptRole::User ? "user" : "assistant")));
            if (!r) return result<DispatchOutcome>::err(r.error());
            JsonValue block = JsonValue::Object();
            r = block.Set("type", JsonValue(std::string("text")));
            if (!r) return result<DispatchOutcome>::err(r.error());
            r = block.Set("text", JsonValue(message.content.TextValue()));
            if (!r) return result<DispatchOutcome>::err(r.error());
            r = item.Set("content", std::move(block));
            if (!r) return result<DispatchOutcome>::err(r.error());
            r = messages.Push(std::move(item));
            if (!r) return result<DispatchOutcome>::err(r.error());
        }
        JsonValue result_value = JsonValue::Object();
        result<void> set = result_value.Set("messages", std::move(messages));
        if (!set) return result<DispatchOutcome>::err(set.error());
        if (!produced.description.empty()) {
            // M1：description 是 handler 可自由写入的公共字段；非法 UTF-8 若直接构造
            // JsonValue 会抛 std::invalid_argument 逃出帧入口（uri/mimeType/text/blob 均已在
            // 各自构造期校验，description 是唯一漏网的自由文本）。与「结果由库构造并校验」
            // 同口径：非法 → 固定 -32603，不回显异常文本、不注入请求值。
            if (!detail::IsValidUtf8(produced.description)) return internal();
            set = result_value.Set("description", JsonValue(produced.description));
            if (!set) return result<DispatchOutcome>::err(set.error());
        }
        return finish(std::move(result_value), /*cacheable=*/false);
    }

    return result<DispatchOutcome>::ok(
        ErrorOutcome(request.id, Failure(JsonRpcErrorCode::MethodNotFound, "Method not found")));
}

// ---- 公共 McpServer（薄转发；公共面类型都在 include/）----------------------------

McpServer::McpServer(ServerOptions options) : m_impl(std::make_shared<Impl>(std::move(options))) {}

McpServer::~McpServer() = default;

result<std::shared_ptr<McpServer>> McpServer::Create(ServerOptions options) {
    // 严格检查有限配置：0 上限/分页、空 name/version、非 UTF-8 文本一律拒绝。
    if (options.max_message_bytes == 0 || options.max_depth == 0 ||
        options.max_inflight_requests == 0 || options.max_registrations == 0 ||
        options.page_size == 0 || options.server_name.empty() || options.server_version.empty() ||
        options.request_timeout <= std::chrono::milliseconds{0})
        return result<std::shared_ptr<McpServer>>::err(MakeCoreError(
            ErrorCode::InvalidArgument, "mcp.server.options", "invalid server options"));
    if (!detail::IsValidUtf8(options.server_name) || !detail::IsValidUtf8(options.server_version) ||
        (options.instructions && !detail::IsValidUtf8(*options.instructions)))
        return result<std::shared_ptr<McpServer>>::err(MakeCoreError(
            ErrorCode::InvalidArgument, "mcp.server.options", "server text must be valid UTF-8"));
    return result<std::shared_ptr<McpServer>>::ok(
        std::shared_ptr<McpServer>(new McpServer(std::move(options))));
}

result<void> McpServer::AddResource(ResourceDefinition definition, ResourceHandler handler) {
    return m_impl->AddResource(std::move(definition), std::move(handler));
}

result<void> McpServer::AddPrompt(PromptDefinition definition, PromptHandler handler) {
    return m_impl->AddPrompt(std::move(definition), std::move(handler));
}

result<void> McpServer::AddTool(ToolDefinition definition, ToolHandler handler) {
    return m_impl->AddTool(std::move(definition), std::move(handler));
}

result<void> McpServer::Start() { return m_impl->Start(); }

void McpServer::Close() noexcept { m_impl->Close(); }

bool McpServer::IsStarted() const noexcept { return m_impl->IsStarted(); }

bool McpServer::IsClosed() const noexcept { return m_impl->IsClosed(); }

const ServerOptions& McpServer::Options() const noexcept { return m_impl->m_options; }

const std::vector<std::string>& McpServer::AdvertisedVersions() const noexcept {
    return m_impl->m_advertised;
}

std::shared_ptr<McpSession> CreateSession(const std::shared_ptr<McpServer>& server) {
    return std::shared_ptr<McpSession>(new McpSession(server->m_impl));
}

} // namespace bbt::infra::mcp

namespace bbt::infra::mcp {

// ---- 2-5：subscriptions/listen 与资源发布 -------------------------------------

result<void> McpServer::Impl::AdmitForSubscription() {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (!m_started)
        return result<void>::err(MakeCoreError(ErrorCode::InvalidContext, "mcp.server.not_started",
                                               "server is not started"));
    if (m_closed)
        return result<void>::err(
            MakeCoreError(ErrorCode::Closed, "mcp.server.closed", "server is closed"));
    // 见 Server.hpp 契约：调用方（listen 帧）已持有一个名额并将在 HandleFrame 返回时
    // 释放；此处允许一次同线程瞬时 +1（转移），拒绝面只在 m_inflight 已超过 max 时。
    if (m_inflight > m_options.max_inflight_requests)
        return result<void>::err(MakeCoreError(ErrorCode::Overloaded, "mcp.server.overloaded",
                                               "max in-flight requests reached"));
    ++m_inflight;
    return result<void>::ok();
}

result<DispatchOutcome> McpServer::Impl::HandleSubscriptionsListen(McpSession& session,
                                                                   const JsonRpcRequest& request) {
    auto invalid = [&](const char* detail) {
        return result<DispatchOutcome>::ok(
            ErrorOutcome(request.id, Failure(JsonRpcErrorCode::InvalidParams, detail)));
    };
    if (!request.params || !request.params->IsObject())
        return invalid("Invalid params: notifications filter is required");
    auto notifications = request.params->Find(std::string(kWireKeyNotifications));
    if (!notifications || !notifications.value().IsObject())
        return invalid("Invalid params: notifications filter is required");
    std::variant<SubscriptionFilter, ProtocolFailure> parsed =
        ParseSubscriptionFilter(notifications.value());
    if (auto* failure = std::get_if<ProtocolFailure>(&parsed))
        return result<DispatchOutcome>::ok(ErrorOutcome(request.id, *failure));
    const SubscriptionFilter requested = std::move(std::get<SubscriptionFilter>(parsed));

    // server 实际支持面（ack 只报真接纳的子集；「unsupported types are omitted」）：
    // 注册快照不可变 → 三个 listChanged 均无发布机制，不接纳；resourceSubscriptions
    // 只接纳已注册 uri（未注册 uri 永远不会被发布）。
    SubscriptionFilter accepted;
    {
        std::lock_guard<std::mutex> registry_lock(m_registry_mutex);
        for (const std::string& uri : requested.resource_uris)
            if (m_resources.count(uri) != 0) accepted.resource_uris.push_back(uri);
    }

    // 同一会话重复的活动 listen id 不重复接纳（在途 id 单次接纳语义）。
    {
        std::lock_guard<std::mutex> subscriptions_lock(m_subscriptions_mutex);
        for (const auto& [token, entry] : m_subscriptions) {
            (void)token;
            if (entry->listen_id == request.id && entry->session == &session)
                return invalid("Invalid params: subscription id already in flight");
        }
    }

    // 长请求：转移并占用一个在途名额，直至订阅结束（cancel / Close 释放）。
    result<void> admitted = AdmitForSubscription();
    if (!admitted) return result<DispatchOutcome>::err(admitted.error());

    // 聚合初始化一次性成形（Admission 只可移动构造，不可赋值）。
    auto entry = std::make_unique<SubscriptionEntry>(SubscriptionEntry{
        request.id,
        &session,
        /*tools_list_changed=*/false,  // 无 listChanged 发布机制（不接纳，ack 省略）
        /*prompts_list_changed=*/false,
        /*resources_list_changed=*/false,
        accepted.resource_uris,
        /*ack_sent=*/false,
        Admission(*this)});

    // ack 帧：本订阅的第一条消息（规范 MUST），params._meta 带 subscriptionId。
    JsonValue ack_params = JsonValue::Object();
    result<void> set =
        ack_params.Set(std::string(kWireKeyNotifications), MakeSubscriptionFilterJson(accepted));
    if (!set) return result<DispatchOutcome>::err(set.error());
    auto meta = MakeSubscriptionIdMeta(request.id);
    if (!meta) return result<DispatchOutcome>::err(meta.error());
    set = ack_params.Set("_meta", std::move(meta).value());
    if (!set) return result<DispatchOutcome>::err(set.error());
    JsonValue ack = JsonValue::Object();
    set = ack.Set("jsonrpc", JsonValue("2.0"));
    if (!set) return result<DispatchOutcome>::err(set.error());
    set = ack.Set("method", JsonValue(std::string(kNotificationAcknowledged)));
    if (!set) return result<DispatchOutcome>::err(set.error());
    set = ack.Set("params", std::move(ack_params));
    if (!set) return result<DispatchOutcome>::err(set.error());
    const std::string ack_frame = ack.Serialize();

    std::size_t token = 0;
    {
        std::lock_guard<std::mutex> subscriptions_lock(m_subscriptions_mutex);
        token = m_next_subscription_token++;
        m_subscriptions.emplace(token, std::move(entry));
    }
    // 注册后立即投递 ack（此时发布路径因 ack_sent=false 不会抢先发事件）；sink 在
    // 锁外调用。sink 未挂载（transport seam 未接）→ 帧丢弃，订阅照常成立。
    session.EmitNotificationFrame(ack_frame);
    {
        std::lock_guard<std::mutex> subscriptions_lock(m_subscriptions_mutex);
        // C1：find 语义——ack 投递（锁外）期间条目可能已被并发 EndSubscription/
        // Close 移除。operator[] 会在键缺失时把 null unique_ptr 插回表内（破坏
        // 「表内条目永不为 null」不变量，下一次 EndSubscription/
        // PublishResourceChanged 遍历解引用即崩；复审探针 cancel-race 6/6 SIGSEGV
        // 实证）。条目已逝 → 订阅已结束，不置位、不做事。
        auto stored = m_subscriptions.find(token);
        if (stored != m_subscriptions.end()) stored->second->ack_sent = true;
    }
    // 长请求无即时 JSON-RPC 响应（ack 是通知；完成响应只在流结束时经 sink 发出）。
    return result<DispatchOutcome>::ok(DispatchOutcome{});
}

namespace {

// 流结束响应帧：resultType=complete + result._meta.subscriptionId = listen 请求 id
//（与响应 id 同值，规范 SubscriptionsListenResultMeta 必带）。构造失败返回空。
// EndSubscription（cancel 路径）与 McpSession::EndAllSubscriptions（transport 收口
// 路径）共用同一 wire 形状，避免双份漂移。
std::optional<std::string> MakeSubscriptionFlowEndFrame(const RequestId& listen_id) {
    JsonValue result_value = JsonValue::Object();
    if (!result_value.Set("resultType", JsonValue("complete"))) return std::nullopt;
    auto meta = MakeSubscriptionIdMeta(listen_id);
    if (!meta) return std::nullopt;
    if (!result_value.Set("_meta", std::move(meta).value())) return std::nullopt;
    return MakeSuccessResponse(listen_id, std::move(result_value)).Serialize();
}

} // namespace

void McpServer::Impl::EndSubscription(const McpSession& session, const RequestId& listen_id) {
    // 3-3 F1：恢复会话域匹配（早版全局匹配为补「HTTP 跨连接帧取消」缺口，与规格
    // S4「不跨连接误取消别人的请求」冲突；HTTP 订阅终止回归断连驱动 S20）。
    std::unique_ptr<SubscriptionEntry> entry;
    {
        std::lock_guard<std::mutex> subscriptions_lock(m_subscriptions_mutex);
        for (auto it = m_subscriptions.begin(); it != m_subscriptions.end(); ++it) {
            if (it->second->session == &session && it->second->listen_id == listen_id) {
                entry = std::move(it->second);
                m_subscriptions.erase(it);
                break;
            }
        }
    }
    if (!entry) return;  // 非在册订阅 id 的 cancel：静默丢弃（晚到/未知均无害）
    auto frame = MakeSubscriptionFlowEndFrame(entry->listen_id);
    if (!frame) return;
    McpSession::NotificationSink sink;
    if (entry->session && entry->session->CopyNotificationSink(sink)) sink(*frame);
    // entry 析构 → Admission 归还长请求在途名额（此处不持任何锁）。
}

// ---- 3-3：request 栈外 owner 的登记/退休与 cancel 会话域路由 ----------------------

std::shared_ptr<RequestOwner> McpServer::Impl::RegisterRequestOwner(
    const McpSession& session, const RequestId& id,
    std::chrono::steady_clock::time_point deadline, bool dedup_id) {
    auto owner = std::make_shared<RequestOwner>(id);
    owner->deadline = deadline;
    owner->session = &session;  // F1：会话归属（cancel 会话域路由的判据）
    std::lock_guard<std::mutex> lock(m_owners_mutex);
    if (dedup_id) {
        // S4 前半句：同会话同 id 在途只接纳一次。去重与登记同一临界区（原子准入，
        // 并发重复帧恰有一个胜出）；跨会话同 id 不受影响（合法常态，multimap 容纳）。
        auto range = m_request_owners.equal_range(id.Key());
        for (auto it = range.first; it != range.second; ++it)
            if (it->second->session == &session) return nullptr;
    }
    m_request_owners.emplace(id.Key(), owner);
    return owner;
}

void McpServer::Impl::RetireOwner(const RequestOwner* owner) {
    if (!owner) return;
    std::lock_guard<std::mutex> lock(m_owners_mutex);
    auto range = m_request_owners.equal_range(owner->request_id.Key());
    for (auto it = range.first; it != range.second; ++it) {
        if (it->second.get() == owner) {
            m_request_owners.erase(it);
            return;
        }
    }
}

void McpServer::Impl::CancelInflightForSession(const McpSession& session, const RequestId& id) {
    // F1：只命中本会话登记的同 id owner（S4 后半句）；跨会话同 id 条目原样保留。
    // 表移除在锁内一次完成；abort 置位与终态 CAS 在锁外（owner 已被本调用独占持有，
    // 提交门只经 shared_ptr 消费空壳，无表内竞争）。
    std::vector<std::shared_ptr<RequestOwner>> victims;
    {
        std::lock_guard<std::mutex> lock(m_owners_mutex);
        auto range = m_request_owners.equal_range(id.Key());
        for (auto it = range.first; it != range.second;) {
            if (it->second->session == &session) {
                victims.push_back(it->second);
                it = m_request_owners.erase(it);
            } else {
                ++it;
            }
        }
    }
    for (auto& victim : victims) {
        victim->abort->store(true, std::memory_order_release);  // 协作式退出位立即可见
        victim->TryFinish(RequestOwner::Terminal::kCancelled);  // 恰一次；竞争失败=空壳
    }
}

// 3-1 transport teardown seam：结束本会话全部在册订阅并经 sink 投递流结束帧。
// 条目移除与 EndSubscription 同锁域（表内条目永不为 null 的 C1 不变量不受影响）；
// sink 调用在锁外逐条进行，帧构造失败跳过该条（与 cancel 路径同口径）。
void McpSession::EndAllSubscriptions() {
    std::vector<std::unique_ptr<McpServer::Impl::SubscriptionEntry>> entries;
    {
        std::lock_guard<std::mutex> subscriptions_lock(m_core->m_subscriptions_mutex);
        for (auto it = m_core->m_subscriptions.begin();
             it != m_core->m_subscriptions.end();) {
            if (it->second->session == this) {
                entries.push_back(std::move(it->second));
                it = m_core->m_subscriptions.erase(it);
            } else {
                ++it;
            }
        }
    }
    for (auto& entry : entries) {
        auto frame = MakeSubscriptionFlowEndFrame(entry->listen_id);
        if (!frame) continue;
        McpSession::NotificationSink sink;
        if (entry->session && entry->session->CopyNotificationSink(sink)) sink(*frame);
        // entry 析构 → Admission 归还长请求在途名额（此处不持任何锁）。
    }
}

result<std::size_t> McpServer::Impl::PublishResourceChanged(const std::string& uri) {
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        if (!m_started)
            return result<std::size_t>::err(MakeCoreError(ErrorCode::InvalidContext,
                                                          "mcp.server.not_started",
                                                          "server is not started"));
        if (m_closed)
            return result<std::size_t>::err(
                MakeCoreError(ErrorCode::Closed, "mcp.server.closed", "server is closed"));
    }
    {
        std::lock_guard<std::mutex> registry_lock(m_registry_mutex);
        if (m_resources.count(uri) == 0)
            return result<std::size_t>::err(MakeCoreError(ErrorCode::InvalidArgument,
                                                          "mcp.publish.unknown_uri",
                                                          "resource uri is not registered"));
    }
    struct Target {
        McpSession::NotificationSink sink;
        std::string frame;
    };
    std::vector<Target> targets;
    {
        std::lock_guard<std::mutex> subscriptions_lock(m_subscriptions_mutex);
        for (auto& [token, entry] : m_subscriptions) {
            (void)token;
            if (!entry->ack_sent) continue;  // ack 先于任何事件
            if (std::find(entry->resource_uris.begin(), entry->resource_uris.end(), uri) ==
                entry->resource_uris.end())
                continue;  // 过滤器未接纳该 uri
            McpSession::NotificationSink sink;
            if (!entry->session || !entry->session->CopyNotificationSink(sink)) continue;
            JsonValue params = JsonValue::Object();
            result<void> set = params.Set("uri", JsonValue(uri));
            if (!set) return result<std::size_t>::err(set.error());
            auto meta = MakeSubscriptionIdMeta(entry->listen_id);
            if (!meta) return result<std::size_t>::err(meta.error());
            set = params.Set("_meta", std::move(meta).value());
            if (!set) return result<std::size_t>::err(set.error());
            JsonValue notification = JsonValue::Object();
            set = notification.Set("jsonrpc", JsonValue("2.0"));
            if (!set) return result<std::size_t>::err(set.error());
            set = notification.Set("method", JsonValue(std::string(kNotificationResourceUpdated)));
            if (!set) return result<std::size_t>::err(set.error());
            set = notification.Set("params", std::move(params));
            if (!set) return result<std::size_t>::err(set.error());
            targets.push_back(Target{std::move(sink), notification.Serialize()});
        }
    }
    // 锁外投递（token 顺序 → 确定性事件顺序）。
    for (Target& target : targets) target.sink(target.frame);
    return result<std::size_t>::ok(targets.size());
}

// 公共面薄转发。
result<std::size_t> McpServer::PublishResourceChanged(const std::string& uri) {
    return m_impl->PublishResourceChanged(uri);
}

} // namespace bbt::infra::mcp
