#pragma once
// MCP 协议基础类型与 wire 常量（公共面）。
//
// 契约来源：WFER mcp-server-foundation revision7（specs R2/R3/R4、design
// 「最小公共契约候选」）。本文件只放自有/标准类型，不出现 Boost/Beast/jsoncons。
//
// 覆盖范围（本包已实现并测试的部分）：JSON-RPC 2.0 单帧输入完整性、owned 请求 id、
// modern per-request `params._meta` 校验、discover/error/result 构造基础，以及
// 2-4 的 resources/prompts 自有原语（ResourceContents/ResourceResult/ResourceDefinition、
// Content/PromptArgument/PromptDefinition/PromptMessage/PromptResult、RequestContext）。
// 2-5 增量：subscriptions/listen（ack/filters/subscriptionId）与 MRTR 基础类型
// （id-keyed inputRequests/inputResponses、opaque string requestState）。
// 未覆盖（未实现，故不在此宣告）：transport、tools 与 Schema adapter、资源模板注册、
// 业务 handler 返回 InputRequired 的接线、deadline、Content 的 image/audio/
// embedded-resource/resource-link kind。

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>

namespace bbt::infra::mcp {

// 本包按 wire 字符串测试的 revision（规范日期字符串，非 SDK 版本）。
// 2026-07-28 是唯一现代 revision（modern 每请求 `_meta` 路径只接受它）；
// 2025-11-25 是 legacy revision，仅出现在对外 supportedVersions
// 与 -32022 的 data.supported，不经现代校验接受（legacy lifecycle 经
// initialize 握手承载）。2025-06-18 按 owner 决策（infra-all-delivery
// fix-mcp-policy-r1）从服务矩阵移除：其 spec crosswalk 未核对，服务它
// 会冒充「已支持的旧版」（规范对不服务版本要求反提议/拒绝而非静默接纳）。
inline constexpr std::string_view kProtocolVersion2026_07_28 = "2026-07-28";
inline constexpr std::string_view kProtocolVersion2025_11_25 = "2025-11-25";

// JSON-RPC 2.0 与 MCP 2026 定义的错误码（规范数值，不自造自定义码）。
enum class JsonRpcErrorCode : int {
    ParseError = -32700,
    InvalidRequest = -32600,
    MethodNotFound = -32601,
    InvalidParams = -32602,
    InternalError = -32603,
    // MCP 2026 定义（-32020/-32021/-32022）。-32000..-32099 属 JSON-RPC
    // implementation-defined server error 区（-32020..-32099 已划为 MCP 规范
    // 保留区）；本版不新增任何该区错误码。
    HeaderMismatch = -32020,
    MissingRequiredClientCapability = -32021,
    UnsupportedProtocolVersion = -32022,
    // 服务端期限拒绝不用独立错误码：architecture §3.3 定 v1 仅以 -32603
    // （InternalError）+ data.kind="deadline_exceeded" 区分，不向
    // -32000..-32099 分配新码（避免与 MCP 保留区冲突）。
};

// 2-5 wire 常量（2026-07-28 固定 schema/mcp_types 2.0.0 逐字）。
inline constexpr std::string_view kMethodSubscriptionsListen = "subscriptions/listen";
inline constexpr std::string_view kNotificationAcknowledged =
    "notifications/subscriptions/acknowledged";
inline constexpr std::string_view kNotificationResourceUpdated =
    "notifications/resources/updated";
inline constexpr std::string_view kNotificationCancelled = "notifications/cancelled";
// 订阅流标识键：ack/事件通知 params._meta 与流结束响应 result._meta 必带。
inline constexpr std::string_view kMetaKeySubscriptionId =
    "io.modelcontextprotocol/subscriptionId";
// subscriptions/listen params 的过滤器键（camelCase）与 MRTR wire 键。
inline constexpr std::string_view kFilterKeyToolsListChanged = "toolsListChanged";
inline constexpr std::string_view kFilterKeyPromptsListChanged = "promptsListChanged";
inline constexpr std::string_view kFilterKeyResourcesListChanged = "resourcesListChanged";
inline constexpr std::string_view kFilterKeyResourceSubscriptions = "resourceSubscriptions";
inline constexpr std::string_view kWireKeyNotifications = "notifications";
inline constexpr std::string_view kWireKeyInputRequests = "inputRequests";
inline constexpr std::string_view kWireKeyInputResponses = "inputResponses";
inline constexpr std::string_view kWireKeyRequestState = "requestState";

// 输入/帧级预算。默认保守（本机、单帧）。
struct ProtocolLimits {
    std::size_t max_message_bytes = 1U << 20; // 1 MiB
    std::size_t max_depth = 32;
};

// 协议错误（wire error 的机器可读形态）：code/message/data。
// message 为固定文本，不回显请求内容；data 仅在规范要求时携带（如 -32022）。
struct ProtocolFailure {
    JsonRpcErrorCode code = JsonRpcErrorCode::InternalError;
    std::string message;
    std::optional<JsonValue> data;
};

// 请求 id：string 或整数（保留精确值）。null / float / 其他类型在输入校验层被拒。
// 本类型自有字符串与整数，不借用被解析帧的内存（帧析构后仍有效）。
class RequestId {
public:
    RequestId() = default;
    explicit RequestId(std::string value) : m_value(std::move(value)) {}
    explicit RequestId(std::int64_t value) : m_value(value) {}
    explicit RequestId(std::uint64_t value) : m_value(value) {}

    bool IsString() const noexcept { return m_value.index() == 0; }
    std::string_view AsString() const { return std::get<0>(m_value); }
    std::int64_t AsInt64() const { return std::get<1>(m_value); }
    std::uint64_t AsUInt64() const { return std::get<2>(m_value); }

    // 原值回包（string 保持 string，整数保持原符号/宽度）。
    JsonValue ToJson() const;

    // 稳定、可比较的键（在途去重/订阅 id 用）；不参与 wire 语义。
    std::string Key() const;

    friend bool operator==(const RequestId& a, const RequestId& b) noexcept {
        return a.m_value == b.m_value;
    }
    friend bool operator!=(const RequestId& a, const RequestId& b) noexcept {
        return !(a == b);
    }

private:
    std::variant<std::string, std::int64_t, std::uint64_t> m_value;
};

// ---- Resources / Prompts 自有原语（R6；不依赖 Schema / transport）----------------
//
// 说明：本切片只实现规范里 **resources/list · resources/read ·
// resources/templates/list · prompts/list · prompts/get** 所需的类型。tools、
// 资源模板注册（ResourceTemplate 的 uriTemplate 匹配）、资源订阅/listChanged、
// MRTR 的 InputRequired 均**未实现**，故不在此提供半成品接口。

// 资源内容二择一（规范 TextResourceContents | BlobResourceContents）。
// 构造即校验，失败不产生对象：
//   - uri 非空且为合法 UTF-8；
//   - mime_type 可为空（规范 optional），非空时须为合法 UTF-8；
//   - Text：text 须为合法 UTF-8；
//   - Blob：blob 须为规范 base64（标准字母表 A-Za-z0-9+/、长度 4 的倍数、仅尾部
//     '=' 填充且最多 2 个），不接受 URL-safe 字母表或空白。
class ResourceContents {
public:
    static result<ResourceContents> Text(std::string uri, std::string mime_type, std::string text);
    static result<ResourceContents> Blob(std::string uri, std::string mime_type, std::string base64);

    const std::string& Uri() const noexcept { return m_uri; }
    const std::string& MimeType() const noexcept { return m_mime_type; }
    bool IsText() const noexcept { return m_is_text; }
    // 仅在 IsText() 为真时有意义（否则为空串）。
    const std::string& Text() const noexcept { return m_body; }
    // 仅在 !IsText() 为真时有意义（否则为空串）。
    const std::string& Blob() const noexcept { return m_body; }

private:
    ResourceContents(std::string uri, std::string mime_type, std::string body, bool is_text)
        : m_uri(std::move(uri)), m_mime_type(std::move(mime_type)), m_body(std::move(body)),
          m_is_text(is_text) {}

    std::string m_uri;
    std::string m_mime_type;
    std::string m_body;
    bool m_is_text = true;
};

// 资源读取结果。库在分派时校验 contents 非空（空 contents 是服务端缺陷 → 固定内部错误）。
struct ResourceResult {
    std::vector<ResourceContents> contents;
};

// 资源注册定义（规范 Resource extends BaseMetadata, Icons）：
//   uri/name 必需且非空；description/mime_type 可选。annotations/size/icons 本切片
//   不支持（不接受则无法忠实构造，故不提供字段，避免静默丢弃业务意图）。
struct ResourceDefinition {
    std::string uri;
    std::string name;
    std::string description;
    std::string mime_type;
};

// 提示词内容块 kind。本切片只实现规范 text kind；image/audio/embedded-resource/
// resource-link 是既定完整 Content 模型的一部分，**未删除**，留待后续切片实现
// （在此不宣告，避免空桩）。
enum class ContentKind { Text };

// 提示词内容块。构造即校验 UTF-8（非法序列抛 std::invalid_argument，与 JsonValue 一致）。
class Content {
public:
    Content() = default;  // 空 text 块
    static Content Text(std::string text);
    ContentKind Kind() const noexcept { return m_kind; }
    const std::string& TextValue() const noexcept { return m_text; }

private:
    ContentKind m_kind = ContentKind::Text;
    std::string m_text;
};

struct PromptArgument {
    std::string name;         // 必需、非空、合法 UTF-8
    std::string description;  // 可选
    bool required = false;
};

struct PromptDefinition {
    std::string name;                       // 必需、非空
    std::string description;                // 可选
    std::vector<PromptArgument> arguments;  // 可选；参数名不得重复
};

// 规范 Role 只有 user / assistant，故用枚举而非自由字符串。
enum class PromptRole { User, Assistant };

struct PromptMessage {
    PromptRole role = PromptRole::User;
    Content content;
};

// 提示词结果。库在分派时校验 messages 非空（空 messages 是服务端缺陷 → 固定内部错误）。
struct PromptResult {
    std::string description;  // 可选
    std::vector<PromptMessage> messages;
};

// 关闭域只读视图（owned、可 copy）：handler 可把它复制到栈外（例如交给工作线程）
// 后继续观察。只由库的 Close 置位，本切片**不提供**取消/超时/deadline API，
// 也不强杀任何 stack——它只让已在执行的回调能协作式退出。
class CancellationView {
public:
    CancellationView() = default;  // 未关联（永不取消）
    explicit CancellationView(std::shared_ptr<const std::atomic<bool>> flag)
        : m_flag(std::move(flag)) {}

    bool Cancelled() const noexcept {
        return m_flag && m_flag->load(std::memory_order_acquire);
    }

private:
    std::shared_ptr<const std::atomic<bool>> m_flag;
};

// MRTR（2026-07-28）服务端→客户端输入请求的 wire value：**只有 method + params**，
// 无内层 id/jsonrpc（嵌套载荷不构成独立 JSON-RPC 帧）。params 缺省为 null 值。
struct ClientInputRequest {
    std::string method;  // sampling/createMessage | elicitation/create | roots/list
    JsonValue params;    // 原样载荷（owned copy）
};

// InputRequired 结果（resultType=input_required 的自有形态）：
//   - inputRequests 是**以服务端分配 id 为 key 的 map**，value 只含 method+params；
//   - requestState 是 opaque string（业务自校验，协议层不做格式解释）；
//   - 两者至少其一非空（规范 MUST），全缺在 Create 即拒。
// 本切片不接线业务 handler 返回本类型（不做 tools/handler 接线）；wire 构造与
// 客户端能力校验由协议层提供并有 contract 检查。
class InputRequired {
public:
    static result<InputRequired> Create(std::map<std::string, ClientInputRequest> input_requests,
                                        std::optional<std::string> request_state);

    const std::map<std::string, ClientInputRequest>& InputRequests() const noexcept {
        return m_input_requests;
    }
    const std::optional<std::string>& RequestState() const noexcept { return m_request_state; }

private:
    InputRequired() = default;
    std::map<std::string, ClientInputRequest> m_input_requests;
    std::optional<std::string> m_request_state;
};

// ---- tools 自有原语（R5；Schema 语义校验经私有 adapter 编译接缝，公共面无第三方类型）
//
// 2-6 增量：tools 注册/调用（tools/list、tools/call）与业务 MRTR 接线：
//   - ToolHandler 返回 HandlerResult<ToolResult>：完成值或 input_required（modern
//     MRTR；wire 构造与客户端能力门禁由库经既有 Protocol 原语完成）；
//   - input_schema 必需（根 type=object），output_schema 可选（声明则校验
//     structuredContent）；Schema 编译校验发生在注册期（fail-closed，一次成型）。

// 工具定义（规范 Tool extends BaseMetadata, Icons）：name 必需非空；title/description
// 可选；input_schema 必需且根须为 object（原始 JsonValue 保存；结构由 adapter 编译
// 校验）；output_schema 可选。annotations/icons 本切片不支持（不提供字段，避免静默
// 丢弃业务意图，与 ResourceDefinition 同口径）。
struct ToolDefinition {
    std::string name;
    std::string title;
    std::string description;
    JsonValue input_schema;
    std::optional<JsonValue> output_schema;
};

// 工具执行结果：content 允许为空数组（协议允许）；is_error 表示业务错误（wire
// isError=true，不构成协议错误）；structured_content 仅在已声明 output_schema 时
// 允许（未声明 → 库按协议结构缺陷固定拒绝，不静默放行未校验输出）。
struct ToolResult {
    std::vector<Content> content;
    std::optional<JsonValue> structured_content;
    bool is_error = false;
};

// 业务 handler 通用结果：完成值（Completed）或 input_required（modern MRTR 专用）。
// legacy 路径收到 InputRequired 由库固定拒绝（不产生 modern 结构）。
template <typename Complete>
class HandlerResult {
public:
    static HandlerResult Completed(Complete value) {
        HandlerResult out;
        out.m_value = std::move(value);
        return out;
    }
    static HandlerResult Deferred(InputRequired required) {
        HandlerResult out;
        out.m_value = std::move(required);
        return out;
    }
    bool IsCompleted() const noexcept { return m_value.index() == 0; }
    bool IsInputRequired() const noexcept { return m_value.index() == 1; }
    const Complete& Value() const { return std::get<0>(m_value); }
    const InputRequired& Required() const { return std::get<1>(m_value); }

private:
    std::variant<Complete, InputRequired> m_value;
};

// 回调上下文：owned（帧析构后仍有效），只读消费。
//
// 本切片真实支撑：本次请求 id（原值 string/integer）、协商/声明的协议版本、
// 客户端 capabilities（modern 取自逐请求 `_meta`；legacy 取自 initialize 的 owned
// 副本）、服务端关闭域只读视图（Cancellation()，真实由 Close 置位）。
// **未实现**故不提供假接口：deadline、input_responses/request_state（MRTR）。
// 不暴露 fd/Beast/jsoncons/coroutine 内部类型。
class RequestContext {
public:
    // 由库构造（业务只读消费，不需要自行构造）。
    RequestContext(RequestId id, std::string protocol_version, JsonValue client_capabilities)
        : RequestContext(std::move(id), std::move(protocol_version),
                         std::move(client_capabilities), CancellationView{}, {}, {}) {}
    RequestContext(RequestId id, std::string protocol_version, JsonValue client_capabilities,
                   CancellationView cancellation)
        : RequestContext(std::move(id), std::move(protocol_version),
                         std::move(client_capabilities), std::move(cancellation), {}, {}) {}
    // 2-5：modern MRTR 重试请求携带的 owned 输入（id-keyed inputResponses 的每项为
    // object 载荷；requestState 为 opaque string 原值回传）。legacy 请求两者恒空。
    RequestContext(RequestId id, std::string protocol_version, JsonValue client_capabilities,
                   CancellationView cancellation,
                   std::map<std::string, JsonValue> input_responses,
                   std::optional<std::string> request_state,
                   std::chrono::steady_clock::time_point deadline = {})
        : m_id(std::move(id)), m_protocol_version(std::move(protocol_version)),
          m_client_capabilities(std::move(client_capabilities)),
          m_cancellation(std::move(cancellation)),
          m_input_responses(std::move(input_responses)), m_request_state(std::move(request_state)),
          m_deadline(deadline) {}

    const RequestId& Id() const noexcept { return m_id; }
    const std::string& ProtocolVersion() const noexcept { return m_protocol_version; }
    const JsonValue& ClientCapabilities() const noexcept { return m_client_capabilities; }
    // 服务端关闭域（Close 置位）；可 copy 到栈外存活。
    const CancellationView& Cancellation() const noexcept { return m_cancellation; }
    // 3-3（R10）：本请求的期限（由栈外 request owner 持有）。epoch = 无期限
    //（request_timeout<=0 的配置不会出现——Create 严格拒绝非正值）。
    bool HasDeadline() const noexcept { return m_deadline.time_since_epoch().count() != 0; }
    std::chrono::steady_clock::time_point Deadline() const noexcept { return m_deadline; }
    // 期限已过（handler 协作式退出观察点之一；终态落定仍在 owner/提交门）。
    bool DeadlineExceeded() const noexcept {
        return HasDeadline() && std::chrono::steady_clock::now() >= m_deadline;
    }
    // MRTR 重试输入（未携带时为空 map / nullopt）。仅 modern 路径填充。
    const std::map<std::string, JsonValue>& InputResponses() const noexcept {
        return m_input_responses;
    }
    const std::optional<std::string>& RequestState() const noexcept { return m_request_state; }

private:
    RequestId m_id;
    std::string m_protocol_version;
    JsonValue m_client_capabilities;
    CancellationView m_cancellation;
    std::map<std::string, JsonValue> m_input_responses;
    std::optional<std::string> m_request_state;
    std::chrono::steady_clock::time_point m_deadline{};
};

// 工具回调：接（已通过 input schema 校验的）owned arguments，返回完成值或
// input_required。库负责 wire 构造/envelope/resultType，回调不可覆盖。
using ToolHandler =
    std::function<result<HandlerResult<ToolResult>>(const RequestContext&, JsonValue arguments)>;

} // namespace bbt::infra::mcp
