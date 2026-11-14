#pragma once

/**
 * @file McpTypes.hpp
 * @brief MCP 协议基础类型与 wire 常量（公共面）。
 *
 * 契约来源：官方 MCP 规范 2026-07-28；逐条核对见
 * docs/decisions/0013-mcp-streamable-http-crosswalk.md。本文件只放自有/标准
 * 类型，不出现 Boost/Beast/jsoncons。
 *
 * 覆盖范围（已实现的部分）：JSON-RPC 2.0 单帧输入完整性、owned 请求 id、
 * modern per-request `params._meta` 校验、discover/error/result 构造基础，
 * resources/prompts 自有原语（ResourceContents/ResourceResult/
 * ResourceDefinition、Content/PromptArgument/PromptDefinition/PromptMessage/
 * PromptResult、RequestContext），subscriptions/listen（ack/filters/
 * subscriptionId）与 MRTR 基础类型（id-keyed inputRequests/inputResponses、
 * opaque string requestState），以及 tools 注册/调用类型。
 *
 * 未覆盖（未实现，故不在此宣告）：transport、Schema adapter 的第三方类型、
 * 资源模板注册、Content 的 image/audio/embedded-resource/resource-link kind。
 */
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

/**
 * @brief 本包按 wire 字符串测试的协议 revision（规范日期字符串，非 SDK 版本）。
 *
 * 2026-07-28 是唯一现代 revision（modern 每请求 `_meta` 路径只接受它）；
 * 2025-11-25 是 legacy revision，仅出现在对外 supportedVersions 与 -32022 的
 * data.supported，不经现代校验接受（legacy lifecycle 经 initialize 握手承载）。
 * 2025-06-18 不在服务矩阵：其 spec crosswalk 未核对，服务它会冒充「已支持的
 * 旧版」（规范要求对不服务版本反提议/拒绝而非静默接纳）。
 */
inline constexpr std::string_view kProtocolVersion2026_07_28 = "2026-07-28";

/**
 * @brief legacy revision：2025-11-25 只经 initialize 握手承载，并作为对外声明值回显。
 */
inline constexpr std::string_view kProtocolVersion2025_11_25 = "2025-11-25";

/**
 * @brief JSON-RPC 2.0 与 MCP 2026 定义的错误码（规范数值，不自造自定义码）。
 *
 * -32000..-32099 属 JSON-RPC implementation-defined server error 区
 * （-32020..-32099 已划为 MCP 规范保留区）；本版不新增任何该区错误码。服务端
 * 期限拒绝不用独立错误码：仅以 -32603（InternalError）+
 * data.kind="deadline_exceeded" 区分，避免与 MCP 保留区冲突。
 */
enum class JsonRpcErrorCode : int {
    ParseError = -32700,
    InvalidRequest = -32600,
    MethodNotFound = -32601,
    InvalidParams = -32602,
    InternalError = -32603,
    HeaderMismatch = -32020,
    MissingRequiredClientCapability = -32021,
    UnsupportedProtocolVersion = -32022,
};

/**
 * subscriptions/listen 的方法名（2026-07-28 固定 schema/mcp_types 2.0.0 逐字）。
 */
inline constexpr std::string_view kMethodSubscriptionsListen = "subscriptions/listen";

/**
 * 订阅确认通知方法名。
 */
inline constexpr std::string_view kNotificationAcknowledged = "notifications/subscriptions/acknowledged";

/**
 * 资源更新通知方法名。
 */
inline constexpr std::string_view kNotificationResourceUpdated = "notifications/resources/updated";

/**
 * 取消通知方法名。
 */
inline constexpr std::string_view kNotificationCancelled = "notifications/cancelled";

/**
 * 订阅流标识键：ack/事件通知 params._meta 与流结束响应 result._meta 必带。
 */
inline constexpr std::string_view kMetaKeySubscriptionId = "io.modelcontextprotocol/subscriptionId";

/**
 * subscriptions/listen 的 tools 列表变更过滤器键。
 */
inline constexpr std::string_view kFilterKeyToolsListChanged = "toolsListChanged";

/**
 * subscriptions/listen 的 prompts 列表变更过滤器键。
 */
inline constexpr std::string_view kFilterKeyPromptsListChanged = "promptsListChanged";

/**
 * subscriptions/listen 的 resources 列表变更过滤器键。
 */
inline constexpr std::string_view kFilterKeyResourcesListChanged = "resourcesListChanged";

/**
 * subscriptions/listen 的资源订阅过滤器键。
 */
inline constexpr std::string_view kFilterKeyResourceSubscriptions = "resourceSubscriptions";

/**
 * MRTR wire 键：通知列表。
 */
inline constexpr std::string_view kWireKeyNotifications = "notifications";

/**
 * MRTR wire 键：服务端输入请求 map。
 */
inline constexpr std::string_view kWireKeyInputRequests = "inputRequests";

/**
 * MRTR wire 键：客户端输入响应 map。
 */
inline constexpr std::string_view kWireKeyInputResponses = "inputResponses";

/**
 * MRTR wire 键：opaque 请求状态字符串。
 */
inline constexpr std::string_view kWireKeyRequestState = "requestState";

/**
 * @brief 输入/帧级预算。默认保守（本机、单帧）。
 */
struct ProtocolLimits {

    /**
     * 单帧消息字节上限（1 MiB）
     */
    std::size_t max_message_bytes = 1U << 20;

    /**
     * JSON 嵌套深度上限
     */
    std::size_t max_depth = 32;
};

/**
 * @brief 协议错误（wire error 的机器可读形态）：code/message/data。
 *
 * message 为固定文本，不回显请求内容；data 仅在规范要求时携带（如 -32022）。
 */
struct ProtocolFailure {

    /**
     * 规范错误码
     */
    JsonRpcErrorCode code = JsonRpcErrorCode::InternalError;

    /**
     * 固定文本，不回显请求内容
     */
    std::string message;

    /**
     * 规范要求时携带的附加数据
     */
    std::optional<JsonValue> data;
};

/**
 * @brief 请求 id：string 或整数（保留精确值）。
 *
 * null / float / 其他类型在输入校验层被拒。本类型自有字符串与整数，不借用
 * 被解析帧的内存（帧析构后仍有效）。
 */
class RequestId {
public:
    RequestId() = default;

    /**
     * @brief 以字符串形请求 id 构造。
     *
     * @param value 原值（owned）
     */
    explicit RequestId(std::string value)
        : m_value(std::move(value))
    {
    }

    /**
     * @brief 以有符号整数请求 id 构造。
     *
     * @param value 原值
     */
    explicit RequestId(std::int64_t value)
        : m_value(value)
    {
    }

    /**
     * @brief 以无符号整数请求 id 构造。
     *
     * @param value 原值
     */
    explicit RequestId(std::uint64_t value)
        : m_value(value)
    {
    }

    /**
     * @brief 是否为字符串形态。
     */
    bool IsString() const noexcept { return m_value.index() == 0; }

    /**
     * @brief 字符串形态的原值（未持有字符串时为空串）。
     */
    std::string_view AsString() const { return std::get<0>(m_value); }

    /**
     * @brief 有符号整数原值。
     */
    std::int64_t AsInt64() const { return std::get<1>(m_value); }

    /**
     * @brief 无符号整数原值。
     */
    std::uint64_t AsUInt64() const { return std::get<2>(m_value); }

    /**
     * @brief 原值回包（string 保持 string，整数保持原符号/宽度）。
     */
    JsonValue ToJson() const;

    /**
     * @brief 稳定、可比较的键（在途去重/订阅 id 用）；不参与 wire 语义。
     */
    std::string Key() const;

    /**
     * @brief 按存储形态与原值比较（string 与 integer 永不相等）。
     */
    friend bool operator==(const RequestId& a, const RequestId& b) noexcept { return a.m_value == b.m_value; }

    /**
     * @brief operator== 的取反。
     */
    friend bool operator!=(const RequestId& a, const RequestId& b) noexcept { return !(a == b); }

private:
    std::variant<std::string, std::int64_t, std::uint64_t> m_value;
};

/**
 * @name Resources / Prompts 自有原语（不依赖 Schema / transport）
 * @brief 只实现规范里 resources/list · resources/read · resources/templates/list ·
 *        prompts/list · prompts/get 所需的类型。
 *
 * 资源模板注册（ResourceTemplate 的 uriTemplate 匹配）、资源订阅/listChanged、MRTR 的
 * InputRequired 接线均未实现，故不在此提供半成品接口；tools 见下文。
 * @{
 */

/**
 * @brief 资源内容二择一（规范 TextResourceContents | BlobResourceContents）。
 *
 * 构造即校验，失败不产生对象：
 *   - uri 非空且为合法 UTF-8；
 *   - mime_type 可为空（规范 optional），非空时须为合法 UTF-8；
 *   - Text：text 须为合法 UTF-8；
 *   - Blob：blob 须为规范 base64（标准字母表 A-Za-z0-9+/、长度 4 的倍数、仅
 *     尾部 '=' 填充且最多 2 个），不接受 URL-safe 字母表或空白。
 */
class ResourceContents {
public:
    /**
     * @brief 构造文本资源内容。
     *
     * @param uri 资源 URI（非空、合法 UTF-8）
     * @param mime_type MIME 类型（可空）
     * @param text 文本内容（合法 UTF-8）
     * @return 校验失败时为 InvalidArgument
     */
    static result<ResourceContents> Text(std::string uri, std::string mime_type, std::string text);

    /**
     * @brief 构造二进制资源内容。
     *
     * @param uri 资源 URI（非空、合法 UTF-8）
     * @param mime_type MIME 类型（可空）
     * @param base64 规范 base64 编码的载荷
     * @return 校验失败时为 InvalidArgument
     */
    static result<ResourceContents> Blob(std::string uri, std::string mime_type, std::string base64);

    /**
     * @brief 资源 URI。
     */
    const std::string& Uri() const noexcept { return m_uri; }

    /**
     * @brief MIME 类型（可为空）。
     */
    const std::string& MimeType() const noexcept { return m_mime_type; }

    /**
     * @brief 文本形态为 true，二进制形态为 false。
     */
    bool IsText() const noexcept { return m_is_text; }

    /**
     * @brief 文本内容；仅在 IsText() 为真时有意义（否则为空串）。
     */
    const std::string& Text() const noexcept { return m_body; }

    /**
     * @brief base64 载荷；仅在 !IsText() 为真时有意义（否则为空串）。
     */
    const std::string& Blob() const noexcept { return m_body; }

private:
    ResourceContents(std::string uri, std::string mime_type, std::string body, bool is_text)
        : m_uri(std::move(uri))
        , m_mime_type(std::move(mime_type))
        , m_body(std::move(body))
        , m_is_text(is_text)
    {
    }

    std::string m_uri;
    std::string m_mime_type;
    std::string m_body;
    bool m_is_text = true;
};

/**
 * @brief 资源读取结果：非空资源内容列表。
 *
 * 库在分派时校验 contents 非空（空 contents 是服务端缺陷 → 固定内部错误）。
 */
struct ResourceResult {

    /**
     * 资源内容列表（不得为空）
     */
    std::vector<ResourceContents> contents;
};

/**
 * @brief 资源注册定义（规范 Resource extends BaseMetadata, Icons）。
 *
 * uri/name 必需且非空；description/mime_type 可选。annotations/size/icons
 * 不支持（不接受则无法忠实构造，故不提供字段，避免静默丢弃业务意图）。
 */
struct ResourceDefinition {

    /**
     * 资源 URI（必需、非空）
     */
    std::string uri;

    /**
     * 资源名（必需、非空）
     */
    std::string name;

    /**
     * 描述（可选）
     */
    std::string description;

    /**
     * MIME 类型（可选）
     */
    std::string mime_type;
};

/**
 * @brief 提示词内容块 kind。只实现规范 text kind；image/audio/embedded-resource/
 * resource-link 是完整 Content 模型的一部分，留待后续实现（在此不宣告，
 * 避免空桩）。
 */
enum class ContentKind { Text };

/**
 * @brief 提示词内容块。
 *
 * 构造即校验 UTF-8（非法序列抛 std::invalid_argument，与 JsonValue 一致）。
 */
class Content {
public:
    /**
     * @brief 空 text 块
     */
    Content() = default;

    /**
     * @brief 构造文本内容块。
     *
     * @param text 文本（合法 UTF-8）
     * @return 内容块；非法 UTF-8 抛 std::invalid_argument
     */
    static Content Text(std::string text);

    /**
     * @brief 内容块 kind。
     */
    ContentKind Kind() const noexcept { return m_kind; }

    /**
     * @brief 文本值（kind 为 Text 时有效）。
     */
    const std::string& TextValue() const noexcept { return m_text; }

private:
    ContentKind m_kind = ContentKind::Text;
    std::string m_text;
};

/**
 * @brief 提示词参数声明。
 */
struct PromptArgument {

    /**
     * 参数名（必需、非空、合法 UTF-8）
     */
    std::string name;

    /**
     * 描述（可选）
     */
    std::string description;

    /**
     * 是否必填
     */
    bool required = false;
};

/**
 * @brief 提示词注册定义。
 */
struct PromptDefinition {

    /**
     * 提示词名（必需、非空）
     */
    std::string name;

    /**
     * 描述（可选）
     */
    std::string description;

    /**
     * 参数声明（可选；参数名不得重复）
     */
    std::vector<PromptArgument> arguments;
};

/**
 * @brief 规范 Role 只有 user / assistant，故用枚举而非自由字符串。
 */
enum class PromptRole { User, Assistant };

/**
 * @brief 一条提示词消息。
 */
struct PromptMessage {

    /**
     * 发言人角色
     */
    PromptRole role = PromptRole::User;

    /**
     * 内容块
     */
    Content content;
};

/**
 * @brief 提示词结果：可选描述 + 非空消息列表。
 *
 * 库在分派时校验 messages 非空（空 messages 是服务端缺陷 → 固定内部错误）。
 */
struct PromptResult {

    /**
     * 描述（可选）
     */
    std::string description;

    /**
     * 消息列表（不得为空）
     */
    std::vector<PromptMessage> messages;
};

/**
 * @brief 关闭域只读视图（owned、可 copy）。
 *
 * handler 可把它复制到栈外（例如交给工作线程）后继续观察。只由库的 Close
 * 置位，不提供取消/超时/deadline API，也不强杀任何 stack——它只让已在执行的
 * 回调能协作式退出。
 */
class CancellationView {
public:
    /**
     * @brief 未关联（永不取消）。
     */
    CancellationView() = default;

    /**
     * @brief 关联到库的关闭标志。
     *
     * @param flag 关闭标志（库内部持有，视图只共享只读引用）
     */
    explicit CancellationView(std::shared_ptr<const std::atomic<bool>> flag)
        : m_flag(std::move(flag))
    {
    }

    /**
     * @brief 库是否已发出关闭信号。
     */
    bool Cancelled() const noexcept;

private:
    std::shared_ptr<const std::atomic<bool>> m_flag;
};

/**
 * @brief MRTR（2026-07-28）服务端→客户端输入请求的 wire value。
 *
 * 只有 method + params，无内层 id/jsonrpc（嵌套载荷不构成独立 JSON-RPC 帧）。
 * params 缺省为 null 值。
 */
struct ClientInputRequest {

    /**
     * sampling/createMessage | elicitation/create | roots/list
     */
    std::string method;

    /**
     * 原样载荷（owned copy）
     */
    JsonValue params;
};

/**
 * @brief InputRequired 结果（resultType=input_required 的自有形态）。
 *
 *   - inputRequests 是以服务端分配 id 为 key 的 map，value 只含 method+params；
 *   - requestState 是 opaque string（业务自校验，协议层不做格式解释）；
 *   - 两者至少其一非空（规范 MUST），全缺在 Create 即拒。
 *
 * 业务 handler 返回本类型的接线不在本类型内；wire 构造与客户端能力校验由协议
 * 层提供并有 contract 检查。
 */
class InputRequired {
public:
    /**
     * @brief 构造 InputRequired 结果。
     *
     * @param input_requests id-keyed 输入请求 map
     * @param request_state opaque 状态字符串
     * @return 两者皆空时为 InvalidArgument
     */
    static result<InputRequired>
    Create(std::map<std::string, ClientInputRequest> input_requests, std::optional<std::string> request_state);

    /**
     * @brief id-keyed 输入请求。
     */
    const std::map<std::string, ClientInputRequest>& InputRequests() const noexcept { return m_input_requests; }

    /**
     * @brief opaque 状态字符串（未携带时为空）。
     */
    const std::optional<std::string>& RequestState() const noexcept { return m_request_state; }

private:
    InputRequired() = default;
    std::map<std::string, ClientInputRequest> m_input_requests;
    std::optional<std::string> m_request_state;
};

/**
 * @}
 *
 * @name tools 自有原语（Schema 语义校验经私有 adapter 编译接缝，公共面无第三方类型）
 * @brief tools 注册/调用（tools/list、tools/call）与业务 MRTR 接线。
 *
 *   - ToolHandler 返回 HandlerResult<ToolResult>：完成值或 input_required（modern MRTR；
 *     wire 构造与客户端能力门禁由库经既有 Protocol 原语完成）；
 *   - input_schema 必需（根 type=object），output_schema 可选（声明则校验 structuredContent）；
 *     Schema 编译校验发生在注册期（fail-closed，一次成型）。
 * @{
 */

/**
 * @brief 工具定义（规范 Tool extends BaseMetadata, Icons）。
 *
 * name 必需非空；title/description 可选；input_schema 必需且根须为 object
 * （原始 JsonValue 保存；结构由 adapter 编译校验）；output_schema 可选。
 * annotations/icons 当前不支持（不提供字段，避免静默丢弃业务意图，与
 * ResourceDefinition 同口径）。
 */
struct ToolDefinition {

    /**
     * 工具名（必需、非空）
     */
    std::string name;

    /**
     * 显示标题（可选）
     */
    std::string title;

    /**
     * 描述（可选）
     */
    std::string description;

    /**
     * 输入 schema（必需，根须为 object）
     */
    JsonValue input_schema;

    /**
     * 输出 schema（可选）
     */
    std::optional<JsonValue> output_schema;
};

/**
 * @brief 工具执行结果。
 *
 * content 允许为空数组（协议允许）；is_error 表示业务错误（wire isError=true，
 * 不构成协议错误）；structured_content 仅在已声明 output_schema 时允许（未声明
 * → 库按协议结构缺陷固定拒绝，不静默放行未校验输出）。
 */
struct ToolResult {

    /**
     * 内容块列表（可为空）
     */
    std::vector<Content> content;

    /**
     * 结构化输出（须声明 output_schema）
     */
    std::optional<JsonValue> structured_content;

    /**
     * 业务错误标记（wire isError）
     */
    bool is_error = false;
};

/**
 * @brief 业务 handler 通用结果：完成值（Completed）或 input_required（modern
 *        MRTR 专用）。
 *
 * legacy 路径收到 InputRequired 由库固定拒绝（不产生 modern 结构）。
 * @tparam Complete 完成值类型
 */
template <typename Complete> class HandlerResult {
public:
    /**
     * @brief 以完成值构造。
     *
     * @param value 完成值
     */
    static HandlerResult Completed(Complete value);

    /**
     * @brief 以 input_required 构造（modern MRTR）。
     *
     * @param required 待客户端补全的输入请求
     */
    static HandlerResult Deferred(InputRequired required);

    /**
     * @brief 是否携带完成值。
     */
    bool IsCompleted() const noexcept { return m_value.index() == 0; }

    /**
     * @brief 是否携带 input_required。
     */
    bool IsInputRequired() const noexcept { return m_value.index() == 1; }

    /**
     * @brief 完成值（IsCompleted() 为真时有效）。
     */
    const Complete& Value() const { return std::get<0>(m_value); }

    /**
     * @brief input_required（IsInputRequired() 为真时有效）。
     */
    const InputRequired& Required() const { return std::get<1>(m_value); }

private:
    std::variant<Complete, InputRequired> m_value;
};

/**
 * @brief 回调上下文：owned（帧析构后仍有效），只读消费。
 *
 * 真实支撑：本次请求 id（原值 string/integer）、协商/声明的协议版本、
 * 客户端 capabilities（modern 取自逐请求 `_meta`；legacy 取自 initialize 的
 * owned 副本）、服务端关闭域只读视图（Cancellation()，真实由 Close 置位）、
 * 本请求期限、modern MRTR 重试携带的 inputResponses/requestState。
 * 不暴露 fd/Beast/jsoncons/coroutine 内部类型。
 */
class RequestContext {
public:
    /**
     * @brief 由库构造（业务只读消费，不需要自行构造）。
     *
     * @param id 本次请求 id
     * @param protocol_version 协商/声明的协议版本
     * @param client_capabilities 客户端 capabilities
     */
    RequestContext(RequestId id, std::string protocol_version, JsonValue client_capabilities);

    /**
     * @brief 同上，附带服务端关闭域视图。
     *
     * @param id 本次请求 id
     * @param protocol_version 协商/声明的协议版本
     * @param client_capabilities 客户端 capabilities
     * @param cancellation 关闭域只读视图
     */
    RequestContext(
        RequestId id, std::string protocol_version, JsonValue client_capabilities, CancellationView cancellation);

    /**
     * @brief 完整构造：附带 modern MRTR 重试输入与期限。
     *
     * id-keyed inputResponses 的每项为 object 载荷；requestState 为 opaque
     * string 原值回传。legacy 请求两者恒空。
     *
     * @param id 本次请求 id
     * @param protocol_version 协商/声明的协议版本
     * @param client_capabilities 客户端 capabilities
     * @param cancellation 关闭域只读视图
     * @param input_responses MRTR 输入响应（id-keyed）
     * @param request_state opaque 状态字符串
     * @param deadline 本请求期限；epoch 表示无期限
     */
    RequestContext(
        RequestId id,
        std::string protocol_version,
        JsonValue client_capabilities,
        CancellationView cancellation,
        std::map<std::string, JsonValue> input_responses,
        std::optional<std::string> request_state,
        std::chrono::steady_clock::time_point deadline = {});

    /**
     * @brief 本次请求 id。
     */
    const RequestId& Id() const noexcept { return m_id; }

    /**
     * @brief 协商/声明的协议版本。
     */
    const std::string& ProtocolVersion() const noexcept { return m_protocol_version; }

    /**
     * @brief 客户端 capabilities。
     */
    const JsonValue& ClientCapabilities() const noexcept { return m_client_capabilities; }

    /**
     * @brief 服务端关闭域（Close 置位）；可 copy 到栈外存活。
     */
    const CancellationView& Cancellation() const noexcept { return m_cancellation; }

    /**
     * @brief 是否带期限。epoch = 无期限（request_timeout<=0 的配置不会出现——Create
     * 严格拒绝非正值）。
     */
    bool HasDeadline() const noexcept { return m_deadline.time_since_epoch().count() != 0; }

    /**
     * @brief 本请求期限（由栈外 request owner 持有）。
     */
    std::chrono::steady_clock::time_point Deadline() const noexcept { return m_deadline; }

    /**
     * @brief 期限已过（handler 协作式退出观察点之一；终态落定仍在 owner/提交门）。
     */
    bool DeadlineExceeded() const noexcept;

    /**
     * @brief MRTR 重试输入（未携带时为空 map）。仅 modern 路径填充。
     */
    const std::map<std::string, JsonValue>& InputResponses() const noexcept { return m_input_responses; }

    /**
     * @brief MRTR 重试的 opaque 状态字符串（未携带时为空）。
     */
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

/**
 * @brief 工具回调：接（已通过 input schema 校验的）owned arguments，返回完成值或
 * input_required。库负责 wire 构造/envelope/resultType，回调不可覆盖。
 */
using ToolHandler = std::function<result<HandlerResult<ToolResult>>(const RequestContext&, JsonValue arguments)>;

/**
 * @}
 */
// clang-format off
}
// clang-format on

#include <bbt/infra/mcp/__TMcpTypes.hpp>
