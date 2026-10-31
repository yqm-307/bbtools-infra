#pragma once
// MCP JSON-RPC 信封、modern `_meta` 与结果构造（内部实现面，非安装头）。
//
// 契约来源：WFER mcp-server-foundation revision7（specs R2/R3/R4 中不依赖
// Schema/transport 的部分）+ 固定规范 confirmed-spec-schema.txt（2026-07-28）。
// 测试经 ${PROJECT_SOURCE_DIR}/bbt include 路径消费。

#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>
#include <bbt/infra/mcp/McpTypes.hpp>

namespace bbt::infra::mcp {

// modern `_meta` 的规范命名空间键（2026-07-28 RequestMetaObject）。
// 规范只定义这三处命名空间键，不存在裸 protocolVersion/clientCapabilities。
// 协议版本与客户端能力为必需；clientInfo 为可选（SHOULD）。
inline constexpr std::string_view kMetaKeyProtocolVersion =
    "io.modelcontextprotocol/protocolVersion";
inline constexpr std::string_view kMetaKeyClientCapabilities =
    "io.modelcontextprotocol/clientCapabilities";
inline constexpr std::string_view kMetaKeyClientInfo =
    "io.modelcontextprotocol/clientInfo";

// 已校验的 JSON-RPC 2.0 请求帧。
struct JsonRpcRequest {
    bool notification = false;      // 无 id 成员：不回 JSON-RPC 响应
    RequestId id;                   // notification 时为默认值，无 wire 含义
    std::string method;
    std::optional<JsonValue> params;  // 仅在 object/array 时保留
    std::optional<JsonValue> meta;    // params._meta（存在且为 object 时）
};

// 解析并校验单一 JSON-RPC 2.0 请求帧（不做方法分派）。
//   非 JSON / 超深 / 超大 / 重复键 / 不可表示数值 → -32700
//   顶层非 object（含 batch 数组）、缺/错 jsonrpc、缺 method、
//   请求里混入 result|error、id 为 null/float/非 string|integer → -32600
//   params 非 object|array → -32602
//
// detected_id：失败时若「id 已确定为 string/integer」则写入该 id，否则置 nullopt。
//   JSON-RPC 2.0 只允许 id 无法确定（parse error / invalid request 中的非法 id）
//   时回 null；params 形状等 id 已知的错误必须原值回包，调用方据此关联在途请求。
//   每次调用先清空（不沿用上一次调用的 id）；notification 不写 id。
//
// notification_recognized（F2 分类）：仅当帧通过**完整基本信封**校验（jsonrpc 合法、
//   未混入 result|error、method 为非空 string）且**无 id 成员**时置 true，否则置
//   false（每次调用先清空）。它只区分「可识别的 JSON-RPC 通知」与「不构成请求的
//   帧」：无 id 且基本信封合法、但 params 形状非法时，本函数仍返回失败（保留
//   InvalidParams），调用方据此按 JSON-RPC 2.0 §4.1「通知不得回包」静默丢弃，
//   而不是回 -32602 或把非法 params 当成可执行通知。非法 method / 非法 jsonrpc /
//   result 混入 / parse 失败这类不构成请求的帧**不**置位，仍回 -32700/-32600。
result<JsonRpcRequest> ParseRequestFrame(std::string_view raw, const ProtocolLimits& limits,
                                         std::optional<RequestId>* detected_id = nullptr,
                                         bool* notification_recognized = nullptr);

// Error → wire 错误（按固定 domain_code 映射，不含 data）。
ProtocolFailure ToProtocolFailure(const Error& error);

// modern 每请求独立校验结果（无跨请求状态）。
struct ModernMeta {
    std::string protocol_version;
    JsonValue client_capabilities;  // owned object copy
};

// 校验 modern 请求 params._meta 的两项必需命名空间键：
//   io.modelcontextprotocol/protocolVersion(string)
//   io.modelcontextprotocol/clientCapabilities(object)
// 允许规范 meta 的其他扩展键（如可选 clientInfo）存在，不做白名单拒绝。
//   缺字段/类型错 → -32602（message 固定文本，不回显输入）
//   版本不受支持 → -32022，data 含 requested:string 与 supported:string[]
//
// 版本语义（M3）：**modern 仅接受 kProtocolVersion2026_07_28**（acceptance 固定，
// 不接受 legacy revision）。`advertised_versions` 只用于 -32022 的
// data.supported——它是**调用方声明已实现**的 revision 列表，其真实性由调用方的
// 实现决定（本函数只忠实回显该实参，不自行宣称任何 revision 已被服务），
// 与 modern acceptance 是两件事，不得复用后者做现代校验。
// 失败分支需要结构化 data，而本仓 Error.details 只有 string 键值对，无法忠实承载
// supported 数组，故用 variant 表达「二择一」而不是伪造第二个通用错误类型。
std::variant<ModernMeta, ProtocolFailure> ValidateModernMeta(
    const std::optional<JsonValue>& params,
    const std::vector<std::string>& advertised_versions);

// 客户端 capabilities 声明所依据的 revision（决定校验用的「已知 shape」集合）。
// 两套 shape 来自各自 revision 的固定 schema：modern 2026-07-28 与 legacy
// 2025-11-25。legacy 的 sampling/elicitation 子字段约束随 revision
// 不同，故必须按「本次协商/反提议到的版本」选择，不能混用。2025-06-18
// 已不在服务矩阵（spec crosswalk 未核对，owner 决策移除），不为它保留
// 宽松 profile——其「子字段自由」规则曾掩盖真实 shape 差异。
enum class CapabilitiesRevision {
    Modern2026_07_28,
    Legacy2025_11_25,
};

// 按指定 revision 校验客户端声明的 capabilities（规范 ClientCapabilities）：
//   - 顶层必须是 object；能力集合是**开放**的——未知顶层字段一律保留，
//     不凭类型名一概拒绝自定义能力；
//   - 只校验该 revision 规范**已知**字段的 shape：experimental / extensions 为
//     string→object 映射；roots/sampling/elicitation/tasks 及各自规范已知子字段
//     为 object（roots.listChanged 为 boolean）；modern extensions 的 key 必须带
//     合法 `_meta` prefix（spec 命名规则），name 允许为空，不禁止官方保留域；
//   - 已知 object 内的未知扩展键原样保留（不因未知而拒）；
//   - 成功返回 owned object 副本（raw，不做规范化）；失败返回 -32602，message 为
//     固定诊断、不回显任何输入值。
std::variant<JsonValue, ProtocolFailure> ValidateClientCapabilities(
    const JsonValue& capabilities, CapabilitiesRevision revision);

// modern 成功结果由库写 resultType=complete（业务不可覆盖）。
result<void> ApplyModernResultType(JsonValue& result);
// CacheableResult（discover/tools/list/...）固定 ttlMs=0、cacheScope=private。
result<void> ApplyPrivateCacheHints(JsonValue& result);

// {"jsonrpc":"2.0","id":<原值>,"result":<result>}
JsonValue MakeSuccessResponse(const RequestId& id, JsonValue result_value);
// {"jsonrpc":"2.0","id":<原值|null>,"error":{"code":..,"message":..[,"data":..]}}
// id 为 nullopt 表示请求 id 无法确定（parse error / 非法 id），按规范回 null。
JsonValue MakeErrorResponse(const std::optional<RequestId>& id, const ProtocolFailure& failure);

// ---- 2-5：subscriptions/listen 与 MRTR（wire 构造/校验原语）--------------------
//
// wire 真源：官方 mcp_types 2.0.0 `_v2026_07_28`（与固定 schema 同源的安装版类型）。
// 本切片不接线业务 handler 返回 InputRequired（不做 tools/handler 接线）；此处提供
// 可运行的原语与 contract 检查，端到端 input_required 产出不在本切片宣告。

// subscriptions/listen params.notifications（SubscriptionFilter）的已校验形态。
// 三个 listChanged 键为 opt-in 布尔；resourceSubscriptions 为 uri 列表（去重保序）。
// 未知键按规范扩展允许（round-trip），不回显。
struct SubscriptionFilter {
    bool tools_list_changed = false;
    bool prompts_list_changed = false;
    bool resources_list_changed = false;
    std::vector<std::string> resource_uris;
};

// 解析过滤器：非 object / 布尔键非 boolean / resourceSubscriptions 非 string 数组
// → -32602（固定文本，不回显输入）。
std::variant<SubscriptionFilter, ProtocolFailure> ParseSubscriptionFilter(
    const JsonValue& notifications);

// accepted 子集 → wire 过滤器 object：只写为真的布尔键与非空 uri 数组
//（规范「unsupported types are omitted」；空支持子集 → 空 object）。
JsonValue MakeSubscriptionFilterJson(const SubscriptionFilter& filter);

// {"io.modelcontextprotocol/subscriptionId": <id 原值>}（ack/事件/流结束响应必带）。
result<JsonValue> MakeSubscriptionIdMeta(const RequestId& id);

// MRTR：InputRequired → wire result（调用方负责不叠加 resultType=complete）：
//   {"resultType":"input_required"[,"inputRequests":{<srv-id>:{method,params}}]
//    [,"requestState":"<opaque>"]}
// inputRequests 以服务端分配 id 为 key，value 只含 method+params（无内层 id/jsonrpc）。
result<JsonValue> MakeInputRequiredResult(const InputRequired& input_required);

// MRTR 客户端能力门禁（S15）：input request method → 必需 client capability：
//   sampling/createMessage→sampling、elicitation/create→elicitation、roots/list→roots。
// 必需键缺失或非 object → -32021，data.requiredCapabilities={<缺失能力>: {}}；
// 未知 method → -32602（服务端缺陷面，不回显输入）。
std::optional<ProtocolFailure> CheckInputRequestCapabilities(
    const JsonValue& client_capabilities,
    const std::map<std::string, ClientInputRequest>& input_requests);

// MRTR 重试请求的 owned 输入（RequestContext 用；legacy 恒空）。
struct MrtrRetryInputs {
    std::map<std::string, JsonValue> input_responses;  // id-keyed，每项必须为 object
    std::optional<std::string> request_state;          // opaque string 原值
};

// 从重试请求 params 提取 inputResponses/requestState：两者皆缺 → 空输入（首次请求
// 合法，非错误）；inputResponses 非 object/含空键/条目非 object、requestState 非
// string → -32602。服务器对 modern 无跨请求状态，故只校验形状，不与上一轮
// inputRequests 键做关联（opaque requestState 由业务自校验）。
std::variant<MrtrRetryInputs, ProtocolFailure> ParseMrtrRetryInputs(const JsonValue& params);

// server/discover 结果（库内建，无需业务注册）：
// supportedVersions（实参） + capabilities（实参；未实现的能力不得传入） +
// 可选 instructions + resultType=complete + ttlMs=0 + cacheScope=private。
result<JsonValue> MakeDiscoverResult(const std::vector<std::string>& supported_versions,
                                     JsonValue capabilities,
                                     const std::optional<std::string>& instructions);

} // namespace bbt::infra::mcp
