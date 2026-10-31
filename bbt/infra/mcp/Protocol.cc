// MCP 协议输入完整性 / owned 请求 id / modern `_meta` 校验 / discover·error·result
// 构造基础（specs R2/R3/R4 中不依赖 Schema 与 transport 的部分）。
//
// 本文件不做方法分派、不建立会话、不持有在途状态：所有校验都是「单帧、无跨请求
// 状态」的纯函数，因此 modern 请求之间不可能互相污染（S5 的隔离要求由结构保证，
// 并由 tests/Test_mcp_protocol.cc 逐项断言）。

#include <algorithm>
#include "detail/Protocol.hpp"

#include <utility>

namespace bbt::infra::mcp {

namespace {

// 固定错误构造：domain_code 机器可读，message 为规范固定文本（不回显输入）。
Error MakeProtocolError(ErrorCode code, const char* domain_code, const char* message) {
    Error e;
    e.code = code;
    e.domain_code = domain_code;
    e.message = message;
    return e;
}

template <class T>
result<T> InvalidRequest() {
    return result<T>::err(
        MakeProtocolError(ErrorCode::ProtocolError, "mcp.rpc.invalid_request",
                          "invalid request: not a JSON-RPC 2.0 request"));
}

template <class T>
result<T> InvalidParams() {
    return result<T>::err(
        MakeProtocolError(ErrorCode::ProtocolError, "mcp.rpc.invalid_params",
                          "invalid params: params must be an object or array"));
}

ProtocolFailure Failure(JsonRpcErrorCode code, const char* message) {
    ProtocolFailure f;
    f.code = code;
    f.message = message;
    return f;
}

// -32602 的固定诊断文本（不回显请求内容）。
ProtocolFailure MetaShapeFailure(const char* detail) {
    ProtocolFailure f;
    f.code = JsonRpcErrorCode::InvalidParams;
    f.message = detail;
    return f;
}

// ---- `_meta`/extensions key 命名（spec 39-50，ASCII 规则）----------------------

bool IsAsciiAlpha(char c) { return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'); }
bool IsAsciiDigit(char c) { return c >= '0' && c <= '9'; }
bool IsAsciiAlnum(char c) { return IsAsciiAlpha(c) || IsAsciiDigit(c); }

// label：首字母 letter，尾 letter|digit，内部 letter|digit|'-'。
bool IsValidMetaLabel(std::string_view label) {
    if (label.empty()) return false;
    if (!IsAsciiAlpha(label.front())) return false;
    if (!IsAsciiAlnum(label.back())) return false;
    for (char c : label)
        if (!IsAsciiAlnum(c) && c != '-') return false;
    return true;
}

// prefix：labels 用 '.' 分隔（无空 label），后跟 '/'；如 io.modelcontextprotocol/。
bool IsValidMetaPrefix(std::string_view prefix) {
    std::size_t start = 0;
    while (true) {
        const std::size_t dot = prefix.find('.', start);
        const std::string_view label =
            dot == std::string_view::npos ? prefix.substr(start) : prefix.substr(start, dot - start);
        if (!IsValidMetaLabel(label)) return false;
        if (dot == std::string_view::npos) return true;
        start = dot + 1;
    }
}

// name：允许为空；非空时首尾字母数字，内部字母数字/'-'/'_'/'.'（不含 '/'）。
bool IsValidMetaName(std::string_view name) {
    if (name.empty()) return true;
    if (!IsAsciiAlnum(name.front()) || !IsAsciiAlnum(name.back())) return false;
    for (char c : name)
        if (!IsAsciiAlnum(c) && c != '-' && c != '_' && c != '.') return false;
    return true;
}

// extensions key：prefix（必需且合法）+ '/' + name（空/合法）。
bool IsValidExtensionKey(std::string_view key) {
    const std::size_t slash = key.find('/');
    if (slash == std::string_view::npos) return false;
    return IsValidMetaPrefix(key.substr(0, slash)) && IsValidMetaName(key.substr(slash + 1));
}

// ---- capabilities 已知 shape 的最小校验工具（不做通用 schema 引擎）-----------

// 已知 object 字段：不存在 → 通过；存在则必须是 object。
bool ObjectIfPresent(const JsonValue& parent, std::string_view field) {
    auto value = parent.Find(field);
    if (!value) return true;  // 缺省即合法
    return value.value().IsObject();
}

// 已知 boolean 字段（roots.listChanged）：不存在 → 通过；存在则必须是 bool。
bool BoolIfPresent(const JsonValue& parent, std::string_view field) {
    auto value = parent.Find(field);
    if (!value) return true;
    return value.value().IsBool();
}

// string→object 映射（experimental / extensions）：每个成员值必须是 object。
bool IsObjectMap(const JsonValue& value) {
    if (!value.IsObject()) return false;
    auto keys = value.Keys();
    if (!keys) return false;
    for (const std::string& key : keys.value()) {
        auto item = value.Find(key);
        if (!item || !item.value().IsObject()) return false;
    }
    return true;
}

// experimental（三 revision 同 shape）：可选，string→object 映射。
bool ExperimentalOk(const JsonValue& capabilities) {
    if (!ObjectIfPresent(capabilities, "experimental")) return false;
    auto experimental = capabilities.Find("experimental");
    return !experimental || IsObjectMap(experimental.value());
}

// roots（legacy 已知子字段 listChanged:boolean；modern 无已知子字段约束）。
bool RootsOk(const JsonValue& capabilities, bool legacy) {
    if (!ObjectIfPresent(capabilities, "roots")) return false;
    if (!legacy) return true;
    auto roots = capabilities.Find("roots");
    return !roots || BoolIfPresent(roots.value(), "listChanged");
}

// sampling/elicitation（legacy 2025-11-25 要求 object + 已知子字段约束）。
bool ObjectSubfieldsOk(const JsonValue& capabilities, std::string_view field,
                       std::initializer_list<std::string_view> subfields) {
    if (!ObjectIfPresent(capabilities, field)) return false;
    auto parent = capabilities.Find(field);
    if (!parent) return true;
    for (std::string_view subfield : subfields)
        if (!ObjectIfPresent(parent.value(), subfield)) return false;
    return true;
}

// extensions（仅 modern 有已知约束）：string→object 映射，key 须带合法 prefix。
bool ExtensionsOk(const JsonValue& capabilities) {
    if (!ObjectIfPresent(capabilities, "extensions")) return false;
    auto extensions = capabilities.Find("extensions");
    if (!extensions) return true;
    if (!IsObjectMap(extensions.value())) return false;
    auto keys = extensions.value().Keys();
    if (!keys) return false;
    for (const std::string& key : keys.value())
        if (!IsValidExtensionKey(key)) return false;
    return true;
}

// legacy 2025-11-25 tasks 嵌套（list/cancel/requests.{sampling.createMessage,
// elicitation.create} 均为 object）。
bool TasksOk(const JsonValue& capabilities) {
    if (!ObjectIfPresent(capabilities, "tasks")) return false;
    auto tasks = capabilities.Find("tasks");
    if (!tasks) return true;
    if (!ObjectIfPresent(tasks.value(), "list") || !ObjectIfPresent(tasks.value(), "cancel") ||
        !ObjectIfPresent(tasks.value(), "requests"))
        return false;
    auto requests = tasks.value().Find("requests");
    if (!requests) return true;
    return ObjectSubfieldsOk(requests.value(), "sampling", {"createMessage"}) &&
           ObjectSubfieldsOk(requests.value(), "elicitation", {"create"});
}

} // namespace

result<JsonRpcRequest> ParseRequestFrame(std::string_view raw, const ProtocolLimits& limits,
                                         std::optional<RequestId>* detected_id,
                                         bool* notification_recognized) {
    // 每次调用先清空：失败路径绝不沿用上一次调用的 id / 通知分类。
    if (detected_id) detected_id->reset();
    if (notification_recognized) *notification_recognized = false;

    auto parsed = JsonValue::Parse(raw, limits.max_message_bytes, limits.max_depth);
    if (!parsed) return result<JsonRpcRequest>::err(parsed.error());
    JsonValue frame = std::move(parsed).value();

    // batch（顶层数组）与任何非 object 顶层都被明确拒为非法信封。
    if (!frame.IsObject()) return InvalidRequest<JsonRpcRequest>();

    auto version = frame.Find("jsonrpc");
    if (!version) return InvalidRequest<JsonRpcRequest>();
    auto version_text = version.value().AsString();
    if (!version_text || version_text.value() != "2.0") return InvalidRequest<JsonRpcRequest>();

    // id：string 或整数，保留精确值；无 id 成员即通知（不回响应）。
    // 先把 id 定下来：后续信封/params 失败若 id 已知，必须原值回包（JSON-RPC 2.0）。
    JsonRpcRequest request;
    auto id = frame.Find("id");
    if (id) {
        JsonValue& id_value = id.value();
        switch (id_value.Kind()) {
            case JsonKind::String:
                request.id = RequestId(std::move(id_value.AsString()).value());
                break;
            case JsonKind::Int64:
                request.id = RequestId(id_value.AsInt64().value());
                break;
            case JsonKind::UInt64:
                request.id = RequestId(id_value.AsUInt64().value());
                break;
            default:  // null / bool / double / array / object：id 不可确定
                return InvalidRequest<JsonRpcRequest>();
        }
        if (detected_id) *detected_id = request.id;
    } else {
        request.notification = true;
    }

    // 请求里混入 result/error（即 response 伪装成 request）明确拒。id 已知则已记录。
    if (frame.Find("result") || frame.Find("error")) return InvalidRequest<JsonRpcRequest>();

    auto method = frame.Find("method");
    if (!method) return InvalidRequest<JsonRpcRequest>();
    auto method_text = method.value().AsString();
    if (!method_text || method_text.value().empty()) return InvalidRequest<JsonRpcRequest>();

    request.method = std::move(method_text).value();

    // 到这里「基本信封」已合法（jsonrpc 合法、未混入 result|error、method 为非空
    // string），且无 id 成员 → 该帧是可识别的 JSON-RPC 通知。**仅在此之后**标记
    // 分类（F2）：若后续 params 形状非法，调用方据此按 JSON-RPC 2.0 §4.1「通知
    // 不得回包」静默丢弃（不回 -32602），且不把非法 params 当成可执行通知推进状态。
    // 非法 method / 非法 jsonrpc / result 混入 / parse 失败这类不构成请求的帧在此
    // 之前已返回，故不会被误分类（仍走 -32700/-32600）。
    if (request.notification && notification_recognized) *notification_recognized = true;

    auto params = frame.Find("params");
    if (params) {
        JsonValue& params_value = params.value();
        if (!params_value.IsObject() && !params_value.IsArray())
            return InvalidParams<JsonRpcRequest>();
        if (params_value.IsObject()) {
            auto meta = params_value.Find("_meta");
            if (meta && meta.value().IsObject()) request.meta = std::move(meta).value();
        }
        request.params = std::move(params_value);
    }

    return result<JsonRpcRequest>::ok(std::move(request));
}

ProtocolFailure ToProtocolFailure(const Error& error) {
    const std::string& domain_code = error.domain_code;
    // 帧级失败（JsonValue::Parse 的固定 domain_code）统一映射为 -32700：原因区分
    // 保留在 domain_code 中，wire 上仍是规范定义的 Parse error。
    if (domain_code.compare(0, 9, "mcp.json.") == 0)
        return Failure(JsonRpcErrorCode::ParseError, "Parse error");
    if (domain_code == "mcp.rpc.invalid_request")
        return Failure(JsonRpcErrorCode::InvalidRequest, "Invalid Request");
    if (domain_code == "mcp.rpc.invalid_params")
        return Failure(JsonRpcErrorCode::InvalidParams, "Invalid params");
    if (domain_code == "mcp.rpc.method_not_found")
        return Failure(JsonRpcErrorCode::MethodNotFound, "Method not found");
    return Failure(JsonRpcErrorCode::InternalError, "Internal error");
}

std::variant<ModernMeta, ProtocolFailure> ValidateModernMeta(
    const std::optional<JsonValue>& params,
    const std::vector<std::string>& advertised_versions) {
    if (!params || !params->IsObject())
        return MetaShapeFailure("Invalid params: params object with _meta is required");
    auto meta = params->Find("_meta");
    if (!meta || !meta.value().IsObject())
        return MetaShapeFailure("Invalid params: params._meta object is required");

    // 规范命名空间键（2026-07-28 RequestMetaObject），非裸 protocolVersion。
    auto version = meta.value().Find(kMetaKeyProtocolVersion);
    if (!version)
        return MetaShapeFailure(
            "Invalid params: params._meta.io.modelcontextprotocol/protocolVersion is required");
    auto version_text = version.value().AsString();
    if (!version_text)
        return MetaShapeFailure(
            "Invalid params: params._meta.io.modelcontextprotocol/protocolVersion must be a string");

    auto capabilities = meta.value().Find(kMetaKeyClientCapabilities);
    if (!capabilities)
        return MetaShapeFailure(
            "Invalid params: params._meta.io.modelcontextprotocol/clientCapabilities is required");
    if (!capabilities.value().IsObject())
        return MetaShapeFailure(
            "Invalid params: params._meta.io.modelcontextprotocol/clientCapabilities must be an "
            "object");

    const std::string requested = std::move(version_text).value();

    // modern acceptance 固定为唯一现代 revision；legacy revision 不因出现在
    // advertised_versions 里而被现代路径接受（两套方法表隔离，见 specs S7）。
    if (requested != kProtocolVersion2026_07_28) {
        // -32022：data 同时含 requested:string 与 supported:string[]（真实服务列表）。
        JsonValue data = JsonValue::Object();
        // Set 目标为新建 object、键为 ASCII 常量，不会失败（本仓 result 无 nodiscard）。
        data.Set("requested", JsonValue(requested));
        JsonValue list = JsonValue::Array();
        for (const std::string& candidate : advertised_versions) list.Push(JsonValue(candidate));
        data.Set("supported", std::move(list));
        ProtocolFailure failure = Failure(JsonRpcErrorCode::UnsupportedProtocolVersion,
                                         "Unsupported protocol version");
        failure.data = std::move(data);
        return failure;
    }

    // F6：已知字段按本 revision 的规范 shape 校验（失败 → -32602 固定诊断），
    // 成功后仍持有 owned raw object（未知字段保留，不做规范化）。
    auto shape = ValidateClientCapabilities(capabilities.value(),
                                            CapabilitiesRevision::Modern2026_07_28);
    if (auto* failure = std::get_if<ProtocolFailure>(&shape)) return *failure;

    ModernMeta result;
    result.protocol_version = requested;
    result.client_capabilities = std::move(std::get<JsonValue>(shape));
    return result;
}

std::variant<JsonValue, ProtocolFailure> ValidateClientCapabilities(
    const JsonValue& capabilities, CapabilitiesRevision revision) {
    if (!capabilities.IsObject())
        return MetaShapeFailure("Invalid params: clientCapabilities must be an object");
    if (!ExperimentalOk(capabilities))
        return MetaShapeFailure(
            "Invalid params: clientCapabilities.experimental must map strings to objects");
    if (!RootsOk(capabilities, revision != CapabilitiesRevision::Modern2026_07_28))
        return MetaShapeFailure(
            "Invalid params: clientCapabilities.roots has an invalid shape");
    // sampling/elicitation：两 revision 都要求 object；子字段约束（context/tools、
    // form/url 须 object）仅 modern 与 2025-11-25 已知——两 revision 同规则，
    // 分支只留语义区分（unknown 子字段原样保留）。
    if (!ObjectIfPresent(capabilities, "sampling"))
        return MetaShapeFailure("Invalid params: clientCapabilities.sampling must be an object");
    if (!ObjectIfPresent(capabilities, "elicitation"))
        return MetaShapeFailure("Invalid params: clientCapabilities.elicitation must be an object");
    {
        auto sampling = capabilities.Find("sampling");
        auto elicitation = capabilities.Find("elicitation");
        if ((sampling && (!ObjectIfPresent(sampling.value(), "context") ||
                          !ObjectIfPresent(sampling.value(), "tools"))) ||
            (elicitation && (!ObjectIfPresent(elicitation.value(), "form") ||
                             !ObjectIfPresent(elicitation.value(), "url"))))
            return MetaShapeFailure(
                "Invalid params: clientCapabilities.sampling/elicitation has an invalid shape");
    }
    if (revision == CapabilitiesRevision::Modern2026_07_28 && !ExtensionsOk(capabilities))
        return MetaShapeFailure(
            "Invalid params: clientCapabilities.extensions keys must carry a valid _meta prefix "
            "and map strings to objects");
    if (revision == CapabilitiesRevision::Legacy2025_11_25 && !TasksOk(capabilities))
        return MetaShapeFailure("Invalid params: clientCapabilities.tasks has an invalid shape");
    // owned 副本：调用方传入的帧对象析构后仍有效（值语义深复制）。
    return JsonValue(capabilities);
}

// 形参名避开模板名 result，否则 result<void> 会被解析成比较表达式。
result<void> ApplyModernResultType(JsonValue& target) {
    if (!target.IsObject())
        return result<void>::err(MakeProtocolError(ErrorCode::InvalidArgument,
                                                   "mcp.result.not_object",
                                                   "result must be a JSON object"));
    return target.Set("resultType", JsonValue("complete"));
}

result<void> ApplyPrivateCacheHints(JsonValue& target) {
    if (!target.IsObject())
        return result<void>::err(MakeProtocolError(ErrorCode::InvalidArgument,
                                                   "mcp.result.not_object",
                                                   "result must be a JSON object"));
    result<void> ttl = target.Set("ttlMs", JsonValue(std::int64_t(0)));
    if (!ttl) return ttl;
    return target.Set("cacheScope", JsonValue("private"));
}

JsonValue MakeSuccessResponse(const RequestId& id, JsonValue result_value) {
    // 下列 Set 的目标为新建 object、键为 ASCII 常量，不会失败；本仓 result 无
    // [[nodiscard]]，此处显式忽略是已知不可失败路径（非静默吞错）。
    JsonValue response = JsonValue::Object();
    response.Set("jsonrpc", JsonValue("2.0"));
    response.Set("id", id.ToJson());
    response.Set("result", std::move(result_value));
    return response;
}

JsonValue MakeErrorResponse(const std::optional<RequestId>& id, const ProtocolFailure& failure) {
    // 同 MakeSuccessResponse：新建 object + ASCII 常量键，Set 不可失败。
    JsonValue error = JsonValue::Object();
    error.Set("code", JsonValue(static_cast<std::int64_t>(failure.code)));
    error.Set("message", JsonValue(failure.message));
    if (failure.data) error.Set("data", *failure.data);

    JsonValue response = JsonValue::Object();
    response.Set("jsonrpc", JsonValue("2.0"));
    response.Set("id", id ? id->ToJson() : JsonValue());
    response.Set("error", std::move(error));
    return response;
}

result<JsonValue> MakeDiscoverResult(const std::vector<std::string>& supported_versions,
                                     JsonValue capabilities,
                                     const std::optional<std::string>& instructions) {
    if (!capabilities.IsObject())
        return result<JsonValue>::err(MakeProtocolError(ErrorCode::InvalidArgument,
                                                        "mcp.discover.capabilities",
                                                        "capabilities must be a JSON object"));
    if (supported_versions.empty())
        return result<JsonValue>::err(MakeProtocolError(ErrorCode::InvalidArgument,
                                                        "mcp.discover.versions",
                                                        "at least one supported version is required"));

    JsonValue output = JsonValue::Object();
    JsonValue versions = JsonValue::Array();
    for (const std::string& version : supported_versions) versions.Push(JsonValue(version));
    // 新建 object + ASCII 常量键：Set 不可失败（同上）。
    output.Set("supportedVersions", std::move(versions));
    output.Set("capabilities", std::move(capabilities));
    if (instructions) output.Set("instructions", JsonValue(*instructions));

    result<void> typed = ApplyModernResultType(output);
    if (!typed) return result<JsonValue>::err(typed.error());
    result<void> cached = ApplyPrivateCacheHints(output);
    if (!cached) return result<JsonValue>::err(cached.error());
    return result<JsonValue>::ok(std::move(output));
}

} // namespace bbt::infra::mcp

// ---- 2-5：subscriptions/listen 与 MRTR ----------------------------------------

namespace bbt::infra::mcp {

namespace {

ProtocolFailure InvalidParamsFailure(const char* message) {
    ProtocolFailure failure;
    failure.code = JsonRpcErrorCode::InvalidParams;
    failure.message = message;
    return failure;
}

} // namespace

std::variant<SubscriptionFilter, ProtocolFailure> ParseSubscriptionFilter(
    const JsonValue& notifications) {
    if (!notifications.IsObject())
        return InvalidParamsFailure("Invalid params: notifications filter must be an object");
    SubscriptionFilter filter;
    auto flag = [&](std::string_view key, bool& out) -> std::optional<ProtocolFailure> {
        auto value = notifications.Find(std::string(key));
        if (!value) return std::nullopt;
        if (!value.value().IsBool())
            return InvalidParamsFailure(
                "Invalid params: notifications filter flags must be boolean");
        out = value.value().AsBool().value();
        return std::nullopt;
    };
    if (auto e = flag(kFilterKeyToolsListChanged, filter.tools_list_changed)) return *e;
    if (auto e = flag(kFilterKeyPromptsListChanged, filter.prompts_list_changed)) return *e;
    if (auto e = flag(kFilterKeyResourcesListChanged, filter.resources_list_changed)) return *e;

    if (auto list = notifications.Find(std::string(kFilterKeyResourceSubscriptions))) {
        if (!list.value().IsArray())
            return InvalidParamsFailure(
                "Invalid params: resourceSubscriptions must be an array of strings");
        const std::size_t size = list.value().Size();
        for (std::size_t i = 0; i < size; ++i) {
            auto item = list.value().At(i);
            if (!item || !item.value().IsString())
                return InvalidParamsFailure(
                    "Invalid params: resourceSubscriptions must be an array of strings");
            const std::string uri(item.value().AsString().value());
            // 去重保序：同一 uri 重复请求只接纳一次。
            if (std::find(filter.resource_uris.begin(), filter.resource_uris.end(), uri) ==
                filter.resource_uris.end())
                filter.resource_uris.push_back(uri);
        }
    }
    return filter;
}

JsonValue MakeSubscriptionFilterJson(const SubscriptionFilter& filter) {
    JsonValue out = JsonValue::Object();
    // 新建 object + ASCII 常量键 → Set 不可失败（ADR 0010 第 11 条同口径）。
    if (filter.tools_list_changed)
        out.Set(std::string(kFilterKeyToolsListChanged), JsonValue(true));
    if (filter.prompts_list_changed)
        out.Set(std::string(kFilterKeyPromptsListChanged), JsonValue(true));
    if (filter.resources_list_changed)
        out.Set(std::string(kFilterKeyResourcesListChanged), JsonValue(true));
    if (!filter.resource_uris.empty()) {
        JsonValue uris = JsonValue::Array();
        for (const std::string& uri : filter.resource_uris) uris.Push(JsonValue(uri));
        out.Set(std::string(kFilterKeyResourceSubscriptions), std::move(uris));
    }
    return out;
}

result<JsonValue> MakeSubscriptionIdMeta(const RequestId& id) {
    JsonValue meta = JsonValue::Object();
    result<void> set = meta.Set(std::string(kMetaKeySubscriptionId), id.ToJson());
    if (!set) return result<JsonValue>::err(set.error());
    return result<JsonValue>::ok(std::move(meta));
}

result<JsonValue> MakeInputRequiredResult(const InputRequired& input_required) {
    // 至少其一非空（规范 MUST）；InputRequired::Create 已保证，此处防御性再查。
    if (input_required.InputRequests().empty() && !input_required.RequestState()) {
        Error error;
        error.code = ErrorCode::InvalidArgument;
        error.domain_code = "mcp.mrtr.empty";
        error.message = "input_required requires inputRequests or requestState";
        return result<JsonValue>::err(std::move(error));
    }
    JsonValue out = JsonValue::Object();
    result<void> set = out.Set("resultType", JsonValue("input_required"));
    if (!set) return result<JsonValue>::err(set.error());
    if (!input_required.InputRequests().empty()) {
        JsonValue requests = JsonValue::Object();
        for (const auto& [key, request] : input_required.InputRequests()) {
            JsonValue value = JsonValue::Object();
            set = value.Set("method", JsonValue(request.method));
            if (!set) return result<JsonValue>::err(set.error());
            set = value.Set("params", request.params);  // 只含 method+params，无内层 id/jsonrpc
            if (!set) return result<JsonValue>::err(set.error());
            set = requests.Set(key, std::move(value));  // id-keyed map
            if (!set) return result<JsonValue>::err(set.error());
        }
        set = out.Set(std::string(kWireKeyInputRequests), std::move(requests));
        if (!set) return result<JsonValue>::err(set.error());
    }
    if (input_required.RequestState()) {
        set = out.Set(std::string(kWireKeyRequestState), JsonValue(*input_required.RequestState()));
        if (!set) return result<JsonValue>::err(set.error());
    }
    return result<JsonValue>::ok(std::move(out));
}

std::optional<ProtocolFailure> CheckInputRequestCapabilities(
    const JsonValue& client_capabilities,
    const std::map<std::string, ClientInputRequest>& input_requests) {
    static const char* kInvalidMethod = "Invalid params: unknown input request method";
    JsonValue missing = JsonValue::Object();
    bool any_missing = false;
    for (const auto& [key, request] : input_requests) {
        (void)key;
        std::string_view capability;
        if (request.method == "sampling/createMessage")
            capability = "sampling";
        else if (request.method == "elicitation/create")
            capability = "elicitation";
        else if (request.method == "roots/list")
            capability = "roots";
        else
            return InvalidParamsFailure(kInvalidMethod);
        // 存在且为 object 即视为已声明（shape 由 ValidateClientCapabilities 按三
        // revision 校验）；显式 null 与缺失同义。
        auto declared = client_capabilities.Find(std::string(capability));
        if (!declared || !declared.value().IsObject()) {
            any_missing = true;
            missing.Set(std::string(capability), JsonValue::Object());
        }
    }
    if (!any_missing) return std::nullopt;
    ProtocolFailure failure;
    failure.code = JsonRpcErrorCode::MissingRequiredClientCapability;
    failure.message = "Missing required client capability";
    JsonValue data = JsonValue::Object();
    result<void> set = data.Set("requiredCapabilities", std::move(missing));
    if (!set) {
        Error error;
        error.code = ErrorCode::InternalError;
        error.domain_code = "mcp.mrtr.data";
        error.message = "failed to build error data";
        return ProtocolFailure{};  // 不可达防御：新建 object + ASCII 键
    }
    failure.data = std::move(data);
    return failure;
}

std::variant<MrtrRetryInputs, ProtocolFailure> ParseMrtrRetryInputs(const JsonValue& params) {
    MrtrRetryInputs out;
    if (auto responses = params.Find(std::string(kWireKeyInputResponses))) {
        if (!responses.value().IsObject())
            return InvalidParamsFailure("Invalid params: inputResponses must be an object");
        auto keys = responses.value().Keys();
        if (!keys)
            return InvalidParamsFailure("Invalid params: inputResponses must be an object");
        for (const std::string& key : keys.value()) {
            if (key.empty())
                return InvalidParamsFailure("Invalid params: inputResponses keys must be non-empty");
            auto item = responses.value().Find(key);
            if (!item || !item.value().IsObject())
                return InvalidParamsFailure("Invalid params: inputResponses entries must be objects");
            out.input_responses.emplace(key, item.value());
        }
    }
    if (auto state = params.Find(std::string(kWireKeyRequestState))) {
        if (!state.value().IsString())
            return InvalidParamsFailure("Invalid params: requestState must be a string");
        out.request_state = std::string(state.value().AsString().value());
    }
    return out;
}

} // namespace bbt::infra::mcp
