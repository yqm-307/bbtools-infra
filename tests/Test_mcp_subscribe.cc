// 2-5 切片 contract 检查：subscriptions/listen（ack/filters/subscriptionId、长请求）
// 与 MRTR 基础 message patterns（specs S14/S15）。
//
//   S14：每订阅第一条 ack（含空支持子集）、accepted 过滤（未支持类型省略）、
//        subscriptionId 携带与多订阅隔离、事件顺序、cancel 结束流可核验；
//        请求级 progress 不属于本切片（未实现 progress，无帧落入 listen 流）。
//   S15：InputRequired wire（id-keyed inputRequests、opaque requestState）、
//        客户端能力门禁（-32021 + data.requiredCapabilities）、重试请求经真实分派
//        携带 inputResponses/requestState 到达 handler、新 id 重试、legacy 不发
//        modern 结构。
//
// 边界（不冒充）：真实 stdio/HTTP SSE transport 属 3-x，本文件经内部面 sink 驱动
// 帧出口；业务 handler 返回 InputRequired 的接线不在本切片（无端到端
// input_required 产出断言）。无第三方测试框架；经 src/ 内部头消费实现面。

#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <csignal>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>
#include <bbt/infra/mcp/McpTypes.hpp>

#include "mcp/Protocol.hpp"
#include "mcp/Server.hpp"

namespace {

int g_checks = 0;
int g_failures = 0;

void Check(bool ok, const char* expr, int line) {
    ++g_checks;
    if (!ok) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d  %s\n", __FILE__, line, expr);
    }
}

#define CHECK(expr) Check(static_cast<bool>(expr), #expr, __LINE__)

using bbt::infra::result;
using bbt::infra::mcp::CheckInputRequestCapabilities;
using bbt::infra::mcp::ClientInputRequest;
using bbt::infra::mcp::CreateSession;
using bbt::infra::mcp::InputRequired;
using bbt::infra::mcp::JsonValue;
using bbt::infra::mcp::MakeInputRequiredResult;
using bbt::infra::mcp::McpServer;
using bbt::infra::mcp::McpSession;
using bbt::infra::mcp::ProtocolFailure;
using bbt::infra::mcp::RequestContext;
using bbt::infra::mcp::RequestId;
using bbt::infra::mcp::ResourceContents;
using bbt::infra::mcp::ResourceDefinition;
using bbt::infra::mcp::ResourceResult;
using bbt::infra::mcp::ServerOptions;

constexpr const char* kSubIdKey = "io.modelcontextprotocol/subscriptionId";

// --- 小工具 -------------------------------------------------------------------

JsonValue ParseOk(const std::string& text) {
    auto parsed = JsonValue::Parse(text, 1U << 20, 32);
    if (!parsed) {
        ++g_checks;
        ++g_failures;
        std::fprintf(stderr, "FAIL frame is not valid JSON: %s\n", text.c_str());
        return JsonValue();
    }
    return std::move(parsed).value();
}

std::optional<JsonValue> Find(const JsonValue& value, const std::string& key) {
    auto found = value.Find(key);
    if (!found) return std::nullopt;
    return std::move(found).value();
}

std::string Str(const std::optional<JsonValue>& value) {
    if (!value) return "<missing>";
    auto text = value->AsString();
    if (!text) return "<not-string>";
    return std::move(text).value();
}

// JsonValue::At 返回 result 而非 optional。
std::string Str(const result<JsonValue>& value) {
    if (!value) return "<missing>";
    auto text = value.value().AsString();
    if (!text) return "<not-string>";
    return std::move(text).value();
}

JsonValue ModernMeta(JsonValue capabilities) {
    JsonValue meta = JsonValue::Object();
    meta.Set("io.modelcontextprotocol/protocolVersion", JsonValue("2026-07-28"));
    meta.Set("io.modelcontextprotocol/clientCapabilities", std::move(capabilities));
    return meta;
}

// modern 请求帧（id 为 string）。
std::string ModernFrame(const std::string& id, const std::string& method, JsonValue params) {
    JsonValue frame = JsonValue::Object();
    frame.Set("jsonrpc", JsonValue("2.0"));
    frame.Set("id", JsonValue(id));
    frame.Set("method", JsonValue(method));
    params.Set("_meta", ModernMeta(JsonValue::Object()));
    frame.Set("params", std::move(params));
    return frame.Serialize();
}

// subscriptions/listen params：notifications 过滤器 + modern _meta（ModernFrame 补）。
JsonValue ListenParams(JsonValue filter) {
    JsonValue params = JsonValue::Object();
    params.Set("notifications", std::move(filter));
    return params;
}

// legacy 请求帧。
std::string LegacyFrame(const std::string& id, const std::string& method, JsonValue params) {
    JsonValue frame = JsonValue::Object();
    frame.Set("jsonrpc", JsonValue("2.0"));
    frame.Set("id", JsonValue(id));
    frame.Set("method", JsonValue(method));
    frame.Set("params", std::move(params));
    return frame.Serialize();
}

// 帧出口记录器。
struct SinkLog {
    std::vector<std::string> frames;
    void Record(const std::string& frame) { frames.push_back(frame); }
    std::size_t Size() const { return frames.size(); }
    JsonValue Last() { return ParseOk(frames.back()); }
};

// 带一个已注册资源 mem://a 的已启动 server + 会话。
struct Rig {
    std::shared_ptr<McpServer> server;
    std::shared_ptr<McpSession> session;
    std::shared_ptr<SinkLog> log;
    int handler_calls = 0;
    std::map<std::string, JsonValue> seen_input_responses;
    std::optional<std::string> seen_request_state;
};

Rig MakeRig(std::size_t max_inflight = 64) {
    Rig rig;
    ServerOptions options;
    options.max_inflight_requests = max_inflight;
    rig.server = std::move(McpServer::Create(std::move(options)).value());
    ResourceDefinition definition;
    definition.uri = "mem://a";
    definition.name = "a";
    Rig* rig_ptr = &rig;
    auto handler = [rig_ptr](const RequestContext& context,
                             const std::string& uri) -> result<ResourceResult> {
        rig_ptr->handler_calls += 1;
        rig_ptr->seen_input_responses = context.InputResponses();
        rig_ptr->seen_request_state = context.RequestState();
        ResourceResult produced;
        auto contents = ResourceContents::Text(uri, "text/plain", "payload");
        produced.contents.push_back(std::move(contents).value());
        return result<ResourceResult>::ok(std::move(produced));
    };
    CHECK(rig.server->AddResource(definition, handler));
    CHECK(rig.server->Start());
    rig.session = CreateSession(rig.server);
    rig.log = std::make_shared<SinkLog>();
    rig.session->SetNotificationSink([log = rig.log](const std::string& frame) {
        log->Record(frame);
    });
    return rig;
}

// 解析 HandleFrame 的响应（断言必须回包）。
JsonValue Respond(McpSession& session, const std::string& frame) {
    auto outcome = session.HandleFrame(frame);
    ++g_checks;
    if (!outcome) {
        ++g_failures;
        std::fprintf(stderr, "FAIL HandleFrame returned core error\n");
        return JsonValue();
    }
    if (!outcome.value().has_response) {
        ++g_failures;
        std::fprintf(stderr, "FAIL expected a response frame\n");
        return JsonValue();
    }
    return ParseOk(outcome.value().response);
}

// 断言错误响应的 code。
std::int64_t ErrorCodeOf(McpSession& session, const std::string& frame) {
    JsonValue response = Respond(session, frame);
    auto code = Find(Find(response, "error").value_or(JsonValue()), "code");
    if (!code) return 0;
    auto number = code->AsInt64();
    return number ? number.value() : 0;
}


} // namespace

int main() {
    using bbt::infra::mcp::JsonRpcErrorCode;

    // ================= S14：subscriptions/listen ==============================

    {
        // ack 先行 + accepted 子集过滤 + subscriptionId。
        Rig rig = MakeRig();
        JsonValue filter = JsonValue::Object();
        filter.Set("toolsListChanged", JsonValue(true));  // server 无 listChanged 机制 → 省略
        JsonValue uris = JsonValue::Array();
        uris.Push(JsonValue("mem://a"));
        uris.Push(JsonValue("mem://unknown"));  // 未注册 → 省略
        filter.Set("resourceSubscriptions", std::move(uris));
        auto outcome = rig.session->HandleFrame(
            ModernFrame("sub-a", "subscriptions/listen", ListenParams(std::move(filter))));
        CHECK(outcome);
        CHECK(!outcome.value().has_response);  // 长请求：ack 是通知，无即时响应
        CHECK(rig.log->Size() == 1);
        JsonValue ack = rig.log->Last();
        CHECK(Str(Find(ack, "method")) == "notifications/subscriptions/acknowledged");
        const JsonValue ack_params = Find(ack, "params").value_or(JsonValue());
        const JsonValue accepted = Find(ack_params, "notifications").value_or(JsonValue());
        CHECK(accepted.IsObject());
        CHECK(!Find(accepted, "toolsListChanged"));  // 未支持类型被省略
        CHECK(!Find(accepted, "promptsListChanged"));
        auto echoed = Find(accepted, "resourceSubscriptions");
        CHECK(echoed && echoed->IsArray() && echoed->Size() == 1);
        CHECK(Str(echoed->At(0)) == "mem://a");  // 未注册 uri 不被接纳
        auto sub_meta = Find(ack_params, "_meta");
        CHECK(sub_meta && Str(Find(*sub_meta, kSubIdKey)) == "sub-a");
    }

    {
        // 空支持子集 + 整数 listen id + 第二订阅独立。
        Rig rig = MakeRig();
        JsonValue filter = JsonValue::Object();
        filter.Set("promptsListChanged", JsonValue(true));  // server 不支持 → ack 空 object
        JsonValue frame = JsonValue::Object();
        frame.Set("jsonrpc", JsonValue("2.0"));
        frame.Set("id", JsonValue(static_cast<std::int64_t>(2)));
        frame.Set("method", JsonValue("subscriptions/listen"));
        JsonValue params = JsonValue::Object();
        params.Set("notifications", std::move(filter));
        params.Set("_meta", ModernMeta(JsonValue::Object()));
        frame.Set("params", std::move(params));
        auto outcome = rig.session->HandleFrame(frame.Serialize());
        CHECK(outcome && !outcome.value().has_response);
        CHECK(rig.log->Size() == 1);
        JsonValue ack = rig.log->Last();
        const JsonValue accepted =
            Find(Find(ack, "params").value_or(JsonValue()), "notifications").value_or(JsonValue());
        CHECK(accepted.IsObject() && accepted.Size() == 0);  // 空支持子集
        auto sub_meta = Find(Find(ack, "params").value_or(JsonValue()), "_meta");
        auto id_value = sub_meta ? Find(*sub_meta, kSubIdKey) : std::nullopt;
        CHECK(id_value);
        auto as_int = id_value->AsInt64();
        CHECK(as_int && as_int.value() == 2);  // subscriptionId = listen 请求 id 原值
    }

    {
        // 发布过滤 + id 隔离 + 事件顺序 + cancel 结束流。
        Rig rig = MakeRig();
        auto outcome_a = rig.session->HandleFrame(ModernFrame(
            "sub-a", "subscriptions/listen", ListenParams([] {
                JsonValue filter = JsonValue::Object();
                JsonValue uris = JsonValue::Array();
                uris.Push(JsonValue("mem://a"));
                filter.Set("resourceSubscriptions", std::move(uris));
                return filter;
            }())));
        auto outcome_b = rig.session->HandleFrame(ModernFrame(
            "sub-b", "subscriptions/listen", ListenParams([] {
                JsonValue filter = JsonValue::Object();
                filter.Set("toolsListChanged", JsonValue(true));
                return filter;
            }())));
        CHECK(outcome_a && outcome_b);
        CHECK(rig.log->Size() == 2);  // 两条 ack

        auto published = rig.server->PublishResourceChanged("mem://a");
        CHECK(published);
        CHECK(published.value() == 1);  // 只有 sub-a 接纳该 uri
        CHECK(rig.log->Size() == 3);
        JsonValue event = rig.log->Last();
        CHECK(Str(Find(event, "method")) == "notifications/resources/updated");
        const JsonValue event_params = Find(event, "params").value_or(JsonValue());
        CHECK(Str(Find(event_params, "uri")) == "mem://a");
        auto event_meta = Find(event_params, "_meta");
        CHECK(event_meta && Str(Find(*event_meta, kSubIdKey)) == "sub-a");

        // 事件顺序：两次发布 → sub-a 依次收到两帧。
        CHECK(rig.server->PublishResourceChanged("mem://a").value() == 1);
        CHECK(rig.log->Size() == 4);
        JsonValue event2 = rig.log->Last();
        CHECK(Str(Find(event2, "method")) == "notifications/resources/updated");

        // 未注册 uri 发布被拒；不产生帧。
        auto bad = rig.server->PublishResourceChanged("mem://ghost");
        CHECK(!bad);
        CHECK(rig.log->Size() == 4);

        // 非订阅 id 的 cancel：静默丢弃，无帧。
        auto stray = rig.session->HandleFrame([] {
            JsonValue params = JsonValue::Object();
            params.Set("requestId", JsonValue("sub-nope"));
            JsonValue frame = JsonValue::Object();
            frame.Set("jsonrpc", JsonValue("2.0"));
            frame.Set("method", JsonValue("notifications/cancelled"));
            frame.Set("params", std::move(params));
            return frame.Serialize();
        }());
        CHECK(stray && !stray.value().has_response);
        CHECK(rig.log->Size() == 4);

        // cancel sub-a → 流结束响应帧（id 原值 + result._meta.subscriptionId）。
        auto cancel = rig.session->HandleFrame([] {
            JsonValue params = JsonValue::Object();
            params.Set("requestId", JsonValue("sub-a"));
            JsonValue frame = JsonValue::Object();
            frame.Set("jsonrpc", JsonValue("2.0"));
            frame.Set("method", JsonValue("notifications/cancelled"));
            frame.Set("params", std::move(params));
            return frame.Serialize();
        }());
        CHECK(cancel && !cancel.value().has_response);
        CHECK(rig.log->Size() == 5);
        JsonValue closing = rig.log->Last();
        CHECK(Str(Find(closing, "id")) == "sub-a");
        const JsonValue closing_result = Find(closing, "result").value_or(JsonValue());
        CHECK(Str(Find(closing_result, "resultType")) == "complete");
        auto closing_meta = Find(closing_result, "_meta");
        CHECK(closing_meta && Str(Find(*closing_meta, kSubIdKey)) == "sub-a");

        // 结束后发布不再投递。
        CHECK(rig.server->PublishResourceChanged("mem://a").value() == 0);
        CHECK(rig.log->Size() == 5);

        // 同会话重复活动 listen id 被拒（sub-b 仍在册）。
        auto duplicate = rig.session->HandleFrame(ModernFrame(
            "sub-b", "subscriptions/listen", ListenParams(JsonValue::Object())));
        CHECK(duplicate);
        JsonValue dup_response = ParseOk(duplicate.value().response);
        auto code = Find(Find(dup_response, "error").value_or(JsonValue()), "code");
        CHECK(code);
        auto code_value = code->AsInt64();
        CHECK(code_value &&
              code_value.value() == static_cast<std::int64_t>(JsonRpcErrorCode::InvalidParams));
    }

    {
        // 非法过滤器 → -32602；legacy 表无 subscriptions/listen → -32601。
        Rig rig = MakeRig();
        CHECK(ErrorCodeOf(*rig.session, ModernFrame("s1", "subscriptions/listen",
                                                    JsonValue::Object())) ==
              static_cast<std::int64_t>(JsonRpcErrorCode::InvalidParams));
        JsonValue bad_filter = JsonValue::Object();
        bad_filter.Set("resourceSubscriptions", JsonValue("mem://a"));  // 非 array
        CHECK(ErrorCodeOf(*rig.session, ModernFrame("s2", "subscriptions/listen",
                                                    ListenParams(std::move(bad_filter)))) ==
              static_cast<std::int64_t>(JsonRpcErrorCode::InvalidParams));
        JsonValue bad_flag = JsonValue::Object();
        bad_flag.Set("toolsListChanged", JsonValue("yes"));
        CHECK(ErrorCodeOf(*rig.session, ModernFrame("s3", "subscriptions/listen",
                                                    ListenParams(std::move(bad_flag)))) ==
              static_cast<std::int64_t>(JsonRpcErrorCode::InvalidParams));
        CHECK(ErrorCodeOf(*rig.session, LegacyFrame("s4", "subscriptions/listen",
                                                    JsonValue::Object())) ==
              static_cast<std::int64_t>(JsonRpcErrorCode::MethodNotFound));
        CHECK(rig.log->Size() == 0);  // 失败路径零帧
    }

    {
        // 长请求占用在途名额（稳态上界）：max_inflight=1 → 订阅即占满，普通 request
        // 核心拒绝；通知类帧（cancel）不占名额，仍可准入并结束订阅（I1 回修回归）。
        Rig saturated = MakeRig(/*max_inflight=*/1);
        auto listen = saturated.session->HandleFrame(ModernFrame(
            "solo", "subscriptions/listen", ListenParams([] {
                JsonValue filter = JsonValue::Object();
                JsonValue uris = JsonValue::Array();
                uris.Push(JsonValue("mem://a"));
                filter.Set("resourceSubscriptions", std::move(uris));
                return filter;
            }())));
        CHECK(listen && !listen.value().has_response);
        auto blocked =
            saturated.session->HandleFrame(LegacyFrame("x", "ping", JsonValue::Object()));
        CHECK(!blocked);  // 普通 request 超在途上限 → 核心拒绝（准入语义不变）
        // 饱和通道上 cancel 仍可达（通知不占名额）：流结束帧 + 名额释放。
        auto cancel = saturated.session->HandleFrame([] {
            JsonValue params = JsonValue::Object();
            params.Set("requestId", JsonValue("solo"));
            JsonValue frame = JsonValue::Object();
            frame.Set("jsonrpc", JsonValue("2.0"));
            frame.Set("method", JsonValue("notifications/cancelled"));
            frame.Set("params", std::move(params));
            return frame.Serialize();
        }());
        CHECK(cancel && !cancel.value().has_response);
        CHECK(saturated.log->Size() == 2);  // ack + 流结束响应
        JsonValue closing = saturated.log->Last();
        CHECK(Str(Find(closing, "id")) == "solo");
        CHECK(Str(Find(Find(closing, "result").value_or(JsonValue()), "resultType")) ==
              "complete");
        auto released = saturated.server->PublishResourceChanged("mem://a");
        CHECK(released && released.value() == 0);  // 订阅已结束：0 帧、名额已归还
        auto freed =
            saturated.session->HandleFrame(LegacyFrame("y", "ping", JsonValue::Object()));
        CHECK(freed && freed.value().has_response);  // 后续 request 可入
    }

    {
        // cancel 释放长请求名额：max_inflight=2 容纳订阅 + cancel 通知；结束后归位。
        Rig rig = MakeRig(/*max_inflight=*/2);
        auto listen = rig.session->HandleFrame(ModernFrame(
            "solo", "subscriptions/listen", ListenParams([] {
                JsonValue filter = JsonValue::Object();
                JsonValue uris = JsonValue::Array();
                uris.Push(JsonValue("mem://a"));
                filter.Set("resourceSubscriptions", std::move(uris));
                return filter;
            }())));
        CHECK(listen && !listen.value().has_response);
        auto cancel = rig.session->HandleFrame([] {
            JsonValue params = JsonValue::Object();
            params.Set("requestId", JsonValue("solo"));
            JsonValue frame = JsonValue::Object();
            frame.Set("jsonrpc", JsonValue("2.0"));
            frame.Set("method", JsonValue("notifications/cancelled"));
            frame.Set("params", std::move(params));
            return frame.Serialize();
        }());
        CHECK(cancel && !cancel.value().has_response);
        auto freed = rig.session->HandleFrame(LegacyFrame("x", "ping", JsonValue::Object()));
        CHECK(freed);  // 名额已归还
        CHECK(rig.log->Size() == 2);  // ack + 流结束响应
    }

    {
        // Close 丢弃在册订阅：发布被拒，不再产生帧。
        Rig rig = MakeRig();
        auto listen = rig.session->HandleFrame(ModernFrame(
            "end", "subscriptions/listen", ListenParams([] {
                JsonValue filter = JsonValue::Object();
                JsonValue uris = JsonValue::Array();
                uris.Push(JsonValue("mem://a"));
                filter.Set("resourceSubscriptions", std::move(uris));
                return filter;
            }())));
        CHECK(listen);
        rig.server->Close();
        auto after = rig.server->PublishResourceChanged("mem://a");
        CHECK(!after);
        CHECK(rig.log->Size() == 1);  // 仅 ack；abrupt 封口不发流结束响应
    }

    // 能力只反映真实机制：无 legacy subscribe/unsubscribe handler（FamilyOf 无
    // resources/subscribe/unsubscribe，SDK 实测 -32601），故不宣告 subscribe；
    // resources 原语与 modern subscriptions/listen 照常；无 listChanged。
    {
        Rig rig = MakeRig();
        JsonValue response = Respond(*rig.session, ModernFrame("d1", "server/discover",
                                                               JsonValue::Object()));
        const JsonValue result = Find(response, "result").value_or(JsonValue());
        const JsonValue capabilities = Find(result, "capabilities").value_or(JsonValue());
        const JsonValue resources = Find(capabilities, "resources").value_or(JsonValue());
        CHECK(!Find(resources, "subscribe"));
        CHECK(!Find(resources, "listChanged"));
    }

    // ================= S15：MRTR 基础 message patterns ========================

    {
        // InputRequired::Create：至少其一非空；空键/空 method 拒。
        auto empty = InputRequired::Create({}, std::nullopt);
        CHECK(!empty);
        std::map<std::string, ClientInputRequest> requests;
        ClientInputRequest sampling;
        sampling.method = "sampling/createMessage";
        JsonValue sampling_params = JsonValue::Object();
        sampling_params.Set("messages", JsonValue::Array());
        sampling.params = sampling_params;
        requests.emplace("srv-1", sampling);
        auto with_map = InputRequired::Create(requests, std::nullopt);
        CHECK(with_map);
        auto with_state = InputRequired::Create({}, std::optional<std::string>("opaque"));
        CHECK(with_state);
        std::map<std::string, ClientInputRequest> bad_key;
        bad_key.emplace("", sampling);
        CHECK(!InputRequired::Create(bad_key, std::nullopt));
        std::map<std::string, ClientInputRequest> bad_method;
        ClientInputRequest no_method;
        no_method.method = "";
        bad_method.emplace("k", no_method);
        CHECK(!InputRequired::Create(bad_method, std::nullopt));
    }

    {
        // wire golden：id-keyed inputRequests（value 只含 method+params，无内层
        // id/jsonrpc）、requestState opaque 原值。
        std::map<std::string, ClientInputRequest> requests;
        ClientInputRequest sampling;
        sampling.method = "sampling/createMessage";
        JsonValue sampling_params = JsonValue::Object();
        JsonValue messages = JsonValue::Array();
        JsonValue message = JsonValue::Object();
        message.Set("role", JsonValue("user"));
        message.Set("content", JsonValue("hi"));
        messages.Push(std::move(message));
        sampling_params.Set("messages", std::move(messages));
        sampling.params = sampling_params;
        requests.emplace("srv-1", sampling);
        auto input_required = InputRequired::Create(requests,
                                                    std::optional<std::string>("st\u00e9-✈|{}"));
        CHECK(input_required);
        auto wire = MakeInputRequiredResult(input_required.value());
        CHECK(wire);
        JsonValue parsed = ParseOk(wire.value().Serialize());
        CHECK(Str(Find(parsed, "resultType")) == "input_required");
        const JsonValue input_requests = Find(parsed, "inputRequests").value_or(JsonValue());
        CHECK(input_requests.IsObject() && input_requests.Size() == 1);
        const JsonValue entry = Find(input_requests, "srv-1").value_or(JsonValue());
        CHECK(Str(Find(entry, "method")) == "sampling/createMessage");
        CHECK(Find(entry, "params") && !Find(entry, "id") && !Find(entry, "jsonrpc"));
        CHECK(Str(Find(parsed, "requestState")) == "st\u00e9-✈|{}");

        // 只有 requestState 的形态。
        auto state_only = InputRequired::Create({}, std::optional<std::string>("s"));
        auto wire2 = MakeInputRequiredResult(state_only.value());
        CHECK(wire2);
        JsonValue parsed2 = ParseOk(wire2.value().Serialize());
        CHECK(!Find(parsed2, "inputRequests"));
        CHECK(Str(Find(parsed2, "requestState")) == "s");
    }

    {
        // 能力门禁：缺失 → -32021 + data.requiredCapabilities；具备 → 通过。
        std::map<std::string, ClientInputRequest> requests;
        ClientInputRequest sampling;
        sampling.method = "sampling/createMessage";
        sampling.params = JsonValue::Object();
        requests.emplace("srv-1", sampling);

        auto denied = CheckInputRequestCapabilities(JsonValue::Object(), requests);
        CHECK(denied);
        CHECK(denied->code == JsonRpcErrorCode::MissingRequiredClientCapability);
        const JsonValue data = denied->data.value_or(JsonValue());
        const JsonValue required = Find(data, "requiredCapabilities").value_or(JsonValue());
        CHECK(required.IsObject() && Find(required, "sampling"));
        CHECK(Find(required, "sampling")->IsObject());

        JsonValue caps = JsonValue::Object();
        caps.Set("sampling", JsonValue::Object());
        CHECK(!CheckInputRequestCapabilities(caps, requests));

        // 显式 null 与缺失同义；elicitation/roots 各自独立门禁。
        std::map<std::string, ClientInputRequest> elicit;
        ClientInputRequest elicitation;
        elicitation.method = "elicitation/create";
        elicitation.params = JsonValue::Object();
        elicit.emplace("srv-2", elicitation);
        JsonValue null_sampling = JsonValue::Object();
        null_sampling.Set("sampling", JsonValue());
        auto denied_null = CheckInputRequestCapabilities(null_sampling, elicit);
        CHECK(denied_null &&
              denied_null->code == JsonRpcErrorCode::MissingRequiredClientCapability);
        JsonValue elicit_caps = JsonValue::Object();
        elicit_caps.Set("elicitation", JsonValue::Object());
        CHECK(!CheckInputRequestCapabilities(elicit_caps, elicit));

        std::map<std::string, ClientInputRequest> roots;
        ClientInputRequest roots_request;
        roots_request.method = "roots/list";
        roots_request.params = JsonValue::Object();
        roots.emplace("srv-3", roots_request);
        JsonValue roots_caps = JsonValue::Object();
        roots_caps.Set("roots", JsonValue::Object());
        CHECK(!CheckInputRequestCapabilities(roots_caps, roots));

        ClientInputRequest unknown;
        unknown.method = "mystery/invoke";
        unknown.params = JsonValue::Object();
        std::map<std::string, ClientInputRequest> bad;
        bad.emplace("srv-4", unknown);
        auto invalid = CheckInputRequestCapabilities(roots_caps, bad);
        CHECK(invalid && invalid->code == JsonRpcErrorCode::InvalidParams);
    }

    {
        // 重试经真实分派：新 id + inputResponses/requestState 到达 handler → complete。
        Rig rig = MakeRig();
        JsonValue params = JsonValue::Object();
        params.Set("uri", JsonValue("mem://a"));
        JsonValue responses = JsonValue::Object();
        JsonValue entry = JsonValue::Object();
        entry.Set("role", JsonValue("user"));
        responses.Set("srv-1", std::move(entry));
        params.Set("inputResponses", std::move(responses));
        params.Set("requestState", JsonValue("opaque-1"));
        JsonValue response = Respond(*rig.session,
                                     ModernFrame("100", "resources/read", std::move(params)));
        CHECK(rig.handler_calls == 1);
        CHECK(rig.seen_input_responses.count("srv-1") == 1);
        CHECK(rig.seen_input_responses.at("srv-1").IsObject());
        CHECK(rig.seen_request_state && *rig.seen_request_state == "opaque-1");
        const JsonValue result = Find(response, "result").value_or(JsonValue());
        CHECK(Str(Find(result, "resultType")) == "complete");

        // 新 id 重试（服务端无 modern 会话状态）：同载荷原样可达。
        JsonValue params2 = JsonValue::Object();
        params2.Set("uri", JsonValue("mem://a"));
        JsonValue responses2 = JsonValue::Object();
        JsonValue entry2 = JsonValue::Object();
        entry2.Set("role", JsonValue("assistant"));
        responses2.Set("srv-1", std::move(entry2));
        params2.Set("inputResponses", std::move(responses2));
        params2.Set("requestState", JsonValue("opaque-2"));
        JsonValue response2 = Respond(*rig.session,
                                      ModernFrame("101", "resources/read", std::move(params2)));
        CHECK(rig.handler_calls == 2);
        CHECK(rig.seen_request_state && *rig.seen_request_state == "opaque-2");
        CHECK(Str(Find(Find(response2, "result").value_or(JsonValue()), "resultType")) ==
              "complete");
    }

    {
        // 重试形状非法 → -32602，handler 计数为 0。
        Rig rig = MakeRig();
        JsonValue bad_entry = JsonValue::Object();
        bad_entry.Set("uri", JsonValue("mem://a"));
        JsonValue responses = JsonValue::Object();
        responses.Set("srv-1", JsonValue("not-an-object"));
        bad_entry.Set("inputResponses", std::move(responses));
        CHECK(ErrorCodeOf(*rig.session, ModernFrame("200", "resources/read",
                                                    std::move(bad_entry))) ==
              static_cast<std::int64_t>(JsonRpcErrorCode::InvalidParams));
        CHECK(rig.handler_calls == 0);

        JsonValue bad_state = JsonValue::Object();
        bad_state.Set("uri", JsonValue("mem://a"));
        bad_state.Set("requestState", JsonValue(static_cast<std::int64_t>(7)));
        CHECK(ErrorCodeOf(*rig.session, ModernFrame("201", "resources/read",
                                                    std::move(bad_state))) ==
              static_cast<std::int64_t>(JsonRpcErrorCode::InvalidParams));
        CHECK(rig.handler_calls == 0);

        JsonValue bad_map = JsonValue::Object();
        bad_map.Set("uri", JsonValue("mem://a"));
        bad_map.Set("inputResponses", JsonValue::Array());
        CHECK(ErrorCodeOf(*rig.session, ModernFrame("202", "resources/read",
                                                    std::move(bad_map))) ==
              static_cast<std::int64_t>(JsonRpcErrorCode::InvalidParams));
        CHECK(rig.handler_calls == 0);
    }

    {
        // legacy 不混入 modern 结构：带 inputResponses 的 legacy 请求原样可达 handler，
        // 上下文恒空，结果无 resultType/ttlMs。
        Rig rig = MakeRig();
        JsonValue init_params = JsonValue::Object();
        init_params.Set("protocolVersion", JsonValue("2025-11-25"));
        init_params.Set("capabilities", JsonValue::Object());
        JsonValue client_info = JsonValue::Object();
        client_info.Set("name", JsonValue("legacy"));
        client_info.Set("version", JsonValue("1"));
        init_params.Set("clientInfo", std::move(client_info));
        Respond(*rig.session, LegacyFrame("l0", "initialize", std::move(init_params)));

        JsonValue params = JsonValue::Object();
        params.Set("uri", JsonValue("mem://a"));
        JsonValue responses = JsonValue::Object();
        responses.Set("srv-1", JsonValue::Object());
        params.Set("inputResponses", std::move(responses));
        params.Set("requestState", JsonValue("opaque"));
        JsonValue response = Respond(*rig.session,
                                     LegacyFrame("l1", "resources/read", std::move(params)));
        CHECK(rig.handler_calls == 1);
        CHECK(rig.seen_input_responses.empty());
        CHECK(!rig.seen_request_state);
        const JsonValue result = Find(response, "result").value_or(JsonValue());
        CHECK(!Find(result, "resultType") && !Find(result, "ttlMs"));
        CHECK(!Find(result, "inputRequests"));  // legacy 不会收到 modern 结构
    }

    // ================= C1 回修回归：ack 窗口竞态（cancel/Close）=================
    {
        // 借复审探针 T6 的确定化写法：sink 睡 20ms 拉长「emplace → 锁外投递 ack →
        // 置 ack_sent」窗口，主线程 8ms 后 cancel/Close 命中窗口中段；fork 隔离崩溃
        //（C1 前该写法 cancel-race 6/6 SIGSEGV——operator[] 把 null 插回表内）。
        // 断言：cancel/Close 两变体各 20 次重复无 SIGSEGV、无异常退出，且事后
        // 发布（0 帧）/再取消（静默）证明表无污染。
        struct MutexLog {
            std::mutex m;
            std::vector<std::string> frames;
            void Record(const std::string& frame) {
                std::lock_guard<std::mutex> lock(m);
                frames.push_back(frame);
            }
        };
        std::string cancel_raw = [] {
            JsonValue params = JsonValue::Object();
            params.Set("requestId", JsonValue("race"));
            JsonValue frame = JsonValue::Object();
            frame.Set("jsonrpc", JsonValue("2.0"));
            frame.Set("method", JsonValue("notifications/cancelled"));
            frame.Set("params", std::move(params));
            return frame.Serialize();
        }();
        enum class RaceKind { kCancel, kClose };
        auto child = [&cancel_raw](RaceKind kind) -> int {
            ServerOptions options;
            options.max_inflight_requests = 64;
            auto server = std::move(McpServer::Create(std::move(options)).value());
            ResourceDefinition definition;
            definition.uri = "mem://a";
            definition.name = "a";
            auto handler = [](const RequestContext&, const std::string& uri)
                -> result<ResourceResult> {
                ResourceResult produced;
                auto contents = ResourceContents::Text(uri, "text/plain", "payload");
                produced.contents.push_back(std::move(contents).value());
                return result<ResourceResult>::ok(std::move(produced));
            };
            if (!server->AddResource(definition, handler)) return 2;
            if (!server->Start()) return 2;
            auto session = CreateSession(server);
            auto log = std::make_shared<MutexLog>();
            session->SetNotificationSink([log](const std::string& frame) {
                std::this_thread::sleep_for(std::chrono::milliseconds(20));  // 拉长窗口
                log->Record(frame);
            });
            std::thread listener([&] {
                (void)session->HandleFrame(ModernFrame(
                    "race", "subscriptions/listen", ListenParams([] {
                        JsonValue filter = JsonValue::Object();
                        JsonValue uris = JsonValue::Array();
                        uris.Push(JsonValue("mem://a"));
                        filter.Set("resourceSubscriptions", std::move(uris));
                        return filter;
                    }())));
            });
            std::this_thread::sleep_for(std::chrono::milliseconds(8));  // 落在窗口中段
            if (kind == RaceKind::kCancel)
                (void)session->HandleFrame(cancel_raw);
            else
                server->Close();
            listener.join();
            // 事后核验：无崩溃 + 表无污染。cancel 变体：订阅已结束 → 发布 0 帧；
            // close 变体：封口后再取消 → 静默（命中空表，绝不解引用悬垂条目）。
            if (kind == RaceKind::kCancel) {
                auto published = server->PublishResourceChanged("mem://a");
                if (!published || published.value() != 0) return 3;
            } else {
                (void)session->HandleFrame(cancel_raw);
            }
            return 0;
        };

        int segv = 0, other = 0;
        for (int rep = 0; rep < 20; ++rep) {
            for (RaceKind kind : {RaceKind::kCancel, RaceKind::kClose}) {
                pid_t pid = fork();
                if (pid == 0) _exit(child(kind));  // 子进程：崩溃隔离
                int status = 0;
                waitpid(pid, &status, 0);
                if (WIFSIGNALED(status) && WTERMSIG(status) == SIGSEGV)
                    ++segv;
                else if (!(WIFEXITED(status) && WEXITSTATUS(status) == 0))
                    ++other;
            }
        }
        CHECK(segv == 0);  // 40 次窗口竞态无一崩溃（C1 前必现）
        CHECK(other == 0);  // 表无污染：事后核验全通过
    }

    std::printf("mcp.subscribe: %d checks / %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
