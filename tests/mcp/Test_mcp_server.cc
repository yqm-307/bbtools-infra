// MCP transport-independent 核心契约测试（task 2-2 生命周期/内建方法分派切片）：
//   - McpServer Start 一次 / Close 幂等不重开 / 有界准入；
//   - modern `server/discover`（无握手、逐请求 `_meta` 校验、serverInfo stamp）；
//   - legacy `initialize`（两 revision 协商 + 反提议）→ `notifications/initialized`
//     → `ping`；
//   - 两 era 方法表隔离（modern 无 initialize/ping；legacy 无 server/discover）；
//   - 错误 id 原值关联、通知永不回包、会话按 channel 隔离。
//
// 全部经真实 core 入口 McpSession::HandleFrame（不直接调用底层 helper 冒充分派）。
// 无第三方测试框架依赖；经 ${PROJECT_SOURCE_DIR}/bbt 消费内部实现头（与仓内其他
// impl 层测试同口径，仅本目标可见）。

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>
#include <bbt/infra/mcp/McpTypes.hpp>

#include "mcp/detail/Server.hpp"

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

using bbt::infra::ErrorCode;
using bbt::infra::result;
using bbt::infra::mcp::CreateSession;
using bbt::infra::mcp::DispatchOutcome;
using bbt::infra::mcp::JsonValue;
using bbt::infra::mcp::McpServer;
using bbt::infra::mcp::McpSession;
using bbt::infra::mcp::ServerOptions;

// --- 小工具（读响应 JSON）-----------------------------------------------------

std::optional<JsonValue> Find(const JsonValue& value, const std::string& key) {
    auto found = value.Find(key);
    if (!found) return std::nullopt;
    return std::move(found).value();
}

JsonValue ParseOk(const std::string& text) {
    auto parsed = JsonValue::Parse(text, 1U << 20, 32);
    if (!parsed) {
        ++g_checks;
        ++g_failures;
        std::fprintf(stderr, "FAIL response is not valid JSON: %s\n", text.c_str());
        return JsonValue();
    }
    return std::move(parsed).value();
}

std::string Str(const std::optional<JsonValue>& value) {
    if (!value) return "<missing>";
    auto text = value->AsString();
    if (!text) return "<not-string>";
    return std::move(text).value();
}

// result<JsonValue> 重载：JsonValue::At 返回 result 而非 optional。
std::string Str(const result<JsonValue>& value) {
    if (!value) return "<missing>";
    auto text = value.value().AsString();
    if (!text) return "<not-string>";
    return std::move(text).value();
}

std::int64_t Int(const std::optional<JsonValue>& value) {
    if (!value) return -999999;
    auto number = value->AsInt64();
    if (!number) return -999999;
    return number.value();
}

// 解析一次 HandleFrame 的响应；断言必须回包。
JsonValue Respond(McpSession& session, const std::string& frame, int line) {
    auto outcome = session.HandleFrame(frame);
    ++g_checks;
    if (!outcome) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d  HandleFrame returned error\n", __FILE__, line);
        return JsonValue();
    }
    if (!outcome.value().has_response) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d  expected a response, got notification outcome\n", __FILE__,
                     line);
        return JsonValue();
    }
    return ParseOk(outcome.value().response);
}

// 断言一次 HandleFrame 不产生响应（合法通知）。
void ExpectNoResponse(McpSession& session, const std::string& frame, int line) {
    auto outcome = session.HandleFrame(frame);
    ++g_checks;
    if (!outcome) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d  HandleFrame returned error\n", __FILE__, line);
        return;
    }
    if (outcome.value().has_response) {
        ++g_failures;
        std::fprintf(stderr, "FAIL %s:%d  expected no response, got: %s\n", __FILE__, line,
                     outcome.value().response.c_str());
    }
}

// wire 错误码（-32700/-32600/-32602/-32601/-32022）。
std::int64_t ErrorCodeOf(const JsonValue& response) {
    auto error = Find(response, "error");
    if (!error) return 0;
    return Int(Find(*error, "code"));
}

// --- golden 帧（原始字串，独立于实现常量）------------------------------------

constexpr const char* kMetaVersionKey = "io.modelcontextprotocol/protocolVersion";
constexpr const char* kMetaCapsKey = "io.modelcontextprotocol/clientCapabilities";
constexpr const char* kServerInfoKey = "io.modelcontextprotocol/serverInfo";

// modern 请求：两必需命名空间键齐全。
const std::string kModernDiscover =
    R"({"jsonrpc":"2.0","id":1,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}})";

// legacy initialize（2025-11-25）。
const std::string kLegacyInit =
    R"({"jsonrpc":"2.0","id":2,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"cli","version":"1"},"protocolVersion":"2025-11-25"}})";

result<std::shared_ptr<McpServer>> StartedServer(ServerOptions options = ServerOptions{}) {
    auto created = McpServer::Create(std::move(options));
    if (!created) return created;
    std::shared_ptr<McpServer> server = std::move(created).value();
    auto started = server->Start();
    if (!started) return result<std::shared_ptr<McpServer>>::err(started.error());
    return result<std::shared_ptr<McpServer>>::ok(std::move(server));
}

// --- 1. 生命周期 / 配置校验 / 准入 -------------------------------------------

void TestLifecycleAndOptions() {
    ServerOptions bad;
    bad.max_inflight_requests = 0;
    CHECK(!McpServer::Create(bad));
    bad = ServerOptions{};
    bad.max_message_bytes = 0;
    CHECK(!McpServer::Create(bad));
    bad = ServerOptions{};
    bad.max_depth = 0;
    CHECK(!McpServer::Create(bad));
    bad = ServerOptions{};
    bad.server_name.clear();
    CHECK(!McpServer::Create(bad));
    bad = ServerOptions{};
    bad.server_version.clear();
    CHECK(!McpServer::Create(bad));
    bad = ServerOptions{};
    bad.server_name = std::string("\xFF\xFE", 2);  // 非法 UTF-8
    CHECK(!McpServer::Create(bad));
    bad = ServerOptions{};
    bad.instructions = std::string("\xC3\x28", 2);  // 非法 UTF-8 续字节
    CHECK(!McpServer::Create(bad));

    auto created = McpServer::Create(ServerOptions{});
    CHECK(created);
    auto server = std::move(created).value();
    CHECK(!server->IsStarted());
    CHECK(!server->IsClosed());
    CHECK(server->AdvertisedVersions().size() == 2);  // 2025-06-18 已移除

    auto session = CreateSession(server);
    CHECK(session != nullptr);
    // 未 Start：拒绝准入（transport 映射为不可用，而非协议错误）。
    auto early = session->HandleFrame(kModernDiscover);
    CHECK(!early);
    CHECK(early.error().code == ErrorCode::InvalidContext);
    CHECK(early.error().domain_code == "mcp.server.not_started");

    CHECK(server->Start());
    CHECK(server->IsStarted());
    auto twice = server->Start();
    CHECK(!twice);
    CHECK(twice.error().domain_code == "mcp.server.started");

    server->Close();
    CHECK(server->IsClosed());
    server->Close();  // 幂等
    CHECK(server->IsClosed());
    auto reopen = server->Start();  // 不重开
    CHECK(!reopen);
    CHECK(reopen.error().code == ErrorCode::Closed);
    auto after = session->HandleFrame(kModernDiscover);
    CHECK(!after);
    CHECK(after.error().code == ErrorCode::Closed);
}

// --- 2. modern discover ------------------------------------------------------

void TestModernDiscover() {
    auto started = StartedServer();
    CHECK(started);
    auto server = std::move(started).value();
    auto session = CreateSession(server);

    JsonValue response = Respond(*session, kModernDiscover, __LINE__);
    CHECK(Int(Find(response, "id")) == 1);
    CHECK(Str(Find(response, "jsonrpc")) == "2.0");
    CHECK(!Find(response, "error"));

    auto result_value = Find(response, "result");
    CHECK(result_value.has_value());
    CHECK(result_value->IsObject());

    // supportedVersions：真实服务列表（2 个——2025-06-18 已移除），顺序即对外声明顺序。
    auto versions = Find(*result_value, "supportedVersions");
    CHECK(versions.has_value() && versions->IsArray() && versions->Size() == 2);
    auto v0 = versions->At(0);
    auto v1 = versions->At(1);
    CHECK(static_cast<bool>(v0) && Str(v0) == "2026-07-28");
    CHECK(static_cast<bool>(v1) && Str(v1) == "2025-11-25");

    // modern 成功结果必含 resultType=complete + 固定缓存字段。
    CHECK(Str(Find(*result_value, "resultType")) == "complete");
    CHECK(Int(Find(*result_value, "ttlMs")) == 0);
    CHECK(Str(Find(*result_value, "cacheScope")) == "private");

    // 本切片未注册业务能力 → capabilities 是真实空 object（不宣告未实现能力）。
    auto capabilities = Find(*result_value, "capabilities");
    CHECK(capabilities.has_value() && capabilities->IsObject() && capabilities->Size() == 0);

    // discover 不缺 serverInfo（规范 SHOULD；modern 结果 stamp）。
    auto meta = Find(*result_value, "_meta");
    CHECK(meta.has_value() && meta->IsObject());
    auto server_info = Find(*meta, kServerInfoKey);
    CHECK(server_info.has_value() && server_info->IsObject());
    CHECK(Str(Find(*server_info, "name")) == "bbt-infra-mcp");
    CHECK(Str(Find(*server_info, "version")) == "0.1.0");

    // modern 无握手：会话状态完全未被触碰。
    CHECK(!session->InitializeAccepted());
    CHECK(!session->Initialized());
    CHECK(!session->NegotiatedVersion().has_value());
}

void TestModernMetaRejections() {
    auto started = StartedServer();
    auto server = std::move(started).value();
    auto session = CreateSession(server);

    // 半构造信封：只有 protocolVersion，缺 clientCapabilities → -32602（id 保留）。
    const std::string half =
        R"({"jsonrpc":"2.0","id":7,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28"}}})";
    JsonValue half_response = Respond(*session, half, __LINE__);
    CHECK(ErrorCodeOf(half_response) == -32602);
    CHECK(Int(Find(half_response, "id")) == 7);

    // protocolVersion 类型错（数字）→ -32602，不是 -32022。
    const std::string wrong_type =
        R"({"jsonrpc":"2.0","id":8,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":20260728,"io.modelcontextprotocol/clientCapabilities":{}}}})";
    JsonValue type_response = Respond(*session, wrong_type, __LINE__);
    CHECK(ErrorCodeOf(type_response) == -32602);
    CHECK(Int(Find(type_response, "id")) == 8);

    // clientCapabilities 非 object → -32602。
    const std::string caps_scalar =
        R"({"jsonrpc":"2.0","id":9,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":"nope"}}})";
    CHECK(ErrorCodeOf(Respond(*session, caps_scalar, __LINE__)) == -32602);

    // unsupported modern version → -32022，data.supported 只列真实服务版本，
    // data.requested 为原请求 string。
    const std::string bad_version =
        R"({"jsonrpc":"2.0","id":10,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"1900-01-01","io.modelcontextprotocol/clientCapabilities":{}}}})";
    JsonValue version_response = Respond(*session, bad_version, __LINE__);
    CHECK(ErrorCodeOf(version_response) == -32022);
    CHECK(Int(Find(version_response, "id")) == 10);
    auto error = Find(version_response, "error");
    CHECK(error.has_value());
    auto data = Find(*error, "data");
    CHECK(data.has_value() && data->IsObject());
    auto supported = Find(*data, "supported");
    CHECK(supported.has_value() && supported->IsArray() && supported->Size() == 2);
    CHECK(Str(Find(*data, "requested")) == "1900-01-01");

    // legacy revision 经规范命名空间键走 modern 路径 → -32022（两 era 严格隔离，
    // 服务版本列表含 legacy 不等于现代路径接受它）。
    const std::string legacy_via_modern =
        R"({"jsonrpc":"2.0","id":11,"method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2025-11-25","io.modelcontextprotocol/clientCapabilities":{}}}})";
    JsonValue legacy_response = Respond(*session, legacy_via_modern, __LINE__);
    CHECK(ErrorCodeOf(legacy_response) == -32022);
    auto legacy_error = Find(legacy_response, "error");
    auto legacy_data = legacy_error ? Find(*legacy_error, "data") : std::nullopt;
    CHECK(legacy_data.has_value() && Str(Find(*legacy_data, "requested")) == "2025-11-25");

    // 无 `_meta` 的 server/discover 在本切片路由为 legacy（无 transport 头判 era）
    // → legacy 表无该方法 → -32601。这是本切片的诚实边界（HTTP 头判 era 属 3-2）。
    const std::string no_meta =
        R"({"jsonrpc":"2.0","id":12,"method":"server/discover"})";
    JsonValue no_meta_response = Respond(*session, no_meta, __LINE__);
    CHECK(ErrorCodeOf(no_meta_response) == -32601);
}

// --- 3. 两 era 方法表隔离 -----------------------------------------------------

void TestMethodTableIsolation() {
    auto started = StartedServer();
    auto server = std::move(started).value();
    auto session = CreateSession(server);

    // modern 表没有 initialize。
    const std::string modern_init =
        R"({"jsonrpc":"2.0","id":21,"method":"initialize","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}},"capabilities":{},"clientInfo":{"name":"c","version":"1"},"protocolVersion":"2026-07-28"}})";
    JsonValue modern_init_response = Respond(*session, modern_init, __LINE__);
    CHECK(ErrorCodeOf(modern_init_response) == -32601);
    CHECK(!session->InitializeAccepted());  // 隔离：modern 帧不推进 legacy 状态

    // modern 表没有 ping。
    const std::string modern_ping =
        R"({"jsonrpc":"2.0","id":22,"method":"ping","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}})";
    CHECK(ErrorCodeOf(Respond(*session, modern_ping, __LINE__)) == -32601);

    // legacy 表没有 server/discover（无 `_meta` → 路由 legacy）。
    CHECK(ErrorCodeOf(Respond(*session, R"({"jsonrpc":"2.0","id":23,"method":"server/discover"})",
                              __LINE__)) == -32601);

    // modern 信封的 notifications/initialized：不回包，且不推进 legacy 状态。
    const std::string modern_initialized =
        R"({"jsonrpc":"2.0","method":"notifications/initialized","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}})";
    ExpectNoResponse(*session, modern_initialized, __LINE__);
    CHECK(!session->Initialized());
}

// --- 4. legacy initialize / initialized / ping -------------------------------

void TestLegacyLifecycle() {
    auto started = StartedServer();
    auto server = std::move(started).value();
    auto session = CreateSession(server);

    // ping 在握手前可用（SDK _INIT_EXEMPT），结果为空 object，无 modern 字段。
    const std::string ping = R"({"jsonrpc":"2.0","id":30,"method":"ping"})";
    JsonValue ping_response = Respond(*session, ping, __LINE__);
    CHECK(Int(Find(ping_response, "id")) == 30);
    auto ping_result = Find(ping_response, "result");
    CHECK(ping_result.has_value() && ping_result->IsObject() && ping_result->Size() == 0);
    CHECK(!Find(*ping_result, "resultType"));

    // 已服务 legacy revision 协商成功（2025-06-18 已移除出服务矩阵，见下）。
    {
        auto fresh = CreateSession(server);
        const std::string frame =
            std::string(R"({"jsonrpc":"2.0","id":31,"method":"initialize","params":{)") +
            R"("capabilities":{},"clientInfo":{"name":"c","version":"1"},"protocolVersion":"2025-11-25"}})";
        JsonValue response = Respond(*fresh, frame, __LINE__);
        auto result_value = Find(response, "result");
        CHECK(result_value.has_value());
        CHECK(Str(Find(*result_value, "protocolVersion")) == "2025-11-25");
        // legacy 结果不写 modern 专有字段。
        CHECK(!Find(*result_value, "resultType"));
        CHECK(!Find(*result_value, "ttlMs"));
        CHECK(!Find(*result_value, "cacheScope"));
        auto capabilities = Find(*result_value, "capabilities");
        CHECK(capabilities.has_value() && capabilities->IsObject() && capabilities->Size() == 0);
        auto server_info = Find(*result_value, "serverInfo");
        CHECK(server_info.has_value() && Str(Find(*server_info, "name")) == "bbt-infra-mcp");
        CHECK(fresh->InitializeAccepted());
        CHECK(fresh->NegotiatedVersion().has_value() &&
              fresh->NegotiatedVersion().value() == "2025-11-25");
        CHECK(!fresh->Initialized());  // 握手未完成（还没发 initialized 通知）
    }

    // 未服务的版本 → 反提议最新 legacy（2025-11-25），不宣称支持该版本。
    // 2025-06-18 曾是被服务的 legacy revision，现已移除——它的 initialize
    // 不再被采纳，与未知版本同走反提议路径。
    for (const char* requested : {"1999-01-01", "2025-03-26", "2025-06-18", "2026-07-28"}) {
        auto fresh = CreateSession(server);
        const std::string frame =
            std::string(R"({"jsonrpc":"2.0","id":32,"method":"initialize","params":{)") +
            R"("capabilities":{},"clientInfo":{"name":"c","version":"1"},"protocolVersion":")" +
            requested + R"("}})";
        JsonValue response = Respond(*fresh, frame, __LINE__);
        auto result_value = Find(response, "result");
        CHECK(result_value.has_value());
        CHECK(Str(Find(*result_value, "protocolVersion")) == "2025-11-25");
        CHECK(fresh->NegotiatedVersion().value() == "2025-11-25");
    }

    // notifications/initialized：不回包，推进 legacy 状态。
    auto ready = CreateSession(server);
    JsonValue init_response = Respond(*ready, kLegacyInit, __LINE__);
    CHECK(Find(init_response, "result").has_value());
    CHECK(ready->InitializeAccepted());
    ExpectNoResponse(*ready, R"({"jsonrpc":"2.0","method":"notifications/initialized"})", __LINE__);
    CHECK(ready->Initialized());
    CHECK(ready->NegotiatedVersion().value() == "2025-11-25");

    // 重复 initialize：SDK 语义为再次协商（确定行为，不报错）。
    JsonValue again = Respond(*ready, kLegacyInit, __LINE__);
    CHECK(Find(again, "result").has_value());
    CHECK(Str(Find(*Find(again, "result"), "protocolVersion")) == "2025-11-25");
}

// F6：legacy initialize 的 capabilities 按本次 negotiated/反提议 revision 校验；
// 非法 initialize 不推进/覆盖 accepted/version/caps，错误 id 原值保真。
void TestLegacyInitializeCapabilitiesRevision() {
    auto started = StartedServer();
    auto server = std::move(started).value();

    auto init_frame = [](const char* id, const char* version, const char* caps) {
        return std::string(R"({"jsonrpc":"2.0","id":)") + id +
               R"(,"method":"initialize","params":{"capabilities":)" + caps +
               R"(,"clientInfo":{"name":"c","version":"1"},"protocolVersion":")" + version +
               R"("}})";
    };

    // 2025-11-25 profile：sampling.context 须 object → -32602，状态不推进。
    //（2025-06-18 宽松 profile 已随该版本从服务矩阵移除——不再有「子字段自由」
    // 的第二套 shape。）
    auto nov = CreateSession(server);
    JsonValue nov_bad = Respond(
        *nov, init_frame("\"nov-bad\"", "2025-11-25", R"({"sampling":{"context":"bad"}})"),
        __LINE__);
    CHECK(ErrorCodeOf(nov_bad) == -32602);
    CHECK(Str(Find(nov_bad, "id")) == "nov-bad");
    CHECK(!nov->InitializeAccepted());
    CHECK(!nov->NegotiatedVersion().has_value());
    CHECK(nov->ClientCapabilities().Size() == 0);

    // 不支持版本 → 反提议 2025-11-25 并按该 profile 校验：tasks.list 非 object → 拒。
    auto counter = CreateSession(server);
    JsonValue counter_bad = Respond(
        *counter, init_frame("\"c1\"", "2025-03-26", R"({"tasks":{"list":"bad"}})"), __LINE__);
    CHECK(ErrorCodeOf(counter_bad) == -32602);
    CHECK(!counter->InitializeAccepted());
    CHECK(!counter->NegotiatedVersion().has_value());

    // 同一不支持版本 + 合法 11 月 caps → 反提议成功。
    auto counter_ok = CreateSession(server);
    JsonValue counter_good = Respond(
        *counter_ok, init_frame("\"c2\"", "2025-03-26", R"({"tasks":{"list":{}}})"), __LINE__);
    CHECK(Find(counter_good, "result").has_value());
    CHECK(counter_ok->NegotiatedVersion().value() == "2025-11-25");

    // 已握手会话收到非法 initialize：-32602 且不覆盖既有 accepted/version/caps。
    auto existing = CreateSession(server);
    CHECK(Find(Respond(*existing, init_frame("\"e1\"", "2025-11-25", "{}"), __LINE__), "result")
              .has_value());
    const std::string before = existing->ClientCapabilities().Serialize();
    JsonValue overwrite = Respond(
        *existing, init_frame("\"e2\"", "2025-11-25", R"({"sampling":{"context":"bad"}})"), __LINE__);
    CHECK(ErrorCodeOf(overwrite) == -32602);
    CHECK(existing->InitializeAccepted());  // 非法 initialize 不撤销既有握手
    CHECK(existing->NegotiatedVersion().has_value() &&
          existing->NegotiatedVersion().value() == "2025-11-25");
    CHECK(existing->ClientCapabilities().Serialize() == before);

    // 拒绝后会话仍可用（ping 豁免；无注册 → 业务方法走 -32601 门禁）。
    JsonValue ping = Respond(*nov, R"({"jsonrpc":"2.0","id":"p1","method":"ping"})", __LINE__);
    CHECK(ErrorCodeOf(ping) == 0);
}

void TestLegacyInitializeRejections() {
    auto started = StartedServer();
    auto server = std::move(started).value();
    auto session = CreateSession(server);

    // 缺 clientInfo → -32602，id 原值保留。
    const std::string no_client_info =
        R"({"jsonrpc":"2.0","id":40,"method":"initialize","params":{"capabilities":{},"protocolVersion":"2025-11-25"}})";
    JsonValue response = Respond(*session, no_client_info, __LINE__);
    CHECK(ErrorCodeOf(response) == -32602);
    CHECK(Int(Find(response, "id")) == 40);
    CHECK(!session->InitializeAccepted());  // 失败不推进状态

    // 缺 capabilities。
    const std::string no_caps =
        R"({"jsonrpc":"2.0","id":41,"method":"initialize","params":{"clientInfo":{},"protocolVersion":"2025-11-25"}})";
    CHECK(ErrorCodeOf(Respond(*session, no_caps, __LINE__)) == -32602);

    // 缺 protocolVersion。
    const std::string no_version =
        R"({"jsonrpc":"2.0","id":42,"method":"initialize","params":{"capabilities":{},"clientInfo":{}}})";
    CHECK(ErrorCodeOf(Respond(*session, no_version, __LINE__)) == -32602);

    // protocolVersion 类型错。
    const std::string bad_version =
        R"({"jsonrpc":"2.0","id":43,"method":"initialize","params":{"capabilities":{},"clientInfo":{},"protocolVersion":20251125}})";
    CHECK(ErrorCodeOf(Respond(*session, bad_version, __LINE__)) == -32602);

    // params 形状错（标量）→ -32602，id 保留。
    const std::string scalar_params =
        R"({"jsonrpc":"2.0","id":44,"method":"initialize","params":3})";
    JsonValue scalar_response = Respond(*session, scalar_params, __LINE__);
    CHECK(ErrorCodeOf(scalar_response) == -32602);
    CHECK(Int(Find(scalar_response, "id")) == 44);
    CHECK(!session->InitializeAccepted());

    // F1：clientInfo 的必需 name/version 必须为 string（固定 schema
    // Implementation: name:string + version:string）。缺任一或类型错 → -32602，
    // id 原值保留，且失败不推进会话状态。
    const std::string info_empty =
        R"({"jsonrpc":"2.0","id":45,"method":"initialize","params":{"capabilities":{},"clientInfo":{},"protocolVersion":"2025-11-25"}})";
    JsonValue info_empty_response = Respond(*session, info_empty, __LINE__);
    CHECK(ErrorCodeOf(info_empty_response) == -32602);
    CHECK(Int(Find(info_empty_response, "id")) == 45);
    CHECK(!session->InitializeAccepted());
    CHECK(!session->NegotiatedVersion().has_value());

    // 缺 version。
    const std::string info_no_version =
        R"({"jsonrpc":"2.0","id":46,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"c"},"protocolVersion":"2025-11-25"}})";
    JsonValue info_no_version_response = Respond(*session, info_no_version, __LINE__);
    CHECK(ErrorCodeOf(info_no_version_response) == -32602);
    CHECK(Int(Find(info_no_version_response, "id")) == 46);

    // 缺 name。
    const std::string info_no_name =
        R"({"jsonrpc":"2.0","id":47,"method":"initialize","params":{"capabilities":{},"clientInfo":{"version":"1"},"protocolVersion":"2025-11-25"}})";
    CHECK(ErrorCodeOf(Respond(*session, info_no_name, __LINE__)) == -32602);

    // name 类型错（数字）。
    const std::string info_name_type =
        R"({"jsonrpc":"2.0","id":48,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":3,"version":"1"},"protocolVersion":"2025-11-25"}})";
    CHECK(ErrorCodeOf(Respond(*session, info_name_type, __LINE__)) == -32602);

    // version 类型错（数字）。
    const std::string info_version_type =
        R"({"jsonrpc":"2.0","id":49,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"c","version":3},"protocolVersion":"2025-11-25"}})";
    CHECK(ErrorCodeOf(Respond(*session, info_version_type, __LINE__)) == -32602);
    CHECK(!session->InitializeAccepted());  // 全部失败后仍未推进

    // 空字符串是合法 string（schema 未要求非空）→ 接受并推进状态（不擅自加 nonempty）。
    auto empty_strings = CreateSession(server);
    const std::string info_empty_strings =
        R"({"jsonrpc":"2.0","id":50,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"","version":""},"protocolVersion":"2025-11-25"}})";
    CHECK(Find(Respond(*empty_strings, info_empty_strings, __LINE__), "result").has_value());
    CHECK(empty_strings->InitializeAccepted());

    // 已成功协商后，错误 clientInfo 的再次 initialize 不得破坏原状态（原状态完整保留）。
    auto negotiated = CreateSession(server);
    CHECK(Find(Respond(*negotiated, kLegacyInit, __LINE__), "result").has_value());
    CHECK(negotiated->InitializeAccepted());
    CHECK(negotiated->NegotiatedVersion().value() == "2025-11-25");
    const std::string bad_reinit =
        R"({"jsonrpc":"2.0","id":51,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"c"},"protocolVersion":"2025-06-18"}})";
    JsonValue bad_reinit_response = Respond(*negotiated, bad_reinit, __LINE__);
    CHECK(ErrorCodeOf(bad_reinit_response) == -32602);
    CHECK(Int(Find(bad_reinit_response, "id")) == 51);
    // 原状态未被清空、未被改成失败帧提议的版本（clientInfo 缺 version 在
    // params 校验即拒，先于版本协商——版本提议本身不再可达）。
    CHECK(negotiated->InitializeAccepted());
    CHECK(negotiated->NegotiatedVersion().has_value() &&
          negotiated->NegotiatedVersion().value() == "2025-11-25");
}

// --- 5. 错误 id / 通知 / 帧拒绝 ----------------------------------------------

void TestErrorAndNotificationHandling() {
    auto started = StartedServer();
    auto server = std::move(started).value();
    auto session = CreateSession(server);

    // parse error → -32700，id null（无法确定 id）。
    JsonValue parse_error = Respond(*session, "{", __LINE__);
    CHECK(ErrorCodeOf(parse_error) == -32700);
    auto null_id = Find(parse_error, "id");
    CHECK(null_id.has_value() && null_id->IsNull());

    // batch（顶层数组）→ -32600。
    CHECK(ErrorCodeOf(Respond(*session, "[]", __LINE__)) == -32600);

    // 非法信封：缺 jsonrpc → -32600。
    CHECK(ErrorCodeOf(Respond(*session, R"({"id":50,"method":"ping"})", __LINE__)) == -32600);

    // id 已确定 + params 形状错 → -32602 且 id 原值回包（关联在途请求）。
    const std::string known_id_bad_params =
        R"({"jsonrpc":"2.0","id":77,"method":"ping","params":3})";
    JsonValue known_response = Respond(*session, known_id_bad_params, __LINE__);
    CHECK(ErrorCodeOf(known_response) == -32602);
    CHECK(Int(Find(known_response, "id")) == 77);

    // 字符串 id 原值回包（精确身份）。
    const std::string string_id = R"({"jsonrpc":"2.0","id":"req-77","method":"ping"})";
    JsonValue string_response = Respond(*session, string_id, __LINE__);
    CHECK(Str(Find(string_response, "id")) == "req-77");

    // 超过 2^53 的整数 id 原值保留。
    const std::string big_id =
        R"({"jsonrpc":"2.0","id":9007199254740993,"method":"ping"})";
    JsonValue big_response = Respond(*session, big_id, __LINE__);
    CHECK(Int(Find(big_response, "id")) == 9007199254740993LL);

    // 未知方法（有 id）→ -32601，id 保留。
    JsonValue unknown = Respond(*session, R"({"jsonrpc":"2.0","id":60,"method":"foo/bar"})", __LINE__);
    CHECK(ErrorCodeOf(unknown) == -32601);
    CHECK(Int(Find(unknown, "id")) == 60);

    // 合法通知（无 id）永不回包 —— 含未知方法与 legacy initialized。
    ExpectNoResponse(*session, R"({"jsonrpc":"2.0","method":"foo/bar"})", __LINE__);
    ExpectNoResponse(*session, R"({"jsonrpc":"2.0","method":"notifications/initialized"})", __LINE__);
    ExpectNoResponse(*session, R"({"jsonrpc":"2.0","method":"notifications/cancelled"})", __LINE__);

    // 无 id 的 initialize（被当作通知）→ 不回包；但仍是「合法通知」，不产生响应。
    ExpectNoResponse(*session,
                     R"({"jsonrpc":"2.0","method":"initialize","params":{"capabilities":{},"clientInfo":{},"protocolVersion":"2025-11-25"}})",
                     __LINE__);

    // 超大帧 → -32700（mcp.json.max_bytes）。
    ServerOptions tiny;
    tiny.max_message_bytes = 64;
    auto small_server = StartedServer(tiny).value();
    auto small_session = CreateSession(small_server);
    const std::string huge = std::string(R"({"jsonrpc":"2.0","id":70,"method":"ping","pad":")") +
                             std::string(200, 'x') + R"("})";
    CHECK(ErrorCodeOf(Respond(*small_session, huge, __LINE__)) == -32700);

    // 超深帧 → -32700（mcp.json.max_depth）。
    ServerOptions shallow;
    shallow.max_depth = 4;
    auto shallow_server = StartedServer(shallow).value();
    auto shallow_session = CreateSession(shallow_server);
    const std::string deep =
        R"({"jsonrpc":"2.0","id":71,"method":"ping","params":[[[[[[[[1]]]]]]]]})";
    CHECK(ErrorCodeOf(Respond(*shallow_session, deep, __LINE__)) == -32700);
}

// --- 5b. 通知 params 非法：静默丢弃（JSON-RPC 2.0 §4.1 通知不回包）-------------

void TestNotificationParamsDropped() {
    auto started = StartedServer();
    auto server = std::move(started).value();
    auto session = CreateSession(server);

    // F2：无 id 且 params 形状非法（数字/字符串/null）→ 不回包（合法通知不回包），
    // 且不得推进任何会话状态——尤其 notifications/initialized 带坏 params 时
    // Initialized() 必须保持 false（非法 params 不是可执行通知）。
    ExpectNoResponse(*session,
                     R"({"jsonrpc":"2.0","method":"notifications/initialized","params":3})",
                     __LINE__);
    CHECK(!session->Initialized());
    ExpectNoResponse(*session,
                     R"({"jsonrpc":"2.0","method":"notifications/initialized","params":"x"})",
                     __LINE__);
    CHECK(!session->Initialized());
    ExpectNoResponse(*session,
                     R"({"jsonrpc":"2.0","method":"notifications/initialized","params":null})",
                     __LINE__);
    CHECK(!session->Initialized());
    ExpectNoResponse(*session, R"({"jsonrpc":"2.0","method":"notifications/cancelled","params":3})",
                     __LINE__);
    CHECK(!session->InitializeAccepted());

    // 对照：params 合法（缺省/object）的同一通知 → 仍不回包且推进状态。
    ExpectNoResponse(*session, R"({"jsonrpc":"2.0","method":"notifications/initialized"})",
                     __LINE__);
    CHECK(session->Initialized());

    // 有 id 的同一帧仍是请求：params 形状非法 → -32602 且 id 原值回包。
    JsonValue with_id =
        Respond(*session, R"({"jsonrpc":"2.0","id":77,"method":"ping","params":3})", __LINE__);
    CHECK(ErrorCodeOf(with_id) == -32602);
    CHECK(Int(Find(with_id, "id")) == 77);

    // 非法 method（method:1）且无 id：仍属不构成请求的帧 → -32600，id null。
    JsonValue bad_method = Respond(*session, R"({"jsonrpc":"2.0","method":1,"params":3})", __LINE__);
    CHECK(ErrorCodeOf(bad_method) == -32600);
    auto bad_method_id = Find(bad_method, "id");
    CHECK(bad_method_id.has_value() && bad_method_id->IsNull());

    // 无 id 的 parse 坏帧 → 仍 -32700，id null（不被当作「通知」吞掉）。
    JsonValue parse_error = Respond(*session, "{", __LINE__);
    CHECK(ErrorCodeOf(parse_error) == -32700);
    auto parse_id = Find(parse_error, "id");
    CHECK(parse_id.has_value() && parse_id->IsNull());

    // 无 id 且 result 混入（response 伪装成 request）→ -32600，id null（不分类为通知）。
    JsonValue with_result = Respond(
        *session, R"({"jsonrpc":"2.0","method":"m","params":3,"result":{}})", __LINE__);
    CHECK(ErrorCodeOf(with_result) == -32600);

    // 连续调用：通知分类与 id 不跨帧泄漏（golden regression）。
    auto fresh = CreateSession(server);
    ExpectNoResponse(*fresh,
                     R"({"jsonrpc":"2.0","method":"notifications/initialized","params":3})",
                     __LINE__);
    CHECK(!fresh->Initialized());  // (1) 坏 params 未推进状态
    JsonValue e1 = Respond(*fresh, "{", __LINE__);  // (2) 分类不得泄漏：parse 坏仍回 -32700
    CHECK(ErrorCodeOf(e1) == -32700);
    auto e1_id = Find(e1, "id");
    CHECK(e1_id.has_value() && e1_id->IsNull());
    JsonValue r1 =
        Respond(*fresh, R"({"jsonrpc":"2.0","id":91,"method":"ping","params":3})", __LINE__);
    CHECK(ErrorCodeOf(r1) == -32602);  // (3) 有 id 仍原值回包
    CHECK(Int(Find(r1, "id")) == 91);
    ExpectNoResponse(*fresh, R"({"jsonrpc":"2.0","method":"notifications/cancelled","params":null})",
                     __LINE__);  // (4) id 已清，仍丢弃
    JsonValue r2 = Respond(*fresh, R"({"jsonrpc":"2.0","id":92,"method":"ping"})", __LINE__);
    CHECK(Int(Find(r2, "id")) == 92);  // (5) 后续合法请求正常回包
    CHECK(Find(r2, "result").has_value());
}

// --- 6. 会话按 channel 隔离 ---------------------------------------------------

void TestSessionIsolation() {
    auto started = StartedServer();
    auto server = std::move(started).value();
    auto alpha = CreateSession(server);
    auto beta = CreateSession(server);

    CHECK(Find(Respond(*alpha, kLegacyInit, __LINE__), "result").has_value());
    ExpectNoResponse(*alpha, R"({"jsonrpc":"2.0","method":"notifications/initialized"})", __LINE__);
    CHECK(alpha->InitializeAccepted());
    CHECK(alpha->Initialized());

    // beta 未被 alpha 的 legacy 握手污染（不是全局 legacy flag）。
    CHECK(!beta->InitializeAccepted());
    CHECK(!beta->Initialized());
    CHECK(!beta->NegotiatedVersion().has_value());

    // beta 独立协商：提议 2025-06-18 不再被采纳（已从服务矩阵移除）——
    // 反提议 2025-11-25。隔离断言不变：beta 的结果不影响 alpha。
    const std::string beta_init =
        R"({"jsonrpc":"2.0","id":80,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"beta","version":"1"},"protocolVersion":"2025-06-18"}})";
    CHECK(Find(Respond(*beta, beta_init, __LINE__), "result").has_value());
    CHECK(beta->NegotiatedVersion().value() == "2025-11-25");
    CHECK(alpha->NegotiatedVersion().value() == "2025-11-25");  // alpha 不受影响

    // 另一 server 实例也完全独立。
    auto other = StartedServer().value();
    auto gamma = CreateSession(other);
    CHECK(!gamma->InitializeAccepted());
    CHECK(gamma->HandleFrame(kModernDiscover));
    CHECK(!gamma->Initialized());
}

// --- 7. 有界准入：并发下无泄漏 / 无串味 / 上界真实生效 -------------------------

// 每线程跑 kPerThread 帧 modern discover；返回 (成功数, Overloaded 数, 其它错误数)。
struct AdmitCounts {
    int ok = 0;
    int overloaded = 0;
    int wrong = 0;
};

AdmitCounts RunConcurrent(const std::shared_ptr<McpServer>& server, int threads, int per_thread) {
    std::vector<std::thread> workers;
    std::vector<AdmitCounts> counts(static_cast<std::size_t>(threads));
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([&, t]() {
            auto session = CreateSession(server);
            for (int i = 0; i < per_thread; ++i) {
                auto outcome = session->HandleFrame(kModernDiscover);
                if (!outcome) {
                    // 唯一允许的核心拒绝是「超过在途上限」；其它错误是真实缺陷。
                    if (outcome.error().code == ErrorCode::Overloaded)
                        ++counts[t].overloaded;
                    else
                        ++counts[t].wrong;
                    continue;
                }
                if (!outcome.value().has_response) {
                    ++counts[t].wrong;
                    continue;
                }
                JsonValue response = ParseOk(outcome.value().response);
                auto result_value = Find(response, "result");
                if (Int(Find(response, "id")) != 1 || !result_value ||
                    Str(Find(*result_value, "resultType")) != "complete")
                    ++counts[t].wrong;
                else
                    ++counts[t].ok;
            }
        });
    }
    for (auto& worker : workers) worker.join();
    AdmitCounts total;
    for (int t = 0; t < threads; ++t) {
        CHECK(counts[t].wrong == 0);
        CHECK(counts[t].ok + counts[t].overloaded == per_thread);
        total.ok += counts[t].ok;
        total.overloaded += counts[t].overloaded;
        total.wrong += counts[t].wrong;
    }
    return total;
}

void TestConcurrentAdmission() {
    // 阶段 A：上界 = 1。并发调用必然出现「成功或 Overloaded」二择一，绝不允许
    // 第三种结果（无泄漏、无串味、无半份响应）。这同时真实触发 Overloaded 路径。
    ServerOptions tight;
    tight.max_inflight_requests = 1;
    auto tight_server = StartedServer(tight).value();
    AdmitCounts tight_counts = RunConcurrent(tight_server, 4, 50);
    std::printf("mcp.server: admission(max_inflight=1): ok=%d overloaded=%d wrong=%d\n",
                tight_counts.ok, tight_counts.overloaded, tight_counts.wrong);
    CHECK(tight_counts.wrong == 0);
    // 并发后准入名额已归还：同一 server 仍能正常接纳。
    auto session = CreateSession(tight_server);
    CHECK(session->HandleFrame(kModernDiscover));
    tight_server->Close();
    auto after_close = session->HandleFrame(kModernDiscover);
    CHECK(!after_close && after_close.error().code == ErrorCode::Closed);

    // 阶段 B：上界远大于并发度（64 > 4 线程）→ 全部成功，无伪 Overloaded。
    ServerOptions roomy;
    roomy.max_inflight_requests = 64;
    auto roomy_server = StartedServer(roomy).value();
    AdmitCounts roomy_counts = RunConcurrent(roomy_server, 4, 50);
    std::printf("mcp.server: admission(max_inflight=64): ok=%d overloaded=%d wrong=%d\n",
                roomy_counts.ok, roomy_counts.overloaded, roomy_counts.wrong);
    CHECK(roomy_counts.ok == 200);
    CHECK(roomy_counts.overloaded == 0);
    CHECK(roomy_counts.wrong == 0);
}

// --- 7b. 同一 session 并发驱动：状态一致（F3 同步回归）------------------------

void TestSameSessionConcurrentState() {
    auto started = StartedServer();
    auto server = std::move(started).value();

    // 有限同会话并发：两个线程在显式 barrier 后同时驱动**同一个** McpSession，
    // 各自提交 legacy 状态转移，随后断言状态整体一致（未丢写 / 未撕裂），而不是
    // 仅「不崩溃」。注意：本用例**不是**「无数据竞争」的证明——race 的存在与消除
    // 由源码级完整同步（m_state_mutex 覆盖三字段全部读写）保证；测试只覆盖同一
    // session 被并发驱动时可观察状态不被破坏。不使用 sleep / 压力 / TSan。
    constexpr int kRounds = 32;
    for (int round = 0; round < kRounds; ++round) {
        auto shared = CreateSession(server);
        std::atomic<int> ready{0};
        auto barrier = [&ready]() {
            ready.fetch_add(1, std::memory_order_acq_rel);
            while (ready.load(std::memory_order_acquire) < 2) std::this_thread::yield();
        };
        const std::string init_a =
            R"({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"a","version":"1"},"protocolVersion":"2025-11-25"}})";
        const std::string init_b =
            R"({"jsonrpc":"2.0","id":2,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"b","version":"1"},"protocolVersion":"2025-06-18"}})";
        std::thread t1([&]() {
            barrier();
            shared->HandleFrame(init_a);
        });
        std::thread t2([&]() {
            barrier();
            shared->HandleFrame(init_b);
        });
        t1.join();
        t2.join();

        // 两个并发 initialize 都被接受（重复 init 重新协商是既定行为）；协商版本
        // 恒为唯一已服务 legacy revision（2025-06-18 的提议会被反提议，不落
        // 2025-06-18），状态字段整体一致、无空值/撕裂值。
        CHECK(shared->InitializeAccepted());
        std::optional<std::string> version = shared->NegotiatedVersion();
        CHECK(version.has_value());
        if (version) CHECK(*version == "2025-11-25");
    }
}

} // namespace

int main() {
    TestLifecycleAndOptions();
    TestModernDiscover();
    TestModernMetaRejections();
    TestMethodTableIsolation();
    TestLegacyLifecycle();
    TestLegacyInitializeRejections();
    TestLegacyInitializeCapabilitiesRevision();
    TestErrorAndNotificationHandling();
    TestNotificationParamsDropped();
    TestSessionIsolation();
    TestConcurrentAdmission();
    TestSameSessionConcurrentState();

    std::printf("mcp.server: %d checks, %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
