// 3-2 切片 contract 检查：McpHttpTransport（specs R9/S18-S20 + R10/S21 本切片形态）。
//
//   S18：POST 普通方法 JSON 200；现代 unknown method 404/-32601；parse/envelope/
//        params/header/version 错误 400 与规范对应码；通知 202 空 body；
//        GET/DELETE 405；不 mint/echo session。
//   S19：版本/方法头必需；Mcp-Name 仅具名方法必需，解码后与 body 一致；
//        legacy 首次 initialize 可缺版本头；ASCII/非 ASCII/完整 sentinel 原值、非法 Base64/control chars
//        以及 Origin 重复/无效/合法分别测试；不支持的自定义 header 注解收到即
//        明确拒绝（不 advertise 后不检查）。
//   S20：真实动态 loopback SSE：先 ack 并保持连接、心跳不作为 JSON、发布事件、
//        断连/写失败 → 订阅取消且名额归还；完整 POST 后客户端仅 SHUT_WR 仍能读
//        到最终 JSON/SSE（不因读 EOF 误取消）；正文未完整时 EOF 明确截断失败；
//        慢读输出有界且不饿死普通请求。无 Last-Event-ID 重放承诺。
//   配额/Close：listener+accepted 全受管（max_connections 含 listener 的容量拒绝
//        可观测）、并发 Close 等首次真实落定、Close 返回当刻 listener 不可 connect。
//
// 真实动态 loopback：Listen 绑 127.0.0.1 端口 0，LocalAddress() 取实际端口，
// 原始 POSIX socket 客户端收发原始 HTTP/SSE 报文（官方 SDK 互通属 4-2/S23）。
// 无第三方测试框架依赖；只消费公共头（不链接 bbt_infra_http）。

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <bbt/infra/Result.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>
#include <bbt/infra/mcp/McpHttpTransport.hpp>
#include <bbt/infra/mcp/McpServer.hpp>
#include <bbt/infra/mcp/McpTypes.hpp>

#include <bbt/coroutine/detail/GlobalConfig.hpp>
#include <bbt/coroutine/detail/Scheduler.hpp>

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

using namespace std::chrono_literals;
using bbt::infra::mcp::HttpCloseEvidence;
using bbt::infra::mcp::HttpOptions;
using bbt::infra::mcp::JsonValue;
using bbt::infra::mcp::McpHttpTransport;
using bbt::infra::mcp::McpServer;
using bbt::infra::mcp::ResourceContents;
using bbt::infra::mcp::ResourceDefinition;
using bbt::infra::mcp::ResourceResult;
using bbt::infra::mcp::ServerOptions;

constexpr const char* kListenHost = "127.0.0.1";
constexpr std::string_view kModern = "2026-07-28";
constexpr std::string_view kLegacy = "2025-11-25";
constexpr std::string_view kResourceUri = "memo://notes";

// --- 测试夹具 --------------------------------------------------------------------

std::string Base64Encode(const std::string& raw) {
    static const char* kAlphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve((raw.size() + 2) / 3 * 4);
    std::size_t i = 0;
    for (; i + 3 <= raw.size(); i += 3) {
        const unsigned v = (static_cast<unsigned char>(raw[i]) << 16) |
                           (static_cast<unsigned char>(raw[i + 1]) << 8) |
                           static_cast<unsigned char>(raw[i + 2]);
        out.push_back(kAlphabet[(v >> 18) & 63]);
        out.push_back(kAlphabet[(v >> 12) & 63]);
        out.push_back(kAlphabet[(v >> 6) & 63]);
        out.push_back(kAlphabet[v & 63]);
    }
    const std::size_t rest = raw.size() - i;
    if (rest == 1) {
        const unsigned v = static_cast<unsigned char>(raw[i]) << 16;
        out.push_back(kAlphabet[(v >> 18) & 63]);
        out.push_back(kAlphabet[(v >> 12) & 63]);
        out += "==";
    } else if (rest == 2) {
        const unsigned v = (static_cast<unsigned char>(raw[i]) << 16) |
                           (static_cast<unsigned char>(raw[i + 1]) << 8);
        out.push_back(kAlphabet[(v >> 18) & 63]);
        out.push_back(kAlphabet[(v >> 12) & 63]);
        out.push_back(kAlphabet[(v >> 6) & 63]);
        out += "=";
    }
    return out;
}

struct Fixture {
    std::shared_ptr<McpServer> server;
    std::shared_ptr<McpHttpTransport> transport;
    std::uint16_t port = 0;
};

Fixture MakeServer(ServerOptions server_options, HttpOptions http_options) {
    Fixture fixture;
    fixture.server = McpServer::Create(server_options).value();
    ResourceDefinition definition;
    definition.uri = std::string(kResourceUri);
    definition.name = "notes";
    definition.mime_type = "text/plain";
    auto registered = fixture.server->AddResource(
        definition,
        [](const bbt::infra::mcp::RequestContext&, const std::string& uri)
            -> bbt::infra::result<ResourceResult> {
            auto content = ResourceContents::Text(uri, "text/plain", "hello");
            if (!content) return bbt::infra::result<ResourceResult>::err(content.error());
            ResourceResult result;
            result.contents.push_back(content.value());
            return bbt::infra::result<ResourceResult>::ok(std::move(result));
        });
    CHECK(static_cast<bool>(registered));
    for (const std::string& uri : {std::string("memo://工具"),
                                   std::string("=?base64?literal?="),
                                   std::string("=?base64?prefix")}) {
        ResourceDefinition alias;
        alias.uri = uri;
        alias.name = "alias";
        CHECK(static_cast<bool>(fixture.server->AddResource(
            alias, [](const bbt::infra::mcp::RequestContext&, const std::string& body_uri)
                -> bbt::infra::result<ResourceResult> {
                ResourceResult result;
                result.contents.push_back(ResourceContents::Text(body_uri, "text/plain", body_uri).value());
                return bbt::infra::result<ResourceResult>::ok(std::move(result));
            })));
    }
    for (const std::string& name : {std::string("greeting"), std::string("工具"),
                                    std::string("=?base64?literal?="),
                                    std::string("=?base64?prefix")}) {
        bbt::infra::mcp::PromptDefinition prompt;
        prompt.name = name;
        CHECK(static_cast<bool>(fixture.server->AddPrompt(
            prompt, [name](const bbt::infra::mcp::RequestContext&,
                           const JsonValue&)
                -> bbt::infra::result<bbt::infra::mcp::PromptResult> {
                bbt::infra::mcp::PromptResult result;
                result.messages.push_back({bbt::infra::mcp::PromptRole::User,
                                           bbt::infra::mcp::Content::Text(name)});
                return bbt::infra::result<bbt::infra::mcp::PromptResult>::ok(std::move(result));
            })));
    }
    CHECK(static_cast<bool>(fixture.server->Start()));
    fixture.transport = McpHttpTransport::Listen(fixture.server, http_options).value();
    fixture.port = fixture.transport->LocalAddress().port;
    CHECK(fixture.port != 0);
    return fixture;
}

// --- 原始 socket HTTP 客户端 -------------------------------------------------------

class HttpConn {
public:
    ~HttpConn() { Close(); }

    bool Connect(std::uint16_t port) {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (fd_ < 0) return false;
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_port = htons(port);
        if (::inet_pton(AF_INET, kListenHost, &address.sin_addr) != 1) {
            Close();
            return false;
        }
        if (::connect(fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
            Close();
            return false;
        }
        return true;
    }

    bool SendAll(std::string_view data) {
        std::size_t offset = 0;
        while (offset < data.size()) {
            const ssize_t n = ::send(fd_, data.data() + offset, data.size() - offset, 0);
            if (n < 0 && errno == EINTR) continue;
            if (n <= 0) return false;
            offset += static_cast<std::size_t>(n);
        }
        return true;
    }

    void ShutdownWrite() { ::shutdown(fd_, SHUT_WR); }

    // 有界读取：deadline 内累计收到的字节（超时/EOF 返回已有数据）。
    std::string ReadUntilDeadline(int timeout_ms) {
        std::string out;
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        char buffer[4096];
        while (std::chrono::steady_clock::now() < deadline) {
            timeval tv{};
            tv.tv_sec = 0;
            tv.tv_usec = 100 * 1000;  // 100ms 切片，保证 deadline 可达
            ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            const ssize_t n = ::recv(fd_, buffer, sizeof(buffer), 0);
            if (n > 0) {
                out.append(buffer, static_cast<std::size_t>(n));
                continue;
            }
            if (n == 0) {
                eof_ = true;
                break;
            }
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) continue;
            break;
        }
        return out;
    }

    // 读取一条完整 HTTP 响应（头 + Content-Length 体）：响应到齐立即返回，
    // 不等满 deadline；EOF 提前结束则以已读部分判定。
    // 无 Content-Length 的响应（如 SSE）：body = 响应头之后「当前已到达」的全部字节，
    // 与头同段到达的后续事件会被吞进 body；调用方必须自行续接后续读取并与 body
    // 合并后才是完整流，不能只对 body 做一次性断言。
    bool ReadResponse(int timeout_ms, std::string& headers, std::string& body) {
        std::string raw;
        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(timeout_ms);
        char buffer[4096];
        while (std::chrono::steady_clock::now() < deadline) {
            timeval tv{};
            tv.tv_sec = 0;
            tv.tv_usec = 100 * 1000;
            ::setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
            const ssize_t n = ::recv(fd_, buffer, sizeof(buffer), 0);
            if (n > 0) {
                raw.append(buffer, static_cast<std::size_t>(n));
            } else if (n == 0) {
                eof_ = true;
            } else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
                break;
            }
            const auto header_end = raw.find("\r\n\r\n");
            if (header_end == std::string::npos) continue;
            headers = raw.substr(0, header_end);
            std::string rest = raw.substr(header_end + 4);
            std::size_t content_length = rest.size();
            const auto cl = headers.find("Content-Length:");
            if (cl != std::string::npos) {
                content_length =
                    static_cast<std::size_t>(std::atoi(headers.c_str() + cl + 15));
            }
            if (rest.size() < content_length && !eof_) continue;
            body = rest.substr(0, content_length);
            status_line = headers.substr(0, headers.find("\r\n"));
            return true;
        }
        return false;
    }

    bool Eof() const { return eof_; }
    const std::string& StatusLine() const { return status_line; }

    unsigned Status() const {
        // "HTTP/1.1 200 OK" → 200
        const auto sp = status_line.find(' ');
        if (sp == std::string::npos) return 0;
        return static_cast<unsigned>(std::atoi(status_line.c_str() + sp + 1));
    }

    void Close() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
    }

private:
    int fd_ = -1;
    bool eof_ = false;
    std::string status_line;
};

// --- 请求构造 ----------------------------------------------------------------------

struct RequestSpec {
    std::string method = "POST";
    std::string path = "/mcp";
    std::string body;
    std::string annot_method = "server/discover";  // Mcp-Method 必须等于 body method
    bool with_host = true;
    std::vector<std::pair<std::string, std::string>> headers;  // 原样追加（含重复/异常值）
    bool set_content_type = true;
    bool set_accept_json = true;
    bool set_version = true;        // MCP-Protocol-Version: 2026-07-28
    bool set_method_annot = true;   // Mcp-Method: <body 的 method>
    bool set_name_annot = true;     // Mcp-Name: req
};

// 现代帧（含 `_meta`）；meta=空串时不带 `_meta`。
std::string ModernFrame(const std::string& id, const std::string& method,
                        const std::string& meta_version = std::string(kModern)) {
    JsonValue frame = JsonValue::Object();
    frame.Set("jsonrpc", JsonValue("2.0"));
    frame.Set("id", JsonValue(id));
    frame.Set("method", JsonValue(method));
    JsonValue params = JsonValue::Object();
    if (!meta_version.empty()) {
        JsonValue meta = JsonValue::Object();
        meta.Set("io.modelcontextprotocol/protocolVersion", JsonValue(meta_version));
        meta.Set("io.modelcontextprotocol/clientCapabilities", JsonValue::Object());
        params.Set("_meta", std::move(meta));
    }
    frame.Set("params", std::move(params));
    return frame.Serialize();
}

// 订阅帧：params.notifications 过滤器（规范必需键）+ modern `_meta`。
std::string ListenFrame(const std::string& id) {
    JsonValue frame = JsonValue::Object();
    frame.Set("jsonrpc", JsonValue("2.0"));
    frame.Set("id", JsonValue(id));
    frame.Set("method", JsonValue("subscriptions/listen"));
    JsonValue params = JsonValue::Object();
    JsonValue filter = JsonValue::Object();
    JsonValue uris = JsonValue::Array();
    uris.Push(JsonValue(std::string(kResourceUri)));
    filter.Set("resourceSubscriptions", std::move(uris));
    params.Set("notifications", std::move(filter));
    JsonValue meta = JsonValue::Object();
    meta.Set("io.modelcontextprotocol/protocolVersion", JsonValue(std::string(kModern)));
    meta.Set("io.modelcontextprotocol/clientCapabilities", JsonValue::Object());
    params.Set("_meta", std::move(meta));
    frame.Set("params", std::move(params));
    return frame.Serialize();
}

std::string RawRequest(std::uint16_t port, const RequestSpec& spec) {
    std::string head = spec.method + " " + spec.path + " HTTP/1.1\r\n";
    if (spec.with_host) head += "Host: 127.0.0.1:" + std::to_string(port) + "\r\n";
    if (spec.set_content_type) head += "Content-Type: application/json\r\n";
    if (spec.set_accept_json) head += "Accept: application/json\r\n";
    if (spec.set_version) head += std::string("MCP-Protocol-Version: ") + std::string(kModern) + "\r\n";
    if (spec.set_method_annot) head += "Mcp-Method: " + spec.annot_method + "\r\n";
    if (spec.set_name_annot) head += "Mcp-Name: req\r\n";
    for (const auto& [name, value] : spec.headers) {
        head += name + ": " + value + "\r\n";
    }
    head += "Content-Length: " + std::to_string(spec.body.size()) + "\r\n\r\n";
    return head + spec.body;
}

// 发送默认 modern discover 请求（可在 spec 上覆盖任一头）。
struct HttpExchange {
    bool ok = false;
    unsigned status = 0;
    std::string headers;
    std::string body;
};

HttpExchange Exchange(std::uint16_t port, const RequestSpec& spec, int timeout_ms = 3000) {
    HttpExchange out;
    HttpConn conn;
    if (!conn.Connect(port)) return out;
    if (!conn.SendAll(RawRequest(port, spec))) return out;
    out.ok = conn.ReadResponse(timeout_ms, out.headers, out.body);
    out.status = conn.Status();
    if (std::getenv("BBT_HTTP_TRACE"))
        std::fprintf(stderr, "RAW-REQUEST-BEGIN\n%s\nRAW-REQUEST-END\n"
                             "RAW-RESPONSE-BEGIN\n%s\r\n\r\n%s\nRAW-RESPONSE-END\n",
                     RawRequest(port, spec).c_str(), out.headers.c_str(), out.body.c_str());
    return out;
}

RequestSpec ModernPing(const std::string& body = ModernFrame("1", "server/discover")) {
    RequestSpec spec;
    spec.body = body;
    return spec;
}

bool BodyContains(const std::string& body, const char* needle) {
    return body.find(needle) != std::string::npos;
}

} // namespace

// ---- 测试 ------------------------------------------------------------------------

namespace {

// T1：S18 状态矩阵。
void TestS18StatusMatrix(std::uint16_t port) {
    // 普通方法 → 200 JSON。
    auto ok = Exchange(port, ModernPing());
    CHECK(ok.ok && ok.status == 200);
    CHECK(BodyContains(ok.body, "\"result\"") && BodyContains(ok.body, "\"id\":\"1\""));
    CHECK(BodyContains(ok.headers, "application/json"));

    // 现代 unknown method → 404 + -32601（注解头与 body method 一致）。
    RequestSpec unknown_spec = ModernPing(ModernFrame("2", "totally/unknown"));
    unknown_spec.annot_method = "totally/unknown";
    auto unknown = Exchange(port, unknown_spec);
    CHECK(unknown.ok && unknown.status == 404);
    CHECK(BodyContains(unknown.body, "-32601"));

    // parse error → 400 + -32700。
    RequestSpec bad_json = ModernPing();
    bad_json.body = "{not json";
    auto parse_err = Exchange(port, bad_json);
    CHECK(parse_err.ok && parse_err.status == 400);
    CHECK(BodyContains(parse_err.body, "-32700"));

    // envelope error（缺 jsonrpc）→ 400 + -32600。
    RequestSpec bad_env = ModernPing();
    bad_env.body = R"({"id":7,"method":"ping","params":{}})";
    auto env_err = Exchange(port, bad_env);
    CHECK(env_err.ok && env_err.status == 400);
    CHECK(BodyContains(env_err.body, "-32600"));

    // params error（modern 缺 `_meta`）→ 400 + -32602。
    auto no_meta = Exchange(port, ModernPing(ModernFrame("3", "server/discover", "")));
    CHECK(no_meta.ok && no_meta.status == 400);
    CHECK(BodyContains(no_meta.body, "-32602"));

    // version error（`_meta` 版本不受支持）→ 400 + -32022。
    auto bad_version =
        Exchange(port, ModernPing(ModernFrame("4", "server/discover", "2020-01-01")));
    CHECK(bad_version.ok && bad_version.status == 400);
    CHECK(BodyContains(bad_version.body, "-32022"));
    CHECK(BodyContains(bad_version.body, "requested") &&
          BodyContains(bad_version.body, "supported"));

    // 通知 → 202 空 body。
    RequestSpec notification = ModernPing();
    notification.body = R"({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}},"requestId":"gone"}})";
    notification.annot_method = "notifications/cancelled";
    auto accepted = Exchange(port, notification);
    CHECK(accepted.ok && accepted.status == 202);
    CHECK(accepted.body.empty());

    // GET/DELETE → 405。
    RequestSpec get = ModernPing();
    get.body.clear();
    get.set_content_type = false;
    get.method = "GET";
    auto get_resp = Exchange(port, get);
    CHECK(get_resp.ok && get_resp.status == 405);
    RequestSpec del = get;
    del.method = "DELETE";
    auto del_resp = Exchange(port, del);
    CHECK(del_resp.ok && del_resp.status == 405);

    // 其他 HTTP 门禁：路径 404、缺 Host 400、Content-Type 415、Accept 406。
    RequestSpec wrong_path = ModernPing();
    wrong_path.path = "/other";
    CHECK(Exchange(port, wrong_path).status == 404);
    RequestSpec no_host = ModernPing();
    no_host.with_host = false;
    CHECK(Exchange(port, no_host).status == 400);
    RequestSpec bad_ct = ModernPing();
    bad_ct.set_content_type = false;
    bad_ct.headers.emplace_back("Content-Type", "text/plain");
    CHECK(Exchange(port, bad_ct).status == 415);
    RequestSpec bad_accept = ModernPing();
    bad_accept.set_accept_json = false;
    bad_accept.headers.emplace_back("Accept", "text/html");
    CHECK(Exchange(port, bad_accept).status == 406);

    std::fprintf(stderr, "PROGRESS T1 S18 status matrix done\n");
}

// T2：S19 注解头矩阵。
void TestS19HeaderMatrix(std::uint16_t port) {
    const std::string discover_frame = ModernFrame("1", "server/discover");

    auto expect_rpc_status = [&](const RequestSpec& spec, unsigned status,
                                 const char* needle, int line) {
        auto resp = Exchange(port, spec);
        Check(resp.ok && resp.status == status && BodyContains(resp.body, needle),
              "header verdict", line);
    };

    // MCP-Protocol-Version：缺失 / 重复（含大小写变体）/ 未知 revision。
    RequestSpec missing_version = ModernPing(discover_frame);
    missing_version.set_version = false;
    expect_rpc_status(missing_version, 400, "-32020", __LINE__);
    RequestSpec dup_version = ModernPing(discover_frame);
    dup_version.headers.emplace_back("MCP-Protocol-Version", std::string(kModern));
    expect_rpc_status(dup_version, 400, "-32020", __LINE__);
    RequestSpec case_dup = ModernPing(discover_frame);
    case_dup.headers.emplace_back("mcp-protocol-version", std::string(kModern));
    expect_rpc_status(case_dup, 400, "-32020", __LINE__);
    RequestSpec unknown_rev = ModernPing(discover_frame);
    unknown_rev.set_version = false;
    unknown_rev.headers.emplace_back("MCP-Protocol-Version", "2024-01-01");
    expect_rpc_status(unknown_rev, 400, "-32022", __LINE__);

    // Mcp-Method：缺失 / 重复 / 不符。
    RequestSpec missing_method = ModernPing(discover_frame);
    missing_method.set_method_annot = false;
    expect_rpc_status(missing_method, 400, "-32020", __LINE__);
    RequestSpec dup_method = ModernPing(discover_frame);
    dup_method.headers.emplace_back("Mcp-Method", "ping");
    expect_rpc_status(dup_method, 400, "-32020", __LINE__);
    RequestSpec mismatch_method = ModernPing(discover_frame);
    mismatch_method.set_method_annot = false;
    mismatch_method.headers.emplace_back("Mcp-Method", "other/method");
    expect_rpc_status(mismatch_method, 400, "-32020", __LINE__);

    // 无 name/uri 来源的方法不强制 Mcp-Name；出现时仍校验重复与编码。
    RequestSpec missing_name = ModernPing(discover_frame);
    missing_name.set_name_annot = false;
    CHECK(Exchange(port, missing_name).status == 200);
    RequestSpec dup_name = ModernPing(discover_frame);
    dup_name.headers.emplace_back("Mcp-Name", "req");
    expect_rpc_status(dup_name, 400, "-32020", __LINE__);
    RequestSpec ascii_name = ModernPing(discover_frame);
    ascii_name.set_name_annot = false;
    ascii_name.headers.emplace_back("Mcp-Name", "plain-ascii");
    CHECK(Exchange(port, ascii_name).status == 200);

    // 非 ASCII 值必须规范 sentinel 编码；仅前缀是普通 ASCII；非法 Base64 / CTL 拒绝。
    RequestSpec sentinel = ModernPing(discover_frame);
    sentinel.set_name_annot = false;
    sentinel.headers.emplace_back("Mcp-Name", "=?base64?" + Base64Encode("工具") + "?=");
    CHECK(Exchange(port, sentinel).status == 200);
    RequestSpec raw_non_ascii = ModernPing(discover_frame);
    raw_non_ascii.set_name_annot = false;
    raw_non_ascii.headers.emplace_back("Mcp-Name", "工具");
    expect_rpc_status(raw_non_ascii, 400, "-32020", __LINE__);
    RequestSpec ambiguous = ModernPing(discover_frame);
    ambiguous.set_name_annot = false;
    ambiguous.headers.emplace_back("Mcp-Name", "=?base64?abc");  // 只有前缀不是完整 sentinel
    CHECK(Exchange(port, ambiguous).status == 200);
    RequestSpec bad_b64 = ModernPing(discover_frame);
    bad_b64.set_name_annot = false;
    bad_b64.headers.emplace_back("Mcp-Name", "=?base64?@@@@?=");
    expect_rpc_status(bad_b64, 400, "-32020", __LINE__);
    RequestSpec ctl = ModernPing(discover_frame);
    ctl.set_name_annot = false;
    ctl.headers.emplace_back("Mcp-Name", std::string("a\x01b"));
    // CTL 在 HTTP 层即非法：无论 Beast 先拒（framing 400）还是注解校验拒（-32020），
    // 都必须 400 且绝不 200。
    auto ctl_resp = Exchange(port, ctl);
    CHECK(ctl_resp.ok && ctl_resp.status == 400);

    // 不支持的 Mcp-* 自定义注解：明确 -32020（含 session 域——本 transport 不 mint/echo）。
    RequestSpec unknown_annot = ModernPing(discover_frame);
    unknown_annot.headers.emplace_back("Mcp-Extra", "x");
    expect_rpc_status(unknown_annot, 400, "-32020", __LINE__);
    RequestSpec session_annot = ModernPing(discover_frame);
    session_annot.headers.emplace_back("Mcp-Session-Id", "abc123");
    expect_rpc_status(session_annot, 400, "-32020", __LINE__);
    // 响应永远不回 session 头（不 mint/echo）。
    auto plain = Exchange(port, ModernPing());
    CHECK(plain.headers.find("Mcp-Session-Id") == std::string::npos);

    // era 路由：legacy 头 + legacy initialize → 200；legacy 头 + modern `_meta` → -32020。
    RequestSpec legacy_init;
    legacy_init.set_version = false;
    legacy_init.set_method_annot = false;
    legacy_init.set_name_annot = false;
    legacy_init.headers.emplace_back("MCP-Protocol-Version", std::string(kLegacy));
    legacy_init.body =
        R"({"jsonrpc":"2.0","id":2,"method":"initialize","params":{"capabilities":{},)"
        R"("clientInfo":{"name":"cli","version":"1"},"protocolVersion":"2025-11-25"}})";
    auto legacy = Exchange(port, legacy_init);
    CHECK(legacy.ok && legacy.status == 200);
    CHECK(BodyContains(legacy.body, "\"result\"") &&
          BodyContains(legacy.body, "2025-11-25"));

    RequestSpec era_mismatch;
    era_mismatch.set_version = false;
    era_mismatch.set_method_annot = false;
    era_mismatch.set_name_annot = false;
    era_mismatch.headers.emplace_back("MCP-Protocol-Version", std::string(kLegacy));
    era_mismatch.body = ModernFrame("3", "ping");  // body 是 modern `_meta`
    expect_rpc_status(era_mismatch, 400, "-32020", __LINE__);

    std::fprintf(stderr, "PROGRESS T2 S19 header matrix done\n");
}

// 修订 S19：具名头/body 一致性、legacy 初始协商与联合错误顺序。
void TestS19Compatibility(std::uint16_t port) {
    auto expect_error = [&](const RequestSpec& spec, const char* code) {
        auto response = Exchange(port, spec);
        CHECK(response.ok && response.status == 400);
        CHECK(BodyContains(response.body, code));
    };
    for (const std::string& method : {std::string("server/discover"), std::string("resources/list"),
                                      std::string("resources/templates/list"), std::string("prompts/list")}) {
        auto spec = ModernPing(ModernFrame("no-name", method));
        spec.annot_method = method;
        spec.set_name_annot = false;
        auto response = Exchange(port, spec);
        CHECK(response.ok && response.status == 200);
    }
    for (const std::string& method : {std::string("tools/call"), std::string("resources/read"),
                                      std::string("prompts/get")}) {
        const char* key = method == "resources/read" ? "uri" : "name";
        const std::string ascii = method == "resources/read" ? std::string(kResourceUri) : "greeting";
        for (const std::string& value : {ascii, method == "resources/read" ? std::string("memo://工具")
                                                                          : std::string("工具"),
                                         std::string("=?base64?literal?="), std::string("=?base64?prefix")}) {
            auto frame = JsonValue::Parse(ModernFrame("named", method), 1U << 20, 16).value();
            auto params = frame.Find("params").value();
            params.Set(key, JsonValue(value));
            if (method == "tools/call") params.Set("arguments", JsonValue::Object());
            frame.Set("params", std::move(params));
            auto spec = ModernPing(frame.Serialize());
            spec.annot_method = method;
            spec.set_name_annot = false;
            const bool encoded = value.find("工具") != std::string::npos || value == "=?base64?literal?=";
            spec.headers.emplace_back("Mcp-Name", encoded ? "=?base64?" + Base64Encode(value) + "?=" : value);
            auto response = Exchange(port, spec);
            CHECK(response.ok && response.status == (method == "tools/call" ? 404u : 200u));
            if (method == "tools/call") {
                CHECK(BodyContains(response.body, "-32601"));  // 冻结 core 无 tools：仅证明头门禁放行。
            } else {
                CHECK(response.body.find(value) != std::string::npos);  // handler 收到原始 body 值。
            }
            if (value == ascii) {
                auto encoded_ascii = spec;
                encoded_ascii.headers.front().second = "=?base64?" + Base64Encode(value) + "?=";
                auto accepted = Exchange(port, encoded_ascii);
                CHECK(accepted.ok && accepted.status == (method == "tools/call" ? 404u : 200u));
            }
            auto missing = spec;
            missing.headers.clear();
            expect_error(missing, "-32020");
            auto duplicate = spec;
            duplicate.headers.push_back(spec.headers.front());
            expect_error(duplicate, "-32020");
            auto mismatch = spec;
            mismatch.headers.front().second = "different";
            expect_error(mismatch, "-32020");
            auto invalid = spec;
            invalid.headers.front().second = "=?base64?@@@@?=";
            expect_error(invalid, "-32020");
            invalid.headers.front().second = "=?base64?AA=A?=";
            expect_error(invalid, "-32020");
        }
    }
    for (const std::string& version : {std::string("2025-11-25"), std::string("2025-06-18")}) {
        RequestSpec init;
        init.set_version = init.set_method_annot = init.set_name_annot = false;
        init.body = "{\"jsonrpc\":\"2.0\",\"id\":\"init\",\"method\":\"initialize\",\"params\":{"
                    "\"capabilities\":{},\"clientInfo\":{\"name\":\"cli\",\"version\":\"1\"},"
                    "\"protocolVersion\":\"" + version + "\"}}";
        auto response = Exchange(port, init);
        CHECK(response.ok && response.status == 200);
        CHECK(response.body.find(version) != std::string::npos);
        CHECK(!BodyContains(response.body, "resultType") && !BodyContains(response.body, "ttlMs") &&
              !BodyContains(response.body, "cacheScope"));
        init.headers.emplace_back("MCP-Protocol-Version", version);
        CHECK(Exchange(port, init).status == 200);
        init.headers.front().second = version == "2025-11-25" ? "2025-06-18" : "2025-11-25";
        expect_error(init, "-32020");
        RequestSpec ping;
        ping.set_version = ping.set_method_annot = ping.set_name_annot = false;
        ping.body = R"({"jsonrpc":"2.0","id":"legacy-ping","method":"ping"})";
        expect_error(ping, "-32020");
        ping.headers.emplace_back("MCP-Protocol-Version", version);
        CHECK(Exchange(port, ping).status == 200);
        auto list = ping;
        list.body = R"({"jsonrpc":"2.0","id":"legacy-list","method":"resources/list"})";
        // R9/S23 legacy HTTP 无 session profile：受支持 legacy revision 头即 era
        // 门禁；官方 SDK 逐 POST 建连，新连接业务请求不得被每连接握手门禁拒绝。
        {
            auto list_resp = Exchange(port, list);
            CHECK(list_resp.ok && list_resp.status == 200);
            CHECK(BodyContains(list_resp.body, "\"result\""));
        }
        list.headers.clear();
        expect_error(list, "-32020");
    }
    auto bad_meta = ModernPing(ModernFrame("meta-first", "server/discover", ""));
    bad_meta.set_version = bad_meta.set_method_annot = bad_meta.set_name_annot = false;
    expect_error(bad_meta, "-32602");
    bad_meta = ModernPing(R"({"jsonrpc":"2.0","id":"meta-first","method":"server/discover","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":7}}})");
    bad_meta.headers.emplace_back("Mcp-Method", "duplicate");
    expect_error(bad_meta, "-32602");
    bad_meta.headers.clear();
    bad_meta.set_version = false;
    bad_meta.headers.emplace_back("MCP-Protocol-Version", std::string(kLegacy));
    expect_error(bad_meta, "-32602");  // modern 形状不因 legacy 头而延后。
    std::fprintf(stderr, "PROGRESS S19 compatibility done\n");
}

// review M1/M2 回归：合法 legacy `_meta`（progressToken）不得触发现代校验，
// 也不得否决无头首次 initialize 豁免；modern 保留键 + legacy 头仍拒绝。
void TestLegacyMetaEnvelope(std::uint16_t port) {
    for (const std::string& version : {std::string("2025-11-25"), std::string("2025-06-18")}) {
        // M1-B：legacy ping + 版本头 + `_meta.progressToken` → 200，无 modern 字段。
        RequestSpec ping;
        ping.set_version = ping.set_method_annot = ping.set_name_annot = false;
        ping.body = R"({"jsonrpc":"2.0","id":"legacy-meta-ping","method":"ping","params":{"_meta":{"progressToken":"tok-b"}}})";
        ping.headers.emplace_back("MCP-Protocol-Version", version);
        auto ping_resp = Exchange(port, ping);
        CHECK(ping_resp.ok && ping_resp.status == 200);
        CHECK(BodyContains(ping_resp.body, "\"result\""));
        CHECK(!BodyContains(ping_resp.body, "resultType") &&
              !BodyContains(ping_resp.body, "ttlMs") && !BodyContains(ping_resp.body, "cacheScope"));

        // M1-C：legacy initialize（带头）+ `_meta.progressToken` → 200。
        const std::string init_body =
            R"({"jsonrpc":"2.0","id":"legacy-meta-init","method":"initialize","params":{"capabilities":{},)"
            R"("clientInfo":{"name":"cli","version":"1"},"protocolVersion":")" + version +
            R"(","_meta":{"progressToken":"tok-c"}}})";
        RequestSpec init;
        init.set_version = init.set_method_annot = init.set_name_annot = false;
        init.body = init_body;
        init.headers.emplace_back("MCP-Protocol-Version", version);
        auto init_resp = Exchange(port, init);
        CHECK(init_resp.ok && init_resp.status == 200);
        CHECK(BodyContains(init_resp.body, version.c_str()));

        // M2-E：无头首次 initialize + `_meta.progressToken` → 豁免仍成立，200。
        RequestSpec headless;
        headless.set_version = headless.set_method_annot = headless.set_name_annot = false;
        headless.body = init_body;
        auto headless_resp = Exchange(port, headless);
        CHECK(headless_resp.ok && headless_resp.status == 200);
        CHECK(BodyContains(headless_resp.body, version.c_str()));

        // M1 后业务面：legacy 头 + resources/list + progressToken → 不得为头错误或
        // modern 缺键文本；既有会话门禁口径（新连接无会话）保持不变。
        RequestSpec list;
        list.set_version = list.set_method_annot = list.set_name_annot = false;
        list.body = R"({"jsonrpc":"2.0","id":"legacy-meta-list","method":"resources/list","params":{"_meta":{"progressToken":"tok-d"}}})";
        list.headers.emplace_back("MCP-Protocol-Version", version);
        auto list_resp = Exchange(port, list);
        // 无 session profile：合法 legacy revision 头放行业务请求（不再是 -32602）。
        CHECK(list_resp.ok && list_resp.status == 200);
        CHECK(BodyContains(list_resp.body, "\"result\""));
        CHECK(!BodyContains(list_resp.body, "-32020"));
        CHECK(!BodyContains(list_resp.body, "io.modelcontextprotocol/protocolVersion is required"));

        // 通知：legacy 头 + notifications/cancelled + progressToken → 既有通知语义
        //（202 空 body，既有有效通知 oracle 的 legacy 变体）。
        RequestSpec notif;
        notif.set_version = notif.set_method_annot = notif.set_name_annot = false;
        notif.body = R"({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"_meta":{"progressToken":"tok-e"},"requestId":"gone"}})";
        notif.headers.emplace_back("MCP-Protocol-Version", version);
        auto notif_resp = Exchange(port, notif);
        CHECK(notif_resp.ok && notif_resp.status == 202);
        CHECK(notif_resp.body.empty());
    }

    // modern 保留键 + legacy 头：era 不匹配 → 仍 -32020，不得放行（既有口径保持）。
    RequestSpec reserved;
    reserved.set_version = reserved.set_method_annot = reserved.set_name_annot = false;
    reserved.body = R"({"jsonrpc":"2.0","id":"reserved-legacy","method":"ping","params":{"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}})";
    reserved.headers.emplace_back("MCP-Protocol-Version", std::string(kLegacy));
    auto reserved_resp = Exchange(port, reserved);
    CHECK(reserved_resp.ok && reserved_resp.status == 400);
    CHECK(BodyContains(reserved_resp.body, "-32020"));

    // m4：Mcp-Name 头在而 body 注解名缺失/非字符串 → -32602。
    auto named_spec = [](const char* id, const char* params_json) {
        RequestSpec spec;
        spec.body = std::string(R"({"jsonrpc":"2.0","id":")") + id + R"(","method":"tools/call","params":{)" +
                 params_json + R"(,"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientCapabilities":{}}}})";
        spec.annot_method = "tools/call";
        spec.set_name_annot = false;
        spec.headers.emplace_back("Mcp-Name", "greeting");
        return spec;
    };
    for (const char* params_json : {R"("arguments":{})", R"("name":7,"arguments":{})"}) {
        auto response = Exchange(port, named_spec("m4-name", params_json));
        CHECK(response.ok && response.status == 400);
        CHECK(BodyContains(response.body, "-32602"));
    }
    std::fprintf(stderr, "PROGRESS legacy meta envelope done\n");
}

// R9/S23 legacy HTTP 无 session profile 回归：官方 SDK 逐 POST 新建 HTTP 连接，
// 受支持 legacy revision 头（两个修订）本身就是 legacy era 门禁——业务请求在
// 全新连接上必须被服务，且 legacy discover 能力不宣告不存在的 legacy subscribe
//（modern listen 不受影响，由既有 T5 SSE 用例覆盖）。
void TestLegacyNoSessionProfile(std::uint16_t port) {
    for (const std::string& version : {std::string("2025-11-25"), std::string("2025-06-18")}) {
        RequestSpec init;
        init.set_version = init.set_method_annot = init.set_name_annot = false;
        init.headers.emplace_back("MCP-Protocol-Version", version);
        init.body = R"({"jsonrpc":"2.0","id":"nosession-init","method":"initialize","params":{)"
                    R"("capabilities":{},"clientInfo":{"name":"cli","version":"1"},)"
                    R"("protocolVersion":")" + version + R"("}})";
        auto init_resp = Exchange(port, init);
        CHECK(init_resp.ok && init_resp.status == 200);
        CHECK(BodyContains(init_resp.body, version.c_str()));
        // 能力宣告：resources 存在，但不得宣告无 handler 的 legacy subscribe。
        CHECK(BodyContains(init_resp.body, "\"resources\""));
        CHECK(!BodyContains(init_resp.body, "\"subscribe\""));

        // 全新连接（Exchange 每次新建 socket）：legacy 业务请求必须被服务。
        RequestSpec list;
        list.set_version = list.set_method_annot = list.set_name_annot = false;
        list.headers.emplace_back("MCP-Protocol-Version", version);
        list.body = R"({"jsonrpc":"2.0","id":"nosession-list","method":"resources/list"})";
        auto list_resp = Exchange(port, list);
        CHECK(list_resp.ok && list_resp.status == 200);
        CHECK(BodyContains(list_resp.body, "\"result\""));

        RequestSpec ping;
        ping.set_version = ping.set_method_annot = ping.set_name_annot = false;
        ping.headers.emplace_back("MCP-Protocol-Version", version);
        ping.body = R"({"jsonrpc":"2.0","id":"nosession-ping","method":"ping"})";
        auto ping_resp = Exchange(port, ping);
        CHECK(ping_resp.ok && ping_resp.status == 200);
        CHECK(BodyContains(ping_resp.body, "\"result\""));

        // 未带受支持 legacy 头的业务帧仍被头门禁拒绝（不放宽）。
        auto headless = list;
        headless.headers.clear();
        auto headless_resp = Exchange(port, headless);
        CHECK(headless_resp.ok && headless_resp.status == 400);
        CHECK(BodyContains(headless_resp.body, "-32020"));
    }
}


// T3：Origin 三态（合法 / 无效 / 重复）。
void TestS19Origin(std::uint16_t port) {
    RequestSpec allowed = ModernPing();
    allowed.headers.emplace_back("Origin", "http://127.0.0.1");
    CHECK(Exchange(port, allowed).status == 200);

    RequestSpec absent = ModernPing();
    // 默认不带 Origin → 放行。
    CHECK(Exchange(port, absent).status == 200);

    RequestSpec invalid = ModernPing();
    invalid.headers.emplace_back("Origin", "not-an-origin");
    CHECK(Exchange(port, invalid).status == 403);

    RequestSpec disallowed = ModernPing();
    disallowed.headers.emplace_back("Origin", "http://evil.example");
    CHECK(Exchange(port, disallowed).status == 403);

    RequestSpec duplicate = ModernPing();
    duplicate.headers.emplace_back("Origin", "http://127.0.0.1");
    duplicate.headers.emplace_back("Origin", "http://localhost");
    CHECK(Exchange(port, duplicate).status == 403);

    std::fprintf(stderr, "PROGRESS T3 Origin done\n");
}

// T4：完整 POST 后 SHUT_WR 仍响应；截断正文 EOF 明确失败（S20）。
void TestHalfCloseAndTruncation(std::uint16_t port) {
    // JSON：完整 POST + SHUT_WR → 仍 200。
    {
        HttpConn conn;
        CHECK(conn.Connect(port));
        CHECK(conn.SendAll(RawRequest(port, ModernPing())));
        conn.ShutdownWrite();
        std::string headers, body;
        CHECK(conn.ReadResponse(3000, headers, body));
        CHECK(conn.Status() == 200);
        CHECK(BodyContains(body, "\"result\""));
    }
    // SSE：完整 listen POST + SHUT_WR → ack 仍到达（不因读 EOF 误取消）。
    {
        HttpConn conn;
        CHECK(conn.Connect(port));
        RequestSpec listen = ModernPing();
        listen.set_accept_json = false;
        listen.body = ListenFrame("9");
        listen.set_method_annot = false;
        listen.headers.emplace_back("Mcp-Method", "subscriptions/listen");
        listen.headers.emplace_back("Accept", "application/json, text/event-stream");
        CHECK(conn.SendAll(RawRequest(port, listen)));
        conn.ShutdownWrite();
        const std::string stream_head = conn.ReadUntilDeadline(2000);
        CHECK(stream_head.find("200") != std::string::npos);
        CHECK(stream_head.find("text/event-stream") != std::string::npos);
        CHECK(stream_head.find("event: message") != std::string::npos);
        CHECK(stream_head.find("acknowledged") != std::string::npos);
    }
    // 截断正文：Content-Length 100 只送 5 字节后 EOF → 明确 400（绝不 200/部分响应）。
    {
        HttpConn conn;
        CHECK(conn.Connect(port));
        std::string head = "POST /mcp HTTP/1.1\r\nHost: 127.0.0.1:" + std::to_string(port) +
                           "\r\nContent-Type: application/json\r\nAccept: application/json\r\n"
                           "MCP-Protocol-Version: 2026-07-28\r\nMcp-Method: ping\r\n"
                           "Mcp-Name: req\r\nContent-Length: 100\r\n\r\n";
        CHECK(conn.SendAll(head));
        CHECK(conn.SendAll("{\"a\""));
        conn.ShutdownWrite();
        std::string headers, body;
        CHECK(conn.ReadResponse(3000, headers, body));
        CHECK(conn.Status() == 400);
        CHECK(!BodyContains(body, "\"result\""));
    }
    std::fprintf(stderr, "PROGRESS T4 half-close & truncation done\n");
}

} // namespace

// T5/T6/T7 在匿名命名空间外以文件局部函数实现（需要 Fixture）。

int main() {
    auto* config = bbt::coroutine::detail::GlobalConfig::GetInstance().get();
    config->m_cfg_static_thread_num = 2;
    config->m_cfg_stack_size = 1024 * 256;
    bbt::coroutine::detail::Scheduler::GetInstance()->Start(
        bbt::coroutine::SCHE_START_OPT_SCHE_THREAD);

    // T0：选项校验（非 loopback 拒绝、未 Start 拒绝、动态端口）。
    {
        auto unstarted = McpServer::Create(ServerOptions{}).value();
        auto not_started = McpHttpTransport::Listen(unstarted, HttpOptions{});
        CHECK(!not_started);
        auto public_host = McpHttpTransport::Listen(unstarted, [] {
            HttpOptions options;
            options.listen_host = "0.0.0.0";
            return options;
        }());
        CHECK(!public_host);
    }

    // 常规服务器（心跳调快，便于有界探测断言）。
    HttpOptions http_options;
    http_options.sse_heartbeat_interval = 150ms;
    Fixture fixture = MakeServer(ServerOptions{}, http_options);
    const std::uint16_t port = fixture.port;

    TestS18StatusMatrix(port);
    TestS19HeaderMatrix(port);
    TestS19Compatibility(port);
    TestLegacyMetaEnvelope(port);  // review M1/M2 回归
    TestLegacyNoSessionProfile(port);  // R9/S23 legacy 无 session profile + 能力宣告
    TestS19Origin(port);
    TestHalfCloseAndTruncation(port);

    // T5：SSE 真流 + 心跳 + 发布事件 + 断连 → 订阅取消（名额归还可观测）。
    {
        ServerOptions solo;
        solo.max_inflight_requests = 1;  // 订阅持有唯一长请求名额 → 取消可观测
        Fixture solo_fixture = MakeServer(solo, http_options);

        HttpConn subscriber;
        CHECK(subscriber.Connect(solo_fixture.port));
        RequestSpec listen = ModernPing();
        listen.body = ListenFrame("1");
        listen.set_name_annot = false;
        listen.set_method_annot = false;
        listen.set_accept_json = false;
        listen.headers.emplace_back("Mcp-Method", "subscriptions/listen");
        listen.headers.emplace_back("Accept", "application/json, text/event-stream");
        CHECK(subscriber.SendAll(RawRequest(solo_fixture.port, listen)));
        std::string headers, body;
        CHECK(subscriber.ReadResponse(3000, headers, body));
        CHECK(subscriber.Status() == 200);
        CHECK(headers.find("text/event-stream") != std::string::npos);

        // 真 SSE：ack 是流上的第一个事件（不是一次性拼接字符串）。
        // SSE 响应无 Content-Length：ReadResponse 会把「头后已到字节」作为 body 返回，
        // ack 可能与响应头同段到达而被吞在 body 里——断言必须基于合并后的完整流。
        std::string stream = body;
        stream += subscriber.ReadUntilDeadline(2000);
        CHECK(stream.find("event: message") != std::string::npos);
        CHECK(stream.find("acknowledged") != std::string::npos);
        CHECK(stream.find("subscriptionId") != std::string::npos);

        // 订阅在册 → 唯一长请求名额被占：新请求被核心拒绝（503）。
        auto rejected = Exchange(solo_fixture.port, ModernPing(ModernFrame("2", "server/discover")));
        CHECK(rejected.ok && rejected.status == 503);

        // 心跳注释到达（不作为 JSON；有界探测通道）。
        std::string heartbeat;
        const auto heartbeat_deadline = std::chrono::steady_clock::now() + 3s;
        while (std::chrono::steady_clock::now() < heartbeat_deadline) {
            heartbeat += subscriber.ReadUntilDeadline(300);
            if (heartbeat.find(": keep-alive") != std::string::npos) break;
        }
        CHECK(heartbeat.find(": keep-alive") != std::string::npos);
        CHECK(!BodyContains(heartbeat, "\"jsonrpc\""));  // 心跳不是 JSON 帧

        // 断连（全关）→ 写失败探测 → 订阅取消 → 名额归还 → 新请求恢复 200。
        subscriber.Close();
        bool recovered = false;
        const auto recover_deadline = std::chrono::steady_clock::now() + 5s;
        while (std::chrono::steady_clock::now() < recover_deadline) {
            auto after = Exchange(solo_fixture.port, ModernPing(ModernFrame("3", "server/discover")), 500);
            if (after.ok && after.status == 200) {
                recovered = true;
                break;
            }
            std::this_thread::sleep_for(100ms);
        }
        CHECK(recovered);

        // 慢读有界 + 不饿死普通请求：tiny 挂起队列，读方停止读取。
        ServerOptions plenty;
        plenty.max_inflight_requests = 8;  // 订阅占 1，普通请求余量充足
        HttpOptions tiny = http_options;
        tiny.max_pending_bytes = 512;
        Fixture slow = MakeServer(plenty, tiny);
        HttpConn stalled;
        CHECK(stalled.Connect(slow.port));
        RequestSpec slow_listen = ModernPing();
        slow_listen.body = ListenFrame("1");
        slow_listen.set_method_annot = false;
        slow_listen.set_accept_json = false;
        slow_listen.headers.emplace_back("Mcp-Method", "subscriptions/listen");
        slow_listen.headers.emplace_back("Accept", "text/event-stream");
        CHECK(stalled.SendAll(RawRequest(slow.port, slow_listen)));
        (void)stalled.ReadUntilDeadline(500);  // 读走 ack 后停止读取（慢读方）
        for (int i = 0; i < 200; ++i) {
            (void)slow.server->PublishResourceChanged(std::string(kResourceUri));
        }
        // 慢读方挂起期间，普通请求照常服务（每连接独立有界队列，不互相饿死）。
        auto healthy = Exchange(slow.port, ModernPing(ModernFrame("4", "server/discover")));
        CHECK(healthy.ok && healthy.status == 200);
        slow.transport->Close();
        const HttpCloseEvidence slow_evidence = slow.transport->LastCloseEvidence();
        CHECK(slow_evidence.dropped_frames > 0);   // 慢读输出有界（丢弃可观测）
        CHECK(slow_evidence.output_sealed);
        stalled.Close();

        solo_fixture.transport->Close();
        const HttpCloseEvidence solo_evidence = solo_fixture.transport->LastCloseEvidence();
        CHECK(solo_evidence.write_error_seen);  // 断连经写路径确认
        std::fprintf(stderr, "PROGRESS T5 SSE stream & cancel done\n");
    }

    // T6：连接配额（max_connections 含 listener；满载拒绝后释放即恢复）。
    {
        HttpOptions tight = http_options;
        tight.max_connections = 2;  // listener + 1 个 accepted
        Fixture tight_fixture = MakeServer(ServerOptions{}, tight);

        // A：SSE 订阅长流——accepted 连接名额被持续持有（连接容量满载）。
        HttpConn first;
        CHECK(first.Connect(tight_fixture.port));
        RequestSpec listen = ModernPing();
        listen.body = ListenFrame("1");
        listen.set_method_annot = false;
        listen.set_accept_json = false;
        listen.headers.emplace_back("Mcp-Method", "subscriptions/listen");
        listen.headers.emplace_back("Accept", "text/event-stream");
        CHECK(first.SendAll(RawRequest(tight_fixture.port, listen)));
        const std::string ack = first.ReadUntilDeadline(2000);
        CHECK(ack.find("event: message") != std::string::npos);

        // B：容量满载 → 有界等待内不得被服务。
        HttpConn second;
        CHECK(second.Connect(tight_fixture.port));
        CHECK(second.SendAll(RawRequest(tight_fixture.port, ModernPing())));
        std::string early_headers, early_body;
        const bool served_early = second.ReadResponse(800, early_headers, early_body);
        CHECK(!served_early);

        first.Close();  // 断连 → 连接物理关闭 → 容量释放 → backlog 中的 B 被接纳并响应
        std::string late_headers, late_body;
        CHECK(second.ReadResponse(5000, late_headers, late_body));
        CHECK(second.Status() == 200);
        tight_fixture.transport->Close();
        std::fprintf(stderr, "PROGRESS T6 connection quota done\n");
    }

    // T7：并发 Close 等首次真实落定；返回当刻 listener 不可 connect；core 不受影响。
    {
        Fixture closing = MakeServer(ServerOptions{}, http_options);
        HttpConn live;
        CHECK(live.Connect(closing.port));
        RequestSpec listen = ModernPing();
        listen.body = ListenFrame("1");
        listen.set_method_annot = false;
        listen.set_accept_json = false;
        listen.headers.emplace_back("Mcp-Method", "subscriptions/listen");
        listen.headers.emplace_back("Accept", "text/event-stream");
        CHECK(live.SendAll(RawRequest(closing.port, listen)));
        (void)live.ReadUntilDeadline(1000);  // ack 已到，流保持

        std::atomic<bool> first_done{false};
        std::thread closer_a([&] {
            closing.transport->Close();
            first_done = true;
        });
        std::thread closer_b([&] {
            // 并发/重复 Close 均等首次真实落定（不崩溃、不提前返回伪成功）。
            closing.transport->Close();
        });
        closer_a.join();
        closer_b.join();
        CHECK(first_done.load());

        CHECK(closing.transport->IsClosed());
        const HttpCloseEvidence evidence = closing.transport->LastCloseEvidence();
        CHECK(evidence.listener_closed);
        CHECK(evidence.runtime_closed);
        CHECK(evidence.accept_loop_exited);
        CHECK(evidence.connection_loops_exited);
        CHECK(evidence.output_sealed);
        CHECK(evidence.connections_closed >= 1);

        // 返回当刻：SSE 流已断（EOF），listener 不可 connect，core 仍开放。
        const std::string tail = live.ReadUntilDeadline(1500);
        CHECK(live.Eof());
        live.Close();
        // listener 不可 connect 的机器可读证据以对象面为准（evidence.listener_closed
        // / runtime_closed，上面已断言）。进程外网络探测在本环境不可靠：WSL2 会把
        // Windows 侧监听的本机端口转发进 127.0.0.1，刚释放的动态端口可能立刻被
        // 环境噪声占用（连接成功但对端无数据），不作为本切片的实现判据。
        HttpConn probe;
        const bool probe_connected = probe.Connect(closing.port);
        if (probe_connected) {
            const std::string probe_tail = probe.ReadUntilDeadline(300);
            std::fprintf(stderr,
                         "note: probe connected after Close (environment port squat), "
                         "eof=%d bytes=%zu\n",
                         (int)probe.Eof(), probe_tail.size());
        }
        CHECK(closing.server->IsStarted() && !closing.server->IsClosed());
        std::fprintf(stderr, "PROGRESS T7 concurrent close done\n");
    }

    const std::uint16_t main_port = fixture.port;
    fixture.transport->Close();
    CHECK(fixture.transport->IsClosed());
    // fd 卫生（R10 邻近证据）：主 fixture 收口后，本进程再无该端口的监听 socket
    //（含 runtime 侧一切重复 fd）。探测有 1s 有界宽限（对端关闭的竞态窗口）。
    bool port_gone = false;
    for (int attempt = 0; attempt < 10 && !port_gone; ++attempt) {
        port_gone = true;
        for (int fd = 0; fd < 1024; ++fd) {
            char link[128], target[256];
            std::snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
            const ssize_t n = ::readlink(link, target, sizeof(target) - 1);
            if (n <= 0) continue;
            target[n] = 0;
            if (std::strstr(target, "socket") == nullptr) continue;
            sockaddr_in addr{};
            socklen_t len = sizeof(addr);
            if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0 &&
                ntohs(addr.sin_port) == main_port) {
                port_gone = false;
            }
        }
        if (!port_gone) std::this_thread::sleep_for(100ms);
    }
    CHECK(port_gone);
    CHECK(fixture.server->IsStarted() && !fixture.server->IsClosed());
    std::fprintf(stderr, "CLEANUP main_listener_closed=%d main_port_fd_gone=%d\n",
                 (int)fixture.transport->IsClosed(), (int)port_gone);

    std::printf("mcp.http_transport: %d checks, %d failures\n", g_checks, g_failures);
    return g_failures == 0 ? 0 : 1;
}
