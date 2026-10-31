// MCP Streamable HTTP transport（3-2 切片，architecture §8）：精确 POST /mcp +
// Accept 双媒体 + Origin/Host 门禁 + modern 路由头校验（MCP-Protocol-Version /
// Mcp-Method / Mcp-Name 含 =?base64?…?= sentinel）+ JSON-vs-deferred SSE 响应
// + subscriptions/listen 长流 + keepalive + 断连驱动订阅结束 + bounded queue。
//
// 形态契约（WFER mcp-server-foundation + 固定规范 2026-07-28 + SDK v2.3.0）：
//   - 不新增 I/O 线程/io_context：全部请求处理跑在 http::Server 的受管协程
//     dispatch 内；每 HTTP 连接一个独立 McpSession（会话态 = 连接局部；
//     SDK legacy 客户端逐 POST 新建连接即天然等价）。
//   - 帧进出：session->HandleFrame 同步调用（在途上限/期限/cancel/owner 由
//     core 3-3 承担）；订阅 ack/事件/流结束帧经会话 NotificationSink 路由进
//     本连接的 McpEventBody 有界队列。
//   - legacy 头门禁（无 session profile）：MCP-Protocol-Version 为已服务
//     legacy revision（2025-11-25）且**无** modern 信封的 POST 在 dispatch 前
//     AdoptLegacyHeaderGate（不回填 caps、不伪造 initialize；initialize
//     已落定时不覆盖）。现代请求不做门禁（逐请求 _meta 自校验）。
//   - SSE：subscriptions/listen 结果 body 为 text/event-stream chunked；
//     帧按「data: <frame>\n\n」、keepalive 按「: \n\n」分块；无 id 字段
//     （不承诺重放）、无视 Last-Event-ID、绝不写 Mcp-Session-Id。
//   - 关闭：连接终止/超时/peer RST → http 层 Cancel 一次 → McpEventBody 封队
//     并 EndAllSubscriptions（S20 断连驱动）；transport Close 委托
//     http::Server 四段收口，不关 Engine。

#include <bbt/infra/mcp/McpHttpTransport.hpp>

#include "mcp/detail/Server.hpp"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdio>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/infra/http/Server.hpp>
#include <bbt/infra/mcp/JsonValue.hpp>
#include <bbt/infra/mcp/McpTypes.hpp>
#include <bbt/infra/net/Engine.hpp>

namespace bbt::infra::mcp {
namespace {

using bbt::coroutine::sync::CoWaiter;
using bbt::coroutine::sync::CombinedWaitOptions;
using bbt::coroutine::sync::CombinedWaitStatus;
using http::Headers;
using http::IncomingRequest;
using http::Response;
using http::ServerReply;
using http::StreamStart;

constexpr std::string_view kPath = "/mcp";
constexpr std::string_view kMediaJson = "application/json";
constexpr std::string_view kMediaSse = "text/event-stream";
constexpr std::string_view kHeaderProtocolVersion = "mcp-protocol-version";
constexpr std::string_view kHeaderMethod = "mcp-method";
constexpr std::string_view kHeaderName = "mcp-name";
constexpr std::string_view kHeaderAccept = "accept";
constexpr std::string_view kHeaderOrigin = "origin";
constexpr std::string_view kHeaderHost = "host";
constexpr std::string_view kHeaderContentType = "content-type";
constexpr std::string_view kB64SentinelPrefix = "=?base64?";
constexpr std::string_view kB64SentinelSuffix = "?=";

Error MakeMcpHttpError(ErrorCode code, const char* domain_code) {
    Error e;
    e.code = code;
    e.domain_code = domain_code;
    return e;
}

// ---- HTTP 头工具 --------------------------------------------------------------

bool EqualsNoCase(std::string_view left, std::string_view right) {
    if (left.size() != right.size()) return false;
    for (std::size_t i = 0; i < left.size(); ++i)
        if (std::tolower(static_cast<unsigned char>(left[i])) !=
            std::tolower(static_cast<unsigned char>(right[i])))
            return false;
    return true;
}

// 全部同名头的小写名 → 值列表（重复头逐一保留，用于重复路由头拒绝）。
std::vector<std::string_view> HeaderValues(const IncomingRequest& request,
                                           std::string_view name) {
    std::vector<std::string_view> out;
    for (const auto& field : request.headers)
        if (EqualsNoCase(field.first, name)) out.push_back(field.second);
    return out;
}

std::optional<std::string_view> SingleHeader(const IncomingRequest& request,
                                             std::string_view name) {
    auto values = HeaderValues(request, name);
    if (values.size() != 1) return std::nullopt;
    return values.front();
}

std::string_view Trim(std::string_view v) {
    while (!v.empty() && (v.front() == ' ' || v.front() == '\t')) v.remove_prefix(1);
    while (!v.empty() && (v.back() == ' ' || v.back() == '\t')) v.remove_suffix(1);
    return v;
}

// Accept 媒体 range 匹配：精确等价（大小写不敏感）或 type/* 或 *//*；参数忽略。
bool AcceptsMedia(std::string_view accept, std::string_view type) {
    const auto slash = type.find('/');
    const std::string_view prefix = type.substr(0, slash + 1);
    for (std::size_t pos = 0; pos <= accept.size();) {
        const auto comma = accept.find(',', pos);
        const std::string_view entry =
            Trim(accept.substr(pos, comma == std::string_view::npos
                                          ? std::string_view::npos
                                          : comma - pos));
        const std::string_view media = Trim(entry.substr(0, entry.find(';')));
        bool hit = EqualsNoCase(media, type) || media == "*/*";
        if (!hit && media.size() == prefix.size() + 1 && media.back() == '*' &&
            EqualsNoCase(media.substr(0, prefix.size()), prefix))
            hit = true;
        if (hit) return true;
        if (comma == std::string_view::npos) break;
        pos = comma + 1;
    }
    return false;
}

// ---- Mcp-Name base64 sentinel（SDK shared/inbound.py 同规则）---------------------
// =?base64?{b64}?= 包裹 → 严格解码（标准字母表、长度 4 的倍数、仅尾部 '=' ≤2、
// UTF-8 良构）；任一项失败 → nullopt（畸形 sentinel 不匹配任何体值）。

bool B64Alphabet(unsigned char c) {
    return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
           (c >= '0' && c <= '9') || c == '+' || c == '/';
}

result<std::string> B64Decode(std::string_view payload) {
    if (payload.empty() || payload.size() % 4 != 0)
        return result<std::string>::err(
            MakeMcpHttpError(ErrorCode::ProtocolError, "mcp.http.b64_length"));
    std::size_t pad = 0;
    if (payload.back() == '=') ++pad;
    if (pad == 1 && payload[payload.size() - 2] == '=') ++pad;
    for (std::size_t i = 0; i + pad < payload.size(); ++i)
        if (!B64Alphabet(static_cast<unsigned char>(payload[i])))
            return result<std::string>::err(
                MakeMcpHttpError(ErrorCode::ProtocolError, "mcp.http.b64_char"));
    std::string out;
    out.reserve(payload.size() / 4 * 3);
    auto sextet = [](unsigned char c) -> unsigned {
        if (c >= 'A' && c <= 'Z') return c - 'A';
        if (c >= 'a' && c <= 'z') return c - 'a' + 26;
        if (c >= '0' && c <= '9') return c - '0' + 52;
        return c == '+' ? 62 : 63;
    };
    for (std::size_t i = 0; i < payload.size(); i += 4) {
        unsigned n = 0;
        for (int j = 0; j < 4; ++j) {
            const unsigned char c = payload[i + j];
            n = (n << 6) | (c == '=' ? 0u : sextet(c));
        }
        out.push_back(static_cast<char>((n >> 16) & 0xFF));
        out.push_back(static_cast<char>((n >> 8) & 0xFF));
        out.push_back(static_cast<char>(n & 0xFF));
    }
    out.resize(out.size() - pad);
    if (!bbt::infra::detail::IsValidUtf8(out))
        return result<std::string>::err(
            MakeMcpHttpError(ErrorCode::ProtocolError, "mcp.http.b64_utf8"));
    return result<std::string>::ok(std::move(out));
}

// 解码头值（SDK shared/inbound.py decode_header_value 同规则）：非 sentinel 且
// 全为可打印 ASCII（0x20-0x7E）→ 原样返回；sentinel 形状但解码失败、或含
// 非 ASCII/控制字节 → nullopt（永不匹配，不猜发送方意图）。
std::optional<std::string> DecodeHeaderValue(std::string_view value) {
    if (value.size() >= kB64SentinelPrefix.size() + kB64SentinelSuffix.size() &&
        value.substr(0, kB64SentinelPrefix.size()) == kB64SentinelPrefix &&
        value.substr(value.size() - kB64SentinelSuffix.size()) == kB64SentinelSuffix) {
        const auto payload = value.substr(kB64SentinelPrefix.size(),
                                          value.size() - kB64SentinelPrefix.size() -
                                              kB64SentinelSuffix.size());
        auto decoded = B64Decode(payload);
        if (!decoded) return std::nullopt;
        return std::move(decoded).value();
    }
    for (const unsigned char c : value)
        if (c < 0x20 || c > 0x7E) return std::nullopt;
    return std::string(value);
}

// modern 信封判定（与 core HasModernEnvelope 同判据，本 TU 局部实现——core 同名
// 函数是匿名命名空间，不可复用）：params._meta 是 object 且含规范保留键
// io.modelcontextprotocol/protocolVersion。
bool ModernEnvelope(const std::optional<JsonValue>& params) {
    if (!params || !params->IsObject()) return false;
    auto meta = params->Find("_meta");
    if (!meta || !meta.value().IsObject()) return false;
    return static_cast<bool>(meta.value().Find(std::string(kMetaKeyProtocolVersion)));
}

// ---- 应答构造 -------------------------------------------------------------------

Response JsonErrorBody(int status, const std::optional<RequestId>& id,
                       JsonRpcErrorCode code, const char* message,
                       std::optional<JsonValue> data = std::nullopt) {
    ProtocolFailure failure;
    failure.code = code;
    failure.message = message;
    failure.data = std::move(data);
    return {status, {{std::string(kHeaderContentType), std::string(kMediaJson)}},
            MakeErrorResponse(id, failure).Serialize()};
}

Response JsonBody(int status, std::string body) {
    return {status, {{std::string(kHeaderContentType), std::string(kMediaJson)}},
            std::move(body)};
}

Response Empty(int status) { return {status, {}, {}}; }

// ---- McpEventBody（私有；SSE 拉取源）----------------------------------------------
//
// 一个 SSE 流对应一个 McpEventBody（subscriptions/listen 的 HTTP 出帧面）：
// listen 帧由 transport 创建并接管本连接会话 sink。ack/事件/流结束帧经 Push
// 入队（满即丢 + 计数 + 限量 stderr 诊断，非阻塞调用方）；流结束响应（success
// response id==listen id）排干后 EOF。Start 等首帧（ack 先于一切事件）或
// keepalive tick；Next 空队挂起至新帧/tick/deadline；「: 」keepalive 不重置
// http 层 stream deadline。

class McpEventBody : public http::ResponseBody,
                     public std::enable_shared_from_this<McpEventBody> {
public:
    McpEventBody(std::shared_ptr<McpSession> session, std::size_t max_events,
                 std::chrono::milliseconds keepalive)
        : m_session(std::move(session)),
          m_max_events(max_events),
          m_keepalive(keepalive) {}

    ~McpEventBody() override { Detach(); }

    // 挂载会话 sink（Start 前调用；weak_ptr 防 UAF：body 析构后晚到帧只落
    // 空 weak，不再交付）。
    void Attach() {
        std::weak_ptr<McpEventBody> weak = shared_from_this();
        m_session->SetNotificationSink([weak](const std::string& frame) {
            if (auto body = weak.lock()) body->Push(frame);
        });
    }

    bbt_co_async(result<StreamStart>) Start(
        const bbt::coroutine::WaitOptions& options) override {
        const auto status = WaitEvent(options);
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_cancelled)
                return result<StreamStart>::err(
                    MakeMcpHttpError(ErrorCode::Closed, "mcp.http.stream_closed"));
            if (!m_queue.empty())
                return result<StreamStart>::ok(MakeSseStart(PopLocked()));
        }
        if (status == CombinedWaitStatus::InvalidContext)
            return result<StreamStart>::err(MakeMcpHttpError(
                ErrorCode::InvalidContext, "mcp.http.stream_context"));
        if (status == CombinedWaitStatus::TimedOut)
            return result<StreamStart>::err(
                MakeMcpHttpError(ErrorCode::TimedOut, "mcp.http.stream_start"));
        // Completed（被 keepalive tick 到点唤醒且无帧）→ 注释块开头。
        return result<StreamStart>::ok(
            MakeSseStart(std::string(kKeepaliveChunk)));
    }

    bbt_co_async(result<std::optional<std::string>>) Next(
        const bbt::coroutine::WaitOptions& options) override {
        for (;;) {
            {
                std::lock_guard<std::mutex> lock(m_mtx);
                if (!m_queue.empty())
                    return result<std::optional<std::string>>::ok(
                        std::optional<std::string>{PopLocked()});
                if (m_eof)
                    return result<std::optional<std::string>>::ok(
                        std::optional<std::string>{});
                if (m_cancelled)
                    return result<std::optional<std::string>>::err(
                        MakeMcpHttpError(ErrorCode::Closed, "mcp.http.stream_closed"));
            }
            const auto status = WaitEvent(options);
            if (status == CombinedWaitStatus::InvalidContext)
                return result<std::optional<std::string>>::err(MakeMcpHttpError(
                    ErrorCode::InvalidContext, "mcp.http.stream_context"));
            // Completed / TimedOut(tick 到点) / Cancelled（防御）→ loop 复查；
            // 空队时按 tick 语义产出 keepalive 注释块。
            {
                std::lock_guard<std::mutex> lock(m_mtx);
                if (m_queue.empty() && !m_eof && !m_cancelled)
                    return result<std::optional<std::string>>::ok(
                        std::optional<std::string>{std::string(kKeepaliveChunk)});
            }
        }
    }

    // http 层 commit 后错误/写失败/Close 的唯一取消点（幂等）：封队、卸 sink、
    // 断连驱动订阅结束（S20）。Cancel 后不再交付任何帧。
    void Cancel() noexcept override {
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_cancelled) return;
            m_cancelled = true;
        }
        Detach();
        WakeAll();
    }

    // sink 出口（任意线程，从不阻塞）：满即丢帧 + 计数 + 限量 stderr 诊断。
    void Push(const std::string& frame) {
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_cancelled || m_eof) return;
            if (m_queue.size() >= m_max_events) {
                ++m_dropped;
                if (m_dropped <= kDropDiagLimit)
                    std::fprintf(stderr,
                                 "bbt.mcp.http: stream event dropped (queue full)\n");
                return;
            }
            m_queue.push_back(frame);
            if (IsFlowEndFrame(frame)) m_eof_pending = true;
        }
        WakeAll();
    }

    std::size_t DroppedEvents() const noexcept {
        std::lock_guard<std::mutex> lock(m_mtx);
        return m_dropped;
    }

private:
    static constexpr int kDropDiagLimit = 10;
    static constexpr std::string_view kKeepaliveChunk = ": \n\n";

    static StreamStart MakeSseStart(std::string first) {
        StreamStart start;
        start.mode = StreamStart::Mode::Chunked;
        start.status_code = 200;
        start.headers = {{std::string(kHeaderContentType), std::string(kMediaSse)},
                         {"X-Accel-Buffering", "no"},
                         {"Cache-Control", "no-cache"}};
        start.first_chunk = std::move(first);
        return start;
    }

    // 流结束响应帧判定（success response 携带 resultType=complete；订阅流上
    // 唯一含该特征且带 id 的帧）。串特征判定，不重复 core 序列化。
    static bool IsFlowEndFrame(const std::string& frame) {
        return frame.find("\"id\"") != std::string::npos &&
               frame.find("\"resultType\":\"complete\"") != std::string::npos;
    }

    // 锁内取首元素编码为 SSE chunk；流结束帧消费后置 m_eof（Next 后续返回 EOF）。
    std::string PopLocked() {
        auto frame = std::move(m_queue.front());
        m_queue.pop_front();
        if (m_eof_pending && m_queue.empty()) m_eof = true;
        std::string out = "data: ";
        out += frame;
        out += "\n\n";
        return out;
    }

    void Detach() {
        m_session->SetNotificationSink(McpSession::NotificationSink{});
        m_session->EndAllSubscriptions();  // S20 断连驱动订阅结束
        std::lock_guard<std::mutex> lock(m_mtx);
        m_closed = true;
    }

    // 空队挂起：min(options.deadline, now+keepalive_tick)；被 Push/WakeAll
    // Notify 唤醒为 Completed。挂起期间被 Cancel → 唤醒后由 m_cancelled 分支收口。
    CombinedWaitStatus WaitEvent(const bbt::coroutine::WaitOptions& options) {
        auto waiter = CoWaiter::Create();
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            if (m_cancelled || m_closed) return CombinedWaitStatus::Cancelled;
            if (!m_queue.empty() || m_eof) return CombinedWaitStatus::Completed;
            m_waiters.push_back(waiter);
        }
        CombinedWaitOptions wait;
        const auto tick = std::chrono::steady_clock::now() + m_keepalive;
        wait.deadline = std::min(options.deadline, tick);
        const auto status = waiter->Wait(wait);
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            for (auto it = m_waiters.begin(); it != m_waiters.end(); ++it)
                if (*it == waiter) { m_waiters.erase(it); break; }
        }
        return status;
    }

    void WakeAll() {
        std::vector<CoWaiter::SPtr> woken;
        {
            std::lock_guard<std::mutex> lock(m_mtx);
            woken.swap(m_waiters);
        }
        for (auto& waiter : woken) waiter->Notify();
    }

    std::shared_ptr<McpSession> m_session;
    std::size_t m_max_events;
    std::chrono::milliseconds m_keepalive;
    mutable std::mutex m_mtx;
    std::deque<std::string> m_queue;
    std::size_t m_dropped = 0;
    bool m_eof_pending = false;  // 流结束帧已入队（队中仍有前序帧）
    bool m_eof = false;          // 结束帧已交付，下次 Next → EOF
    bool m_closed = false;
    bool m_cancelled = false;
    std::vector<CoWaiter::SPtr> m_waiters;
};

} // namespace

struct McpHttpTransport::Impl {
    Impl(std::shared_ptr<McpServer> server, std::shared_ptr<net::Engine> engine,
         McpHttpOptions options)
        : m_server(std::move(server)), m_engine(std::move(engine)),
          m_options(std::move(options)) {}

    std::shared_ptr<McpServer> m_server;
    std::shared_ptr<net::Engine> m_engine;
    McpHttpOptions m_options;
    std::shared_ptr<http::Server> m_http;
    std::atomic<bool> m_closed{false};

    // ---- 头门禁 -----------------------------------------------------------------

    bool AcceptOk(const IncomingRequest& request) const {
        auto values = HeaderValues(request, kHeaderAccept);
        if (values.empty()) return false;
        bool json = false, sse = false;
        for (const auto v : values) {
            json = json || AcceptsMedia(v, kMediaJson);
            sse = sse || AcceptsMedia(v, kMediaSse);
        }
        return json && sse;
    }

    bool OriginOk(const IncomingRequest& request) const {
        auto values = HeaderValues(request, kHeaderOrigin);
        if (values.empty()) return true;  // 无 Origin：非浏览器客户端，放行
        for (const auto v : values)
            if (std::find(m_options.allowed_origins.begin(),
                          m_options.allowed_origins.end(),
                          std::string(Trim(v))) != m_options.allowed_origins.end())
                return true;
        return false;
    }

    // Host 白名单：host[:port] 比较（[v6] 剥括号、尾随点去除、大小写不敏感；
    // 条目带 :port 时完整比较端口）。
    bool HostOk(const IncomingRequest& request) const {
        auto host = SingleHeader(request, kHeaderHost);
        if (!host) return false;
        const std::string_view value = Trim(*host);
        std::string_view host_only, port;
        if (!SplitAuthority(value, host_only, port)) return false;
        for (const auto& allowed : m_options.allowed_hosts) {
            std::string_view e_host, e_port;
            if (!SplitAuthority(allowed, e_host, e_port)) continue;
            if (!EqualsNoCase(host_only, e_host)) continue;
            if (!e_port.empty() && e_port != port) continue;
            return true;
        }
        return false;
    }

    static bool SplitAuthority(std::string_view value, std::string_view& host,
                               std::string_view& port) {
        host = value;
        port = {};
        if (!value.empty() && value.front() == '[') {
            const auto end = value.find(']');
            if (end == std::string_view::npos) return false;
            host = value.substr(1, end - 1);
            if (end + 1 < value.size()) {
                if (value[end + 1] != ':') return false;
                port = value.substr(end + 2);
            }
        } else {
            const auto colon = value.find(':');
            if (colon != std::string_view::npos) {
                host = value.substr(0, colon);
                port = value.substr(colon + 1);
            }
        }
        if (!host.empty() && host.back() == '.') host.remove_suffix(1);
        return !host.empty();
    }

    // ---- 请求管线 ----------------------------------------------------------------

    result<ServerReply> Handle(IncomingCallContext, IncomingRequest request) {
        // 路径门禁（先于解析体）。
        if (request.target != kPath)
            return result<ServerReply>::ok(ServerReply{Empty(404)});
        if (request.method != "POST")
            return result<ServerReply>::ok(ServerReply{Empty(405)});
        if (!OriginOk(request))
            return result<ServerReply>::ok(ServerReply{JsonErrorBody(
                403, std::nullopt, JsonRpcErrorCode::InvalidRequest,
                "Origin is not allowed")});
        if (!HostOk(request))
            return result<ServerReply>::ok(ServerReply{JsonErrorBody(
                421, std::nullopt, JsonRpcErrorCode::InvalidRequest,
                "Host is not allowed")});
        if (!AcceptOk(request))
            return result<ServerReply>::ok(ServerReply{JsonErrorBody(
                406, std::nullopt, JsonRpcErrorCode::InvalidRequest,
                "Accept must include application/json and text/event-stream")});
        // MCP 请求体上限（http 层 max_body_bytes 是更大的物理上限；此处给出
        // 可诊断的 413 而非裸断连）。
        if (request.body.size() > m_options.max_body_bytes)
            return result<ServerReply>::ok(ServerReply{Empty(413)});

        auto session = CreateSession(m_server);
        // Sink 先于 dispatch 挂载：subscriptions/listen 的 ack 帧在 HandleFrame 内
        // 同步经 sink 发出（订阅 MUST 先 ack）；等 dispatch 返回再接 sink 会丢掉它。
        // 普通请求期间无订阅帧可路由（订阅在本会话不存在），捕获缓冲恒空。
        struct Captured {
            std::mutex mutex;
            std::deque<std::string> frames;
        };
        auto captured = std::make_shared<Captured>();
        {
            std::weak_ptr<Captured> sink_target = captured;
            session->SetNotificationSink([sink_target](const std::string& frame) {
                if (auto target = sink_target.lock()) {
                    std::lock_guard<std::mutex> lock(target->mutex);
                    target->frames.push_back(frame);
                }
            });
        }
        // 帧预解析：检测 id/通知/信封（core 侧会重解析并完整校验；此处只为头
        // 门禁与响应路由）。解析失败仍放行给 core → 标准 -32700/-32600。
        ProtocolLimits limits;
        // core 同口径：帧装配上限取 server 的 max_message_bytes（413 的 HTTP 层
        // max_body_bytes 是传输上限，不收紧协议帧上限）。
        limits.max_message_bytes = m_server->Options().max_message_bytes;
        limits.max_depth = m_server->Options().max_depth;
        std::optional<RequestId> detected_id;
        bool notification_recognized = false;
        auto parsed = ParseRequestFrame(request.body, limits, &detected_id,
                                        &notification_recognized);
        const bool modern = parsed && ModernEnvelope(parsed.value().params);

        // 帧不可解析 → 无 era 可言，路由头门禁不适用：放行给 core 的标准
        // -32700/-32600（头校验是帧级语义，垃圾帧无 method/版本可比）。
        if (parsed) {
            auto gate = HeaderGate(request, &parsed.value(), modern,
                                   detected_id, *session);
            if (gate) return result<ServerReply>::ok(ServerReply{std::move(*gate)});
        }

        auto outcome = session->HandleFrame(request.body);
        if (!outcome) {
            // 核心拒绝（未 Start/已 Close/超在途）：503 + JSON-RPC 错误 body。
            const auto failure = ToProtocolFailure(outcome.error());
            return result<ServerReply>::ok(ServerReply{JsonErrorBody(
                503, detected_id, failure.code, "Service unavailable")});
        }
        const DispatchOutcome& done = outcome.value();

        if (!done.has_response) {
            if (parsed && !parsed.value().notification &&
                parsed.value().method == kMethodSubscriptionsListen) {
                // dispatch 期间捕获的帧（首个是 ack）预置流内；Attach 后新帧
                // 直接进 body 队列。ack_sent 已在 HandleFrame 内置位，之后
                // PublishResourceChanged 的事件经 sink 路由，无窗口丢失。
                auto body = std::make_shared<McpEventBody>(
                    session, m_options.max_stream_events,
                    m_options.keepalive_interval);
                {
                    std::lock_guard<std::mutex> lock(captured->mutex);
                    while (!captured->frames.empty()) {
                        body->Push(captured->frames.front());
                        captured->frames.pop_front();
                    }
                }
                body->Attach();
                return result<ServerReply>::ok(
                    ServerReply{std::shared_ptr<http::ResponseBody>(body)});
            }
            return result<ServerReply>::ok(ServerReply{Empty(202)});
        }

        int status = 200;
        // 结构化错误码 → HTTP 状态（modern）：core 产出的 JSON-RPC error body 按
        // 顶层 error.code 判定，不做 body 子串匹配（子串可被 data/message 里的
        // 任意文本欺骗）。解析失败（core 不会产出非 JSON body，防御兜底）按
        // 200 处理，不凭猜测冒 4xx。
        //   -32601 → 404（modern 未知方法；SDK 原样上抛）
        //   -32022 → 400（UnsupportedProtocolVersionError：「For HTTP, the
        //     response status code MUST be 400 Bad Request」，2026-07-28 schema.ts）
        if (modern) {
            const auto code = ResponseErrorCode(done.response);
            if (code == JsonRpcErrorCode::MethodNotFound)
                status = 404;
            else if (code == JsonRpcErrorCode::UnsupportedProtocolVersion)
                status = 400;
        }
        return result<ServerReply>::ok(
            ServerReply{JsonBody(status, std::move(done.response))});
    }

    // JSON-RPC 响应帧的结构化错误码：解析 body → error.code（int64）。body 非
    // JSON、非 object、无 error 成员或 code 非整数 → nullopt（不判定）。
    static std::optional<JsonRpcErrorCode> ResponseErrorCode(
        const std::string& body) {
        auto frame = JsonValue::Parse(body, kResponseParseMaxBytes,
                                      kResponseParseMaxDepth);
        if (!frame || !frame.value().IsObject()) return std::nullopt;
        auto error = frame.value().Find("error");
        if (!error || !error.value().IsObject()) return std::nullopt;
        auto code = error.value().Find("code");
        if (!code) return std::nullopt;
        auto value = code.value().AsInt64();
        if (!value) return std::nullopt;
        return static_cast<JsonRpcErrorCode>(value.value());
    }
    // 响应体再解析上限：只判定结构化错误码，独立于请求帧上限（core 产出体
    // 恒远小于请求上限，4MiB/64 层是防御性兜底，不收紧协议）。
    static constexpr std::size_t kResponseParseMaxBytes = 4U << 20;
    static constexpr std::size_t kResponseParseMaxDepth = 64;

    // 路由头三件套（modern）+ legacy 会话门禁。返回 nullopt = 通过；否则为
    // 已构造的 400 + -32020 应答。
    std::optional<Response> HeaderGate(const IncomingRequest& request,
                                       const JsonRpcRequest* parsed, bool modern,
                                       const std::optional<RequestId>& detected_id,
                                       McpSession& session) const {
        auto mismatch = [&](const char* message) {
            return JsonErrorBody(400, detected_id, JsonRpcErrorCode::HeaderMismatch,
                                 message);
        };
        // 重复路由头即拒绝（第一/最后副本读者分歧）。
        for (const auto name : {kHeaderProtocolVersion, kHeaderMethod, kHeaderName})
            if (HeaderValues(request, name).size() > 1)
                return mismatch("duplicate MCP routing header");

        const auto version_header = SingleHeader(request, kHeaderProtocolVersion);
        const auto method_header = SingleHeader(request, kHeaderMethod);
        const auto name_header = SingleHeader(request, kHeaderName);

        if (modern) {
            std::string_view envelope_version;
            {
                if (parsed->params && parsed->params->IsObject()) {
                    auto meta = parsed->params->Find("_meta");
                    if (meta && meta.value().IsObject()) {
                        auto pv = meta.value().Find(std::string(kMetaKeyProtocolVersion));
                        if (pv && pv.value().IsString())
                            envelope_version = pv.value().AsString().value();
                    }
                }
            }
            if (!version_header || envelope_version.empty() ||
                *version_header != envelope_version)
                return mismatch(
                    "mcp-protocol-version header does not match the request "
                    "envelope's protocol version");
            if (!method_header || *method_header != parsed->method)
                return mismatch(
                    "mcp-method header does not match the request body's method");
            const bool name_method = parsed->method == "tools/call" ||
                                     parsed->method == "prompts/get" ||
                                     parsed->method == "resources/read";
            if (name_header && !name_method)
                return mismatch("mcp-name header present for a non-named method");
            if (name_method) {
                if (!name_header)
                    return mismatch("mcp-name header is required for this method");
                const std::string_view key =
                    parsed->method == "resources/read" ? "uri" : "name";
                std::optional<std::string> expected;
                if (parsed->params && parsed->params->IsObject()) {
                    auto body_value = parsed->params->Find(std::string(key));
                    if (body_value && body_value.value().IsString())
                        expected = body_value.value().AsString().value();
                }
                auto decoded = DecodeHeaderValue(*name_header);
                if (!decoded || !expected || *decoded != *expected)
                    return mismatch(
                        "mcp-name header does not match the request body's "
                        "parameter");
            }
            return std::nullopt;
        }

        // legacy 无 session profile：Mcp-Method/Mcp-Name 不属于 legacy 交通；
        // 版本头只在两真实 revision 时采纳连接会话门禁，其余拒绝。
        if (method_header || name_header)
            return mismatch(
                "mcp-method/mcp-name headers are not part of the legacy HTTP "
                "profile");
        if (version_header) {
            if (*version_header == kProtocolVersion2025_11_25)
                session.AdoptLegacyHeaderGate(std::string(*version_header));
            else
                return mismatch(
                    "mcp-protocol-version header is not a served legacy "
                    "revision");
        }
        return std::nullopt;
    }
};

// ---- 公共 API ------------------------------------------------------------------

result<std::shared_ptr<McpHttpTransport>> McpHttpTransport::Create(
    std::shared_ptr<McpServer> server, std::shared_ptr<net::Engine> engine,
    McpHttpOptions options) {
    using R = result<std::shared_ptr<McpHttpTransport>>;
    if (!server || !server->IsStarted())
        return R::err(MakeMcpHttpError(ErrorCode::InvalidContext,
                                     "mcp.http.server_not_started"));
    if (options.allowed_hosts.empty() || options.max_body_bytes == 0 ||
        options.max_chunk_bytes == 0 || options.max_stream_events == 0 ||
        options.max_stream_lifetime <= std::chrono::milliseconds{0} ||
        options.stream_write_timeout <= std::chrono::milliseconds{0} ||
        options.keepalive_interval <= std::chrono::milliseconds{0} ||
        options.max_stream_lifetime > kNetworkLimitsMaxIncomingTimeout ||
        options.stream_write_timeout > kNetworkLimitsMaxIncomingTimeout ||
        options.keepalive_interval > kNetworkLimitsMaxIncomingTimeout ||
        options.max_body_bytes > kNetworkLimitsMaxBodyBytes ||
        options.max_chunk_bytes > kNetworkLimitsMaxBodyBytes)
        return R::err(MakeMcpHttpError(ErrorCode::InvalidArgument,
                                     "mcp.http.bad_options"));
    if (!engine)
        return R::err(MakeMcpHttpError(ErrorCode::InvalidArgument,
                                     "mcp.http.null_engine"));

    auto impl = std::make_shared<Impl>(std::move(server), engine, std::move(options));
    std::weak_ptr<Impl> weak = impl;

    http::ServerOptions http_options;
    // MCP 的 max_body_bytes 是语义门禁（413）；http 的 Beast body_limit 取两者
    // 较大者兜底，保证超限请求能到达门禁拿到 413 而非裸断连。
    http_options.limits.max_body_bytes =
        std::max(impl->m_options.max_body_bytes, http_options.limits.max_body_bytes);
    http::StreamingOptions streaming;
    streaming.max_chunk_bytes = impl->m_options.max_chunk_bytes;
    streaming.max_stream_lifetime = impl->m_options.max_stream_lifetime;
    streaming.stream_write_timeout = impl->m_options.stream_write_timeout;

    auto made = http::Server::CreateStreaming(
        engine,
        [weak](IncomingCallContext context, IncomingRequest request) {
            auto impl = weak.lock();
            if (!impl)
                return result<ServerReply>::err(
                    MakeError(ErrorCode::Closed, "transport closed"));
            return impl->Handle(context, std::move(request));
        },
        http_options, streaming);
    if (!made) return R::err(made.error());
    impl->m_http = std::move(made).value();
    return R::ok(std::shared_ptr<McpHttpTransport>(new McpHttpTransport(std::move(impl))));
}

McpHttpTransport::McpHttpTransport(std::shared_ptr<Impl> impl) noexcept
    : m_impl(std::move(impl)) {}

McpHttpTransport::~McpHttpTransport() { Close(); }

result<void> McpHttpTransport::Start() {
    // Loopback 默认（architecture §8.4 Security）：未显式 opt-in 时，任一非
    // loopback 监听地址即拒。绑定地址在 Create 时（engine 尚 Idle，无
    // LocalAddresses 快照）不可知，故检查落在 Start——engine Running 后
    // LocalAddresses 是内核真实绑定（port 0 已解析）。allowed_hosts/
    // allowed_origins 防的是头伪造，与本检查职责不同，不代替它。
    if (!m_impl->m_options.allow_non_loopback) {
        std::vector<net::Address> addresses;
        if (!m_impl->m_engine->LocalAddresses(addresses)) {
            for (const auto& address : addresses) {
                if (!address.IsLoopback())
                    return result<void>::err(MakeMcpHttpError(
                        ErrorCode::UnsupportedRoute, "mcp.http.non_loopback"));
            }
        }
        // LocalAddresses 失败（非 Running 态）：http::Server::Start 会以自己的
        // Running 契约拒，不在这里重复编造错误。
    }
    return m_impl->m_http->Start();
}
void McpHttpTransport::StopAccepting() noexcept { m_impl->m_http->StopAccepting(); }
result<void> McpHttpTransport::Drain(bbt::coroutine::Deadline deadline) {
    return m_impl->m_http->Drain(deadline);
}
void McpHttpTransport::Close() noexcept {
    m_impl->m_closed.store(true, std::memory_order_release);
    m_impl->m_http->Close();
}
result<void> McpHttpTransport::WaitStopped(bbt::coroutine::Deadline deadline) {
    return m_impl->m_http->WaitStopped(deadline);
}
bool McpHttpTransport::IsClosed() const noexcept {
    return m_impl->m_closed.load(std::memory_order_acquire);
}

} // namespace bbt::infra::mcp
