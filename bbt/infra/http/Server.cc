#include <bbt/infra/http/Server.hpp>

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <utility>
#include <vector>

#include <boost/beast/http.hpp>
#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/Processer.hpp>
#include <bbt/infra/http/detail/Http11.hpp>
#include <bbt/infra/http/detail/ServerTestAccess.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/ICoConnection.hpp>

namespace bbt::infra::http {
namespace {
namespace wire = boost::beast::http;
using Clock = std::chrono::steady_clock;
using namespace std::chrono_literals;

result<void> Failure(ErrorCode code, const char* message) {
    return result<void>::err(MakeError(code, message));
}

bool NativeHost() {
    return bbt::coroutine::GetLocalCoroutineId() == 0 &&
           bbt::coroutine::detail::Processer::TryGetLocalProcesser() == nullptr;
}

bool Token(std::string_view text) {
    if (text.empty()) { return false; }
    for (const unsigned char c : text) {
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
              (c >= '0' && c <= '9') || std::string_view("!#$%&'*+-.^_`|~").find(c) != std::string_view::npos)) {
            return false;
        }
    }
    return true;
}

bool FieldValue(std::string_view text) {
    for (const unsigned char c : text) {
        if ((c < 32 && c != '\t') || c == 127) { return false; }
    }
    return true;
}

bool ValidHost(boost::beast::string_view host) {
    if (host.empty()) { return false; }
    boost::beast::string_view port;
    if (host.front() == '[') {
        const auto end = host.find(']');
        if (end == boost::beast::string_view::npos || end <= 1 || host.find('%') != boost::beast::string_view::npos) {
            return false;
        }
        std::error_code error;
        const auto address = net::Address::Ip(std::string(host.substr(1, end - 1)), 0, error);
        if (error || address.Family() != net::AddressFamily::IPv6) { return false; }
        if (end + 1 < host.size()) {
            if (host[end + 1] != ':') { return false; }
            port = host.substr(end + 2);
            if (port.empty()) { return false; }
        }
    } else {
        const auto colon = host.find(':');
        if (colon != boost::beast::string_view::npos) {
            port = host.substr(colon + 1);
            host = host.substr(0, colon);
            if (port.empty()) { return false; }
        }
        if (host.empty()) { return false; }
        // 本阶段明确采用 ASCII DNS/数值 host 子集，不接受 userinfo、路径或转义 authority。
        if (host.back() == '.') { host.remove_suffix(1); }
        if (host.empty()) { return false; }
        while (!host.empty()) {
            const auto dot = host.find('.');
            const auto label = host.substr(0, dot);
            if (label.empty() || label.size() > 63 || label.front() == '-' || label.back() == '-') { return false; }
            for (const unsigned char c : label) {
                if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                      (c >= '0' && c <= '9') || c == '-')) { return false; }
            }
            if (dot == boost::beast::string_view::npos) { break; }
            host.remove_prefix(dot + 1);
            if (host.empty()) { return false; }
        }
    }
    unsigned number = 0;
    for (const unsigned char c : port) {
        if (c < '0' || c > '9') { return false; }
        number = number * 10 + (c - '0');
        if (number > 65535) { return false; }
    }
    return true;
}

bool ValidRequestHead(const wire::request<wire::string_body>& request, std::size_t max_target) {
    const auto target = request.target();
    if (request.version() != 11 || target.empty() || target.front() != '/' ||
        target.size() > max_target || request.method() == wire::verb::connect ||
        request.count(wire::field::host) != 1 || !ValidHost(request[wire::field::host]) ||
        request.count(wire::field::expect) || request.count(wire::field::upgrade) ||
        wire::token_list(request[wire::field::connection]).exists("upgrade")) { return false; }
    const auto hex = [](unsigned char c) {
        return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
    };
    for (std::size_t i = 0; i < target.size(); ++i) {
        const unsigned char c = target[i];
        if (c == '%') {
            if (i + 2 >= target.size() || !hex(target[i + 1]) || !hex(target[i + 2])) { return false; }
            i += 2;
            continue;
        }
        if (!((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
              std::string_view("-._~!$&'()*+,;=:@/?").find(c) != std::string_view::npos)) { return false; }
    }
    return true;
}

result<std::string> SerializeResponse(Response response, bool head, bool keep_alive,
                                      const ServerOptions& options) {
    if (response.status_code < 200 || response.status_code > 599 ||
        response.body.size() > options.limits.max_body_bytes) {
        return result<std::string>::err(MakeError(ErrorCode::InvalidArgument, "invalid server response"));
    }
    wire::response<wire::string_body> message{static_cast<wire::status>(response.status_code), 11};
    std::size_t header_bytes = 0;
    for (const auto& field : response.headers) {
        header_bytes += field.first.size() + field.second.size() + 4;
        if (header_bytes > options.limits.max_header_bytes || !Token(field.first) || !FieldValue(field.second) ||
            boost::beast::iequals(field.first, "Content-Length") ||
            boost::beast::iequals(field.first, "Transfer-Encoding") ||
            boost::beast::iequals(field.first, "Connection") ||
            boost::beast::iequals(field.first, "Upgrade") || boost::beast::iequals(field.first, "Trailer")) {
            return result<std::string>::err(MakeError(ErrorCode::InvalidArgument, "invalid server response header"));
        }
        message.insert(field.first, field.second);
    }
    message.keep_alive(keep_alive);
    const bool no_content = response.status_code == 204 || response.status_code == 304;
    if (!no_content) {
        message.content_length(response.body.size());
        if (!head) { message.body() = std::move(response.body); }
    }
    {
        std::string bytes;
        detail::StringSink sink(bytes);
        boost::system::error_code error;
        wire::write(sink, message, error);
        if (error) {
            return result<std::string>::err(MakeError(ErrorCode::ProtocolError, error.message()));
        }
        return result<std::string>::ok(std::move(bytes));
    }
}

result<std::string> SerializeResponseHead(int status_code, const Headers& headers, bool head,
                                        bool chunked, bool keep_alive,
                                        const ServerOptions& options) {
    if (status_code < 200 || status_code > 599) {
        return result<std::string>::err(MakeError(ErrorCode::InvalidArgument, "invalid server response"));
    }
    if (chunked && (head || status_code == 204 || status_code == 304)) {
        return result<std::string>::err(MakeError(ErrorCode::InvalidArgument, "chunked reply disallowed"));
    }
    wire::response<wire::empty_body> message{static_cast<wire::status>(status_code), 11};
    std::size_t header_bytes = 0;
    for (const auto& field : headers) {
        header_bytes += field.first.size() + field.second.size() + 4;
        if (header_bytes > options.limits.max_header_bytes || !Token(field.first) || !FieldValue(field.second) ||
            boost::beast::iequals(field.first, "Content-Length") ||
            boost::beast::iequals(field.first, "Transfer-Encoding") ||
            boost::beast::iequals(field.first, "Connection") ||
            boost::beast::iequals(field.first, "Upgrade") || boost::beast::iequals(field.first, "Trailer")) {
            return result<std::string>::err(MakeError(ErrorCode::InvalidArgument, "invalid server response header"));
        }
        message.insert(field.first, field.second);
    }
    message.keep_alive(keep_alive);
    if (chunked) { message.chunked(true); }
    else if (status_code != 204 && status_code != 304) { message.content_length(0); }
    std::string bytes;
    detail::StringSink sink(bytes);
    wire::response_serializer<wire::empty_body> serializer{message};
    boost::system::error_code error;
    wire::write_header(sink, serializer, error);
    if (error) {
        return result<std::string>::err(MakeError(ErrorCode::ProtocolError, error.message()));
    }
    return result<std::string>::ok(std::move(bytes));
}

// chunked wire 块手工编码（十六进制长度 + CRLF + 数据 + CRLF）；Boost
// write_chunk 需要 serializer 状态，手工编码同等正确且不持有 serializer。
std::string EncodeChunk(std::string_view data) {
    static constexpr char kHex[] = "0123456789ABCDEF";
    char head[24];
    std::size_t size = data.size();
    int digits = 0;
    for (std::size_t v = size; v; v >>= 4) { ++digits; }
    if (digits == 0) { digits = 1; }
    for (int i = digits - 1; i >= 0; --i) { head[i] = kHex[size & 0xF]; size >>= 4; }
    std::string out(head, head + digits);
    out += "\r\n";
    out += data;
    out += "\r\n";
    return out;
}
} // namespace

struct Server::State : std::enable_shared_from_this<Server::State> {
    struct Session {
        net::Connection connection;
        bool authorized{false}; // 与 stopping 同 mutex 仲裁，包含回复发送责任
        bool task_done{false};
        std::shared_ptr<ResponseBody> source; // 活动流源（关闭时 Cancel 一次）
        bool source_cancelled{false};
    };
    // 最后一个 callback 捕获销毁时才减任务数；不是 handler 返回即算任务结束。
    struct TaskLease {
        std::shared_ptr<State> state;
        std::shared_ptr<Session> session;
        ~TaskLease() {
            if (!state) { return; } // 未计数（停止拒绝/失败回滚）无计数责任，无下溢
            std::lock_guard<std::mutex> lock(state->mutex);
            if (session) { session->task_done = true; }
            --state->tasks;
            state->cv.notify_all();
        }
    };

    std::shared_ptr<net::Engine> engine;
    ServerHandler handler;
    StreamingServerHandler streaming_handler; // 非空即 streaming server（CreateStreaming）
    ServerOptions options;
    StreamingOptions streaming{};
    mutable std::mutex mutex;
    std::condition_variable cv;
    bool started{false};
    bool stopping{false};
    bool closing{false};
    bool fail_next_task{false}; // 窄私有故障注入，不扩大公开 API
    std::size_t accepted{0}, requests{0}, inflight{0}, tasks{0};
    std::vector<std::shared_ptr<Session>> sessions;

    void Sweep() {
        sessions.erase(std::remove_if(sessions.begin(), sessions.end(), [](const auto& session) {
            return session->task_done && session->connection.Released();
        }), sessions.end());
    }

    bool Submit(const std::shared_ptr<Session>& session) {
        auto lease = std::make_shared<TaskLease>();
        bool registered = false;
        bool fail = false;
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (stopping) {
                // 持久封口：Stop/Close 后不再预约新任务，避免 WaitStopped 返回后
                // 任务复活。session 负责自身 task_done，由 Sweep 回收。
                if (session) { session->task_done = true; }
                return false; // lease 无 state（未计数），析构不 --tasks，无下溢
            }
            ++tasks;
            fail = std::exchange(fail_next_task, false);
            if (fail) {
                --tasks; // 立即回滚计数，lease 不承担计数责任
                if (session) { session->task_done = true; }
                return false;
            }
            lease->state = shared_from_this();
            lease->session = session;
        }
        g_scheduler->RegistCoroutineTask(128 * 1024, [lease] {
            auto state = lease->state;
            auto connection = lease->session;
            try {
                if (connection) { state->Serve(connection); }
                else { state->Accept(); }
            } catch (...) {
                // 包括分配/编解码异常；不让 runtime 任务异常遗失责任。
                if (!connection) { state->Stop(false); }
            }
            if (connection) {
                connection->connection.RequestClose();
                std::lock_guard<std::mutex> lock(state->mutex);
                if (connection->authorized) {
                    connection->authorized = false;
                    --state->inflight;
                    state->cv.notify_all();
                }
            }
        }, registered);
        return registered;
    }

    void Stop(bool force) {
        std::vector<std::shared_ptr<ResponseBody>> sources;
        {
            std::lock_guard<std::mutex> lock(mutex);
            stopping = true;
            closing = closing || force;
            for (const auto& session : sessions) {
                if (closing || !session->authorized) { session->connection.RequestClose(); }
                if (closing && session->source && !session->source_cancelled) {
                    session->source_cancelled = true;
                    sources.push_back(session->source);
                }
            }
            cv.notify_all();
        }
        // Cancel 在锁外：实现可能挂起或回调，不能持锁调用未知代码。
        for (const auto& source : sources) { source->Cancel(); }
        engine->RequestStopAccepting();
    }

    void Accept() {
        for (;;) {
            bool full;
            {
                std::lock_guard<std::mutex> lock(mutex);
                Sweep();
                if (stopping) { return; }
                full = sessions.size() >= options.limits.max_connections;
            }
            if (full) { bbtco_sleep(5); continue; }
            // 在取得连接前准备物理记账槽；接纳后不再首次分配责任记录。
            auto session = std::make_shared<Session>();
            const auto accept_result = engine->CoAccept({Clock::now() + 50ms});
            if (accept_result.error == std::errc::timed_out) { continue; }
            if (accept_result.error) { Stop(false); return; }
            session->connection = accept_result.connection.Raw();

            {
                std::lock_guard<std::mutex> lock(mutex);
                ++accepted;
                sessions.push_back(session); // 唯一 accept 生产者：物理额度未回收不重用
                if (stopping) {
                    session->connection.RequestClose();
                    session->task_done = true;
                    return;
                }
            }
            try {
                if (!Submit(session)) { session->connection.RequestClose(); }
            } catch (...) {
                session->connection.RequestClose();
                std::lock_guard<std::mutex> lock(mutex);
                session->task_done = true;
            }
            bbtco_yield;
        }
    }

    bool Write(const std::shared_ptr<Session>& session, const std::string& bytes,
               bbt::coroutine::Deadline deadline, std::chrono::milliseconds per_write) {
        deadline = std::min(deadline, Clock::now() + per_write);
        std::size_t offset = 0;
        std::size_t chunk = 16 * 1024;
        while (offset < bytes.size()) {
            {
                std::lock_guard<std::mutex> lock(mutex);
                if (closing) { return false; }
            }
            if (detail::CheckCall(deadline)) { return false; }
            const auto count = std::min(chunk, bytes.size() - offset);
            net::CoTcpConnection connection{session->connection};
            const auto report = connection.Write(bytes.substr(offset, count), {deadline});
            const auto& outcome = report.outcome;
            // 公开 Rejected + 无 operation = 零提交；有界减半探到 net 可接纳上限。
            // 不在已接纳/可能发送的路径重试，也不假设 Engine 默认写额度。
            if (report.wait_error == std::errc::no_buffer_space &&
                outcome.phase == net::WritePhase::Rejected && !report.operation.Valid() && count > 1) {
                chunk = count / 2;
                continue;
            }
            if (report.wait_error || outcome.phase != net::WritePhase::Done || outcome.error) {
                return false;
            }
            offset += count;
            bbtco_yield;
        }
        return true;
    }

    void Serve(const std::shared_ptr<Session>& session) {
        std::string pending;
        for (;;) {
            // session 状态机：ReadingRequest → Dispatching →（streaming server 时）
            // WaitingStreamStart → WritingBuffered | Streaming → Closing → Released。
            const auto deadline = Clock::now() + options.limits.incoming_timeout;
            wire::request_parser<wire::string_body> parser;
            parser.header_limit(static_cast<std::uint32_t>(options.limits.max_header_bytes));
            parser.body_limit(options.limits.max_body_bytes);
            bool checked_header = false;
            while (!parser.is_done()) {
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    if (stopping) { return; }
                }
                if (detail::CheckCall(deadline)) { return; }
                if (!pending.empty()) {
                    boost::system::error_code error;
                    const auto consumed = parser.put(boost::asio::buffer(pending), error);
                    pending.erase(0, consumed);
                    if (error && error != wire::error::need_more) { return; }
                    if (parser.is_header_done() && !checked_header) {
                        // eager=false 先停在 header：Expect 必须在等待 body 前拒绝。
                        if (!ValidRequestHead(parser.get(), options.max_target_bytes)) { return; }
                        checked_header = true;
                        parser.eager(true);
                    }
                    if (parser.is_done()) { break; }
                    if (consumed) { bbtco_yield; continue; }
                }
                net::CoTcpConnection connection{session->connection};
                const auto read = connection.Read({deadline});
                if (read.state != net::ReadState::Data) {
                    // EOF 前的完整请求已在上面交付；半包 EOF/错误明确放弃。
                    return;
                }
                read.data.AppendTo(pending);
                bbtco_yield;
            }
            if (!ValidRequestHead(parser.get(), options.max_target_bytes)) { return; }
            auto message = parser.release();
            const bool head = message.method() == wire::verb::head;
            bool keep_alive = message.keep_alive();
            IncomingRequest request;
            request.method = std::string(message.method_string());
            request.target = std::string(message.target());
            request.body = std::move(message.body());
            for (const auto& field : message) {
                request.headers.emplace_back(std::string(field.name_string()), std::string(field.value()));
            }
            {
                std::lock_guard<std::mutex> lock(mutex);
                if (stopping || Clock::now() >= deadline || inflight >= options.limits.max_inflight) { return; }
                session->authorized = true;
                ++requests;
                ++inflight;
            }
            bool sent = false;
            bool streaming_done = false;
            if (streaming_handler) {
                // Dispatching：handler 返回 Response 或 ResponseBody 拉取源。
                auto reply = result<ServerReply>::err(MakeError(ErrorCode::InternalError, "handler failed"));
                try { reply = streaming_handler({deadline, {}}, std::move(request)); }
                catch (...) { /* 回调异常隔离为 500；授权责任继续包含回复发送。 */ }
                if (!reply) { reply = result<ServerReply>::ok(ServerReply{Response{500, {}, {}}}); }
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    keep_alive = keep_alive && !stopping;
                }
                if (detail::CheckCall(deadline)) { /* 超总期限：明确放弃。 */ }
                else if (auto* buffered = std::get_if<Response>(&reply.value())) {
                    auto bytes = SerializeResponse(std::move(*buffered), head, keep_alive, options);
                    if (bytes) { sent = Write(session, bytes.value(), deadline, options.write_timeout); }
                } else {
                    auto source = std::get<std::shared_ptr<ResponseBody>>(std::move(reply.value()));
                    sent = ServeStream(session, std::move(source), head, keep_alive, deadline);
                    streaming_done = true;
                }
            } else {
                auto response = result<Response>::err(MakeError(ErrorCode::InternalError, "handler failed"));
                try { response = handler({deadline, {}}, std::move(request)); }
                catch (...) { /* 回调异常隔离为 500；授权责任继续包含回复发送。 */ }
                if (!response) { response = result<Response>::ok(Response{500, {}, {}}); }
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    keep_alive = keep_alive && !stopping;
                }
                if (!detail::CheckCall(deadline)) {
                    auto bytes = SerializeResponse(std::move(response).value(), head, keep_alive, options);
                    if (bytes) { sent = Write(session, bytes.value(), deadline, options.write_timeout); }
                }
            }
            if (!sent) { session->connection.RequestClose(); } // 明确放弃在计数结算前封写
            {
                std::lock_guard<std::mutex> lock(mutex);
                session->authorized = false;
                if (streaming_done) { session->source.reset(); }
                --inflight;
                cv.notify_all();
                if (stopping) { keep_alive = false; }
            }
            if (!sent || !keep_alive) { return; }
            bbtco_yield; // hot pipeline 每请求让出，不允许缓存命中无限占 worker
        }
    }

    // WaitingStreamStart → WritingBuffered | Streaming：Start 在总期限内提交；
    // commit 后切到独立流期限 min(stream_start + max_stream_lifetime, 之后每次
    // 写再取 stream_write_timeout 较早者)。commit 后源错误/超时/peer EOF 取消
    // 源并关闭连接（不重发、不写 trailer），只留本地诊断责任。
    bool ServeStream(const std::shared_ptr<Session>& session,
                     std::shared_ptr<ResponseBody> source, bool head, bool keep_alive,
                     bbt::coroutine::Deadline deadline) {
        bool cancel_now = false;
        {
            std::lock_guard<std::mutex> lock(mutex);
            session->source = source;
            // Close 可能已过 Stop() 的取消收集（注册前窗口）：补一次登记检查，
            // 与 Stop() 的「source 非空则取消」合起来保证恰好一次。
            if (closing && !session->source_cancelled) {
                session->source_cancelled = true;
                cancel_now = true;
            }
        }
        if (cancel_now) { source->Cancel(); return false; }
        auto started = result<StreamStart>::err(MakeError(ErrorCode::InternalError, "stream start failed"));
        try { started = bbt_co_await source->Start({deadline}); }
        catch (...) { }
        if (!started) {
            // 头提交前失败：经正常错误响应路径（500），不当作流错误。
            auto bytes = SerializeResponse({500, {}, {}}, head, keep_alive, options);
            return bytes && Write(session, bytes.value(), deadline, options.write_timeout);
        }
        StreamStart& start = started.value();
        if (start.mode == StreamStart::Mode::Buffered) {
            Response response{start.status_code, std::move(start.headers), std::move(start.body)};
            auto bytes = SerializeResponse(std::move(response), head, keep_alive, options);
            if (!bytes) { CancelSource(session, source); return false; }
            if (!Write(session, bytes.value(), deadline, options.write_timeout)) {
                CancelSource(session, source);
                return false;
            }
            return true;
        }
        auto head_bytes = SerializeResponseHead(start.status_code, start.headers, head, true,
                                                keep_alive, options);
        if (!head_bytes) { CancelSource(session, source); return false; }
        // commit：头发出后进入 Streaming；流期限从此时起算。
        if (!Write(session, head_bytes.value(), deadline, options.write_timeout)) {
            CancelSource(session, source);
            return false;
        }
        const auto stream_deadline = std::min(Clock::now() + streaming.max_stream_lifetime, deadline);
        auto write_chunk = [&](std::string_view data) {
            if (data.empty()) { return true; }
            if (data.size() > streaming.max_chunk_bytes) { return false; }
            const auto wire = EncodeChunk(data);
            return Write(session, wire, stream_deadline, streaming.stream_write_timeout);
        };
        if (!write_chunk(start.first_chunk)) { CancelSource(session, source); return false; }
        for (;;) {
            auto next = result<std::optional<std::string>>::err(
                MakeError(ErrorCode::InternalError, "stream next failed"));
            try { next = bbt_co_await source->Next({stream_deadline}); }
            catch (...) { }
            if (!next) { CancelSource(session, source); return false; } // commit 后源错误：取消源、关连接
            if (!next.value()) { return FinishStream(session); }          // EOF：0-chunk 收尾
            if (!write_chunk(next.value().value())) { CancelSource(session, source); return false; }
        }
    }

    bool FinishStream(const std::shared_ptr<Session>& session) {
        // 正常 EOF：写 0-chunk 终止流。
        return Write(session, "0\r\n\r\n", Clock::now() + streaming.stream_write_timeout,
                     streaming.stream_write_timeout);
    }

    void CancelSource(const std::shared_ptr<Session>& session,
                      const std::shared_ptr<ResponseBody>& source) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (session->source_cancelled) { return; }
            session->source_cancelled = true;
        }
        source->Cancel();
    }
};

void ServerTestAccess::FailNextTask(Server& server) {
    std::lock_guard<std::mutex> lock(server.m_state->mutex);
    server.m_state->fail_next_task = true;
}

Server::Server(std::shared_ptr<State> state) noexcept : m_state(std::move(state)) {}
Server::~Server() noexcept { Close(); }

result<std::shared_ptr<Server>> Server::Create(std::shared_ptr<net::Engine> engine,
                                              ServerHandler handler, ServerOptions options) {
    using R = result<std::shared_ptr<Server>>;
    const auto valid = ValidateNetworkLimits(options.limits);
    if (!engine || !handler || !valid || options.handoff_capacity == 0 ||
        options.handoff_capacity > kNetworkLimitsMaxConnections || options.max_target_bytes == 0 ||
        options.max_target_bytes > options.limits.max_header_bytes || options.write_timeout <= 0ms ||
        options.write_timeout > kNetworkLimitsMaxIncomingTimeout) {
        return R::err(MakeError(ErrorCode::InvalidArgument, "invalid HTTP Server options"));
    }
    if (engine->Phase() != net::EnginePhase::Idle) {
        return R::err(MakeError(ErrorCode::InvalidContext, "Server::Create requires Idle Engine"));
    }
    auto state = std::make_shared<State>();
    state->engine = std::move(engine);
    state->handler = std::move(handler);
    state->options = options;
    state->sessions.reserve(options.limits.max_connections);
    auto server = std::shared_ptr<Server>(new Server(state));
    if (state->engine->EnableAcceptHandoff(options.handoff_capacity)) {
        return R::err(MakeError(ErrorCode::InvalidContext, "accept handoff configuration failed"));
    }
    return R::ok(std::move(server));
}

result<std::shared_ptr<Server>> Server::CreateStreaming(
    std::shared_ptr<net::Engine> engine, StreamingServerHandler handler,
    ServerOptions options, StreamingOptions streaming) {
    using R = result<std::shared_ptr<Server>>;
    const auto valid = ValidateNetworkLimits(options.limits);
    if (!engine || !handler || !valid || options.handoff_capacity == 0 ||
        options.handoff_capacity > kNetworkLimitsMaxConnections || options.max_target_bytes == 0 ||
        options.max_target_bytes > options.limits.max_header_bytes || options.write_timeout <= 0ms ||
        options.write_timeout > kNetworkLimitsMaxIncomingTimeout ||
        streaming.max_chunk_bytes == 0 || streaming.max_chunk_bytes > kNetworkLimitsMaxBodyBytes ||
        streaming.max_stream_lifetime <= 0ms ||
        streaming.max_stream_lifetime > kNetworkLimitsMaxIncomingTimeout ||
        streaming.stream_write_timeout <= 0ms ||
        streaming.stream_write_timeout > kNetworkLimitsMaxIncomingTimeout) {
        return R::err(MakeError(ErrorCode::InvalidArgument, "invalid HTTP Server streaming options"));
    }
    if (engine->Phase() != net::EnginePhase::Idle) {
        return R::err(MakeError(ErrorCode::InvalidContext, "Server::CreateStreaming requires Idle Engine"));
    }
    auto state = std::make_shared<State>();
    state->engine = std::move(engine);
    state->streaming_handler = std::move(handler);
    state->options = options;
    state->streaming = streaming;
    state->sessions.reserve(options.limits.max_connections);
    auto server = std::shared_ptr<Server>(new Server(state));
    if (state->engine->EnableAcceptHandoff(options.handoff_capacity)) {
        return R::err(MakeError(ErrorCode::InvalidContext, "accept handoff configuration failed"));
    }
    return R::ok(std::move(server));
}

result<void> Server::Start() {
    auto state = m_state;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->stopping || state->started) { return Failure(ErrorCode::Closed, "Server already started or sealed"); }
        std::vector<net::Address> addresses;
        if (!g_scheduler->IsInitialized() || state->engine->Phase() != net::EnginePhase::Running ||
            state->engine->LocalAddresses(addresses) || addresses.empty()) {
            return Failure(ErrorCode::InvalidContext, "Server::Start requires Running listening Engine");
        }
        state->started = true;
    }
    try {
        if (state->Submit({})) { return result<void>::ok(); }
    } catch (...) { }
    state->Stop(false);
    return Failure(ErrorCode::RuntimeUnavailable, "accept task registration failed");
}

void Server::StopAccepting() noexcept { m_state->Stop(false); }
void Server::Close() noexcept { m_state->Stop(true); }

result<void> Server::Drain(bbt::coroutine::Deadline deadline) {
    if (!NativeHost()) { return Failure(ErrorCode::InvalidContext, "Drain requires native host"); }
    auto state = m_state;
    std::unique_lock<std::mutex> lock(state->mutex);
    if (!state->stopping) { return Failure(ErrorCode::InvalidArgument, "StopAccepting before Drain"); }
    if (!state->cv.wait_until(lock, deadline, [&] { return state->inflight == 0; })) {
        return Failure(ErrorCode::TimedOut, "Drain deadline expired; handler/reply responsibility retained");
    }
    return result<void>::ok();
}

result<void> Server::WaitStopped(bbt::coroutine::Deadline deadline) {
    if (!NativeHost()) { return Failure(ErrorCode::InvalidContext, "WaitStopped requires native host"); }
    auto state = m_state;
    std::unique_lock<std::mutex> lock(state->mutex);
    if (!state->stopping) { return Failure(ErrorCode::InvalidArgument, "StopAccepting before WaitStopped"); }
    for (;;) {
        state->Sweep();
        if (state->tasks == 0 && state->sessions.empty() && state->engine->AcceptingStopped()) {
            return result<void>::ok();
        }
        if (Clock::now() >= deadline) { return Failure(ErrorCode::TimedOut, "Server physical stop deadline expired"); }
        state->cv.wait_until(lock, std::min(deadline, Clock::now() + 5ms));
    }
}

ServerStats Server::Stats() const noexcept {
    auto state = m_state;
    std::lock_guard<std::mutex> lock(state->mutex);
    state->Sweep();
    std::size_t retiring = 0;
    for (const auto& session : state->sessions) {
        if (session->task_done || session->connection.Closed()) { ++retiring; }
    }
    return {state->accepted, state->requests, state->sessions.size(), retiring, state->inflight, state->tasks};
}

} // namespace bbt::infra::http
