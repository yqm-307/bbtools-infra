#include <bbt/infra/http/Client.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <exception>
#include <limits>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>
#include <bbt/coroutine/detail/Processer.hpp>

#include <bbt/infra/http/detail/Http11.hpp>
#include <bbt/infra/http/detail/Pool.hpp>

namespace bbt::infra::http {
namespace {

using detail::Origin;

Error InvalidArgument(std::string message)
{
    return MakeError(ErrorCode::InvalidArgument, "http client: " + std::move(message));
}

Error Unsupported(std::string message)
{
    return MakeError(ErrorCode::UnsupportedRoute, "http client: " + std::move(message));
}

bool EqualsNoCase(std::string_view left, std::string_view right)
{
    if (left.size() != right.size()) {
        return false;
    }
    for (std::size_t i = 0; i < left.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(left[i])) != std::tolower(static_cast<unsigned char>(right[i]))) {
            return false;
        }
    }
    return true;
}

bool Token(std::string_view value)
{
    if (value.empty()) {
        return false;
    }
    for (unsigned char c : value) {
        const bool alpha = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
        if (!alpha && !(c >= '0' && c <= '9')
            && std::string_view("!#$%&'*+-.^_`|~").find(c) == std::string_view::npos) {
            return false;
        }
    }
    return true;
}

result<void> ValidateHeaders(const Headers& headers)
{
    std::size_t bytes = 0;
    unsigned hosts = 0;
    for (const auto& header : headers) {
        if (!Token(header.first)) {
            return result<void>::err(InvalidArgument("invalid header name"));
        }
        if (header.first.size() > 8192 || header.second.size() > 32768
            || (bytes += header.first.size() + header.second.size() + 4) > 65536) {
            return result<void>::err(InvalidArgument("request headers exceed limit"));
        }
        for (unsigned char c : header.second) {
            if ((c < 32 && c != '\t') || c == 127) {
                return result<void>::err(InvalidArgument("invalid header value"));
            }
        }
        if (EqualsNoCase(header.first, "Content-Length") || EqualsNoCase(header.first, "Transfer-Encoding")) {
            return result<void>::err(InvalidArgument("request framing is managed by Client"));
        }
        if (EqualsNoCase(header.first, "Host") && ++hosts > 1) {
            return result<void>::err(InvalidArgument("duplicate Host header"));
        }
    }
    return result<void>::ok();
}

/**
 * 最小百分号编码：保留 unreserved，其余 %XX（空格同样编码，不用 +）。
 */
std::string PercentEncode(std::string_view value)
{
    static const char* hex = "0123456789ABCDEF";
    std::string out;
    out.reserve(value.size());
    for (const char byte : value) {
        const auto c = static_cast<unsigned char>(byte);
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_'
            || c == '.' || c == '~') {
            out.push_back(static_cast<char>(c));
        } else {
            out.push_back('%');
            out.push_back(hex[c >> 4]);
            out.push_back(hex[c & 0xF]);
        }
    }
    return out;
}

struct ParsedUrl {
    Origin origin;
    net::Address address;

    /**
     * 含起始 '/'
     */
    std::string path;

    /**
     * URL 自带 query（原样透传）
     */
    std::string query;
};

/**
 * 仅 http scheme 与数值 IP host（DNS 未交付：显式报错，不静默降级）。
 */
result<ParsedUrl> ParseUrl(const std::string& url)
{
    for (unsigned char c : url) {
        if (c <= 32 || c == 127) {
            return result<ParsedUrl>::err(InvalidArgument("invalid URL character"));
        }
    }
    const auto scheme_end = url.find("://");
    if (scheme_end == std::string::npos) {
        return result<ParsedUrl>::err(InvalidArgument("url missing scheme: " + url));
    }
    const std::string scheme = url.substr(0, scheme_end);
    if (EqualsNoCase(scheme, "https")) {
        return result<ParsedUrl>::err(Unsupported("https/TLS 未交付，本轮仅支持 http"));
    }
    if (!EqualsNoCase(scheme, "http")) {
        return result<ParsedUrl>::err(Unsupported("unsupported scheme: " + scheme));
    }
    std::string rest = url.substr(scheme_end + 3);
    rest.erase(rest.find('#') == std::string::npos ? rest.size() : rest.find('#'));
    std::string authority = rest;
    std::string path = "/";
    std::string query;
    const auto path_start = rest.find('/');
    const auto query_start = rest.find('?');
    std::size_t authority_end = rest.size();
    if (path_start != std::string::npos) {
        authority_end = std::min(authority_end, path_start);
    }
    if (query_start != std::string::npos) {
        authority_end = std::min(authority_end, query_start);
    }
    authority = rest.substr(0, authority_end);
    if (path_start != std::string::npos) {
        const auto from = path_start;
        if (query_start != std::string::npos && query_start > path_start) {
            path = rest.substr(from, query_start - from);
        } else if (query_start == std::string::npos) {
            path = rest.substr(from);
        } else {
            path = "/";
        }
    }
    if (query_start != std::string::npos) {
        query = rest.substr(query_start + 1);
    }

    std::string host;
    int port = 80;
    auto parse_port = [&](std::string_view text) {
        const auto parsed = std::from_chars(text.data(), text.data() + text.size(), port);
        return parsed.ec == std::errc{} && parsed.ptr == text.data() + text.size();
    };
    if (!authority.empty() && authority.front() == '[') {

        /**
         * IPv6 字面量：[::1]:8080
         */
        const auto close = authority.find(']');
        if (close == std::string::npos) {
            return result<ParsedUrl>::err(InvalidArgument("ipv6 literal missing ']'"));
        }
        host = authority.substr(1, close - 1);
        if (close + 2 < authority.size() && authority[close + 1] == ':') {
            if (!parse_port(std::string_view(authority).substr(close + 2))) {
                return result<ParsedUrl>::err(InvalidArgument("invalid URL port"));
            }
        } else if (close + 1 < authority.size()) {
            return result<ParsedUrl>::err(InvalidArgument("ipv6 authority trailing garbage"));
        }
    } else {
        const auto colon = authority.rfind(':');

        /**
         * 仅一个冒号（IPv4/host:port）
         */
        if (colon != std::string::npos && colon + 1 < authority.size() && authority.find(':') == colon) {
            host = authority.substr(0, colon);
            if (!parse_port(std::string_view(authority).substr(colon + 1))) {
                return result<ParsedUrl>::err(InvalidArgument("invalid URL port"));
            }
        } else {
            host = authority;
        }
    }
    if (host.empty()) {
        return result<ParsedUrl>::err(InvalidArgument("url missing host"));
    }
    if (port <= 0 || port > 65535) {
        return result<ParsedUrl>::err(InvalidArgument("url port out of range"));
    }
    Origin origin{"http", host, static_cast<std::uint16_t>(port)};
    std::error_code error;
    auto address = net::Address::Ip(host, port, error);
    if (error) {

        /**
         * net Address 仅数值：主机名在这里被拒绝（DNS 未交付，非静默降级）。
         */
        return result<ParsedUrl>::err(InvalidArgument("host must be numeric IP (DNS 未交付): " + host));
    }
    ParsedUrl out;
    out.origin = std::move(origin);
    out.address = address;
    out.path = path.empty() ? "/" : path;
    out.query = query;
    return result<ParsedUrl>::ok(std::move(out));
}

std::string HostHeader(const ParsedUrl& parsed)
{
    auto host = parsed.origin.host;
    if (host.find(':') != std::string::npos) {
        host = "[" + host + "]";
    }
    if (parsed.origin.port != 80) {
        host += ":" + std::to_string(parsed.origin.port);
    }
    return host;
}

result<ParsedUrl> ValidatePrepared(const PreparedRequest& request)
{
    if (!Token(request.method)) {
        return result<ParsedUrl>::err(InvalidArgument("invalid method"));
    }
    if (request.method == "CONNECT") {
        return result<ParsedUrl>::err(Unsupported("CONNECT tunnel unsupported"));
    }
    if (auto headers = ValidateHeaders(request.headers); !headers) {
        return result<ParsedUrl>::err(std::move(headers.error()));
    }
    unsigned hosts = 0;
    for (const auto& header : request.headers) {
        if (EqualsNoCase(header.first, "Host") && !header.second.empty()) {
            ++hosts;
        }
    }
    if (hosts != 1) {
        return result<ParsedUrl>::err(InvalidArgument("exactly one Host required"));
    }
    for (const auto phase :
         {request.connect_timeout, request.read_timeout, request.write_timeout, request.pool_timeout}) {
        if (phase <= std::chrono::milliseconds{0}) {
            return result<ParsedUrl>::err(InvalidArgument("timeout must be positive or disabled"));
        }
    }
    auto parsed = ParseUrl(request.url);
    if (!parsed) {
        return parsed;
    }
    const auto& url = parsed.value();
    const auto target = url.path + (url.query.empty() ? "" : "?" + url.query);
    if (request.target_for_wire != target || request.target_for_wire.find('#') != std::string::npos
        || request.address.ToString() != url.address.ToString() || request.host_header != HostHeader(url)) {
        return result<ParsedUrl>::err(InvalidArgument("inconsistent prepared URL/address/target"));
    }
    return parsed;
}

// clang-format off
}
// clang-format on

ClientOptions& ClientOptions::SetTimeout(std::chrono::milliseconds value) noexcept
{
    connect_timeout = read_timeout = write_timeout = pool_timeout = value;
    return *this;
}

ClientOptions& ClientOptions::SetConnectTimeout(std::chrono::milliseconds value) noexcept
{
    connect_timeout = value;
    return *this;
}

ClientOptions& ClientOptions::SetReadTimeout(std::chrono::milliseconds value) noexcept
{
    read_timeout = value;
    return *this;
}

ClientOptions& ClientOptions::SetWriteTimeout(std::chrono::milliseconds value) noexcept
{
    write_timeout = value;
    return *this;
}

ClientOptions& ClientOptions::SetPoolTimeout(std::chrono::milliseconds value) noexcept
{
    pool_timeout = value;
    return *this;
}

ClientOptions& ClientOptions::DisableTimeout() noexcept
{
    return SetTimeout(std::chrono::milliseconds::max());
}

ClientOptions& ClientOptions::DisableConnectTimeout() noexcept
{
    return SetConnectTimeout(std::chrono::milliseconds::max());
}

ClientOptions& ClientOptions::DisableReadTimeout() noexcept
{
    return SetReadTimeout(std::chrono::milliseconds::max());
}

ClientOptions& ClientOptions::DisableWriteTimeout() noexcept
{
    return SetWriteTimeout(std::chrono::milliseconds::max());
}

ClientOptions& ClientOptions::DisablePoolTimeout() noexcept
{
    return SetPoolTimeout(std::chrono::milliseconds::max());
}

RequestOptions& RequestOptions::SetTimeout(std::chrono::milliseconds value) noexcept
{
    connect_timeout = read_timeout = write_timeout = pool_timeout = value;
    return *this;
}

RequestOptions& RequestOptions::SetConnectTimeout(std::chrono::milliseconds value) noexcept
{
    connect_timeout = value;
    return *this;
}

RequestOptions& RequestOptions::SetReadTimeout(std::chrono::milliseconds value) noexcept
{
    read_timeout = value;
    return *this;
}

RequestOptions& RequestOptions::SetWriteTimeout(std::chrono::milliseconds value) noexcept
{
    write_timeout = value;
    return *this;
}

RequestOptions& RequestOptions::SetPoolTimeout(std::chrono::milliseconds value) noexcept
{
    pool_timeout = value;
    return *this;
}

RequestOptions& RequestOptions::DisableTimeout() noexcept
{
    return SetTimeout(std::chrono::milliseconds::max());
}

RequestOptions& RequestOptions::DisableConnectTimeout() noexcept
{
    return SetConnectTimeout(std::chrono::milliseconds::max());
}

RequestOptions& RequestOptions::DisableReadTimeout() noexcept
{
    return SetReadTimeout(std::chrono::milliseconds::max());
}

RequestOptions& RequestOptions::DisableWriteTimeout() noexcept
{
    return SetWriteTimeout(std::chrono::milliseconds::max());
}

RequestOptions& RequestOptions::DisablePoolTimeout() noexcept
{
    return SetPoolTimeout(std::chrono::milliseconds::max());
}

/**
 * State：公开句柄复制 state 后挂起，恢复后不再访问公开对象（仓内同范式）。
 */
struct Client::State {
    State(std::shared_ptr<net::Engine> engine_arg, ClientOptions options_arg)
        : engine(std::move(engine_arg))
        , options(std::move(options_arg))
        , pool(engine, options.limits, options.max_body_bytes, options.max_header_bytes)
    {
    }

    const std::shared_ptr<net::Engine> engine;
    const ClientOptions options;
    detail::ConnectionPool pool;
};

result<std::shared_ptr<Client>> Client::Create(std::shared_ptr<net::Engine> engine, ClientOptions options)
{
    if (!engine || engine->Phase() == net::EnginePhase::Stopped) {
        return result<std::shared_ptr<Client>>::err(InvalidArgument("engine required (client 不自建执行底座)"));
    }
    if (options.limits.max_connections == 0) {
        return result<std::shared_ptr<Client>>::err(
            InvalidArgument("limits.max_connections must be greater than zero"));
    }
    if (options.max_body_bytes == 0) {
        return result<std::shared_ptr<Client>>::err(InvalidArgument("max_body_bytes must be greater than zero"));
    }
    if (options.max_request_body_bytes && *options.max_request_body_bytes == 0) {
        return result<std::shared_ptr<Client>>::err(
            InvalidArgument("max_request_body_bytes must be greater than zero"));
    }
    if (options.max_header_bytes && *options.max_header_bytes == 0) {
        return result<std::shared_ptr<Client>>::err(InvalidArgument("max_header_bytes must be greater than zero"));
    }
    if (options.max_header_bytes && *options.max_header_bytes > std::numeric_limits<std::uint32_t>::max()) {
        return result<std::shared_ptr<Client>>::err(InvalidArgument("max_header_bytes exceeds HTTP parser limit"));
    }
    if (auto headers = ValidateHeaders(options.headers); !headers) {
        return result<std::shared_ptr<Client>>::err(std::move(headers.error()));
    }
    if (options.limits.keepalive_expiry && *options.limits.keepalive_expiry < std::chrono::milliseconds{0}) {
        return result<std::shared_ptr<Client>>::err(InvalidArgument("negative keepalive expiry"));
    }

    /**
     * base_url 允许为空（每请求给绝对 URL）；非空则必须能解析。
     */
    if (!options.base_url.empty()) {
        auto parsed = ParseUrl(options.base_url);
        if (!parsed) {
            return result<std::shared_ptr<Client>>::err(std::move(parsed.error()));
        }
    }
    for (const auto phase :
         {options.connect_timeout, options.read_timeout, options.write_timeout, options.pool_timeout}) {
        if (phase <= std::chrono::milliseconds{0}) {
            return result<std::shared_ptr<Client>>::err(InvalidArgument("timeout must be positive or disabled"));
        }
    }
    return result<std::shared_ptr<Client>>::ok(
        std::shared_ptr<Client>(new Client(std::make_shared<State>(std::move(engine), std::move(options)))));
}

Client::Client(std::shared_ptr<State> state) noexcept
    : m_state(std::move(state))
{
}

Client::~Client() noexcept
{
    _BeginClose();
}

result<PreparedRequest>
Client::BuildRequest(std::string_view method, std::string_view url, const RequestOptions& options) const
{
    const auto state = m_state;
    if (!Token(method)) {
        return result<PreparedRequest>::err(InvalidArgument("method must not be empty"));
    }
    std::string absolute(url);
    if (absolute.find("://") == std::string::npos) {
        if (state->options.base_url.empty()) {
            return result<PreparedRequest>::err(InvalidArgument("relative url requires client base_url"));
        }
        if (!absolute.empty() && absolute.front() != '/') {
            absolute.insert(absolute.begin(), '/');
        }
        auto base = state->options.base_url;
        if (!base.empty() && base.back() == '/' && !absolute.empty() && absolute.front() == '/') {
            base.pop_back();
        }
        absolute = base + absolute;
    }
    auto parsed = ParseUrl(absolute);
    if (!parsed) {
        return result<PreparedRequest>::err(std::move(parsed.error()));
    }

    /**
     * headers merge：client 打底，同名（不区分大小写）请求级覆盖。
     */
    Headers headers = state->options.headers;
    for (const auto& header : options.headers) {
        bool replaced = false;
        for (auto& existing : headers) {
            if (EqualsNoCase(existing.first, header.first)) {
                existing.second = header.second;
                replaced = true;
                break;
            }
        }
        if (!replaced) {
            headers.emplace_back(header.first, header.second);
        }
    }
    bool has_host = false;
    for (const auto& header : headers) {
        if (EqualsNoCase(header.first, "Host")) {
            has_host = true;
            break;
        }
    }
    std::string host_header = HostHeader(parsed.value());
    if (!has_host) {
        headers.emplace_back("Host", host_header);
    }

    /**
     * params merge：client 在前，请求级追加在后；URL 自带 query 透传保留。
     */
    QueryParams params = state->options.params;
    for (const auto& param : options.params) {
        params.push_back(param);
    }
    std::string target = parsed.value().path;
    std::string query = parsed.value().query;
    for (const auto& param : params) {
        if (!query.empty()) {
            query.push_back('&');
        }
        query += PercentEncode(param.first) + "=" + PercentEncode(param.second);
    }
    if (!query.empty()) {
        target += "?" + query;
    }

    PreparedRequest request;
    request.method.assign(method);
    request.url = "http://" + host_header + target;
    request.target_for_wire = std::move(target);
    request.address = parsed.value().address;
    request.host_header = std::move(host_header);
    request.headers = std::move(headers);
    request.connect_timeout = options.connect_timeout.value_or(state->options.connect_timeout);
    request.read_timeout = options.read_timeout.value_or(state->options.read_timeout);
    request.write_timeout = options.write_timeout.value_or(state->options.write_timeout);
    request.pool_timeout = options.pool_timeout.value_or(state->options.pool_timeout);
    auto valid = ValidatePrepared(request);
    if (!valid) {
        return result<PreparedRequest>::err(std::move(valid.error()));
    }
    return result<PreparedRequest>::ok(std::move(request));
}

bbt_co_async(result<Response>) Client::Get(std::string_view url, const RequestOptions& options, const CallOptions& call)
{
    auto request = BuildRequest("GET", url, options);
    if (!request) {
        return result<Response>::err(std::move(request.error()));
    }
    return Send(request.value(), call);
}

bbt_co_async(result<Response>) Client::Post(
    std::string_view url, RequestBody body, const RequestOptions& options, const CallOptions& call)
{
    auto request = BuildRequest("POST", url, options);
    if (!request) {
        return result<Response>::err(std::move(request.error()));
    }

    /**
     * body 与 Content-Type：请求级 headers 已含同名头时不覆盖。
     */
    if (!body.content_type.empty()) {
        bool has_type = false;
        for (const auto& header : request.value().headers) {
            if (EqualsNoCase(header.first, "Content-Type")) {
                has_type = true;
                break;
            }
        }
        if (!has_type) {
            request.value().headers.emplace_back("Content-Type", body.content_type);
        }
    }
    request.value().body = std::move(body.content);
    return Send(request.value(), call);
}

bbt_co_async(result<Response>) Client::Request(
    std::string_view method, std::string_view url, const RequestOptions& options, const CallOptions& call)
{
    auto request = BuildRequest(method, url, options);
    if (!request) {
        return result<Response>::err(std::move(request.error()));
    }
    return Send(request.value(), call);
}

bbt_co_async(result<Response>) Client::Send(const PreparedRequest& request, const CallOptions& call)
{
    const auto state = m_state;
    if (bbt::coroutine::GetLocalCoroutineId() == 0) {
        return result<Response>::err(MakeError(ErrorCode::InvalidContext, "http client: coroutine context required"));
    }
    if (state->pool.IsClosed()) {
        if (const auto reason = state->pool.CloseReason()) {
            return result<Response>::err(*reason);
        }
        return result<Response>::err(MakeError(ErrorCode::Closed, "http client: closed"));
    }

    /**
     * 封口引起的失败以关闭原因对外；已提交（请求可能已生效）的 OutcomeUnknown
     * 事实不追改，只附加关闭原因供排障。
     */
    const auto fail = [&state](Error error) {
        const auto reason = state->pool.CloseReason();
        if (!reason) {
            return result<Response>::err(std::move(error));
        }
        if (error.request_phase && *error.request_phase == RequestPhase::RequestCommitted) {
            error.details.emplace_back("close_reason", reason->message);
            return result<Response>::err(std::move(error));
        }
        return result<Response>::err(*reason);
    };
    try {
        auto valid = ValidatePrepared(request);
        if (!valid) {
            return result<Response>::err(std::move(valid.error()));
        }
        if (const auto gate = detail::CheckCall(call.deadline)) {
            return result<Response>::err(MakeError(
                gate == std::errc::operation_canceled ? ErrorCode::Cancelled : ErrorCode::TimedOut,
                "http client: call ended before submission"));
        }
        auto serialized = detail::SerializeRequest(request);
        if (!serialized) {
            return result<Response>::err(std::move(serialized.error()));
        }
        if (state->options.max_request_body_bytes && request.body.size() > *state->options.max_request_body_bytes) {
            return result<Response>::err(InvalidArgument("request body exceeds max_request_body_bytes"));
        }
        const auto deadlines = detail::ResolveDeadlines(request, call.deadline, std::chrono::steady_clock::now());
        auto leased = bbt_co_await state->pool.Acquire(valid.value().origin, request.address, deadlines);
        if (!leased) {
            return fail(std::move(leased.error()));
        }
        auto exchanged = bbt_co_await leased.value().connection->Exchange(request, serialized.value(), deadlines);
        if (!exchanged) {
            return fail(std::move(exchanged.error()));
        }
        leased.value().Release(exchanged.value().keep_alive);
        return result<Response>::ok(std::move(exchanged.value().response));
    } catch (const std::exception& error) {
        return result<Response>::err(MakeError(ErrorCode::InternalError, "http client: " + std::string(error.what())));
    }
}

void Client::_BeginClose(Error reason) noexcept
{
    const auto state = m_state;
    if (!state) {
        return;
    }
    try {
        (void)state->pool.Close(bbt::infra::CloseOptions::Immediate(std::move(reason)));
    } catch (...) {

        /**
         * noexcept 契约：封口本身已生效（布尔位先写），原因复制失败不放大。
         */
    }
}

result<void> Client::Close(const CloseOptions& options)
{
    const auto state = m_state;
    if (!state) {
        return result<void>::err(MakeError(ErrorCode::Closed, "http client: closed"));
    }
    return state->pool.Close(options);
}

bool Client::IsClosed() const noexcept
{
    const auto state = m_state;
    return state && state->pool.IsClosed();
}

std::optional<Error> Client::CloseReason() const noexcept
{
    const auto state = m_state;
    return state ? state->pool.CloseReason() : std::nullopt;
}

result<void> Client::_WaitReleased(bbt::coroutine::Deadline deadline)
{
    const auto state = m_state;
    if (!state) {
        return result<void>::err(MakeError(ErrorCode::Closed, "http client: closed"));
    }
    return state->pool.Close(bbt::infra::CloseOptions{bbt::infra::CloseMode::Immediate, true, {}, deadline});
}

PoolStats Client::Stats() const noexcept
{
    const auto state = m_state;
    return state ? state->pool.Stats() : PoolStats{};
}

// clang-format off
}
// clang-format on

namespace bbt::infra::http {

}
