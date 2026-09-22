#include "http/HttpDetail.hpp"

#include <cctype>

#include <boost/asio/error.hpp>
#include <boost/beast/http/error.hpp>

#include <bbt/coroutine/detail/Define.hpp>

namespace bbt::infra::http_detail {

namespace {

bool EqualNoCase(std::string_view a, std::string_view b) noexcept {
    if (a.size() != b.size())
        return false;
    for (std::size_t i = 0; i < a.size(); ++i) {
        const unsigned char ca = static_cast<unsigned char>(a[i]);
        const unsigned char cb = static_cast<unsigned char>(b[i]);
        if (std::tolower(ca) != std::tolower(cb))
            return false;
    }
    return true;
}

bool HasBadChar(std::string_view s) noexcept {
    for (char ch : s) {
        const unsigned char c = static_cast<unsigned char>(ch);
        if (c <= 0x20 || c == 0x7F)
            return true;
    }
    return false;
}

result<void> InvalidArg(const char* domain_code, std::string message) {
    Error e = MakeError(ErrorCode::InvalidArgument, std::move(message));
    e.domain_code = domain_code;
    return result<void>::err(std::move(e));
}

// beast http 错误经 make_error_code 落到 detail::http_error_category
// （impl/error.ipp），无公开具名访问器；经枚举实例取类别做等值比较。
const boost::system::error_category& HttpErrorCat() {
    return boost::system::error_code(
        boost::beast::http::error::end_of_stream).category();
}

// authority := host | host:port | [v6lit] | [v6lit]:port；不含 userinfo。
result<ParsedUrl> ParseAuthority(std::string_view authority) {
    ParsedUrl out;
    if (authority.empty() || authority.find('@') != std::string_view::npos)
        return result<ParsedUrl>::err(InvalidArg("bad_authority",
            "http url: empty authority or userinfo not allowed").error());

    std::string_view host;
    std::string_view port_sv;
    if (authority.front() == '[') {
        const auto close = authority.find(']');
        if (close == std::string_view::npos)
            return result<ParsedUrl>::err(InvalidArg("bad_authority",
                "http url: unterminated ipv6 literal").error());
        host = authority.substr(0, close + 1);
        if (close + 1 < authority.size()) {
            if (authority[close + 1] != ':')
                return result<ParsedUrl>::err(InvalidArg("bad_authority",
                    "http url: unexpected bytes after ipv6 literal").error());
            port_sv = authority.substr(close + 2);
        }
    } else {
        const auto colon = authority.rfind(':');
        if (colon != std::string_view::npos) {
            host = authority.substr(0, colon);
            port_sv = authority.substr(colon + 1);
        } else {
            host = authority;
        }
    }
    if (host.empty() || HasBadChar(host))
        return result<ParsedUrl>::err(InvalidArg("bad_host",
            "http url: invalid host").error());

    unsigned long port = 80;
    if (!port_sv.empty()) {
        port = 0;
        for (char ch : port_sv) {
            if (ch < '0' || ch > '9')
                return result<ParsedUrl>::err(InvalidArg("bad_port",
                    "http url: port must be decimal").error());
            port = port * 10 + static_cast<unsigned long>(ch - '0');
            if (port > 65535)
                return result<ParsedUrl>::err(InvalidArg("bad_port",
                    "http url: port out of range").error());
        }
        if (port == 0)
            return result<ParsedUrl>::err(InvalidArg("bad_port",
                "http url: port out of range").error());
    }

    out.host = std::string(host);
    out.port = static_cast<std::uint16_t>(port);
    return result<ParsedUrl>::ok(std::move(out));
}

} // namespace

result<ParsedUrl> ParseHttpUrl(std::string_view url) {
    constexpr std::string_view kHttp  = "http://";
    constexpr std::string_view kHttps = "https://";

    if (HasBadChar(url))
        return result<ParsedUrl>::err(InvalidArg("bad_url_char",
            "http url: control or blank character").error());
    if (EqualNoCase(url.substr(0, kHttps.size()), kHttps))
        return result<ParsedUrl>::err(InvalidArg("https_unsupported",
            "https not supported in this slice; refused, no plaintext fallback")
            .error());
    if (!EqualNoCase(url.substr(0, kHttp.size()), kHttp))
        return result<ParsedUrl>::err(InvalidArg("scheme_unsupported",
            "http url: only http:// scheme supported").error());

    const std::string_view rest = url.substr(kHttp.size());
    const auto          slash   = rest.find('/');
    const std::string_view authority = rest.substr(0, slash);
    if (authority.find('#') != std::string_view::npos)
        return result<ParsedUrl>::err(InvalidArg("bad_url_char",
            "http url: fragment not allowed").error());

    auto parsed = ParseAuthority(authority);
    if (!parsed)
        return parsed;

    std::string_view target =
        slash == std::string_view::npos ? std::string_view{} : rest.substr(slash);
    if (target.empty())
        target = "/";
    if (target.find('#') != std::string_view::npos ||
        !IsValidTarget(target))
        return result<ParsedUrl>::err(InvalidArg("bad_target",
            "http url: invalid request target").error());
    parsed.value().target = std::string(target);
    return parsed;
}

bool IsTokenChar(char c) noexcept {
    const unsigned char u = static_cast<unsigned char>(c);
    if (u >= '0' && u <= '9') return true;
    if (u >= 'A' && u <= 'Z') return true;
    if (u >= 'a' && u <= 'z') return true;
    switch (c) {
    case '!': case '#': case '$': case '%': case '&': case '\'':
    case '*': case '+': case '-': case '.': case '^': case '_':
    case '`': case '|': case '~':
        return true;
    default:
        return false;
    }
}

bool IsValidToken(std::string_view s) noexcept {
    if (s.empty())
        return false;
    for (char ch : s)
        if (!IsTokenChar(ch))
            return false;
    return true;
}

bool IsValidHeaderValue(std::string_view s) noexcept {
    for (char ch : s) {
        const unsigned char c = static_cast<unsigned char>(ch);
        if (c == 0x09 || (c >= 0x20 && c != 0x7F) || c >= 0x80)
            continue;
        return false;
    }
    return true;
}

bool IsValidTarget(std::string_view s) noexcept {
    if (s.empty())
        return false;
    for (char ch : s) {
        const unsigned char c = static_cast<unsigned char>(ch);
        if (c < 0x21 || c == 0x7F)
            return false;
    }
    return true;
}

bool IsReservedRequestHeader(std::string_view name) noexcept {
    static constexpr std::string_view kReserved[] = {
        "host", "content-length", "transfer-encoding",
        "connection", "expect", "upgrade",
    };
    for (auto r : kReserved)
        if (EqualNoCase(name, r))
            return true;
    return false;
}

result<ParsedUrl> ValidateRequest(const HttpRequest& req,
                                  const NetworkLimits& limits) {
    if (!IsValidToken(req.method))
        return result<ParsedUrl>::err(InvalidArg("bad_method",
            "http request: invalid method token").error());

    auto parsed = ParseHttpUrl(req.url);
    if (!parsed)
        return parsed;

    std::size_t header_bytes = 0;
    for (const auto& h : req.headers) {
        if (!IsValidToken(h.first))
            return result<ParsedUrl>::err(InvalidArg("bad_header_name",
                "http request: invalid header name").error());
        if (!IsValidHeaderValue(h.second))
            return result<ParsedUrl>::err(InvalidArg("bad_header_value",
                "http request: invalid header value (CRLF injection refused)")
                .error());
        if (IsReservedRequestHeader(h.first))
            return result<ParsedUrl>::err(InvalidArg("reserved_header",
                "http request: framing header managed by adapter").error());
        // 线上形态为 "name: value\r\n"，每头 +4 字节开销计入预算。
        header_bytes += h.first.size() + h.second.size() + 4;
        if (header_bytes > limits.max_header_bytes)
            return result<ParsedUrl>::err(InvalidArg("headers_too_large",
                "http request: headers exceed max_header_bytes").error());
    }
    if (req.body.size() > limits.max_body_bytes)
        return result<ParsedUrl>::err(InvalidArg("body_too_large",
            "http request: body exceeds max_body_bytes").error());
    return parsed;
}

result<void> ValidateResponse(const HttpResponse& res,
                              const NetworkLimits& limits) {
    std::size_t header_bytes = 0;
    for (const auto& h : res.headers) {
        if (!IsValidToken(h.first))
            return InvalidArg("bad_header_name",
                "http response: invalid header name");
        if (!IsValidHeaderValue(h.second))
            return InvalidArg("bad_header_value",
                "http response: invalid header value (CRLF injection refused)");
        if (IsReservedRequestHeader(h.first))
            return InvalidArg("reserved_header",
                "http response: framing header managed by adapter");
        header_bytes += h.first.size() + h.second.size() + 4;
        if (header_bytes > limits.max_header_bytes)
            return InvalidArg("headers_too_large",
                "http response: headers exceed max_header_bytes");
    }
    if (res.body.size() > limits.max_body_bytes)
        return InvalidArg("body_too_large",
            "http response: body exceeds max_body_bytes");
    return result<void>::ok();
}

bool IsHttpErrorCode(const boost::system::error_code& ec) noexcept {
    return ec.category() == HttpErrorCat();
}

Error ClassifyBackendError(const boost::system::error_code& ec,
                           std::string message) {
    namespace errc = boost::asio::error;
    Error e;
    e.message          = std::move(message);
    e.backend_category = ec.category().name();
    e.backend_code     = ec.value();

    if (ec == errc::operation_aborted) {
        e.code        = ErrorCode::Cancelled;
        e.domain_code = "operation_aborted";
        return e;
    }
    // beast http 解析域错误均为对端协议违规；但对端提前断流（不足一条
    // 完整消息）归 TransportError 而非 ProtocolError。
    if (IsHttpErrorCode(ec)) {
        if (ec == boost::beast::http::error::partial_message ||
            ec == boost::beast::http::error::end_of_stream) {
            e.code        = ErrorCode::TransportError;
            e.domain_code = "incomplete_message";
            return e;
        }
        e.code        = ErrorCode::ProtocolError;
        e.domain_code = "http_parse";
        if (ec == boost::beast::http::error::body_limit)
            e.domain_code = "body_limit";
        else if (ec == boost::beast::http::error::header_limit)
            e.domain_code = "header_limit";
        return e;
    }
    if (ec.category() == boost::asio::error::get_addrinfo_category() ||
        ec == errc::host_not_found ||
        ec == errc::host_not_found_try_again ||
        ec == errc::service_not_found ||
        ec == errc::socket_type_not_supported) {
        e.code        = ErrorCode::Unavailable;
        e.domain_code = "resolve_failed";
        return e;
    }
    // 其余 asio 基础错误按传输层失败归类（refused/reset/eof/timed_out...）
    e.code        = ErrorCode::TransportError;
    e.domain_code = "transport";
    return e;
}

unsigned ErrorCodeToHttpStatus(ErrorCode code) noexcept {
    switch (code) {
    case ErrorCode::InvalidArgument:   return 400;
    case ErrorCode::NotFound:
    case ErrorCode::UnsupportedRoute:  return 404;
    case ErrorCode::Overloaded:
    case ErrorCode::Unavailable:       return 503;
    case ErrorCode::TimedOut:          return 504;
    case ErrorCode::Closed:            return 503;
    default:                           return 500;
    }
}

} // namespace bbt::infra::http_detail
