#include <bbt/infra/http/Router.hpp>

#include <algorithm>
#include <memory>
#include <set>
#include <utility>

namespace bbt::infra::http {
namespace {

std::vector<std::string> Segments(const std::string& path)
{

    /**
     * 保留空段：重复斜杠按字面匹配，不折叠；「/」本身切出空串。
     */
    std::vector<std::string> parts;
    std::size_t begin = 1;
    while (begin <= path.size()) {
        const auto end = path.find('/', begin);
        parts.push_back(path.substr(begin, end == std::string::npos ? end : end - begin));
        if (end == std::string::npos) {
            break;
        }
        begin = end + 1;
    }
    return parts;
}

bool IsParameter(const std::string& part)
{
    return part.size() > 2 && part.front() == '{' && part.back() == '}';
}

/**
 * HTTP token（RFC 9110 tchar）：方法名按 token 校验而非仅标识符。
 */
bool IsTchar(char c)
{
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')) {
        return true;
    }
    switch (c) {
    case '!':
    case '#':
    case '$':
    case '%':
    case '&':
    case '\'':
    case '*':
    case '+':
    case '-':
    case '.':
    case '^':
    case '_':
    case '`':
    case '|':
    case '~':
        return true;
    default:
        return false;
    }
}

bool IsMethodToken(const std::string& method)
{
    return !method.empty() && std::all_of(method.begin(), method.end(), IsTchar);
}

bool IsParamName(const std::string& name)
{
    return !name.empty() && std::all_of(name.begin(), name.end(), [](unsigned char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
    });
}

bool ValidPath(const std::string& path, bool pattern)
{
    if (path.empty() || path.front() != '/' || path.size() > 8192) {
        return false;
    }
    std::set<std::string> names;
    for (const auto& part : Segments(path)) {
        if (pattern && IsParameter(part)) {
            const auto name = part.substr(1, part.size() - 2);
            if (!IsParamName(name) || !names.insert(name).second) {
                return false;
            }
        } else if (part == "." || part == ".." || !std::all_of(part.begin(), part.end(), [](unsigned char c) {
                       return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-'
                              || c == '_' || c == '.' || c == '~';
                   })) {
            return false;
        }
    }
    return true;
}

/**
 * 形状 = 参数名归一后的路径：同形状不同参数名视为同一注册位（重复声明）。
 */
std::string Shape(const std::string& pattern)
{
    std::string shape;
    for (const auto& part : Segments(pattern)) {
        shape += '/' + (IsParameter(part) ? std::string("{}") : part);
    }
    return shape;
}

/**
 * 按 pattern 重新从实际请求段取参数名：参数归属最终选中的路由，不能沿用
 * 其它方法同形状候选的名字（POST /u/{name} 选中后不得交付 GET /u/{id} 的名字）。
 */
std::map<std::string, std::string>
ExtractParams(const std::vector<std::string>& pattern, const std::vector<std::string>& parts)
{
    std::map<std::string, std::string> params;
    for (std::size_t i = 0; i < pattern.size(); ++i) {
        if (IsParameter(pattern[i])) {
            params.emplace(pattern[i].substr(1, pattern[i].size() - 2), parts[i]);
        }
    }
    return params;
}

// clang-format off
}
// clang-format on

Handler detail::ComposeMiddleware(Handler downstream, const std::vector<Middleware>& middlewares)
{
    for (auto it = middlewares.rbegin(); it != middlewares.rend(); ++it) {
        downstream = [middleware = *it, inner = std::move(downstream)](Request& request) -> Reply {
            bool called = false;
            Next next = [&called, &inner](Request& current) -> Reply {
                if (called) {
                    return Reply::err(MakeError(ErrorCode::InvalidContext, "middleware next called twice"));
                }
                called = true;
                return inner(current);
            };
            return middleware(request, next);
        };
    }
    return downstream;
}

Reply Text(int status, std::string body)
{
    Response response;
    response.status_code = status;
    response.headers.emplace_back("Content-Type", "text/plain; charset=utf-8");
    response.body = std::move(body);
    return Reply::ok(std::move(response));
}

RouteBuilder<> Router::Get(std::string path)
{
    return RouteBuilder<>(*this, "GET", std::move(path));
}

RouteBuilder<> Router::Post(std::string path)
{
    return RouteBuilder<>(*this, "POST", std::move(path));
}

void Router::AddRoute(std::string method, std::string path, Handler handler)
{
    if (!IsMethodToken(method)) {
        if (!m_error) {
            m_error = MakeError(ErrorCode::InvalidArgument, "invalid route method");
        }
        return;
    }
    if (!ValidPath(path, true)) {
        if (!m_error) {
            m_error = MakeError(ErrorCode::InvalidArgument, "invalid route path");
        }
        return;
    }
    if (!handler) {
        if (!m_error) {
            m_error = MakeError(ErrorCode::InvalidArgument, "empty route handler");
        }
        return;
    }
    m_routes.push_back({std::move(method), std::move(path), std::move(handler)});
}

void Router::Use(Middleware middleware)
{
    if (!middleware) {
        if (!m_error) {
            m_error = MakeError(ErrorCode::InvalidArgument, "empty middleware");
        }
        return;
    }
    m_middlewares.push_back(std::move(middleware));
}

void Router::Include(std::string prefix, const Router& group)
{

    /**
     * 自包含必须先于任何副作用拒绝：否则追加自身路由的迭代永不终止。
     */
    if (&group == this) {
        if (!m_error) {
            m_error = MakeError(ErrorCode::InvalidArgument, "router cannot include itself");
        }
        return;
    }
    if (group.m_error && !m_error) {
        m_error = group.m_error;
    }

    /**
     * 空前缀允许；非空前缀须为合法路径模板且不以 '/' 结尾（否则必然产生重复斜杠字面）。
     */
    if (!prefix.empty() && (prefix.back() == '/' || !ValidPath(prefix, false))) {
        if (!m_error) {
            m_error = MakeError(ErrorCode::InvalidArgument, "invalid router prefix");
        }
        return;
    }

    /**
     * Include 拷贝组当前快照并保留组内中间件链；之后修改原组不影响已挂入的路由。
     */
    for (const auto& route : group.m_routes) {
        AddRoute(route.method, prefix + route.path, detail::ComposeMiddleware(route.handler, group.m_middlewares));
    }
}

result<ServerHandler> Router::Build() const
{
    using Built = result<ServerHandler>;
    if (m_error) {
        return Built::err(*m_error);
    }
    std::set<std::pair<std::string, std::string>> seen;
    for (const auto& route : m_routes) {
        if (!seen.emplace(route.method, Shape(route.path)).second) {
            return Built::err(MakeError(ErrorCode::InvalidArgument, "duplicate or equivalent route"));
        }
    }
    auto routes = std::make_shared<const std::vector<Route>>(m_routes);
    Handler dispatch = [routes](Request& request) -> Reply {
        const auto path = request.http.target.substr(0, request.http.target.find('?'));

        /**
         * ponytail: 首版仅支持 ASCII 未编码路径；完整 URI 解析（百分号解码、
         * 规范化与鉴权路径一致性）需接成熟解析器并补边界测试后再开放。
         */
        if (!ValidPath(path, false)) {
            return Text(400, "unsupported or invalid path");
        }
        const auto parts = Segments(path);
        const Route* selected = nullptr;
        std::vector<int> best_rank;
        std::set<std::string> allowed;
        for (const auto& route : *routes) {
            const auto pattern = Segments(route.path);
            if (pattern.size() != parts.size()) {
                continue;
            }
            bool matches = true;
            std::vector<int> rank;
            for (std::size_t i = 0; i < parts.size(); ++i) {
                const bool parameter = IsParameter(pattern[i]);
                if (parameter && parts[i].empty()) {

                    /**
                     * 参数必须匹配一个非空段；空段只允许静态字面匹配。
                     */
                    matches = false;
                    break;
                }
                if (!parameter && pattern[i] != parts[i]) {
                    matches = false;
                    break;
                }
                rank.push_back(parameter ? 0 : 1);
            }
            if (!matches) {
                continue;
            }

            /**
             * 先确定最具体路径（静态段优先），再在其上匹配方法；不回退到较
             * 宽泛的参数路径，避免把「静态 405」错误地落到参数 handler 上。
             */
            if (!selected || rank > best_rank) {
                selected = &route;
                best_rank = rank;
                allowed.clear();
            }
            if (rank == best_rank) {
                allowed.insert(route.method);
                if (route.method == request.http.method) {
                    selected = &route;
                }
            }
        }
        if (!selected) {
            return Text(404, "not found");
        }
        if (selected->method != request.http.method) {
            auto reply = Text(405, "method not allowed");
            std::string methods;
            for (const auto& method : allowed) {
                methods += (methods.empty() ? "" : ", ") + method;
            }
            reply.value().headers.emplace_back("Allow", std::move(methods));
            return reply;
        }
        request.params = ExtractParams(Segments(selected->path), parts);
        return selected->handler(request);
    };
    auto pipeline = detail::ComposeMiddleware(std::move(dispatch), m_middlewares);
    ServerHandler handler = [pipeline =
                                 std::move(pipeline)](IncomingCallContext context, IncomingRequest incoming) -> Reply {
        Request request{std::move(context), std::move(incoming), {}};
        return pipeline(request);
    };
    return Built::ok(std::move(handler));
}

// clang-format off
}
// clang-format on
