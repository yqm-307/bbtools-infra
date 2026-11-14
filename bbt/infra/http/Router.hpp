/**
 * @file Router.hpp
 * @brief HTTP 路由与中间件装配（bbt::infra::http）：方法/路径匹配、路径参数、
 *        前缀分组与 Build 时自拥有 ServerHandler 快照。
 *
 * Router 只装配，不联网、不建线程：单线程装配期注册路由与全局中间件，
 * 配置错误集中由 Build 返回；Build 产出的 ServerHandler 自拥有路由表快照，
 * 可直接交给 http::Server::Create。请求处理期间不修改已发布的路由表。
 * 首版只支持 ASCII 未编码路径与显式方法名；依赖注入仅在匹配后的 handler 链内生效，
 * 不做热更新或完整 URI 解析。
 */
#pragma once

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/infra/http/Server.hpp>
#include <bbt/infra/http/Dependency.hpp>

namespace bbt::infra::http {

/**
 * @brief 单次请求内有效的拥有型请求：调用上下文、HTTP 消息与已匹配的路径参数。
 *
 * params 由最终选中路由的模板解析而来，只在本次请求的 handler 链内有效。
 */
struct Request {
    IncomingCallContext context;
    IncomingRequest http;
    std::map<std::string, std::string> params;
};

/**
 * @brief 路由处理结果：Response 或错误。
 */
using Reply = result<Response>;

/**
 * @brief 单路由处理函数，可读本次请求并可读写路径参数。
 */
using Handler = std::function<Reply(Request&)>;

/**
 * @brief 中间件链的下一环；同一链内只可调用一次，重复调用返回错误。
 */
using Next = Handler;

/**
 * @brief 全局或路由组中间件：包住下游 Next，先注册先进入、逆序退出。
 */
using Middleware = std::function<Reply(Request&, const Next&)>;

template <class... DependencyNodes> class RouteBuilder;

namespace detail {

/**
 * @brief 包装下游 handler 的中间件链；先注册先进入，逆序退出，Next 只可调用一次。
 */
Handler ComposeMiddleware(Handler downstream, const std::vector<Middleware>& middlewares);

// clang-format off
}
// clang-format on

/**
 * @brief 构造 text/plain 缓冲响应。
 */
Reply Text(int status, std::string body);

/**
 * @brief 装配期注册的轻量路由表。
 *
 * AddRoute/Use/Include 直接注册并返回 void，Get/Post 返回链式 builder；配置错误由 Router
 * 保存并在 Build 统一返回，调用者必须检查 Build 的 result。Build 校验后复制路由表生成自拥有快照，原 Router
 * 销毁或继续装配均不改变已发出的 handler。Include 复制组的当前路由与
 * 中间件，不保持对原组的动态引用。
 */
class Router {
public:
    /**
     * @brief 返回单路由链式装配器：Get(path).Use(...).Depend(...).Handle(handler)。
     *
     * builder 只借用 Router，必须在装配期立即完成；也支持 builder(handler) 的直接注册写法。
     */
    RouteBuilder<> Get(std::string path);

    /**
     * @brief 与 Get 同一形态的 POST 链式装配器。
     */
    RouteBuilder<> Post(std::string path);

    /**
     * @brief 注册任意 HTTP token 方法的路由；方法大小写敏感，不自动改写。
     */
    void AddRoute(std::string method, std::string path, Handler handler);

    /**
     * @brief 注册全局中间件：先注册先进入、逆序退出，覆盖 400/404/405。
     */
    void Use(Middleware middleware);

    /**
     * @brief 以前缀挂入路由组：复制组当前路由并保留其组内中间件链。
     *
     * 自包含（prefix, *this）在副作用前被拒绝；prefix 必须为空或以 '/'
     * 开头且不以 '/' 结尾。组错误状态随 Include 传播。
     */
    void Include(std::string prefix, const Router& group);

    /**
     * @brief 校验配置并生成自拥有的 ServerHandler 快照。
     *
     * @return 配置错误（非法方法/路径、空 handler、同方法同形状重复等）时返回错误
     */
    result<ServerHandler> Build() const;

private:
    struct Route {
        std::string method;
        std::string path;
        Handler handler;
    };

private:
    std::vector<Route> m_routes;
    std::vector<Middleware> m_middlewares;
    std::optional<Error> m_error;
};

// clang-format off
}
// clang-format on

namespace bbt::infra::http {

/**
 * @brief 单条路由的链式装配器：Use → Depend → Handle，注册期只借用 Router。
 *
 * Depend/Use 都返回新的 builder（Depend 的依赖列表类型随之扩展），不修改已有
 * 实例；Handle/operator() 才真正向 Router 注册路由。builder 不持有 Router 寿命，
 * 只用于装配期表达式。
 *
 * 依赖解析发生在请求期：Handle 把下游 handler 包成「建 DependencyScope →
 * ResolveAll → 用已解析值调用 handler」，所以依赖只在路由匹配后的 handler 链内
 * 生效，失败时不调用 handler。
 */
template <class... DependencyNodes> class RouteBuilder {
public:
    /**
     * @brief 由 Router::Get/Post 构造；调用者一般不需要直接构造。
     *
     * @param router 装配期借用的 Router
     * @param method 已规范化的方法名
     * @param path 路由模板，可含 {name} 参数段
     * @param dependencies 已累积的依赖节点
     * @param middlewares 已挂的中间件链
     * @param invalid_middleware 是否挂过空中间件，注册时据此报错
     */
    RouteBuilder(
        Router& router,
        std::string method,
        std::string path,
        Dependencies<DependencyNodes...> dependencies = {},
        std::vector<Middleware> middlewares = {},
        bool invalid_middleware = false);

    /**
     * @brief 追加一个依赖节点，返回依赖列表扩展后的新 builder。
     *
     * @param dependency 依赖声明，provider 只依赖 Request
     */
    template <class T> auto Depend(Dependency<T> dependency) const;

    /**
     * @brief 追加一个中间件，返回中间件链扩展后的新 builder。
     */
    RouteBuilder Use(Middleware middleware) const;

    /**
     * @brief 以 handler 注册路由并结束链式装配。
     *
     * handler 可接收 (Request&, 依赖值...) 或 (依赖值...)，必须返回 Reply；依赖
     * 按 Depend 顺序解析，解析失败直接返回该错误、不调用 handler。
     */
    template <class Function> void Handle(Function function) const;

    /**
     * @brief 等价于 Handle(handler)；handler 为空时注册失败而不是静默忽略。
     */
    void operator()(Handler handler) const;

private:
    Router& m_router;
    std::string m_method;
    std::string m_path;
    Dependencies<DependencyNodes...> m_dependencies;
    std::vector<Middleware> m_middlewares;
    bool m_invalid_middleware = false;
};

// clang-format off
}
// clang-format on

#include <bbt/infra/http/__TRouter.hpp>
