/**
 * @file __TRouter.hpp
 * @brief RouteBuilder 的模板定义，由 Router.hpp 末尾包含，不单独使用。
 */
#pragma once

namespace bbt::infra::http {

template <class... DependencyNodes>
RouteBuilder<DependencyNodes...>::RouteBuilder(
    Router& router,
    std::string method,
    std::string path,
    Dependencies<DependencyNodes...> dependencies,
    std::vector<Middleware> middlewares,
    bool invalid_middleware)
    : m_router(router)
    , m_method(std::move(method))
    , m_path(std::move(path))
    , m_dependencies(std::move(dependencies))
    , m_middlewares(std::move(middlewares))
    , m_invalid_middleware(invalid_middleware)
{
}

template <class... DependencyNodes>
template <class T>
auto RouteBuilder<DependencyNodes...>::Depend(Dependency<T> dependency) const
{
    return RouteBuilder<DependencyNodes..., Dependency<T>>(
        m_router, m_method, m_path, m_dependencies.Depend(std::move(dependency)), m_middlewares, m_invalid_middleware);
}

template <class... DependencyNodes>
RouteBuilder<DependencyNodes...> RouteBuilder<DependencyNodes...>::Use(Middleware middleware) const
{
    auto copy = *this;
    if (!middleware) {
        copy.m_invalid_middleware = true;
    } else {
        copy.m_middlewares.push_back(std::move(middleware));
    }
    return copy;
}

template <class... DependencyNodes>
template <class Function>
void RouteBuilder<DependencyNodes...>::Handle(Function function) const
{
    if (m_invalid_middleware) {
        m_router.AddRoute(m_method, m_path, {});
        return;
    }
    Handler downstream = [dependencies = m_dependencies, function = std::move(function)](Request& request) -> Reply {
        // clang-format off

        /**
         * 依赖按请求构造：scope 只活到本次 handler 返回，其中的依赖对象随它逆序
         * 释放，因此 handler 不能把依赖引用带出调用。
         */
        // clang-format on
        detail::DependencyScope scope;
        auto resolved = detail::ResolveAll(dependencies.m_declarations, scope, request);
        if (!resolved) {
            return Reply::err(resolved.error());
        }
        return std::apply(
            [&](const auto&... values) -> Reply {
                // clang-format off

                /**
                 * 两种 handler 形态共用同一份解析结果：带 Request& 的写法能读路径
                 * 参数，不带 Request& 的写法只消费依赖值。
                 */
                // clang-format on
                if constexpr (
                    std::is_invocable_r_v<Reply, Function&, Request&, const typename DependencyNodes::Value&...>) {
                    return std::invoke(function, request, *values...);
                } else {
                    static_assert(
                        std::is_invocable_r_v<Reply, Function&, const typename DependencyNodes::Value&...>,
                        "route handler must return Reply and accept its declared dependencies");
                    return std::invoke(function, *values...);
                }
            },
            resolved.value().values);
    };
    m_router.AddRoute(m_method, m_path, detail::ComposeMiddleware(std::move(downstream), m_middlewares));
}

template <class... DependencyNodes> void RouteBuilder<DependencyNodes...>::operator()(Handler handler) const
{
    if (!handler) {
        m_router.AddRoute(m_method, m_path, {});
        return;
    }
    Handle(std::move(handler));
}

// clang-format off
}
// clang-format on
