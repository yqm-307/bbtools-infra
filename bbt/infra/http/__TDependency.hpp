/**
 * @file __TDependency.hpp
 * @brief Dependency/Dependencies 与 detail 解析器的模板定义，由 Dependency.hpp 末尾包含。
 */
#pragma once

namespace bbt::infra::http {
namespace detail {

/**
 * @brief 逐层展开声明 tuple：解析第 I 个节点后前移索引，任一节点失败即停。
 *
 * 声明顺序就是解析顺序，因此前置节点一定先于依赖它的节点构造。
 */
template <std::size_t I, class Tuple, class Resolved>
result<void> ResolveOne(const Tuple& declarations, DependencyScope& scope, Request& request, Resolved& values)
{
    if constexpr (I == std::tuple_size_v<Tuple>) {
        return result<void>::ok();
    } else {
        auto value = std::get<I>(declarations).Resolve(scope, request);
        if (!value) {
            return result<void>::err(value.error());
        }
        std::get<I>(values) = std::move(value).value();
        return ResolveOne<I + 1>(declarations, scope, request, values);
    }
}

template <class... Dependencies>
result<ResolvedValues<Dependencies...>>
ResolveAll(const std::tuple<Dependencies...>& declarations, DependencyScope& scope, Request& request)
{
    ResolvedValues<Dependencies...> values;
    auto resolved = ResolveOne<0>(declarations, scope, request, values.values);
    if (!resolved) {
        return result<ResolvedValues<Dependencies...>>::err(resolved.error());
    }
    return result<ResolvedValues<Dependencies...>>::ok(std::move(values));
}

// clang-format off
}
// clang-format on

template <class T> template <class Provider> Dependency<T> Dependency<T>::Create(Provider provider)
{
    static_assert(
        std::is_same_v<std::invoke_result_t<Provider&, Request&>, Result>, "dependency provider must return result<T>");
    return Dependency([provider = std::move(provider)](detail::DependencyScope&, Request& request) {
        auto outcome = std::invoke(provider, request);
        if (!outcome) {
            return result<Stored>::err(outcome.error());
        }
        return result<Stored>::ok(std::make_shared<const T>(std::move(outcome).value()));
    });
}

template <class T>
result<std::shared_ptr<const T>> Dependency<T>::Resolve(detail::DependencyScope& scope, Request& request) const
{

    /**
     * 先查请求级缓存，命中即复用；未命中才构造并登记，因此同一请求内的重复依赖
     * 只有一次构造代价。
     */
    if (auto object = scope.Find(m_identity.get())) {
        return result<Stored>::ok(std::static_pointer_cast<const T>(std::move(object)));
    }
    auto outcome = m_factory(scope, request);
    if (!outcome) {
        return result<Stored>::err(outcome.error());
    }
    scope.Store(m_identity.get(), outcome.value());
    return result<Stored>::ok(std::static_pointer_cast<const T>(std::move(outcome).value()));
}

template <class T>
Dependency<T>::Dependency(Factory factory)
    : m_identity(std::make_shared<int>(0))
    , m_factory(std::move(factory))
{
}

template <class... Nodes>
Dependencies<Nodes...>::Dependencies(std::tuple<Nodes...> declarations)
    : m_declarations(std::move(declarations))
{
}

template <class... Nodes> template <class T> auto Dependencies<Nodes...>::Depend(Dependency<T> dependency) const
{
    return Dependencies<Nodes..., Dependency<T>>(
        std::tuple_cat(m_declarations, std::make_tuple(std::move(dependency))));
}

template <class... Nodes>
template <class T, class Provider>
Dependency<T> Dependencies<Nodes...>::Provide(Provider provider) const
{
    using Result = result<T>;
    static_assert(
        std::is_same_v<std::invoke_result_t<Provider&, Request&, const typename Nodes::Value&...>, Result>,
        "dependency provider must return result<T>");
    return Dependency<T>([declarations = m_declarations,
                          provider = std::move(provider)](detail::DependencyScope& scope, Request& request) {
        // clang-format off

        /**
         * 前置节点先按声明顺序解析完，再以它们的值调用用户 provider：前置失败时
         * 当前节点不会被构造，调用者拿到的是第一个失败节点给出的错误。
         */
        // clang-format on
        auto resolved = detail::ResolveAll(declarations, scope, request);
        if (!resolved) {
            return result<std::shared_ptr<const T>>::err(resolved.error());
        }
        return std::apply(
            [&](const auto&... values) {
                auto outcome = std::invoke(provider, request, *values...);
                if (!outcome) {
                    return result<std::shared_ptr<const T>>::err(outcome.error());
                }
                return result<std::shared_ptr<const T>>::ok(std::make_shared<const T>(std::move(outcome).value()));
            },
            resolved.value().values);
    });
}

// clang-format off
}
// clang-format on
