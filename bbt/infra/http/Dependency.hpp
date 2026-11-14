/**
 * @file Dependency.hpp
 * @brief HTTP 请求依赖声明、按请求解析与子依赖组合。
 *
 * 依赖声明在装配期构造；每个请求拥有独立 Scope，按声明节点身份缓存结果，并在请求
 * handler 返回或异常离开时逆序释放。依赖只在路由已匹配后解析，不能把引用保存到请求之外。
 */
#pragma once

#include <cstddef>
#include <functional>
#include <memory>

#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include <bbt/infra/Result.hpp>

namespace bbt::infra::http {

struct Request;

namespace detail {

/**
 * @brief 单次请求内的依赖结果表：按声明节点身份查找，持有已构造对象。
 *
 * identity 取自 Dependency 构造时分配的稳定节点身份；同一声明在同一 Scope 内只
 * 构造一次，第二次 Find 命中缓存。Scope 是请求级 owner，请求结束即整体释放。
 */
class DependencyScope {
public:
    DependencyScope() = default;
    DependencyScope(const DependencyScope&) = delete;
    DependencyScope& operator=(const DependencyScope&) = delete;

    ~DependencyScope();

    /**
     * @brief 按节点身份查已构造对象；本请求尚未构造过时返回空。
     */
    std::shared_ptr<const void> Find(const void* identity) const;

    /**
     * @brief 记录节点身份对应的已构造对象，供同一请求的后续解析复用。
     */
    void Store(const void* identity, std::shared_ptr<const void> object);

private:
    struct Value {
        const void* identity;
        std::shared_ptr<const void> object;
    };

    std::vector<Value> m_values;
};

template <class T> class DependencyAccess;

/**
 * @brief 一条依赖链的解析结果：按声明顺序保存每个节点的对象。
 */
template <class... Dependencies> class ResolvedValues {
public:
    using Tuple = std::tuple<std::shared_ptr<const typename Dependencies::Value>...>;

    Tuple values;
};

/**
 * @brief 按声明顺序解析第 I 个及之后的节点；任一节点失败即带该错误返回。
 */
template <std::size_t I, class Tuple, class Resolved>
result<void> ResolveOne(const Tuple& declarations, DependencyScope& scope, Request& request, Resolved& values);

/**
 * @brief 解析整条依赖链；成功时把各节点对象按声明顺序交给 ResolvedValues。
 */
template <class... Dependencies>
result<ResolvedValues<Dependencies...>>
ResolveAll(const std::tuple<Dependencies...>& declarations, DependencyScope& scope, Request& request);

// clang-format off
}
// clang-format on

template <class... Nodes> class Dependencies;

/**
 * @brief 单个依赖声明：provider 在请求期、需要该依赖时构造一次。
 *
 * 声明本身与请求无关，装配期可复制、可跨请求复用；真正构造发生在 Resolve，
 * 结果记入该请求的 DependencyScope，同一请求内的重复声明只构造一次。
 */
template <class T> class Dependency {
public:
    using Value = T;
    using Result = result<T>;

    /**
     * @brief 用一个只依赖 Request 的 provider 创建依赖声明。
     *
     * @param provider 返回 T，或返回携带 Error 的 result<T>
     */
    template <class Provider> static Dependency Create(Provider provider);

    /**
     * @brief 在给定 Scope 内解析本节点；已构造过则直接复用缓存对象。
     *
     * @param scope 当前请求的结果表，首次构造的对象记入其中
     * @param request 当前请求，provider 可读的消息与路径参数
     */
    result<std::shared_ptr<const T>> Resolve(detail::DependencyScope& scope, Request& request) const;

private:
    using Stored = std::shared_ptr<const T>;
    using Factory = std::function<result<Stored>(detail::DependencyScope&, Request&)>;

    explicit Dependency(Factory factory);

    template <class... Nodes> friend class Dependencies;

    /**
     * 节点身份：同一份声明复制出的副本共享它，因此同一请求内命中同一缓存槽；
     * 不同声明之间身份互不相同。
     */
    std::shared_ptr<const int> m_identity;
    Factory m_factory;
};

/**
 * @brief 依赖图的装配器；Depend 声明前置节点，Provide 声明当前节点。
 *
 * 装配期只累积声明；Provide 生成的 provider 在请求期先按声明顺序解析前置节点，
 * 再以这些值调用用户 provider，前置失败直接短路、不构造后续节点。
 */
template <class... Nodes> class Dependencies {
public:
    Dependencies(std::tuple<Nodes...> declarations = {});

    /**
     * @brief 追加一个前置节点声明，返回节点列表扩展后的新装配器。
     */
    template <class T> auto Depend(Dependency<T> dependency) const;

    /**
     * @brief 以已声明的前置节点为输入声明当前节点。
     *
     * provider 必须返回 result<T>，可接收 (Request&, 前置值...)。
     */
    template <class T, class Provider> Dependency<T> Provide(Provider provider) const;

private:
    template <class...> friend class RouteBuilder;
    std::tuple<Nodes...> m_declarations;
};

// clang-format off
}
// clang-format on

#include <bbt/infra/http/__TDependency.hpp>
