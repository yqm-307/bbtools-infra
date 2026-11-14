#include <bbt/infra/http/Dependency.hpp>

#include <utility>

namespace bbt::infra::http {
namespace detail {

DependencyScope::~DependencyScope()
{

    /**
     * 逆序释放：后入表的节点先销毁，排在前面的前置节点最后销毁。Scope 是请求级
     * 唯一 owner，这里不等 vector 析构，好在请求收尾时就把顺序固定下来。
     */
    while (!m_values.empty()) {
        m_values.pop_back();
    }
}

std::shared_ptr<const void> DependencyScope::Find(const void* identity) const
{
    for (const auto& value : m_values) {
        if (value.identity == identity) {
            return value.object;
        }
    }
    return nullptr;
}

void DependencyScope::Store(const void* identity, std::shared_ptr<const void> object)
{
    m_values.push_back({identity, std::move(object)});
}

// clang-format off
}
// clang-format on

// clang-format off
}
// clang-format on
