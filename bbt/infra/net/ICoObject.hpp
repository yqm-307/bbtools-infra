#pragma once

/**
 * @file ICoObject.hpp
 * @brief net 对上游协程对象接口的扩展：当前执行环境查询。
 *
 * 只扩展 infra 的契约，不向上游命名空间复制定义。执行环境每次现查，与对象创建时的身份无关。
 */

#include <cstdint>
#include <system_error>

#include <bbt/coroutine/object/interface/ICoObject.hpp>
#include <bbt/infra/net/Annotations.hpp>

namespace bbt::infra::net {

/**
 * @brief 当前执行环境的快照。
 *
 * coroutine_id 为 0 表示当前不在受管协程中，此时不能挂起。
 */
struct CoEnvironment {
    std::uint64_t coroutine_id{0}; ///< 当前受管协程的 id；0 表示不在受管协程中

    /** @brief 当前环境是否允许挂起：只有受管协程（id 非 0）可以挂起。 */
    BBT_INFRA_NET_NONSUSPENDING bool CanSuspend() const noexcept
    {
        return coroutine_id != 0;
    }
};

/**
 * @brief 在上游 ICoObject 之上增加 infra 的执行环境查询。
 */
class ICoObject : public bbt::coroutine::ICoObject {
public:
    /**
     * @brief 读取当前执行环境。
     *
     * 通过上游的无创建查询读取当前协程 id，不会创建 Processer。TLS 辅助对象仍可能在首次访问时
     * 初始化，因此「不创建」不等于整条路径非阻塞。
     */
    CoEnvironment GetEnvironment() const;

    /**
     * @brief 确认当前处于受管协程域。
     *
     * 判定与 detail 中的 RequireCoroutineContext 同源。普通线程、runtime idle、raw 回调等非受管协程域
     * 返回 operation_not_permitted。
     */
    std::error_code RequireCoroutine() const;
};

} // namespace bbt::infra::net
