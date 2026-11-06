/** @file ICoNetwork.hpp
 * @brief 网络相关受管对象的公共身份边界。
 *
 * ICoNetwork 只表达「这是一个协程网络受管对象」，不承诺派生类可以监听或按字节读写；
 * 关闭能力由 ICoCloseable 独立提供。
 */
#pragma once

#include <bbt/infra/ICoObject.hpp>

namespace bbt::infra {

/**
 * @brief 协程网络受管对象的身份标记，继承 bbt::coroutine::ICoObject。
 */
class ICoNetwork : public bbt::coroutine::ICoObject {
public:
    /// @brief 虚析构；身份标记不持有资源。
    ~ICoNetwork() override = default;
};

} // namespace bbt::infra
