#pragma once
// co-network/v1 N0：网络相关受管对象的公共身份边界。
// ICoNetwork 只表达「这是一个协程网络受管对象」，不承诺任意派生类
// 都可监听或按字节读写；关闭能力由 ICoCloseable 独立提供。

#include <bbt/infra/ICoObject.hpp>

namespace bbt::infra {

class ICoNetwork : public bbt::coroutine::ICoObject {
public:
    ~ICoNetwork() override = default;
};

} // namespace bbt::infra
