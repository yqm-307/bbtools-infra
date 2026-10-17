#pragma once

#include <cstdint>
#include <system_error>

#include <bbt/coroutine/object/interface/ICoObject.hpp>
#include <bbt/infra/net/Annotations.hpp>

namespace bbt::infra::net {

struct CoEnvironment {
    std::uint64_t coroutine_id{0};

    BBT_INFRA_NET_NONSUSPENDING bool CanSuspend() const noexcept { return coroutine_id != 0; }
};

// 只扩展 infra 契约，不向上游命名空间复制定义。环境每次现查，与创建身份无关。
class ICoObject : public bbt::coroutine::ICoObject {
public:
    // 固定上游查询不创建 Processer；每次按当前环境判定，见决策 0010。
    // TLS helper 仍可能首次初始化；不把无创建测试等同于完整路径的非阻塞证明。
    CoEnvironment   GetEnvironment() const;
    std::error_code RequireCoroutine() const;
};

} // namespace bbt::infra::net
