#pragma once
/**
 * @brief Consul 内部交付阶段屏障，不属于公共 API；只暂停，不执行用户回调。
 */
#include <atomic>
#include <memory>
#include <bbt/coroutine/sync/CoWaiter.hpp>

namespace bbt::infra::consul {
class Client;

struct HealthDeliveryPause {
    std::atomic<bool> reached{false};
    bbt::coroutine::sync::CoWaiter::SPtr resume = bbt::coroutine::sync::CoWaiter::Create();
};

struct ClientTestAccess {
    // 串行测试为指定 Client 的下一次健康查询交付安装一次性屏障。
    static std::shared_ptr<HealthDeliveryPause> PauseHealthDelivery(const Client& client);
};

// clang-format off
}
// clang-format on
