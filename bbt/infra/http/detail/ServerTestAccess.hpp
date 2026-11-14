/**
 * @file ServerTestAccess.hpp
 * @brief HTTP 测试专用注入点，不是公共契约。
 */
#pragma once

#include <functional>

namespace bbt::infra::http {
class Server;

/**
 * @brief 测试注入入口：只服务于本模块测试，不进入公共边界。
 */
class ServerTestAccess {
public:
    /**
     * @brief 让下一次任务登记失败一次，用于验证登记失败时不留残余计数。
     */
    static void FailNextTask(Server& server);

    /**
     * @brief 在下一次任务登记前（锁外、计数之前）执行一次 hook，用于确定性
     *        制造 Stop/WaitStopped 与登记窗口的竞争；不是公共契约。
     */
    static void SetRegistrationBarrierOnce(Server& server, std::function<void()> barrier);
};

// clang-format off
}
// clang-format on
