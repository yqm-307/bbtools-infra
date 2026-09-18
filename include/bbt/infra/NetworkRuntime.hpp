#pragma once
// co-network/v1 N1b-1：网络 Runtime 装配面。
// Runtime 是资源拥有者；工厂返回的 shared_ptr 是受托管对象引用。
// RPC 工厂在 N2 就绪前不导出（不导出能假成功的工厂）。

#include <memory>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/HttpServer.hpp>
#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra {

// 正常生命周期：Scheduler::Start → NetworkRuntime::Create/Start → 使用
// → server.StopAccepting → 等待 handler 结束 → Runtime.RequestClose/WaitClosed
// → 释放已关闭网络对象 → Scheduler::Stop。
class NetworkRuntime : public ICoNetwork, public ICoCloseable {
public:
    // Create/Start/工厂在启动控制线程使用，不挂起协程。
    // Create 要求 Scheduler 已启动（对象身份与完成信号需要运行时代际），
    // 否则返回 Error(RuntimeUnavailable)。
    static result<std::shared_ptr<NetworkRuntime>> Create(NetworkLimits limits);

    // 同一 runtime 只成功启动一次；关闭后不重开。
    // 要求 Scheduler 处于与 Create 相同的运行时代际。
    virtual result<void> Start() = 0;

    virtual result<std::shared_ptr<HttpClient>> CreateHttpClient() = 0;
    virtual result<std::shared_ptr<HttpServer>> ListenHttp(
        ListenAddress address, HttpHandler handler) = 0;
};

} // namespace bbt::infra
