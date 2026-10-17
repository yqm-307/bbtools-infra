#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <system_error>

#include <bbt/infra/net/Action.hpp>
#include <bbt/infra/net/CoConnectionHandler.hpp>
#include <bbt/infra/net/EventHandler.hpp>
#include <bbt/infra/net/ICoConnection.hpp>

namespace bbt::infra::net {

// 私有适配（header-only）：把公共 EventHandler 适配成既有 ICoConnectionHandler，
// 并把 Action 落到真实语义：Action::Close 在事件协程内 Close(Unblock)（封口 +
// 可靠投递物理关闭），Action::Shutdown 只在该事件的同一受管协程任务内转发一次
// handler_->OnShutdownRequest()（向上请求上报：不封口、不关闭、不改引擎状态，见
// EventHandler::OnShutdownRequest 注释），不伪造、不新增 detail 状态。
// 只转 OnOpen/OnTraffic/OnClose 三个已交付回调与 OnShutdownRequest 上报口；
// 不转 OnTick，也不交付「已关闭生命周期通知」（不造 stub）。
//
// 活跃连接计数（F1 修复）：计数拥有者是与 ServerEngine 共享的
// std::shared_ptr<std::atomic<std::size_t>>（每 Run 一代，Engine 不重置陈旧计数）。
// 适配器只持该共享计数，不持 ServerEngine/Engine/handler，不建环、无 reaper；
// OnClose 内用 CountGuard 在作用域退出完成恰一次减计数（正常/抛出/跨挂起都不漏减），
// guard 持共享计数 ⇒ OnClose 挂起跨 ServerEngine 销毁时仍保活计数对象、不写已释放内存。
class EventHandlerAdapter final : public ICoConnectionHandler {
public:
    // live_connections 为该 Engine 当次 Run 的活跃连接计数（接纳成功 +1 / OnClose -1）；
    // 共享所有权；传空则不计数（本 Engine 始终传入）。
    // handler 与 Engine/ServerEngine 共享所有权（不借用裸引用）：本适配器（及其所在
    // 会话 registry / 挂起中的事件回调）存活期间 handler 不析构，替代原借用形态后
    // 挂起 OnClose 跨 ServerEngine 销毁仍安全。
    // 析构线程：最后一个拥有者所在线程，可能是受管协程；本类不保证在控制线程析构，
    // 析构（含是否释放 handler 最后引用）不得阻塞 worker/owner 线程，也不回调
    // Engine::Stop、不引入 reaper。
    EventHandlerAdapter(std::shared_ptr<EventHandler> handler,
        std::shared_ptr<std::atomic<std::size_t>> live_connections) noexcept
        : handler_(std::move(handler)), live_connections_(std::move(live_connections)) {}

    void OnOpen(ICoConnection& connection) override {
        ApplyAction(connection, handler_->OnOpen(connection));
    }

    void OnTraffic(ICoConnection& connection, bool wake) override {
        ApplyAction(connection, handler_->OnTraffic(connection, wake));
    }

    void OnClose(ICoConnection& connection, const std::error_code& reason) override {
        // 计数恰完成一次：正常返回、用户抛出、挂起跨 Engine 销毁都不漏减。
        const CountGuard guard{live_connections_};
        handler_->OnClose(connection, reason);
    }

private:
    // RAII：作用域退出完成一次减计数；guard 持共享计数跨挂起保活计数对象本身。
    class CountGuard {
    public:
        explicit CountGuard(std::shared_ptr<std::atomic<std::size_t>> counter) noexcept
            : counter_(std::move(counter)) {}
        CountGuard(const CountGuard&) = delete;
        CountGuard& operator=(const CountGuard&) = delete;
        ~CountGuard() { if (counter_) { (void)counter_->fetch_sub(1); } }

    private:
        std::shared_ptr<std::atomic<std::size_t>> counter_;
    };

    // 未交付/未知动作一律不附加动作，不降级成 Close。
    // 非 static（由 OnOpen/OnTraffic 调用）：Shutdown 分支需要把上报转发给本适配器按值持有的
    // handler_，不新增 ctor 参数、不新增成员。
    void ApplyAction(ICoConnection& connection, Action action) {
        if (action == Action::Shutdown) {
            // 向上请求上报：与产生它的事件同一个受管协程任务内同步转发一次。
            // 不 Close 该连接、不封门、不停 ticker、不改任何引擎状态；不降级成 Close。
            handler_->OnShutdownRequest();
            return;
        }
        if (action != Action::Close) { return; }
        // 干净关闭（空原因）：事件协程内合法，封口后命令可靠接纳。
        (void)connection.Close(CloseMode::Unblock, std::error_code{},
            std::chrono::steady_clock::now() + kCloseDeadline);
    }

    static constexpr std::chrono::seconds kCloseDeadline{30};
    std::shared_ptr<EventHandler> handler_;
    std::shared_ptr<std::atomic<std::size_t>> live_connections_;
};

} // namespace bbt::infra::net
