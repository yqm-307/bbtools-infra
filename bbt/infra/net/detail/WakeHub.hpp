#pragma once

// Co 等待订阅枢纽（detail 私有部件）：登记 CoWaiter，状态变化时在锁外
// NotifyAll。登记与投递分离，调用者按「登记 → 复查谓词 → 等待」模式使用，
// 复查由 CoWaiter 的 on_registered 回调完成，避免早到通知丢失。
//
// 有界：登记时清理已失效 weak 项；超过上限返回 nullptr，调用者把它当
// 「当前无法登记」处理，不无界堆积 weak。

#include <array>
#include <atomic>
#include <new>
#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

#include <bbt/coroutine/sync/CoWaiter.hpp>

namespace bbt::infra::net {

class WakeHub {
public:
    static constexpr std::size_t kMaxWaiters = 256;
    // Create 只建对象；Add 放到 WaitWithCallback 的 on_registered 内（此时
    // runtime 已 _RegistAwaitEvent），避免「创建后、真正登记事件前」的 Notify
    // 在未注册事件上丢失。
    static void SetCreateFaultOnceForTest()
    {
        s_create_fault_once.store(true);
    }
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> Create();
    bool Add(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter);
    // 兼容便捷入口（外部调用者若在等待前 Register，仍需 on_registered 复查）。
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> Register();

    // 必须在调用者外层锁之外调用。
    //
    // Notify 可能因等待者尚未进入 COND_WAIT、或当前等待已进入终态而返回 -1。这里
    // 保留 weak 项只作防御：已交付项在下一次 prune 随 weak 失效移除，重复
    // Notify 对已决议项返回 -1 且无副作用。
    //
    // 上游 CoWaiter 的 PENDING 路径能兑现「on_registered 同步 Notify」。Busy 之后
    // 若未先采 release 代次，可能丢失释放通知，因此 Read 必须先采代次再尝试。
    // 这一时序是 net 层的责任，不能据此推断 runtime 存在缺口，也不能因此回退
    // Create、Add 或有界订阅的设计。
    //
    // 有界栈 snapshot + hub 锁下复制、锁外 Notify：不持锁跨 Notify，也不在
    // Notify 路径分配（Add 上限 = 数组容量）。
    void NotifyAll();

private:
    void PruneLocked();

    mutable std::mutex m_mutex;
    std::vector<std::weak_ptr<bbt::coroutine::sync::CoWaiter>> m_waiters;
    inline static std::atomic<bool> s_create_fault_once{false};
};

} // namespace bbt::infra::net

#include <bbt/infra/net/detail/impl/WakeHubImpl.hpp>
