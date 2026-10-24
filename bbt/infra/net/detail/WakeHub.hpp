#pragma once

// Co 等待订阅枢纽（detail 私有部件）：登记 CoWaiter，状态变化时在锁外
// NotifyAll。登记与投递分离，调用者按「登记 → 复查谓词 → 等待」模式使用，
// 复查由 CoWaiter 的 on_registered 回调完成，避免早到通知丢失。
//
// 有界：登记时清理已失效 weak 项；超过上限返回 nullptr，调用者把它当
// 「本轮无法登记」处理，不无界堆积 weak。

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
    static void SetCreateFaultOnceForTest() { s_create_fault_once.store(true); }
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> Create() {
        if (s_create_fault_once.exchange(false)) {
            throw std::bad_alloc();  // 私有 test：等待对象建立单次失败
        }
        return std::make_shared<bbt::coroutine::sync::CoWaiter>();
    }
    bool Add(const std::shared_ptr<bbt::coroutine::sync::CoWaiter>& waiter) {
        std::lock_guard<std::mutex> lock(m_mutex);
        PruneLocked();
        if (m_waiters.size() >= kMaxWaiters) {
            return false;
        }
        m_waiters.push_back(waiter);
        return true;
    }
    // 兼容便捷入口（外部调用者若在等待前 Register，仍需 on_registered 复查）。
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> Register() {
        auto waiter = Create();
        if (!Add(waiter)) {
            return nullptr;
        }
        return waiter;
    }

    // 必须在调用者外层锁之外调用。
    //
    // Notify 可能因等待者尚未进入 COND_WAIT、或本轮等待已终态而返回 -1。这里
    // 保留 weak 项只作防御：已交付项在下一次 prune 随 weak 失效移除，重复
    // Notify 对已决议项返回 -1 且无副作用。
    //
    // 说明：上游 CoWaiter 的 PENDING 路径已能兑现「on_registered 同步 Notify」
    // （独立 probe 2000/2000 验证）；B5 CoRead 实测卡死已定位为 net 在 Busy 之后
    // 才采 release 代次，**不是**早期 Notify 丢失，故不要据此推断 runtime 缺口或
    // 回滚 Create/Add/有界订阅。
    //
    // 有界栈 snapshot + hub 锁下复制、锁外 Notify：不持锁跨 Notify，也不在
    // Notify 路径分配（Add 上限 = 数组容量）。
    void NotifyAll() {
        std::unique_lock<std::mutex> lock(m_mutex);
        if (m_waiters.empty()) {
            return;  // 登记后复查谓词；空 hub 不需要构造 snapshot 或再次加锁。
        }
        std::array<std::shared_ptr<bbt::coroutine::sync::CoWaiter>, kMaxWaiters> snapshot;
        std::size_t count = 0;
        for (auto& weak : m_waiters) {
            if (count >= kMaxWaiters) {
                break;
            }
            if (auto waiter = weak.lock()) {
                snapshot[count++] = std::move(waiter);
            }
        }
        lock.unlock();
        for (std::size_t i = 0; i < count; ++i) {
            (void)snapshot[i]->Notify();
        }
        std::lock_guard<std::mutex> prune_lock(m_mutex);
        PruneLocked();
    }

private:
    void PruneLocked() {
        std::size_t write = 0;
        for (std::size_t read = 0; read < m_waiters.size(); ++read) {
            if (!m_waiters[read].expired()) {
                if (write != read) {
                    m_waiters[write] = std::move(m_waiters[read]);
                }
                ++write;
            }
        }
        m_waiters.resize(write);
    }

    mutable std::mutex m_mutex;
    std::vector<std::weak_ptr<bbt::coroutine::sync::CoWaiter>> m_waiters;
    inline static std::atomic<bool> s_create_fault_once{false};
};

} // namespace bbt::infra::net
