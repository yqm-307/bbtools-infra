#pragma once

/**
 * 有界等待订阅：各通道独立计额，锁内复制快照，锁外通知。
 * 每轮使用新的 CoWaiter，避免已取走的旧快照通知下一轮等待。
 */
#include <array>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include <bbt/coroutine/sync/CoWaiter.hpp>
#include <bbt/infra/net/detail/TestHooks.hpp>

namespace bbt::infra::net {

enum class WaitLane { Data, Release, Write, Close, Connect, Accept, Count };

class WaitSet {
public:
    static constexpr std::size_t kMaxWaiters = 256;
    bool Add(WaitLane lane, const bbt::coroutine::sync::CoWaiter::SPtr& waiter);
    void Remove(WaitLane lane, const bbt::coroutine::sync::CoWaiter::SPtr& waiter);
    void Notify(WaitLane lane);

private:
    static constexpr std::size_t kLaneCount = static_cast<std::size_t>(WaitLane::Count);
    std::mutex m_mutex;
    std::array<std::weak_ptr<bbt::coroutine::sync::CoWaiter>, kLaneCount> m_first;
    std::array<std::vector<std::weak_ptr<bbt::coroutine::sync::CoWaiter>>, kLaneCount> m_overflow;
};

enum class WaitProblem { None, CreateFailed, RegisterFull };

struct WaitChangeResult {
    bbt::coroutine::WaitStatus status{bbt::coroutine::WaitStatus::InvalidContext};
    WaitProblem problem{WaitProblem::None};
};

bbt::coroutine::sync::CoWaiter::SPtr CreateWaiter(const std::shared_ptr<TestHooks>& hooks = {});

/**
 * runtime 登记成功后再 Add、运行屏障、复查持久谓词；返回时仅注销本轮。
 * 原样传递 deadline 与异常，不在这里选择公共错误映射或增加轮询分片。
 */
WaitChangeResult WaitChange(
    WaitSet& set,
    WaitLane lane,
    const bbt::coroutine::WaitOptions& options,
    const std::function<bool()>& ready,
    const std::shared_ptr<TestHooks>& hooks,
    const std::function<void()>& on_added = {});

// clang-format off
}
// clang-format on
