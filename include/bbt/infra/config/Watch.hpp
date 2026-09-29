#pragma once
// config/v1：最小 watch——在项目协程执行域内轮询 source，按 (version, revision)
// 去重后回调消费者。契约来源：docs/decisions/0006（bbtools-infra#35），公开契约
// 与边界整理见 docs/config-watch-v1.md。
//
// 覆盖：初始快照、版本更新、重复版本去重、失败/恢复通知、关闭与晚到通知收口。
// 回调在协程域执行，不新增 I/O 线程、不在第三方线程执行应用逻辑。
//
// 关闭语义（同 co-network/v1 §关闭规则）：
//   - RequestClose() 幂等：Open → Closing；轮询循环在下一拍（≤ poll_interval）
//     观察到关闭请求后 drain，不再投递消费者，然后置 Closed。
//   - WaitClosed() 只在协程内等待，每个对象至多一个并发等待者，第二个返回
//     AlreadyWaiting；映射固定：Completed→Closed、TimedOut/Cancelled/
//     InvalidContext/AlreadyWaiting/RuntimeUnavailable 一一同名。超时/取消
//     不撤销关闭，也不代表资源已释放。调用方须在等待期间保持 Watcher 存活。
//   - 关闭请求落在「本次拍的 Read 已完成、事件尚未投递」之间时，该事件被丢弃；
//     关闭后不再调用消费者。
//
// 回调与源约束（前置条件，违反即超出本契约）：
//   - ISource::Read() 与消费者回调都在协程域内联执行，必须同步返回、不得挂起
//     （不得调用任何协程等待原语）。轮询循环在本次拍内持有源与回调的强引用，
//     而协程契约 §6 的强制 Stop 不展开挂起栈；Stop 命中用户代码挂起点时，
//     配置源可能在外部句柄释放后仍被永久保留。
//   - 回调不得阻塞（不持内部锁调用），可在回调内调用 RequestClose/IsClosed。

#include <chrono>
#include <functional>
#include <memory>

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/config/Snapshot.hpp>
#include <bbt/infra/config/Source.hpp>

namespace bbt::infra::config {

enum class WatchEventKind { Initial, Updated, Failed, Recovered };

struct WatchEvent {
    WatchEventKind kind{WatchEventKind::Initial};
    Snapshot snapshot; // Initial/Updated/Recovered 有效
    Error error;       // Failed 有效
};

struct WatchOptions {
    std::chrono::milliseconds poll_interval{100};
    bool deliver_initial{true};
};

using WatchCallback = std::function<void(const WatchEvent&)>;

class Watcher final : public bbt::infra::ICoCloseable,
                      public std::enable_shared_from_this<Watcher> {
public:
    using SPtr = std::shared_ptr<Watcher>;

    // 前置：Scheduler 已运行；否则返回 RuntimeUnavailable。
    static result<SPtr> Create(SourcePtr source, WatchCallback callback,
                               WatchOptions options = {});

    ~Watcher() override;

    bbt::coroutine::CoObjectInfo GetObjectInfo() const;

    void RequestClose() noexcept override;
    bool IsClosed() const noexcept override;
    bbt::infra::CloseStatus WaitClosed(bbt::coroutine::Deadline deadline,
                                       bbt::coroutine::CancellationToken cancel) override;

    // 实现细节类型（.cc 内定义；此处仅前向声明，消费者不可用其成员）。
    struct Impl;

private:
    explicit Watcher(std::shared_ptr<Impl> impl);
    std::shared_ptr<Impl> m_impl;
};

} // namespace bbt::infra::config
