#pragma once
// config/v1：最小 watch——在项目协程执行域内轮询 source，按 (version, revision)
// 去重后回调消费者。契约来源：docs/decisions/0006（bbtools-infra#35），公开契约
// 与边界整理见 docs/config-watch-v1.md。
//
// 覆盖：初始快照、版本更新、重复版本去重、失败/恢复通知、关闭与晚到通知收口。
// 回调在协程域执行，不新增 I/O 线程、不在第三方线程执行应用逻辑。
//
// 关闭语义（co-network/v1 进程寿命运行时修订；旧 RequestClose/WaitClosed/
// CloseStatus/运行时代际/取消令牌全部删除，不保留兼容壳）：
//   - Close() 幂等、可在任意线程调用；返回即轮询协程已停止、其等待与定时器
//     已释放，此后不会再调用消费者回调（本次拍已读出但尚未投递的事件随之丢弃）。
//     关闭不 flush 未投递事件、不重开、不做请求迁移。
//   - Close() 在其调用线程上有界等待轮询协程退出，上限
//     detail::kCloseDrainTimeout（5s）：先封口并唤醒正在睡眠的轮询协程
//     （detail::CloseWaiters + CoWaiter::Notify，跨线程安全），再等它退出。
//     超时即返回，此时 IsClosed() 仍为 false——运行时未被驱动/调度停摆时无法
//     从调用线程停掉一个已挂起的协程；owner 须在运行时被驱动期间完成收口。
//   - 已注册但从未被调度过的轮询协程：Close() 不等待（无从停）。它首次运行的
//     第一拍动作就是「已封口 → 退出」，不读源、不回调；此处按「不可能再回调」
//     落定 IsClosed()，只是没有可观察的协程退出可等。
//   - 回调内可调用 Close()/IsClosed()（回调在轮询协程内联执行）：此时 Close()
//     只封口并落定 IsClosed()，物理停止发生在本次回调返回后的下一拍；封口后
//     不会再有任何回调。不得在回调内同步等待本对象关闭（那是等待自身）。
//
// owner 域与持有口径：
//   - Watcher 由创建者按 SPtr 强持有；轮询协程自身只持 weak_ptr，挂起期间不
//     把 Impl/source/callback 钉在自己的栈上。
//   - ~Watcher() 调用 Close() 做同步收口：先停轮询协程，再释放 Impl。
//   - source 与 callback 的强引用由 Watcher 句柄（Impl）持有，随句柄析构释放：
//     Close() 不销毁可能正在执行的回调对象，也不承诺源在 Close() 返回瞬间析构。
//
// 回调与源约束（前置条件，违反即超出本契约）：
//   - ISource::Read() 与消费者回调都在协程域内联执行，必须同步返回、不得挂起
//     （不得调用任何协程等待原语）。
//   - 回调不得阻塞（不持内部锁调用），可在回调内调用 Close()/IsClosed()。
//   - poll_interval 必须 > 0 且不超过定时器 int 毫秒口径上限（约 24.8 天），
//     否则 Create 返回 InvalidArgument（超限时协程定时器不挂，会永久挂起）。

#include <chrono>
#include <functional>
#include <memory>

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoObject.hpp>
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

    // 前置：协程运行时已初始化（g_scheduler->IsInitialized()）；否则返回
    // RuntimeUnavailable。
    static result<SPtr> Create(SourcePtr source, WatchCallback callback,
                               WatchOptions options = {});

    ~Watcher() override;

    bbt::coroutine::CoObjectInfo GetObjectInfo() const;

    void Close() noexcept override;
    bool IsClosed() const noexcept override;

    // 实现细节类型（.cc 内定义；此处仅前向声明，消费者不可用其成员）。
    struct Impl;

private:
    explicit Watcher(std::shared_ptr<Impl> impl);
    std::shared_ptr<Impl> m_impl;
};

} // namespace bbt::infra::config
