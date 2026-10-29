#pragma once
// config/v1：最小 watch——在项目协程执行域内轮询 source，按 (version, revision)
// 去重后回调消费者。契约来源：docs/decisions/0006（bbtools-infra#35），公开契约
// 与边界整理见 docs/config-watch-v1.md。
//
// 覆盖：初始快照、版本更新、重复版本去重、失败/恢复通知、关闭与晚到通知收口。
// 回调在协程域执行，不新增 I/O 线程、不在第三方线程执行应用逻辑。
//
// 关闭语义（F3 修订：Watcher 不是 ICoCloseable，不承诺同步物理释放）：
//   - RequestClose() 幂等、可在任意域调用、非挂起：只封口并唤醒正在睡眠的
//     轮询协程（detail::CloseWaiters + CoWaiter::Notify，跨线程安全）。返回
//     即经过回调起点线性化点：此后不会再**开始**任何消费者回调（本次拍
//     已读出但尚未进入 callback 的事件随之丢弃），不 flush、不重开；
//     已获准的在途回调可执行完成（回调不得阻塞是契约前置）。
//   - IsClosed() 只报告逻辑封口（RequestClose 已生效，或轮询协程已退出）。
//     它不代表轮询协程已物理退出——物理事实由 WaitStopped 成功证明。
//   - WaitStopped(deadline) 仅 native 宿主控制域调用：等待轮询协程真实退出，
//     成功返回即协程已退出、其等待与定时器已释放，此后无任何后续回调/等待
//     资源责任。前置：先 RequestClose（未封口调用返回 InvalidArgument）。
//     在受管协程、已登记 Processer 域或本 watcher 回调内调用返回
//     InvalidContext（那是等待自身/被管域挂起宿主线程）。deadline 到期
//     返回 TimedOut——不冒充完成；运行时被驱动期间轮询协程总会收口，
//     owner 须在 runtime 存活期间完成等待。
//   - 已注册但从未被调度过的轮询协程：WaitStopped 不提前报物理完成——它
//     首次运行的第一拍就是「已封口 → 退出」，届时 WaitStopped 返回成功；
//     runtime 一直不驱动它则到期 TimedOut。
//   - 回调内可调用 RequestClose()/IsClosed()（回调在轮询协程内联执行）：
//     封口后本次回调返回后的下一拍循环退出，期间不会再有任何回调。
//     回调内不得调用 WaitStopped（等待自身，返回 InvalidContext）。
//
// owner 域与持有口径：
//   - Watcher 由创建者按 SPtr 强持有；轮询协程自身只持 weak_ptr，挂起期间不
//     把 Impl/source/callback 钉在自己的栈上。
//   - ~Watcher() 只调用 RequestClose() 请求封口，不等待：需要排空的宿主先
//     WaitStopped 再释放句柄。句柄全部释放后 Impl 随之销毁；轮询协程下一拍
//     见 weak 失效即退出，退出记号由协程帧上的独立 shared_ptr 落定，不靠
//     Impl 存活。
//   - source 与 callback 的强引用由 Watcher 句柄（Impl）持有，随句柄析构
//     释放：RequestClose 不销毁可能正在执行的回调对象。
//
// 回调与源约束（前置条件，违反即超出本契约）：
//   - ISource::Read() 与消费者回调都在协程域内联执行，必须同步返回、不得挂起
//     （不得调用任何协程等待原语）。
//   - 回调不得阻塞（不持内部锁调用），可在回调内调用 RequestClose()/IsClosed()。
//   - poll_interval 必须 > 0 且不超过定时器 int 毫秒口径上限（约 24.8 天），
//     否则 Create 返回 InvalidArgument（超限时协程定时器不挂，会永久挂起）。

#include <chrono>
#include <functional>
#include <memory>

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

class Watcher final : public std::enable_shared_from_this<Watcher> {
public:
    using SPtr = std::shared_ptr<Watcher>;

    // 前置：协程运行时已初始化（g_scheduler->IsInitialized()）；否则返回
    // RuntimeUnavailable。
    static result<SPtr> Create(SourcePtr source, WatchCallback callback,
                               WatchOptions options = {});

    ~Watcher();

    bbt::coroutine::CoObjectInfo GetObjectInfo() const;

    // 逻辑封口：幂等、任意域、非挂起。返回仅表示封口已登记。
    void RequestClose() noexcept;

    // 仅 native 宿主控制域：等待轮询协程真实退出。
    // InvalidContext：在受管协程/Processer 域或本 watcher 回调内调用；
    // InvalidArgument：未先 RequestClose；TimedOut：deadline 到期未退出。
    result<void> WaitStopped(bbt::coroutine::Deadline deadline);

    // 逻辑封口投影：true 表示已 RequestClose（或轮询协程已退出）。
    // 不代表协程已物理退出。
    bool IsClosed() const noexcept;

    // 实现细节类型（.cc 内定义；此处仅前向声明，消费者不可用其成员）。
    struct Impl;

private:
    explicit Watcher(std::shared_ptr<Impl> impl);
    std::shared_ptr<Impl> m_impl;
};

} // namespace bbt::infra::config
