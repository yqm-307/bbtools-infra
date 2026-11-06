/** @file Watch.hpp
 * @brief 最小 watch：在项目协程执行域内轮询 source，按 (version, revision) 去重后回调消费者。
 *
 * 覆盖：初始快照、版本更新、重复版本去重、失败与恢复通知、关闭与晚到通知收口。
 * 回调在协程域执行，不新增 I/O 线程，不在第三方线程执行应用逻辑。
 *
 * 契约来源：docs/decisions/0006-infra-foundation-and-dynamic-config.md；公开契约与边界整理见 docs/guides/config-watch.md。
 */
#pragma once

#include <chrono>
#include <functional>
#include <memory>

#include <bbt/infra/ICoObject.hpp>
#include <bbt/infra/Result.hpp>
#include <bbt/infra/config/Snapshot.hpp>
#include <bbt/infra/config/Source.hpp>

namespace bbt::infra::config {

/// @brief 事件种类：Initial 为首次快照，Updated 为版本变化，Failed/Recovered 为读取失败与恢复。
enum class WatchEventKind { Initial, Updated, Failed, Recovered };

/**
 * @brief 投递给消费者的一次事件。
 */
struct WatchEvent {
    WatchEventKind kind{WatchEventKind::Initial}; ///< 事件种类。
    Snapshot snapshot;                            ///< Initial、Updated、Recovered 时有效。
    Error error;                                  ///< Failed 时有效。
};

/**
 * @brief watch 选项。
 */
struct WatchOptions {
    std::chrono::milliseconds poll_interval{100}; ///< 轮询间隔，须大于零。
    bool deliver_initial{true};                   ///< 是否投递首次快照。
};

/// @brief 消费者回调类型。
using WatchCallback = std::function<void(const WatchEvent&)>;

/**
 * @brief 轮询 watch 的句柄：拥有 source 与回调，在协程域内按 (version, revision) 去重后投递事件。
 *
 * 关闭语义（Watcher 不是 ICoCloseable，不承诺同步物理释放）：
 *   - RequestClose() 幂等、可在任意域调用、非挂起。它只封口并唤醒正在睡眠的轮询协程。
 *     返回即经过回调起点线性化点：此后不会再开始任何消费者回调；本次轮询已读出但尚未进入回调的事件随之丢弃，
 *     不 flush、不重开。已获准的在途回调可执行完成，回调不得阻塞是契约前置。
 *   - IsClosed() 报告逻辑封口：RequestClose 已生效或轮询协程已退出。它不代表协程已物理退出，物理事实只由 WaitStopped 证明。
 *   - WaitStopped(deadline) 仅 native 宿主控制域调用，等待轮询协程真实退出。成功返回即协程已退出。
 *     前置是先 RequestClose，否则返回 InvalidArgument。在受管协程、已登记的 Processer 域或本 watcher 回调内调用返回 InvalidContext。
 *     deadline 到期返回 TimedOut，不冒充完成。
 *   - 回调内可调用 RequestClose() 与 IsClosed()，二者在回调线程内重入安全；封口后不会开始新的回调。回调内不得调用 WaitStopped，否则返回 InvalidContext。
 *
 * owner 与持有口径：
 *   - Watcher 由创建者按 SPtr 强持有；轮询协程自身只持 weak_ptr，挂起期间不把 Impl、source 或 callback 钉在协程栈上。
 *   - ~Watcher() 只调用 RequestClose() 请求封口，不等待。需要排空的宿主应先 WaitStopped 再释放句柄。
 *     句柄全部释放后 Impl 随之销毁；轮询协程下一拍见 weak 失效即退出，退出记号由协程帧上的独立 shared_ptr 落定，不依赖 Impl 存活。
 *
 * 回调与源约束（前置条件，违反即超出本契约）：
 *   - ISource::Read() 与消费者回调在轮询协程内联执行，须同步返回。
 *   - 回调不得阻塞。
 *   - poll_interval 必须大于零，且不超过 std::numeric_limits<int>::max() 毫秒（协程定时器的 int 毫秒口径），否则 Create 返回 InvalidArgument。
 */
class Watcher final : public std::enable_shared_from_this<Watcher> {
public:
    /// @brief Watcher 的共享句柄类型。
    using SPtr = std::shared_ptr<Watcher>;

    /**
     * @brief 创建 watcher 并注册轮询协程。
     *
     * 协程运行时未初始化返回 RuntimeUnavailable；poll_interval 非法返回 InvalidArgument；
     * 协程注册失败返回 RuntimeUnavailable。
     * @param source 配置源。
     * @param callback 消费者回调。
     * @param options 选项。
     */
    static result<SPtr> Create(SourcePtr source, WatchCallback callback, WatchOptions options = {});

    /// @brief 请求封口（不等待），见类说明。
    ~Watcher();

    /// @brief 返回对象标识，用于运行时诊断。
    bbt::coroutine::CoObjectInfo GetObjectInfo() const;

    /**
     * @brief 逻辑封口：幂等、任意域可调用、非挂起。返回仅表示封口已登记。
     */
    void RequestClose() noexcept;

    /**
     * @brief 等待轮询协程真实退出，仅 native 宿主控制域调用。
     * @param deadline 等待期限。
     * @return 成功表示协程已退出；错误见类说明。
     */
    result<void> WaitStopped(bbt::coroutine::Deadline deadline);

    /**
     * @brief 逻辑封口投影：已 RequestClose 或轮询协程已退出时为 true。
     *
     * 不代表协程已物理退出，物理事实只由 WaitStopped 证明。
     */
    bool IsClosed() const noexcept;

    /// @brief 实现细节类型，在 .cc 中定义；此处仅前向声明，消费者不可使用其成员。
    struct Impl;

private:
    explicit Watcher(std::shared_ptr<Impl> impl);
    std::shared_ptr<Impl> m_impl;
};

} // namespace bbt::infra::config
