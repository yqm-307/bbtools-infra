#pragma once

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra::udp {

// CoUDP：co-io-adapter/v1 §5 无连接 datagram 传输（Issue #31 切片）。
// 一个 Send 对应一个完整 datagram，保留报文边界；零长度 datagram 合法，
// 不是 EOF（UDP 无 EOF）。Receive 返回实际 peer 与截断状态。
// 单一 owner 使用契约（§6.2.1）：同一时刻至多一个执行主体操作本对象；
// 关闭协议按 §6.4：RequestClose 任意线程安全、先封口唤醒在途等待者，
// 物理 close 由在途 syscall 计数归零门控（晚于封口），杜绝封口即 close。
class CoUDP final : public ICoNetwork, public ICoCloseable,
                    public std::enable_shared_from_this<CoUDP> {
public:
    using SPtr = std::shared_ptr<CoUDP>;

    // §4.0.1：控制线程配置操作，不挂起、不等网络事件；local 只接受数值
    // 地址（ip 不得为空，通配须显式写 0.0.0.0；端口 0 表示动态分配），
    // 要求 Scheduler 已 Start。LocalAddress 返回实际绑定地址。
    static result<SPtr> BindUDP(SocketAddress local);

    ~CoUDP() override;

    bbt::coroutine::CoObjectInfo GetObjectInfo() const override;

    CoUDP(const CoUDP&) = delete;
    CoUDP& operator=(const CoUDP&) = delete;

    // §5 Try*：只尝试一次，绝不挂起；无数据/不可发返回
    // IoState::WouldBlock（不经过 IoWait，无双重等待路径）。参数、代际与
    // 关闭状态照 §4 检查：代际不匹配即 RuntimeUnavailable，且不要求协程
    // 上下文（非协程线程可调用）。
    result<DatagramRead> TryReceive(MutableBytes dst);
    IoResult TrySend(ConstBytes packet, const SocketAddress& peer);

    // §5 可挂起方法：WouldBlock 时经 CoWaiter 组合等待（deadline/cancel/
    // close 三源首胜，复用上游 sync::CoWaiter，不复制等待状态机）。
    result<DatagramRead> Receive(MutableBytes dst, const CallOptions& options);
    IoResult Send(ConstBytes packet, const SocketAddress& peer,
                  const CallOptions& options);

    // 实际绑定地址；未成功 bind 过返回 { "", 0 }。
    SocketAddress LocalAddress() const;

    void RequestClose() noexcept override;
    bool IsClosed() const noexcept override;
    CloseStatus WaitClosed(bbt::coroutine::Deadline deadline,
                           bbt::coroutine::CancellationToken cancel) override;

    void SetClosedHook(std::function<void()> hook) noexcept {
        m_closed_hook = std::move(hook);
    }

    // §4.0.1.7：Runtime 级在途配额注入（仅受管对象；未托管入口不装钩）。
    // admit 在 _CheckEntry 通过、计入 m_inflight 之前调用——返回 false
    // 即 Overloaded，立即返回不挂起；quota_scope 保活 Runtime 共享账本，
    // Stop 不展开挂起协程栈时由对象侧归还，不依赖栈上 RAII。
    void SetInflightHooks(std::function<bool()> admit,
                          std::function<void()> release,
                          std::shared_ptr<void> quota_scope) noexcept {
        m_inflight_admit = std::move(admit);
        m_inflight_release = std::move(release);
        m_inflight_scope = std::move(quota_scope);
    }

    // 测试接缝（Issue #32）：可等待 op 在参数/上下文/代际检查与名额登记
    // （m_quota_held++）完成后、进入首次等待循环之前在协程内触发一次；
    // 生产路径不安装，为空零开销。约定：noexcept、不得回调本对象/取 m_mtx/
    // 阻塞——持锁线程与同 scheduler 上其他协程依赖它快速返回。
    void SetWaitEntryGateForTest(std::function<void()> gate) noexcept {
        m_wait_entry_gate_for_test = std::move(gate);
    }

private:
    explicit CoUDP(int fd, bbt::coroutine::CoObjectInfo info) noexcept;

    // §5.2 组合等待输入登记（readable/writable + deadline + cancel + close）
    // 单轮一次性挂起；返回原因。仅协程内可调用。
    enum class WaitOutcome { Ready, Closed, Cancelled, TimedOut,
                             InvalidContext, RuntimeUnavailable };
    WaitOutcome _Wait(bool readable, const CallOptions& options);
    void _CloseFd() noexcept;
    result<void> _CheckEntry(bool in_coroutine_only) const;

    int m_fd{-1};
    // 关闭协议状态（§6.4）：m_close_requested 任意线程置位（封口）；
    // m_inflight 在途 syscall 计数；物理 close 仅在 owner 线程于
    // in-flight==0 时执行。m_fd/m_local 仅在锁内变更。
    mutable std::mutex m_mtx;
    bool m_close_requested{false};
    bool m_physically_closed{false};
    int  m_inflight{0};
    // §4.0.1.7：本对象尚未归还账本的名额数；op 正常收尾归还 1，
    // RequestClose/析构一次性归还剩余——Stop 不展开栈时由后者兜底。
    int  m_quota_held{0};
    SocketAddress m_local;
    bbt::coroutine::CoObjectInfo m_info;
    // 关闭源：RequestClose 置位；组合进每次等待的 cancel，实现任意线程
    // 封口唤醒在途 Receive/Send/WaitClosed。
    std::shared_ptr<bbt::coroutine::CancellationSource> m_close_source{
        std::make_shared<bbt::coroutine::CancellationSource>()};
    std::shared_ptr<bbt::coroutine::CancellationSource> m_closed_source{
        std::make_shared<bbt::coroutine::CancellationSource>()};
    std::atomic_bool m_close_waiting{false};
    std::function<void()> m_closed_hook;
    // §4.0.1.7 在途配额（仅受管对象注入）：admit/release 与 Runtime
    // 账本严格配对，scope 保活账本跨越 Stop→Start 代际。
    std::function<bool()> m_inflight_admit;
    std::function<void()> m_inflight_release;
    std::shared_ptr<void> m_inflight_scope;
    // §4.0.1.7 测试接缝：见 SetWaitEntryGateForTest。
    std::function<void()> m_wait_entry_gate_for_test;
};

} // namespace bbt::infra::udp

// co-io-adapter/v1 §5 冻结公开名 bbt::infra::CoUDP：实现落在
// bbt::infra::udp（与 §4 的 CoTCP/CoTCPListener 同规则）。此处把冻结名
// 接上，使只包含本头的消费者也能按契约名书写；NetworkRuntime.hpp 的
// 同名 using 声明指向同一实体，重复声明合法。
namespace bbt::infra {
using udp::CoUDP;
} // namespace bbt::infra
