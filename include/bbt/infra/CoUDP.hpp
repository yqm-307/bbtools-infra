#pragma once

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

// src 内部装配面（owner 装配 + 测试接缝）前置声明；定义在
// src/detail/TransportWiring.hpp（不安装、不进 INSTALL_INTERFACE）。
namespace bbt::infra::detail {
struct TransportWiring;
} // namespace bbt::infra::detail

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


private:
    friend struct bbt::infra::detail::TransportWiring;

    /* P4-B 硬停强制物理关闭（I1/I3/I4）。**不是公共 API**：本入口是私有非虚成员，
     * 普通消费者（协议 binding / 应用代码）不可调用，也不在 §4/§5 的契约签名与
     * 文档契约里。唯一可达路径是内部装配面 detail::TransportWiring 的对象级转发器
     * （friend 关系）——由 owner 的批量硬停入口逐对象调用，同仓测试亦经该装配面调用。
     * 前置条件（调用方保证）：执行本对象 op 的调度器已静默——Scheduler::Stop()
     * 已返回，此后不存在任何线程/回调会执行本对象的 op 或等待回调。
     * 语义：不受门控，直接物理 close 并落定（close/closesource 取消 + closed_hook），
     * 幂等（物理关闭恰好一次）；返回 true 表示本次调用完成了物理关闭。
     * 唯一生产调用点是 owner 的批量硬停入口
     * （detail::TransportWiring::ForceCloseAfterQuiescence，逐对象调用）：本入口
     * 不参与运行期路径，运行期关闭仍必须走 RequestClose 的在途门控。
     * 危险性：非静默状态下调用不安全——在途 op 持有裸 fd 且已注册给 poller，
     * 立即关闭会让同一 fd 号被新 socket 复用（跨连接串写/UAF）。 */
    bool ForceCloseAfterQuiescence() noexcept;

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
    // 以下装配状态只经 detail::TransportWiring 读写（不安装到公共面）：
    // ClosedHook（物理关闭落定记账）、在途账本钩、测试等待入口 gate。
    std::function<void()> m_closed_hook;
    // §4.0.1.7 在途配额（仅受管对象注入）：admit/release 与 Runtime
    // 账本严格配对，scope 保活账本跨越 Stop→Start 代际。
    std::function<bool()> m_inflight_admit;
    std::function<void()> m_inflight_release;
    std::shared_ptr<void> m_inflight_scope;
    // §4.0.1.7 测试接缝：名额登记完成、进入首次等待前回调一次。
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
