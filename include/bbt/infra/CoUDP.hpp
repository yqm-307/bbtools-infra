#pragma once

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <condition_variable>
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
// detail::CloseWaiters（关闭期挂起等待者登记，定义在 src/detail/IoSupport.hpp）
// 同理只前置声明：公共头只以 shared_ptr 持有该仓库内部实现。
namespace bbt::infra::detail {
struct TransportWiring;
class CloseWaiters;
} // namespace bbt::infra::detail

namespace bbt::infra::udp {

// CoUDP：co-io-adapter/v1 §5 无连接 datagram 传输（Issue #31 切片）。
// 一个 Send 对应一个完整 datagram，保留报文边界；零长度 datagram 合法，
// 不是 EOF（UDP 无 EOF）。Receive 返回实际 peer 与截断状态。
// 单一 owner 使用契约（§6.2.1）：同一时刻至多一个执行主体操作本对象；
// 关闭协议按 §1（进程寿命运行时修订）：Close() 任意线程安全、幂等，先封口
// 唤醒在途等待者，再于自身锁 + condition_variable 上有界等待在途 syscall
// 计数归零，最后物理 close 并一次性跑 closed hook——晚于封口的物理关闭仍受
// 在途计数门控，杜绝封口即 close。
class CoUDP final : public ICoNetwork, public ICoCloseable,
                    public std::enable_shared_from_this<CoUDP> {
public:
    using SPtr = std::shared_ptr<CoUDP>;

    // §4.0.1：控制线程配置操作，不挂起、不等网络事件；local 只接受数值
    // 地址（ip 不得为空，通配须显式写 0.0.0.0；端口 0 表示动态分配），
    // 要求 coroutine 运行时已初始化。LocalAddress 返回实际绑定地址。
    static result<SPtr> BindUDP(SocketAddress local);

    ~CoUDP() override;

    bbt::coroutine::CoObjectInfo GetObjectInfo() const override;

    CoUDP(const CoUDP&) = delete;
    CoUDP& operator=(const CoUDP&) = delete;

    // §5 Try*：只尝试一次，绝不挂起；无数据/不可发返回
    // IoState::WouldBlock（不经过 IoWait，无双重等待路径）。参数与关闭状态
    // 照 §4 检查；不要求协程上下文（非协程线程可调用），但要求 coroutine
    // 运行时已初始化。
    result<DatagramRead> TryReceive(MutableBytes dst);
    IoResult TrySend(ConstBytes packet, const SocketAddress& peer);

    // §5 可挂起方法：WouldBlock 时经 CoWaiter 组合等待（deadline/close 两源
    // 首胜，复用上游 sync::CoWaiter，不复制等待状态机；业务取消由协程级
    // RequestCancel 或上层带载荷 Notify 表达，infra 不再持有取消令牌）。
    result<DatagramRead> Receive(MutableBytes dst, const CallOptions& options);
    IoResult Send(ConstBytes packet, const SocketAddress& peer,
                  const CallOptions& options);

    // 实际绑定地址；未成功 bind 过返回 { "", 0 }。
    SocketAddress LocalAddress() const;

    // 幂等、任意线程可调用；返回即物理资源已释放（fd 已关、后端不再访问）。
    // 实现口径见 §1 与类注释：封口 → 唤醒全部挂起等待者 → 有界排空在途
    // syscall → 物理 close 并一次性跑 closed hook。
    void Close() noexcept override;
    bool IsClosed() const noexcept override;


private:
    friend struct bbt::infra::detail::TransportWiring;

    explicit CoUDP(int fd, bbt::coroutine::CoObjectInfo info) noexcept;

    // §5.2 组合等待输入登记（readable/writable + deadline + close）单轮一次性
    // 挂起；返回原因。仅协程内可调用。close 源改为对象级 CloseWaiters：
    // 封口唤醒以 Completed 抵达，本函数解释为 Closed。
    enum class WaitOutcome { Ready, Closed, Cancelled, TimedOut,
                             InvalidContext, RuntimeUnavailable };
    WaitOutcome _Wait(bool readable, const CallOptions& options);
    void _CloseFd() noexcept;              // m_mtx held；物理释放（幂等）
    bool _SettleClosedLocked() noexcept;   // m_mtx held；物理收口恰好一次
    result<void> _CheckEntry(bool in_coroutine_only) const;

    int m_fd{-1};
    // 关闭协议状态（§1/§6.4）：m_close_requested 任意线程置位（封口）；
    // m_inflight 在途 syscall 计数；物理 close 由 Close 在 in-flight==0 时
    // 执行（有界等待后放弃等待者也照此释放）。m_fd/m_local 仅在锁内变更。
    mutable std::mutex m_mtx;
    std::condition_variable m_cv;
    bool m_close_requested{false};
    bool m_physically_closed{false};
    int  m_inflight{0};
    // §4.0.1.7：本对象尚未归还账本的名额数；op 正常收尾归还 1，
    // Close/析构一次性归还剩余。
    int  m_quota_held{0};
    SocketAddress m_local;
    bbt::coroutine::CoObjectInfo m_info;
    // 关闭期挂起等待者登记（Close 的唤醒侧）：Receive/Send 的组合等待在事件
    // 登记成功后登记，封口后登记失败者自行 Notify。
    std::shared_ptr<bbt::infra::detail::CloseWaiters> m_close_waiters;
    // 以下装配状态只经 detail::TransportWiring 读写（不安装到公共面）：
    // ClosedHook（物理关闭落定记账）、在途账本钩、测试等待入口 gate。
    std::function<void()> m_closed_hook;
    // §4.0.1.7 在途配额（仅受管对象注入）：admit/release 与 Runtime
    // 账本严格配对，scope 保活账本。
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
