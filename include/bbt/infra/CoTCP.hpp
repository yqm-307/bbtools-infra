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

// src 内部装配面（owner 装配 + 测试接缝）只在此前置声明，定义在
// src/detail/TransportWiring.hpp（不安装、不进 INSTALL_INTERFACE）。
// 公共头不承载其定义，也不把它们计入对外契约与 ABI。
// detail::CloseWaiters（关闭期挂起等待者登记，定义在 src/detail/IoSupport.hpp）
// 同理只前置声明：公共头只以 shared_ptr 持有其仓库内部实现。
namespace bbt::infra::detail {
struct TransportWiring;
struct DialWaitOptions;
class CloseWaiters;
} // namespace bbt::infra::detail

namespace bbt::infra::tcp {

class CoTCPListener;

class CoTCP final : public ICoNetwork, public ICoCloseable,
                    public std::enable_shared_from_this<CoTCP> {
public:
    using SPtr = std::shared_ptr<CoTCP>;

    // co-io-adapter/v1 §4.0.1：未受 Runtime 托管的直接 transport 入口
    // （无 Capacity/在途账本语义）。受管路径另经 src 内部装配面
    // （detail::TransportWiring）装配受管 dial 等待段接缝。
    static result<SPtr> DialTCP(std::string host, std::uint16_t port,
                                const CallOptions& options);

    ~CoTCP() override;

    bbt::coroutine::CoObjectInfo GetObjectInfo() const override;

    CoTCP(const CoTCP&) = delete;
    CoTCP& operator=(const CoTCP&) = delete;

    IoResult TryReadSome(MutableBytes dst);
    IoResult TryWriteSome(ConstBytes src);
    IoResult ReadSome(MutableBytes dst, const CallOptions& options);
    IoResult WriteSome(ConstBytes src, const CallOptions& options);
    IoResult WriteAll(ConstBytes src, const CallOptions& options);

    // 幂等、任意线程可调用；返回即物理资源已释放（fd 已关、后端不再访问），
    // 未发送数据直接丢弃、不 flush、不重开。实现口径（§1）：
    //   1) 原子封口并唤醒全部挂起 op（detail::CloseWaiters::
    //      SealWakeAndDrainRegistrations：挂起 op 在事件登记成功后自行登记，
    //      封口后登记失败者自行 Notify，不丢唤醒）；
    //   2) 数据路径的「最后封口检查 + 非阻塞 syscall」持同一把 m_mtx，故封口
    //      置位后不可能再有 syscall 越过本锁（修复 check→syscall 窗口）；
    //   3) 等待兴趣登记屏障（detail::CloseWaiters::BeginRegister/EndRegister）
    //      把「最后一次封口检查」与「fd 兴趣登记」拉成同一线性化协议：封口后
    //      晚到 op 得到明确 Closed，物理 close 不早于在途登记完成，旧 fd 不会
    //      在已关/复用后被登记；
    //   4) 物理 close（shutdown + close）并一次性跑 closed hook；并发/重复
    //      Close 无独立上限地等首次调用者的真实物理落定。
    // 关闭由 owner 主动发起；coroutine runtime 不参与资源收口。
    void Close() noexcept override;
    bool IsClosed() const noexcept override;


private:
    friend class CoTCPListener;
    friend struct bbt::infra::detail::TransportWiring;

    explicit CoTCP(int fd, bbt::coroutine::CoObjectInfo info) noexcept;

    // 受管 DialTCP：受管等待段接缝（owner 级关闭唤醒登记 + 挂起落定回调）
    // 由 owner 经 detail::TransportWiring 装配；公共静态入口以默认语义调用。
    static result<SPtr> _DialTcp(std::string host, std::uint16_t port,
                                 const CallOptions& options,
                                 const bbt::infra::detail::DialWaitOptions& dial_wait);

    // 参数之外的入口门禁：协程上下文（仅可挂起入口要求）与 coroutine 运行时
    // 是否已初始化（对象身份不再携带代际）。
    result<void> _CheckEntry(bool in_coroutine_only) const;

    result<std::size_t> _ReadSome(void* buffer, std::size_t size,
                                  const CallOptions& options);
    result<std::size_t> _WriteSome(const void* buffer, std::size_t size,
                                   const CallOptions& options);
    // WriteAll 的重试循环主体：与 _ReadSome/_WriteSome 共用入口门禁，但
    // **不**在自身入口占名额——在途粒度是每一轮内部单次写尝试（每轮
    // admit/归还，见 0005 §4.0.1.7）；已写字节数经 error.transferred_bytes
    // 回报。公共 WriteAll(ConstBytes) 只做 IoProgress 映射。
    result<std::size_t> _WriteAll(const void* buffer, std::size_t size,
                                  const CallOptions& options);
    void _CloseFd() noexcept;              // m_mtx held；物理释放（幂等）
    // m_mtx held：物理收口恰好一次（释放 fd + 落定 closed + 唤醒并发 Close
    // 等待者）；返回本次调用是否完成收口。closed hook 由调用方在锁外跑一次。
    bool _SettleClosedLocked() noexcept;
    void _EndIo() noexcept;
    // §4.0.1.7：归还本对象仍持有的在途名额（m_mtx held；幂等）。
    // op 正常结束经 _EndIo 归还 1；封口时仍挂起的 op（有界排空超时或同线程
    // 恢复）由 Close 的 DrainQuota 兜底归还剩余名额。
    void _DrainQuotaLocked() noexcept {
        if (m_inflight_release && m_quota_held > 0) {
            const auto n = m_quota_held;
            m_quota_held = 0;
            for (std::size_t i = 0; i < n; ++i)
                m_inflight_release();
        }
    }

    int m_fd{-1};
    // m_closed：物理收口已完成（IsClosed 语义）；m_closing：已封口（Close
    // 首次进入；任意线程读得，故为原子）。
    std::atomic_bool m_closing{false};
    bool m_closed{false};
    int m_inflight{0};
    // §4.0.1.7：本对象已 admit 未归还的 Runtime 在途名额（m_mtx 保护）。
    std::size_t m_quota_held{0};
    mutable std::mutex m_mtx;
    // Close 的在途排空等待（等待兴趣登记期 fd 有效）与并发 Close 的收口等待
    // （m_mtx + m_closed / m_inflight 为谓词）。
    std::condition_variable m_cv;
    bbt::coroutine::CoObjectInfo m_info;
    // 关闭期挂起等待者登记（Close 的唤醒侧）；挂起 op 在事件登记成功后的
    // on_registered 内 Add，封口后 Add 失败者自行 Notify。
    std::shared_ptr<bbt::infra::detail::CloseWaiters> m_close_waiters;
    // 以下五组装配状态只经 detail::TransportWiring 读写（不安装到公共面）：
    //   m_closed_hook / m_inflight_* 由 Runtime owner 装配（登记与账本归还）；
    //   m_wait_entry_gate_for_test / m_io_wait_registered_gate_for_test 仅供
    //   测试确定性观察，生产路径不安装（空 std::function 零开销）。
    std::function<void()> m_closed_hook;   // 物理关闭落定通知（Runtime 托管记账）
    // §4.0.1.7 在途配额（仅受管对象注入）：admit/release 经 shared_ptr
    // 账本与 Runtime 配对；scope 保活账本，对象侧 teardown/析构归还剩余名额，
    // 不依赖协程栈 RAII。
    std::function<bool()> m_inflight_admit;
    std::function<void()> m_inflight_release;
    std::shared_ptr<void> m_inflight_scope;
    // §4.0.1.7 测试接缝：名额登记完成、进入首次等待前回调一次。
    std::function<void()> m_wait_entry_gate_for_test;
    // §4.0.1.7 F3 测试接缝：数据路径挂起落定后回调（_WaitFd on_registered），
    // 证明 op 真实挂在 fd 等待上。
    std::function<void()> m_io_wait_registered_gate_for_test;
};

class CoTCPListener final : public ICoNetwork, public ICoCloseable,
                            public std::enable_shared_from_this<CoTCPListener> {
public:
    using SPtr = std::shared_ptr<CoTCPListener>;

    static result<SPtr> ListenTCP(std::string host, std::uint16_t port,
                                  int backlog = 128);

    ~CoTCPListener() override;

    bbt::coroutine::CoObjectInfo GetObjectInfo() const override;

    CoTCPListener(const CoTCPListener&) = delete;
    CoTCPListener& operator=(const CoTCPListener&) = delete;

    result<std::shared_ptr<CoTCP>> Accept(const CallOptions& options);
    SocketAddress LocalAddress() const;

    // 幂等、任意线程可调用；语义与 CoTCP::Close 完全一致（封口 → 唤醒挂起
    // Accept/等待者 → 物理 close 并一次性跑 closed hook；最后封口检查 +
    // 非阻塞 accept4 与 _CloseFd 持同一把 m_mtx 配对）。
    void Close() noexcept override;
    bool IsClosed() const noexcept override;


private:
    friend struct bbt::infra::detail::TransportWiring;

    explicit CoTCPListener(int fd, bbt::coroutine::CoObjectInfo info) noexcept;
    result<void> _CheckEntry() const;
    void _CloseFd() noexcept;              // m_mtx held；物理释放（幂等）
    bool _SettleClosedLocked() noexcept;   // m_mtx held；物理收口恰好一次
    void _EndIo() noexcept;
    // §4.0.1.7：归还本 listener 仍持有的在途名额（m_mtx held；幂等）。
    void _DrainQuotaLocked() noexcept {
        if (m_inflight_release && m_quota_held > 0) {
            const auto n = m_quota_held;
            m_quota_held = 0;
            for (std::size_t i = 0; i < n; ++i)
                m_inflight_release();
        }
    }
    // 正常终态出账一个连接容量预留（m_mtx held；幂等）。预留的容量归还
    // （m_accept_release）在 Accept 的唯一返回路径上执行一次——挂起的 Accept
    // 由 Close 唤醒后照常走该路径，故不再需要停机期的兜底出账。
    void _ConsumeAcceptReservedLocked() noexcept {
        if (m_accept_reserved > 0) --m_accept_reserved;
    }

    int m_fd{-1};
    std::atomic_bool m_closing{false};
    bool m_closed{false};
    int m_inflight{0};
    // §4.0.1.7：已 admit 未归还的 Runtime 在途名额（m_mtx 保护）。
    std::size_t m_quota_held{0};
    // §4.0.1.7：本 listener 已 m_accept_admit 未出账的连接容量预留数
    // （m_mtx 保护）。
    std::size_t m_accept_reserved{0};
    mutable std::mutex m_mtx;
    std::condition_variable m_cv;
    bbt::coroutine::CoObjectInfo m_info;
    std::shared_ptr<bbt::infra::detail::CloseWaiters> m_close_waiters;
    // 以下装配状态只经 detail::TransportWiring 读写（不安装到公共面）：
    // ClosedHook（物理关闭落定记账）、Accept 容量/接纳收养钩、在途账本钩、
    // 测试等待入口 gate。
    std::function<void()> m_closed_hook;   // 物理关闭落定通知（Runtime 托管记账）
    std::function<bool()> m_accept_admit;
    std::function<void()> m_accept_release;
    std::function<bool(std::shared_ptr<CoTCP>)> m_accept_adopt;
    // §4.0.1.7 在途配额（语义同 CoTCP 同名成员）：Accept 的 admit 先于
    // m_accept_admit 判定，未托管 listener 不装钩（Unlimited 语义）。
    std::function<bool()> m_inflight_admit;
    std::function<void()> m_inflight_release;
    std::shared_ptr<void> m_inflight_scope;
    // §4.0.1.7 测试接缝：名额登记完成、首次 accept4/等待前回调一次。
    std::function<void()> m_wait_entry_gate_for_test;
    // 测试接缝：Accept 已成功取得 child、尚未执行 owner adopt 前回调一次。
    std::function<void()> m_accept_adopt_gate_for_test;
};

} // namespace bbt::infra::tcp
