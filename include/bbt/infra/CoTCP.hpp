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

// src 内部装配面（owner 装配 + 测试接缝）只在此前置声明，定义在
// src/detail/TransportWiring.hpp（不安装、不进 INSTALL_INTERFACE）。
// 公共头不承载其定义，也不把它们计入对外契约与 ABI。
namespace bbt::infra::detail {
struct TransportWiring;
struct DialWaitOptions;
} // namespace bbt::infra::detail

namespace bbt::infra::tcp {

class CoTCPListener;

class CoTCP final : public ICoNetwork, public ICoCloseable,
                    public std::enable_shared_from_this<CoTCP> {
public:
    using SPtr = std::shared_ptr<CoTCP>;

    // co-io-adapter/v1 §4.0.1：未受 Runtime 托管的直接 transport 入口
    // （无 Capacity/在途账本语义）。受管路径另经 src 内部装配面
    // （detail::TransportWiring）装配等待段取消/归还接缝。
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

    void RequestClose() noexcept override;
    bool IsClosed() const noexcept override;
    CloseStatus WaitClosed(bbt::coroutine::Deadline deadline,
                           bbt::coroutine::CancellationToken cancel) override;


private:
    friend class CoTCPListener;
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

    explicit CoTCP(int fd) noexcept;

    // 受管 DialTCP：等待段接缝（取消源 + 挂起落定回调）由 owner 经
    // detail::TransportWiring 装配；公共静态入口以上述默认语义调用本函数。
    static result<SPtr> _DialTcp(std::string host, std::uint16_t port,
                                 const CallOptions& options,
                                 const bbt::infra::detail::DialWaitOptions& dial_wait);

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
    result<void> _Wait(bool readable, const CallOptions& options);
    void _CloseFd() noexcept; // m_mtx held
    void _EndIo() noexcept;
    // §4.0.1.7：归还本对象仍持有的在途名额（m_mtx held；幂等）。
    // op 正常结束经 _EndIo 归还 1；Stop 致挂起协程不展开栈时，由
    // RequestClose/析构的 DrainQuota 兜底归还剩余名额。
    void _DrainQuotaLocked() noexcept {
        if (m_inflight_release && m_quota_held > 0) {
            const auto n = m_quota_held;
            m_quota_held = 0;
            for (std::size_t i = 0; i < n; ++i)
                m_inflight_release();
        }
    }

    int m_fd{-1};
    bool m_closed{false};
    bool m_closing{false};
    int m_inflight{0};
    // §4.0.1.7：本对象已 admit 未归还的 Runtime 在途名额（m_mtx 保护）。
    std::size_t m_quota_held{0};
    mutable std::mutex m_mtx;
    std::atomic_bool m_close_waiting{false};
    std::shared_ptr<bbt::coroutine::CancellationSource> m_close_source{
        std::make_shared<bbt::coroutine::CancellationSource>()};
    std::shared_ptr<bbt::coroutine::CancellationSource> m_closed_source{
        std::make_shared<bbt::coroutine::CancellationSource>()};
    bbt::coroutine::CoObjectInfo m_info;
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> m_waiter;
    // 以下五组装配状态只经 detail::TransportWiring 读写（不安装到公共面）：
    //   m_closed_hook / m_inflight_* 由 Runtime owner 装配（登记与账本归还）；
    //   m_wait_entry_gate_for_test / m_io_wait_registered_gate_for_test 仅供
    //   测试确定性观察，生产路径不安装（空 std::function 零开销）。
    std::function<void()> m_closed_hook;   // 物理关闭落定通知（Runtime 托管记账）
    // §4.0.1.7 在途配额（仅受管对象注入）：admit/release 经 shared_ptr
    // 账本与 Runtime 配对；scope 保活账本，Stop 不展开栈时由对象侧
    // teardown/析构归还，不依赖协程栈 RAII。
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

    explicit CoTCPListener(int fd) noexcept;
    result<void> _CheckEntry() const;
    void _CloseFd() noexcept; // m_mtx held
    void _EndIo() noexcept;
    // §4.0.1.7：归还本 listener 仍持有的在途名额（m_mtx held；幂等）。
    // op 结束归还 1；Stop 致挂起协程不展开栈时由 RequestClose/析构兜底。
    void _DrainQuotaLocked() noexcept {
        if (m_inflight_release && m_quota_held > 0) {
            const auto n = m_quota_held;
            m_quota_held = 0;
            for (std::size_t i = 0; i < n; ++i)
                m_inflight_release();
        }
    }

    // §4.0.1.7：归还本 listener 仍持有的「接纳容量预留」（m_mtx held；幂等）。
    // Accept 在 m_accept_admit 成功后登记 1，正常终态（adopt 转移 / release
    // 归还）出账；硬停致挂起 Accept 永不返回时不执行任何出账，只能由
    // ForceCloseAfterQuiescence 一次性归还——否则 owner 的连接名额永不归零，
    // 托管链在硬停后无法落定（与 _DrainQuotaLocked 同一 Stop 安全思路）。
    void _DrainAcceptReservedLocked() noexcept {
        while (m_accept_reserved > 0) {
            --m_accept_reserved;
            if (m_accept_release) m_accept_release();
        }
    }
    // 正常终态出账一个预留（m_mtx held；幂等）。
    void _ConsumeAcceptReservedLocked() noexcept {
        if (m_accept_reserved > 0) --m_accept_reserved;
    }

    int m_fd{-1};
    bool m_closed{false};
    bool m_closing{false};
    int m_inflight{0};
    // §4.0.1.7：已 admit 未归还的 Runtime 在途名额（m_mtx 保护）。
    std::size_t m_quota_held{0};
    // §4.0.1.7：本 listener 已 m_accept_admit 未出账的连接容量预留数
    // （m_mtx 保护）。见 _DrainAcceptReservedLocked。
    std::size_t m_accept_reserved{0};
    mutable std::mutex m_mtx;
    std::atomic_bool m_close_waiting{false};
    std::shared_ptr<bbt::coroutine::CancellationSource> m_close_source{
        std::make_shared<bbt::coroutine::CancellationSource>()};
    std::shared_ptr<bbt::coroutine::CancellationSource> m_closed_source{
        std::make_shared<bbt::coroutine::CancellationSource>()};
    bbt::coroutine::CoObjectInfo m_info;
    std::shared_ptr<bbt::coroutine::sync::CoWaiter> m_waiter;
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
};

} // namespace bbt::infra::tcp
