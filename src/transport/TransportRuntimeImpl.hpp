#pragma once
// transport_detail::TransportRuntimeImpl：TransportRuntime 的唯一实现。
//
// P2：基础 transport 的资源托管与配额账本（容量名额、在途账本、受管对象
// 强持有与物理关闭收口、受管 DialTCP 等待段的取消/归还）集中在此，从
// http/NetworkRuntimeImpl 移出后不再有第二份实现。本头仅供 src/ 内部使用，
// 不安装、不进公开面。

#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <string_view>
#include <vector>

#include <bbt/infra/CoTCP.hpp>   // tcp::DialWaitPermit
#include <bbt/infra/CoUDP.hpp>
#include <bbt/infra/TransportRuntime.hpp>

#include "detail/IoSupport.hpp"

namespace bbt::infra {
namespace transport_detail {

class TransportRuntimeImpl : public TransportRuntime,
                            public std::enable_shared_from_this<TransportRuntimeImpl> {
public:
    TransportRuntimeImpl(
        NetworkLimits limits,
        bbt::coroutine::CoObjectInfo info,
        std::shared_ptr<bbt::coroutine::CompletionSignal> close_sig)
        : m_limits(limits),
          m_info(std::move(info)),
          m_close(std::move(close_sig)),
          m_inflight_quota(
              std::make_shared<bbt::infra::detail::InflightLedger>(
                  limits.max_inflight)) {}

    result<void> Start() override;
    result<std::shared_ptr<CoTCP>> DialTCP(
        TcpEndpoint endpoint, const CallOptions& options) override;
    result<std::shared_ptr<CoTCPListener>> ListenTCP(
        SocketAddress local, unsigned backlog) override;
    result<std::shared_ptr<CoUDP>> BindUDP(SocketAddress local) override;

    void RequestClose() noexcept override;
    bool IsClosed() const noexcept override { return m_close.IsClosed(); }
    CloseStatus WaitClosed(bbt::coroutine::Deadline          deadline,
                           bbt::coroutine::CancellationToken cancel) override {
        return m_close.WaitClosed(deadline, std::move(cancel),
                                  m_info.generation);
    }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return m_info;
    }

    // 测试接缝见公开头契约注释（Issue #32）。
    std::size_t InflightQuotaHeldForTest() const noexcept override {
        return m_inflight_quota->Held();
    }
    void SetDialWaitEntryGateForTest(std::function<void()> gate) noexcept override {
        m_dial_wait_entry_gate_for_test = std::move(gate);
    }

private:
    enum State : int { kCreated = 0, kRunning = 1, kClosingOrClosed = 2 };

    result<void> CheckUsableForFactory() const;
    // co-io-adapter/v1 §4.0.1.7 容量原子预留（m_transport_mtx）：已封口 →
    // Closed，超 max_connections → Overloaded；成功时名额已计入。
    result<void> ReserveConnectionSlot(std::string_view factory) noexcept;
    // 物理关闭落定回调（任意线程，经各受管对象 ClosedHook 触发）。child 是
    // 稳定身份：登记/移除/计数按同一身份一次性配对。
    void OnTransportClosed(const ICoCloseable* child) noexcept;
    // 已封口且受管 transport 全部物理关闭 → 落定 Closed 并通知组合方。
    void MaybeMarkClosed() noexcept;

    NetworkLimits                  m_limits;
    bbt::coroutine::CoObjectInfo   m_info;
    bbt::infra::detail::ManagedCloseState m_close;
    std::atomic<int>               m_state{kCreated};

    // 测试接缝钩子（Issue #32 F2）：受管 DialTCP 挂起落定后回调；见公开头
    // 契约注释。生产路径不安装。
    std::function<void()>          m_dial_wait_entry_gate_for_test;

    // 受管 transport 记账，由 m_transport_mtx 保护：
    //   m_tcp_children   已交付对象的强持有（托管引用；对象物理关闭后经
    //                    ClosedHook 按稳定身份移除，运行期间不积累历史对象）。
    //   m_transport_count 真实 socket 名额（listener/accepted/dialed TCP、UDP）。
    //   m_transport_sealed 封口后工厂拒绝；根因是「拒绝路径不建活跃连接」。
    //   m_dial_cancels / m_dial_permits 未完成受管 DialTCP 的取消源与归还
    //                    凭据：挂起协程被 Scheduler::Stop 硬销毁（不展开栈）
    //                    时栈上局部对象不析构，归还职责只能由本 owner 的堆
    //                    对象承担。两路径幂等互斥（Fail 的 released 原子位）。
    std::mutex                                     m_transport_mtx;
    std::vector<std::shared_ptr<ICoCloseable>>     m_tcp_children;
    std::size_t                                    m_transport_count{0};
    bool                                           m_transport_sealed{false};
    std::vector<std::shared_ptr<bbt::coroutine::CancellationSource>>
                                                   m_dial_cancels;
    std::vector<std::shared_ptr<tcp::DialWaitPermit>> m_dial_permits;

    // §4.0.1.7：本 owner 的在途账本（max_inflight）。覆盖受管 DialTCP 等待
    // 段、CoTCPListener::Accept 与 TCP/UDP 可等待数据操作；Try* 不占名额。
    // 经 shared_ptr 与各受管对象共享，挂起协程被强制 Stop（不展开栈）时由
    // 对象侧 RequestClose/析构归还剩余名额——见 detail::InflightLedger 注释。
    std::shared_ptr<bbt::infra::detail::InflightLedger> m_inflight_quota;
};

} // namespace transport_detail
} // namespace bbt::infra
