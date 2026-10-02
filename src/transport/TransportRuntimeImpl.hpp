#pragma once
// transport_detail::TransportRuntimeImpl：TransportRuntime 的唯一实现。
//
// P2：基础 transport 的资源托管与配额账本（容量名额、在途账本、受管对象
// 强持有与物理关闭收口、受管 DialTCP 等待段的关闭唤醒/归还）集中在此，从
// http/NetworkRuntimeImpl 移出后不再有第二份实现。本头仅供 src/ 内部使用，
// 不安装、不进公开面。

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <string_view>
#include <vector>

#include "detail/TransportWiring.hpp"   // 内部装配面（DialWaitOptions / TransportWiring）
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
        bbt::coroutine::CoObjectInfo info)
        : m_limits(limits),
          m_info(std::move(info)),
          m_inflight_quota(
              std::make_shared<bbt::infra::detail::InflightLedger>(
                  limits.max_inflight)),
          m_close_waiters(
              std::make_shared<bbt::infra::detail::CloseWaiters>()) {}

    result<void> Start() override;
    result<std::shared_ptr<CoTCP>> DialTCP(
        TcpEndpoint endpoint, const CallOptions& options) override;
    result<std::shared_ptr<CoTCPListener>> ListenTCP(
        SocketAddress local, unsigned backlog) override;
    result<std::shared_ptr<CoUDP>> BindUDP(SocketAddress local) override;

    // owner 主动同步关闭：幂等、任意线程可调用。先封口（拒绝新资源）并唤醒
    // owner 级挂起等待者（受管 DialTCP 的 connect 等待段），再逐对象 Close()。
    // 所有调用者均等待在途工厂和受管 transport 名额归零、真实 Closed 后返回；
    // 不以独立超时上限放弃收口。见 src/detail/TransportWiring.hpp。
    void Close() noexcept override;
    bool IsClosed() const noexcept override { return m_close.IsClosed(); }
    bbt::coroutine::CoObjectInfo GetObjectInfo() const override {
        return m_info;
    }

    // 测试探针：受管 transport 名额（m_transport_count）。收敛判据用它验证
    // 「对象级 closed_hook 与名额归还一一配对」；生产路径不使用。
    std::size_t TransportsHeldForTest() noexcept {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        return m_transport_count;
    }

    // 测试接缝（Issue #32）：公开面无这两个槽位；源码内与测试经
    // detail::TransportWiring 转到本实现（见 src/detail/TransportWiring.hpp）。
    std::size_t InflightQuotaHeldForTest() const noexcept {
        return m_inflight_quota->Held();
    }
    void SetDialWaitEntryGateForTest(std::function<void()> gate) noexcept {
        m_dial_wait_entry_gate_for_test = std::move(gate);
    }
    // R1 探针：在途工厂装配计数（m_transport_mtx 保护）。用于确定性断言
    // 「owner Close 已封口且等到在途工厂归零」。生产路径不使用。
    std::size_t PendingFactoriesForTest() noexcept {
        std::lock_guard<std::mutex> lk(m_transport_mtx);
        return m_pending_factories;
    }
    // R1 探针：owner 级关闭唤醒登记是否已封口。用于受控交错回归在「Close 已
    // 封口、尚未返回」处确定性放行被暂停的工厂线程。生产路径不使用。
    bool CloseSealedForTest() noexcept {
        return m_close_waiters->Closed();
    }

private:
    enum State : int { kCreated = 0, kRunning = 1, kClosingOrClosed = 2 };

    result<void> CheckUsableForFactory() const;
    // co-io-adapter/v1 §4.0.1.7 容量原子预留（m_transport_mtx）：已封口 →
    // Closed，超 max_connections → Overloaded；成功时名额已计入，并登记
    // 「在途工厂」（R1：资源装配进行中）。
    result<void> ReserveConnectionSlot(std::string_view factory) noexcept;
    // R1：在途工厂账本注销 + 终态再判定（工厂每一条返回路径恰好调用一次）。
    void EndFactory() noexcept;
    // 物理关闭落定回调（任意线程，经各受管对象 ClosedHook 触发）。child 是
    // 稳定身份：登记/移除/计数按同一身份一次性配对。
    void OnTransportClosed(const ICoCloseable* child) noexcept;
    // 已封口、受管 transport 名额归零、且无在途工厂 → 落定 Closed 并通知组合
    // 方。谓词更新（m_close.MarkClosed）与通知在同一临界区（m_transport_mtx）
    // 内完成，且等待者在本锁下的谓词读取与入队配对 ⇒ 不丢唤醒、无独立短上限。
    void MaybeMarkClosed() noexcept;

    NetworkLimits                  m_limits;
    bbt::coroutine::CoObjectInfo   m_info;
    bbt::infra::detail::ManagedCloseState m_close;
    std::atomic<int>               m_state{kCreated};
    std::condition_variable        m_close_cv;   // 并发 Close 的落定等待

    // 测试接缝钩子（Issue #32 F2）：受管 DialTCP 挂起落定后回调；见
    // src/detail/TransportWiring.hpp。生产路径不安装。
    std::function<void()>          m_dial_wait_entry_gate_for_test;

    // 受管 transport 记账，由 m_transport_mtx 保护：
    //   m_tcp_children   已交付对象的强持有（托管引用；对象物理关闭后经
    //                    ClosedHook 按稳定身份移除，运行期间不积累历史对象）。
    //   m_transport_count 真实 socket 名额（listener/accepted/dialed TCP、UDP）。
    //   m_transport_sealed 封口后工厂拒绝；根因是「拒绝路径不建活跃连接」。
    //   m_pending_factories 在途工厂（R1）：从容量预留到 child 交接/自行关闭之间
    //                    的装配过程。Close 封口后等待它归零才收口，保证不遗漏
    //                    「已预留名额、fd 尚未交接」的在途资源。
    // 未完成受管 DialTCP 的等待段名额归还职责在协程自身返回路径（运行时不再
    // 硬销毁挂起协程），owner->Close() 经 close_waiters 唤醒 connect 等待段使
    // 其尽快返回；DNS 等待段受调用方 deadline 界定（见 TransportWiring.hpp）。
    std::mutex                                     m_transport_mtx;
    std::vector<std::shared_ptr<ICoCloseable>>     m_tcp_children;
    std::size_t                                    m_transport_count{0};
    std::size_t                                    m_pending_factories{0};   // 在途工厂
    bool                                           m_transport_sealed{false};
    bool                                           m_closed_published{false}; // 终态只发布一次
    std::condition_variable                        m_pending_cv;   // 在途工厂排空等待

    // §4.0.1.7：本 owner 的在途账本（max_inflight）。覆盖受管 DialTCP 等待
    // 段、CoTCPListener::Accept 与 TCP/UDP 可等待数据操作；Try* 不占名额。
    // 经 shared_ptr 与各受管对象共享，关闭期由对象侧归还剩余名额——见
    // detail::InflightLedger 注释。
    std::shared_ptr<bbt::infra::detail::InflightLedger> m_inflight_quota;

    // owner 级关闭唤醒登记：受管 DialTCP 的 connect 等待段经
    // DialWaitOptions::close_waiters 挂在这里，Close() 一次性封口并唤醒。
    std::shared_ptr<bbt::infra::detail::CloseWaiters>   m_close_waiters;
};

} // namespace transport_detail
} // namespace bbt::infra
