#pragma once
// NetworkRuntimeImpl：HTTP 切片运行时实现。持有 HttpIoEngine（coroutine
// 共享 executor 上的 strand 执行域，不拥有 io_context/线程）并强持有
// 仍需托管的子对象（活跃/关闭中）；子对象物理关闭后经 ClosedHook 按
// 稳定身份反登记，运行期间不积累历史对象（Issue #38）。
//
// P2：基础 transport 的资源托管与配额 owner 已移出本切片，改为组合一个
// TransportRuntime（见 include/bbt/infra/TransportRuntime.hpp 与
// src/transport/）。本运行时只保留 HTTP 协议 owner 职责——HTTP 请求/
// 连接预算、handler drain、io 域封口——不再持有第二份 transport 账本、
// 也不重复 transport 的关闭实现。
//
// 物理清理顺序（契约 §132 与「逻辑结果与物理清理分离」）：
//   RequestClose → 投递到 io 域逐个执行子对象 teardown → 等所有子对象
//   按 in-flight async 计数归零后 MarkClosed → 引擎 SealOnIoDomain
//   仅封死 TryPost，随后 runtime 自身 MarkClosed。
//   任何一个环节未完成前 WaitClosed 不返回 Closed——TimedOut/Cancelled
//   原样上报，infra 不伪造「清理完成」。

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include <bbt/infra/NetworkRuntime.hpp>
// P2：TransportRuntime 是基础 transport 的资源/配额 owner（bbt::infra_transport
// target，不链接 HTTP）。本运行时组合它，而不是自己再实现一份 transport 记账。
#include <bbt/infra/TransportRuntime.hpp>

#include "detail/IoSupport.hpp"
#include "http/HttpDetail.hpp"
#include "http/HttpIoEngine.hpp"

namespace bbt::infra {

class CoTCP;
class CoTCPListener;

namespace http_detail {

class NetworkRuntimeImpl : public NetworkRuntime,
                           public std::enable_shared_from_this<NetworkRuntimeImpl> {
public:
    NetworkRuntimeImpl(NetworkLimits                  limits,
                       std::shared_ptr<HttpIoEngine>  engine,
                       bbt::coroutine::CoObjectInfo   info,
                       std::shared_ptr<bbt::coroutine::CompletionSignal> close_sig,
                       std::shared_ptr<TransportRuntime> transport)
        : m_limits(limits),
          m_engine(std::move(engine)),
          m_info(std::move(info)),
          m_close(std::move(close_sig)),
          m_transport(std::move(transport)) {}

    result<void> Start() override;
    result<std::shared_ptr<HttpClient>> CreateHttpClient() override;
    result<std::shared_ptr<HttpServer>> ListenHttp(ListenAddress address,
                                                   HttpHandler handler) override;
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

    // 子对象物理清理落定回调（任意线程，经各子对象 ClosedHook 触发）。
    // child 是稳定身份：登记/移除/计数按同一身份一次性配对。
    void OnChildClosed(const IIoTeardown* child) noexcept;

    // P2 装配接缝：把 transport owner 的「受管 transport 全部物理关闭」落定
    // 并入本 runtime 的 finalize 门控（唯一通知来源，不轮询）。Create 构造
    // 出对象后调用一次。
    void AdoptTransportOwner();

    // 测试接缝（Issue #38 竞态证据）：工厂在 CheckAndAdoptLocked 复检通过、
    // 登记提交之前调用此钩子（持 m_lifecycle_mtx）。测试用它把 factory
    // 调用停在「复检已过、登记未提交」的临界段内，再在同一线程外发起
    // RequestClose——从而证明登记提交与 RequestClose 的真实调用窗口
    // 重叠（不靠 sleep/时序推断）。生产路径不安装此钩子；为空时零开销。
    // 钩子约定：必须 noexcept，不得回调本对象/再次取本锁/阻塞在锁内
    // 等待由持锁线程释放的资源以外的事件。
    void SetAdoptCommitGateForTest(std::function<void()> gate) noexcept {
        m_adopt_commit_gate_for_test = std::move(gate);
    }

    // 测试接缝（Issue #32）：直读在途账本当前名额数。账本唯一真源在
    // transport owner 侧，这里只转发——本切片不保留第二份计数。
    std::size_t InflightQuotaHeldForTest() const noexcept {
        return m_transport->InflightQuotaHeldForTest();
    }

    // 测试接缝（Issue #32 F2）：受管 DialTCP 等待段的挂起落定钩子。
    // 受管 dial 的等待段现在归 transport owner，这里只转发（同一份实现）。
    // 约定：noexcept、不取锁不阻塞、不得回调本对象。
    void SetDialWaitEntryGateForTest(std::function<void()> gate) noexcept {
        m_transport->SetDialWaitEntryGateForTest(std::move(gate));
    }

private:
    enum State : int { kCreated = 0, kRunning = 1, kClosingOrClosed = 2 };

    // io 域：全部子对象 Closed 后封口引擎并落定 runtime Closed。
    void FinalizeEngine() noexcept;
    void MaybeFinalize() noexcept;

    // io 域：封口子对象注册 → 逐个 teardown → 全部物理关闭后封口引擎。
    void TeardownOnIoDomain() noexcept;
    // TryPost 失败：逻辑封口 + 子对象 off-domain 收口，不提前 MarkClosed。
    void TeardownOffDomain() noexcept;
    result<void> CheckUsableForFactory() const;
    result<std::shared_ptr<bbt::coroutine::CompletionSignal>>
        NewCloseSignal() const;
    // 工厂在锁内完成「未关闭」复检 + 登记 + 计数，杜绝与 teardown 竞态
    // 产生的孤儿子对象（登记了的必被 teardown 收口）。
    result<void> CheckAndAdoptLocked(
        const std::shared_ptr<IIoTeardown>& child);

    // Issue #37：HTTP 出站配额。与 transport owner 的 socket 容量账本并列、
    // 由 m_lifecycle_mtx 保护；作用域是整个 Runtime（同一 runtime 下
    // 所有 HttpClient 共享同一预算），不是单个 client——多 client
    // 不能各自绕过 owner 总量。
    //   m_http_conn_count    在途/已建立出站连接名额（max_connections）
    //   m_http_inflight_count 已接纳未物理收口的请求名额（max_inflight）
    // 两者都在「资源发起之前」原子预留，不足即 Overloaded；名额由
    // ClientOp 持有，在 UnregisterOp（finished 且 in-flight 归零 =
    // 后端不再触碰 op）后才归还，所有结束路径只归还一次。
    result<void> TryAdmitHttpRequest() noexcept;
    void ReleaseHttpRequest() noexcept;

    NetworkLimits                  m_limits;
    std::shared_ptr<HttpIoEngine>  m_engine;
    bbt::coroutine::CoObjectInfo   m_info;
    ManagedCloseState              m_close;
    std::atomic<int>               m_state{kCreated};

    // 生命周期记账（m_lifecycle_mtx 保护）：
    //   m_children 强持有仍需托管的子对象（活跃/关闭中）；子对象物理
    //   关闭落定后经 ClosedHook 按稳定身份一次性移除，运行期间不积累
    //   历史对象（Issue #38）。m_sealed 由 teardown 置位后工厂拒绝；
    //   m_unclosed 计数未物理关闭的子对象；m_teardown/m_finalize_started
    //   与 OnChildClosed 的递减配对，防止丢最后一份关闭通知。
    std::mutex                                     m_lifecycle_mtx;
    std::vector<std::shared_ptr<IIoTeardown>>      m_children;
    std::size_t                                    m_unclosed{0};
    bool                                           m_sealed{false};
    bool                                           m_teardown{false};
    bool                                           m_finalize_started{false};

    // 测试接缝钩子：仅在 CheckAndAdoptLocked 复检通过、登记提交前在持锁
    // 状态下调用；生产路径不安装。见 SetAdoptCommitGateForTest 契约注释。
    std::function<void()>                          m_adopt_commit_gate_for_test;

    // Issue #37：HTTP 出站配额计数，由 m_lifecycle_mtx 保护。
    // 与 transport owner 的 socket 容量账本分离（后者由 TransportRuntime
    // 独占实现）：transport 计量真实 socket 对象，
    // HTTP 出站这里计量「已接纳的出站请求/连接」这一逻辑名额，
    // 归还发生在 op 物理收口（UnregisterOp）而非协程栈析构。
    std::size_t                                    m_http_conn_count{0};
    std::size_t                                    m_http_inflight_count{0};

    // P2：基础 transport 的资源托管与配额 owner（容量名额、在途账本、受管
    // 对象强持有、物理关闭收口、受管 DialTCP 等待段的取消/归还）都在这里，
    // 本切片只组合引用，不再复制账本或关闭实现。
    std::shared_ptr<TransportRuntime>              m_transport;
};

} // namespace http_detail
} // namespace bbt::infra
