#pragma once
// NetworkRuntimeImpl：HTTP 切片运行时实现。持有 HttpIoEngine（coroutine
// 共享 executor 上的 strand 执行域，不拥有 io_context/线程）并强持有
// 全部受管子对象。
//
// 物理清理顺序（契约 §132 与「逻辑结果与物理清理分离」）：
//   RequestClose → 投递到 io 域逐个执行子对象 teardown → 等所有子对象
//   按 in-flight async 计数归零后 MarkClosed → 引擎 SealOnIoDomain
//   仅封死 TryPost，随后 runtime 自身 MarkClosed。
//   任何一个环节未完成前 WaitClosed 不返回 Closed——TimedOut/Cancelled
//   原样上报，infra 不伪造「清理完成」。

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

#include <bbt/infra/NetworkRuntime.hpp>

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
                       std::shared_ptr<bbt::coroutine::CompletionSignal> close_sig)
        : m_limits(limits),
          m_engine(std::move(engine)),
          m_info(std::move(info)),
          m_close(std::move(close_sig)) {}

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
    void OnChildClosed() noexcept;
    void OnTransportClosed(const ICoCloseable* child) noexcept;

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

    // Issue #37：HTTP 出站配额。与 transport m_transport_count 并列、
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
    //   m_children 强持有受管对象；m_sealed 由 teardown 置位后工厂拒绝；
    //   m_unclosed 计数未物理关闭的子对象；m_teardown/m_finalize_started
    //   与 OnChildClosed 的递减配对，防止丢最后一份关闭通知。
    std::mutex                                     m_lifecycle_mtx;
    std::vector<std::shared_ptr<IIoTeardown>>      m_children;
    std::size_t                                    m_unclosed{0};
    bool                                           m_sealed{false};
    bool                                           m_teardown{false};
    bool                                           m_finalize_started{false};

    // co-io-adapter/v1 §4.0.1.7 容量门禁：m_transport_count 记录 Runtime
    // 同时拥有的 transport socket（listener/accepted/dialed TCP；协议 Conn
    // 引用同一 socket 不重复计数）。m_transport_mtx 与 m_lifecycle_mtx 分离，
    // 避免 DialTCP 协程内路径与 HTTP 生命周期锁相互阻塞。
    std::mutex                                     m_transport_mtx;
    std::size_t                                    m_transport_count{0};
    bool                                           m_transport_sealed{false};
    // 已交付 TCP transport 的强持有（Runtime 托管引用；对象 Closed 后经
    // ClosedHook 回调释放）。由 m_transport_mtx 保护。
    std::vector<std::shared_ptr<ICoCloseable>>     m_tcp_children;

    // Issue #37：HTTP 出站配额计数，由 m_lifecycle_mtx 保护。
    // 与 m_transport_count 分离：transport 计量真实 socket 对象，
    // HTTP 出站这里计量「已接纳的出站请求/连接」这一逻辑名额，
    // 归还发生在 op 物理收口（UnregisterOp）而非协程栈析构。
    std::size_t                                    m_http_conn_count{0};
    std::size_t                                    m_http_inflight_count{0};
};

} // namespace http_detail
} // namespace bbt::infra
