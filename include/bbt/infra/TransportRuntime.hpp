#pragma once
// P2：基础 transport 的资源托管与配额 owner。
//
// TransportRuntime 只托管基础 TCP/UDP transport —— 容量名额
// （max_connections）、在途账本（max_inflight）、受管对象的强持有与物理
// 关闭收口，以及受管 DialTCP 等待段的取消/归还。它不包含任何协议工厂
// （HTTP/Redis/Mongo），其实现目标 bbt_infra_transport 也不链接 HTTP：
// 受管 TCP/UDP 消费者不再需要 HTTP 才能构建。
//
// 形态说明（不是新的万能传输抽象）：本类形制与既有 `NetworkRuntime` +
// `src/http/NetworkRuntimeImpl` 一致——公开面只暴露能力与资源语义，唯一
// 实现按 owner 职责放在 src/transport/。协议 owner（当前的 NetworkRuntimeImpl）
// 组合一个 TransportRuntime 并把 transport 落定并入自身 finalize 门控，
// 不复制账本、不重复关闭实现。
//
// 资源归属（一个名额一个账本，同一事件只归还一次）：
//   - max_connections：真实 socket 对象（listener/accepted/dialed TCP、UDP
//     各算一个；协议侧连接对象引用同一 socket 不重复计数）。在「资源发起
//     之前」原子预留，物理关闭落定后经 ClosedHook 归还——逻辑超时不提前
//     归还容量。
//   - max_inflight：可等待 transport op 的在途预算（受管 DialTCP 等待段、
//     CoTCPListener::Accept、TCP ReadSome/WriteSome/WriteAll、UDP
//     Receive/Send）。Try* 不经账本。上层协议连接/请求预算由协议 owner
//     另行管，两者不共用同一计数。
// 一个 socket 只有一个物理关闭责任方：本 owner 只发起 RequestClose，
// 关闭落定以对象侧 ClosedHook 为唯一信号。

#include <cstddef>
#include <functional>
#include <memory>

#include <bbt/infra/ICoCloseable.hpp>
#include <bbt/infra/ICoNetwork.hpp>
#include <bbt/infra/NetworkTypes.hpp>
#include <bbt/infra/Result.hpp>

namespace bbt::infra {

namespace tcp {
class CoTCP;
class CoTCPListener;
} // namespace tcp
namespace udp {
class CoUDP;
} // namespace udp

// 按 co-io-adapter/v1 §4 冻结名书写工厂签名，类型与 tcp::/udp:: 实现一致。
using tcp::CoTCP;
using tcp::CoTCPListener;
using udp::CoUDP;

class TransportRuntime : public ICoNetwork, public ICoCloseable {
public:
    // 只校验装配参数；网络资源在 Start 才实际占用。要求 Scheduler 已启动
    // （对象身份与完成信号需要运行时代际），否则 Error(RuntimeUnavailable)。
    static result<std::shared_ptr<TransportRuntime>> Create(NetworkLimits limits);

    // 同一 owner 只成功启动一次；关闭后不重开。要求 Scheduler 处于与
    // Create 相同的运行时代际。
    virtual result<void> Start() = 0;

    // 受管内建工厂。工厂返回的 shared_ptr 是受托管引用：owner 强持有交付
    // 对象直至物理关闭，超 max_connections 返回 Overloaded；已封口返回
    // Closed。DialTCP 只能在受管 coroutine 内调用（可因 DNS/connect
    // 挂起），ListenTCP/BindUDP 是控制线程配置操作。
    virtual result<std::shared_ptr<CoTCP>> DialTCP(
        TcpEndpoint endpoint, const CallOptions& options) = 0;
    virtual result<std::shared_ptr<CoTCPListener>> ListenTCP(
        SocketAddress local, unsigned backlog) = 0;
    virtual result<std::shared_ptr<CoUDP>> BindUDP(SocketAddress local) = 0;

    // P2 owner 组合接缝：本 owner 的受管 transport 全部物理关闭落定
    // （IsClosed 观察点）后回调一次。协议 owner 用它把 transport 落定并入
    // 自身 finalize 门控；发布前一次性安装，生产路径至多一个装配者。
    // 约定：noexcept、不取本 owner 的锁、不阻塞。
    void SetClosedHook(std::function<void()> hook) noexcept {
        m_closed_hook = std::move(hook);
    }

    // 测试接缝（Issue #32）：直读共享在途账本当前名额数，用于跨对象/跨 op
    // 的确定性断言。不替代公开语义——生产路径不使用。
    virtual std::size_t InflightQuotaHeldForTest() const noexcept = 0;

    // 测试接缝（Issue #32 F2）：非空时，受管 DialTCP 的 DNS/connect 等待段
    // 在协程真正挂起（await event 注册进 parked 表）后回调一次，用于确定性
    // 观察「dial 已进入等待」。生产路径不安装，空钩子零开销。
    // 约定：noexcept、不取锁不阻塞、不得回调本对象。
    virtual void SetDialWaitEntryGateForTest(
        std::function<void()> gate) noexcept = 0;

protected:
    TransportRuntime() = default;

    void NotifyClosed() noexcept {
        auto hook = m_closed_hook;   // 发布后只读；拷贝后调用，避免重入歧义
        if (hook)
            hook();
    }

private:
    std::function<void()> m_closed_hook;
};

} // namespace bbt::infra
