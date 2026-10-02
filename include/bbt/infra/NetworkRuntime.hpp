#pragma once
// co-network/v1 N1b-1：网络 Runtime 装配面。
// Runtime 是资源拥有者；工厂返回的 shared_ptr 是受托管对象引用。
// RPC 工厂在 N2 就绪前不导出（不导出能假成功的工厂）。

#include <memory>

#include <bbt/infra/HttpClient.hpp>
#include <bbt/infra/HttpServer.hpp>
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

// co-io-adapter/v1 §4 冻结名是 bbt::infra::CoTCP/CoTCPListener；首版实现
// 位于 bbt::infra::tcp（与 CoUDP 同规则）。经 using 声明把冻结名接上，
// Runtime 工厂签名按契约名书写，类型与实现一致。
using tcp::CoTCP;
using tcp::CoTCPListener;
using udp::CoUDP;

// P2 归属说明：本入口是协议 owner + 兼容装配面。基础 TCP/UDP transport 的
// 资源托管与配额 owner 是 TransportRuntime（include/bbt/infra/
// TransportRuntime.hpp，target bbt_infra_transport）：DialTCP/ListenTCP/
// BindUDP 在这里只做转发，容量名额与在途账本不在本类内实现，也不存在第二份。
// 受管 TCP/UDP 消费者可以只链接 bbt::infra_transport，不依赖 HTTP。
//
// 正常生命周期（进程寿命运行时）：Scheduler::Start → NetworkRuntime::Create/
// Start → 使用 → server.StopAccepting → Runtime.Close()（同步；返回即
// 物理释放：子对象逐个收口、transport 收口、io 域封口）。没有 Stop/
// 重启，也没有 RequestClose/WaitClosed。
class NetworkRuntime : public ICoNetwork, public ICoCloseable {
public:
    // Create/Start/工厂在启动控制线程使用，不挂起协程。
    // Create 要求 Scheduler 已初始化（对象身份需要运行时已初始化），
    // 否则返回 Error(RuntimeUnavailable)。
    static result<std::shared_ptr<NetworkRuntime>> Create(NetworkLimits limits);

    // 同一 runtime 只成功启动一次；关闭后不重开。
    virtual result<void> Start() = 0;

    virtual result<std::shared_ptr<HttpClient>> CreateHttpClient() = 0;
    virtual result<std::shared_ptr<HttpServer>> ListenHttp(
        ListenAddress address, HttpHandler handler) = 0;

    // co-io-adapter/v1 §4.0.1：受管 TCP 工厂。工厂返回的 shared_ptr 是
    // 受托管引用；Runtime 强持有交付对象直至 Closed，超容量返回 Overloaded。
    // DialTCP 只能在受管 coroutine 内调用（可因 DNS/connect 挂起）；
    // ListenTCP 是控制线程配置操作，要求 Runtime 已 Start，backlog 1..INT_MAX。
    virtual result<std::shared_ptr<CoTCP>> DialTCP(
        TcpEndpoint endpoint, const CallOptions& options) = 0;
    virtual result<std::shared_ptr<CoTCPListener>> ListenTCP(
        SocketAddress local, unsigned backlog) = 0;
    virtual result<std::shared_ptr<CoUDP>> BindUDP(SocketAddress local) = 0;
};

} // namespace bbt::infra
