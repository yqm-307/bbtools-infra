#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <system_error>
#include <vector>

#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/EventHandler.hpp>
#include <bbt/infra/net/Options.hpp>

namespace bbt::infra::net {

class ServerEngine;

// 公共 Engine（首包局部交付）：真实 TCP 单 main listener + num_event_loop 个 sub owner。
// 详细执行域/强停顺序/测试判据见 docs/architecture/gnet-network-execution.md 的
// 「首包公共 Engine」小节与 docs/plans/gnet-network-migration.md 对应小节。
//
// 已交付的真实行为：
// - Run：native 控制线程生命周期入口（不是 CoObject 业务方法）。起 main owner 与
//   N 个 sub owner（各自 EventLoop 线程 + 私有 bridge），绑定 address（port 0 由内核
//   分配），发布实际绑定快照并进入 Starting，然后在受管协程内跑 EventHandler::OnBoot
//   并等待完成（等待在锁外进行，不持任何 mutex 跨 OnBoot），完成后挂上 accept 就绪等待
//   并进入 Running；不占调用方进程主循环。
// - accept：LoadBalancer 选 sub owner（三算法复用既有实现；LeastConnections 用每
//   sub 真实 session 计数）→ 唯一 fd 移交该 sub owner → 既有 bridge 接纳 →
//   受管 OnOpen/OnTraffic(含 Wake)/OnClose。
// - ticker（Options::ticker）：Run 进入 Running 且 accept 就绪等待挂好后立即在受管协程
//   投递第一次 OnTick；返回 delay 决定下一次投递，<= 0 停止 ticker（Engine 继续服务，端口
//   仍接纳/回显）；Stop/回落取消 timer 并拒绝重挂，在途 tick 自然完成（不等 handler）。
//   单 ticker 串行不重入；具体语义见 EventHandler::OnTick 注释。
// - Stop：native 强停，顺序为 撤 listener/封接纳 → 清在途 handoff → 每 sub 物理全闭
//   → RequestStop/Join → owner context 仍活时 DetachOwner → owner/context 销毁。
//   不等 handler/OnClose/drain；不 terminate、不 detach 线程、不重开 runtime 停机。
//   挂起中的 handler 之后继续收尾：晚写/晚 Wake 明确拒绝，Close 为幂等观察。
//
// 域门禁（每次现查，不缓存 TLS）：Run/Stop 只能在非受管协程、且非本 Engine 的
// owner 线程上调用；受管协程内或本 Engine owner 线程内一律在**任何副作用之前**
// 返回 operation_not_permitted。Stop 以 error_code 返回结果，不做 void noexcept 吞错。
// 业务连接操作（ICoConnection 的 Wake/Write/Close 与事件内 Reader）仍只允许受管协程；
// 本接口不构成普通控制线程连接 Block 的新许可。
//
// 幂等/终态/并发：运行或启动中重复 Run → device_or_resource_busy（并发 Run 只有一个
// 可进入）；Stop 之后 Run → operation_canceled（终态），Stop 首胜不会被后续 Run 覆盖回
// Running；Stop 未启动时也进入同一终态（随后 Run → operation_canceled），已停止时幂等成功。
// Stop 在 Starting（OnBoot 未完成）也真实收口网络并返回，不等业务回调完成；已经开始的
// OnBoot 不会被强杀，只按值持 handler 自然完成，10s Boot 等待语义不变。
//
// 当前支持的选项（其余显式拒绝，不静默忽略）：
//   lb、num_event_loop/multicore、reuse_addr、tcp_no_delay、read/write_buffer_cap、
//   max_inbound/outbound_bytes、max_pending_events、ticker（engine 全局单 ticker，语义见
//   EventHandler::OnTick）。
// 未接线请求（reuse_port、keepalive、socket recv/send buffer、edge_triggered_io、
// multicast_interface_index、bind_to_device、lock_os_thread、log_path/logger）
// 返回 operation_not_supported。
class Engine final {
public:
    // 所有权（2026-10-16 用户授权修订）：构造是唯一入口，按值接收 handler 的
    // shared_ptr；Engine 与 ServerEngine、适配器以及挂起中的业务回调**共同
    // 持有**该对象，不留裸借用，也不用 no-op deleter 制造假拥有。不是线程安全声明：
    // handler 自身仍须按契约实现。
    // - 允许传空 shared_ptr 构造（延后注入/负例）；Run 在任何线程/socket/受管任务
    //   副作用之前以 invalid_argument 明确拒绝空拥有关系，不在异步入口留裸引用；
    //   未 Run 时 Stop/LocalAddresses/CountConnections 按未运行语义返回，不解引用。
    // - 无库内强环：业务回调只持 handler 与其连接句柄，不反向持 Engine/ServerEngine；
    //   Engine 也不被挂起中的 OnBoot 延寿（Boot 任务只按值持 handler 与窄结果句柄，
    //   不保活网络域）。用户自建的反向持有是否成环不在本库保证范围内。
    // - handler 的析构线程是最后一个拥有者所在线程，可能正是受管协程；不保证在控制
    //   线程析构，也不要求 handler 析构阻塞 worker。handler 析构不回调 Engine::Stop、
    //   不引入 reaper。
    // - handler 必须活过其最后一个回调返回（由共同持有保证），而不是必须活过 Engine
    //   的整个使用期：Engine 强停/销毁后挂起回调仍安全收尾。
    explicit Engine(std::shared_ptr<EventHandler> handler);
    ~Engine();

    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    // 失败回落：nullptr 则失败，错误信息在 error/ok 中返回。
    // 非阻塞启动（native 控制线程入口）；启动中/运行中重复调用 → busy，终态 → canceled。
    // 接受集只有 IPv4（当前接纳路径只服务 IPv4 stream）：Unix/IPv6 等**合法**但未支持的
    // 家族在**任何** owner 线程/listener socket/OnBoot 登记之前返回 operation_not_supported，
    // 不发布 bound 快照、不进入 Starting，也不进入 Stopped 终态（可换 IPv4 地址重试）；
    // 空/无效 Address 仍是 invalid_argument，域门禁与 busy/canceled 的优先级不变。
    std::error_code Run(const Address& address, const Options& options = {});

    // native 强停。未启动/已停止时幂等成功（返回空）；未启动调用同样进入终态。
    std::error_code Stop();

    // 实际绑定地址的稳定快照（port 0 请求 → 内核分配的真实端口）。
    // 已绑定（Starting，含 OnBoot 执行期间）与 Running 可读；未运行/已停止 →
    // operation_not_permitted，且不改写 out。
    std::error_code LocalAddresses(std::vector<Address>& out) const;

    // 当前活跃连接数（接纳成功、尚未 OnClose；跨 sub owner 汇总）。Starting 为 0，
    // Running 为本代真实计数，强停后为 0。观测只取短锁，不在 owner 线程调用。
    std::size_t CountConnections() const;

    // 注意：析构必须在合法控制域（非受管协程、非本 Engine owner 线程）执行；
    // 若仍在运行/启动中会按 Stop 的同一顺序强停收口。

private:
    // 发布稳定 server 句柄（锁内只做发布/读取，不在锁内调用 server 的任何方法）。
    std::error_code EnsureServer(std::shared_ptr<ServerEngine>& out);
    std::shared_ptr<ServerEngine> Snapshot() const;

    std::shared_ptr<EventHandler> handler_;
    // facade 短锁：只保护 server_ 句柄的发布与读取，绝不跨 server->Run/Stop
    //（否则并发的 Stop/观测会被 native Run 的 Boot 等待挡住）。
    mutable std::mutex mutex_;
    std::shared_ptr<ServerEngine> server_;
};

} // namespace bbt::infra::net
