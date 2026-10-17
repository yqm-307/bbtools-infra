#pragma once

#include <memory>
#include <system_error>

#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/CoConnectionHandler.hpp>
#include <bbt/infra/net/CoTcpConnection.hpp>
#include <bbt/infra/net/ICoConnection.hpp>
#include <bbt/infra/net/Options.hpp>

namespace bbt::infra::net {

// 薄 TCP Client（首包最小形态）：一个 EventLoop owner + 一个 ConnectionBridge
// （即公共 Engine 单个 sub owner 的构成），不建第二套 runtime/owner 管理器、
// 无 listener/main accept，也不复用 ServerEngine（其启动序列无条件绑定 listener）。
//
// 执行域（不缓存 TLS，每次现查）：
// - 控制域：构造/析构/Start/Stop。Start/Stop 在受管协程内或本 Client owner 线程内
//   一律在任何副作用之前返回 operation_not_permitted。
// - 业务域：Dial 只在受管协程内合法，可挂起等待（挂起只让出 Processer）。
// - 析构无返回值，故不是可以「返回错误」的操作：调用者必须保证析构在控制线程执行、
//   且对本对象的 Dial 已返回（析构与成员调用并发不受支持）。在受管协程内或本 Client
//   owner 线程内析构同样不受支持，也不承诺在那里有界收口。
//   OnOpen/OnTraffic/OnClose 由桥的 dispatcher 在受管协程内派发，见
//   ICoConnectionHandler。
//
// 生命周期：Client 与每个被接纳的会话共同强持 handler；handler 活过其最后一个
// 回调，已交付的 shell（CoTcpConnection）与被接纳会话可跨 Client 销毁存活。禁止
// handler 反向持 Client。
//
// 契约边界（本包不承诺、不实现的）：
// - 不提供 Enroll/EnrollContext/DialContext（后续包范围），不发明 Engine::Connect。
// - Dial 返回 ≠ OnOpen 已执行：库对「Dial 返回」与「OnOpen 首次执行」不提供
//   顺序保证（OnOpen 可能在 Dial 返回前、中、后任意时刻开始）。
// - deadline 到期只结束本次等待，不回滚已投递的非阻塞 connect；此时结果可能
//   未知、fd/watcher 仍有 owner 域登记并做有界取消与晚到清理。
class Client final {
public:
    // 按值共同持有 handler；空指针在 Start 时以 invalid_argument 明确拒绝。
    explicit Client(std::shared_ptr<ICoConnectionHandler> handler);
    ~Client();
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    // 控制域；起 1 个 owner（EventLoop + 桥），无 listener/accept。
    // 启动者唯一：同一时刻只允许一位调用者进入启动序列，并发第二次 Start 一律
    // device_or_resource_busy；且 Stop 抢先进入终态后，启动中的那次不得复活 owner。
    // 失败回落（起过的 owner 全部回收、首错优先返回），Idle 可重试；运行/启动中
    // 重复调用 → device_or_resource_busy；Stop 之后 → operation_canceled（终态）。
    // 只接受并复用 tcp_no_delay / read·write_buffer_cap / max_inbound·outbound_bytes /
    // max_pending_events；其余未接线选项显式返回 operation_not_supported（不静默忽略）。
    // 单 owner：num_event_loop 默认（未指定 → 归一化为 1）与显式 1 被接受，归一化
    // 结果 >1（显式多值或 multicore 推导）返回 operation_not_supported。
    std::error_code Start(const Options& options = {});

    // 控制域；先取消在途 Dial 的 readiness 等待再收口（否则 EventLoop::Join 会被
    // 未决 async_wait 拖住）。顺序：SealService → owner 域取消全部在途 Dial →
    // StopServiceOwner → Join 前核 NetworkCloseReady → RequestStop/Join → DetachOwner。
    // 承诺范围：单次瞬态投递/执行/关闭故障由一次有界补偿推进（与 ServerEngine 停机
    // 收口同量级：首错保留，资源已收口也不改判为成功）；持续拒绝或 owner 长阻塞属承诺
    // 外，此时不进入无界 Join，保留登记簿与 owner 返回 operation_in_progress。
    // 幂等；未 Start 亦入终态（此后 Dial 拒绝）。返回真实错误，不报假成功；未收口返回
    // operation_in_progress 并可由再次 Stop 推进。
    std::error_code Stop();

    // 域：受管协程；可挂起。
    // 成功：out 非空、该连接已登记并将派发 OnOpen。
    // 失败：out 不变。deadline 到期返回 timed_out（只结束等待，不保证回滚已投递
    // connect，结果可能未知）；若完成已先赢则返回成功。
    // 非受管协程/owner 线程 → operation_not_permitted（副作用之前，无 fd）。
    // 未 Start → operation_not_permitted；Stopping/Stopped → operation_canceled。
    std::error_code Dial(const Address& peer, std::shared_ptr<CoTcpConnection>& out,
        CoDeadline deadline);

private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

} // namespace bbt::infra::net
