#pragma once

#include <chrono>
#include <cstddef>
#include <string>
#include <string_view>
#include <system_error>

#include <bbt/infra/net/ICoObject.hpp>

namespace bbt::infra::net {

// 显式关闭模式，无默认值（0010 冻结取舍）：
// - Block：仅在受管协程内挂起等待物理关闭完成；不等待自身 OnClose。
// - Unblock：封口并可靠投递物理关闭命令；返回时已封口，不等物理完成。
enum class CloseMode { Block, Unblock };

// 受管协程业务操作的绝对期限（steady_clock）。到期只结束等待，不回滚
// 已投递命令；命令结果可能未知，不自动重试，晚到结果不写调用栈。
using CoDeadline = std::chrono::steady_clock::time_point;

// 公共连接操作契约（P3-c1）：真实 Co 业务面。派生本仓 ICoObject；
// 具体连接类型组合既有 CoObject 身份，避免菱形/新 id 机制。
//
// 域与门禁（每次现查，不缓存线程 TLS）：
// - 所有跨域业务命令（Wake/Write/Close）必须在受管协程内调用；副作用前
//   RequireCoroutine，非受管协程（普通线程/TLS-idle/owner 线程）一律
//   以 operation_not_permitted 拒绝且无副作用。
// - 事件内 Reader（InboundBuffered/Peek/Discard）还要求当前 connection
//   的活跃事件协程 id 匹配；仅 OnOpen/OnTraffic/OnClose 事件内合法。
// - NONSUSPENDING 标签仅在完整路径满足「不挂起、不阻塞」时标注；本接口
//   的跨域命令会挂起等待，不标注。
class ICoConnection : public ICoObject {
public:
    // 唤醒事件：请求向 handler 派发一次 OnTraffic(wake=true)。
    // 域：受管协程；不挂起（仅向 owner 投递有界命令）。
    // 错误：operation_not_permitted（非受管协程）；封口/关闭后
    // operation_canceled（明确拒绝，不静默丢弃）；owner 命令队列满
    // resource_unavailable_try_again；owner 已封口 operation_canceled。
    // 完成点：owner 命令被接纳即返回；与并发 Close 竞争时该事件可能被
    // 丢弃但计数可观测（wake_rejected_sealed/late_dropped）。
    virtual std::error_code Wake() = 0;

    // 写入：取得不可变字符串所有权，经 owner 有界待发队列接纳。
    // 域：受管协程；可挂起（等待 owner 接纳结果，deadline 内）。
    // 语义：成功=命令被接纳（写入待发队列），≠ 内核全发，≠ 对端收到。
    // 错误：operation_not_permitted；timed_out（deadline 到期，已投递
    // 命令结果未知，不自动重试，不晚写调用栈）；owner 队列满
    // no_buffer_space/resource_unavailable_try_again；非 Open 状态由
    // 物理连接拒绝（operation_not_supported 等）。
    // 完成点：owner 命令执行完成（接纳或拒绝）。
    virtual std::error_code Write(std::string data, CoDeadline deadline) = 0;

    // 关闭：CloseMode::{Block, Unblock} 显式无默认；共用
    // Open→Closing→Closed 首胜，物理收口恰好一次；晚写/晚 Wake/旧
    // Action 不能重开。
    // 域：受管协程；Block 可挂起（仅等物理完成，不等自身 OnClose；
    // OnClose 内重复 Block 直接观察已物理关闭，不自等）；Unblock 不
    // 挂起：封口（短锁）后可靠投递，返回时已封口且命令被接纳；投递
    // 失败回滚封口保持原状态，无假 Closing。
    // 错误：operation_not_permitted；Block 超时 timed_out（物理关闭
    // 可能仍在进行，结果未知）；Unblock 投递失败映射 owner 队列错误。
    // 已封口/已 Closed 后的 Close 为明确幂等观察（返回成功）。
    virtual std::error_code Close(CloseMode mode, std::error_code reason, CoDeadline deadline) = 0;

    // ---- 事件内 Reader（gnet 式最小子集）：只读当前事件的独立快照，
    // 不等网络。快照由内部 dispatcher 在 handler 进入前取得、保存于
    // 连接自有存储（不存 handler 栈指针），事件结束按已消费量归还，
    // 未消费尾部保序。Peek 借用视图在挂起期间有效，但下一次游标变更
    // 或事件返回即失效，不能跨事件使用。Wake 事件不借出快照：Reader
    // 观察为空（InboundBuffered()==0，Peek 空 view，Discard(0) 成功，
    // Discard(n>0) 返回 result_out_of_range）。完整 Read/Next/WriteTo
    // 表是后续包，本包不加。----
    //
    // 域：仅当前 connection 的活跃事件协程内；非事件协程调用
    // InboundBuffered/Peek 抛 std::system_error(operation_not_permitted)，
    // Discard 以错误码返回同一拒绝。不挂起、不阻塞事件协程（内部短锁
    // 在合法单事件位模式下无竞争）。
    virtual std::size_t InboundBuffered() const = 0;
    virtual std::string_view Peek() const = 0;
    // 消费（丢弃）n 字节；n 超过剩余量拒绝且不变（result_out_of_range）。
    virtual std::error_code Discard(std::size_t bytes) = 0;

protected:
    ~ICoConnection() = default;
};

} // namespace bbt::infra::net
