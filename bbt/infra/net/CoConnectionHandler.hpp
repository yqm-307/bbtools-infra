#pragma once

#include <system_error>

#include <bbt/infra/net/ICoConnection.hpp>

namespace bbt::infra::net {

// 业务 handler 契约（P3-c1）：OnOpen → OnTraffic(含 Wake) → OnClose
// 单连接串行；三个回调全部运行在受管协程（bbtools-coroutine Processer），
// 不在 I/O owner 线程执行。同连接业务事件不重入，活跃事件峰值恒为 1；
// 挂起期间 owner、其他连接与其他业务协程继续推进。
//
// 生命周期：连接被内部工厂接纳后先派发 OnOpen；数据就绪/Wake 合并派发
// OnTraffic；物理关闭完成后（撤销登记、最后内核访问与唯一 fd 关闭之后）
// 串行派发 OnClose。OnOpen/OnTraffic 中抛出异常会被观测计数并触发封口
// + 物理关闭，OnClose 仍串行执行；OnClose 中的异常同样计数，不改变已
// 完成的物理收口。
//
// handler 实现 IQ：不得在回调内长期阻塞执行线程；需要等待时使用受管
// 协程挂起点（如 CoWaiter），让出 Processer。
class ICoConnectionHandler {
public:
    // 连接建立后派发的首个事件；受管协程内执行。
    virtual void OnOpen(ICoConnection& connection) = 0;

    // 数据就绪或 Wake 事件。wake=true 表示该事件由 Wake 合并/触发；
    // Wake-only 事件无新数据（Reader 观察为空）。事件内可通过
    // ICoConnection 的 Reader 消费当前独立快照；未消费尾部保序保留。
    virtual void OnTraffic(ICoConnection& connection, bool wake) = 0;

    // 物理关闭完成后的终态事件；reason 为关闭原因。此后不再有任何
    // 本连接业务事件。
    virtual void OnClose(ICoConnection& connection, const std::error_code& reason) = 0;

protected:
    ~ICoConnectionHandler() = default;
};

} // namespace bbt::infra::net
