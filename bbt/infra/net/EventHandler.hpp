#pragma once

#include <chrono>
#include <system_error>

#include <bbt/infra/net/Action.hpp>
#include <bbt/infra/net/ICoConnection.hpp>

namespace bbt::infra::net {

// 公共业务事件处理接口（首包）：本类自身即默认空实现，不是别名/转发壳。
// 派生覆盖需要的回调即可，未覆盖的回调是真实 no-op。
//
// 执行域：OnBoot/OnTick/OnOpen/OnTraffic/OnClose 全部运行在受管协程（bbtools-coroutine
// Processer），不在 owner/网络线程执行；同连接事件串行，活跃事件峰值 1。
// 实现内不得长期阻塞执行线程；需要等待时使用受管协程挂起点让出 Processer。
//
// 生命周期与错误可见性：
// - OnBoot 在 Run 的启动阶段执行，返回 false 或抛出即启动失败（Run 返回错误、
//   已起线程/socket 全回收、不开放业务接纳）。挂起会让 Run 等待到内部截止期限，
//   到期以 timed_out 失败并回落，不留下半启动状态。
// - OnOpen/OnTraffic 返回 Action；返回 Action::Close 时该连接被封口并物理关闭
//   （关闭原因为空 error_code = 干净关闭），OnClose 仍按契约串行送达。
// - OnOpen/OnTraffic/OnClose 抛出异常会被观测计数并触发既有封口+物理关闭语义，
//   不改变已经完成的物理收口。
// - OnClose 是终态通知，之后不再有本连接业务事件。
//
// 所有权（2026-10-16 用户授权）：Engine 唯一入口按值接收本对象的 shared_ptr，
// 由 Engine/ServerEngine/适配器/挂起中的回调共同持有；本对象不反向持有 Engine。
// 最后一个引用可能在受管协程线程释放（挂起回调返回时）：析构不得阻塞 worker/owner
// 线程，也不得回调 Engine::Stop。
//
// OnTick 见下方签名（仅在 Options::ticker 启用时投递，具体语义写在该处注释）。
// OnShutdownRequest 见下方签名（Action::Shutdown 的向上请求上报口，非「已关闭」通知）。
// 首包**不声明**「已关闭生命周期通知」（0010 目标里的 OnShutdown 名字保留给它，本包未交付、
// 也不造 stub）；未经实现的能力不出现在公共契约里。
class EventHandler {
public:
    // 析构必须可经基类指针删除（public virtual，非 protected）。
    virtual ~EventHandler() = default;

    // 启动阶段（受管协程）：默认成功。false/抛出 = 启动失败。
    virtual bool OnBoot() { return true; }

    // 连接建立后的首个事件（受管协程）。
    virtual Action OnOpen(ICoConnection& connection) {
        (void)connection;
        return Action::None;
    }

    // 数据就绪或 Wake 事件；wake=true 表示由 Wake 合并/触发（事件内 Reader 为空）。
    virtual Action OnTraffic(ICoConnection& connection, bool wake) {
        (void)connection;
        (void)wake;
        return Action::None;
    }

    // 物理关闭完成后的终态事件；reason 为关闭原因。
    virtual void OnClose(ICoConnection& connection, const std::error_code& reason) {
        (void)connection;
        (void)reason;
    }

    // engine 级「关闭请求上报」：只在某次 OnOpen/OnTraffic 真实返回 Action::Shutdown 时被调用。
    // 一次真实返回上报一次（不去重、无「关闭首胜/不可复活」；同一连接连续多次返回即连续上报），
    // 且本回调不是「已关闭」通知：该请求本身不改变引擎状态；并发显式 Stop 仍可关闭服务/停 ticker，
    // 因此本回调不证明调用时刻的运行状态（不要把它当作「引擎仍 Running、端口仍接纳、ticker 仍投递」的保证）。
    //
    // 执行域：与产生它的事件**同一个受管协程任务**（同步于该连接既有的事件位内），不新增线程、
    // 不新增投递队列/引擎路由/状态位；同连接原事件仍序列化，不同连接的各次请求**可能并发**，
    // 因此本回调不构成 engine 全局串行点。
    //
    // 返回值/决策：本回调不返回 Action，上报不参与任何决策，也就不存在「上报 → 再产生动作」的
    // 自激回环。引擎不因本回调改变任何状态（不 SealService、不停 ticker、不改 phase_、
    // 不 Close 该连接、不拒绝后续业务）；**完全忽略它是合法且零成本的策略**。要停止引擎，请在
    // 自己的控制域（非受管协程、非本 Engine owner 线程）调用既有 Engine::Stop()。
    //
    // 可挂起：挂起只让出 Processer，不占 owner/网络线程；挂起期间该连接的后续事件自然等待本回调
    // 返回，其它连接继续进展。不得在本回调内调用 Engine::Stop()/Run/Join 或长期阻塞：受管协程被
    // 域门禁在**任何副作用之前**拒绝（operation_not_permitted，清晰报错、不阻塞）。
    //
    // 抛出：沿用 OnOpen/OnTraffic 相同的既有 handler 异常策略（计入 bridge 的 handler 异常观测，
    // 并触发该连接的封口 + 物理关闭收尾），异常不传播到 owner/网络线程，**不**改变 engine 停止
    // 状态、不自动 Stop；本回调不把它吞成成功。
    virtual void OnShutdownRequest() {}

    // engine 全局单 ticker 事件（受管协程；engine 级事件位，与 OnBoot/连接事件各自串行）。
    // 启用条件：Options::ticker == true。首次投递在 Run 成功进入 Running 且 listener 已挂
    // accept 就绪等待之后**立即**发生，不等待也不依赖本回调的返回值。
    // 返回值 = 相对**本次返回时刻**的下一次投递延迟（std::chrono::nanoseconds）：
    //   - 返回 <= 0：本次之后不再投递（ticker 停止）；
    //   - 返回 > 0 ：至少经过该延迟后才投递下一次。
    // 语义边界：
    //   - 「停止 ticker」不等于停止服务：Engine 仍 Running，端口仍接纳与回显；它也不是
    //     Action::Shutdown（该动作只表示向上请求、不停 ticker），也不把停止语义降级成
    //     Action::Close。
    //   - 默认实现返回 0 ⇒ 启用 ticker 而未覆盖本回调时恰好投递一次。
    // 串行/不重入：下一次投递只在本次返回之后安排，因此同刻至多一个 OnTick；在回调内等待/
    // 挂起会自然推迟下一次投递，且只让出 Processer，不占 owner/网络线程。
    // 极端返回值按 steady_clock 可表示范围饱和（溢出保护），不构成业务时限上限。
    // 抛出：本次 tick 记为失败，ticker 停止（Engine 继续服务，不封口任何连接），异常不传播
    // 到 owner/网络线程。
    // 内部投递失败（owner 命令队列拒绝：满/已封口、timer 装配失败、tick 协程登记失败）同样
    // 停止 ticker：Engine 继续服务、不封口连接、不重试、不向 Processer/owner 抛出；停投原因
    // 记在内部状态（不新增公共观测 API）。ticker 停止后不再有 OnTick 投递。
    virtual std::chrono::nanoseconds OnTick() { return std::chrono::nanoseconds{0}; }
};

} // namespace bbt::infra::net
