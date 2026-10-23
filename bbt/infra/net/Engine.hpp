#pragma once

// 公共 Engine（R2）：Engine::Run 占调用它的宿主 native 控制线程，自动绑定
// 已登记 Processer 槽，每个 Reactor 由对应 P 的 driver 推进自有 Asio context；
// 不新建库内控制线程或网络 I/O 线程。
//
// 生命周期（详见 docs/architecture/gnet-network-execution.md §6）：
//   - Run 先校验宿主控制域：在协程上下文或已登记 Processer 上调用返回
//     operation_not_permitted；runtime 未初始化返回 operation_not_supported；
//     未声明 RuntimeProgress::Independent 返回 operation_not_permitted。
//     全部发生在 driver 绑定/socket 副作用之前。
//   - server 布局（listen_addresses 非空）要求 handler 与 handoff 至少一个存在：
//     两者都无时 Run 返回 operation_not_permitted，同样发生在副作用之前。
//   - 成功后自动绑槽（server 1 main + N sub；client-only 无 main），安装 listener，
//     发布 Running（此时 LocalAddresses 才可能非空），进入控制循环。
//   - RequestClose 并发安全、幂等，只封口并唤醒控制域；Run 返回才表示本 Engine
//     网络资源/回调/driver 已收口。
//   - 控制域按持有 token 有限复查 QueryProcesserDriver；Failed 时封口、同代解绑、
//     以冻结状态、LastError/LastFailure 与内部完成订阅为替代出口，不伪调 owner
//     用户回调（包括不伪报建连成功/失败）。
//
// 原始回调在 owner 非业务协程边界短、非阻塞、不可挂起执行。
// 运行时对象（Scheduler/driver/Processer）由宿主负责，本类不取消它们的寿命。

#include <cstddef>
#include <exception>
#include <cstdint>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/Options.hpp>
#include <bbt/infra/net/RawHandler.hpp>

namespace bbt::infra::net {

enum class EnginePhase { Idle, Starting, Running, Closing, Stopped };

// 宿主对 runtime 推进方式的必填声明：Independent 表示 runtime 内部 EventLoop
// 由独立于 Run 的执行流持续推进（THREAD 的 S 线程 / SCHE_LOOP 另一线程 /
// NO_LOOP 另一执行流持续 LoopOnce）。未声明在绑定前拒绝。
enum class RuntimeProgress { Unspecified, Independent };

struct EngineConfig {
    std::vector<Address> listen_addresses;  // 空 = client-only（无 main accept owner）
    Options options;
    RuntimeProgress runtime_progress{RuntimeProgress::Unspecified};
};

struct EngineImpl;  // 定义在 Engine.cc

class EngineTestAccess;
struct CoAccess;

class Engine {
public:
    // handler 可为空：client-only 布局允许无 raw 接收者（Co 消费），
    // 但 server 布局必须另有 handoff 接收者（见 Run）。
    explicit Engine(std::shared_ptr<RawHandler> handler);
    ~Engine();
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    // 阻塞的宿主控制线程入口；成功收口后返回。前置校验失败在绑定/socket
    // 副作用前返回。
    std::error_code Run(const EngineConfig& config);

    void RequestClose() noexcept;
    EnginePhase Phase() const noexcept;
    // 启动失败/Run 收口错误（无错误为空）；可在 Run 返回后查询。
    std::error_code LastError() const;
    // driver Failed 时保留的异常（无则空）；不伪调 owner 用户回调。
    std::exception_ptr LastFailure() const;

    // 实际绑定地址快照（port 0 → 内核分配）。
    //   - Running/Closing：返回本代快照。
    //   - Starting：可返回当前快照，可能尚未安装 listener 因而为空；这不是
    //     "无监听"结论，只在 Running 之后才有完整语义。
    //   - Idle/Stopped：返回 operation_not_permitted，且不改写 out。
    std::error_code LocalAddresses(std::vector<Address>& out) const;

    std::size_t CountConnections() const;
    // 用户原始回调抛出的隔离计数（非 engine 故障；不影响 Run 错误返回）。
    std::size_t HandlerErrorCount() const;

    // 观测（非业务）：本代各 owner 绑定的 ProcesserId。server 的 main 是
    // main_owners 第一项；client-only main_owners 为空。未运行返回空。
    std::error_code ReactorOwnerIds(std::vector<std::uint64_t>& main_owners,
                                    std::vector<std::uint64_t>& sub_owners) const;

    // 非阻塞建连，只用于配置了 RawHandler 的 Engine（raw 唯一交付）：
    //   - 无 handler：提交前返回 operation_not_supported，out_connect_id 不改；
    //     Co 建连走 CoDial，不公开中间 ID。
    //   - 返回成功只表示本次提交已接纳：out_connect_id 必为本次非零操作 ID，
    //     且该 ID 只在本次 Engine 内唯一（不是取消/等待句柄）。
    //   - 正常 owner 推进下，成功或失败由 OnConnect 恰好交付一次，回调的
    //     operation 等于返回的 out_connect_id。回调可能先于本函数返回，调用者
    //     不得假设"先拿到 id 再收到回调"，须用回调自带的 operation 关联。
    //   - 所有提交拒绝路径（无 handler、非 Running/已封口、容量满、投递失败）
    //     保持 out_connect_id 原值，且不产生本次 OnConnect。
    //   - driver Failed 时不伪调 owner 回调：只经 LastError/LastFailure 与
    //     收口状态报告。
    std::error_code AsyncConnect(const Address& address, std::uint64_t& out_connect_id);

    // 打开 CoAccept handoff（Run 前调用），容量 > 0 才有界预留给 accept。
    // 保留既有"handler + handoff"共存语义：同一连接可同时通知 OnAccept 并入
    // handoff 队列；两个读取者并存时只保证线性化的字节前缀，不自动路由。
    std::error_code EnableAcceptHandoff(std::size_t capacity);

private:
    friend class EngineTestAccess;
    friend struct CoAccess;

    // ---- Co 内部步骤（detail/CoInternal.hpp 是唯一调用方）----
    // 两段式建连是 CoDial 的内部细节：提交（登记接收意图）与领取（等待结果）。
    // 不公开，避免冻结一个无法放弃的中间 ID。
    std::error_code CoSubmitConnect(const Address& address, std::uint64_t& out_connect_id);
    // 领取：仅成功的本次结果交付 out；失败不改 out，且必须由本函数内部
    // 归还 claim/登记责任并让 owner 回收未交付 fd。
    std::error_code CoWaitConnect(std::uint64_t connect_id, Connection& out,
                                  const bbt::coroutine::WaitOptions& options);
    // 从有界 handoff 队列取已接纳连接；配置/生命周期错误立即返回，不等。
    std::error_code CoWaitAccepted(Connection& out,
                                   const bbt::coroutine::WaitOptions& options);

    std::shared_ptr<EngineImpl> m_impl;
};

} // namespace bbt::infra::net
