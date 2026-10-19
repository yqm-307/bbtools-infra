#pragma once

// 公共 Engine（R2）：Engine::Run 占调用它的宿主 native 控制线程，自动绑定
// 已登记 Processer 槽，每个 Reactor 由对应 P 的 driver 推进自有 Asio context；
// 不新建库内控制线程或网络 I/O 线程。
//
// 生命周期（详见 docs/architecture/gnet-network-execution.md §6）：
//   - Run 先校验 runtime 已初始化（RuntimeNotInitialized）与宿主「runtime 内部
//     EventLoop 由独立执行流持续推进」的必填声明（RuntimeProgressRequired），
//     在任何绑定/建 socket 副作用之前拒绝。
//   - 成功后自动绑槽（server 1 main + N sub；client-only 无 main），安装 listener，
//     发布 Running，进入控制循环。
//   - RequestClose 并发安全、幂等，只封口并唤醒控制域；Run 返回才表示本 Engine
//     网络资源/回调/driver 已收口。
//   - 控制域按持有 token 有限复查 QueryProcesserDriver；Failed 时封口、同代解绑、
//     以冻结状态与内部完成订阅为替代出口，不伪调 owner 用户回调。
//
// 原始回调在 owner 非业务协程边界短、非阻塞、不可挂起执行。

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

struct EngineImpl;  // detail 定义在 Engine.cc

class EngineTestAccess;

class Engine {
public:
    explicit Engine(std::shared_ptr<RawHandler> handler);
    ~Engine();
    Engine(const Engine&) = delete;
    Engine& operator=(const Engine&) = delete;

    // 阻塞的宿主控制线程入口；成功收口后返回。失败在绑定/socket 副作用前返回。
    std::error_code Run(const EngineConfig& config);

    void RequestClose() noexcept;
    EnginePhase Phase() const noexcept;
    // 启动失败/Run 收口错误（无错误为空）；可在 Run 返回后查询。
    std::error_code LastError() const;
    // driver Failed 时保留的异常（无则空）；不伪调 owner 用户回调。
    std::exception_ptr LastFailure() const;

    // 实际绑定地址快照（port 0 → 内核分配）。未运行/已停止返回
    // operation_not_permitted，且不改写 out。
    std::error_code LocalAddresses(std::vector<Address>& out) const;

    std::size_t CountConnections() const;
    // 用户原始回调抛出的隔离计数（非 engine 故障；不影响 Run 错误返回）。
    std::size_t HandlerErrorCount() const;

    // 观测（非业务）：本代各 owner 绑定的 ProcesserId。server 的 main 是
    // main_owners 第一项；client-only main_owners 为空。未运行返回空。
    std::error_code ReactorOwnerIds(std::vector<std::uint64_t>& main_owners,
                                    std::vector<std::uint64_t>& sub_owners) const;



    // client 侧非阻塞建连：client-only 与 server 布局都可用（用 sub owner）。
    // 有 RawHandler 时结果只经 OnConnect 唯一交付，不能用 WaitConnect 二次领取；
    // 无 RawHandler 时 out_id 可交给 WaitConnect 等待。
    std::error_code AsyncConnect(const Address& address, std::uint64_t& out_connect_id);
    // Co 专用：只支持无 RawHandler 的 Engine（否则 operation_not_supported）。
    // 提交即登记接收责任，完成早于 WaitConnect 也保留结果，消费后释放。
    std::error_code AsyncConnectAwaitable(const Address& address,
                                          std::uint64_t& out_connect_id);
    // 协程内等待建连结果：有 RawHandler 返回 operation_not_supported，且不取消
    // raw pending。每个 id 只允许一个 receiver，第二个返回 device_or_resource_busy。
    // 超时/取消终结该 receiver 并由 owner 回收未交付连接，不能再用同 id 重试；
    // waiter 建立失败返回 not_enough_memory 并回滚 claim，同 id 可重试。
    std::error_code WaitConnect(std::uint64_t connect_id, Connection& out,
                                const bbt::coroutine::WaitOptions& options);

    // CoAccept 薄封装：从有界 handoff 队列取已接纳连接（队列满时暂停 native
    // accept，不丢 fd）。注册后复查，早到通知不丢。
    std::error_code WaitAccepted(Connection& out,
                                 const bbt::coroutine::WaitOptions& options);

    // 打开 CoAccept handoff（Run 前调用），容量 > 0 才有界预留给 accept。
    std::error_code EnableAcceptHandoff(std::size_t capacity);

private:
    friend class EngineTestAccess;
    std::shared_ptr<EngineImpl> m_impl;
};

} // namespace bbt::infra::net
