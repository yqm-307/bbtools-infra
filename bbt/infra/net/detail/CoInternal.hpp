#pragma once

// Co 内部步骤与执行域 gate（内部部件，不列入公共头自检）。
//
// 为什么独立：CoRead/CoWrite/CoAccept/CoDial 需要（a）在消费/提交前统一校验
// 当前执行域，(b) 访问连接内部状态与 Engine 的私有等待步骤。这两件事都不能
// 出现在公共头里——否则等于把测试接缝与等待实现冻结成公共契约。
//
// 四层 namespace 禁止：本头不新增命名空间层级，全部落在 bbt::infra::net。

#include <cstdint>
#include <functional>
#include <memory>
#include <system_error>

#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Connection.hpp>

namespace bbt::infra::net {

class ConnectionState;
class Engine;
struct ReclaimRecord;  // detail：物理回收共享释放记录

// 统一执行域 gate：复用上游无创建查询 GetLocalCoroutineId，
// 与 ICoObject::RequireCoroutine 判定同源。不为检查构造 CoObject、不缓存 TLS、
// 不创建 Processer。非法域返回 operation_not_permitted。
std::error_code RequireCoroutineContext();

// WaitStatus → net 错误码（Completed 返回空）。逐项映射实际上游枚举：
// TimedOut→timed_out，Cancelled→operation_canceled，
// InvalidContext→operation_not_permitted，AlreadyWaiting→device_or_resource_busy，
// RuntimeUnavailable→operation_not_supported。上游新增未映射状态显式返回
// operation_not_supported，不吞成 io_error。
std::error_code WaitStatusError(bbt::coroutine::WaitStatus status);

// 拨号私有 test 屏障的存储引用：Engine::CoDial 在提交后、领取 attach 前调用。
// 定义在 CoConnection.cc；CoTestAccess::SetDialBarrier 是唯一写入者。
// 返回的 function 正常为空（无屏障）。
std::function<void()>& DialBarrierForTest();

// Engine 私有 Co 步骤与连接内部状态的门面：CoConnection.cc 与 Engine.cc
// 是唯一调用方。两段式建连（提交 → 领取）在此收口，公共面只留
// Engine::CoDial/CoAccept。
struct CoAccess {
    // 返回连接内部状态的强引用：Co 等待必须在挂起期间保活 ring/hub。
    static std::shared_ptr<ConnectionState> StateOf(const Connection& connection);

    // 提交：只用于无 handler 的 Engine（否则 operation_not_supported 且零提交）。
    // reclaim 非空时为带句柄 CoDial 的本次责任记录；同时是 ReclaimReceipt 的
    // 唯一内部构造入口。
    static std::error_code SubmitConnect(Engine& engine, const Address& address,
                                        std::uint64_t& out_connect_id,
                                        const std::shared_ptr<ReclaimRecord>& reclaim = {});
    static ReclaimReceipt MakeReceipt(std::shared_ptr<ReclaimRecord> record);
    // 领取：成功才写 out；任何未交付失败都由内部归还 claim 并让 owner 回收 fd。
    static std::error_code WaitConnect(Engine& engine, std::uint64_t connect_id, Connection& out,
                                      const bbt::coroutine::WaitOptions& options);
    // 从 handoff 队列取连接；配置/生命周期错误立即返回，不等永远不会来的事件。
    static std::error_code WaitAccepted(Engine& engine, Connection& out,
                                       const bbt::coroutine::WaitOptions& options);
};

} // namespace bbt::infra::net
