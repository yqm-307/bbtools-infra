#include <bbt/infra/net/CoConnection.hpp>

#include <cstdint>
#include <functional>
#include <mutex>
#include <system_error>
#include <utility>

#include <bbt/coroutine/coroutine.hpp>

#include <bbt/infra/net/detail/CoInternal.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/TestAccess.hpp>

// 实现放 .cc：Co 入口需要连接内部状态、Engine 私有等待步骤与统一 gate，
// 这些都不能经公共头传出；同时避免把 <bbt/coroutine/coroutine.hpp> 带进
// 每个含 CoConnection.hpp 的消费 TU。

namespace bbt::infra::net {

namespace {

std::error_code InvalidContext()
{
    return std::make_error_code(std::errc::operation_not_permitted);
}

std::error_code NoBufferSpace()
{
    return std::make_error_code(std::errc::no_buffer_space);
}

// WaitStatus 完整枚举逐项映射（不设 io_error 默认项）：上游该 SHA 恰有
// Completed/TimedOut/Cancelled/InvalidContext/AlreadyWaiting/RuntimeUnavailable。
// 返回 true 表示已是终态（调用者应把 status_error 交回或终止等待）。
struct WaitMapping {
    bool terminal{false};
    std::error_code error;
};

WaitMapping MapWaitStatus(bbt::coroutine::WaitStatus status)
{
    // Completed 表示本次等待被通知，还需调用者复查谓词；其余状态一律终结。
    if (status == bbt::coroutine::WaitStatus::Completed) {
        return {false, {}};
    }
    return {true, WaitStatusError(status)};
}

// CoDial 私有 test 屏障（正常为空）：提交之后、领取 attach 之前。
std::function<void()>& DialBarrierForTest()
{
    static std::function<void()> barrier;
    return barrier;
}

} // namespace

std::error_code RequireCoroutineContext()
{
    // 与 ICoObject::RequireCoroutine 同源判定：不构造 CoObject、不缓存 TLS。
    return bbt::coroutine::GetLocalCoroutineId() != 0 ? std::error_code{} : InvalidContext();
}

std::error_code WaitStatusError(bbt::coroutine::WaitStatus status)
{
    switch (status) {
    case bbt::coroutine::WaitStatus::Completed:
        return {};
    case bbt::coroutine::WaitStatus::TimedOut:
        return std::make_error_code(std::errc::timed_out);
    case bbt::coroutine::WaitStatus::Cancelled:
        return std::make_error_code(std::errc::operation_canceled);
    case bbt::coroutine::WaitStatus::InvalidContext:
        return InvalidContext();
    case bbt::coroutine::WaitStatus::AlreadyWaiting:
        // 唯一等待位被占用：本地并发冲突，不是 I/O 结果。
        return std::make_error_code(std::errc::device_or_resource_busy);
    case bbt::coroutine::WaitStatus::RuntimeUnavailable:
        return std::make_error_code(std::errc::operation_not_supported);
    }
    return std::make_error_code(std::errc::operation_not_supported);
}

void CoTestAccess::SetDialBarrier(std::function<void()> barrier)
{
    DialBarrierForTest() = std::move(barrier);
}

std::shared_ptr<ConnectionState> CoAccess::StateOf(const Connection& connection)
{
    return connection.m_state;
}

std::error_code CoAccess::SubmitConnect(Engine& engine, const Address& address,
                                       std::uint64_t& out_connect_id)
{
    return engine.CoSubmitConnect(address, out_connect_id);
}

std::error_code CoAccess::WaitConnect(Engine& engine, std::uint64_t connect_id, Connection& out,
                                     const bbt::coroutine::WaitOptions& options)
{
    return engine.CoWaitConnect(connect_id, out, options);
}

std::error_code CoAccess::WaitAccepted(Engine& engine, Connection& out,
                                      const bbt::coroutine::WaitOptions& options)
{
    return engine.CoWaitAccepted(out, options);
}

std::error_code CoRead(Connection& connection, std::string& out,
                       const bbt::coroutine::WaitOptions& options)
{
    // gate 在读取任何状态、取任何保护之前：非法域零消费。
    if (const auto gate = RequireCoroutineContext()) {
        return gate;
    }
    for (;;) {
        auto state = CoAccess::StateOf(connection);
        if (!state) {
            return make_error_code(NetErrc::Closed);
        }
        // 关键时序：在导致 Busy 的 TryAccess 之前采 release 代次。若在 Busy 之后
        // 才采，持锁方可能已在「Busy 返回」与「采样」之间释放并推进代次，导致
        // on_registered 复查相等而永久等下一次 release（已由诊断 probe 复现）。
        const std::uint64_t generation = state->ring.AccessGeneration();
        auto access = connection.TryAccess();
        if (access) {
            // 恢复后原子复制+消费；强保证：out 追加失败时 ring 不消费。
            return access.ConsumeInto(out);
        }
        if (access.Eof()) {
            return make_error_code(NetErrc::Eof);
        }
        if (access.Error()) {
            return access.ErrorCode();
        }
        const bool busy = access.Busy();
        // waiter 创建在等待前，真正加入 hub 放到 on_registered（runtime 已登记
        // 事件后），避免早到 Notify 落在未登记事件上丢失。
        if (busy) {
            state->ring.InvokeBusyBeforeWaiterHookForTest();
        } else {
            state->ring.InvokeWouldBlockBeforeWaiterHookForTest();
        }
        auto waiter = busy ? state->ring.CreateReleaseWaiter()
                           : state->ring.CreateDataWaiter();
        if (!waiter) {
            return NoBufferSpace();
        }
        bool add_failed = false;
        const auto status = waiter->WaitWithCallback(options, [&] {
            const bool added = busy ? state->ring.AddReleaseWaiter(waiter)
                                    : state->ring.AddDataWaiter(waiter);
            if (!added) {
                add_failed = true;  // 满额：有界拒绝，不在库内自激重试
                waiter->Notify();
                return true;
            }
            state->ring.InvokeAfterAddHookForTest();
            const bool ready = busy
                ? (state->ring.AccessGeneration() != generation)
                : (state->ring.Size() > 0 || state->ring.ReadEof()
                   || state->ring.HasReadError() || state->ring.ReadClosed());
            if (ready) {
                waiter->Notify();
            }
            return true;
        });
        if (add_failed) {
            return NoBufferSpace();
        }
        const auto mapped = MapWaitStatus(status);
        if (mapped.terminal) {
            return mapped.error;  // 超时/取消不消费
        }
    }
}

std::error_code CoWrite(Connection& connection, const std::string& data,
                        const bbt::coroutine::WaitOptions& options,
                        WriteOutcome& outcome, WriteOperation& operation)
{
    // 每次进入先写本次结果与无效 op：任何后续提前返回都覆盖为本次语义，
    // 绝不泄露调用者变量里的上次结果。
    outcome = WriteOutcome{WritePhase::Rejected, 0, {}};
    operation = WriteOperation();
    // gate 在消费/提交之前：非法域零提交，且不留下任何写责任。
    if (const auto gate = RequireCoroutineContext()) {
        outcome.error = gate;
        return gate;
    }
    auto state = CoAccess::StateOf(connection);
    if (!state) {
        outcome.error = make_error_code(NetErrc::Closed);
        return make_error_code(NetErrc::Closed);
    }
    const std::size_t wanted = data.size();
    for (;;) {
        WriteOperation op;
        const auto error = connection.AsyncWrite(data, op);
        if (!error) {
            operation = op;
            for (;;) {
                auto status = op.Query();
                if (status.phase != WritePhase::InFlight) {
                    outcome = status;
                    return status.error;
                }
                auto waiter = state->CreateWriteWaiter();
                if (!waiter) {
                    outcome = op.Query();
                    return NoBufferSpace();
                }
                bool add_failed = false;
                const auto wait = waiter->WaitWithCallback(options, [&] {
                    if (!state->AddWriteWaiter(waiter)) {
                        add_failed = true;
                        waiter->Notify();
                        return true;
                    }
                    if (op.Query().phase != WritePhase::InFlight) {
                        waiter->Notify();
                    }
                    return true;
                });
                if (add_failed) {
                    // 已接纳写：保留 operation 与当前 outcome，不撤销发送。
                    outcome = op.Query();
                    if (outcome.phase == WritePhase::Unknown) {
                        outcome.phase = WritePhase::InFlight;
                    }
                    return NoBufferSpace();
                }
                const auto mapped = MapWaitStatus(wait);
                if (mapped.terminal) {
                    // 已接纳：outcome 是本次 op 当时采样，可能已经 Done；
                    // 返回错误描述等待结果，outcome.error 描述发送结果。
                    outcome = op.Query();
                    if (outcome.phase == WritePhase::Unknown) {
                        outcome.phase = WritePhase::InFlight;
                    }
                    return mapped.error;
                }
            }
        }
        // 能力上永远不可能接纳的超大请求：立即拒绝，绝不无限等。
        if (!error || error == NoBufferSpace() || error == make_error_code(NetErrc::WriteSettling)) {
            if (wanted > state->MaxOutboundBytes()) {
                outcome = WriteOutcome{WritePhase::Rejected, 0, NoBufferSpace()};
                operation = WriteOperation();
                return NoBufferSpace();
            }
            if (state->Closed() || connection.Closed()) {
                outcome = WriteOutcome{WritePhase::Rejected, 0, make_error_code(NetErrc::Closed)};
                operation = WriteOperation();
                return make_error_code(NetErrc::Closed);
            }
            auto waiter = state->CreateWriteWaiter();
            if (!waiter) {
                outcome = WriteOutcome{WritePhase::Rejected, 0, NoBufferSpace()};
                operation = WriteOperation();
                return NoBufferSpace();
            }
            bool add_failed = false;
            const auto wait = waiter->WaitWithCallback(options, [&] {
                if (!state->AddWriteWaiter(waiter)) {
                    add_failed = true;
                    waiter->Notify();
                    return true;
                }
                if (state->CanAcceptWrite(wanted) || state->Closed()) {
                    waiter->Notify();
                }
                return true;
            });
            if (add_failed) {
                outcome = WriteOutcome{WritePhase::Rejected, 0, NoBufferSpace()};
                operation = WriteOperation();
                return NoBufferSpace();
            }
            const auto mapped = MapWaitStatus(wait);
            if (mapped.terminal) {
                outcome = WriteOutcome{WritePhase::Rejected, 0, mapped.error};
                operation = WriteOperation();
                return mapped.error;
            }
            continue;
        }
        outcome = WriteOutcome{WritePhase::Rejected, 0, error};
        operation = WriteOperation();
        return error;
    }
}

std::error_code CoAccept(Engine& engine, Connection& out,
                         const bbt::coroutine::WaitOptions& options)
{
    // 配置/生命周期快速拒绝放在 gate 之前不成立——gate 必须先发生，避免
    // 非协程域观察到"配置错误以外的副作用"。这里 gate 之后仍是零副作用。
    if (const auto gate = RequireCoroutineContext()) {
        return gate;
    }
    Connection result;  // 临时结果：失败不覆盖调用者原 Connection
    const auto error = CoAccess::WaitAccepted(engine, result, options);
    if (error) {
        return error;
    }
    out = std::move(result);
    return {};
}

std::error_code CoDial(Engine& engine, const Address& address, Connection& out,
                       const bbt::coroutine::WaitOptions& options)
{
    if (const auto gate = RequireCoroutineContext()) {
        return gate;
    }
    std::uint64_t id = 0;
    // 提交即登记接收意图；提交失败零副作用，无需回收。
    if (const auto error = CoAccess::SubmitConnect(engine, address, id)) {
        return error;
    }
    if (auto& barrier = DialBarrierForTest()) {
        barrier();  // 测试：强制 owner 完成早于领取 attach
    }
    Connection result;  // 临时结果：失败不覆盖调用者原 Connection
    const auto error = CoAccess::WaitConnect(engine, id, result, options);
    if (error) {
        // 领取失败的全部路径（等待建立失败、登记满、非法域/运行时不可用、
        // 超时/取消）已在 CoWaitConnect 内归还 claim 并让 owner 回收未交付 fd。
        return error;
    }
    out = std::move(result);
    return {};
}

} // namespace bbt::infra::net
