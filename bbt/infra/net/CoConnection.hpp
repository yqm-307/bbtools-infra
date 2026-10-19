#pragma once

// Co 薄封装（R2）：只复用既有 CoWaiter 与同一 raw 内核，不建第二收包队列、
// CoDial 要求 client-only Engine 未配置 RawHandler；配置了 raw handler 时
// AsyncConnectAwaitable 在提交前返回 operation_not_supported，raw 建连由
// OnConnect 唯一交付，Co 等待不得取消它。
// 使用 CoDial 的 client-only Engine 若无 raw 接收者，应以空 RawHandler 构造，
// 这样等待超时/取消后 owner 才能回收未交付的 socket（有 raw handler 时视为
// 已交付，不会因另一个 Co 等待取消而误关）。
// 不自动为连接建业务协程、不等业务回包、不自动关闭/重连/重发。
//
// 等待模式统一为「登记 waiter → on_registered 只观察/复查谓词并 Notify →
// 挂起 → 恢复后原子消费」。on_registered 绝不消费、绝不改 out；因此超时/
// 取消一旦赢仲裁不会附带消费。所有等待登记有界（WakeHub 上限）。

#include <cstdint>
#include <functional>
#include <string>
#include <system_error>

#include <bbt/coroutine/sync/CoWaiter.hpp>

#include <bbt/infra/net/Address.hpp>
#include <bbt/infra/net/Connection.hpp>
#include <bbt/infra/net/Engine.hpp>
#include <bbt/infra/net/Error.hpp>

namespace bbt::infra::net {

// 同一 ring 真源；WouldBlock 等数据/终态，Busy 等访问代次释放。超时不消费。
inline std::error_code CoRead(Connection& connection, std::string& out,
                              const bbt::coroutine::WaitOptions& options = {}) {
    for (;;) {
        auto state = connection.State();
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
            return std::make_error_code(std::errc::no_buffer_space);
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
            return std::make_error_code(std::errc::no_buffer_space);
        }
        if (status == bbt::coroutine::WaitStatus::TimedOut) {
            return std::make_error_code(std::errc::timed_out);  // 超时不消费
        }
        if (status == bbt::coroutine::WaitStatus::Cancelled) {
            return std::make_error_code(std::errc::operation_canceled);
        }
        if (status != bbt::coroutine::WaitStatus::Completed) {
            return std::make_error_code(std::errc::io_error);
        }
    }
}

// 组合 AsyncWrite 与容量/本地完成通知；同一绝对 deadline 沿重试保持。
//   - {} + outcome.phase==Done：本地完成（仍非对端回包）。
//   - timed_out + outcome.phase==InFlight：已接纳、仍在途，operation 可查最终结果。
//   - timed_out + outcome.phase==Rejected：容量等待超时，未提交。
inline std::error_code CoWrite(Connection& connection, const std::string& data,
                               const bbt::coroutine::WaitOptions& options,
                               WriteOutcome& outcome, WriteOperation& operation) {
    auto state = connection.State();
    if (!state) {
        outcome = WriteOutcome{WritePhase::Rejected, 0, make_error_code(NetErrc::Closed)};
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
                    return std::make_error_code(std::errc::no_buffer_space);
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
                    return std::make_error_code(std::errc::no_buffer_space);
                }
                if (wait == bbt::coroutine::WaitStatus::TimedOut) {
                    outcome = op.Query();
                    if (outcome.phase == WritePhase::Unknown) {
                        outcome.phase = WritePhase::InFlight;
                    }
                    return std::make_error_code(std::errc::timed_out);
                }
                if (wait == bbt::coroutine::WaitStatus::Cancelled) {
                    outcome = op.Query();
                    if (outcome.phase == WritePhase::Unknown) {
                        outcome.phase = WritePhase::InFlight;
                    }
                    return std::make_error_code(std::errc::operation_canceled);
                }
                if (wait != bbt::coroutine::WaitStatus::Completed) {
                    outcome = op.Query();
                    return std::make_error_code(std::errc::io_error);
                }
            }
        }
        // 能力上永远不可能接纳的超大请求：立即拒绝，绝不无限等。
        if (!error || error == std::make_error_code(std::errc::no_buffer_space)
            || error == make_error_code(NetErrc::WriteSettling)) {
            if (wanted > state->MaxOutboundBytes()) {
                outcome = WriteOutcome{WritePhase::Rejected, 0,
                                       std::make_error_code(std::errc::no_buffer_space)};
                return std::make_error_code(std::errc::no_buffer_space);
            }
            if (state->Closed() || connection.Closed()) {
                outcome = WriteOutcome{WritePhase::Rejected, 0, make_error_code(NetErrc::Closed)};
                return make_error_code(NetErrc::Closed);
            }
            auto waiter = state->CreateWriteWaiter();
            if (!waiter) {
                return std::make_error_code(std::errc::no_buffer_space);
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
                outcome = WriteOutcome{WritePhase::Rejected, 0,
                                       std::make_error_code(std::errc::no_buffer_space)};
                return std::make_error_code(std::errc::no_buffer_space);
            }
            if (wait == bbt::coroutine::WaitStatus::TimedOut) {
                outcome = WriteOutcome{WritePhase::Rejected, 0,
                                       std::make_error_code(std::errc::timed_out)};
                return std::make_error_code(std::errc::timed_out);
            }
            if (wait == bbt::coroutine::WaitStatus::Cancelled) {
                outcome = WriteOutcome{WritePhase::Rejected, 0,
                                       std::make_error_code(std::errc::operation_canceled)};
                return std::make_error_code(std::errc::operation_canceled);
            }
            if (wait != bbt::coroutine::WaitStatus::Completed) {
                outcome = WriteOutcome{WritePhase::Rejected, 0,
                                       std::make_error_code(std::errc::io_error)};
                return std::make_error_code(std::errc::io_error);
            }
            continue;
        }
        outcome = WriteOutcome{WritePhase::Rejected, 0, error};
        return error;
    }
}

inline std::error_code CoAccept(Engine& engine, Connection& out,
                                const bbt::coroutine::WaitOptions& options = {}) {
    return engine.WaitAccepted(out, options);
}

// 私有 test 屏障：AsyncConnectAwaitable 之后、WaitConnect 之前（正常为空）。
inline std::function<void()>& CoDialBarrierForTest() {
    static std::function<void()> barrier;
    return barrier;
}
inline void SetCoDialBarrierForTest(std::function<void()> barrier) {
    CoDialBarrierForTest() = std::move(barrier);
}

inline std::error_code CoDial(Engine& engine, const Address& address, Connection& out,
                              const bbt::coroutine::WaitOptions& options = {}) {
    std::uint64_t id = 0;
    const auto error = engine.AsyncConnectAwaitable(address, id);
    if (error) {
        return error;
    }
    if (auto& barrier = CoDialBarrierForTest()) {
        barrier();  // 测试：强制 owner 完成早于 WaitConnect attach
    }
    return engine.WaitConnect(id, out, options);
}

} // namespace bbt::infra::net
