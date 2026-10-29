#include <bbt/infra/net/CoTcpConnection.hpp>

#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <bbt/infra/net/CoConnection.hpp>
#include <bbt/infra/net/Error.hpp>
#include <bbt/infra/net/detail/CoInternal.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>

// 实现放 .cc：Read/Write/WaitClosed 的等待路径要复用 ConnectionState/
// ReceiveRing/close hub 与统一 gate；这些是 detail 部件，不进公共头。
//
// Read 走 ring 的 pop-all 移交：Busy/WouldBlock 等待舞步与 CoRead 同构
// （Busy 前先采 release 代次、waiter 在 on_registered 才入 hub、复查谓词），
// 唯一差异是取数原语——TryPopAll 移交整段 backing（含绕环两段与
// pending/detached 尾部），载荷零复制；额度在移交成功瞬间立即释放。

namespace bbt::infra::net {

ConnectionStatus CoTcpConnection::Status() const
{
    ConnectionStatus status;
    status.valid = m_conn.Valid();
    if (!status.valid) {
        return status;
    }
    status.readable_bytes = const_cast<Connection&>(m_conn).ReadableBytes();
    status.read_eof = m_conn.ReadEof();
    status.read_error = m_conn.ReadError();
    status.closed = m_conn.Closed();
    status.close_reason = m_conn.CloseReason();
    status.released = m_conn.Released();
    status.write = m_conn.WriteAcceptance();  // 写接纳状态真源采样
    return status;
}

ReadOutcome CoTcpConnection::Read(
    const bbt::coroutine::WaitOptions& options)
{
    ReadOutcome outcome;
    // gate 与 CoRead 同源且先于任何状态判定/保护取得。
    if (const auto gate = RequireCoroutineContext()) {
        outcome.state = ReadState::Error;
        outcome.error = gate;
        return outcome;
    }
    for (;;) {
        auto state = CoAccess::StateOf(m_conn);
        if (!state) {
            outcome.state = ReadState::Error;
            outcome.error = make_error_code(NetErrc::Closed);
            return outcome;
        }
        // 与 CoRead 相同的关键时序：在导致 Busy 的尝试之前采 release 代次，
        // 否则持锁方可能在「Busy 返回」与「采样」之间释放并推进代次，造成
        // on_registered 复查相等而永久等下一次 release。
        const std::uint64_t generation = state->ring.AccessGeneration();
        auto attempt = state->ring.TryPopAll();
        if (attempt.status == ReadStatus::Ok) {
            // 段元数据在 pop 事务内已构造为 Buffer-ready（P1-1）：Read 不做任何
            // post-pop 分配，直接把 takeout 与段表移进 Buffer——pop 已提交的
            // 字节在此后没有可失败的中间步骤。
            outcome.state = ReadState::Data;
            // shared_ptr move 后 takeout 本身置空：先取走段表（move vector
            // 不分配），再转 backing——两步都不抛。
            std::vector<BufferSegment> segments =
                std::move(attempt.takeout->segments);
            outcome.data = Buffer(std::move(attempt.takeout), std::move(segments));
            outcome.error = {};
            return outcome;
        }
        if (attempt.status == ReadStatus::Eof) {
            outcome.state = ReadState::Eof;
            outcome.error = {};
            return outcome;
        }
        if (attempt.status == ReadStatus::Error) {
            outcome.state = ReadState::Error;
            outcome.error = attempt.error;
            return outcome;
        }
        const bool busy = attempt.status == ReadStatus::Busy;
        if (busy) {
            state->ring.InvokeBusyBeforeWaiterHookForTest();
        } else {
            state->ring.InvokeWouldBlockBeforeWaiterHookForTest();
        }
        auto waiter = busy ? state->ring.CreateReleaseWaiter()
                           : state->ring.CreateDataWaiter();
        if (!waiter) {
            outcome.state = ReadState::Error;
            outcome.error = std::make_error_code(std::errc::no_buffer_space);
            return outcome;
        }
        bool add_failed = false;
        const auto status = waiter->WaitWithCallback(options, [&] {
            const bool added = busy ? state->ring.AddReleaseWaiter(waiter)
                                    : state->ring.AddDataWaiter(waiter);
            if (!added) {
                add_failed = true;
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
            outcome.state = ReadState::Error;
            outcome.error = std::make_error_code(std::errc::no_buffer_space);
            return outcome;
        }
        if (status != bbt::coroutine::WaitStatus::Completed) {
            outcome.state = ReadState::Error;
            outcome.error = WaitStatusError(status);  // 超时/取消不消费
            return outcome;
        }
    }
}

WriteOperation CoTcpConnection::AsyncWrite(std::string data)
{
    // 非挂起提交：全量接纳或零接纳。拒绝返回「拒绝票」——无效句柄
    // （Valid()=false）但 Query() 给出 Rejected/0/具体原因，不分配
    // 操作记录（终评 §2）。
    const auto state = CoAccess::StateOf(m_conn);
    if (!state) {
        return WriteOperation(nullptr, {}, make_error_code(NetErrc::Closed));
    }
    std::shared_ptr<WriteOpRecord> record;
    const auto error = state->AcceptWrite(std::move(data), record);
    if (error) {
        return WriteOperation(nullptr, state, error);
    }
    return WriteOperation(std::move(record), state);
}

WriteReport CoTcpConnection::Write(std::string data,
                                   const bbt::coroutine::WaitOptions& options)
{
    // 等容量 → 接纳 → 等本地完成，同一绝对 deadline 沿重试保持。
    // wait_error 只描述等待/调用结果：接纳后 Done+error 落在
    // outcome.error（不冒充等待失败）；等容量失败为 Rejected+无效 op。
    WriteReport report;
    report.outcome = WriteOutcome{WritePhase::Rejected, 0, {}};
    if (const auto gate = RequireCoroutineContext()) {
        report.wait_error = gate;
        report.outcome.error = gate;
        return report;
    }
    const auto state = CoAccess::StateOf(m_conn);
    if (!state) {
        report.wait_error = make_error_code(NetErrc::Closed);
        report.outcome.error = report.wait_error;
        return report;
    }
    const std::size_t wanted = data.size();
    const auto no_buffer = std::make_error_code(std::errc::no_buffer_space);
    const auto settling = make_error_code(NetErrc::WriteSettling);
    for (;;) {
        std::shared_ptr<WriteOpRecord> record;
        const auto error = state->AcceptWrite(data, record);
        if (!error) {
            report.operation = WriteOperation(std::move(record), state);
            // 等同一 record 终态；超时/取消不撤销已接纳发送，outcome
            // 是返回时采样（等待首胜后可能已采到 Done）。
            report.wait_error = report.operation.Wait(options, &report.outcome);
            return report;
        }
        // 能力上永远不可能接纳的超大请求：立即拒绝，绝不无限等。
        if (error == no_buffer || error == settling) {
            if (wanted > state->MaxOutboundBytes()) {
                report.wait_error = no_buffer;
                report.outcome.error = no_buffer;
                return report;
            }
            if (m_conn.Closed()) {
                report.wait_error = make_error_code(NetErrc::Closed);
                report.outcome.error = report.wait_error;
                return report;
            }
            auto waiter = state->CreateWriteWaiter();
            if (!waiter) {
                report.wait_error = no_buffer;
                report.outcome.error = no_buffer;
                return report;
            }
            bool add_failed = false;
            const auto status = waiter->WaitWithCallback(options, [&] {
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
                report.wait_error = no_buffer;
                report.outcome.error = no_buffer;
                return report;
            }
            if (status != bbt::coroutine::WaitStatus::Completed) {
                // 容量等待超时/取消：本次未提交，Rejected+无效 op。
                report.wait_error = WaitStatusError(status);
                report.outcome.error = report.wait_error;
                return report;
            }
            continue;
        }
        report.wait_error = error;
        report.outcome.error = error;
        return report;
    }
}

std::error_code CoTcpConnection::WaitClosed(
    const bbt::coroutine::WaitOptions& options)
{
    // gate 先于状态判定（与 CoRead/CoWrite 同契约）。
    if (const auto gate = RequireCoroutineContext()) {
        return gate;
    }
    const auto state = CoAccess::StateOf(m_conn);
    if (!state) {
        return {};  // 无效句柄视为已关闭终态
    }
    for (;;) {
        if (m_conn.Closed()) {
            return {};
        }
        // close hub 专用等待：Create/Add 两段（Add 在 on_registered 内），
        // 不直接用 RegisterCloseWaiter——后者在等待前注册，早到
        // MarkClosed 的 NotifyAll 会落在未登记事件上丢失。
        auto waiter = state->CreateCloseWaiter();
        if (!waiter) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        bool add_failed = false;
        const auto status = waiter->WaitWithCallback(options, [&] {
            if (!state->AddCloseWaiter(waiter)) {
                add_failed = true;
                waiter->Notify();
                return true;
            }
            if (m_conn.Closed()) {
                waiter->Notify();
            }
            return true;
        });
        if (add_failed) {
            return std::make_error_code(std::errc::no_buffer_space);
        }
        if (status != bbt::coroutine::WaitStatus::Completed) {
            return WaitStatusError(status);  // 超时/取消不关闭连接
        }
    }
}

} // namespace bbt::infra::net
