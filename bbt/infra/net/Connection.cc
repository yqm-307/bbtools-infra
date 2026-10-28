#include <bbt/infra/net/Connection.hpp>

#include <utility>

#include <bbt/infra/net/Error.hpp>
#include <bbt/infra/net/detail/ConnectionState.hpp>
#include <bbt/infra/net/detail/ReclaimRecord.hpp>
#include <bbt/infra/net/detail/ReceiveRing.hpp>

// 实现放 .cc：Connection/WriteOperation 的转发需要 ConnectionState/ReceiveRing
// 完整定义，放在公共头会把 state/ring 布局与内部枚举经公共面传出。

namespace bbt::infra::net {

// ReclaimReceipt：无效句柄没有已提交资源责任（Released=true）；有效句柄的
// 释放状态在共享记录上，终态在句柄寿命内永久可查，观察不改变回收本身。
bool ReclaimReceipt::Valid() const noexcept { return m_record != nullptr; }

bool ReclaimReceipt::Released() const noexcept {
    return !m_record || m_record->released.load(std::memory_order_acquire);
}

ReclaimReceipt::ReclaimReceipt(std::shared_ptr<ReclaimRecord> record)
    : m_record(std::move(record)) {}

WriteOperation::WriteOperation(std::shared_ptr<WriteOpRecord> record)
    : m_record(std::move(record)) {}

bool WriteOperation::Valid() const noexcept { return m_record != nullptr; }

std::uint64_t WriteOperation::Id() const noexcept { return m_record ? m_record->id : 0; }

WriteOutcome WriteOperation::Query() const {
    if (!m_record) {
        return {};
    }
    std::lock_guard<std::mutex> lock(m_record->mutex);
    if (!m_record->terminal) {
        return WriteOutcome{WritePhase::InFlight, m_record->sent, {}};
    }
    return WriteOutcome{WritePhase::Done, m_record->sent, m_record->error};
}

Connection::Connection(std::shared_ptr<ConnectionState> state) : m_state(std::move(state)) {}

bool Connection::Valid() const noexcept { return m_state != nullptr; }

std::uint64_t Connection::Id() const noexcept { return m_state ? m_state->m_id : 0; }

std::error_code Connection::AsyncWrite(std::string data, WriteOperation& out_operation) {
    out_operation = WriteOperation();  // 拒绝时保持无效，不泄露上次结果
    if (!m_state) {
        return make_error_code(NetErrc::Closed);
    }
    std::shared_ptr<WriteOpRecord> record;
    const auto error = m_state->AcceptWrite(std::move(data), record);
    if (!error) {
        out_operation = WriteOperation(std::move(record));
    }
    return error;
}

ReadResult Connection::TryRead(std::string& out) {
    if (!m_state) {
        return {ReadStatus::Error, 0, make_error_code(NetErrc::Closed)};
    }
    return m_state->ring.TryRead(out);
}

ReadAccess Connection::TryAccess() {
    if (!m_state) {
        return {};
    }
    auto attempt = m_state->ring.TryAccessAttempt();
    return ReadAccess(m_state, std::move(attempt.lock), attempt.status, attempt.error);
}

std::size_t Connection::ReadableBytes() {
    return m_state ? m_state->ring.Size() : 0;
}

std::error_code Connection::ReadError() const {
    return m_state ? m_state->ring.ReadError() : std::error_code{};
}

bool Connection::ReadEof() const { return m_state ? m_state->ring.ReadEof() : true; }

std::error_code Connection::LastWriteError(std::uint64_t* operation) {
    return m_state ? m_state->LastWriteError(operation) : make_error_code(NetErrc::Closed);
}

AcceptanceState Connection::WriteAcceptance() const {
    return m_state ? m_state->Acceptance() : AcceptanceState::Closed;
}

bool Connection::Closed() const { return m_state ? m_state->Closed() : true; }

bool Connection::Released() const noexcept {
    // 无效句柄没有资源责任；有效句柄以 owner 物理收口标记为准（conn_state
    // 记录在 fd 真实关闭且 owner 责任清理后置位）。
    return !m_state || m_state->OwnerReleased();
}

std::error_code Connection::CloseReason() const {
    return m_state ? m_state->CloseReason() : make_error_code(NetErrc::Closed);
}

void Connection::RequestClose(std::error_code reason) {
    if (m_state) {
        m_state->RequestClose(reason);
    }
}

} // namespace bbt::infra::net
