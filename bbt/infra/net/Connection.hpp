#pragma once

// 公共连接句柄（R2）：稳定值语义，持共享连接状态（ring/操作结果/关闭状态）。
// 句柄寿命独立于 socket/context：显式关闭或 Engine 收口后，已收数据仍可读；
// 残留句柄不访问已销毁 context，也不与 Engine 成强环。
//
// 线程/域约束：AsyncWrite/TryRead/状态查询/RequestClose 可在任意合法调用域；
// 物理 I/O 与原始回调只由 owner 推进。net 不做分帧、请求匹配或重发。

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>

#include <bbt/infra/net/detail/ConnectionState.hpp>

namespace bbt::infra::net {

// 单次已接纳发送的观察句柄：持操作记录，终态在句柄寿命内永久可查；
// 释放句柄不取消已接纳发送。不是通用 Future/Task 平台。
class WriteOperation {
public:
    WriteOperation() = default;
    explicit WriteOperation(std::shared_ptr<WriteOpRecord> record)
        : m_record(std::move(record)) {}

    bool Valid() const noexcept { return m_record != nullptr; }
    std::uint64_t Id() const noexcept { return m_record ? m_record->id : 0; }

    WriteOutcome Query() const {
        if (!m_record) {
            return {};
        }
        std::lock_guard<std::mutex> lock(m_record->mutex);
        if (!m_record->terminal) {
            return WriteOutcome{WritePhase::InFlight, m_record->sent, {}};
        }
        return WriteOutcome{WritePhase::Done, m_record->sent, m_record->error};
    }

private:
    std::shared_ptr<WriteOpRecord> m_record;
};

class Connection {
public:
    Connection() = default;
    explicit Connection(std::shared_ptr<ConnectionState> state) : m_state(std::move(state)) {}

    bool Valid() const noexcept { return m_state != nullptr; }
    std::uint64_t Id() const noexcept { return m_state ? m_state->m_id : 0; }
    const std::shared_ptr<ConnectionState>& State() const noexcept { return m_state; }

    // 全量接纳或零接纳；成功后库取得数据所有权，观察句柄用于终态查询/等待。
    std::error_code AsyncWrite(std::string data, WriteOperation& out_operation) {
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

    // 竞争 Busy（零消费）；空且无终态 WouldBlock；Ok 已复制并消费。
    ReceiveRing::ReadResult TryRead(std::string& out) {
        if (!m_state) {
            return {ReceiveRing::ReadStatus::Error, 0, make_error_code(NetErrc::Closed)};
        }
        return m_state->ring.TryRead(out);
    }

    // 同一短保护内的 Peek+Consume 组合守卫；不得在守卫内存活期间挂起/回调。
    ReceiveRing::ReadAccess TryAccess() {
        if (!m_state) {
            return {};
        }
        return m_state->ring.TryAccess();
    }

    std::size_t ReadableBytes() { return m_state ? m_state->ring.Size() : 0; }

    std::error_code ReadError() const { return m_state ? m_state->ring.ReadError() : std::error_code{}; }
    bool ReadEof() const { return m_state ? m_state->ring.ReadEof() : true; }

    std::error_code LastWriteError(std::uint64_t* operation = nullptr) {
        return m_state ? m_state->LastWriteError(operation) : make_error_code(NetErrc::Closed);
    }

    AcceptanceState WriteAcceptance() const {
        return m_state ? m_state->Acceptance() : AcceptanceState::Closed;
    }

    bool Closed() const { return m_state ? m_state->Closed() : true; }
    std::error_code CloseReason() const {
        return m_state ? m_state->CloseReason() : make_error_code(NetErrc::Closed);
    }

    // 请求封口：立即拒绝新写并唤 owner；物理 fd 关闭、已 recv 尾部入真源、
    // 最终读 EOF 与 OnClose 由 owner 兑现。请求后读侧可能短暂 WouldBlock，
    // 不得在尾部尚未入 ring 前返回 EOF。幂等；首次请求的 reason（包括默认 {}）
    // 固定，后续请求不覆盖，最终关闭状态不回退。
    void RequestClose(std::error_code reason = {}) {
        if (m_state) {
            m_state->RequestClose(reason);
        }
    }

private:
    std::shared_ptr<ConnectionState> m_state;
};

} // namespace bbt::infra::net
